#include "cluster.h"
#include "binary.h"
#include "darray.h"
#include "encoding.h"
#include "raft.h"
#include <arpa/inet.h>
#include <fcntl.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>

// Global hashring, initializing to 0 (zero-initialization).
static cluster_t cluster = {0};

static int set_nonblocking(int fd)
{
    int flags, result;
    flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1)
        return -1;

    result = fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    if (result == -1)
        return -1;

    return 0;
}

static int tcp_connect(const cluster_node_t *node, int nonblocking)
{

    int s, retval = -1;
    struct addrinfo *servinfo, *p;
    const struct addrinfo hints = {.ai_family   = AF_UNSPEC,
                                   .ai_socktype = SOCK_STREAM,
                                   .ai_flags    = AI_PASSIVE};

    char port_string[6];
    snprintf(port_string, sizeof(port_string), "%d", node->port);

    if (getaddrinfo(node->ip, port_string, &hints, &servinfo) != 0)
        return -1;

    for (p = servinfo; p != NULL; p = p->ai_next) {
        /* Try to create the socket and to connect it.
         * If we fail in the socket() call, or on connect(), we retry with
         * the next entry in servinfo. */
        if ((s = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
            continue;

        /* Try to connect. */
        if (connect(s, p->ai_addr, p->ai_addrlen) == -1) {
            close(s);
            break;
        }

        /* If we ended an iteration of the for loop without errors, we
         * have a connected socket. Let's return to the caller. */
        retval = s;
        break;
    }

    // Set now non-blocking so it's possible to block on the connect and have a
    // ready-to-write socket immediately
    if (nonblocking && set_nonblocking(retval) < 0)
        goto err;

    freeaddrinfo(servinfo);
    return retval; /* Will be -1 if no connection succeded. */

err:

    close(s);
    perror("socket(2) opening socket failed");
    return -1;
}

static ssize_t tcp_send(const cluster_node_t *node,
                        const cluster_message_t *message)
{
    // TODO temporary
    uint8_t buf[BUFSIZ] = {0};
    ssize_t length      = cluster.encoding->cluster_message_write(buf, message);
    return send(node->sock_fd, buf, length, 0);
}

static void tcp_close(cluster_node_t *node)
{
    if (node->sock_fd < 0)
        return;
    close(node->sock_fd);
    node->sock_fd = -1;
}

#define MAX_SHARDS       128
#define VNODE_MULTIPLIER 10

typedef struct {
    uint32_t hash;
    int shard_id;
} vnode_t;

typedef struct {
    size_t num_shards;
    size_t num_vnodes;
    vnode_t vnodes[MAX_SHARDS * VNODE_MULTIPLIER];
    cluster_node_t nodes[MAX_SHARDS];
} hashring_t;

// Global hashring, initializing to 0 (zero-initialization).
static hashring_t ring = {0};

// Basic Murmur3 hash function for simplicity
static uint32_t murmur3(const char *key, uint32_t seed)
{
    uint32_t h = seed;
    while (*key) {
        h ^= (uint8_t)(*key++);
        h *= 0x5bd1e995;
        h ^= h >> 15;
    }
    h ^= h >> 13;
    h *= 0x5bd1e995;
    h ^= h >> 15;
    return h;
}

/* Lookup function */
static cluster_node_t *hashring_lookup(const char *key)
{
    uint32_t hash = murmur3(key, 0);

    for (size_t i = 0; i < ring.num_vnodes; ++i) {
        if (ring.vnodes[i].hash >= hash)
            return &ring.nodes[ring.vnodes[i].shard_id];
    }

    /* Wrap-around case: return the lowest hash vnode */
    return &ring.nodes[ring.vnodes[0].shard_id];
}

static int vnodes_cmp(const void *a, const void *b)
{
    return ((const vnode_t *)a)->hash - ((const vnode_t *)b)->hash;
}

void hashring_init(const cluster_node_t *shards, size_t num_shards)
{
    if (num_shards > MAX_SHARDS) {
        fprintf(stderr, "Too many shards!\n");
        exit(EXIT_FAILURE);
    }

    ring.num_shards    = num_shards;
    ring.num_vnodes    = num_shards * VNODE_MULTIPLIER;

    size_t vnode_count = 0;
    for (size_t i = 0; i < num_shards; ++i) {
        ring.nodes[i] = shards[i]; // Copy node info only once

        for (size_t v = 0; v < VNODE_MULTIPLIER; ++v) {
            char buf[64];
            snprintf(buf, sizeof(buf), "%s:%d-%zu", shards[i].ip,
                     shards[i].port, v);
            ring.vnodes[vnode_count].hash     = murmur3(buf, 0);
            ring.vnodes[vnode_count].shard_id = i;
            ++vnode_count;
        }
    }

    qsort(ring.vnodes, vnode_count, sizeof(vnode_t), vnodes_cmp);
}

int cluster_node_from_string(const char *str, cluster_node_t *node)
{
    if (!str || !node)
        return -1;

    char buf[MAX_VALUE_SIZE] = {0};
    strncpy(buf, str, MAX_VALUE_SIZE);
    char *ptr   = (char *)buf;
    char *token = strtok(ptr, ":");
    strncpy(node->ip, token, IP_LENGTH);
    token           = strtok(NULL, "\0");
    node->port      = atoi(token);
    node->connected = 0;

    return 0;
}

struct replica_opts {
    struct sockaddr_in saddr;
    char store[BUFSIZ];
};

static pthread_t replica_thread;
static struct replica_opts opts = {0};

static void *replica_start(void *arg)
{
    raft_server_start(&opts.saddr, opts.store);
    return NULL;
}

void cluster_start(const cluster_node_t nodes[], size_t num_nodes,
                   const cluster_node_t replicas[], size_t num_replicas, int id,
                   const char *store, node_type_t type)
{
    cluster.node_id                 = id;

    cluster_encoding_t *encoding    = calloc(1, sizeof(*encoding));
    encoding->cluster_message_read  = cluster_bin_message_read;
    encoding->cluster_message_write = cluster_bin_message_write;

    cluster_transport_t *transport  = calloc(1, sizeof(*transport));
    transport->connect              = tcp_connect;
    transport->close                = tcp_close;
    transport->send_data            = tcp_send;

    cluster.encoding                = encoding;
    cluster.transport               = transport;

    for (size_t i = 0; i < num_nodes; ++i) {
        strncpy(cluster.nodes[i].ip, nodes[i].ip, IP_LENGTH);
        cluster.nodes[i].port = nodes[i].port;
    }

    for (size_t i = 0; i < num_replicas; ++i)
        raft_register_node(replicas[i].ip, replicas[i].port);

    hashring_init(nodes, num_nodes);

    opts.saddr.sin_family = AF_INET;
    strncpy(opts.store, store, BUFSIZ);

    const cluster_node_t *replica_node =
        type == NT_SHARD ? &nodes[id] : &replicas[id];

    // Start raft replicas
    opts.saddr.sin_port = htons(replica_node->port);

    if (inet_pton(AF_INET, replica_node->ip, &opts.saddr.sin_addr) <= 0) {
        perror("Invalid peer IP address");
        return;
    }

    pthread_create(&replica_thread, NULL, &replica_start, NULL);
    cluster.is_running = true;
}

void cluster_stop(void)
{
    for (size_t i = 0; i < cluster.num_nodes; ++i)
        cluster.transport->close(&cluster.nodes[i]);

    pthread_join(replica_thread, NULL);
    cluster.is_running = false;

    free(cluster.encoding);
    free(cluster.transport);
}

ssize_t cluster_message_read(const uint8_t *buf, cluster_message_t *cm)
{
    return cluster.encoding->cluster_message_read(buf, cm);
}

int cluster_submit(const cluster_message_t *message)
{
    cluster_node_t *node = hashring_lookup(message->key);
    cluster_node_t *this = &cluster.nodes[cluster.node_id];
    if (strncmp(node->ip, this->ip, IP_LENGTH) == 0 &&
        node->port == this->port) {
        log_info("Submitting in the current node");
        raft_submit(read_i32(message->payload.data));
        // TODO remove/arena
        free(message->payload.data);
        return 0;
    }

    log_info("Redirecting entry to shard %s:%d", node->ip, node->port);

    if (!node->connected &&
        (node->sock_fd = cluster.transport->connect(node, 0))) {
        node->connected = 1;
        log_info("Connected to the target node");
    }

    return cluster.transport->send_data(node, message);
}
