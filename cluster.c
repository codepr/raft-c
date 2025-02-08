#include "cluster.h"
#include "raft.h"
#include <arpa/inet.h>
#include <fcntl.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>

#define MAX_SHARDS 128

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
    return send(node->sock_fd, message->payload.data, message->payload.size, 0);
}

static void tcp_close(cluster_node_t *node)
{
    if (node->sock_fd < 0)
        return;
    close(node->sock_fd);
    node->sock_fd = -1;
}

typedef struct {
    uint32_t hash;
    int shard_id;
} vnode_t;

typedef struct {
    size_t num_shards;
    vnode_t vnodes[MAX_SHARDS * 10];
    cluster_node_t nodes[MAX_SHARDS];
} hashring_t;

// Global hashring, initializing to 0 (zero-initialization).
static hashring_t ring = {0};

// Basic Murmur3 hash function for simplicity
static uint32_t murmur3(const char *key, uint32_t seed)
{
    uint32_t h = seed;
    while (*key) {
        h ^= *key++;
        h *= 0x5bd1e995;
        h ^= h >> 15;
    }
    return h;
}

static cluster_node_t *hashring_lookup(const char *key)
{
    uint32_t hash = murmur3(key, 0);

    for (size_t i = 0; i < ring.num_shards * 10; ++i) {
        if (ring.vnodes[i].hash >= hash)
            return &ring.nodes[ring.vnodes[i].shard_id];
    }

    return &ring.nodes[ring.vnodes[0].shard_id];
}

static int vnodes_cmp(const void *ptr_a, const void *ptr_b)
{
    const vnode_t *a = ptr_a;
    const vnode_t *b = ptr_b;
    return (a->hash < b->hash) - (a->hash > b->hash);
}

void hashring_init(const cluster_node_t *shards, size_t num_shards)
{
    ring.num_shards         = num_shards;

    size_t vnodes_per_shard = 10;
    size_t vnode_count      = 0;

    for (size_t i = 0; i < num_shards; ++i) {
        for (size_t v = 0; v < vnodes_per_shard; ++v) {
            char buf[64] = {0};
            snprintf(buf, sizeof(buf), "%s:%d-%zu", shards[i].ip,
                     shards[i].port, v);

            ring.vnodes[vnode_count].hash     = murmur3(buf, 0);
            ring.vnodes[vnode_count].shard_id = i;
            ++vnode_count;

            ring.nodes[i] = shards[i];
        }
    }

    qsort(ring.vnodes, vnode_count, sizeof(vnode_t), vnodes_cmp);
}

int cluster_node_from_string(const char *str, cluster_node_t *node)
{
    if (!str || !node)
        return -1;

    char *ptr   = (char *)str;
    char *token = strtok(ptr, ":");
    strncpy(node->ip, token, IP_LENGTH);
    token      = strtok(NULL, "\0");
    node->port = atoi(token);

    return 0;
}

static pthread_t raft_thread;
static struct sockaddr_in raft_addr = {0};

static void *raft_start(void *arg)
{
    const struct sockaddr_in *peer = arg;
    raft_server_start(peer);
    return NULL;
}

void cluster_start(const cluster_node_t nodes[], size_t num_nodes,
                   const cluster_node_t replicas[], size_t num_replicas, int id,
                   node_type_t node_type)
{
    if (node_type == NODE_MAIN) {
        cluster.node_id   = id;
        cluster.transport = (cluster_transport_t){
            .connect = tcp_connect, .send_data = tcp_send, .close = tcp_close};

        for (size_t i = 0; i < num_nodes; ++i) {
            strncpy(cluster.nodes[i].ip, nodes[i].ip, IP_LENGTH);
            cluster.nodes[i].port = nodes[i].port;
        }

        hashring_init(nodes, num_nodes);
    }

    // Start raft replicas
    raft_addr.sin_family = AF_INET;
    raft_addr.sin_port   = htons(replicas[0].port);

    if (inet_pton(AF_INET, replicas[0].ip, &raft_addr.sin_addr) <= 0) {
        perror("Invalid peer IP address");
        return;
    }

    for (size_t i = 0; i < num_replicas; ++i)
        raft_register_node(replicas[i].ip, replicas[i].port);

    pthread_create(&raft_thread, NULL, &raft_start, &raft_addr);
    cluster.is_running = true;
}

void cluster_stop(void)
{
    for (size_t i = 0; i < cluster.num_nodes; ++i)
        cluster.transport.close(&cluster.nodes[i]);
    pthread_join(raft_thread, NULL);
    cluster.is_running = false;
}

int cluster_submit(const char *key, const cluster_message_t *message)
{
    cluster_node_t *node = hashring_lookup(key);
    cluster_node_t *this = &cluster.nodes[cluster.node_id];
    if (strncmp(node->ip, this->ip, IP_LENGTH) == 0 &&
        node->port == this->port) {
        raft_submit(*(int *)message->payload.data);
        return 0;
    }

    return cluster.transport.send_data(node, message);
}
