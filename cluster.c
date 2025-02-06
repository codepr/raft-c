#include "cluster.h"
#include "raft.h"
#include <arpa/inet.h>
#include <string.h>
#include <unistd.h>

#define MAX_SHARDS 128

// Global hashring, initializing to 0 (zero-initialization).
static cluster_t cluster = {0};

// Global UDP socket
static int sock_fd       = -1;

static ssize_t udp_send(const cluster_node_t *dest,
                        const cluster_payload_t *payload)
{
    struct sockaddr_in shard_addr = {0};
    shard_addr.sin_family         = AF_INET;
    shard_addr.sin_port           = htons(dest->port);

    if (inet_pton(AF_INET, dest->ip, &shard_addr.sin_addr) <= 0) {
        perror("Invalid peer IP address");
        return -1;
    }

    if (sock_fd == -1) {
        sock_fd = socket(AF_INET, SOCK_DGRAM, 0);
        if (sock_fd < 0) {
            perror("Failed to create UDP socket");
            return -1;
        }
    }

    return sendto(sock_fd, payload->data, payload->size, 0,
                  (struct sockaddr *)&shard_addr, sizeof(shard_addr));
}

static void udp_close(void)
{
    if (sock_fd < 0)
        return;
    close(sock_fd);
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

void cluster_init(const cluster_node_t nodes[], size_t length, int node_id)
{
    cluster.node_id = node_id;
    cluster.transport =
        (cluster_transport_t){.send_data = udp_send, .close = udp_close};

    for (size_t i = 0; i < length; ++i) {
        strncpy(cluster.nodes[i].ip, nodes[i].ip, IP_LENGTH);
        cluster.nodes[i].port = nodes[i].port;
        raft_register_node(nodes[i].ip, nodes[i].port);
    }

    hashring_init(nodes, length);
}

void cluster_deinit(void) { cluster.transport.close(); }

int cluster_submit(const char *key, const cluster_payload_t *payload)
{
    cluster_node_t *node = hashring_lookup(key);
    cluster_node_t *this = &cluster.nodes[cluster.node_id];
    if (strncmp(node->ip, this->ip, IP_LENGTH) == 0 &&
        node->port == this->port) {
        raft_submit(*(int *)payload->data);
        return 0;
    }

    return cluster.transport.send_data(node, payload);
}
