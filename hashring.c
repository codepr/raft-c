#include "hashring.h"

#include <arpa/inet.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// Global hashring, initializing to 0 (zero-initialization).
static hashring_t ring = {0};

// Global UDP socket
static int sock_fd     = -1;

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

ssize_t udp_send(const raft_node_t *dest, const void *data, size_t size)
{
    struct sockaddr_in shard_addr = {0};
    shard_addr.sin_family         = AF_INET;
    shard_addr.sin_port           = htons(dest->port);

    if (inet_pton(AF_INET, dest->ip_addr, &shard_addr.sin_addr) <= 0) {
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

    return sendto(sock_fd, data, size, 0, (struct sockaddr *)&shard_addr,
                  sizeof(shard_addr));
}

void udp_close(void)
{
    if (sock_fd < 0)
        return;
    close(sock_fd);
}

static int hashring_lookup(const char *key)
{
    uint32_t hash = murmur3(key, 0);

    for (size_t i = 0; i < ring.num_shards * 10; ++i) {
        if (ring.vnodes[i].hash >= hash)
            return ring.vnodes[i].shard_id;
    }

    return ring.vnodes[0].shard_id;
}

static int vnodes_cmp(const void *ptr_a, const void *ptr_b)
{
    const vnode_t *a = ptr_a;
    const vnode_t *b = ptr_b;
    return (a->hash < b->hash) - (a->hash > b->hash);
}

void hashring_init(raft_node_t *shards, size_t num_shards)
{
    ring.num_shards = num_shards;
    ring.transport =
        (hashring_transport_t){.send_data = udp_send, .close = udp_close};

    size_t vnodes_per_shard = 10;
    size_t vnode_count      = 0;

    for (size_t i = 0; i < num_shards; ++i) {
        for (size_t v = 0; v < vnodes_per_shard; ++v) {
            char buf[64] = {0};
            snprintf(buf, sizeof(buf), "%s:%d-%zu", shards[i].ip_addr,
                     shards[i].port, v);

            ring.vnodes[vnode_count].hash     = murmur3(buf, 0);
            ring.vnodes[vnode_count].shard_id = i;
            ++vnode_count;

            ring.nodes[i] = shards[i];
        }
    }

    qsort(ring.vnodes, vnode_count, sizeof(vnode_t), vnodes_cmp);
}

void hashring_deinit(void) { ring.transport.close(); }

int hashring_submit(const char *key, const hashring_payload_t *payload)
{
    uint32_t shard_id      = hashring_lookup(key);
    raft_node_t *raft_node = &ring.nodes[shard_id];

    return ring.transport.send_data(raft_node, payload->data, payload->size);
}
