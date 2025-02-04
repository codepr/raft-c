#ifndef HASHRING_H
#define HASHRING_H

#include "raft.h"
#include <stdint.h>
#include <stdlib.h>

#define MAX_SHARDS 128

typedef struct {
    ssize_t (*send_data)(const raft_node_t *dest, const void *data,
                         size_t size);
    void (*close)(void);
} hashring_transport_t;

typedef struct {
    uint32_t hash;
    int shard_id;
} vnode_t;

typedef struct {
    size_t num_shards;
    vnode_t vnodes[MAX_SHARDS * 10];
    raft_node_t nodes[MAX_SHARDS];
    hashring_transport_t transport;
} hashring_t;

typedef struct {
    void *data;
    size_t size;
} hashring_payload_t;

void hashring_init(raft_node_t *shards, size_t num_shards);
void hashring_deinit(void);
int hashring_submit(const char *key, const hashring_payload_t *payload);

#endif
