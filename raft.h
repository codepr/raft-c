#ifndef RAFT_H
#define RAFT_H

#include <netdb.h>
#include <stdlib.h>

#define IP_LENGTH 16

typedef struct {
    char ip_addr[IP_LENGTH];
    int port;
} raft_node_t;

int raft_register_node(const char *addr, int port);
int raft_submit(int value);
void raft_seed_nodes(const raft_node_t nodes[], size_t length);
void raft_server_start(const struct sockaddr_in *peer);

typedef struct raft_state raft_state_t;

typedef struct raft_persistence {
    int (*save_state)(const raft_state_t *state);
    int (*load_state)(raft_state_t *state);
} raft_persistence_t;

void raft_set_persistence(raft_persistence_t *backend);
int raft_save_state(const raft_persistence_t *backend);
int raft_load_state(raft_persistence_t *backend);

#endif
