#ifndef RAFT_H
#define RAFT_H

#include <stdlib.h>

#define IP_LENGTH 16

typedef struct {
    char ip_addr[IP_LENGTH];
    int port;
} raft_node_t;

int raft_register_node(const char *addr, int port);
int raft_submit(int value);
void raft_seed_nodes(const raft_node_t nodes[], size_t length);
void raft_server_start(int node_id);

#endif
