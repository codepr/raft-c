#ifndef CLUSTER_H
#define CLUSTER_H

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

#define IP_LENGTH         16
#define MAX_CLUSTER_NODES 64

typedef enum { NODE_MAIN, NODE_REPLICA } node_type_t;

typedef struct {
    char ip[IP_LENGTH];
    int port;
    int sock_fd;
} cluster_node_t;

typedef struct {
    void *data;
    size_t size;
} cluster_payload_t;

typedef enum { CM_CLUSTER_JOIN, CM_CLUSTER_DATA } cm_type_t;

typedef struct {
    cm_type_t type;
    cluster_payload_t payload;
} cluster_message_t;

typedef struct {
    int (*connect)(const cluster_node_t *node, int nonblocking);
    ssize_t (*send_data)(const cluster_node_t *node,
                         const cluster_message_t *message);
    void (*close)(cluster_node_t *node);
} cluster_transport_t;

typedef struct {
    int node_id;
    bool is_running;
    size_t num_nodes;
    cluster_node_t nodes[MAX_CLUSTER_NODES];
    cluster_transport_t transport;
} cluster_t;

int cluster_node_from_string(const char *str, cluster_node_t *node);
void cluster_start(const cluster_node_t nodes[], size_t num_nodes,
                   const cluster_node_t replicas[], size_t num_replicas, int id,
                   const char *store, node_type_t node_type);
void cluster_stop(void);
int cluster_submit(const char *key, const cluster_message_t *message);

#endif
