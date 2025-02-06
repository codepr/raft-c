#ifndef CLUSTER_H
#define CLUSTER_H

#include <stdio.h>
#include <stdlib.h>

#define IP_LENGTH         16
#define MAX_CLUSTER_NODES 64

typedef struct {
    char ip[IP_LENGTH];
    int port;
} cluster_node_t;

typedef struct {
    void *data;
    size_t size;
} cluster_payload_t;

typedef struct {
    ssize_t (*send_data)(const cluster_node_t *dest,
                         const cluster_payload_t *payload);
    void (*close)(void);
} cluster_transport_t;

typedef struct {
    int node_id;
    size_t num_nodes;
    cluster_node_t nodes[MAX_CLUSTER_NODES];
    cluster_transport_t transport;
} cluster_t;

int cluster_node_from_string(const char *str, cluster_node_t *node);
void cluster_init(const cluster_node_t nodes[], size_t length, int node_id);
void cluster_deinit(void);
int cluster_submit(const char *key, const cluster_payload_t *payload);

#endif
