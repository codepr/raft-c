#ifndef CLUSTER_H
#define CLUSTER_H

#include "config.h"
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

#define IP_LENGTH          16
#define MAX_CLUSTER_NODES  64
#define CLUSTER_MAGIC_BYTE 0xA1B2 // Unique identifier for cluster messages

typedef struct {
    char ip[IP_LENGTH];
    int port;
    int sock_fd;
    int connected;
} cluster_node_t;

typedef struct {
    void *data;
    size_t size;
} cluster_payload_t;

typedef enum { CM_CLUSTER_JOIN, CM_CLUSTER_DATA } cm_type_t;

typedef struct cluster_message {
    cm_type_t type;
    char key[MAX_KEY_SIZE];
    cluster_payload_t payload;
} cluster_message_t;

/**
 ** Cluster message encoding interface, allows for multiple serialization
 ** types, by default it uses the binary defined in encoding.h
 **/
typedef struct cluster_encoding {
    ssize_t (*cluster_message_write)(uint8_t *buf, const cluster_message_t *cm);
    ssize_t (*cluster_message_read)(const uint8_t *buf, cluster_message_t *cm);
} cluster_encoding_t;

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
    cluster_encoding_t *encoding;
    cluster_transport_t *transport;
} cluster_t;

int cluster_node_from_string(const char *str, cluster_node_t *node);
void cluster_start(const cluster_node_t nodes[], size_t num_nodes,
                   const cluster_node_t replicas[], size_t num_replicas, int id,
                   const char *store, node_type_t type);
void cluster_stop(void);
ssize_t cluster_message_read(const uint8_t *buf, cluster_message_t *cm);
int cluster_submit(const cluster_message_t *message);

#endif
