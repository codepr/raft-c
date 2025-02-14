#ifndef ENCODING_H
#define ENCODING_H

#include "cluster.h"
#include "raft.h"
#include <stdint.h>

// RAFT message encoding
ssize_t raft_bin_message_write(uint8_t *buf, const raft_message_t *rm);
message_type_t raft_bin_message_read(const uint8_t *buf, raft_message_t *rm);

// Cluster message encoding
ssize_t cluster_bin_message_write(uint8_t *buf, const cluster_message_t *cm);
cm_type_t cluster_bin_message_read(const uint8_t *buf, cluster_message_t *cm);
#endif
