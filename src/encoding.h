#ifndef ENCODING_H
#define ENCODING_H

#include <stdint.h>
#include <sys/types.h>

// RAFT message encoding
typedef struct raft_message raft_message_t;

ssize_t raft_bin_message_write(uint8_t *buf, const raft_message_t *rm);
ssize_t raft_bin_message_read(const uint8_t *buf, raft_message_t *rm);

// Cluster message encoding
typedef struct cluster_message cluster_message_t;

ssize_t cluster_bin_message_write(uint8_t *buf, const cluster_message_t *cm);
ssize_t cluster_bin_message_read(const uint8_t *buf, cluster_message_t *cm);

// CDFS chunks encoding
typedef struct cdfs_message cdfs_message_t;

ssize_t cdfs_bin_chunk_write(uint8_t *buf, const cdfs_message_t *cm);
ssize_t cdfs_bin_chunk_read(const uint8_t *buf, cdfs_message_t *cm);

#endif
