#ifndef ENCODING_H
#define ENCODING_H

#include "raft.h"
#include <stdint.h>

ssize_t raft_bin_message_write(uint8_t *buf, const raft_message_t *rm);
message_type_t raft_bin_message_read(const uint8_t *buf, raft_message_t *rm);

#endif
