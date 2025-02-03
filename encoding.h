#ifndef ENCODING_H
#define ENCODING_H

#include "raft.h"
#include <stdint.h>

int add_node_rpc_write(uint8_t *buf, const raft_add_node_t *ga);
int add_node_rpc_read(raft_add_node_t *ga, const uint8_t *buf);
int request_vote_rpc_write(uint8_t *buf, const request_vote_rpc_t *rv);
int request_vote_reply_write(uint8_t *buf, const request_vote_reply_t *rv);
int request_vote_rpc_read(request_vote_rpc_t *rv, const uint8_t *buf);
int request_vote_reply_read(request_vote_reply_t *rv, const uint8_t *buf);
int append_entries_rpc_write(uint8_t *buf, const append_entries_rpc_t *ae);
int append_entries_reply_write(uint8_t *buf, const append_entries_reply_t *ae);
int append_entries_rpc_read(append_entries_rpc_t *ae, const uint8_t *buf);
int append_entries_reply_read(append_entries_reply_t *ae, const uint8_t *buf);

#endif
