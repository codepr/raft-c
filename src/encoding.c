#include "encoding.h"
#include "binary.h"
#include "cdfs_node.h"
#include "cluster.h"
#include "darray.h"
#include "raft.h"
#include <string.h>

static ssize_t request_vote_rpc_write(uint8_t *buf,
                                      const request_vote_rpc_t *rv)
{
    // term
    ssize_t bytes = write_i32(buf, rv->term);
    buf += sizeof(int32_t);

    // candidate_id
    bytes += write_i32(buf, rv->candidate_id);
    buf += sizeof(int32_t);

    // last_log_term
    bytes += write_i32(buf, rv->last_log_term);
    buf += sizeof(int32_t);

    // last_log_index
    bytes += write_i32(buf, rv->last_log_index);
    buf += sizeof(int32_t);

    return bytes;
}

static ssize_t request_vote_reply_write(uint8_t *buf,
                                        const request_vote_reply_t *rv)
{
    // term
    ssize_t bytes = write_i32(buf, rv->term);
    buf += sizeof(int32_t);

    bytes += write_u8(buf++, rv->vote_granted);

    return bytes;
}

static ssize_t request_vote_rpc_read(request_vote_rpc_t *rv, const uint8_t *buf)
{
    ssize_t bytes = 0;
    rv->term      = read_i32(buf);
    buf += sizeof(int32_t);
    bytes += sizeof(int32_t);

    rv->candidate_id = read_i32(buf);
    buf += sizeof(int32_t);
    bytes += sizeof(int32_t);

    rv->last_log_term = read_i32(buf);
    buf += sizeof(int32_t);
    bytes += sizeof(int32_t);

    rv->last_log_index = read_i32(buf);
    buf += sizeof(int32_t);
    bytes += sizeof(int32_t);

    return bytes;
}

static ssize_t request_vote_reply_read(request_vote_reply_t *rv,
                                       const uint8_t *buf)
{
    ssize_t bytes = 0;
    rv->term      = read_i32(buf);
    buf += sizeof(int32_t);
    bytes += sizeof(int32_t);
    rv->vote_granted = read_u8(buf++);
    bytes++;
    return bytes;
}

static ssize_t append_entries_rpc_write(uint8_t *buf,
                                        const append_entries_rpc_t *ae)
{
    // term
    ssize_t bytes = write_i32(buf, ae->term);
    buf += sizeof(int32_t);

    // leader_id
    bytes += write_i32(buf, ae->leader_id);
    buf += sizeof(int32_t);

    // prev_log_term
    bytes += write_i32(buf, ae->prev_log_term);
    buf += sizeof(int32_t);

    // prev_log_index
    bytes += write_i32(buf, ae->prev_log_index);
    buf += sizeof(int32_t);

    // leader_commit
    bytes += write_i32(buf, ae->leader_commit);
    buf += sizeof(int32_t);

    if (ae->entries.capacity) {
        // entries count
        bytes += write_u32(buf, ae->entries.length);
        buf += sizeof(uint32_t);

        // entries
        for (size_t i = 0; i < ae->entries.length; ++i) {
            bytes += write_i32(buf, ae->entries.items[i].term);
            buf += sizeof(int32_t);
            bytes += write_i32(buf, ae->entries.items[i].value);
            buf += sizeof(int32_t);
        }
    }

    return bytes;
}

static ssize_t append_entries_reply_write(uint8_t *buf,
                                          const append_entries_reply_t *ae)
{
    // term
    ssize_t bytes = write_i32(buf, ae->term);
    buf += sizeof(int32_t);

    bytes += write_u8(buf++, ae->success);

    return bytes;
}
static ssize_t add_node_rpc_write(uint8_t *buf, const add_node_rpc_t *ga)
{
    uint8_t ip_addr_len = strlen(ga->ip_addr);
    ssize_t bytes       = write_u8(buf++, ip_addr_len);
    memcpy(buf, ga->ip_addr, ip_addr_len);
    bytes += ip_addr_len;
    buf += ip_addr_len;
    bytes += write_i32(buf, ga->port);
    return bytes;
}

static ssize_t add_node_rpc_read(add_node_rpc_t *ga, const uint8_t *buf)
{
    ssize_t bytes      = 0;
    size_t ip_addr_len = read_u8(buf++);
    bytes++;
    if (ip_addr_len > IP_LENGTH)
        return -1;
    strncpy(ga->ip_addr, (char *)buf, ip_addr_len);
    buf += ip_addr_len;
    bytes += ip_addr_len;
    ga->port = read_i32(buf);
    bytes += sizeof(int32_t);
    return bytes;
}

static ssize_t append_entries_rpc_read(append_entries_rpc_t *ae,
                                       const uint8_t *buf)
{
    ssize_t bytes = 0;
    ae->term      = read_i32(buf);
    buf += sizeof(int32_t);
    bytes += sizeof(int32_t);

    ae->leader_id = read_i32(buf);
    buf += sizeof(int32_t);
    bytes += sizeof(int32_t);

    ae->prev_log_term = read_i32(buf);
    buf += sizeof(int32_t);
    bytes += sizeof(int32_t);

    ae->prev_log_index = read_i32(buf);
    buf += sizeof(int32_t);
    bytes += sizeof(int32_t);

    ae->leader_commit = read_i32(buf);
    buf += sizeof(int32_t);
    bytes += sizeof(int32_t);

    uint32_t entries_count = read_u32(buf);
    buf += sizeof(uint32_t);
    bytes += sizeof(int32_t);

    for (int i = 0; i < entries_count; ++i) {
        log_entry_t entry;
        entry.term = read_i32(buf);
        buf += sizeof(int32_t);
        bytes += sizeof(int32_t);
        entry.value = read_i32(buf);
        buf += sizeof(int32_t);
        bytes += sizeof(int32_t);
        da_append(&ae->entries, entry);
    }

    return bytes;
}

static ssize_t append_entries_reply_read(append_entries_reply_t *ae,
                                         const uint8_t *buf)
{
    ssize_t bytes = 0;
    ae->term      = read_i32(buf);
    buf += sizeof(int32_t);
    bytes += sizeof(int32_t);
    ae->success = read_u8(buf++);
    bytes++;
    return bytes;
}

static ssize_t forward_value_rpc_write(uint8_t *buf,
                                       const forward_value_rpc_t *fv)
{
    ssize_t bytes = write_i32(buf, fv->value);
    buf += sizeof(int32_t);
    return bytes;
}

static ssize_t forward_value_rpc_read(forward_value_rpc_t *fv,
                                      const uint8_t *buf)
{
    ssize_t bytes = 0;
    fv->value     = read_i32(buf);
    bytes += sizeof(int32_t);
    buf += sizeof(int32_t);
    return bytes;
}

ssize_t raft_bin_message_write(uint8_t *buf, const raft_message_t *rm)
{
    ssize_t bytes = write_u8(buf++, rm->type);
    switch (rm->type) {
    case MT_RAFT_CLUSTER_JOIN_RPC:
        bytes += add_node_rpc_write(buf, &rm->add_node_rpc);
        break;
    case MT_RAFT_ADD_PEER_RPC:
        bytes += add_node_rpc_write(buf, &rm->add_node_rpc);
        break;
    case MT_RAFT_FORWARD_VALUE_RPC:
        bytes += forward_value_rpc_write(buf, &rm->forward_value_rpc);
        break;
    case MT_RAFT_APPEND_ENTRIES_RPC:
        bytes += append_entries_rpc_write(buf, &rm->append_entries_rpc);
        break;
    case MT_RAFT_APPEND_ENTRIES_REPLY:
        bytes += append_entries_reply_write(buf, &rm->append_entries_reply);
        break;
    case MT_RAFT_REQUEST_VOTE_RPC:
        bytes += request_vote_rpc_write(buf, &rm->request_vote_rpc);
        break;
    case MT_RAFT_REQUEST_VOTE_REPLY:
        bytes += request_vote_reply_write(buf, &rm->request_vote_reply);
        break;
    }

    return bytes;
}

ssize_t raft_bin_message_read(const uint8_t *buf, raft_message_t *rm)
{
    ssize_t bytes = 0;
    rm->type      = read_u8(buf++);
    bytes++;
    switch (rm->type) {
    case MT_RAFT_CLUSTER_JOIN_RPC:
        bytes += add_node_rpc_read(&rm->add_node_rpc, buf);
        break;
    case MT_RAFT_ADD_PEER_RPC:
        bytes += add_node_rpc_read(&rm->add_node_rpc, buf);
        break;
    case MT_RAFT_FORWARD_VALUE_RPC:
        bytes += forward_value_rpc_read(&rm->forward_value_rpc, buf);
        break;
    case MT_RAFT_APPEND_ENTRIES_RPC:
        bytes += append_entries_rpc_read(&rm->append_entries_rpc, buf);
        break;
    case MT_RAFT_APPEND_ENTRIES_REPLY:
        bytes += append_entries_reply_read(&rm->append_entries_reply, buf);
        break;
    case MT_RAFT_REQUEST_VOTE_RPC:
        bytes += request_vote_rpc_read(&rm->request_vote_rpc, buf);
        break;
    case MT_RAFT_REQUEST_VOTE_REPLY:
        bytes += request_vote_reply_read(&rm->request_vote_reply, buf);
        break;
    }
    return bytes;
}

ssize_t cluster_bin_message_write(uint8_t *buf, const cluster_message_t *cm)
{
    ssize_t bytes = write_u8(buf++, cm->type);
    switch (cm->type) {
    case CM_CLUSTER_JOIN:
        break;
    case CM_CLUSTER_DATA:
        bytes += write_i32(buf, strlen(cm->key));
        buf += sizeof(int32_t);
        memcpy(buf, cm->key, strlen(cm->key));
        buf += strlen(cm->key);
        bytes += strlen(cm->key);
        bytes += write_i32(buf, cm->payload.size);
        buf += sizeof(int32_t);
        memcpy(buf, cm->payload.data, cm->payload.size);
        bytes += cm->payload.size;
        break;
    }
    return bytes;
}

ssize_t cluster_bin_message_read(const uint8_t *buf, cluster_message_t *cm)
{
    ssize_t bytes   = 0;
    // TODO remove / arena
    uint8_t *data   = calloc(1, MAX_VALUE_SIZE);
    int32_t keysize = 0;
    cm->type        = read_u8(buf++);
    bytes++;
    switch (cm->type) {
    case CM_CLUSTER_JOIN:
        break;
    case CM_CLUSTER_DATA:
        keysize = read_i32(buf);
        buf += sizeof(int32_t);
        bytes += sizeof(int32_t);
        memcpy(cm->key, buf, keysize);
        buf += keysize;
        bytes += keysize;
        cm->payload.size = read_i32(buf);
        buf += sizeof(int32_t);
        bytes += sizeof(int32_t);
        memcpy(data, buf, cm->payload.size);
        cm->payload.data = data;
        bytes += cm->payload.size;
        break;
    }
    return bytes;
}

ssize_t cdfs_bin_chunk_write(uint8_t *buf, const cdfs_message_t *cm)
{
    size_t filename_size = 0;
    ssize_t bytes        = write_u8(buf++, cm->type);
    bytes += write_i32(buf, cm->size);
    buf += sizeof(int32_t);
    switch (cm->type) {
    case CMT_PUSH_FILE:
    case CMT_PULL_FILE:
        // Filename size, this because the store message is followed by the
        // stream of bytes representing the content of the transmitted file
        filename_size = strlen(cm->filename);
        bytes += write_u8(buf++, filename_size);
        buf += sizeof(uint8_t);
        memcpy(buf, cm->filename, filename_size);
        bytes += filename_size;
    case CMT_PUSH_CHUNK:
    case CMT_PULL_CHUNK:
        // Filename size, this because the store message is followed by the
        // stream of bytes representing the content of the transmitted file
        filename_size = strlen(cm->chunk.filename);
        bytes += write_u8(buf++, filename_size);
        buf += sizeof(uint8_t);
        memcpy(buf, cm->chunk.filename, filename_size);
        bytes += filename_size;
        buf += filename_size;
        // Hash size, this because the store message is followed by the
        // stream of bytes representing the content of the transmitted file
        bytes += write_u8(buf++, SHA256_SIZE);
        buf += sizeof(uint8_t);
        memcpy(buf, cm->chunk.hash, SHA256_SIZE);
        bytes += SHA256_SIZE;
    }

    return bytes;
}

ssize_t cdfs_bin_chunk_read(const uint8_t *buf, cdfs_message_t *cm)
{
    size_t filename_size = 0;
    size_t hash_size     = 0;
    cm->type             = read_u8(buf++);
    cm->size             = read_i32(buf);
    buf += sizeof(int32_t);
    switch (cm->type) {
    case CMT_PUSH_FILE:
    case CMT_PULL_FILE:
        filename_size = read_i32(buf);
        buf += sizeof(int32_t);
        memcpy(cm->filename, buf, filename_size);
    case CMT_PUSH_CHUNK:
    case CMT_PULL_CHUNK:
        filename_size = read_i32(buf);
        buf += sizeof(int32_t);
        memcpy(cm->chunk.filename, buf, filename_size);
        buf += filename_size;
        hash_size = read_i32(buf);
        buf += sizeof(int32_t);
        memcpy(cm->chunk.hash, buf, hash_size);
    }
    return cm->type;
}
