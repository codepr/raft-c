#include "encoding.h"
#include "binary.h"
#include "darray.h"
#include <string.h>

int request_vote_rpc_write(uint8_t *buf, const request_vote_rpc_t *rv)
{
    // term
    int bytes = write_i32(buf, rv->term);
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

int request_vote_reply_write(uint8_t *buf, const request_vote_reply_t *rv)
{
    // term
    int bytes = write_i32(buf, rv->term);
    buf += sizeof(int32_t);

    bytes += write_u8(buf++, rv->vote_granted);

    return bytes;
}

int request_vote_rpc_read(request_vote_rpc_t *rv, const uint8_t *buf)
{
    rv->term = read_i32(buf);
    buf += sizeof(int32_t);

    rv->candidate_id = read_i32(buf);
    buf += sizeof(int32_t);

    rv->last_log_term = read_i32(buf);
    buf += sizeof(int32_t);

    rv->last_log_index = read_i32(buf);
    buf += sizeof(int32_t);

    return 0;
}

int request_vote_reply_read(request_vote_reply_t *rv, const uint8_t *buf)
{
    rv->term = read_i32(buf);
    buf += sizeof(int32_t);
    rv->vote_granted = read_u8(buf++);
    return 0;
}

int append_entries_rpc_write(uint8_t *buf, const append_entries_rpc_t *ae)
{
    // term
    int bytes = write_i32(buf, ae->term);
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

int append_entries_reply_write(uint8_t *buf, const append_entries_reply_t *ae)
{
    // term
    int bytes = write_i32(buf, ae->term);
    buf += sizeof(int32_t);

    bytes += write_u8(buf++, ae->success);

    return bytes;
}
int add_node_rpc_write(uint8_t *buf, const raft_add_node_t *ga)
{
    uint8_t ip_addr_len = strlen(ga->ip_addr);
    int bytes           = write_u8(buf++, ip_addr_len);
    memcpy(buf, ga->ip_addr, ip_addr_len);
    bytes += ip_addr_len;
    buf += ip_addr_len;
    bytes += write_i32(buf, ga->port);
    return bytes;
}

int add_node_rpc_read(raft_add_node_t *ga, const uint8_t *buf)
{
    size_t ip_addr_len = read_u8(buf++);
    if (ip_addr_len > IP_LENGTH)
        return -1;
    strncpy(ga->ip_addr, (char *)buf, ip_addr_len);
    buf += ip_addr_len;
    ga->port = read_i32(buf);
    return 0;
}

int append_entries_rpc_read(append_entries_rpc_t *ae, const uint8_t *buf)
{
    ae->term = read_i32(buf);
    buf += sizeof(int32_t);

    ae->leader_id = read_i32(buf);
    buf += sizeof(int32_t);

    ae->prev_log_term = read_i32(buf);
    buf += sizeof(int32_t);

    ae->prev_log_index = read_i32(buf);
    buf += sizeof(int32_t);

    ae->leader_commit = read_i32(buf);
    buf += sizeof(int32_t);

    uint32_t entries_count = read_u32(buf);
    buf += sizeof(uint32_t);

    for (int i = 0; i < entries_count; ++i) {
        log_entry_t entry;
        entry.term = read_i32(buf);
        buf += sizeof(int32_t);
        entry.value = read_i32(buf);
        buf += sizeof(int32_t);
        da_append(&ae->entries, entry);
    }

    return 0;
}

int append_entries_reply_read(append_entries_reply_t *ae, const uint8_t *buf)
{
    ae->term = read_i32(buf);
    buf += sizeof(int32_t);
    ae->success = read_u8(buf++);
    return 0;
}
