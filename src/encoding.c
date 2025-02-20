#include "encoding.h"
#include "binary.h"
#include "cluster.h"
#include "darray.h"
#include "raft.h"
#include <string.h>

static ssize_t encode_string(uint8_t *dst, const char *src, size_t length)
{
    size_t i = 0, j = 0;

    // Payload length just after the $ indicator
    size_t n = snprintf((char *)dst, 21, "%lu", length);
    i += n;

    // CRLF
    dst[i++] = '\r';
    dst[i++] = '\n';

    // The query content
    while (length-- > 0)
        dst[i++] = src[j++];

    // CRLF
    dst[i++] = '\r';
    dst[i++] = '\n';

    return i;
}

ssize_t encode_request(const request_t *r, uint8_t *dst)
{
    dst[0] = '$';
    return 1 + encode_string(dst + 1, r->query, r->length);
}

ssize_t decode_request(const uint8_t *data, request_t *dst)
{
    if (data[0] != '$')
        return -1;

    size_t i           = 0;
    const uint8_t *ptr = &data[1];

    // Read length
    while (*ptr != '\r' && *(ptr + 1) != '\n') {
        dst->length *= 10;
        dst->length += *ptr - '0';
        ptr++;
    }

    // Jump over \r\n
    ptr += 2;

    // Read query string
    while (*ptr != '\r' && *(ptr + 1) != '\n')
        dst->query[i++] = *ptr++;

    return dst->length;
}

ssize_t encode_response(const response_t *r, uint8_t *dst)
{
    if (r->type == STRING_RSP) {
        // String response
        dst[0] = r->string_response.rc == 0 ? '$' : '!';
        return 1 + encode_string(dst + 1, r->string_response.message,
                                 r->string_response.length);
    }
    // Array response
    dst[0]    = '#';
    ssize_t i = 1;
    size_t j  = 0;

    // Array length
    size_t n  = snprintf((char *)dst + i, 20, "%lu", r->array_response.length);
    i += n;

    // CRLF
    dst[i++] = '\r';
    dst[i++] = '\n';

    // Records
    while (j < r->array_response.length) {
        // Timestamp
        dst[i++] = ':';
        n        = snprintf((char *)dst + i, 21, "%" PRIu64,
                            r->array_response.records[j].timestamp);
        i += n;
        dst[i++] = '\r';
        dst[i++] = '\n';
        // Value
        dst[i++] = ';';
        n        = snprintf((char *)dst + i, 21, "%lf",
                            r->array_response.records[j].value);
        i += n;
        dst[i++] = '\r';
        dst[i++] = '\n';
        j++;
    }

    return i;
}

static ssize_t decode_string(const uint8_t *ptr, response_t *dst)
{
    size_t i = 0, n = 1;

    // For simplicty, assume the only error code is 1 for now, it's not used ATM
    dst->string_response.rc = *ptr == '!' ? 1 : 0;
    ptr++;

    // Read length
    while (*ptr != '\r' && *(ptr + 1) != '\n' && n++) {
        dst->string_response.length *= 10;
        dst->string_response.length += *ptr - '0';
        ptr++;
    }

    // Move forward after CRLF
    ptr += 2;
    n += 2;

    while (*ptr != '\r' && *(ptr + 1) != '\n')
        dst->string_response.message[i++] = *ptr++;

    return i + n;
}

ssize_t decode_response(const uint8_t *data, response_t *dst)
{
    uint8_t byte   = *data;
    ssize_t length = 0;

    dst->type      = byte == '#' ? ARRAY_RSP : STRING_RSP;

    switch (byte) {
    case '$':
    case '!':
        // Treat error and common strings the same for now
        length = decode_string(data, dst);
        break;
    case '#':
        data++;
        length++;
        // Read length
        dst->array_response.length = 0;
        while (*data != '\r' && *(data + 1) != '\n' && length++) {
            dst->array_response.length *= 10;
            dst->array_response.length += *data - '0';
            data++;
        }

        // Jump over \r\n
        data += 2;
        length += 2;

        // Read records
        size_t j             = 0;
        size_t total_records = dst->array_response.length;
        uint8_t buf[32];
        size_t k = 0;
        // TODO arena malloc here
        dst->array_response.records =
            malloc(total_records * sizeof(*dst->array_response.records));
        while (total_records-- > 0) {
            // Timestamp
            if (*data++ != ':')
                goto cleanup;

            while (*data != '\r' && *(data + 1) != '\n' && length++)
                buf[k++] = *data++;

            dst->array_response.records[j].timestamp = atoll((const char *)buf);
            memset(buf, 0x00, sizeof(buf));
            k = 0;

            // Skip CRLF + ;
            data += 3;
            length += 3;

            // Value
            while (*data != '\r' && *(data + 1) != '\n' && length++)
                buf[k++] = *data++;

            buf[k]                               = '\0';

            dst->array_response.records[j].value = strtold((char *)buf, NULL);

            // Skip CRLF
            data += 2;
            length += 2;
            j++;
        }
        break;
    default:
        break;
    }

    return length;

cleanup:
    free(dst->array_response.records);
    return -1;
}

void free_response(response_t *rs)
{
    if (rs->type == ARRAY_RSP)
        free(rs->array_response.records);
}

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
