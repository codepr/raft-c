#include "encoding.h"
#include "binary.h"
#include "cluster.h"
#include "darray.h"
#include "raft.h"
#include <string.h>

#define CRLF            "\r\n"
#define CRLF_LEN        2
#define MAX_NUM_STR_LEN 21 // Large enough for 64-bit integers

// CRLF helpers

static size_t setcrlf(uint8_t *dst, size_t pos)
{
    dst[pos++] = '\r';
    dst[pos++] = '\n';
    return pos;
}

// TODO fix this with a proper iscrlf function accounting for
// errors, bool is not enough
static bool iscrlf(const uint8_t *buf)
{
    if (*buf != '\r')
        return false;

    return (buf[0] == '\r' && buf[1] == '\n');
}

static const uint8_t *skipcrlf(const uint8_t *buf) { return buf + CRLF_LEN; }

static ssize_t encode_string(uint8_t *dst, const char *src, size_t length)
{
    if (!dst || !src || length + MAX_NUM_STR_LEN + 2 * CRLF_LEN > QUERYSIZE)
        return -1;

    size_t pos = 0;

    // Payload length just after the $ indicator
    size_t n   = snprintf((char *)dst, MAX_NUM_STR_LEN, "%lu", length);
    if (n < 0 || n >= MAX_NUM_STR_LEN)
        return -1;

    pos += n;

    // CRLF
    pos = setcrlf(dst, pos);

    // The query content
    memcpy(dst + pos, src, length);
    pos += length;

    // CRLF
    pos = setcrlf(dst, pos);

    return pos;
}

ssize_t encode_request(const request_t *r, uint8_t *dst)
{
    if (!r || !dst)
        return -1;

    dst[0]              = MARKER_STRING_SUCCESS;

    ssize_t string_size = encode_string(dst + 1, r->query, r->length);
    if (string_size < 0)
        return -1;

    return 1 + string_size;
}

ssize_t decode_request(const uint8_t *data, request_t *dst)
{
    if (!data || !dst || data[0] != MARKER_STRING_SUCCESS)
        return -1;

    const uint8_t *ptr  = &data[1];
    size_t total_length = 1;

    dst->length         = 0;

    // Read length
    while (!iscrlf(ptr)) {
        // Validate digit
        if (*ptr < '0' || *ptr > '9')
            return -1;

        dst->length *= 10;
        dst->length += *ptr - '0';
        ptr++;
        total_length++;
    }

    if (dst->length >= QUERYSIZE)
        return -1;

    // Jump over \r\n
    ptr = skipcrlf(ptr);
    total_length += CRLF_LEN;

    size_t i = 0;

    // Read query string
    while (!iscrlf(ptr)) {
        if (i >= dst->length)
            return -1;

        dst->query[i++] = *ptr++;
        total_length++;
    }

    if (i < QUERYSIZE)
        dst->query[i] = '\0';

    if (i != dst->length)
        return -1;

    total_length += CRLF_LEN;

    return total_length;
}

static ssize_t encode_record(const record_t *r, uint8_t *dst, ssize_t offset)
{
    // Check buffer space
    if (offset + 1 + MAX_NUM_STR_LEN + CRLF_LEN >= QUERYSIZE) {
        return -1;
    }

    // Timestamp
    dst[offset++] = MARKER_TIMESTAMP;
    ssize_t n     = snprintf((char *)dst + offset, MAX_NUM_STR_LEN, "%" PRIu64,
                             r->timestamp);
    if (n < 0 || n >= MAX_NUM_STR_LEN)
        return -1;

    offset += n;

    offset = setcrlf(dst, offset);

    // Check buffer space again
    if (offset + 1 + MAX_NUM_STR_LEN + CRLF_LEN >= QUERYSIZE) {
        return -1;
    }

    // Value
    dst[offset++] = MARKER_VALUE;
    n = snprintf((char *)dst + offset, MAX_NUM_STR_LEN, "%lf", r->value);
    if (n < 0 || n >= MAX_NUM_STR_LEN)
        return -1;

    offset += n;

    offset = setcrlf(dst, offset);

    return offset;
}

static ssize_t encode_array_respose(const response_t *r, uint8_t *dst)
{
    // Check unknown response type
    if (r->type != RT_ARRAY)
        return -1;

    // Array response
    dst[0]        = MARKER_ARRAY;
    size_t offset = 1;

    // Array length
    size_t n      = snprintf((char *)dst + offset, MAX_NUM_STR_LEN, "%zu",
                             r->array_response.length);
    if (n < 0 || n >= MAX_NUM_STR_LEN)
        return -1;

    offset += n;

    if (offset + CRLF_LEN >= QUERYSIZE)
        return -1;

    // CRLF
    offset = setcrlf(dst, offset);

    // Records
    for (size_t i = 0; i < r->array_response.length; ++i) {
        offset = encode_record(&r->array_response.items[i], dst, offset);
        // TODO proper error handling
        if (offset < 0)
            return -1;
    }

    return offset;
}

/**
 * Add streaming flag to response
 * Format: "~<length>\r\n<chunk1>\r\n~<length>\r\n<chunk2>\r\n...~0\r\n"
 */
static ssize_t encode_stream_response(const response_t *r, uint8_t *dst)
{
    dst[0]         = MARKER_STREAM;
    ssize_t offset = 1;
    ssize_t n      = snprintf((char *)dst + offset, MAX_NUM_STR_LEN, "%zu",
                              r->stream_response.batch.length);

    if (n < 0 || n >= MAX_NUM_STR_LEN)
        return -1;

    offset += n;
    offset = setcrlf(dst, offset);

    // Copy chunk data
    // Records
    for (size_t i = 0; i < r->stream_response.batch.length; ++i) {
        offset = encode_record(&r->stream_response.batch.items[i], dst, offset);
        // TODO proper error handling
        if (offset < 0)
            return -1;
    }

    offset = setcrlf(dst, offset);

    // Add termination sequence for final chunk
    if (r->stream_response.is_final) {
        dst[offset++] = MARKER_STREAM;
        dst[offset++] = '0';
        offset        = setcrlf(dst, offset);
    }

    return offset;
}

ssize_t encode_response(const response_t *r, uint8_t *dst)
{
    if (!r || !dst)
        return -1;

    ssize_t pos = 0;

    switch (r->type) {
    case RT_STRING:

        // String response
        dst[0]              = r->string_response.rc == 0 ? MARKER_STRING_SUCCESS
                                                         : MARKER_STRING_ERROR;
        ssize_t string_size = encode_string(dst + 1, r->string_response.message,
                                            r->string_response.length);

        if (string_size < 0)
            return -1;

        pos = 1 + string_size;
        break;
    case RT_ARRAY:
        pos = encode_array_respose(r, dst);
        break;
    case RT_STREAM:
        pos = encode_stream_response(r, dst);
        break;
    default:
        pos = -1;
        break;
    }

    return pos;
}

static ssize_t decode_string(const uint8_t *ptr, response_t *dst)
{
    if (!ptr || !dst)
        return -1;

    ssize_t total_length = 0;

    if (*ptr == MARKER_STRING_SUCCESS)
        dst->string_response.rc = 0;
    else if (*ptr == MARKER_STRING_ERROR)
        dst->string_response.rc = 1;
    else
        return -1;

    ptr++;
    total_length++;

    dst->string_response.length = 0;

    while (!iscrlf(ptr)) {
        // Validate digit
        if (*ptr < '0' || *ptr > '9')
            return -1;

        dst->string_response.length *= 10;
        dst->string_response.length += *ptr - '0';
        ptr++;
        total_length++;
    }

    // Check if we have enough space
    if (dst->string_response.length >= QUERYSIZE) {
        return -1;
    }

    ptr = skipcrlf(ptr);
    total_length += CRLF_LEN;

    size_t i = 0;
    while (!iscrlf(ptr)) {
        if (i >= dst->string_response.length)
            return -1; // Mismatch between declared and actual length

        if (i < QUERYSIZE - 1) { // Leave room for null terminator
            dst->string_response.message[i++] = *ptr;
        }
        ptr++;
        total_length++;
    }

    // Null-terminate the message
    if (i < QUERYSIZE) {
        dst->string_response.message[i] = '\0';
    }

    // Check if actual length matches declared length
    if (i != dst->string_response.length) {
        return -1;
    }

    // Account for final CRLF
    total_length += CRLF_LEN;

    return total_length;
}

static const uint8_t *copy_value(const uint8_t *ptr, char dst[MAX_NUM_STR_LEN],
                                 size_t *pos, ssize_t *total_length)
{
    while (!iscrlf(ptr) && *pos < MAX_NUM_STR_LEN - 1) {
        dst[(*pos)++] = *ptr++;
        (*total_length)++;
    }

    if (*pos == 0 || *pos >= MAX_NUM_STR_LEN - 1)
        return NULL;

    dst[*pos] = '\0';

    return ptr;
}

static const uint8_t *decode_record(const uint8_t *ptr, record_t *r,
                                    ssize_t *total_length)
{
    // Read timestamp
    if (*ptr != MARKER_TIMESTAMP)
        return NULL;

    ptr++;
    (*total_length)++;

    // Parse timestamp
    char timestamp_str[MAX_NUM_STR_LEN] = {0};
    size_t ts_pos                       = 0;

    ptr = copy_value(ptr, timestamp_str, &ts_pos, total_length);
    if (!ptr)
        return NULL;

    char *endptr;
    r->timestamp = strtoull(timestamp_str, &endptr, 10);

    if (*endptr != '\0')
        return NULL;

    // Skip CRLF
    ptr = skipcrlf(ptr);
    *total_length += CRLF_LEN;

    // Check for value marker
    if (*ptr != MARKER_VALUE)
        return NULL;

    ptr++;
    (*total_length)++;

    // Parse value
    char value_str[MAX_NUM_STR_LEN] = {0};
    size_t val_pos                  = 0;

    ptr = copy_value(ptr, value_str, &val_pos, total_length);
    if (!ptr)
        return NULL;

    r->value = strtold(value_str, &endptr);
    if (*endptr != '\0')
        return NULL;

    // Skip CRLF
    ptr = skipcrlf(ptr);
    *total_length += CRLF_LEN;

    return ptr;
}

static ssize_t decode_records(const uint8_t *ptr, response_t *dst)
{
    ssize_t total_length = 0;

    for (size_t i = 0; i < dst->array_response.length; i++) {
        ptr = decode_record(ptr, &dst->array_response.items[i], &total_length);
        // TODO proper error handling
        if (!ptr)
            return -1;
    }

    return total_length;
}

/**
 * Decode a stream response from the buffer
 * Format: "~<length>\r\n<chunk1>\r\n~<length>\r\n<chunk2>\r\n...~0\r\n"
 */
static ssize_t decode_stream_response(const uint8_t *data, response_t *dst,
                                      size_t datasize)
{
    if (!data || !dst)
        return -1;

    if (*data != MARKER_STREAM)
        return -1;

    data++;

    ssize_t offset                   = 1;
    size_t batch_length              = 0;

    // Parse batch length
    size_t pos                       = 0;
    char num_buffer[MAX_NUM_STR_LEN] = {0};

    data = copy_value(data, num_buffer, &pos, &offset);
    if (!data)
        return -1;

    char *endptr;

    batch_length = strtoull(num_buffer, &endptr, 10);
    if (*endptr != '\0')
        return -1;

    // Skip CRLF
    data = skipcrlf(data);
    offset += CRLF_LEN;

    // Initialize response structure
    dst->type                         = RT_STREAM;
    dst->stream_response.batch.length = batch_length;
    dst->stream_response.is_final     = false;

    // Allocate memory for batch items
    if (batch_length > 0) {
        dst->stream_response.batch.items =
            malloc(batch_length * sizeof(record_t));
        if (!dst->stream_response.batch.items)
            return -1;
    } else {
        dst->stream_response.batch.items = NULL;
    }

    // Parse records
    for (size_t i = 0; i < batch_length; i++) {
        data =
            decode_record(data, &dst->stream_response.batch.items[i], &offset);
        // TODO proper error hadling
        if (!data)
            goto cleanup_error;
    }

    // Skip blank line (CRLF)
    if (!iscrlf(data))
        goto cleanup_error;

    data = skipcrlf(data);
    offset += CRLF_LEN;

    // Check for final chunk marker
    if (offset + 3 < datasize) {
        if (*data == MARKER_STREAM && *(data + 1) == '0') {
            dst->stream_response.is_final = true;

            data += 2;
            offset += 2;

            if (!iscrlf(data))
                goto cleanup_error;

            // Skip to the end of the termination sequence
            data = skipcrlf(data);
            offset += CRLF_LEN;
        }
    }

    return offset;

cleanup_error:

    free(dst->stream_response.batch.items);
    dst->stream_response.batch.items  = NULL;
    dst->stream_response.batch.length = 0;
    return -1;
}

ssize_t decode_response(const uint8_t *data, response_t *dst, size_t datasize)
{
    if (!data || !dst)
        return -1;

    uint8_t marker = *data;
    ssize_t length = 0;

    dst->type      = marker == '#' ? RT_ARRAY : RT_STRING;

    switch (marker) {
    case MARKER_STRING_SUCCESS:
    case MARKER_STRING_ERROR:
        // Treat error and common strings the same for now
        dst->type = RT_STRING;
        length    = decode_string(data, dst);
        break;
    case MARKER_STREAM:
        length = decode_stream_response(data, dst, datasize);
        break;
    case MARKER_ARRAY:
        dst->type                  = RT_ARRAY;

        const uint8_t *ptr         = data + 1;
        ssize_t total_length       = 1; // Account for the # marker

        // Clear array length
        dst->array_response.length = 0;

        // Read array length
        while (!iscrlf(ptr)) {
            // Validate digit
            if (*ptr < '0' || *ptr > '9') {
                return -1;
            }

            dst->array_response.length *= 10;
            dst->array_response.length += *ptr - '0';
            ptr++;
            total_length++;
        }

        // Skip CRLF
        ptr = skipcrlf(ptr);
        total_length += CRLF_LEN;

        // Allocate memory for records
        if (dst->array_response.length > 0) {
            dst->array_response.items = calloc(
                dst->array_response.length, sizeof(*dst->array_response.items));
            if (!dst->array_response.items) {
                return -1;
            }
        } else {
            dst->array_response.items = NULL;
            return total_length;
        }

        ssize_t records_count = decode_records(ptr, dst);
        if (records_count < 0)
            goto cleanup;

        length = total_length + records_count;
        break;

    default:
        return -1;
    }

    return length;

cleanup:
    da_free(&dst->array_response);
    return -1;
}

void free_response(response_t *rs)
{
    if (!rs)
        return;

    if (rs->type == RT_ARRAY)
        da_free(&rs->array_response);
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
