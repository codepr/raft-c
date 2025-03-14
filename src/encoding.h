#ifndef ENCODING_H
#define ENCODING_H

#include "timeseries.h"
#include <stdint.h>
#include <sys/types.h>

// For the time being, fixed size to keep it simple, to be allocated
// as a future iteration
#define QUERYSIZE 512

/**
 ** Server text-based protocol
 **/

typedef enum {
    MARKER_STRING_SUCCESS = '$',
    MARKER_STRING_ERROR   = '!',
    MARKER_STREAM         = '~',
    MARKER_ARRAY          = '#',
    MARKER_TIMESTAMP      = ':',
    MARKER_VALUE          = ';'
} protocol_marker_t;

/*
 * Define a basic request, for the time being it's fine to treat
 * every request as a simple string paired with it's length.
 */
typedef struct request {
    size_t length;
    char query[QUERYSIZE];
} request_t;

/*
 * Define a response of type string, ideally RC (return code) should have a
 * meaning going forward.
 */
typedef struct {
    size_t length;
    uint8_t rc;
    char message[QUERYSIZE];
} string_response_t;

/*
 * Define a response of type array, mainly used as SELECT response.
 */
typedef record_array_t array_response_t;

typedef struct stream_response {
    int is_final;
    record_array_t batch;
} stream_response_t;

typedef enum { RT_STRING, RT_STREAM, RT_ARRAY } response_type_t;

/*
 * Define a generic response which can either be a string response or an
 * array response for the time being
 */
typedef struct response {
    response_type_t type;
    union {
        string_response_t string_response;
        stream_response_t stream_response;
        array_response_t array_response;
    };
} response_t;

// Encode a request into an array of bytes
ssize_t encode_request(const request_t *r, uint8_t *dst);

// Decode a request from an array of bytes into a Request struct
ssize_t decode_request(const uint8_t *data, request_t *dst);

// Encode a response into an array of bytes
ssize_t encode_response(const response_t *r, uint8_t *dst);

// Decode a response from an array of bytes into a Response struct
ssize_t decode_response(const uint8_t *data, response_t *dst, size_t datasize);

// Free an array response
void free_response(response_t *rs);

/**
** Cluster binary Interface functions
**/

// RAFT message encoding
typedef struct raft_message raft_message_t;

ssize_t raft_bin_message_write(uint8_t *buf, const raft_message_t *rm);
ssize_t raft_bin_message_read(const uint8_t *buf, raft_message_t *rm);

// Cluster message encoding
typedef struct cluster_message cluster_message_t;

ssize_t cluster_bin_message_write(uint8_t *buf, const cluster_message_t *cm);
ssize_t cluster_bin_message_read(const uint8_t *buf, cluster_message_t *cm);

#endif
