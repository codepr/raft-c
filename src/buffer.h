// buffer.h
#ifndef BUFFER_H
#define BUFFER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

// Error codes
typedef enum {
    BUFFER_OK              = 0,
    BUFFER_ERROR_NULL      = -1,
    BUFFER_ERROR_MEMORY    = -2,
    BUFFER_ERROR_OVERFLOW  = -3,
    BUFFER_ERROR_UNDERFLOW = -4,
    BUFFER_ERROR_IO        = -5
} buffer_error_t;

// Buffer structure
typedef struct buffer {
    uint8_t *data;       // Buffer data
    size_t capacity;     // Total capacity
    size_t size;         // Current used size
    size_t read_pos;     // Current read position
    size_t write_pos;    // Current write position
    size_t max_capacity; // Maximum capacity if auto_resize is true
    bool auto_resize;    // Whether to resize automatically
} buffer_t;

// Buffer creation and destruction
buffer_t *buffer_create(size_t capacity, bool auto_resize, size_t max_capacity);
void buffer_free(buffer_t *buffer);

// Core buffer operations
buffer_error_t buffer_write(buffer_t *buf, const void *data, size_t length);
buffer_error_t buffer_read(buffer_t *buf, void *out, size_t length);
buffer_error_t buffer_peek(buffer_t *buf, void *out, size_t length);
buffer_error_t buffer_skip(buffer_t *buf, size_t length);

// Utility functions
buffer_error_t buffer_reset(buffer_t *buf);
buffer_error_t buffer_clear(buffer_t *buf);
buffer_error_t buffer_compact(buffer_t *buf);
bool buffer_is_empty(const buffer_t *buffer);
size_t buffer_remaining_read(const buffer_t *buf);
size_t buffer_remaining_write(const buffer_t *buf);

// Network I/O integration
buffer_error_t buffer_read_from_fd(buffer_t *buf, int fd, int nonblocking,
                                   size_t max_length);
buffer_error_t buffer_write_to_fd(buffer_t *buf, int fd, int nonblocking,
                                  size_t max_length);

#endif
