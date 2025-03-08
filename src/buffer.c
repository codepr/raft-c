#include "buffer.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

buffer_t *buffer_create(size_t capacity, bool auto_resize, size_t max_capacity)
{
    buffer_t *buffer = malloc(sizeof(buffer_t));
    if (!buffer)
        return NULL;

    buffer->data = malloc(capacity);
    if (!buffer->data) {
        free(buffer);
        return NULL;
    }

    buffer->capacity     = capacity;
    buffer->size         = 0;
    buffer->read_pos     = 0;
    buffer->write_pos    = 0;
    buffer->auto_resize  = auto_resize;
    buffer->max_capacity = max_capacity;

    return buffer;
}

void buffer_free(buffer_t *buf)
{
    if (buf) {
        if (buf->data) {
            free(buf->data);
        }
        free(buf);
    }
}

static buffer_error_t buffer_ensure_capacity(buffer_t *buf, size_t additional)
{
    if (!buf)
        return BUFFER_ERROR_NULL;

    size_t required = buf->write_pos + additional;

    if (required <= buf->capacity) {
        return BUFFER_OK;
    }

    if (!buf->auto_resize) {
        return BUFFER_ERROR_OVERFLOW;
    }

    size_t new_capacity = buf->capacity * 2;
    while (new_capacity < required) {
        new_capacity *= 2;
    }

    if (new_capacity > buf->max_capacity) {
        if (buf->max_capacity < required) {
            return BUFFER_ERROR_OVERFLOW;
        }
        new_capacity = buf->max_capacity;
    }

    uint8_t *new_data = realloc(buf->data, new_capacity);
    if (!new_data) {
        return BUFFER_ERROR_MEMORY;
    }

    buf->data     = new_data;
    buf->capacity = new_capacity;

    return BUFFER_OK;
}

buffer_error_t buffer_write(buffer_t *buf, const void *data, size_t length)
{
    if (!buf || !data)
        return BUFFER_ERROR_NULL;

    buffer_error_t err = buffer_ensure_capacity(buf, length);
    if (err != BUFFER_OK)
        return err;

    memcpy(buf->data + buf->write_pos, data, length);
    buf->write_pos += length;

    if (buf->write_pos > buf->size) {
        buf->size = buf->write_pos;
    }

    return BUFFER_OK;
}

buffer_error_t buffer_read(buffer_t *buf, void *out, size_t length)
{
    if (!buf || !out)
        return BUFFER_ERROR_NULL;

    if (buf->read_pos + length > buf->size) {
        return BUFFER_ERROR_UNDERFLOW;
    }

    memcpy(out, buf->data + buf->read_pos, length);
    buf->read_pos += length;

    return BUFFER_OK;
}

buffer_error_t buffer_peek(buffer_t *buf, void *out, size_t length)
{
    if (!buf || !out)
        return BUFFER_ERROR_NULL;

    if (buf->read_pos + length > buf->size) {
        return BUFFER_ERROR_UNDERFLOW;
    }

    memcpy(out, buf->data + buf->read_pos, length);

    return BUFFER_OK;
}

buffer_error_t buffer_skip(buffer_t *buf, size_t length)
{
    if (!buf)
        return BUFFER_ERROR_NULL;

    if (buf->read_pos + length > buf->size) {
        return BUFFER_ERROR_UNDERFLOW;
    }

    buf->read_pos += length;

    return BUFFER_OK;
}

buffer_error_t buffer_reset(buffer_t *buf)
{
    if (!buf)
        return BUFFER_ERROR_NULL;

    buf->read_pos  = 0;
    buf->write_pos = 0;
    buf->size      = 0;

    return BUFFER_OK;
}

buffer_error_t buffer_clear(buffer_t *buf) { return buffer_reset(buf); }

buffer_error_t buffer_compact(buffer_t *buf)
{
    if (!buf)
        return BUFFER_ERROR_NULL;

    if (buf->read_pos == 0) {
        return BUFFER_OK;
    }

    if (buf->read_pos >= buf->size) {
        buf->read_pos  = 0;
        buf->write_pos = 0;
        buf->size      = 0;
        return BUFFER_OK;
    }

    size_t remaining = buf->size - buf->read_pos;
    memmove(buf->data, buf->data + buf->read_pos, remaining);

    buf->read_pos  = 0;
    buf->write_pos = remaining;
    buf->size      = remaining;

    return BUFFER_OK;
}

bool buffer_is_empty(const buffer_t *buf)
{
    if (!buf)
        return true;

    return buf->read_pos >= buf->size;
}

size_t buffer_remaining_read(const buffer_t *buf)
{
    if (!buf)
        return 0;

    return buf->size - buf->read_pos;
}

size_t buffer_remaining_write(const buffer_t *buf)
{
    if (!buf)
        return 0;

    return buf->capacity - buf->write_pos;
}

buffer_error_t buffer_read_from_fd(buffer_t *buf, int fd, size_t max_length)
{
    if (!buf)
        return BUFFER_ERROR_NULL;
    if (fd < 0)
        return BUFFER_ERROR_IO;

    if (max_length == 0) {
        max_length = buf->capacity - buf->write_pos;
    }

    if (max_length == 0) {
        buffer_error_t err = buffer_ensure_capacity(buf, 1024);
        if (err != BUFFER_OK)
            return err;
        max_length = buf->capacity - buf->write_pos;
    }

    ssize_t bytes_read = read(fd, buf->data + buf->write_pos, max_length);

    if (bytes_read < 0) {
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
            return BUFFER_OK; // No data available but no error
        }
        return BUFFER_ERROR_IO;
    }

    if (bytes_read == 0) {
        return BUFFER_OK; // EOF, but not an error
    }

    buf->write_pos += bytes_read;
    if (buf->write_pos > buf->size) {
        buf->size = buf->write_pos;
    }

    return BUFFER_OK;
}

buffer_error_t buffer_write_to_fd(buffer_t *buf, int fd, size_t max_length)
{
    if (!buf)
        return BUFFER_ERROR_NULL;
    if (fd < 0)
        return BUFFER_ERROR_IO;

    size_t available = buf->size - buf->read_pos;
    if (available == 0)
        return BUFFER_OK;

    if (max_length == 0 || max_length > available) {
        max_length = available;
    }

    ssize_t bytes_written = write(fd, buf->data + buf->read_pos, max_length);

    if (bytes_written < 0) {
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
            return BUFFER_OK; // Would block, but not an error
        }
        return BUFFER_ERROR_IO;
    }

    buf->read_pos += bytes_written;

    return BUFFER_OK;
}
