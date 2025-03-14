#include "tcc.h"
#include "buffer.h"

#define BUFFER_INITIAL_CAPACITY 2048
#define BUFFER_MAX_CAPACITY     4096

tcc_t *tcc_create(int fd, int nonblocking)
{
    tcc_t *tcc = calloc(1, sizeof(*tcc));
    if (!tcc)
        return NULL;

    tcc->buffer =
        buffer_create(BUFFER_INITIAL_CAPACITY, true, BUFFER_MAX_CAPACITY);

    if (!tcc->buffer) {
        free(tcc);
        return NULL;
    }

    tcc->fd          = fd;
    tcc->batch_size  = 1000;
    tcc->nonblocking = nonblocking;

    return tcc;
}

void tcc_free(tcc_t *tcc)
{
    buffer_free(tcc->buffer);
    free(tcc);
}

int tcc_read_buffer(tcc_t *ctx)
{
    if (!ctx || !ctx->buffer)
        return -1;

    return buffer_read_from_fd(ctx->buffer, ctx->fd, ctx->nonblocking, 0);
}

int tcc_flush_buffer(tcc_t *ctx)
{
    if (!ctx || !ctx->buffer)
        return -1;

    if (buffer_is_empty(ctx->buffer))
        return 0; // Nothing to flush

    return buffer_write_to_fd(ctx->buffer, ctx->fd, ctx->nonblocking, 0);
}
