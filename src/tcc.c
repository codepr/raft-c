#include "tcc.h"
#include "buffer.h"

int tcc_read_buffer(tcc_t *ctx)
{
    if (!ctx || !ctx->buffer)
        return -1;

    return buffer_read_from_fd(ctx->buffer, ctx->fd, 0);
}

int tcc_flush_buffer(tcc_t *ctx)
{
    if (!ctx || !ctx->buffer)
        return -1;

    if (buffer_is_empty(ctx->buffer))
        return 0; // Nothing to flush

    return buffer_write_to_fd(ctx->buffer, ctx->fd, 0);
}
