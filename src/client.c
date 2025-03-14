#include "client.h"
#include "buffer.h"
#include "encoding.h"
#include "network.h"
#include "tcc.h"
#include <errno.h>
#include <netdb.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

/*
 * Create a non-blocking socket and use it to connect to the specified host and
 * port
 */
int client_connect(client_t *c)
{
    int fd = tcp_connect(c->opts->s_addr, c->opts->s_port, 0);
    if (fd < 0)
        return CLIENT_FAILURE;

    if (c->tcc) {
        c->tcc->fd = fd;
    } else {
        c->tcc = tcc_create(fd, 0);
        if (!c->tcc) {
            close(fd);
            return -1;
        }
    }

    return CLIENT_SUCCESS;
}

void client_disconnect(client_t *c)
{
    close(c->tcc->fd);
    tcc_free(c->tcc);
}

int client_send_command(client_t *c, char *buf)
{
    request_t rq = {.length = strlen(buf) - 1};
    snprintf(rq.query, sizeof(rq.query), "%s", buf);
    ssize_t n = buffer_encode_request(c->tcc->buffer, &rq);
    if (n < 0)
        return n;

    if (tcc_flush_buffer(c->tcc) != 0) {
        // TODO add proper errors
        return -1;
    }

    return n;
}

int client_recv_response(client_t *c, response_t *rs)
{
    ssize_t n = tcc_read_buffer(c->tcc);
    if (n < 0)
        return n;
    return buffer_decode_response(c->tcc->buffer, rs);
}
