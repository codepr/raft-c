#include "client.h"
#include "encoding.h"
#include "network.h"
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
    c->fd = fd;
    return CLIENT_SUCCESS;
}

void client_disconnect(client_t *c) { close(c->fd); }

int client_send_command(client_t *c, char *buf)
{
    uint8_t data[BUFSIZ];
    request_t rq = {.length = strlen(buf) - 1};
    snprintf(rq.query, sizeof(rq.query), "%s", buf);
    ssize_t n = encode_request(&rq, data);
    if (n < 0)
        return -1;

    return write(c->fd, data, n);
}

int client_recv_response(client_t *c, response_t *rs)
{
    uint8_t data[BUFSIZ];
    ssize_t n = read(c->fd, data, BUFSIZ);
    if (n < 0)
        return -1;

    n = decode_response(data, rs, n);
    if (n < 0)
        return -1;

    return n;
}
