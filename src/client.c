#include "client.h"
#include "encoding.h"
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
static int raftc_connect(const struct connect_options *opts)
{

    /* socket: create the socket */
    int fd = socket(opts->s_family, SOCK_STREAM, 0);
    if (fd < 0)
        goto err;

    /* Set socket timeout for read and write if present on options */
    if (opts->timeout > 0) {
        struct timeval tv;
        tv.tv_sec  = opts->timeout;
        tv.tv_usec = 0;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv, sizeof(tv));
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (const char *)&tv, sizeof(tv));
    }

    if (opts->s_family == AF_INET) {
        struct sockaddr_in addr;
        struct hostent *server;

        /* gethostbyname: get the server's DNS entry */
        server = gethostbyname(opts->s_addr);
        if (server == NULL)
            goto err;

        /* build the server's address */
        addr.sin_family = opts->s_family;
        addr.sin_port   = htons(opts->s_port);
        addr.sin_addr   = *((struct in_addr *)server->h_addr);
        bzero(&(addr.sin_zero), 8);

        /* connect: create a connection with the server */
        if (connect(fd, (const struct sockaddr *)&addr, sizeof(addr)) == -1)
            goto err;

    } else if (opts->s_family == AF_UNIX) {
        struct sockaddr_un addr;
        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        if (*opts->s_addr == '\0') {
            *addr.sun_path = '\0';
            strncpy(addr.sun_path + 1, opts->s_addr + 1,
                    sizeof(addr.sun_path) - 2);
        } else {
            strncpy(addr.sun_path, opts->s_addr, sizeof(addr.sun_path) - 1);
        }

        if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == -1)
            goto err;
    }

    return fd;

err:

    if (errno == EINPROGRESS)
        return fd;

    perror("socket(2) opening socket failed");
    return CLIENT_FAILURE;
}

int client_connect(client_t *c)
{
    int fd = raftc_connect(c->opts);
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

    n = decode_response(data, rs);
    if (n < 0)
        return -1;

    return n;
}
