#ifndef CLIENT_H
#define CLIENT_H

#include <netdb.h>
#include <stdio.h>

#define CLIENT_SUCCESS     0
#define CLIENT_FAILURE     -1
#define CLIENT_UNKNOWN_CMD -2

typedef struct response response_t;

typedef struct client client_t;

/*
 * Connection options, use this structure to specify connection related opts
 * like socket family, host port and timeout for communication
 */
struct connect_options {
    int timeout;
    int s_family;
    int s_port;
    char *s_addr;
};

/*
 * Pretty basic connection wrapper, just a FD with a buffer tracking bytes and
 * some options for connection
 */
struct client {
    int fd;
    const struct connect_options *opts;
};

int client_connect(client_t *c);

void client_disconnect(client_t *c);

int client_send_command(client_t *c, char *buf);

int client_recv_response(client_t *c, response_t *rs);

#endif // CLIENT_H
