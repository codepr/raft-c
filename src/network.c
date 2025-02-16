#include "network.h"
#include "logger.h"
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define BACKLOG 128

static int set_nonblocking(int fd)
{
    int flags, result;
    flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1)
        return -1;

    result = fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    if (result == -1)
        return -1;

    return 0;
}

char *get_ip_str(const struct sockaddr_in *sa, char *s, size_t maxlen)
{
    switch (sa->sin_family) {
    case AF_INET:
        inet_ntop(AF_INET, &sa->sin_addr, s, maxlen);
        break;

    default:
        strncpy(s, "Unknown AF", maxlen);
        return NULL;
    }

    return s;
}

int tcp_accept(int server_fd)
{
    int fd;
    struct sockaddr_in addr;
    socklen_t addrlen = sizeof(addr);

    fd                = accept(server_fd, (struct sockaddr *)&addr, &addrlen);
    if (fd <= 0)
        goto err;

    if (set_nonblocking(fd) < 0)
        goto err;

    return fd;

err:
    log_error("server_accept -> accept() %s", strerror(errno));
    return -1;
}

int tcp_listen(const char *host, int port)
{
    int listen_fd               = -1;
    const struct addrinfo hints = {.ai_family   = AF_UNSPEC,
                                   .ai_socktype = SOCK_STREAM,
                                   .ai_flags    = AI_PASSIVE};
    struct addrinfo *result, *rp;
    char port_string[6];

    snprintf(port_string, 6, "%d", port);

    if (getaddrinfo(host, port_string, &hints, &result) != 0)
        return -1;

    for (rp = result; rp != NULL; rp = rp->ai_next) {
        listen_fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (listen_fd < 0)
            continue;

        if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &(int){1},
                       sizeof(int)) < 0)
            return -1;

        /* Bind it to the addr:port opened on the network interface */
        if (bind(listen_fd, rp->ai_addr, rp->ai_addrlen) == 0)
            break; // successful bind
        close(listen_fd);
    }

    freeaddrinfo(result);
    if (!rp)
        return -1;

    if (set_nonblocking(listen_fd) < 0)
        return -1;

    if (listen(listen_fd, BACKLOG) != 0)
        return -1;

    return listen_fd;
}

int tcp_connect(const char *host, int port, int nonblocking)
{
    int s, retval = -1;
    struct addrinfo *servinfo, *p;
    const struct addrinfo hints = {.ai_family   = AF_UNSPEC,
                                   .ai_socktype = SOCK_STREAM,
                                   .ai_flags    = AI_PASSIVE};

    char port_string[6];
    snprintf(port_string, sizeof(port_string), "%d", port);

    if (getaddrinfo(host, port_string, &hints, &servinfo) != 0)
        return -1;

    for (p = servinfo; p != NULL; p = p->ai_next) {
        /* Try to create the socket and to connect it.
         * If we fail in the socket() call, or on connect(), we retry with
         * the next entry in servinfo. */
        if ((s = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
            continue;

        /* Try to connect. */
        if (connect(s, p->ai_addr, p->ai_addrlen) == -1) {
            close(s);
            break;
        }

        /* If we ended an iteration of the for loop without errors, we
         * have a connected socket. Let's return to the caller. */
        retval = s;
        break;
    }

    // Set now non-blocking so it's possible to block on the connect and have a
    // ready-to-write socket immediately
    if (nonblocking && set_nonblocking(retval) < 0)
        goto err;

    freeaddrinfo(servinfo);
    return retval; /* Will be -1 if no connection succeded. */

err:

    close(s);
    perror("socket(2) opening socket failed");
    return -1;
}

ssize_t send_nonblocking(int fd, const unsigned char *buf, size_t len)
{
    size_t total     = 0;
    size_t bytesleft = len;
    ssize_t n        = 0;

    while (total < len) {
        n = send(fd, buf + total, bytesleft, MSG_NOSIGNAL);
        if (n == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;
            else
                goto err;
        }
        total += n;
        bytesleft -= n;
    }

    return total;

err:

    fprintf(stderr, "send(2) - error sending data: %s\n", strerror(errno));
    return -1;
}

/**
 ** Cluster communication and management - UDP network utility functions
 **
 ** The RAFT machine will be implemented by using UDP as the transport layer,
 ** making it lightweight and connection-less. We're don't really care about
 ** delivery guarantee ** and the communication is simplified.
 **/

int udp_listen(const char *host, int port)
{
    int listen_fd               = -1;
    const struct addrinfo hints = {.ai_family   = AF_INET,
                                   .ai_socktype = SOCK_DGRAM,
                                   .ai_flags    = AI_PASSIVE};
    struct addrinfo *result, *rp;
    char port_string[6];

    snprintf(port_string, 6, "%d", port);

    if (getaddrinfo(host, port_string, &hints, &result) != 0)
        return -1;

    for (rp = result; rp != NULL; rp = rp->ai_next) {
        listen_fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (listen_fd < 0)
            continue;

        if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &(int){1},
                       sizeof(int)) < 0)
            return -1;

        /* Bind it to the addr:port opened on the network interface */
        if (bind(listen_fd, rp->ai_addr, rp->ai_addrlen) == 0)
            break; // successful bind
        close(listen_fd);
    }

    freeaddrinfo(result);
    if (!rp)
        return -1;

    if (set_nonblocking(listen_fd) < 0)
        return -1;

    return listen_fd;
}
