#include "cluster.h"
#include "config.h"
#include "raft.h"
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define BACKLOG        128
#define CLIENT_TIMEOUT 10000

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

static int tcp_listen(const char *host, int port)
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

static int tcp_accept(int server_fd)
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
    fprintf(stderr, "server_accept -> accept() %s\n", strerror(errno));
    return -1;
}

static void server_start(int server_fd)
{
    fd_set read_fds;
    int max_fd     = server_fd;
    int num_events = 0, i = 0;
    unsigned char buf[BUFSIZ];
    int client_fds[FD_SETSIZE];

    // Initialize client_fds array
    for (i = 0; i < FD_SETSIZE; i++) {
        client_fds[i] = -1;
    }

    while (1) {
        memset(buf, 0x00, sizeof(buf));

        FD_ZERO(&read_fds);
        FD_SET(server_fd, &read_fds);

        // Re-arm client descriptors to be monitored by the select call
        for (i = 0; i < FD_SETSIZE; ++i) {
            if (client_fds[i] >= 0) {
                FD_SET(client_fds[i], &read_fds);
                if (client_fds[i] > max_fd)
                    max_fd = client_fds[i];
            }
        }

        num_events = select(max_fd + 1, &read_fds, NULL, NULL, NULL);
        if (num_events < 0) {
            fprintf(stderr, "select() error: %s\n", strerror(errno));
            exit(EXIT_FAILURE);
        }

        if (FD_ISSET(server_fd, &read_fds)) {
            // New connection
            int client_fd = tcp_accept(server_fd);
            if (client_fd < 0) {
                fprintf(stderr, "accept() error: %s\n", strerror(errno));
                continue;
            }

            for (i = 0; i < FD_SETSIZE; ++i) {
                if (client_fds[i] < 0) {
                    client_fds[i] = client_fd;
                    break;
                }
            }

            if (i == FD_SETSIZE) {
                fprintf(stderr, "too many clients connected");
                close(client_fd);
                continue;
            }
        }

        for (i = 0; i < FD_SETSIZE; ++i) {
            int fd = client_fds[i];
            if (fd >= 0 && FD_ISSET(fd, &read_fds)) {
                // TODO read data here
                ssize_t n = recv(fd, buf, sizeof(buf), 0);
                if (n <= 0) {
                    close(fd);
                    client_fds[i] = -1;
                    printf("Client disconnected\n");
                    continue;
                }
                // TODO read message here, assuming integers from a netcat
                // client or telnet, just plain bytes

                int value = atoi((const char *)buf);
                raft_submit(value);
            }
        }
    }
}

typedef struct {
    int type;
    node_type_t node_type;
    union {
        int node_id;
        int port;
    };
} config_t;

static void print_usage(const char *prog_name)
{
    fprintf(stderr, "Usage: %s -n <node_id> -p <port>\n", prog_name);
    exit(EXIT_FAILURE);
}

static void parse_args(int argc, char *argv[], config_t *config)
{
    int opt;
    while ((opt = getopt(argc, argv, "n:p:t:")) != -1) {
        switch (opt) {
        case 'n':
            config->type    = 0;
            config->node_id = atoi(optarg);
            break;
        case 'p':
            config->type = 1;
            config->port = atoi(optarg);
            break;
        case 't':
            if (strncasecmp(optarg, "main", 4) == 0)
                config->type = NODE_MAIN;
            else if (strncasecmp(optarg, "replica", 7) == 0)
                config->type = NODE_REPLICA;
            break;
        default:
            print_usage(argv[0]);
        }
    }

    // Validate required arguments
    if (config->node_id == -1 || config->port == -1) {
        print_usage(argv[0]);
    }
}

int main(int argc, char **argv)
{
    if (argc < 2)
        exit(EXIT_FAILURE);

    config_set_default();
    config_print();
    config_free();

    config_t config = {-1};

    parse_args(argc, argv, &config);

    cluster_node_t peers[3] = {
        {"127.0.0.1", 8777}, {"127.0.0.1", 8778}, {"127.0.0.1", 8779}};
    cluster_node_t replicas[3] = {
        {"127.0.0.1", 18777}, {"127.0.0.1", 18778}, {"127.0.0.1", 18779}};
    cluster_node_start(peers, 3, replicas, 3, config.node_id, config.node_type);

    int server_fd = 0;

    if (config.type == 0)
        server_fd =
            tcp_listen(peers[config.node_id].ip, peers[config.node_id].port);
    else
        server_fd = tcp_listen("127.0.0.1", config.port);

    if (server_fd < 0)
        exit(EXIT_FAILURE);
    server_start(server_fd);
}
