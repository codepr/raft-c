#include "server.h"
#include "cluster.h"
#include "config.h"
#include "encoding.h"
#include "iomux.h"
#include "logger.h"
#include "network.h"
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

static ssize_t handle_client(int fd, iomux_t *iomux, uint8_t buf[BUFSIZ])
{
    return 1;
}

static ssize_t handle_peer(int fd, iomux_t *iomux, uint8_t buf[BUFSIZ])
{
    cluster_message_t cm = {0};
    ssize_t n            = recv(fd, buf, BUFSIZ, 0);
    if (n <= 0)
        return n;

    cluster_message_read(buf, &cm);

    switch (cm.type) {
    case CM_CLUSTER_JOIN:
        break;
    case CM_CLUSTER_DATA:
        cluster_submit(&cm);
        break;
    }

    return n;
}

static int server_start(int serverfd, int clusterfd)
{
    uint8_t buf[BUFSIZ]        = {0};
    int clientfds[FD_SETSIZE]  = {0};
    int clusterfds[FD_SETSIZE] = {0};
    int numevents              = 0;

    iomux_t *iomux             = iomux_create();
    if (!iomux)
        return -1;

    // Initialize client_fds array
    for (int i = 0; i < FD_SETSIZE; i++) {
        clientfds[i]  = -1;
        clusterfds[i] = -1;
    }

    iomux_add(iomux, serverfd, IOMUX_READ);

    if (clusterfd > 0)
        iomux_add(iomux, clusterfd, IOMUX_READ);

    while (1) {
        memset(buf, 0x00, sizeof(buf));

        numevents = iomux_wait(iomux, -1);
        if (numevents < 0)
            log_critical("iomux error: %s", strerror(errno));

        for (int i = 0; i < numevents; ++i) {
            int fd = iomux_get_event_fd(iomux, i);

            if (fd == serverfd) {
                // New connection
                int clientfd = tcp_accept(serverfd, 1);
                if (clientfd < 0) {
                    log_error("accept() error: %s", strerror(errno));
                    continue;
                }

                if (clientfds[clientfd] != -1) {
                    log_warning("client connecting on an open socket");
                    continue;
                }

                clientfds[clientfd] = clientfd;

                iomux_add(iomux, clientfd, IOMUX_READ);

            } else if (fd == clusterfd) {
                // New cluster node connected
                int nodefd = tcp_accept(clusterfd, 1);
                if (nodefd < 0) {
                    log_error("accept() error: %s", strerror(errno));
                    continue;
                }

                if (clusterfds[clusterfd] != -1) {
                    log_warning("peer connecting on an open socket");
                    continue;
                }

                clusterfds[clusterfd] = clusterfd;

                iomux_add(iomux, clusterfd, IOMUX_READ);
            } else if (clientfds[fd] != -1) {
                ssize_t err = handle_client(fd, iomux, buf);
                if (err <= 0) {
                    close(fd);
                    clientfds[fd] = -1;
                    log_info("Client disconnected");
                    continue;
                }
            } else if (clusterfds[fd] != -1) {
                ssize_t err = handle_peer(fd, iomux, buf);
                if (err <= 0) {
                    close(fd);
                    clusterfds[fd] = -1;
                    log_info("Peer disconnected");
                    continue;
                }
            }
        }
    }

    iomux_free(iomux);
    close(serverfd);
    if (clusterfd > 0)
        close(clusterfd);

    return 0;
}

typedef struct {
    char config_file[64];
    int node_id;
    int port;
} args_t;

static void print_usage(const char *prog_name)
{
    fprintf(stderr, "Usage: %s -n <node_id> -p <port>\n", prog_name);
    exit(EXIT_FAILURE);
}

static void parse_args(int argc, char *argv[], args_t *args)
{
    int opt;
    while ((opt = getopt(argc, argv, "c:n:p:")) != -1) {
        switch (opt) {
        case 'c':
            strncpy(args->config_file, optarg, 64);
            config_load(optarg);
            break;
        case 'n':
            args->node_id = atoi(optarg);
            break;
        case 'p':
            args->port = atoi(optarg);
            break;
        default:
            print_usage(argv[0]);
            break;
        }
    }
}

int main(int argc, char **argv)
{
    if (argc < 2)
        exit(EXIT_FAILURE);

    config_set_default();

    log_info("Application node start");

    args_t config                                    = {0};
    config.node_id                                   = -1;
    char node_strings[MAX_LIST_SIZE][MAX_VALUE_SIZE] = {0};
    char raft_strings[MAX_LIST_SIZE][MAX_VALUE_SIZE] = {0};
    cluster_node_t nodes[3]                          = {0};
    cluster_node_t replicas[3]                       = {0};
    int node_id                                      = -1;

    parse_args(argc, argv, &config);

    config_print();

    int nodes_num    = config_get_list("shard_leaders", node_strings);
    int replicas_num = config_get_list("raft_replicas", raft_strings);

    for (int i = 0; i < nodes_num; ++i)
        cluster_node_from_string(node_strings[i], &nodes[i]);

    for (int i = 0; i < replicas_num; ++i)
        cluster_node_from_string(raft_strings[i], &replicas[i]);

    node_id = config.node_id >= 0 ? config.node_id : config_get_int("id");

    cluster_start(nodes, nodes_num, replicas, replicas_num, node_id,
                  "raft_state.bin", config_get_enum("type"));

    int cluster_fd      = -1;
    int server_fd       = -1;

    cluster_node_t this = {0};
    cluster_node_from_string(config_get("host"), &this);

    server_fd = tcp_listen(this.ip, this.port, 1);
    if (server_fd < 0)
        exit(EXIT_FAILURE);

    log_info("Listening on %s", config_get("host"));

    if (config_get_enum("type") == NT_SHARD) {
        if (config.port > 0)
            cluster_fd = tcp_listen("127.0.0.1", config.port, 1);
        else
            cluster_fd = tcp_listen(nodes[node_id].ip, nodes[node_id].port, 1);

        if (cluster_fd < 0)
            exit(EXIT_FAILURE);

        log_info("Cluster channel on %s:%d", nodes[node_id].ip,
                 nodes[node_id].port);
    }

    server_start(server_fd, cluster_fd);

    config_free();
}
