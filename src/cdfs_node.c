#include "cdfs_node.h"
#include "cluster.h"
#include "config.h"
#include "encoding.h"
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

#define HEADERSIZE sizeof(uint8_t) + sizeof(int32_t)

static fd_set write_fds;
static int max_fd                = 0;
static int nodes_fds[FD_SETSIZE] = {0};

static void async_broadcast(const file_chunk_t *chunks, size_t num_chunks)
{

    int num_events = 0;
    // Initialize client_fds array
    for (int i = 0; i < FD_SETSIZE; i++)
        nodes_fds[i] = -1;

    FD_ZERO(&write_fds);

    for (int i = 0; i < num_chunks; i++) {
        cluster_node_t *node = cluster_node_lookup((char *)chunks[i].hash);

        cluster_connect(node, 1);
        nodes_fds[i] = node->sock_fd;

        max_fd       = max_fd < node->sock_fd ? node->sock_fd : max_fd;

        FD_SET(node->sock_fd, &write_fds);
    }

    while (num_chunks > 0) {
        num_events = select(max_fd + 1, NULL, &write_fds, NULL, NULL);
        if (num_events < 0)
            log_critical("select() error: %s", strerror(errno));

        for (int i = 0; i < FD_SETSIZE; ++i) {
            if (nodes_fds[i] >= 0 && FD_ISSET(nodes_fds[i], &write_fds)) {
                // TODO send chunks here
            }
        }
    }

    log_info("All chunks uploaded");
}

static void server_start(int server_fd, int cluster_fd)
{
    fd_set read_fds;
    int max_fd     = cluster_fd > server_fd ? cluster_fd : server_fd;
    int num_events = 0, i = 0;
    unsigned char buf[BUFSIZ]   = {0};
    int client_fds[FD_SETSIZE]  = {0};
    int cluster_fds[FD_SETSIZE] = {0};
    cluster_message_t cm        = {0};

    // Initialize client_fds array
    for (i = 0; i < FD_SETSIZE; i++) {
        client_fds[i]  = -1;
        cluster_fds[i] = -1;
    }

    while (1) {
        memset(buf, 0x00, sizeof(buf));

        FD_ZERO(&read_fds);
        FD_SET(server_fd, &read_fds);
        if (cluster_fd >= 0)
            FD_SET(cluster_fd, &read_fds);

        // Re-arm client descriptors and cluster ones to be monitored by the
        // select call
        for (i = 0; i < FD_SETSIZE; ++i) {
            if (client_fds[i] >= 0) {
                FD_SET(client_fds[i], &read_fds);
                if (client_fds[i] > max_fd)
                    max_fd = client_fds[i];
            }

            if (cluster_fds[i] >= 0) {
                FD_SET(cluster_fds[i], &read_fds);
                if (cluster_fds[i] > max_fd)
                    max_fd = cluster_fds[i];
            }
        }

        num_events = select(max_fd + 1, &read_fds, NULL, NULL, NULL);
        if (num_events < 0)
            log_critical("select() error: %s", strerror(errno));

        if (FD_ISSET(server_fd, &read_fds)) {
            // New connection
            int client_fd = tcp_accept(server_fd, 1);
            if (client_fd < 0) {
                log_error("accept() error: %s", strerror(errno));
                continue;
            }

            for (i = 0; i < FD_SETSIZE; ++i) {
                if (client_fds[i] < 0) {
                    client_fds[i] = client_fd;
                    break;
                }
            }

            if (i == FD_SETSIZE) {
                log_error("Too many clients connected");
                close(client_fd);
                continue;
            }
        } else if (cluster_fd >= 0 && FD_ISSET(cluster_fd, &read_fds)) {
            // New cluster node connected
            int node_fd = tcp_accept(cluster_fd, 1);
            if (node_fd < 0) {
                log_error("accept() error: %s", strerror(errno));
                continue;
            }

            for (i = 0; i < FD_SETSIZE; ++i) {
                if (cluster_fds[i] < 0) {
                    cluster_fds[i] = node_fd;
                    break;
                }
            }

            if (i == FD_SETSIZE) {
                log_error("Too many nodes connected");
                close(node_fd);
                continue;
            }
        }

        for (i = 0; i < FD_SETSIZE; ++i) {
            if (client_fds[i] >= 0 && FD_ISSET(client_fds[i], &read_fds)) {
                ssize_t n = recv_nonblocking(client_fds[i], buf, HEADERSIZE);
                if (n <= 0) {
                    close(client_fds[i]);
                    client_fds[i] = -1;
                    log_info("Client disconnected");
                    continue;
                }
                // TODO check for message size against the buffer
                cdfs_header_t header = {0};
                cdfs_bin_header_read(buf, &header);

                n = recv_nonblocking(client_fds[i], buf, header.size);
                if (n <= 0) {
                    close(client_fds[i]);
                    client_fds[i] = -1;
                    log_info("Client disconnected");
                    continue;
                }
                cdfs_message_t message = {0};
                cdfs_bin_chunk_read(buf, &message);

                switch (header.type) {
                case CMT_PUSH_FILE:
                    // TODO - Metadata node, redirect or error if not
                    // - split the file into chunks
                    // - generate merkle tree
                    // - store the file name and map it to locations
                    //     filename -> {hash, [nodes]}
                    break;
                case CMT_PULL_FILE:
                    // TODO - Metadata node, redirect or error if not
                    // - retrieve filename key
                    // - async retrieve all chunks from each location
                    // - merge chunks and send file back to the client
                    // Alternative
                    // - send list of locations and allow client to connect and
                    //   retrieve all the chunks
                    break;
                case CMT_PUSH_CHUNK:
                    // - TODO - Shard node, redirect or error if not
                    // - deserialized
                    // - store filename -> {hash, chunk}
                    break;
                case CMT_PULL_CHUNK:
                    // - TODO - Shard node, redirect or error if not
                    // - retrieve filename -> {hash, chunk}
                    // - serialize and send back to requester (generally a
                    //   metadata node)
                    break;
                }

                // cm.type = CM_CLUSTER_DATA;
                // strncpy(cm.key, key, MAX_KEY_SIZE);
                // write_i32(value, val);
                // cm.payload.data = value;
                // cm.payload.size = sizeof(int);
                // cluster_submit(&cm);
            } else if (cluster_fds[i] >= 0 &&
                       FD_ISSET(cluster_fds[i], &read_fds)) {
                ssize_t n = recv(cluster_fds[i], buf, sizeof(buf), 0);
                if (n <= 0) {
                    close(client_fds[i]);
                    client_fds[i] = -1;
                    log_info("Client disconnected");
                    continue;
                }

                cluster_message_read(buf, &cm);

                switch (cm.type) {
                case CM_CLUSTER_JOIN:
                    break;
                case CM_CLUSTER_DATA:
                    cluster_submit(&cm);
                    break;
                }
            }
        }
    }
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
