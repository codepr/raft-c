#include "server.h"
#include "cluster.h"
#include "config.h"
#include "encoding.h"
#include "iomux.h"
#include "logger.h"
#include "network.h"
#include "parser.h"
#include "timeseries.h"
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

#define add_string_response(resp, str, rc)                                     \
    do {                                                                       \
        (resp).type   = STRING_RSP;                                            \
        size_t length = strlen((str));                                         \
        memset((resp).string_response.message, 0x00,                           \
               sizeof((resp).string_response.message));                        \
        strncpy((resp).string_response.message, (str), length);                \
        (resp).string_response.length = length;                                \
    } while (0)

// testing dummy
static timeseries_db_t *db = NULL;

static response_t execute_statement(const ast_node_t *stm)
{
    response_t rs    = {0};
    record_t r       = {0};
    timeseries_t *ts = NULL;
    int err          = 0;
    struct timespec tv;

    switch (stm->type) {
    case STATEMENT_CREATE:
        if (!stm->create.single) {
            db = tsdb_init(stm->create.db_name);
            if (!db)
                goto err;
        } else {
            if (!db)
                db = tsdb_init(stm->create.db_name);

            if (!db)
                goto err;

            ts = ts_create(db, stm->create.ts_name, 0, DP_IGNORE);
        }
        if (!ts)
            goto err;
        else
            add_string_response(rs, "Ok", 0);
        break;
    case STATEMENT_INSERT:
        if (!db)
            db = tsdb_init(stm->insert.db_name);

        if (!db)
            goto err;

        ts = ts_get(db, stm->insert.ts_name);
        if (!ts)
            goto err_not_found;

        uint64_t timestamp = 0;
        for (size_t i = 0; i < stm->insert.record_array.length; ++i) {
            if (stm->insert.record_array.items[i].timestamp == -1) {
                clock_gettime(CLOCK_REALTIME, &tv);
                timestamp = tv.tv_sec * 1e9 + tv.tv_nsec;
            } else {
                timestamp = stm->insert.record_array.items[i].timestamp;
            }

            err = ts_insert(ts, timestamp,
                            stm->insert.record_array.items[i].value);
            if (err < 0)
                goto err;
            else
                add_string_response(rs, "Ok", 0);
        }

        break;
    case STATEMENT_SELECT:
        if (!db)
            db = tsdb_init(stm->select.db_name);

        if (!db)
            goto err;

        ts = ts_get(db, stm->select.ts_name);
        if (!ts)
            goto err_not_found;

        int err = 0;

        record_array_t coll;

        if (stm->select.mask & SM_SINGLE) {
            err = ts_find(ts, stm->select.start_time, &r);
            if (err < 0) {
                log_error("Couldn't find the record %" PRIu64,
                          stm->select.start_time);
                goto err_not_found;
            } else {
                log_info("Record found: %" PRIu64 " %.2lf", r.timestamp,
                         r.value);
                rs.type                  = ARRAY_RSP;
                rs.array_response.length = 1;
                rs.array_response.records =
                    calloc(1, sizeof(*rs.array_response.records));
                rs.array_response.records[0].timestamp = r.timestamp;
                rs.array_response.records[0].value     = r.value;
            }
        } else if (stm->select.mask & SM_RANGE) {
            err = ts_range(ts, stm->select.start_time, stm->select.end_time,
                           &coll);
            if (err < 0) {
                log_error("Couldn't find the record %" PRIu64,
                          stm->select.start_time);
            } else {
                for (size_t i = 0; i < coll.length; i++) {
                    record_t r = coll.items[i];
                    log_info(" %" PRIu64
                             " {.sec: %li, .nsec: %li .value: %.02f }",
                             r.timestamp, r.tv.tv_sec, r.tv.tv_nsec, r.value);
                }
            }
            rs.array_response.length = coll.length;
            for (size_t i = 0; i < coll.length; ++i) {
                rs.array_response.records[i].timestamp = r.timestamp;
                rs.array_response.records[i].value     = r.value;
            }
        }
        break;
    default:
        log_error("Unknown command");
        break;
    }

    if (ts)
        ts_close(ts);

    return rs;

err:
    add_string_response(rs, "Err", err);
    return rs;

err_not_found:

    add_string_response(rs, "Not found", err);
    return rs;
}

static ssize_t handle_client(int fd, iomux_t *iomux, const uint8_t buf[BUFSIZ])
{
    request_t rq          = {0};
    response_t rs         = {0};
    ast_node_t *statement = NULL;

    ssize_t n             = decode_request(buf, &rq);
    if (n < 0) {
        log_error("Can't decode a request from data");
        rs.type               = STRING_RSP;
        rs.string_response.rc = 1;
        strncpy(rs.string_response.message, "Err", 4);
        rs.string_response.length = 4;
    } else {
        log_info("Payload received");
        // Parse into Statement
        statement = ast_parse(rq.query);
        // Execute it
        rs        = execute_statement(statement);
    }
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

                log_info("New client connected");
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
                ssize_t n = recv_nonblocking(fd, buf, sizeof(buf));
                if (n <= 0) {
                    clientfds[fd] = -1;
                    close(fd);
                    log_info("Client disconnected\n");
                    continue;
                }
                ssize_t err = handle_client(fd, iomux, buf);
                if (err <= 0) {
                    close(fd);
                    clientfds[fd] = -1;
                    log_info("Client disconnected");
                    continue;
                }
            } else if (clusterfds[fd] != -1) {
                ssize_t n = recv_nonblocking(fd, buf, sizeof(buf));
                if (n <= 0) {
                    clientfds[fd] = -1;
                    close(fd);
                    log_info("Client disconnected\n");
                    continue;
                }
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
    config_set_default();

    log_info("Application node start");

    args_t config                                    = {0};
    config.node_id                                   = -1;
    char node_strings[MAX_LIST_SIZE][MAX_VALUE_SIZE] = {0};
    char raft_strings[MAX_LIST_SIZE][MAX_VALUE_SIZE] = {0};
    cluster_node_t nodes[3]                          = {0};
    cluster_node_t replicas[3]                       = {0};
    int node_id                                      = -1;
    int cluster_fd                                   = -1;
    int server_fd                                    = -1;
    cluster_node_t this                              = {0};
    cluster_node_from_string(config_get("host"), &this);

    parse_args(argc, argv, &config);

    config_print();

    if (config_get_enum("type") != NT_STANDALONE) {

        int nodes_num    = config_get_list("shard_leaders", node_strings);
        int replicas_num = config_get_list("raft_replicas", raft_strings);

        for (int i = 0; i < nodes_num; ++i)
            cluster_node_from_string(node_strings[i], &nodes[i]);

        for (int i = 0; i < replicas_num; ++i)
            cluster_node_from_string(raft_strings[i], &replicas[i]);

        node_id = config.node_id >= 0 ? config.node_id : config_get_int("id");

        cluster_start(nodes, nodes_num, replicas, replicas_num, node_id,
                      "raft_state.bin", config_get_enum("type"));
    }

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
