#include "server.h"
#include "buffer.h"
#include "cluster.h"
#include "config.h"
#include "dbcontext.h"
#include "encoding.h"
#include "iomux.h"
#include "logger.h"
#include "network.h"
#include "statement_execute.h"
#include "statement_parse.h"
#include "tcc.h"
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

#define set_fmt_response(resp, rc, fmt, ...)                                   \
    do {                                                                       \
        (resp)->type = RT_STRING;                                              \
        (resp)->string_response.length =                                       \
            snprintf((resp)->string_response.message, QUERYSIZE,               \
                     fmt __VA_OPT__(, ) __VA_ARGS__);                          \
    } while (0)

#define set_string_response(resp, rc, str)                                     \
    do {                                                                       \
        (resp)->type = RT_STRING;                                              \
        (resp)->string_response.length =                                       \
            snprintf((resp)->string_response.message, QUERYSIZE, "%s", (str)); \
    } while (0)

#define not_implemented(resp)                                                  \
    set_string_response((resp), 1, "Error: not supported")

/**
 * Process a USE statement and generate appropriate response
 *
 * @param stmt The statement representing a META instruction
 * @param response Pointer to response structure to be filled
 */
// static void execute_use(const stmt_t *stmt, response_t *rs)
// {
//     if (dbcontext_setactive(stmt->create.db_name) < 0)
//         set_string_response(rs, 1, "Database '%s' not found",
//                             stmt->create.db_name);
//     else
//         set_string_response(rs, 0, "Database '%s' selected",
//                             stmt->create.db_name);
// }
//
// /**
//  * Process a CREATEDB statement and generate appropriate response
//  *
//  * @param stmt The statement representing a META instruction
//  * @param response Pointer to response structure to be filled
//  */
// static void execute_createdb(const stmt_t *stmt, response_t *rs)
// {
//     timeseries_db_t *tsdb = dbcontext_get(stmt->create.db_name);
//     if (tsdb) {
//         set_string_response(rs, 1, "Database '%s' already exists",
//                             stmt->create.db_name);
//         return;
//     }
//
//     tsdb = dbcontext_add(stmt->create.db_name);
//     if (!tsdb) {
//         set_string_response(rs, 1, "Error creating '%s' database",
//                             stmt->create.db_name);
//         return;
//     }
//
//     set_string_response(rs, 0, "Database '%s' created successfully",
//                         stmt->create.db_name);
// }
//
// /**
//  * Process a CREATE statement and generate appropriate response
//  *
//  * @param stmt The statement representing a CREATE instruction
//  * @param response Pointer to response structure to be filled
//  */
// static void execute_create(const stmt_t *stmt, response_t *rs)
// {
//     timeseries_db_t *tsdb = NULL;
//     timeseries_t *ts      = NULL;
//
//     // Get the specified database or use active database
//     if (stmt->create.db_name[0] != '\0') {
//         tsdb = dbcontext_get(stmt->create.db_name);
//         if (!tsdb) {
//             set_string_response(rs, 1, "Database '%s' not found",
//                                 stmt->create.db_name);
//
//             return;
//         }
//     }
//
//     tsdb = dbcontext_getactive();
//     if (!tsdb) {
//         set_string_response(rs, 1, "No active database");
//
//         return;
//     }
//
//     ts_opts_t opts = {.retention = stmt->create.retention.value};
//
//     // Create timeseries
//     // TODO handle duplication policy
//     ts             = ts_create(tsdb, stmt->create.ts_name, opts);
//
//     if (ts) {
//         set_string_response(
//             rs, 0, "TimeSeries '%s' created successfully in database
//             '%s'", stmt->create.ts_name, tsdb->datapath);
//     } else {
//         set_string_response(rs, 1, "Failed to create TimeSeries '%s'",
//                             stmt->create.ts_name);
//     }
// }
//
// /**
//  * Process a DELETE statement and generate appropriate response
//  *
//  * @param stmt The statement representing a DELETE instruction
//  * @param response Pointer to response structure to be filled
//  */
// static void execute_delete(const stmt_t *stmt, response_t *rs)
// {
//     // Currently, deletion is not directly supported in the provided API
//     not_implemented(rs);
// }
//
// /**
//  * Process a META statement and generate appropriate response
//  *
//  * @param stmt The statement representing a META instruction
//  * @param response Pointer to response structure to be filled
//  */
// static void execute_meta(const stmt_t *stmt, response_t *rs)
// {
//     // Currently, meta is not directly supported in the provided API
//     not_implemented(rs);
// }
//
// /**
//  * Process an INSERT statement and generate appropriate response
//  *
//  * @param stmt The statement representing an INSERT instruction
//  * @param response Pointer to response structure to be filled
//  */
// static void execute_insert(const stmt_t *stmt, response_t *rs)
// {
//     // Active database not supported yet
//     timeseries_db_t *tsdb = dbcontext_getactive();
//     if (!tsdb) {
//         set_string_response(rs, 1,
//                             "No database in the system, create one
//                             first");
//         return;
//     }
//
//     timeseries_t *ts = ts_get(tsdb, stmt->insert.ts_name);
//     if (!ts) {
//         set_string_response(rs, 1, "Timeseries '%s' not found",
//                             stmt->create.ts_name);
//         return;
//     }
//
//     int success_count = 0;
//     int error_count   = 0;
//
//     // Insert each record
//     for (size_t i = 0; i < stmt->insert.record_array.length; i++) {
//         stmt_record_t *record = &stmt->insert.record_array.items[i];
//
//         int result            = ts_insert(ts, record->timestamp,
//         record->value); if (result == 0) {
//             success_count++;
//         } else {
//             error_count++;
//         }
//     }
//
//     // Set response based on insertion results
//     if (error_count == 0) {
//         set_string_response(rs, 0, "Successfully inserted %d points",
//                             success_count);
//     } else {
//         set_string_response(rs, 1, "Inserted %d points with %d errors",
//                             success_count, error_count);
//     }
// }
//
// /**
//  * Process a SELECT statement and generate appropriate response
//  *
//  * @param stmt The statemenet representing a SELECT instruction
//  * @param response Pointer to response structure to be filled
//  */
// static void execute_select(const stmt_t *stmt, response_t *rs)
// {
//     timeseries_db_t *tsdb = dbcontext_getactive();
//     if (!tsdb) {
//         set_string_response(rs, 1,
//                             "No database in the system, create one
//                             first");
//         return;
//     }
//
//     timeseries_t *ts = ts_get(tsdb, stmt->select.ts_name);
//     if (!ts) {
//         set_string_response(rs, 1, "Timeseries '%s' not found",
//                             stmt->create.ts_name);
//         return;
//     }
//
//     record_array_t records = {0};
//
//     // Query data based on select mask
//     // if (stmt->select.flags & QF_BASE) {
//     //     // Single point query
//     //     record_t record;
//     //     if (ts_find(ts, stmt->select.timeunit.tsinterval.start,
//     &record)
//     ==
//     //     0) {
//     //         // Prepare array response with single record
//     //         rs->type                  = RT_ARRAY;
//     //         rs->array_response.length = 1;
//     //         rs->array_response.records =
//     //             malloc(sizeof(*rs->array_response.records));
//     //         if (!rs->array_response.records) {
//     //             set_string_response(rs, 1, "Error: Memory allocation
//     //             failed");
//     //
//     //             return;
//     //         }
//     //         rs->array_response.records[0].timestamp =
//     record.timestamp;
//     //         rs->array_response.records[0].value     = record.value;
//     //
//     //         return;
//     //     }
//     //     set_string_response(rs, 1,
//     //                         "Error: Point not found at timestamp %"
//     PRIu64,
//     //                         stmt->select.timeunit.tsinterval.start);
//     //     return;
//     // }
//
//     if (stmt->select.flags & QF_RNGE) {
//         // Range query
//         int result =
//             ts_range(ts, stmt->select.selector.interval.start.value,
//                      stmt->select.selector.interval.end.value, &records);
//
//         if (result == 0 && records.length > 0) {
//             // Prepare array response from records
//             rs->type                  = RT_ARRAY;
//             rs->array_response.length = records.length;
//             rs->array_response.records =
//                 malloc(records.length *
//                 sizeof(*rs->array_response.records));
//
//             if (!rs->array_response.records) {
//                 set_string_response(rs, 1, "Error: Memory allocation
//                 failed"); return;
//             }
//
//             for (size_t i = 0; i < records.length; i++) {
//                 rs->array_response.records[i].timestamp =
//                     records.items[i].timestamp;
//                 rs->array_response.records[i].value =
//                 records.items[i].value;
//             }
//
//             // Free the record array items (data has been copied)
//             free(records.items);
//
//             return;
//         }
//         rs->type = RT_STRING;
//         if (result != 0) {
//             set_string_response(rs, 1,
//                                 "Error: Failed to query range [%" PRIu64
//                                 ", %" PRIu64 "]",
//                                 stmt->select.selector.interval.start.value,
//                                 stmt->select.selector.interval.end.value);
//         } else {
//             set_string_response(
//                 rs, 0, "No data found in range [%" PRIu64 ", %" PRIu64
//                 "]", stmt->select.selector.interval.start.value,
//                 stmt->select.selector.interval.end.value);
//         }
//         return;
//     }
//
//     // Unsupported query type
//     // TODO
//     set_string_response(rs, 1, "Error: Unsupported query type");
// }

static response_t execute_statement(tcc_t *ctx, const stmt_t *stmt)
{
    response_t rs = {0};
    if (!stmt) {
        // Handle parse error with a string response
        set_string_response(&rs, 1, "Error: Failed to parse the query");
        goto exit;
    }

    execute_stmt_result_t exec_result = stmt_execute(ctx, stmt);

    switch (exec_result.code) {
    case EXEC_SUCCESS_STRING:
        set_string_response(&rs, 0, exec_result.message);
        break;
    case EXEC_SUCCESS_ARRAY:
        // Prepare array response from records
        rs.type           = RT_ARRAY;
        rs.array_response = exec_result.result_set;

        // if (!rs.array_response.records) {
        //     set_string_response(&rs, 1, "Error: Memory allocation failed");
        //     goto exit;
        // }

        break;
    case EXEC_ERROR_UNSUPPORTED:
        not_implemented(&rs);
        break;
        // TODO handle other error cases
    default:
        set_string_response(&rs, 1, exec_result.message);
        break;
    }

    // switch (stmt->type) {
    // case STMT_USE:
    //     execute_use(stmt, &rs);
    //     break;
    // case STMT_CREATEDB:
    //     execute_createdb(stmt, &rs);
    //     break;
    // case STMT_CREATE:
    //     execute_create(stmt, &rs);
    //     break;
    // case STMT_INSERT:
    //     execute_insert(stmt, &rs);
    //     break;
    // case STMT_SELECT:
    //     execute_select(stmt, &rs);
    //     break;
    // case STMT_DELETE:
    //     execute_delete(stmt, &rs);
    //     break;
    // case STMT_META:
    //     execute_meta(stmt, &rs);
    //     break;
    // default:
    //     // Unknown statement type (should not happen due to earlier
    //     check) set_string_response(&rs, 1, "Error: Unsupported statement
    //     type"); break;
    // }

exit:
    return rs;
}

static ssize_t handle_client(tcc_t *ctx)
{
    request_t rq                 = {0};
    response_t rs                = {0};
    ssize_t bytes_read           = 0;
    ssize_t bytes_written        = 0;
    uint8_t response_buf[BUFSIZ] = {0};
    stmt_t *stmt                 = NULL;

    int err                      = tcc_read_buffer(ctx);
    if (err < 0)
        return -1;

    bytes_read = decode_request(ctx->buffer->data, &rq);
    if (bytes_read < 0) {
        log_error("Failed to decode client request: %zd", bytes_read);
        set_string_response(&rs, 1, "Failed to decode request");
        goto exit;
    }

    log_debug("Received query: %.*s", (int)rq.length, rq.query);
    // Parse into Statement
    stmt          = stmt_parse(rq.query);
    // Execute it
    rs            = execute_statement(ctx, stmt);

    // Encode the response

    bytes_written = encode_response(&rs, response_buf);
    if (bytes_written <= 0) {
        log_error("Failed to encode response: %zd", bytes_written);
        if (stmt)
            stmt_free(stmt);
        if (rs.type == RT_ARRAY)
            free_response(&rs);
        return -1;
    }

    // Send the response back to the client
    ssize_t sent = send_nonblocking(ctx->fd, response_buf, bytes_written);
    if (sent != bytes_written) {
        log_error("Failed to send complete response: %zd/%zd", sent,
                  bytes_written);
        if (stmt)
            stmt_free(stmt);
        if (rs.type == RT_ARRAY)
            free_response(&rs);
        return -1;
    }

    // Clean up
    if (stmt)
        stmt_free(stmt);
    if (rs.type == RT_ARRAY)
        free_response(&rs);

exit:

    return bytes_read;
}

static ssize_t handle_peer(tcc_t *ctx)
{

    int err = tcc_read_buffer(ctx);
    if (err < 0)
        return -1;
    return 0;
    // cluster_message_t cm = {0};
    // ssize_t n            = recv(ctx->fd, buf, BUFSIZ, 0);
    // if (n <= 0)
    //     return n;
    //
    // cluster_message_read(buf, &cm);
    //
    // switch (cm.type) {
    // case CM_CLUSTER_JOIN:
    //     break;
    // case CM_CLUSTER_DATA:
    //     cluster_submit(&cm);
    //     break;
    // }
    //
    // return n;
}

static int server_start(int serverfd, int clusterfd)
{
    tcc_t *clientfds[FD_SETSIZE]  = {NULL};
    tcc_t *clusterfds[FD_SETSIZE] = {NULL};
    int numevents                 = 0;

    iomux_t *iomux                = iomux_create();
    if (!iomux)
        return -1;

    iomux_add(iomux, serverfd, IOMUX_READ);

    if (clusterfd > 0)
        iomux_add(iomux, clusterfd, IOMUX_READ);

    // Init db context
    int n = dbcontext_init(DBCTX_BASESIZE);
    if (n < 0) {
        return -1;
    } else {
        log_info("init %d databases", n);
    }

    while (1) {
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

                if (clientfds[clientfd] != NULL) {
                    log_warning("client connecting on an open socket");
                    continue;
                }

                clientfds[clientfd] = tcc_create(clientfd, 1);
                if (!clientfds[clientfd])
                    log_critical("Out of memory on client connection");

                log_info("New client connected");
                iomux_add(iomux, clientfd, IOMUX_READ);

            } else if (fd == clusterfd) {
                // New cluster node connected
                int nodefd = tcp_accept(clusterfd, 1);
                if (nodefd < 0) {
                    log_error("accept() error: %s", strerror(errno));
                    continue;
                }

                if (clusterfds[clusterfd] != NULL) {
                    log_warning("peer connecting on an open socket");
                    continue;
                }

                clusterfds[clusterfd] = tcc_create(clusterfd, 1);
                if (!clusterfds[clusterfd])
                    log_critical("Out of memory on peer connection");

                iomux_add(iomux, clusterfd, IOMUX_READ);
            } else if (clientfds[fd] != NULL) {
                buffer_clear(clientfds[fd]->buffer);
                int err = handle_client(clientfds[fd]);
                if (err <= 0) {
                    tcc_free(clientfds[fd]);
                    clientfds[fd] = NULL;
                    close(fd);
                    log_info("Client disconnected");
                    continue;
                }
            } else if (clusterfds[fd] != NULL) {
                buffer_clear(clusterfds[fd]->buffer);
                int err = handle_peer(clusterfds[fd]);
                if (err <= 0) {
                    tcc_free(clusterfds[fd]);
                    close(fd);
                    clusterfds[fd] = NULL;
                    log_info("Peer disconnected");
                    continue;
                }
            }
        }
    }

    for (int i = 0; i < FD_SETSIZE; ++i) {
        if (clientfds[i])
            tcc_free(clientfds[i]);
        if (clusterfds[i])
            tcc_free(clusterfds[i]);
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
