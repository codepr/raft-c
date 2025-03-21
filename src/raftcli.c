#include "buffer.h"
#include "client.h"
#include "encoding.h"
#include "statement_parse.h"
#include "tcc.h"
#include "timeutil.h"
#include <errno.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define LOCALHOST    "127.0.0.1"
#define DEFAULT_PORT 18777

static const char *cmd_usage(const char *cmd)
{
    if (strncasecmp(cmd, "createdb", 8) == 0)
        return "CREATEDB <database-name>";

    if (strncasecmp(cmd, "use", 3) == 0)
        return "USE <database-name>";

    if (strncasecmp(cmd, "create", 6) == 0)
        return "CREATE <timeseries-name> [<retention>] [<duplication>]";

    if (strncasecmp(cmd, "insert", 6) == 0)
        return "INSERT INTO <timeseries-name> VALUES (<timestamp>, <value>) | "
               "VALUES (<timestamp>, <value>), ... | VALUE <value>";

    if (strncasecmp(cmd, "select", 6) == 0)
        return "SELECT <value|function(value)> FROM <timeseries-name> [BETWEEN "
               "<start_time> AND <end_time>] [WHERE value <comparator> "
               "<value>] [SAMPLE BY <interval>] [LIMIT <n>]";

    if (strncasecmp(cmd, "delete", 6) == 0)
        return "DELETE <timeseries-name> | DELETE <timeseries-name> FROM "
               "<database-name>";

    if (strcmp(cmd, ".databases") == 0)
        return ".databases - List all databases";

    if (strcmp(cmd, ".timeseries") == 0)
        return ".timeseries - List all timeseries in the active database";

    return NULL;
}

static void prompt(client_t *c)
{
    if (c->opts->s_family == AF_INET)
        printf("%s:%i> ", c->opts->s_addr, c->opts->s_port);
    else if (c->opts->s_family == AF_UNIX)
        printf("%s> ", c->opts->s_addr);
}

static void print_response(const response_t *rs)
{
    if (rs->type == RT_STRING) {
        printf("(string) %s\n", rs->string_response.message);
    } else if (rs->type == RT_STREAM) {
        printf("(stream)\n");
        for (size_t i = 0; i < rs->stream_response.batch.length; ++i)
            printf("%ld) %" PRIu64 " %.6f\n", i,
                   rs->stream_response.batch.items[i].timestamp,
                   rs->stream_response.batch.items[i].value);
    } else {
        printf("(array)\n");
        for (size_t i = 0; i < rs->array_response.length; ++i)
            printf("%ld) %" PRIu64 " %.6f\n", i,
                   rs->array_response.items[i].timestamp,
                   rs->array_response.items[i].value);
    }
}

static void print_usage(const char *prog_name)
{
    // TOOD assumes localhost for the time being
    fprintf(stderr, "Usage: %s -p <port>\n", prog_name);
    exit(EXIT_FAILURE);
}

static void parse_args(int argc, char *argv[], int *port, int *mode,
                       char **dbname)
{
    int opt;
    *dbname = NULL;

    while ((opt = getopt(argc, argv, "p:d")) != -1) {
        switch (opt) {
        case 'p':
            *port = atoi(optarg);
            break;
        case 'd':
            *mode = 0;
            break;
        default:
            print_usage(argv[0]);
            break;
        }
    }

    if (optind < argc)
        *dbname = argv[optind];
}

static void runclidbg(client_t *c)
{
    char *line      = NULL;
    size_t line_len = 0LL;
    stmt_t *stmt    = NULL;

    while (1) {
        prompt(c);
        getline(&line, &line_len, stdin);

        stmt = stmt_parse(line);
        printf("\n");
        stmt_print(stmt);
        printf("\n");
        stmt_free(stmt);
    }
    free(line);
}

static void runcli(client_t *c, const char *dbname)
{
    int err                    = 0;
    char *line                 = NULL;
    size_t line_len            = 0LL;
    struct timespec start_time = {0};
    struct timespec end_time   = {0};
    response_t rs              = {0};
    double delta               = 0.0;

    if (dbname) {
        char usecmd[64] = {0};
        snprintf(usecmd, sizeof(usecmd), "use %s\n", dbname);
        err = client_send_command(c, usecmd);
        client_recv_response(c, &rs);
        print_response(&rs);
    }

    while (1) {
        memset(&rs, 0x00, sizeof(rs));
        prompt(c);
        getline(&line, &line_len, stdin);
        (void)clocktime(&start_time);
        err = client_send_command(c, line);
        if (err <= 0) {
            if (err == CLIENT_SUCCESS) {
                client_disconnect(c);
                break;
            } else if (err == CLIENT_UNKNOWN_CMD) {
                printf("Unknown command or malformed one\n");
                const char *usage = cmd_usage(line);
                if (usage)
                    printf("\nSuggesed usage: %s\n\n", usage);
            } else if (err == CLIENT_FAILURE) {
                printf("Couldn't send the command: %s\n", strerror(errno));
            }
            continue;
        }
        ssize_t result_count = 0;
        do {
            client_recv_response(c, &rs);
            print_response(&rs);
            result_count += rs.stream_response.batch.length;
        } while (rs.type == RT_STREAM && rs.stream_response.is_final == 0);
        (void)clocktime(&end_time);
        if (rs.type == RT_STREAM) {
            buffer_decode_response(c->tcc->buffer, &rs);
            print_response(&rs);
            delta = timespec_seconds(&end_time) - timespec_seconds(&end_time);
            printf("%lu results in %lf seconds.\n", result_count, delta);
        } else if (rs.type == RT_ARRAY) {
            delta = timespec_seconds(&end_time) - timespec_seconds(&end_time);
            printf("%lu results in %lf seconds.\n", rs.array_response.length,
                   delta);
        }
    }
    free(line);
}

int main(int argc, char **argv)
{
    int connport   = DEFAULT_PORT;
    int connmode   = AF_INET;
    char *connhost = LOCALHOST;
    int mode       = 1;
    char *dbname   = NULL;

    parse_args(argc, argv, &connport, &mode, &dbname);

    struct connect_options conn_opts = {.s_family = connmode,
                                        .s_addr   = connhost,
                                        .s_port   = connport,
                                        .timeout  = 0};
    client_t c                       = {.opts = &conn_opts};

    if (mode && client_connect(&c) < 0)
        exit(EXIT_FAILURE);

    if (mode)
        runcli(&c, dbname);
    else
        runclidbg(&c);

    client_disconnect(&c);

    return 0;
}
