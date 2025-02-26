#include "client.h"
#include "encoding.h"
#include "statement.h"
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
    if (strncasecmp(cmd, "create", 6) == 0)
        return "CREATE <database-name>|<timeseries-name> [INTO <database "
               "name>] [retention] [dup policy]";
    if (strncasecmp(cmd, "delete", 6) == 0)
        return "DELETE <database-name>|<timeseries-name>";
    if (strncasecmp(cmd, "insert", 3) == 0)
        return "INSERT <timeseries-name> INTO <database-name> timestamp|* "
               "value, ..";
    if (strncasecmp(cmd, "select", 5) == 0)
        return "SELECT <timeseries-name> FROM <database-name> [RANGE|AT] "
               "start_timestamp [end_timestamp] [WHERE] <identifier> "
               "[<|>|<=|>=|=|!=] [AGGREGATE] [MIN|MAX|AVG] [BY literal]";
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
    if (rs->type == STRING_RSP) {
        printf("%s\n", rs->string_response.message);
    } else {
        for (size_t i = 0; i < rs->array_response.length; ++i)
            printf("%" PRIu64 " %.6f\n",
                   rs->array_response.records[i].timestamp,
                   rs->array_response.records[i].value);
    }
}

static void print_usage(const char *prog_name)
{
    // TOOD assumes localhost for the time being
    fprintf(stderr, "Usage: %s -p <port>\n", prog_name);
    exit(EXIT_FAILURE);
}

static void parse_args(int argc, char *argv[], int *port, int *mode)
{
    int opt;
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

static void runcli(client_t *c)
{
    int err                    = 0;
    char *line                 = NULL;
    size_t line_len            = 0LL;
    struct timespec start_time = {0};
    struct timespec end_time   = {0};
    response_t rs              = {0};
    double delta               = 0.0;

    while (1) {
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
        client_recv_response(c, &rs);
        (void)clocktime(&end_time);
        print_response(&rs);
        if (rs.type == ARRAY_RSP) {
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

    parse_args(argc, argv, &connport, &mode);

    struct connect_options conn_opts = {.s_family = connmode,
                                        .s_addr   = connhost,
                                        .s_port   = connport,
                                        .timeout  = 0};
    client_t c                       = {.opts = &conn_opts};

    if (mode && client_connect(&c) < 0)
        exit(EXIT_FAILURE);

    if (mode)
        runcli(&c);
    else
        runclidbg(&c);

    client_disconnect(&c);

    return 0;
}
