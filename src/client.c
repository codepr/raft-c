#include "encoding.h"
#include "network.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static void print_usage(const char *prog_name)
{
    // TOOD assumes localhost for the time being
    fprintf(stderr, "Usage: %s -p <port>\n", prog_name);
    exit(EXIT_FAILURE);
}

static void parse_args(int argc, char *argv[], int *port)
{
    int opt;
    while ((opt = getopt(argc, argv, "p:")) != -1) {
        switch (opt) {
        case 'p':
            *port = atoi(optarg);
            break;
        default:
            print_usage(argv[0]);
            break;
        }
    }
}

int main(int argc, char **argv)
{
    char command[255];
    size_t payload_length = 0;
    int port              = 0;

    parse_args(argc, argv, &port);

    int fd = tcp_connect("127.0.0.1", port, 0);
    if (fd < 0)
        exit(EXIT_FAILURE);

    while (1) {
        printf("dfs> ");
        if (!fgets(command, sizeof(command), stdin))
            break;

        command[strcspn(command, "\n")] = 0;

        if (strncmp(command, "exit", 4) == 0) {
            break;
        } else if (strncmp(command, "store", 5) == 0) {
            (void)strtok(command, " ");
            char *filename = strtok(NULL, "\n");
            if (!filename) {
                printf("No file specifed\n");
            } else {
                FILE *fp = fopen(filename, "rb");
                if (!fp) {
                    fprintf(stderr, "Open file %s: %s", filename,
                            strerror(errno));
                    free(filename);
                    continue;
                }

                if (fseek(fp, 0, SEEK_END) < 0) {
                    fclose(fp);
                    free(filename);
                    continue;
                }

                long size = ftell(fp);
                rewind(fp);

                unsigned char payload[255];
                cdfs_message_t m = {.header.type = CMT_PULL_FILE,
                                    .header.size = size};
                int n = snprintf((char *)m.filename, 255, "%s", filename);

                n     = cdfs_bin_chunk_write(payload, &m);
                ssize_t read = fread(payload + n, size, 1, fp);
                if (read < 0) {
                    fprintf(stderr, "fread %s\n", strerror(errno));
                    free(filename);
                    fclose(fp);
                    continue;
                }

                payload_length = n + size;
                n              = send(fd, payload, payload_length, 0);
                if (n < 0) {
                    fprintf(stderr, "send %s\n", strerror(errno));
                    free(filename);
                    fclose(fp);
                    continue;
                }
                fclose(fp);
            }
        } else {
            printf("Unknown command %s\n", command);
        }
    }

    close(fd);

    return 0;
}
