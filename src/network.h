#ifndef NETWORK_H
#define NETWORK_H

#include <sys/types.h>

struct sockaddr_in;

char *get_ip_str(const struct sockaddr_in *sa, char *s, size_t maxlen);
int tcp_accept(int server_fd);
int tcp_listen(const char *host, int port);
int tcp_connect(const char *host, int port, int nonblocking);
ssize_t send_nonblocking(int fd, const unsigned char *buf, size_t len);

int udp_listen(const char *host, int port);

#endif
