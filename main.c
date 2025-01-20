#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/**
 ** Dynamic array utility macros
 **/

#define da_extend(da)                                                          \
    do {                                                                       \
        (da)->capacity *= 2;                                                   \
        (da)->items =                                                          \
            realloc((da)->items, (da)->capacity * sizeof(*(da)->items));       \
        if (!(da)->items) {                                                    \
            fprintf(stderr, "DA realloc failed");                              \
            exit(EXIT_FAILURE);                                                \
        }                                                                      \
    } while (0)

#define da_append(da, item)                                                    \
    do {                                                                       \
        assert((da));                                                          \
        if ((da)->length + 1 == (da)->capacity)                                \
            da_extend((da));                                                   \
        (da)->items[(da)->length++] = (item);                                  \
    } while (0)

/**
 ** TCP network utility functions
 **/

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
    fprintf(stderr, "server_accept -> accept() %s\n", strerror(errno));
    return -1;
}

int tcp_connect(const char *host, int port, int nonblocking)
{

    int s, retval = -1;
    struct addrinfo *servinfo, *p;
    struct timeval tv           = {0, CLIENT_TIMEOUT};
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

        setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(struct timeval));
        setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(struct timeval));

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
/**
 ** Protocol utility functions
 **/

static int write_u8(uint8_t *buf, uint8_t val)
{
    *buf++ = val;
    return sizeof(uint8_t);
}

static uint8_t read_u8(const uint8_t *const buf) { return ((uint8_t)*buf); }

// write_u32() -- store a 32-bit int into a char buffer (like htonl())
static int write_u32(uint8_t *buf, uint32_t val)
{
    *buf++ = val >> 24;
    *buf++ = val >> 16;
    *buf++ = val >> 8;
    *buf++ = val;

    return sizeof(uint32_t);
}

// read_u32() -- unpack a 32-bit unsigned from a char buffer (like ntohl())
static uint32_t read_u32(const uint8_t *const buf)
{
    return ((uint32_t)buf[0] << 24) | ((uint32_t)buf[1] << 16) |
           ((uint32_t)buf[2] << 8) | buf[3];
}

/**
 ** RPC structure definition
 **/

// RequestVoteRPC

typedef struct request_vote_rpc {
    int term;
    int candidate_id;
    int last_log_term;
    int last_log_index;
} request_vote_rpc_t;

typedef struct request_vote_response {
    int term;
    bool vote_granted;
} request_vote_response_t;

static int request_vote_rpc_write(uint8_t *buf, const request_vote_rpc_t *rv)
{
    // term
    int bytes = write_u32(buf, rv->term);
    buf += sizeof(uint32_t);

    // candidate_id
    bytes += write_u32(buf, rv->candidate_id);
    buf += sizeof(uint32_t);

    // last_log_term
    bytes += write_u32(buf, rv->last_log_term);
    buf += sizeof(uint32_t);

    // last_log_index
    bytes += write_u32(buf, rv->last_log_index);
    buf += sizeof(uint32_t);

    return bytes;
}

static int request_vote_response_write(uint8_t *buf,
                                       const request_vote_response_t *rv)
{
    // term
    int bytes = write_u32(buf, rv->term);
    buf += sizeof(uint32_t);

    bytes += write_u8(buf++, rv->vote_granted);

    return bytes;
}

static int request_vote_rpc_read(request_vote_rpc_t *rv, const uint8_t *buf)
{
    rv->term = read_u32(buf);
    buf += sizeof(uint32_t);

    rv->candidate_id = read_u32(buf);
    buf += sizeof(uint32_t);

    rv->last_log_term = read_u32(buf);
    buf += sizeof(uint32_t);

    rv->last_log_index = read_u32(buf);
    buf += sizeof(uint32_t);

    return 0;
}

static int request_vote_response_read(request_vote_response_t *rv,
                                      const uint8_t *buf)
{
    rv->term = read_u32(buf);
    buf += sizeof(uint32_t);
    rv->vote_granted = read_u8(buf++);
    return 0;
}

// AppendEntrierRPC
typedef struct append_entries_rpc {
    int term;
    int leader_id;
    int prev_log_term;
    int prev_log_index;
    int leader_commit;
    struct {
        size_t length;
        size_t capacity;
        int *items;
    } *entries;
} append_entries_rpc_t;

typedef struct append_entries_response {
    int term;
    bool success;
} append_entries_response_t;

static int append_entries_rpc_write(uint8_t *buf,
                                    const append_entries_rpc_t *ae)
{
    // term
    int bytes = write_u32(buf, ae->term);
    buf += sizeof(uint32_t);

    // leader_id
    bytes += write_u32(buf, ae->leader_id);
    buf += sizeof(uint32_t);

    // prev_log_term
    bytes += write_u32(buf, ae->prev_log_term);
    buf += sizeof(uint32_t);

    // prev_log_index
    bytes += write_u32(buf, ae->prev_log_index);
    buf += sizeof(uint32_t);

    // leader_commit
    bytes += write_u32(buf, ae->leader_commit);
    buf += sizeof(uint32_t);

    // entries count
    bytes += write_u32(buf, ae->entries->length);
    buf += sizeof(uint32_t);

    // entries
    for (size_t i = 0; i < ae->entries->length; ++i) {
        bytes += write_u32(buf, ae->entries->items[i]);
        buf += sizeof(uint32_t);
    }

    return bytes;
}

static int append_entries_response_write(uint8_t *buf,
                                         const append_entries_response_t *ae)
{
    // term
    int bytes = write_u32(buf, ae->term);
    buf += sizeof(uint32_t);

    bytes += write_u8(buf++, ae->success);

    return bytes;
}

static int append_entries_rpc_read(append_entries_rpc_t *ae, const uint8_t *buf)
{
    ae->term = read_u32(buf);
    buf += sizeof(uint32_t);

    ae->leader_id = read_u32(buf);
    buf += sizeof(uint32_t);

    ae->prev_log_term = read_u32(buf);
    buf += sizeof(uint32_t);

    ae->prev_log_index = read_u32(buf);
    buf += sizeof(uint32_t);

    ae->leader_commit = read_u32(buf);
    buf += sizeof(uint32_t);

    uint32_t entries_count = read_u32(buf);
    buf += sizeof(uint32_t);

    for (int i = 0; i < entries_count; ++i) {
        da_append(ae->entries, read_u32(buf));
        buf += sizeof(uint32_t);
    }

    return 0;
}

static int append_entries_reponse_reqd(append_entries_response_t *ae,
                                       const uint8_t *buf)
{
    ae->term = read_u32(buf);
    buf += sizeof(uint32_t);
    ae->success = read_u8(buf++);
    return 0;
}

/**
 ** Raft machine State
 **/

typedef enum raft_machine_state {
    RS_CANDIDATE,
    RS_FOLLOWER,
    RS_LEADER
} raft_machine_state_t;

typedef struct raft_state {
    raft_machine_state_t state;
    int current_term;
    int voted_for;
    struct {
        size_t length;
        size_t capacity;
        int *items;
    } *log;
    union {
        struct {
            int commit_index;
            int last_applied;
        } volatile;
        struct {
            int *next_index;
            int *match_index;
        } leader_volatile;
    };
} raft_state_t;

int main(void)
{
    printf("Hello world\n");
    return 0;
}
