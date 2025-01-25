#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define RANDOM(a, b) rand() % ((b) + 1 - (a)) + (a)

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

#define da_back(da)                                                            \
    do {                                                                       \
        assert((da));                                                          \
        (da)->items[(da)->length - 1];                                         \
    } while (0)

/**
 ** UDP network utility functions
 **/

#define BACKLOG     128
#define MAX_NODES   15
#define NODES_COUNT 3

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

static int udp_listen(const char *host, int port)
{

    int listen_fd               = -1;
    const struct addrinfo hints = {.ai_family   = AF_INET,
                                   .ai_socktype = SOCK_DGRAM,
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

    return listen_fd;
}

static unsigned long long get_microseconds_timestamp(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);

    // Convert the time to microseconds
    return (unsigned long long)(ts.tv_sec * 1000000 + ts.tv_nsec / 1000);
}

static unsigned long long get_seconds_timestamp(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);

    // Convert the time to microseconds
    return (unsigned long long)ts.tv_sec;
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

// write_i32() -- store a 32-bit int into a char buffer (like htonl())
static int write_i32(uint8_t *buf, int32_t val)
{
    *buf++ = val >> 24;
    *buf++ = val >> 16;
    *buf++ = val >> 8;
    *buf++ = val;

    return sizeof(int32_t);
}

// read_i32() -- unpack a 32-bit int from a char buffer (like ntohl())
static int32_t read_i32(const uint8_t *buf)
{
    uint32_t i2 = ((int32_t)buf[0] << 24) | ((int32_t)buf[1] << 16) |
                  ((int32_t)buf[2] << 8) | buf[3];
    int32_t val;

    // change unsigned numbers to signed
    if (i2 <= 0x7fffffffu)
        val = i2;
    else
        val = -1 - (int32_t)(0xffffffffu - i2);

    return val;
}

/**
 ** RPC structure definition
 **/

// Generic header to identify requests

typedef enum message_type {
    MT_REQUEST_VOTE_RQ,
    MT_REQUEST_VOTE_RS,
    MT_APPEND_ENTRIES_RQ,
    MT_APPEND_ENTRIES_RS
} message_type_t;

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
    // Message type
    int bytes = write_u8(buf++, MT_REQUEST_VOTE_RQ);

    // term
    bytes += write_i32(buf, rv->term);
    buf += sizeof(uint32_t);

    // candidate_id
    bytes += write_i32(buf, rv->candidate_id);
    buf += sizeof(uint32_t);

    // last_log_term
    bytes += write_i32(buf, rv->last_log_term);
    buf += sizeof(uint32_t);

    // last_log_index
    bytes += write_i32(buf, rv->last_log_index);
    buf += sizeof(uint32_t);

    return bytes;
}

static int request_vote_response_write(uint8_t *buf,
                                       const request_vote_response_t *rv)
{
    // Message type
    int bytes = write_u8(buf++, MT_REQUEST_VOTE_RS);

    // term
    bytes += write_i32(buf, rv->term);
    buf += sizeof(uint32_t);

    bytes += write_u8(buf++, rv->vote_granted);

    return bytes;
}

static int request_vote_rpc_read(request_vote_rpc_t *rv, const uint8_t *buf)
{
    rv->term = read_i32(buf);
    buf += sizeof(uint32_t);

    rv->candidate_id = read_i32(buf);
    buf += sizeof(uint32_t);

    rv->last_log_term = read_i32(buf);
    buf += sizeof(uint32_t);

    rv->last_log_index = read_i32(buf);
    buf += sizeof(uint32_t);

    return 0;
}

static int request_vote_response_read(request_vote_response_t *rv,
                                      const uint8_t *buf)
{
    rv->term = read_i32(buf);
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
{ // Message type
    int bytes = write_u8(buf++, MT_APPEND_ENTRIES_RQ);

    // term
    bytes += write_i32(buf, ae->term);
    buf += sizeof(uint32_t);

    // leader_id
    bytes += write_i32(buf, ae->leader_id);
    buf += sizeof(uint32_t);

    // prev_log_term
    bytes += write_i32(buf, ae->prev_log_term);
    buf += sizeof(uint32_t);

    // prev_log_index
    bytes += write_i32(buf, ae->prev_log_index);
    buf += sizeof(uint32_t);

    // leader_commit
    bytes += write_i32(buf, ae->leader_commit);
    buf += sizeof(uint32_t);

    if (ae->entries) {
        // entries count
        bytes += write_u32(buf, ae->entries->length);
        buf += sizeof(uint32_t);

        // entries
        for (size_t i = 0; i < ae->entries->length; ++i) {
            bytes += write_i32(buf, ae->entries->items[i]);
            buf += sizeof(uint32_t);
        }
    }

    return bytes;
}

static int append_entries_response_write(uint8_t *buf,
                                         const append_entries_response_t *ae)
{
    // Message type
    int bytes = write_u8(buf++, MT_APPEND_ENTRIES_RS);

    // term
    bytes += write_i32(buf, ae->term);
    buf += sizeof(uint32_t);

    bytes += write_u8(buf++, ae->success);

    return bytes;
}

static int append_entries_rpc_read(append_entries_rpc_t *ae, const uint8_t *buf)
{
    ae->term = read_i32(buf);
    buf += sizeof(uint32_t);

    ae->leader_id = read_i32(buf);
    buf += sizeof(uint32_t);

    ae->prev_log_term = read_i32(buf);
    buf += sizeof(uint32_t);

    ae->prev_log_index = read_i32(buf);
    buf += sizeof(uint32_t);

    ae->leader_commit = read_i32(buf);
    buf += sizeof(uint32_t);

    uint32_t entries_count = read_u32(buf);
    buf += sizeof(uint32_t);

    for (int i = 0; i < entries_count; ++i) {
        da_append(ae->entries, read_i32(buf));
        buf += sizeof(uint32_t);
    }

    return 0;
}

static int append_entries_response_read(append_entries_response_t *ae,
                                        const uint8_t *buf)
{
    ae->term = read_i32(buf);
    buf += sizeof(uint32_t);
    ae->success = read_u8(buf++);
    return 0;
}

typedef union raft_message {
    request_vote_rpc_t request_vote_rpc;
    request_vote_response_t request_vote_response;
    append_entries_rpc_t append_entries_rpc;
    append_entries_response_t append_entries_response;
} raft_message_t;

static message_type_t message_read(const uint8_t *buf, raft_message_t *rm)
{
    message_type_t message_type = read_u8(buf++);
    switch (message_type) {
    case MT_APPEND_ENTRIES_RQ:
        append_entries_rpc_read(&rm->append_entries_rpc, buf);
        break;
    case MT_APPEND_ENTRIES_RS:
        append_entries_response_read(&rm->append_entries_response, buf);
        break;
    case MT_REQUEST_VOTE_RQ:
        request_vote_rpc_read(&rm->request_vote_rpc, buf);
        break;
    case MT_REQUEST_VOTE_RS:
        request_vote_response_read(&rm->request_vote_response, buf);
        break;
    }
    return message_type;
}

/**
 ** Raft machine State
 **/

typedef enum raft_machine_state {
    RS_FOLLOWER,
    RS_CANDIDATE,
    RS_LEADER
} raft_machine_state_t;

typedef struct log_entry {
    int term;
    int value;
} log_entry_t;

typedef struct raft_state {
    raft_machine_state_t state;
    int current_term;
    int voted_for;
    struct {
        size_t length;
        size_t capacity;
        log_entry_t *items;
    } log;
    union {
        struct {
            int commit_index;
            int last_applied;
        } volatile;
        struct {
            int next_index[MAX_NODES];
            int match_index[MAX_NODES];
        } leader_volatile;
    };
} raft_state_t;

static raft_state_t raft_state = {0};
static int node_nr             = 3;
static int votes_received      = 0;
static int node_id             = 0;

static void transition_to_leader(void)
{
    raft_state.state = RS_LEADER;
    votes_received   = 0;
    printf("[INFO] Transition to LEADER\n");
}

static void transition_to_follower(int term)
{
    raft_state.state        = RS_FOLLOWER;
    raft_state.voted_for    = -1;
    raft_state.current_term = term;

    printf("[INFO] Transition to FOLLOWER\n");
}

static void transition_to_candidate(void)
{
    raft_state.state = RS_CANDIDATE;

    printf("[INFO] Transition to CANDIDATE\n");
}

typedef struct peer {
    char addr[16];
    int port;
} peer_t;

static int send_append_entries(int fd, const struct sockaddr_in *peer,
                               const append_entries_rpc_t *ae)
{
    uint8_t buf[BUFSIZ];
    size_t length = append_entries_rpc_write(&buf[0], ae);
    return sendto(fd, buf, length, 0, (struct sockaddr *)peer, sizeof(*peer));
}

static int send_append_entries_response(int fd, const struct sockaddr_in *peer,
                                        const append_entries_response_t *ae)
{
    uint8_t buf[BUFSIZ];
    size_t length = append_entries_response_write(&buf[0], ae);
    return sendto(fd, buf, length, 0, (struct sockaddr *)peer, sizeof(*peer));
}

static int send_request_vote(int fd, const struct sockaddr_in *peer,
                             const request_vote_rpc_t *rv)
{
    uint8_t buf[BUFSIZ];
    size_t length = request_vote_rpc_write(&buf[0], rv);
    return sendto(fd, buf, length, 0, (struct sockaddr *)peer, sizeof(*peer));
}

static int send_request_vote_response(int fd, const struct sockaddr_in *peer,
                                      const request_vote_response_t *rv)
{
    uint8_t buf[BUFSIZ];
    size_t length = request_vote_response_write(&buf[0], rv);
    return sendto(fd, buf, length, 0, (struct sockaddr *)peer, sizeof(*peer));
}

static void start_election(int fd, peer_t peers[NODES_COUNT])
{
    if (raft_state.state != RS_CANDIDATE)
        return;

    printf("[INFO] Start election current_term=%d\n", raft_state.current_term);
    votes_received = 1;
    raft_state.current_term++;
    raft_state.voted_for        = node_id;
    const request_vote_rpc_t rv = {
        .term         = raft_state.current_term,
        .candidate_id = node_id,
        .last_log_term =
            raft_state.log.length == 0
                ? 0
                : raft_state.log.items[raft_state.log.length - 1].term,
        .last_log_index = raft_state.log.length};
    for (int i = 0; i < NODES_COUNT; ++i) {
        if (i == node_id)
            continue;

        struct sockaddr_in peer_addr = {0};
        peer_addr.sin_family         = AF_INET;
        peer_addr.sin_port           = htons(peers[i].port);

        if (inet_pton(AF_INET, peers[i].addr, &peer_addr.sin_addr) <= 0) {
            perror("Invalid peer IP address");
            return;
        }
        if (send_request_vote(fd, &peer_addr, &rv) < 0)
            fprintf(stderr, "failed to send RequestVoteRPC to client %s: %s\n",
                    peers[i].addr, strerror(errno));
    }
}

static void handle_request_vote_rq(int fd, const struct sockaddr_in *peer,
                                   const request_vote_rpc_t *rv)
{
    printf("[INFO] Received RequestVoteRPC [current_term=%d, voted_for=%d]\n",
           raft_state.current_term, raft_state.voted_for);
    // - If current term > candidate term reply false
    // - If candidate id is unset (0) or it's set and the log is up to date with
    //   the state, reply true
    if (rv->term > raft_state.current_term) {
        printf("[INFO] Term out of date in RequestVote\n");
        transition_to_follower(rv->term);
    }
    request_vote_response_t rv_response = {0};
    if (raft_state.current_term == rv->term &&
        (raft_state.voted_for == -1 ||
         raft_state.voted_for == rv->candidate_id)) {
        rv_response.vote_granted = true;
        raft_state.voted_for     = rv->candidate_id;

    } else {
        rv_response.vote_granted = false;
    }
    rv_response.term = raft_state.current_term;
    send_request_vote_response(fd, peer, &rv_response);
}

static void handle_request_vote_rs(int fd, const struct sockaddr_in *peer,
                                   const request_vote_response_t *rv)
{
    printf("[INFO] Received RequestVoteReply(term=%d, vote_granted=%d)\n",
           rv->term, rv->vote_granted);
    // Already a leader, discard vote
    if (raft_state.state != RS_CANDIDATE)
        return;
    if (rv->term > raft_state.current_term) {
        transition_to_follower(rv->term);
    } else {
        if (rv->vote_granted)
            ++votes_received;
        if (votes_received > (node_nr / 2))
            transition_to_leader();
    }
}

static void handle_append_entries_rq(int fd, const struct sockaddr_in *peer,
                                     const append_entries_rpc_t *ae)
{
    printf("[INFO] Received AppendEntriesRPC\n");

    append_entries_response_t ae_response = {0};

    if (ae->term > raft_state.current_term) {
        printf("[INFO] Term out of date in AppendEntriesRPC\n");
        transition_to_follower(ae->term);
    }

    if (ae->term == raft_state.current_term) {
        if (raft_state.state != RS_FOLLOWER) {
            transition_to_follower(ae->term);
        }
        ae_response.success = true;
    }

    ae_response.term = raft_state.current_term;

    send_append_entries_response(fd, peer, &ae_response);
}

static void handle_append_entries_rs(int fd, const struct sockaddr_in *peer,
                                     const append_entries_response_t *ae)
{
    if (ae->term > raft_state.current_term)
        transition_to_follower(ae->term);
}

static void broadcast_heartbeat(int fd, peer_t peers[NODES_COUNT])
{
    printf("[INFO] Broadcast heartbeat\n");
    const append_entries_rpc_t ae = {.term      = raft_state.current_term,
                                     .leader_id = node_id};
    for (int i = 0; i < NODES_COUNT; ++i) {
        if (i == node_id)
            continue;

        struct sockaddr_in peer_addr = {0};
        peer_addr.sin_family         = AF_INET;
        peer_addr.sin_port           = htons(peers[i].port);

        if (inet_pton(AF_INET, peers[i].addr, &peer_addr.sin_addr) <= 0) {
            perror("Invalid peer IP address");
            return;
        }
        if (send_append_entries(fd, &peer_addr, &ae) < 0)
            fprintf(stderr,
                    "failed to send AppendEntriesRPC to client %s: %s\n",
                    peers[i].addr, strerror(errno));
    }
}

#define HEARTBEAT_TIMEOUT_S 1

static void server_start(peer_t peers[NODES_COUNT])
{
    int sock_fd = udp_listen(peers[node_id].addr, peers[node_id].port);

    fd_set read_fds;
    unsigned char buf[BUFSIZ];
    struct sockaddr_in peer_addr;

    socklen_t addr_len             = sizeof(peer_addr);
    int max_fd                     = sock_fd;
    int num_events                 = 0;
    ssize_t n                      = 0;
    time_t select_timeout_s        = HEARTBEAT_TIMEOUT_S;
    time_t last_heartbeat_s        = 0;
    time_t remaining_s             = 0;
    useconds_t select_timeout_us   = RANDOM(150000, 300000);
    useconds_t remaining_us        = 0;
    useconds_t last_update_time_us = 0;
    struct timeval tv              = {select_timeout_s, select_timeout_us};

    while (1) {
        memset(buf, 0x00, sizeof(buf));

        FD_ZERO(&read_fds);
        FD_SET(sock_fd, &read_fds);

        num_events = select(max_fd + 1, &read_fds, NULL, NULL, &tv);
        if (num_events < 0) {
            fprintf(stderr, "select() error: %s\n", strerror(errno));
            exit(EXIT_FAILURE);
        }

        if (FD_ISSET(sock_fd, &read_fds)) {
            raft_message_t raft_message;
            n = recvfrom(sock_fd, buf, sizeof(buf), 0,
                         (struct sockaddr *)&peer_addr, &addr_len);
            if (n < 0)
                fprintf(stderr, "[ERROR] recvfrom error: %s\n",
                        strerror(errno));
            message_type_t message_type = message_read(buf, &raft_message);
            switch (message_type) {
            case MT_APPEND_ENTRIES_RQ:
                last_update_time_us = get_microseconds_timestamp();
                last_heartbeat_s    = get_seconds_timestamp();
                handle_append_entries_rq(sock_fd, &peer_addr,
                                         &raft_message.append_entries_rpc);
                break;
            case MT_APPEND_ENTRIES_RS:
                handle_append_entries_rs(sock_fd, &peer_addr,
                                         &raft_message.append_entries_response);
                break;
            case MT_REQUEST_VOTE_RQ:
                handle_request_vote_rq(sock_fd, &peer_addr,
                                       &raft_message.request_vote_rpc);
                break;
            case MT_REQUEST_VOTE_RS:
                handle_request_vote_rs(sock_fd, &peer_addr,
                                       &raft_message.request_vote_response);
                if (raft_state.state == RS_LEADER) {
                    broadcast_heartbeat(sock_fd, peers);
                    last_heartbeat_s  = get_seconds_timestamp();
                    select_timeout_us = 0;
                    select_timeout_s  = HEARTBEAT_TIMEOUT_S;
                    tv.tv_sec         = select_timeout_s;
                    tv.tv_usec        = select_timeout_us;
                }
                break;
            }
        }

        // Check if the election timeout is over, if the raft_state is not
        // RS_CANDIDATE, skip it
        remaining_s = get_seconds_timestamp() - last_heartbeat_s;

        if (raft_state.state == RS_LEADER) {
            // We're in RS_LEADER state, sending heartbeats
            if (remaining_s >= select_timeout_s) {
                broadcast_heartbeat(sock_fd, peers);
                last_heartbeat_s = get_seconds_timestamp();
                tv.tv_sec        = HEARTBEAT_TIMEOUT_S;
                tv.tv_usec       = 0;
            } else {
                tv.tv_sec  = select_timeout_s - remaining_s;
                tv.tv_usec = 0;
            }
            continue;
        } else {
            remaining_us = get_microseconds_timestamp() - last_update_time_us;
            // We're in RS_CANDIDATE state, starting an election
            if (remaining_s > select_timeout_s) {
                if (remaining_us >= select_timeout_us) {
                    transition_to_candidate();
                    start_election(sock_fd, peers);
                    select_timeout_us   = RANDOM(150000, 300000);
                    last_update_time_us = get_microseconds_timestamp();
                    tv.tv_sec           = 0;
                    tv.tv_usec          = select_timeout_us;
                } else {
                    tv.tv_sec  = 0;
                    tv.tv_usec = select_timeout_us - remaining_us;
                }
            }
        }
    }
}

int main(int argc, char **argv)
{
    if (argc > 1)
        node_id = atoi(argv[1]);

    if (node_id > NODES_COUNT)
        exit(EXIT_FAILURE);

    peer_t nodes[NODES_COUNT] = {
        {"127.0.0.1", 8777}, {"127.0.0.1", 8778}, {"127.0.0.1", 8779}};

    printf("[INFO] Cluster topology\n");
    for (int i = 0; i < NODES_COUNT; ++i)
        printf("[INFO]\t - %s:%d (%d)\n", nodes[i].addr, nodes[i].port, i);

    printf("[INFO] Node %d starting\n", node_id);
    srand(time(NULL));
    server_start(nodes);

    return 0;
}
