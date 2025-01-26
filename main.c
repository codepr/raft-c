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
 ** Binary protocol utility functions
 **
 ** A simple binary protocol to communicate over the wire. The RPC calls
 ** are pretty simple and easy to serialize.
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
    uint32_t i2 = ((int64_t)buf[0] << 24) | ((int32_t)buf[1] << 16) |
                  ((int64_t)buf[2] << 8) | buf[3];
    int32_t val;

    // change unsigned numbers to signed
    if (i2 <= 0x7fffffffu)
        val = i2;
    else
        val = -1 - (int64_t)(0xffffffffu - i2);

    return val;
}

// write_u64() -- store a 64-bit int into a char buffer (like htonl())
static int write_u64(uint8_t *buf, uint64_t val)
{
    *buf++ = val >> 56;
    *buf++ = val >> 48;
    *buf++ = val >> 40;
    *buf++ = val >> 32;
    *buf++ = val >> 24;
    *buf++ = val >> 16;
    *buf++ = val >> 8;
    *buf++ = val;

    return sizeof(uint64_t);
}

// read_u64() -- unpack a 64-bit unsigned from a char buffer (like ntohl())
uint64_t read_u64(const uint8_t *buf)
{
    return ((uint64_t)buf[0] << 56) | ((uint64_t)buf[1] << 48) |
           ((uint64_t)buf[2] << 40) | ((uint64_t)buf[3] << 32) |
           ((uint64_t)buf[4] << 24) | ((uint64_t)buf[5] << 16) |
           ((uint64_t)buf[6] << 8) | buf[7];
}

/**
 ** Cluster communication and management - UDP network utility functions
 **
 ** The RAFT machine will be implemented by using UDP as the transport layer,
 ** making it lightweight and connection-less. We're don't really care about
 ** delivery guarantee ** and the communication is simplified.
 **/

#define BACKLOG     128
#define MAX_NODES   15
#define NODES_COUNT 3

typedef struct peer {
    char addr[16];
    int port;
} peer_t;

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

    // Converts the time to microseconds
    return (unsigned long long)(ts.tv_sec * 1000000 + ts.tv_nsec / 1000);
}

static unsigned long long get_seconds_timestamp(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);

    // Returns the time in seconds
    return (unsigned long long)ts.tv_sec;
}

/**
 ** RPC structure definition
 **
 ** - RequestVoteRPC     used when an election starts
 ** - RequestVoteReply   reply to a RequestVoteRPC with the vote to elect a
 **                      leader
 ** - AppendEntriesRPC   Used for both heartbeats (empty, coming from the leader
 **                      to prevent elections, and to keep the log up to date in
 **                      the cluster)
 ** - AppendEntriesReply reply to an AppendEntriesRPC
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
        log_entry_t *items;
    } *entries;
} append_entries_rpc_t;

typedef struct append_entries_response {
    int term;
    bool success;
} append_entries_response_t;

static int request_vote_rpc_write(uint8_t *buf, const request_vote_rpc_t *rv);
static int request_vote_response_write(uint8_t *buf,
                                       const request_vote_response_t *rv);
static int request_vote_rpc_read(request_vote_rpc_t *rv, const uint8_t *buf);
static int request_vote_response_read(request_vote_response_t *rv,
                                      const uint8_t *buf);
static int append_entries_rpc_write(uint8_t *buf,
                                    const append_entries_rpc_t *ae);
static int append_entries_response_write(uint8_t *buf,
                                         const append_entries_response_t *ae);
static int append_entries_rpc_read(append_entries_rpc_t *ae,
                                   const uint8_t *buf);
static int append_entries_response_read(append_entries_response_t *ae,
                                        const uint8_t *buf);
static void start_election(int fd, peer_t peers[NODES_COUNT]);

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

#define MAX_PENDING_REQUESTS 256
#define ELECTION_TIMEOUT()   RANDOM(150000, 300000);

// Simple wrapper for a generic RAFT
// message

typedef struct raft_message {
    uint64_t request_id;
    message_type_t type;
    union {
        request_vote_rpc_t request_vote_rpc;
        request_vote_response_t request_vote_response;
        append_entries_rpc_t append_entries_rpc;
        append_entries_response_t append_entries_response;
    };
} raft_message_t;

static int send_raft_message(int fd, const struct sockaddr_in *peer,
                             const raft_message_t *rm);

static ssize_t raft_message_write(uint8_t *buf, const raft_message_t *rm)
{
    ssize_t bytes = write_u8(buf++, rm->type);
    bytes += write_u64(buf, rm->request_id);
    buf += sizeof(uint64_t);
    switch (rm->type) {
    case MT_APPEND_ENTRIES_RQ:
        bytes += append_entries_rpc_write(buf, &rm->append_entries_rpc);
        break;
    case MT_APPEND_ENTRIES_RS:
        bytes +=
            append_entries_response_write(buf, &rm->append_entries_response);
        break;
    case MT_REQUEST_VOTE_RQ:
        bytes += request_vote_rpc_write(buf, &rm->request_vote_rpc);
        break;
    case MT_REQUEST_VOTE_RS:
        bytes += request_vote_response_write(buf, &rm->request_vote_response);
        break;
    }

    return bytes;
}

static message_type_t raft_message_read(const uint8_t *buf, raft_message_t *rm)
{
    rm->type       = read_u8(buf++);
    rm->request_id = read_u64(buf);
    buf += sizeof(uint64_t);
    switch (rm->type) {
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
    return rm->type;
}

typedef struct {
    uint64_t request_id;
    struct sockaddr_in peer;
    time_t timestamp;
    raft_message_t message;
} pending_request_t;

typedef struct {
    raft_state_t machine;
    peer_t nodes[NODES_COUNT];
    pending_request_t pending_requests[MAX_PENDING_REQUESTS];
    uint64_t next_request_id;
    int votes_received;
    int node_id;
} consensus_module_t;

static consensus_module_t cm = {0};

static void transition_to_leader(void)
{
    cm.machine.state  = RS_LEADER;
    cm.votes_received = 0;
    printf("[INFO] Transition to LEADER\n");
}

static void transition_to_follower(int term)
{
    cm.machine.state        = RS_FOLLOWER;
    cm.machine.voted_for    = -1;
    cm.machine.current_term = term;

    printf("[INFO] Transition to "
           "FOLLOWER\n");
}

static void transition_to_candidate(void)
{
    cm.machine.state = RS_CANDIDATE;

    printf("[INFO] Transition to "
           "CANDIDATE\n");
}

static int request_vote_rpc_write(uint8_t *buf, const request_vote_rpc_t *rv)
{
    // Message type
    // int bytes = write_u8(buf++, MT_REQUEST_VOTE_RQ);

    // term
    int bytes = write_i32(buf, rv->term);
    buf += sizeof(int32_t);

    // candidate_id
    bytes += write_i32(buf, rv->candidate_id);
    buf += sizeof(int32_t);

    // last_log_term
    bytes += write_i32(buf, rv->last_log_term);
    buf += sizeof(int32_t);

    // last_log_index
    bytes += write_i32(buf, rv->last_log_index);
    buf += sizeof(int32_t);

    return bytes;
}

static int request_vote_response_write(uint8_t *buf,
                                       const request_vote_response_t *rv)
{
    // Message type
    // int bytes = write_u8(buf++, MT_REQUEST_VOTE_RS);

    // term
    int bytes = write_i32(buf, rv->term);
    buf += sizeof(int32_t);

    bytes += write_u8(buf++, rv->vote_granted);

    return bytes;
}

static int request_vote_rpc_read(request_vote_rpc_t *rv, const uint8_t *buf)
{
    rv->term = read_i32(buf);
    buf += sizeof(int32_t);

    rv->candidate_id = read_i32(buf);
    buf += sizeof(int32_t);

    rv->last_log_term = read_i32(buf);
    buf += sizeof(int32_t);

    rv->last_log_index = read_i32(buf);
    buf += sizeof(int32_t);

    return 0;
}

static int request_vote_response_read(request_vote_response_t *rv,
                                      const uint8_t *buf)
{
    rv->term = read_i32(buf);
    buf += sizeof(int32_t);
    rv->vote_granted = read_u8(buf++);
    return 0;
}

static int append_entries_rpc_write(uint8_t *buf,
                                    const append_entries_rpc_t *ae)
{ // Message type
    // int bytes = write_u8(buf++, MT_APPEND_ENTRIES_RQ);

    // term
    int bytes = write_i32(buf, ae->term);
    buf += sizeof(int32_t);

    // leader_id
    bytes += write_i32(buf, ae->leader_id);
    buf += sizeof(int32_t);

    // prev_log_term
    bytes += write_i32(buf, ae->prev_log_term);
    buf += sizeof(int32_t);

    // prev_log_index
    bytes += write_i32(buf, ae->prev_log_index);
    buf += sizeof(int32_t);

    // leader_commit
    bytes += write_i32(buf, ae->leader_commit);
    buf += sizeof(int32_t);

    if (ae->entries) {
        // entries count
        bytes += write_u32(buf, ae->entries->length);
        buf += sizeof(uint32_t);

        // entries
        for (size_t i = 0; i < ae->entries->length; ++i) {
            bytes += write_i32(buf, ae->entries->items[i].term);
            buf += sizeof(int32_t);
            bytes += write_i32(buf, ae->entries->items[i].value);
            buf += sizeof(int32_t);
        }
    }

    return bytes;
}

static int append_entries_response_write(uint8_t *buf,
                                         const append_entries_response_t *ae)
{
    // Message type
    // int bytes = write_u8(buf++, MT_APPEND_ENTRIES_RS);

    // term
    int bytes = write_i32(buf, ae->term);
    buf += sizeof(int32_t);

    bytes += write_u8(buf++, ae->success);

    return bytes;
}

static int append_entries_rpc_read(append_entries_rpc_t *ae, const uint8_t *buf)
{
    ae->term = read_i32(buf);
    buf += sizeof(int32_t);

    ae->leader_id = read_i32(buf);
    buf += sizeof(int32_t);

    ae->prev_log_term = read_i32(buf);
    buf += sizeof(int32_t);

    ae->prev_log_index = read_i32(buf);
    buf += sizeof(int32_t);

    ae->leader_commit = read_i32(buf);
    buf += sizeof(int32_t);

    uint32_t entries_count = read_u32(buf);
    buf += sizeof(uint32_t);

    for (int i = 0; i < entries_count; ++i) {
        log_entry_t entry;
        entry.term = read_i32(buf);
        buf += sizeof(int32_t);
        entry.value = read_i32(buf);
        buf += sizeof(int32_t);
        da_append(ae->entries, entry);
    }

    return 0;
}

static int append_entries_response_read(append_entries_response_t *ae,
                                        const uint8_t *buf)
{
    ae->term = read_i32(buf);
    buf += sizeof(int32_t);
    ae->success = read_u8(buf++);
    return 0;
}

static int send_raft_message(int fd, const struct sockaddr_in *peer,
                             const raft_message_t *rm)
{
    uint8_t buf[BUFSIZ] = {0};
    ssize_t length      = raft_message_write(&buf[0], rm);
    return sendto(fd, buf, length, 0, (struct sockaddr *)peer, sizeof(*peer));
}

static void start_election(int fd, peer_t peers[NODES_COUNT])
{
    if (cm.machine.state != RS_CANDIDATE)
        return;

    printf("[INFO] Start election "
           "current_term=%d\n",
           cm.machine.current_term);
    cm.votes_received = 1;
    cm.machine.current_term++;
    cm.machine.voted_for   = cm.node_id;

    raft_message_t message = {
        .type             = MT_REQUEST_VOTE_RQ,
        .request_vote_rpc = {
            .term         = cm.machine.current_term,
            .candidate_id = cm.node_id,
            .last_log_term =
                cm.machine.log.length == 0
                    ? 0
                    : cm.machine.log.items[cm.machine.log.length - 1].term,
            .last_log_index = cm.machine.log.length}};
    for (int i = 0; i < NODES_COUNT; ++i) {
        if (i == cm.node_id)
            continue;

        struct sockaddr_in peer_addr = {0};
        peer_addr.sin_family         = AF_INET;
        peer_addr.sin_port           = htons(peers[i].port);

        if (inet_pton(AF_INET, peers[i].addr, &peer_addr.sin_addr) <= 0) {
            perror("Invalid peer IP "
                   "address");
            return;
        }

        message.request_id = cm.next_request_id++;
        if (send_raft_message(fd, &peer_addr, &message) < 0)
            fprintf(stderr,
                    "failed to send "
                    "RequestVoteRPC to "
                    "client %s: %s\n",
                    peers[i].addr, strerror(errno));
    }
}

static void handle_request_vote_rq(int fd, const struct sockaddr_in *peer,
                                   const request_vote_rpc_t *rv)
{
    printf("[INFO] Received RequestVoteRPC "
           "[current_term=%d, "
           "voted_for=%d]\n",
           cm.machine.current_term, cm.machine.voted_for);
    // - If current term > candidate term
    // reply false
    // - If candidate id is unset (0) or
    // it's set and the log is up to date
    // with
    //   the state, reply true
    if (rv->term > cm.machine.current_term) {
        printf("[INFO] Term out of date "
               "in RequestVote\n");
        transition_to_follower(rv->term);
    }
    request_vote_response_t rv_response = {0};
    if (cm.machine.current_term == rv->term &&
        (cm.machine.voted_for == -1 ||
         cm.machine.voted_for == rv->candidate_id)) {
        rv_response.vote_granted = true;
        cm.machine.voted_for     = rv->candidate_id;

    } else {
        rv_response.vote_granted = false;
    }
    rv_response.term             = cm.machine.current_term;

    const raft_message_t message = {.type       = MT_REQUEST_VOTE_RS,
                                    .request_id = cm.next_request_id++,
                                    .request_vote_response = rv_response};
    if (send_raft_message(fd, peer, &message) < 0)
        fprintf(stderr,
                "failed to send "
                "RequestVoteReply to "
                "client %d: %s\n",
                peer->sin_addr.s_addr, strerror(errno));
}

static void handle_request_vote_rs(int fd, const struct sockaddr_in *peer,
                                   const request_vote_response_t *rv)
{
    printf("[INFO] Received "
           "RequestVoteReply(term=%d, "
           "vote_granted=%d)\n",
           rv->term, rv->vote_granted);
    // Already a leader, discard vote
    if (cm.machine.state != RS_CANDIDATE)
        return;
    if (rv->term > cm.machine.current_term) {
        transition_to_follower(rv->term);
    } else {
        if (rv->vote_granted)
            ++cm.votes_received;
        if (cm.votes_received > (NODES_COUNT / 2))
            transition_to_leader();
    }
}

static void handle_append_entries_rq(int fd, const struct sockaddr_in *peer,
                                     const append_entries_rpc_t *ae)
{
    printf("[INFO] Received "
           "AppendEntriesRPC\n");

    append_entries_response_t ae_response = {0};

    if (ae->term > cm.machine.current_term) {
        printf("[INFO] Term out of date "
               "in AppendEntriesRPC\n");
        transition_to_follower(ae->term);
    }

    if (ae->term == cm.machine.current_term) {
        if (cm.machine.state != RS_FOLLOWER) {
            transition_to_follower(ae->term);
        }
        ae_response.success = true;
    }

    ae_response.term             = cm.machine.current_term;

    const raft_message_t message = {.type       = MT_APPEND_ENTRIES_RS,
                                    .request_id = cm.next_request_id++,
                                    .append_entries_response = ae_response};

    if (send_raft_message(fd, peer, &message) < 0)
        fprintf(stderr,
                "failed to send "
                "AppendEntriesReply to "
                "client %d: %s\n",
                peer->sin_addr.s_addr, strerror(errno));
}

static void handle_append_entries_rs(int fd, const struct sockaddr_in *peer,
                                     const append_entries_response_t *ae)
{
    if (ae->term > cm.machine.current_term) {
        transition_to_follower(ae->term);
        return;
    }

    // TODO get peer ID from nodes
    int peer_id = 0;
    // for (int i = 0; i < NODES_COUNT;
    // ++i) {
    //     if (strncmp(nodes[i].addr,
    //     peer->sin_addr
    // }

    if (cm.machine.state == RS_LEADER && cm.machine.current_term == ae->term) {
        if (ae->success) {
            cm.machine.leader_volatile.next_index[peer_id] =
                cm.machine.log.length;
            cm.machine.leader_volatile.match_index[peer_id] =
                cm.machine.leader_volatile.next_index[peer_id] - 1;
            // TODO finish state update
        } else {
            cm.machine.leader_volatile.next_index[peer_id]--;
        }
    }
}

static void broadcast_heartbeat(int fd, peer_t peers[NODES_COUNT])
{
    printf("[INFO] Broadcast heartbeat\n");
    raft_message_t message = {
        .type               = MT_APPEND_ENTRIES_RQ,
        .append_entries_rpc = {.term      = cm.machine.current_term,
                               .leader_id = cm.node_id}};
    for (int i = 0; i < NODES_COUNT; ++i) {
        if (i == cm.node_id)
            continue;

        int prev_log_index = cm.machine.leader_volatile.next_index[i] - 1;
        int prev_log_term  = prev_log_index >= 0
                                 ? cm.machine.log.items[prev_log_index].term
                                 : -1;

        message.request_id = cm.next_request_id++,
        message.append_entries_rpc.prev_log_index = prev_log_index;
        message.append_entries_rpc.prev_log_term  = prev_log_term;
        message.append_entries_rpc.leader_commit  = cm.machine.commit_index;

        for (int j = cm.machine.leader_volatile.next_index[i];
             j < cm.machine.log.length; ++j) {
            da_append(message.append_entries_rpc.entries,
                      cm.machine.log.items[j]);
        }

        struct sockaddr_in peer_addr = {0};
        peer_addr.sin_family         = AF_INET;
        peer_addr.sin_port           = htons(peers[i].port);

        if (inet_pton(AF_INET, peers[i].addr, &peer_addr.sin_addr) <= 0) {
            perror("Invalid peer IP "
                   "address");
            return;
        }
        if (send_raft_message(fd, &peer_addr, &message) < 0)
            fprintf(stderr,
                    "failed to send "
                    "AppendEntriesRPC to "
                    "client %s: %s\n",
                    peers[i].addr, strerror(errno));
    }
}

#define HEARTBEAT_TIMEOUT_S 1

static void server_start(peer_t peers[NODES_COUNT])
{
    int sock_fd = udp_listen(peers[cm.node_id].addr, peers[cm.node_id].port);

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
    useconds_t select_timeout_us   = ELECTION_TIMEOUT();
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
                fprintf(stderr,
                        "[ERROR] recvfrom "
                        "error: %s\n",
                        strerror(errno));
            message_type_t message_type = raft_message_read(buf, &raft_message);
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
                if (cm.machine.state == RS_LEADER) {
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

        // Check if the election timeout
        // is over, if the raft_state is
        // not RS_CANDIDATE, skip it
        remaining_s = get_seconds_timestamp() - last_heartbeat_s;

        if (cm.machine.state == RS_LEADER) {
            // We're in RS_LEADER state,
            // sending heartbeats
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
            // We're in RS_CANDIDATE
            // state, starting an election
            if (remaining_s > select_timeout_s) {
                if (remaining_us >= select_timeout_us) {
                    transition_to_candidate();
                    start_election(sock_fd, peers);
                    select_timeout_us   = ELECTION_TIMEOUT();
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
    cm.nodes[0] = (peer_t){"127.0.0.1", 8777};
    cm.nodes[1] = (peer_t){"127.0.0.1", 8778};
    cm.nodes[2] = (peer_t){"127.0.0.1", 8779};

    if (argc > 1)
        cm.node_id = atoi(argv[1]);

    if (cm.node_id > NODES_COUNT)
        exit(EXIT_FAILURE);

    printf("[INFO] Cluster topology\n");
    for (int i = 0; i < NODES_COUNT; ++i)
        printf("[INFO]\t - %s:%d (%d)\n", cm.nodes[i].addr, cm.nodes[i].port,
               i);

    printf("[INFO] Node %d starting\n", cm.node_id);
    srand(time(NULL));
    server_start(cm.nodes);

    return 0;
}
