#include "raft.h"
#include <arpa/inet.h>
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
#include <time.h>
#include <unistd.h>

#define RANDOM(a, b)       rand() % ((b) + 1 - (a)) + (a)
#define ELECTION_TIMEOUT() RANDOM(150000, 300000);

/**
 ** Dynamic array utility macros
 **/

#define da_extend(da)                                                          \
    do {                                                                       \
        (da)->capacity += 1;                                                   \
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
        if ((da)->length + 1 >= (da)->capacity)                                \
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
    uint32_t i2 = ((int64_t)buf[0] << 24) | ((int64_t)buf[1] << 16) |
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

#define IP_LENGTH   16
#define BACKLOG     128
#define NODES_COUNT 3
// TODO temporary
static int registered_nodes = 0;

typedef struct peer {
    struct sockaddr_in addr;
    time_t last_active;
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

static char *get_ip_str(const struct sockaddr_in *sa, char *s, size_t maxlen)
{
    switch (sa->sin_family) {
    case AF_INET:
        inet_ntop(AF_INET, &sa->sin_addr, s, maxlen);
        break;

    default:
        strncpy(s, "Unknown AF", maxlen);
        return NULL;
    }

    return s;
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

typedef struct request_vote_reply {
    int term;
    bool vote_granted;
} request_vote_reply_t;

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
    } entries;
} append_entries_rpc_t;

typedef struct append_entries_reply {
    int term;
    bool success;
} append_entries_reply_t;

static int request_vote_rpc_write(uint8_t *buf, const request_vote_rpc_t *rv);
static int request_vote_reply_write(uint8_t *buf,
                                    const request_vote_reply_t *rv);
static int request_vote_rpc_read(request_vote_rpc_t *rv, const uint8_t *buf);
static int request_vote_reply_read(request_vote_reply_t *rv,
                                   const uint8_t *buf);
static int append_entries_rpc_write(uint8_t *buf,
                                    const append_entries_rpc_t *ae);
static int append_entries_reply_write(uint8_t *buf,
                                      const append_entries_reply_t *ae);
static int append_entries_rpc_read(append_entries_rpc_t *ae,
                                   const uint8_t *buf);
static int append_entries_reply_read(append_entries_reply_t *ae,
                                     const uint8_t *buf);
static void start_election(int fd);

/**
 ** Raft machine State
 **/

typedef enum raft_machine_state {
    RS_FOLLOWER,
    RS_CANDIDATE,
    RS_LEADER
} raft_machine_state_t;

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
            int next_index[NODES_COUNT];
            int match_index[NODES_COUNT];
        } leader_volatile;
    };
} raft_state_t;

// Simple wrapper for a generic RAFT
// message

typedef struct raft_message {
    uint64_t request_id;
    message_type_t type;
    union {
        request_vote_rpc_t request_vote_rpc;
        request_vote_reply_t request_vote_reply;
        append_entries_rpc_t append_entries_rpc;
        append_entries_reply_t append_entries_reply;
    };
} raft_message_t;

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
        bytes += append_entries_reply_write(buf, &rm->append_entries_reply);
        break;
    case MT_REQUEST_VOTE_RQ:
        bytes += request_vote_rpc_write(buf, &rm->request_vote_rpc);
        break;
    case MT_REQUEST_VOTE_RS:
        bytes += request_vote_reply_write(buf, &rm->request_vote_reply);
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
        append_entries_reply_read(&rm->append_entries_reply, buf);
        break;
    case MT_REQUEST_VOTE_RQ:
        request_vote_rpc_read(&rm->request_vote_rpc, buf);
        break;
    case MT_REQUEST_VOTE_RS:
        request_vote_reply_read(&rm->request_vote_reply, buf);
        break;
    }
    return rm->type;
}

typedef struct {
    uint64_t request_id;
    struct sockaddr_in peer;
    time_t timestamp;
    int last_log_entry_index;
    int entries_length;
} pending_request_t;

typedef struct {
    raft_state_t machine;
    peer_t nodes[NODES_COUNT];
    struct {
        size_t length;
        size_t capacity;
        pending_request_t *items;
    } pending_requests;
    uint64_t next_request_id;
    int votes_received;
    int node_id;
} consensus_module_t;

static consensus_module_t cm = {0};

static void transition_to_leader(void)
{
    cm.machine.state  = RS_LEADER;
    cm.votes_received = 0;
    for (int i = 0; i < NODES_COUNT; ++i) {
        cm.machine.leader_volatile.next_index[i]  = cm.machine.log.length;
        cm.machine.leader_volatile.match_index[i] = -1;
    }
    printf("[INFO] Transition to LEADER\n");
}

static void transition_to_follower(int term)
{
    cm.machine.state        = RS_FOLLOWER;
    cm.machine.voted_for    = -1;
    cm.machine.current_term = term;

    printf("[INFO] Transition to FOLLOWER\n");
}

static void transition_to_candidate(void)
{
    cm.machine.state = RS_CANDIDATE;

    printf("[INFO] Transition to CANDIDATE\n");
}

static int last_log_term(void)
{
    return cm.machine.log.length == 0
               ? 0
               : cm.machine.log.items[cm.machine.log.length - 1].term;
}

static int last_log_index(void) { return cm.machine.log.length - 1; }

static int request_vote_rpc_write(uint8_t *buf, const request_vote_rpc_t *rv)
{
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

static int request_vote_reply_write(uint8_t *buf,
                                    const request_vote_reply_t *rv)
{
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

static int request_vote_reply_read(request_vote_reply_t *rv, const uint8_t *buf)
{
    rv->term = read_i32(buf);
    buf += sizeof(int32_t);
    rv->vote_granted = read_u8(buf++);
    return 0;
}

static int append_entries_rpc_write(uint8_t *buf,
                                    const append_entries_rpc_t *ae)
{
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

    if (ae->entries.capacity) {
        // entries count
        bytes += write_u32(buf, ae->entries.length);
        buf += sizeof(uint32_t);

        // entries
        for (size_t i = 0; i < ae->entries.length; ++i) {
            bytes += write_i32(buf, ae->entries.items[i].term);
            buf += sizeof(int32_t);
            bytes += write_i32(buf, ae->entries.items[i].value);
            buf += sizeof(int32_t);
        }
    }

    return bytes;
}

static int append_entries_reply_write(uint8_t *buf,
                                      const append_entries_reply_t *ae)
{
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
        da_append(&ae->entries, entry);
    }

    return 0;
}

static int append_entries_reply_read(append_entries_reply_t *ae,
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

    if (rm->type == MT_APPEND_ENTRIES_RQ) {
        // Record pending request
        const pending_request_t pending_request = {
            .request_id           = rm->request_id,
            .peer                 = *peer,
            .timestamp            = time(NULL),
            .entries_length       = rm->append_entries_rpc.entries.length,
            .last_log_entry_index = rm->append_entries_rpc.prev_log_index +
                                    rm->append_entries_rpc.entries.length};

        da_append(&cm.pending_requests, pending_request);
    }

    ssize_t length = raft_message_write(&buf[0], rm);
    return sendto(fd, buf, length, 0, (struct sockaddr *)peer, sizeof(*peer));
}

static void start_election(int fd)
{
    if (cm.machine.state != RS_CANDIDATE)
        return;

    printf("[INFO] Start election current_term=%d\n", cm.machine.current_term);
    cm.votes_received = 1;
    cm.machine.current_term++;
    cm.machine.voted_for   = cm.node_id;

    raft_message_t message = {
        .type             = MT_REQUEST_VOTE_RQ,
        .request_vote_rpc = {.term           = cm.machine.current_term,
                             .candidate_id   = cm.node_id,
                             .last_log_term  = last_log_term(),
                             .last_log_index = last_log_index()}};
    for (int i = 0; i < NODES_COUNT; ++i) {
        if (i == cm.node_id)
            continue;

        message.request_id = cm.next_request_id++;
        if (send_raft_message(fd, &cm.nodes[i].addr, &message) < 0)
            fprintf(stderr, "failed to send RequestVoteRPC to client %i: %s\n",
                    cm.nodes[i].addr.sin_addr.s_addr, strerror(errno));
    }
}

static void handle_request_vote_rq(int fd, const struct sockaddr_in *peer,
                                   const request_vote_rpc_t *rv)
{
    printf("[INFO] Received RequestVoteRPC [current_term=%d, voted_for=%d]\n",
           cm.machine.current_term, cm.machine.voted_for);
    // - If current term > candidate term
    // reply false
    // - If candidate id is unset (0) or
    // it's set and the log is up to date
    // with
    //   the state, reply true
    if (rv->term > cm.machine.current_term) {
        printf("[INFO] Term out of date in RequestVote\n");
        transition_to_follower(rv->term);
    }

    request_vote_reply_t rv_reply = {0};
    if (cm.machine.current_term == rv->term &&
        (cm.machine.voted_for == -1 ||
         cm.machine.voted_for == rv->candidate_id) &&
        (rv->last_log_term > last_log_term() ||
         (rv->last_log_term == last_log_term() &&
          rv->last_log_index >= last_log_index()))) {
        rv_reply.vote_granted = true;
        cm.machine.voted_for  = rv->candidate_id;

    } else {
        rv_reply.vote_granted = false;
    }
    rv_reply.term                = cm.machine.current_term;

    const raft_message_t message = {.type               = MT_REQUEST_VOTE_RS,
                                    .request_id         = cm.next_request_id++,
                                    .request_vote_reply = rv_reply};
    if (send_raft_message(fd, peer, &message) < 0)
        fprintf(stderr, "failed to send RequestVoteReply to client %d: %s\n",
                peer->sin_addr.s_addr, strerror(errno));
}

static void handle_request_vote_rs(int fd, const struct sockaddr_in *peer,
                                   const request_vote_reply_t *rv)
{
    printf("[INFO] Received RequestVoteReply(term=%d, vote_granted=%d)\n",
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
                                     const append_entries_rpc_t *ae,
                                     uint64_t request_id)
{
    printf("[INFO] Received AppendEntriesRPC");
    for (int i = 0; i < ae->entries.length; ++i)
        printf("(term=%d, value=%d) ", ae->entries.items[i].term,
               ae->entries.items[i].value);
    printf("\n");

    append_entries_reply_t ae_reply = {0};

    if (ae->term > cm.machine.current_term) {
        printf("[INFO] Term out of date in AppendEntriesRPC\n");
        transition_to_follower(ae->term);
    }

    if (ae->term == cm.machine.current_term) {
        if (cm.machine.state != RS_FOLLOWER) {
            transition_to_follower(ae->term);
        }

        if (ae->prev_log_index == -1 ||
            (cm.machine.log.capacity &&
             ((ae->prev_log_index < cm.machine.log.length) &&
              ae->prev_log_term ==
                  cm.machine.log.items[ae->prev_log_index].term))) {
            ae_reply.success     = true;

            int log_insert_index = ae->prev_log_index + 1;
            int new_entris_index = 0;

            if (ae->entries.capacity && cm.machine.log.capacity) {
                while (1) {
                    if (log_insert_index >= cm.machine.log.length ||
                        new_entris_index >= ae->entries.length)
                        break;
                    if (cm.machine.log.items[log_insert_index].term !=
                        ae->entries.items[new_entris_index].term)
                        break;
                    log_insert_index++;
                    new_entris_index++;
                }

                if (new_entris_index < ae->entries.length) {
                    for (int i = log_insert_index; i < ae->entries.length;
                         ++i, new_entris_index++) {
                        cm.machine.log.items[i] =
                            ae->entries.items[new_entris_index];
                    }
                }
            }

            if (ae->leader_commit > cm.machine.commit_index) {
                cm.machine.commit_index =
                    ae->leader_commit < cm.machine.log.length - 1
                        ? ae->leader_commit
                        : cm.machine.log.length - 1;
            }
        }
    }
    ae_reply.term                = cm.machine.current_term;

    const raft_message_t message = {.type       = MT_APPEND_ENTRIES_RS,
                                    .request_id = request_id,
                                    .append_entries_reply = ae_reply};

    if (send_raft_message(fd, peer, &message) < 0)
        fprintf(stderr, "failed to send AppendEntriesReply to client %d: %s\n",
                peer->sin_addr.s_addr, strerror(errno));
}

int find_peer_index(const struct sockaddr_in *peer)
{
    for (int i = 0; i < NODES_COUNT; ++i) {
        if (cm.nodes[i].addr.sin_addr.s_addr == peer->sin_addr.s_addr &&
            htons(cm.nodes[i].addr.sin_port) == htons(peer->sin_port)) {
            return i;
        }
    }
    return -1;
}

static int remove_pending_request(const pending_request_t *req)
{
    for (int i = 0; i < cm.pending_requests.length; ++i) {
        if (cm.pending_requests.items[i].request_id == req->request_id) {
            // Shift remaining requests to fill the gap
            memmove(&cm.pending_requests.items[i],
                    &cm.pending_requests.items[i + 1],
                    (cm.pending_requests.length - i - 1) *
                        sizeof(pending_request_t));
            cm.pending_requests.length--;
            return 0;
        }
    }
    return -1; // Not found
}

static pending_request_t *find_pending_request(uint64_t request_id)
{
    for (int i = 0; i < cm.pending_requests.length; ++i) {
        if (cm.pending_requests.items[i].request_id == request_id)
            return &cm.pending_requests.items[i];
    }
    return NULL;
}

static void handle_append_entries_rs(int fd, const struct sockaddr_in *peer,
                                     const append_entries_reply_t *ae,
                                     uint64_t request_id)
{
    if (ae->term > cm.machine.current_term) {
        transition_to_follower(ae->term);
        return;
    }

    int peer_id = find_peer_index(peer);
    if (peer_id < 0) {
        printf("[ERROR] Could not find peer ID for reply\n");
        return;
    }

    pending_request_t *pending_request = find_pending_request(request_id);
    if (pending_request) {
        printf("[INFO] Update pending request %llu\n", request_id);
        cm.machine.leader_volatile.next_index[peer_id] =
            pending_request->entries_length;
        cm.machine.leader_volatile.match_index[peer_id] =
            pending_request->last_log_entry_index;

        remove_pending_request(pending_request);
    }

    if (cm.machine.state == RS_LEADER && cm.machine.current_term == ae->term) {
        if (ae->success) {
            cm.machine.leader_volatile.next_index[peer_id] =
                cm.machine.log.length == 0 ? 0 : cm.machine.log.length;
            cm.machine.leader_volatile.match_index[peer_id] =
                cm.machine.leader_volatile.next_index[peer_id] == -1
                    ? -1
                    : cm.machine.leader_volatile.next_index[peer_id] - 1;

            int current_commit_index = cm.machine.commit_index;
            for (int i = cm.machine.commit_index + 1; i < cm.machine.log.length;
                 ++i) {
                if (cm.machine.log.items[i].term == cm.machine.current_term) {
                    int match_count = 1;
                    for (int j = 0; j < NODES_COUNT; ++j) {
                        if (cm.machine.leader_volatile.match_index[i] >= i)
                            match_count++;
                    }
                    if (match_count * 2 > NODES_COUNT + 1)
                        cm.machine.commit_index = i;
                }
            }
            if (cm.machine.commit_index != current_commit_index) {
                printf("[INFO] Leader sets commit_index=%d\n",
                       cm.machine.commit_index);
            }
        } else {
            if (cm.machine.leader_volatile.next_index[peer_id] > 0)
                cm.machine.leader_volatile.next_index[peer_id]--;
        }
    }
}

static void broadcast_heartbeat(int fd)
{
    printf("[INFO] Broadcast heartbeat\n");
    raft_message_t message = {
        .type               = MT_APPEND_ENTRIES_RQ,
        .append_entries_rpc = {.term      = cm.machine.current_term,
                               .leader_id = cm.node_id}};
    for (int i = 0; i < NODES_COUNT; ++i) {
        if (i == cm.node_id)
            continue;

        int prev_log_index = cm.machine.leader_volatile.next_index[i] > 0
                                 ? cm.machine.leader_volatile.next_index[i] - 1
                                 : 0;
        int prev_log_term  = cm.machine.log.length && prev_log_index >= 0
                                 ? cm.machine.log.items[prev_log_index].term
                                 : -1;

        message.request_id = cm.next_request_id++,
        message.append_entries_rpc.prev_log_index = prev_log_index;
        message.append_entries_rpc.prev_log_term  = prev_log_term;
        message.append_entries_rpc.leader_commit  = cm.machine.commit_index;

        for (int j = cm.machine.leader_volatile.next_index[i];
             j < cm.machine.log.length; ++j) {
            da_append(&message.append_entries_rpc.entries,
                      cm.machine.log.items[j]);
        }

        if (send_raft_message(fd, &cm.nodes[i].addr, &message) < 0)
            fprintf(stderr,
                    "failed to send AppendEntriesRPC to client %i: %s\n",
                    cm.nodes[i].addr.sin_addr.s_addr, strerror(errno));
    }
}

#define HEARTBEAT_TIMEOUT_S 1

void raft_register_node(const char *addr, int port)
{
    struct sockaddr_in s_addr = {0};
    s_addr.sin_family         = AF_INET;
    s_addr.sin_port           = htons(port);

    if (inet_pton(AF_INET, addr, &s_addr.sin_addr) <= 0) {
        perror("Invalid peer IP address");
        return;
    }
    cm.nodes[registered_nodes++] =
        (peer_t){.addr = s_addr, .last_active = time(NULL)};
}

void raft_server_start(int node_id)
{
    cm.node_id = node_id;
    char ip[IP_LENGTH];
    get_ip_str(&cm.nodes[cm.node_id].addr, ip, IP_LENGTH);
    printf("[INFO] UDP listen on %s:%d\n", ip,
           htons(cm.nodes[cm.node_id].addr.sin_port));
    int sock_fd = udp_listen(ip, htons(cm.nodes[cm.node_id].addr.sin_port));

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
            raft_message_t raft_message = {0};
            n                           = recvfrom(sock_fd, buf, sizeof(buf), 0,
                                                   (struct sockaddr *)&peer_addr, &addr_len);
            if (n < 0)
                fprintf(stderr, "[ERROR] recvfrom error: %s\n",
                        strerror(errno));
            message_type_t message_type = raft_message_read(buf, &raft_message);
            switch (message_type) {
            case MT_APPEND_ENTRIES_RQ:
                last_update_time_us = get_microseconds_timestamp();
                last_heartbeat_s    = get_seconds_timestamp();
                handle_append_entries_rq(sock_fd, &peer_addr,
                                         &raft_message.append_entries_rpc,
                                         raft_message.request_id);
                break;
            case MT_APPEND_ENTRIES_RS:
                handle_append_entries_rs(sock_fd, &peer_addr,
                                         &raft_message.append_entries_reply,
                                         raft_message.request_id);
                break;
            case MT_REQUEST_VOTE_RQ:
                handle_request_vote_rq(sock_fd, &peer_addr,
                                       &raft_message.request_vote_rpc);
                break;
            case MT_REQUEST_VOTE_RS:
                handle_request_vote_rs(sock_fd, &peer_addr,
                                       &raft_message.request_vote_reply);
                if (cm.machine.state == RS_LEADER) {
                    broadcast_heartbeat(sock_fd);
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
                broadcast_heartbeat(sock_fd);
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
                    start_election(sock_fd);
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

int raft_submit(int value)
{
    if (cm.machine.state != RS_LEADER)
        return -1;

    printf("[INFO] Received value %d\n", value);
    int submit_index  = cm.machine.log.length;
    log_entry_t entry = {.term = cm.machine.current_term, .value = value};
    da_append(&cm.machine.log, entry);
    return submit_index;
}

// static void init_nodes(void)
// {
//     struct {
//         char addr[IP_LENGTH];
//         int port;
//     } peers[NODES_COUNT] = {
//         {"127.0.0.1", 8777}, {"127.0.0.1", 8778}, {"127.0.0.1", 8779}};
//
//     for (int i = 0; i < NODES_COUNT; ++i) {
//         printf("[INFO] Cluster topology\n");
//         printf("[INFO]\t - %s:%d (%d)\n", peers[i].addr, peers[i].port, i);
//
//         struct sockaddr_in s_addr = {0};
//         s_addr.sin_family         = AF_INET;
//         s_addr.sin_port           = htons(peers[i].port);
//
//         if (inet_pton(AF_INET, peers[i].addr, &s_addr.sin_addr) <= 0) {
//             perror("Invalid peer IP address");
//             return;
//         }
//         cm.nodes[i] = (peer_t){.addr = s_addr, .last_active = time(NULL)};
//     }
// }

// int main(int argc, char **argv)
// {
//     init_nodes();
//
//     if (argc > 1)
//         cm.node_id = atoi(argv[1]);
//
//     if (cm.node_id > NODES_COUNT)
//         exit(EXIT_FAILURE);
//
//     printf("[INFO] Node %d starting\n", cm.node_id);
//     srand(time(NULL));
//     raft_server_start(cm.node_id);
//
//     return 0;
// }
