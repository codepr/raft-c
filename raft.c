#include "raft.h"
#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdarg.h>
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

#define LL_DEBUG           0
#define LL_INFO            1
#define LL_WARNING         2
#define LL_ERROR           3
#define LL_CRITICAL        4

#define LOG_LEVEL          LL_DEBUG

#define RAFT_LOG(level, level_str, fmt, ...)                                   \
    do {                                                                       \
        if (level >= LOG_LEVEL) {                                              \
            fprintf(stderr, "[%s] " fmt "%s",                                  \
                    level_str __VA_OPT__(, ) __VA_ARGS__, "\n");               \
        }                                                                      \
        if (level == LL_CRITICAL)                                              \
            exit(EXIT_FAILURE);                                                \
    } while (0)

#define log_debug(fmt, ...)                                                    \
    RAFT_LOG(LL_DEBUG, "DEBUG", fmt __VA_OPT__(, ) __VA_ARGS__)
#define log_info(fmt, ...)                                                     \
    RAFT_LOG(LL_INFO, "INFO", fmt __VA_OPT__(, ) __VA_ARGS__)
#define log_warning(fmt, ...)                                                  \
    RAFT_LOG(LL_WARNING, "WARNING", fmt __VA_OPT__(, ) __VA_ARGS__)
#define log_error(fmt, ...)                                                    \
    RAFT_LOG(LL_ERROR, "ERROR", fmt __VA_OPT__(, ) __VA_ARGS__)
#define log_critical(fmt, ...)                                                 \
    RAFT_LOG(LL_CRITICAL, "CRITICAL", fmt __VA_OPT__(, ) __VA_ARGS__)

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
            log_critical("DA realloc failed");                                 \
        }                                                                      \
    } while (0)

#define da_append(da, item)                                                    \
    do {                                                                       \
        assert((da));                                                          \
        if ((da)->length + 1 >= (da)->capacity)                                \
            da_extend((da));                                                   \
        (da)->items[(da)->length++] = (item);                                  \
    } while (0)

#define da_insert(da, i, item)                                                 \
    do {                                                                       \
        assert((da));                                                          \
        if ((i) >= (da)->length)                                               \
            da_extend((da));                                                   \
        (da)->items[i] = (item);                                               \
        if ((i) >= (da)->length)                                               \
            (da)->length++;                                                    \
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

/**
 ** Cluster communication and management - UDP network utility functions
 **
 ** The RAFT machine will be implemented by using UDP as the transport layer,
 ** making it lightweight and connection-less. We're don't really care about
 ** delivery guarantee ** and the communication is simplified.
 **/

#define MAX_NODES_COUNT 15

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
    MT_GOSSIP_CLUSTER_JOIN_RPC,
    MT_GOSSIP_ADD_PEER_RPC,
    MT_RAFT_REQUEST_VOTE_RPC,
    MT_RAFT_REQUEST_VOTE_REPLY,
    MT_RAFT_APPEND_ENTRIES_RPC,
    MT_RAFT_APPEND_ENTRIES_REPLY
} message_type_t;

// Gossip structs

typedef struct {
    char ip_addr[IP_LENGTH];
    int port;
} gossip_add_node_t;

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

static int gossip_add_node_rpc_write(uint8_t *buf, const gossip_add_node_t *ga);
static int gossip_add_node_rpc_read(gossip_add_node_t *ga, const uint8_t *buf);
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
    RS_LEADER,
    RS_DEAD
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
    struct {
        int commit_index;
        int last_applied;
    } state_volatile;
    struct {
        int next_index[MAX_NODES_COUNT];
        int match_index[MAX_NODES_COUNT];
    } leader_volatile;
} raft_state_t;

// Simple wrapper for a generic RAFT
// message

typedef struct raft_message {
    message_type_t type;
    union {
        gossip_add_node_t gossip_add_node_rpc;
        request_vote_rpc_t request_vote_rpc;
        request_vote_reply_t request_vote_reply;
        append_entries_rpc_t append_entries_rpc;
        append_entries_reply_t append_entries_reply;
    };
} raft_message_t;

static ssize_t raft_message_write(uint8_t *buf, const raft_message_t *rm)
{
    ssize_t bytes = write_u8(buf++, rm->type);
    switch (rm->type) {
    case MT_GOSSIP_CLUSTER_JOIN_RPC:
        bytes += gossip_add_node_rpc_write(buf, &rm->gossip_add_node_rpc);
        break;
    case MT_GOSSIP_ADD_PEER_RPC:
        bytes += gossip_add_node_rpc_write(buf, &rm->gossip_add_node_rpc);
        break;
    case MT_RAFT_APPEND_ENTRIES_RPC:
        bytes += append_entries_rpc_write(buf, &rm->append_entries_rpc);
        break;
    case MT_RAFT_APPEND_ENTRIES_REPLY:
        bytes += append_entries_reply_write(buf, &rm->append_entries_reply);
        break;
    case MT_RAFT_REQUEST_VOTE_RPC:
        bytes += request_vote_rpc_write(buf, &rm->request_vote_rpc);
        break;
    case MT_RAFT_REQUEST_VOTE_REPLY:
        bytes += request_vote_reply_write(buf, &rm->request_vote_reply);
        break;
    }

    return bytes;
}

static message_type_t raft_message_read(const uint8_t *buf, raft_message_t *rm)
{
    rm->type = read_u8(buf++);
    switch (rm->type) {
    case MT_GOSSIP_CLUSTER_JOIN_RPC:
        gossip_add_node_rpc_read(&rm->gossip_add_node_rpc, buf);
        break;
    case MT_GOSSIP_ADD_PEER_RPC:
        gossip_add_node_rpc_read(&rm->gossip_add_node_rpc, buf);
        break;
    case MT_RAFT_APPEND_ENTRIES_RPC:
        append_entries_rpc_read(&rm->append_entries_rpc, buf);
        break;
    case MT_RAFT_APPEND_ENTRIES_REPLY:
        append_entries_reply_read(&rm->append_entries_reply, buf);
        break;
    case MT_RAFT_REQUEST_VOTE_RPC:
        request_vote_rpc_read(&rm->request_vote_rpc, buf);
        break;
    case MT_RAFT_REQUEST_VOTE_REPLY:
        request_vote_reply_read(&rm->request_vote_reply, buf);
        break;
    }
    return rm->type;
}

// Represents a peer in the Raft cluster.
typedef struct peer {
    struct sockaddr_in addr;
    time_t last_active;
    size_t saved_log_length;
} peer_t;

/*
 * Represents the consensus module that handles the Raft consensus mechanism.
 * It contains the Raft state machine, the list of peers, and some other
 * information related to the consensus process.
 */
typedef struct {
    raft_state_t machine;
    struct {
        size_t capacity;
        size_t length;
        peer_t *items;
    } nodes;
    int votes_received;
    int node_id;
    int current_leader_id;
} consensus_module_t;

// Global consensus module, initializing to 0 (zero-initialization).
static consensus_module_t cm = {0};

static void transition_to_leader(void)
{
    cm.machine.state     = RS_LEADER;
    cm.votes_received    = 0;
    cm.current_leader_id = cm.node_id;
    for (int i = 0; i < cm.nodes.length; ++i) {
        cm.machine.leader_volatile.next_index[i]  = cm.machine.log.length;
        cm.machine.leader_volatile.match_index[i] = -1;
    }
    log_info("Transition to LEADER");
}

static void transition_to_follower(int term)
{
    cm.machine.state        = RS_FOLLOWER;
    cm.machine.voted_for    = -1;
    cm.machine.current_term = term;

    log_info("Transition to FOLLOWER");
}

static void transition_to_candidate(void)
{
    cm.machine.state = RS_CANDIDATE;

    log_info("Transition to CANDIDATE");
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
static int gossip_add_node_rpc_write(uint8_t *buf, const gossip_add_node_t *ga)
{
    uint8_t ip_addr_len = strlen(ga->ip_addr);
    int bytes           = write_u8(buf++, ip_addr_len);
    memcpy(buf, ga->ip_addr, ip_addr_len);
    bytes += ip_addr_len;
    buf += ip_addr_len;
    bytes += write_i32(buf, ga->port);
    return bytes;
}

static int gossip_add_node_rpc_read(gossip_add_node_t *ga, const uint8_t *buf)
{
    size_t ip_addr_len = read_u8(buf++);
    if (ip_addr_len > IP_LENGTH)
        return -1;
    strncpy(ga->ip_addr, (char *)buf, ip_addr_len);
    buf += ip_addr_len;
    ga->port = read_i32(buf);
    return 0;
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

    ssize_t length      = raft_message_write(&buf[0], rm);
    return sendto(fd, buf, length, 0, (struct sockaddr *)peer, sizeof(*peer));
}

static void start_election(int fd)
{
    if (cm.machine.state != RS_CANDIDATE)
        return;

    log_info("Start election current_term=%d", cm.machine.current_term);
    cm.votes_received = 1;
    cm.machine.current_term++;
    cm.machine.voted_for   = cm.node_id;

    raft_message_t message = {
        .type             = MT_RAFT_REQUEST_VOTE_RPC,
        .request_vote_rpc = {.term           = cm.machine.current_term,
                             .candidate_id   = cm.node_id,
                             .last_log_term  = last_log_term(),
                             .last_log_index = last_log_index()}};
    for (int i = 0; i < cm.nodes.length; ++i) {
        if (i == cm.node_id)
            continue;

        if (send_raft_message(fd, &cm.nodes.items[i].addr, &message) < 0)
            log_error("Failed to send RequestVoteRPC to client %i: %s",
                      cm.nodes.items[i].addr.sin_addr.s_addr, strerror(errno));
    }
}

static void handle_gossip_cluster_join_rpc(int fd, const gossip_add_node_t *an)
{
    raft_message_t raft_message = {.type = MT_GOSSIP_CLUSTER_JOIN_RPC,
                                   .gossip_add_node_rpc = *an};
    if (cm.machine.state == RS_LEADER) {
        // TODO inefficient double translation (host str, port int) <=>
        // struct sockaddr_in
        log_info("Cluster join request, updating followers");
        raft_register_node(an->ip_addr, an->port);
        // Forward the new node to the other nodes
        raft_message.type = MT_GOSSIP_ADD_PEER_RPC;

        for (int i = 0; i < cm.nodes.length; ++i) {
            send_raft_message(fd, &cm.nodes.items[i].addr, &raft_message);
        }
    } else {
        log_info("Cluster join request, forwarding to leader");
        // Forward to the raft leader
        send_raft_message(fd, &cm.nodes.items[cm.current_leader_id].addr,
                          &raft_message);
    }
}

static void handle_gossip_add_node_rpc(const gossip_add_node_t *an)
{
    log_info("New node (%s:%d)joined the cluster, updating table", an->ip_addr,
             an->port);
    raft_register_node(an->ip_addr, an->port);
}

static void handle_request_vote_rq(int fd, const struct sockaddr_in *peer,
                                   const request_vote_rpc_t *rv)
{
    log_info("Received RequestVoteRPC [current_term=%d, voted_for=%d]",
             cm.machine.current_term, cm.machine.voted_for);
    // - If current term > candidate term
    // reply false
    // - If candidate id is unset (0) or
    // it's set and the log is up to date
    // with
    //   the state, reply true
    if (rv->term > cm.machine.current_term) {
        log_info("Term out of date in RequestVote");
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

    const raft_message_t message = {.type = MT_RAFT_REQUEST_VOTE_REPLY,
                                    .request_vote_reply = rv_reply};
    if (send_raft_message(fd, peer, &message) < 0)
        log_error("Failed to send RequestVoteReply to client %d: %s",
                  peer->sin_addr.s_addr, strerror(errno));
}

static void handle_request_vote_rs(int fd, const struct sockaddr_in *peer,
                                   const request_vote_reply_t *rv)
{
    log_info("Received RequestVoteReply(term=%d, vote_granted=%d)", rv->term,
             rv->vote_granted);
    // Already a leader, discard vote
    if (cm.machine.state != RS_CANDIDATE)
        return;
    if (rv->term > cm.machine.current_term) {
        transition_to_follower(rv->term);
    } else {
        if (rv->vote_granted)
            ++cm.votes_received;
        if (cm.votes_received > (cm.nodes.length / 2))
            transition_to_leader();
    }
}

static int find_peer_index(const struct sockaddr_in *peer)
{
    for (int i = 0; i < cm.nodes.length; ++i) {
        if (cm.nodes.items[i].addr.sin_addr.s_addr == peer->sin_addr.s_addr &&
            htons(cm.nodes.items[i].addr.sin_port) == htons(peer->sin_port)) {
            return i;
        }
    }
    return -1;
}

static void handle_append_entries_rq(int fd, const struct sockaddr_in *peer,
                                     const append_entries_rpc_t *ae)
{
    log_info("Received AppendEntriesRPC");
    cm.nodes.items[cm.node_id].last_active = time(NULL);

    // Keep the current leader up to date
    cm.current_leader_id                   = find_peer_index(peer);

    for (int i = 0; i < ae->entries.length; ++i)
        log_debug("\t(term=%d, value=%d) ", ae->entries.items[i].term,
                  ae->entries.items[i].value);

    append_entries_reply_t ae_reply = {0};

    if (ae->term > cm.machine.current_term) {
        log_info("Term out of date in AppendEntriesRPC");
        transition_to_follower(ae->term);
    }

    for (int i = 0; i < cm.machine.log.length; ++i)
        log_debug("\t %d ~> (term=%d value=%d)", i,
                  cm.machine.log.items[i].term, cm.machine.log.items[i].value);

    if (ae->term == cm.machine.current_term) {
        if (cm.machine.state != RS_FOLLOWER) {
            transition_to_follower(ae->term);
        }

        if (ae->prev_log_index == -1 ||
            (cm.machine.log.capacity &&
             (ae->prev_log_index < cm.machine.log.length &&
              ae->prev_log_term ==
                  cm.machine.log.items[ae->prev_log_index].term))) {
            ae_reply.success      = true;

            int left_index        = ae->prev_log_index + 1;
            int new_entries_index = 0;

            while (1) {
                if (left_index >= cm.machine.log.length ||
                    new_entries_index >= ae->entries.length)
                    break;
                if (cm.machine.log.items[left_index].term !=
                    ae->entries.items[new_entries_index].term)
                    break;
                left_index++;
                new_entries_index++;
            }

            if (new_entries_index < ae->entries.length) {
                for (int i = left_index; i < left_index + ae->entries.length;
                     ++i, new_entries_index++) {
                    da_insert(&cm.machine.log, i,
                              ae->entries.items[new_entries_index]);
                }
            }

            if (ae->leader_commit > cm.machine.state_volatile.commit_index) {
                cm.machine.state_volatile.commit_index =
                    ae->leader_commit < cm.machine.log.length - 1
                        ? ae->leader_commit
                        : cm.machine.log.length - 1;
            }
        }
    }
    ae_reply.term                = cm.machine.current_term;

    const raft_message_t message = {.type = MT_RAFT_APPEND_ENTRIES_REPLY,
                                    .append_entries_reply = ae_reply};

    if (send_raft_message(fd, peer, &message) < 0)
        log_error("Failed to send AppendEntriesReply to peer %d: %s",
                  peer->sin_addr.s_addr, strerror(errno));
}

static void handle_append_entries_rs(int fd, const struct sockaddr_in *peer,
                                     const append_entries_reply_t *ae)
{
    if (ae->term > cm.machine.current_term) {
        transition_to_follower(ae->term);
        return;
    }

    int peer_id = find_peer_index(peer);
    if (peer_id < 0) {
        log_error("Could not find peer ID for reply");
        return;
    }

    if (cm.machine.state == RS_LEADER && cm.machine.current_term == ae->term) {
        if (ae->success) {
            log_debug("Update peer=%d", peer_id);
            cm.machine.leader_volatile.next_index[peer_id] =
                cm.nodes.items[peer_id].saved_log_length;
            cm.machine.leader_volatile.match_index[peer_id] =
                cm.machine.leader_volatile.next_index[peer_id] - 1;

            int current_commit_index = cm.machine.state_volatile.commit_index;
            for (int i = cm.machine.state_volatile.commit_index + 1;
                 i < cm.machine.log.length; ++i) {
                if (cm.machine.log.items[i].term == cm.machine.current_term) {
                    int match_count = 1;
                    for (int j = 0; j < cm.nodes.length; ++j) {
                        if (cm.machine.leader_volatile.match_index[i] >= i)
                            match_count++;
                    }
                    if (match_count * 2 > cm.nodes.length + 1)
                        cm.machine.state_volatile.commit_index = i;
                }
            }
            if (cm.machine.state_volatile.commit_index !=
                current_commit_index) {
                log_info("Leader sets commit_index=%d",
                         cm.machine.state_volatile.commit_index);

                cm.machine.state_volatile.last_applied =
                    cm.machine.state_volatile.commit_index;
            }
        } else {
            if (cm.machine.leader_volatile.next_index[peer_id])
                cm.machine.leader_volatile.next_index[peer_id]--;
            log_info("AppendEntriesReply from %d success=false next_index=%d",
                     peer_id, cm.machine.leader_volatile.next_index[peer_id]);
        }
    }
}

static void broadcast_heartbeat(int fd)
{
    log_info("Broadcast heartbeat (term=%d)", cm.machine.current_term);

    raft_message_t message = {
        .type               = MT_RAFT_APPEND_ENTRIES_RPC,
        .append_entries_rpc = {.term      = cm.machine.current_term,
                               .leader_id = cm.node_id,
                               .entries   = {0}}};
    for (int i = 0; i < cm.nodes.length; ++i) {
        if (i == cm.node_id)
            continue;

        int prev_log_index = cm.machine.leader_volatile.next_index[i] - 1;
        int prev_log_term  = cm.machine.log.length && prev_log_index >= 0
                                 ? cm.machine.log.items[prev_log_index].term
                                 : -1;

        message.append_entries_rpc.prev_log_term  = prev_log_term;
        message.append_entries_rpc.prev_log_index = prev_log_index;
        message.append_entries_rpc.leader_commit =
            cm.machine.state_volatile.commit_index;

        for (int j = cm.machine.leader_volatile.next_index[i];
             j < cm.machine.log.length; ++j) {
            da_append(&message.append_entries_rpc.entries,
                      cm.machine.log.items[j]);
        }

        // - Try to send the raft message out to the peer node, if it fails
        //   mark the node as offline to avoid recording a pending request.
        // - If it goes through correctly, record a pending request for that
        //   peer node
        if (send_raft_message(fd, &cm.nodes.items[i].addr, &message) < 0)
            log_error("Failed to send AppendEntriesRPC to client %i: %s",
                      cm.nodes.items[i].addr.sin_addr.s_addr, strerror(errno));

        // Save current log length at the time of broadcast for this peer
        cm.nodes.items[i].saved_log_length        = cm.machine.log.length;

        // Manually reset index for the next peer
        message.append_entries_rpc.entries.length = 0;
    }

    // TODO use a single append entries packet and free it once
    // at the exit of the program
    if (message.append_entries_rpc.entries.capacity)
        free(message.append_entries_rpc.entries.items);
}

#define HEARTBEAT_TIMEOUT_S 1

static int raft_register_peer(const struct sockaddr_in *s_addr)
{
    int node_id = -1;
    // If already registerd exit
    if ((node_id = find_peer_index(s_addr)) > 0)
        return node_id;

    peer_t new_peer = (peer_t){
        .addr = *s_addr, .last_active = time(NULL), .saved_log_length = 0};

    da_append(&cm.nodes, new_peer);

    return cm.nodes.length - 1;
}

int raft_register_node(const char *addr, int port)
{
    struct sockaddr_in s_addr = {0};
    s_addr.sin_family         = AF_INET;
    s_addr.sin_port           = htons(port);

    if (inet_pton(AF_INET, addr, &s_addr.sin_addr) <= 0) {
        perror("Invalid peer IP address");
        return -1;
    }

    return raft_register_peer(&s_addr);
}

void raft_seed_nodes(const raft_node_t nodes[], size_t length)
{
    for (size_t i = 0; i < length; ++i)
        raft_register_node(nodes[i].ip_addr, nodes[i].port);
}

void send_join_request(int fd, const struct sockaddr_in *seed_addr,
                       const struct sockaddr_in *peer)
{
    char ip[IP_LENGTH];
    get_ip_str(peer, ip, IP_LENGTH);
    raft_message_t raft_message = {
        .type                = MT_GOSSIP_CLUSTER_JOIN_RPC,
        .gossip_add_node_rpc = {.port = htons(peer->sin_port)}};

    strncpy(raft_message.gossip_add_node_rpc.ip_addr, ip, IP_LENGTH);

    send_raft_message(fd, seed_addr, &raft_message);
}

void raft_server_start(const struct sockaddr_in *peer)
{
    srand(time(NULL) ^ getpid());

    int node_id = find_peer_index(peer);
    if (node_id == -1) {
        node_id          = raft_register_peer(peer);
        cm.machine.state = RS_DEAD;
    }
    cm.node_id = node_id;
    char ip[IP_LENGTH];
    get_ip_str(&cm.nodes.items[cm.node_id].addr, ip, IP_LENGTH);
    log_info("UDP listen on %s:%d", ip,
             htons(cm.nodes.items[cm.node_id].addr.sin_port));
    int sock_fd =
        udp_listen(ip, htons(cm.nodes.items[cm.node_id].addr.sin_port));

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
        if (num_events < 0)
            log_critical("select() error: %s\n", strerror(errno));

        if (FD_ISSET(sock_fd, &read_fds)) {
            raft_message_t raft_message = {0};
            n                           = recvfrom(sock_fd, buf, sizeof(buf), 0,
                                                   (struct sockaddr *)&peer_addr, &addr_len);
            if (n < 0)
                log_error("recvfrom error: %s", strerror(errno));
            message_type_t message_type = raft_message_read(buf, &raft_message);
            switch (message_type) {
            case MT_GOSSIP_CLUSTER_JOIN_RPC:
                handle_gossip_cluster_join_rpc(
                    sock_fd, &raft_message.gossip_add_node_rpc);
                break;
            case MT_GOSSIP_ADD_PEER_RPC:
                handle_gossip_add_node_rpc(&raft_message.gossip_add_node_rpc);
                break;
            case MT_RAFT_APPEND_ENTRIES_RPC:
                last_update_time_us = get_microseconds_timestamp();
                last_heartbeat_s    = get_seconds_timestamp();
                handle_append_entries_rq(sock_fd, &peer_addr,
                                         &raft_message.append_entries_rpc);
                break;
            case MT_RAFT_APPEND_ENTRIES_REPLY:
                handle_append_entries_rs(sock_fd, &peer_addr,
                                         &raft_message.append_entries_reply);
                break;
            case MT_RAFT_REQUEST_VOTE_RPC:
                handle_request_vote_rq(sock_fd, &peer_addr,
                                       &raft_message.request_vote_rpc);
                break;
            case MT_RAFT_REQUEST_VOTE_REPLY:
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

        switch (cm.machine.state) {
        case RS_LEADER:
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
        case RS_DEAD:
            // Join cluster here
            send_join_request(sock_fd, &cm.nodes.items[0].addr, peer);
            tv.tv_sec = HEARTBEAT_TIMEOUT_S;
            break;
        default:
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
            break;
        }
    }
}

int raft_submit(int value)
{
    if (cm.machine.state != RS_LEADER)
        return -1;

    log_info("Received value %d", value);
    int submit_index  = cm.machine.log.length;
    log_entry_t entry = {.term = cm.machine.current_term, .value = value};
    da_append(&cm.machine.log, entry);
    return submit_index;
}
