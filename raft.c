#include "raft.h"
#include "darray.h"
#include "encoding.h"
#include "storage.h"
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

#define RANDOM(a, b) rand() % ((b) + 1 - (a)) + (a)

/**
 ** Cluster communication and management - UDP network utility functions
 **
 ** The RAFT machine will be implemented by using UDP as the transport layer,
 ** making it lightweight and connection-less. We're don't really care about
 ** delivery guarantee ** and the communication is simplified.
 **/

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
 ** Raft machine State, see section 5.2 of the paper for a summary
 **/

#define ELECTION_TIMEOUT()  RANDOM(150000, 300000);
#define HEARTBEAT_TIMEOUT_S 1

/**
 ** Consensus module definition
 ** Represents the generic global state of the cluster, tracking
 **  - RAFT machine state
 **  - Votes received from peers
 **  - Connected cluster nodes
 **  - The ID of the node
 **  - The ID of the current RAFT leader
 **/

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
static consensus_module_t cm          = {0};

static raft_encoding_t *raft_encoding = NULL;

static ssize_t raft_encode(uint8_t *buf, const raft_message_t *rm)
{
    return raft_encoding->raft_message_write(buf, rm);
}
static message_type_t raft_decode(const uint8_t *buf, raft_message_t *rm)
{
    return raft_encoding->raft_message_read(buf, rm);
}

static raft_persistence_t *raft_storage = NULL;

static int raft_save_state(void)
{
    // no-op backend
    if (!raft_storage)
        return -1;

    return raft_storage->save_state(&cm.machine);
}

static int raft_load_state(void)
{
    // no-op backend
    if (!raft_storage)
        return -1;

    return raft_storage->load_state(&cm.machine);
}

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
    return cm.machine.log.length == 0 ? 0 : da_back(&cm.machine.log).term;
}

static int last_log_index(void) { return cm.machine.log.length - 1; }

static int send_raft_message(int fd, const struct sockaddr_in *peer,
                             const raft_message_t *rm)
{
    uint8_t buf[BUFSIZ] = {0};

    ssize_t length      = raft_encode(&buf[0], rm);
    return sendto(fd, buf, length, 0, (struct sockaddr *)peer, sizeof(*peer));
}

/**
 ** RAFT handlers definition, the callbacks to process incoming
 ** messages from the nodes in the cluster
 **/

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

static void handle_cluster_join_rpc(int fd, const add_node_rpc_t *an)
{
    raft_message_t raft_message = {.type         = MT_RAFT_CLUSTER_JOIN_RPC,
                                   .add_node_rpc = *an};
    if (cm.machine.state == RS_LEADER) {
        // TODO inefficient double translation (host str, port int) <=>
        // struct sockaddr_in
        log_info("Cluster join request, updating followers");
        raft_register_node(an->ip_addr, an->port);
        // Forward the new node to the other nodes
        raft_message.type = MT_RAFT_ADD_PEER_RPC;

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

static void handle_add_node_rpc(const add_node_rpc_t *an)
{
    log_info("New node (%s:%d) joined the cluster, updating table", an->ip_addr,
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

#define NODE_ACTIVE_DEADLINE 3

static int online_nodes(void)
{
    int count  = 0;
    time_t now = time(NULL);
    for (int i = 0; i < cm.nodes.length; ++i)
        if (now - cm.nodes.items[i].last_active < NODE_ACTIVE_DEADLINE)
            ++count;
    return count;
}

static void handle_request_vote_rs(int fd, const struct sockaddr_in *peer,
                                   const request_vote_reply_t *rv)
{
    log_info("Received RequestVoteReply(term=%d, vote_granted=%d)", rv->term,
             rv->vote_granted);
    int peer_id = find_peer_index(peer);
    if (peer_id)
        cm.nodes.items[peer_id].last_active = time(NULL);
    // Already a leader, discard vote
    if (cm.machine.state != RS_CANDIDATE)
        return;
    if (rv->term > cm.machine.current_term) {
        transition_to_follower(rv->term);
    } else {
        if (rv->vote_granted)
            ++cm.votes_received;
        if (cm.votes_received > (online_nodes() / 2))
            transition_to_leader();
    }
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

    cm.nodes.items[peer_id].last_active = time(NULL);

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

    int node_id = raft_register_peer(&s_addr);
    if (node_id)
        cm.nodes.items[node_id].last_active = time(NULL);

    return node_id;
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
        .type         = MT_RAFT_CLUSTER_JOIN_RPC,
        .add_node_rpc = {.port = htons(peer->sin_port)}};

    strncpy(raft_message.add_node_rpc.ip_addr, ip, IP_LENGTH);

    send_raft_message(fd, seed_addr, &raft_message);
}

static int raft_save_state(void);
static int raft_load_state(void);

void raft_server_start(const struct sockaddr_in *peer)
{
    raft_encoding_t binary_encoder;
    raft_persistence_t file_storage;

    if (!raft_encoding) {
        binary_encoder =
            (raft_encoding_t){.raft_message_write = raft_bin_message_write,
                              .raft_message_read  = raft_bin_message_read};
        raft_set_encoding(&binary_encoder);
    }

    if (!raft_storage) {
        file_storage = (raft_persistence_t){
            .save_state = file_save_state,
            .load_state = file_load_state,
        };
        raft_set_persistence(&file_storage);
    }

    raft_load_state();

    srand(time(NULL) ^ getpid());

    int node_id = find_peer_index(peer);
    if (node_id == -1) {
        node_id          = raft_register_peer(peer);
        cm.machine.state = RS_DEAD;
    }

    cm.nodes.items[node_id].last_active = time(NULL);

    cm.node_id                          = node_id;
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
            // Assume a single message, long at most BUFSIZ bytes
            raft_message_t raft_message = {0};
            n                           = recvfrom(sock_fd, buf, sizeof(buf), 0,
                                                   (struct sockaddr *)&peer_addr, &addr_len);
            if (n < 0)
                log_error("recvfrom error: %s", strerror(errno));
            message_type_t message_type = raft_decode(buf, &raft_message);
            switch (message_type) {
            case MT_RAFT_CLUSTER_JOIN_RPC:
                handle_cluster_join_rpc(sock_fd, &raft_message.add_node_rpc);
                break;
            case MT_RAFT_ADD_PEER_RPC:
                handle_add_node_rpc(&raft_message.add_node_rpc);
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
                // Immediately send the first AppendEntriesRPC
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
            if (remaining_s >= select_timeout_s) {
                send_join_request(sock_fd, &cm.nodes.items[0].addr, peer);
                tv.tv_sec  = HEARTBEAT_TIMEOUT_S;
                tv.tv_usec = 0;
            } else {
                tv.tv_sec  = select_timeout_s - remaining_s;
                tv.tv_usec = 0;
            }
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

void raft_set_encoding(raft_encoding_t *backend) { raft_encoding = backend; }

void raft_set_persistence(raft_persistence_t *backend)
{
    raft_storage = backend;
}

int raft_submit(int value)
{
    if (cm.machine.state != RS_LEADER)
        return -1;

    log_info("Received value %d", value);
    int submit_index  = cm.machine.log.length;

    log_entry_t entry = {.term = cm.machine.current_term, .value = value};
    da_append(&cm.machine.log, entry);

    raft_save_state();

    return submit_index;
}
