#ifndef RAFT_H
#define RAFT_H

#include <netdb.h>
#include <stdbool.h>
#include <stdlib.h>

#define IP_LENGTH       16
#define MAX_NODES_COUNT 15

/**
 ** Raft machine State, see section 5.2 of the paper for a summary
 **/

typedef enum raft_machine_state {
    RS_FOLLOWER,
    RS_CANDIDATE,
    RS_LEADER,
    RS_DEAD
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
    struct {
        int commit_index;
        int last_applied;
    } state_volatile;
    struct {
        int next_index[MAX_NODES_COUNT];
        int match_index[MAX_NODES_COUNT];
    } leader_volatile;
} raft_state_t;

int raft_register_node(const char *addr, int port);
int raft_submit(int value);
int raft_submit_sharded(const char *key, int value);
void raft_server_start(const struct sockaddr_in *peer, const char *store);

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
    MT_RAFT_CLUSTER_JOIN_RPC,
    MT_RAFT_ADD_PEER_RPC,
    MT_RAFT_FORWARD_VALUE_RPC,
    MT_RAFT_REQUEST_VOTE_RPC,
    MT_RAFT_REQUEST_VOTE_REPLY,
    MT_RAFT_APPEND_ENTRIES_RPC,
    MT_RAFT_APPEND_ENTRIES_REPLY
} message_type_t;

// RAFT dynamic node handling structs
// Extension of the raft RPCs, originally these are not
// part of RAFT, usually node membership is handled by
// other kind of protocols, such as gossip.

typedef struct {
    char ip_addr[IP_LENGTH];
    int port;
} add_node_rpc_t;

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

typedef struct forward_value_rpc {
    int value;
} forward_value_rpc_t;

// Simple wrapper for a generic RAFT
// message

typedef struct raft_message {
    message_type_t type;
    union {
        add_node_rpc_t add_node_rpc;
        forward_value_rpc_t forward_value_rpc;
        request_vote_rpc_t request_vote_rpc;
        request_vote_reply_t request_vote_reply;
        append_entries_rpc_t append_entries_rpc;
        append_entries_reply_t append_entries_reply;
    };
} raft_message_t;

/**
 ** RAFT message encoding interface, allows for multiple serialization
 ** types, by default it uses the binary defined in encoding.h
 **/

typedef struct raft_encoding {
    ssize_t (*raft_message_write)(uint8_t *buf, const raft_message_t *rm);
    message_type_t (*raft_message_read)(const uint8_t *buf, raft_message_t *rm);
} raft_encoding_t;

void raft_set_encoding(raft_encoding_t *backend);

/**
 ** RAFT persistence interface, allows for multiple persistence backends, by
 ** default it uses the file persistence backend defined in storage.h
 **/
typedef struct raft_persistence {
    int (*open_store)(void *context);
    int (*close_store)(void *context);
    int (*save_state)(void *context, const raft_state_t *state);
    int (*load_state)(void *context, raft_state_t *state);
} raft_persistence_t;

void raft_set_persistence(void *context, raft_persistence_t *backend);

#endif
