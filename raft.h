#ifndef RAFT_H
#define RAFT_H

void raft_register_node(const char *addr, int port);
void raft_server_start(int node_id);
int raft_submit(int value);

#endif
