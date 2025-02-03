#ifndef STORAGE_H
#define STORAGE_H

#include "raft.h"

int file_save_state(const raft_state_t *state);
int file_load_state(raft_state_t *state);

#endif
