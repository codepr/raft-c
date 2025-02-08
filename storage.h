#ifndef STORAGE_H
#define STORAGE_H

#include "raft.h"
#include <stdio.h>

typedef struct {
    char path[BUFSIZ];
    FILE *fp;
} file_context_t;

int file_open(void *context);
int file_save_state(void *context, const raft_state_t *state);
int file_load_state(void *context, raft_state_t *state);
int file_close(void *context);

#endif
