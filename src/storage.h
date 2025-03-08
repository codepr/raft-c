#ifndef STORAGE_H
#define STORAGE_H

#include "raft.h"
#include <stdio.h>

#define PATHBUF_SIZE BUFSIZ

typedef struct {
    char path[BUFSIZ];
    FILE *fp;
} file_context_t;

// Genereal APIs

typedef struct buffer buffer_t;

int makedir(const char *path);
int buffer_read_file(FILE *fp, buffer_t *b);
ssize_t read_file(FILE *fp, uint8_t *buf);
ssize_t filesize(FILE *fp, long offset);

// Contexst APIs

int file_open(void *context, const char *mode);
int file_save_state(void *context, const raft_state_t *state);
int file_load_state(void *context, raft_state_t *state);
int file_close(void *context);

#endif
