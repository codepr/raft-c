#ifndef CDFS_NODE_H
#define CDFS_NODE_H

#include "file_chunk.h"
#include <stdlib.h>

typedef enum {
    CMT_PUSH_FILE,
    CMT_PUSH_CHUNK,
    CMT_PULL_FILE,
    CMT_PULL_CHUNK
} cdfs_message_type;

typedef struct cdfs_message {
    cdfs_message_type type;
    size_t size;
    union {
        char filename[FILENAME_SIZE];
        struct {
            char filename[FILENAME_SIZE];
            char hash[SHA256_SIZE];
        } chunk;
    };
} cdfs_message_t;

#endif
