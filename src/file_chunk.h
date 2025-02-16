#ifndef FILE_CHUNK_H
#define FILE_CHUNK_H

#include "hash.h"
#include <stdio.h>
#include <stdlib.h>

#define FILENAME_SIZE 255

typedef struct file_chunk {
    char name[FILENAME_SIZE];
    uint8_t *data;
    size_t size;
    uint8_t hash[SHA256_SIZE];
} file_chunk_t;

typedef struct {
    FILE *fp;
    file_chunk_t chunk;
} buffered_chunk_t;

int file_chunk_buffered_write(buffered_chunk_t *file);

int file_chunk_buffered_read(const char *path, buffered_chunk_t *file);

void file_chunk_buffered_close(buffered_chunk_t *file);

typedef struct {
    file_chunk_t *items;
    size_t capacity;
    size_t length;
} file_chunk_array_t;

int file_chunk_split_file(const char name[FILENAME_SIZE], size_t size,
                          file_chunk_array_t *array);

#endif
