#ifndef WAL_H
#define WAL_H

#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#define WAL_PATHSIZE 512

typedef struct wal {
    FILE *fp;
    char path[WAL_PATHSIZE];
    size_t size;
} wal_t;

int wal_init(wal_t *w, const char *path, uint64_t base_timestamp, int main);

int wal_load(wal_t *w, const char *path, uint64_t base_timestamp, int main);

int wal_delete(wal_t *w);

int wal_append(wal_t *wal, uint64_t ts, double_t value);

size_t wal_size(const wal_t *wal);

#endif
