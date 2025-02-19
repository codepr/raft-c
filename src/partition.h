#ifndef PARTITION_H
#define PARTITION_H

#include "commitlog.h"
#include "index.h"

typedef struct timeseries_chunk timeseries_chunk_t;

typedef struct partition {
    commitlog_t clog;
    index_t index;
    uint64_t start_ts;
    uint64_t end_ts;
} partition_t;

int partition_init(partition_t *p, const char *path, uint64_t base);

int partition_load(partition_t *p, const char *path, uint64_t base);

int partition_flush_chunk(partition_t *p, const timeseries_chunk_t *tc);

int partition_find(const partition_t *p, uint8_t *dst, uint64_t timestamp);

int partition_range(const partition_t *p, uint8_t *dst, uint64_t t0,
                    uint64_t t1);

#endif
