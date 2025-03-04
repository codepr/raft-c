#ifndef COMMITLOG_H
#define COMMITLOG_H

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

typedef struct commitlog {
    FILE *fp;
    size_t size;
    uint64_t base_timestamp;
    uint64_t base_ns;
    uint64_t current_timestamp;
} commitlog_t;

int cl_init(commitlog_t *cl, const char *path, uint64_t base);

int cl_load(commitlog_t *cl, const char *path, uint64_t base);

void cl_set_base_ns(commitlog_t *cl, uint64_t ns);

int cl_append_data(commitlog_t *cl, const uint8_t *data, size_t len);

int cl_append_batch(commitlog_t *cl, const uint8_t *batch, size_t len);

int cl_read_at(const commitlog_t *cl, uint8_t **buf, size_t offset, size_t len);

void cl_print(const commitlog_t *cl);

#endif
