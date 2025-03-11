#ifndef TIMESERIES_H
#define TIMESERIES_H

#include "partition.h"
#include "storage.h"
#include "wal.h"
#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>

#define TS_NAME_MAX_LENGTH        (1 << 9)
#define TS_CHUNK_SIZE             900 // 15 min
#define TS_MAX_PARTITIONS         16
#define DATAPATH_SIZE             (1 << 8)

// Errors
#define TS_E_UNKNOWN              -1
#define TS_E_OOM                  -2
#define TS_E_NULL_POINTER         -3
#define TS_E_INIT_PARTITION_FAIL  -4
#define TS_E_FLUSH_PARTITION_FAIL -5
#define TS_E_WAL_INIT_FAIL        -6
#define TS_E_WAL_LOAD_FAIL        -7
#define TS_E_WAL_APPEND_FAIL      -8
#define TS_E_FLUSH_CHUNK_FAIL     -9
#define TS_E_INVALID_RANGE        -10

extern const char *BASEPATH;
extern const size_t TS_FLUSHSIZE;
extern const size_t TS_MIN_FLUSHSIZE;
extern const size_t TS_BATCH_OFFSET;

/*
 * Enum defining the rules to apply when a duplicate point is
 * inserted in the timeseries.
 *
 * It currently just support
 * - IGNORE drops the point, returning a failure at insert attempt
 * - INSERT just appends the point
 * - UPDATE updates the point with the new value
 */
typedef enum dup_policy { DP_IGNORE, DP_INSERT } duplication_policy_t;

/*
 * Simple record struct, wrap around a column inside the database, defined as a
 * key-val couple alike, though it's used only to describe the value of each
 * column
 */
typedef struct record {
    struct timespec tv;
    uint64_t timestamp;
    double_t value;
    int is_set;
} record_t;

extern size_t ts_record_timestamp(const uint8_t *buf);

extern size_t ts_record_write(const record_t *r, uint8_t *buf);

extern size_t ts_record_read(record_t *r, const uint8_t *buf);

extern size_t ts_record_batch_write(const record_t *r[], uint8_t *buf,
                                    size_t count);

typedef struct record_array {
    size_t length;
    size_t capacity;
    record_t *items;
} record_array_t;

/*
 * Time series chunk, main data structure to handle the time-series, it carries
 * a base offset which represents the 1st timestamp inserted and the
 * columns data. Data are stored in a single array, using a base_offset as a
 * strating timestamp, resulting in the timestamps fitting in the allocated
 * space.
 */
typedef struct ts_chunk {
    wal_t wal;
    uint64_t base_offset;
    uint64_t start_ts;
    uint64_t end_ts;
    size_t max_index;
    record_array_t points[TS_CHUNK_SIZE];
} ts_chunk_t;

typedef struct ts_opts {
    int64_t retention;
    size_t flushsize;
    duplication_policy_t policy;
} ts_opts_t;

/*
 * Time series, main data structure to handle the time-series, it carries some
 * basic informations like the name of the series and the retention time. Data
 * are stored in 2 timeseries_chunk_t, a current and latest timestamp one and
 * one to account for out of order points that will be merged later when
 * flushing on disk.
 */
typedef struct timeseries {
    char name[TS_NAME_MAX_LENGTH];
    char db_datapath[DATAPATH_SIZE];
    char pathbuf[PATHBUF_SIZE];
    ts_chunk_t *head;
    ts_chunk_t *prev;
    partition_t partitions[TS_MAX_PARTITIONS];
    size_t partition_nr;
    ts_opts_t opts;
} timeseries_t;

typedef int (*ts_record_batch_callback_t)(const record_array_t *ra,
                                          void *userdata);

extern int ts_init(timeseries_t *ts);

extern void ts_close(timeseries_t *ts);

extern int ts_insert(timeseries_t *ts, uint64_t timestamp, double_t value);

extern int ts_find(const timeseries_t *ts, uint64_t timestamp, record_t *r);

extern int ts_range(const timeseries_t *ts, uint64_t t0, uint64_t t1,
                    record_array_t *out);

extern int ts_scan(const timeseries_t *ts, record_array_t *out);

extern int ts_stream(const timeseries_t *ts,
                     ts_record_batch_callback_t callback, void *userdata);

extern void ts_print(const timeseries_t *ts);

typedef struct timeseries_db {
    char datapath[DATAPATH_SIZE];
} timeseries_db_t;

extern timeseries_db_t *tsdb_init(const char *datapath);

extern void tsdb_close(timeseries_db_t *tsdb);

extern timeseries_t *ts_create(const timeseries_db_t *tsdb, const char *name,
                               ts_opts_t opts);

extern timeseries_t *ts_get(const timeseries_db_t *tsdb, const char *name);

#endif
