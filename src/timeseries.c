#include "timeseries.h"
#include "binary.h"
#include "darray.h"
#include "logger.h"
#include <dirent.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

static const char *BASE_PATH         = "logdata";
static const size_t LINEAR_THRESHOLD = 192;
static const size_t RECORD_BINARY_SIZE =
    (sizeof(uint64_t) * 2) + sizeof(double_t);
const size_t TS_FLUSH_SIZE   = 512; // 512b
const size_t TS_BATCH_OFFSET = sizeof(uint64_t) * 3;

timeseries_db_t *tsdb_init(const char *datapath)
{
    if (!datapath)
        return NULL;

    if (strlen(datapath) > DATAPATH_SIZE)
        return NULL;

    if (makedir(BASE_PATH) < 0)
        return NULL;

    timeseries_db_t *tsdb = malloc(sizeof(*tsdb));
    if (!tsdb)
        return NULL;

    strncpy(tsdb->datapath, datapath, strlen(datapath) + 1);

    // Create the DB path if it doesn't exist
    char pathbuf[PATHBUF_SIZE];
    snprintf(pathbuf, sizeof(pathbuf), "%s/%s", BASE_PATH, tsdb->datapath);
    if (makedir(pathbuf) < 0)
        return NULL;

    return tsdb;
}

void tsdb_close(timeseries_db_t *tsdb) { free(tsdb); }

timeseries_t *ts_create(const timeseries_db_t *tsdb, const char *name,
                        int64_t retention, duplication_policy_t policy)
{
    if (!tsdb || !name)
        return NULL;

    if (strlen(name) > TS_NAME_MAX_LENGTH)
        return NULL;

    timeseries_t *ts = malloc(sizeof(*ts));
    if (!ts)
        return NULL;

    ts->retention    = retention;
    ts->partition_nr = 0;
    ts->policy       = policy;
    for (int i = 0; i < TS_MAX_PARTITIONS; ++i)
        memset(&ts->partitions[i], 0x00, sizeof(ts->partitions[i]));

    snprintf(ts->name, TS_NAME_MAX_LENGTH, "%s", name);
    snprintf(ts->db_datapath, DATAPATH_SIZE, "%s", tsdb->datapath);

    if (ts_init(ts) < 0) {
        ts_close(ts);
        free(ts);
        return NULL;
    }

    return ts;
}

timeseries_t *ts_get(const timeseries_db_t *tsdb, const char *name)
{
    if (!tsdb || !name)
        return NULL;

    if (strlen(name) > TS_NAME_MAX_LENGTH)
        return NULL;

    timeseries_t *ts = malloc(sizeof(*ts));
    if (!ts)
        return NULL;

    for (int i = 0; i < TS_MAX_PARTITIONS; ++i)
        memset(&ts->partitions[i], 0x00, sizeof(ts->partitions[i]));

    snprintf(ts->db_datapath, DATAPATH_SIZE, "%s", tsdb->datapath);
    snprintf(ts->name, TS_NAME_MAX_LENGTH, "%s", name);

    // TODO consider adding some metadata file which saves TS info such as the
    // duplication policy
    if (ts_init(ts) < 0) {
        ts_close(ts);
        return NULL;
    }

    return ts;
}

static void ts_chunk_zero(timeseries_chunk_t *tc)
{
    tc->base_offset = 0;
    tc->start_ts    = 0;
    tc->end_ts      = 0;
    tc->max_index   = 0;
    tc->wal.size    = 0;
}

static int ts_chunk_init(timeseries_chunk_t *tc, const char *path,
                         uint64_t base_ts, int main)
{
    tc->base_offset = base_ts;
    tc->start_ts    = 0;
    tc->end_ts      = 0;
    tc->max_index   = 0;

    for (int i = 0; i < TS_CHUNK_SIZE; ++i)
        tc->points[i] = (record_array_t){0};

    if (wal_init(&tc->wal, path, tc->base_offset, main) < 0)
        return TS_E_WAL_INIT_FAIL;

    return 0;
}

static void ts_chunk_destroy(timeseries_chunk_t *tc)
{
    if (tc->base_offset != 0) {
        for (int i = 0; i < TS_CHUNK_SIZE; ++i) {
            da_free(&tc->points[i]);
            tc->points[i].length = 0;
        }
    }

    wal_delete(&tc->wal);

    tc->base_offset = 0;
    tc->start_ts    = 0;
    tc->end_ts      = 0;
    tc->max_index   = 0;
}

static int ts_chunk_record_fit(const timeseries_chunk_t *tc, uint64_t sec)
{
    // Relative offset inside the 2 arrays
    ssize_t index = sec - tc->base_offset;

    if (index < 0)
        return -1;

    // Index outside of the head chunk range
    // 1. Flush the tail chunk to persistence
    // 2. Create a new head chunk set on the next 15 min
    // 3. Make the current head chunk the new tail chunk
    if (index > TS_CHUNK_SIZE - 1)
        return 1;

    return 0;
}

/*
 * Compare two Record structures, taking into account the timestamp of each.
 *
 * It returns an integer following the rules:
 *
 * . -1 r1 is lesser than r2
 * .  0 r1 is equal to r2
 * .  1 r1 is greater than r2
 */
static int record_cmp(const record_t *r1, const record_t *r2)
{
    if (r1->timestamp == r2->timestamp)
        return 0;
    if (r1->timestamp > r2->timestamp)
        return 1;
    return -1;
}

/*
 * Set a record in the chunk at a relative index based on the first timestamp
 * stored e.g.
 *
 * Base 1782999282
 * Timestamp 1782999288
 * Index 6
 *
 * The information stored is initially formed by a timestamp and a long double
 * value.
 *
 * Remarks
 *
 * - This function assumes the record will fit in the chunk, by previously
 *   checking it with `ts_chunk_record_fit(2)`
 *
 */
static int ts_chunk_set_record(timeseries_chunk_t *tc, uint64_t sec,
                               uint64_t nsec, double_t value)
{
    // Relative offset inside the 2 arrays
    size_t index   = tc->base_offset == 0 ? 0 : sec - tc->base_offset;

    // Append to the last record in this timestamp bucket
    record_t point = {
        .value     = value,
        .timestamp = sec * (uint64_t)1e9 + nsec,
        .tv        = (struct timespec){.tv_sec = sec, .tv_nsec = nsec},
        .is_set    = 1,
    };

    // Check if the timestamp is ordered
    if (tc->end_ts != 0 && tc->end_ts > point.timestamp) {
        ssize_t i = 0;
        da_bsearch(&tc->points[index], &point, record_cmp, &i);
        if (i < 0)
            return TS_E_UNKNOWN;
        // Simple shift of existing elements, maybe worth adding a
        // support vector for out of order (in chunk range) records and
        // merge them when flushing, must profile NB WAL doesn't need any
        // change as it will act as an event log, replayable to obtain
        // the up-to-date state
        da_insert_shift(&tc->points[index], (size_t)i, point);
    } else {
        da_append(&tc->points[index], point);
        tc->max_index = index > tc->max_index ? index : tc->max_index;
    }

    tc->base_offset = point.timestamp < tc->base_offset || tc->base_offset == 0
                          ? point.timestamp
                          : tc->base_offset;
    tc->start_ts    = tc->start_ts == 0 ? point.timestamp : tc->start_ts;
    tc->end_ts = tc->end_ts < point.timestamp ? point.timestamp : tc->end_ts;

    return 0;
}

static int ts_chunk_load(timeseries_chunk_t *tc, const char *pathbuf,
                         uint64_t base_timestamp, int main)
{

    int err = wal_load(&tc->wal, pathbuf, base_timestamp, main);
    if (err < 0)
        return TS_E_WAL_LOAD_FAIL;

    uint8_t *buf = malloc(tc->wal.size + 1);
    if (!buf)
        return TS_E_OOM;
    ssize_t n = read_file(tc->wal.fp, buf);
    if (n < 0)
        return TS_E_UNKNOWN;

    tc->base_offset = base_timestamp;
    for (int i = 0; i < TS_CHUNK_SIZE; ++i)
        tc->points[i] = (record_array_t){0};

    uint8_t *ptr = buf;
    uint64_t timestamp;
    double_t value;

    while (n > 0) {
        timestamp     = read_i64(ptr);
        value         = read_f64(ptr + sizeof(uint64_t));

        uint64_t sec  = timestamp / (uint64_t)1e9;
        uint64_t nsec = timestamp % (uint64_t)1e9;

        if ((err = ts_chunk_set_record(tc, sec, nsec, value)) < 0)
            return err;

        ptr += sizeof(uint64_t) + sizeof(double_t);
        n -= (sizeof(uint64_t) + sizeof(double_t));
    }

    free(buf);

    return 0;
}

int ts_init(timeseries_t *ts)
{
    snprintf(ts->pathbuf, sizeof(ts->pathbuf), "%s/%s/%s", BASE_PATH,
             ts->db_datapath, ts->name);

    if (makedir(ts->pathbuf) < 0)
        return TS_E_UNKNOWN;

    ts_chunk_zero(&ts->head);
    ts_chunk_zero(&ts->head);

    struct dirent **namelist;
    int err = 0, ok = 0;
    int n = scandir(ts->pathbuf, &namelist, NULL, alphasort);
    if (n == -1)
        goto exit;

    for (int i = 0; i < n; ++i) {
        const char *dot = strrchr(namelist[i]->d_name, '.');
        if (strncmp(namelist[i]->d_name, "wal-", 4) == 0 &&
            strncmp(dot, ".log", 4) == 0) {
            uint64_t base_timestamp = atoll(namelist[i]->d_name + 6);
            if (namelist[i]->d_name[4] == 'h') {
                err = ts_chunk_load(&ts->head, ts->pathbuf, base_timestamp, 1);
            } else if (namelist[i]->d_name[4] == 't') {
                err = ts_chunk_load(&ts->prev, ts->pathbuf, base_timestamp, 0);
            }
            ok = err == 0;
        } else if (namelist[i]->d_name[0] == 'c') {
            // There is a log partition
            uint64_t base_timestamp = atoll(namelist[i]->d_name + 3);
            err = partition_load(&ts->partitions[ts->partition_nr++],
                                 ts->pathbuf, base_timestamp);
        }

        free(namelist[i]);

        if (err < 0)
            goto exit;
    }

    free(namelist);

    return ok;

exit:
    free(namelist);
    return err;
}

void ts_close(timeseries_t *ts)
{
    ts_chunk_destroy(&ts->head);
    ts_chunk_destroy(&ts->prev);
    free(ts);
}

static void ts_deinit(timeseries_t *ts)
{
    ts_chunk_destroy(&ts->head);
    ts_chunk_destroy(&ts->prev);
}

static int ts_flush_prev(timeseries_t *ts, const char *path)
{
    // Flush the prev chunk to persistence
    partition_t *pt = &ts->partitions[ts->partition_nr];

    if (!pt->initialized && partition_init(pt, path, ts->head.base_offset) < 0)
        return TS_E_FLUSH_CHUNK_FAIL;

    if (partition_flush_chunk(pt, &ts->prev) < 0)
        return TS_E_FLUSH_CHUNK_FAIL;

    // Clean up the prev chunk and delete it's WAL
    ts_chunk_destroy(&ts->prev);

    return 0;
}

/**
 * Handle insertion of a timestamp that's earlier than the current head chunk.
 *
 * @param ts Pointer to the timeseries.
 * @param pathbuf Path to the timeseries data files.
 * @param timestamp Full timestamp in nanoseconds.
 * @param sec Seconds part of the timestamp.
 * @param nsec Nanoseconds part of the timestamp.
 * @param value Value to insert.
 * @return 0 on success, error code on failure.
 */
static int ts_handle_out_of_order_insert(timeseries_t *ts, uint64_t timestamp,
                                         uint64_t sec, uint64_t nsec,
                                         double_t value)
{
    // If the chunk is empty, it also means the base offset is 0, we set
    // it here with the first record inserted
    if (ts->prev.base_offset == 0) {
        ts_chunk_init(&ts->prev, ts->pathbuf, sec, 0);
    }

    // If we successfully insert the record, we can return
    if (ts_chunk_record_fit(&ts->prev, sec) == 0) {
        // Persist to disk for disaster recovery
        if (wal_append(&ts->prev.wal, timestamp, value) < 0)
            return TS_E_WAL_APPEND_FAIL;

        return ts_chunk_set_record(&ts->prev, sec, nsec, value);
    }

    // Check if the record is older than the prev
    if (ts_chunk_record_fit(&ts->prev, sec) < 0) {
        // Flush current prev to make room for a new one
        if (ts_flush_prev(ts, ts->pathbuf) < 0)
            return TS_E_FLUSH_CHUNK_FAIL;

        // Promote a new prev chunk for the OOO insert
        if (ts_chunk_init(&ts->prev, ts->pathbuf, sec, 0) < 0)
            return TS_E_UNKNOWN;

        // Persist to disk for disaster recovery
        if (wal_append(&ts->prev.wal, timestamp, value) < 0)
            return TS_E_WAL_APPEND_FAIL;

        return ts_chunk_set_record(&ts->prev, sec, nsec, value);
    }

    return TS_E_UNKNOWN;
}

/**
 * Rotate chunks when a new head chunk is needed.
 *
 * @param ts Pointer to the timeseries.
 * @param pathbuf Path to the timeseries data files.
 * @param sec Seconds part of the timestamp.
 * @return 0 on success, error code on failure.
 */
static int ts_rotate_chunks(timeseries_t *ts, uint64_t sec)
{
    // Flush current prev chunk to disk
    if (ts_flush_prev(ts, ts->pathbuf) < 0)
        return TS_E_FLUSH_CHUNK_FAIL;
    // Set the current head as new prev
    ts->prev = ts->head;

    // Reset the current head as new head
    ts_chunk_destroy(&ts->head);
    if (ts_chunk_init(&ts->head, ts->pathbuf, sec, 1) < 0)
        return TS_E_UNKNOWN;

    return 0;
}

/*
 * Set a record in a timeseries.
 *
 * This function sets a record with the specified timestamp and value in the
 * given timeseries. The function handles the storage of records in memory and
 * on disk to ensure data integrity and efficient usage of resources.
 *
 * @param ts A pointer to the timeseries_t structure representing the
 * timeseries.
 * @param timestamp The timestamp of the record to be set, in nanoseconds.
 * @param value The value of the record to be set.
 * @return 0 on success, -1 on failure.
 */
int ts_insert(timeseries_t *ts, uint64_t timestamp, double_t value)
{
    if (!ts)
        return TS_E_NULL_POINTER;

    // Extract seconds and nanoseconds from timestamp
    uint64_t sec  = timestamp / (uint64_t)1e9;
    uint64_t nsec = timestamp % (uint64_t)1e9;

    // if the limit is reached we dump the chunks into disk and create 2 new
    // ones
    if (wal_size(&ts->head.wal) >= TS_FLUSH_SIZE) {
        uint64_t base       = ts->prev.base_offset > 0 ? ts->prev.base_offset
                                                       : ts->head.base_offset;
        size_t partition_nr = ts->partition_nr == 0 ? 0 : ts->partition_nr - 1;

        partition_t *pt     = &ts->partitions[ts->partition_nr];
        if (ts->partitions[partition_nr].clog.base_timestamp < base) {
            if (!pt->initialized && partition_init(pt, ts->pathbuf, base) < 0) {
                return TS_E_INIT_PARTITION_FAIL;
            }
            partition_nr = ts->partition_nr++;
        }

        if (!pt->initialized && partition_init(pt, ts->pathbuf, base) < 0) {
            return TS_E_INIT_PARTITION_FAIL;
        }

        // Dump chunks into disk and create new ones
        if (partition_flush_chunk(pt, &ts->prev) < 0)
            return TS_E_FLUSH_PARTITION_FAIL;

        if (partition_flush_chunk(pt, &ts->head) < 0)
            return TS_E_FLUSH_PARTITION_FAIL;

        // Reset clean both head and prev in-memory chunks
        ts_deinit(ts);
    }

    // Let it crash for now if the timestamp is out of bounds in the ooo? What
    // should we do if we receive a timestamp that is smaller than both the head
    // base and the prev base?
    if (sec < ts->head.base_offset) {
        return ts_handle_out_of_order_insert(ts, timestamp, sec, nsec, value);
    }

    if (ts->head.base_offset == 0 &&
        ts_chunk_init(&ts->head, ts->pathbuf, sec, 1) < 0)
        return TS_E_UNKNOWN;

    // Persist to disk for disaster recovery
    if (wal_append(&ts->head.wal, timestamp, value) < 0)
        return TS_E_WAL_APPEND_FAIL;

    // Check if the timestamp is in range of the current chunk, otherwise
    // create a new in-memory segment
    if (ts_chunk_record_fit(&ts->head, sec) > 0 &&
        ts_rotate_chunks(ts, sec) < 0)
        return TS_E_UNKNOWN;

    // Insert it into the head chunk
    return ts_chunk_set_record(&ts->head, sec, nsec, value);
}

static int ts_search_index(const timeseries_chunk_t *tc, uint64_t sec,
                           const record_t *target, record_t *dst)
{
    if (tc->base_offset > sec)
        return 1;

    size_t index = 0;
    ssize_t idx  = 0;

    if ((index = sec - tc->base_offset) > TS_CHUNK_SIZE)
        return -1;

    if (tc->points[index].length < LINEAR_THRESHOLD)
        da_search(&tc->points[index], target, record_cmp, &idx);
    else
        da_bsearch(&tc->points[index], target, record_cmp, &idx);

    if (idx < 0)
        return 1;

    *dst = da_get(tc->points[index], idx);

    return 0;
}

/*
 * Finds a record in a timeseries data structure.
 *
 * This function searches for a record with the specified timestamp in the
 * given timeseries data structure. It first checks the in-memory chunks
 * (head and previous) and then looks for the record on disk if not found
 * in memory.
 *
 * @param ts A pointer to the timeseries_t structure representing the
 * timeseries.
 * @param timestamp The timestamp of the record to be found, specified in
 * nanoseconds since the Unix epoch.
 * @param r A pointer to a Record structure where the found record will be
 * stored.
 * @return 0 if the record is found and successfully stored in 'r', -1 if an
 * error occurs, or a negative value indicating the result of the search:
 *         - 1 if the record is found in memory.
 *         - 0 if the record is not found in memory but found on disk.
 *         - Negative value if an error occurs during the search.
 */
int ts_find(const timeseries_t *ts, uint64_t timestamp, record_t *r)
{
    uint64_t sec    = timestamp / (uint64_t)1e9;
    record_t target = {.timestamp = timestamp};
    int err         = 0;

    // First check the current chunk
    if (ts->head.base_offset > 0 && ts->head.base_offset <= sec) {
        err = ts_search_index(&ts->head, sec, &target, r);
        if (err <= 0)
            return err;
    }
    // Then check the OOO chunk
    if (err != 1 && ts->prev.base_offset > 0) {
        err = ts_search_index(&ts->prev, sec, &target, r);
        if (err <= 0)
            return err;
    }

    // We have no persistence, can't stop here, no record found
    if (ts->partitions[0].start_ts == 0)
        return -1;

    // Look for the record on disk
    uint8_t buf[RECORD_BINARY_SIZE];
    ssize_t partition_i = 0;
    for (size_t n = 0; n <= ts->partition_nr; ++n) {
        if (ts->partitions[n].clog.base_timestamp > 0 &&
            ts->partitions[n].clog.base_timestamp <= sec) {
            uint64_t curr_ts =
                ts->partitions[n].clog.base_timestamp * (uint64_t)1e9 +
                ts->partitions[n].clog.base_ns;
            partition_i = curr_ts > timestamp ? n - 1 : n;
        }
        if (ts->partitions[n].clog.base_timestamp > sec)
            break;
    }

    // This shouldn't happen, but in case, we return timestamp not found
    if (partition_i < 0)
        return -1;

    // Fetch single record from the partition
    err = partition_find(&ts->partitions[partition_i], buf, timestamp);
    if (err < 0)
        return -1;

    // We got a match
    ts_record_read(r, buf);

    return 0;
}

static void ts_chunk_range(const timeseries_chunk_t *tc, uint64_t t0,
                           uint64_t t1, record_array_t *out)
{
    uint64_t sec0 = t0 / (uint64_t)1e9;
    uint64_t sec1 = t1 / (uint64_t)1e9;
    size_t low, high, idx_low = 0, idx_high = 0;
    // Find the low
    low             = sec0 - tc->base_offset;
    record_t target = {.timestamp = t0};

    if (tc->points[low].length < LINEAR_THRESHOLD)
        da_search(&tc->points[low], &target, record_cmp, &idx_low);
    else
        da_bsearch(&tc->points[low], &target, record_cmp, &idx_low);

    // Find the high
    // TODO let it crash on edge cases for now (can this happen now?)
    high             = sec1 - tc->base_offset;
    target.timestamp = t1;

    if (tc->points[high].length < LINEAR_THRESHOLD)
        da_search(&tc->points[high], &target, record_cmp, &idx_high);
    else
        da_bsearch(&tc->points[high], &target, record_cmp, &idx_high);

    // Collect the records
    for (size_t i = low; i < high + 1; ++i) {

        size_t end = i == high ? idx_high : tc->points[i].length;

        for (size_t j = idx_low; j < end + 1; ++j) {
            record_t r = da_get(tc->points[i], j);
            if (r.is_set == 1)
                da_append(out, r);
        }
        idx_low = 0;
    }
}

// Helper function to fetch records from a partition within a given time range
static int fetch_records_from_partition(const partition_t *partition,
                                        uint64_t start, uint64_t end,
                                        record_array_t *out)
{
    // TODO malloc
    uint8_t buf[4096];
    uint8_t *ptr      = &buf[0];
    size_t record_len = 0;
    int n             = partition_range(partition, ptr, start, end);
    if (n < 0)
        return -1;

    while (n > 0) {
        record_t record;
        record_len = ts_record_read(&record, ptr);
        da_append(out, record);
        ptr += record_len;
        n -= record_len;
        start = record.timestamp;
    }
    return 0;
}

/**
 * Check if the requested range is within the head chunk.
 *
 * @param ts Pointer to the timeseries.
 * @param sec0 Seconds part of the start timestamp.
 * @param start Start timestamp in nanoseconds.
 * @return true if range is in head chunk, false otherwise.
 */
static inline bool is_range_in_head_chunk(const timeseries_t *ts, uint64_t sec0,
                                          uint64_t start)
{
    return (ts->head.base_offset > 0 && ts->head.base_offset <= sec0 &&
            ts->head.start_ts <= start &&
            sec0 - ts->head.base_offset <= TS_CHUNK_SIZE);
}

/**
 * Check if the requested range is within the prev chunk.
 *
 * @param ts Pointer to the timeseries.
 * @param sec0 Seconds part of the start timestamp.
 * @param end End timestamp in nanoseconds.
 * @return true if range is in prev chunk, false otherwise.
 */
static inline bool is_range_in_prev_chunk(const timeseries_t *ts, uint64_t sec0,
                                          uint64_t end)
{
    return (ts->prev.base_offset > 0 && ts->prev.base_offset <= sec0 &&
            ts->prev.start_ts <= end &&
            sec0 - ts->prev.base_offset <= TS_CHUNK_SIZE);
}

static size_t find_starting_partition(const timeseries_t *ts, uint64_t start)
{
    size_t partition_i = 0;

    while (partition_i < ts->partition_nr &&
           ts->partitions[partition_i].end_ts < start)
        partition_i++;

    return partition_i;
}

/**
 * Retrieve records from a timeseries within a specified time range.
 *
 * This function fetches all records with timestamps between start and end
 * from the given timeseries and stores them in the provided output array.
 *
 * @param ts A pointer to the timeseries to query.
 * @param start The start timestamp of the range, in nanoseconds.
 * @param end The end timestamp of the range, in nanoseconds.
 * @param out Pointer to a record_array_t to store the results.
 * @return 0 on success, error code on failure.
 */
int ts_range(const timeseries_t *ts, uint64_t start, uint64_t end,
             record_array_t *out)
{
    if (!ts || !out)
        return TS_E_NULL_POINTER;

    if (start > end) {
        return TS_E_INVALID_RANGE;
    }

    uint64_t sec0 = start / (uint64_t)1e9;

    // Check if the range falls in the head chunk
    if (is_range_in_head_chunk(ts, sec0, start)) {
        ts_chunk_range(&ts->head, start, end, out);
        return 0;
    }

    // Check if the range falls in the prev chunk
    if (is_range_in_prev_chunk(ts, sec0, end)) {
        ts_chunk_range(&ts->prev, start, end, out);
        return 0;
    }

    // Search in the persistence
    const partition_t *curr_p = NULL;
    size_t partition_i        = find_starting_partition(ts, start);

    // Fetch records from partitions within the time range
    while (partition_i < ts->partition_nr &&
           ts->partitions[partition_i].start_ts <= end) {
        curr_p = &ts->partitions[partition_i];

        // Fetch records from the current partition
        if (curr_p->end_ts >= end) {
            if (fetch_records_from_partition(curr_p, start, end, out) < 0)
                return -1;
            return 0;
        } else {
            if (fetch_records_from_partition(curr_p, start, curr_p->end_ts,
                                             out) < 0)
                return -1;
            start = curr_p->end_ts;
            partition_i++;
        }
    }

    // Fetch records from the previous chunk if it exists
    if (ts->prev.base_offset != 0) {
        ts_chunk_range(&ts->prev, start, end, out);
        start = da_back(&ts->prev.points[ts->prev.max_index]).timestamp;
    }

    // Fetch records from the current chunk if it exists
    if (ts->head.base_offset != 0) {
        ts_chunk_range(&ts->head, ts->head.start_ts, end, out);
    }

    return 0;
}

void ts_print(const timeseries_t *ts)
{
    for (int i = 0; i < TS_CHUNK_SIZE; ++i) {
        record_array_t p = ts->head.points[i];
        for (size_t j = 0; j < p.length; ++j) {
            record_t r = da_get(p, j);
            if (!r.is_set)
                continue;
            log_info("%" PRIu64 " {.sec: %lu, .nsec: %lu, .value: %.02f}",
                     r.timestamp, r.tv.tv_sec, r.tv.tv_nsec, r.value);
        }
    }
}

size_t ts_record_timestamp(const uint8_t *buf)
{
    return read_i64(buf + sizeof(uint64_t));
}

size_t ts_record_write(const record_t *r, uint8_t *buf)
{
    // Record full size u64
    size_t record_size = RECORD_BINARY_SIZE;
    write_i64(buf, record_size);
    buf += sizeof(uint64_t);
    // Timestamp u64
    write_i64(buf, r->timestamp);
    buf += sizeof(uint64_t);
    // Value
    write_f64(buf, r->value);
    return record_size;
}

size_t ts_record_read(record_t *r, const uint8_t *buf)
{
    // Record size u64
    size_t record_size = read_i64(buf);
    buf += sizeof(uint64_t);
    // Timestamp u64
    r->timestamp  = read_i64(buf);
    r->tv.tv_sec  = r->timestamp / (uint64_t)1e9;
    r->tv.tv_nsec = r->timestamp % (uint64_t)1e9;
    buf += sizeof(uint64_t);
    // Value
    r->value = read_f64(buf);

    return record_size;
}

size_t ts_record_batch_write(const record_t *r[], uint8_t *buf, size_t count)
{
    uint64_t last_timestamp = r[count - 1]->timestamp;
    // For now we assume fixed size of records
    uint64_t batch_size = count * ((sizeof(uint64_t) * 2) + sizeof(double_t));
    write_i64(buf, batch_size);
    write_i64(buf + sizeof(uint64_t), last_timestamp);
    size_t offset = sizeof(uint64_t) * 2;
    for (size_t i = 0; i < count; ++i) {
        offset += ts_record_write(r[i], buf + offset);
    }
    return batch_size;
}
