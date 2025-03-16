#include "../src/darray.h"
#include "../src/timeseries.h"
#include "test_helpers.h"
#include <time.h>
#include <unistd.h>

#define POINTSNR  90
#define INTERVAL  115000
#define TESTDIR   "logdata/testdb/"
#define TESTCASES 100

static uint64_t timestamps[POINTSNR] = {0};

static int min_timeseries_test(timeseries_t *ts)
{
    TEST_HEADER;

    record_t r = {0};
    if (ts_min(ts, timestamps[10], timestamps[80], &r) < 0) {
        fprintf(stderr, " FAIL: ts_min failed\n");
        return -1;
    }

    ASSERT_EQ(r.timestamp, timestamps[10]);
    ASSERT_FEQ(r.value, 10.0);

    TEST_FOOTER;

    return 0;
}

static int max_timeseries_test(timeseries_t *ts)
{
    TEST_HEADER;

    record_t r = {0};
    if (ts_max(ts, timestamps[10], timestamps[80], &r) < 0) {
        fprintf(stderr, " FAIL: ts_max failed\n");
        return -1;
    }

    ASSERT_EQ(r.timestamp, timestamps[80]);
    ASSERT_FEQ(r.value, 80.0);

    TEST_FOOTER;

    return 0;
}

static int last_timeseries_test(timeseries_t *ts)
{
    TEST_HEADER;

    record_t r = {0};
    if (ts_last(ts, &r) < 0) {
        fprintf(stderr, " FAIL: ts_last failed\n");
        return -1;
    }

    ASSERT_EQ(r.timestamp, timestamps[POINTSNR - 1]);

    TEST_FOOTER;

    return 0;
}

static int first_timeseries_test(timeseries_t *ts)
{
    TEST_HEADER;

    record_t r = {0};
    if (ts_first(ts, &r) < 0) {
        fprintf(stderr, " FAIL: ts_first failed\n");
        return -1;
    }

    ASSERT_EQ(r.timestamp, timestamps[0]);

    TEST_FOOTER;

    return 0;
}

static int avg_sample_timeseries_test(timeseries_t *ts)
{
    TEST_HEADER;

    // 1ms interval
    uint64_t interval_ns = 1000 * (uint64_t)1e6;

    record_array_t out   = {0};

    if (ts_avg_sample(ts, timestamps[25], timestamps[75], interval_ns, &out) <
        0) {
        fprintf(stderr, " FAIL: ts_avg_sample failed\n");
        return -1;
    }

    ASSERT_TRUE(out.length > 0, "FAIL: Sampled records should be more than 0");
    for (size_t i = 0; i < out.length; ++i) {
        ASSERT_TRUE(out.items[i].timestamp % interval_ns == 0,
                    " FAIL: Sample records should have timestamps multiple of "
                    "the set interval\n");
    }

    da_free(&out);

    // 2000us interval
    interval_ns = 2000 * (uint64_t)1e6;

    if (ts_avg_sample(ts, timestamps[25], timestamps[75], interval_ns, &out) <
        0) {
        fprintf(stderr, " FAIL: ts_avg_sample failed\n");
        return -1;
    }

    ASSERT_TRUE(out.length > 0,
                "FAIL: Sampled records should be more than 0\n");
    for (size_t i = 0; i < out.length; ++i) {
        ASSERT_TRUE(out.items[i].timestamp % interval_ns == 0,
                    " FAIL: Sample records should have timestamps multiple of "
                    "the set interval\n");
    }

    da_free(&out);

    TEST_FOOTER;

    return 0;
}

static int scan_entire_timeseries_test(timeseries_t *ts)
{
    TEST_HEADER;

    record_array_t out = {0};

    if (ts_scan(ts, &out, NULL, NULL) < 0) {
        fprintf(stderr, " FAIL: ts_scan failed\n");
        return -1;
    }

    ASSERT_EQ(out.length, POINTSNR);

    for (int i = 0; i < POINTSNR; ++i) {
        ASSERT_EQ(out.items[i].timestamp, timestamps[i]);
        ASSERT_EQ(out.items[i].value, i);
    }

    TEST_FOOTER;

    return 0;
}

static int scan_entire_timeseries_out_of_order_test(timeseries_t *ts)
{
    TEST_HEADER;

    int rc              = 0;
    int cases           = TESTCASES / 2;
    int index           = 0;
    int seen[TESTCASES] = {0};
    uint64_t newts      = 0;
    uint64_t delta      = 3e4;
    double_t value      = 0.0f;
    record_array_t out  = {0};

    for (int i = 0; i < cases; ++i) {
        // Not the most efficient way to do it..
        do {
            index = (double_t)rand() / RAND_MAX * (POINTSNR - 1);
        } while (seen[index]); // Retry if duplicate

        seen[index] = 1; // Mark value as used
        newts       = timestamps[index] + delta;
        value       = (double_t)(rand() % cases);

        if (ts_insert(ts, newts, value) < 0) {
            fprintf(stderr, " FAIL: ts_insert failed for record {%llu, %.2f}\n",
                    timestamps[index], value);
            rc = -1;
            goto exit;
        }
    }

    if (ts_scan(ts, &out, NULL, NULL) < 0) {
        fprintf(stderr, " FAIL: ts_scan failed\n");
        return -1;
    }

    // This is pretty brittle
    // TODO make it decent
    ASSERT_EQ(out.length, POINTSNR + TESTCASES + TESTCASES / 2);

exit:

    TEST_FOOTER;

    return rc;
}

// Test ts_insert with NULL timeseries
static int insert_null_test(timeseries_t *ts)
{
    (void)ts;
    TEST_HEADER;

    int result = ts_insert(NULL, 1000000000ULL, 42.0);
    ASSERT_EQ(result, TS_E_NULL_POINTER);

    TEST_FOOTER;

    return 0;
}

static int find_single_record_test(timeseries_t *ts)
{
    TEST_HEADER;

    int cases  = TESTCASES;
    int index  = 0;
    record_t r = {0};

    for (int i = 0; i < cases; ++i) {
        index = rand() % (POINTSNR - 1);

        if (ts_find(ts, timestamps[index], &r) < 0) {
            fprintf(stderr, " FAIL: ts_find failed for timestamp %llu\n",
                    timestamps[index]);
            return -1;
        }

        ASSERT_EQ(timestamps[index], r.timestamp);
        ASSERT_FEQ((double_t)index, r.value);
    }

    TEST_FOOTER;

    return 0;
}

static int find_range_invalid_test(timeseries_t *ts)
{
    TEST_HEADER;

    record_array_t out = {0};
    int result         = ts_range(ts, timestamps[10], timestamps[8], &out);

    ASSERT_EQ(result, TS_E_INVALID_RANGE);

    da_free(&out);
    TEST_FOOTER;

    return 0;
}

static int find_range_null_test(timeseries_t *ts)
{
    TEST_HEADER;

    int result = ts_range(ts, timestamps[2], timestamps[8], NULL);

    ASSERT_EQ(result, TS_E_NULL_POINTER);

    TEST_FOOTER;

    return 0;
}

static int find_range_records_test(timeseries_t *ts)
{
    TEST_HEADER;

    int rc             = 0;
    int start          = 0;
    int end            = 0;
    int cases          = TESTCASES;
    record_array_t out = {0};

    for (int i = 0; i < cases; ++i) {
        start = rand() % (POINTSNR - 1);
        end   = rand() % ((POINTSNR - 1) + 1 - start) + start;

        if (ts_range(ts, timestamps[start], timestamps[end], &out) < 0) {
            fprintf(stderr, " FAIL: ts_range failed for range [%llu - %llu]\n",
                    timestamps[start], timestamps[end]);
            rc = -1;
            goto exit;
        }

        assert(out.length == (end - start) + 1);

        for (size_t i = 0; i < out.length; i++) {
            record_t r = out.items[i];
            ASSERT_EQ(timestamps[start + i], r.timestamp);
            ASSERT_FEQ((double_t)start + i, r.value);
        }

        da_reset(&out);
    }

exit:
    da_free(&out);

    TEST_FOOTER;

    return rc;
}

static int insert_out_of_order_test(timeseries_t *ts)
{
    TEST_HEADER;

    int rc              = 0;
    int cases           = TESTCASES / 2;
    int index           = 0;
    int seen[TESTCASES] = {0};
    uint64_t newts      = 0;
    uint64_t delta      = 3e4;
    double_t value      = 0.0f;
    record_t r          = {0};

    for (int i = 0; i < cases; ++i) {
        // Not the most efficient way to do it..
        do {
            index = (double_t)rand() / RAND_MAX * (POINTSNR - 1);
        } while (seen[index]); // Retry if duplicate

        seen[index] = 1; // Mark value as used
        newts       = timestamps[index] + delta;
        value       = (double_t)(rand() % cases);

        if (ts_insert(ts, newts, value) < 0) {
            fprintf(stderr, " FAIL: ts_insert failed for record {%llu, %.2f}\n",
                    timestamps[index], value);
            rc = -1;
            goto exit;
        }
        if (ts_find(ts, newts, &r) < 0) {
            fprintf(stderr, " FAIL: ts_find failed for timestamp %llu\n",
                    timestamps[index]);
            rc = -1;
            goto exit;
        }

        ASSERT_EQ(newts, r.timestamp);
        ASSERT_FEQ(value, r.value);
    }

exit:

    TEST_FOOTER;

    return rc;
}

static int insert_out_of_bounds_test(timeseries_t *ts)
{
    TEST_HEADER;

    int cases           = TESTCASES / 2;
    int index           = 0;
    int seen[TESTCASES] = {0};
    uint64_t newts      = 0;
    uint64_t delta      = 5e9;
    double_t value      = 0.0f;
    record_t r          = {0};

    for (int i = 0; i < cases; ++i) {
        // Not the most efficient way to do it..
        do {
            index = (double_t)rand() / RAND_MAX * (POINTSNR - 1);
        } while (seen[index]); // Retry if duplicate

        seen[index] = 1; // Mark value as used

        newts       = timestamps[index] + delta;
        value       = (double_t)(rand() % cases);

        if (ts_insert(ts, newts, value) < 0) {
            fprintf(stderr, " FAIL: ts_insert failed for record {%llu, %.2f}\n",
                    timestamps[index], value);
            return -1;
        }
        if (ts_find(ts, newts, &r) < 0) {
            fprintf(stderr, " FAIL: ts_find failed for timestamp %llu\n",
                    timestamps[index]);
            return -1;
        }

        ASSERT_EQ(newts, r.timestamp);
        ASSERT_FEQ(value, r.value);

        r = (record_t){0};
    }

    TEST_FOOTER;

    return 0;
}

int timeseries_test(void)
{
    printf("* %s\n\n", __FUNCTION__);

    int cases   = 14;
    int success = cases;

    srand(47);

    timeseries_db_t *db = tsdb_create("testdb");
    if (!db)
        return -1;

    ts_opts_t opts   = {0};
    timeseries_t *ts = ts_create(db, "temperatures", opts);
    if (!ts)
        return -1;

    // Test setup

    struct timespec tv = {0};
    for (int i = 0; i < POINTSNR; ++i) {
        clock_gettime(CLOCK_REALTIME, &tv);
        uint64_t timestamp = tv.tv_sec * (uint64_t)1e9 + tv.tv_nsec;
        timestamps[i]      = timestamp;
        ts_insert(ts, timestamp, (double_t)i);
        usleep(rand() % INTERVAL);
    }

    success += last_timeseries_test(ts);
    success += first_timeseries_test(ts);
    success += min_timeseries_test(ts);
    success += max_timeseries_test(ts);
    success += avg_sample_timeseries_test(ts);
    success += scan_entire_timeseries_test(ts);
    success += find_single_record_test(ts);
    success += find_range_records_test(ts);
    success += find_range_null_test(ts);
    success += find_range_invalid_test(ts);
    success += insert_null_test(ts);
    success += insert_out_of_order_test(ts);
    success += insert_out_of_bounds_test(ts);
    success += scan_entire_timeseries_out_of_order_test(ts);

    ts_close(ts);
    tsdb_close(db);

    rm_recursive(TESTDIR);

    printf("\n Test suite summary: %d passed, %d failed\n", success,
           cases - success);

    return success < cases ? -1 : 0;
}
