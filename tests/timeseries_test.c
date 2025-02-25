#include "../src/darray.h"
#include "../src/timeseries.h"
#include "test_helpers.h"
#include <time.h>
#include <unistd.h>

#define POINTSNR 90
#define INTERVAL 115000
#define TESTDIR  "logdata/testdb/"

static uint64_t timestamps[POINTSNR] = {0};

// Test ts_insert with NULL timeseries
static int insert_null_test(timeseries_t *ts)
{
    (void)ts;
    printf("%s..", __FUNCTION__);
    fflush(stdout);

    int result = ts_insert(NULL, 1000000000ULL, 42.0);
    ASSERT_EQ(result, TS_E_NULL_POINTER);

    printf("PASS\n");

    return 0;
}

static int find_single_record_test(timeseries_t *ts)
{
    printf("%s..", __FUNCTION__);
    fflush(stdout);

    int cases  = 100;
    int index  = 0;
    record_t r = {0};

    for (int i = 0; i < cases; ++i) {
        index = rand() % (POINTSNR - 1);

        if (ts_find(ts, timestamps[index], &r) < 0) {
            fprintf(stderr, "FAIL: ts_find failed for timestamp %llu\n",
                    timestamps[index]);
            return -1;
        }

        ASSERT_EQ(timestamps[index], r.timestamp);
        ASSERT_FEQ((double_t)index, r.value);
    }

    printf("PASS\n");

    return 0;
}

static int find_range_invalid_test(timeseries_t *ts)
{
    printf("%s..", __FUNCTION__);
    fflush(stdout);

    record_array_t out = {0};
    int result         = ts_range(ts, timestamps[10], timestamps[8], &out);

    ASSERT_EQ(result, TS_E_INVALID_RANGE);

    printf("PASS\n");

    return 0;
}

static int find_range_null_test(timeseries_t *ts)
{
    printf("%s..", __FUNCTION__);
    fflush(stdout);

    int result = ts_range(ts, timestamps[2], timestamps[8], NULL);

    ASSERT_EQ(result, TS_E_NULL_POINTER);

    printf("PASS\n");

    return 0;
}

static int find_range_records_test(timeseries_t *ts)
{
    printf("%s..", __FUNCTION__);
    fflush(stdout);

    int start          = 0;
    int end            = 0;
    int cases          = 100;
    record_array_t out = {0};

    for (int i = 0; i < cases; ++i) {
        start = rand() % (POINTSNR - 1);
        end   = rand() % ((POINTSNR - 1) + 1 - start) + start;

        if (ts_range(ts, timestamps[start], timestamps[end], &out) < 0) {
            fprintf(stderr, "FAIL: ts_range failed for range [%llu - %llu]\n",
                    timestamps[start], timestamps[end]);
            return -1;
        }

        assert(out.length == (end - start) + 1);

        for (size_t i = 0; i < out.length; i++) {
            record_t r = out.items[i];
            ASSERT_EQ(timestamps[start + i], r.timestamp);
            ASSERT_FEQ((double_t)start + i, r.value);
        }

        da_reset(&out);
    }

    da_free(&out);

    printf("PASS\n");

    return 0;
}

static int insert_out_of_order_test(timeseries_t *ts)
{
    printf("%s..", __FUNCTION__);
    fflush(stdout);

    int cases      = 50;
    int index      = 0;
    int seen[100]  = {0};
    uint64_t newts = 0;
    uint64_t delta = 3e4;
    double_t value = 0.0f;
    record_t r     = {0};

    for (int i = 0; i < cases; ++i) {
        // Not the most efficient way to do it..
        do {
            index = (double_t)rand() / RAND_MAX * (POINTSNR - 1);
        } while (seen[index]); // Retry if duplicate

        seen[index] = 1; // Mark value as used
        newts       = timestamps[index] + delta;
        value       = (double_t)(rand() % cases);

        if (ts_insert(ts, newts, value) < 0) {
            fprintf(stderr, "FAIL: ts_insert failed for record {%llu, %.2f}\n",
                    timestamps[index], value);
            return -1;
        }
        if (ts_find(ts, newts, &r) < 0) {
            fprintf(stderr, "FAIL: ts_find failed for timestamp %llu\n",
                    timestamps[index]);
            return -1;
        }

        ASSERT_EQ(newts, r.timestamp);
        ASSERT_FEQ(value, r.value);
    }

    printf("PASS\n");

    return 0;
}

static int insert_out_of_bounds_test(timeseries_t *ts)
{
    printf("%s..", __FUNCTION__);
    fflush(stdout);

    int cases      = 50;
    int index      = 0;
    int seen[100]  = {0};
    uint64_t newts = 0;
    uint64_t delta = 5e9;
    double_t value = 0.0f;
    record_t r     = {0};

    for (int i = 0; i < cases; ++i) {
        // Not the most efficient way to do it..
        do {
            index = (double_t)rand() / RAND_MAX * (POINTSNR - 1);
        } while (seen[index]); // Retry if duplicate

        seen[index] = 1; // Mark value as used

        newts       = timestamps[index] + delta;
        value       = (double_t)(rand() % cases);

        if (ts_insert(ts, newts, value) < 0) {
            fprintf(stderr, "FAIL: ts_insert failed for record {%llu, %.2f}\n",
                    timestamps[index], value);
            return -1;
        }
        if (ts_find(ts, newts, &r) < 0) {
            fprintf(stderr, "FAIL: ts_find failed for timestamp %llu\n",
                    timestamps[index]);
            return -1;
        }

        ASSERT_EQ(newts, r.timestamp);
        ASSERT_FEQ(value, r.value);

        r = (record_t){0};
    }

    printf("PASS\n");

    return 0;
}

int timeseries_test(void)
{
    int cases   = 7;
    int success = cases;

    srand(time(NULL));

    timeseries_db_t *db = tsdb_init("testdb");
    if (!db)
        return -1;

    timeseries_t *ts = ts_create(db, "temperatures", 0, DP_IGNORE);
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

    success += find_single_record_test(ts);
    success += find_range_records_test(ts);
    success += find_range_null_test(ts);
    success += find_range_invalid_test(ts);
    success += insert_null_test(ts);
    success += insert_out_of_order_test(ts);
    success += insert_out_of_bounds_test(ts);

    ts_close(ts);
    tsdb_close(db);

    rm_recursive(TESTDIR);

    printf("\nTest Summary: %d Passed, %d Failed\n", success, cases - success);

    return success < cases ? -1 : 0;
}
