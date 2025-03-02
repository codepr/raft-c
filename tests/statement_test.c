#include "../src/statement.h"
#include "test_helpers.h"
#include "tests.h"
#include <stdio.h>

static int parse_create_db_test(void)
{
    printf("%s..", __FUNCTION__);
    fflush(stdout);

    stmt_t *stmt = stmt_parse("CREATEDB test-db");

    ASSERT_EQ(stmt->type, STMT_CREATEDB);
    ASSERT_SEQ(stmt->create.db_name, "test-db");

    stmt_free(stmt);

    printf("PASS\n");
    return 0;
}

static int parse_create_ts_test(void)
{
    printf("%s..", __FUNCTION__);
    fflush(stdout);

    stmt_t *stmt = stmt_parse("CREATE ts-test");

    ASSERT_EQ(stmt->type, STMT_CREATE);
    ASSERT_SEQ(stmt->create.ts_name, "ts-test");

    stmt_free(stmt);

    printf("PASS\n");
    return 0;
}

static int parse_create_ts_retention_duplication_test(void)
{
    printf("%s..", __FUNCTION__);
    fflush(stdout);

    stmt_t *stmt = stmt_parse("CREATE ts-test 3d 'ignore'");

    ASSERT_EQ(stmt->type, STMT_CREATE);
    ASSERT_SEQ(stmt->create.ts_name, "ts-test");
    ASSERT_TRUE(stmt->create.has_retention,
                "FAIL: has_retention should be true\n");
    ASSERT_EQ(stmt->create.retention.interval.value, 3);
    ASSERT_SEQ(stmt->create.retention.interval.unit, "d");
    ASSERT_TRUE(stmt->create.has_duplication,
                "FAIL: has_duplication should be true\n");
    ASSERT_SEQ(stmt->create.duplication, "ignore");

    stmt_free(stmt);

    printf("PASS\n");
    return 0;
}

static int parse_insert_test(void)
{
    printf("%s..", __FUNCTION__);
    fflush(stdout);

    stmt_t *stmt =
        stmt_parse("INSERT INTO test-ts values (87829132377, 12.2344)");

    ASSERT_EQ(stmt->insert.record_array.length, 1);
    ASSERT_EQ(stmt->insert.record_array.items[0].timestamp, 87829132377);
    ASSERT_FEQ(stmt->insert.record_array.items[0].value, 12.2344);
    ASSERT_SEQ(stmt->insert.ts_name, "test-ts");

    stmt_free(stmt);

    printf("PASS\n");
    return 0;
}

static int parse_insert_single_test(void)
{
    printf("%s..", __FUNCTION__);
    fflush(stdout);

    stmt_t *stmt = stmt_parse("INSERT INTO test-ts value 12.2344");

    ASSERT_EQ(stmt->insert.record_array.length, 1);
    ASSERT_FEQ(stmt->insert.record_array.items[0].value, 12.2344);
    ASSERT_SEQ(stmt->insert.ts_name, "test-ts");

    stmt_free(stmt);

    printf("PASS\n");
    return 0;
}

static int parse_insert_multi_test(void)
{
    printf("%s..", __FUNCTION__);
    fflush(stdout);

    size_t query_len     = 512;
    size_t records_len   = 20;
    char *part           = "INSERT INTO test-ts VALUES ";
    char *query          = malloc(query_len);
    uint64_t *timestamps = calloc(query_len, sizeof(uint64_t));
    double_t *values     = calloc(query_len, sizeof(double_t));

    size_t offset        = snprintf(query, query_len, "%s", part);

    for (size_t i = 0; i < records_len; ++i) {
        size_t remaining   = query_len - offset;

        // Generate sample timestamp and value
        uint64_t timestamp = 100000 + i * 1000;
        double_t value     = (double_t)(rand() % 10000) / 10.0;

        timestamps[i]      = timestamp;
        values[i]          = value;

        int written = snprintf(query + offset, remaining, "(%llu, %.1f),",
                               timestamp, value);

        if (written < 0 || (size_t)written >= remaining) {
            fprintf(stderr, "FAIL: Query buffer too small\n");
            break;
        }

        offset += written;
    }

    query[offset - 1] = '\0';

    stmt_t *stmt      = stmt_parse(query);

    ASSERT_EQ(stmt->insert.record_array.length, records_len);
    ASSERT_SEQ(stmt->insert.ts_name, "test-ts");

    for (size_t i = 0; i < records_len; ++i) {
        ASSERT_EQ(stmt->insert.record_array.items[i].timestamp, timestamps[i]);
        ASSERT_FEQ(stmt->insert.record_array.items[i].value, values[i]);
    }

    stmt_free(stmt);
    free(timestamps);
    free(values);
    free(query);

    printf("PASS\n");
    return 0;
}

static int parse_delete_test(void)
{
    printf("%s..", __FUNCTION__);
    fflush(stdout);

    stmt_t *stmt = stmt_parse("DELETE test-db");

    ASSERT_SEQ(stmt->delete.db_name, "test-db");

    stmt_free(stmt);

    printf("PASS\n");
    return 0;
}

static int parse_delete_ts_test(void)
{
    printf("%s..", __FUNCTION__);
    fflush(stdout);

    stmt_t *stmt = stmt_parse("DELETE ts-test FROM test-db");

    ASSERT_SEQ(stmt->delete.db_name, "test-db");
    ASSERT_SEQ(stmt->delete.ts_name, "ts-test");

    stmt_free(stmt);

    printf("PASS\n");
    return 0;
}

static int parse_select_test(void)
{
    printf("%s..", __FUNCTION__);
    fflush(stdout);

    int rc       = 0;

    stmt_t *stmt = stmt_parse("SELECT records FROM ts-test BETWEEN 2382913 AND "
                              "39238293 WHERE test-value > 12.2 AND "
                              "test-value < 16.2 SAMPLE BY 4d");

    if (!stmt) {
        fprintf(stderr, "FAIL: parsing failed\n");
        rc = -1;
        goto exit;
    }

    ASSERT_SEQ(stmt->select.ts_name, "ts-test");
    ASSERT_EQ(stmt->select.sampling.interval.value, 4);
    ASSERT_SEQ(stmt->select.sampling.interval.unit, "d");
    ASSERT_EQ(stmt->select.timeunit.tsinterval.start, 2382913);
    ASSERT_EQ(stmt->select.timeunit.tsinterval.end, 39238293);
    ASSERT_EQ(stmt->select.where->boolean_op, BOOL_OP_AND);
    ASSERT_FEQ(stmt->select.where->value, 12.2);
    ASSERT_EQ(stmt->select.where->operator, OP_GREATER);
    ASSERT_FEQ(stmt->select.where->right->value, 16.2);
    ASSERT_EQ(stmt->select.where->right->operator, OP_LESS);

    printf("PASS\n");

exit:
    stmt_free(stmt);

    return rc;
}

static int parse_select_fn_test(void)
{
    printf("%s..", __FUNCTION__);
    fflush(stdout);

    int rc = 0;

    stmt_t *stmt =
        stmt_parse("SELECT min(records) FROM ts-test BETWEEN 2382913 AND "
                   "39238293 ");

    if (!stmt) {
        fprintf(stderr, "FAIL: parsing failed\n");
        rc = -1;
        goto exit;
    }

    ASSERT_SEQ(stmt->select.ts_name, "ts-test");
    ASSERT_EQ(stmt->select.function, FN_MIN);
    ASSERT_EQ(stmt->select.timeunit.tsinterval.start, 2382913);
    ASSERT_EQ(stmt->select.timeunit.tsinterval.end, 39238293);

    printf("PASS\n");

exit:
    stmt_free(stmt);

    return rc;
}

static int parse_select_date_test(void)
{
    printf("%s..", __FUNCTION__);
    fflush(stdout);

    int rc       = 0;

    stmt_t *stmt = stmt_parse(
        "SELECT latest(records) FROM ts-test BETWEEN '2025-03-01' AND "
        "'2025-05-01' ");

    if (!stmt) {
        fprintf(stderr, "FAIL: parsing failed\n");
        rc = -1;
        goto exit;
    }

    ASSERT_SEQ(stmt->select.ts_name, "ts-test");
    ASSERT_EQ(stmt->select.function, FN_LATEST);
    ASSERT_SEQ(stmt->select.timeunit.dateinterval.start, "2025-03-01");
    ASSERT_SEQ(stmt->select.timeunit.dateinterval.end, "2025-05-01");

    printf("PASS\n");

exit:
    stmt_free(stmt);

    return rc;
}

static int parse_select_limit_test(void)
{
    printf("%s..", __FUNCTION__);
    fflush(stdout);

    int rc       = 0;

    stmt_t *stmt = stmt_parse(
        "SELECT latest(records) FROM ts-test BETWEEN '2025-03-01' AND "
        "'2025-05-01' LIMIT 20");

    if (!stmt) {
        fprintf(stderr, "FAIL: parsing failed\n");
        rc = -1;
        goto exit;
    }

    ASSERT_SEQ(stmt->select.ts_name, "ts-test");
    ASSERT_EQ(stmt->select.function, FN_LATEST);
    ASSERT_EQ(stmt->select.limit, 20);
    ASSERT_SEQ(stmt->select.timeunit.dateinterval.start, "2025-03-01");
    ASSERT_SEQ(stmt->select.timeunit.dateinterval.end, "2025-05-01");

    printf("PASS\n");

exit:
    stmt_free(stmt);

    return rc;
}

static int parse_select_where_test(void)
{
    printf("%s..", __FUNCTION__);
    fflush(stdout);

    int rc       = 0;

    stmt_t *stmt = stmt_parse("SELECT avg(records) FROM ts-test WHERE value > "
                              "3.14159 AND timestamp < 2.5e-3");

    if (!stmt) {
        fprintf(stderr, "FAIL: parsing failed\n");
        rc = -1;
        goto exit;
    }

    ASSERT_SEQ(stmt->select.ts_name, "ts-test");
    ASSERT_EQ(stmt->select.function, FN_AVG);
    ASSERT_SEQ(stmt->select.where->key, "value");
    ASSERT_FEQ(stmt->select.where->value, 3.14159);
    ASSERT_EQ(stmt->select.where->operator, OP_GREATER);
    ASSERT_EQ(stmt->select.where->boolean_op, BOOL_OP_AND);
    ASSERT_SEQ(stmt->select.where->right->key, "timestamp");
    ASSERT_FEQ(stmt->select.where->right->value, 2.5e-3);
    ASSERT_EQ(stmt->select.where->right->operator, OP_LESS);

    printf("PASS\n");

exit:
    stmt_free(stmt);

    return rc;
}

int parser_test(void)
{
    printf("* %s\n\n", __FUNCTION__);
    fflush(stdout);

    int cases   = 12;
    int success = cases;

    success += parse_create_db_test();
    success += parse_create_ts_test();
    success += parse_delete_ts_test();
    success += parse_insert_test();
    success += parse_delete_test();
    success += parse_select_test();
    success += parse_select_fn_test();
    success += parse_select_date_test();
    success += parse_select_limit_test();
    success += parse_select_where_test();
    success += parse_insert_multi_test();
    success += parse_insert_single_test();
    success += parse_create_ts_retention_duplication_test();

    printf("\nTest suite summary: %d passed, %d failed\n", success,
           cases - success);

    return 0;
}
