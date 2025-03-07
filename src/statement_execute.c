#include "statement_execute.h"
#include "dbcontext.h"
#include "timeutil.h"
#include <inttypes.h>

/**
 * Process a USE statement and generate appropriate response
 *
 * Attempts to set a database as 'active'.
 * PRE: A database with the given name must exist in the system
 */
static execute_stmt_result_t execute_use(const stmt_t *stmt)
{
    execute_stmt_result_t result = {0};

    if (dbcontext_setactive(stmt->use.db_name) < 0) {
        result.code = EXEC_ERROR_DB_NOT_FOUND;
        snprintf(result.message, MESSAGE_SIZE, "Database '%s' not found",
                 stmt->use.db_name);
    } else {
        result.code = EXEC_SUCCESS_STRING;
        snprintf(result.message, MESSAGE_SIZE, "Database '%s' selected",
                 stmt->use.db_name);
    }

    return result;
}

/**
 * Process a CREATEDB statement and generate appropriate response
 *
 * Attempts to create a database. Before creation, it checks if the database
 * already exists, otherwise it creates the directory structure and initialize
 * it.
 */
static execute_stmt_result_t execute_createdb(const stmt_t *stmt)
{
    execute_stmt_result_t result = {0};
    timeseries_db_t *tsdb        = dbcontext_get(stmt->create.db_name);
    if (tsdb) {
        result.code = EXEC_ERROR_INVALID_VALUE;
        snprintf(result.message, MESSAGE_SIZE, "Database '%s' already exist",
                 stmt->use.db_name);
        return result;
    }

    tsdb = dbcontext_add(stmt->create.db_name);
    if (!tsdb) {
        result.code = EXEC_ERROR_DB_NOT_CREATED;
        snprintf(result.message, MESSAGE_SIZE, "Error creating '%s' database",
                 stmt->create.db_name);
        return result;
    }

    result.code = EXEC_SUCCESS_STRING;
    snprintf(result.message, MESSAGE_SIZE, "Database '%s' created",
             stmt->create.db_name);

    return result;
}

/**
 * Process a CREATE statement and generate appropriate response
 *
 * Attempts to create a time-series inside the currently selected
 * database.
 * PRE: A database must be 'active'
 */
static execute_stmt_result_t execute_create(const stmt_t *stmt)
{
    execute_stmt_result_t result = {0};

    timeseries_db_t *tsdb        = NULL;
    timeseries_t *ts             = NULL;

    // Get the specified database or use active database
    if (stmt->create.db_name[0] != '\0') {
        tsdb = dbcontext_get(stmt->create.db_name);
        if (!tsdb) {
            result.code = EXEC_ERROR_DB_NOT_FOUND;
            snprintf(result.message, MESSAGE_SIZE, "Database '%s' not found",
                     stmt->create.db_name);
            return result;
        }
    }

    tsdb = dbcontext_getactive();
    if (!tsdb) {
        result.code = EXEC_ERROR_DB_NOT_FOUND;
        snprintf(result.message, MESSAGE_SIZE, "No active database found");
        return result;
    }

    ts_opts_t opts = {.retention = stmt->create.retention.value};

    // Create timeseries
    // TODO handle duplication policy
    ts             = ts_create(tsdb, stmt->create.ts_name, opts);
    if (!ts) {
        result.code = EXEC_ERROR_TS_NOT_CREATED;
        snprintf(result.message, MESSAGE_SIZE,
                 "Failed to create timeseries '%s'", stmt->create.ts_name);
        return result;
    }

    result.code = EXEC_SUCCESS_STRING;
    snprintf(result.message, MESSAGE_SIZE, "Timeseries '%s' created",
             stmt->create.ts_name);

    return result;
}

/**
 * Process a INSERT statement and generate appropriate response
 *
 * Attempts to insert point(s) into a specified time-series inside the currently
 * selected database.
 * PRE: A database must be 'active' and the specified time-series must exist
 */
static execute_stmt_result_t execute_insert(const stmt_t *stmt)
{
    execute_stmt_result_t result = {0};

    // Active database not supported yet
    timeseries_db_t *tsdb        = dbcontext_getactive();
    if (!tsdb) {
        result.code = EXEC_ERROR_DB_NOT_FOUND;
        snprintf(result.message, MESSAGE_SIZE,
                 "No database found, create one first");
        return result;
    }

    timeseries_t *ts = ts_get(tsdb, stmt->insert.ts_name);
    if (!ts) {
        result.code = EXEC_ERROR_TS_NOT_FOUND;
        snprintf(result.message, MESSAGE_SIZE, "Timeseries '%s' not found",
                 stmt->insert.ts_name);
        return result;
    }

    int success_count = 0;
    int error_count   = 0;

    // Insert each record
    for (size_t i = 0; i < stmt->insert.record_array.length; i++) {
        stmt_record_t *record = &stmt->insert.record_array.items[i];

        int result            = ts_insert(ts, record->timestamp, record->value);
        if (result == 0) {
            success_count++;
        } else {
            error_count++;
        }
    }

    result.code = EXEC_SUCCESS_STRING;
    // Set response based on insertion results
    if (error_count == 0) {
        snprintf(result.message, MESSAGE_SIZE,
                 "Successfully inserted %d points", success_count);
    } else {
        snprintf(result.message, MESSAGE_SIZE,
                 "Inserted %d points with %d errors", success_count,
                 error_count);
    }

    return result;
}

static int eval_op(const stmt_timeunit_t *op1, stmt_timeunit_t *op2,
                   binary_op_t binop)
{
    time_t t0 = 0;

    switch (op1->type) {
    case TU_VALUE:
        t0 = op1->value;
        break;
    case TU_DATE:
        t0 = datetime_seconds(op1->date);
        if (t0 < 0)
            return -1;
        break;
    case TU_FUNC:
        t0 = current_micros();
        break;
    case TU_SPAN:
        t0 = timespan_seconds(op1->timespan.value, op1->timespan.unit);
        break;
    case TU_OPS:
        t0 = eval_op(op1->binop.tu1, op1->binop.tu2, op1->binop.binary_op);
        break;
    }

    time_t t1 = 0;

    switch (op1->type) {
    case TU_VALUE:
        t1 = op2->value;
        break;
    case TU_DATE:
        t1 = datetime_seconds(op2->date);
        if (t1 < 0)
            return -1;
        break;
    case TU_FUNC:
        t1 = current_micros();
        break;
    case TU_SPAN:
        t1 = timespan_seconds(op2->timespan.value, op2->timespan.unit);
        break;
    case TU_OPS:
        t1 = eval_op(op2->binop.tu1, op2->binop.tu2, op2->binop.binary_op);
        break;
    }

    switch (binop) {
    case BIN_OP_ADD:
        return t0 + t1;
        break;
    case BIN_OP_SUB:
        return t0 - t1;
        break;
    case BIN_OP_MUL:
        return t0 * t1;
        break;
    }

    return -1;
}

static int extract_timestamps(const stmt_selector_t *selector, uint64_t *t0,
                              uint64_t *t1)
{
    if (selector->type == S_SINGLE) {
        time_t unixtime = 0LL;

        switch (selector->timeunit.type) {
        case TU_VALUE:
            *t0 = selector->timeunit.value;
            break;
        case TU_DATE:
            unixtime = datetime_seconds(selector->timeunit.date) * 1000000;
            if (unixtime < 0)
                return -1;
            *t0 = unixtime;
            break;
        case TU_FUNC:
            *t0 = current_micros();
            break;
        case TU_SPAN:
            *t0 = timespan_seconds(selector->timeunit.timespan.value,
                                   selector->timeunit.timespan.unit);
            break;
        case TU_OPS:
            *t0 = eval_op(selector->timeunit.binop.tu1,
                          selector->timeunit.binop.tu2,
                          selector->timeunit.binop.binary_op);
            break;
        }
    }

    return 0;
}

static execute_stmt_result_t execute_select_range(const stmt_t *stmt,
                                                  timeseries_t *ts)
{
    execute_stmt_result_t result = {0};
    record_array_t records       = {0};
    uint64_t t0                  = 0ULL;
    uint64_t t1                  = 0ULL;

    if (extract_timestamps(&stmt->select.selector, &t0, &t1) < 0) {
        result.code = EXEC_ERROR_INVALID_TIMESTAMP;
        snprintf(result.message, MESSAGE_SIZE,
                 "Selector with invalid timestamp");
        return result;
    }

    // Range query
    int err = ts_range(ts, stmt->select.selector.interval.start.value,
                       stmt->select.selector.interval.end.value, &records);

    if (err < 0) {
        result.code = EXEC_ERROR_INVALID_TIMESTAMP;
        snprintf(result.message, MESSAGE_SIZE,
                 "Error: failed to query range [%" PRIu64 ", %" PRIu64 "]",
                 stmt->select.selector.interval.start.value,
                 stmt->select.selector.interval.end.value);
        return result;
    }

    if (records.length > 0) {
        // Prepare array response from records
        result.result_set.records =
            malloc(records.length * sizeof(*result.result_set.records));
        if (!result.result_set.records) {
            result.code = EXEC_ERROR_MEMORY;
            snprintf(result.message, MESSAGE_SIZE, "Out of memory");
            return result;
        }

        for (size_t i = 0; i < records.length; i++) {
            result.result_set.records[i].timestamp = records.items[i].timestamp;
            result.result_set.records[i].value     = records.items[i].value;
        }

        // Free the record array items (data has been copied)
        free(records.items);

        result.code = EXEC_SUCCESS_ARRAY;
        return result;
    }

    result.code = EXEC_ERROR_EMPTY_RESULTSET;
    snprintf(result.message, MESSAGE_SIZE,
             "No data found in range [%" PRIu64 ", %" PRIu64 "]",
             stmt->select.selector.interval.start.value,
             stmt->select.selector.interval.end.value);
    return result;
}

static execute_stmt_result_t execute_select(const stmt_t *stmt)
{
    execute_stmt_result_t result = {0};

    timeseries_db_t *tsdb        = dbcontext_getactive();
    if (!tsdb) {
        result.code = EXEC_ERROR_DB_NOT_FOUND;
        snprintf(result.message, MESSAGE_SIZE,
                 "No database in the system, create one first");
        return result;
    }

    timeseries_t *ts = ts_get(tsdb, stmt->select.ts_name);
    if (!ts) {
        snprintf(result.message, MESSAGE_SIZE, "Timeseries '%s' not found",
                 stmt->insert.ts_name);
        return result;
    }

    // Query data based on select mask
    // if (stmt->select.flags & QF_BASE) {
    //     // Single point query
    //     record_t record;
    //     if (ts_find(ts, stmt->select.timeunit.tsinterval.start, &record) ==
    //     0) {
    //         // Prepare array response with single record
    //         rs->type                  = RT_ARRAY;
    //         rs->array_response.length = 1;
    //         rs->array_response.records =
    //             malloc(sizeof(*rs->array_response.records));
    //         if (!rs->array_response.records) {
    //             set_string_response(rs, 1, "Error: Memory allocation
    //             failed");
    //
    //             return;
    //         }
    //         rs->array_response.records[0].timestamp = record.timestamp;
    //         rs->array_response.records[0].value     = record.value;
    //
    //         return;
    //     }
    //     set_string_response(rs, 1,
    //                         "Error: Point not found at timestamp %" PRIu64,
    //                         stmt->select.timeunit.tsinterval.start);
    //     return;
    // }

    if (stmt->select.flags & QF_RNGE) {
        return execute_select_range(stmt, ts);
    }

    // Unsupported query type
    // TODO support WHERE
    snprintf(result.message, MESSAGE_SIZE, "Error: Unsupported query type");
    result.code = EXEC_ERROR_UNSUPPORTED;
    return result;
}

static execute_stmt_result_t execute_delete(const stmt_t *stmt)
{
    execute_stmt_result_t result = {0};
    return result;
}

static execute_stmt_result_t execute_meta(const stmt_t *stmt)
{
    execute_stmt_result_t result = {0};
    return result;
}

/**
 * Main execution function, handle each query
 */
execute_stmt_result_t stmt_execute(const stmt_t *stmt)
{
    execute_stmt_result_t result = {0};

    if (!stmt) {
        result.code = EXEC_ERROR_NULLPTR;
        return result;
    }

    switch (stmt->type) {
    case STMT_USE:
        result = execute_use(stmt);
        break;
    case STMT_CREATEDB:
        result = execute_createdb(stmt);
        break;
    case STMT_CREATE:
        result = execute_create(stmt);
        break;
    case STMT_INSERT:
        result = execute_insert(stmt);
        break;
    case STMT_SELECT:
        result = execute_select(stmt);
        break;
    case STMT_DELETE:
        result = execute_delete(stmt);
        break;
    case STMT_META:
        result = execute_meta(stmt);
        break;
    default:
        // Unknown statement type (should not happen due to earlier check)
        result.code = EXEC_ERROR_UNKNOWN_STATEMENT;
        break;
    }
    return result;
}
