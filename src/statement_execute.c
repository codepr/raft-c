#include "statement_execute.h"
#include "buffer.h"
#include "dbcontext.h"
#include "encoding.h"
#include "logger.h"
#include "tcc.h"
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

static int64_t eval_op(const stmt_timeunit_t *op1, stmt_timeunit_t *op2,
                       binary_op_t binop)
{
    int64_t t0 = 0;

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
        t0 = current_nanos();
        break;
    case TU_SPAN:
        t0 = timespan_seconds(op1->timespan.value, op1->timespan.unit);
        break;
    case TU_OPS:
        t0 = eval_op(op1->binop.tu1, op1->binop.tu2, op1->binop.binary_op);
        break;
    }

    int64_t t1 = 0;

    switch (op2->type) {
    case TU_VALUE:
        t1 = op2->value;
        break;
    case TU_DATE:
        t1 = datetime_seconds(op2->date);
        if (t1 < 0)
            return -1;
        break;
    case TU_FUNC:
        t1 = current_nanos();
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
    case BIN_OP_SUB:
        return t0 - t1;
    case BIN_OP_MUL:
        return t0 * t1;
    }

    return -1;
}

static int extract_timestamp(const stmt_timeunit_t *tu, int64_t *timestamp)
{
    int64_t unixtime = 0LL;

    switch (tu->type) {
    case TU_VALUE:
        *timestamp = tu->value;
        break;
    case TU_DATE:
        unixtime = datetime_seconds(tu->date);
        if (unixtime < 0)
            return -1;
        *timestamp = unixtime;
        break;
    case TU_FUNC:
        *timestamp = current_nanos();
        break;
    case TU_SPAN:
        *timestamp = timespan_seconds(tu->timespan.value, tu->timespan.unit);
        break;
    case TU_OPS:
        *timestamp = eval_op(tu->binop.tu1, tu->binop.tu2, tu->binop.binary_op);
        break;
    }

    return 0;
}

static int extract_timestamps(const stmt_selector_t *selector, int64_t *t0,
                              int64_t *t1)
{
    if (selector->type == S_SINGLE) {
        return extract_timestamp(&selector->timeunit, t0);
    } else if (selector->type == S_INTERVAL) {
        if (extract_timestamp(&selector->interval.start, t0) < 0)
            return -1;
        if (extract_timestamp(&selector->interval.end, t1) < 0)
            return -1;
    }
    return 0;
}

static execute_stmt_result_t execute_select_range(const stmt_t *stmt,
                                                  timeseries_t *ts)
{
    execute_stmt_result_t result = {0};
    int64_t t0                   = 0ULL;
    int64_t t1                   = 0ULL;

    if (extract_timestamps(&stmt->select.selector, &t0, &t1) < 0) {
        result.code = EXEC_ERROR_INVALID_TIMESTAMP;
        snprintf(result.message, MESSAGE_SIZE,
                 "Selector with invalid timestamp");
        return result;
    }

    // Range query
    int err = ts_range(ts, t0, t1, &result.result_set);

    if (err < 0) {
        result.code = EXEC_ERROR_INVALID_TIMESTAMP;
        snprintf(result.message, MESSAGE_SIZE,
                 "Error: failed to query range [%" PRIu64 ", %" PRIu64 "]", t0,
                 t1);
        return result;
    }

    result.code = result.result_set.length == 0 ? EXEC_ERROR_EMPTY_RESULTSET
                                                : EXEC_SUCCESS_ARRAY;

    snprintf(result.message, MESSAGE_SIZE,
             "No data found in range [%" PRIu64 ", %" PRIu64 "]", t0, t1);

    return result;
}

static int stream_callback(const record_array_t *ra, void *user_data)
{
    tcc_t *ctx = user_data;
    if (!ctx || !ra)
        return TS_E_NULL_POINTER;

    // If an error occurred previously, stop processing
    if (ctx->error_code != 0)
        return ctx->error_code;

    buffer_reset(ctx->buffer);

    // Protocol v1: 8 bytes timestamp + 8 bytes value
    size_t bytes_needed = 16;

    // Check if we need to flush the buffer
    if (buffer_remaining_read(ctx->buffer) < bytes_needed) {
        if (tcc_flush_buffer(ctx) != 0) {
            // TODO add proper errors
            ctx->error_code = -1;
            return -1;
        }
    }

    // Send batch
    response_t chunk               = {0};

    chunk.type                     = RT_STREAM;
    chunk.stream_response.batch    = *ra;
    chunk.stream_response.is_final = ra->length < ctx->batch_size;

    // TODO bit of a dirty way to send the chunk
    ssize_t bytes = buffer_encode_response(ctx->buffer, &chunk);
    if (bytes < 0) {
        // TODO add proper errors
        ctx->error_code = -1;
        return -1;
    }

    if (tcc_flush_buffer(ctx) != 0) {
        // TODO add proper errors
        ctx->error_code = -1;
        return -1;
    }

    // Count records sent
    ctx->records_sent += ra->length;

    return 0;
}

static execute_stmt_result_t execute_select(tcc_t *ctx, const stmt_t *stmt)
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
                 stmt->select.ts_name);
        return result;
    }

    // Query data based on select mask
    if (stmt->select.flags & QF_BASE) {
        if (ts_stream(ts, stream_callback, ctx) < 0) {
            result.code = EXEC_ERROR_EMPTY_RESULTSET;
            snprintf(result.message, MESSAGE_SIZE, "Unable to stream results");
            return result;
        }

        result.code = EXEC_SUCCESS_STRING;
        snprintf(result.message, MESSAGE_SIZE, "stream end - %zu records sent",
                 ctx->records_sent);

        return result;
    }

    if (stmt->select.flags & QF_RNGE) {
        return execute_select_range(stmt, ts);
    }

    // Unsupported query type
    // TODO support WHERE
    snprintf(result.message, MESSAGE_SIZE, "Error: Unsupported query type");
    result.code = EXEC_ERROR_UNSUPPORTED;
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
    int64_t timestamp = 0;

    // Insert each record
    for (size_t i = 0; i < stmt->insert.record_array.length; i++) {
        stmt_record_t *record = &stmt->insert.record_array.items[i];

        if (extract_timestamp(&record->timeunit, &timestamp) < 0) {
            error_count++;
            continue;
        }

        log_info("Insert (%lli, %lf)", timestamp, record->value);
        int result = ts_insert(ts, timestamp, record->value);
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
execute_stmt_result_t stmt_execute(tcc_t *ctx, const stmt_t *stmt)
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
        result = execute_select(ctx, stmt);
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
