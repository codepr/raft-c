#include "statement_execute.h"
#include "dbcontext.h"

static execute_stmt_result_t execute_use(const stmt_t *stmt)
{
    execute_stmt_result_t result = {0};

    if (dbcontext_setactive(stmt->use.db_name) < 0) {
        result.code = EXEC_ERROR_DB_NOT_FOUND;
        snprintf(result.error_message, ERROR_MESSAGE_SIZE,
                 "Database '%s' not found", stmt->use.db_name);
    } else {
        result.code = EXEC_SUCCESS;
    }

    return result;
}

static execute_stmt_result_t execute_createdb(const stmt_t *stmt)
{
    execute_stmt_result_t result = {0};
    timeseries_db_t *tsdb        = dbcontext_get(stmt->create.db_name);
    if (tsdb) {
        result.code = EXEC_ERROR_DB_NOT_FOUND;
        snprintf(result.error_message, ERROR_MESSAGE_SIZE,
                 "Database '%s' not found", stmt->use.db_name);
        return result;
    }

    tsdb = dbcontext_add(stmt->create.db_name);
    if (!tsdb) {
        result.code = EXEC_ERROR_DB_NOT_CREATED;
        snprintf(result.error_message, ERROR_MESSAGE_SIZE,
                 "Error creating '%s' database", stmt->create.db_name);
    }
    return result;
}

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
            snprintf(result.error_message, ERROR_MESSAGE_SIZE,
                     "Database '%s' not found", stmt->create.db_name);
            return result;
        }
    }

    tsdb = dbcontext_getactive();
    if (!tsdb) {
        result.code = EXEC_ERROR_DB_NOT_FOUND;
        snprintf(result.error_message, ERROR_MESSAGE_SIZE,
                 "No active database found");
        return result;
    }

    ts_opts_t opts = {.retention = stmt->create.retention.value};

    // Create timeseries
    // TODO handle duplication policy
    ts             = ts_create(tsdb, stmt->create.ts_name, opts);
    if (!ts) {
        result.code = EXEC_ERROR_TS_NOT_CREATED;
        snprintf(result.error_message, ERROR_MESSAGE_SIZE,
                 "Failed to create timeseries '%s'", stmt->create.ts_name);
    }

    return result;
}

static execute_stmt_result_t execute_insert(const stmt_t *stmt)
{
    execute_stmt_result_t result = {0};

    // Active database not supported yet
    timeseries_db_t *tsdb        = dbcontext_getactive();
    if (!tsdb) {
        result.code = EXEC_ERROR_DB_NOT_FOUND;
        snprintf(result.error_message, ERROR_MESSAGE_SIZE,
                 "No database found, create one first");
        return result;
    }

    timeseries_t *ts = ts_get(tsdb, stmt->insert.ts_name);
    if (!ts) {
        result.code = EXEC_ERROR_TS_NOT_FOUND;
        snprintf(result.error_message, ERROR_MESSAGE_SIZE,
                 "Timeseries '%s' not found", stmt->insert.ts_name);
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

    // Set response based on insertion results
    if (error_count == 0) {
        snprintf(result.error_message, ERROR_MESSAGE_SIZE,
                 "Successfully inserted %d points", success_count);
    } else {
        snprintf(result.error_message, ERROR_MESSAGE_SIZE,
                 "Inserted %d points with %d errors", success_count,
                 error_count);
    }

    return result;
}

static int extract_timestamps(const stmt_selector_t *selector, uint64_t *t0,
                              uint64_t *t1)
{
    if (selector->type == S_SINGLE) {
        switch (selector->timeunit.type) {
        case TU_VALUE:
            break;
        case TU_DATE:
            break;
        case TU_FUNC:
            break;
        case TU_SPAN:
            break;
        case TU_OPS:
            break;
        }
    }

    return 0;
}

static execute_stmt_result_t execute_select(const stmt_t *stmt)
{
    execute_stmt_result_t result = {0};

    timeseries_db_t *tsdb        = dbcontext_getactive();
    if (!tsdb) {
        result.code = EXEC_ERROR_DB_NOT_FOUND;
        snprintf(result.error_message, ERROR_MESSAGE_SIZE,
                 "No database in the system, create one first");
        return result;
    }

    timeseries_t *ts = ts_get(tsdb, stmt->select.ts_name);
    if (!ts) {
        snprintf(result.error_message, ERROR_MESSAGE_SIZE,
                 "Timeseries '%s' not found", stmt->insert.ts_name);
        return result;
    }

    record_array_t records = {0};

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

    uint64_t t0            = 0ULL;
    uint64_t t1            = 0ULL;

    if (stmt->select.flags & QF_RNGE) {
        if (extract_timestamps(&stmt->select.selector, &t0, &t1) < 0)
            // TODO error here
            // Range query
            int result =
                ts_range(ts, stmt->select.selector.interval.start.value,
                         stmt->select.selector.interval.end.value, &records);

        if (result == 0 && records.length > 0) {
            // Prepare array response from records
            rs->type                  = RT_ARRAY;
            rs->array_response.length = records.length;
            rs->array_response.records =
                malloc(records.length * sizeof(*rs->array_response.records));

            if (!rs->array_response.records) {
                set_string_response(rs, 1, "Error: Memory allocation failed");
                return;
            }

            for (size_t i = 0; i < records.length; i++) {
                rs->array_response.records[i].timestamp =
                    records.items[i].timestamp;
                rs->array_response.records[i].value = records.items[i].value;
            }

            // Free the record array items (data has been copied)
            free(records.items);

            return;
        }
        rs->type = RT_STRING;
        if (result != 0) {
            set_string_response(rs, 1,
                                "Error: Failed to query range [%" PRIu64
                                ", %" PRIu64 "]",
                                stmt->select.selector.interval.start.value,
                                stmt->select.selector.interval.end.value);
        } else {
            set_string_response(
                rs, 0, "No data found in range [%" PRIu64 ", %" PRIu64 "]",
                stmt->select.selector.interval.start.value,
                stmt->select.selector.interval.end.value);
        }
        return;
    }

    // Unsupported query type
    // TODO
    set_string_response(rs, 1, "Error: Unsupported query type");
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
