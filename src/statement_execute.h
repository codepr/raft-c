#ifndef STATEMENT_EXECUTE_H
#define STATEMENT_EXECUTE_H

#include "statement_parse.h"
#include "timeseries.h"
#include <stdlib.h>

#define MESSAGE_SIZE 256

// Execution result types
typedef enum {
    EXEC_SUCCESS_STRING,
    EXEC_SUCCESS_ARRAY,
    EXEC_ERROR_UNSUPPORTED,
    EXEC_ERROR_EMPTY_RESULTSET,
    EXEC_ERROR_DB_NOT_FOUND,
    EXEC_ERROR_TS_NOT_FOUND,
    EXEC_ERROR_TS_NOT_CREATED,
    EXEC_ERROR_DB_NOT_CREATED,
    EXEC_ERROR_INVALID_TIMESTAMP,
    EXEC_ERROR_INVALID_VALUE,
    EXEC_ERROR_MEMORY,
    EXEC_ERROR_IO,
    EXEC_ERROR_NULLPTR,
    EXEC_ERROR_UNKNOWN_STATEMENT
} execute_result_code_t;

typedef struct {
    execute_result_code_t code;
    char message[MESSAGE_SIZE];

    // For SELECT statements
    record_array_t result_set;

    // Metadata about the execution
    int64_t execution_time_ns; // Execution time in nanoseconds
    int64_t records_affected;  // For INSERT/DELETE operations
} execute_stmt_result_t;

typedef struct tcc tcc_t;

// Main execution function
execute_stmt_result_t stmt_execute(tcc_t *ctx, const stmt_t *stmt);

// Helper functions for statement preparation
int64_t stmt_resolve_timestamp(const stmt_timeunit_t *timeunit);
double stmt_compute_aggregation(function_t fn, const stmt_record_t *records,
                                size_t count);
bool stmt_eval_where_clause(const where_clause_t *where,
                            const stmt_record_t *record);

#endif
