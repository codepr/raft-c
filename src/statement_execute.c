#include "statement_execute.h"

static execute_stmt_result_t execute_use(const stmt_t *stmt)
{
    execute_stmt_result_t result = {0};
    return result;
}

static execute_stmt_result_t execute_createdb(const stmt_t *stmt)
{
    execute_stmt_result_t result = {0};
    return result;
}

static execute_stmt_result_t execute_create(const stmt_t *stmt)
{
    execute_stmt_result_t result = {0};
    return result;
}
static execute_stmt_result_t execute_insert(const stmt_t *stmt)
{
    execute_stmt_result_t result = {0};
    return result;
}

static execute_stmt_result_t execute_select(const stmt_t *stmt)
{
    execute_stmt_result_t result = {0};
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
