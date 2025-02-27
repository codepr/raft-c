#ifndef STATEMENT_H
#define STATEMENT_H

#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#define IDENTIFIER_LENGTH 64
#define RECORDS_LENGTH    32

/*
 * String view APIs definition
 * A string view is merely a pointer to an existing string, or better, to a
 * slice of an existing string (which may be of entire target string length),
 * thus it's not nul-terminated
 */
typedef struct {
    size_t length;
    const char *p;
} string_view_t;

// Function to create a string view from a given source string and length
string_view_t string_view_from_parts(const char *src, size_t len);

// Function to create a string view from a null-terminated source string
string_view_t string_view_from_cstring(const char *src);

// Function to chop a string view by a delimiter and return the remaining view
string_view_t string_view_chop_by_delim(string_view_t *view, const char delim);

/**
 **  Grammar for the SQL query language
 **
 ** COMMAND     ::= CREATE_CMD | INSERT_CMD | SELECT_CMD | DELETE_CMD
 **
 ** CREATE_CMD  ::= "CREATE" IDENTIFIER
 **               | "CREATE" IDENTIFIER "INTO" IDENTIFIER [RETENTION]
 ** [DUPLICATION]
 **
 ** INSERT_CMD  ::= "INSERT" IDENTIFIER "INTO" IDENTIFIER TIMESTAMP VALUE_LIST
 **
 ** SELECT_CMD  ::= "SELECT" IDENTIFIER "FROM" IDENTIFIER
 **                 "RANGE" TIMESTAMP "TO" TIMESTAMP
 **                 "WHERE" "value" COMPARATOR IDENTIFIER
 **                 "AGGREGATE" AGG_FUNC "BY" IDENTIFIER
 **
 ** DELETE_CMD  ::= "DELETE" IDENTIFIER
 **               | "DELETE" IDENTIFIER "FROM" IDENTIFIER
 **
 ** RETENTION   ::= NUMBER
 ** DUPLICATION ::= NUMBER
 ** COMPARATOR  ::= ">" | "<" | "=" | "<=" | ">=" | "!="
 ** AGG_FUNC    ::= "AVG" | "MIN" | "MAX"
 ** VALUE_LIST  ::= VALUE ("," VALUE)*
 ** VALUE       ::= NUMBER
 ** TIMESTAMP   ::= NUMBER | "*"
 ** IDENTIFIER  ::= [A-Za-z_][A-Za-z0-9_]*
 **
 ** E.g.
 **
 ** INSERT cputime INTO load * 12.2 * 19.2 829232932 11.56
 **
 ** Meta commands
 **
 ** META_CMD    ::= ".databases" | ".timeseries"
 **
 **/

typedef struct token token_t;

// Define aggregate function types
typedef enum { AGG_NONE, AGG_AVG, AGG_MIN, AGG_MAX } aggregate_fn_t;

// Define operator types
typedef enum {
    OP_NONE,
    OP_EQUAL,
    OP_NOT_EQUAL,
    OP_GREATER_EQUAL,
    OP_GREATER,
    OP_LESS_EQUAL,
    OP_LESS
} operator_t;

// Define boolean operators
typedef enum { BOOL_OP_NONE, BOOL_OP_AND, BOOL_OP_OR } boolean_op_t;

// Define structure for CREATE statement
typedef struct {
    int single;
    char db_name[IDENTIFIER_LENGTH];
    char ts_name[IDENTIFIER_LENGTH];
    int retention;
    int duplication;
} stmt_create_t;

// Define structure for DELETE statement
typedef struct {
    int single;
    char db_name[IDENTIFIER_LENGTH];
    char ts_name[IDENTIFIER_LENGTH];
} stmt_delete_t;

// Define a pair (timestamp, value) for INSERT statements
typedef struct {
    int64_t timestamp;
    double_t value;
} stmt_record_t;

// Define structure for INSERT statement
typedef struct {
    char db_name[IDENTIFIER_LENGTH];
    char ts_name[IDENTIFIER_LENGTH];
    struct {
        size_t length;
        size_t capacity;
        stmt_record_t *items;
    } record_array;
} stmt_insert_t;

// Define structure for WHERE clause in SELECT statement
typedef struct where_clause {
    char key[IDENTIFIER_LENGTH];
    operator_t operator;
    double_t value;

    struct where_clause *left;
    struct where_clause *right;
    boolean_op_t boolean_op;

} where_clause_t;

// Define structure for SELECT statement
typedef struct {
    char db_name[IDENTIFIER_LENGTH];
    char ts_name[IDENTIFIER_LENGTH];
    int64_t start_time;
    int64_t end_time;

    // WHERE clause
    where_clause_t *where;

    // AGGREGATE information
    aggregate_fn_t agg_function;
    char group_by[IDENTIFIER_LENGTH]; // The "BY" identifier in the grammar
} stmt_select_t;

// Define statement types
typedef enum {
    STMT_EMPTY,
    STMT_META,
    STMT_CREATE,
    STMT_DELETE,
    STMT_INSERT,
    STMT_SELECT,
    STMT_UNKNOWN
} stmt_type_t;

typedef enum { META_DATABASES, META_TIMESERIES, META_UNKNOWN } meta_command_t;

// Define a generic statement
typedef struct {
    stmt_type_t type;
    union {
        stmt_create_t create;
        stmt_delete_t delete;
        stmt_insert_t insert;
        stmt_select_t select;
        meta_command_t meta;
    };
} stmt_t;

stmt_t *stmt_parse(const char *input);
void stmt_free(stmt_t *stmt);
void stmt_print(const stmt_t *stmt);

// Create statement builders
stmt_t *stmt_make_empty();
stmt_t *stmt_make_create(bool single, const char *db_name, const char *ts_name,
                         int retention, int duplication);
stmt_t *stmt_make_delete(bool single, const char *db_name, const char *ts_name);
stmt_t *stmt_make_insert(const char *db_name, const char *ts_name);
bool stmt_insert_add_record(stmt_t *stmt, int64_t timestamp, double value);
stmt_t *stmt_make_select(const char *db_name, const char *ts_name,
                         int64_t start_time, int64_t end_time,
                         aggregate_fn_t agg_function, uint64_t interval);
where_clause_t *stmt_make_where_condition(const char *key, operator_t op,
                                          double value);
bool stmt_add_where_condition(where_clause_t *base, boolean_op_t op,
                              where_clause_t *condition);
bool stmt_set_where_clause(stmt_t *stmt, where_clause_t *where);

// Helper functions for parsing specific parts
operator_t stmt_parse_operator(const char *op_str);
aggregate_fn_t stmt_parse_aggregate_function(const char *agg_str);

#endif
