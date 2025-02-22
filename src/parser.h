#ifndef PARSER_H
#define PARSER_H

#include <math.h>
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
 **/

typedef struct token token_t;

// Define aggregate function types
typedef enum { AFN_NONE, AFN_AVG, AFN_MIN, AFN_MAX } aggregate_fn_t;

// Define operator types
typedef enum { OP_NONE, OP_EQ, OP_NE, OP_GE, OP_GT, OP_LE, OP_LT } operator_t;

// Define boolean operators
typedef enum { BOOL_NONE, BOOL_AND } boolean_op_t;

/*
 * Select mask, to define the kind of type of query
 * - Single point lookup
 * - Range of points
 * - With a WHERE clause
 * - With an aggregation function
 * - With an interval to aggregate on
 */
typedef enum select_mask {
    SM_SINGLE    = 0x01,
    SM_RANGE     = 0x02,
    SM_WHERE     = 0x04,
    SM_AGGREGATE = 0x08,
    SM_BY        = 0x10
} select_mask_t;

// Define structure for CREATE statement
typedef struct {
    int single;
    char db_name[IDENTIFIER_LENGTH];
    char ts_name[IDENTIFIER_LENGTH];
    int retention;
    int duplication;
} ast_node_create_t;

// Define structure for DELETE statement
typedef struct {
    int single;
    char db_name[IDENTIFIER_LENGTH];
    char ts_name[IDENTIFIER_LENGTH];
} ast_node_delete_t;

// Define a pair (timestamp, value) for INSERT statements
typedef struct {
    int64_t timestamp;
    double_t value;
} ast_node_record_t;

// Define structure for INSERT statement
typedef struct {
    char db_name[IDENTIFIER_LENGTH];
    char ts_name[IDENTIFIER_LENGTH];
    struct {
        size_t length;
        size_t capacity;
        ast_node_record_t *items;
    } record_array;
} ast_node_insert_t;

// Define structure for WHERE clause in SELECT statement
typedef struct ast_node_where {
    char key[IDENTIFIER_LENGTH];
    operator_t operator;
    double_t value;

    struct ast_node_where *left;
    struct ast_node_where *right;
    boolean_op_t boolean_op;

} ast_node_where_t;

// Define structure for SELECT statement
typedef struct {
    char db_name[IDENTIFIER_LENGTH];
    char ts_name[IDENTIFIER_LENGTH];
    int64_t start_time;
    int64_t end_time;
    aggregate_fn_t af;
    ast_node_where_t *where;
    uint64_t interval;
    select_mask_t mask;
} ast_node_select_t;

// Define statement types
typedef enum {
    STATEMENT_EMPTY,
    STATEMENT_CREATE,
    STATEMENT_DELETE,
    STATEMENT_INSERT,
    STATEMENT_SELECT,
    STATEMENT_UNKNOWN
} ast_node_type_t;

// Define a generic statement
typedef struct {
    ast_node_type_t type;
    union {
        ast_node_create_t create;
        ast_node_delete_t delete;
        ast_node_insert_t insert;
        ast_node_select_t select;
    };
} ast_node_t;

ast_node_t *ast_parse(const char *input);
void ast_free(ast_node_t *node);
void print_ast_node(const ast_node_t *node);

#endif
