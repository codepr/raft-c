#ifndef PARSER_H
#define PARSER_H

#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

typedef struct token token_t;

// Define aggregate function types
typedef enum { AFN_AVG, AFN_MIN, AFN_MAX } aggregate_fn_t;

// Define operator types
typedef enum { OP_EQ, OP_NE, OP_GE, OP_GT, OP_LE, OP_LT } operator_t;

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
    char db_name[IDENTIFIER_LENGTH];
    char ts_name[IDENTIFIER_LENGTH];
    uint8_t mask;
} statement_create_t;

// Define a pair (timestamp, value) for INSERT statements
typedef struct {
    int64_t timestamp;
    double_t value;
} create_record_t;

// Define structure for INSERT statement
typedef struct {
    size_t record_len;
    char db_name[IDENTIFIER_LENGTH];
    char ts_name[IDENTIFIER_LENGTH];
    create_record_t records[RECORDS_LENGTH];
} statement_insert_t;

// Define structure for WHERE clause in SELECT statement
typedef struct {
    char key[IDENTIFIER_LENGTH];
    operator_t operator;
    double_t value;
} statement_where_t;

// Define structure for SELECT statement
typedef struct {
    char db_name[IDENTIFIER_LENGTH];
    char ts_name[IDENTIFIER_LENGTH];
    int64_t start_time;
    int64_t end_time;
    aggregate_fn_t af;
    statement_where_t where;
    uint64_t interval;
    select_mask_t mask;
} statement_select_t;

// Define statement types
typedef enum {
    STATEMENT_EMPTY,
    STATEMENT_CREATE,
    STATEMENT_INSERT,
    STATEMENT_SELECT,
    STATEMENT_UNKNOWN
} statement_type_t;

// Define a generic statement
typedef struct {
    statement_type_t type;
    union {
        statement_create_t create;
        statement_insert_t insert;
        statement_select_t select;
    };
} statement_t;

// Parse a statement
statement_t parse(const char *input);

// Debug helpers

void print_statement(const statement_t *statement);

#endif // PARSER_H
