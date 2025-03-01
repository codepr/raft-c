#ifndef STATEMENT_H
#define STATEMENT_H

#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#define IDENTIFIER_LENGTH 64
#define RECORDS_LENGTH    32
#define TS_MAXSIZE        24

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
string_view_t sv_from_parts(const char *src, size_t len);

// Function to create a string view from a null-terminated source string
string_view_t sv_from_cstring(const char *src);

// Function to chop a string view by a delimiter and return the remaining view
string_view_t sv_chop_by_delim(string_view_t *view, char delim);

/**
 **  Grammar for the SQL query language. In its simplest implementation,
 **  no labels no complex filtering:
 **
 ** - General handling
 **
 ** - Create new database
 **
 **     CREATEDB metrics
 **
 ** - Set a database as active
 **
 **     USE metrics
 **
 ** - Create a new timeseries in the active database
 **
 **     CREATE cpu_usage
 **
 ** - Insertion queries
 **
 **     INSERT INTO timeseries_name VALUES (timestamp, value)
 **
 ** - Single point inserts
 **
 **     INSERT INTO cpu_usage VALUES (1643673600, 78.5)
 **     INSERT INTO cpu_usage VALUES ('2023-01-01 12:30:00', 78.5)
 **
 ** - Multiple points inserts
 **
 **     INSERT INTO cpu_usage VALUES
 **         (1643673600, 78.5),
 **         (1643673660, 80.2),
 **         (1643673720, 75.1)
 **
 ** - Current time insert
 **
 **     INSERT INTO cpu_usage VALUES (now(), 78.5)
 **
 ** - Auto-timestamp (implicit)
 **
 **     INSERT INTO cpu_usage VALUE 78.5
 **
 ** - Selection queries
 **
 **     SELECT [value | function(value)]
 **     FROM timeseries_name
 **     [BETWEEN start_time AND end_time]
 **     [SAMPLE BY interval]
 **     [LIMIT n]
 **
 ** - Simple selection
 **
 **     SELECT value FROM cpu_usage
 **
 ** - Time range queries
 **
 **     SELECT value FROM cpu_usage BETWEEN 1612137600 AND 1612224000
 **     SELECT value FROM cpu_usage BETWEEN '2023-01-01 00:00:00' AND
 **     '2023-01-02 00:00:00'
 **     SELECT value FROM cpu_usage BETWEEN now() - 24h AND now()
 **
 ** - Aggregations
 **
 **     SELECT avg(value) FROM cpu_usage BETWEEN '2023-01-01' AND '2023-01-31'
 **     SELECT min(value), max(value), avg(value) FROM cpu_usage
 **
 ** - Limiting results
 **
 **     SELECT value FROM cpu_usage LIMIT 100
 **     SELECT latest(value) FROM cpu_usage
 **     SELECT earliest(value, 10) FROM cpu_usage
 **
 ** - Downsampling
 **
 **     SELECT value FROM cpu_usage SAMPLE BY 1h
 **     SELECT avg(value) FROM cpu_usage BETWEEN '2023-01-01' AND '2023-01-31'
 **     SAMPLE BY 1d
 **
 ** COMMAND     ::= CREATE_CMD | INSERT_CMD | SELECT_CMD | DELETE_CMD
 **
 ** CREATE_CMD  ::= "CREATE" IDENTIFIER [RETENTION] [DUPLICATION]
 **
 ** INSERT_CMD  ::= "INSERT" "INTO" IDENTIFIER VALUE_LIST
 **
 ** SELECT_CMD  ::= "SELECT" IDENTIFIER | AGG_FUNC(IDENTIFIER) "FROM" IDENTIFIER
 **                 "BETWEEN" TIMESTAMP "AND" TIMESTAMP
 **                 "WHERE" "value" COMPARATOR IDENTIFIER
 **                 "SAMPLE" "BY" IDENTIFIER
 **
 ** DELETE_CMD  ::= "DELETE" IDENTIFIER
 **               | "DELETE" IDENTIFIER "FROM" IDENTIFIER
 **
 ** RETENTION   ::= NUMBER
 ** DUPLICATION ::= NUMBER
 ** COMPARATOR  ::= ">" | "<" | "=" | "<=" | ">=" | "!="
 ** AGG_FUNC    ::= "avg" | "min" | "max"
 ** VALUE_LIST  ::= (TIMESTAMP, VALUE)+
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
typedef enum { FN_NONE, FN_AVG, FN_MIN, FN_MAX, FN_NOW, FN_LATEST } function_t;

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

typedef enum {
    TU_SINGLE,
    TU_INTERVAL,
    TU_TSINTERVAL,
    TU_DATEINTERVAL
} stmt_tu_type_t;

typedef struct stmt_timeunit {
    stmt_tu_type_t type;
    union {
        int64_t value;
        struct {
            int64_t value;
            char unit[TS_MAXSIZE];
        } interval;
        struct {
            int64_t start;
            int64_t end;
        } tsinterval;
        struct {
            char start[TS_MAXSIZE];
            char end[TS_MAXSIZE];
        } dateinterval;
    };
} stmt_timeunit_t;

// Define structure for CREATE statement
typedef struct {
    char db_name[IDENTIFIER_LENGTH];
    char ts_name[IDENTIFIER_LENGTH];
    bool has_retention;
    stmt_timeunit_t retention;
    bool has_duplication;
    char duplication[TS_MAXSIZE];
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

/*
 * Select mask, to define the kind of type of query
 * - Single point lookup
 * - Range of points
 * - With a WHERE clause
 * - With an aggregation function
 * - With an interval to aggregate on
 */
typedef enum query_flags {
    QF_BASE = 0,
    QF_RNGE = 1 << 0,
    QF_FUNC = 1 << 1,
    QF_COND = 1 << 2,
    QF_SMPL = 1 << 3,
    QF_LIMT = 1 << 4,
} query_flags_t;

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
    char ts_name[IDENTIFIER_LENGTH];
    stmt_timeunit_t timeunit;
    // WHERE clause
    where_clause_t *where;

    // AGGREGATE information
    function_t function;
    stmt_timeunit_t sampling;

    // Limit
    int64_t limit;

    // Info about the content of the query
    query_flags_t flags;
} stmt_select_t;

// Define statement types
typedef enum {
    STMT_EMPTY,
    STMT_USE,
    STMT_META,
    STMT_CREATEDB,
    STMT_CREATE,
    STMT_DELETE,
    STMT_INSERT,
    STMT_SELECT,
    STMT_UNKNOWN
} stmt_type_t;

typedef enum { META_DATABASES, META_TIMESERIES, META_UNKNOWN } meta_command_t;

typedef stmt_create_t stmt_use_t;

// Define a generic statement
typedef struct {
    stmt_type_t type;
    union {
        stmt_use_t use;
        stmt_create_t create;
        stmt_delete_t delete;
        stmt_insert_t insert;
        stmt_select_t select;
        meta_command_t meta;
    };
} stmt_t;

void stmt_init(void);
stmt_t *stmt_parse(const char *input);
void stmt_free(stmt_t *stmt);
void stmt_print(const stmt_t *stmt);

#endif
