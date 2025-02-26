#include "statement.h"
#include "darray.h"
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

string_view_t string_view_from_parts(const char *src, size_t len)
{
    string_view_t view = {.length = len, .p = src};
    return view;
}

string_view_t string_view_from_cstring(const char *src)
{
    return string_view_from_parts(src, strlen(src));
}

string_view_t string_view_chop_by_delim(string_view_t *view, const char delim)
{
    size_t i = 0;
    while (i < view->length && view->p[i] != delim) {
        i += 1;
    }

    string_view_t result = string_view_from_parts(view->p, i);

    if (i < view->length) {
        view->length -= i + 1;
        view->p += i + 1;
    } else {
        view->length -= i;
        view->p += i;
    }

    return result;
}

/*
 * Basic lexer, breaks down the input string (in the form of a string_view_t)
 * splitting it by space or ',' to allow the extraction of tokens.
 */
typedef struct {
    string_view_t view;
    size_t length;
} lexer_t;

// Function to get the next token from the lexer
static string_view_t lexer_next(lexer_t *l)
{
    string_view_t lexiom = string_view_chop_by_delim(&l->view, ' ');
    l->length            = l->view.length;
    return lexiom;
}

// Define token types
typedef enum {
    TOKEN_CREATE,
    TOKEN_INSERT,
    TOKEN_INTO,
    TOKEN_NUMBER,
    TOKEN_IDENTIFIER,
    TOKEN_SELECT,
    TOKEN_DELETE,
    TOKEN_FROM,
    TOKEN_AND,
    TOKEN_RANGE,
    TOKEN_TO,
    TOKEN_WHERE,
    TOKEN_WILDCARD,
    TOKEN_OPERATOR_EQ,
    TOKEN_OPERATOR_NE,
    TOKEN_OPERATOR_LE,
    TOKEN_OPERATOR_LT,
    TOKEN_OPERATOR_GE,
    TOKEN_OPERATOR_GT,
    TOKEN_AGGREGATE_AVG,
    TOKEN_AGGREGATE_MIN,
    TOKEN_AGGREGATE_MAX,
    TOKEN_AGGREGATE,
    TOKEN_BY,
    TOKEN_EOF
} token_type_t;

// Define token structure
typedef struct token {
    token_type_t type;
    char value[IDENTIFIER_LENGTH];
} token_t;

typedef struct token_array {
    size_t length;
    size_t capacity;
    token_t *items;
} token_array_t;

static int tokenize_next(lexer_t *l, token_t *t)
{
    if (l->length == 0) {
        t->type     = TOKEN_EOF;
        t->value[0] = EOF;
        return EOF;
    }

    string_view_t token = lexer_next(l);
    size_t length       = 0;

    // Main commands

    if (strncasecmp(token.p, "CREATE", 6) == 0) {
        length  = 6;
        t->type = TOKEN_CREATE;
    } else if (strncasecmp(token.p, "INSERT", 6) == 0) {
        length  = 6;
        t->type = TOKEN_INSERT;
    } else if (strncasecmp(token.p, "SELECT", 6) == 0) {
        length  = 6;
        t->type = TOKEN_SELECT;
    } else if (strncasecmp(token.p, "DELETE", 6) == 0) {
        length  = 6;
        t->type = TOKEN_DELETE;
    } else if (strncasecmp(token.p, "WHERE", 5) == 0) {
        length  = 5;
        t->type = TOKEN_WHERE;
    } else if (strncasecmp(token.p, "INTO", 4) == 0) {
        length  = 4;
        t->type = TOKEN_INTO;
    } else if (strncasecmp(token.p, "FROM", 4) == 0) {
        length  = 4;
        t->type = TOKEN_FROM;
    } else if (strncasecmp(token.p, "RANGE", 5) == 0) {
        length  = 4;
        t->type = TOKEN_RANGE;
    } else if (strncasecmp(token.p, "AND", 3) == 0) {
        length  = 3;
        t->type = TOKEN_AND;
    } else if (strncasecmp(token.p, "TO", 2) == 0) {
        length  = 2;
        t->type = TOKEN_TO;
    } else if (strncasecmp(token.p, "BY", 2) == 0) {
        length  = 2;
        t->type = TOKEN_BY;
    } else if (strncasecmp(token.p, "AGGREGATE", 9) == 0) {
        length  = 9;
        t->type = TOKEN_AGGREGATE;
    } else if (strncasecmp(token.p, "AVG", 3) == 0) {
        length  = 3;
        t->type = TOKEN_AGGREGATE_AVG;
    } else if (strncasecmp(token.p, "MIN", 3) == 0) {
        length  = 3;
        t->type = TOKEN_AGGREGATE_MIN;
    } else if (strncasecmp(token.p, "MAX", 3) == 0) {
        length  = 3;
        t->type = TOKEN_AGGREGATE_MAX;
    } else if (strncasecmp(token.p, "<=", 2) == 0) {
        length  = 2;
        t->type = TOKEN_OPERATOR_LE;
    } else if (strncasecmp(token.p, ">=", 2) == 0) {
        length  = 2;
        t->type = TOKEN_OPERATOR_GE;
    } else if (strncasecmp(token.p, "!=", 2) == 0) {
        length  = 2;
        t->type = TOKEN_OPERATOR_NE;
    } else if (strncasecmp(token.p, "<", 1) == 0) {
        length  = 1;
        t->type = TOKEN_OPERATOR_LT;
    } else if (strncasecmp(token.p, ">", 1) == 0) {
        length  = 1;
        t->type = TOKEN_OPERATOR_GT;
    } else if (*token.p == '*' && token.length == 1) {
        length  = 1;
        t->type = TOKEN_WILDCARD;
    } else if (sscanf(token.p, "%" PRIu64, &(uint64_t){0}) == 1) {
        length  = token.length;
        t->type = TOKEN_NUMBER;
    } else if (sscanf(token.p, "%lf", &(double_t){0.0f}) == 1) {
        length  = token.length;
        t->type = TOKEN_NUMBER;
    } else {
        length  = token.length;
        t->type = TOKEN_IDENTIFIER;
    }

    strncpy(t->value, token.p, length);

    return 1;
}

static ssize_t tokenize(const char *query, token_array_t *token_array)
{
    string_view_t view = string_view_from_cstring(query);
    lexer_t l          = {.view = view, .length = view.length};
    token_t token      = {0};

    while (tokenize_next(&l, &token) != EOF) {
        da_append(token_array, token);
        memset(&token, 0x00, sizeof(token));
    }

    token.type     = EOF;
    token.value[0] = EOF;
    da_append(token_array, token);

    return token_array->length;
}

// Helper function to safely copy identifiers
static void copy_identifier(char *dest, const char *src)
{
    size_t len = strlen(src);
    if (len >= IDENTIFIER_LENGTH) {
        len = IDENTIFIER_LENGTH - 1;
    }
    strncpy(dest, src, len);
    dest[len] = '\0';
}

typedef struct {
    token_t *tokens;
    size_t pos;
} parser_t;

static token_t *parser_peek(const parser_t *p) { return &p->tokens[p->pos]; }

static int expect(parser_t *p, token_type_t type)
{
    if (p->tokens[p->pos].type != type) {
        fprintf(stderr, "Unexpected token: '%s' after '%s' at %zu\n",
                p->tokens[p->pos].value, p->tokens[p->pos - 1].value, p->pos);
        return -1;
    }

    p->pos++;

    return 0;
}

static char *expect_identifier(parser_t *p)
{
    if (expect(p, TOKEN_IDENTIFIER) < 0)
        return NULL;
    return p->tokens[p->pos - 1].value;
}

static int expect_operator(parser_t *p)
{
    token_t *t    = parser_peek(p);
    operator_t op = OP_EQUAL;

    switch (t->type) {
    case TOKEN_OPERATOR_EQ:
        break;
    case TOKEN_OPERATOR_NE:
        op = OP_NOT_EQUAL;
        break;
    case TOKEN_OPERATOR_LE:
        op = OP_LESS_EQUAL;
        break;

    case TOKEN_OPERATOR_GE:
        op = OP_GREATER_EQUAL;
        break;

    case TOKEN_OPERATOR_LT:
        op = OP_LESS;
        break;

    case TOKEN_OPERATOR_GT:
        op = OP_GREATER;
        break;

    default:
        fprintf(stderr, "Unexpected operator token: %s at %zu\n",
                p->tokens[p->pos].type == EOF ? "EOF" : p->tokens[p->pos].value,
                p->pos);
        return -1;
    }
    p->pos++;
    return op;
}

static int expect_aggregatefn(parser_t *p)
{
    token_t *t        = parser_peek(p);
    aggregate_fn_t fn = AGG_AVG;

    switch (t->type) {
    case TOKEN_AGGREGATE_AVG:
        break;
    case TOKEN_AGGREGATE_MAX:
        fn = AGG_MAX;
        break;
    case TOKEN_AGGREGATE_MIN:
        fn = AGG_MIN;
        break;
    default:
        fprintf(stderr, "Unexpected aggregate fn token: %s at %zu\n",
                p->tokens[p->pos].type == EOF ? "EOF" : p->tokens[p->pos].value,
                p->pos);
        return -1;
    }
    p->pos++;
    return fn;
}

static int expect_integer(parser_t *p, int64_t *num)
{
    if (parser_peek(p)->type == TOKEN_WILDCARD) {

        if (expect(p, TOKEN_WILDCARD) < 0)
            return -1;

        *num = -1;
        return 0;
    }
    if (expect(p, TOKEN_NUMBER) < 0)
        return -1;

    *num = atoll(p->tokens[p->pos - 1].value);

    return 0;
}

static int expect_float(parser_t *p, double_t *val)
{
    double_t value = 0.0f;
    if (expect(p, TOKEN_NUMBER) < 0)
        return -1;

    if (sscanf(p->tokens[p->pos - 1].value, "%lf", &value) != 1) {
        fprintf(stderr, "Expected float value: %s after %s at %lu\n",
                p->tokens[p->pos - 1].value, p->tokens[p->pos - 2].value,
                p->pos);
        return -1;
    }

    *val = value;

    return 0;
}

static int expect_boolean(parser_t *p)
{
    if (expect(p, TOKEN_AND) < 0)
        return -1;
    return BOOL_OP_AND;
}

static void where_clause_free(where_clause_t *node)
{
    if (!node)
        return;

    where_clause_free(node->right);
    free(node);
}

static where_clause_t *parse_where(parser_t *p)
{
    where_clause_t *node = calloc(1, sizeof(*node));
    if (!node)
        return NULL;

    char *key = expect_identifier(p);
    if (!key)
        goto err;

    copy_identifier(node->key, key);

    node->operator= expect_operator(p);
    if (node->operator<0)
        goto err;
    if (expect_float(p, &node->value) < 0)
        goto err;

    if (parser_peek(p)->type == TOKEN_AND) {
        node->boolean_op = expect_boolean(p);
        if (node->boolean_op < 0)
            goto err;

        // Recursively parse right-hand condition
        node->right = parse_where(p);
        node->left  = node; // Promote current condition as left child
    }
    return node;

err:
    where_clause_free(node);
    return NULL;
}

static stmt_t *parse_create(parser_t *p)
{
    stmt_t *node = calloc(1, sizeof(*node));
    if (!node)
        return NULL;

    node->type                     = STMT_CREATE;
    node->create.single            = 1;
    char tsname[IDENTIFIER_LENGTH] = {0};

    if (expect(p, TOKEN_CREATE) < 0)
        goto err;

    char *tsid = expect_identifier(p);
    if (!tsid)
        goto err;

    copy_identifier(tsname, tsid);

    if (parser_peek(p)->type == TOKEN_INTO) {
        node->create.single = 0;
        if (expect(p, TOKEN_INTO) < 0)
            goto err;

        char *dbname = expect_identifier(p);
        if (!dbname)
            goto err;

        copy_identifier(node->create.db_name, dbname);
        copy_identifier(node->create.ts_name, tsname);

        if (parser_peek(p)->type == TOKEN_NUMBER &&
            expect_integer(p, (int64_t *)&node->create.retention) < 0)
            goto err;
        if (parser_peek(p)->type == TOKEN_NUMBER &&
            expect_integer(p, (int64_t *)&node->create.duplication) < 0)
            goto err;
    } else {
        copy_identifier(node->create.db_name, tsname);
    }

    return node;

err:
    free(node);
    return NULL;
}

static stmt_t *parse_delete(parser_t *p)
{
    stmt_t *node = calloc(1, sizeof(*node));
    if (!node)
        return NULL;

    node->type                     = STMT_DELETE;
    node->delete.single            = 1;
    char tsname[IDENTIFIER_LENGTH] = {0};

    if (expect(p, TOKEN_DELETE) < 0)
        goto err;

    char *tsid = expect_identifier(p);
    if (!tsid)
        goto err;

    copy_identifier(tsname, tsid);

    if (parser_peek(p)->type == TOKEN_FROM) {
        node->delete.single = 0;
        if (expect(p, TOKEN_FROM) < 0)
            goto err;

        char *dbname = expect_identifier(p);
        if (!dbname)
            goto err;

        copy_identifier(node->delete.db_name, dbname);
        copy_identifier(node->delete.ts_name, tsname);

    } else {
        copy_identifier(node->delete.db_name, tsname);
    }

    return node;

err:
    free(node);
    return NULL;
}

static stmt_t *parse_insert(parser_t *p)
{
    stmt_t *node = calloc(1, sizeof(*node));
    if (!node)
        return NULL;

    node->type = STMT_INSERT;

    if (expect(p, TOKEN_INSERT) < 0)
        goto err;

    char *tsname = expect_identifier(p);
    if (!tsname)
        goto err;

    copy_identifier(node->insert.ts_name, tsname);

    if (expect(p, TOKEN_INTO) < 0)
        goto err;

    char *dbname = expect_identifier(p);
    if (!dbname)
        goto err;

    copy_identifier(node->insert.db_name, dbname);

    stmt_record_t record = {0};
    while (parser_peek(p)->type == TOKEN_WILDCARD ||
           parser_peek(p)->type == TOKEN_NUMBER) {

        if (expect_integer(p, &record.timestamp) < 0 ||
            expect_float(p, &record.value) < 0)
            goto err;
        da_append(&node->insert.record_array, record);
    }

    return node;

err:
    da_free(&node->insert.record_array);
    free(node);
    return NULL;
}

static stmt_t *parse_select(parser_t *p)
{
    stmt_t *node = calloc(1, sizeof(*node));
    if (!node)
        return NULL;

    node->type              = STMT_SELECT;
    node->select.start_time = -1;
    node->select.end_time   = -1;

    if (expect(p, TOKEN_SELECT) < 0)
        goto err;

    char *tsname = expect_identifier(p);
    if (!tsname)
        goto err;

    copy_identifier(node->select.ts_name, tsname);

    if (expect(p, TOKEN_FROM) < 0)
        goto err;

    char *dbname = expect_identifier(p);
    if (!dbname)
        goto err;

    copy_identifier(node->select.db_name, dbname);

    if (parser_peek(p)->type == TOKEN_RANGE) {
        if (expect(p, TOKEN_RANGE) < 0)
            goto err;
        if (expect_integer(p, &node->select.start_time) < 0)
            goto err;
        if (expect(p, TOKEN_TO) < 0)
            goto err;
        if (expect_integer(p, &node->select.end_time) < 0)
            goto err;
    }

    if (parser_peek(p)->type == TOKEN_WHERE) {
        if (expect(p, TOKEN_WHERE) < 0)
            goto err;
        node->select.where = parse_where(p);
    }

    if (parser_peek(p)->type == TOKEN_AGGREGATE) {
        if (expect(p, TOKEN_AGGREGATE) < 0)
            goto err;
        node->select.agg_function = expect_aggregatefn(p);
        if (node->select.agg_function < 0)
            goto err;

        if (parser_peek(p)->type == TOKEN_BY) {
            if (expect(p, TOKEN_BY) < 0)
                goto err;

            char *groupby = expect_identifier(p);
            if (!groupby)
                goto err;

            copy_identifier(node->select.group_by, groupby);
        }
    }

    return node;

err:
    free(node);
    return NULL;
}

stmt_t *stmt_parse(const char *input)
{
    token_array_t token_array = {0};
    size_t token_count        = tokenize(input, &token_array);
    stmt_t *node              = NULL;

    if (token_count < 1)
        goto err;

    parser_t parser = {.tokens = token_array.items, .pos = 0};

    switch (token_array.items[0].type) {
    case TOKEN_CREATE:
        node = parse_create(&parser);
        break;
    case TOKEN_INSERT:
        node = parse_insert(&parser);
        break;
    case TOKEN_SELECT:
        node = parse_select(&parser);
        break;
    case TOKEN_DELETE:
        node = parse_delete(&parser);
        break;
    default:
        break;
    }

err:
    da_free(&token_array);

    return node;
}

void stmt_free(stmt_t *node)
{
    if (!node)
        return;

    switch (node->type) {
    case STMT_CREATE:
    case STMT_INSERT:
        free(node);
        break;
    case STMT_SELECT:
        if (node->select.where)
            where_clause_free(node->select.where);
        free(node);
        break;
    default:
        break;
    }
}

// Create an empty statement
stmt_t *stmt_make_empty()
{
    stmt_t *stmt = malloc(sizeof(stmt_t));
    if (!stmt)
        return NULL;

    stmt->type = STMT_EMPTY;
    return stmt;
}

// Create a CREATE statement
stmt_t *stmt_make_create(bool single, const char *db_name, const char *ts_name,
                         int retention, int duplication)
{
    stmt_t *stmt = malloc(sizeof(stmt_t));
    if (!stmt)
        return NULL;

    stmt->type          = STMT_CREATE;
    stmt->create.single = single;
    copy_identifier(stmt->create.db_name, db_name);

    if (ts_name) {
        copy_identifier(stmt->create.ts_name, ts_name);
    } else {
        stmt->create.ts_name[0] = '\0';
    }

    stmt->create.retention   = retention;
    stmt->create.duplication = duplication;

    return stmt;
}

// Create a DELETE statement
stmt_t *stmt_make_delete(bool single, const char *db_name, const char *ts_name)
{
    stmt_t *stmt = malloc(sizeof(stmt_t));
    if (!stmt)
        return NULL;

    stmt->type          = STMT_DELETE;
    stmt->delete.single = single;
    copy_identifier(stmt->delete.db_name, db_name);

    if (ts_name) {
        copy_identifier(stmt->delete.ts_name, ts_name);
    } else {
        stmt->delete.ts_name[0] = '\0';
    }

    return stmt;
}

// Create an INSERT statement
stmt_t *stmt_make_insert(const char *db_name, const char *ts_name)
{
    stmt_t *stmt = malloc(sizeof(stmt_t));
    if (!stmt)
        return NULL;

    stmt->type = STMT_INSERT;
    copy_identifier(stmt->insert.db_name, db_name);
    copy_identifier(stmt->insert.ts_name, ts_name);

    return stmt;
}

// Add a record to an INSERT statement
bool stmt_insert_add_record(stmt_t *stmt, int64_t timestamp, double value)
{
    if (!stmt || stmt->type != STMT_INSERT)
        return false;

    stmt_record_t record = {.value = value, .timestamp = timestamp};

    da_append(&stmt->insert.record_array, record);

    return true;
}

// Create a SELECT statement
stmt_t *stmt_make_select(const char *db_name, const char *ts_name,
                         int64_t start_time, int64_t end_time,
                         aggregate_fn_t agg_function, uint64_t interval)
{
    stmt_t *stmt = malloc(sizeof(stmt_t));
    if (!stmt)
        return NULL;

    stmt->type = STMT_SELECT;
    copy_identifier(stmt->select.db_name, db_name);
    copy_identifier(stmt->select.ts_name, ts_name);

    stmt->select.start_time   = start_time;
    stmt->select.end_time     = end_time;
    stmt->select.agg_function = agg_function;
    stmt->select.where        = NULL;

    return stmt;
}

// Create a WHERE condition
where_clause_t *stmt_make_where_condition(const char *key, operator_t op,
                                          double value)
{
    where_clause_t *condition = malloc(sizeof(where_clause_t));
    if (!condition)
        return NULL;

    copy_identifier(condition->key, key);
    condition->operator= op;
    condition->value      = value;
    condition->left       = NULL;
    condition->right      = NULL;
    condition->boolean_op = BOOL_OP_NONE;

    return condition;
}

// Add a WHERE condition to an existing condition tree
bool stmt_add_where_condition(where_clause_t *base, boolean_op_t op,
                              where_clause_t *condition)
{
    if (!base || !condition)
        return false;

    // If the base condition doesn't have a boolean operator yet, add this
    // condition
    if (base->boolean_op == BOOL_OP_NONE) {
        base->boolean_op = op;
        base->left       = malloc(sizeof(where_clause_t));
        if (!base->left)
            return false;

        // Copy current base condition to left child
        memcpy(base->left, base, sizeof(where_clause_t));
        base->left->left       = NULL;
        base->left->right      = NULL;
        base->left->boolean_op = BOOL_OP_NONE;

        // Set the right child to the new condition
        base->right            = condition;

        // Clear the base condition's key and operator
        base->key[0]           = '\0';
        return true;
    }

    // Otherwise, recursively add to the right subtree
    where_clause_t *new_parent = malloc(sizeof(where_clause_t));
    if (!new_parent)
        return false;

    new_parent->boolean_op = op;
    new_parent->left       = base->right;
    new_parent->right      = condition;
    new_parent->key[0]     = '\0';

    base->right            = new_parent;
    return true;
}

// Set the WHERE clause for a SELECT statement
bool stmt_set_where_clause(stmt_t *stmt, where_clause_t *where)
{
    if (!stmt || stmt->type != STMT_SELECT)
        return false;

    // Free existing WHERE clause if present
    if (stmt->select.where) {
        // Would need a recursive free function here
        free(stmt->select.where);
    }

    stmt->select.where = where;
    return true;
}

static void print_where(const where_clause_t *where)
{
    if (!where)
        return;

    printf("    key=%s\n", where->key);

    switch (where->operator) {
    case OP_NONE:
        printf("    operator=NONE\n");
        break;
    case OP_EQUAL:
        printf("    operator=EQ\n");
        break;
    case OP_NOT_EQUAL:
        printf("    operator=NE\n");
        break;
    case OP_GREATER_EQUAL:
        printf("    operator=GE\n");
        break;
    case OP_LESS_EQUAL:
        printf("    operator=LE\n");
        break;
    case OP_GREATER:
        printf("    operator=GT\n");
        break;
    case OP_LESS:
        printf("    operator=LT\n");
        break;
    }

    printf("    value=%.2f\n", where->value);

    if (where->right)
        printf("    boolean=%s\n",
               where->boolean_op == BOOL_OP_AND ? "AND" : "NA");

    print_where(where->right);
}

// Print a statement for debugging
void stmt_print(const stmt_t *stmt)
{
    if (!stmt) {
        printf("NULL statement\n");
        return;
    }

    switch (stmt->type) {
    case STMT_EMPTY:
        printf("Empty statement\n");
        break;

    case STMT_CREATE:
        printf("CREATE statement:\n");
        printf("  Single: %s\n", stmt->create.single ? "true" : "false");
        printf("  DB Name: %s\n", stmt->create.db_name);
        if (!stmt->create.single) {
            printf("  TS Name: %s\n", stmt->create.ts_name);
            printf("  Retention: %d\n", stmt->create.retention);
            printf("  Duplication: %d\n", stmt->create.duplication);
        }
        break;

    case STMT_DELETE:
        printf("DELETE statement:\n");
        printf("  Single: %s\n", stmt->delete.single ? "true" : "false");
        printf("  DB Name: %s\n", stmt->delete.db_name);
        if (!stmt->delete.single) {
            printf("  TS Name: %s\n", stmt->delete.ts_name);
        }
        break;

    case STMT_INSERT:
        printf("INSERT statement:\n");
        printf("  DB Name: %s\n", stmt->insert.db_name);
        printf("  TS Name: %s\n", stmt->insert.ts_name);
        printf("  Records (%zu):\n", stmt->insert.record_array.length);
        for (size_t i = 0; i < stmt->insert.record_array.length; i++) {
            printf("    [%" PRIu64 "] = %f\n",
                   stmt->insert.record_array.items[i].timestamp,
                   stmt->insert.record_array.items[i].value);
        }
        break;

    case STMT_SELECT:
        printf("SELECT statement:\n");
        printf("  DB Name: %s\n", stmt->select.db_name);
        printf("  TS Name: %s\n", stmt->select.ts_name);
        printf("  Time Range: %" PRIi64 " to %" PRIi64 "\n",
               stmt->select.start_time, stmt->select.end_time);
        printf("  Aggregate Function: %d\n", stmt->select.agg_function);
        printf("  WHERE Clause:\n");
        print_where(stmt->select.where);
        break;

    case STMT_UNKNOWN:
        printf("Unknown statement\n");
        break;
    }
}
