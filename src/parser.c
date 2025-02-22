#include "parser.h"
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

// Function to get the next token by a separator from the lexer
static string_view_t lexer_next_by_sep(lexer_t *l, char sep)
{
    string_view_t lexiom = string_view_chop_by_delim(&l->view, sep);
    l->length            = l->view.length;
    return lexiom;
}

// Function to peek at the next token from the lexer without consuming it
static string_view_t lexer_peek(lexer_t *l)
{
    size_t length        = l->length;
    string_view_t lexiom = lexer_next(l);
    l->view.p -= length - l->length;
    l->view.length = length;
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

typedef struct {
    token_t *tokens;
    size_t pos;
} parser_t;

static token_t next_token(parser_t *p) { return p->tokens[p->pos++]; }

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
    operator_t op = OP_EQ;

    switch (t->type) {
    case TOKEN_OPERATOR_EQ:
        break;
    case TOKEN_OPERATOR_NE:
        op = OP_NE;
        break;
    case TOKEN_OPERATOR_LE:
        op = OP_LE;
        break;

    case TOKEN_OPERATOR_GE:
        op = OP_GE;
        break;

    case TOKEN_OPERATOR_LT:
        op = OP_LE;
        break;

    case TOKEN_OPERATOR_GT:
        op = OP_GT;
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
    aggregate_fn_t fn = AFN_AVG;

    switch (t->type) {
    case TOKEN_AGGREGATE_AVG:
        break;
    case TOKEN_AGGREGATE_MAX:
        fn = AFN_MAX;
        break;
    case TOKEN_AGGREGATE_MIN:
        fn = AFN_MIN;
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
    return BOOL_AND;
}

static void ast_node_where_free(ast_node_where_t *node)
{
    if (!node)
        return;

    ast_node_where_free(node->right);
    free(node);
}

static ast_node_where_t *parse_where(parser_t *p)
{
    ast_node_where_t *node = calloc(1, sizeof(*node));
    if (!node)
        return NULL;

    char *key = expect_identifier(p);
    if (!key)
        goto err;

    strncpy(node->key, key, IDENTIFIER_LENGTH);
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
    ast_node_where_free(node);
    return NULL;
}

static ast_node_t *parse_create(parser_t *p)
{
    ast_node_t *node = calloc(1, sizeof(*node));
    if (!node)
        return NULL;

    node->type                     = STATEMENT_CREATE;
    node->create.single            = 1;
    char tsname[IDENTIFIER_LENGTH] = {0};

    if (expect(p, TOKEN_CREATE) < 0)
        goto err;

    char *tsid = expect_identifier(p);
    if (!tsid)
        goto err;

    strncpy(tsname, tsid, IDENTIFIER_LENGTH);

    if (parser_peek(p)->type == TOKEN_INTO) {
        node->create.single = 0;
        if (expect(p, TOKEN_INTO) < 0)
            goto err;
        char *dbname = expect_identifier(p);
        if (!dbname)
            goto err;
        strncpy(node->create.db_name, dbname, IDENTIFIER_LENGTH);
        strncpy(node->create.ts_name, tsname, sizeof(tsname));

        if (parser_peek(p)->type == TOKEN_NUMBER &&
            expect_integer(p, (int64_t *)&node->create.retention) < 0)
            goto err;
        if (parser_peek(p)->type == TOKEN_NUMBER &&
            expect_integer(p, (int64_t *)&node->create.duplication) < 0)
            goto err;
    } else {
        strncpy(node->create.db_name, tsname, sizeof(tsname));
    }

    return node;

err:
    free(node);
    return NULL;
}

static ast_node_t *parse_delete(parser_t *p)
{
    ast_node_t *node = calloc(1, sizeof(*node));
    if (!node)
        return NULL;

    node->type                     = STATEMENT_DELETE;
    node->delete.single            = 1;
    char tsname[IDENTIFIER_LENGTH] = {0};

    if (expect(p, TOKEN_DELETE) < 0)
        goto err;

    char *tsid = expect_identifier(p);
    if (!tsid)
        goto err;

    strncpy(tsname, tsid, IDENTIFIER_LENGTH);

    if (parser_peek(p)->type == TOKEN_FROM) {
        node->delete.single = 0;
        if (expect(p, TOKEN_FROM) < 0)
            goto err;
        char *dbname = expect_identifier(p);
        if (!dbname)
            goto err;
        strncpy(node->delete.db_name, dbname, IDENTIFIER_LENGTH);
        strncpy(node->delete.ts_name, tsname, sizeof(tsname));

    } else {
        strncpy(node->delete.db_name, tsname, sizeof(tsname));
    }

    return node;

err:
    free(node);
    return NULL;
}

static ast_node_t *parse_insert(parser_t *p)
{
    ast_node_t *node = calloc(1, sizeof(*node));
    if (!node)
        return NULL;

    node->type = STATEMENT_INSERT;

    if (expect(p, TOKEN_INSERT) < 0)
        goto err;

    char *tsname = expect_identifier(p);
    if (!tsname)
        goto err;

    strncpy(node->insert.ts_name, tsname, IDENTIFIER_LENGTH);

    if (expect(p, TOKEN_INTO) < 0)
        goto err;

    char *dbname = expect_identifier(p);
    if (!dbname)
        goto err;

    strncpy(node->insert.db_name, dbname, IDENTIFIER_LENGTH);

    ast_node_record_t record = {0};
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

static ast_node_t *parse_select(parser_t *p)
{
    ast_node_t *node = calloc(1, sizeof(*node));
    if (!node)
        return NULL;

    node->type              = STATEMENT_SELECT;
    node->select.start_time = -1;
    node->select.end_time   = -1;

    if (expect(p, TOKEN_SELECT) < 0)
        goto err;

    char *tsname = expect_identifier(p);
    if (!tsname)
        goto err;

    strncpy(node->select.ts_name, tsname, IDENTIFIER_LENGTH);

    if (expect(p, TOKEN_FROM) < 0)
        goto err;

    char *dbname = expect_identifier(p);
    if (!dbname)
        goto err;

    strncpy(node->select.db_name, dbname, IDENTIFIER_LENGTH);

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
        node->select.af = expect_aggregatefn(p);
        if (node->select.af < 0)
            goto err;

        if (parser_peek(p)->type == TOKEN_BY) {
            if (expect(p, TOKEN_BY) < 0)
                goto err;
            if (expect_integer(p, (int64_t *)&node->select.interval) < 0)
                goto err;
        }
    }

    return node;

err:
    free(node);
    return NULL;
}

ast_node_t *ast_parse(const char *input)
{
    token_array_t token_array = {0};
    size_t token_count        = tokenize(input, &token_array);
    ast_node_t *node          = NULL;

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

void ast_free(ast_node_t *node)
{
    if (!node)
        return;

    switch (node->type) {
    case STATEMENT_CREATE:
    case STATEMENT_INSERT:
        free(node);
        break;
    case STATEMENT_SELECT:
        if (node->select.where)
            ast_node_where_free(node->select.where);
        free(node);
        break;
    default:
        break;
    }
}

static void print_create(const ast_node_create_t *create)
{
    printf("ASTNode<create>\n");
    if (create->single) {
        printf("  dbname=%s\n", create->db_name);
    } else {
        printf("  tsname=%s\n", create->ts_name);
        printf("  into=%s\n", create->db_name);
    }
    if (create->retention)
        printf("  retention=%i\n", create->retention);
    if (create->duplication)
        printf("  duplication=%i\n", create->duplication);
}

static void print_delete(const ast_node_delete_t *delete)
{
    printf("ASTNode<delete>\n");
    if (delete->single) {
        printf("  dbname=%s\n", delete->db_name);
    } else {
        printf("  tsname=%s\n", delete->ts_name);
        printf("  from=%s\n", delete->db_name);
    }
}

static void print_insert(const ast_node_insert_t *insert)
{
    printf("ASTNode<insert>\n");
    printf("  tsname=%s\n", insert->ts_name);
    printf("  into=%s\n", insert->db_name);
    printf("  values=\n");
    for (size_t i = 0; i < insert->record_array.length; ++i) {
        printf("    - (%" PRId64 ", %.2f) ",
               insert->record_array.items[i].timestamp,
               insert->record_array.items[i].value);
    }
}

static void print_where(const ast_node_where_t *where)
{
    if (!where)
        return;

    printf("  ASTNode<where>\n");
    printf("    key=%s\n", where->key);

    switch (where->operator) {
    case OP_NONE:
        printf("    operator=NONE\n");
        break;
    case OP_EQ:
        printf("    operator=EQ\n");
        break;
    case OP_NE:
        printf("    operator=NE\n");
        break;
    case OP_GE:
        printf("    operator=GE\n");
        break;
    case OP_LE:
        printf("    operator=LE\n");
        break;
    case OP_GT:
        printf("    operator=GT\n");
        break;
    case OP_LT:
        printf("    operator=LT\n");
        break;
    }

    printf("    value=%.2f\n", where->value);

    if (where->right)
        printf("    boolean=%s\n",
               where->boolean_op == BOOL_AND ? "AND" : "NA");

    print_where(where->right);
}

static void print_select(const ast_node_select_t *select)
{
    printf("ASTNode<select>\n");
    if (*select->db_name)
        printf("  dbname: %s\n", select->db_name);
    if (*select->ts_name)
        printf("  tsname: %s\n", select->ts_name);
    if (select->start_time)
        printf("  starttime: %lli\n", select->start_time);
    if (select->end_time)
        printf("  endtime: %lli\n", select->end_time);
    if (select->where)
        print_where(select->where);
    switch (select->af) {
    case AFN_AVG:
        printf("  aggregate: AVG\n");
        break;
    case AFN_MIN:
        printf("  aggregate: MIN\n");
        break;
    case AFN_MAX:
        printf("  aggregate: MAX\n");
        break;
    default:
        break;
    }
    if (select->interval)
        printf("  interval: %llu\n", select->interval);
}

void print_ast_node(const ast_node_t *node)
{
    if (!node)
        return;
    switch (node->type) {
    case STATEMENT_CREATE:
        print_create(&node->create);
        break;
    case STATEMENT_INSERT:
        print_insert(&node->insert);
        break;
    case STATEMENT_SELECT:
        print_select(&node->select);
        break;
    case STATEMENT_DELETE:
        print_delete(&node->delete);
        break;
    default:
        printf("Unrecognized node\n");
        break;
    }
}
