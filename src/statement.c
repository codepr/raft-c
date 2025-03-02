#include "statement.h"
#include "darray.h"
#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

string_view_t sv_from_parts(const char *src, size_t len)
{
    string_view_t view = {.length = len, .p = src};
    return view;
}

string_view_t sv_from_cstring(const char *src)
{
    return sv_from_parts(src, strlen(src));
}

// Skip whitespace in a string view
void sv_trim_left(string_view_t *view)
{
    size_t i = 0;
    while (i < view->length && isspace(view->p[i])) {
        i += 1;
    }

    view->p += i;
    view->length -= i;
}

// Case-insensitive comparison of string view with C string
bool sv_equals_cstr_ignorecase(string_view_t sv, const char *cstr)
{
    size_t cstr_len = strlen(cstr);
    if (sv.length != cstr_len)
        return false;

    for (size_t i = 0; i < sv.length; i++) {
        if (toupper(sv.p[i]) != toupper(cstr[i])) {
            return false;
        }
    }

    return true;
}

string_view_t sv_chop_by_delim(string_view_t *view, char delim)
{
    size_t i = 0;
    while (i < view->length && view->p[i] != delim) {
        i += 1;
    }

    string_view_t result = sv_from_parts(view->p, i);

    if (i < view->length) {
        view->length -= i + 1;
        view->p += i + 1;
    } else {
        view->length -= i;
        view->p += i;
    }

    return result;
}

// Define token types
typedef enum {
    TOKEN_USE,
    TOKEN_META,
    TOKEN_VALUE,
    TOKEN_VALUES,
    TOKEN_SAMPLE,
    TOKEN_CREATE,
    TOKEN_CREATEDB,
    TOKEN_INSERT,
    TOKEN_LIMIT,
    TOKEN_INTO,
    TOKEN_NUMBER,
    TOKEN_TIMEUNIT,
    TOKEN_IDENTIFIER,
    TOKEN_SELECT,
    TOKEN_DELETE,
    TOKEN_FROM,
    TOKEN_AND,
    TOKEN_BETWEEN,
    TOKEN_WHERE,
    TOKEN_WILDCARD,
    TOKEN_LPAREN,
    TOKEN_RPAREN,
    TOKEN_COMMA,
    TOKEN_ERROR,
    TOKEN_OPERATOR_EQ,
    TOKEN_OPERATOR_NE,
    TOKEN_OPERATOR_LE,
    TOKEN_OPERATOR_LT,
    TOKEN_OPERATOR_GE,
    TOKEN_OPERATOR_GT,
    TOKEN_LITERAL,
    TOKEN_FUNC_MIN,
    TOKEN_FUNC_MAX,
    TOKEN_FUNC_AVG,
    TOKEN_FUNC_NOW,
    TOKEN_FUNC_LATEST,
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

static bool match_separator(string_view_t *source, token_t *token)
{
    // Check for single-character tokens
    switch (*source->p) {
    case '(':
        token->type = TOKEN_LPAREN;
        break;
    case ')':
        token->type = TOKEN_RPAREN;
        break;
    case ',':
        token->type = TOKEN_COMMA;
        break;
    default:
        return false;
    }

    *token->value = *source->p;
    source->p += 1;
    source->length -= 1;
    return true;
}

static bool match_literal(string_view_t *source, token_t *token)
{
    if (*source->p != '\'')
        return false;

    size_t i = 1; // Skip opening quote
    while (i < source->length && source->p[i] != '\'') {
        i++;
    }

    if (i >= source->length) {
        // Unterminated string
        token->type = TOKEN_ERROR;
        return false;
    }

    // Include content without quotes
    token->type = TOKEN_LITERAL;
    strncpy(token->value, source->p + 1, i - 1);

    // Skip past the closing quote
    source->p += i + 1;
    source->length -= i + 1;
    return true;
}

static bool match_number(string_view_t *source, token_t *token)
{
    if (isdigit(*source->p) ||
        (*source->p == '.' && source->length > 1 && isdigit(*source->p + 1))) {

        size_t i = 0;

        // Process integer part
        while (i < source->length && isdigit(source->p[i])) {
            i++;
        }

        // Process decimal point and fractional part
        if (i < source->length && source->p[i] == '.') {
            i++; // Skip the decimal point

            // Process digits after decimal point
            while (i < source->length && isdigit(source->p[i])) {
                i++;
            }
        }

        // Process exponent part (e.g., 1.23e-4)
        if (i < source->length &&
            (source->p[i] == 'e' || source->p[i] == 'E')) {
            i++; // Skip the 'e' or 'E'

            // Optional sign for exponent
            if (i < source->length &&
                (source->p[i] == '+' || source->p[i] == '-')) {
                i++;
            }

            // Must have at least one digit after exponent
            if (i >= source->length || !isdigit(source->p[i])) {
                // Invalid number format
                token->type = TOKEN_ERROR;
                strncpy(token->value, source->p, i);
                source->p += i;
                source->length -= i;
                return false;
            }

            // Process digits in exponent
            while (i < source->length && isdigit(source->p[i])) {
                i++;
            }
        }

        token->type = TOKEN_NUMBER;
        strncpy(token->value, source->p, i);

        source->p += i;
        source->length -= i;

        return true;
    }

    return false;
}

static bool match_timeunit(string_view_t *source, token_t *token)
{
    if (!isdigit(source->p[0]))
        return false;

    size_t i = 0;
    while (i < source->length && isdigit(source->p[i])) {
        i++;
    }

    if (source->p[i] != 'd' && source->p[i] != 'h' && source->p[i] != 'm' &&
        source->p[i] != 's')
        return false;

    size_t unit_len = 1;
    // Special case for "ms"
    if (source->length > 1 && source->p[i] == 'm' && source->p[i + 1] == 's') {
        unit_len = 2;
    }

    token->type = TOKEN_TIMEUNIT;
    strncpy(token->value, source->p, unit_len + i);

    source->p += unit_len + i;
    source->length -= unit_len + i;

    return true;
}

// Check if a char is a valid identifier character
static inline bool is_identifier_char(char c)
{
    return !isspace(c) && c != '(' && c != ')';
}

static bool match_keyword(string_view_t *source, token_t *token,
                          const token_t *prev)
{
    if (prev->type == TOKEN_LPAREN || prev->type == TOKEN_WHERE)
        return false;
    size_t i = 0;
    while (i < source->length && is_identifier_char(source->p[i])) {
        i++;
    }

    string_view_t value = sv_from_parts(source->p, i);

    // Check for keywords
    if (sv_equals_cstr_ignorecase(value, "USE")) {
        token->type = TOKEN_USE;
    } else if (sv_equals_cstr_ignorecase(value, "CREATEDB")) {
        token->type = TOKEN_CREATEDB;
    } else if (sv_equals_cstr_ignorecase(value, "DELETE")) {
        token->type = TOKEN_DELETE;
    } else if (sv_equals_cstr_ignorecase(value, "CREATE")) {
        token->type = TOKEN_CREATE;
    } else if (sv_equals_cstr_ignorecase(value, "INSERT")) {
        token->type = TOKEN_INSERT;
    } else if (sv_equals_cstr_ignorecase(value, "SELECT")) {
        token->type = TOKEN_SELECT;
    } else if (sv_equals_cstr_ignorecase(value, "FROM")) {
        token->type = TOKEN_FROM;
    } else if (sv_equals_cstr_ignorecase(value, "INTO")) {
        token->type = TOKEN_INTO;
    } else if (sv_equals_cstr_ignorecase(value, "WHERE")) {
        token->type = TOKEN_WHERE;
    } else if (sv_equals_cstr_ignorecase(value, "BETWEEN")) {
        token->type = TOKEN_BETWEEN;
    } else if (sv_equals_cstr_ignorecase(value, "AND")) {
        token->type = TOKEN_AND;
    } else if (sv_equals_cstr_ignorecase(value, "SAMPLE")) {
        token->type = TOKEN_SAMPLE;
    } else if (sv_equals_cstr_ignorecase(value, "BY")) {
        token->type = TOKEN_BY;
    } else if (sv_equals_cstr_ignorecase(value, "LIMIT")) {
        token->type = TOKEN_LIMIT;
    } else if (sv_equals_cstr_ignorecase(value, "VALUE")) {
        token->type = TOKEN_VALUE;
    } else if (sv_equals_cstr_ignorecase(value, "VALUES")) {
        token->type = TOKEN_VALUES;
    } else if (sv_equals_cstr_ignorecase(value, ".databases") ||
               sv_equals_cstr_ignorecase(value, ".timeseries")) {
        token->type = TOKEN_META;
    } else if (sv_equals_cstr_ignorecase(value, ">")) {
        token->type = TOKEN_OPERATOR_GT;
    } else if (sv_equals_cstr_ignorecase(value, "<")) {
        token->type = TOKEN_OPERATOR_LT;
    } else if (sv_equals_cstr_ignorecase(value, "=")) {
        token->type = TOKEN_OPERATOR_EQ;
    } else if (sv_equals_cstr_ignorecase(value, ">=")) {
        token->type = TOKEN_OPERATOR_GE;
    } else if (sv_equals_cstr_ignorecase(value, "<=")) {
        token->type = TOKEN_OPERATOR_LE;
    } else if (sv_equals_cstr_ignorecase(value, "!=")) {
        token->type = TOKEN_OPERATOR_NE;
    } else {
        return false;
    }

    strncpy(token->value, value.p, value.length);

    source->p += i;
    source->length -= i;

    return true;
}

static bool match_function(string_view_t *source, token_t *token)
{
    if (sv_equals_cstr_ignorecase(*source, "min")) {
        token->type = TOKEN_FUNC_MIN;
    } else if (sv_equals_cstr_ignorecase(*source, "max")) {
        token->type = TOKEN_FUNC_MAX;
    } else if (sv_equals_cstr_ignorecase(*source, "avg")) {
        token->type = TOKEN_FUNC_AVG;
    } else if (sv_equals_cstr_ignorecase(*source, "now")) {
        token->type = TOKEN_FUNC_NOW;
    } else if (sv_equals_cstr_ignorecase(*source, "latest")) {
        token->type = TOKEN_FUNC_LATEST;
    } else {
        token->type = TOKEN_ERROR;
        return false;
    }
    return true;
}

static bool match_identifier(string_view_t *source, token_t *token)
{
    size_t i = 0;
    while (i < source->length && is_identifier_char(source->p[i])) {
        i++;
    }

    string_view_t value     = sv_from_parts(source->p, i);

    // It's an identifier - check if it's followed by a parenthesis
    string_view_t remaining = *source;
    remaining.p += i;
    remaining.length -= i;

    sv_trim_left(&remaining);

    if (remaining.length > 0 && remaining.p[0] == '(') {
        if (!match_function(&value, token))
            return false;
    } else {
        token->type = TOKEN_IDENTIFIER;
    }

    strncpy(token->value, value.p, value.length);

    source->p += i;
    source->length -= i;

    return true;
}

// Get next token from string view
static token_t tokenize_next(string_view_t *source, const token_t *prev)
{
    token_t token = {0};

    // Skip leading whitespace
    sv_trim_left(source);

    // Check for EOF
    if (source->length == 0) {
        token.type = TOKEN_EOF;
        return token;
    }

    // Check in order for
    // - single-character tokens
    // - string literals
    // - timeunits
    // - numbers
    // - identifiers or keywords
    if (match_separator(source, &token) || match_literal(source, &token) ||
        match_timeunit(source, &token) || match_number(source, &token) ||
        match_keyword(source, &token, prev) || match_identifier(source, &token))
        return token;

    // If we reach here, it's an unexpected character
    token.type   = TOKEN_ERROR;
    *token.value = *source->p;
    source->p += 1;
    source->length -= 1;

    return token;
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

static ssize_t tokenize(const char *query, token_array_t *token_array)
{
    string_view_t view = sv_from_cstring(query);
    token_t token      = {0};

    do {
        token = tokenize_next(&view, &token);
        da_append(token_array, token);
    } while (token.type != TOKEN_EOF && token.type != TOKEN_ERROR);

    token.type     = EOF;
    token.value[0] = EOF;
    da_append(token_array, token);

    return token_array->length;
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

static int expect_timeunit(parser_t *p, stmt_timeunit_t *tu)
{
    if (expect(p, TOKEN_TIMEUNIT) < 0)
        return -1;
    char *timeunit = p->tokens[p->pos - 1].value;
    char *endptr;
    errno              = 0;

    tu->interval.value = strtoll(timeunit, &endptr, 10);
    if (errno != 0)
        return -1;

    char *ptr = &tu->interval.unit[0];

    for (; isalpha(*endptr) && is_identifier_char(*endptr); *ptr++ = *endptr++)
        ;

    return 0;
}

static char *expect_literal(parser_t *p)
{
    if (expect(p, TOKEN_LITERAL) < 0)
        return NULL;
    return p->tokens[p->pos - 1].value;
}

static char *expect_identifier(parser_t *p)
{
    if (expect(p, TOKEN_IDENTIFIER) < 0)
        return NULL;
    return p->tokens[p->pos - 1].value;
}

static char *expect_meta(parser_t *p)
{
    if (expect(p, TOKEN_META) < 0)
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

static int expect_function(parser_t *p)
{
    token_t *t    = parser_peek(p);
    function_t fn = FN_NONE;

    switch (t->type) {
    case TOKEN_FUNC_AVG:
        fn = FN_AVG;
        break;
    case TOKEN_FUNC_MAX:
        fn = FN_MAX;
        break;
    case TOKEN_FUNC_MIN:
        fn = FN_MIN;
        break;
    case TOKEN_FUNC_NOW:
        fn = FN_NOW;
        break;
    case TOKEN_FUNC_LATEST:
        fn = FN_LATEST;
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

static stmt_t *parse_meta(parser_t *p)
{
    stmt_t *node = calloc(1, sizeof(*node));
    if (!node)
        return NULL;

    node->type = STMT_META;

    char *meta = expect_meta(p);
    if (!meta)
        goto err;

    node->meta = strncasecmp(meta, ".databases", 10) == 0    ? META_DATABASES
                 : strncasecmp(meta, ".timeseries", 11) == 0 ? META_TIMESERIES
                                                             : META_UNKNOWN;

    return node;

err:
    free(node);
    return NULL;
}

static stmt_t *parse_use(parser_t *p)
{
    stmt_t *node = calloc(1, sizeof(*node));
    if (!node)
        return NULL;

    node->type = STMT_USE;

    if (expect(p, TOKEN_USE) < 0)
        goto err;

    char *tsname = expect_identifier(p);
    if (!tsname)
        goto err;

    copy_identifier(node->use.db_name, tsname);

    return node;

err:
    free(node);
    return NULL;
}

static stmt_t *parse_createdb(parser_t *p)
{
    stmt_t *node = calloc(1, sizeof(*node));
    if (!node)
        return NULL;

    node->type = STMT_CREATEDB;

    if (expect(p, TOKEN_CREATEDB) < 0)
        goto err;

    char *tsname = expect_identifier(p);
    if (!tsname)
        goto err;

    copy_identifier(node->create.db_name, tsname);

    return node;

err:
    free(node);
    return NULL;
}

static stmt_t *parse_create(parser_t *p)
{
    stmt_t *node = calloc(1, sizeof(*node));
    if (!node)
        return NULL;

    node->type = STMT_CREATE;

    if (expect(p, TOKEN_CREATE) < 0)
        goto err;

    char *tsid = expect_identifier(p);
    if (!tsid)
        goto err;

    copy_identifier(node->create.ts_name, tsid);

    if (parser_peek(p)->type == TOKEN_EOF)
        return node;

    // Optional retention + duplication policy integers

    if (parser_peek(p)->type == TOKEN_TIMEUNIT) {
        if (expect_timeunit(p, &node->create.retention) < 0)
            goto err;

        node->create.retention.type = TU_INTERVAL;
        node->create.has_retention  = true;
    } else if (parser_peek(p)->type == TOKEN_NUMBER) {
        node->create.has_retention = true;
        if (expect_integer(p, (int64_t *)&node->create.retention) < 0)
            goto err;

        node->create.retention.type = TU_SINGLE;
        node->create.has_retention  = true;
    }

    if (parser_peek(p)->type == TOKEN_LITERAL) {
        char *duplication = expect_literal(p);
        if (!duplication)
            goto err;

        copy_identifier(node->create.duplication, duplication);
        node->create.has_duplication = true;
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

    if (expect(p, TOKEN_INTO) < 0)
        goto err;

    char *tsname = expect_identifier(p);
    if (!tsname)
        goto err;

    copy_identifier(node->insert.ts_name, tsname);

    if (parser_peek(p)->type == TOKEN_VALUE) {

        if (expect(p, TOKEN_VALUE) < 0)
            goto err;

        stmt_record_t record = {0};
        record.timestamp     = current_micros();
        if (expect_float(p, &record.value) < 0)
            goto err;
        da_append(&node->insert.record_array, record);
        return node;
    }

    if (parser_peek(p)->type == TOKEN_VALUES) {

        if (expect(p, TOKEN_VALUES) < 0)
            goto err;

        stmt_record_t record = {0};
        do {
            if (expect(p, TOKEN_LPAREN) < 0)
                goto err;
            if (parser_peek(p)->type == TOKEN_FUNC_NOW) {
                // Just consume the tokens till the comma
                if (expect(p, TOKEN_FUNC_NOW) < 0)
                    goto err;
                if (expect(p, TOKEN_LPAREN) < 0)
                    goto err;
                if (expect(p, TOKEN_RPAREN) < 0)
                    goto err;

                record.timestamp = current_micros();
            } else {
                if (expect_integer(p, &record.timestamp) < 0)
                    goto err;
            }
            if (expect(p, TOKEN_COMMA) < 0)
                goto err;
            if (expect_float(p, &record.value) < 0)
                goto err;

            da_append(&node->insert.record_array, record);

            if (expect(p, TOKEN_RPAREN) < 0)
                goto err;
        } while (parser_peek(p)->type == TOKEN_COMMA &&
                 expect(p, TOKEN_COMMA) == 0);

        return node;
    }

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

    node->type         = STMT_SELECT;
    node->select.flags = QF_BASE;

    if (expect(p, TOKEN_SELECT) < 0)
        goto err;

    if (parser_peek(p)->type >= TOKEN_FUNC_MIN &&
        parser_peek(p)->type <= TOKEN_FUNC_LATEST) {
        node->select.function = expect_function(p);
        // TODO skip for now
        if (expect(p, TOKEN_LPAREN) < 0)
            goto err;
        if (expect(p, TOKEN_IDENTIFIER) < 0)
            goto err;
        if (expect(p, TOKEN_RPAREN) < 0)
            goto err;

        node->select.flags |= QF_FUNC;
    } else {
        // TODO skip for now
        if (expect(p, TOKEN_IDENTIFIER) < 0)
            goto err;
    }

    if (expect(p, TOKEN_FROM) < 0)
        goto err;

    char *tsname = expect_identifier(p);
    if (!tsname)
        goto err;

    copy_identifier(node->select.ts_name, tsname);

    if (parser_peek(p)->type == TOKEN_BETWEEN) {
        if (expect(p, TOKEN_BETWEEN) < 0)
            goto err;
        if (parser_peek(p)->type == TOKEN_NUMBER) {
            node->select.timeunit.type = TU_TSINTERVAL;
            if (expect_integer(p, &node->select.timeunit.tsinterval.start) < 0)
                goto err;
            if (expect(p, TOKEN_AND) < 0)
                goto err;
            if (expect_integer(p, &node->select.timeunit.tsinterval.end) < 0)
                goto err;
        } else if (parser_peek(p)->type == TOKEN_LITERAL) {
            node->select.timeunit.type = TU_DATEINTERVAL;

            char *startdate            = expect_literal(p);
            if (!startdate)
                goto err;

            copy_identifier(node->select.timeunit.dateinterval.start,
                            startdate);

            if (expect(p, TOKEN_AND) < 0)
                goto err;

            char *enddate = expect_literal(p);
            if (!enddate)
                goto err;

            copy_identifier(node->select.timeunit.dateinterval.end, enddate);
        } else if (parser_peek(p)->type == TOKEN_FUNC_NOW) {
            // Just consume the tokens till the comma
            if (expect(p, TOKEN_FUNC_NOW) < 0)
                goto err;
            if (expect(p, TOKEN_LPAREN) < 0)
                goto err;
            if (expect(p, TOKEN_RPAREN) < 0)
                goto err;

            node->select.timeunit.type             = TU_TSINTERVAL;
            node->select.timeunit.tsinterval.start = current_micros();

            if (expect(p, TOKEN_AND) < 0)
                goto err;

            if (parser_peek(p)->type == TOKEN_FUNC_NOW) {
                // Just consume the tokens till the comma
                if (expect(p, TOKEN_FUNC_NOW) < 0)
                    goto err;
                if (expect(p, TOKEN_LPAREN) < 0)
                    goto err;
                if (expect(p, TOKEN_RPAREN) < 0)
                    goto err;

                node->select.timeunit.tsinterval.end = current_micros();
            } else {
                if (expect_integer(p, &node->select.timeunit.tsinterval.end) <
                    0)
                    goto err;
            }
        }
        node->select.flags |= QF_RNGE;
    }

    if (parser_peek(p)->type == TOKEN_WHERE) {
        if (expect(p, TOKEN_WHERE) < 0)
            goto err;
        node->select.where = parse_where(p);
        node->select.flags |= QF_COND;
    }

    if (parser_peek(p)->type == TOKEN_SAMPLE) {
        // Consume token
        if (expect(p, TOKEN_SAMPLE) < 0)
            goto err;

        if (expect(p, TOKEN_BY) < 0)
            goto err;

        if (parser_peek(p)->type == TOKEN_TIMEUNIT) {
            if (expect_timeunit(p, &node->select.sampling) < 0)
                goto err;
        }

        node->select.flags |= QF_SMPL;
    }

    if (parser_peek(p)->type == TOKEN_LIMIT) {
        if (expect(p, TOKEN_LIMIT) < 0)
            goto err;

        if (expect_integer(p, &node->select.limit) < 0)
            goto err;

        node->select.flags |= QF_LIMT;
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
    case TOKEN_USE:
        node = parse_use(&parser);
        break;
    case TOKEN_META:
        node = parse_meta(&parser);
        break;
    case TOKEN_CREATEDB:
        node = parse_createdb(&parser);
        break;
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

static void print_timeunit(const stmt_timeunit_t *tu)
{
    printf("  BETWEEN: ");
    switch (tu->type) {
    case TU_SINGLE:
        printf("    [%" PRIi64 "]", tu->value);
        break;
    case TU_INTERVAL:
        printf("    [%" PRIi64 "%s]", tu->interval.value, tu->interval.unit);
        break;
    case TU_TSINTERVAL:
        printf("    start: [%" PRIi64 "] end: [%" PRIi64 "]",
               tu->tsinterval.start, tu->tsinterval.end);
        break;
    case TU_DATEINTERVAL:
        printf("    start: [%s] end: [%s]", tu->dateinterval.start,
               tu->dateinterval.end);
        break;
    }
    printf("\n");
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

    case STMT_CREATEDB:
        printf("CREATEDB statement:\n");
        printf("  DB Name: %s\n", stmt->create.db_name);
        break;

    case STMT_USE:
        printf("USE statement:\n");
        printf("   DB Name: %s\n", stmt->use.ts_name);
        break;
        break;

    case STMT_CREATE:
        printf("CREATE statement:\n");
        printf("  TS Name: %s\n", stmt->create.ts_name);
        if (stmt->create.has_retention) {
            if (stmt->create.retention.type == TU_SINGLE)
                printf("  Retention: %lld\n", stmt->create.retention.value);
            else if (stmt->create.retention.type == TU_INTERVAL)
                printf("  Retention: %lld%s\n",
                       stmt->create.retention.interval.value,
                       stmt->create.retention.interval.unit);
        }
        if (stmt->create.has_duplication) {
            printf("  Duplication: %s\n", stmt->create.duplication);
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
        printf("  INTO: %s\n", stmt->insert.ts_name);
        printf("  VALUES (%zu):\n", stmt->insert.record_array.length);
        for (size_t i = 0; i < stmt->insert.record_array.length; i++) {
            printf("    [%" PRIu64 "] = %f\n",
                   stmt->insert.record_array.items[i].timestamp,
                   stmt->insert.record_array.items[i].value);
        }
        break;

    case STMT_SELECT:
        printf("SELECT statement:\n");
        printf("  FROM: %s\n", stmt->select.ts_name);
        if (stmt->select.flags & QF_RNGE) {
            print_timeunit(&stmt->select.timeunit);
        }
        if (stmt->select.flags & QF_FUNC)
            printf("  Aggregate Function: %d\n", stmt->select.function);
        if (stmt->select.flags & QF_COND) {
            printf("  WHERE Clause:\n");
            print_where(stmt->select.where);
        }
        if (stmt->select.flags & QF_SMPL) {
            printf("  SAMPLE BY: %" PRIu64 "%s\n",
                   stmt->select.sampling.interval.value,
                   stmt->select.sampling.interval.unit);
        }
        if (stmt->select.flags & QF_LIMT) {
            printf("   LIMIT: %" PRIi64 "\n", stmt->select.limit);
        }
        break;

    case STMT_META:
        printf("METACMD statement:\n");
        printf("  %s\n", stmt->meta == META_DATABASES    ? ".databases"
                         : stmt->meta == META_TIMESERIES ? ".timeseries"
                                                         : "unknown");
        break;
    case STMT_UNKNOWN:
        printf("Unknown statement\n");
        break;
    }
}
