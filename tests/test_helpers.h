#ifndef TEST_HELPERS
#define TEST_HELPERS

#include <stdint.h>
#include <string.h>

#define TOLERANCE     0.00001
#define fequals(a, b) fabs((a) - (b)) < TOLERANCE

#define ASSERT_TRUE(condition, msg)                                            \
    do {                                                                       \
        if (!(condition)) {                                                    \
            fprintf(stderr, (msg));                                            \
            return -1;                                                         \
        }                                                                      \
    } while (0)

#define ASSERT_EQ(actual, expected)                                            \
    do {                                                                       \
        if ((actual) != (expected)) {                                          \
            fprintf(stderr,                                                    \
                    "FAIL: integer expected %llu, got %llu (line %d)\n",       \
                    (uint64_t)(expected), (uint64_t)(actual), __LINE__);       \
            return -1;                                                         \
        }                                                                      \
    } while (0)

#define ASSERT_FEQ(actual, expected)                                           \
    do {                                                                       \
        if (!(fequals((actual), (expected)))) {                                \
            fprintf(stderr, "FAIL: float expected %.5f, got %.5f (line %d)\n", \
                    (double_t)(expected), (double_t)(actual), __LINE__);       \
            return -1;                                                         \
        }                                                                      \
    } while (0)

#define ASSERT_SEQ(actual, expected)                                           \
    do {                                                                       \
        if (!(actual)) {                                                       \
            fprintf(stderr, "FAIL: NULL actual\n");                            \
            return -1;                                                         \
        }                                                                      \
        if (strncmp((actual), (expected), strlen((expected))) != 0) {          \
            fprintf(stderr, "FAIL: expected %s, got %s (line %d)\n",           \
                    (expected), (actual), __LINE__);                           \
            return -1;                                                         \
        }                                                                      \
    } while (0)

void rm_recursive(const char *path);

#endif
