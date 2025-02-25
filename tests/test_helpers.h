#ifndef TEST_HELPERS
#define TEST_HELPERS

#include <stdint.h>
#include <string.h>

#define TOLERANCE     0.00001
#define fequals(a, b) fabs((a) - (b)) < TOLERANCE

#define ASSERT_EQ(actual, expected)                                            \
    do {                                                                       \
        if ((actual) != (expected)) {                                          \
            fprintf(stderr, "FAIL: integer expected %llu, Got %llu\n",         \
                    (uint64_t)(expected), (uint64_t)(actual));                 \
            return -1;                                                         \
        }                                                                      \
    } while (0)

#define ASSERT_FEQ(actual, expected)                                           \
    do {                                                                       \
        if (!(fequals((actual), (expected)))) {                                \
            fprintf(stderr, "FAIL: float expected %.5f, Got %.5f\n",           \
                    (double_t)(expected), (double_t)(actual));                 \
            return -1;                                                         \
        }                                                                      \
    } while (0)

#define ASSERT_SEQ(actual, expected)                                           \
    do {                                                                       \
        if (strncmp((actual), (expected), strlen((expected))) != 0) {          \
            fprintf(stderr, "FAIL: expected %s, Got %s\n", (expected),         \
                    (actual));                                                 \
            return -1;                                                         \
        }                                                                      \
    } while (0)

void rm_recursive(const char *path);

#endif
