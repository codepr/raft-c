#ifndef TESTS_H
#define TESTS_H

#include <stdint.h>

#define TOLERANCE     0.00001
#define fequals(a, b) fabs((a) - (b)) < TOLERANCE

#define ASSERT_EQ(actual, expected)                                            \
    do {                                                                       \
        if ((actual) != (expected)) {                                          \
            fprintf(stderr, "FAIL: expected %llu, Got %llu\n",                 \
                    (uint64_t)(expected), (uint64_t)(actual));                 \
            return -1;                                                         \
        }                                                                      \
    } while (0)

#define ASSERT_FEQ(actual, expected)                                           \
    do {                                                                       \
        if (!(fequals((actual), (expected)))) {                                \
            fprintf(stderr, "FAIL: expected %.5f, Got %.5f\n",                 \
                    (double_t)(expected), (double_t)(actual));                 \
            return -1;                                                         \
        }                                                                      \
    } while (0)

int timeseries_test(void);

#endif
