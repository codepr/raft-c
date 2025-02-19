#ifndef DARRAY_H
#define DARRAY_H

#include "logger.h"
#include <assert.h>

/**
 ** Dynamic array utility macros
 **/

#define darray(T)                                                              \
    struct {                                                                   \
        size_t length;                                                         \
        size_t capacity;                                                       \
        T *items;                                                              \
    }

#define da_extend(da)                                                          \
    do {                                                                       \
        (da)->capacity += 1;                                                   \
        (da)->capacity *= 2;                                                   \
        (da)->items =                                                          \
            realloc((da)->items, (da)->capacity * sizeof(*(da)->items));       \
        if (!(da)->items) {                                                    \
            log_critical("DA realloc failed");                                 \
        }                                                                      \
    } while (0)

#define da_append(da, item)                                                    \
    do {                                                                       \
        assert((da));                                                          \
        if ((da)->length + 1 >= (da)->capacity)                                \
            da_extend((da));                                                   \
        (da)->items[(da)->length++] = (item);                                  \
    } while (0)

#define da_insert(da, i, item)                                                 \
    do {                                                                       \
        assert((da));                                                          \
        if ((i) >= (da)->length)                                               \
            da_extend((da));                                                   \
        (da)->items[i] = (item);                                               \
        if ((i) >= (da)->length)                                               \
            (da)->length++;                                                    \
    } while (0)

#define da_search(da, target, cmp, res)                                        \
    do {                                                                       \
        *(res) = -1;                                                           \
        if ((cmp)(&(da)->items[0], (target)) < 1) {                            \
            for (size_t i = 0; i < (da)->length; ++i) {                        \
                if ((cmp)(&(da)->items[i], (target)) == 0) {                   \
                    *(res) = i;                                                \
                }                                                              \
            }                                                                  \
        }                                                                      \
    } while (0)

#define da_bsearch(da, target, cmp, res)                                       \
    do {                                                                       \
        if ((cmp)(&(da)->items[0], (target)) >= 0) {                           \
            *(res) = 0;                                                        \
        } else if ((cmp)(&(da)->items[(da)->length - 1], (target)) <= 0) {     \
            *(res) = (da)->length - 1;                                         \
        } else {                                                               \
            size_t left = 0, middle = 0, right = (da)->length - 1;             \
            int found = 0;                                                     \
            while (left <= right) {                                            \
                middle = floor((left + right) / 2);                            \
                if ((cmp)(&(da)->items[middle], (target)) < 0) {               \
                    left = middle + 1;                                         \
                } else if ((cmp)(&(da)->items[middle], (target)) > 0) {        \
                    right = middle - 1;                                        \
                } else {                                                       \
                    *(res) = middle;                                           \
                    found  = 1;                                                \
                    break;                                                     \
                }                                                              \
            }                                                                  \
            if (found == 0) {                                                  \
                *(res) = left;                                                 \
            }                                                                  \
        }                                                                      \
    } while (0)

#define da_back(da)       (da)->items[(da)->length - 1]

#define da_get(da, index) (da).items[(index)]

#define da_free(da)                                                            \
    do {                                                                       \
        if ((da)->items) {                                                     \
            free((da)->items);                                                 \
            (da)->items = NULL;                                                \
        }                                                                      \
    } while (0)

#endif
