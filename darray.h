#ifndef DARRAY_H
#define DARRAY_H

#include "logger.h"
#include <assert.h>

/**
 ** Dynamic array utility macros
 **/

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

#define da_back(da) (da)->items[(da)->length - 1]

#endif
