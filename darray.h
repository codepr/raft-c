#ifndef DARRAY_H
#define DARRAY_H

#include <assert.h>
#include <stdio.h>

#define LL_DEBUG    0
#define LL_INFO     1
#define LL_WARNING  2
#define LL_ERROR    3
#define LL_CRITICAL 4

#define LOG_LEVEL   LL_DEBUG

#define RAFT_LOG(level, level_str, fmt, ...)                                   \
    do {                                                                       \
        if (level >= LOG_LEVEL) {                                              \
            fprintf(stderr, "[%s] " fmt "%s",                                  \
                    level_str __VA_OPT__(, ) __VA_ARGS__, "\n");               \
        }                                                                      \
        if (level == LL_CRITICAL)                                              \
            exit(EXIT_FAILURE);                                                \
    } while (0)

#define log_debug(fmt, ...)                                                    \
    RAFT_LOG(LL_DEBUG, "DEBUG", fmt __VA_OPT__(, ) __VA_ARGS__)
#define log_info(fmt, ...)                                                     \
    RAFT_LOG(LL_INFO, "INFO", fmt __VA_OPT__(, ) __VA_ARGS__)
#define log_warning(fmt, ...)                                                  \
    RAFT_LOG(LL_WARNING, "WARNING", fmt __VA_OPT__(, ) __VA_ARGS__)
#define log_error(fmt, ...)                                                    \
    RAFT_LOG(LL_ERROR, "ERROR", fmt __VA_OPT__(, ) __VA_ARGS__)
#define log_critical(fmt, ...)                                                 \
    RAFT_LOG(LL_CRITICAL, "CRITICAL", fmt __VA_OPT__(, ) __VA_ARGS__)

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
