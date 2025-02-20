#include "timeutil.h"

long long current_micros(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);

    // Converts the time to microseconds
    return (long long)(ts.tv_sec * 1000000 + ts.tv_nsec / 1000);
}

long long current_seconds(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);

    // Returns the time in seconds
    return (long long)ts.tv_sec;
}

double timespec_seconds(struct timespec *ts)
{
    return (double)ts->tv_sec + (double)ts->tv_nsec * 1.0e-9;
}
