#include "timeutil.h"
#include <string.h>

static const struct {
    char unit;
    int mul;
} units[4] = {{'s', 1}, {'m', 60}, {'h', 60 * 60}, {'d', 24 * 60 * 60}};

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

int clocktime(struct timespec *ts)
{
    return clock_gettime(CLOCK_PROCESS_CPUTIME_ID, ts);
}

double timespec_seconds(struct timespec *ts)
{
    return (double)ts->tv_sec + (double)ts->tv_nsec * 1.0e-9;
}

long long timespan_seconds(long long mul, const char *ts)
{
    long long value = -1LL;

    if (strlen(ts) == 2) {
        if (strncmp(ts, "ns", strlen(ts)) == 0) {
            value = mul / (1000 * 1000 * 1000);
        }
        if (strncmp(ts, "us", strlen(ts)) == 0) {
            value = mul / (1000 * 1000);
        }
        if (strncmp(ts, "ms", strlen(ts)) == 0) {
            value = mul / 1000;
        }
    } else if (strlen(ts) == 1) {
        for (int i = 0; i < 4; ++i)
            if (units[i].unit == ts[0])
                value = mul * units[i].mul;
    }

    return value;
}

long long datetime_seconds(const char *dt)
{
    // TODO
    return -1;
}
