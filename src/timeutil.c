#include "timeutil.h"
#include <string.h>

static const struct {
    char unit;
    int mul;
} units[4] = {{'s', 1}, {'m', 60}, {'h', 60 * 60}, {'d', 24 * 60 * 60}};

int64_t current_nanos(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

    // Converts the time to nanoseconds
    return (int64_t)(ts.tv_sec * 1000000000 + ts.tv_nsec);
}

int64_t current_micros(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);

    // Converts the time to microseconds
    return (int64_t)(ts.tv_sec * 1000000 + ts.tv_nsec / 1000);
}

time_t current_seconds(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);

    // Returns the time in seconds
    return ts.tv_sec;
}

int clocktime(struct timespec *ts)
{
    return clock_gettime(CLOCK_PROCESS_CPUTIME_ID, ts);
}

double timespec_seconds(struct timespec *ts)
{
    return (double)ts->tv_sec + (double)ts->tv_nsec * 1.0e-9;
}

time_t timespan_seconds(long long mul, const char *ts)
{
    time_t value = -1LL;

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

time_t datetime_seconds(const char *datetime_str)
{
    struct tm time_info = {0};
    char format[32];

    // Check if the string contains time component
    if (strchr(datetime_str, ' ') != NULL) {
        // Format: "2025-01-08 12:55:00"
        strncpy(format, "%Y-%m-%d %H:%M:%S", sizeof(format));
    } else {
        // Format: "2025-01-08"
        strncpy(format, "%Y-%m-%d", sizeof(format));
        // Set default time to midnight
        time_info.tm_hour = 0;
        time_info.tm_min  = 0;
        time_info.tm_sec  = 0;
    }

    // Parse the string according to the determined format
    if (strptime(datetime_str, format, &time_info) == NULL)
        return -1;

    // Convert to Unix timestamp
    time_t unix_time = mktime(&time_info);

    return unix_time;
    return -1;
}
