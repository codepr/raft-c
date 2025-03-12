#include "timeutil.h"
#include <stdlib.h>
#include <string.h>

static const struct {
    char unit;
    int64_t mul;
} units[4] = {{'s', 1 * (int64_t)1e9},
              {'m', 60 * (int64_t)1e9},
              {'h', 60 * 60 * (int64_t)1e9},
              {'d', 24 * 60 * 60 * (int64_t)1e9}};

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

int64_t timespan_seconds(long long mul, const char *ts)
{
    int64_t value = -1LL;

    if (strlen(ts) == 2) {
        if (strncmp(ts, "ns", strlen(ts)) == 0) {
            value = mul;
        }
        if (strncmp(ts, "us", strlen(ts)) == 0) {
            value = mul * (int64_t)1e3;
        }
        if (strncmp(ts, "ms", strlen(ts)) == 0) {
            value = mul * (int64_t)1e6;
        }
    } else if (strlen(ts) == 1) {
        for (int i = 0; i < 4; ++i)
            if (units[i].unit == ts[0]) {
                value = mul * units[i].mul;
                break;
            }
    }

    return value;
}

int64_t datetime_seconds(const char *datetime_str)
{
    struct tm time_info = {0};
    char format[32];
    char *nanosec_str   = NULL;
    int nanoseconds     = 0;

    // Make a copy of the input string to avoid modifying it
    char *datetime_copy = strdup(datetime_str);
    if (!datetime_copy)
        return -1;

    // Check if the string contains nanoseconds part (.123456789)
    char *dot_pos = strchr(datetime_copy, '.');
    if (dot_pos != NULL) {
        // Separate nanoseconds part
        *dot_pos    = '\0';
        nanosec_str = dot_pos + 1;

        // Parse nanoseconds and pad with zeros if needed
        nanoseconds = atoi(nanosec_str);
        int digits  = strlen(nanosec_str);
        for (int i = digits; i < 9; i++) {
            nanoseconds *= 10;
        }
    }

    // Check if the string contains time component
    if (strchr(datetime_copy, ' ') != NULL) {
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
    if (strptime(datetime_copy, format, &time_info) == NULL) {
        free(datetime_copy);
        return -1;
    }

    // Done with the copy
    free(datetime_copy);

    // Convert to Unix timestamp (seconds)
    time_t unix_time = mktime(&time_info);
    if (unix_time == -1)
        return -1;

    // Convert to nanoseconds (10^9 nanoseconds per second)
    int64_t nanosecs = ((int64_t)unix_time * 1000000000LL) + nanoseconds;

    return nanosecs;
}
