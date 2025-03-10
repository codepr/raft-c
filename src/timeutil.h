#ifndef TIMEUTIL_H
#define TIMEUTIL_H

#include <time.h>
#include <unistd.h>

time_t current_seconds(void);
int64_t current_micros(void);
int64_t current_nanos(void);
int clocktime(struct timespec *ts);
double timespec_seconds(struct timespec *ts);
time_t timespan_seconds(long long mul, const char *ts);
time_t datetime_seconds(const char *datetime_str);

#endif
