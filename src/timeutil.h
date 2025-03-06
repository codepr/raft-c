#ifndef TIMEUTIL_H
#define TIMEUTIL_H

#include <time.h>

long long current_seconds(void);
long long current_micros(void);
int clocktime(struct timespec *ts);
double timespec_seconds(struct timespec *ts);
long long timespan_seconds(long long mul, const char *ts);
long long datetime_seconds(const char *dt);

#endif
