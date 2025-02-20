#ifndef TIMEUTIL_H
#define TIMEUTIL_H

#include <time.h>

long long current_seconds(void);
long long current_micros(void);
double timespec_seconds(struct timespec *ts);

#endif
