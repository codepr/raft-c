#ifndef TSDBMANAGER_H
#define TSDBMANAGER_H

#include "timeseries.h"
#include <stdlib.h>
#include <string.h>

typedef struct db_hashmap_entry {
    char name[DATAPATH_SIZE];
    timeseries_db_t *db;
    struct db_hashmap_entry *next;
} tsdb_ht_entry_t;

typedef struct db_hashmap {
    tsdb_ht_entry_t **buckets;
    size_t size;
    timeseries_db_t *active_db; // Track the currently active database
} tsdb_ht_t;

extern tsdb_ht_t *tsdb_ht;

int tsdbmanager_init(size_t size);
void tsdbmanager_free(void);
timeseries_db_t *tsdbmanager_add(const char *name);
timeseries_db_t *tsdbmanager_get(const char *name);
int tsdbmanager_setactive(const char *name);
timeseries_db_t *tsdbmanager_getactive(void);

#endif
