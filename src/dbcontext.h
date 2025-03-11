#ifndef DBCONTEXT_H
#define DBCONTEXT_H

#include "timeseries.h"
#include <stdlib.h>
#include <string.h>

extern const size_t DBCTX_BASESIZE;

typedef struct db_ht_entry {
    char name[DATAPATH_SIZE];
    timeseries_db_t *db;
    struct db_ht_entry *next;
} tsdb_ht_entry_t;

typedef struct db_ht {
    tsdb_ht_entry_t **buckets;
    size_t size;
    timeseries_db_t *active_db; // Track the currently active database
} tsdb_ht_t;

extern tsdb_ht_t *tsdb_ht;

int dbcontext_init(size_t size);
void dbcontext_free(void);
timeseries_db_t *dbcontext_add(const char *name);
timeseries_db_t *dbcontext_get(const char *name);
int dbcontext_setactive(const char *name);
timeseries_db_t *dbcontext_getactive(void);

#endif
