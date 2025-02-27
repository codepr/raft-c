#include "tsdbmanager.h"
#include "hash.h"

#define BASESIZE 64

tsdb_ht_t *tsdb_ht = NULL;

static size_t hash_dbname(const char *name, size_t mapsize)
{
    uint32_t hash = murmur3_hash((const uint8_t *)name, 0);
    return hash % mapsize;
}

int tsdbmanager_init(size_t size)
{
    if (tsdb_ht != NULL) {
        return 0; // Already initialized
    }

    tsdb_ht = malloc(sizeof(tsdb_ht_t));
    if (!tsdb_ht) {
        return -1;
    }

    tsdb_ht->buckets = calloc(size, sizeof(tsdb_ht_entry_t *));
    if (!tsdb_ht->buckets) {
        free(tsdb_ht);
        tsdb_ht = NULL;
        return -1;
    }

    tsdb_ht->size      = size;
    tsdb_ht->active_db = NULL;

    return 0;
}

void tsdbmanager_free(void)
{
    if (!tsdb_ht) {
        return;
    }

    // Free all entries and their associated databases
    for (size_t i = 0; i < tsdb_ht->size; i++) {
        tsdb_ht_entry_t *entry = tsdb_ht->buckets[i];
        while (entry) {
            tsdb_ht_entry_t *next = entry->next;
            tsdb_close(entry->db);
            free(entry);
            entry = next;
        }
    }

    free(tsdb_ht->buckets);
    free(tsdb_ht);
    tsdb_ht = NULL;
}

timeseries_db_t *tsdbmanager_add(const char *name)
{
    if (!tsdb_ht) {
        if (tsdbmanager_init(BASESIZE) != 0) {
            return NULL;
        }
    }

    size_t bucket          = hash_dbname(name, tsdb_ht->size);

    // Check if database already exists
    tsdb_ht_entry_t *entry = tsdb_ht->buckets[bucket];
    while (entry) {
        if (strcmp(entry->name, name) == 0) {
            // Database already exists, return it
            return entry->db;
        }
        entry = entry->next;
    }

    // Create new database
    timeseries_db_t *new_db = tsdb_init(name);
    if (!new_db) {
        return NULL;
    }

    // Create and populate new entry
    entry = malloc(sizeof(tsdb_ht_entry_t));
    if (!entry) {
        free(new_db);
        return NULL;
    }

    strncpy(entry->name, name, DATAPATH_SIZE - 1);
    entry->name[DATAPATH_SIZE - 1] = '\0';
    entry->db                      = new_db;

    // Add to hashmap
    entry->next                    = tsdb_ht->buckets[bucket];
    tsdb_ht->buckets[bucket]       = entry;

    // Set as active if no active database
    if (!tsdb_ht->active_db) {
        tsdb_ht->active_db = new_db;
    }

    return new_db;
}

timeseries_db_t *tsdbmanager_get(const char *name)
{
    if (!tsdb_ht) {
        return NULL;
    }

    size_t bucket          = hash_dbname(name, tsdb_ht->size);
    tsdb_ht_entry_t *entry = tsdb_ht->buckets[bucket];

    while (entry) {
        if (strcmp(entry->name, name) == 0) {
            return entry->db;
        }
        entry = entry->next;
    }

    return NULL;
}

int tsdbmanager_setactive(const char *name)
{
    timeseries_db_t *db = tsdbmanager_get(name);
    if (!db) {
        return -1;
    }

    tsdb_ht->active_db = db;
    return 0;
}

timeseries_db_t *tsdbmanager_getactive(void)
{
    return tsdb_ht ? tsdb_ht->active_db : NULL;
}
