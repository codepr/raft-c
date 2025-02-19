#ifndef INDEX_H
#define INDEX_H

#include <stdint.h>
#include <stdio.h>

/*
 * Keeps the state for an index file on disk, updated every interval values
 * to make it easier to read data efficiently from the main segment storage
 * on disk.
 */
typedef struct index {
    FILE *fp;
    size_t size;
    uint64_t base_timestamp;
} index_t;

/*
 * Represents an offset range containing the requested point(s), with a
 * start and end bounds.
 */
typedef struct range {
    int64_t start;
    int64_t end;
} range_t;

// Initializes a index_t structure
int index_init(index_t *pi, const char *path, uint64_t base);

// Closes the index file associated with a index_t structure
int index_close(index_t *pi);

// Loads a index_t structure from disk
int index_load(index_t *pi, const char *path, uint64_t base);

// Appends an offset to the index file associated with a index_t
// structure
int index_append(index_t *pi, uint64_t ts, uint64_t offset);

// Finds the offset range for a given timestamp in the index file
int index_find(const index_t *pi, uint64_t ts, range_t *r);

// Prints information about a PersistentIndex structure
void index_print(const index_t *pi);

#endif
