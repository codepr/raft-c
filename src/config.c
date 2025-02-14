#include "config.h"
#include "hash.h"
#include "logger.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BUCKET_SIZE       128

// Default config table
#define ID                "0"
#define TYPE              "shard"
#define HOST              "127.0.0.1:18777"
#define SHARD_LEADERS     "127.0.0.1:8777 127.0.0.1:8877 127.0.0.1:8977"
#define RAFT_REPLICAS     "127.0.0.1:9777 127.0.0.1:9778"
#define RAFT_HEARTBEAT_MS "150"

static config_entry_t *config_map[BUCKET_SIZE] = {0};

// Simple hash function for string keys
static unsigned int hash(const char *key)
{
    return simple_hash((const uint8_t *)key) % BUCKET_SIZE;
}

void config_set(const char *key, const char *value)
{
    unsigned index        = hash(key);
    config_entry_t *entry = calloc(1, sizeof(*entry));
    if (!entry)
        return;

    strncpy(entry->key, key, MAX_KEY_SIZE);
    strncpy(entry->value, value, MAX_VALUE_SIZE);

    if (config_map[index] &&
        strncmp(config_map[index]->key, key, MAX_KEY_SIZE) == 0) {
        free(config_map[index]);
        config_map[index] = entry;
    } else {
        entry->next       = config_map[index];
        config_map[index] = entry;
    }
}

void config_set_default(void)
{
    config_set("id", ID);
    config_set("type", TYPE);
    config_set("host", HOST);
    config_set("shard_leaders", SHARD_LEADERS);
    config_set("raft_replicas", RAFT_REPLICAS);
    config_set("raft_heartbeat_ms", RAFT_HEARTBEAT_MS);
}

const char *config_get(const char *key)
{
    unsigned index        = hash(key);
    config_entry_t *entry = config_map[index];

    while (entry) {
        if (strncmp(entry->key, key, MAX_KEY_SIZE) == 0)
            return entry->value;
        entry = entry->next;
    }

    return NULL;
}

int config_get_int(const char *key)
{
    const char *value = config_get(key);
    if (!value)
        return -1;

    return atoi(value);
}

int config_get_list(const char *key, char out[MAX_LIST_SIZE][MAX_VALUE_SIZE])
{
    const char *list = config_get(key);
    if (!list)
        return -1;

    int count = 0;
    char item[MAX_VALUE_SIZE];
    strncpy(item, list, MAX_VALUE_SIZE);

    for (char *token = strtok(item, " "); token && count < MAX_LIST_SIZE;
         token       = strtok(NULL, " "), count++) {
        strncpy(out[count], token, MAX_VALUE_SIZE);
    }

    return count;
}

int config_get_enum(const char *key)
{
    const char *value = config_get(key);
    if (!value)
        return -1;

    if (strncasecmp(value, "shard", MAX_VALUE_SIZE) == 0)
        return NT_SHARD;
    if (strncasecmp(value, "replica", MAX_VALUE_SIZE) == 0)
        return NT_REPLICA;
    return -1;
}

static int scan_delim(const char *ptr, char *buf, char delim)
{
    int size = 0;
    while (*ptr != delim && *ptr != '\0') {
        *buf++ = *ptr++;
        ++size;
    }

    return size;
}

int config_load(const char *filepath)
{
    int off     = 0;
    int line_nr = 0;
    FILE *fp    = fopen(filepath, "r");
    if (!fp)
        return -1;

    char line[MAX_LINE_SIZE] = {0};

    while (fgets(line, sizeof(line), fp)) {
        line_nr++;
        char *ptr = line;

        // Skip spaces and comments
        while (isspace(*ptr))
            ptr++;
        if (*ptr == '#' || *ptr == '\0')
            continue;

        char key[MAX_KEY_SIZE]     = {0};
        char value[MAX_VALUE_SIZE] = {0};
        off                        = scan_delim(ptr, key, ' ');

        ptr += off;

        // Skip spaces and comments
        while (isspace(*ptr))
            ptr++;

        off = scan_delim(ptr, value, '\n');

        if (off != 0)
            config_set(key, value);
        else
            log_error("Error reading config at line %d", line_nr);
    }

    fclose(fp);
    return 0;
}

void config_print(void)
{
    for (int i = 0; i < BUCKET_SIZE; ++i) {
        for (config_entry_t *entry = config_map[i]; entry;
             entry                 = entry->next) {
            log_info("\t%s %s", entry->key, entry->value);
        }
    }
}

void config_free()
{
    for (int i = 0; i < BUCKET_SIZE; i++) {
        config_entry_t *entry = config_map[i];
        while (entry) {
            config_entry_t *temp = entry;
            entry                = entry->next;
            free(temp);
        }
        config_map[i] = NULL;
    }
}
