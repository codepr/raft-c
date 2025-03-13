#include "wal.h"
#include "binary.h"
#include "logger.h"
#include "storage.h"
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define WAL_RECORDSIZE sizeof(uint64_t) + sizeof(double_t)

static const char t[2] = {'t', 'h'};

int wal_init(wal_t *w, const char *path, uint64_t base_timestamp, int main)
{
    snprintf(w->path, sizeof(w->path), "%s/wal-%c-%.20" PRIu64 ".log", path,
             t[main], base_timestamp);
    w->fp = fopen(w->path, "w+");
    if (!w->fp)
        goto errdefer;

    w->size = 0;

    return 0;

errdefer:
    log_error("WAL init %s: %s", path, strerror(errno));
    return -1;
}

int wal_delete(wal_t *w)
{
    if (!w->fp)
        return -1;
    int err = fclose(w->fp);
    if (err < 0)
        return -1;
    w->size = 0;
    char tmp[WAL_PATHSIZE + 5];
    snprintf(tmp, sizeof(tmp), "%s.log", w->path);

    return remove(tmp);
}

int wal_load(wal_t *w, const char *path, uint64_t base_timestamp, int main)
{
    char path_buf[PATHBUF_SIZE];
    snprintf(path_buf, sizeof(path_buf), "%s/wal-%c-%.20" PRIu64 ".log", path,
             t[main], base_timestamp);
    w->fp = fopen(path_buf, "w+");
    if (!w->fp)
        goto errdefer;

    w->size = filesize(w->fp, 0);

    log_debug("Successfully loaded WAL %s (%ld)", path_buf, w->size);

    return 0;

errdefer:
    log_error("WAL from disk %s: %s", path, strerror(errno));
    return -1;
}

int wal_append(wal_t *wal, uint64_t ts, double_t value)
{
    uint8_t buf[WAL_RECORDSIZE];

    write_i64(buf, ts);
    write_f64(buf + sizeof(uint64_t), value);

    // TODO Fix to handle multiple points in the same timestamp
    if (pwrite(fileno(wal->fp), buf, WAL_RECORDSIZE, wal->size) < 0)
        return -1;

    wal->size += WAL_RECORDSIZE;
    return 0;
}

size_t wal_size(const wal_t *wal) { return wal->size; }
