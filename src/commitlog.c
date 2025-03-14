#include "commitlog.h"
#include "binary.h"
#include "buffer.h"
#include "logger.h"
#include "storage.h"
#include "timeseries.h"
#include <inttypes.h>
#include <unistd.h>

int cl_init(commitlog_t *cl, const char *path, uint64_t base)
{
    char path_buf[PATHBUF_SIZE];
    snprintf(path_buf, sizeof(path_buf), "%s/c-%.20" PRIu64 ".log", path, base);

    cl->fp = fopen(path_buf, "w+");
    if (!cl->fp)
        return -1;

    cl->base_timestamp    = base;
    cl->base_ns           = 0;
    cl->current_timestamp = base;
    cl->size              = 0;

    return 0;
}

void cl_set_base_ns(commitlog_t *cl, uint64_t ns) { cl->base_ns = ns; }

int cl_load(commitlog_t *cl, const char *path, uint64_t base)
{
    char path_buf[PATHBUF_SIZE];
    snprintf(path_buf, sizeof(path_buf), "%s/c-%.20" PRIu64 ".log", path, base);

    cl->fp = fopen(path_buf, "w+");
    if (!cl->fp)
        return -1;

    cl->base_timestamp   = base;

    uint64_t record_size = 0;

    buffer_t buffer;
    if (buffer_read_file(cl->fp, &buffer) < 0)
        return -1;

    size_t size  = buffer.size;
    uint8_t *buf = buffer.data;
    cl->size     = buffer.size;

    if (size > 0) {
        while (size > 0) {
            record_size = read_i64(buf);
            size -= record_size;
            buf += record_size;
        }

        uint64_t first_ts     = ts_record_timestamp(buffer.data);
        uint64_t latest_ts    = ts_record_timestamp(buf - record_size);

        cl->current_timestamp = latest_ts;
        cl->base_ns           = first_ts % (uint64_t)1e9;
    }

    free(buffer.data);

    return 0;
}

int cl_append_data(commitlog_t *cl, const uint8_t *data, size_t len)
{
    int bytes = pwrite(fileno(cl->fp), data, len, cl->size);
    if (bytes < 0) {
        perror("write_at");
        return -1;
    }

    cl->size += bytes;
    cl->current_timestamp = ts_record_timestamp(data);

    return 0;
}

int cl_append_batch(commitlog_t *cl, const uint8_t *batch, size_t len)
{
    cl->current_timestamp = ts_record_timestamp(batch);
    size_t start_offset   = sizeof(uint64_t) * 2;

    // If not set before, set the base nanoseconds from the first timestamp of
    // the batch, which is located at the first record of the batch, after the
    // batch size and last timestamp of the batch
    if (cl->base_ns == 0) {
        uint64_t first_timestamp = ts_record_timestamp(batch + start_offset);
        cl->base_ns              = first_timestamp % (uint64_t)1e9;
    }

    int n = pwrite(fileno(cl->fp), batch + start_offset, len, cl->size);
    if (n < 0) {
        perror("write_at");
        return -1;
    }

    cl->size += len;

    return 0;
}

int cl_read_at(const commitlog_t *cl, uint8_t **buf, size_t offset, size_t len)
{
    return pread(fileno(cl->fp), *buf, len, offset);
}

void cl_print(const commitlog_t *cl)
{
    if (cl->size == 0)
        return;
    uint8_t buf[4096];
    uint8_t *p     = &buf[0];
    ssize_t read   = 0;
    uint64_t ts    = 0;
    double_t value = 0.0;
    ssize_t len    = read_file(cl->fp, buf);
    while (read < len) {
        ts    = read_i64(p + sizeof(uint64_t));
        value = read_f64(p + sizeof(uint64_t) * 2);
        read += sizeof(uint64_t) * 2 + sizeof(double_t);
        p += sizeof(uint64_t) * 2 + sizeof(double_t);
        log_info("%" PRIu64 "-> %.02f", ts, value);
    }
}
