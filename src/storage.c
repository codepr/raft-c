#include "storage.h"
#include "binary.h"
#include "darray.h"
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

int makedir(const char *path)
{
    struct stat st = {0};

    if (stat(path, &st) == -1) {
        return mkdir(path, 0700);
    }
    return 0;
}

int buffer_read_file(FILE *fp, buffer_t *b)
{
    /* Get the buffer size */
    if (fseek(fp, 0, SEEK_END) < 0) {
        log_error("Error reading file: fseek %s", strerror(errno));
        return -1;
    }

    size_t size = ftell(fp);

    /* Set position of stream to the beginning */
    rewind(fp);

    /* Allocate the buffer (no need to initialize it with calloc) */
    b->data = calloc(size + 1, sizeof(uint8_t));

    /* Read the file into the buffer */
    fread(b->data, 1, size, fp);

    b->size       = size;

    /* NULL-terminate the buffer */
    b->data[size] = '\0';
    return 0;
}

ssize_t read_file(FILE *fp, uint8_t *buf)
{
    /* Get the buffer size */
    if (fseek(fp, 0, SEEK_END) < 0) {
        perror("fseek");
        return -1;
    }

    ssize_t size = ftell(fp);

    /* Set position of stream to the beginning */
    rewind(fp);

    /* Read the file into the buffer */
    fread(buf, 1, size, fp);

    /* NULL-terminate the buffer */
    buf[size] = '\0';
    return size;
}

ssize_t filesize(FILE *fp, long offset)
{
    if (fseek(fp, 0, SEEK_END) < 0) {
        fclose(fp);
        return -1;
    }

    ssize_t size = ftell(fp);

    // reset to offset
    fseek(fp, offset, SEEK_SET);
    return size;
}

int file_open(void *context, const char *mode)
{
    file_context_t *fcontext = context;

    fcontext->fp             = fopen(fcontext->path, mode);
    if (!fcontext->fp)
        return -1;

    return 0;
}

int file_close(void *context)
{
    file_context_t *fcontext = context;
    if (!fcontext->fp)
        return -1;

    fclose(fcontext->fp);
    fcontext->fp = NULL;

    return 0;
}

int file_save_state(void *context, const raft_state_t *state)
{
    file_context_t *fcontext = context;
    if (!fcontext->fp)
        return -1;

    // TODO placeholder size
    uint8_t buf[BUFSIZ];
    uint8_t *ptr   = &buf[0];
    ssize_t length = write_i32(ptr, state->current_term);
    ptr += sizeof(int32_t);
    length += write_i32(ptr, state->voted_for);
    ptr += sizeof(int32_t);
    length += write_i32(ptr, state->log.length);
    ptr += sizeof(int32_t);
    for (int i = 0; i < state->log.length; ++i) {
        length += write_i32(ptr, state->log.items[i].term);
        ptr += sizeof(int32_t);
        length += write_i32(ptr, state->log.items[i].value);
        ptr += sizeof(int32_t);
    }
    fwrite(buf, length, 1, fcontext->fp);
    return 0;
}

int file_load_state(void *context, raft_state_t *state)
{
    file_context_t *fcontext = context;
    if (!fcontext->fp)
        return -1;

    // TODO placeholder size
    uint8_t buf[BUFSIZ];
    ssize_t n = fread(buf, sizeof(buf), 1, fcontext->fp);
    if (n <= 0)
        return -1;

    uint8_t *ptr        = &buf[0];
    state->current_term = read_i32(ptr);
    ptr += sizeof(int32_t);
    state->voted_for = read_i32(ptr);
    ptr += sizeof(int32_t);
    size_t record_count = read_i32(ptr);
    ptr += sizeof(int32_t);
    for (size_t i = 0; i < record_count; ++i) {
        log_entry_t entry;
        entry.term = read_i32(ptr);
        ptr += sizeof(int32_t);
        entry.value = read_i32(ptr);
        ptr += sizeof(int32_t);
        da_append(&state->log, entry);
    }

    return 0;
}
