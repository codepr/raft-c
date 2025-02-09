#include "storage.h"
#include "binary.h"
#include "darray.h"
#include <stdio.h>

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
