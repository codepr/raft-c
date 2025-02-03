#include "storage.h"
#include "binary.h"
#include "darray.h"
#include <stdio.h>

int file_save_state(const raft_state_t *state)
{
    // TODO move hardcoded filepath out
    FILE *file = fopen("raft_state.bin", "wb");
    if (!file)
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
    fwrite(buf, length, 1, file);
    fclose(file);
    return 0;
}

int file_load_state(raft_state_t *state)
{
    // TODO move hardcoded filepath out
    FILE *file = fopen("raft_state.bin", "rb");
    if (!file)
        return -1;

    // TODO placeholder size
    uint8_t buf[BUFSIZ];
    fread(buf, sizeof(buf), 1, file);
    fclose(file);
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
