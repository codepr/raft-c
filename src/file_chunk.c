#include "file_chunk.h"
#include <string.h>

int file_chunk_buffered_read(const char *path, buffered_chunk_t *file)
{
    FILE *fp = fopen(path, "rb");
    if (!fp) {
        perror("Failed to open file");
        return -1;
    }

    fseek(fp, 0, SEEK_END);
    file->chunk.size = ftell(fp);
    rewind(fp);

    file->chunk.data = malloc(file->chunk.size);
    if (!file->chunk.data) {
        perror("Memory allocation failed");
        fclose(fp);
        return -1;
    }

    fread(file->chunk.data, file->chunk.size, 1, fp);
    fclose(fp);

    strncpy(file->chunk.name, path, sizeof(file->chunk.name));

    return 1;
}

int file_chunk_buffered_write(buffered_chunk_t *file)
{
    FILE *fp = fopen(file->chunk.name, "wb");
    if (!fp) {
        perror("Failed to open file");
        return -1;
    }

    fwrite(file->chunk.data, file->chunk.size, 1, fp);
    fclose(fp);

    return 1;
}

void file_chunk_buffered_close(buffered_chunk_t *file)
{
    free(file->chunk.data);
    file->chunk.data = NULL;
}
