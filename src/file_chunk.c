#include "file_chunk.h"
#include "darray.h"
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

int file_chunk_split_file(const char name[FILENAME_SIZE], size_t size,
                          file_chunk_array_t *array)
{
    FILE *fp = fopen(name, "rb");
    if (!fp) {
        perror("Failed to open file");
        return -1;
    }

    int i              = 0;
    ssize_t bytes      = 0;
    file_chunk_t chunk = {0};

    while ((bytes = fread(chunk.data, size, 1, fp)) != EOF) {
        snprintf(chunk.name, FILENAME_SIZE, "%s-%i", name, i);
        i++;
        chunk.size = bytes;
        da_append(array, chunk);
        memset(&chunk, 0x00, sizeof(chunk));
    }

    return 1;
}
