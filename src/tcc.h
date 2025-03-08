#ifndef TCC_H
#define TCC_H

// TCP connection context header

#include <stdint.h>
#include <stdlib.h>

typedef struct buffer buffer_t;

typedef struct tcc {
    int fd;              // Socket file descriptor
    int error_code;      // Tracks if any error occurred during sending
    size_t records_sent; // Counter for sent records
    size_t batch_size;   // Number of records to batch before flushing
    buffer_t *buffer;    // Input/Output buffer
} tcc_t;

int tcc_read_buffer(tcc_t *ctx);
int tcc_flush_buffer(tcc_t *ctx);

#endif
