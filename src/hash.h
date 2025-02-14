#ifndef HASH_H
#define HASH_H

#include <stdint.h>
#include <stdlib.h>

#define SHA256_SIZE 32

uint32_t simple_hash(const uint8_t *in);
uint32_t murmur3_hash(const uint8_t *in, uint32_t seed);
int32_t sha256_hash(const uint8_t *in, size_t len, uint8_t out[SHA256_SIZE]);

#endif
