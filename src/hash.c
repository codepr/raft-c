#define LONESHA256_STATIC
#include "hash.h"
#include "lonesha256.h"

uint32_t simple_hash(const uint8_t *in)
{
    unsigned int h = 0;
    while (*in)
        h = (h * 31) + *(in++);
    return h;
}

uint32_t murmur3_hash(const uint8_t *in, uint32_t seed)
{
    uint32_t h = seed;
    while (*in) {
        h ^= (uint8_t)(*in++);
        h *= 0x5bd1e995;
        h ^= h >> 15;
    }
    h ^= h >> 13;
    h *= 0x5bd1e995;
    h ^= h >> 15;
    return h;
}

int32_t sha256_hash(const uint8_t *in, size_t len, uint8_t out[SHA256_SIZE])
{
    return lonesha256(out, in, len);
}
