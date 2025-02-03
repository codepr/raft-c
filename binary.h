#ifndef BINARY_H
#define BINARY_H

#include <stdint.h>

int write_u8(uint8_t *buf, uint8_t val);
uint8_t read_u8(const uint8_t *const buf);
int write_u32(uint8_t *buf, uint32_t val);
uint32_t read_u32(const uint8_t *const buf);
int write_i32(uint8_t *buf, int32_t val);
int32_t read_i32(const uint8_t *buf);

#endif
