#ifndef BINARY_H
#define BINARY_H

#include <math.h>
#include <stdint.h>

int write_u8(uint8_t *buf, uint8_t val);
uint8_t read_u8(const uint8_t *const buf);
int write_u32(uint8_t *buf, uint32_t val);
uint32_t read_u32(const uint8_t *const buf);
int write_i32(uint8_t *buf, int32_t val);
int32_t read_i32(const uint8_t *buf);
int write_i64(uint8_t *buf, int64_t val);
int64_t read_i64(const uint8_t *buf);
int write_f64(uint8_t *buf, double_t val);
double_t read_f64(const uint8_t *buf);

#endif
