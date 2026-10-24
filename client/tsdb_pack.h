/*
 * The packed body of an INSERT_VALUES frame (flags & TSDB_PACKED), shared by
 * tsdbd and the C client.
 *
 * Inserts are mostly regular clocks and sensor readings: timestamps that
 * advance by one step, values whose high bytes rarely change. A cheap reshape
 * makes them compress several times better than as raw arrays:
 *
 *   timestamps   zigzag deltas from the previous row (the first from 0), LEB128 varints
 *   values       byte planes: byte 0 of every value, then byte 1, ... (bool: one plane)
 *   valid        the bitmap, unchanged (with TSDB_VALID)
 *   series       zigzag deltas, LEB128 varints (with TSDB_MANY_SERIES)
 *
 * and the whole is one zstd frame. On the wire, after the 16-byte header:
 *
 *   u32 raw_len (bytes of the reshaped body), then the zstd frame
 */
#ifndef TSDB_PACK_H
#define TSDB_PACK_H

#include <stddef.h>
#include <stdint.h>

/* The largest reshaped body of n rows: what an encoder needs and a decoder accepts. */
size_t tsdb_pack_raw_bound(size_t n, unsigned type, int valid, int many);

/* Reshapes rows into out (tsdb_pack_raw_bound bytes); returns its length.
 * values in the wire layout of `type` (bool: one byte each). */
size_t tsdb_pack_reshape(size_t n, unsigned type, const int64_t *ts, const void *values, const uint8_t *valid,
                         const uint32_t *series, uint8_t *out);

/* Undoes tsdb_pack_reshape into the plain wire arrays (ts 8-aligned).
 * Returns 0, or -1 when the body does not hold n rows exactly. */
int tsdb_pack_unshape(const uint8_t *in, size_t len, size_t n, unsigned type, int valid, int many, int64_t *ts,
                      uint8_t *values, uint8_t *valid_out, uint32_t *series);

#endif
