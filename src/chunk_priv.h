/* SPDX-License-Identifier: AGPL-3.0-only */
/* What the files of the chunk layer (chunk.c, chunkstore.c, chunkread.c) share besides chunk.h. */
#ifndef TSDB_CHUNK_PRIV_H
#define TSDB_CHUNK_PRIV_H

#include "chunk.h"

uint64_t chunk_le64(const uint8_t *p);
void chunk_put_le64(uint8_t *p, uint64_t v);
/* The footer of a chunk file for this header image and sums table (little endian), and its check. */
void chunk_ftr_make(uint8_t ftr[CHUNK_FTR_BYTES], const uint8_t *hdr, const uint8_t *table, uint64_t ncells);
int chunk_ftr_check(const uint8_t *hdr, const uint8_t *table, uint64_t ncells, const uint8_t ftr[CHUNK_FTR_BYTES]);
/* The length of cell i of chunk pos. */
uint64_t chunk_pos_cell_len(const chunk_geom *g, unsigned pos, uint64_t i);
/* The codec for (k, m), built once (an iso_rs *); NULL without an erasure codec or on a failed allocation. */
void *chunk_rs_get(unsigned k, unsigned m);
/* A cell whose sum the caller computed from this very buffer (the encoder's): not hashed again. */
int chunk_wr_cell_trusted(chunk_wr *w, const void *data, size_t len, uint64_t sum);
/* The volumes a store has an EPOCH for (malloc'ed, caller frees), with their epochs. */
int chunk_store_volumes(chunk_store *st, uint32_t **vols, uint64_t **epochs, size_t *n);

#endif
