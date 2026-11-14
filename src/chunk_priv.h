/* SPDX-License-Identifier: AGPL-3.0-only */
/* What the files of the chunk layer (chunk.c, chunkstore.c, chunkread.c) share besides chunk.h. */
#ifndef TSDB_CHUNK_PRIV_H
#define TSDB_CHUNK_PRIV_H

#include "chunk.h"

#include <pthread.h>
#include <stdatomic.h>

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

/* True while the volume's EPOCH lost a slot at open and has not been raised since (puts and deletes are refused). */
bool chunk_store_epoch_unsure(chunk_store *st, uint32_t volume);

/* A backend's stores (chunkstore.c) as the heal code (chunkheal.c) needs them. */
struct chunk_backend {
    int n;
    _Atomic(chunk_store *) *st;  /* NULL while DOWN; set under `mu` by attach and replace, read without a lock */
    atomic_int *state;           /* CHUNK_STORE_* (an open store that failed in service reads as DOWN_IOERR) */
    char **path;                 /* what each index was opened or attached at (for messages) */
    chunk_store **retired;       /* stores replaced after they failed in service: kept open until close, never freed
                                  * under a thread that may still hold a writer on one */
    int nretired;
    pthread_mutex_t mu;          /* the in-memory fence, and the swaps of st[] */
    struct fence {
        uint32_t vol;
        uint64_t epoch;
    } *f;
    size_t nf, capf;
};
/* The store the position `pos` of the object is placed on: (chunk_be_place + pos) mod n. */
int chunk_be_pos_store(const chunk_backend *be, const uint8_t oid[16], unsigned pos);
/* The fence before a put, a delete or a heal at epoch e (refused below it, raised above it). */
int chunk_be_prepare(chunk_backend *be, uint32_t volume, uint64_t e);
/* The store i if it is open (UP or REPLACING), else NULL. */
chunk_store *chunk_be_open_store(const chunk_backend *be, int i);
/* Opens a store as chunk_store_open does, without the log lines of its refusals (the backend logs one line). */
int chunk_store_open_quiet(const char *path, const uint8_t *expect_id, chunk_store **out);
/* A chunk's header without opening the whole chunk: any volume uuid (the caller learns it). */
int chunk_store_peek(chunk_store *st, uint32_t volume, const uint8_t oid[16], unsigned pos, chunk_hdr *h);
/* chunk_wr_open for a rebuilt chunk: the header carries hdr_epoch (the epoch the object's chunks were written at, which
 * may be below the volume's), the fence is checked and raised at fence_epoch (the healer's). */
int chunk_wr_open_heal(chunk_store *st, const chunk_ident *id, const chunk_geom *g, uint64_t hdr_epoch,
                       uint64_t fence_epoch, chunk_wr **out);
/* Encodes stripe s of the object: `stripe` holds its data bytes (min(k * cell, len - s * k * cell) of them); the cells
 * of every position go to fn as chunk_encode gives them (idx = s). */
int chunk_encode_stripe(const chunk_geom *g, uint64_t s, const uint8_t *stripe, chunk_cell_fn fn, void *ctx);

/* What heal needs of an open object (chunkread.c): a position's own sums table (NULL when its chunk is not open), its
 * status (CHUNK_ST_OK when open, else why not) and the highest writer epoch among the open chunks. */
const uint64_t *chunk_obj_pos_sums(const chunk_obj *o, unsigned pos);
int chunk_obj_pos_status(const chunk_obj *o, unsigned pos);
uint64_t chunk_obj_epoch(const chunk_obj *o);

#endif
