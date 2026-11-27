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
int chunk_store_dirfd(const chunk_store *st);
const char *chunk_store_path(const chunk_store *st);

/* Opens a chunk for reading without requiring a specific vuuid (for per-store scrub, which walks names). Checks
 * header, size, sums table, and footer. */
int chunk_rd_open_any(chunk_store *st, uint32_t volume, const uint8_t oid[16], unsigned pos, chunk_rd **out);
/* Extended cell scrubber behind chunk_rd_scrub: verifies every cell's xxh64, accumulates *bad count and *bad_bitmap
 * (bit i for cell i < 63, bit 63 sticky for any cell >= 63), counts cell bytes read in *bytes_out, and invokes
 * optional on_cell(ctx, cell_bytes) after each cell (non-zero stops early and is returned). */
typedef int (*chunk_scrub_cell_cb)(void *ctx, size_t bytes);
int chunk_rd_scrub_ex(chunk_rd *r, uint64_t *bad, uint64_t *bad_bitmap, uint64_t *bytes_out,
                      chunk_scrub_cell_cb on_cell, void *ctx);

/* True while the volume's EPOCH lost a slot at open and has not been raised since (puts and deletes are refused). */
bool chunk_store_epoch_unsure(chunk_store *st, uint32_t volume);
/* A request showed epoch e (E14: every request raises the fence first). Not authoritative, unlike chunk_store_raise: an
 * unsure volume is left as it is and the answer is CHUNK_ERR_EPOCH_UNSURE (only the metadata group's epoch clears it,
 * D9); else EPOCH is at least e, durably, when it returns 0. */
int chunk_store_see(chunk_store *st, uint32_t volume, uint64_t e);
void chunk_store_test_fail(chunk_store *st);                    /* test hook: the store failed in service */
int chunk_store_test_flip(chunk_store *st, const chunk_ident *id); /* test hook: one byte of the first cell flipped */

/* A backend's lazy joinable I/O worker pool (chunkread.c), torn down in chunk_be_close. */
typedef struct chunk_rpool chunk_rpool;
void chunk_rpool_destroy(chunk_rpool *p);

/* A backend's stores (chunkstore.c) as the heal code (chunkheal.c) needs them. */
struct chunk_backend {
    atomic_int n;
    int cap;                     /* allocated slots (>= n; fixed across runtime store additions for lock-free reads) */
    bool read_only;              /* R15 D13: opened read-only (no flock, no EPOCH write, no put/del) */
    _Atomic(chunk_store *) *st;  /* NULL while DOWN; guarded by st_mu for swaps and ref acquisition */
    atomic_int *state;           /* CHUNK_STORE_* (an open store that failed in service reads as DOWN_IOERR) */
    atomic_bool *st_failed;      /* mirrors chunk_store_failed(st[i]) for lock-free chunk_be_store_state() */
    atomic_bool *st_lost;        /* R15 E15: true iff slot i was declared permanently LOST in TIERS */
    chunk_store **pending_close; /* stores detached by chunk_be_set_down: unreffed on attach worker or close, never on
                                  * health/monitor/reader threads where close() on a hung NFS mount could block */
    char **path;                 /* what each index was opened or attached at (for messages; guarded by st_mu) */
    chunk_store **retired;       /* stores replaced after they failed in service: kept until close, never freed
                                  * under a thread that may still hold a writer on one */
    int nretired;
    pthread_mutex_t mu;          /* the in-memory fence, and serializes install() */
    pthread_mutex_t st_mu;       /* pure leaf lock (never held across I/O; lock order: mu > st_mu): guards st[],
                                  * state[] transitions, path[], refcount acquisition, and on_ioerr */
    struct fence {
        uint32_t vol;
        uint64_t epoch;
    } *f;
    size_t nf, capf;
    _Atomic(chunk_rpool *) rpool; /* lazy joinable I/O worker pool for parallel chunk_obj reads (chunkread.c) */
    void (*on_ioerr)(void *ctx, int store); /* optional runtime I/O error hook (r15-health; guarded by st_mu) */
    void *ioerr_ctx;
    unsigned ioerr_active;       /* in-flight on_ioerr calls; drained when clearing the callback */
    pthread_cond_t ioerr_cv;
    atomic_uint put_max_pct;     /* R15 D7: >0 caps regular puts at this fs % (single-server tiers: 95; 0: off) */
};
/* R15 (r15-lost): LOST marking, open-as-REPLACING, runtime store additions, and pre-initialized replacement rebuild. */
void chunk_be_set_lost(chunk_backend *be, int i, bool lost);
bool chunk_be_store_is_lost(const chunk_backend *be, int i);
int chunk_be_mark_lost(chunk_backend *be, int i);
int chunk_be_set_replacing(chunk_backend *be, int i);
int chunk_be_add_store(chunk_backend *be, const char *path, const uint8_t *expect_id, const uint8_t *expect_db,
                       bool replacing);
int chunk_be_add_lost(chunk_backend *be, const char *path);
int chunk_be_fence_store(chunk_backend *be, int index);
int chunk_be_replace_open(chunk_backend *be, int index, const char *new_path, const uint8_t expect_id[16],
                          const uint8_t *expect_db, const chunk_heal_opts *o, chunk_heal_report *rep);
#ifdef TSDB_TEST_HOOKS
int chunk_be_promote_up_norebuild(chunk_backend *be, int index, const char *new_path, const uint8_t expect_id[16],
                                  const uint8_t *expect_db, const chunk_heal_opts *o);
#endif
int chunk_be_volumes_list(chunk_backend *be, uint32_t **out, size_t *nout);
/* Placement for an explicit store count `n`, and inference of an object's original store count from observed
 * (store, pos) chunks so objects written before `tier add` still map missing positions to their true stores. */
int chunk_be_place_n(const uint8_t oid[16], unsigned pos, int n);
int chunk_be_infer_place_n(const chunk_backend *be, const uint8_t oid[16], unsigned np, const int *obs_store,
                           const unsigned *obs_pos, size_t nobs, int target_store);
/* Reference counting on chunk_store: chunk_store_open returns 1 ref; chunk_store_close is chunk_store_unref. */
chunk_store *chunk_store_ref(chunk_store *st);
void chunk_store_unref(chunk_store *st);
/* Binds a freshly opened store to backend slot (be, idx) BEFORE publishing it into be->st[idx]. */
void chunk_store_bind_be(chunk_store *st, chunk_backend *be, int idx);
/* Acquires a reference to open store i under st_mu; caller must chunk_store_unref(). NULL when DOWN. */
chunk_store *chunk_be_store_ref(const chunk_backend *be, int i);
/* Acquires a reference to raw store i regardless of state under st_mu; caller must chunk_store_unref(). */
chunk_store *chunk_be_raw_store_ref(const chunk_backend *be, int i);
/* Releases any store parked on pending_close[i] by chunk_be_set_down (call on attach worker or close only). */
void chunk_be_release_down(chunk_backend *be, int i);
/* Reads chunk bytes and sets *real_io = true only on a real kernel storage I/O error (not truncated file or EINVAL). */
int chunk_rd_read_ex(chunk_rd *r, uint64_t off, void *buf, size_t len, bool *real_io);
/* Thread-safe copy of backend store i's current path under st_mu. Returns false when out of range or unset. */
bool chunk_be_store_path_copy(const chunk_backend *be, int i, char *buf, size_t cap);
/* Registers/clears the runtime I/O error callback (called outside any lock when a read hits TSDB_ERR_IO). */
void chunk_be_set_ioerr_cb(chunk_backend *be, void (*fn)(void *ctx, int store), void *ctx);
void chunk_be_note_ioerr(chunk_backend *be, int store);
/* Opens a chunk for reading and optionally reports which store index it was opened from (*store_out, -1 if none). */
int chunk_be_rd_open_at(chunk_backend *be, const chunk_ident *id, chunk_rd **out, int *store_out);
/* The store the position `pos` of the object is placed on: (chunk_be_place + pos) mod n. */
int chunk_be_pos_store(const chunk_backend *be, const uint8_t oid[16], unsigned pos);
/* The fence before a put, a delete or a heal at epoch e (refused below it, raised above it). */
int chunk_be_prepare(chunk_backend *be, uint32_t volume, uint64_t e);
/* The store i if it is open (UP or REPLACING), else NULL. */
chunk_store *chunk_be_open_store(const chunk_backend *be, int i);
/* The raw store pointer at index i regardless of state (NULL only if never opened); for replace/scrub checks. */
chunk_store *chunk_be_raw_store(const chunk_backend *be, int i);
/* Opens a store as chunk_store_open does, without the log lines of its refusals (the backend logs one line). */
int chunk_store_open_quiet(const char *path, const uint8_t *expect_id, chunk_store **out);
int chunk_store_open_db_quiet(const char *path, const uint8_t *expect_id, const uint8_t *expect_db, chunk_store **out);
int chunk_store_open_ro_quiet(const char *path, const uint8_t *expect_id, chunk_store **out);
/* A chunk's header without opening the whole chunk: any volume uuid (the caller learns it). */
int chunk_store_peek(chunk_store *st, uint32_t volume, const uint8_t oid[16], unsigned pos, chunk_hdr *h);
/* Names-only listing of one open store's volume directory via its open dirfd (no chunk files opened or read). */
int chunk_store_list_names(chunk_store *st, uint32_t volume, chunk_list_fn fn, void *ctx);
/* chunk_wr_open for a rebuilt chunk: the header carries hdr_epoch (the epoch the object's chunks were written at, which
 * may be below the volume's), the fence is checked and raised at fence_epoch (the healer's). */
int chunk_wr_open_heal(chunk_store *st, const chunk_ident *id, const chunk_geom *g, uint64_t hdr_epoch,
                       uint64_t fence_epoch, chunk_wr **out);
/* R12 (E14, §5.9): backend-level replace-put for chunk repair. Prefers the store already holding (volume, oid, pos)
 * (even if corrupt) so the atomic rename replaces it in place, falling back to chunk_be_place. Writes hdr_epoch under
 * fence_epoch (bypassing the 95% regular-put cap) and returns a ref-counted chunk_store in *st_out for read-back. */
int chunk_be_wr_open_replace(chunk_backend *be, const chunk_ident *id, const chunk_geom *g, uint64_t hdr_epoch,
                             uint64_t fence_epoch, chunk_wr **out, chunk_store **st_out);
/* R15 r15-full: configure the single-server tier regular-put fs cap (0 = off; 95 = CHUNK_STORE_PUT_MAX_PCT). */
void chunk_be_set_put_cap(chunk_backend *be, unsigned max_pct);
/* R15 r15-full: true if be has a put cap and any open store exceeds it (sampled at most 1/s per store). */
bool chunk_be_any_store_full(chunk_backend *be);
/* R15 r15-full: refresh store filesystem byte counts via fstatvfs (<= 1/s; called from probe/put paths, never /metrics). */
void chunk_store_fs_refresh(chunk_store *st);
/* R15 r15-full: lock-free, syscall-free read of cached store filesystem byte counts (for /metrics). 0 or -1. */
int chunk_store_fs_bytes_cached(chunk_store *st, uint64_t *used_out, uint64_t *free_out, uint64_t *total_out);
/* Encodes stripe s of the object: `stripe` holds its data bytes (min(k * cell, len - s * k * cell) of them); the cells
 * of every position go to fn as chunk_encode gives them (idx = s). */
int chunk_encode_stripe(const chunk_geom *g, uint64_t s, const uint8_t *stripe, chunk_cell_fn fn, void *ctx);

/* What heal needs of an open object (chunkread.c): a position's own sums table (NULL when its chunk is not open), its
 * status (CHUNK_ST_OK when open, else why not) and the highest writer epoch among the open chunks. */
const uint64_t *chunk_obj_pos_sums(const chunk_obj *o, unsigned pos);
int chunk_obj_pos_status(const chunk_obj *o, unsigned pos);
uint64_t chunk_obj_epoch(const chunk_obj *o);
int chunk_obj_place_n(const chunk_obj *o);

#endif
