/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Local chunk moves between two stores of one backend (R13, r13-lmove; docs/design-cluster-data.md E50, §15.3,
 * PLAN.md D18).
 *
 * Moves one chunk `(volume, oid, pos)` from store `from` to store `to` on the same server without network traffic and
 * without any window with zero durable copies:
 *   1. Raises the volume's durable EPOCH fence on `to` first (at least the volume's current fence and the chunk's own
 *      writer epoch).
 *   2. Copies the chunk into a new temporary file (`tmp_*`) on `to` while verifying every cell against its encode-time
 *      sum and pacing at `rate_bps` (default 50 MiB/s; cluster `balance_mb_s` when configured).
 *   3. Writes sums + footer and fsyncs the temporary file on `to`.
 *   4. Under the backend's per-(volume, oid) lock (serializing against concurrent MK_GC deletes and r12-repair
 *      replace-puts): checks that the source file still exists with the same inode, atomically renames `tmp_*` into
 *      place on `to` and fsyncs `to`'s volume directory, reads back and verifies every cell on `to` against the sums,
 *      updates the backend's local location index, then unlinks the source on `from` and fsyncs `from`'s directory.
 */
#ifndef TSDB_CHUNKMOVE_H
#define TSDB_CHUNKMOVE_H

#include "chunk.h"

#include <stdbool.h>
#include <stdint.h>

#define CHUNKMOVE_DEFAULT_MB_S 50u

/* Persistent virtual-time token bucket shared across a sequence of chunk_move calls (e.g. the drain driver). */
typedef struct chunk_move_pacer {
    uint64_t rate_bps; /* 0: CHUNKMOVE_DEFAULT_MB_S MiB/s; UINT64_MAX: unpaced */
    uint64_t next_ns;
    bool init;
} chunk_move_pacer;

void chunk_move_pacer_init(chunk_move_pacer *p, uint64_t rate_bps);
void chunk_move_pacer_set_rate(chunk_move_pacer *p, uint64_t rate_bps);

typedef struct chunk_move_opts {
    uint64_t rate_bps;        /* 0: default (50 MiB/s); UINT64_MAX: unpaced (ignored when `pacer` != NULL) */
    chunk_move_pacer *pacer;  /* optional shared pacer across multiple moves */
    int (*cancel)(void *ctx); /* optional: non-zero aborts copy promptly with TSDB_ERR_STOPPED */
    void *cancel_ctx;
    void (*pace)(void *ctx, uint64_t bytes); /* optional progress callback per cell copied */
    void *pace_ctx;
    uint64_t *bytes_moved; /* optional: set to total chunk file bytes on success */
} chunk_move_opts;

/* Moves `id` (if `id->vuuid` is all zeros, accepts any vuuid and preserves the source header's vuuid) from store
 * `from` to store `to` within `be`. Returns 0 on success, TSDB_ERR_NOTFOUND if source is absent or deleted concurrently,
 * TSDB_ERR_STOPPED if cancelled, CHUNK_ERR_FULL on ENOSPC, CHUNK_ERR_EPOCH_UNSURE if either store's volume EPOCH is
 * unsure, TSDB_ERR_CORRUPT if source is damaged, or another negative TSDB_ERR_* / CHUNK_ERR_*. */
int chunk_move(chunk_backend *be, const chunk_ident *id, int from, int to, const chunk_move_opts *o);

#ifdef TSDB_TEST_HOOKS
enum {
    CHUNKMOVE_MUT_NONE = 0,
    CHUNKMOVE_MUT_UNLINKFIRST = 1, /* moveunlinkfirst: unlinks source before target tmp is synced and renamed */
    CHUNKMOVE_MUT_KEEPWRONG = 2,   /* movekeepwrong: start-time dedupe keeps the lower-preference (fast-disk) copy */
};
extern int chunkmove_mutant;
int chunkmove_mutant_parse(const char *name);

/* Optional deterministic race hook invoked right after target tmp fsync and before acquiring per-oid lock. */
typedef void (*chunkmove_hook_fn)(void *ctx);
void chunkmove_set_before_lock_hook(chunkmove_hook_fn fn, void *ctx);
#endif

#endif /* TSDB_CHUNKMOVE_H */
