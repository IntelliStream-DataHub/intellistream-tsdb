/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The cold operations of one volume (R8, design-cluster-data.md E22, §5.7): everything the engine does to the objects
 * of its settled parts, behind one table of function pointers and a context, so that the engine never names a chunk
 * store, a store index or a backend. settle.c puts, eccache.c and eckeep.c open and read, ecgc.c deletes and lists,
 * tier.c raises the epoch; each through the ops of the volume it works for.
 *
 * The ops and their context are per attached engine (per volume), not per tier: on a single server every volume gets
 * the directory ops (src/coldops_dir.c) over the tier's stores, as R7 did; in a cluster each volume has its own stripe
 * group and r8f gives it ops over the peers (server/chunkpeer.c). tier_engine_attach makes the context through the
 * attach's `make` once META's vuuid is known, and the tier holds it in a reference-counted `coldvol` (src/tier.h):
 * the engine holds one reference and the tier's registry of volumes one (what the cache's readers, the gc worker and
 * tier_epoch look up by volume number; a reopen's attach replaces the entry, and the tier keeps it until it closes, so
 * a delete queued by an engine that closed since still finds its volume's ops; a queued delete holds no reference, and
 * an attach that fails replaces nothing). So the context, and the epoch provider's arg, outlive the engine: they live
 * until the volume's next successful attach or tier_close; the ops table outlives the tier. The last reference frees
 * the context (ctx_free); lookups hold one while they call an op. An object handle (cold_obj) never uses the context
 * after obj_open returned: the cache closes objects long after the engine that opened them closed, so a handle holds
 * what it reads through.
 *
 * Codes are the engine's (TSDB_ERR_*, or the CHUNK_ERR_* of src/chunk.h, which callers map); every op may be called
 * from any thread, concurrently, except ctx_free.
 */
#ifndef TSDB_COLDOPS_H
#define TSDB_COLDOPS_H

#include "chunk.h"

#include <stdbool.h>
#include <stdint.h>

typedef struct cold_obj cold_obj; /* an object open for reading: the ops' own type */

/* Why an object's chunks are deleted (for the ops' log lines and, in a cluster, their refusal for direct deletes). */
enum {
    COLD_DEL_GC = 1,    /* a retired .ec part's object (R7 single server) */
    COLD_DEL_SWEEP = 2, /* the open's sweep of orphans (R7 single server) */
    COLD_DEL_MKGC = 3,  /* a committed MK_GC marker (R8 cluster volume, r8k-gc) */
};

/* Candidate former stripe holder (R9 D12) or retired stripe position (R10) eligible for MK_GC_RELEASE. */
typedef struct cold_release {
    uint32_t pos;
    uint32_t node;
    uint64_t version;
    uint8_t retired; /* R10: 1 when releasing a retired stripe position (pos >= active, retired_ver > 0) */
} cold_release;

/* Live object descriptor passed by ecgcmk_work to release_verify (R9 D12). */
typedef struct cold_live_obj {
    uint8_t oid[16];
    unsigned npos;
    uint32_t positions;
    uint64_t stripe_version;
} cold_live_obj;

typedef struct cold_ops {
    const char *name; /* "dir", "peer": for messages */
    /* Encodes the object from src in one pass and puts every position (E26): per-cell sums into sums_out
     * (chunk_sums_total entries), every chunk header at `epoch`. Nothing committed on a failure before the last stripe
     * (positions committed before a failure while committing are orphans, E14's business). */
    int (*put_stream)(void *ctx, const uint8_t oid[16], const chunk_geom *g, uint64_t epoch, const chunk_put_src *src,
                      uint64_t *sums_out);
    /* An object for reading (chunk_obj_open's contract: TSDB_ERR_NOTFOUND only when every position is reachable and
     * none has a chunk; TSDB_ERR_UNAVAILABLE while one is away). */
    int (*obj_open)(void *ctx, const uint8_t oid[16], const chunk_geom *expect, const uint64_t *rec_sums,
                    cold_obj **out);
    int (*obj_read)(cold_obj *o, uint64_t off, uint64_t len, void *buf, unsigned flags, chunk_report *rep);
    const chunk_geom *(*obj_geom)(const cold_obj *o);
    void (*obj_close)(cold_obj *o);
    /* Deletes chunk `pos` of the object at `epoch` wherever it is (TSDB_ERR_NOTFOUND: none; a position that is away
     * keeps its copy: ask writable after). The chunk's vuuid is the context's: a volume's never changes under a
     * tier. */
    int (*del)(void *ctx, const uint8_t oid[16], unsigned pos, uint64_t epoch, int reason);
    /* The volume's chunks by name. chunk_listed.store is the ops' token for where the chunk is, meaningful only to
     * `owner`; size, epoch and vuuid are 0. A listing cut short is an error, never the end. */
    int (*list_names)(void *ctx, chunk_list_fn fn, void *fctx);
    /* The volume uuid in the header of a chunk list_names gave: 0 and vuuid, or an error (no header read). */
    int (*owner)(void *ctx, const chunk_listed *c, uint8_t vuuid[16]);
    /* The authoritative epoch (from the attach's epoch provider, never the writer's own): durable in every position
     * that is up before it returns; it clears an unsure EPOCH. Non-zero when no position holds it. Called by
     * tier_engine_attach only: whoever moves a volume's provider while it runs (r8f) calls it again (src/tier.h). */
    int (*see_epoch)(void *ctx, uint64_t e);
    uint64_t (*epoch)(void *ctx); /* the fence the positions hold now (0: none) */
    /* Whether puts and deletes may go on (every position up and none failed in service); false is sticky until the
     * next open of the handle. */
    bool (*writable)(void *ctx);
    /* Every position up now (eckeep's digest waits otherwise: a REPLACING store lacks chunks it will have). */
    bool (*all_up)(void *ctx);
    void (*ctx_free)(void *ctx);
    /* R8 (r8g-cut, design-cluster-data.md 2.3 step 3): the version of the stripe list the positions are placed by now;
     * a leader's settle puts under one and carries it in MK_COLD only while it is still the copy's. NULL: 0 (the
     * directory ops, one server: the list never changes). */
    uint64_t (*stripe_version)(void *ctx);
    /* R8 (r8k-gc, E14, D13): delete position `pos` of `oid` at `epoch` under a committed MK_GC at log position `mk_pos`
     * (must be > 0). Returns 0, TSDB_ERR_NOTFOUND (ABSENT counts as gone), CHUNK_ERR_FENCED (caller stops GC), or
     * another error (kept for retry). NULL: fall back to `del(..., COLD_DEL_MKGC)`. */
    int (*del_gc)(void *ctx, const uint8_t oid[16], unsigned pos, uint64_t epoch, uint64_t mk_pos);
    /* R8 (r8k-gc, E14): whether orphan collection is suspended for this volume (`gc_suspended` in the metadata copy:
     * set by force-leader / restore until `volume gc resume`). NULL: false. */
    bool (*gc_suspended)(void *ctx);
    /* R8 (r8k-gc): number of positions in the volume's stripe group (0 if unknown). NULL: use settings k + m. */
    unsigned (*nstripe)(void *ctx);
    /* R8 (r8k-gc): read a listed chunk's vuuid and writer epoch when list_names does not fill them (directory ops). */
    int (*chunk_info)(void *ctx, const chunk_listed *c, uint8_t vuuid[16], uint64_t *epoch);
    /* R9 (r9-degsettle, E10): put only positions whose bit is set in `posmask` (0 = all k'+m); sets *written_mask_out
     * (when non-NULL) to the bitmap actually written. NULL: fall back to put_stream (full only). */
    int (*put_stream_pos)(void *ctx, const uint8_t oid[16], const chunk_geom *g, uint64_t epoch,
                          const chunk_put_src *src, uint32_t posmask, uint32_t *written_mask_out, uint64_t *sums_out);
    /* R9 (r9-degsettle, E10): obj_open with a known written-positions bitmap (0 = all k'+m); cleared bits are known
     * erasures (CHUNK_ST_UNWRITTEN), never requested. NULL: fall back to obj_open. */
    int (*obj_open_pos)(void *ctx, const uint8_t oid[16], const chunk_geom *expect, const uint64_t *rec_sums,
                        uint32_t positions, cold_obj **out);
    /* R9 (r9-degsettle, E10): probe stripe positions in `probe_mask` (0 = all nstripe) and return bitmap of those UP
     * (and when require_epoch > 0, holding a durable non-unsure fence >= require_epoch) in *up_mask_out, and nstripe
     * in *npos_out. NULL: degraded settles not supported (single-server tiers). */
    int (*stripe_up_mask)(void *ctx, uint64_t require_epoch, uint32_t probe_mask, uint32_t *up_mask_out,
                          unsigned *npos_out);
    /* R9 (r9-degsettle, E10): notify the server's reconcile queue of a degraded settle's unwritten position. */
    void (*note_missing)(void *ctx, const uint8_t oid[16], unsigned pos);
    /* R9 (r9-whole, D10): obj_open with written-positions bitmap and the object's stripe_version (0: unknown/old).
     * While a position is FILLING at version V, reads of objects with stripe_version < V read from `prev` (A) while
     * A is UP and listed as prev, else treat pos as an erasure unless B reported the object filled. NULL: fall back. */
    int (*obj_open_ver)(void *ctx, const uint8_t oid[16], const chunk_geom *expect, const uint64_t *rec_sums,
                        uint32_t positions, uint64_t stripe_version, cold_obj **out);
    /* R9 (r9-whole, D12): discover up to `cap` former holders of WHOLE positions that are UP and may still hold
     * stale chunks of `(volume, pos)` since `version`. NULL: no position releases (single-server tiers). */
    unsigned (*release_candidates)(void *ctx, cold_release *out, unsigned cap);
    /* R9 (r9-whole, D12): verify that the current holder of `pos` is WHOLE (>= `ver`, != `node`), holds every live
     * object in `objs[0..nobjs)` (checked via CHUNK_LIST), and that `node` holds >= 1 chunk of `(volume, pos)`.
     * Returns 1 if ready to append MK_GC_RELEASE, 0 if `node` already holds 0 chunks of `(volume, pos)`, -1 to wait. */
    int (*release_verify)(void *ctx, unsigned pos, uint32_t node, uint64_t ver, const cold_live_obj *objs,
                          size_t nobjs);
    /* R9 (r9-whole, D12): delete all chunks of `(volume, pos)` on `node` under committed MK_GC_RELEASE at `mk_pos`.
     * Returns 0 when all chunks of `(volume, pos)` on `node` are gone, CHUNK_ERR_FENCED if fenced, or retryable err. */
    int (*release_pos)(void *ctx, unsigned pos, uint32_t node, uint64_t ver, uint64_t epoch, uint64_t mk_pos);
} cold_ops;

/* ------------------------------------------------------------ the directory ops (src/coldops_dir.c) */
struct tier;
extern const cold_ops coldops_dir;
/* The attach's `make` for the directory ops: arg NULL is the tier's backend (reads through the one the cache was
 * handed, eccache_backend; puts, deletes and the epoch through the tier's, whose stores a failure halts); arg a
 * chunk_backend * is that backend (tests: a volume over stores of its own; not owned, it must outlive the context). */
int coldops_dir_make(void *arg, struct tier *t, uint32_t volume, const uint8_t vuuid[16], void **ctx);

#endif
