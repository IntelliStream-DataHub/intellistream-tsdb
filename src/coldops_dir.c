/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The directory ops (src/coldops.h): one volume's cold operations over R7's chunk_backend, with R7's behaviour
 * unchanged. Every volume of a single server's tier gets these over the tier's one backend (a store flocks its
 * directory, so one backend per handle: src/tier.h); the context names the volume, its uuid and the tier. Store
 * indices, placement (chunk_be_place) and store states stay here: nothing above the ops knows them.
 *
 * Which backend: reads (obj_open) go through the one the cache was handed (eccache_backend: tests swap it while no
 * reader runs, and without one a read is TSDB_ERR_UNAVAILABLE), the rest through the tier's (t->be). They are the same
 * backend in every tier but test_tier's tierperengine mutant. A context made with a backend of its own (tests: a
 * second volume over other stores) uses that one for everything.
 */
#include "coldops.h"
#include "chunk_priv.h"
#include "eccache.h"
#include "tier.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct dirctx {
    tier *t;
    chunk_backend *own; /* not owned; NULL: the tier's */
    uint32_t volume;
    uint8_t vuuid[16];
} dirctx;

static chunk_backend *wbe(dirctx *d) { return d->own ? d->own : tier_backend(d->t); }

static void ident(const dirctx *d, const uint8_t oid[16], unsigned pos, chunk_ident *id)
{
    memset(id, 0, sizeof *id);
    id->volume = d->volume;
    memcpy(id->vuuid, d->vuuid, 16);
    memcpy(id->oid, oid, 16);
    id->pos = (uint16_t)pos;
}

static int d_put_stream_pos(void *ctx, const uint8_t oid[16], const chunk_geom *g, uint64_t epoch,
                            const chunk_put_src *src, uint32_t posmask, uint32_t *written_mask_out, uint64_t *sums_out)
{
    dirctx *d = ctx;
    chunk_backend *be = wbe(d);
    if (!be)
        return TSDB_ERR_UNAVAILABLE;
    unsigned npos = chunk_npos(g);
    uint32_t full = npos < 32 ? ((1u << npos) - 1u) : ~0u;
    uint32_t target = posmask ? (posmask & full) : full;
    int rc = chunk_be_put_stream(be, d->volume, d->vuuid, oid, g, epoch, src, target, sums_out);
    if (rc == 0 && written_mask_out)
        *written_mask_out = target;
    return rc;
}

static int d_put_stream(void *ctx, const uint8_t oid[16], const chunk_geom *g, uint64_t epoch,
                        const chunk_put_src *src, uint64_t *sums_out)
{
    return d_put_stream_pos(ctx, oid, g, epoch, src, 0, NULL, sums_out);
}

static int d_obj_open_pos(void *ctx, const uint8_t oid[16], const chunk_geom *expect, const uint64_t *rec_sums,
                          uint32_t positions, cold_obj **out)
{
    dirctx *d = ctx;
    int rc = 0;
    chunk_backend *be = d->own ? d->own : eccache_backend(d->t, &rc);
    if (!be)
        return rc ? rc : TSDB_ERR_UNAVAILABLE;
    chunk_obj *o = NULL;
    rc = chunk_obj_open_pos(be, d->volume, d->vuuid, oid, expect, rec_sums, positions, &o);
    *out = rc ? NULL : (cold_obj *)o; /* a chunk_obj holds the backend, which the tier keeps open until it closes */
    return rc;
}

static int d_obj_open(void *ctx, const uint8_t oid[16], const chunk_geom *expect, const uint64_t *rec_sums,
                      cold_obj **out)
{
    return d_obj_open_pos(ctx, oid, expect, rec_sums, 0, out);
}

static int d_obj_read(cold_obj *o, uint64_t off, uint64_t len, void *buf, unsigned flags, chunk_report *rep)
{
    return chunk_obj_read((chunk_obj *)o, off, len, buf, flags, rep);
}

static const chunk_geom *d_obj_geom(const cold_obj *o) { return chunk_obj_geom((const chunk_obj *)o); }

static void d_obj_close(cold_obj *o) { chunk_obj_close((chunk_obj *)o); }

static int d_del(void *ctx, const uint8_t oid[16], unsigned pos, uint64_t epoch, int reason)
{
    (void)reason;
    dirctx *d = ctx;
    chunk_backend *be = wbe(d);
    if (!be)
        return TSDB_ERR_UNAVAILABLE;
    chunk_ident id;
    ident(d, oid, pos, &id);
    return chunk_be_del(be, &id, epoch);
}

static int d_list_names(void *ctx, chunk_list_fn fn, void *fctx)
{
    dirctx *d = ctx;
    chunk_backend *be = wbe(d);
    if (!be)
        return TSDB_ERR_UNAVAILABLE;
    return chunk_be_list_names(be, d->volume, fn, fctx); /* chunk_listed.store: the index, our token */
}

static int d_owner(void *ctx, const chunk_listed *c, uint8_t vuuid[16])
{
    dirctx *d = ctx;
    chunk_backend *be = wbe(d);
    chunk_store *st = be ? chunk_be_store_ref(be, c->store) : NULL;
    chunk_hdr h;
    int rc = st ? chunk_store_peek(st, d->volume, c->oid, c->pos, &h) : TSDB_ERR_UNAVAILABLE;
    chunk_store_unref(st);
    if (rc == 0)
        memcpy(vuuid, h.vuuid, 16);
    return rc;
}

static int d_chunk_info(void *ctx, const chunk_listed *c, uint8_t vuuid[16], uint64_t *epoch)
{
    dirctx *d = ctx;
    chunk_backend *be = wbe(d);
    chunk_store *st = be ? chunk_be_store_ref(be, c->store) : NULL;
    chunk_hdr h;
    int rc = st ? chunk_store_peek(st, d->volume, c->oid, c->pos, &h) : TSDB_ERR_UNAVAILABLE;
    chunk_store_unref(st);
    if (rc == 0) {
        if (vuuid)
            memcpy(vuuid, h.vuuid, 16);
        if (epoch)
            *epoch = h.epoch;
    }
    return rc;
}

static bool d_all_up(void *ctx)
{
    dirctx *d = ctx;
    chunk_backend *be = wbe(d);
    if (!be)
        return false;
    for (int i = 0; i < chunk_be_nstores(be); i++)
        if (chunk_be_store_state(be, i) != CHUNK_STORE_UP)
            return false;
    return true;
}

static bool d_writable(void *ctx)
{
    dirctx *d = ctx;
    if (!d->own)
        return tier_writable(d->t);
    for (int i = 0; i < chunk_be_nstores(d->own); i++) {
        chunk_store *st = chunk_be_store_ref(d->own, i);
        bool bad = chunk_be_store_state(d->own, i) != CHUNK_STORE_UP || !st || chunk_store_failed(st);
        chunk_store_unref(st);
        if (bad)
            return false;
    }
    return true;
}

static uint64_t d_epoch(void *ctx)
{
    dirctx *d = ctx;
    chunk_backend *be = wbe(d);
    return be ? chunk_be_epoch(be, d->volume) : 0;
}

/* The epoch, given as the authoritative one at every open: it clears a volume whose EPOCH lost a slot (a torn store),
 * which would refuse every put. The call does not reach every store: the backend's fence is the highest epoch any
 * store holds, so a store behind it (a power cut before its EPOCH was stored, or a store that failed to take it at an
 * earlier open) is not raised, and not every error fails the store (a mkdir of s<N> refused leaves it UP). So each
 * store UP is raised by itself where it does not hold the epoch: one that still does not halts the tier (no put may
 * reach a store without its fence); none holding it is an error (the open fails). A context over a backend of its own
 * (tests only) halts nothing and stays writable: a put or delete there below a store's EPOCH is refused by the store
 * itself (src/chunk.h), and an unsure store refuses it, which is all its tests need. Ops that r8f writes keep the
 * invariant per context instead (a store that did not take the epoch makes writable false). */
static int d_see_epoch(void *ctx, uint64_t e)
{
    dirctx *d = ctx;
    chunk_backend *be = wbe(d);
    if (!be || tier_mutant == TIER_MUT_UNSURE)
        return 0; /* tierperengine's tier has none (its engine raised its own); tierunsure: planted */
    uint32_t vol = d->volume;
    int rc = chunk_be_see_epoch(be, vol, e);
    if (rc == TSDB_ERR_NOMEM)
        return rc;
    int held = tier_mutant == TIER_MUT_FENCE; /* planted: the call's word taken for every store */
    bool any_non_lost = false;
    for (int i = 0; i < chunk_be_nstores(be) && tier_mutant != TIER_MUT_FENCE; i++) {
        if (!chunk_be_store_is_lost(be, i))
            any_non_lost = true;
        chunk_store *st = chunk_be_store_ref(be, i);
        if (chunk_be_store_state(be, i) != CHUNK_STORE_UP || !st || chunk_store_failed(st)) {
            chunk_store_unref(st);
            continue; /* tier_writable halts for these */
        }
        bool has = chunk_store_epoch(st, vol) >= e && !chunk_store_epoch_unsure(st, vol);
        if (!has && chunk_store_raise(st, vol, e) == 0)
            has = chunk_store_epoch(st, vol) >= e && !chunk_store_epoch_unsure(st, vol);
        chunk_store_unref(st);
        if (has)
            held++;
        else if (!d->own)
            tier_halt_store(d->t, i, "did not take the volume's epoch at its open");
    }
    /* Only reachable with held == 0 && !any_non_lost in salvage mode (tier_open_salvage) when every store is
     * permanently LOST so no store exists to hold the epoch; any non-LOST store failing to hold still fails open. */
    if (held == 0 && any_non_lost)
        return rc ? rc : TSDB_ERR_IO;
    return 0;
}

static void d_free(void *ctx) { free(ctx); }

const cold_ops coldops_dir = {
    .name = "dir",
    .put_stream = d_put_stream,
    .obj_open = d_obj_open,
    .obj_read = d_obj_read,
    .obj_geom = d_obj_geom,
    .obj_close = d_obj_close,
    .del = d_del,
    .list_names = d_list_names,
    .owner = d_owner,
    .see_epoch = d_see_epoch,
    .epoch = d_epoch,
    .writable = d_writable,
    .all_up = d_all_up,
    .ctx_free = d_free,
    .chunk_info = d_chunk_info,
    .put_stream_pos = d_put_stream_pos,
    .obj_open_pos = d_obj_open_pos,
};

int coldops_dir_make(void *arg, struct tier *t, uint32_t volume, const uint8_t vuuid[16], void **ctx)
{
    dirctx *d = calloc(1, sizeof *d);
    if (!d)
        return TSDB_ERR_NOMEM;
    d->t = t;
    d->own = arg;
    d->volume = volume;
    memcpy(d->vuuid, vuuid, 16);
    *ctx = d;
    return 0;
}
