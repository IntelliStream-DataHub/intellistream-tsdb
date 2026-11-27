/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Healing, attaching and replacing the stores of a chunk backend (docs/design-cluster-data.md §5.5, §5.9, E15, E55).
 *
 * A chunk that is missing or damaged in its store is never edited: the object's data is read back through the
 * decoder (any k' good positions, each cell checked against its sum), the object is encoded again stripe by stripe,
 * and the cells of the position to rebuild go into a new chunk file (tmp, fsync, rename over the old name, directory
 * fsync: after a power loss the old file or the whole new one is there). What makes a rebuilt cell trustworthy is that
 * the same re-encode is compared, cell by cell, with the sums table of every chunk that survives: a set of chunks that
 * are not one encoding is refused and nothing is written. The rebuilt chunk is then read back and scrubbed.
 *
 * Which objects there are comes from the listings of the OTHER stores: an m > 0 chunk found only on the store being
 * healed is an orphan (its object was deleted while the store was away, E14) and never a reason to rebuild anything.
 * Single-copy (m = 0) chunks live only at pos 0 on their own store, so heal also scans the target store's names and
 * peeks target-only pos-0 candidates: whole m = 0 chunks count as chunks_ok, damaged ones (and conservatively any
 * damaged-header pos-0 chunk found only on the target) are reported unrecoverable and left untouched. An object with
 * fewer than k' good positions is not rebuilt either, so the remains of a deleted object stay what they are. For
 * k' = 1 with m > 0 (a copy is enough to decode) the layer cannot tell an orphan from a survivor: the caller's `live`
 * can.
 *
 * Epochs: a heal is fenced like a put (refused below the volume's EPOCH, raised above it), but the chunk it writes
 * carries the epoch the object was written at, so a delete at a lower epoch than the object's stays refused (E14).
 * A new store has every volume's fence raised, durably, before the first chunk is written to it.
 */
#include "internal.h"
#include "chunk.h"
#include "chunk_priv.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ---------------------------------------------------------------- which objects */
typedef struct ent {
    uint8_t oid[16];
    int store;
    uint16_t pos;
} ent;

typedef struct entv {
    ent *v;
    size_t n, cap;
    int store;
} entv;

static int ent_add(void *vctx, const chunk_listed *c)
{
    entv *e = vctx;
    if (c->status != CHUNK_ST_OK) /* a chunk whose header does not parse names nothing */
        return 0;
    if (e->n == e->cap) {
        size_t nc = e->cap ? 2 * e->cap : 256;
        ent *nv = realloc(e->v, nc * sizeof *nv);
        if (!nv)
            return TSDB_ERR_NOMEM;
        e->v = nv;
        e->cap = nc;
    }
    memcpy(e->v[e->n].oid, c->oid, 16);
    e->v[e->n].store = e->store;
    e->v[e->n].pos = c->pos;
    e->n++;
    return 0;
}

static int ent_cmp(const void *a, const void *b) { return memcmp(a, b, 16); }

typedef struct oid_vec {
    uint8_t (*v)[16];
    size_t n, cap;
    bool pos0_only;
} oid_vec;

static int oid_vec_add(void *vctx, const chunk_listed *c)
{
    oid_vec *e = vctx;
    if (e->pos0_only && c->pos != 0)
        return 0;
    if (e->n == e->cap) {
        size_t nc = e->cap ? 2 * e->cap : 64;
        uint8_t (*nv)[16] = realloc(e->v, nc * sizeof *nv);
        if (!nv)
            return TSDB_ERR_NOMEM;
        e->v = nv;
        e->cap = nc;
    }
    memcpy(e->v[e->n++], c->oid, 16);
    return 0;
}

/* ---------------------------------------------------------------- one object */
typedef struct heal_ctx {
    chunk_backend *be;
    uint32_t vol;
    int store;
    chunk_store *tgt;
    const chunk_heal_opts *o;
    chunk_heal_report *rep;
} heal_ctx;

typedef struct enc_ctx {
    chunk_obj *ob;
    chunk_wr *w[CHUNK_POS_MAX];
    uint64_t *sums[CHUNK_POS_MAX]; /* what was written, for the read-back */
    bool need[CHUNK_POS_MAX];
    int mismatch;                  /* the position whose own table disagreed with the re-encode, else -1 */
} enc_ctx;

static int enc_cell(void *vctx, unsigned pos, uint64_t idx, const uint8_t *data, size_t len, uint64_t sum)
{
    enc_ctx *c = vctx;
    if (c->need[pos]) {
        c->sums[pos][idx] = sum;
        return chunk_wr_cell_trusted(c->w[pos], data, len, sum);
    }
    const uint64_t *tab = chunk_obj_pos_sums(c->ob, pos);
    if (tab && tab[idx] != sum && chunk_mutant != CHUNK_MUT_HEAL_NOVERIFY) {
        c->mismatch = (int)pos;
        return TSDB_ERR_CORRUPT;
    }
    return 0;
}

static bool is_live(const heal_ctx *h, const uint8_t oid[16])
{
    if (chunk_mutant == CHUNK_MUT_HEAL_RESURRECT || !h->o->live)
        return true; /* planted: the caller's records are not asked */
    return h->o->live(h->o->live_ctx, oid) != 0;
}

/* A store that is transiently away and not being refilled: what it holds may still come back and make an object
 * readable. A permanently LOST store will never come back, and a REPLACING store (the target of this heal, or another
 * one being replaced) is being rebuilt, so neither counts as transiently away. */
static bool store_gone(chunk_backend *be, int i)
{
    if (chunk_be_store_is_lost(be, i))
        return false;
    int st = chunk_be_store_state(be, i);
    return st != CHUNK_STORE_UP && st != CHUNK_STORE_REPLACING;
}

static bool any_gone(chunk_backend *be, int except)
{
    if (chunk_mutant == CHUNK_MUT_HEAL_COUNT_TARGET) /* planted: the target's own absent positions are "away" too */
        return true;
    int bn = chunk_be_nstores(be);
    for (int i = 0; i < bn; i++)
        if (i != except && store_gone(be, i))
            return true;
    return false;
}

/* Heals the positions of the object that are placed on the target store (only_pos >= 0: just that one, always with a
 * scrub). When obs_store/obs_pos are given (nobs > 0, from listing other open stores), infers the object's original
 * store count so objects settled before a `tier add` still map missing positions to their true stores and never
 * duplicate positions already intact on another store. */
static int heal_object(heal_ctx *h, const uint8_t vuuid[16], const uint8_t oid[16], const chunk_geom *g, uint64_t epoch,
                       int only_pos, bool scrub, const int *obs_store, const unsigned *obs_pos, size_t nobs)
{
    chunk_heal_report *rep = h->rep;
    unsigned np = chunk_npos(g), k = g->k;
    bool need[CHUNK_POS_MAX] = {false}, miss[CHUNK_POS_MAX] = {false}, mine[CHUNK_POS_MAX] = {false};
    int found_on[CHUNK_POS_MAX];
    for (unsigned p = 0; p < CHUNK_POS_MAX; p++)
        found_on[p] = -1;
    for (size_t q = 0; q < nobs; q++)
        if (obs_pos[q] < CHUNK_POS_MAX)
            found_on[obs_pos[q]] = obs_store[q];
    int place_n = nobs > 0 ? chunk_be_infer_place_n(h->be, oid, np, obs_store, obs_pos, nobs, h->store)
                           : chunk_be_nstores(h->be);
    bool any_mine = false;
    for (unsigned p = 0; p < np; p++) {
        if (only_pos >= 0) {
            if ((int)p == only_pos)
                mine[p] = any_mine = true;
        } else if (g->m == 0) {
            /* m = 0 objects have only pos 0: if found on another store (ev), not mine; if scanned from tgt (tv), mine. */
            if (p == 0 && found_on[0] < 0)
                mine[0] = any_mine = true;
        } else if (found_on[p] < 0 && chunk_be_place_for_obj(h->be, oid, p, place_n) == h->store) {
            mine[p] = any_mine = true;
        }
    }
    if (!any_mine)
        return 0;
    rep->objects++;
    if (!is_live(h, oid)) {
        rep->not_live++;
        return 0;
    }
    /* what is on the target, by itself: whole, missing, or damaged */
    unsigned nneed = 0;
    for (unsigned p = 0; p < np; p++) {
        if (!mine[p])
            continue;
        chunk_ident id = {.volume = h->vol, .pos = (uint16_t)p};
        memcpy(id.vuuid, vuuid, 16);
        memcpy(id.oid, oid, 16);
        chunk_rd *r = NULL;
        int rc = chunk_rd_open(h->tgt, &id, &r);
        if (rc == 0) {
            const chunk_hdr *hd = chunk_rd_hdr(r);
            uint64_t nbad = 0;
            if (memcmp(&hd->g, g, sizeof *g) != 0)
                rc = TSDB_ERR_CORRUPT; /* another encoding's chunk under this name */
            else if (scrub || only_pos >= 0) {
                int sr = chunk_rd_scrub(r, &nbad);
                if (sr)
                    rc = sr; /* a cell that could not be read is not a whole one: counted failed below, never ok */
                else if (nbad > 0)
                    rc = TSDB_ERR_CORRUPT;
                if (h->o->pace)
                    h->o->pace(h->o->pace_ctx, chunk_file_size(g, p));
            }
            chunk_rd_close(r);
            if (rc == 0) {
                rep->chunks_ok++;
                continue;
            }
        }
        if (rc == TSDB_ERR_NOTFOUND) {
            need[p] = miss[p] = true;
            nneed++;
        } else if (rc == TSDB_ERR_CORRUPT) {
            need[p] = true;
            nneed++;
        } else {
            char hex[33];
            chunk_oid_hex(oid, hex);
            rep->failed++; /* an I/O error: the old file may be fine; a second heal retries */
            fprintf(stderr, "tsdb: heal: object %s position %u: the chunk could not be read (%d)\n", hex, p, rc);
        }
    }
    if (nneed == 0)
        return 0;
    if (g->m == 0) {
        /* single-copy (D5, k' = 1): no parity or other position to rebuild from; report damage and change nothing */
        char hex[33];
        chunk_oid_hex(oid, hex);
        rep->unrecoverable++;
        if (!chunk_log_quiet)
            fprintf(stderr, "tsdb: heal: object %s (single-copy, m=0): chunk is %s and cannot be rebuilt\n", hex,
                    miss[0] ? "missing" : "damaged");
        return 0;
    }

    chunk_obj *ob = NULL;
    int rc = chunk_obj_open(h->be, h->vol, vuuid, oid, g, NULL, &ob);
    if (rc) {
        /* UNAVAILABLE only when a store that is away could still make the object readable: the target's own
         * positions (REPLACING reads them as DOWN) are what this heal makes, never a way back for the object */
        if (rc == TSDB_ERR_UNAVAILABLE && any_gone(h->be, h->store))
            rep->unavailable++;
        else
            rep->unrecoverable++;
        return 0;
    }
    if (chunk_obj_epoch(ob) > epoch)
        epoch = chunk_obj_epoch(ob);
    int ob_place_n = chunk_obj_place_n(ob);
    if (ob_place_n > 0)
        place_n = ob_place_n;
    /* positions that can still be read, counting those on stores that are away as not lost */
    unsigned navail = 0, ndown = 0;
    for (unsigned p = 0; p < np; p++) {
        int st = chunk_obj_pos_status(ob, p);
        navail += st == CHUNK_ST_OK;
        ndown += st == CHUNK_ST_DOWN && (chunk_mutant == CHUNK_MUT_HEAL_COUNT_TARGET ||
                                         store_gone(h->be, chunk_be_place_for_obj(h->be, oid, p, place_n)));
    }
    unsigned want = chunk_mutant == CHUNK_MUT_HEAL_KMINUS1 ? k - 1 : k;
    if (navail < want) {
        if (navail + ndown >= k)
            rep->unavailable++;
        else
            rep->unrecoverable++;
        chunk_obj_close(ob);
        return 0;
    }

    enc_ctx ec;
    memset(&ec, 0, sizeof ec);
    ec.ob = ob;
    ec.mismatch = -1;
    int result = 0, fatal = 0; /* result: stop this object (counted); fatal: stop the heal (an error code) */
    uint8_t *buf = NULL;
    size_t cap = (size_t)k * (g->cell < g->len ? g->cell : (size_t)g->len);
    for (unsigned p = 0; p < np; p++)
        if (need[p]) {
            ec.need[p] = true;
            ec.sums[p] = malloc((size_t)chunk_pos_cells(g, p) * sizeof(uint64_t));
            if (!ec.sums[p])
                result = fatal = TSDB_ERR_NOMEM;
        }
    buf = result ? NULL : malloc(cap ? cap : 1);
    if (!buf && !result)
        result = fatal = TSDB_ERR_NOMEM;
    uint64_t hdr_epoch = chunk_mutant == CHUNK_MUT_HEAL_EPOCH0 ? 0 : epoch; /* planted: the object's epoch is lost */
    for (unsigned p = 0; p < np && result == 0; p++)
        if (need[p]) {
            chunk_ident id = {.volume = h->vol, .pos = (uint16_t)p};
            memcpy(id.vuuid, vuuid, 16);
            memcpy(id.oid, oid, 16);
            int r = chunk_wr_open_heal(h->tgt, &id, g, hdr_epoch, h->o->epoch, &ec.w[p]);
            if (r) {
                rep->failed++;
                fprintf(stderr, "tsdb: heal: cannot start the chunk of position %u (%d)\n", p, r);
                result = 1; /* this object only */
            }
        }
    bool done;
    for (uint64_t s = 0; result == 0 && s < chunk_nstripes(g); s++) {
        if (h->o->cancel && h->o->cancel(h->o->cancel_ctx)) {
            result = fatal = TSDB_ERR_STOPPED;
            break;
        }
        uint64_t off = s * (uint64_t)k * g->cell, n = g->len - off < (uint64_t)k * g->cell ? g->len - off
                                                                                           : (uint64_t)k * g->cell;
        int r = chunk_obj_read(ob, off, n, buf, 0, NULL);
        if (r && chunk_mutant == CHUNK_MUT_HEAL_KMINUS1) { /* planted: a stripe short of cells is made up (zeros) */
            memset(buf, 0, (size_t)n);
            r = 0;
        }
        if (r) {
            if (r == TSDB_ERR_UNAVAILABLE && any_gone(h->be, h->store))
                rep->unavailable++;
            else if (r == TSDB_ERR_CORRUPT || r == TSDB_ERR_UNAVAILABLE || r == TSDB_ERR_IO)
                rep->unrecoverable++;
            else
                rep->failed++;
            result = 1;
            break;
        }
        r = chunk_encode_stripe(g, s, buf, enc_cell, &ec);
        if (r) {
            rep->failed++;
            if (ec.mismatch >= 0)
                fprintf(stderr, "tsdb: heal: object: the chunk of position %d does not match the re-encode of the "
                                "others: the chunks are not one encoding, nothing is written\n", ec.mismatch);
            result = 1;
            break;
        }
        if (h->o->pace) {
            uint64_t sbytes = 0;
            for (unsigned p = 0; p < np; p++)
                if (need[p] && s < chunk_pos_cells(g, p))
                    sbytes += chunk_pos_cell_len(g, p, s);
            if (sbytes > 0)
                h->o->pace(h->o->pace_ctx, sbytes);
        }
    }
    done = result == 0;
    /* the caller's records once more, right before a name is made (a delete may have come in while this ran) */
    if (done && !is_live(h, oid)) {
        rep->not_live++;
        done = false;
    }
    for (unsigned p = 0; p < np; p++) {
        if (!ec.w[p])
            continue;
        if (!done) {
            chunk_wr_abort(ec.w[p]);
            ec.w[p] = NULL;
            continue;
        }
        int r = chunk_wr_commit(ec.w[p]); /* frees the writer on every path */
        ec.w[p] = NULL;
        if (r) {
            rep->failed++;
            fprintf(stderr, "tsdb: heal: the rebuilt chunk of position %u could not be made (%d)\n", p, r);
            done = false; /* the ones after it are aborted; those before it stand */
            if (chunk_store_failed(h->tgt))
                fatal = TSDB_ERR_IO;
            continue;
        }
        uint64_t fsz = chunk_file_size(g, p);
        rep->bytes += fsz;
        if (h->o->pace) {
            uint64_t clen = chunk_pos_len(g, p);
            if (fsz > clen)
                h->o->pace(h->o->pace_ctx, fsz - clen);
        }
        bool ok = true;
        if (chunk_fault_heal_flip > 0 && --chunk_fault_heal_flip == 0) { /* test hook: a write that landed wrong */
            chunk_ident fid = {.volume = h->vol, .pos = (uint16_t)p};
            memcpy(fid.vuuid, vuuid, 16);
            memcpy(fid.oid, oid, 16);
            (void)chunk_store_test_flip(h->tgt, &fid);
        }
        if (chunk_mutant != CHUNK_MUT_HEAL_NOVERIFY) { /* planted: no read-back */
            chunk_ident id = {.volume = h->vol, .pos = (uint16_t)p};
            memcpy(id.vuuid, vuuid, 16);
            memcpy(id.oid, oid, 16);
            chunk_rd *rd = NULL;
            uint64_t nbad = 0;
            ok = chunk_rd_open(h->tgt, &id, &rd) == 0;
            if (ok) {
                ok = chunk_rd_hdr(rd)->epoch == hdr_epoch && memcmp(chunk_rd_sums(rd), ec.sums[p],
                         (size_t)chunk_pos_cells(g, p) * sizeof(uint64_t)) == 0 &&
                     chunk_rd_scrub(rd, &nbad) == 0 && nbad == 0;
                chunk_rd_close(rd);
            }
        }
        if (!ok) {
            rep->failed++;
            fprintf(stderr, "tsdb: heal: the rebuilt chunk of position %u did not read back whole\n", p);
        } else if (miss[p])
            rep->rebuilt_missing++;
        else
            rep->rebuilt_damaged++;
    }
    for (unsigned p = 0; p < np; p++)
        free(ec.sums[p]);
    free(buf);
    chunk_obj_close(ob);
    return fatal;
}

static void note_target_m0_err(const heal_ctx *h, const uint8_t oid[16], bool damaged)
{
    if (chunk_be_pos_store(h->be, oid, 0) != h->store)
        return;
    h->rep->objects++;
    if (!is_live(h, oid)) {
        h->rep->not_live++;
        return;
    }
    if (damaged) {
        if (any_gone(h->be, h->store)) {
            /* another store is DOWN: a damaged-header pos-0 chunk may belong to an m > 0 object whose peers are away */
            h->rep->unavailable++;
        } else {
            h->rep->unrecoverable++;
            if (!chunk_log_quiet) {
                char hex[33];
                chunk_oid_hex(oid, hex);
                fprintf(stderr, "tsdb: heal: object %s (single-copy, m=0): chunk is damaged and cannot be rebuilt\n",
                        hex);
            }
        }
    } else {
        h->rep->failed++;
    }
}

/* ---------------------------------------------------------------- one store, one volume */
int chunk_be_heal(chunk_backend *be, uint32_t volume, int store, const chunk_heal_opts *o, chunk_heal_report *rep)
{
    static const chunk_heal_opts none;
    chunk_heal_report local;
    if (!rep)
        rep = &local;
    memset(rep, 0, sizeof *rep);
    if (be->read_only)
        return TSDB_ERR_READONLY;
    if (!o)
        o = &none;
    int bn = chunk_be_nstores(be);
    if (store < 0 || store >= bn)
        return TSDB_ERR_INVALID;
    int stt = chunk_be_store_state(be, store);
    if (stt != CHUNK_STORE_UP && stt != CHUNK_STORE_REPLACING)
        return stt == CHUNK_STORE_DOWN_IOERR && chunk_be_raw_store(be, store) ? TSDB_ERR_IO : TSDB_ERR_UNAVAILABLE;
    chunk_store *tgt = chunk_be_store_ref(be, store);
    if (!tgt)
        return TSDB_ERR_UNAVAILABLE;
    int rc = chunk_be_prepare(be, volume, o->epoch);
    if (rc) {
        chunk_store_unref(tgt);
        return rc;
    }
    entv ev = {.store = -1};
    for (int i = 0; i < bn && rc == 0; i++) {
        if (i == store)
            continue;
        chunk_store *st = chunk_be_store_ref(be, i);
        if (!st)
            continue;
        ev.store = i;
        rc = chunk_store_list(st, volume, false, ent_add, &ev);
        chunk_store_unref(st);
    }
    if (rc) {
        free(ev.v);
        chunk_store_unref(tgt);
        return rc;
    }
    if (ev.n > 1)
        qsort(ev.v, ev.n, sizeof *ev.v, ent_cmp);
    heal_ctx h = {be, volume, store, tgt, o, rep};
    for (size_t i = 0; i < ev.n && rc == 0;) {
        if ((o->cancel && o->cancel(o->cancel_ctx)) || (o->yield && o->yield(o->yield_ctx))) {
            rc = TSDB_ERR_STOPPED;
            break;
        }
        size_t j = i;
        while (j < ev.n && memcmp(ev.v[j].oid, ev.v[i].oid, 16) == 0)
            j++;
        chunk_hdr hd;
        bool got = false;
        int obs_st[CHUNK_POS_MAX];
        unsigned obs_pos[CHUNK_POS_MAX];
        size_t nobs = 0;
        for (size_t x = i; x < j; x++) {
            if (nobs < CHUNK_POS_MAX && ev.v[x].pos < CHUNK_POS_MAX) {
                obs_st[nobs] = ev.v[x].store;
                obs_pos[nobs] = ev.v[x].pos;
                nobs++;
            }
            if (!got) { /* the first chunk of the object whose header reads */
                chunk_store *st = chunk_be_store_ref(be, ev.v[x].store);
                got = st && chunk_store_peek(st, volume, ev.v[x].oid, ev.v[x].pos, &hd) == 0;
                chunk_store_unref(st);
            }
        }
        if (!got) {
            rep->failed++;
        } else {
            rc = heal_object(&h, hd.vuuid, ev.v[i].oid, &hd.g, hd.epoch, -1, o->scrub, obs_st, obs_pos, nobs);
            if (chunk_store_failed(tgt) && rc == 0)
                rc = TSDB_ERR_IO;
        }
        i = j;
    }
    if (rc == 0) {
        /* Single-copy (m = 0) objects live only at pos 0 on this store and are never listed by other stores. Scan only
         * this store's open dirfd (names only), skip oids already seen in ev, and peek each remaining candidate's
         * header once so live m > 0 objects cost zero extra opens or reads and m > 0 orphans cost one header peek. */
        oid_vec tv = {.pos0_only = true};
        rc = chunk_store_list_names(tgt, volume, oid_vec_add, &tv);
        for (size_t i = 0; i < tv.n && rc == 0; i++) {
            if ((o->cancel && o->cancel(o->cancel_ctx)) || (o->yield && o->yield(o->yield_ctx))) {
                rc = TSDB_ERR_STOPPED;
                break;
            }
            if (ev.n > 0 && bsearch(tv.v[i], ev.v, ev.n, sizeof *ev.v, ent_cmp))
                continue;
            chunk_hdr hd;
            int pr = chunk_store_peek(tgt, volume, tv.v[i], 0, &hd);
            if (pr == TSDB_ERR_NOTFOUND)
                continue;
            if (pr == 0) {
                if (hd.g.m != 0)
                    continue; /* m > 0 chunk found only here: an E14 orphan, never rebuilt */
                rc = heal_object(&h, hd.vuuid, tv.v[i], &hd.g, hd.epoch, -1, o->scrub, NULL, NULL, 0);
                if (chunk_store_failed(tgt) && rc == 0)
                    rc = TSDB_ERR_IO;
            } else {
                note_target_m0_err(&h, tv.v[i], pr == TSDB_ERR_CORRUPT);
            }
        }
        free(tv.v);
    }
    free(ev.v);
    chunk_store_unref(tgt);
    return rc;
}

int chunk_be_repair(chunk_backend *be, const chunk_ident *id, const chunk_heal_opts *o, chunk_heal_report *rep)
{
    static const chunk_heal_opts none;
    chunk_heal_report local;
    if (!rep)
        rep = &local;
    memset(rep, 0, sizeof *rep);
    if (be->read_only)
        return TSDB_ERR_READONLY;
    if (!o)
        o = &none;
    int store = -1;
    int bn = chunk_be_nstores(be);
    /* Locate existing chunk (handles objects written before a `tier add`), falling back to placement. */
    for (int i = 0; i < bn; i++) {
        chunk_store *st = chunk_be_store_ref(be, i);
        if (!st)
            continue;
        chunk_hdr tmp_hd;
        int pr = chunk_store_peek(st, id->volume, id->oid, id->pos, &tmp_hd);
        chunk_store_unref(st);
        if (pr == 0 || pr == TSDB_ERR_CORRUPT) {
            store = i;
            break;
        }
    }
    chunk_obj *ob = NULL;
    int rc = chunk_be_prepare(be, id->volume, o->epoch);
    if (rc)
        return rc;
    int open_rc = chunk_obj_open(be, id->volume, id->vuuid, id->oid, NULL, NULL, &ob);
    if (store < 0)
        store = (open_rc == 0 && ob) ? chunk_be_place_for_obj(be, id->oid, id->pos, chunk_obj_place_n(ob))
                                     : chunk_be_pos_store(be, id->oid, id->pos);
    int stt = chunk_be_store_state(be, store);
    if (stt != CHUNK_STORE_UP && stt != CHUNK_STORE_REPLACING) {
        if (ob)
            chunk_obj_close(ob);
        return TSDB_ERR_UNAVAILABLE;
    }
    chunk_store *tgt = chunk_be_store_ref(be, store);
    if (!tgt) {
        if (ob)
            chunk_obj_close(ob);
        return TSDB_ERR_UNAVAILABLE;
    }
    if (open_rc) { /* nothing of the object can be read: say which kind of nothing */
        heal_ctx h = {be, id->volume, store, tgt, o, rep};
        chunk_hdr hd;
        bool single = id->pos == 0 && chunk_store_peek(tgt, id->volume, id->oid, 0, &hd) == 0 &&
                      memcmp(hd.vuuid, id->vuuid, 16) == 0 && hd.g.m == 0;
        if (single) {
            rep->objects++;
            if (!is_live(&h, id->oid)) {
                rep->not_live++;
                chunk_store_unref(tgt);
                return 0;
            }
            rep->unrecoverable++;
            if (!chunk_log_quiet) {
                char hex[33];
                chunk_oid_hex(id->oid, hex);
                fprintf(stderr, "tsdb: repair: object %s (single-copy, m=0): chunk is damaged and cannot be rebuilt\n",
                        hex);
            }
            chunk_store_unref(tgt);
            return 0;
        }
        if (open_rc == TSDB_ERR_UNAVAILABLE && any_gone(be, store))
            rep->unavailable++;
        else
            rep->unrecoverable++;
        chunk_store_unref(tgt);
        return 0;
    }
    chunk_geom g = *chunk_obj_geom(ob);
    uint64_t epoch = chunk_obj_epoch(ob);
    chunk_obj_close(ob);
    if (id->pos >= chunk_npos(&g)) {
        chunk_store_unref(tgt);
        return TSDB_ERR_INVALID;
    }
    heal_ctx h = {be, id->volume, store, tgt, o, rep};
    rc = heal_object(&h, id->vuuid, id->oid, &g, epoch, id->pos, true, NULL, NULL, 0);
    chunk_store_unref(tgt);
    return rc;
}

/* ---------------------------------------------------------------- settle, attach, replace */
/* Every volume any open store (or the fence) knows. */
static int volumes_of(chunk_backend *be, uint32_t **out, size_t *nout)
{
    uint32_t *v = NULL;
    size_t n = 0, cap = 0;
    pthread_mutex_lock(&be->mu);
    for (size_t i = 0; i < be->nf; i++) {
        if (n == cap) {
            cap = cap ? 2 * cap : 8;
            uint32_t *nvv = realloc(v, cap * sizeof *v);
            if (!nvv) {
                pthread_mutex_unlock(&be->mu);
                free(v);
                return TSDB_ERR_NOMEM;
            }
            v = nvv;
        }
        v[n++] = be->f[i].vol;
    }
    pthread_mutex_unlock(&be->mu);
    int bn = chunk_be_nstores(be);
    for (int i = 0; i < bn; i++) {
        chunk_store *st = chunk_be_store_ref(be, i);
        uint32_t *vols;
        uint64_t *eps;
        size_t nv;
        int vrc = st ? chunk_store_volumes(st, &vols, &eps, &nv) : -1;
        chunk_store_unref(st);
        if (vrc != 0)
            continue;
        for (size_t j = 0; j < nv; j++) {
            bool have = false;
            for (size_t x = 0; x < n; x++)
                have |= v[x] == vols[j];
            if (have)
                continue;
            if (n == cap) {
                cap = cap ? 2 * cap : 8;
                uint32_t *nvv = realloc(v, cap * sizeof *v);
                if (!nvv) {
                    free(vols);
                    free(eps);
                    free(v);
                    return TSDB_ERR_NOMEM;
                }
                v = nvv;
            }
            v[n++] = vols[j];
        }
        free(vols);
        free(eps);
    }
    *out = v;
    *nout = n;
    return 0;
}

int chunk_be_volumes_list(chunk_backend *be, uint32_t **out, size_t *nout)
{
    return volumes_of(be, out, nout);
}

static void report_add(chunk_heal_report *to, const chunk_heal_report *r)
{
    to->objects += r->objects;
    to->chunks_ok += r->chunks_ok;
    to->rebuilt_missing += r->rebuilt_missing;
    to->rebuilt_damaged += r->rebuilt_damaged;
    to->unrecoverable += r->unrecoverable;
    to->unavailable += r->unavailable;
    to->not_live += r->not_live;
    to->failed += r->failed;
    to->bytes += r->bytes;
}

static int be_settle(chunk_backend *be, int store, const chunk_heal_opts *o, chunk_heal_report *rep,
                     const chunk_heal_report *extra)
{
    chunk_heal_report local;
    if (!rep)
        rep = &local;
    memset(rep, 0, sizeof *rep);
    if (extra)
        report_add(rep, extra);
    if (store < 0 || store >= chunk_be_nstores(be))
        return TSDB_ERR_INVALID;
    uint32_t *vols = NULL;
    size_t nv = 0;
    int rc = volumes_of(be, &vols, &nv);
    for (size_t i = 0; i < nv && rc == 0; i++) {
        if (o && ((o->cancel && o->cancel(o->cancel_ctx)) || (o->yield && o->yield(o->yield_ctx)))) {
            rc = TSDB_ERR_STOPPED;
            break;
        }
        chunk_heal_opts ho;
        memset(&ho, 0, sizeof ho);
        if (o)
            ho = *o;
        if (ho.epoch == 0)
            ho.epoch = chunk_be_epoch(be, vols[i]); /* the backend's own account: the volume's current fence */
        chunk_heal_report r;
        rc = chunk_be_heal(be, vols[i], store, &ho, &r);
        report_add(rep, &r);
    }
    free(vols);
    if (rc)
        return rc;
    if (chunk_be_store_state(be, store) == CHUNK_STORE_REPLACING) {
        /* objects that only a store that is away lists were never seen: nothing is known about them, so the store
         * is not promoted while any other store is gone (a REPLACING one is open and was listed) */
        if (chunk_mutant == CHUNK_MUT_SETTLE_UP_ALWAYS ||
            (rep->failed == 0 && rep->unavailable == 0 && !any_gone(be, store))) {
            if (o && o->before_up) {
                int brc = o->before_up(o->before_up_ctx, store);
                if (brc != 0)
                    return brc;
            }
            char pbuf[PATH_MAX];
            bool promoted = false;
            pthread_mutex_lock(&be->st_mu);
            int cur = atomic_load_explicit(&be->state[store], memory_order_relaxed);
            chunk_store *cur_st = atomic_load_explicit(&be->st[store], memory_order_relaxed);
            if (cur == CHUNK_STORE_REPLACING && cur_st && !chunk_store_failed(cur_st)) {
                atomic_store_explicit(&be->state[store], CHUNK_STORE_UP, memory_order_release);
                snprintf(pbuf, sizeof pbuf, "%s", be->path[store] ? be->path[store] : "-");
                promoted = true;
            }
            pthread_mutex_unlock(&be->st_mu);
            if (promoted) {
                fprintf(stderr, "tsdb: chunk backend: store %d (%s) is UP: %llu chunks rebuilt, %llu objects past repair\n",
                        store, pbuf, (unsigned long long)(rep->rebuilt_missing + rep->rebuilt_damaged),
                        (unsigned long long)rep->unrecoverable);
                return 0;
            }
        }
        return TSDB_ERR_UNAVAILABLE; /* filled as far as the other stores allow: settle again later */
    }
    return 0;
}

int chunk_be_settle(chunk_backend *be, int store, const chunk_heal_opts *o, chunk_heal_report *rep)
{
    if (be->read_only)
        return TSDB_ERR_READONLY;
    return be_settle(be, store, o, rep, NULL);
}

/* Puts the open store in the place of index i (which is DOWN) as REPLACING. */
static int install(chunk_backend *be, int i, chunk_store *ns, const char *path)
{
    char *p = strdup(path);
    if (!p)
        return TSDB_ERR_NOMEM;
    pthread_mutex_lock(&be->mu);
    chunk_store *old = chunk_be_open_store(be, i);
    chunk_store **nr = NULL;
    if (old) { /* one that failed in service: kept open until close, a thread may still hold a writer on it */
        nr = realloc(be->retired, (size_t)(be->nretired + 1) * sizeof *nr);
        if (!nr) {
            pthread_mutex_unlock(&be->mu);
            free(p);
            return TSDB_ERR_NOMEM;
        }
        be->retired = nr;
    }
    pthread_mutex_lock(&be->st_mu);
    old = atomic_load_explicit(&be->st[i], memory_order_relaxed);
    if (old)
        be->retired[be->nretired++] = old;
    char *old_path = be->path[i];
    be->path[i] = p;
    chunk_store_bind_be(ns, be, i);
    atomic_store_explicit(&be->st_failed[i], chunk_store_failed(ns), memory_order_release);
    atomic_store_explicit(&be->st_lost[i], false, memory_order_release);
    atomic_store_explicit(&be->state[i], CHUNK_STORE_REPLACING, memory_order_release);
    atomic_store_explicit(&be->st[i], ns, memory_order_release);
    pthread_mutex_unlock(&be->st_mu);
    pthread_mutex_unlock(&be->mu);
    free(old_path);
    return 0;
}

/* Every volume's fence into the new store, durably. */
static int fence_new_store(chunk_backend *be, chunk_store *ns)
{
    uint32_t *vols = NULL;
    size_t nv = 0;
    int rc = volumes_of(be, &vols, &nv);
    for (size_t i = 0; i < nv && rc == 0; i++)
        rc = chunk_store_raise(ns, vols[i], chunk_be_epoch(be, vols[i]));
    free(vols);
    return rc;
}

int chunk_be_fence_store(chunk_backend *be, int index)
{
    chunk_store *st = chunk_be_store_ref(be, index);
    if (!st)
        return TSDB_ERR_UNAVAILABLE;
    int rc = fence_new_store(be, st);
    chunk_store_unref(st);
    return rc;
}

/* The caller's epoch below a volume's fence: refused before anything is made (a store half-filled for nothing). */
static int check_epoch(chunk_backend *be, const chunk_heal_opts *o)
{
    if (!o || o->epoch == 0)
        return 0;
    uint32_t *vols = NULL;
    size_t nv = 0;
    int rc = volumes_of(be, &vols, &nv);
    for (size_t i = 0; i < nv && rc == 0; i++)
        if (o->epoch < chunk_be_epoch(be, vols[i]) && chunk_mutant != CHUNK_MUT_NOFENCE)
            rc = CHUNK_ERR_FENCED;
    free(vols);
    return rc;
}

static bool id_taken(chunk_backend *be, int except, const uint8_t id[16])
{
    int bn = chunk_be_nstores(be);
    for (int i = 0; i < bn; i++) {
        if (i == except)
            continue;
        chunk_store *st = chunk_be_raw_store_ref(be, i);
        bool match = st && memcmp(chunk_store_id(st), id, 16) == 0;
        chunk_store_unref(st);
        if (match)
            return true;
    }
    return false;
}

int chunk_be_attach(chunk_backend *be, int index, const char *path, const uint8_t *id, const chunk_heal_opts *o,
                    chunk_heal_report *rep)
{
    if (be->read_only)
        return TSDB_ERR_READONLY;
    if (index < 0 || index >= chunk_be_nstores(be) || !path)
        return TSDB_ERR_INVALID;
    int stt = chunk_be_store_state(be, index);
    if (stt == CHUNK_STORE_UP || stt == CHUNK_STORE_REPLACING || chunk_be_raw_store(be, index))
        return TSDB_ERR_INVALID; /* open already (or failed/damaged in service: its descriptors cannot be trusted) */
    chunk_store *ns = NULL;
    int rc = check_epoch(be, o);
    if (rc)
        return rc;
    /* Release the old store parked by chunk_be_set_down (and its flock) here on the attach thread before reopening. */
    chunk_be_release_down(be, index);
    rc = chunk_store_open_quiet(path, id, &ns);
    if (rc)
        return rc;
    if (id_taken(be, index, chunk_store_id(ns))) {
        chunk_store_close(ns);
        return TSDB_ERR_INVALID; /* another index already is this store */
    }
    uint32_t *vols;
    uint64_t *eps;
    size_t nv;
    rc = chunk_store_volumes(ns, &vols, &eps, &nv);
    if (rc == 0) { /* (on an error chunk_store_volumes has freed them) */
        for (size_t i = 0; i < nv && rc == 0; i++)
            rc = chunk_be_see_epoch(be, vols[i], eps[i]); /* it may have seen an epoch the others have not */
        free(vols);
        free(eps);
    }
    if (rc == 0)
        rc = fence_new_store(be, ns); /* and the epochs it missed while it was away, before any chunk is made in it */
    if (rc == 0)
        rc = install(be, index, ns, path);
    if (rc) {
        chunk_store_close(ns);
        return rc;
    }
    if (chunk_mutant == CHUNK_MUT_ATTACHNOHEAL) { /* planted: UP without healing missing/damaged chunks */
        if (rep)
            memset(rep, 0, sizeof *rep);
        pthread_mutex_lock(&be->st_mu);
        if (atomic_load_explicit(&be->state[index], memory_order_relaxed) == CHUNK_STORE_REPLACING)
            atomic_store_explicit(&be->state[index], CHUNK_STORE_UP, memory_order_release);
        pthread_mutex_unlock(&be->st_mu);
        return 0;
    }
    return chunk_be_settle(be, index, o, rep);
}

/* Counts live single-copy (m = 0) objects on a retiring store that is still open (failed in service) via its open
 * dirfd: they have no other position to rebuild from, so replacing the store counts them unrecoverable and writes
 * nothing. Any listing or header-read failure on the retiring store is counted in rep->failed (and unreadable pos-0
 * candidates also in rep->unrecoverable, or rep->unavailable when another store is DOWN) so m = 0 loss is never hidden
 * behind unrecoverable == 0 && failed == 0. */
static void count_lost_m0(chunk_backend *be, int index, chunk_store *old, const chunk_heal_opts *o,
                          chunk_heal_report *rep)
{
    static const chunk_heal_opts none;
    if (!o)
        o = &none;
    uint32_t *vols = NULL;
    uint64_t *eps = NULL;
    size_t nv = 0;
    if (chunk_store_volumes(old, &vols, &eps, &nv) != 0) {
        rep->failed++;
        fprintf(stderr, "tsdb: replace: store %d: cannot list volumes on retiring store\n", index);
        return;
    }
    for (size_t vi = 0; vi < nv; vi++) {
        uint32_t vol = vols[vi];
        oid_vec mine = {.pos0_only = true}, other = {.pos0_only = false};
        if (chunk_store_list_names(old, vol, oid_vec_add, &mine) != 0) {
            rep->failed++;
            fprintf(stderr, "tsdb: replace: store %d: cannot list volume %u on retiring store\n", index, vol);
            free(mine.v);
            continue;
        }
        if (mine.n == 0) {
            free(mine.v);
            continue;
        }
        int orc = 0;
        int bn = chunk_be_nstores(be);
        for (int s = 0; s < bn && orc == 0; s++) {
            if (s == index)
                continue;
            chunk_store *st = chunk_be_store_ref(be, s);
            if (!st)
                continue;
            orc = chunk_store_list_names(st, vol, oid_vec_add, &other);
            chunk_store_unref(st);
        }
        if (orc != 0) {
            rep->failed++;
            fprintf(stderr, "tsdb: replace: store %d: cannot list peer stores for volume %u\n", index, vol);
            free(mine.v);
            free(other.v);
            continue;
        }
        if (other.n > 1)
            qsort(other.v, other.n, sizeof *other.v, ent_cmp);
        heal_ctx h = {be, vol, index, old, o, rep};
        bool gone = any_gone(be, index);
        for (size_t i = 0; i < mine.n; i++) {
            if (other.n > 0 && bsearch(mine.v[i], other.v, other.n, sizeof *other.v, ent_cmp))
                continue;
            if (chunk_be_pos_store(be, mine.v[i], 0) != index)
                continue;
            chunk_hdr hd;
            int pr = chunk_store_peek(old, vol, mine.v[i], 0, &hd);
            if (pr == 0 && hd.g.m != 0)
                continue; /* provably an m > 0 orphan */
            rep->objects++;
            if (!is_live(&h, mine.v[i])) {
                rep->not_live++;
                continue;
            }
            if (pr != 0 && gone) {
                /* another store is DOWN: a damaged/unreadable pos-0 chunk may belong to an m > 0 object whose peers are away */
                rep->unavailable++;
                if (pr != TSDB_ERR_CORRUPT)
                    rep->failed++;
            } else {
                rep->unrecoverable++;
                if (pr != 0 && pr != TSDB_ERR_CORRUPT)
                    rep->failed++;
            }
            if (!chunk_log_quiet) {
                char hex[33];
                chunk_oid_hex(mine.v[i], hex);
                if (pr == 0 || pr == TSDB_ERR_CORRUPT)
                    fprintf(stderr, "tsdb: replace: store %d: object %s (single-copy, m=0) is %s\n", index, hex,
                            (pr != 0 && gone) ? "unavailable" : "unrecoverable");
                else
                    fprintf(stderr, "tsdb: replace: store %d: object %s: cannot read header on retiring store (%d), "
                                    "counted %s and failed\n",
                            index, hex, pr, gone ? "unavailable" : "unrecoverable");
            }
        }
        free(mine.v);
        free(other.v);
    }
    free(vols);
    free(eps);
}

int chunk_be_replace(chunk_backend *be, int index, const char *new_path, uint8_t new_id_out[16], const chunk_heal_opts *o,
                     chunk_heal_report *rep)
{
    if (be->read_only)
        return TSDB_ERR_READONLY;
    if (index < 0 || index >= chunk_be_nstores(be) || !new_path)
        return TSDB_ERR_INVALID;
    int stt = chunk_be_store_state(be, index);
    if (stt == CHUNK_STORE_UP || stt == CHUNK_STORE_REPLACING)
        return TSDB_ERR_INVALID; /* an UP store is not lost; a REPLACING one is settled, not replaced */
    uint8_t id[16];
    int rc = check_epoch(be, o);
    if (rc == 0)
        rc = chunk_store_init(new_path, NULL, id);
    if (rc)
        return rc;
    if (new_id_out)
        memcpy(new_id_out, id, 16);
    chunk_store *ns = NULL;
    rc = chunk_store_open_quiet(new_path, id, &ns);
    if (rc)
        return rc;
    if (chunk_mutant != CHUNK_MUT_REPLACE_LATE) /* planted: raised after the first chunk */
        rc = fence_new_store(be, ns);
    chunk_heal_report lost_m0;
    memset(&lost_m0, 0, sizeof lost_m0);
    chunk_store *old = rc == 0 ? chunk_be_raw_store_ref(be, index) : NULL;
    if (old) {
        count_lost_m0(be, index, old, o, &lost_m0);
        chunk_store_unref(old);
    } else if (rc == 0)
        fprintf(stderr, "tsdb: replace: store %d was not open (%s): any single-copy (m=0) objects placed on it cannot "
                        "be enumerated here and are lost\n",
                index, chunk_store_state_name(stt));
    if (rc == 0)
        rc = install(be, index, ns, new_path);
    if (rc) {
        chunk_store_close(ns);
        return rc;
    }
    rc = be_settle(be, index, o, rep, &lost_m0);
    if (chunk_mutant == CHUNK_MUT_REPLACE_LATE && fence_new_store(be, ns) != 0 && rc == 0)
        rc = TSDB_ERR_IO;
    return rc;
}

/* R15 (r15-lost): replaces slot `index` with a store already initialized and bound to db_id by tsdb_tier_lost /
 * CLI `tier lost --replace`, installs it as REPLACING, and settles (rebuilds) all positions onto it. */
int chunk_be_replace_open(chunk_backend *be, int index, const char *new_path, const uint8_t expect_id[16],
                          const uint8_t *expect_db, const chunk_heal_opts *o, chunk_heal_report *rep)
{
    if (!be || index < 0 || index >= chunk_be_nstores(be) || !new_path)
        return TSDB_ERR_INVALID;
    if (be->read_only)
        return TSDB_ERR_READONLY;
    int stt = chunk_be_store_state(be, index);
    if (stt == CHUNK_STORE_UP || stt == CHUNK_STORE_REPLACING)
        return TSDB_ERR_INVALID;
    int rc = check_epoch(be, o);
    if (rc)
        return rc;
    chunk_be_release_down(be, index);
    chunk_store *ns = NULL;
    rc = chunk_store_open_db_quiet(new_path, expect_id, expect_db, &ns);
    if (rc)
        return rc;
    if (id_taken(be, index, chunk_store_id(ns))) {
        chunk_store_close(ns);
        return TSDB_ERR_INVALID;
    }
    rc = fence_new_store(be, ns);
    chunk_heal_report lost_m0;
    memset(&lost_m0, 0, sizeof lost_m0);
    chunk_store *old = rc == 0 ? chunk_be_store_ref(be, index) : NULL;
    if (old) {
        count_lost_m0(be, index, old, o, &lost_m0);
        chunk_store_unref(old);
    }
    if (rc == 0)
        rc = install(be, index, ns, new_path);
    if (rc) {
        chunk_store_close(ns);
        return rc;
    }
    return be_settle(be, index, o, rep, &lost_m0);
}

#ifdef TSDB_TEST_HOOKS
/* Planted mutant helper for `lostnoreplace` (TSDB_TEST_HOOKS): installs (if not already REPLACING) and promotes
 * the replacement store directly to UP without rebuilding any of the LOST store's positions. */
int chunk_be_promote_up_norebuild(chunk_backend *be, int index, const char *new_path, const uint8_t expect_id[16],
                                  const uint8_t *expect_db, const chunk_heal_opts *o)
{
    if (!be || index < 0 || index >= chunk_be_nstores(be))
        return TSDB_ERR_INVALID;
    int stt = chunk_be_store_state(be, index);
    if (stt != CHUNK_STORE_REPLACING) {
        if (!new_path)
            return TSDB_ERR_INVALID;
        chunk_be_release_down(be, index);
        chunk_store *ns = NULL;
        int rc = chunk_store_open_db_quiet(new_path, expect_id, expect_db, &ns);
        if (rc)
            return rc;
        rc = fence_new_store(be, ns);
        if (rc == 0)
            rc = install(be, index, ns, new_path);
        if (rc) {
            chunk_store_close(ns);
            return rc;
        }
    }
    if (o && o->before_up) {
        int brc = o->before_up(o->before_up_ctx, index);
        if (brc != 0)
            return brc;
    }
    pthread_mutex_lock(&be->st_mu);
    if (atomic_load_explicit(&be->state[index], memory_order_relaxed) == CHUNK_STORE_REPLACING)
        atomic_store_explicit(&be->state[index], CHUNK_STORE_UP, memory_order_release);
    pthread_mutex_unlock(&be->st_mu);
    return 0;
}
#endif
