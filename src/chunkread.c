/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Reading an object back from its chunks, with decode (design-cluster-data.md §3.1-3.2, E2, E10, E11, E26).
 *
 * chunk_read_range works one stripe at a time over a chunk_source (files here, memory in tests and in fuzz_ec, the
 * network in R8). The healthy path reads the cells a byte range needs from their data chunks. A cell that is absent,
 * cannot be read or fails its sum is an erasure: the stripe is rebuilt from any k' good cells of its k' + m, data
 * cells first, then parity, and a data cell past the end of the object counts as a good cell of zeros. Fewer than k'
 * good cells is TSDB_ERR_CORRUPT naming the object, never wrong bytes.
 *
 * What chooses the good cells is the encode-time sum of each cell (E26): a 64-bit xxh64 per cell, from the object
 * record when the caller has it (which also catches a chunk of another encoding that is self-consistent), else from
 * the chunk's own table. A wrong survivor decodes to a plausible wrong answer, so nothing is ever tried and checked
 * after the fact by a short sum. The geometry is the object's own (the caller's record, or the headers), never today's
 * settings (E2).
 *
 * Two modes. The default reads every cell it uses whole and checks it, which is what a scrub, a repair or the second
 * try of a reader wants. CHUNK_RR_RAW reads only the bytes asked for and, when a cell fails to read, the same byte
 * range of the others (Reed-Solomon works column by column, so a 10 KB granule costs 10 KB from each of k' cells, not
 * k' MiB): it cannot see rot, so its caller must check what it gets (a granule's crc, E11) and ask again without RAW
 * when that fails.
 */
#include "internal.h"
#include "chunk.h"
#include "chunk_priv.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef TSDB_EC
#include "ec/rs.h"
#endif

enum { PS_UNKNOWN = 0, PS_GOOD, PS_BAD };

typedef struct rr {
    const chunk_source *src;
    const chunk_geom *g;
    const uint64_t *rec;
    chunk_report *rep;
    bool raw;
    uint64_t base[CHUNK_POS_MAX];
    uint8_t *slot[CHUNK_POS_MAX]; /* a buffer of one cell per position, made when first used, kept for the call */
    size_t cap;
    uint8_t zero_[1];
} rr;

static void note(rr *r, unsigned pos, int st)
{
    r->rep->st[pos] = (uint8_t)st;
    r->rep->bad_cells++;
}

static uint8_t *rr_slot(rr *r, unsigned pos)
{
    if (!r->slot[pos])
        r->slot[pos] = malloc(r->cap ? r->cap : 1);
    return r->slot[pos];
}

/* Reads cell `idx` of chunk `pos` whole into dst (len bytes) and checks it: 0 or a CHUNK_ST_* status. */
static int fetch_checked(rr *r, unsigned pos, uint64_t idx, size_t len, uint8_t *dst)
{
    uint64_t want;
    if (r->rec) {
        want = r->rec[r->base[pos] + idx];
    } else {
        int st = 0;
        const uint64_t *tab = r->src->sums(r->src->ctx, pos, &st);
        if (!tab)
            return st ? st : CHUNK_ST_IOERR;
        want = tab[idx];
    }
    int st = r->src->read(r->src->ctx, pos, idx * r->g->cell, dst, len);
    if (st)
        return st;
    if (xxh64(dst, len, 0) != want && chunk_mutant != CHUNK_MUT_NOSUMS) /* planted: sums not checked */
        return CHUNK_ST_DAMAGED;
    return 0;
}

static void copy_out(const uint8_t *cell_bytes, uint64_t x, uint64_t y, uint8_t *dst)
{
    memcpy(dst, cell_bytes + x, (size_t)(y - x));
}

/* The stripe `s` has needed cells that failed (bad[i]): rebuilds them from the others into dst. */
static int degrade(rr *r, uint64_t s, uint64_t off, const bool *bad, const bool *have, const uint64_t *xs,
                   const uint64_t *ys, uint8_t *buf)
{
#ifndef TSDB_EC
    (void)r, (void)s, (void)off, (void)bad, (void)have, (void)xs, (void)ys, (void)buf;
    return TSDB_ERR_INVALID;
#else
    const chunk_geom *g = r->g;
    unsigned k = g->k, m = g->m;
    uint64_t ncells = chunk_ncells(g), Ls = chunk_stripe_len(g, s);
    uint64_t x0 = Ls, y0 = 0;
    for (unsigned i = 0; i < k; i++)
        if (bad[i]) {
            if (xs[i] < x0)
                x0 = xs[i];
            if (ys[i] > y0)
                y0 = ys[i];
        }
    if (!r->raw)
        x0 = 0, y0 = Ls; /* whole cells, each checked */
    size_t w = (size_t)(y0 - x0);
    uint8_t *shards[CHUNK_POS_MAX];
    bool present[CHUNK_POS_MAX], isbad[CHUNK_POS_MAX];
    unsigned ngood = 0;
    memset(shards, 0, sizeof shards);
    memset(isbad, 0, sizeof isbad);
    for (unsigned i = 0; i < k; i++)
        isbad[i] = bad[i];
    for (unsigned p = 0; p < k + m; p++)
        present[p] = false;
    int sts[CHUNK_POS_MAX] = {0};
    for (unsigned p = 0; p < k + m && ngood < k; p++) { /* data first, then parity */
        if (isbad[p])
            continue;
        uint64_t c = p < k ? s * k + p : 0, cl = p < k ? chunk_cell_len(g, c) : Ls;
        uint8_t *b = rr_slot(r, p);
        if (!b)
            return TSDB_ERR_NOMEM;
        if (!r->raw && p < k && have[p]) { /* the direct pass read and checked it whole: only the padding is left */
            memset(b + cl, 0, w - (size_t)cl);
        } else if (p < k && c >= ncells) { /* beyond the object: zeros, no read */
            memset(b, 0, w);
            r->rep->st[p] = CHUNK_ST_VIRTUAL;
        } else if (r->raw) {
            size_t avail = x0 < cl ? (size_t)((y0 < cl ? y0 : cl) - x0) : 0;
            int st = avail ? r->src->read(r->src->ctx, p, s * g->cell + x0, b, avail) : 0;
            if (st) {
                sts[p] = st;
                note(r, p, st);
                continue;
            }
            memset(b + avail, 0, w - avail);
        } else {
            int st = fetch_checked(r, p, s, (size_t)cl, b);
            if (st) {
                sts[p] = st;
                note(r, p, st);
                continue;
            }
            memset(b + cl, 0, w - (size_t)cl);
        }
        shards[p] = b;
        present[p] = true;
        ngood++;
    }
    if (ngood < k) {
        unsigned absent = 0, damaged = 0, io = 0;
        for (unsigned p = 0; p < k + m; p++)
            absent += sts[p] == CHUNK_ST_ABSENT, damaged += sts[p] == CHUNK_ST_DAMAGED, io += sts[p] == CHUNK_ST_IOERR;
        unsigned nbad = 0;
        for (unsigned p = 0; p < k + m; p++)
            nbad += isbad[p] || sts[p];
        if (!chunk_log_quiet)
            fprintf(stderr, "tsdb: object %s: stripe %llu has %u good chunks of the %u it needs (%u of %u positions "
                        "unusable: %u absent, %u damaged, %u unreadable): not readable\n", r->src->name,
                (unsigned long long)s, ngood, k, nbad, k + m, absent, damaged, io);
        return TSDB_ERR_CORRUPT;
    }
    /* the buffers the decode fills: every absent data position gets one of its own */
    for (unsigned p = 0; p < k + m; p++)
        if (!present[p]) {
            shards[p] = rr_slot(r, p);
            if (!shards[p])
                return TSDB_ERR_NOMEM;
        }
    iso_rs *rs = chunk_rs_get(k, m);
    if (!rs)
        return TSDB_ERR_NOMEM;
    if (iso_rs_decode_data(rs, shards, present, w) != ISO_OK)
        return TSDB_ERR_CORRUPT;
    for (unsigned i = 0; i < k; i++) {
        if (!bad[i])
            continue;
        uint64_t c = s * k + i;
        if (!r->raw && r->rec && xxh64(shards[i], (size_t)chunk_cell_len(g, c), 0) != r->rec[r->base[i] + s] &&
            chunk_mutant != CHUNK_MUT_NOSUMS) {
            if (!chunk_log_quiet)
                fprintf(stderr, "tsdb: object %s: stripe %llu rebuilt cell %u does not match its encode-time sum: the "
                            "chunks are not one encoding\n", r->src->name, (unsigned long long)s, i);
            return TSDB_ERR_CORRUPT;
        }
        copy_out(shards[i], xs[i] - x0, ys[i] - x0, buf + (c * g->cell + xs[i] - off));
        r->rep->decoded_cells++;
    }
    return 0;
#endif
}

int chunk_read_range(const chunk_source *src, const chunk_geom *g, const uint64_t *rec_sums, uint64_t off,
                     uint64_t len, void *vbuf, unsigned flags, chunk_report *rep)
{
    chunk_report local;
    if (!rep)
        rep = &local;
    memset(rep, 0, sizeof *rep);
    if (!chunk_geom_valid(g) || off > g->len || len > g->len - off)
        return TSDB_ERR_INVALID;
    if (len == 0)
        return 0;
    uint8_t *buf = vbuf;
    rr r;
    memset(&r, 0, sizeof r);
    r.src = src;
    r.g = g;
    r.rec = rec_sums;
    r.rep = rep;
    r.raw = (flags & CHUNK_RR_RAW) != 0;
    r.cap = g->cell < g->len ? g->cell : (size_t)g->len;
    for (unsigned p = 0; p < chunk_npos(g); p++)
        r.base[p] = chunk_sums_base(g, p);
    unsigned k = g->k;
    uint64_t c0 = off / g->cell, c1 = (off + len - 1) / g->cell;
    int rc = 0;
    for (uint64_t s = c0 / k; s <= c1 / k && rc == 0; s++) {
        bool bad[CHUNK_K_MAX] = {false}, have[CHUNK_K_MAX] = {false};
        uint64_t xs[CHUNK_K_MAX] = {0}, ys[CHUNK_K_MAX] = {0};
        bool any = false;
        for (unsigned i = 0; i < k; i++) {
            uint64_t c = s * k + i;
            if (c < c0 || c > c1)
                continue;
            uint64_t cs = c * g->cell, cl = chunk_cell_len(g, c);
            uint64_t a = off > cs ? off : cs, b = off + len < cs + cl ? off + len : cs + cl;
            xs[i] = a - cs;
            ys[i] = b - cs;
            uint8_t *dst = buf + (a - off);
            int st;
            if (r.raw) {
                st = src->read(src->ctx, i, s * g->cell + xs[i], dst, (size_t)(ys[i] - xs[i]));
            } else {
                uint8_t *slot = rr_slot(&r, i);
                st = slot ? fetch_checked(&r, i, s, (size_t)cl, slot) : CHUNK_ST_IOERR;
                if (st == 0) {
                    copy_out(slot, xs[i], ys[i], dst);
                    have[i] = true; /* whole and checked: a source for the others' rebuild */
                }
            }
            if (st) {
                note(&r, i, st);
                bad[i] = true;
                any = true;
            }
        }
        if (any)
            rc = degrade(&r, s, off, bad, have, xs, ys, buf);
    }
    for (unsigned p = 0; p < CHUNK_POS_MAX; p++)
        free(r.slot[p]);
    return rc;
}

/* ---------------------------------------------------------------- an object in a backend */
struct chunk_obj {
    chunk_geom g;
    chunk_rd *rd[CHUNK_POS_MAX];
    int st[CHUNK_POS_MAX];
    const uint64_t *rec;
    char name[40];
};

static int obj_read(void *ctx, unsigned pos, uint64_t off, void *buf, size_t len)
{
    chunk_obj *o = ctx;
    if (pos >= CHUNK_POS_MAX)
        return CHUNK_ST_IOERR;
    if (!o->rd[pos])
        return o->st[pos] ? o->st[pos] : CHUNK_ST_ABSENT;
    return chunk_rd_read(o->rd[pos], off, buf, len) == 0 ? 0 : CHUNK_ST_IOERR;
}

static const uint64_t *obj_sums(void *ctx, unsigned pos, int *status)
{
    chunk_obj *o = ctx;
    if (pos >= CHUNK_POS_MAX || !o->rd[pos]) {
        *status = pos < CHUNK_POS_MAX && o->st[pos] ? o->st[pos] : CHUNK_ST_ABSENT;
        return NULL;
    }
    return chunk_rd_sums(o->rd[pos]);
}

static int st_of(int rc)
{
    return rc == TSDB_ERR_NOTFOUND ? CHUNK_ST_ABSENT : rc == TSDB_ERR_CORRUPT ? CHUNK_ST_DAMAGED : CHUNK_ST_IOERR;
}

int chunk_obj_open(chunk_backend *be, uint32_t volume, const uint8_t vuuid[16], const uint8_t oid[16],
                   const chunk_geom *expect, const uint64_t *rec_sums, chunk_obj **out)
{
    chunk_obj *o = calloc(1, sizeof *o);
    if (!o)
        return TSDB_ERR_NOMEM;
    o->rec = rec_sums;
    chunk_oid_hex(oid, o->name);
    bool have = expect != NULL;
    if (have)
        o->g = *expect;
    bool alive = false;
    int worst = TSDB_ERR_NOTFOUND;
    for (unsigned p = 0; p < CHUNK_POS_MAX; p++) {
        if (have && p >= chunk_npos(&o->g))
            break;
        chunk_ident id = {.volume = volume, .pos = (uint16_t)p};
        memcpy(id.vuuid, vuuid, 16);
        memcpy(id.oid, oid, 16);
        chunk_rd *rd;
        int rc = chunk_be_rd_open(be, &id, &rd);
        if (rc == 0) {
            const chunk_geom *hg = &chunk_rd_hdr(rd)->g;
            if (!have) { /* the geometry is the first valid chunk's: the object's own, recorded in every header */
                o->g = *hg;
                have = true;
            }
            if (memcmp(hg, &o->g, sizeof *hg) != 0) { /* another encoding's chunk: not this object's */
                chunk_rd_close(rd);
                o->st[p] = CHUNK_ST_DAMAGED;
                continue;
            }
            o->rd[p] = rd;
            alive = true;
        } else {
            o->st[p] = st_of(rc);
            if (rc == TSDB_ERR_IO || (rc == TSDB_ERR_CORRUPT && worst != TSDB_ERR_IO))
                worst = rc;
        }
        if (!have && o->st[p] == CHUNK_ST_ABSENT && p + 1 >= CHUNK_POS_MAX)
            break;
    }
    if (!alive || !have) {
        if (worst != TSDB_ERR_NOTFOUND && !chunk_log_quiet)
            fprintf(stderr, "tsdb: object %s: no chunk of it can be read: not readable\n", o->name);
        chunk_obj_close(o);
        return worst == TSDB_ERR_NOTFOUND ? TSDB_ERR_NOTFOUND : TSDB_ERR_CORRUPT;
    }
    if (chunk_mutant == CHUNK_MUT_SETTINGS) { /* planted: the layout taken from today's settings, not from the object */
        chunk_geom t;
        if (chunk_geom_derive(&t, chunk_settings_k, chunk_settings_m, o->g.len) == 0)
            o->g = t;
    }
    *out = o;
    return 0;
}

void chunk_obj_close(chunk_obj *o)
{
    if (!o)
        return;
    for (unsigned p = 0; p < CHUNK_POS_MAX; p++)
        chunk_rd_close(o->rd[p]);
    free(o);
}

const chunk_geom *chunk_obj_geom(const chunk_obj *o) { return &o->g; }

int chunk_obj_source(chunk_obj *o, chunk_source *src)
{
    memset(src, 0, sizeof *src);
    src->ctx = o;
    memcpy(src->name, o->name, sizeof o->name);
    src->read = obj_read;
    src->sums = obj_sums;
    return 0;
}

int chunk_obj_read(chunk_obj *o, uint64_t off, uint64_t len, void *buf, unsigned flags, chunk_report *rep)
{
    chunk_source src;
    chunk_obj_source(o, &src);
    return chunk_read_range(&src, &o->g, o->rec, off, len, buf, flags, rep);
}
