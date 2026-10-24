/*
 * Concurrent writers on the same keys: the model and the checks, shared by
 * test_tsdb (embedded) and test_server (over the network).
 *
 * Writer w = 0..writers-1 makes `rounds` writes, one after the other. Each
 * covers the timestamps j in [w * shift, w * shift + nts) of nseries series
 * (keys: series x timestamp); with shift 0 every write covers the same key
 * set K. The value of a key names the write: code(w, r, key), so every row
 * read says which write it came from. A large insert carries every key twice,
 * a poison value (-code) first: within a batch the later row wins, so a poison
 * value read anywhere means the batch order was lost. Typed runs write nulls
 * at keys picked by (w, r, key), so a null is attributable too.
 *
 * Nothing here needs the commit order, which only the database knows:
 *  - final state: every key holds a value of a write that covers it, from its
 *    writer's last round (a writer's writes are ordered: its last one beats
 *    its earlier ones), or a null where that write put one;
 *  - inserts are atomic and totally ordered: a key x showing writer A means
 *    A's last insert is ordered after every other last insert covering x.
 *    These "B before A" edges between writers must not form a cycle (with full
 *    overlap: every key shows the same insert);
 *  - a query reads one series from a snapshot: when it shows insert (A, r) at
 *    one key, every other key of that insert in the series is there and does
 *    not show an older insert of A; with full overlap, all its rows come from
 *    one insert, and either every key is there or none;
 *  - no stale reads: once (A, r) was acknowledged, or a reader has seen it, a
 *    later query (of any series, for writes that cover all series at once)
 *    does not show an older write of A at a key that (A, r) covers.
 * Appends (one row per call), per-series inserts and replaces are not atomic across keys
 * or series; runs that use them keep the checks that still apply.
 */
#ifndef SAMEKEYS_H
#define SAMEKEYS_H

#include "tsdb.h"

#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define SK_MAX_WRITERS 16
#define SK_MAX_SERIES 8

/* write kinds */
/* SK_REPLACE: per series, tsdb_replace_range of exactly the writer's window, so
 * it leaves the same state as a per-series insert of the window; it checks that
 * the tombstone hides neither its own rows nor newer inserts, and that no reader
 * sees the window empty in between. */
enum { SK_SMALL = 1, SK_LARGE = 2, SK_APPEND = 4, SK_SERIES = 8, SK_REPLACE = 16 };
/* key states */
enum { SK_ABSENT = 0, SK_VALUE = 1, SK_NULL = 2, SK_GARBAGE = 3 };

typedef struct sk_cfg {
    const char *name;
    int writers, rounds;
    uint32_t nseries, nts, shift; /* shift <= nts: the windows cover every timestamp */
    int64_t t0, step;
    int type; /* TSDB_FLOAT32, TSDB_FLOAT64 or TSDB_INT64 */
    bool nulls;
    unsigned kinds; /* SK_* the writers pick from */
} sk_cfg;

typedef struct sk_val {
    int8_t st;
    int64_t code;
} sk_val;

static atomic_int sk_reports; /* detailed messages are capped */

#define SK_FAIL(...)                                                   \
    do {                                                               \
        if (atomic_fetch_add(&sk_reports, 1) < 20)                     \
            fprintf(stderr, "  samekeys: " __VA_ARGS__);               \
    } while (0)

static inline uint32_t sk_span(const sk_cfg *c) { return c->nts + c->shift * (uint32_t)(c->writers - 1); }
static inline uint32_t sk_nkeys(const sk_cfg *c) { return c->nseries * sk_span(c); }
static inline int64_t sk_ts(const sk_cfg *c, uint32_t j) { return c->t0 + (int64_t)j * c->step; }
static inline int64_t sk_end(const sk_cfg *c) { return sk_ts(c, sk_span(c)); }
static inline size_t sk_width(const sk_cfg *c) { return c->type == TSDB_FLOAT32 ? 4 : 8; }
static inline bool sk_covers(const sk_cfg *c, int w, uint32_t j)
{
    uint32_t lo = (uint32_t)w * c->shift;
    return j >= lo && j < lo + c->nts;
}
/* writes that cover every series at once */
static inline bool sk_atomic_kind(int kind) { return kind == SK_SMALL || kind == SK_LARGE; }

static inline uint64_t sk_mix(uint64_t x)
{
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdull;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ull;
    x ^= x >> 33;
    return x;
}

static inline int64_t sk_id(int w, int r) { return (int64_t)w * 65536 + r + 1; }

/* float32 holds 24 bits exactly: the write alone; the wider types add the key */
static inline int64_t sk_code(const sk_cfg *c, int w, int r, uint32_t key)
{
    return c->type == TSDB_FLOAT32 ? sk_id(w, r) : sk_id(w, r) * 1048576 + key;
}

/* Where write (w, r) puts a null; never at its first timestamp of a series,
 * so every write shows itself at some key. */
static inline bool sk_null(const sk_cfg *c, int w, int r, uint32_t key)
{
    uint32_t j = key % sk_span(c);
    return c->nulls && j != (uint32_t)w * c->shift && sk_mix(((uint64_t)w << 48) ^ ((uint64_t)r << 24) ^ key) % 5 == 0;
}

static inline bool sk_decode(const sk_cfg *c, int64_t code, uint32_t key, int *w, int *r)
{
    int64_t id = code;
    if (c->type != TSDB_FLOAT32) {
        if (code <= 0 || code % 1048576 != (int64_t)key)
            return false;
        id = code / 1048576;
    }
    if (id <= 0)
        return false; /* a poison value, or something nobody wrote */
    *w = (int)((id - 1) / 65536);
    *r = (int)((id - 1) % 65536);
    return *w < c->writers && *r < c->rounds;
}

/* ------------------------------------------------------------- batches */

typedef struct sk_batch {
    uint32_t *series;
    int64_t *ts;
    uint8_t *v;     /* float, double or int64_t per the type */
    uint8_t *valid; /* bitmap; every bit set where there is no null */
    size_t n;
} sk_batch;

static void sk_batch_free(sk_batch *b)
{
    free(b->series);
    free(b->ts);
    free(b->v);
    free(b->valid);
    memset(b, 0, sizeof *b);
}

static void sk_put(const sk_cfg *c, sk_batch *b, uint32_t sid, int64_t ts, int64_t code, bool null)
{
    size_t i = b->n++;
    b->series[i] = sid;
    b->ts[i] = ts;
    if (null)
        code = 0;
    if (c->type == TSDB_FLOAT32) {
        float f = (float)code;
        memcpy(b->v + 4 * i, &f, 4);
    } else if (c->type == TSDB_FLOAT64) {
        double d = (double)code;
        memcpy(b->v + 8 * i, &d, 8);
    } else {
        memcpy(b->v + 8 * i, &code, 8);
    }
    if (null)
        b->valid[i >> 3] &= (uint8_t)~(1u << (i & 7));
    else
        b->valid[i >> 3] |= (uint8_t)(1u << (i & 7));
}

/* The rows of write (w, r): of every series, or of series `only` (< nseries).
 * large: every key twice, poison first. order 0: by (series, ts); 1: by
 * (ts, series), what collectors send; 2: shuffled. Poison rows always come
 * before the real ones. */
static void sk_build(const sk_cfg *c, const uint32_t *ids, int w, int r, bool large, int order, uint64_t seed,
                     uint32_t only, sk_batch *b)
{
    uint32_t ns = only < c->nseries ? 1 : c->nseries, s0 = only < c->nseries ? only : 0;
    size_t per = (size_t)ns * c->nts, cap = per * (large ? 2 : 1);
    b->series = malloc(cap * sizeof *b->series);
    b->ts = malloc(cap * sizeof *b->ts);
    b->v = malloc(cap * 8);
    b->valid = calloc(cap / 8 + 1, 1);
    b->n = 0;
    uint32_t *keys = malloc(per * sizeof *keys);
    if (!b->series || !b->ts || !b->v || !b->valid || !keys) {
        fprintf(stderr, "out of memory\n");
        exit(1);
    }
    uint32_t span = sk_span(c), lo = (uint32_t)w * c->shift;
    for (int copy = large ? 0 : 1; copy < 2; copy++) {
        size_t k = 0;
        if (order == 1) {
            for (uint32_t j = lo; j < lo + c->nts; j++)
                for (uint32_t s = s0; s < s0 + ns; s++)
                    keys[k++] = s * span + j;
        } else {
            for (uint32_t s = s0; s < s0 + ns; s++)
                for (uint32_t j = lo; j < lo + c->nts; j++)
                    keys[k++] = s * span + j;
        }
        if (order == 2) {
            uint64_t x = sk_mix(seed + (uint64_t)copy);
            for (size_t i = per - 1; i > 0; i--) {
                x = sk_mix(x + i);
                size_t m = (size_t)(x % (i + 1));
                uint32_t t = keys[i];
                keys[i] = keys[m];
                keys[m] = t;
            }
        }
        for (size_t i = 0; i < per; i++) {
            uint32_t key = keys[i], s = key / span, j = key % span;
            int64_t code = sk_code(c, w, r, key);
            if (copy == 0)
                sk_put(c, b, ids[s], sk_ts(c, j), -code, false);
            else
                sk_put(c, b, ids[s], sk_ts(c, j), code, sk_null(c, w, r, key));
        }
    }
    free(keys);
}

/* ------------------------------------------------------------- reading */

/* Rows of one series into row[span]: a tsdb_values_fn. */
typedef struct sk_collect {
    const sk_cfg *c;
    sk_val *row;
    bool desc;
    bool any;
    int64_t last;
    size_t n;
    int errors;
} sk_collect;

static void sk_collect_begin(sk_collect *k, const sk_cfg *c, sk_val *row, bool desc)
{
    memset(k, 0, sizeof *k);
    k->c = c;
    k->row = row;
    k->desc = desc;
    memset(row, 0, sk_span(c) * sizeof *row);
}

static int sk_collect_cb(void *ctx, const int64_t *ts, const tsdb_values *v, size_t n)
{
    sk_collect *k = ctx;
    const sk_cfg *c = k->c;
    for (size_t i = 0; i < n; i++) {
        int64_t d = ts[i] - c->t0;
        if (k->any && (k->desc ? ts[i] >= k->last : ts[i] <= k->last)) {
            SK_FAIL("%s: rows out of order or repeated at ts %lld\n", c->name, (long long)ts[i]);
            k->errors++;
        }
        k->any = true;
        k->last = ts[i];
        k->n++;
        if (d < 0 || d % c->step || d / c->step >= sk_span(c)) {
            SK_FAIL("%s: a row at ts %lld, which nobody wrote\n", c->name, (long long)ts[i]);
            k->errors++;
            continue;
        }
        sk_val *o = &k->row[d / c->step];
        o->code = 0;
        if (v->valid && !((v->valid[i >> 3] >> (i & 7)) & 1)) {
            o->st = SK_NULL;
            continue;
        }
        if ((int)v->type != c->type) {
            o->st = SK_GARBAGE;
            continue;
        }
        if (c->type == TSDB_INT64) {
            o->st = SK_VALUE;
            o->code = ((const int64_t *)v->values)[i];
            continue;
        }
        double x = c->type == TSDB_FLOAT32 ? ((const float *)v->values)[i] : ((const double *)v->values)[i];
        if (!(x > -4e18 && x < 4e18) || x != (double)(int64_t)x) {
            o->st = SK_GARBAGE;
            continue;
        }
        o->st = SK_VALUE;
        o->code = (int64_t)x;
    }
    return 0;
}

/* ---------------------------------------------------------- reader checks */

/* What one reader has seen so far: the highest round of each writer, per
 * series, and per key (for the writes that are not atomic). */
typedef struct sk_seen {
    int16_t maxr[SK_MAX_SERIES][SK_MAX_WRITERS];
    int16_t *key; /* nkeys x writers */
} sk_seen;

static void sk_seen_init(sk_seen *s, const sk_cfg *c)
{
    for (int i = 0; i < SK_MAX_SERIES; i++)
        for (int w = 0; w < SK_MAX_WRITERS; w++)
            s->maxr[i][w] = -1;
    size_t n = (size_t)sk_nkeys(c) * (size_t)c->writers;
    s->key = malloc(n * sizeof *s->key);
    if (!s->key)
        exit(1);
    for (size_t i = 0; i < n; i++)
        s->key[i] = -1;
}

static void sk_seen_free(sk_seen *s) { free(s->key); }

/* Checks one query of series s (index into the run's series); acked[w]:
 * rounds of writer w acknowledged before the query began, started[w]: rounds
 * it had begun when the query returned. Returns the errors; *id the insert all
 * rows came from (full overlap) or -1, for statistics. */
static int sk_check_snapshot(const sk_cfg *c, uint32_t s, const sk_val *row, const int *acked, const int *started,
                             sk_seen *seen, int64_t *id)
{
    int errors = 0, W = c->writers;
    uint32_t span = sk_span(c);
    bool snap = !(c->kinds & SK_APPEND);               /* every write of the series is one batch */
    bool cross = !(c->kinds & (SK_APPEND | SK_SERIES | SK_REPLACE)); /* ... and of every series together */
    int16_t inq[SK_MAX_WRITERS];
    for (int w = 0; w < W; w++)
        inq[w] = -1;
    *id = -1;
    int64_t one = 0;
    bool same = true;
    uint32_t present = 0;
    for (uint32_t j = 0; j < span; j++) {
        uint32_t key = s * span + j;
        const sk_val *v = &row[j];
        if (v->st == SK_ABSENT)
            continue;
        present++;
        if (v->st == SK_GARBAGE) {
            SK_FAIL("%s: series %u ts %u: a value nobody wrote\n", c->name, s, j);
            errors++;
            continue;
        }
        if (v->st == SK_NULL) {
            bool ok = false;
            for (int w = 0; w < W && !ok; w++)
                for (int r = 0; sk_covers(c, w, j) && r < started[w] && !ok; r++)
                    ok = sk_null(c, w, r, key);
            if (!ok) {
                SK_FAIL("%s: series %u ts %u: a null no write had\n", c->name, s, j);
                errors++;
            }
            continue;
        }
        int w, r;
        if (!sk_decode(c, v->code, key, &w, &r)) {
            SK_FAIL("%s: series %u ts %u: value %lld is %s\n", c->name, s, j, (long long)v->code,
                    v->code < 0 ? "a poison row (the earlier copy of a key in one batch)" : "nobody's");
            errors++;
            continue;
        }
        if (!sk_covers(c, w, j) || r >= started[w] || sk_null(c, w, r, key)) {
            SK_FAIL("%s: series %u ts %u: write (%d, %d) did not put this value here\n", c->name, s, j, w, r);
            errors++;
            continue;
        }
        int16_t *km = &seen->key[(size_t)key * (size_t)W + (size_t)w];
        if (r < *km) {
            SK_FAIL("%s: series %u ts %u: went back from write (%d, %d) to (%d, %d)\n", c->name, s, j, w, *km, w, r);
            errors++;
        }
        if (r > *km)
            *km = (int16_t)r;
        if (r > inq[w])
            inq[w] = (int16_t)r;
        if (one == 0)
            one = sk_id(w, r);
        else if (sk_id(w, r) != one)
            same = false;
    }
    /* A write acknowledged before the query began, or (whole batches) seen at
     * one key of this query or by an earlier one, is at all of its keys of
     * this series, or something newer is: never an older write of its writer. */
    for (int w = 0; w < W; w++) {
        int16_t *mr = &seen->maxr[cross ? 0 : s][w];
        int16_t need = (int16_t)(acked[w] - 1), why = 0;
        if (snap && inq[w] > need) {
            need = inq[w];
            why = 1;
        }
        if (snap && *mr > need) {
            need = *mr;
            why = 2;
        }
        if (need < 0)
            continue;
        static const char *const whys[] = {"acknowledged before the query", "in the same query", "seen before"};
        for (uint32_t j = w * c->shift; j < w * c->shift + c->nts; j++) {
            const sk_val *v = &row[j];
            int vw, vr;
            if (v->st == SK_ABSENT) {
                SK_FAIL("%s: series %u: write (%d, %d) is %s, but ts %u is missing\n", c->name, s, w, need, whys[why],
                        j);
                errors++;
            } else if (v->st == SK_VALUE && sk_decode(c, v->code, s * span + j, &vw, &vr) && vw == w && vr < need) {
                SK_FAIL("%s: series %u ts %u: older write (%d, %d) where (%d, %d) is %s\n", c->name, s, j, w, vr, w,
                        need, whys[why]);
                errors++;
            }
        }
        if (snap)
            *mr = need;
    }
    if (!snap)
        return errors;
    if (c->shift == 0 && present && one == 0) {
        SK_FAIL("%s: series %u: only nulls\n", c->name, s);
        errors++;
    } else if (c->shift == 0 && present) {
        /* full overlap: one insert per series, all of its keys */
        if (!same || present != span) {
            SK_FAIL("%s: series %u: a torn snapshot: %u of %u keys, %s\n", c->name, s, present, span,
                    same ? "one insert" : "several inserts");
            errors++;
        }
        int w = (int)((one - 1) / 65536), r = (int)((one - 1) % 65536);
        for (uint32_t j = 0; same && j < span; j++)
            if ((row[j].st == SK_NULL) != sk_null(c, w, r, s * span + j)) {
                SK_FAIL("%s: series %u ts %u: null does not match insert (%d, %d)\n", c->name, s, j, w, r);
                errors++;
            }
        if (same)
            *id = one;
    }
    return errors;
}

/* An aggregate over all of series s against the insert it says it saw
 * (full overlap, batches only): count, min and max must all be from one. */
static int sk_check_agg(const sk_cfg *c, uint32_t s, const tsdb_bucket *b, size_t nb, const int *started)
{
    if (nb == 0)
        return 0;
    uint32_t span = sk_span(c);
    if (nb != 1) {
        SK_FAIL("%s: series %u: %zu buckets for one\n", c->name, s, nb);
        return 1;
    }
    int64_t lo = c->type == TSDB_INT64 ? b->imin : (int64_t)b->min;
    int64_t hi = c->type == TSDB_INT64 ? b->imax : (int64_t)b->max;
    int w, r, w2, r2;
    /* min is the first known key's code, max the last one's (codes grow with the key) */
    uint32_t kmin = (uint32_t)(lo % 1048576), kmax = (uint32_t)(hi % 1048576);
    if (c->type == TSDB_FLOAT32)
        kmin = kmax = s * span;
    if (lo <= 0 || !sk_decode(c, lo, kmin, &w, &r) || !sk_decode(c, hi, kmax, &w2, &r2) || w != w2 || r != r2 ||
        r >= started[w]) {
        SK_FAIL("%s: series %u: aggregate min %lld max %lld are not from one insert\n", c->name, s, (long long)lo,
                (long long)hi);
        return 1;
    }
    uint64_t count = 0;
    __int128 sum = 0;
    for (uint32_t j = 0; j < span; j++) {
        uint32_t key = s * span + j;
        if (!sk_null(c, w, r, key)) {
            count++;
            sum += sk_code(c, w, r, key);
        }
    }
    bool sum_ok = c->type == TSDB_INT64 ? b->isum == (int64_t)sum : b->sum == (double)sum;
    if (b->count != count || !sum_ok) {
        SK_FAIL("%s: series %u: aggregate count %llu sum %g, insert (%d, %d) has %llu and %g\n", c->name, s,
                (unsigned long long)b->count, b->sum, w, r, (unsigned long long)count, (double)sum);
        return 1;
    }
    return 0;
}

/* ----------------------------------------------------------- final check */

/* closure[i][j]: i is ordered before j */
static bool sk_acyclic(int W, bool before[SK_MAX_WRITERS][SK_MAX_WRITERS], int *at)
{
    for (int k = 0; k < W; k++)
        for (int i = 0; i < W; i++)
            if (before[i][k])
                for (int j = 0; j < W; j++)
                    before[i][j] |= before[k][j];
    for (int i = 0; i < W; i++)
        if (before[i][i]) {
            *at = i;
            return false;
        }
    return true;
}

/* The state after every writer finished: st[nkeys]; last_kind[w] the kind of
 * writer w's last write. Returns the errors. */
static int sk_check_final(const sk_cfg *c, const sk_val *st, const int *last_kind)
{
    int errors = 0, W = c->writers, last = c->rounds - 1;
    uint32_t span = sk_span(c), nk = sk_nkeys(c);
    int *win = malloc(nk * sizeof *win);
    if (!win)
        exit(1);
    for (uint32_t key = 0; key < nk; key++) {
        uint32_t j = key % span;
        const sk_val *v = &st[key];
        win[key] = -1;
        if (v->st == SK_ABSENT || v->st == SK_GARBAGE) {
            SK_FAIL("%s: key %u (series %u ts %u) is %s\n", c->name, key, key / span, j,
                    v->st == SK_ABSENT ? "missing" : "garbage");
            errors++;
        } else if (v->st == SK_NULL) {
            int cand = -1, ncand = 0;
            for (int w = 0; w < W; w++)
                if (sk_covers(c, w, j) && sk_null(c, w, last, key)) {
                    cand = w;
                    ncand++;
                }
            if (ncand == 0) {
                SK_FAIL("%s: key %u: null, but no last write put one there\n", c->name, key);
                errors++;
            } else if (ncand == 1) {
                win[key] = cand;
            }
        } else {
            int w, r;
            if (!sk_decode(c, v->code, key, &w, &r) || !sk_covers(c, w, j) || sk_null(c, w, r, key)) {
                SK_FAIL("%s: key %u: value %lld is not from a write that covers it%s\n", c->name, key,
                        (long long)v->code, v->code < 0 ? " (a poison row)" : "");
                errors++;
            } else if (r != last) {
                SK_FAIL("%s: key %u: shows round %d of writer %d, whose last round %d also wrote it\n", c->name, key,
                        r, w, last);
                errors++;
            } else {
                win[key] = w;
            }
        }
    }
    /* ordering: per series for every batch kind, across series for whole inserts */
    for (int scope = 0; scope <= (int)c->nseries; scope++) {
        bool global = scope == (int)c->nseries;
        bool before[SK_MAX_WRITERS][SK_MAX_WRITERS] = {{false}};
        bool node[SK_MAX_WRITERS];
        for (int w = 0; w < W; w++)
            node[w] = global ? sk_atomic_kind(last_kind[w]) : last_kind[w] != SK_APPEND;
        for (uint32_t key = 0; key < nk; key++) {
            int a = win[key];
            if (a < 0 || !node[a] || (!global && key / span != (uint32_t)scope))
                continue;
            for (int u = 0; u < W; u++)
                if (u != a && node[u] && sk_covers(c, u, key % span))
                    before[u][a] = true;
        }
        int at;
        if (!sk_acyclic(W, before, &at)) {
            SK_FAIL("%s: %s: the keys disagree on the order of the writes (writer %d is before itself)\n", c->name,
                    global ? "across series" : "within a series", at);
            errors++;
            continue;
        }
        /* A null shared by several last writes: one of them must be last among the writes covering the key. When
         * one of them is no node of this scope (a per-series insert or an append across series, an append within
         * one), the scope does not order it: that write may be the newest and its null the one shown, though every
         * other candidate is ordered before another write. Such a key is left to the scope that orders all of its
         * candidates (within its series, unless one is an append). */
        for (uint32_t key = 0; key < nk; key++) {
            if (st[key].st != SK_NULL || win[key] >= 0 || (!global && key / span != (uint32_t)scope))
                continue;
            bool ok = false, any = false, unordered = false;
            for (int cnd = 0; cnd < W && !ok; cnd++) {
                if (!sk_covers(c, cnd, key % span) || !sk_null(c, cnd, last, key))
                    continue;
                if (!node[cnd]) {
                    unordered = true;
                    continue;
                }
                any = true;
                bool later = false;
                for (int u = 0; u < W; u++)
                    later |= u != cnd && node[u] && sk_covers(c, u, key % span) && before[cnd][u];
                ok = !later;
            }
            if (any && !ok && !unordered) {
                SK_FAIL("%s: %s: key %u: null, but every write with that null is ordered before another\n", c->name,
                        global ? "across series" : "within a series", key);
                errors++;
            }
        }
    }
    if (c->shift == 0) {
        bool batches = true;
        for (int w = 0; w < W; w++)
            batches &= sk_atomic_kind(last_kind[w]);
        int first = -1;
        for (uint32_t key = 0; batches && key < nk; key++)
            if (win[key] >= 0 && first < 0)
                first = win[key];
            else if (win[key] >= 0 && win[key] != first) {
                SK_FAIL("%s: key %u shows writer %d, key 0.. writer %d\n", c->name, key, win[key], first);
                errors++;
                break;
            }
    }
    free(win);
    return errors;
}

/* Picks a write kind from c->kinds. */
static int sk_pick(const sk_cfg *c, uint64_t x)
{
    int opts[5], n = 0;
    for (int k = 1; k <= SK_REPLACE; k <<= 1)
        if (c->kinds & (unsigned)k)
            opts[n++] = k;
    return opts[x % (uint64_t)n];
}

#endif
