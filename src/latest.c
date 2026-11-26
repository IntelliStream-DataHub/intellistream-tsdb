/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The latest row of a series (docs/design-latest.md, step 1: the slow path alone; there is no table yet).
 *
 * latest_at answers, for one series and an end bound, "the row with the greatest timestamp below `end` whose
 * last-write-wins value is not null" (or, with_nulls, the greatest timestamp whatever its value). It is defined
 * by the existing queries: the answer equals the last non-null row (the last row) of an ascending raw query over
 * [cutoff, end), which is what tests/test_latest.c compares it with.
 *
 * It does not build a snapshot. Each source of the series (a memtable's run, or a part's granules of the series)
 * is a cursor that knows its newest live row below a bound `hi`; the answer is the greatest of those timestamps,
 * resolved among the sources that hold it by priority (memtables above parts, then the higher block). A null
 * that wins at that timestamp hides every older row there, so with the default rule the search goes on below it
 * (`hi = T`). Parts are visited newest first (by their last timestamp) and only while one of them can still hold
 * a row at or above the best timestamp so far, so a series that is written regularly looks at the newest
 * partition alone, and an expired part is never looked at (its last timestamp is below the series' cutoff). Per
 * part the series is found by an O(1) check of the part's first and last series id, then a binary search of its
 * run table. Per source one granule is looked at at a time: a granule below the bound that no tombstone touches
 * gives its newest timestamp from the index without being decoded; it is decoded only when its rows decide
 * (the value of the winner, a granule the bound cuts, a granule a tombstone touches, a null that hides a row).
 *
 * Tombstones are found by a binary search in the trailers of the parts that hold any (a part's tombstones hide
 * rows of lower blocks of its partition), not by a scan of every part's tombstones per series.
 *
 * One cut of the engine (cut_take) serves a whole chunk of series: the parts and memtables as queries see them
 * (the same visibility rules), taken after the chunk's cutoffs are read, as snapshot_take callers do.
 */
#include "internal.h"

#include <stdlib.h>
#include <string.h>

typedef struct lsrc {
    const part *p; /* NULL: a memtable run */
    uint64_t prio; /* higher wins on equal timestamps */
    bool have;     /* T is the newest live row below the bound */
    int64_t T;
    /* rows [0, n) of ts/v are the candidates left: a memtable's run, or the decoded granule `cur` of a part */
    const int64_t *ts;
    const uint8_t *v;
    size_t n;
    bool dec;                /* ts/v are the decoded granule (part) */
    int64_t *dts;            /* reusable owned buffers of dec (persist across series in lctx) */
    uint8_t *dv;
    size_t dts_cap;          /* capacity of dts in int64_t elements */
    size_t dv_cap;           /* capacity of dv in bytes */
    /* a part: granules [gi, gk) are not yet looked at; cur is the one in hand */
    uint32_t gi, gk, cur;
    bool idx;                /* cur is a plain granule below the bound: T is its ts_max, not decoded */
    tomb_set h;              /* tombstones of the series that hide this part's rows (reused across series) */
} lsrc;

typedef struct lctx {
    eng *db;
    qcut c;
    int type;
    size_t w;
    uint64_t null;
    part **ord; /* the cut's parts by last timestamp, newest first */
    size_t np;
    part **tp;  /* the parts with tombstones, by (partition, min_block) */
    size_t ntp;
    codec_ctx *cc;
    lsrc *src;
    size_t ns, cap;
} lctx;

static int cmp_part_ts(const void *a, const void *b)
{
    const part *x = *(part *const *)a, *y = *(part *const *)b;
    if (x->max_ts != y->max_ts)
        return x->max_ts < y->max_ts ? 1 : -1;
    return (x->max_block < y->max_block) - (x->max_block > y->max_block);
}

static int cmp_part_tomb(const void *a, const void *b)
{
    const part *x = *(part *const *)a, *y = *(part *const *)b;
    if (x->partition != y->partition)
        return x->partition < y->partition ? -1 : 1;
    return (x->min_block > y->min_block) - (x->min_block < y->min_block);
}

static void src_reset(lsrc *s)
{
    s->p = NULL;
    s->prio = 0;
    s->have = false;
    s->T = 0;
    s->ts = NULL;
    s->v = NULL;
    s->n = 0;
    s->dec = false;
    s->gi = s->gk = s->cur = 0;
    s->idx = false;
    s->h.n = 0;
}

static void src_release(lsrc *s)
{
    free(s->dts);
    free(s->dv);
    tomb_set_free(&s->h);
    memset(s, 0, sizeof *s);
}

static void ctx_reset_series(lctx *x)
{
    for (size_t i = 0; i < x->ns; i++)
        src_reset(&x->src[i]);
    x->ns = 0;
}

static lsrc *ctx_new_src(lctx *x)
{
    if (x->ns == x->cap) {
        size_t cap = x->cap ? 2 * x->cap : 8;
        lsrc *s = realloc(x->src, cap * sizeof *s);
        if (!s)
            return NULL;
        memset(s + x->cap, 0, (cap - x->cap) * sizeof *s);
        x->src = s;
        x->cap = cap;
    }
    return &x->src[x->ns++];
}

/* The tombstones of `series` that hide rows of p: those of the parts of p's partition with a higher block. */
static int tombs_for(const lctx *x, const part *p, uint32_t series, tomb_set *h)
{
    size_t lo = 0, hi = x->ntp; /* the first part after (partition, max_block) */
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        const part *q = x->tp[mid];
        if (q->partition < p->partition || (q->partition == p->partition && q->min_block <= p->max_block))
            lo = mid + 1;
        else
            hi = mid;
    }
    int rc = 0;
    for (size_t i = lo; rc == 0 && i < x->ntp && x->tp[i]->partition == p->partition; i++) {
        const part *q = x->tp[i];
        uint32_t a = 0, b = q->nt;
        while (a < b) { /* the first tombstone of a series >= `series` */
            uint32_t mid = a + (b - a) / 2;
            if (q->t[mid].series < series)
                a = mid + 1;
            else
                b = mid;
        }
        for (; rc == 0 && a < q->nt && q->t[a].series == series; a++)
            rc = tomb_set_add(h, &q->t[a], 1);
    }
    tomb_set_finish(h);
    return rc;
}

/* Decodes granule s->cur whole, without the rows its tombstones hide, as the source's rows. Reuses s->dts/s->dv. */
static int src_decode(lctx *x, lsrc *s, uint32_t series)
{
    uint32_t cnt = gx_count(s->p, s->cur);
    size_t need_ts = cnt ? cnt : 1;
    size_t need_v = need_ts * x->w;
    if (need_ts > s->dts_cap) {
        size_t cap = s->dts_cap ? s->dts_cap * 2 : 256;
        while (cap < need_ts)
            cap *= 2;
        int64_t *ts = realloc(s->dts, cap * sizeof *ts);
        if (!ts)
            return TSDB_ERR_NOMEM;
        s->dts = ts;
        s->dts_cap = cap;
    }
    if (need_v > s->dv_cap) {
        size_t cap = s->dv_cap ? s->dv_cap * 2 : 2048;
        while (cap < need_v)
            cap *= 2;
        uint8_t *v = realloc(s->dv, cap);
        if (!v)
            return TSDB_ERR_NOMEM;
        s->dv = v;
        s->dv_cap = cap;
    }
    granule gb;
    int rc = granule_read_at(x->cc, s->p, s->cur, x->type, s->dts, s->dv, cnt, s->p->verify, &gb);
    if (rc)
        return rc;
    size_t n = gb.count < cnt ? gb.count : cnt;
    if (s->h.n)
        n = tomb_set_filter(&s->h, series, INT64_MIN, s->dts, s->dv, x->w, n);
    s->ts = s->dts;
    s->v = s->dv;
    s->n = n;
    s->dec = true;
    s->idx = false;
    return 0;
}

/* The source's newest live row in [lo, hi): sets have and T. */
static int src_next(lctx *x, lsrc *s, uint32_t series, int64_t lo, int64_t hi)
{
    s->have = false;
    for (;;) {
        if (s->dec || !s->p) {
            while (s->n && s->ts[s->n - 1] >= hi)
                s->n--;
            if (s->n && s->ts[s->n - 1] >= lo) {
                s->T = s->ts[s->n - 1];
                s->have = true;
                return 0;
            }
            if (!s->p)
                return 0;
            s->dec = false; /* this granule has nothing more: the one below */
            s->n = 0;
            continue;
        }
        if (s->idx) {
            int64_t tmax = gx_ts_max(s->p, s->cur);
            if (tmax < hi) {
                s->T = tmax;
                s->have = true;
                return 0;
            }
            int rc = src_decode(x, s, series); /* the bound cuts the granule */
            if (rc)
                return rc;
            continue;
        }
        if (s->gk <= s->gi)
            return 0;
        uint32_t g = --s->gk;
        int64_t tmin, tmax;
        gx_ts(s->p, g, &tmin, &tmax);
        if (tmin >= hi)
            continue;
        if (tmax < lo) {
            s->gk = s->gi;
            return 0;
        }
        s->cur = g;
        if (tmax < hi && !(s->h.n && tomb_set_hits(&s->h, series, tmin, tmax))) {
            s->idx = true;
            continue;
        }
        int rc = src_decode(x, s, series);
        if (rc)
            return rc;
    }
}

/* The value slot of the source's row at T (its newest): a plain granule is decoded now. */
static int src_slot(lctx *x, lsrc *s, uint32_t series, uint64_t *out)
{
    if (s->p && !s->dec) {
        int rc = src_decode(x, s, series);
        if (rc)
            return rc;
    }
    if (!s->n)
        return TSDB_ERR_CORRUPT; /* the index promised a row the granule does not hold */
    *out = slot_get(s->v, x->w, s->n - 1);
    return 0;
}

/* Starts a source for part p if it holds the series in [lo, hi). */
static int part_activate(lctx *x, const part *p, uint32_t series, int64_t lo, int64_t hi, bool *added)
{
    *added = false;
    const gix *ix = &p->ix;
    if (!ix->nruns || series < ix->rser[0] || series > ix->rser[ix->nruns - 1])
        return 0;
    uint32_t gi, gj;
    part_find(p, series, lo, hi, &gi, &gj);
    if (gi >= gj)
        return 0;
    lsrc *s = ctx_new_src(x);
    if (!s)
        return TSDB_ERR_NOMEM;
    s->p = p;
    s->prio = p->max_block;
    s->gi = gi;
    s->gk = gj;
    int rc = x->ntp ? tombs_for(x, p, series, &s->h) : 0;
    if (rc == 0)
        rc = src_next(x, s, series, lo, hi);
    *added = true;
    return rc;
}

typedef struct latest_hit {
    int kind; /* TSDB_LATEST_* */
    int64_t ts;
    uint64_t slot;
} latest_hit;

enum { LK_VALUE = 0, LK_NULL = 1, LK_NONE = 2 }; /* tsdb_latest_kind */

static int latest_at(lctx *x, uint32_t series, int64_t lo, int64_t end, bool with_nulls, const mem_run *runs, size_t nm,
                     latest_hit *out)
{
    *out = (latest_hit){LK_NONE, 0, 0};
    if (lo >= end)
        return 0;
    int rc = 0;
    for (size_t i = 0; rc == 0 && i < nm; i++) {
        if (!runs[i].n)
            continue;
        lsrc *s = ctx_new_src(x);
        if (!s) {
            rc = TSDB_ERR_NOMEM;
            break;
        }
        s->prio = UINT64_MAX - (nm - i);
        s->ts = runs[i].ts;
        s->v = runs[i].v;
        s->n = runs[i].n;
        rc = src_next(x, s, series, lo, end);
    }
    int64_t hi = end;
    size_t next = 0;
    while (rc == 0) {
        bool haveM = false;
        int64_t M = 0;
        for (size_t i = 0; i < x->ns; i++)
            if (x->src[i].have && (!haveM || x->src[i].T > M)) {
                haveM = true;
                M = x->src[i].T;
            }
        /* a part whose last timestamp is below M holds no row at M or above */
        while (rc == 0 && next < x->np) {
            const part *p = x->ord[next];
            if (p->max_ts < lo) {
                next = x->np;
                break;
            }
            if (haveM && p->max_ts < M)
                break;
            next++;
            bool added;
            rc = part_activate(x, p, series, lo, hi, &added);
            if (rc == 0 && added) {
                lsrc *s = &x->src[x->ns - 1];
                if (s->have && (!haveM || s->T > M)) {
                    haveM = true;
                    M = s->T;
                }
            }
        }
        if (rc || !haveM)
            break;
        lsrc *best = NULL;
        for (size_t i = 0; i < x->ns; i++) {
            lsrc *s = &x->src[i];
            if (s->have && s->T == M && (!best || s->prio > best->prio))
                best = s;
        }
        uint64_t slot;
        rc = src_slot(x, best, series, &slot);
        if (rc)
            break;
        bool isnull = slot == x->null;
        if (!isnull || with_nulls) {
            *out = (latest_hit){isnull ? LK_NULL : LK_VALUE, M, slot};
            break;
        }
        hi = M; /* a null won at M: every older row there is hidden, the search goes on below */
        for (size_t i = 0; rc == 0 && i < x->ns; i++)
            if (x->src[i].have && x->src[i].T == M)
                rc = src_next(x, &x->src[i], series, lo, hi);
    }
    ctx_reset_series(x);
    return rc;
}

/* The search of n ids on the cut `c` (which the caller owns and releases), with the lower bounds lo[] (the cutoffs,
 * read before the cut). out[i].status/type are set by the caller; ids with a status are skipped. */
enum { LM_SLICE = 64, LM_SLICE_ROWS = 4096 };
static int search_on_cut(eng *db, const qcut *c, const uint32_t *local, size_t n, const int64_t *lo,
                         const int64_t *ends, int64_t end, bool with_nulls, latest_row *out)
{
    if (n == 0)
        return 0;
    lctx x = {0};
    x.db = db;
    x.c = *c;
    int rc = 0;
    x.np = x.c.np;
    size_t nm = x.c.ntails + x.c.nms;
    bool need_lock = x.c.ntails || x.c.has_active;
    size_t slice_cap = need_lock ? MIN(n ? n : 1, (size_t)LM_SLICE) : 1;
    x.ord = malloc((x.np ? x.np : 1) * sizeof *x.ord);
    x.tp = malloc((x.np ? x.np : 1) * sizeof *x.tp);
    mem_run *sruns = nm ? calloc(slice_cap * nm, sizeof *sruns) : NULL;
    bool *ssorted = (nm && need_lock) ? calloc(slice_cap * nm, sizeof *ssorted) : NULL;
    if (!x.ord || !x.tp || (nm && !sruns) || (nm && need_lock && !ssorted))
        rc = TSDB_ERR_NOMEM;
    if (rc == 0) {
        memcpy(x.ord, x.c.parts, x.np * sizeof *x.ord);
        qsort(x.ord, x.np, sizeof *x.ord, cmp_part_ts);
        for (size_t i = 0; i < x.np; i++)
            if (x.c.parts[i]->nt)
                x.tp[x.ntp++] = x.c.parts[i];
        qsort(x.tp, x.ntp, sizeof *x.tp, cmp_part_tomb);
        x.cc = cc_get(db);
        if (!x.cc)
            rc = TSDB_ERR_NOMEM;
    }
    size_t nimm = x.c.nms - (x.c.has_active ? 1 : 0);
    for (size_t a = 0; rc == 0 && a < n;) {
        size_t b = a;
        if (nm && need_lock) {
            size_t copied = 0;
            pthread_mutex_lock(&db->buf_lock);
            while (b < n && (b - a) < slice_cap && (b == a || copied < LM_SLICE_ROWS)) {
                size_t si = b - a;
                if (out[b].status == 0) {
                    int64_t lim = ends ? ends[b] : end;
                    if (lo[b] < lim) {
                        for (size_t j = 0; rc == 0 && j < x.c.ntails; j++) {
                            mem_run *r = &sruns[si * nm + j];
                            rc = mt_copy_raw(x.c.tails[j], local[b], lo[b], lim, x.c.tail_visible, r,
                                             &ssorted[si * nm + j]);
                            copied += r->n;
                        }
                        if (rc == 0 && x.c.has_active) {
                            size_t j = nm - 1;
                            mem_run *r = &sruns[si * nm + j];
                            rc = mt_copy_raw(x.c.ms[x.c.nms - 1], local[b], lo[b], lim, x.c.visible, r,
                                             &ssorted[si * nm + j]);
                            copied += r->n;
                        }
                    }
                }
                b++;
                if (rc)
                    break;
            }
            pthread_mutex_unlock(&db->buf_lock);
        } else {
            b = a + 1;
        }
        for (size_t i = a; i < b; i++) {
            size_t si = need_lock ? (i - a) : 0;
            mem_run *runs = nm ? &sruns[si * nm] : NULL;
            if (rc == 0 && out[i].status == 0) {
                x.type = out[i].type;
                x.w = type_width(x.type);
                x.null = type_null(x.type);
                int64_t lim = ends ? ends[i] : end;
                if (nm && lo[i] < lim) {
                    if (need_lock) {
                        const bool *sorted = &ssorted[si * nm];
                        for (size_t j = 0; rc == 0 && j < x.c.ntails; j++)
                            rc = mt_finish(&runs[j], x.w, sorted[j]);
                        if (rc == 0 && x.c.has_active)
                            rc = mt_finish(&runs[nm - 1], x.w, sorted[nm - 1]);
                    }
                    for (size_t j = 0; rc == 0 && j < nimm; j++)
                        rc = mt_collect(x.c.ms[j], local[i], x.w, lo[i], lim, x.c.visible, &runs[x.c.ntails + j]);
                }
                if (rc == 0) {
                    latest_hit h;
                    rc = latest_at(&x, local[i], lo[i], lim, with_nulls, runs, nm, &h);
                    if (rc == 0) {
                        out[i].kind = (uint8_t)h.kind;
                        out[i].ts = h.ts;
                        out[i].slot = h.slot;
                    }
                }
            }
            for (size_t j = 0; j < nm; j++)
                mem_run_free(&runs[j]);
        }
        a = b;
    }
    if (x.cc)
        cc_put(db, x.cc);
    for (size_t i = 0; i < x.cap; i++)
        src_release(&x.src[i]);
    free(x.src);
    free(x.ord);
    free(x.tp);
    free(sruns);
    free(ssorted);
    return rc;
}

/* The slow path for n ids on one cut of the engine: no table, every id searched (also the table path's searches). */
int eng_latest_search(eng *db, const uint32_t *local, size_t n, const int64_t *ends, int64_t end, bool with_nulls,
                      latest_row *out)
{
    int64_t *lo = malloc((n ? n : 1) * sizeof *lo);
    if (!lo)
        return TSDB_ERR_NOMEM;
    bool has_ret = atomic_load_explicit(&db->has_retention, memory_order_relaxed);
    int64_t now = has_ret ? eng_now_ms(db) : 0;
    for (size_t i = 0; i < n; i++) {
        out[i] = (latest_row){.kind = LK_NONE};
        int ty = eng_series_type(db, local[i]);
        if (ty < 0)
            out[i].status = TSDB_ERR_NOTFOUND;
        else
            out[i].type = (uint8_t)ty;
        lo[i] = ty < 0 ? 0 : (has_ret ? eng_cutoff_at(db, local[i], now) : INT64_MIN); /* before the cut, as a query reads it */
    }
    qcut c;
    int rc = cut_take(db, &c);
    if (rc) {
        free(lo);
        return rc;
    }
    rc = search_on_cut(db, &c, local, n, lo, ends, end, with_nulls, out);
    cut_release(&c);
    free(lo);
    return rc;
}

/* The warmer's fill (ltab.c, 3.8): for each id the newest non-null row in the PARTS of the cut, whatever the cutoff
 * (the table keeps what the parts hold; lookups apply the cutoff). The caller's cut has no memtable sources. */
int eng_latest_fill(eng *db, const qcut *c, const uint32_t *local, size_t n, latest_row *out)
{
    int64_t *lo = malloc((n ? n : 1) * sizeof *lo);
    if (!lo)
        return TSDB_ERR_NOMEM;
    for (size_t i = 0; i < n; i++) {
        out[i] = (latest_row){.kind = LK_NONE};
        int ty = eng_series_type(db, local[i]);
        if (ty < 0)
            out[i].status = TSDB_ERR_NOTFOUND;
        else
            out[i].type = (uint8_t)ty;
        lo[i] = INT64_MIN;
    }
    int rc = search_on_cut(db, c, local, n, lo, NULL, INT64_MAX, false, out);
    free(lo);
    return rc;
}
