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
    int64_t *dts;            /* owned buffers of dec */
    uint8_t *dv;
    /* a part: granules [gi, gk) are not yet looked at; cur is the one in hand */
    uint32_t gi, gk, cur;
    bool idx;                /* cur is a plain granule below the bound: T is its ts_max, not decoded */
    tomb_set h;              /* tombstones of the series that hide this part's rows */
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
    mem_run *runs; /* the memtable runs of the series in hand */
    size_t nruns;
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
        src_release(&x->src[i]);
    x->ns = 0;
    for (size_t i = 0; i < x->nruns; i++)
        mem_run_free(&x->runs[i]);
}

static lsrc *ctx_new_src(lctx *x)
{
    if (x->ns == x->cap) {
        size_t cap = x->cap ? 2 * x->cap : 8;
        lsrc *s = realloc(x->src, cap * sizeof *s);
        if (!s)
            return NULL;
        x->src = s;
        x->cap = cap;
    }
    lsrc *s = &x->src[x->ns++];
    memset(s, 0, sizeof *s);
    return s;
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

/* Decodes granule s->cur whole, without the rows its tombstones hide, as the source's rows. */
static int src_decode(lctx *x, lsrc *s, uint32_t series)
{
    uint32_t cnt = gx_count(s->p, s->cur);
    int64_t *ts = malloc((cnt ? cnt : 1) * sizeof *ts);
    uint8_t *v = malloc((cnt ? cnt : 1) * x->w);
    if (!ts || !v) {
        free(ts);
        free(v);
        return TSDB_ERR_NOMEM;
    }
    granule gb;
    gx_load(s->p, s->cur, GW_LOC, &gb);
    int rc = granule_read(x->cc, s->p, &gb, x->type, ts, v, cnt, s->p->verify);
    if (rc) {
        free(ts);
        free(v);
        return rc;
    }
    size_t n = gb.count < cnt ? gb.count : cnt;
    if (s->h.n)
        n = tomb_set_filter(&s->h, series, INT64_MIN, ts, v, x->w, n);
    free(s->dts);
    free(s->dv);
    s->dts = ts;
    s->dv = v;
    s->ts = ts;
    s->v = v;
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

static int latest_at(lctx *x, uint32_t series, int64_t lo, int64_t end, bool with_nulls, latest_hit *out)
{
    *out = (latest_hit){LK_NONE, 0, 0};
    if (lo >= end)
        return 0;
    int rc = 0;
    /* the memtables' rows of the series (tails first, then the flushing ones, then the active one) */
    size_t nm = x->c.ntails + x->c.nms;
    if (nm) {
        bool locked = x->c.ntails || x->c.has_active;
        if (locked)
            pthread_mutex_lock(&x->db->buf_lock);
        for (size_t i = 0; rc == 0 && i < nm; i++) {
            bool tail = i < x->c.ntails;
            const memtable *m = tail ? x->c.tails[i] : x->c.ms[i - x->c.ntails];
            rc = mt_collect(m, series, x->w, lo, end, tail ? x->c.tail_visible : x->c.visible, &x->runs[i]);
        }
        if (locked)
            pthread_mutex_unlock(&x->db->buf_lock);
        for (size_t i = 0; rc == 0 && i < nm; i++) {
            if (!x->runs[i].n)
                continue;
            lsrc *s = ctx_new_src(x);
            if (!s) {
                rc = TSDB_ERR_NOMEM;
                break;
            }
            s->prio = UINT64_MAX - (nm - i);
            s->ts = x->runs[i].ts;
            s->v = x->runs[i].v;
            s->n = x->runs[i].n;
            rc = src_next(x, s, series, lo, end);
        }
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

/* The slow path for n ids on one cut of the engine: no table, every id searched (also the table path's searches). */
int eng_latest_search(eng *db, const uint32_t *local, size_t n, const int64_t *ends, int64_t end, bool with_nulls,
                      latest_row *out)
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
        lo[i] = ty < 0 ? 0 : eng_cutoff(db, local[i]); /* before the cut, as a query reads it */
    }
    lctx x = {0};
    x.db = db;
    int rc = cut_take(db, &x.c);
    if (rc) {
        free(lo);
        return rc;
    }
    x.np = x.c.np;
    x.ord = malloc((x.np ? x.np : 1) * sizeof *x.ord);
    x.tp = malloc((x.np ? x.np : 1) * sizeof *x.tp);
    x.runs = calloc(x.c.ntails + x.c.nms + 1, sizeof *x.runs);
    x.nruns = x.c.ntails + x.c.nms;
    if (!x.ord || !x.tp || !x.runs)
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
    for (size_t i = 0; rc == 0 && i < n; i++) {
        if (out[i].status)
            continue;
        x.type = out[i].type;
        x.w = type_width(x.type);
        x.null = type_null(x.type);
        latest_hit h;
        rc = latest_at(&x, local[i], lo[i], ends ? ends[i] : end, with_nulls, &h);
        if (rc == 0) {
            out[i].kind = (uint8_t)h.kind;
            out[i].ts = h.ts;
            out[i].slot = h.slot;
        }
    }
    if (rc)
        ctx_reset_series(&x);
    if (x.cc)
        cc_put(db, x.cc);
    for (size_t i = 0; i < x.ns; i++)
        src_release(&x.src[i]);
    free(x.src);
    free(x.ord);
    free(x.tp);
    free(x.runs);
    cut_release(&x.c);
    free(lo);
    return rc;
}
