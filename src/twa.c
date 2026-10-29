/*
 * Time-weighted averages.
 *
 * Consecutive readings of a series bound segments; a segment carries the
 * value of its first reading (STEP) or a straight line between both
 * (LINEAR), and covers nothing when its first reading is null (LINEAR: either
 * reading), or when it is longer than the staleness limit. A bucket's result is
 * the integral of the segments over the bucket and the query range, cut
 * exactly at the edges (include/tsdb.h has the contract).
 *
 * Fast path: each granule stores its first and last values and, over the
 * segments between its own rows, both areas, the time they cover and the
 * longest segment (granule_twa, written with the granule). A granule that no
 * other source overlaps, that lies inside the range and inside one bucket, and
 * whose longest segment is within the staleness limit adds its stored area
 * without decoding; only the segment joining it to its neighbour comes from
 * its first and last values. Granules that a bucket edge cuts are decoded in
 * parallel into per-bucket partial sums plus their first and last readings;
 * the joins between pieces are added in time order. Clusters where sources
 * overlap go through the deduplicating merge (last write wins), like the
 * other aggregates.
 *
 * The value in force at the start of the range comes from the last reading
 * before it, and a reading after the end closes the last segment: the
 * snapshot reaches back and forward by the staleness limit (without one, as far
 * as needed), and each source contributes its last granule before the range
 * and its first after it. Rows of those granules only bound segments; the
 * parts of segments outside [start, end) are cut off.
 */
#include "internal.h"
#include "tsdb_protocol.h"

#include <math.h>
#include <stdlib.h>

/* A TWA bucket needs no reading of its own (a segment across it is enough), so two readings far apart can ask
 * for a bucket per millisecond between them: the limit is what one reply can carry over the wire, far below
 * the aggregates' MAX_BUCKETS, and bounds a request's memory (about 100 bytes a bucket) to some 400 MB. */
#define TWA_MAX_BUCKETS (4u << 20)
_Static_assert((uint64_t)TWA_MAX_BUCKETS * TSDB_TWA_ROW + 64 <= TSDB_MAX_FRAME, "a TWA reply must fit a frame");

/* ------------------------------------------------------------- slots */

static inline bool slot_ok(int type, const uint8_t *p)
{
    switch (type) {
    case TSDB_FLOAT64:
    case TSDB_INT64: {
        uint64_t x;
        memcpy(&x, p, 8);
        return x != type_null(type);
    }
    default: return load32(p) != (uint32_t)type_null(type);
    }
}

static inline double slot_val(int type, const uint8_t *p)
{
    switch (type) {
    case TSDB_FLOAT32: {
        float f;
        memcpy(&f, p, 4);
        return f;
    }
    case TSDB_FLOAT64: {
        double d;
        memcpy(&d, p, 8);
        return d;
    }
    case TSDB_INT64: {
        int64_t i;
        memcpy(&i, p, 8);
        return (double)i;
    }
    default: return (double)load32(p);
    }
}

/* ------------------------------------------------- granule statistics */

/* A short granule: the segments in order, as seg() below adds a segment inside one bucket, and the first and last
 * readings from the same loop (main's loop exactly: no reading is loaded twice). */
static inline void twa_seq(int type, const int64_t *ts, const uint8_t *v, size_t n, size_t w, granule *g)
{
    bool okp = slot_ok(type, v);
    double vp = okp ? slot_val(type, v) : 0;
    double step = 0, lin = 0;
    uint64_t sms = 0, lms = 0, gap = 0;
    g->tw_first = vp;
    g->tw_flags = okp ? 0 : GT_FIRST_NULL;
    for (size_t i = 1; i < n; i++) {
        uint64_t dt = (uint64_t)ts[i] - (uint64_t)ts[i - 1];
        gap = dt > gap ? dt : gap;
        bool ok = slot_ok(type, v + i * w);
        double x = ok ? slot_val(type, v + i * w) : 0;
        if (okp) {
            step += vp * (double)dt;
            sms += dt;
            if (ok) {
                lin += (vp + x) * 0.5 * (double)dt;
                lms += dt;
            }
        }
        vp = x;
        okp = ok;
    }
    g->tw_last = vp;
    if (!okp)
        g->tw_flags |= GT_LAST_NULL;
    if (__builtin_expect(isunordered(step, lin), 0)) { /* canon_nan on both, behind one test */
        step = canon_nan(step);
        lin = canon_nan(lin);
    }
    g->tw_step = step;
    g->tw_lin = lin;
    g->tw_step_ms = sms;
    g->tw_lin_ms = lms;
    g->tw_gap = gap > UINT32_MAX ? UINT32_MAX : (uint32_t)gap;
}

static inline __attribute__((always_inline)) void granule_twa_t(int type, const int64_t *ts, const void *vv, size_t n,
                                                                 granule *g)
{
    const uint8_t *v = vv;
    size_t w = type_width(type);
    g->tw_first = g->tw_last = g->tw_step = g->tw_lin = 0;
    g->tw_step_ms = g->tw_lin_ms = 0;
    g->tw_gap = 0;
    g->tw_flags = GT_FIRST_NULL | GT_LAST_NULL;
    if (n == 0 || type == TSDB_STATE)
        return;
    if (n <= TWA_SEQ_MAX) {
        twa_seq(type, ts, v, n, w, g);
        return;
    }
    bool ok0 = slot_ok(type, v), okl = slot_ok(type, v + (n - 1) * w);
    twa_sums o;
    simd_twa(type, ts, v, n, &o);
    g->tw_first = ok0 ? slot_val(type, v) : 0;
    g->tw_flags = (ok0 ? 0 : GT_FIRST_NULL) | (okl ? 0 : GT_LAST_NULL);
    g->tw_last = okl ? slot_val(type, v + (n - 1) * w) : 0;
    g->tw_step = o.step;
    g->tw_lin = o.lin;
    g->tw_step_ms = o.sms;
    g->tw_lin_ms = o.lms;
    uint64_t gap = o.gap;
    g->tw_gap = gap > UINT32_MAX ? UINT32_MAX : (uint32_t)gap;
}

/* one copy per type: the slots' loads and null tests then cost no switch (short granules are mostly that) */
void granule_twa(int type, const int64_t *ts, const void *v, size_t n, granule *g)
{
    switch (type) {
    case TSDB_FLOAT32: granule_twa_t(TSDB_FLOAT32, ts, v, n, g); break;
    case TSDB_FLOAT64: granule_twa_t(TSDB_FLOAT64, ts, v, n, g); break;
    case TSDB_INT64: granule_twa_t(TSDB_INT64, ts, v, n, g); break;
    case TSDB_BOOL: granule_twa_t(TSDB_BOOL, ts, v, n, g); break;
    default: granule_twa_t(type, ts, v, n, g); break;
    }
}

/* ------------------------------------------------------------ query */

typedef struct twa_q {
    const bucketer *bk;
    bool linear;
    uint64_t max_gap; /* 0: none */
    int64_t start, end;
    int64_t floor; /* retention: readings before it have expired and start no segment */
    int type;
    size_t w;
} twa_q;

typedef struct twa_pt {
    int64_t t;
    double v;
    bool ok;
} twa_pt;

/* partial sums for one bucket */
typedef struct twa_part {
    int64_t k;
    double area;
    uint64_t ms;
    uint64_t count;
} twa_part;

/* A walk over readings in time order: the previous reading, the bucket last
 * looked up, and partial sums per bucket (keys only grow along a walk). */
typedef struct twa_cur {
    bool have;
    twa_pt p;
    int64_t k, kb, ke; /* bucket k is [kb, ke) */
    /* the newest bucket's sums, not yet in v: per row they stay in registers rather than going through memory
     * (cur_seal moves them; the same additions in the same order, so the same bits) */
    bool pend;
    twa_part e;
    twa_part *v;
    size_t n, cap;
    int err;
} twa_cur;

static void cur_init(twa_cur *c)
{
    memset(c, 0, sizeof *c);
    c->kb = 1;
    c->ke = 0; /* no bucket cached */
}

static inline void cur_bucket(const twa_q *q, twa_cur *c, int64_t t)
{
    if (t >= c->kb && t < c->ke)
        return;
    c->k = bucketer_key(q->bk, t);
    if (q->bk->kind == BK_ONE) {
        c->kb = INT64_MIN;
        c->ke = INT64_MAX;
    } else {
        c->kb = bucketer_start(q->bk, c->k);
        c->ke = c->k == INT64_MAX ? INT64_MAX : bucketer_start(q->bk, c->k + 1);
    }
}

static void cur_push(twa_cur *c, twa_part e)
{
    if (c->n == c->cap) {
        size_t cap = c->cap ? c->cap * 2 : 16;
        twa_part *v = realloc(c->v, cap * sizeof *v);
        if (!v) {
            c->err = TSDB_ERR_NOMEM;
            return;
        }
        c->v = v;
        c->cap = cap;
    }
    c->v[c->n++] = e;
}

/* moves the pending sums into v: before anything reads v, n or err */
static inline void cur_seal(twa_cur *c)
{
    if (c->pend) {
        cur_push(c, c->e);
        c->pend = false;
    }
}

static inline void cur_add(twa_cur *c, double area, uint64_t ms, uint64_t count)
{
    if (c->pend && c->e.k == c->k) {
        c->e.area += area;
        c->e.ms += ms;
        c->e.count += count;
        return;
    }
    if (c->pend)
        cur_push(c, c->e);
    c->e = (twa_part){c->k, area, ms, count};
    c->pend = true;
}

static inline double interp(twa_pt a, twa_pt b, int64_t x)
{
    if (x == a.t)
        return a.v;
    if (x == b.t)
        return b.v;
    /* a line to an infinity is that infinity throughout (+inf to -inf: NaN), so that a segment comes out the
     * same wherever bucket edges cut it, and as granule_twa's uncut trapezoid */
    if (!isfinite(a.v) || !isfinite(b.v))
        return a.v + b.v;
    return a.v + (b.v - a.v) * ((double)((uint64_t)x - (uint64_t)a.t) / (double)((uint64_t)b.t - (uint64_t)a.t));
}

/* The segment from reading a to the next reading b, cut at the range and the bucket edges. */
static inline void seg(const twa_q *q, twa_cur *c, twa_pt a, twa_pt b)
{
    if (!a.ok || (q->linear && !b.ok))
        return;
    if (q->max_gap && (uint64_t)b.t - (uint64_t)a.t > q->max_gap)
        return;
    if (a.t < q->floor) /* an expired reading: no value is in force until the next live one */
        return;
    int64_t x = MAX(a.t, q->start), y = MIN(b.t, q->end);
    while (x < y) {
        cur_bucket(q, c, x);
        int64_t e = MIN(y, c->ke);
        uint64_t len = (uint64_t)e - (uint64_t)x;
        double area = q->linear ? (interp(a, b, x) + interp(a, b, e)) * 0.5 * (double)len : a.v * (double)len;
        cur_add(c, area, len, 0);
        x = e;
    }
}

static inline void feed(const twa_q *q, twa_cur *c, twa_pt p)
{
    if (c->have)
        seg(q, c, c->p, p);
    if (p.ok && p.t >= q->start && p.t < q->end) {
        cur_bucket(q, c, p.t);
        cur_add(c, 0, 0, 1);
    }
    /* field by field: a struct copy goes through a 16-byte load of what was just stored in pieces, a store
     * forwarding stall on every row */
    c->p.t = p.t;
    c->p.v = p.v;
    c->p.ok = p.ok;
    c->have = true;
}

static inline __attribute__((always_inline)) void feed_rows_as(const twa_q *q, twa_cur *c, const int64_t *ts,
                                                               const uint8_t *v, size_t n)
{
    twa_cur l = *c; /* a local the compiler can keep in registers, whatever it inlines */
    for (size_t i = 0; i < n; i++) {
        const uint8_t *s = v + i * q->w;
        bool ok = slot_ok(q->type, s);
        feed(q, &l, (twa_pt){ts[i], ok ? slot_val(q->type, s) : 0, ok});
    }
    *c = l;
}

/* The per-row loop, once per method and for float32 on its own: with those known, the compiler keeps the loop's
 * state in registers instead of leaving it to its inlining budget for the whole file. */
static void feed_rows(const twa_q *q, twa_cur *c, const int64_t *ts, const uint8_t *v, size_t n)
{
    twa_q l = *q;
    if (q->type == TSDB_FLOAT32) {
        l.type = TSDB_FLOAT32;
        l.w = 4;
        if (q->linear) {
            l.linear = true;
            feed_rows_as(&l, c, ts, v, n);
        } else {
            l.linear = false;
            feed_rows_as(&l, c, ts, v, n);
        }
    } else if (q->linear) {
        l.linear = true;
        feed_rows_as(&l, c, ts, v, n);
    } else {
        l.linear = false;
        feed_rows_as(&l, c, ts, v, n);
    }
}

typedef struct twa_acc {
    double area;
    uint64_t ms;
    uint64_t count;
} twa_acc;

typedef struct twa_out {
    twa_acc *acc;
    int64_t b0;
    size_t nb;
} twa_out;

/* Moves a walk's partial sums into the result. */
static void flush(twa_out *o, twa_cur *c)
{
    cur_seal(c);
    for (size_t i = 0; i < c->n; i++) {
        uint64_t k = (uint64_t)c->v[i].k - (uint64_t)o->b0;
        if (k >= o->nb) /* cannot happen: contributions lie within [dmin, dmax] */
            continue;
        o->acc[k].area += c->v[i].area;
        o->acc[k].ms += c->v[i].ms;
        o->acc[k].count += c->v[i].count;
    }
    c->n = 0;
}

static int rows_cb(void *ctx, const int64_t *ts, const void *v, size_t n)
{
    void **a = ctx;
    feed_rows(a[0], a[1], ts, v, n);
    return 0;
}

/* One step of the walk: a granule from statistics, a decoded granule, a memtable run or a merged cluster. */
enum { PC_STATS, PC_POINT, PC_DECODE, PC_MEM, PC_MERGE };
typedef struct twa_piece {
    int kind;
    uint32_t src;
    uint32_t k;        /* PC_STATS, PC_POINT, PC_DECODE: the granule's entry in parts[src]; else TW_NONE */
    size_t a, b;       /* PC_MERGE: items [a, b) */
    /* PC_DECODE, filled by the workers */
    twa_pt first, last;
    twa_part *v;
    size_t n;
} twa_piece;

typedef struct twa_item {
    int64_t lo, hi;
    uint32_t src;
    uint32_t k; /* the granule's entry in parts[src]; TW_NONE for a memtable run */
} twa_item;
#define TW_NONE UINT32_MAX

static int cmp_twa_item(const void *a, const void *b)
{
    const twa_item *x = a, *y = b;
    return (x->lo > y->lo) - (x->lo < y->lo);
}

typedef struct twa_job {
    eng *db;
    qstats *qs;
    const twa_q *q;
    const snapshot *sn;
    twa_piece *pc;
    size_t *work;
    size_t nwork;
    atomic_size_t next;
    atomic_int err;
} twa_job;

static void twa_worker(void *arg, int idx)
{
    (void)idx;
    twa_job *j = arg;
    qstats *mine = cur_qstats;
    cur_qstats = j->qs;
    codec_ctx *cc = cc_get(j->db);
    size_t cap = j->db->granule_rows;
    int64_t *ts = qscratch_get(0, cap * sizeof *ts);
    uint8_t *v = qscratch_get(1, cap * sizeof(uint64_t));
    if (!cc || !ts || !v) {
        atomic_store(&j->err, TSDB_ERR_NOMEM);
        goto out;
    }
    for (;;) {
        size_t w = atomic_fetch_add(&j->next, 1);
        if (w >= j->nwork || atomic_load_explicit(&j->err, memory_order_relaxed))
            break;
        twa_piece *pc = &j->pc[j->work[w]];
        const part *p = j->sn->parts[pc->src];
        granule gb;
        int rc = granule_read_at(cc, p, pc->k, j->q->type, ts, v, p->verify, &gb);
        if (rc) {
            atomic_store(&j->err, rc);
            break;
        }
        size_t n = gb.count;
        twa_cur c;
        cur_init(&c);
        feed_rows(j->q, &c, ts, v, n);
        cur_seal(&c);
        if (c.err) {
            free(c.v);
            atomic_store(&j->err, c.err);
            break;
        }
        bool ok0 = slot_ok(j->q->type, v), ok1 = slot_ok(j->q->type, v + (n - 1) * j->q->w);
        pc->first = (twa_pt){ts[0], ok0 ? slot_val(j->q->type, v) : 0, ok0};
        pc->last = (twa_pt){ts[n - 1], ok1 ? slot_val(j->q->type, v + (n - 1) * j->q->w) : 0, ok1};
        pc->v = c.v;
        pc->n = c.n;
        int lv = sched_leave();
        if (lv < 0)
            atomic_store(&j->err, lv);
        if (lv)
            break;
    }
out:
    if (cc)
        cc_put(j->db, cc);
    qscratch_put(ts);
    qscratch_put(v);
    cur_qstats = mine;
}

/* One TWA query over one snapshot, window by window (bucket_windows in query.c: order, limit, early stop).
 * A window [start, end) between bucket edges is computed exactly as a query of that range: the readings
 * around it come from the same snapshot, which reaches around the whole query range. */
typedef struct twa_run {
    eng *db;
    snapshot sn;
    twa_q q;
    uint32_t series;
    int64_t lo, hi; /* the snapshot's reach */
    int64_t max_gap;
    bool force_decode;
    int64_t first, last; /* the earliest and the latest reading in the snapshot (granule bounds) */
} twa_run;

/* Covered time lies between two readings and a counted reading is one: no bucket before the first reading or
 * after the last is returned. Empty stretches between readings may still be covered, so only the ends skip. */
static bool twa_bound(void *ctx, int64_t lo, int64_t hi, bool newest, int64_t *at)
{
    const twa_run *R = ctx;
    if (R->last < lo || R->first >= hi)
        return false;
    *at = newest ? MIN(R->last, hi - 1) : MAX(R->first, lo);
    return true;
}

static int twa_window(void *ctx, int64_t start, int64_t end, void **outp, size_t *nout)
{
    const twa_run *R = ctx;
    eng *db = R->db;
    snapshot sn = R->sn; /* a copy to read: eng_query_twa releases it */
    uint32_t series = R->series;
    int64_t lo = R->lo, hi = R->hi, max_gap = R->max_gap;
    bool force_decode = R->force_decode;
    twa_q q = R->q;
    q.start = start;
    q.end = end;
    const bucketer *bk = q.bk;
    int type = q.type;
    tsdb_twa_bucket **out = (tsdb_twa_bucket **)outp;
    *out = NULL;
    *nout = 0;
    int rc = 0;

    size_t nitem = 0, cap = 0;
    twa_item *items = NULL;
    twa_piece *pc = NULL;
    size_t *work = NULL;
    src *srcs = NULL;
    codec_ctx *cc = NULL;
    twa_acc *acc = NULL;
    twa_cur c;
    cur_init(&c);
    size_t npc = 0, nwork = 0;
    int64_t dmin = INT64_MAX, dmax = INT64_MIN;
    for (size_t i = 0; i < sn.np + sn.nmem && rc == 0; i++) {
        uint32_t gi = 0, gj = 1;
        if (i < sn.np) {
            /* Only within the entry's window: granules that tombstones touch were moved to mem runs (their live
             * rows only), so the neighbours found here and there are the nearest live readings. */
            const part *p = sn.parts[i];
            snap_find(&sn, i, series, start, end, &gi, &gj);
            /* the last granule before the range, unless one in it already starts before it */
            if (gi > sn.gi[i] && gx_series(p, gi - 1) == series && (gi == gj || gx_ts_min(p, gi) >= start))
                gi--;
            /* and the first after it */
            if (gj < sn.gj[i] && gx_series(p, gj) == series && (gj == gi || gx_ts_max(p, gj - 1) < end))
                gj++;
        } else if (mem_from_memtable(&sn, i - sn.np)) {
            QS_ADD(memtable_rows, sn.mem[i - sn.np].n);
        }
        for (uint32_t k = gi; k < gj; k++) {
            if (nitem == cap) {
                cap = cap ? cap * 2 : 256;
                twa_item *it = realloc(items, cap * sizeof *it);
                if (!it) {
                    rc = TSDB_ERR_NOMEM;
                    break;
                }
                items = it;
            }
            twa_item *it = &items[nitem++];
            it->src = (uint32_t)i;
            if (i < sn.np) {
                it->k = k;
                it->lo = gx_ts_min(sn.parts[i], k);
                it->hi = gx_ts_max(sn.parts[i], k);
            } else {
                const mem_run *r = &sn.mem[i - sn.np];
                it->k = TW_NONE;
                it->lo = r->ts[0];
                it->hi = r->ts[r->n - 1];
            }
        }
    }
    /* Outside the range, only the latest reading before it and the earliest after it bound segments that reach
     * into it: keep the items that hold them (several where sources tie on that time: last write wins there).
     * Without a staleness limit the snapshot spans all history, and every part would add a granule. */
    int64_t mb = INT64_MIN, ma = INT64_MAX;
    for (size_t k = 0; k < nitem; k++) {
        if (items[k].hi < start)
            mb = MAX(mb, items[k].hi);
        if (items[k].lo >= end)
            ma = MIN(ma, items[k].lo);
    }
    size_t kept = 0;
    uint32_t last_src = UINT32_MAX;
    for (size_t k = 0; k < nitem; k++) {
        const twa_item *it = &items[k];
        if (it->hi < mb || it->lo > ma)
            continue;
        if (it->k != TW_NONE) {
            QS_ADD(granules, 1);
            if (it->src != last_src) /* items come grouped by source */
                QS_ADD(parts, 1);
            last_src = it->src;
        }
        dmin = MIN(dmin, it->lo);
        dmax = MAX(dmax, it->hi);
        items[kept++] = *it;
    }
    nitem = kept;
    dmin = MAX(dmin, start);
    dmax = MIN(dmax, end - 1);
    if (rc || nitem == 0 || dmin > dmax)
        goto done;

    twa_out o = {.b0 = 0, .nb = 1};
    if (bk->kind != BK_ONE) {
        o.b0 = bucketer_key(bk, dmin);
        uint64_t span = (uint64_t)bucketer_key(bk, dmax) - (uint64_t)o.b0 + 1;
        if (span > TWA_MAX_BUCKETS) {
            rc = TSDB_ERR_TOO_MANY_BUCKETS;
            goto done;
        }
        if (!qbudget_ok(span * (sizeof *acc + sizeof(tsdb_twa_bucket)))) {
            rc = TSDB_ERR_DEFER; /* a multi-series task ahead of the head: run it at its turn */
            goto done;
        }
        o.nb = (size_t)span;
    }
    acc = calloc(o.nb, sizeof *acc);
    pc = malloc(nitem * sizeof *pc);
    work = malloc(nitem * sizeof *work);
    srcs = malloc((sn.np + sn.nmem) * sizeof *srcs);
    if (!acc || !pc || !work || !srcs) {
        rc = TSDB_ERR_NOMEM;
        goto done;
    }
    o.acc = acc;

    /* pieces in time order: clusters of overlapping items */
    for (size_t k = 1; k < nitem; k++) /* usually in order already (see sort_items in query.c) */
        if (items[k].lo < items[k - 1].lo) {
            qsort(items, nitem, sizeof *items, cmp_twa_item);
            break;
        }
    for (size_t a = 0; a < nitem;) {
        size_t b = a + 1;
        int64_t h = items[a].hi;
        bool multi = false;
        while (b < nitem && items[b].lo <= h) {
            multi |= items[b].src != items[a].src;
            h = MAX(h, items[b].hi);
            b++;
        }
        if (multi) {
            pc[npc++] = (twa_piece){.kind = PC_MERGE, .a = a, .b = b};
        } else {
            for (size_t k = a; k < b; k++) {
                const twa_item *it = &items[k];
                twa_piece *x = &pc[npc++];
                *x = (twa_piece){.kind = PC_MEM, .src = it->src, .k = it->k};
                if (it->k == TW_NONE)
                    continue;
                if (!force_decode && (it->hi < start || it->lo >= end)) {
                    x->kind = PC_POINT;
                    continue;
                }
                uint32_t gap = (uint32_t)gix_get(&sn.parts[it->src]->ix, it->k, GF_GAP);
                bool usable = !force_decode && it->lo >= start && it->hi < end &&
                              bucketer_key(bk, it->lo) == bucketer_key(bk, it->hi) &&
                              (!max_gap || (gap < UINT32_MAX && gap <= (uint64_t)max_gap));
                x->kind = usable ? PC_STATS : PC_DECODE;
                if (!usable)
                    work[nwork++] = npc - 1;
            }
        }
        a = b;
    }

    if (nwork) {
        twa_job j = {.db = db, .qs = cur_qstats, .q = &q, .sn = &sn, .pc = pc, .work = work, .nwork = nwork};
        atomic_init(&j.next, 0);
        atomic_init(&j.err, 0);
        uint64_t rows = 0;
        for (size_t w = 0; w < nwork; w++)
            rows += (uint64_t)gx_count(sn.parts[pc[work[w]].src], pc[work[w]].k) * TWA_ROW_WEIGHT;
        sched_run(db->sched, query_threads_for(db, rows, nwork), twa_worker, &j);
        rc = atomic_load(&j.err);
        if (rc)
            goto done;
    }

    cc = cc_get(db);
    if (!cc) {
        rc = TSDB_ERR_NOMEM;
        goto done;
    }
    for (size_t i = 0; i < npc && rc == 0; i++) {
        twa_piece *x = &pc[i];
        switch (x->kind) {
        case PC_STATS: {
            granule gb;
            gb.tw_step = gb.tw_lin = 0; /* the one not asked for stays unread */
            gb.tw_step_ms = gb.tw_lin_ms = 0;
            gx_load(sn.parts[x->src], x->k, GW_TWEND | (q.linear ? GW_TWLIN : GW_TWSTEP), &gb);
            const granule *g = &gb;
            QS_ADD(granules_from_stats, 1);
            QS_ADD(rows_from_stats, g->count);
            twa_pt first = {g->ts_min, g->tw_first, !(g->tw_flags & GT_FIRST_NULL)};
            if (c.have)
                seg(&q, &c, c.p, first);
            cur_bucket(&q, &c, g->ts_min);
            cur_add(&c, q.linear ? g->tw_lin : g->tw_step, q.linear ? g->tw_lin_ms : g->tw_step_ms,
                    g->count - g->nulls);
            c.p = (twa_pt){g->ts_max, g->tw_last, !(g->tw_flags & GT_LAST_NULL)};
            c.have = true;
            break;
        }
        case PC_POINT: {
            /* before or after the range: the segments inside it do not reach into the range */
            granule gb;
            gx_load(sn.parts[x->src], x->k, GW_TWEND, &gb);
            const granule *g = &gb;
            twa_pt first = {g->ts_min, g->tw_first, !(g->tw_flags & GT_FIRST_NULL)};
            if (c.have)
                seg(&q, &c, c.p, first);
            c.p = (twa_pt){g->ts_max, g->tw_last, !(g->tw_flags & GT_LAST_NULL)};
            c.have = true;
            break;
        }
        case PC_DECODE:
            if (c.have)
                seg(&q, &c, c.p, x->first);
            c.p = x->last;
            c.have = true;
            {
                twa_cur d = {.v = x->v, .n = x->n};
                flush(&o, &d);
            }
            break;
        case PC_MEM: {
            const mem_run *r = &sn.mem[x->src - sn.np];
            feed_rows(&q, &c, r->ts, r->v, r->n);
            break;
        }
        default: {
            /* sources overlap here: merge them so replaced rows do not count */
            size_t ns = 0;
            for (uint32_t s = 0; s < sn.np + sn.nmem; s++) {
                uint32_t gi = UINT32_MAX, gj = 0;
                bool any = false;
                for (size_t k = x->a; k < x->b; k++) {
                    if (items[k].src != s)
                        continue;
                    any = true;
                    if (s < sn.np) {
                        uint32_t idx = items[k].k;
                        gi = MIN(gi, idx);
                        gj = MAX(gj, idx + 1);
                    }
                }
                if (!any)
                    continue;
                if (s < sn.np)
                    src_init_part(&srcs[ns++], sn.parts[s], gi, gj, type);
                else
                    src_init_mem(&srcs[ns++], &sn.mem[s - sn.np], mem_prio(&sn, s - sn.np), type);
            }
            void *ctx[2] = {&q, &c};
            rc = merge_stream(cc, srcs, ns, lo, hi, rows_cb, ctx);
            for (size_t k = 0; k < ns; k++)
                src_free(&srcs[k]);
        }
        }
        cur_seal(&c);
        if (c.err)
            rc = c.err;
        flush(&o, &c);
    }

    if (rc == 0) {
        size_t nonempty = 0;
        for (size_t k = 0; k < o.nb; k++)
            nonempty += acc[k].ms || acc[k].count;
        tsdb_twa_bucket *res = malloc((nonempty ? nonempty : 1) * sizeof *res);
        if (!res) {
            rc = TSDB_ERR_NOMEM;
        } else {
            size_t n = 0;
            for (size_t k = 0; k < o.nb; k++) {
                const twa_acc *a = &acc[k];
                if (!a->ms && !a->count)
                    continue;
                tsdb_twa_bucket *r = &res[n++];
                r->start = bucketer_start(bk, o.b0 + (int64_t)k);
                r->area = isnan(a->area) ? __builtin_nan("") : a->area;
                r->covered_ms = a->ms > INT64_MAX ? INT64_MAX : (int64_t)a->ms;
                r->count = a->count;
                r->twa = a->ms ? a->area / (double)a->ms : __builtin_nan("");
                if (isnan(r->twa))
                    r->twa = __builtin_nan("");
            }
            *out = res;
            *nout = n;
        }
    }
done:
    if (cc)
        cc_put(db, cc);
    for (size_t i = 0; i < npc; i++)
        if (pc[i].kind == PC_DECODE)
            free(pc[i].v);
    free(c.v);
    free(pc);
    free(work);
    free(srcs);
    free(acc);
    free(items);
    return rc;
}

int eng_query_twa(eng *db, uint32_t series, int64_t start, int64_t end, const bucketer *bk,
                  const tsdb_twa_opts *opts, tsdb_twa_bucket **out, size_t *nout)
{
    *out = NULL;
    *nout = 0;
    int method = (int)opts->method;
    int64_t max_gap = opts->max_gap_ms;
    int type = eng_series_type(db, series);
    if (type == TSDB_STATE || max_gap < 0 || (method != TSDB_TWA_STEP && method != TSDB_TWA_LINEAR))
        return TSDB_ERR_INVALID;
    int64_t cutoff = eng_cutoff(db, series); /* expired rows are gone at once, merged away or not */
    start = MAX(start, cutoff);
    if (start >= end || type < 0)
        return 0;
    twa_run R = {.db = db, .series = series, .max_gap = max_gap, .force_decode = opts->no_stats,
                 .first = INT64_MAX, .last = INT64_MIN};
    R.q = (twa_q){.bk = bk, .linear = method == TSDB_TWA_LINEAR, .max_gap = (uint64_t)max_gap, .start = start,
                  .end = end, .floor = cutoff, .type = type, .w = type_width(type)};
    /* the readings around the range that bound its first and last segments; none expired */
    R.lo = MAX(max_gap ? sat_add(start, -max_gap) : INT64_MIN, cutoff);
    R.hi = max_gap ? sat_add(sat_add(end, max_gap), 1) : INT64_MAX;
    int rc = snapshot_take(db, series, R.q.w, R.lo, R.hi, &R.sn);
    if (rc)
        return rc;
    for (size_t i = 0; i < R.sn.np; i++)
        if (R.sn.gi[i] < R.sn.gj[i]) {
            R.first = MIN(R.first, gx_ts_min(R.sn.parts[i], R.sn.gi[i]));
            R.last = MAX(R.last, gx_ts_max(R.sn.parts[i], R.sn.gj[i] - 1));
        }
    for (size_t i = 0; i < R.sn.nmem; i++)
        if (R.sn.mem[i].n) {
            R.first = MIN(R.first, R.sn.mem[i].ts[0]);
            R.last = MAX(R.last, R.sn.mem[i].ts[R.sn.mem[i].n - 1]);
        }
    bk_windows w = {.bk = bk, .desc = opts->order == TSDB_DESC, .limit = opts->limit, .ctx = &R,
                    .run = twa_window, .bound = twa_bound, .row = sizeof(tsdb_twa_bucket)};
    rc = bucket_windows(&w, start, end, (void **)out, nout);
    snapshot_release(&R.sn);
    return rc;
}
