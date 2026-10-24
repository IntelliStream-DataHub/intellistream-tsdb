/*
 * Queries.
 *
 * A query reads one series from a consistent snapshot: the parts that
 * overlap the range plus the rows of the series in the memtables. Each
 * part's granule index is sorted by (series, ts), so finding the granules of
 * one series in a time range is two binary searches per part (the sparse
 * primary index of MergeTree).
 *
 * (series, ts) is unique: when several sources hold the same timestamp, only
 * the newest source's row counts (memtables, newest first, then parts by
 * block number), so results never contain duplicates, as if every read used
 * ReplacingMergeTree's FINAL.
 *
 * Aggregates: granules are grouped into clusters of overlapping time ranges.
 * A granule that overlaps nothing from another source cannot hold a replaced
 * row, so when it lies inside the range and inside one bucket it is answered
 * from its stored count/sum/min/max without decoding; otherwise it is decoded
 * in parallel and folded with the SIMD kernels. Clusters spanning several
 * sources go through the deduplicating merge.
 *
 * Raw: sources whose time ranges are disjoint are concatenated; otherwise
 * they are merged by timestamp, again keeping the newest row per timestamp.
 *
 * Values move as slots of the series' type (4 or 8 bytes); aggregation skips
 * nulls, and a bucket whose rows are all null is reported with count 0.
 */
#include "internal.h"

#include <stdlib.h>

#define MAX_BUCKETS (64u << 20)
#define PARALLEL_MIN_GRANULES 8
#define AGG_STRIPES 64
#define AGG_STRIPE_SHIFT 10 /* 1024 consecutive buckets share a lock */

/* --------------------------------------------------------------- snapshot */

typedef struct snapshot {
    part **parts;
    size_t np;
    mem_run *mem; /* rows of the series from memtables, oldest memtable first */
    size_t nmem;
} snapshot;

static void snapshot_release(snapshot *s)
{
    db_release(s->parts, s->np);
    for (size_t i = 0; i < s->nmem; i++)
        mem_run_free(&s->mem[i]);
    free(s->mem);
    memset(s, 0, sizeof *s);
}

/* Parts and memtables as of one instant: a flush publishes its parts and
 * retires its memtable under db->lock, so nothing is seen twice or missed. */
static int snapshot_take(eng *db, uint32_t series, size_t w, int64_t start, int64_t end, snapshot *s)
{
    memset(s, 0, sizeof *s);
    memtable *ms[MAX_FLUSHING];
    size_t nms = 0;
    int rc = 0;
    mem_run active = {0};
    pthread_mutex_lock(&db->lock);
    /* With a WAL synced on every write, a row is visible once it is durable:
     * no reader sees a write that a power failure could still take back. That
     * includes the flushing memtables: a flush rotates the active one before
     * it writes and syncs the rest of its segment. */
    uint64_t visible = db->wal_enabled && db->opt.wal_sync_ms == 0 ? atomic_load(&db->wal_durable) : UINT64_MAX;
    s->parts = malloc((db->nparts ? db->nparts : 1) * sizeof *s->parts);
    if (s->parts) {
        for (size_t i = 0; i < db->nparts; i++) {
            part *p = db->parts[i];
            if (p->max_ts >= start && p->min_ts < end) {
                part_ref(p);
                s->parts[s->np++] = p;
            }
        }
    } else {
        rc = TSDB_ERR_NOMEM;
    }
    for (size_t i = 0; i < db->nflushing; i++) {
        mt_ref(db->flushing[i]);
        ms[nms++] = db->flushing[i];
    }
    /* a follower's WAL tails: written to by the follower thread, so read under buf_lock */
    mem_run tails[MAX_TAILS];
    size_t ntails = 0;
    if (rc == 0 && (db->active || db->ntails)) {
        pthread_mutex_lock(&db->buf_lock);
        for (size_t i = 0; rc == 0 && i < db->ntails; i++) {
            rc = mt_collect(db->tails[i].m, series, w, start, end, UINT64_MAX, &tails[ntails]);
            if (tails[ntails].n)
                ntails++;
            else
                mem_run_free(&tails[ntails]);
        }
        if (rc == 0 && db->active)
            rc = mt_collect(db->active, series, w, start, end, visible, &active);
        pthread_mutex_unlock(&db->buf_lock);
    }
    pthread_mutex_unlock(&db->lock);

    s->mem = calloc(nms + ntails + 1, sizeof *s->mem);
    for (size_t i = 0; i < ntails; i++) { /* oldest first, like the writer's memtables */
        if (s->mem)
            s->mem[s->nmem++] = tails[i];
        else
            mem_run_free(&tails[i]);
    }
    if (!s->mem && rc == 0)
        rc = TSDB_ERR_NOMEM;
    for (size_t i = 0; i < nms; i++) {
        /* flushing memtables are immutable: read them without locks */
        mem_run r = {0};
        if (rc == 0)
            rc = mt_collect(ms[i], series, w, start, end, visible, &r);
        if (r.n && s->mem)
            s->mem[s->nmem++] = r;
        else
            mem_run_free(&r);
        mt_unref(ms[i]);
    }
    if (active.n && s->mem)
        s->mem[s->nmem++] = active;
    else
        mem_run_free(&active);
    if (rc)
        snapshot_release(s);
    return rc;
}

/* Granules [*gi, *gj) of series s that may hold rows in [start, end). */
static void part_find(const part *p, uint32_t s, int64_t start, int64_t end, uint32_t *gi, uint32_t *gj)
{
    const granule *g = p->g;
    uint32_t lo = 0, hi = p->ng;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (g[mid].series < s || (g[mid].series == s && g[mid].ts_max < start))
            lo = mid + 1;
        else
            hi = mid;
    }
    *gi = lo;
    hi = p->ng;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (g[mid].series < s || (g[mid].series == s && g[mid].ts_min < end))
            lo = mid + 1;
        else
            hi = mid;
    }
    *gj = lo;
}

/* ---------------------------------------------------- deduplicating merge */

/* A time-ordered stream of unique timestamps from one source: a range of a
 * part's granules, or a memtable run. */
typedef struct src {
    const part *p;   /* NULL for a memtable run */
    uint32_t gi, gj; /* granules still to load */
    uint64_t prio;   /* newer sources win on equal timestamps */
    bool loaded;
    size_t pos, n;   /* rows [pos, n) of ts/v are in range and unread */
    int64_t *ts;
    uint8_t *v;      /* slots of `type` */
    int type;
    size_t w;        /* slot width */
    bool own;        /* ts/v are decode buffers owned by the cursor */
} src;

static void src_init_part(src *c, const part *p, uint32_t gi, uint32_t gj, int type)
{
    memset(c, 0, sizeof *c);
    c->p = p;
    c->gi = gi;
    c->gj = gj;
    c->prio = p->max_block;
    c->type = type;
    c->w = type_width(type);
}

/* Memtable runs are newer than every part; later memtables are newer. */
static void src_init_mem(src *c, const mem_run *r, uint64_t prio, int type)
{
    memset(c, 0, sizeof *c);
    c->prio = prio;
    c->ts = r->ts;
    c->v = r->v;
    c->n = r->n;
    c->loaded = r->n > 0;
    c->type = type;
    c->w = type_width(type);
}

static void src_free(src *c)
{
    if (c->own) {
        free(c->ts);
        free(c->v);
    }
}

static int64_t src_first_ts(const src *c) { return c->p ? c->p->g[c->gi].ts_min : c->ts[0]; }
static int64_t src_last_ts(const src *c) { return c->p ? c->p->g[c->gj - 1].ts_max : c->ts[c->n - 1]; }

/* Decodes granules until one has rows in [start, end), or the source ends. */
static int src_fill(codec_ctx *cc, src *c, int64_t start, int64_t end)
{
    if (c->loaded || !c->p)
        return 0;
    if (!c->own && c->gi < c->gj) {
        c->ts = malloc(c->p->grows * sizeof *c->ts);
        c->v = malloc(c->p->grows * c->w);
        c->own = true;
        if (!c->ts || !c->v)
            return TSDB_ERR_NOMEM;
    }
    while (c->gi < c->gj) {
        const granule *g = &c->p->g[c->gi++];
        int rc = granule_read(cc, c->p, g, c->type, c->ts, c->v, c->p->verify);
        if (rc)
            return rc;
        c->pos = g->ts_min >= start ? 0 : lower_bound_i64(c->ts, g->count, start);
        c->n = g->ts_max < end ? g->count : lower_bound_i64(c->ts, g->count, end);
        if (c->pos < c->n) {
            c->loaded = true;
            return 0;
        }
    }
    return 0;
}

static int src_advance(codec_ctx *cc, src *c, size_t rows, int64_t start, int64_t end)
{
    c->pos += rows;
    if (c->pos < c->n)
        return 0;
    c->loaded = false;
    return src_fill(cc, c, start, end);
}


/* Emits the union of the sources in time order, one row per timestamp: the
 * row of the source with the highest priority. Every live source is kept
 * decoded, so the head timestamps are exact. */
static int merge_stream(codec_ctx *cc, src *cur, size_t nc, int64_t start, int64_t end, slot_fn fn, void *ctx)
{
    int rc = 0;
    for (size_t i = 0; i < nc && rc == 0; i++)
        rc = src_fill(cc, &cur[i], start, end);
    while (rc == 0) {
        src *c = NULL;
        int64_t bound = INT64_MAX;
        bool has_bound = false;
        for (size_t i = 0; i < nc; i++) {
            src *x = &cur[i];
            if (!x->loaded)
                continue;
            int64_t t = x->ts[x->pos];
            if (!c || t < c->ts[c->pos] || (t == c->ts[c->pos] && x->prio > c->prio)) {
                if (c) {
                    int64_t ct = c->ts[c->pos];
                    if (!has_bound || ct < bound)
                        bound = ct;
                    has_bound = true;
                }
                c = x;
            } else if (!has_bound || t < bound) {
                bound = t;
                has_bound = true;
            }
        }
        if (!c)
            break;
        int64_t head = c->ts[c->pos];
        size_t e = c->n;
        if (has_bound)
            e = head < bound ? c->pos + lower_bound_i64(c->ts + c->pos, c->n - c->pos, bound) : c->pos + 1;
        if (fn(ctx, c->ts + c->pos, c->v + c->pos * c->w, e - c->pos))
            return TSDB_ERR_STOPPED;
        rc = src_advance(cc, c, e - c->pos, start, end);
        if (has_bound && head == bound) {
            /* the same timestamp in older sources: replaced, drop it */
            for (size_t i = 0; i < nc && rc == 0; i++)
                if (&cur[i] != c && cur[i].loaded && cur[i].ts[cur[i].pos] == head)
                    rc = src_advance(cc, &cur[i], 1, start, end);
        }
    }
    return rc;
}

/* --------------------------------------------------------------- buckets */

static const int64_t unit_ms[] = {0, 1000, 60000, 3600000};

int bucketer_init(bucketer *bk, int64_t start, const tsdb_group *g)
{
    memset(bk, 0, sizeof *bk);
    bk->start = start;
    if (!g) {
        bk->kind = BK_ONE;
        return 0;
    }
    if (g->unit < TSDB_SECOND || g->unit > TSDB_YEAR)
        return TSDB_ERR_INVALID;
    if (g->timezone && strcmp(g->timezone, "UTC") != 0)
        return TSDB_ERR_INVALID; /* UTC only: a client turns local periods into bounds */
    bk->count = g->count ? g->count : 1;
    if (g->unit <= TSDB_HOUR) {
        bk->kind = BK_FIXED;
        bk->interval = unit_ms[g->unit] * bk->count;
        return 0;
    }
    bk->kind = BK_CALENDAR;
    bk->unit = g->unit;
    return 0;
}

int bucketer_bounds(bucketer *bk, const int64_t *bounds, size_t n)
{
    memset(bk, 0, sizeof *bk);
    if (n == 0)
        return TSDB_ERR_INVALID;
    for (size_t i = 1; i < n; i++)
        if (bounds[i] <= bounds[i - 1])
            return TSDB_ERR_INVALID;
    bk->kind = BK_BOUNDS;
    bk->bounds = bounds;
    bk->nbounds = n;
    return 0;
}

static inline int64_t days_from_civil(int64_t y, unsigned m, unsigned d)
{
    y -= m <= 2;
    int64_t era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = (unsigned)(y - era * 400);
    unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (int64_t)doe - 719468;
}

static inline void civil_from_days(int64_t days, int64_t *y, int *m)
{
    int64_t z = days + 719468, era = (z >= 0 ? z : z - 146096) / 146097;
    unsigned doe = (unsigned)(z - era * 146097), yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100), mp = (5 * doy + 2) / 153;
    *m = (int)(mp < 10 ? mp + 3 : mp - 9);
    *y = (int64_t)yoe + era * 400 + (*m <= 2);
}

/* index of the UTC calendar unit (days, ISO weeks, months, quarters, years since 1970) */
static int64_t cal_index(const bucketer *bk, int64_t ts)
{
    int64_t day = floor_div(ts, 86400000);
    switch (bk->unit) {
    case TSDB_DAY: return day;
    case TSDB_WEEK: return floor_div(day + 3, 7); /* 1970-01-01 was a Thursday */
    default: {
        int64_t y;
        int m;
        civil_from_days(day, &y, &m);
        int64_t month = (y - 1970) * 12 + (m - 1);
        return bk->unit == TSDB_MONTH ? month : bk->unit == TSDB_QUARTER ? floor_div(month, 3) : y - 1970;
    }
    }
}

/* the day (since 1970) on which calendar unit `idx` begins */
static int64_t cal_start_day(const bucketer *bk, int64_t idx)
{
    switch (bk->unit) {
    case TSDB_DAY: return idx;
    case TSDB_WEEK: return idx * 7 - 3;
    case TSDB_MONTH:
    case TSDB_QUARTER: {
        int64_t month = bk->unit == TSDB_MONTH ? idx : idx * 3;
        return days_from_civil(1970 + floor_div(month, 12), (unsigned)(month - floor_div(month, 12) * 12) + 1, 1);
    }
    default: return days_from_civil(1970 + idx, 1, 1);
    }
}

static inline int64_t bk_key(const bucketer *bk, int64_t ts)
{
    switch (bk->kind) {
    case BK_ONE: return 0;
    case BK_FIXED: return floor_div(ts, bk->interval);
    case BK_BOUNDS: { /* the last bound at or before ts */
        size_t i = lower_bound_i64(bk->bounds, bk->nbounds, sat_add(ts, 1));
        return (int64_t)i - 1;
    }
    default: return floor_div(cal_index(bk, ts), bk->count);
    }
}

/* The first instant (ms) of bucket `key`. */
static inline int64_t bk_start(const bucketer *bk, int64_t key)
{
    switch (bk->kind) {
    case BK_ONE: return bk->start;
    case BK_FIXED: return sat_mul(key, bk->interval);
    case BK_BOUNDS: return key < 0 ? INT64_MIN : (size_t)key < bk->nbounds ? bk->bounds[key] : INT64_MAX;
    default: return sat_mul(cal_start_day(bk, key * bk->count), 86400000);
    }
}

/* -------------------------------------------------------------- aggregate */

typedef struct local_bucket {
    int64_t b;
    agg_state a;
} local_bucket;

typedef struct agg_target {
    int64_t start, end, b0;
    const bucketer *bk;
    agg_state *acc;
    int type;
    size_t w;
} agg_target;



/* One run of rows into a: float32 without nulls goes straight to the SIMD
 * kernel, which matters when buckets hold a handful of rows each. */
static inline void fold_run(const agg_target *t, const uint8_t *v, size_t n, bool may_be_null, agg_state *a)
{
    if (t->type == TSDB_FLOAT32 && !may_be_null) {
        a->rows += n;
        simd_agg_f32((const float *)v, n, a);
    } else {
        agg_fold_hint(t->type, v, n, may_be_null, a);
    }
}

/* Folds time-sorted rows (all within [start, end)) into per-bucket states,
 * one entry per bucket touched; returns the number of entries. */
static size_t fold_rows(const agg_target *t, const int64_t *ts, const void *vv, size_t n, bool may_be_null,
                        local_bucket *out)
{
    const uint8_t *v = vv;
    if (!n)
        return 0;
    if (t->bk->kind == BK_ONE) {
        out[0].b = t->b0;
        agg_init(&out[0].a, t->type);
        fold_run(t, v, n, may_be_null, &out[0].a);
        return 1;
    }
    size_t nl = 0;
    for (size_t i = 0; i < n;) {
        /* one run of rows per bucket: the next bucket's start ends it */
        int64_t b = bk_key(t->bk, ts[i]);
        int64_t bend = bk_start(t->bk, b + 1);
        size_t e = bend == INT64_MAX ? n : i + 1 + lower_bound_i64(ts + i + 1, n - i - 1, bend);
        out[nl].b = b;
        agg_init(&out[nl].a, t->type);
        fold_run(t, v + i * t->w, e - i, may_be_null, &out[nl].a);
        nl++;
        i = e;
    }
    return nl;
}

static inline void fold_into(const agg_target *t, const local_bucket *l, size_t nl)
{
    agg_state *acc = t->acc;
    /* the type decided once, not per bucket: buckets can be a handful of rows each */
    if (t->type == TSDB_FLOAT32 || t->type == TSDB_FLOAT64) {
        for (size_t k = 0; k < nl; k++) {
            agg_state *a = &acc[l[k].b - t->b0];
            const agg_state *x = &l[k].a;
            a->rows += x->rows;
            a->count += x->count;
            a->sum += x->sum;
            a->min = x->min < a->min ? x->min : a->min;
            a->max = x->max > a->max ? x->max : a->max;
        }
    } else {
        for (size_t k = 0; k < nl; k++)
            agg_combine(&acc[l[k].b - t->b0], &l[k].a, t->type);
    }
}

typedef struct fold_ctx {
    const agg_target *t;
    local_bucket *scratch;
    size_t cap;
} fold_ctx;

static int fold_emit(void *ctx, const int64_t *ts, const void *v, size_t n)
{
    fold_ctx *f = ctx;
    if (n > f->cap) {
        local_bucket *s = realloc(f->scratch, n * sizeof *s);
        if (!s)
            return 1;
        f->scratch = s;
        f->cap = n;
    }
    fold_into(f->t, f->scratch, fold_rows(f->t, ts, v, n, true, f->scratch));
    return 0;
}

typedef struct gref {
    const part *p;
    const granule *g;
} gref;

typedef struct agg_job {
    eng *db;
    qstats *qs; /* the caller's: workers count into it too */
    const gref *work;
    size_t nwork;
    atomic_size_t next;
    agg_target t;
    /* striped by bucket range: workers decoding different time ranges rarely meet */
    pthread_mutex_t locks[AGG_STRIPES];
    atomic_int err;
} agg_job;

static void agg_worker(void *arg, int idx)
{
    (void)idx;
    agg_job *j = arg;
    eng *db = j->db;
    qstats *mine = cur_qstats;
    cur_qstats = j->qs;
    codec_ctx *cc = cc_get(db);
    size_t cap = db->granule_rows;
    int64_t *ts = malloc(cap * sizeof *ts);
    uint8_t *v = malloc(cap * sizeof(uint64_t));
    local_bucket *local = malloc(cap * sizeof *local); /* a granule touches at most count buckets */
    if (!cc || !ts || !v || !local) {
        atomic_store(&j->err, TSDB_ERR_NOMEM);
        goto out;
    }
    for (;;) {
        size_t w = atomic_fetch_add(&j->next, 1);
        if (w >= j->nwork || atomic_load_explicit(&j->err, memory_order_relaxed))
            break;
        const gref *r = &j->work[w];
        int rc = granule_read(cc, r->p, r->g, j->t.type, ts, v, r->p->verify);
        if (rc) {
            atomic_store(&j->err, rc);
            break;
        }
        size_t n = r->g->count;
        size_t lo = lower_bound_i64(ts, n, j->t.start);
        size_t hi = lower_bound_i64(ts, n, j->t.end);
        size_t nl = fold_rows(&j->t, ts + lo, v + lo * j->t.w, hi > lo ? hi - lo : 0, r->g->nulls > 0, local);
        if (nl) {
            /* local buckets ascend: fold each run that falls in one stripe under its lock */
            for (size_t a = 0; a < nl;) {
                size_t stripe = (size_t)((local[a].b - j->t.b0) >> AGG_STRIPE_SHIFT);
                size_t e = a + 1;
                while (e < nl && (size_t)((local[e].b - j->t.b0) >> AGG_STRIPE_SHIFT) == stripe)
                    e++;
                pthread_mutex_t *m = &j->locks[stripe % AGG_STRIPES];
                pthread_mutex_lock(m);
                fold_into(&j->t, local + a, e - a);
                pthread_mutex_unlock(m);
                a = e;
            }
        }
    }
out:
    if (cc)
        cc_put(db, cc);
    free(ts);
    free(v);
    free(local);
    cur_qstats = mine;
}

/* One granule, or one memtable run, with its time range and source. */
typedef struct item {
    int64_t lo, hi;
    uint32_t src; /* index into the snapshot's sources: parts, then memtable runs */
    const granule *g;
} item;

static int cmp_item(const void *a, const void *b)
{
    const item *x = a, *y = b;
    return (x->lo > y->lo) - (x->lo < y->lo);
}

static uint64_t mem_prio(const snapshot *sn, size_t i) { return UINT64_MAX - (sn->nmem - i); }

int eng_query_agg(eng *db, uint32_t series, int64_t start, int64_t end, int64_t interval, tsdb_bucket **out,
                  size_t *nout)
{
    *out = NULL;
    *nout = 0;
    if (interval < 0)
        return TSDB_ERR_INVALID;
    bucketer bk = {.kind = interval ? BK_FIXED : BK_ONE, .start = start, .interval = interval};
    return eng_query_group(db, series, start, end, &bk, out, nout);
}

int eng_query_group(eng *db, uint32_t series, int64_t start, int64_t end, const bucketer *bk, tsdb_bucket **out,
                    size_t *nout)
{
    *out = NULL;
    *nout = 0;
    int type = eng_series_type(db, series);
    if (start >= end || type < 0)
        return 0;
    snapshot sn;
    int rc = snapshot_take(db, series, type_width(type), start, end, &sn);
    if (rc)
        return rc;

    /* every granule and memtable run of the series in the range */
    size_t nitem = 0, cap = 0;
    item *items = NULL;
    int64_t dmin = INT64_MAX, dmax = INT64_MIN;
    for (size_t i = 0; i < sn.np + sn.nmem && rc == 0; i++) {
        uint32_t gi = 0, gj = 1;
        if (i < sn.np) {
            part_find(sn.parts[i], series, start, end, &gi, &gj);
            if (gi < gj) {
                QS_ADD(parts, 1);
                QS_ADD(granules, gj - gi);
            }
        } else {
            QS_ADD(memtable_rows, sn.mem[i - sn.np].n);
        }
        for (uint32_t k = gi; k < gj; k++) {
            if (nitem == cap) {
                cap = cap ? cap * 2 : 256;
                item *it = realloc(items, cap * sizeof *it);
                if (!it) {
                    rc = TSDB_ERR_NOMEM;
                    break;
                }
                items = it;
            }
            item *it = &items[nitem++];
            it->src = (uint32_t)i;
            if (i < sn.np) {
                it->g = &sn.parts[i]->g[k];
                it->lo = it->g->ts_min;
                it->hi = it->g->ts_max;
            } else {
                const mem_run *r = &sn.mem[i - sn.np];
                it->g = NULL;
                it->lo = r->ts[0];
                it->hi = r->ts[r->n - 1];
            }
            dmin = MIN(dmin, MAX(it->lo, start));
            dmax = MAX(dmax, MIN(it->hi, end - 1));
        }
    }
    agg_state *acc = NULL;
    gref *work = NULL;
    src *srcs = NULL;
    codec_ctx *cc = NULL;
    fold_ctx fc = {0};
    agg_target t = {.start = start, .end = end, .bk = bk, .type = type, .w = type_width(type)};
    size_t nb = 1;
    if (rc || nitem == 0)
        goto done;

    if (bk->kind != BK_ONE) {
        t.b0 = bk_key(bk, dmin);
        uint64_t span = (uint64_t)bk_key(bk, dmax) - (uint64_t)t.b0 + 1;
        if (span > MAX_BUCKETS) {
            rc = TSDB_ERR_TOO_MANY_BUCKETS;
            goto done;
        }
        nb = (size_t)span;
    }
    acc = malloc(nb * sizeof *acc);
    work = malloc(nitem * sizeof *work);
    srcs = malloc((sn.np + sn.nmem) * sizeof *srcs);
    cc = cc_get(db);
    if (!acc || !work || !srcs || !cc) {
        rc = TSDB_ERR_NOMEM;
        goto done;
    }
    for (size_t k = 0; k < nb; k++)
        agg_init(&acc[k], type);
    t.acc = acc;
    fc.t = &t;

    /* clusters of items with overlapping time ranges */
    qsort(items, nitem, sizeof *items, cmp_item);
    size_t nwork = 0;
    for (size_t a = 0; a < nitem && rc == 0;) {
        size_t b = a + 1;
        int64_t hi = items[a].hi;
        bool multi = false;
        while (b < nitem && items[b].lo <= hi) {
            multi |= items[b].src != items[a].src;
            hi = MAX(hi, items[b].hi);
            b++;
        }
        if (!multi) {
            for (size_t k = a; k < b && rc == 0; k++) {
                const item *it = &items[k];
                if (!it->g) { /* a memtable run: already clipped to the range */
                    const mem_run *r = &sn.mem[it->src - sn.np];
                    if (fold_emit(&fc, r->ts, r->v, r->n))
                        rc = TSDB_ERR_NOMEM;
                    continue;
                }
                const granule *g = it->g;
                bool inside = g->ts_min >= start && g->ts_max < end;
                int64_t bmin = bk_key(bk, g->ts_min);
                int64_t bmax = bk_key(bk, g->ts_max);
                bool one = bmin == bmax; /* keys only grow with time: one key, one bucket */
                if (inside && one) {
                    agg_state ga;
                    QS_ADD(granules_from_stats, 1);
                    QS_ADD(rows_from_stats, g->count);
                    agg_from_granule(type, g, &ga);
                    agg_combine(&acc[bmin - t.b0], &ga, type);
                } else
                    work[nwork++] = (gref){sn.parts[it->src], g};
            }
        } else {
            /* sources overlap here: merge them so replaced rows are not counted */
            size_t ns = 0;
            for (uint32_t s = 0; s < sn.np + sn.nmem; s++) {
                uint32_t gi = UINT32_MAX, gj = 0;
                bool any = false;
                for (size_t k = a; k < b; k++) {
                    if (items[k].src != s)
                        continue;
                    any = true;
                    if (s < sn.np) {
                        uint32_t idx = (uint32_t)(items[k].g - sn.parts[s]->g);
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
            rc = merge_stream(cc, srcs, ns, start, end, fold_emit, &fc);
            if (rc == TSDB_ERR_STOPPED)
                rc = TSDB_ERR_NOMEM;
            for (size_t k = 0; k < ns; k++)
                src_free(&srcs[k]);
        }
        a = b;
    }
    if (rc == 0 && nwork) {
        agg_job j = {.db = db, .qs = cur_qstats, .work = work, .nwork = nwork, .t = t};
        atomic_init(&j.next, 0);
        atomic_init(&j.err, 0);
        for (int k = 0; k < AGG_STRIPES; k++)
            pthread_mutex_init(&j.locks[k], NULL);
        int threads = nwork >= PARALLEL_MIN_GRANULES ? (int)MIN((size_t)db->opt.query_threads, nwork) : 1;
        pool_run(db->qpool, threads, agg_worker, &j);
        for (int k = 0; k < AGG_STRIPES; k++)
            pthread_mutex_destroy(&j.locks[k]);
        rc = atomic_load(&j.err);
    }
    if (rc == 0) {
        size_t nonempty = 0;
        for (size_t k = 0; k < nb; k++)
            nonempty += acc[k].rows != 0;
        tsdb_bucket *res = malloc((nonempty ? nonempty : 1) * sizeof *res);
        if (!res) {
            rc = TSDB_ERR_NOMEM;
        } else {
            size_t o = 0;
            for (size_t k = 0; k < nb; k++) {
                if (!acc[k].rows)
                    continue;
                res[o].start = bk_start(bk, t.b0 + (int64_t)k);
                agg_to_bucket(type, &acc[k], &res[o]);
                o++;
            }
            *out = res;
            *nout = o;
        }
    }
done:
    if (cc)
        cc_put(db, cc);
    free(fc.scratch);
    free(acc);
    free(work);
    free(srcs);
    free(items);
    snapshot_release(&sn);
    return rc;
}

/* -------------------------------------------------------------------- raw */

static int cmp_src_first(const void *a, const void *b)
{
    int64_t x = src_first_ts(a), y = src_first_ts(b);
    return (x > y) - (x < y);
}

/* Streams [start, end) of one series from a snapshot, in time order. */
static int raw_range(eng *db, codec_ctx *cc, const snapshot *sn, uint32_t series, int type, int64_t start,
                     int64_t end, slot_fn fn, void *ctx)
{
    size_t w = type_width(type);
    src *cur = calloc(sn->np + sn->nmem + 1, sizeof *cur);
    size_t nc = 0;
    int rc = cur ? 0 : TSDB_ERR_NOMEM;
    (void)db;
    for (size_t i = 0; rc == 0 && i < sn->np; i++) {
        uint32_t gi, gj;
        part_find(sn->parts[i], series, start, end, &gi, &gj);
        if (gi < gj) {
            src_init_part(&cur[nc++], sn->parts[i], gi, gj, type);
            QS_ADD(parts, 1);
            QS_ADD(granules, gj - gi);
        }
    }
    /* memtable runs were cut to the snapshot's range: cut them to this one */
    mem_run *clipped = calloc(sn->nmem + 1, sizeof *clipped);
    if (!clipped)
        rc = TSDB_ERR_NOMEM;
    for (size_t i = 0; rc == 0 && i < sn->nmem; i++) {
        const mem_run *r = &sn->mem[i];
        size_t lo = lower_bound_i64(r->ts, r->n, start), hi = lower_bound_i64(r->ts, r->n, end);
        if (lo >= hi)
            continue;
        clipped[i] = (mem_run){r->ts + lo, r->v + lo * w, hi - lo};
        QS_ADD(memtable_rows, hi - lo);
        src_init_mem(&cur[nc++], &clipped[i], mem_prio(sn, i), type);
    }
    if (rc || nc == 0)
        goto out;

    qsort(cur, nc, sizeof *cur, cmp_src_first);
    bool disjoint = true;
    for (size_t i = 1; i < nc; i++)
        if (src_last_ts(&cur[i - 1]) >= src_first_ts(&cur[i]))
            disjoint = false;
    if (disjoint) {
        /* no timestamp can appear twice: stream the sources one after another */
        for (size_t i = 0; i < nc && rc == 0; i++) {
            src *c = &cur[i];
            rc = src_fill(cc, c, start, end);
            while (rc == 0 && c->loaded) {
                if (fn(ctx, c->ts + c->pos, c->v + c->pos * c->w, c->n - c->pos)) {
                    rc = TSDB_ERR_STOPPED;
                    break;
                }
                rc = src_advance(cc, c, c->n - c->pos, start, end);
            }
        }
    } else {
        rc = merge_stream(cc, cur, nc, start, end, fn, ctx);
    }
out:
    for (size_t i = 0; i < nc; i++)
        src_free(&cur[i]);
    free(cur);
    free(clipped);
    return rc;
}

/* ascending with a limit: stop once enough rows went out */
typedef struct limit_ctx {
    slot_fn fn;
    void *ctx;
    size_t left;
    bool done;
} limit_ctx;

static int limit_cb(void *arg, const int64_t *ts, const void *v, size_t n)
{
    limit_ctx *l = arg;
    size_t take = n < l->left ? n : l->left;
    if (take && l->fn(l->ctx, ts, v, take))
        return 1;
    l->left -= take;
    if (l->left == 0) {
        l->done = true;
        return 1;
    }
    return 0;
}

/* descending: a window of rows is collected in time order, then sent back to front */
typedef struct window_buf {
    int64_t *ts;
    uint8_t *v;
    size_t n, cap, w;
} window_buf;

static int window_cb(void *arg, const int64_t *ts, const void *v, size_t n)
{
    window_buf *w = arg;
    if (w->n + n > w->cap) {
        size_t cap = MAX(w->cap * 2, w->n + n);
        int64_t *t2 = realloc(w->ts, cap * sizeof *t2);
        if (!t2)
            return 1;
        w->ts = t2;
        uint8_t *v2 = realloc(w->v, cap * w->w);
        if (!v2)
            return 1;
        w->v = v2;
        w->cap = cap;
    }
    memcpy(w->ts + w->n, ts, n * sizeof *ts);
    memcpy(w->v + w->n * w->w, v, n * w->w);
    w->n += n;
    return 0;
}

typedef struct span {
    int64_t lo;
    uint64_t rows;
} span;

static int cmp_span_desc(const void *a, const void *b)
{
    int64_t x = ((const span *)a)->lo, y = ((const span *)b)->lo;
    return (x < y) - (x > y);
}

size_t desc_window_rows = 65536; /* tests make it small to exercise many windows */

/* Newest first: walks the range backwards in windows of about
 * desc_window_rows rows, sized from the granule index, all from one snapshot. */
static int raw_desc(eng *db, codec_ctx *cc, const snapshot *sn, uint32_t series, int type, int64_t start,
                    int64_t end, size_t limit, slot_fn fn, void *ctx)
{
    size_t wd = type_width(type);
    size_t ns = 0, cap = 64;
    span *sp = malloc(cap * sizeof *sp);
    int rc = sp ? 0 : TSDB_ERR_NOMEM;
    for (size_t i = 0; rc == 0 && i < sn->np + sn->nmem; i++) {
        uint32_t gi = 0, gj = 1;
        if (i < sn->np)
            part_find(sn->parts[i], series, start, end, &gi, &gj);
        for (uint32_t k = gi; k < gj; k++) {
            if (ns == cap) {
                span *s2 = realloc(sp, (cap *= 2) * sizeof *sp);
                if (!s2) {
                    rc = TSDB_ERR_NOMEM;
                    break;
                }
                sp = s2;
            }
            if (i < sn->np)
                sp[ns++] = (span){sn->parts[i]->g[k].ts_min, sn->parts[i]->g[k].count};
            else if (sn->mem[i - sn->np].n)
                sp[ns++] = (span){sn->mem[i - sn->np].ts[0], sn->mem[i - sn->np].n};
        }
    }
    if (rc == 0 && ns)
        qsort(sp, ns, sizeof *sp, cmp_span_desc);
    window_buf w = {.w = wd};
    int64_t *rt = NULL;
    uint8_t *rv = NULL;
    size_t rcap = 0, sent = 0, k = 0;
    int64_t hi = end;
    while (rc == 0 && hi > start && (limit == 0 || sent < limit)) {
        /* the window's lower edge: enough granules starting at or after it */
        uint64_t rows = 0;
        int64_t lo = start;
        while (k < ns) {
            if (sp[k].lo >= hi) {
                k++;
                continue;
            }
            rows += sp[k].rows;
            lo = MAX(sp[k].lo, start);
            k++;
            if (rows >= desc_window_rows)
                break;
        }
        if (k >= ns && rows < desc_window_rows)
            lo = start;
        w.n = 0;
        rc = raw_range(db, cc, sn, series, type, lo, hi, window_cb, &w);
        if (rc == TSDB_ERR_STOPPED)
            rc = TSDB_ERR_NOMEM; /* window_cb only stops when it cannot allocate */
        if (rc)
            break;
        if (w.n > rcap) {
            free(rt);
            free(rv);
            rt = malloc(w.n * sizeof *rt);
            rv = malloc(w.n * wd);
            rcap = rt && rv ? w.n : 0;
            if (!rcap) {
                rc = TSDB_ERR_NOMEM;
                break;
            }
        }
        size_t m = w.n;
        if (limit && m > limit - sent)
            m = limit - sent;
        for (size_t i = 0; i < m; i++) {
            rt[i] = w.ts[w.n - 1 - i];
            memcpy(rv + i * wd, w.v + (w.n - 1 - i) * wd, wd);
        }
        if (m && fn(ctx, rt, rv, m)) {
            rc = TSDB_ERR_STOPPED;
            break;
        }
        sent += m;
        if (lo <= start)
            break;
        hi = lo;
    }
    free(w.ts);
    free(w.v);
    free(rt);
    free(rv);
    free(sp);
    return rc;
}

int eng_query_raw_ex(eng *db, uint32_t series, int64_t start, int64_t end, bool descending, size_t limit,
                     slot_fn fn, void *ctx)
{
    int type = eng_series_type(db, series);
    if (start >= end || type < 0)
        return 0;
    snapshot sn;
    int rc = snapshot_take(db, series, type_width(type), start, end, &sn);
    if (rc)
        return rc;
    codec_ctx *cc = cc_get(db);
    if (!cc) {
        snapshot_release(&sn);
        return TSDB_ERR_NOMEM;
    }
    if (descending) {
        rc = raw_desc(db, cc, &sn, series, type, start, end, limit, fn, ctx);
    } else if (limit) {
        limit_ctx l = {fn, ctx, limit, false};
        rc = raw_range(db, cc, &sn, series, type, start, end, limit_cb, &l);
        if (rc == TSDB_ERR_STOPPED && l.done)
            rc = 0;
    } else {
        rc = raw_range(db, cc, &sn, series, type, start, end, fn, ctx);
    }
    cc_put(db, cc);
    snapshot_release(&sn);
    return rc;
}

void tsdb_free(void *p) { free(p); }

void tsdb_points_free(tsdb_points *p)
{
    free(p->ts);
    free(p->values);
    memset(p, 0, sizeof *p);
}
