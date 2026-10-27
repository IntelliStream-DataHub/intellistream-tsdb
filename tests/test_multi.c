/* tsdb_query_multi: every kind, mode, order and limit equals the single-series call, byte for byte, on data with
 * all five types, nulls, memtable rows, unmerged overlaps, deletes, replaces, retention and two volumes; for 1, 2,
 * 7 and 32 threads and with forced deferral; stop, cancel, max_rows, statistics, TRIM against *_group_local
 * across DST, and one cut per request (no insert seen by some series of a request and not by others), also over 12 volumes. */
#include "internal.h"
#include "tsdb.h"
#include "tsdb_calendar.h"

#include <math.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            exit(1);                                                                 \
        }                                                                            \
    } while (0)
#define OK(expr)                                                                                          \
    do {                                                                                                  \
        int rc_ = (expr);                                                                                 \
        if (rc_ != 0) {                                                                                   \
            fprintf(stderr, "%s:%d: %s = %d (%s)\n", __FILE__, __LINE__, #expr, rc_, tsdb_strerror(rc_)); \
            exit(1);                                                                                      \
        }                                                                                                 \
    } while (0)

static const int64_t T0 = 1711846800000LL; /* 2024-03-31 01:00 UTC: Oslo's DST starts that night */
static int64_t clock_now;
static int64_t test_clock(void *arg)
{
    (void)arg;
    return clock_now;
}

static char root[512], vol2[512];
static uint32_t ids[16];
static size_t nids;
static uint32_t big; /* two rows 5 M ms apart: interval 1 ms is TOO_MANY_BUCKETS */

static size_t api_w(tsdb_type t) { return t == TSDB_BOOL ? 1 : t == TSDB_FLOAT64 || t == TSDB_INT64 ? 8 : 4; }

static void put(tsdb *db, uint32_t id, tsdb_type t, const int64_t *ts, size_t n, int salt, int nulls)
{
    uint8_t *v = calloc(n, 8), *valid = calloc((n + 7) / 8, 1);
    for (size_t i = 0; i < n; i++) {
        double x = sin((double)(ts[i] / 1000 + salt) * 0.01) * 100 + salt;
        switch (t) {
        case TSDB_FLOAT32: ((float *)v)[i] = (float)x; break;
        case TSDB_FLOAT64: ((double *)v)[i] = x * 1.000001; break;
        case TSDB_INT64: ((int64_t *)v)[i] = (int64_t)(x * 1000); break;
        case TSDB_BOOL: v[i] = x > salt; break;
        case TSDB_STATE: ((uint32_t *)v)[i] = 1 + (uint32_t)(ts[i] / 7000 + salt) % 5; break;
        }
        if (!nulls || i % 13 != 5)
            valid[i / 8] |= (uint8_t)(1u << (i % 8));
    }
    tsdb_values vals = {t, v, nulls ? valid : NULL};
    OK(tsdb_insert_series_values(db, id, ts, &vals, n));
    free(v);
    free(valid);
}

static void fill(tsdb *db)
{
    int64_t *ts = malloc(40000 * sizeof *ts);
    char name[64];
    for (int vol = 0; vol < 2; vol++)
        for (int t = 0; t < 5; t++) {
            uint32_t id;
            snprintf(name, sizeof name, "s%d_%d", vol, t);
            OK(tsdb_series_create(db, name, (tsdb_type)t, vol ? vol2 : NULL, &id));
            if (t == TSDB_STATE)
                for (int k = 0; k < 5; k++) {
                    uint32_t code;
                    char st[16];
                    snprintf(st, sizeof st, "st%d", k);
                    OK(tsdb_state(db, id, st, &code));
                }
            ids[nids++] = id;
        }
    /* batch A: 30000 rows, 1 s apart with jitter, over a DST change; flushed */
    for (size_t k = 0; k < nids; k++) {
        tsdb_type t;
        OK(tsdb_series_type(db, ids[k], &t));
        for (size_t i = 0; i < 30000; i++)
            ts[i] = T0 - 3600000LL * 4 + (int64_t)i * 1000 + (int64_t)((i * 7 + k) % 5) * 100;
        put(db, ids[k], t, ts, 30000, (int)k, 0);
    }
    OK(tsdb_flush(db));
    /* batch B overlaps A's middle with other values and nulls: a second part over the same range */
    for (size_t k = 0; k < nids; k++) {
        tsdb_type t;
        OK(tsdb_series_type(db, ids[k], &t));
        for (size_t i = 0; i < 6000; i++)
            ts[i] = T0 - 3600000LL + (int64_t)i * 2000 + 300;
        put(db, ids[k], t, ts, 6000, (int)k + 50, 1);
    }
    OK(tsdb_flush(db));
    /* deletes and replaces, in both volumes */
    OK(tsdb_delete_range(db, ids[1], T0, T0 + 600000));
    OK(tsdb_delete_range(db, ids[7], T0 - 7200000, T0 - 7000000));
    {
        int64_t rts[100];
        for (int i = 0; i < 100; i++)
            rts[i] = T0 + 3600000 + i * 1500;
        tsdb_type t;
        OK(tsdb_series_type(db, ids[2], &t));
        uint8_t v[800];
        for (int i = 0; i < 100; i++)
            ((int64_t *)v)[i] = i * 3;
        tsdb_values vals = {t, v, NULL};
        OK(tsdb_replace_range(db, ids[2], T0 + 3600000, T0 + 3800000, rts, &vals, 100));
    }
    /* batch C: in the memtable */
    for (size_t k = 0; k < nids; k++) {
        tsdb_type t;
        OK(tsdb_series_type(db, ids[k], &t));
        for (size_t i = 0; i < 2000; i++)
            ts[i] = T0 + 30000LL * 1000 - 3600000LL * 4 + (int64_t)i * 1000 - 500000;
        put(db, ids[k], t, ts, 2000, (int)k + 99, 1);
    }
    /* retention: the first hour of one series has expired */
    clock_now = T0 + 86400000LL;
    OK(tsdb_set_retention(db, ids[5], 86400000LL + 3 * 3600000LL));
    OK(tsdb_series_create(db, "big", TSDB_FLOAT64, NULL, &big));
    int64_t bts[2] = {T0, T0 + 5000000};
    double bv[2] = {1, 2};
    tsdb_values bvals = {TSDB_FLOAT64, bv, NULL};
    OK(tsdb_insert_series_values(db, big, bts, &bvals, 2));
    free(ts);
}

/* ------------------------------------------------------------ reference: the single calls */

typedef struct res {
    int status;
    size_t n;
    int64_t *ts;
    uint8_t *vals, *valid;
    int type;
    void *buckets;
} res;

static void res_free(res *r)
{
    free(r->ts);
    free(r->vals);
    free(r->valid);
    free(r->buckets);
    memset(r, 0, sizeof *r);
}

static int res_values(void *ctx, const int64_t *ts, const tsdb_values *v, size_t n)
{
    res *r = ctx;
    size_t w = api_w(v->type);
    r->type = v->type;
    r->ts = realloc(r->ts, (r->n + n) * 8);
    r->vals = realloc(r->vals, (r->n + n) * w);
    r->valid = realloc(r->valid, (r->n + n + 7) / 8 + 1);
    for (size_t i = 0; i < n; i++) {
        size_t k = r->n + i;
        bool b = !v->valid || (v->valid[i / 8] >> (i % 8) & 1);
        if (k % 8 == 0)
            r->valid[k / 8] = 0;
        if (b)
            r->valid[k / 8] |= (uint8_t)(1u << (k % 8));
    }
    memcpy(r->ts + r->n, ts, n * 8);
    memcpy(r->vals + r->n * w, v->values, n * w);
    r->n += n;
    return 0;
}

static size_t row_of(tsdb_multi_kind k)
{
    return k == TSDB_MULTI_BUCKETS ? sizeof(tsdb_bucket)
           : k == TSDB_MULTI_SPREAD ? sizeof(tsdb_spread_bucket)
                                    : sizeof(tsdb_twa_bucket);
}

/* group: a zone through *_group_local, the reference of TRIM */
static void single(tsdb *db, const tsdb_multi_query *q, const tsdb_group *local, size_t i, uint32_t id, res *r)
{
    memset(r, 0, sizeof *r);
    int64_t s = q->starts ? q->starts[i] : q->start, e = q->ends ? q->ends[i] : q->end;
    int64_t iv = q->intervals ? q->intervals[i] : q->interval_ms;
    void *out = NULL;
    size_t nb = 0;
    int rc;
    switch (q->kind) {
    case TSDB_MULTI_VALUES: rc = tsdb_query_values(db, id, s, e, &q->opts, res_values, r); break;
    case TSDB_MULTI_LTTB: rc = tsdb_query_lttb_ex(db, id, s, e, q->threshold, &q->opts, res_values, r); break;
    default:
        if (local) {
            if (q->kind == TSDB_MULTI_BUCKETS)
                rc = tsdb_query_group_local(db, id, s, e, local, &q->opts, (tsdb_bucket **)&out, &nb);
            else if (q->kind == TSDB_MULTI_SPREAD)
                rc = tsdb_query_spread_group_local(db, id, s, e, local, &q->opts, (tsdb_spread_bucket **)&out, &nb);
            else
                rc = tsdb_query_twa_group_local(db, id, s, e, local, &q->twa, (tsdb_twa_bucket **)&out, &nb);
        } else if (q->nbounds) {
            if (q->kind == TSDB_MULTI_BUCKETS)
                rc = tsdb_query_bounds(db, id, s, e, q->bounds, q->nbounds, &q->opts, (tsdb_bucket **)&out, &nb);
            else if (q->kind == TSDB_MULTI_SPREAD)
                rc = tsdb_query_spread_bounds(db, id, s, e, q->bounds, q->nbounds, &q->opts,
                                              (tsdb_spread_bucket **)&out, &nb);
            else
                rc = tsdb_query_twa_bounds(db, id, s, e, q->bounds, q->nbounds, &q->twa, (tsdb_twa_bucket **)&out,
                                           &nb);
        } else if (q->group) {
            if (q->kind == TSDB_MULTI_BUCKETS)
                rc = tsdb_query_group_local(db, id, s, e, q->group, &q->opts, (tsdb_bucket **)&out, &nb);
            else if (q->kind == TSDB_MULTI_SPREAD)
                rc = tsdb_query_spread_group_local(db, id, s, e, q->group, &q->opts, (tsdb_spread_bucket **)&out,
                                                   &nb);
            else
                rc = tsdb_query_twa_group_local(db, id, s, e, q->group, &q->twa, (tsdb_twa_bucket **)&out, &nb);
        } else {
            if (q->kind == TSDB_MULTI_BUCKETS)
                rc = tsdb_query_agg_ex(db, id, s, e, iv, &q->opts, (tsdb_bucket **)&out, &nb);
            else if (q->kind == TSDB_MULTI_SPREAD)
                rc = tsdb_query_spread(db, id, s, e, iv, &q->opts, (tsdb_spread_bucket **)&out, &nb);
            else
                rc = tsdb_query_twa(db, id, s, e, iv, &q->twa, (tsdb_twa_bucket **)&out, &nb);
        }
        r->buckets = out;
        r->n = nb;
    }
    r->status = rc;
    if (rc) {
        free(r->buckets);
        r->buckets = NULL;
        r->n = 0;
        free(r->ts), free(r->vals), free(r->valid);
        r->ts = NULL, r->vals = NULL, r->valid = NULL;
    }
}

static char cur_mode[64];

/* Float sums, and the TWA areas and variances built from them, are combined in the order parallel decode finishes
 * (docs: "they may differ in the last bits with the order in which rows were combined"), and a request's series
 * get as many decode threads as are free: those fields may differ in the last bits. Everything else is exact. */
static bool dbl_close(const void *x, const void *y)
{
    double a, b;
    memcpy(&a, x, 8);
    memcpy(&b, y, 8);
    if (memcmp(&a, &b, 8) == 0 || (isnan(a) && isnan(b)))
        return true;
    return fabs(a - b) <= 1e-12 * fmax(fabs(a), fabs(b));
}

static bool bucket_same(tsdb_multi_kind kind, const char *x, const char *y)
{
    /* the double fields that are sums or come from sums, by 8-byte word */
    static const int agg[] = {2}, spread[] = {2, 9, 10, 11, 12}, twa[] = {1, 2};
    const int *f = kind == TSDB_MULTI_TWA ? twa : kind == TSDB_MULTI_SPREAD ? spread : agg;
    size_t nf = kind == TSDB_MULTI_TWA ? 2 : kind == TSDB_MULTI_SPREAD ? 5 : 1;
    size_t words = row_of(kind) / 8;
    for (size_t w = 0; w < words; w++) {
        bool sum = false;
        for (size_t j = 0; j < nf; j++)
            sum |= (size_t)f[j] == w;
        if (sum ? !dbl_close(x + 8 * w, y + 8 * w) : memcmp(x + 8 * w, y + 8 * w, 8) != 0)
            return false;
    }
    return true;
}
static void same(const tsdb_multi_query *q, const res *a, const tsdb_series_result *b, const char *what, size_t i)
{
    if (a->status != b->status || a->n != b->n) {
        fprintf(stderr, "%s: series #%zu (%u): single status %d n %zu, multi status %d n %zu\n", what, i, b->series,
                a->status, a->n, b->status, b->n);
        exit(1);
    }
    if (!a->n)
        return;
    if (q->kind == TSDB_MULTI_VALUES || q->kind == TSDB_MULTI_LTTB) {
        CHECK(b->values.type == (tsdb_type)a->type);
        CHECK(memcmp(a->ts, b->ts, a->n * 8) == 0);
        size_t w = api_w(b->values.type);
        for (size_t k = 0; k < a->n; k++) {
            bool va = a->valid[k / 8] >> (k % 8) & 1;
            bool vb = !b->values.valid || (b->values.valid[k / 8] >> (k % 8) & 1);
            CHECK(va == vb);
            if (va && memcmp(a->vals + k * w, (const uint8_t *)b->values.values + k * w, w) != 0) {
                fprintf(stderr, "%s: series #%zu row %zu differs\n", what, i, k);
                exit(1);
            }
        }
    } else if (memcmp(a->buckets, b->buckets, a->n * row_of(q->kind)) != 0) {
        size_t row = row_of(q->kind);
        for (size_t k = 0; k < a->n; k++)
            if (!bucket_same(q->kind, (const char *)a->buckets + k * row, (const char *)b->buckets + k * row)) {
                const double *x = (const void *)((const char *)a->buckets + k * row);
                const double *y = (const void *)((const char *)b->buckets + k * row);
                fprintf(stderr, "%s: series #%zu bucket %zu of %zu differs (%s):", what, i, k, a->n, cur_mode);
                for (size_t f = 0; f < row / 8; f++)
                    fprintf(stderr, " [%zu] %.17g/%.17g", f, x[f], y[f]);
                fprintf(stderr, "\n");
                exit(1);
            }
    }
}

static uint32_t list[64];
static size_t nlist;

static void compare(tsdb *db, const tsdb_multi_query *q, const tsdb_group *local, const char *what)
{
    static const int threads[] = {1, 2, 7, 32};
    res *ref = calloc(nlist, sizeof *ref);
    for (size_t i = 0; i < nlist; i++) {
        single(db, q, local, i, list[i], &ref[i]);
        /* tsdb_query_twa_group_local looks the type up first; TRIM answers an unknown id as the clients' local
         * groups and every single request do: no rows */
        if (local && ref[i].status == TSDB_ERR_NOTFOUND && list[i] == 999999)
            ref[i].status = 0;
    }
    for (int defer = 0; defer < 3; defer++) {
        for (size_t t = 0; t < sizeof threads / sizeof *threads; t++) {
            size_t sb = multi_defer_bytes, sr = multi_defer_rows;
            tsdb_multi_opts mo = {.threads = threads[t]};
            if (defer == 1) /* every series ahead of the head defers */
                multi_defer_bytes = 1, multi_defer_rows = 1;
            if (defer == 2) /* results ahead have no room: one at a time once the first does not fit */
                mo.max_held_bytes = 1;
            tsdb_series_result *out;
            OK(tsdb_query_multi_collect(db, q, list, nlist, &mo, &out));
            multi_defer_bytes = sb, multi_defer_rows = sr;
            snprintf(cur_mode, sizeof cur_mode, "threads %d defer %d", threads[t], defer);
            for (size_t i = 0; i < nlist; i++) {
                CHECK(out[i].index == i && out[i].series == list[i]);
                same(q, &ref[i], &out[i], what, i);
            }
            tsdb_series_results_free(out, nlist);
        }
    }
    for (size_t i = 0; i < nlist; i++)
        res_free(&ref[i]);
    free(ref);
}

static void kinds(tsdb *db)
{
    int64_t starts[64], ends[64], ivs[64];
    for (size_t i = 0; i < nlist; i++) {
        starts[i] = T0 - 3600000LL * 3 + (int64_t)i * 1234567 % 7200000;
        ends[i] = starts[i] + 3600000LL * (1 + (int64_t)i % 9);
        ivs[i] = (int64_t)(1 + i % 4) * 60000;
    }
    const tsdb_query_opts opts[] = {{TSDB_ASC, 0}, {TSDB_DESC, 0}, {TSDB_ASC, 100}, {TSDB_DESC, 7}};
    const int64_t ranges[][2] = {{INT64_MIN, INT64_MAX}, {T0 - 3600000, T0 + 7200000}};
    char what[128];
    for (size_t o = 0; o < 4; o++)
        for (size_t r = 0; r < 3; r++) {
            tsdb_multi_query q = {.kind = TSDB_MULTI_VALUES, .opts = opts[o]};
            if (r < 2)
                q.start = ranges[r][0], q.end = ranges[r][1];
            else
                q.starts = starts, q.ends = ends;
            snprintf(what, sizeof what, "values o%zu r%zu", o, r);
            compare(db, &q, NULL, what);
            q.kind = TSDB_MULTI_LTTB;
            q.threshold = 300;
            snprintf(what, sizeof what, "lttb o%zu r%zu", o, r);
            compare(db, &q, NULL, what);
        }
    tsdb_group hour = {TSDB_HOUR, 1, NULL}, oslo = {TSDB_DAY, 1, "Europe/Oslo"}, lh = {TSDB_HOUR, 6, "Australia/Lord_Howe"};
    int64_t *bounds;
    size_t nb;
    OK(tsdb_calendar_bounds("Europe/Oslo", TSDB_HOUR, 2, T0 - 86400000, T0 + 2 * 86400000, &bounds, &nb));
    for (int kind = TSDB_MULTI_BUCKETS; kind <= TSDB_MULTI_TWA; kind++)
        for (size_t o = 0; o < 4; o++)
            for (int mode = 0; mode < 6; mode++)
                for (size_t r = 0; r < 3; r++) {
                    tsdb_multi_query q = {.kind = (tsdb_multi_kind)kind, .opts = opts[o]};
                    q.twa.order = opts[o].order;
                    q.twa.limit = opts[o].limit;
                    q.twa.method = (tsdb_twa_method)(r & 1);
                    if (r < 2)
                        q.start = ranges[r][0], q.end = ranges[r][1];
                    else
                        q.starts = starts, q.ends = ends;
                    switch (mode) {
                    case 0: q.interval_ms = 0; break;
                    case 1: q.interval_ms = 600000; break;
                    case 2: q.intervals = ivs; break;
                    case 3: q.group = &hour; break;
                    case 4: q.group = &oslo; break;
                    case 5: q.bounds = bounds, q.nbounds = nb; break;
                    }
                    snprintf(what, sizeof what, "kind %d o%zu mode %d r%zu", kind, o, mode, r);
                    compare(db, &q, NULL, what);
                }
    free(bounds);
    /* TRIM: one union of local bounds serves every series, as *_group_local per series computes them */
    const tsdb_group *zones[] = {&oslo, &lh};
    for (int z = 0; z < 2; z++) {
        OK(tsdb_calendar_bounds(zones[z]->timezone, zones[z]->unit, zones[z]->count, T0 - 86400000 * 3,
                                T0 + 86400000 * 3, &bounds, &nb));
        for (int kind = TSDB_MULTI_BUCKETS; kind <= TSDB_MULTI_TWA; kind++)
            for (size_t r = 0; r < 3; r++) {
                tsdb_multi_query q = {.kind = (tsdb_multi_kind)kind, .bounds = bounds, .nbounds = nb};
                if (r < 2)
                    q.start = ranges[r][0], q.end = ranges[r][1];
                else
                    q.starts = starts, q.ends = ends;
                /* buckets trim both sides; TWA only the open ones */
                if (kind != TSDB_MULTI_TWA)
                    q.trim = TSDB_MULTI_TRIM_LO | TSDB_MULTI_TRIM_HI;
                else
                    q.trim = (r == 0 ? TSDB_MULTI_TRIM_LO | TSDB_MULTI_TRIM_HI : 0);
                if (kind == TSDB_MULTI_TWA && !q.trim)
                    continue; /* closed ranges: the union of bounds is the reference, tested above */
                snprintf(what, sizeof what, "trim zone %d kind %d r%zu", z, kind, r);
                compare(db, &q, zones[z], what);
            }
        free(bounds);
    }
}

/* ------------------------------------------------------------ stop, cancel, max_rows, statistics */

typedef struct counter {
    int calls, stop_at;
    uint64_t rows;
    size_t last_index;
    int lasts;
} counter;

static int count_fn(void *ctx, const tsdb_series_result *r)
{
    counter *c = ctx;
    c->calls++;
    c->rows += r->status ? 0 : r->n;
    CHECK(r->index >= c->last_index);
    c->last_index = r->index;
    c->lasts += r->last;
    return c->stop_at && c->lasts >= c->stop_at; /* after stop_at series */
}

static int cancel_after;
static int cancelled_fn(void *ctx)
{
    (void)ctx;
    return --cancel_after < 0;
}

static void control(tsdb *db)
{
    tsdb_multi_query q = {.kind = TSDB_MULTI_VALUES, .start = INT64_MIN, .end = INT64_MAX};
    for (int t = 1; t <= 8; t *= 2) {
        tsdb_multi_opts mo = {.threads = t};
        counter c = {0};
        OK(tsdb_query_multi(db, &q, list, nlist, &mo, count_fn, &c));
        CHECK(c.lasts == (int)nlist);
        uint64_t total = c.rows;
        int calls = c.calls;
        /* fn stops: no call after it */
        (void)calls;
        counter s = {.stop_at = (int)nlist / 2};
        CHECK(tsdb_query_multi(db, &q, list, nlist, &mo, count_fn, &s) == TSDB_ERR_STOPPED);
        CHECK(s.lasts == (int)nlist / 2 && s.last_index == nlist / 2 - 1);
        /* cancelled between series */
        cancel_after = 3;
        mo.cancelled = cancelled_fn;
        counter k = {0};
        CHECK(tsdb_query_multi(db, &q, list, nlist, &mo, count_fn, &k) == TSDB_ERR_STOPPED);
        CHECK(k.lasts <= 3); /* polled before every step of the conductor, not only between deliveries */
        mo.cancelled = NULL;
        /* max_rows: the series that would pass it is not sent */
        mo.max_rows = total - 1;
        counter m = {0};
        CHECK(tsdb_query_multi(db, &q, list, nlist, &mo, count_fn, &m) == TSDB_ERR_TOO_MANY_BUCKETS);
        CHECK(m.rows <= total - 1);
        mo.max_rows = total;
        counter m2 = {0};
        OK(tsdb_query_multi(db, &q, list, nlist, &mo, count_fn, &m2));
        CHECK(m2.rows == total);
    }
    /* statistics are the sums of the single calls' (one thread: nothing is run twice) */
    tsdb_query_stats sum = {0}, st;
    for (size_t i = 0; i < nlist; i++) {
        res r;
        single(db, &q, NULL, i, list[i], &r);
        res_free(&r);
        tsdb_last_query_stats(&st);
        sum.rows_decoded += st.rows_decoded;
        sum.rows_from_stats += st.rows_from_stats;
        sum.memtable_rows += st.memtable_rows;
        sum.parts += st.parts;
        sum.granules += st.granules;
        sum.result_rows += st.result_rows;
    }
    tsdb_multi_opts one = {.threads = 1};
    counter c = {0};
    OK(tsdb_query_multi(db, &q, list, nlist, &one, count_fn, &c));
    tsdb_last_query_stats(&st);
    CHECK(st.rows_decoded == sum.rows_decoded && st.rows_from_stats == sum.rows_from_stats);
    CHECK(st.memtable_rows == sum.memtable_rows && st.parts == sum.parts && st.granules == sum.granules);
    CHECK(st.result_rows == sum.result_rows && st.result_rows == c.rows);
    /* bad queries */
    tsdb_multi_query bad = {.kind = (tsdb_multi_kind)9};
    CHECK(tsdb_query_multi(db, &bad, list, nlist, NULL, count_fn, &c) == TSDB_ERR_INVALID);
    bad.kind = TSDB_MULTI_BUCKETS;
    bad.trim = 4;
    CHECK(tsdb_query_multi(db, &bad, list, nlist, NULL, count_fn, &c) == TSDB_ERR_INVALID);
}

/* ------------------------------------------------------------ one cut per request */

typedef struct cutrun {
    tsdb *db;
    uint32_t *s;
    size_t n;
    atomic_int stop;
    atomic_long checked, torn;
} cutrun;

static void *cut_writer(void *a)
{
    cutrun *r = a;
    int64_t *ts = malloc(r->n * sizeof *ts);
    float *v = malloc(r->n * sizeof *v);
    for (int64_t g = 1; !atomic_load(&r->stop); g++) {
        for (size_t i = 0; i < r->n; i++)
            ts[i] = T0 + 100000000 + g, v[i] = (float)g;
        OK(tsdb_insert(r->db, r->s, ts, v, r->n)); /* one insert: every series or none */
        if (g % 500 == 0)
            OK(tsdb_flush(r->db));
    }
    free(ts);
    free(v);
    return NULL;
}

static int newest_fn(void *ctx, const tsdb_series_result *r)
{
    int64_t *last = ctx;
    if (r->n)
        last[r->index] = r->ts[0];
    return 0;
}

static void *cut_reader(void *a)
{
    cutrun *r = a;
    int64_t *last = malloc(r->n * sizeof *last);
    tsdb_multi_query q = {.kind = TSDB_MULTI_VALUES, .start = T0 + 100000000, .end = INT64_MAX,
                          .opts = {TSDB_DESC, 1}};
    tsdb_multi_opts mo = {.threads = 4};
    while (!atomic_load(&r->stop)) {
        for (size_t i = 0; i < r->n; i++)
            last[i] = -1;
        OK(tsdb_query_multi(r->db, &q, r->s, r->n, &mo, newest_fn, last));
        for (size_t i = 1; i < r->n; i++)
            if (last[i] != last[0])
                atomic_fetch_add(&r->torn, 1);
        atomic_fetch_add(&r->checked, 1);
    }
    free(last);
    return NULL;
}

static void cuts(tsdb *db, double secs)
{
    cutrun r = {.db = db, .n = 32};
    r.s = malloc(r.n * sizeof *r.s);
    for (size_t i = 0; i < r.n; i++) {
        char name[32];
        snprintf(name, sizeof name, "cut%zu", i);
        OK(tsdb_series(db, name, &r.s[i]));
    }
    pthread_t w, rd[3];
    pthread_create(&w, NULL, cut_writer, &r);
    for (int i = 0; i < 3; i++)
        pthread_create(&rd[i], NULL, cut_reader, &r);
    usleep((useconds_t)(secs * 1e6));
    atomic_store(&r.stop, 1);
    pthread_join(w, NULL);
    for (int i = 0; i < 3; i++)
        pthread_join(rd[i], NULL);
    printf("  cuts: %ld requests over %zu series, %ld torn\n", atomic_load(&r.checked), r.n, atomic_load(&r.torn));
    CHECK(atomic_load(&r.checked) > 0);
    CHECK(atomic_load(&r.torn) == 0);
    free(r.s);
}

/* More volumes than the request's first room for cuts (4): the first series of the 5th, 9th... volume grows the
 * array while other threads read their cuts (a use-after-free before each cut was allocated on its own; make asan
 * sees it). Series interleaved over the volumes, so first touches race; answers equal single queries. */
static void many_volumes(const char *base)
{
    enum { NV = 12, PER = 6, ROWS = 6000 };
    char r[600], vol[700], name[64], cmd[1400];
    snprintf(r, sizeof r, "%s/test_multi-%d-nv", base, (int)getpid());
    snprintf(cmd, sizeof cmd, "rm -rf '%s'*", r);
    if (system(cmd)) {
    }
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.merge_threads = 0;
    o.query_threads = 32;
    o.granule_rows = 1024;
    tsdb *db;
    OK(tsdb_open(r, &o, &db));
    uint32_t vids[NV * PER], order[NV * PER];
    int64_t *ts = malloc(ROWS * sizeof *ts);
    float *v = malloc(ROWS * sizeof *v);
    uint32_t *sid = malloc(ROWS * sizeof *sid);
    CHECK(ts && v && sid);
    for (int k = 0; k < NV; k++) {
        snprintf(vol, sizeof vol, "%s-vol%d", r, k);
        for (int j = 0; j < PER; j++) {
            snprintf(name, sizeof name, "nv%d_%d", k, j);
            uint32_t id;
            OK(tsdb_series_create(db, name, TSDB_FLOAT32, k ? vol : NULL, &id));
            for (int i = 0; i < ROWS; i++)
                ts[i] = T0 + (int64_t)i * 1000, v[i] = (float)(i % 97 + k + j), sid[i] = id;
            OK(tsdb_insert(db, sid, ts, v, ROWS));
            vids[k * PER + j] = id;
        }
    }
    free(ts);
    free(v);
    free(sid);
    size_t m = 0;
    for (int j = 0; j < PER; j++)
        for (int k = 0; k < NV; k++)
            order[m++] = vids[k * PER + j];
    tsdb_multi_query q = {.kind = TSDB_MULTI_BUCKETS, .start = INT64_MIN, .end = INT64_MAX, .interval_ms = 60000};
    tsdb_bucket *ref[NV * PER];
    size_t nref[NV * PER];
    for (size_t i = 0; i < m; i++)
        OK(tsdb_query_agg_ex(db, order[i], q.start, q.end, q.interval_ms, NULL, &ref[i], &nref[i]));
    for (int it = 0; it < 60; it++) {
        tsdb_series_result *out;
        tsdb_multi_opts mo = {.threads = 32};
        OK(tsdb_query_multi_collect(db, &q, order, m, &mo, &out));
        for (size_t i = 0; i < m; i++) {
            CHECK(out[i].status == 0 && out[i].n == nref[i]);
            CHECK(memcmp(out[i].buckets, ref[i], nref[i] * sizeof(tsdb_bucket)) == 0);
        }
        tsdb_series_results_free(out, m);
    }
    for (size_t i = 0; i < m; i++)
        tsdb_free(ref[i]);
    OK(tsdb_close(db));
    if (system(cmd)) {
    }
}

/* The memory bounds of one request and a series that fails ahead of the head. The head's first delivery waits, so
 * helpers run the others ahead (at most W = 2 x threads). Budget hooks count what results ahead hold. */
typedef struct {
    atomic_long out, peak, takes, deferred;
} budget;
static int b_take(void *ctx, size_t n)
{
    budget *b = ctx;
    long o = atomic_fetch_add(&b->out, (long)n) + (long)n, p = atomic_load(&b->peak);
    while (o > p && !atomic_compare_exchange_weak(&b->peak, &p, o)) {
    }
    atomic_fetch_add(&b->takes, 1);
    return 0;
}
static void b_give(void *ctx, size_t n) { atomic_fetch_sub(&((budget *)ctx)->out, (long)n); }
static void b_deferred(void *ctx) { atomic_fetch_add(&((budget *)ctx)->deferred, 1); }
typedef struct {
    int status[64];
    uint64_t rows;
} slow_ctx;
static int slow_head(void *ctx, const tsdb_series_result *r)
{
    slow_ctx *s = ctx;
    if (r->index == 0 && r->last)
        usleep(300000);
    s->rows += r->status ? 0 : r->n;
    if (r->last && r->index < 64)
        s->status[r->index] = r->status;
    return 0;
}

static void find_part(const char *dir, char *out, size_t len)
{
    DIR *d = opendir(dir);
    CHECK(d != NULL);
    struct dirent *e;
    bool found = false;
    while (!found && (e = readdir(d)) != NULL) {
        size_t n = strlen(e->d_name);
        if (e->d_name[0] == 'p' && n > 5 && strcmp(e->d_name + n - 5, ".part") == 0)
            found = snprintf(out, len, "%s/%s", dir, e->d_name) > 0;
    }
    closedir(d);
    CHECK(found);
}

static void bounds(const char *base)
{
    enum { ROWS = 20000 };
    char r[600], vol[700], cmd[1400], pfile[1000];
    snprintf(r, sizeof r, "%s/test_multi-%d-bd", base, (int)getpid());
    snprintf(vol, sizeof vol, "%s-bad", r);
    snprintf(cmd, sizeof cmd, "rm -rf '%s'*", r);
    if (system(cmd)) {
    }
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.merge_threads = 0;
    o.query_threads = 8;
    o.granule_rows = 1024;
    tsdb *db;
    OK(tsdb_open(r, &o, &db));
    uint32_t good[8], wide, bad;
    int64_t *ts = malloc(ROWS * sizeof *ts);
    float *v = malloc(ROWS * sizeof *v);
    CHECK(ts && v);
    for (int i = 0; i < ROWS; i++)
        ts[i] = T0 + (int64_t)i * 1000, v[i] = (float)(i % 101);
    char name[32];
    for (int k = 0; k < 8; k++) {
        snprintf(name, sizeof name, "bd%d", k);
        OK(tsdb_series_create(db, name, TSDB_FLOAT32, NULL, &good[k]));
        OK(tsdb_insert_series(db, good[k], ts, v, ROWS));
    }
    OK(tsdb_series_create(db, "bd_bad", TSDB_FLOAT32, vol, &bad)); /* alone in its volume: one part to damage */
    OK(tsdb_insert_series(db, bad, ts, v, ROWS));
    int64_t two[2] = {T0, T0 + 20000000};
    float tv[2] = {1, 2};
    OK(tsdb_series_create(db, "bd_wide", TSDB_FLOAT32, NULL, &wide)); /* 20,000 buckets of 1 s for two rows */
    OK(tsdb_insert_series(db, wide, two, tv, 2));
    OK(tsdb_close(db));
    find_part(vol, pfile, sizeof pfile);
    {
        /* a flipped byte in the last granule's values: rows come, then CORRUPT */
        int dfd = open(vol, O_RDONLY | O_DIRECTORY);
        part *pt;
        CHECK(dfd >= 0);
        OK(part_open(dfd, strrchr(pfile, '/') + 1, &pt));
        CHECK(pt->ng >= 10);
        const granule *g = &pt->g[pt->ng - 1];
        off_t at = (off_t)(g->off + g->ts_len + g->val_len / 2);
        part_unref(pt);
        int fd = openat(dfd, strrchr(pfile, '/') + 1, O_RDWR);
        uint8_t b;
        CHECK(fd >= 0 && pread(fd, &b, 1, at) == 1);
        b ^= 0x5a;
        CHECK(pwrite(fd, &b, 1, at) == 1);
        close(fd);
        close(dfd);
    }
    o.verify_checksums = 1;
    OK(tsdb_open(r, &o, &db));
    uint32_t list[17];
    tsdb_multi_query qv = {.kind = TSDB_MULTI_VALUES, .start = INT64_MIN, .end = INT64_MAX};

    /* a series that fails ahead holds nothing, and is never charged for the rows it had before the error */
    list[0] = good[0];
    for (int i = 1; i < 9; i++)
        list[i] = bad;
    budget b = {0};
    tsdb_multi_opts mo = {.threads = 8, .budget_take = b_take, .budget_give = b_give, .on_deferred = b_deferred,
                          .hook_ctx = &b};
    slow_ctx sc = {0};
    OK(tsdb_query_multi(db, &qv, list, 9, &mo, slow_head, &sc));
    CHECK(sc.status[0] == 0);
    for (int i = 1; i < 9; i++)
        CHECK(sc.status[i] == TSDB_ERR_CORRUPT);
    CHECK(atomic_load(&b.takes) == 0 && atomic_load(&b.out) == 0);
    CHECK(atomic_load(&b.deferred) == 0); /* and nothing flipped the call to one series at a time */

    /* results ahead stop at max_held_bytes: a refused one makes the call head-only */
    for (int i = 0; i < 16; i++)
        list[i] = good[i % 8];
    memset(&b, 0, sizeof b);
    memset(&sc, 0, sizeof sc);
    mo.max_held_bytes = 600 << 10; /* one result of 20,000 float32 rows (room for 32,768: 384 KiB) fits */
    OK(tsdb_query_multi(db, &qv, list, 16, &mo, slow_head, &sc));
    CHECK(sc.rows == 16 * (uint64_t)ROWS);
    fprintf(stderr, "  held ahead: peak %ld KiB of %zu KiB, %ld deferred\n", atomic_load(&b.peak) >> 10,
            mo.max_held_bytes >> 10, atomic_load(&b.deferred));
    /* each helper takes its result's 384 KiB before the check against max_held and gives it back when refused */
    CHECK(atomic_load(&b.peak) <= (long)mo.max_held_bytes + 8 * (400 << 10));
    CHECK(atomic_load(&b.deferred) >= 1 && atomic_load(&b.out) == 0); /* refused, not merely held less */
    mo.max_held_bytes = 0;

    /* bucket accumulators over multi_defer_bytes ahead of the head defer to its turn, before allocating */
    size_t sb = multi_defer_bytes;
    multi_defer_bytes = 64 << 10;
    tsdb_multi_query qb = {.kind = TSDB_MULTI_BUCKETS, .start = INT64_MIN, .end = INT64_MAX, .interval_ms = 1000};
    list[0] = good[0];
    for (int i = 1; i < 9; i++)
        list[i] = wide;
    memset(&b, 0, sizeof b);
    memset(&sc, 0, sizeof sc);
    OK(tsdb_query_multi(db, &qb, list, 9, &mo, slow_head, &sc));
    multi_defer_bytes = sb;
    for (int i = 0; i < 9; i++)
        CHECK(sc.status[i] == 0);
    tsdb_bucket *one;
    size_t n0, nw;
    OK(tsdb_query_agg_ex(db, good[0], qb.start, qb.end, qb.interval_ms, NULL, &one, &n0));
    tsdb_free(one);
    OK(tsdb_query_agg_ex(db, wide, qb.start, qb.end, qb.interval_ms, NULL, &one, &nw));
    tsdb_free(one);
    CHECK(sc.rows == n0 + 8 * nw);
    CHECK(atomic_load(&b.deferred) >= 1);

    /* so does LTTB's buffer of two buckets: 3 points of 20,000 rows buffer 2 x 20,000 points (640 KiB) */
    multi_defer_bytes = 64 << 10;
    tsdb_multi_query ql = {.kind = TSDB_MULTI_LTTB, .start = INT64_MIN, .end = INT64_MAX, .threshold = 3};
    for (int i = 0; i < 9; i++)
        list[i] = good[i % 8];
    memset(&b, 0, sizeof b);
    memset(&sc, 0, sizeof sc);
    OK(tsdb_query_multi(db, &ql, list, 9, &mo, slow_head, &sc));
    multi_defer_bytes = sb;
    for (int i = 0; i < 9; i++)
        CHECK(sc.status[i] == 0);
    CHECK(sc.rows == 9 * 3);
    fprintf(stderr, "  LTTB ahead: %ld deferred\n", atomic_load(&b.deferred));
    CHECK(atomic_load(&b.deferred) >= 1);

    /* TRIM keeps a series' bounds within one request's: 49 hourly bounds, of which 5.6 hours of rows need 7 */
    int64_t hb[49];
    for (int i = 0; i < 49; i++)
        hb[i] = T0 - 24 * 3600000LL + (int64_t)i * 3600000;
    size_t sm = multi_max_bounds;
    multi_max_bounds = 8;
    tsdb_multi_query qt = {.kind = TSDB_MULTI_BUCKETS, .start = INT64_MIN, .end = INT64_MAX, .bounds = hb,
                           .nbounds = 49, .trim = TSDB_MULTI_TRIM_LO | TSDB_MULTI_TRIM_HI};
    OK(tsdb_query_bounds(db, good[0], qt.start, qt.end, hb, 49, NULL, &one, &n0));
    tsdb_free(one);
    CHECK(n0 == 6);
    for (int trim = 0; trim < 3; trim++) {
        qt.trim = trim == 0 ? TSDB_MULTI_TRIM_LO | TSDB_MULTI_TRIM_HI : trim == 1 ? TSDB_MULTI_TRIM_LO
                                                                                    : TSDB_MULTI_TRIM_HI;
        memset(&sc, 0, sizeof sc);
        OK(tsdb_query_multi(db, &qt, good, 8, NULL, slow_head, &sc));
        for (int i = 0; i < 8; i++)
            CHECK(sc.status[i] == (trim == 0 ? 0 : TSDB_ERR_TOO_MANY_BUCKETS));
        CHECK(sc.rows == (trim == 0 ? 8 * n0 : 0));
    }
    multi_max_bounds = sm;
    OK(tsdb_close(db));
    free(ts);
    free(v);
    if (system(cmd)) {
    }
}

int main(int argc, char **argv)
{
    const char *base = argc > 1 ? argv[1] : "/tmp";
    snprintf(root, sizeof root, "%s/test_multi-%d", base, (int)getpid());
    snprintf(vol2, sizeof vol2, "%s/test_multi-%d-vol2", base, (int)getpid());
    char cmd[1200];
    snprintf(cmd, sizeof cmd, "rm -rf '%s' '%s'", root, vol2);
    if (system(cmd)) {
    }
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.merge_threads = 0; /* overlaps stay unmerged */
    o.query_threads = 32;
    o.clock_ms = test_clock;
    o.granule_rows = 1024;
    clock_now = T0;
    tsdb *db;
    OK(tsdb_open(root, &o, &db));
    fill(db);
    /* every series, a duplicate, an unknown id, the big one */
    for (size_t i = 0; i < nids; i++)
        list[nlist++] = ids[i];
    list[nlist++] = ids[3];
    list[nlist++] = 999999;
    list[nlist++] = big;
    list[nlist++] = ids[0];
    {
        /* the big series over 4 M buckets: its own status */
        tsdb_multi_query q = {.kind = TSDB_MULTI_BUCKETS, .start = INT64_MIN, .end = INT64_MAX, .interval_ms = 1};
        tsdb_series_result *out;
        OK(tsdb_query_multi_collect(db, &q, list, nlist, NULL, &out));
        CHECK(out[nlist - 2].status == TSDB_ERR_TOO_MANY_BUCKETS);
        CHECK(out[nlist - 3].status == 0 && out[nlist - 3].n == 0); /* unknown id */
        tsdb_series_results_free(out, nlist);
        compare(db, &q, NULL, "big");
        q.kind = TSDB_MULTI_TWA;
        compare(db, &q, NULL, "big twa");
    }
    kinds(db);
    printf("  kinds: equal to the single calls\n");
    control(db);
    printf("  stop, cancel, max_rows, statistics\n");
    OK(tsdb_close(db));
    /* again with a WAL synced on every write: rows are visible once durable */
    o.fsync = 1;
    o.merge_threads = 2;
    o.flush_interval_ms = 50;
    OK(tsdb_open(root, &o, &db));
    cuts(db, 2.0);
    OK(tsdb_close(db));
    many_volumes(base);
    printf("  %d volumes in one request\n", 12);
    bounds(base);
    printf("  held results, deferral and failures ahead\n");
    snprintf(cmd, sizeof cmd, "rm -rf '%s' '%s'", root, vol2);
    if (system(cmd)) {
    }
    printf("test_multi: ok\n");
    return 0;
}
