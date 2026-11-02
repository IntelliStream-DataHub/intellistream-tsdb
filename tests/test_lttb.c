/*
 * LTTB downsampling against a reference: the straightforward array version of
 * Steinarsson's algorithm, run over what tsdb_query_values returns for the
 * same range (nulls, NaN and infinities dropped), must pick exactly the points
 * tsdb_query_lttb streams, with their stored bits. Many shapes, every numeric
 * type, irregular timestamps, thresholds around n, ranges that cut granules;
 * data in memtables, in overlapping parts with corrections (last write wins),
 * merged, and seen by a follower; both the buffered and the two-pass way.
 * Deletes, replaces and retention are checked against a model kept in memory.
 */
#define _GNU_SOURCE
#include "internal.h"
#include "tsdb.h"
#include "lttb_ref.h"

#include <dirent.h>
#include <stddef.h>
#include <fcntl.h>
#include <math.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int failures;
#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                              \
        }                                                                            \
    } while (0)
#define OK(expr)                                                                                    \
    do {                                                                                            \
        int rc_ = (expr);                                                                           \
        if (rc_ != 0) {                                                                             \
            fprintf(stderr, "%s:%d: %s -> %s\n", __FILE__, __LINE__, #expr, tsdb_strerror(rc_)); \
            exit(1);                                                                                \
        }                                                                                           \
    } while (0)

static uint64_t rng = 0x9e3779b97f4a7c15ull;
static uint64_t rnd(void)
{
    rng ^= rng << 13;
    rng ^= rng >> 7;
    rng ^= rng << 17;
    return rng;
}
static double rndu(void) { return (double)(rnd() >> 11) / 9007199254740992.0; }

typedef struct pts {
    pt *p;
    size_t n, cap;
    bool all;      /* keep nulls, NaN and infinities too (to look at the output) */
    bool had_valid; /* a chunk came with a validity bitmap */
} pts;

static int collect(void *ctx, const int64_t *ts, const tsdb_values *col, size_t n)
{
    pts *o = ctx;
    o->had_valid |= col->valid != NULL;
    for (size_t i = 0; i < n; i++) {
        uint64_t bits = 0;
        double y = 0;
        if (!col_point(col, i, &bits, &y) && !o->all)
            continue;
        if (o->n == o->cap) {
            o->cap = o->cap ? o->cap * 2 : 1024;
            o->p = realloc(o->p, o->cap * sizeof *o->p);
        }
        o->p[o->n++] = (pt){ts[i], bits, y};
    }
    return 0;
}

/* An independent model of a series' rows from the corrections on (ok: a point LTTB may pick), changed by hand
 * for deletes, replaces, inserts and retention: a bug in the read path that LTTB and tsdb_query_values share
 * cannot hide in a reference read from the same database. */
typedef struct mrow {
    int64_t ts;
    uint64_t bits;
    double y;
    bool ok;
} mrow;
typedef struct model {
    mrow *r;
    size_t n, cap;
} model;

static size_t model_lower(const model *m, int64_t t)
{
    size_t lo = 0, hi = m->n;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (m->r[mid].ts < t)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo;
}

/* last write wins: rows at existing timestamps are replaced (nulls too) */
static void model_put(model *m, const int64_t *ts, const tsdb_values *col, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        mrow r = {ts[i], 0, 0, false};
        r.ok = col_point(col, i, &r.bits, &r.y);
        size_t at = model_lower(m, ts[i]);
        if (at < m->n && m->r[at].ts == ts[i]) {
            m->r[at] = r;
            continue;
        }
        if (m->n == m->cap) {
            m->cap = m->cap ? m->cap * 2 : 1024;
            m->r = realloc(m->r, m->cap * sizeof *m->r);
        }
        memmove(m->r + at + 1, m->r + at, (m->n - at) * sizeof *m->r);
        m->r[at] = r;
        m->n++;
    }
}

static void model_delete(model *m, int64_t a, int64_t b)
{
    size_t i = model_lower(m, a), j = model_lower(m, b);
    if (i < j) {
        memmove(m->r + i, m->r + j, (m->n - j) * sizeof *m->r);
        m->n -= j - i;
    }
}

static int model_cb(void *ctx, const int64_t *ts, const tsdb_values *col, size_t n)
{
    model_put(ctx, ts, col, n);
    return 0;
}

/* the model's rows in [a, b): every one (all) or the points */
static void model_points(const model *m, int64_t a, int64_t b, bool all, pts *out)
{
    for (size_t i = model_lower(m, a); i < m->n && m->r[i].ts < b; i++) {
        if (!all && !m->r[i].ok)
            continue;
        if (out->n == out->cap) {
            out->cap = out->cap ? out->cap * 2 : 1024;
            out->p = realloc(out->p, out->cap * sizeof *out->p);
        }
        out->p[out->n++] = (pt){m->r[i].ts, m->r[i].bits, m->r[i].y};
    }
}

static int checks;

/* tsdb_query_lttb against the reference over the model m, or else over tsdb_query_values on truth, and the
 * properties */
static void check_one(tsdb *db, tsdb *truth, const model *md, tsdb_sid s, int64_t start, int64_t end, size_t t,
                      const char *what)
{
    pts raw = {0}, got = {.all = true};
    if (!md || truth == db) { /* with a model, only for the statistics */
        OK(tsdb_query_values(truth, s, start, end, NULL, collect, &raw));
        if (md)
            raw.n = 0;
    }
    tsdb_query_stats rs;
    tsdb_last_query_stats(&rs);
    if (md)
        model_points(md, start, end, false, &raw);
    OK(tsdb_query_lttb(db, s, start, end, t, collect, &got));
    tsdb_query_stats ls;
    tsdb_last_query_stats(&ls);
    size_t *idx = malloc((raw.n + 1) * sizeof *idx);
    size_t m = ref_lttb(raw.p, raw.n, t, idx);
    checks++;
    bool ok = got.n == m && got.n == (raw.n < t ? raw.n : t) && !got.had_valid;
    for (size_t i = 0; ok && i < m; i++)
        ok = got.p[i].ts == raw.p[idx[i]].ts && got.p[i].bits == raw.p[idx[i]].bits;
    for (size_t i = 1; ok && i < got.n; i++)
        ok = got.p[i].ts > got.p[i - 1].ts;
    if (ok && m) /* first and last kept */
        ok = got.p[0].ts == raw.p[0].ts && got.p[m - 1].ts == raw.p[raw.n - 1].ts;
    if (ok && db == truth && m) /* statistics: one read of the range (none when the count found no point) */
        ok = ls.result_rows == got.n && ls.rows == rs.rows && ls.parts == rs.parts;
    if (!ok) {
        fprintf(stderr, "%s: series %" PRIu64 " [%lld, %lld) threshold %zu: %zu points of %zu, expected %zu\n", what, s,
                (long long)start, (long long)end, t, got.n, raw.n, m);
        for (size_t i = 0; i < m && i < got.n; i++)
            if (got.p[i].ts != raw.p[idx[i]].ts || got.p[i].bits != raw.p[idx[i]].bits) {
                fprintf(stderr, "  first difference at %zu: ts %lld, expected %lld\n", i, (long long)got.p[i].ts,
                        (long long)raw.p[idx[i]].ts);
                break;
            }
        failures++;
    }
    free(idx);
    free(raw.p);
    free(got.p);
}

#define T0 1700000000000LL

static void check_series(tsdb *db, tsdb *truth, const model *md, tsdb_sid s, const char *what)
{
    pts all = {.all = true};
    if (md)
        model_points(md, TSDB_TS_MIN, TSDB_TS_MAX, true, &all);
    else
        OK(tsdb_query_values(truth, s, TSDB_TS_MIN, TSDB_TS_MAX, NULL, collect, &all));
    if (all.n == 0) {
        free(all.p);
        return;
    }
    int64_t lo = all.p[0].ts, hi = all.p[all.n - 1].ts + 1;
    pts known = {0};
    if (md)
        model_points(md, lo, hi, false, &known);
    else
        OK(tsdb_query_values(truth, s, lo, hi, NULL, collect, &known));
    size_t n = known.n;
    free(known.p);
    size_t ths[] = {3, 4, 5, 7, 10, 100, n > 3 ? n - 1 : 3, n, n + 1, 2 * n + 5, 1000000, 3 + rnd() % (n + 1)};
    size_t saved = lttb_buffer_bytes;
    for (int way = 0; way < 2; way++) {
        lttb_buffer_bytes = way ? 0 : saved; /* 0: always the two-pass way */
        for (size_t k = 0; k < sizeof ths / sizeof *ths; k++)
            check_one(db, truth, md, s, TSDB_TS_MIN, TSDB_TS_MAX, ths[k], what);
        for (int r = 0; r < 6; r++) { /* ranges that cut granules */
            int64_t a = all.p[rnd() % all.n].ts + (int64_t)(rnd() % 3) - 1;
            int64_t b = all.p[rnd() % all.n].ts + (int64_t)(rnd() % 3) - 1;
            if (a > b) {
                int64_t x = a;
                a = b;
                b = x;
            }
            check_one(db, truth, md, s, a, b + 1, 3 + rnd() % 40, what);
            check_one(db, truth, md, s, a, b + 1, 3 + rnd() % (n + 2), what);
        }
    }
    lttb_buffer_bytes = saved;
    free(all.p);
}

enum { WALK, STEPS, SPIKES, CONSTANT, MONOTONIC, NANINF, HUGE, NSHAPES };
static const char *shape_name[] = {"walk", "steps", "spikes", "constant", "monotonic", "nan-inf", "huge"};

/* one value of a shape as a double (the caller converts); *special: 1 NaN, 2 +inf, 3 -inf */
static double shape_value(int shape, size_t i, double *walk, int *special)
{
    *special = 0;
    switch (shape) {
    case WALK: *walk += rndu() * 2 - 1; return *walk;
    case STEPS: return (double)((i / 97) % 5) * 10;
    case SPIKES: return rnd() % 50 == 0 ? 1000 + (double)(rnd() % 1000) : (double)(rnd() % 3);
    case CONSTANT: return 42;
    case MONOTONIC: return (double)i * 0.5;
    case NANINF:
        if (rnd() % 20 == 0)
            *special = 1 + (int)(rnd() % 3);
        return sin((double)i / 50) * 100;
    default: return (rnd() & 1 ? 1 : -1) * 1.5e308 * rndu();
    }
}

static void fill(int type, int shape, size_t n, int64_t *ts, void *vals, uint8_t *valid, int64_t *clock,
                 double *walk, size_t base)
{
    memset(valid, 0xff, (n + 7) / 8);
    for (size_t i = 0; i < n; i++) {
        *clock += 1 + (int64_t)(rnd() % (rnd() % 7 == 0 ? 50000 : 1000)); /* irregular */
        ts[i] = *clock;
        int special;
        double v = shape_value(shape, base + i, walk, &special);
        if (rnd() % 23 == 0)
            valid[i >> 3] &= (uint8_t)~(1u << (i & 7));
        switch (type) {
        case TSDB_FLOAT32:
            ((float *)vals)[i] = special == 1 ? NAN : special == 2 ? INFINITY : special == 3 ? -INFINITY
                               : shape == HUGE ? (float)(v / 1e270) : (float)v;
            break;
        case TSDB_FLOAT64:
            ((double *)vals)[i] = special == 1 ? NAN : special == 2 ? INFINITY : special == 3 ? -INFINITY : v;
            break;
        case TSDB_INT64:
            ((int64_t *)vals)[i] = shape == HUGE ? (int64_t)(v / 1e290) : (int64_t)llround(v * 1000);
            break;
        default:
            ((uint8_t *)vals)[i] = shape == CONSTANT ? 1 : v > 0 ? (uint8_t)(rnd() % 4 != 0) : (uint8_t)(rnd() % 4 == 0);
            break;
        }
    }
}

static const int types[] = {TSDB_FLOAT32, TSDB_FLOAT64, TSDB_INT64, TSDB_BOOL};
static const char *type_name[] = {"float32", "float64", "int64", "bool"};

#define NSER (4 * NSHAPES)

static model models[NSER];

/* ms: the models of the series (NULL: read the reference from truth) */
static void check_all(tsdb *db, tsdb *truth, const model *ms, const tsdb_sid *ids, const char *what)
{
    for (int k = 0; k < NSER; k++) {
        char w[128];
        snprintf(w, sizeof w, "%s %s %s", what, type_name[k / NSHAPES], shape_name[k % NSHAPES]);
        check_series(db, truth, ms ? &ms[k] : NULL, ids[k], w);
    }
}

/* hand-checked: the largest triangle, and the first of equal ones */
static void test_by_hand(tsdb *db)
{
    tsdb_sid s, b;
    OK(tsdb_series_create(db, "hand", TSDB_FLOAT64, NULL, &s));
    int64_t ts[5] = {T0, T0 + 1, T0 + 2, T0 + 3, T0 + 4};
    double v[5] = {0, 1, 10, 2, 0};
    OK(tsdb_insert_series_values(db, s, ts, &(tsdb_values){TSDB_FLOAT64, v, NULL}, 5));
    pts got = {0};
    OK(tsdb_query_lttb(db, s, T0, T0 + 5, 3, collect, &got));
    CHECK(got.n == 3 && got.p[0].ts == T0 && got.p[1].ts == T0 + 2 && got.p[2].ts == T0 + 4);
    got.n = 0;
    OK(tsdb_series_create(db, "tie", TSDB_FLOAT64, NULL, &b));
    double v2[5] = {0, 5, 1, 5, 0};
    OK(tsdb_insert_series_values(db, b, ts, &(tsdb_values){TSDB_FLOAT64, v2, NULL}, 5));
    OK(tsdb_query_lttb(db, b, T0, T0 + 5, 3, collect, &got));
    CHECK(got.n == 3 && got.p[1].ts == T0 + 1);

    /* the paper's bucket bounds: with n = 17 and threshold 13, every = 15/11 and 11 * every rounds
     * below 15, so the last bucket is point 14 alone and point 15 only enters the last mean: the
     * spike there is not chosen (exact integer bounds would choose it) */
    tsdb_sid pp;
    OK(tsdb_series_create(db, "paper", TSDB_FLOAT64, NULL, &pp));
    int64_t pts17[32];
    double pv[32];
    for (int i = 0; i < 32; i++) {
        pts17[i] = T0 + i;
        pv[i] = i == 15 ? 100 : (i * 7919 % 13) / 4.0;
    }
    OK(tsdb_insert_series_values(db, pp, pts17, &(tsdb_values){TSDB_FLOAT64, pv, NULL}, 17));
    got.n = 0;
    OK(tsdb_query_lttb(db, pp, T0, T0 + 17, 13, collect, &got));
    CHECK(got.n == 13 && got.p[11].ts == T0 + 14 && got.p[12].ts == T0 + 16);
    check_one(db, db, NULL, pp, T0, T0 + 17, 13, "paper bounds");
    OK(tsdb_insert_series_values(db, pp, pts17 + 17, &(tsdb_values){TSDB_FLOAT64, pv + 17, NULL}, 15));
    size_t saved = lttb_buffer_bytes;
    for (int way = 0; way < 2; way++) {
        lttb_buffer_bytes = way ? 0 : saved;
        for (size_t t = 3; t < 32; t++) /* 13, 15, 24 and 28 differ from integer bounds */
            check_one(db, db, NULL, pp, T0, T0 + 32, t, "paper bounds");
    }
    lttb_buffer_bytes = saved;
    free(got.p);

    /* the contract's errors */
    tsdb_sid st;
    OK(tsdb_series_create(db, "state", TSDB_STATE, NULL, &st));
    pts none = {0};
    for (size_t t = 0; t < 3; t++)
        CHECK(tsdb_query_lttb(db, s, T0, T0 + 5, t, collect, &none) == TSDB_ERR_INVALID);
    CHECK(tsdb_query_lttb(db, st, T0, T0 + 5, 10, collect, &none) == TSDB_ERR_INVALID);
    CHECK(tsdb_query_lttb(db, s, T0 + 5, T0, 10, collect, &none) == 0 && none.n == 0);
    CHECK(tsdb_query_lttb(db, TSDB_SID(0, 999999), T0, T0 + 5, 10, collect, &none) == 0 && none.n == 0);
    free(none.p);
}

static int stop_cb(void *ctx, const int64_t *ts, const tsdb_values *col, size_t n) { return 1; }

static int count_cb(void *ctx, const int64_t *ts, const tsdb_values *col, size_t n)
{
    *(size_t *)ctx += n;
    return 0;
}

/* A granule index that promises more points than the blocks hold (its null count zeroed, the footer
 * hash forged to match) is CORRUPT; the two-pass way finds it in the first pass and sends nothing. */
static void test_count_mismatch(const char *base)
{
    char dir[600];
    snprintf(dir, sizeof dir, "%s-forged", base);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.granule_rows = 64;
    o.wal_max_batch_rows = 100; /* the insert becomes a part */
    o.merge_threads = 0;
    tsdb *db;
    tsdb_sid s;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series_create(db, "nulls", TSDB_FLOAT64, NULL, &s));
    enum { N = 2000 };
    static int64_t ts[N];
    static double v[N];
    static uint8_t valid[N / 8];
    for (int i = 0; i < N; i++) {
        ts[i] = T0 + i;
        v[i] = sin(i / 10.0);
        if (i % 10)
            valid[i >> 3] |= (uint8_t)(1u << (i & 7));
    }
    OK(tsdb_insert_series_values(db, s, ts, &(tsdb_values){TSDB_FLOAT64, v, valid}, N));
    OK(tsdb_flush(db));
    OK(tsdb_close(db));

    int forged = 0;
    DIR *d = opendir(dir);
    struct dirent *e;
    while (d && (e = readdir(d)) != NULL) {
        size_t l = strlen(e->d_name);
        if (e->d_name[0] != 'p' || l < 5 || strcmp(e->d_name + l - 5, ".part") != 0)
            continue;
        char path[1200];
        snprintf(path, sizeof path, "%s/%s", dir, e->d_name);
        FILE *f = fopen(path, "r+b");
        if (!f)
            continue;
        fseek(f, 0, SEEK_END);
        long size = ftell(f);
        uint8_t *buf = malloc((size_t)size);
        rewind(f);
        if (fread(buf, 1, (size_t)size, f) == (size_t)size) {
            /* the index decoded, every null count cleared, encoded again as the writer does; the file rebuilt
             * around it (its length may change) and rehashed */
            part_footer ft;
            memcpy(&ft, buf + size - sizeof ft, sizeof ft);
            part pp;
            memset(&pp, 0, sizeof pp);
            granule *gr = calloc(ft.ngranules + 1, sizeof *gr);
            CHECK(gr && gix_parse(buf + ft.index_off, (size_t)ft.index_len, ft.ngranules, &pp.ix) == 0);
            for (uint32_t g = 0; g < ft.ngranules; g++) {
                gx_load(&pp, g, GW_ALL, &gr[g]);
                forged += gr[g].nulls != 0;
                gr[g].nulls = 0;
            }
            uint8_t *ix;
            size_t ilen;
            CHECK(gix_encode(gr, ft.ngranules, &ix, &ilen) == 0);
            size_t tail = (size_t)size - (size_t)(ft.index_off + ft.index_len), nlen = (size_t)ft.index_off + ilen + tail;
            uint8_t *out = malloc(nlen);
            memcpy(out, buf, (size_t)ft.index_off);
            memcpy(out + ft.index_off, ix, ilen);
            memcpy(out + ft.index_off + ilen, buf + ft.index_off + ft.index_len, tail);
            ft.index_len = ilen;
            uint64_t x = xxh64(out, sizeof(part_header), 0);
            x = xxh64(out + ft.index_off, ilen, x);
            if (tail > sizeof ft) /* tombstones */
                x = xxh64(out + ft.index_off + ilen, tail - sizeof ft, x);
            ft.index_hash = xxh64(&ft, offsetof(part_footer, index_hash), x);
            memcpy(out + nlen - sizeof ft, &ft, sizeof ft);
            CHECK(ftruncate(fileno(f), 0) == 0);
            rewind(f);
            CHECK(fwrite(out, 1, nlen, f) == nlen);
            free(out);
            free(ix);
            free(gr);
        }
        free(buf);
        fclose(f);
    }
    if (d)
        closedir(d);
    CHECK(forged > 0);

    OK(tsdb_open(dir, &o, &db));
    size_t saved = lttb_buffer_bytes, sent = 0;
    lttb_buffer_bytes = 0; /* the two-pass way */
    CHECK(tsdb_query_lttb(db, s, TSDB_TS_MIN, TSDB_TS_MAX, 1000, count_cb, &sent) == TSDB_ERR_CORRUPT);
    CHECK(sent == 0);
    lttb_buffer_bytes = saved;
    CHECK(tsdb_query_lttb(db, s, TSDB_TS_MIN, TSDB_TS_MAX, 1000, count_cb, &sent) == TSDB_ERR_CORRUPT);
    OK(tsdb_close(db));
    char cmd[700];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (system(cmd) != 0)
        failures++;
}

/* Concurrent writes, flushes and merges do not tear a result: a writer rewrites runs of existing
 * timestamps (n stays the same) while queries check the count, the order and the fixed ends. */
enum { CN = 60000 };
static tsdb *cdb;
static tsdb_sid cs;
static atomic_int cstop;
static int64_t cts[CN];

typedef struct cgot {
    size_t n;
    int64_t first, last;
    int bad;
} cgot;

static int conc_cb(void *ctx, const int64_t *ts, const tsdb_values *col, size_t n)
{
    cgot *g = ctx;
    for (size_t i = 0; i < n; i++) {
        if (g->n == 0)
            g->first = ts[i];
        else if (ts[i] <= g->last)
            g->bad = 1;
        g->last = ts[i];
        g->n++;
    }
    return 0;
}

static void *conc_writer(void *arg)
{
    (void)arg;
    unsigned r = 1;
    static double v[4000];
    static int64_t t[4000];
    for (int k = 1; !atomic_load(&cstop); k++) {
        size_t m = 50 + rand_r(&r) % 3900, at = rand_r(&r) % (CN - m);
        for (size_t i = 0; i < m; i++) {
            t[i] = cts[at + i];
            v[i] = (double)(rand_r(&r) % 1000);
        }
        OK(tsdb_insert_series_values(cdb, cs, t, &(tsdb_values){TSDB_FLOAT64, v, NULL}, m));
        if (k % 7 == 0)
            OK(tsdb_flush(cdb));
        if (k % 23 == 0)
            OK(tsdb_optimize(cdb));
    }
    return NULL;
}

static void test_concurrent(const char *base)
{
    char dir[600];
    snprintf(dir, sizeof dir, "%s-conc", base);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.granule_rows = 128;
    o.wal_max_batch_rows = 1000;
    OK(tsdb_open(dir, &o, &cdb));
    OK(tsdb_series_create(cdb, "x", TSDB_FLOAT64, NULL, &cs));
    static double v[CN];
    for (int i = 0; i < CN; i++) {
        cts[i] = T0 + i * 10 + (i % 3);
        v[i] = sin(i / 100.0);
    }
    for (int off = 0; off < CN; off += 10000)
        OK(tsdb_insert_series_values(cdb, cs, cts + off, &(tsdb_values){TSDB_FLOAT64, v + off, NULL}, 10000));
    pthread_t w;
    /* join only a writer that started: joining the uninitialized handle of a failed create (the process limit ran
     * out) hung the test for 10 hours */
    int prc = pthread_create(&w, NULL, conc_writer, NULL);
    if (prc != 0) {
        fprintf(stderr, "concurrent: cannot start the writer thread: %s\n", strerror(prc));
        failures++;
    }
    size_t ths[] = {3, 100, 5000, CN - 1}, saved = lttb_buffer_bytes;
    int bad = 0;
    for (int it = 0; it < 160; it++) {
        lttb_buffer_bytes = it % 8 < 4 ? saved : 0; /* both ways */
        size_t t = ths[it % 4];
        cgot g = {0};
        int rc = tsdb_query_lttb(cdb, cs, TSDB_TS_MIN, TSDB_TS_MAX, t, conc_cb, &g);
        if (rc || g.n != t || g.bad || g.first != cts[0] || g.last != cts[CN - 1]) {
            if (bad++ < 5)
                fprintf(stderr, "concurrent: threshold %zu: rc %d, %zu points, order %s\n", t, rc, g.n,
                        g.bad ? "broken" : "ok");
        }
    }
    lttb_buffer_bytes = saved;
    atomic_store(&cstop, 1);
    if (prc == 0)
        pthread_join(w, NULL);
    CHECK(bad == 0);
    OK(tsdb_close(cdb));
    char cmd[700];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (system(cmd) != 0)
        failures++;
}

#define LTTB_NOW 1800000000000LL
static int64_t lttb_clock(void *arg)
{
    (void)arg;
    return LTTB_NOW;
}

int main(int argc, char **argv)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/tsdb-lttb-%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (system(cmd) != 0)
        return 1;
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.granule_rows = 64;
    o.partition_ms = 3600000; /* several partitions */
    o.wal_max_batch_rows = 300; /* small batches stay in the memtable, larger ones become parts */
    o.buffer_rows = 1 << 22;
    o.flush_interval_ms = 600000;
    o.merge_threads = 0; /* merges only when asked for */
    o.clock_ms = lttb_clock; /* retention: expired points stay put while the checks run */
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    test_by_hand(db);

    tsdb_sid ids[NSER];
    int64_t clock[NSER];
    double walk[NSER];
    size_t base[NSER];
    enum { BATCH = 1200 };
    int64_t ts[BATCH];
    double vals[BATCH];
    uint8_t valid[(BATCH + 7) / 8];
    for (int k = 0; k < NSER; k++) {
        char name[64];
        snprintf(name, sizeof name, "%s.%s", type_name[k / NSHAPES], shape_name[k % NSHAPES]);
        OK(tsdb_series_create(db, name, types[k / NSHAPES], NULL, &ids[k]));
        clock[k] = T0;
        walk[k] = 0;
        base[k] = 0;
    }
    /* parts (large batches) and memtable rows (small ones), in time order */
    for (int round = 0; round < 6; round++)
        for (int k = 0; k < NSER; k++) {
            size_t n = round % 3 == 2 ? 150 : BATCH;
            fill(types[k / NSHAPES], k % NSHAPES, n, ts, vals, valid, &clock[k], &walk[k], base[k]);
            base[k] += n;
            OK(tsdb_insert_series_values(db, ids[k], ts, &(tsdb_values){types[k / NSHAPES], vals, valid}, n));
        }
    check_all(db, db, NULL, ids, "parts+memtable");
    OK(tsdb_flush(db));
    check_all(db, db, NULL, ids, "parts");

    /* corrections: rewrite random earlier timestamps, some to null, in overlapping parts and the memtable */
    for (int round = 0; round < 3; round++)
        for (int k = 0; k < NSER; k++) {
            pts all = {.all = true};
            OK(tsdb_query_values(db, ids[k], TSDB_TS_MIN, TSDB_TS_MAX, NULL, collect, &all));
            size_t n = round == 2 ? 100 : 700;
            int64_t c = 0;
            double w = 0;
            fill(types[k / NSHAPES], k % NSHAPES, n, ts, vals, valid, &c, &w, 0);
            size_t at = rnd() % (all.n - n);
            for (size_t i = 0; i < n; i++)
                ts[i] = all.p[at + i].ts; /* a contiguous run of existing timestamps, sorted */
            OK(tsdb_insert_series_values(db, ids[k], ts, &(tsdb_values){types[k / NSHAPES], vals, valid}, n));
            free(all.p);
        }
    check_all(db, db, NULL, ids, "corrections");

    /* deletes and replaces: the count pass must not count rows a tombstone hides (their granules' statistics
     * still hold them until a merge), in parts, with inserts on top in the memtable, and after merges; from here
     * on the reference is the model */
    for (int k = 0; k < NSER; k++)
        OK(tsdb_query_values(db, ids[k], TSDB_TS_MIN, TSDB_TS_MAX, NULL, model_cb, &models[k]));
    for (int round = 0; round < 5; round++) {
        for (int k = 0; k < NSER; k++) {
            model *md = &models[k];
            pts all = {.all = true};
            model_points(md, TSDB_TS_MIN, TSDB_TS_MAX, true, &all);
            size_t n = 1 + rnd() % (round == 3 ? 60 : 400);
            size_t at = rnd() % (all.n - 2 * n - 1);
            int64_t a = all.p[at].ts + (int64_t)(rnd() % 2), b = all.p[at + 2 * n].ts + (int64_t)(rnd() % 2);
            if (k % 3 == 0) {
                OK(tsdb_delete_range(db, ids[k], a, b));
                model_delete(md, a, b);
            } else {
                int64_t c = 0;
                double w = 0;
                fill(types[k / NSHAPES], k % NSHAPES, n, ts, vals, valid, &c, &w, 0);
                size_t m = 0;
                for (size_t i = 0; i < n; i++) /* every other old timestamp: the ones between go */
                    if (all.p[at + 1 + 2 * i].ts >= a && all.p[at + 1 + 2 * i].ts < b)
                        ts[m++] = all.p[at + 1 + 2 * i].ts;
                tsdb_values tv = {types[k / NSHAPES], vals, valid};
                OK(k % 3 == 1 ? tsdb_replace_range(db, ids[k], a, b, ts, &tv, m)
                              : tsdb_insert_series_values(db, ids[k], ts, &tv, m)); /* inserts: memtable */
                if (k % 3 == 1)
                    model_delete(md, a, b);
                model_put(md, ts, &tv, m);
                if (k % 3 == 2) {
                    OK(tsdb_delete_range(db, ids[k], all.p[at].ts, all.p[at].ts + 1)); /* one row */
                    model_delete(md, all.p[at].ts, all.p[at].ts + 1);
                }
            }
            free(all.p);
        }
        if (round == 1)
            OK(tsdb_flush(db));
        if (round == 3) /* merges drop what the tombstones hid, and keep the tombstones of rows in the memtable */
            OK(tsdb_optimize(db));
        char w[32];
        snprintf(w, sizeof w, "deletes %d", round);
        check_all(db, db, models, ids, w);
    }

    /* a follower sees the same points */
    tsdb_options fo;
    tsdb_options_init(&fo);
    fo.follow = 1;
    fo.refresh_ms = 20;
    fo.clock_ms = lttb_clock;
    tsdb *f;
    OK(tsdb_commit(db));
    OK(tsdb_flush(db));
    OK(tsdb_open(dir, &fo, &f));
    OK(tsdb_refresh(f));
    check_all(f, db, models, ids, "follower");
    OK(tsdb_close(f));

    OK(tsdb_optimize(db));
    check_all(db, db, models, ids, "merged");

    /* retention: expired points never count, merged away or not */
    for (int k = 0; k < NSER; k++) {
        int64_t lim = LTTB_NOW - models[k].r[models[k].n / 3].ts - (int64_t)(rnd() % 2);
        OK(tsdb_set_retention(db, ids[k], lim));
        model_delete(&models[k], TSDB_TS_MIN, LTTB_NOW - lim); /* rows before the cutoff expire */
    }
    check_all(db, db, models, ids, "retention");
    OK(tsdb_open(dir, &fo, &f));
    check_all(f, db, models, ids, "retention, follower");
    OK(tsdb_close(f));
    OK(tsdb_optimize(db));
    check_all(db, db, models, ids, "retention, merged");

    /* a callback that stops */
    CHECK(tsdb_query_lttb(db, ids[0], TSDB_TS_MIN, TSDB_TS_MAX, 50, stop_cb, NULL) == TSDB_ERR_STOPPED);
    CHECK(tsdb_query_lttb(db, ids[0], TSDB_TS_MIN, TSDB_TS_MAX, 1000000, stop_cb, NULL) == TSDB_ERR_STOPPED);

    OK(tsdb_close(db));
    if (system(cmd) != 0)
        failures++;
    test_count_mismatch(dir);
    test_concurrent(dir);
    printf("test_lttb: %d comparisons, %s\n", checks, failures ? "FAILED" : "ok");
    return failures != 0;
}
