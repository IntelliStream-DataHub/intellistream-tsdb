/* Tests: codec round trips, and every query checked against a brute-force reference. */
#include "internal.h"

#include <dirent.h>
#include <fcntl.h>
#include <math.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static int failures;
#define CHECK(cond)                                                                          \
    do {                                                                                     \
        if (!(cond)) {                                                                       \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond);         \
            failures++;                                                                      \
        }                                                                                    \
    } while (0)
#define OK(expr)                                                                             \
    do {                                                                                     \
        int rc_ = (expr);                                                                    \
        if (rc_ != 0) {                                                                      \
            fprintf(stderr, "%s:%d: %s -> %s\n", __FILE__, __LINE__, #expr, tsdb_strerror(rc_)); \
            exit(1);                                                                         \
        }                                                                                    \
    } while (0)

static uint64_t rng = 88172645463325252ull;
static uint64_t rnd(void)
{
    rng ^= rng << 13;
    rng ^= rng >> 7;
    rng ^= rng << 17;
    return rng;
}

/* ------------------------------------------------------------------ codecs */

static void test_dd(const int64_t *in, size_t n, const char *what)
{
    uint8_t *enc = calloc(dd_bound(n) + BLK_SLACK, 1);
    int64_t *dec = malloc((n + 1) * sizeof *dec);
    size_t len = dd_encode(in, n, enc);
    CHECK(len <= dd_bound(n));
    int rc = dd_decode(enc, len, n, dec);
    CHECK(rc == 0);
    bool same = rc == 0 && (n == 0 || memcmp(in, dec, n * sizeof *in) == 0);
    if (!same)
        fprintf(stderr, "  doubledelta mismatch: %s (n=%zu)\n", what, n);
    CHECK(same);
    free(enc);
    free(dec);
}

static void test_gorilla(const uint32_t *in, size_t n, const char *what)
{
    uint8_t *enc = calloc(gorilla_bound(n) + BLK_SLACK, 1);
    uint32_t *dec = malloc((n + 1) * sizeof *dec);
    size_t len = gorilla_encode(in, n, enc);
    CHECK(len <= gorilla_bound(n));
    int rc = gorilla_decode(enc, len, n, dec);
    CHECK(rc == 0);
    bool same = rc == 0 && (n == 0 || memcmp(in, dec, n * sizeof *in) == 0);
    if (!same)
        fprintf(stderr, "  gorilla mismatch: %s (n=%zu)\n", what, n);
    CHECK(same);
    free(enc);
    free(dec);
}

static void test_codecs(void)
{
    enum { N = 20000 };
    int64_t *t = malloc(N * sizeof *t);
    uint32_t *u = malloc(N * sizeof *u);
    float *f = (float *)u;

    for (size_t n = 0; n < 5; n++) {
        for (size_t i = 0; i < n; i++)
            t[i] = (int64_t)rnd();
        test_dd(t, n, "tiny");
    }
    for (int i = 0; i < N; i++)
        t[i] = 1700000000000 + i * 1000;
    test_dd(t, N, "regular");
    for (int i = 0; i < N; i++)
        t[i] = 1700000000000 + i * 1000 + (int64_t)(rnd() % 7) - 3;
    test_dd(t, N, "jitter");
    for (int shift = 0; shift < 64; shift += 7) {
        for (int i = 0; i < N; i++)
            t[i] = (int64_t)(rnd() >> shift) * ((rnd() & 1) ? 1 : -1);
        test_dd(t, N, "random magnitudes");
    }
    for (int i = 0; i < N; i++)
        t[i] = (i & 1) ? INT64_MAX : INT64_MIN;
    test_dd(t, N, "extremes");
    for (int i = 0; i < N; i++) /* every delta-of-delta bucket boundary */
        t[i] = (int64_t)i * i * (i % 5 == 0 ? 1 : (int64_t)(rnd() % 5000));
    test_dd(t, N, "mixed");

    for (int i = 0; i < N; i++)
        f[i] = 21.5f;
    test_gorilla(u, N, "constant");
    for (int i = 0; i < N; i++)
        f[i] = (float)(round(sin(i / 100.0) * 1000) / 10.0);
    test_gorilla(u, N, "sine");
    for (int i = 0; i < N; i++)
        u[i] = (uint32_t)rnd();
    test_gorilla(u, N, "random bits");
    float specials[] = {0.0f, -0.0f, NAN, INFINITY, -INFINITY, 1e-45f, 3.4e38f, -1.0f};
    for (int i = 0; i < N; i++)
        f[i] = specials[rnd() % 8];
    test_gorilla(u, N, "specials");
    for (size_t n = 0; n < 4; n++)
        test_gorilla(u, n, "tiny");

    /* other XOR codecs and reshaping transforms round trip on every dataset shape */
    for (int shape = 0; shape < 4; shape++) {
        for (int i = 0; i < N; i++) {
            switch (shape) {
            case 0: u[i] = (uint32_t)rnd(); break;
            case 1: f[i] = specials[rnd() % 8]; break;
            case 2: f[i] = (float)(round(sin(i / 50.0) * 1000) / 10.0); break;
            default: f[i] = (i % 300 < 150) ? 1.5f : (float)(rnd() % 1000); break;
            }
        }
        for (size_t n = 0; n <= 3; n++) {
            size_t m = n == 3 ? N : n;
            uint8_t *enc = calloc(patas_bound(m) + chimp128_bound(m) + alprd_bound(m) + 4 * m + BLK_SLACK, 1);
            uint32_t *dec = malloc((m + 1) * 4);
            size_t len;
#define RT(enc_fn, dec_fn)                                                    \
            len = enc_fn(u, m, enc);                                          \
            CHECK(dec_fn(enc, len, m, dec) == 0 && (m == 0 || !memcmp(u, dec, m * 4)));
            RT(chimp_encode, chimp_decode);
            RT(chimp128_encode, chimp128_decode);
            RT(patas_encode, patas_decode);
            RT(delta32_encode, delta32_decode);
            RT(bytesplit_encode, bytesplit_decode);
            RT(alprd_encode, alprd_decode);
#undef RT
            uint8_t *ab = calloc(MAX(alp_bound(m), decimal_bp_bound(m)) + BLK_SLACK, 1);
            len = alp_encode(f, m, ab);
            if (len)
                CHECK(alp_decode(ab, len, m, (float *)dec) == 0 && !memcmp(u, dec, m * 4));
            len = decimal_bp_encode(f, m, ab);
            if (len)
                CHECK(decimal_bp_decode(ab, len, m, (float *)dec) == 0 && !memcmp(u, dec, m * 4));
            free(ab);
            len = decimal_encode(f, m, enc);
            if (len)
                CHECK(decimal_decode(enc, len, m, (float *)dec) == 0 && !memcmp(u, dec, m * 4));
            free(enc);
            free(dec);
        }
    }

    /* timestamp candidates: grid residual and frame-of-reference delta */
    for (int shape = 0; shape < 5; shape++) {
        for (int i = 0; i < N; i++) {
            switch (shape) {
            case 0: t[i] = 1700000000000 + i * 1000 + (int64_t)(rnd() % 41) - 20; break;
            case 1: t[i] = (int64_t)rnd(); break;
            case 2: t[i] = (i & 1) ? INT64_MAX : INT64_MIN; break;
            case 3: t[i] = -5000000 + i * 7 + (int64_t)(rnd() % 3); break;
            default: t[i] = 1700000000000 + (int64_t)i * i; break;
            }
        }
        for (size_t m = 0; m <= 3; m++) {
            size_t n = m == 3 ? N : m;
            uint8_t *enc = calloc(tsfor_bound(n) + BLK_SLACK, 1);
            int64_t *dec = malloc((n + 1) * sizeof *dec);
            size_t len = ts_grid_encode(t, n, enc);
            if (len)
                CHECK(ts_grid_decode(enc, len, n, dec) == 0 && !memcmp(t, dec, n * sizeof *t));
            len = ts_delta_encode(t, n, enc);
            if (len)
                CHECK(ts_delta_decode(enc, len, n, dec) == 0 && !memcmp(t, dec, n * sizeof *t));
            free(enc);
            free(dec);
        }
    }

    /* block layer with and without zstd */
    codec_ctx *c = codec_new();
    for (int level = 0; level <= 3; level += 3) {
        for (int i = 0; i < 8192; i++) {
            t[i] = 1700000000000 + i * 10000;
            f[i] = (float)(i % 100) / 4;
        }
        uint8_t *blk = calloc(block_bound(8192) + BLK_SLACK, 1);
        int64_t *t2 = malloc(8192 * sizeof *t2);
        float *f2 = malloc(8192 * sizeof *f2);
        size_t tl = block_encode_ts(c, level, t, 8192, blk);
        CHECK(block_decode_ts(c, blk, tl, 8192, t2) == 0);
        CHECK(memcmp(t, t2, 8192 * sizeof *t) == 0);
        for (int jitter = 1; jitter <= 3; jitter++) { /* picks grid / delta / DoubleDelta */
            for (int i = 0; i < 8192; i++)
                t[i] = 1700000000000 + i * 1000 + (int64_t)(rnd() % (uint64_t)(jitter * 40 + 1)) - jitter * 20;
            tl = block_encode_ts(c, level, t, 8192, blk);
            CHECK(tl > 0 && block_decode_ts(c, blk, tl, 8192, t2) == 0 && !memcmp(t, t2, 8192 * sizeof *t));
        }
        for (int i = 0; i < 8192; i++)
            t[i] = 1700000000000 + i * 10000;
        for (int mode = 0; mode < 3; mode++) {
            for (int shape = 0; shape < 5; shape++) {
                for (int i = 0; i < 8192; i++) {
                    if (shape == 0) f[i] = (float)((int)(rnd() % 30000) - 15000) / 8192.0f; /* binary fixed point */
                    else if (shape == 4) f[i] = (float)(i % 100) / 4;
                    else if (shape == 1) u[i] = (uint32_t)rnd();
                    else if (shape == 2) f[i] = (float)i * 0.1f;
                    else f[i] = (float)(50 + 20 * sin(i / 500.0));
                }
                size_t vl = block_encode_f32(c, level, mode, f, 8192, blk);
                CHECK(vl > 0 && block_decode_f32(c, blk, vl, 8192, f2) == 0);
                CHECK(memcmp(f, f2, 8192 * sizeof *f) == 0);
                for (int k = 0; k < 50; k++) { /* corruption must not crash */
                    uint8_t save = blk[1 + rnd() % (vl - 1)];
                    size_t at = 1 + rnd() % (vl - 1);
                    save = blk[at];
                    blk[at] ^= (uint8_t)(1 + rnd() % 255);
                    (void)block_decode_f32(c, blk, vl, 8192, f2);
                    blk[at] = save;
                }
            }
        }
        size_t vl = block_encode_f32(c, level, TSDB_VALUES_GORILLA, f, 8192, blk);
        blk[vl / 2] ^= 0x55; /* corruption must not crash */
        (void)block_decode_f32(c, blk, vl, 8192, f2);
        free(blk);
        free(t2);
        free(f2);
    }
    codec_free(c);

    /* SIMD aggregation agrees with a scalar loop, NaN ignored by min/max */
    for (size_t n = 0; n < 200; n++) {
        for (size_t i = 0; i < n; i++)
            f[i] = (rnd() % 17 == 0) ? NAN : (float)((int64_t)(rnd() % 20001) - 10000) / 8;
        agg_state a;
        agg_init(&a);
        simd_agg_f32(f, n, &a);
        float mn = INFINITY, mx = -INFINITY;
        for (size_t i = 0; i < n; i++) {
            if (f[i] < mn) mn = f[i];
            if (f[i] > mx) mx = f[i];
        }
        CHECK(a.count == n && a.min == mn && a.max == mx);
    }
    free(t);
    free(u);
}

/* --------------------------------------------------------------- reference */

typedef struct row {
    uint32_t s;
    int64_t t;
    float v;
} row;

typedef struct ref {
    row *r; /* every write, in order */
    size_t n, cap;
    row *fin; /* last write per (series, ts), sorted; rebuilt when stale */
    size_t nfin;
    bool stale;
} ref;

static void ref_add(ref *r, uint32_t s, int64_t t, float v)
{
    if (r->n == r->cap) {
        r->cap = r->cap ? r->cap * 2 : 1024;
        r->r = realloc(r->r, r->cap * sizeof *r->r);
    }
    r->r[r->n++] = (row){s, t, v};
    r->stale = true;
}

typedef struct seqrow {
    row x;
    size_t seq;
} seqrow;

static int cmp_seqrow(const void *a, const void *b)
{
    const seqrow *x = a, *y = b;
    if (x->x.s != y->x.s)
        return x->x.s < y->x.s ? -1 : 1;
    if (x->x.t != y->x.t)
        return x->x.t < y->x.t ? -1 : 1;
    return (x->seq > y->seq) - (x->seq < y->seq);
}

/* The table as it should read: one row per (series, ts), the last written. */
static void ref_final(ref *r)
{
    if (!r->stale && r->fin)
        return;
    seqrow *a = malloc((r->n + 1) * sizeof *a);
    for (size_t i = 0; i < r->n; i++)
        a[i] = (seqrow){r->r[i], i};
    qsort(a, r->n, sizeof *a, cmp_seqrow);
    free(r->fin);
    r->fin = malloc((r->n + 1) * sizeof *r->fin);
    r->nfin = 0;
    for (size_t i = 0; i < r->n; i++) {
        if (r->nfin && r->fin[r->nfin - 1].s == a[i].x.s && r->fin[r->nfin - 1].t == a[i].x.t)
            r->nfin--;
        r->fin[r->nfin++] = a[i].x;
    }
    free(a);
    r->stale = false;
}

static void check_raw(tsdb *db, ref *r, uint32_t s, int64_t start, int64_t end)
{
    ref_final(r);
    tsdb_points p;
    OK(tsdb_query_points(db, s, start, end, &p));
    size_t k = 0;
    bool ok = true;
    for (size_t i = 0; i < r->nfin; i++) {
        const row *x = &r->fin[i];
        if (x->s != s || x->t < start || x->t >= end)
            continue;
        if (k >= p.n || p.ts[k] != x->t || memcmp(&p.values[k], &x->v, 4) != 0) {
            if (ok && k < p.n)
                fprintf(stderr, "  raw s=%u row %zu: got (%lld, %g) want (%lld, %g)\n", s, k, (long long)p.ts[k],
                        p.values[k], (long long)x->t, x->v);
            ok = false;
        }
        k++;
    }
    if (k != p.n)
        fprintf(stderr, "  raw s=%u [%lld, %lld): got %zu rows, want %zu\n", s, (long long)start, (long long)end, p.n, k);
    CHECK(ok && k == p.n);
    for (size_t i = 1; i < p.n; i++)
        CHECK(p.ts[i - 1] < p.ts[i]); /* sorted and never duplicated */
    tsdb_points_free(&p);
}

static bool same_f(float a, float b) { return a == b || (isnan(a) && isnan(b)); }

static void check_agg(tsdb *db, ref *r, uint32_t s, int64_t start, int64_t end, int64_t interval)
{
    tsdb_bucket *b;
    size_t nb;
    OK(tsdb_query_agg(db, s, start, end, interval, &b, &nb));
    /* reference buckets */
    ref_final(r);
    size_t cap = 16, nr = 0;
    tsdb_bucket *e = malloc(cap * sizeof *e);
    for (size_t i = 0; i < r->nfin; i++) {
        row *x = &r->fin[i];
        if (x->s != s || x->t < start || x->t >= end)
            continue;
        int64_t bs = interval ? floor_div(x->t, interval) * interval : start;
        size_t k;
        for (k = 0; k < nr && e[k].start != bs; k++)
            ;
        if (k == nr) {
            if (nr == cap)
                e = realloc(e, (cap *= 2) * sizeof *e);
            e[nr++] = (tsdb_bucket){bs, 0, 0, INFINITY, -INFINITY};
        }
        e[k].count++;
        e[k].sum += x->v;
        if (x->v < e[k].min) e[k].min = x->v;
        if (x->v > e[k].max) e[k].max = x->v;
    }
    CHECK(nb == nr);
    size_t matched = 0;
    for (size_t i = 0; i < nb; i++) {
        if (i > 0)
            CHECK(b[i - 1].start < b[i].start);
        for (size_t k = 0; k < nr; k++) {
            if (e[k].start != b[i].start)
                continue;
            if (e[k].min > e[k].max)
                e[k].min = e[k].max = NAN;
            bool ok = e[k].count == b[i].count && same_f(e[k].min, b[i].min) && same_f(e[k].max, b[i].max) &&
                      (isnan(e[k].sum) ? isnan(b[i].sum)
                                       : fabs(e[k].sum - b[i].sum) <= 1e-9 * fmax(1.0, fabs(e[k].sum)) + 1e-6);
            if (!ok)
                fprintf(stderr, "  bucket %lld: got n=%llu sum=%g min=%g max=%g, want n=%llu sum=%g min=%g max=%g\n",
                        (long long)b[i].start, (unsigned long long)b[i].count, b[i].sum, b[i].min, b[i].max,
                        (unsigned long long)e[k].count, e[k].sum, e[k].min, e[k].max);
            CHECK(ok);
            matched++;
        }
    }
    CHECK(matched == nb);
    free(e);
    free(b);
}

/* descending and limited raw queries against the reference */
static void check_order(tsdb *db, ref *r, uint32_t s, int64_t start, int64_t end, tsdb_order order, size_t limit)
{
    ref_final(r);
    tsdb_query_opts q = {order, limit};
    tsdb_points p;
    OK(tsdb_query_points_ex(db, s, start, end, &q, &p));
    /* the expected rows in ascending order */
    size_t ne = 0;
    for (size_t i = 0; i < r->nfin; i++)
        ne += r->fin[i].s == s && r->fin[i].t >= start && r->fin[i].t < end;
    size_t want = limit && limit < ne ? limit : ne;
    bool ok = p.n == want;
    size_t k = 0;
    for (size_t i = 0; ok && i < r->nfin; i++) {
        const row *x = &r->fin[i];
        if (x->s != s || x->t < start || x->t >= end)
            continue;
        /* position of this row in the result */
        size_t pos = order == TSDB_ASC ? k : ne - 1 - k;
        if (pos < p.n && (p.ts[pos] != x->t || memcmp(&p.values[pos], &x->v, 4) != 0))
            ok = false;
        k++;
    }
    if (!ok)
        fprintf(stderr, "  order %d limit %zu s=%u: got %zu rows, want %zu\n", order, limit, s, p.n, want);
    CHECK(ok);
    tsdb_points_free(&p);
}

static void check_all(tsdb *db, ref *r, uint32_t nseries, int64_t t0, int64_t span)
{
    for (uint32_t s = 0; s < nseries; s++) {
        check_order(db, r, s, INT64_MIN, INT64_MAX, TSDB_DESC, 0);
        check_order(db, r, s, INT64_MIN, INT64_MAX, TSDB_DESC, 1 + rnd() % 300);
        check_order(db, r, s, INT64_MIN, INT64_MAX, TSDB_ASC, 1 + rnd() % 300);
        int64_t a = t0 + (int64_t)(rnd() % (uint64_t)span) - span / 10;
        check_order(db, r, s, a, a + (int64_t)(rnd() % (uint64_t)span) + 1, TSDB_DESC, rnd() % 2 ? 0 : 1 + rnd() % 5000);
    }
    for (uint32_t s = 0; s < nseries; s++) {
        check_raw(db, r, s, INT64_MIN, INT64_MAX);
        check_agg(db, r, s, INT64_MIN, INT64_MAX, 0);
        for (int q = 0; q < 6; q++) {
            int64_t a = t0 + (int64_t)(rnd() % (uint64_t)span) - span / 10;
            int64_t b = a + (int64_t)(rnd() % (uint64_t)(span / 2)) + 1;
            check_raw(db, r, s, a, b);
            check_agg(db, r, s, a, b, 0);
            int64_t intervals[] = {1, 1000, 60000, 3600000, 7 * 86400000LL};
            check_agg(db, r, s, a, b, intervals[rnd() % 5]);
        }
    }
}

static void rm_rf(const char *dir)
{
    char cmd[512];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (system(cmd) != 0)
        exit(1);
}

static size_t count_files(const char *dir, const char *suffix)
{
    DIR *d = opendir(dir);
    struct dirent *e;
    size_t n = 0;
    while ((e = readdir(d))) {
        size_t l = strlen(e->d_name), sl = strlen(suffix);
        n += l > sl && strcmp(e->d_name + l - sl, suffix) == 0;
    }
    closedir(d);
    return n;
}

static tsdb_options test_opts(int merge_threads)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.partition_ms = 3600000; /* 1 hour, to get many partitions */
    o.granule_rows = 64;      /* many granules, many boundaries */
    o.fsync = 0;
    o.merge_threads = merge_threads;
    o.query_threads = 4;
    o.wal_max_batch_rows = 1000; /* both write paths: WAL + memtable, and direct parts */
    return o;
}

static void test_db(const char *dir)
{
    rm_rf(dir);
    tsdb_options o = test_opts(0);
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    enum { NS = 7 };
    uint32_t ids[NS];
    char name[32];
    for (int i = 0; i < NS; i++) {
        snprintf(name, sizeof name, "sensor.%d", i);
        OK(tsdb_series(db, name, &ids[i]));
        CHECK(ids[i] == (uint32_t)i);
    }
    uint32_t again;
    OK(tsdb_series(db, "sensor.3", &again));
    CHECK(again == 3);
    CHECK(tsdb_series_lookup(db, "nope", &again) == TSDB_ERR_NOTFOUND);

    ref r = {0};
    const int64_t t0 = 1700000000000;
    const int64_t span = 6 * 3600000;
    /* batches of every shape: sorted per series, time-major, random, duplicates, negative ts */
    for (int batch = 0; batch < 40; batch++) {
        size_t n = 1 + rnd() % 3000;
        uint32_t *s = malloc(n * sizeof *s);
        int64_t *t = malloc(n * sizeof *t);
        float *v = malloc(n * sizeof *v);
        int shape = batch % 4;
        int64_t base = t0 + (int64_t)(rnd() % (uint64_t)span);
        for (size_t i = 0; i < n; i++) {
            switch (shape) {
            case 0: s[i] = (uint32_t)(batch % NS); t[i] = base + (int64_t)i * 1000; break;
            case 1: s[i] = (uint32_t)(i % NS); t[i] = base + (int64_t)(i / NS) * 10000; break;
            case 2: s[i] = (uint32_t)(rnd() % NS); t[i] = t0 + (int64_t)(rnd() % (uint64_t)span); break;
            default: s[i] = (uint32_t)(rnd() % 2); t[i] = base + (int64_t)(rnd() % 50) * 1000; break;
            }
            v[i] = (rnd() % 50 == 0) ? NAN : (float)((int64_t)(rnd() % 2000) - 1000) / 10.0f;
            if (shape == 2 && rnd() % 2)
                v[i] = (float)(i % 10); /* repeated values */
            ref_add(&r, s[i], t[i], v[i]);
        }
        if (shape == 0)
            OK(tsdb_insert_series(db, s[0], t, v, n));
        else
            OK(tsdb_insert(db, s, t, v, n));
        free(s);
        free(t);
        free(v);
    }
    /* pre-epoch timestamps across a partition boundary */
    for (int i = 0; i < 500; i++) {
        int64_t t = -1800000 + i * 7919;
        float v = (float)i;
        OK(tsdb_insert_series(db, 5, &t, &v, 1));
        ref_add(&r, 5, t, v);
    }
    /* bad input is rejected */
    uint32_t bad = 99;
    int64_t bt = 0;
    float bv = 0;
    CHECK(tsdb_insert(db, &bad, &bt, &bv, 1) == TSDB_ERR_INVALID);

    tsdb_stats st;
    tsdb_get_stats(db, &st);
    ref_final(&r);
    printf("  wrote %zu rows (%zu unique keys): %llu parts, %llu partitions, %llu rows in the memtable\n", r.n,
           r.nfin, (unsigned long long)st.parts, (unsigned long long)st.partitions,
           (unsigned long long)st.memtable_rows);
    CHECK(st.memtable_rows > 0);
    check_all(db, &r, NS, t0, span);

    OK(tsdb_optimize(db));
    tsdb_get_stats(db, &st);
    CHECK(st.rows == r.nfin); /* one part per partition: every replaced row is gone */
    CHECK(st.memtable_rows == 0);
    CHECK(st.parts == st.partitions);
    printf("  after optimize: %llu parts, %llu merges, %llu granules copied without decoding\n",
           (unsigned long long)st.parts, (unsigned long long)st.merges,
           (unsigned long long)st.passthrough_granules);
    check_all(db, &r, NS, t0, span);

    /* buffered appends */
    for (int i = 0; i < 1000; i++) {
        int64_t t = t0 + span + i * 500;
        float v = (float)(i % 13);
        OK(tsdb_append(db, (uint32_t)(i % NS), t, v));
        ref_add(&r, (uint32_t)(i % NS), t, v);
    }
    OK(tsdb_flush(db));
    check_all(db, &r, NS, t0, span + 600000);
    OK(tsdb_close(db));

    /* reopen read-only and read-write */
    o.read_only = 1;
    OK(tsdb_open(dir, &o, &db));
    CHECK(tsdb_series_count(db) == NS);
    CHECK(strcmp(tsdb_series_name(db, 4), "sensor.4") == 0);
    check_all(db, &r, NS, t0, span);
    CHECK(tsdb_insert_series(db, 0, &bt, &bv, 1) == TSDB_ERR_READONLY);
    OK(tsdb_close(db));
    o.read_only = 0;

    /* crash leftovers: a tmp file, and sources still present next to their merged part */
    OK(tsdb_open(dir, &o, &db));
    tsdb *db2;
    CHECK(tsdb_open(dir, &o, &db2) == TSDB_ERR_LOCKED);
    OK(tsdb_close(db));
    char path[512];
    snprintf(path, sizeof path, "%s/tmp_1_1.part", dir);
    int fd = open(path, O_CREAT | O_WRONLY, 0644);
    CHECK(fd >= 0 && write(fd, "junk", 4) == 4);
    close(fd);
    {
        /* copy a merged part as if it were a level-0 source covered by it */
        DIR *d = opendir(dir);
        struct dirent *e;
        while ((e = readdir(d))) {
            long long p;
            unsigned long long a, b;
            unsigned lvl;
            if (sscanf(e->d_name, "p%lld_%llu_%llu_%u.part", &p, &a, &b, &lvl) == 4 && lvl > 0 && b > a) {
                char src[512], dst[512], cmd[1200];
                snprintf(src, sizeof src, "%s/%s", dir, e->d_name);
                snprintf(dst, sizeof dst, "%s/p%lld_%llu_%llu_0.part", dir, p, a, a);
                snprintf(cmd, sizeof cmd, "cp '%s' '%s'", src, dst);
                CHECK(system(cmd) == 0);
                break;
            }
        }
        closedir(d);
    }
    snprintf(path, sizeof path, "%s/p0_999999_999999_0.part", dir);
    fd = open(path, O_CREAT | O_WRONLY, 0644); /* garbage part: moved aside */
    CHECK(fd >= 0 && write(fd, "garbage garbage garbage garbage garbage garbage garbage garbage garbage", 71) == 71);
    close(fd);
    OK(tsdb_open(dir, &o, &db));
    CHECK(count_files(dir, ".broken") == 1);
    snprintf(path, sizeof path, "%s/tmp_1_1.part", dir);
    CHECK(access(path, F_OK) != 0);
    check_all(db, &r, NS, t0, span);

    /* retention: drop everything before t0 + 3h */
    int64_t cut = t0 + 3 * 3600000;
    int64_t cut_part = floor_div(cut, o.partition_ms) * o.partition_ms;
    OK(tsdb_drop_before(db, cut));
    size_t kept = 0;
    for (size_t i = 0; i < r.n; i++)
        if (r.r[i].t >= cut_part)
            r.r[kept++] = r.r[i];
    r.n = kept;
    r.stale = true;
    check_all(db, &r, NS, t0, span);
    OK(tsdb_close(db));
    free(r.r);
    free(r.fin);
}

/* -------------------------------------------------- replace semantics, WAL */

static void test_replace(const char *dir)
{
    rm_rf(dir);
    tsdb_options o = test_opts(2);
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    uint32_t id;
    OK(tsdb_series(db, "tank.level", &id));
    ref r = {0};
    const int64_t t0 = 1700000000000;
    /* the same timestamps written over and over, through every path */
    for (int round = 0; round < 12; round++) {
        size_t n = round % 3 == 0 ? 5000 : 300; /* direct parts and WAL + memtable */
        int64_t *t = malloc(n * sizeof *t);
        float *v = malloc(n * sizeof *v);
        for (size_t i = 0; i < n; i++) {
            t[i] = t0 + (int64_t)(rnd() % 2000) * 1000;
            v[i] = (float)(round * 1000 + (int)i);
            ref_add(&r, id, t[i], v[i]);
        }
        OK(tsdb_insert_series(db, id, t, v, n)); /* duplicates inside one batch too */
        free(t);
        free(v);
        if (round == 4)
            OK(tsdb_flush(db));
        check_all(db, &r, 1, t0, 2000000);
    }
    for (int i = 0; i < 300; i++) {
        int64_t t = t0 + (int64_t)(rnd() % 2000) * 1000;
        OK(tsdb_append(db, id, t, (float)-i));
        ref_add(&r, id, t, (float)-i);
    }
    check_all(db, &r, 1, t0, 2000000); /* memtable rows replace part rows at once */
    OK(tsdb_optimize(db));
    tsdb_stats st;
    tsdb_get_stats(db, &st);
    ref_final(&r);
    CHECK(st.rows == r.nfin);
    check_all(db, &r, 1, t0, 2000000);
    printf("  %zu writes of %zu keys: last write wins in every path\n", r.n, r.nfin);
    OK(tsdb_close(db));
    free(r.r);
    free(r.fin);
}

static void copy_file(const char *from, const char *to)
{
    char cmd[1200];
    snprintf(cmd, sizeof cmd, "cp '%s' '%s'", from, to);
    CHECK(system(cmd) == 0);
}

static bool find_file(const char *dir, const char *prefix, char *out, size_t len)
{
    DIR *d = opendir(dir);
    struct dirent *e;
    bool found = false;
    while (!found && (e = readdir(d)))
        if (strncmp(e->d_name, prefix, strlen(prefix)) == 0) {
            snprintf(out, len, "%s/%s", dir, e->d_name);
            found = true;
        }
    closedir(d);
    return found;
}

static void test_wal(const char *dir)
{
    const int64_t t0 = 1700000000000;
    for (int mode = 0; mode < 2; mode++) {
        /* a child writes and dies without closing; acknowledged rows must survive */
        rm_rf(dir);
        tsdb_options o = test_opts(0);
        o.wal_sync_ms = mode == 0 ? 0 : 20;
        o.flush_interval_ms = 1000000;
        pid_t pid = fork();
        if (pid == 0) {
            tsdb *db;
            uint32_t id;
            if (tsdb_open(dir, &o, &db) || tsdb_series(db, "pipe.flow", &id))
                _exit(1);
            for (int i = 0; i < 2000; i++)
                if (tsdb_append(db, id, t0 + i * 100, (float)i * 0.5f))
                    _exit(1);
            int64_t t[10];
            float v[10];
            for (int i = 0; i < 10; i++) {
                t[i] = t0 + i * 100; /* replaces the first ten */
                v[i] = -1;
            }
            if (tsdb_insert_series(db, id, t, v, 10))
                _exit(1);
            if (mode == 1)
                usleep(200000); /* let the background sync run */
            _exit(0); /* no tsdb_close: a crash */
        }
        int status;
        waitpid(pid, &status, 0);
        CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
        tsdb *db;
        OK(tsdb_open(dir, &o, &db));
        ref r = {0};
        for (int i = 0; i < 2000; i++)
            ref_add(&r, 0, t0 + i * 100, i < 10 ? -1.0f : (float)i * 0.5f);
        check_all(db, &r, 1, t0, 200000);
        tsdb_stats st;
        tsdb_get_stats(db, &st);
        CHECK(st.rows == 2000 && st.memtable_rows == 0); /* replayed into parts */
        printf("  crash with wal_sync_ms=%d: %llu rows recovered from the WAL\n", o.wal_sync_ms,
               (unsigned long long)st.rows);
        OK(tsdb_close(db));
        free(r.r);
        free(r.fin);
    }

    /* crash after a memtable's part was published but before its WAL segment
     * was deleted: replay must not bring back rows replaced since */
    rm_rf(dir);
    tsdb_options o = test_opts(0);
    o.wal_max_batch_rows = 100;
    tsdb *db;
    uint32_t id;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series(db, "pipe.pressure", &id));
    int64_t t = t0;
    float v = 1.0f;
    OK(tsdb_append(db, id, t, v));
    char wal[600], saved[700];
    CHECK(find_file(dir, "wal_", wal, sizeof wal));
    snprintf(saved, sizeof saved, "%s.saved", wal);
    copy_file(wal, saved);
    OK(tsdb_flush(db)); /* part published, segment deleted */
    int64_t ts[200];
    float vs[200];
    for (int i = 0; i < 200; i++) {
        ts[i] = t0 + i;
        vs[i] = 2.0f;
    }
    OK(tsdb_insert_series(db, id, ts, vs, 200)); /* a newer part replaces the value */
    OK(tsdb_close(db));
    copy_file(saved, wal); /* the stale segment reappears, as after the crash */
    unlink(saved);
    OK(tsdb_open(dir, &o, &db));
    tsdb_points p;
    OK(tsdb_query_points(db, id, t0, t0 + 1, &p));
    CHECK(p.n == 1 && p.values[0] == 2.0f);
    tsdb_points_free(&p);
    OK(tsdb_close(db));
    printf("  stale WAL segment after a flush is not replayed over newer data\n");
}

/* ------------------------------------------------------------ concurrency */

typedef struct conc {
    tsdb *db;
    int id;
    atomic_int *done;
} conc;

static void *writer(void *a)
{
    conc *c = a;
    int64_t t[500];
    float v[500];
    for (int b = 0; b < 200; b++) {
        for (int i = 0; i < 500; i++) {
            t[i] = 1700000000000 + ((int64_t)b * 500 + i) * 1000;
            v[i] = (float)c->id;
        }
        OK(tsdb_insert_series(c->db, (uint32_t)c->id, t, v, 500));
    }
    return NULL;
}

static void *reader(void *a)
{
    conc *c = a;
    while (!atomic_load(c->done)) {
        for (uint32_t s = 0; s < 4; s++) {
            tsdb_bucket *b;
            size_t nb;
            OK(tsdb_query_agg(c->db, s, INT64_MIN, INT64_MAX, 60000, &b, &nb));
            for (size_t i = 0; i < nb; i++)
                CHECK(b[i].min == (float)s && b[i].max == (float)s);
            free(b);
            tsdb_points p;
            OK(tsdb_query_points(c->db, s, INT64_MIN, INT64_MAX, &p));
            for (size_t i = 1; i < p.n; i++)
                CHECK(p.ts[i - 1] < p.ts[i]);
            tsdb_points_free(&p);
        }
    }
    return NULL;
}

static void test_concurrency(const char *dir)
{
    rm_rf(dir);
    tsdb_options o = test_opts(3);
    o.granule_rows = 1024;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    for (int i = 0; i < 4; i++) {
        char name[16];
        uint32_t id;
        snprintf(name, sizeof name, "s%d", i);
        OK(tsdb_series(db, name, &id));
    }
    atomic_int done = 0;
    pthread_t w[4], r[3];
    conc cw[4], cr[3];
    for (int i = 0; i < 4; i++) {
        cw[i] = (conc){db, i, &done};
        pthread_create(&w[i], NULL, writer, &cw[i]);
    }
    for (int i = 0; i < 3; i++) {
        cr[i] = (conc){db, i, &done};
        pthread_create(&r[i], NULL, reader, &cr[i]);
    }
    for (int i = 0; i < 4; i++)
        pthread_join(w[i], NULL);
    atomic_store(&done, 1);
    for (int i = 0; i < 3; i++)
        pthread_join(r[i], NULL);
    OK(tsdb_optimize(db));
    tsdb_stats st;
    tsdb_get_stats(db, &st);
    CHECK(st.rows == 4 * 200 * 500 && st.memtable_rows == 0);
    printf("  4 writers + 3 readers: %llu rows, %llu merges, %llu parts left\n", (unsigned long long)st.rows,
           (unsigned long long)st.merges, (unsigned long long)st.parts);
    for (uint32_t s = 0; s < 4; s++) {
        tsdb_bucket *b;
        size_t nb;
        OK(tsdb_query_agg(db, s, INT64_MIN, INT64_MAX, 0, &b, &nb));
        CHECK(nb == 1 && b[0].count == 100000 && b[0].sum == 100000.0 * s);
        free(b);
    }
    OK(tsdb_close(db));
}

int main(int argc, char **argv)
{
    const char *base = argc > 1 ? argv[1] : "/tmp";
    char dir[256];
    simd_init();
    printf("simd: %s\n", simd_name());
    printf("codecs\n");
    test_codecs();
    printf("database\n");
    desc_window_rows = 37; /* descending queries cross many windows */
    snprintf(dir, sizeof dir, "%s/tsdb-test-%d", base, (int)getpid());
    test_db(dir);
    printf("replace semantics\n");
    test_replace(dir);
    printf("write-ahead log\n");
    test_wal(dir);
    printf("concurrency\n");
    test_concurrency(dir);
    rm_rf(dir);
    if (failures) {
        printf("FAILED: %d checks\n", failures);
        return 1;
    }
    printf("all tests passed\n");
    return 0;
}
