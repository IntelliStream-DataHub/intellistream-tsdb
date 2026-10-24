/* Tests: codec round trips, and every query checked against a brute-force reference. */
#include "internal.h"
#include "samekeys.h"

#include <dirent.h>
#include <fcntl.h>
#include <math.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/syscall.h>
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
        agg_init(&a, TSDB_FLOAT32);
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
            e[nr++] = (tsdb_bucket){.start = bs, .min = INFINITY, .max = -INFINITY};
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

/* The library's fsync and fdatasync calls land here: the test binary's
 * definitions win over libc's. A test can hold the next call, to look at the
 * database while a write waits for the disk. */
static pthread_mutex_t hold_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t hold_cv = PTHREAD_COND_INITIALIZER;
static int hold_armed; /* HOLD_*: hold the next such call */
static int hold_begun, hold_ended, hold_go;
static bool hold_others; /* only calls from threads other than hold_main */
static pthread_t hold_main;
enum { HOLD_FSYNC = 1, HOLD_FDATASYNC = 2 };

static void hold_point(int which)
{
    pthread_mutex_lock(&hold_lock);
    if ((hold_armed & which) && !(hold_others && pthread_equal(pthread_self(), hold_main))) {
        hold_armed &= ~which;
        int me = ++hold_begun;
        pthread_cond_broadcast(&hold_cv);
        while (hold_go < me)
            pthread_cond_wait(&hold_cv, &hold_lock);
        hold_ended = me;
        pthread_cond_broadcast(&hold_cv);
    }
    pthread_mutex_unlock(&hold_lock);
}

int fsync(int fd)
{
    hold_point(HOLD_FSYNC);
    return (int)syscall(SYS_fsync, fd);
}

int fdatasync(int fd)
{
    hold_point(HOLD_FDATASYNC);
    return (int)syscall(SYS_fdatasync, fd);
}

static void hold_arm(int which)
{
    pthread_mutex_lock(&hold_lock);
    hold_armed |= which;
    pthread_mutex_unlock(&hold_lock);
}

static void hold_wait(void) /* until the next call is held */
{
    pthread_mutex_lock(&hold_lock);
    while (hold_begun <= hold_go)
        pthread_cond_wait(&hold_cv, &hold_lock);
    pthread_mutex_unlock(&hold_lock);
}

static void hold_release(void) /* lets the held call go on */
{
    pthread_mutex_lock(&hold_lock);
    int me = ++hold_go;
    pthread_cond_broadcast(&hold_cv);
    while (hold_ended < me)
        pthread_cond_wait(&hold_cv, &hold_lock);
    pthread_mutex_unlock(&hold_lock);
}

static void *flush_main(void *db)
{
    OK(tsdb_flush(db));
    return NULL;
}

static void *append_two(void *db)
{
    OK(tsdb_append(db, 0, 1700000000000, 2.0f));
    return NULL;
}

static float value_at(tsdb *db, int64_t t)
{
    tsdb_points p;
    OK(tsdb_query_points(db, 0, t, t + 1, &p));
    float v = p.n == 1 ? p.values[0] : NAN;
    tsdb_points_free(&p);
    return v;
}

/* Regression: with wal_sync_ms = 0 a row is visible once the WAL is synced up
 * to it. A memtable flush moved the active memtable to the flushing list and
 * only then wrote and synced the rest of its WAL segment, and queries read
 * flushing memtables without that limit: meanwhile a row was visible that a
 * power failure could still take back (its writer not yet acknowledged). */
static void test_flush_visibility(const char *dir)
{
    rm_rf(dir);
    tsdb_options o = test_opts(0);
    o.fsync = 1;
    o.wal_sync_ms = 0;
    o.flush_interval_ms = 1000000;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    uint32_t id;
    OK(tsdb_series(db, "s", &id));
    const int64_t t = 1700000000000;
    OK(tsdb_append(db, id, t, 1.0f));
    /* the flush holds the WAL writer role while it creates the next segment
     * (and fsyncs the directory): a row appended now waits for it */
    hold_arm(HOLD_FSYNC);
    pthread_t fl, wr;
    pthread_create(&fl, NULL, flush_main, db);
    hold_wait();
    pthread_create(&wr, NULL, append_two, db);
    tsdb_stats st;
    do {
        usleep(1000);
        OK(tsdb_get_stats(db, &st));
    } while (st.memtable_rows < 2);
    CHECK(value_at(db, t) == 1.0f); /* in the active memtable, not durable */
    /* the flush rotates the memtable, then syncs the rest of the old segment */
    hold_arm(HOLD_FDATASYNC);
    hold_release();
    hold_wait();
    CHECK(value_at(db, t) == 1.0f); /* in the flushing memtable, still not durable */
    hold_release();
    pthread_join(wr, NULL);
    pthread_join(fl, NULL);
    CHECK(value_at(db, t) == 2.0f);
    OK(tsdb_close(db));
    printf("  a row in a memtable being flushed is not visible before its WAL record is durable\n");
}

typedef struct pins {
    tsdb *db;
    int64_t t; /* a row in each of the partitions of t and t + 1 hour, or only t's */
    bool two;
} pins;

static void *insert_parts(void *a)
{
    pins *p = a;
    int64_t ts[200];
    float v[200];
    for (int i = 0; i < 200; i++) {
        ts[i] = p->t + (p->two && i >= 100 ? 3600000 : 0) + i;
        v[i] = 2.0f;
    }
    OK(tsdb_insert_series(p->db, 0, ts, v, 200));
    return NULL;
}

static void *optimize_main(void *db)
{
    OK(tsdb_optimize(db));
    return NULL;
}

static void count_partition(void *ctx, const tsdb_part_info *p)
{
    int64_t *c = ctx; /* c[0]: the partition, c[1]: its parts */
    c[1] += p->partition == c[0];
}

static int64_t parts_in(tsdb *db, int64_t partition)
{
    int64_t c[2] = {partition, 0};
    OK(tsdb_foreach_part(db, count_partition, c));
    return c[1];
}

/* Regression: after publishing its parts, an insert read each part's
 * partition to wait for merges there (parts_to_delay_insert), without a
 * reference: meanwhile the part could have been merged away and freed. A use
 * after free that AddressSanitizer and ThreadSanitizer report (it was found by
 * test_same_keys under ThreadSanitizer). Here the insert waits in its first
 * partition, which has too many parts, while its part in the second one is
 * merged away. */
static void test_publish_then_merge(const char *dir)
{
    rm_rf(dir);
    tsdb_options o = test_opts(1);
    o.fsync = 1; /* so the merge thread can be held in its fdatasync */
    o.wal_max_batch_rows = 100;
    o.obsolete_part_lifetime_s = 0; /* a replaced part goes with its last reference */
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    uint32_t id;
    OK(tsdb_series(db, "s", &id));
    const int64_t t = 1700002800000; /* partitions of an hour: p0 and p1 = p0 + 1 */
    const int64_t p0 = t / 3600000;
    pins one = {db, t, false}, both = {db, t, true};
    hold_main = pthread_self();
    hold_others = true;
    hold_arm(HOLD_FDATASYNC); /* the merge thread's first merge: its parts stay "merging" */
    insert_parts(&both);
    for (int i = 1; i < PARTS_TO_DELAY_INSERT; i++)
        insert_parts(&one);
    hold_wait();
    pthread_t more, ins, opt;
    pthread_create(&more, NULL, insert_parts, &one); /* one too many: waits for merges */
    while (parts_in(db, p0) <= PARTS_TO_DELAY_INSERT)
        usleep(1000);
    pthread_create(&ins, NULL, insert_parts, &both); /* waits in p0 after publishing its parts */
    while (parts_in(db, p0 + 1) < 2)
        usleep(1000);
    pthread_create(&opt, NULL, optimize_main, db); /* merges p1 (p0 is busy), frees the insert's part there */
    while (parts_in(db, p0 + 1) != 1)
        usleep(1000);
    hold_release(); /* merges in p0 go on, and the insert stops waiting */
    pthread_join(more, NULL);
    pthread_join(ins, NULL);
    pthread_join(opt, NULL);
    hold_others = false;
    CHECK(value_at(db, t) == 2.0f && value_at(db, t + 3600000 + 150) == 2.0f);
    OK(tsdb_close(db));
    printf("  an insert does not touch its parts once they are published\n");
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

/* ------------------------------------------ concurrent writes to the same keys */

/* Several threads write the same series and timestamps at once (samekeys.h has
 * the model and the checks), through every write path, while the memtable is
 * flushed, merges run and readers query. */
typedef struct skx {
    const sk_cfg *c;
    tsdb *db;
    uint32_t ids[SK_MAX_SERIES];
    atomic_int started[SK_MAX_WRITERS]; /* rounds begun */
    atomic_int acked[SK_MAX_WRITERS];   /* rounds acknowledged */
    int last_kind[SK_MAX_WRITERS];
    atomic_int writing; /* writers still running */
    atomic_int errors;
    atomic_long snapshots, torn_across, aggs;
    long merges;
    tsdb *follower; /* a follower of the same directory, or NULL */
    atomic_long follower_snapshots;
    atomic_int follower_errors;
} skx;

typedef struct skt {
    skx *x;
    int i;
} skt;

static int sk_write(skx *x, int w, int r, int kind, uint64_t seed)
{
    const sk_cfg *c = x->c;
    sk_batch b;
    int rc = 0;
    if (kind == SK_APPEND) { /* one row per call, in random order */
        sk_build(c, x->ids, w, r, false, 2, seed, UINT32_MAX, &b);
        for (size_t i = 0; i < b.n && rc == 0; i++) {
            float f;
            memcpy(&f, b.v + 4 * i, 4);
            rc = tsdb_append(x->db, b.series[i], b.ts[i], f);
        }
        sk_batch_free(&b);
        return rc;
    }
    bool plain = c->type == TSDB_FLOAT32 && !c->nulls && (seed >> 20) % 2; /* the float32 calls, too */
    for (uint32_t s = 0; s < (kind == SK_SERIES ? c->nseries : 1) && rc == 0; s++) {
        bool large = kind == SK_LARGE || (kind == SK_SERIES && (seed >> 24) % 2);
        sk_build(c, x->ids, w, r, large, (int)(seed % 3), seed, kind == SK_SERIES ? s : UINT32_MAX, &b);
        tsdb_values col = {(tsdb_type)c->type, b.v, c->nulls ? b.valid : NULL};
        if (kind == SK_SERIES)
            rc = plain ? tsdb_insert_series(x->db, b.series[0], b.ts, (const float *)(void *)b.v, b.n)
                       : tsdb_insert_series_values(x->db, b.series[0], b.ts, &col, b.n);
        else
            rc = plain ? tsdb_insert(x->db, b.series, b.ts, (const float *)(void *)b.v, b.n)
                       : tsdb_insert_values(x->db, b.series, b.ts, &col, b.n);
        sk_batch_free(&b);
    }
    return rc;
}

static void *sk_writer(void *a)
{
    skt *t = a;
    skx *x = t->x;
    for (int r = 0; r < x->c->rounds; r++) {
        uint64_t seed = sk_mix(((uint64_t)t->i << 32) ^ (uint64_t)r ^ 0x5eedull);
        int kind = sk_pick(x->c, seed >> 40);
        atomic_store(&x->started[t->i], r + 1);
        OK(sk_write(x, t->i, r, kind, seed));
        atomic_store(&x->acked[t->i], r + 1);
        x->last_kind[t->i] = kind;
    }
    atomic_fetch_sub(&x->writing, 1);
    return NULL;
}

static void *sk_reader(void *a)
{
    skt *t = a;
    skx *x = t->x;
    const sk_cfg *c = x->c;
    uint32_t span = sk_span(c);
    sk_seen seen;
    sk_seen_init(&seen, c);
    sk_val *row = malloc(span * sizeof *row);
    int started[SK_MAX_WRITERS], acked[SK_MAX_WRITERS];
    bool batches = !(c->kinds & SK_APPEND);
    for (uint64_t it = 0; atomic_load(&x->writing) > 0; it++) {
        int64_t prev = -1;
        for (uint32_t s = 0; s < c->nseries; s++) {
            sk_collect k;
            for (int w = 0; w < c->writers; w++)
                acked[w] = atomic_load(&x->acked[w]);
            sk_collect_begin(&k, c, row, false);
            OK(tsdb_query_values(x->db, x->ids[s], c->t0, sk_end(c), NULL, sk_collect_cb, &k));
            for (int w = 0; w < c->writers; w++)
                started[w] = atomic_load(&x->started[w]);
            int64_t id;
            int e = k.errors + sk_check_snapshot(c, s, row, acked, started, &seen, &id);
            if (s > 0 && id >= 0 && prev >= 0 && id != prev)
                atomic_fetch_add(&x->torn_across, 1); /* allowed: one query, one series, one snapshot */
            prev = id;
            atomic_fetch_add(&x->snapshots, 1);
            if (batches && c->shift == 0 && (it + s) % 3 == (uint64_t)t->i % 3) {
                tsdb_bucket *b;
                size_t nb;
                OK(tsdb_query_agg(x->db, x->ids[s], c->t0, sk_end(c), 0, &b, &nb));
                for (int w = 0; w < c->writers; w++)
                    started[w] = atomic_load(&x->started[w]);
                e += sk_check_agg(c, s, b, nb, started);
                free(b);
                atomic_fetch_add(&x->aggs, 1);
            }
            if ((it + s) % 4 == 1) { /* newest first, a few windows */
                tsdb_query_opts desc = {TSDB_DESC, span / 3};
                sk_collect_begin(&k, c, row, true);
                OK(tsdb_query_values(x->db, x->ids[s], c->t0, sk_end(c), &desc, sk_collect_cb, &k));
                e += k.errors + (k.n > span / 3);
            }
            if (e)
                atomic_fetch_add(&x->errors, e);
        }
    }
    free(row);
    sk_seen_free(&seen);
    return NULL;
}

/* Queries a follower: it lags behind the writer, so nothing acknowledged has
 * to be there yet, but what it shows must be a snapshot of whole inserts. */
static void *sk_follower_reader(void *a)
{
    skt *t = a;
    skx *x = t->x;
    const sk_cfg *c = x->c;
    sk_seen seen;
    sk_seen_init(&seen, c);
    sk_val *row = malloc(sk_span(c) * sizeof *row);
    int started[SK_MAX_WRITERS], acked[SK_MAX_WRITERS] = {0};
    while (atomic_load(&x->writing) > 0) {
        for (uint32_t s = 0; s < c->nseries; s++) {
            sk_collect k;
            sk_collect_begin(&k, c, row, false);
            OK(tsdb_query_values(x->follower, x->ids[s], c->t0, sk_end(c), NULL, sk_collect_cb, &k));
            for (int w = 0; w < c->writers; w++)
                started[w] = atomic_load(&x->started[w]);
            int64_t id;
            int e = k.errors + sk_check_snapshot(c, s, row, acked, started, &seen, &id);
            atomic_fetch_add(&x->follower_snapshots, 1);
            if (e)
                atomic_fetch_add(&x->follower_errors, e);
        }
    }
    free(row);
    sk_seen_free(&seen);
    return NULL;
}

static void *sk_maintenance(void *a)
{
    skx *x = ((skt *)a)->x;
    for (int i = 0; atomic_load(&x->writing) > 0; i++) {
        usleep(10000);
        OK(i % 10 == 9 ? tsdb_optimize(x->db) : tsdb_flush(x->db));
    }
    return NULL;
}

static void sk_read_all(const sk_cfg *c, tsdb *db, const uint32_t *ids, sk_val *st)
{
    for (uint32_t s = 0; s < c->nseries; s++) {
        sk_collect k;
        sk_collect_begin(&k, c, st + (size_t)s * sk_span(c), false);
        OK(tsdb_query_values(db, ids[s], c->t0, sk_end(c), NULL, sk_collect_cb, &k));
        CHECK(k.errors == 0);
    }
}

static tsdb_options sk_options(const sk_cfg *c, int wal_sync_ms)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.partition_ms = 3600000; /* every write spans several partitions */
    o.granule_rows = 64;
    o.fsync = 0;
    o.merge_threads = 2;
    o.query_threads = 4;
    size_t kw = (size_t)c->nseries * c->nts;
    o.wal_max_batch_rows = kw + kw / 2; /* a write of every key once: WAL + memtable; twice: parts */
    o.buffer_rows = 2 * kw;             /* the memtable is flushed every few small writes */
    o.flush_interval_ms = 10;
    o.wal_sync_ms = wal_sync_ms;
    o.obsolete_part_lifetime_s = 0;
    return o;
}

/* Runs the writers, readers and maintenance; the state they leave in st. */
static void sk_phase(skx *x, sk_val *st)
{
    const sk_cfg *c = x->c;
    enum { READERS = 3 };
    pthread_t th[SK_MAX_WRITERS + READERS + 1];
    skt args[SK_MAX_WRITERS + READERS + 1];
    int n = 0;
    atomic_store(&x->writing, c->writers);
    for (int w = 0; w < c->writers; w++) {
        args[n] = (skt){x, w};
        pthread_create(&th[n], NULL, sk_writer, &args[n]);
        n++;
    }
    for (int i = 0; i < READERS; i++) {
        args[n] = (skt){x, i};
        pthread_create(&th[n], NULL, sk_reader, &args[n]);
        n++;
    }
    args[n] = (skt){x, 0};
    pthread_create(&th[n], NULL, sk_maintenance, &args[n]);
    n++;
    pthread_t fth;
    skt farg = {x, 0};
    if (x->follower)
        pthread_create(&fth, NULL, sk_follower_reader, &farg);
    for (int i = 0; i < n; i++)
        pthread_join(th[i], NULL);
    if (x->follower)
        pthread_join(fth, NULL);
    CHECK(atomic_load(&x->errors) == 0);
    tsdb_stats sts;
    OK(tsdb_get_stats(x->db, &sts));
    x->merges = (long)sts.merges;
    sk_read_all(c, x->db, x->ids, st);
    CHECK(sk_check_final(c, st, x->last_kind) == 0);
}

static void sk_same(const sk_cfg *c, const sk_val *want, tsdb *db, const uint32_t *ids, const char *after)
{
    size_t nk = sk_nkeys(c);
    sk_val *st = calloc(nk, sizeof *st);
    sk_read_all(c, db, ids, st);
    bool same = true;
    for (size_t i = 0; i < nk; i++)
        same &= st[i].st == want[i].st && st[i].code == want[i].code;
    if (!same)
        fprintf(stderr, "  %s: the state changed after %s\n", c->name, after);
    CHECK(same);
    free(st);
}

/* crash: the writers run in a child that exits without closing the database,
 * and the parent reopens it: the WAL is replayed next to the parts written meanwhile. */
static void sk_run(const char *base, const sk_cfg *c, int wal_sync_ms, bool crash)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/tsdb-samekeys-%d", base, (int)getpid());
    rm_rf(dir);
    tsdb_options o = sk_options(c, wal_sync_ms);
    size_t nk = sk_nkeys(c);
    sk_val *st = calloc(nk, sizeof *st);
    skx *x = calloc(1, sizeof *x);
    x->c = c;
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    int pfd[2] = {-1, -1};
    pid_t child = 0;
    if (crash) {
        CHECK(pipe(pfd) == 0);
        child = fork();
        CHECK(child >= 0);
    }
    int failures0 = failures;
    if (child == 0) {
        OK(tsdb_open(dir, &o, &x->db));
        for (uint32_t s = 0; s < c->nseries; s++) {
            char name[64];
            snprintf(name, sizeof name, "same.%u", s);
            OK(tsdb_series_create(x->db, name, (tsdb_type)c->type, NULL, &x->ids[s]));
        }
        if (!crash) {
            tsdb_options fo = o;
            fo.follow = 1;
            fo.refresh_ms = 2;
            OK(tsdb_open(dir, &fo, &x->follower));
        }
        sk_phase(x, st);
        if (x->follower) {
            OK(tsdb_close(x->follower));
            CHECK(atomic_load(&x->follower_errors) == 0);
        }
        if (crash) {
            OK(tsdb_commit(x->db)); /* durable, as every write is with wal_sync_ms = 0 */
            bool ok = write(pfd[1], st, nk * sizeof *st) == (ssize_t)(nk * sizeof *st) &&
                      write(pfd[1], x, sizeof *x) == (ssize_t)sizeof *x;
            _exit(ok && failures == failures0 ? 0 : 1); /* no close: the memtable stays in the WAL */
        }
    } else {
        close(pfd[1]);
        uint8_t *dst = (uint8_t *)st;
        size_t want = nk * sizeof *st, got = 0;
        skx copy;
        while (got < want + sizeof copy) {
            ssize_t r = read(pfd[0], got < want ? dst + got : (uint8_t *)&copy + (got - want),
                             got < want ? want - got : sizeof copy - (got - want));
            if (r <= 0)
                break;
            got += (size_t)r;
        }
        close(pfd[0]);
        int status;
        waitpid(child, &status, 0);
        CHECK(got == want + sizeof copy && WIFEXITED(status) && WEXITSTATUS(status) == 0);
        memcpy(x->ids, copy.ids, sizeof x->ids);
        memcpy(x->last_kind, copy.last_kind, sizeof x->last_kind);
        x->snapshots = copy.snapshots;
        x->torn_across = copy.torn_across;
        x->aggs = copy.aggs;
        x->merges = copy.merges;
        OK(tsdb_open(dir, &o, &x->db));
        sk_same(c, st, x->db, x->ids, "a crash and WAL replay");
        CHECK(sk_check_final(c, st, x->last_kind) == 0);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    OK(tsdb_flush(x->db));
    sk_same(c, st, x->db, x->ids, "tsdb_flush");
    OK(tsdb_optimize(x->db));
    sk_same(c, st, x->db, x->ids, "tsdb_optimize");
    tsdb_stats sts;
    OK(tsdb_get_stats(x->db, &sts));
    CHECK(sts.rows == nk && sts.memtable_rows == 0); /* one row per key: nothing older survives */
    OK(tsdb_close(x->db));
    OK(tsdb_open(dir, &o, &x->db));
    sk_same(c, st, x->db, x->ids, "close and reopen");
    OK(tsdb_close(x->db));
    printf("  %-28s %d writers x %d rounds, %zu keys: %ld merges, %ld snapshots (%ld aggregates, %ld on a "
           "follower), %ld torn across series, %.2f s%s\n",
           c->name, c->writers, c->rounds, nk, x->merges, (long)x->snapshots, (long)x->aggs,
           (long)x->follower_snapshots, (long)x->torn_across,
           (double)(t1.tv_sec - t0.tv_sec) + (double)(t1.tv_nsec - t0.tv_nsec) / 1e9, crash ? ", crash + replay" : "");
    rm_rf(dir);
    free(st);
    free(x);
}

static void test_same_keys(const char *base)
{
    const int64_t t0 = 1700000000000 + 1234; /* partitions do not start at a key */
    const unsigned batches = SK_SMALL | SK_LARGE;
    static sk_cfg c[6];
    c[0] = (sk_cfg){"float32, same keys", 6, 1000, 3, 80, 0, t0, 97000, TSDB_FLOAT32, false, batches};
    c[1] = (sk_cfg){"int64 + nulls, same keys", 6, 1000, 3, 80, 0, t0, 97000, TSDB_INT64, true, batches};
    c[2] = (sk_cfg){"float64 + nulls, same keys", 5, 1000, 2, 120, 0, t0, 61000, TSDB_FLOAT64, true, batches};
    c[3] = (sk_cfg){"float32, shifted keys", 6, 1000, 3, 80, 20, t0, 97000, TSDB_FLOAT32, false, batches};
    c[4] = (sk_cfg){"int64 + nulls, shifted keys", 6, 1000, 3, 80, 37, t0, 97000, TSDB_INT64, true, batches};
    c[5] = (sk_cfg){"appends, per series, batches", 6, 200, 3, 60, 10, t0, 97000, TSDB_FLOAT32, false,
                    batches | SK_APPEND | SK_SERIES};
    sk_run(base, &c[0], 0, false);  /* WAL synced on every write: rows visible once durable */
    sk_run(base, &c[1], 5, true);   /* WAL synced in the background */
    sk_run(base, &c[2], -1, false); /* no WAL */
    sk_run(base, &c[3], 0, true);
    sk_run(base, &c[4], 0, false);
    sk_run(base, &c[5], 0, false);
}

typedef struct part_count {
    int n;
    int64_t created, written;
} part_count;

static void count_parts(void *ctx, const tsdb_part_info *p)
{
    part_count *c = ctx;
    c->n++;
    c->created = p->created_s;
    c->written = p->written_s;
}

/* Regression: batches committed out of time order (several loaders) left
 * parts whose merged granules overlap in time; background merges stopped
 * there for an hour, and every aggregate decoded those granules instead of
 * using their statistics. An idle partition is now merged down soon. */
static void test_idle_merge(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/tsdb-idle-%d", base, (int)getpid());
    tsdb_options o;
    tsdb_options_init(&o);
    o.granule_rows = 256;
    o.fsync = 0;
    o.wal_max_batch_rows = 100; /* every batch becomes a part */
    o.merge_idle_s = 1;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    uint32_t id;
    OK(tsdb_series(db, "s", &id));
    /* two parts of the same size whose rows interleave in time: the merge
     * heuristics leave a pair like this for about an hour */
    enum { B = 1000 };
    int64_t ts[B];
    float v[B];
    const int64_t t0 = 1700000000000;
    for (int k = 0; k < 2; k++) {
        for (int i = 0; i < B; i++) {
            ts[i] = t0 + (int64_t)(2 * i + k) * 1000;
            v[i] = (float)i;
        }
        OK(tsdb_insert_series(db, id, ts, v, B));
    }
    part_count pc = {0, 0, 0};
    for (int tries = 0; tries < 100; tries++) { /* idle after 1 s; merges look every second */
        pc.n = 0;
        tsdb_foreach_part(db, count_parts, &pc);
        if (pc.n == 1)
            break;
        usleep(100000);
    }
    CHECK(pc.n == 1);
    /* Regression: a merge's output counted as a new write, so a partition
     * needing several merges waited merge_idle_s again after each. The part
     * keeps the time its rows were written, a second or more before the merge. */
    CHECK(pc.written < pc.created);
    tsdb_bucket *b;
    size_t nb;
    OK(tsdb_query_agg(db, id, INT64_MIN + 1, INT64_MAX, 0, &b, &nb));
    tsdb_query_stats st;
    tsdb_last_query_stats(&st);
    CHECK(nb == 1 && b[0].count == 2 * B && st.rows_decoded == 0 && st.rows_from_stats == 2 * B);
    free(b);
    OK(tsdb_close(db));
    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    CHECK(system(cmd) == 0);
}

int main(int argc, char **argv)
{
    const char *base = argc > 1 ? argv[1] : "/tmp";
    char dir[256];
    simd_init();
    if (argc > 2 && strcmp(argv[2], "samekeys") == 0) { /* just these, e.g. under a sanitizer */
        test_same_keys(base);
        printf(failures ? "FAILED: %d checks\n" : "all tests passed\n", failures);
        return failures != 0;
    }
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
    test_flush_visibility(dir);
    test_publish_then_merge(dir);
    printf("concurrency\n");
    test_concurrency(dir);
    rm_rf(dir);
    printf("concurrent writes to the same keys\n");
    test_same_keys(base);
    printf("idle partitions merged\n");
    test_idle_merge(base);
    if (failures) {
        printf("FAILED: %d checks\n", failures);
        return 1;
    }
    printf("all tests passed\n");
    return 0;
}
