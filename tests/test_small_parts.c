/* Many small parts (granules of 16 rows, inserts of 1 to 40 rows) of five series of every value type, written in
 * random order with replaces, deletes, retention, memtable rows and a reopen, and every query kind over random
 * windows checked against a model of what each series holds. Raw queries step through hundreds of parts and partial
 * granules: the decode buffers are sized by the largest granule a source reads and freed when it drains (run it under
 * make asan). Found the mutants of that sizing and freeing that the other suites let through.
 *
 *   test_small_parts [scratch dir] [seeds]   (default /tmp, 4 seeds) */
#define _GNU_SOURCE
#include "tsdb.h"
#include <ftw.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

extern size_t desc_window_rows; /* internal: a small window makes DESC queries step back through many windows */

#define K 3000 /* row slots per series, one per second */
#define T0 1699920000000LL
#define TS(k) (T0 + (int64_t)(k) * 1000)
enum { NS = 5 };

static uint64_t rs;
static int fails;
static const tsdb_type types[NS] = {TSDB_FLOAT32, TSDB_FLOAT32, TSDB_FLOAT64, TSDB_INT64, TSDB_BOOL};
static uint32_t ids[NS];
static signed char present[NS][K]; /* the model: 0 absent, 1 a value, 2 null */
static int64_t val[NS][K];
static int64_t cutk[NS]; /* rows below this slot are expired */

static uint64_t rnd(void)
{
    rs ^= rs << 13;
    rs ^= rs >> 7;
    rs ^= rs << 17;
    return rs;
}

#define CK(c, ...)                                                                                                 \
    do {                                                                                                           \
        if (!(c) && fails++ < 30) {                                                                                \
            printf("FAIL %s:%d ", __FILE__, __LINE__);                                                             \
            printf(__VA_ARGS__);                                                                                   \
            printf("\n");                                                                                          \
        }                                                                                                          \
    } while (0)
#define OK(x)                                                                                                      \
    do {                                                                                                           \
        int rc_ = (x);                                                                                             \
        if (rc_) {                                                                                                 \
            printf("ERR %s = %d line %d\n", #x, rc_, __LINE__);                                                    \
            exit(2);                                                                                               \
        }                                                                                                          \
    } while (0)

static int rm_one(const char *p, const struct stat *s, int flag, struct FTW *f)
{
    (void)s, (void)flag, (void)f;
    return remove(p);
}
static void rm_rf(const char *dir) { nftw(dir, rm_one, 16, FTW_DEPTH | FTW_PHYS); }

static int64_t clk(void *a)
{
    (void)a;
    return TS(K) + 5000;
}

static void put_val(int s, void *buf, size_t i, int64_t x)
{
    switch (types[s]) {
    case TSDB_FLOAT32: ((float *)buf)[i] = x == 0 && (rnd() & 1) ? -0.0f : (float)x; break;
    case TSDB_FLOAT64: ((double *)buf)[i] = (double)x; break;
    case TSDB_INT64: ((int64_t *)buf)[i] = x; break;
    default: ((uint8_t *)buf)[i] = (uint8_t)(x & 1); break;
    }
}

static int64_t norm(int s, int64_t x) { return types[s] == TSDB_BOOL ? (x & 1) : x; }

static int64_t get_val(int s, const void *buf, size_t i)
{
    switch (types[s]) {
    case TSDB_FLOAT32: return (int64_t)((const float *)buf)[i];
    case TSDB_FLOAT64: return (int64_t)((const double *)buf)[i];
    case TSDB_INT64: return ((const int64_t *)buf)[i];
    default: return ((const uint8_t *)buf)[i];
    }
}

/* rows at the slots ks (sorted, unique) of series s, one in 9 null; replace: [rlo, rhi) is replaced by them */
static void write_rows(tsdb *db, int s, const int *ks, int n, int replace, int rlo, int rhi)
{
    int64_t ts[64];
    uint8_t vb[64 * 8], valid[8];
    memset(valid, 0, sizeof valid);
    for (int i = 0; i < n; i++) {
        ts[i] = TS(ks[i]);
        put_val(s, vb, i, (int64_t)(rnd() % 2001) - 1000);
        if (rnd() % 9 != 0)
            valid[i >> 3] |= 1 << (i & 7);
    }
    tsdb_values v = {types[s], vb, valid};
    if (replace) {
        OK(tsdb_replace_range(db, ids[s], TS(rlo), TS(rhi), ts, &v, n));
        for (int k = rlo; k < rhi; k++)
            present[s][k] = 0;
    } else {
        OK(tsdb_insert_series_values(db, ids[s], ts, &v, n));
    }
    for (int i = 0; i < n; i++) {
        present[s][ks[i]] = (valid[i >> 3] >> (i & 7) & 1) ? 1 : 2;
        val[s][ks[i]] = norm(s, get_val(s, vb, i));
    }
}

typedef struct {
    int s;
    int64_t *ts;
    int64_t *v;
    signed char *p;
    size_t n, cap;
} coll;

static int coll_cb(void *ctx, const int64_t *ts, const tsdb_values *col, size_t n)
{
    coll *c = ctx;
    for (size_t i = 0; i < n; i++) {
        if (c->n == c->cap) {
            c->cap = c->cap ? 2 * c->cap : 256;
            c->ts = realloc(c->ts, c->cap * sizeof *c->ts);
            c->v = realloc(c->v, c->cap * sizeof *c->v);
            c->p = realloc(c->p, c->cap);
        }
        int known = !col->valid || (col->valid[i >> 3] >> (i & 7) & 1);
        c->ts[c->n] = ts[i];
        c->v[c->n] = known ? norm(c->s, get_val(c->s, col->values, i)) : 0;
        c->p[c->n++] = known ? 1 : 2;
    }
    return 0;
}

static int raw32_cb(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    volatile double acc = 0; /* reads every value and timestamp handed out (ASan) */
    for (size_t i = 0; i < n; i++)
        acc += v[i] + (double)ts[i];
    *(size_t *)ctx += n;
    return 0;
}

static int lo_k(int s, int k) { return k < cutk[s] ? (int)cutk[s] : k; }

static void check_raw(tsdb *db, int s, int a, int b, int desc, size_t limit)
{
    coll c = {.s = s};
    tsdb_query_opts o = {desc ? TSDB_DESC : TSDB_ASC, limit};
    OK(tsdb_query_values(db, ids[s], TS(a), TS(b), &o, coll_cb, &c));
    size_t j = 0;
    int lo = lo_k(s, a);
    for (int t = 0; t < b - lo && (!limit || j < limit); t++) {
        int k = desc ? b - 1 - t : lo + t;
        if (!present[s][k])
            continue;
        if (j < c.n) {
            CK(c.ts[j] == TS(k), "s%d raw [%d,%d) desc %d limit %zu row %zu: slot %lld, want %d", s, a, b, desc, limit,
               j, (long long)((c.ts[j] - T0) / 1000), k);
            CK(c.p[j] == present[s][k] && (present[s][k] == 2 || c.v[j] == val[s][k]), "s%d value at slot %d", s, k);
        }
        j++;
    }
    CK(j == c.n, "s%d raw [%d,%d) desc %d limit %zu: %zu rows, want %zu", s, a, b, desc, limit, c.n, j);
    free(c.ts);
    free(c.v);
    free(c.p);
}

static size_t bucket_count(const tsdb_bucket *bk, size_t nb)
{
    size_t cnt = 0;
    for (size_t i = 0; i < nb; i++)
        cnt += bk[i].count;
    return cnt;
}

/* every aggregating query over [a, b): counts and sums against the model, the others run (ASan) */
static void check_agg(tsdb *db, int s, int a, int b)
{
    int64_t iv = (int64_t)(1 + rnd() % 40) * 1000;
    tsdb_bucket *bk;
    size_t nb;
    OK(tsdb_query_agg(db, ids[s], TS(a), TS(b), rnd() & 1 ? 0 : iv, &bk, &nb));
    double sum = 0;
    int64_t isum = 0;
    for (size_t i = 0; i < nb; i++) {
        sum += bk[i].sum;
        isum += bk[i].isum;
    }
    size_t cnt = bucket_count(bk, nb), wc = 0;
    double ws = 0;
    int64_t wis = 0;
    for (int k = lo_k(s, a); k < b; k++)
        if (present[s][k] == 1) {
            wc++;
            ws += (double)val[s][k];
            wis += val[s][k];
        }
    CK(cnt == wc, "s%d agg [%d,%d): count %zu, want %zu", s, a, b, cnt, wc);
    if (types[s] == TSDB_FLOAT32 || types[s] == TSDB_FLOAT64) /* integers below 2^53: exact in any order */
        CK(sum == ws, "s%d agg [%d,%d): sum %g, want %g", s, a, b, sum, ws);
    if (types[s] == TSDB_INT64)
        CK(isum == wis, "s%d agg [%d,%d): isum", s, a, b);
    tsdb_free(bk);

    tsdb_spread_bucket *sb;
    tsdb_query_opts o = {rnd() & 1 ? TSDB_DESC : TSDB_ASC, rnd() % 3};
    OK(tsdb_query_spread(db, ids[s], TS(a), TS(b), iv, &o, &sb, &nb));
    tsdb_free(sb);

    tsdb_group g = {TSDB_MINUTE, 1 + (uint32_t)(rnd() % 3), NULL};
    OK(tsdb_query_group(db, ids[s], TS(a), TS(b), &g, &bk, &nb));
    cnt = bucket_count(bk, nb);
    CK(cnt == wc, "s%d group [%d,%d): count %zu, want %zu", s, a, b, cnt, wc);
    tsdb_free(bk);

    int64_t bounds[8];
    size_t n = 2 + rnd() % 6;
    if (n > (size_t)(b - a) + 1)
        n = (size_t)(b - a) + 1;
    for (size_t i = 0; i < n; i++)
        bounds[i] = TS(a) + (int64_t)((size_t)(b - a) * i / (n - 1)) * 1000;
    OK(tsdb_query_bounds(db, ids[s], TS(a), TS(b), bounds, n, NULL, &bk, &nb));
    cnt = bucket_count(bk, nb);
    CK(cnt == wc, "s%d bounds [%d,%d): count %zu, want %zu", s, a, b, cnt, wc);
    tsdb_free(bk);

    if (types[s] != TSDB_BOOL) {
        tsdb_twa_bucket *tb;
        tsdb_twa_opts to = {rnd() & 1, rnd() % 3 ? 0 : 5000, rnd() & 1 ? TSDB_DESC : TSDB_ASC, rnd() % 4,
                            (int)(rnd() & 1)};
        OK(tsdb_query_twa(db, ids[s], TS(a), TS(b), iv, &to, &tb, &nb));
        tsdb_free(tb);
        OK(tsdb_query_twa_bounds(db, ids[s], TS(a), TS(b), bounds, n, &to, &tb, &nb));
        tsdb_free(tb);
    }

    coll c = {.s = s};
    size_t th = rnd() & 1 ? 3 + rnd() % 20 : 1000000;
    OK(tsdb_query_lttb(db, ids[s], TS(a), TS(b), th, coll_cb, &c));
    if (th == 1000000 && types[s] != TSDB_BOOL) /* a threshold above the rows: all of them */
        CK(c.n == wc, "s%d lttb [%d,%d): %zu rows, want %zu", s, a, b, c.n, wc);
    free(c.ts);
    free(c.v);
    free(c.p);
    if (types[s] == TSDB_FLOAT32) {
        size_t rows = 0;
        OK(tsdb_query_raw(db, ids[s], TS(a), TS(b), raw32_cb, &rows));
    }
}

static void query_all(tsdb *db, int rounds)
{
    for (int r = 0; r < rounds; r++)
        for (int s = 0; s < NS; s++) {
            int a = (int)(rnd() % K), b = a + 1 + (int)(rnd() % (rnd() & 1 ? 60 : K));
            if (rnd() % 8 == 0) {
                a = 0;
                b = K + 100;
            }
            if (b > K)
                b = K;
            check_raw(db, s, a, b, 0, 0);
            check_raw(db, s, a, b, 1, rnd() % 4 ? 0 : 1 + rnd() % 50);
            check_raw(db, s, a, b, 0, 1 + rnd() % 50);
            check_agg(db, s, a, b);
        }
}

static void run(const char *dir, unsigned seed)
{
    rs = seed * 2654435761u + 1;
    memset(present, 0, sizeof present);
    memset(cutk, 0, sizeof cutk);
    rm_rf(dir);
    tsdb_options o;
    tsdb_options_init(&o);
    o.granule_rows = 16;
    o.merge_threads = 0;
    o.fsync = 0;
    o.wal_max_batch_rows = 8; /* larger inserts go straight to parts */
    o.flush_interval_ms = 1000000;
    o.partition_ms = 600000;
    o.clock_ms = clk;
    o.wal_sync_ms = -1;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    const char *names[NS] = {"f32disjoint", "f32mixed", "f64", "i64", "bool"};
    for (int s = 0; s < NS; s++)
        OK(tsdb_series_create(db, names[s], types[s], NULL, &ids[s]));
    int next_a = 0, ks[64];
    for (int it = 0; it < 900; it++) {
        int s = (int)(rnd() % NS);
        if (s == 0) { /* disjoint and increasing: parts of 1 to 3 granules, the last one short */
            int n = 1 + (int)(rnd() % 40);
            if (next_a + n > K)
                continue;
            for (int i = 0; i < n; i++)
                ks[i] = next_a + i;
            next_a += n + (int)(rnd() % 3);
            write_rows(db, s, ks, n, 0, 0, 0);
        } else { /* scattered over a window, overlapping earlier parts; one in 25 a replace */
            int w0 = (int)(rnd() % K), n = 1 + (int)(rnd() % 40), span = n + (int)(rnd() % 200), m = 0;
            for (int k = w0; k < w0 + span && k < K && m < n; k++)
                if (rnd() % (unsigned)(span / n + 1) == 0)
                    ks[m++] = k;
            if (!m)
                continue;
            if (rnd() % 25 == 0) {
                int rlo = ks[0] - (int)(rnd() % 5), rhi = ks[m - 1] + 1 + (int)(rnd() % 5);
                write_rows(db, s, ks, m, 1, rlo < 0 ? 0 : rlo, rhi);
            } else {
                write_rows(db, s, ks, m, 0, 0, 0);
            }
        }
        if (it % 61 == 60) {
            int s2 = 1 + (int)(rnd() % (NS - 1)), d0 = (int)(rnd() % K), d1 = d0 + 1 + (int)(rnd() % 80);
            if (d1 > K)
                d1 = K;
            OK(tsdb_delete_range(db, ids[s2], TS(d0), TS(d1)));
            for (int k = d0; k < d1; k++)
                present[s2][k] = 0;
        }
        if (it % 97 == 0)
            OK(tsdb_flush(db));
        if (it % 150 == 149)
            query_all(db, 2);
    }
    OK(tsdb_set_retention(db, ids[2], clk(NULL) - TS(700))); /* float64: slots below 700 expire */
    cutk[2] = 700;
    query_all(db, 40);
    for (int i = 0; i < 30; i++) { /* rows that stay in the memtable */
        int s = 1 + (int)(rnd() % (NS - 1)), m = 0, w0 = (int)(rnd() % (K - 10));
        for (int k = w0; k < w0 + 8; k++)
            if (rnd() & 1)
                ks[m++] = k;
        if (m)
            write_rows(db, s, ks, m, 0, 0, 0);
    }
    query_all(db, 40);
    OK(tsdb_close(db));
    OK(tsdb_open(dir, &o, &db));
    query_all(db, 20);
    OK(tsdb_close(db));
    rm_rf(dir);
}

int main(int argc, char **argv)
{
    char dir[600];
    snprintf(dir, sizeof dir, "%s/tsdb-small-parts-%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    unsigned seeds = argc > 2 ? (unsigned)strtoul(argv[2], NULL, 10) : 4;
    desc_window_rows = 23;
    for (unsigned seed = 1; seed <= seeds; seed++)
        run(dir, seed);
    if (fails) {
        printf("test_small_parts: %d failures\n", fails);
        return 1;
    }
    printf("test_small_parts: ok (%u seeds)\n", seeds);
    return 0;
}
