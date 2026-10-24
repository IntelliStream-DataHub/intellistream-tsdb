/* Tests: value types (float64, int64, bool, state) and nulls, against a
 * reference; their codecs; and reading what version 1 wrote. */
#include "internal.h"

#include <fcntl.h>
#include <inttypes.h>
#include <math.h>
#include <stdlib.h>
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
#define OK(expr)                                                                                 \
    do {                                                                                         \
        int rc_ = (expr);                                                                        \
        if (rc_ != 0) {                                                                          \
            fprintf(stderr, "%s:%d: %s -> %s\n", __FILE__, __LINE__, #expr, tsdb_strerror(rc_)); \
            exit(1);                                                                             \
        }                                                                                        \
    } while (0)

static uint64_t rng = 0x9e3779b97f4a7c15ull;
static uint64_t rnd(void)
{
    rng ^= rng << 13;
    rng ^= rng >> 7;
    rng ^= rng << 17;
    return rng;
}

static const char *type_name[] = {"float32", "float64", "int64", "bool", "state"};

/* ------------------------------------------------------------------ codecs */

static size_t roundtrip(int type, const void *v, size_t n, int level, const char *what)
{
    size_t w = type_width(type);
    codec_ctx *c = codec_new();
    uint8_t *blk = calloc(block_bound(n) + BLK_SLACK, 1);
    uint8_t *out = calloc(n * w + BLK_SLACK, 1);
    size_t len = block_encode_vals(c, level, TSDB_VALUES_AUTO, type, v, n, blk);
    CHECK(len > 0 && len <= block_bound(n));
    int rc = block_decode_vals(c, type, blk, len, n, out);
    bool same = rc == 0 && memcmp(v, out, n * w) == 0;
    if (!same)
        fprintf(stderr, "  %s round trip failed: %s (n=%zu, level %d, rc %d, kind %d)\n", type_name[type], what, n,
                level, rc, blk[0]);
    CHECK(same);
    /* a truncated block must not decode */
    if (len > 1)
        CHECK(block_decode_vals(c, type, blk, len - 1, n, out) != 0 || len - 1 == 0);
    free(blk);
    free(out);
    codec_free(c);
    return len;
}

static void both_levels(int type, const void *v, size_t n, const char *what)
{
    roundtrip(type, v, n, 0, what);
    roundtrip(type, v, n, 1, what);
}

static void test_codecs(void)
{
    enum { N = 8192 };
    static const size_t sizes[] = {1, 2, 3, 1024, 1025, 2049, N};
    int64_t *i = malloc(N * sizeof *i);
    double *d = malloc(N * sizeof *d);
    uint32_t *u = malloc(N * sizeof *u);
    for (size_t si = 0; si < sizeof sizes / sizeof *sizes; si++) {
        size_t n = sizes[si];
        /* int64 */
        for (size_t k = 0; k < n; k++)
            i[k] = 7;
        both_levels(TSDB_INT64, i, n, "constant");
        for (size_t k = 0; k < n; k++)
            i[k] = 1000000000000 + (int64_t)k;
        both_levels(TSDB_INT64, i, n, "counter");
        for (size_t k = 0; k < n; k++)
            i[k] = (int64_t)rnd();
        both_levels(TSDB_INT64, i, n, "random");
        for (size_t k = 0; k < n; k++)
            i[k] = k % 2 ? INT64_MAX : INT64_MIN + 1;
        both_levels(TSDB_INT64, i, n, "extremes");
        for (size_t k = 0; k < n; k++)
            i[k] = (int64_t)(k * 5000000000ull); /* deltas over 32 bits */
        both_levels(TSDB_INT64, i, n, "wide deltas");
        for (size_t k = 0; k < n; k++)
            i[k] = rnd() % 7 == 0 ? NULL_I64 : (int64_t)(rnd() % 1000);
        both_levels(TSDB_INT64, i, n, "with nulls");
        /* float64 */
        for (size_t k = 0; k < n; k++)
            d[k] = round((20 + 5 * sin((double)k / 50) + (double)(rnd() % 100) / 1000) * 100) / 100;
        both_levels(TSDB_FLOAT64, d, n, "two decimals");
        for (size_t k = 0; k < n; k++) {
            uint64_t bits = rnd();
            memcpy(&d[k], &bits, 8);
        }
        both_levels(TSDB_FLOAT64, d, n, "random bits");
        for (size_t k = 0; k < n; k++)
            d[k] = (double)(rnd() % 1000000) * 1.7;
        both_levels(TSDB_FLOAT64, d, n, "scaled");
        for (size_t k = 0; k < n; k++) {
            static const double odd[] = {0.0, -0.0, 1e308, -1e-308, 0.1, 3.0};
            d[k] = odd[k % 6];
            if (k % 11 == 5)
                d[k] = NAN;
            if (k % 13 == 7)
                d[k] = INFINITY;
            if (k % 17 == 3) {
                uint64_t nb = NULL_F64;
                memcpy(&d[k], &nb, 8);
            }
        }
        both_levels(TSDB_FLOAT64, d, n, "specials");
        /* bool and state */
        for (size_t k = 0; k < n; k++)
            u[k] = (k / 37) % 2;
        both_levels(TSDB_BOOL, u, n, "runs");
        for (size_t k = 0; k < n; k++)
            u[k] = rnd() % 3; /* 2 is null */
        both_levels(TSDB_BOOL, u, n, "random with nulls");
        for (size_t k = 0; k < n; k++)
            u[k] = (uint32_t)(1 + (k / 100) % 5) * (k % 97 != 0); /* 0 is null */
        both_levels(TSDB_STATE, u, n, "codes");
    }
    /* compact where it should be */
    for (size_t k = 0; k < N; k++)
        d[k] = round((20 + 5 * sin((double)k / 50)) * 10) / 10;
    size_t len = roundtrip(TSDB_FLOAT64, d, N, 1, "one decimal");
    CHECK(len < N * 2); /* under 2 bytes a value (8 raw) */
    /* Regression: values computed as count * resolution (k * 0.001, not k / 1000)
     * missed the decimal codec and took 4x the space */
    for (size_t k = 0; k < N; k++)
        d[k] = (double)(int64_t)(850 + 40 * sin((double)k / 300) + (double)(rnd() % 3)) * 0.001;
    CHECK(roundtrip(TSDB_FLOAT64, d, N, 1, "count times resolution") < N / 2);
    /* and binary fractions (ADC counts) take a binary scale, not a long decimal one */
    for (size_t k = 0; k < N; k++)
        d[k] = round(sin((double)k / 7) * 8192) / 8192;
    CHECK(roundtrip(TSDB_FLOAT64, d, N, 1, "binary fractions") < N * 2);
    for (size_t k = 0; k < N; k++)
        i[k] = 5000 + (int64_t)k * 10;
    CHECK(roundtrip(TSDB_INT64, i, N, 1, "regular counter") < N / 4);
    for (size_t k = 0; k < N; k++)
        u[k] = k > N / 2;
    CHECK(roundtrip(TSDB_BOOL, u, N, 1, "one change") < 64);
    free(i);
    free(d);
    free(u);
}

/* --------------------------------------------------------- engine, typed */

/* Reference: every write in order; the expected state is the last write per ts. */
typedef struct row {
    int64_t ts;
    uint64_t slot; /* the value as slot bits; the type's null for nulls */
    uint64_t seq;
} row;

typedef struct series_ref {
    uint32_t id;
    int type;
    row *rows;
    size_t n, cap;
} series_ref;

static uint64_t write_seq;

static void ref_add(series_ref *r, int64_t ts, uint64_t slot)
{
    if (r->n == r->cap) {
        r->cap = r->cap ? r->cap * 2 : 1024;
        r->rows = realloc(r->rows, r->cap * sizeof *r->rows);
    }
    r->rows[r->n++] = (row){ts, slot, write_seq++};
}

static int cmp_row(const void *a, const void *b)
{
    const row *x = a, *y = b;
    if (x->ts != y->ts)
        return x->ts < y->ts ? -1 : 1;
    return x->seq < y->seq ? -1 : x->seq > y->seq;
}

/* the final rows in time order (last write per ts) */
static size_t ref_final(series_ref *r, row **out)
{
    row *f = malloc((r->n + 1) * sizeof *f);
    memcpy(f, r->rows, r->n * sizeof *f);
    qsort(f, r->n, sizeof *f, cmp_row);
    size_t o = 0;
    for (size_t i = 0; i < r->n; i++) {
        if (o && f[o - 1].ts == f[i].ts)
            o--;
        f[o++] = f[i];
    }
    *out = f;
    return o;
}

/* a random value of the type, sometimes null */
static uint64_t gen_slot(int type, size_t k)
{
    if (rnd() % 9 == 0)
        return type_null(type);
    switch (type) {
    case TSDB_FLOAT32: {
        float f = (float)(round((15 + 10 * sin((double)k / 300)) * 10) / 10);
        uint32_t b;
        memcpy(&b, &f, 4);
        return b;
    }
    case TSDB_FLOAT64: {
        double x = rnd() % 5 ? round((100 + (double)(rnd() % 100000) / 7) * 1000) / 1000 : (double)rnd() / 3.0;
        uint64_t b;
        memcpy(&b, &x, 8);
        return b;
    }
    case TSDB_INT64: return rnd() % 50 ? (uint64_t)(int64_t)(k * 1000 + rnd() % 10) : rnd() >> 1;
    case TSDB_BOOL: return (k / 50) % 2;
    default: return 1 + (k / 200) % 3;
    }
}

/* Rows [k0, k0 + n) of series r, time step `step` ms, as one batch through
 * the public column API (valid bitmap for nulls, bytes for bools), recorded
 * in the reference. db NULL: only recorded (what a child process wrote). */
static void insert_batch(tsdb *db, series_ref *r, int64_t t0, int64_t step, size_t k0, size_t n)
{
    size_t w = type_width(r->type);
    int64_t *ts = malloc(n * sizeof *ts);
    uint8_t *v = calloc(n, 8);
    uint8_t *valid = calloc((n + 7) / 8, 1);
    for (size_t k = 0; k < n; k++) {
        uint64_t s = gen_slot(r->type, k0 + k);
        ts[k] = t0 + (int64_t)(k0 + k) * step;
        bool known = s != type_null(r->type);
        valid[k >> 3] |= (uint8_t)(known << (k & 7));
        if (r->type == TSDB_BOOL)
            v[k] = (uint8_t)s;
        else
            slot_put(v, w, k, known ? s : 0);
        ref_add(r, ts[k], s);
    }
    tsdb_values col = {(tsdb_type)r->type, v, valid};
    if (db)
        OK(tsdb_insert_series_values(db, r->id, ts, &col, n));
    free(ts);
    free(v);
    free(valid);
}

typedef struct got {
    int type;
    row *rows;
    size_t n, cap;
} got;

static int collect(void *ctx, const int64_t *ts, const tsdb_values *col, size_t n)
{
    got *g = ctx;
    if ((int)col->type != g->type)
        return 1;
    if (g->n + n > g->cap) {
        g->cap = MAX(g->cap * 2, g->n + n);
        g->rows = realloc(g->rows, g->cap * sizeof *g->rows);
    }
    size_t w = type_width(g->type);
    for (size_t i = 0; i < n; i++) {
        bool known = !col->valid || ((col->valid[i >> 3] >> (i & 7)) & 1);
        uint64_t s = g->type == TSDB_BOOL ? ((const uint8_t *)col->values)[i] : slot_get(col->values, w, i);
        g->rows[g->n++] = (row){ts[i], known ? s : type_null(g->type), 0};
    }
    return 0;
}

static void check_raw(tsdb *db, series_ref *r, const char *when)
{
    row *f;
    size_t nf = ref_final(r, &f);
    got g = {r->type, NULL, 0, 0};
    OK(tsdb_query_values(db, r->id, INT64_MIN, INT64_MAX, NULL, collect, &g));
    bool same = g.n == nf;
    for (size_t i = 0; same && i < nf; i++)
        same = g.rows[i].ts == f[i].ts && g.rows[i].slot == f[i].slot;
    if (!same)
        fprintf(stderr, "  %s raw mismatch %s: got %zu rows, want %zu\n", type_name[r->type], when, g.n, nf);
    CHECK(same);
    /* newest first, limited */
    g.n = 0;
    tsdb_query_opts o = {TSDB_DESC, 77};
    OK(tsdb_query_values(db, r->id, INT64_MIN, INT64_MAX, &o, collect, &g));
    size_t want = MIN((size_t)77, nf);
    same = g.n == want;
    for (size_t i = 0; same && i < want; i++)
        same = g.rows[i].ts == f[nf - 1 - i].ts && g.rows[i].slot == f[nf - 1 - i].slot;
    CHECK(same);
    free(g.rows);
    free(f);
}

/* The reference aggregate of rows in [lo, hi), written out on its own (not
 * with the engine's fold, which it checks). */
static void ref_agg(int type, const row *f, size_t nf, int64_t lo, int64_t hi, tsdb_bucket *b, uint64_t *rows)
{
    memset(b, 0, sizeof *b);
    b->min = b->max = NAN;
    double sum = 0, mn = INFINITY, mx = -INFINITY;
    __int128 is = 0;
    int64_t imn = INT64_MAX, imx = INT64_MIN;
    uint64_t count = 0;
    *rows = 0;
    for (size_t i = 0; i < nf; i++) {
        if (f[i].ts < lo || f[i].ts >= hi)
            continue;
        (*rows)++;
        if (f[i].slot == type_null(type))
            continue;
        count++;
        if (type == TSDB_FLOAT32 || type == TSDB_FLOAT64) {
            double x;
            if (type == TSDB_FLOAT32) {
                float y;
                uint32_t bits = (uint32_t)f[i].slot;
                memcpy(&y, &bits, 4);
                x = y;
            } else {
                memcpy(&x, &f[i].slot, 8);
            }
            sum += x;
            if (x < mn)
                mn = x;
            if (x > mx)
                mx = x;
        } else {
            int64_t x = (int64_t)f[i].slot;
            is += x;
            imn = x < imn ? x : imn;
            imx = x > imx ? x : imx;
        }
    }
    b->count = count;
    if (!count || type == TSDB_STATE)
        return;
    if (type == TSDB_FLOAT32 || type == TSDB_FLOAT64) {
        b->sum = sum;
        if (mn <= mx) {
            b->min = mn;
            b->max = mx;
        }
        return;
    }
    b->isum = is > INT64_MAX ? INT64_MAX : is < INT64_MIN ? INT64_MIN : (int64_t)is;
    b->imin = imn;
    b->imax = imx;
    b->sum = (double)is;
    b->min = (double)imn;
    b->max = (double)imx;
}

static bool same_double(double a, double b) { return (isnan(a) && isnan(b)) || a == b || fabs(a - b) <= 1e-9 * fabs(b); }

static void check_agg(tsdb *db, series_ref *r, int64_t t0, int64_t interval, const char *when)
{
    row *f;
    size_t nf = ref_final(r, &f);
    if (!nf) {
        free(f);
        return;
    }
    tsdb_bucket *b;
    size_t nb;
    int64_t start = t0 - 5 * interval / 2, end = f[nf - 1].ts + 1;
    OK(tsdb_query_agg(db, r->id, start, end, interval, &b, &nb));
    size_t k = 0;
    bool ok = true;
    for (int64_t lo = floor_div(start, interval) * interval; lo < end && ok; lo += interval) {
        tsdb_bucket want;
        uint64_t rows;
        ref_agg(r->type, f, nf, MAX(lo, start), MIN(lo + interval, end), &want, &rows);
        if (!rows)
            continue;
        if (k >= nb || b[k].start != lo || b[k].count != want.count || !same_double(b[k].sum, want.sum) ||
            !same_double(b[k].min, want.min) || !same_double(b[k].max, want.max) || b[k].isum != want.isum ||
            b[k].imin != want.imin || b[k].imax != want.imax) {
            fprintf(stderr, "  %s agg mismatch %s at bucket %zu: count %" PRIu64 "/%" PRIu64 ", sum %g/%g\n",
                    type_name[r->type], when, k, k < nb ? b[k].count : 0, want.count, k < nb ? b[k].sum : 0,
                    want.sum);
            ok = false;
        }
        k++;
    }
    CHECK(ok && k == nb);
    free(b);
    free(f);
}

static void check_all(tsdb *db, series_ref *refs, size_t nr, int64_t t0, const char *when)
{
    for (size_t i = 0; i < nr; i++) {
        check_raw(db, &refs[i], when);
        check_agg(db, &refs[i], t0, 3600000, when);
        check_agg(db, &refs[i], t0, 7 * 60000, when);
    }
}

static tsdb *open_db(const char *dir)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.granule_rows = 256;
    o.partition_ms = 6 * 3600000;
    o.fsync = 0;
    o.wal_max_batch_rows = 1000;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    return db;
}

static void test_engine(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/types-%d", base, (int)getpid());
    tsdb *db = open_db(dir);
    const int64_t t0 = 1700000000000;
    series_ref refs[NTYPES];
    for (int t = 0; t < NTYPES; t++) {
        char name[32];
        snprintf(name, sizeof name, "s.%s", type_name[t]);
        refs[t] = (series_ref){0, t, NULL, 0, 0};
        OK(tsdb_series_create(db, name, (tsdb_type)t, NULL, &refs[t].id));
        tsdb_type got_type;
        OK(tsdb_series_type(db, refs[t].id, &got_type));
        CHECK((int)got_type == t);
    }
    /* parts (large batches), memtable (small ones), overwrites of both */
    for (int t = 0; t < NTYPES; t++) {
        insert_batch(db, &refs[t], t0, 1000, 0, 20000);
        insert_batch(db, &refs[t], t0, 1000, 20000, 300);
        insert_batch(db, &refs[t], t0, 1000, 5000, 3000);   /* overwrites part rows */
        insert_batch(db, &refs[t], t0, 1000, 20100, 100);   /* overwrites memtable rows */
        insert_batch(db, &refs[t], t0 + 500, 1000, 0, 400); /* between existing rows */
    }
    check_all(db, refs, NTYPES, t0, "before flush");
    OK(tsdb_flush(db));
    check_all(db, refs, NTYPES, t0, "after flush");
    OK(tsdb_optimize(db));
    check_all(db, refs, NTYPES, t0, "after optimize");

    /* type errors */
    uint32_t id;
    CHECK(tsdb_series_create(db, "s.float64", TSDB_INT64, NULL, &id) == TSDB_ERR_INVALID);
    OK(tsdb_series_create(db, "s.float64", TSDB_FLOAT64, NULL, &id));
    CHECK(id == refs[TSDB_FLOAT64].id);
    OK(tsdb_series(db, "s.int64", &id)); /* gets a series of any type */
    CHECK(id == refs[TSDB_INT64].id);
    CHECK(tsdb_series_create(db, "bad", (tsdb_type)9, NULL, &id) == TSDB_ERR_INVALID);
    int64_t ts1 = t0;
    float f1 = 1;
    CHECK(tsdb_insert_series(db, refs[TSDB_FLOAT64].id, &ts1, &f1, 1) == TSDB_ERR_INVALID);
    double d1 = 1;
    tsdb_values col = {TSDB_FLOAT64, &d1, NULL};
    CHECK(tsdb_insert_series_values(db, refs[TSDB_INT64].id, &ts1, &col, 1) == TSDB_ERR_INVALID);
    uint32_t mixed[2] = {refs[TSDB_FLOAT64].id, refs[TSDB_INT64].id};
    int64_t ts2[2] = {t0, t0};
    double d2[2] = {1, 2};
    col.values = d2;
    CHECK(tsdb_insert_values(db, mixed, ts2, &col, 2) == TSDB_ERR_INVALID);
    tsdb_points pts;
    CHECK(tsdb_query_points(db, refs[TSDB_FLOAT64].id, INT64_MIN, INT64_MAX, &pts) == TSDB_ERR_INVALID);

    /* state names */
    uint32_t c_run, c_stop, c_again;
    OK(tsdb_state(db, refs[TSDB_STATE].id, "running", &c_run));
    OK(tsdb_state(db, refs[TSDB_STATE].id, "stopped", &c_stop));
    OK(tsdb_state(db, refs[TSDB_STATE].id, "running", &c_again));
    CHECK(c_run >= 1 && c_stop != c_run && c_again == c_run);
    CHECK(tsdb_state_lookup(db, refs[TSDB_STATE].id, "idle", &c_again) == TSDB_ERR_NOTFOUND);
    CHECK(tsdb_state_name(db, refs[TSDB_STATE].id, c_stop) && strcmp(tsdb_state_name(db, refs[TSDB_STATE].id, c_stop), "stopped") == 0);
    CHECK(tsdb_state_name(db, refs[TSDB_STATE].id, 0) == NULL);

    /* exact integer sums, saturating, and a bucket of nulls only */
    uint32_t big;
    OK(tsdb_series_create(db, "big", TSDB_INT64, NULL, &big));
    enum { NB = 2000 };
    int64_t bt[NB], bv[NB];
    uint8_t bvalid[(NB + 7) / 8];
    memset(bvalid, 0xff, sizeof bvalid);
    for (int k = 0; k < NB; k++) {
        bt[k] = t0 + k * 1000;
        bv[k] = INT64_MAX - k;
        if (k >= 1000 && k < 1100)
            bvalid[k >> 3] &= (uint8_t)~(1u << (k & 7)); /* nulls only, in one bucket */
    }
    tsdb_values bcol = {TSDB_INT64, bv, bvalid};
    OK(tsdb_insert_series_values(db, big, bt, &bcol, NB));
    tsdb_bucket *b;
    size_t nb;
    OK(tsdb_query_agg(db, big, t0, t0 + 1000 * 1000, 0, &b, &nb));
    CHECK(nb == 1 && b[0].count == 1000 && b[0].isum == INT64_MAX && b[0].imax == INT64_MAX &&
          b[0].imin == INT64_MAX - 999 && b[0].sum > 9e21);
    free(b);
    OK(tsdb_query_agg(db, big, t0 + 1000 * 1000, t0 + 1100 * 1000, 0, &b, &nb));
    CHECK(nb == 1 && b[0].count == 0 && isnan(b[0].min) && b[0].isum == 0);
    free(b);
    OK(tsdb_query_agg(db, big, t0, t0 + 2 * 1000, 0, &b, &nb));
    CHECK(nb == 1 && b[0].count == 2 && b[0].isum == INT64_MAX && b[0].sum == 2.0 * (double)INT64_MAX - 1);
    free(b);

    OK(tsdb_close(db));
    db = open_db(dir);
    check_all(db, refs, NTYPES, t0, "after reopen");
    OK(tsdb_state_lookup(db, refs[TSDB_STATE].id, "stopped", &c_again));
    CHECK(c_again == c_stop);

    /* a crash with typed rows only in the WAL: they come back from it */
    tsdb_close(db);
    pid_t pid = fork();
    if (pid == 0) {
        db = open_db(dir);
        for (int t = 0; t < NTYPES; t++) {
            insert_batch(db, &refs[t], t0, 1000, 30000, 50); /* small: WAL + memtable */
            insert_batch(db, &refs[t], t0, 1000, 29990, 20); /* overwrites some of them */
        }
        _exit(0); /* no close: nothing flushed */
    }
    int status;
    waitpid(pid, &status, 0);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    for (int t = 0; t < NTYPES; t++) { /* the same generator state as the child: the same rows */
        insert_batch(NULL, &refs[t], t0, 1000, 30000, 50);
        insert_batch(NULL, &refs[t], t0, 1000, 29990, 20);
    }
    db = open_db(dir);
    check_all(db, refs, NTYPES, t0, "after WAL replay");
    tsdb_close(db);
    for (int t = 0; t < NTYPES; t++)
        free(refs[t].rows);
    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    CHECK(system(cmd) == 0);
}

/* ------------------------------------------------ reading version 1 files */

static void put(uint8_t **p, size_t *len, size_t *cap, const void *data, size_t n)
{
    if (*len + n > *cap) {
        *cap = MAX(*cap * 2, *len + n);
        *p = realloc(*p, *cap);
    }
    memcpy(*p + *len, data, n);
    *len += n;
}

static void write_file(const char *dir, const char *name, const void *data, size_t len)
{
    char path[700];
    snprintf(path, sizeof path, "%s/%s", dir, name);
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    CHECK(fd >= 0 && write(fd, data, len) == (ssize_t)len);
    close(fd);
}

static int meta_version(const char *dir)
{
    char path[700], buf[256] = "";
    snprintf(path, sizeof path, "%s/META", dir);
    FILE *f = fopen(path, "r");
    size_t n = f ? fread(buf, 1, sizeof buf - 1, f) : 0;
    if (f)
        fclose(f);
    buf[n] = '\0';
    char *v = strstr(buf, "version=");
    return v ? atoi(v + 8) : -1;
}

/* A part and a WAL segment in the version 1 formats (float32 values, 64-byte
 * index entries, 16-byte WAL rows), written by hand, are read, merged with
 * new data and survive a reopen. */
static void test_v1(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/types-v1-%d", base, (int)getpid());
    tsdb *db = open_db(dir);
    uint32_t id;
    OK(tsdb_series(db, "legacy", &id));
    CHECK(id == 0);
    OK(tsdb_close(db));

    const int64_t t0 = 1700000000000, pms = 6 * 3600000;
    enum { NP = 1000, G = 256, NW = 10, WAL_SEQ = 99 };
    int64_t ts[NP];
    float v[NP];
    double want_sum = 0;
    for (int k = 0; k < NP; k++) {
        ts[k] = t0 + k * 1000;
        v[k] = (float)(k % 100) / 4;
        want_sum += v[k];
    }
    uint8_t *buf = NULL;
    size_t len = 0, cap = 0;
    part_header h = {{'T', 'S', 'D', 'B', 'P', 'A', 'R', 'T'}, 1, G, 0};
    put(&buf, &len, &cap, &h, sizeof h);
    granule_v1 idx[(NP + G - 1) / G];
    codec_ctx *c = codec_new();
    uint8_t *blk = malloc(2 * block_bound(G));
    size_t ng = 0;
    for (int k0 = 0; k0 < NP; k0 += G, ng++) {
        int n = MIN(G, NP - k0);
        size_t tl = block_encode_ts(c, 1, ts + k0, (size_t)n, blk);
        size_t vl = block_encode_f32(c, 1, TSDB_VALUES_AUTO, v + k0, (size_t)n, blk + tl);
        agg_state a;
        agg_init(&a, TSDB_FLOAT32);
        simd_agg_f32(v + k0, (size_t)n, &a);
        idx[ng] = (granule_v1){0, (uint32_t)n, ts[k0], ts[k0 + n - 1], len, (uint32_t)tl, (uint32_t)vl, a.sum,
                               (float)a.min, (float)a.max, (uint32_t)xxh64(blk, tl + vl, 0), 0};
        put(&buf, &len, &cap, blk, tl + vl);
    }
    while (len % 8)
        put(&buf, &len, &cap, "", 1);
    part_footer f = {len, NP, ts[0], ts[NP - 1], (uint32_t)ng, 1, xxh64(idx, ng * sizeof *idx, 0),
                     {'T', 'S', 'D', 'B', 'E', 'N', 'D', '1'}};
    put(&buf, &len, &cap, idx, ng * sizeof *idx);
    put(&buf, &len, &cap, &f, sizeof f);
    char name[96];
    snprintf(name, sizeof name, "p%" PRId64 "_1_1_0.part", floor_div(t0, pms));
    write_file(dir, name, buf, len);
    /* Regression: a database made by version 1 (META says so) did not open */
    char meta[128];
    int ml = snprintf(meta, sizeof meta, "version=1\npartition_ms=%" PRId64 "\ngranule_rows=%d\n", pms, G);
    write_file(dir, "META", meta, (size_t)ml);
    free(buf);
    free(blk);
    codec_free(c);

    /* a version 1 WAL segment: rows after the part's */
    uint8_t wal[WAL_HDR + NW * 16];
    store32(wal, NW);
    for (int k = 0; k < NW; k++) {
        uint32_t sid = 0;
        int64_t t = t0 + NP * 1000 + k * 1000;
        float x = 100 + (float)k;
        memcpy(wal + WAL_HDR + 16 * k, &sid, 4);
        memcpy(wal + WAL_HDR + 16 * k + 4, &t, 8);
        memcpy(wal + WAL_HDR + 16 * k + 12, &x, 4);
        want_sum += x;
    }
    store32(wal + 4, (uint32_t)xxh64(wal + WAL_HDR, NW * 16, 0));
    char wname[64];
    snprintf(wname, sizeof wname, "wal_%016x.log", WAL_SEQ); /* segment numbers are hex in names */
    write_file(dir, wname, wal, sizeof wal);

    for (int round = 0; round < 3; round++) {
        if (round == 0) { /* a reader opens it as it is, and changes nothing */
            /* Regression: the MANIFEST a version 1 writer left (format 1)
             * was taken for a torn one, and readers saw no data */
            char man[512];
            int mlen = snprintf(man, sizeof man,
                                "tsdb-manifest 1\nversion 7\nseries 1\nwal_flushed %d\nwal_active %d\nparts 1\n%s\n",
                                WAL_SEQ - 1, WAL_SEQ, name);
            mlen += snprintf(man + mlen, sizeof man - (size_t)mlen, "checksum %016" PRIx64 "\n",
                             xxh64(man, (size_t)mlen, 0));
            write_file(dir, "MANIFEST", man, (size_t)mlen);
            tsdb_options ro;
            tsdb_options_init(&ro);
            ro.read_only = 1;
            OK(tsdb_open(dir, &ro, &db));
            tsdb_points p;
            OK(tsdb_query_points(db, 0, INT64_MIN, INT64_MAX, &p));
            CHECK(p.n == NP + NW); /* the WAL rows too, replayed into memory */
            tsdb_points_free(&p);
            OK(tsdb_close(db));
            CHECK(meta_version(dir) == 1);
        }
        db = open_db(dir);
        CHECK(meta_version(dir) == FORMAT_VERSION); /* a writer marks it, so version 1 refuses it */
        if (round == 1) {
            float x = 7;
            int64_t t = t0; /* overwrites the first row of the old part */
            OK(tsdb_insert_series(db, 0, &t, &x, 1));
            want_sum += 7 - v[0];
            OK(tsdb_optimize(db));
        }
        tsdb_points p;
        OK(tsdb_query_points(db, 0, INT64_MIN, INT64_MAX, &p));
        CHECK(p.n == NP + NW);
        bool same = p.n == NP + NW;
        for (size_t k = 0; same && k < NP; k++)
            same = p.ts[k] == ts[k] && (p.values[k] == v[k] || (k == 0 && round > 0 && p.values[k] == 7));
        for (size_t k = 0; same && k < NW; k++)
            same = p.values[NP + k] == 100 + (float)k;
        CHECK(same);
        tsdb_points_free(&p);
        tsdb_bucket *b;
        size_t nb;
        OK(tsdb_query_agg(db, 0, INT64_MIN, INT64_MAX, 0, &b, &nb));
        CHECK(nb == 1 && b[0].count == NP + NW && fabs(b[0].sum - want_sum) < 1e-6);
        free(b);
        OK(tsdb_close(db));
    }
    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    CHECK(system(cmd) == 0);
}

static void sum_granules(void *ctx, const tsdb_part_info *p) { *(uint64_t *)ctx += p->granules; }

/* tsdb_last_query_stats: what each kind of query reads. */
static void test_query_stats(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/types-stats-%d", base, (int)getpid());
    tsdb *db = open_db(dir); /* 256-row granules; batches of 1000+ rows become parts */
    uint32_t id;
    OK(tsdb_series_create(db, "s", TSDB_FLOAT64, NULL, &id));
    enum { N = 100000 };
    const int64_t t0 = 1700000000000;
    int64_t *ts = malloc(N * sizeof *ts);
    double *v = malloc(N * sizeof *v);
    for (int k = 0; k < N; k++) {
        ts[k] = t0 + k * 1000;
        v[k] = k % 100;
    }
    OK(tsdb_insert_series_values(db, id, ts, &(tsdb_values){TSDB_FLOAT64, v, NULL}, N));
    tsdb_query_stats st;
    tsdb_bucket *b;
    size_t nb;
    /* one bucket over everything: every granule from its statistics */
    OK(tsdb_query_agg(db, id, t0, t0 + (int64_t)N * 1000, 0, &b, &nb));
    free(b);
    tsdb_last_query_stats(&st);
    CHECK(st.rows == N && st.rows_from_stats == N && st.rows_decoded == 0 && st.bytes_read == 0);
    uint64_t granules = 0;
    OK(tsdb_foreach_part(db, sum_granules, &granules));
    CHECK(st.granules == granules && st.granules_from_stats == granules && st.result_rows == 1);
    CHECK(st.parts >= 1 && st.bytes == (uint64_t)N * 16 && st.elapsed_s > 0);
    /* buckets of 1 s: every granule decoded, by the pool's workers too */
    OK(tsdb_query_agg(db, id, t0, t0 + (int64_t)N * 1000, 1000, &b, &nb));
    free(b);
    tsdb_last_query_stats(&st);
    CHECK(st.rows_decoded == N && st.rows_from_stats == 0 && st.granules_from_stats == 0 && st.bytes_read > 0);
    CHECK(st.result_rows == N && nb == N);
    /* a raw window inside one granule: that granule */
    int64_t lo = t0 + 1000 * 1000;
    got g = {TSDB_FLOAT64, NULL, 0, 0};
    OK(tsdb_query_values(db, id, lo, lo + 10 * 1000, NULL, collect, &g));
    free(g.rows);
    tsdb_last_query_stats(&st);
    CHECK(st.granules == 1 && st.rows_decoded == 256 && st.result_rows == 10);
    /* rows in the memtable */
    int64_t tm = t0 + (int64_t)N * 1000;
    double vm = 1;
    OK(tsdb_insert_series_values(db, id, &tm, &(tsdb_values){TSDB_FLOAT64, &vm, NULL}, 1));
    OK(tsdb_query_agg(db, id, tm, tm + 1, 0, &b, &nb));
    free(b);
    tsdb_last_query_stats(&st);
    CHECK(st.memtable_rows == 1 && st.rows == 1 && st.parts == 0 && st.result_rows == 1);
    free(ts);
    free(v);
    OK(tsdb_close(db));
    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    CHECK(system(cmd) == 0);
}

int main(int argc, char **argv)
{
    const char *base = argc > 1 ? argv[1] : "/tmp";
    test_codecs();
    test_engine(base);
    test_v1(base);
    test_query_stats(base);
    if (failures) {
        printf("FAILED: %d checks\n", failures);
        return 1;
    }
    printf("type tests passed\n");
    return 0;
}
