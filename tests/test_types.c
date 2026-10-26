/* Tests: value types (float64, int64, bool, state) and nulls, against a
 * reference; their codecs; and refusing databases of older formats. */
#include "internal.h"
#include "../tools/datasets.h"
#include "hydraulic_psi.h"

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
        /* no fixed point but compressible (AUTO then compresses every candidate), and not applicable kinds
         * (estimates of INFINITY) must not be taken for the smallest */
        for (size_t k = 0; k < n; k++) {
            static const double few[] = {1.0 / 3, M_PI, 1.4142135623730951, -2.0 / 7};
            d[k] = few[(k / 5 + rnd() % 2) % 4];
        }
        both_levels(TSDB_FLOAT64, d, n, "a few odd doubles");
        for (size_t k = 0; k < n; k++)
            d[k] = (float)(20 + 5 * sin((double)k / 50) + (double)(rnd() % 100) / 1000);
        both_levels(TSDB_FLOAT64, d, n, "float32 readings");
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

/* The smallest float64 block of any kind, every one encoded and compressed in
 * full: what AUTO's sample test has to come close to. */
static size_t f64_full_try(codec_ctx *c, const double *d, size_t n, uint8_t *blk)
{
    static const int kinds[] = {BLK_F64_DECIMAL, BLK_XOR64, BLK_BYTESPLIT64, BLK_DELTA_BP, BLK_ALPRD};
    size_t best = SIZE_MAX;
    CHECK(codec_scratch_reserve(c, block_bound(n)) == 0);
    for (size_t k = 0; k < sizeof kinds / sizeof *kinds; k++) {
        size_t len = 0;
        switch (kinds[k]) {
        case BLK_F64_DECIMAL: len = f64_decimal_encode(d, n, c->scratch); break;
        case BLK_XOR64: len = xor64_encode((const uint64_t *)d, n, c->scratch); break;
        case BLK_DELTA_BP: len = delta_bp64_encode((const uint64_t *)d, n, c->scratch); break;
        case BLK_ALPRD: len = alprd64_encode((const uint64_t *)d, n, c->scratch); break;
        default:
            for (size_t i = 0; i < n; i++)
                for (int b = 0; b < 8; b++)
                    c->scratch[(size_t)b * n + i] = (uint8_t)(((const uint64_t *)d)[i] >> (8 * b));
            len = 8 * n;
        }
        if (len)
            best = MIN(best, block_wrap(c, 1, kinds[k], len, blk));
    }
    return best;
}

/* AUTO ranks float64 candidates on a sample and encodes only the one or two
 * that can win: the blocks must stay within a hair of trying every kind in full. */
static void test_f64_sample(void)
{
    enum { N = 8192 };
    double *d = malloc(N * sizeof *d);
    uint8_t *blk = malloc(block_bound(N) + BLK_SLACK);
    codec_ctx *c = codec_new();
    for (int set = 0; set < 10; set++) {
        static const char *names[] = {"random bits", "random in a range", "unrounded walk", "calibrated counts",
                                      "noisy sine", "unit conversion", "a few odd doubles", "float32 readings",
                                      "smooth unrounded", "a walk that comes back"};
        double x = 1000;
        for (size_t k = 0; k < N; k++) {
            uint64_t r = rnd();
            switch (set) {
            case 0: memcpy(&d[k], &r, 8); break;
            case 1: d[k] = (double)(r >> 11) / 9007199254740992.0 * 100; break;
            case 2: x += ((double)(r % 2001) - 1000) / 7777.0; d[k] = x - 990; break;
            case 3: x += (double)(r % 2001) - 1000; d[k] = x * 0.0123456789; break; /* ADC counts times a factor */
            case 4: d[k] = 50 + 10 * sin((double)k / 500) + (double)(r % 1000) / 3333.0; break;
            case 5: d[k] = round((2 + sin((double)k / 300) + (double)(r % 10) / 1000) * 1000) / 1000 * 14.503773773; break;
            case 6: d[k] = (double[]){1.0 / 3, M_PI, 1.4142135623730951, -2.0 / 7}[(k / 5 + r % 2) % 4]; break;
            case 7: d[k] = (float)(20 + 5 * sin((double)k / 50) + (double)(r % 100) / 1000); break;
            case 8: d[k] = 100 + 30 * sin((double)k / 2000) + (double)(r % 1000) * 1e-9; break;
            default: /* ZSTD's matches lie 3000 values apart, which no run of the sample spans */
                x += ((double)(r % 2001) - 1000) / 7777.0;
                d[k] = k < 3000 ? x : d[k - 3000];
            }
        }
        size_t len = roundtrip(TSDB_FLOAT64, d, N, 1, names[set]), full = f64_full_try(c, d, N, blk);
        if (len > full + full / 100)
            fprintf(stderr, "  float64 %s: AUTO %zu bytes, the best kind %zu\n", names[set], len, full);
        CHECK(len <= full + full / 100);
    }
    /* Sensor readings in float32 offset by 2^-30 (so not float32 any more): compressible and
     * without fixed point, where the estimates are least sure and AUTO compresses every candidate.
     * Over 16 blocks, as one block may favour the other. */
    for (int ds = 0; ds < DS_KINDS; ds++) {
        ds_state st;
        int64_t ts;
        float f;
        size_t sum = 0, sum_full = 0;
        ds_init(&st, (ds_kind)ds, 3, 1704067200000);
        for (int g = 0; g < 16; g++) {
            for (size_t k = 0; k < N; k++) {
                ds_next(&st, &ts, &f);
                d[k] = f + 0x1p-30;
            }
            sum += roundtrip(TSDB_FLOAT64, d, N, 1, ds_info[ds].name);
            sum_full += f64_full_try(c, d, N, blk);
        }
        if (sum > sum_full + sum_full / 100)
            fprintf(stderr, "  float64 %s + 2^-30: AUTO %zu bytes, the best kinds %zu\n", ds_info[ds].name, sum, sum_full);
        CHECK(sum <= sum_full + sum_full / 100);
    }
    codec_free(c);
    free(blk);
    free(d);
}

/* The smallest float32 block of AUTO's candidate kinds, every one encoded and compressed in full. */
static size_t f32_full_try(codec_ctx *c, const float *f, size_t n, uint8_t *blk)
{
    static const int kinds[] = {BLK_DECIMAL_BP, BLK_CHIMP, BLK_DELTA32, BLK_BYTESPLIT, BLK_ALPRD, BLK_DELTA_BP};
    const uint32_t *u = (const uint32_t *)f;
    size_t best = SIZE_MAX;
    CHECK(codec_scratch_reserve(c, block_bound(n)) == 0);
    for (size_t k = 0; k < sizeof kinds / sizeof *kinds; k++) {
        size_t len = 0;
        switch (kinds[k]) {
        case BLK_DECIMAL_BP: len = decimal_bp_encode(f, n, c->scratch); break;
        case BLK_CHIMP: len = chimp_encode(u, n, c->scratch); break;
        case BLK_DELTA32: len = delta32_encode(u, n, c->scratch); break;
        case BLK_BYTESPLIT: len = bytesplit_encode(u, n, c->scratch); break;
        case BLK_ALPRD: len = alprd_encode(u, n, c->scratch); break;
        default: len = delta_bp32_encode(u, n, c->scratch);
        }
        if (len)
            best = MIN(best, block_wrap(c, 1, kinds[k], len, blk));
    }
    return best;
}

/* float32 AUTO ranks its candidates on a sample compressed with ZSTD. Real
 * pressures converted from bar to psi repeat their deltas across the block:
 * ranked by entropy estimates, AUTO took bit-packed deltas 1.5 times the size
 * of plain deltas with ZSTD on these two granules. */
static void test_f32_sample(void)
{
    enum { N = HYD_N };
    float *f = malloc(N * sizeof *f);
    uint8_t *blk = malloc(block_bound(N) + BLK_SLACK);
    codec_ctx *c = codec_new();
    static const struct {
        const char *name;
        const int32_t *mbar;
    } real[] = {{"PS2 in psi", hyd_ps2}, {"PS3 in psi", hyd_ps3}};
    for (size_t r = 0; r < sizeof real / sizeof *real; r++) {
        for (size_t k = 0; k < N; k++)
            f[k] = (float)(real[r].mbar[k] / 1000.0 * 14.503773773);
        size_t len = roundtrip(TSDB_FLOAT32, f, N, 1, real[r].name), full = f32_full_try(c, f, N, blk);
        if (len > full + full / 100)
            fprintf(stderr, "  float32 %s: AUTO %zu bytes, the best kind %zu\n", real[r].name, len, full);
        CHECK(len <= full + full / 100);
    }
    /* the generated sensors, 16 blocks each */
    for (int ds = 0; ds < DS_KINDS; ds++) {
        ds_state st;
        int64_t ts;
        size_t sum = 0, sum_full = 0;
        ds_init(&st, (ds_kind)ds, 3, 1704067200000);
        for (int g = 0; g < 16; g++) {
            for (size_t k = 0; k < N; k++)
                ds_next(&st, &ts, &f[k]);
            sum += roundtrip(TSDB_FLOAT32, f, N, 1, ds_info[ds].name);
            sum_full += f32_full_try(c, f, N, blk);
        }
        if (sum > sum_full + sum_full / 100)
            fprintf(stderr, "  float32 %s: AUTO %zu bytes, the best kinds %zu\n", ds_info[ds].name, sum, sum_full);
        CHECK(sum <= sum_full + sum_full / 100);
    }
    codec_free(c);
    free(blk);
    free(f);
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

/* ------------------------------------------------ older formats */

static void write_file(const char *dir, const char *name, const void *data, size_t len)
{
    char path[700];
    snprintf(path, sizeof path, "%s/%s", dir, name);
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    CHECK(fd >= 0 && write(fd, data, len) == (ssize_t)len);
    close(fd);
}

/* No release had the older formats, so they are not read: a database whose
 * META names another version is refused, by writers and readers alike. */
static void test_old_version(const char *base)
{
    char dir[600], cmd[1300];
    snprintf(dir, sizeof dir, "%s/types-oldver-%d", base, (int)getpid());
    snprintf(cmd, sizeof cmd, "rm -rf '%s' && mkdir -p '%s'", dir, dir);
    CHECK(system(cmd) == 0);
    tsdb_options o;
    tsdb_options_init(&o);
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_close(db));
    for (int v = 1; v <= FORMAT_VERSION + 1; v++) {
        if (v == FORMAT_VERSION)
            continue;
        char meta[128];
        int n = snprintf(meta, sizeof meta, "version=%d\npartition_ms=86400000\ngranule_rows=8192\n", v);
        write_file(dir, "META", meta, (size_t)n);
        o.read_only = 0;
        CHECK(tsdb_open(dir, &o, &db) == TSDB_ERR_CORRUPT);
        o.read_only = 1;
        CHECK(tsdb_open(dir, &o, &db) == TSDB_ERR_CORRUPT);
    }
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

/* ------------------------------------------------- canonical NaN results */

/* Every aggregate result that is NaN is the one quiet NaN 0x7ff8000000000000,
 * whatever payloads the NaNs in the data had and in whatever order the engine
 * added them (memtable, parts, overlapping sources, merges, threads); raw
 * reads return each stored NaN bit for bit. */
#define NAN_ROWS 40000
#define NAN_STEP 1000 /* ms: 40000 rows span two 6-hour partitions */
#define NAN_ALL0 5000 /* rows [NAN_ALL0, NAN_ALL0 + 300) are NaN only */

static const uint64_t nan_f64[] = {
    0x7ff8000000000001ull, 0xfff8000000000000ull, 0x7ff0000000000001ull, 0xfff0000000000001ull,
    0x7ff0000000001001ull, /* signalling: quiets into TSDB_NULL_F64_BITS */
    0x7ff8000000001000ull, 0x7ff8000000001002ull, 0xfff8000000001001ull, 0x7fffffffffffffffull,
    0xffffffffffffffffull, 0x7ff8dead00000000ull, 0x7ff4000000000000ull};
static const uint32_t nan_f32[] = {0x7fc00001u, 0xffc00000u, 0x7f800001u, 0xff800001u, 0x7f801001u, 0x7fc01000u,
                                   0x7fc01002u, 0xffc01001u, 0x7fffffffu, 0xffffffffu, 0x7fc0deadu, 0x7fa00000u};

static uint64_t nan_slot(int type)
{
    uint64_t r = rnd();
    if (r % 3) /* from the list */
        return type == TSDB_FLOAT64 ? nan_f64[r / 3 % 12] : nan_f32[r / 3 % 12];
    /* any other: random sign and payload, never the null */
    uint64_t s;
    do {
        uint64_t p = rnd();
        if (type == TSDB_FLOAT64)
            s = (p & 0x800fffffffffffffull) | 0x7ff0000000000000ull;
        else
            s = (p & 0x807fffffu) | 0x7f800000u;
    } while ((type == TSDB_FLOAT64 ? (s & 0x000fffffffffffffull) : (s & 0x7fffffu)) == 0 || s == type_null(type));
    return s;
}

static uint64_t num_slot(int type, size_t k)
{
    double d = (double)(k % 997) * 0.25 - 50;
    if (type == TSDB_FLOAT64) {
        uint64_t s;
        memcpy(&s, &d, 8);
        return s;
    }
    float f = (float)d;
    uint32_t s;
    memcpy(&s, &f, 4);
    return s;
}

/* Writes rows k0, k0 + stride, ... below k1 in batches of `batch`; with `nans` one row in that many is a NaN
 * (every row in the NaN-only range), one in 101 a null, the rest numbers. */
static void nan_write(tsdb *db, uint32_t id, int type, uint64_t *ref, size_t k0, size_t k1, size_t stride,
                      size_t batch, unsigned nans)
{
    int64_t *ts = malloc(batch * sizeof *ts);
    uint8_t *v = calloc(batch, 8), *valid = calloc((batch + 7) / 8, 1);
    size_t w = type_width(type);
    for (size_t k = k0; k < k1;) {
        size_t n = 0;
        memset(valid, 0, (batch + 7) / 8);
        for (; n < batch && k < k1; k += stride, n++) {
            bool all = k >= NAN_ALL0 && k < NAN_ALL0 + 300;
            uint64_t s = all || rnd() % nans == 0 ? nan_slot(type) : rnd() % 101 == 0 ? type_null(type)
                                                                                       : num_slot(type, k + rnd() % 7);
            ts[n] = 1700000000000 + (int64_t)k * NAN_STEP;
            bool known = s != type_null(type);
            valid[n >> 3] |= (uint8_t)(known << (n & 7));
            slot_put(v, w, n, known ? s : 0);
            ref[k] = s;
        }
        OK(tsdb_insert_series_values(db, id, ts, &(tsdb_values){(tsdb_type)type, v, valid}, n));
    }
    free(ts);
    free(v);
    free(valid);
}

static bool is_canon(double d)
{
    uint64_t b;
    memcpy(&b, &d, 8);
    return d == d || b == 0x7ff8000000000000ull;
}

/* every NaN in the buckets canonical; returns the number of NaN sums and adds the NaN mins to *nan_min */
static size_t nan_check_buckets(const tsdb_bucket *b, size_t nb, size_t *nan_min, const char *what)
{
    size_t nan_sum = 0, bad = 0;
    for (size_t i = 0; i < nb; i++) {
        nan_sum += isnan(b[i].sum);
        *nan_min += isnan(b[i].min) && b[i].count;
        bad += !is_canon(b[i].sum) + !is_canon(b[i].min) + !is_canon(b[i].max);
        if (!is_canon(b[i].sum) || !is_canon(b[i].min) || !is_canon(b[i].max)) {
            uint64_t s, mn, mx;
            memcpy(&s, &b[i].sum, 8), memcpy(&mn, &b[i].min, 8), memcpy(&mx, &b[i].max, 8);
            if (bad < 4)
                fprintf(stderr, "  %s bucket %zu: sum %016" PRIx64 " min %016" PRIx64 " max %016" PRIx64 "\n", what,
                        i, s, mn, mx);
        }
        /* the version 1 wire rows narrow min and max to float32: canonical there too */
        float f[2] = {(float)b[i].min, (float)b[i].max};
        uint32_t fb[2];
        memcpy(fb, f, 8);
        CHECK(!isnan(f[0]) || fb[0] == 0x7fc00000u);
        CHECK(!isnan(f[1]) || fb[1] == 0x7fc00000u);
    }
    CHECK(bad == 0);
    return nan_sum;
}

static void nan_check(tsdb *db, const uint32_t *ids, uint64_t *const *refs, const char *when)
{
    const int64_t t0 = 1700000000000, t1 = t0 + (int64_t)NAN_ROWS * NAN_STEP;
    for (int t = 0; t < 2; t++) {
        int type = t ? TSDB_FLOAT64 : TSDB_FLOAT32;
        char what[96];
        snprintf(what, sizeof what, "%s %s", type_name[type], when);
        size_t nan_sum = 0, nan_min = 0, nb;
        tsdb_bucket *b;
        /* one bucket over all: granule statistics for most granules */
        OK(tsdb_query_agg(db, ids[t], t0, t1, 0, &b, &nb));
        size_t known = 0;
        for (size_t k = 0; k < NAN_ROWS; k++)
            known += refs[t][k] != type_null(type);
        CHECK(nb == 1 && b[0].count == known && isnan(b[0].sum));
        nan_sum += nan_check_buckets(b, nb, &nan_min, what);
        free(b);
        static const int64_t iv[] = {7000, 60000, 256000, 3600000};
        for (size_t i = 0; i < sizeof iv / sizeof *iv; i++) {
            OK(tsdb_query_agg(db, ids[t], t0, t1, iv[i], &b, &nb));
            nan_sum += nan_check_buckets(b, nb, &nan_min, what);
            free(b);
        }
        OK(tsdb_query_group(db, ids[t], t0, t1, &(tsdb_group){TSDB_HOUR, 1, NULL}, &b, &nb));
        nan_sum += nan_check_buckets(b, nb, &nan_min, what);
        free(b);
        int64_t bounds[] = {t0, t0 + 3333 * NAN_STEP, t0 + 4999 * NAN_STEP, t0 + 17777 * NAN_STEP, t0 + 30001 * NAN_STEP};
        OK(tsdb_query_bounds(db, ids[t], t0, t1, bounds, 5, NULL, &b, &nb));
        nan_sum += nan_check_buckets(b, nb, &nan_min, what);
        free(b);
        /* the NaN-only rows: min and max NaN too */
        OK(tsdb_query_agg(db, ids[t], t0 + NAN_ALL0 * NAN_STEP, t0 + (NAN_ALL0 + 300) * NAN_STEP, 0, &b, &nb));
        CHECK(nb == 1 && b[0].count == 300 && isnan(b[0].min) && isnan(b[0].max));
        nan_sum += nan_check_buckets(b, nb, &nan_min, what);
        free(b);
        CHECK(nan_sum > 50 && nan_min >= 2);
        /* raw: every payload as written */
        got g = {type, NULL, 0, 0};
        OK(tsdb_query_values(db, ids[t], INT64_MIN, INT64_MAX, NULL, collect, &g));
        bool same = g.n == NAN_ROWS;
        for (size_t k = 0; same && k < NAN_ROWS; k++)
            same = g.rows[k].ts == t0 + (int64_t)k * NAN_STEP && g.rows[k].slot == refs[t][k];
        if (!same)
            fprintf(stderr, "  %s: raw NaN payloads changed\n", what);
        CHECK(same);
        free(g.rows);
    }
}

static void test_nan_canonical(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/types-nan-%d", base, (int)getpid());
    tsdb_options o;
    tsdb_options_init(&o);
    o.granule_rows = 256;
    o.partition_ms = 6 * 3600000;
    o.fsync = 0;
    o.wal_max_batch_rows = 1000;
    o.query_threads = 8;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    uint32_t ids[2];
    uint64_t *refs[2];
    OK(tsdb_series_create(db, "nan.f32", TSDB_FLOAT32, NULL, &ids[0]));
    OK(tsdb_series_create(db, "nan.f64", TSDB_FLOAT64, NULL, &ids[1]));
    for (int t = 0; t < 2; t++) {
        int type = t ? TSDB_FLOAT64 : TSDB_FLOAT32;
        refs[t] = malloc(NAN_ROWS * sizeof *refs[t]);
        /* small batches into the WAL and memtable, large ones straight into parts */
        nan_write(db, ids[t], type, refs[t], 0, NAN_ROWS / 2, 1, 500, 37);
        nan_write(db, ids[t], type, refs[t], NAN_ROWS / 2, NAN_ROWS, 1, 4000, 23);
    }
    nan_check(db, ids, refs, "memtable and parts");
    OK(tsdb_flush(db));
    nan_check(db, ids, refs, "flushed");
    /* a read-only follower of the same directory answers from what it refreshes: canonical there too */
    tsdb_options ro;
    tsdb_options_init(&ro);
    ro.read_only = 1;
    ro.query_threads = 8;
    tsdb *fol;
    OK(tsdb_open(dir, &ro, &fol));
    nan_check(fol, ids, refs, "follower");
    /* rewrite every third row of a range: overlapping sources, newer NaNs over older ones */
    for (int t = 0; t < 2; t++) {
        nan_write(db, ids[t], t ? TSDB_FLOAT64 : TSDB_FLOAT32, refs[t], 3001, 31001, 3, 2500, 5);
        nan_write(db, ids[t], t ? TSDB_FLOAT64 : TSDB_FLOAT32, refs[t], 10002, 12002, 3, 300, 3);
    }
    nan_check(db, ids, refs, "overlapping");
    OK(tsdb_flush(db));
    OK(tsdb_refresh(fol));
    nan_check(fol, ids, refs, "follower, overlapping");
    OK(tsdb_optimize(db));
    nan_check(db, ids, refs, "merged");
    OK(tsdb_refresh(fol));
    nan_check(fol, ids, refs, "follower, merged");
    OK(tsdb_close(fol));
    OK(tsdb_close(db));
    for (int threads = 1; threads <= 8; threads += 7) {
        o.query_threads = threads;
        OK(tsdb_open(dir, &o, &db));
        nan_check(db, ids, refs, threads == 1 ? "reopened, 1 thread" : "reopened, 8 threads");
        OK(tsdb_close(db));
    }
    free(refs[0]);
    free(refs[1]);
    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    CHECK(system(cmd) == 0);
}

int main(int argc, char **argv)
{
    const char *base = argc > 1 ? argv[1] : "/tmp";
    test_codecs();
    test_f64_sample();
    test_f32_sample();
    test_engine(base);
    test_old_version(base);
    test_query_stats(base);
    test_nan_canonical(base);
    if (failures) {
        printf("FAILED: %d checks\n", failures);
        return 1;
    }
    printf("type tests passed\n");
    return 0;
}
