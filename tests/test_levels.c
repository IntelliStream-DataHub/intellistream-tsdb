/*
 * Every CPU level gives the same bytes and the same numbers.
 *
 * A portable build compiles the codecs and value folds per x86-64 level
 * (src/levels.h) and picks the aggregation kernels at runtime (src/simd.c);
 * a part written on one CPU is read on another, and its granule statistics
 * answer queries. So at every level this CPU has, and with every aggregation
 * kernel up to it, this encodes the sensor datasets, the type-bench streams and
 * edge cases (NaN payloads, infinities, -0, denormals, nulls, int64 extremes,
 * odd lengths) with each codec and the block layer, decodes them, folds them
 * into aggregates and granule statistics, and compares everything, byte for
 * byte, with the baseline. Decoded values must also equal the input.
 *
 * In a build without CPU levels (the default -march=native `make`) the codecs
 * exist once and only the aggregation kernels vary; the test still runs.
 *
 *   test_levels            (TSDB_CPU caps the levels it can reach)
 */
#include "internal.h"
#include "../tools/datasets.h"

#include <float.h>
#include <stdio.h>
#include <stdlib.h>

static int failures;
#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                              \
        }                                                                            \
    } while (0)

/* ---- what one configuration produced: a name and a hash per output ---- */
typedef struct rec {
    size_t n, cap;
    char (*name)[160];
    uint64_t *hash;
} rec;

static uint64_t fnv(const void *p, size_t len, uint64_t h)
{
    const uint8_t *b = p;
    for (size_t i = 0; i < len; i++)
        h = (h ^ b[i]) * 0x100000001b3ull;
    return h;
}

static void put(rec *r, const char *what, const char *cs, const void *p, size_t len)
{
    if (r->n == r->cap) {
        r->cap = r->cap ? 2 * r->cap : 4096;
        r->name = realloc(r->name, r->cap * sizeof *r->name);
        r->hash = realloc(r->hash, r->cap * sizeof *r->hash);
    }
    snprintf(r->name[r->n], sizeof r->name[0], "%.80s / %.70s", cs, what);
    r->hash[r->n++] = fnv(p, len, fnv(&len, sizeof len, 0xcbf29ce484222325ull));
}

/* ---- the inputs ---- */
#define GR 8192
typedef struct input {
    char name[64];
    int type;
    size_t n;
    void *v;     /* n slots of type_width(type) */
    int64_t *ts; /* n sorted timestamps */
} input;

static input in[256];
static int nin;

static input *add(const char *name, int type, size_t n)
{
    input *x = &in[nin++];
    snprintf(x->name, sizeof x->name, "%s", name);
    x->type = type;
    x->n = n;
    x->v = calloc(n ? n : 1, type_width(type));
    x->ts = calloc(n ? n : 1, sizeof *x->ts);
    for (size_t i = 0; i < n; i++)
        x->ts[i] = 1700000000000 + (int64_t)i * 1000;
    return x;
}

static uint64_t rnd = 42;
static uint64_t next(void) { return rnd = ds_mix(rnd + 0x9e3779b97f4a7c15ull); }

static void make_inputs(void)
{
    char name[64];
    /* the sensor streams of codec-bench and type-bench, as float32 and float64, with their timestamps */
    for (int k = 0; k < DS_KINDS; k++) {
        ds_state st;
        snprintf(name, sizeof name, "f32 %s", ds_info[k].name);
        input *x = add(name, TSDB_FLOAT32, GR);
        ds_init(&st, (ds_kind)k, 3, 1704067200000);
        for (size_t i = 0; i < GR; i++)
            ds_next(&st, &x->ts[i], (float *)x->v + i);
        snprintf(name, sizeof name, "f64 %s", ds_info[k].name);
        x = add(name, TSDB_FLOAT64, GR);
        ds_init(&st, (ds_kind)k, 7, 1700000000000);
        for (size_t i = 0; i < GR; i++)
            ds_next_f64(&st, &x->ts[i], (double *)x->v + i);
    }
    /* type-bench's integer, bool and state streams */
    input *x = add("int64 totalizer", TSDB_INT64, GR);
    double liters = 0;
    for (size_t i = 0; i < GR; i++) {
        liters += (245 + 5 * sin((double)i / 3600) + (double)(next() % 100) / 100) / 3.6;
        ((int64_t *)x->v)[i] = (int64_t)liters;
    }
    x = add("int64 random rate", TSDB_INT64, GR);
    for (size_t i = 0; i < GR; i++)
        ((int64_t *)x->v)[i] = 50000 + (int64_t)(next() % 20000);
    x = add("int64 sequence", TSDB_INT64, GR);
    for (size_t i = 0; i < GR; i++)
        ((int64_t *)x->v)[i] = 9000000000000 + (int64_t)i * 3;
    x = add("int64 extremes", TSDB_INT64, GR);
    for (size_t i = 0; i < GR; i++) {
        static const int64_t e[] = {INT64_MAX, INT64_MIN + 1, 0, -1, 1, INT64_MAX - 1, INT64_MIN}; /* MIN: null */
        ((int64_t *)x->v)[i] = i % 3 ? e[next() % 7] : (int64_t)next();
    }
    x = add("int64 random bits", TSDB_INT64, GR);
    for (size_t i = 0; i < GR; i++)
        ((int64_t *)x->v)[i] = (int64_t)next();
    x = add("int64 nulls", TSDB_INT64, 1000);
    for (size_t i = 0; i < 1000; i++)
        ((int64_t *)x->v)[i] = next() % 4 ? NULL_I64 : (int64_t)(next() % 100) - 50;
    /* int64 kernels: lane sums in 32-bit halves and a sign count, squares in vectors only within int32 */
    static const size_t i64_n[] = {GR, 1000, 33, 32, 31, 17, 16, 15, 9, 8, 7, 1};
    for (size_t k = 0; k < sizeof i64_n / sizeof *i64_n; k++) {
        snprintf(name, sizeof name, "int64 int32 range with nulls, %zu", i64_n[k]);
        x = add(name, TSDB_INT64, i64_n[k]);
        for (size_t i = 0; i < x->n; i++) {
            static const int64_t e[] = {INT32_MIN, INT32_MAX, -1, 0, 1, INT32_MIN + 1};
            uint64_t r = next();
            ((int64_t *)x->v)[i] = r % 5 == 0 ? NULL_I64 : r % 5 == 1 ? e[(r >> 8) % 6] : (int32_t)(r >> 16);
        }
        snprintf(name, sizeof name, "int64 just past int32, %zu", i64_n[k]);
        x = add(name, TSDB_INT64, i64_n[k]);
        for (size_t i = 0; i < x->n; i++)
            ((int64_t *)x->v)[i] = i == x->n / 2 ? (int64_t)INT32_MAX + 2 : (int32_t)next();
        snprintf(name, sizeof name, "int64 most negative, %zu", i64_n[k]);
        x = add(name, TSDB_INT64, i64_n[k]);
        for (size_t i = 0; i < x->n; i++)
            ((int64_t *)x->v)[i] = next() % 9 ? INT64_MIN + 1 : NULL_I64;
        snprintf(name, sizeof name, "int64 most positive, %zu", i64_n[k]);
        x = add(name, TSDB_INT64, i64_n[k]);
        for (size_t i = 0; i < x->n; i++)
            ((int64_t *)x->v)[i] = INT64_MAX - (int64_t)(next() % 3);
    }
    x = add("int64 only nulls", TSDB_INT64, 100);
    for (size_t i = 0; i < 100; i++)
        ((int64_t *)x->v)[i] = NULL_I64;
    x = add("bool door", TSDB_BOOL, GR);
    for (size_t i = 0; i < GR; i++)
        ((uint32_t *)x->v)[i] = i ? (next() % 1200 == 0 ? !((uint32_t *)x->v)[i - 1] : ((uint32_t *)x->v)[i - 1]) : 0;
    x = add("bool coin with nulls", TSDB_BOOL, GR);
    for (size_t i = 0; i < GR; i++)
        ((uint32_t *)x->v)[i] = (uint32_t)(next() % 3); /* 2 is null */
    x = add("state machine mode", TSDB_STATE, GR);
    for (size_t i = 0; i < GR; i++)
        ((uint32_t *)x->v)[i] = i ? (next() % 300 == 0 ? 1 + (uint32_t)(next() % 5) : ((uint32_t *)x->v)[i - 1]) : 1;
    x = add("state with nulls", TSDB_STATE, 777);
    for (size_t i = 0; i < 777; i++)
        ((uint32_t *)x->v)[i] = (uint32_t)(next() % 7); /* 0 is null */
    /* bool and state kernels: nulls at every lane position, codes at the top of uint32 (squares near 2^64),
     * values a bool should not hold (a damaged block decodes to anything), runs of only nulls */
    for (size_t k = 0; k < sizeof i64_n / sizeof *i64_n; k++) {
        snprintf(name, sizeof name, "state near 2^32 with nulls, %zu", i64_n[k]);
        x = add(name, TSDB_STATE, i64_n[k]);
        for (size_t i = 0; i < x->n; i++) {
            uint64_t r = next();
            ((uint32_t *)x->v)[i] = r % 4 == 0 ? NULL_STATE : r % 4 == 1 ? UINT32_MAX - (uint32_t)(r >> 40) % 3
                                    : r % 4 == 2 ? 1 + (uint32_t)(r >> 40) % 3 : (uint32_t)(r >> 16);
        }
        snprintf(name, sizeof name, "bool odd values with nulls, %zu", i64_n[k]);
        x = add(name, TSDB_BOOL, i64_n[k]);
        for (size_t i = 0; i < x->n; i++) {
            static const uint32_t e[] = {0, 1, NULL_BOOL, 3, UINT32_MAX, 0x80000000u};
            uint64_t r = next();
            ((uint32_t *)x->v)[i] = r % 3 ? (uint32_t)(r >> 8) % 3 : e[(r >> 8) % 6];
        }
    }
    x = add("state only nulls", TSDB_STATE, 100);
    for (size_t i = 0; i < 100; i++)
        ((uint32_t *)x->v)[i] = NULL_STATE;
    x = add("bool only nulls", TSDB_BOOL, 99);
    for (size_t i = 0; i < 99; i++)
        ((uint32_t *)x->v)[i] = NULL_BOOL;

    /* float32 edge cases */
    static const uint32_t f32_special[] = {
        0x00000000, 0x80000000, 0x7f800000, 0xff800000, 0x7fc00000, 0xffc00000, 0x7f800001, 0x7fffffff,
        0x00000001, 0x807fffff, 0x00800000, 0x7f7fffff, 0xff7fffff, 0x3f800000, 0x4b800001, 0xcf000000,
    };
    static const size_t lens[] = {1, 2, 3, 15, 16, 17, 31, 33, 1000, 8191, GR};
    for (size_t l = 0; l < sizeof lens / sizeof *lens; l++) {
        size_t n = lens[l];
        snprintf(name, sizeof name, "f32 specials, %zu", n);
        x = add(name, TSDB_FLOAT32, n);
        for (size_t i = 0; i < n; i++) {
            uint32_t b = next() % 3 ? f32_special[next() % 16] : (uint32_t)(next() % 100000) * 0x3c23d70a / 100000;
            memcpy((uint32_t *)x->v + i, &b, 4);
        }
        snprintf(name, sizeof name, "f32 decimals with nulls, %zu", n);
        x = add(name, TSDB_FLOAT32, n);
        for (size_t i = 0; i < n; i++) {
            float f = (float)((int64_t)(next() % 200001) - 100000) / 100;
            uint32_t b;
            memcpy(&b, &f, 4);
            ((uint32_t *)x->v)[i] = next() % 5 ? b : NULL_F32;
        }
    }
    struct { const char *name; float a, step; } ramps[] = {
        {"f32 zeros", 0, 0},          {"f32 -0", -0.0f, 0},        {"f32 2^24 counter", 16777000, 1},
        {"f32 beyond 2^24", 3e7f, 3}, {"f32 tiny", 1e-40f, 1e-45f}, {"f32 huge", 3e38f, -1e34f},
        {"f32 0.1 steps", 0, 0.1f},   {"f32 -1e9 ramp", -1e9f, 12345.678f},
    };
    for (size_t r = 0; r < sizeof ramps / sizeof *ramps; r++) {
        x = add(ramps[r].name, TSDB_FLOAT32, GR);
        for (size_t i = 0; i < GR; i++)
            ((float *)x->v)[i] = ramps[r].a + (float)i * ramps[r].step;
    }
    x = add("f32 random bits", TSDB_FLOAT32, GR);
    for (size_t i = 0; i < GR; i++)
        ((uint32_t *)x->v)[i] = (uint32_t)next();
    /* finite values of random sign and magnitude: their sum in double rounds, so it depends on the
     * order of the additions (the kernels must share one); lengths around the 32-lane groups too */
    static const size_t spread_n[] = {GR, 64, 31, 33, 47, 95, 1000};
    for (size_t k = 0; k < sizeof spread_n / sizeof *spread_n; k++) {
        snprintf(name, sizeof name, "f32 random exponents, %zu", spread_n[k]);
        x = add(name, TSDB_FLOAT32, spread_n[k]);
        for (size_t i = 0; i < x->n; i++) {
            uint64_t r = next();
            uint32_t exp = 127 - 40 + (uint32_t)(r % 81); /* 2^-40 .. 2^40 */
            ((uint32_t *)x->v)[i] = (uint32_t)(r >> 63) << 31 | exp << 23 | (uint32_t)(r >> 8 & 0x7fffff);
        }
    }
    /* the same with nulls, which the kernels mask to +0.0 in their lane (lane order kept): every position mod
     * 16 and 32, runs of nulls longer than a lane group, nothing but nulls; and float64 likewise */
    static const size_t null_n[] = {GR, 1000, 95, 64, 47, 33, 32, 31, 17, 16, 15, 1};
    for (size_t k = 0; k < sizeof null_n / sizeof *null_n; k++) {
        for (int t = 0; t < 2; t++) {
            snprintf(name, sizeof name, "%s random exponents with nulls, %zu", t ? "f64" : "f32", null_n[k]);
            x = add(name, t ? TSDB_FLOAT64 : TSDB_FLOAT32, null_n[k]);
            for (size_t i = 0; i < x->n; i++) {
                uint64_t r = next();
                bool null = r % 3 == 0 || (i / 40) % 5 == 4; /* scattered, and runs of 40 */
                if (t) {
                    uint64_t exp = 1023 - 60 + (r >> 8) % 121; /* 2^-60 .. 2^60 */
                    ((uint64_t *)x->v)[i] = null ? NULL_F64 : (r >> 63) << 63 | exp << 52 | (next() >> 12);
                } else {
                    uint32_t exp = 127 - 40 + (uint32_t)((r >> 8) % 81);
                    ((uint32_t *)x->v)[i] =
                        null ? NULL_F32 : (uint32_t)(r >> 63) << 31 | exp << 23 | (uint32_t)(r >> 20 & 0x7fffff);
                }
            }
        }
    }
    x = add("f32 only nulls", TSDB_FLOAT32, 100);
    for (size_t i = 0; i < 100; i++)
        ((uint32_t *)x->v)[i] = NULL_F32;
    x = add("f64 only nulls", TSDB_FLOAT64, 100);
    for (size_t i = 0; i < 100; i++)
        ((uint64_t *)x->v)[i] = NULL_F64;
    x = add("f32 NaN, inf and nulls", TSDB_FLOAT32, 200);
    for (size_t i = 0; i < 200; i++) {
        static const uint32_t sp[] = {0x7fc00000, 0xffc00001, 0x7f800000, 0xff800000, 0x80000000, 0, NULL_F32,
                                      0x3f800000};
        ((uint32_t *)x->v)[i] = sp[next() % 8];
    }
    x = add("f64 -0 and +0 with nulls", TSDB_FLOAT64, 100);
    for (size_t i = 0; i < 100; i++)
        ((uint64_t *)x->v)[i] = next() % 3 ? (next() & 1 ? 0x8000000000000000ull : 0) : NULL_F64;
    x = add("f32 -0 and +0", TSDB_FLOAT32, 100);
    for (size_t i = 0; i < 100; i++)
        ((float *)x->v)[i] = next() & 1 ? -0.0f : 0.0f;

    /* float64 edge cases */
    static const uint64_t f64_special[] = {
        0, 0x8000000000000000ull, 0x7ff0000000000000ull, 0xfff0000000000000ull, 0x7ff8000000000000ull,
        0xfff8000000000001ull, 0x7ff0000000000001ull, 1, 0x000fffffffffffffull, 0x7fefffffffffffffull,
        0x3ff0000000000000ull, 0x4340000000000001ull, 0xc3e0000000000000ull, 0x43e0000000000000ull,
        0x3fb999999999999aull, NULL_F64,
    };
    for (size_t l = 0; l < sizeof lens / sizeof *lens; l++) {
        size_t n = lens[l];
        snprintf(name, sizeof name, "f64 specials, %zu", n);
        x = add(name, TSDB_FLOAT64, n);
        for (size_t i = 0; i < n; i++)
            ((uint64_t *)x->v)[i] = f64_special[next() % 16];
        snprintf(name, sizeof name, "f64 decimals, %zu", n);
        x = add(name, TSDB_FLOAT64, n);
        for (size_t i = 0; i < n; i++) {
            double d = (double)((int64_t)(next() % 2000000001) - 1000000000) / 1000;
            memcpy((uint64_t *)x->v + i, &d, 8);
            if (next() % 7 == 0)
                ((uint64_t *)x->v)[i] = NULL_F64;
        }
    }
    struct { const char *name; double a, step; } ramps64[] = {
        {"f64 2^53 counter", 9007199254740000.0, 1}, {"f64 1e15 decimals", 1e15, 0.01},
        {"f64 1e300 ramp", 1e300, -1e296},            {"f64 0.1 steps", 0, 0.1},
    };
    for (size_t r = 0; r < sizeof ramps64 / sizeof *ramps64; r++) {
        x = add(ramps64[r].name, TSDB_FLOAT64, GR);
        for (size_t i = 0; i < GR; i++)
            ((double *)x->v)[i] = ramps64[r].a + (double)i * ramps64[r].step;
    }
    x = add("f64 random bits", TSDB_FLOAT64, GR);
    for (size_t i = 0; i < GR; i++)
        ((uint64_t *)x->v)[i] = next();

    /* timestamps: regular, jittered, random gaps, duplicates, negative, extremes */
    x = add("ts jitter", TSDB_INT64, GR);
    for (size_t i = 0; i < GR; i++)
        x->ts[i] = 1700000000000 + (int64_t)i * 1000 + (int64_t)(next() % 41) - 20;
    x = add("ts random gaps and duplicates", TSDB_INT64, GR);
    for (size_t i = 1; i < GR; i++)
        x->ts[i] = x->ts[i - 1] + (next() % 4 ? (int64_t)(next() % 100000) : 0);
    x = add("ts negative", TSDB_INT64, GR);
    for (size_t i = 0; i < GR; i++)
        x->ts[i] = -86400000000 + (int64_t)i * 10;
    x = add("ts extremes", TSDB_INT64, 5);
    int64_t te[] = {INT64_MIN + 1, -1, 0, 1, INT64_MAX};
    memcpy(x->ts, te, sizeof te);
}

/* ---- one configuration: everything into r ---- */
typedef size_t (*enc_fn)(const void *, size_t, uint8_t *);
typedef int (*dec_fn)(const uint8_t *, size_t, size_t, void *);

static uint8_t *buf, *dec;
#define BUF(n) ((n) * 16 + 4096)

/* a transform codec alone: its bytes, and its decoding (which must give back the input) */
static void codec(rec *r, const input *x, const char *what, enc_fn enc, dec_fn dc, const void *v, size_t w)
{
    memset(buf, 0, BUF(x->n));
    size_t len = enc(v, x->n, buf);
    put(r, what, x->name, buf, len);
    if (!len) /* not applicable to these values */
        return;
    memset(dec, 0xa5, x->n * w);
    int rc = dc(buf, len, x->n, dec);
    put(r, what, x->name, dec, x->n * w);
    if (rc != 0 || memcmp(dec, v, x->n * w) != 0) {
        fprintf(stderr, "%s / %s: does not round-trip\n", x->name, what);
        failures++;
    }
}

#define CODEC(name, src, w) codec(r, x, #name, (enc_fn)name##_encode, (dec_fn)name##_decode, src, w)

static void run(rec *r, codec_ctx *c)
{
    for (int k = 0; k < nin; k++) {
        const input *x = &in[k];
        size_t w = type_width(x->type);
        char what[64];
        /* the block layer, as parts store values and timestamps, with and without ZSTD */
        for (int level = 0; level <= 3; level += 3) {
            int ncodecs = x->type == TSDB_FLOAT32 ? 3 : 1;
            for (int vc = 0; vc < ncodecs; vc++) {
                memset(buf, 0, BUF(x->n));
                size_t len = block_encode_vals(c, level, vc, x->type, x->v, x->n, buf);
                snprintf(what, sizeof what, "block zstd %d codec %d", level, vc);
                put(r, what, x->name, buf, len);
                CHECK(len > 0);
                memset(dec, 0xa5, x->n * w);
                CHECK(block_decode_vals(c, x->type, buf, len, x->n, dec) == 0);
                CHECK(memcmp(dec, x->v, x->n * w) == 0);
                put(r, what, x->name, dec, x->n * w);
            }
            memset(buf, 0, BUF(x->n));
            size_t len = block_encode_ts(c, level, x->ts, x->n, buf);
            snprintf(what, sizeof what, "ts block zstd %d", level);
            put(r, what, x->name, buf, len);
            CHECK(block_decode_ts(c, buf, len, x->n, (int64_t *)dec) == 0);
            CHECK(memcmp(dec, x->ts, x->n * 8) == 0);
        }
        /* each transform on its own */
        CODEC(dd, x->ts, 8);
        CODEC(ts_grid, x->ts, 8);
        CODEC(ts_delta, x->ts, 8);
        switch (x->type) {
        case TSDB_FLOAT32:
            CODEC(gorilla, x->v, 4);
            CODEC(chimp, x->v, 4);
            CODEC(chimp128, x->v, 4);
            CODEC(patas, x->v, 4);
            CODEC(delta32, x->v, 4);
            CODEC(bytesplit, x->v, 4);
            CODEC(alprd, x->v, 4);
            CODEC(decimal, x->v, 4);
            CODEC(decimal_bp, x->v, 4);
            CODEC(alp, x->v, 4);
            break;
        case TSDB_FLOAT64:
            CODEC(f64_decimal, x->v, 8);
            CODEC(xor64, x->v, 8);
            break;
        case TSDB_INT64:
            CODEC(int_bp, x->v, 8);
            CODEC(dd, x->v, 8);
            break;
        }
        /* aggregates: whole, every short prefix (the kernels' tails), and the statistics parts store */
        for (size_t m = 0; m <= x->n; m = m < 40 ? m + 1 : m == x->n ? m + 1 : x->n) {
            struct {
                agg_state a;
                agg_dev x;
            } a; /* the state and its spread, compared together */
            memset(&a, 0, sizeof a);
            agg_init(&a.a, x->type);
            agg_dev_init(&a.x);
            agg_fold(x->type, x->v, m, &a.a, &a.x);
            snprintf(what, sizeof what, "agg_fold %zu", m);
            put(r, what, x->name, &a, sizeof a);
            memset(&a, 0, sizeof a); /* without the spread: another tail (sum_finish) */
            agg_init(&a.a, x->type);
            agg_fold(x->type, x->v, m, &a.a, NULL);
            snprintf(what, sizeof what, "agg_fold %zu without spread", m);
            put(r, what, x->name, &a, sizeof a);
            granule g;
            memset(&g, 0, sizeof g);
            granule_stats(x->type, x->v, m, &g);
            snprintf(what, sizeof what, "granule_stats %zu", m);
            put(r, what, x->name, &g, sizeof g);
            memset(&a, 0, sizeof a);
            agg_from_granule(x->type, &g, &a.a, &a.x);
            put(r, "agg_from_granule", x->name, &a, sizeof a);
            for (int sp = 0; m && g.nulls == 0 && sp < 2; sp++) { /* queries without and with the spread */
                memset(&a, 0, sizeof a);
                agg_init(&a.a, x->type);
                agg_dev_init(&a.x);
                agg_fold_hint(x->type, x->v, m, false, &a.a, sp ? &a.x : NULL);
                snprintf(what, sizeof what, "agg_fold_hint %zu%s", m, sp ? " spread" : "");
                put(r, what, x->name, &a, sizeof a);
            }
        }
        snprintf(what, sizeof what, "xxh64");
        uint64_t h = xxh64(x->v, x->n * w, 7);
        put(r, what, x->name, &h, sizeof h);
    }
}

static bool same(const rec *a, const rec *b, const char *label)
{
    if (a->n != b->n) {
        fprintf(stderr, "%s: %zu outputs, baseline %zu\n", label, b->n, a->n);
        return false;
    }
    int bad = 0;
    for (size_t i = 0; i < a->n; i++)
        if (a->hash[i] != b->hash[i] && bad++ < 20)
            fprintf(stderr, "%s differs from the baseline: %s\n", label, b->name[i]);
    return bad == 0;
}

#ifdef TSDB_LEVELS
/* The dispatch itself: a level the CPU has must be used, and must call that level's compile. A broken
 * CPU check or table would otherwise ship baseline code everywhere with every comparison still equal. */
uint64_t xxh64_base(const void *data, size_t len, uint64_t seed);
#ifdef TSDB_HAVE_v3
uint64_t xxh64_v3(const void *data, size_t len, uint64_t seed);
#endif
#ifdef TSDB_HAVE_v4
uint64_t xxh64_v4(const void *data, size_t len, uint64_t seed);
#endif

static const void *level_fn(int level)
{
    switch (level) {
#ifdef TSDB_HAVE_v3
    case CPU_V3: return (const void *)xxh64_v3;
#endif
#ifdef TSDB_HAVE_v4
    case CPU_V4: return (const void *)xxh64_v4;
#endif
    default: return (const void *)xxh64_base;
    }
}

static void check_dispatch(void)
{
    int hw = CPU_BASE;
    __builtin_cpu_init();
    if (__builtin_cpu_supports("avx2") && __builtin_cpu_supports("bmi") && __builtin_cpu_supports("bmi2") &&
        __builtin_cpu_supports("fma") && __builtin_cpu_supports("lzcnt") && __builtin_cpu_supports("movbe"))
        hw = CPU_V3;
    if (hw == CPU_V3 && __builtin_cpu_supports("avx512f") && __builtin_cpu_supports("avx512bw") &&
        __builtin_cpu_supports("avx512cd") && __builtin_cpu_supports("avx512dq") &&
        __builtin_cpu_supports("avx512vl"))
        hw = CPU_V4;
#ifndef TSDB_HAVE_v4
    if (hw == CPU_V4)
        hw = CPU_V3;
#endif
#ifndef TSDB_HAVE_v3
    hw = CPU_BASE;
#endif
    const char *env = getenv("TSDB_CPU");
    if (!env || !*env) {
        printf("dispatch: this CPU has %s, the build uses up to %s\n", cpu_level_name(hw),
               cpu_level_name(cpu_level_max()));
        CHECK(cpu_level_max() == hw);
        CHECK(cpu_level() == hw);
    }
    for (int level = CPU_BASE; level <= cpu_level_max(); level++) {
        CHECK(cpu_set_level(level) == 0);
        CHECK(cpu_level() == level);
        CHECK(cpu_level_probe() == level_fn(level));
    }
}
#else
static void check_dispatch(void) {}
#endif

int main(void)
{
    check_dispatch();
    make_inputs();
    buf = malloc(BUF(GR) + BLK_SLACK);
    dec = malloc(GR * 8 + BLK_SLACK);
    static const char *const kernels[] = {"scalar", "avx2", "avx512"};
    rec base = {0};
    int configs = 0;
    for (int level = CPU_BASE; level <= cpu_level_max(); level++) {
        for (int k = 0; k <= level; k++) { /* each kernel the level allows: v2 has scalar only */
            setenv("TSDB_SIMD", kernels[k], 1);
            if (cpu_set_level(level) != 0)
                break;
            if (strcmp(simd_name(), kernels[k]) != 0)
                continue; /* this CPU lacks it */
            codec_ctx *c = codec_new();
            rec r = {0};
            run(&r, c);
            codec_free(c);
            char label[64];
            snprintf(label, sizeof label, "level %s, kernel %s", cpu_level_name(level), simd_name());
            if (!configs++) {
                base = r;
                printf("%s: %zu outputs\n", label, r.n);
                continue;
            }
            bool ok = same(&base, &r, label);
            printf("%s: %s\n", label, ok ? "identical" : "DIFFERENT");
            failures += !ok;
            free(r.name);
            free(r.hash);
        }
    }
    unsetenv("TSDB_SIMD");
    free(base.name);
    free(base.hash);
    if (configs > 1)
        printf("%d inputs, %d configurations compared\n", nin, configs);
    else /* an older CPU, or TSDB_CPU=v2 on a build without levels: still decodes every input exactly */
        printf("%d inputs, one configuration on this CPU: nothing to compare (SKIPPED the comparison)\n", nin);
    if (failures) {
        printf("test_levels: %d failures\n", failures);
        return 1;
    }
    printf("test_levels: all passed\n");
    return 0;
}
