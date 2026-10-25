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
#include "bitpack.h"
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

static input in[512];
static int nin;

static input *add(const char *name, int type, size_t n)
{
    if (nin == (int)(sizeof in / sizeof *in)) {
        fprintf(stderr, "test_levels: more than %zu inputs\n", sizeof in / sizeof *in);
        exit(1);
    }
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
        snprintf(name, sizeof name, "int64 just below int32, %zu", i64_n[k]); /* squares from all 64 bits */
        x = add(name, TSDB_INT64, i64_n[k]);
        for (size_t i = 0; i < x->n; i++)
            ((int64_t *)x->v)[i] = i == x->n / 2 ? (int64_t)INT32_MIN - 1 - (int64_t)(i & 1) : (int32_t)next();
        snprintf(name, sizeof name, "int64 most negative, %zu", i64_n[k]);
        x = add(name, TSDB_INT64, i64_n[k]);
        for (size_t i = 0; i < x->n; i++)
            ((int64_t *)x->v)[i] = next() % 9 ? INT64_MIN + 1 : NULL_I64;
        snprintf(name, sizeof name, "int64 most positive, %zu", i64_n[k]);
        x = add(name, TSDB_INT64, i64_n[k]);
        for (size_t i = 0; i < x->n; i++)
            ((int64_t *)x->v)[i] = INT64_MAX - (int64_t)(next() % 3);
    }
    /* TWA's cheap int64 conversion (SSE, AVX) takes [-2^51, 2^51): its edges early, one value past each end later */
    for (int k = 0; k < 2; k++) {
        x = add(k ? "int64 around -2^51" : "int64 around 2^51", TSDB_INT64, 1000);
        for (size_t i = 0; i < x->n; i++) {
            static const int64_t e[] = {-(INT64_C(1) << 51), (INT64_C(1) << 51) - 1, -(INT64_C(1) << 51) + 1,
                                        (INT64_C(1) << 51) - 2};
            uint64_t r = next();
            ((int64_t *)x->v)[i] = i == 300  ? (k ? -(INT64_C(1) << 51) - 1 : INT64_C(1) << 51)
                                   : i == 600 ? (k ? INT64_C(1) << 51 : -(INT64_C(1) << 51) - 1)
                                   : r % 7 == 0 ? NULL_I64
                                   : r % 7 == 1 ? e[(r >> 8) % 4]
                                                : (int64_t)(r >> 13) - (INT64_C(1) << 50);
        }
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

    /* short runs: values.c's tiny path (a fixed tree, n <= 12) and the short-run kernels must give the kernels'
     * bits for every prefix of these (short_run): signed zeros, infinities, NaNs and nulls at every position (11
     * rotations of 11 specials: each special at each position), and random draws from them */
    static const uint64_t sp64[11] = {0, 0x8000000000000000ull, 0x7ff0000000000000ull, 0xfff0000000000000ull,
                                      0x7ff8000000000000ull, 0xfff8000000000001ull, NULL_F64, 0x3ff0000000000000ull,
                                      0xbff0000000000000ull, 0x7fefffffffffffffull, 0xffefffffffffffffull};
    static const uint32_t sp32[11] = {0, 0x80000000u, 0x7f800000u, 0xff800000u, 0x7fc00000u, 0xffc00001u,
                                      NULL_F32, 0x3f800000u, 0xbf800000u, 0x7f7fffffu, 0xff7fffffu};
    for (int t = 0; t < 2; t++)
        for (int r = 0; r < 15; r++) {
            snprintf(name, sizeof name, "%s short specials %d", t ? "f32" : "f64", r);
            x = add(name, t ? TSDB_FLOAT32 : TSDB_FLOAT64, 34);
            for (size_t i = 0; i < 34; i++) {
                /* 0-10: rotations; 11, 12: zeros of both signs and nulls; 13, 14: anything */
                size_t k = r < 11 ? (i + (size_t)r) % 11 : r < 13 ? next() % 3 == 2 ? 6 : next() % 2 : next() % 11;
                if (t)
                    ((uint32_t *)x->v)[i] = sp32[k];
                else
                    ((uint64_t *)x->v)[i] = sp64[k];
            }
        }

    /* ties of zeros: which lane min/max keep is the tree's priority order (the bit-reversed lane order); input t has
     * nulls in the first t lanes of that order, -0.0 in lane t of it and +0.0 elsewhere, so -0.0 must win both
     * min and max, and a tiny path that visits any two lanes out of order gets +0.0 for some t */
    static const int prio16[16] = {0, 8, 4, 12, 2, 10, 6, 14, 1, 9, 5, 13, 3, 11, 7, 15};
    _Static_assert(SIMD_LANES_F64 == 16, "the order above is for 16 lanes");
    for (int t = 0; t < 16; t++) {
        snprintf(name, sizeof name, "f64 zero ties %d", t);
        x = add(name, TSDB_FLOAT64, 34);
        for (size_t i = 0; i < 34; i++)
            ((uint64_t *)x->v)[i] = 0;
        for (int k = 0; k < t; k++)
            ((uint64_t *)x->v)[prio16[k]] = NULL_F64;
        ((uint64_t *)x->v)[prio16[t]] = 0x8000000000000000ull;
    }
    /* float32 (simd_small_f32): a run of up to 16 uses lanes 0-15 of the 32, in the same order */
    for (int t = 0; t < 16; t++) {
        snprintf(name, sizeof name, "f32 zero ties %d", t);
        x = add(name, TSDB_FLOAT32, 34);
        for (size_t i = 0; i < 34; i++)
            ((uint32_t *)x->v)[i] = 0;
        for (int k = 0; k < t; k++)
            ((uint32_t *)x->v)[prio16[k]] = NULL_F32;
        ((uint32_t *)x->v)[prio16[t]] = 0x80000000u;
    }

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

/* A run shorter than simd_short (simd_short_f64 for float64) takes values.c's tiny path (float64, n <= 12) or simd_small_*: it must give the
 * bits of the lane kernels of the level, which it skips, with and without the spread and the search for nulls,
 * from a new state, from states whose bounds and sum are zeros of either sign, and from one whose bounds are no
 * float32 values (the float32 kernels start their lanes at the bounds rounded to float: here zeros of either sign). */
static void short_run(const input *x, size_t m)
{
    for (int st = 0; st < 4; st++)
    for (int sp = 0; sp < 2; sp++)
        for (int hint = 0; hint < 2; hint++) {
            size_t nulls = 0;
            for (size_t i = 0; i < m; i++)
                nulls += x->type == TSDB_FLOAT32 ? ((const uint32_t *)x->v)[i] == NULL_F32
                                                 : ((const uint64_t *)x->v)[i] == NULL_F64;
            if (hint && nulls)
                continue; /* "no nulls" would be a lie */
            struct {
                agg_state a;
                agg_dev x;
            } a, b;
            memset(&a, 0, sizeof a);
            memset(&b, 0, sizeof b);
            agg_init(&a.a, x->type);
            agg_dev_init(&a.x);
            if (st) { /* a state with zeros of either sign as bounds and sum: which operand wins on ties */
                a.a.count = 3;
                a.a.sum = st == 1 ? -0.0 : 0.0;
                a.a.min = st == 1 ? 0.0 : -0.0;
                a.a.max = st == 1 ? -0.0 : 0.0;
                a.x.m2 = 1;
            }
            if (st == 3) {
                a.a.min = 1e-300;
                a.a.max = -1e-300;
            }
            b = a;
            agg_fold_hint(x->type, x->v, m, !hint, &a.a, sp ? &a.x : NULL);
            b.a.rows += m;
            if (x->type == TSDB_FLOAT32)
                simd_fold_f32(x->v, m, nulls, &b.a, sp ? &b.x : NULL);
            else
                simd_fold_f64(x->v, m, nulls, &b.a, sp ? &b.x : NULL);
            if (memcmp(&a, &b, sizeof a) != 0) {
                fprintf(stderr, "%s, %zu values%s%s, state %d: the short path differs from the kernel %s\n", x->name,
                        m, sp ? ", spread" : "", hint ? ", no nulls" : "", st, simd_name());
                failures++;
            }
        }
}

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

/* ---- the running sums of the bit-packed decoders, against plain sequential loops ----
 * decimal_bp and int_bp sum zigzag deltas four lanes at a time, relative to the vector's start, where the
 * widths and the starting value allow it (bitpack.h, zz_prefix32). Crafted blocks put every width and the
 * extremes of the widest deltas it takes (and the next width) at vectors of every length, from starting
 * values at and beyond 2^53, where a double stops being exact: the output must equal these references. */
static int ref_dbp_decode(const uint8_t *in, size_t len, size_t n, float *out)
{
    uint64_t k;
    memcpy(&k, in + 2, 8);
    static const double inv[] = {1e0, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9, 1e-10};
    const double sc = in[0] <= 10 ? inv[in[0]] : 1.0 / (double)(1L << (in[0] - 10));
    const uint8_t *p = in + 14, *end = in + len;
    uint32_t z[1024];
    out[0] = (float)((double)(int64_t)k * sc);
    for (size_t v0 = 0; v0 + 1 < n; v0 += 1024) {
        size_t nv = MIN((size_t)1024, n - 1 - v0);
        int w = *p++;
        unpack(p, nv, w, z);
        p += packed_bytes(nv, w);
        for (size_t i = 0; i < nv; i++) {
            k += (uint64_t)((int64_t)(z[i] >> 1) ^ -(int64_t)(z[i] & 1));
            out[v0 + 1 + i] = (float)((double)(int64_t)k * sc);
        }
    }
    return p == end ? 0 : -1;
}

static int ref_ibp_decode(const uint8_t *in, size_t len, size_t n, int64_t *out)
{
    uint64_t k;
    memcpy(&k, in, 8);
    const uint8_t *p = in + 8, *end = in + len;
    uint32_t z[1024];
    out[0] = (int64_t)k;
    for (size_t v0 = 1; v0 < n; v0 += 1024) {
        size_t nv = MIN((size_t)1024, n - v0);
        int w = *p++;
        unpack(p, nv, w, z);
        p += packed_bytes(nv, w);
        for (size_t i = 0; i < nv; i++) {
            k += (uint64_t)((int64_t)(z[i] >> 1) ^ -(int64_t)(z[i] & 1));
            out[v0 + i] = (int64_t)k;
        }
    }
    return p == end ? 0 : -1;
}

static void crafted_sums(rec *r)
{
    static const size_t lens[] = {2, 3, 4, 5, 6, 7, 8, 9, 1024, 1025, 1026, 1027, 1028, 1029, 2049, 2050, 3075, 8192};
    const int64_t P53 = 9007199254740992ll;
    const int64_t firsts[] = {0, 1, -1, 123456789, P53 - 1, P53, P53 + 1, P53 + 3, -P53 + 1, -P53, -P53 - 1, -P53 - 3,
                              INT64_MAX, INT64_MIN, (int64_t)1 << 62};
    enum { NF = sizeof firsts / sizeof *firsts };
    uint64_t s = 99;
    uint32_t *z = malloc(1024 * sizeof *z);
    uint8_t *blk = malloc(14 + 9 * (1024 * 4 + 8) + BLK_SLACK);
    for (int c = 0; c < 600; c++) {
        size_t n = lens[c % (sizeof lens / sizeof *lens)];
        int64_t first = c % 4 == 3 ? (int64_t)(s = ds_mix(s + c)) : firsts[(c / 4) % NF];
        bool dbp = c & 1;
        size_t hdr = dbp ? 14 : 8;
        memset(blk, 0, 14);
        if (dbp) {
            blk[0] = (uint8_t)(c / 2 % 35);
            memcpy(blk + 2, &first, 8);
        } else {
            memcpy(blk, &first, 8);
        }
        uint8_t *o = blk + hdr;
        for (size_t v0 = 0; v0 + 1 < n; v0 += 1024) {
            size_t nv = MIN((size_t)1024, n - 1 - v0);
            s = ds_mix(s + v0 + 1);
            int mode = (int)(s % 5);
            /* widths around the widest the relative sums take, or any; deltas the most negative (odd zigzags),
             * the most positive (even), or random */
            int w = mode < 3 ? ZZ_PREFIX32_MAX_W - 1 + (int)(s >> 8) % 3 : (int)((s >> 8) % 33);
            uint32_t top = w == 32 ? UINT32_MAX : (uint32_t)((1ull << w) - 1);
            for (size_t i = 0; i < nv; i++) {
                s = ds_mix(s + i);
                z[i] = mode == 0 ? top : mode == 1 ? top & ~1u : (uint32_t)s & top;
            }
            *o++ = (uint8_t)w;
            pack(z, nv, w, o);
            o += packed_bytes(nv, w);
        }
        size_t len = (size_t)(o - blk);
        memset(o, 0x5a, BLK_SLACK);
        memset(dec, 0xa5, n * 8);
        memset(dec + n * 8, 0xa5, n * 8);
        int rc = dbp ? decimal_bp_decode(blk, len, n, (float *)dec) : int_bp_decode(blk, len, n, (int64_t *)dec);
        int rc2 = dbp ? ref_dbp_decode(blk, len, n, (float *)(dec + n * 8))
                      : ref_ibp_decode(blk, len, n, (int64_t *)(dec + n * 8));
        size_t w = dbp ? 4 : 8;
        char what[64];
        snprintf(what, sizeof what, "crafted %s %d", dbp ? "decimal_bp" : "int_bp", c);
        put(r, what, "running sums", dec, n * w);
        if (rc != 0 || rc2 != 0 || memcmp(dec, dec + n * 8, n * w) != 0) {
            fprintf(stderr, "%s (n %zu, first %lld): differs from the sequential sum\n", what, n, (long long)first);
            failures++;
        }
    }
    free(z);
    free(blk);
}

/* The lengths of the prefixes folded and searched: every one up to SHORT_LENS (the short runs, the kernels' tails,
 * most of the sets' thresholds), the longer thresholds on both sides (check_thresholds), then all of it. */
#define SHORT_LENS 66
static const size_t long_lens[] = {127, 128, 255, 256, TWA_SEQ_MAX, TWA_SEQ_MAX + 1};
_Static_assert(TWA_SEQ_MAX > 256, "long_lens ascending");
static size_t next_len(size_t m, size_t n)
{
    if (m < SHORT_LENS)
        return m + 1;
    for (size_t i = 0; i < sizeof long_lens / sizeof *long_lens; i++)
        if (long_lens[i] > m && long_lens[i] < n)
            return long_lens[i];
    return m < n ? n : n + 1;
}

/* main's granule_twa: the segments one after another (twa.c below TWA_SEQ_MAX points), NaN canonical */
static void twa_ref(int type, const int64_t *ts, const void *v, size_t n, twa_sums *o)
{
    size_t w = type_width(type);
    uint64_t nul = type_null(type);
    double step = 0, lin = 0, vp = 0;
    uint64_t sms = 0, lms = 0, gap = 0;
    bool okp = false;
    for (size_t i = 0; i < n; i++) {
        uint64_t b = 0;
        memcpy(&b, (const uint8_t *)v + i * w, w);
        bool ok = w == 4 ? (uint32_t)b != (uint32_t)nul : b != nul;
        double x = 0;
        if (ok) {
            float f;
            double d;
            memcpy(&f, &b, 4);
            memcpy(&d, &b, 8);
            x = type == TSDB_FLOAT32 ? f : type == TSDB_FLOAT64 ? d : type == TSDB_INT64 ? (double)(int64_t)b
                                                                                         : (double)(uint32_t)b;
        }
        if (i) {
            uint64_t dt = (uint64_t)ts[i] - (uint64_t)ts[i - 1];
            gap = dt > gap ? dt : gap;
            if (okp) {
                step += vp * (double)dt;
                sms += dt;
                if (ok) {
                    lin += (vp + x) * 0.5 * (double)dt;
                    lms += dt;
                }
            }
        }
        vp = x;
        okp = ok;
    }
    *o = (twa_sums){.step = canon_nan(step), .lin = canon_nan(lin), .sms = sms, .lms = lms, .gap = gap};
}

static void run(rec *r, codec_ctx *c)
{
    crafted_sums(r);
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
        /* aggregates: whole, every short prefix (the kernels' tails, the short runs), and the statistics parts
         * store */
        for (size_t m = 0; m <= x->n; m = next_len(m, x->n)) {
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
            if ((x->type == TSDB_FLOAT32 && m < simd_short) || (x->type == TSDB_FLOAT64 && m < simd_short_f64))
                short_run(x, m);
            for (int sp = 0; m && g.nulls == 0 && sp < 2; sp++) { /* queries without and with the spread */
                memset(&a, 0, sizeof a);
                agg_init(&a.a, x->type);
                agg_dev_init(&a.x);
                agg_fold_hint(x->type, x->v, m, false, &a.a, sp ? &a.x : NULL);
                snprintf(what, sizeof what, "agg_fold_hint %zu%s", m, sp ? " spread" : "");
                put(r, what, x->name, &a, sizeof a);
            }
        }
        /* TWA areas and LTTB picks (simd.c): gaps beyond 2^55 (dt rounds when converted), large time
         * offsets, every short prefix (the lanes' tails), equal areas (cy == ay: the area follows y alone) */
        /* Then a timeline across all of int64 (TSDB_TS_MIN is INT64_MIN): from near INT64_MIN, one jump of
         * 2^63 + 2^62 past 0, so dt and ts - t0 reach 2^63 and more and only an unsigned conversion is right. */
        int64_t *ts = malloc((x->n + 1) * sizeof *ts);
        uint64_t *sl = malloc((x->n + 1) * sizeof *sl);
        /* and one with every gap below 2^52, where the SSE and AVX TWA kernels keep their cheap conversion, and
         * one with gaps around 2^52 (2^52 - 1 early, so the shorter prefixes keep the cheap conversion) */
        static const char *const tl[] = {"", " wide", " narrow", " 2^52"};
        static const uint64_t edge[] = {(1ull << 52) - 1, 1ull << 52, (1ull << 52) + 1, (1ull << 53) - 1};
        for (int wide = 0; wide < 4; wide++) {
            uint64_t z = 0x9e3779b97f4a7c15u * (uint64_t)(x->n + 1);
            uint64_t t = wide == 1 ? (uint64_t)INT64_MIN + 10000 : wide == 3 ? 0 : 1ull << 62;
            for (size_t i = 0; i < x->n; i++) {
                z = z * 6364136223846793005u + 1442695040888963407u;
                uint64_t r = z >> 11;
                if (wide == 3)
                    t += i % 61 == 7 ? edge[i / 61 % 4] : r % 997;
                else if (wide == 2)
                    t += r % 997;
                else if (wide)
                    t += i == x->n / 2 ? (1ull << 63) + (1ull << 62) : 1 + r % 997;
                else
                    t += 1 + (r % 61 == 0 ? (r << 3) | (1ull << 55) | 1 : r % 997);
                ts[i] = (int64_t)t;
                sl[i] = w == 4 ? ((const uint32_t *)x->v)[i] : ((const uint64_t *)x->v)[i];
            }
            for (size_t m = 0; m <= x->n; m = next_len(m, x->n)) {
                granule g;
                memset(&g, 0, sizeof g);
                granule_twa(x->type, ts, x->v, m, &g);
                snprintf(what, sizeof what, "granule_twa %zu%s", m, tl[wide]);
                put(r, what, x->name, &g, sizeof g);
                if (!m)
                    continue;
                if (x->type != TSDB_STATE && m >= 2) {
                    /* the kernel at every length (the same bits in every set), though granule_twa takes it only
                     * beyond TWA_SEQ_MAX points: there granule_twa has its bits, below it the sequential loop's */
                    twa_sums o, q;
                    memset(&o, 0, sizeof o);
                    simd_twa(x->type, ts, x->v, m, &o);
                    snprintf(what, sizeof what, "simd_twa %zu%s", m, tl[wide]);
                    put(r, what, x->name, &o, sizeof o);
                    if (m <= TWA_SEQ_MAX)
                        twa_ref(x->type, ts, x->v, m, &q);
                    else
                        q = o;
                    uint32_t gap = q.gap > UINT32_MAX ? UINT32_MAX : (uint32_t)q.gap;
                    if (memcmp(&g.tw_step, &q.step, 8) || memcmp(&g.tw_lin, &q.lin, 8) ||
                        g.tw_step_ms != q.sms || g.tw_lin_ms != q.lms || g.tw_gap != gap) {
                        fprintf(stderr, "%s, %zu points%s: granule_twa differs from the %s %s\n", x->name, m,
                                tl[wide], m <= TWA_SEQ_MAX ? "sequential loop" : "kernel", simd_name());
                        failures++;
                    }
                }
                size_t k[2] = {
                    lttb_argmax(x->type, ts, sl, m, ts[0] - 5000, 0, 1.5, (double)(t - (uint64_t)ts[0]), -3),
                    lttb_argmax(x->type, ts, sl, m, ts[0], -7, 1, 1e6, 1)};
                snprintf(what, sizeof what, "lttb_argmax %zu%s", m, tl[wide]);
                put(r, what, x->name, k, sizeof k);
            }
        }
        free(ts);
        free(sl);
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

/* each threshold of the selected kernel set is tried on both sides */
static void check_thresholds(void)
{
    /* TWA_SEQ_MAX is the same for every set; SIZE_MAX means the set never takes that kernel (its loop is faster) */
    const size_t t[] = {simd_i64_min, simd_i64_dev_min, simd_u32_min, simd_lttb_small, simd_short, simd_short_f64,
                        TWA_SEQ_MAX + 1};
    for (size_t i = 0; i < sizeof t / sizeof *t; i++) {
        if (t[i] == SIZE_MAX)
            continue;
        bool lo = t[i] <= SHORT_LENS, hi = t[i] <= SHORT_LENS;
        for (size_t j = 0; j < sizeof long_lens / sizeof *long_lens; j++) {
            lo |= long_lens[j] == t[i] - 1;
            hi |= long_lens[j] == t[i];
        }
        if (!lo || !hi) {
            fprintf(stderr, "kernel %s: threshold %zu (#%zu) is not tested on both sides\n", simd_name(), t[i], i);
            failures++;
        }
    }
}

int main(void)
{
    check_dispatch();
    make_inputs();
    buf = malloc(BUF(GR) + BLK_SLACK);
    dec = malloc(GR * 16 + BLK_SLACK);
    static const char *const kernels[] = {"scalar", "sse", "avx", "avx2", "avx512"};
    static const int kmin[] = {CPU_BASE, CPU_BASE, CPU_BASE, CPU_V3, CPU_V4}; /* the lowest level of each */
    rec base = {0};
    int configs = 0;
    for (int level = CPU_BASE; level <= cpu_level_max(); level++) {
        for (int k = 0; k < 5 && kmin[k] <= level; k++) { /* each kernel the level allows */
            setenv("TSDB_SIMD", kernels[k], 1);
            if (cpu_set_level(level) != 0)
                break;
            if (strcmp(simd_name(), kernels[k]) != 0)
                continue; /* this CPU lacks it */
            check_thresholds();
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
