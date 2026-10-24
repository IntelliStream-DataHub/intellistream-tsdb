/*
 * The aggregation folds over decoded values, per type and aggregate, at the
 * CPU level and kernels this process runs with (TSDB_CPU=v2|v3|v4 in a
 * `make dist` build, TSDB_SIMD=scalar|sse|avx|avx2|avx512):
 *
 *   fold-bench [--rows 8192] [--ms 200] [--only SUBSTRING] [--vary]
 *
 * Each case folds one granule's worth of decoded values (--rows, 8192 as the
 * engine writes them, so they sit in L1/L2 as after a decode) over and over
 * for --ms milliseconds and prints millions of rows per second:
 *
 *   agg      count/sum/min/max (agg_fold without the spread)
 *   spread   the same with the spread's second pass (agg_fold with agg_dev; what granule_stats runs on every write)
 *   twa      a granule's step and linear areas (granule_twa, also on every write)
 *   lttb     LTTB's triangle-area argmax over one bucket (lttb_argmax)
 *
 * for float32, float64, int64, bool and state, each without nulls and with
 * 10% nulls. tools/fold-bench.sh runs it at every level interleaved.
 *
 * With --vary, agg, spread and twa cycle through up to 256 different granules
 * (other values, null positions and gaps, about 256 KB in all), so branches on
 * the data are not learned from one granule repeated: short granules otherwise
 * favour branchy code that the predictor has memorised.
 */
#include "../src/internal.h"
#include "common.h"

#include <math.h>
#include <stdlib.h>

static uint64_t rng = 0x9e3779b97f4a7c15ull;
static uint64_t rnd(void)
{
    rng ^= rng << 13;
    rng ^= rng >> 7;
    rng ^= rng << 17;
    return rng;
}

static const char *type_label(int t)
{
    switch (t) {
    case TSDB_FLOAT32: return "float32";
    case TSDB_FLOAT64: return "float64";
    case TSDB_INT64: return "int64";
    case TSDB_BOOL: return "bool";
    default: return "state";
    }
}

/* realistic values: a drifting sensor reading at 0.01, a counter with a large offset, a door, a mode */
static void fill(int type, void *v, size_t n, bool nulls)
{
    double x = 20;
    int64_t c = INT64_C(1) << 40;
    uint32_t b = 0, s = 3;
    for (size_t i = 0; i < n; i++) {
        x += (double)(int64_t)(rnd() % 21 - 10) * 0.01;
        c += (int64_t)(rnd() % 1000);
        if (rnd() % 50 == 0)
            b ^= 1;
        if (rnd() % 100 == 0)
            s = 1 + (uint32_t)(rnd() % 5);
        bool null = nulls && rnd() % 10 == 0;
        switch (type) {
        case TSDB_FLOAT32: {
            float f = (float)x;
            uint32_t u = NULL_F32;
            if (null)
                memcpy(&f, &u, 4);
            ((float *)v)[i] = f;
            break;
        }
        case TSDB_FLOAT64: {
            uint64_t u = NULL_F64;
            if (null)
                memcpy(&((double *)v)[i], &u, 8);
            else
                ((double *)v)[i] = x;
            break;
        }
        case TSDB_INT64: ((int64_t *)v)[i] = null ? NULL_I64 : c; break;
        case TSDB_BOOL: ((uint32_t *)v)[i] = null ? NULL_BOOL : b; break;
        default: ((uint32_t *)v)[i] = null ? NULL_STATE : s; break;
        }
    }
}

static volatile double sink;

/* runs f until ms have passed, returns millions of rows per second */
#define TIME(rows, ms, body)                                                                                  \
    ({                                                                                                        \
        double t0_ = now_s(), t_;                                                                             \
        uint64_t it_ = 0;                                                                                     \
        do {                                                                                                  \
            for (int r_ = 0; r_ < 16; r_++, it_++) {                                                          \
                body;                                                                                         \
            }                                                                                                 \
            t_ = now_s() - t0_;                                                                               \
        } while (t_ * 1000 < (ms));                                                                           \
        (double)it_ * (double)(rows) / t_ / 1e6;                                                              \
    })

int main(int argc, char **argv)
{
    size_t rows = 8192;
    double ms = 200;
    const char *only = NULL;
    bool vary = false;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--rows") && i + 1 < argc)
            rows = (size_t)strtoull(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--ms") && i + 1 < argc)
            ms = atof(argv[++i]);
        else if (!strcmp(argv[i], "--only") && i + 1 < argc)
            only = argv[++i];
        else if (!strcmp(argv[i], "--vary"))
            vary = true;
        else {
            fprintf(stderr, "usage: fold-bench [--rows N] [--ms MS] [--only SUBSTRING] [--vary]\n");
            return 2;
        }
    }
    simd_init();
    printf("# kernels: %s%s\n", tsdb_simd_level(), vary ? ", varied granules" : "");
    static const int types[] = {TSDB_FLOAT32, TSDB_FLOAT64, TSDB_INT64, TSDB_BOOL, TSDB_STATE};
    /* np granules (a power of two) of rows each, one after the other */
    size_t np = 1;
    while (vary && np < 256 && (np * 2) * rows * 16 <= (256u << 10))
        np *= 2;
    void *pool = malloc(np * rows * 8 + 64);
    int64_t *tpool = malloc(np * rows * 8 + 64);
    void *v = pool;
    int64_t *ts = malloc(rows * 8 + 64); /* LTTB's points */
    uint64_t *slots = malloc(rows * 8 + 64);
    for (size_t p = 0; p < np; p++)
        for (size_t i = 0; i < rows; i++) /* varied: gaps of 1 to 5 s, now and then a minute */
            tpool[p * rows + i] = INT64_C(1700000000000) + (int64_t)i * (vary ? 5000 : 1000) +
                                  (int64_t)(rnd() % (vary ? (rnd() % 8 ? 4000 : 60000) : 100));
    for (size_t p = 0; vary && p < np; p++) /* keep each granule strictly increasing */
        for (size_t i = 1; i < rows; i++)
            if (tpool[p * rows + i] <= tpool[p * rows + i - 1])
                tpool[p * rows + i] = tpool[p * rows + i - 1] + 1;
    size_t pi = 0;
    for (size_t t = 0; t < sizeof types / sizeof *types; t++)
        for (int nulls = 0; nulls <= 1; nulls++) {
            int type = types[t];
            size_t w = type_width(type);
            for (size_t p = 0; p < np; p++)
                fill(type, (uint8_t *)pool + p * rows * w, rows, nulls);
            for (int k = 0; k < 4; k++) {
                static const char *const what[] = {"agg", "spread", "twa", "lttb"};
                char name[64];
                snprintf(name, sizeof name, "%s %s%s", what[k], type_label(type), nulls ? " nulls" : "");
                if (only && !strstr(name, only))
                    continue;
                if (k == 2 && type == TSDB_STATE)
                    continue; /* no TWA of states */
                double r = 0;
                if (k == 0 || k == 1) {
                    r = TIME(rows, ms, {
                        agg_state a;
                        agg_dev x;
                        agg_init(&a, type);
                        agg_dev_init(&x);
                        size_t q_ = pi++ & (np - 1);
                        agg_fold(type, (const uint8_t *)pool + q_ * rows * w, rows, &a, k ? &x : NULL);
                        sink += (double)a.count;
                    });
                } else if (k == 2) {
                    r = TIME(rows, ms, {
                        granule g;
                        size_t q_ = pi++ & (np - 1);
                        granule_twa(type, tpool + q_ * rows, (const uint8_t *)pool + q_ * rows * w, rows, &g);
                        sink += g.tw_step;
                    });
                } else {
                    /* LTTB's buffer holds points only (no nulls, NaN or infinities): the slot bits */
                    size_t n = 0;
                    for (size_t i = 0; i < rows; i++) {
                        uint64_t s = 0;
                        memcpy(&s, (const uint8_t *)v + i * w, w);
                        if (w == 4 ? s == (uint32_t)type_null(type) : s == type_null(type))
                            continue;
                        slots[n] = s;
                        ts[n] = INT64_C(1700000000000) + (int64_t)i * 1000;
                        n++;
                    }
                    r = TIME(n, ms, {
                        size_t j = lttb_argmax(type, ts, slots, n, ts[0] - 5000, 0, 20, (double)rows * 1000 + 5000,
                                               21);
                        sink += (double)j;
                    });
                }
                printf("%-24s %10.1f M rows/s\n", name, r);
                fflush(stdout);
            }
        }
    free(pool);
    free(tpool);
    free(ts);
    free(slots);
    return 0;
}
