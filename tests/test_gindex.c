/*
 * The packed granule index (src/gindex.c): every entry decodes to exactly the bits it was written with.
 *
 * Directly: gix_encode of entries built from rows of every value type the way the part writer builds them
 * (granule_stats and granule_twa over each granule's rows), over the edge cases: nulls, NaN payloads, infinities,
 * -0.0, all-equal values, one-row granules, ranges across all of int64, timestamps before 1970, many series per
 * part, jittered timestamps; and of entries whose fields are random bit patterns of every width. Each index is
 * parsed from a buffer of its own exact size plus the 8 bytes a reader may load past it (ASan sees any read
 * beyond), and every entry is loaded whole (gx_load GW_ALL, compared with memcmp) and by field group (each
 * group's fields compared), with the accessors (gx_count, gx_ts_min/max, gx_series, gix_run).
 *
 * Through the engine: a database per value type and layout (64-row and 1-row granules in day partitions, and
 * partitions of 2^61 ms, so granules span most of the time axis), written straight to parts and then optimized
 * (the merge writes the index again); every entry of every part is compared with the statistics recomputed from
 * its decoded rows, and its location and checksum with the blocks it points at.
 *
 *   test_gindex [scratch dir]
 */
#include "internal.h"
#include "tsdb.h"

#include <dirent.h>
#include <fcntl.h>
#include <float.h>
#include <ftw.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int failures;
#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                              \
        }                                                                            \
    } while (0)
#define OK(expr)                                                                        \
    do {                                                                                \
        int rc_ = (expr);                                                               \
        if (rc_ != 0) {                                                                 \
            fprintf(stderr, "%s:%d: %s returned %d\n", __FILE__, __LINE__, #expr, rc_); \
            exit(1);                                                                    \
        }                                                                               \
    } while (0)

static uint64_t rs = 0x2545f4914f6cdd1dull;
static uint64_t rnd(void)
{
    rs ^= rs << 13;
    rs ^= rs >> 7;
    rs ^= rs << 17;
    return rs;
}

static const char *const tname[] = {"float32", "float64", "int64", "bool", "state"};
static const int types[] = {TSDB_FLOAT32, TSDB_FLOAT64, TSDB_INT64, TSDB_BOOL, TSDB_STATE};
#define NTYPES 5

/* ------------------------------------------------------------------ the rows */

enum { S_SENSOR, S_NULLS, S_SPECIAL, S_EQUAL, S_HUGE, S_PRE1970, S_JITTER, S_ALLNULL, S_N };
static const char *const sname[] = {"sensor", "nulls", "special", "equal", "huge", "pre1970", "jitter", "allnull"};

static void put_slot(int type, void *v, size_t i, uint64_t bits)
{
    if (type_width(type) == 4)
        ((uint32_t *)v)[i] = (uint32_t)bits;
    else
        ((uint64_t *)v)[i] = bits;
}

static uint64_t f32b(float f)
{
    uint32_t u;
    memcpy(&u, &f, 4);
    return u;
}
static uint64_t f64b(double d)
{
    uint64_t u;
    memcpy(&u, &d, 8);
    return u;
}

/* n rows of scenario s for type: strictly increasing ts, values as slots of the type's width */
static void gen_rows(int type, int s, size_t n, int64_t *ts, void *v)
{
    int64_t t = 1700000000000;
    uint64_t step = 1000;
    if (s == S_HUGE) {
        t = INT64_MIN + 1;
        step = (uint64_t)(INT64_MAX / (int64_t)(n + 1)) * 2 - 3; /* across the whole axis */
    } else if (s == S_PRE1970) {
        t = -2208988800000; /* 1900 */
        step = 61000;
    }
    for (size_t i = 0; i < n; i++) {
        if (s == S_JITTER)
            t += 1 + (int64_t)(rnd() % 1999);
        else if (s == S_PRE1970)
            t += (int64_t)step + (int64_t)(rnd() % 997) - 498;
        else if (i)
            t = (int64_t)((uint64_t)t + step);
        ts[i] = t;
    }
    for (size_t i = 0; i < n; i++) {
        uint64_t b = 0;
        double x = 20 + 5 * sin((double)i / 50) + (double)(rnd() % 100) / 100;
        switch (type) {
        case TSDB_FLOAT32: {
            static const uint32_t sp[] = {0x7fc00000u, 0xffc00001u, 0x7f800000u, 0xff800000u, 0x80000000u, 0,
                                          0x7f7fffffu, 0x00000001u, 0xff7fffffu, 0x7fa00000u, 0x3f800000u};
            b = s == S_SPECIAL ? sp[rnd() % 11] : s == S_EQUAL ? f32b(21.5f)
                : s == S_HUGE  ? f32b((float)((rnd() & 1 ? 1 : -1) * ldexp(1.0, (int)(rnd() % 250) - 125)))
                               : f32b((float)(round(x * 10) / 10));
            break;
        }
        case TSDB_FLOAT64: {
            static const uint64_t sp[] = {0x7ff8000000000000ull, 0xfff8000000000001ull, 0x7ff0000000000000ull,
                                          0xfff0000000000000ull, 0x8000000000000000ull, 0, 0x7fefffffffffffffull,
                                          1, 0xffefffffffffffffull, 0x7ff4000000000000ull, 0x3ff0000000000000ull};
            b = s == S_SPECIAL ? sp[rnd() % 11] : s == S_EQUAL ? f64b(-0.25)
                : s == S_HUGE  ? f64b((rnd() & 1 ? 1 : -1) * ldexp(1.0, (int)(rnd() % 2000) - 1000))
                               : f64b(x);
            break;
        }
        case TSDB_INT64: {
            static const int64_t sp[] = {INT64_MAX, INT64_MIN + 1, 0, -1, 1, INT64_MAX - 1, -(INT64_C(1) << 53)};
            b = (uint64_t)(s == S_SPECIAL ? sp[rnd() % 7] : s == S_EQUAL ? INT64_C(-7)
                           : s == S_HUGE  ? (int64_t)rnd() | 1 /* odd: never the null */
                                          : (int64_t)(x * 1000));
            break;
        }
        case TSDB_BOOL:
            b = s == S_EQUAL ? 1 : s == S_SPECIAL ? rnd() % 2 : i / 7 % 2;
            break;
        default: /* state: 0 is the null */
            b = s == S_EQUAL ? 3 : s == S_SPECIAL || s == S_HUGE ? 1 + rnd() % UINT32_MAX : 1 + i / 13 % 5;
            break;
        }
        if (s == S_ALLNULL || (s == S_NULLS && (i % 3 == 1 || (i / 40) % 4 == 2)))
            b = type_null(type);
        put_slot(type, v, i, b);
    }
}

/* entries over rows [0, n) in granules of gr rows; the series changes every `per` granules (0: never), by a
 * gap of sgap. Locations and checksums are made up (the index does not care what they point at). */
static size_t build(int type, const int64_t *ts, const void *v, size_t n, size_t gr, size_t per, uint32_t sgap,
                    granule *g)
{
    size_t w = type_width(type), k = 0;
    uint32_t series = (uint32_t)(rnd() % 1000);
    uint64_t off = sizeof(part_header);
    for (size_t a = 0; a < n; a += gr, k++) {
        size_t m = MIN(gr, n - a);
        granule *e = &g[k];
        memset(e, 0, sizeof *e);
        granule_stats(type, (const uint8_t *)v + a * w, m, e);
        granule_twa(type, ts + a, (const uint8_t *)v + a * w, m, e);
        if (per && k && k % per == 0)
            series += sgap;
        e->series = series;
        e->count = (uint32_t)m;
        e->ts_min = ts[a];
        e->ts_max = ts[a + m - 1];
        e->off = off;
        e->ts_len = (uint32_t)(4 + rnd() % (2 * m + 40));
        e->val_len = (uint32_t)(4 + rnd() % (8 * m + 40));
        e->crc = (uint32_t)rnd();
        off += e->ts_len + e->val_len;
    }
    return k;
}

/* ------------------------------------------------------------------ the round trip */

#define SAME(a, b) (memcmp(&(a), &(b), sizeof(a)) == 0)

/* the fields of group `want` in d equal those of g (the fields gx_load documents for the group) */
static bool group_same(unsigned want, const granule *d, const granule *g)
{
    bool ok = d->count == g->count;
    bool in = want & GW_INT;
    if (want & (GW_TIME | GW_TWEND))
        ok &= d->ts_min == g->ts_min && d->ts_max == g->ts_max;
    if (want & GW_LOC)
        ok &= d->off == g->off && d->ts_len == g->ts_len && d->val_len == g->val_len && d->crc == g->crc &&
              d->nulls == g->nulls;
    if (want & GW_STAT)
        ok &= d->nulls == g->nulls && d->imin == g->imin && d->imax == g->imax && d->isum_lo == g->isum_lo &&
              d->isum_hi == (in ? g->isum_hi : 0);
    if (want & GW_SPREAD)
        ok &= d->isq0 == g->isq0 && d->isq1 == (in ? g->isq1 : 0) && d->isq2 == (in ? g->isq2 : 0);
    if (want & GW_TWEND)
        ok &= SAME(d->tw_first, g->tw_first) && SAME(d->tw_last, g->tw_last) && d->tw_flags == g->tw_flags;
    if (want & GW_TWSTEP)
        ok &= d->nulls == g->nulls && SAME(d->tw_step, g->tw_step) && d->tw_step_ms == g->tw_step_ms;
    if (want & GW_TWLIN)
        ok &= d->nulls == g->nulls && SAME(d->tw_lin, g->tw_lin) && d->tw_lin_ms == g->tw_lin_ms;
    if (want & GW_SERIES)
        ok &= d->series == g->series;
    return ok;
}

static const unsigned groups[] = {GW_TIME, GW_LOC, GW_STAT, GW_STAT | GW_INT, GW_SPREAD, GW_SPREAD | GW_INT,
                                  GW_TWEND, GW_TWSTEP, GW_TWLIN, GW_TWSTEP | GW_TWLIN, GW_TWSTEP | GW_STAT,
                                  GW_TWLIN | GW_TWEND, GW_TW, GW_SERIES, GW_ALL};

static size_t total_idx, total_bytes, total_entries, total_runs;

/* encodes g[0..n), parses and loads every entry back; the index's length in *ilen */
static void roundtrip(const granule *g, size_t n, const char *label, size_t *ilen)
{
    uint8_t *enc;
    size_t len;
    int rc = gix_encode(g, n, &enc, &len);
    if (rc != 0) {
        fprintf(stderr, "%s: gix_encode returned %d\n", label, rc);
        failures++;
        return;
    }
    CHECK(len % 8 == 0);
    if (ilen)
        *ilen = len;
    total_idx++;
    total_bytes += len;
    total_entries += n;
    uint8_t *b = malloc(len + 8); /* exactly what a reader may touch: the index and the 8 bytes after it */
    memcpy(b, enc, len);
    memset(b + len, 0xee, 8);
    free(enc);
    part pp;
    memset(&pp, 0, sizeof pp);
    if (gix_parse(b, len, (uint32_t)n, &pp.ix) != 0) {
        fprintf(stderr, "%s: gix_parse refused what gix_encode wrote\n", label);
        failures++;
        free(b);
        return;
    }
    pp.ng = (uint32_t)n;
    total_runs += pp.ix.nruns;
    int bad = 0;
    for (size_t i = 0; i < n && bad < 3; i++) {
        granule d;
        memset(&d, 0, sizeof d);
        gx_load(&pp, (uint32_t)i, GW_ALL, &d);
        if (memcmp(&d, &g[i], sizeof d) != 0) {
            fprintf(stderr, "%s: entry %zu of %zu does not decode to what was written\n", label, i, n);
            bad++;
        }
        for (size_t k = 0; k < sizeof groups / sizeof *groups; k++) {
            memset(&d, 0xa5, sizeof d);
            gx_load(&pp, (uint32_t)i, groups[k], &d);
            if (!group_same(groups[k], &d, &g[i])) {
                fprintf(stderr, "%s: entry %zu, group %#x differs\n", label, i, groups[k]);
                bad++;
            }
        }
        int64_t lo, hi;
        gx_ts(&pp, (uint32_t)i, &lo, &hi);
        if (gx_count(&pp, (uint32_t)i) != g[i].count || gx_ts_min(&pp, (uint32_t)i) != g[i].ts_min ||
            gx_ts_max(&pp, (uint32_t)i) != g[i].ts_max || lo != g[i].ts_min || hi != g[i].ts_max ||
            gx_series(&pp, (uint32_t)i) != g[i].series) {
            fprintf(stderr, "%s: entry %zu: an accessor differs\n", label, i);
            bad++;
        }
        if (i == 0 || g[i].series != g[i - 1].series) { /* the run of this series, and of the ids around it */
            uint32_t a, e;
            gix_run(&pp.ix, g[i].series, &a, &e);
            size_t end = i;
            while (end < n && g[end].series == g[i].series)
                end++;
            if (a != i || e != end) {
                fprintf(stderr, "%s: the run of series %u is [%u, %u), not [%zu, %zu)\n", label, g[i].series, a, e,
                        i, end);
                bad++;
            }
            if (g[i].series > 0 && (i == 0 || g[i - 1].series != g[i].series - 1)) {
                gix_run(&pp.ix, g[i].series - 1, &a, &e);
                if (a != i || e != i) {
                    fprintf(stderr, "%s: series %u (absent) has the run [%u, %u)\n", label, g[i].series - 1, a, e);
                    bad++;
                }
            }
        }
    }
    failures += bad;
    /* the same bytes as a shorter or longer index, or with another entry count: refused */
    gix y;
    CHECK(gix_parse(b, len - 8, (uint32_t)n, &y) == TSDB_ERR_CORRUPT || len < 8);
    /* one entry more fits only where the zero padding holds a whole entry (or entries take no bytes): never a
       read beyond the index */
    if (gix_parse(b, len, (uint32_t)n + 1, &y) == 0)
        CHECK((size_t)(pp.ix.e - b) + (n + 1) * pp.ix.stride <= len);
    if (n > 0)
        CHECK(gix_parse(b, len, (uint32_t)n - 1, &y) == TSDB_ERR_CORRUPT);
    free(b);
}

static void test_rows(void)
{
    static const size_t grs[] = {1, 2, 3, 64, 8192};
    const size_t n = 8192 * 3 + 17;
    int64_t *ts = malloc(n * sizeof *ts);
    uint64_t *v = malloc(n * 8);
    granule *g = malloc(n * sizeof *g);
    for (int t = 0; t < NTYPES; t++)
        for (int s = 0; s < S_N; s++) {
            size_t rows = s == S_HUGE || s == S_SPECIAL ? 3000 : n; /* the 1-row granules of all of n are slow */
            gen_rows(types[t], s, rows, ts, v);
            for (size_t k = 0; k < sizeof grs / sizeof *grs; k++)
                for (int layout = 0; layout < 4; layout++) {
                    /* one series; a series per granule (many series per part); runs of 5 granules with gaps in
                       the ids; a series per granule ending at the largest ids */
                    static const size_t per[] = {0, 1, 5, 1};
                    static const uint32_t gap[] = {0, 1, 100003, 1}; /* 4919 runs x 100003 < 2^32 */
                    size_t ng = build(types[t], ts, v, rows, grs[k], per[layout], gap[layout], g);
                    if (layout == 3)
                        for (size_t i = 0; i < ng; i++)
                            g[i].series = UINT32_MAX - (uint32_t)(ng - 1 - i);
                    char label[160];
                    snprintf(label, sizeof label, "%s %s, %zu rows per granule, layout %d (%zu entries)", tname[t],
                             sname[s], grs[k], layout, ng);
                    size_t len = 0;
                    roundtrip(g, ng, label, &len);
                    if (s == S_SENSOR && layout == 0 && (grs[k] == 64 || grs[k] == 8192))
                        printf("  %-50s %6.1f bytes per entry\n", label, (double)len / (double)ng);
                }
        }
    free(ts);
    free(v);
    free(g);
}

/* a value of a random width, shift and base: the frames gix_encode picks from */
static uint64_t field_rand(int regime, uint64_t base, size_t i)
{
    switch (regime) {
    case 0: return base;                                            /* constant */
    case 1: return base + (rnd() & 0xff);                           /* narrow */
    case 2: return base + ((rnd() & 0xffff) << 20);                 /* shifted */
    case 3: return rnd();                                           /* all 64 bits */
    case 4: return base + (uint64_t)i * 4096 + (rnd() & 0x3f);      /* a line */
    case 5: return base - (uint64_t)i * 977;                        /* a falling line */
    case 6: return (uint64_t)(int64_t)(int32_t)(rnd() & 0xfff) - 2048; /* small, signed */
    case 7: return f64b((double)(float)((double)(rnd() & 0xffff) / 64)); /* a float32 as a double */
    default: return rnd() >> (rnd() % 64);                          /* any width */
    }
}

/* tw_step and tw_lin of a granule on a regular grid are derived from its sum and its first and last value, but never
 * from a NaN: the payload of first + last with both NaN is the compiler's to pick (not defined by IEEE 754), and the
 * encoder and the decoder evaluate it separately. Entries with NaN ends of different payloads, with a finite sum, and
 * with tw_lin and tw_step equal to each payload and to what gix_dlin / gix_dstep give here (whichever it is at this
 * optimization level), must round-trip: gix_encode refused them at -O1 and above before the encoder skipped NaNs. */
static double dbl_from(uint64_t u)
{
    double d;
    memcpy(&d, &u, 8);
    return d;
}

static void test_nan_ends(void)
{
    const double nans[] = {dbl_from(0x7ff8000000000001ull), dbl_from(0xfff8000000000abcull),
                           dbl_from(0x7ff8123456789abcull), dbl_from(0xffffffffffffffffull)};
    granule g[64];
    memset(g, 0, sizeof g);
    size_t n = 0;
    for (int a = 0; a < 4; a++)
        for (int b = 0; b < 4; b++)
            for (int how = 0; how < 3 && n + 3 <= 64; how++) {
                granule *e = &g[n++];
                e->series = (uint32_t)(n / 8); /* ascending */
                e->count = 100;
                e->ts_min = 1000;
                e->ts_max = 1000 + 99 * 60000;
                e->fsum = 123.5; /* finite: the areas are derivable but for the NaN ends */
                e->tw_first = nans[a];
                e->tw_last = nans[b];
                e->tw_gap = 60000;
                e->tw_step_ms = e->tw_lin_ms = (uint64_t)(e->ts_max - e->ts_min);
                volatile double fs = e->fsum, f1 = e->tw_first, f2 = e->tw_last; /* not folded at compile time */
                double lin = gix_dlin(fs, f1, f2, e->tw_gap), stp = gix_dstep(fs, f2, e->tw_gap);
                e->tw_lin = how == 0 ? nans[a] : how == 1 ? nans[b] : lin;
                e->tw_step = how == 2 ? stp : nans[b];
            }
    for (size_t i = 1; i < n; i++)
        if (g[i].series < g[i - 1].series)
            g[i].series = g[i - 1].series;
    roundtrip(g, n, "NaN ends on a grid", NULL);
    for (size_t i = 0; i < n; i++) { /* one entry at a time as well: the flags are per entry */
        char label[64];
        snprintf(label, sizeof label, "NaN ends on a grid, entry %zu alone", i);
        roundtrip(&g[i], 1, label, NULL);
    }
}

static void test_random_fields(void)
{
    static const size_t ns[] = {1, 2, 3, 7, 100, 1000};
    granule *g = malloc(1000 * sizeof *g);
    for (int iter = 0; iter < 400; iter++) {
        size_t n = ns[iter % 6];
        int reg[GF_N];
        uint64_t base[GF_N];
        for (int f = 0; f < GF_N; f++) {
            reg[f] = (int)(rnd() % 9);
            base[f] = rnd();
        }
        uint32_t series = (uint32_t)(rnd() % 50);
        for (size_t i = 0; i < n; i++) {
            granule *e = &g[i];
            uint64_t u[16];
            for (int k = 0; k < 16; k++)
                u[k] = field_rand(reg[k + 4], base[k + 4], i);
            if (i && rnd() % 3 == 0)
                series += 1 + (uint32_t)(rnd() % (iter % 2 ? 3 : 100000));
            e->series = series;
            e->count = (uint32_t)field_rand(reg[0], base[0], i);
            e->ts_min = (int64_t)field_rand(reg[1], base[1], i);
            e->ts_max = (int64_t)((uint64_t)e->ts_min + field_rand(reg[2] % 3, base[2] & 0xffff, i));
            e->off = field_rand(reg[3], base[3], i);
            e->ts_len = (uint32_t)u[0];
            e->val_len = (uint32_t)u[1];
            e->nulls = (uint32_t)u[2];
            e->crc = (uint32_t)u[3];
            e->imin = (int64_t)u[4];
            e->imax = (int64_t)u[5];
            e->isum_lo = u[6];
            e->isum_hi = (int64_t)u[7];
            e->isq0 = u[8];
            e->isq1 = u[9];
            e->isq2 = u[10];
            memcpy(&e->tw_first, &u[11], 8);
            memcpy(&e->tw_last, &u[12], 8);
            memcpy(&e->tw_step, &u[13], 8);
            e->tw_lin = rnd() % 4 ? e->tw_step : gix_dlin(e->fsum, e->tw_first, e->tw_last, 60000);
            e->tw_step_ms = (uint64_t)(e->ts_max - e->ts_min) - (u[14] & (rnd() % 2 ? 0 : 0xffff));
            e->tw_lin_ms = rnd() % 2 ? e->tw_step_ms : u[15];
            e->tw_gap = rnd() % 2 ? 60000 : (uint32_t)rnd();
            e->tw_flags = (uint32_t)(rnd() % 4);
            if (rnd() % 3 == 0) /* on a grid: the areas follow from the sum and the ends */
                e->tw_step = gix_dstep(e->fsum, e->tw_last, e->tw_gap);
        }
        char label[64];
        snprintf(label, sizeof label, "random fields #%d (%zu entries)", iter, n);
        roundtrip(g, n, label, NULL);
    }
    /* entries whose series go down are refused, not stored */
    if (1) {
        memset(g, 0, 2 * sizeof *g);
        g[0].series = 5;
        g[1].series = 4;
        uint8_t *enc = NULL;
        size_t len = 0;
        CHECK(gix_encode(g, 2, &enc, &len) == TSDB_ERR_INVALID && enc == NULL);
    }
    /* an empty index */
    uint8_t *enc = NULL;
    size_t len = 0;
    CHECK(gix_encode(g, 0, &enc, &len) == 0);
    gix y;
    uint8_t *b = calloc(1, len + 8);
    memcpy(b, enc, len);
    CHECK(gix_parse(b, len, 0, &y) == 0 && y.n == 0 && y.nruns == 0);
    CHECK(gix_parse(b, len, 1, &y) == TSDB_ERR_CORRUPT);
    free(b);
    free(enc);
    free(g);
}

/* ------------------------------------------------------------------ through the engine */

static int rm_one(const char *p, const struct stat *s, int flag, struct FTW *f)
{
    (void)s, (void)flag, (void)f;
    return remove(p);
}
static void rm_rf(const char *dir) { nftw(dir, rm_one, 16, FTW_DEPTH | FTW_PHYS); }

/* every entry of every part in dir against its rows; returns the entries checked */
static size_t check_parts(const char *dir, int type, const char *label)
{
    int dfd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    DIR *d = fdopendir(dup(dfd));
    struct dirent *de;
    codec_ctx *cc = codec_new();
    int64_t *ts = malloc(MAX_GRANULE_ROWS * sizeof *ts);
    uint8_t *v = malloc(MAX_GRANULE_ROWS * 8);
    size_t checked = 0, parts = 0;
    int bad = 0;
    while (d && (de = readdir(d))) {
        if (de->d_name[0] != 'p' || !strstr(de->d_name, ".part"))
            continue;
        part *p;
        if (part_open(dfd, de->d_name, false, &p) != 0) {
            fprintf(stderr, "%s: part %s does not open\n", label, de->d_name);
            failures++;
            continue;
        }
        parts++;
        for (uint32_t i = 0; i < p->ng && bad < 5; i++) {
            granule s, r;
            memset(&s, 0, sizeof s);
            gx_load(p, i, GW_ALL, &s);
            if (granule_read(cc, p, &s, type, ts, v, true) != 0) { /* verify: the crc covers the blocks */
                fprintf(stderr, "%s: %s entry %u does not decode\n", label, de->d_name, i);
                bad++;
                continue;
            }
            memset(&r, 0, sizeof r);
            granule_stats(type, v, s.count, &r);
            granule_twa(type, ts, v, s.count, &r);
            r.series = s.series;
            r.count = s.count;
            r.ts_min = ts[0];
            r.ts_max = ts[s.count - 1];
            r.off = s.off;
            r.ts_len = s.ts_len;
            r.val_len = s.val_len;
            r.crc = s.crc;
            if (memcmp(&r, &s, sizeof r) != 0) {
                fprintf(stderr, "%s: %s entry %u: the index differs from its rows' statistics\n", label,
                        de->d_name, i);
                bad++;
            }
            checked++;
        }
        part_unref(p);
    }
    failures += bad;
    if (d)
        closedir(d);
    close(dfd);
    codec_free(cc);
    free(ts);
    free(v);
    CHECK(parts > 0);
    return checked;
}

static void test_engine(const char *base)
{
    struct cfg {
        uint32_t grows;
        int64_t pms;
        const char *name;
    } cfgs[] = {{64, 86400000, "64-row granules"}, {1, 86400000, "1-row granules"},
                {64, INT64_C(1) << 61, "partitions of 2^61 ms"}};
    const size_t n = 2000;
    int64_t *ts = malloc(n * sizeof *ts);
    uint64_t *v = malloc(n * 8);
    uint8_t *valid = malloc(n);
    for (size_t c = 0; c < sizeof cfgs / sizeof *cfgs; c++)
        for (int t = 0; t < NTYPES; t++) {
            char dir[512], label[160];
            snprintf(dir, sizeof dir, "%s/e%zu-%s", base, c, tname[t]);
            snprintf(label, sizeof label, "engine, %s, %s", cfgs[c].name, tname[t]);
            rm_rf(dir);
            tsdb_options o;
            tsdb_options_init(&o);
            o.granule_rows = cfgs[c].grows;
            o.partition_ms = cfgs[c].pms;
            o.partition_classes = 0;
            o.merge_threads = 0;
            o.wal_max_batch_rows = 1; /* straight to parts */
            o.fsync = 0;
            tsdb *db;
            OK(tsdb_open(dir, &o, &db));
            for (int s = 0; s < S_N; s++) {
                if (cfgs[c].grows == 1 && s != S_SPECIAL && s != S_NULLS && s != S_PRE1970)
                    continue;
                char sn[32];
                snprintf(sn, sizeof sn, "s-%s", sname[s]);
                tsdb_sid id;
                OK(tsdb_series_create(db, sn, (tsdb_type)types[t], NULL, &id));
                size_t rows = cfgs[c].grows == 1 ? 300 : n;
                gen_rows(types[t], s, rows, ts, v);
                if (s == S_HUGE && cfgs[c].pms < INT64_C(1) << 61)
                    rows = 40; /* a partition per row in day partitions */
                for (size_t i = 0; i < rows; i++) {
                    uint64_t b = type_width(types[t]) == 4 ? ((uint32_t *)v)[i] : v[i];
                    valid[i] = b != type_null(types[t]);
                }
                if (s == S_HUGE) /* the last instant a row may have is TSDB_TS_MAX - 1 */
                    for (size_t i = 0; i < rows; i++)
                        CHECK(ts[i] < INT64_MAX);
                tsdb_values tv = {.type = (tsdb_type)types[t], .values = v, .valid = valid};
                /* in two halves: two parts per partition, which the optimize merges */
                OK(tsdb_insert_series_values(db, id, ts, &tv, rows / 2));
                tsdb_values tv2 = {.type = (tsdb_type)types[t],
                                   .values = (const uint8_t *)v + rows / 2 * type_width(types[t]),
                                   .valid = valid + rows / 2};
                OK(tsdb_insert_series_values(db, id, ts + rows / 2, &tv2, rows - rows / 2));
            }
            OK(tsdb_close(db));
            size_t before = check_parts(dir, types[t], label);
            OK(tsdb_open(dir, &o, &db));
            OK(tsdb_optimize(db));
            OK(tsdb_close(db));
            char l2[200];
            snprintf(l2, sizeof l2, "%s, optimized", label);
            size_t after = check_parts(dir, types[t], l2);
            CHECK(before > 0 && after > 0);
            rm_rf(dir);
        }
    free(ts);
    free(v);
    free(valid);
}

int main(int argc, char **argv)
{
    char base[400];
    snprintf(base, sizeof base, "%s/tsdb-gindex-%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    rm_rf(base);
    if (mkdir(base, 0755) != 0) {
        perror(base);
        return 1;
    }
    test_rows();
    test_random_fields();
    test_nan_ends();
    test_engine(base);
    printf("%zu indexes round-tripped: %zu entries in %zu runs, %.1f bytes per entry\n", total_idx, total_entries,
           total_runs, total_entries ? (double)total_bytes / (double)total_entries : 0);
    rm_rf(base);
    if (failures) {
        fprintf(stderr, "test_gindex: %d failures\n", failures);
        return 1;
    }
    printf("test_gindex: ok\n");
    return 0;
}
