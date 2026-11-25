/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Bucketed and downsampled queries over two overlapping parts with mutated granule payloads:
 *
 * Creates two small overlapping float32 parts (3 granules of 64 rows each, interleaved
 * timestamps, periodic nulls, uncompressed insert blocks) so merge_stream interleaves them.
 * Keeps headers, indexes, and footers intact so tsdb_open succeeds, opens read-only with
 * verify_checksums = 0 and latest_cache = 0, and pwrite-mutates the granule payloads in place
 * on each fuzz input.
 *
 * Runs every query kind over the whole range and over a fuzz-derived window: raw, values,
 * dense and sparse bucketed group, spread, TWA (STEP and LINEAR), LTTB, minute and hour
 * buckets, bounds, tsdb_query_multi (all 5 kinds), and tsdb_latest.
 */
#include "fuzz.h"
#include "internal.h"

#include <dirent.h>
#include <float.h>

static const char *D;
static tsdb *db;
static tsdb_sid sid;
static int pfd[2] = {-1, -1};
static uint64_t pay_off[2], pay_len[2];
static size_t total_pay;
static uint8_t *orig_pay;
static uint8_t *mut_buf;

/* Locations inside part 0's payload, for fuzz_seeds(). */
static uint64_t seed_ts_off, seed_ts_len;
static uint64_t seed_val_off, seed_val_len;

static const int64_t T0 = 1704067200000LL; /* 2024-01-01T00:00:00Z */
enum { N_PER_PART = 192, STEP_MS = 15000 };
static const int64_t T_END = 1704067200000LL + (int64_t)(2 * N_PER_PART) * STEP_MS;

static volatile uint64_t fuzz_sink;

static int pwrite_all(int fd, const void *buf, size_t len, off_t off)
{
    const uint8_t *p = buf;
    while (len) {
        ssize_t w = pwrite(fd, p, len, off);
        if (w < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        p += w;
        off += w;
        len -= (size_t)w;
    }
    return 0;
}

static int pread_all(int fd, void *buf, size_t len, off_t off)
{
    uint8_t *p = buf;
    while (len) {
        ssize_t r = pread(fd, p, len, off);
        if (r <= 0) {
            if (r < 0 && errno == EINTR)
                continue;
            return -1;
        }
        p += r;
        off += r;
        len -= (size_t)r;
    }
    return 0;
}

static void bucketed_cleanup(void)
{
    agg_dense_buckets = 0;
    if (db) {
        tsdb_close(db);
        db = NULL;
    }
    for (int i = 0; i < 2; i++) {
        if (pfd[i] >= 0) {
            close(pfd[i]);
            pfd[i] = -1;
        }
    }
    free(orig_pay);
    orig_pay = NULL;
    free(mut_buf);
    mut_buf = NULL;
}

int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    (void)argc, (void)argv;
    D = fuzz_dir("bucketed");
    atexit(fuzz_rm_at_exit);
    atexit(bucketed_cleanup);

    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.merge_threads = 0;
    o.granule_rows = 64;
    o.wal_max_batch_rows = 100;
    o.insert_zstd_level = 0;
    o.stats = -1;
    o.latest_cache = 0;

    for (int half = 0; half < 2; half++) {
        tsdb *w;
        if (tsdb_open(D, &o, &w) != 0) {
            fprintf(stderr, "fuzz-bucketed: tsdb_open failed\n");
            exit(1);
        }
        tsdb_sid s;
        if (tsdb_series(w, "s", &s) != 0) {
            fprintf(stderr, "fuzz-bucketed: tsdb_series failed\n");
            exit(1);
        }
        int64_t ts[N_PER_PART];
        float v[N_PER_PART];
        uint8_t valid[(N_PER_PART + 7) / 8];
        memset(valid, 0, sizeof valid);
        for (int i = 0; i < N_PER_PART; i++) {
            ts[i] = T0 + (int64_t)(2 * i + half) * STEP_MS;
            v[i] = (float)((i + half * 13) % 37) * 0.25f;
            if (i % 29 != 7)
                valid[i >> 3] |= (uint8_t)(1u << (i & 7));
        }
        tsdb_values vals = {TSDB_FLOAT32, v, valid};
        if (tsdb_insert_series_values(w, s, ts, &vals, N_PER_PART) != 0 || tsdb_close(w) != 0) {
            fprintf(stderr, "fuzz-bucketed: insert/close failed\n");
            exit(1);
        }
    }

    struct dirent **ents = NULL;
    int nent = scandir(D, &ents, NULL, alphasort);
    if (nent < 0) {
        perror("scandir");
        exit(1);
    }
    char pnames[2][256];
    int np = 0;
    for (int i = 0; i < nent; i++) {
        if (np < 2 && part_file_name(ents[i]->d_name))
            snprintf(pnames[np++], sizeof pnames[0], "%s", ents[i]->d_name);
        free(ents[i]);
    }
    free(ents);
    if (np != 2) {
        fprintf(stderr, "fuzz-bucketed: expected 2 parts, found %d\n", np);
        exit(1);
    }

    int dfd = open(D, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd < 0) {
        perror("open");
        exit(1);
    }
    for (int k = 0; k < 2; k++) {
        part *pt = NULL;
        if (part_open(dfd, pnames[k], false, &pt) != 0 || pt->ng < 2) {
            fprintf(stderr, "fuzz-bucketed: part_open failed for %s\n", pnames[k]);
            exit(1);
        }
        pay_off[k] = pt->hdr_len;
        pay_len[k] = pt->index_off - pt->hdr_len;
        if (k == 0) {
            granule g0, g1;
            memset(&g0, 0, sizeof g0);
            memset(&g1, 0, sizeof g1);
            gx_load(pt, 0, GW_LOC, &g0);
            gx_load(pt, 1, GW_LOC, &g1);
            seed_ts_off = g1.off - pay_off[0];
            seed_ts_len = g1.ts_len;
            seed_val_off = g0.off + g0.ts_len - pay_off[0];
            seed_val_len = g0.val_len;
        }
        part_unref(pt);
        pfd[k] = openat(dfd, pnames[k], O_RDWR | O_CLOEXEC);
        if (pfd[k] < 0) {
            perror("openat");
            exit(1);
        }
    }
    close(dfd);

    total_pay = (size_t)(pay_len[0] + pay_len[1]);
    orig_pay = malloc(total_pay ? total_pay : 1);
    mut_buf = malloc(total_pay ? total_pay : 1);
    if (!orig_pay || !mut_buf)
        exit(1);
    if (pread_all(pfd[0], orig_pay, (size_t)pay_len[0], (off_t)pay_off[0]) != 0 ||
        pread_all(pfd[1], orig_pay + pay_len[0], (size_t)pay_len[1], (off_t)pay_off[1]) != 0) {
        fprintf(stderr, "fuzz-bucketed: pread failed\n");
        exit(1);
    }

    tsdb_options qo;
    tsdb_options_init(&qo);
    qo.fsync = 0;
    qo.merge_threads = 0;
    qo.query_threads = 2;
    qo.verify_checksums = 0;
    qo.read_only = 1;
    qo.stats = -1;
    qo.latest_cache = 0;
    if (tsdb_open(D, &qo, &db) != 0 || tsdb_series_lookup(db, "s", &sid) != 0) {
        fprintf(stderr, "fuzz-bucketed: query open failed\n");
        exit(1);
    }
    return 0;
}

static int raw_cb(void *ctx, const int64_t *ts, const float *values, size_t n)
{
    uint64_t acc = *(uint64_t *)ctx;
    for (size_t i = 0; i < n; i++) {
        uint32_t bits = 0;
        memcpy(&bits, &values[i], 4);
        acc ^= (uint64_t)ts[i] + bits;
    }
    *(uint64_t *)ctx = acc;
    return 0;
}

static int values_cb(void *ctx, const int64_t *ts, const tsdb_values *v, size_t n)
{
    uint64_t acc = *(uint64_t *)ctx;
    const float *fv = v->values;
    for (size_t i = 0; i < n; i++) {
        uint32_t bits = 0;
        if (!v->valid || (v->valid[i >> 3] >> (i & 7) & 1))
            memcpy(&bits, &fv[i], 4);
        acc ^= (uint64_t)ts[i] + bits;
    }
    *(uint64_t *)ctx = acc;
    return 0;
}

typedef struct multi_ctx {
    tsdb_multi_kind kind;
    uint64_t acc;
} multi_ctx;

static int multi_cb(void *ctx, const tsdb_series_result *r)
{
    multi_ctx *mc = ctx;
    mc->acc ^= (uint64_t)r->index + (uint64_t)r->status + (uint64_t)r->n;
    if (r->n > 0) {
        if (mc->kind == TSDB_MULTI_VALUES || mc->kind == TSDB_MULTI_LTTB) {
            values_cb(&mc->acc, r->ts, &r->values, r->n);
        } else if (mc->kind == TSDB_MULTI_BUCKETS) {
            const tsdb_bucket *b = r->buckets;
            mc->acc ^= (uint64_t)b[0].start + b[r->n - 1].count;
        } else if (mc->kind == TSDB_MULTI_SPREAD) {
            const tsdb_spread_bucket *b = r->buckets;
            mc->acc ^= (uint64_t)b[0].b.start + b[r->n - 1].b.count;
        } else if (mc->kind == TSDB_MULTI_TWA) {
            const tsdb_twa_bucket *b = r->buckets;
            mc->acc ^= (uint64_t)b[0].start + b[r->n - 1].count;
        }
    }
    return 0;
}

static void touch_buckets(const tsdb_bucket *b, size_t nb, uint64_t *acc)
{
    if (!b || !nb)
        return;
    uint64_t bits = 0;
    memcpy(&bits, &b[nb - 1].sum, 8);
    *acc ^= (uint64_t)b[0].start + b[nb - 1].count + bits;
}

static void touch_spread(const tsdb_spread_bucket *b, size_t nb, uint64_t *acc)
{
    if (!b || !nb)
        return;
    uint64_t bits = 0;
    memcpy(&bits, &b[nb - 1].stddev_pop, 8);
    *acc ^= (uint64_t)b[0].b.start + b[nb - 1].b.count + bits;
}

static void touch_twa(const tsdb_twa_bucket *b, size_t nb, uint64_t *acc)
{
    if (!b || !nb)
        return;
    uint64_t bits = 0;
    memcpy(&bits, &b[nb - 1].twa, 8);
    *acc ^= (uint64_t)b[0].start + b[nb - 1].covered_ms + bits;
}

static void run_suite(int64_t start, int64_t end, const tsdb_query_opts *qopts, const tsdb_twa_opts *topts,
                      size_t lttb_thresh, int64_t fixed_iv)
{
    uint64_t acc = 0;
    const tsdb_group minute = {TSDB_MINUTE, 1, NULL};
    const tsdb_group hour = {TSDB_HOUR, 1, NULL};
    const int64_t bounds[4] = {T0 + 10 * 60000LL, T0 + 30 * 60000LL, T0 + 50 * 60000LL, T0 + 70 * 60000LL};

    /* Raw */
    tsdb_query_raw_ex(db, sid, start, end, qopts, raw_cb, &acc);
    tsdb_query_values(db, sid, start, end, qopts, values_cb, &acc);

    /* Dense & sparse bucketed group (minute, hour, fixed interval, single bucket, bounds) */
    tsdb_bucket *b = NULL;
    size_t nb = 0;
    agg_dense_buckets = 0;
    tsdb_query_group_ex(db, sid, start, end, &minute, qopts, &b, &nb);
    touch_buckets(b, nb, &acc);
    free(b), b = NULL, nb = 0;

    agg_dense_buckets = 4; /* sparse: chunks of 4 buckets */
    tsdb_query_group_ex(db, sid, start, end, &minute, qopts, &b, &nb);
    touch_buckets(b, nb, &acc);
    free(b), b = NULL, nb = 0;
    agg_dense_buckets = 0;

    tsdb_query_group_ex(db, sid, start, end, &hour, qopts, &b, &nb);
    touch_buckets(b, nb, &acc);
    free(b), b = NULL, nb = 0;

    tsdb_query_agg_ex(db, sid, start, end, fixed_iv, qopts, &b, &nb);
    touch_buckets(b, nb, &acc);
    free(b), b = NULL, nb = 0;

    tsdb_query_agg_ex(db, sid, start, end, 0, qopts, &b, &nb);
    touch_buckets(b, nb, &acc);
    free(b), b = NULL, nb = 0;

    tsdb_query_bounds(db, sid, start, end, bounds, 4, qopts, &b, &nb);
    touch_buckets(b, nb, &acc);
    free(b), b = NULL, nb = 0;

    /* Spread: dense, sparse, interval, minute, hour, bounds */
    tsdb_spread_bucket *sb = NULL;
    tsdb_query_spread(db, sid, start, end, fixed_iv, qopts, &sb, &nb);
    touch_spread(sb, nb, &acc);
    free(sb), sb = NULL, nb = 0;

    tsdb_query_spread_group(db, sid, start, end, &minute, qopts, &sb, &nb);
    touch_spread(sb, nb, &acc);
    free(sb), sb = NULL, nb = 0;

    agg_dense_buckets = 4;
    tsdb_query_spread_group(db, sid, start, end, &minute, qopts, &sb, &nb);
    touch_spread(sb, nb, &acc);
    free(sb), sb = NULL, nb = 0;
    agg_dense_buckets = 0;

    tsdb_query_spread_group(db, sid, start, end, &hour, qopts, &sb, &nb);
    touch_spread(sb, nb, &acc);
    free(sb), sb = NULL, nb = 0;

    tsdb_query_spread_bounds(db, sid, start, end, bounds, 4, qopts, &sb, &nb);
    touch_spread(sb, nb, &acc);
    free(sb), sb = NULL, nb = 0;

    /* TWA: interval, minute, hour, bounds */
    tsdb_twa_bucket *tb = NULL;
    tsdb_query_twa(db, sid, start, end, fixed_iv, topts, &tb, &nb);
    touch_twa(tb, nb, &acc);
    free(tb), tb = NULL, nb = 0;

    tsdb_query_twa_group(db, sid, start, end, &minute, topts, &tb, &nb);
    touch_twa(tb, nb, &acc);
    free(tb), tb = NULL, nb = 0;

    tsdb_query_twa_group(db, sid, start, end, &hour, topts, &tb, &nb);
    touch_twa(tb, nb, &acc);
    free(tb), tb = NULL, nb = 0;

    tsdb_query_twa_bounds(db, sid, start, end, bounds, 4, topts, &tb, &nb);
    touch_twa(tb, nb, &acc);
    free(tb), tb = NULL, nb = 0;

    /* LTTB */
    tsdb_query_lttb_ex(db, sid, start, end, lttb_thresh, qopts, values_cb, &acc);

    /* tsdb_query_multi (2 series slots across 2 threads, all 5 kinds + group/bounds variants) */
    const tsdb_sid sids[2] = {sid, sid};
    const tsdb_multi_opts mo = {.threads = 2};
    static const tsdb_multi_kind kinds[] = {TSDB_MULTI_VALUES, TSDB_MULTI_BUCKETS, TSDB_MULTI_SPREAD,
                                            TSDB_MULTI_TWA, TSDB_MULTI_LTTB};
    for (size_t m = 0; m < sizeof kinds / sizeof *kinds; m++) {
        tsdb_multi_query q = {.kind = kinds[m],
                              .start = start,
                              .end = end,
                              .opts = qopts ? *qopts : (tsdb_query_opts){0},
                              .interval_ms = fixed_iv,
                              .threshold = lttb_thresh,
                              .twa = topts ? *topts : (tsdb_twa_opts){0}};
        multi_ctx mc = {kinds[m], 0};
        tsdb_query_multi(db, &q, sids, 2, &mo, multi_cb, &mc);
        acc ^= mc.acc;
    }
    {
        tsdb_multi_query q = {.kind = TSDB_MULTI_BUCKETS,
                              .start = start,
                              .end = end,
                              .opts = qopts ? *qopts : (tsdb_query_opts){0},
                              .group = &hour};
        multi_ctx mc = {TSDB_MULTI_BUCKETS, 0};
        tsdb_query_multi(db, &q, sids, 2, &mo, multi_cb, &mc);
        acc ^= mc.acc;
    }
    {
        tsdb_multi_query q = {.kind = TSDB_MULTI_SPREAD,
                              .start = start,
                              .end = end,
                              .opts = qopts ? *qopts : (tsdb_query_opts){0},
                              .bounds = bounds,
                              .nbounds = 4};
        multi_ctx mc = {TSDB_MULTI_SPREAD, 0};
        tsdb_query_multi(db, &q, sids, 2, &mo, multi_cb, &mc);
        acc ^= mc.acc;
    }

    /* tsdb_latest */
    tsdb_latest_result lr[2];
    tsdb_latest(db, sids, 2, NULL, lr);
    tsdb_latest_opts lo = {.end = end, .with_nulls = qopts && qopts->order == TSDB_DESC, .threads = 2};
    tsdb_latest(db, sids, 2, &lo, lr);
    acc ^= (uint64_t)lr[0].ts + (uint64_t)lr[1].kind;

    fuzz_sink ^= acc;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (!D)
        LLVMFuzzerInitialize(NULL, NULL);

    uint8_t hdr[8] = {0};
    if (size)
        memcpy(hdr, data, size < sizeof hdr ? size : sizeof hdr);

    memcpy(mut_buf, orig_pay, total_pay);
    size_t rem = size > sizeof hdr ? size - sizeof hdr : 0;
    if (rem > total_pay)
        rem = total_pay;
    if (rem)
        memcpy(mut_buf, data + sizeof hdr, rem);

    if (pwrite_all(pfd[0], mut_buf, (size_t)pay_len[0], (off_t)pay_off[0]) != 0 ||
        pwrite_all(pfd[1], mut_buf + pay_len[0], (size_t)pay_len[1], (off_t)pay_off[1]) != 0)
        return 0;

    /* 1. Whole range, ascending, no limit, both STEP and LINEAR TWA */
    tsdb_twa_opts full_twa = {.method = (hdr[0] & 0x80) ? TSDB_TWA_LINEAR : TSDB_TWA_STEP};
    run_suite(T0, T_END, NULL, &full_twa, 20, 60000);

    /* 2. Random window and options derived from fuzz bytes */
    uint32_t r0, r1;
    memcpy(&r0, hdr, 4);
    memcpy(&r1, hdr + 4, 4);
    uint32_t span = (uint32_t)(T_END - T0 + 60000);
    int64_t t1 = T0 - 30000 + (int64_t)(r0 % span);
    int64_t t2 = T0 - 30000 + (int64_t)(r1 % span);
    int64_t wlo = t1 <= t2 ? t1 : t2;
    int64_t whi = t1 <= t2 ? t2 : t1;
    if (whi == wlo && (hdr[1] & 1))
        whi++;

    uint8_t f = hdr[0];
    tsdb_query_opts qopts = {
        .order = (f & 1) ? TSDB_DESC : TSDB_ASC,
        .limit = (f & 2) ? (size_t)((f >> 2) & 7) + 1 : 0,
    };
    tsdb_twa_opts topts = {
        .method = (f & 4) ? TSDB_TWA_LINEAR : TSDB_TWA_STEP,
        .max_gap_ms = (f & 8) ? 30000 : 0,
        .order = qopts.order,
        .limit = qopts.limit,
        .no_stats = (f & 16) != 0,
    };
    static const int64_t ivs[4] = {60000, 3600000, 15000, 0};
    size_t lttb_thresh = 3 + (size_t)(hdr[1] % 30);
    run_suite(wlo, whi, &qopts, &topts, lttb_thresh, ivs[(f >> 5) & 3]);
    return 0;
}

static void write_seed(const char *dir, const char *name, const uint8_t hdr[8], const uint8_t *pay, size_t plen)
{
    uint8_t *b = malloc(8 + plen);
    if (!b)
        exit(1);
    memcpy(b, hdr, 8);
    if (plen)
        memcpy(b + 8, pay, plen);
    fuzz_put(dir, name, b, 8 + plen);
    free(b);
}

void fuzz_seeds(const char *dir)
{
    uint8_t h0[8] = {0};
    write_seed(dir, "seed-valid-full", h0, orig_pay, total_pay);

    /* Sub-window with DESC, limit, LINEAR TWA, max_gap_ms, no_stats */
    uint8_t h1[8] = {0x9f, 0x05, 0x02, 0x00, 0x40, 0x20, 0x30, 0x00};
    write_seed(dir, "seed-valid-win", h1, orig_pay, total_pay);

    /* Damaged timestamp block (granule 1 of part 0) */
    uint8_t *dmg = malloc(total_pay);
    if (!dmg)
        exit(1);
    memcpy(dmg, orig_pay, total_pay);
    if (seed_ts_len >= 17 && dmg[seed_ts_off] == BLK_DD) {
        int64_t w[2];
        memcpy(w, dmg + seed_ts_off + 1, sizeof w);
        w[0] += (int64_t)1 << 40;
        memcpy(dmg + seed_ts_off + 1, w, sizeof w);
    } else if (seed_ts_len > 1) {
        for (size_t i = 1; i < seed_ts_len && i < 16; i++)
            dmg[seed_ts_off + i] ^= 0x55;
    }
    write_seed(dir, "seed-damaged-ts-high", h0, dmg, total_pay);

    memcpy(dmg, orig_pay, total_pay);
    if (seed_ts_len >= 17 && dmg[seed_ts_off] == BLK_DD) {
        int64_t w[2];
        memcpy(w, dmg + seed_ts_off + 1, sizeof w);
        w[1] = -w[1];
        memcpy(dmg + seed_ts_off + 1, w, sizeof w);
    } else if (seed_ts_len > 2) {
        dmg[seed_ts_off + 1] ^= 0x80;
        dmg[seed_ts_off + 2] ^= 0xff;
    }
    write_seed(dir, "seed-damaged-ts-back", h0, dmg, total_pay);

    /* Damaged value block (granule 0 of part 0) */
    memcpy(dmg, orig_pay, total_pay);
    if (seed_val_len > 1) {
        for (size_t i = 1; i < seed_val_len && i < 12; i++)
            dmg[seed_val_off + i] ^= (uint8_t)(0xa5 ^ i);
    }
    write_seed(dir, "seed-damaged-val", h1, dmg, total_pay);
    free(dmg);

    /* Short input */
    write_seed(dir, "seed-short", h1, orig_pay, total_pay < 8 ? total_pay : 8);
}
