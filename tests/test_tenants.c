/*
 * Tenants, hot lists and residency (docs/design-stats.md steps 3 and 4; src/stats_tenant.c, src/stats_hot.c).
 *
 * The oracle: a tenant's numbers are the sums of its series' per-series numbers (tsdb_series_stats, itself checked
 * against decoded rows and part files in test_stats), with the tenant of each name found by this test's own
 * longest-prefix match. It is compared after every way the part list changes (flush, merge, delete, replace,
 * retention, drop_before, a reopen) and after every change of the tenant list (a prefix added, removed, a series
 * moved between tenants), against a database opened fresh with the same list. Totals must also add up to the
 * volumes' own totals (nothing unattributed, nothing counted twice) and never wrap.
 *
 * The approximate numbers are checked against bounds: the hot-reads list against exact counts of a Zipf stream (every
 * series above W / k plus its sampling noise is listed, every estimate is within its error and the noise), a round
 * robin over eight series is sampled evenly, the writes list is exact below 256 series, bytes per row lists each
 * series once, HyperLogLog is within 10% on 20,000 series, ingest equals the rows inserted while merges run, and
 * residency drops after posix_fadvise(DONTNEED) and rises with a query (skipped where mincore does not tell).
 *
 *   test_tenants [scratch dir]
 *   test_tenants --map-bench N [scratch dir]   the tenant map's memory with N series created across tenants
 */
#include "internal.h"
#include "tsdb.h"

#include <dirent.h>
#include <fcntl.h>
#include <ftw.h>
#include <inttypes.h>
#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
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

static int rm_one(const char *p, const struct stat *sb, int flag, struct FTW *ftw)
{
    (void)sb, (void)flag, (void)ftw;
    return remove(p);
}
static void rm_rf(const char *dir) { nftw(dir, rm_one, 16, FTW_DEPTH | FTW_PHYS); }

#define DAY 86400000ll
#define T0 1704067200000ll /* 2024-01-01 UTC */

static tsdb_options base_opts(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.merge_threads = 0; /* the test decides when a merge happens */
    o.query_threads = 1;
    o.granule_rows = 64;
    o.wal_max_batch_rows = 1000; /* a batch of more rows goes straight to parts */
    o.stats = 1;
    o.stats_sweep_mb_s = -1;
    o.stats_recompute_s = 3600; /* only a settle counts a stale partition again: the test decides when */
    return o;
}

static void put_rows(tsdb *db, tsdb_sid id, int type, int64_t t0, int64_t step, size_t k0, size_t n)
{
    int64_t *ts = malloc(n * sizeof *ts);
    uint8_t *v = calloc(n, 8);
    uint8_t *valid = calloc((n + 7) / 8, 1);
    for (size_t i = 0; i < n; i++) {
        size_t k = k0 + i;
        ts[i] = t0 + (int64_t)k * step;
        if (k % 7 != 3)
            valid[i >> 3] |= (uint8_t)(1u << (i & 7));
        switch (type) {
        case TSDB_FLOAT32: ((float *)v)[i] = (float)(k % 1000) * 0.25f; break;
        case TSDB_FLOAT64: ((double *)v)[i] = (double)(k % 1000) * 0.125; break;
        case TSDB_INT64: ((int64_t *)v)[i] = (int64_t)(k / 3); break;
        case TSDB_BOOL: v[i] = (uint8_t)((k / 5) & 1); break;
        default: ((uint32_t *)v)[i] = 1 + (uint32_t)((k / 50) % 3); break;
        }
    }
    tsdb_values col = {(tsdb_type)type, v, valid};
    OK(tsdb_insert_series_values(db, id, ts, &col, n));
    free(ts);
    free(v);
    free(valid);
}

typedef struct ser {
    tsdb_sid id;
    char name[48];
    int type;
} ser;

/* the longest prefix of name in pf[0..np): its index, or -1 */
static int longest(const char *const *pf, size_t np, const char *name)
{
    int best = -1;
    for (size_t i = 0; i < np; i++)
        if (strncmp(name, pf[i], strlen(pf[i])) == 0 && (best < 0 || strlen(pf[i]) > strlen(pf[best])))
            best = (int)i;
    return best;
}

typedef struct sums {
    uint64_t series, rows, nulls, logical, ts, val, index, granules;
} sums;

/* the tenants' numbers against the sums of the series' (the memtables are empty: tsdb_flush first) */
static void check_tenants(tsdb *db, const ser *sv, size_t ns, const char *const *pf, size_t np, const char *when)
{
    OK(tsdb_flush(db));
    OK(tsdb_stats_settle(db, 60000));
    sums want[16];
    memset(want, 0, sizeof want);
    for (size_t i = 0; i < ns; i++) {
        tsdb_series_stat st;
        size_t done;
        OK(tsdb_series_stats(db, &sv[i].id, 1, 0, &st, &done));
        int t = longest(pf, np, sv[i].name) + 1; /* 0: none */
        sums *w = &want[t];
        w->series++;
        w->rows += st.rows - st.memtable_rows;
        w->nulls += st.nulls;
        w->logical += st.logical_bytes;
        w->ts += st.ts_bytes;
        w->val += st.value_bytes;
        w->index += st.index_bytes;
        w->granules += st.granules;
    }
    sums total;
    memset(&total, 0, sizeof total);
    for (size_t t = 0; t <= np; t++) {
        tsdb_tenant_stats ts;
        OK(tsdb_tenant_stats_get(db, t ? pf[t - 1] : NULL, TSDB_TENANT_VOLUME_SUM, &ts));
        const sums *w = &want[t];
        if (ts.series != w->series || ts.rows != w->rows || ts.nulls != w->nulls || ts.logical_bytes != w->logical ||
            ts.ts_bytes != w->ts || ts.value_bytes != w->val || ts.index_bytes != w->index || ts.granules != w->granules)
            fprintf(stderr,
                    "  %s tenant %s: series %" PRIu64 "/%" PRIu64 " rows %" PRIu64 "/%" PRIu64 " nulls %" PRIu64 "/%" PRIu64
                    " logical %" PRIu64 "/%" PRIu64 " ts %" PRIu64 "/%" PRIu64 " val %" PRIu64 "/%" PRIu64 " index %" PRIu64
                    "/%" PRIu64 " granules %" PRIu64 "/%" PRIu64 "\n",
                    when, t ? pf[t - 1] : "(none)", ts.series, w->series, ts.rows, w->rows, ts.nulls, w->nulls,
                    ts.logical_bytes, w->logical, ts.ts_bytes, w->ts, ts.value_bytes, w->val, ts.index_bytes, w->index,
                    ts.granules, w->granules);
        CHECK(ts.series == w->series);
        CHECK(ts.rows == w->rows);
        CHECK(ts.nulls == w->nulls);
        CHECK(ts.logical_bytes == w->logical);
        CHECK(ts.ts_bytes == w->ts);
        CHECK(ts.value_bytes == w->val);
        CHECK(ts.index_bytes == w->index);
        CHECK(ts.granules == w->granules);
        CHECK(!(ts.flags & TSDB_TENANT_PARTIAL));
        CHECK(ts.stale_s == 0);
        total.rows += ts.rows;
        total.logical += ts.logical_bytes;
        total.ts += ts.ts_bytes;
        total.val += ts.value_bytes;
        total.index += ts.index_bytes;
    }
    uint64_t vl = 0, vts = 0, vval = 0, vidx = 0; /* nothing unattributed, nothing counted twice */
    for (uint32_t v = 0; v < tsdb_volume_count(db); v++) {
        tsdb_volume_stats vs;
        OK(tsdb_get_volume_stats(db, v, &vs));
        vl += vs.logical_bytes;
        vts += vs.ts_bytes;
        vval += vs.value_bytes;
        vidx += vs.index_bytes;
    }
    if (total.logical != vl || total.ts != vts || total.val != vval || total.index != vidx)
        fprintf(stderr, "  %s: tenants sum logical %" PRIu64 " ts %" PRIu64 " val %" PRIu64 " idx %" PRIu64
                        ", volumes %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 "\n",
                when, total.logical, total.ts, total.val, total.index, vl, vts, vval, vidx);
    CHECK(total.logical == vl && total.ts == vts && total.val == vval && total.index == vidx);
}

static void add_series(tsdb *db, ser *sv, size_t *ns, const char *name, int type, const char *vol)
{
    snprintf(sv[*ns].name, sizeof sv[*ns].name, "%s", name);
    sv[*ns].type = type;
    OK(tsdb_series_create(db, name, (tsdb_type)type, vol, &sv[*ns].id));
    (*ns)++;
}

static void load_series(tsdb *db, const ser *sv, size_t ns, size_t rows, size_t k0)
{
    for (size_t i = 0; i < ns; i++)
        for (size_t k = 0; k < rows; k += 5000)
            put_rows(db, sv[i].id, sv[i].type, T0, 60000, k0 + k, rows - k < 5000 ? rows - k : 5000);
}

/* ---------------------------------------------------------------------------- the oracle */

static void test_oracle(const char *base)
{
    char dir[512], vol2[512];
    snprintf(dir, sizeof dir, "%s/oracle", base);
    snprintf(vol2, sizeof vol2, "%s/oracle-vol2", base);
    OK(mkdir(dir, 0755));
    const char *pf[] = {"a/", "a/b/", "c/"};
    tsdb_options o = base_opts();
    o.tenants = pf;
    o.ntenants = 3;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    uint32_t vi;
    OK(tsdb_volume_add(db, vol2, &vi));
    ser sv[16];
    size_t ns = 0;
    add_series(db, sv, &ns, "a/x1", TSDB_FLOAT32, dir);
    add_series(db, sv, &ns, "a/b/y1", TSDB_FLOAT64, vol2);
    add_series(db, sv, &ns, "a/b/y2", TSDB_INT64, dir);
    add_series(db, sv, &ns, "c/z1", TSDB_BOOL, vol2);
    add_series(db, sv, &ns, "c/z2", TSDB_STATE, dir);
    add_series(db, sv, &ns, "d/w", TSDB_FLOAT64, vol2);
    add_series(db, sv, &ns, "a/b", TSDB_FLOAT32, dir); /* "a/b" is under a/ but not a/b/ */
    const char *pfn[] = {"a/", "a/b/", "c/"};
    check_tenants(db, sv, ns, pfn, 3, "empty");

    load_series(db, sv, ns, 12000, 0);
    check_tenants(db, sv, ns, pfn, 3, "after the inserts");
    OK(tsdb_optimize(db));
    check_tenants(db, sv, ns, pfn, 3, "after the merge");

    /* a delete and a replace: tombstone parts, rows the merge keeps hidden until it runs */
    OK(tsdb_delete_range(db, sv[0].id, T0 + 1000 * 60000, T0 + 3000 * 60000));
    check_tenants(db, sv, ns, pfn, 3, "after the delete");
    OK(tsdb_optimize(db));
    check_tenants(db, sv, ns, pfn, 3, "after the delete's merge");
    int64_t rts[300];
    float rv[300];
    for (int i = 0; i < 300; i++) {
        rts[i] = T0 + (4000 + i) * 60000ll;
        rv[i] = (float)i;
    }
    tsdb_values rcol = {TSDB_FLOAT32, rv, NULL};
    OK(tsdb_replace_range(db, sv[0].id, T0 + 4000 * 60000ll, T0 + 4300 * 60000ll, rts, &rcol, 300));
    check_tenants(db, sv, ns, pfn, 3, "after the replace");

    /* retention drops whole partitions: subtracted with no walk, and counted as retention_dropped_rows */
    OK(tsdb_optimize(db));
    check_tenants(db, sv, ns, pfn, 3, "settled before the drop");
    tsdb_tenant_stats before[4], after[4];
    for (int t = 0; t < 4; t++)
        OK(tsdb_tenant_stats_get(db, t ? pfn[t - 1] : NULL, TSDB_TENANT_VOLUME_SUM, &before[t]));
    OK(tsdb_drop_before(db, T0 + 2 * DAY));
    check_tenants(db, sv, ns, pfn, 3, "after drop_before");
    uint64_t dropped = 0, lost = 0;
    for (int t = 0; t < 4; t++) {
        OK(tsdb_tenant_stats_get(db, t ? pfn[t - 1] : NULL, TSDB_TENANT_VOLUME_SUM, &after[t]));
        dropped += after[t].retention_dropped_rows - before[t].retention_dropped_rows;
        lost += before[t].rows - after[t].rows;
        CHECK(after[t].rows <= before[t].rows);
    }
    CHECK(dropped == lost && dropped > 0);

    /* more rows after the drop, then a class switch is not needed: the hourly series spans partitions */
    load_series(db, sv, ns, 3000, 12000);
    check_tenants(db, sv, ns, pfn, 3, "after more inserts");
    tsdb_close(db);

    /* the open follows the parts with the list in the options: the same numbers */
    OK(tsdb_open(dir, &o, &db));
    check_tenants(db, sv, ns, pfn, 3, "after a reopen");

    /* counters since start: the reopen counts nothing as ingest, the numbers are stored ones */
    tsdb_tenant_stats ts;
    OK(tsdb_tenant_stats_get(db, "a/", TSDB_TENANT_VOLUME_SUM, &ts));
    CHECK(ts.ingested_rows == 0 && ts.merged_rows == 0 && ts.rows > 0);
    OK(tsdb_tenant_stats_get(db, "a/", 0, &ts)); /* one volume (a/b/y1 is in the second) */
    CHECK(ts.rows > 0);
    CHECK(tsdb_tenant_stats_get(db, "nope/", TSDB_TENANT_VOLUME_SUM, &ts) == TSDB_ERR_NOTFOUND);
    CHECK(tsdb_tenant_stats_get(db, "a/", 9, &ts) == TSDB_ERR_INVALID);
    char name[64];
    CHECK(tsdb_tenant_prefix(db, 0, name, sizeof name) == 2 && strcmp(name, "a/") == 0);
    CHECK(tsdb_tenant_prefix(db, 1, name, sizeof name) == 4 && strcmp(name, "a/b/") == 0);
    CHECK(tsdb_tenant_prefix(db, 3, name, sizeof name) == TSDB_ERR_NOTFOUND);

    /* ---- list changes: a sub-prefix added (a series moves from a/ to it), one removed (a/b/'s go back to a/), a
     * new top prefix for what was none, everything compared with the oracle and with a database opened fresh */
    const char *l1[] = {"a/", "a/b/", "c/", "a/b/y1", "d/"};
    OK(tsdb_set_tenants(db, l1, 5));
    check_tenants(db, sv, ns, l1, 5, "after adding a/b/y1 and d/");
    const char *l2[] = {"a/", "c/", "d/"};
    OK(tsdb_set_tenants(db, l2, 3));
    check_tenants(db, sv, ns, l2, 3, "after removing a/b/ and a/b/y1");
    OK(tsdb_tenant_stats_get(db, "c/", TSDB_TENANT_VOLUME_SUM, &ts));
    CHECK(ts.generation == 3); /* open, then two lists */
    CHECK(tsdb_tenant_stats_get(db, "a/b/", TSDB_TENANT_VOLUME_SUM, &ts) == TSDB_ERR_NOTFOUND);
    const char *l3[] = {"c/", "d/w", "a/b/y"};
    OK(tsdb_set_tenants(db, l3, 3));
    check_tenants(db, sv, ns, l3, 3, "after a wholly new list");
    const char *l3b[] = {"d/w", "c/", "a/b/y", "c/", ""}; /* order, duplicates and an empty prefix change nothing */
    OK(tsdb_set_tenants(db, l3b, 5));
    check_tenants(db, sv, ns, l3b, 5 - 2, "the same list again"); /* the oracle's list without the repeats: same */
    tsdb_close(db);

    o.tenants = l3;
    o.ntenants = 3;
    OK(tsdb_open(dir, &o, &db)); /* a fresh open with the last list gives the same numbers (compared above) */
    check_tenants(db, sv, ns, l3, 3, "a fresh open with the new list");
    tsdb_close(db);
}

/* ------------------------------------------------------------------- ingest and merges */

typedef struct opt_ctx {
    tsdb *db;
    atomic_bool stop;
} opt_ctx;

static void *optimize_loop(void *arg)
{
    opt_ctx *c = arg;
    while (!atomic_load(&c->stop)) {
        tsdb_optimize(c->db);
        usleep(1000);
    }
    return NULL;
}

static void test_ingest(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/ingest", base);
    OK(mkdir(dir, 0755));
    const char *pf[] = {"i/", "j/"};
    tsdb_options o = base_opts();
    o.tenants = pf;
    o.ntenants = 2;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    ser sv[4];
    size_t ns = 0;
    add_series(db, sv, &ns, "i/a", TSDB_FLOAT32, dir);
    add_series(db, sv, &ns, "i/b", TSDB_FLOAT64, dir);
    add_series(db, sv, &ns, "j/a", TSDB_INT64, dir);
    opt_ctx c = {db, false};
    pthread_t th;
    pthread_create(&th, NULL, optimize_loop, &c); /* merges run while parts arrive: level-0 parts leave within a tick */
    uint64_t sent[2] = {0, 0};
    for (int round = 0; round < 12; round++) {
        for (size_t i = 0; i < ns; i++) {
            size_t n = (i == 0 ? 3000 : 1500) + (size_t)round * 7;
            put_rows(db, sv[i].id, sv[i].type, T0 + round * 3 * DAY, 60000, 0, n); /* no overlap: nothing overwritten */
            sent[sv[i].name[0] == 'j'] += n;
        }
        OK(tsdb_flush(db));
        for (size_t i = 0; i < ns; i++) { /* and small writes through the WAL and a memtable flush */
            put_rows(db, sv[i].id, sv[i].type, T0 + 50 * DAY + round * 1000000, 1000, 0, 100);
            sent[sv[i].name[0] == 'j'] += 100;
        }
        OK(tsdb_flush(db));
    }
    atomic_store(&c.stop, true);
    pthread_join(th, NULL);
    OK(tsdb_stats_settle(db, 60000));
    tsdb_tenant_stats ti, tj;
    OK(tsdb_tenant_stats_get(db, "i/", TSDB_TENANT_VOLUME_SUM, &ti));
    OK(tsdb_tenant_stats_get(db, "j/", TSDB_TENANT_VOLUME_SUM, &tj));
    CHECK(ti.ingested_rows == sent[0]);
    CHECK(tj.ingested_rows == sent[1]);
    CHECK(ti.ingested_bytes > 0 && ti.ingested_bytes <= ti.ingested_rows * 16);
    CHECK(ti.merged_rows > 0 && tj.merged_rows > 0); /* the merges that ran were seen */
    CHECK(ti.merge_removed_rows == 0 && tj.merge_removed_rows == 0); /* nothing overwritten or deleted */
    check_tenants(db, sv, ns, pf, 2, "after a load with merges running");
    OK(tsdb_optimize(db));
    check_tenants(db, sv, ns, pf, 2, "after the final merge");
    OK(tsdb_tenant_stats_get(db, "i/", TSDB_TENANT_VOLUME_SUM, &ti));
    CHECK(ti.ingested_rows == sent[0]); /* a merge is not ingest */
    CHECK(ti.rows == sent[0]);
    /* overwriting rows: merges remove them, and say so */
    put_rows(db, sv[0].id, sv[0].type, T0, 60000, 0, 3000);
    OK(tsdb_optimize(db));
    check_tenants(db, sv, ns, pf, 2, "after an overwrite and its merge");
    OK(tsdb_tenant_stats_get(db, "i/", TSDB_TENANT_VOLUME_SUM, &ti));
    CHECK(ti.merge_removed_rows >= 3000);
    CHECK(ti.ingested_rows == sent[0] + 3000);
    tsdb_close(db);
}

/* ---------------------------------------------------------------- active series (HyperLogLog) */

static void test_hll(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/hll", base);
    OK(mkdir(dir, 0755));
    const char *pf[] = {"h/"};
    tsdb_options o = base_opts();
    o.tenants = pf;
    o.ntenants = 1;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    enum { N = 20000 };
    tsdb_sid *ids = malloc(N * sizeof *ids);
    int64_t *ts = malloc(N * sizeof *ts);
    float *v = malloc(N * sizeof *v);
    for (int i = 0; i < N; i++) {
        char name[32];
        snprintf(name, sizeof name, "h/s%05d", i);
        OK(tsdb_series_create(db, name, TSDB_FLOAT32, dir, &ids[i]));
        ts[i] = T0 + i;
        v[i] = (float)i;
    }
    int64_t hour = 1704067200000ll + 5 * 3600000ll + 600000;
    atomic_store(&stats_hot_clock_ms, hour);
    OK(tsdb_insert(db, ids, ts, v, N)); /* one part: every series flushed in this hour */
    OK(tsdb_stats_settle(db, 60000));
    tsdb_tenant_stats st;
    OK(tsdb_tenant_stats_get(db, "h/", TSDB_TENANT_VOLUME_SUM, &st));
    CHECK(st.series == N && st.series_created == N);
    double err = fabs((double)st.active_series_cur - N) / N;
    if (err > 0.10)
        fprintf(stderr, "  hll: %" PRIu64 " of %d\n", st.active_series_cur, N);
    CHECK(err <= 0.10);
    CHECK(st.active_series_prev == 0);
    atomic_store(&stats_hot_clock_ms, hour + 3600000); /* the next hour: 10,000 of them again */
    for (int i = 0; i < N / 2; i++)
        ts[i] = T0 + 100000 + i;
    OK(tsdb_insert(db, ids, ts, v, N / 2));
    OK(tsdb_stats_settle(db, 60000));
    OK(tsdb_tenant_stats_get(db, "h/", TSDB_TENANT_VOLUME_SUM, &st));
    err = fabs((double)st.active_series_cur - N / 2) / (N / 2);
    CHECK(err <= 0.10);
    err = fabs((double)st.active_series_prev - N) / N;
    CHECK(err <= 0.10);
    atomic_store(&stats_hot_clock_ms, hour + 3 * 3600000); /* two hours on: nothing is current or previous */
    OK(tsdb_tenant_stats_get(db, "h/", TSDB_TENANT_VOLUME_SUM, &st));
    CHECK(st.active_series_cur == 0 && st.active_series_prev == 0);
    atomic_store(&stats_hot_clock_ms, 0);
    free(ids);
    free(ts);
    free(v);
    tsdb_close(db);
}

/* -------------------------------------------------------------------------------- hot lists */

static uint64_t rng_state = 88172645463325252ull;
static uint64_t rng(void)
{
    rng_state ^= rng_state << 13;
    rng_state ^= rng_state >> 7;
    rng_state ^= rng_state << 17;
    return rng_state;
}

static int q_cb(void *ctx, const int64_t *ts, const tsdb_values *col, size_t n)
{
    (void)ctx, (void)ts, (void)col, (void)n;
    return 0;
}

static void test_hot(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/hot", base);
    OK(mkdir(dir, 0755));
    tsdb_options o = base_opts();
    o.stats_resident_s = 3600;
    tsdb *db;
    atomic_store(&stats_hot_clock_ms, T0 + 7 * 60000); /* the windows do not move during the test */
    OK(tsdb_open(dir, &o, &db));
    enum { NS = 600 };
    tsdb_sid ids[NS];
    uint64_t truth[NS];
    for (int i = 0; i < NS; i++) {
        char name[32];
        snprintf(name, sizeof name, "z%03d", i);
        OK(tsdb_series_create(db, name, TSDB_FLOAT64, dir, &ids[i]));
        put_rows(db, ids[i], TSDB_FLOAT64, T0, 1000, 0, 20);
        truth[i] = 0;
    }

    /* Zipf: series i weighs 1 / (i + 1) */
    double cum[NS], tot = 0;
    for (int i = 0; i < NS; i++)
        cum[i] = (tot += 1.0 / (i + 1));
    const int Q = 60000;
    for (int q = 0; q < Q; q++) {
        double x = (double)(rng() >> 11) / (double)(1ull << 53) * tot;
        int i = 0;
        while (i < NS - 1 && cum[i] < x)
            i++;
        OK(tsdb_query_values(db, ids[i], INT64_MIN, INT64_MAX, NULL, q_cb, NULL));
        truth[i]++;
    }
    tsdb_hot hot[256];
    size_t n = 0;
    uint64_t thr = 0;
    OK(tsdb_hot_series(db, TSDB_HOT_READS, hot, 256, &n, &thr));
    CHECK(n > 0 && n <= 256);
    int idx_of[NS];
    for (int i = 0; i < NS; i++)
        idx_of[i] = -1;
    for (size_t k = 0; k < n; k++) {
        int i = (int)(hot[k].id - ids[0]);
        if (i >= 0 && i < NS)
            idx_of[i] = (int)k;
        if (k)
            CHECK(hot[k - 1].estimate >= hot[k].estimate);
    }
    int missing = 0, off = 0;
    for (int i = 0; i < NS; i++) {
        double sigma = sqrt(7.0 * (double)(truth[i] + 1)); /* samples ~ Binomial(count, 1/8), each weighing 8 */
        if (idx_of[i] >= 0) {
            const tsdb_hot *h = &hot[idx_of[i]];
            double est = (double)h->estimate, t = (double)truth[i];
            /* high by at most err (the space-saving floor) plus sampling noise; low by sampling noise only */
            if (est < t - 5 * sigma || est > t + (double)h->error + 5 * sigma)
                off++;
        } else if ((double)truth[i] > (double)thr + 5 * sqrt(7.0 * (double)thr + 1) + 8) {
            missing++; /* an unlisted series weighs at most the threshold, up to the sampling noise */
        }
    }
    if (off || missing)
        fprintf(stderr, "  hot reads: %d estimates out of bounds, %d heavy series missing (threshold %" PRIu64 ")\n", off,
                missing, thr);
    CHECK(off == 0 && missing == 0);
    CHECK(idx_of[0] == 0 && idx_of[1] >= 0 && idx_of[1] < 3); /* the heaviest are first */
    tsdb_series_stat ss[2];
    size_t done;
    tsdb_sid two[2] = {ids[0], ids[NS - 1]};
    OK(tsdb_series_stats(db, two, 2, 0, ss, &done));
    CHECK(ss[0].reads_est == hot[0].estimate && ss[0].reads_err == hot[0].error);
    CHECK(ss[0].writes_est == 0 || ss[0].writes_est >= 20); /* listed with its rows or below the floor */

    /* a round robin over eight series: sampled evenly, never aliased with the one in eight */
    int64_t rr_before[8];
    for (int i = 0; i < 8; i++) {
        tsdb_hot dummy[256];
        size_t m;
        OK(tsdb_hot_series(db, TSDB_HOT_READS, dummy, 256, &m, NULL));
        rr_before[i] = 0;
        for (size_t k = 0; k < m; k++)
            if (dummy[k].id == ids[200 + i])
                rr_before[i] = (int64_t)dummy[k].estimate;
    }
    const int R = 8000;
    for (int q = 0; q < R; q++)
        OK(tsdb_query_values(db, ids[200 + q % 8], INT64_MIN, INT64_MAX, NULL, q_cb, NULL));
    OK(tsdb_hot_series(db, TSDB_HOT_READS, hot, 256, &n, &thr));
    for (int i = 0; i < 8; i++) {
        int64_t est = -1;
        for (size_t k = 0; k < n; k++)
            if (hot[k].id == ids[200 + i])
                est = (int64_t)hot[k].estimate - rr_before[i];
        double sigma = sqrt(7.0 * 1000);
        if (est < 1000 - 5 * sigma - (int64_t)thr || est > 1000 + 5 * sigma + (int64_t)thr)
            fprintf(stderr, "  round robin: series %d estimated %" PRId64 " of 1000 (threshold %" PRIu64 ")\n", i, est,
                    thr);
        CHECK(est >= 1000 - 5 * sigma - (int64_t)thr && est <= 1000 + 5 * sigma + (int64_t)thr);
    }

    /* the writes list: every series flushed 20 rows, so below 256 series' worth it is exact; the rest ride the floor */
    OK(tsdb_flush(db)); /* the 600 memtable flushes become one level-0 part: the sweeper feeds the list with it */
    OK(tsdb_stats_settle(db, 60000));
    OK(tsdb_hot_series(db, TSDB_HOT_WRITES, hot, 256, &n, &thr));
    CHECK(n == 256);
    for (size_t k = 0; k < n; k++)
        CHECK(hot[k].estimate >= 20 && hot[k].estimate >= hot[k].error);
    atomic_store(&stats_hot_clock_ms, 0);
    tsdb_close(db);

    /* a small database: exact writes, and bytes per row once a merge wrote full granules */
    snprintf(dir, sizeof dir, "%s/hot2", base);
    OK(mkdir(dir, 0755));
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid w[5];
    const int64_t rows[5] = {500, 4000, 100, 2500, 800};
    for (int i = 0; i < 5; i++) {
        char name[16];
        snprintf(name, sizeof name, "w%d", i);
        OK(tsdb_series_create(db, name, i % 2 ? TSDB_FLOAT64 : TSDB_INT64, dir, &w[i]));
        put_rows(db, w[i], i % 2 ? TSDB_FLOAT64 : TSDB_INT64, T0, 1000, 0, (size_t)rows[i]);
    }
    OK(tsdb_flush(db));
    OK(tsdb_stats_settle(db, 60000));
    OK(tsdb_hot_series(db, TSDB_HOT_WRITES, hot, 10, &n, &thr));
    CHECK(n == 5 && thr == 0);
    int64_t order[5] = {1, 3, 4, 0, 2}; /* by rows, most first */
    for (size_t k = 0; k < n; k++) {
        CHECK(hot[k].id == w[order[k]]);
        CHECK(hot[k].estimate == (uint64_t)rows[order[k]] && hot[k].error == 0);
    }
    OK(tsdb_optimize(db));
    OK(tsdb_stats_settle(db, 60000));
    OK(tsdb_hot_series(db, TSDB_HOT_BYTES_PER_ROW, hot, 10, &n, &thr));
    CHECK(n == 5 && thr == 0); /* every series has a full granule of 64 rows */
    n = 0;
    for (int pass = 0; pass < 2; pass++) { /* each series once, and its value the series' own bytes per row */
        OK(tsdb_hot_series(db, TSDB_HOT_BYTES_PER_ROW, hot, 10, &n, &thr));
        for (size_t k = 0; k < n; k++)
            for (size_t j = k + 1; j < n; j++)
                CHECK(hot[k].id != hot[j].id);
    }
    for (size_t k = 0; k < n; k++) {
        tsdb_series_stat st;
        OK(tsdb_series_stats(db, &hot[k].id, 1, 0, &st, &done));
        CHECK(st.rows > 0 && hot[k].estimate == (st.ts_bytes + st.value_bytes) * 1000 / st.rows);
        if (k)
            CHECK(hot[k - 1].estimate >= hot[k].estimate);
    }
    CHECK(tsdb_hot_series(db, 9, hot, 10, &n, &thr) == TSDB_ERR_INVALID);
    CHECK(tsdb_hot_series(db, TSDB_HOT_READS, hot, 257, &n, &thr) == TSDB_ERR_INVALID);
    tsdb_close(db);

    o.stats = -1;
    OK(tsdb_open(dir, &o, &db));
    CHECK(tsdb_hot_series(db, TSDB_HOT_READS, hot, 10, &n, &thr) == TSDB_ERR_INVALID);
    tsdb_tenant_stats st;
    CHECK(tsdb_tenant_stats_get(db, NULL, TSDB_TENANT_VOLUME_SUM, &st) == TSDB_ERR_INVALID);
    OK(tsdb_set_tenants(db, NULL, 0)); /* nothing counts, nothing fails */
    tsdb_close(db);
}

/* -------------------------------------------------------------------------------- residency */

static void drop_cache(const char *dir)
{
    DIR *d = opendir(dir);
    struct dirent *e;
    while (d && (e = readdir(d))) {
        if (e->d_name[0] != 'p' || !strstr(e->d_name, ".part"))
            continue;
        char path[1024];
        snprintf(path, sizeof path, "%s/%s", dir, e->d_name);
        int fd = open(path, O_RDONLY);
        if (fd >= 0) {
            fdatasync(fd);
            posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
            close(fd);
        }
    }
    if (d)
        closedir(d);
}

static void test_residency(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/resident", base);
    OK(mkdir(dir, 0755));
    const char *pf[] = {"r/"};
    tsdb_options o = base_opts();
    o.fsync = 1;
    o.granule_rows = 1024;
    o.tenants = pf;
    o.ntenants = 1;
    o.stats_resident_s = 1;
    o.stats = 0; /* lazy: nothing reads the file before the test asks */
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid id;
    OK(tsdb_series_create(db, "r/big", TSDB_FLOAT64, dir, &id));
    /* random-looking values: incompressible, so the part is large enough to span many pages */
    size_t n = 400000;
    int64_t *ts = malloc(n * sizeof *ts);
    double *v = malloc(n * sizeof *v);
    for (size_t i = 0; i < n; i++) {
        ts[i] = T0 + (int64_t)i * 1000;
        v[i] = (double)(rng() >> 11) / 1e6;
    }
    tsdb_values col = {TSDB_FLOAT64, v, NULL};
    OK(tsdb_insert_series_values(db, id, ts, &col, n));
    free(ts);
    free(v);
    tsdb_close(db);
    drop_cache(dir);
    OK(tsdb_open(dir, &o, &db));
    tsdb_series_stat st;
    size_t done;
    OK(tsdb_series_stats(db, &id, 1, TSDB_SSTAT_REQ_RESIDENT, &st, &done));
    uint64_t bytes = st.ts_bytes + st.value_bytes;
    uint64_t cold = st.resident_bytes;
    if (cold * 10 > bytes * 9) { /* a file system whose pages stay (tmpfs) or a kernel that reads it all ahead */
        printf("test_tenants: residency skipped (%" PRIu64 " of %" PRIu64 " bytes stay in the cache)\n", cold, bytes);
        tsdb_close(db);
        return;
    }
    OK(tsdb_query_values(db, id, INT64_MIN, INT64_MAX, NULL, q_cb, NULL)); /* decodes every granule */
    OK(tsdb_series_stats(db, &id, 1, TSDB_SSTAT_REQ_RESIDENT, &st, &done));
    CHECK(st.resident_bytes > cold);
    CHECK(st.resident_bytes * 10 >= bytes * 9 && st.resident_bytes <= bytes + 8192);
    /* the sampled number of the tenant follows: a sweep after the query sees it resident */
    OK(tsdb_stats_settle(db, 60000));
    uint64_t seen = 0;
    for (int tries = 0; tries < 100 && seen * 2 < bytes; tries++) {
        tsdb_tenant_stats ts2;
        OK(tsdb_tenant_stats_get(db, "r/", TSDB_TENANT_VOLUME_SUM, &ts2));
        seen = ts2.resident_bytes;
        if (seen * 2 < bytes)
            usleep(100000);
    }
    CHECK(seen * 2 >= bytes && seen <= bytes * 2);
    CHECK(tsdb_series_stats(db, &id, 1, 4, &st, &done) == TSDB_ERR_INVALID);
    tsdb_close(db);
}

/* ------------------------------------------------------------------------ lazy start and races */

typedef struct race_ctx {
    tsdb *db;
    const tsdb_sid *ids;
    int n;
    atomic_bool stop;
    atomic_int errs;
} race_ctx;

static void *race_reader(void *arg)
{
    race_ctx *c = arg;
    uint64_t x = (uint64_t)(uintptr_t)&x | 1;
    while (!atomic_load(&c->stop)) {
        x ^= x << 13, x ^= x >> 7, x ^= x << 17;
        if (tsdb_query_values(c->db, c->ids[x % (uint64_t)c->n], INT64_MIN, INT64_MAX, NULL, q_cb, NULL) != 0)
            atomic_fetch_add(&c->errs, 1);
        tsdb_hot start[1];
        size_t n;
        uint64_t thr;
        if ((x & 63) == 0 && tsdb_hot_series(c->db, TSDB_HOT_READS, start, 1, &n, &thr) != 0)
            atomic_fetch_add(&c->errs, 1);
    }
    return NULL;
}

static void *race_writer(void *arg)
{
    race_ctx *c = arg;
    for (int round = 0; !atomic_load(&c->stop); round++) {
        for (int i = 0; i < c->n && !atomic_load(&c->stop); i++) {
            int64_t ts[200];
            double v[200];
            for (int k = 0; k < 200; k++) {
                ts[k] = T0 + ((int64_t)round * 200 + k) * 1000;
                v[k] = (double)k;
            }
            tsdb_values col = {TSDB_FLOAT64, v, NULL};
            if (tsdb_insert_series_values(c->db, c->ids[i], ts, &col, 200) != 0)
                atomic_fetch_add(&c->errs, 1);
        }
    }
    return NULL;
}

/* stats = 0: the first tenant call starts the sweeper and the numbers are right; then readers, a writer and a stream of
 * list changes run together (TSan), and once they stop the numbers are the oracle's for the last list */
static void test_lazy_and_races(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/race", base);
    OK(mkdir(dir, 0755));
    tsdb_options o = base_opts();
    o.stats = 0;
    o.stats_recompute_s = 1;
    const char *l0[] = {"a/"};
    o.tenants = l0;
    o.ntenants = 1;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    enum { NS = 12 };
    ser sv[NS];
    size_t ns = 0;
    tsdb_sid ids[NS];
    for (int i = 0; i < NS; i++) {
        char name[32];
        snprintf(name, sizeof name, "%c/s%02d", "abcd"[i % 4], i);
        add_series(db, sv, &ns, name, TSDB_FLOAT64, dir);
        ids[i] = sv[i].id;
    }
    load_series(db, sv, ns, 3000, 0);
    OK(tsdb_flush(db));
    tsdb_tenant_stats ts;
    OK(tsdb_tenant_stats_get(db, "a/", TSDB_TENANT_VOLUME_SUM, &ts)); /* starts the sweeper; may be partial */
    check_tenants(db, sv, ns, l0, 1, "lazy start");
    OK(tsdb_tenant_stats_get(db, "a/", TSDB_TENANT_VOLUME_SUM, &ts));
    CHECK(ts.series == 3 && ts.rows == 9000);

    race_ctx c = {.db = db, .ids = ids, .n = NS};
    pthread_t th[5];
    for (int i = 0; i < 4; i++)
        pthread_create(&th[i], NULL, race_reader, &c);
    pthread_create(&th[4], NULL, race_writer, &c);
    const char *ls[][3] = {{"a/", "b/", "c/"}, {"b/", "d/", NULL}, {"a/", "c/s02", "d/"}, {"c/", NULL, NULL}};
    const size_t lsn[] = {3, 2, 3, 1};
    for (int r = 0; r < 24; r++) {
        OK(tsdb_set_tenants(db, ls[r % 4], lsn[r % 4]));
        struct timespec ms = {0, 20 * 1000 * 1000};
        nanosleep(&ms, NULL);
        tsdb_tenant_stats t;
        OK(tsdb_tenant_stats_get(db, NULL, TSDB_TENANT_VOLUME_SUM, &t)); /* never fails, never goes negative */
        CHECK(t.rows < (uint64_t)1 << 40 && t.series <= NS);
    }
    atomic_store(&c.stop, true);
    for (int i = 0; i < 5; i++)
        pthread_join(th[i], NULL);
    CHECK(atomic_load(&c.errs) == 0);
    OK(tsdb_set_tenants(db, ls[2], lsn[2]));
    /* the writer put rows into the series too: the oracle is the series' own numbers (check_tenants reads them) */
    check_tenants(db, sv, ns, ls[2], lsn[2], "after the race");
    tsdb_close(db);
}

/* ------------------------------------------------------------------------ the map's memory */

static void map_bench(const char *base, size_t n)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/mapbench", base);
    OK(mkdir(dir, 0755));
    const char *pf[16];
    char names[16][16];
    for (int i = 0; i < 16; i++) {
        snprintf(names[i], sizeof names[i], "t%02d/", i);
        pf[i] = names[i];
    }
    for (int layout = 0; layout < 2; layout++) { /* 0: interleaved by tenant (the Pulsar sink's order), 1: in runs */
        char d2[600];
        snprintf(d2, sizeof d2, "%s/%d", dir, layout);
        OK(mkdir(d2, 0755));
        tsdb_options o = base_opts();
        o.tenants = pf;
        o.ntenants = 16;
        o.stats = 1;
        tsdb *db;
        OK(tsdb_open(d2, &o, &db));
        struct timespec a, b;
        clock_gettime(CLOCK_MONOTONIC, &a);
        for (size_t i = 0; i < n; i++) {
            char name[64];
            size_t t = layout == 0 ? i % 16 : i / ((n + 15) / 16);
            snprintf(name, sizeof name, "t%02zu/s%zu", t, i);
            tsdb_sid id;
            OK(tsdb_series_create(db, name, TSDB_FLOAT32, d2, &id));
        }
        clock_gettime(CLOCK_MONOTONIC, &b);
        double create_s = (double)(b.tv_sec - a.tv_sec) + (double)(b.tv_nsec - a.tv_nsec) / 1e9;
        tsdb_tenant_stats st;
        clock_gettime(CLOCK_MONOTONIC, &a);
        for (int tries = 0; tries < 6000; tries++) { /* the sweeper fills the map in the background */
            OK(tsdb_tenant_stats_get(db, "t00/", TSDB_TENANT_VOLUME_SUM, &st));
            if (st.series >= n / 16)
                break;
            usleep(10000);
        }
        clock_gettime(CLOCK_MONOTONIC, &b);
        tsdb_volume_stats vs;
        OK(tsdb_get_volume_stats(db, 0, &vs));
        printf("map-bench: %zu series %s: created in %.1f s, map filled %.1f s later, map %.1f MB (%.2f bytes a series)\n",
               n, layout ? "in runs per tenant" : "interleaved", create_s,
               (double)(b.tv_sec - a.tv_sec) + (double)(b.tv_nsec - a.tv_nsec) / 1e9, (double)vs.stats_tmap_bytes / 1e6,
               (double)vs.stats_tmap_bytes / (double)n);
        tsdb_close(db);
    }
}

int main(int argc, char **argv)
{
    int bench = argc > 2 && strcmp(argv[1], "--map-bench") == 0;
    const char *scratch = bench ? (argc > 3 ? argv[3] : "/tmp") : (argc > 1 ? argv[1] : "/tmp");
    char base[400];
    snprintf(base, sizeof base, "%s/tsdb-tenants-%d", scratch, (int)getpid());
    rm_rf(base);
    if (mkdir(base, 0755) != 0) {
        perror(base);
        return 1;
    }
    if (bench) {
        map_bench(base, (size_t)strtoull(argv[2], NULL, 10));
        rm_rf(base);
        return 0;
    }
    test_oracle(base);
    test_ingest(base);
    test_hll(base);
    test_hot(base);
    test_residency(base);
    test_lazy_and_races(base);
    rm_rf(base);
    if (failures) {
        fprintf(stderr, "test_tenants: %d failures\n", failures);
        return 1;
    }
    printf("test_tenants: ok\n");
    return 0;
}
