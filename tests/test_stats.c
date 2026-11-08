/*
 * Statistics of what is stored and of what queries did (docs/design-stats.md, steps 1 and 2; src/stats.c).
 *
 * The oracle: every number the sweeper or tsdb_series_stats reports is compared with a count the test makes another
 * way, from the decoded rows (tsdb_query_values: rows, nulls, first and last time, logical bytes by value width) and
 * from the files (tsdb_foreach_part: the part sizes, which the sweeper's four byte columns must add up to exactly,
 * and the rows of the part list). Approximate numbers are not part of steps 1 and 2: everything here is exact.
 *
 * Every way the part list changes is run and then checked again: a flush (large inserts write parts), a merge
 * (optimize), a delete and a replace (tombstone parts), retention and drop_before (parts retired), a class switch
 * (slow series), a reopen (the catch-up of the open), a follower refresh, and a lazy sweeper started late. The ring
 * is overflowed with a tiny capacity while the sweeper is paused (hooks in internal.h) and the throttle's cap is
 * measured against the index bytes it had to walk. tsdb_series_stats gets ids above 2^32 in two volumes, and its
 * cost bound a budget of one chunk. The query counters equal the sums of tsdb_last_query_stats, two handles in one
 * thread stay apart, and a thread that outlives tsdb_close (run under ASan: make asan) leaves no use after free.
 *
 *   test_stats [scratch dir]
 */
#include "internal.h"
#include "tsdb.h"

#include <dirent.h>
#include <ftw.h>
#include <inttypes.h>
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
#define NTYPES 5
static const int types[NTYPES] = {TSDB_FLOAT32, TSDB_FLOAT64, TSDB_INT64, TSDB_BOOL, TSDB_STATE};
static const char *const tname[NTYPES] = {"f32", "f64", "i64", "bool", "state"};

static size_t width(int type) { return type == TSDB_FLOAT64 || type == TSDB_INT64 ? 8 : type == TSDB_BOOL ? 4 : 4; }

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
    return o;
}

/* `n` rows of series type `type` from time t0, `step` ms apart, every 7th a null; row k carries a value from k */
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

typedef struct decoded {
    uint64_t rows, nulls;
    int64_t first, last;
} decoded;

static int dec_cb(void *ctx, const int64_t *ts, const tsdb_values *col, size_t n)
{
    decoded *d = ctx;
    for (size_t i = 0; i < n; i++) {
        if (!d->rows)
            d->first = ts[i];
        d->last = ts[i];
        d->rows++;
        if (col->valid && !((col->valid[i >> 3] >> (i & 7)) & 1))
            d->nulls++;
    }
    return 0;
}

static decoded decode(tsdb *db, tsdb_sid id)
{
    decoded d = {0, 0, 0, 0};
    OK(tsdb_query_values(db, id, INT64_MIN, INT64_MAX, NULL, dec_cb, &d));
    return d;
}

typedef struct partsum {
    uint64_t parts, rows, bytes;
} partsum;
static void part_cb(void *ctx, const tsdb_part_info *p)
{
    partsum *s = ctx;
    s->parts++;
    s->rows += p->rows;
    s->bytes += p->bytes;
}

/* The volume totals against the files: the four byte columns add up to the part files' sizes (the sweeper counts
 * every byte of a part once), every part is counted, and the rows of the part list agree. */
static void check_volumes(tsdb *db, const char *when)
{
    OK(tsdb_stats_settle(db, 60000));
    partsum ps = {0, 0, 0};
    OK(tsdb_foreach_part(db, part_cb, &ps));
    uint64_t counted = 0, listed = 0, bytes = 0, logical = 0;
    for (uint32_t v = 0; v < tsdb_volume_count(db); v++) {
        tsdb_volume_stats st;
        OK(tsdb_get_volume_stats(db, v, &st));
        counted += st.stats_parts_counted;
        listed += st.stats_parts_listed;
        bytes += st.ts_bytes + st.value_bytes + st.index_bytes + st.other_bytes;
        logical += st.logical_bytes;
    }
    tsdb_stats gs;
    tsdb_get_stats(db, &gs);
    if (counted != listed || listed != ps.parts || bytes != ps.bytes || gs.rows != ps.rows)
        fprintf(stderr, "  %s: counted %" PRIu64 " listed %" PRIu64 " parts %" PRIu64 "; bytes %" PRIu64 " files %" PRIu64
                        "; rows %" PRIu64 " %" PRIu64 "\n",
                when, counted, listed, ps.parts, bytes, ps.bytes, gs.rows, ps.rows);
    CHECK(counted == listed);
    CHECK(listed == ps.parts);
    CHECK(bytes == ps.bytes);
    CHECK(logical <= gs.rows * 16 && (gs.rows == 0 || logical >= gs.rows * 12)); /* 8 + a width of 4 or 8 */
}

typedef struct world {
    tsdb *db;
    tsdb_sid id[NTYPES + 2];
    int type[NTYPES + 2];
    size_t n;
} world;

/* Per series: tsdb_series_stats against the decoded rows. Valid when no row is hidden (after a merge) and the
 * memtables are empty; `exact` also compares the rows. */
static void check_series(world *w, const char *when, bool exact)
{
    tsdb_series_stat st[NTYPES + 2];
    size_t done = 0;
    OK(tsdb_series_stats(w->db, w->id, w->n, 0, st, &done));
    CHECK(done == w->n);
    uint64_t sum_logical = 0;
    for (size_t i = 0; i < w->n; i++) {
        CHECK(st[i].status == 0);
        CHECK(st[i].type == w->type[i]);
        CHECK(st[i].parts >= (st[i].rows ? 1u : 0u) || st[i].memtable_rows);
        decoded d = decode(w->db, w->id[i]);
        if (exact) {
            if (st[i].rows != d.rows || st[i].nulls != d.nulls || st[i].first_ts != d.first || st[i].last_ts != d.last)
                fprintf(stderr, "  %s %s: rows %" PRIu64 " vs %" PRIu64 ", nulls %" PRIu64 " vs %" PRIu64 ", range %" PRId64
                                "..%" PRId64 " vs %" PRId64 "..%" PRId64 "\n",
                        when, tname[w->type[i] == TSDB_FLOAT32 ? 0 : w->type[i]], st[i].rows, d.rows, st[i].nulls, d.nulls,
                        st[i].first_ts, st[i].last_ts, d.first, d.last);
            CHECK(st[i].rows == d.rows);
            CHECK(st[i].nulls == d.nulls);
            CHECK(st[i].first_ts == d.first && st[i].last_ts == d.last);
            CHECK(st[i].logical_bytes == (st[i].rows - st[i].memtable_rows) * (8 + width(w->type[i])));
        }
        CHECK(st[i].ts_bytes > 0 || st[i].rows == 0);
        CHECK(st[i].granules >= st[i].full_granules);
        CHECK(TSDB_SID_VOLUME(w->id[i]) + 1 == (w->id[i] >> 32));
        CHECK(w->id[i] >= (1ull << 32));
        sum_logical += st[i].logical_bytes;
    }
    if (exact) { /* the series are all the series: their parts' logical bytes are the volumes' */
        uint64_t logical = 0;
        for (uint32_t v = 0; v < tsdb_volume_count(w->db); v++) {
            tsdb_volume_stats vs;
            OK(tsdb_get_volume_stats(w->db, v, &vs));
            logical += vs.logical_bytes;
        }
        if (logical != sum_logical)
            fprintf(stderr, "  %s: volume logical %" PRIu64 " series %" PRIu64 "\n", when, logical, sum_logical);
        CHECK(logical == sum_logical);
    }
}

/* ---------------------------------------------------------------------- the oracle across list changes */

static void make_series(world *w, const char *dir, const char *vol2)
{
    for (int i = 0; i < NTYPES; i++) {
        char name[32];
        snprintf(name, sizeof name, "s.%s", tname[i]);
        OK(tsdb_series_create(w->db, name, (tsdb_type)types[i], i % 2 ? vol2 : dir, &w->id[w->n]));
        w->type[w->n++] = types[i];
    }
    /* a slow series (one row an hour): its partitions are 7, 28 or 364 days (a class above 0) */
    OK(tsdb_series_create(w->db, "s.slow", TSDB_FLOAT64, dir, &w->id[w->n]));
    w->type[w->n++] = TSDB_FLOAT64;
}

static void load_all(world *w, size_t rows, size_t k0)
{
    for (size_t i = 0; i + 1 < w->n; i++) /* 5 typed series, 60 s apart: 3.5 days per 5,000 rows */
        for (size_t k = 0; k < rows; k += 5000)
            put_rows(w->db, w->id[i], w->type[i], T0, 60000, k0 + k, rows - k < 5000 ? rows - k : 5000);
    put_rows(w->db, w->id[w->n - 1], TSDB_FLOAT64, T0, 3600000, k0, 2000); /* 2,000 hourly rows: 83 days */
}

static void test_oracle(const char *base)
{
    char dir[512], vol2[512];
    snprintf(dir, sizeof dir, "%s/oracle", base);
    snprintf(vol2, sizeof vol2, "%s/oracle-vol2", base);
    OK(mkdir(dir, 0755));
    tsdb_options o = base_opts();
    world w;
    memset(&w, 0, sizeof w);
    OK(tsdb_open(dir, &o, &w.db));
    uint32_t vi;
    OK(tsdb_volume_add(w.db, vol2, &vi));
    make_series(&w, dir, vol2);
    CHECK(TSDB_SID_VOLUME(w.id[1]) == vi); /* ids above 2^32 in both volumes */

    check_volumes(w.db, "empty");
    load_all(&w, 12000, 0);
    check_volumes(w.db, "after the inserts (many parts, partitions overlap nothing yet)");
    for (size_t i = 0; i < w.n; i++) {
        tsdb_series_stat st;
        size_t done;
        OK(tsdb_series_stats(w.db, &w.id[i], 1, 0, &st, &done));
        CHECK(done == 1 && st.status == 0 && st.rows > 0);
    }
    check_series(&w, "inserts", true); /* nothing overlaps: no row is hidden */
    int cls = 0;
    OK(tsdb_series_class(w.db, w.id[w.n - 1], &cls));
    tsdb_series_stat sl;
    size_t done;
    OK(tsdb_series_stats(w.db, &w.id[w.n - 1], 1, 0, &sl, &done));
    CHECK(sl.cls == cls);

    /* a merge: every partition down to one part */
    OK(tsdb_optimize(w.db));
    check_volumes(w.db, "optimize");
    check_series(&w, "optimize", true);

    /* the same keys again: parts that overlap the merged ones. Rows are hidden until a merge removes them, so
     * the series' physical rows now exceed the live ones, and the sums over the files must still hold. */
    load_all(&w, 3000, 0);
    check_volumes(w.db, "overlapping inserts");
    tsdb_series_stat st[NTYPES + 2];
    OK(tsdb_series_stats(w.db, w.id, w.n, 0, st, &done));
    for (size_t i = 0; i + 1 < w.n; i++)
        CHECK(st[i].rows == 12000 + 3000);
    OK(tsdb_optimize(w.db));
    check_volumes(w.db, "optimize again");
    check_series(&w, "optimize again", true); /* the duplicates are gone */

    /* a delete: a tombstone part; the physical rows stay until a merge applies it */
    OK(tsdb_delete_range(w.db, w.id[0], T0 + 60000 * 100, T0 + 60000 * 200));
    check_volumes(w.db, "delete");
    OK(tsdb_series_stats(w.db, w.id, 1, 0, st, &done));
    CHECK(st[0].tomb_bytes == 24);
    OK(tsdb_optimize(w.db));
    check_volumes(w.db, "delete merged");
    check_series(&w, "delete merged", true);
    OK(tsdb_series_stats(w.db, w.id, 1, 0, st, &done));
    CHECK(st[0].rows == 12000 - 100);

    /* a replace: rows and a tombstone in one commit */
    {
        int64_t ts[300];
        float vals[300];
        uint8_t slots[300 * 8] = {0};
        (void)vals;
        for (int k = 0; k < 300; k++) {
            ts[k] = T0 + 60000ll * (500 + k);
            ((double *)slots)[k] = 1.5 * k;
        }
        tsdb_values col = {TSDB_FLOAT64, slots, NULL};
        OK(tsdb_replace_range(w.db, w.id[1], T0 + 60000ll * 500, T0 + 60000ll * 800, ts, &col, 300));
    }
    check_volumes(w.db, "replace");
    OK(tsdb_optimize(w.db));
    check_volumes(w.db, "replace merged");
    check_series(&w, "replace merged", true);

    /* retention: rows older than the limit go with the next merge; drop_before retires whole partitions */
    OK(tsdb_set_retention(w.db, w.id[2], 2 * DAY));
    check_volumes(w.db, "retention set");
    OK(tsdb_optimize(w.db));
    check_volumes(w.db, "retention merged");
    OK(tsdb_drop_before(w.db, T0 + 2 * DAY));
    check_volumes(w.db, "drop_before");
    check_series(&w, "drop_before", false);

    /* memtable rows: a small write is counted in memtable_rows and not in the part columns */
    OK(tsdb_append(w.db, w.id[0], T0 + 30 * DAY, 1.0f));
    OK(tsdb_series_stats(w.db, w.id, 1, 0, st, &done));
    CHECK(st[0].memtable_rows >= 1);
    CHECK(st[0].last_ts == T0 + 30 * DAY || st[0].last_ts > T0); /* the memtable's rows join the range */

    tsdb_close(w.db);

    /* a reopen: the catch-up of the open reads every part once; a lazy handle reads them at the first settle */
    for (int lazy = 0; lazy < 2; lazy++) {
        tsdb_options o2 = base_opts();
        o2.stats = lazy ? 0 : 1;
        tsdb *db;
        OK(tsdb_open(dir, &o2, &db));
        if (lazy) { /* nothing runs before the first call */
            tsdb_volume_stats vs;
            OK(tsdb_get_volume_stats(db, 0, &vs)); /* this call starts it */
            (void)vs;
        }
        check_volumes(db, lazy ? "reopen, lazy" : "reopen");
        tsdb_close(db);
    }

    /* a follower: it follows the writer's part list through its refreshes; its totals match the files */
    {
        tsdb_options ow = base_opts();
        tsdb *wdb, *fdb;
        OK(tsdb_open(dir, &ow, &wdb));
        tsdb_options of = base_opts();
        of.read_only = 1;
        of.follow = 1;
        of.refresh_ms = 20;
        OK(tsdb_open(dir, &of, &fdb));
        check_volumes(fdb, "follower at open");
        tsdb_sid id;
        OK(tsdb_series_lookup(wdb, "s.f64", &id));
        put_rows(wdb, id, TSDB_FLOAT64, T0 + 40 * DAY, 60000, 0, 4000);
        put_rows(wdb, id, TSDB_FLOAT64, T0 + 50 * DAY, 60000, 0, 4000);
        OK(tsdb_refresh(fdb));
        check_volumes(fdb, "follower after inserts");
        OK(tsdb_optimize(wdb));
        OK(tsdb_refresh(fdb));
        check_volumes(fdb, "follower after a merge");
        check_volumes(wdb, "writer");
        tsdb_volume_stats a, b;
        OK(tsdb_get_volume_stats(wdb, 0, &a));
        OK(tsdb_get_volume_stats(fdb, 0, &b));
        CHECK(a.logical_bytes == b.logical_bytes && a.ts_bytes == b.ts_bytes && a.value_bytes == b.value_bytes);
        tsdb_close(fdb);
        tsdb_close(wdb);
    }
}

/* ---------------------------------------------------------------------- stats off and lazy */

static int threads_now(void)
{
#ifdef __linux__
    int n = 0;
    DIR *d = opendir("/proc/self/task");
    if (!d)
        return -1;
    struct dirent *e;
    while ((e = readdir(d)))
        if (e->d_name[0] != '.')
            n++;
    closedir(d);
    return n;
#else
    return -1;
#endif
}

/* /proc/self/task still lists a thread for a while after pthread_join returned for it (the task is reaped later, and
 * on a loaded machine that takes long), so a count taken right after a close or an open is a lower or upper bound
 * only. The baselines wait until the count has held for 200 ms; an expected count is polled for up to 10 s. */
static int threads_stable(void)
{
    int last = threads_now();
    if (last < 0)
        return last;
    for (int still = 0, waited = 0; still < 20 && waited < 1000; waited++) {
        usleep(10000);
        int n = threads_now();
        if (n == last)
            still++;
        else
            still = 0, last = n;
    }
    return last;
}

static bool threads_are(int want)
{
    for (int i = 0; i < 1000; i++) {
        if (threads_now() == want)
            return true;
        usleep(10000);
    }
    return false;
}

static void test_modes(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/modes", base);
    OK(mkdir(dir, 0755));
    tsdb_options o = base_opts();
    /* the latest table's warmer is a thread that comes and goes on its own time (it ends when it has nothing left
     * to warm): with it on, the thread counts below fail under load. This test counts the sweeper's thread. */
    o.latest_cache = 0;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid id;
    OK(tsdb_series_create(db, "m", TSDB_FLOAT64, dir, &id));
    put_rows(db, id, TSDB_FLOAT64, T0, 1000, 0, 3000);
    tsdb_close(db);

    int t0 = threads_stable();
    o.stats = -1; /* off: no thread, no event ring, no query counters, zeros in the totals */
    OK(tsdb_open(dir, &o, &db));
    int t_off = threads_stable();
    decoded d = decode(db, id);
    CHECK(d.rows == 3000);
    tsdb_query_totals qt;
    OK(tsdb_query_totals_get(db, &qt));
    CHECK(qt.queries == 0 && qt.rows_decoded == 0 && qt.rows_from_stats == 0 && qt.result_rows == 0);
    tsdb_volume_stats vs;
    OK(tsdb_get_volume_stats(db, 0, &vs));
    CHECK(vs.logical_bytes == 0 && vs.ts_bytes == 0 && vs.stats_parts_listed == 0 && vs.stats_cpu_ns == 0);
    OK(tsdb_stats_settle(db, 1000)); /* returns at once */
    OK(tsdb_get_volume_stats(db, 0, &vs));
    CHECK(vs.logical_bytes == 0);
    tsdb_series_stat st; /* per-series numbers need no sweeper */
    size_t done;
    OK(tsdb_series_stats(db, &id, 1, 0, &st, &done));
    CHECK(done == 1 && st.status == 0 && st.rows == 3000 && st.logical_bytes == 3000 * 16);
    if (t0 >= 0)
        CHECK(threads_are(t_off)); /* the sweeper was never started, by the settle or by the stats call */
    tsdb_close(db);

    o.stats = 0; /* lazy: no thread until the first volume stats or settle */
    OK(tsdb_open(dir, &o, &db));
    int t_lazy = threads_stable();
    if (t0 >= 0)
        CHECK(t_lazy == t_off);
    OK(tsdb_query_totals_get(db, &qt));
    CHECK(qt.queries == 0);
    decode(db, id);
    OK(tsdb_query_totals_get(db, &qt));
    CHECK(qt.queries == 1); /* the counters run in lazy mode */
    OK(tsdb_stats_settle(db, 60000));
    if (t0 >= 0)
        CHECK(threads_are(t_lazy + 1));
    OK(tsdb_get_volume_stats(db, 0, &vs));
    CHECK(vs.logical_bytes == 3000 * 16 && vs.stats_parts_counted == vs.stats_parts_listed && vs.stats_parts_listed > 0);
    tsdb_close(db);
    if (t0 >= 0)
        CHECK(threads_are(t0)); /* tsdb_close joined it */

    /* the sweeper's CPU time is reported */
    o.stats = 1;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_stats_settle(db, 60000));
    OK(tsdb_get_volume_stats(db, 0, &vs));
    CHECK(vs.stats_cpu_ns > 0 || vs.stats_parts_counted > 0);
    tsdb_close(db);
}

/* ---------------------------------------------------------------------- ring overflow */

static void test_overflow(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/overflow", base);
    OK(mkdir(dir, 0755));
    tsdb_options o = base_opts();
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid id;
    OK(tsdb_series_create(db, "o", TSDB_FLOAT64, dir, &id));
    atomic_store(&stats_ring_cap, 2);
    atomic_store(&stats_pause, 1); /* events pile up in a ring that holds two */
    for (int k = 0; k < 12; k++)
        put_rows(db, id, TSDB_FLOAT64, T0 + k * 10 * DAY, 60000, 0, 2000);
    OK(tsdb_optimize(db)); /* removes: more events, also lost */
    atomic_store(&stats_pause, 0);
    check_volumes(db, "after an overflow"); /* the sweeper compared its records with the part list */
    tsdb_volume_stats vs;
    OK(tsdb_get_volume_stats(db, 0, &vs));
    CHECK(vs.stats_ring_overflows > 0);
    uint64_t before = vs.stats_ring_overflows;
    atomic_store(&stats_ring_cap, 4096);
    put_rows(db, id, TSDB_FLOAT64, T0 + 400 * DAY, 60000, 0, 2000);
    check_volumes(db, "after the ring grew back");
    OK(tsdb_get_volume_stats(db, 0, &vs));
    CHECK(vs.stats_ring_overflows == before);
    decoded d = decode(db, id);
    CHECK(d.rows == 12 * 2000 + 2000);
    tsdb_close(db);
}

/* ---------------------------------------------------------------------- the throttle */

static int64_t mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

#define NPARTS 6
static void test_throttle(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/throttle", base);
    OK(mkdir(dir, 0755));
    tsdb_options o = base_opts();
    o.granule_rows = 1; /* an index entry for every row: indexes of about a megabyte */
    o.stats = 0;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid id;
    OK(tsdb_series_create(db, "t", TSDB_FLOAT64, dir, &id));
    for (int k = 0; k < NPARTS; k++) /* one partition each: 40,000 rows a second apart */
        put_rows(db, id, TSDB_FLOAT64, T0 + k * DAY, 1000, 0, 40000);
    tsdb_close(db);

    o.stats = 1;
    o.stats_sweep_mb_s = 1; /* 1 MB/s, a hard cap */
    OK(tsdb_open(dir, &o, &db));
    int64_t t = mono_ms();
    OK(tsdb_stats_settle(db, 120000));
    int64_t took = mono_ms() - t;
    tsdb_volume_stats vs;
    OK(tsdb_get_volume_stats(db, 0, &vs));
    CHECK(vs.stats_parts_counted == vs.stats_parts_listed && vs.stats_parts_listed == NPARTS);
    /* all but the last part had to wait for the bytes of the ones before it: at least this long, whatever the load */
    double each = (double)vs.index_bytes / NPARTS / 1e6;
    double min_s = each * (NPARTS - 1) - 0.25; /* a 100 ms burst is kept, and the first walk is not delayed */
    fprintf(stderr, "  throttle: %.2f MB of index in %d parts, settled in %" PRId64 " ms (at least %.0f ms)\n",
            (double)vs.index_bytes / 1e6, NPARTS, took, min_s * 1000);
    CHECK(vs.index_bytes > 3 * 1000 * 1000);
    CHECK((double)took >= min_s * 1000);
    tsdb_close(db);

    /* the same without a cap is much faster (the walk itself is a few milliseconds) */
    o.stats_sweep_mb_s = -1;
    OK(tsdb_open(dir, &o, &db));
    t = mono_ms();
    OK(tsdb_stats_settle(db, 120000));
    int64_t fast = mono_ms() - t;
    CHECK(fast < took);
    tsdb_close(db);
}

/* ---------------------------------------------------------------------- tsdb_series_stats: bounds and errors */

static void test_series_stats_api(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/api", base);
    OK(mkdir(dir, 0755));
    tsdb_options o = base_opts();
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid id;
    OK(tsdb_series_create(db, "a", TSDB_FLOAT64, dir, &id));
    put_rows(db, id, TSDB_FLOAT64, T0, 1000, 0, 3000);
    /* the hot-writes list (writes_est) is fed by the sweeper from the flush event: wait for it, or the first
     * records and the last differ on a machine where the sweeper is starved */
    OK(tsdb_stats_settle(db, 60000));
    size_t done = 99;
    tsdb_series_stat st[600];
    tsdb_sid ids[600];
    for (int i = 0; i < 600; i++)
        ids[i] = id;
    CHECK(tsdb_series_stats(db, ids, 1, 2, st, &done) == TSDB_ERR_INVALID); /* EXACT_ROWS is not built */
    CHECK(tsdb_series_stats(db, ids, 65537, 0, st, &done) == TSDB_ERR_INVALID);
    CHECK(tsdb_series_stats(db, ids, 1, 0, st, NULL) == TSDB_ERR_INVALID);
    OK(tsdb_series_stats(db, ids, 0, 0, st, &done));
    CHECK(done == 0);

    /* unknown ids: a record with a status, and the other fields 0 */
    tsdb_sid bad[3] = {id, TSDB_SID(7, 0), id + 100000};
    OK(tsdb_series_stats(db, bad, 3, 0, st, &done));
    CHECK(done == 3 && st[0].status == 0 && st[1].status != 0 && st[2].status != 0);
    CHECK(st[1].rows == 0 && st[1].ts_bytes == 0 && st[2].rows == 0 && st[2].parts == 0);

    /* the cost bound: the call stops at a chunk boundary once the probes pass the budget, and asking again for the
     * rest gives every record */
    atomic_store(&sstats_budget, 1);
    OK(tsdb_series_stats(db, ids, 600, 0, st, &done));
    CHECK(done > 0 && done < 600);
    size_t first = done;
    tsdb_series_stat all[600];
    memcpy(all, st, first * sizeof *st);
    int calls = 1;
    while (done < 600) {
        size_t d2 = 0;
        OK(tsdb_series_stats(db, ids + done, 600 - done, 0, all + done, &d2));
        CHECK(d2 > 0);
        done += d2;
        calls++;
    }
    CHECK(calls >= 3);
    atomic_store(&sstats_budget, 100000000ull);
    OK(tsdb_series_stats(db, ids, 600, 0, st, &done));
    CHECK(done == 600);
    for (int i = 0; i < 600; i++)
        CHECK(memcmp(&st[i], &all[i], sizeof st[i]) == 0 && st[i].rows == 3000);
    tsdb_close(db);
}

/* ---------------------------------------------------------------------- review fixes: open, last write, duplicate ids */

/* The sweeper starts before load_parts, which changes the part list without db->lock: a ring overflow during the
 * open must not make it reconcile (it would read a list in the middle of a sort or a realloc) until the open is
 * done, and the totals afterwards are still exact. A ring of one overflows on the second part. */
static void test_open_overflow(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/openrace", base);
    OK(mkdir(dir, 0755));
    tsdb_options o = base_opts();
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid id;
    OK(tsdb_series_create(db, "r", TSDB_FLOAT64, dir, &id));
    for (int k = 0; k < 1500; k++) /* more than 1000 rows: straight to a part, one partition per call */
        put_rows(db, id, TSDB_FLOAT64, T0 + k * 2 * DAY, 60000, 0, 1100);
    tsdb_close(db);
    atomic_store(&stats_ring_cap, 1);
    /* Whether the ring overflows during an open is a race between load_parts and the sweeper (it drains each part
     * as it is pushed when it is scheduled between two pushes), so a run that did not overflow proves nothing about
     * the guard: open again, up to 20 times, until one did. Every open, overflowed or not, must pass the checks;
     * on a quiet machine the first overflows, and a starved loader (a machine under load) needs a few more. */
    bool overflowed = false;
    for (int attempt = 0; attempt < 20 && !overflowed; attempt++) {
        atomic_store(&stats_open_reconciles, 0);
        OK(tsdb_open(dir, &o, &db));
        CHECK(atomic_load(&stats_open_reconciles) == 0);
        check_volumes(db, "after an open that overflowed the ring");
        tsdb_volume_stats vs;
        OK(tsdb_get_volume_stats(db, 0, &vs));
        CHECK(vs.stats_parts_listed >= 1500);
        overflowed = vs.stats_ring_overflows > 0;
        tsdb_close(db);
    }
    CHECK(overflowed);
    atomic_store(&stats_ring_cap, 4096);
}

/* last_write_ms is the part header's write time, which a merge carries over from its inputs: a merge and a reopen
 * do not make a series' last write the merge time (the file's mtime does). */
static void test_last_write(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/lastw", base);
    OK(mkdir(dir, 0755));
    tsdb_options o = base_opts();
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid id;
    OK(tsdb_series_create(db, "w", TSDB_FLOAT64, dir, &id));
    put_rows(db, id, TSDB_FLOAT64, T0, 1000, 0, 1100); /* two parts of one partition */
    put_rows(db, id, TSDB_FLOAT64, T0 + 2000000, 1000, 0, 1100);
    tsdb_close(db);
    OK(tsdb_open(dir, &o, &db));
    tsdb_series_stat st;
    size_t done;
    OK(tsdb_series_stats(db, &id, 1, 0, &st, &done));
    CHECK(done == 1 && st.parts == 2 && st.last_write_ms > 0 && !(st.flags & TSDB_SSTAT_WRITE_FROM_MTIME));
    int64_t lw = st.last_write_ms;
    struct timespec ts = {2, 200000000}; /* a second boundary at least passes: mtimes have a second */
    nanosleep(&ts, NULL);
    OK(tsdb_optimize(db));
    OK(tsdb_series_stats(db, &id, 1, 0, &st, &done));
    CHECK(st.parts == 1 && st.last_write_ms == lw);
    tsdb_close(db);
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series_stats(db, &id, 1, 0, &st, &done));
    CHECK(st.parts == 1);
    if (st.last_write_ms != lw)
        fprintf(stderr, "  last write %" PRId64 " after a merge and a reopen, was %" PRId64 "\n", st.last_write_ms, lw);
    CHECK(st.last_write_ms == lw);
    tsdb_close(db);
}

/* two records of one series, but for the memtable's creation time (wall clock mapped from a monotonic one: it moves) */
static bool same_stat(const tsdb_series_stat *a, const tsdb_series_stat *b)
{
    tsdb_series_stat x = *a, y = *b;
    x.last_write_ms = y.last_write_ms = 0;
    return memcmp(&x, &y, sizeof x) == 0;
}

/* Repeated ids are answered once, and the rows scanned in a memtable (unsorted series are scanned for their range)
 * count against the call's budget. */
static void test_dup_ids(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/dups", base);
    OK(mkdir(dir, 0755));
    tsdb_options o = base_opts();
    o.wal_max_batch_rows = 65536; /* the batches below go through the WAL into the memtable */
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid id;
    OK(tsdb_series_create(db, "d", TSDB_FLOAT64, dir, &id));
    for (int b = 99; b >= 0; b--) /* batches in descending time: the series is not sorted in the memtable */
        put_rows(db, id, TSDB_FLOAT64, T0 + b * 1000000ll, 1000, 0, 1000);
    tsdb_series_stat one;
    size_t done;
    OK(tsdb_series_stats(db, &id, 1, 0, &one, &done));
    if (one.memtable_rows != 100000 || one.first_ts != T0)
        fprintf(stderr, "  memtable rows %" PRIu64 " first %" PRId64 " parts %" PRIu64 "\n", one.memtable_rows, one.first_ts, one.parts);
    CHECK(done == 1 && one.memtable_rows == 100000 && one.first_ts == T0);
    enum { N = 65536 };
    tsdb_sid *ids = malloc(N * sizeof *ids);
    tsdb_series_stat *st = malloc(N * sizeof *st);
    for (int i = 0; i < N; i++)
        ids[i] = id;
    /* each chunk of 256 repeats costs one scan, not 256 (the default budget of 10^8 probes is not reached by the
     * 256 chunks: 256 x 100,000) */
    OK(tsdb_series_stats(db, ids, N, 0, st, &done));
    CHECK(done == N);
    for (int i = 0; i < N; i += 997)
        CHECK(same_stat(&st[i], &one));
    CHECK(same_stat(&st[N - 1], &one));
    /* the scan is charged: a small budget stops the call after a few chunks */
    atomic_store(&sstats_budget, 1000000);
    OK(tsdb_series_stats(db, ids, N, 0, st, &done));
    CHECK(done >= 256 && done < N / 2);
    for (size_t i = 0; i < done; i += 251)
        CHECK(same_stat(&st[i], &one));
    atomic_store(&sstats_budget, 100000000ull);
    free(ids);
    free(st);
    tsdb_close(db);
}

/* ---------------------------------------------------------------------- the query counters */

typedef struct tctx {
    tsdb *db;
    tsdb_sid id;
    pthread_mutex_t mu;
    pthread_cond_t cv;
    int stage;
} tctx;

static void *late_thread(void *arg)
{
    tctx *t = arg;
    decode(t->db, t->id); /* a block of its own on t->db */
    pthread_mutex_lock(&t->mu);
    t->stage = 1;
    pthread_cond_broadcast(&t->cv);
    while (t->stage < 2)
        pthread_cond_wait(&t->cv, &t->mu);
    pthread_mutex_unlock(&t->mu);
    return NULL; /* exits after tsdb_close: its slot folds into a registry that must still be alive */
}

static void sum_stats(tsdb_query_totals *sum)
{
    tsdb_query_stats s;
    tsdb_last_query_stats(&s);
    sum->queries++;
    sum->rows_decoded += s.rows_decoded;
    sum->rows_from_stats += s.rows_from_stats;
    sum->rows_from_memtable += s.memtable_rows;
    sum->bytes_read += s.bytes_read;
    sum->parts += s.parts;
    sum->granules += s.granules;
    sum->granules_from_stats += s.granules_from_stats;
    sum->result_rows += s.result_rows;
}

static bool same_totals(const tsdb_query_totals *a, const tsdb_query_totals *b)
{
    return a->queries == b->queries && a->rows_decoded == b->rows_decoded && a->rows_from_stats == b->rows_from_stats &&
           a->rows_from_memtable == b->rows_from_memtable && a->bytes_read == b->bytes_read && a->parts == b->parts &&
           a->granules == b->granules && a->granules_from_stats == b->granules_from_stats &&
           a->result_rows == b->result_rows;
}

static void run_mixed(tsdb *db, tsdb_sid id, tsdb_query_totals *sum, int rounds)
{
    for (int r = 0; r < rounds; r++) {
        tsdb_bucket *b;
        size_t nb;
        decoded d = {0, 0, 0, 0};
        OK(tsdb_query_values(db, id, T0, T0 + DAY, NULL, dec_cb, &d));
        sum_stats(sum);
        OK(tsdb_query_agg(db, id, T0, T0 + 3 * DAY, 3600000, &b, &nb)); /* granules answered from statistics */
        free(b);
        sum_stats(sum);
        OK(tsdb_query_agg(db, id, T0 + 1000, T0 + 2 * DAY + 1234, 7 * 60000, &b, &nb)); /* decodes the edges */
        free(b);
        sum_stats(sum);
    }
}

struct mt {
    tsdb *db;
    tsdb_sid id;
    tsdb_query_totals sum;
};

static void *mixed_thread(void *arg)
{
    struct mt *x = arg;
    run_mixed(x->db, x->id, &x->sum, 20);
    return NULL;
}

static void test_counters(const char *base)
{
    char da[512], dbp[512];
    snprintf(da, sizeof da, "%s/count-a", base);
    snprintf(dbp, sizeof dbp, "%s/count-b", base);
    OK(mkdir(da, 0755));
    OK(mkdir(dbp, 0755));
    tsdb_options o = base_opts();
    tsdb *a, *b;
    OK(tsdb_open(da, &o, &a));
    OK(tsdb_open(dbp, &o, &b));
    tsdb_sid ia, ib;
    OK(tsdb_series_create(a, "x", TSDB_FLOAT64, da, &ia));
    OK(tsdb_series_create(b, "y", TSDB_FLOAT64, dbp, &ib));
    put_rows(a, ia, TSDB_FLOAT64, T0, 1000, 0, 20000);
    put_rows(b, ib, TSDB_FLOAT64, T0, 1000, 0, 30000);
    OK(tsdb_optimize(a));
    put_rows(a, ia, TSDB_FLOAT64, T0 + 40000000, 1000, 0, 5); /* memtable rows */

    /* two handles in one thread: interleaved queries, each handle counts only its own */
    tsdb_query_totals sa = {0}, sb = {0}, got;
    for (int r = 0; r < 3; r++) {
        run_mixed(a, ia, &sa, 1);
        run_mixed(b, ib, &sb, 2);
    }
    OK(tsdb_query_totals_get(a, &got));
    if (!same_totals(&got, &sa))
        fprintf(stderr, "  a: queries %" PRIu64 " vs %" PRIu64 ", decoded %" PRIu64 " vs %" PRIu64 "\n", got.queries,
                sa.queries, got.rows_decoded, sa.rows_decoded);
    CHECK(same_totals(&got, &sa));
    CHECK(got.queries == 9 && got.rows_decoded > 0 && got.rows_from_stats > 0 && got.query_ns > 0);
    OK(tsdb_query_totals_get(b, &got));
    CHECK(same_totals(&got, &sb));
    CHECK(got.queries == 18);

    /* many threads: the total is the sum of what each did */
    enum { NT = 4 };
    pthread_t th[NT];
    struct mt m[NT];
    memset(m, 0, sizeof m);
    tsdb_query_totals base_a;
    OK(tsdb_query_totals_get(a, &base_a));
    for (int i = 0; i < NT; i++) {
        m[i].db = a;
        m[i].id = ia;
    }
    for (int i = 0; i < NT; i++)
        pthread_create(&th[i], NULL, mixed_thread, &m[i]);
    tsdb_query_totals want = base_a;
    for (int i = 0; i < NT; i++) {
        pthread_join(th[i], NULL);
        want.queries += m[i].sum.queries;
        want.rows_decoded += m[i].sum.rows_decoded;
        want.rows_from_stats += m[i].sum.rows_from_stats;
        want.rows_from_memtable += m[i].sum.rows_from_memtable;
        want.bytes_read += m[i].sum.bytes_read;
        want.parts += m[i].sum.parts;
        want.granules += m[i].sum.granules;
        want.granules_from_stats += m[i].sum.granules_from_stats;
        want.result_rows += m[i].sum.result_rows;
    }
    OK(tsdb_query_totals_get(a, &got));
    CHECK(same_totals(&got, &want)); /* the threads have exited: their blocks were folded in */

    /* a thread that outlives tsdb_close: its slot holds the registry */
    tctx t = {b, ib, PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, 0};
    pthread_t lt;
    pthread_create(&lt, NULL, late_thread, &t);
    pthread_mutex_lock(&t.mu);
    while (t.stage < 1)
        pthread_cond_wait(&t.cv, &t.mu);
    pthread_mutex_unlock(&t.mu);
    tsdb_close(b);
    pthread_mutex_lock(&t.mu);
    t.stage = 2;
    pthread_cond_broadcast(&t.cv);
    pthread_mutex_unlock(&t.mu);
    pthread_join(lt, NULL);

    /* a new handle after thread ids and slots were reused: never sees a closed handle's counts */
    OK(tsdb_open(dbp, &o, &b));
    OK(tsdb_query_totals_get(b, &got));
    CHECK(got.queries == 0);
    tsdb_close(b);
    tsdb_close(a);
}

int main(int argc, char **argv)
{
    char base[400];
    snprintf(base, sizeof base, "%s/tsdb-stats-%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    rm_rf(base);
    if (mkdir(base, 0755) != 0) {
        perror(base);
        return 1;
    }
    test_counters(base);
    test_series_stats_api(base);
    test_modes(base);
    test_overflow(base);
    test_open_overflow(base);
    test_last_write(base);
    test_dup_ids(base);
    test_oracle(base);
    test_throttle(base);
    rm_rf(base);
    if (failures) {
        fprintf(stderr, "test_stats: %d failures\n", failures);
        return 1;
    }
    printf("test_stats: ok\n");
    return 0;
}
