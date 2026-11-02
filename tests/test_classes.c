/* Partition classes (docs/design.md): a series slower than about one row per 1.3 s gets partitions of 7, 28 or 364
 * days, chosen from its rate. Checks, each against a model of what the series hold:
 *   1. a bulk load of every generator kind lands in its class, hdd_temp at <= 0.285 bytes per reading (0.268 with
 *      granules cut at days, 0.326 with partition_classes = 0), and no kind is larger than with classes off by more
 *      than 0.5%;
 *   2. a slow series fed by appends starts in class 0 and moves after an idle merge, at T after its newest row;
 *   3. hysteresis (a rate near a threshold does not move) and the speed-up guard at flush;
 *   4. deletes, replaces, overwrites and merges across switches whose T lies before rows already written
 *      (the overlap a large insert can make): the same rows in two keys, queries as the model, before and after
 *      optimize and a reopen;
 *   5. a crash after a switch with a WAL segment that a power loss could bring back: no deleted row returns
 *      (the switch's T lies past every WAL row of the series that may be replayed);
 *   6. CLASSES damage: a torn tail is cut, a bad record before a good one fails the open;
 *   7. retention: a short limit caps the class, an expired class-3 key goes whole, tsdb_drop_before drops keys;
 *   8. a database of format 6 is refused;
 *   9. a switch from another thread in the middle of a placement (between sorting rows and grouping them into
 *      parts): the placement keeps the history it began with, no part has a series run going back in time;
 *  10. a row far in the future (in another series, or in the series itself until it is deleted) does not put a
 *      series' switch off for good;
 *  11. the WAL tail kept for switches stays small over thousands of flushes;
 *  12. an open refuses parts whose rows lie in a class CLASSES does not give them (a lost CLASSES tail);
 *  13. granules in class 1-3 partitions end at day edges (GRANULE_CUT): after small and large inserts, merges and
 *      a reopen no granule spans a day and none is short mid-day; with the same rows in a database with classes
 *      off (where partitions are days) the granules are as many, every query kind (raw both ways, aggregates at
 *      several intervals, calendar groups, bounds, spread, TWA both methods, LTTB) answers bit for bit the same,
 *      and day buckets over whole days are answered from granule statistics alone.
 *  14. a switch waiting for a large insert in flight does not hold flushes back: a flush finishes while the insert
 *      is held inside its placement and the switch is pending; the switch goes through after the insert.
 *
 *   test_classes [scratch dir] */
#define _GNU_SOURCE
#include "tsdb.h"
#include "../tools/datasets.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <inttypes.h>
#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

int tsdb_test_class_switch(tsdb *t, uint32_t series, int cls, const int64_t *at); /* internal test hooks */
void tsdb_test_precommit(tsdb *t, void (*fn)(void *), void *arg);
void tsdb_test_midplace(tsdb *t, void (*fn)(void *), void *arg);
void tsdb_test_wal_state(tsdb *t, size_t *nwtail, uint64_t *removed, uint64_t *removed_synced);
typedef struct tsdb_test_granule {
    uint32_t series, count;
    int64_t ts_min, ts_max;
    uint32_t grows;
} tsdb_test_granule;
void tsdb_test_granules(tsdb *t, void (*fn)(void *ctx, int cls, uint32_t i, const tsdb_test_granule *g), void *ctx);

#define DAY 86400000LL
#define T0 (19723 * DAY) /* 2024-01-01 */

static int fails;
static int classes_off; /* keys are base partition numbers */
static char base[512];

#define CK(c, ...)                                                                                                 \
    do {                                                                                                           \
        if (!(c) && fails++ < 40) {                                                                                \
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

static void dir_of(char *buf, size_t len, const char *name)
{
    snprintf(buf, len, "%s/tsdb_classes_%d_%s", base, (int)getpid(), name);
    rm_rf(buf);
}

static uint64_t rs = 88172645463325252ULL;
static uint64_t rnd(void)
{
    rs ^= rs << 13;
    rs ^= rs >> 7;
    rs ^= rs << 17;
    return rs;
}

/* ---------------------------------------------------------------- model: a grid of slots per series */

typedef struct grid {
    uint32_t id;
    int64_t t0, step;
    size_t k;
    signed char *has;
    float *v;
} grid;

static void grid_init(grid *g, uint32_t id, int64_t t0, int64_t step, size_t k)
{
    g->id = id, g->t0 = t0, g->step = step, g->k = k;
    g->has = calloc(k, 1);
    g->v = calloc(k, sizeof *g->v);
}
static void grid_free(grid *g) { free(g->has), free(g->v); }
static int64_t gts(const grid *g, size_t i) { return g->t0 + (int64_t)i * g->step; }
static float vof(size_t i, int gen) { return (float)((i * 7 + (size_t)gen * 1000003) % 100000) * 0.25f; }

/* Writes every stride-th slot of [a, b) with generation gen, by tsdb_insert_series in batches of `batch`. */
static void put_s(tsdb *db, grid *g, size_t a, size_t b, size_t stride, int gen, size_t batch)
{
    int64_t *ts = malloc(batch * sizeof *ts);
    float *v = malloc(batch * sizeof *v);
    for (size_t i = a; i < b;) {
        size_t n = 0;
        for (; i < b && n < batch; i += stride, n++) {
            ts[n] = gts(g, i), v[n] = vof(i, gen);
            g->has[i] = 1, g->v[i] = v[n];
        }
        OK(tsdb_insert_series(db, g->id, ts, v, n));
    }
    free(ts), free(v);
}
static void put(tsdb *db, grid *g, size_t a, size_t b, int gen, size_t batch) { put_s(db, g, a, b, 1, gen, batch); }
static void put_append(tsdb *db, grid *g, size_t a, size_t b, int gen)
{
    for (size_t i = a; i < b; i++) {
        OK(tsdb_append(db, g->id, gts(g, i), vof(i, gen)));
        g->has[i] = 1, g->v[i] = vof(i, gen);
    }
}

/* Every row of the series and the aggregate of a few windows as in the model. */
static void check(tsdb *db, const grid *g, const char *what)
{
    tsdb_points p = {0};
    OK(tsdb_query_points(db, g->id, g->t0 - DAY, gts(g, g->k) + DAY, &p));
    size_t j = 0, bad = 0;
    for (size_t i = 0; i < g->k; i++) {
        if (!g->has[i])
            continue;
        if (j >= p.n || p.ts[j] != gts(g, i) || p.values[j] != g->v[i]) {
            if (bad++ < 3)
                CK(0, "%s: series %u slot %zu (ts %" PRId64 "): got %s ts %" PRId64 " %g, want %g", what, g->id, i,
                   gts(g, i), j < p.n ? "row" : "nothing", j < p.n ? p.ts[j] : 0, j < p.n ? p.values[j] : 0.0,
                   g->v[i]);
            while (j < p.n && p.ts[j] < gts(g, i))
                j++;
            if (j < p.n && p.ts[j] == gts(g, i))
                j++;
            continue;
        }
        j++;
    }
    CK(j == p.n && !bad, "%s: series %u: %zu rows read, %zu matched, %zu bad", what, g->id, p.n, j, bad);
    tsdb_points_free(&p);
    for (int w = 0; w < 6; w++) {
        size_t a = w ? rnd() % g->k : 0, b = w ? a + rnd() % (g->k - a) + 1 : g->k;
        uint64_t cnt = 0;
        double sum = 0;
        for (size_t i = a; i < b; i++)
            if (g->has[i])
                cnt++, sum += g->v[i];
        tsdb_bucket *bk = NULL;
        size_t nb = 0;
        OK(tsdb_query_agg(db, g->id, gts(g, a), gts(g, b), 0, &bk, &nb));
        uint64_t got = nb ? bk[0].count : 0;
        double gs = nb ? bk[0].sum : 0;
        CK(got == cnt && fabs(gs - sum) <= 1e-9 * fabs(sum) + 1e-6, "%s: series %u agg [%zu, %zu): %" PRIu64
           " rows sum %f, want %" PRIu64 " sum %f", what, g->id, a, b, got, gs, cnt, sum);
        free(bk);
    }
}

/* ---------------------------------------------------------------- parts */

typedef struct pstat {
    int ncls[4];
    int bad_range;
    uint64_t rows[4];
    int64_t min_ts[4], max_ts[4];
    int64_t key_lo, key_hi; /* filter: only parts whose range starts in [key_lo, key_hi) */
} pstat;

static void on_part(void *ctx, const tsdb_part_info *in)
{
    pstat *ps = ctx;
    if (in->cls < 0 || in->cls > 3 || in->start_ms >= in->end_ms ||
        (in->rows && (in->min_ts < in->start_ms || in->max_ts >= in->end_ms)) ||
        in->partition * DAY != in->start_ms || (classes_off && in->cls != 0)) {
        if (ps->bad_range++ < 3)
            printf("bad part %s: class %d, range [%" PRId64 ", %" PRId64 "), rows [%" PRId64 ", %" PRId64 "]\n",
                   in->name, in->cls, in->start_ms, in->end_ms, in->min_ts, in->max_ts);
        return;
    }
    if (ps->key_hi && (in->start_ms < ps->key_lo || in->start_ms >= ps->key_hi))
        return;
    if (!in->rows)
        return;
    if (!ps->ncls[in->cls]++)
        ps->min_ts[in->cls] = in->min_ts, ps->max_ts[in->cls] = in->max_ts;
    ps->rows[in->cls] += in->rows;
    ps->min_ts[in->cls] = in->min_ts < ps->min_ts[in->cls] ? in->min_ts : ps->min_ts[in->cls];
    ps->max_ts[in->cls] = in->max_ts > ps->max_ts[in->cls] ? in->max_ts : ps->max_ts[in->cls];
}

static pstat parts_of(tsdb *db)
{
    pstat ps = {0};
    OK(tsdb_foreach_part(db, on_part, &ps));
    CK(!ps.bad_range, "%d parts outside their partition or with a bad class", ps.bad_range);
    return ps;
}

static int cls_of(tsdb *db, uint32_t id)
{
    int c = -1;
    OK(tsdb_series_class(db, id, &c));
    return c;
}

static long classes_bytes(const char *dir)
{
    char p[600];
    snprintf(p, sizeof p, "%s/CLASSES", dir);
    struct stat st;
    return stat(p, &st) ? 0 : (long)st.st_size;
}

/* ---------------------------------------------------------------- 1. bulk loads of every kind */

static uint64_t load_kind(int kind, int classes, int *cls_out, size_t nrows)
{
    char dir[600];
    dir_of(dir, sizeof dir, "bulk");
    tsdb_options o;
    tsdb_options_init(&o);
    o.partition_classes = classes;
    o.merge_threads = 0;
    classes_off = !classes;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid id;
    OK(tsdb_series(db, "s", &id));
    ds_state s;
    ds_init(&s, (ds_kind)kind, 0, T0);
    size_t batch = 100000;
    int64_t *ts = malloc(batch * sizeof *ts);
    float *v = malloc(batch * sizeof *v);
    for (size_t done = 0; done < nrows;) {
        size_t n = nrows - done < batch ? nrows - done : batch;
        for (size_t i = 0; i < n; i++)
            ds_next(&s, &ts[i], &v[i]);
        OK(tsdb_insert_series(db, id, ts, v, n));
        done += n;
    }
    free(ts), free(v);
    OK(tsdb_optimize(db));
    tsdb_stats st;
    OK(tsdb_get_stats(db, &st));
    CK(st.rows == nrows, "kind %s: %" PRIu64 " rows", ds_info[kind].name, st.rows);
    pstat ps = parts_of(db);
    *cls_out = -1;
    for (int c = 0; c < 4; c++)
        if (ps.ncls[c])
            *cls_out = *cls_out < 0 ? c : 9; /* 9: several */
    CK(cls_of(db, id) == (*cls_out == 9 ? -2 : *cls_out), "kind %s: tsdb_series_class %d, parts of class %d",
       ds_info[kind].name, cls_of(db, id), *cls_out);
    OK(tsdb_close(db));
    classes_off = 0;
    rm_rf(dir);
    return st.bytes;
}

static void test_bulk(void)
{
    static const int want[DS_KINDS] = {3, 0, 2, 2, 0, 0, 0, 0};
    size_t n = 1000000;
    for (int k = 0; k < DS_KINDS; k++) {
        int c_on, c_off;
        uint64_t on = load_kind(k, 1, &c_on, n), off = load_kind(k, 0, &c_off, n);
        printf("  %-10s class %d: %.4f bytes/row (classes off: %.4f)\n", ds_info[k].name, c_on, (double)on / n,
               (double)off / n);
        CK(c_on == want[k], "kind %s: class %d, want %d", ds_info[k].name, c_on, want[k]);
        CK(c_off == 0, "kind %s, classes off: class %d", ds_info[k].name, c_off);
        CK((double)on <= (double)off * 1.005, "kind %s: %" PRIu64 " bytes, %" PRIu64 " with classes off",
           ds_info[k].name, on, off);
        if (k == DS_HDD_TEMP)
            /* 0.268 with granules cut at days (0.326 without classes): fails if classes stop working */
            CK((double)on / n <= 0.285, "hdd_temp: %.4f bytes/row", (double)on / n);
    }
}

/* ---------------------------------------------------------------- 2 and 3. live series */

static int wait_class(tsdb *db, uint32_t id, int want, int seconds)
{
    for (int i = 0; i < seconds * 20; i++) {
        if (cls_of(db, id) == want)
            return 1;
        usleep(50000);
    }
    return 0;
}

static void test_live(void)
{
    char dir[600];
    dir_of(dir, sizeof dir, "live");
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_idle_s = 1;
    o.flush_interval_ms = 3600000;
    o.merge_threads = 1;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid a, b, c, d;
    OK(tsdb_series(db, "slow", &a));       /* 1/min by appends: moves to class 3 after an idle merge */
    OK(tsdb_series(db, "near0", &b));      /* 60,000 rows a day: below class 0's target, inside its band */
    OK(tsdb_series(db, "near1", &c));      /* class 2 from a bulk insert, then 10,800 a day: inside its band */
    OK(tsdb_series(db, "speedup", &d));    /* class 3, then a fast flush */
    grid ga, gb, gc, gd;
    grid_init(&ga, a, T0, 60000, 35 * 1440);
    grid_init(&gb, b, T0, 1440, 2 * 60000);
    grid_init(&gc, c, T0, 1000, 33 * 86400);
    grid_init(&gd, d, T0, 1000, 33 * 86400);

    /* a and b: a first flush of fewer than 64 rows decides class 0 */
    put_append(db, &ga, 0, 10, 0);
    put_append(db, &gb, 0, 10, 0);
    OK(tsdb_flush(db));
    CK(cls_of(db, a) == 0 && cls_of(db, b) == 0, "classes %d %d after a small flush", cls_of(db, a), cls_of(db, b));
    put(db, &ga, 10, 3 * 1440, 0, 1000);
    put(db, &gb, 10, gb.k, 0, 1000);
    OK(tsdb_flush(db));
    /* c: 30 days of 8,640 rows a day in one large insert; d: 30 days at 1/min, flushed alone */
    put_s(db, &gc, 0, 30 * 86400, 10, 0, 300000);
    CK(cls_of(db, c) == 2, "a bulk insert of 8,640 rows a day: class %d", cls_of(db, c));
    put_s(db, &gd, 0, 30 * 86400, 60, 0, 50000 * 60);
    OK(tsdb_flush(db));
    CK(cls_of(db, d) == 3, "1,440 rows a day in one flush: class %d", cls_of(db, d));
    long before = classes_bytes(dir);
    CK(before == 2 * 24, "CLASSES holds %ld bytes after two decisions", before);

    CK(wait_class(db, a, 3, 30), "the slow series did not move to class 3 after idle merges (class %d)", cls_of(db, a));
    sleep(3); /* let the other idle merges observe their partitions too */
    CK(cls_of(db, b) == 0, "60,000 rows a day left class 0: class %d", cls_of(db, b));

    /* a's rows so far stay in class 0; the next ones go to its class-3 partition (T is the next day boundary) */
    pstat ps = parts_of(db);
    CK(ps.rows[3] == 30 * 1440 && ps.rows[2] == 30 * 8640, "rows of class 3: %" PRIu64 ", class 2: %" PRIu64,
       ps.rows[3], ps.rows[2]);
    /* T lies after the newest row of every WAL segment that could still be replayed, d's 30 days included */
    put(db, &ga, 32 * 1440, 35 * 1440, 0, 500);
    OK(tsdb_flush(db));
    ps = parts_of(db);
    CK(ps.rows[3] == 33 * 1440, "rows of class 3 after the switch: %" PRIu64, ps.rows[3]);

    /* c: days of 10,800 rows stay in class 2 (class 1 starts at 9,363 a day, but the band is 2x wider) */
    for (size_t slot = 30 * 86400; slot < gc.k; slot += 8) {
        gc.v[slot] = vof(slot, 1), gc.has[slot] = 1;
        OK(tsdb_append(db, c, gts(&gc, slot), gc.v[slot]));
    }
    OK(tsdb_flush(db));
    /* d: 1,000 rows a second apart after its newest: the guard moves it to class 0 at the flush */
    put_append(db, &gd, 30 * 86400 + 3600, 30 * 86400 + 4600, 0);
    OK(tsdb_flush(db));
    CK(cls_of(db, d) == 0, "after a fast flush of a class-3 series: class %d", cls_of(db, d));
    put(db, &gd, 31 * 86400, 31 * 86400 + 1600, 0, 400); /* the next day: class 0 */
    OK(tsdb_flush(db));
    sleep(3);
    CK(cls_of(db, c) == 2, "10,800 rows a day left class 2: class %d", cls_of(db, c));
    long after = classes_bytes(dir);
    CK(after == 4 * 24, "CLASSES holds %ld bytes, want 4 records (2 decisions, 2 switches)", after);

    grid *gs[] = {&ga, &gb, &gc, &gd};
    for (int i = 0; i < 4; i++)
        check(db, gs[i], "live");
    OK(tsdb_close(db));
    OK(tsdb_open(dir, &o, &db));
    CK(cls_of(db, a) == 3 && cls_of(db, d) == 0 && cls_of(db, c) == 2, "classes after a reopen: %d %d %d",
       cls_of(db, a), cls_of(db, d), cls_of(db, c));
    for (int i = 0; i < 4; i++)
        check(db, gs[i], "live, reopened");
    OK(tsdb_optimize(db));
    for (int i = 0; i < 4; i++)
        check(db, gs[i], "live, optimized");
    OK(tsdb_close(db));
    for (int i = 0; i < 4; i++)
        grid_free(gs[i]);
    rm_rf(dir);
}

/* ---------------------------------------------------------------- 4. overlap across forced switches */

static void del(tsdb *db, grid *g, size_t a, size_t b)
{
    OK(tsdb_delete_range(db, g->id, gts(g, a), gts(g, b)));
    for (size_t i = a; i < b; i++)
        g->has[i] = 0;
}

static void repl(tsdb *db, grid *g, size_t a, size_t b, int gen)
{
    size_t n = 0;
    int64_t *ts = malloc((b - a) * sizeof *ts);
    float *v = malloc((b - a) * sizeof *v);
    for (size_t i = a; i < b; i++) {
        g->has[i] = 0;
        if (rnd() % 3)
            continue;
        ts[n] = gts(g, i), v[n] = vof(i, gen), g->has[i] = 1, g->v[i] = v[n++];
    }
    tsdb_values tv = {TSDB_FLOAT32, v, NULL};
    OK(tsdb_replace_range(db, g->id, gts(g, a), gts(g, b), ts, &tv, n));
    free(ts), free(v);
}

/* A switch from another thread while a large insert, a replace or a flush is placing its rows: in the middle
 * (the midplace hook, between sorting the rows and grouping them into parts) or after placing them, before the
 * commit (precommit). The placement holds the class history frozen, so the switch waits for it and computes its
 * T with the rows listed. Before, the switch went through at once: a T that did not see the pending parts left
 * rows in the old class's partitions on both sides of T, where later rows of the same times land in the new
 * class's (two copies of one row in two partitions, which merges and tombstones then rank wrongly), or a
 * placement that read the history twice wrote a series run going back in time. The hook waits up to 50 ms for
 * the switch, which then only finishes before it returns when nothing holds it back (the bug). */
typedef struct race {
    tsdb *db;
    uint32_t id;
    int armed, to, fired, where; /* where: 0 midplace, 1 precommit */
    const int64_t *at;
    pthread_t th;
    int running, done, early;
} race;

static void *race_main(void *arg)
{
    race *r = arg;
    OK(tsdb_test_class_switch(r->db, r->id, r->to, r->at));
    __atomic_store_n(&r->done, 1, __ATOMIC_SEQ_CST);
    return NULL;
}

static void race_go(race *r, int where)
{
    if (!r->armed || r->where != where)
        return;
    r->armed = 0; /* once */
    r->done = 0;
    if (pthread_create(&r->th, NULL, race_main, r) != 0) {
        printf("pthread_create failed\n");
        exit(2);
    }
    r->running = 1;
    for (int i = 0; i < 50 && !__atomic_load_n(&r->done, __ATOMIC_SEQ_CST); i++)
        usleep(1000);
    r->early += __atomic_load_n(&r->done, __ATOMIC_SEQ_CST);
}
static void race_mid(void *arg) { race_go(arg, 0); }
static void race_pre(void *arg) { race_go(arg, 1); }

static void race_join(race *r)
{
    if (!r->running)
        return;
    pthread_join(r->th, NULL);
    r->running = 0;
    r->fired++;
}

static void test_overlap(int seed)
{
    rs = 88172645463325252ULL + (uint64_t)seed * 0x9E3779B97F4A7C15ULL;
    char dir[600];
    dir_of(dir, sizeof dir, "overlap");
    tsdb_options o;
    tsdb_options_init(&o);
    o.wal_max_batch_rows = 16; /* inserts of 16 rows or more go straight to parts, without the WAL */
    o.merge_idle_s = -1;
    o.merge_threads = 1;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid id, other;
    OK(tsdb_series(db, "e", &id));
    OK(tsdb_series(db, "f", &other));
    grid g, h;
    grid_init(&g, id, T0, 60000, 40 * 1440);
    grid_init(&h, other, T0, 30000, 40 * 2880);
    put(db, &g, 0, 10, 0, 10); /* 10 rows: class 0 */
    put(db, &h, 0, 10, 0, 10);
    size_t end = 3 * 1440; /* the series' newest slot so far, which writers push on */
    put(db, &g, 10, end, 0, 1000);
    put(db, &h, 10, 2 * end, 0, 2000);
    CK(cls_of(db, id) == 0, "class %d", cls_of(db, id));
    race r = {db, id, 0, 0, 0, 0, NULL, 0, 0, 0, 0};
    tsdb_test_precommit(db, race_pre, &r);
    tsdb_test_midplace(db, race_mid, &r);
    static const int order[] = {2, 1, 0, 3, 1, 2, 0};
    int nsw = 0;
    for (int round = 0; round < 32; round++) {
        int op = round % 4;
        size_t a, b;
        if (op == 0 || op == 2) { /* across the newest row: these rows race a switch */
            a = end - 1 - rnd() % (end < 2880 ? end - 1 : 2880);
            b = end + 1 + rnd() % 1440;
            r.armed = 1, r.to = order[nsw % 7], r.where = (round / 4) & 1;
        } else {
            a = rnd() % end;
            b = a + 1 + rnd() % 1500;
        }
        b = b > g.k ? g.k : b;
        switch (op) {
        case 0: put(db, &g, a, b, round + 1, 16 + rnd() % 300); break;
        case 1: del(db, &g, a, b); break;
        case 2: repl(db, &g, a, b, round + 1); break;
        default: /* small writes through the WAL */
            for (size_t i = a; i < b && i < a + 200; i++) {
                OK(tsdb_append(db, id, gts(&g, i), vof(i, round + 1)));
                g.has[i] = 1, g.v[i] = vof(i, round + 1);
            }
            check(db, &g, "overlap, appended");
            if (rnd() & 1)
                OK(tsdb_flush(db));
        }
        race_join(&r);
        if (r.fired) /* the series moved: the next race switches it on */
            nsw++, r.fired = 0;
        r.armed = 0;
        end = b > end ? b : end;
        char what[64];
        snprintf(what, sizeof what, "overlap round %d", round);
        check(db, &g, what);
        check(db, &h, "overlap round, the other series");
    }
    tsdb_test_precommit(db, NULL, NULL);
    tsdb_test_midplace(db, NULL, NULL);
    CK(nsw >= 8, "only %d switches raced a writer", nsw);
    CK(!r.early, "%d switches went through while a placement held the class history", r.early);
    pstat ps = parts_of(db);
    CK(ps.ncls[0] && (ps.ncls[1] || ps.ncls[2] || ps.ncls[3]), "parts of classes 0/1/2/3: %d %d %d %d", ps.ncls[0],
       ps.ncls[1], ps.ncls[2], ps.ncls[3]);
    OK(tsdb_optimize(db));
    check(db, &g, "overlap, optimized");
    check(db, &h, "overlap, optimized, the other series");
    OK(tsdb_close(db));
    OK(tsdb_open(dir, &o, &db));
    check(db, &g, "overlap, reopened");
    check(db, &h, "overlap, reopened, the other series");
    del(db, &g, 0, g.k);
    OK(tsdb_optimize(db));
    check(db, &g, "overlap, all deleted");
    OK(tsdb_close(db));
    grid_free(&g), grid_free(&h);
    rm_rf(dir);
}

/* ---------------------------------------------------------------- 5. WAL replay after a switch */

/* The next unlinkat of a WAL segment fails with EIO (the engine is linked statically, so this one is called). */
static int fail_wal_unlink;

int unlinkat(int dirfd, const char *path, int flags)
{
    if (fail_wal_unlink && !strncmp(path, "wal_", 4)) {
        fail_wal_unlink = 0;
        errno = EIO;
        return -1;
    }
    return (int)syscall(SYS_unlinkat, dirfd, path, flags);
}

/* The child writes day D-2 (flushed), then day D through the WAL, and hard-links the WAL segments. Day D is
 * deleted (its flush fails to unlink the segment) and merged to an empty part, which stops the segment from
 * replaying into day D's partition. A later flush unlinks the segment, and no directory fsync follows before the
 * switch and the crash: a power loss may bring the unlink back (the test restores the name). The switch's T must
 * stay after the segment's rows: a T before day D would map them to the new class's key, where no part stops them,
 * and the deleted rows would come back. (Once a directory fsync follows an unlink, as the delete's own commit does
 * when the unlink succeeds at its flush, the segment cannot come back, and T may go before day D.) */
static void test_wal_guard(void)
{
    char dir[600];
    dir_of(dir, sizeof dir, "walguard");
    const int64_t D = T0 + 10 * DAY;
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0; /* no background WAL directory sync */
    o.flush_interval_ms = 3600000;
    printf("  (a log line about removing a WAL segment is expected next)\n");
    fflush(stdout);
    pid_t pid = fork();
    if (pid == 0) {
        tsdb *db;
        OK(tsdb_open(dir, &o, &db));
        tsdb_sid id, x;
        OK(tsdb_series(db, "w", &id));
        OK(tsdb_series(db, "x", &x));
        for (int i = 0; i < 10; i++)
            OK(tsdb_append(db, id, D - 2 * DAY + i * 60000, 1.0f));
        OK(tsdb_flush(db));
        for (int i = 0; i < 10; i++)
            OK(tsdb_append(db, id, D + i * 60000, 2.0f));
        DIR *dd = opendir(dir);
        struct dirent *e;
        char a[900], b[900];
        while ((e = readdir(dd)))
            if (!strncmp(e->d_name, "wal_", 4)) {
                snprintf(a, sizeof a, "%s/%s", dir, e->d_name);
                snprintf(b, sizeof b, "%s/keep_%s", dir, e->d_name);
                if (link(a, b))
                    _exit(3);
            }
        closedir(dd);
        size_t nt;
        uint64_t r0, r1, r2, sy;
        tsdb_test_wal_state(db, &nt, &r0, &sy);
        fail_wal_unlink = 1;
        OK(tsdb_delete_range(db, id, D, D + DAY)); /* flushes day D first; that segment's unlink fails */
        tsdb_test_wal_state(db, &nt, &r1, &sy);
        if (fail_wal_unlink || r1 != r0)
            _exit(4);
        OK(tsdb_optimize(db));
        OK(tsdb_append(db, x, D - 2 * DAY, 3.0f));
        OK(tsdb_flush(db)); /* unlinks day D's segment; no directory fsync after it */
        tsdb_test_wal_state(db, &nt, &r2, &sy);
        if (r2 <= r0 || sy > r0)
            _exit(5);
        OK(tsdb_test_class_switch(db, id, 3, NULL));
        _exit(0); /* a crash: nothing more is synced */
    }
    int st;
    waitpid(pid, &st, 0);
    CK(WIFEXITED(st) && WEXITSTATUS(st) == 0, "child status %d", st);
    DIR *dd = opendir(dir);
    struct dirent *e;
    char a[900], b[900];
    int revived = 0;
    while ((e = readdir(dd)))
        if (!strncmp(e->d_name, "keep_", 5)) {
            snprintf(a, sizeof a, "%s/%s", dir, e->d_name);
            snprintf(b, sizeof b, "%s/%s", dir, e->d_name + 5);
            if (access(b, F_OK)) {
                OK(rename(a, b));
                revived++;
            } else {
                unlink(a);
            }
        }
    closedir(dd);
    CK(revived == 1, "%d WAL segments brought back", revived);
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid id;
    OK(tsdb_series_lookup(db, "w", &id));
    tsdb_points p = {0};
    OK(tsdb_query_points(db, id, D - 3 * DAY, D + 3 * DAY, &p));
    size_t back = 0;
    for (size_t i = 0; i < p.n; i++)
        back += p.ts[i] >= D;
    CK(p.n == 10 && back == 0, "after the crash: %zu rows, %zu of them deleted ones", p.n, back);
    tsdb_points_free(&p);
    OK(tsdb_close(db));
    rm_rf(dir);
}

/* ---------------------------------------------------------------- 6. CLASSES damage */

static void test_damage(void)
{
    char dir[600], path[700];
    dir_of(dir, sizeof dir, "damage");
    snprintf(path, sizeof path, "%s/CLASSES", dir);
    tsdb_options o;
    tsdb_options_init(&o);
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid id;
    OK(tsdb_series(db, "g", &id));
    int64_t t1 = T0 + DAY, t2 = T0 + 2 * DAY;
    OK(tsdb_test_class_switch(db, id, 2, &t1));
    OK(tsdb_test_class_switch(db, id, 3, &t2));
    OK(tsdb_close(db));
    uint8_t buf[48];
    int fd = open(path, O_RDONLY);
    CK(fd >= 0 && read(fd, buf, sizeof buf) == 48, "CLASSES is not 48 bytes");
    close(fd);

    CK(truncate(path, 40) == 0, "truncate");
    OK(tsdb_open(dir, &o, &db));
    CK(cls_of(db, id) == 2, "a torn tail: class %d, want the first record's 2", cls_of(db, id));
    OK(tsdb_close(db));
    CK(classes_bytes(dir) == 24, "the torn tail was not cut: %ld bytes", classes_bytes(dir));

    buf[10] ^= 1; /* the first record bad, the second good */
    fd = open(path, O_WRONLY | O_TRUNC);
    CK(write(fd, buf, sizeof buf) == 48, "write");
    close(fd);
    printf("  (a log line about CLASSES is expected next)\n");
    int rc = tsdb_open(dir, &o, &db);
    CK(rc == TSDB_ERR_CORRUPT, "a bad record before a good one: open returned %d", rc);
    if (rc == 0)
        tsdb_close(db);
    rm_rf(dir);
}

/* ---------------------------------------------------------------- 7. retention */

static int64_t now_ms;
static int64_t clk(void *arg)
{
    (void)arg;
    return __atomic_load_n(&now_ms, __ATOMIC_SEQ_CST);
}

static void test_retention(void)
{
    char dir[600];
    dir_of(dir, sizeof dir, "retention");
    const int64_t Y0 = 54 * 364 * DAY; /* a class-3 boundary (2023-09-09) */
    __atomic_store_n(&now_ms, Y0 + 2 * 364 * DAY + DAY, __ATOMIC_SEQ_CST);
    tsdb_options o;
    tsdb_options_init(&o);
    o.clock_ms = clk;
    o.merge_threads = 1;
    o.merge_idle_s = -1;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid h, y;
    OK(tsdb_series(db, "month", &h));
    OK(tsdb_series(db, "years", &y));
    OK(tsdb_set_retention(db, h, 30 * DAY));
    OK(tsdb_set_retention(db, y, 3000 * DAY));
    grid gh, gy;
    grid_init(&gh, h, Y0 + 2 * 364 * DAY - 20 * DAY, 60000, 20 * 1440);
    grid_init(&gy, y, Y0, 3600000, 2 * 364 * 24);
    put(db, &gh, 0, gh.k, 0, 100000);
    put(db, &gy, 0, gy.k, 0, 100000);
    OK(tsdb_flush(db)); /* fewer rows than wal_max_batch_rows: the flush decides */
    CK(cls_of(db, h) == 0, "1/min with a 30-day retention: class %d, want 0 (at most an eighth of it)", cls_of(db, h));
    CK(cls_of(db, y) == 3, "1/hour with a 3000-day retention: class %d", cls_of(db, y));
    pstat ps = parts_of(db);
    CK(ps.ncls[3] == 2 && ps.rows[3] == gy.k, "class-3 parts %d with %" PRIu64 " rows", ps.ncls[3], ps.rows[3]);
    check(db, &gy, "years");

    /* the first year expires past its end: its key goes whole */
    __atomic_store_n(&now_ms, Y0 + 364 * DAY + 3000 * DAY, __ATOMIC_SEQ_CST);
    int gone = 0;
    for (int i = 0; i < 200 && !gone; i++) {
        usleep(50000);
        ps = parts_of(db);
        gone = ps.ncls[3] == 1;
    }
    CK(gone && ps.min_ts[3] >= Y0 + 364 * DAY, "the expired class-3 key is still there (%d parts)", ps.ncls[3]);
    for (size_t i = 0; i < 364 * 24; i++)
        gy.has[i] = 0;
    check(db, &gy, "years, after the first expired");

    /* tsdb_drop_before drops whole keys only: inside the second year nothing goes, at its end all of it */
    OK(tsdb_drop_before(db, Y0 + 364 * DAY + 100 * DAY));
    ps = parts_of(db);
    CK(ps.ncls[3] == 1 && ps.rows[3] == 364 * 24, "drop_before inside a class-3 key: %d parts, %" PRIu64 " rows",
       ps.ncls[3], ps.rows[3]);
    OK(tsdb_drop_before(db, Y0 + 2 * 364 * DAY));
    ps = parts_of(db);
    CK(ps.ncls[3] == 0, "drop_before at the end of a class-3 key: %d parts left", ps.ncls[3]);
    OK(tsdb_close(db));
    grid_free(&gh), grid_free(&gy);
    rm_rf(dir);
}

/* ---------------------------------------------------------------- 8. format 6 */

static void test_format(void)
{
    char dir[600], path[700];
    dir_of(dir, sizeof dir, "format");
    tsdb_options o;
    tsdb_options_init(&o);
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_close(db));
    snprintf(path, sizeof path, "%s/META", dir);
    char buf[512] = {0};
    int fd = open(path, O_RDWR);
    ssize_t n = read(fd, buf, sizeof buf - 1);
    char *v = strstr(buf, "version=7");
    CK(n > 0 && v, "META: %s", buf);
    if (v) {
        v[8] = '6';
        CK(pwrite(fd, buf, (size_t)n, 0) == n, "pwrite");
    }
    close(fd);
    printf("  (a log line about format version 6 is expected next)\n");
    int rc = tsdb_open(dir, &o, &db);
    CK(rc == TSDB_ERR_CORRUPT, "a format-6 database: open returned %d", rc);
    if (rc == 0)
        tsdb_close(db);
    rm_rf(dir);

    /* classes off: keys are the base partition numbers, as in format 6 */
    dir_of(dir, sizeof dir, "off");
    classes_off = 1;
    o.partition_classes = 0;
    o.merge_threads = 0;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid id;
    OK(tsdb_series(db, "x", &id));
    grid g;
    grid_init(&g, id, T0, 60000, 3 * 1440);
    put(db, &g, 0, g.k, 0, 10000);
    OK(tsdb_flush(db));
    pstat ps = parts_of(db);
    CK(ps.ncls[0] == 3 && cls_of(db, id) == 0, "classes off: %d parts of class 0", ps.ncls[0]);
    OK(tsdb_close(db));
    tsdb_options_init(&o); /* fixed at creation: the default does not turn them on */
    o.merge_threads = 0;
    OK(tsdb_open(dir, &o, &db));
    put(db, &g, 0, g.k, 1, 10000);
    OK(tsdb_flush(db));
    ps = parts_of(db);
    CK(ps.ncls[0] == 6 && !ps.ncls[3], "classes off, reopened with the default: %d %d", ps.ncls[0], ps.ncls[3]);
    check(db, &g, "classes off");
    OK(tsdb_close(db));
    classes_off = 0;
    grid_free(&g);
    rm_rf(dir);
}

/* ---------------------------------------------------------------- fresh series, first written by a replace */

/* A fresh series takes its class from its first flush, large insert or replace, from the beginning of time. A
 * replace once placed its rows in class 0 without deciding the class: the next large insert classified the series
 * from the beginning of time while those rows stayed in class-0 partitions, two copies of one row in two
 * partitions; after a delete there, the merged class-0 part outranked the newer copy and old values came back
 * (test_delete's crash seed 5001 found it). */
static void test_fresh(void)
{
    char dir[600];
    dir_of(dir, sizeof dir, "fresh");
    tsdb_options o;
    tsdb_options_init(&o);
    o.granule_rows = 64; /* class 0 needs 512 rows a day */
    o.wal_max_batch_rows = 100;
    o.merge_threads = 0;
    o.fsync = 0;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid id;
    OK(tsdb_series(db, "fresh", &id));
    grid g;
    grid_init(&g, id, 1700000000000 / DAY * DAY, 600000, 2000); /* one row in 10 minutes, 14 days */
    repl(db, &g, 0, 2000, 1); /* the first rows of the series */
    int c = cls_of(db, id);
    CK(c > 0, "a slow series first written by a replace: class %d", c);
    put(db, &g, 0, 2000, 2, 2000); /* a large insert over the same times */
    del(db, &g, 5, 6);             /* a tombstone where the replace's rows are */
    OK(tsdb_optimize(db));
    check(db, &g, "fresh series after a replace, an insert and a delete");
    pstat ps = parts_of(db);
    int n = 0;
    for (int k = 0; k < 4; k++)
        n += ps.ncls[k] > 0;
    CK(n == 1 && ps.ncls[c] > 0, "the series' rows lie in %d classes (want only %d)", n, c);
    grid_free(&g);
    OK(tsdb_close(db));
    rm_rf(dir);
}

/* ---------------------------------------------------------------- 9. a switch in the middle of a placement */

/* The jepsen classes workload found it (seed 21 under --nemesis pause): a large insert grouped its sorted rows by
 * partition reading the live history, a switch landed between its sort and the grouping, and one part got the
 * series run [24000, 24005, 24010, 8000 .. 11995]. Queries binary-search runs: an overwrite of every row left 800
 * of them showing the older value, and 800 duplicates, also after optimize. Classes: 0 before 8000, 2 from 8000,
 * 1 from 12000; the switch to 2 at 24000 races the insert (or the replace) in the middle. */
#define MP_T0 1700000000000LL

static void mid_check(tsdb *db, uint32_t s, size_t n, float want, const char *what)
{
    tsdb_points p = {0};
    OK(tsdb_query_points(db, s, MP_T0, MP_T0 + 30000, &p));
    size_t bad = 0, old = 0;
    for (size_t i = 0; i < p.n; i++)
        bad += i && p.ts[i] <= p.ts[i - 1], old += p.values[i] != want;
    CK(p.n == n && !bad && !old, "%s: %zu rows (want %zu), %zu out of order, %zu not the newest value", what, p.n, n,
       bad, old);
    tsdb_points_free(&p);
}

static void test_midplace(int how) /* 0: tsdb_insert_series, 1: tsdb_insert, 2: tsdb_replace_range */
{
    char dir[600];
    dir_of(dir, sizeof dir, "midplace");
    tsdb_options o;
    tsdb_options_init(&o);
    o.partition_ms = 2000, o.granule_rows = 512, o.merge_threads = 0, o.wal_max_batch_rows = 500;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid s;
    OK(tsdb_series(db, "s", &s));
    int64_t a8 = MP_T0 + 8000, a12 = MP_T0 + 12000, a24 = MP_T0 + 24000;
    OK(tsdb_test_class_switch(db, s, 2, &a8));
    OK(tsdb_test_class_switch(db, s, 1, &a12));
    static int64_t ts[6000];
    static float v[6000];
    static tsdb_sid ids[6000];
    size_t n = 0;
    for (int64_t t = 0; t <= 24010; t += 5)
        ts[n] = MP_T0 + t, v[n] = 1, ids[n] = s, n++;
    race r = {db, s, 1, 2, 0, 0, &a24, 0, 0, 0, 0};
    tsdb_test_midplace(db, race_mid, &r);
    tsdb_values tv = {TSDB_FLOAT32, v, NULL};
    if (how == 0)
        OK(tsdb_insert_series(db, s, ts, v, n));
    else if (how == 1)
        OK(tsdb_insert(db, ids, ts, v, n));
    else
        OK(tsdb_replace_range(db, s, MP_T0, MP_T0 + 30000, ts, &tv, n));
    race_join(&r);
    tsdb_test_midplace(db, NULL, NULL);
    CK(r.fired == 1 && !r.early, "how %d: the switch fired %d times, %d of them while the insert placed its rows",
       how, r.fired, r.early);
    mid_check(db, s, n, 1, "after the raced insert");
    for (size_t i = 0; i < n; i++)
        v[i] = 2;
    OK(tsdb_insert_series(db, s, ts, v, n));
    mid_check(db, s, n, 2, "overwritten");
    OK(tsdb_optimize(db));
    mid_check(db, s, n, 2, "overwritten, optimized");
    OK(tsdb_close(db));
    OK(tsdb_open(dir, &o, &db)); /* the open checks every part against CLASSES */
    mid_check(db, s, n, 2, "reopened");
    OK(tsdb_close(db));
    rm_rf(dir);
}

/* ---------------------------------------------------------------- 10. rows far in the future */

static int day_rows[4];
static int64_t day_lo, day_hi;
static void on_day_part(void *c, const tsdb_part_info *in)
{
    (void)c;
    if (in->rows && in->min_ts >= day_lo && in->max_ts < day_hi && in->cls >= 0 && in->cls < 4)
        day_rows[in->cls] += (int)in->rows;
}

static void fast_day(tsdb *db, tsdb_sid c, int day)
{
    for (int i = 0; i < 5000; i++)
        OK(tsdb_append(db, c, T0 + day * DAY + 3600000 + i * 10LL, (float)i));
    OK(tsdb_flush(db));
}

/* Where day `day` of series c landed: the rows of class-0 parts. */
static int day_in_class0(tsdb *db, int day)
{
    memset(day_rows, 0, sizeof day_rows);
    day_lo = T0 + day * DAY, day_hi = day_lo + DAY;
    OK(tsdb_foreach_part(db, on_day_part, NULL));
    return day_rows[0];
}

/* Series c, bulk-loaded at one row a minute (class 3), gets 100 rows a second: the flush guard moves it to class 0
 * from the next day. Before the fix the switch's T was bounded by the newest row of ANY series in a flushed WAL
 * segment, and that list was never pruned: one row ten years ahead in series a put c's switch (and every later
 * one) ten years ahead, for good, also after the row was deleted and the database reopened; the log blamed c.
 * Own: the far row is c's own: no switch is written while it exists (logged), and once it is deleted and merged
 * away the guard moves c. */
static void test_far(int own)
{
    char dir[600];
    dir_of(dir, sizeof dir, "far");
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0, o.flush_interval_ms = 3600000, o.wal_max_batch_rows = 1000; /* the load: parts */
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid a, c;
    OK(tsdb_series(db, "a", &a));
    OK(tsdb_series(db, "c", &c));
    size_t n = 30 * 1440;
    int64_t *ts = malloc(n * sizeof *ts);
    float *v = malloc(n * sizeof *v);
    for (size_t i = 0; i < n; i++)
        ts[i] = T0 + (int64_t)i * 60000, v[i] = (float)(i % 100);
    OK(tsdb_insert_series(db, c, ts, v, n));
    free(ts), free(v);
    CK(cls_of(db, c) == 3, "c after a 30-day load at 1/min: class %d", cls_of(db, c));
    OK(tsdb_append(db, a, T0, 1));
    OK(tsdb_flush(db));
    if (own)
        printf("  (a log line about a row far in the future is expected next)\n");
    OK(tsdb_append(db, own ? c : a, T0 + 3650 * DAY, 1)); /* one wrong timestamp, ten years ahead, flushed with */
    fast_day(db, c, 31);                                   /* c's fast day */
    if (own) {
        CK(cls_of(db, c) == 3, "own far row: c switched (class %d), its T would lie ten years ahead", cls_of(db, c));
        fast_day(db, c, 32);
        CK(day_in_class0(db, 32) == 0, "own far row: day 32 in class 0 (%d rows)", day_rows[0]);
        OK(tsdb_delete_range(db, c, T0 + 3000 * DAY, INT64_MAX));
        OK(tsdb_optimize(db)); /* the deleted row is merged away */
        fast_day(db, c, 33);
        CK(cls_of(db, c) == 0, "own far row deleted: c still in class %d", cls_of(db, c));
        fast_day(db, c, 34);
        CK(day_in_class0(db, 34) == 5000, "own far row deleted: day 34: %d rows in class 0, classes 1-3: %d %d %d",
           day_rows[0], day_rows[1], day_rows[2], day_rows[3]);
    } else {
        CK(cls_of(db, c) == 0, "c after a fast day: class %d", cls_of(db, c));
        fast_day(db, c, 32);
        CK(day_in_class0(db, 32) == 5000, "a's far row: day 32 of c: %d rows in class 0, classes 1-3: %d %d %d",
           day_rows[0], day_rows[1], day_rows[2], day_rows[3]);
    }
    OK(tsdb_close(db));
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_close(db));
    rm_rf(dir);
}

/* ---------------------------------------------------------------- 11. the WAL tail of switches */

/* The newest rows of flushed memtables whose WAL segment may still be replayed bound a switch's T. Before the fix
 * the list only shrank when the merge thread synced the directory for an empty part or a dropped partition: one
 * entry per flush, forever, scanned under the engine's locks at every flush (135 -> 189 us per flush over 60,000
 * flushes). Every directory fsync now moves wal_removed_synced on. */
static void test_wtail(int fsync_on)
{
    char dir[600];
    dir_of(dir, sizeof dir, "wtail");
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0, o.flush_interval_ms = 3600000, o.fsync = fsync_on;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid id;
    OK(tsdb_series(db, "w", &id));
    size_t nt = 0, most = 0;
    uint64_t removed = 0, synced = 0;
    for (int i = 0; i < 1500; i++) {
        OK(tsdb_append(db, id, T0 + i * 1000LL, (float)i));
        OK(tsdb_flush(db));
        tsdb_test_wal_state(db, &nt, &removed, &synced);
        most = nt > most ? nt : most;
    }
    CK(most <= 2, "fsync %d: the WAL tail held up to %zu entries over 1500 flushes", fsync_on, most);
    CK(removed >= 1500 && synced + 1 >= removed, "fsync %d: segments removed up to %" PRIu64 ", durably %" PRIu64,
       fsync_on, removed, synced);
    OK(tsdb_close(db));
    rm_rf(dir);
}

/* ---------------------------------------------------------------- 12. parts against CLASSES at open */

/* With fsync = 0 a power loss can drop the tail of CLASSES while parts placed by its records survive: rows of one
 * (series, time) could then be placed in two partitions, unnoticed. The open checks every part's rows against the
 * history (in the partition's range, sorted, in the class it gives them) and fails with TSDB_ERR_CORRUPT. */
static void test_open_check(void)
{
    char dir[600], path[700];
    dir_of(dir, sizeof dir, "opencheck");
    snprintf(path, sizeof path, "%s/CLASSES", dir);
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0, o.wal_max_batch_rows = 16; /* straight to parts: the load decides the class */
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid id;
    OK(tsdb_series(db, "g", &id));
    grid g;
    grid_init(&g, id, T0, 60000, 10 * 1440);
    put(db, &g, 0, 1440, 0, 1440); /* fresh: one record */
    int64_t t2 = T0 + 2 * DAY;
    int c0 = cls_of(db, id);
    OK(tsdb_test_class_switch(db, id, c0 == 3 ? 0 : 3, &t2));
    put(db, &g, 2 * 1440, 3 * 1440, 0, 1440); /* in the new class */
    OK(tsdb_close(db));
    OK(tsdb_open(dir, &o, &db)); /* intact: opens */
    check(db, &g, "open check, intact");
    OK(tsdb_close(db));
    long len = classes_bytes(dir);
    CK(len >= 48 && truncate(path, len - 24) == 0, "CLASSES: %ld bytes", len); /* the switch is lost */
    printf("  (a log line about a part in another class is expected next)\n");
    int rc = tsdb_open(dir, &o, &db);
    CK(rc == TSDB_ERR_CORRUPT, "a part whose class CLASSES lost: open returned %d", rc);
    if (rc == 0)
        tsdb_close(db);
    o.read_only = 1;
    rc = tsdb_open(dir, &o, &db);
    CK(rc == TSDB_ERR_CORRUPT, "the same, read-only: open returned %d", rc);
    if (rc == 0)
        tsdb_close(db);
    grid_free(&g);
    rm_rf(dir);
}

/* A series that comes back to a class: its rows before and after a stay in another class share one partition of
 * that class, often one granule, with no row in between. The open check judges such a granule by its rows, not by
 * its bounds (it once refused every such database), and still finds a row the history puts in the other class. */
uint64_t xxh64(const void *data, size_t len, uint64_t seed); /* the engine's, for a CLASSES record by hand */

static void test_comeback(void)
{
    char dir[600], path[700];
    dir_of(dir, sizeof dir, "comeback");
    snprintf(path, sizeof path, "%s/CLASSES", dir);
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0, o.wal_max_batch_rows = 16;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid id;
    OK(tsdb_series(db, "back", &id));
    grid g;
    grid_init(&g, id, T0, 600000, 8 * 144); /* every 10 minutes: the longest class */
    put(db, &g, 0, 144, 0, 144);
    CK(cls_of(db, id) == 3, "a slow series: class %d", cls_of(db, id));
    /* away to class 0 from day 2 to 4, and from 5 days 5 h for 1 h 40 min */
    int64_t at[4] = {gts(&g, 288), gts(&g, 576), gts(&g, 720 + 30), gts(&g, 720 + 40)};
    int to[4] = {0, 3, 0, 3};
    for (int k = 0; k < 4; k++)
        OK(tsdb_test_class_switch(db, id, to[k], &at[k]));
    put(db, &g, 144, 8 * 144, 0, 8 * 144); /* one batch: a class-3 part with rows on both sides of each stay */
    OK(tsdb_optimize(db));
    OK(tsdb_close(db));
    int rc = tsdb_open(dir, &o, &db);
    CK(rc == 0, "a series back in its class: open returned %d", rc);
    if (rc == 0) {
        check(db, &g, "back in its class");
        OK(tsdb_close(db));
    }
    o.read_only = 1;
    rc = tsdb_open(dir, &o, &db);
    CK(rc == 0, "the same, read-only: open returned %d", rc);
    if (rc == 0)
        tsdb_close(db);
    /* the second stay begins 1 h earlier in CLASSES: rows 5 days 4 h to 5 h, inside a class-3 granule whose first
     * and last rows are in class 3, now belong to class 0 */
    int fd = open(path, O_RDWR);
    CK(fd >= 0, "open %s", path);
    uint8_t rec[24];
    int moved = 0;
    for (off_t off = 0; fd >= 0 && pread(fd, rec, 24, off) == 24; off += 24) {
        uint32_t s;
        int64_t from;
        memcpy(&s, rec + 4, 4), memcpy(&from, rec + 8, 8);
        if (s != id || from != at[2])
            continue;
        from = gts(&g, 720 + 24);
        memcpy(rec + 8, &from, 8);
        tsdb_sid c = (uint32_t)xxh64(rec + 4, 20, (uint64_t)off);
        memcpy(rec, &c, 4);
        moved = pwrite(fd, rec, 24, off) == 24;
    }
    if (fd >= 0)
        close(fd);
    CK(moved, "the record of the second stay was not found");
    printf("  (log lines about a part in another class are expected next)\n");
    o.read_only = 0;
    rc = tsdb_open(dir, &o, &db);
    CK(rc == TSDB_ERR_CORRUPT, "rows inside a granule in another class: open returned %d", rc);
    if (rc == 0)
        tsdb_close(db);
    o.read_only = 1;
    rc = tsdb_open(dir, &o, &db);
    CK(rc == TSDB_ERR_CORRUPT, "the same, read-only: open returned %d", rc);
    if (rc == 0)
        tsdb_close(db);
    grid_free(&g);
    rm_rf(dir);
}

/* ---------------------------------------------------------------- 13. granules end at day edges */

static int64_t floor_div(int64_t a, int64_t b) { return a / b - (a % b < 0); }
static uint64_t mix(uint64_t x)
{
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}

/* Rates inside their class' band, so flushes keep them where the test puts them; one starts before the epoch. */
typedef struct dc_spec {
    const char *name;
    int cls;
    tsdb_type type;
    int64_t t0, step, jitter; /* row i at t0 + i step + a jitter below step */
    /* every per_day-th row lies exactly at t0 + k DAY, without jitter */
    size_t n, per_day, nulls;
} dc_spec;
static const dc_spec dc_specs[] = {
    {"c1_f64", 1, TSDB_FLOAT64, T0 + 5 * DAY + 1234, 7000, 0, 16 * 12343, 0, 97},
    {"c1_f32", 1, TSDB_FLOAT32, T0 - 2 * DAY, 3000, 2000, 5 * 28800, 28800, 0},
    {"c2_i64", 2, TSDB_INT64, -10 * DAY, 45000, 20000, 70 * 1920, 1920, 53},
    {"c3_f32", 3, TSDB_FLOAT32, T0 - 40 * DAY, 60000, 30000, 60 * 1440, 1440, 0},
};
#define DC_N (sizeof dc_specs / sizeof dc_specs[0])

/* rows of series k loaded after round 1 (60% in small inserts, 20% in one large one) and round 2 (the rest) */
static size_t dc_loaded(size_t k, int round) { return round == 1 ? dc_specs[k].n * 4 / 5 & ~(size_t)7 : dc_specs[k].n; }

typedef struct dc_data {
    int64_t *ts;
    uint8_t *v, *valid;
    int width;
} dc_data;

static void dc_make(dc_data *d, int k)
{
    const dc_spec *s = &dc_specs[k];
    d->width = s->type == TSDB_FLOAT32 ? 4 : 8;
    d->ts = malloc(s->n * sizeof *d->ts);
    d->v = malloc(s->n * 8);
    d->valid = calloc(s->n / 8 + 1, 1);
    for (size_t i = 0; i < s->n; i++) {
        int edge = s->per_day && i % s->per_day == 0;
        d->ts[i] = s->t0 + (int64_t)i * s->step + (s->jitter && !edge ? (int64_t)(mix(i * 4 + k) % s->jitter) : 0);
        uint64_t r = mix(i * 4 + k + 1000003);
        double x = (double)(i % 5000) * 0.01 + (double)(r % 1000) * 0.001 + k; /* drifts, with noise */
        if (s->type == TSDB_FLOAT32)
            ((float *)d->v)[i] = (float)x;
        else if (s->type == TSDB_FLOAT64)
            ((double *)d->v)[i] = x * 1.0000001;
        else
            ((int64_t *)d->v)[i] = (int64_t)(r % 200001) - 100000;
        if (!s->nulls || i % s->nulls != 5)
            d->valid[i >> 3] |= (uint8_t)(1u << (i & 7));
    }
}

static void dc_put(tsdb *db, tsdb_sid id, int k, const dc_data *d, size_t a, size_t b, size_t batch)
{
    for (size_t i = a; i < b; i += batch) { /* a and batch are multiples of 8: the bitmap starts at a byte */
        size_t m = b - i < batch ? b - i : batch;
        tsdb_values v = {dc_specs[k].type, d->v + i * d->width, d->valid + i / 8};
        OK(tsdb_insert_series_values(db, id, d->ts + i, &v, m));
    }
}

typedef struct dc_gran {
    uint64_t n[4], cross, short_mid;
    tsdb_test_granule prev;
} dc_gran;

static void on_granule(void *ctx, int cls, uint32_t i, const tsdb_test_granule *g)
{
    dc_gran *c = ctx;
    c->n[cls]++;
    int64_t d0 = floor_div(g->ts_min, DAY);
    if (cls && d0 != floor_div(g->ts_max, DAY) && c->cross++ < 3)
        printf("  granule of series %u in a class %d partition spans days: [%" PRId64 ", %" PRId64 "], %u rows\n",
               g->series, cls, g->ts_min, g->ts_max, g->count);
    /* within a part a series' rows go through one writer: a short granule ends only at a series' end or a cut */
    if (i && c->prev.series == g->series && c->prev.count < c->prev.grows &&
        (!cls || floor_div(c->prev.ts_max, DAY) == d0) && c->short_mid++ < 3)
        printf("  short granule of series %u (%u rows, ending %" PRId64 ") before one at %" PRId64 "\n", g->series,
               c->prev.count, c->prev.ts_max, g->ts_min);
    c->prev = *g;
}

static dc_gran dc_granules(tsdb *db, const char *what, int classes)
{
    dc_gran c = {0};
    tsdb_test_granules(db, on_granule, &c);
    CK(!c.cross, "%s: %" PRIu64 " granules in class partitions span a day edge", what, c.cross);
    CK(!c.short_mid, "%s: %" PRIu64 " short granules followed by more rows of their series and day", what,
       c.short_mid);
    CK(!classes || !c.n[0], "%s: %" PRIu64 " granules in class 0 partitions", what, c.n[0]);
    return c;
}

/* every answer of every query kind, as hashes: the two databases must agree bit for bit */
#define DC_Q 40
typedef struct dc_ans {
    uint64_t h[DC_Q];
    int nq;
} dc_ans;

static uint64_t fnv(uint64_t h, const void *p, size_t n)
{
    const uint8_t *b = p;
    for (size_t i = 0; i < n; i++)
        h = (h ^ b[i]) * 0x100000001b3ULL;
    return h;
}

typedef struct dc_vh {
    uint64_t h;
    size_t n;
} dc_vh;

static int on_dc_values(void *ctx, const int64_t *ts, const tsdb_values *v, size_t n)
{
    dc_vh *c = ctx;
    int w = v->type == TSDB_FLOAT32 ? 4 : v->type == TSDB_BOOL ? 1 : 8;
    for (size_t i = 0; i < n; i++) {
        uint8_t ok = !v->valid || (v->valid[i >> 3] >> (i & 7) & 1);
        c->h = fnv(c->h, &ts[i], 8);
        c->h = fnv(c->h, &ok, 1);
        if (ok)
            c->h = fnv(c->h, (const uint8_t *)v->values + i * w, (size_t)w);
    }
    c->n += n;
    return 0;
}

#define DC_ARR(call, arr, nb)                                                                                      \
    do {                                                                                                           \
        arr = NULL, nb = 0;                                                                                        \
        OK(call);                                                                                                  \
        a->h[a->nq++] = fnv(fnv(0xcbf29ce484222325ULL, &nb, sizeof nb), arr, nb * sizeof *arr);                   \
        free(arr);                                                                                                 \
    } while (0)

static void dc_answers(tsdb *db, uint32_t id, int64_t lo, int64_t hi, const int64_t *bounds, size_t nbounds,
                       dc_ans *a)
{
    a->nq = 0;
    dc_vh vh = {0xcbf29ce484222325ULL, 0};
    OK(tsdb_query_values(db, id, lo, hi, NULL, on_dc_values, &vh));
    a->h[a->nq++] = fnv(vh.h, &vh.n, sizeof vh.n);
    tsdb_query_opts desc = {TSDB_DESC, 1000};
    vh = (dc_vh){0xcbf29ce484222325ULL, 0};
    OK(tsdb_query_values(db, id, lo, hi, &desc, on_dc_values, &vh));
    a->h[a->nq++] = fnv(vh.h, &vh.n, sizeof vh.n);
    static const int64_t iv[] = {0, 1020000, 3600000, DAY, 216000000, 7 * DAY};
    tsdb_bucket *b;
    tsdb_spread_bucket *sb;
    tsdb_twa_bucket *tb;
    size_t nb;
    for (size_t i = 0; i < sizeof iv / sizeof iv[0]; i++)
        DC_ARR(tsdb_query_agg(db, id, lo, hi, iv[i], &b, &nb), b, nb);
    static const tsdb_group gr[] = {{TSDB_DAY, 1, NULL}, {TSDB_WEEK, 1, NULL}, {TSDB_MONTH, 1, NULL}};
    for (size_t i = 0; i < 3; i++)
        DC_ARR(tsdb_query_group(db, id, lo, hi, &gr[i], &b, &nb), b, nb);
    DC_ARR(tsdb_query_bounds(db, id, lo, hi, bounds, nbounds, NULL, &b, &nb), b, nb);
    DC_ARR(tsdb_query_agg_ex(db, id, lo, hi, DAY, &desc, &b, &nb), b, nb);
    for (size_t i = 0; i < 4; i++)
        DC_ARR(tsdb_query_spread(db, id, lo, hi, iv[i], NULL, &sb, &nb), sb, nb);
    DC_ARR(tsdb_query_spread_group(db, id, lo, hi, &gr[0], NULL, &sb, &nb), sb, nb);
    DC_ARR(tsdb_query_spread_bounds(db, id, lo, hi, bounds, nbounds, NULL, &sb, &nb), sb, nb);
    for (int m = 0; m < 2; m++) {
        tsdb_twa_opts to = {.method = m ? TSDB_TWA_LINEAR : TSDB_TWA_STEP};
        for (size_t i = 0; i < 4; i++)
            DC_ARR(tsdb_query_twa(db, id, lo, hi, iv[i], &to, &tb, &nb), tb, nb);
        DC_ARR(tsdb_query_twa_group(db, id, lo, hi, &gr[0], &to, &tb, &nb), tb, nb);
        DC_ARR(tsdb_query_twa_bounds(db, id, lo, hi, bounds, nbounds, &to, &tb, &nb), tb, nb);
    }
    static const size_t th[] = {50, 777};
    for (size_t i = 0; i < 2; i++) {
        vh = (dc_vh){0xcbf29ce484222325ULL, 0};
        OK(tsdb_query_lttb(db, id, lo, hi, th[i], on_dc_values, &vh));
        a->h[a->nq++] = fnv(vh.h, &vh.n, sizeof vh.n);
    }
}

/* the answers of on and off agree for whole ranges, whole days and random windows of every series */
static void dc_compare(tsdb *on, tsdb *off, const tsdb_sid *ids_on, const tsdb_sid *ids_off, int round,
                       const dc_data *d, const char *what)
{
    for (size_t k = 0; k < DC_N; k++) {
        size_t n = dc_loaded(k, round);
        int64_t lo = d[k].ts[0], hi = d[k].ts[n - 1] + 1, d0 = floor_div(lo, DAY) * DAY;
        int64_t win[9][2] = {{lo - DAY, hi + DAY}, {d0 + DAY, d0 + 4 * DAY}, {lo + 1, hi - 1}};
        for (int w = 3; w < 9; w++) {
            int64_t x = lo + (int64_t)(rnd() % (uint64_t)(hi - lo)), y = lo + (int64_t)(rnd() % (uint64_t)(hi - lo));
            win[w][0] = x < y ? x : y, win[w][1] = (x < y ? y : x) + 1;
        }
        for (int w = 0; w < 9; w++) {
            int64_t bounds[12], s = win[w][0], span = win[w][1] - win[w][0];
            for (int i = 0; i < 12; i++) /* strictly increasing, the first one before the window */
                bounds[i] = s - DAY / 3 + (int64_t)i * (span / 11 + 1) + (int64_t)(rnd() % 1000);
            dc_ans x, y;
            dc_answers(on, ids_on[k], win[w][0], win[w][1], bounds, 12, &x);
            dc_answers(off, ids_off[k], win[w][0], win[w][1], bounds, 12, &y);
            for (int q = 0; q < x.nq; q++)
                if (x.h[q] != y.h[q]) {
                    CK(0, "%s: series %s window %d [%" PRId64 ", %" PRId64 "): query %d differs from classes off",
                       what, dc_specs[k].name, w, win[w][0], win[w][1], q);
                    break;
                }
        }
    }
}

/* day buckets over whole days come from granule statistics alone */
static void dc_stats(tsdb *db, const tsdb_sid *ids, const dc_data *d, int round, const char *what)
{
    for (size_t k = 0; k < DC_N; k++) {
        size_t n = dc_loaded(k, round);
        int64_t lo = floor_div(d[k].ts[0], DAY) * DAY, hi = (floor_div(d[k].ts[n - 1], DAY) + 1) * DAY;
        tsdb_bucket *b = NULL;
        size_t nb = 0;
        OK(tsdb_query_agg(db, ids[k], lo, hi, DAY, &b, &nb));
        tsdb_query_stats st;
        tsdb_last_query_stats(&st);
        free(b);
        CK(st.rows_decoded == 0 && st.granules_from_stats == st.granules && st.rows_from_stats == n,
           "%s: %s by day: %" PRIu64 " rows decoded, %" PRIu64 " of %" PRIu64 " granules from statistics, %" PRIu64
           " rows from them (%zu rows)", what, dc_specs[k].name, st.rows_decoded, st.granules_from_stats, st.granules,
           st.rows_from_stats, n);
    }
}

static tsdb *dc_open(const char *dir, int classes)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.partition_classes = classes;
    o.merge_threads = 0;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    return db;
}

static void test_daycut(void)
{
    char don[600], doff[600];
    dir_of(don, sizeof don, "daycut-on");
    dir_of(doff, sizeof doff, "daycut-off");
    dc_data d[DC_N];
    tsdb_sid ion[DC_N], ioff[DC_N];
    for (size_t k = 0; k < DC_N; k++)
        dc_make(&d[k], (int)k);
    tsdb *on = dc_open(don, 1), *off = dc_open(doff, 0);
    for (size_t k = 0; k < DC_N; k++) {
        OK(tsdb_series_create(on, dc_specs[k].name, dc_specs[k].type, NULL, &ion[k]));
        OK(tsdb_series_create(off, dc_specs[k].name, dc_specs[k].type, NULL, &ioff[k]));
        OK(tsdb_test_class_switch(on, ion[k], dc_specs[k].cls, NULL));
    }
    /* round 1: 60% in small inserts (WAL, memtable, flush), 20% as one large insert; round 2 the rest */
    for (int round = 1; round <= 2; round++) {
        for (int x = 0; x < 2; x++) {
            tsdb *db = x ? off : on;
            const tsdb_sid *ids = x ? ioff : ion;
            for (size_t k = 0; k < DC_N; k++) {
                size_t n = dc_specs[k].n, a = n * 3 / 5 & ~(size_t)7, b = dc_loaded(k, 1);
                if (round == 1) {
                    dc_put(db, ids[k], (int)k, &d[k], 0, a, 2000);
                    OK(tsdb_flush(db));
                    dc_put(db, ids[k], (int)k, &d[k], a, b, b);
                } else {
                    dc_put(db, ids[k], (int)k, &d[k], b, n, n);
                }
            }
        }
        char what[64];
        snprintf(what, sizeof what, "round %d, loaded", round);
        dc_granules(on, what, 1);
        dc_granules(off, what, 0);
        OK(tsdb_optimize(on));
        OK(tsdb_optimize(off));
        snprintf(what, sizeof what, "round %d, optimized", round);
        dc_gran gon = dc_granules(on, what, 1), goff = dc_granules(off, what, 0);
        uint64_t non = gon.n[1] + gon.n[2] + gon.n[3], noff = goff.n[0];
        printf("  %s: %" PRIu64 " granules in class partitions, %" PRIu64 " with classes off\n", what, non, noff);
        CK(non == noff, "%s: %" PRIu64 " granules, %" PRIu64 " with classes off (one per day and 8192 rows in both)",
           what, non, noff);
        for (size_t k = 0; k < DC_N; k++)
            CK(cls_of(on, ion[k]) == dc_specs[k].cls, "%s: %s in class %d", what, dc_specs[k].name,
               cls_of(on, ion[k]));
        dc_compare(on, off, ion, ioff, round, d, what);
        dc_stats(on, ion, d, round, what);
        dc_stats(off, ioff, d, round, what);
    }
    OK(tsdb_close(on));
    on = dc_open(don, 1);
    dc_granules(on, "reopened", 1);
    dc_compare(on, off, ion, ioff, 2, d, "reopened");
    OK(tsdb_close(on));
    OK(tsdb_close(off));
    for (size_t k = 0; k < DC_N; k++)
        free(d[k].ts), free(d[k].v), free(d[k].valid);
    rm_rf(don);
    rm_rf(doff);
}

/* 14. The switch holds flush_lock while it waits for the write lock on the class history; waiting out a large
 * insert there (seconds for 1 G rows) stopped every flush. Here the insert is held inside its placement (the
 * precommit hook) until the flush is done or 3 s have passed. */
typedef struct hold {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    int in, go;
    tsdb *db;
    uint32_t id;
} hold;

static void hold_pre(void *arg)
{
    hold *h = arg;
    pthread_mutex_lock(&h->mu);
    h->in = 1;
    pthread_cond_broadcast(&h->cv);
    while (!h->go)
        pthread_cond_wait(&h->cv, &h->mu);
    pthread_mutex_unlock(&h->mu);
}

static void *hold_insert(void *arg)
{
    hold *h = arg;
    int64_t ts[100];
    float v[100];
    for (int i = 0; i < 100; i++)
        ts[i] = T0 + i * 1000LL, v[i] = (float)i;
    OK(tsdb_insert_series(h->db, h->id, ts, v, 100));
    return NULL;
}

typedef struct stall_sw {
    tsdb *db;
    uint32_t id;
    int done;
} stall_sw;

static void *stall_switch(void *arg)
{
    stall_sw *w = arg;
    OK(tsdb_test_class_switch(w->db, w->id, 1, NULL));
    __atomic_store_n(&w->done, 1, __ATOMIC_SEQ_CST);
    return NULL;
}

static void *stall_flush(void *arg)
{
    stall_sw *w = arg;
    OK(tsdb_append(w->db, w->id, T0 + 5 * DAY, 1.0f));
    OK(tsdb_flush(w->db));
    __atomic_store_n(&w->done, 1, __ATOMIC_SEQ_CST);
    return NULL;
}

static void test_stall(void)
{
    char dir[600];
    dir_of(dir, sizeof dir, "stall");
    tsdb_options o;
    tsdb_options_init(&o);
    o.wal_max_batch_rows = 16; /* the 100-row insert goes straight to parts */
    o.merge_idle_s = -1;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid a, b, c;
    OK(tsdb_series(db, "a", &a));
    OK(tsdb_series(db, "b", &b));
    OK(tsdb_series(db, "c", &c));
    OK(tsdb_append(db, b, T0, 1.0f));
    OK(tsdb_flush(db));
    hold h = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, 0, 0, db, a};
    tsdb_test_precommit(db, hold_pre, &h);
    pthread_t ti, ts, tf;
    stall_sw sw = {db, b, 0}, fl = {db, c, 0};
    CK(pthread_create(&ti, NULL, hold_insert, &h) == 0, "pthread_create");
    pthread_mutex_lock(&h.mu);
    while (!h.in)
        pthread_cond_wait(&h.cv, &h.mu);
    pthread_mutex_unlock(&h.mu);
    CK(pthread_create(&ts, NULL, stall_switch, &sw) == 0, "pthread_create");
    usleep(50000); /* the switch is pending */
    CK(!__atomic_load_n(&sw.done, __ATOMIC_SEQ_CST), "the switch went through while a placement held the history");
    CK(pthread_create(&tf, NULL, stall_flush, &fl) == 0, "pthread_create");
    for (int i = 0; i < 300 && !__atomic_load_n(&fl.done, __ATOMIC_SEQ_CST); i++)
        usleep(10000);
    CK(__atomic_load_n(&fl.done, __ATOMIC_SEQ_CST), "a flush waited for a large insert behind a pending switch");
    CK(!__atomic_load_n(&sw.done, __ATOMIC_SEQ_CST), "the switch went through while a placement held the history");
    pthread_mutex_lock(&h.mu);
    h.go = 1;
    pthread_cond_broadcast(&h.cv);
    pthread_mutex_unlock(&h.mu);
    pthread_join(ti, NULL);
    pthread_join(ts, NULL);
    pthread_join(tf, NULL);
    tsdb_test_precommit(db, NULL, NULL);
    CK(cls_of(db, b) == 1, "class %d after the switch", cls_of(db, b));
    OK(tsdb_close(db));
}

int main(int argc, char **argv)
{
    snprintf(base, sizeof base, "%s", argc > 1 ? argv[1] : "/tmp");
    setvbuf(stdout, NULL, _IOLBF, 0);
    const char *only = argc > 2 ? argv[2] : NULL; /* one section: bulk, live, overlap, fresh, wal, damage, retention, format, midplace, far, wtail, opencheck, comeback, daycut, stall */
#define SECTION(name, title, call)                                                                                 \
    if (!only || !strcmp(only, name)) {                                                                            \
        printf("%s\n", title);                                                                                     \
        call;                                                                                                      \
    }
    SECTION("bulk", "bulk loads of every kind", test_bulk());
    SECTION("live", "live series: idle merges, hysteresis, the speed-up guard", test_live());
    SECTION("overlap", "large inserts and replaces racing switches", for (int seed = 0; seed < 6; seed++) test_overlap(seed));
    SECTION("fresh", "a fresh series first written by a replace", test_fresh());
    SECTION("wal", "WAL replay after a switch", test_wal_guard());
    SECTION("damage", "CLASSES damage", test_damage());
    SECTION("retention", "retention", test_retention());
    SECTION("format", "format", test_format());
    SECTION("midplace", "a switch in the middle of a placement", for (int h = 0; h < 3; h++) test_midplace(h));
    SECTION("far", "rows far in the future", test_far(0); test_far(1));
    SECTION("wtail", "the WAL tail of switches", test_wtail(1); test_wtail(0));
    SECTION("opencheck", "parts against CLASSES at open", test_open_check());
    SECTION("comeback", "a series back in a class it left", test_comeback());
    SECTION("daycut", "granules end at day edges in class partitions", test_daycut());
    SECTION("stall", "a pending switch does not hold flushes back", test_stall());
#undef SECTION
    if (fails) {
        printf("test_classes: %d FAILED\n", fails);
        return 1;
    }
    printf("test_classes: all passed\n");
    return 0;
}
