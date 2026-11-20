/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * r7-skip (src/ecskip.h): merges, rewrites, optimize, the TTL work and the backpressure of inserts around .ec and
 * settling parts, with parts marked by eng_test_mark_ec (r7-iface's hook: in memory, nothing on disk; a marked part's
 * bytes are still in its map, so a merge that took one would succeed and the part would be gone: the cases check
 * that the marked part is still listed, by reference, and that its neighbours were merged).
 *  merge     the merge thread merges the ranges on either side of a marked part and never one across it, and never
 *            rewrites the marked part alone once the partition is idle;
 *  optimize  the same for tsdb_optimize, which also rewrites a lone ordinary part between two marked ones, and says
 *            nothing is wrong (a damaged part makes it fail; a marked one does not);
 *  ttl       a partition that has expired whole is retired with its .ec and settling parts (a stuck settle is not
 *            waited for); in a partition that has expired in part the ordinary part is rewritten without its expired
 *            rows and the .ec part is left as it is, queries clamping as before;
 *  drop      tsdb_drop_before retires .ec and settling parts and does not wait for a settle that never ends;
 *  stall     400 .ec parts in a partition do not hold inserts into it (db_wait_merges leaves them out of the count);
 *  walls     nor do marked parts that leave no two unmarked neighbours, .ec or settling (a settle is not waited for);
 *  base      .ec parts do not lower the merge base of the young parts above them (merge_allowed's part count).
 * The merge thread is started by hand after the marks (the engine is opened with merge_threads = 0), so that no
 * merge takes a part before it is marked.
 *
 * Usage: test_ecskip [--mutant NAME] [SCRATCH], NAME one of src/ecskip.h's mutants. Without TSDB_EC it says it is
 * skipped.
 */
#include "internal.h"
#include "ecpart.h"
#include "ecskip.h"

#include <dirent.h>
#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static int failures;
#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);                                               \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

#define DAY 86400000LL
#define HOUR 3600000LL

static char scratch[512];

static void rm_rf(const char *path)
{
    DIR *d = opendir(path);
    if (d) {
        struct dirent *e;
        while ((e = readdir(d))) {
            if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, ".."))
                continue;
            char p[1024];
            snprintf(p, sizeof p, "%s/%s", path, e->d_name);
            struct stat st;
            if (lstat(p, &st) == 0 && S_ISDIR(st.st_mode))
                rm_rf(p);
            else
                unlink(p);
        }
        closedir(d);
    }
    rmdir(path);
}

static void fresh(char *out, size_t len, const char *what)
{
    snprintf(out, len, "%s/test_ecskip_%d_%s", scratch, (int)getpid(), what);
    rm_rf(out);
}

static bool exists(const char *dir, const char *name)
{
    char p[1024];
    snprintf(p, sizeof p, "%s/%s", dir, name);
    return access(p, F_OK) == 0;
}

static int64_t mono_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (int64_t)t.tv_sec * 1000 + t.tv_nsec / 1000000;
}

static int64_t fixed_now;
static int64_t clock_fn(void *arg)
{
    (void)arg;
    return fixed_now;
}

static void base_opts(tsdb_options *o)
{
    tsdb_options_init(o);
    o->merge_threads = 0; /* started by hand after the marks (start_merger) */
    o->create_volumes = 0;
    o->partition_classes = 0; /* one partition per day */
    o->wal_max_batch_rows = 100; /* every put below is a part of its own */
    o->obsolete_part_lifetime_s = 3600; /* retired files stay, to be looked at */
}

/* The engine's own merge thread, started after the marks: eng_open with merge_threads = 0 started none, eng_close
 * stops and joins it like one it started (db->nmergers) */
static void start_merger(eng *db)
{
    db->mergers = calloc(1, sizeof *db->mergers);
    CHECK(db->mergers && tsdb_thread_create(&db->mergers[0], merge_thread_main, db) == 0);
    if (db->mergers)
        db->nmergers = 1;
}

/* one part: n rows from t0, step ms apart, as one insert straight to a part */
static void put(eng *db, uint32_t s, int64_t t0, int64_t step, int n)
{
    int64_t *ts = malloc((size_t)n * sizeof *ts);
    float *v = malloc((size_t)n * sizeof *v);
    CHECK(ts && v);
    if (!ts || !v) {
        free(ts);
        free(v);
        return;
    }
    for (int i = 0; i < n; i++)
        ts[i] = t0 + (int64_t)i * step, v[i] = (float)((t0 / 1000 + i) % 977);
    CHECK(eng_insert_series(db, TSDB_FLOAT32, s, ts, v, (size_t)n) == 0);
    free(ts);
    free(v);
}

typedef struct {
    size_t n;
    double sum;
} acc;

static int acc_fn(void *ctx, const int64_t *ts, const void *v, size_t n)
{
    (void)ts;
    acc *a = ctx;
    for (size_t i = 0; i < n; i++)
        a->sum += ((const float *)v)[i];
    a->n += n;
    return 0;
}

static acc read_range(eng *db, uint32_t s, int64_t lo, int64_t hi)
{
    acc a = {0, 0};
    CHECK(eng_query_raw_ex(db, s, lo, hi, false, 0, acc_fn, &a) == 0);
    return a;
}

static acc read_all(eng *db, uint32_t s) { return read_range(db, s, INT64_MIN, INT64_MAX); }

static size_t nparts(eng *db)
{
    pthread_mutex_lock(&db->lock);
    size_t n = db->nparts;
    pthread_mutex_unlock(&db->lock);
    return n;
}

/* the listed parts of one partition (a day), in list order, each referenced: free with unref_all */
static size_t parts_of(eng *db, int64_t day, part **out, size_t cap)
{
    size_t k = 0;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++)
        if (db->parts[i]->partition == day && k < cap) {
            part_ref(db->parts[i]);
            out[k++] = db->parts[i];
        }
    pthread_mutex_unlock(&db->lock);
    return k;
}

static void unref_all(part **ps, size_t n)
{
    for (size_t i = 0; i < n; i++)
        part_unref(ps[i]);
}

static bool listed(eng *db, const part *p)
{
    bool yes = false;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts && !yes; i++)
        yes = db->parts[i] == p;
    pthread_mutex_unlock(&db->lock);
    return yes;
}

static const char *flag_name(unsigned flags) { return flags & EC_MARK_EC ? ".ec" : "settling"; }

/* merge: A1 A2 X B1 B2 in one partition, X marked; the idle merge thread makes A X B of it and leaves X alone */
static void merge_case(unsigned flags)
{
    char dir[600];
    fresh(dir, sizeof dir, "merge");
    tsdb_options o;
    base_opts(&o);
    o.merge_idle_s = 1; /* idle after a second: every range may merge, and a part only inserts wrote is rewritten */
    eng *db = NULL;
    CHECK(eng_open(dir, &o, &db) == 0);
    if (!db)
        return;
    uint32_t s;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    for (int i = 0; i < 5; i++)
        put(db, s, (int64_t)i * 1000 * 1000, 1000, 1000);
    acc before = read_all(db, s);
    part *ps[8];
    size_t n = parts_of(db, 0, ps, 8);
    CHECK(n == 5);
    if (n != 5) {
        unref_all(ps, n);
        eng_close(db);
        return;
    }
    part *x = ps[2];
    CHECK(eng_test_mark_ec(db, x, flags) == 0);
    start_merger(db);
    /* the two ranges merge (then no two parts are left that may merge, X being a barrier), and X is never rewritten
     * alone: wait for three parts, then as long again for a solo rewrite that must not come */
    int64_t until = mono_ms() + 20000;
    while (nparts(db) > 3 && mono_ms() < until)
        usleep(50000);
    usleep(3000000);
    part *qs[8];
    size_t m = parts_of(db, 0, qs, 8);
    CHECK(m == 3);
    CHECK(listed(db, x));
    if (m == 3) {
        CHECK(qs[1] == x);
        CHECK(qs[0]->level > 0 && qs[2]->level > 0); /* both sides were merged */
        CHECK(qs[0]->min_block == ps[0]->min_block && qs[0]->max_block == ps[1]->max_block);
        CHECK(qs[2]->min_block == ps[3]->min_block && qs[2]->max_block == ps[4]->max_block);
    }
    acc after = read_all(db, s);
    CHECK(after.n == before.n && after.sum == before.sum && before.n == 5000);
    unref_all(qs, m);
    unref_all(ps, n);
    eng_close(db);
    printf("  merge (%s): the merge thread merges the ranges on either side and never across or alone\n",
           flag_name(flags));
    rm_rf(dir);
}

/* optimize: A1 A2 X B1 B2 on day 0, E1 Z E2 on day 2 (X, E1, E2 marked): A X B and E1 Z' E2, and no error */
static void optimize_case(unsigned flags)
{
    char dir[600];
    fresh(dir, sizeof dir, "optimize");
    tsdb_options o;
    base_opts(&o);
    eng *db = NULL;
    CHECK(eng_open(dir, &o, &db) == 0);
    if (!db)
        return;
    uint32_t s;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    for (int i = 0; i < 5; i++)
        put(db, s, (int64_t)i * 1000 * 1000, 1000, 1000);
    for (int i = 0; i < 3; i++)
        put(db, s, 2 * DAY + (int64_t)i * 1000 * 1000, 1000, 1000);
    acc before = read_all(db, s);
    part *a[8], *e[8];
    size_t na = parts_of(db, 0, a, 8), ne = parts_of(db, 2, e, 8);
    CHECK(na == 5 && ne == 3);
    if (na != 5 || ne != 3) {
        unref_all(a, na);
        unref_all(e, ne);
        eng_close(db);
        return;
    }
    CHECK(eng_test_mark_ec(db, a[2], flags) == 0);
    CHECK(eng_test_mark_ec(db, e[0], flags) == 0);
    CHECK(eng_test_mark_ec(db, e[2], flags) == 0);
    CHECK(eng_optimize(db) == 0);
    part *qa[8], *qe[8];
    size_t ma = parts_of(db, 0, qa, 8), me = parts_of(db, 2, qe, 8);
    CHECK(ma == 3 && me == 3);
    CHECK(listed(db, a[2]) && listed(db, e[0]) && listed(db, e[2]));
    if (ma == 3) {
        CHECK(qa[1] == a[2] && qa[0]->level > 0 && qa[2]->level > 0);
        CHECK(qa[0]->min_block == a[0]->min_block && qa[0]->max_block == a[1]->max_block);
    }
    if (me == 3) /* the lone part only inserts wrote is rewritten alone, its neighbours are not */
        CHECK(qe[0] == e[0] && qe[2] == e[2] && qe[1] != e[1] && qe[1]->level > 0 && !listed(db, e[1]));
    acc after = read_all(db, s);
    CHECK(after.n == before.n && after.sum == before.sum && before.n == 8000);
    unref_all(qa, ma);
    unref_all(qe, me);
    unref_all(a, na);
    unref_all(e, ne);
    eng_close(db);
    printf("  optimize (%s): ranges between marked parts merge, a lone ordinary part is rewritten, marked ones never\n",
           flag_name(flags));
    rm_rf(dir);
}

/* ttl: a fixed clock 40 days after the epoch and a retention whose cutoff is day 30 + 12 h. Day 10 (X0 .ec, S0
 * settling and never done, O0) has expired whole: retired, no wait. Day 30: X (.ec, rows 11 h - 13 h) and Y
 * (rows 9 h - 10 h and 14 h - 15 h): Y is rewritten without its expired hour, X is left as it is, and the answers
 * are what the clamp gave before. */
static void ttl_case(void)
{
    char dir[600];
    fresh(dir, sizeof dir, "ttl");
    tsdb_options o;
    base_opts(&o);
    fixed_now = 40 * DAY;
    o.clock_ms = clock_fn;
    o.merge_idle_s = -1;       /* no idle merges or solo rewrites: only the TTL work runs */
    o.retention_merge_s = 1;  /* a part with expired rows is rewritten a second after it was written (0: 3600) */
    eng *db = NULL;
    CHECK(eng_open(dir, &o, &db) == 0);
    if (!db)
        return;
    uint32_t s;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    for (int i = 0; i < 3; i++)
        put(db, s, 10 * DAY + (int64_t)i * HOUR + 500, 1000, 1000);
    put(db, s, 30 * DAY + 11 * HOUR + 500, 1000, 7200);
    {
        int64_t ts[7200];
        float v[7200];
        for (int i = 0; i < 7200; i++) {
            ts[i] = 30 * DAY + (i < 3600 ? 9 * HOUR : 14 * HOUR - 3600 * 1000) + 500 + (int64_t)i * 1000;
            v[i] = (float)(i % 101);
        }
        CHECK(eng_insert_series(db, TSDB_FLOAT32, s, ts, v, 7200) == 0);
    }
    part *old[8], *cur[8];
    size_t nold = parts_of(db, 10, old, 8), ncur = parts_of(db, 30, cur, 8);
    CHECK(nold == 3 && ncur == 2);
    if (nold != 3 || ncur != 2) {
        unref_all(old, nold);
        unref_all(cur, ncur);
        eng_close(db);
        return;
    }
    CHECK(eng_test_mark_ec(db, old[0], EC_MARK_EC) == 0);
    CHECK(eng_test_mark_ec(db, old[1], EC_MARK_SETTLING) == 0); /* a settle that never ends */
    CHECK(eng_test_mark_ec(db, cur[0], EC_MARK_EC) == 0);
    CHECK(eng_set_retention(db, s, fixed_now - (30 * DAY + 12 * HOUR)) == 0);
    acc before = read_all(db, s);
    CHECK(before.n == 3600 + 3600); /* X's 12 h - 13 h and Y's 14 h - 15 h */
    start_merger(db);
    int64_t until = mono_ms() + 20000;
    while ((listed(db, old[0]) || listed(db, old[1]) || listed(db, old[2]) || listed(db, cur[1])) &&
           mono_ms() < until)
        usleep(50000);
    usleep(2500000); /* time for a TTL rewrite of X that must not come */
    CHECK(!listed(db, old[0]) && !listed(db, old[1]) && !listed(db, old[2]));
    for (int i = 0; i < 3; i++) {
        char name[sizeof old[i]->name + 16];
        snprintf(name, sizeof name, "retired_%s", old[i]->name);
        CHECK(exists(dir, name));
    }
    CHECK(listed(db, cur[0]) && !listed(db, cur[1]));
    part *now_[8];
    size_t nnow = parts_of(db, 30, now_, 8);
    CHECK(nnow == 2);
    if (nnow == 2) {
        CHECK(now_[0] == cur[0]);
        CHECK(now_[1] != cur[1] && now_[1]->rows == 3600); /* Y without its expired hour */
        CHECK(cur[0]->rows == 7200);                         /* X with its expired hour, hidden by the clamp */
    }
    acc after = read_all(db, s);
    CHECK(after.n == before.n && after.sum == before.sum);
    unref_all(now_, nnow);
    unref_all(old, nold);
    unref_all(cur, ncur);
    eng_close(db);
    printf("  ttl: an expired partition goes with its .ec and settling parts; in part expired, only the .part is"
           " rewritten\n");
    rm_rf(dir);
}

struct drop_arg {
    eng *db;
    int64_t ts;
    int rc;
    atomic_bool done;
};

static void *drop_thread(void *arg)
{
    struct drop_arg *d = arg;
    d->rc = eng_drop_before(d->db, d->ts);
    atomic_store(&d->done, true);
    return NULL;
}

/* drop: day 0 holds X (.ec), S (settling, and the settle never ends) and O; day 5 holds P. tsdb_drop_before(day 1)
 * retires X, S and O at once and keeps P. */
static void drop_case(void)
{
    char dir[600];
    fresh(dir, sizeof dir, "drop");
    tsdb_options o;
    base_opts(&o);
    eng *db = NULL;
    CHECK(eng_open(dir, &o, &db) == 0);
    if (!db)
        return;
    uint32_t s;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    for (int i = 0; i < 3; i++)
        put(db, s, (int64_t)i * 1000 * 1000, 1000, 1000);
    put(db, s, 5 * DAY, 1000, 1000);
    part *d0[8], *d5[8];
    size_t n0 = parts_of(db, 0, d0, 8), n5 = parts_of(db, 5, d5, 8);
    CHECK(n0 == 3 && n5 == 1);
    if (n0 != 3 || n5 != 1) {
        unref_all(d0, n0);
        unref_all(d5, n5);
        eng_close(db);
        return;
    }
    CHECK(eng_test_mark_ec(db, d0[0], EC_MARK_EC) == 0);
    CHECK(eng_test_mark_ec(db, d0[1], EC_MARK_SETTLING) == 0);
    struct drop_arg d = {.db = db, .ts = DAY, .rc = -999};
    atomic_init(&d.done, false);
    pthread_t t;
    CHECK(pthread_create(&t, NULL, drop_thread, &d) == 0);
    int64_t until = mono_ms() + 5000;
    while (!atomic_load(&d.done) && mono_ms() < until)
        usleep(10000);
    bool in_time = atomic_load(&d.done);
    CHECK(in_time); /* a drop never waits for a settle */
    if (!in_time) { /* end the settle, so that the drop that waits for it returns */
        pthread_mutex_lock(&db->lock);
        d0[1]->settling = false;
        pthread_cond_broadcast(&db->merged_cv);
        pthread_mutex_unlock(&db->lock);
    }
    pthread_join(t, NULL);
    CHECK(d.rc == 0);
    for (int i = 0; i < 3; i++) {
        CHECK(!listed(db, d0[i]) && d0[i]->dropped);
        char name[sizeof d0[i]->name + 16];
        snprintf(name, sizeof name, "retired_%s", d0[i]->name);
        CHECK(exists(dir, name) && !exists(dir, d0[i]->name));
    }
    CHECK(listed(db, d5[0]));
    CHECK(read_range(db, s, 0, DAY).n == 0 && read_range(db, s, 5 * DAY, 6 * DAY).n == 1000);
    unref_all(d0, n0);
    unref_all(d5, n5);
    eng_close(db);
    printf("  drop: .ec and settling parts are retired with their partition, and a settle is not waited for\n");
    rm_rf(dir);
}

/* stall: 400 .ec parts in a partition, inserts into it go on at once. Ordinary parts that can never pair
 * (max_part_bytes 1) keep two neighbours with no barrier between, so a count of the .ec parts would wait out
 * wait_stall_ms. */
static void stall_case(void)
{
    char dir[600];
    fresh(dir, sizeof dir, "stall");
    tsdb_options o;
    base_opts(&o);
    o.merge_idle_s = -1;
    o.max_part_bytes = 1; /* no two parts may merge */
    eng *db = NULL;
    CHECK(eng_open(dir, &o, &db) == 0);
    if (!db)
        return;
    uint32_t s;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    for (int i = 0; i < 400; i++)
        put(db, s, (int64_t)i * 200 * 1000, 1000, 120);
    part *ps[512];
    size_t n = parts_of(db, 0, ps, 512);
    CHECK(n == 400);
    for (size_t i = 0; i < n; i++)
        CHECK(eng_test_mark_ec(db, ps[i], EC_MARK_EC) == 0);
    start_merger(db);
    int was = atomic_load(&wait_stall_ms);
    atomic_store(&wait_stall_ms, 4000);
    int64_t worst = 0;
    for (int i = 0; i < 4; i++) {
        int64_t t0 = mono_ms();
        put(db, s, 80000 * 1000 + (int64_t)i * 200 * 1000, 1000, 120);
        worst = MAX(worst, mono_ms() - t0);
    }
    atomic_store(&wait_stall_ms, was);
    CHECK(worst < 2000);
    CHECK(nparts(db) == 404);
    CHECK(read_all(db, s).n == 404 * 120);
    unref_all(ps, n);
    eng_close(db);
    printf("  stall: 400 .ec parts in a partition do not hold its inserts (slowest %" PRId64 " ms)\n", worst);
    rm_rf(dir);
}

/* walls: 620 parts in a partition, every second one marked (.ec or settling, the last old one marked too), so that
 * no two unmarked parts are neighbours and the count is past PARTS_TO_DELAY_INSERT (311 or 621 counted). An insert
 * there goes on at once: no merge can lower the count, and a settle is not waited for (db_wait_merges). Its new part
 * lands after a marked one, so it has no neighbour to pair with either. */
static void walls_case(unsigned flags)
{
    char dir[600];
    fresh(dir, sizeof dir, "walls");
    tsdb_options o;
    base_opts(&o);
    o.merge_idle_s = -1;
    o.max_part_bytes = 1; /* a wait could only run out: no two parts may merge */
    eng *db = NULL;
    CHECK(eng_open(dir, &o, &db) == 0);
    if (!db)
        return;
    uint32_t s;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    for (int i = 0; i < 620; i++)
        put(db, s, (int64_t)i * 120 * 1000, 500, 120);
    part *ps[640];
    size_t n = parts_of(db, 0, ps, 640);
    CHECK(n == 620);
    for (size_t i = 1; i < n; i += 2)
        CHECK(eng_test_mark_ec(db, ps[i], flags) == 0);
    start_merger(db);
    int was = atomic_load(&wait_stall_ms);
    atomic_store(&wait_stall_ms, 4000);
    int64_t t0 = mono_ms();
    put(db, s, 80000 * 1000, 500, 120);
    int64_t took = mono_ms() - t0;
    atomic_store(&wait_stall_ms, was);
    CHECK(took < 2000);
    CHECK(nparts(db) == 621);
    CHECK(read_all(db, s).n == 621 * 120);
    unref_all(ps, n);
    eng_close(db);
    printf("  walls (%s): a partition whose pairs are all broken by marked parts does not hold its inserts (%" PRId64
           " ms)\n",
           flag_name(flags), took);
    rm_rf(dir);
}

/* base: 50 .ec parts and two young unmarked parts above them in one partition. The objects do not count towards
 * the partition's parts (merge_allowed): the young pair keeps the base of a partition of two parts, so it is not
 * merged within seconds of its writing (with the 50 counted the base would be 2 and the pair merged at once). */
static void base_case(void)
{
    char dir[600];
    fresh(dir, sizeof dir, "base");
    tsdb_options o;
    base_opts(&o);
    o.merge_idle_s = -1; /* no idle merges: only merge_allowed decides */
    eng *db = NULL;
    CHECK(eng_open(dir, &o, &db) == 0);
    if (!db)
        return;
    uint32_t s;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    for (int i = 0; i < 52; i++)
        put(db, s, (int64_t)i * 1000 * 1000, 1000, 120);
    part *ps[64];
    size_t n = parts_of(db, 0, ps, 64);
    CHECK(n == 52);
    for (size_t i = 0; i < n && i < 50; i++)
        CHECK(eng_test_mark_ec(db, ps[i], EC_MARK_EC) == 0);
    start_merger(db);
    usleep(3000000);
    CHECK(nparts(db) == 52);
    if (n == 52)
        CHECK(listed(db, ps[50]) && listed(db, ps[51]));
    CHECK(read_all(db, s).n == 52 * 120);
    unref_all(ps, n);
    eng_close(db);
    printf("  base: .ec parts do not lower the merge base of the young parts above them\n");
    rm_rf(dir);
}

int main(int argc, char **argv)
{
    int a = 1;
    if (a < argc && !strcmp(argv[a], "--mutant")) {
        static const char *names[] = {"",           "mergeec",  "soloec",     "dropwaitsettle", "waitcountec",
                                      "waitnobreak", "waitwall", "waitsettle", "mergecountec"};
        if (a + 1 >= argc) {
            fprintf(stderr, "usage: test_ecskip [--mutant NAME (src/ecskip.h)] [SCRATCH]\n");
            return 2;
        }
        for (int m = 1; m < (int)(sizeof names / sizeof names[0]); m++)
            if (!strcmp(argv[a + 1], names[m]))
                ecskip_mutant = m;
        if (!ecskip_mutant) {
            fprintf(stderr, "test_ecskip: unknown mutant %s\n", argv[a + 1]);
            return 2;
        }
        a += 2;
    }
    snprintf(scratch, sizeof scratch, "%s", a < argc ? argv[a] : "/tmp");
#ifndef TSDB_EC
    printf("test_ecskip: skipped: no TSDB_EC in this build\n");
    return 0;
#else
    merge_case(EC_MARK_EC);
    merge_case(EC_MARK_SETTLING);
    optimize_case(EC_MARK_EC);
    optimize_case(EC_MARK_SETTLING);
    ttl_case();
    drop_case();
    stall_case();
    walls_case(EC_MARK_EC);
    walls_case(EC_MARK_SETTLING);
    base_case();
    if (failures) {
        fprintf(stderr, "test_ecskip: %d failures\n", failures);
        return 1;
    }
    printf("test_ecskip: ok\n");
    return 0;
#endif
}
