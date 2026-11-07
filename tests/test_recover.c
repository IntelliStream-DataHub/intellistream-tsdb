/*
 * In-process recovery of a volume (tsdb_volume_quiesce / idle / reopen, ha.md 15.5 steps 2 and 4.3).
 *
 * The root and one extra volume. A quiesced volume refuses calls with TSDB_ERR_UNAVAILABLE while the other one goes
 * on; the reopen waits until the last call inside the old engine has returned (here: a query held in its callback);
 * rows acknowledged into the WAL and not yet flushed come back from it; a name pointer taken before the reopen is
 * still valid after it (tsdb_series_name: "valid until tsdb_close"); ids and writes go on.
 *
 * Revalidation (ha.md 15.5 steps 3 and 4): a server that comes back with an older state. The test cuts the files
 * itself: the WAL emptied (the rows come back from memory), SERIES emptied (the names are appended again and the ids
 * stay), a part removed (reported, and the volume stays refused until resumed) and a merge taken back (not a loss).
 *
 *   ./build/test_recover [scratch dir]
 */
#define _GNU_SOURCE
#include "tsdb.h"

#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "portable.h"

static int failures;
static const char *scratch = "/tmp";

#define CHECK(cond)                                                                          \
    do {                                                                                     \
        if (!(cond)) {                                                                       \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond);         \
            failures++;                                                                      \
        }                                                                                    \
    } while (0)

#define OK(expr)                                                                             \
    do {                                                                                     \
        int rc_ = (expr);                                                                    \
        if (rc_ != 0) {                                                                      \
            fprintf(stderr, "%s:%d: %s = %d (%s)\n", __FILE__, __LINE__, #expr, rc_, tsdb_strerror(rc_)); \
            failures++;                                                                      \
        }                                                                                    \
    } while (0)

static void sleep_ms(int ms)
{
    struct timespec d = {ms / 1000, (ms % 1000) * 1000000L};
    nanosleep(&d, NULL);
}

static int count_cb(void *ctx, const int64_t *ts, const tsdb_values *v, size_t n)
{
    (void)ts, (void)v;
    *(size_t *)ctx += n;
    return 0;
}

static size_t count(tsdb *db, tsdb_sid s)
{
    size_t n = 0;
    OK(tsdb_query_values(db, s, 0, 1000000, NULL, count_cb, &n));
    return n;
}

/* a reader that stays inside the engine until released */
typedef struct {
    tsdb *db;
    tsdb_sid s;
    int inside, release, rc; /* accessed with __atomic (volatile is not enough for TSan) */
} holder;

static int hold_cb(void *ctx, const int64_t *ts, const tsdb_values *v, size_t n)
{
    (void)ts, (void)v, (void)n;
    holder *h = ctx;
    __atomic_store_n(&h->inside, 1, __ATOMIC_SEQ_CST);
    while (!__atomic_load_n(&h->release, __ATOMIC_SEQ_CST))
        sleep_ms(5);
    return 0;
}

static void *hold_main(void *arg)
{
    holder *h = arg;
    h->rc = tsdb_query_values(h->db, h->s, 0, 1000000, NULL, hold_cb, h);
    return NULL;
}

/* ---------------------------------------------------------- revalidation */

static void sh(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void sh(const char *fmt, ...)
{
    char cmd[4400];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(cmd, sizeof cmd, fmt, ap);
    va_end(ap);
    if (system(cmd) != 0) {
        fprintf(stderr, "failed: %s\n", cmd);
        failures++;
    }
}

static tsdb *reval_open(const char *root, const char *vol, tsdb_sid *s, const char *name)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.wal_max_batch_rows = 200;
    o.merge_idle_s = -1;
    o.partition_classes = 0;
    tsdb *db;
    OK(tsdb_open(root, &o, &db));
    uint32_t vi;
    OK(tsdb_volume_add(db, vol, &vi));
    OK(tsdb_series_at(db, name, vol, s));
    return db;
}

static void stop_volume(tsdb *db)
{
    OK(tsdb_volume_quiesce(db, 1));
    CHECK(tsdb_volume_idle(db, 1, 5000) == 0);
}

static void revalidation(void)
{
    char root[1024], vol[1024];
    snprintf(root, sizeof root, "%s/reval_root", scratch);
    snprintf(vol, sizeof vol, "%s/reval_vol", scratch);
    int64_t ts[600];
    float v[600];
    for (int i = 0; i < 600; i++)
        ts[i] = i, v[i] = (float)i;
    tsdb_volume_report rep;

    /* the WAL lost its newest state: the rows memory holds are written again before the open */
    sh("rm -rf '%s' '%s'", root, vol);
    tsdb_sid s;
    tsdb *db = reval_open(root, vol, &s, "wal");
    OK(tsdb_insert_series(db, s, ts, v, 50));
    OK(tsdb_insert_series(db, s, ts + 50, v + 50, 50));
    stop_volume(db);
    sh("for f in '%s'/wal_*.log; do : > \"$f\"; done", vol);
    OK(tsdb_volume_reopen(db, 1, &rep));
    CHECK(rep.wal_mismatch == 1 && rep.relogged_rows == 100 && !rep.diverged);
    CHECK(count(db, s) == 100);
    OK(tsdb_insert_series(db, s, ts + 100, v + 100, 10));
    CHECK(count(db, s) == 110);
    tsdb_close(db); /* and the log the reopen wrote carries them through another open */
    db = reval_open(root, vol, &s, "wal");
    CHECK(count(db, s) == 110);

    /* a healthy volume: nothing to report */
    stop_volume(db);
    OK(tsdb_volume_reopen(db, 1, &rep));
    CHECK(rep.wal_mismatch == 0 && rep.series_repaired == 0 && rep.lost_parts == 0 && !rep.diverged);
    CHECK(count(db, s) == 110);
    tsdb_close(db);

    /* SERIES lost its entries: appended again from memory, the ids stay, the rows of the WAL find their series */
    sh("rm -rf '%s' '%s'", root, vol);
    tsdb_sid a, b, c;
    db = reval_open(root, vol, &a, "n1");
    OK(tsdb_series_at(db, "n2", vol, &b));
    OK(tsdb_series_at(db, "n3", vol, &c));
    OK(tsdb_insert_series(db, a, ts, v, 10));
    OK(tsdb_insert_series(db, b, ts, v, 20));
    OK(tsdb_insert_series(db, c, ts, v, 30));
    stop_volume(db);
    sh(": > '%s'/SERIES", vol);
    OK(tsdb_volume_reopen(db, 1, &rep));
    CHECK(rep.series_repaired == 3 && !rep.diverged);
    tsdb_sid a2, b2, c2;
    OK(tsdb_series_at(db, "n1", vol, &a2));
    OK(tsdb_series_at(db, "n2", vol, &b2));
    OK(tsdb_series_at(db, "n3", vol, &c2));
    CHECK(a2 == a && b2 == b && c2 == c);
    CHECK(count(db, a) == 10 && count(db, b) == 20 && count(db, c) == 30);
    tsdb_close(db);

    /* a part gone: reported, and refused until the caller accepts */
    sh("rm -rf '%s' '%s'", root, vol);
    db = reval_open(root, vol, &s, "parts");
    OK(tsdb_insert_series(db, s, ts, v, 300)); /* above wal_max_batch_rows: straight into a part */
    CHECK(count(db, s) == 300);
    stop_volume(db);
    sh("mkdir -p '%s/aside' && mv '%s'/p*.part '%s/aside/'", vol, vol, vol);
    OK(tsdb_volume_reopen(db, 1, &rep));
    CHECK(rep.lost_parts == 1 && rep.lost_part_rows == 300 && rep.diverged);
    CHECK(tsdb_insert_series(db, s, ts, v, 1) == TSDB_ERR_UNAVAILABLE);
    OK(tsdb_volume_resume(db, 1));
    CHECK(count(db, s) == 0);
    tsdb_close(db);

    /* a merge taken back: its sources are there again and the merged part is not. The merge carried an overwrite
     * (the third insert) that the sources do not hold, and rows counting cannot see that: lost, DIVERGED */
    sh("rm -rf '%s' '%s'", root, vol);
    db = reval_open(root, vol, &s, "merged");
    OK(tsdb_insert_series(db, s, ts, v, 250));
    OK(tsdb_insert_series(db, s, ts + 250, v + 250, 250));
    sh("mkdir -p '%s/before' && cp '%s'/p*.part '%s/before/'", vol, vol, vol);
    float v2[600];
    for (int i = 0; i < 600; i++)
        v2[i] = 2;
    OK(tsdb_insert_series(db, s, ts + 100, v2, 250)); /* overlaps both, acknowledged */
    OK(tsdb_optimize(db));
    CHECK(count(db, s) == 500);
    stop_volume(db);
    sh("rm -f '%s'/p*.part && cp '%s/before'/p*.part '%s/'", vol, vol, vol);
    OK(tsdb_volume_reopen(db, 1, &rep));
    CHECK(rep.lost_parts == 1 && rep.diverged);
    tsdb_close(db);

    /* a delete merged away and taken back: the deleted rows would come back */
    sh("rm -rf '%s' '%s'", root, vol);
    db = reval_open(root, vol, &s, "deleted");
    OK(tsdb_insert_series(db, s, ts, v, 250));
    OK(tsdb_insert_series(db, s, ts + 250, v + 250, 250));
    sh("mkdir -p '%s/before' && cp '%s'/p*.part '%s/before/'", vol, vol, vol);
    OK(tsdb_delete_range(db, s, 100, 200));
    OK(tsdb_optimize(db));
    CHECK(count(db, s) == 400);
    stop_volume(db);
    sh("rm -f '%s'/p*.part && cp '%s/before'/p*.part '%s/'", vol, vol, vol);
    OK(tsdb_volume_reopen(db, 1, &rep));
    CHECK(rep.diverged && rep.lost_parts >= 1);
    tsdb_close(db);

    /* an acknowledged retention limit lost with RETENTION: the expired rows would come back */
    sh("rm -rf '%s' '%s'", root, vol);
    db = reval_open(root, vol, &s, "ret");
    int64_t now = (int64_t)time(NULL) * 1000, rt[300];
    float rv[300];
    for (int i = 0; i < 300; i++)
        rt[i] = now - 300000 + i * 1000, rv[i] = 1;
    OK(tsdb_insert_series(db, s, rt, rv, 300));
    OK(tsdb_set_retention(db, s, 60000));
    CHECK(count(db, s) < 100);
    stop_volume(db);
    sh("rm -f '%s'/RETENTION", vol);
    OK(tsdb_volume_reopen(db, 1, &rep));
    CHECK(rep.lost_log_bytes > 0 && rep.diverged);
    tsdb_close(db);

    /* an async WAL: rows past the last sync were acknowledged and live in memory only; the storage lost nothing,
     * and the reopen must still keep them */
    sh("rm -rf '%s' '%s'", root, vol);
    {
        tsdb_options o;
        tsdb_options_init(&o);
        o.wal_max_batch_rows = 200;
        o.merge_idle_s = -1;
        o.partition_classes = 0;
        o.wal_sync_ms = 3000;
        OK(tsdb_open(root, &o, &db));
        uint32_t vi;
        OK(tsdb_volume_add(db, vol, &vi));
        OK(tsdb_series_at(db, "async", vol, &s));
    }
    OK(tsdb_insert_series(db, s, ts, v, 100));
    CHECK(count(db, s) == 100);
    stop_volume(db);
    OK(tsdb_volume_reopen(db, 1, &rep));
    CHECK(rep.relogged_rows == 100 && !rep.diverged);
    CHECK(count(db, s) == 100);
    tsdb_close(db);

    /* a reopen that fails after it re-logged can be repeated once the cause is gone */
    sh("rm -rf '%s' '%s'", root, vol);
    db = reval_open(root, vol, &s, "retry");
    OK(tsdb_insert_series(db, s, ts, v, 300)); /* a part */
    OK(tsdb_insert_series(db, s, ts + 250, v, 50)); /* WAL rows */
    stop_volume(db);
    sh("for f in '%s'/wal_*.log; do : > \"$f\"; done; chmod 000 '%s'/p*.part", vol, vol);
    if (geteuid() != 0) { /* root reads a part with mode 000 */
        CHECK(tsdb_volume_reopen(db, 1, &rep) != 0);
        sh("chmod 644 '%s'/p*.part", vol);
        OK(tsdb_volume_reopen(db, 1, &rep));
        CHECK(rep.wal_mismatch == 1 && !rep.diverged);
        CHECK(count(db, s) == 300);
    }
    tsdb_close(db);
}

/* tsdb_latest (merged from latest-s1) routes like the other calls: it holds a reference on the engine of every id
 * until it returns (else a quiesce never gets idle), and an id on a quiesced volume is UNAVAILABLE, not NOTFOUND. */
static void latest_quiesced(void)
{
    char root[1024], vol[1024];
    snprintf(root, sizeof root, "%s/latest_q_root", scratch);
    snprintf(vol, sizeof vol, "%s/latest_q_vol", scratch);
    sh("rm -rf '%s' '%s'", root, vol);
    tsdb_sid s, r;
    tsdb *db = reval_open(root, vol, &s, "lat");
    OK(tsdb_series_at(db, "rootlat", root, &r));
    int64_t ts[10];
    float v[10];
    for (int i = 0; i < 10; i++)
        ts[i] = i, v[i] = (float)i;
    OK(tsdb_insert_series(db, s, ts, v, 10));
    OK(tsdb_insert_series(db, r, ts, v, 10));
    tsdb_sid ids[2] = {s, r};
    tsdb_latest_result out[2];
    OK(tsdb_latest(db, ids, 2, NULL, out));
    CHECK(out[0].status == 0 && out[0].ts == 9 && out[1].status == 0 && out[1].ts == 9);
    CHECK(tsdb_volume_quiesce(db, 1) == 0);
    CHECK(tsdb_volume_idle(db, 1, 2000) == 0); /* a leaked reference of the call above keeps it busy */
    OK(tsdb_latest(db, ids, 2, NULL, out));
    CHECK(out[0].status == TSDB_ERR_UNAVAILABLE);
    CHECK(out[1].status == 0 && out[1].ts == 9); /* the other volume goes on */
    OK(tsdb_volume_resume(db, 1));
    OK(tsdb_latest(db, ids, 2, NULL, out));
    CHECK(out[0].status == 0 && out[0].ts == 9);
    tsdb_close(db);
}


/* ------------------------------------------------- security review 0.9.1 */

/* src/volumes.c, src/db.c: test hooks (internal.h) */
void tsdb_test_reopen_hook(void (*fn)(void *, int phase), void *arg);
void *tsdb_test_meta_hold(tsdb *t, tsdb_sid id);
int tsdb_test_meta_use(void *hold);
void tsdb_test_meta_release(void *hold);
void tsdb_test_before_parts(void (*fn)(void *), void *arg);
extern atomic_int merge_fault_ms;

/* A name or type lookup (route_meta) answers on a quiesced volume and takes a reference on its engine. One that took
 * it after the reopen's check that the volume is idle must be waited for: the reopen freed the old engine under it
 * (a use after free that tsdbd's requests could trigger during domain recovery; ASan reports it). */
typedef struct {
    tsdb *db;
    tsdb_sid s;
    void *hold;
    pthread_t th;
    int released; /* __atomic */
    int type;
} late;

static void *late_main(void *arg)
{
    late *l = arg;
    sleep_ms(300); /* a preempted caller: the reopen has swapped the engines meanwhile */
    l->type = tsdb_test_meta_use(l->hold); /* reads the old engine */
    __atomic_store_n(&l->released, 1, __ATOMIC_SEQ_CST);
    tsdb_test_meta_release(l->hold);
    return NULL;
}

static void late_hook(void *arg, int phase)
{
    late *l = arg;
    if (phase != 0)
        return;
    l->hold = tsdb_test_meta_hold(l->db, l->s); /* after the check at the top of the reopen */
    CHECK(l->hold != NULL);
    CHECK(pthread_create(&l->th, NULL, late_main, l) == 0);
}

static void meta_reference_at_reopen(void)
{
    char root[1024], vol[1024];
    snprintf(root, sizeof root, "%s/metaref_root", scratch);
    snprintf(vol, sizeof vol, "%s/metaref_vol", scratch);
    sh("rm -rf '%s' '%s'", root, vol);
    late l = {0};
    tsdb *db = reval_open(root, vol, &l.s, "meta");
    l.db = db;
    int64_t ts[10] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    float v[10] = {0};
    OK(tsdb_insert_series(db, l.s, ts, v, 10));
    stop_volume(db);
    tsdb_test_reopen_hook(late_hook, &l);
    OK(tsdb_volume_reopen(db, 1, NULL));
    tsdb_test_reopen_hook(NULL, NULL);
    CHECK(__atomic_load_n(&l.released, __ATOMIC_SEQ_CST) == 1); /* the old engine outlived its last reference */
    pthread_join(l.th, NULL);
    CHECK(l.type == TSDB_FLOAT32);
    OK(tsdb_volume_resume(db, 1));
    tsdb_type ty;
    OK(tsdb_series_type(db, l.s, &ty));
    CHECK(ty == TSDB_FLOAT32);
    tsdb_close(db);
}

/* the new engine is open: time for its merge threads to merge across the gap of the lost part, as they do whenever
 * the caller is not scheduled for a moment */
static void let_merges_run(void *arg, int phase)
{
    (void)arg;
    if (phase == 1)
        sleep_ms(500);
}

static void clear_fault(void *arg)
{
    (void)arg;
    atomic_store(&merge_fault_ms, 0);
}

/* The parts the storage lost are compared with the new engine's parts before its merge threads start. After they run,
 * a merge across the gap of a lost part makes the part look covered and the loss goes unreported. */
static void merge_clears_loss(void)
{
    char root[1024], vol[1024];
    snprintf(root, sizeof root, "%s/lostmid_root", scratch);
    snprintf(vol, sizeof vol, "%s/lostmid_vol", scratch);
    int64_t ts[300];
    float v[300];
    for (int i = 0; i < 300; i++)
        v[i] = (float)i;
    for (int round = 0; round < 5; round++) {
        sh("rm -rf '%s' '%s'", root, vol);
        tsdb_sid s;
        tsdb *db = reval_open(root, vol, &s, "lostmid");
        atomic_store(&merge_fault_ms, 1); /* the old engine's merges fail: its parts stay as they are */
        for (int k = 0; k < 40; k++) { /* 40 parts of one partition, one block range each */
            for (int i = 0; i < 300; i++)
                ts[i] = k * 1000 + i;
            OK(tsdb_insert_series(db, s, ts, v, 300));
        }
        stop_volume(db);
        /* the new engine merges at once (the hook runs in its open, after the old engine halted) */
        tsdb_test_before_parts(clear_fault, NULL);
        tsdb_test_reopen_hook(let_merges_run, NULL);
        sh("cd '%s' && f=$(ls p*.part | sort -t_ -k2,2n | sed -n 20p) && rm -f \"$f\"", vol);
        tsdb_volume_report rep;
        OK(tsdb_volume_reopen(db, 1, &rep));
        tsdb_test_before_parts(NULL, NULL);
        tsdb_test_reopen_hook(NULL, NULL);
        atomic_store(&merge_fault_ms, 0);
        CHECK(rep.lost_parts == 1 && rep.lost_part_rows == 300 && rep.diverged);
        OK(tsdb_volume_resume(db, 1));
        CHECK(count(db, s) == 39 * 300);
        tsdb_close(db);
    }
}

int main(int argc, char **argv)
{
    if (argc > 1)
        scratch = argv[1];
    char root[1024], vol[1024], cmd[2200];
    snprintf(root, sizeof root, "%s/recover_root", scratch);
    snprintf(vol, sizeof vol, "%s/recover_vol", scratch);
    snprintf(cmd, sizeof cmd, "rm -rf '%s' '%s'", root, vol);
    if (system(cmd) != 0)
        return 1;
    tsdb_options o;
    tsdb_options_init(&o);
    o.wal_max_batch_rows = 200;
    o.merge_idle_s = -1;
    tsdb *db;
    OK(tsdb_open(root, &o, &db));
    uint32_t vi;
    OK(tsdb_volume_add(db, vol, &vi));
    CHECK(vi == 1);
    tsdb_sid a, b;
    OK(tsdb_series_at(db, "root_series", root, &a));
    OK(tsdb_series_at(db, "vol_series", vol, &b));
    CHECK(TSDB_SID_VOLUME(b) == 1);
    int64_t ts[50];
    float v[50];
    for (int i = 0; i < 50; i++)
        ts[i] = i, v[i] = (float)i;
    OK(tsdb_insert_series(db, a, ts, v, 50));
    OK(tsdb_insert_series(db, b, ts, v, 50)); /* small: acknowledged in the WAL, not flushed */
    const char *name_before = tsdb_series_name(db, b);
    CHECK(name_before && strcmp(name_before, "vol_series") == 0);

    /* a reader inside the volume's engine */
    holder h = {.db = db, .s = b};
    pthread_t th;
    CHECK(pthread_create(&th, NULL, hold_main, &h) == 0);
    while (!__atomic_load_n(&h.inside, __ATOMIC_SEQ_CST))
        sleep_ms(5);
    OK(tsdb_volume_quiesce(db, 1));
    CHECK(tsdb_insert_series(db, b, ts, v, 1) == TSDB_ERR_UNAVAILABLE);
    int64_t t2[2] = {100, 101};
    float v2[2] = {1, 2};
    tsdb_sid both[2] = {a, b};
    CHECK(tsdb_insert_series(db, a, t2, v2, 2) == 0); /* the other volume goes on */
    {
        tsdb_values col = {TSDB_FLOAT32, v2, NULL};
        CHECK(tsdb_insert_values(db, both, t2, &col, 2) == TSDB_ERR_UNAVAILABLE);
    }
    size_t n = 0;
    CHECK(tsdb_query_values(db, b, 0, 1000, NULL, count_cb, &n) == TSDB_ERR_UNAVAILABLE);
    CHECK(tsdb_flush(db) == TSDB_ERR_UNAVAILABLE); /* the quiesced volume is not flushed, the other one is */
    CHECK(tsdb_series_name(db, b) == name_before); /* dictionaries answer */
    CHECK(tsdb_volume_idle(db, 1, 200) == 1);      /* the reader is still inside */
    CHECK(tsdb_volume_reopen(db, 1, NULL) == TSDB_ERR_INVALID); /* not idle */
    __atomic_store_n(&h.release, 1, __ATOMIC_SEQ_CST);
    pthread_join(th, NULL);
    CHECK(h.rc == 0);
    CHECK(tsdb_volume_idle(db, 1, 5000) == 0);
    OK(tsdb_volume_reopen(db, 1, NULL));
    CHECK(tsdb_volume_reopen(db, 0, NULL) == TSDB_ERR_INVALID);

    CHECK(count(db, b) == 50); /* from the WAL (or the flush at the reopen's replay) */
    CHECK(count(db, a) == 52);
    CHECK(strcmp(name_before, "vol_series") == 0); /* the pointer taken before is valid after */
    CHECK(tsdb_series_name(db, b) && strcmp(tsdb_series_name(db, b), "vol_series") == 0);
    tsdb_sid b2;
    OK(tsdb_series_at(db, "vol_series", vol, &b2));
    CHECK(b2 == b); /* ids go on */
    OK(tsdb_insert_series(db, b, t2, v2, 2));
    CHECK(count(db, b) == 52);
    OK(tsdb_volume_quiesce(db, 1));
    OK(tsdb_volume_resume(db, 1));
    OK(tsdb_insert_series(db, b, t2, v2, 2));
    tsdb_close(db);
    OK(tsdb_open(root, &o, &db));
    OK(tsdb_series_at(db, "vol_series", vol, &b));
    CHECK(count(db, b) == 52);
    tsdb_close(db);
    revalidation();
    latest_quiesced();
    meta_reference_at_reopen();
    merge_clears_loss();
    if (failures) {
        fprintf(stderr, "test_recover: %d failures\n", failures);
        return 1;
    }
    printf("test_recover: ok\n");
    return 0;
}
