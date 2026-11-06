/*
 * Abandoned requests (tsdb_set_abandon_flag, ha.md 15.3).
 *
 * tsdbd parks a request on a volume whose storage hangs and tells it to discard its work when the volume is
 * declared stuck. The engine checks the flag where a request's effects become durable or visible: a small write
 * before its WAL append (an abandoned row in the WAL would replay after a crash: a refused write turned visible), a
 * large write, delete or replace under publish_lock before any block number is taken. Each test sets the flag on
 * the calling thread (the hook of a large write runs on its thread, inside the window the fence gives) and checks:
 * the call returns TSDB_ERR_ABANDONED, nothing of it is visible, nothing of it is in the WAL or in a part, a
 * reopen agrees, and the engine goes on (a later flush returns: the WAL's row count was not moved by the refusal).
 *
 *   ./build/test_abandon [scratch dir]
 */
#define _GNU_SOURCE
#include "tsdb.h"

#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "portable.h"

void tsdb_test_precommit(tsdb *t, void (*fn)(void *), void *arg); /* the library's test hook (src/internal.h) */

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

static volatile int abandon_flag;

static tsdb_options opts(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.partition_ms = 2000;
    o.granule_rows = 64;
    o.wal_max_batch_rows = 200;
    o.merge_idle_s = -1;
    o.wal_sync_ms = 0;
    return o;
}

static void rmrf(const char *p)
{
    char cmd[1200];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", p);
    if (system(cmd) != 0)
        failures++;
}

static int count_cb(void *ctx, const int64_t *ts, const tsdb_values *v, size_t n)
{
    (void)ts, (void)v;
    *(size_t *)ctx += n;
    return 0;
}

/* rows of the series with start <= ts < end */
static size_t count(tsdb *db, tsdb_sid s, int64_t start, int64_t end)
{
    size_t n = 0;
    OK(tsdb_query_values(db, s, start, end, NULL, count_cb, &n));
    return n;
}

static void sleep_ms(int ms)
{
    struct timespec d = {ms / 1000, (ms % 1000) * 1000000L};
    nanosleep(&d, NULL);
}

static int first_cb(void *ctx, const int64_t *ts, const tsdb_values *v, size_t n)
{
    (void)ts;
    int64_t *out = ctx;
    if (n && *out == -1)
        *out = ((const int64_t *)v->values)[0];
    return 0;
}

static int64_t value_at(tsdb *db, tsdb_sid s, int64_t t)
{
    int64_t out = -1;
    OK(tsdb_query_values(db, s, t, t + 1, NULL, first_cb, &out));
    return out;
}

static void fill(int64_t *ts, int64_t *v, size_t n, int64_t t0, int64_t step, int64_t val)
{
    for (size_t i = 0; i < n; i++)
        ts[i] = t0 + (int64_t)i * step, v[i] = val;
}

/* the hook: the large write is at its commit, the thread's flag goes up */
static void raise_flag(void *arg)
{
    (void)arg;
    abandon_flag = 1;
}

static tsdb *open_fresh(const char *name, tsdb_sid *s)
{
    char dir[1024];
    snprintf(dir, sizeof dir, "%s/%s", scratch, name);
    rmrf(dir);
    tsdb_options o = opts();
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series_create(db, "a", TSDB_INT64, NULL, s));
    return db;
}

static tsdb *reopen(tsdb *db, const char *name)
{
    char dir[1024];
    snprintf(dir, sizeof dir, "%s/%s", scratch, name);
    tsdb_close(db);
    tsdb_options o = opts();
    tsdb *d2;
    OK(tsdb_open(dir, &o, &d2));
    return d2;
}

/* a large insert abandoned at its commit: no part, no rows, the engine goes on, a reopen agrees */
static void test_large_insert(void)
{
    tsdb_sid s;
    tsdb *db = open_fresh("abandon_large", &s);
    int64_t ts[300], v[300];
    tsdb_values col = {TSDB_INT64, v, NULL};
    fill(ts, v, 60, 0, 10, 1);
    OK(tsdb_insert_series_values(db, s, ts, &col, 60)); /* small: acknowledged, stays */
    fill(ts, v, 300, 5000, 10, 2);                      /* 300 >= wal_max_batch_rows: straight to parts */
    abandon_flag = 0;
    tsdb_set_abandon_flag(&abandon_flag);
    tsdb_test_precommit(db, raise_flag, NULL);
    int rc = tsdb_insert_series_values(db, s, ts, &col, 300);
    tsdb_test_precommit(db, NULL, NULL);
    tsdb_set_abandon_flag(NULL);
    CHECK(rc == TSDB_ERR_ABANDONED);
    CHECK(count(db, s, 5000, 9000) == 0);
    CHECK(count(db, s, 0, 1000) == 60);
    fill(ts, v, 10, 6000, 10, 3);
    OK(tsdb_insert_series_values(db, s, ts, &col, 10)); /* the engine goes on */
    OK(tsdb_flush(db));                                 /* and its WAL count is whole (a flush would wait) */
    db = reopen(db, "abandon_large");
    CHECK(count(db, s, 5000, 9000) == 10);
    CHECK(count(db, s, 0, 1000) == 60);
    tsdb_close(db);
    printf("  a large insert abandoned at its commit\n");
}

/* a delete abandoned: the rows stay, a replace abandoned: the old rows stay and the new ones are not there */
static void test_delete_replace(void)
{
    tsdb_sid s;
    tsdb *db = open_fresh("abandon_delete", &s);
    int64_t ts[300], v[300];
    tsdb_values col = {TSDB_INT64, v, NULL};
    fill(ts, v, 100, 0, 10, 1);
    OK(tsdb_insert_series_values(db, s, ts, &col, 100));
    abandon_flag = 0;
    tsdb_set_abandon_flag(&abandon_flag);
    tsdb_test_precommit(db, raise_flag, NULL);
    int rc = tsdb_delete_range(db, s, 0, 1000);
    tsdb_test_precommit(db, NULL, NULL);
    CHECK(rc == TSDB_ERR_ABANDONED);
    CHECK(count(db, s, 0, 1000) == 100);

    abandon_flag = 0;
    fill(ts, v, 300, 0, 3, 9);
    tsdb_test_precommit(db, raise_flag, NULL);
    rc = tsdb_replace_range(db, s, 0, 1000, ts, &col, 300);
    tsdb_test_precommit(db, NULL, NULL);
    tsdb_set_abandon_flag(NULL);
    CHECK(rc == TSDB_ERR_ABANDONED);
    CHECK(count(db, s, 0, 1000) == 100);
    /* with the flag down the same calls go through: the fence and the class freeze were released */
    OK(tsdb_delete_range(db, s, 0, 500));
    CHECK(count(db, s, 0, 1000) == 50);
    db = reopen(db, "abandon_delete");
    CHECK(count(db, s, 0, 1000) == 50);
    tsdb_close(db);
    printf("  a delete and a replace abandoned at their commit\n");
}

/* a small write parked behind a large write's fence, abandoned while it waits: refused, not in the WAL */
typedef struct {
    tsdb *db;
    tsdb_sid s;
    pthread_t th;
    bool started;
    volatile int rc;
    volatile int in_flight;
    int woke_early;
} parked;

static void *small_main(void *arg)
{
    parked *p = arg;
    tsdb_set_abandon_flag(&abandon_flag);
    int64_t ts = 5050, v = 77;
    tsdb_values col = {TSDB_INT64, &v, NULL};
    p->in_flight = 1;
    int rc = tsdb_insert_series_values(p->db, p->s, &ts, &col, 1);
    tsdb_set_abandon_flag(NULL);
    __atomic_store_n(&p->rc, rc, __ATOMIC_SEQ_CST);
    return NULL;
}

static void park_window(void *arg)
{
    parked *p = arg;
    p->rc = 1; /* running */
    p->started = pthread_create(&p->th, NULL, small_main, p) == 0;
    sleep_ms(300); /* it waits on the fence of the large write, which is at its commit */
    p->woke_early = __atomic_load_n(&p->rc, __ATOMIC_SEQ_CST) != 1;
    abandon_flag = 1; /* the volume is declared stuck; the large write itself is on another thread, unflagged */
}

static void test_parked_small(void)
{
    tsdb_sid s;
    tsdb *db = open_fresh("abandon_parked", &s);
    int64_t ts[300], v[300];
    tsdb_values col = {TSDB_INT64, v, NULL};
    fill(ts, v, 300, 5000, 10, 2); /* covers 5050 */
    abandon_flag = 0;
    parked p = {.db = db, .s = s};
    tsdb_test_precommit(db, park_window, &p);
    OK(tsdb_insert_series_values(db, s, ts, &col, 300)); /* this thread has no flag: it completes */
    tsdb_test_precommit(db, NULL, NULL);
    CHECK(p.started);
    if (p.started)
        pthread_join(p.th, NULL);
    CHECK(!p.woke_early);
    CHECK(p.rc == TSDB_ERR_ABANDONED);
    /* the large write holds 5050 with value 2; the abandoned small write (77) must not have overwritten it */
    CHECK(value_at(db, s, 5050) == 2);
    OK(tsdb_flush(db)); /* its WAL count is whole */
    db = reopen(db, "abandon_parked");
    CHECK(count(db, s, 5000, 9000) == 300);
    CHECK(value_at(db, s, 5050) == 2);
    /* a later small write is acknowledged and survives */
    int64_t t1 = 20000, v1 = 5;
    tsdb_values c1 = {TSDB_INT64, &v1, NULL};
    OK(tsdb_insert_series_values(db, s, &t1, &c1, 1));
    db = reopen(db, "abandon_parked");
    CHECK(count(db, s, 20000, 20001) == 1);
    tsdb_close(db);
    printf("  a small write abandoned while parked behind a large write\n");
}

/* without a flag (embedded use) and with it down, nothing is abandoned */
static void test_not_set(void)
{
    tsdb_sid s;
    tsdb *db = open_fresh("abandon_none", &s);
    int64_t ts[300], v[300];
    tsdb_values col = {TSDB_INT64, v, NULL};
    fill(ts, v, 300, 0, 10, 1);
    abandon_flag = 0;
    tsdb_set_abandon_flag(&abandon_flag);
    OK(tsdb_insert_series_values(db, s, ts, &col, 300));
    tsdb_set_abandon_flag(NULL);
    abandon_flag = 1; /* up, but this thread has not registered it */
    OK(tsdb_insert_series_values(db, s, ts, &col, 5));
    CHECK(count(db, s, 0, 4000) == 300);
    tsdb_close(db);
    abandon_flag = 0;
    printf("  no flag, no abandon\n");
}

int main(int argc, char **argv)
{
    if (argc > 1)
        scratch = argv[1];
    test_large_insert();
    test_delete_replace();
    test_parked_small();
    test_not_set();
    if (failures) {
        fprintf(stderr, "test_abandon: %d failures\n", failures);
        return 1;
    }
    printf("test_abandon: ok\n");
    return 0;
}
