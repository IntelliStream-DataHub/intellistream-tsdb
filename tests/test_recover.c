/*
 * In-process recovery of a volume (tsdb_volume_quiesce / idle / reopen, ha.md 15.5 steps 2 and 4.3).
 *
 * The root and one extra volume. A quiesced volume refuses calls with TSDB_ERR_UNAVAILABLE while the other one goes
 * on; the reopen waits until the last call inside the old engine has returned (here: a query held in its callback);
 * rows acknowledged into the WAL and not yet flushed come back from it; a name pointer taken before the reopen is
 * still valid after it (tsdb_series_name: "valid until tsdb_close"); ids and writes go on.
 *
 *   ./build/test_recover [scratch dir]
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
    volatile int inside, release, rc;
} holder;

static int hold_cb(void *ctx, const int64_t *ts, const tsdb_values *v, size_t n)
{
    (void)ts, (void)v, (void)n;
    holder *h = ctx;
    h->inside = 1;
    while (!h->release)
        sleep_ms(5);
    return 0;
}

static void *hold_main(void *arg)
{
    holder *h = arg;
    h->rc = tsdb_query_values(h->db, h->s, 0, 1000000, NULL, hold_cb, h);
    return NULL;
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
    while (!h.inside)
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
    CHECK(tsdb_volume_reopen(db, 1) == TSDB_ERR_INVALID); /* not idle */
    h.release = 1;
    pthread_join(th, NULL);
    CHECK(h.rc == 0);
    CHECK(tsdb_volume_idle(db, 1, 5000) == 0);
    OK(tsdb_volume_reopen(db, 1));
    CHECK(tsdb_volume_reopen(db, 0) == TSDB_ERR_INVALID);

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
    if (failures) {
        fprintf(stderr, "test_recover: %d failures\n", failures);
        return 1;
    }
    printf("test_recover: ok\n");
    return 0;
}
