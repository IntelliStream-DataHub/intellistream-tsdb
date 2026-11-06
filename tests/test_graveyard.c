/*
 * The graveyard: a part a merge replaced is renamed to retired_<name> and deleted obsolete_part_lifetime_s later
 * (a follower on another host may still have it mapped). The lifetime is measured on the monotonic clock: a step of
 * the wall clock (NTP, an operator, a VM resumed) must not delete retired parts early, while a follower reads them
 * (docs/ha.md, B6).
 *
 * The fake time source: this test defines time(), which the library (linked into the test) calls instead of
 * libc's, and adds a settable step to the real wall clock. The monotonic clock is not touched. Code that reads
 * the lifetime from time() sees the step; code that uses CLOCK_MONOTONIC does not.
 *
 *   ./build/test_graveyard [scratch dir]
 */
#define _GNU_SOURCE
#include "tsdb.h"

#include <dirent.h>
#include <stdatomic.h>
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
#define OK(expr)                                                                             \
    do {                                                                                     \
        int rc_ = (expr);                                                                    \
        if (rc_ != 0) {                                                                      \
            fprintf(stderr, "%s:%d: %s = %s\n", __FILE__, __LINE__, #expr, tsdb_strerror(rc_)); \
            failures++;                                                                      \
        }                                                                                    \
    } while (0)

#ifndef __APPLE__
static atomic_llong wall_step_s; /* added to the wall clock the library reads through time() */

time_t time(time_t *t)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    time_t r = ts.tv_sec + (time_t)atomic_load(&wall_step_s);
    if (t)
        *t = r;
    return r;
}
#endif

static char dir[600];

static int retired_files(void)
{
    DIR *d = opendir(dir);
    int n = 0;
    for (struct dirent *e; d && (e = readdir(d));)
        n += strncmp(e->d_name, "retired_", 8) == 0;
    if (d)
        closedir(d);
    return n;
}

static void sleep_ms(int ms)
{
    struct timespec d = {ms / 1000, (ms % 1000) * 1000000L};
    nanosleep(&d, NULL);
}

static int64_t mono_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (int64_t)t.tv_sec * 1000 + t.tv_nsec / 1000000;
}

static tsdb_options opts(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.partition_classes = 0;
    o.flush_interval_ms = 50; /* the flusher thread also purges the graveyard, at every wake-up */
    o.merge_threads = 0;
    o.obsolete_part_lifetime_s = 2;
    return o;
}

/* Waits (at most 10 s) for the retired files to go; returns how long that took in ms, -1 when they stay. */
static int64_t wait_gone(void)
{
    int64_t t0 = mono_ms();
    while (mono_ms() - t0 < 10000) {
        if (retired_files() == 0)
            return mono_ms() - t0;
        sleep_ms(50);
    }
    return -1;
}

/* A wall clock stepped a day ahead (and back) while the retired parts wait: they stay, then go after the lifetime.
 * t0 is taken by the caller before the call that sets the deadline (the merge, the open), so the time measured is at
 * least the parts' real age and a slow open cannot fail correct code. */
static void check_step(const char *what, int want, int64_t t0)
{
    atomic_store(&wall_step_s, 86400);
    sleep_ms(700); /* several flusher cycles */
    if (retired_files() != want) {
        fprintf(stderr, "%s: %d retired parts left after a wall clock step of a day (want %d)\n", what,
                retired_files(), want);
        failures++;
    }
    atomic_store(&wall_step_s, -86400); /* and a step back must not keep them forever either */
    int64_t gone = wait_gone();
    int64_t total = mono_ms() - t0;
    if (gone < 0) {
        fprintf(stderr, "%s: the retired parts were never deleted\n", what);
        failures++;
    } else if (total < 1900) {
        fprintf(stderr, "%s: deleted after %lld ms, before the lifetime of 2 s\n", what, (long long)total);
        failures++;
    }
    atomic_store(&wall_step_s, 0);
}

static void make_retired(tsdb *db)
{
    tsdb_sid id;
    OK(tsdb_series(db, "s", &id));
    int64_t ts[100];
    float v[100];
    for (int round = 0; round < 2; round++) {
        for (int i = 0; i < 100; i++) {
            ts[i] = round * 100 + i;
            v[i] = (float)i;
        }
        OK(tsdb_insert_series(db, id, ts, v, 100)); /* a part of its own */
    }
    OK(tsdb_optimize(db)); /* one part from two: the sources are retired */
}

int main(int argc, char **argv)
{
    const char *base = argc > 1 ? argv[1] : "/tmp";
    char cmd[700];
    snprintf(dir, sizeof dir, "%s/tsdb-graveyard-%d", base, (int)getpid());
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (system(cmd) != 0)
        return 1;
    printf("graveyard lifetime and the wall clock\n");
#ifdef __APPLE__
    printf("  skipped: the fake time source needs ELF symbol interposition\n");
    return 0;
#else
    tsdb_options o = opts();
    tsdb *db = NULL;
    int64_t t0 = mono_ms();
    OK(tsdb_open(dir, &o, &db));
    make_retired(db);
    int n = retired_files();
    CHECK(n > 0);
    check_step("retired by a merge", n, t0);
    tsdb_close(db);
    printf("  parts retired by a merge outlive a step of the wall clock\n");

    /* the same for retired files a restart finds: collected obsolete_part_lifetime_s after the open */
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (system(cmd) != 0)
        return 1;
    OK(tsdb_open(dir, &o, &db));
    make_retired(db);
    tsdb_close(db); /* the lifetime has not passed: they stay for the next open */
    n = retired_files();
    CHECK(n > 0);
    t0 = mono_ms();
    OK(tsdb_open(dir, &o, &db));
    check_step("found at open", n, t0);
    tsdb_close(db);
    printf("  retired parts found at open too\n");

    if (!failures) {
        snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
        if (system(cmd) != 0)
            failures++;
    }
    printf(failures ? "FAILED: %d checks\n" : "all tests passed\n", failures);
    return failures != 0;
#endif
}
