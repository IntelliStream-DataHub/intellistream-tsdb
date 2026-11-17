/* SPDX-License-Identifier: AGPL-3.0-only */
/* Crash points: see crashpt.h. */
#include "crashpt.h"

#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

const char *const crash_point_names[CRASH_POINT_COUNT] = {
#define X(id, name) name,
    CRASH_POINTS(X)
#undef X
};

_Atomic int crashpt_state = -1;
static int armed_id = -1; /* written once, before crashpt_state leaves -1 (release), read after an acquire load */
static long armed_n = 1;
static pthread_once_t arm_once = PTHREAD_ONCE_INIT;
static _Atomic long hits;
static char armed_after[PATH_MAX]; /* "@path": passes count only once this file exists; written once, like armed_id */
static _Atomic int after_seen;

static void say(const char *msg, const char *name, long n)
{
    char line[200];
    int len = snprintf(line, sizeof line, "tsdbd: crash point %s (%s %ld)\n", name, msg, n);
    if (len > 0)
        (void)!write(2, line, (size_t)len);
}

/* Reads TSDB_CRASH_AT once (pthread_once: the points sit in several threads, and `armed_id` is written here only). */
static void arm_from_env(void)
{
    const char *e = getenv("TSDB_CRASH_AT");
    if (!e || !*e) {
        atomic_store_explicit(&crashpt_state, 0, memory_order_release);
        return;
    }
    char spec[PATH_MAX + 96];
    snprintf(spec, sizeof spec, "%s", e);
    char *at = strchr(spec, '@');
    if (at) {
        *at = 0;
        if (!at[1]) {
            say("empty trigger path in TSDB_CRASH_AT, exiting with status 2: ", spec, 0);
            _exit(2);
        }
        snprintf(armed_after, sizeof armed_after, "%s", at + 1);
    }
    long n = 1;
    char *colon = strchr(spec, ':');
    if (colon) {
        *colon = 0;
        char *end;
        n = strtol(colon + 1, &end, 10);
        if (*end || n < 1) {
            say("bad count in TSDB_CRASH_AT, exiting with status 2: ", spec, n);
            _exit(2);
        }
    }
    int id = -1;
    for (int i = 0; i < CRASH_POINT_COUNT; i++)
        if (!strcmp(spec, crash_point_names[i]))
            id = i;
    if (id < 0) {
        say("unknown, exiting with status 2: ", spec, 0);
        _exit(2);
    }
    armed_id = id;
    armed_n = n;
    atomic_store_explicit(&crashpt_state, 1, memory_order_release);
}

void crashpt_slow(enum crash_point_id id)
{
    pthread_once(&arm_once, arm_from_env);
    if (atomic_load_explicit(&crashpt_state, memory_order_acquire) != 1 || id != armed_id)
        return;
    if (armed_after[0] && !atomic_load_explicit(&after_seen, memory_order_relaxed)) {
        struct stat st;
        if (stat(armed_after, &st) != 0)
            return; /* not triggered yet: this pass does not count */
        atomic_store_explicit(&after_seen, 1, memory_order_relaxed);
    }
    long h = atomic_fetch_add(&hits, 1) + 1;
    if (h != armed_n)
        return;
    char line[200];
    int len = snprintf(line, sizeof line, "tsdbd: crash point %s (hit %ld): exiting with status %d\n",
                       crash_point_names[id], h, CRASH_EXIT_STATUS);
    if (len > 0)
        (void)!write(2, line, (size_t)len);
    _exit(CRASH_EXIT_STATUS);
}
