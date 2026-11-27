/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The disk side of a snapshot's install that runs at prepare (snapinst.h; R4 plan D5): what a crash left of an install
 * (v<N>.snap, a swap half done) is finished or undone by snap_leftovers before anything of v<N> is looked at. The
 * install itself is server/snapinst_run.c (tsdbd only: it takes the volume from the manager).
 */
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "tsdb.h"
#include "../src/snapshot.h"
#include "cvolfs.h"
#include "snapinst.h"

/* Active snapshot install progress table (leaf mutex, no I/O; HBS_PROGRESS, R14). */
#define SNAPINST_PROG_MAX 32
static pthread_mutex_t snapinst_prog_mu = PTHREAD_MUTEX_INITIALIZER;
static struct {
    int active;
    struct snapinst_progress p;
} snapinst_prog[SNAPINST_PROG_MAX];

static uint64_t snapinst_wall_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

void snapinst_progress_begin(uint32_t volume, uint64_t bytes_total, uint64_t started_ms)
{
    uint64_t now = snapinst_wall_ms();
    if (!started_ms)
        started_ms = now;
    pthread_mutex_lock(&snapinst_prog_mu);
    int slot = -1;
    for (int i = 0; i < SNAPINST_PROG_MAX; i++) {
        if (snapinst_prog[i].active && snapinst_prog[i].p.volume == volume) {
            slot = i;
            break;
        }
        if (!snapinst_prog[i].active && slot < 0)
            slot = i;
    }
    if (slot >= 0) {
        snapinst_prog[slot].active = 1;
        snapinst_prog[slot].p = (struct snapinst_progress){
            .volume = volume,
            .bytes_done = 0,
            .bytes_total = bytes_total,
            .started_ms = started_ms,
            .last_progress_ms = now,
        };
    }
    pthread_mutex_unlock(&snapinst_prog_mu);
}

void snapinst_progress_update(uint32_t volume, uint64_t bytes_done)
{
    uint64_t now = snapinst_wall_ms();
    pthread_mutex_lock(&snapinst_prog_mu);
    for (int i = 0; i < SNAPINST_PROG_MAX; i++) {
        if (snapinst_prog[i].active && snapinst_prog[i].p.volume == volume) {
            uint64_t tot = snapinst_prog[i].p.bytes_total;
            snapinst_prog[i].p.bytes_done = (tot > 0 && bytes_done > tot) ? tot : bytes_done;
            snapinst_prog[i].p.last_progress_ms = now;
            break;
        }
    }
    pthread_mutex_unlock(&snapinst_prog_mu);
}

void snapinst_progress_touch(uint32_t volume)
{
    uint64_t now = snapinst_wall_ms();
    pthread_mutex_lock(&snapinst_prog_mu);
    for (int i = 0; i < SNAPINST_PROG_MAX; i++) {
        if (snapinst_prog[i].active && snapinst_prog[i].p.volume == volume) {
            snapinst_prog[i].p.last_progress_ms = now;
            break;
        }
    }
    pthread_mutex_unlock(&snapinst_prog_mu);
}

void snapinst_progress_end(uint32_t volume)
{
    pthread_mutex_lock(&snapinst_prog_mu);
    for (int i = 0; i < SNAPINST_PROG_MAX; i++) {
        if (snapinst_prog[i].active && snapinst_prog[i].p.volume == volume) {
            snapinst_prog[i].active = 0;
            break;
        }
    }
    pthread_mutex_unlock(&snapinst_prog_mu);
}

uint32_t snapinst_get_progress(struct snapinst_progress *out, uint32_t cap)
{
    if (!out || cap == 0)
        return 0;
    uint32_t n = 0;
    pthread_mutex_lock(&snapinst_prog_mu);
    for (int i = 0; i < SNAPINST_PROG_MAX && n < cap; i++) {
        if (snapinst_prog[i].active)
            out[n++] = snapinst_prog[i].p;
    }
    pthread_mutex_unlock(&snapinst_prog_mu);
    return n;
}

/* TSDB_REPLD_TEST=snapleftoversrace (test_cluster snapfence): a v<N>.snap without SNAPDONE is left 400 ms before
 * snap_leftovers runs, and one that is gone after that wait was removed by another thread meanwhile, which in
 * snap_leftovers (exists, then an open) is an I/O error: refused as it would be (review F1; mutant snapdropafter) */
static int leftovers_race(const char *root, uint32_t number)
{
#ifdef TSDB_TEST_HOOKS
    const char *te = getenv("TSDB_REPLD_TEST");
    if (!te || !strstr(te, "snapleftoversrace"))
        return 0;
    char snap[PATH_MAX], done[PATH_MAX + 16];
    snprintf(snap, sizeof snap, "%s/v%" PRIu32 ".snap", root, number);
    snprintf(done, sizeof done, "%s/SNAPDONE", snap);
    struct stat sb;
    if (lstat(snap, &sb) != 0 || lstat(done, &sb) == 0)
        return 0;
    usleep(400000);
    if (lstat(snap, &sb) == 0 || errno != ENOENT)
        return 0;
    cvolfs_say("TEST: volume %" PRIu32 ": %s was removed by another thread during its leftovers (snapleftoversrace)",
               number, snap);
    return TSDB_ERR_IO;
#else
    (void)root;
    (void)number;
    return 0;
#endif
}

int snapinst_leftovers(const char *root, uint32_t number)
{
#ifdef TSDB_TEST_HOOKS
    const char *mu = getenv("TSDB_REPLD_MUTANT");
    if (mu && !strcmp(mu, "snapnoleftovers"))
        return 0; /* planted: a crash's v<N>.snap, v<N>.old left as they are */
#endif
    int rc = leftovers_race(root, number);
    if (rc == 0)
        rc = snap_leftovers(root, number);
    if (rc != 0)
        cvolfs_say("volume %" PRIu32 ": what an install left in %s cannot be finished or undone: %s", number, root,
                   tsdb_strerror(rc));
    return rc ? -1 : 0;
}
