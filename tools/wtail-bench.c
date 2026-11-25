/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * wtail-bench: what the WAL tail of the class switches (classes.c, cls_wal_tail_*) costs a flush, and how long it
 * holds db->lock, which every snapshot and lookup takes.
 *
 * A replica in steady state keeps more flushed segments than CLS_WTAIL_CAP (64), so at every flush its oldest entry
 * folds into the next one: a merge of two id lists, the size of every series flushed since. The bench flushes
 * memtables of --series series each (ids moving through a ring of --union ids, so the folded entry grows to the
 * union) into a bare engine with nothing gone, as the flush path does: the entry is built before any lock, listed
 * under db->lock, and folded outside it (sorted only there, lazily). A probe thread takes and releases db->lock all the
 * while and records how long each acquisition waited: what a reader sees. --scatter 1 lists each memtable's ids out of
 * order (a stride through them), as arrival order does in production; the default lists them ascending.
 *
 *   build/wtail-bench [--series N] [--union U] [--flushes F] [--scatter 0|1]
 */
#include "../src/internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static double now_s(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (double)t.tv_sec + (double)t.tv_nsec * 1e-9;
}

static eng *db;
static _Atomic int stop, measuring;
static double probe_max;
static uint64_t probe_n, probe_1ms, probe_10ms;

static void *probe(void *a)
{
    (void)a;
    while (!atomic_load(&stop)) {
        /* sampled before the wait: a wait the flush's hold caused ends after the flush cleared `measuring` */
        int meas = atomic_load(&measuring);
        double t0 = now_s();
        pthread_mutex_lock(&db->lock);
        double w = now_s() - t0;
        pthread_mutex_unlock(&db->lock);
        if (meas) {
            probe_n++;
            probe_1ms += w > 1e-3;
            probe_10ms += w > 1e-2;
            if (w > probe_max)
                probe_max = w;
        }
        struct timespec ts = {0, 20000}; /* a reader every 20 us */
        nanosleep(&ts, NULL);
    }
    return NULL;
}

int main(int argc, char **argv)
{
    size_t series = 100000, uni = 2100000, flushes = 0, scatter = 0;
    for (int i = 1; i + 1 < argc; i += 2) {
        if (strcmp(argv[i], "--series") == 0)
            series = strtoull(argv[i + 1], NULL, 10);
        else if (strcmp(argv[i], "--union") == 0)
            uni = strtoull(argv[i + 1], NULL, 10);
        else if (strcmp(argv[i], "--flushes") == 0)
            flushes = strtoull(argv[i + 1], NULL, 10);
        else if (strcmp(argv[i], "--scatter") == 0)
            scatter = strtoull(argv[i + 1], NULL, 10);
        else {
            fprintf(stderr, "usage: wtail-bench [--series N] [--union U] [--flushes F] [--scatter 0|1]\n");
            return 2;
        }
    }
    size_t cap = atomic_load(&cls_wtail_cap);
    if (!flushes)
        flushes = cap + 30;
    if (uni < series)
        uni = series;
    db = calloc(1, sizeof *db);
    if (!db)
        return 1;
    pthread_mutex_init(&db->lock, NULL);
    db->wtail_all_max = INT64_MIN;
    pthread_t pt;
    pthread_create(&pt, NULL, probe, NULL);
    double t_new = 0, t_locked = 0, t_fold = 0, t_locked_max = 0;
    size_t nmeas = 0;
    uint64_t next = 0;
    for (size_t f = 0; f < flushes; f++) {
        memtable *m = mt_new(f + 1);
        for (size_t i = 0; i < series; i++) {
            size_t k = scatter ? (size_t)(((uint64_t)i * 999983u) % series) : i; /* 999983 is prime */
            uint32_t id = (uint32_t)((next + k) % uni);
            int64_t ts = (int64_t)f * 1000;
            uint64_t slot = 0;
            if (mt_add(m, NULL, id, &ts, &slot, 8, 1, 0, -1) != 0) {
                fprintf(stderr, "mt_add failed\n");
                return 1;
            }
        }
        next += series;
        bool meas = f > cap; /* every flush past the cap folds */
        atomic_store(&measuring, meas);
        double t0 = now_s();
        struct cls_wtail *e = cls_wal_tail_new(m);
        double t1 = now_s();
        pthread_mutex_lock(&db->lock);
        double t2 = now_s();
        struct cls_wtail *dead = NULL;
        cls_wal_tail_locked(db, e, f + 1, (f + 1) * series, (int64_t)f * 1000, &dead);
        double t3 = now_s();
        pthread_mutex_unlock(&db->lock);
        cls_wal_tail_fold(db, dead);
        double t4 = now_s();
        atomic_store(&measuring, 0);
        if (meas) {
            nmeas++;
            t_new += t1 - t0, t_locked += t3 - t2, t_fold += t4 - t3;
            if (t3 - t2 > t_locked_max)
                t_locked_max = t3 - t2;
        }
        mt_unref(m);
    }
    atomic_store(&stop, 1);
    pthread_join(pt, NULL);
    size_t un = 0;
    for (struct cls_wtail *e = db->wtail; e; e = e->next)
        un = e->n > un ? e->n : un;
    printf("wtail-bench: %zu series per flush%s, union %zu (largest entry %zu), %zu flushes past the cap %zu\n", series,
           scatter ? " (scattered)" : "", uni, un, nmeas, cap);
    printf("  per flush: entry %.3f ms (no lock), listed under db->lock %.3f ms (max %.3f), after the unlock %.3f ms\n",
           1e3 * t_new / (double)nmeas, 1e3 * t_locked / (double)nmeas, 1e3 * t_locked_max,
           1e3 * t_fold / (double)nmeas);
    printf("  a reader's wait for db->lock: max %.3f ms, %llu of %llu waits over 1 ms, %llu over 10 ms\n",
           1e3 * probe_max, (unsigned long long)probe_1ms, (unsigned long long)probe_n,
           (unsigned long long)probe_10ms);
    return 0;
}
