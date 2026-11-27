/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Single-server tier FULL hysteresis before ENOSPC (src/tierfill.h; R15 r15-full, E53, §15.7, D7).
 */
#include "tierfill.h"
#include "chunk.h"
#include "chunk_priv.h"
#include "settle.h"
#include "tier.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/statvfs.h>
#include <time.h>

int tierfill_mutant;

static __thread bool tl_refused;
static __thread uint32_t tl_refused_vol;

static int64_t wall_ms_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

void tierfill_update(eng *db, uint32_t fill_pct)
{
    if (!db || !db->ecstate || db->ec_cluster)
        return;
    const tier_settings *cfg = tier_eng_settings(db);
    if (!cfg || !cfg->from_tiers)
        return;
    tierfill_state *fs = &db->ecstate->fill;
    uint32_t hi = cfg->young_high_pct;
    if (hi == 0) {
        /* Setting disabled: clear any previously latched FULL state rather than stranding it true. */
        if (atomic_exchange_explicit(&fs->full, false, memory_order_acq_rel)) {
            atomic_store_explicit(&fs->since_ms, 0, memory_order_release);
            fprintf(stderr, "tsdb: %s: young_high_pct disabled: volume FULL cleared\n", db->dir);
        }
        return;
    }
    uint32_t lo = hi > 2u ? hi - 2u : 1u;
    bool cur = atomic_load_explicit(&fs->full, memory_order_relaxed);
    for (;;) {
        bool next = cur;
        if (!cur && fill_pct >= hi)
            next = true;
        else if (cur && fill_pct < lo)
            next = false;
        if (next == cur)
            return;
        if (atomic_compare_exchange_weak_explicit(&fs->full, &cur, next, memory_order_release, memory_order_relaxed)) {
            if (next) {
                atomic_store_explicit(&fs->since_ms, wall_ms_now(), memory_order_release);
                fprintf(stderr,
                        "tsdb: %s: volume filesystem fill %u%% >= young_high_pct %u%%: volume FULL "
                        "(refusing inserts until < %u%%)\n",
                        db->dir, fill_pct, hi, lo);
            } else {
                atomic_store_explicit(&fs->since_ms, 0, memory_order_release);
                fprintf(stderr, "tsdb: %s: volume filesystem fill %u%% < %u%%: volume FULL cleared\n", db->dir,
                        fill_pct, lo);
            }
            return;
        }
    }
}

void tierfill_eng_seed(eng *db)
{
    if (!db || !db->ecstate || db->ec_cluster)
        return;
    const tier_settings *cfg = tier_eng_settings(db);
    if (cfg && cfg->from_tiers && (cfg->tier_high_pct > 0 || cfg->young_high_pct > 0))
        tierfill_update(db, ecpolicy_sample_fill(db));
}

bool tierfill_is_full(const eng *db)
{
    if (!db || !db->ecstate || db->ec_cluster)
        return false;
#ifdef TSDB_TEST_HOOKS
    if (tierfill_mutant == TIERFILL_MUT_FULLNOFLAG)
        return false;
#endif
    return atomic_load_explicit(&db->ecstate->fill.full, memory_order_acquire);
}

void tierfill_note_refusal(eng *db, uint32_t vol)
{
    if (db && db->ecstate)
        atomic_fetch_add_explicit(&db->ecstate->fill.refusals, 1, memory_order_relaxed);
    tl_refused = true;
    tl_refused_vol = vol;
}

void tierfill_clear_last_refused(void)
{
    tl_refused = false;
    tl_refused_vol = 0;
}

bool tierfill_take_last_refused(uint32_t *vol_out)
{
    if (!tl_refused)
        return false;
    tl_refused = false;
    if (vol_out)
        *vol_out = tl_refused_vol;
    return true;
}

int tierfill_store_bytes(const struct tier *t, int store, uint64_t *used_out, uint64_t *free_out, uint64_t *total_out)
{
#ifdef TSDB_EC
    if (!t || !t->be)
        return -1;
    chunk_store *st = chunk_be_store_ref(t->be, store);
    if (!st)
        return -1;
    int rc = chunk_store_fs_bytes_cached(st, used_out, free_out, total_out);
    chunk_store_unref(st);
    return rc;
#else
    (void)t;
    (void)store;
    (void)used_out;
    (void)free_out;
    (void)total_out;
    return -1;
#endif
}

/* ---------------------------------------------------------------- per-volume detached-capable 1 s sampler */

/* Each open single-server tiered engine gets its own lightweight sampler worker (256 KiB stack).
 * Locking discipline:
 *   - NEVER touches t->cmu or t->eng_active, so a hung NFS volume directory cannot block eng_close or recovery of any
 *     other volume on the same tier, nor stall other volumes' FULL updates.
 *   - w->mu is a pure leaf mutex, NEVER held across statvfs(w->dir).
 *   - Refcounted (refs = 2: owner + worker): tierfill_eng_stop clears w->db = NULL under w->mu and waits up to 500 ms
 *     for clean exit; if the worker is stuck in the kernel, it detaches the thread under w->mu while !w->done and
 *     returns without hanging eng_close / tsdb_close. Whichever side drops the last ref destroys w. */
struct tierfill_worker {
    atomic_int refs;
    pthread_mutex_t mu;
    pthread_cond_t cv;
    pthread_t th;
    char *dir; /* owned copy of db->dir; never touches db during statvfs */
    eng *db;   /* guarded by mu; cleared to NULL by tierfill_eng_stop before db can be freed */
    bool stop;
    bool done;
};

static void worker_unref(struct tierfill_worker *w)
{
    if (!w || atomic_fetch_sub_explicit(&w->refs, 1, memory_order_acq_rel) != 1)
        return;
    pthread_cond_destroy(&w->cv);
    pthread_mutex_destroy(&w->mu);
    free(w->dir);
    free(w);
}

/* Sample integer filesystem fill % (floor of used * 100 / total, using f_bavail) of `dir` without touching `eng`. */
static bool sample_dir_fill(const char *dir, uint32_t *pct_out, bool *real_out)
{
    *real_out = false;
    int ov = ecpolicy_test_fill_override();
    if (ov >= 0) {
        *pct_out = (uint32_t)ov;
        return true;
    }
    struct statvfs sv;
    if (!dir || statvfs(dir, &sv) != 0 || sv.f_blocks == 0)
        return false;
    uint64_t total = (uint64_t)sv.f_blocks;
    uint64_t avail = (uint64_t)sv.f_bavail < total ? (uint64_t)sv.f_bavail : total;
    uint64_t used = total - avail;
    uint32_t pct = used > UINT64_MAX / 100u ? (uint32_t)(used / (total / 100u)) : (uint32_t)((used * 100u) / total);
    if (pct > 100u)
        pct = 100u;
    atomic_fetch_add_explicit(&ecpolicy_fill_samples, 1, memory_order_relaxed);
    *pct_out = pct;
    *real_out = true;
    return true;
}

static bool eng_is_closing(const eng *db)
{
    return atomic_load_explicit(&db->stop, memory_order_acquire) ||
           atomic_load_explicit(&db->quiesced, memory_order_acquire);
}

static void *tierfill_worker_main(void *arg)
{
    struct tierfill_worker *w = arg;
    pthread_mutex_lock(&w->mu);
    for (;;) {
        if (w->stop || !w->db)
            break;
        int poll_ms = atomic_load_explicit(&settle_poll_ms, memory_order_relaxed);
        if (poll_ms <= 0 || poll_ms > 1000)
            poll_ms = 1000;
        else if (poll_ms < 10)
            poll_ms = 10;
        tsdb_cond_timedwait_ms(&w->cv, &w->mu, poll_ms);
        if (w->stop || !w->db)
            break;
        bool want = !eng_is_closing(w->db);
        if (want) {
            const tier_settings *cfg = tier_eng_settings(w->db);
            want = cfg && cfg->from_tiers && (cfg->tier_high_pct > 0 || cfg->young_high_pct > 0);
        }
        if (want && ecpolicy_test_fill_override() < 0 && !ecpolicy_fill_due(w->db))
            want = false; /* keep real statvfs <= 1/s per engine even when settle_poll_ms is small in tests */
        pthread_mutex_unlock(&w->mu);

        uint32_t pct = 0;
        bool real_sample = false;
        bool ok = want && sample_dir_fill(w->dir, &pct, &real_sample);

        pthread_mutex_lock(&w->mu);
        if (ok && !w->stop && w->db && !eng_is_closing(w->db)) {
            ecpolicy_record_fill(w->db, pct, real_sample);
            tierfill_update(w->db, pct);
        }
    }
    w->done = true;
    pthread_cond_broadcast(&w->cv);
    pthread_mutex_unlock(&w->mu);
    worker_unref(w);
    return NULL;
}

int tierfill_eng_start(eng *db)
{
    if (!db || !db->tier || !db->tier->from_tiers || !db->ecstate || db->ec_cluster)
        return 0;
    if (db->ecstate->fill.worker)
        return 0;
    struct tierfill_worker *w = calloc(1, sizeof *w);
    if (!w)
        return TSDB_ERR_NOMEM;
    w->dir = strdup(db->dir);
    if (!w->dir) {
        free(w);
        return TSDB_ERR_NOMEM;
    }
    atomic_init(&w->refs, 2); /* one for db->ecstate->fill.worker, one for the worker thread */
    pthread_mutex_init(&w->mu, NULL);
    tsdb_cond_init_mono(&w->cv);
    w->db = db;

    pthread_attr_t at;
    pthread_attr_init(&at);
    pthread_attr_setstacksize(&at, 256 * 1024);
    int rc = pthread_create(&w->th, &at, tierfill_worker_main, w);
    pthread_attr_destroy(&at);
    if (rc != 0) {
        atomic_store(&w->refs, 1);
        worker_unref(w);
        return TSDB_ERR_NOMEM;
    }
    db->ecstate->fill.worker = w;
    return 0;
}

void tierfill_eng_stop(eng *db)
{
    if (!db || !db->ecstate)
        return;
    struct tierfill_worker *w = db->ecstate->fill.worker;
    if (!w)
        return;
    db->ecstate->fill.worker = NULL;

    pthread_mutex_lock(&w->mu);
    w->stop = true;
    w->db = NULL;
    pthread_cond_broadcast(&w->cv);
    struct timespec dl;
    tsdb_cond_deadline_ms(&dl, 500);
    while (!w->done) {
        if (pthread_cond_timedwait(&w->cv, &w->mu, &dl) != 0)
            break;
    }
    bool join = w->done;
    if (!join)
        pthread_detach(w->th); /* worker cannot exit until it acquires w->mu; safe while !w->done */
    pthread_mutex_unlock(&w->mu);

    if (join)
        pthread_join(w->th, NULL);
    worker_unref(w);
}
