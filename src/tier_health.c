/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Runtime tier store health: per-store O_DIRECT probes, stall/I/O-error detection, queued attach/heal worker,
 * missed-delete sweep, and --health-domain integration (R15 r15-health, design-cluster-data.md §15.7, PLAN.md D8).
 *
 * Patterned after src/health.c:
 *  - Each active store gets its own detached probe worker thread(s) (at most 2, a second spawned if the first stays
 *    stuck) that writes, fdatasyncs, and reads back a 4 KiB O_DIRECT probe file (<store>/PROBE.tier) with ZERO engine
 *    or tier locks held.
 *  - One monitor thread (no I/O) checks the clock every 50 ms: a probe outstanding >= tier_stall_s (default 10 s) or
 *    an in-service I/O error drives the store DOWN (chunk_be_set_down -> DOWN_STALLED / DOWN_IOERR) so new reads
 *    decode around it and settles pause.
 *  - After N consecutive fast healthy probes (default 3) — or when a --health-domain covering the store recovers —
 *    the store index is queued onto a dedicated joinable attach/heal worker thread (NEVER called on the probe or
 *    health-monitor callback thread). The worker verifies store META (id + db_id), runs chunk_be_attach (healing
 *    missing/damaged chunks without resurrecting deleted objects), sweeps any orphan chunks whose deletes the store
 *    missed while DOWN, reopens cached object handles, and resumes settles & GC once every store is UP.
 *
 * Lock order: th->mu is a leaf (never held across I/O, chunk_be_* calls, or engine locks).
 */
#define _GNU_SOURCE
#include "tier.h"
#include "chunk.h"
#include "chunk_priv.h"
#include "eccache.h"
#include "ecgc.h"
#include "ecpart.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifdef TSDB_EC

#define TH_PROBE_BYTES 4096
#define TH_MAX_WORKERS 2
#define TH_STALL_MS_DEFAULT 10000
#define TH_EVERY_MS_DEFAULT 1000
#define TH_OK_NEED_DEFAULT 3
#define TH_FAIL_NEED_DEFAULT 1
#define TH_FAST_MS_DEFAULT 2000

static atomic_int g_test_stall_ms;
static atomic_int g_test_every_ms;
static atomic_int g_test_ok_need;
static atomic_int g_test_fast_ms;
atomic_uint_fast64_t tier_health_attach_attempts;

typedef struct th_opts {
    int stall_ms;
    int every_ms;
    int ok_need;
    int fail_need;
    int fast_ms;
    int spawn_ms;
    bool disabled;
} th_opts;

typedef struct th_store {
    int index;
    bool active; /* true iff opened UP: LOST / missing-at-open stores are never probed or auto-attached */
    char *path;
    uint8_t id[16];
    int64_t inflight_ms[TH_MAX_WORKERS];
    int64_t started_ms[TH_MAX_WORKERS];
    bool alive[TH_MAX_WORKERS];
    int running;
    int fails;
    int ok_streak;
    int64_t next_probe_ms;
    bool probe_now;
    bool domain_down;
    bool attach_queued;
    bool attaching;
    bool attach_perm_err;
    bool wait_other_up; /* REPLACING waiting for another DOWN/LOST store to recover: do not spin-retry settle */
    int64_t retry_after_ms;
    int64_t last_ioerr_attach_ms; /* rate-limits repeated DOWN_IOERR -> attach -> DOWN_IOERR flaps */
    int ioerr_flaps;
    int attach_fails; /* exponential backoff for persistent attach/settle errors (EPOCH_UNSURE, FENCED, ENOSPC) */
    struct tier_health *th;
    struct th_store *next_retired; /* retired on store replacement so D-state old probes never touch the new slot */
} th_store;

struct tier_health {
    tier *t;
    th_opts o;
    pthread_mutex_t mu;
    pthread_cond_t cv;        /* wakes probe workers and quiesce */
    th_store *stores[TIERS_STORES_MAX]; /* individually allocated slots: safe across runtime store additions */
    th_store *retired;        /* old th_store instances retired on replacement; freed at close when !stuck */
    int nstores;
    bool closing;
    atomic_bool closing_atomic; /* lock-free mirror of closing for chunk_heal_opts.cancel */
    bool quiesced;
    bool stuck;        /* a detached probe thread was still running after deadline (leak th only) */
    bool need_reopen; /* off-thread eccache_reopen_all request from tier_store_set_down */
    void (*wake_fn)(void *); /* wakes tierctl's single tier worker */
    void *wake_ctx;
    pthread_t monitor_th;
    bool monitor_started;
};

static void th_wake_locked(struct tier_health *th)
{
    if (!th->closing && th->wake_fn)
        th->wake_fn(th->wake_ctx);
}

static int64_t th_mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static int64_t th_wall_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void th_load_opts(th_opts *o)
{
    o->stall_ms = TH_STALL_MS_DEFAULT;
    o->every_ms = TH_EVERY_MS_DEFAULT;
    o->ok_need = TH_OK_NEED_DEFAULT;
    o->fail_need = TH_FAIL_NEED_DEFAULT;
    o->fast_ms = TH_FAST_MS_DEFAULT;
    o->spawn_ms = TH_STALL_MS_DEFAULT;
    o->disabled = false;
#ifdef TSDB_TEST_HOOKS
    bool explicit_probe = false;
    const char *env = getenv("TSDB_TEST_TIER_HEALTH");
    if (env && *env) {
        char *copy = strdup(env), *save = NULL;
        if (copy) {
            for (char *kv = strtok_r(copy, ",", &save); kv; kv = strtok_r(NULL, ",", &save)) {
                char *eq = strchr(kv, '=');
                if (!eq)
                    continue;
                *eq = '\0';
                int v = atoi(eq + 1);
                if (strcmp(kv, "probe") == 0) {
                    o->disabled = (v == 0);
                    explicit_probe = true;
                    continue;
                }
                if (v <= 0)
                    continue;
                if (strcmp(kv, "stall_ms") == 0)
                    o->stall_ms = v;
                else if (strcmp(kv, "stall_s") == 0)
                    o->stall_ms = v * 1000;
                else if (strcmp(kv, "every_ms") == 0)
                    o->every_ms = v;
                else if (strcmp(kv, "ok_need") == 0)
                    o->ok_need = v;
                else if (strcmp(kv, "fail_need") == 0)
                    o->fail_need = v;
                else if (strcmp(kv, "fast_ms") == 0)
                    o->fast_ms = v;
                else if (strcmp(kv, "spawn_ms") == 0)
                    o->spawn_ms = v;
            }
            free(copy);
        }
    }
    /* Under libcrashfs.so fault injection, disable background probes unless a test explicitly opts in (probe=1) so
     * periodic PROBE.tier fdatasyncs never inject timing-dependent sync steps into crashfs enumerations. */
    if (!explicit_probe && getenv("CRASHFS_ROOT") != NULL)
        o->disabled = true;
    int v;
    if ((v = atomic_load_explicit(&g_test_stall_ms, memory_order_relaxed)) > 0)
        o->stall_ms = v;
    if ((v = atomic_load_explicit(&g_test_every_ms, memory_order_relaxed)) < 0)
        o->disabled = true;
    else if (v > 0)
        o->every_ms = v;
    if ((v = atomic_load_explicit(&g_test_ok_need, memory_order_relaxed)) > 0)
        o->ok_need = v;
    if ((v = atomic_load_explicit(&g_test_fast_ms, memory_order_relaxed)) > 0)
        o->fast_ms = v;
#endif
    if (o->fast_ms > o->stall_ms)
        o->fast_ms = o->stall_ms / 2 > 50 ? o->stall_ms / 2 : 50;
    if (o->spawn_ms > o->stall_ms)
        o->spawn_ms = o->stall_ms;
}

/* ------------------------------------------------------------ probe I/O (zero locks held) */

/* O_DIRECT write + fdatasync + read-back of <dir>/PROBE.tier (4 KiB). Falls back without O_DIRECT on EINVAL (tmpfs).
 * Returns 0 on success or a positive errno on failure. */
static int store_probe_io(const char *dir, uint64_t seq)
{
    char path[PATH_MAX];
    int r = snprintf(path, sizeof path, "%s/PROBE.tier", dir);
    if (r <= 0 || (size_t)r >= sizeof path)
        return ENAMETOOLONG;
    void *wbuf = NULL, *rbuf = NULL;
    if (posix_memalign(&wbuf, TH_PROBE_BYTES, TH_PROBE_BYTES) != 0 ||
        posix_memalign(&rbuf, TH_PROBE_BYTES, TH_PROBE_BYTES) != 0) {
        free(wbuf);
        free(rbuf);
        return ENOMEM;
    }
    memset(wbuf, 0, TH_PROBE_BYTES);
    memset(rbuf, 0, TH_PROBE_BYTES);
    snprintf((char *)wbuf, TH_PROBE_BYTES, "tsdb tier probe %" PRIu64 " %" PRId64 "\n", seq, th_wall_ms());

    int fd = open(path, O_RDWR | O_CREAT | O_DIRECT | O_CLOEXEC, 0644);
    if (fd < 0 && errno == EINVAL)
        fd = open(path, O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    int err = 0;
    if (fd < 0) {
        err = errno ? errno : EIO;
    } else {
        ssize_t w = pwrite(fd, wbuf, TH_PROBE_BYTES, 0);
        if (w != TH_PROBE_BYTES)
            err = (w < 0 && errno) ? errno : EIO;
        else if (fdatasync(fd) != 0)
            err = errno ? errno : EIO;
        else {
            ssize_t rd = pread(fd, rbuf, TH_PROBE_BYTES, 0);
            if (rd != TH_PROBE_BYTES)
                err = (rd < 0 && errno) ? errno : EIO;
            else if (memcmp(rbuf, wbuf, TH_PROBE_BYTES) != 0)
                err = EIO;
        }
        close(fd);
    }
    free(wbuf);
    free(rbuf);
    return err;
}

/* ------------------------------------------------------------ probe & monitor threads */

typedef struct probe_arg {
    th_store *st;
    int slot;
} probe_arg;

/* Non-outage errors (matching src/health.c): a full pool/disk (ENOSPC/EDQUOT) or local memory/path limit must NOT
 * take a store DOWN for reads (ZFS returns ENOSPC on 4 KiB overwrite when a pool is full). */
static bool th_is_storage_err(int err)
{
    return err != 0 && err != ENOSPC && err != EDQUOT && err != ENOMEM && err != ENAMETOOLONG;
}

static void *probe_main(void *arg)
{
    probe_arg *pa = arg;
    th_store *st = pa->st;
    int slot = pa->slot;
    free(pa);
    struct tier_health *th = st->th;
    uint64_t seq = 0;

    for (;;) {
        pthread_mutex_lock(&th->mu);
        int64_t now = th_mono_ms();
        while (!th->closing && st->active && !st->probe_now && now < st->next_probe_ms) {
            tsdb_cond_timedwait_ms(&th->cv, &th->mu, (uint64_t)(st->next_probe_ms - now));
            now = th_mono_ms();
        }
        bool closing = th->closing || !st->active || !st->path;
        _Static_assert(TH_MAX_WORKERS == 2, "surplus check assumes TH_MAX_WORKERS == 2");
        bool surplus = st->running > 1 && st->started_ms[slot] <= st->started_ms[1 - slot] && st->alive[1 - slot];
        st->probe_now = false;
        if (closing || surplus) {
            st->running--;
            st->alive[slot] = false;
            pthread_cond_broadcast(&th->cv);
            pthread_mutex_unlock(&th->mu);
            return NULL;
        }
        st->next_probe_ms = now + th->o.every_ms;
        st->inflight_ms[slot] = now;
        char pbuf[PATH_MAX];
        snprintf(pbuf, sizeof pbuf, "%s", st->path);
        int idx = st->index;
        pthread_mutex_unlock(&th->mu);

        int err = store_probe_io(pbuf, ++seq);

        /* Refresh cached store filesystem fill/byte counters while st->inflight_ms[slot] is still armed so an NFS hang
         * in fstatvfs() is caught by the stall detector (R15 D8) instead of silently freezing this probe worker.
         * Acquire a refcounted chunk_store under th->mu after checking !th->closing && st->active so a retired or
         * detached probe waking after replacement or tier_health_quiesce() never touches a replaced slot or freed `t`
         * (chunk_store_unref frees cs safely without owner_be). */
        if (err == 0) {
            chunk_store *cs = NULL;
            pthread_mutex_lock(&th->mu);
            if (!th->closing && st->active && th->t && th->t->be)
                cs = chunk_be_store_ref(th->t->be, idx);
            pthread_mutex_unlock(&th->mu);
            if (cs) {
                chunk_store_fs_refresh(cs);
                chunk_store_unref(cs);
            }
        }
        int64_t done = th_mono_ms();
        int ms = (int)(done - now);

        bool need_down = false;
        bool queue_attach = false;
        pthread_mutex_lock(&th->mu);
        st->inflight_ms[slot] = 0;
        if (th->closing || !st->active) {
            st->running--;
            st->alive[slot] = false;
            pthread_cond_broadcast(&th->cv);
            pthread_mutex_unlock(&th->mu);
            return NULL;
        }
        /* Safe to use `t` below: !th->closing while st->running > 0 keeps tier_health_quiesce() waiting before tier_free. */
        tier *t = th->t;
        if (err != 0) {
            st->ok_streak = 0;
            if (th_is_storage_err(err)) {
                st->wait_other_up = false;
                st->fails++;
                if (st->fails >= th->o.fail_need)
                    need_down = true;
            }
        } else {
            st->fails = 0;
            if (ms <= th->o.fast_ms)
                st->ok_streak++;
            else
                st->ok_streak = 0;
            bool other_stuck = false;
            for (int k = 0; k < TH_MAX_WORKERS; k++)
                if (st->inflight_ms[k] && done - st->inflight_ms[k] >= th->o.fast_ms)
                    other_stuck = true;
            if (!other_stuck && !st->domain_down && !st->attach_queued && !st->attaching && !st->attach_perm_err &&
                !st->wait_other_up && st->ok_streak >= th->o.ok_need)
                queue_attach = true;
        }
        pthread_mutex_unlock(&th->mu);

        if (need_down) {
            int cur = chunk_be_store_state(t->be, idx);
            if (cur == CHUNK_STORE_UP || cur == CHUNK_STORE_REPLACING ||
                (cur == CHUNK_STORE_DOWN_IOERR && chunk_be_open_store(t->be, idx) != NULL))
                tier_store_set_down(t, idx, CHUNK_STORE_DOWN_IOERR);
        } else if (queue_attach) {
            int cur = chunk_be_store_state(t->be, idx);
            if (cur == CHUNK_STORE_DOWN_IOERR || cur == CHUNK_STORE_DOWN_STALLED || cur == CHUNK_STORE_REPLACING) {
                pthread_mutex_lock(&th->mu);
                if (!th->closing && st->active && !st->domain_down && !st->attaching && !st->attach_perm_err &&
                    !(cur == CHUNK_STORE_REPLACING && st->wait_other_up)) {
                    st->attach_queued = true;
                    th_wake_locked(th);
                }
                pthread_mutex_unlock(&th->mu);
            }
        }
    }
}

/* Starts a detached probe worker on st; th->mu held. */
static int start_probe_worker(struct tier_health *th, th_store *st)
{
    int slot = st->alive[0] ? 1 : 0;
    if (st->alive[slot])
        return TSDB_ERR_INVALID;
    probe_arg *pa = malloc(sizeof *pa);
    if (!pa)
        return TSDB_ERR_NOMEM;
    pa->st = st;
    pa->slot = slot;
    pthread_t pt;
    pthread_attr_t at;
    pthread_attr_init(&at);
    pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED);
    pthread_attr_setstacksize(&at, 256 * 1024);
    st->alive[slot] = true;
    st->started_ms[slot] = th_mono_ms();
    st->running++;
    int rc = pthread_create(&pt, &at, probe_main, pa);
    pthread_attr_destroy(&at);
    if (rc != 0) {
        st->alive[slot] = false;
        st->running--;
        free(pa);
        return TSDB_ERR_NOMEM;
    }
    return 0;
}

static void *monitor_main(void *arg)
{
    struct tier_health *th = arg;
    for (;;) {
        struct timespec nap = {0, 50 * 1000000L};
        nanosleep(&nap, NULL);

        int down_idx[TIER_DIRS_MAX];
        int down_reason[TIER_DIRS_MAX];
        int ndown = 0;

        pthread_mutex_lock(&th->mu);
        if (th->closing) {
            pthread_mutex_unlock(&th->mu);
            return NULL;
        }
        int64_t now = th_mono_ms();
        tier *t = th->t;
        for (int i = 0; i < th->nstores; i++) {
            th_store *st = th->stores[i];
            if (!st || !st->active)
                continue;
            int64_t oldest = 0;
            bool any = false;
            for (int k = 0; k < TH_MAX_WORKERS; k++)
                if (st->inflight_ms[k] && (!any || st->inflight_ms[k] < oldest)) {
                    oldest = st->inflight_ms[k];
                    any = true;
                }
            int64_t age = any ? now - oldest : 0;
            if (any && age >= th->o.spawn_ms && st->running < TH_MAX_WORKERS)
                (void)start_probe_worker(th, st);
            if (any && age >= th->o.stall_ms) {
                st->ok_streak = 0;
                if (ndown < TIER_DIRS_MAX) {
                    down_idx[ndown] = i;
                    down_reason[ndown] = CHUNK_STORE_DOWN_STALLED;
                    ndown++;
                }
            }
        }
        pthread_mutex_unlock(&th->mu);

        for (int k = 0; k < ndown; k++) {
            int cur = chunk_be_store_state(t->be, down_idx[k]);
            if (cur == CHUNK_STORE_UP || cur == CHUNK_STORE_REPLACING ||
                (cur == CHUNK_STORE_DOWN_IOERR && chunk_be_open_store(t->be, down_idx[k]) != NULL))
                tier_store_set_down(t, down_idx[k], down_reason[k]);
        }
    }
}

/* ------------------------------------------------------------ live OID snapshot & missed-delete sweep */

typedef struct oid_set {
    uint8_t (*v)[16];
    size_t n, cap;
} oid_set;

static int oid_cmp16(const void *a, const void *b) { return memcmp(a, b, 16); }

static int oid_set_push(oid_set *s, const uint8_t oid[16])
{
    if (s->n == s->cap) {
        size_t nc = s->cap ? s->cap * 2 : 64;
        uint8_t (*nv)[16] = realloc(s->v, nc * 16);
        if (!nv)
            return TSDB_ERR_NOMEM;
        s->v = nv;
        s->cap = nc;
    }
    memcpy(s->v[s->n++], oid, 16);
    return 0;
}

static bool oid_set_has(const oid_set *s, const uint8_t oid[16])
{
    return s->n > 0 && bsearch(oid, s->v, s->n, 16, oid_cmp16) != NULL;
}

/* True if an engine is halted or quiesced (e.g., old engine during tsdb_volume_reopen before eng_close runs).
 * Uses only atomic fields to avoid any data race with plain db->retiring. */
static bool eng_is_closing(const eng *db)
{
    return atomic_load_explicit(&db->stop, memory_order_acquire) ||
           atomic_load_explicit(&db->quiesced, memory_order_acquire);
}

/* Caller holds db->lock: true if any settle or reencode of db is currently in flight (p->settling set before
 * writing any chunk and cleared only after ec_swap_commit lists the .ec part or rolls back). */
static bool has_settling_locked(const eng *db)
{
    for (size_t i = 0; i < db->nparts; i++)
        if (db->parts[i]->settling)
            return true;
    return false;
}

/* Collects every .ec part OID (listed + retired) of a single engine under db->lock (no I/O under lock).
 * When abort_if_settling is true and any part of db has p->settling set, sets *settling_out = true and returns 0
 * without populating `out` so callers never delete chunks of an in-flight settle. */
static int collect_eng_oids(eng *db, bool include_retired, bool abort_if_settling, bool *settling_out, oid_set *out)
{
    if (settling_out)
        *settling_out = false;
    pthread_mutex_lock(&db->lock);
    if (abort_if_settling && has_settling_locked(db)) {
        pthread_mutex_unlock(&db->lock);
        if (settling_out)
            *settling_out = true;
        return 0;
    }
    int rc = 0;
    for (size_t i = 0; i < db->nparts && rc == 0; i++) {
        struct ecpart *ep = atomic_load_explicit(&db->parts[i]->ec, memory_order_acquire);
        if (ep && !ep->test_mark)
            rc = oid_set_push(out, ep->rec.oid);
    }
    uint8_t (*ret)[16] = NULL;
    size_t nret = 0;
    if (rc == 0 && include_retired)
        rc = ecgc_list_retired_locked(db, &ret, &nret);
    pthread_mutex_unlock(&db->lock);
    for (size_t i = 0; i < nret && rc == 0; i++)
        rc = oid_set_push(out, ret[i]);
    free(ret);
    return rc;
}

/* True if any ready non-closing engine of tier t currently has an in-flight settle or reencode.
 * Does NOT block on t->eng_opening > 0 so a hung volume open on another NFS domain never blocks store reattachment
 * (collect_all_live_oids already falls back to live=NULL and sweep_reattached_store skips sweeping while eng_opening > 0). */
static bool any_engine_settling(tier *t)
{
    pthread_mutex_lock(&t->cmu);
    t->eng_active++;
    size_t n = t->nengs;
    eng **copy = n ? malloc(n * sizeof *copy) : NULL;
    if (n && !copy) {
        if (--t->eng_active == 0)
            pthread_cond_broadcast(&t->eng_cv);
        pthread_mutex_unlock(&t->cmu);
        return true;
    }
    if (n)
        memcpy(copy, t->engs, n * sizeof *copy);
    pthread_mutex_unlock(&t->cmu);

    bool busy = false;
    for (size_t i = 0; i < n && !busy; i++) {
        if (eng_is_closing(copy[i]))
            continue;
        pthread_mutex_lock(&copy[i]->lock);
        busy = has_settling_locked(copy[i]);
        pthread_mutex_unlock(&copy[i]->lock);
    }
    free(copy);
    pthread_mutex_lock(&t->cmu);
    if (--t->eng_active == 0)
        pthread_cond_broadcast(&t->eng_cv);
    pthread_mutex_unlock(&t->cmu);
    return busy;
}

/* Collects all live OIDs across every ready single-server engine of tier t (for chunk_heal_opts.live).
 * Sets *complete_out = true ONLY when every attached volume of the tier is open and ready (no opening/closing/
 * quiesced volume and no in-flight settle); otherwise callers pass live = NULL so heal conservatively rebuilds all
 * surviving objects instead of skipping a quiesced, reopening, or settling volume's objects. */
static int collect_all_live_oids(tier *t, oid_set *out, bool *complete_out)
{
    memset(out, 0, sizeof *out);
    *complete_out = false;
    int rc = 0;
    pthread_mutex_lock(&t->cmu);
    if (t->eng_opening > 0 || t->nengs == 0 || t->nengs < t->ncvs) {
        pthread_mutex_unlock(&t->cmu);
        return 0;
    }
    t->eng_active++;
    size_t n = t->nengs;
    eng **copy = n ? malloc(n * sizeof *copy) : NULL;
    if (n && !copy) {
        if (--t->eng_active == 0)
            pthread_cond_broadcast(&t->eng_cv);
        pthread_mutex_unlock(&t->cmu);
        return TSDB_ERR_NOMEM;
    }
    if (n)
        memcpy(copy, t->engs, n * sizeof *copy);
    pthread_mutex_unlock(&t->cmu);

    /* Ensure every volume known to the backend is open and ready in copy[] before claiming a complete live set
     * (prevents skipping a second volume's objects during multi-volume tsdb_open before load_layout finishes). */
    bool incomplete = false;
    if (t->be) {
        uint32_t *bvols = NULL;
        size_t nbvols = 0;
        if (chunk_be_volumes_list(t->be, &bvols, &nbvols) != 0) {
            incomplete = true;
        } else {
            for (size_t vi = 0; vi < nbvols && !incomplete; vi++) {
                bool found = false;
                for (size_t ei = 0; ei < n; ei++)
                    if (copy[ei]->tier_vol == bvols[vi] && !eng_is_closing(copy[ei])) {
                        found = true;
                        break;
                    }
                if (!found)
                    incomplete = true;
            }
            free(bvols);
        }
    }

    for (size_t i = 0; i < n && rc == 0 && !incomplete; i++) {
        if (eng_is_closing(copy[i])) {
            incomplete = true;
            break;
        }
        bool settling = false;
        rc = collect_eng_oids(copy[i], false, true, &settling, out);
        if (settling)
            incomplete = true;
    }

    free(copy);
    pthread_mutex_lock(&t->cmu);
    if (--t->eng_active == 0)
        pthread_cond_broadcast(&t->eng_cv);
    pthread_mutex_unlock(&t->cmu);
    if (rc == 0 && !incomplete) {
        if (out->n > 1)
            qsort(out->v, out->n, 16, oid_cmp16);
        *complete_out = true;
    } else {
        free(out->v);
        memset(out, 0, sizeof *out);
    }
    return rc;
}

static int heal_live_cb(void *ctx, const uint8_t oid[16])
{
    const oid_set *s = ctx;
    return oid_set_has(s, oid) ? 1 : 0;
}

typedef struct sweep_chunk {
    uint8_t oid[16];
    uint16_t pos;
} sweep_chunk;

typedef struct sweep_list_ctx {
    sweep_chunk *v;
    size_t n, cap;
    bool nomem;
} sweep_list_ctx;

static int sweep_add_cb(void *ctx, const chunk_listed *c)
{
    sweep_list_ctx *s = ctx;
    if (s->n == s->cap) {
        size_t nc = s->cap ? s->cap * 2 : 64;
        sweep_chunk *nv = realloc(s->v, nc * sizeof *nv);
        if (!nv) {
            s->nomem = true;
            return 1;
        }
        s->v = nv;
        s->cap = nc;
    }
    memcpy(s->v[s->n].oid, c->oid, 16);
    s->v[s->n].pos = c->pos;
    s->n++;
    return 0;
}

typedef struct sweep_vol {
    uint32_t vol;
    uint8_t vuuid[16];
} sweep_vol;

/* Sweeps chunks on a just-reattached store whose deletes it missed while DOWN. Runs while t->halted is still true
 * (no new settles starting) and only deletes chunks belonging to an active, non-closing volume in t->engs[] that are
 * neither named by any listed/retired .ec part nor held by an open reader.
 *
 * Locking discipline: t->eng_active is NEVER held across store I/O (chunk_store_list_names / peek / del), so a store
 * that hangs again during the sweep cannot block eng_close or recovery of unrelated volumes.
 *
 * Ordering proof against in-flight settles (eng_settle / eng_reencode):
 *   settle sets p->settling = true under db->lock (T1), puts chunks (T2), lists .ec under db->lock (T3), and clears
 *   p->settling = false under db->lock (T4): T1 < T2 < T3 < T4.
 *   We list the store's chunks FIRST (S1) with no engine pin, and ONLY AFTER S1 completes do we briefly pin the
 *   non-closing engine matching (vol, vuuid) to check has_settling_locked(db) and collect `named` under db->lock (S2),
 *   then release the pin before peek/del (S3). Any chunk visible at S1 from an in-flight settle has T1 < T2 <= S1 <
 *   S2, so at S2 either p->settling is still true (sweep aborts for this volume) or T4 <= S2 so the .ec part is
 *   already listed in `named` (chunk is spared). */
static void sweep_reattached_store(tier *t, int store_idx)
{
    if (!t || !t->be || atomic_load(&ecgc_test_nosweep))
        return;
    chunk_store *st = chunk_be_store_ref(t->be, store_idx);
    if (!st)
        return;

    /* Snapshot candidate (vol, vuuid) pairs of active non-closing engines without holding eng_active across I/O. */
    pthread_mutex_lock(&t->cmu);
    size_t n = t->nengs;
    sweep_vol *vols = n ? malloc(n * sizeof *vols) : NULL;
    size_t nv = 0;
    if (vols) {
        for (size_t i = 0; i < n; i++) {
            eng *db = t->engs[i];
            if (eng_is_closing(db))
                continue;
            const uint8_t *vu = tier_vuuid(db);
            if (!vu)
                continue;
            bool dup = false;
            for (size_t j = 0; j < nv; j++)
                if (vols[j].vol == db->tier_vol) {
                    dup = true;
                    break;
                }
            if (!dup) {
                vols[nv].vol = db->tier_vol;
                memcpy(vols[nv].vuuid, vu, 16);
                nv++;
            }
        }
    }
    pthread_mutex_unlock(&t->cmu);

    for (size_t i = 0; i < nv; i++) {
        uint32_t vol = vols[i].vol;
        const uint8_t *vuuid = vols[i].vuuid;
        /* S1: list chunks on the reattached store BEFORE reading db->parts / p->settling (zero eng_active held). */
        sweep_list_ctx lc = {0};
        int lrc = chunk_store_list_names(st, vol, sweep_add_cb, &lc);
        if (lrc != 0 || lc.nomem || lc.n == 0) {
            free(lc.v);
            continue;
        }
        /* S2: briefly pin the matching active non-closing engine to snapshot listed + retired OIDs under db->lock.
         * If the volume is reopening (2 engines or opening > 0), closing, or settling, skip sweeping it safely. */
        oid_set named = {0};
        bool ok_to_sweep = false;
        pthread_mutex_lock(&t->cmu);
        eng *target = NULL;
        bool ambiguous = t->eng_opening > 0;
        for (size_t k = 0; k < t->nengs && !ambiguous; k++) {
            eng *cand = t->engs[k];
            if (cand->tier_vol != vol)
                continue;
            if (eng_is_closing(cand)) {
                ambiguous = true; /* volume reopen in flight: do not sweep against an old/transitioning engine */
                break;
            }
            const uint8_t *cvu = tier_vuuid(cand);
            if (!cvu || memcmp(cvu, vuuid, 16) != 0)
                continue;
            if (target)
                ambiguous = true;
            else
                target = cand;
        }
        if (target && !ambiguous) {
            t->eng_active++;
            pthread_mutex_unlock(&t->cmu);
            bool settling = false;
            if (collect_eng_oids(target, true, true, &settling, &named) == 0 && !settling)
                ok_to_sweep = true;
            pthread_mutex_lock(&t->cmu);
            if (--t->eng_active == 0)
                pthread_cond_broadcast(&t->eng_cv);
        }
        pthread_mutex_unlock(&t->cmu);

        if (!ok_to_sweep) {
            free(named.v);
            free(lc.v);
            continue;
        }
        if (named.n > 1)
            qsort(named.v, named.n, 16, oid_cmp16);

        /* S3: delete orphan chunks on the store with zero eng_active held. */
        uint64_t epoch = tier_epoch(t, vol);
        size_t deleted = 0;
        if (epoch > 0) {
            for (size_t j = 0; j < lc.n; j++) {
                if (oid_set_has(&named, lc.v[j].oid) || ecgc_held(t, vol, lc.v[j].oid))
                    continue;
                chunk_hdr h;
                if (chunk_store_peek(st, vol, lc.v[j].oid, lc.v[j].pos, &h) != 0)
                    continue;
                if (memcmp(h.vuuid, vuuid, 16) != 0)
                    continue; /* another volume's chunk: leave untouched */
                chunk_ident id = {.volume = vol, .pos = lc.v[j].pos};
                memcpy(id.vuuid, vuuid, 16);
                memcpy(id.oid, lc.v[j].oid, 16);
                if (chunk_store_del(st, &id, epoch) == 0)
                    deleted++;
            }
        }
        if (deleted > 0)
            fprintf(stderr, "tsdb: tier store %d: swept %zu orphan chunk(s) of volume %u whose deletes were missed\n",
                    store_idx, deleted, vol);
        free(lc.v);
        free(named.v);
    }

    free(vols);
    chunk_store_unref(st);
}

/* ------------------------------------------------------------ attach/heal execution (driven by tierctl's worker) */

typedef struct th_cancel_pair {
    const struct tier_health *th;
    const chunk_heal_opts *tpl;
} th_cancel_pair;

static int th_cancel_cb(void *ctx)
{
    const th_cancel_pair *cp = ctx;
    if (atomic_load_explicit(&cp->th->closing_atomic, memory_order_acquire))
        return 1;
    if (cp->tpl && cp->tpl->cancel && cp->tpl->cancel(cp->tpl->cancel_ctx))
        return 1;
    return 0;
}

bool tier_health_any_settling(tier *t)
{
    return t ? any_engine_settling(t) : false;
}

int tier_health_live_oids(tier *t, uint8_t (**oids_out)[16], size_t *n_out, bool *complete_out)
{
    *oids_out = NULL;
    *n_out = 0;
    *complete_out = false;
    if (!t)
        return 0;
    oid_set s = {0};
    int rc = collect_all_live_oids(t, &s, complete_out);
    if (rc == 0 && *complete_out) {
        *oids_out = s.v;
        *n_out = s.n;
    } else {
        free(s.v);
    }
    return rc;
}

void tier_health_set_wake(tier *t, void (*fn)(void *), void *ctx)
{
    if (!t)
        return;
    struct tier_health *th = atomic_load_explicit(&t->health, memory_order_acquire);
    if (!th)
        return;
    pthread_mutex_lock(&th->mu);
    th->wake_fn = fn;
    th->wake_ctx = ctx;
    bool pending = th->need_reopen;
    for (int i = 0; !pending && i < th->nstores; i++)
        if (th->stores[i] && th->stores[i]->active && th->stores[i]->attach_queued)
            pending = true;
    if (pending)
        th_wake_locked(th);
    pthread_mutex_unlock(&th->mu);
}

bool tier_health_take_reopen(tier *t)
{
    if (!t)
        return false;
    struct tier_health *th = atomic_load_explicit(&t->health, memory_order_acquire);
    if (!th)
        return false;
    pthread_mutex_lock(&th->mu);
    bool r = !th->closing && th->need_reopen;
    if (r)
        th->need_reopen = false;
    pthread_mutex_unlock(&th->mu);
    return r;
}

bool tier_health_has_pending(tier *t)
{
    if (!t)
        return false;
    struct tier_health *th = atomic_load_explicit(&t->health, memory_order_acquire);
    if (!th)
        return false;
    bool pending = false;
    int64_t now = th_mono_ms();
    pthread_mutex_lock(&th->mu);
    if (!th->closing) {
        if (th->need_reopen) {
            pending = true;
        } else {
            for (int i = 0; i < th->nstores; i++) {
                th_store *st = th->stores[i];
                if (st && st->active && st->attach_queued && !st->domain_down && !st->attach_perm_err &&
                    now >= st->retry_after_ms) {
                    pending = true;
                    break;
                }
            }
        }
    }
    pthread_mutex_unlock(&th->mu);
    return pending;
}

bool tier_health_busy(tier *t)
{
    if (!t)
        return false;
    struct tier_health *th = atomic_load_explicit(&t->health, memory_order_acquire);
    if (!th)
        return false;
    bool busy = false;
    pthread_mutex_lock(&th->mu);
    if (!th->closing) {
        if (th->need_reopen) {
            busy = true;
        } else {
            for (int i = 0; i < th->nstores; i++) {
                th_store *st = th->stores[i];
                if (st && st->active && (st->attaching || st->attach_queued)) {
                    busy = true;
                    break;
                }
            }
        }
    }
    pthread_mutex_unlock(&th->mu);
    return busy;
}

bool tier_health_run_one(tier *t, const chunk_heal_opts *ho_tpl, int64_t *wait_ms_out)
{
    if (wait_ms_out)
        *wait_ms_out = 500;
    if (!t || !t->be)
        return false;
    struct tier_health *th = atomic_load_explicit(&t->health, memory_order_acquire);
    if (!th)
        return false;

    pthread_mutex_lock(&th->mu);
    if (th->closing) {
        pthread_mutex_unlock(&th->mu);
        return false;
    }
    int pick = -1;
    int64_t now = th_mono_ms();
    int64_t wait_ms = 500;
    for (int i = 0; i < th->nstores; i++) {
        th_store *st = th->stores[i];
        if (!st || !st->active || !st->attach_queued || st->domain_down || st->attach_perm_err)
            continue;
        if (now >= st->retry_after_ms) {
            pick = i;
            break;
        }
        int64_t rem = st->retry_after_ms - now;
        if (rem < wait_ms)
            wait_ms = rem;
    }
    if (pick < 0) {
        pthread_mutex_unlock(&th->mu);
        if (wait_ms_out)
            *wait_ms_out = wait_ms;
        return false;
    }
    th_store *st = th->stores[pick];
    st->attach_queued = false;
    st->attaching = true;
    char path[PATH_MAX];
    snprintf(path, sizeof path, "%s", st->path ? st->path : "");
    uint8_t expect_id[16];
    memcpy(expect_id, st->id, 16);
    pthread_mutex_unlock(&th->mu);

    int cur = chunk_be_store_state(t->be, pick);
    int rc = 0;
    if (cur == CHUNK_STORE_UP || chunk_be_store_is_lost(t->be, pick)) {
        pthread_mutex_lock(&th->mu);
        st->attaching = false;
        st->wait_other_up = false;
        pthread_mutex_unlock(&th->mu);
        return true;
    }
    /* Guarantee t->halted is set across the entire attach + sweep so no new settle starts until sweep finishes. */
    tier_ensure_paused(t, pick, "is recovering");

    /* Wait until any in-flight settle/reencode completes before healing and sweeping, so heal's live() set and
     * the missed-delete sweep see a complete part list. (Does not block on hung unrelated volume opens.) */
    if (any_engine_settling(t)) {
        pthread_mutex_lock(&th->mu);
        st->attaching = false;
        st->retry_after_ms = th_mono_ms() + 200;
        if (!th->closing && st->active && !st->domain_down && !st->attach_perm_err)
            st->attach_queued = true;
        pthread_mutex_unlock(&th->mu);
        if (wait_ms_out)
            *wait_ms_out = 200;
        return false;
    }

    /* Verify returning store identity and (when opened from TIERS) database binding before attaching. */
    atomic_fetch_add_explicit(&tier_health_attach_attempts, 1, memory_order_relaxed);
    if (cur == CHUNK_STORE_DOWN_IOERR || cur == CHUNK_STORE_DOWN_STALLED) {
        uint8_t got_id[16];
        chunk_store_owner ow;
        rc = chunk_store_meta_read(path, got_id, &ow);
        if (rc == 0 && memcmp(got_id, expect_id, 16) != 0)
            rc = CHUNK_ERR_STORE_ID;
        if (rc == 0 && t->from_tiers && (!ow.db_set || memcmp(ow.db_id, t->db_id, 16) != 0)) {
            fprintf(stderr, "tsdb: tier store %d (%s): returning store META db= does not match database: refused\n",
                    pick, path);
            rc = CHUNK_ERR_STORE_ID;
        }
    }

    th_cancel_pair cpair = {.th = th, .tpl = ho_tpl};
    if (th_cancel_cb(&cpair)) {
        pthread_mutex_lock(&th->mu);
        st->attaching = false;
        pthread_mutex_unlock(&th->mu);
        return false;
    }

    if (rc == 0) {
        oid_set live = {0};
        bool use_live = !atomic_load(&ecgc_test_nosweep);
        bool complete = false;
        if (use_live && (collect_all_live_oids(t, &live, &complete) != 0 || !complete))
            use_live = false;
        /* Scrub on DOWN_IOERR attach so a chunk with a bad sector inside its body is rebuilt instead of causing
         * repeated read-EIO -> DOWN -> attach flaps. Keep DOWN_STALLED fast with scrub = false. */
        chunk_heal_opts ho = {
            .epoch = 0,
            .scrub = (cur == CHUNK_STORE_DOWN_IOERR),
            .cancel = th_cancel_cb,
            .cancel_ctx = &cpair,
            .pace = ho_tpl ? ho_tpl->pace : NULL,
            .pace_ctx = ho_tpl ? ho_tpl->pace_ctx : NULL,
        };
        if (use_live) {
            ho.live = heal_live_cb;
            ho.live_ctx = &live;
        }
        chunk_heal_report rep;
        memset(&rep, 0, sizeof rep);
        if (th_cancel_cb(&cpair)) {
            rc = TSDB_ERR_STOPPED;
        } else if (cur == CHUNK_STORE_REPLACING) {
            rc = chunk_be_settle(t->be, pick, &ho, &rep);
        } else if (cur == CHUNK_STORE_DOWN_IOERR || cur == CHUNK_STORE_DOWN_STALLED) {
            /* If the store previously failed in service without detaching st[], detach it now. */
            if (chunk_be_open_store(t->be, pick) != NULL)
                (void)chunk_be_set_down(t->be, pick, CHUNK_STORE_DOWN_IOERR);
            rc = chunk_be_attach(t->be, pick, path, expect_id, &ho, &rep);
        } else {
            rc = TSDB_ERR_INVALID;
        }
        free(live.v);
    }

    if (th_cancel_cb(&cpair)) {
        pthread_mutex_lock(&th->mu);
        st->attaching = false;
        pthread_mutex_unlock(&th->mu);
        return false;
    }

    if (rc == 0) {
        sweep_reattached_store(t, pick);
        eccache_reopen_all(t);
        tier_resume_if_all_up(t);
        int64_t done_ms = th_mono_ms();
        pthread_mutex_lock(&th->mu);
        st->attaching = false;
        st->wait_other_up = false;
        st->ok_streak = 0;
        st->fails = 0;
        st->attach_fails = 0;
        st->retry_after_ms = 0;
        if (cur == CHUNK_STORE_DOWN_IOERR) {
            if (st->last_ioerr_attach_ms > 0 && done_ms - st->last_ioerr_attach_ms < 60000) {
                if (st->ioerr_flaps < 6)
                    st->ioerr_flaps++;
            } else {
                st->ioerr_flaps = 0;
            }
            st->last_ioerr_attach_ms = done_ms;
        } else {
            st->ioerr_flaps = 0;
        }
        pthread_mutex_unlock(&th->mu);
        /* If another store is still REPLACING waiting for this store to come UP, wake it to settle now. */
        int n = th->nstores;
        for (int j = 0; j < n; j++) {
            if (j != pick && chunk_be_store_state(t->be, j) == CHUNK_STORE_REPLACING) {
                pthread_mutex_lock(&th->mu);
                th_store *sj = th->stores[j];
                if (!th->closing && sj && sj->active && !sj->domain_down && !sj->attaching) {
                    sj->wait_other_up = false;
                    sj->attach_queued = true;
                    sj->retry_after_ms = 0;
                    th_wake_locked(th);
                }
                pthread_mutex_unlock(&th->mu);
            }
        }
    } else if (rc == CHUNK_ERR_STORE_ID || rc == TSDB_ERR_CORRUPT) {
        fprintf(stderr, "tsdb: tier store %d (%s): attach refused (%d): not retrying automatically\n", pick, path, rc);
        pthread_mutex_lock(&th->mu);
        st->attaching = false;
        st->attach_perm_err = true;
        pthread_mutex_unlock(&th->mu);
    } else if (rc == CHUNK_ERR_BUSY) {
        pthread_mutex_lock(&th->mu);
        st->attaching = false;
        if (st->attach_fails < 5)
            st->attach_fails++;
        int64_t backoff = (int64_t)200 << (st->attach_fails - 1);
        if (backoff > 5000)
            backoff = 5000;
        st->retry_after_ms = th_mono_ms() + backoff;
        if (!th->closing && st->active && !st->domain_down && !st->attach_perm_err)
            st->attach_queued = true;
        pthread_mutex_unlock(&th->mu);
    } else if (rc == TSDB_ERR_UNAVAILABLE) {
        bool other_down = false;
        int bn = chunk_be_nstores(t->be);
        for (int j = 0; j < bn; j++) {
            if (j == pick || chunk_be_store_is_lost(t->be, j))
                continue;
            int sj = chunk_be_store_state(t->be, j);
            if (sj != CHUNK_STORE_UP && sj != CHUNK_STORE_REPLACING) {
                other_down = true;
                break;
            }
        }
        pthread_mutex_lock(&th->mu);
        st->attaching = false;
        if (other_down) {
            st->wait_other_up = true;
        } else {
            st->wait_other_up = false;
            if (st->attach_fails < 4)
                st->attach_fails++;
            int64_t backoff = (int64_t)5000 << (st->attach_fails - 1);
            if (backoff > 60000)
                backoff = 60000;
            st->retry_after_ms = th_mono_ms() + backoff;
            if (!th->closing && st->active && !st->domain_down && !st->attach_perm_err)
                st->attach_queued = true;
        }
        pthread_mutex_unlock(&th->mu);
    } else {
        if (chunk_be_store_state(t->be, pick) == CHUNK_STORE_REPLACING)
            tier_store_set_down(t, pick, CHUNK_STORE_DOWN_IOERR);
        pthread_mutex_lock(&th->mu);
        st->attaching = false;
        st->wait_other_up = false;
        st->ok_streak = 0;
        if (st->attach_fails < 6)
            st->attach_fails++;
        int64_t backoff = (int64_t)1000 << st->attach_fails;
        if (backoff > 60000)
            backoff = 60000;
        int64_t until = th_mono_ms() + backoff;
        if (until > st->retry_after_ms)
            st->retry_after_ms = until;
        pthread_mutex_unlock(&th->mu);
    }
    return true;
}

void tier_health_store_lost(tier *t, int store)
{
    if (!t)
        return;
    struct tier_health *th = atomic_load_explicit(&t->health, memory_order_acquire);
    if (!th || store < 0 || store >= TIERS_STORES_MAX)
        return;
    pthread_mutex_lock(&th->mu);
    if (store < th->nstores && th->stores[store]) {
        th_store *st = th->stores[store];
        st->active = false;
        st->attach_queued = false;
        st->wait_other_up = false;
        st->ok_streak = 0;
        st->fails = 0;
        memset(st->inflight_ms, 0, sizeof st->inflight_ms);
        pthread_cond_broadcast(&th->cv);
    }
    pthread_mutex_unlock(&th->mu);
}

void tier_health_store_replaced(tier *t, int store, const char *new_path, const uint8_t new_id[16])
{
    if (!t || !new_path || !new_id)
        return;
    struct tier_health *th = atomic_load_explicit(&t->health, memory_order_acquire);
    if (!th || store < 0 || store >= TIERS_STORES_MAX)
        return;
    th_store *ns = calloc(1, sizeof *ns);
    char *pcopy = strdup(new_path);
    if (!ns || !pcopy) {
        free(ns);
        free(pcopy);
        return;
    }
    ns->index = store;
    ns->th = th;
    ns->path = pcopy;
    memcpy(ns->id, new_id, 16);
    ns->active = true;
    ns->next_probe_ms = th_mono_ms() + th->o.every_ms;

    pthread_mutex_lock(&th->mu);
    if (!th->closing && store < th->nstores) {
        /* Retire the previous th_store rather than mutating it in place: a probe thread still hung in kernel
         * D-state on the dead store's path will see old->active == false when/if it ever wakes and can never
         * trip monitor_main's stall check on the replacement store. */
        th_store *old = th->stores[store];
        if (old) {
            old->active = false;
            old->attach_queued = false;
            old->wait_other_up = false;
            memset(old->inflight_ms, 0, sizeof old->inflight_ms);
            old->next_retired = th->retired;
            th->retired = old;
        }
        th->stores[store] = ns;
        ns = NULL;
        if (!th->o.disabled)
            (void)start_probe_worker(th, th->stores[store]);
        pthread_cond_broadcast(&th->cv);
    }
    pthread_mutex_unlock(&th->mu);
    if (ns) {
        free(ns->path);
        free(ns);
    }
}

int tier_health_store_added(tier *t, int store, const char *path, const uint8_t id[16], bool active)
{
    if (!t || store < 0 || store >= TIERS_STORES_MAX)
        return TSDB_ERR_INVALID;
    struct tier_health *th = atomic_load_explicit(&t->health, memory_order_acquire);
    if (!th)
        return 0;
    th_store *ns = calloc(1, sizeof *ns);
    char *pcopy = path ? strdup(path) : NULL;
    if (!ns || (path && !pcopy)) {
        free(ns);
        free(pcopy);
        return TSDB_ERR_NOMEM;
    }
    ns->index = store;
    ns->th = th;
    ns->path = pcopy;
    if (id)
        memcpy(ns->id, id, 16);
    ns->active = active;
    ns->next_probe_ms = th_mono_ms() + th->o.every_ms;

    pthread_mutex_lock(&th->mu);
    if (th->closing || th->stores[store] != NULL) {
        pthread_mutex_unlock(&th->mu);
        free(ns->path);
        free(ns);
        return 0;
    }
    th->stores[store] = ns;
    if (store + 1 > th->nstores)
        th->nstores = store + 1;
    if (active && !th->o.disabled)
        (void)start_probe_worker(th, ns);
    pthread_mutex_unlock(&th->mu);
    return 0;
}

/* ------------------------------------------------------------ I/O error callback */

static void th_on_ioerr(void *ctx, int store)
{
    struct tier_health *th = ctx;
    if (!th || !th->t || store < 0 || store >= TIERS_STORES_MAX)
        return;
    tier_store_set_down(th->t, store, CHUNK_STORE_DOWN_IOERR);
    pthread_mutex_lock(&th->mu);
    if (!th->closing && store < th->nstores && th->stores[store] && th->stores[store]->active) {
        th_store *st = th->stores[store];
        st->ok_streak = 0;
        st->wait_other_up = false;
        if (st->ioerr_flaps > 0) {
            int64_t backoff = (int64_t)1000 << (st->ioerr_flaps > 6 ? 6 : st->ioerr_flaps);
            if (backoff > 60000)
                backoff = 60000;
            int64_t until = th_mono_ms() + backoff;
            if (until > st->retry_after_ms)
                st->retry_after_ms = until;
        }
        st->probe_now = true;
        pthread_cond_broadcast(&th->cv);
    }
    pthread_mutex_unlock(&th->mu);
}

/* ------------------------------------------------------------ lifecycle & API */

int tier_health_open(tier *t)
{
    if (!t || !t->be)
        return 0;
    int n = chunk_be_nstores(t->be);
    if (n <= 0 || n > TIERS_STORES_MAX)
        return 0;
    struct tier_health *th = calloc(1, sizeof *th);
    if (!th)
        return TSDB_ERR_NOMEM;
    th->t = t;
    th_load_opts(&th->o);
    pthread_mutex_init(&th->mu, NULL);
    tsdb_cond_init_mono(&th->cv);
    th->nstores = n;
    int64_t now = th_mono_ms();
    for (int i = 0; i < n; i++) {
        th_store *st = calloc(1, sizeof *st);
        if (!st) {
            atomic_store_explicit(&t->health, th, memory_order_release);
            tier_health_close(t);
            return TSDB_ERR_NOMEM;
        }
        th->stores[i] = st;
        st->index = i;
        st->th = th;
        st->path = t->cfg.dirs[i] ? strdup(t->cfg.dirs[i]) : NULL;
        if (t->cfg.dirs[i] && !st->path) {
            atomic_store_explicit(&t->health, th, memory_order_release);
            tier_health_close(t);
            return TSDB_ERR_NOMEM;
        }
        chunk_store *cs = chunk_be_store_ref(t->be, i);
        if (cs && chunk_be_store_state(t->be, i) == CHUNK_STORE_UP) {
            st->active = true;
            memcpy(st->id, chunk_store_id(cs), 16);
            st->next_probe_ms = now + th->o.every_ms;
        }
        chunk_store_unref(cs);
    }
    atomic_store_explicit(&t->health, th, memory_order_release);
    if (th->o.disabled)
        return 0;
    chunk_be_set_ioerr_cb(t->be, th_on_ioerr, th);
    if (tsdb_thread_create(&th->monitor_th, monitor_main, th) != 0) {
        tier_health_close(t);
        return TSDB_ERR_NOMEM;
    }
    th->monitor_started = true;
    pthread_mutex_lock(&th->mu);
    for (int i = 0; i < n; i++) {
        if (th->stores[i] && th->stores[i]->active) {
            int rc = start_probe_worker(th, th->stores[i]);
            if (rc != 0) {
                pthread_mutex_unlock(&th->mu);
                tier_health_close(t);
                return rc;
            }
        }
    }
    pthread_mutex_unlock(&th->mu);
    return 0;
}

/* Stops all health/probe threads and marks th->closing = true, while keeping th allocated until tier_put()
 * calls tier_health_close() so any concurrent caller holding a tier ref sees th->closing == true without UAF. */
void tier_health_quiesce(tier *t)
{
    if (!t)
        return;
    struct tier_health *th = atomic_load_explicit(&t->health, memory_order_acquire);
    if (!th)
        return;
    if (t->be)
        chunk_be_set_ioerr_cb(t->be, NULL, NULL);
    pthread_mutex_lock(&th->mu);
    if (th->quiesced) {
        pthread_mutex_unlock(&th->mu);
        return;
    }
    th->closing = true;
    th->wake_fn = NULL;
    th->wake_ctx = NULL;
    atomic_store_explicit(&th->closing_atomic, true, memory_order_release);
    th->quiesced = true;
    pthread_cond_broadcast(&th->cv);
    pthread_mutex_unlock(&th->mu);

    if (th->monitor_started) {
        pthread_join(th->monitor_th, NULL);
        th->monitor_started = false;
    }

    struct timespec dl_prb;
    tsdb_cond_deadline_ms(&dl_prb, 2000);
    bool probe_stuck = false;
    pthread_mutex_lock(&th->mu);
    for (int i = 0; !probe_stuck && i < th->nstores; i++) {
        if (!th->stores[i])
            continue;
        while (th->stores[i]->running > 0) {
            if (pthread_cond_timedwait(&th->cv, &th->mu, &dl_prb) != 0) {
                probe_stuck = true;
                break;
            }
        }
    }
    for (th_store *r = th->retired; !probe_stuck && r; r = r->next_retired) {
        while (r->running > 0) {
            if (pthread_cond_timedwait(&th->cv, &th->mu, &dl_prb) != 0) {
                probe_stuck = true;
                break;
            }
        }
    }
    th->stuck = probe_stuck;
    pthread_mutex_unlock(&th->mu);
}

/* Called only from tier_put() when the last reference to t is dropped (or on tier_health_open() error). */
bool tier_health_close(tier *t)
{
    if (!t)
        return false;
    tier_health_quiesce(t);
    struct tier_health *th = atomic_load_explicit(&t->health, memory_order_acquire);
    if (!th)
        return false;
    atomic_store_explicit(&t->health, NULL, memory_order_release);
    if (th->stuck)
        return false; /* only a probe thread is still stuck in the kernel: leave th allocated, free t */
    for (int i = 0; i < TIERS_STORES_MAX; i++) {
        if (th->stores[i]) {
            free(th->stores[i]->path);
            free(th->stores[i]);
        }
    }
    for (th_store *r = th->retired, *nr; r; r = nr) {
        nr = r->next_retired;
        free(r->path);
        free(r);
    }
    pthread_cond_destroy(&th->cv);
    pthread_mutex_destroy(&th->mu);
    free(th);
    return false;
}

void tier_health_kick_reopen(tier *t)
{
    if (!t)
        return;
    struct tier_health *th = atomic_load_explicit(&t->health, memory_order_acquire);
    if (!th)
        return;
    pthread_mutex_lock(&th->mu);
    if (!th->closing) {
        th->need_reopen = true;
        th_wake_locked(th);
    }
    pthread_mutex_unlock(&th->mu);
}

void tier_health_domain_state(tier *t, int store, bool down)
{
    if (!t || !t->be)
        return;
    struct tier_health *th = atomic_load_explicit(&t->health, memory_order_acquire);
    if (!th || store < 0 || store >= TIERS_STORES_MAX)
        return;
    pthread_mutex_lock(&th->mu);
    if (th->closing || store >= th->nstores || !th->stores[store] || !th->stores[store]->active) {
        pthread_mutex_unlock(&th->mu);
        return;
    }
    th_store *st = th->stores[store];
    if (down) {
        st->domain_down = true;
        st->ok_streak = 0;
        st->attach_queued = false;
        st->wait_other_up = false;
        pthread_mutex_unlock(&th->mu);
        tier_store_set_down(t, store, CHUNK_STORE_DOWN_STALLED);
        return;
    }
    st->domain_down = false;
    st->wait_other_up = false;
    bool need_queue = !st->attaching && !st->attach_perm_err;
    if (need_queue) {
        int cur = chunk_be_store_state(t->be, store);
        if (cur == CHUNK_STORE_DOWN_STALLED || cur == CHUNK_STORE_DOWN_IOERR || cur == CHUNK_STORE_REPLACING) {
            st->attach_queued = true;
            st->retry_after_ms = 0;
            th_wake_locked(th);
        }
    }
    pthread_mutex_unlock(&th->mu);
}

void tier_health_test_opts(int stall_ms, int every_ms, int ok_need, int fast_ms)
{
    atomic_store_explicit(&g_test_stall_ms, stall_ms > 0 ? stall_ms : 0, memory_order_relaxed);
    atomic_store_explicit(&g_test_every_ms, every_ms != 0 ? every_ms : 0, memory_order_relaxed);
    atomic_store_explicit(&g_test_ok_need, ok_need > 0 ? ok_need : 0, memory_order_relaxed);
    atomic_store_explicit(&g_test_fast_ms, fast_ms > 0 ? fast_ms : 0, memory_order_relaxed);
}

#else /* !TSDB_EC */

atomic_uint_fast64_t tier_health_attach_attempts;

int tier_health_open(tier *t)
{
    (void)t;
    return 0;
}
void tier_health_quiesce(tier *t) { (void)t; }
bool tier_health_close(tier *t)
{
    (void)t;
    return false;
}
void tier_health_kick_reopen(tier *t) { (void)t; }
void tier_health_domain_state(tier *t, int store, bool down)
{
    (void)t;
    (void)store;
    (void)down;
}
void tier_health_test_opts(int stall_ms, int every_ms, int ok_need, int fast_ms)
{
    (void)stall_ms;
    (void)every_ms;
    (void)ok_need;
    (void)fast_ms;
}
void tier_health_set_wake(tier *t, void (*fn)(void *), void *ctx)
{
    (void)t, (void)fn, (void)ctx;
}
bool tier_health_take_reopen(tier *t)
{
    (void)t;
    return false;
}
bool tier_health_has_pending(tier *t)
{
    (void)t;
    return false;
}
bool tier_health_busy(tier *t)
{
    (void)t;
    return false;
}
bool tier_health_run_one(tier *t, const struct chunk_heal_opts *ho_tpl, int64_t *wait_ms_out)
{
    (void)t, (void)ho_tpl;
    if (wait_ms_out)
        *wait_ms_out = 500;
    return false;
}
bool tier_health_any_settling(tier *t)
{
    (void)t;
    return false;
}
int tier_health_live_oids(tier *t, uint8_t (**oids_out)[16], size_t *n_out, bool *complete_out)
{
    (void)t;
    *oids_out = NULL;
    *n_out = 0;
    *complete_out = false;
    return 0;
}
void tier_health_store_lost(tier *t, int store)
{
    (void)t, (void)store;
}
void tier_health_store_replaced(tier *t, int store, const char *new_path, const uint8_t new_id[16])
{
    (void)t, (void)store, (void)new_path, (void)new_id;
}
int tier_health_store_added(tier *t, int store, const char *path, const uint8_t id[16], bool active)
{
    (void)t, (void)store, (void)path, (void)id, (void)active;
    return 0;
}

#endif
