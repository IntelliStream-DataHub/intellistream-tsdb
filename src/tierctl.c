/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Local tier controller: one tier worker thread per handle (nice 19, zero engine locks across chunk I/O),
 * periodic TIERS stat/re-read, paced replacement rebuilds (chunk_be_replace_open / chunk_be_settle),
 * crash/power-loss resume via durable REBUILDING marker, and execution of r15-health's queued attaches
 * (docs/design-cluster-data.md §15.7, E15, E55; R15 D9, D10, D20).
 */
#define _GNU_SOURCE
#include "tierctl.h"
#include "chunk.h"
#include "chunk_priv.h"
#include "eccache.h"
#include "ecgc.h"
#include "ecpart.h"
#include "internal.h"
#include "settle.h"
#include "tier.h"
#include "tiers.h"
#include "tierscrub.h"

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/resource.h>
#include <sys/syscall.h>
#endif

#ifdef TSDB_TEST_HOOKS
int tierctl_mutant;
static atomic_int g_poll_ms = 1000;
static pthread_mutex_t g_hook_mu = PTHREAD_MUTEX_INITIALIZER;
static void (*g_rebuild_hook)(void *ctx, int store, uint64_t bytes_done);
static void *g_rebuild_hook_ctx;

static inline int tierctl_mut(void) { return tierctl_mutant; }
#else
static inline int tierctl_mut(void) { return 0; }
#endif

void tierctl_set_poll_ms(int ms)
{
#ifdef TSDB_TEST_HOOKS
    atomic_store_explicit(&g_poll_ms, ms > 0 ? ms : 1000, memory_order_relaxed);
#else
    (void)ms;
#endif
}

void tierctl_set_rebuild_hook(void (*fn)(void *ctx, int store, uint64_t bytes_done), void *ctx)
{
#ifdef TSDB_TEST_HOOKS
    pthread_mutex_lock(&g_hook_mu);
    g_rebuild_hook = fn;
    g_rebuild_hook_ctx = ctx;
    pthread_mutex_unlock(&g_hook_mu);
#else
    (void)fn;
    (void)ctx;
#endif
}

bool tierctl_store_rebuilding(const char *store_path)
{
    if (!store_path || !*store_path)
        return false;
    char p[PATH_MAX + 32];
    int n = snprintf(p, sizeof p, "%s/%s", store_path, TIERCTL_REBUILDING_FILE);
    if (n <= 0 || (size_t)n >= sizeof p)
        return false;
    return access(p, F_OK) == 0;
}

#ifdef TSDB_EC

#define TIERCTL_POLL_MS_DEFAULT 1000

typedef struct ctl_slot {
    uint8_t id[16];     /* active id in backend (replacement id when REPLACED_BY, else original id) */
    uint8_t state;      /* last applied TIERS_STATE_* */
    int be_idx;         /* slot index in t->be, or -1 if already REMOVED at open */
    char *path;
    bool needs_rebuild; /* true while REPLACED_BY has not yet finished rebuild (REBUILDING present) */
    bool needs_shrink;  /* true while SHRINKING has not yet re-encoded all live objects and marked REMOVED */
    bool wait_other_up; /* parked waiting for another transiently DOWN (non-LOST) store to recover */
    bool shrink_park_logged; /* logged once while shrink waits on another LOST/DOWN store */
    bool shrink_err_logged;  /* logged once while shrink is blocked by damaged/unrecoverable object */
    uint64_t bytes_done_saved; /* accumulated bytes_done preserved across between-object yields */
    int64_t retry_after_ms;
    int fails;
} ctl_slot;

struct tierctl {
    tier *t;
    pthread_mutex_t mu;
    pthread_cond_t cv;
    pthread_cond_t idle_cv;
    pthread_cond_t shrink_cv;
    eng *shrink_cur; /* single engine pinned during eng_reencode (unrelated volumes never wait) */
    bool closing;
    atomic_bool closing_atomic;
    bool quiesced;
    bool worker_started;
    bool worker_done;
    bool worker_owns_ref;
    bool worker_stuck;
    pthread_t worker_th;

    /* TIERS stat/re-read tracking */
    bool check_tiers_now;
    bool checking_tiers;
    bool have_tiers_st;
    struct stat tiers_st;
    uint64_t tiers_file_end;
    int64_t next_stat_ms;
    int nslots;
    ctl_slot slots[TIERS_STORES_MAX];

    /* Live rate settings from TIERS SETTINGS (defaults from tiers.h) */
    atomic_uint rebuild_mb_s;
    atomic_uint scrub_mb_s;
    atomic_uint scrub_days;

    /* Live rebuild progress (guarded by mu) */
    tierctl_progress prog;
};

static int64_t ctl_mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static int64_t ctl_mono_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;
}

static int ctl_poll_ms(void)
{
#ifdef TSDB_TEST_HOOKS
    int v = atomic_load_explicit(&g_poll_ms, memory_order_relaxed);
    return v > 0 ? v : TIERCTL_POLL_MS_DEFAULT;
#else
    return TIERCTL_POLL_MS_DEFAULT;
#endif
}

static int ctl_ofd_lock(int fd, short type)
{
    struct flock fl = {.l_type = type, .l_whence = SEEK_SET};
    int r;
    while ((r = fcntl(fd, F_OFD_SETLKW, &fl)) != 0 && errno == EINTR)
        ;
    return r == 0 ? 0 : TSDB_ERR_IO;
}

/* ---------------------------------------------------------------- durable REBUILDING marker */

static int write_rebuilding_marker(const char *store_path, bool sync)
{
    int dfd = open(store_path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd < 0)
        return TSDB_ERR_IO;
    char tmp[64];
    snprintf(tmp, sizeof tmp, "%s.tmp", TIERCTL_REBUILDING_FILE);
    (void)unlinkat(dfd, tmp, 0);
    int fd = openat(dfd, tmp, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0644);
    if (fd < 0) {
        close(dfd);
        return TSDB_ERR_IO;
    }
    static const char msg[] = "rebuilding\n";
    int rc = 0;
    ssize_t w = write(fd, msg, sizeof msg - 1);
    if (w != (ssize_t)(sizeof msg - 1))
        rc = TSDB_ERR_IO;
    if (rc == 0 && sync && tsdb_fsync(fd) != 0)
        rc = TSDB_ERR_IO;
    if (close(fd) != 0 && rc == 0)
        rc = TSDB_ERR_IO;
    if (rc == 0 && renameat(dfd, tmp, dfd, TIERCTL_REBUILDING_FILE) != 0)
        rc = TSDB_ERR_IO;
    if (rc != 0)
        (void)unlinkat(dfd, tmp, 0);
    if (rc == 0 && sync && tsdb_fsync(dfd) != 0)
        rc = TSDB_ERR_IO;
    close(dfd);
    return rc;
}

static int clear_rebuilding_marker(const char *store_path)
{
    int dfd = open(store_path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd < 0)
        return TSDB_ERR_IO;
    int rc = 0;
    if (unlinkat(dfd, TIERCTL_REBUILDING_FILE, 0) != 0 && errno != ENOENT)
        rc = TSDB_ERR_IO;
    if (rc == 0 && tsdb_fsync(dfd) != 0)
        rc = TSDB_ERR_IO;
    close(dfd);
    return rc;
}

/* ---------------------------------------------------------------- pacer & progress callbacks */

typedef struct ctl_pacer {
    struct tierctl *ctl;
    int store;
    uint32_t rate_mb_s;
    int64_t start_ns;
    int64_t next_tiers_check_ms;
    uint64_t bytes_since;
    uint64_t total_bytes;
} ctl_pacer;

static bool tiers_file_changed_stat(struct tierctl *ctl)
{
    tier *t = ctl->t;
    if (!t || !t->from_tiers || !t->dir)
        return false;
    char tp[PATH_MAX + 16];
    snprintf(tp, sizeof tp, "%s/%s", t->dir, TIERS_FILE);
    struct stat st;
    if (stat(tp, &st) != 0)
        return false;
    bool changed = false;
    pthread_mutex_lock(&ctl->mu);
    if (!ctl->have_tiers_st || st.st_size != ctl->tiers_st.st_size || st.st_ino != ctl->tiers_st.st_ino ||
        st.st_mtime != ctl->tiers_st.st_mtime
#ifdef __linux__
        || st.st_mtim.tv_nsec != ctl->tiers_st.st_mtim.tv_nsec
#endif
    )
        changed = true;
    pthread_mutex_unlock(&ctl->mu);
    return changed;
}

/* Lightweight mid-object refresh of rebuild_mb_s only (no backend mutations), so a live `tier set --rebuild-mb-s`
 * takes effect within 1 s even during a multi-gigabyte single object rebuild. */
static void ctl_refresh_rebuild_rate(struct tierctl *ctl)
{
    tier *t = ctl->t;
    if (!t || !t->from_tiers || !t->dir)
        return;
    int rootfd = open(t->dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (rootfd < 0)
        return;
    tiers_view tv;
    tiers_view_init(&tv);
    if (tiers_load(rootfd, false, false, &tv) == 0 && tv.exists) {
        uint32_t rb = tv.global.rebuild_mb_s ? tv.global.rebuild_mb_s : TIERS_REBUILD_MB_S_DEFAULT;
        atomic_store_explicit(&ctl->rebuild_mb_s, rb, memory_order_relaxed);
    }
    tiers_view_free(&tv);
    close(rootfd);
}

static int ctl_cancel_cb(void *ctx)
{
    const struct tierctl *ctl = ctx;
    return atomic_load_explicit(&ctl->closing_atomic, memory_order_acquire) ? 1 : 0;
}

/* Checked only between objects/volumes so a long rebuild yields promptly for TIERS changes and health attaches
 * without discarding an in-progress chunk. */
static int ctl_yield_cb(void *ctx)
{
    struct tierctl *ctl = ctx;
    if (atomic_load_explicit(&ctl->closing_atomic, memory_order_acquire))
        return 1;
    if (tier_health_has_pending(ctl->t))
        return 1;
    pthread_mutex_lock(&ctl->mu);
    bool want_tiers = ctl->check_tiers_now;
    int64_t due = ctl->next_stat_ms;
    pthread_mutex_unlock(&ctl->mu);
    if (want_tiers)
        return 1;
    if (ctl->t && ctl->t->from_tiers && ctl_mono_ms() >= due) {
        if (tiers_file_changed_stat(ctl)) {
            pthread_mutex_lock(&ctl->mu);
            ctl->check_tiers_now = true;
            pthread_mutex_unlock(&ctl->mu);
            return 1;
        }
        pthread_mutex_lock(&ctl->mu);
        ctl->next_stat_ms = ctl_mono_ms() + ctl_poll_ms();
        pthread_mutex_unlock(&ctl->mu);
    }
    return 0;
}

static void ctl_pace_cb(void *ctx, uint64_t bytes_done)
{
    ctl_pacer *p = ctx;
    if (!p || bytes_done == 0)
        return;
    struct tierctl *ctl = p->ctl;
    p->total_bytes += bytes_done;

    /* At most once per poll interval, check whether TIERS changed so rebuild_mb_s updates live even mid-object. */
    int64_t now_ms = ctl_mono_ms();
    if (ctl->t && ctl->t->from_tiers && now_ms >= p->next_tiers_check_ms) {
        p->next_tiers_check_ms = now_ms + ctl_poll_ms();
        if (tiers_file_changed_stat(ctl)) {
            ctl_refresh_rebuild_rate(ctl);
            pthread_mutex_lock(&ctl->mu);
            ctl->check_tiers_now = true;
            pthread_mutex_unlock(&ctl->mu);
        }
    }

    uint32_t mb_s = atomic_load_explicit(&ctl->rebuild_mb_s, memory_order_relaxed);
    if (mb_s == 0)
        mb_s = TIERS_REBUILD_MB_S_DEFAULT;

    pthread_mutex_lock(&ctl->mu);
    if (ctl->prog.active && ctl->prog.store == p->store) {
        ctl->prog.bytes_done = p->total_bytes;
        ctl->prog.rate_mb_s = mb_s;
    }
    pthread_mutex_unlock(&ctl->mu);

#ifdef TSDB_TEST_HOOKS
    {
        void (*hook)(void *, int, uint64_t);
        void *hctx;
        pthread_mutex_lock(&g_hook_mu);
        hook = g_rebuild_hook;
        hctx = g_rebuild_hook_ctx;
        pthread_mutex_unlock(&g_hook_mu);
        if (hook)
            hook(hctx, p->store, p->total_bytes);
    }
#endif

    if (tierctl_mut() == TIERCTL_MUT_REBUILDFAST)
        return; /* planted mutant: ignores rebuild_mb_s pacing */

    if (p->start_ns == 0 || p->rate_mb_s != mb_s) {
        p->start_ns = ctl_mono_ns();
        p->bytes_since = 0;
        p->rate_mb_s = mb_s;
    }
    p->bytes_since += bytes_done;

    uint64_t rate_bps = (uint64_t)mb_s * (1024ull * 1024ull);
    uint64_t target_ns = (uint64_t)(((__uint128_t)p->bytes_since * 1000000000ull) / rate_bps);
    int64_t want_ns = p->start_ns + (int64_t)target_ns;

    int64_t now = ctl_mono_ns();
    if (now >= want_ns)
        return;

    pthread_mutex_lock(&ctl->mu);
    while (!ctl->closing) {
        int64_t cur_ns = ctl_mono_ns();
        if (cur_ns >= want_ns)
            break;
        uint64_t rem_ms = (uint64_t)((want_ns - cur_ns + 999999LL) / 1000000LL);
        if (rem_ms == 0)
            rem_ms = 1;
        int r = tsdb_cond_timedwait_ms(&ctl->cv, &ctl->mu, rem_ms);
        if (r == ETIMEDOUT)
            break;
    }
    pthread_mutex_unlock(&ctl->mu);
}

typedef struct ctl_before_up_ctx {
    const char *path;
} ctl_before_up_ctx;

static int ctl_before_up_cb(void *ctx, int store)
{
    (void)store;
    const ctl_before_up_ctx *b = ctx;
    if (!b || !b->path)
        return 0;
    return clear_rebuilding_marker(b->path);
}

/* Estimate total chunk bytes to rebuild for slot `store` by scanning one UP peer store's chunk files. */
static uint64_t estimate_store_bytes(chunk_backend *be, int skip_store)
{
    int n = chunk_be_nstores(be);
    for (int i = 0; i < n; i++) {
        if (i == skip_store)
            continue;
        if (chunk_be_store_state(be, i) != CHUNK_STORE_UP)
            continue;
        char pbuf[PATH_MAX];
        if (!chunk_be_store_path_copy(be, i, pbuf, sizeof pbuf))
            continue;
        int sfd = open(pbuf, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        if (sfd < 0)
            continue;
        DIR *sd = fdopendir(sfd);
        if (!sd) {
            close(sfd);
            continue;
        }
        uint64_t tot = 0;
        struct dirent *de;
        while ((de = readdir(sd)) != NULL) {
            if (de->d_name[0] != 's' || !isdigit((unsigned char)de->d_name[1]))
                continue;
            int vfd = openat(dirfd(sd), de->d_name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
            if (vfd < 0)
                continue;
            DIR *vd = fdopendir(vfd);
            if (!vd) {
                close(vfd);
                continue;
            }
            struct dirent *fe;
            while ((fe = readdir(vd)) != NULL) {
                size_t fl = strlen(fe->d_name);
                if (fl <= 6 || strcmp(fe->d_name + fl - 6, ".chunk") != 0)
                    continue;
                struct stat st;
                if (fstatat(dirfd(vd), fe->d_name, &st, AT_SYMLINK_NOFOLLOW) == 0 && S_ISREG(st.st_mode) &&
                    st.st_size > 0)
                    tot += (uint64_t)st.st_size;
            }
            closedir(vd);
        }
        closedir(sd);
        return tot;
    }
    return 0;
}

/* ---------------------------------------------------------------- live OID callback */

typedef struct ctl_oid_set {
    uint8_t (*v)[16];
    size_t n;
} ctl_oid_set;

static int ctl_oid_cmp16(const void *a, const void *b) { return memcmp(a, b, 16); }

static int ctl_live_cb(void *ctx, const uint8_t oid[16])
{
    const ctl_oid_set *s = ctx;
    return s->n > 0 && bsearch(oid, s->v, s->n, 16, ctl_oid_cmp16) != NULL ? 1 : 0;
}

/* ---------------------------------------------------------------- apply TIERS changes */

static void apply_live_settings(struct tierctl *ctl, tiers_view *tv)
{
    tier *t = ctl->t;
    atomic_store_explicit(&ctl->rebuild_mb_s, tv->global.rebuild_mb_s ? tv->global.rebuild_mb_s : TIERS_REBUILD_MB_S_DEFAULT,
                          memory_order_relaxed);
    atomic_store_explicit(&ctl->scrub_mb_s, tv->global.scrub_mb_s ? tv->global.scrub_mb_s : TIERS_SCRUB_MB_S_DEFAULT,
                          memory_order_relaxed);
    atomic_store_explicit(&ctl->scrub_days, tv->global.scrub_days, memory_order_relaxed);

    size_t n_elig = 0;
    bool all_sc = true;
    for (size_t i = 0; i < tv->nstores; i++) {
        if (tv->stores[i].state == TIERS_STATE_REMOVED || tv->stores[i].state == TIERS_STATE_SHRINKING)
            continue;
        n_elig++;
        if (!tv->stores[i].single_copy)
            all_sc = false;
    }
    if (n_elig == 0)
        all_sc = false;
    int def_k = 0, def_m = 0;
    bool has_def = tier_default_geom((int)n_elig, all_sc, &def_k, &def_m);

    tiers_view old_tv;
    pthread_mutex_lock(&t->cmu);
    old_tv = t->tview;
    t->tview = *tv; /* transfer ownership */
    memset(tv, 0, sizeof *tv);
    for (size_t i = 0; i < t->ncvs; i++) {
        coldvol *cv = t->cvs[i];
        tiers_settings vs;
        tiers_view_settings_for(&t->tview, cv->volume, &vs);
        if (vs.set_mask & TSDB_TIER_SET_REDUNDANCY) {
            bool ok = vs.m == 0 ? (vs.k == 1 && all_sc)
                                : (vs.k >= 1 && vs.k <= CHUNK_K_MAX && vs.m <= CHUNK_M_MAX &&
                                   (int)vs.k + (int)vs.m <= (int)n_elig);
            if (ok) {
                tier_settings_set_km(&cv->set, (int)vs.k, (int)vs.m);
            } else {
                fprintf(stderr, "tsdb: tier: ignoring invalid live redundancy %u+%u in %s for %zu store(s)\n",
                        (unsigned)vs.k, (unsigned)vs.m, TIERS_FILE, n_elig);
            }
        } else if (has_def) {
            tier_settings_set_km(&cv->set, def_k, def_m);
        }
        if (!t->ov_grace) {
            atomic_store_explicit(&cv->set.tier_after_s, vs.tier_after_s, memory_order_relaxed);
            atomic_store_explicit(&cv->set.grace_s, (int64_t)vs.tier_after_s, memory_order_relaxed);
        }
        atomic_store_explicit(&cv->set.tier_high_pct, vs.tier_high_pct, memory_order_relaxed);
        atomic_store_explicit(&cv->set.young_high_pct, vs.young_high_pct, memory_order_relaxed);
        atomic_store_explicit(&cv->set.rebuild_mb_s, vs.rebuild_mb_s, memory_order_relaxed);
        atomic_store_explicit(&cv->set.scrub_mb_s, vs.scrub_mb_s, memory_order_relaxed);
        atomic_store_explicit(&cv->set.scrub_days, vs.scrub_days, memory_order_relaxed);
    }
    pthread_mutex_unlock(&t->cmu);
    tiers_view_free(&old_tv);
}

static void apply_tiers_changes(struct tierctl *ctl)
{
    tier *t = ctl->t;
    if (!t || !t->from_tiers || !t->dir || !t->be)
        return;

    int rootfd = open(t->dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (rootfd < 0)
        return;

    /* Brief shared OFD lock on VOLUMES (if present) so we never observe a half-appended TIERS record. */
    int vfd = openat(rootfd, "VOLUMES", O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (vfd >= 0 && ctl_ofd_lock(vfd, F_RDLCK) != 0) {
        close(vfd);
        vfd = -1;
    }

    struct stat st;
    int sr = fstatat(rootfd, TIERS_FILE, &st, AT_SYMLINK_NOFOLLOW);
    tiers_view tv;
    tiers_view_init(&tv);
    int rc = sr == 0 ? tiers_load(rootfd, false, false, &tv) : TSDB_ERR_IO;

    if (vfd >= 0)
        close(vfd);
    close(rootfd);

    if (rc != 0 || !tv.exists) {
        tiers_view_free(&tv);
        return;
    }

    pthread_mutex_lock(&ctl->mu);
    ctl->tiers_st = st;
    ctl->have_tiers_st = true;
    ctl->tiers_file_end = tv.file_end;
    int cur_n = ctl->nslots;
    pthread_mutex_unlock(&ctl->mu);

    bool need_reopen = false;
    bool all_ok = true;

    /* 1. Act on STATE changes for existing slots (0 .. cur_n - 1). */
    int common = (int)tv.nstores < cur_n ? (int)tv.nstores : cur_n;
    for (int i = 0; i < common; i++) {
        const tiers_store *ts = &tv.stores[i];
        const uint8_t *want_id = ts->state == TIERS_STATE_REPLACED_BY ? ts->replaced_by : ts->id;

        pthread_mutex_lock(&ctl->mu);
        uint8_t prev_state = ctl->slots[i].state;
        int be_idx = ctl->slots[i].be_idx;
        bool id_changed = memcmp(ctl->slots[i].id, want_id, 16) != 0;
        pthread_mutex_unlock(&ctl->mu);

        if (ts->state == TIERS_STATE_LOST && prev_state != TIERS_STATE_LOST) {
            if (be_idx >= 0) {
                tier_health_store_lost(t, be_idx);
                tier_ensure_paused(t, be_idx, "is LOST");
                (void)chunk_be_mark_lost(t->be, be_idx);
                need_reopen = true;
                chunk_be_release_down(t->be, be_idx);
            }
            char *pcopy = strdup(ts->path);
            pthread_mutex_lock(&ctl->mu);
            ctl->slots[i].state = TIERS_STATE_LOST;
            memcpy(ctl->slots[i].id, ts->id, 16);
            ctl->slots[i].needs_rebuild = false;
            ctl->slots[i].needs_shrink = false;
            ctl->slots[i].wait_other_up = false;
            ctl->slots[i].bytes_done_saved = 0;
            ctl->slots[i].retry_after_ms = 0;
            ctl->slots[i].fails = 0;
            if (pcopy) {
                free(ctl->slots[i].path);
                ctl->slots[i].path = pcopy;
            }
            /* A store transitioning from transiently DOWN to permanently LOST unblocks parked rebuilds. */
            for (int j = 0; j < ctl->nslots; j++)
                if (ctl->slots[j].needs_rebuild || ctl->slots[j].needs_shrink) {
                    ctl->slots[j].wait_other_up = false;
                    ctl->slots[j].retry_after_ms = 0;
                }
            pthread_mutex_unlock(&ctl->mu);
            fprintf(stderr, "tsdb: tier: store %d (%s) declared LOST in %s\n", i, ts->path, TIERS_FILE);
        } else if (ts->state == TIERS_STATE_REPLACED_BY && (prev_state != TIERS_STATE_REPLACED_BY || id_changed)) {
            if (be_idx >= 0) {
                tier_health_store_lost(t, be_idx);
                tier_ensure_paused(t, be_idx, "is being replaced");
                (void)chunk_be_mark_lost(t->be, be_idx);
                need_reopen = true;
                chunk_be_release_down(t->be, be_idx);
            }
            char *pcopy = strdup(ts->path);
            pthread_mutex_lock(&ctl->mu);
            ctl->slots[i].state = TIERS_STATE_REPLACED_BY;
            memcpy(ctl->slots[i].id, ts->replaced_by, 16);
            ctl->slots[i].needs_rebuild = be_idx >= 0;
            ctl->slots[i].needs_shrink = false;
            ctl->slots[i].wait_other_up = false;
            ctl->slots[i].bytes_done_saved = 0;
            ctl->slots[i].retry_after_ms = 0;
            ctl->slots[i].fails = 0;
            if (pcopy) {
                free(ctl->slots[i].path);
                ctl->slots[i].path = pcopy;
            }
            for (int j = 0; j < ctl->nslots; j++)
                if (j != i && (ctl->slots[j].needs_rebuild || ctl->slots[j].needs_shrink)) {
                    ctl->slots[j].wait_other_up = false;
                    ctl->slots[j].retry_after_ms = 0;
                }
            pthread_mutex_unlock(&ctl->mu);
            fprintf(stderr, "tsdb: tier: store %d replaced by %s in %s: queuing rebuild\n", i, ts->path, TIERS_FILE);
        } else if (ts->state == TIERS_STATE_SHRINKING && prev_state != TIERS_STATE_SHRINKING) {
            if (be_idx >= 0) {
                tier_health_store_lost(t, be_idx);
                (void)chunk_be_mark_lost(t->be, be_idx);
                chunk_be_set_excluded(t->be, be_idx, true);
                need_reopen = true;
                chunk_be_release_down(t->be, be_idx);
            }
            char *pcopy = strdup(ts->path);
            pthread_mutex_lock(&ctl->mu);
            ctl->slots[i].state = TIERS_STATE_SHRINKING;
            memcpy(ctl->slots[i].id, ts->id, 16);
            ctl->slots[i].needs_rebuild = false;
            ctl->slots[i].needs_shrink = true;
            ctl->slots[i].wait_other_up = false;
            ctl->slots[i].bytes_done_saved = 0;
            ctl->slots[i].retry_after_ms = 0;
            ctl->slots[i].fails = 0;
            if (pcopy) {
                free(ctl->slots[i].path);
                ctl->slots[i].path = pcopy;
            }
            for (int j = 0; j < ctl->nslots; j++)
                if (ctl->slots[j].needs_rebuild || ctl->slots[j].needs_shrink) {
                    ctl->slots[j].wait_other_up = false;
                    ctl->slots[j].retry_after_ms = 0;
                }
            pthread_mutex_unlock(&ctl->mu);
            fprintf(stderr, "tsdb: tier: store %d (%s) is SHRINKING in %s: queuing re-encode onto remaining stores\n",
                    i, ts->path, TIERS_FILE);
        } else if (ts->state == TIERS_STATE_REMOVED && prev_state != TIERS_STATE_REMOVED) {
            if (be_idx >= 0) {
                tier_health_store_lost(t, be_idx);
                (void)chunk_be_mark_lost(t->be, be_idx);
                chunk_be_set_excluded(t->be, be_idx, true);
                need_reopen = true;
                chunk_be_release_down(t->be, be_idx);
            }
            pthread_mutex_lock(&ctl->mu);
            ctl->slots[i].state = TIERS_STATE_REMOVED;
            memcpy(ctl->slots[i].id, ts->id, 16);
            ctl->slots[i].needs_rebuild = false;
            ctl->slots[i].needs_shrink = false;
            ctl->slots[i].wait_other_up = false;
            ctl->slots[i].retry_after_ms = 0;
            ctl->slots[i].fails = 0;
            pthread_mutex_unlock(&ctl->mu);
            fprintf(stderr, "tsdb: tier: store %d (%s) marked REMOVED in %s\n", i, ts->path, TIERS_FILE);
        }
    }

    if (need_reopen)
        eccache_reopen_all(t);

    /* 2. Open newly added stores (cur_n .. tv.nstores - 1). */
    for (int i = cur_n; i < (int)tv.nstores && i < TIERS_STORES_MAX; i++) {
        const tiers_store *ts = &tv.stores[i];
        int add_rc = 0;
        bool is_rebuilding = false;
        bool is_shrinking = false;
        int new_be_idx = -1;
        const uint8_t *want_id = ts->state == TIERS_STATE_REPLACED_BY ? ts->replaced_by : ts->id;
        if (ts->state == TIERS_STATE_REMOVED) {
            new_be_idx = -1;
        } else if (ts->state == TIERS_STATE_LOST || ts->state == TIERS_STATE_SHRINKING) {
            add_rc = chunk_be_add_lost(t->be, ts->path);
            if (add_rc == 0) {
                new_be_idx = chunk_be_nstores(t->be) - 1;
                (void)tier_health_store_added(t, new_be_idx, ts->path, ts->id, false);
                if (ts->state == TIERS_STATE_SHRINKING) {
                    chunk_be_set_excluded(t->be, new_be_idx, true);
                    is_shrinking = true;
                } else {
                    tier_ensure_paused(t, new_be_idx, "is LOST");
                }
            }
        } else {
            is_rebuilding = ts->state == TIERS_STATE_REPLACED_BY && tierctl_store_rebuilding(ts->path);
            add_rc = chunk_be_add_store(t->be, ts->path, want_id, t->db_id, is_rebuilding);
            if (add_rc == 0) {
                new_be_idx = chunk_be_nstores(t->be) - 1;
                (void)tier_health_store_added(t, new_be_idx, ts->path, want_id, !is_rebuilding);
            }
        }
        if (add_rc != 0) {
            fprintf(stderr, "tsdb: tier: newly added store %d (%s) could not be opened (%d): will retry\n", i, ts->path,
                    add_rc);
            all_ok = false;
            break; /* keep slot indices strictly in order */
        }
        char *pcopy = strdup(ts->path);
        pthread_mutex_lock(&ctl->mu);
        ctl->slots[i].state = ts->state;
        ctl->slots[i].be_idx = new_be_idx;
        memcpy(ctl->slots[i].id, want_id, 16);
        ctl->slots[i].path = pcopy;
        ctl->slots[i].needs_rebuild = is_rebuilding;
        ctl->slots[i].needs_shrink = is_shrinking;
        ctl->slots[i].retry_after_ms = 0;
        ctl->slots[i].fails = 0;
        ctl->nslots = i + 1;
        pthread_mutex_unlock(&ctl->mu);
        if (new_be_idx >= 0 && !is_rebuilding && !is_shrinking && ts->state != TIERS_STATE_LOST)
            tierscrub_on_store_changed(t, new_be_idx, ts->path);
        fprintf(stderr, "tsdb: tier: opened newly added store %d (%s)\n", i, ts->path);
    }

    /* 3. Apply live SETTINGS (updates rebuild_mb_s, scrub_mb_s, scrub_days, and every volume's coldvol->set). */
    apply_live_settings(ctl, &tv);

    if (all_ok)
        tier_resume_if_all_up(t);
    else {
        /* Retry opening any store that failed on the next poll tick. */
        pthread_mutex_lock(&ctl->mu);
        ctl->have_tiers_st = false;
        pthread_mutex_unlock(&ctl->mu);
    }
}

/* ---------------------------------------------------------------- paced replacement rebuild */

static void run_rebuild(struct tierctl *ctl, int idx)
{
    tier *t = ctl->t;
    if (!t || !t->be)
        return;

    char path[PATH_MAX];
    uint8_t expect_id[16];
    int be_idx = -1;
    pthread_mutex_lock(&ctl->mu);
    if (ctl->closing || !ctl->slots[idx].needs_rebuild || !ctl->slots[idx].path || ctl->slots[idx].be_idx < 0) {
        if (ctl->slots[idx].be_idx < 0)
            ctl->slots[idx].needs_rebuild = false;
        pthread_mutex_unlock(&ctl->mu);
        return;
    }
    be_idx = ctl->slots[idx].be_idx;
    snprintf(path, sizeof path, "%s", ctl->slots[idx].path);
    memcpy(expect_id, ctl->slots[idx].id, 16);
    pthread_mutex_unlock(&ctl->mu);

    /* Pause settles/GC across the rebuild so live OIDs are stable and no new chunks are placed on a partial store. */
    tier_ensure_paused(t, be_idx, "is rebuilding");

    /* Wait briefly if any settle/reencode is currently in flight. */
    if (tier_health_any_settling(t)) {
        pthread_mutex_lock(&ctl->mu);
        ctl->slots[idx].retry_after_ms = ctl_mono_ms() + 200;
        pthread_mutex_unlock(&ctl->mu);
        return;
    }

    pthread_mutex_lock(&ctl->mu);
    if (ctl->closing || !ctl->slots[idx].needs_rebuild) {
        pthread_mutex_unlock(&ctl->mu);
        return;
    }
    uint32_t mb_s = atomic_load_explicit(&ctl->rebuild_mb_s, memory_order_relaxed);
    if (mb_s == 0)
        mb_s = TIERS_REBUILD_MB_S_DEFAULT;
    ctl->prog.active = true;
    ctl->prog.store = idx;
    uint64_t saved_done = ctl->slots[idx].bytes_done_saved;
    ctl->prog.bytes_done = saved_done;
    ctl->prog.bytes_est = 0;
    ctl->prog.rate_mb_s = mb_s;
    pthread_mutex_unlock(&ctl->mu);

    uint64_t est = estimate_store_bytes(t->be, be_idx);
    pthread_mutex_lock(&ctl->mu);
    ctl->prog.bytes_est = est;
    pthread_mutex_unlock(&ctl->mu);

    ctl_pacer pacer = {
        .ctl = ctl,
        .store = idx,
        .next_tiers_check_ms = ctl_mono_ms() + ctl_poll_ms(),
        .total_bytes = saved_done,
    };
    ctl_before_up_ctx bctx = {.path = path};

    ctl_oid_set live = {0};
    bool complete = false;
    bool use_live = !atomic_load(&ecgc_test_nosweep);
    if (use_live && (tier_health_live_oids(t, &live.v, &live.n, &complete) != 0 || !complete))
        use_live = false;

    chunk_heal_opts ho = {
        .epoch = 0,
        .scrub = false,
        .live = use_live ? ctl_live_cb : NULL,
        .live_ctx = use_live ? &live : NULL,
        .cancel = ctl_cancel_cb,
        .cancel_ctx = ctl,
        .yield = ctl_yield_cb,
        .yield_ctx = ctl,
        .pace = ctl_pace_cb,
        .pace_ctx = &pacer,
        .before_up = ctl_before_up_cb,
        .before_up_ctx = &bctx,
    };

    chunk_heal_report rep;
    memset(&rep, 0, sizeof rep);

    int cur = chunk_be_store_state(t->be, be_idx);
    int rc = 0;
    if (atomic_load_explicit(&ctl->closing_atomic, memory_order_acquire)) {
        rc = TSDB_ERR_STOPPED;
#ifdef TSDB_TEST_HOOKS
    } else if (tierctl_mut() == TIERCTL_MUT_LOSTNOREPLACE) {
        /* Planted mutant `lostnoreplace`: promotes replacement store to UP without rebuilding any chunks. */
        rc = chunk_be_promote_up_norebuild(t->be, be_idx, path, expect_id, t->db_id, &ho);
#endif
    } else if (cur == CHUNK_STORE_REPLACING) {
        /* Resuming a rebuild after kill/power loss or transient unavailability: ensure every volume's fence is in ns. */
        rc = chunk_be_fence_store(t->be, be_idx);
        if (rc == 0)
            rc = chunk_be_settle(t->be, be_idx, &ho, &rep);
    } else {
        /* Live replacement triggered from TIERS: install pre-initialized store at slot be_idx and rebuild. */
        rc = chunk_be_replace_open(t->be, be_idx, path, expect_id, t->db_id, &ho, &rep);
    }
    free(live.v);

    /* Check whether another non-LOST store is currently transiently DOWN (if so, park instead of timer-spinning). */
    bool other_down = false;
    if (rc == TSDB_ERR_UNAVAILABLE) {
        int bn = chunk_be_nstores(t->be);
        for (int j = 0; j < bn; j++) {
            if (j == be_idx || chunk_be_store_is_lost(t->be, j) || chunk_be_store_excluded(t->be, j))
                continue;
            int st = chunk_be_store_state(t->be, j);
            if (st != CHUNK_STORE_UP && st != CHUNK_STORE_REPLACING) {
                other_down = true;
                break;
            }
        }
    }

    pthread_mutex_lock(&ctl->mu);
    ctl->prog.active = false;
    ctl->prog.store = -1;
    ctl->prog.objects_done = rep.objects;
    ctl->prog.rebuilt_chunks = rep.rebuilt_missing + rep.rebuilt_damaged;
    if (rc == 0) {
        ctl->prog.rebuilds_done++;
        ctl->slots[idx].needs_rebuild = false;
        ctl->slots[idx].wait_other_up = false;
        ctl->slots[idx].bytes_done_saved = 0;
        ctl->slots[idx].fails = 0;
        ctl->slots[idx].retry_after_ms = 0;
    } else if (rc == TSDB_ERR_STOPPED) {
        /* Clean yield between objects (or closing): preserve progress and resume right after pending work runs. */
        ctl->slots[idx].bytes_done_saved = pacer.total_bytes;
        ctl->slots[idx].retry_after_ms = 0;
    } else if (rc == TSDB_ERR_UNAVAILABLE && other_down) {
        /* Parked until the other transiently DOWN store recovers via health attach or is declared LOST in TIERS. */
        ctl->slots[idx].wait_other_up = true;
        ctl->slots[idx].bytes_done_saved = pacer.total_bytes;
        ctl->slots[idx].fails = 0;
        ctl->slots[idx].retry_after_ms = 0;
    } else {
        ctl->slots[idx].bytes_done_saved = pacer.total_bytes;
        if (ctl->slots[idx].fails < 6)
            ctl->slots[idx].fails++;
        int64_t backoff = (int64_t)500 << (ctl->slots[idx].fails - 1);
        if (backoff > 15000)
            backoff = 15000;
        ctl->slots[idx].retry_after_ms = ctl_mono_ms() + backoff;
    }
    pthread_cond_broadcast(&ctl->idle_cv);
    pthread_mutex_unlock(&ctl->mu);

    if (rc == 0 && !atomic_load_explicit(&ctl->closing_atomic, memory_order_acquire)) {
        tier_health_store_replaced(t, be_idx, path, expect_id);
        tierscrub_on_store_changed(t, be_idx, path);
        eccache_reopen_all(t);
        tier_resume_if_all_up(t);
        /* If another slot is still REPLACING or SHRINKING waiting for this store to come UP, retry it immediately. */
        pthread_mutex_lock(&ctl->mu);
        for (int j = 0; j < ctl->nslots; j++)
            if (j != idx && (ctl->slots[j].needs_rebuild || ctl->slots[j].needs_shrink)) {
                ctl->slots[j].wait_other_up = false;
                ctl->slots[j].retry_after_ms = 0;
            }
        pthread_mutex_unlock(&ctl->mu);
    }
}

/* ---------------------------------------------------------------- paced shrink re-encode (r15-shrink, D9) */

static int ctl_read_volumes(int vfd, char ***paths_out, size_t *n_out);
static void ctl_free_paths(char **paths, size_t n);

static bool ctl_eng_closing(const eng *db)
{
    return atomic_load_explicit(&db->stop, memory_order_acquire) ||
           atomic_load_explicit(&db->quiesced, memory_order_acquire);
}

/* Returns 1 if live .ec part `p` still needs re-encoding onto the remaining active stores (either its width exceeds
 * the remaining active store count, or at least one position is absent from its post-shrink target store), 0 if every
 * position already lives on its exact post-shrink active store, or -1 if an active store could not be checked. */
static int part_needs_shrink(chunk_backend *be, uint32_t vol, const uint8_t oid[16], uint16_t k, uint16_t m)
{
    unsigned np = (unsigned)k + (unsigned)m;
    if (np == 0 || np > CHUNK_POS_MAX)
        return 1;
    int ne = chunk_be_active_count(be);
    if (ne < (int)np)
        return 1;
    for (unsigned pos = 0; pos < np; pos++) {
        int target = chunk_be_place(be, oid, pos);
        int has = chunk_be_store_has_chunk(be, target, vol, oid, pos);
        if (has < 0)
            return -1;
        if (has == 0)
            return 1;
    }
    return 0;
}

/* Counts expected total volumes (root + VOLUMES lines) for root dir `dir`. Returns true on clean read. */
static bool ctl_expected_vol_count(const char *dir, size_t *out)
{
    *out = 1;
    if (!dir || !*dir)
        return false;
    int rootfd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (rootfd < 0)
        return false;
    int vfd = openat(rootfd, "VOLUMES", O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    close(rootfd);
    if (vfd < 0)
        return errno == ENOENT;
    if (ctl_ofd_lock(vfd, F_RDLCK) != 0) {
        close(vfd);
        return false;
    }
    char **paths = NULL;
    size_t nv = 0;
    int rc = ctl_read_volumes(vfd, &paths, &nv);
    close(vfd);
    ctl_free_paths(paths, nv);
    if (rc != 0)
        return false;
    *out = 1 + nv;
    return true;
}

typedef struct shrink_item {
    uint32_t vol;
    int64_t partition;
    char name[96];
    uint8_t oid[16];
    uint16_t k, m;
    uint64_t obj_len;
    bool busy;
    bool damaged;
    bool need;
} shrink_item;

/* Appends TIERS_STATE_REMOVED for slot `slot_idx` under the VOLUMES OFD write lock. Always syncs TIERS (sync=true)
 * deliberately so REMOVED is durable on disk independently of per-volume opt.fsync, matching tiers_append_state calls
 * from CLI and tsdb_tier_lost. */
static int finish_shrink_removed(struct tierctl *ctl, int slot_idx)
{
    tier *t = ctl->t;
    if (!t || !t->dir)
        return TSDB_ERR_INVALID;
    int rootfd = open(t->dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (rootfd < 0)
        return TSDB_ERR_IO;
    int vfd = openat(rootfd, "VOLUMES", O_RDWR | O_APPEND | O_CREAT | O_CLOEXEC, 0644);
    if (vfd < 0) {
        close(rootfd);
        return TSDB_ERR_IO;
    }
    int rc = ctl_ofd_lock(vfd, F_WRLCK);
    tiers_view tv;
    tiers_view_init(&tv);
    if (rc == 0)
        rc = tiers_load(rootfd, true, true, &tv);
    char spath[PATH_MAX] = "";
    if (rc == 0) {
        if ((size_t)slot_idx >= tv.nstores) {
            rc = TSDB_ERR_INVALID;
        } else if (tv.stores[slot_idx].state == TIERS_STATE_SHRINKING) {
            snprintf(spath, sizeof spath, "%s", tv.stores[slot_idx].path);
            rc = tiers_append_state(rootfd, true, tv.stores[slot_idx].id, TIERS_STATE_REMOVED, 0, false, NULL, NULL);
        } else if (tv.stores[slot_idx].state == TIERS_STATE_REMOVED) {
            snprintf(spath, sizeof spath, "%s", tv.stores[slot_idx].path);
            rc = 0; /* already marked REMOVED */
        } else {
            rc = TSDB_ERR_INVALID;
        }
    }
    struct stat st;
    bool have_st = rc == 0 && fstatat(rootfd, TIERS_FILE, &st, AT_SYMLINK_NOFOLLOW) == 0;
    tiers_view_free(&tv);
    close(vfd);
    close(rootfd);

    if (rc == 0) {
        pthread_mutex_lock(&t->cmu);
        if ((size_t)slot_idx < t->tview.nstores)
            t->tview.stores[slot_idx].state = TIERS_STATE_REMOVED;
        pthread_mutex_unlock(&t->cmu);

        pthread_mutex_lock(&ctl->mu);
        ctl->slots[slot_idx].state = TIERS_STATE_REMOVED;
        ctl->slots[slot_idx].needs_shrink = false;
        ctl->slots[slot_idx].wait_other_up = false;
        ctl->slots[slot_idx].shrink_park_logged = false;
        ctl->slots[slot_idx].shrink_err_logged = false;
        ctl->slots[slot_idx].bytes_done_saved = 0;
        ctl->slots[slot_idx].fails = 0;
        ctl->slots[slot_idx].retry_after_ms = 0;
        ctl->prog.shrink_active = false;
        ctl->prog.shrink_store = -1;
        ctl->prog.shrink_objects_left = 0;
        ctl->prog.shrinks_done++;
        if (have_st) {
            ctl->tiers_st = st;
            ctl->have_tiers_st = true;
        }
        pthread_cond_broadcast(&ctl->idle_cv);
        pthread_mutex_unlock(&ctl->mu);
        fprintf(stderr, "tsdb: tier: store %d (%s) shrink complete: marked REMOVED in %s\n", slot_idx,
                spath[0] ? spath : "-", TIERS_FILE);
    }
    return rc;
}

/* Pins ONLY the single engine matching `vol` into ctl->shrink_cur (strict lock order: t->cmu -> ctl->mu).
 * Returns the pinned engine (caller MUST call ctl_shrink_unpin_eng), or NULL if absent/closing. */
static eng *ctl_shrink_pin_eng(struct tierctl *ctl, uint32_t vol)
{
    tier *t = ctl->t;
    eng *found = NULL;
    pthread_mutex_lock(&t->cmu);
    for (size_t i = 0; i < t->nengs; i++) {
        eng *db = t->engs[i];
        if (db->tier_vol == vol && !ctl_eng_closing(db)) {
            found = db;
            break;
        }
    }
    if (found) {
        pthread_mutex_lock(&ctl->mu);
        ctl->shrink_cur = found;
        /* Dekker re-check under ctl->mu against eng_halt() setting db->stop before calling tierctl_eng_stop(db). */
        if (ctl_eng_closing(found)) {
            ctl->shrink_cur = NULL;
            pthread_cond_broadcast(&ctl->shrink_cv);
            found = NULL;
        }
        pthread_mutex_unlock(&ctl->mu);
    }
    pthread_mutex_unlock(&t->cmu);
    return found;
}

static void ctl_shrink_unpin_eng(struct tierctl *ctl)
{
    pthread_mutex_lock(&ctl->mu);
    ctl->shrink_cur = NULL;
    pthread_cond_broadcast(&ctl->shrink_cv);
    pthread_mutex_unlock(&ctl->mu);
}

static void run_shrink(struct tierctl *ctl, int slot_idx)
{
    tier *t = ctl->t;
    if (!t || !t->be)
        return;

    /* Ensure all non-excluded stores are UP; park if another store is transiently DOWN, REPLACING, or LOST. */
    int bn = chunk_be_nstores(t->be);
    int elig = 0;
    bool other_down = false, other_lost = false;
    int first_lost = -1, first_down = -1;
    for (int j = 0; j < bn; j++) {
        if (chunk_be_store_excluded(t->be, j))
            continue;
        elig++;
        int st = chunk_be_store_state(t->be, j);
        if (st != CHUNK_STORE_UP) {
            if (chunk_be_store_is_lost(t->be, j)) {
                other_lost = true;
                if (first_lost < 0)
                    first_lost = j;
            } else {
                other_down = true;
                if (first_down < 0)
                    first_down = j;
            }
        }
    }
    if (elig == 0 || other_down || other_lost) {
        bool do_log = false;
        pthread_mutex_lock(&ctl->mu);
        if (!ctl->slots[slot_idx].shrink_park_logged) {
            ctl->slots[slot_idx].shrink_park_logged = true;
            do_log = true;
        }
        if (other_down && !other_lost)
            ctl->slots[slot_idx].wait_other_up = true;
        else
            ctl->slots[slot_idx].retry_after_ms = ctl_mono_ms() + 1000;
        pthread_cond_broadcast(&ctl->idle_cv);
        pthread_mutex_unlock(&ctl->mu);
        if (do_log) {
            if (other_lost)
                fprintf(stderr, "tsdb: tier: shrink of store %d waiting: another store (slot %d) is LOST; "
                                "replace or shrink it so remaining stores are UP\n",
                        slot_idx, first_lost);
            else if (other_down)
                fprintf(stderr, "tsdb: tier: shrink of store %d waiting: another store (slot %d) is not UP\n",
                        slot_idx, first_down);
        }
        return;
    }

    tier_resume_if_all_up(t);
    if (!tier_writable(t)) {
        bool do_log = false;
        pthread_mutex_lock(&ctl->mu);
        if (!ctl->slots[slot_idx].shrink_park_logged) {
            ctl->slots[slot_idx].shrink_park_logged = true;
            do_log = true;
        }
        ctl->slots[slot_idx].retry_after_ms = ctl_mono_ms() + 500;
        pthread_cond_broadcast(&ctl->idle_cv);
        pthread_mutex_unlock(&ctl->mu);
        if (do_log)
            fprintf(stderr, "tsdb: tier: shrink of store %d waiting: tier is not writable (halted or fenced)\n",
                    slot_idx);
        return;
    }

    pthread_mutex_lock(&ctl->mu);
    if (ctl->closing || !ctl->slots[slot_idx].needs_shrink) {
        pthread_mutex_unlock(&ctl->mu);
        return;
    }
    ctl->slots[slot_idx].shrink_park_logged = false;
    uint32_t mb_s = atomic_load_explicit(&ctl->rebuild_mb_s, memory_order_relaxed);
    if (mb_s == 0)
        mb_s = TIERS_REBUILD_MB_S_DEFAULT;
    uint64_t saved_done = ctl->slots[slot_idx].bytes_done_saved;
    ctl->prog.shrink_active = true;
    ctl->prog.shrink_store = slot_idx;
    ctl->prog.rate_mb_s = mb_s;
    pthread_mutex_unlock(&ctl->mu);

    ctl_pacer pacer = {
        .ctl = ctl,
        .store = slot_idx,
        .next_tiers_check_ms = ctl_mono_ms() + ctl_poll_ms(),
        .total_bytes = saved_done,
    };

    int rc = 0;
    bool retry_soon = false;
    bool park_down = false;

    for (;;) {
        if (atomic_load_explicit(&ctl->closing_atomic, memory_order_acquire) || ctl_yield_cb(ctl)) {
            rc = TSDB_ERR_STOPPED;
            break;
        }

        size_t expect_vols = 1;
        if (!ctl_expected_vol_count(t->dir, &expect_vols)) {
            retry_soon = true;
            break;
        }
        uint32_t *bvols = NULL;
        size_t nbvols = 0;
        if (chunk_be_volumes_list(t->be, &bvols, &nbvols) != 0) {
            retry_soon = true;
            break;
        }

        /* Step 1 (memory-only): snapshot live .ec part metadata under t->eng_active, then release t->eng_active
         * BEFORE any store fstatat or re-encode I/O (tier_health.c locking rule: eng_active is never held across I/O). */
        shrink_item *items = NULL;
        size_t nitems = 0, capitems = 0;
        bool ready = false, incomplete = false, oom = false, any_settling = false;

        pthread_mutex_lock(&t->cmu);
        ready = t->eng_opening == 0 && t->nengs > 0 && t->nengs >= t->ncvs && t->nengs >= expect_vols;
        size_t nengs = ready ? t->nengs : 0;
        eng **engs = (ready && nengs) ? malloc(nengs * sizeof *engs) : NULL;
        if (ready && engs) {
            memcpy(engs, t->engs, nengs * sizeof *engs);
            t->eng_active++;
        }
        pthread_mutex_unlock(&t->cmu);

        if (!ready || !engs) {
            free(bvols);
            free(engs);
            if (!ready) {
                bool do_log = false;
                pthread_mutex_lock(&ctl->mu);
                if (!ctl->slots[slot_idx].shrink_park_logged) {
                    ctl->slots[slot_idx].shrink_park_logged = true;
                    do_log = true;
                }
                pthread_mutex_unlock(&ctl->mu);
                if (do_log)
                    fprintf(stderr, "tsdb: tier: shrink of store %d waiting: not all %zu volume(s) are open/ready\n",
                            slot_idx, expect_vols);
            }
            retry_soon = true;
            break;
        }

        for (size_t i = 0; i < nengs && !incomplete; i++)
            if (ctl_eng_closing(engs[i]))
                incomplete = true;
        for (size_t vi = 0; vi < nbvols && !incomplete; vi++) {
            bool found = false;
            for (size_t ei = 0; ei < nengs; ei++)
                if (engs[ei]->tier_vol == bvols[vi]) {
                    found = true;
                    break;
                }
            if (!found)
                incomplete = true;
        }
        free(bvols);

        for (size_t ei = 0; ei < nengs && !incomplete && !oom; ei++) {
            eng *db = engs[ei];
            pthread_mutex_lock(&db->lock);
            for (size_t pi = 0; pi < db->nparts; pi++) {
                part *p = db->parts[pi];
                if (p->settling)
                    any_settling = true;
                if (p->dropped)
                    continue;
                struct ecpart *ep = atomic_load_explicit(&p->ec, memory_order_acquire);
                if (!ep || ep->test_mark)
                    continue;
                if (nitems == capitems) {
                    size_t ncap = capitems ? capitems * 2 : 64;
                    shrink_item *ni = realloc(items, ncap * sizeof *ni);
                    if (!ni) {
                        oom = true;
                        break;
                    }
                    items = ni;
                    capitems = ncap;
                }
                shrink_item *it = &items[nitems++];
                memset(it, 0, sizeof *it);
                it->vol = db->tier_vol;
                it->partition = p->partition;
                snprintf(it->name, sizeof it->name, "%s", p->name);
                memcpy(it->oid, ep->rec.oid, 16);
                it->k = ep->rec.k;
                it->m = ep->rec.m;
                it->obj_len = ep->rec.object_len;
                it->busy = p->settling || p->merging || p->pending != NULL;
                it->damaged = atomic_load_explicit(&p->damaged, memory_order_acquire);
            }
            pthread_mutex_unlock(&db->lock);
        }

        free(engs);
        pthread_mutex_lock(&t->cmu);
        if (--t->eng_active == 0)
            pthread_cond_broadcast(&t->eng_cv);
        pthread_mutex_unlock(&t->cmu);

        if (incomplete || oom) {
            free(items);
            if (incomplete) {
                bool do_log = false;
                pthread_mutex_lock(&ctl->mu);
                if (!ctl->slots[slot_idx].shrink_park_logged) {
                    ctl->slots[slot_idx].shrink_park_logged = true;
                    do_log = true;
                }
                pthread_mutex_unlock(&ctl->mu);
                if (do_log)
                    fprintf(stderr, "tsdb: tier: shrink of store %d waiting: a volume engine is opening, closing, "
                                    "or quiesced\n",
                            slot_idx);
            }
            retry_soon = true;
            break;
        }

        /* Step 2 (outside t->eng_active): check chunk placement once per pass across all snapshotted objects. */
        uint64_t left = 0;
        bool scan_err = false, any_damaged = false;
        uint32_t dmg_vol = 0;
        int64_t dmg_part = 0;
        size_t ncand = 0;
        for (size_t i = 0; i < nitems; i++) {
            if (atomic_load_explicit(&ctl->closing_atomic, memory_order_acquire) || ctl_yield_cb(ctl)) {
                rc = TSDB_ERR_STOPPED;
                break;
            }
            int need = part_needs_shrink(t->be, items[i].vol, items[i].oid, items[i].k, items[i].m);
            if (need < 0) {
                scan_err = true;
                break;
            }
            if (need > 0) {
                left++;
                if (items[i].damaged) {
                    if (!any_damaged) {
                        dmg_vol = items[i].vol;
                        dmg_part = items[i].partition;
                    }
                    any_damaged = true;
                } else if (!items[i].busy) {
                    items[i].need = true;
                    ncand++;
                }
            }
        }
        if (rc == TSDB_ERR_STOPPED) {
            free(items);
            break;
        }
        if (scan_err) {
            free(items);
            for (int j = 0; j < bn; j++)
                if (!chunk_be_store_excluded(t->be, j) && !chunk_be_store_is_lost(t->be, j) &&
                    chunk_be_store_state(t->be, j) != CHUNK_STORE_UP)
                    park_down = true;
            if (!park_down) {
                bool do_log = false;
                pthread_mutex_lock(&ctl->mu);
                if (!ctl->slots[slot_idx].shrink_park_logged) {
                    ctl->slots[slot_idx].shrink_park_logged = true;
                    do_log = true;
                }
                pthread_mutex_unlock(&ctl->mu);
                if (do_log)
                    fprintf(stderr, "tsdb: tier: shrink of store %d waiting: chunk store stat check failed; retrying\n",
                            slot_idx);
            }
            retry_soon = !park_down;
            break;
        }

        pthread_mutex_lock(&ctl->mu);
        ctl->prog.shrink_objects_left = left;
        pthread_mutex_unlock(&ctl->mu);

#ifdef TSDB_TEST_HOOKS
        if (tierctl_mut() == TIERCTL_MUT_SHRINKEARLY && left > 0) {
            /* Planted mutant `shrinkearly`: marks STATE REMOVED while live objects still reference the store. */
            free(items);
            rc = finish_shrink_removed(ctl, slot_idx);
            break;
        }
#endif

        if (left == 0) {
            free(items);
            if (any_settling || tier_health_any_settling(t)) {
                retry_soon = true;
                break;
            }
            /* Re-verify all non-excluded stores are still UP before appending STATE REMOVED. */
            bool all_up = true;
            for (int j = 0; j < chunk_be_nstores(t->be); j++)
                if (!chunk_be_store_excluded(t->be, j) && chunk_be_store_state(t->be, j) != CHUNK_STORE_UP)
                    all_up = false;
            if (!all_up) {
                park_down = true;
                break;
            }
            ecgc_kick_tier(t);
            rc = finish_shrink_removed(ctl, slot_idx);
            break;
        }

        if (ncand == 0) {
            free(items);
            if (any_damaged) {
                bool do_log = false;
                pthread_mutex_lock(&ctl->mu);
                if (!ctl->slots[slot_idx].shrink_err_logged) {
                    ctl->slots[slot_idx].shrink_err_logged = true;
                    do_log = true;
                }
                pthread_mutex_unlock(&ctl->mu);
                if (do_log)
                    fprintf(stderr, "tsdb: tier: shrink of store %d blocked by damaged .ec part in volume %u "
                                    "partition %" PRId64 "; run `tsdb %s tier salvage` to retire unreadable parts\n",
                            slot_idx, dmg_vol, dmg_part, t->dir ? t->dir : "DIR");
                rc = TSDB_ERR_CORRUPT;
            } else {
                retry_soon = true;
            }
            break;
        }

        /* Step 3 (O(N) worklist): re-encode every candidate collected in this pass, pinning only its single engine
         * through ctl->shrink_cur so unrelated volumes never wait and t->eng_active stays 0. */
        bool made_progress = false;
        for (size_t i = 0; i < nitems; i++) {
            if (!items[i].need)
                continue;
            if (atomic_load_explicit(&ctl->closing_atomic, memory_order_acquire) || ctl_yield_cb(ctl)) {
                rc = TSDB_ERR_STOPPED;
                break;
            }

            eng *db = ctl_shrink_pin_eng(ctl, items[i].vol);
            if (!db) {
                retry_soon = true;
                break;
            }

            part *cand_p = NULL;
            pthread_mutex_lock(&db->lock);
            for (size_t pi = 0; pi < db->nparts; pi++) {
                part *p = db->parts[pi];
                if (p->dropped || p->partition != items[i].partition || strcmp(p->name, items[i].name) != 0)
                    continue;
                struct ecpart *ep = atomic_load_explicit(&p->ec, memory_order_acquire);
                if (!ep || ep->test_mark || memcmp(ep->rec.oid, items[i].oid, 16) != 0)
                    continue;
                if (p->settling || p->merging || p->pending || atomic_load_explicit(&p->damaged, memory_order_acquire))
                    break;
                part_ref(p);
                cand_p = p;
                break;
            }
            pthread_mutex_unlock(&db->lock);

            int rrc = TSDB_ERR_NOTFOUND;
            if (cand_p) {
                size_t got = 0;
                rrc = eng_reencode(db, items[i].partition, &cand_p, 1, &got);
                part_unref(cand_p);
            }
            ctl_shrink_unpin_eng(ctl);

            if (rrc == 0) {
                made_progress = true;
                pthread_mutex_lock(&ctl->mu);
                ctl->prog.shrink_objects_done++;
                if (ctl->prog.shrink_objects_left > 0)
                    ctl->prog.shrink_objects_left--;
                pthread_mutex_unlock(&ctl->mu);
                ctl_pace_cb(&pacer, items[i].obj_len ? items[i].obj_len : 1);
                continue;
            }
            if (rrc == TSDB_ERR_NOTFOUND)
                continue; /* part dropped, busy, or damaged concurrently; avoid tight spin */
            if (rrc == TSDB_ERR_STOPPED) {
                rc = TSDB_ERR_STOPPED;
                break;
            }
            if (rrc == TSDB_ERR_IO || rrc == TSDB_ERR_CORRUPT) {
                bool do_log = false;
                pthread_mutex_lock(&ctl->mu);
                if (!ctl->slots[slot_idx].shrink_err_logged) {
                    ctl->slots[slot_idx].shrink_err_logged = true;
                    do_log = true;
                }
                pthread_mutex_unlock(&ctl->mu);
                if (do_log)
                    fprintf(stderr, "tsdb: tier: shrink of store %d failed re-encoding %s (volume %u partition "
                                    "%" PRId64 "): %s; if unrecoverable, run `tsdb %s tier salvage`\n",
                            slot_idx, items[i].name, items[i].vol, items[i].partition, tsdb_strerror(rrc),
                            t->dir ? t->dir : "DIR");
            }
            rc = rrc;
            if (rrc == TSDB_ERR_UNAVAILABLE) {
                for (int j = 0; j < chunk_be_nstores(t->be); j++)
                    if (!chunk_be_store_excluded(t->be, j) && !chunk_be_store_is_lost(t->be, j) &&
                        chunk_be_store_state(t->be, j) != CHUNK_STORE_UP)
                        park_down = true;
            }
            break;
        }
        free(items);

        if (rc != 0 || park_down || retry_soon)
            break;
        if (!made_progress) {
            /* Every candidate returned NOTFOUND transiently: back off briefly instead of spinning at 100% CPU. */
            retry_soon = true;
            break;
        }
    }

    pthread_mutex_lock(&ctl->mu);
    if (ctl->slots[slot_idx].needs_shrink) {
        ctl->prog.shrink_active = false;
        ctl->prog.shrink_store = -1;
        ctl->slots[slot_idx].bytes_done_saved = pacer.total_bytes;
        if (rc == TSDB_ERR_STOPPED) {
            ctl->slots[slot_idx].retry_after_ms = 0;
        } else if (park_down) {
            ctl->slots[slot_idx].wait_other_up = true;
            ctl->slots[slot_idx].fails = 0;
            ctl->slots[slot_idx].retry_after_ms = 0;
        } else if (retry_soon) {
            ctl->slots[slot_idx].retry_after_ms = ctl_mono_ms() + 200;
        } else if (rc != 0) {
            if (ctl->slots[slot_idx].fails < 6)
                ctl->slots[slot_idx].fails++;
            int64_t backoff = (int64_t)500 << (ctl->slots[slot_idx].fails - 1);
            if (backoff > 15000)
                backoff = 15000;
            ctl->slots[slot_idx].retry_after_ms = ctl_mono_ms() + backoff;
        }
    }
    pthread_cond_broadcast(&ctl->idle_cv);
    pthread_mutex_unlock(&ctl->mu);
}

/* ---------------------------------------------------------------- single tier worker thread */

static void ctl_wake_from_health(void *ctx)
{
    struct tierctl *ctl = ctx;
    if (!ctl)
        return;
    pthread_mutex_lock(&ctl->mu);
    if (!ctl->closing)
        pthread_cond_signal(&ctl->cv);
    pthread_mutex_unlock(&ctl->mu);
}

static void *worker_main(void *arg)
{
    struct tierctl *ctl = arg;
    tier *t = ctl->t;
#ifdef __linux__
    setpriority(PRIO_PROCESS, (id_t)syscall(SYS_gettid), 19); /* this thread only (D20) */
#endif
    int64_t next_stat_ms = ctl_mono_ms();
    pthread_mutex_lock(&ctl->mu);
    ctl->next_stat_ms = next_stat_ms;
    pthread_mutex_unlock(&ctl->mu);

    for (;;) {
        if (atomic_load_explicit(&ctl->closing_atomic, memory_order_acquire))
            break;

        /* 1. Off-thread eccache reopen requests from tier_store_set_down. */
        if (tier_health_take_reopen(t)) {
            eccache_reopen_all(t);
            continue;
        }

        /* 2. Periodic TIERS stat/re-read (every 1 s, or immediately when woken by in-process API). */
        int64_t now = ctl_mono_ms();
        bool do_stat = false, forced = false;
        if (t->from_tiers && t->dir) {
            pthread_mutex_lock(&ctl->mu);
            forced = ctl->check_tiers_now;
            if (forced || now >= next_stat_ms) {
                ctl->check_tiers_now = false;
                ctl->checking_tiers = true;
                do_stat = true;
            }
            pthread_mutex_unlock(&ctl->mu);
        } else {
            pthread_mutex_lock(&ctl->mu);
            ctl->check_tiers_now = false;
            pthread_mutex_unlock(&ctl->mu);
        }
        if (do_stat) {
            int pms = ctl_poll_ms();
            next_stat_ms = now + pms;
            pthread_mutex_lock(&ctl->mu);
            ctl->next_stat_ms = next_stat_ms;
            pthread_mutex_unlock(&ctl->mu);
            if (forced || tiers_file_changed_stat(ctl))
                apply_tiers_changes(ctl);
            pthread_mutex_lock(&ctl->mu);
            ctl->checking_tiers = false;
            pthread_cond_broadcast(&ctl->idle_cv);
            pthread_mutex_unlock(&ctl->mu);
        }

        if (atomic_load_explicit(&ctl->closing_atomic, memory_order_acquire))
            break;

        /* 3. Queued r15-health attaches (run ahead of / between rebuild objects so a transiently stalled store
         * recovers promptly and unblocks any parked rebuild). */
        int64_t h_wait = 500;
        ctl_pacer hpacer = {.ctl = ctl, .store = -1, .next_tiers_check_ms = ctl_mono_ms() + ctl_poll_ms()};
        chunk_heal_opts htpl = {
            .cancel = ctl_cancel_cb,
            .cancel_ctx = ctl,
            .pace = ctl_pace_cb,
            .pace_ctx = &hpacer,
        };
        if (tier_health_run_one(t, &htpl, &h_wait)) {
            /* A store may have just come UP: unpark any rebuild/shrink waiting on another transiently DOWN store. */
            pthread_mutex_lock(&ctl->mu);
            for (int i = 0; i < ctl->nslots; i++)
                if ((ctl->slots[i].needs_rebuild || ctl->slots[i].needs_shrink) && ctl->slots[i].wait_other_up) {
                    ctl->slots[i].wait_other_up = false;
                    ctl->slots[i].retry_after_ms = 0;
                }
            pthread_mutex_unlock(&ctl->mu);
            continue;
        }

        /* 4. Pending replacement rebuilds and shrinks (yield cleanly between objects for TIERS & health attaches). */
        int pick_rb = -1, pick_sh = -1;
        int64_t wait_ms = t->from_tiers ? ctl_poll_ms() : 500;
        if (h_wait < wait_ms)
            wait_ms = h_wait;
        now = ctl_mono_ms();
        if (t->from_tiers && next_stat_ms > now && next_stat_ms - now < wait_ms)
            wait_ms = next_stat_ms - now;
        else if (t->from_tiers && next_stat_ms <= now)
            wait_ms = 0;

        bool any_rb_or_sh = false;
        pthread_mutex_lock(&ctl->mu);
        for (int i = 0; i < ctl->nslots; i++) {
            if (ctl->slots[i].needs_rebuild || ctl->slots[i].needs_shrink)
                any_rb_or_sh = true;
            if (!ctl->slots[i].needs_rebuild || ctl->slots[i].wait_other_up)
                continue;
            if (now >= ctl->slots[i].retry_after_ms) {
                pick_rb = i;
                break;
            }
            int64_t rem = ctl->slots[i].retry_after_ms - now;
            if (rem < wait_ms)
                wait_ms = rem;
        }
        if (pick_rb < 0) {
            for (int i = 0; i < ctl->nslots; i++) {
                if (!ctl->slots[i].needs_shrink || ctl->slots[i].wait_other_up)
                    continue;
                if (now >= ctl->slots[i].retry_after_ms) {
                    pick_sh = i;
                    break;
                }
                int64_t rem = ctl->slots[i].retry_after_ms - now;
                if (rem < wait_ms)
                    wait_ms = rem;
            }
        }
        pthread_mutex_unlock(&ctl->mu);

        if (pick_rb >= 0) {
            tierscrub_set_paused(t, true);
            run_rebuild(ctl, pick_rb);
            continue;
        }
        if (pick_sh >= 0) {
            tierscrub_set_paused(t, true);
            run_shrink(ctl, pick_sh);
            continue;
        }

        /* 5. Background chunk scrub (R15 r15-scrub): runs only when no rebuild, shrink, or attach is active/pending. */
        if (!any_rb_or_sh && !tier_health_busy(t) && !tier_health_has_pending(t)) {
            if (tierscrub_step(t, &wait_ms))
                continue;
        } else {
            tierscrub_set_paused(t, true);
        }

        /* 6. Sleep until next poll, retry, or wakeup. */
        pthread_mutex_lock(&ctl->mu);
        if (!ctl->closing && !ctl->check_tiers_now && wait_ms > 0)
            tsdb_cond_timedwait_ms(&ctl->cv, &ctl->mu, (uint64_t)wait_ms);
        pthread_mutex_unlock(&ctl->mu);
    }

    pthread_mutex_lock(&ctl->mu);
    bool drop_ref = ctl->worker_owns_ref;
    ctl->worker_done = true;
    pthread_cond_broadcast(&ctl->cv);
    pthread_cond_broadcast(&ctl->idle_cv);
    pthread_mutex_unlock(&ctl->mu);
    if (drop_ref)
        tier_put(t);
    return NULL;
}

/* ---------------------------------------------------------------- lifecycle & status API */

int tierctl_open(tier *t)
{
    if (!t || !t->be)
        return 0;
    struct tierctl *ctl = calloc(1, sizeof *ctl);
    if (!ctl)
        return TSDB_ERR_NOMEM;
    ctl->t = t;
    ctl->prog.store = -1;
    ctl->prog.shrink_store = -1;
    pthread_mutex_init(&ctl->mu, NULL);
    tsdb_cond_init_mono(&ctl->cv);
    tsdb_cond_init_mono(&ctl->idle_cv);
    pthread_cond_init(&ctl->shrink_cv, NULL);
    atomic_init(&ctl->closing_atomic, false);

    uint32_t rb_mb = TIERS_REBUILD_MB_S_DEFAULT;
    uint32_t sc_mb = TIERS_SCRUB_MB_S_DEFAULT;
    uint32_t sc_days = TIERS_SCRUB_DAYS_DEFAULT;
    if (t->from_tiers) {
        if (t->tview.global.rebuild_mb_s)
            rb_mb = t->tview.global.rebuild_mb_s;
        if (t->tview.global.scrub_mb_s)
            sc_mb = t->tview.global.scrub_mb_s;
        sc_days = t->tview.global.scrub_days;

        int n = (int)t->tview.nstores;
        if (n > TIERS_STORES_MAX)
            n = TIERS_STORES_MAX;
        ctl->nslots = n;
        for (int i = 0; i < n; i++) {
            const tiers_store *ts = &t->tview.stores[i];
            const uint8_t *cur_id = ts->state == TIERS_STATE_REPLACED_BY ? ts->replaced_by : ts->id;
            memcpy(ctl->slots[i].id, cur_id, 16);
            ctl->slots[i].state = ts->state;
            ctl->slots[i].be_idx = t->tiers_to_be[i];
            ctl->slots[i].path = ts->path ? strdup(ts->path) : NULL;
            if (ts->path && !ctl->slots[i].path) {
                atomic_store_explicit(&t->ctl, ctl, memory_order_release);
                tierctl_close(t);
                return TSDB_ERR_NOMEM;
            }
            if (ctl->slots[i].be_idx >= 0 && chunk_be_store_state(t->be, ctl->slots[i].be_idx) == CHUNK_STORE_REPLACING)
                ctl->slots[i].needs_rebuild = true;
            if (ts->state == TIERS_STATE_SHRINKING)
                ctl->slots[i].needs_shrink = true;
        }
        ctl->tiers_file_end = t->tview.file_end;
        if (t->dir) {
            char tp[PATH_MAX + 16];
            snprintf(tp, sizeof tp, "%s/%s", t->dir, TIERS_FILE);
            if (stat(tp, &ctl->tiers_st) == 0)
                ctl->have_tiers_st = true;
        }
    }
    atomic_init(&ctl->rebuild_mb_s, rb_mb);
    atomic_init(&ctl->scrub_mb_s, sc_mb);
    atomic_init(&ctl->scrub_days, sc_days);

    atomic_store_explicit(&t->ctl, ctl, memory_order_release);
    if (tierscrub_open(t) != 0) {
        tierctl_close(t);
        return TSDB_ERR_NOMEM;
    }
    tier_health_set_wake(t, ctl_wake_from_health, ctl);

    if (tsdb_thread_create(&ctl->worker_th, worker_main, ctl) != 0) {
        tier_health_set_wake(t, NULL, NULL);
        tierctl_close(t);
        return TSDB_ERR_NOMEM;
    }
    ctl->worker_started = true;
    return 0;
}

void tierctl_eng_stop(eng *db)
{
    if (!db || !db->tier)
        return;
    struct tierctl *ctl = atomic_load_explicit(&db->tier->ctl, memory_order_acquire);
    if (!ctl)
        return;
    pthread_mutex_lock(&ctl->mu);
    while (ctl->shrink_cur == db)
        pthread_cond_wait(&ctl->shrink_cv, &ctl->mu);
    pthread_mutex_unlock(&ctl->mu);
}

void tierctl_quiesce(tier *t)
{
    if (!t)
        return;
    struct tierctl *ctl = atomic_load_explicit(&t->ctl, memory_order_acquire);
    if (!ctl)
        return;
    tier_health_set_wake(t, NULL, NULL);
    pthread_mutex_lock(&ctl->mu);
    if (ctl->quiesced) {
        pthread_mutex_unlock(&ctl->mu);
        return;
    }
    ctl->closing = true;
    atomic_store_explicit(&ctl->closing_atomic, true, memory_order_release);
    ctl->quiesced = true;
    pthread_cond_broadcast(&ctl->cv);
    pthread_cond_broadcast(&ctl->idle_cv);
    pthread_mutex_unlock(&ctl->mu);
    tierscrub_quiesce(t);

    struct timespec dl;
    tsdb_cond_deadline_ms(&dl, 2000);
    bool do_join = false, do_detach = false;
    pthread_mutex_lock(&ctl->mu);
    if (ctl->worker_started) {
        while (!ctl->worker_done) {
            if (pthread_cond_timedwait(&ctl->cv, &ctl->mu, &dl) != 0)
                break;
        }
        if (ctl->worker_done) {
            do_join = true;
        } else {
            do_detach = true;
            if (tier_try_get(t) != NULL)
                ctl->worker_owns_ref = true;
            else
                ctl->worker_stuck = true;
        }
        ctl->worker_started = false;
    }
    pthread_mutex_unlock(&ctl->mu);
    if (do_join)
        pthread_join(ctl->worker_th, NULL);
    else if (do_detach)
        pthread_detach(ctl->worker_th);
}

bool tierctl_close(tier *t)
{
    if (!t)
        return false;
    tierctl_quiesce(t);
    struct tierctl *ctl = atomic_load_explicit(&t->ctl, memory_order_acquire);
    if (!ctl)
        return false;
    if (ctl->worker_stuck)
        return true;
    tierscrub_close(t);
    atomic_store_explicit(&t->ctl, NULL, memory_order_release);
    for (int i = 0; i < ctl->nslots; i++)
        free(ctl->slots[i].path);
    pthread_cond_destroy(&ctl->shrink_cv);
    pthread_cond_destroy(&ctl->idle_cv);
    pthread_cond_destroy(&ctl->cv);
    pthread_mutex_destroy(&ctl->mu);
    free(ctl);
    return false;
}

void tierctl_wake(tier *t)
{
    if (!t)
        return;
    struct tierctl *ctl = atomic_load_explicit(&t->ctl, memory_order_acquire);
    if (!ctl)
        return;
    pthread_mutex_lock(&ctl->mu);
    if (!ctl->closing) {
        if (t->from_tiers && t->dir)
            ctl->check_tiers_now = true;
        pthread_cond_signal(&ctl->cv);
    }
    pthread_mutex_unlock(&ctl->mu);
}

bool tierctl_get_progress(const tier *t, tierctl_progress *out)
{
    if (!t || !out)
        return false;
    memset(out, 0, sizeof *out);
    out->store = -1;
    out->shrink_store = -1;
    struct tierctl *ctl = atomic_load_explicit(&((tier *)t)->ctl, memory_order_acquire);
    if (!ctl)
        return false;
    pthread_mutex_lock(&ctl->mu);
    *out = ctl->prog;
    if (out->rate_mb_s == 0)
        out->rate_mb_s = atomic_load_explicit(&ctl->rebuild_mb_s, memory_order_relaxed);
    pthread_mutex_unlock(&ctl->mu);
    return true;
}

int tierctl_wait_idle(tier *t, int timeout_ms)
{
    if (!t)
        return 0;
    struct tierctl *ctl = atomic_load_explicit(&t->ctl, memory_order_acquire);
    if (!ctl)
        return 0;
    struct timespec dl;
    tsdb_cond_deadline_ms(&dl, timeout_ms > 0 ? (uint64_t)timeout_ms : 10000);
    pthread_mutex_lock(&ctl->mu);
    for (;;) {
        bool busy = ctl->check_tiers_now || ctl->checking_tiers || ctl->prog.active || ctl->prog.shrink_active;
        for (int i = 0; !busy && i < ctl->nslots; i++)
            if ((ctl->slots[i].needs_rebuild || ctl->slots[i].needs_shrink) && !ctl->slots[i].wait_other_up)
                busy = true;
        if (!busy || ctl->closing) {
            pthread_mutex_unlock(&ctl->mu);
            return 0;
        }
        if (pthread_cond_timedwait(&ctl->idle_cv, &ctl->mu, &dl) == ETIMEDOUT) {
            pthread_mutex_unlock(&ctl->mu);
            return TSDB_ERR_UNAVAILABLE;
        }
    }
}

bool tierctl_is_closing(const tier *t)
{
    if (!t)
        return true;
    struct tierctl *ctl = atomic_load_explicit(&((tier *)t)->ctl, memory_order_acquire);
    return !ctl || atomic_load_explicit(&ctl->closing_atomic, memory_order_acquire);
}

bool tierctl_scrub_should_yield(tier *t)
{
    if (!t)
        return true;
    struct tierctl *ctl = atomic_load_explicit(&t->ctl, memory_order_acquire);
    if (!ctl || atomic_load_explicit(&ctl->closing_atomic, memory_order_acquire))
        return true;
    if (tier_health_has_pending(t) || tier_health_busy(t))
        return true;
    bool busy = false;
    int64_t due = 0;
    pthread_mutex_lock(&ctl->mu);
    if (ctl->closing || ctl->check_tiers_now || ctl->checking_tiers || ctl->prog.active || ctl->prog.shrink_active)
        busy = true;
    for (int i = 0; !busy && i < ctl->nslots; i++)
        if (ctl->slots[i].needs_rebuild || ctl->slots[i].needs_shrink)
            busy = true;
    due = ctl->next_stat_ms;
    pthread_mutex_unlock(&ctl->mu);
    if (busy)
        return true;
    if (t->from_tiers && ctl_mono_ms() >= due) {
        if (tiers_file_changed_stat(ctl)) {
            pthread_mutex_lock(&ctl->mu);
            ctl->check_tiers_now = true;
            pthread_mutex_unlock(&ctl->mu);
            return true;
        }
        pthread_mutex_lock(&ctl->mu);
        ctl->next_stat_ms = ctl_mono_ms() + ctl_poll_ms();
        pthread_mutex_unlock(&ctl->mu);
    }
    return false;
}

bool tierctl_wait_interruptible_ms(tier *t, uint64_t wait_ms)
{
    if (!t)
        return false;
    struct tierctl *ctl = atomic_load_explicit(&t->ctl, memory_order_acquire);
    if (!ctl)
        return false;
    pthread_mutex_lock(&ctl->mu);
    if (!ctl->closing && !ctl->check_tiers_now && wait_ms > 0)
        (void)tsdb_cond_timedwait_ms(&ctl->cv, &ctl->mu, wait_ms);
    bool ok = !ctl->closing;
    pthread_mutex_unlock(&ctl->mu);
    return ok;
}

uint32_t tierctl_scrub_mb_s(const tier *t)
{
    if (!t)
        return TIERS_SCRUB_MB_S_DEFAULT;
    struct tierctl *ctl = atomic_load_explicit(&((tier *)t)->ctl, memory_order_acquire);
    if (!ctl)
        return TIERS_SCRUB_MB_S_DEFAULT;
    uint32_t v = atomic_load_explicit(&ctl->scrub_mb_s, memory_order_relaxed);
    return v ? v : TIERS_SCRUB_MB_S_DEFAULT;
}

uint32_t tierctl_scrub_days(const tier *t)
{
    if (!t)
        return TIERS_SCRUB_DAYS_DEFAULT;
    struct tierctl *ctl = atomic_load_explicit(&((tier *)t)->ctl, memory_order_acquire);
    if (!ctl)
        return TIERS_SCRUB_DAYS_DEFAULT;
    return atomic_load_explicit(&ctl->scrub_days, memory_order_relaxed);
}

/* ---------------------------------------------------------------- tsdb_tier_lost / tierctl_lost_impl */

static int read_fd_all(int fd, size_t max_bytes, char **out, size_t *len_out)
{
    *out = NULL;
    *len_out = 0;
    struct stat st;
    if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size < 0 || (uint64_t)st.st_size > max_bytes)
        return TSDB_ERR_IO;
    size_t sz = (size_t)st.st_size;
    char *buf = malloc(sz + 1);
    if (!buf)
        return TSDB_ERR_NOMEM;
    size_t got = 0;
    while (got < sz) {
        ssize_t r = pread(fd, buf + got, sz - got, (off_t)got);
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0) {
            free(buf);
            return TSDB_ERR_IO;
        }
        got += (size_t)r;
    }
    buf[sz] = '\0';
    *out = buf;
    *len_out = sz;
    return 0;
}

static bool ctl_path_within(const char *a, const char *b)
{
    size_t bl = strlen(b);
    if (bl == 1 && b[0] == '/')
        return a[0] == '/';
    return strncmp(a, b, bl) == 0 && (a[bl] == '\0' || a[bl] == '/');
}

static void ctl_path_norm(const char *p, char out[PATH_MAX])
{
    size_t n = strlen(p);
    if (n >= PATH_MAX)
        n = PATH_MAX - 1;
    memcpy(out, p, n);
    out[n] = '\0';
    while (n > 1 && out[n - 1] == '/')
        out[--n] = '\0';
}

static int ctl_same_dir(const char *a, const char *b)
{
    struct stat sa, sb;
    if (stat(a, &sa) != 0 || stat(b, &sb) != 0)
        return 0;
    return sa.st_dev == sb.st_dev && sa.st_ino == sb.st_ino ? 1 : 0;
}

static int ctl_sync_parent(const char *path)
{
    char par[PATH_MAX];
    snprintf(par, sizeof par, "%s", path);
    char *sl = strrchr(par, '/');
    if (!sl)
        return 0;
    if (sl == par)
        par[1] = '\0';
    else
        *sl = '\0';
    int fd = open(par, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0)
        return TSDB_ERR_IO;
    int rc = tsdb_fsync(fd) == 0 ? 0 : TSDB_ERR_IO;
    close(fd);
    return rc;
}

static int ctl_sync_adopted(const char *canon)
{
    int dfd = open(canon, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd < 0)
        return TSDB_ERR_IO;
    int mfd = openat(dfd, "META", O_RDONLY | O_CLOEXEC);
    int rc = 0;
    if (mfd < 0 || tsdb_fsync(mfd) != 0)
        rc = TSDB_ERR_IO;
    if (mfd >= 0)
        close(mfd);
    if (rc == 0 && tsdb_fsync(dfd) != 0)
        rc = TSDB_ERR_IO;
    close(dfd);
    return rc;
}

static long ctl_meta_volume(const char *txt)
{
    for (const char *l = txt; *l;) {
        if (strncmp(l, "volume=", 7) == 0)
            return strtol(l + 7, NULL, 10);
        const char *nl = strchr(l, '\n');
        l = nl ? nl + 1 : l + strlen(l);
    }
    return 0;
}

static int ctl_check_root_meta(int rootfd, const char *root)
{
    int fd = openat(rootfd, "META", O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    char *mtxt = NULL;
    size_t mlen = 0;
    int rc = read_fd_all(fd, 1u << 20, &mtxt, &mlen);
    close(fd);
    if (rc != 0)
        return rc;
    char *vl = strstr(mtxt, "version="), unknown[96];
    unsigned feats = 0;
    meta_requires_parse(mtxt, &feats, unknown, sizeof unknown);
    long vnum = ctl_meta_volume(mtxt);
    bool has_v8 = false;
    for (const char *l = mtxt; *l && !has_v8;) {
        if (strncmp(l, "parts_v8=", 9) == 0)
            has_v8 = true;
        const char *nl = strchr(l, '\n');
        l = nl ? nl + 1 : l + strlen(l);
    }
    if ((vl ? atoi(vl + 8) : 0) != FORMAT_VERSION || unknown[0] ||
        (feats & (META_REQ_CLUSTER | META_REQ_REPLICA)) || vnum != 0) {
        fprintf(stderr, "tsdb: tier lost: %s/META is not a local format %d database root: refused\n", root,
                FORMAT_VERSION);
        free(mtxt);
        return TSDB_ERR_INVALID;
    }
    if (has_v8) {
        fprintf(stderr, "tsdb: tier lost: %s/META still records format-8 parts (parts_v8=); run `tsdb %s optimize` "
                        "first: refused\n", root, root);
        free(mtxt);
        return TSDB_ERR_INVALID;
    }
    free(mtxt);
    return 0;
}

static void ctl_free_paths(char **paths, size_t n)
{
    for (size_t i = 0; i < n; i++)
        free(paths[i]);
    free(paths);
}

static int ctl_read_volumes(int vfd, char ***paths_out, size_t *n_out)
{
    *paths_out = NULL;
    *n_out = 0;
    char *txt = NULL;
    size_t len = 0;
    int rc = read_fd_all(vfd, 16u << 20, &txt, &len);
    if (rc != 0)
        return rc;
    char **paths = NULL;
    size_t n = 0, cap = 0;
    for (char *line = txt; line < txt + len;) {
        char *nl = memchr(line, '\n', len - (size_t)(line - txt));
        if (!nl)
            break;
        *nl = '\0';
        size_t ll = (size_t)(nl - line);
        if (ll < 10 || line[8] != ' ' || line[9] != '/') {
            rc = TSDB_ERR_CORRUPT;
            break;
        }
        if (n == cap) {
            cap = cap ? cap * 2 : 8;
            char **np = realloc(paths, cap * sizeof *np);
            if (!np) {
                rc = TSDB_ERR_NOMEM;
                break;
            }
            paths = np;
        }
        paths[n] = strdup(line + 9);
        if (!paths[n]) {
            rc = TSDB_ERR_NOMEM;
            break;
        }
        n++;
        line = nl + 1;
    }
    free(txt);
    if (rc != 0) {
        ctl_free_paths(paths, n);
        return rc;
    }
    *paths_out = paths;
    *n_out = n;
    return 0;
}

/* Directory check for replacement store (matching r15-tiers' tier_dir_check, plus allowing REBUILDING on adopt). */
static int ctl_dir_check(const char *canon, const uint8_t db_id[16], const tiers_view *tv, bool *adopt_out,
                         uint8_t sid_out[16])
{
    *adopt_out = false;
    int fd = open(canon, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0)
        return TSDB_ERR_IO;
    DIR *d = fdopendir(fd);
    if (!d) {
        close(fd);
        return TSDB_ERR_IO;
    }
    bool has_meta = false;
    char other[256] = "";
    struct dirent *e;
    errno = 0;
    while ((e = readdir(d)) != NULL) {
        if (strcmp(e->d_name, ".") == 0 || strcmp(e->d_name, "..") == 0)
            continue;
        if (strcmp(e->d_name, "META") == 0) {
            has_meta = true;
        } else if (strcmp(e->d_name, "META.tmp") == 0 || strcmp(e->d_name, TIERCTL_REBUILDING_FILE) == 0 ||
                   strcmp(e->d_name, TIERCTL_REBUILDING_FILE ".tmp") == 0) {
            /* crashed init/rebuilding temporary; cleaned or adopted below */
        } else if (!other[0]) {
            snprintf(other, sizeof other, "%s", e->d_name);
        }
    }
    int rerr = errno;
    closedir(d);
    if (rerr)
        return TSDB_ERR_IO;

    if (has_meta) {
        uint8_t exist_sid[16];
        chunk_store_owner ow = {0};
        int mrc = chunk_store_meta_read(canon, exist_sid, &ow);
        if (mrc == 0 && ow.db_set) {
            char have_hex[33];
            chunk_oid_hex(ow.db_id, have_hex);
            if (memcmp(ow.db_id, db_id, 16) != 0) {
                char want_hex[33];
                chunk_oid_hex(db_id, want_hex);
                fprintf(stderr, "tsdb: tier lost --replace %s: store META belongs to database %s, not %s: refused\n",
                        canon, have_hex, want_hex);
                return TSDB_ERR_INVALID;
            }
            bool known = false;
            for (size_t i = 0; i < tv->nseen_ids; i++)
                if (memcmp(tv->seen_ids[i], exist_sid, 16) == 0)
                    known = true;
            if (known) {
                fprintf(stderr, "tsdb: tier lost --replace %s: store is already recorded in TIERS: refused\n", canon);
                return TSDB_ERR_INVALID;
            }
            if (other[0]) {
                fprintf(stderr, "tsdb: tier lost --replace %s: directory is not empty (found %s): refused\n", canon,
                        other);
                return TSDB_ERR_INVALID;
            }
            memcpy(sid_out, exist_sid, 16);
            *adopt_out = true;
            return 0;
        }
        if (mrc == 0) {
            fprintf(stderr, "tsdb: tier lost --replace %s: directory already holds an unbound/cluster store META: "
                            "refused\n", canon);
            return TSDB_ERR_INVALID;
        }
        fprintf(stderr, "tsdb: tier lost --replace %s: directory is not empty (found META): refused\n", canon);
        return TSDB_ERR_INVALID;
    }
    if (other[0]) {
        fprintf(stderr, "tsdb: tier lost --replace %s: directory is not empty (found %s): refused\n", canon, other);
        return TSDB_ERR_INVALID;
    }
    return 0;
}

static bool parse_hex16(const char *s, uint8_t out[16])
{
    if (!s || strlen(s) != 32)
        return false;
    for (int i = 0; i < 16; i++) {
        int a = s[2 * i] >= '0' && s[2 * i] <= '9'       ? s[2 * i] - '0'
              : s[2 * i] >= 'a' && s[2 * i] <= 'f'       ? s[2 * i] - 'a' + 10
              : s[2 * i] >= 'A' && s[2 * i] <= 'F'       ? s[2 * i] - 'A' + 10
                                                         : -1;
        int b = s[2 * i + 1] >= '0' && s[2 * i + 1] <= '9' ? s[2 * i + 1] - '0'
              : s[2 * i + 1] >= 'a' && s[2 * i + 1] <= 'f' ? s[2 * i + 1] - 'a' + 10
              : s[2 * i + 1] >= 'A' && s[2 * i + 1] <= 'F' ? s[2 * i + 1] - 'A' + 10
                                                           : -1;
        if (a < 0 || b < 0)
            return false;
        out[i] = (uint8_t)((a << 4) | b);
    }
    return true;
}

/* Resolves STORE specification (32-hex id, index "N" or "sN", or store path) into a slot index in tv. */
static int resolve_store(const tiers_view *tv, const char *spec, size_t *idx_out)
{
    if (!spec || !*spec)
        return TSDB_ERR_INVALID;
    uint8_t hid[16];
    if (parse_hex16(spec, hid)) {
        for (size_t i = 0; i < tv->nstores; i++) {
            const tiers_store *st = &tv->stores[i];
            const uint8_t *cur = st->state == TIERS_STATE_REPLACED_BY ? st->replaced_by : st->id;
            if (memcmp(cur, hid, 16) == 0 || (st->state == TIERS_STATE_LOST && memcmp(st->id, hid, 16) == 0)) {
                *idx_out = i;
                return 0;
            }
        }
        return TSDB_ERR_NOTFOUND;
    }
    if (spec[0] == '/') {
        char norm[PATH_MAX], canon[PATH_MAX];
        ctl_path_norm(spec, norm);
        /* Match recorded paths directly before calling realpath(): a lost NFS mount can hang in realpath(). */
        for (size_t i = 0; i < tv->nstores; i++) {
            if (strcmp(tv->stores[i].path, spec) == 0 || strcmp(tv->stores[i].path, norm) == 0) {
                *idx_out = i;
                return 0;
            }
        }
        const char *rp = realpath(spec, canon);
        if (rp) {
            for (size_t i = 0; i < tv->nstores; i++) {
                if (strcmp(tv->stores[i].path, rp) == 0) {
                    *idx_out = i;
                    return 0;
                }
            }
        }
        return TSDB_ERR_NOTFOUND;
    }
    const char *num = (spec[0] == 's' || spec[0] == 'S') ? spec + 1 : spec;
    if (!isdigit((unsigned char)num[0]))
        return TSDB_ERR_INVALID;
    char *end = NULL;
    errno = 0;
    unsigned long v = strtoul(num, &end, 10);
    if (errno != 0 || !end || *end != '\0' || v >= tv->nstores)
        return TSDB_ERR_NOTFOUND;
    *idx_out = (size_t)v;
    return 0;
}

static int tierctl_shrink_impl(int rootfd, const char *root, bool sync, const char *store, uint32_t flags, int *slot_out)
{
    if (flags & ~TSDB_TIER_ALLOW_SHARED_DEV)
        return TSDB_ERR_INVALID;
    int rc = ctl_check_root_meta(rootfd, root);
    if (rc != 0)
        return rc;

    bool vol_existed = faccessat(rootfd, "VOLUMES", F_OK, AT_SYMLINK_NOFOLLOW) == 0;
    int vfd = openat(rootfd, "VOLUMES", O_RDWR | O_APPEND | O_CREAT | O_CLOEXEC, 0644);
    if (vfd < 0)
        return TSDB_ERR_IO;
    tiers_view tv;
    tiers_view_init(&tv);

    rc = ctl_ofd_lock(vfd, F_WRLCK);
    if (rc == 0)
        rc = tiers_load(rootfd, true, sync, &tv);
    bool has_db_id = false;
    uint8_t db_id[16];
    memset(db_id, 0, sizeof db_id);
    if (rc == 0) {
        if (tv.nstores == 0) {
            fprintf(stderr, "tsdb: tier lost --shrink: %s has no tier stores\n", root);
            rc = TSDB_ERR_INVALID;
        } else {
            rc = tiers_db_id_read(rootfd, &has_db_id, db_id);
            if (rc == 0 && !has_db_id) {
                fprintf(stderr, "tsdb: tier lost --shrink: %s/%s lists %zu store(s) but root META has no db_id=: "
                                "refused\n", root, TIERS_FILE, tv.nstores);
                rc = TSDB_ERR_CORRUPT;
            }
        }
    }

    size_t tidx = 0;
    if (rc == 0) {
        rc = resolve_store(&tv, store, &tidx);
        if (rc != 0)
            fprintf(stderr, "tsdb: tier lost --shrink: store '%s' not found in %s (%zu store(s))\n", store, TIERS_FILE,
                    tv.nstores);
    }

    bool already_shrinking = false;
    if (rc == 0) {
        const tiers_store *target = &tv.stores[tidx];
        if (target->state == TIERS_STATE_REMOVED) {
            fprintf(stderr, "tsdb: tier lost %s --shrink: store %zu is already REMOVED: refused\n", store, tidx);
            rc = TSDB_ERR_INVALID;
        } else if (target->state == TIERS_STATE_SHRINKING) {
            already_shrinking = true;
        }
    }

    bool other_lost = false;
    if (rc == 0 && !already_shrinking) {
        size_t remaining = 0, n_elig_cur = 0;
        bool all_sc_cur = true, all_sc_rem = true;
        for (size_t i = 0; i < tv.nstores; i++) {
            uint8_t st = tv.stores[i].state;
            if (st != TIERS_STATE_REMOVED && st != TIERS_STATE_SHRINKING) {
                n_elig_cur++;
                if (!tv.stores[i].single_copy)
                    all_sc_cur = false;
            }
            if (i != tidx && st == TIERS_STATE_LOST)
                other_lost = true;
            if (i != tidx && tiers_state_is_active(st)) {
                remaining++;
                if (!tv.stores[i].single_copy)
                    all_sc_rem = false;
            }
        }
        if (n_elig_cur == 0)
            all_sc_cur = false;
        if (remaining == 0)
            all_sc_rem = false;

        int gk = 0, gm = 0;
        bool have_geom = false;
        if (tv.global.set_mask & TSDB_TIER_SET_REDUNDANCY) {
            gk = (int)tv.global.k;
            gm = (int)tv.global.m;
            have_geom = true;
        } else {
            have_geom = tier_default_geom((int)n_elig_cur, all_sc_cur, &gk, &gm);
        }
        if (remaining == 0 || (!all_sc_rem && remaining < 2)) {
            fprintf(stderr, "tsdb: tier lost %s --shrink: %zu active store(s) would remain, too few for a tier; "
                            "use --replace instead: refused\n", store, remaining);
            rc = TSDB_ERR_INVALID;
        } else if (have_geom && remaining < (size_t)(gk + gm)) {
            fprintf(stderr, "tsdb: tier lost %s --shrink: %zu active store(s) would remain, fewer than k + m = %d "
                            "(%d+%d); set a smaller redundancy with `tier set --redundancy` first: refused\n",
                    store, remaining, gk + gm, gk, gm);
            rc = TSDB_ERR_INVALID;
        }
        for (size_t v = 0; rc == 0 && v < tv.nvols; v++) {
            const tiers_settings *vs = &tv.vols[v].s;
            if (!(vs->set_mask & TSDB_TIER_SET_REDUNDANCY))
                continue;
            if (remaining < (size_t)vs->k + (size_t)vs->m) {
                fprintf(stderr, "tsdb: tier lost %s --shrink: volume %u redundancy %u+%u needs %u store(s), only %zu "
                                "active store(s) would remain; set a smaller redundancy with `tier set --volume %u "
                                "--redundancy` first: refused\n",
                        store, tv.vols[v].volume, (unsigned)vs->k, (unsigned)vs->m, (unsigned)(vs->k + vs->m),
                        remaining, tv.vols[v].volume);
                rc = TSDB_ERR_INVALID;
            }
        }
    }

    if (rc == 0 && !already_shrinking) {
        const tiers_store *target = &tv.stores[tidx];
        uint8_t lost_id[16];
        if (target->state == TIERS_STATE_REPLACED_BY)
            memcpy(lost_id, target->replaced_by, 16);
        else
            memcpy(lost_id, target->id, 16);
        if (target->state != TIERS_STATE_LOST)
            rc = tiers_append_state(rootfd, sync, lost_id, TIERS_STATE_LOST, 0, false, NULL, NULL);
        if (rc == 0)
            rc = tiers_append_state(rootfd, sync, lost_id, TIERS_STATE_SHRINKING, 0, false, NULL, NULL);
        if (rc == 0 && !vol_existed && sync && tsdb_fsync(rootfd) != 0)
            rc = TSDB_ERR_IO;
        if (rc == 0 && other_lost)
            fprintf(stderr, "tsdb: tier lost %s --shrink: warning: another store is LOST; shrink will wait until "
                            "that store is replaced or shrunk too\n",
                    store);
    }

    if (rc == 0 && slot_out)
        *slot_out = (int)tidx;

    tiers_view_free(&tv);
    close(vfd);
    return rc;
}

int tierctl_lost_impl(int rootfd, const char *root, bool sync, const char *store, const char *replace_path,
                      const char *opt_class, uint32_t flags, uint8_t new_id_out[16], int *slot_out)
{
    if (!root || !store || !*store)
        return TSDB_ERR_INVALID;
    if (!replace_path) {
        if (opt_class)
            return TSDB_ERR_INVALID;
        return tierctl_shrink_impl(rootfd, root, sync, store, flags, slot_out);
    }
    if (!*replace_path)
        return TSDB_ERR_INVALID;
    if (flags & ~(TSDB_TIER_SINGLE_COPY | TSDB_TIER_ALLOW_SHARED_DEV))
        return TSDB_ERR_INVALID;
    uint8_t opt_cls = TSDB_TIER_CLASS_UNKNOWN;
    if (opt_class && tiers_class_parse(opt_class, &opt_cls) != 0) {
        fprintf(stderr, "tsdb: tier lost: unknown class '%s' (expected nvme, ssd, hdd or unknown)\n", opt_class);
        return TSDB_ERR_INVALID;
    }
    if (tiers_path_valid(replace_path) != 0) {
        fprintf(stderr, "tsdb: tier lost --replace %s: store path must be absolute without '.' or '..'\n", replace_path);
        return TSDB_ERR_INVALID;
    }
    int rc = ctl_check_root_meta(rootfd, root);
    if (rc != 0)
        return rc;

    char norm[PATH_MAX];
    ctl_path_norm(replace_path, norm);
    if (ctl_path_within(norm, root) || ctl_path_within(root, norm)) {
        fprintf(stderr, "tsdb: tier lost --replace %s: overlaps database root %s: refused\n", norm, root);
        return TSDB_ERR_INVALID;
    }

    char canon[PATH_MAX];
    bool made = mkdir(replace_path, 0755) == 0;
    if (!made && errno != EEXIST)
        return TSDB_ERR_IO;
    if (!realpath(replace_path, canon)) {
        rc = TSDB_ERR_IO;
        goto fail_mkdir;
    }
    if (tiers_path_valid(canon) != 0) {
        rc = TSDB_ERR_INVALID;
        goto fail_mkdir;
    }
    struct stat st_new;
    if (stat(canon, &st_new) != 0) {
        rc = TSDB_ERR_IO;
        goto fail_mkdir;
    }
    if (!S_ISDIR(st_new.st_mode)) {
        fprintf(stderr, "tsdb: tier lost --replace %s: not a directory: refused\n", canon);
        rc = TSDB_ERR_INVALID;
        goto fail_mkdir;
    }

    int vfd = openat(rootfd, "VOLUMES", O_RDWR | O_APPEND | O_CREAT | O_CLOEXEC, 0644);
    if (vfd < 0) {
        rc = TSDB_ERR_IO;
        goto fail_mkdir;
    }
    char **vpaths = NULL;
    size_t nv = 0;
    tiers_view tv;
    tiers_view_init(&tv);

    rc = ctl_ofd_lock(vfd, F_WRLCK);
    if (rc == 0)
        rc = ctl_read_volumes(vfd, &vpaths, &nv);
    if (rc == 0)
        rc = tiers_load(rootfd, true, sync, &tv);
    bool has_db_id = false;
    uint8_t db_id[16];
    memset(db_id, 0, sizeof db_id);
    if (rc == 0) {
        if (tv.nstores == 0) {
            fprintf(stderr, "tsdb: tier lost: %s has no tier stores\n", root);
            rc = TSDB_ERR_INVALID;
        } else {
            rc = tiers_db_id_read(rootfd, &has_db_id, db_id);
            if (rc == 0 && !has_db_id) {
                fprintf(stderr, "tsdb: tier lost: %s/%s lists %zu store(s) but root META has no db_id=: refused\n",
                        root, TIERS_FILE, tv.nstores);
                rc = TSDB_ERR_CORRUPT;
            }
        }
    }

    size_t tidx = 0;
    if (rc == 0) {
        rc = resolve_store(&tv, store, &tidx);
        if (rc != 0)
            fprintf(stderr, "tsdb: tier lost: store '%s' not found in %s (%zu store(s))\n", store, TIERS_FILE,
                    tv.nstores);
        else if (tv.stores[tidx].state == TIERS_STATE_SHRINKING || tv.stores[tidx].state == TIERS_STATE_REMOVED) {
            fprintf(stderr, "tsdb: tier lost --replace: store %zu is %s and cannot be replaced: refused\n", tidx,
                    tv.stores[tidx].state == TIERS_STATE_REMOVED ? "REMOVED" : "SHRINKING");
            rc = TSDB_ERR_INVALID;
        }
    }

    /* Path nesting / overlap checks against root, volumes, and every OTHER non-lost store. */
    if (rc == 0 && (ctl_path_within(canon, root) || ctl_path_within(root, canon) || ctl_same_dir(canon, root) > 0)) {
        fprintf(stderr, "tsdb: tier lost --replace %s: overlaps database root %s: refused\n", canon, root);
        rc = TSDB_ERR_INVALID;
    }
    for (size_t i = 0; rc == 0 && i < nv; i++) {
        if (ctl_path_within(canon, vpaths[i]) || ctl_path_within(vpaths[i], canon) ||
            ctl_same_dir(canon, vpaths[i]) > 0) {
            fprintf(stderr, "tsdb: tier lost --replace %s: overlaps volume %s: refused\n", canon, vpaths[i]);
            rc = TSDB_ERR_INVALID;
        }
    }
    for (size_t i = 0; rc == 0 && i < tv.nstores; i++) {
        if (i == tidx || !tiers_state_is_active(tv.stores[i].state))
            continue;
        if (ctl_path_within(canon, tv.stores[i].path) || ctl_path_within(tv.stores[i].path, canon) ||
            ctl_same_dir(canon, tv.stores[i].path) > 0) {
            fprintf(stderr, "tsdb: tier lost --replace %s: overlaps existing tier store %s: refused\n", canon,
                    tv.stores[i].path);
            rc = TSDB_ERR_INVALID;
        }
    }

    /* Device (st_dev) isolation check (E58, D18) unless TSDB_TIER_ALLOW_SHARED_DEV is set. */
    if (rc == 0 && !(flags & TSDB_TIER_ALLOW_SHARED_DEV)) {
        struct stat st_cmp;
        if (stat("/", &st_cmp) == 0 && st_new.st_dev == st_cmp.st_dev) {
            fprintf(stderr, "tsdb: tier lost --replace %s: is on the same device as /: refused (pass "
                            "--allow-shared-device for tests)\n", canon);
            rc = TSDB_ERR_INVALID;
        }
        if (rc == 0 && fstat(rootfd, &st_cmp) == 0 && st_new.st_dev == st_cmp.st_dev) {
            fprintf(stderr, "tsdb: tier lost --replace %s: is on the same device as database root %s: refused (pass "
                            "--allow-shared-device for tests)\n", canon, root);
            rc = TSDB_ERR_INVALID;
        }
        for (size_t i = 0; rc == 0 && i < nv; i++) {
            if (stat(vpaths[i], &st_cmp) == 0 && st_new.st_dev == st_cmp.st_dev) {
                fprintf(stderr, "tsdb: tier lost --replace %s: is on the same device as volume %s: refused (pass "
                                "--allow-shared-device for tests)\n", canon, vpaths[i]);
                rc = TSDB_ERR_INVALID;
            }
        }
        for (size_t i = 0; rc == 0 && i < tv.nstores; i++) {
            if (i == tidx || !tiers_state_is_active(tv.stores[i].state))
                continue;
            if (stat(tv.stores[i].path, &st_cmp) == 0 && st_new.st_dev == st_cmp.st_dev) {
                fprintf(stderr, "tsdb: tier lost --replace %s: is on the same device as tier store %s: refused (pass "
                                "--allow-shared-device for tests)\n", canon, tv.stores[i].path);
                rc = TSDB_ERR_INVALID;
            }
        }
    }

    /* Check replacement directory is empty (or crashed init of this db), never non-empty or foreign db. */
    bool adopt = false;
    uint8_t new_sid[16];
    memset(new_sid, 0, sizeof new_sid);
    if (rc == 0)
        rc = ctl_dir_check(canon, db_id, &tv, &adopt, new_sid);

    /* Step 1: initialize replacement store META (bound to db_id) + durable REBUILDING marker BEFORE TIERS update. */
    if (rc == 0 && made && sync)
        rc = ctl_sync_parent(canon);
    if (rc == 0) {
        if (adopt) {
            if (sync)
                rc = ctl_sync_adopted(canon);
        } else {
            chunk_store_owner ow = {.db_set = true};
            memcpy(ow.db_id, db_id, 16);
            rc = chunk_store_init2(canon, NULL, &ow, new_sid);
        }
    }
    if (rc == 0)
        rc = write_rebuilding_marker(canon, sync);

    /* Step 2: append STATE LOST (if store is not already LOST) and STATE REPLACED_BY to TIERS under VOLUMES lock. */
    if (rc == 0) {
        const tiers_store *target = &tv.stores[tidx];
        uint8_t lost_id[16];
        if (target->state == TIERS_STATE_REPLACED_BY)
            memcpy(lost_id, target->replaced_by, 16);
        else
            memcpy(lost_id, target->id, 16);

        uint8_t new_cls = opt_class ? opt_cls : target->cls;
        bool new_sc = (flags & TSDB_TIER_SINGLE_COPY) ? true : target->single_copy;

        if (target->state != TIERS_STATE_LOST)
            rc = tiers_append_state(rootfd, sync, lost_id, TIERS_STATE_LOST, 0, false, NULL, NULL);
        if (rc == 0)
            rc = tiers_append_state(rootfd, sync, lost_id, TIERS_STATE_REPLACED_BY, new_cls, new_sc, new_sid, canon);
    }

    if (rc == 0) {
        if (new_id_out)
            memcpy(new_id_out, new_sid, 16);
        if (slot_out)
            *slot_out = (int)tidx;
    }

    tiers_view_free(&tv);
    ctl_free_paths(vpaths, nv);
    close(vfd);

fail_mkdir:
    if (rc != 0 && made)
        (void)rmdir(replace_path);
    return rc;
}

int tierctl_count_shrink_objects(const struct tiers_view *tv, const char *const *vol_paths, size_t nvols,
                                 uint64_t *objects_out)
{
    if (!objects_out)
        return TSDB_ERR_INVALID;
    *objects_out = 0;
    if (!tv || !vol_paths || nvols == 0)
        return 0;

    const char *elig_paths[TIERS_STORES_MAX];
    size_t n_elig = 0;
    for (size_t i = 0; i < tv->nstores && n_elig < TIERS_STORES_MAX; i++)
        if (tv->stores[i].state != TIERS_STATE_SHRINKING && tv->stores[i].state != TIERS_STATE_REMOVED)
            elig_paths[n_elig++] = tv->stores[i].path;

    uint64_t count = 0;
    for (size_t v = 0; v < nvols; v++) {
        if (!vol_paths[v])
            continue;
        int vfd = open(vol_paths[v], O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        if (vfd < 0)
            continue;
        DIR *d = fdopendir(vfd);
        if (!d) {
            close(vfd);
            continue;
        }
        struct dirent *e;
        while ((e = readdir(d)) != NULL) {
            const char *nm = e->d_name;
            size_t nl = strlen(nm);
            if (nl <= 3 || strcmp(nm + nl - 3, ".ec") != 0)
                continue;
            if (strncmp(nm, "retired_", 8) == 0 || strncmp(nm, "tmp_", 4) == 0)
                continue;
            int fd = openat(dirfd(d), nm, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
            if (fd < 0)
                continue;
            struct stat st;
            if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size < (off_t)(EC_TRAILER_BYTES + EC_REC_FIXED)) {
                close(fd);
                continue;
            }
            uint8_t tr[EC_TRAILER_BYTES];
            if (pread(fd, tr, sizeof tr, st.st_size - (off_t)EC_TRAILER_BYTES) != (ssize_t)sizeof tr ||
                memcmp(tr + EC_TR_MAGIC, EC_TRAILER_MAGIC, 8) != 0) {
                close(fd);
                continue;
            }
            uint64_t rec_off = 0, rec_len = 0;
            memcpy(&rec_off, tr + EC_TR_REC_OFF, 8);
            memcpy(&rec_len, tr + EC_TR_REC_LEN, 8);
            if (rec_len < EC_REC_FIXED || rec_off > (uint64_t)st.st_size - EC_TRAILER_BYTES ||
                rec_len > (uint64_t)st.st_size - EC_TRAILER_BYTES - rec_off) {
                close(fd);
                continue;
            }
            uint8_t rec[EC_REC_FIXED];
            if (pread(fd, rec, sizeof rec, (off_t)rec_off) != (ssize_t)sizeof rec) {
                close(fd);
                continue;
            }
            close(fd);

            uint8_t oid[16];
            uint16_t k = 0, m = 0;
            memcpy(oid, rec + EC_REC_OID, 16);
            memcpy(&k, rec + EC_REC_K, 2);
            memcpy(&m, rec + EC_REC_M, 2);
            unsigned np = (unsigned)k + (unsigned)m;
            if (np == 0 || np > CHUNK_POS_MAX || n_elig < (size_t)np) {
                count++;
                continue;
            }
            char ohex[33];
            chunk_oid_hex(oid, ohex);
            uint64_t h = xxh64(oid, 16, 0);
            bool need = false;
            for (unsigned pos = 0; pos < np; pos++) {
                size_t si = (size_t)((h + (uint64_t)pos) % (uint64_t)n_elig);
                char cpath[PATH_MAX + 128];
                int n = snprintf(cpath, sizeof cpath, "%s/s%zu/o%s.%u.chunk", elig_paths[si], v, ohex, pos);
                if (n <= 0 || (size_t)n >= sizeof cpath || access(cpath, F_OK) != 0) {
                    need = true;
                    break;
                }
            }
            if (need)
                count++;
        }
        closedir(d);
    }
    *objects_out = count;
    return 0;
}

int tierctl_lost_dir(const char *dir, const char *store, const char *replace_path, const char *opt_class,
                     uint32_t flags, uint8_t new_id_out[16], int *slot_out)
{
    if (!dir || !*dir)
        return TSDB_ERR_INVALID;
    char root[PATH_MAX];
    if (!realpath(dir, root))
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    int rootfd = open(root, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (rootfd < 0)
        return TSDB_ERR_IO;
    int rc = tierctl_lost_impl(rootfd, root, true, store, replace_path, opt_class, flags, new_id_out, slot_out);
    close(rootfd);
    return rc;
}

#else /* !TSDB_EC */

int tierctl_open(struct tier *t)
{
    (void)t;
    return 0;
}
void tierctl_eng_stop(struct eng *db) { (void)db; }
void tierctl_quiesce(struct tier *t) { (void)t; }
bool tierctl_close(struct tier *t)
{
    (void)t;
    return false;
}
void tierctl_wake(struct tier *t) { (void)t; }
bool tierctl_get_progress(const struct tier *t, tierctl_progress *out)
{
    (void)t;
    if (out) {
        memset(out, 0, sizeof *out);
        out->store = -1;
        out->shrink_store = -1;
    }
    return false;
}
int tierctl_wait_idle(struct tier *t, int timeout_ms)
{
    (void)t;
    (void)timeout_ms;
    return 0;
}
bool tierctl_is_closing(const struct tier *t)
{
    (void)t;
    return true;
}
bool tierctl_scrub_should_yield(struct tier *t)
{
    (void)t;
    return true;
}
bool tierctl_wait_interruptible_ms(struct tier *t, uint64_t wait_ms)
{
    (void)t, (void)wait_ms;
    return false;
}
uint32_t tierctl_scrub_mb_s(const struct tier *t)
{
    (void)t;
    return TIERS_SCRUB_MB_S_DEFAULT;
}
uint32_t tierctl_scrub_days(const struct tier *t)
{
    (void)t;
    return TIERS_SCRUB_DAYS_DEFAULT;
}
int tierctl_lost_impl(int rootfd, const char *root, bool sync, const char *store, const char *replace_path,
                      const char *opt_class, uint32_t flags, uint8_t new_id_out[16], int *slot_out)
{
    (void)rootfd, (void)root, (void)sync, (void)store, (void)replace_path, (void)opt_class, (void)flags;
    (void)new_id_out, (void)slot_out;
    fprintf(stderr, "tsdb: tier lost: not built with the erasure codec (TSDB_EC)\n");
    return TSDB_ERR_INVALID;
}
int tierctl_count_shrink_objects(const struct tiers_view *tv, const char *const *vol_paths, size_t nvols,
                                 uint64_t *objects_out)
{
    (void)tv, (void)vol_paths, (void)nvols;
    if (objects_out)
        *objects_out = 0;
    return 0;
}
int tierctl_lost_dir(const char *dir, const char *store, const char *replace_path, const char *opt_class,
                     uint32_t flags, uint8_t new_id_out[16], int *slot_out)
{
    (void)dir, (void)store, (void)replace_path, (void)opt_class, (void)flags, (void)new_id_out, (void)slot_out;
    fprintf(stderr, "tsdb: tier lost: not built with the erasure codec (TSDB_EC)\n");
    return TSDB_ERR_INVALID;
}

#endif
