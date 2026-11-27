/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R9 rebuild and balancer traffic pacer (server/pace.h; track r9-pace; docs/design-cluster-data.md E13, §8.4; D18).
 *
 * Design:
 * - Two independent directional buckets: PACE_SEND (0) and PACE_RECV (1), each guarded by its own mutex and
 *   monotonic condition variable so sends and receives never contend or block each other.
 * - Active jobs are tracked in insertion order per direction. The effective margin of a direction is the minimum
 *   margin among all registered non-balance rebuild jobs (or the first registered job under PACE_MUT_FIFO).
 * - Margin 0 is unlimited (PACE_RATE_UNLIMITED) and exempt from foreground p99 halving.
 * - Margin 1 runs at 4 * rebuild_bytes_s (halved when p99_halved is set, unless PACE_MUT_NOHALVE).
 * - Margin >= 2 runs at rebuild_bytes_s (halved when p99_halved is set, unless PACE_MUT_NOHALVE).
 * - Balance jobs run at balance_bytes_s (0 = rebuild_bytes_s), halved on p99 load, and are refused (PACE_REFUSED)
 *   whenever any non-balance rebuild job is registered on that direction.
 * - Token bucket uses a virtual-time clock with a bounded burst window (50 ms), matching server/peerlink.c's
 *   bucket_take() so burst and steady-state pacing agree across both link-level and blocking callers.
 */
#include "pace.h"

#include "../src/compat.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define PACE_NS_PER_MS 1000000ull
#define PACE_NS_PER_S 1000000000ull
#define PACE_BURST_NS (50ull * PACE_NS_PER_MS)
#define PACE_SLEEP_SLICE_NS (50ull * PACE_NS_PER_MS)

struct pace_job {
    uint64_t id;
    int margin;
    int is_balance;
};

struct pace_bucket {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    int cv_init;
    uint64_t vt_ns;
    uint64_t last_rate;
    uint64_t taken_bytes;
    uint64_t wait_ns;
    uint64_t class_bytes[PACE_CLS_MAX];
    uint64_t balance_refused;
    struct pace_job *jobs;
    size_t njobs;
    size_t cap;
    size_t nrebuild;
    size_t nbalance;
};

static struct {
    struct pace_bucket b[2];
    atomic_uint_fast64_t rebuild_bytes_s;
    atomic_uint_fast64_t balance_bytes_s;
    atomic_uint_fast64_t next_id;
    atomic_int halved;
    atomic_int mutant;
    pthread_once_t env_once;
} P = {
    .b = {{.mu = PTHREAD_MUTEX_INITIALIZER}, {.mu = PTHREAD_MUTEX_INITIALIZER}},
    .rebuild_bytes_s = PACE_DEFAULT_REBUILD_BYTES_S,
    .balance_bytes_s = 0,
    .next_id = 1,
    .halved = 0,
    .mutant = PACE_MUT_NONE,
    .env_once = PTHREAD_ONCE_INIT,
};

static uint64_t mono_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * PACE_NS_PER_S + (uint64_t)ts.tv_nsec;
}

static void cond_ensure_locked(struct pace_bucket *b)
{
    if (!b->cv_init) {
        tsdb_cond_init_mono(&b->cv);
        b->cv_init = 1;
    }
}

int pace_mutant_parse(const char *name)
{
    if (!name || !*name || strcmp(name, "none") == 0)
        return PACE_MUT_NONE;
    if (strcmp(name, "pacefifo") == 0)
        return PACE_MUT_FIFO;
    if (strcmp(name, "nohalve") == 0)
        return PACE_MUT_NOHALVE;
    return -1;
}

static void env_init_once(void)
{
#ifdef TSDB_TEST_HOOKS
    const char *env = getenv("TSDB_PACE_MUTANT");
    if (env && *env) {
        int m = pace_mutant_parse(env);
        if (m > 0)
            atomic_store_explicit(&P.mutant, m, memory_order_relaxed);
    }
#endif
}

static inline int cur_mutant(void)
{
    pthread_once(&P.env_once, env_init_once);
    return atomic_load_explicit(&P.mutant, memory_order_relaxed);
}

void pace_set_mutant(int mut)
{
    pthread_once(&P.env_once, env_init_once);
    atomic_store_explicit(&P.mutant, mut, memory_order_relaxed);
}

static inline int valid_dir(enum pace_dir dir)
{
    return dir == PACE_SEND || dir == PACE_RECV;
}

static inline int class_for(int margin, int is_balance)
{
    if (is_balance || margin == PACE_MARGIN_BALANCE)
        return PACE_CLS_BALANCE;
    if (margin <= 0)
        return PACE_CLS_MARGIN0;
    if (margin == 1)
        return PACE_CLS_MARGIN1;
    return PACE_CLS_MARGIN2PLUS;
}

/* Compute effective rate (bytes/s) for a given margin/balance class, applying p99 halving except at margin 0. */
static uint64_t rate_for_margin(int margin, int is_balance, int halved, int mut)
{
    uint64_t rb = atomic_load_explicit(&P.rebuild_bytes_s, memory_order_relaxed);
    if (!rb)
        rb = PACE_DEFAULT_REBUILD_BYTES_S;
    uint64_t bb = atomic_load_explicit(&P.balance_bytes_s, memory_order_relaxed);
    if (!bb)
        bb = rb;

    int apply_halve = halved && mut != PACE_MUT_NOHALVE;

    if (is_balance || margin == PACE_MARGIN_BALANCE) {
        uint64_t r = bb;
        if (apply_halve)
            r = r > 1 ? r / 2 : 1;
        return r;
    }
    if (margin <= 0)
        return PACE_RATE_UNLIMITED; /* margin 0 is unlimited and NEVER halved */

    uint64_t r;
    if (margin == 1)
        r = rb <= UINT64_MAX / 4 ? rb * 4 : UINT64_MAX;
    else
        r = rb;

    if (apply_halve && r != PACE_RATE_UNLIMITED)
        r = r > 1 ? r / 2 : 1;
    return r;
}

/* Effective margin of registered jobs on `b` (b->mu held).
 * Sets *is_bal = 1 when only balance jobs are registered.
 * Returns PACE_MARGIN_NONE when no jobs are registered. */
static int active_margin_locked(const struct pace_bucket *b, int mut, int *is_bal)
{
    *is_bal = 0;
    if (b->nrebuild > 0) {
        int best = -1;
        for (size_t i = 0; i < b->njobs; i++) {
            if (b->jobs[i].is_balance)
                continue;
            int m = b->jobs[i].margin;
            if (mut == PACE_MUT_FIFO)
                return m; /* planted bug: rate follows the FIRST registered rebuild job */
            if (best < 0 || m < best)
                best = m;
        }
        return best >= 0 ? best : 2;
    }
    if (b->nbalance > 0) {
        *is_bal = 1;
        return PACE_MARGIN_BALANCE;
    }
    return PACE_MARGIN_NONE;
}

/* Reconcile virtual clock when the effective rate increases (e.g. a margin-0 or margin-1 job joins)
 * so waiting threads accelerate immediately instead of sitting behind a stale slow-rate debt. */
static void adjust_clock_locked(struct pace_bucket *b, uint64_t new_rate)
{
    uint64_t old_rate = b->last_rate;
    b->last_rate = new_rate;
    if (new_rate == PACE_RATE_UNLIMITED) {
        b->vt_ns = 0;
        if (b->cv_init)
            pthread_cond_broadcast(&b->cv);
        return;
    }
    if (old_rate && old_rate != PACE_RATE_UNLIMITED && new_rate > old_rate && b->vt_ns) {
        uint64_t now = mono_ns();
        if (b->vt_ns > now) {
            uint64_t rem = b->vt_ns - now;
            /* Scale remaining wait down proportionally to the higher rate. */
            uint64_t scaled = (uint64_t)((double)rem * ((double)old_rate / (double)new_rate));
            b->vt_ns = now + scaled;
        }
        if (b->cv_init)
            pthread_cond_broadcast(&b->cv);
    }
}

void pace_configure(const struct pace_cfg *cfg)
{
    if (!cfg)
        return;
    uint64_t rb = cfg->rebuild_bytes_s ? cfg->rebuild_bytes_s : PACE_DEFAULT_REBUILD_BYTES_S;
    atomic_store_explicit(&P.rebuild_bytes_s, rb, memory_order_relaxed);
    atomic_store_explicit(&P.balance_bytes_s, cfg->balance_bytes_s, memory_order_relaxed);
    int mut = cur_mutant();
    int h = atomic_load_explicit(&P.halved, memory_order_relaxed);
    for (int d = 0; d < 2; d++) {
        struct pace_bucket *b = &P.b[d];
        pthread_mutex_lock(&b->mu);
        int is_bal = 0;
        int am = active_margin_locked(b, mut, &is_bal);
        uint64_t nr = am == PACE_MARGIN_NONE ? rate_for_margin(2, 0, h, mut) : rate_for_margin(am, is_bal, h, mut);
        adjust_clock_locked(b, nr);
        pthread_mutex_unlock(&b->mu);
    }
}

void pace_set_p99_halved(int halved)
{
    int nh = halved ? 1 : 0;
    int prev = atomic_exchange_explicit(&P.halved, nh, memory_order_relaxed);
    if (prev == nh)
        return;
    int mut = cur_mutant();
    for (int d = 0; d < 2; d++) {
        struct pace_bucket *b = &P.b[d];
        pthread_mutex_lock(&b->mu);
        int is_bal = 0;
        int am = active_margin_locked(b, mut, &is_bal);
        uint64_t nr = am == PACE_MARGIN_NONE ? rate_for_margin(2, 0, nh, mut) : rate_for_margin(am, is_bal, nh, mut);
        adjust_clock_locked(b, nr);
        pthread_mutex_unlock(&b->mu);
    }
}

uint64_t pace_job_begin_ex(enum pace_dir dir, int margin, int is_balance)
{
    if (!valid_dir(dir))
        return 0;
    int bal = is_balance || margin == PACE_MARGIN_BALANCE;
    int m = bal ? PACE_MARGIN_BALANCE : (margin < 0 ? 0 : margin);
    uint64_t id = atomic_fetch_add_explicit(&P.next_id, 1, memory_order_relaxed);
    if (!id)
        id = atomic_fetch_add_explicit(&P.next_id, 1, memory_order_relaxed);

    int mut = cur_mutant();
    int h = atomic_load_explicit(&P.halved, memory_order_relaxed);
    struct pace_bucket *b = &P.b[dir];
    pthread_mutex_lock(&b->mu);
    if (b->njobs == b->cap) {
        size_t ncap = b->cap ? b->cap * 2 : 16;
        struct pace_job *nj = realloc(b->jobs, ncap * sizeof *nj);
        if (!nj) {
            pthread_mutex_unlock(&b->mu);
            return 0;
        }
        b->jobs = nj;
        b->cap = ncap;
    }
    b->jobs[b->njobs++] = (struct pace_job){.id = id, .margin = m, .is_balance = bal};
    if (bal)
        b->nbalance++;
    else
        b->nrebuild++;
    int is_bal = 0;
    int am = active_margin_locked(b, mut, &is_bal);
    uint64_t nr = am == PACE_MARGIN_NONE ? rate_for_margin(2, 0, h, mut) : rate_for_margin(am, is_bal, h, mut);
    adjust_clock_locked(b, nr);
    pthread_mutex_unlock(&b->mu);
    return id;
}

void pace_job_update_ex(enum pace_dir dir, uint64_t job_id, int margin)
{
    if (!valid_dir(dir) || !job_id)
        return;
    int mut = cur_mutant();
    int h = atomic_load_explicit(&P.halved, memory_order_relaxed);
    struct pace_bucket *b = &P.b[dir];
    pthread_mutex_lock(&b->mu);
    for (size_t i = 0; i < b->njobs; i++) {
        if (b->jobs[i].id == job_id) {
            if (!b->jobs[i].is_balance)
                b->jobs[i].margin = margin < 0 ? 0 : margin;
            int is_bal = 0;
            int am = active_margin_locked(b, mut, &is_bal);
            uint64_t nr =
                am == PACE_MARGIN_NONE ? rate_for_margin(2, 0, h, mut) : rate_for_margin(am, is_bal, h, mut);
            adjust_clock_locked(b, nr);
            break;
        }
    }
    pthread_mutex_unlock(&b->mu);
}

static void remove_job_at_locked(struct pace_bucket *b, size_t idx, int mut, int h)
{
    if (b->jobs[idx].is_balance) {
        if (b->nbalance)
            b->nbalance--;
    } else {
        if (b->nrebuild)
            b->nrebuild--;
    }
    if (idx + 1 < b->njobs)
        memmove(&b->jobs[idx], &b->jobs[idx + 1], (b->njobs - idx - 1) * sizeof b->jobs[0]);
    b->njobs--;
    int is_bal = 0;
    int am = active_margin_locked(b, mut, &is_bal);
    uint64_t nr = am == PACE_MARGIN_NONE ? rate_for_margin(2, 0, h, mut) : rate_for_margin(am, is_bal, h, mut);
    adjust_clock_locked(b, nr);
}

void pace_job_end_ex(enum pace_dir dir, uint64_t job_id)
{
    if (!valid_dir(dir) || !job_id)
        return;
    int mut = cur_mutant();
    int h = atomic_load_explicit(&P.halved, memory_order_relaxed);
    struct pace_bucket *b = &P.b[dir];
    pthread_mutex_lock(&b->mu);
    for (size_t i = 0; i < b->njobs; i++) {
        if (b->jobs[i].id == job_id) {
            remove_job_at_locked(b, i, mut, h);
            break;
        }
    }
    pthread_mutex_unlock(&b->mu);
}

void pace_job_begin(enum pace_dir dir, int margin)
{
    (void)pace_job_begin_ex(dir, margin, margin == PACE_MARGIN_BALANCE);
}

void pace_job_end(enum pace_dir dir, int margin)
{
    if (!valid_dir(dir))
        return;
    int bal = margin == PACE_MARGIN_BALANCE;
    int m = bal ? PACE_MARGIN_BALANCE : (margin < 0 ? 0 : margin);
    int mut = cur_mutant();
    int h = atomic_load_explicit(&P.halved, memory_order_relaxed);
    struct pace_bucket *b = &P.b[dir];
    pthread_mutex_lock(&b->mu);
    for (size_t i = 0; i < b->njobs; i++) {
        if (b->jobs[i].is_balance == bal && b->jobs[i].margin == m) {
            remove_job_at_locked(b, i, mut, h);
            break;
        }
    }
    pthread_mutex_unlock(&b->mu);
}

/* Attempt to take `bytes` from `b` (b->mu held).
 * If `force_cap_ns` is non-zero, admits the bytes even when `vt_ns > now` and clamps `vt_ns` to `now + force_cap_ns`
 * so forced admission (when a link callback hits its wait cap) never builds unbounded virtual clock debt.
 * Returns 0 if admitted, PACE_REFUSED if balance must yield, or wait_ns > 0 if caller should wait. */
static uint64_t try_take_locked(struct pace_bucket *b, uint64_t bytes, int margin, int use_active_only,
                                uint64_t force_cap_ns)
{
    int mut = cur_mutant();
    int h = atomic_load_explicit(&P.halved, memory_order_relaxed);
    int req_bal = !use_active_only && (margin == PACE_MARGIN_BALANCE);

    /* Balance jobs yield whenever ANY non-balance rebuild job is active on this direction. */
    if (req_bal && b->nrebuild > 0) {
        b->balance_refused++;
        return PACE_REFUSED;
    }

    int act_bal = 0;
    int am = active_margin_locked(b, mut, &act_bal);

    int eff_margin;
    int eff_bal;
    if (req_bal) {
        eff_margin = PACE_MARGIN_BALANCE;
        eff_bal = 1;
    } else if (use_active_only) {
        if (am == PACE_MARGIN_NONE) {
            eff_margin = 2;
            eff_bal = 0;
        } else {
            eff_margin = am;
            eff_bal = act_bal;
        }
    } else {
        /* Rebuild caller with explicit margin >= 0. */
        int cm = margin < 0 ? 0 : margin;
        if (b->nrebuild > 0) {
            /* Under PACE_MUT_FIFO, follow ONLY the first registered job's margin. */
            eff_margin = (mut == PACE_MUT_FIFO) ? am : (cm < am ? cm : am);
        } else {
            eff_margin = cm;
        }
        eff_bal = 0;
    }

    int cls = class_for(eff_margin, eff_bal);
    uint64_t rate = rate_for_margin(eff_margin, eff_bal, h, mut);

    if (rate == PACE_RATE_UNLIMITED || bytes == 0) {
        b->taken_bytes += bytes;
        b->class_bytes[cls] += bytes;
        return 0;
    }

    uint64_t now = mono_ns();
    if (b->vt_ns <= now || force_cap_ns) {
        uint64_t floor = now > PACE_BURST_NS ? now - PACE_BURST_NS : 0;
        uint64_t cost = (uint64_t)((double)bytes * 1e9 / (double)rate);
        if (cost == 0 && bytes > 0)
            cost = 1;
        uint64_t next_vt = (b->vt_ns > floor ? b->vt_ns : floor) + cost;
        if (force_cap_ns && next_vt > now + force_cap_ns)
            next_vt = now + force_cap_ns;
        b->vt_ns = next_vt;
        b->taken_bytes += bytes;
        b->class_bytes[cls] += bytes;
        return 0;
    }
    return b->vt_ns - now;
}

uint64_t pace_try_take(enum pace_dir dir, uint64_t bytes, int margin)
{
    if (!valid_dir(dir))
        return 0;
    struct pace_bucket *b = &P.b[dir];
    pthread_mutex_lock(&b->mu);
    uint64_t w = try_take_locked(b, bytes, margin, 0, 0);
    pthread_mutex_unlock(&b->mu);
    return w;
}

uint64_t pace_take_capped(enum pace_dir dir, uint64_t bytes, int margin, uint64_t max_wait_ns)
{
    if (!valid_dir(dir))
        return 0;
    struct pace_bucket *b = &P.b[dir];
    uint64_t t0 = 0;
    uint64_t total_wait = 0;

    pthread_mutex_lock(&b->mu);
    for (;;) {
        uint64_t w = try_take_locked(b, bytes, margin, 0, 0);
        if (w == 0) {
            if (t0) {
                uint64_t now = mono_ns();
                total_wait = now > t0 ? now - t0 : 0;
                b->wait_ns += total_wait;
            }
            pthread_mutex_unlock(&b->mu);
            return total_wait;
        }
        if (w == PACE_REFUSED) {
            pthread_mutex_unlock(&b->mu);
            return PACE_REFUSED;
        }
        uint64_t now = mono_ns();
        if (!t0)
            t0 = now;
        uint64_t elapsed = now > t0 ? now - t0 : 0;
        if (max_wait_ns && elapsed >= max_wait_ns) {
            (void)try_take_locked(b, bytes, margin, 0, max_wait_ns);
            b->wait_ns += elapsed;
            pthread_mutex_unlock(&b->mu);
            return elapsed;
        }
        cond_ensure_locked(b);
        uint64_t sl = w > PACE_SLEEP_SLICE_NS ? PACE_SLEEP_SLICE_NS : w;
        if (max_wait_ns && sl > max_wait_ns - elapsed)
            sl = max_wait_ns - elapsed;
        struct timespec dl;
        clock_gettime(TSDB_COND_CLOCK, &dl);
        uint64_t ns = (uint64_t)dl.tv_nsec + sl;
        dl.tv_sec += (time_t)(ns / PACE_NS_PER_S);
        dl.tv_nsec = (long)(ns % PACE_NS_PER_S);
        pthread_cond_timedwait(&b->cv, &b->mu, &dl);
    }
}

uint64_t pace_take(enum pace_dir dir, uint64_t bytes, int margin)
{
    return pace_take_capped(dir, bytes, margin, 0);
}

uint64_t pace_peerlink_send(void *arg, int kind, size_t n)
{
    (void)arg;
    (void)kind;
    struct pace_bucket *b = &P.b[PACE_SEND];
    pthread_mutex_lock(&b->mu);
    uint64_t w = try_take_locked(b, (uint64_t)n, 0, 1, 0);
    pthread_mutex_unlock(&b->mu);
    return w;
}

void pace_stats(struct pace_stats *out)
{
    if (!out)
        return;
    memset(out, 0, sizeof *out);
    int mut = cur_mutant();
    int h = atomic_load_explicit(&P.halved, memory_order_relaxed);
    out->halved = (h && mut != PACE_MUT_NOHALVE) ? 1 : 0;
    for (int d = 0; d < 2; d++) {
        struct pace_bucket *b = &P.b[d];
        pthread_mutex_lock(&b->mu);
        out->taken_bytes[d] = b->taken_bytes;
        out->wait_ns[d] = b->wait_ns;
        out->balance_refused[d] = b->balance_refused;
        for (int c = 0; c < PACE_CLS_MAX; c++)
            out->class_bytes[d][c] = b->class_bytes[c];
        int is_bal = 0;
        int am = active_margin_locked(b, mut, &is_bal);
        out->active_margin[d] = am;
        out->rate_bytes_s[d] =
            am == PACE_MARGIN_NONE ? rate_for_margin(2, 0, h, mut) : rate_for_margin(am, is_bal, h, mut);
        pthread_mutex_unlock(&b->mu);
    }
}

void pace_reset(void)
{
    pthread_once(&P.env_once, env_init_once);
    atomic_store_explicit(&P.rebuild_bytes_s, PACE_DEFAULT_REBUILD_BYTES_S, memory_order_relaxed);
    atomic_store_explicit(&P.balance_bytes_s, 0, memory_order_relaxed);
    atomic_store_explicit(&P.halved, 0, memory_order_relaxed);
    atomic_store_explicit(&P.mutant, PACE_MUT_NONE, memory_order_relaxed);
    for (int d = 0; d < 2; d++) {
        struct pace_bucket *b = &P.b[d];
        pthread_mutex_lock(&b->mu);
        free(b->jobs);
        b->jobs = NULL;
        b->njobs = b->cap = b->nrebuild = b->nbalance = 0;
        b->vt_ns = 0;
        b->last_rate = 0;
        b->taken_bytes = 0;
        b->wait_ns = 0;
        b->balance_refused = 0;
        memset(b->class_bytes, 0, sizeof b->class_bytes);
        pthread_mutex_unlock(&b->mu);
    }
}
