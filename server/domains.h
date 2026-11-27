/* SPDX-License-Identifier: AGPL-3.0-only */
/* tsdbd and storage domains (docs/ha.md 15.2, 15.3, 15.5; included by tsdbd.c, one translation unit).
 *
 * A volume whose directory lies under a --health-domain prefix belongs to that domain (the root never does: its
 * failure ends the server as before, and so does the failure of a volume outside every domain). Three things follow:
 *
 *  - Admission. A request that names a series by id enters its volume through dom_enter, and a packed insert
 *    enters every volume its rows name (dom_enter_ids). While the domain is UP nothing is refused (a busy volume is
 *    busy, and its callers queue in the pool like everyone else's). While it is not UP at most DOM_CAP requests are
 *    inside a volume: the rest are refused with TSDB_ERR_UNAVAILABLE at once, so a volume whose server hangs holds
 *    few workers. Requests inside count as waiting (worker_waits) while their domain is not UP: the pool starts
 *    extra workers, and healthy volumes go on. A request that cannot say which volumes it touches (QUERY_MULTI,
 *    COMMIT, FLUSH, a series created by name, ...) counts as waiting while any domain is not UP (dom_any_enter).
 *  - Outage. A domain going DOWN quiesces its volumes (new calls on them are refused with UNAVAILABLE, names and types
 *    still answer). The recovery thread of each waits for the domain to leave DOWN (RECOVERING), then for the calls
 *    inside to return (no deadline: a hung NFS call cannot be interrupted), then tsdb_volume_reopen compares the
 *    stopped engine's memory with the storage and opens the directory again. A volume that lost acknowledged data
 *    stays quiesced (DIVERGED) for good: a later outage does not recover it again (the new engine never knew what
 *    was lost, so a second revalidation would say all is well); restarting tsdbd accepts the loss. The other volumes
 *    of the domain go on. When every volume of the domain is back the domain is UP.
 *  - Fail-stop. An engine that failed (fsyncgate) in a domain is recovered the same way, by a reopen that replays
 *    what is on disk, never by retrying the fsync. Outside a domain the failure ends the process, status 1.
 *
 * Not here (O6, later): the per-volume queue ahead of the workers, parking with abandonment, per-connection write
 * sets. A request that is already inside a hung call keeps its worker until the call returns. */
#ifndef TSDBD_DOMAINS_H
#define TSDBD_DOMAINS_H

#define DOM_CAP 4         /* requests inside one domain volume at a time */
#define DOM_MAX_VOLS 1024 /* volumes with admission state; a higher number is outside every domain */
#define DOM_MAX 64
#define DOM_MAX_STORES 64 /* single-server tier stores (matches TIER_DIRS_MAX) */

typedef struct dom_vol {
    atomic_int dom;          /* domain index, -1 outside every domain, -2 not decided yet; workers decide it
                                concurrently (to the same value), and others read it in dom_enter (review m4) */
    int inflight, counted;   /* under dom_mu: requests inside, and how many of them count as waiting workers */
    atomic_bool recovering;  /* a recovery thread runs */
    atomic_bool diverged;    /* the reopen found lost data: quiesced until tsdbd restarts (sticky) */
    atomic_int div_logged;   /* vol_diverged's log-once bits: 1 the engine's DIVERGED file, 2 the reopen's report */
} dom_vol;

typedef struct dom_disk_state {
    uint8_t store_id[16];
    uint32_t vols[CLUSTER_REPORT_VOLS];
    uint32_t nvols;
    atomic_bool recovering;
} dom_disk_state;

static struct {
    tsdb *db;
    tsdb_health *health;
    struct tier *tier; /* refcounted active single-server tier (for --health-domain store mapping and STATUS) */
    int nstores;
    int store_dom[DOM_MAX_STORES]; /* domain index covering tier store i, or -1 */
    char prefix[DOM_MAX][PATH_MAX];
    int ndoms;
    atomic_bool nonup[DOM_MAX];
    dom_vol vol[DOM_MAX_VOLS];
    pthread_mutex_t mu; /* guards inflight and counted of every volume and any_*; a leaf */
    int any_inflight, any_counted; /* requests that name no volume, and how many count as waiting workers */
    atomic_int threads; /* recovery threads alive */
    atomic_bool stop;
    void (*waits)(int delta);
    /* R13 (r13-fault): per-disk cluster fault-domain state & recovery threads. */
    pthread_mutex_t disk_mu; /* pure leaf guarding disks[].store_id/vols/nvols */
    dom_disk_state disks[DISKS_MAX];
    atomic_int disk_threads;
    atomic_bool disk_stop;
} dom = {.mu = PTHREAD_MUTEX_INITIALIZER, .disk_mu = PTHREAD_MUTEX_INITIALIZER};

/* Seconds on the monotonic clock: the lines of a storage event can be set beside the harness's own clock (jepsen's
 * outage trace) and the ones of another volume. */
static double dom_now_s(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static void dom_log(const char *what, size_t v, const char *extra)
{
    fprintf(stderr, "tsdbd: [%.3f] volume %zu: %s%s%s\n", dom_now_s(), v, what, extra ? ": " : "",
            extra ? extra : "");
}

/* The one answer to "is volume v DIVERGED" (r8i, D8): the engine's durable flag (the DIVERGED file, src/ecdiverge.h:
 * a settle's digest that failed, set again by every open) or the outage reopen's report (dom.vol[v].diverged, memory
 * only: a restart accepts that loss). Both are TSDB_WHY_DIVERGED; vol_health, and through it UNAVAILABLE, STATUS,
 * TOPOLOGY and the metrics, read only this, so the two flags never give two answers. `which` names the source (the
 * engine's first); the first sighting of each source is logged once per volume. No I/O: the engine flag is an atomic
 * read under the tsdb read lock. */
static bool vol_diverged(size_t v, const char **which)
{
    const char *w = NULL;
    int bit = 0;
    if (dom.db && tsdb_volume_diverged(dom.db, v) == 1) /* the root too: it has a tier, and refuses when diverged */

        w = "the volume's DIVERGED file", bit = 1;
    else if (v < DOM_MAX_VOLS && atomic_load(&dom.vol[v].diverged))
        w = "the outage reopen's report", bit = 2;
    if (which)
        *which = w;
    if (!w)
        return false;
    if (v < DOM_MAX_VOLS && !(atomic_fetch_or(&dom.vol[v].div_logged, bit) & bit))
        dom_log("DIVERGED, refused until an operator acts (tsdb DIR volume diverged)", v, w);
    return true;
}

/* Longest --health-domain prefix holding `path` (pure string comparison, no I/O). */
static int dom_match_path(const char *path)
{
    if (!path || dom.ndoms == 0)
        return -1;
    int best = -1;
    size_t best_len = 0;
    for (int i = 0; i < dom.ndoms; i++) {
        size_t n = strlen(dom.prefix[i]);
        if (strncmp(path, dom.prefix[i], n) == 0 && (path[n] == 0 || path[n] == '/' || n == 1) && n >= best_len) {
            best = i;
            best_len = n;
        }
    }
    return best;
}

/* The domain of volume v: the longest prefix that holds its directory. Decided once, by comparing strings (no I/O:
 * the call must never wait for a server that is away). */
static int dom_of(size_t v)
{
    if (v == 0 || v >= DOM_MAX_VOLS || dom.ndoms == 0)
        return -1;
    int d = atomic_load(&dom.vol[v].dom);
    if (d != -2)
        return d;
    const char *path = tsdb_volume_path(dom.db, (uint32_t)v);
    if (!path)
        return -1; /* no such volume (yet): not decided */
    int best = dom_match_path(path);
    atomic_store(&dom.vol[v].dom, best);
    return best;
}

/* A request names its series by id: the volume it will use, or -1 (a name, many ids, no series). */
static int dom_volume_of(uint8_t op, const uint8_t *p, size_t len)
{
    if (dom.ndoms == 0)
        return -1;
    const auth_op *e = &auth_ops[op];
    size_t at;
    if (e->where == W_ID0)
        at = 0;
    else if (e->where == W_ID8)
        at = 8;
    else
        return -1;
    tsdb_sid id;
    if (len < at + 8)
        return -1;
    memcpy(&id, p + at, 8);
    if (id == TSDB_MANY_SERIES || id < ((tsdb_sid)1 << 32) || (id >> 48) != 0)
        return -1;
    size_t v = TSDB_SID_VOLUME(id);
    return dom_of(v) >= 0 ? (int)v : -1;
}

/* 0: the request may run (call dom_exit(v) after, v >= 0), TSDB_ERR_UNAVAILABLE: its domain is not UP and the volume
 * is full of requests. */
static int dom_enter(int v)
{
    int d = atomic_load(&dom.vol[v].dom), w = 0;
    pthread_mutex_lock(&dom.mu);
    bool nonup = atomic_load(&dom.nonup[d]);
    if (nonup && dom.vol[v].inflight >= DOM_CAP) {
        pthread_mutex_unlock(&dom.mu);
        unav_vol = (uint16_t)v; /* the reply names it (tsdbd.c) */
        return TSDB_ERR_UNAVAILABLE;
    }
    dom.vol[v].inflight++;
    if (nonup) {
        dom.vol[v].counted++;
        w = 1;
    }
    pthread_mutex_unlock(&dom.mu);
    if (w && dom.waits)
        dom.waits(1);
    return 0;
}

static void dom_exit(int v)
{
    int w = 0;
    pthread_mutex_lock(&dom.mu);
    dom.vol[v].inflight--;
    if (dom.vol[v].counted > dom.vol[v].inflight) {
        dom.vol[v].counted--;
        w = 1;
    }
    pthread_mutex_unlock(&dom.mu);
    if (w && dom.waits)
        dom.waits(-1);
}

/* A packed insert: enters every domain volume its ids name, once each. 0 (call dom_exit_all with the same list), or
 * TSDB_ERR_UNAVAILABLE with nothing entered. */
static int dom_enter_ids(const tsdb_sid *ids, size_t n, uint16_t *list, size_t *nlist)
{
    *nlist = 0;
    if (dom.ndoms == 0)
        return 0;
    uint8_t seen[DOM_MAX_VOLS];
    memset(seen, 0, sizeof seen);
    for (size_t i = 0; i < n; i++) {
        tsdb_sid id = ids[i];
        if (id < ((tsdb_sid)1 << 32) || (id >> 48) != 0)
            continue; /* the library refuses it */
        size_t v = TSDB_SID_VOLUME(id);
        if (v >= DOM_MAX_VOLS || seen[v] || dom_of(v) < 0)
            continue;
        seen[v] = 1;
    }
    for (size_t v = 1; v < DOM_MAX_VOLS; v++) {
        if (!seen[v])
            continue;
        int rc = dom_enter((int)v);
        if (rc != 0) {
            for (size_t k = 0; k < *nlist; k++)
                dom_exit(list[k]);
            *nlist = 0;
            return rc;
        }
        list[(*nlist)++] = (uint16_t)v;
    }
    return 0;
}

static void dom_exit_all(const uint16_t *list, size_t nlist)
{
    for (size_t k = 0; k < nlist; k++)
        dom_exit(list[k]);
}

static bool dom_any_nonup(void)
{
    for (int d = 0; d < dom.ndoms; d++)
        if (atomic_load(&dom.nonup[d]))
            return true;
    return false;
}

/* Is a request that entered with v (dom_enter's volume, -1: dom_any_enter, 0: no domain) inside a domain that is not UP?
 * The recovery of such a domain waits for the request to return. */
static bool dom_inside_nonup(int v)
{
    if (v == -1)
        return dom_any_nonup();
    if (v <= 0 || v >= DOM_MAX_VOLS)
        return false;
    int d = atomic_load(&dom.vol[v].dom);
    return d >= 0 && atomic_load(&dom.nonup[d]);
}

/* A request that names no volume (or names them only inside its handler) while a domain is configured: it may be
 * stuck in the call of a hung one, so it counts as a waiting worker while any domain is not UP. */
static void dom_any_enter(void)
{
    int w = 0;
    pthread_mutex_lock(&dom.mu);
    dom.any_inflight++;
    if (dom_any_nonup()) {
        dom.any_counted++;
        w = 1;
    }
    pthread_mutex_unlock(&dom.mu);
    if (w && dom.waits)
        dom.waits(1);
}

static void dom_any_exit(void)
{
    int w = 0;
    pthread_mutex_lock(&dom.mu);
    dom.any_inflight--;
    if (dom.any_counted > dom.any_inflight) {
        dom.any_counted--;
        w = 1;
    }
    pthread_mutex_unlock(&dom.mu);
    if (w && dom.waits)
        dom.waits(-1);
}

/* Counts the requests already inside the volumes of d as waiting workers (the domain just left UP). */
static void dom_count_inside(int d)
{
    int k = 0;
    pthread_mutex_lock(&dom.mu);
    atomic_store(&dom.nonup[d], true);
    size_t n = tsdb_volume_count(dom.db);
    for (size_t v = 1; v < n && v < DOM_MAX_VOLS; v++)
        if (dom_of(v) == d) {
            k += dom.vol[v].inflight - dom.vol[v].counted;
            dom.vol[v].counted = dom.vol[v].inflight;
        }
    k += dom.any_inflight - dom.any_counted;
    dom.any_counted = dom.any_inflight;
    pthread_mutex_unlock(&dom.mu);
    for (; k > 0 && dom.waits; k--)
        dom.waits(1);
}

static int dom_all_back(int d)
{
    size_t n = tsdb_volume_count(dom.db);
    for (size_t v = 1; v < n && v < DOM_MAX_VOLS; v++)
        if (dom_of(v) == d && atomic_load(&dom.vol[v].recovering))
            return 0; /* a diverged volume stays refused on its own; it does not hold the others' domain down */
    return 1;
}

static void dom_sleep_ms(int ms)
{
    struct timespec d = {ms / 1000, (long)(ms % 1000) * 1000000L};
    nanosleep(&d, NULL);
}

static void *dom_recover_main(void *arg)
{
    size_t v = (size_t)(intptr_t)arg;
    int d = dom_of(v);
    int waited = 0;
    while (!atomic_load(&dom.stop)) {
        tsdb_volume_quiesce(dom.db, v);
        /* the domain: a failed engine on an UP domain goes straight on; after an outage wait for RECOVERING */
        int st = tsdb_health_state(dom.health, d);
        if (st == TSDB_DOM_DOWN) {
            dom_sleep_ms(50);
            continue;
        }
        int idle = tsdb_volume_idle(dom.db, v, 1000);
        if (idle != 0) {
            if (++waited % 30 == 0)
                dom_log("waiting for calls that have not returned", v, NULL);
            continue;
        }
        tsdb_volume_report rep;
        memset(&rep, 0, sizeof rep);
        int rc = tsdb_volume_reopen(dom.db, v, &rep);
        if (rc != 0) {
            dom_log("reopen failed, will retry", v, tsdb_strerror(rc));
            dom_sleep_ms(1000);
            continue;
        }
        char b[256];
        snprintf(b, sizeof b,
                 "relogged %llu rows, repaired %llu series and %llu states, lost %llu parts (%llu rows) and %llu bytes "
                 "of RETENTION and CLASSES",
                 (unsigned long long)rep.relogged_rows, (unsigned long long)rep.series_repaired,
                 (unsigned long long)rep.states_repaired, (unsigned long long)rep.lost_parts,
                 (unsigned long long)rep.lost_part_rows, (unsigned long long)rep.lost_log_bytes);
        if (rep.diverged) {
            atomic_store(&dom.vol[v].diverged, true);
            dom_log("DIVERGED: data the server acknowledged is gone; the volume stays unavailable until tsdbd is "
                    "restarted",
                    v, b);
            break;
        }
        tsdb_volume_resume(dom.db, v);
        dom_log("recovered", v, b);
        break;
    }
    atomic_store(&dom.vol[v].recovering, false);
    if (d >= 0 && !atomic_load(&dom.stop) && dom_all_back(d)) {
        tsdb_health_set_up(dom.health, d);
    }
    atomic_fetch_sub(&dom.threads, 1);
    return NULL;
}

/* Starts the recovery of volume v unless one runs. */
static void dom_recover(size_t v)
{
    bool no = false;
    if (v >= DOM_MAX_VOLS || atomic_load(&dom.vol[v].diverged) ||
        !atomic_compare_exchange_strong(&dom.vol[v].recovering, &no, true))
        return; /* diverged is sticky: only a restart accepts the loss */
    tsdb_volume_quiesce(dom.db, v);
    atomic_fetch_add(&dom.threads, 1);
    pthread_t t;
    pthread_attr_t at;
    pthread_attr_init(&at);
    pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED);
    if (pthread_create(&t, &at, dom_recover_main, (void *)(intptr_t)v) != 0) {
        atomic_fetch_sub(&dom.threads, 1);
        atomic_store(&dom.vol[v].recovering, false);
        dom_log("cannot start the recovery thread", v, strerror(errno));
    }
    pthread_attr_destroy(&at);
}

static struct tier *dom_tier_ref(int *nstores)
{
    pthread_mutex_lock(&dom.mu);
    struct tier *t = dom.tier ? tier_get(dom.tier) : NULL;
    if (nstores)
        *nstores = t ? dom.nstores : 0;
    pthread_mutex_unlock(&dom.mu);
    return t;
}

/* The health monitor reports a state change (a leaf: no lock of the health module is held, it must not block). */
static void dom_on_change(void *ctx, int d, int old_state, int new_state)
{
    (void)ctx;
    if (atomic_load(&dom.stop) || d < 0 || d >= dom.ndoms)
        return;
    fprintf(stderr, "tsdbd: [%.3f] storage domain %s: %d -> %d\n", dom_now_s(), dom.prefix[d], old_state, new_state);
    if (new_state == TSDB_DOM_UP) {
        pthread_mutex_lock(&dom.mu);
        atomic_store(&dom.nonup[d], false);
        pthread_mutex_unlock(&dom.mu);
        int ns = 0;
        struct tier *t = dom_tier_ref(&ns);
        if (t) {
            for (int s = 0; s < ns; s++)
                if (dom.store_dom[s] == d)
                    tier_health_domain_state(t, s, false);
            tier_put(t);
        }
    } else {
        dom_count_inside(d);
        /* A recovery thread that finished before this call fired may already have set the domain UP: its UP came
         * first, so the nonup flag this call set must go again. */
        if (tsdb_health_state(dom.health, d) == TSDB_DOM_UP) {
            pthread_mutex_lock(&dom.mu);
            atomic_store(&dom.nonup[d], false);
            pthread_mutex_unlock(&dom.mu);
        }
    }
    if (new_state == TSDB_DOM_RECOVERING) {
        /* R15 D8: tier stores covered by this domain enqueue attach/heal on the tier attach worker (never blocking
         * here, and never quiescing any volume). */
        int ns = 0;
        struct tier *t = dom_tier_ref(&ns);
        if (t) {
            for (int s = 0; s < ns; s++)
                if (dom.store_dom[s] == d)
                    tier_health_domain_state(t, s, false);
            tier_put(t);
        }
        if (dom_all_back(d)) {
            /* No volume of the domain has a recovery thread to set it UP: all are diverged (dom_recover skips them, and
             * a diverged volume stays refused on its own) or the domain has none (or covers only tier stores). Without
             * this the domain stayed DOWN for good after any outage, and every request that names no volume counted as
             * a waiting worker. After dom_count_inside: the UP this fires clears the flag that call sets. */
            tsdb_health_set_up(dom.health, d);
        }
    }
    if (new_state == TSDB_DOM_DOWN) {
        size_t n = tsdb_volume_count(dom.db);
        for (size_t v = 1; v < n && v < DOM_MAX_VOLS; v++)
            if (dom_of(v) == d)
                dom_recover(v); /* quiesces at once; the thread waits for RECOVERING */
        /* R15 D8: a --health-domain over a tier store drives the store DOWN without quiescing any volume. */
        int ns = 0;
        struct tier *t = dom_tier_ref(&ns);
        if (t) {
            for (int s = 0; s < ns; s++)
                if (dom.store_dom[s] == d)
                    tier_health_domain_state(t, s, true);
            tier_put(t);
        }
    }
}

/* "key=value,..." from TSDBD_TEST_HEALTH: the thresholds scaled down, for tests only. */
static void dom_test_options(tsdb_health_options *o)
{
#ifdef TSDB_TEST_HOOKS
    const char *s = getenv("TSDBD_TEST_HEALTH");
    if (!s)
        return;
    char *copy = strdup(s), *save = NULL;
    for (char *kv = strtok_r(copy, ",", &save); kv; kv = strtok_r(NULL, ",", &save)) {
        char *eq = strchr(kv, '=');
        if (!eq)
            continue;
        *eq = 0;
        int val = atoi(eq + 1);
        if (!strcmp(kv, "down"))
            o->down_ms = val;
        else if (!strcmp(kv, "read"))
            o->read_every_ms = val;
        else if (!strcmp(kv, "write"))
            o->write_every_ms = val;
        else if (!strcmp(kv, "recover"))
            o->recover_ms = val;
        else if (!strcmp(kv, "flap"))
            o->flap_recover_ms = val;
        else if (!strcmp(kv, "fast"))
            o->fast_ms = val;
        else if (!strcmp(kv, "spawn"))
            o->spawn_ms = val;
    }
    free(copy);
#else
    (void)o;
#endif
}

/* Registers the prefixes and starts the monitor. Returns 0, or -1 with a message. */
static int dom_init(tsdb *db, char *const *prefixes, int n, void (*waits)(int))
{
    dom.db = db;
    dom.waits = waits;
    dom.tier = NULL;
    dom.nstores = 0;
    for (int i = 0; i < DOM_MAX_VOLS; i++)
        atomic_store(&dom.vol[i].dom, -2);
    for (int i = 0; i < DOM_MAX_STORES; i++)
        dom.store_dom[i] = -1;
    struct tier *t = tier_find_by_dir(tsdb_volume_path(db, 0));
    if (t) {
        dom.tier = t;
        int ns = tier_store_count(t);
        dom.nstores = ns > DOM_MAX_STORES ? DOM_MAX_STORES : ns;
    }
    if (n == 0)
        return 0;
    if (n > DOM_MAX) {
        fprintf(stderr, "tsdbd: at most %d --health-domain\n", DOM_MAX);
        tier_put(dom.tier);
        dom.tier = NULL;
        dom.nstores = 0;
        return -1;
    }
    tsdb_health_options o;
    memset(&o, 0, sizeof o);
    o.on_change = dom_on_change;
    dom_test_options(&o);
    dom.health = tsdb_health_open(&o);
    if (!dom.health) {
        fprintf(stderr, "tsdbd: cannot start the storage health monitor\n");
        tier_put(dom.tier);
        dom.tier = NULL;
        dom.nstores = 0;
        return -1;
    }
    for (int i = 0; i < n; i++) {
        char *rp = realpath(prefixes[i], NULL);
        if (!rp) {
            fprintf(stderr, "tsdbd: --health-domain %s: %s\n", prefixes[i], strerror(errno));
            tier_put(dom.tier);
            dom.tier = NULL;
            dom.nstores = 0;
            return -1;
        }
        snprintf(dom.prefix[i], sizeof dom.prefix[i], "%s", rp);
        free(rp);
        dom.ndoms = i + 1; /* before add_domain: a state change may call back at once */
    }
    /* Map tier stores before starting domain probes so the very first DOWN callback already sees store_dom[].
     * realpath() runs once at startup (consistent with tier_open, which already opened the stores). */
    if (dom.tier) {
        for (int s = 0; s < dom.nstores; s++) {
            char sp[PATH_MAX], canon[PATH_MAX];
            if (!tier_store_path(dom.tier, s, sp, sizeof sp))
                continue;
            const char *rp = realpath(sp, canon) ? canon : sp;
            dom.store_dom[s] = dom_match_path(rp);
        }
    }
    for (int i = 0; i < n; i++) {
        if (tsdb_health_add_domain(dom.health, dom.prefix[i], dom.prefix[i]) != i) {
            fprintf(stderr, "tsdbd: --health-domain %s: cannot probe it\n", prefixes[i]);
            tier_put(dom.tier);
            dom.tier = NULL;
            dom.nstores = 0;
            return -1;
        }
    }
    size_t nv = tsdb_volume_count(db);
    for (size_t v = 1; v < nv; v++)
        dom_of(v); /* decide now, while every path can be read */
    return 0;
}

/* One look at every volume (the failwatch thread, every 20 ms). Starts the recovery of an engine that failed in a
 * domain. Returns 1 (msg set) when something failed that ends the server: the root, a volume outside every domain,
 * or a failure no volume reports. */
static int dom_failwatch(char *msg, size_t msglen)
{
    static int unseen; /* consecutive looks at a failure that no volume reports */
    if (!tsdb_failed(dom.db, msg, msglen)) {
        unseen = 0;
        return 0;
    }
    size_t n = tsdb_volume_count(dom.db);
    int seen = 0;
    for (size_t v = 0; v < n; v++) {
        tsdb_volume_stats st;
        if (tsdb_get_volume_stats(dom.db, (uint32_t)v, &st) != 0 || !st.failed)
            continue;
        seen = 1;
        unseen = 0;
        char own[512]; /* this volume's own error: msg is the first failed volume's, which may be another one */
        if (tsdb_volume_failed(dom.db, (uint32_t)v, own, sizeof own) != TSDB_ERR_IO || !own[0])
            snprintf(own, sizeof own, "%s", msg);
        if (v > 0 && repld_engine_failed((uint32_t)v, own)) {
            uint8_t sid[16];
            if (cvol_vol_disk((uint32_t)v, NULL, 0, sid) == 0)
                disk_probe_now_store(sid);
            continue; /* a cluster volume: its driver quiesces it and reports it failed, the server goes on (D13) */
        }
        if (dom_of(v) < 0)
            return 1;
        if (!atomic_load(&dom.vol[v].recovering) && !atomic_load(&dom.vol[v].diverged)) {
            dom_log("storage failure, recovering in place", v, own);
            tsdb_health_probe_now(dom.health, dom_of(v));
            dom_recover(v);
        }
    }
    /* A recovery may have swapped in the new engine between tsdb_failed and the loop above: one look that finds no
     * failing volume proves nothing, the next tick decides (a failure that stays unreported ends the server). */
    return !seen && ++unseen >= 5 ? 1 : 0;
}

static void dom_shutdown(void)
{
    atomic_store(&dom.stop, true);
    for (int i = 0; i < 600 && atomic_load(&dom.threads) > 0; i++)
        dom_sleep_ms(50); /* a reopen stuck on a hung server is left: the process ends anyway */
    pthread_mutex_lock(&dom.mu);
    struct tier *t = dom.tier;
    dom.tier = NULL;
    dom.nstores = 0;
    pthread_mutex_unlock(&dom.mu);
    for (int d = 0; d < dom.ndoms; d++)
        if (tsdb_health_state(dom.health, d) != TSDB_DOM_UP) {
            tier_put(t);
            return; /* a probe may hang in the server's call: the process ends with it */
        }
    if (dom.health)
        tsdb_health_close(dom.health);
    tier_put(t);
}

/* ---- R13 (r13-fault; E57, §15.6, PLAN.md D16): per-disk cluster fault domains ---- */

static int dom_disk_state_of_vol(uint32_t v, tsdb_domain_info *out)
{
    uint8_t sid[16];
    if (!cvol_active() || cvol_vol_disk(v, NULL, 0, sid) != 0)
        return -1;
    tsdb_domain_info di;
    if (disk_probe_info_by_store(sid, &di) != 0)
        return -1;
    if (out)
        *out = di;
    return di.state;
}

static void dom_disk_record_vols(int disk_idx, const uint8_t store_id[16], const uint32_t *vols, uint32_t nv)
{
    pthread_mutex_lock(&dom.disk_mu);
    dom_disk_state *ds = &dom.disks[disk_idx];
    memcpy(ds->store_id, store_id, 16);
    for (uint32_t i = 0; i < nv && ds->nvols < CLUSTER_REPORT_VOLS; i++) {
        int dup = 0;
        for (uint32_t j = 0; j < ds->nvols; j++) {
            if (ds->vols[j] == vols[i]) {
                dup = 1;
                break;
            }
        }
        if (!dup)
            ds->vols[ds->nvols++] = vols[i];
    }
    pthread_mutex_unlock(&dom.disk_mu);
}

static void *dom_disk_recover_main(void *arg)
{
    int disk_idx = (int)(intptr_t)arg;
    while (!atomic_load(&dom.disk_stop)) {
        int st = disk_probe_state(disk_idx);
        if (st < 0)
            break;
        if (st == TSDB_DOM_DOWN) {
            dom_sleep_ms(50);
            continue;
        }
        uint32_t vols[CLUSTER_REPORT_VOLS];
        uint32_t nv = 0;
        uint8_t sid[16];
        pthread_mutex_lock(&dom.disk_mu);
        memcpy(sid, dom.disks[disk_idx].store_id, 16);
        nv = dom.disks[disk_idx].nvols;
        if (nv > CLUSTER_REPORT_VOLS)
            nv = CLUSTER_REPORT_VOLS;
        if (nv > 0)
            memcpy(vols, dom.disks[disk_idx].vols, nv * sizeof vols[0]);
        pthread_mutex_unlock(&dom.disk_mu);

        /* Also include any currently-failed volume on this disk that may have stopped while disk was DOWN. */
        uint32_t cur[CLUSTER_REPORT_VOLS];
        uint32_t ncur = cvol_vols_on_disk(sid, cur, CLUSTER_REPORT_VOLS);
        for (uint32_t i = 0; i < ncur && nv < CLUSTER_REPORT_VOLS; i++) {
            if (!repld_failed(cur[i]))
                continue;
            int dup = 0;
            for (uint32_t j = 0; j < nv; j++) {
                if (vols[j] == cur[i]) {
                    dup = 1;
                    break;
                }
            }
            if (!dup)
                vols[nv++] = cur[i];
        }

        int relapsed = 0;
        int retry = 0;
        uint8_t done_v[CLUSTER_REPORT_VOLS] = {0};
        for (uint32_t i = 0; i < nv; i++) {
            if (atomic_load(&dom.disk_stop))
                break;
            if (disk_probe_state(disk_idx) == TSDB_DOM_DOWN) {
                relapsed = 1;
                break;
            }
            if (vol_diverged(vols[i], NULL)) {
                done_v[i] = 1;
                continue;
            }
            dom_log("reopening replica from disk after fast-disk outage", vols[i], NULL);
            if (cvol_disk_reopen(vols[i]) == 0) {
                done_v[i] = 1;
            } else {
                retry = 1;
                break;
            }
        }
        if (!relapsed && !retry && !atomic_load(&dom.disk_stop)) {
            if (cvol_disk_reopen_refused(sid) < 0)
                retry = 1;
        }
        if (relapsed)
            continue;
        /* Wait briefly for the manager to finish prepare + REPLICA_OPEN + repld_open before clearing RECOVERING. */
        for (int wait_i = 0; wait_i < 600 && !atomic_load(&dom.disk_stop); wait_i++) {
            if (disk_probe_state(disk_idx) == TSDB_DOM_DOWN) {
                relapsed = 1;
                break;
            }
            int still = 0;
            for (uint32_t i = 0; i < nv; i++) {
                if (done_v[i] && !vol_diverged(vols[i], NULL) && cvol_vol_reopening(vols[i])) {
                    still = 1;
                    break;
                }
            }
            if (!still)
                break;
            dom_sleep_ms(50);
        }
        if (relapsed)
            continue;

        pthread_mutex_lock(&dom.disk_mu);
        uint32_t keep = 0;
        for (uint32_t j = 0; j < dom.disks[disk_idx].nvols; j++) {
            int done = 0;
            for (uint32_t i = 0; i < nv; i++) {
                if (done_v[i] && dom.disks[disk_idx].vols[j] == vols[i]) {
                    done = 1;
                    break;
                }
            }
            if (!done)
                dom.disks[disk_idx].vols[keep++] = dom.disks[disk_idx].vols[j];
        }
        dom.disks[disk_idx].nvols = keep;
        pthread_mutex_unlock(&dom.disk_mu);
        if (retry && !atomic_load(&dom.disk_stop)) {
            dom_sleep_ms(100);
            continue;
        }
        break;
    }
    atomic_store(&dom.disks[disk_idx].recovering, false);
    if (!atomic_load(&dom.disk_stop) && disk_probe_state(disk_idx) == TSDB_DOM_RECOVERING)
        disk_probe_set_up(disk_idx);
    atomic_fetch_sub(&dom.disk_threads, 1);
    return NULL;
}

/* Per-disk health callback (runs on health.c monitor/probe thread, no locks held, must not block). */
static void dom_on_disk_change(void *ctx, int disk_idx, const uint8_t store_id[16], const char *path, int disk_class,
                               uint8_t uses, int old_state, int new_state)
{
    (void)ctx;
    (void)path;
    (void)disk_class;
    (void)old_state;
    if (atomic_load(&dom.disk_stop) || disk_idx < 0 || disk_idx >= DISKS_MAX)
        return;

    if (new_state == TSDB_DOM_DOWN) {
        if (!(uses & META_DISK_USE_LOG))
            return;
        uint32_t vols[CLUSTER_REPORT_VOLS];
        uint32_t nv = cvol_vols_on_disk(store_id, vols, CLUSTER_REPORT_VOLS);
        dom_disk_record_vols(disk_idx, store_id, vols, nv);
#ifdef TSDB_TEST_HOOKS
        if (disks_mutant() == DISKS_MUT_STALLNORESIGN) {
            fprintf(stderr, "tsdbd: disks: MUTANT stallnoresign: keeping leaders on DOWN fast disk %s\n", path);
            return;
        }
#endif
        for (uint32_t i = 0; i < nv; i++)
            (void)repld_disk_down(vols[i], "fast disk probe DOWN");
        return;
    }

    if (new_state == TSDB_DOM_RECOVERING) {
        if (!(uses & META_DISK_USE_LOG)) {
            disk_probe_set_up(disk_idx);
            return;
        }
        uint32_t nv = 0;
        pthread_mutex_lock(&dom.disk_mu);
        nv = dom.disks[disk_idx].nvols;
        pthread_mutex_unlock(&dom.disk_mu);
        if (nv == 0) {
            uint32_t cur[CLUSTER_REPORT_VOLS];
            uint32_t ncur = cvol_vols_on_disk(store_id, cur, CLUSTER_REPORT_VOLS);
            for (uint32_t i = 0; i < ncur; i++) {
                if (repld_failed(cur[i])) {
                    dom_disk_record_vols(disk_idx, store_id, &cur[i], 1);
                    nv++;
                }
            }
        }
        if (nv == 0) {
            disk_probe_set_up(disk_idx);
            return;
        }
        bool no = false;
        if (!atomic_compare_exchange_strong(&dom.disks[disk_idx].recovering, &no, true))
            return;
        atomic_fetch_add(&dom.disk_threads, 1);
        pthread_t t;
        pthread_attr_t at;
        pthread_attr_init(&at);
        pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED);
        if (pthread_create(&t, &at, dom_disk_recover_main, (void *)(intptr_t)disk_idx) != 0) {
            atomic_fetch_sub(&dom.disk_threads, 1);
            atomic_store(&dom.disks[disk_idx].recovering, false);
            fprintf(stderr, "tsdbd: disks: cannot start recovery thread for disk %d: %s\n", disk_idx, strerror(errno));
            disk_probe_set_up(disk_idx);
        }
        pthread_attr_destroy(&at);
    }
}

static int dom_disk_init(char *err, size_t errlen)
{
    atomic_store(&dom.disk_stop, false);
    atomic_store(&dom.disk_threads, 0);
    pthread_mutex_lock(&dom.disk_mu);
    for (int i = 0; i < DISKS_MAX; i++) {
        memset(dom.disks[i].store_id, 0, 16);
        dom.disks[i].nvols = 0;
        atomic_store(&dom.disks[i].recovering, false);
    }
    pthread_mutex_unlock(&dom.disk_mu);
    return disk_probe_start(dom_on_disk_change, NULL, err, errlen);
}

static void dom_disk_shutdown(void)
{
    atomic_store(&dom.disk_stop, true);
    disk_probe_stop();
    for (int i = 0; i < 100 && atomic_load(&dom.disk_threads) > 0; i++)
        dom_sleep_ms(50);
}

#endif
