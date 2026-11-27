/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * tsdbd's instance of the peer cold ops (server/chunkpeer.h, R8 r8f): the transport over the chunk server's links
 * (chunksrv_call, chunksrv_put_remote on chunksrv_cluster(); this member's own position handed to its own chunk server
 * in process), the view of a volume from the metadata copy (cluster_copy_get: the stripe group by position, the
 * volume's epoch, cluster_copy_caught_up_at), the tier's cluster hook (tier_set_cluster: an ec volume attaches with
 * coldops_peer, the copy's k and m and the --ec-* settings) and chunksrv's epoch source for an unsure store.
 *
 * Links (r8f requirement 2): chunksrv_cluster.c already dials CHUNK and CHUNKB to every member of the copy and drops
 * the ones that left (chunksrv_keep_peers), a superset of every stripe group this member leads or replicates, kept
 * as fwd_cluster.c keeps FWD links; nothing here dials.
 *
 * The epoch source is installed by a small thread once the chunk server serves (chunksrv_cluster() is NULL before):
 * until then an unsure store refuses puts and deletes, the safe side.
 */
#include "chunkpeer.h"

#include "../include/tsdb.h"
#include "../src/compat.h"
#include "../src/cvol.h"
#include "../src/eccache.h"
#include "../src/eccold.h"
#include "../src/ecgc.h"
#include "../src/ecgcmk.h"
#include "../src/ecpart.h"
#include "../src/feed.h"
#include "../src/internal.h"
#include "../src/tier.h"
#include "chunksrv.h"
#include "chunkrepair.h"
#include "cluster.h"
#include "crashpt.h"
#include "creport.h"
#include "cvol.h"
#include "margins.h"
#include "meta.h"
#include "pace.h"
#include "place.h"
#include "repld.h"
#include "sfill.h"
#include "sreconcile.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#ifdef TSDB_EC

static struct chunkpeer G;
static _Atomic(tsdb *) G_db;
static pthread_t src_th;
static atomic_bool src_stop, src_started;
/* the source thread lives until chunkpeer_cluster_stop: one that returned early (its work is done once the chunk server
 * is there) stayed a finished thread nobody joined until the clean stop, and ThreadSanitizer reports such a thread at
 * every other end of tsdbd (a fail-stop's exit(1)): status 66 instead of 1 (test_cluster exitthreads) */
static pthread_mutex_t src_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t src_cv = PTHREAD_COND_INITIALIZER;

static int cp_objlist_srv(void *arg, uint32_t volume, uint64_t max_version, bool has_cursor,
                          const uint8_t cursor_oid[16], uint32_t max_entries, size_t max_bytes,
                          uint8_t vuuid_out[16], struct cs_objlist_item **items_out, uint32_t *n_out,
                          bool *more_out);
static void cp_reopen_srv(void *arg, uint32_t peer, uint32_t volume, const uint8_t oid[16]);
static int cp_gcpending_srv(void *arg, uint32_t volume, uint8_t (**oids_out)[16], uint32_t *n_out,
                            bool *truncated_out);
static void cp_reconcile_srv(void *arg, uint32_t volume, uint32_t pos, uint64_t stripe_version);
/* Leader-side handler for CS_OP_DAMAGED (R12, r12-chunk; D9): validates leadership and whether the reported object is
 * live in the volume's manifest (unknown/retired objects are left for E14 orphan GC), logs the damage, and increments
 * tsdb_chunk_scrub_reports_received_total. Active leader-driven cell repair / rebuild orchestration belongs to the
 * separate repair/rebuild track. */
static int cp_damaged_srv(void *arg, uint32_t peer, uint32_t volume, const uint8_t oid[16], uint16_t pos,
                          uint64_t bad_cells);
static void cp_on_read_damaged(void *arg, uint32_t volume, const uint8_t oid[16], unsigned pos, uint32_t node);
static void cp_read_pace(void *arg, uint32_t volume, size_t bytes);
static void cp_release_gc_crash(void) { crash_point(CP_RELEASE_GC); }
static void cp_gc_kick(uint32_t volume)
{
    tsdb *db = atomic_load_explicit(&G_db, memory_order_acquire);
    if (!db)
        return;
    eng *e = tsdb_cvol_ref(db, volume);
    if (e) {
        if (e->tier)
            ecgc_kick_tier(e->tier);
        tsdb_cvol_unref(&e);
    }
}

/* Per-volume counter of CHUNK_DAMAGED reports received while leading (R12, r12-chunk). */
static pthread_mutex_t cp_dmg_mu = PTHREAD_MUTEX_INITIALIZER;
struct cp_dmg_ent {
    uint32_t volume;
    uint64_t count;
};
static struct cp_dmg_ent *cp_dmg_vols;
static size_t cp_dmg_n, cp_dmg_cap;

static uint64_t cp_dmg_inc(uint32_t volume)
{
    uint64_t val = 0;
    pthread_mutex_lock(&cp_dmg_mu);
    for (size_t i = 0; i < cp_dmg_n; i++) {
        if (cp_dmg_vols[i].volume == volume) {
            val = ++cp_dmg_vols[i].count;
            pthread_mutex_unlock(&cp_dmg_mu);
            return val;
        }
    }
    if (cp_dmg_n == cp_dmg_cap) {
        size_t nc = cp_dmg_cap ? cp_dmg_cap * 2 : 16;
        struct cp_dmg_ent *nv = realloc(cp_dmg_vols, nc * sizeof *nv);
        if (nv) {
            cp_dmg_vols = nv;
            cp_dmg_cap = nc;
        }
    }
    if (cp_dmg_n < cp_dmg_cap) {
        cp_dmg_vols[cp_dmg_n].volume = volume;
        val = cp_dmg_vols[cp_dmg_n].count = 1;
        cp_dmg_n++;
    }
    pthread_mutex_unlock(&cp_dmg_mu);
    return val;
}

static void cp_dmg_collect(void *ctx, struct creport_sink *sink)
{
    (void)ctx;
    struct cp_dmg_ent *snap = NULL;
    size_t n = 0;
    pthread_mutex_lock(&cp_dmg_mu);
    n = cp_dmg_n;
    if (n > 0) {
        snap = malloc(n * sizeof *snap);
        if (snap)
            memcpy(snap, cp_dmg_vols, n * sizeof *snap);
    }
    pthread_mutex_unlock(&cp_dmg_mu);
    if (!snap)
        return;
    const struct cluster_copy *c = cluster_copy_get();
    for (size_t i = 0; i < n; i++) {
        if (c) {
            const struct cluster_copy_vol *cv = cluster_copy_vol(c, snap[i].volume);
            if (!cv || cv->redundancy != META_RED_EC || cv->leader != c->self)
                continue;
        }
        char vbuf[16];
        snprintf(vbuf, sizeof vbuf, "%" PRIu32, snap[i].volume);
        struct creport_label lbl = {"volume", vbuf};
        creport_sink_put_u64(sink, &lbl, 1, snap[i].count);
    }
    if (c)
        cluster_copy_put(c);
    free(snap);
}

static uint32_t self_id(const struct cluster_copy *c) { return c ? c->self : 0; }

/* TSDB_CHUNKPEER_TEST=holdput=MS[:PATH]: the first put that reaches a member (with PATH: the first once PATH exists)
 * holds the chunk server MS ms more and then reads its stores, so that a test can stop tsdbd (SIGTERM) while a settle's
 * put is in flight: the shutdown must wait for it (chunksrv_cluster_hold), never free the chunk server under it. The
 * hook says which happened: "still open" (its stores read), or "closed under it" (not read: a use after free, which
 * ASan would report and a plain build might not notice). */
static int hold_hook_ms;
static char hold_hook_path[512];
static atomic_bool hold_done;
static char pause_midput_path[512];
#ifdef TSDB_TEST_HOOKS
/* TSDB_CHUNKPEER_TEST=read_delay=FILE: while FILE exists with "<node> <delay_ms> [max_count]", delays CS_OP_READ
 * calls to `node` by `delay_ms` (up to `max_count` calls when > 0, else every call); changing FILE resets the count. */
static char read_delay_path[512];
static pthread_mutex_t read_delay_mu = PTHREAD_MUTEX_INITIALIZER;
static char read_delay_last[128];
static uint64_t read_delay_seen;

static void read_delay_hook(uint32_t node)
{
    int fd = open(read_delay_path, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return;
    char buf[128];
    ssize_t n = read(fd, buf, sizeof buf - 1);
    close(fd);
    if (n <= 0)
        return;
    buf[n] = '\0';
    unsigned target = 0;
    int ms = 0;
    unsigned long long max_cnt = 0;
    if (sscanf(buf, "%u %d %llu", &target, &ms, &max_cnt) < 2 || ms <= 0 || target != node)
        return;
    bool do_sleep = false;
    pthread_mutex_lock(&read_delay_mu);
    if (strcmp(buf, read_delay_last) != 0) {
        snprintf(read_delay_last, sizeof read_delay_last, "%s", buf);
        read_delay_seen = 0;
    }
    if (max_cnt == 0 || read_delay_seen < max_cnt) {
        read_delay_seen++;
        do_sleep = true;
    }
    pthread_mutex_unlock(&read_delay_mu);
    if (do_sleep) {
        struct timespec ts = {ms / 1000, (long)(ms % 1000) * 1000000L};
        nanosleep(&ts, NULL);
    }
}
#endif

static void pause_midput_hook(unsigned pos)
{
    if (pos != 0 || !pause_midput_path[0] || access(pause_midput_path, F_OK) != 0)
        return;
    fprintf(stderr, "tsdbd: test: pause_midput: paused after position 0\n");
    while (access(pause_midput_path, F_OK) == 0) {
        struct timespec ts = {0, 50 * 1000000L};
        nanosleep(&ts, NULL);
    }
    fprintf(stderr, "tsdbd: test: pause_midput: resumed before position 1\n");
}

static void hold_hook(struct chunksrv *cs)
{
    if (hold_hook_path[0] && access(hold_hook_path, F_OK) != 0)
        return;
    if (atomic_exchange(&hold_done, true))
        return;
    int closes = chunksrv_cluster_closes();
    fprintf(stderr, "tsdbd: test: holdput: a put holds the chunk server for %d ms\n", hold_hook_ms);
    struct timespec ts = {hold_hook_ms / 1000, (long)(hold_hook_ms % 1000) * 1000000L};
    nanosleep(&ts, NULL);
    if (chunksrv_cluster_closes() != closes) {
        fprintf(stderr, "tsdbd: test: holdput: released, the chunk server closed under it\n");
        return;
    }
    int up = 0;
    (void)chunksrv_stores(cs, &up, false);
    fprintf(stderr, "tsdbd: test: holdput: released, the chunk server still open (%d stores up)\n", up);
}

static int net_call(void *arg, uint32_t node, const struct cs_req *r, int timeout_ms, int32_t *rc, uint8_t **body,
                    size_t *body_len)
{
    (void)arg;
#ifdef TSDB_TEST_HOOKS
    if (r->op == CS_OP_READ && read_delay_path[0])
        read_delay_hook(node);
#endif
    struct chunksrv *cs = chunksrv_cluster_hold(); /* the engines' threads call: tsdbd frees it before tsdb_close */
    if (!cs)
        return TSDB_ERR_UNAVAILABLE; /* no chunk server here (yet, or any more): every member is away as far as we know */
    const struct cluster_copy *c = cluster_copy_get();
    uint32_t self = self_id(c);
    cluster_copy_put(c);
    int ret = self && node == self ? chunkpeer_local_call(cs, self, r, rc, body, body_len)
                                   : chunksrv_call(cs, node, r, timeout_ms, rc, body, body_len);
    chunksrv_cluster_release();
    return ret;
}

static int net_put(void *arg, uint32_t node, const struct cs_put_meta *m, int fd, uint64_t base, uint64_t sum,
                   int timeout_ms, uint64_t *answered)
{
    (void)arg;
    struct chunksrv *cs = chunksrv_cluster_hold();
    if (!cs)
        return TSDB_ERR_UNAVAILABLE;
    const struct cluster_copy *c = cluster_copy_get();
    uint32_t self = self_id(c);
    cluster_copy_put(c);
    int rc;
    if (self && node == self) {
        rc = chunkpeer_local_put(cs, self, m, fd, base, answered);
    } else {
        /* chunksrv_put_remote compares the PUT_OK's body itself (TSDB_ERR_IO when it differs): 0 means it answered
         * the hash of this meta and sum */
        rc = chunksrv_put_remote(cs, node, m, fd, base, sum, timeout_ms);
        if (rc == 0) {
            uint8_t meta[CS_PUT_META_MAX];
            size_t ml = cs_put_meta_encode(m, meta);
            *answered = cs_put_answer_hash_len(meta, ml, sum);
        }
    }
    if (hold_hook_ms > 0) /* TSDB_CHUNKPEER_TEST=holdput=MS: a put in flight across tsdbd's shutdown (test_cluster) */
        hold_hook(cs);
    chunksrv_cluster_release();
    if (rc == 0 && pause_midput_path[0])
        pause_midput_hook(m->pos);
    return rc;
}

static int view(void *arg, uint32_t volume, struct chunkpeer_view *v)
{
    (void)arg;
    memset(v, 0, sizeof *v);
    const struct cluster_copy *c = cluster_copy_get();
    if (!c)
        return TSDB_ERR_NOTFOUND;
    const struct cluster_copy_vol *cv = cluster_copy_vol(c, volume);
    int rc = TSDB_ERR_NOTFOUND;
    if (cv && cv->redundancy == META_RED_EC && cv->nstripe && cv->nstripe <= CHUNK_POS_MAX) {
        v->caught_up = cluster_copy_caught_up_at(c);
        v->epoch = cv->epoch;
        v->stripe_version = cv->stripe_version;
        v->k = cv->ec_k;
        v->m = cv->ec_m;
        v->nstripe = cv->nstripe;
        v->gc_suspended = cv->gc_suspended;
        memcpy(v->stripe, cv->stripe, cv->nstripe * sizeof *cv->stripe);
        for (uint32_t i = 0; i < cv->nstripe; i++) {
            v->stripe_filling[i] = cv->stripe_pos[i].state == META_STRIPE_FILLING ? 1 : 0;
            uint32_t prev = cv->stripe_pos[i].prev;
            v->stripe_prev[i] = prev;
            v->stripe_set_ver[i] = cv->stripe_pos[i].set_ver;
            if (prev != 0) {
                const struct cluster_copy_member *pm = cluster_copy_member(c, prev);
                v->stripe_prev_up[i] = (pm && (pm->state == META_UP || pm->state == META_DRAINING)) ? 1 : 0;
            }
        }
        rc = 0;
    }
    cluster_copy_put(c);
    return rc;
}

static int cp_node_state(void *arg, uint32_t node)
{
    (void)arg;
    const struct cluster_copy *c = cluster_copy_get();
    if (!c)
        return CP_NODE_DOWN;
    int st = CP_NODE_DOWN;
    if (cluster_copy_caught_up_at(c)) {
        const struct cluster_copy_member *mb = cluster_copy_member(c, node);
        if (!mb || mb->state == META_DESTROYED)
            st = CP_NODE_DESTROYED;
        else if (mb->state == META_UP || mb->state == META_DRAINING)
            st = CP_NODE_UP;
    }
    cluster_copy_put(c);
    return st;
}

/* tier_cluster_fn: what an ec volume attaches with */
static int attach(void *arg, uint32_t volume, tier_attach *a, tier_settings *s)
{
    struct chunkpeer *cp = arg;
    struct chunkpeer_view v;
    int rc = cp->view(cp->view_arg, volume, &v);
    if (rc)
        return rc == TSDB_ERR_NOTFOUND ? TSDB_ERR_UNAVAILABLE : rc; /* the copy does not hold it as ec (yet) */
    s->k = (int)v.k;
    s->m = (int)v.m;
    s->grace_s = g_ec_cfg.settle_grace_s;
    s->keep_s = g_ec_cfg.settle_keep_s;
    s->coarse_s = 0;
    s->cache_mb = g_ec_cfg.cache_bytes >> 20;
    s->force_s = g_ec_cfg.settle_force_s;
    s->wait_max_s = g_ec_cfg.settle_wait_max_s;
    a->ops = &coldops_peer;
    a->make = chunkpeer_make;
    a->make_arg = cp;
    a->epoch = chunkpeer_epoch;
    a->epoch_arg = cp;
    uint64_t le = 0;
    if (repld_leading(volume, &le) > 0 && le > 0)
        a->lead_epoch = le; /* r8f F4: pin this leader's own epoch, never a newer leader's from the copy */
    return 0;
}

static int cp_rel_guard(void *arg, uint32_t volume, unsigned pos, uint64_t stripe_ver);

static void *src_main(void *arg)
{
    (void)arg;
    while (!atomic_load(&src_stop)) {
        struct chunksrv *cs = chunksrv_cluster();
        if (cs) {
            G.local = cs;
            chunksrv_epoch_source(cs, chunkpeer_epoch_source, &G);
            chunksrv_set_objlist(cs, cp_objlist_srv, NULL);
            chunksrv_set_reopen(cs, cp_reopen_srv, NULL);
            chunksrv_set_gcpending(cs, cp_gcpending_srv, NULL);
            chunksrv_set_reconcile(cs, cp_reconcile_srv, NULL);
            chunksrv_set_damaged(cs, cp_damaged_srv, NULL);
            chunksrv_set_read_pacer(cs, cp_read_pace, NULL);
            chunksrv_set_rel_guard(cs, cp_rel_guard, NULL);
            fprintf(stderr, "tsdbd: chunk peer: the epoch source is set\n");
            pthread_mutex_lock(&src_mu);
            while (!atomic_load(&src_stop))
                pthread_cond_wait(&src_cv, &src_mu);
            pthread_mutex_unlock(&src_mu);
            return NULL;
        }
        struct timespec ts = {0, 50 * 1000000L};
        nanosleep(&ts, NULL);
    }
    return NULL;
}

int chunkpeer_cluster_start(const char *stage_dir)
{
#ifdef TSDB_TEST_HOOKS
    const char *th = getenv("TSDB_CHUNKPEER_TEST");
    if (th && !strncmp(th, "holdput=", 8)) {
        hold_hook_ms = atoi(th + 8);
        const char *colon = strchr(th + 8, ':');
        if (colon)
            snprintf(hold_hook_path, sizeof hold_hook_path, "%s", colon + 1);
    } else if (th && !strncmp(th, "pause_midput=", 13)) {
        snprintf(pause_midput_path, sizeof pause_midput_path, "%s", th + 13);
        chunkpeer_test_serial_put = 1;
    } else if (th && !strncmp(th, "read_delay=", 11)) {
        snprintf(read_delay_path, sizeof read_delay_path, "%s", th + 11);
    }
    const char *mu = getenv("TSDB_CHUNKPEER_MUTANT");
    if (mu && *mu) {
        int k = chunkpeer_mutant_parse(mu);
        if (k < 0) {
            fprintf(stderr, "tsdbd: TSDB_CHUNKPEER_MUTANT=%s is not a planted bug\n", mu);
            return -1;
        }
        chunkpeer_mutant = k;
        fprintf(stderr, "tsdbd: chunkpeer MUTANT %s (tests only)\n", mu);
    }
#endif
    ecgc_env_init();
    chunkrepair_env_init();
    atomic_store_explicit(&ecgcmk_release_crash_hook, cp_release_gc_crash, memory_order_release);
    cluster_set_gc_kick(cp_gc_kick);
    G.net = (struct chunkpeer_net){.call = net_call, .put = net_put, .arg = &G};
    G.view = view;
    G.view_arg = &G;
    G.node_state = cp_node_state;
    G.note_missing = sreconcile_note_missing;
    G.on_damaged = cp_on_read_damaged;
    G.on_damaged_arg = NULL;
    snprintf(G.stage_dir, sizeof G.stage_dir, "%s", stage_dir);
    tier_set_cluster(attach, &G, g_ec_cfg.cache_bytes >> 20);
    (void)creport_metric_collector("tsdb_chunk_scrub_reports_received_total", CREPORT_COUNTER,
                                   "Total CHUNK_DAMAGED reports received by this volume leader.", cp_dmg_collect, NULL);
    if (pthread_create(&src_th, NULL, src_main, NULL) != 0)
        return -1;
    atomic_store(&src_started, true);
    return 0;
}

void chunkpeer_cluster_stop(void)
{
    creport_metric_collector_del("tsdb_chunk_scrub_reports_received_total");
    cluster_set_gc_kick(NULL);
    atomic_store_explicit(&ecgcmk_release_crash_hook, NULL, memory_order_release);
    if (!atomic_exchange(&src_started, false))
        return;
    pthread_mutex_lock(&src_mu);
    atomic_store(&src_stop, true);
    pthread_cond_signal(&src_cv);
    pthread_mutex_unlock(&src_mu);
    pthread_join(src_th, NULL);
}

struct chunkpeer *chunkpeer_cluster(void) { return &G; }

int chunkpeer_cluster_prepare(uint32_t volume, const char *vdir)
{
    struct chunkpeer_view v;
    if (view(NULL, volume, &v) != 0)
        return 0; /* a replicas volume (or one the copy does not hold as ec): no tier */
    int dfd = open(vdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd < 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    /* before the engine reads META: requires=ec is what makes it attach (eccold_may_attach), tier what makes every
     * later open need one; the attach adds vuuid= (the volume's uuid) */
    int rc = meta_require(dfd, true, META_REQ_TIER | META_REQ_EC);
    close(dfd);
    if (rc)
        fprintf(stderr, "tsdbd: volume %u: META could not be given requires=tier,ec (%s): not opened\n", volume,
                tsdb_strerror(rc));
    return rc;
}

/* ---------------------------------------------------------------- R9 OBJLIST/REOPEN servers & sfill cluster glue */

static int ec_rec_oid_cmp(const void *a, const void *b)
{
    const ec_record *const *ra = a;
    const ec_record *const *rb = b;
    return memcmp((*ra)->oid, (*rb)->oid, 16);
}

static void rec_heap_sift_up(const ec_record **h, size_t i)
{
    while (i > 0) {
        size_t p = (i - 1) / 2;
        if (memcmp(h[p]->oid, h[i]->oid, 16) >= 0)
            break;
        const ec_record *tmp = h[p];
        h[p] = h[i];
        h[i] = tmp;
        i = p;
    }
}

static void rec_heap_sift_down(const ec_record **h, size_t n, size_t i)
{
    for (;;) {
        size_t l = 2 * i + 1, r = l + 1, big = i;
        if (l < n && memcmp(h[l]->oid, h[big]->oid, 16) > 0)
            big = l;
        if (r < n && memcmp(h[r]->oid, h[big]->oid, 16) > 0)
            big = r;
        if (big == i)
            break;
        const ec_record *tmp = h[i];
        h[i] = h[big];
        h[big] = tmp;
        i = big;
    }
}

static int cp_objlist_srv(void *arg, uint32_t volume, uint64_t max_version, bool has_cursor,
                          const uint8_t cursor_oid[16], uint32_t max_entries, size_t max_bytes,
                          uint8_t vuuid_out[16], struct cs_objlist_item **items_out, uint32_t *n_out,
                          bool *more_out)
{
    (void)arg;
    *items_out = NULL;
    *n_out = 0;
    *more_out = false;
    if (max_entries == 0 || max_entries > CS_OBJLIST_MAX)
        return TSDB_ERR_INVALID;
    tsdb *db = atomic_load_explicit(&G_db, memory_order_acquire);
    if (!db)
        return TSDB_ERR_UNAVAILABLE;

    /* Serve OBJLIST only from the volume's lease-holding leader (cvol_led && cluster_lease_valid) whose copy has
     * already adopted >= max_version (D10 barrier) and has no in-flight settle at stripe_version < max_version.
     * Repeat the leader + lease + version check after db_snapshot so a deposition racing with db_snapshot can never
     * serve an incomplete list. */
    if (!cvol_led(volume) || !cluster_lease_valid())
        return TSDB_ERR_UNAVAILABLE;
    const struct cluster_copy *c = cluster_copy_get();
    if (!c)
        return TSDB_ERR_UNAVAILABLE;
    const struct cluster_copy_vol *cv = cluster_copy_vol(c, volume);
    uint64_t snap_epoch = 0;
    bool allowed = cv && cv->redundancy == META_RED_EC && c->self != 0 && cv->leader == c->self &&
                   (max_version == 0 || cv->stripe_version >= max_version);
    if (allowed)
        snap_epoch = cv->epoch;
    cluster_copy_put(c);
    if (!allowed)
        return TSDB_ERR_UNAVAILABLE;

    eng *e = tsdb_cvol_ref(db, volume);
    if (!e)
        return TSDB_ERR_UNAVAILABLE;
    uint64_t active_sv = eccold_active_sv(e);
    if (active_sv != 0 && (max_version == 0 || active_sv < max_version)) {
        tsdb_cvol_unref(&e);
        return TSDB_ERR_UNAVAILABLE;
    }
    const uint8_t *vu = tier_vuuid(e);
    if (!vu) {
        tsdb_cvol_unref(&e);
        return TSDB_ERR_UNAVAILABLE;
    }
    memcpy(vuuid_out, vu, 16);

    size_t np = 0;
    part **ps = db_snapshot(e, INT64_MIN, INT64_MAX, &np);
    if (!ps) {
        tsdb_cvol_unref(&e);
        return TSDB_ERR_NOMEM;
    }

    /* Post-snapshot leader, lease, epoch, and active-settle re-verification (r9-fill Round-3 follow-up + D10). */
    bool still_ok = cvol_led(volume) && cluster_lease_valid();
    if (still_ok) {
        uint64_t post_sv = eccold_active_sv(e);
        if (post_sv != 0 && (max_version == 0 || post_sv < max_version))
            still_ok = false;
    }
    if (still_ok) {
        const struct cluster_copy *c2 = cluster_copy_get();
        const struct cluster_copy_vol *cv2 = c2 ? cluster_copy_vol(c2, volume) : NULL;
        still_ok = cv2 && cv2->redundancy == META_RED_EC && c2->self != 0 && cv2->leader == c2->self &&
                   cv2->epoch == snap_epoch && (max_version == 0 || cv2->stripe_version >= max_version);
        cluster_copy_put(c2);
    }
    if (!still_ok) {
        db_release(ps, np);
        tsdb_cvol_unref(&e);
        return TSDB_ERR_UNAVAILABLE;
    }

    /* Bounded top-K selection (keep at most max_entries + 1 smallest unique oids above cursor) so each page
     * uses O(K) memory and avoids sorting all N parts of a large volume. */
    size_t keep = (size_t)max_entries + 1;
    const ec_record **recs = malloc(keep * sizeof *recs);
    if (!recs) {
        db_release(ps, np);
        tsdb_cvol_unref(&e);
        return TSDB_ERR_NOMEM;
    }
    size_t nu = 0;
    for (size_t i = 0; i < np; i++) {
        struct ecpart *ep = atomic_load_explicit(&ps[i]->ec, memory_order_acquire);
        if (!ep || ep->test_mark)
            continue;
        if (max_version != 0 && ep->rec.stripe_version >= max_version)
            continue;
        if (has_cursor && memcmp(ep->rec.oid, cursor_oid, 16) <= 0)
            continue;
        if (nu == keep && memcmp(ep->rec.oid, recs[0]->oid, 16) >= 0)
            continue;
        bool dup = false;
        for (size_t j = 0; j < nu; j++)
            if (memcmp(recs[j]->oid, ep->rec.oid, 16) == 0) {
                dup = true;
                break;
            }
        if (dup)
            continue;
        if (nu < keep) {
            recs[nu] = &ep->rec;
            rec_heap_sift_up(recs, nu++);
        } else {
            recs[0] = &ep->rec;
            rec_heap_sift_down(recs, nu, 0);
        }
    }
    if (nu > 1)
        qsort(recs, nu, sizeof *recs, ec_rec_oid_cmp);

    size_t take = 0;
    size_t bytes = CS_OBJLIST_HDR;
    for (size_t i = 0; i < nu && take < max_entries; i++) {
        size_t need = CS_OBJLIST_ENTRY_HDR + (size_t)(recs[i]->nsums * 8);
        if (take > 0 && bytes + need > max_bytes)
            break;
        if (bytes + need > CS_OBJLIST_BYTES_MAX + CS_OBJLIST_HDR) {
            if (take == 0) {
                free(recs);
                db_release(ps, np);
                tsdb_cvol_unref(&e);
                return TSDB_ERR_INVALID;
            }
            break;
        }
        bytes += need;
        take++;
    }
    *more_out = take < nu;

    struct cs_objlist_item *out = take ? calloc(take, sizeof *out) : NULL;
    if (take && !out) {
        free(recs);
        db_release(ps, np);
        tsdb_cvol_unref(&e);
        return TSDB_ERR_NOMEM;
    }
    for (size_t i = 0; i < take; i++) {
        const ec_record *r = recs[i];
        memcpy(out[i].oid, r->oid, 16);
        if (chunk_geom_make(&out[i].g, r->k, r->m, r->cell_bytes, r->object_len) != 0) {
            cs_objlist_items_free(out, (uint32_t)(i + 1));
            free(recs);
            db_release(ps, np);
            tsdb_cvol_unref(&e);
            return TSDB_ERR_CORRUPT;
        }
        out[i].positions = r->positions;
        out[i].writer_epoch = r->writer_epoch;
        out[i].stripe_version = r->stripe_version;
        out[i].nsums = r->nsums;
        if (r->nsums > 0) {
            out[i].sums = malloc((size_t)r->nsums * sizeof(uint64_t));
            if (!out[i].sums) {
                cs_objlist_items_free(out, (uint32_t)(i + 1));
                free(recs);
                db_release(ps, np);
                tsdb_cvol_unref(&e);
                return TSDB_ERR_NOMEM;
            }
            memcpy(out[i].sums, r->sums, (size_t)r->nsums * sizeof(uint64_t));
        }
    }
    free(recs);
    db_release(ps, np);
    tsdb_cvol_unref(&e);
    *items_out = out;
    *n_out = (uint32_t)take;
    return 0;
}

/* R9 (D12, r9-whole): server-side guard for CS_OP_DEL_REL. Refuses release deletes if this node's metadata copy
 * is not caught up, lags behind `stripe_ver`, or shows this node holding/filling `pos` or `pos` moved past `stripe_ver`. */
static int cp_rel_guard(void *arg, uint32_t volume, unsigned pos, uint64_t stripe_ver)
{
    (void)arg;
    const struct cluster_copy *c = cluster_copy_get();
    if (!c)
        return TSDB_ERR_UNAVAILABLE;
    int rc = 0;
    if (!cluster_copy_caught_up_at(c)) {
        rc = TSDB_ERR_UNAVAILABLE;
    } else {
        const struct cluster_copy_vol *cv = cluster_copy_vol(c, volume);
        if (!cv || cv->redundancy != META_RED_EC || pos >= cv->nstripe) {
            rc = TSDB_ERR_UNAVAILABLE;
        } else if (cv->stripe_version < stripe_ver) {
            rc = TSDB_ERR_UNAVAILABLE;
        } else {
            uint64_t sver = cv->stripe_pos[pos].set_ver ? cv->stripe_pos[pos].set_ver : cv->stripe_version;
            if (sver < stripe_ver) {
                rc = TSDB_ERR_UNAVAILABLE;
            } else if (cv->stripe[pos] == c->self || cv->stripe_pos[pos].state == META_STRIPE_FILLING) {
                rc = TSDB_ERR_LOCKED;
            } else {
                for (uint32_t p = 0; p < cv->nstripe && p < META_STRIPE_MAX; p++) {
                    if (cv->stripe_pos[p].state == META_STRIPE_FILLING && cv->stripe_pos[p].prev == c->self) {
                        rc = TSDB_ERR_LOCKED;
                        break;
                    }
                }
            }
        }
    }
    cluster_copy_put(c);
    return rc;
}

static void cp_reopen_srv(void *arg, uint32_t peer, uint32_t volume, const uint8_t oid[16])
{
    (void)arg;
    const struct cluster_copy *c = cluster_copy_get();
    if (c) {
        const struct cluster_copy_vol *cv = cluster_copy_vol(c, volume);
        if (cv && cv->redundancy == META_RED_EC) {
            for (uint32_t p = 0; p < cv->nstripe && p < META_STRIPE_MAX; p++) {
                if (cv->stripe[p] == peer && cv->stripe_pos[p].state == META_STRIPE_FILLING) {
                    uint64_t sver = cv->stripe_pos[p].set_ver ? cv->stripe_pos[p].set_ver : cv->stripe_version;
                    if (sver > 0)
                        chunkpeer_note_filled(peer, volume, p, sver, oid);
                }
            }
        }
        cluster_copy_put(c);
    }
    tsdb *db = atomic_load_explicit(&G_db, memory_order_acquire);
    if (!db)
        return;
    eng *e = tsdb_cvol_ref(db, volume);
    if (e) {
        if (e->tier)
            eccache_reopen(e->tier, volume, oid);
        tsdb_cvol_unref(&e);
    }
}

static int cp_gcpending_srv(void *arg, uint32_t volume, uint8_t (**oids_out)[16], uint32_t *n_out,
                            bool *truncated_out)
{
    (void)arg;
    *oids_out = NULL;
    *n_out = 0;
    if (truncated_out)
        *truncated_out = false;
    tsdb *db = atomic_load_explicit(&G_db, memory_order_acquire);
    if (!db)
        return TSDB_ERR_UNAVAILABLE;
    if (!cvol_led(volume) || !cluster_lease_valid())
        return TSDB_ERR_UNAVAILABLE;
    const struct cluster_copy *c = cluster_copy_get();
    if (!c)
        return TSDB_ERR_UNAVAILABLE;
    const struct cluster_copy_vol *cv = cluster_copy_vol(c, volume);
    bool allowed = cv && cv->redundancy == META_RED_EC && c->self != 0 && cv->leader == c->self;
    cluster_copy_put(c);
    if (!allowed)
        return TSDB_ERR_UNAVAILABLE;

    eng *e = tsdb_cvol_ref(db, volume);
    if (!e)
        return TSDB_ERR_UNAVAILABLE;
    uint8_t (*oids)[16] = NULL;
    size_t n = 0;
    int rc = ecgcmk_pending_list(e, &oids, &n);
    if (rc == 0 && n > 0 && e->tier)
        ecgc_kick_tier(e->tier);
    tsdb_cvol_unref(&e);
    if (rc != 0) {
        free(oids);
        return rc;
    }
    if (n > CS_GCPENDING_MAX) {
        n = CS_GCPENDING_MAX;
        if (truncated_out)
            *truncated_out = true;
    }
    *oids_out = oids;
    *n_out = (uint32_t)n;
    return 0;
}

static void cp_repair_enqueue(uint32_t volume, const uint8_t oid[16], unsigned pos, uint32_t node);

/* Leader-side handler for CS_OP_DAMAGED (R12, r12-chunk / r12-repair; D5, D9): validates leadership and whether the
 * reported object is live in the volume's manifest (unknown/retired/pending-MK_GC objects are left for E14 orphan GC),
 * logs the damage, increments tsdb_chunk_scrub_reports_received_total, and enqueues `(volume, oid, pos)` for repair. */
static int cp_damaged_srv(void *arg, uint32_t peer, uint32_t volume, const uint8_t oid[16], uint16_t pos,
                          uint64_t bad_cells)
{
    (void)arg;
    tsdb *db = atomic_load_explicit(&G_db, memory_order_acquire);
    if (!db)
        return TSDB_ERR_UNAVAILABLE;
    if (!cvol_led(volume) || !cluster_lease_valid())
        return TSDB_ERR_UNAVAILABLE;
    const struct cluster_copy *c = cluster_copy_get();
    if (!c)
        return TSDB_ERR_UNAVAILABLE;
    const struct cluster_copy_vol *cv = cluster_copy_vol(c, volume);
    bool allowed = cv && cv->redundancy == META_RED_EC && c->self != 0 && cv->leader == c->self;
    bool target_match = allowed && pos < cv->nstripe && cv->stripe[pos] == peer;
    cluster_copy_put(c);
    if (!allowed)
        return TSDB_ERR_UNAVAILABLE;

    eng *e = tsdb_cvol_ref(db, volume);
    if (!e)
        return TSDB_ERR_UNAVAILABLE;

    bool live = ecgc_oid_is_live(e, oid);
    tsdb_cvol_unref(&e);

    uint64_t cnt = cp_dmg_inc(volume);
    char hex[33];
    chunk_oid_hex(oid, hex);
    if (live) {
        fprintf(stderr,
                "tsdbd: volume %" PRIu32 ": CHUNK_DAMAGED from node %" PRIu32 " oid=%s pos=%u bad_cells=0x%" PRIx64
                " (live, reports=%" PRIu64 ")\n",
                volume, peer, hex, (unsigned)pos, bad_cells, cnt);
        if (target_match)
            cp_repair_enqueue(volume, oid, pos, peer);
    } else {
        /* D9: unknown or retired object is never repaired or deleted by scrub; left for E14 orphan GC. */
        fprintf(stderr,
                "tsdbd: volume %" PRIu32 ": CHUNK_DAMAGED from node %" PRIu32 " oid=%s pos=%u bad_cells=0x%" PRIx64
                " (unknown/retired, left for E14 orphan GC, reports=%" PRIu64 ")\n",
                volume, peer, hex, (unsigned)pos, bad_cells, cnt);
    }
    return 0;
}

static void cp_on_read_damaged(void *arg, uint32_t volume, const uint8_t oid[16], unsigned pos, uint32_t node)
{
    (void)arg;
    if (!cvol_led(volume))
        return;
    const struct cluster_copy *c = cluster_copy_get();
    if (!c)
        return;
    const struct cluster_copy_vol *cv = cluster_copy_vol(c, volume);
    bool ok = cv && cv->redundancy == META_RED_EC && c->self != 0 && cv->leader == c->self && pos < cv->nstripe &&
              cv->stripe[pos] == node && cv->stripe_pos[pos].state != META_STRIPE_FILLING;
    cluster_copy_put(c);
    if (!ok)
        return;
    cp_repair_enqueue(volume, oid, pos, node);
}

static int vol_settled_margin(const struct cluster_copy *c, const struct cluster_copy_vol *v)
{
    uint32_t destroyed[256];
    size_t ndest = 0;
    for (uint32_t i = 0; i < c->nmembers && ndest < 256; i++)
        if (c->members[i].state == META_DESTROYED)
            destroyed[ndest++] = c->members[i].id;
    struct margins mg;
    margins_of(c, v, destroyed, ndest, false, MARGIN_MUT_NONE, &mg);
    int settled = mg.settled;
    /* margins_of counts every UP/DRAINING stripe member, including a FILLING target that does not yet hold settled
     * objects; discount each FILLING position whose current holder was counted UP unless its prev holder is still UP. */
    for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++) {
        if (v->stripe_pos[p].state != META_STRIPE_FILLING)
            continue;
        const struct cluster_copy_member *cur = cluster_copy_member(c, v->stripe[p]);
        bool cur_up = cur && (cur->state == META_UP || cur->state == META_DRAINING);
        if (!cur_up)
            continue;
        const struct cluster_copy_member *pm =
            v->stripe_pos[p].prev ? cluster_copy_member(c, v->stripe_pos[p].prev) : NULL;
        bool prev_up = pm && (pm->state == META_UP || pm->state == META_DRAINING);
        if (!prev_up)
            settled--;
    }
    return settled < 0 ? 0 : settled;
}

static void cp_read_pace(void *arg, uint32_t volume, size_t bytes)
{
    (void)arg;
    int margin = 2;
    const struct cluster_copy *c = cluster_copy_get();
    if (c) {
        const struct cluster_copy_vol *cv = cluster_copy_vol(c, volume);
        if (cv && cv->redundancy == META_RED_EC && cv->nstripe > 0)
            margin = vol_settled_margin(c, cv);
        cluster_copy_put(c);
    }
    (void)pace_take_capped(PACE_SEND, bytes, margin, 1000000000ull);
}

/* ---- sfill_ops for cluster tsdbd */

static int sf_op_objlist(void *arg, uint32_t volume, uint64_t max_version, bool has_cursor,
                         const uint8_t cursor_oid[16], uint32_t max_entries,
                         uint8_t vuuid_out[16], struct sfill_obj **items_out, uint32_t *n_out, bool *more_out)
{
    (void)arg;
    *items_out = NULL;
    *n_out = 0;
    *more_out = false;
    uint32_t leader = 0;
    uint32_t self = 0;

    const struct cluster_copy *c = cluster_copy_get();
    if (!c)
        return TSDB_ERR_UNAVAILABLE;
    self = c->self;
    const struct cluster_copy_vol *cv = cluster_copy_vol(c, volume);
    if (cv && cv->redundancy == META_RED_EC && cv->leader != 0) {
        const struct cluster_copy_member *lm = cluster_copy_member(c, cv->leader);
        if (lm && (lm->state == META_UP || lm->state == META_DRAINING))
            leader = cv->leader;
    }
    cluster_copy_put(c);
    if (leader == 0)
        return TSDB_ERR_UNAVAILABLE;

    struct chunksrv *cs = chunksrv_cluster_hold();
    if (!cs)
        return TSDB_ERR_UNAVAILABLE;
    struct cs_objlist_item *raw = NULL;
    uint32_t n = 0;
    bool more = false;
    int rc = chunkpeer_objlist_page(cs, self, leader, volume, max_version, has_cursor, cursor_oid,
                                    max_entries, 5000, vuuid_out, &raw, &n, &more);
    if (rc == 0) {
        struct sfill_obj *out = n ? calloc(n, sizeof *out) : NULL;
        if (n && !out) {
            cs_objlist_items_free(raw, n);
            rc = TSDB_ERR_NOMEM;
        } else {
            for (uint32_t j = 0; j < n; j++) {
                memcpy(out[j].oid, raw[j].oid, 16);
                out[j].g = raw[j].g;
                out[j].positions = raw[j].positions;
                out[j].writer_epoch = raw[j].writer_epoch;
                out[j].stripe_version = raw[j].stripe_version;
                out[j].nsums = raw[j].nsums;
                out[j].sums = raw[j].sums;
                raw[j].sums = NULL; /* transferred ownership */
            }
            free(raw);
            *items_out = out;
            *n_out = n;
            *more_out = more;
        }
    }
    chunksrv_cluster_release();
    return rc;
}

static int sf_op_read_cell(void *arg, uint32_t node, uint32_t volume, uint64_t epoch,
                           const uint8_t vuuid[16], const uint8_t oid[16], unsigned pos,
                           uint64_t off, void *buf, size_t len)
{
    (void)arg;
    struct chunksrv *cs = chunksrv_cluster_hold();
    if (!cs)
        return CHUNK_ST_DOWN;
    const struct cluster_copy *c = cluster_copy_get();
    uint32_t self = self_id(c);
    cluster_copy_put(c);
    int st = chunkpeer_read_rebuild(cs, self, node, volume, epoch, vuuid, oid, pos, off, buf, len, 5000);
    chunksrv_cluster_release();
    return st;
}

static int sf_op_vol_view(void *arg, uint32_t volume, uint64_t *epoch_out,
                          uint32_t stripe_out[CHUNK_POS_MAX], unsigned *nstripe_out,
                          bool *prev_up_out, uint32_t prev_node)
{
    (void)arg;
    const struct cluster_copy *c = cluster_copy_get();
    if (!c)
        return -1;
    const struct cluster_copy_vol *cv = cluster_copy_vol(c, volume);
    int rc = -1;
    if (cv && cv->redundancy == META_RED_EC && cv->nstripe > 0 && cv->nstripe <= CHUNK_POS_MAX) {
        *epoch_out = cv->epoch;
        *nstripe_out = cv->nstripe;
        memcpy(stripe_out, cv->stripe, cv->nstripe * sizeof(uint32_t));
        bool pup = false;
        if (prev_node != 0) {
            const struct cluster_copy_member *pm = cluster_copy_member(c, prev_node);
            pup = pm && (pm->state == META_UP || pm->state == META_DRAINING);
        }
        *prev_up_out = pup;
        rc = 0;
    }
    cluster_copy_put(c);
    return rc;
}

static _Atomic(struct chunksrv *) sf_held_cs;

static chunk_backend *sf_op_be_hold(void *arg)
{
    (void)arg;
    struct chunksrv *cs = chunksrv_cluster_hold();
    if (!cs)
        return NULL;
    chunk_backend *be = chunksrv_backend(cs);
    if (!be) {
        chunksrv_cluster_release();
        return NULL;
    }
    atomic_store_explicit(&sf_held_cs, cs, memory_order_release);
    chunksrv_scrub_pause_all(cs, 1);
    return be;
}

static void sf_op_be_release(void *arg, chunk_backend *be)
{
    (void)arg;
    if (be) {
        struct chunksrv *cs = atomic_load_explicit(&sf_held_cs, memory_order_acquire);
        if (cs)
            chunksrv_scrub_pause_all(cs, -1);
        chunksrv_cluster_release();
    }
}

static void sf_op_notify_reopen(void *arg, uint32_t volume, const uint8_t oid[16])
{
    (void)arg;
    uint32_t reps[META_REPLICAS_MAX];
    uint32_t nreps = 0;
    uint32_t self = 0;

    const struct cluster_copy *c = cluster_copy_get();
    if (!c)
        return;
    self = c->self;
    const struct cluster_copy_vol *cv = cluster_copy_vol(c, volume);
    if (cv && cv->redundancy == META_RED_EC) {
        for (uint32_t i = 0; i < cv->nrep && nreps < META_REPLICAS_MAX; i++) {
            uint32_t id = cv->rep[i].node;
            const struct cluster_copy_member *m = cluster_copy_member(c, id);
            if (m && (m->state == META_UP || m->state == META_DRAINING))
                reps[nreps++] = id;
        }
    }
    cluster_copy_put(c);
    if (nreps == 0)
        return;

    struct chunksrv *cs = chunksrv_cluster_hold();
    if (!cs)
        return;
    for (uint32_t i = 0; i < nreps; i++)
        (void)chunkpeer_notify_reopen(cs, self, reps[i], volume, oid, 2000);
    chunksrv_cluster_release();
}

static struct {
    struct sfill *sf;
    struct sreconcile *rec;
    struct chunkrepair *cr;
    pthread_t thr;
    pthread_mutex_t mu;
    pthread_mutex_t rec_mu; /* protects SF.rec / SF.cr lookup from RPC handlers and report merge */
    pthread_mutex_t rep_mu; /* serializes merged HBS_FILL publication from sfill + sreconcile */
    pthread_cond_t cv;
    bool started;
    bool stop;
} SF = {
    .mu = PTHREAD_MUTEX_INITIALIZER,
    .rec_mu = PTHREAD_MUTEX_INITIALIZER,
    .rep_mu = PTHREAD_MUTEX_INITIALIZER,
};

static void cp_repair_enqueue(uint32_t volume, const uint8_t oid[16], unsigned pos, uint32_t node)
{
    pthread_mutex_lock(&SF.rec_mu);
    if (SF.cr)
        (void)chunkrepair_enqueue(SF.cr, volume, oid, pos, node);
    pthread_mutex_unlock(&SF.rec_mu);
}

static int cp_repair_lookup_live(void *arg, uint32_t volume, const uint8_t oid[16], struct chunkrepair_obj *out)
{
    (void)arg;
    memset(out, 0, sizeof *out);
    tsdb *db = atomic_load_explicit(&G_db, memory_order_acquire);
    if (!db || !cvol_led(volume) || !cluster_lease_valid())
        return TSDB_ERR_UNAVAILABLE;
    eng *e = tsdb_cvol_ref(db, volume);
    if (!e)
        return TSDB_ERR_UNAVAILABLE;
    if (!ecgc_oid_is_live(e, oid)) {
        tsdb_cvol_unref(&e);
        return TSDB_ERR_NOTFOUND;
    }
    const uint8_t *vu = tier_vuuid(e);
    if (!vu) {
        tsdb_cvol_unref(&e);
        return TSDB_ERR_UNAVAILABLE;
    }
    memcpy(out->vuuid, vu, 16);

    size_t np = 0;
    part **ps = db_snapshot(e, INT64_MIN, INT64_MAX, &np);
    if (!ps) {
        tsdb_cvol_unref(&e);
        return TSDB_ERR_NOMEM;
    }
    int rc = TSDB_ERR_NOTFOUND;
    for (size_t i = 0; i < np; i++) {
        struct ecpart *ep = atomic_load_explicit(&ps[i]->ec, memory_order_acquire);
        if (!ep || ep->test_mark || memcmp(ep->rec.oid, oid, 16) != 0)
            continue;
        const ec_record *r = &ep->rec;
        if (chunk_geom_make(&out->g, r->k, r->m, r->cell_bytes, r->object_len) != 0) {
            rc = TSDB_ERR_CORRUPT;
            break;
        }
        memcpy(out->oid, r->oid, 16);
        out->positions = r->positions;
        out->writer_epoch = r->writer_epoch;
        out->stripe_version = r->stripe_version;
        out->nsums = r->nsums;
        if (r->nsums > 0) {
            out->sums = malloc((size_t)r->nsums * sizeof(uint64_t));
            if (!out->sums) {
                rc = TSDB_ERR_NOMEM;
                break;
            }
            memcpy(out->sums, r->sums, (size_t)r->nsums * sizeof(uint64_t));
        }
        rc = 0;
        break;
    }
    db_release(ps, np);
    tsdb_cvol_unref(&e);
    return rc;
}

static int cp_repair_vol_view(void *arg, uint32_t volume, struct chunkrepair_vol_view *out)
{
    (void)arg;
    memset(out, 0, sizeof *out);
    if (!cvol_led(volume) || !cluster_lease_valid())
        return TSDB_ERR_UNAVAILABLE;
    uint64_t le = 0;
    if (repld_leading(volume, &le) != 1 || le == 0)
        return TSDB_ERR_UNAVAILABLE;
    const struct cluster_copy *c = cluster_copy_get();
    if (!c)
        return TSDB_ERR_UNAVAILABLE;
    const struct cluster_copy_vol *cv = cluster_copy_vol(c, volume);
    int rc = TSDB_ERR_UNAVAILABLE;
    if (cv && cv->redundancy == META_RED_EC && c->self != 0 && cv->leader == c->self && cv->nstripe > 0 &&
        cv->nstripe <= CHUNK_POS_MAX) {
        out->fence_epoch = le; /* leader's own pinned epoch (E14: deposed leader gets fenced) */
        out->stripe_version = cv->stripe_version;
        out->k = cv->ec_k;
        out->m = cv->ec_m;
        out->nstripe = cv->nstripe;
        out->is_leader = true;
        memcpy(out->stripe, cv->stripe, cv->nstripe * sizeof(uint32_t));
        for (uint32_t i = 0; i < cv->nstripe; i++) {
            out->stripe_filling[i] = cv->stripe_pos[i].state == META_STRIPE_FILLING ? 1 : 0;
            uint32_t prev = cv->stripe_pos[i].prev;
            out->stripe_prev[i] = prev;
            out->stripe_set_ver[i] = cv->stripe_pos[i].set_ver;
            if (prev != 0) {
                const struct cluster_copy_member *pm = cluster_copy_member(c, prev);
                out->stripe_prev_up[i] = (pm && (pm->state == META_UP || pm->state == META_DRAINING)) ? 1 : 0;
            }
        }
        rc = 0;
    }
    cluster_copy_put(c);
    return rc;
}

static bool cp_repair_is_filled(void *arg, uint32_t node, uint32_t volume, unsigned pos, uint64_t set_ver,
                                const uint8_t oid[16])
{
    (void)arg;
    return chunkpeer_is_filled(node, volume, pos, set_ver, oid);
}

static int cp_repair_check_and_put(void *arg, uint32_t node, const struct cs_put_meta *m, int fd, uint64_t base,
                                   uint64_t sum, bool skip_live_check)
{
    (void)arg;
    tsdb *db = atomic_load_explicit(&G_db, memory_order_acquire);
    if (!db)
        return TSDB_ERR_UNAVAILABLE;
    /* Re-verify leadership and live/not-retired/not-pending-MK_GC under the per-oid GC lock, then release BOTH the GC
     * lock and engine reference before network I/O so a slow/hung target node can neither stall tsdb_cvol_replace
     * (demote/transfer/install) nor block ecgcmk on a hash-stripe collision. Any delete racing after unlock is
     * prevented from resurrecting the chunk by the chunk server's atomic replace_only check under st->mu at open and
     * right before renameat (D9). */
    if (!skip_live_check) {
        if (!cvol_led(m->volume) || !cluster_lease_valid())
            return TSDB_ERR_UNAVAILABLE;
        eng *e = tsdb_cvol_ref(db, m->volume);
        if (!e)
            return TSDB_ERR_UNAVAILABLE;
        tier *t = e->tier;
        if (t)
            ecgc_oid_lock(t, m->volume, m->oid);
        bool live = ecgc_oid_is_live(e, m->oid);
        if (t)
            ecgc_oid_unlock(t, m->volume, m->oid);
        tsdb_cvol_unref(&e);
        if (!live)
            return TSDB_ERR_NOTFOUND;
    }
    int base_tmo = G.timeout_ms > 0 ? G.timeout_ms : 30000;
    uint64_t mb = cs_put_total(m) >> 20;
    int extra_ms = mb > 270u ? 270000 : (int)(mb * 1000u); /* bounded <= 270000 ms before cast to int */
    int tmo = base_tmo + extra_ms;
    uint64_t answered = 0;
    int rc = net_put(NULL, node, m, fd, base, sum, tmo, &answered);
    if (rc == 0) {
        uint8_t enc[CS_PUT_META_MAX];
        size_t encl = cs_put_meta_encode(m, enc);
        if (answered != cs_put_answer_hash_len(enc, encl, sum))
            rc = TSDB_ERR_IO;
    }
    return rc;
}

static void cp_repair_collect_results(void *ctx, struct creport_sink *sink)
{
    (void)ctx;
    struct chunkrepair_stats st;
    memset(&st, 0, sizeof st);
    bool have = false;
    pthread_mutex_lock(&SF.rec_mu);
    if (SF.cr) {
        chunkrepair_get_stats(SF.cr, &st);
        have = true;
    }
    pthread_mutex_unlock(&SF.rec_mu);
    if (!have)
        return;
    for (int i = 0; i < CHUNKREPAIR_RES_COUNT; i++) {
        struct creport_label lbl = {"result", chunkrepair_res_name(i)};
        creport_sink_put_u64(sink, &lbl, 1, st.results[i]);
    }
}

static void cp_repair_collect_bytes(void *ctx, struct creport_sink *sink)
{
    (void)ctx;
    struct chunkrepair_stats st;
    memset(&st, 0, sizeof st);
    bool have = false;
    pthread_mutex_lock(&SF.rec_mu);
    if (SF.cr) {
        chunkrepair_get_stats(SF.cr, &st);
        have = true;
    }
    pthread_mutex_unlock(&SF.rec_mu);
    if (have)
        creport_sink_put_u64(sink, NULL, 0, st.bytes_repaired);
}

static void cp_repair_collect_qdepth(void *ctx, struct creport_sink *sink)
{
    (void)ctx;
    size_t d = 0;
    bool have = false;
    pthread_mutex_lock(&SF.rec_mu);
    if (SF.cr) {
        d = chunkrepair_queue_depth(SF.cr);
        have = true;
    }
    pthread_mutex_unlock(&SF.rec_mu);
    if (have)
        creport_sink_put_u64(sink, NULL, 0, (uint64_t)d);
}

static void cp_reconcile_srv(void *arg, uint32_t volume, uint32_t pos, uint64_t stripe_version)
{
    (void)arg;
    pthread_mutex_lock(&SF.rec_mu);
    if (SF.rec)
        (void)sreconcile_trigger_ex(SF.rec, volume, pos, stripe_version);
    pthread_mutex_unlock(&SF.rec_mu);
}

static void publish_all_fills(void)
{
    pthread_mutex_lock(&SF.rep_mu);
    struct sfill_report sbuf[CLUSTER_REPORT_FILLS];
    struct sfill_report rbuf[CLUSTER_REPORT_FILLS];
    uint32_t ns = 0, nr = 0;
    pthread_mutex_lock(&SF.rec_mu);
    if (SF.sf)
        ns = sfill_report(SF.sf, sbuf, CLUSTER_REPORT_FILLS);
    if (SF.rec)
        nr = sreconcile_report(SF.rec, rbuf, CLUSTER_REPORT_FILLS);
    pthread_mutex_unlock(&SF.rec_mu);

    struct cluster_fill_report cf[CLUSTER_REPORT_FILLS];
    uint32_t n = 0;
    bool any_whole = false;
    for (uint32_t i = 0; i < ns && n < CLUSTER_REPORT_FILLS; i++) {
        cf[n++] = (struct cluster_fill_report){
            .volume = sbuf[i].volume,
            .pos = sbuf[i].pos,
            .version = sbuf[i].stripe_version,
            .done = sbuf[i].done,
            .total = sbuf[i].total,
            .whole = sbuf[i].whole,
        };
        if (sbuf[i].whole)
            any_whole = true;
    }
    for (uint32_t i = 0; i < nr && n < CLUSTER_REPORT_FILLS; i++) {
        bool dup = false;
        for (uint32_t j = 0; j < ns; j++) {
            if (sbuf[j].volume == rbuf[i].volume && sbuf[j].pos == rbuf[i].pos) {
                dup = true;
                break;
            }
        }
        if (dup)
            continue;
        cf[n++] = (struct cluster_fill_report){
            .volume = rbuf[i].volume,
            .pos = rbuf[i].pos,
            .version = rbuf[i].stripe_version,
            .done = rbuf[i].done,
            .total = rbuf[i].total,
            .whole = rbuf[i].whole,
        };
    }
    cluster_set_fills(n ? cf : NULL, n);
    pthread_mutex_unlock(&SF.rep_mu);
    if (any_whole)
        cluster_beat_soon();
}

static void sf_op_on_report(void *arg, const struct sfill_report *reps, uint32_t nreps)
{
    (void)arg;
    (void)reps;
    (void)nreps;
    publish_all_fills();
}

/* ---- sreconcile_ops for cluster tsdbd */

static int rec_op_local_list(void *arg, uint32_t volume, bool has_cursor, const uint8_t cursor_oid[16],
                             uint16_t cursor_pos, uint32_t max_entries, chunk_listed **items_out, uint32_t *n_out,
                             bool *more_out)
{
    (void)arg;
    const struct cluster_copy *c = cluster_copy_get();
    uint32_t self = self_id(c);
    cluster_copy_put(c);
    if (self == 0)
        return TSDB_ERR_UNAVAILABLE;
    struct chunksrv *cs = chunksrv_cluster_hold();
    if (!cs)
        return TSDB_ERR_UNAVAILABLE;
    int rc = chunkpeer_list_page(cs, self, self, volume, has_cursor, cursor_oid, cursor_pos, max_entries, 5000,
                                 items_out, n_out, more_out);
    chunksrv_cluster_release();
    return rc;
}

static bool rec_op_local_exists(void *arg, uint32_t volume, const uint8_t oid[16], unsigned pos)
{
    (void)arg;
    struct chunksrv *cs = chunksrv_cluster_hold();
    if (!cs)
        return false;
    chunk_backend *be = chunksrv_backend(cs);
    bool exists = false;
    if (be) {
        int nst = chunk_be_nstores(be);
        for (int i = 0; i < nst && !exists; i++) {
            chunk_store *st = chunk_be_store(be, i);
            if (!st)
                continue;
            chunk_listed cl;
            if (chunk_store_list_one(st, volume, oid, pos, false, &cl) == 0)
                exists = true;
        }
    }
    chunksrv_cluster_release();
    return exists;
}

static int rec_op_gc_pending(void *arg, uint32_t volume, uint8_t (**oids_out)[16], uint32_t *n_out,
                             bool *truncated_out)
{
    (void)arg;
    *oids_out = NULL;
    *n_out = 0;
    if (truncated_out)
        *truncated_out = false;
    uint32_t leader = 0, self = 0;
    const struct cluster_copy *c = cluster_copy_get();
    if (!c)
        return TSDB_ERR_UNAVAILABLE;
    self = c->self;
    const struct cluster_copy_vol *cv = cluster_copy_vol(c, volume);
    if (cv && cv->redundancy == META_RED_EC && cv->leader != 0) {
        const struct cluster_copy_member *lm = cluster_copy_member(c, cv->leader);
        if (lm && (lm->state == META_UP || lm->state == META_DRAINING))
            leader = cv->leader;
    }
    cluster_copy_put(c);
    if (leader == 0)
        return TSDB_ERR_UNAVAILABLE;
    struct chunksrv *cs = chunksrv_cluster_hold();
    if (!cs)
        return TSDB_ERR_UNAVAILABLE;
    int rc = chunkpeer_gcpending(cs, self, leader, volume, 5000, oids_out, n_out, truncated_out);
    chunksrv_cluster_release();
    return rc;
}

static int rec_op_vol_view(void *arg, uint32_t volume, uint32_t pos, struct sreconcile_vol_view *out)
{
    (void)arg;
    memset(out, 0, sizeof *out);
    const struct cluster_copy *c = cluster_copy_get();
    if (!c)
        return -1;
    const struct cluster_copy_vol *cv = cluster_copy_vol(c, volume);
    int rc = -1;
    if (cv && cv->redundancy == META_RED_EC && pos < cv->nstripe && pos < META_STRIPE_MAX && c->self != 0) {
        out->held_whole = (cv->stripe[pos] == c->self && cv->stripe_pos[pos].state == META_STRIPE_WHOLE);
        out->stripe_version = cv->stripe_version;
        out->leader = cv->leader;
        out->margin = vol_settled_margin(c, cv);
        out->concurrency = c->rebuild_concurrency;
        rc = 0;
    }
    cluster_copy_put(c);
    return rc;
}

static int rec_op_list_held(void *arg, struct sreconcile_held_pos **out, uint32_t *n_out)
{
    (void)arg;
    *out = NULL;
    *n_out = 0;
    const struct cluster_copy *c = cluster_copy_get();
    if (!c || c->self == 0) {
        if (c)
            cluster_copy_put(c);
        return -1;
    }
    uint32_t cap = 16, n = 0;
    struct sreconcile_held_pos *arr = malloc(cap * sizeof *arr);
    if (!arr) {
        cluster_copy_put(c);
        return -1;
    }
    for (uint32_t vi = 0; vi < c->nvolumes; vi++) {
        const struct cluster_copy_vol *v = &c->volumes[vi];
        if (v->redundancy != META_RED_EC || v->nstripe == 0)
            continue;
        for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++) {
            if (v->stripe[p] == c->self && v->stripe_pos[p].state == META_STRIPE_WHOLE) {
                if (n == cap) {
                    cap *= 2;
                    struct sreconcile_held_pos *na = realloc(arr, cap * sizeof *na);
                    if (!na) {
                        free(arr);
                        cluster_copy_put(c);
                        return -1;
                    }
                    arr = na;
                }
                arr[n++] = (struct sreconcile_held_pos){
                    .volume = v->number,
                    .pos = p,
                    .stripe_version = v->stripe_version,
                };
            }
        }
    }
    cluster_copy_put(c);
    *out = arr;
    *n_out = n;
    return 0;
}

static int rec_op_store_count(void *arg)
{
    (void)arg;
    struct chunksrv *cs = chunksrv_cluster_hold();
    if (!cs)
        return 0;
    int n = chunksrv_stores(cs, NULL, false);
    chunksrv_cluster_release();
    return n;
}

static int rec_op_store_info(void *arg, int idx, struct sreconcile_store_info *out)
{
    (void)arg;
    memset(out, 0, sizeof *out);
    struct chunksrv *cs = chunksrv_cluster_hold();
    if (!cs)
        return -1;
    int rc = chunksrv_store_info(cs, idx, out->path, sizeof out->path, &out->state, out->live_id);
    chunksrv_cluster_release();
    if (rc != 0)
        return -1;
    if (out->path[0])
        out->disk_ok = (chunk_store_meta_read(out->path, out->disk_id, NULL) == 0);
    return 0;
}

static int rec_op_place_store(void *arg, const uint8_t oid[16], unsigned pos)
{
    (void)arg;
    struct chunksrv *cs = chunksrv_cluster_hold();
    if (!cs)
        return -1;
    chunk_backend *be = chunksrv_backend(cs);
    int idx = (be && chunk_be_nstores(be) > 0) ? chunk_be_place(be, oid, pos) : -1;
    chunksrv_cluster_release();
    return idx;
}

static int rec_op_notify_holder(void *arg, uint32_t volume, uint32_t pos)
{
    (void)arg;
    uint32_t self = 0, holder = 0;
    uint64_t sver = 0;
    bool holder_up = false;
    bool done = false;

    const struct cluster_copy *c = cluster_copy_get();
    if (!c)
        return 0;
    self = c->self;
    const struct cluster_copy_vol *cv = cluster_copy_vol(c, volume);
    if (!cv || cv->redundancy != META_RED_EC || pos >= cv->nstripe || pos >= META_STRIPE_MAX ||
        cv->stripe_pos[pos].state != META_STRIPE_WHOLE) {
        done = true;
    } else {
        holder = cv->stripe[pos];
        sver = cv->stripe_version;
        const struct cluster_copy_member *hm = cluster_copy_member(c, holder);
        holder_up = hm && (hm->state == META_UP || hm->state == META_DRAINING);
    }
    cluster_copy_put(c);
    if (done)
        return 1;
    if (!holder_up || holder == 0)
        return 0;
    if (self != 0 && holder == self) {
        pthread_mutex_lock(&SF.rec_mu);
        if (SF.rec)
            (void)sreconcile_trigger_ex(SF.rec, volume, pos, sver);
        pthread_mutex_unlock(&SF.rec_mu);
        return 1;
    }
    struct chunksrv *cs = chunksrv_cluster_hold();
    if (!cs)
        return 0;
    int rc = chunkpeer_notify_reconcile(cs, self, holder, volume, pos, sver, 500);
    chunksrv_cluster_release();
    return rc == 0 ? 1 : 0;
}

static void rec_op_on_report(void *arg, const struct sfill_report *reps, uint32_t nreps)
{
    (void)arg;
    (void)reps;
    (void)nreps;
    publish_all_fills();
}

struct sf_key {
    uint32_t vol;
    uint32_t pos;
};

#ifdef TSDB_TEST_HOOKS
struct nomark_del_item {
    uint8_t oid[16];
    uint8_t vuuid[16];
};

struct nomark_del_ctx {
    uint32_t vol;
    unsigned pos;
    struct nomark_del_item *items;
    size_t n, cap;
};

static int nomark_list_cb(void *arg, const chunk_listed *c)
{
    struct nomark_del_ctx *ctx = arg;
    if (c->pos != ctx->pos)
        return 0;
    if (ctx->n == ctx->cap) {
        size_t nc = ctx->cap ? ctx->cap * 2 : 32;
        struct nomark_del_item *nv = realloc(ctx->items, nc * sizeof *nv);
        if (!nv)
            return TSDB_ERR_NOMEM;
        ctx->items = nv;
        ctx->cap = nc;
    }
    memcpy(ctx->items[ctx->n].oid, c->oid, 16);
    memcpy(ctx->items[ctx->n].vuuid, c->vuuid, 16);
    ctx->n++;
    return 0;
}

/* Planted bugs releasenomark / partialdelnow: deletes local chunks of (vol, pos) directly without MK_GC_RELEASE. */
static void nomark_delete_local_pos(uint32_t vol, unsigned pos, uint64_t epoch)
{
    struct chunksrv *cs = chunksrv_cluster_hold();
    if (!cs)
        return;
    chunk_backend *be = chunksrv_backend(cs);
    if (be) {
        struct nomark_del_ctx d = {.vol = vol, .pos = pos};
        if (chunk_be_list(be, vol, false, nomark_list_cb, &d) == 0) {
            uint64_t ep = epoch ? epoch : 1;
            uint64_t cur = chunk_be_epoch(be, vol);
            if (cur > ep)
                ep = cur;
            for (size_t i = 0; i < d.n; i++) {
                chunk_ident id = {.volume = vol, .pos = (uint16_t)pos};
                memcpy(id.oid, d.items[i].oid, 16);
                memcpy(id.vuuid, d.items[i].vuuid, 16);
                (void)chunk_be_del(be, &id, ep);
            }
        }
        free(d.items);
    }
    chunksrv_cluster_release();
}
#endif

struct sf_vol_state {
    uint32_t vol;
    uint64_t stripe_version;
    uint32_t filling_mask;
};

static void *sf_watch_main(void *arg)
{
    (void)arg;
    unsigned long seen = 0;
    bool have = false;
    bool rec_started = false;
    int prev_self_state = -1;
    uint64_t prev_lease_gen = 0;
    struct sf_key *active = NULL;
    uint32_t nactive = 0, cap_active = 0;
    struct sf_key *next_active = NULL;
    uint32_t cap_next = 0;
    struct sf_vol_state *vstates = NULL;
    uint32_t nvstates = 0, cap_vstates = 0;
    uint32_t *kick_vols = NULL;
    uint32_t cap_kick = 0;
    uint32_t *reopen_vols = NULL;
    uint32_t cap_reopen = 0;
#ifdef TSDB_TEST_HOOKS
    struct {
        uint32_t vol, pos;
        uint64_t epoch;
    } *nomark_del = NULL;
    uint32_t cap_nomark = 0;
#endif

    for (;;) {
        pthread_mutex_lock(&SF.mu);
        bool stop = SF.stop;
        pthread_mutex_unlock(&SF.mu);
        if (stop)
            break;

        unsigned long b = cluster_copy_builds();
        uint64_t cur_lease_gen = cluster_lease_gen();
        bool need_check = !have || b != seen || (!rec_started && chunksrv_cluster() != NULL) ||
                          (prev_lease_gen != 0 && cur_lease_gen != prev_lease_gen);
        if (need_check) {
            const struct cluster_copy *c = cluster_copy_get();
            if (c) {
                uint32_t nnext = 0;
                int cur_self_state = -1;
                uint32_t nkick = 0;
                uint32_t nreopen = 0;
#ifdef TSDB_TEST_HOOKS
                uint32_t nnomark = 0;
#endif
                if (c->self != 0) {
                    const struct cluster_copy_member *sm = cluster_copy_member(c, c->self);
                    if (sm)
                        cur_self_state = sm->state;
                    for (uint32_t vi = 0; vi < c->nvolumes; vi++) {
                        const struct cluster_copy_vol *v = &c->volumes[vi];
                        if (v->redundancy != META_RED_EC || v->nstripe == 0)
                            continue;
                        if (v->leader == c->self) {
                            if (nkick == cap_kick) {
                                uint32_t nc = cap_kick ? cap_kick * 2 : 64;
                                uint32_t *nv = realloc(kick_vols, (size_t)nc * sizeof *nv);
                                if (nv) {
                                    kick_vols = nv;
                                    cap_kick = nc;
                                }
                            }
                            if (nkick < cap_kick)
                                kick_vols[nkick++] = v->number;
                        }
                        uint32_t fmask = 0;
                        int margin = vol_settled_margin(c, v);
                        for (uint32_t p = 0; p < v->nstripe && p < META_STRIPE_MAX; p++) {
                            if (v->stripe_pos[p].state == META_STRIPE_FILLING && p < 32)
                                fmask |= 1u << p;
#ifdef TSDB_TEST_HOOKS
                            if ((ecgc_mutant == GC_MUT_RELEASE_NOMARK ||
                                 (place_mutant_env() == PLACE_MUT_PARTIAL_DEL_NOW &&
                                  v->stripe_pos[p].state == META_STRIPE_FILLING)) &&
                                v->stripe[p] != c->self && v->stripe_pos[p].prev == c->self) {
                                if (nnomark == cap_nomark) {
                                    uint32_t nc = cap_nomark ? cap_nomark * 2 : 32;
                                    void *nv = realloc(nomark_del, (size_t)nc * sizeof *nomark_del);
                                    if (nv) {
                                        nomark_del = nv;
                                        cap_nomark = nc;
                                    }
                                }
                                if (nnomark < cap_nomark) {
                                    nomark_del[nnomark].vol = v->number;
                                    nomark_del[nnomark].pos = p;
                                    nomark_del[nnomark].epoch = v->epoch;
                                    nnomark++;
                                }
                            }
#endif
                            if (v->stripe[p] == c->self && v->stripe_pos[p].state == META_STRIPE_FILLING) {
                                struct sfill_job_cfg cfg = {
                                    .volume = v->number,
                                    .pos = p,
                                    .stripe_version = v->stripe_version,
                                    .prev_node = v->stripe_pos[p].prev,
                                    .leader = v->leader,
                                    .ec_k = v->ec_k,
                                    .ec_m = v->ec_m,
                                    .margin = margin,
                                    .concurrency = c->rebuild_concurrency,
                                };
                                if (nnext == cap_next) {
                                    uint32_t ncap = cap_next ? cap_next * 2 : 32;
                                    struct sf_key *nv = realloc(next_active, (size_t)ncap * sizeof *nv);
                                    if (!nv)
                                        continue;
                                    next_active = nv;
                                    cap_next = ncap;
                                }
                                sreconcile_stop_pos(SF.rec, v->number, p);
                                /* Drain any in-flight CS_OP_DEL_REL on (vol, pos) before starting fill so
                                 * local_chunk_intact cannot race a guard-approved delete (r9-whole R2). */
                                struct chunksrv *cs = chunksrv_cluster_hold();
                                if (cs) {
                                    chunksrv_rel_barrier(cs, v->number, p);
                                    chunksrv_cluster_release();
                                }
                                if (sfill_start(SF.sf, &cfg) == 0)
                                    next_active[nnext++] = (struct sf_key){.vol = v->number, .pos = p};
                            }
                        }
                        /* Detect stripe_version or fill-state transitions (e.g. STRIPE_WHOLE / STRIPE_SET) so cached
                         * open objects reopen against the new stripe map and stale filled-object entries are purged. */
                        int vsi = -1;
                        for (uint32_t j = 0; j < nvstates; j++)
                            if (vstates[j].vol == v->number) {
                                vsi = (int)j;
                                break;
                            }
                        bool changed = false;
                        if (vsi < 0) {
                            if (nvstates == cap_vstates) {
                                uint32_t nc = cap_vstates ? cap_vstates * 2 : 32;
                                struct sf_vol_state *nv = realloc(vstates, (size_t)nc * sizeof *nv);
                                if (nv) {
                                    vstates = nv;
                                    cap_vstates = nc;
                                }
                            }
                            if (nvstates < cap_vstates)
                                vstates[nvstates++] = (struct sf_vol_state){
                                    .vol = v->number,
                                    .stripe_version = v->stripe_version,
                                    .filling_mask = fmask,
                                };
                        } else if (vstates[vsi].stripe_version != v->stripe_version ||
                                   vstates[vsi].filling_mask != fmask) {
                            vstates[vsi].stripe_version = v->stripe_version;
                            vstates[vsi].filling_mask = fmask;
                            changed = true;
                        }
                        if (changed) {
                            if (nreopen == cap_reopen) {
                                uint32_t nc = cap_reopen ? cap_reopen * 2 : 32;
                                uint32_t *nv = realloc(reopen_vols, (size_t)nc * sizeof *nv);
                                if (nv) {
                                    reopen_vols = nv;
                                    cap_reopen = nc;
                                }
                            }
                            if (nreopen < cap_reopen)
                                reopen_vols[nreopen++] = v->number;
                        }
                    }
                }
                uint32_t self_now = c->self;
                cluster_copy_put(c);

                tsdb *db = atomic_load_explicit(&G_db, memory_order_acquire);
                for (uint32_t i = 0; i < nreopen; i++) {
                    chunkpeer_filled_clear_vol(reopen_vols[i]);
                    if (db) {
                        eng *e = tsdb_cvol_ref(db, reopen_vols[i]);
                        if (e) {
                            if (e->tier)
                                eccache_reopen_vol(e->tier, reopen_vols[i]);
                            tsdb_cvol_unref(&e);
                        }
                    }
                }
                for (uint32_t i = 0; i < nkick; i++)
                    cp_gc_kick(kick_vols[i]);
#ifdef TSDB_TEST_HOOKS
                for (uint32_t i = 0; i < nnomark; i++)
                    nomark_delete_local_pos(nomark_del[i].vol, nomark_del[i].pos, nomark_del[i].epoch);
#endif

                /* Stop any previously active job no longer FILLING on this node. */
                for (uint32_t i = 0; i < nactive; i++) {
                    bool still = false;
                    for (uint32_t j = 0; j < nnext; j++)
                        if (next_active[j].vol == active[i].vol && next_active[j].pos == active[i].pos) {
                            still = true;
                            break;
                        }
                    if (!still)
                        sfill_stop(SF.sf, active[i].vol, active[i].pos);
                }
                /* Swap reusable active/next_active buffers in O(1) without allocating. */
                struct sf_key *tmp = active;
                uint32_t tmp_cap = cap_active;
                active = next_active;
                cap_active = cap_next;
                nactive = nnext;
                next_active = tmp;
                cap_next = tmp_cap;
                seen = b;
                have = true;

                /* R9 (D15): startup and return reconciliation once metadata copy has caught up and chunksrv serves. */
                if (!rec_started) {
                    if (self_now != 0 && cluster_users_caught_up() && chunksrv_cluster() != NULL) {
                        rec_started = true;
                        prev_self_state = cur_self_state;
                        prev_lease_gen = cur_lease_gen;
                        sreconcile_on_start_ex(SF.rec);
                    }
                } else {
                    bool state_returned = (prev_self_state == META_DOWN || prev_self_state == META_OUT) &&
                                          (cur_self_state == META_UP || cur_self_state == META_DRAINING);
                    bool lease_returned = (prev_lease_gen != 0 && cur_lease_gen != prev_lease_gen) &&
                                          (cur_self_state == META_UP || cur_self_state == META_DRAINING);
                    if (cur_self_state >= 0)
                        prev_self_state = cur_self_state;
                    if (cur_lease_gen != 0)
                        prev_lease_gen = cur_lease_gen;
                    if (state_returned || lease_returned)
                        sreconcile_on_return(SF.rec);
                }
            }
        }

        if (rec_started)
            sreconcile_kick_noted(SF.rec);

        pthread_mutex_lock(&SF.mu);
        if (!SF.stop) {
            struct timespec ts;
            tsdb_cond_deadline_ns(&ts, 100 * 1000000LL);
            pthread_cond_timedwait(&SF.cv, &SF.mu, &ts);
        }
        pthread_mutex_unlock(&SF.mu);
    }
    free(active);
    free(next_active);
    free(vstates);
    free(kick_vols);
    free(reopen_vols);
#ifdef TSDB_TEST_HOOKS
    free(nomark_del);
#endif
    return NULL;
}

int sfill_cluster_start(struct tsdb *db, const char *state_dir)
{
    if (SF.started)
        return 0;
    atomic_store_explicit(&G_db, db, memory_order_release);
    struct sfill_ops ops = {
        .objlist = sf_op_objlist,
        .read_cell = sf_op_read_cell,
        .vol_view = sf_op_vol_view,
        .local_be_hold = sf_op_be_hold,
        .local_be_release = sf_op_be_release,
        .notify_reopen = sf_op_notify_reopen,
        .on_report = sf_op_on_report,
        .arg = NULL,
    };
    struct sfill *sf = sfill_new_with_ops(&ops);
    if (!sf)
        return -1;
    struct sreconcile_ops rops = {
        .objlist = sf_op_objlist,
        .local_list = rec_op_local_list,
        .local_exists = rec_op_local_exists,
        .gc_pending = rec_op_gc_pending,
        .vol_view = rec_op_vol_view,
        .list_held = rec_op_list_held,
        .store_count = rec_op_store_count,
        .store_info = rec_op_store_info,
        .place_store = rec_op_place_store,
        .notify_holder = rec_op_notify_holder,
        .on_report = rec_op_on_report,
        .arg = NULL,
    };
    struct sreconcile *rec = sreconcile_new(sf, &rops, state_dir);
    if (!rec) {
        sfill_free(sf);
        return -1;
    }
    struct chunkrepair_cfg crcfg;
    memset(&crcfg, 0, sizeof crcfg);
    snprintf(crcfg.stage_dir, sizeof crcfg.stage_dir, "%s", G.stage_dir[0] ? G.stage_dir : "/tmp");
    crcfg.nworkers = CHUNKREPAIR_DEFAULT_WORKERS;
    crcfg.queue_max = CHUNKREPAIR_QUEUE_MAX;
    struct chunkrepair_ops crops = {
        .lookup_live = cp_repair_lookup_live,
        .vol_view = cp_repair_vol_view,
        .read_cell = sf_op_read_cell,
        .check_and_put = cp_repair_check_and_put,
        .is_filled = cp_repair_is_filled,
        .notify_reopen = sf_op_notify_reopen,
        .arg = NULL,
    };
    struct chunkrepair *cr = chunkrepair_new(&crcfg, &crops);
    if (!cr) {
        sreconcile_stop(rec);
        sfill_free(sf);
        sreconcile_free(rec);
        return -1;
    }
    pthread_mutex_lock(&SF.rec_mu);
    SF.sf = sf;
    SF.rec = rec;
    SF.cr = cr;
    pthread_mutex_unlock(&SF.rec_mu);

    (void)creport_metric_collector("tsdb_chunk_repairs_total", CREPORT_COUNTER,
                                   "Volume leader chunk repair outcomes by result.", cp_repair_collect_results, NULL);
    (void)creport_metric_collector("tsdb_chunk_repair_bytes_total", CREPORT_COUNTER,
                                   "Total chunk bytes written by volume leader chunk repairs.", cp_repair_collect_bytes,
                                   NULL);
    (void)creport_metric_collector("tsdb_chunk_repair_queue_depth", CREPORT_GAUGE,
                                   "Damaged chunks currently queued or actively being repaired on this leader.",
                                   cp_repair_collect_qdepth, NULL);

    pthread_mutex_lock(&SF.mu);
    tsdb_cond_init_mono(&SF.cv);
    SF.stop = false;
    pthread_mutex_unlock(&SF.mu);
    if (pthread_create(&SF.thr, NULL, sf_watch_main, NULL) != 0) {
        pthread_cond_destroy(&SF.cv);
        creport_metric_collector_del("tsdb_chunk_repairs_total");
        creport_metric_collector_del("tsdb_chunk_repair_bytes_total");
        creport_metric_collector_del("tsdb_chunk_repair_queue_depth");
        pthread_mutex_lock(&SF.rec_mu);
        SF.sf = NULL;
        SF.rec = NULL;
        SF.cr = NULL;
        pthread_mutex_unlock(&SF.rec_mu);
        chunkrepair_free(cr);
        sreconcile_stop(rec);
        sfill_free(sf);
        sreconcile_free(rec);
        return -1;
    }
    SF.started = true;
    return 0;
}

void sfill_cluster_stop(void)
{
    if (!SF.started)
        return;
    pthread_mutex_lock(&SF.mu);
    SF.stop = true;
    pthread_cond_signal(&SF.cv);
    pthread_mutex_unlock(&SF.mu);
    pthread_join(SF.thr, NULL);
    pthread_cond_destroy(&SF.cv);

    creport_metric_collector_del("tsdb_chunk_repairs_total");
    creport_metric_collector_del("tsdb_chunk_repair_bytes_total");
    creport_metric_collector_del("tsdb_chunk_repair_queue_depth");

    pthread_mutex_lock(&SF.rec_mu);
    struct sfill *sf = SF.sf;
    struct sreconcile *rec = SF.rec;
    struct chunkrepair *cr = SF.cr;
    SF.sf = NULL;
    SF.rec = NULL;
    SF.cr = NULL;
    pthread_mutex_unlock(&SF.rec_mu);

    /* Order: free chunkrepair workers, stop reconcile coordinator (no new tasks queued), free sfill (drains/drops
     * tasks calling on_task_done while rec is still alive), then free rec. */
    chunkrepair_free(cr);
    sreconcile_stop(rec);
    sfill_free(sf);
    sreconcile_free(rec);
    SF.started = false;
    cluster_set_fills(NULL, 0);
}

uint32_t sfill_cluster_progress(struct sfill_progress *out, uint32_t cap)
{
    if (!out || cap == 0)
        return 0;
    pthread_mutex_lock(&SF.rec_mu);
    struct sfill *sf = SF.sf;
    uint32_t n = sf ? sfill_get_progress(sf, out, cap) : 0;
    pthread_mutex_unlock(&SF.rec_mu);
    return n;
}

#else /* a build without the erasure codec: no cluster tier; an ec volume's open is refused by its META */

int chunkpeer_cluster_start(const char *stage_dir)
{
    (void)stage_dir;
    return 0;
}
void chunkpeer_cluster_stop(void) {}
struct chunkpeer *chunkpeer_cluster(void) { return NULL; }
int chunkpeer_cluster_prepare(uint32_t volume, const char *vdir)
{
    (void)volume;
    (void)vdir;
    return 0;
}
int sfill_cluster_start(struct tsdb *db, const char *state_dir)
{
    (void)db;
    (void)state_dir;
    return 0;
}
void sfill_cluster_stop(void) {}
uint32_t sfill_cluster_progress(struct sfill_progress *out, uint32_t cap)
{
    (void)out;
    (void)cap;
    return 0;
}

#endif
