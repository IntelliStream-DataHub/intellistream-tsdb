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
#include "../src/feed.h"
#include "../src/tier.h"
#include "chunksrv.h"
#include "cluster.h"
#include "meta.h"
#include "repld.h"
#include "sfill.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#ifdef TSDB_EC

static struct chunkpeer G;
static pthread_t src_th;
static atomic_bool src_stop, src_started;
/* the source thread lives until chunkpeer_cluster_stop: one that returned early (its work is done once the chunk server
 * is there) stayed a finished thread nobody joined until the clean stop, and ThreadSanitizer reports such a thread at
 * every other end of tsdbd (a fail-stop's exit(1)): status 66 instead of 1 (test_cluster exitthreads) */
static pthread_mutex_t src_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t src_cv = PTHREAD_COND_INITIALIZER;

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
            uint8_t meta[CS_PUT_META_LEN];
            cs_put_meta_encode(m, meta);
            *answered = cs_put_answer_hash(meta, sum);
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
        rc = 0;
    }
    cluster_copy_put(c);
    return rc;
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

static void *src_main(void *arg)
{
    (void)arg;
    while (!atomic_load(&src_stop)) {
        struct chunksrv *cs = chunksrv_cluster();
        if (cs) {
            G.local = cs;
            chunksrv_epoch_source(cs, chunkpeer_epoch_source, &G);
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
    G.net = (struct chunkpeer_net){.call = net_call, .put = net_put, .arg = &G};
    G.view = view;
    G.view_arg = &G;
    G.note_missing = sreconcile_note_missing;
    snprintf(G.stage_dir, sizeof G.stage_dir, "%s", stage_dir);
    tier_set_cluster(attach, &G, g_ec_cfg.cache_bytes >> 20);
    if (pthread_create(&src_th, NULL, src_main, NULL) != 0)
        return -1;
    atomic_store(&src_started, true);
    return 0;
}

void chunkpeer_cluster_stop(void)
{
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

#endif
