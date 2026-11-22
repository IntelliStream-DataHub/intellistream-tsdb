/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * tsdbd's chunk server (chunksrv.h) on the cluster, as fwd_cluster.c does it for the forwarding links: the stores of
 * `--chunk-dir` are opened at start (each one's flock: this process is their one owner; a store another process holds
 * refuses the start, a missing, damaged or failing one is DOWN and logged), checked against this member's cluster and
 * node (D11: a store whose META names another cluster or node refuses the start with a line naming both), and a thread
 * makes the peerlink once this server is a member, registers it for PL_KIND_CHUNK and PL_KIND_CHUNKB in the cluster's
 * acceptor table and keeps its peers to the metadata copy's members. Every member holds the links, stores or not: a
 * member without stores still calls the others.
 *
 * A server that is not a member yet at start (it waits for `tsdb cluster node add`) checks its stores when it learns
 * its node; a mismatch then cannot refuse the start, so the stores are closed (the flock released) and the member
 * serves none, with the same line.
 *
 * The number of stores up goes to the metadata group's report through cluster_set_chunk_stores (r8c-meta3) where that
 * call exists: it is referenced weakly, so this file links against a cluster.c without it.
 */
#include "chunksrv.h"

#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "cluster.h"
#include "peerlink.h"
#include "../client/tsdb_crypto.h"

#ifdef __ELF__
extern void cluster_set_chunk_stores(uint32_t n) __attribute__((weak));
#endif

static struct {
    pthread_t thr;
    int started;
    _Atomic int stop;
    struct chunksrv *cs; /* owned by the thread until it listens, then published in `pub` */
    _Atomic(struct chunksrv *) pub;
    uint64_t rate;
    int checked; /* the owner check ran */
    int last_up; /* stores up last reported (-1: never) */
} G;

static void report_up(struct chunksrv *cs)
{
    int up = 0;
    chunksrv_stores(cs, &up, false);
    if (up == G.last_up)
        return;
    if (G.last_up >= 0)
        fprintf(stderr, "tsdbd: chunk stores up: %d (was %d)\n", up, G.last_up);
    G.last_up = up;
#ifdef __ELF__
    if (cluster_set_chunk_stores)
        cluster_set_chunk_stores((uint32_t)up);
#endif
}

static void set_peers(struct chunksrv *cs, const struct cluster_copy *c, uint32_t self)
{
    for (uint32_t i = 0; i < c->nmembers; i++) {
        const struct cluster_copy_member *m = &c->members[i];
        if (m->id == self || m->state == META_OUT || !m->peer[0])
            continue;
        chunksrv_set_peer(cs, m->id, m->peer); /* a failure is tried again at the next build */
    }
}

/* The node became known after the start: a store of another owner is closed, the member serves none. */
static void check_late(const struct cluster_self *me)
{
    char err[512];
    if (chunksrv_check_owner(G.cs, me->uuid, me->id, err, sizeof err) == 0)
        return;
    fprintf(stderr, "tsdbd: %s; serving no chunk store\n", err);
    chunksrv_close(G.cs);
    G.cs = chunksrv_open(NULL, 0, err, sizeof err);
    if (!G.cs)
        fprintf(stderr, "tsdbd: chunk server: %s\n", err);
}

static void *cc_main(void *arg)
{
    (void)arg;
    unsigned long seen = 0;
    int have = 0;
    uint32_t self = 0;
    while (!atomic_load(&G.stop)) {
        struct chunksrv *cs = atomic_load(&G.pub);
        if (!cs && G.cs) {
            struct cluster_self me;
            if (cluster_self(&me) == 0 && me.have_node) {
                if (!G.checked) {
                    check_late(&me);
                    G.checked = 1;
                }
                struct chunksrv_net_cfg nc = {.self_id = me.id, .secret = me.secret, .secret_len = me.secret_len,
                                              .rate = G.rate};
                memcpy(nc.uuid, me.uuid, 16);
                memcpy(nc.ident, me.ident, 16);
                snprintf(nc.advertise, sizeof nc.advertise, "%s", me.advertise);
                if (G.cs && chunksrv_listen(G.cs, &nc) == 0) {
                    cluster_set_peerlink_kind(PL_KIND_CHUNK, chunksrv_peerlink(G.cs));
                    cluster_set_peerlink_kind(PL_KIND_CHUNKB, chunksrv_peerlink(G.cs));
                    self = me.id;
                    cs = G.cs;
                    atomic_store(&G.pub, cs);
                    fprintf(stderr, "tsdbd: chunk server up as node %" PRIu32 ", %d stores\n", me.id,
                            chunksrv_stores(cs, NULL, false));
                } else if (G.cs) {
                    fprintf(stderr, "tsdbd: chunk server: cannot make its links (tried again)\n");
                }
                tsdb_wipe(&me, sizeof me);
                tsdb_wipe(&nc, sizeof nc);
            }
        }
        if (cs) {
            unsigned long b = cluster_copy_builds();
            if (!have || b != seen) {
                const struct cluster_copy *c = cluster_copy_get();
                if (c) {
                    set_peers(cs, c, self);
                    cluster_copy_put(c);
                    seen = b;
                    have = 1;
                }
            }
            report_up(cs);
        }
        nanosleep(&(struct timespec){0, 100 * 1000000L}, NULL);
    }
    return NULL;
}

int chunksrv_cluster_start(const char *const *paths, int n, uint64_t rate, char *err, size_t errlen)
{
    if (G.started)
        return 0;
    G.cs = chunksrv_open(paths, n, err, errlen);
    if (!G.cs)
        return -1;
    chunksrv_stores(G.cs, NULL, n > 0);
    struct cluster_self me;
    if (cluster_self(&me) == 0 && me.have_node) { /* D11: known now, so a foreign store refuses the start */
        int rc = chunksrv_check_owner(G.cs, me.uuid, me.id, err, errlen);
        tsdb_wipe(&me, sizeof me);
        if (rc != 0) {
            chunksrv_close(G.cs);
            G.cs = NULL;
            return -1;
        }
        G.checked = 1;
    }
    G.rate = rate;
    G.last_up = -1;
    atomic_store(&G.stop, 0);
    if (pthread_create(&G.thr, NULL, cc_main, NULL) != 0) {
        snprintf(err, errlen, "cannot start the chunk server's thread");
        chunksrv_close(G.cs);
        G.cs = NULL;
        return -1;
    }
    G.started = 1;
    return 0;
}

void chunksrv_cluster_stop(void)
{
    if (!G.started)
        return;
    atomic_store(&G.stop, 1);
    pthread_join(G.thr, NULL);
    G.started = 0;
    if (atomic_load(&G.pub)) {
        /* the cluster thread is stopped (tsdbd stops it first): nothing hands a socket over any more */
        cluster_set_peerlink_kind(PL_KIND_CHUNK, NULL);
        cluster_set_peerlink_kind(PL_KIND_CHUNKB, NULL);
    }
}

void chunksrv_cluster_free(void)
{
    atomic_store(&G.pub, NULL);
    chunksrv_close(G.cs); /* tsdbd's workers are joined: no client call into it is running; unlistens first */
    G.cs = NULL;
    G.checked = 0;
}

struct chunksrv *chunksrv_cluster(void) { return atomic_load(&G.pub); }
