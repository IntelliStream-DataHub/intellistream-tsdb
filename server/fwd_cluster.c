/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * tsdbd's forwarding links (fwd.h): one thread that makes the instance once this server is a member, registers its
 * peerlink for PL_KIND_FWD and PL_KIND_FWDQ in the cluster's acceptor table, and keeps its members to the metadata
 * copy (polled through cluster_copy_builds, a read of one shared counter, every 100 ms). Every node holds the links,
 * replicated volumes or not: forwarding needs them wherever a client may enter (ha.md 9.1).
 */
#include "fwd.h"

#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "cluster.h"
#include "../client/tsdb_crypto.h"

static struct {
    pthread_t thr;
    int started;
    _Atomic int stop;
    _Atomic(struct fwd *) f;
    void (*on_event)(void *arg, const struct fwd_ev *ev);
    void *arg;
} G;

/* Link up and down, of the links this node dialed. A peer that refuses the FWD kinds (a rolling upgrade from a version
 * without them) makes a link that goes down at every redial: the downs are logged once a second at most, with the count
 * of those left out. */
void fwd_cluster_log_event(void *arg, const struct fwd_ev *ev)
{
    (void)arg;
    if (ev->type != FWD_EV_LINK || !ev->dialed)
        return;
    static _Atomic time_t last;
    static atomic_uint skipped;
    if (!ev->up) {
        time_t t = time(NULL);
        if (atomic_exchange_explicit(&last, t, memory_order_relaxed) == t) {
            atomic_fetch_add_explicit(&skipped, 1, memory_order_relaxed);
            return;
        }
        unsigned k = atomic_exchange_explicit(&skipped, 0, memory_order_relaxed);
        if (k)
            fprintf(stderr, "tsdbd: fwd: %u link downs not logged in the last second\n", k);
    }
    if (1)
        fprintf(stderr, "tsdbd: fwd link to node %" PRIu32 " (%s) %s, connection %" PRIu64 "\n", ev->peer,
                ev->kind == PL_KIND_FWDQ ? "fwdq" : "fwd", ev->up ? "up" : "down", ev->gen);
}

static void members_of(struct fwd *f, const struct cluster_copy *c)
{
    struct fwd_member *m = calloc(c->nmembers + 1, sizeof *m);
    if (!m)
        return; /* the next build tries again */
    uint32_t n = 0;
    for (uint32_t i = 0; i < c->nmembers; i++) {
        const struct cluster_copy_member *cm = &c->members[i];
        if (cm->state >= META_OUT || !cm->peer[0])
            continue;
        m[n].id = cm->id;
        snprintf(m[n].peer, sizeof m[n].peer, "%s", cm->peer);
        n++;
    }
    fwd_set_members(f, m, n);
    free(m);
}

static void *fc_main(void *arg)
{
    (void)arg;
    unsigned long seen = 0;
    int have = 0;
    while (!atomic_load(&G.stop)) {
        struct fwd *f = atomic_load(&G.f);
        if (!f) {
            struct cluster_self cs;
            if (cluster_self(&cs) == 0 && cs.have_node) {
                struct fwd_cfg fc = {.secret = cs.secret, .secret_len = cs.secret_len, .arg = G.arg,
                                     .on_event = G.on_event ? G.on_event : fwd_cluster_log_event};
                fc.self.id = cs.id;
                memcpy(fc.self.uuid, cs.uuid, 16);
                memcpy(fc.self.ident, cs.ident, 16);
                snprintf(fc.advertise, sizeof fc.advertise, "%s", cs.advertise);
                f = fwd_new(&fc);
                tsdb_wipe(&cs, sizeof cs);
                if (f) {
                    cluster_set_peerlink_kind(PL_KIND_FWD, fwd_peerlink(f));
                    cluster_set_peerlink_kind(PL_KIND_FWDQ, fwd_peerlink(f));
                    atomic_store(&G.f, f);
                } else {
                    fprintf(stderr, "tsdbd: fwd: cannot make the forwarding links (tried again)\n");
                }
            }
        }
        unsigned long b = cluster_copy_builds();
        if (f && (!have || b != seen)) {
            const struct cluster_copy *c = cluster_copy_get();
            if (c) {
                members_of(f, c);
                cluster_copy_put(c);
                seen = b;
                have = 1;
            }
        }
        nanosleep(&(struct timespec){0, 100 * 1000000L}, NULL);
    }
    return NULL;
}

int fwd_cluster_start(void (*on_event)(void *arg, const struct fwd_ev *ev), void *arg)
{
    if (G.started)
        return 0;
    G.on_event = on_event;
    G.arg = arg;
    atomic_store(&G.stop, 0);
    if (pthread_create(&G.thr, NULL, fc_main, NULL) != 0)
        return -1;
    G.started = 1;
    return 0;
}

void fwd_cluster_stop(void)
{
    if (!G.started)
        return;
    atomic_store(&G.stop, 1);
    pthread_join(G.thr, NULL);
    G.started = 0;
    if (atomic_load(&G.f)) {
        /* the cluster thread is stopped (tsdbd stops it first): nothing hands a socket over any more */
        cluster_set_peerlink_kind(PL_KIND_FWD, NULL);
        cluster_set_peerlink_kind(PL_KIND_FWDQ, NULL);
    }
}

void fwd_cluster_free(void)
{
    fwd_free(atomic_exchange(&G.f, NULL)); /* tsdbd's workers are joined: no call into it is running */
}

struct fwd *fwd_cluster(void) { return atomic_load(&G.f); }
