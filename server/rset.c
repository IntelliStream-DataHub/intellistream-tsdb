/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Replica set changes in the replication driver (rset.h; R4 plan D8, design-cluster.md 8.1 step 5).
 *
 * A record whose set_version moved is not applied where it arrives (repld_update, the volume manager's thread): the
 * core may have actions queued in the old indexes, and the volume's thread may be in the middle of carrying them out
 * with rv->mu let go of (an APPLY, a SAVE). rset_change keeps the newest such record (rv->set->next) and has the
 * thread take it at its next META (rset_apply, at the head of IN_META, and at every tick while one waits): there the
 * core's queue is drained through the OLD record, then, under rv->mu in one step, the core is reconfigured
 * (repl_reconfig, old_of by node and rid: a reset member, same node with a new rid, is a new learner) and set to the
 * new version, and the driver's own per-index state moves with its members: the cursors rv->peer[] (acked, which the
 * leader's keep bound comes from, sent, want, heard...), rv->self, rv->nnodes and the record itself. An ISR request the
 * driver reports (rv->isr_req, a bitmask of old indexes) is dropped: the core asks again in the new ones. Until then
 * the record stays the old one, so every index the driver and the core use means the same node.
 *
 * A snapshot being installed or sent (rv->snap, rsnap.c) holds the change back: it finishes under the old set version
 * (or is abandoned by rsnap.c), never under a core reconfigured mid-install (task item 5).
 *
 * Every message the driver builds carries the core's set version (rset_stamp); a peer message of another version is
 * dropped before the driver looks at it (rset_msg_ok), counted, and said at most every 5 s with the count.
 *
 * Mutants (TSDB_REPLD_MUTANT): `setkeepold` keeps the old set (the stub's behaviour: a learner never joins),
 * `setnoversion` lets a message of any set version through the driver, `setdrvnoremap` remaps the core but not the
 * driver's cursors (test_cluster removeindex), `setoldofnode` maps a reset member (same node, new rid) to its old
 * index, carrying the old replica's ack over to the fresh one (test_cluster resetlost). TSDB_REPLD_TEST=setstale: while <dir>/setstale exists, this server's
 * drivers keep the set they have (test_cluster setstale: the others drop its messages).
 */
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "rset.h"

struct rset {
    int pending;                 /* `next` waits for the volume's thread */
    struct cluster_vol_rec next; /* the newest record of another set version */
    uint64_t drain_waited;       /* the next_gen the setdrainwait hook waited at */
    uint64_t next_gen;           /* bumped at every write of `next`: apply() works on a copy and starts over when it moved */
    uint64_t dropped;            /* peer messages of another set version, dropped */
    uint64_t dropped_said, said_ms;
    uint64_t defer_said;         /* the set version a deferral was said for */
    int stale_said;
};

static uint64_t ms_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

static struct rset *set_of(struct rvol *rv)
{
    if (!rv->set)
        rv->set = calloc(1, sizeof *rv->set);
    return rv->set;
}

static int held_stale(void)
{
    char f[sizeof G.dir + 16];
    if (!G.set_stale)
        return 0;
    snprintf(f, sizeof f, "%s/setstale", G.dir);
    return access(f, F_OK) == 0;
}

int rset_change(struct rvol *rv, const struct cluster_vol_rec *r)
{
    struct rset *s = rv->set;
    int pend = s && s->pending;
    if (r->set_version == rv->rec.set_version && !pend) {
        rec_set(rv, r); /* the same set: epoch, ISR, leader, addresses, a rid filled in at its first open */
        return 0;
    }
    if (r->set_version < (pend ? s->next.set_version : rv->rec.set_version))
        return 0; /* older than one already seen (a rebuilt copy): never back */
    if (G.mutant == RM_SETKEEPOLD) {
        if (!rv->set_changed)
            say("volume %" PRIu32 ": its replica set changed; this driver keeps the old one until a restart", r->number);
        rv->set_changed = 1;
        return 0;
    }
    if (!(s = set_of(rv)))
        return 0; /* no memory: the next push tries again */
    s->next = *r;
    __atomic_fetch_add(&s->next_gen, 1, __ATOMIC_RELAXED); /* the setdrainwait hook reads it with rv->mu let go */
    s->pending = 1;
    return 1;
}

/* 0 applied, 1 it waits. It works on a copy of `next`: drain() lets rv->mu go, and a push meanwhile (rset_change) may
 * write a newer record there, of another member list. Then self and old_of, computed from the record it began with,
 * no longer describe it: it starts over from the new one (mutant setnextstale reconfigures the core with the new
 * count and the old maps, and hands the driver the new record). */
static int apply(struct rvol *rv, struct rset *s)
{
    struct cluster_vol_rec cur;
    const struct cluster_vol_rec *n = &cur;
    uint64_t gen;
again:
    cur = s->next;
    gen = s->next_gen;
    if (held_stale()) {
        if (!s->stale_said)
            say("volume %" PRIu32 ": setstale: kept on replica set version %" PRIu64 " (the record has %" PRIu64 ")",
                rv->number, rv->rec.set_version, n->set_version);
        s->stale_said = 1;
        return 1;
    }
    int self = -1;
    for (uint32_t i = 0; i < n->nrep; i++)
        if (n->rep[i].node == G.self)
            self = (int)i;
    if (self < 0 || n->nrep > REPL_MAX_NODES || rv->snap) {
        if (s->defer_said != n->set_version)
            say("volume %" PRIu32 ": replica set version %" PRIu64 " waits: %s", rv->number, n->set_version,
                self < 0 ? "it no longer names this server (the volume manager closes the volume)"
                         : rv->snap ? "a snapshot of the volume is in progress under the old set" : "too many replicas");
        s->defer_said = n->set_version;
        return 1;
    }
    int old_of[REPL_MAX_NODES];
    for (uint32_t i = 0; i < n->nrep; i++) {
        old_of[i] = -1;
        for (uint32_t j = 0; j < rv->rec.nrep; j++)
            if (rv->rec.rep[j].node == n->rep[i].node &&
                (rv->rec.rep[j].rid == n->rep[i].rid || !rv->rec.rep[j].rid || !n->rep[i].rid ||
                 G.mutant == RM_SETOLDOFNODE)) /* planted: by node alone */
                old_of[i] = (int)j; /* a reset member (same node, a new rid) is new: -1 */
    }
    int rc = REPL_E_PENDING;
    for (int tries = 0; tries < 8 && rc == REPL_E_PENDING; tries++) {
        if (tries == 0 && G.set_drain_wait && s->drain_waited != gen) {
            s->drain_waited = gen; /* test hook: a drain that lets rv->mu go long enough for the next push */
            say("volume %" PRIu32 ": setdrainwait: replica set version %" PRIu64 " drains for up to 10 s", rv->number,
                n->set_version);
            pthread_mutex_unlock(&rv->mu);
            for (int k = 0; k < 1000 && __atomic_load_n(&s->next_gen, __ATOMIC_RELAXED) == gen; k++)
                usleep(10000);
            pthread_mutex_lock(&rv->mu);
        } else {
            rc = repl_reconfig(rv->core, (int)n->nrep, self, old_of);
            if (rc == REPL_E_PENDING)
                drain(rv); /* the queued actions go out through the old record, in the old indexes */
        }
        if (rv->failed || !rv->core || !s->pending)
            return 1; /* nothing for the caller to hand over */
        if (s->next_gen != gen) {
            if (G.mutant != RM_SETNEXTSTALE)
                goto again; /* a newer record came while the queue drained: nothing of this one was applied yet */
            for (uint32_t i = cur.nrep; i < s->next.nrep && i < REPL_MAX_NODES; i++)
                old_of[i] = 0; /* planted: the new member count with this record's self and old_of (past its count:
                                * whatever the stack held, 0 here) */
            cur.nrep = s->next.nrep;
            memcpy(cur.rep, s->next.rep, sizeof cur.rep);
            cur.set_version = s->next.set_version;
            gen = s->next_gen;
        }
    }
    if (rc == REPL_E_PENDING)
        return 1; /* the next tick */
    if (rc != REPL_OK) {
        /* a leader whose record's ISR holds a removed member: the group never removes an ISR member, so this leader
         * was deposed (src/repl.h). The volume stops (LOCAL_FAULT) and is opened again at the next start. */
        say("volume %" PRIu32 ": the core refuses replica set version %" PRIu64 " (%d)", rv->number, n->set_version, rc);
        s->pending = 0;
        vol_fail(rv, "a replica set change the core refused");
        return 1;
    }
    repl_set_version(rv->core, n->set_version);
    if (G.mutant != RM_SETDRVNOREMAP) {
        struct rpeer np[REPL_MAX_NODES];
        memset(np, 0, sizeof np);
        for (uint32_t i = 0; i < n->nrep; i++)
            if (old_of[i] >= 0)
                np[i] = rv->peer[old_of[i]];
        memcpy(rv->peer, np, sizeof np);
    }
    rv->isr_want = 0; /* a bitmask of old indexes: the core asks again */
    uint64_t was = rv->rec.set_version;
    /* each member with what the driver carries over for it: a peer's ack is the keep bound's input, and a test reads
     * it back (removeindex: the member that stays keeps its own ack, never the removed one's) */
    char nodes[REPL_MAX_NODES * 48 + 1];
    size_t o = 0;
    nodes[0] = 0;
    for (uint32_t i = 0; i < n->nrep && o < sizeof nodes - 48; i++) {
        o += (size_t)snprintf(nodes + o, sizeof nodes - o, "%s%" PRIu32, i ? ", " : "", n->rep[i].node);
        if ((int)i == self)
            o += (size_t)snprintf(nodes + o, sizeof nodes - o, " (this)");
        else if (old_of[i] < 0)
            o += (size_t)snprintf(nodes + o, sizeof nodes - o, " (new)");
        else
            o += (size_t)snprintf(nodes + o, sizeof nodes - o, " (acked %" PRIu64 ")", rv->peer[i].acked);
    }
    rec_set(rv, n);
    rv->self = self;
    rv->nnodes = (int)n->nrep;
    s->pending = 0;
    say("volume %" PRIu32 ": replica set version %" PRIu64 " -> %" PRIu64 ": nodes %s; this is replica %d of %d",
        rv->number, was, rv->rec.set_version, nodes, self, rv->nnodes);
    return 0;
}

int rset_apply(struct rvol *rv)
{
    struct rset *s = rv->set;
    if (!s || !s->pending || rv->failed || !rv->core)
        return 0;
    return apply(rv, s) ? 1 : 2;
}

int rset_msg_ok(struct rvol *rv, const struct repl_msg *m)
{
    uint64_t v = repl_get_set_version(rv->core);
    if (m->set_version == v || G.mutant == RM_SETNOVERSION)
        return 1;
    struct rset *s = set_of(rv);
    if (!s)
        return 0;
    s->dropped++;
    uint64_t now = ms_now();
    if (now >= s->said_ms + 5000) {
        say("volume %" PRIu32 ": dropped %" PRIu64 " messages of another replica set version (the last: version %"
            PRIu64 " from replica %d, this driver is at %" PRIu64 ")", rv->number, s->dropped - s->dropped_said,
            m->set_version, m->from, v);
        s->dropped_said = s->dropped;
        s->said_ms = now;
    }
    return 0;
}

void rset_stamp(struct rvol *rv, struct repl_msg *m) { m->set_version = repl_get_set_version(rv->core); }

uint64_t rset_dropped(const struct rvol *rv) { return rv->set ? rv->set->dropped : 0; }

void rset_cmd(struct rvol *rv, const struct cluster_volcmd *c, uint32_t from)
{
    if (!rv->said_cmd_drop)
        say("volume %" PRIu32 ": DROP_MEMBER of node %" PRIu32 " at epoch %" PRIu64 " (arg %" PRIu64 ") from node %"
            PRIu32 ": not built yet (r4-move); ignored", rv->number, c->node, c->epoch, c->arg, from);
    rv->said_cmd_drop = 1;
}
