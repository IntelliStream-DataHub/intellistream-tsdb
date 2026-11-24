/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Planned leader transfer, the volume leader's side (R4 plan D14, D15; rtransfer.h).
 *
 * The Raft leader sends TRANSFER{vol, epoch, target} (cluster_volcmd, `arg` the transfer's id). The writer then stops
 * admitting: rtr_admit_ok answers 0 for every WAL record, client rows and the engine's own markers alike (NOW seal,
 * DROP, CLASS, SERIES, RETENTION, a large write's COMMIT), exactly as the lease refusal does, so nothing new is
 * appended and every refused write is definite (never admitted; tsdbd answers UNAVAILABLE PROMOTING with a retry after
 * about 50 ms, repld_draining). Once the commit point is the written end and the target acknowledged that end as
 * durable, READY{vol, epoch, end, target} goes to the current Raft leader (the cluster thread's, `.to = 0`: after a Raft
 * leader change the resends reach the new one, which answers a transfer it does not hold with CANCEL), and the Raft
 * leader that holds it promotes the target with that end as its floor (ctransfer.c). The drain waits at most transfer_wait_ms; a timeout, a lost epoch or a CANCEL before
 * READY admits again and says why once.
 *
 * After READY the writer keeps refusing: a promotion may be under way, and a record admitted now would lie past the end
 * the target is promoted with (a cut). Only CANCEL, a lost epoch (the fence, a promotion) or the hard cap of
 * 10 x transfer_wait_ms admit again; records admitted after the cap are counted as "resumed blind" (they may be cut).
 *
 * The VOLCMD frame carries no Raft term (cluster.h), so `arg` carries the transfer's id, which increases across Raft
 * leaders (term << 32 and up, ctransfer.c next_id): a command older than the latest this volume heard is ignored, so a
 * deposed Raft leader's late TRANSFER cannot start a drain the new one does not know of, and a CANCEL ends only the
 * transfer it names or an earlier one, never a later attempt's hold (D14). The new Raft leader sends CANCEL (term << 32)
 * to every volume leader in its first duty round (ctr_duty), and answers a READY of a transfer it does not hold with
 * a CANCEL of that id (ctr_ready).
 *
 * Everything runs under rv->mu (rtr_cmd and rtr_tick on the volume's thread, rtr_admit_ok from op_admit, and
 * repld_draining from tsdbd's error path).
 */
#include <inttypes.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "crashpt.h"
#include "repld.h"
#include "rtransfer.h"

enum { RT_IDLE = 0, RT_DRAIN, RT_READY };
#define READY_RESEND_MS 200
#define HARD_CAP_X 10
/* After READY's epoch is lost (the fence of the promotion) the volume is a replica here for the moment the target
 * needs to be promoted and the members' copies to name it: writes that reach this node meanwhile are told to retry
 * after about 50 ms (repld_draining) instead of the 200 ms of an unplanned promotion, which made up half of a
 * transfer's write pause. */
#define HANDOVER_MS 2000

struct rtr {
    uint64_t heard; /* the highest transfer id a TRANSFER or CANCEL of this volume came with */
    uint64_t id;    /* this transfer's (READY carries it) */
    int state;
    uint64_t epoch;  /* the core's epoch the transfer is for */
    uint32_t target; /* node id */
    uint32_t to;     /* the Raft leader that asked (only the mutant readypinned sends READY there) */
    uint64_t t0, ready_ms, sent_ms, end;
    int blind; /* past the hard cap: admitting, every record counted */
    int crashed;
    uint64_t handed_ms; /* when a READY transfer lost its epoch (0: none): HANDOVER_MS of short retries */
};

/* STATUS's counters (rtr_counts) */
static atomic_ulong n_timedout, n_blind, n_ready, n_cuts;

enum { RTM_NONE, RTM_NODRAIN, RTM_ADMIT, RTM_RESUME, RTM_PINNED };
/* TSDB_REPLD_MUTANT's names of this file (repld.c's parser knows none of them): READY at once, admitting during the
 * drain, admitting again after READY on its own timer (transfer_wait_ms), READY sent to the Raft leader that asked
 * instead of the current one */
static int rtr_mutant(void)
{
    static atomic_int m = -1;
    int v = atomic_load_explicit(&m, memory_order_relaxed);
    if (v < 0) {
        const char *e = getenv("TSDB_REPLD_MUTANT");
        v = !e                              ? RTM_NONE
            : !strcmp(e, "transfernodrain") ? RTM_NODRAIN
            : !strcmp(e, "transferadmit")   ? RTM_ADMIT
            : !strcmp(e, "transferresume")  ? RTM_RESUME
            : !strcmp(e, "readypinned")     ? RTM_PINNED
                                            : RTM_NONE;
        if (v != RTM_NONE)
            say("MUTANT %s (tests only)", e);
        atomic_store_explicit(&m, v, memory_order_relaxed);
    }
    return v;
}

static uint64_t mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

static struct rtr *tr_get(struct rvol *rv)
{
    if (!rv->tr) {
        rv->tr = calloc(1, sizeof *rv->tr);
        if (!rv->tr)
            abort(); /* a few bytes once per volume */
    }
    return rv->tr;
}

static uint64_t wait_ms(const struct rvol *rv) { return rv->rec.transfer_wait_ms ? rv->rec.transfer_wait_ms : 2000; }

static int tr_index(const struct rvol *rv, uint32_t node)
{
    for (uint32_t i = 0; i < rv->rec.nrep; i++)
        if (rv->rec.rep[i].node == node)
            return (int)i;
    return -1;
}

static void tr_end(struct rvol *rv, struct rtr *t, const char *why)
{
    t->handed_ms = t->state == RT_READY && !t->blind && !strcmp(why, "lost its epoch") ? mono_ms() : 0;
    if (t->state != RT_IDLE)
        say("volume %" PRIu32 ": transfer to node %" PRIu32 " at epoch %" PRIu64 " %s after %" PRIu64
            " ms; admits again",
            rv->number, t->target, t->epoch, why, mono_ms() - t->t0);
    t->state = RT_IDLE;
    t->blind = 0;
}

int rtr_admit_ok(struct rvol *rv)
{
    struct rtr *t = rv->tr;
    if (!t || t->state == RT_IDLE)
        return 1;
    if (t->blind) {
        atomic_fetch_add_explicit(&n_blind, 1, memory_order_relaxed);
        return 1;
    }
    return t->state == RT_DRAIN && rtr_mutant() == RTM_ADMIT;
}

int rtr_draining(struct rvol *rv)
{
    const struct rtr *t = rv->tr;
    if (t && t->handed_ms && rv->mode != M_WRITER && mono_ms() - t->handed_ms < HANDOVER_MS)
        return 1; /* handed over: a replica here until the copies name the new leader */
    return t && t->state != RT_IDLE && !t->blind;
}

void rtr_cmd(struct rvol *rv, const struct cluster_volcmd *c, uint32_t from)
{
    struct rtr *t = tr_get(rv);
    const char *what = c->kind == CL_VOLCMD_CANCEL ? "CANCEL" : "TRANSFER";
    if (c->arg < t->heard) {
        say("volume %" PRIu32 ": %s from node %" PRIu32 " of transfer %" PRIu64 " ignored (%" PRIu64
            " was heard)",
            rv->number, what, from, c->arg, t->heard);
        return;
    }
    t->heard = c->arg;
    if (c->kind == CL_VOLCMD_CANCEL) {
        if (t->state != RT_IDLE && c->arg >= t->id && (!c->epoch || c->epoch == t->epoch))
            tr_end(rv, t, "cancelled by the Raft leader");
        return;
    }
    uint64_t epoch = rv->core ? repl_epoch(rv->core) : 0;
    int ti = tr_index(rv, c->node);
    const char *no = rv->mode != M_WRITER || !rv->core || !repl_is_leader(rv->core) ? "this node does not lead it"
                     : c->epoch != epoch                                            ? "another epoch"
                     : ti < 0 || ti == rv->self                                     ? "the target is no other member"
                                                                                    : NULL;
    if (no) {
        say("volume %" PRIu32 ": TRANSFER to node %" PRIu32 " at epoch %" PRIu64 " refused: %s", rv->number, c->node,
            c->epoch, no);
        return;
    }
    if (t->state != RT_IDLE && t->epoch == c->epoch && t->target == c->node) {
        t->to = from; /* the same transfer asked again (a new Raft leader's) */
        t->id = c->arg;
        t->sent_ms = 0;
        return;
    }
    memset(t, 0, sizeof *t);
    t->heard = t->id = c->arg;
    t->state = RT_DRAIN;
    t->epoch = c->epoch;
    t->target = c->node;
    t->to = from;
    t->t0 = mono_ms();
    say("volume %" PRIu32 ": transfer to node %" PRIu32 " at epoch %" PRIu64 ": drains (written %" PRIu64
        ", commit %" PRIu64 ")",
        rv->number, c->node, c->epoch, repl_written(rv->core), repl_commit(rv->core));
}

static void send_ready(struct rvol *rv, struct rtr *t, uint64_t now)
{
    struct cluster_vmsg m = {.kind = CL_VMSG_READY, .volume = rv->number, .epoch = t->epoch, .durable = t->end,
                             .rid = t->target, .start = t->id, .to = rtr_mutant() == RTM_PINNED ? t->to : 0};
    cluster_vol_msg(&m);
    t->sent_ms = now;
}

/* TSDB_REPLD_TEST=drainhold: no READY while <dir>/drainhold exists (the drain goes on to a CANCEL or its timeout) */
static int drain_held(void)
{
    if (!G.drain_hold)
        return 0;
    char f[sizeof G.dir + 16];
    snprintf(f, sizeof f, "%s/drainhold", G.dir);
    return access(f, F_OK) == 0;
}

void rtr_tick(struct rvol *rv)
{
    struct rtr *t = rv->tr;
    if (!t || t->state == RT_IDLE)
        return;
    uint64_t now = mono_ms(), w = wait_ms(rv);
    if (rv->mode != M_WRITER || !rv->core || !repl_is_leader(rv->core) || repl_epoch(rv->core) != t->epoch) {
        /* the fence can reach this member before the 100 ms below (a Raft leader on this node fenced it in ~100 ms:
         * test_cluster transferkill-ready): READY has left all the same, and the promotion is still under way */
        if (t->state == RT_READY && !t->blind && !t->crashed) {
            t->crashed = 1;
            crash_point(CP_TRANSFER_READY);
        }
        tr_end(rv, t, "lost its epoch");
        return;
    }
    if (t->state == RT_DRAIN) {
        uint64_t written = repl_written(rv->core);
        int ti = tr_index(rv, t->target);
        int done = repl_commit(rv->core) >= written && ti >= 0 && repl_peer_match(rv->core, ti) >= written &&
                   !drain_held();
        if (done || rtr_mutant() == RTM_NODRAIN) {
            t->state = RT_READY;
            t->end = written;
            t->ready_ms = now;
            atomic_fetch_add_explicit(&n_ready, 1, memory_order_relaxed);
            say("volume %" PRIu32 ": transfer to node %" PRIu32 " at epoch %" PRIu64 ": READY at %" PRIu64
                " after %" PRIu64 " ms",
                rv->number, t->target, t->epoch, t->end, now - t->t0);
            send_ready(rv, t, now);
            return;
        }
        if (now - t->t0 >= w) {
            atomic_fetch_add_explicit(&n_timedout, 1, memory_order_relaxed);
            say("volume %" PRIu32 ": transfer to node %" PRIu32 ": the drain timed out (commit %" PRIu64
                ", written %" PRIu64 ", the target's durable end %" PRIu64 ")",
                rv->number, t->target, repl_commit(rv->core), written, ti >= 0 ? repl_peer_match(rv->core, ti) : 0);
            tr_end(rv, t, "timed out");
        }
        return;
    }
    /* RT_READY: only CANCEL, a lost epoch or the hard cap admit again */
    if (t->blind)
        return;
    if (!t->crashed && now - t->ready_ms >= 100) { /* READY has left (the cluster thread flushes every 50 ms) */
        t->crashed = 1;
        crash_point(CP_TRANSFER_READY);
    }
    uint64_t cap = rtr_mutant() == RTM_RESUME ? w : HARD_CAP_X * w;
    if (now - t->ready_ms >= cap) {
        say("volume %" PRIu32 ": transfer to node %" PRIu32 ": no promotion and no CANCEL %" PRIu64
            " ms after READY; admits again blind (such writes may be cut)",
            rv->number, t->target, now - t->ready_ms);
        t->blind = 1;
        return;
    }
    if (now - t->sent_ms >= READY_RESEND_MS)
        send_ready(rv, t, now);
}

int repld_draining(uint32_t v)
{
    struct rvol *rv = vol_find(v);
    if (!rv)
        return 0;
    pthread_mutex_lock(&rv->mu);
    int d = rtr_draining(rv);
    pthread_mutex_unlock(&rv->mu);
    return d;
}

void rtr_note_cut(void) { atomic_fetch_add_explicit(&n_cuts, 1, memory_order_relaxed); }

void repld_transfer_counts(uint64_t out[CL_TR_N])
{
    out[CL_TR_DRAIN_TIMEDOUT] = atomic_load_explicit(&n_timedout, memory_order_relaxed);
    out[CL_TR_BLIND] = atomic_load_explicit(&n_blind, memory_order_relaxed);
    out[CL_TR_READY] = atomic_load_explicit(&n_ready, memory_order_relaxed);
    out[CL_TR_CUTS] = atomic_load_explicit(&n_cuts, memory_order_relaxed);
}
