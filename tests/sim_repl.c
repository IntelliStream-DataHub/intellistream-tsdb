/* SPDX-License-Identifier: AGPL-3.0-only */
/* The replication core (src/repl.c) under seeded random schedules: docs/design-cluster.md §13.1.
 *
 * One volume of 3 to 5 replicas, each the real core driven by a fake server: a log on a fake disk (records are
 * durable only after a SYNC finished; a crash keeps the durable records and a random prefix of the others, as a
 * torn WAL tail is cut), the REPL state (saved durably and at once), a network that delays, reorders, loses and
 * duplicates messages and cuts links (one way or both, to the metadata group too), and a fake metadata group: one
 * linearizable record of the volume, failure detection by heartbeats, promotion by FENCE and the core's
 * repl_meta_promote(), ISR changes by repl_meta_isr_change(). Faults: crashes (with quick restarts, so a leader
 * comes back before anyone noticed), wiped disks (a new replica instance), partitions, pauses (a leader that
 * resumes after it was deposed), leadership moves. Clients write to whoever thinks it leads. Per run, at random: the
 * metadata group fences every replica rather than the ISR; the driver reports the end a SYNC was issued for rather
 * than what is durable now (wrong after a cut; the core must not believe it); slow syncs (up to 30 ticks, so they span
 * cuts); replicas trim their logs at the saved floor, so a lagging one gets a snapshot from its leader.
 *
 * The checker (after every event, incrementally):
 *  1. Every position any replica applies is the same record everywhere (applied logs are prefixes of one history),
 *     was durable on the applying replica, and is durable on every intact ISR member as the metadata records it.
 *     That is "no replica applies past the commit point", and with 2, "no acknowledged write is lost": an intact
 *     member is one whose replica instance is still the one the ISR names (a wiped disk is not), and wipes are
 *     only made while another intact member is left (at most 2 of 3 copies lost; a crash loses no durable data).
 *  2. A write acknowledged OK is committed: its record is the one at its positions in that history.
 *  3. No two replicas acknowledge in one epoch.
 *  4. A cut never removes a committed record, nor goes below what the replica applied; the core never fails.
 *  5. At the end every fault heals: a leader must take a write, every replica must rejoin the ISR, and every log must
 *     equal the leader's (a returning replica cut its divergent tail), applied to its end. A run may instead end
 *     waiting for an operator (every ISR disk lost) only after a leader's shrink of the ISR (D1), or when no replica
 *     holds every acknowledged write: a promotion never strands the volume while one does (A2).
 *  6. A replica sends in its saved fence's epoch only, and answers a fence only once it saved it (§6.2/§6.3).
 *  7. An acknowledged write is durable on at least min_isr replicas when the writer hears OK.
 *  8. Then every replica but the leader goes down for good: at min ISR 1 a write is still acknowledged (the lag
 *     rule shrinks the ISR to the leader, A2); at a higher min ISR it is not.
 *  9. Replica set changes (D8, D11; per run, at random, never during a failover): an ADD of a fresh learner on a new
 *     node (it fills from the leader's log, or by snapshot where the leader trimmed), a REMOVE of a member outside
 *     the ISR (the indexes above it shift), a RESET (a member outside the ISR wiped under a new rid: a remove and an
 *     add at one index), and a move (ADD B; once B joined, the leader is told repl_leave(A, the ISR's size before B
 *     joined); REMOVE A once it is out of the ISR). Each raises the set version. The driver reconfigures its core
 *     (repl_reconfig, repl_set_version) when a META of a newer version arrives, restarts it when the core refuses
 *     (a deposed leader), and drops an older one; the metadata group drops ISR requests, fence replies and
 *     resignations of another version. Checked: a peer message of another set version is never taken (the core's
 *     drop counter moves); a move's shrink (the ISR request the core made for the leave, REPL_WHY_LEAVE) never leaves
 *     the ISR below the move's min_isr (a lag drop during a move may, as it must be able to); a leader that knows a
 *     member is leaving never gets it into the ISR again; a removed member's copies count toward check 7 only when
 *     it was removed after the write (it may have been counted while it was a member).
 * 10. Replay starts (D17; per run): replicas raise their replay start to their floor before they move the log start
 *     up to it, and a snapshot installs the source's segments from its replay start (D3: log start S <= P + 1). The
 *     replay floor of the leader of the metadata's epoch is never above the first position at or above its replay
 *     start that any member still holds, down members and the leader included (an empty log holds nothing to
 *     replay). A snapshot is installed only when the core still wants it (repl_snapshot_wanted), and then above the
 *     log end.
 * 11. Planned transfers (D14; per run, at random, and to move off a leaving leader): the metadata group sends
 *     TRANSFER(target, an ISR member) to the leader; the leader's driver refuses every write from then on (definite:
 *     never admitted), waits up to XF_WAIT ticks for commit = written = the target's acknowledged durable end
 *     (repl_peer_match), then sends READY(end); the metadata group fences every ISR member, waits for every reply (up
 *     to XF_FENCE_WAIT) and promotes with all of them through repl_meta_promote_to(target, end); a promotion that does
 *     not happen is dropped, CANCEL goes to the old leader and the failover goes on as an ordinary one. The old leader
 *     admits again only on a CANCEL naming this transfer or a later one, a lost epoch (a fence deposes it), a drain
 *     that timed out before READY, or the hard cap (10 x XF_WAIT after READY). A new metadata leader (an event)
 *     forgets its transfers and sends CANCEL to every node. Faults of their own: in some runs the metadata leader
 *     stalls on a READY (up to 2 x XF_CAP: the hard cap ends the hold first, and the promotion then cuts what the old
 *     leader admitted after it), and TRANSFER and CANCEL take up to 170 ticks more (a CANCEL arrives after a later
 *     TRANSFER, or before its own). Checked, by the checker's own account of each hold (the TRANSFER and CANCEL
 *     delivered, the first READY sent, the core's epoch, and its own bounds CHK_XF_WAIT and CHK_XF_CAP, never the
 *     driver model's flags or timers): no write is admitted while a hold binds; after a successful transfer the new
 *     leader is the target and no member is `unanswered`; no replica of the new epoch cuts a record the old leader
 *     had written before READY (the drain left nothing uncommitted) or admitted after READY within the hard cap; a
 *     write admitted after the cap may be cut (counted; it was never acknowledged, which the TRUNCATE check proves).
 *     A run of XF_FLOOR_SEEDS seeds or more fails when no READY, no transfer done, no hard cap, no write after it,
 *     no cut of one or no stale CANCEL happened.
 *     Planned bugs of the driver model (not repl.c): 60 transfernodrain (READY at once, with the target's match as
 *     the end), 61 transferresume (the old leader's hold ends on its own timer, XF_WAIT after READY), 62
 *     transferfewreplies (only the target's fence reply goes to the promotion), 63 transferstalecancel (any CANCEL
 *     ends the hold, an earlier transfer's too). The simulator draws transfer ids from one counter; tsdbd needs them
 *     to increase across metadata leaders too (from the replicated state), or CANCEL(<= x) and the driver's ignored
 *     TRANSFERs mean nothing.
 *
 * `sim_repl --seeds N [--first S] [--ticks T]`; a failure prints the seed, `--seed S --trace` replays it. Built with
 * -DREPL_MUTANTS (build/sim_repl_mut), `--mutant M` plants bug M of repl.c and runs seeds until the checker
 * catches it (`make sim-repl-mutants`).
 */
#include "repl.h"

#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define NM      REPL_MAX_NODES
#define NSIM    6             /* members at most (set changes add up to this) */
#define MAXV    48            /* set versions per run */
#define META    NM            /* the metadata group's index in the link matrix */
#define SIM_HB  100           /* a server's heartbeat to the metadata group (not the core's) */

#define HB_TICKS      5
#define NODE_TIMEOUT  40
#define WATCH_TICKS   10
#define FENCE_WAIT    15
#define XF_WAIT       40      /* transfer_wait_ms: the drain's bound */
#define XF_CAP        (10 * XF_WAIT) /* after READY: the old leader admits again on its own only after this */
#define XF_READY_WAIT (XF_WAIT + 30) /* the metadata group cancels a transfer without READY after this */
#define XF_FENCE_WAIT (3 * FENCE_WAIT) /* a planned promotion waits this long for every fence reply */
/* The checker's own numbers for D14's hold (transfer_wait_ms and its hard cap of 10 x it): written out, never derived
 * from XF_WAIT and XF_CAP, so that a driver model that gets its timers wrong fails instead of moving the check along. */
#define CHK_XF_WAIT   40
#define CHK_XF_CAP    400
#define XF_FLOOR_SEEDS 2000 /* a run of this many seeds or more fails when a transfer path never ran */
#define SIM_TRANSFER  101     /* metadata -> leader: xid, xnid (the target) */
#define SIM_READY     102     /* leader -> metadata: xid, xend */
#define SIM_CANCEL    103     /* metadata -> node: every transfer up to xid */

/* Planned bugs of the simulator's own driver model (the transfer, check 11): bits 60-62 of --mutant. */
#ifdef REPL_MUTANTS
#define SMUT(n) ((repl_mutants >> (n)) & 1)
#else
#define SMUT(n) 0
#endif

struct srec {
    uint64_t pos, epoch, id, wseq;
    uint32_t n;
};

struct node {
    int nid;                  /* the node, for ever: indexes shift with set changes, messages are addressed by nid */
    uint64_t replay;          /* the replay start (D17): records below it went into parts, the segments may remain */
    struct repl *r;           /* NULL while down */
    struct repl_config cfg;
    uint64_t down_until, pause_until, wiped_at;
    struct srec *log;
    size_t n, cap, ndur;      /* records; the first ndur are durable */
    uint64_t wseq;
    uint64_t rid, fence, led, floor;   /* the REPL file */
    int sync_busy;
    uint64_t sync_at, sync_wseq, sync_target;
    uint64_t start;           /* first retained position: the records below went into "parts" (still in log[]) */
    int snap_src;             /* a snapshot from the node of this nid is due at snap_at (0: none asked) */
    uint64_t snap_at;
    uint64_t applied, last_epoch;
    size_t *pend;             /* indexes into ops of writes waiting for their reply */
    size_t npend, cpend;
    struct {                  /* the driver's planned transfer (D14), in memory only */
        int on, ready, tnid;
        uint64_t xid, epoch, at, ready_at, end;
    } xf;
    uint64_t xf_cancelled;    /* the highest transfer a CANCEL named (a TRANSFER it overtook is ignored) */
    struct {                  /* the checker's view of D14's hold, from the messages alone; never read by the driver */
        int on, ready, tnid;
        uint64_t xid, epoch, at, ready_at, cancelled;
    } xw;
};

struct smsg {
    int from_nid, to_nid;     /* -1: the metadata group */
    struct repl_msg m;
    struct repl_rec *rec;
    uint64_t *ids;
    uint64_t at;
    uint64_t xid, xend;       /* SIM_TRANSFER, SIM_READY, SIM_CANCEL */
    int xnid;
};

enum { OP_PENDING, OP_OK, OP_UNKNOWN };

struct op {
    uint64_t id, pos, at;
    uint32_t n;
    int node, status;
};

static struct sim {
    uint64_t seed, now, rng;
    int nn, trace, chaos, writes, min_isr;
    /* Per run: the metadata group fences every replica, not only the ISR (both legal); the driver reports the end a
     * SYNC was issued for, not what is durable now after a cut (a driver the core must survive); replicas trim their
     * logs below the floor (segments into parts), so a lagging one needs a snapshot. */
    int fence_all, naive_sync, trim;
    int replay_on, setchg;    /* replicas raise replay starts (D17); set changes happen (D8) */
    uint64_t setv, next_nid;
    int memv[MAXV][NM], nmemv[MAXV];   /* the nid of each member index, per set version */
    uint64_t removed_at[MAXV];
    int nremoved;
    struct {
        int phase;            /* 0 none, 1 B added, 2 B joined: A leaves */
        int a, b;             /* nids */
        uint32_t min;
        uint64_t remove_at;
    } mv;
    int shrunk;               /* a leader's shrink cut the ISR last; no promotion dropped an intact member since */
    struct node node[NM];
    struct smsg *q;
    size_t nq, cq;
    uint64_t cut[NM + 1][NM + 1];      /* link a -> b is down until this tick */
    uint32_t loss, dup, maxdelay, maxsync, nemesis, wrate;
    /* the metadata group */
    struct repl_vmeta vm;
    uint64_t last_hb[NM];
    int fo;
    uint64_t fo_epoch, fo_version, fo_start;
    uint32_t fo_fenced;
    int fo_off;               /* nid of a leaving leader this failover moves off (a planned transfer, D14), 0: none */
    struct repl_fence_reply rep[NM];
    int nrep;
    int fo_planned, fo_tnid;  /* a planned promotion (D14): its target (nid), READY's end, the transfer */
    uint64_t fo_end, fo_xid;
    struct {                  /* the metadata group's transfer: 1 TRANSFER sent, 2 fencing after READY */
        int phase, lnid, tnid;
        uint64_t xid, epoch, at;
    } xf;
    uint64_t next_xid;
    int xfer_on;              /* per run: transfers at random, xrate per mille a tick */
    int xf_answer;            /* per run: READY for a transfer the group does not hold is answered with CANCEL (else
                                 its leader holds until the hard cap) */
    uint32_t xrate;
    uint64_t meta_pause_until; /* the metadata leader stalls (a GC pause, a slow disk): it neither ticks nor reads */
    uint32_t xf_stall;        /* per run: per mille of READYs the metadata leader stalls on, for up to 2 x XF_CAP */
    uint32_t xf_slow;         /* per run: TRANSFER and CANCEL take up to this many ticks more (stale CANCELs) */
    uint64_t xdelay;          /* extra delay of the message being enqueued */
    uint64_t xstall_xid;      /* the transfer the last stall was on (one stall per transfer) */
    /* Check 11: the old leader's records above its commit point when it sent READY (none, with a drain) and every
     * write it admitted after READY (`late`: after CHK_XF_CAP, which a promotion may cut), and once its transfer
     * succeeded, the epoch whose replicas must not cut the others. */
    struct {
        uint64_t xid, epoch, *pos, *id;
        unsigned char *late;
        size_t n, cap;
        int on;
    } xc;
    /* clients */
    struct op *ops;
    size_t nops, cops;
    uint64_t next_rid, next_marker, cur_op;
    /* checker */
    uint64_t *committed;
    size_t ccap;
    uint64_t cmax;
    /* Per committed position: the epoch of the first replica to apply it (no lower than the epoch that committed
     * it), and whether a writer got OK for it. */
    uint64_t *cepoch;
    size_t cecap;
    unsigned char *cacked;
    size_t cacap;
    int *acker;
    size_t cacker;
    int bad;
    char why[512];
    struct {
        uint64_t acked, refused, unknown, promotions, stale, none, resigns, cuts, isr, crashes, wipes, parts, pauses,
            moves, msgs, applied, waits, refuses, xcuts, ack_isr1, ack_isrn, wiped_acked, wipe_leader,
            fence_rep_out, deg_runs, deg_ok, trims, snaps, lies, floors, adds, removes, resets, mvdone, reconfs,
            restarts, xver, leaves, leavereq, replays, planned, xfers, xready, xdone, xcancels, xdrainout, xcap,
            xrefused, xfailover, metachg, xstalls, xlate, xlatecuts, xstale;
    } st, tot;
} S;

/* ------------------------------------------------------------------ utilities */

static uint64_t rnd(void)
{
    uint64_t x = S.rng;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    return S.rng = x;
}

static uint32_t rn(uint32_t n)
{
    return n ? (uint32_t)(rnd() % n) : 0;
}

static int chance(uint32_t per_mille)
{
    return rn(1000) < per_mille;
}

static void *grow(void *p, size_t *cap, size_t need, size_t size)
{
    if (need <= *cap)
        return p;
    size_t c = *cap ? *cap : 16;
    while (c < need)
        c *= 2;
    p = realloc(p, c * size);
    if (!p) {
        fprintf(stderr, "sim_repl: out of memory\n");
        exit(2);
    }
    memset((char *)p + *cap * size, 0, (c - *cap) * size);
    *cap = c;
    return p;
}

static void violation(const char *fmt, ...)
{
    if (S.bad)
        return;
    S.bad = 1;
    va_list ap;
    va_start(ap, fmt);
    int k = snprintf(S.why, sizeof S.why, "tick %" PRIu64 ": ", S.now);
    vsnprintf(S.why + k, sizeof S.why - k, fmt, ap);
    va_end(ap);
}

static void trace(const char *fmt, ...)
{
    if (!S.trace)
        return;
    va_list ap;
    va_start(ap, fmt);
    printf("%6" PRIu64 " ", S.now);
    vprintf(fmt, ap);
    putchar('\n');
    va_end(ap);
}

static int popcount32(uint32_t x)
{
    int n = 0;
    for (; x; x &= x - 1)
        n++;
    return n;
}

static int slot_of(int nid)
{
    for (int i = 0; i < S.nn; i++)
        if (S.node[i].nid == nid)
            return i;
    return -1;
}

static int idx_in(uint64_t v, int nid)
{
    for (int i = 0; i < S.nmemv[v]; i++)
        if (S.memv[v][i] == nid)
            return i;
    return -1;
}

/* ------------------------------------------------------------------ the fake disk */

static long log_find(const struct node *nd, uint64_t pos)
{
    size_t lo = 0, hi = nd->n;
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        if (nd->log[mid].pos <= pos)
            lo = mid + 1;
        else
            hi = mid;
    }
    if (!lo || pos >= nd->log[lo - 1].pos + nd->log[lo - 1].n)
        return -1;
    return (long)lo - 1;
}

static uint64_t log_end(const struct node *nd)
{
    return nd->n ? nd->log[nd->n - 1].pos + nd->log[nd->n - 1].n - 1 : 0;
}

static uint64_t dur_end(const struct node *nd)
{
    return nd->ndur ? nd->log[nd->ndur - 1].pos + nd->log[nd->ndur - 1].n - 1 : 0;
}

static int rec_find(void *ctx, uint64_t pos, uint64_t *start, uint32_t *n)
{
    int s = slot_of((int)(intptr_t)ctx);
    if (s < 0)
        return -1;
    const struct node *nd = &S.node[s];
    if (pos < nd->start)
        return 1;
    long i = log_find(nd, pos);
    if (i < 0)
        return -1;
    *start = nd->log[i].pos;
    *n = nd->log[i].n;
    return 0;
}

/* ------------------------------------------------------------------ the checker */

static int intact(int i)
{
    return (S.vm.isr & (1u << i)) && S.node[i].rid == S.vm.rid[i];
}

/* Positions from..to are committed: node i, an intact ISR member, must hold them durably. */
static void check_node(int i, uint64_t from, uint64_t to)
{
    const struct node *nd = &S.node[i];
    for (uint64_t p = from; p <= to && !S.bad; p++) {
        long k = log_find(nd, p);
        if (k < 0 || (size_t)k >= nd->ndur || nd->log[k].id != S.committed[p])
            violation("ISR member %d lacks committed position %" PRIu64 " (record %" PRIu64 ", it has %s %" PRIu64
                      ")", i, p, S.committed[p], k < 0 ? "nothing" : (size_t)k >= nd->ndur ? "unsynced" : "record",
                      k < 0 ? 0 : nd->log[k].id);
    }
}

static void check_isr(uint64_t from)
{
    for (int i = 0; i < S.nn; i++)
        if (intact(i))
            check_node(i, from, S.cmax);
}

/* The replay floor (design-cluster.md 5.4): a leader's floor is never above a position some node still holds at or
 * above its replay start (and so may replay after a restart), down nodes included: a partition-class switch takes its
 * T past every row at or above it. */
static void check_floor(int l)
{
    uint64_t f = repl_replay_floor(S.node[l].r);
    if (f == 0)
        return;
    S.st.floors++;
    for (int i = 0; i < S.nn; i++) {
        const struct node *nd = &S.node[i];
        uint64_t from = nd->start > nd->replay ? nd->start : nd->replay;
        for (size_t k = 0; k < nd->n; k++) {
            if (nd->log[k].pos + nd->log[k].n <= from)
                continue; /* went into parts */
            uint64_t first = nd->log[k].pos > from ? nd->log[k].pos : from;
            if (first < f)
                violation("leader %d gives replay floor %" PRIu64 ", node %d still holds position %" PRIu64, l, f, i,
                          first);
            break;
        }
    }
}

static void check_apply(int i, uint64_t from, uint64_t to)
{
    const struct node *nd = &S.node[i];
    uint64_t old = S.cmax;
    for (uint64_t p = from; p <= to && !S.bad; p++) {
        long k = log_find(nd, p);
        if (k < 0) {
            violation("node %d applied position %" PRIu64 " which it does not have", i, p);
            return;
        }
        if ((size_t)k >= nd->ndur) {
            violation("node %d applied position %" PRIu64 " before it was durable", i, p);
            return;
        }
        if (p <= S.cmax) {
            if (S.committed[p] != nd->log[k].id)
                violation("node %d applied record %" PRIu64 " at %" PRIu64 ", committed is %" PRIu64, i,
                          nd->log[k].id, p, S.committed[p]);
        } else if (p == S.cmax + 1) {
            S.committed = grow(S.committed, &S.ccap, p + 1, sizeof *S.committed);
            S.committed[p] = nd->log[k].id;
            S.cepoch = grow(S.cepoch, &S.cecap, p + 1, sizeof *S.cepoch);
            S.cepoch[p] = repl_epoch(nd->r);
            S.cacked = grow(S.cacked, &S.cacap, p + 1, sizeof *S.cacked);
            S.cacked[p] = 0;
            S.cmax = p;
        } else {
            violation("node %d applied %" PRIu64 " past the committed history (%" PRIu64 ")", i, p, S.cmax);
        }
    }
    if (S.cmax > old)
        check_isr(old + 1);
}

/* ------------------------------------------------------------------ the network */

static int link_down(int a, int b)
{
    return S.cut[a][b] > S.now;
}

static void enqueue(struct smsg *m, int from, int to)
{
    if (to < 0 || link_down(from, to) || chance(S.loss)) {
        free(m->rec);
        free(m->ids);
        return;
    }
    S.st.msgs++;
    m->at = S.now + 1 + rn(S.maxdelay) + S.xdelay;
    S.q = grow(S.q, &S.cq, S.nq + 2, sizeof *S.q);
    S.q[S.nq++] = *m;
    if (chance(S.dup)) {
        struct smsg d = *m;
        if (m->m.nrec) {
            d.rec = malloc(m->m.nrec * sizeof *d.rec);
            d.ids = malloc(m->m.nrec * sizeof *d.ids);
            if (!d.rec || !d.ids)
                exit(2);
            memcpy(d.rec, m->rec, m->m.nrec * sizeof *d.rec);
            memcpy(d.ids, m->ids, m->m.nrec * sizeof *d.ids);
        }
        d.at = S.now + 1 + rn(S.maxdelay * 3) + S.xdelay;
        S.q[S.nq++] = d;
    }
}

static void send_from(int i, const struct repl_action *a)
{
    const struct node *nd = &S.node[i];
    int t = a->msg.type;
    if (t == REPL_M_REFUSE)
        S.st.refuses++;
    /* Check 6 (§6.2/§6.3): a replica saves a higher epoch as its fence (durably) before it acts on it, and never
     * acts in an epoch below its saved fence. Without it a crash right after the raise forgets the fence. */
    if ((t == REPL_M_APPEND || t == REPL_M_ACK || t == REPL_M_QUERY || t == REPL_M_ANSWER ||
         t == REPL_M_SNAPSHOT) && a->msg.epoch != nd->fence)
        violation("node %d sends type %d in epoch %" PRIu64 ", its saved fence is %" PRIu64, i, t, a->msg.epoch,
                  nd->fence);
    /* The replay floor (design-cluster.md 5.4): a node never reports a log start above the first position it still
     * holds, or a leader takes a partition-class switch past rows that node may still replay. */
    if ((t == REPL_M_ACK || t == REPL_M_FENCE_REPLY) && a->msg.start > (nd->start > 1 ? nd->start : 1))
        violation("node %d reports log start %" PRIu64 ", its log starts at %" PRIu64, i, a->msg.start, nd->start);
    if ((t == REPL_M_FENCE_REPLY || t == REPL_M_RESIGN) && a->msg.epoch > nd->fence)
        violation("node %d answers fence %" PRIu64 " before saving it (saved %" PRIu64 ")", i, a->msg.epoch,
                  nd->fence);
    /* Check 9: a move's shrink (D11) never leaves fewer than the move's min_isr members. */
    if (t == REPL_M_ISR_REQ && repl_isr_why(nd->r) == REPL_WHY_LEAVE) {
        S.st.leavereq++;
        if (S.mv.phase == 2 && (uint32_t)popcount32(a->msg.meta.isr) < S.mv.min)
            violation("node %d asks for isr %#x to let a member leave, below the move's min ISR %u", i,
                      a->msg.meta.isr, S.mv.min);
    }
    if (t == REPL_M_APPEND && a->upto > a->msg.prev_pos && a->msg.prev_pos + 1 < nd->start)
        violation("node %d sends from %" PRIu64 ", below its log start %" PRIu64, i, a->msg.prev_pos + 1,
                  nd->start);
    /* The core names members by index in the set version it stamps; the network delivers to a node. */
    struct smsg m = {.m = a->msg, .from_nid = nd->nid, .to_nid = -1};
    int to = META;
    if (a->msg.to != REPL_META) {
        uint64_t v = a->msg.set_version;
        if (v >= MAXV || a->msg.to < 0 || a->msg.to >= S.nmemv[v]) {
            violation("node %d sends to member %d of set version %" PRIu64 ", which has no such member", i, a->msg.to,
                      v);
            return;
        }
        m.to_nid = S.memv[v][a->msg.to];
        to = slot_of(m.to_nid); /* -1: removed since */
    }
    m.m.rec = NULL;
    m.m.nrec = 0;
    if (a->msg.type == REPL_M_APPEND && a->upto > a->msg.prev_pos) {
        long k = log_find(nd, a->msg.prev_pos + 1);
        if (k < 0 || nd->log[k].pos != a->msg.prev_pos + 1) {
            violation("node %d sends from %" PRIu64 ", not a record start", i, a->msg.prev_pos + 1);
            return;
        }
        size_t e = (size_t)k;
        while (e < nd->n && nd->log[e].pos <= a->upto)
            e++;
        if (nd->log[e - 1].pos + nd->log[e - 1].n - 1 != a->upto) {
            violation("node %d sends up to %" PRIu64 ", not a record end", i, a->upto);
            return;
        }
        m.m.nrec = (uint32_t)(e - (size_t)k);
        m.rec = malloc(m.m.nrec * sizeof *m.rec);
        m.ids = malloc(m.m.nrec * sizeof *m.ids);
        if (!m.rec || !m.ids)
            exit(2);
        for (uint32_t j = 0; j < m.m.nrec; j++) {
            const struct srec *s = &nd->log[(size_t)k + j];
            m.rec[j] = (struct repl_rec){.pos = s->pos, .epoch = s->epoch, .n = s->n};
            m.ids[j] = s->id;
        }
    }
    enqueue(&m, i, to);
}

static void meta_send(int to, int type, uint64_t epoch)
{
    struct smsg m = {.from_nid = -1, .to_nid = S.node[to].nid};
    m.m.type = type;
    m.m.from = REPL_META;
    m.m.to = to;
    m.m.set_version = S.setv;
    m.m.epoch = epoch;
    m.m.leader = -1;
    m.m.meta = S.vm;
    enqueue(&m, META, to);
}

static void meta_broadcast(void)
{
    for (int i = 0; i < S.nn; i++)
        meta_send(i, REPL_M_META, S.vm.epoch);
}

/* ------------------------------------------------------------------ driving one replica */

static void reply(int i, const struct repl_action *a)
{
    struct node *nd = &S.node[i];
    for (size_t k = 0; k < nd->npend; k++) {
        struct op *o = &S.ops[nd->pend[k]];
        if (o->pos != a->first)
            continue;
        nd->pend[k] = nd->pend[--nd->npend];
        if (o->pos + o->n - 1 != a->pos)
            violation("node %d replied for %" PRIu64 "..%" PRIu64 ", the write was %u positions", i, a->first, a->pos,
                      o->n);
        if (a->status != REPL_OK) {
            o->status = OP_UNKNOWN;
            S.st.unknown++;
            return;
        }
        o->status = OP_OK;
        S.st.acked++;
        for (uint64_t p = o->pos; p <= a->pos; p++)
            if (p > S.cmax || S.committed[p] != o->id)
                violation("write %" PRIu64 " acknowledged at %" PRIu64 " by node %d is not the committed record",
                          o->id, p, i);
            else
                S.cacked[p] = 1;
        if (popcount32(S.vm.isr) > 1)
            S.st.ack_isrn++;
        else
            S.st.ack_isr1++;
        /* Check 7 (min ISR): an acknowledged write is durable on at least min_isr replicas. A disk lost since the
         * write may have held a copy when the leader counted it (its ACK was in flight): it counts. */
        for (uint64_t p = o->pos; p <= a->pos && !S.bad; p++) {
            int copies = 0;
            for (int j = 0; j < S.nn; j++) {
                long k = log_find(&S.node[j], p);
                copies += (k >= 0 && (size_t)k < S.node[j].ndur && S.node[j].log[k].id == o->id) ||
                          (S.node[j].wiped_at && S.node[j].wiped_at >= o->at);
            }
            for (int q = 0; q < S.nremoved; q++)
                copies += S.removed_at[q] >= o->at;
            if (copies < S.min_isr)
                violation("write %" PRIu64 " acknowledged at %" PRIu64 " with %d durable copies, min ISR %d", o->id,
                          p, copies, S.min_isr);
        }
        uint64_t e = repl_epoch(nd->r);
        S.acker = grow(S.acker, &S.cacker, e + 1, sizeof *S.acker);
        if (S.acker[e] && S.acker[e] != nd->nid)
            violation("nodes of nid %d and %d both acknowledged in epoch %" PRIu64, S.acker[e], nd->nid, e);
        S.acker[e] = nd->nid;
        trace("node %d: OK write %" PRIu64 " at %" PRIu64 " epoch %" PRIu64, i, o->id, o->pos, e);
        return;
    }
    violation("node %d replied for %" PRIu64 ", which no writer waits for", i, a->first);
}

/* ------------------------------------------------------------------ check 11: D14's hold, as the checker sees it */

/* The checker keeps its own account of each leader's hold, from what the driver must obey (the TRANSFER and CANCEL it
 * was delivered, the READY it sent, its core's epoch) and the spec's bounds (CHK_XF_WAIT, CHK_XF_CAP), never from the
 * driver model's flags: a driver that lets go too early is caught by the write it admits. */
static void xc_add(uint64_t pos, uint64_t id, unsigned char late)
{
    if (S.xc.n == S.xc.cap) {
        S.xc.cap = S.xc.cap ? 2 * S.xc.cap : 16;
        S.xc.pos = realloc(S.xc.pos, S.xc.cap * sizeof *S.xc.pos);
        S.xc.id = realloc(S.xc.id, S.xc.cap * sizeof *S.xc.id);
        S.xc.late = realloc(S.xc.late, S.xc.cap * sizeof *S.xc.late);
        if (!S.xc.pos || !S.xc.id || !S.xc.late)
            exit(2);
    }
    S.xc.pos[S.xc.n] = pos;
    S.xc.id[S.xc.n] = id;
    S.xc.late[S.xc.n++] = late;
}

/* A TRANSFER for its epoch, newer than any CANCEL and any transfer it holds, binds the leader it reaches. */
static void chk_transfer(int i, const struct smsg *sm)
{
    struct node *nd = &S.node[i];
    if (!nd->r || !repl_is_leader(nd->r) || repl_epoch(nd->r) != sm->m.epoch || sm->xid <= nd->xw.cancelled ||
        (nd->xw.on && nd->xw.xid >= sm->xid))
        return;
    nd->xw.on = 1;
    nd->xw.ready = 0;
    nd->xw.xid = sm->xid;
    nd->xw.epoch = sm->m.epoch;
    nd->xw.at = S.now;
    nd->xw.tnid = sm->xnid;
}

/* A CANCEL ends the hold of its transfer and of every earlier one, never of a later one. */
static void chk_cancel(int i, const struct smsg *sm)
{
    struct node *nd = &S.node[i];
    if (sm->xid > nd->xw.cancelled)
        nd->xw.cancelled = sm->xid;
    if (nd->xw.on && nd->xw.xid <= sm->xid)
        nd->xw.on = 0;
}

/* The leader sends its first READY for the transfer it holds: from now on only the hard cap lets it admit on its own.
 * Check 11's record starts with what it has above its commit point (nothing, after a drain); it replaces the last
 * transfer's (one transfer is checked at a time). */
static void chk_ready(int i, uint64_t xid)
{
    struct node *nd = &S.node[i];
    if (!nd->xw.on || nd->xw.xid != xid || nd->xw.ready)
        return;
    nd->xw.ready = 1;
    nd->xw.ready_at = S.now;
    uint64_t c = repl_commit(nd->r);
    S.xc.on = 0;
    S.xc.xid = xid;
    S.xc.n = 0;
    for (size_t k = nd->n; k > 0 && nd->log[k - 1].pos + nd->log[k - 1].n - 1 > c; k--)
        xc_add(nd->log[k - 1].pos, nd->log[k - 1].id, 0);
}

/* Whether node i's hold still binds. It ends (lazily, here) with the epoch it was given for, and before READY at the
 * drain's bound or once the target left the set. */
static int chk_held(int i)
{
    struct node *nd = &S.node[i];
    if (!nd->xw.on)
        return 0;
    if (!nd->r || !repl_is_leader(nd->r) || repl_epoch(nd->r) != nd->xw.epoch ||
        (!nd->xw.ready && (S.now - nd->xw.at >= CHK_XF_WAIT ||
                           idx_in(repl_get_set_version(nd->r), nd->xw.tnid) < 0))) {
        nd->xw.on = 0;
        return 0;
    }
    return 1;
}

/* A client write the core admitted on node i (pos: its first position). */
static void chk_admit(int i, uint64_t id, uint64_t pos)
{
    struct node *nd = &S.node[i];
    if (!chk_held(i))
        return;
    int late = nd->xw.ready && S.now - nd->xw.ready_at >= CHK_XF_CAP;
    if (nd->xw.ready && S.xc.xid == nd->xw.xid)
        xc_add(pos, id, late ? 2 : 1);
    if (late) {
        S.st.xlate++; /* D14: after the hard cap; a promotion that still comes may cut it (never acknowledged) */
        return;
    }
    if (nd->xw.ready)
        violation("node %d admits write %" PRIu64 " %" PRIu64 " ticks after READY of transfer %" PRIu64
                  " (hard cap %d)", i, id, S.now - nd->xw.ready_at, nd->xw.xid, CHK_XF_CAP);
    else
        violation("node %d admits write %" PRIu64 " while transfer %" PRIu64 " holds it (TRANSFER %" PRIu64
                  " ticks ago, no READY)", i, id, nd->xw.xid, S.now - nd->xw.at);
}

static void drive(int i, const struct smsg *cur)
{
    struct node *nd = &S.node[i];
    struct repl_action a;
    while (nd->r && repl_next_action(nd->r, &a)) {
        switch (a.type) {
        case REPL_A_WRITE: {
            if (a.rec.pos != log_end(nd) + 1 || a.rec.n == 0) {
                violation("node %d writes at %" PRIu64 ", its log ends at %" PRIu64, i, a.rec.pos, log_end(nd));
                return;
            }
            uint64_t id;
            if (a.src == REPL_SRC_CLIENT) {
                id = S.cur_op;
                chk_admit(i, id, a.rec.pos);
            } else if (a.src == REPL_SRC_EPOCH) {
                id = ++S.next_marker | 1ull << 62;
                trace("node %d: EPOCH %" PRIu64 " marker at %" PRIu64, i, a.rec.epoch, a.rec.pos);
            } else {
                if (!cur || a.idx >= cur->m.nrec || cur->rec[a.idx].pos != a.rec.pos) {
                    violation("node %d writes a record the message does not have", i);
                    return;
                }
                id = cur->ids[a.idx];
            }
            nd->log = grow(nd->log, &nd->cap, nd->n + 1, sizeof *nd->log);
            nd->log[nd->n++] = (struct srec){.pos = a.rec.pos, .epoch = a.rec.epoch, .id = id, .wseq = ++nd->wseq,
                                             .n = a.rec.n};
            break;
        }
        case REPL_A_SYNC:
            if (!nd->sync_busy) {
                nd->sync_busy = 1;
                nd->sync_at = S.now + 1 + rn(S.maxsync);
                nd->sync_wseq = nd->wseq;
                nd->sync_target = log_end(nd);
            }
            break;
        case REPL_A_TRUNCATE: {
            S.st.cuts++;
            long k = a.pos ? log_find(nd, a.pos) : -1;
            if (a.pos && (k < 0 || nd->log[k].pos + nd->log[k].n - 1 != a.pos)) {
                violation("node %d cuts at %" PRIu64 ", not a record end", i, a.pos);
                return;
            }
            if (a.pos < nd->applied)
                violation("node %d cuts at %" PRIu64 " below its applied end %" PRIu64, i, a.pos, nd->applied);
            /* An acknowledged record is never cut. A committed one is never cut by a replica of the epoch that
             * committed it or a later one (every leader from then on has it). A replica that still follows an
             * older epoch's leader cuts by that leader's history, which may lack a record a later epoch committed
             * (the next leader came from the old ISR without the old leader's tail); it is outside the later ISR,
             * and catches up from that epoch's leader when it learns of it (seed 200636). */
            size_t keep = a.pos ? (size_t)k + 1 : 0;
            uint64_t e = repl_epoch(nd->r);
            for (size_t j = keep; j < nd->n; j++) {
                uint64_t p = nd->log[j].pos;
                if (p <= S.cmax && S.committed[p] == nd->log[j].id && !(S.cacked[p] || S.cepoch[p] <= e))
                    S.st.xcuts++;
                if (p <= S.cmax && S.committed[p] == nd->log[j].id && (S.cacked[p] || S.cepoch[p] <= e))
                    violation("node %d (epoch %" PRIu64 ") cuts %s record %" PRIu64 " at %" PRIu64
                              " (committed by epoch %" PRIu64 ")", i, e, S.cacked[p] ? "acknowledged" : "committed",
                              nd->log[j].id, p, S.cepoch[p]);
            }
            /* Check 11: once its transfer succeeded, nothing the old leader wrote before READY or admitted after it
             * within the hard cap is cut; what it admitted after the cap may be (D14: counted). */
            for (size_t j = keep; S.xc.on && e >= S.xc.epoch && j < nd->n; j++)
                for (size_t q = 0; q < S.xc.n; q++) {
                    if (S.xc.pos[q] != nd->log[j].pos || S.xc.id[q] != nd->log[j].id)
                        continue;
                    if (S.xc.late[q] == 2) {
                        S.st.xlatecuts++;
                        continue;
                    }
                    violation("node %d (epoch %" PRIu64 ") cuts record %" PRIu64 " at %" PRIu64 ", %s READY of "
                              "transfer %" PRIu64, i, e, nd->log[j].id, nd->log[j].pos,
                              S.xc.late[q] ? "admitted within the hard cap after" : "written before", S.xc.xid);
                }
            trace("node %d: cut after %" PRIu64 " (end was %" PRIu64 ")", i, a.pos, log_end(nd));
            nd->n = keep;
            if (nd->ndur > keep)
                nd->ndur = keep;
            /* The excuse above holds only outside the current ISR: an intact member keeps every committed record. */
            if (intact(i))
                check_node(i, 1, S.cmax);
            break;
        }
        case REPL_A_SAVE:
            if (a.fence < nd->fence || a.led < nd->led)
                violation("node %d saves fence %" PRIu64 " led %" PRIu64 " over %" PRIu64 " %" PRIu64, i, a.fence,
                          a.led, nd->fence, nd->led);
            nd->fence = a.fence;
            nd->led = a.led;
            nd->floor = a.floor;
            break;
        case REPL_A_SEND:
            send_from(i, &a);
            break;
        case REPL_A_APPLY:
            if (a.pos > nd->applied) {
                S.st.applied += a.pos - nd->applied;
                check_apply(i, nd->applied + 1, a.pos);
                nd->applied = a.pos;
            }
            break;
        case REPL_A_REPLY:
            reply(i, &a);
            break;
        case REPL_A_FAIL:
            violation("node %d: the core failed: %s", i, a.reason);
            return;
        case REPL_A_SNAPSHOT: {
            uint64_t v = repl_get_set_version(nd->r);
            if (v >= MAXV || a.pos >= (uint64_t)S.nmemv[v] || S.memv[v][a.pos] == nd->nid) {
                violation("node %d asks for a snapshot from member %" PRIu64, i, a.pos);
                return;
            }
            if (!nd->snap_src) { /* a repeat while a copy is in progress is ignored */
                nd->snap_src = S.memv[v][a.pos];
                nd->snap_at = S.now + 1 + rn(10);
            }
            trace("node %d: snapshot from member %" PRIu64 " asked", i, a.pos);
            break;
        }
        }
        if (S.bad)
            return;
    }
    if (!nd->r)
        return;
    if (repl_written(nd->r) != log_end(nd))
        violation("node %d: the core's written end %" PRIu64 " is not the log's %" PRIu64, i, repl_written(nd->r),
                  log_end(nd));
    if (repl_epoch(nd->r) < nd->last_epoch)
        violation("node %d went back from epoch %" PRIu64 " to %" PRIu64, i, nd->last_epoch, repl_epoch(nd->r));
    nd->last_epoch = repl_epoch(nd->r);
}

static int alive(int i);

static void node_open(int i)
{
    struct node *nd = &S.node[i];
    struct repl_epoch *h = calloc(nd->n + 1, sizeof *h);
    uint32_t nh = 0;
    if (!h)
        exit(2);
    for (size_t k = 0; k < nd->n; k++) {
        if (nh && nd->log[k].epoch < h[nh - 1].epoch)
            violation("node %d's log goes back from epoch %" PRIu64 " to %" PRIu64, i, h[nh - 1].epoch,
                      nd->log[k].epoch);
        if (!nh || nd->log[k].epoch > h[nh - 1].epoch)
            h[nh++] = (struct repl_epoch){.epoch = nd->log[k].epoch, .start = nd->log[k].pos};
    }
    struct repl_durable d = {.rid = nd->rid, .fence = nd->fence, .led = nd->led, .floor = nd->floor,
                             .log_start = nd->start, .log_end = log_end(nd), .hist = h, .nhist = nh};
    /* The driver opens the core in the set the metadata copy names now. */
    nd->cfg.self = i;
    nd->cfg.nnodes = S.nn;
    nd->cfg.ctx = (void *)(intptr_t)nd->nid;
    nd->r = repl_open(&nd->cfg, &d);
    free(h);
    if (!nd->r) {
        violation("node %d: repl_open failed", i);
        return;
    }
    repl_set_version(nd->r, S.setv);
    if (S.replay_on && nd->replay)
        repl_replay_start(nd->r, nd->replay);
    nd->applied = 0;
    nd->last_epoch = 0;
    nd->sync_busy = 0;
    nd->pause_until = 0;
    /* A transfer lives in the driver's memory, the highest CANCEL too: after a restart a late TRANSFER of an earlier
     * transfer is no longer known as cancelled; it is refused only because the core leads no epoch it names (every
     * epoch this core leads begins after the open). */
    memset(&nd->xf, 0, sizeof nd->xf);
    nd->xf_cancelled = 0;
    trace("node %d: open, log end %" PRIu64 " fence %" PRIu64 " led %" PRIu64 " floor %" PRIu64, i, log_end(nd),
          nd->fence, nd->led, nd->floor);
    drive(i, NULL);
}

static void node_crash(int i, int wipe)
{
    struct node *nd = &S.node[i];
    if (nd->r) {
        repl_free(nd->r);
        nd->r = NULL;
    }
    nd->sync_busy = 0;
    nd->snap_src = 0;
    size_t keep = nd->ndur + rn((uint32_t)(nd->n - nd->ndur + 1));
    nd->n = nd->ndur = keep;
    for (size_t k = 0; k < nd->npend; k++) {
        S.ops[nd->pend[k]].status = OP_UNKNOWN;
        S.st.unknown++;
    }
    nd->npend = 0;
    if (wipe) {
        for (size_t k = 0; k < nd->n; k++)
            for (uint64_t p = nd->log[k].pos; p < nd->log[k].pos + nd->log[k].n; p++)
                if (p <= S.cmax && S.cacked[p] && S.committed[p] == nd->log[k].id)
                    S.st.wiped_acked++;
        if (S.vm.leader == i)
            S.st.wipe_leader++;
        nd->n = nd->ndur = 0;
        nd->start = nd->replay = 0;
        nd->wiped_at = S.now;
        nd->rid = ++S.next_rid;
        nd->fence = nd->led = nd->floor = 0;
        S.st.wipes++;
    } else {
        S.st.crashes++;
        if (intact(i))
            check_node(i, 1, S.cmax);
    }
    nd->down_until = S.now + 5 + rn(80);
    trace("node %d: %s, log end now %" PRIu64, i, wipe ? "WIPED" : "crash", log_end(nd));
}

/* A replica whose next record its leader no longer retains copies the source's applied state P (its parts: the
 * committed records up to P) and reopens on it with log start P + 1. The driver installs only a snapshot that covers
 * its whole log and more (P above its log end), and only from a source it reaches (the source may have restarted or
 * lost its disk since it asked: then P is low and nothing is installed). */
static void node_snapshot(int i)
{
    struct node *nd = &S.node[i];
    int s = slot_of(nd->snap_src);
    nd->snap_src = 0;
    if (s < 0)
        return; /* removed since */
    const struct node *src = &S.node[s];
    if (!src->r || !alive(s) || link_down(s, i) || link_down(i, s) || !repl_is_leader(src->r) ||
        repl_epoch(src->r) != repl_epoch(nd->r))
        return; /* the leader tells it again */
    uint64_t p = src->applied;
    if (!repl_snapshot_wanted(nd->r, p))
        return;
    if (p <= log_end(nd)) {
        violation("node %d wants a snapshot at %" PRIu64 ", its log ends at %" PRIu64, i, p, log_end(nd));
        return;
    }
    size_t k = 0;
    while (k < src->n && src->log[k].pos + src->log[k].n - 1 <= p)
        k++;
    if (!k || src->log[k - 1].pos + src->log[k - 1].n - 1 != p) {
        violation("node %d's applied end %" PRIu64 " is not a record end of its log", s, p);
        return;
    }
    repl_free(nd->r);
    nd->r = NULL;
    for (size_t j = 0; j < nd->npend; j++) {
        S.ops[nd->pend[j]].status = OP_UNKNOWN;
        S.st.unknown++;
    }
    nd->npend = 0;
    nd->log = grow(nd->log, &nd->cap, k + 1, sizeof *nd->log);
    memcpy(nd->log, src->log, k * sizeof *nd->log);
    nd->n = nd->ndur = k;
    for (size_t j = 0; j < k; j++)
        nd->log[j].wseq = 0;
    nd->wseq = 0;
    /* D3: the source's segments from its replay start come along (log start S <= P + 1); the new replica's replay
     * start is S, so the source's floor (held at S by the snapshot notice) stays at or below it. */
    uint64_t st = src->replay > src->start ? src->replay : src->start;
    if (st > p + 1)
        st = p + 1;
    nd->start = nd->replay = st;
    nd->floor = p;
    S.st.snaps++;
    trace("node %d: snapshot from node %d at %" PRIu64, i, s, p);
    node_open(i);
}

/* ------------------------------------------------------------------ planned transfers (D14): the driver's side */

static void xf_release(int i, const char *why)
{
    struct node *nd = &S.node[i];
    if (!nd->xf.on)
        return;
    nd->xf.on = 0;
    trace("node %d: transfer %" PRIu64 " over (%s): admits again", i, nd->xf.xid, why);
}

/* Whether the driver admits a write: never while a transfer holds it. */
static int xf_admits(int i)
{
    return !S.node[i].xf.on;
}

static void xf_send_ready(int i)
{
    struct node *nd = &S.node[i];
    struct smsg m = {.from_nid = nd->nid, .to_nid = -1, .xid = nd->xf.xid, .xend = nd->xf.end};
    m.m.type = SIM_READY;
    m.m.from = i;
    m.m.to = REPL_META;
    m.m.epoch = nd->xf.epoch;
    m.m.set_version = repl_get_set_version(nd->r);
    chk_ready(i, nd->xf.xid);
    enqueue(&m, i, META);
}

static void xf_ready(int i, uint64_t end)
{
    struct node *nd = &S.node[i];
    nd->xf.ready = 1;
    nd->xf.ready_at = S.now;
    nd->xf.end = end;
    S.st.xready++;
    trace("node %d: READY for transfer %" PRIu64 " at %" PRIu64 " (written %" PRIu64 ", commit %" PRIu64 ")", i,
          nd->xf.xid, end, repl_written(nd->r), repl_commit(nd->r));
    xf_send_ready(i);
}

/* Each tick on an alive node: the drain, READY (sent again while it waits), and the ends of a transfer's hold. */
static void xf_tick(int i)
{
    struct node *nd = &S.node[i];
    if (!nd->xf.on || !nd->r)
        return;
    if (!repl_is_leader(nd->r) || repl_epoch(nd->r) != nd->xf.epoch) {
        xf_release(i, "lost its epoch");
        return;
    }
    if (nd->xf.ready) {
        if (S.now - nd->xf.ready_at >= XF_CAP || (SMUT(61) && S.now - nd->xf.ready_at >= XF_WAIT)) {
            S.st.xcap++; /* transferresume, planted: its own timer, XF_WAIT after READY, ends the hold */
            xf_release(i, "hard cap");
        } else if ((S.now - nd->xf.ready_at) % 10 == 0) {
            xf_send_ready(i);
        }
        return;
    }
    int t = idx_in(repl_get_set_version(nd->r), nd->xf.tnid);
    uint64_t w = repl_written(nd->r);
    if (t >= 0 && SMUT(60)) {
        xf_ready(i, repl_peer_match(nd->r, t)); /* planted: READY at once, the target's end as the drained end */
        return;
    }
    if (t >= 0 && nd->xf.tnid != nd->nid && repl_commit(nd->r) == w &&
        repl_peer_match(nd->r, t) >= w) {
        xf_ready(i, w);
        return;
    }
    if (t < 0 || S.now - nd->xf.at >= XF_WAIT) {
        S.st.xdrainout++;
        xf_release(i, t < 0 ? "target gone" : "drain timed out");
    }
}

/* SIM_TRANSFER and SIM_CANCEL, which the driver handles (the core never sees them). */
static void xf_recv(int i, const struct smsg *sm)
{
    struct node *nd = &S.node[i];
    if (sm->m.type == SIM_CANCEL) {
        if (sm->xid > nd->xf_cancelled)
            nd->xf_cancelled = sm->xid;
        if (nd->xf.on && nd->xf.xid > sm->xid)
            S.st.xstale++; /* an earlier transfer's CANCEL, overtaken by a later TRANSFER */
        if (nd->xf.on && (nd->xf.xid <= sm->xid || SMUT(63))) /* transferstalecancel, planted: any CANCEL */
            xf_release(i, "CANCEL");
        return;
    }
    /* A TRANSFER for another epoch, one already cancelled (a CANCEL that overtook it) or older than the one held */
    if (!repl_is_leader(nd->r) || repl_epoch(nd->r) != sm->m.epoch || sm->xid <= nd->xf_cancelled ||
        (nd->xf.on && nd->xf.xid >= sm->xid))
        return;
    nd->xf.on = 1;
    nd->xf.ready = 0;
    nd->xf.xid = sm->xid;
    nd->xf.epoch = sm->m.epoch;
    nd->xf.at = S.now;
    nd->xf.tnid = sm->xnid;
    trace("node %d: TRANSFER %" PRIu64 " to nid %d: stops admitting", i, sm->xid, sm->xnid);
    xf_tick(i);
}

/* ------------------------------------------------------------------ the metadata group */

static void fo_begin(const char *why)
{
    if (S.fo)
        return;
    S.fo = 1;
    S.fo_epoch = S.vm.epoch;
    S.fo_version = S.vm.isr_version;
    S.fo_fenced = S.vm.isr;
    S.fo_start = S.now;
    S.fo_off = 0;
    S.fo_planned = 0;
    S.nrep = 0;
    trace("meta: failover of epoch %" PRIu64 " (%s), fencing isr %#x", S.vm.epoch, why, S.vm.isr);
    for (int i = 0; i < S.nn; i++)
        if ((S.fo_fenced & (1u << i)) || S.fence_all)
            meta_send(i, REPL_M_FENCE, S.fo_epoch + 1);
}

static void meta_isr_changed(uint32_t old, const uint64_t *oldrid)
{
    for (int i = 0; i < S.nn; i++)
        if (intact(i) && (!(old & (1u << i)) || oldrid[i] != S.vm.rid[i]))
            check_node(i, 1, S.cmax);
}

/* The metadata group's side of a planned transfer (D14). */
static void xf_meta_send(int to, int type, uint64_t xid, int xnid)
{
    if (to < 0)
        return;
    struct smsg m = {.from_nid = -1, .to_nid = S.node[to].nid, .xid = xid, .xnid = xnid};
    m.m.type = type;
    m.m.from = REPL_META;
    m.m.to = to;
    m.m.set_version = S.setv;
    m.m.epoch = S.vm.epoch;
    m.m.leader = -1;
    S.xdelay = S.xf_slow ? rn(S.xf_slow) : 0;
    enqueue(&m, META, to);
    S.xdelay = 0;
}

static void xf_abort(const char *why)
{
    if (S.xf.phase != 1)
        return;
    trace("meta: transfer %" PRIu64 " cancelled (%s)", S.xf.xid, why);
    S.st.xcancels++;
    xf_meta_send(slot_of(S.xf.lnid), SIM_CANCEL, S.xf.xid, 0);
    S.xf.phase = 0;
}

static void xf_start(int t)
{
    int l = S.vm.leader;
    if (S.fo || S.xf.phase || l < 0 || t < 0 || t >= S.nn || t == l || !(S.vm.isr & (1u << t)))
        return;
    S.xf.phase = 1;
    S.xf.lnid = S.node[l].nid;
    S.xf.tnid = S.node[t].nid;
    S.xf.xid = ++S.next_xid;
    S.xf.epoch = S.vm.epoch;
    S.xf.at = S.now;
    S.st.xfers++;
    trace("meta: transfer %" PRIu64 " of epoch %" PRIu64 " from node %d to node %d", S.xf.xid, S.vm.epoch, l, t);
    xf_meta_send(l, SIM_TRANSFER, S.xf.xid, S.xf.tnid);
}

static void meta_ready(const struct smsg *sm)
{
    int l = slot_of(sm->from_nid);
    if (S.xf.phase != 1 || sm->xid != S.xf.xid) {
        /* READY for a transfer this group does not hold (cancelled, done, or forgotten by a new metadata leader): its
         * leader would hold until the hard cap. A repeat of the one being fenced is ignored. */
        if (S.xf_answer && !(S.xf.phase == 2 && sm->xid == S.xf.xid))
            xf_meta_send(l, SIM_CANCEL, sm->xid, 0);
        return;
    }
    if (S.xf_stall && sm->xid != S.xstall_xid && chance(S.xf_stall)) {
        S.xstall_xid = sm->xid;
        /* The metadata leader stalls with READY in hand (a pause, a slow disk) for up to twice the hard cap: the old
         * leader's cap ends its hold first, and the promotion, when it comes, cuts what it admitted since. */
        S.meta_pause_until = S.now + XF_CAP / 2 + rn(2 * XF_CAP);
        S.st.xstalls++;
        trace("meta: stalls until %" PRIu64 " with READY of transfer %" PRIu64, S.meta_pause_until, sm->xid);
        S.q = grow(S.q, &S.cq, S.nq + 1, sizeof *S.q);
        S.q[S.nq] = *sm; /* read again after the stall (no records to own) */
        S.q[S.nq++].at = S.now + 1;
        return;
    }
    int t = slot_of(S.xf.tnid);
    if (S.fo || l < 0 || l != S.vm.leader || sm->m.epoch != S.vm.epoch || t < 0 || !(S.vm.isr & (1u << t))) {
        xf_abort("READY no longer fits the record");
        return;
    }
    fo_begin("planned transfer");
    S.fo_planned = 1;
    S.fo_tnid = S.xf.tnid;
    S.fo_end = sm->xend;
    S.fo_xid = S.xf.xid;
    S.xf.phase = 2;
}

/* A new metadata leader: the transfers lived in the old one's memory. A planned promotion under way goes on as an
 * ordinary failover (its fences are out: the old leader is deposed), and every node is told CANCEL. */
static void meta_new_leader(void)
{
    S.st.metachg++;
    trace("meta: new metadata leader: transfers forgotten, CANCEL to every node");
    S.xf.phase = 0;
    S.meta_pause_until = 0; /* a stalled leader is replaced */
    if (S.fo && S.fo_planned) {
        S.fo_planned = 0;
        S.st.xfailover++;
    }
    if (!S.xf_answer && chance(500)) /* and lost its lead again before its first duty round: the hard cap ends it */
        return;
    for (int i = 0; i < S.nn; i++)
        xf_meta_send(i, SIM_CANCEL, S.next_xid, 0);
}

static void meta_tick(void)
{
    if (S.now % WATCH_TICKS == 0)
        meta_broadcast();
    if (!S.fo && S.vm.leader >= 0 && S.now - S.last_hb[S.vm.leader] > NODE_TIMEOUT)
        fo_begin("leader silent");
    if (S.xf.phase == 1 && (S.fo || S.vm.epoch != S.xf.epoch || slot_of(S.xf.lnid) != S.vm.leader ||
                            S.now - S.xf.at > XF_READY_WAIT))
        xf_abort(S.fo ? "a failover" : S.now - S.xf.at > XF_READY_WAIT ? "no READY" : "the record moved");
    if (!S.fo)
        return;
    uint32_t old = S.vm.isr;
    uint64_t oldrid[NM];
    memcpy(oldrid, S.vm.rid, sizeof oldrid);
    int rc = REPL_E_NONE, target = -1, planned = 0;
    if (S.fo_planned) {
        /* D14: every fenced member's reply, the old leader's included, so that none is left `unanswered` */
        uint32_t got = 0;
        for (int k = 0; k < S.nrep; k++)
            got |= 1u << S.rep[k].node;
        int t = slot_of(S.fo_tnid);
        if ((S.fo_fenced & ~got) && t >= 0 && S.now - S.fo_start < XF_FENCE_WAIT)
            return;
        if (!(S.fo_fenced & ~got) && t >= 0) {
            struct repl_fence_reply one[1];
            const struct repl_fence_reply *rp = S.rep;
            int n = S.nrep;
            if (SMUT(62)) { /* planted: only the target's reply */
                n = 0;
                for (int k = 0; k < S.nrep; k++)
                    if (S.rep[k].node == t)
                        one[n++] = S.rep[k];
                rp = one;
            }
            rc = repl_meta_promote_to(&S.vm, S.fo_epoch, S.fo_version, S.fo_fenced, rp, n, t, S.fo_end);
            planned = rc == REPL_OK;
            if (planned) {
                S.st.xdone++;
                trace("meta: transfer %" PRIu64 " done: epoch %" PRIu64 " leader %d", S.fo_xid, S.vm.epoch,
                      S.vm.leader);
                if (S.vm.leader != t)
                    violation("transfer %" PRIu64 " to node %d promoted node %d", S.fo_xid, t, S.vm.leader);
                if (S.vm.unanswered)
                    violation("transfer %" PRIu64 " left members %#x unanswered", S.fo_xid, S.vm.unanswered);
                if (S.xc.xid == S.fo_xid) {
                    S.xc.on = 1;
                    S.xc.epoch = S.vm.epoch;
                }
            }
        }
        if (!planned) {
            /* Dropped: CANCEL, and the failover goes on as an ordinary one (the old leader is fenced) */
            S.st.xfailover++;
            trace("meta: transfer %" PRIu64 " dropped (%s): an ordinary failover", S.fo_xid,
                  t < 0 ? "target gone" : rc == REPL_E_NONE && !(S.fo_fenced & ~got) ? "target short" : "replies");
            xf_meta_send(slot_of(S.xf.lnid), SIM_CANCEL, S.fo_xid, 0);
        }
        S.fo_planned = 0;
        S.xf.phase = 0;
    }
    if (!planned && S.nrep < popcount32(S.fo_fenced) && S.now - S.fo_start < FENCE_WAIT)
        return;
    /* Moving off a leaving leader: the target is the replier (other than it) with the highest durable end. A fenced
     * ISR member with the record's rid holds every committed position (the commit point is the ISR's lowest match), so
     * only the leaving leader's uncommitted tail, never acknowledged, is cut. Asking for the highest end of all replies
     * instead (D14's drained end) failed while clients kept the leader ahead: the old leader won every retry (seeds
     * 14328, 39648 and 7 more of 100,000); the drain before the fence is r4-simtr's. Without such a reply, the usual
     * choice, which may pick the leaving member again: the mover then tries once more. */
    int off = S.fo_off && !planned ? slot_of(S.fo_off) : -1;
    for (int i = 0; off >= 0 && i < S.nrep; i++) {
        int nd = S.rep[i].node;
        if (nd < 0 || nd >= S.nn || nd == off || !(S.vm.isr & S.fo_fenced & (1u << nd)) || S.rep[i].rid != S.vm.rid[nd])
            continue;
        if (target < 0 || S.rep[i].durable > S.rep[target].durable)
            target = i;
    }
    if (!planned)
        rc = target >= 0 ? repl_meta_promote_to(&S.vm, S.fo_epoch, S.fo_version, S.fo_fenced, S.rep, S.nrep,
                                                S.rep[target].node, S.rep[target].durable)
                         : repl_meta_promote(&S.vm, S.fo_epoch, S.fo_version, S.fo_fenced, S.rep, S.nrep);
    if (rc == REPL_OK && target >= 0)
        S.st.planned++;
    if (rc == REPL_OK) {
        S.fo = 0;
        /* The D1 excuse goes only with a promotion that drops an intact member (one still on the disk the old ISR
         * named: P-F1's bug). A promotion that keeps a leader's shrunk set, or drops only lost disks, keeps it: the
         * leader shrank to itself, lost its disk, the metadata applied the shrink in that tick and promoted the set
         * on a fence reply the old instance sent before the loss (seeds 299557 and 436347). */
        for (int i = 0; i < S.nn; i++)
            if ((old & ~S.vm.isr & (1u << i)) && S.node[i].rid == oldrid[i])
                S.shrunk = 0;
        S.st.promotions++;
        S.last_hb[S.vm.leader] = S.now;
        trace("meta: epoch %" PRIu64 " leader %d isr %#x", S.vm.epoch, S.vm.leader, S.vm.isr);
        meta_isr_changed(old, oldrid);
        meta_broadcast();
    } else if (rc == REPL_E_STALE) {
        S.fo = 0;
        S.st.stale++;
        if (S.vm.epoch == S.fo_epoch)
            fo_begin("stale basis");
    } else {
        S.st.none++;
        S.nrep = 0;
        S.fo_start = S.now;
        for (int i = 0; i < S.nn; i++)
            if ((S.fo_fenced & (1u << i)) || S.fence_all)
                meta_send(i, REPL_M_FENCE, S.fo_epoch + 1);
    }
}

static void meta_recv(const struct smsg *sm)
{
    const struct repl_msg *m = &sm->m;
    if (m->type == SIM_HB) {
        int s = slot_of(sm->from_nid);
        if (s >= 0)
            S.last_hb[s] = S.now;
        return;
    }
    if (m->type == SIM_READY) {
        meta_ready(sm);
        return;
    }
    /* Indexes name members only within one set version (D8). */
    if (m->set_version != S.setv) {
        S.st.xver++;
        return;
    }
    if (m->from < 0 || m->from >= S.nn)
        return;
    switch (m->type) {
    case REPL_M_FENCE_REPLY:
        if (!S.fo || m->epoch != S.fo_epoch + 1)
            break;
        if (!(S.fo_fenced & (1u << m->from)))
            S.st.fence_rep_out++;
        for (int k = 0; k < S.nrep; k++)
            if (S.rep[k].node == m->from) {
                S.rep[k] = S.rep[--S.nrep];
                break;
            }
        S.rep[S.nrep++] = (struct repl_fence_reply){.node = m->from, .durable = m->durable,
                                                    .last_epoch = m->last_epoch, .rid = m->rid};
        break;
    case REPL_M_ISR_REQ: {
        uint32_t old = S.vm.isr;
        uint64_t oldrid[NM];
        memcpy(oldrid, S.vm.rid, sizeof oldrid);
        if (repl_meta_isr_change(&S.vm, &m->meta) == REPL_OK) {
            S.st.isr++;
            S.shrunk = (old & ~S.vm.isr) != 0;
            trace("meta: isr %#x -> %#x (epoch %" PRIu64 ", by %d)", old, S.vm.isr, S.vm.epoch, m->from);
            int a = slot_of(S.mv.a), b = slot_of(S.mv.b);
            if (S.mv.phase == 1 && b >= 0 && !(old & (1u << b)) && (S.vm.isr & (1u << b))) {
                S.mv.phase = 2; /* B joined: A may leave while the ISR keeps the size it had before */
                S.mv.min = (uint32_t)popcount32(old);
                trace("meta: move: node %d joined, node %d leaves (min isr %u)", b, a, S.mv.min);
            }
            const struct node *snd = &S.node[m->from];
            if (S.mv.phase == 2 && a >= 0 && !(old & (1u << a)) && (S.vm.isr & (1u << a)) && snd->r &&
                repl_leaving(snd->r) == a)
                violation("node %d got leaving member %d into the ISR again (%#x -> %#x)", m->from, a, old,
                          S.vm.isr);
            meta_isr_changed(old, oldrid);
        }
        meta_send(m->from, REPL_M_META, S.vm.epoch);
        break;
    }
    case REPL_M_RESIGN:
        if (m->epoch == S.vm.epoch && m->from == S.vm.leader) {
            S.st.resigns++;
            fo_begin("resigned");
        }
        break;
    default:
        break;
    }
}

/* ------------------------------------------------------------------ events */

static void fail_pending(struct node *nd)
{
    for (size_t k = 0; k < nd->npend; k++) {
        S.ops[nd->pend[k]].status = OP_UNKNOWN;
        S.st.unknown++;
    }
    nd->npend = 0;
}

/* A META of a newer set version (D8): the driver maps the members by node (old_of) and reconfigures its core; a core
 * that refuses (a deposed leader with a removed member in its ISR) is restarted in the current set. Returns whether
 * the META may be handed over. */
static int node_reconfig(int i, uint64_t cv, uint64_t nv)
{
    struct node *nd = &S.node[i];
    int old_of[NM], n = S.nmemv[nv], self = idx_in(nv, nd->nid);
    for (int k = 0; k < n; k++)
        old_of[k] = idx_in(cv, S.memv[nv][k]);
    int rc = repl_reconfig(nd->r, n, self, old_of);
    if (rc == REPL_E_PENDING) /* drive() carries out every action after each message: the queue is empty here */
        violation("node %d: repl_reconfig found actions still queued", i);
    if (rc == REPL_OK) {
        repl_set_version(nd->r, nv);
        S.st.reconfs++;
        trace("node %d: set version %" PRIu64 " -> %" PRIu64, i, cv, nv);
        return 1;
    }
    S.st.restarts++;
    trace("node %d: refuses set version %" PRIu64 ", restarted", i, nv);
    repl_free(nd->r);
    nd->r = NULL;
    nd->sync_busy = 0;
    nd->snap_src = 0;
    fail_pending(nd);
    node_open(i);
    return nd->r && nv == S.setv;
}

static void node_recv(int i, struct smsg *sm)
{
    struct node *nd = &S.node[i];
    struct repl_msg *m = &sm->m;
    m->rec = sm->rec;
    if (m->type == SIM_TRANSFER || m->type == SIM_CANCEL) {
        if (m->type == SIM_TRANSFER)
            chk_transfer(i, sm);
        else
            chk_cancel(i, sm);
        xf_recv(i, sm);
        return;
    }
    if (m->from == REPL_META && m->type == REPL_M_META) {
        uint64_t cv = repl_get_set_version(nd->r);
        if (m->set_version < cv)
            return; /* an older copy of the record */
        if (m->set_version > cv && !node_reconfig(i, cv, m->set_version))
            return;
    }
    uint64_t d0 = repl_dropped(nd->r);
    repl_recv(nd->r, m);
    if (m->type != REPL_M_FENCE && m->set_version != repl_get_set_version(nd->r)) {
        S.st.xver++;
        if (repl_dropped(nd->r) == d0)
            violation("node %d took a type %d message of set version %" PRIu64 " in set version %" PRIu64, i,
                      m->type, m->set_version, repl_get_set_version(nd->r));
    }
    drive(i, sm);
}

static void deliver(struct smsg *m)
{
    int to = m->to_nid < 0 ? META : slot_of(m->to_nid);
    int from = m->from_nid < 0 ? META : slot_of(m->from_nid);
    if (to < 0) {
        /* to a removed member */
    } else if (from >= 0 && link_down(from, to)) {
        /* cut while in flight (a removed sender's last messages still arrive) */
    } else if (to == META) {
        meta_recv(m);
    } else if (S.node[to].r) {
        node_recv(to, m);
    }
    free(m->rec);
    free(m->ids);
}

static void client_write(void)
{
    int cand[NM], nc = 0, lead[NM], nl = 0;
    for (int i = 0; i < S.nn; i++) {
        const struct node *nd = &S.node[i];
        if (!nd->r || nd->pause_until > S.now)
            continue;
        cand[nc++] = i;
        if (repl_is_leader(nd->r))
            lead[nl++] = i;
    }
    if (!nc)
        return;
    int i = nl && chance(850) ? lead[rn(nl)] : cand[rn(nc)];
    struct node *nd = &S.node[i];
    if (!xf_admits(i)) { /* D14: refused before anything is written, a definite answer */
        S.st.refused++;
        S.st.xrefused++;
        return;
    }
    S.ops = grow(S.ops, &S.cops, S.nops + 1, sizeof *S.ops);
    struct op *o = &S.ops[S.nops];
    *o = (struct op){.id = S.nops + 1, .n = 1 + rn(4), .node = i, .status = OP_PENDING, .at = S.now};
    S.cur_op = o->id;
    if (repl_write(nd->r, o->n, &o->pos) != REPL_OK) {
        S.st.refused++;
        drive(i, NULL);
        return;
    }
    nd->pend = grow(nd->pend, &nd->cpend, nd->npend + 1, sizeof *nd->pend);
    nd->pend[nd->npend++] = S.nops++;
    drive(i, NULL);
}

static int alive(int i)
{
    return S.node[i].r && S.node[i].pause_until <= S.now;
}

/* ------------------------------------------------------------------ replica set changes (D8, D11) */

static void new_version(void)
{
    S.setv++;
    S.nmemv[S.setv] = S.nn;
    for (int j = 0; j < S.nn; j++)
        S.memv[S.setv][j] = S.node[j].nid;
    trace("meta: set version %" PRIu64 ", %d members", S.setv, S.nn);
}

static int add_member(void)
{
    int i = S.nn;
    struct node *nd = &S.node[i];
    memset(nd, 0, sizeof *nd);
    nd->nid = (int)++S.next_nid;
    nd->rid = ++S.next_rid;
    nd->cfg = S.node[0].cfg;
    S.vm.rid[i] = 0;
    S.last_hb[i] = S.now;
    for (int a = 0; a <= META; a++)
        S.cut[a][i] = S.cut[i][a] = 0;
    S.nn++;
    new_version();
    S.st.adds++;
    trace("meta: add node %d (nid %d, rid %" PRIu64 ")", i, nd->nid, nd->rid);
    node_open(i);
    meta_broadcast();
    return nd->nid;
}

static uint32_t squeeze(uint32_t x, int k)
{
    return (x & ((1u << k) - 1)) | ((x >> (k + 1)) << k);
}

static void remove_member(int k)
{
    struct node gone = S.node[k];
    repl_free(gone.r);
    fail_pending(&gone);
    free(gone.log);
    free(gone.pend);
    memmove(&S.node[k], &S.node[k + 1], (size_t)(S.nn - k - 1) * sizeof *S.node);
    memset(&S.node[S.nn - 1], 0, sizeof *S.node);
    for (int j = k; j < S.nn - 1; j++) {
        S.vm.rid[j] = S.vm.rid[j + 1];
        S.last_hb[j] = S.last_hb[j + 1];
    }
    S.vm.rid[S.nn - 1] = 0;
    S.vm.isr = squeeze(S.vm.isr, k);
    S.vm.unanswered = squeeze(S.vm.unanswered, k);
    if (S.vm.leader > k)
        S.vm.leader--;
    for (int a = k; a < META; a++)
        for (int b = 0; b <= META; b++)
            S.cut[a][b] = a + 1 < META ? S.cut[a + 1][b] : 0;
    for (int b = k; b < META; b++)
        for (int a = 0; a <= META; a++)
            S.cut[a][b] = b + 1 < META ? S.cut[a][b + 1] : 0;
    S.nn--;
    if (S.nremoved < MAXV)
        S.removed_at[S.nremoved++] = S.now;
    new_version();
    S.st.removes++;
    trace("meta: remove node %d (nid %d)", k, gone.nid);
    meta_broadcast();
}

/* A member outside the ISR that is not the leader, or -1. */
static int pick_outside(void)
{
    int c[NM], n = 0;
    for (int j = 0; j < S.nn; j++)
        if (!(S.vm.isr & (1u << j)) && j != S.vm.leader)
            c[n++] = j;
    return n ? c[rn((uint32_t)n)] : -1;
}

static void set_change(void)
{
    if (S.fo || S.mv.phase || S.setv + 2 >= MAXV)
        return;
    uint32_t c = rn(4);
    if (c == 0 && S.nn < NSIM) {
        add_member();
    } else if (c == 1 && S.nn > 3) {
        int k = pick_outside();
        if (k >= 0)
            remove_member(k);
    } else if (c == 2) {
        int k = pick_outside();
        if (k < 0)
            return;
        node_crash(k, 1); /* a new instance under a new rid: a remove and an add at one index */
        S.node[k].nid = (int)++S.next_nid;
        new_version();
        S.st.resets++;
        trace("meta: reset node %d (nid %d)", k, S.node[k].nid);
        meta_broadcast();
    } else if (c == 3 && S.nn < NSIM) {
        int c2[NM], n = 0;
        for (int j = 0; j < S.nn; j++)
            if ((S.vm.isr & (1u << j)) && j != S.vm.leader)
                c2[n++] = j;
        if (!n)
            return;
        int a = c2[rn((uint32_t)n)];
        S.mv.a = S.node[a].nid;
        S.mv.phase = 1;
        S.mv.remove_at = 0;
        trace("meta: move node %d out", a);
        S.mv.b = add_member();
    }
}

/* Each tick: once B joined, the leader of the metadata's epoch is told that A leaves; A goes once out of the ISR. */
static void mover(void)
{
    if (S.mv.phase != 2)
        return;
    int a = slot_of(S.mv.a), l = S.vm.leader;
    if (a < 0) {
        S.mv.phase = 0;
        return;
    }
    if (l == a) {
        /* A failover may have made the leaving member the leader: move off it (every tick once the faults healed,
         * or the move never ends: seeds 31511, 37712 and 14 more of 100,000 before), but only once another member is
         * in the ISR: with the leaving one alone there, every retry promoted it again and kept the others from
         * joining (seeds 89586, 75053 and 4 more). */
        if (!S.fo && !S.xf.phase && (S.vm.isr & ~(1u << l)) && (!S.chaos || chance(20))) {
            if (S.xfer_on && chance(500)) { /* D14's transfer, to a random other ISR member */
                int c[NM], n = 0;
                for (int j = 0; j < S.nn; j++)
                    if (j != l && (S.vm.isr & (1u << j)))
                        c[n++] = j;
                xf_start(c[rn((uint32_t)n)]);
            } else {
                fo_begin("moving off a leaving member");
                S.fo_off = S.mv.a;
            }
        }
        return;
    }
    struct node *L = l >= 0 ? &S.node[l] : NULL;
    if (L && L->r && alive(l) && repl_is_leader(L->r) && repl_epoch(L->r) == S.vm.epoch &&
        repl_get_set_version(L->r) == S.setv && repl_leaving(L->r) != a) {
        if (repl_leave(L->r, a, S.mv.min) == REPL_OK)
            S.st.leaves++;
        drive(l, NULL);
    }
    if (S.vm.isr & (1u << a)) {
        S.mv.remove_at = 0;
        return;
    }
    if (!S.mv.remove_at) {
        S.mv.remove_at = S.now + 20 + rn(40); /* time for a leader to flap it back in (leaveflap) */
        return;
    }
    if (S.now >= S.mv.remove_at && !S.fo) {
        remove_member(a);
        S.mv.phase = 0;
        S.st.mvdone++;
    }
}

static void nemesis(void)
{
    int i = (int)rn((uint32_t)S.nn);
    struct node *nd = &S.node[i];
    uint32_t k = rn(100);
    if (k < 25) {
        if (nd->r)
            node_crash(i, 0);
    } else if (k < 33) {
        /* Lose a disk only while another intact ISR member is left: at most two of three copies go. */
        int other = 0;
        for (int j = 0; j < S.nn; j++)
            if (j != i && intact(j))
                other = 1;
        if (other) {
            node_crash(i, 1);
        }
    } else if (k < 40) {
        if (nd->r) {
            node_crash(i, 0);
            nd->down_until = S.now;
        }
    } else if (k < 58) {
        uint64_t until = S.now + 10 + rn(120);
        int mode = (int)rn(5), side[NM + 1], target = (int)rn((uint32_t)S.nn + 1);
        if (target == S.nn)
            target = META;
        for (int a = 0; a <= META; a++)
            side[a] = (int)rn(2);
        S.st.parts++;
        for (int a = 0; a <= META; a++) {
            for (int b = 0; b <= META; b++) {
                if (a == b || (a < META && a >= S.nn) || (b < META && b >= S.nn))
                    continue;
                int x = 0;
                switch (mode) {
                case 0: x = a == i || b == i; break;                                 /* isolated */
                case 1: x = (a == i && b == META) || (a == META && b == i); break;  /* from metadata only */
                case 2: x = (a == i || b == i) && a != META && b != META; break;    /* from peers only */
                case 3: x = side[a] != side[b]; break;                               /* a random split */
                case 4: x = a == i && b == target; break;                            /* one link, one way */
                }
                if (x && S.cut[a][b] < until)
                    S.cut[a][b] = until;
            }
        }
        trace("nemesis: partition mode %d around node %d until %" PRIu64, mode, i, until);
    } else if (k < 68) {
        if (nd->r && nd->pause_until <= S.now) {
            nd->pause_until = S.now + 10 + rn(100);
            S.st.pauses++;
            trace("nemesis: pause node %d until %" PRIu64, i, nd->pause_until);
        }
    } else if (k < 84 && S.meta_pause_until > S.now) {
        /* the metadata leader is stalled: no move, no set change */
    } else if (k < 76) {
        S.st.moves++;
        fo_begin("moved");
    } else if (k < 84 && S.setchg) {
        set_change();
    } else if (k < 88 && S.xfer_on) {
        meta_new_leader();
    }
}

static void step(void)
{
    S.now++;
    if (S.chaos && chance(S.nemesis))
        nemesis();
    for (int i = 0; i < S.nn && !S.bad; i++)
        if (!S.node[i].r && S.node[i].down_until <= S.now)
            node_open(i);
    /* messages due, in random order */
    size_t nd = 0;
    struct smsg *due = NULL;
    size_t cdue = 0;
    for (size_t k = 0; k < S.nq;) {
        struct smsg *m = &S.q[k];
        int to = m->to_nid < 0 ? META : slot_of(m->to_nid);
        if (m->at <= S.now && !(to >= 0 && to < S.nn && S.node[to].pause_until > S.now) &&
            !(to == META && S.meta_pause_until > S.now)) {
            due = grow(due, &cdue, nd + 1, sizeof *due);
            due[nd++] = *m;
            S.q[k] = S.q[--S.nq];
        } else {
            k++;
        }
    }
    for (size_t k = nd; k > 1; k--) {
        size_t j = rn((uint32_t)k);
        struct smsg t = due[k - 1];
        due[k - 1] = due[j];
        due[j] = t;
    }
    for (size_t k = 0; k < nd; k++) {
        if (S.bad) {
            free(due[k].rec);
            free(due[k].ids);
            continue;
        }
        deliver(&due[k]);
    }
    free(due);
    for (int i = 0; i < S.nn && !S.bad; i++) {
        struct node *n = &S.node[i];
        if (!alive(i))
            continue;
        if (n->sync_busy && n->sync_at <= S.now) {
            size_t lo = 0, hi = n->n;
            while (lo < hi) {
                size_t mid = (lo + hi) / 2;
                if (n->log[mid].wseq <= n->sync_wseq)
                    lo = mid + 1;
                else
                    hi = mid;
            }
            if (lo > n->ndur)
                n->ndur = lo;
            n->sync_busy = 0;
            /* A naive driver reports the end it issued the sync for: more than is durable when the log was cut and
             * written again meanwhile (the core must not believe it). */
            if (n->sync_target > dur_end(n))
                S.st.lies++;
            repl_synced(n->r, S.naive_sync ? n->sync_target : dur_end(n));
            drive(i, NULL);
        }
        if (n->snap_src && n->snap_at <= S.now) {
            node_snapshot(i);
            if (S.bad || !n->r)
                continue;
        }
        if (S.trim && S.replay_on) {
            /* D17: the floor's rows are in parts first (the replay start), the segments go later (the log start). */
            if (chance(50) && n->floor + 1 > n->replay && n->floor + 1 > n->start && n->floor <= dur_end(n)) {
                n->replay = n->floor + 1;
                repl_replay_start(n->r, n->replay);
                S.st.replays++;
            }
            if (chance(30) && n->replay > n->start) {
                n->start = n->replay;
                repl_log_start(n->r, n->start);
                S.st.trims++;
            }
        } else if (S.trim && chance(50) && n->floor + 1 > n->start && n->floor <= dur_end(n)) {
            n->start = n->floor + 1; /* segments at or below the saved floor went into parts and are deleted */
            repl_log_start(n->r, n->start);
            S.st.trims++;
        }
        repl_tick(n->r);
        drive(i, NULL);
        xf_tick(i);
        if (S.bad)
            break;
        if (S.now % HB_TICKS == (uint64_t)i % HB_TICKS) {
            struct smsg m = {.from_nid = n->nid, .to_nid = -1};
            m.m.type = SIM_HB;
            m.m.from = i;
            m.m.to = REPL_META;
            enqueue(&m, i, META);
        }
    }
    /* The floor that counts is the one of the metadata's epoch's leader: a deposed leader in an older set does not
     * know members added since (nor do they take its records). */
    for (int i = 0; i < S.nn && !S.bad; i++)
        if (S.node[i].r && repl_is_leader(S.node[i].r) && repl_epoch(S.node[i].r) == S.vm.epoch)
            check_floor(i);
    int meta_up = S.meta_pause_until <= S.now;
    if (!S.bad && meta_up)
        meta_tick();
    if (!S.bad && meta_up)
        mover();
    if (S.xfer_on && S.chaos && !S.bad && meta_up && chance(S.xrate))
        xf_start((int)rn((uint32_t)S.nn));
    if (S.writes && !S.bad && chance(S.wrate))
        client_write();
}

/* ------------------------------------------------------------------ one run */

static void reset(void)
{
    for (int i = 0; i < NM; i++) {
        repl_free(S.node[i].r);
        free(S.node[i].log);
        free(S.node[i].pend);
    }
    for (size_t k = 0; k < S.nq; k++) {
        free(S.q[k].rec);
        free(S.q[k].ids);
    }
    free(S.q);
    free(S.ops);
    free(S.committed);
    free(S.cepoch);
    free(S.cacked);
    free(S.acker);
    free(S.xc.pos);
    free(S.xc.id);
    free(S.xc.late);
    int trace_on = S.trace;
    __typeof__(S.tot) tot = S.tot;
    memset(&S, 0, sizeof S);
    S.tot = tot;
    S.trace = trace_on;
}

static int converged(void)
{
    int l = S.vm.leader;
    if (S.fo || l < 0 || !S.node[l].r || !repl_is_leader(S.node[l].r) || repl_epoch(S.node[l].r) != S.vm.epoch ||
        S.node[l].xf.on)
        return 0;
    if (S.vm.isr != (1u << S.nn) - 1)
        return 0;
    uint64_t w = repl_written(S.node[l].r);
    for (int i = 0; i < S.nn; i++)
        if (!S.node[i].r || repl_written(S.node[i].r) != w || repl_applied(S.node[i].r) != w)
            return 0;
    return 1;
}

static void describe(void)
{
    fprintf(stderr, "  meta: epoch %" PRIu64 " leader %d isr %#x version %" PRIu64 " failover %d set version %"
            PRIu64 " move %d\n", S.vm.epoch, S.vm.leader, S.vm.isr, S.vm.isr_version, S.fo, S.setv, S.mv.phase);
    for (int i = 0; i < S.nn; i++) {
        const struct node *nd = &S.node[i];
        if (!nd->r) {
            fprintf(stderr, "  node %d: down, log end %" PRIu64 "\n", i, log_end(nd));
            continue;
        }
        fprintf(stderr,
                "  node %d (nid %d, set %" PRIu64 "): %s epoch %" PRIu64 " written %" PRIu64 " durable %" PRIu64
                " applied %" PRIu64 " commit %" PRIu64 " fence %" PRIu64 " rid %" PRIu64 "%s\n",
                i, nd->nid, repl_get_set_version(nd->r), repl_is_leader(nd->r) ? "leader" : "replica",
                repl_epoch(nd->r), repl_written(nd->r), repl_durable_end(nd->r), repl_applied(nd->r),
                repl_commit(nd->r), nd->fence, nd->rid,
                repl_failed(nd->r) ? " FAILED" : "");
    }
}

static int run(uint64_t seed, uint64_t ticks)
{
    reset();
    S.seed = seed;
    S.rng = seed * 0x9e3779b97f4a7c15ull + 0x632be59bd9b4e019ull;
    for (int k = 0; k < 4; k++)
        rnd();
    uint32_t k = rn(100);
    S.nn = k < 70 ? 3 : k < 88 ? 4 : 5;
    int min_isr = chance(200) ? 2 : 1;
    S.min_isr = min_isr;
    S.loss = rn(60);
    S.dup = rn(40);
    S.maxdelay = 1 + rn(6);
    S.maxsync = 1 + rn(5);
    S.nemesis = 5 + rn(30);
    S.wrate = 100 + rn(600);
    uint32_t batch = 1 + rn(16);
    S.fence_all = chance(500);
    S.naive_sync = chance(300);
    S.trim = chance(500);
    S.replay_on = chance(500);
    S.setchg = chance(500);
    S.xfer_on = chance(600);
    S.xrate = 2 + rn(10);
    S.xf_answer = chance(500);
    S.xf_stall = chance(300) ? 100 + rn(400) : 0;
    S.xf_slow = chance(300) ? 20 + rn(150) : 0;
    if (chance(200))
        S.maxsync = 1 + rn(30); /* a slow fdatasync against a fast round trip: syncs span cuts */
    S.vm = (struct repl_vmeta){.epoch = 1, .isr_version = 1, .leader = 0, .isr = (1u << S.nn) - 1};
    S.nmemv[0] = S.nn;
    for (int i = 0; i < S.nn; i++) {
        struct node *nd = &S.node[i];
        nd->nid = (int)++S.next_nid;
        S.memv[0][i] = nd->nid;
        nd->rid = ++S.next_rid;
        S.vm.rid[i] = nd->rid;
        nd->cfg = (struct repl_config){.self = i, .nnodes = S.nn, .min_isr = min_isr, .isr_lag_ticks = 30,
                                       .resend_ticks = 10, .heartbeat_ticks = 3, .floor_ticks = 20,
                                       .idle_heartbeat_ticks = seed % 3 ? 3 + (uint32_t)(seed % 10) : 0,
                                       .unanswered_ticks = 20, .join_hold_ticks = seed % 4 ? 30 : 0,
                                       .max_batch = batch, .window = 32 * batch, .rec_find = rec_find};
        node_open(i);
    }
    meta_broadcast();
    S.chaos = S.writes = 1;
    for (uint64_t t = 0; t < ticks && !S.bad; t++)
        step();
    /* Heal everything and stop the faults: the volume must come back whole. */
    S.chaos = 0;
    S.writes = 0;
    memset(S.cut, 0, sizeof S.cut);
    S.loss = S.dup = 0;
    S.meta_pause_until = 0;
    S.xf_stall = S.xf_slow = 0;
    for (int i = 0; i < S.nn; i++) {
        S.node[i].pause_until = 0;
        S.node[i].down_until = 0;
    }
    uint64_t longest = 0;
    for (int i = 0; i < S.nn; i++)
        if (log_end(&S.node[i]) > longest)
            longest = log_end(&S.node[i]);
    /* A wiped replica copies the whole log (or takes a snapshot); a move under way still has to finish. */
    uint64_t deadline = S.now + 3000 + 2 * longest + 300 * S.setv;
    size_t final = 0;
    int phase = 0;
    while (!S.bad && S.now < deadline) {
        step();
        if (phase == 0 && converged() && !S.mv.phase) {
            int l = S.vm.leader;
            S.ops = grow(S.ops, &S.cops, S.nops + 1, sizeof *S.ops);
            final = S.nops;
            S.ops[final] = (struct op){.id = S.nops + 1, .n = 1, .node = l, .status = OP_PENDING};
            S.cur_op = S.ops[final].id;
            if (repl_write(S.node[l].r, 1, &S.ops[final].pos) == REPL_OK) {
                struct node *nd = &S.node[l];
                nd->pend = grow(nd->pend, &nd->cpend, nd->npend + 1, sizeof *nd->pend);
                nd->pend[nd->npend++] = S.nops++;
                phase = 1;
            }
            drive(l, NULL);
        } else if (phase == 1 && S.ops[final].status != OP_PENDING) {
            if (S.ops[final].status != OP_OK)
                phase = 0;
            else
                phase = 2;
        } else if (phase == 2 && converged() && !S.mv.phase) {
            phase = 3;
            break;
        }
    }
    int any = 0;
    for (int i = 0; i < S.nn; i++)
        any |= intact(i);
    if (!S.bad && phase != 3 && !any && S.st.wipes) {
        /* Every ISR member the metadata names lost its disk: the volume waits for an operator (§6.2, no unclean
         * election). With min ISR 1 that follows from one wipe after the leader shrank the ISR to itself (D1): the
         * leader's shrink, applied by the metadata group before or just after the loss, is the only excuse. A wait
         * after a promotion while a replica still holds every acknowledged write is a liveness failure (A2: the
         * promotion dropped a member it could have kept; seed 76 before the old leader stayed in the ISR). */
        int holder = -1;
        for (int i = 0; i < S.nn && holder < 0 && S.st.acked; i++) {
            int all = 1;
            for (size_t j = 0; j < S.nops && all; j++) {
                const struct op *o = &S.ops[j];
                long r = log_find(&S.node[i], o->pos);
                if (o->status == OP_OK && (r < 0 || (size_t)r >= S.node[i].ndur || S.node[i].log[r].id != o->id))
                    all = 0;
            }
            if (all)
                holder = i;
        }
        if (holder >= 0 && !S.shrunk) {
            violation("liveness: the volume waits for an operator although node %d holds every acknowledged write "
                      "(metadata isr %#x names only lost disks, not left so by a leader's shrink)", holder, S.vm.isr);
            fprintf(stderr, "seed %" PRIu64 ": %s\n", seed, S.why);
            describe();
            return 1;
        }
        S.tot.waits++;
        uint64_t *a = (uint64_t *)&S.st, *b = (uint64_t *)&S.tot;
        for (size_t j = 0; j < sizeof S.st / sizeof(uint64_t); j++)
            b[j] += a[j];
        return 0;
    }
    if (!S.bad && phase != 3)
        violation("liveness: the volume did not converge after every fault healed (phase %d)", phase);
    if (!S.bad) {
        const struct node *L = &S.node[S.vm.leader];
        for (int i = 0; i < S.nn && !S.bad; i++) {
            const struct node *nd = &S.node[i];
            if (nd->n != L->n)
                violation("node %d has %zu records, the leader %zu", i, nd->n, L->n);
            for (size_t j = 0; j < nd->n && j < L->n && !S.bad; j++)
                if (nd->log[j].pos != L->log[j].pos || nd->log[j].id != L->log[j].id ||
                    nd->log[j].epoch != L->log[j].epoch || nd->log[j].n != L->log[j].n)
                    violation("node %d differs from the leader at record %zu (position %" PRIu64 ")", i, j,
                              nd->log[j].pos);
        }
        for (size_t j = 0; j < S.nops && !S.bad; j++) {
            const struct op *o = &S.ops[j];
            long r = log_find(L, o->pos);
            if (o->status == OP_OK && (r < 0 || L->log[r].id != o->id))
                violation("acknowledged write %" PRIu64 " at %" PRIu64 " is lost", o->id, o->pos);
        }
    }
    if (!S.bad && phase == 3) {
        /* Check 8, availability (A2) with every replica but the leader down: min ISR 1 must acknowledge (the lag
         * rule shrinks the ISR to the leader); a higher min ISR must not. */
        int l = S.vm.leader;
        S.st.deg_runs++;
        for (int i = 0; i < S.nn; i++)
            if (i != l) {
                node_crash(i, 0);
                S.node[i].down_until = UINT64_MAX;
            }
        S.ops = grow(S.ops, &S.cops, S.nops + 1, sizeof *S.ops);
        size_t w = S.nops;
        S.ops[w] = (struct op){.id = S.nops + 1, .n = 1, .node = l, .status = OP_PENDING, .at = S.now};
        S.cur_op = S.ops[w].id;
        if (repl_write(S.node[l].r, 1, &S.ops[w].pos) == REPL_OK) {
            S.node[l].pend = grow(S.node[l].pend, &S.node[l].cpend, S.node[l].npend + 1, sizeof *S.node[l].pend);
            S.node[l].pend[S.node[l].npend++] = S.nops++;
        }
        drive(l, NULL);
        for (int t = 0; t < 1000 && !S.bad && S.ops[w].status == OP_PENDING; t++)
            step();
        if (!S.bad && S.ops[w].status == OP_OK) {
            S.st.deg_ok++;
            if (S.min_isr > 1)
                violation("degraded: acknowledged with only the leader up at min ISR %d", S.min_isr);
        } else if (!S.bad && S.min_isr == 1) {
            violation("degraded: no acknowledgement with only the leader up at min ISR 1 (isr %#x)", S.vm.isr);
        }
    }
    if (S.bad) {
        fprintf(stderr, "seed %" PRIu64 ": %s\n", seed, S.why);
        describe();
    }
    uint64_t *a = (uint64_t *)&S.st, *b = (uint64_t *)&S.tot;
    for (size_t j = 0; j < sizeof S.st / sizeof(uint64_t); j++)
        b[j] += a[j];
    return S.bad;
}

int main(int argc, char **argv)
{
    uint64_t seeds = 200, first = 1, ticks = 3000;
    int quiet = 0;
    uint64_t mutant = 0;
    const char *mname = "";
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--seeds") && i + 1 < argc) {
            seeds = strtoull(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--first") && i + 1 < argc) {
            first = strtoull(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--seed") && i + 1 < argc) {
            first = strtoull(argv[++i], NULL, 10);
            seeds = 1;
        } else if (!strcmp(argv[i], "--ticks") && i + 1 < argc) {
            ticks = strtoull(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--trace")) {
            S.trace = 1;
        } else if (!strcmp(argv[i], "--quiet")) {
            quiet = 1;
        } else if (!strcmp(argv[i], "--mutant") && i + 1 < argc) {
            mname = argv[++i]; /* N or N,M,...: several planted together */
            for (const char *p = mname; *p;) {
                char *e;
                unsigned long b = strtoul(p, &e, 10);
                if (e == p || b == 0 || b > 63) {
                    fprintf(stderr, "sim_repl: bad --mutant %s\n", mname);
                    return 2;
                }
                mutant |= 1ull << b;
                p = *e == ',' ? e + 1 : e;
            }
        } else {
            fprintf(stderr, "usage: sim_repl [--seeds N] [--first S | --seed S] [--ticks T] [--trace] [--quiet]"
                            " [--mutant M]\n");
            return 2;
        }
    }
#ifdef REPL_MUTANTS
    repl_mutants = mutant;
#else
    if (mutant) {
        fprintf(stderr, "sim_repl: --mutant needs the build with -DREPL_MUTANTS (sim_repl_mut)\n");
        return 2;
    }
#endif
    for (uint64_t s = first; s < first + seeds; s++) {
        if (run(s, ticks)) {
            if (mutant) {
                printf("mutant %s: KILLED at seed %" PRIu64 ": %s\n", mname, s, S.why);
                reset();
                return 0;
            }
            fprintf(stderr, "sim_repl: FAILED at seed %" PRIu64 " (replay: --seed %" PRIu64 " --ticks %" PRIu64
                            " --trace)\n", s, s, ticks);
            reset();
            return 1;
        }
    }
    reset();
    if (mutant) {
        printf("mutant %s: SURVIVED %" PRIu64 " seeds\n", mname, seeds);
        return 1;
    }
    if (!quiet)
        printf("sim_repl: %" PRIu64 " seeds x %" PRIu64 " ticks ok: %" PRIu64 " writes acknowledged, %" PRIu64
               " refused, %" PRIu64 " indeterminate; %" PRIu64 " promotions (%" PRIu64 " stale, %" PRIu64
               " without candidate), %" PRIu64 " resignations, %" PRIu64 " ISR changes, %" PRIu64 " cuts; faults: %"
               PRIu64 " crashes, %" PRIu64 " wipes, %" PRIu64 " partitions, %" PRIu64 " pauses, %" PRIu64
               " moves; %" PRIu64 " messages; %" PRIu64 " runs ended waiting for an operator (no ISR disk left)\n",
               seeds, ticks, S.tot.acked, S.tot.refused, S.tot.unknown, S.tot.promotions, S.tot.stale, S.tot.none,
               S.tot.resigns, S.tot.isr, S.tot.cuts, S.tot.crashes, S.tot.wipes, S.tot.parts, S.tot.pauses,
               S.tot.moves, S.tot.msgs, S.tot.waits);
    if (!quiet)
        printf("coverage: %" PRIu64 " refusals, %" PRIu64 " excused cuts of committed records, acks with isr 1 / >1: %"
               PRIu64 " / %" PRIu64 ", %" PRIu64 " acked copies wiped, %" PRIu64 " wipes of the leader, %" PRIu64
               " fence replies from outside the fenced set, degraded runs %" PRIu64 " (ok %" PRIu64 "), %" PRIu64
               " trims, %" PRIu64 " snapshots, %" PRIu64 " syncs that span a cut, %" PRIu64 " replay floors checked\n",
               S.tot.refuses, S.tot.xcuts, S.tot.ack_isr1, S.tot.ack_isrn, S.tot.wiped_acked, S.tot.wipe_leader,
               S.tot.fence_rep_out, S.tot.deg_runs, S.tot.deg_ok, S.tot.trims, S.tot.snaps, S.tot.lies, S.tot.floors);
    if (!quiet)
        printf("set changes: %" PRIu64 " adds, %" PRIu64 " removes, %" PRIu64 " resets, %" PRIu64 " moves done (%"
               PRIu64 " leaves told, %" PRIu64 " leave requests), %" PRIu64 " reconfigurations, %" PRIu64
               " cores restarted, %" PRIu64 " messages of another set version dropped, %" PRIu64 " replay starts raised, %"
               PRIu64 " planned transfers off a leaving leader\n",
               S.tot.adds, S.tot.removes, S.tot.resets, S.tot.mvdone, S.tot.leaves, S.tot.leavereq, S.tot.reconfs,
               S.tot.restarts, S.tot.xver, S.tot.replays, S.tot.planned);
    /* Always printed: the soak judges the transfers' coverage from it. */
    printf("transfers: %" PRIu64 " started, %" PRIu64 " READY, %" PRIu64 " done, %" PRIu64 " cancelled before READY, %"
           PRIu64 " drains timed out, %" PRIu64 " hard caps, %" PRIu64 " writes refused while held, %" PRIu64
           " dropped into a failover, %" PRIu64 " new metadata leaders, %" PRIu64 " metadata stalls on READY, %" PRIu64
           " writes admitted after the hard cap (%" PRIu64 " of them cut), %" PRIu64 " stale CANCELs\n",
           S.tot.xfers, S.tot.xready, S.tot.xdone, S.tot.xcancels, S.tot.xdrainout, S.tot.xcap, S.tot.xrefused,
           S.tot.xfailover, S.tot.metachg, S.tot.xstalls, S.tot.xlate, S.tot.xlatecuts, S.tot.xstale);
    /* Floors: over enough seeds every path of check 11 must have run, or a model that stopped reaching it passes. */
    if (seeds >= XF_FLOOR_SEEDS &&
        !(S.tot.xready && S.tot.xdone && S.tot.xcap && S.tot.xlate && S.tot.xlatecuts && S.tot.xstale)) {
        fprintf(stderr, "sim_repl: transfer coverage below its floor over %" PRIu64 " seeds (READY, done, hard caps, "
                        "writes after the cap, their cuts and stale CANCELs must each be above 0)\n", seeds);
        return 1;
    }
    return 0;
}
