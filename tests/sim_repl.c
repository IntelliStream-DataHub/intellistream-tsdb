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
#define META    NM            /* the metadata group's index in the link matrix */
#define SIM_HB  100           /* a server's heartbeat to the metadata group (not the core's) */

#define HB_TICKS      5
#define NODE_TIMEOUT  40
#define WATCH_TICKS   10
#define FENCE_WAIT    15

struct srec {
    uint64_t pos, epoch, id, wseq;
    uint32_t n;
};

struct node {
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
    int snap_src;             /* a snapshot from this node is due at snap_at (0: none asked) */
    uint64_t snap_at;
    uint64_t applied, last_epoch;
    size_t *pend;             /* indexes into ops of writes waiting for their reply */
    size_t npend, cpend;
};

struct smsg {
    struct repl_msg m;
    struct repl_rec *rec;
    uint64_t *ids;
    uint64_t at;
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
    struct repl_fence_reply rep[NM];
    int nrep;
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
            fence_rep_out, deg_runs, deg_ok, trims, snaps, lies;
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
    const struct node *nd = ctx;
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
    if (link_down(from, to) || chance(S.loss)) {
        free(m->rec);
        free(m->ids);
        return;
    }
    S.st.msgs++;
    m->at = S.now + 1 + rn(S.maxdelay);
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
        d.at = S.now + 1 + rn(S.maxdelay * 3);
        S.q[S.nq++] = d;
    }
}

static int addr(int x)
{
    return x == REPL_META ? META : x;
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
    if ((t == REPL_M_FENCE_REPLY || t == REPL_M_RESIGN) && a->msg.epoch > nd->fence)
        violation("node %d answers fence %" PRIu64 " before saving it (saved %" PRIu64 ")", i, a->msg.epoch,
                  nd->fence);
    if (t == REPL_M_APPEND && a->upto > a->msg.prev_pos && a->msg.prev_pos + 1 < nd->start)
        violation("node %d sends from %" PRIu64 ", below its log start %" PRIu64, i, a->msg.prev_pos + 1,
                  nd->start);
    struct smsg m = {.m = a->msg};
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
    enqueue(&m, i, addr(a->msg.to));
}

static void meta_send(int to, int type, uint64_t epoch)
{
    struct smsg m = {0};
    m.m.type = type;
    m.m.from = REPL_META;
    m.m.to = to;
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
            if (copies < S.min_isr)
                violation("write %" PRIu64 " acknowledged at %" PRIu64 " with %d durable copies, min ISR %d", o->id,
                          p, copies, S.min_isr);
        }
        uint64_t e = repl_epoch(nd->r);
        S.acker = grow(S.acker, &S.cacker, e + 1, sizeof *S.acker);
        if (S.acker[e] && S.acker[e] != i + 1)
            violation("nodes %d and %d both acknowledged in epoch %" PRIu64, S.acker[e] - 1, i, e);
        S.acker[e] = i + 1;
        trace("node %d: OK write %" PRIu64 " at %" PRIu64 " epoch %" PRIu64, i, o->id, o->pos, e);
        return;
    }
    violation("node %d replied for %" PRIu64 ", which no writer waits for", i, a->first);
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
        case REPL_A_SNAPSHOT:
            if (a.pos >= (uint64_t)S.nn || (int)a.pos == i) {
                violation("node %d asks for a snapshot from node %" PRIu64, i, a.pos);
                return;
            }
            if (!nd->snap_src) { /* a repeat while a copy is in progress is ignored */
                nd->snap_src = (int)a.pos + 1;
                nd->snap_at = S.now + 1 + rn(10);
            }
            trace("node %d: snapshot from node %" PRIu64 " asked", i, a.pos);
            break;
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
    nd->r = repl_open(&nd->cfg, &d);
    free(h);
    if (!nd->r) {
        violation("node %d: repl_open failed", i);
        return;
    }
    nd->applied = 0;
    nd->last_epoch = 0;
    nd->sync_busy = 0;
    nd->pause_until = 0;
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
        nd->start = 0;
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
    int s = nd->snap_src - 1;
    nd->snap_src = 0;
    const struct node *src = &S.node[s];
    if (!src->r || !alive(s) || link_down(s, i) || link_down(i, s) || src->applied <= log_end(nd))
        return; /* the leader tells it again */
    uint64_t p = src->applied;
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
    nd->start = p + 1;
    nd->floor = p;
    S.st.snaps++;
    trace("node %d: snapshot from node %d at %" PRIu64, i, s, p);
    node_open(i);
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

static void meta_tick(void)
{
    if (S.now % WATCH_TICKS == 0)
        meta_broadcast();
    if (!S.fo && S.vm.leader >= 0 && S.now - S.last_hb[S.vm.leader] > NODE_TIMEOUT)
        fo_begin("leader silent");
    if (!S.fo || (S.nrep < popcount32(S.fo_fenced) && S.now - S.fo_start < FENCE_WAIT))
        return;
    uint32_t old = S.vm.isr;
    uint64_t oldrid[NM];
    memcpy(oldrid, S.vm.rid, sizeof oldrid);
    int rc = repl_meta_promote(&S.vm, S.fo_epoch, S.fo_version, S.fo_fenced, S.rep, S.nrep);
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

static void meta_recv(const struct repl_msg *m)
{
    if (m->from < 0 || m->from >= S.nn)
        return;
    switch (m->type) {
    case SIM_HB:
        S.last_hb[m->from] = S.now;
        break;
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

static void deliver(struct smsg *m)
{
    int to = addr(m->m.to);
    if (link_down(addr(m->m.from), to)) {
        /* cut while in flight */
    } else if (to == META) {
        meta_recv(&m->m);
    } else if (to >= 0 && to < S.nn && S.node[to].r) {
        m->m.rec = m->rec;
        repl_recv(S.node[to].r, &m->m);
        drive(to, m);
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
    } else if (k < 76) {
        S.st.moves++;
        fo_begin("moved");
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
        int to = addr(m->m.to);
        if (m->at <= S.now && !(to < S.nn && S.node[to].pause_until > S.now)) {
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
        if (S.trim && chance(50) && n->floor + 1 > n->start && n->floor <= dur_end(n)) {
            n->start = n->floor + 1; /* segments at or below the saved floor went into parts and are deleted */
            S.st.trims++;
        }
        repl_tick(n->r);
        drive(i, NULL);
        if (S.now % HB_TICKS == (uint64_t)i % HB_TICKS) {
            struct smsg m = {0};
            m.m.type = SIM_HB;
            m.m.from = i;
            m.m.to = REPL_META;
            enqueue(&m, i, META);
        }
    }
    if (!S.bad)
        meta_tick();
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
    int trace_on = S.trace;
    __typeof__(S.tot) tot = S.tot;
    memset(&S, 0, sizeof S);
    S.tot = tot;
    S.trace = trace_on;
}

static int converged(void)
{
    int l = S.vm.leader;
    if (S.fo || l < 0 || !S.node[l].r || !repl_is_leader(S.node[l].r) || repl_epoch(S.node[l].r) != S.vm.epoch)
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
    fprintf(stderr, "  meta: epoch %" PRIu64 " leader %d isr %#x version %" PRIu64 " failover %d\n", S.vm.epoch,
            S.vm.leader, S.vm.isr, S.vm.isr_version, S.fo);
    for (int i = 0; i < S.nn; i++) {
        const struct node *nd = &S.node[i];
        if (!nd->r) {
            fprintf(stderr, "  node %d: down, log end %" PRIu64 "\n", i, log_end(nd));
            continue;
        }
        fprintf(stderr,
                "  node %d: %s epoch %" PRIu64 " written %" PRIu64 " durable %" PRIu64 " applied %" PRIu64
                " commit %" PRIu64 " fence %" PRIu64 " rid %" PRIu64 "%s\n",
                i, repl_is_leader(nd->r) ? "leader" : "replica", repl_epoch(nd->r), repl_written(nd->r),
                repl_durable_end(nd->r), repl_applied(nd->r), repl_commit(nd->r), nd->fence, nd->rid,
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
    if (chance(200))
        S.maxsync = 1 + rn(30); /* a slow fdatasync against a fast round trip: syncs span cuts */
    S.vm = (struct repl_vmeta){.epoch = 1, .isr_version = 1, .leader = 0, .isr = (1u << S.nn) - 1};
    for (int i = 0; i < S.nn; i++) {
        struct node *nd = &S.node[i];
        nd->rid = ++S.next_rid;
        S.vm.rid[i] = nd->rid;
        nd->cfg = (struct repl_config){.self = i, .nnodes = S.nn, .min_isr = min_isr, .isr_lag_ticks = 30,
                                       .resend_ticks = 10, .heartbeat_ticks = 3, .floor_ticks = 20,
                                       .unanswered_ticks = 20, .max_batch = batch, .window = 32 * batch, .rec_find = rec_find, .ctx = nd};
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
    for (int i = 0; i < S.nn; i++) {
        S.node[i].pause_until = 0;
        S.node[i].down_until = 0;
    }
    uint64_t longest = 0;
    for (int i = 0; i < S.nn; i++)
        if (log_end(&S.node[i]) > longest)
            longest = log_end(&S.node[i]);
    uint64_t deadline = S.now + 3000 + 2 * longest; /* a wiped replica copies the whole log (no snapshot here) */
    size_t final = 0;
    int phase = 0;
    while (!S.bad && S.now < deadline) {
        step();
        if (phase == 0 && converged()) {
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
        } else if (phase == 2 && converged()) {
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
               " trims, %" PRIu64 " snapshots, %" PRIu64 " syncs that span a cut\n",
               S.tot.refuses, S.tot.xcuts, S.tot.ack_isr1, S.tot.ack_isrn, S.tot.wiped_acked, S.tot.wipe_leader,
               S.tot.fence_rep_out, S.tot.deg_runs, S.tot.deg_ok, S.tot.trims, S.tot.snaps, S.tot.lies);
    return 0;
}
