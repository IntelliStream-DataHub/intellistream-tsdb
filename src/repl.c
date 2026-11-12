/* SPDX-License-Identifier: AGPL-3.0-only */
/* The replication core of a cluster volume: src/repl.h has the design, docs/design-cluster.md the reasons.
 *
 * State of one replica:
 * - its log: positions 1..written, of which 1..durable are synced, 1..applied applied (the commit floor), and the
 *   epoch history of the log (hist, derived from the records: cut with them);
 * - its view: `epoch` and `leader` it follows or leads, `fence` (the highest epoch it has saved: it refuses records of
 *   any lower one), `led` (the highest epoch it has led: never led again after a restart), the metadata record of
 *   its epoch when it has one (`meta`);
 * - as a follower, `fstate`: IDLE (no leader to follow), TRUNC (asking the leader where its log diverges), FOLLOW
 *   (its log is a prefix of the leader's: `verified` == written);
 * - as a leader, one `struct peer` per other replica, the ISR request in flight, the writers waiting for their
 *   reply, and a ring of its written end per tick (the lag rule).
 *
 * Every function that changes state pushes the actions that make it so, in the order they must be carried out: a
 * SAVE before anything that relies on the fence, a WRITE before the SEND that ships it, an APPLY before the REPLY.
 *
 * MUT(n) marks the planted bugs of the mutation runs (tests/sim_repl.c --mutant N); it is 0 in every other build.
 */
#include "repl.h"

#include <stdlib.h>
#include <string.h>

#ifdef REPL_MUTANTS
uint64_t repl_mutants;
#define MUT(n) ((repl_mutants >> (n)) & 1)
#else
#define MUT(n) 0
#endif

enum { F_IDLE, F_TRUNC, F_FOLLOW };

struct peer {
    uint64_t next;      /* next position to send; 0: unknown until its first ACK in this epoch */
    uint64_t match;     /* durable end it acknowledged in this epoch, as instance `rid` */
    uint64_t acked;     /* written end it last reported */
    uint64_t rid;       /* the instance its ACKs came from; 0: none yet in this epoch */
    uint64_t last_send, last_ack;
    uint64_t snap_sent; /* when it was last told to install a snapshot (its next position is not retained) */
};

struct pending {
    uint64_t first, last;
};

struct repl {
    struct repl_config cfg;
    uint64_t rid, fence, led, floor_saved, floor_tick;
    struct repl_epoch *hist;
    uint32_t nhist, chist;
    uint64_t written, durable, applied, commit, verified;
    uint64_t tail;      /* first position of the last record while its WRITE is still queued; 0: ask the driver */
    int sync_busy;
    uint64_t sync_end;  /* the written end the SYNC in flight was issued for, lowered by every cut since */
    uint64_t now;

    uint64_t epoch;
    int leader, leading, fstate;
    struct repl_vmeta meta;
    int have_meta;
    uint64_t qseq, last_query;
    int resigning;
    uint64_t last_resign;
    uint32_t unans;     /* leader: members the promotion kept without a fence reply (meta.unanswered) */

    struct peer peer[REPL_MAX_NODES];
    uint64_t lead_tick;
    uint64_t *wend;
    uint32_t nwend;
    int isr_busy;
    struct repl_vmeta isr_req;
    uint64_t isr_sent;
    struct pending *pend;
    size_t npend, cpend, hpend;

    struct repl_action *act;
    size_t nact, cact, hact;
    int failed, fail_told;
    const char *fail_reason;
};

/* ------------------------------------------------------------------ helpers */

static void fail(struct repl *r, const char *why)
{
    if (!r->failed) {
        r->failed = 1;
        r->fail_reason = why;
    }
    r->leading = 0;
    r->fstate = F_IDLE;
}

static struct repl_action *push(struct repl *r, int type)
{
    if (r->failed)
        return NULL;
    if (r->nact == r->cact) {
        size_t c = r->cact ? r->cact * 2 : 64;
        struct repl_action *a = realloc(r->act, c * sizeof *a);
        if (!a) {
            fail(r, "out of memory");
            return NULL;
        }
        r->act = a;
        r->cact = c;
    }
    struct repl_action *a = &r->act[r->nact++];
    memset(a, 0, sizeof *a);
    a->type = type;
    return a;
}

static struct repl_msg *send(struct repl *r, int type, int to)
{
    struct repl_action *a = push(r, REPL_A_SEND);
    if (!a)
        return NULL;
    a->msg.type = type;
    a->msg.from = r->cfg.self;
    a->msg.to = to;
    a->msg.epoch = r->epoch;
    a->msg.leader = -1;
    return &a->msg;
}

static int popcount32(uint32_t x)
{
    int n = 0;
    for (; x; x &= x - 1)
        n++;
    return n;
}

static uint64_t min64(uint64_t a, uint64_t b)
{
    return a < b ? a : b;
}

static uint64_t last_epoch(const struct repl *r)
{
    return r->nhist ? r->hist[r->nhist - 1].epoch : 0;
}

/* The epoch of the record holding `pos` (0 for position 0 or an empty log). */
static uint64_t epoch_at(const struct repl *r, uint64_t pos)
{
    uint32_t lo = 0, hi = r->nhist;
    while (lo < hi) {
        uint32_t mid = (lo + hi) / 2;
        if (r->hist[mid].start <= pos)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo ? r->hist[lo - 1].epoch : 0;
}

static int hist_push(struct repl *r, uint64_t epoch, uint64_t start)
{
    if (epoch <= last_epoch(r))
        return 0;
    if (r->nhist == r->chist) {
        uint32_t c = r->chist ? r->chist * 2 : 16;
        struct repl_epoch *h = realloc(r->hist, c * sizeof *h);
        if (!h) {
            fail(r, "out of memory");
            return -1;
        }
        r->hist = h;
        r->chist = c;
    }
    r->hist[r->nhist].epoch = epoch;
    r->hist[r->nhist].start = start;
    r->nhist++;
    return 0;
}

/* The last position of epoch `e` and below in this log: one before the first epoch above it starts. */
static uint64_t end_of_epoch(const struct repl *r, uint64_t e)
{
    for (uint32_t i = 0; i < r->nhist; i++)
        if (r->hist[i].epoch > e)
            return MUT(16) ? r->hist[i].start : r->hist[i].start - 1;
    return r->written;
}

static void save(struct repl *r)
{
    struct repl_action *a = push(r, REPL_A_SAVE);
    if (!a)
        return;
    a->fence = r->fence;
    a->led = r->led;
    a->floor = r->applied;
    r->floor_saved = r->applied;
    r->floor_tick = r->now;
}

static void raise_fence(struct repl *r, uint64_t e)
{
    if (e > r->fence) {
        r->fence = e;
        if (!MUT(22))
            save(r);
    }
}

static void sync_maybe(struct repl *r)
{
    if (!r->sync_busy && r->written > r->durable && push(r, REPL_A_SYNC)) {
        r->sync_busy = 1;
        r->sync_end = r->written;
    }
}

/* The record holding `pos`: 0; 1 if the log no longer retains it (in parts, below the log's start). The last record
 * the core wrote itself is answered here: its WRITE may still wait in the action queue (repl_write ships it in the
 * same call), so the driver's index does not have it yet. */
static int find_rec(struct repl *r, uint64_t pos, uint64_t *s, uint32_t *n)
{
    if (r->tail && pos >= r->tail && pos <= r->written) {
        *s = r->tail;
        *n = (uint32_t)(r->written - r->tail + 1);
        return 0;
    }
    int rc = r->cfg.rec_find(r->cfg.ctx, pos, s, n);
    if (rc == 1 && !MUT(36))
        return 1;
    if (rc != 0 || *s > pos || *s + *n - 1 < pos) {
        fail(r, "the log has no record at a position below its end");
        return -1;
    }
    return 0;
}

/* Keep positions 1..end (rounded down to a record end); never below the applied end. 1 if something was cut.
 * The applied end is a record end, and the log may no longer retain it (the driver trims at or below the floor):
 * no lookup there. Rounding happens: a replica whose last epoch the leader lacks cuts at the end of the leader's
 * epoch before it, which may fall inside a record of an older epoch that only the replica has (tests/test_repl.c);
 * the next question cuts further. */
static int cut(struct repl *r, uint64_t end)
{
    if (end >= r->written)
        return 0;
    if (end > r->applied || (MUT(36) && end > 0)) {
        uint64_t s;
        uint32_t n;
        int rc = find_rec(r, end, &s, &n);
        if (rc < 0)
            return -1;
        if (rc > 0) {
            fail(r, "the log does not retain a position above its applied end");
            return -1;
        }
        if (s + n - 1 != end)
            end = MUT(24) ? s + n - 1 : s - 1;
    }
    if (end < r->applied) {
        fail(r, "a cut below the applied end");
        return -1;
    }
    struct repl_action *a = push(r, REPL_A_TRUNCATE);
    if (!a)
        return -1;
    a->pos = end;
    r->written = end;
    r->tail = 0;
    r->durable = min64(r->durable, end);
    r->sync_end = min64(r->sync_end, end);
    r->verified = min64(r->verified, end);
    while (r->nhist && r->hist[r->nhist - 1].start > end)
        r->nhist--;
    return 1;
}

/* ------------------------------------------------------------------ follower */

static void query(struct repl *r)
{
    struct repl_msg *m = send(r, REPL_M_QUERY, r->leader);
    if (!m)
        return;
    m->last_epoch = last_epoch(r);
    m->written = r->written;
    m->rid = r->rid;
    m->end = ++r->qseq;
    r->last_query = r->now;
}

static void ack(struct repl *r, int resync)
{
    if (r->leading || r->fstate != F_FOLLOW)
        return;
    struct repl_msg *m = send(r, REPL_M_ACK, r->leader);
    if (!m)
        return;
    m->durable = MUT(9) ? r->verified : min64(r->durable, r->verified);
    if (MUT(17) && m->durable && m->durable < r->verified)
        m->durable++;
    m->written = r->verified;
    m->rid = r->rid;
    m->resync = resync;
}

static void apply_to(struct repl *r, uint64_t t)
{
    if (t > r->applied) {
        struct repl_action *a = push(r, REPL_A_APPLY);
        if (!a)
            return;
        a->pos = t;
        r->applied = t;
    }
}

static void apply_follower(struct repl *r)
{
    uint64_t t = MUT(21) ? r->commit : min64(r->commit, r->durable);
    if (!MUT(3))
        t = min64(t, r->fstate == F_FOLLOW ? r->verified : r->applied);
    apply_to(r, t);
}

static void step_down(struct repl *r)
{
    if (!r->leading)
        return;
    r->leading = 0;
    for (size_t i = r->hpend; i < r->npend; i++) {
        struct repl_action *a = push(r, REPL_A_REPLY);
        if (!a)
            break;
        a->first = r->pend[i].first;
        a->pos = r->pend[i].last;
        a->status = REPL_MOVED;
    }
    r->npend = r->hpend = 0;
    r->isr_busy = 0;
}

/* Follow `leader` in `epoch`: first find where our logs part (QUERY / ANSWER), then take appends. */
static void follow(struct repl *r, uint64_t epoch, int leader)
{
    step_down(r);
    r->epoch = epoch;
    r->leader = leader;
    r->resigning = 0;
    r->fstate = F_TRUNC;
    r->verified = r->applied;
    query(r);
}

/* A higher epoch whose leader we do not know (a QUERY, an ACK, a REFUSE without leader, a FENCE). */
static void deposed(struct repl *r, uint64_t e)
{
    raise_fence(r, e);
    step_down(r);
    if (r->epoch < r->fence)
        r->fstate = F_IDLE;
}

static void on_append(struct repl *r, const struct repl_msg *m)
{
    if (!MUT(2) && (m->epoch < r->epoch || (m->epoch < r->fence && !MUT(26)))) {
        struct repl_msg *s = send(r, REPL_M_REFUSE, m->from);
        if (s) {
            s->epoch = r->epoch > r->fence ? r->epoch : r->fence;
            s->leader = s->epoch == r->epoch && (r->leading || r->fstate != F_IDLE) ? r->leader : -1;
        }
        return;
    }
    if (m->epoch > r->epoch) {
        raise_fence(r, m->epoch);
        follow(r, m->epoch, m->from);
        return;
    }
    if (r->leading)
        return;
    if (MUT(3) && m->commit > r->commit) {
        r->commit = m->commit;
        apply_follower(r);
    }
    if (r->fstate != F_FOLLOW || m->from != r->leader)
        return;
    if (m->prev_pos > r->written) {
        ack(r, 1);
        return;
    }
    /* While following, the log is a prefix of the leader's (verified by QUERY / ANSWER, then grown only from the
     * leader), and a leader never rewrites its epoch's log (one instance per epoch: `led`, rid). So another epoch at
     * a position both logs have is damage or a second instance leading one epoch (a restored directory, a gap of the
     * design): never silent, never repaired by a cut. The simulator never reaches these two (MUT(30)). */
    if (!MUT(30) && m->prev_pos && epoch_at(r, m->prev_pos) != m->prev_epoch) {
        fail(r, "the log differs from the leader's below the verified end");
        return;
    }
    int wrote = 0;
    for (uint32_t i = 0; i < m->nrec; i++) {
        const struct repl_rec *rec = &m->rec[i];
        if (rec->pos <= r->written) {
            /* One leader per epoch writes each position once: the same epoch here is the same record. */
            if (epoch_at(r, rec->pos) == rec->epoch)
                continue;
            fail(r, "the log differs from the leader's below the verified end");
            return;
        }
        if (rec->pos != r->written + 1 || rec->n == 0)
            break;
        if (rec->epoch < last_epoch(r)) {
            r->fstate = F_TRUNC;
            r->verified = r->applied;
            query(r);
            return;
        }
        struct repl_action *a = push(r, REPL_A_WRITE);
        if (!a || hist_push(r, rec->epoch, rec->pos) < 0)
            return;
        a->rec = *rec;
        a->src = REPL_SRC_MSG;
        a->idx = i;
        r->written = rec->pos + rec->n - 1;
        r->tail = rec->pos;
        wrote = 1;
    }
    r->verified = r->written;
    if (m->commit > r->commit)
        r->commit = m->commit;
    if (wrote)
        sync_maybe(r);
    apply_follower(r);
    ack(r, m->nrec == 0);
}

static void on_answer(struct repl *r, const struct repl_msg *m)
{
    if (r->leading || ((m->epoch != r->epoch || m->from != r->leader) && !MUT(20)) || r->fstate != F_TRUNC)
        return;
    /* Only an answer about the log as it is now: one to an earlier query (a query number restarts with the
     * process) may be about a log that has grown or been cut since, and its end would cut what matches. For the
     * same last epoch and end, an older answer is as good as a new one: the leader's end of an epoch only grows,
     * and never below what the asker got from it. */
    if (MUT(15) ? m->end != r->qseq : m->prev_epoch != last_epoch(r) || m->prev_pos != r->written)
        return;
    uint64_t keep = min64(m->written, end_of_epoch(r, m->last_epoch));
    if (keep < r->written && !MUT(4)) {
        if (cut(r, keep) < 0)
            return;
        query(r);
        return;
    }
    r->fstate = F_FOLLOW;
    r->verified = r->written;
    apply_follower(r);
    ack(r, 1);
}

/* ------------------------------------------------------------------ leader */

static uint64_t wend_at(const struct repl *r, uint64_t tick)
{
    return r->wend[tick % r->nwend];
}

static void advance(struct repl *r)
{
    if (!r->leading)
        return;
    uint32_t set = r->meta.isr;
    if (r->isr_busy && !MUT(6))
        set |= r->isr_req.isr;
    if (r->isr_busy && MUT(18))
        set = r->isr_req.isr;
    uint64_t c = r->durable;
    for (int i = 0; i < r->cfg.nnodes; i++) {
        if (i == r->cfg.self || !(set & (1u << i)))
            continue;
        const struct peer *p = &r->peer[i];
        uint64_t want = (r->meta.isr & (1u << i)) ? r->meta.rid[i] : r->isr_req.rid[i];
        if (MUT(1) && p->rid == 0)
            continue;
        c = min64(c, p->rid == want ? p->match : 0);
    }
    if (c > r->commit)
        r->commit = c;
    apply_to(r, min64(r->commit, r->durable));
    if (popcount32(r->meta.isr) < r->cfg.min_isr && !MUT(31))
        return;
    uint64_t done = MUT(7) ? r->durable : r->applied;
    while (r->hpend < r->npend && r->pend[r->hpend].last <= done) {
        struct repl_action *a = push(r, REPL_A_REPLY);
        if (!a)
            return;
        a->first = r->pend[r->hpend].first;
        a->pos = r->pend[r->hpend].last;
        a->status = REPL_OK;
        r->hpend++;
    }
    if (r->hpend == r->npend)
        r->hpend = r->npend = 0;
}

static void send_append(struct repl *r, int to, int heartbeat)
{
    struct peer *p = &r->peer[to];
    uint64_t from = p->next ? p->next : r->written + 1, upto = from - 1;
    if (p->next && from <= r->written && from - 1 <= p->acked + r->cfg.window) {
        uint64_t s;
        uint32_t n;
        int rc = find_rec(r, from, &s, &n);
        if (rc < 0)
            return;
        if (rc > 0) {
            /* Its next record went into parts and its segment is gone (§8.3): the replica needs a snapshot (§8.1
             * step 3). Never a fail-stop: every next leader would meet the same replica. */
            if (!p->snap_sent || r->now - p->snap_sent >= r->cfg.resend_ticks) {
                struct repl_msg *m = send(r, REPL_M_SNAPSHOT, to);
                if (m)
                    m->written = from - 1;
                p->snap_sent = r->now;
                p->last_send = r->now;
            }
            return;
        }
        upto = r->written;
        if (upto - from + 1 > r->cfg.max_batch) {
            upto = from + r->cfg.max_batch - 1;
            if (find_rec(r, upto, &s, &n) < 0)
                return;
            upto = s + n - 1;
        }
    }
    if (upto < from && !heartbeat)
        return;
    struct repl_action *a = push(r, REPL_A_SEND);
    if (!a)
        return;
    a->msg.type = REPL_M_APPEND;
    a->msg.from = r->cfg.self;
    a->msg.to = to;
    a->msg.epoch = r->epoch;
    a->msg.leader = r->cfg.self;
    a->msg.prev_pos = from - 1;
    a->msg.prev_epoch = epoch_at(r, from - 1);
    a->msg.commit = r->commit;
    a->upto = upto;
    if (upto >= from)
        p->next = upto + 1;
    p->last_send = r->now;
}

static void replicate(struct repl *r)
{
    for (int i = 0; i < r->cfg.nnodes; i++)
        if (i != r->cfg.self && r->peer[i].next && r->peer[i].next <= r->written)
            send_append(r, i, 0);
}

static void isr_request(struct repl *r, uint32_t isr, int add, uint64_t rid)
{
    r->isr_req = r->meta;
    r->isr_req.isr = isr;
    if (add >= 0)
        r->isr_req.rid[add] = rid;
    for (int i = 0; i < REPL_MAX_NODES; i++)
        if (!(isr & (1u << i)))
            r->isr_req.rid[i] = 0;
    r->isr_busy = 1;
    r->isr_sent = r->now;
    struct repl_msg *m = send(r, REPL_M_ISR_REQ, REPL_META);
    if (m)
        m->meta = r->isr_req;
}

/* One ISR change at a time: drop a member that lags or answers as another instance, else add a caught-up one. */
static void isr_check(struct repl *r)
{
    if (r->isr_busy || r->meta.epoch != r->epoch)
        return;
    uint32_t isr = r->meta.isr;
    int lagged = r->now - r->lead_tick >= r->cfg.isr_lag_ticks;
    uint64_t old = lagged ? wend_at(r, r->now - r->cfg.isr_lag_ticks) : 0;
    /* A member the promotion kept without a fence reply (most often the old leader, dead) that has not acknowledged
     * soon after the takeover goes at once, not after isr_lag: until then it holds the commit point. */
    int grace = r->cfg.unanswered_ticks && r->now - r->lead_tick >= r->cfg.unanswered_ticks && !MUT(37);
    for (int i = 0; i < r->cfg.nnodes; i++) {
        if (i == r->cfg.self || !(isr & (1u << i)))
            continue;
        const struct peer *p = &r->peer[i];
        if ((p->rid && p->rid != r->meta.rid[i]) ||
            (lagged && !MUT(29) && (p->rid != r->meta.rid[i] || p->match < old)) ||
            (grace && (r->unans & (1u << i)) && !p->rid)) {
            isr_request(r, isr & ~(1u << i), -1, 0);
            return;
        }
    }
    for (int i = 0; i < r->cfg.nnodes; i++) {
        if (i == r->cfg.self || (isr & (1u << i)))
            continue;
        const struct peer *p = &r->peer[i];
        /* Its log must also reach the start of this epoch (Kafka's leaderEpochStartOffset): a new leader's commit
         * point starts at what it had applied, below what its predecessor may have committed, and every position
         * up to its EPOCH marker may be committed already. */
        uint64_t start = r->nhist ? r->hist[r->nhist - 1].start : 0;
        if (p->rid && p->match >= r->commit && p->match >= old && (p->match >= start || MUT(12))) {
            isr_request(r, isr | (1u << i), i, p->rid);
            return;
        }
    }
}

static void lead(struct repl *r, const struct repl_vmeta *v)
{
    r->epoch = v->epoch;
    r->leader = r->cfg.self;
    if (r->fence < v->epoch)
        r->fence = v->epoch;
    if (MUT(23)) {
        save(r);
        r->led = v->epoch;
    } else {
        r->led = v->epoch;
        save(r);
    }
    r->leading = 1;
    r->fstate = F_IDLE;
    r->resigning = 0;
    r->unans = v->unanswered & ~(1u << r->cfg.self);
    r->isr_busy = 0;
    r->npend = r->hpend = 0;
    memset(r->peer, 0, sizeof r->peer);
    for (int i = 0; i < r->cfg.nnodes; i++)
        r->peer[i].last_ack = r->now;
    r->lead_tick = r->now;
    for (uint32_t i = 0; i < r->nwend; i++)
        r->wend[i] = r->written;
    if (r->commit < r->applied)
        r->commit = r->applied;
    /* The epoch's first record; the old epoch's tail stays before it (§6.2 step 3). */
    struct repl_action *a = push(r, REPL_A_WRITE);
    if (!a || hist_push(r, r->epoch, r->written + 1) < 0)
        return;
    a->rec.pos = r->written + 1;
    a->rec.epoch = r->epoch;
    a->rec.n = 1;
    a->src = REPL_SRC_EPOCH;
    r->tail = ++r->written;
    sync_maybe(r);
    for (int i = 0; i < r->cfg.nnodes; i++)
        if (i != r->cfg.self)
            send_append(r, i, 1);
}

static void resign(struct repl *r)
{
    struct repl_msg *m = send(r, REPL_M_RESIGN, REPL_META);
    if (m)
        m->rid = r->rid;
    r->last_resign = r->now;
}

static void adopt_meta(struct repl *r, const struct repl_vmeta *v)
{
    if (v->epoch < r->epoch)
        return;
    if (v->epoch == r->epoch) {
        if (r->have_meta && r->meta.epoch == v->epoch && v->isr_version <= r->meta.isr_version)
            return;
        r->meta = *v;
        r->have_meta = 1;
        if (r->leading) {
            if (r->isr_busy && v->isr_version > r->isr_req.isr_version)
                r->isr_busy = 0;
            advance(r);
        }
        return;
    }
    if (v->epoch < r->fence && !MUT(26))
        return; /* a promotion past this record is under way */
    raise_fence(r, v->epoch);
    r->meta = *v;
    r->have_meta = 1;
    if (v->leader != r->cfg.self) {
        follow(r, v->epoch, v->leader);
    } else if ((r->led >= v->epoch && !MUT(5)) || (v->rid[r->cfg.self] != r->rid && !MUT(13))) {
        /* Led before a restart, or the record names another instance of this replica (a wiped disk: its `led` is
         * gone with it). Either may have sent records of this epoch that its log no longer has. */
        step_down(r);
        r->epoch = v->epoch;
        r->leader = r->cfg.self;
        r->fstate = F_IDLE;
        r->resigning = 1;
        resign(r);
    } else {
        lead(r, v);
    }
}

/* ------------------------------------------------------------------ events */

static void on_ack(struct repl *r, const struct repl_msg *m)
{
    if (m->epoch > r->epoch) {
        deposed(r, m->epoch);
        return;
    }
    if (!r->leading || (m->epoch != r->epoch && !MUT(8)) || m->from < 0 || m->from >= r->cfg.nnodes)
        return;
    struct peer *p = &r->peer[m->from];
    if (m->rid != p->rid) {
        p->rid = m->rid;
        if (!MUT(19))
            p->match = 0;
    }
    if (m->durable > p->match)
        p->match = m->durable;
    p->acked = m->written;
    p->last_ack = r->now;
    if (m->resync || !p->next)
        p->next = m->written + 1;
    advance(r);
    if (p->next <= r->written)
        send_append(r, m->from, 0);
}

static void on_query(struct repl *r, const struct repl_msg *m)
{
    if (m->epoch > r->epoch) {
        deposed(r, m->epoch);
        return;
    }
    if (!r->leading || m->epoch != r->epoch)
        return;
    /* KIP-279: the largest epoch of ours not above the asker's last one, and where it ends here. */
    uint64_t e = 0;
    for (uint32_t i = 0; i < r->nhist && r->hist[i].epoch <= m->last_epoch; i++)
        e = r->hist[i].epoch;
    struct repl_msg *s = send(r, REPL_M_ANSWER, m->from);
    if (!s)
        return;
    s->leader = r->cfg.self;
    s->last_epoch = e;
    s->written = e ? end_of_epoch(r, e) : 0;
    s->end = m->end;
    s->prev_epoch = m->last_epoch;
    s->prev_pos = m->written;
}

static void on_fence(struct repl *r, const struct repl_msg *m)
{
    if (m->epoch > r->epoch)
        deposed(r, m->epoch);
    if (r->fence < m->epoch)
        return; /* cannot happen: deposed() saved it */
    struct repl_msg *s = send(r, REPL_M_FENCE_REPLY, REPL_META);
    if (!s)
        return;
    s->epoch = m->epoch;
    s->durable = r->durable;
    s->last_epoch = epoch_at(r, r->durable);
    s->rid = r->rid;
}

void repl_recv(struct repl *r, const struct repl_msg *m)
{
    if (r->failed)
        return;
    switch (m->type) {
    case REPL_M_APPEND:
        on_append(r, m);
        break;
    case REPL_M_ACK:
        on_ack(r, m);
        break;
    case REPL_M_REFUSE:
        if (m->epoch > r->epoch) {
            if (m->leader >= 0 && m->leader != r->cfg.self && m->epoch >= r->fence) {
                raise_fence(r, m->epoch);
                follow(r, m->epoch, m->leader);
            } else {
                deposed(r, m->epoch);
            }
        }
        break;
    case REPL_M_QUERY:
        on_query(r, m);
        break;
    case REPL_M_ANSWER:
        on_answer(r, m);
        break;
    case REPL_M_FENCE:
        on_fence(r, m);
        break;
    case REPL_M_META:
        adopt_meta(r, &m->meta);
        break;
    case REPL_M_SNAPSHOT:
        /* The driver copies the leader's applied state and reopens this replica on it. The leader repeats the notice
         * (resend_ticks) until the replica's ACKs show it no longer needs one: a driver ignores a repeat while a copy
         * is in progress, and a copy that failed is asked for again. */
        if (!r->leading && m->epoch == r->epoch && m->from == r->leader && r->fstate == F_FOLLOW) {
            if (m->written < r->verified) {
                /* The leader went back to a position we have (it lost an ACK): tell it where we are instead. */
                ack(r, 1);
                break;
            }
            struct repl_action *a = push(r, REPL_A_SNAPSHOT);
            if (a)
                a->pos = (uint64_t)m->from;
        }
        break;
    default:
        break;
    }
}

int repl_write(struct repl *r, uint32_t n, uint64_t *pos)
{
    if (r->failed)
        return REPL_E_FAILED;
    if (!r->leading)
        return REPL_E_NOTLEADER;
    if (popcount32(r->meta.isr) < r->cfg.min_isr && !MUT(31))
        return REPL_E_ISR;
    if (n == 0)
        n = 1;
    if (r->npend == r->cpend) {
        size_t c = r->cpend ? r->cpend * 2 : 64;
        struct pending *p = realloc(r->pend, c * sizeof *p);
        if (!p)
            return REPL_E_FAILED;
        r->pend = p;
        r->cpend = c;
    }
    struct repl_action *a = push(r, REPL_A_WRITE);
    if (!a)
        return REPL_E_FAILED;
    a->rec.pos = r->written + 1;
    a->rec.epoch = r->epoch;
    a->rec.n = n;
    a->src = REPL_SRC_CLIENT;
    *pos = a->rec.pos;
    r->pend[r->npend].first = a->rec.pos;
    r->pend[r->npend].last = a->rec.pos + n - 1;
    r->npend++;
    r->tail = r->written + 1;
    r->written += n;
    sync_maybe(r);
    replicate(r);
    return REPL_OK;
}

void repl_synced(struct repl *r, uint64_t end)
{
    if (r->failed)
        return;
    r->sync_busy = 0;
    /* A sync covers no more than was written when it was issued, and nothing a cut removed since (rewritten after
     * the cut, the positions are new records the sync never saw). */
    r->durable = min64(MUT(35) ? end : min64(end, r->sync_end), r->written);
    sync_maybe(r);
    if (r->leading) {
        advance(r);
    } else if (r->fstate == F_FOLLOW) {
        apply_follower(r);
        ack(r, 0);
    }
}

void repl_tick(struct repl *r)
{
    if (r->failed)
        return;
    r->now++;
    if (r->leading) {
        r->wend[r->now % r->nwend] = r->written;
        for (int i = 0; i < r->cfg.nnodes; i++) {
            if (i == r->cfg.self)
                continue;
            struct peer *p = &r->peer[i];
            if (!MUT(27) && p->next && p->next > p->acked + 1 && r->now - p->last_ack >= r->cfg.resend_ticks) {
                p->next = p->acked + 1; /* go back: something was lost */
                p->last_ack = r->now;
            }
            if (p->next && p->next <= r->written)
                send_append(r, i, 0);
            else if (r->now - p->last_send >= r->cfg.heartbeat_ticks)
                send_append(r, i, 1);
        }
        if (r->isr_busy && r->now - r->isr_sent >= r->cfg.resend_ticks) {
            r->isr_sent = r->now;
            struct repl_msg *m = send(r, REPL_M_ISR_REQ, REPL_META);
            if (m)
                m->meta = r->isr_req;
        }
        isr_check(r);
    } else if (r->fstate == F_TRUNC && r->now - r->last_query >= r->cfg.resend_ticks && !MUT(28)) {
        query(r);
    } else if (r->resigning && r->now - r->last_resign >= r->cfg.resend_ticks) {
        resign(r);
    }
    if (r->applied > r->floor_saved && r->now - r->floor_tick >= r->cfg.floor_ticks)
        save(r);
}

int repl_next_action(struct repl *r, struct repl_action *a)
{
    if (r->failed && !r->fail_told) {
        memset(a, 0, sizeof *a);
        a->type = REPL_A_FAIL;
        a->reason = r->fail_reason;
        r->fail_told = 1;
        r->nact = r->hact = 0;
        return 1;
    }
    if (r->hact == r->nact) {
        /* Every WRITE is carried out: the driver's index has the tail now (and may trim it once it is applied). */
        r->nact = r->hact = 0;
        r->tail = 0;
        return 0;
    }
    *a = r->act[r->hact++];
    return 1;
}

/* ------------------------------------------------------------------ open */

struct repl *repl_open(const struct repl_config *cfg, const struct repl_durable *d)
{
    if (cfg->nnodes < 1 || cfg->nnodes > REPL_MAX_NODES || cfg->self < 0 || cfg->self >= cfg->nnodes ||
        !cfg->rec_find || d->rid == 0 || cfg->max_batch == 0)
        return NULL;
    struct repl *r = calloc(1, sizeof *r);
    if (!r)
        return NULL;
    r->cfg = *cfg;
    r->nwend = cfg->isr_lag_ticks + 1;
    r->wend = calloc(r->nwend, sizeof *r->wend);
    if (!r->wend) {
        free(r);
        return NULL;
    }
    r->rid = d->rid;
    r->fence = d->fence;
    r->led = d->led;
    r->written = r->durable = d->log_end;
    r->leader = -1;
    for (uint32_t i = 0; i < d->nhist; i++) {
        if ((i && (d->hist[i].epoch <= d->hist[i - 1].epoch || d->hist[i].start <= d->hist[i - 1].start)) ||
            d->hist[i].start == 0 || d->hist[i].start > d->log_end) {
            fail(r, "the epoch history does not match the log");
            return r;
        }
        if (hist_push(r, d->hist[i].epoch, d->hist[i].start) < 0)
            return r;
    }
    if (d->floor > d->log_end) {
        /* The floor is saved only below the durable end and a cut never passes it: the log lost acknowledged
         * records. Damage is never silent. */
        fail(r, "the commit floor is above the durable log end");
        return r;
    }
    if (d->log_start > 1 && d->log_start - 1 > d->floor) {
        /* Only applied positions leave the log (into parts): a start above the floor lost records. */
        fail(r, "the log starts above the commit floor");
        return r;
    }
    r->sync_end = r->written;
    r->floor_saved = d->floor;
    apply_to(r, d->floor);
    r->commit = r->applied;
    return r;
}

void repl_free(struct repl *r)
{
    if (!r)
        return;
    free(r->hist);
    free(r->wend);
    free(r->pend);
    free(r->act);
    free(r);
}

int repl_is_leader(const struct repl *r) { return r->leading; }
uint64_t repl_epoch(const struct repl *r) { return r->epoch; }
uint64_t repl_written(const struct repl *r) { return r->written; }
uint64_t repl_durable_end(const struct repl *r) { return r->durable; }
uint64_t repl_applied(const struct repl *r) { return r->applied; }
uint64_t repl_commit(const struct repl *r) { return r->commit; }
int repl_failed(const struct repl *r) { return r->failed; }

/* ------------------------------------------------------------------ the metadata group's rules */

int repl_meta_isr_change(struct repl_vmeta *m, const struct repl_vmeta *req)
{
    if (req->epoch != m->epoch || req->isr_version != m->isr_version || req->leader != m->leader ||
        m->leader < 0 || !(req->isr & (1u << m->leader)))
        return REPL_E_STALE;
    for (int i = 0; i < REPL_MAX_NODES; i++) {
        uint32_t b = 1u << i;
        if (!(req->isr & b))
            m->rid[i] = 0;
        else if (!(m->isr & b))
            m->rid[i] = req->rid[i];
    }
    m->isr = req->isr;
    m->unanswered &= req->isr;
    m->isr_version++;
    return REPL_OK;
}

int repl_meta_promote(struct repl_vmeta *m, uint64_t basis_epoch, uint64_t basis_version, uint32_t fenced,
                      const struct repl_fence_reply *r, int n)
{
    if (m->epoch != basis_epoch || (m->isr_version != basis_version && !MUT(10)))
        return REPL_E_STALE;
    /* With the version unchanged, m->isr is the set the fences went to. */
    uint32_t from = MUT(10) ? fenced : m->isr & fenced;
    int best = -1;
    uint32_t isr = 0, answered = 0;
    for (int i = 0; i < n; i++) {
        int node = r[i].node;
        if (node < 0 || node >= REPL_MAX_NODES || (!(from & (1u << node)) && !MUT(25)) || (isr & (1u << node)))
            continue;
        answered |= 1u << node;
        if (r[i].rid != m->rid[node] && !MUT(11))
            continue;
        isr |= 1u << node;
        if (best < 0 || r[i].durable > r[best].durable || (r[i].durable == r[best].durable && node < r[best].node))
            best = i;
    }
    if (best < 0)
        return REPL_E_NONE;
    /* Members that did not answer stay in the ISR, the old leader too: they hold every committed position (the old
     * leader's durable end was in every commit point it made), and the new leader drops those that do not ACK soon
     * (`unanswered`). Keeping only the replies (§6.2) shrinks the ISR to one copy whenever replies are slow, and a
     * loss of that copy then stops the volume although others hold everything (tests/sim_repl.c found it: an earlier
     * seed 183 for slow replies; seed 76 for the old leader, paused through the failover while the promoted member
     * lost its disk). A reply with another rid is a new disk: out. */
    uint32_t keep = MUT(14) ? 0 : from & ~answered;
    if (MUT(34) && m->leader >= 0)
        keep &= ~(1u << m->leader);
    isr |= keep;
    m->unanswered = keep;
    for (int i = 0; i < REPL_MAX_NODES; i++)
        if (!(isr & (1u << i)))
            m->rid[i] = 0;
    m->epoch = basis_epoch + 1;
    m->leader = r[best].node;
    m->isr = isr;
    m->isr_version++;
    return REPL_OK;
}
