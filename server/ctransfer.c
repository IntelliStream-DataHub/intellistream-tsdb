/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Planned leader transfer, the Raft leader's side (R4 plan D14, D15; design-cluster.md 3.2 "Leader balance";
 * cluster_int.h). Runs on the cluster thread only, like the rest of cluster.c's state.
 *
 * `tsdb cluster volume transfer PEER V NODE` (ctr_admin): NODE must be an ISR member of V, UP and heard, and not its
 * leader; one transfer per volume, none while a promotion of V runs. The Raft leader sends TRANSFER{V, epoch, NODE,
 * arg = its Raft term} to the volume's leader (resent until READY), whose driver drains (rtransfer.c) and answers
 * READY{V, epoch, end, NODE, start = the term}. On READY the promotion starts as PLANNED with the target and READY's
 * end (promo_start_target): FENCE to every ISR member, every reply passes the candidate filter (ctr_reply_passes: the
 * drained old leader too, so nobody is left `unanswered`), and the grant names the target and min_end, which meta's
 * promote_rule turns into the target or nothing. The admin op is answered once the record names the target at a later
 * epoch, or with why not:
 *   - no READY within transfer_wait_ms + READY_SLACK_NS: CANCEL, "timed out";
 *   - no grant within PROMO_MOVE_NS of the promotion (cluster.c drops it, ctr_promo_dropped): CANCEL, and an old leader
 *     already fenced is deposed: the existing path reopens it as a replica and a failover follows ("fell back");
 *   - the record moved to another leader: "fell back to failover".
 * The state lives in memory only: a new Raft leader sends CANCEL{V, 0, arg = its term} to every volume leader in its
 * first duty rounds (until each is sent), and a volume leader ignores commands of an older term (rtransfer.c).
 *
 * Leader balance (D15, ctr_duty): leader_balance_s > 0, a volume led by another member than rep[0], rep[0] UP and in
 * the ISR without a break for leader_balance_s (from when this Raft leader saw it join, not from its last heartbeat),
 * no learner (a move), no promotion and no transfer of the volume: transferred back. At most one transfer per server
 * (as target or as old leader) and one start per second cluster-wide; after a failed transfer to a member none goes to
 * it for a backoff doubling from leader_balance_s up to BACKOFF_MAX_S. Never while the Raft leader is younger than its
 * lease (its view of who is UP is not yet its own).
 */
#include <inttypes.h>
#include <stdatomic.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cluster_int.h"

#define MS UINT64_C(1000000)
#define READY_SLACK_NS (1000 * MS)   /* past transfer_wait_ms: the drain's own timeout and its line come first */
#define RESEND_NS (200 * MS)         /* TRANSFER and CANCEL again while unanswered */
#define PROMO_HOLD_NS (2500 * MS)    /* a promotion that never started or never ended (PROMO_MOVE_NS + a round) */
#define BALANCE_GAP_NS (1000 * MS)   /* one balance start per second cluster-wide */
#define BACKOFF_MAX_S 600

enum { CT_ASKED = 1, CT_PROMOTING };

struct ctr {
    uint32_t vol, target, old;
    uint64_t epoch; /* the basis */
    uint64_t term;
    int state, balance, has_conn;
    uint64_t conn;
    uint32_t reqid;
    uint64_t t0, sent_ns, ready_ns, end;
};

static struct ctr g_tr[64];
static uint32_t g_ntr;
/* per volume: when rep[0] was seen to join the ISR (0: not in it) and the CANCEL still owed after an election */
struct cvs {
    uint32_t vol, node;
    uint64_t isr_since;
    int cancel_owed;
    const char *why; /* the balance's last reason not to move it back, said once */
};
static struct cvs *g_cv;
static uint32_t g_ncv, g_ccv;
/* per member: the balance backoff */
struct cback {
    uint32_t node, fails;
    uint64_t until;
};
static struct cback g_back[64];
static uint32_t g_nback;
static uint64_t g_term_led;     /* the Raft term whose CANCELs were queued */
static uint64_t g_last_balance; /* the last balance start */

/* STATUS (cluster_int.h ctr_counts) */
static atomic_ulong n_started, n_done, n_timedout, n_cancelled, n_fallback, n_balance;

enum { CTM_NONE, CTM_ANYCAND, CTM_FEWREPLIES, CTM_BALANCEFLAP, CTM_NOCANCEL };
/* TSDB_CLUSTER_MUTANT's names of this file (cluster.c's parser knows none of them) */
static int ctr_mutant(void)
{
    static int m = -1; /* the cluster thread's */
    if (m < 0) {
        const char *e = getenv("TSDB_CLUSTER_MUTANT");
        m = !e                                 ? CTM_NONE
            : !strcmp(e, "transferanycand")    ? CTM_ANYCAND
            : !strcmp(e, "transferfewreplies") ? CTM_FEWREPLIES
            : !strcmp(e, "balanceflap")        ? CTM_BALANCEFLAP
            : !strcmp(e, "nocancel")           ? CTM_NOCANCEL
                                               : CTM_NONE;
        if (m != CTM_NONE)
            cl_say("MUTANT %s (tests only)", e);
    }
    return m;
}

static uint64_t since(uint64_t now, uint64_t t) { return now > t ? now - t : 0; }

static struct ctr *tr_find(uint32_t vol)
{
    for (uint32_t i = 0; i < g_ntr; i++)
        if (g_tr[i].vol == vol)
            return &g_tr[i];
    return NULL;
}

static struct cvs *cv_get(uint32_t vol)
{
    for (uint32_t i = 0; i < g_ncv; i++)
        if (g_cv[i].vol == vol)
            return &g_cv[i];
    if (g_ncv == g_ccv) {
        g_ccv = g_ccv ? 2 * g_ccv : 16;
        g_cv = realloc(g_cv, g_ccv * sizeof *g_cv);
        if (!g_cv)
            abort();
    }
    struct cvs *c = &g_cv[g_ncv++];
    memset(c, 0, sizeof *c);
    c->vol = vol;
    return c;
}

static struct cback *back_get(uint32_t node)
{
    for (uint32_t i = 0; i < g_nback; i++)
        if (g_back[i].node == node)
            return &g_back[i];
    if (g_nback == sizeof g_back / sizeof g_back[0])
        return NULL;
    g_back[g_nback] = (struct cback){.node = node};
    return &g_back[g_nback++];
}

static int member_up(uint32_t node, uint64_t now)
{
    const struct meta_member *m = meta_member(cl_meta(), node);
    return m && m->state == META_UP && cl_heard_fresh(node, now);
}

static void tr_reply(struct ctr *t, int32_t rc, const char *fmt, ...) __attribute__((format(printf, 3, 4)));
static void tr_reply(struct ctr *t, int32_t rc, const char *fmt, ...)
{
    char b[256];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(b, sizeof b, fmt, ap);
    va_end(ap);
    if (n < 0)
        n = 0;
    if ((size_t)n >= sizeof b)
        n = (int)sizeof b - 1;
    cl_say("volume %" PRIu32 ": transfer to node %" PRIu32 ": %s", t->vol, t->target, b);
    if (t->has_conn)
        cl_admin_reply(t->conn, t->reqid, rc, b, (size_t)n);
    t->has_conn = 0;
}

static void send_cmd(uint8_t kind, uint32_t to, uint32_t vol, uint64_t epoch, uint32_t node)
{
    struct cluster_volcmd c = {.kind = kind, .volume = vol, .epoch = epoch, .node = node, .arg = cl_term()};
    (void)cl_volcmd_send(to, &c);
}

static void tr_failed(struct ctr *t, uint64_t now)
{
    if (!t->balance)
        return;
    struct cback *b = back_get(t->target);
    if (!b || ctr_mutant() == CTM_BALANCEFLAP)
        return;
    uint64_t s = cl_meta()->set.leader_balance_s;
    for (uint32_t i = 0; i < b->fails && s < BACKOFF_MAX_S; i++)
        s *= 2;
    if (s > BACKOFF_MAX_S)
        s = BACKOFF_MAX_S;
    b->fails++;
    b->until = now + s * 1000 * MS;
    cl_say("leader balance: no transfer to node %" PRIu32 " for %" PRIu64 " s (%" PRIu32 " failed)", t->target, s,
           b->fails);
}

static void tr_remove(struct ctr *t) { *t = g_tr[--g_ntr]; }

static const char *tr_check(const struct meta_volume *v, uint32_t node, uint64_t now)
{
    if (!v)
        return "no such volume";
    if (v->kind != META_VOL_REPLICATED || !v->nhist)
        return "not a replicated volume";
    if (!v->leader)
        return "the volume has no leader";
    if (node == v->leader)
        return "the node leads it already";
    if (!meta_volume_in_isr(v, node))
        return "the node is not in its ISR";
    if (!member_up(node, now))
        return "the node is not up";
    if (!member_up(v->leader, now))
        return "its leader is not up";
    if (tr_find(v->number))
        return "a transfer of it is under way";
    if (cl_promo_running(v->number))
        return "a promotion of it is under way";
    if (g_ntr == sizeof g_tr / sizeof g_tr[0])
        return "too many transfers under way";
    return NULL;
}

static struct ctr *tr_start(const struct meta_volume *v, uint32_t node, int balance, uint64_t now)
{
    struct ctr *t = &g_tr[g_ntr++];
    memset(t, 0, sizeof *t);
    t->vol = v->number;
    t->target = node;
    t->old = v->leader;
    t->epoch = v->epoch;
    t->term = cl_term();
    t->state = CT_ASKED;
    t->balance = balance;
    t->t0 = t->sent_ns = now;
    atomic_fetch_add_explicit(&n_started, 1, memory_order_relaxed);
    cl_say("volume %" PRIu32 ": %s transfer from node %" PRIu32 " to node %" PRIu32 " at epoch %" PRIu64,
           v->number, balance ? "leader balance:" : "", v->leader, node, v->epoch);
    send_cmd(CL_VOLCMD_TRANSFER, v->leader, v->number, v->epoch, node);
    return t;
}

void ctr_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    uint32_t vol, node;
    char b[160];
    if (cluster_volume_force_decode(body, n, &vol, &node) != 0) {
        int l = snprintf(b, sizeof b, "a transfer's body is 8 bytes (got %zu)", n);
        cl_admin_reply(conn, reqid, META_ERR_INVALID, b, (size_t)l);
        return;
    }
    uint64_t now = cl_now_ns();
    const struct meta_volume *v = meta_volume(cl_meta(), vol);
    const char *no = tr_check(v, node, now);
    if (no) {
        int l = snprintf(b, sizeof b, "volume %" PRIu32 " to node %" PRIu32 ": %s", vol, node, no);
        cl_admin_reply(conn, reqid, !v ? META_ERR_NOTFOUND : META_ERR_INVALID, b, (size_t)l);
        return;
    }
    struct ctr *t = tr_start(v, node, 0, now);
    t->has_conn = 1, t->conn = conn, t->reqid = reqid;
}

void ctr_ready(uint32_t from, const struct cluster_vmsg *m)
{
    struct ctr *t = tr_find(m->volume);
    const struct meta_volume *v = meta_volume(cl_meta(), m->volume);
    if (!t || !v || t->state != CT_ASKED || from != t->old || m->epoch != t->epoch || m->rid != t->target ||
        m->start != t->term || v->epoch != t->epoch || v->leader != t->old)
        return; /* a late or repeated READY, or one of another term */
    uint64_t now = cl_now_ns();
    if (cl_promo_running(v->number)) { /* a failover started meanwhile: it decides */
        tr_reply(t, META_ERR_STALE, "a promotion started before READY; fell back to failover");
        atomic_fetch_add_explicit(&n_fallback, 1, memory_order_relaxed);
        tr_failed(t, now);
        tr_remove(t);
        return;
    }
    t->state = CT_PROMOTING;
    t->ready_ns = now;
    t->end = m->durable;
    cl_say("volume %" PRIu32 ": READY from node %" PRIu32 " at %" PRIu64 " after %" PRIu64 " ms; promotes node %" PRIu32,
           v->number, from, m->durable, since(now, t->t0) / MS, t->target);
    if (ctr_mutant() == CTM_ANYCAND)
        promo_start_target(v, META_GR_PLANNED, 0, 0, now);
    else
        promo_start_target(v, META_GR_PLANNED, t->target, m->durable, now);
}

int ctr_reply_passes(uint32_t target, uint32_t node)
{
    return ctr_mutant() == CTM_FEWREPLIES ? node == target : 1;
}

void ctr_promo_dropped(uint32_t vol, uint64_t epoch, uint64_t now)
{
    struct ctr *t = tr_find(vol);
    if (!t || t->epoch != epoch)
        return;
    send_cmd(CL_VOLCMD_CANCEL, t->old, vol, epoch, t->target);
    atomic_fetch_add_explicit(&n_fallback, 1, memory_order_relaxed);
    tr_reply(t, META_ERR_STALE, "no grant within the promotion's time; cancelled (an old leader already fenced is "
                                "reopened as a replica and the volume fails over)");
    tr_failed(t, now);
    tr_remove(t);
}

/* the CANCELs a new Raft leader owes every volume leader (a transfer's state died with the last one) */
static void cancels(uint64_t now)
{
    const struct meta_state *sm = cl_meta();
    uint64_t term = cl_term();
    if (term != g_term_led) {
        g_term_led = term;
        for (uint32_t i = 0; i < sm->nvolumes; i++)
            if (sm->volumes[i].kind == META_VOL_REPLICATED && sm->volumes[i].leader)
                cv_get(sm->volumes[i].number)->cancel_owed = ctr_mutant() != CTM_NOCANCEL;
    }
    for (uint32_t i = 0; i < g_ncv; i++) {
        struct cvs *c = &g_cv[i];
        if (!c->cancel_owed)
            continue;
        const struct meta_volume *v = meta_volume(sm, c->vol);
        if (!v || !v->leader) {
            c->cancel_owed = 0;
            continue;
        }
        if (tr_find(c->vol)) { /* this leader's own transfer: its CANCEL would end it */
            c->cancel_owed = 0;
            continue;
        }
        struct cluster_volcmd cmd = {.kind = CL_VOLCMD_CANCEL, .volume = c->vol, .epoch = 0, .node = 0,
                                     .arg = term};
        if (cl_volcmd_send(v->leader, &cmd) == 0)
            c->cancel_owed = 0;
    }
    (void)now;
}

static void transfers(uint64_t now)
{
    const struct meta_state *sm = cl_meta();
    for (uint32_t i = 0; i < g_ntr;) {
        struct ctr *t = &g_tr[i];
        const struct meta_volume *v = meta_volume(sm, t->vol);
        if (!v) {
            tr_reply(t, META_ERR_NOTFOUND, "the volume is gone");
            tr_remove(t);
            continue;
        }
        if (v->epoch != t->epoch) {
            if (v->leader == t->target) {
                atomic_fetch_add_explicit(&n_done, 1, memory_order_relaxed);
                if (t->balance)
                    atomic_fetch_add_explicit(&n_balance, 1, memory_order_relaxed);
                struct cback *b = t->balance ? back_get(t->target) : NULL;
                if (b)
                    b->fails = 0;
                tr_reply(t, META_OK, "node %" PRIu32 " leads at epoch %" PRIu64 " (%" PRIu64 " ms, READY at %" PRIu64
                                     ")",
                         t->target, v->epoch, since(now, t->t0) / MS, t->end);
            } else {
                atomic_fetch_add_explicit(&n_fallback, 1, memory_order_relaxed);
                tr_reply(t, META_ERR_STALE, "fell back to failover: node %" PRIu32 " leads at epoch %" PRIu64,
                         v->leader, v->epoch);
                tr_failed(t, now);
            }
            tr_remove(t);
            continue;
        }
        if (t->state == CT_ASKED) {
            uint64_t wait = (uint64_t)sm->set.transfer_wait_ms * MS + READY_SLACK_NS;
            if (since(now, t->t0) >= wait) {
                send_cmd(CL_VOLCMD_CANCEL, t->old, t->vol, t->epoch, t->target);
                atomic_fetch_add_explicit(&n_timedout, 1, memory_order_relaxed);
                tr_reply(t, META_ERR_STALE, "timed out: no READY within %" PRIu64 " ms; cancelled", wait / MS);
                tr_failed(t, now);
                tr_remove(t);
                continue;
            }
            if (since(now, t->sent_ns) >= RESEND_NS) {
                t->sent_ns = now;
                send_cmd(CL_VOLCMD_TRANSFER, t->old, t->vol, t->epoch, t->target);
            }
        } else if (!cl_promo_running(t->vol) && since(now, t->ready_ns) >= PROMO_HOLD_NS) {
            /* the promotion never started (the epoch limit) or ended without moving the record */
            send_cmd(CL_VOLCMD_CANCEL, t->old, t->vol, t->epoch, t->target);
            atomic_fetch_add_explicit(&n_cancelled, 1, memory_order_relaxed);
            tr_reply(t, META_ERR_STALE, "the promotion ended without a new leader; cancelled");
            tr_failed(t, now);
            tr_remove(t);
            continue;
        }
        i++;
    }
}

static int busy_server(uint32_t node)
{
    for (uint32_t i = 0; i < g_ntr; i++)
        if (g_tr[i].target == node || g_tr[i].old == node)
            return 1;
    return 0;
}

static void balance(uint64_t now)
{
    const struct meta_state *sm = cl_meta();
    uint32_t hold_s = sm->set.leader_balance_s;
    int flap = ctr_mutant() == CTM_BALANCEFLAP;
    for (uint32_t i = 0; i < sm->nvolumes; i++) { /* the hold: from rep[0]'s join, as this Raft leader saw it */
        const struct meta_volume *v = &sm->volumes[i];
        if (v->kind != META_VOL_REPLICATED || !v->nrep)
            continue;
        struct cvs *c = cv_get(v->number);
        uint32_t pref = v->rep[0].node;
        if (c->node != pref || !meta_volume_in_isr(v, pref))
            c->isr_since = 0;
        c->node = pref;
        if (meta_volume_in_isr(v, pref) && !c->isr_since) {
            c->isr_since = now;
            if (hold_s && v->leader && v->leader != pref)
                cl_say("leader balance: volume %" PRIu32 ": node %" PRIu32 " in the ISR (version %" PRIu64
                       "), led by node %" PRIu32 ": moves back after %" PRIu32 " s", v->number, pref,
                       (uint64_t)v->isr_version, v->leader, hold_s);
        }
    }
    if (!hold_s || cl_leader_age_ns(now) < (uint64_t)cl_lease_ms() * MS ||
        (g_last_balance && since(now, g_last_balance) < BALANCE_GAP_NS))
        return;
    for (uint32_t i = 0; i < sm->nvolumes; i++) {
        const struct meta_volume *v = &sm->volumes[i];
        if (v->kind != META_VOL_REPLICATED || !v->nrep || !v->nhist || !v->leader)
            continue;
        uint32_t pref = v->rep[0].node;
        if (v->leader == pref || v->rep[0].replaces)
            continue;
        int learner = 0;
        for (uint32_t j = 0; j < v->nrep; j++)
            learner |= v->rep[j].learner || v->rep[j].replaces;
        struct cvs *c = cv_get(v->number);
        if (learner || (!flap && (!c->isr_since || since(now, c->isr_since) < (uint64_t)hold_s * 1000 * MS)))
            continue;
        const struct cback *b = NULL;
        for (uint32_t j = 0; j < g_nback; j++)
            if (g_back[j].node == pref)
                b = &g_back[j];
        if (b && now < b->until)
            continue;
        const char *no = busy_server(pref) || busy_server(v->leader) ? "a transfer of its server is under way"
                                                                       : tr_check(v, pref, now);
        if (no) {
            if (no != c->why)
                cl_say("leader balance: volume %" PRIu32 " stays on node %" PRIu32 " for now: %s", v->number,
                       v->leader, no);
            c->why = no;
            continue;
        }
        c->why = NULL;
        tr_start(v, pref, 1, now);
        g_last_balance = now;
        return;
    }
}

void ctr_duty(uint64_t now)
{
    if (!cl_leading()) {
        for (uint32_t i = 0; i < g_ntr; i++)
            tr_reply(&g_tr[i], CLUSTER_ERR_NOTLEADER, "this server no longer leads the group; the next leader "
                                                      "cancels the transfer");
        g_ntr = 0;
        g_term_led = 0;
        return;
    }
    cancels(now);
    transfers(now);
    balance(now);
}

void ctr_counts(struct ctr_counts *c)
{
    c->started = atomic_load_explicit(&n_started, memory_order_relaxed);
    c->done = atomic_load_explicit(&n_done, memory_order_relaxed);
    c->timedout = atomic_load_explicit(&n_timedout, memory_order_relaxed);
    c->cancelled = atomic_load_explicit(&n_cancelled, memory_order_relaxed);
    c->fallback = atomic_load_explicit(&n_fallback, memory_order_relaxed);
    c->balance = atomic_load_explicit(&n_balance, memory_order_relaxed);
}
