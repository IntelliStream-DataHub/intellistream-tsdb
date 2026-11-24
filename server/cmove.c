/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Replica moves, the Raft leader's side (R4 plan D10, D11; design-cluster.md 3.2; cluster_int.h). Runs on the cluster
 * thread only, like the rest of cluster.c's state.
 *
 * `tsdb cluster volume move PEER V --log --from A --to B` (cmove_admin): A a member of V, B UP, heard, allowed volumes
 * and not a member, no move of V running. It proposes REPLICA_ADD(V, epoch, B, a fresh rid, replaces = A) and answers
 * at once: the move runs in the background. The record is the move's state: B's `replaces` says which member it moves
 * in for, so a new Raft leader resumes every move it finds there (cmove_duty), and the REMOVE of A that ends it puts B
 * into A's slot of rep[] (meta.c), so moving rep[0] keeps the preferred leader's slot and no balance transfer follows.
 *
 * The steps (cmove_duty, every round, each retried every second while its condition holds; a step whose basis moved,
 * a new epoch or ISR, is recomputed from the record at the next round):
 *   1. catch-up: B fills as a learner (the driver's, a snapshot when the leader's early segments are gone) until the
 *      volume leader asks for it in the ISR. Meanwhile the duty notes the ISR's size (`min_isr`): the size before B
 *      joined. A Raft leader that resumes a move after B joined never saw it and takes the replication factor (the
 *      members but B).
 *   2. transfer: when A leads the volume, a planned transfer first (D15; ctransfer.c) to rep[0] when that is not A,
 *      else to B: B takes rep[0]'s slot at the end, so no balance transfer follows.
 *   3. shrink: VOLCMD DROP_MEMBER{V, epoch, A, arg = term << 8 | min_isr} to the volume leader, whose driver calls
 *      repl_leave (rset.c; D11): the core asks for the ISR without A through its own ISR requests (only the leader
 *      changes the ISR, C11), and only while that keeps min_isr members, and never asks for A again. So the move never
 *      lowers the number of in-sync copies below what the volume had before B joined.
 *   4. remove: once A is out of the ISR, REPLICA_REMOVE(V, epoch, A); A's server removes its directory (r4-setchg).
 * `--cancel` before A left the ISR runs the same steps with B in A's place (B shrunk out first when it joined, then
 * REPLICA_REMOVE of B) and sends DROP_MEMBER with node 0 (repl_leave(-1)), so A is not left `leaving`. A cancel lives
 * in this Raft leader's memory: a new one resumes the move forwards (the operator cancels again).
 *
 * Crash points (crashpt.h): move-added when this Raft leader first sees the move's REPLICA_ADD applied, move-shrunk
 * when it first sees A out of the ISR, before its REMOVE is proposed.
 *
 * Mutants (TSDB_CLUSTER_MUTANT, test_cluster move*): moveshrinkfirst (DROP_MEMBER before B joined, with one fewer
 * in-sync copy allowed for the slot B is to fill: the core's own min_isr check would otherwise hold it back), movenoreplaces
 * (only moves this Raft leader started are driven: the record's `replaces` is forgotten after an election),
 * moveremovebeforeshrink (REMOVE of A while it is in the ISR: meta refuses it), moveleaderfirst (REMOVE of the
 * leader's replica without a transfer: refused by meta), movenominisr (DROP_MEMBER with min_isr 0).
 *
 * TSDB_CMOVE_TEST=hold_shrink=PATH holds the shrink step while PATH exists (test_cluster movelag).
 *
 * The test hook TSDB_CLUSTER_TEST vol_trace=1 (cmove_trace) appends a line to <state-dir>/voltrace for every applied
 * entry that changed a replicated volume's set, epoch, leader or ISR: the test follows every committed record.
 */
#include <inttypes.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../client/tsdb_crypto.h"
#include "cluster_int.h"
#include "crashpt.h"

#define MS UINT64_C(1000000)
#define STEP_NS (1000 * MS) /* each step's command or proposal again while its condition holds */

enum { MM_NONE = 0, MM_SHRINKFIRST, MM_NOREPLACES, MM_REMOVEBEFORESHRINK, MM_LEADERFIRST, MM_NOMINISR };
static int mv_mutant(void)
{
    static int m = -1; /* the cluster thread's */
    if (m < 0) {
        const char *e = getenv("TSDB_CLUSTER_MUTANT");
        m = !e                                     ? MM_NONE
            : !strcmp(e, "moveshrinkfirst")        ? MM_SHRINKFIRST
            : !strcmp(e, "movenoreplaces")         ? MM_NOREPLACES
            : !strcmp(e, "moveremovebeforeshrink") ? MM_REMOVEBEFORESHRINK
            : !strcmp(e, "moveleaderfirst")        ? MM_LEADERFIRST
            : !strcmp(e, "movenominisr")           ? MM_NOMINISR
                                                   : MM_NONE;
        if (m != MM_NONE)
            cl_say("MUTANT %s (tests only)", e);
    }
    return m;
}

enum { ST_CATCHUP = 0, ST_TRANSFER, ST_SHRINK, ST_REMOVE };
static const char *const step_name[] = {"catch-up", "transfer", "shrink", "remove"};

struct mv {
    uint32_t vol, a, b; /* moving from a to b */
    uint64_t rid;       /* b's instance */
    uint32_t min_isr;   /* the ISR's size before b joined (have_min: known) */
    int have_min, joined, mine, cancel, said_added, said_shrunk, step;
    const char *why;    /* the transfer's last reason not to start, said once */
    uint64_t t0, sent_ns;
};
static struct mv g_mv[64];
static uint32_t g_nmv;
static uint64_t g_term; /* the Raft term g_mv belongs to */
static atomic_ulong n_started, n_done, n_cancelled;

/* TSDB_CMOVE_TEST=hold_shrink=PATH (test_cluster movelag): the shrink step waits while PATH exists, so a test can make
 * another member lag after the learner joined and before the move asks for the ISR without the old member */
static int shrink_held(void)
{
    static const char *path; /* the cluster thread's */
    static int read;
    if (!read) {
        read = 1;
        const char *e = getenv("TSDB_CMOVE_TEST");
        if (e && !strncmp(e, "hold_shrink=", 12) && e[12])
            path = e + 12;
    }
    return path && access(path, F_OK) == 0;
}

static uint64_t since(uint64_t now, uint64_t t) { return now > t ? now - t : 0; }

static struct mv *mv_find(uint32_t vol)
{
    for (uint32_t i = 0; i < g_nmv; i++)
        if (g_mv[i].vol == vol)
            return &g_mv[i];
    return NULL;
}

static struct mv *mv_new(uint32_t vol, uint32_t a, uint32_t b, uint64_t rid, uint64_t now)
{
    if (g_nmv == sizeof g_mv / sizeof g_mv[0])
        return NULL;
    struct mv *m = &g_mv[g_nmv++];
    memset(m, 0, sizeof *m);
    m->vol = vol, m->a = a, m->b = b, m->rid = rid, m->t0 = now;
    return m;
}

/* the move recorded in v: the member that moves in (its `replaces` names the other), or NULL */
static const struct meta_replica *moving_in(const struct meta_volume *v)
{
    for (uint32_t i = 0; i < v->nrep; i++)
        if (v->rep[i].replaces)
            return &v->rep[i];
    return NULL;
}

static int member_up(uint32_t node, uint64_t now)
{
    const struct meta_member *m = meta_member(cl_meta(), node);
    return m && m->state == META_UP && cl_heard_fresh(node, now);
}

static uint64_t fresh_rid(const struct meta_volume *v)
{
    for (;;) {
        uint64_t rid = 0;
        tsdb_random(&rid, sizeof rid);
        int dup = !rid;
        for (uint32_t j = 0; j < v->nrep; j++)
            dup |= v->rep[j].rid == rid;
        if (!dup)
            return rid;
    }
}

static void drop_member(const struct meta_volume *v, uint32_t node, uint32_t min_isr)
{
    struct cluster_volcmd c = {.kind = CL_VOLCMD_DROP_MEMBER, .volume = v->number, .epoch = v->epoch, .node = node,
                               .arg = cl_term() << 8 | (min_isr & 0xff)};
    (void)cl_volcmd_send(v->leader, &c);
}

static void remove_member(const struct meta_volume *v, uint32_t node)
{
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_replica_remove(&e, v->number, v->epoch, node);
    (void)cl_propose(&e); /* frees e; refused or lost: the next second proposes again while the condition holds */
}

/* the transfer away from `out`: rep[0] when that is neither side of the move and is up and in the ISR, else the other
 * side of the move (b for a move, a for a cancel), which takes or keeps rep[0]'s slot */
static uint32_t transfer_target(const struct meta_volume *v, const struct mv *m, uint32_t out, uint64_t now)
{
    uint32_t pref = v->rep[0].node, other = out == m->a ? m->b : m->a;
    if (pref != out && pref != other && meta_volume_in_isr(v, pref) && member_up(pref, now))
        return pref;
    return other;
}

static int due(const struct mv *m, uint64_t now) { return !m->sent_ns || since(now, m->sent_ns) >= STEP_NS; }

static void step(const struct meta_volume *v, struct mv *m, uint64_t now)
{
    int mut = mv_mutant();
    if (!m->said_added) {
        m->said_added = 1;
        crash_point(CP_MOVE_ADDED);
        cl_say("volume %" PRIu32 ": move from node %" PRIu32 " to node %" PRIu32 " (replica %016" PRIx64
               ") driven by this Raft leader%s", v->number, m->a, m->b, m->rid, m->mine ? "" : " (resumed)");
    }
    int b_in = meta_volume_in_isr(v, m->b);
    if (!m->cancel && !b_in && !m->joined && mut != MM_SHRINKFIRST) {
        m->min_isr = v->nisr; /* the last size seen before b joined */
        m->have_min = 1;
        m->step = ST_CATCHUP;
        return;
    }
    if (!m->have_min) /* resumed after the join: the replication factor (every member but b) */
        m->min_isr = v->nrep - 1, m->have_min = 1;
    if (b_in && !m->joined) {
        m->joined = 1;
        cl_say("volume %" PRIu32 ": node %" PRIu32 " joined the ISR (version %" PRIu64 "); the ISR keeps at least %"
               PRIu32 " members without node %" PRIu32, v->number, m->b, v->isr_version, m->min_isr, m->a);
    }
    uint32_t out = m->cancel ? m->b : m->a;
    if (m->cancel && !b_in) {
        m->step = ST_REMOVE;
        if (due(m, now)) {
            m->sent_ns = now;
            if (v->leader)
                drop_member(v, 0, 0); /* repl_leave(-1): nobody is leaving */
            remove_member(v, m->b);
        }
        return;
    }
    if (meta_volume_in_isr(v, out)) {
        if (v->leader == out && mut != MM_LEADERFIRST) {
            m->step = ST_TRANSFER;
            if (cl_promo_running(v->number) || ctr_running(v->number) || !due(m, now))
                return;
            m->sent_ns = now;
            uint32_t to = transfer_target(v, m, out, now);
            const char *no = ctr_move_start(v->number, to, now);
            if (no && no != m->why)
                cl_say("volume %" PRIu32 ": the move's transfer from node %" PRIu32 " to node %" PRIu32
                       " waits: %s", v->number, out, to, no);
            m->why = no;
            return;
        }
        m->step = ST_SHRINK;
        if (!due(m, now) || shrink_held())
            return;
        m->sent_ns = now;
        if (mut == MM_REMOVEBEFORESHRINK || (mut == MM_LEADERFIRST && v->leader == out))
            remove_member(v, out);
        else if (v->leader)
            drop_member(v, out, mut == MM_NOMINISR                   ? 0
                               : mut == MM_SHRINKFIRST && m->min_isr ? m->min_isr - 1 /* "b takes a's slot" */
                                                                     : m->min_isr);
        return;
    }
    if (!m->said_shrunk) {
        m->said_shrunk = 1;
        if (!m->cancel)
            crash_point(CP_MOVE_SHRUNK);
        cl_say("volume %" PRIu32 ": node %" PRIu32 " left the ISR (version %" PRIu64 ", %" PRIu32 " members): its "
               "REPLICA_REMOVE follows", v->number, out, v->isr_version, v->nisr);
        m->sent_ns = 0;
    }
    m->step = ST_REMOVE;
    if (!due(m, now))
        return;
    m->sent_ns = now;
    remove_member(v, out);
}

void cmove_duty(uint64_t now)
{
    if (!cl_leading() || cl_term() != g_term) {
        g_nmv = 0; /* deposed, or a new term: the record says what is running */
        g_term = cl_leading() ? cl_term() : 0;
        if (!g_term)
            return;
    }
    const struct meta_state *sm = cl_meta();
    for (uint32_t i = 0; i < g_nmv;) { /* moves that ended (the record has no `replaces` now, or another one) */
        const struct meta_volume *v = meta_volume(sm, g_mv[i].vol);
        const struct meta_replica *in = v ? moving_in(v) : NULL;
        if (in && in->node == g_mv[i].b && in->replaces == g_mv[i].a && in->rid == g_mv[i].rid) {
            i++;
            continue;
        }
        struct mv *m = &g_mv[i];
        int a_member = v && meta_replica(v, m->a), b_member = v && meta_replica(v, m->b);
        const char *how = !v ? "ended: the volume is gone" : a_member && !b_member ? "cancelled: the old set is back"
                          : !a_member && b_member ? "done" : "ended";
        atomic_fetch_add_explicit(!a_member && b_member ? &n_done : &n_cancelled, 1, memory_order_relaxed);
        cl_say("volume %" PRIu32 ": move from node %" PRIu32 " to node %" PRIu32 " %s after %" PRIu64 " ms", m->vol,
               m->a, m->b, how, since(now, m->t0) / MS);
        *m = g_mv[--g_nmv];
    }
    for (uint32_t i = 0; i < sm->nvolumes; i++) {
        const struct meta_volume *v = &sm->volumes[i];
        if (v->kind != META_VOL_REPLICATED || !v->nhist)
            continue;
        const struct meta_replica *in = moving_in(v);
        if (!in)
            continue;
        struct mv *m = mv_find(v->number);
        if (!m) {
            if (mv_mutant() == MM_NOREPLACES)
                continue; /* planted: a move this Raft leader did not start is forgotten */
            if (!(m = mv_new(v->number, in->replaces, in->node, in->rid, now)))
                continue;
        }
        step(v, m, now);
    }
}

static void reply(uint64_t conn, uint32_t reqid, int32_t rc, const char *fmt, ...)
    __attribute__((format(printf, 4, 5)));
static void reply(uint64_t conn, uint32_t reqid, int32_t rc, const char *fmt, ...)
{
    char b[320];
    va_list ap;
    va_start(ap, fmt);
    int l = vsnprintf(b, sizeof b, fmt, ap);
    va_end(ap);
    cl_admin_reply(conn, reqid, rc, b, l < 0 ? 0 : (size_t)l < sizeof b ? (size_t)l : sizeof b - 1);
}

void cmove_admin(uint64_t conn, uint32_t reqid, const uint8_t *body, size_t n)
{
    uint32_t vol, from, to, flags;
    if (cluster_volume_move_decode(body, n, &vol, &from, &to, &flags) != 0) {
        reply(conn, reqid, META_ERR_INVALID, "a move's body is 16 bytes (got %zu)", n);
        return;
    }
    uint64_t now = cl_now_ns();
    int cancel = (flags & CLUSTER_MOVE_CANCEL) != 0;
    char what[112];
    snprintf(what, sizeof what, "%smove of volume %" PRIu32 " from node %" PRIu32 " to node %" PRIu32,
             cancel ? "cancel of the " : "", vol, from, to);
    const struct meta_volume *v = meta_volume(cl_meta(), vol);
    if (!v) {
        reply(conn, reqid, META_ERR_NOTFOUND, "%s: no such volume", what);
        return;
    }
    const char *no = !(flags & CLUSTER_MOVE_LOG)                  ? "--log is required (positions come with R8)"
                     : v->kind != META_VOL_REPLICATED || !v->nhist ? "not a replicated volume"
                     : from == to                                  ? "the same node twice"
                                                                   : NULL;
    const struct meta_replica *in = moving_in(v);
    if (!no && cancel) {
        if (!in || in->node != to || in->replaces != from)
            no = "no such move is running";
        else if (!meta_volume_in_isr(v, from) && meta_volume_in_isr(v, to))
            no = "too late: the old member has left the ISR (the move finishes)";
    } else if (!no) {
        const struct meta_member *mb = meta_member(cl_meta(), to);
        no = in                                         ? "a move of the volume is running"
             : !meta_replica(v, from)                   ? "the old node is not a member of the volume"
             : meta_replica(v, to)                      ? "the new node is a member already"
             : !mb || !(mb->flags & META_MF_VOLUMES_OK) ? "the new node may not hold volumes"
             : !member_up(to, now)                      ? "the new node is not up"
             : v->nrep >= META_REPLICAS_MAX             ? "the volume has as many replicas as it may"
             : g_nmv == sizeof g_mv / sizeof g_mv[0]    ? "too many moves under way"
                                                        : NULL;
    }
    if (no) {
        reply(conn, reqid, META_ERR_INVALID, "%s: %s", what, no);
        return;
    }
    if (cancel) {
        struct mv *m = mv_find(vol);
        if (!m && !(m = mv_new(vol, from, to, in->rid, now))) {
            reply(conn, reqid, CLUSTER_ERR_RETRY, "%s: too many moves under way", what);
            return;
        }
        m->cancel = 1, m->sent_ns = 0, m->said_shrunk = 0;
        cl_say("volume %" PRIu32 ": %s by the operator (step %s)", vol, what, step_name[m->step]);
        reply(conn, reqid, META_OK, "%s: cancelling (node %" PRIu32 " leaves, node %" PRIu32 " stays)\n", what, to,
              from);
        return;
    }
    uint64_t rid = fresh_rid(v);
    struct meta_entry e;
    meta_entry_init(&e);
    meta_op_replica_add(&e, vol, v->epoch, to, rid, from);
    if (cl_propose(&e) != 0) { /* frees e */
        reply(conn, reqid, CLUSTER_ERR_RETRY, "%s: not proposed (try again)", what);
        return;
    }
    struct mv *m = mv_find(vol);
    if (m) /* an entry of a move whose end this leader has not seen yet */
        *m = g_mv[--g_nmv];
    if ((m = mv_new(vol, from, to, rid, now))) {
        m->mine = 1;
        m->min_isr = v->nisr, m->have_min = 1;
    }
    atomic_fetch_add_explicit(&n_started, 1, memory_order_relaxed);
    cl_say("volume %" PRIu32 ": %s proposed (replica %016" PRIx64 ", ISR of %" PRIu32 ")", vol, what, rid, v->nisr);
    reply(conn, reqid, META_OK,
          "%s: proposed (replica %016" PRIx64 "); it runs in the background: learner, join, shrink, remove\n", what,
          rid);
}

size_t cmove_status(char *out, size_t n, uint64_t now)
{
    size_t o = 0;
    if (n)
        out[0] = 0;
    for (uint32_t i = 0; i < g_nmv && o + 1 < n; i++) {
        const struct mv *m = &g_mv[i];
        int l = snprintf(out + o, n - o, "move volume %" PRIu32 " node %" PRIu32 " -> node %" PRIu32 "%s: %s, %" PRIu64
                         " s\n", m->vol, m->a, m->b, m->cancel ? " (cancelling)" : "", step_name[m->step],
                         since(now, m->t0) / (1000 * MS));
        if (l < 0)
            break;
        o += (size_t)l < n - o ? (size_t)l : n - o - 1;
    }
    return o;
}

void cmove_counts(unsigned long *started, unsigned long *done, unsigned long *cancelled)
{
    *started = atomic_load_explicit(&n_started, memory_order_relaxed);
    *done = atomic_load_explicit(&n_done, memory_order_relaxed);
    *cancelled = atomic_load_explicit(&n_cancelled, memory_order_relaxed);
}

/* TSDB_CLUSTER_TEST vol_trace=1: a line per applied entry that changed a replicated volume:
 * "<index> v<N> set <sv> epoch <e> leader <l> isrv <iv> isr <a,b,..> rep <node[:L][<r]>,..>" (L: a learner, <r: it
 * moves in for r) */
void cmove_trace(const struct meta_state *sm, uint64_t index, const char *path)
{
    static struct {
        uint32_t vol, leader, nisr;
        uint64_t sv, epoch, iv;
    } last[256];
    static uint32_t nlast;
    for (uint32_t i = 0; i < sm->nvolumes; i++) {
        const struct meta_volume *v = &sm->volumes[i];
        if (v->kind != META_VOL_REPLICATED)
            continue;
        uint32_t k = 0;
        while (k < nlast && last[k].vol != v->number)
            k++;
        if (k == nlast) {
            if (nlast == sizeof last / sizeof last[0])
                continue;
            memset(&last[nlast], 0, sizeof last[0]);
            last[nlast++].vol = v->number;
        }
        if (last[k].sv == v->set_version && last[k].epoch == v->epoch && last[k].iv == v->isr_version &&
            last[k].leader == v->leader && last[k].nisr == v->nisr)
            continue;
        last[k].sv = v->set_version, last[k].epoch = v->epoch, last[k].iv = v->isr_version;
        last[k].leader = v->leader, last[k].nisr = v->nisr;
        char line[1024];
        size_t o = (size_t)snprintf(line, sizeof line, "%" PRIu64 " v%" PRIu32 " set %" PRIu64 " epoch %" PRIu64
                                    " leader %" PRIu32 " isrv %" PRIu64 " isr ", index, v->number, v->set_version,
                                    v->epoch, v->leader, v->isr_version);
        for (uint32_t j = 0; j < v->nisr && o < sizeof line - 64; j++)
            o += (size_t)snprintf(line + o, sizeof line - o, "%s%" PRIu32, j ? "," : "", v->isr[j]);
        o += (size_t)snprintf(line + o, sizeof line - o, " rep ");
        for (uint32_t j = 0; j < v->nrep && o < sizeof line - 64; j++) {
            o += (size_t)snprintf(line + o, sizeof line - o, "%s%" PRIu32 "%s", j ? "," : "", v->rep[j].node,
                                  v->rep[j].learner ? ":L" : "");
            if (v->rep[j].replaces)
                o += (size_t)snprintf(line + o, sizeof line - o, "<%" PRIu32, v->rep[j].replaces);
        }
        FILE *f = fopen(path, "a");
        if (f) {
            fprintf(f, "%s\n", line);
            fclose(f);
        }
    }
}
