/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The verbs transport (server/plrdma.[ch]) under peerlink, over the fake fabric of tests/plr_fake.h (docs/design-rdma.md
 * §8): two pools (two tsdbd) on one fabric, a connection each, numbers exchanged as rd-upgrade will exchange them
 * (encoded and decoded), the link threads of two peerlinks on the transports with a tests/pl_fake.h pair as each
 * connection's TCP anchor. Every case ends with the pools whole (every slot back, none lost), no queue pair left and no
 * violation of the fake (a buffer posted twice, a slot taken back while the NIC holds it, a queue pair destroyed
 * undrained, numbers that are not the peer's, a SEND without a posted receive).
 *
 * Two hosts on real verbs (--verbs-listen, --verbs-dial, --tcp): below, at "two hosts".
 *
 * Cases: basic (frames both ways, many slots each, small credit windows, so both sides run out of credits with frames
 * queued), pause (a paused link that only PINGs for 10 x dead_ms is not counted dead), storm (an idle link sends no
 * stream of CREDITs), written (a frame posted and then flushed by a failed queue pair is WRITTEN), qperr (a send that
 * fails mid-stream ends the connection on both sides; the next connection carries on), pool (memlock, a failed
 * registration, a pool with room for two connections, the bounds of the slots), ep (the endpoint's encoding), asym
 * (two pools with other slot sizes, data and control slots: the peer's numbers rule what is sent), anchor (a byte on
 * the anchor ends the connection on both sides), wakeup (with PINGs far apart, a completion that lands between a poll
 * of the CQ and the arm must not wait for the next timer), errors (a device, a queue pair and its connect that fail;
 * a drain that never finishes keeps its slots out of the pool). The rigs' fabric flushes a failed queue pair
 * asynchronously, one work request per queue and tick, as an HCA does.
 *
 * --mutant NAME plants a bug of plrdma.h's list; each must fail this test.
 */
#include <inttypes.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

#include "compat.h"
#include "peerlink.h"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function" /* test_peerlink uses the rest of the fake */
#include "pl_fake.h"
#pragma GCC diagnostic pop
#include "plr_fake.h"
#include "plrdma.h"

static int failures;
#define CHECK(cond)                                                                                                    \
    do {                                                                                                               \
        if (!(cond)) {                                                                                                 \
            fprintf(stderr, "%s:%d: FAILED: %s\n", __FILE__, __LINE__, #cond);                                         \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

static int quick;

static uint64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

#define WAIT_FOR(cond, ms)                                                                                             \
    ({                                                                                                                 \
        uint64_t t_end_ = now_ms() + (ms);                                                                             \
        int ok_ = 0;                                                                                                   \
        while (!(ok_ = !!(cond)) && now_ms() < t_end_)                                                                 \
            usleep(2000);                                                                                              \
        ok_;                                                                                                           \
    })

static const uint8_t SECRET[32] = "plrdma test secret 0123456789ab";

struct node {
    uint32_t id;
    struct peerlink *pl;
    _Atomic uint64_t in, next, bad;
    _Atomic int ups, downs, written, unwritten;
    _Atomic uint64_t sent_token;
    /* two hosts (--verbs-listen, --verbs-dial): the bench's phase as its frames say it */
    _Atomic int bench, ended;
    _Atomic uint64_t bn, bsize, bdone, replied;
    uint64_t b_t0, b_t1, b_cpu0, b_cpu1; /* the receiver's: written by the link thread before bdone moves */
    uint64_t r_ns, r_cpu;                /* the sender's copy of the receiver's, before replied moves */
};

static void fill(uint8_t *b, size_t len, uint64_t i)
{
    uint64_t x = i * 0x9e3779b97f4a7c15ull + len;
    memcpy(b, &i, 8);
    for (size_t k = 8; k < len; k++) {
        x = x * 6364136223846793005ull + 1442695040888963407ull;
        b[k] = (uint8_t)(x >> 56);
    }
}

static void hosts_frame(struct node *nd, uint8_t type, const uint8_t *p, size_t n);

static void on_frame(void *arg, struct plink *l, uint8_t type, const uint8_t *p, size_t n)
{
    struct node *nd = arg;
    (void)l;
    if (type >= 2 && type <= 4) {
        hosts_frame(nd, type, p, n);
        return;
    }
    if (type == 1 && atomic_load(&nd->bench)) {
        hosts_frame(nd, type, p, n);
        return;
    }
    if (type != 1 || n < 8) {
        atomic_fetch_add(&nd->bad, 1);
        return;
    }
    uint64_t i;
    memcpy(&i, p, 8);
    uint8_t *want = malloc(n);
    fill(want, n, i);
    if (i != atomic_load(&nd->next) || memcmp(want, p, n) != 0)
        atomic_fetch_add(&nd->bad, 1);
    free(want);
    atomic_store(&nd->next, i + 1);
    atomic_fetch_add(&nd->in, 1);
}

static void on_link(void *arg, struct plink *l, int up)
{
    struct node *nd = arg;
    (void)l;
    atomic_fetch_add(up ? &nd->ups : &nd->downs, 1);
}

static void on_sent(void *arg, struct plink *l, uint64_t token, uint64_t gen, int written)
{
    struct node *nd = arg;
    (void)l;
    (void)gen;
    atomic_fetch_add(written ? &nd->written : &nd->unwritten, 1);
    atomic_store(&nd->sent_token, token * 2 + (uint64_t)!!written);
}

static struct node *node_new(uint32_t id, uint32_t ping_ms, uint32_t dead_ms)
{
    struct node *nd = calloc(1, sizeof *nd);
    nd->id = id;
    struct pl_cfg c;
    memset(&c, 0, sizeof c);
    c.self.id = id;
    memset(c.self.uuid, 0x5a, 16);
    memset(c.self.ident, 0xB0 + (int)id, 16);
    c.secret = SECRET;
    c.secret_len = 32;
    c.arg = nd;
    c.on_link = on_link;
    c.on_frame = on_frame;
    c.on_sent = on_sent;
    c.ping_ms = ping_ms;
    c.dead_ms = dead_ms;
    c.backoff_min_ms = 20;
    c.backoff_max_ms = 200;
    nd->pl = peerlink_new(&c);
    if (!nd->pl) {
        fprintf(stderr, "peerlink_new failed\n");
        exit(2);
    }
    return nd;
}

/* ------------------------------------------------------------------------------------------------- a rig */

struct rig {
    struct pfk_fab *f;
    struct plr_pool *pa, *pb;
    struct node *a, *b;
    struct plf_pair *anchor;
    struct pl_transport *ta, *tb;
    struct pl_transport *xa, *xb; /* the anchor's ends, which ta and tb own */
    uint32_t qa, qb; /* the queue pairs' numbers */
};

static _Atomic int early_frees; /* slots the pools took back while the fake's NIC held them */

static void on_free(void *arg, const void *slot, size_t n)
{
    if (pfk_owned(arg, slot, n)) {
        fprintf(stderr, "plr_fake: VIOLATION: a slot back in the pool while a work request holds it\n");
        atomic_fetch_add(&early_frees, 1);
    }
}

static struct plr_pool_cfg small_cfg(void)
{
    struct plr_pool_cfg c = PLR_POOL_CFG_DEFAULT;
    c.pool_bytes = 1 << 20;
    c.slot = 4096;
    c.data_slots = 4;
    c.ctl_slots = 4;
    c.send_slots = 8;
    c.rnr_retry = 0;
    return c;
}

/* cb: b's configuration (NULL: a's) */
static void pools_open(struct rig *r, const struct plr_pool_cfg *cb)
{
    char err[256];
    struct plr_pool_cfg c = small_cfg();
    r->f = pfk_new();
    r->f->slow_flush = 1;
    r->pa = plr_pool_open(&pfk_ops, &c, err, sizeof err);
    r->pb = plr_pool_open(&pfk_ops, cb ? cb : &c, err, sizeof err);
    if (!r->pa || !r->pb) {
        fprintf(stderr, "pool: %s\n", err);
        exit(2);
    }
    plr_pool_test_on_free(r->pa, on_free, r->f);
    plr_pool_test_on_free(r->pb, on_free, r->f);
}

/* a connection of (a, b, LOG) on the fabric, as an upgrade would make it: endpoints through their encoding */
static void attach(struct rig *r)
{
    char err[256];
    struct plr_conn *ca = plr_conn_new(r->pa, err, sizeof err), *cb = ca ? plr_conn_new(r->pb, err, sizeof err) : NULL;
    if (!ca || !cb) {
        fprintf(stderr, "conn: %s\n", err);
        exit(2);
    }
    struct plr_ep ea, eb, da, db;
    uint8_t wa[PLR_EP_LEN], wb[PLR_EP_LEN];
    plr_conn_ep(ca, &ea);
    plr_conn_ep(cb, &eb);
    plr_ep_encode(&ea, wa);
    plr_ep_encode(&eb, wb);
    CHECK(plr_ep_decode(&da, wa, sizeof wa) == 0 && plr_ep_decode(&db, wb, sizeof wb) == 0);
    r->qa = ea.qpn;
    r->qb = eb.qpn;
    CHECK(plr_conn_start(cb, &da, err, sizeof err) == 0);
    CHECK(plr_conn_start(ca, &db, err, sizeof err) == 0);
    struct pl_transport *xa, *xb;
    r->anchor = plf_new(65536, &xa, &xb);
    r->xa = xa;
    r->xb = xb;
    r->ta = plr_conn_transport(ca, xa);
    r->tb = plr_conn_transport(cb, xb);
    int up_a = atomic_load(&r->a->ups), up_b = atomic_load(&r->b->ups);
    CHECK(peerlink_test_attach(r->a->pl, r->b->id, PL_KIND_LOG, 0, (const uint8_t *)"rdma", 4, r->ta) == 0);
    CHECK(peerlink_test_attach(r->b->pl, r->a->id, PL_KIND_LOG, 1, (const uint8_t *)"rdma", 4, r->tb) == 0);
    CHECK(WAIT_FOR(atomic_load(&r->a->ups) > up_a && atomic_load(&r->b->ups) > up_b, 3000));
}

static void rig_up_cfg(struct rig *r, uint32_t ping_ms, uint32_t dead_ms, const struct plr_pool_cfg *cb)
{
    memset(r, 0, sizeof *r);
    pools_open(r, cb);
    r->a = node_new(1, ping_ms, dead_ms);
    r->b = node_new(2, ping_ms, dead_ms);
    attach(r);
}

static void rig_up(struct rig *r, uint32_t ping_ms, uint32_t dead_ms) { rig_up_cfg(r, ping_ms, dead_ms, NULL); }

/* both peerlinks closed (every connection drained), then the end checks */
static void rig_down(struct rig *r, const char *name)
{
    peerlink_free(r->b->pl);
    peerlink_free(r->a->pl);
    struct plr_pool_stats sa, sb;
    plr_pool_stats(r->pa, &sa);
    plr_pool_stats(r->pb, &sb);
    int qps = pfk_live_qps(r->f), v = atomic_load(&r->f->violations), ef = atomic_load(&early_frees);
    printf("  %s: pools %u/%u and %u/%u free, lost %u %u, conns %u %u, queue pairs left %d, violations %d + %d, "
           "RNR %ld\n",
           name, (unsigned)sa.free, (unsigned)sa.slots, (unsigned)sb.free, (unsigned)sb.slots, (unsigned)sa.lost,
           (unsigned)sb.lost, (unsigned)sa.conns, (unsigned)sb.conns, qps, v, ef, r->f->rnr);
    CHECK(sa.free == sa.slots && sb.free == sb.slots && sa.lost == 0 && sb.lost == 0);
    CHECK(sa.conns == 0 && sb.conns == 0 && qps == 0);
    CHECK(v == 0 && ef == 0);
    plr_pool_close(r->pa);
    plr_pool_close(r->pb);
    pfk_free(r->f);
    if (r->anchor)
        plf_put(r->anchor);
    free(r->a);
    free(r->b);
    atomic_store(&early_frees, 0);
}

static struct plink *link_of(struct node *nd, uint32_t peer)
{
    struct plink *l = NULL;
    WAIT_FOR((l = peerlink_find(nd->pl, peer, PL_KIND_LOG)) != NULL && plink_up(l), 3000);
    return l;
}

static void send_frames(struct plink *l, uint64_t *next, int n, uint64_t *rs)
{
    for (int i = 0; i < n; i++) {
        *rs ^= *rs << 13, *rs ^= *rs >> 7, *rs ^= *rs << 17;
        size_t len = 8 + *rs % 9000;
        if (i % 61 == 0)
            len = 8 + 200000; /* fifty slots of it */
        uint8_t *b = malloc(len);
        fill(b, len, (*next)++);
        int rc = plink_send(l, 1, b, len, 5000);
        free(b);
        CHECK(rc == PL_OK);
        if (rc != PL_OK)
            return; /* one line is enough: the link is gone or held back for good */
    }
}

/* ------------------------------------------------------------------------------------------------- cases */

static void test_basic(void)
{
    struct rig r;
    rig_up(&r, 100, 2000);
    struct plink *la = link_of(r.a, 2), *lb = link_of(r.b, 1);
    CHECK(la && lb);
    if (la && lb) {
        int n = quick ? 300 : 1500;
        uint64_t na = 0, nb = 0, s1 = 7, s2 = 11;
        send_frames(la, &na, n, &s1); /* both ways at once: both sides run out of credits with frames queued */
        send_frames(lb, &nb, n, &s2);
        CHECK(WAIT_FOR(atomic_load(&r.a->in) == (uint64_t)n && atomic_load(&r.b->in) == (uint64_t)n, 20000));
        struct plr_conn_stats sa, sb;
        plr_conn_stats(r.ta, &sa);
        plr_conn_stats(r.tb, &sb);
        printf("  basic: %" PRIu64 " and %" PRIu64 " frames in, bad %" PRIu64 " %" PRIu64 ", downs %d %d; messages out "
               "%" PRIu64 " %" PRIu64 ", credits out %" PRIu64 " %" PRIu64 ", stalls %" PRIu64 " %" PRIu64 "\n",
               atomic_load(&r.a->in), atomic_load(&r.b->in), atomic_load(&r.a->bad), atomic_load(&r.b->bad),
               atomic_load(&r.a->downs), atomic_load(&r.b->downs), sa.msgs_out, sb.msgs_out, sa.credit_out,
               sb.credit_out, sa.stalls, sb.stalls);
        CHECK(atomic_load(&r.a->bad) == 0 && atomic_load(&r.b->bad) == 0);
        CHECK(atomic_load(&r.a->downs) == 0 && atomic_load(&r.b->downs) == 0);
        CHECK(sa.stalls > 0 && sb.stalls > 0); /* the windows were small enough to be used up */
    }
    rig_down(&r, "basic");
}

/* b reads nothing; a sends until held back. For 10 x dead_ms neither side counts the other dead: b PINGs on data
 * credits a returns by CREDIT, on control credits b posts again although paused. Then everything arrives. */
static void test_pause(void)
{
    struct rig r;
    uint32_t dead = 200;
    rig_up(&r, 40, dead);
    struct plink *la = link_of(r.a, 2), *lb = link_of(r.b, 1);
    CHECK(la && lb);
    if (la && lb) {
        plink_pause(lb, 1);
        uint64_t next = 0, rs = 5;
        int n = 40;
        send_frames(la, &next, n, &rs);
        usleep(10 * dead * 1000);
        struct plr_conn_stats sa, sb;
        plr_conn_stats(r.ta, &sa);
        plr_conn_stats(r.tb, &sb);
        printf("  pause: after %u ms paused: downs %d %d, frames in %" PRIu64 ", credits out %" PRIu64 " %" PRIu64
               ", stalls of a %" PRIu64 "\n",
               10 * dead, atomic_load(&r.a->downs), atomic_load(&r.b->downs), atomic_load(&r.b->in), sa.credit_out,
               sb.credit_out, sa.stalls);
        CHECK(atomic_load(&r.a->downs) == 0 && atomic_load(&r.b->downs) == 0);
        CHECK(sa.credit_out > 4); /* more CREDITs than control slots: they were posted again while paused */
        plink_pause(lb, 0);
        CHECK(WAIT_FOR(atomic_load(&r.b->in) == (uint64_t)n, 10000));
        CHECK(atomic_load(&r.b->bad) == 0);
    }
    rig_down(&r, "pause");
}

/* after traffic, an idle link: CREDITs only where data credits were owed, never a stream of them */
static void test_storm(void)
{
    struct rig r;
    rig_up(&r, 50, 2000);
    struct plink *la = link_of(r.a, 2);
    CHECK(la != NULL);
    if (la) {
        uint64_t next = 0, rs = 3;
        send_frames(la, &next, 50, &rs);
        CHECK(WAIT_FOR(atomic_load(&r.b->in) == 50, 10000));
        usleep(300000);
        struct plr_conn_stats a0, b0, a1, b1;
        plr_conn_stats(r.ta, &a0);
        plr_conn_stats(r.tb, &b0);
        usleep(1000000);
        plr_conn_stats(r.ta, &a1);
        plr_conn_stats(r.tb, &b1);
        uint64_t d = a1.credit_out - a0.credit_out + b1.credit_out - b0.credit_out;
        printf("  storm: CREDITs in an idle second %" PRIu64 " (PINGs every 50 ms)\n", d);
        CHECK(d <= 100);
    }
    rig_down(&r, "storm");
}

/* a frame posted while the NIC is held, then the queue pair fails: its sends are flushed, and it is WRITTEN (D5) */
static void test_written(void)
{
    struct rig r;
    rig_up(&r, 100, 2000);
    struct plink *la = link_of(r.a, 2);
    CHECK(la != NULL);
    if (la) {
        uint64_t next = 0, rs = 1;
        send_frames(la, &next, 5, &rs);
        CHECK(WAIT_FOR(atomic_load(&r.b->in) == 5, 5000));
        /* every data credit back before the NIC is held: else b's CREDIT may be what the hold keeps, the frame stays
         * in peerlink's buffer, and NEVER_WRITTEN would be right */
        struct plr_conn_stats s0;
        CHECK(WAIT_FOR((plr_conn_stats(r.ta, &s0), s0.credits == small_cfg().data_slots), 3000));
        atomic_store(&r.f->hold, 1);
        uint8_t b[300];
        fill(b, sizeof b, next++);
        int downs = atomic_load(&r.a->downs);
        plr_conn_stats(r.ta, &s0);
        CHECK(plink_sendv(la, 0, 1, NULL, 0, b, sizeof b, 1000, 1, 77, 0) == PL_OK);
        /* the frame itself taken by the transport (a CREDIT or a PING posted meanwhile carries fewer bytes) */
        struct plr_conn_stats s1;
        CHECK(WAIT_FOR((plr_conn_stats(r.ta, &s1), s1.bytes_out >= s0.bytes_out + sizeof b), 3000));
        CHECK(pfk_sq_posted(r.f, r.qa) > 0);
        pfk_fail_qp(r.f, r.qa);
        atomic_store(&r.f->hold, 0);
        CHECK(WAIT_FOR(atomic_load(&r.a->sent_token) / 2 == 77, 5000));
        uint64_t st = atomic_load(&r.a->sent_token);
        printf("  written: a frame posted, then flushed: on_sent %s, downs of a %d\n",
               st / 2 != 77 ? "never" : st & 1 ? "WRITTEN" : "NEVER_WRITTEN", atomic_load(&r.a->downs) - downs);
        CHECK(st == 77 * 2 + 1);
        CHECK(WAIT_FOR(atomic_load(&r.a->downs) > downs && atomic_load(&r.b->downs) > 0, 3000));
    }
    rig_down(&r, "written");
}

/* the 40th send after now fails mid-stream: both sides go down (the anchor's end tells the other), and a new
 * connection carries on from there */
static void test_qperr(void)
{
    struct rig r;
    rig_up(&r, 100, 2000);
    struct plink *la = link_of(r.a, 2);
    CHECK(la != NULL);
    if (la) {
        pthread_mutex_lock(&r.f->mu);
        r.f->err_send_at = r.f->sends + 40;
        pthread_mutex_unlock(&r.f->mu);
        uint64_t next = 0, rs = 9;
        for (int i = 0; i < 30; i++) { /* until the link goes: PL_E_DOWN or a full queue afterwards */
            uint8_t *b = malloc(20000);
            fill(b, 20000, next);
            if (plink_send(la, 1, b, 20000, 200) == PL_OK)
                next++;
            free(b);
        }
        (void)rs;
        CHECK(WAIT_FOR(atomic_load(&r.a->downs) > 0 && atomic_load(&r.b->downs) > 0, 5000));
        uint64_t got = atomic_load(&r.b->in);
        printf("  qperr: down on both sides after a failed send; %" PRIu64 " frames arrived of %" PRIu64
               " queued, bad %" PRIu64 "\n",
               got, next, atomic_load(&r.b->bad));
        CHECK(atomic_load(&r.b->bad) == 0);
        /* the next connection (the redial upgrades again): the frames after the last one that arrived */
        WAIT_FOR(0, 100);
        plf_put(r.anchor);
        attach(&r);
        la = link_of(r.a, 2);
        CHECK(la != NULL);
        if (la) {
            atomic_store(&r.b->next, got);
            uint64_t n2 = got, s = 4;
            send_frames(la, &n2, 100, &s);
            CHECK(WAIT_FOR(atomic_load(&r.b->in) == got + 100, 10000));
            CHECK(atomic_load(&r.b->bad) == 0);
        }
    }
    rig_down(&r, "qperr");
}

/* b's pool has smaller slots, more data and more control slots than a's: each side sends what the peer's numbers allow
 * (a message no longer than the peer's slot, as many DATA as the peer has data slots), both ways, then an idle link */
static void test_asym(void)
{
    struct plr_pool_cfg cb = small_cfg();
    cb.slot = 2048;
    cb.data_slots = 6;
    cb.ctl_slots = 5;
    struct rig r;
    rig_up_cfg(&r, 50, 2000, &cb);
    struct plink *la = link_of(r.a, 2), *lb = link_of(r.b, 1);
    CHECK(la && lb);
    if (la && lb) {
        int n = quick ? 200 : 800;
        uint64_t na = 0, nb = 0, s1 = 17, s2 = 19;
        send_frames(la, &na, n, &s1);
        send_frames(lb, &nb, n, &s2);
        CHECK(WAIT_FOR(atomic_load(&r.a->in) == (uint64_t)n && atomic_load(&r.b->in) == (uint64_t)n, 20000));
        usleep(300000);
        struct plr_conn_stats a0, b0, a1, b1;
        plr_conn_stats(r.ta, &a0);
        plr_conn_stats(r.tb, &b0);
        usleep(1000000);
        plr_conn_stats(r.ta, &a1);
        plr_conn_stats(r.tb, &b1);
        uint64_t d = a1.credit_out - a0.credit_out + b1.credit_out - b0.credit_out;
        printf("  asym: %" PRIu64 " and %" PRIu64 " frames in, bad %" PRIu64 " %" PRIu64 ", downs %d %d, credits held "
               "%" PRIu64 " %" PRIu64 ", CREDITs in an idle second %" PRIu64 "\n",
               atomic_load(&r.a->in), atomic_load(&r.b->in), atomic_load(&r.a->bad), atomic_load(&r.b->bad),
               atomic_load(&r.a->downs), atomic_load(&r.b->downs), a1.credits, b1.credits, d);
        CHECK(atomic_load(&r.a->bad) == 0 && atomic_load(&r.b->bad) == 0);
        CHECK(atomic_load(&r.a->downs) == 0 && atomic_load(&r.b->downs) == 0);
        CHECK(a1.credits == cb.data_slots && b1.credits == small_cfg().data_slots); /* each the peer's data slots */
        CHECK(d <= 100);
    }
    rig_down(&r, "asym");
}

/* a byte on the anchor (which carries none once the link is on RDMA) ends the connection on both sides */
static void test_anchor(void)
{
    struct rig r;
    rig_up(&r, 100, 2000);
    struct plink *la = link_of(r.a, 2);
    CHECK(la != NULL);
    if (la) {
        int da = atomic_load(&r.a->downs), db = atomic_load(&r.b->downs);
        /* on a's end of the anchor: the byte arrives at b's */
        struct iovec v = {(void *)"x", 1};
        CHECK(r.xa->ops->writev(r.xa, &v, 1) == 1);
        CHECK(WAIT_FOR(atomic_load(&r.b->downs) > db && atomic_load(&r.a->downs) > da, 3000));
        printf("  anchor: a byte on the anchor: downs of a %d, of b %d\n", atomic_load(&r.a->downs) - da,
               atomic_load(&r.b->downs) - db);
    }
    rig_down(&r, "anchor");
}

/* PINGs 10 s apart: nothing but a completion's event wakes the link threads. Frames of two messages each, and every
 * completion made while its queue pair is not armed shows only at the arm, without an event (the fake's arm_race): the
 * second message of a frame lands that way, and poll_fds must look once more after arming or it waits for a timer. */
static void test_wakeup(void)
{
    struct rig r;
    rig_up(&r, 10000, 60000);
    struct plink *la = link_of(r.a, 2);
    CHECK(la != NULL);
    if (la) {
        pthread_mutex_lock(&r.f->mu);
        r.f->arm_race = 1;
        pthread_mutex_unlock(&r.f->mu);
        uint64_t next = 0, worst = 0;
        int n = quick ? 60 : 200, late = 0;
        for (int i = 0; i < n && !late; i++) {
            uint8_t b[6000]; /* two messages of a 4 KiB slot */
            fill(b, sizeof b, next++);
            uint64_t t0 = now_ms();
            CHECK(plink_send(la, 1, b, sizeof b, 5000) == PL_OK);
            if (!WAIT_FOR(atomic_load(&r.b->in) == next, 3000))
                late = 1;
            uint64_t dt = now_ms() - t0;
            worst = dt > worst ? dt : worst;
            usleep(2000);
        }
        printf("  wakeup: %" PRIu64 " single frames, the slowest delivered in %" PRIu64 " ms%s\n", next, worst,
               late ? " (one waited for a timer: a lost wakeup)" : "");
        CHECK(!late);
        pthread_mutex_lock(&r.f->mu);
        r.f->arm_race = 0;
        pthread_mutex_unlock(&r.f->mu);
    }
    rig_down(&r, "wakeup");
}

/* the error paths of a connection's setup, and a drain that never finishes */
static void test_errors(void)
{
    char err[256];
    struct pfk_fab *f = pfk_new();
    struct plr_pool_cfg c = small_cfg();
    f->fail_dev = 1;
    struct plr_pool *p = plr_pool_open(&pfk_ops, &c, err, sizeof err);
    printf("  errors: no device: %s\n", p ? "opened" : err);
    CHECK(!p);
    if (p)
        plr_pool_close(p);
    f->fail_dev = 0;
    p = plr_pool_open(&pfk_ops, &c, err, sizeof err);
    CHECK(p != NULL);
    if (!p) {
        pfk_free(f);
        return;
    }
    struct plr_pool_stats st;
    /* no queue pair: the slots the connection took go back */
    f->fail_qp_new = 1;
    struct plr_conn *x = plr_conn_new(p, err, sizeof err);
    printf("  errors: no queue pair: %s\n", x ? "made" : err);
    CHECK(!x);
    f->fail_qp_new = 0;
    plr_pool_stats(p, &st);
    CHECK(st.free == st.slots && st.conns == 0 && pfk_live_qps(f) == 0);
    /* the connect fails: the caller frees a queue pair that never reached RTS */
    x = plr_conn_new(p, err, sizeof err);
    struct plr_conn *y = x ? plr_conn_new(p, err, sizeof err) : NULL;
    CHECK(x && y);
    if (x && y) {
        struct plr_ep ex, ey;
        plr_conn_ep(x, &ex);
        plr_conn_ep(y, &ey);
        f->fail_rts = 1;
        int rc = plr_conn_start(x, &ey, err, sizeof err);
        printf("  errors: the connect refused: %s\n", rc ? err : "connected");
        CHECK(rc < 0);
        f->fail_rts = 0;
        plr_conn_free(x);
        plr_conn_free(y);
        plr_pool_stats(p, &st);
        CHECK(st.free == st.slots && st.lost == 0 && st.conns == 0 && pfk_live_qps(f) == 0);
    }
    /* a drain that never finishes: the queue pair goes, its slots stay out of the pool for good */
    x = plr_conn_new(p, err, sizeof err);
    CHECK(x != NULL);
    if (x) {
        int d0 = plrdma_drain_ms;
        plrdma_drain_ms = 200;
        pthread_mutex_lock(&f->mu);
        f->no_flush = 1;
        pthread_mutex_unlock(&f->mu);
        plr_conn_free(x);
        plrdma_drain_ms = d0;
        pthread_mutex_lock(&f->mu);
        f->no_flush = 0;
        pthread_mutex_unlock(&f->mu);
        uint32_t per = c.data_slots + c.ctl_slots + c.send_slots;
        plr_pool_stats(p, &st);
        printf("  errors: a drain that never finished: %u slots lost, %u/%u free, queue pairs left %d\n",
               (unsigned)st.lost, (unsigned)st.free, (unsigned)st.slots, pfk_live_qps(f));
        CHECK(st.lost == per && st.free == st.slots - per && st.conns == 0 && pfk_live_qps(f) == 0);
    }
    plr_pool_close(p);
    CHECK(atomic_load(&f->violations) == 0);
    pfk_free(f);
}

static void test_pool(void)
{
    char err[256];
    struct pfk_fab *f = pfk_new();
    struct plr_pool_cfg c = small_cfg();
    /* the memlock limit named, before anything is registered */
    struct rlimit rl0, rl;
    CHECK(getrlimit(RLIMIT_MEMLOCK, &rl0) == 0);
    rl = rl0;
    rl.rlim_cur = 512 * 1024;
    int lowered = setrlimit(RLIMIT_MEMLOCK, &rl) == 0;
    struct plr_pool *p = plr_pool_open(&pfk_ops, &c, err, sizeof err);
    printf("  pool: with RLIMIT_MEMLOCK 512 KiB and a 1 MiB pool: %s\n", p ? "opened" : err);
    CHECK(!lowered || (!p && strstr(err, "RLIMIT_MEMLOCK") && strstr(err, "1024 KiB")));
    if (p)
        plr_pool_close(p);
    CHECK(setrlimit(RLIMIT_MEMLOCK, &rl0) == 0 || !lowered);
    /* a registration that fails */
    f->fail_reg = 1;
    p = plr_pool_open(&pfk_ops, &c, err, sizeof err);
    printf("  pool: registration refused: %s\n", p ? "opened" : err);
    CHECK(!p && strstr(err, "registering"));
    f->fail_reg = 0;
    /* control slots below 4 (a credit storm), above 255 (one header byte), and more slots than a ring counts */
    static const uint16_t bad[][2] = {{4, 1}, {4, 3}, {4, 256}, {65532, 4}};
    for (size_t i = 0; i < sizeof bad / sizeof bad[0]; i++) {
        struct plr_pool_cfg b = c;
        b.data_slots = bad[i][0];
        b.ctl_slots = bad[i][1];
        b.slot = 1024;
        b.pool_bytes = (size_t)b.slot * (b.data_slots + b.ctl_slots + b.send_slots);
        p = plr_pool_open(&pfk_ops, &b, err, sizeof err);
        printf("  pool: %u data and %u control slots: %s\n", (unsigned)b.data_slots, (unsigned)b.ctl_slots,
               p ? "opened" : err);
        CHECK(!p && strstr(err, "control slots must be"));
        if (p)
            plr_pool_close(p);
    }
    /* room for two connections: the third is refused with a line, and fits once one is gone */
    c.pool_bytes = (size_t)2 * 16 * c.slot;
    p = plr_pool_open(&pfk_ops, &c, err, sizeof err);
    CHECK(p != NULL);
    if (p) {
        struct plr_conn *x = plr_conn_new(p, err, sizeof err), *y = plr_conn_new(p, err, sizeof err);
        struct plr_conn *z = plr_conn_new(p, err, sizeof err);
        printf("  pool: a third connection in a pool for two: %s\n", z ? "made" : err);
        CHECK(x && y && !z && strstr(err, "free slots"));
        plr_conn_free(y);
        z = plr_conn_new(p, err, sizeof err);
        CHECK(z != NULL);
        plr_conn_free(x);
        plr_conn_free(z);
        struct plr_pool_stats st;
        plr_pool_stats(p, &st);
        CHECK(st.free == st.slots && st.conns == 0);
        plr_pool_close(p);
    }
    CHECK(pfk_live_qps(f) == 0 && atomic_load(&f->violations) == 0);
    pfk_free(f);
}

static void test_ep(void)
{
    struct plr_ep e = {0}, d;
    e.lid = 21;
    for (int i = 0; i < 16; i++)
        e.gid[i] = (uint8_t)(0xf0 + i);
    e.qpn = 0x123456;
    e.psn = 0xabcdef;
    e.mtu = 5;
    e.link_layer = PLR_LL_IB;
    e.port = 1;
    e.slot = 32768;
    e.data = 12;
    e.ctl = 4;
    uint8_t w[PLR_EP_LEN];
    plr_ep_encode(&e, w);
    CHECK(plr_ep_decode(&d, w, sizeof w) == 0);
    CHECK(d.lid == e.lid && !memcmp(d.gid, e.gid, 16) && d.qpn == e.qpn && d.psn == e.psn && d.mtu == 5 &&
          d.link_layer == PLR_LL_IB && d.port == 1 && d.slot == 32768 && d.data == 12 && d.ctl == 4);
    CHECK(plr_ep_decode(&d, w, sizeof w - 1) < 0);
    uint8_t x[PLR_EP_LEN];
    static const int offs[] = {0, 1, 2, 25, 33, 34, 36};
    for (size_t i = 0; i < sizeof offs / sizeof offs[0]; i++) {
        memcpy(x, w, sizeof w);
        if (offs[i] == 34 || offs[i] == 36)
            x[offs[i]] = x[offs[i] + 1] = 0; /* no credits */
        else if (offs[i] == 33)
            memset(x + 30, 0, 4); /* no slot */
        else
            x[offs[i]] = 0xff; /* another version, an MTU or link layer no port has, a QPN above 2^24 */
        CHECK(plr_ep_decode(&d, x, sizeof x) < 0);
    }
    /* a peer's control slots below 4 or above 255, and data and control slots above what a ring counts */
    static const uint16_t bad[][2] = {{12, 3}, {12, 1}, {12, 256}, {65535, 4}};
    for (size_t i = 0; i < sizeof bad / sizeof bad[0]; i++) {
        struct plr_ep b = e;
        b.data = bad[i][0];
        b.ctl = bad[i][1];
        plr_ep_encode(&b, x);
        CHECK(plr_ep_decode(&d, x, sizeof x) < 0);
    }
}

/* ------------------------------------------------------------------------------------------------- two hosts */
/*
 * --verbs-listen ADDR --port-file F on one host, --verbs-dial ADDR:PORT on the other: the transport on real verbs
 * (docs/design-rdma.md §3, "verbs under peerlink"). ADDR is an Ethernet address of the hosts: the TCP connection
 * carries the two endpoints (as rd-upgrade's OFFER and ACCEPT will) and then stays as the connection's anchor, never
 * another byte. --tcp (the dialer's): the same link on that TCP connection itself, for the comparison; --log: a log
 * link, which compresses every frame (zstd-1), instead of a bulk link (unthrottled here), which does not. The listener
 * binds port 0 and writes "PORT PID" to F (atomically); it serves one session and exits.
 * A session: `check` (frames of random sizes both ways at once, every byte compared, both sides out of credits with
 * frames queued), then the dialer's bench: per frame size (--sizes), a BEGIN frame, N frames one way (--bytes in
 * all), the receiver's DONE with its elapsed time and CPU; Gbit/s from the sender's clock (BEGIN to DONE), CPU s/GB of
 * each process (getrusage: every thread), then END. Each pool must be whole after the link closed.
 */
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static uint64_t cpu_ns(void)
{
    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);
    return ((uint64_t)ru.ru_utime.tv_sec + (uint64_t)ru.ru_stime.tv_sec) * 1000000000ull +
           ((uint64_t)ru.ru_utime.tv_usec + (uint64_t)ru.ru_stime.tv_usec) * 1000ull;
}

/* BEGIN (2): size, count; DONE (3): elapsed ns, cpu ns; END (4). Type 1 in a bench: the frame's number first. */
static void hosts_frame(struct node *nd, uint8_t type, const uint8_t *p, size_t n)
{
    uint64_t a = 0, b = 0;
    if (type != 4 && type != 1 && n != 16) {
        atomic_fetch_add(&nd->bad, 1);
        return;
    }
    if (type == 2 || type == 3) {
        memcpy(&a, p, 8);
        memcpy(&b, p + 8, 8);
    }
    switch (type) {
    case 1: {
        uint64_t i = 0;
        if (n >= 8)
            memcpy(&i, p, 8);
        if (n != atomic_load(&nd->bsize) || i != atomic_load(&nd->next))
            atomic_fetch_add(&nd->bad, 1);
        atomic_store(&nd->next, i + 1);
        if (atomic_fetch_add(&nd->in, 1) + 1 == atomic_load(&nd->bn)) {
            nd->b_t1 = now_ns();
            nd->b_cpu1 = cpu_ns();
            atomic_fetch_add(&nd->bdone, 1);
        }
        break;
    }
    case 2:
        nd->b_t0 = now_ns();
        nd->b_cpu0 = cpu_ns();
        atomic_store(&nd->bsize, a);
        atomic_store(&nd->bn, b);
        atomic_store(&nd->next, 0);
        atomic_store(&nd->in, 0);
        atomic_store(&nd->bench, 1);
        break;
    case 3:
        nd->r_ns = a;
        nd->r_cpu = b;
        atomic_fetch_add(&nd->replied, 1);
        break;
    case 4:
        atomic_store(&nd->ended, 1);
        break;
    }
}

/* the anchor: a plain socket (peerlink's own TCP transport is not exported) */
struct ttcp {
    struct pl_transport t;
    int fd;
};
static ssize_t ttcp_read(struct pl_transport *t, void *p, size_t n)
{
    return recv(((struct ttcp *)t)->fd, p, n, MSG_DONTWAIT);
}
static ssize_t ttcp_writev(struct pl_transport *t, const struct iovec *iov, int n)
{
    struct msghdr m;
    memset(&m, 0, sizeof m);
    m.msg_iov = (struct iovec *)iov;
    m.msg_iovlen = (size_t)n;
    return sendmsg(((struct ttcp *)t)->fd, &m, MSG_DONTWAIT | MSG_NOSIGNAL);
}
static int ttcp_poll_fds(struct pl_transport *t, int want, struct pollfd *pf, int *ready)
{
    pf[0].fd = ((struct ttcp *)t)->fd;
    pf[0].events = (short)(((want & PL_TR_IN) ? POLLIN : 0) | ((want & PL_TR_OUT) ? POLLOUT : 0));
    pf[0].revents = 0;
    *ready = 0;
    return 1;
}
static int ttcp_poll_ready(struct pl_transport *t, const struct pollfd *pf, int k)
{
    (void)t;
    int r = 0;
    if (k > 0 && (pf[0].revents & (POLLIN | POLLHUP | POLLERR)))
        r |= PL_TR_IN;
    if (k > 0 && (pf[0].revents & POLLOUT))
        r |= PL_TR_OUT;
    return r;
}
static void ttcp_close(struct pl_transport *t)
{
    close(((struct ttcp *)t)->fd);
    free(t);
}
static const struct pl_tr_ops ttcp_ops = {"tcp", ttcp_read, ttcp_writev, ttcp_poll_fds, ttcp_poll_ready, ttcp_close};

static struct pl_transport *ttcp_new(int fd)
{
    struct ttcp *t = calloc(1, sizeof *t);
    if (!t)
        exit(2);
    t->t.ops = &ttcp_ops;
    t->fd = fd;
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
    return &t->t;
}

static int full_io(int fd, void *p, size_t n, int wr)
{
    for (size_t k = 0; k < n;) {
        ssize_t r = wr ? send(fd, (uint8_t *)p + k, n - k, MSG_NOSIGNAL) : recv(fd, (uint8_t *)p + k, n - k, 0);
        if (r <= 0)
            return -1;
        k += (size_t)r;
    }
    return 0;
}

static int parse_addr(const char *s, struct sockaddr_in *sa, int need_port)
{
    char host[64];
    const char *c = strrchr(s, ':');
    size_t hl = c ? (size_t)(c - s) : strlen(s);
    if (hl >= sizeof host || (need_port && !c))
        return -1;
    memcpy(host, s, hl);
    host[hl] = 0;
    memset(sa, 0, sizeof *sa);
    sa->sin_family = AF_INET;
    sa->sin_port = htons(c ? (uint16_t)atoi(c + 1) : 0);
    return inet_pton(AF_INET, host, &sa->sin_addr) == 1 ? 0 : -1;
}

struct hosts_opt {
    const char *listen, *dial, *port_file, *dev, *sizes;
    int tcp, kind;
    uint64_t bytes;
    struct plr_pool_cfg cfg;
};

static void hosts_bench(struct node *nd, struct plink *l, struct pl_transport *t, const struct hosts_opt *o)
{
    char sizes[256];
    snprintf(sizes, sizeof sizes, "%s", o->sizes);
    for (char *sv = NULL, *tok = strtok_r(sizes, ",", &sv); tok; tok = strtok_r(NULL, ",", &sv)) {
        uint64_t size = strtoull(tok, NULL, 0), count = o->bytes / size;
        if (size < 8 || size > PL_FRAME_MAX)
            continue;
        if (count > 4000000)
            count = 4000000;
        if (count < 100)
            count = 100;
        uint8_t *b = malloc(size);
        fill(b, size, 0);
        struct plr_conn_stats s0, s1;
        memset(&s0, 0, sizeof s0);
        memset(&s1, 0, sizeof s1);
        if (!o->tcp)
            plr_conn_stats(t, &s0);
        uint64_t rep = atomic_load(&nd->replied), t0 = now_ns(), c0 = cpu_ns(), hdr[2] = {size, count};
        int rc = plink_send(l, 2, hdr, 16, 10000);
        for (uint64_t i = 0; rc == PL_OK && i < count; i++) {
            memcpy(b, &i, 8);
            rc = plink_send(l, 1, b, size, 10000);
        }
        free(b);
        CHECK(rc == PL_OK && WAIT_FOR(atomic_load(&nd->replied) > rep, 120000));
        uint64_t ns = now_ns() - t0, c = cpu_ns() - c0;
        if (!o->tcp)
            plr_conn_stats(t, &s1);
        double gb = (double)size * (double)count / 1e9;
        printf("bench %s %s %" PRIu64 ": %.2f Gbit/s, CPU s/GB sender %.3f receiver %.3f (%" PRIu64 " frames; receiver "
               "%.2f Gbit/s; messages %" PRIu64 ", credits in %" PRIu64 ", stalls %" PRIu64 ")\n",
               o->tcp ? "tcp" : "verbs", o->kind == PL_KIND_BULK ? "bulk" : "log", size, gb * 8 / ((double)ns / 1e9), (double)c / 1e9 / gb,
               (double)nd->r_cpu / 1e9 / gb, count, gb * 8 / ((double)nd->r_ns / 1e9), s1.msgs_out - s0.msgs_out,
               s1.credit_in - s0.credit_in, s1.stalls - s0.stalls);
        if (rc != PL_OK)
            return;
    }
}

static int hosts_run(const struct hosts_opt *o)
{
    int dialer = o->dial != NULL, fd = -1;
    struct sockaddr_in sa;
    if (parse_addr(dialer ? o->dial : o->listen, &sa, dialer) < 0) {
        fprintf(stderr, "bad address %s\n", dialer ? o->dial : o->listen);
        return 2;
    }
    if (dialer) {
        fd = socket(AF_INET, SOCK_STREAM, 0);
        if (connect(fd, (struct sockaddr *)&sa, sizeof sa) < 0) {
            perror("connect");
            return 2;
        }
    } else {
        int ls = socket(AF_INET, SOCK_STREAM, 0);
        socklen_t sl = sizeof sa;
        if (bind(ls, (struct sockaddr *)&sa, sizeof sa) < 0 || listen(ls, 1) < 0 ||
            getsockname(ls, (struct sockaddr *)&sa, &sl) < 0) {
            perror("listen");
            return 2;
        }
        if (o->port_file) {
            char tmp[512];
            snprintf(tmp, sizeof tmp, "%s.tmp", o->port_file);
            FILE *f = fopen(tmp, "w");
            if (!f || fprintf(f, "%u %d\n", (unsigned)ntohs(sa.sin_port), (int)getpid()) < 0 || fclose(f) != 0 ||
                rename(tmp, o->port_file) < 0) {
                perror("port file");
                return 2;
            }
        }
        printf("PORT %u\n", (unsigned)ntohs(sa.sin_port));
        struct pollfd pf = {ls, POLLIN, 0};
        if (poll(&pf, 1, 120000) != 1 || (fd = accept(ls, NULL, NULL)) < 0) {
            fprintf(stderr, "no dialer within 120 s\n");
            return 2;
        }
        close(ls);
    }
    tsdb_nosigpipe(fd); /* where MSG_NOSIGNAL is 0 (src/compat.h) */
    uint8_t mode[2] = {(uint8_t)o->tcp, (uint8_t)o->kind}; /* the dialer's */
    if (full_io(fd, mode, 2, dialer) < 0 || (mode[1] != PL_KIND_LOG && mode[1] != PL_KIND_BULK)) {
        fprintf(stderr, "handshake: the peer went\n");
        return 2;
    }
    struct hosts_opt oo = *o;
    oo.tcp = mode[0];
    oo.kind = mode[1];
    o = &oo;
    struct plr_pool *pool = NULL;
    struct pl_transport *t;
    if (o->tcp) {
        t = ttcp_new(fd);
    } else {
        char err[256];
        const struct plr_ops *ops = plrdma_verbs(err, sizeof err);
        struct plr_pool_cfg c = o->cfg;
        c.dev = o->dev;
        pool = ops ? plr_pool_open(ops, &c, err, sizeof err) : NULL;
        struct plr_conn *conn = pool ? plr_conn_new(pool, err, sizeof err) : NULL;
        if (!conn) {
            fprintf(stderr, "verbs: %s\n", err);
            return 2;
        }
        struct plr_ep me, peer;
        uint8_t w[PLR_EP_LEN], r[PLR_EP_LEN];
        plr_conn_ep(conn, &me);
        plr_ep_encode(&me, w);
        if (full_io(fd, w, sizeof w, 1) < 0 || full_io(fd, r, sizeof r, 0) < 0 || plr_ep_decode(&peer, r, sizeof r) < 0) {
            fprintf(stderr, "endpoint exchange failed\n");
            return 2;
        }
        if (plr_conn_start(conn, &peer, err, sizeof err) < 0) {
            fprintf(stderr, "start: %s\n", err);
            return 2;
        }
        printf("verbs: lid %u qpn %u mtu %u -> lid %u qpn %u; slot %u, credits %u + %u\n", (unsigned)me.lid,
               (unsigned)me.qpn, (unsigned)me.mtu, (unsigned)peer.lid, (unsigned)peer.qpn, (unsigned)peer.slot,
               (unsigned)peer.data, (unsigned)peer.ctl);
        t = plr_conn_transport(conn, ttcp_new(fd));
    }
    uint32_t self = dialer ? 1 : 2, other = dialer ? 2 : 1;
    struct node *nd = node_new(self, 200, 5000);
    peerlink_set_bulk_rate(nd->pl, PL_RATE_UNLIMITED);
    CHECK(peerlink_test_attach(nd->pl, other, o->kind, dialer, (const uint8_t *)"host", 4, t) == 0);
    struct plink *l = NULL;
    WAIT_FOR((l = peerlink_find(nd->pl, other, o->kind)) != NULL && plink_up(l), 3000);
    CHECK(l != NULL);
    if (l) {
        int n = 2000; /* not --quick: the two sides must agree */
        uint64_t next = 0, rs = dialer ? 7 : 11;
        uint64_t c0 = cpu_ns(), t0 = now_ns();
        send_frames(l, &next, n, &rs);
        CHECK(WAIT_FOR(atomic_load(&nd->in) == (uint64_t)n, 60000));
        printf("check %s: %" PRIu64 " frames in, bad %" PRIu64 ", downs %d (%.0f ms, CPU %.0f ms)\n",
               o->tcp ? "tcp" : "verbs", atomic_load(&nd->in), atomic_load(&nd->bad), atomic_load(&nd->downs),
               (double)(now_ns() - t0) / 1e6, (double)(cpu_ns() - c0) / 1e6);
        CHECK(atomic_load(&nd->bad) == 0 && atomic_load(&nd->downs) == 0);
        if (dialer) {
            /* the listener's DONE of its check first: a BEGIN before it has counted would reset its count */
            CHECK(WAIT_FOR(atomic_load(&nd->replied) > 0, 60000));
            hosts_bench(nd, l, t, o);
            CHECK(plink_send(l, 4, NULL, 0, 10000) == PL_OK);
            CHECK(WAIT_FOR(atomic_load(&nd->downs) > 0, 30000)); /* the listener closes */
        } else {
            uint64_t z[2] = {0, 0}, done = 0;
            CHECK(plink_send(l, 3, z, 16, 10000) == PL_OK);
            while (!atomic_load(&nd->ended) && atomic_load(&nd->downs) == 0) {
                if (atomic_load(&nd->bdone) > done) {
                    done = atomic_load(&nd->bdone);
                    uint64_t r[2] = {nd->b_t1 - nd->b_t0, nd->b_cpu1 - nd->b_cpu0};
                    CHECK(plink_send(l, 3, r, 16, 10000) == PL_OK);
                }
                usleep(200);
            }
            CHECK(atomic_load(&nd->ended));
            CHECK(atomic_load(&nd->bad) == 0);
        }
    }
    peerlink_free(nd->pl);
    if (pool) {
        struct plr_pool_stats st;
        plr_pool_stats(pool, &st);
        printf("pool: %u/%u slots free, %u lost, %u connections\n", (unsigned)st.free, (unsigned)st.slots,
               (unsigned)st.lost, (unsigned)st.conns);
        CHECK(st.free == st.slots && st.lost == 0 && st.conns == 0);
        plr_pool_close(pool);
    }
    free(nd);
    printf("test_plrdma %s: %s (%d failures)\n", dialer ? "dial" : "listen", failures ? "FAILED" : "ok", failures);
    return failures ? 1 : 0;
}

static const struct {
    const char *name;
    int m;
} mutants[] = {{"writtencqe", PLR_MUT_WRITTENCQE}, {"noctl", PLR_MUT_NOCTL},       {"ctlstorm", PLR_MUT_CTLSTORM},
               {"pausectl", PLR_MUT_PAUSECTL},     {"nodrain", PLR_MUT_NODRAIN},   {"unsigearly", PLR_MUT_UNSIGEARLY},
               {"nocredit", PLR_MUT_NOCREDIT},     {"splitoff", PLR_MUT_SPLITOFF}, {"room", PLR_MUT_ROOM},
               {"crown", PLR_MUT_CROWN},           {"norecvmark", PLR_MUT_NORECVMARK},
               {"norearm", PLR_MUT_NOREARM},       {"anchorbyte", PLR_MUT_ANCHORBYTE}};

int main(int argc, char **argv)
{
    const char *only = NULL;
    struct hosts_opt ho;
    memset(&ho, 0, sizeof ho);
    ho.cfg = (struct plr_pool_cfg)PLR_POOL_CFG_DEFAULT;
    ho.cfg.pool_bytes = (size_t)4 << 20; /* the test servers' memlock is 8 MB */
    ho.cfg.rnr_retry = 0; /* a SEND that finds no posted receive fails the queue pair: the credit rule on real verbs */
    ho.sizes = "4096,16384,65536,262144,1048576,4194304";
    ho.bytes = (uint64_t)4 << 30;
    ho.kind = PL_KIND_BULK; /* frames as they are: the bulk link does not compress */
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--quick"))
            quick = 1;
        else if (!strcmp(argv[i], "--verbs-listen") && i + 1 < argc)
            ho.listen = argv[++i];
        else if (!strcmp(argv[i], "--verbs-dial") && i + 1 < argc)
            ho.dial = argv[++i];
        else if (!strcmp(argv[i], "--port-file") && i + 1 < argc)
            ho.port_file = argv[++i];
        else if (!strcmp(argv[i], "--dev") && i + 1 < argc)
            ho.dev = argv[++i];
        else if (!strcmp(argv[i], "--sizes") && i + 1 < argc)
            ho.sizes = argv[++i];
        else if (!strcmp(argv[i], "--bytes") && i + 1 < argc)
            ho.bytes = strtoull(argv[++i], NULL, 0);
        else if (!strcmp(argv[i], "--tcp"))
            ho.tcp = 1;
        else if (!strcmp(argv[i], "--log"))
            ho.kind = PL_KIND_LOG; /* zstd-1 on every frame, as the log link does */
        else if (!strcmp(argv[i], "--slot") && i + 1 < argc)
            ho.cfg.slot = (uint32_t)strtoul(argv[++i], NULL, 0);
        else if (!strcmp(argv[i], "--credits") && i + 1 < argc)
            ho.cfg.data_slots = (uint16_t)strtoul(argv[++i], NULL, 0);
        else if (!strcmp(argv[i], "--send-slots") && i + 1 < argc)
            ho.cfg.send_slots = (uint16_t)strtoul(argv[++i], NULL, 0);
        else if (!strcmp(argv[i], "--rnr-retry") && i + 1 < argc)
            ho.cfg.rnr_retry = (uint8_t)strtoul(argv[++i], NULL, 0);
        else if (!strcmp(argv[i], "--only") && i + 1 < argc)
            only = argv[++i];
        else if (!strcmp(argv[i], "--mutant") && i + 1 < argc) {
            const char *m = argv[++i];
            size_t k = 0;
            while (k < sizeof mutants / sizeof mutants[0] && strcmp(mutants[k].name, m))
                k++;
            if (k == sizeof mutants / sizeof mutants[0]) {
                fprintf(stderr, "unknown mutant %s\n", m);
                return 2;
            }
            plrdma_mutant = mutants[k].m;
            printf("MUTANT %s\n", m);
        } /* else: the scratch directory, unused */
    }
    setvbuf(stdout, NULL, _IOLBF, 0);
    if (ho.listen || ho.dial)
        return hosts_run(&ho);
    static const struct {
        const char *name;
        void (*fn)(void);
    } cases[] = {{"ep", test_ep},         {"pool", test_pool},       {"basic", test_basic}, {"pause", test_pause},
                 {"storm", test_storm},   {"written", test_written}, {"qperr", test_qperr},  {"asym", test_asym},
                 {"anchor", test_anchor}, {"wakeup", test_wakeup},   {"errors", test_errors}};
    for (size_t i = 0; i < sizeof cases / sizeof cases[0]; i++) {
        if (only && !strstr(only, cases[i].name))
            continue;
        printf("%s\n", cases[i].name);
        cases[i].fn();
        if (failures && plrdma_mutant)
            break; /* caught: the rest would only take longer */
    }
    printf("test_plrdma: %s (%d failures)\n", failures ? "FAILED" : "ok", failures);
    return failures ? 1 : 0;
}
