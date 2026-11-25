/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The verbs transport (server/plrdma.[ch]) under peerlink, over the fake fabric of tests/plr_fake.h (docs/design-rdma.md
 * §8): two pools (two tsdbd) on one fabric, a connection each, numbers exchanged as rd-upgrade will exchange them
 * (encoded and decoded), the link threads of two peerlinks on the transports with a tests/pl_fake.h pair as each
 * connection's TCP anchor. Every case ends with the pools whole (every slot back, none lost), no queue pair left and no
 * violation of the fake (a buffer posted twice, a slot taken back while the NIC holds it, a queue pair destroyed
 * undrained, numbers that are not the peer's, a SEND without a posted receive).
 *
 * Cases: basic (frames both ways, many slots each, small credit windows, so both sides run out of credits with frames
 * queued), pause (a paused link that only PINGs for 10 x dead_ms is not counted dead), storm (an idle link sends no
 * stream of CREDITs), written (a frame posted and then flushed by a failed queue pair is WRITTEN), qperr (a send that
 * fails mid-stream ends the connection on both sides; the next connection carries on), pool (memlock, a failed
 * registration, a pool with room for two connections), ep (the endpoint's encoding).
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

static void on_frame(void *arg, struct plink *l, uint8_t type, const uint8_t *p, size_t n)
{
    struct node *nd = arg;
    (void)l;
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

static void pools_open(struct rig *r)
{
    char err[256];
    struct plr_pool_cfg c = small_cfg();
    r->f = pfk_new();
    r->pa = plr_pool_open(&pfk_ops, &c, err, sizeof err);
    r->pb = plr_pool_open(&pfk_ops, &c, err, sizeof err);
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
    r->ta = plr_conn_transport(ca, xa);
    r->tb = plr_conn_transport(cb, xb);
    int up_a = atomic_load(&r->a->ups), up_b = atomic_load(&r->b->ups);
    CHECK(peerlink_test_attach(r->a->pl, r->b->id, PL_KIND_LOG, 0, (const uint8_t *)"rdma", 4, r->ta) == 0);
    CHECK(peerlink_test_attach(r->b->pl, r->a->id, PL_KIND_LOG, 1, (const uint8_t *)"rdma", 4, r->tb) == 0);
    CHECK(WAIT_FOR(atomic_load(&r->a->ups) > up_a && atomic_load(&r->b->ups) > up_b, 3000));
}

static void rig_up(struct rig *r, uint32_t ping_ms, uint32_t dead_ms)
{
    memset(r, 0, sizeof *r);
    pools_open(r);
    r->a = node_new(1, ping_ms, dead_ms);
    r->b = node_new(2, ping_ms, dead_ms);
    attach(r);
}

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
        atomic_store(&r.f->hold, 1);
        uint8_t b[300];
        fill(b, sizeof b, next++);
        int downs = atomic_load(&r.a->downs);
        CHECK(plink_sendv(la, 0, 1, NULL, 0, b, sizeof b, 1000, 1, 77, 0) == PL_OK);
        CHECK(WAIT_FOR(pfk_sq_posted(r.f, r.qa) > 0, 3000));
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
}

static const struct {
    const char *name;
    int m;
} mutants[] = {{"writtencqe", PLR_MUT_WRITTENCQE}, {"noctl", PLR_MUT_NOCTL},       {"ctlstorm", PLR_MUT_CTLSTORM},
               {"pausectl", PLR_MUT_PAUSECTL},     {"nodrain", PLR_MUT_NODRAIN},   {"unsigearly", PLR_MUT_UNSIGEARLY},
               {"nocredit", PLR_MUT_NOCREDIT},     {"splitoff", PLR_MUT_SPLITOFF}};

int main(int argc, char **argv)
{
    const char *only = NULL;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--quick"))
            quick = 1;
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
    static const struct {
        const char *name;
        void (*fn)(void);
    } cases[] = {{"ep", test_ep},         {"pool", test_pool},       {"basic", test_basic}, {"pause", test_pause},
                 {"storm", test_storm},   {"written", test_written}, {"qperr", test_qperr}};
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
