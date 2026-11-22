/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Forwarding one hop, the request half (server/fwdreq.h): the FORWARD envelope, the reply's tail, and the entry
 * node's rules (fwdr_run) driven over a fake link, no tsdbd, no sockets. The link's events reach the table the way
 * fwd.c's dispatcher hands them over (fwdr_on_event, from another thread after a delay, or at once inside the send).
 * The clock is CLOCK_MONOTONIC plus an offset a case may push forward (the stall hook does, to stand for an entry
 * node stalled past fwd_max_age). Usage: test_fwd [SCRATCHDIR] [--only NAME] [--mutant noage|retry|unavonbreak|teardown]
 * (each planted bug of fwdr_req must fail a case).
 *
 *   env        envelope round trip; short, another magic, hops 0 refused; the payload is the client's bytes
 *   tail       tail round trip at the end of a longer reply; short or another magic refused
 *   done       a write: SENT, REPLY with tail -> relayed without the tail, stats kept, age stamped, waits balanced
 *   old        a frame older than max_age is never sent (mutant noage sends it)
 *   stall      the entry stalls at the pre-send hook past max_age: never sent (mutant noage sends it)
 *   gone       a client gone before the send: never sent, no answer
 *   unreach    sends refused (link down) until wait_ms: UNAVAILABLE, definite, tried more than once
 *   refusedok  refused twice, then taken: the reply comes (a send the link never took may be tried again)
 *   toobig     above the link's size: MOVED's code, never sent again
 *   neverwr    SENT NEVER_WRITTEN (the connection ended with it queued): definite
 *   lost       WRITTEN then LOST: indeterminate, sent once (mutant retry sends it again, unavonbreak answers UNREACH)
 *   timeout    WRITTEN then TIMEOUT: indeterminate, sent once
 *   cancelnw   no outcome by wait_ms, the cancel takes it back: UNREACH
 *   cancelwr   no outcome by wait_ms, the cancel says written: INDET
 *   cancelinv  no outcome by wait_ms, the cancel finds nothing in flight, the outcome follows: it decides
 *   chunks     a read: chunks relayed in order, one credit per chunk taken but the last, tail stats
 *   relayfail  the client's connection goes mid-stream: the owner's stream is canceled, INDET
 *   clientgone written, the client goes while the reply is awaited: canceled, INDET
 *   notail     a last piece without a tail: relayed whole, the client's connection is closed
 *   teardown   the relay fails while the link delivers the frame's next chunk: fwdr_run does not return (and free its
 *              waiter) while that delivery still holds the waiter's lock (mutant teardown: it did, review F1)
 *   late       events of frames nobody waits for (late replies) are dropped and counted
 */
#define _GNU_SOURCE
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "fwd.h"
#include "fwdreq.h"
#include "peerlink.h"

static int failures;
static const char *mutant = "";
#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #c);                                      \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

static int64_t mono_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000 + ts.tv_nsec;
}

/* ---- the fake world */

struct later {
    struct fwdr_tab *tab;
    int delay_ms;
    struct fwd_ev ev;
    uint8_t body[512];
};

struct fake {
    struct fwdr_tab *tab;
    atomic_llong offset_ns;       /* added to the clock */
    int64_t stall_ns;             /* the stall hook adds this each time */
    atomic_int gone;              /* client_gone answers this */
    int64_t gone_after_ns;        /* client_gone turns true this long after the first send (0: never) */
    int64_t first_send_ns;
    int sends, cancels, credits_n;
    uint32_t credits;
    int rc_script[8], nrc;        /* send's answers in order; past the end: FWD_OK */
    int cancel_rc;
    int depth, max_depth;         /* waits */
    uint8_t relayed[4096];
    size_t nrel;
    int relay_fail_at;            /* relay returns false on this call (1-based; 0 never) */
    int relays;
    uint32_t stamped_age;         /* age_ms the message carried at the last send */
    /* what happens on an accepted send */
    void (*on_send)(struct fake *f, uint64_t frame_id, uint32_t peer, int kind);
    void (*on_cancel)(struct fake *f, uint64_t frame_id, uint32_t peer);
    pthread_t th[16];
    int nth;
};

static int64_t f_now(void *a) { return mono_ns() + atomic_load(&((struct fake *)a)->offset_ns); }
static void f_sleep(void *a, int ms)
{
    (void)a;
    usleep((useconds_t)ms * 1000);
}
static void f_waits(void *a, int d)
{
    struct fake *f = a;
    f->depth += d;
    if (f->depth > f->max_depth)
        f->max_depth = f->depth;
}
static void f_stall(void *a)
{
    struct fake *f = a;
    if (f->stall_ns)
        atomic_fetch_add(&f->offset_ns, f->stall_ns);
}
static bool f_gone(void *a)
{
    struct fake *f = a;
    if (f->gone_after_ns && f->first_send_ns && mono_ns() - f->first_send_ns >= f->gone_after_ns)
        atomic_store(&f->gone, 1);
    return atomic_load(&f->gone) != 0;
}
static int f_send(void *a, uint32_t peer, int kind, const void *p, size_t n, uint64_t frame_id, uint64_t deadline_ns)
{
    (void)deadline_ns;
    struct fake *f = a;
    int i = f->sends++;
    if (!f->first_send_ns)
        f->first_send_ns = mono_ns();
    if (n >= FWDR_ENV)
        memcpy(&f->stamped_age, (const uint8_t *)p + FWDR_AGE_AT, 4);
    int rc = i < f->nrc ? f->rc_script[i] : FWD_OK;
    if (rc == FWD_OK && f->on_send)
        f->on_send(f, frame_id, peer, kind);
    return rc;
}
static int f_cancel(void *a, uint32_t peer, uint64_t frame_id)
{
    struct fake *f = a;
    f->cancels++;
    if (f->on_cancel)
        f->on_cancel(f, frame_id, peer);
    return f->cancel_rc;
}
static int f_credit(void *a, uint32_t peer, uint64_t frame_id, uint32_t chunks)
{
    (void)peer, (void)frame_id;
    struct fake *f = a;
    f->credits_n++;
    f->credits += chunks;
    return FWD_OK;
}
static bool f_relay(void *a, const uint8_t *p, size_t n)
{
    struct fake *f = a;
    f->relays++;
    if (f->relay_fail_at && f->relays == f->relay_fail_at)
        return false;
    if (f->nrel + n <= sizeof f->relayed) {
        memcpy(f->relayed + f->nrel, p, n);
        f->nrel += n;
    }
    return true;
}

static void *later_main(void *a)
{
    struct later *l = a;
    if (l->delay_ms)
        usleep((useconds_t)l->delay_ms * 1000);
    l->ev.p = l->body;
    fwdr_on_event(l->tab, &l->ev);
    free(l);
    return NULL;
}

/* an event for frame_id from peer, delivered after delay_ms from another thread (<0: at once, in this thread) */
static void ev_at(struct fake *f, int delay_ms, int type, int kind, uint32_t peer, uint64_t frame_id, int outcome,
                  int last, const void *body, size_t n)
{
    struct later *l = calloc(1, sizeof *l);
    l->tab = f->tab;
    l->delay_ms = delay_ms < 0 ? 0 : delay_ms;
    l->ev = (struct fwd_ev){.type = type, .kind = kind, .peer = peer, .frame_id = frame_id, .outcome = outcome,
                            .last = last, .n = n};
    if (n)
        memcpy(l->body, body, n);
    if (delay_ms < 0 || f->nth >= 16) {
        later_main(l);
        return;
    }
    pthread_create(&f->th[f->nth++], NULL, later_main, l);
}

static size_t with_tail(uint8_t *out, const char *s, uint32_t flags, uint64_t rows)
{
    size_t n = strlen(s);
    memcpy(out, s, n);
    struct fwdr_tail t = {.flags = flags};
    t.st.rows = rows;
    t.st.result_rows = rows / 2;
    t.st.elapsed_s = 0.25;
    t.st.granules = 7;
    fwdr_tail_encode(out + n, &t);
    return n + FWDR_TAIL;
}

static uint64_t next_frame = 1000;

struct run {
    struct fake f;
    struct fwdr_ops o;
    struct fwdr_req q;
    uint8_t msg[FWDR_ENV + FWDR_INNER + 16];
    struct fwdr_tail tail;
    uint64_t hop_ns;
    int res;
    int64_t took_ms;
};

static void run_init(struct run *r, struct fwdr_tab *tab, int kind)
{
    memset(r, 0, sizeof *r);
    r->f.tab = tab;
    r->o = (struct fwdr_ops){.arg = &r->f, .now_ns = f_now, .sleep_ms = f_sleep, .waits = f_waits, .stall = f_stall,
                             .client_gone = f_gone, .send = f_send, .cancel = f_cancel, .credit = f_credit,
                             .relay = f_relay};
    struct fwdr_env e = {.epoch = 3, .hops = 1, .flags = FWDR_F_USER, .origin = 2, .uid = 9, .proto = 15,
                         .vol = 4, .users_applied = 77, .id = 5, .op = 6};
    fwdr_env_encode(r->msg, &e);
    memcpy(r->msg + FWDR_ENV + FWDR_INNER, "payload!", 8);
    r->q = (struct fwdr_req){.peer = 1, .kind = kind, .msg = r->msg, .n = FWDR_ENV + FWDR_INNER + 8,
                             .frame_id = next_frame++, .arrival_ns = mono_ns(), .max_age_ms = 1000,
                             .wait_ms = 100, .reply_ms = 30000};
    r->q.mut_noage = !strcmp(mutant, "noage");
    r->q.mut_retry = !strcmp(mutant, "retry");
    r->q.mut_unavonbreak = !strcmp(mutant, "unavonbreak");
    r->q.mut_teardown = !strcmp(mutant, "teardown");
}

static void run_go(struct run *r)
{
    int64_t t0 = mono_ns();
    r->res = fwdr_run(&r->o, r->f.tab, &r->q, &r->tail, &r->hop_ns);
    r->took_ms = (mono_ns() - t0) / 1000000;
    for (int i = 0; i < r->f.nth; i++)
        pthread_join(r->f.th[i], NULL);
    r->f.nth = 0;
}

/* ---- the cases */

static void t_env(struct fwdr_tab *tab)
{
    (void)tab;
    uint8_t b[FWDR_ENV + FWDR_INNER + 3];
    struct fwdr_env e = {.epoch = 0xdeadbeef, .hops = 1, .flags = FWDR_F_USER, .origin = 7, .uid = 123456,
                         .proto = 15, .age_ms = 999, .vol = 65535, .users_applied = 0x0102030405060708ull,
                         .id = 0xfeedface, .op = 39};
    fwdr_env_encode(b, &e);
    memcpy(b + FWDR_ENV + FWDR_INNER, "xyz", 3);
    struct fwdr_env d;
    CHECK(fwdr_env_decode(b, sizeof b, &d) == 0);
    CHECK(d.epoch == e.epoch && d.hops == 1 && d.flags == FWDR_F_USER && d.origin == 7 && d.uid == 123456);
    CHECK(d.proto == 15 && d.age_ms == 999 && d.vol == 65535 && d.users_applied == e.users_applied);
    CHECK(d.id == e.id && d.op == 39 && d.len == 3 && d.payload == b + FWDR_ENV + FWDR_INNER);
    CHECK(!memcmp(d.payload, "xyz", 3));
    CHECK(fwdr_env_decode(b, FWDR_ENV + FWDR_INNER, &d) == 0 && d.len == 0);
    CHECK(fwdr_env_decode(b, FWDR_ENV + FWDR_INNER - 1, &d) == -1);
    b[0] ^= 1;
    CHECK(fwdr_env_decode(b, sizeof b, &d) == -1);
    b[0] ^= 1;
    e.hops = 0;
    fwdr_env_encode(b, &e);
    CHECK(fwdr_env_decode(b, sizeof b, &d) == -1);
}

static void t_tail(struct fwdr_tab *tab)
{
    (void)tab;
    uint8_t b[300];
    struct fwdr_tail t = {.flags = FWDR_T_STATS | FWDR_T_CLOSE};
    t.st = (tsdb_query_stats){.rows = 1, .rows_decoded = 2, .rows_from_stats = 3, .memtable_rows = 4, .bytes = 5,
                              .bytes_read = 6, .parts = 7, .granules = 8, .granules_from_stats = 9,
                              .result_rows = 10, .elapsed_s = 1.5};
    memset(b, 0xab, 100);
    fwdr_tail_encode(b + 100, &t);
    struct fwdr_tail d;
    memset(&d, 0, sizeof d);
    CHECK(fwdr_tail_decode(b, 100 + FWDR_TAIL, &d) == 0);
    CHECK(d.flags == (FWDR_T_STATS | FWDR_T_CLOSE) && d.st.rows == 1 && d.st.rows_decoded == 2);
    CHECK(d.st.rows_from_stats == 3 && d.st.memtable_rows == 4 && d.st.bytes == 5 && d.st.bytes_read == 6);
    CHECK(d.st.parts == 7 && d.st.granules == 8 && d.st.granules_from_stats == 9 && d.st.result_rows == 10);
    CHECK(d.st.elapsed_s == 1.5);
    CHECK(fwdr_tail_decode(b + 100, FWDR_TAIL, &d) == 0);
    CHECK(fwdr_tail_decode(b + 101, FWDR_TAIL - 1, &d) == -1);
    CHECK(fwdr_tail_decode(b, 100 + FWDR_TAIL - 1, &d) == -1); /* the last 96 bytes are not a tail */
}

static void reply_written(struct fake *f, uint64_t id, uint32_t peer, int kind)
{
    uint8_t b[256];
    size_t n = with_tail(b, "answer", FWDR_T_STATS, 42);
    ev_at(f, 5, FWD_EV_SENT, kind, peer, id, FWD_WRITTEN, 0, NULL, 0);
    ev_at(f, 30, FWD_EV_REPLY, kind, peer, id, 0, 0, b, n);
}

static void t_done(struct fwdr_tab *tab)
{
    struct run r;
    run_init(&r, tab, PL_KIND_FWD);
    r.q.arrival_ns -= 250 * 1000000ll; /* read 250 ms ago */
    r.f.on_send = reply_written;
    run_go(&r);
    CHECK(r.res == FWDR_DONE);
    CHECK(r.f.sends == 1 && r.f.nrel == 6 && !memcmp(r.f.relayed, "answer", 6));
    CHECK(r.tail.flags == FWDR_T_STATS && r.tail.st.rows == 42 && r.tail.st.result_rows == 21);
    CHECK(r.tail.st.granules == 7 && r.tail.st.elapsed_s == 0.25);
    CHECK(r.f.stamped_age >= 250 && r.f.stamped_age < 1000);
    CHECK(r.hop_ns >= 20 * 1000000ull);
    CHECK(r.f.depth == 0 && r.f.max_depth >= 1); /* worker_waits around the wait, and balanced */
    CHECK(r.f.cancels == 0 && r.f.credits_n == 0);
}

static void t_old(struct fwdr_tab *tab)
{
    struct run r;
    run_init(&r, tab, PL_KIND_FWD);
    r.q.arrival_ns -= 1500 * 1000000ll;
    r.f.on_send = reply_written;
    run_go(&r);
    CHECK(r.res == FWDR_OLD);
    CHECK(r.f.sends == 0 && r.f.nrel == 0);
}

static void t_stall(struct fwdr_tab *tab)
{
    struct run r;
    run_init(&r, tab, PL_KIND_FWD);
    r.f.stall_ns = 1200 * 1000000ll; /* the hook just before the send: the entry node stalls 1.2 s there */
    r.f.on_send = reply_written;
    run_go(&r);
    CHECK(r.res == FWDR_OLD);
    CHECK(r.f.sends == 0);
}

static void t_gone(struct fwdr_tab *tab)
{
    struct run r;
    run_init(&r, tab, PL_KIND_FWD);
    atomic_store(&r.f.gone, 1);
    r.f.on_send = reply_written;
    run_go(&r);
    CHECK(r.res == FWDR_GONE);
    CHECK(r.f.sends == 0 && r.f.nrel == 0);
}

static void t_unreach(struct fwdr_tab *tab)
{
    struct run r;
    run_init(&r, tab, PL_KIND_FWD);
    r.q.wait_ms = 80;
    for (int i = 0; i < 8; i++)
        r.f.rc_script[i] = FWD_E_DOWN;
    r.f.nrc = 8;
    run_go(&r);
    CHECK(r.res == FWDR_UNREACH);
    CHECK(r.f.sends >= 3);
    CHECK(r.took_ms >= 70 && r.took_ms < 1000);
    CHECK(r.f.depth == 0 && r.f.max_depth >= 1); /* the retry sleeps count as waiting */
}

static void t_refusedok(struct fwdr_tab *tab)
{
    struct run r;
    run_init(&r, tab, PL_KIND_FWD);
    r.f.rc_script[0] = FWD_E_DOWN;
    r.f.rc_script[1] = FWD_E_FULL;
    r.f.nrc = 2;
    r.f.on_send = reply_written;
    run_go(&r);
    CHECK(r.res == FWDR_DONE);
    CHECK(r.f.sends == 3 && r.f.nrel == 6);
}

static void t_toobig(struct fwdr_tab *tab)
{
    struct run r;
    run_init(&r, tab, PL_KIND_FWD);
    r.f.rc_script[0] = FWD_E_TOOBIG;
    r.f.nrc = 1;
    run_go(&r);
    CHECK(r.res == FWDR_TOOBIG);
    CHECK(r.f.sends == 1);
}

static void sent_never(struct fake *f, uint64_t id, uint32_t peer, int kind)
{
    ev_at(f, 10, FWD_EV_SENT, kind, peer, id, FWD_NEVER_WRITTEN, 0, NULL, 0);
}

static void t_neverwr(struct fwdr_tab *tab)
{
    struct run r;
    run_init(&r, tab, PL_KIND_FWD);
    r.f.on_send = sent_never;
    run_go(&r);
    CHECK(r.res == FWDR_UNREACH);
    CHECK(r.f.sends == 1 && r.f.cancels == 0);
}

static void written_then(struct fake *f, uint64_t id, uint32_t peer, int kind, int end)
{
    ev_at(f, 5, FWD_EV_SENT, kind, peer, id, FWD_WRITTEN, 0, NULL, 0);
    ev_at(f, 20, end, kind, peer, id, 0, 0, NULL, 0);
}
static void written_lost(struct fake *f, uint64_t id, uint32_t peer, int kind)
{
    if (f->sends == 1)
        written_then(f, id, peer, kind, FWD_EV_LOST);
    else
        reply_written(f, id, peer, kind); /* a second send would be answered: the owner applies it twice */
}
static void written_timeout(struct fake *f, uint64_t id, uint32_t peer, int kind)
{
    if (f->sends == 1)
        written_then(f, id, peer, kind, FWD_EV_TIMEOUT);
    else
        reply_written(f, id, peer, kind);
}

static void t_lost(struct fwdr_tab *tab)
{
    struct run r;
    run_init(&r, tab, PL_KIND_FWD);
    r.f.on_send = written_lost;
    run_go(&r);
    CHECK(r.res == FWDR_INDET);
    CHECK(r.f.sends == 1 && r.f.nrel == 0);
    CHECK(r.f.depth == 0);
}

static void t_timeout(struct fwdr_tab *tab)
{
    struct run r;
    run_init(&r, tab, PL_KIND_FWDQ);
    r.f.on_send = written_timeout;
    run_go(&r);
    CHECK(r.res == FWDR_INDET);
    CHECK(r.f.sends == 1 && r.f.nrel == 0);
}

static void t_cancelnw(struct fwdr_tab *tab)
{
    struct run r;
    run_init(&r, tab, PL_KIND_FWD);
    r.q.wait_ms = 60;
    r.f.cancel_rc = FWD_NEVER_WRITTEN; /* no outcome came: still queued at the deadline, taken back */
    run_go(&r);
    CHECK(r.res == FWDR_UNREACH);
    CHECK(r.f.sends == 1 && r.f.cancels == 1);
    CHECK(r.took_ms >= 50 && r.took_ms < 1000);
}

static void t_cancelwr(struct fwdr_tab *tab)
{
    struct run r;
    run_init(&r, tab, PL_KIND_FWD);
    r.q.wait_ms = 60;
    r.f.cancel_rc = FWD_WRITTEN; /* bytes of it went: the owner may apply it */
    run_go(&r);
    CHECK(r.res == FWDR_INDET);
    CHECK(r.f.sends == 1 && r.f.cancels == 1);
}

static void cancel_then_never(struct fake *f, uint64_t id, uint32_t peer)
{
    ev_at(f, 15, FWD_EV_SENT, PL_KIND_FWD, peer, id, FWD_NEVER_WRITTEN, 0, NULL, 0);
}

static void t_cancelinv(struct fwdr_tab *tab)
{
    struct run r;
    run_init(&r, tab, PL_KIND_FWD);
    r.q.wait_ms = 60;
    r.f.cancel_rc = FWD_E_INVALID; /* its outcome was on the way to the table: wait for it */
    r.f.on_cancel = cancel_then_never;
    run_go(&r);
    CHECK(r.res == FWDR_UNREACH);
    CHECK(r.f.cancels == 1);
}

static void chunks3(struct fake *f, uint64_t id, uint32_t peer, int kind)
{
    uint8_t b[256];
    ev_at(f, 5, FWD_EV_SENT, kind, peer, id, FWD_WRITTEN, 0, NULL, 0);
    ev_at(f, 15, FWD_EV_CHUNK, kind, peer, id, 0, 0, "one,", 4);
    ev_at(f, 25, FWD_EV_CHUNK, kind, peer, id, 0, 0, "two,", 4);
    size_t n = with_tail(b, "three", FWDR_T_STATS, 1000);
    ev_at(f, 35, FWD_EV_CHUNK, kind, peer, id, 0, 1, b, n);
}

static void t_chunks(struct fwdr_tab *tab)
{
    struct run r;
    run_init(&r, tab, PL_KIND_FWDQ);
    r.f.on_send = chunks3;
    run_go(&r);
    CHECK(r.res == FWDR_DONE);
    CHECK(r.f.nrel == 13 && !memcmp(r.f.relayed, "one,two,three", 13));
    CHECK(r.f.credits_n == 2 && r.f.credits == 2);
    CHECK(r.tail.st.rows == 1000 && r.tail.st.result_rows == 500);
    CHECK(r.f.depth == 0);
}

static void t_relayfail(struct fwdr_tab *tab)
{
    struct run r;
    run_init(&r, tab, PL_KIND_FWDQ);
    r.f.on_send = chunks3;
    r.f.relay_fail_at = 2;
    run_go(&r);
    CHECK(r.res == FWDR_INDET);
    CHECK(r.f.cancels == 1 && r.f.nrel == 4);
    CHECK(r.f.depth == 0);
}

static void written_only(struct fake *f, uint64_t id, uint32_t peer, int kind)
{
    ev_at(f, 5, FWD_EV_SENT, kind, peer, id, FWD_WRITTEN, 0, NULL, 0);
}

static void t_clientgone(struct fwdr_tab *tab)
{
    struct run r;
    run_init(&r, tab, PL_KIND_FWD);
    r.f.on_send = written_only;
    r.f.gone_after_ns = 200 * 1000000ll;
    run_go(&r);
    CHECK(r.res == FWDR_INDET);
    CHECK(r.f.cancels == 1 && r.f.sends == 1);
    CHECK(r.took_ms < 3000); /* noticed at the next one-second tick */
}

static void notail(struct fake *f, uint64_t id, uint32_t peer, int kind)
{
    ev_at(f, -1, FWD_EV_SENT, kind, peer, id, FWD_WRITTEN, 0, NULL, 0);
    ev_at(f, -1, FWD_EV_REPLY, kind, peer, id, 0, 0, "bare reply", 10);
}

static void t_notail(struct fwdr_tab *tab)
{
    struct run r;
    run_init(&r, tab, PL_KIND_FWD);
    r.f.on_send = notail;
    run_go(&r);
    CHECK(r.res == FWDR_DONE);
    CHECK(r.f.nrel == 10 && (r.tail.flags & FWDR_T_CLOSE));
}

static void t_late(struct fwdr_tab *tab)
{
    uint64_t d0 = fwdr_tab_dropped(tab);
    struct run r;
    run_init(&r, tab, PL_KIND_FWD);
    r.f.on_send = written_lost;
    run_go(&r);
    CHECK(r.res == FWDR_INDET);
    /* the owner's reply comes after the entry gave up: nobody takes it */
    struct fwd_ev ev = {.type = FWD_EV_REPLY, .kind = PL_KIND_FWD, .peer = r.q.peer, .frame_id = r.q.frame_id,
                        .p = (const uint8_t *)"late", .n = 4};
    fwdr_on_event(tab, &ev);
    /* the same frame id from another peer is not this request's */
    ev.peer = 99;
    fwdr_on_event(tab, &ev);
    /* a REQUEST or LINK event is never the table's: ignored, not counted */
    ev.type = FWD_EV_LINK;
    fwdr_on_event(tab, &ev);
    CHECK(fwdr_tab_dropped(tab) == d0 + 2);
}

/* teardown: D (the dispatcher) finds the waiter of the frame's second chunk under the table's lock while R (fwdr_run)
 * relays the first; the relay fails, R cancels (once D found the waiter), lets its waiter's lock go and takes the waiter out of the table, while
 * D, once it has the waiter's lock, keeps it 200 ms. R must not return (its waiter is on its stack) before D let go. */
static struct {
    atomic_int found, canceled, held_done, d_done;
    uint64_t frame;
} td;

static void td_hook(void *arg, const struct fwd_ev *ev, int point)
{
    (void)arg;
    if (ev->frame_id != td.frame || ev->n != 2 || memcmp(ev->p, "c2", 2))
        return;
    if (point == 0) { /* wait for R's cancel, then give R time to reach the table's lock */
        atomic_store(&td.found, 1);
        for (int i = 0; i < 2000 && !atomic_load(&td.canceled); i++)
            usleep(1000);
        usleep(50000);
    } else {
        usleep(200000);
        atomic_store(&td.held_done, 1);
    }
}

static struct fwdr_tab *td_tab;
static void *td_deliver(void *a)
{
    fwdr_on_event(td_tab, a);
    atomic_store(&td.d_done, 1);
    return NULL;
}

static struct fwd_ev td_ev;
static pthread_t td_th;

static void td_send(struct fake *f, uint64_t id, uint32_t peer, int kind)
{
    ev_at(f, -1, FWD_EV_SENT, kind, peer, id, FWD_WRITTEN, 0, NULL, 0);
    ev_at(f, -1, FWD_EV_CHUNK, kind, peer, id, 0, 0, "c1", 2);
    td_ev = (struct fwd_ev){.type = FWD_EV_CHUNK, .kind = kind, .peer = peer, .frame_id = id, .p = (const uint8_t *)"c2",
                            .n = 2};
    pthread_create(&td_th, NULL, td_deliver, &td_ev);
}

static void td_cancel(struct fake *f, uint64_t id, uint32_t peer)
{
    (void)f, (void)id, (void)peer;
    for (int i = 0; i < 2000 && !atomic_load(&td.found); i++) /* D's thread may start late: let it find the waiter */
        usleep(1000);
    atomic_store(&td.canceled, 1);
}

/* R's frame stays above fwdr_run's until D is done, so a mutant run touches stale stack, never a reused one */
static __attribute__((noinline)) int td_run(struct run *r, int *done_at_return)
{
    int res = fwdr_run(&r->o, r->f.tab, &r->q, &r->tail, &r->hop_ns);
    *done_at_return = atomic_load(&td.held_done);
    for (uint64_t i = 0; i < 4000000000ull && !atomic_load(&td.d_done); i++) /* no calls: they would reuse the stack */
        __asm__ volatile("" ::: "memory");
    return res;
}

static void t_teardown(struct fwdr_tab *tab)
{
    for (int rep = 0; rep < 3; rep++) {
        struct run r;
        run_init(&r, tab, PL_KIND_FWDQ);
        atomic_store(&td.found, 0);
        atomic_store(&td.canceled, 0);
        atomic_store(&td.held_done, 0);
        atomic_store(&td.d_done, 0);
        td.frame = r.q.frame_id;
        td_tab = tab;
        r.f.on_send = td_send;
        r.f.on_cancel = td_cancel;
        r.f.relay_fail_at = 1;
        fwdr_tab_test_hook(tab, td_hook, NULL);
        int done_at_return = 0;
        r.res = td_run(&r, &done_at_return);
        pthread_join(td_th, NULL);
        fwdr_tab_test_hook(tab, NULL, NULL);
        if (!done_at_return)
            fprintf(stderr, "teardown: fwdr_run returned while the chunk's delivery still held its waiter\n");
        CHECK(done_at_return);
        CHECK(r.res == FWDR_INDET && r.f.cancels == 1);
        if (!done_at_return)
            break;
    }
}

static const struct {
    const char *name;
    void (*fn)(struct fwdr_tab *);
} cases[] = {
    {"env", t_env},           {"tail", t_tail},           {"done", t_done},           {"old", t_old},
    {"stall", t_stall},       {"gone", t_gone},           {"unreach", t_unreach},     {"refusedok", t_refusedok},
    {"toobig", t_toobig},     {"neverwr", t_neverwr},     {"lost", t_lost},           {"timeout", t_timeout},
    {"cancelnw", t_cancelnw}, {"cancelwr", t_cancelwr},   {"cancelinv", t_cancelinv}, {"chunks", t_chunks},
    {"relayfail", t_relayfail}, {"clientgone", t_clientgone}, {"notail", t_notail},   {"late", t_late},
    {"teardown", t_teardown},
};

int main(int argc, char **argv)
{
    const char *only = NULL;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--only") && i + 1 < argc)
            only = argv[++i];
        else if (!strcmp(argv[i], "--mutant") && i + 1 < argc) {
            mutant = argv[++i];
            if (strcmp(mutant, "noage") && strcmp(mutant, "retry") && strcmp(mutant, "unavonbreak") &&
                strcmp(mutant, "teardown")) {
                fprintf(stderr, "test_fwd: unknown mutant %s\n", mutant);
                return 2;
            }
        } else if (argv[i][0] == '-') {
            fprintf(stderr, "usage: test_fwd [DIR] [--only NAME] [--mutant noage|retry|unavonbreak|teardown]\n");
            return 2;
        }
    }
    struct fwdr_tab *tab = fwdr_tab_new();
    for (size_t i = 0; i < sizeof cases / sizeof cases[0]; i++) {
        if (only && strcmp(only, cases[i].name))
            continue;
        int f0 = failures;
        cases[i].fn(tab);
        printf("%-10s %s\n", cases[i].name, failures == f0 ? "ok" : "FAILED");
    }
    fwdr_tab_free(tab);
    if (failures) {
        fprintf(stderr, "test_fwd: %d check(s) failed\n", failures);
        return 1;
    }
    printf("test_fwd: ok\n");
    return 0;
}
