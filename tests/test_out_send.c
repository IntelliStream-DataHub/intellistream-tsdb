/* SPDX-License-Identifier: AGPL-3.0-only */
/* The worker's own send of its replies (out_put, out_send in server/tsdbd.c), on a socket pair whose reader is
 * this test, with no event loop: what a non-blocking send leaves stays at the front of the output, in order, and
 * belongs to the event loop from then on (tx_loop), so the worker's later replies are only appended until the loop
 * has drained the output, instead of each costing a send that the full socket refuses. Once the loop has drained
 * it, the worker sends again itself.
 *
 * It includes server/tsdbd.c, as tests/fuzz/fuzz_frames.c does, to reach its static functions. Usage: test_out_send
 * (no scratch directory needed). */
#define _GNU_SOURCE
#define main tsdbd_main
#include "../server/tsdbd.c"
#undef main

static int failures;
#define CHECK(cond)                                                                                                \
    do {                                                                                                           \
        if (!(cond)) {                                                                                             \
            fprintf(stderr, "%s:%d: FAILED: %s\n", __FILE__, __LINE__, #cond);                                     \
            failures++;                                                                                            \
        }                                                                                                          \
    } while (0)

/* Everything the peer can read now. */
static size_t drain(int fd, uint8_t *to, size_t cap)
{
    size_t got = 0;
    for (;;) {
        ssize_t n = recv(fd, to + got, cap - got, MSG_DONTWAIT);
        if (n > 0) {
            got += (size_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR)
            continue;
        return got;
    }
}

/* Security review m1: the send timeout judges bytes consumed per window, not a change of the socket's queue. A client
 * that takes 1 KB a second changes SIOCOUTQ at every tick and used to hold the connection for as long as it liked;
 * one that takes 100 KB a second keeps it, however long. */
static void send_window(void)
{
    enum { TO = 2, TICK = 1000 };
    for (int rate = 0; rate <= 1; rate++) { /* 0: 1 KB/s, 1: 100 KB/s */
        int64_t t0 = 0, base = -1, acked = 0, now_ms = 0;
        bool cut = false;
        int ticks = 0;
        for (; ticks < 600 && !cut; ticks++) {
            now_ms += TICK;
            acked += rate ? 100 * 1024 : 1024;
            send_window_note(&t0, &base, acked, now_ms, TO);
            cut = send_window_expired(t0, now_ms, TO);
        }
        if (rate == 0)
            CHECK(cut && ticks <= 4); /* the first reading is the baseline: closed at the first window's end */
        else
            CHECK(!cut);
    }
    /* just under and just at the minimum rate over one window */
    int64_t t0 = 0, base = -1;
    send_window_note(&t0, &base, 0, 0, TO);
    send_window_note(&t0, &base, (int64_t)TO * SEND_MIN_RATE - 1, 1999, TO);
    CHECK(send_window_expired(t0, 2000, TO) && base == 0);
    send_window_note(&t0, &base, (int64_t)TO * SEND_MIN_RATE, 1999, TO);
    CHECK(!send_window_expired(t0, 2000, TO) && t0 == 1999 && base == (int64_t)TO * SEND_MIN_RATE);
}

/* Review of the m2 fix (F3): inside a domain that is not UP the deadline of out_wait runs over all the request's
 * waits, not from each one. A client that reads steadily ends every wait (the main thread broadcasts when a whole
 * buffer has gone out) well inside --send-timeout, and was never cut, holding the volume's recovery up for as long
 * as the reply lasted. The "main thread" here empties the output every 0.6 s; the worker, as out_put does, waits
 * again each time, for up to 12 s (--send-timeout 2). Outside a domain that is not UP, or in a new request, the
 * clock starts again. */
typedef struct {
    conn *c;
    atomic_bool stop;
} drainer;

static void *drainer_main(void *arg)
{
    drainer *d = arg;
    while (!atomic_load(&d->stop)) {
        struct timespec ts = {0, 600 * 1000000L};
        nanosleep(&ts, NULL);
        pthread_mutex_lock(&d->c->lock);
        d->c->out.len = 0; /* a whole buffer has gone out */
        pthread_cond_broadcast(&d->c->cv);
        pthread_mutex_unlock(&d->c->lock);
    }
    return NULL;
}

/* waits as a worker does until the connection is closed or `max_ms` have passed; returns the ms it took, -1 if cut not */
static int waits_until_cut(conn *c, int max_ms)
{
    int64_t t0 = mono_ms();
    while (mono_ms() - t0 < max_ms && !atomic_load(&c->closing)) {
        pthread_mutex_lock(&c->lock);
        c->out.len = OUT_CAP + 1; /* the next piece did not fit: wait for space */
        out_wait(c);
        pthread_mutex_unlock(&c->lock);
    }
    return atomic_load(&c->closing) ? (int)(mono_ms() - t0) : -1;
}

static void out_wait_deadline(void)
{
    int sv[2];
#ifdef __APPLE__
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0)
        return;
    tsdb_sock_flags(sv[0], SOCK_CLOEXEC | SOCK_NONBLOCK);
    tsdb_sock_flags(sv[1], SOCK_CLOEXEC);
#else
    if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sv) != 0)
        return;
#endif
    conn *c = calloc(1, sizeof *c);
    c->fd = sv[0];
    pthread_mutex_init(&c->lock, NULL);
    conn_cv_init(&c->cv);
    send_timeout_s = 2;
    workq.base_workers = workq.running_workers = 8; /* worker_waits must not start an extra worker */
    dom.ndoms = 1;
    atomic_store(&dom.vol[1].dom, 0);
    atomic_store(&dom.nonup[0], true);
    atomic_store(&c->dom_v, 1);
    buf_reserve(&c->out, OUT_CAP + 2);
    drainer d = {.c = c};
    pthread_t th;
    CHECK(pthread_create(&th, NULL, drainer_main, &d) == 0);

    int ms = waits_until_cut(c, 12000);
    CHECK(ms >= 1800 && ms < 4500); /* --send-timeout from the first wait, over waits that each end after 0.6 s */

    /* a new request (handle() resets the stamp) and a domain that is back up: no cut */
    atomic_store(&c->closing, false);
    atomic_store(&c->nonup_since, 0);
    atomic_store(&dom.nonup[0], false);
    CHECK(waits_until_cut(c, 4500) == -1);
    CHECK(atomic_load(&c->nonup_since) == 0);

    /* it went down again after the request had waited a while: the clock starts at its first wait inside */
    atomic_store(&dom.nonup[0], true);
    ms = waits_until_cut(c, 12000);
    CHECK(ms >= 1800 && ms < 4500);

    atomic_store(&d.stop, true);
    pthread_join(th, NULL);
    atomic_store(&dom.nonup[0], false);
    dom.ndoms = 0;
    close(sv[0]);
    close(sv[1]);
    free(c->out.p);
    free(c);
}

static void make_frame(uint8_t *dst, uint32_t id, uint8_t op, const uint8_t *payload, size_t plen)
{
    uint32_t flen = (uint32_t)(5 + plen);
    memcpy(dst, &flen, 4);
    memcpy(dst + 4, &id, 4);
    dst[8] = op;
    if (plen)
        memcpy(dst + 9, payload, plen);
}

static void feed_in(conn *c, const uint8_t *src, size_t n)
{
    CHECK(in_reserve(c, n) == 0);
    memcpy(c->in.p + c->in_pad + c->in.len, src, n);
    c->in.len += n;
    in_account(c);
}

/* Zero-copy large-frame (> IN_KEEP) ownership transfer, pipelined small+large and large+small frames,
 * 16-byte payload alignment, and exact req_mem.held / c->q_bytes accounting. */
static void test_parse_zerocopy(void)
{
    size_t held0 = request_memory_now();
    conn *c = conn_alloc();
    CHECK(c != NULL && ((uintptr_t)c & 63u) == 0);
    c->fd = -1;
    atomic_store(&c->authed, true);
    pthread_mutex_init(&c->lock, NULL);
    conn_cv_init(&c->cv);

    /* 1. Large frame (> IN_KEEP) delivered in two pieces: header first triggers IN_HDR_PAD alloc, rest transfers
     *    ownership of c->in.p directly to r with zero copy and 16-byte aligned payload. */
    enum { BIG_PL = (128u << 10) + 13, SMALL_PL = 64 };
    uint8_t *big_pl = malloc(BIG_PL);
    uint8_t *big_fr = malloc(9 + BIG_PL);
    for (size_t i = 0; i < BIG_PL; i++)
        big_pl[i] = (uint8_t)((i * 31 + 7) & 0xff);
    make_frame(big_fr, 42, TSDB_OP_INSERT, big_pl, BIG_PL);

    feed_in(c, big_fr, 256);
    CHECK(parse(c) == 0);
    CHECK(c->q_head == NULL);
    CHECK(c->in_pad == IN_HDR_PAD);
    CHECK(c->in.cap == IN_HDR_PAD + 9 + BIG_PL);
    CHECK(c->in_charged == c->in.cap);
    uint8_t *orig_buf = c->in.p;

    feed_in(c, big_fr + 256, 9 + BIG_PL - 256);
    CHECK(parse(c) == 0);
    CHECK(c->in.p == NULL && c->in.len == 0 && c->in.cap == 0 && c->in_pad == 0 && c->in_charged == 0);
    req *r = c->q_head;
    CHECK(r != NULL && r->next == NULL);
    CHECK(r->id == 42 && r->op == TSDB_OP_INSERT && r->len == BIG_PL);
    CHECK(r->owned_buf == orig_buf && r->owned_cap == IN_HDR_PAD + 9 + BIG_PL);
    CHECK(r->payload == orig_buf + IN_HDR_PAD + 9);
    CHECK(((uintptr_t)r->payload & 15u) == 0);
    CHECK(memcmp(r->payload, big_pl, BIG_PL) == 0);
    CHECK(c->q_bytes == REQ_COST(r));
    CHECK(request_memory_now() == held0 + REQ_COST(r));
    c->q_head = c->q_tail = NULL;
    c->q_bytes = 0;
    req_free(r);
    CHECK(request_memory_now() == held0);

    /* 2. Pipelined small frame + prefix of large frame in one chunk, followed by rest of large frame:
     *    small frame uses single-alloc inline_data, large frame still gets zero-copy ownership transfer! */
    uint8_t small_pl[SMALL_PL];
    uint8_t small_fr[9 + SMALL_PL];
    for (size_t i = 0; i < SMALL_PL; i++)
        small_pl[i] = (uint8_t)(0xa0 + i);
    make_frame(small_fr, 100, TSDB_OP_STATS, small_pl, SMALL_PL);
    make_frame(big_fr, 101, TSDB_OP_INSERT, big_pl, BIG_PL);

    size_t pref = 512;
    uint8_t *combo = malloc(sizeof small_fr + pref);
    memcpy(combo, small_fr, sizeof small_fr);
    memcpy(combo + sizeof small_fr, big_fr, pref);
    feed_in(c, combo, sizeof small_fr + pref);
    free(combo);
    CHECK(parse(c) == 0);
    CHECK(c->q_head != NULL && c->q_head == c->q_tail);
    r = c->q_head;
    CHECK(r->id == 100 && r->len == SMALL_PL && r->owned_buf == NULL && r->payload == r->inline_data);
    CHECK(((uintptr_t)r->payload & 15u) == 0);
    CHECK(memcmp(r->payload, small_pl, SMALL_PL) == 0);
    CHECK(c->in_pad == IN_HDR_PAD && c->in.cap == IN_HDR_PAD + 9 + BIG_PL);
    orig_buf = c->in.p;

    feed_in(c, big_fr + pref, 9 + BIG_PL - pref);
    CHECK(parse(c) == 0);
    req *r2 = r->next;
    CHECK(r2 != NULL && r2 == c->q_tail && r2->next == NULL);
    CHECK(r2->id == 101 && r2->len == BIG_PL && r2->owned_buf == orig_buf);
    CHECK(((uintptr_t)r2->payload & 15u) == 0);
    CHECK(memcmp(r2->payload, big_pl, BIG_PL) == 0);
    CHECK(c->in.p == NULL && c->in.len == 0 && c->in_pad == 0 && c->in_charged == 0);
    CHECK(c->q_bytes == REQ_COST(r) + REQ_COST(r2));
    c->q_head = c->q_tail = NULL;
    c->q_bytes = 0;
    req_free(r);
    req_free(r2);
    CHECK(request_memory_now() == held0);

    /* 3. Large frame + trailing small frame delivered together in one buffer: large frame falls back cleanly to
     *    single-alloc inline_data (both frames parsed, 16-byte aligned, zero leak). */
    size_t both_len = 9 + BIG_PL + sizeof small_fr;
    uint8_t *both = malloc(both_len);
    memcpy(both, big_fr, 9 + BIG_PL);
    memcpy(both + 9 + BIG_PL, small_fr, sizeof small_fr);
    feed_in(c, both, both_len);
    free(both);
    CHECK(parse(c) == 0);
    r = c->q_head;
    CHECK(r != NULL && r->next != NULL && r->next->next == NULL);
    r2 = r->next;
    CHECK(r->id == 101 && r->len == BIG_PL && r->owned_buf == NULL && r->payload == r->inline_data);
    CHECK(((uintptr_t)r->payload & 15u) == 0 && memcmp(r->payload, big_pl, BIG_PL) == 0);
    CHECK(r2->id == 100 && r2->len == SMALL_PL && r2->owned_buf == NULL && r2->payload == r2->inline_data);
    CHECK(((uintptr_t)r2->payload & 15u) == 0 && memcmp(r2->payload, small_pl, SMALL_PL) == 0);
    c->q_head = c->q_tail = NULL;
    c->q_bytes = 0;
    req_free(r);
    req_free(r2);
    in_shrink(c);
    tsdb_free_big(c->in.p, c->in.cap);
    c->in = (buf){0};
    c->in_pad = 0;
    in_account(c);
    CHECK(request_memory_now() == held0);

    pthread_mutex_destroy(&c->lock);
    pthread_cond_destroy(&c->cv);
    free(c);
    free(big_pl);
    free(big_fr);
}

typedef struct {
    conn *c;
    atomic_bool entered;
    atomic_bool woke;
} waiter_ctx;

static void *out_waiter_thread(void *arg)
{
    waiter_ctx *w = arg;
    pthread_mutex_lock(&w->c->lock);
    w->c->out.len = OUT_CAP + 1;
    atomic_store(&w->entered, true);
    out_wait(w->c);
    pthread_mutex_unlock(&w->c->lock);
    atomic_store(&w->woke, true);
    return NULL;
}

/* Verifies c->out_waiters tracks blocked workers and submit_send() wakes them cleanly when draining c->out. */
static void test_out_waiters_wakeup(void)
{
    int sv[2];
#ifdef __APPLE__
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0)
        return;
    tsdb_sock_flags(sv[0], SOCK_CLOEXEC | SOCK_NONBLOCK);
    tsdb_sock_flags(sv[1], SOCK_CLOEXEC);
#else
    if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sv) != 0)
        return;
#endif
    conn *c = conn_alloc();
    c->fd = sv[0];
    pthread_mutex_init(&c->lock, NULL);
    conn_cv_init(&c->cv);
    workq.base_workers = workq.running_workers = 8;
    dom.ndoms = 0;
    CHECK(buf_reserve(&c->out, OUT_CAP + 2) == 0);
    io = IO_EPOLL;

    waiter_ctx w = {.c = c};
    pthread_t th;
    CHECK(pthread_create(&th, NULL, out_waiter_thread, &w) == 0);
    /* Wait until the worker is actually inside out_wait (out_waiters == 1). */
    for (int i = 0; i < 200; i++) {
        pthread_mutex_lock(&c->lock);
        size_t nw = c->out_waiters;
        pthread_mutex_unlock(&c->lock);
        if (nw == 1)
            break;
        struct timespec ts = {0, 5 * 1000000L};
        nanosleep(&ts, NULL);
    }
    pthread_mutex_lock(&c->lock);
    CHECK(c->out_waiters == 1);
    pthread_mutex_unlock(&c->lock);
    CHECK(!atomic_load(&w.woke));

    /* submit_send drains c->out into c->sending and must broadcast c->cv because out_waiters > 0. */
    CHECK(submit_send(c));
    pthread_join(th, NULL);
    CHECK(atomic_load(&w.woke));
    pthread_mutex_lock(&c->lock);
    CHECK(c->out_waiters == 0);
    CHECK(!atomic_load(&c->out_pending));
    pthread_mutex_unlock(&c->lock);

    /* Clear epoll ready list entry left by submit_send. */
    ready_head = ready_tail = NULL;
    nready = 0;
    free(c->out.p);
    free(c->sending.p);
    pthread_mutex_destroy(&c->lock);
    pthread_cond_destroy(&c->cv);
    free(c);
    close(sv[0]);
    close(sv[1]);
}

int main(void)
{
    send_window();
    out_wait_deadline();
    test_parse_zerocopy();
    test_out_waiters_wakeup();
    enum { BIG = 256 << 10, MORE = 1024, SMALL = 100 };
    uint8_t *data = malloc(BIG + MORE + SMALL), *got = malloc(BIG + MORE + SMALL);
    for (size_t i = 0; i < BIG + MORE + SMALL; i++)
        data[i] = (uint8_t)(i * 7 + (i >> 8));
    int sv[2];
#ifdef __APPLE__ /* compat.h's SOCK_CLOEXEC is not the kernel's; and tsdbd's sockets are non-blocking there */
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) {
        perror("socketpair");
        return 1;
    }
    tsdb_sock_flags(sv[0], SOCK_CLOEXEC | SOCK_NONBLOCK);
    tsdb_sock_flags(sv[1], SOCK_CLOEXEC);
#else
    if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sv) != 0) {
        perror("socketpair");
        return 1;
    }
#endif
    int small = 4096;
    setsockopt(sv[0], SOL_SOCKET, SO_SNDBUF, &small, sizeof small);
    setsockopt(sv[1], SOL_SOCKET, SO_RCVBUF, &small, sizeof small);

    conn *c = calloc(1, sizeof *c);
    c->fd = sv[0];
    c->n_out = (note){NULL, c, false};
    c->n_done = (note){NULL, c, true};
    pthread_mutex_init(&c->lock, NULL);
    conn_cv_init(&c->cv);

    /* A reply larger than the socket takes: the worker sends what fits and hands the rest to the loop. */
    struct iovec v = {data, BIG};
    CHECK(out_put(c, &v, 1));
    size_t first = drain(sv[1], got, BIG + MORE + SMALL);
    CHECK(first > 0 && first < BIG);
    CHECK(c->out.len == BIG - first);
    CHECK(memcmp(got, data, first) == 0);
    CHECK(c->out.len <= BIG && memcmp(c->out.p, data + first, c->out.len) == 0); /* the rest, moved to the front */
    CHECK(!c->tx_worker);
    CHECK(c->tx_loop);
    CHECK(c->notified);
    CHECK(atomic_load(&c->out_pending));
    CHECK(atomic_load(&note_stack.top) == &c->n_out && atomic_load(&c->out_queued));

    /* The socket has room again, but the loop owns the output: the next reply is appended, not sent. */
    v = (struct iovec){data + BIG, MORE};
    CHECK(out_put(c, &v, 1));
    CHECK(drain(sv[1], got, BIG + MORE + SMALL) == 0);
    CHECK(c->out.len == BIG + MORE - first);
    CHECK(memcmp(c->out.p, data + first, c->out.len) == 0);
    CHECK(c->tx_loop && !c->tx_worker && atomic_load(&c->out_pending));

    /* The loop drained everything (as submit_send leaves a connection whose output it sent): the worker sends again. */
    size_t rest = c->out.len, all = first;
    for (size_t at = 0; at < rest;) {
        ssize_t n = send(sv[0], c->out.p + at, rest - at, MSG_NOSIGNAL | MSG_DONTWAIT);
        if (n > 0)
            at += (size_t)n;
        else if (n == 0 || (errno != EAGAIN && errno != EINTR)) {
            perror("send");
            return 1;
        }
        all += drain(sv[1], got + all, BIG + MORE + SMALL - all);
    }
    pthread_mutex_lock(&c->lock);
    c->out.len = 0;
    c->tx_loop = c->notified = false;
    atomic_store(&c->out_pending, false);
    pthread_mutex_unlock(&c->lock);
    all += drain(sv[1], got + all, BIG + MORE + SMALL - all);
    CHECK(all == BIG + MORE);
    v = (struct iovec){data + BIG + MORE, SMALL};
    CHECK(out_put(c, &v, 1));
    CHECK(c->out.len == 0 && !c->tx_loop && !c->tx_worker && !c->notified && !atomic_load(&c->out_pending));
    all += drain(sv[1], got + all, BIG + MORE + SMALL - all);
    CHECK(all == BIG + MORE + SMALL);
    CHECK(memcmp(got, data, all) == 0);

    /* The only QUERY_MULTI streaming hands a full batch to the loop, which sends it while the worker computes the
     * next series (one connection lost 28% of its windows/s when the worker sent each batch itself): nothing leaves
     * from the worker's thread, though the socket has room. A smaller batch costs less to send than a wakeup of the
     * loop: the worker sends that. */
    atomic_store(&note_stack.top, NULL);
    atomic_store(&c->out_queued, false);
    c->streaming = true;
    atomic_store(&streaming_conns, 1);
    v = (struct iovec){data, SMALL};
    CHECK(out_put(c, &v, 1));
    CHECK(c->out.len == 0 && !c->tx_loop && !c->tx_worker && !c->notified && !atomic_load(&c->out_pending));
    CHECK(drain(sv[1], got, BIG + MORE + SMALL) == SMALL && memcmp(got, data, SMALL) == 0);
    v = (struct iovec){data, HANDOFF_MIN};
    CHECK(out_put(c, &v, 1));
    CHECK(drain(sv[1], got, BIG + MORE + SMALL) == 0);
    CHECK(c->out.len == HANDOFF_MIN && memcmp(c->out.p, data, HANDOFF_MIN) == 0);
    CHECK(c->tx_loop && !c->tx_worker && c->notified && atomic_load(&c->out_pending));
    CHECK(atomic_load(&note_stack.top) == &c->n_out && atomic_load(&c->out_queued));
    /* the loop sent it (here: dropped) and drained the output */
    pthread_mutex_lock(&c->lock);
    c->out.len = 0;
    c->tx_loop = c->notified = false;
    atomic_store(&c->out_pending, false);
    pthread_mutex_unlock(&c->lock);
    atomic_store(&note_stack.top, NULL);
    atomic_store(&c->out_queued, false);
    /* With a second connection streaming, one loop thread would send for all of them: the worker sends again. */
    atomic_store(&streaming_conns, 2);
    v = (struct iovec){data + SMALL, SMALL};
    CHECK(out_put(c, &v, 1));
    CHECK(c->out.len == 0 && !c->tx_loop && !c->tx_worker && !c->notified);
    CHECK(drain(sv[1], got, BIG + MORE + SMALL) == SMALL && memcmp(got, data + SMALL, SMALL) == 0);
    /* and so does a connection that is not the one streaming (a single reply, or the end of a QUERY_MULTI) */
    c->streaming = false;
    atomic_store(&streaming_conns, 1);
    v = (struct iovec){data + 2 * SMALL, SMALL};
    CHECK(out_put(c, &v, 1));
    CHECK(c->out.len == 0 && !c->tx_loop && !c->tx_worker && !c->notified);
    CHECK(drain(sv[1], got, BIG + MORE + SMALL) == SMALL && memcmp(got, data + 2 * SMALL, SMALL) == 0);
    CHECK(atomic_load(&note_stack.top) == NULL);
    atomic_store(&streaming_conns, 0);

    atomic_store(&note_stack.top, NULL);
    free(c->out.p);
    pthread_mutex_destroy(&c->lock);
    pthread_cond_destroy(&c->cv);
    free(c);
    close(sv[0]);
    close(sv[1]);
    free(data);
    free(got);
    if (failures)
        fprintf(stderr, "test_out_send: %d FAILED\n", failures);
    else
        printf("test_out_send: OK\n");
    return failures != 0;
}
