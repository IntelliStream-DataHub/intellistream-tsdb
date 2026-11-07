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

int main(void)
{
    send_window();
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
    pthread_cond_init(&c->cv, NULL);

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
    CHECK(atomic_load(&note_top) == &c->n_out && atomic_load(&c->out_queued));

    /* The socket has room again, but the loop owns the output: the next reply is appended, not sent. */
    v = (struct iovec){data + BIG, MORE};
    CHECK(out_put(c, &v, 1));
    CHECK(drain(sv[1], got, BIG + MORE + SMALL) == 0);
    CHECK(c->out.len == BIG + MORE - first);
    CHECK(memcmp(c->out.p, data + first, c->out.len) == 0);
    CHECK(c->tx_loop && !c->tx_worker);

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
    pthread_mutex_unlock(&c->lock);
    all += drain(sv[1], got + all, BIG + MORE + SMALL - all);
    CHECK(all == BIG + MORE);
    v = (struct iovec){data + BIG + MORE, SMALL};
    CHECK(out_put(c, &v, 1));
    CHECK(c->out.len == 0 && !c->tx_loop && !c->tx_worker && !c->notified);
    all += drain(sv[1], got + all, BIG + MORE + SMALL - all);
    CHECK(all == BIG + MORE + SMALL);
    CHECK(memcmp(got, data, all) == 0);

    /* The only QUERY_MULTI streaming hands a full batch to the loop, which sends it while the worker computes the
     * next series (one connection lost 28% of its windows/s when the worker sent each batch itself): nothing leaves
     * from the worker's thread, though the socket has room. A smaller batch costs less to send than a wakeup of the
     * loop: the worker sends that. */
    atomic_store(&note_top, NULL);
    atomic_store(&c->out_queued, false);
    c->streaming = true;
    atomic_store(&streaming_conns, 1);
    v = (struct iovec){data, SMALL};
    CHECK(out_put(c, &v, 1));
    CHECK(c->out.len == 0 && !c->tx_loop && !c->tx_worker && !c->notified);
    CHECK(drain(sv[1], got, BIG + MORE + SMALL) == SMALL && memcmp(got, data, SMALL) == 0);
    v = (struct iovec){data, HANDOFF_MIN};
    CHECK(out_put(c, &v, 1));
    CHECK(drain(sv[1], got, BIG + MORE + SMALL) == 0);
    CHECK(c->out.len == HANDOFF_MIN && memcmp(c->out.p, data, HANDOFF_MIN) == 0);
    CHECK(c->tx_loop && !c->tx_worker && c->notified);
    CHECK(atomic_load(&note_top) == &c->n_out && atomic_load(&c->out_queued));
    /* the loop sent it (here: dropped) and drained the output */
    pthread_mutex_lock(&c->lock);
    c->out.len = 0;
    c->tx_loop = c->notified = false;
    pthread_mutex_unlock(&c->lock);
    atomic_store(&note_top, NULL);
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
    CHECK(atomic_load(&note_top) == NULL);
    atomic_store(&streaming_conns, 0);

    atomic_store(&note_top, NULL);
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
