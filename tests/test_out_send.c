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

int main(void)
{
    enum { BIG = 256 << 10, MORE = 1024, SMALL = 100 };
    uint8_t *data = malloc(BIG + MORE + SMALL), *got = malloc(BIG + MORE + SMALL);
    for (size_t i = 0; i < BIG + MORE + SMALL; i++)
        data[i] = (uint8_t)(i * 7 + (i >> 8));
    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sv) != 0) {
        perror("socketpair");
        return 1;
    }
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
