/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * An in-memory transport pair for peerlink (server/pltransport.h), for test_peerlink: two ends, each direction a
 * bounded ring of bytes, both under one mutex; a pipe per end wakes that end's link thread in its poll when bytes
 * arrive for it, room frees up or the stream ends. It behaves as a TCP connection would (in order, short writes and
 * EAGAIN when the ring is full, the peer's end read as 0 after the bytes before it, EPIPE on a write after the end) and
 * plants what a test asks for: the stream ended after the n-th byte of one direction (a cut mid-frame), one byte of a
 * direction flipped (the frame check must catch it), a cut now. A small ring makes every frame a short write.
 *
 * Two modes. Level (plf_new): a wake on every write, read and end, as a socket's readiness is; one descriptor to poll.
 * Edge (plf_new_edge), the shape of a verbs transport: reads take at most `slot` bytes (one RECV slot each), a
 * reader's pipe is woken only when its ring goes from empty to not empty (or the stream ends), a writer's own room
 * pipe only when its ring goes from full to not full, and poll_fds drains both pipes before it looks (a completion
 * queue drained and re-armed): bytes or room already there are announced by `*ready` alone, never again by a
 * descriptor. Two descriptors to poll. A link thread that sleeps in poll although `*ready` said so stalls there.
 *
 * It is the transport-level twin of rd-verbs' fake (docs/design-rdma.md §8), which fakes the verbs calls under a
 * verbs transport instead; this one checks that peerlink's link thread runs on any transport through the seam.
 */
#ifndef TSDB_PL_FAKE_H
#define TSDB_PL_FAKE_H

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>
#include <unistd.h>

#include "pltransport.h"

struct plf_dir {
    uint8_t *buf;
    size_t head, len;  /* the bytes waiting: buf[head .. head + len) modulo cap */
    uint64_t total;    /* bytes ever taken in this direction */
    uint64_t cut_at;   /* > 0: the stream ends once this many bytes were taken */
    uint64_t flip_at;  /* > 0: the byte at offset flip_at - 1 is flipped */
};

struct plf_pair {
    pthread_mutex_t mu;
    size_t cap;
    struct plf_dir d[2]; /* d[s]: from end s to end 1 - s */
    int closed[2], ended, refs;
    int wr[2], ww[2]; /* each end's wake pipe: its link thread polls wr[s], the other end writes ww[s] */
    size_t slot;      /* edge mode (> 0): the most one read takes */
    int orr[2], ow[2]; /* edge mode: each end's room pipe (its link thread polls orr[s]) */
};

struct plf_end {
    struct pl_transport t;
    struct plf_pair *p;
    int s, want; /* want: what the last poll_fds asked for, which poll_ready reports (a paused link reads nothing) */
};

static void plf_wake_locked(struct plf_pair *p, int s)
{
    if (write(p->ww[s], "", 1) < 0) {
        /* full: a wake is pending already */
    }
}

/* end s may write again (edge mode: its own room pipe) */
static void plf_wake_room_locked(struct plf_pair *p, int s)
{
    if (write(p->slot ? p->ow[s] : p->ww[s], "", 1) < 0) {
        /* full: a wake is pending already */
    }
}

static void plf_drain(int fd)
{
    char b[64];
    while (read(fd, b, sizeof b) > 0) {
    }
}

static void plf_put(struct plf_pair *p)
{
    pthread_mutex_lock(&p->mu);
    int last = --p->refs == 0;
    pthread_mutex_unlock(&p->mu);
    if (!last)
        return;
    for (int s = 0; s < 2; s++) {
        close(p->wr[s]);
        close(p->ww[s]);
        if (p->slot) {
            close(p->orr[s]);
            close(p->ow[s]);
        }
        free(p->d[s].buf);
    }
    pthread_mutex_destroy(&p->mu);
    free(p);
}

static ssize_t plf_read(struct pl_transport *t, void *out, size_t n)
{
    struct plf_end *e = (struct plf_end *)t;
    struct plf_pair *p = e->p;
    struct plf_dir *d = &p->d[1 - e->s];
    pthread_mutex_lock(&p->mu);
    if (d->len == 0) {
        int end = p->ended || p->closed[1 - e->s];
        pthread_mutex_unlock(&p->mu);
        if (end)
            return 0;
        errno = EAGAIN;
        return -1;
    }
    size_t k = n < d->len ? n : d->len;
    if (p->slot && k > p->slot)
        k = p->slot;
    int was_full = d->len == p->cap;
    for (size_t i = 0; i < k; i++)
        ((uint8_t *)out)[i] = d->buf[(d->head + i) % p->cap];
    d->head = (d->head + k) % p->cap;
    d->len -= k;
    if (!p->slot || was_full)
        plf_wake_room_locked(p, 1 - e->s); /* room for the writer */
    pthread_mutex_unlock(&p->mu);
    return (ssize_t)k;
}

static ssize_t plf_writev(struct pl_transport *t, const struct iovec *iov, int n)
{
    struct plf_end *e = (struct plf_end *)t;
    struct plf_pair *p = e->p;
    struct plf_dir *d = &p->d[e->s];
    pthread_mutex_lock(&p->mu);
    if (p->ended || p->closed[1 - e->s] || p->closed[e->s]) {
        pthread_mutex_unlock(&p->mu);
        errno = EPIPE;
        return -1;
    }
    if (d->len == p->cap) {
        pthread_mutex_unlock(&p->mu);
        errno = EAGAIN;
        return -1;
    }
    size_t took = 0;
    int was_empty = d->len == 0;
    for (int v = 0; v < n && d->len < p->cap; v++) {
        const uint8_t *b = iov[v].iov_base;
        for (size_t i = 0; i < iov[v].iov_len && d->len < p->cap; i++) {
            if (d->cut_at && d->total >= d->cut_at)
                break;
            uint8_t x = b[i];
            if (d->flip_at && d->total == d->flip_at - 1)
                x ^= 0x10;
            d->buf[(d->head + d->len) % p->cap] = x;
            d->len++;
            d->total++;
            took++;
        }
    }
    if (d->cut_at && d->total >= d->cut_at) {
        p->ended = 1;
        plf_wake_locked(p, e->s);
        plf_wake_locked(p, 1 - e->s);
    }
    if (!p->slot || (was_empty && took))
        plf_wake_locked(p, 1 - e->s);
    pthread_mutex_unlock(&p->mu);
    if (!took) {
        errno = EPIPE;
        return -1;
    }
    return (ssize_t)took;
}

/* what end s can do now (the mutex held) */
static int plf_state_locked(struct plf_pair *p, int s, int want)
{
    int end = p->ended || p->closed[1 - s];
    int r = 0;
    if (((want & PL_TR_IN) && p->d[1 - s].len > 0) || end)
        r |= PL_TR_IN; /* an end is reported whether or not the link reads (pltransport.h) */
    if ((want & PL_TR_OUT) && (p->d[s].len < p->cap || end))
        r |= PL_TR_OUT;
    return r;
}

static int plf_poll_fds(struct pl_transport *t, int want, struct pollfd *pf, int *ready)
{
    struct plf_end *e = (struct plf_end *)t;
    struct plf_pair *p = e->p;
    pf[0] = (struct pollfd){p->wr[e->s], POLLIN, 0};
    e->want = want;
    if (p->slot) { /* drained before the look: what is there now is said by *ready only */
        pf[1] = (struct pollfd){p->orr[e->s], POLLIN, 0};
        plf_drain(p->wr[e->s]);
        plf_drain(p->orr[e->s]);
    }
    pthread_mutex_lock(&p->mu);
    *ready = plf_state_locked(p, e->s, want);
    pthread_mutex_unlock(&p->mu);
    return p->slot ? 2 : 1;
}

static int plf_poll_ready(struct pl_transport *t, const struct pollfd *pf, int k)
{
    struct plf_end *e = (struct plf_end *)t;
    for (int i = 0; i < k; i++)
        if (pf[i].revents & POLLIN)
            plf_drain(pf[i].fd);
    pthread_mutex_lock(&e->p->mu);
    int r = plf_state_locked(e->p, e->s, e->want);
    pthread_mutex_unlock(&e->p->mu);
    return r;
}

/* the stream ends now, both ways (the bytes already taken are still read) */
static void plf_cut(struct plf_pair *p)
{
    pthread_mutex_lock(&p->mu);
    p->ended = 1;
    plf_wake_locked(p, 0);
    plf_wake_locked(p, 1);
    pthread_mutex_unlock(&p->mu);
}

static void plf_close(struct pl_transport *t)
{
    struct plf_end *e = (struct plf_end *)t;
    struct plf_pair *p = e->p;
    pthread_mutex_lock(&p->mu);
    p->closed[e->s] = 1;
    plf_wake_locked(p, 1 - e->s);
    if (p->slot)
        plf_wake_room_locked(p, 1 - e->s);
    pthread_mutex_unlock(&p->mu);
    free(e);
    plf_put(p);
}

static const struct pl_tr_ops plf_ops = {"fake", plf_read, plf_writev, plf_poll_fds, plf_poll_ready, plf_close};

static void plf_pipe(int *r, int *w)
{
    int pp[2];
    if (pipe(pp) != 0)
        abort();
    for (int i = 0; i < 2; i++) {
        fcntl(pp[i], F_SETFL, fcntl(pp[i], F_GETFL) | O_NONBLOCK);
        fcntl(pp[i], F_SETFD, FD_CLOEXEC);
    }
    *r = pp[0];
    *w = pp[1];
}

/* a pair with rings of `cap` bytes, in edge mode with reads of at most `slot` bytes (0: level mode): *a and *b are
 * its ends (each closed by its owner); the pair stays until both are closed and the caller's reference is let go
 * (plf_put) */
static struct plf_pair *plf_new_edge(size_t cap, size_t slot, struct pl_transport **a, struct pl_transport **b)
{
    struct plf_pair *p = calloc(1, sizeof *p);
    struct plf_end *ea = calloc(1, sizeof *ea), *eb = calloc(1, sizeof *eb);
    if (!p || !ea || !eb)
        abort();
    pthread_mutex_init(&p->mu, NULL);
    p->cap = cap;
    p->slot = slot;
    p->refs = 3;
    for (int s = 0; s < 2; s++) {
        plf_pipe(&p->wr[s], &p->ww[s]);
        if (slot)
            plf_pipe(&p->orr[s], &p->ow[s]);
        p->d[s].buf = malloc(cap);
        if (!p->d[s].buf)
            abort();
    }
    ea->t.ops = eb->t.ops = &plf_ops;
    ea->p = eb->p = p;
    ea->s = 0;
    eb->s = 1;
    *a = &ea->t;
    *b = &eb->t;
    return p;
}

/* level mode */
static struct plf_pair *plf_new(size_t cap, struct pl_transport **a, struct pl_transport **b)
{
    return plf_new_edge(cap, 0, a, b);
}

/* whether end s was closed (the link thread closes its transport last, after on_link(down)) */
static int plf_closed(struct plf_pair *p, int s)
{
    pthread_mutex_lock(&p->mu);
    int c = p->closed[s];
    pthread_mutex_unlock(&p->mu);
    return c;
}

/* plants (before the bytes pass): direction s ends after `at` bytes; its byte at offset `at` - 1 is flipped */
static void plf_set_cut(struct plf_pair *p, int s, uint64_t at)
{
    pthread_mutex_lock(&p->mu);
    p->d[s].cut_at = at;
    pthread_mutex_unlock(&p->mu);
}

static void plf_set_flip(struct plf_pair *p, int s, uint64_t at)
{
    pthread_mutex_lock(&p->mu);
    p->d[s].flip_at = at;
    pthread_mutex_unlock(&p->mu);
}

static uint64_t plf_total(struct plf_pair *p, int s)
{
    pthread_mutex_lock(&p->mu);
    uint64_t v = p->d[s].total;
    pthread_mutex_unlock(&p->mu);
    return v;
}

#endif
