/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The verbs transport under peerlink: the pool, the connections and their struct pl_transport. The design is in
 * plrdma.h; this file calls the verbs only through struct plr_ops (server/plrdma_dl.c for libibverbs, tests/plr_fake.h
 * for the tests), so it compiles on every platform and links no RDMA library.
 */
#include "plrdma.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

int plrdma_mutant;

enum { MSG_DATA = 1, MSG_CREDIT = 2 };
enum { WR_RECV = 1, WR_SEND = 2, WR_MARK = 3 };
#define WR(kind, i) (((uint64_t)(kind) << 32) | (uint32_t)(i))
#define MARK_SEND 1u
#define MARK_RECV 2u

struct plr_pool {
    const struct plr_ops *ops;
    struct plr_pool_cfg cfg;
    struct plr_dev *dev;
    struct plr_port port;
    uint8_t *base;
    size_t bytes;
    struct plr_mr *mr;
    uint32_t lkey;
    pthread_mutex_t mu; /* a leaf: the free list and the counters */
    uint32_t *free, nfree, nslots, lost, conns;
    void (*on_free)(void *arg, const void *slot, size_t n);
    void *on_free_arg;
};

struct rslot {
    uint32_t len, off; /* a received DATA message: its payload, and how much of it the link read */
};

struct plr_conn {
    struct pl_transport t;
    struct plr_pool *pool;
    const struct plr_ops *ops;
    struct plr_qp *qp;
    uint32_t qpn, psn;
    uint16_t R, D, K, S; /* receive slots (D data + K control), send slots */
    uint32_t *slots;     /* pool slot numbers: [0, R) receive, [R, R + S) send */
    struct plr_ep peer;
    uint32_t room; /* the most stream bytes one message carries */
    int started, have_slots;
    /* receive: the DATA messages arrived and not read whole, in order */
    struct rslot *rs;
    uint16_t *rq;
    uint16_t rq_head, rq_n;
    int posted_rx;
    /* send: the slots in flight, a ring from s_head */
    uint16_t s_head, s_n, unsig, sig_every;
    uint32_t *s_len;
    int cr_data, cr_ctl, owed_data, owed_ctl;
    int err, ended, want, ak;
    unsigned marks;
    struct pl_transport *anchor;
    uint64_t ahead, done_unrep; /* PLR_MUT_WRITTENCQE */
    struct plr_conn_stats st;
};

static uint8_t *slot_at(const struct plr_conn *c, uint32_t i)
{
    return c->pool->base + (size_t)c->slots[i] * c->pool->cfg.slot;
}

static void put16(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)v;
    p[1] = (uint8_t)(v >> 8);
}
static void put32(uint8_t *p, uint32_t v)
{
    for (int i = 0; i < 4; i++)
        p[i] = (uint8_t)(v >> (8 * i));
}
static uint16_t get16(const uint8_t *p) { return (uint16_t)(p[0] | p[1] << 8); }
static uint32_t get32(const uint8_t *p)
{
    return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24;
}

/* ------------------------------------------------------------------------------------------------- the pool */

struct plr_pool *plr_pool_open(const struct plr_ops *ops, const struct plr_pool_cfg *cfg, char *err, size_t errlen)
{
    if (!ops) {
        snprintf(err, errlen, "RDMA: no verbs");
        errno = ENOSYS;
        return NULL;
    }
    if (cfg->slot < 1024 || cfg->slot % 64 || cfg->data_slots < 1 || cfg->ctl_slots < 1 || cfg->send_slots < 2 ||
        cfg->pool_bytes < (size_t)cfg->slot * (cfg->data_slots + cfg->ctl_slots + cfg->send_slots)) {
        snprintf(err, errlen, "RDMA: a pool of %zu bytes cannot hold one connection's slots (%u of %u bytes)",
                 cfg->pool_bytes, (unsigned)(cfg->data_slots + cfg->ctl_slots + cfg->send_slots), (unsigned)cfg->slot);
        errno = EINVAL;
        return NULL;
    }
    size_t bytes = cfg->pool_bytes / cfg->slot * cfg->slot;
    struct rlimit rl;
    if (getrlimit(RLIMIT_MEMLOCK, &rl) == 0 && rl.rlim_cur != RLIM_INFINITY && (uint64_t)rl.rlim_cur < bytes) {
        snprintf(err, errlen,
                 "RDMA: the locked-memory limit (RLIMIT_MEMLOCK, %" PRIu64 " KiB) is below the registered pool's %zu "
                 "KiB: raise LimitMEMLOCK or lower cluster.peer.rdma.pool_mb",
                 (uint64_t)rl.rlim_cur / 1024, bytes / 1024);
        errno = ENOMEM;
        return NULL;
    }
    struct plr_pool *p = calloc(1, sizeof *p);
    if (!p) {
        snprintf(err, errlen, "RDMA: out of memory");
        return NULL;
    }
    p->ops = ops;
    p->cfg = *cfg;
    p->bytes = bytes;
    p->nslots = (uint32_t)(bytes / cfg->slot);
    pthread_mutex_init(&p->mu, NULL);
    if (ops->dev_open(cfg->dev, cfg->port, &p->dev, &p->port, err, errlen) < 0) {
        pthread_mutex_destroy(&p->mu);
        free(p);
        return NULL;
    }
    p->base = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    p->free = malloc(sizeof *p->free * p->nslots);
    if (p->base == MAP_FAILED || !p->free) {
        snprintf(err, errlen, "RDMA: cannot map a pool of %zu bytes", bytes);
        goto fail;
    }
    p->mr = ops->reg(p->dev, p->base, bytes, &p->lkey);
    if (!p->mr) {
        snprintf(err, errlen, "RDMA: registering the pool of %zu KiB failed: %s", bytes / 1024, strerror(errno));
        goto fail;
    }
    for (uint32_t i = 0; i < p->nslots; i++)
        p->free[i] = p->nslots - 1 - i;
    p->nfree = p->nslots;
    return p;
fail:;
    int e = errno ? errno : ENOMEM;
    if (p->base != MAP_FAILED && p->base)
        munmap(p->base, bytes);
    free(p->free);
    ops->dev_close(p->dev);
    pthread_mutex_destroy(&p->mu);
    free(p);
    errno = e;
    return NULL;
}

void plr_pool_close(struct plr_pool *p)
{
    if (!p)
        return;
    if (p->conns)
        fprintf(stderr, "peerlink: rdma: the pool closed with %u connections open\n", (unsigned)p->conns);
    p->ops->dereg(p->mr);
    p->ops->dev_close(p->dev);
    munmap(p->base, p->bytes);
    free(p->free);
    pthread_mutex_destroy(&p->mu);
    free(p);
}

void plr_pool_stats(struct plr_pool *p, struct plr_pool_stats *st)
{
    pthread_mutex_lock(&p->mu);
    *st = (struct plr_pool_stats){p->nslots, p->nfree, p->lost, p->conns};
    pthread_mutex_unlock(&p->mu);
}

const struct plr_port *plr_pool_port(const struct plr_pool *p) { return &p->port; }

void plr_pool_test_on_free(struct plr_pool *p, void (*fn)(void *arg, const void *slot, size_t n), void *arg)
{
    p->on_free = fn;
    p->on_free_arg = arg;
}

static int pool_take(struct plr_pool *p, uint32_t *out, uint32_t n)
{
    pthread_mutex_lock(&p->mu);
    int ok = p->nfree >= n;
    if (ok) {
        for (uint32_t i = 0; i < n; i++)
            out[i] = p->free[--p->nfree];
        p->conns++;
    }
    pthread_mutex_unlock(&p->mu);
    return ok ? 0 : -1;
}

/* the slots of a connection whose queue pair is gone (lost: a drain that did not finish keeps them out for good) */
static void pool_give(struct plr_pool *p, const uint32_t *s, uint32_t n, int lost)
{
    if (!lost && p->on_free)
        for (uint32_t i = 0; i < n; i++)
            p->on_free(p->on_free_arg, p->base + (size_t)s[i] * p->cfg.slot, p->cfg.slot);
    pthread_mutex_lock(&p->mu);
    if (lost)
        p->lost += n;
    else
        for (uint32_t i = 0; i < n; i++)
            p->free[p->nfree++] = s[i];
    p->conns--;
    pthread_mutex_unlock(&p->mu);
}

/* ------------------------------------------------------------------------------------------------- endpoints */

void plr_ep_encode(const struct plr_ep *e, uint8_t out[PLR_EP_LEN])
{
    memset(out, 0, PLR_EP_LEN);
    out[0] = 1; /* the layout's version */
    out[1] = e->mtu;
    out[2] = e->link_layer;
    out[3] = e->port;
    put16(out + 4, e->lid);
    memcpy(out + 6, e->gid, 16);
    put32(out + 22, e->qpn);
    put32(out + 26, e->psn);
    put32(out + 30, e->slot);
    put16(out + 34, e->data);
    put16(out + 36, e->ctl);
}

int plr_ep_decode(struct plr_ep *e, const uint8_t *in, size_t n)
{
    if (n < PLR_EP_LEN || in[0] != 1)
        goto bad;
    e->mtu = in[1];
    e->link_layer = in[2];
    e->port = in[3];
    e->lid = get16(in + 4);
    memcpy(e->gid, in + 6, 16);
    e->qpn = get32(in + 22);
    e->psn = get32(in + 26);
    e->slot = get32(in + 30);
    e->data = get16(in + 34);
    e->ctl = get16(in + 36);
    if (e->mtu < 1 || e->mtu > 5 || (e->link_layer != PLR_LL_IB && e->link_layer != PLR_LL_ETH) ||
        e->qpn >= 1u << 24 || e->psn >= 1u << 24 || e->slot < 1024 || e->slot > (1u << 24) || !e->data || !e->ctl ||
        e->ctl > 255)
        goto bad;
    return 0;
bad:
    errno = EPROTO;
    return -1;
}

/* ------------------------------------------------------------------------------------------------- connections */

static void fail(struct plr_conn *c, int e)
{
    if (!c->err)
        c->err = e ? e : EIO;
}

static void repost(struct plr_conn *c, uint32_t i)
{
    if (c->ops->post_recv(c->qp, WR(WR_RECV, i), slot_at(c, i), c->pool->cfg.slot, c->pool->lkey) < 0) {
        fail(c, errno);
        return;
    }
    c->posted_rx++;
}

static void complete(struct plr_conn *c, const struct plr_wc *w, int closing)
{
    unsigned kind = (unsigned)(w->wr_id >> 32);
    uint32_t i = (uint32_t)w->wr_id;
    if (kind == WR_MARK) {
        c->marks |= i;
        return;
    }
    if (kind == WR_SEND && i < c->S) {
        uint32_t k = (uint32_t)((i + c->S - c->s_head) % c->S) + 1;
        if (k > c->s_n) { /* a completion for a slot not in flight: the provider and this code disagree */
            fail(c, EPROTO);
            return;
        }
        for (uint32_t j = 0; j < k && w->status == PLR_WC_OK; j++)
            c->done_unrep += c->s_len[(c->s_head + j) % c->S];
        c->s_head = (uint16_t)((c->s_head + k) % c->S);
        c->s_n = (uint16_t)(c->s_n - k);
        if (w->status != PLR_WC_OK && !closing && !c->err) {
            fprintf(stderr, "peerlink: rdma: a send completed with status %d (provider %d): connection ended\n",
                    w->status, w->vendor);
            fail(c, w->status == PLR_WC_FLUSH ? ECONNRESET : EIO);
        }
        return;
    }
    if (kind != WR_RECV || i >= c->R) {
        fail(c, EPROTO);
        return;
    }
    c->posted_rx--;
    if (closing)
        return;
    if (w->status != PLR_WC_OK) {
        if (!c->err)
            fprintf(stderr, "peerlink: rdma: a receive completed with status %d (provider %d): connection ended\n",
                    w->status, w->vendor);
        fail(c, w->status == PLR_WC_FLUSH ? ECONNRESET : EIO);
        return;
    }
    const uint8_t *h = slot_at(c, i);
    uint32_t len = get32(h + 4);
    if (w->byte_len < PLR_HDR || len != w->byte_len - PLR_HDR || len > c->pool->cfg.slot - PLR_HDR ||
        (h[0] == MSG_DATA ? len == 0 : h[0] == MSG_CREDIT ? len != 0 : 1)) {
        fprintf(stderr, "peerlink: rdma: a message of type %u and %u bytes does not parse: connection ended\n",
                (unsigned)h[0], (unsigned)w->byte_len);
        fail(c, EPROTO);
        return;
    }
    c->cr_ctl += h[1];
    c->cr_data += get16(h + 2);
    if (c->cr_data > c->peer.data || c->cr_ctl > c->peer.ctl) {
        fprintf(stderr, "peerlink: rdma: the peer returned more credits than it has slots: connection ended\n");
        fail(c, EPROTO);
        return;
    }
    c->st.msgs_in++;
    if (h[0] == MSG_CREDIT) {
        c->st.credit_in++;
        if (plrdma_mutant != PLR_MUT_PAUSECTL) {
            repost(c, i); /* at once, paused or not: control credits are never withheld */
            c->owed_ctl++;
            return;
        }
        len = 0; /* the mutant: queued as an empty slot, posted again when the link reads */
    }
    c->rs[i] = (struct rslot){len, 0};
    c->rq[(c->rq_head + c->rq_n) % c->R] = (uint16_t)i;
    c->rq_n++;
}

static void pump(struct plr_conn *c, int closing)
{
    struct plr_wc wc[16];
    int n;
    while ((n = c->ops->poll(c->qp, wc, 16)) > 0)
        for (int j = 0; j < n; j++)
            complete(c, &wc[j], closing);
    if (n < 0 && !closing)
        fail(c, errno);
}

/* one message out on the next send slot; the credit is the caller's */
static int send_msg(struct plr_conn *c, int type, uint32_t len, uint32_t stream)
{
    uint16_t si = (uint16_t)((c->s_head + c->s_n) % c->S);
    uint8_t *b = slot_at(c, c->R + si);
    int ctl = c->owed_ctl > 255 ? 255 : c->owed_ctl, data = c->owed_data > 65535 ? 65535 : c->owed_data;
    b[0] = (uint8_t)type;
    b[1] = (uint8_t)ctl;
    put16(b + 2, (uint16_t)data);
    put32(b + 4, len);
    int sig = plrdma_mutant == PLR_MUT_WRITTENCQE || ++c->unsig >= c->sig_every || c->s_n + 1 == c->S;
    if (c->ops->post_send(c->qp, WR(WR_SEND, si), b, PLR_HDR + len, c->pool->lkey, sig) < 0) {
        fail(c, errno);
        return -1;
    }
    c->owed_ctl -= ctl;
    c->owed_data -= data;
    if (sig)
        c->unsig = 0;
    c->s_len[si] = stream;
    if (!(plrdma_mutant == PLR_MUT_UNSIGEARLY && !sig))
        c->s_n++; /* the mutant: the next message takes the same slot while this one may still be read by the NIC */
    c->st.msgs_out++;
    return 0;
}

static void maybe_credit(struct plr_conn *c)
{
    if (c->err || c->ended || !c->started)
        return;
    int dthr = c->D / 4 ? c->D / 4 : 1, cthr = plrdma_mutant == PLR_MUT_CTLSTORM ? 1 : (c->K / 2 ? c->K / 2 : 1);
    if (c->owed_data < dthr && c->owed_ctl < cthr)
        return;
    int *cr = plrdma_mutant == PLR_MUT_NOCTL ? &c->cr_data : &c->cr_ctl;
    if (*cr <= 0 || c->s_n >= c->S)
        return;
    if (send_msg(c, MSG_CREDIT, 0, 0) == 0) {
        (*cr)--;
        c->st.credit_out++;
    }
}

static int can_write(const struct plr_conn *c)
{
    if (plrdma_mutant == PLR_MUT_WRITTENCQE && c->done_unrep)
        return 1;
    return (c->cr_data > 0 || plrdma_mutant == PLR_MUT_NOCREDIT) && c->s_n < c->S;
}

static int ready_mask(const struct plr_conn *c, int want)
{
    int r = 0;
    if (c->err || c->ended || ((want & PL_TR_IN) && c->rq_n))
        r |= PL_TR_IN;
    if ((want & PL_TR_OUT) && can_write(c))
        r |= PL_TR_OUT;
    return r;
}

static ssize_t rd_read(struct pl_transport *t, void *out, size_t n)
{
    struct plr_conn *c = (struct plr_conn *)t;
    if (!c->rq_n && !c->err)
        pump(c, 0);
    size_t got = 0;
    while (got < n && c->rq_n) {
        uint16_t i = c->rq[c->rq_head];
        struct rslot *s = &c->rs[i];
        size_t k = s->len - s->off;
        if (k > n - got)
            k = n - got;
        memcpy((uint8_t *)out + got, slot_at(c, i) + PLR_HDR + s->off, k);
        s->off += (uint32_t)k;
        got += k;
        if (s->off == s->len) {
            int was_ctl = s->len == 0; /* only PLR_MUT_PAUSECTL queues a CREDIT's slot */
            c->rq_head = (uint16_t)((c->rq_head + 1) % c->R);
            c->rq_n--;
            if (!c->err)
                repost(c, i);
            if (was_ctl)
                c->owed_ctl++;
            else
                c->owed_data++;
        }
    }
    if (got) {
        c->st.bytes_in += got;
        return (ssize_t)got;
    }
    if (c->err) {
        errno = c->err;
        return -1;
    }
    if (c->ended)
        return 0;
    errno = EAGAIN;
    return -1;
}

static ssize_t rd_writev(struct pl_transport *t, const struct iovec *iov, int n)
{
    struct plr_conn *c = (struct plr_conn *)t;
    if (c->err || c->ended) {
        errno = c->err ? c->err : EPIPE;
        return -1;
    }
    if (c->cr_data <= 0 || c->s_n >= c->S)
        pump(c, 0);
    size_t total = 0;
    for (int i = 0; i < n; i++)
        total += iov[i].iov_len;
    size_t pos = plrdma_mutant == PLR_MUT_WRITTENCQE ? c->ahead : 0, posted = 0;
    int vi = 0;
    size_t vo = pos; /* the position in iov of the next byte */
    while (vi < n && vo >= iov[vi].iov_len) {
        vo -= iov[vi].iov_len;
        vi++;
    }
    while (pos < total && !c->err) {
        if ((c->cr_data <= 0 && plrdma_mutant != PLR_MUT_NOCREDIT) || c->s_n >= c->S) {
            c->st.stalls += c->cr_data <= 0;
            break;
        }
        uint32_t k = (uint32_t)(total - pos < c->room ? total - pos : c->room);
        uint8_t *b = slot_at(c, c->R + (c->s_head + c->s_n) % c->S) + PLR_HDR;
        for (uint32_t got = 0; got < k;) {
            size_t m = iov[vi].iov_len - vo;
            if (m > k - got)
                m = k - got;
            memcpy(b + got, (const uint8_t *)iov[vi].iov_base + vo, m);
            got += (uint32_t)m;
            vo += m;
            if (vo == iov[vi].iov_len) {
                vi++;
                vo = 0;
            }
        }
        uint32_t wire = plrdma_mutant == PLR_MUT_SPLITOFF && k == c->room ? k - 1 : k;
        if (send_msg(c, MSG_DATA, wire, k) < 0)
            break;
        if (plrdma_mutant != PLR_MUT_NOCREDIT || c->cr_data > 0)
            c->cr_data--;
        pos += k;
        posted += k;
    }
    c->st.bytes_out += posted;
    size_t taken = posted;
    if (plrdma_mutant == PLR_MUT_WRITTENCQE) { /* only what completed counts as taken */
        c->ahead += posted;
        pump(c, 0);
        taken = c->done_unrep < c->ahead ? (size_t)c->done_unrep : (size_t)c->ahead;
        c->ahead -= taken;
        c->done_unrep -= taken;
    }
    if (taken)
        return (ssize_t)taken;
    errno = c->err ? c->err : EAGAIN;
    return -1;
}

/* the anchor said something: its end, or bytes it must never carry */
static void anchor_check(struct plr_conn *c)
{
    uint8_t b;
    ssize_t r = c->anchor->ops->read(c->anchor, &b, 1);
    if (r == 0)
        c->ended = 1;
    else if (r > 0) {
        fprintf(stderr, "peerlink: rdma: a byte on the TCP anchor of a link on RDMA: connection ended\n");
        fail(c, EPROTO);
    } else if (errno != EAGAIN && errno != EINTR)
        fail(c, errno);
}

static int rd_poll_fds(struct pl_transport *t, int want, struct pollfd *pf, int *ready)
{
    struct plr_conn *c = (struct plr_conn *)t;
    c->want = want;
    if (!c->err) {
        pump(c, 0);
        if (c->ops->arm(c->qp) < 0)
            fail(c, errno);
        pump(c, 0); /* a completion between the drain and the arm raises no event */
        maybe_credit(c);
    }
    pf[0] = (struct pollfd){c->ops->fd(c->qp), POLLIN, 0};
    int ar = 0;
    c->ak = c->anchor->ops->poll_fds(c->anchor, PL_TR_IN, pf + 1, &ar);
    if (c->ak < 0 || c->ak > PL_TR_POLL_MAX - 1) {
        fail(c, EPROTO);
        c->ak = 0;
    }
    if (ar & PL_TR_IN)
        anchor_check(c);
    *ready = ready_mask(c, want);
    return 1 + c->ak;
}

static int rd_poll_ready(struct pl_transport *t, const struct pollfd *pf, int k)
{
    struct plr_conn *c = (struct plr_conn *)t;
    if (pf[0].revents && !c->err) {
        if (c->ops->take_events(c->qp) < 0)
            fail(c, errno);
        pump(c, 0);
        maybe_credit(c);
    }
    if (k > 1 && (c->anchor->ops->poll_ready(c->anchor, pf + 1, k - 1) & PL_TR_IN))
        anchor_check(c);
    return ready_mask(c, c->want);
}

static long ms_since(const struct timespec *t0)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (t.tv_sec - t0->tv_sec) * 1000 + (t.tv_nsec - t0->tv_nsec) / 1000000;
}

/* the queue pair to the error state and every work request back (flushed or not), then destroyed; 1 if drained */
static int drain(struct plr_conn *c)
{
    if (c->ops->qp_err(c->qp) < 0)
        return 0;
    if (c->ops->post_send(c->qp, WR(WR_MARK, MARK_SEND), NULL, 0, 0, 1) < 0 ||
        c->ops->post_recv(c->qp, WR(WR_MARK, MARK_RECV), NULL, 0, 0) < 0)
        return 0;
    struct timespec t0;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (;;) {
        pump(c, 1);
        if (c->marks == (MARK_SEND | MARK_RECV))
            return 1;
        long left = PLR_DRAIN_MS - ms_since(&t0);
        if (left <= 0)
            return 0;
        if (c->ops->arm(c->qp) < 0)
            return 0;
        pump(c, 1);
        if (c->marks == (MARK_SEND | MARK_RECV))
            return 1;
        struct pollfd pf = {c->ops->fd(c->qp), POLLIN, 0};
        if (poll(&pf, 1, left < 50 ? (int)left : 50) > 0 && c->ops->take_events(c->qp) < 0)
            return 0;
    }
}

static void conn_release(struct plr_conn *c)
{
    uint32_t n = (uint32_t)c->R + c->S;
    if (c->qp) {
        if (plrdma_mutant == PLR_MUT_NODRAIN) {
            pool_give(c->pool, c->slots, n, 0); /* the mutant: back before anything the queue pair holds came back */
            c->have_slots = 0;
            c->ops->qp_free(c->qp);
        } else {
            int ok = drain(c);
            c->ops->qp_free(c->qp);
            if (!ok)
                fprintf(stderr,
                        "peerlink: rdma: a queue pair did not drain within %d ms: its %u slots stay out of the pool\n",
                        PLR_DRAIN_MS, (unsigned)n);
            if (c->have_slots)
                pool_give(c->pool, c->slots, n, !ok);
            c->have_slots = 0;
        }
    }
    if (c->have_slots)
        pool_give(c->pool, c->slots, n, 0);
    free(c->slots);
    free(c->rs);
    free(c->rq);
    free(c->s_len);
    free(c);
}

static void rd_close(struct pl_transport *t)
{
    struct plr_conn *c = (struct plr_conn *)t;
    struct pl_transport *a = c->anchor;
    conn_release(c);
    if (a)
        a->ops->close(a);
}

static const struct pl_tr_ops rd_ops = {"rdma", rd_read, rd_writev, rd_poll_fds, rd_poll_ready, rd_close};

static uint32_t new_psn(void)
{
    static _Atomic uint64_t ctr;
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    uint64_t x = (uint64_t)t.tv_nsec ^ (uint64_t)t.tv_sec << 30 ^ (uint64_t)getpid() << 20 ^ ++ctr * 0x9e3779b97f4a7c15u;
    x ^= x >> 31;
    x *= 0xbf58476d1ce4e5b9u;
    x ^= x >> 27;
    return (uint32_t)x & 0xffffff;
}

struct plr_conn *plr_conn_new(struct plr_pool *p, char *err, size_t errlen)
{
    struct plr_conn *c = calloc(1, sizeof *c);
    if (!c) {
        snprintf(err, errlen, "RDMA: out of memory");
        return NULL;
    }
    c->t.ops = &rd_ops;
    c->pool = p;
    c->ops = p->ops;
    c->D = p->cfg.data_slots;
    c->K = p->cfg.ctl_slots;
    c->R = (uint16_t)(c->D + c->K);
    c->S = p->cfg.send_slots;
    c->sig_every = c->S / 4 ? c->S / 4 : 1;
    c->slots = malloc(sizeof *c->slots * (c->R + c->S));
    c->rs = calloc(c->R, sizeof *c->rs);
    c->rq = calloc(c->R, sizeof *c->rq);
    c->s_len = calloc(c->S, sizeof *c->s_len);
    if (!c->slots || !c->rs || !c->rq || !c->s_len) {
        snprintf(err, errlen, "RDMA: out of memory");
        conn_release(c);
        return NULL;
    }
    if (pool_take(p, c->slots, (uint32_t)c->R + c->S) < 0) {
        struct plr_pool_stats st;
        plr_pool_stats(p, &st);
        snprintf(err, errlen, "RDMA: the pool has %u free slots, a connection needs %u (cluster.peer.rdma.pool_mb)",
                 (unsigned)st.free, (unsigned)(c->R + c->S));
        errno = ENOBUFS;
        conn_release(c);
        return NULL;
    }
    c->have_slots = 1;
    c->qp = c->ops->qp_new(p->dev, c->S + 1, c->R + 1, &c->qpn);
    if (!c->qp) {
        snprintf(err, errlen, "RDMA: creating a queue pair failed: %s", strerror(errno));
        conn_release(c);
        return NULL;
    }
    c->psn = new_psn();
    for (uint32_t i = 0; i < c->R && !c->err; i++)
        repost(c, i);
    if (c->err) {
        snprintf(err, errlen, "RDMA: posting the receives failed: %s", strerror(c->err));
        conn_release(c);
        return NULL;
    }
    return c;
}

void plr_conn_ep(const struct plr_conn *c, struct plr_ep *o)
{
    memset(o, 0, sizeof *o);
    const struct plr_port *pp = &c->pool->port;
    o->lid = pp->lid;
    memcpy(o->gid, pp->gid, 16);
    o->qpn = c->qpn;
    o->psn = c->psn;
    o->mtu = pp->mtu;
    o->link_layer = pp->link_layer;
    o->port = pp->port;
    o->slot = c->pool->cfg.slot;
    o->data = c->D;
    o->ctl = c->K;
}

int plr_conn_start(struct plr_conn *c, const struct plr_ep *peer, char *err, size_t errlen)
{
    const struct plr_port *pp = &c->pool->port;
    if (peer->link_layer != pp->link_layer) {
        snprintf(err, errlen, "RDMA: the peer's port is %s, ours %s", peer->link_layer == PLR_LL_IB ? "InfiniBand" : "Ethernet",
                 pp->link_layer == PLR_LL_IB ? "InfiniBand" : "Ethernet");
        errno = EPROTO;
        return -1;
    }
    const struct plr_pool_cfg *pc = &c->pool->cfg;
    struct plr_qp_attr a = {0};
    a.lid = peer->lid;
    memcpy(a.gid, peer->gid, 16);
    a.use_gid = pp->link_layer == PLR_LL_ETH || memcmp(peer->gid, pp->gid, 8) != 0; /* RoCE, or another subnet */
    a.qpn = peer->qpn;
    a.rq_psn = peer->psn;
    a.sq_psn = c->psn;
    a.mtu = peer->mtu < pp->mtu ? peer->mtu : pp->mtu;
    a.sl = pc->sl;
    a.rnr_retry = pc->rnr_retry;
    a.retry_cnt = pc->retry_cnt;
    a.timeout = pc->timeout;
    if (c->ops->qp_rts(c->qp, &a) < 0) {
        snprintf(err, errlen, "RDMA: connecting the queue pair to QPN %" PRIu32 " (LID %u) failed: %s", peer->qpn,
                 (unsigned)peer->lid, strerror(errno));
        return -1;
    }
    c->peer = *peer;
    c->cr_data = peer->data;
    c->cr_ctl = peer->ctl;
    uint32_t m = peer->slot < pc->slot ? peer->slot : pc->slot;
    c->room = m - PLR_HDR;
    c->started = 1;
    return 0;
}

struct pl_transport *plr_conn_transport(struct plr_conn *c, struct pl_transport *anchor)
{
    c->anchor = anchor;
    return &c->t;
}

void plr_conn_free(struct plr_conn *c)
{
    if (c)
        conn_release(c);
}

void plr_conn_stats(const struct pl_transport *t, struct plr_conn_stats *st)
{
    *st = ((const struct plr_conn *)t)->st;
}
