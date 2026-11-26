/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * A fake verbs fabric behind struct plr_ops (server/plrdma.h), for tests/test_plrdma.c (docs/design-rdma.md §8): RC
 * queue pairs that deliver SENDs into the peer's posted receives, completion queues with a completion channel each (a
 * pipe that gets a byte when an armed CQ gets an entry, as ibv_req_notify_cq: an entry already there raises nothing),
 * and a fabric thread that executes posted sends some 50 µs later, as a NIC does, so a slot reused too early is read
 * after the change. It is strict where the hardware runs with rnr_retry 0 (the tests' setting) or where iWARP is: a
 * SEND that finds no posted receive fails the sender's queue pair at once (status ERR, everything else flushed), so a
 * credit bug is an error, never a stall. It checks what the hardware cannot report: a buffer posted while a work
 * request still holds it, a slot the pool takes back while a work request holds it (pfk_owned, called from the pool's
 * test hook), a queue pair destroyed with work requests outstanding, numbers (LID, QPN, PSNs) that do not match the
 * peer's, a message longer than the receive. Each is counted in `violations` with a line.
 *
 * Faults to plant: the n-th send fails (`err_send_at`), a queue pair fails now (pfk_fail_qp: a port that went away),
 * registration, queue pair creation, the move to RTS or the device's open fails.
 */
#ifndef TSDB_PLR_FAKE_H
#define TSDB_PLR_FAKE_H

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "plrdma.h"

#define PFK_QPS 128
#define PFK_WRS 1024
#define PFK_CQ 4096

struct pfk_wr {
    uint64_t id;
    uint8_t *p;
    uint32_t n;
    int sig;
    uint64_t tick; /* the fabric tick it was posted in: executed only on a later one */
};

struct pfk_fifo {
    struct pfk_wr w[PFK_WRS];
    int head, n;
};

struct plr_dev {
    struct pfk_fab *f;
    struct plr_port port;
};

struct plr_mr {
    struct plr_dev *d;
    uint8_t *p;
    size_t n;
    uint32_t lkey;
};

struct plr_qp {
    struct pfk_fab *f;
    struct plr_dev *d;
    int used, state; /* 0 INIT, 1 RTS, 2 ERR */
    uint32_t qpn, sq_psn;
    struct plr_qp_attr a;
    struct pfk_fifo sq, rq;
    struct pfk_fifo held; /* sends executed whose buffers the NIC may still hold: until a later signalled completion */
    struct plr_wc cq[PFK_CQ];
    int cq_head, cq_n, armed;
    struct plr_wc late[PFK_CQ]; /* arm_race: made while not armed, into cq at the next arm */
    int late_n;
    int ev[2]; /* the completion channel */
};

struct plr_cm_id {
    struct pfk_fab *f;
    int ev[2];
    struct plr_cm_ev q[32];
    int q_head, q_n;
    int is_lis;
    struct plr_cm_addr addr;
    struct plr_qp *qp;
    struct plr_cm_id *peer;
    uint8_t sl;
};

struct pfk_fab {
    pthread_mutex_t mu;
    pthread_t th;
    _Atomic int stop, hold; /* hold: the NIC executes nothing meanwhile */
    uint64_t tick;
    uint16_t next_lid, next_cm_port;
    uint32_t next_qpn, next_key;
    struct plr_qp qp[PFK_QPS];
    struct plr_mr *mrs[16];
    struct plr_cm_id *cm_ids[64];
    /* faults */
    long err_send_at;      /* > 0: the send executed as number err_send_at (from 1) completes with status ERR */
    long hold_after_sends; /* > 0: set hold=1 as soon as sends reaches this count (deterministic mid-exchange hold) */
    long sends;            /* sends executed */
    int fail_reg, fail_qp_new, fail_dev;
    _Atomic int fail_rts;
    _Atomic int fail_cm_listen, fail_cm_dial, fail_cm_addr, fail_cm_route, fail_cm_connect, fail_cm_accept;
    /* slow_flush: a queue pair in error flushes one work request per queue and tick, in order (as an HCA does:
     * asynchronously), so a drain that waits for less than both markers returns while the receive queue still holds
     * slots. no_flush: it flushes nothing (a drain that never finishes). arm_race: a completion made while its queue
     * pair is not armed shows in the CQ only when it is armed next, without an event: the worst timing, a completion
     * that lands after the last poll of the CQ and before the arm. */
    int slow_flush, no_flush, arm_race;
    /* findings */
    _Atomic int violations, cm_live;
    long rnr, delivered;
    uint8_t gid_prefix[8]; /* if use_gid_prefix: override default fe80::0 subnet prefix for next dev_open */
    int use_gid_prefix;
    _Atomic uint8_t last_sl;  /* SL passed to most recent qp_rts */
    _Atomic uint32_t last_qpn; /* QPN of most recent qp_new */
};

static struct pfk_fab *pfk_cur; /* the ops have no context argument for dev_open: the fabric the test made */

static void pfk_violation(struct pfk_fab *f, const char *what)
{
    fprintf(stderr, "plr_fake: VIOLATION: %s\n", what);
    atomic_fetch_add(&f->violations, 1);
}

static void pfk_push(struct pfk_fifo *q, struct pfk_wr w)
{
    q->w[(q->head + q->n) % PFK_WRS] = w;
    q->n++;
}

static struct pfk_wr pfk_pop(struct pfk_fifo *q)
{
    struct pfk_wr w = q->w[q->head];
    q->head = (q->head + 1) % PFK_WRS;
    q->n--;
    return w;
}

static int pfk_overlaps(const struct pfk_fifo *q, const uint8_t *p, size_t n)
{
    for (int i = 0; i < q->n; i++) {
        const struct pfk_wr *w = &q->w[(q->head + i) % PFK_WRS];
        if (w->p && w->n && w->p < p + n && p < w->p + w->n)
            return 1;
    }
    return 0;
}

/* some work request of the fabric holds bytes of [p, p + n): a posted one, or a send whose completion was not seen */
static int pfk_owned_locked(struct pfk_fab *f, const uint8_t *p, size_t n)
{
    for (int i = 0; i < PFK_QPS; i++) {
        struct plr_qp *q = &f->qp[i];
        if (q->used && (pfk_overlaps(&q->sq, p, n) || pfk_overlaps(&q->rq, p, n) || pfk_overlaps(&q->held, p, n)))
            return 1;
    }
    return 0;
}

static int pfk_owned(struct pfk_fab *f, const void *p, size_t n)
{
    pthread_mutex_lock(&f->mu);
    int r = pfk_owned_locked(f, p, n);
    pthread_mutex_unlock(&f->mu);
    return r;
}

static void pfk_wc_rnr_locked(struct plr_qp *q, uint64_t id, int op, int status, uint32_t len, uint8_t rnr)
{
    if (q->f->arm_race && !q->armed) {
        if (q->late_n + q->cq_n == PFK_CQ)
            pfk_violation(q->f, "a completion queue overflowed");
        else
            q->late[q->late_n++] = (struct plr_wc){id, op, status, len, status, rnr};
        return;
    }
    if (q->cq_n + q->late_n == PFK_CQ) {
        pfk_violation(q->f, "a completion queue overflowed");
        return;
    }
    q->cq[(q->cq_head + q->cq_n) % PFK_CQ] = (struct plr_wc){id, op, status, len, status, rnr};
    q->cq_n++;
    if (q->armed) {
        q->armed = 0;
        if (write(q->ev[1], "", 1) < 0) {
            /* full: an event is pending */
        }
    }
}

static void pfk_wc_locked(struct plr_qp *q, uint64_t id, int op, int status, uint32_t len)
{
    pfk_wc_rnr_locked(q, id, op, status, len, 0);
}

/* the queue pair fails: everything posted comes back flushed (every send gets a completion then) */
static void pfk_qp_err_locked(struct plr_qp *q)
{
    q->state = 2;
    if (q->f->slow_flush || q->f->no_flush)
        return; /* pfk_flush_step_locked, a tick at a time */
    while (q->sq.n) {
        struct pfk_wr w = pfk_pop(&q->sq);
        pfk_wc_locked(q, w.id, PLR_WC_SEND, PLR_WC_FLUSH, 0);
    }
    while (q->rq.n) {
        struct pfk_wr w = pfk_pop(&q->rq);
        pfk_wc_locked(q, w.id, PLR_WC_RECV, PLR_WC_FLUSH, 0);
    }
    q->held.n = 0; /* their completions were made: the NIC holds nothing more */
}

static struct plr_qp *pfk_find_locked(struct pfk_fab *f, uint32_t qpn)
{
    for (int i = 0; i < PFK_QPS; i++)
        if (f->qp[i].used && f->qp[i].qpn == qpn)
            return &f->qp[i];
    return NULL;
}

/* slow_flush: a queue pair in error returns one work request of each queue per tick */
static void pfk_flush_step_locked(struct plr_qp *q)
{
    if (q->sq.n) {
        struct pfk_wr w = pfk_pop(&q->sq);
        pfk_wc_locked(q, w.id, PLR_WC_SEND, PLR_WC_FLUSH, 0);
    }
    if (!q->sq.n)
        q->held.n = 0; /* every send before the last one flushed is done: the NIC holds nothing more of them */
    if (q->rq.n) {
        struct pfk_wr w = pfk_pop(&q->rq);
        pfk_wc_locked(q, w.id, PLR_WC_RECV, PLR_WC_FLUSH, 0);
    }
}

/* execute the sends posted before this tick */
static void pfk_step_locked(struct pfk_fab *f)
{
    f->tick++;
    for (int i = 0; i < PFK_QPS; i++) {
        struct plr_qp *q = &f->qp[i];
        if (q->used && q->state == 2 && f->slow_flush && !f->no_flush)
            pfk_flush_step_locked(q);
        while (q->used && q->state == 1 && q->sq.n && q->sq.w[q->sq.head].tick < f->tick - 1) {
            struct pfk_wr w = pfk_pop(&q->sq);
            struct plr_qp *r = pfk_find_locked(f, q->a.qpn);
            f->sends++;
            if (f->err_send_at > 0 && f->sends == f->err_send_at) {
                pfk_wc_locked(q, w.id, PLR_WC_SEND, PLR_WC_ERR, 0);
                pfk_qp_err_locked(q);
                break;
            }
            if (!r || r->state != 1 || r->d->port.lid != q->a.lid || r->a.qpn != q->qpn || q->a.rq_psn != r->sq_psn ||
                r->a.rq_psn != q->sq_psn) { /* no such peer, or packets it drops: the retries run out */
                if (r && r->state == 1)
                    pfk_violation(f, "a queue pair connected with numbers that are not its peer's");
                pfk_wc_locked(q, w.id, PLR_WC_SEND, PLR_WC_ERR, 0);
                pfk_qp_err_locked(q);
                break;
            }
            if (!r->rq.n) { /* rnr_retry 0, or iWARP: no receive posted fails the sender */
                f->rnr++;
                fprintf(stderr, "plr_fake: a SEND found no posted receive (RNR): queue pair %u failed\n",
                        (unsigned)q->qpn);
                pfk_wc_rnr_locked(q, w.id, PLR_WC_SEND, PLR_WC_ERR, 0, 1);
                pfk_qp_err_locked(q);
                break;
            }
            struct pfk_wr rw = pfk_pop(&r->rq);
            if (w.n > rw.n) {
                pfk_violation(f, "a message longer than the receive slot");
                pfk_wc_locked(r, rw.id, PLR_WC_RECV, PLR_WC_ERR, 0);
                pfk_qp_err_locked(r);
                pfk_wc_locked(q, w.id, PLR_WC_SEND, PLR_WC_ERR, 0);
                pfk_qp_err_locked(q);
                break;
            }
            memcpy(rw.p, w.p, w.n);
            f->delivered++;
            pfk_wc_locked(r, rw.id, PLR_WC_RECV, PLR_WC_OK, w.n);
            if (w.sig) {
                q->held.n = 0; /* a signalled completion: every send before it is done too */
                pfk_wc_locked(q, w.id, PLR_WC_SEND, PLR_WC_OK, 0);
            } else
                pfk_push(&q->held, w);
            if (f->hold_after_sends > 0 && f->sends == f->hold_after_sends) {
                atomic_store(&f->hold, 1);
                return;
            }
        }
    }
}

static void pfk_flush_only_locked(struct pfk_fab *f)
{
    for (int i = 0; i < PFK_QPS; i++) {
        struct plr_qp *q = &f->qp[i];
        if (q->used && q->state == 2 && f->slow_flush && !f->no_flush)
            pfk_flush_step_locked(q);
    }
}

static void *pfk_thread(void *arg)
{
    struct pfk_fab *f = arg;
    while (!atomic_load(&f->stop)) {
        pthread_mutex_lock(&f->mu);
        if (!atomic_load(&f->hold))
            pfk_step_locked(f);
        else
            pfk_flush_only_locked(f);
        pthread_mutex_unlock(&f->mu);
        struct timespec ts = {0, 50000};
        nanosleep(&ts, NULL);
    }
    return NULL;
}

static struct pfk_fab *pfk_new(void)
{
    struct pfk_fab *f = calloc(1, sizeof *f);
    pthread_mutex_init(&f->mu, NULL);
    f->next_lid = 7;
    f->next_cm_port = 10001;
    f->next_qpn = 0x400;
    f->next_key = 0x1000;
    pfk_cur = f;
    pthread_create(&f->th, NULL, pfk_thread, f);
    return f;
}

static void pfk_free(struct pfk_fab *f)
{
    atomic_store(&f->stop, 1);
    pthread_join(f->th, NULL);
    for (int i = 0; i < PFK_QPS; i++)
        if (f->qp[i].used) {
            close(f->qp[i].ev[0]);
            close(f->qp[i].ev[1]);
        }
    pthread_mutex_destroy(&f->mu);
    if (pfk_cur == f)
        pfk_cur = NULL;
    free(f);
}

/* a port goes away under queue pair qpn (0: every queue pair) */
static void pfk_fail_qp(struct pfk_fab *f, uint32_t qpn)
{
    pthread_mutex_lock(&f->mu);
    for (int i = 0; i < PFK_QPS; i++)
        if (f->qp[i].used && (!qpn || f->qp[i].qpn == qpn) && f->qp[i].state != 2)
            pfk_qp_err_locked(&f->qp[i]);
    pthread_mutex_unlock(&f->mu);
}

/* sends posted on queue pair qpn and not yet executed */
static int pfk_sq_posted(struct pfk_fab *f, uint32_t qpn)
{
    pthread_mutex_lock(&f->mu);
    struct plr_qp *q = pfk_find_locked(f, qpn);
    int n = q ? q->sq.n : 0;
    pthread_mutex_unlock(&f->mu);
    return n;
}

static int pfk_live_qps(struct pfk_fab *f)
{
    pthread_mutex_lock(&f->mu);
    int n = 0;
    for (int i = 0; i < PFK_QPS; i++)
        n += f->qp[i].used;
    pthread_mutex_unlock(&f->mu);
    return n;
}

/* ------------------------------------------------------------------------------------------------- the ops */

static int pfk_dev_open(const char *dev, int port, struct plr_dev **d, struct plr_port *pi, char *err, size_t errlen)
{
    struct pfk_fab *f = pfk_cur;
    (void)dev;
    if (!f || f->fail_dev) {
        snprintf(err, errlen, "RDMA: no active InfiniBand port (fake)");
        errno = ENODEV;
        return -1;
    }
    struct plr_dev *x = calloc(1, sizeof *x);
    x->f = f;
    pthread_mutex_lock(&f->mu);
    x->port.lid = f->next_lid++;
    if (f->use_gid_prefix)
        memcpy(x->port.gid, f->gid_prefix, 8);
    else {
        x->port.gid[0] = 0xfe;
        x->port.gid[1] = 0x80;
    }
    pthread_mutex_unlock(&f->mu);
    x->port.gid[15] = (uint8_t)x->port.lid;
    x->port.mtu = 5;
    x->port.link_layer = PLR_LL_IB;
    x->port.port = (uint8_t)(port ? port : 1);
    x->port.cm_af = 4;
    x->port.cm_ip[0] = 127;
    x->port.cm_ip[3] = 1;
    *d = x;
    *pi = x->port;
    return 0;
}

static void pfk_dev_close(struct plr_dev *d) { free(d); }

static struct plr_mr *pfk_reg(struct plr_dev *d, void *p, size_t n, uint32_t *lkey)
{
    if (d->f->fail_reg) {
        errno = ENOMEM;
        return NULL;
    }
    struct plr_mr *m = calloc(1, sizeof *m);
    m->d = d;
    m->p = p;
    m->n = n;
    pthread_mutex_lock(&d->f->mu);
    m->lkey = d->f->next_key++;
    for (int i = 0; i < 16; i++)
        if (!d->f->mrs[i]) {
            d->f->mrs[i] = m;
            break;
        }
    pthread_mutex_unlock(&d->f->mu);
    *lkey = m->lkey;
    return m;
}

static void pfk_dereg(struct plr_mr *m)
{
    struct pfk_fab *f = m->d->f;
    pthread_mutex_lock(&f->mu);
    for (int i = 0; i < 16; i++)
        if (f->mrs[i] == m)
            f->mrs[i] = NULL;
    if (pfk_owned_locked(f, m->p, m->n))
        pfk_violation(f, "a region deregistered while work requests hold it");
    pthread_mutex_unlock(&f->mu);
    free(m);
}

/* the scatter entry lies in a region of the device with that key */
static int pfk_key_ok_locked(struct plr_qp *q, const uint8_t *p, uint32_t n, uint32_t lkey)
{
    if (!n)
        return 1;
    for (int i = 0; i < 16; i++) {
        struct plr_mr *m = q->f->mrs[i];
        if (m && m->d == q->d && m->lkey == lkey && p >= m->p && p + n <= m->p + m->n)
            return 1;
    }
    return 0;
}

static struct plr_qp *pfk_qp_new(struct plr_dev *d, int sq, int rq, uint32_t *qpn)
{
    struct pfk_fab *f = d->f;
    if (f->fail_qp_new || sq > PFK_WRS || rq > PFK_WRS) {
        errno = ENOMEM;
        return NULL;
    }
    pthread_mutex_lock(&f->mu);
    struct plr_qp *q = NULL;
    for (int i = 0; i < PFK_QPS && !q; i++)
        if (!f->qp[i].used)
            q = &f->qp[i];
    if (!q) {
        pthread_mutex_unlock(&f->mu);
        errno = ENOMEM;
        return NULL;
    }
    memset(q, 0, sizeof *q);
    q->used = 1;
    q->f = f;
    q->d = d;
    q->qpn = f->next_qpn++;
    f->last_qpn = q->qpn;
    if (pipe(q->ev) < 0) {
        q->used = 0;
        pthread_mutex_unlock(&f->mu);
        return NULL;
    }
    fcntl(q->ev[0], F_SETFL, O_NONBLOCK);
    fcntl(q->ev[1], F_SETFL, O_NONBLOCK);
    *qpn = q->qpn;
    pthread_mutex_unlock(&f->mu);
    return q;
}

static int pfk_qp_rts(struct plr_qp *q, const struct plr_qp_attr *a)
{
    if (q->f->fail_rts) {
        errno = EINVAL;
        return -1;
    }
    pthread_mutex_lock(&q->f->mu);
    int ok = q->state == 0;
    if (ok) {
        q->a = *a;
        q->sq_psn = a->sq_psn;
        q->state = 1;
        q->f->last_sl = a->sl;
    }
    pthread_mutex_unlock(&q->f->mu);
    if (!ok)
        errno = EINVAL;
    return ok ? 0 : -1;
}

static int pfk_qp_err(struct plr_qp *q)
{
    pthread_mutex_lock(&q->f->mu);
    pfk_qp_err_locked(q);
    pthread_mutex_unlock(&q->f->mu);
    return 0;
}

static void pfk_qp_free(struct plr_qp *q)
{
    struct pfk_fab *f = q->f;
    pthread_mutex_lock(&f->mu);
    if (f->no_flush) /* the test withheld the flush: destroying the queue pair stops the NIC */
        q->sq.n = q->rq.n = q->held.n = 0;
    if (q->sq.n || q->rq.n || q->held.n)
        pfk_violation(f, "a queue pair destroyed with work requests outstanding (not drained)");
    close(q->ev[0]);
    close(q->ev[1]);
    q->used = 0;
    pthread_mutex_unlock(&f->mu);
}

static int pfk_fd(struct plr_qp *q) { return q->ev[0]; }

static int pfk_arm(struct plr_qp *q)
{
    pthread_mutex_lock(&q->f->mu);
    for (int i = 0; i < q->late_n; i++) /* they came between the poll and this arm: no event */
        q->cq[(q->cq_head + q->cq_n++) % PFK_CQ] = q->late[i];
    q->late_n = 0;
    q->armed = 1;
    pthread_mutex_unlock(&q->f->mu);
    return 0;
}

static int pfk_take_events(struct plr_qp *q)
{
    char b[64];
    int n = 0;
    ssize_t r;
    while ((r = read(q->ev[0], b, sizeof b)) > 0)
        n += (int)r;
    return n;
}

static int pfk_poll(struct plr_qp *q, struct plr_wc *wc, int n)
{
    pthread_mutex_lock(&q->f->mu);
    int k = 0;
    while (k < n && q->cq_n) {
        wc[k++] = q->cq[q->cq_head];
        q->cq_head = (q->cq_head + 1) % PFK_CQ;
        q->cq_n--;
    }
    pthread_mutex_unlock(&q->f->mu);
    return k;
}

static int pfk_post(struct plr_qp *q, int send, uint64_t id, const void *p, uint32_t n, uint32_t lkey, int sig)
{
    struct pfk_fab *f = q->f;
    pthread_mutex_lock(&f->mu);
    if (!pfk_key_ok_locked(q, p, n, lkey)) {
        pthread_mutex_unlock(&f->mu);
        pfk_violation(f, "a work request outside the registered region or with another key");
        errno = EINVAL;
        return -1;
    }
    if (n && pfk_owned_locked(f, p, n))
        pfk_violation(f, "a buffer posted while another work request still holds it");
    struct pfk_wr w = {id, (uint8_t *)p, n, sig, f->tick};
    if (q->state == 2 && !f->slow_flush && !f->no_flush) /* in error: flushed at once (the drain's markers) */
        pfk_wc_locked(q, id, send ? PLR_WC_SEND : PLR_WC_RECV, PLR_WC_FLUSH, 0);
    else if ((send ? q->sq.n : q->rq.n) == PFK_WRS) {
        pthread_mutex_unlock(&f->mu);
        errno = ENOMEM;
        return -1;
    } else if (send && q->state == 0) {
        pthread_mutex_unlock(&f->mu);
        errno = EINVAL;
        return -1;
    } else
        pfk_push(send ? &q->sq : &q->rq, w);
    pthread_mutex_unlock(&f->mu);
    return 0;
}

static int pfk_post_send(struct plr_qp *q, uint64_t id, const void *p, uint32_t n, uint32_t lkey, int sig)
{
    return pfk_post(q, 1, id, p, n, lkey, sig);
}

static int pfk_post_recv(struct plr_qp *q, uint64_t id, void *p, uint32_t n, uint32_t lkey)
{
    return pfk_post(q, 0, id, p, n, lkey, 1);
}

static const struct plr_ops pfk_ops = {"fake",   pfk_dev_open, pfk_dev_close, pfk_reg,  pfk_dereg,
                                       pfk_qp_new, pfk_qp_rts, pfk_qp_err,    pfk_qp_free, pfk_fd,
                                       pfk_arm,  pfk_take_events, pfk_poll,   pfk_post_send, pfk_post_recv};

/* ------------------------------------------------------------------------------------------------- fake rdma_cm */

static void pfk_cm_push_locked(struct plr_cm_id *id, struct plr_cm_ev ev)
{
    if (!id || id->q_n >= 32)
        return;
    id->q[(id->q_head + id->q_n) % 32] = ev;
    id->q_n++;
    if (write(id->ev[1], "", 1) < 0) {
        /* pipe full: already readable */
    }
}

static struct plr_cm_id *pfk_cm_alloc_locked(struct pfk_fab *f)
{
    int slot = -1;
    for (int i = 0; i < 64; i++) {
        if (!f->cm_ids[i]) {
            slot = i;
            break;
        }
    }
    if (slot < 0)
        return NULL;
    struct plr_cm_id *id = calloc(1, sizeof *id);
    if (!id)
        return NULL;
    if (pipe(id->ev) < 0) {
        free(id);
        return NULL;
    }
    fcntl(id->ev[0], F_SETFL, O_NONBLOCK);
    fcntl(id->ev[1], F_SETFL, O_NONBLOCK);
    id->f = f;
    f->cm_ids[slot] = id;
    atomic_fetch_add(&f->cm_live, 1);
    return id;
}

static void pfk_cm_free_locked(struct plr_cm_id *id)
{
    struct pfk_fab *f = id->f;
    for (int i = 0; i < 64; i++)
        if (f->cm_ids[i] == id)
            f->cm_ids[i] = NULL;
    if (id->peer) {
        id->peer->peer = NULL;
        id->peer = NULL;
    }
    while (id->q_n) {
        struct plr_cm_ev ev = id->q[id->q_head];
        id->q_head = (id->q_head + 1) % 32;
        id->q_n--;
        if (ev.req_id)
            pfk_cm_free_locked(ev.req_id);
    }
    close(id->ev[0]);
    close(id->ev[1]);
    atomic_fetch_sub(&f->cm_live, 1);
    free(id);
}

static struct plr_cm_id *pfk_cm_listen(const struct plr_cm_addr *bind, struct plr_cm_addr *bound, char *err,
                                       size_t errlen)
{
    struct pfk_fab *f = pfk_cur;
    if (!f || atomic_load(&f->fail_cm_listen)) {
        snprintf(err, errlen, "RDMA CM: listen failed (fake)");
        errno = EADDRINUSE;
        return NULL;
    }
    pthread_mutex_lock(&f->mu);
    struct plr_cm_id *id = pfk_cm_alloc_locked(f);
    if (!id) {
        pthread_mutex_unlock(&f->mu);
        snprintf(err, errlen, "RDMA CM: out of CM ids (fake)");
        errno = ENOMEM;
        return NULL;
    }
    id->is_lis = 1;
    id->addr.af = (bind && bind->af) ? bind->af : 4;
    id->addr.port = (bind && bind->port) ? bind->port : f->next_cm_port++;
    if (bind && bind->af)
        memcpy(id->addr.ip, bind->ip, 16);
    if (bound)
        *bound = id->addr;
    pthread_mutex_unlock(&f->mu);
    return id;
}

static struct plr_cm_id *pfk_cm_dial(const struct plr_cm_addr *dst, int timeout_ms, char *err, size_t errlen)
{
    (void)timeout_ms;
    struct pfk_fab *f = pfk_cur;
    if (!f || !dst || !dst->port || atomic_load(&f->fail_cm_dial)) {
        snprintf(err, errlen, "RDMA CM: dial failed (fake)");
        errno = EHOSTUNREACH;
        return NULL;
    }
    pthread_mutex_lock(&f->mu);
    struct plr_cm_id *id = pfk_cm_alloc_locked(f);
    if (!id) {
        pthread_mutex_unlock(&f->mu);
        snprintf(err, errlen, "RDMA CM: out of CM ids (fake)");
        errno = ENOMEM;
        return NULL;
    }
    id->addr = *dst;
    struct plr_cm_ev ev = {.type = atomic_load(&f->fail_cm_addr) ? PLR_CM_EV_ERR : PLR_CM_EV_ADDR_RESOLVED};
    pfk_cm_push_locked(id, ev);
    pthread_mutex_unlock(&f->mu);
    return id;
}

static int pfk_cm_resolve_route(struct plr_cm_id *id, int timeout_ms)
{
    (void)timeout_ms;
    struct pfk_fab *f = id->f;
    pthread_mutex_lock(&f->mu);
    struct plr_cm_ev ev = {.type = atomic_load(&f->fail_cm_route) ? PLR_CM_EV_ERR : PLR_CM_EV_ROUTE_RESOLVED};
    pfk_cm_push_locked(id, ev);
    pthread_mutex_unlock(&f->mu);
    return 0;
}

static struct plr_qp *pfk_cm_qp_create(struct plr_cm_id *id, struct plr_dev *d, int sq, int rq, uint32_t *qpn)
{
    struct plr_qp *q = pfk_qp_new(d, sq, rq, qpn);
    if (!q)
        return NULL;
    pthread_mutex_lock(&id->f->mu);
    id->qp = q;
    pthread_mutex_unlock(&id->f->mu);
    return q;
}

static int pfk_cm_connect(struct plr_cm_id *id, const void *pd, uint8_t pd_len, uint8_t rnr_retry, uint8_t retry_cnt)
{
    (void)rnr_retry;
    (void)retry_cnt;
    struct pfk_fab *f = id->f;
    if (atomic_load(&f->fail_cm_connect)) {
        errno = ECONNREFUSED;
        return -1;
    }
    pthread_mutex_lock(&f->mu);
    struct plr_cm_id *lis = NULL;
    for (int i = 0; i < 64; i++) {
        if (f->cm_ids[i] && f->cm_ids[i]->is_lis && f->cm_ids[i]->addr.port == id->addr.port) {
            lis = f->cm_ids[i];
            break;
        }
    }
    if (!lis) {
        struct plr_cm_ev ev = {.type = PLR_CM_EV_REJECTED};
        pfk_cm_push_locked(id, ev);
        pthread_mutex_unlock(&f->mu);
        return 0;
    }
    struct plr_cm_id *req = pfk_cm_alloc_locked(f);
    if (!req) {
        pthread_mutex_unlock(&f->mu);
        errno = ENOMEM;
        return -1;
    }
    req->peer = id;
    id->peer = req;
    struct plr_cm_ev ev = {0};
    ev.type = PLR_CM_EV_CONNECT_REQUEST;
    ev.req_id = req;
    if (pd && pd_len) {
        size_t copy = pd_len > sizeof ev.pd ? sizeof ev.pd : pd_len;
        memcpy(ev.pd, pd, copy);
        ev.pd_len = pd_len;
    }
    pfk_cm_push_locked(lis, ev);
    pthread_mutex_unlock(&f->mu);
    return 0;
}

static int pfk_cm_accept(struct plr_cm_id *id, uint8_t rnr_retry)
{
    (void)rnr_retry;
    struct pfk_fab *f = id->f;
    pthread_mutex_lock(&f->mu);
    if (atomic_load(&f->fail_cm_accept) || !id->qp || !id->peer || !id->peer->qp || atomic_load(&f->fail_rts)) {
        if (id->peer) {
            struct plr_cm_ev rev = {.type = PLR_CM_EV_REJECTED};
            pfk_cm_push_locked(id->peer, rev);
        }
        pthread_mutex_unlock(&f->mu);
        errno = EIO;
        return -1;
    }
    struct plr_qp *qa = id->qp, *qb = id->peer->qp;
    qa->a.lid = qb->d->port.lid;
    qa->a.qpn = qb->qpn;
    qa->a.rq_psn = 1;
    qa->sq_psn = 2;
    qa->a.sl = id->peer->sl;
    qa->state = 1;
    qb->a.lid = qa->d->port.lid;
    qb->a.qpn = qa->qpn;
    qb->a.rq_psn = 2;
    qb->sq_psn = 1;
    qb->a.sl = id->peer->sl;
    qb->state = 1;
    atomic_store(&f->last_sl, id->peer->sl);
    struct plr_cm_ev est = {.type = PLR_CM_EV_ESTABLISHED};
    pfk_cm_push_locked(id, est);
    pfk_cm_push_locked(id->peer, est);
    pthread_mutex_unlock(&f->mu);
    return 0;
}

static void pfk_cm_reject(struct plr_cm_id *id)
{
    if (!id)
        return;
    struct pfk_fab *f = id->f;
    pthread_mutex_lock(&f->mu);
    if (id->peer) {
        struct plr_cm_ev ev = {.type = PLR_CM_EV_REJECTED};
        pfk_cm_push_locked(id->peer, ev);
    }
    pfk_cm_free_locked(id);
    pthread_mutex_unlock(&f->mu);
}

static int pfk_cm_fd(struct plr_cm_id *id) { return id->ev[0]; }

static int pfk_cm_next_event(struct plr_cm_id *id, struct plr_cm_ev *out)
{
    memset(out, 0, sizeof *out);
    struct pfk_fab *f = id->f;
    pthread_mutex_lock(&f->mu);
    if (!id->q_n) {
        char b[64];
        while (read(id->ev[0], b, sizeof b) > 0)
            ;
        pthread_mutex_unlock(&f->mu);
        return 0;
    }
    *out = id->q[id->q_head];
    id->q_head = (id->q_head + 1) % 32;
    id->q_n--;
    if (!id->q_n) {
        char b[64];
        while (read(id->ev[0], b, sizeof b) > 0)
            ;
    }
    pthread_mutex_unlock(&f->mu);
    return 1;
}

static int pfk_cm_disconnect(struct plr_cm_id *id)
{
    if (!id)
        return 0;
    struct pfk_fab *f = id->f;
    pthread_mutex_lock(&f->mu);
    if (id->qp && id->qp->used && id->qp->state != 2)
        pfk_qp_err_locked(id->qp);
    if (id->peer) {
        struct plr_cm_ev ev = {.type = PLR_CM_EV_DISCONNECTED};
        pfk_cm_push_locked(id->peer, ev);
    }
    pthread_mutex_unlock(&f->mu);
    return 0;
}

static void pfk_cm_destroy(struct plr_cm_id *id)
{
    if (!id)
        return;
    struct pfk_fab *f = id->f;
    pthread_mutex_lock(&f->mu);
    pfk_cm_free_locked(id);
    pthread_mutex_unlock(&f->mu);
}

static int pfk_cm_set_sl(struct plr_cm_id *id, uint8_t sl)
{
    pthread_mutex_lock(&id->f->mu);
    id->sl = sl;
    pthread_mutex_unlock(&id->f->mu);
    return 0;
}

static const struct plr_cm_ops pfk_cm_ops = {
    "fake-cm",
    &pfk_ops,
    pfk_cm_listen,
    pfk_cm_dial,
    pfk_cm_resolve_route,
    pfk_cm_qp_create,
    pfk_cm_connect,
    pfk_cm_accept,
    pfk_cm_reject,
    pfk_cm_fd,
    pfk_cm_next_event,
    pfk_cm_disconnect,
    pfk_cm_destroy,
    pfk_cm_set_sl,
};

#endif
