/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * struct plr_ops over libibverbs, loaded with dlopen at the first use (docs/design-rdma.md §5.4, D4): tsdbd links no
 * RDMA library, so one binary and one tarball run on hosts with and without rdma-core (there: one line, RDMA not
 * available, and the links stay on TCP).
 *
 * Built only where rdma-core's headers were found (TSDB_RDMA, Makefile); elsewhere plrdma_verbs says so. The set-up
 * calls are taken with dlsym under their exported names (the default symbol version, what a link would bind) and
 * called through pointers, never through verbs.h's macros and inlines that name other exported symbols
 * (ibv_get_device_list, ibv_reg_mr, ibv_query_port: tools/check-rdma-syms.sh checks that tsdbd names no ibv_ or
 * rdma_ symbol). The data-path calls (ibv_post_send, ibv_post_recv, ibv_poll_cq, ibv_req_notify_cq) are inline in
 * verbs.h and go through the context's own function table: they need no symbol.
 *
 * Choices (plrdma.h has the design): no remote access on any queue pair (D9: SEND and RECV only, no rkey), a global
 * route only for RoCE or a GID of another subnet, InfiniBand within one subnet by LID; dev NULL takes the first active
 * port whose link layer is InfiniBand (D10). librdmacm (option C, rd-cm) is not loaded here.
 */
#include "plrdma.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>

#ifndef TSDB_RDMA

const struct plr_ops *plrdma_verbs(char *err, size_t errlen)
{
    snprintf(err, errlen, "RDMA: not in this build (NO_RDMA, or rdma-core's headers were missing when it was built)");
    errno = ENOSYS;
    return NULL;
}

#else

#include <dlfcn.h>
#include <fcntl.h>
#include <infiniband/verbs.h>
#include <pthread.h>
#include <stdlib.h>

static struct {
    struct ibv_device **(*get_device_list)(int *);
    void (*free_device_list)(struct ibv_device **);
    const char *(*get_device_name)(struct ibv_device *);
    struct ibv_context *(*open_device)(struct ibv_device *);
    int (*close_device)(struct ibv_context *);
    int (*query_device)(struct ibv_context *, struct ibv_device_attr *);
    int (*query_port)(struct ibv_context *, uint8_t, struct _compat_ibv_port_attr *);
    int (*query_gid)(struct ibv_context *, uint8_t, int, union ibv_gid *);
    struct ibv_pd *(*alloc_pd)(struct ibv_context *);
    int (*dealloc_pd)(struct ibv_pd *);
    struct ibv_mr *(*reg_mr)(struct ibv_pd *, void *, size_t, int);
    int (*dereg_mr)(struct ibv_mr *);
    struct ibv_comp_channel *(*create_comp_channel)(struct ibv_context *);
    int (*destroy_comp_channel)(struct ibv_comp_channel *);
    struct ibv_cq *(*create_cq)(struct ibv_context *, int, void *, struct ibv_comp_channel *, int);
    int (*destroy_cq)(struct ibv_cq *);
    struct ibv_qp *(*create_qp)(struct ibv_pd *, struct ibv_qp_init_attr *);
    int (*modify_qp)(struct ibv_qp *, struct ibv_qp_attr *, int);
    int (*destroy_qp)(struct ibv_qp *);
    int (*get_cq_event)(struct ibv_comp_channel *, struct ibv_cq **, void **);
    void (*ack_cq_events)(struct ibv_cq *, unsigned int);
} V;

static const struct {
    const char *name;
    size_t off;
} syms[] = {
#define S(f) {"ibv_" #f, offsetof(__typeof__(V), f)}
    S(get_device_list), S(free_device_list), S(get_device_name), S(open_device), S(close_device), S(query_device),
    S(query_port),      S(query_gid),        S(alloc_pd),        S(dealloc_pd),  S(reg_mr),       S(dereg_mr),
    S(create_comp_channel), S(destroy_comp_channel), S(create_cq), S(destroy_cq), S(create_qp),   S(modify_qp),
    S(destroy_qp),      S(get_cq_event),     S(ack_cq_events),
#undef S
};

static pthread_once_t once = PTHREAD_ONCE_INIT;
static char load_err[256];
static int loaded;

static void load(void)
{
    void *h = dlopen("libibverbs.so.1", RTLD_NOW | RTLD_LOCAL);
    if (!h) {
        snprintf(load_err, sizeof load_err, "RDMA: not available here (libibverbs.so.1: %s)", dlerror());
        return;
    }
    for (size_t i = 0; i < sizeof syms / sizeof syms[0]; i++) {
        void *p = dlsym(h, syms[i].name);
        if (!p) {
            snprintf(load_err, sizeof load_err, "RDMA: libibverbs.so.1 has no %s", syms[i].name);
            return; /* the library stays loaded: harmless, and it may be in use by another part of the process */
        }
        memcpy((char *)&V + syms[i].off, &p, sizeof p);
    }
    loaded = 1;
}

struct plr_dev {
    struct ibv_context *ctx;
    struct ibv_pd *pd;
    uint8_t port;
    int gid_index;
};

struct plr_mr {
    struct ibv_mr *mr;
};

struct plr_qp {
    struct plr_dev *d;
    struct ibv_comp_channel *ch;
    struct ibv_cq *cq;
    struct ibv_qp *qp;
};

/* ibv_query_port without verbs.h's macro (which inlines a call of the exported compat entry) */
static int port_query(struct ibv_context *ctx, uint8_t port, struct ibv_port_attr *pa)
{
    struct verbs_context *vctx = verbs_get_ctx_op(ctx, query_port);
    memset(pa, 0, sizeof *pa);
    if (vctx)
        return vctx->query_port(ctx, port, pa, sizeof *pa);
    return V.query_port(ctx, port, (struct _compat_ibv_port_attr *)pa);
}

static int dl_dev_open(const char *dev, int port, struct plr_dev **out, struct plr_port *pi, char *err, size_t errlen)
{
    int n = 0;
    struct ibv_device **list = V.get_device_list(&n);
    if (!list) {
        snprintf(err, errlen, "RDMA: no devices (%s)", strerror(errno));
        return -1;
    }
    if (dev)
        snprintf(err, errlen, "RDMA: no device %s with an active port", dev);
    else
        snprintf(err, errlen, "RDMA: no active InfiniBand port (another link layer only when named: D10)");
    int rc = -1;
    for (int i = 0; i < n && rc < 0; i++) {
        const char *name = V.get_device_name(list[i]);
        if (dev && strcmp(name, dev) != 0)
            continue;
        struct ibv_context *ctx = V.open_device(list[i]);
        if (!ctx)
            continue;
        struct ibv_device_attr da;
        int lo = port ? port : 1, hi = port;
        if (!port)
            hi = V.query_device(ctx, &da) == 0 ? da.phys_port_cnt : 1;
        for (int p = lo; p <= hi && rc < 0; p++) {
            struct ibv_port_attr pa;
            if (port_query(ctx, (uint8_t)p, &pa) != 0 || pa.state != IBV_PORT_ACTIVE)
                continue;
            int ib = pa.link_layer == IBV_LINK_LAYER_INFINIBAND || pa.link_layer == IBV_LINK_LAYER_UNSPECIFIED;
            if (!dev && !ib)
                continue; /* D10: RoCE and iWARP only when named */
            union ibv_gid gid;
            if (V.query_gid(ctx, (uint8_t)p, 0, &gid) != 0)
                continue;
            struct ibv_pd *pd = V.alloc_pd(ctx);
            if (!pd) {
                snprintf(err, errlen, "RDMA: %s: no protection domain (%s)", name, strerror(errno));
                continue;
            }
            struct plr_dev *d = calloc(1, sizeof *d);
            if (!d) {
                V.dealloc_pd(pd);
                continue;
            }
            d->ctx = ctx;
            d->pd = pd;
            d->port = (uint8_t)p;
            memset(pi, 0, sizeof *pi);
            pi->lid = pa.lid;
            memcpy(pi->gid, gid.raw, 16);
            pi->mtu = (uint8_t)pa.active_mtu;
            pi->link_layer = ib ? PLR_LL_IB : PLR_LL_ETH;
            pi->port = (uint8_t)p;
            *out = d;
            rc = 0;
        }
        if (rc < 0)
            V.close_device(ctx);
    }
    V.free_device_list(list);
    if (rc < 0)
        errno = ENODEV;
    return rc;
}

static void dl_dev_close(struct plr_dev *d)
{
    if (!d)
        return;
    V.dealloc_pd(d->pd);
    V.close_device(d->ctx);
    free(d);
}

static struct plr_mr *dl_reg(struct plr_dev *d, void *p, size_t n, uint32_t *lkey)
{
    struct plr_mr *m = calloc(1, sizeof *m);
    if (!m)
        return NULL;
    m->mr = V.reg_mr(d->pd, p, n, IBV_ACCESS_LOCAL_WRITE); /* no remote access (D9) */
    if (!m->mr) {
        int e = errno;
        free(m);
        errno = e;
        return NULL;
    }
    *lkey = m->mr->lkey;
    return m;
}

static void dl_dereg(struct plr_mr *m)
{
    if (!m)
        return;
    V.dereg_mr(m->mr);
    free(m);
}

static void qp_destroy(struct plr_qp *q)
{
    if (q->qp)
        V.destroy_qp(q->qp);
    if (q->cq)
        V.destroy_cq(q->cq);
    if (q->ch)
        V.destroy_comp_channel(q->ch);
    free(q);
}

static struct plr_qp *dl_qp_new(struct plr_dev *d, int sq, int rq, uint32_t *qpn)
{
    struct plr_qp *q = calloc(1, sizeof *q);
    if (!q)
        return NULL;
    q->d = d;
    int e = 0;
    if (!(q->ch = V.create_comp_channel(d->ctx)) || fcntl(q->ch->fd, F_SETFL, fcntl(q->ch->fd, F_GETFL) | O_NONBLOCK))
        goto fail;
    if (!(q->cq = V.create_cq(d->ctx, sq + rq + 2, NULL, q->ch, 0)))
        goto fail;
    struct ibv_qp_init_attr ia;
    memset(&ia, 0, sizeof ia);
    ia.send_cq = q->cq;
    ia.recv_cq = q->cq;
    ia.cap.max_send_wr = (uint32_t)sq;
    ia.cap.max_recv_wr = (uint32_t)rq;
    ia.cap.max_send_sge = 1;
    ia.cap.max_recv_sge = 1;
    ia.qp_type = IBV_QPT_RC;
    ia.sq_sig_all = 0;
    if (!(q->qp = V.create_qp(d->pd, &ia)))
        goto fail;
    struct ibv_qp_attr a;
    memset(&a, 0, sizeof a);
    a.qp_state = IBV_QPS_INIT;
    a.pkey_index = 0;
    a.port_num = d->port;
    a.qp_access_flags = 0; /* no remote access (D9) */
    if ((e = V.modify_qp(q->qp, &a, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS)) != 0)
        goto fail;
    *qpn = q->qp->qp_num;
    return q;
fail:
    e = e ? e : errno;
    qp_destroy(q);
    errno = e ? e : EIO;
    return NULL;
}

static int dl_qp_rts(struct plr_qp *q, const struct plr_qp_attr *r)
{
    struct ibv_qp_attr a;
    memset(&a, 0, sizeof a);
    a.qp_state = IBV_QPS_RTR;
    a.path_mtu = (enum ibv_mtu)r->mtu;
    a.dest_qp_num = r->qpn;
    a.rq_psn = r->rq_psn;
    a.max_dest_rd_atomic = 1;
    a.min_rnr_timer = 12;
    a.ah_attr.dlid = r->lid;
    a.ah_attr.sl = r->sl;
    a.ah_attr.port_num = q->d->port;
    if (r->use_gid) {
        a.ah_attr.is_global = 1;
        memcpy(a.ah_attr.grh.dgid.raw, r->gid, 16);
        a.ah_attr.grh.sgid_index = (uint8_t)q->d->gid_index;
        a.ah_attr.grh.hop_limit = 64;
    }
    int e = V.modify_qp(q->qp, &a,
                        IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                            IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);
    if (!e) {
        memset(&a, 0, sizeof a);
        a.qp_state = IBV_QPS_RTS;
        a.timeout = r->timeout;
        a.retry_cnt = r->retry_cnt;
        a.rnr_retry = r->rnr_retry;
        a.sq_psn = r->sq_psn;
        a.max_rd_atomic = 1;
        e = V.modify_qp(q->qp, &a,
                        IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                            IBV_QP_MAX_QP_RD_ATOMIC);
    }
    if (e) {
        errno = e;
        return -1;
    }
    return 0;
}

static int dl_qp_err(struct plr_qp *q)
{
    struct ibv_qp_attr a;
    memset(&a, 0, sizeof a);
    a.qp_state = IBV_QPS_ERR;
    int e = V.modify_qp(q->qp, &a, IBV_QP_STATE);
    if (e) {
        errno = e;
        return -1;
    }
    return 0;
}

static void dl_qp_free(struct plr_qp *q) { qp_destroy(q); }

static int dl_fd(struct plr_qp *q) { return q->ch->fd; }

static int dl_arm(struct plr_qp *q)
{
    int e = ibv_req_notify_cq(q->cq, 0);
    if (e) {
        errno = e;
        return -1;
    }
    return 0;
}

static int dl_take_events(struct plr_qp *q)
{
    int n = 0;
    struct ibv_cq *cq;
    void *ctx;
    while (V.get_cq_event(q->ch, &cq, &ctx) == 0) {
        V.ack_cq_events(cq, 1); /* at once: ibv_destroy_cq waits for every event to be acknowledged */
        n++;
    }
    if (errno != EAGAIN && errno != EWOULDBLOCK)
        return -1;
    return n;
}

static int dl_poll(struct plr_qp *q, struct plr_wc *out, int n)
{
    struct ibv_wc wc[16];
    if (n > 16)
        n = 16;
    int k = ibv_poll_cq(q->cq, n, wc);
    if (k < 0) {
        errno = EIO;
        return -1;
    }
    for (int i = 0; i < k; i++) {
        /* on an error the opcode is not valid: the transport goes by wr_id */
        out[i].wr_id = wc[i].wr_id;
        out[i].op = wc[i].opcode & IBV_WC_RECV ? PLR_WC_RECV : PLR_WC_SEND;
        out[i].status = wc[i].status == IBV_WC_SUCCESS        ? PLR_WC_OK
                        : wc[i].status == IBV_WC_WR_FLUSH_ERR ? PLR_WC_FLUSH
                                                              : PLR_WC_ERR;
        out[i].byte_len = wc[i].byte_len;
        out[i].vendor = (int)wc[i].status;
        out[i].rnr = (uint8_t)(wc[i].status == IBV_WC_RNR_RETRY_EXC_ERR);
    }
    return k;
}

static int dl_post_send(struct plr_qp *q, uint64_t id, const void *p, uint32_t n, uint32_t lkey, int sig)
{
    struct ibv_sge sge = {(uintptr_t)p, n, lkey};
    struct ibv_send_wr wr, *bad;
    memset(&wr, 0, sizeof wr);
    wr.wr_id = id;
    wr.sg_list = n ? &sge : NULL;
    wr.num_sge = n ? 1 : 0;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = sig ? IBV_SEND_SIGNALED : 0;
    int e = ibv_post_send(q->qp, &wr, &bad);
    if (e) {
        errno = e;
        return -1;
    }
    return 0;
}

static int dl_post_recv(struct plr_qp *q, uint64_t id, void *p, uint32_t n, uint32_t lkey)
{
    struct ibv_sge sge = {(uintptr_t)p, n, lkey};
    struct ibv_recv_wr wr, *bad;
    memset(&wr, 0, sizeof wr);
    wr.wr_id = id;
    wr.sg_list = n ? &sge : NULL;
    wr.num_sge = n ? 1 : 0;
    int e = ibv_post_recv(q->qp, &wr, &bad);
    if (e) {
        errno = e;
        return -1;
    }
    return 0;
}

static const struct plr_ops dl_ops = {"verbs",     dl_dev_open, dl_dev_close,   dl_reg,  dl_dereg,
                                      dl_qp_new,   dl_qp_rts,   dl_qp_err,      dl_qp_free, dl_fd,
                                      dl_arm,      dl_take_events, dl_poll,     dl_post_send, dl_post_recv};

const struct plr_ops *plrdma_verbs(char *err, size_t errlen)
{
    pthread_once(&once, load);
    if (!loaded) {
        snprintf(err, errlen, "%s", load_err);
        errno = ENOSYS;
        return NULL;
    }
    return &dl_ops;
}

#endif
