/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * struct plr_ops over libibverbs and struct plr_cm_ops over librdmacm, loaded with dlopen at the first use
 * (docs/design-rdma.md §5.4, D4): tsdbd links no RDMA library, so one binary and one tarball run on hosts with and
 * without rdma-core (there: one line, RDMA not available, and the links stay on TCP).
 *
 * Built only where rdma-core's headers were found (TSDB_RDMA for infiniband/verbs.h, TSDB_RDMA_CM for
 * rdma/rdma_cma.h, Makefile); elsewhere plrdma_verbs / plrdma_cm say so. The set-up calls are taken with dlsym under
 * their exported names (the default symbol version, what a link would bind) and called through pointers, never through
 * verbs.h's macros and inlines that name other exported symbols (ibv_get_device_list, ibv_reg_mr, ibv_query_port:
 * tools/check-rdma-syms.sh checks that tsdbd names no ibv_ or rdma_ symbol). The data-path calls (ibv_post_send,
 * ibv_post_recv, ibv_poll_cq, ibv_req_notify_cq) are inline in verbs.h and go through the context's own function
 * table: they need no symbol.
 *
 * librdmacm.so.1 is loaded lazily ONLY when plrdma_cm() is called (cluster.peer.rdma.cm=true): absence of librdmacm
 * never breaks normal verbs-over-TCP upgrade (plrdma_verbs). For CM mode, pools are opened on librdmacm's shared
 * device contexts (rdma_get_devices), so pool protection domains match rdma_cm_id->verbs in rdma_create_qp.
 *
 * Choices (plrdma.h has the design): no remote access on any queue pair (D9: SEND and RECV only, no rkey), a global
 * route only for RoCE or a GID of another subnet, InfiniBand within one subnet by LID; dev NULL takes the first active
 * port whose link layer is InfiniBand (D10; in CM mode also falls back to an active iWARP port since iWARP requires
 * rdma_cm).
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

const struct plr_cm_ops *plrdma_cm(char *err, size_t errlen)
{
    snprintf(err, errlen, "RDMA CM: not in this build (NO_RDMA, or rdma-core's headers were missing when it was built)");
    errno = ENOSYS;
    return NULL;
}

#else

#include <arpa/inet.h>
#include <dirent.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <infiniband/verbs.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>

#ifdef TSDB_RDMA_CM
#include <rdma/rdma_cma.h>
#endif

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

#ifdef TSDB_RDMA_CM
static struct {
    struct rdma_event_channel *(*create_event_channel)(void);
    void (*destroy_event_channel)(struct rdma_event_channel *);
    int (*create_id)(struct rdma_event_channel *, struct rdma_cm_id **, void *, enum rdma_port_space);
    int (*destroy_id)(struct rdma_cm_id *);
    int (*bind_addr)(struct rdma_cm_id *, struct sockaddr *);
    int (*listen)(struct rdma_cm_id *, int);
    int (*resolve_addr)(struct rdma_cm_id *, struct sockaddr *, struct sockaddr *, int);
    int (*resolve_route)(struct rdma_cm_id *, int);
    int (*create_qp)(struct rdma_cm_id *, struct ibv_pd *, struct ibv_qp_init_attr *);
    void (*destroy_qp)(struct rdma_cm_id *);
    int (*connect)(struct rdma_cm_id *, struct rdma_conn_param *);
    int (*accept)(struct rdma_cm_id *, struct rdma_conn_param *);
    int (*reject)(struct rdma_cm_id *, const void *, uint8_t);
    int (*disconnect)(struct rdma_cm_id *);
    int (*get_cm_event)(struct rdma_event_channel *, struct rdma_cm_event **);
    int (*ack_cm_event)(struct rdma_cm_event *);
    __be16 (*get_src_port)(struct rdma_cm_id *);
    struct ibv_context **(*get_devices)(int *);
    void (*free_devices)(struct ibv_context **);
    int (*migrate_id)(struct rdma_cm_id *, struct rdma_event_channel *);
    int (*set_option)(struct rdma_cm_id *, int, int, void *, size_t);
    const char *(*event_str)(enum rdma_cm_event_type);
} CM;

static const struct {
    const char *name;
    size_t off;
} cm_syms[] = {
#define S(f) {"rdma_" #f, offsetof(__typeof__(CM), f)}
    S(create_event_channel), S(destroy_event_channel), S(create_id),    S(destroy_id), S(bind_addr),
    S(listen),               S(resolve_addr),          S(resolve_route), S(create_qp), S(destroy_qp),
    S(connect),              S(accept),                S(reject),       S(disconnect), S(get_cm_event),
    S(ack_cm_event),         S(get_src_port),          S(get_devices),  S(free_devices), S(migrate_id),
    S(set_option),           S(event_str),
#undef S
};

static pthread_once_t cm_once = PTHREAD_ONCE_INIT;
static char cm_load_err[256];
static int cm_loaded;

static void cm_load(void)
{
    pthread_once(&once, load);
    if (!loaded) {
        snprintf(cm_load_err, sizeof cm_load_err, "%s", load_err);
        return;
    }
    void *h = dlopen("librdmacm.so.1", RTLD_NOW | RTLD_LOCAL);
    if (!h) {
        snprintf(cm_load_err, sizeof cm_load_err, "RDMA CM: not available here (librdmacm.so.1: %s)", dlerror());
        return;
    }
    for (size_t i = 0; i < sizeof cm_syms / sizeof cm_syms[0]; i++) {
        void *p = dlsym(h, cm_syms[i].name);
        if (!p) {
            snprintf(cm_load_err, sizeof cm_load_err, "RDMA CM: librdmacm.so.1 has no %s", cm_syms[i].name);
            return;
        }
        memcpy((char *)&CM + cm_syms[i].off, &p, sizeof p);
    }
    cm_loaded = 1;
}
#endif

struct plr_dev {
    struct ibv_context *ctx;
    struct ibv_pd *pd;
    uint8_t port;
    int gid_index;
    int owned_ctx; /* 1: opened via ibv_open_device; 0: borrowed from rdma_get_devices (never ibv_close_device) */
    int is_iwarp;  /* 1: IBV_TRANSPORT_IWARP (signal every send so SQ flush markers always complete on irdma) */
};

struct plr_mr {
    struct ibv_mr *mr;
};

struct plr_qp {
    struct plr_dev *d;
    struct ibv_comp_channel *ch;
    struct ibv_cq *cq;
    struct ibv_qp *qp;
#ifdef TSDB_RDMA_CM
    struct rdma_cm_id *cm_id; /* non-NULL when QP was created by rdma_create_qp */
#endif
};

/* Best-effort lookup of the netdev IP associated with an RDMA device+port (used by rdma_cm Option C to advertise
 * the RDMA-reachable IP in RDMA_ACCEPT). Reads gid_attrs/ndevs/0 (RoCE/iWARP), or for InfiniBand scans
 * /sys/class/infiniband/<dev>/device/net for an ARPHRD_INFINIBAND (32) interface. */
static int read_sysfs_line(const char *path, char *buf, size_t n)
{
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return -1;
    ssize_t r = read(fd, buf, n - 1);
    close(fd);
    if (r <= 0)
        return -1;
    buf[r] = 0;
    while (r > 0 && (buf[r - 1] == '\n' || buf[r - 1] == '\r' || buf[r - 1] == ' '))
        buf[--r] = 0;
    return r > 0 ? 0 : -1;
}

static void dev_find_cm_ip(const char *dev_name, uint8_t port, int is_ib, uint8_t *af_out, uint8_t ip_out[16])
{
    *af_out = 0;
    memset(ip_out, 0, 16);
    char ifname[64] = "";
    char path[256];
    snprintf(path, sizeof path, "/sys/class/infiniband/%s/ports/%u/gid_attrs/ndevs/0", dev_name, (unsigned)port);
    if (read_sysfs_line(path, ifname, sizeof ifname) != 0 && is_ib) {
        snprintf(path, sizeof path, "/sys/class/infiniband/%s/device/net", dev_name);
        DIR *dir = opendir(path);
        if (dir) {
            struct dirent *de;
            while ((de = readdir(dir)) != NULL) {
                if (de->d_name[0] == '.')
                    continue;
                char tp[320], tbuf[32];
                snprintf(tp, sizeof tp, "/sys/class/net/%s/type", de->d_name);
                if (read_sysfs_line(tp, tbuf, sizeof tbuf) == 0 && atoi(tbuf) == 32) {
                    snprintf(ifname, sizeof ifname, "%s", de->d_name);
                    break;
                }
            }
            closedir(dir);
        }
    }
    if (!ifname[0])
        return;
    struct ifaddrs *iflist = NULL;
    if (getifaddrs(&iflist) != 0)
        return;
    for (struct ifaddrs *ifa = iflist; ifa; ifa = ifa->ifa_next) {
        if (!ifa->ifa_addr || !ifa->ifa_name || strcmp(ifa->ifa_name, ifname) != 0)
            continue;
        if (ifa->ifa_addr->sa_family == AF_INET) {
            *af_out = 4;
            memcpy(ip_out, &((const struct sockaddr_in *)ifa->ifa_addr)->sin_addr, 4);
            break;
        }
        if (ifa->ifa_addr->sa_family == AF_INET6 && !*af_out) {
            const struct in6_addr *a6 = &((const struct sockaddr_in6 *)ifa->ifa_addr)->sin6_addr;
            if (!IN6_IS_ADDR_LINKLOCAL(a6)) {
                *af_out = 6;
                memcpy(ip_out, a6, 16);
            }
        }
    }
    freeifaddrs(iflist);
}

/* ibv_query_port without verbs.h's macro (which inlines a call of the exported compat entry) */
static int port_query(struct ibv_context *ctx, uint8_t port, struct ibv_port_attr *pa)
{
    struct verbs_context *vctx = verbs_get_ctx_op(ctx, query_port);
    memset(pa, 0, sizeof *pa);
    if (vctx)
        return vctx->query_port(ctx, port, pa, sizeof *pa);
    return V.query_port(ctx, port, (struct _compat_ibv_port_attr *)pa);
}

static int try_open_port(struct ibv_context *ctx, const char *name, int port, int allow_non_ib, int owned_ctx,
                         struct plr_dev **out, struct plr_port *pi, char *err, size_t errlen)
{
    struct ibv_device_attr da;
    int lo = port ? port : 1, hi = port;
    if (!port)
        hi = V.query_device(ctx, &da) == 0 ? da.phys_port_cnt : 1;
    for (int p = lo; p <= hi; p++) {
        struct ibv_port_attr pa;
        if (port_query(ctx, (uint8_t)p, &pa) != 0 || pa.state != IBV_PORT_ACTIVE)
            continue;
        int ib = pa.link_layer == IBV_LINK_LAYER_INFINIBAND || pa.link_layer == IBV_LINK_LAYER_UNSPECIFIED;
        if (!allow_non_ib && !ib)
            continue; /* D10: RoCE and iWARP only when named (or iWARP in CM pass) */
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
        d->owned_ctx = owned_ctx;
        d->is_iwarp = ctx->device && ctx->device->transport_type == IBV_TRANSPORT_IWARP;
        memset(pi, 0, sizeof *pi);
        pi->lid = pa.lid;
        memcpy(pi->gid, gid.raw, 16);
        pi->mtu = (uint8_t)pa.active_mtu;
        pi->link_layer = ib ? PLR_LL_IB : PLR_LL_ETH;
        pi->port = (uint8_t)p;
        dev_find_cm_ip(name, (uint8_t)p, ib, &pi->cm_af, pi->cm_ip);
        *out = d;
        return 0;
    }
    return -1;
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
        rc = try_open_port(ctx, name, port, dev != NULL, 1, out, pi, err, errlen);
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
    if (d->owned_ctx)
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
#ifdef TSDB_RDMA_CM
    if (q->cm_id) {
        if (q->cm_id->qp)
            CM.destroy_qp(q->cm_id);
        q->qp = NULL;
    }
#endif
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
    int disc_ok = 0;
#ifdef TSDB_RDMA_CM
    if (q->cm_id)
        disc_ok = CM.disconnect(q->cm_id) == 0;
#endif
    struct ibv_qp_attr a;
    memset(&a, 0, sizeof a);
    a.qp_state = IBV_QPS_ERR;
    int e = V.modify_qp(q->qp, &a, IBV_QP_STATE);
    if (e && !disc_ok) {
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
    wr.send_flags = (sig || q->d->is_iwarp) ? IBV_SEND_SIGNALED : 0;
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

#ifndef TSDB_RDMA_CM

const struct plr_cm_ops *plrdma_cm(char *err, size_t errlen)
{
    snprintf(err, errlen, "RDMA CM: not in this build (rdma/rdma_cma.h was missing when it was built)");
    errno = ENOSYS;
    return NULL;
}

#else

/* ---- librdmacm (Option C) implementation via dlopen */

struct plr_cm_id {
    struct rdma_event_channel *ch;
    struct rdma_cm_id *id;
};

static int dl_cm_dev_open(const char *dev, int port, struct plr_dev **out, struct plr_port *pi, char *err, size_t errlen)
{
    int n = 0;
    struct ibv_context **list = CM.get_devices(&n);
    if (!list) {
        snprintf(err, errlen, "RDMA CM: no devices (%s)", strerror(errno));
        return -1;
    }
    if (dev)
        snprintf(err, errlen, "RDMA CM: no device %s with an active port", dev);
    else
        snprintf(err, errlen, "RDMA CM: no active InfiniBand or iWARP port (RoCE only when named: D10)");
    int rc = -1;
    /* Pass 0: matching named device (any link layer), or unnamed active InfiniBand port (D10).
     * Pass 1 (unnamed only): active iWARP port (iWARP only connects via rdma_cm; RoCE still requires naming). */
    for (int pass = 0; pass < (dev ? 1 : 2) && rc < 0; pass++) {
        for (int i = 0; list[i] && rc < 0; i++) {
            struct ibv_context *ctx = list[i];
            const char *name = V.get_device_name(ctx->device);
            if (dev && strcmp(name, dev) != 0)
                continue;
            if (!dev && pass == 1 && ctx->device->transport_type != IBV_TRANSPORT_IWARP)
                continue;
            rc = try_open_port(ctx, name, port, dev != NULL || pass == 1, 0, out, pi, err, errlen);
        }
    }
    CM.free_devices(list);
    if (rc < 0)
        errno = ENODEV;
    return rc;
}

static const struct plr_ops dl_cm_verbs_ops = {"verbs-cm",   dl_cm_dev_open, dl_dev_close,  dl_reg,        dl_dereg,
                                               dl_qp_new,    dl_qp_rts,      dl_qp_err,     dl_qp_free,    dl_fd,
                                               dl_arm,       dl_take_events, dl_poll,       dl_post_send,  dl_post_recv};

static int cm_addr_to_sa(const struct plr_cm_addr *a, struct sockaddr_storage *ss)
{
    memset(ss, 0, sizeof *ss);
    if (!a || a->af == 0 || a->af == 4) {
        struct sockaddr_in *s4 = (struct sockaddr_in *)ss;
        s4->sin_family = AF_INET;
        s4->sin_port = htons(a ? a->port : 0);
        if (a && a->af == 4)
            memcpy(&s4->sin_addr, a->ip, 4);
        else
            s4->sin_addr.s_addr = INADDR_ANY;
        return 0;
    }
    if (a->af == 6) {
        struct sockaddr_in6 *s6 = (struct sockaddr_in6 *)ss;
        s6->sin6_family = AF_INET6;
        s6->sin6_port = htons(a->port);
        memcpy(&s6->sin6_addr, a->ip, 16);
        return 0;
    }
    errno = EAFNOSUPPORT;
    return -1;
}

static struct rdma_event_channel *cm_new_channel(void)
{
    struct rdma_event_channel *ch = CM.create_event_channel();
    if (!ch)
        return NULL;
    int fl = fcntl(ch->fd, F_GETFL);
    if (fl < 0 || fcntl(ch->fd, F_SETFL, fl | O_NONBLOCK) < 0 || fcntl(ch->fd, F_SETFD, FD_CLOEXEC) < 0) {
        int e = errno;
        CM.destroy_event_channel(ch);
        errno = e;
        return NULL;
    }
    return ch;
}

static struct plr_cm_id *dl_cm_listen(const struct plr_cm_addr *bind, struct plr_cm_addr *bound, char *err,
                                      size_t errlen)
{
    struct sockaddr_storage ss;
    if (cm_addr_to_sa(bind, &ss) != 0) {
        snprintf(err, errlen, "RDMA CM: bad bind address");
        return NULL;
    }
    struct plr_cm_id *cid = calloc(1, sizeof *cid);
    if (!cid) {
        snprintf(err, errlen, "RDMA CM: out of memory");
        return NULL;
    }
    if (!(cid->ch = cm_new_channel())) {
        snprintf(err, errlen, "RDMA CM: create_event_channel failed: %s", strerror(errno));
        free(cid);
        return NULL;
    }
    if (CM.create_id(cid->ch, &cid->id, NULL, RDMA_PS_TCP) != 0) {
        snprintf(err, errlen, "RDMA CM: create_id failed: %s", strerror(errno));
        CM.destroy_event_channel(cid->ch);
        free(cid);
        return NULL;
    }
    int one = 1;
    (void)CM.set_option(cid->id, RDMA_OPTION_ID, RDMA_OPTION_ID_REUSEADDR, &one, sizeof one);
    if (CM.bind_addr(cid->id, (struct sockaddr *)&ss) != 0) {
        snprintf(err, errlen, "RDMA CM: bind_addr failed: %s", strerror(errno));
        CM.destroy_id(cid->id);
        CM.destroy_event_channel(cid->ch);
        free(cid);
        return NULL;
    }
    if (CM.listen(cid->id, 64) != 0) {
        snprintf(err, errlen, "RDMA CM: listen failed: %s", strerror(errno));
        CM.destroy_id(cid->id);
        CM.destroy_event_channel(cid->ch);
        free(cid);
        return NULL;
    }
    if (bound) {
        memset(bound, 0, sizeof *bound);
        bound->port = ntohs(CM.get_src_port(cid->id));
        const struct sockaddr *la = rdma_get_local_addr(cid->id);
        if (la && la->sa_family == AF_INET6) {
            bound->af = 6;
            memcpy(bound->ip, &((const struct sockaddr_in6 *)la)->sin6_addr, 16);
        } else {
            bound->af = 4;
            if (la && la->sa_family == AF_INET)
                memcpy(bound->ip, &((const struct sockaddr_in *)la)->sin_addr, 4);
        }
    }
    return cid;
}

static struct plr_cm_id *dl_cm_dial(const struct plr_cm_addr *dst, int timeout_ms, char *err, size_t errlen)
{
    struct sockaddr_storage ss;
    if (!dst || !dst->port || cm_addr_to_sa(dst, &ss) != 0) {
        snprintf(err, errlen, "RDMA CM: bad destination address");
        errno = EINVAL;
        return NULL;
    }
    struct plr_cm_id *cid = calloc(1, sizeof *cid);
    if (!cid) {
        snprintf(err, errlen, "RDMA CM: out of memory");
        return NULL;
    }
    if (!(cid->ch = cm_new_channel())) {
        snprintf(err, errlen, "RDMA CM: create_event_channel failed: %s", strerror(errno));
        free(cid);
        return NULL;
    }
    if (CM.create_id(cid->ch, &cid->id, NULL, RDMA_PS_TCP) != 0) {
        snprintf(err, errlen, "RDMA CM: create_id failed: %s", strerror(errno));
        CM.destroy_event_channel(cid->ch);
        free(cid);
        return NULL;
    }
    if (CM.resolve_addr(cid->id, NULL, (struct sockaddr *)&ss, timeout_ms > 0 ? timeout_ms : 2000) != 0) {
        int e = errno;
        snprintf(err, errlen, "RDMA CM: resolve_addr failed: %s", strerror(e));
        CM.destroy_id(cid->id);
        CM.destroy_event_channel(cid->ch);
        free(cid);
        errno = e;
        return NULL;
    }
    return cid;
}

static int dl_cm_resolve_route(struct plr_cm_id *cid, int timeout_ms)
{
    return CM.resolve_route(cid->id, timeout_ms > 0 ? timeout_ms : 2000) == 0 ? 0 : -1;
}

static struct plr_qp *dl_cm_qp_create(struct plr_cm_id *cid, struct plr_dev *d, int sq, int rq, uint32_t *qpn)
{
    if (!cid->id->verbs || cid->id->verbs != d->ctx) {
        errno = EXDEV;
        return NULL;
    }
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
    if (CM.create_qp(cid->id, d->pd, &ia) != 0)
        goto fail;
    q->cm_id = cid->id;
    q->qp = cid->id->qp;
    *qpn = q->qp->qp_num;
    return q;
fail:
    e = errno ? errno : EIO;
    qp_destroy(q);
    errno = e;
    return NULL;
}

static int dl_cm_connect(struct plr_cm_id *cid, const void *pd, uint8_t pd_len, uint8_t rnr_retry, uint8_t retry_cnt)
{
    struct rdma_conn_param cp;
    memset(&cp, 0, sizeof cp);
    cp.private_data = pd;
    cp.private_data_len = pd_len;
    cp.responder_resources = 1;
    cp.initiator_depth = 1;
    cp.flow_control = 1;
    cp.retry_count = retry_cnt;
    cp.rnr_retry_count = rnr_retry;
    return CM.connect(cid->id, &cp) == 0 ? 0 : -1;
}

static int dl_cm_accept(struct plr_cm_id *cid, uint8_t rnr_retry)
{
    struct rdma_conn_param cp;
    memset(&cp, 0, sizeof cp);
    cp.responder_resources = 1;
    cp.initiator_depth = 1;
    cp.flow_control = 1;
    cp.rnr_retry_count = rnr_retry;
    return CM.accept(cid->id, &cp) == 0 ? 0 : -1;
}

static void dl_cm_destroy(struct plr_cm_id *cid)
{
    if (!cid)
        return;
    /* Ack any queued events so rdma_destroy_id never blocks on unacked events. */
    struct rdma_cm_event *ev = NULL;
    while (cid->ch && CM.get_cm_event(cid->ch, &ev) == 0) {
        struct rdma_cm_id *child = (ev->event == RDMA_CM_EVENT_CONNECT_REQUEST) ? ev->id : NULL;
        CM.ack_cm_event(ev);
        if (child && child != cid->id) {
            CM.reject(child, NULL, 0);
            CM.destroy_id(child);
        }
    }
    if (cid->id)
        CM.destroy_id(cid->id);
    if (cid->ch)
        CM.destroy_event_channel(cid->ch);
    free(cid);
}

static void dl_cm_reject(struct plr_cm_id *cid)
{
    if (!cid)
        return;
    if (cid->id)
        CM.reject(cid->id, NULL, 0);
    dl_cm_destroy(cid);
}

static int dl_cm_fd(struct plr_cm_id *cid) { return cid->ch->fd; }

static int dl_cm_next_event(struct plr_cm_id *cid, struct plr_cm_ev *out)
{
    memset(out, 0, sizeof *out);
    struct rdma_cm_event *ev = NULL;
    if (CM.get_cm_event(cid->ch, &ev) != 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return 0;
        return -1;
    }
    out->status = ev->status;
    enum rdma_cm_event_type et = ev->event;
    struct rdma_cm_id *req_raw = (et == RDMA_CM_EVENT_CONNECT_REQUEST) ? ev->id : NULL;
    if (et == RDMA_CM_EVENT_CONNECT_REQUEST && ev->param.conn.private_data && ev->param.conn.private_data_len) {
        uint8_t n = ev->param.conn.private_data_len;
        if (n > sizeof out->pd)
            n = (uint8_t)sizeof out->pd;
        memcpy(out->pd, ev->param.conn.private_data, n);
        out->pd_len = n;
    }
    CM.ack_cm_event(ev);
    switch (et) {
    case RDMA_CM_EVENT_ADDR_RESOLVED:
        out->type = PLR_CM_EV_ADDR_RESOLVED;
        return 1;
    case RDMA_CM_EVENT_ROUTE_RESOLVED:
        out->type = PLR_CM_EV_ROUTE_RESOLVED;
        return 1;
    case RDMA_CM_EVENT_CONNECT_REQUEST: {
        struct plr_cm_id *req = calloc(1, sizeof *req);
        struct rdma_event_channel *rch = req ? cm_new_channel() : NULL;
        if (!req || !rch || CM.migrate_id(req_raw, rch) != 0) {
            CM.reject(req_raw, NULL, 0);
            CM.destroy_id(req_raw);
            if (rch)
                CM.destroy_event_channel(rch);
            free(req);
            return 0;
        }
        req->ch = rch;
        req->id = req_raw;
        out->type = PLR_CM_EV_CONNECT_REQUEST;
        out->req_id = req;
        return 1;
    }
    case RDMA_CM_EVENT_ESTABLISHED:
        out->type = PLR_CM_EV_ESTABLISHED;
        return 1;
    case RDMA_CM_EVENT_DISCONNECTED:
        out->type = PLR_CM_EV_DISCONNECTED;
        return 1;
    case RDMA_CM_EVENT_REJECTED:
        out->type = PLR_CM_EV_REJECTED;
        return 1;
    case RDMA_CM_EVENT_ADDR_ERROR:
    case RDMA_CM_EVENT_ROUTE_ERROR:
    case RDMA_CM_EVENT_CONNECT_ERROR:
    case RDMA_CM_EVENT_UNREACHABLE:
    case RDMA_CM_EVENT_DEVICE_REMOVAL:
    case RDMA_CM_EVENT_ADDR_CHANGE:
        out->type = PLR_CM_EV_ERR;
        return 1;
    default:
        out->type = PLR_CM_EV_NONE;
        return 1;
    }
}

static int dl_cm_disconnect(struct plr_cm_id *cid)
{
    return CM.disconnect(cid->id) == 0 ? 0 : -1;
}

static int dl_cm_set_sl(struct plr_cm_id *cid, uint8_t sl)
{
    uint8_t tos = (uint8_t)((sl & 0x0f) << 5);
    (void)CM.set_option(cid->id, RDMA_OPTION_ID, RDMA_OPTION_ID_TOS, &tos, sizeof tos);
    return 0;
}

static const struct plr_cm_ops dl_cm_ops = {
    "rdma-cm",
    &dl_cm_verbs_ops,
    dl_cm_listen,
    dl_cm_dial,
    dl_cm_resolve_route,
    dl_cm_qp_create,
    dl_cm_connect,
    dl_cm_accept,
    dl_cm_reject,
    dl_cm_fd,
    dl_cm_next_event,
    dl_cm_disconnect,
    dl_cm_destroy,
    dl_cm_set_sl,
};

const struct plr_cm_ops *plrdma_cm(char *err, size_t errlen)
{
    pthread_once(&cm_once, cm_load);
    if (!cm_loaded) {
        snprintf(err, errlen, "%s", cm_load_err);
        errno = ENOSYS;
        return NULL;
    }
    return &dl_cm_ops;
}

#endif /* TSDB_RDMA_CM */
#endif /* TSDB_RDMA */
