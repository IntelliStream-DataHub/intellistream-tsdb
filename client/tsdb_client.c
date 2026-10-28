/* Client for tsdbd: blocking sockets, columnar frames, optional pipelining. */
#define _GNU_SOURCE
#include "tsdb_calendar.h"
#include "tsdb_client.h"
#include "tsdb_pack.h"
#include "tsdb_protocol.h"

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdbool.h>
#include <stdio.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>
#include <zstd.h>

#define INSERT_FRAME_ROWS (8u << 20) /* 128 MiB frames at most */
#define RBUF (1u << 20)

struct tsdbc {
    int fd;
    uint32_t next_id;
    size_t pending;  /* acknowledgements not yet read */
    int pending_rc;  /* first error among them */
    uint8_t *rbuf;   /* receive buffer */
    size_t rlen, roff, rcap;
    uint8_t *frame;  /* the body of the last frame read */
    size_t fcap;
    /* packed inserts (protocol version 3) */
    int zlevel;      /* zstd level; 0: send plain arrays */
    uint32_t server_version; /* from HELLO; 0 until asked */
    ZSTD_CCtx *zc;
    uint8_t *raw, *zbuf;
    size_t raw_cap, zbuf_cap;
    uint64_t sent;   /* bytes written to the socket */
    uint32_t expect; /* the id of the oldest request whose reply is not read yet */
    /* Set by an I/O error (a timeout included) or a reply out of step: part of
     * a frame may be unsent or unread, so the next reply read could answer an
     * earlier request. Every later call fails with TSDB_ERR_IO. */
    bool broken;
};

/* Waits up to ms (0: as long as the kernel does) for a connect to finish. */
static int connect_within(int fd, const struct sockaddr *addr, socklen_t len, int ms)
{
    if (ms <= 0)
        return connect(fd, addr, len);
    int fl = fcntl(fd, F_GETFL);
    if (fl < 0 || fcntl(fd, F_SETFL, fl | O_NONBLOCK) != 0)
        return -1;
    int rc = connect(fd, addr, len);
    if (rc != 0 && errno == EINPROGRESS) {
        struct pollfd p = {.fd = fd, .events = POLLOUT};
        int r;
        while ((r = poll(&p, 1, ms)) < 0 && errno == EINTR)
            ; /* a signal restarts the whole wait: bounded by the caller's patience, not exact */
        int err = 0;
        socklen_t el = sizeof err;
        if (r == 1 && getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &el) == 0 && err == 0)
            rc = 0;
        else
            errno = r == 0 ? ETIMEDOUT : err ? err : errno;
    }
    if (fcntl(fd, F_SETFL, fl) != 0)
        rc = -1;
    return rc;
}

int tsdbc_set_keepalive(tsdbc *c, int idle_s, int interval_s, int count)
{
    if (idle_s < 0 || interval_s < 0 || count < 0 || (idle_s && (!interval_s || !count)))
        return TSDB_ERR_INVALID;
    int on = idle_s > 0;
    if (setsockopt(c->fd, SOL_SOCKET, SO_KEEPALIVE, &on, sizeof on) != 0)
        return TSDB_ERR_IO;
    if (on && (setsockopt(c->fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle_s, sizeof idle_s) != 0 ||
               setsockopt(c->fd, IPPROTO_TCP, TCP_KEEPINTVL, &interval_s, sizeof interval_s) != 0 ||
               setsockopt(c->fd, IPPROTO_TCP, TCP_KEEPCNT, &count, sizeof count) != 0))
        return TSDB_ERR_IO;
    return 0;
}

int tsdbc_connect(const char *host, int port, tsdbc **out)
{
    return tsdbc_connect_timeout(host, port, TSDBC_CONNECT_TIMEOUT_MS, out);
}

int tsdbc_connect_timeout(const char *host, int port, int connect_ms, tsdbc **out)
{
    *out = NULL;
    if (connect_ms < 0)
        return TSDB_ERR_INVALID;
    char portstr[16];
    snprintf(portstr, sizeof portstr, "%d", port);
    struct addrinfo hints = {.ai_socktype = SOCK_STREAM}, *ai;
    if (getaddrinfo(host, portstr, &hints, &ai) != 0)
        return TSDB_ERR_IO;
    int fd = -1;
    for (struct addrinfo *a = ai; a; a = a->ai_next) {
        fd = socket(a->ai_family, a->ai_socktype | SOCK_CLOEXEC, a->ai_protocol);
        if (fd >= 0 && connect_within(fd, a->ai_addr, a->ai_addrlen, connect_ms) == 0)
            break;
        if (fd >= 0)
            close(fd);
        fd = -1;
    }
    freeaddrinfo(ai);
    if (fd < 0)
        return TSDB_ERR_IO;
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    tsdbc *c = calloc(1, sizeof *c);
    if (!c || !(c->rbuf = malloc(RBUF))) {
        free(c);
        close(fd);
        return TSDB_ERR_NOMEM;
    }
    c->fd = fd;
    c->rcap = RBUF;
    c->zlevel = 1;
    /* Best effort, as in the other clients: a socket without the probe options still works. */
    tsdbc_set_keepalive(c, TSDBC_KEEPALIVE_IDLE_S, TSDBC_KEEPALIVE_INTERVAL_S, TSDBC_KEEPALIVE_COUNT);
    *out = c;
    return 0;
}

const char *tsdbc_strerror(int err)
{
    switch (err) {
    case TSDB_OK: return "ok";
    case TSDB_ERR_IO: return "I/O error (or connection lost)";
    case TSDB_ERR_NOMEM: return "out of memory";
    case TSDB_ERR_CORRUPT: return "corrupt data or protocol error";
    case TSDB_ERR_INVALID: return "invalid argument";
    case TSDB_ERR_LOCKED: return "database is locked by another process";
    case TSDB_ERR_NOTFOUND: return "not found";
    case TSDB_ERR_TOO_MANY_BUCKETS: return "too many buckets";
    case TSDB_ERR_STOPPED: return "stopped";
    case TSDB_ERR_READONLY: return "database is read only";
    default: return "unknown error";
    }
}

void tsdbc_close(tsdbc *c)
{
    if (!c)
        return;
    /* Closing with acknowledgements unread makes the kernel reset the connection,
     * which discards requests the server has not read yet: collect them first. */
    if (c->pending && !c->broken)
        tsdbc_wait(c);
    close(c->fd);
    free(c->rbuf);
    free(c->frame);
    ZSTD_freeCCtx(c->zc);
    free(c->raw);
    free(c->zbuf);
    free(c);
}

/* ---------------------------------------------------------------- I/O */

static int send_iov(tsdbc *c, struct iovec *iov, int n)
{
    if (c->broken)
        return TSDB_ERR_IO;
    while (n) {
        /* sendmsg with MSG_NOSIGNAL, not writev: a peer that closed the connection must not kill the
         * host process with SIGPIPE; the send fails with EPIPE and the connection is broken. */
        struct msghdr m = {.msg_iov = iov, .msg_iovlen = (size_t)n};
        ssize_t w = sendmsg(c->fd, &m, MSG_NOSIGNAL);
        if (w < 0) {
            if (errno == EINTR)
                continue;
            c->broken = true; /* part of the frame may be on its way */
            return TSDB_ERR_IO;
        }
        c->sent += (uint64_t)w;
        while (n && (size_t)w >= iov->iov_len) {
            w -= (ssize_t)iov->iov_len;
            iov++;
            n--;
        }
        if (n) {
            iov->iov_base = (uint8_t *)iov->iov_base + w;
            iov->iov_len -= (size_t)w;
        }
    }
    return 0;
}

/* Sends one request frame made of up to four payload pieces. */
static int send_req(tsdbc *c, uint8_t op, const void *a, size_t alen, const void *b, size_t blen, const void *d,
                    size_t dlen, const void *e, size_t elen)
{
    uint8_t hdr[TSDB_FRAME_HEADER];
    uint32_t len = (uint32_t)(5 + alen + blen + dlen + elen), id = c->next_id++;
    memcpy(hdr, &len, 4);
    memcpy(hdr + 4, &id, 4);
    hdr[8] = op;
    struct iovec iov[5] = {{hdr, sizeof hdr}, {(void *)a, alen}, {(void *)b, blen}, {(void *)d, dlen},
                           {(void *)e, elen}};
    return send_iov(c, iov, 5);
}

/* Sends one request frame made of the pieces parts[1..n) (parts[0] is filled
 * with the frame header). */
static int send_reqv(tsdbc *c, uint8_t op, struct iovec *parts, int n)
{
    uint8_t hdr[TSDB_FRAME_HEADER];
    size_t body = 5;
    for (int i = 1; i < n; i++)
        body += parts[i].iov_len;
    uint32_t len = (uint32_t)body, id = c->next_id++;
    memcpy(hdr, &len, 4);
    memcpy(hdr + 4, &id, 4);
    hdr[8] = op;
    parts[0] = (struct iovec){hdr, sizeof hdr};
    return send_iov(c, parts, n);
}

static int read_exact(tsdbc *c, void *dst, size_t n)
{
    if (c->broken)
        return TSDB_ERR_IO;
    uint8_t *o = dst;
    while (n) {
        if (c->roff == c->rlen) {
            if (n >= c->rcap) { /* large bodies straight into place */
                ssize_t r = recv(c->fd, o, n, 0);
                if (r <= 0) {
                    if (r < 0 && errno == EINTR)
                        continue;
                    c->broken = true;
                    return TSDB_ERR_IO;
                }
                o += r;
                n -= (size_t)r;
                continue;
            }
            ssize_t r = recv(c->fd, c->rbuf, c->rcap, 0);
            if (r <= 0) {
                if (r < 0 && errno == EINTR)
                    continue;
                c->broken = true;
                return TSDB_ERR_IO;
            }
            c->rlen = (size_t)r;
            c->roff = 0;
        }
        size_t take = c->rlen - c->roff < n ? c->rlen - c->roff : n;
        memcpy(o, c->rbuf + c->roff, take);
        c->roff += take;
        o += take;
        n -= take;
    }
    return 0;
}

/* Reads one reply frame; *body points at the payload after the status. The
 * reply must answer the oldest request not yet answered: replies come in the
 * order of the requests, so any other id means the stream is out of step. A
 * reply counts as answered here; a stream's frames all carry its request's id,
 * and the readers of streams call stream_more for each frame before the last. */
static int read_reply(tsdbc *c, int32_t *status, const uint8_t **body, size_t *blen)
{
    uint8_t hdr[13];
    int rc = read_exact(c, hdr, 13);
    if (rc)
        return rc;
    uint32_t len, id;
    memcpy(&len, hdr, 4);
    memcpy(&id, hdr + 4, 4);
    if (len < 9 || len > TSDB_MAX_FRAME + TSDB_MULTI_PREFIX || hdr[8] != TSDB_OP_REPLY || id != c->expect) {
        c->broken = true;
        return TSDB_ERR_CORRUPT;
    }
    memcpy(status, hdr + 9, 4);
    size_t n = len - 9;
    if (n > c->fcap) {
        uint8_t *f = realloc(c->frame, n);
        if (!f) {
            c->broken = true; /* the body stays unread */
            return TSDB_ERR_NOMEM;
        }
        c->frame = f;
        c->fcap = n;
    }
    if ((rc = read_exact(c, c->frame, n)) != 0)
        return rc;
    *body = c->frame;
    *blen = n;
    c->expect++;
    return 0;
}

/* The frame just read is not the last of its stream. */
static void stream_more(tsdbc *c) { c->expect--; }

/* Reads the pipelined acknowledgements, keeping the first error for tsdbc_wait. */
static int drain(tsdbc *c)
{
    while (c->pending) {
        int32_t st;
        const uint8_t *b;
        size_t n;
        int rc = read_reply(c, &st, &b, &n);
        if (rc)
            return rc;
        c->pending--;
        if (st && !c->pending_rc)
            c->pending_rc = st;
    }
    return 0;
}

int tsdbc_wait(tsdbc *c)
{
    if (c->broken)
        return TSDB_ERR_IO;
    int rc = drain(c);
    if (rc)
        return rc;
    rc = c->pending_rc;
    c->pending_rc = 0;
    return rc;
}

/* A request whose reply is read right away (after any pipelined ones). */
static int call(tsdbc *c, uint8_t op, const void *a, size_t alen, const void *b, size_t blen, const uint8_t **body,
                size_t *n)
{
    int rc = tsdbc_wait(c);
    if (rc)
        return rc;
    if ((rc = send_req(c, op, a, alen, b, blen, NULL, 0, NULL, 0)) != 0)
        return rc;
    int32_t st;
    const uint8_t *bb;
    size_t nn;
    if ((rc = read_reply(c, &st, &bb, &nn)) != 0)
        return rc;
    if (body) {
        *body = bb;
        *n = nn;
    }
    return st;
}

/* -------------------------------------------------------------- calls */

static int series_req(tsdbc *c, const char *name, int create, const char *path, uint32_t *id)
{
    size_t len = strlen(name), plen = path ? strlen(path) : 0;
    if (len == 0 || len > 65535 || plen > 65535)
        return TSDB_ERR_INVALID;
    uint8_t h[3] = {(uint8_t)(create != 0)};
    uint16_t l16 = (uint16_t)len, p16 = (uint16_t)plen;
    memcpy(h + 1, &l16, 2);
    int rc = tsdbc_wait(c);
    if (rc)
        return rc;
    rc = path ? send_req(c, TSDB_OP_SERIES, h, 3, name, len, &p16, 2, path, plen)
              : send_req(c, TSDB_OP_SERIES, h, 3, name, len, NULL, 0, NULL, 0);
    if (rc)
        return rc;
    int32_t st;
    const uint8_t *b;
    size_t n;
    if ((rc = read_reply(c, &st, &b, &n)) != 0)
        return rc;
    if (st)
        return st;
    if (n != 4)
        return TSDB_ERR_CORRUPT;
    memcpy(id, b, 4);
    return 0;
}

int tsdbc_series(tsdbc *c, const char *name, int create, uint32_t *id) { return series_req(c, name, create, NULL, id); }

int tsdbc_series_at(tsdbc *c, const char *name, const char *path, uint32_t *id)
{
    return series_req(c, name, 1, path, id);
}

int tsdbc_series_name(tsdbc *c, uint32_t id, char *buf, size_t len)
{
    const uint8_t *b;
    size_t n;
    int rc = call(c, TSDB_OP_SERIES_NAME, &id, 4, NULL, 0, &b, &n);
    if (rc)
        return rc;
    uint16_t l;
    if (n < 2 || (memcpy(&l, b, 2), n != 2u + l))
        return TSDB_ERR_CORRUPT;
    if (memchr(b + 2, 0, l))
        return TSDB_ERR_CORRUPT; /* names never hold NUL */
    if (len <= l)
        return TSDB_ERR_INVALID; /* a cut name could name another series or state */
    memcpy(buf, b + 2, l);
    buf[l] = '\0';
    return 0;
}

static int insert_values_frames(tsdbc *c, const uint32_t *series, uint32_t s, const int64_t *ts,
                                const tsdb_values *v, size_t n, bool wait);
static bool packing(tsdbc *c);
static uint32_t server_version(tsdbc *c);

static int insert_frames(tsdbc *c, const uint32_t *series, uint32_t s, const int64_t *ts, const float *v, size_t n,
                         bool wait)
{
    if (n && packing(c)) { /* float32 too: the packed form is the smaller one */
        tsdb_values col = {TSDB_FLOAT32, v, NULL};
        return insert_values_frames(c, series, s, ts, &col, n, wait);
    }
    int rc = 0;
    for (size_t off = 0; off < n || (n == 0 && off == 0); off += INSERT_FRAME_ROWS) {
        uint32_t m = (uint32_t)(n - off < INSERT_FRAME_ROWS ? n - off : INSERT_FRAME_ROWS);
        uint32_t h[2] = {m, series ? TSDB_MANY_SERIES : s};
        rc = send_req(c, TSDB_OP_INSERT, h, 8, ts + off, (size_t)m * 8, v + off, (size_t)m * 4,
                      series ? series + off : NULL, series ? (size_t)m * 4 : 0);
        if (rc)
            return rc;
        c->pending++;
        if (n == 0)
            break;
    }
    return wait ? tsdbc_wait(c) : 0;
}

int tsdbc_insert(tsdbc *c, const uint32_t *series, const int64_t *ts, const float *values, size_t n)
{
    return insert_frames(c, series, 0, ts, values, n, true);
}

int tsdbc_insert_series(tsdbc *c, uint32_t series, const int64_t *ts, const float *values, size_t n)
{
    return insert_frames(c, NULL, series, ts, values, n, true);
}

int tsdbc_insert_async(tsdbc *c, const uint32_t *series, const int64_t *ts, const float *values, size_t n)
{
    return insert_frames(c, series, 0, ts, values, n, false);
}

int tsdbc_insert_series_async(tsdbc *c, uint32_t series, const int64_t *ts, const float *values, size_t n)
{
    return insert_frames(c, NULL, series, ts, values, n, false);
}

int tsdbc_query_raw(tsdbc *c, uint32_t series, int64_t start, int64_t end, tsdb_raw_fn fn, void *ctx)
{
    return tsdbc_query_raw_ex(c, series, start, end, NULL, fn, ctx);
}

int tsdbc_query_raw_ex(tsdbc *c, uint32_t series, int64_t start, int64_t end, const tsdb_query_opts *opts,
                       tsdb_raw_fn fn, void *ctx)
{
    int rc = tsdbc_wait(c);
    if (rc)
        return rc;
    uint8_t q[29];
    memcpy(q, &series, 4);
    memcpy(q + 4, &start, 8);
    memcpy(q + 12, &end, 8);
    q[20] = opts && opts->order == TSDB_DESC;
    uint64_t lim = opts ? opts->limit : 0;
    memcpy(q + 21, &lim, 8);
    if ((rc = send_req(c, TSDB_OP_QUERY_RAW, q, opts ? 29 : 20, NULL, 0, NULL, 0, NULL, 0)) != 0)
        return rc;
    int64_t *ts = NULL;
    float *v = NULL;
    size_t cap = 0;
    bool stopped = false;
    for (;;) {
        int32_t st;
        const uint8_t *b;
        size_t len;
        if ((rc = read_reply(c, &st, &b, &len)) != 0)
            break;
        uint32_t n;
        if (len < 5 || (memcpy(&n, b + 1, 4), len != 5 + (size_t)n * 12)) {
            rc = TSDB_ERR_CORRUPT;
            c->broken = true; /* whether more of the stream follows is unknown */
            break;
        }
        if (!b[0])
            stream_more(c);
        if (n && !stopped) {
            if (n > cap) { /* aligned copies of the columns */
                free(ts);
                free(v);
                ts = malloc((size_t)n * 8);
                v = malloc((size_t)n * 4);
                cap = ts && v ? n : 0;
                if (!cap) {
                    rc = TSDB_ERR_NOMEM;
                    c->broken = !b[0]; /* the rest of the stream stays unread */
                    break;
                }
            }
            memcpy(ts, b + 5, (size_t)n * 8);
            memcpy(v, b + 5 + (size_t)n * 8, (size_t)n * 4);
            stopped = fn(ctx, ts, v, n) != 0; /* keep reading to the end of the stream */
        }
        if (b[0]) {
            rc = st ? st : (stopped ? TSDB_ERR_STOPPED : 0);
            break;
        }
    }
    free(ts);
    free(v);
    return rc;
}

static int collect(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    tsdb_points *p = ctx;
    if (p->n + n > p->cap) {
        size_t cap = p->cap * 2 > p->n + n ? p->cap * 2 : p->n + n;
        int64_t *t = realloc(p->ts, cap * sizeof *t);
        if (!t)
            return 1;
        p->ts = t;
        float *vv = realloc(p->values, cap * sizeof *vv);
        if (!vv)
            return 1;
        p->values = vv;
        p->cap = cap;
    }
    memcpy(p->ts + p->n, ts, n * 8);
    memcpy(p->values + p->n, v, n * 4);
    p->n += n;
    return 0;
}

int tsdbc_query_points(tsdbc *c, uint32_t series, int64_t start, int64_t end, tsdb_points *out)
{
    return tsdbc_query_points_ex(c, series, start, end, NULL, out);
}

int tsdbc_query_points_ex(tsdbc *c, uint32_t series, int64_t start, int64_t end, const tsdb_query_opts *opts,
                          tsdb_points *out)
{
    memset(out, 0, sizeof *out);
    int rc = tsdbc_query_raw_ex(c, series, start, end, opts, collect, out);
    if (rc == TSDB_ERR_STOPPED)
        rc = TSDB_ERR_NOMEM;
    if (rc) {
        free(out->ts);
        free(out->values);
        memset(out, 0, sizeof *out);
    }
    return rc;
}

/* Rows of TSDB_BUCKET_ROW bytes into tsdb_bucket; with fields & TSDB_SPREAD rows of TSDB_SPREAD_ROW bytes
 * into tsdb_spread_bucket. */
static int read_buckets(const uint8_t *b, size_t len, uint8_t fields, void **out, size_t *nout)
{
    uint32_t n;
    bool spread = fields & TSDB_SPREAD;
    size_t row = spread ? TSDB_SPREAD_ROW : TSDB_BUCKET_ROW;
    size_t stride = spread ? sizeof(tsdb_spread_bucket) : sizeof(tsdb_bucket);
    if (len < 4 || (memcpy(&n, b, 4), len != 4 + (size_t)n * row))
        return TSDB_ERR_CORRUPT;
    uint8_t *res = malloc((n ? n : 1) * stride);
    if (!res)
        return TSDB_ERR_NOMEM;
    for (uint32_t i = 0; i < n; i++) {
        const uint8_t *o = b + 4 + (size_t)i * row;
        tsdb_bucket *r = (tsdb_bucket *)(res + (size_t)i * stride);
        memcpy(&r->start, o, 8);
        memcpy(&r->count, o + 8, 8);
        memcpy(&r->sum, o + 16, 8);
        memcpy(&r->min, o + 24, 8);
        memcpy(&r->max, o + 32, 8);
        memcpy(&r->isum, o + 40, 8);
        memcpy(&r->imin, o + 48, 8);
        memcpy(&r->imax, o + 56, 8);
        if (spread) {
            tsdb_spread_bucket *x = (tsdb_spread_bucket *)r;
            memcpy(&x->range, o + 64, 8);
            memcpy(&x->var_pop, o + 72, 8);
            memcpy(&x->var_samp, o + 80, 8);
            memcpy(&x->stddev_pop, o + 88, 8);
            memcpy(&x->stddev_samp, o + 96, 8);
        }
    }
    *out = res;
    *nout = n;
    return 0;
}

/* QUERY_BUCKETS: fixed intervals (group NULL) or calendar groups. The fields byte follows the zone only when
 * the caller asks for the spread: without it the request is the one every server version takes. */
static int buckets_req(tsdbc *c, uint32_t series, int64_t start, int64_t end, int64_t interval,
                       const tsdb_group *group, const tsdb_query_opts *opts, uint8_t fields, void **out, size_t *nout)
{
    *out = NULL;
    *nout = 0;
    const char *zone = group && group->timezone ? group->timezone : "";
    size_t zl = strlen(zone);
    if (zl > 65535)
        return TSDB_ERR_INVALID;
    if (fields && server_version(c) < 8) /* an older server has no spread (and refuses the byte) */
        return TSDB_ERR_INVALID;
    uint8_t q[44];
    memcpy(q, &series, 4);
    memcpy(q + 4, &start, 8);
    memcpy(q + 12, &end, 8);
    memcpy(q + 20, &interval, 8);
    q[28] = group ? (uint8_t)group->unit : 0;
    uint32_t count = group ? group->count : 0;
    memcpy(q + 29, &count, 4);
    q[33] = opts && opts->order == TSDB_DESC;
    uint64_t lim = opts ? opts->limit : 0;
    memcpy(q + 34, &lim, 8);
    uint16_t z16 = (uint16_t)zl;
    memcpy(q + 42, &z16, 2);
    uint8_t *tail = malloc(zl + 1);
    if (!tail)
        return TSDB_ERR_NOMEM;
    memcpy(tail, zone, zl);
    tail[zl] = fields;
    const uint8_t *b;
    size_t len;
    int rc = call(c, TSDB_OP_QUERY_BUCKETS, q, 44, tail, zl + (fields != 0), &b, &len);
    free(tail);
    return rc ? rc : read_buckets(b, len, fields, out, nout);
}

/* QUERY_BOUNDS, the fields byte after the bounds as in buckets_req */
static int bounds_req(tsdbc *c, uint32_t series, int64_t start, int64_t end, const int64_t *bounds, size_t n,
                      const tsdb_query_opts *opts, uint8_t fields, void **out, size_t *nout)
{
    *out = NULL;
    *nout = 0;
    if (!bounds || n == 0 || n > (TSDB_MAX_FRAME - 64) / 8)
        return TSDB_ERR_INVALID;
    if (fields && server_version(c) < 8)
        return TSDB_ERR_INVALID;
    uint8_t q[33];
    uint32_t n32 = (uint32_t)n;
    uint64_t lim = opts ? opts->limit : 0;
    memcpy(q, &series, 4);
    memcpy(q + 4, &start, 8);
    memcpy(q + 12, &end, 8);
    q[20] = opts && opts->order == TSDB_DESC;
    memcpy(q + 21, &lim, 8);
    memcpy(q + 29, &n32, 4);
    uint8_t *tail = malloc(n * 8 + 1);
    if (!tail)
        return TSDB_ERR_NOMEM;
    memcpy(tail, bounds, n * 8);
    tail[n * 8] = fields;
    const uint8_t *b;
    size_t len;
    int rc = call(c, TSDB_OP_QUERY_BOUNDS, q, 33, tail, n * 8 + (fields != 0), &b, &len);
    free(tail);
    return rc ? rc : read_buckets(b, len, fields, out, nout);
}

int tsdbc_query_agg(tsdbc *c, uint32_t series, int64_t start, int64_t end, int64_t interval_ms, tsdb_bucket **out,
                    size_t *nout)
{
    return tsdbc_query_agg_ex(c, series, start, end, interval_ms, NULL, out, nout);
}

/* QUERY_BUCKETS with unit 0, not QUERY_AGG: its 32-byte rows have neither order, limit nor the int64 fields */
int tsdbc_query_agg_ex(tsdbc *c, uint32_t series, int64_t start, int64_t end, int64_t interval_ms,
                       const tsdb_query_opts *opts, tsdb_bucket **out, size_t *nout)
{
    if (interval_ms < 0) {
        *out = NULL;
        *nout = 0;
        return TSDB_ERR_INVALID;
    }
    return buckets_req(c, series, start, end, interval_ms, NULL, opts, 0, (void **)out, nout);
}

int tsdbc_query_bounds(tsdbc *c, uint32_t series, int64_t start, int64_t end, const int64_t *bounds, size_t n,
                       const tsdb_query_opts *opts, tsdb_bucket **out, size_t *nout)
{
    return bounds_req(c, series, start, end, bounds, n, opts, 0, (void **)out, nout);
}

static int first_ts(void *ctx, const int64_t *ts, const tsdb_values *v, size_t n)
{
    (void)v;
    if (n)
        *(int64_t *)ctx = ts[0];
    return 0;
}

static int group_req(tsdbc *c, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                     const tsdb_query_opts *opts, uint8_t fields, void **out, size_t *nout)
{
    bool local = group && group->timezone && strcmp(group->timezone, "UTC") != 0;
    if (!local || server_version(c) < 4) /* an older server still takes the zone itself */
        return buckets_req(c, series, start, end, 0, group, opts, fields, out, nout);
    *out = NULL;
    *nout = 0;
    /* local periods become UTC bounds here; an open range is cut to the rows there are */
    int64_t lo = INT64_MAX, hi = INT64_MIN;
    tsdb_query_opts first = {TSDB_ASC, 1}, last = {TSDB_DESC, 1};
    int rc = tsdbc_query_values(c, series, start, end, &first, first_ts, &lo);
    if (rc == 0)
        rc = tsdbc_query_values(c, series, start, end, &last, first_ts, &hi);
    if (rc || lo > hi)
        return rc;
    int64_t *bounds;
    size_t n;
    rc = tsdb_calendar_bounds(group->timezone, group->unit, group->count, lo, hi + 1, &bounds, &n);
    /* more local periods than one request carries: the status of more than the calendar computes, in every client */
    if (rc == 0 && n > (TSDB_MAX_FRAME - 64) / 8)
        rc = TSDB_ERR_TOO_MANY_BUCKETS;
    if (rc == 0)
        rc = bounds_req(c, series, start, end, bounds, n, opts, fields, out, nout);
    free(bounds);
    return rc;
}

int tsdbc_query_group(tsdbc *c, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                      const tsdb_query_opts *opts, tsdb_bucket **out, size_t *nout)
{
    return group_req(c, series, start, end, group, opts, 0, (void **)out, nout);
}

/* --------------------------------------------------------------- spread */

int tsdbc_query_spread(tsdbc *c, uint32_t series, int64_t start, int64_t end, int64_t interval_ms,
                       const tsdb_query_opts *opts, tsdb_spread_bucket **out, size_t *nout)
{
    if (interval_ms < 0) {
        *out = NULL;
        *nout = 0;
        return TSDB_ERR_INVALID;
    }
    return buckets_req(c, series, start, end, interval_ms, NULL, opts, TSDB_SPREAD, (void **)out, nout);
}

int tsdbc_query_spread_group(tsdbc *c, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                             const tsdb_query_opts *opts, tsdb_spread_bucket **out, size_t *nout)
{
    return group_req(c, series, start, end, group, opts, TSDB_SPREAD, (void **)out, nout);
}

int tsdbc_query_spread_bounds(tsdbc *c, uint32_t series, int64_t start, int64_t end, const int64_t *bounds,
                              size_t n, const tsdb_query_opts *opts, tsdb_spread_bucket **out, size_t *nout)
{
    return bounds_req(c, series, start, end, bounds, n, opts, TSDB_SPREAD, (void **)out, nout);
}

/* ------------------------------------------------- time-weighted averages */

static int twa_req(tsdbc *c, uint32_t series, int64_t start, int64_t end, int64_t interval, uint8_t unit,
                   uint32_t count, const int64_t *bounds, size_t n, const tsdb_twa_opts *opts, tsdb_twa_bucket **out,
                   size_t *nout)
{
    *out = NULL;
    *nout = 0;
    if (n > (TSDB_MAX_FRAME - 64 - TSDB_TWA_HEADER) / 8)
        return TSDB_ERR_INVALID;
    if (server_version(c) < 6) /* an older server does not know QUERY_TWA */
        return TSDB_ERR_INVALID;
    tsdb_twa_opts def = {0};
    if (!opts)
        opts = &def;
    uint8_t q[TSDB_TWA_HEADER] = {0};
    uint64_t lim = opts->limit;
    uint32_t n32 = (uint32_t)n;
    memcpy(q, &series, 4);
    memcpy(q + 4, &start, 8);
    memcpy(q + 12, &end, 8);
    q[20] = (uint8_t)opts->method;
    q[21] = opts->order == TSDB_DESC;
    q[22] = unit;
    memcpy(q + 24, &opts->max_gap_ms, 8);
    memcpy(q + 32, &lim, 8);
    memcpy(q + 40, &interval, 8);
    memcpy(q + 48, &count, 4);
    memcpy(q + 52, &n32, 4);
    const uint8_t *b;
    size_t len;
    int rc = call(c, TSDB_OP_QUERY_TWA, q, sizeof q, bounds, n * 8, &b, &len);
    if (rc)
        return rc;
    uint32_t m;
    if (len < 4 || (memcpy(&m, b, 4), len != 4 + (size_t)m * TSDB_TWA_ROW))
        return TSDB_ERR_CORRUPT;
    tsdb_twa_bucket *r = malloc((m ? m : 1) * sizeof *r);
    if (!r)
        return TSDB_ERR_NOMEM;
    for (uint32_t i = 0; i < m; i++) {
        const uint8_t *o = b + 4 + (size_t)i * TSDB_TWA_ROW;
        memcpy(&r[i].start, o, 8);
        memcpy(&r[i].twa, o + 8, 8);
        memcpy(&r[i].area, o + 16, 8);
        memcpy(&r[i].covered_ms, o + 24, 8);
        memcpy(&r[i].count, o + 32, 8);
    }
    *out = r;
    *nout = m;
    return 0;
}

int tsdbc_query_twa(tsdbc *c, uint32_t series, int64_t start, int64_t end, int64_t interval_ms,
                    const tsdb_twa_opts *opts, tsdb_twa_bucket **out, size_t *nout)
{
    if (interval_ms < 0) {
        *out = NULL;
        *nout = 0;
        return TSDB_ERR_INVALID;
    }
    return twa_req(c, series, start, end, interval_ms, 0, 0, NULL, 0, opts, out, nout);
}

int tsdbc_query_twa_bounds(tsdbc *c, uint32_t series, int64_t start, int64_t end, const int64_t *bounds, size_t n,
                           const tsdb_twa_opts *opts, tsdb_twa_bucket **out, size_t *nout)
{
    if (!bounds || n == 0) {
        *out = NULL;
        *nout = 0;
        return TSDB_ERR_INVALID;
    }
    return twa_req(c, series, start, end, 0, 0, 0, bounds, n, opts, out, nout);
}

int tsdbc_query_twa_group(tsdbc *c, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                          const tsdb_twa_opts *opts, tsdb_twa_bucket **out, size_t *nout)
{
    *out = NULL;
    *nout = 0;
    if (!group || group->unit < TSDB_SECOND || group->unit > TSDB_YEAR)
        return group ? TSDB_ERR_INVALID : twa_req(c, series, start, end, 0, 0, 0, NULL, 0, opts, out, nout);
    if (!group->timezone || strcmp(group->timezone, "UTC") == 0)
        return twa_req(c, series, start, end, 0, (uint8_t)group->unit, group->count, NULL, 0, opts, out, nout);
    /* local periods become UTC bounds over the range; an open end is cut to the rows there are */
    int64_t lo = start, hi = end == INT64_MAX ? INT64_MIN : end - 1;
    int rc = 0;
    if (start == INT64_MIN) {
        tsdb_query_opts first = {TSDB_ASC, 1};
        lo = INT64_MAX;
        rc = tsdbc_query_values(c, series, start, end, &first, first_ts, &lo);
    }
    if (rc == 0 && end == INT64_MAX) {
        tsdb_query_opts last = {TSDB_DESC, 1};
        rc = tsdbc_query_values(c, series, start, end, &last, first_ts, &hi);
    }
    if (rc || lo > hi)
        return rc;
    int64_t *bounds;
    size_t n;
    rc = tsdb_calendar_bounds(group->timezone, group->unit, group->count, lo, hi + 1, &bounds, &n);
    if (rc == 0 && n > (TSDB_MAX_FRAME - 64 - TSDB_TWA_HEADER) / 8) /* as in group_req */
        rc = TSDB_ERR_TOO_MANY_BUCKETS;
    if (rc == 0)
        rc = tsdbc_query_twa_bounds(c, series, start, end, bounds, n, opts, out, nout);
    free(bounds);
    return rc;
}

/* ------------------------------------------------------------ value types */

int tsdbc_series_create(tsdbc *c, const char *name, tsdb_type type, const char *path, uint32_t *id)
{
    size_t len = strlen(name), plen = path ? strlen(path) : 0;
    if (len == 0 || len > 65535 || plen > 65535)
        return TSDB_ERR_INVALID;
    uint8_t h[3] = {(uint8_t)type};
    uint16_t l16 = (uint16_t)len, p16 = (uint16_t)plen;
    memcpy(h + 1, &l16, 2);
    int rc = tsdbc_wait(c);
    if (rc)
        return rc;
    struct iovec parts[5] = {{0}, {h, 3}, {(void *)name, len}, {&p16, 2}, {(void *)(path ? path : ""), plen}};
    if ((rc = send_reqv(c, TSDB_OP_SERIES_CREATE, parts, 5)) != 0)
        return rc;
    int32_t st;
    const uint8_t *b;
    size_t n;
    if ((rc = read_reply(c, &st, &b, &n)) != 0)
        return rc;
    if (st)
        return st;
    if (n != 4)
        return TSDB_ERR_CORRUPT;
    memcpy(id, b, 4);
    return 0;
}

int tsdbc_series_type(tsdbc *c, uint32_t id, tsdb_type *type)
{
    const uint8_t *b;
    size_t n;
    int rc = call(c, TSDB_OP_SERIES_TYPE, &id, 4, NULL, 0, &b, &n);
    if (rc)
        return rc;
    if (n != 1 || b[0] > TSDB_STATE)
        return TSDB_ERR_CORRUPT;
    *type = (tsdb_type)b[0];
    return 0;
}

int tsdbc_set_timeout(tsdbc *c, int ms)
{
    if (ms < 0)
        return TSDB_ERR_INVALID;
    struct timeval tv = {ms / 1000, (ms % 1000) * 1000};
    if (setsockopt(c->fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv) != 0 ||
        setsockopt(c->fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv) != 0)
        return TSDB_ERR_IO;
    return 0;
}

int tsdbc_set_compression(tsdbc *c, int level)
{
    if (level > ZSTD_maxCLevel() || level < ZSTD_minCLevel())
        return TSDB_ERR_INVALID;
    c->zlevel = level;
    return 0;
}

/* The server's protocol version, asked once. Not through call(): an error
 * among the pipelined inserts before it belongs to the next tsdbc_wait, and
 * taking it for HELLO's would leave the connection unpacked for good. */
static uint32_t server_version(tsdbc *c)
{
    if (!c->server_version) {
        uint32_t mine = TSDB_PROTOCOL_VERSION;
        int32_t st;
        const uint8_t *b;
        size_t n;
        if (drain(c) || send_req(c, TSDB_OP_HELLO, &mine, 4, NULL, 0, NULL, 0, NULL, 0) ||
            read_reply(c, &st, &b, &n))
            return 1; /* the connection failed; whatever comes next reports it */
        uint32_t theirs = 1; /* a version 1 server does not know HELLO */
        if (st == 0 && n == 4)
            memcpy(&theirs, b, 4);
        c->server_version = theirs;
    }
    return c->server_version;
}

uint32_t tsdbc_server_version(tsdbc *c) { return tsdbc_wait(c) ? 0 : server_version(c); }

uint64_t tsdbc_bytes_sent(const tsdbc *c) { return c->sent; }

/* Whether inserts go packed: compression on, and a server that reads them. */
static bool packing(tsdbc *c) { return c->zlevel && server_version(c) >= 3; }

/* One packed frame: reshape, zstd, send (the buffers are free again on return). */
static int send_packed(tsdbc *c, const uint32_t *series, uint32_t s, const int64_t *ts, const tsdb_values *v,
                       size_t off, uint32_t m)
{
    unsigned aw = tsdb_wire_width(v->type);
    size_t bound = tsdb_pack_raw_bound(m, v->type, v->valid != NULL, series != NULL);
    if (bound > c->raw_cap) {
        free(c->raw);
        c->raw = malloc(bound);
        c->raw_cap = c->raw ? bound : 0;
    }
    size_t zb = ZSTD_compressBound(bound);
    if (zb > c->zbuf_cap) {
        free(c->zbuf);
        c->zbuf = malloc(zb);
        c->zbuf_cap = c->zbuf ? zb : 0;
    }
    if (!c->zc)
        c->zc = ZSTD_createCCtx();
    if (!c->raw_cap || !c->zbuf_cap || !c->zc)
        return TSDB_ERR_NOMEM;
    /* a frame starts at a multiple of 8 rows, so its bitmap slice starts on a byte */
    size_t raw = tsdb_pack_reshape(m, v->type, ts + off, (const uint8_t *)v->values + off * aw,
                                   v->valid ? v->valid + off / 8 : NULL, series ? series + off : NULL, c->raw);
    size_t z = ZSTD_compressCCtx(c->zc, c->zbuf, c->zbuf_cap, c->raw, raw, c->zlevel);
    if (ZSTD_isError(z))
        return TSDB_ERR_NOMEM;
    uint8_t h[TSDB_VALUES_HEADER + 4] = {0};
    uint32_t sid = series ? TSDB_MANY_SERIES : s, rl = (uint32_t)raw;
    memcpy(h, &m, 4);
    memcpy(h + 4, &sid, 4);
    h[8] = (uint8_t)v->type;
    h[9] = (uint8_t)(TSDB_PACKED | (v->valid ? TSDB_VALID : 0));
    memcpy(h + TSDB_VALUES_HEADER, &rl, 4);
    struct iovec parts[3] = {{0}, {h, sizeof h}, {c->zbuf, z}};
    return send_reqv(c, TSDB_OP_INSERT_VALUES, parts, 3);
}

static int insert_values_frames(tsdbc *c, const uint32_t *series, uint32_t s, const int64_t *ts,
                                const tsdb_values *v, size_t n, bool wait)
{
    if (!v || (unsigned)v->type > TSDB_STATE || (n && (!ts || !v->values)))
        return TSDB_ERR_INVALID;
    unsigned aw = tsdb_wire_width(v->type);
    int rc = 0;
    if (n && packing(c)) {
        for (size_t off = 0; off < n; off += INSERT_FRAME_ROWS) {
            uint32_t m = (uint32_t)(n - off < INSERT_FRAME_ROWS ? n - off : INSERT_FRAME_ROWS);
            if ((rc = send_packed(c, series, s, ts, v, off, m)) != 0)
                return rc;
            c->pending++;
        }
        return wait ? tsdbc_wait(c) : 0;
    }
    static const uint8_t pad[3];
    for (size_t off = 0; off < n || (n == 0 && off == 0); off += INSERT_FRAME_ROWS) {
        uint32_t m = (uint32_t)(n - off < INSERT_FRAME_ROWS ? n - off : INSERT_FRAME_ROWS);
        uint8_t h[TSDB_VALUES_HEADER] = {0};
        uint32_t sid = series ? TSDB_MANY_SERIES : s;
        memcpy(h, &m, 4);
        memcpy(h + 4, &sid, 4);
        h[8] = (uint8_t)v->type;
        h[9] = v->valid ? TSDB_VALID : 0;
        size_t vb = (size_t)m * aw, valid = v->valid ? ((size_t)m + 7) / 8 : 0;
        size_t end = TSDB_VALUES_HEADER + (size_t)m * 8 + vb + valid;
        struct iovec parts[7] = {{0},
                                 {h, sizeof h},
                                 {(void *)(ts + off), (size_t)m * 8},
                                 {(uint8_t *)v->values + off * aw, vb},
                                 {(void *)(v->valid ? v->valid + off / 8 : pad), valid}, /* off is a multiple of 8 */
                                 {(void *)pad, series ? ((end + 3) & ~(size_t)3) - end : 0},
                                 {(void *)(series ? series + off : NULL), series ? (size_t)m * 4 : 0}};
        if ((rc = send_reqv(c, TSDB_OP_INSERT_VALUES, parts, 7)) != 0)
            return rc;
        c->pending++;
        if (n == 0)
            break;
    }
    return wait ? tsdbc_wait(c) : 0;
}

int tsdbc_insert_values(tsdbc *c, const uint32_t *series, const int64_t *ts, const tsdb_values *values, size_t n)
{
    return insert_values_frames(c, series, 0, ts, values, n, true);
}

int tsdbc_insert_series_values(tsdbc *c, uint32_t series, const int64_t *ts, const tsdb_values *values, size_t n)
{
    return insert_values_frames(c, NULL, series, ts, values, n, true);
}

int tsdbc_insert_values_async(tsdbc *c, const uint32_t *series, const int64_t *ts, const tsdb_values *values,
                              size_t n)
{
    return insert_values_frames(c, series, 0, ts, values, n, false);
}

int tsdbc_insert_series_values_async(tsdbc *c, uint32_t series, const int64_t *ts, const tsdb_values *values,
                                     size_t n)
{
    return insert_values_frames(c, NULL, series, ts, values, n, false);
}

static int read_values(tsdbc *c, tsdb_values_fn fn, void *ctx);

int tsdbc_query_values(tsdbc *c, uint32_t series, int64_t start, int64_t end, const tsdb_query_opts *opts,
                       tsdb_values_fn fn, void *ctx)
{
    int rc = tsdbc_wait(c);
    if (rc)
        return rc;
    uint8_t q[29];
    memcpy(q, &series, 4);
    memcpy(q + 4, &start, 8);
    memcpy(q + 12, &end, 8);
    q[20] = opts && opts->order == TSDB_DESC;
    uint64_t lim = opts ? opts->limit : 0;
    memcpy(q + 21, &lim, 8);
    if ((rc = send_req(c, TSDB_OP_QUERY_VALUES, q, 29, NULL, 0, NULL, 0, NULL, 0)) != 0)
        return rc;
    return read_values(c, fn, ctx);
}

int tsdbc_query_lttb(tsdbc *c, uint32_t series, int64_t start, int64_t end, size_t threshold, tsdb_values_fn fn,
                     void *ctx)
{
    return tsdbc_query_lttb_ex(c, series, start, end, threshold, NULL, fn, ctx);
}

int tsdbc_query_lttb_ex(tsdbc *c, uint32_t series, int64_t start, int64_t end, size_t threshold,
                        const tsdb_query_opts *opts, tsdb_values_fn fn, void *ctx)
{
    int rc = tsdbc_wait(c);
    if (rc)
        return rc;
    if (server_version(c) < 5)
        return TSDB_ERR_INVALID; /* the server does not know QUERY_LTTB */
    /* order and limit only when they change something: the 28-byte form is what every server takes */
    bool ex = opts && (opts->order == TSDB_DESC || opts->limit);
    if (ex && server_version(c) < 9)
        return TSDB_ERR_INVALID;
    uint8_t q[37];
    uint64_t t = threshold, lim = opts ? opts->limit : 0;
    memcpy(q, &series, 4);
    memcpy(q + 4, &start, 8);
    memcpy(q + 12, &end, 8);
    memcpy(q + 20, &t, 8);
    q[28] = ex && opts->order == TSDB_DESC;
    memcpy(q + 29, &lim, 8);
    if ((rc = send_req(c, TSDB_OP_QUERY_LTTB, q, ex ? 37 : 28, NULL, 0, NULL, 0, NULL, 0)) != 0)
        return rc;
    return read_values(c, fn, ctx);
}

/* The chunks of a QUERY_VALUES or QUERY_LTTB reply, to fn. */
static int read_values(tsdbc *c, tsdb_values_fn fn, void *ctx)
{
    int rc = 0;
    int64_t *ts = NULL;
    uint8_t *v = NULL;
    size_t cap = 0;
    bool stopped = false;
    for (;;) {
        int32_t st;
        const uint8_t *b;
        size_t len;
        if ((rc = read_reply(c, &st, &b, &len)) != 0)
            break;
        uint32_t n;
        if (len < 8 || b[1] > TSDB_STATE || (memcpy(&n, b + 4, 4), false)) {
            rc = TSDB_ERR_CORRUPT;
            c->broken = true;
            break;
        }
        unsigned aw = tsdb_wire_width(b[1]);
        size_t valid = (b[2] & TSDB_VALID) ? ((size_t)n + 7) / 8 : 0;
        if (len != 8 + (size_t)n * (8 + aw) + valid) {
            rc = TSDB_ERR_CORRUPT;
            c->broken = true;
            break;
        }
        if (!b[0])
            stream_more(c);
        if (n && !stopped) {
            if (n > cap) { /* aligned copies of the columns */
                free(ts);
                free(v);
                ts = malloc((size_t)n * 8);
                v = malloc((size_t)n * 8 + (size_t)n / 8 + 1);
                cap = ts && v ? n : 0;
                if (!cap) {
                    rc = TSDB_ERR_NOMEM;
                    c->broken = !b[0]; /* the rest of the stream stays unread */
                    break;
                }
            }
            memcpy(ts, b + 8, (size_t)n * 8);
            memcpy(v, b + 8 + (size_t)n * 8, (size_t)n * aw + valid);
            tsdb_values col = {(tsdb_type)b[1], v, valid ? v + (size_t)n * aw : NULL};
            stopped = fn(ctx, ts, &col, n) != 0; /* keep reading to the end of the stream */
        }
        if (b[0]) {
            rc = st ? st : (stopped ? TSDB_ERR_STOPPED : 0);
            break;
        }
    }
    free(ts);
    free(v);
    return rc;
}

static int state_req(tsdbc *c, uint32_t series, const char *name, bool create, uint32_t *code)
{
    size_t len = strlen(name);
    if (len == 0 || len > 65535)
        return TSDB_ERR_INVALID;
    uint8_t h[7];
    uint16_t l16 = (uint16_t)len;
    memcpy(h, &series, 4);
    h[4] = create;
    memcpy(h + 5, &l16, 2);
    const uint8_t *b;
    size_t n;
    int rc = call(c, TSDB_OP_STATE, h, 7, name, len, &b, &n);
    if (rc)
        return rc;
    if (n != 4)
        return TSDB_ERR_CORRUPT;
    memcpy(code, b, 4);
    return 0;
}

int tsdbc_state(tsdbc *c, uint32_t series, const char *name, uint32_t *code)
{
    return state_req(c, series, name, true, code);
}

int tsdbc_state_lookup(tsdbc *c, uint32_t series, const char *name, uint32_t *code)
{
    return state_req(c, series, name, false, code);
}

int tsdbc_state_name(tsdbc *c, uint32_t series, uint32_t code, char *buf, size_t len)
{
    uint32_t q[2] = {series, code};
    const uint8_t *b;
    size_t n;
    int rc = call(c, TSDB_OP_STATE_NAME, q, 8, NULL, 0, &b, &n);
    if (rc)
        return rc;
    uint16_t l;
    if (n < 2 || (memcpy(&l, b, 2), n != 2u + l))
        return TSDB_ERR_CORRUPT;
    if (memchr(b + 2, 0, l))
        return TSDB_ERR_CORRUPT; /* names never hold NUL */
    if (len <= l)
        return TSDB_ERR_INVALID; /* a cut name could name another series or state */
    memcpy(buf, b + 2, l);
    buf[l] = '\0';
    return 0;
}

int tsdbc_last_query_stats(tsdbc *c, tsdb_query_stats *st)
{
    const uint8_t *b;
    size_t n;
    int rc = call(c, TSDB_OP_LAST_STATS, NULL, 0, NULL, 0, &b, &n);
    if (rc)
        return rc;
    if (n != TSDB_STATS_REPLY)
        return TSDB_ERR_CORRUPT;
    uint64_t v[10];
    memcpy(&st->elapsed_s, b, 8);
    memcpy(v, b + 8, sizeof v);
    st->rows = v[0];
    st->rows_decoded = v[1];
    st->rows_from_stats = v[2];
    st->memtable_rows = v[3];
    st->bytes = v[4];
    st->bytes_read = v[5];
    st->parts = v[6];
    st->granules = v[7];
    st->granules_from_stats = v[8];
    st->result_rows = v[9];
    return 0;
}

int tsdbc_commit(tsdbc *c) { return call(c, TSDB_OP_COMMIT, NULL, 0, NULL, 0, NULL, NULL); }
int tsdbc_flush(tsdbc *c) { return call(c, TSDB_OP_FLUSH, NULL, 0, NULL, 0, NULL, NULL); }

/* Deletes and retention need protocol version 7: an older server would answer TSDB_ERR_INVALID too, but
 * only after the request went out; asking first keeps a pipelined connection's replies in order. */
static int need_deletes(tsdbc *c)
{
    int rc = tsdbc_wait(c);
    if (rc)
        return rc;
    return server_version(c) < 7 ? TSDB_ERR_INVALID : 0;
}

int tsdbc_delete_range(tsdbc *c, uint32_t series, int64_t start, int64_t end)
{
    int rc = need_deletes(c);
    if (rc)
        return rc;
    uint8_t b[24] = {0};
    memcpy(b, &series, 4);
    memcpy(b + 8, &start, 8);
    memcpy(b + 16, &end, 8);
    return call(c, TSDB_OP_DELETE_RANGE, b, sizeof b, NULL, 0, NULL, NULL);
}

/* One frame, unpacked: the server applies it as one tsdb_replace_range, all or nothing. */
int tsdbc_replace_range(tsdbc *c, uint32_t series, int64_t start, int64_t end, const int64_t *ts,
                        const tsdb_values *v, size_t n)
{
    if (!v || (unsigned)v->type > TSDB_STATE || (n && (!ts || !v->values)))
        return TSDB_ERR_INVALID;
    size_t vb = n * tsdb_wire_width(v->type), valid = v->valid ? (n + 7) / 8 : 0;
    if (n > UINT32_MAX || TSDB_REPLACE_HEADER + n * 8 + vb + valid > TSDB_MAX_FRAME - 16)
        return TSDB_ERR_INVALID;
    uint8_t h[TSDB_REPLACE_HEADER] = {0};
    uint32_t m = (uint32_t)n;
    memcpy(h, &m, 4);
    memcpy(h + 4, &series, 4);
    h[8] = (uint8_t)v->type;
    h[9] = v->valid ? TSDB_VALID : 0;
    memcpy(h + 16, &start, 8);
    memcpy(h + 24, &end, 8);
    int rc = need_deletes(c);
    if (rc)
        return rc;
    struct iovec parts[5] = {{0}, {h, sizeof h}, {(void *)ts, n * 8}, {(void *)v->values, vb},
                             {(void *)v->valid, valid}};
    if ((rc = send_reqv(c, TSDB_OP_REPLACE_RANGE, parts, 5)) != 0)
        return rc;
    int32_t st;
    const uint8_t *b;
    size_t len;
    if ((rc = read_reply(c, &st, &b, &len)) != 0)
        return rc;
    return st;
}

int tsdbc_set_retention(tsdbc *c, uint32_t series, int64_t limit_ms)
{
    int rc = need_deletes(c);
    if (rc)
        return rc;
    uint8_t b[16] = {0};
    memcpy(b, &series, 4);
    memcpy(b + 8, &limit_ms, 8);
    return call(c, TSDB_OP_SET_RETENTION, b, sizeof b, NULL, 0, NULL, NULL);
}

int tsdbc_get_retention(tsdbc *c, uint32_t series, int64_t *limit_ms)
{
    int rc = need_deletes(c);
    if (rc)
        return rc;
    const uint8_t *b;
    size_t n;
    rc = call(c, TSDB_OP_GET_RETENTION, &series, 4, NULL, 0, &b, &n);
    if (rc)
        return rc;
    if (n != 8)
        return TSDB_ERR_CORRUPT;
    memcpy(limit_ms, b, 8);
    return 0;
}
int tsdbc_optimize(tsdbc *c) { return call(c, TSDB_OP_OPTIMIZE, NULL, 0, NULL, 0, NULL, NULL); }

int tsdbc_stats(tsdbc *c, tsdb_stats *st)
{
    const uint8_t *b;
    size_t n;
    int rc = call(c, TSDB_OP_STATS, NULL, 0, NULL, 0, &b, &n);
    if (rc)
        return rc;
    if (n != 64)
        return TSDB_ERR_CORRUPT;
    uint64_t v[8];
    memcpy(v, b, 64);
    *st = (tsdb_stats){v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], 0, 0}; /* the last two are local only */
    return 0;
}

/* ------------------------------------------------------- many series (protocol 10) */

/*
 * tsdbc_query_multi sends QUERY_MULTI: the inner op's payload once as a template, the series (and per-series
 * starts, ends, intervals) as arrays, at most TSDB_MULTI_MAX_SERIES per request (a longer list is split, and the
 * indexes run on). The reply is the single op's frames per series in index order, each with the series' index
 * after the status, then an END frame. Local periods become one union of UTC bounds (two VALUES probes give the
 * first and last row of every series), which the server trims per series as group_req does for one.
 */

_Static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__ && sizeof(tsdb_bucket) == TSDB_BUCKET_ROW &&
                   sizeof(tsdb_spread_bucket) == TSDB_SPREAD_ROW && sizeof(tsdb_twa_bucket) == TSDB_TWA_ROW,
               "bucket structs are the wire rows (tsdbd checks every offset)");

typedef struct mrun {
    tsdbc *c;
    const tsdb_multi_opts *mo;
    tsdb_multi_fn fn;
    void *ctx;
    uint64_t max_rows, rows; /* rows or buckets delivered, over all requests of the call */
    bool stopped;
    int64_t *ts; /* aligned copies of a VALUES chunk */
    uint8_t *v;
    size_t cap;
} mrun;

static int send_cancel(tsdbc *c, uint32_t id)
{
    /* the frame carries the id of the query it stops: it gets no reply, so it must not take an id of its own */
    uint8_t f[13];
    uint32_t len = 9;
    memcpy(f, &len, 4);
    memcpy(f + 4, &id, 4);
    f[8] = TSDB_OP_CANCEL;
    memcpy(f + 9, &id, 4);
    struct iovec v = {f, sizeof f};
    return send_iov(c, &v, 1);
}

static bool deliver(mrun *m, const tsdb_series_result *r)
{
    if (m->stopped)
        return false;
    m->rows += r->n;
    if (m->fn(m->ctx, r) != 0 || (m->mo && m->mo->cancelled && m->mo->cancelled(m->mo->hook_ctx)))
        m->stopped = true;
    return !m->stopped;
}

/* Reads the reply of the QUERY_MULTI `id` for series[0 .. n-1] (indexes reported from base). */
static int multi_read(mrun *m, uint32_t id, uint8_t op, uint8_t fields, const uint32_t *series, size_t n, size_t base)
{
    tsdbc *c = m->c;
    bool values = op == TSDB_OP_QUERY_VALUES || op == TSDB_OP_QUERY_LTTB;
    size_t row = op == TSDB_OP_QUERY_TWA ? TSDB_TWA_ROW : fields & TSDB_SPREAD ? TSDB_SPREAD_ROW : TSDB_BUCKET_ROW;
    size_t next = 0;   /* the lowest index the next frame may have */
    bool open = false; /* series `next` has had frames, but not its last */
    bool cancel_sent = false;
    for (;;) {
        int32_t st;
        const uint8_t *b;
        size_t len;
        uint32_t ix;
        int rc = read_reply(c, &st, &b, &len);
        if (rc)
            return rc;
        if (len < TSDB_MULTI_PREFIX)
            goto corrupt;
        memcpy(&ix, b, 4);
        b += 4;
        len -= 4;
        if (ix == TSDB_MULTI_END) {
            if (len != 4 || open)
                goto corrupt;
            return st ? st : m->stopped ? TSDB_ERR_STOPPED : 0;
        }
        stream_more(c);
        if (ix >= n || ix < next || (open && ix != next))
            goto corrupt;
        tsdb_series_result r = {.index = base + ix, .series = series[ix], .status = st, .last = 1};
        if (values) {
            uint32_t k;
            if (len < 8 || b[1] > TSDB_STATE)
                goto corrupt;
            memcpy(&k, b + 4, 4);
            unsigned aw = tsdb_wire_width(b[1]);
            size_t valid = (b[2] & TSDB_VALID) ? ((size_t)k + 7) / 8 : 0;
            if (len != 8 + (size_t)k * (8 + aw) + valid || (st && !b[0]))
                goto corrupt;
            r.values.type = (tsdb_type)b[1];
            r.last = b[0];
            if (k && !m->stopped) {
                if (k > m->cap) {
                    free(m->ts);
                    free(m->v);
                    m->ts = malloc((size_t)k * 8);
                    m->v = malloc((size_t)k * 8 + (size_t)k / 8 + 1);
                    m->cap = m->ts && m->v ? k : 0;
                    if (!m->cap) {
                        c->broken = true; /* the rest of the reply stays unread */
                        return TSDB_ERR_NOMEM;
                    }
                }
                memcpy(m->ts, b + 8, (size_t)k * 8);
                memcpy(m->v, b + 8 + (size_t)k * 8, (size_t)k * aw + valid);
                tsdb_series_result rows = r;
                rows.status = 0;
                rows.last = st ? 0 : r.last; /* rows before an error come first, as from tsdb_query_multi */
                rows.n = k;
                rows.ts = m->ts;
                rows.values.values = m->v;
                rows.values.valid = valid ? m->v + (size_t)k * aw : NULL;
                if (deliver(m, &rows) && st)
                    deliver(m, &r);
            } else if (!m->stopped && (r.last || st)) {
                deliver(m, &r); /* the end of the series, or its error */
            }
            open = !r.last;
            next = r.last ? (size_t)ix + 1 : ix;
        } else {
            uint32_t k = 0;
            if (st ? len != 0 : len < 4 || (memcpy(&k, b, 4), len != 4 + (size_t)k * row))
                goto corrupt;
            r.n = k;
            r.buckets = st ? NULL : b + 4; /* 8-aligned in the frame buffer: the structs are the wire rows */
            deliver(m, &r);
            next = (size_t)ix + 1;
        }
        if (m->stopped && !cancel_sent) {
            if ((rc = send_cancel(c, id)) != 0)
                return rc;
            cancel_sent = true;
        }
    }
corrupt:
    c->broken = true;
    return TSDB_ERR_CORRUPT;
}

static void put64(uint8_t *p, int64_t v) { memcpy(p, &v, 8); }

static uint32_t c_next_id(const tsdbc *c) { return c->next_id; } /* the id send_reqv gives the next request */

/* The template of q: the inner op's payload for series 0xffffffff, with 0 where per-series arrays take over.
 * bounds (n > 0) replace q's group by client bounds. */
static int multi_template(const tsdb_multi_query *q, const int64_t *bounds, size_t nb, uint8_t *op, uint8_t *fields,
                          uint8_t **out, size_t *tlen)
{
    int64_t start = q->starts ? 0 : q->start, end = q->ends ? 0 : q->end;
    bool spread = q->kind == TSDB_MULTI_SPREAD;
    const tsdb_group *g = q->group;
    if (g && g->unit == 0)
        g = NULL; /* as tsdb_query_group: no unit, one bucket */
    int64_t interval = q->intervals ? 0 : q->interval_ms;
    size_t len;
    *fields = spread ? TSDB_SPREAD : 0;
    switch (q->kind) {
    case TSDB_MULTI_VALUES: *op = TSDB_OP_QUERY_VALUES, len = 29; break;
    case TSDB_MULTI_LTTB: *op = TSDB_OP_QUERY_LTTB, len = 37; break;
    case TSDB_MULTI_BUCKETS:
    case TSDB_MULTI_SPREAD:
        if (nb)
            *op = TSDB_OP_QUERY_BOUNDS, len = 33 + nb * 8 + spread;
        else
            *op = TSDB_OP_QUERY_BUCKETS, len = 44 + spread;
        break;
    case TSDB_MULTI_TWA: *op = TSDB_OP_QUERY_TWA, len = TSDB_TWA_HEADER + nb * 8; break;
    default: return TSDB_ERR_INVALID;
    }
    uint8_t *t = calloc(len, 1);
    if (!t)
        return TSDB_ERR_NOMEM;
    uint32_t any = TSDB_MANY_SERIES, cnt = g ? g->count : 0, n32 = (uint32_t)nb;
    memcpy(t, &any, 4);
    put64(t + 4, start);
    put64(t + 12, end);
    uint64_t lim = q->opts.limit;
    uint8_t desc = q->opts.order == TSDB_DESC;
    switch (*op) {
    case TSDB_OP_QUERY_VALUES:
        t[20] = desc;
        memcpy(t + 21, &lim, 8);
        break;
    case TSDB_OP_QUERY_LTTB: {
        uint64_t th = q->threshold;
        memcpy(t + 20, &th, 8);
        t[28] = desc;
        memcpy(t + 29, &lim, 8);
        break;
    }
    case TSDB_OP_QUERY_BOUNDS:
        t[20] = desc;
        memcpy(t + 21, &lim, 8);
        memcpy(t + 29, &n32, 4);
        memcpy(t + 33, bounds, nb * 8);
        if (spread)
            t[33 + nb * 8] = TSDB_SPREAD;
        break;
    case TSDB_OP_QUERY_BUCKETS:
        put64(t + 20, g ? 0 : interval);
        t[28] = g ? (uint8_t)g->unit : 0;
        memcpy(t + 29, &cnt, 4);
        t[33] = desc;
        memcpy(t + 34, &lim, 8);
        if (spread)
            t[44] = TSDB_SPREAD;
        break;
    case TSDB_OP_QUERY_TWA: {
        uint64_t tl = q->twa.limit;
        t[20] = (uint8_t)q->twa.method;
        t[21] = q->twa.order == TSDB_DESC;
        t[22] = nb || !g ? 0 : (uint8_t)g->unit;
        put64(t + 24, q->twa.max_gap_ms);
        memcpy(t + 32, &tl, 8);
        put64(t + 40, nb || g ? 0 : interval);
        uint32_t c2 = nb ? 0 : cnt;
        memcpy(t + 48, &c2, 4);
        memcpy(t + 52, &n32, 4);
        if (nb) /* bounds is NULL without them, and memcpy from NULL is undefined even for 0 bytes */
            memcpy(t + TSDB_TWA_HEADER, bounds, nb * 8);
        break;
    }
    }
    *out = t;
    *tlen = len;
    return 0;
}

/* q over series[0 .. n-1] in requests of at most TSDB_MULTI_MAX_SERIES; bounds as in multi_template, trimmed
 * per series by the server as `trim` asks. */
static int multi_send_all(mrun *m, const tsdb_multi_query *q, const uint32_t *series, size_t n, const int64_t *bounds,
                          size_t nb, unsigned trim)
{
    uint8_t op, fields, *t;
    size_t tlen;
    int rc = multi_template(q, bounds, nb, &op, &fields, &t, &tlen);
    if (rc)
        return rc;
    bool interval_mode = !nb && !(q->group && q->group->unit) &&
                         (q->kind == TSDB_MULTI_BUCKETS || q->kind == TSDB_MULTI_SPREAD || q->kind == TSDB_MULTI_TWA);
    uint8_t flags = (q->starts ? TSDB_MULTI_PER_START : 0) | (q->ends ? TSDB_MULTI_PER_END : 0) |
                    (q->intervals && interval_mode ? TSDB_MULTI_PER_INTERVAL : 0) |
                    (nb && (trim & TSDB_MULTI_TRIM_LO) ? TSDB_MULTI_WIRE_TRIM_LO : 0) |
                    (nb && (trim & TSDB_MULTI_TRIM_HI) ? TSDB_MULTI_WIRE_TRIM_HI : 0);
    int threads = m->mo ? m->mo->threads : 0;
    uint16_t th = threads <= 0 ? 0 : threads > 65535 ? 65535 : (uint16_t)threads;
    static const uint8_t zeros[8];
    for (size_t at = 0; at < n && rc == 0; at += TSDB_MULTI_MAX_SERIES) {
        size_t k = n - at < TSDB_MULTI_MAX_SERIES ? n - at : TSDB_MULTI_MAX_SERIES;
        uint64_t left = 0;
        if (m->max_rows) {
            if (m->rows >= m->max_rows) {
                rc = TSDB_ERR_TOO_MANY_BUCKETS;
                break;
            }
            left = m->max_rows - m->rows;
        }
        uint8_t h[TSDB_MULTI_HEADER] = {op, flags};
        uint32_t k32 = (uint32_t)k, tl32 = (uint32_t)tlen;
        memcpy(h + 2, &th, 2);
        memcpy(h + 4, &k32, 4);
        memcpy(h + 8, &left, 8);
        memcpy(h + 16, &tl32, 4);
        struct iovec parts[8];
        int np = 1;
        parts[np++] = (struct iovec){h, sizeof h};
        if (q->starts)
            parts[np++] = (struct iovec){(void *)(q->starts + at), k * 8};
        if (q->ends)
            parts[np++] = (struct iovec){(void *)(q->ends + at), k * 8};
        if (flags & TSDB_MULTI_PER_INTERVAL)
            parts[np++] = (struct iovec){(void *)(q->intervals + at), k * 8};
        parts[np++] = (struct iovec){(void *)(series + at), k * 4};
        parts[np++] = (struct iovec){(void *)zeros, (8 - (k * 4) % 8) % 8};
        parts[np++] = (struct iovec){t, tlen};
        size_t body = 5;
        for (int i = 1; i < np; i++)
            body += parts[i].iov_len;
        if (body > TSDB_MAX_FRAME) {
            rc = TSDB_ERR_TOO_MANY_BUCKETS; /* bounds that no frame carries */
            break;
        }
        uint32_t id = c_next_id(m->c);
        if ((rc = send_reqv(m->c, TSDB_OP_QUERY_MULTI, parts, np)) == 0)
            rc = multi_read(m, id, op, fields, series + at, k, at);
    }
    free(t);
    return rc;
}

/* ---- local periods ---- */

typedef struct probe {
    int64_t *lo, *hi;
    bool desc;
} probe;

static int probe_fn(void *ctx, const tsdb_series_result *r)
{
    probe *p = ctx;
    if (r->n && !r->status)
        (p->desc ? p->hi : p->lo)[r->index] = r->ts[0];
    return 0;
}

/* One series of a local-period query, by the single-series calls: for bounds no frame carries, and for TWA
 * ranges whose closed sides differ between series. */
static int multi_single(mrun *m, const tsdb_multi_query *q, const uint32_t *series, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        int64_t s = q->starts ? q->starts[i] : q->start, e = q->ends ? q->ends[i] : q->end;
        void *b = NULL;
        size_t nb = 0;
        int st;
        if (q->kind == TSDB_MULTI_TWA)
            st = tsdbc_query_twa_group(m->c, series[i], s, e, q->group, &q->twa, (tsdb_twa_bucket **)&b, &nb);
        else if (q->kind == TSDB_MULTI_SPREAD)
            st = tsdbc_query_spread_group(m->c, series[i], s, e, q->group, &q->opts, (tsdb_spread_bucket **)&b, &nb);
        else
            st = tsdbc_query_group(m->c, series[i], s, e, q->group, &q->opts, (tsdb_bucket **)&b, &nb);
        if (m->c->broken) {
            free(b);
            return st ? st : TSDB_ERR_IO;
        }
        if (st == 0 && m->max_rows && m->rows + nb > m->max_rows) {
            free(b);
            return TSDB_ERR_TOO_MANY_BUCKETS;
        }
        tsdb_series_result r = {.index = i, .series = series[i], .status = st, .last = 1, .n = st ? 0 : nb,
                                .buckets = st ? NULL : b};
        bool go = deliver(m, &r);
        free(b);
        if (!go)
            return TSDB_ERR_STOPPED;
    }
    return 0;
}

/* all equal (*v set), all open (INT64_MIN / INT64_MAX: *v that), or mixed (false) */
static bool uniform(const int64_t *a, int64_t one, size_t n, int64_t *v)
{
    *v = a ? a[0] : one;
    for (size_t i = 1; a && i < n; i++)
        if (a[i] != a[0])
            return false;
    return true;
}

static int multi_local(mrun *m, const tsdb_multi_query *q, const uint32_t *series, size_t n)
{
    bool twa = q->kind == TSDB_MULTI_TWA;
    int64_t s0, e0;
    bool us = uniform(q->starts, q->start, n, &s0), ue = uniform(q->ends, q->end, n, &e0);
    /* buckets cut both sides to the rows; TWA only open sides (a closed one is the range's own edge) */
    bool open_lo = !twa || (us && s0 == INT64_MIN), open_hi = !twa || (ue && e0 == INT64_MAX);
    if (twa && ((!us && !open_lo) || (!ue && !open_hi)))
        return multi_single(m, q, series, n);
    int64_t *lo = malloc(n * 8), *hi = malloc(n * 8);
    int rc = lo && hi ? 0 : TSDB_ERR_NOMEM;
    for (size_t i = 0; rc == 0 && i < n; i++)
        lo[i] = INT64_MAX, hi[i] = INT64_MIN;
    if (rc == 0 && (open_lo || open_hi)) {
        mrun pm = {.c = m->c};
        probe p = {lo, hi, false};
        tsdb_multi_query vq = {.kind = TSDB_MULTI_VALUES, .start = q->start, .end = q->end, .starts = q->starts,
                               .ends = q->ends, .opts = {TSDB_ASC, 1}};
        pm.fn = probe_fn;
        pm.ctx = &p;
        if (open_lo)
            rc = multi_send_all(&pm, &vq, series, n, NULL, 0, 0);
        if (rc == 0 && open_hi) {
            p.desc = true;
            vq.opts.order = TSDB_DESC;
            rc = multi_send_all(&pm, &vq, series, n, NULL, 0, 0);
        }
        free(pm.ts);
        free(pm.v);
    }
    int64_t L = INT64_MAX, H = INT64_MIN;
    for (size_t i = 0; rc == 0 && i < n; i++) {
        if (open_lo && lo[i] < L)
            L = lo[i];
        if (open_hi && hi[i] > H)
            H = hi[i];
    }
    if (!open_lo)
        L = s0;
    if (!open_hi)
        H = e0 == INT64_MIN ? INT64_MIN : e0 - 1;
    free(lo);
    free(hi);
    if (rc)
        return rc;
    if (L > H) { /* no rows anywhere (or an empty range): no buckets, as the single call answers */
        for (size_t i = 0; i < n; i++) {
            tsdb_series_result r = {.index = i, .series = series[i], .last = 1};
            if (!deliver(m, &r))
                return TSDB_ERR_STOPPED;
        }
        return 0;
    }
    int64_t *bounds;
    size_t nb;
    rc = tsdb_calendar_bounds(q->group->timezone, q->group->unit, q->group->count, L, H + 1, &bounds, &nb);
    if (rc)
        return rc;
    if (nb > (TSDB_MAX_FRAME - ((size_t)8 << 20)) / 8) /* the union is too long for one frame: one by one */
        rc = multi_single(m, q, series, n);
    else
        rc = multi_send_all(m, q, series, n, bounds, nb,
                            (open_lo ? TSDB_MULTI_TRIM_LO : 0) | (open_hi ? TSDB_MULTI_TRIM_HI : 0));
    free(bounds);
    return rc;
}

int tsdbc_query_multi(tsdbc *c, const tsdb_multi_query *q, const uint32_t *series, size_t n,
                      const tsdb_multi_opts *mo, tsdb_multi_fn fn, void *ctx)
{
    int rc = tsdbc_wait(c);
    if (rc)
        return rc;
    if (!q || !fn || (n && !series) || (q->nbounds && !q->bounds) ||
        (q->trim & ~(TSDB_MULTI_TRIM_LO | TSDB_MULTI_TRIM_HI)))
        return TSDB_ERR_INVALID;
    if (server_version(c) < 10)
        return c->broken ? TSDB_ERR_IO : TSDB_ERR_INVALID;
    if (n == 0)
        return 0;
    mrun m = {.c = c, .mo = mo, .fn = fn, .ctx = ctx, .max_rows = mo ? mo->max_rows : 0};
    bool buckets = q->kind == TSDB_MULTI_BUCKETS || q->kind == TSDB_MULTI_SPREAD || q->kind == TSDB_MULTI_TWA;
    bool local = buckets && !q->nbounds && q->group && q->group->unit && q->group->timezone &&
                 strcmp(q->group->timezone, "UTC") != 0;
    if (local)
        rc = multi_local(&m, q, series, n);
    else
        rc = multi_send_all(&m, q, series, n, buckets ? q->bounds : NULL, buckets ? q->nbounds : 0, q->trim);
    free(m.ts);
    free(m.v);
    return rc;
}

/* ---- collected ---- */

typedef struct mcollect {
    tsdb_series_result *r;
    size_t stride; /* bytes per bucket; 0: rows */
    size_t *cap;   /* rows allocated per series */
    bool *nulls;   /* the series had a null */
    int err;
} mcollect;

static int collect_fn(void *ctx, const tsdb_series_result *in)
{
    mcollect *k = ctx;
    tsdb_series_result *o = &k->r[in->index];
    o->status = in->status;
    if (in->status || !in->n)
        return 0;
    if (k->stride) {
        void *b = malloc(in->n * k->stride);
        if (!b)
            return k->err = TSDB_ERR_NOMEM;
        memcpy(b, in->buckets, in->n * k->stride);
        o->buckets = b;
        o->n = in->n;
        return 0;
    }
    unsigned aw = tsdb_wire_width(in->values.type);
    size_t need = o->n + in->n, *cap = &k->cap[in->index];
    if (need > *cap) {
        size_t nc = *cap ? *cap * 2 : 1024;
        while (nc < need)
            nc *= 2;
        int64_t *ts = realloc((void *)o->ts, nc * 8);
        if (ts)
            o->ts = ts;
        uint8_t *v = realloc((void *)o->values.values, nc * aw);
        if (v)
            o->values.values = v;
        uint8_t *bits = realloc((void *)o->values.valid, (nc + 7) / 8);
        if (bits)
            o->values.valid = bits;
        if (!ts || !v || !bits)
            return k->err = TSDB_ERR_NOMEM;
        memset(bits + (*cap + 7) / 8, 0, (nc + 7) / 8 - (*cap + 7) / 8);
        *cap = nc;
    }
    uint8_t *bits = (uint8_t *)o->values.valid;
    memcpy((int64_t *)o->ts + o->n, in->ts, in->n * 8);
    memcpy((uint8_t *)o->values.values + o->n * aw, in->values.values, in->n * aw);
    for (size_t i = 0; i < in->n; i++) {
        bool known = !in->values.valid || ((in->values.valid[i >> 3] >> (i & 7)) & 1);
        size_t j = o->n + i;
        bits[j >> 3] = (uint8_t)((bits[j >> 3] & ~(1u << (j & 7))) | ((unsigned)known << (j & 7)));
        k->nulls[in->index] |= !known;
    }
    o->values.type = in->values.type;
    o->n = need;
    return 0;
}

int tsdbc_query_multi_collect(tsdbc *c, const tsdb_multi_query *q, const uint32_t *series, size_t n,
                              const tsdb_multi_opts *mo, tsdb_series_result **out)
{
    *out = NULL;
    if (!q)
        return TSDB_ERR_INVALID;
    mcollect k = {calloc(n ? n : 1, sizeof *k.r), 0, calloc(n ? n : 1, sizeof(size_t)), calloc(n ? n : 1, 1), 0};
    k.stride = q->kind == TSDB_MULTI_TWA      ? sizeof(tsdb_twa_bucket)
               : q->kind == TSDB_MULTI_SPREAD ? sizeof(tsdb_spread_bucket)
               : q->kind == TSDB_MULTI_BUCKETS ? sizeof(tsdb_bucket)
                                               : 0;
    int rc = k.r && k.cap && k.nulls ? 0 : TSDB_ERR_NOMEM;
    for (size_t i = 0; rc == 0 && i < n; i++) {
        k.r[i].index = i;
        k.r[i].series = series[i];
        k.r[i].last = 1;
    }
    if (rc == 0)
        rc = tsdbc_query_multi(c, q, series, n, mo, collect_fn, &k);
    if (rc == TSDB_ERR_STOPPED && k.err)
        rc = k.err;
    for (size_t i = 0; k.r && k.nulls && i < n; i++)
        if (!k.nulls[i]) { /* no nulls: no bitmap, as tsdb_values has it */
            free((void *)k.r[i].values.valid);
            k.r[i].values.valid = NULL;
        }
    free(k.cap);
    free(k.nulls);
    if (rc && rc != TSDB_ERR_STOPPED && rc != TSDB_ERR_TOO_MANY_BUCKETS) {
        tsdbc_series_results_free(k.r, k.r ? n : 0);
        return rc;
    }
    *out = k.r;
    return rc;
}

void tsdbc_series_results_free(tsdb_series_result *r, size_t n)
{
    if (!r)
        return;
    for (size_t i = 0; i < n; i++) {
        free((void *)r[i].ts);
        free((void *)r[i].values.values);
        free((void *)r[i].values.valid);
        free((void *)r[i].buckets);
    }
    free(r);
}
