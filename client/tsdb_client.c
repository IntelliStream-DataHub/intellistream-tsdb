/* Client for tsdbd: blocking sockets, columnar frames, optional pipelining. */
#define _GNU_SOURCE
#include "tsdb_client.h"
#include "tsdb_protocol.h"

#include <errno.h>
#include <netdb.h>
#include <stdbool.h>
#include <stdio.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

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
};

int tsdbc_connect(const char *host, int port, tsdbc **out)
{
    *out = NULL;
    char portstr[16];
    snprintf(portstr, sizeof portstr, "%d", port);
    struct addrinfo hints = {.ai_socktype = SOCK_STREAM}, *ai;
    if (getaddrinfo(host, portstr, &hints, &ai) != 0)
        return TSDB_ERR_IO;
    int fd = -1;
    for (struct addrinfo *a = ai; a; a = a->ai_next) {
        fd = socket(a->ai_family, a->ai_socktype | SOCK_CLOEXEC, a->ai_protocol);
        if (fd >= 0 && connect(fd, a->ai_addr, a->ai_addrlen) == 0)
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
    if (c->pending)
        tsdbc_wait(c);
    close(c->fd);
    free(c->rbuf);
    free(c->frame);
    free(c);
}

/* ---------------------------------------------------------------- I/O */

static int send_iov(tsdbc *c, struct iovec *iov, int n)
{
    while (n) {
        ssize_t w = writev(c->fd, iov, n);
        if (w < 0) {
            if (errno == EINTR)
                continue;
            return TSDB_ERR_IO;
        }
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
    uint8_t *o = dst;
    while (n) {
        if (c->roff == c->rlen) {
            if (n >= c->rcap) { /* large bodies straight into place */
                ssize_t r = recv(c->fd, o, n, 0);
                if (r <= 0) {
                    if (r < 0 && errno == EINTR)
                        continue;
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

/* Reads one reply frame; *body points at the payload after the status. */
static int read_reply(tsdbc *c, int32_t *status, const uint8_t **body, size_t *blen)
{
    uint8_t hdr[13];
    int rc = read_exact(c, hdr, 13);
    if (rc)
        return rc;
    uint32_t len;
    memcpy(&len, hdr, 4);
    if (len < 9 || len > TSDB_MAX_FRAME || hdr[8] != TSDB_OP_REPLY)
        return TSDB_ERR_CORRUPT;
    memcpy(status, hdr + 9, 4);
    size_t n = len - 9;
    if (n > c->fcap) {
        uint8_t *f = realloc(c->frame, n);
        if (!f)
            return TSDB_ERR_NOMEM;
        c->frame = f;
        c->fcap = n;
    }
    if ((rc = read_exact(c, c->frame, n)) != 0)
        return rc;
    *body = c->frame;
    *blen = n;
    return 0;
}

int tsdbc_wait(tsdbc *c)
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
    int rc = c->pending_rc;
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
    if (len == 0)
        return TSDB_ERR_INVALID;
    size_t k = l < len - 1 ? l : len - 1;
    memcpy(buf, b + 2, k);
    buf[k] = '\0';
    return 0;
}

static int insert_frames(tsdbc *c, const uint32_t *series, uint32_t s, const int64_t *ts, const float *v, size_t n,
                         bool wait)
{
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
            break;
        }
        if (n && !stopped) {
            if (n > cap) { /* aligned copies of the columns */
                free(ts);
                free(v);
                ts = malloc((size_t)n * 8);
                v = malloc((size_t)n * 4);
                cap = ts && v ? n : 0;
                if (!cap) {
                    rc = TSDB_ERR_NOMEM;
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

static int read_buckets(const uint8_t *b, size_t len, tsdb_bucket **out, size_t *nout)
{
    uint32_t n;
    if (len < 4 || (memcpy(&n, b, 4), len != 4 + (size_t)n * TSDB_BUCKET_ROW))
        return TSDB_ERR_CORRUPT;
    tsdb_bucket *r = malloc((n ? n : 1) * sizeof *r);
    if (!r)
        return TSDB_ERR_NOMEM;
    for (uint32_t i = 0; i < n; i++) {
        const uint8_t *o = b + 4 + (size_t)i * TSDB_BUCKET_ROW;
        memcpy(&r[i].start, o, 8);
        memcpy(&r[i].count, o + 8, 8);
        memcpy(&r[i].sum, o + 16, 8);
        memcpy(&r[i].min, o + 24, 8);
        memcpy(&r[i].max, o + 32, 8);
        memcpy(&r[i].isum, o + 40, 8);
        memcpy(&r[i].imin, o + 48, 8);
        memcpy(&r[i].imax, o + 56, 8);
    }
    *out = r;
    *nout = n;
    return 0;
}

/* QUERY_BUCKETS: fixed intervals (group NULL) or calendar groups */
static int buckets_req(tsdbc *c, uint32_t series, int64_t start, int64_t end, int64_t interval,
                       const tsdb_group *group, const tsdb_query_opts *opts, tsdb_bucket **out, size_t *nout)
{
    *out = NULL;
    *nout = 0;
    const char *zone = group && group->timezone ? group->timezone : "";
    size_t zl = strlen(zone);
    if (zl > 65535)
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
    const uint8_t *b;
    size_t len;
    int rc = call(c, TSDB_OP_QUERY_BUCKETS, q, 44, zone, zl, &b, &len);
    return rc ? rc : read_buckets(b, len, out, nout);
}

int tsdbc_query_agg(tsdbc *c, uint32_t series, int64_t start, int64_t end, int64_t interval_ms, tsdb_bucket **out,
                    size_t *nout)
{
    if (interval_ms < 0) {
        *out = NULL;
        *nout = 0;
        return TSDB_ERR_INVALID;
    }
    return buckets_req(c, series, start, end, interval_ms, NULL, NULL, out, nout);
}

int tsdbc_query_group(tsdbc *c, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                      const tsdb_query_opts *opts, tsdb_bucket **out, size_t *nout)
{
    return buckets_req(c, series, start, end, 0, group, opts, out, nout);
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

static int insert_values_frames(tsdbc *c, const uint32_t *series, uint32_t s, const int64_t *ts,
                                const tsdb_values *v, size_t n, bool wait)
{
    if (!v || (unsigned)v->type > TSDB_STATE || (n && (!ts || !v->values)))
        return TSDB_ERR_INVALID;
    unsigned aw = tsdb_wire_width(v->type);
    int rc = 0;
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
            break;
        }
        unsigned aw = tsdb_wire_width(b[1]);
        size_t valid = (b[2] & TSDB_VALID) ? ((size_t)n + 7) / 8 : 0;
        if (len != 8 + (size_t)n * (8 + aw) + valid) {
            rc = TSDB_ERR_CORRUPT;
            break;
        }
        if (n && !stopped) {
            if (n > cap) { /* aligned copies of the columns */
                free(ts);
                free(v);
                ts = malloc((size_t)n * 8);
                v = malloc((size_t)n * 8 + (size_t)n / 8 + 1);
                cap = ts && v ? n : 0;
                if (!cap) {
                    rc = TSDB_ERR_NOMEM;
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
    if (len == 0)
        return TSDB_ERR_INVALID;
    size_t k = l < len - 1 ? l : len - 1;
    memcpy(buf, b + 2, k);
    buf[k] = '\0';
    return 0;
}

int tsdbc_commit(tsdbc *c) { return call(c, TSDB_OP_COMMIT, NULL, 0, NULL, 0, NULL, NULL); }
int tsdbc_flush(tsdbc *c) { return call(c, TSDB_OP_FLUSH, NULL, 0, NULL, 0, NULL, NULL); }
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
    *st = (tsdb_stats){v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7]};
    return 0;
}
