/* SPDX-License-Identifier: Apache-2.0 */
/* Client for tsdbd: blocking sockets, columnar frames, optional pipelining. */
#define _GNU_SOURCE
#include "tsdb_calendar.h"
#include "tsdb_client.h"
#include "tsdb_crypto.h"
#include "tsdb_pack.h"
#include "tsdb_protocol.h"
#include "tsdb_wire.h"

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdbool.h>
#include <stdio.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>
#include <zstd.h>

#ifdef __APPLE__ /* no SOCK_CLOEXEC, MSG_NOSIGNAL or TCP_KEEPIDLE: fcntl, SO_NOSIGPIPE and TCP_KEEPALIVE instead */
#define SOCK_CLOEXEC 0
#define TCP_KEEPIDLE TCP_KEEPALIVE
#endif

#define INSERT_FRAME_ROWS (8u << 20)
/* the largest frame: f64 values, a bitmap and an id per row (unpacked), or their packed bound with zstd's margin */
_Static_assert(TSDB_VALUES_HEADER + (uint64_t)INSERT_FRAME_ROWS * 24 + INSERT_FRAME_ROWS / 8 + 8 <= TSDB_MAX_FRAME &&
                   TSDB_VALUES_HEADER + 4 + (uint64_t)INSERT_FRAME_ROWS * 29 + (1u << 20) <= TSDB_MAX_FRAME,
               "INSERT_FRAME_ROWS rows fit a frame");
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
    uint16_t hello_flags;    /* TSDB_HELLO_AUTH: the server requires a login */
    bool scram;              /* the server offers SCRAM-SHA-256 */
    ZSTD_CCtx *zc;
    uint8_t *raw, *zbuf;
    size_t raw_cap, zbuf_cap;
    uint64_t sent;   /* bytes written to the socket */
    uint32_t expect; /* the id of the oldest request whose reply is not read yet */
    /* Set by an I/O error (a timeout included) or a reply out of step: part of
     * a frame may be unsent or unread, so the next reply read could answer an
     * earlier request. Every later call fails with TSDB_ERR_IO. */
    bool broken;
    /* Protocol 13: retry, and the nodes to go to. Every request that can be sent again is kept (a copy of its frame)
     * until its reply is read; UNAVAILABLE and MOVED, which mean "not applied", send it again (see read_reply and
     * drain). */
    int retry_ms;            /* the time budget of one request's retries; 0: none (tsdbc_set_retry) */
    uint8_t last_op;         /* of the request sent last: where an error reply's body starts */
    struct sent {
        uint32_t id;
        uint8_t op;
        size_t len;
        uint8_t *p;          /* the whole frame, header included */
    } *q;
    size_t qhead, qlen, qcap, qbytes;
    bool stream_valid;       /* a reply of stream_id has been delivered in part: it cannot be sent again */
    uint32_t stream_id;
    bool gave_up;            /* read_reply stopped retrying (the budget): drain must not start over */
    int timeout_ms;
    tsdbc_err_info err;
    char **nodes;            /* the connect string's nodes (shuffled once) for a reconnect after MOVED */
    size_t nnodes, node_at;
    char *user, *password, *password_file;
    int connect_ms, require_auth;
    /* tsdbc_latest_by_name: names already resolved to ids (an id never changes meaning, so it is never stale) */
    struct name_ent {
        char *name;
        uint64_t h;
        tsdb_sid id;
    } *nc;
    size_t nc_cap, nc_n;
};

#define QCAP_FRAMES 1024u            /* pipelined inserts kept for a resend: drain before there are more */
#define QCAP_BYTES ((size_t)128 << 20)

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

static int check_server_version(tsdbc *c);
static int login(tsdbc *c, const tsdbc_options *o);

int tsdbc_connect_timeout(const char *host, int port, int connect_ms, tsdbc **out)
{
    if (connect_ms < 0) {
        *out = NULL;
        return TSDB_ERR_INVALID;
    }
    tsdbc_options o = {.connect_ms = connect_ms ? connect_ms : -1};
    return tsdbc_connect_opts(host, port, &o, out);
}

/* The password rule (tsdb_client.h): 1 to TSDBC_PASSWORD_MAX bytes. */
static bool password_ok(const char *pw)
{
    return pw && pw[0] && strnlen(pw, TSDBC_PASSWORD_MAX + 1) <= TSDBC_PASSWORD_MAX;
}

int tsdbc_connect_auth(const char *host, int port, const char *user, const char *password, tsdbc **out)
{
    tsdbc_options o = {.user = user, .password = password};
    return tsdbc_connect_opts(host, port, &o, out);
}

/* A connected socket to host:port within connect_ms (0: as long as the kernel does), or -1. */
static int open_socket(const char *host, int port, int connect_ms)
{
    char portstr[16];
    snprintf(portstr, sizeof portstr, "%d", port);
    struct addrinfo hints = {.ai_socktype = SOCK_STREAM}, *ai;
    if (getaddrinfo(host, portstr, &hints, &ai) != 0)
        return -1;
    int fd = -1;
    for (struct addrinfo *a = ai; a; a = a->ai_next) {
        fd = socket(a->ai_family, a->ai_socktype | SOCK_CLOEXEC, a->ai_protocol);
#ifdef __APPLE__
        if (fd >= 0) {
            int one = 1;
            fcntl(fd, F_SETFD, FD_CLOEXEC);
            setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof one); /* the library must not ignore SIGPIPE itself */
        }
#endif
        if (fd >= 0 && connect_within(fd, a->ai_addr, a->ai_addrlen, connect_ms) == 0)
            break;
        if (fd >= 0)
            close(fd);
        fd = -1;
    }
    freeaddrinfo(ai);
    if (fd >= 0) {
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    }
    return fd;
}

int tsdbc_connect_opts(const char *host, int port, const tsdbc_options *o, tsdbc **out)
{
    *out = NULL;
    if (o->user && (o->password ? !password_ok(o->password) : !o->password_file))
        return TSDB_ERR_INVALID;
    int connect_ms = o->connect_ms == 0 ? TSDBC_CONNECT_TIMEOUT_MS : o->connect_ms < 0 ? 0 : o->connect_ms;
    int fd = open_socket(host, port, connect_ms);
    if (fd < 0)
        return TSDB_ERR_IO;
    tsdbc *c = calloc(1, sizeof *c);
    if (!c || !(c->rbuf = malloc(RBUF))) {
        free(c);
        close(fd);
        return TSDB_ERR_NOMEM;
    }
    c->fd = fd;
    c->rcap = RBUF;
    c->zlevel = 1;
    c->retry_ms = TSDB_BUDGET_MS;
    c->connect_ms = o->connect_ms;
    c->require_auth = o->require_auth;
    if ((o->user && !(c->user = strdup(o->user))) || (o->password && !(c->password = strdup(o->password))) ||
        (o->password_file && !(c->password_file = strdup(o->password_file)))) {
        tsdbc_close(c);
        return TSDB_ERR_NOMEM;
    }
    /* Best effort, as in the other clients: a socket without the probe options still works. */
    tsdbc_set_keepalive(c, TSDBC_KEEPALIVE_IDLE_S, TSDBC_KEEPALIVE_INTERVAL_S, TSDBC_KEEPALIVE_COUNT);
    /* HELLO and the login now, within the connect timeout: an older server is refused before any request reaches
     * it, and one that cannot prove it holds the user's verifier before anything is sent to it */
    struct timeval tv = {connect_ms / 1000, (connect_ms % 1000) * 1000}, none = {0, 0};
    if (connect_ms > 0)
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    int rc = check_server_version(c);
    if (rc == 0)
        rc = login(c, o);
    if (connect_ms > 0)
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &none, sizeof none);
    if (rc) {
        tsdbc_close(c);
        return rc;
    }
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
    case TSDB_ERR_MOVED: return "the volume is owned by another node";
    case TSDB_ERR_UNAVAILABLE: return "the volume is unavailable (storage down or recovering); not applied";
    case TSDB_ERR_PARTIAL: return "applied for some volumes only";
    case TSDB_ERR_DROPPED: return "below the volume's drop watermark; never applied";
    case TSDB_ERR_DENIED: return "permission denied";
    case TSDB_ERR_AUTH: return "login failed or required";
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
    for (size_t i = 0; i < c->qlen; i++)
        free(c->q[c->qhead + i].p);
    free(c->q);
    for (size_t i = 0; i < c->nnodes; i++)
        free(c->nodes[i]);
    free(c->nodes);
    free(c->user);
    if (c->password) {
        tsdb_wipe(c->password, strlen(c->password));
        free(c->password);
    }
    free(c->password_file);
    for (size_t i = 0; i < c->nc_cap; i++)
        free(c->nc[i].name);
    free(c->nc);
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

static int drain(tsdbc *c);

/* The requests kept for a resend: those whose reply is not read yet, oldest first. Entries before c->expect are
 * done and go; frames of a stream that was delivered in part are never sent again (see read_reply). */
static void q_trim(tsdbc *c)
{
    while (c->qlen && (int32_t)(c->q[c->qhead].id - c->expect) < 0) {
        c->qbytes -= c->q[c->qhead].len;
        free(c->q[c->qhead].p);
        c->qhead++;
        c->qlen--;
    }
    if (!c->qlen)
        c->qhead = 0;
}

static bool retained_op(uint8_t op)
{
    return op != TSDB_OP_HELLO && op != TSDB_OP_AUTH_START && op != TSDB_OP_AUTH_CONTINUE &&
           op != TSDB_OP_QUERY_MULTI && op != TSDB_OP_CANCEL;
}

static struct sent *q_find(tsdbc *c, uint32_t id)
{
    for (size_t i = 0; i < c->qlen; i++)
        if (c->q[c->qhead + i].id == id)
            return &c->q[c->qhead + i];
    return NULL;
}

/* Keeps a copy of the frame about to be sent: header and the pieces iov[0..n) of its payload. */
static int q_keep(tsdbc *c, uint8_t op, uint32_t id, const struct iovec *iov, int n)
{
    c->last_op = op;
    if (c->retry_ms <= 0 || !retained_op(op))
        return 0;
    if (c->pending && (c->qlen >= QCAP_FRAMES || c->qbytes >= QCAP_BYTES)) { /* the window: collect (and resend) */
        int rc = drain(c);
        if (rc)
            return rc;
    }
    q_trim(c);
    size_t total = 0;
    for (int i = 0; i < n; i++)
        total += iov[i].iov_len;
    uint8_t *p = malloc(total ? total : 1);
    if (!p)
        return TSDB_ERR_NOMEM;
    size_t at = 0;
    for (int i = 0; i < n; i++) {
        if (iov[i].iov_len)
            memcpy(p + at, iov[i].iov_base, iov[i].iov_len);
        at += iov[i].iov_len;
    }
    if (c->qhead + c->qlen == c->qcap) {
        if (c->qhead) {
            memmove(c->q, c->q + c->qhead, c->qlen * sizeof *c->q);
            c->qhead = 0;
        } else {
            size_t cap = c->qcap ? c->qcap * 2 : 16;
            struct sent *nq = realloc(c->q, cap * sizeof *nq);
            if (!nq) {
                free(p);
                return TSDB_ERR_NOMEM;
            }
            c->q = nq;
            c->qcap = cap;
        }
    }
    c->q[c->qhead + c->qlen++] = (struct sent){id, op, total, p};
    c->qbytes += total;
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
    int rc = q_keep(c, op, id, iov, 5);
    return rc ? rc : send_iov(c, iov, 5);
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
    int rc = q_keep(c, op, id, parts, n);
    return rc ? rc : send_iov(c, parts, n);
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
static void decode_err(tsdbc *c, int32_t st, const uint8_t *b, size_t n, uint8_t op);
static int read_reply_raw(tsdbc *c, int32_t *status, const uint8_t **body, size_t *blen)
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
    if (*status == TSDB_ERR_MOVED || *status == TSDB_ERR_UNAVAILABLE || *status == TSDB_ERR_PARTIAL ||
        *status == TSDB_ERR_DROPPED) {
        struct sent *e = q_find(c, id);
        decode_err(c, *status, c->frame, n, e ? e->op : c->last_op);
    }
    return 0;
}

/* The frame just read is not the last of its stream: the rest of it is still to come, and a delivered part of a
 * reply is never asked for again. */
static void stream_more(tsdbc *c)
{
    c->expect--;
    c->stream_valid = true;
    c->stream_id = c->expect;
}

/* ----------------------------------------------- the body of an error reply, retry, other nodes */

static int64_t mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void sleep_ms(int64_t ms)
{
    struct timespec ts = {(time_t)(ms / 1000), (long)(ms % 1000) * 1000000L};
    while (ms > 0 && nanosleep(&ts, &ts) != 0 && errno == EINTR)
        ;
}

/* c->err from the body of an error reply: after the op's own error shape (tsdb_wire_error_shape). A body that
 * is missing (an older server) or malformed leaves has_body 0: the status alone still tells what to do. */
static void decode_err(tsdbc *c, int32_t st, const uint8_t *b, size_t n, uint8_t op)
{
    tsdbc_err_info *e = &c->err;
    memset(e, 0, sizeof *e);
    e->status = st;
    int off = tsdb_wire_error_shape(op);
    if (off < 0 || (size_t)off > n)
        return;
    b += off;
    n -= (size_t)off;
    unsigned total = 0;
    size_t used = 0;
    switch (st) {
    case TSDB_ERR_UNAVAILABLE: {
        if (tsdb_wire_get_unavail(b, n, e->unavail, TSDBC_ERR_ENTRIES, &total, &used) != 0)
            return;
        int rs = tsdb_wire_get_resend(b + used, n - used, &e->resend_from);
        if (rs < 0)
            return;
        e->has_resend = rs;
        e->unavail_total = total;
        e->nunavail = total < TSDBC_ERR_ENTRIES ? total : TSDBC_ERR_ENTRIES;
        e->has_body = 1;
        return;
    }
    case TSDB_ERR_MOVED: {
        tsdb_moved m[TSDBC_ERR_ENTRIES];
        if (tsdb_wire_get_moved(b, n, m, TSDBC_ERR_ENTRIES, &total, &used) != 0)
            return;
        int rs = tsdb_wire_get_resend(b + used, n - used, &e->resend_from);
        if (rs < 0)
            return;
        e->has_resend = rs;
        e->moved_total = total;
        e->nmoved = total < TSDBC_ERR_ENTRIES ? total : TSDBC_ERR_ENTRIES;
        for (unsigned i = 0; i < e->nmoved; i++) {
            e->moved[i].vol = m[i].vol;
            e->moved[i].epoch = m[i].epoch;
            e->moved[i].flags = m[i].flags;
            e->moved[i].node_id = m[i].node_id;
            memcpy(e->moved[i].addr, m[i].addr, m[i].addr_len);
            e->moved[i].addr[m[i].addr_len] = 0;
        }
        e->has_body = 1;
        return;
    }
    case TSDB_ERR_PARTIAL: {
        tsdb_partial_vol v[TSDBC_ERR_ENTRIES];
        unsigned nv, ne;
        uint32_t rows;
        const uint8_t *bits;
        if (tsdb_wire_get_partial(b, n, v, TSDBC_ERR_ENTRIES, &nv, &rows, &bits, e->unavail, TSDBC_ERR_ENTRIES,
                                  &ne) != 0)
            return;
        e->nvols = nv < TSDBC_ERR_ENTRIES ? nv : TSDBC_ERR_ENTRIES;
        memcpy(e->vols, v, e->nvols * sizeof v[0]);
        e->nrows = rows;
        e->not_applied = bits;
        e->unavail_total = ne;
        e->nunavail = ne < TSDBC_ERR_ENTRIES ? ne : TSDBC_ERR_ENTRIES;
        e->has_body = 1;
        return;
    }
    case TSDB_ERR_DROPPED:
        if (tsdb_wire_get_dropped(b, n, &e->dropped_vol, &e->dropped_before) == 0)
            e->has_body = 1;
        return;
    }
}

int tsdbc_last_error(const tsdbc *c, tsdbc_err_info *out)
{
    *out = c->err;
    return c->err.status;
}

int tsdbc_unavailable(const tsdbc *c, tsdb_unavail *e, unsigned cap, unsigned *n)
{
    if (c->err.status != TSDB_ERR_UNAVAILABLE && c->err.status != TSDB_ERR_PARTIAL)
        return TSDB_ERR_INVALID;
    unsigned k = c->err.nunavail < cap ? c->err.nunavail : cap;
    if (e)
        memcpy(e, c->err.unavail, k * sizeof *e);
    if (n)
        *n = c->err.unavail_total;
    return 0;
}

int tsdbc_set_retry(tsdbc *c, int budget_ms)
{
    if (budget_ms < 0)
        return TSDB_ERR_INVALID;
    if (budget_ms > 0) {
        c->retry_ms = budget_ms;
        return 0;
    }
    if (c->retry_ms == 0)
        return 0; /* already off */
    q_trim(c);    /* the frame of a finished request is kept until the next send: it is not "in flight" */
    if (c->qlen != 0)
        return TSDB_ERR_INVALID; /* with requests in flight (they hold their frames) */
    c->retry_ms = 0;
    return 0;
}

/* "host:port", "[v6]:port" or "host" (the default port) into host and port. */
static int parse_node(const char *s, char *host, size_t cap, int *port)
{
    while (*s == ' ')
        s++;
    const char *end = s + strlen(s);
    while (end > s && end[-1] == ' ')
        end--;
    if (end == s)
        return -1;
    const char *colon = NULL, *h0 = s, *h1 = end;
    if (*s == '[') {
        const char *rb = memchr(s, ']', (size_t)(end - s));
        if (!rb)
            return -1;
        h0 = s + 1;
        h1 = rb;
        if (rb + 1 < end)
            colon = rb[1] == ':' ? rb + 1 : NULL;
        if (rb + 1 < end && !colon)
            return -1;
    } else {
        for (const char *q = s; q < end; q++)
            if (*q == ':')
                colon = colon ? s - 1 : q; /* a second colon: a bare IPv6 address has no port to read */
        if (colon == s - 1) {
            colon = NULL;
        } else if (colon) {
            h1 = colon;
        }
    }
    *port = TSDB_DEFAULT_PORT;
    if (colon) {
        char *pe;
        long v = strtol(colon + 1, &pe, 10);
        if (pe == colon + 1 || pe != end || v < 1 || v > 65535)
            return -1;
        *port = (int)v;
    }
    size_t hl = (size_t)(h1 - h0);
    if (hl == 0 || hl >= cap)
        return -1;
    memcpy(host, h0, hl);
    host[hl] = 0;
    return 0;
}

static int handshake(tsdbc *c, int connect_ms)
{
    tsdbc_options o = {.user = c->user, .password = c->password, .password_file = c->password_file,
                       .require_auth = c->require_auth};
    struct timeval tv = {connect_ms / 1000, (connect_ms % 1000) * 1000}, none = {0, 0};
    if (connect_ms > 0)
        setsockopt(c->fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    int rc = check_server_version(c);
    if (rc == 0)
        rc = login(c, &o);
    if (connect_ms > 0)
        setsockopt(c->fd, SOL_SOCKET, SO_RCVTIMEO, &none, sizeof none);
    return rc;
}

/* Moves the connection to host:port (the requests kept stay kept: the caller sends them again). On failure the
 * old connection is as it was. The connect and the handshake take at most the time left of the retry budget
 * (deadline, a mono_ms value), so a followed MOVED cannot spend a connect timeout per node past it. */
static int reconnect_to(tsdbc *c, const char *host, int port, int64_t deadline)
{
    int connect_ms = c->connect_ms == 0 ? TSDBC_CONNECT_TIMEOUT_MS : c->connect_ms < 0 ? 0 : c->connect_ms;
    int64_t left = deadline - mono_ms();
    if (left <= 0)
        return TSDB_ERR_IO;
    if (connect_ms == 0 || left < connect_ms)
        connect_ms = (int)left;
    int fd = open_socket(host, port, connect_ms);
    if (fd < 0)
        return TSDB_ERR_IO;
    int old = c->fd;
    c->fd = fd;
    size_t pending = c->pending;
    uint32_t next = c->next_id, expect = c->expect;
    c->pending = 0;
    c->rlen = c->roff = 0;
    c->broken = false;
    c->server_version = 0;
    c->hello_flags = 0;
    c->scram = false;
    c->expect = c->next_id; /* the handshake's own ids: the server only echoes them */
    /* the handshake's calls trim the queue of requests kept (everything before c->expect): hide it meanwhile */
    struct sent *kq = c->q;
    size_t khead = c->qhead, klen = c->qlen, kcap = c->qcap, kbytes = c->qbytes;
    c->q = NULL;
    c->qhead = c->qlen = c->qcap = c->qbytes = 0;
    tsdbc_set_keepalive(c, TSDBC_KEEPALIVE_IDLE_S, TSDBC_KEEPALIVE_INTERVAL_S, TSDBC_KEEPALIVE_COUNT);
    int rc = handshake(c, connect_ms);
    free(c->q);
    c->q = kq;
    c->qhead = khead;
    c->qlen = klen;
    c->qcap = kcap;
    c->qbytes = kbytes;
    c->pending = pending;
    c->next_id = next;
    if (rc) {
        close(fd);
        c->fd = old;
        c->broken = false;
        c->expect = expect;
        return rc;
    }
    close(old);
    if (c->timeout_ms > 0)
        tsdbc_set_timeout(c, c->timeout_ms);
    return 0;
}

/* After MOVED: to the address it names, else to another node of the connect string. 0 when connected somewhere
 * new; nonzero when there is nowhere to go (the status stands). */
static int follow_moved(tsdbc *c, const tsdbc_err_info *e, int64_t deadline)
{
    char host[256];
    int port;
    for (unsigned i = 0; i < e->nmoved; i++) {
        if (e->moved[i].addr[0] && parse_node(e->moved[i].addr, host, sizeof host, &port) == 0 &&
            reconnect_to(c, host, port, deadline) == 0)
            return 0;
    }
    for (size_t k = 1; k < c->nnodes; k++) {
        size_t i = (c->node_at + k) % c->nnodes;
        if (parse_node(c->nodes[i], host, sizeof host, &port) == 0 &&
            reconnect_to(c, host, port, deadline) == 0) {
            c->node_at = i;
            return 0;
        }
    }
    return TSDB_ERR_NOTFOUND;
}

static uint32_t retry_delay_ms(const tsdbc_err_info *e)
{
    if (e->status == TSDB_ERR_MOVED)
        return e->nmoved && (e->moved[0].flags & TSDB_MOVED_HANDOFF) ? 200 : 0;
    if (!e->has_body || !e->nunavail)
        return 1000; /* a server without the body (protocol 12) */
    return tsdb_wire_retry_after(e->unavail, e->nunavail);
}

static bool retryable_status(int32_t st) { return st == TSDB_ERR_UNAVAILABLE || st == TSDB_ERR_MOVED; }

/* One step of retrying the request whose reply was UNAVAILABLE or MOVED (c->err describes it): waits the server's
 * retry_after_ms, follows a MOVED. 1: send the request again; 0: give up, the status stands (the budget would be
 * over, or a MOVED with nowhere to go). deadline is 0 until the first refusal of this request. */
static int retry_step(tsdbc *c, const tsdbc_err_info *e, int64_t *deadline, unsigned *tries)
{
    int64_t now = mono_ms();
    if (!*deadline)
        *deadline = now + c->retry_ms;
    int64_t delay = retry_delay_ms(e);
    if (e->status == TSDB_ERR_MOVED && *tries) /* a MOVED that keeps coming: not a tight loop */
        delay = delay > 50 * (int64_t)*tries ? delay : 50 * (int64_t)*tries > 1000 ? 1000 : 50 * (int64_t)*tries;
    if (now + delay > *deadline)
        return 0;
    sleep_ms(delay);
    (*tries)++;
    if (e->status == TSDB_ERR_MOVED && follow_moved(c, e, *deadline) != 0)
        return 0;
    return 1;
}

static int send_kept(tsdbc *c, const struct sent *e)
{
    struct iovec iov = {e->p, e->len};
    return send_iov(c, &iov, 1);
}

/* read_reply_raw, and a request the server refused before doing anything (UNAVAILABLE, MOVED) is sent again, after
 * what the reply says, while it is the only one in flight and none of its reply has been delivered: a request,
 * and a streamed reply that has not begun. Past the time budget (tsdbc_set_retry) the status is returned and
 * c->err says why. Several requests in flight (pipelined inserts) are drain's: it resends them in order. */
static int read_reply(tsdbc *c, int32_t *status, const uint8_t **body, size_t *blen)
{
    int64_t deadline = 0;
    unsigned tries = 0;
    c->gave_up = false;
    for (;;) {
        uint32_t id = c->expect;
        int rc = read_reply_raw(c, status, body, blen);
        if (rc || !retryable_status(*status) || c->retry_ms <= 0)
            return rc;
        struct sent *e = q_find(c, id);
        bool alone = e != NULL;
        for (size_t i = 0; alone && i < c->qlen; i++)
            if (c->q[c->qhead + i].id != id && (int32_t)(c->q[c->qhead + i].id - c->expect) >= 0)
                alone = false;
        if (!alone || (c->stream_valid && c->stream_id == id))
            return 0;
        tsdbc_err_info err = c->err;
        if (!retry_step(c, &err, &deadline, &tries)) {
            c->gave_up = true;
            return 0;
        }
        e = q_find(c, id); /* the queue is not touched by a reconnect, but be sure of the pointer */
        c->expect = id;
        if ((rc = send_kept(c, e)) != 0)
            return rc;
    }
}

/* Reads the pipelined acknowledgements, keeping the first error for tsdbc_wait. */
static int drain(tsdbc *c)
{
    int64_t deadline = 0;
    unsigned tries = 0;
    for (;;) {
        bool failed = false;
        uint32_t first_id = 0;
        tsdbc_err_info first_err;
        while (c->pending) {
            int32_t st;
            const uint8_t *b;
            size_t n;
            uint32_t id = c->expect;
            /* the raw read: read_reply's retry in place of the last refused frame would send it before the earlier
             * refused ones (it cannot see them: their ids are below expect) */
            int rc = read_reply_raw(c, &st, &b, &n);
            if (rc)
                return rc;
            c->pending--;
            if (retryable_status(st) && c->retry_ms > 0 && q_find(c, id)) {
                if (!failed) {
                    failed = true;
                    first_id = id;
                    first_err = c->err;
                }
                continue; /* the rest of the replies are read first: the frames after it go again, in order */
            }
            if (st && !c->pending_rc)
                c->pending_rc = st;
        }
        if (!failed)
            break;
        if (!retry_step(c, &first_err, &deadline, &tries)) {
            if (!c->pending_rc)
                c->pending_rc = first_err.status;
            break;
        }
        /* every frame from the first refused one, in order, also those that were acknowledged after it: a later
         * frame of the same series must not be overtaken by the earlier one's resend */
        size_t i = 0;
        while (i < c->qlen && c->q[c->qhead + i].id != first_id)
            i++;
        c->expect = first_id;
        for (; i < c->qlen; i++) {
            int rc = send_kept(c, &c->q[c->qhead + i]);
            if (rc)
                return rc;
            c->pending++;
        }
    }
    q_trim(c);
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

/* The two halves of a SERIES call, so that a caller can send several requests and then read their replies in order
 * (tsdbc_latest_by_name). Sending does not wait for earlier acknowledgements: the caller has done tsdbc_wait. */
static int series_send(tsdbc *c, const char *name, int create, const char *path)
{
    size_t len = strlen(name), plen = path ? strlen(path) : 0;
    if (len == 0 || len > 65535 || plen > 65535)
        return TSDB_ERR_INVALID;
    uint8_t h[3] = {(uint8_t)(create != 0)};
    uint16_t l16 = (uint16_t)len, p16 = (uint16_t)plen;
    memcpy(h + 1, &l16, 2);
    return path ? send_req(c, TSDB_OP_SERIES, h, 3, name, len, &p16, 2, path, plen)
                : send_req(c, TSDB_OP_SERIES, h, 3, name, len, NULL, 0, NULL, 0);
}

/* Reads the reply of one SERIES request: its status, or a transport error (the connection is then broken). */
static int series_recv(tsdbc *c, tsdb_sid *id)
{
    int32_t st;
    const uint8_t *b;
    size_t n;
    int rc = read_reply(c, &st, &b, &n);
    if (rc)
        return rc;
    if (st)
        return st;
    if (n != 8)
        return TSDB_ERR_CORRUPT;
    memcpy(id, b, 8);
    return 0;
}

static int series_req(tsdbc *c, const char *name, int create, const char *path, tsdb_sid *id)
{
    size_t len = strlen(name), plen = path ? strlen(path) : 0;
    if (len == 0 || len > 65535 || plen > 65535)
        return TSDB_ERR_INVALID;
    int rc = tsdbc_wait(c);
    if (rc)
        return rc;
    rc = series_send(c, name, create, path);
    return rc ? rc : series_recv(c, id);
}

int tsdbc_series(tsdbc *c, const char *name, int create, tsdb_sid *id) { return series_req(c, name, create, NULL, id); }

int tsdbc_series_at(tsdbc *c, const char *name, const char *path, tsdb_sid *id)
{
    return series_req(c, name, 1, path, id);
}

int tsdbc_series_name(tsdbc *c, tsdb_sid id, char *buf, size_t len)
{
    const uint8_t *b;
    size_t n;
    int rc = call(c, TSDB_OP_SERIES_NAME, &id, 8, NULL, 0, &b, &n);
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

static int insert_values_frames(tsdbc *c, const tsdb_sid *series, tsdb_sid s, const int64_t *ts,
                                const tsdb_values *v, size_t n, bool wait);
static bool packing(tsdbc *c);
static uint32_t server_version(tsdbc *c);

static int insert_frames(tsdbc *c, const tsdb_sid *series, tsdb_sid s, const int64_t *ts, const float *v, size_t n,
                         bool wait)
{
    if (n && packing(c)) { /* float32 too: the packed form is the smaller one */
        tsdb_values col = {TSDB_FLOAT32, v, NULL};
        return insert_values_frames(c, series, s, ts, &col, n, wait);
    }
    int rc = 0;
    for (size_t off = 0; off < n || (n == 0 && off == 0); off += INSERT_FRAME_ROWS) {
        uint32_t m = (uint32_t)(n - off < INSERT_FRAME_ROWS ? n - off : INSERT_FRAME_ROWS);
        uint8_t h[16] = {0};
        tsdb_sid sid = series ? TSDB_MANY_SERIES : s;
        memcpy(h, &m, 4);
        memcpy(h + 8, &sid, 8);
        static const uint8_t pad[4];
        struct iovec parts[6] = {{0},
                                 {h, sizeof h},
                                 {(void *)(ts + off), (size_t)m * 8},
                                 {(void *)(v + off), (size_t)m * 4},
                                 {(void *)pad, series ? (size_t)(m & 1) * 4 : 0}, /* the ids at a multiple of 8 */
                                 {(void *)(series ? series + off : NULL), series ? (size_t)m * 8 : 0}};
        rc = send_reqv(c, TSDB_OP_INSERT, parts, 6);
        if (rc)
            return rc;
        c->pending++;
        if (n == 0)
            break;
    }
    return wait ? tsdbc_wait(c) : 0;
}

int tsdbc_insert(tsdbc *c, const tsdb_sid *series, const int64_t *ts, const float *values, size_t n)
{
    return insert_frames(c, series, 0, ts, values, n, true);
}

int tsdbc_insert_series(tsdbc *c, tsdb_sid series, const int64_t *ts, const float *values, size_t n)
{
    return insert_frames(c, NULL, series, ts, values, n, true);
}

int tsdbc_insert_async(tsdbc *c, const tsdb_sid *series, const int64_t *ts, const float *values, size_t n)
{
    return insert_frames(c, series, 0, ts, values, n, false);
}

int tsdbc_insert_series_async(tsdbc *c, tsdb_sid series, const int64_t *ts, const float *values, size_t n)
{
    return insert_frames(c, NULL, series, ts, values, n, false);
}

int tsdbc_query_raw(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, tsdb_raw_fn fn, void *ctx)
{
    return tsdbc_query_raw_ex(c, series, start, end, NULL, fn, ctx);
}

int tsdbc_query_raw_ex(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, const tsdb_query_opts *opts,
                       tsdb_raw_fn fn, void *ctx)
{
    int rc = tsdbc_wait(c);
    if (rc)
        return rc;
    uint8_t q[33];
    memcpy(q, &series, 8);
    memcpy(q + 8, &start, 8);
    memcpy(q + 16, &end, 8);
    q[24] = opts && opts->order == TSDB_DESC;
    uint64_t lim = opts ? opts->limit : 0;
    memcpy(q + 25, &lim, 8);
    if ((rc = send_req(c, TSDB_OP_QUERY_RAW, q, opts ? 33 : 24, NULL, 0, NULL, 0, NULL, 0)) != 0)
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

int tsdbc_query_points(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, tsdb_points *out)
{
    return tsdbc_query_points_ex(c, series, start, end, NULL, out);
}

int tsdbc_query_points_ex(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, const tsdb_query_opts *opts,
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
static int buckets_req(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, int64_t interval,
                       const tsdb_group *group, const tsdb_query_opts *opts, uint8_t fields, void **out, size_t *nout)
{
    *out = NULL;
    *nout = 0;
    const char *zone = group && group->timezone ? group->timezone : "";
    size_t zl = strlen(zone);
    if (zl > 65535)
        return TSDB_ERR_INVALID;
    uint8_t q[48];
    memcpy(q, &series, 8);
    memcpy(q + 8, &start, 8);
    memcpy(q + 16, &end, 8);
    memcpy(q + 24, &interval, 8);
    q[32] = group ? (uint8_t)group->unit : 0;
    uint32_t count = group ? group->count : 0;
    memcpy(q + 33, &count, 4);
    q[37] = opts && opts->order == TSDB_DESC;
    uint64_t lim = opts ? opts->limit : 0;
    memcpy(q + 38, &lim, 8);
    uint16_t z16 = (uint16_t)zl;
    memcpy(q + 46, &z16, 2);
    uint8_t *tail = malloc(zl + 1);
    if (!tail)
        return TSDB_ERR_NOMEM;
    memcpy(tail, zone, zl);
    tail[zl] = fields;
    const uint8_t *b;
    size_t len;
    int rc = call(c, TSDB_OP_QUERY_BUCKETS, q, sizeof q, tail, zl + (fields != 0), &b, &len);
    free(tail);
    return rc ? rc : read_buckets(b, len, fields, out, nout);
}

/* QUERY_BOUNDS, the fields byte after the bounds as in buckets_req */
static int bounds_req(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, const int64_t *bounds, size_t n,
                      const tsdb_query_opts *opts, uint8_t fields, void **out, size_t *nout)
{
    *out = NULL;
    *nout = 0;
    if (!bounds || n == 0 || n > (TSDB_MAX_FRAME - 64) / 8)
        return TSDB_ERR_INVALID;
    uint8_t q[37];
    uint32_t n32 = (uint32_t)n;
    uint64_t lim = opts ? opts->limit : 0;
    memcpy(q, &series, 8);
    memcpy(q + 8, &start, 8);
    memcpy(q + 16, &end, 8);
    q[24] = opts && opts->order == TSDB_DESC;
    memcpy(q + 25, &lim, 8);
    memcpy(q + 33, &n32, 4);
    uint8_t *tail = malloc(n * 8 + 1);
    if (!tail)
        return TSDB_ERR_NOMEM;
    memcpy(tail, bounds, n * 8);
    tail[n * 8] = fields;
    const uint8_t *b;
    size_t len;
    int rc = call(c, TSDB_OP_QUERY_BOUNDS, q, sizeof q, tail, n * 8 + (fields != 0), &b, &len);
    free(tail);
    return rc ? rc : read_buckets(b, len, fields, out, nout);
}

int tsdbc_query_agg(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, int64_t interval_ms, tsdb_bucket **out,
                    size_t *nout)
{
    return tsdbc_query_agg_ex(c, series, start, end, interval_ms, NULL, out, nout);
}

/* QUERY_BUCKETS with unit 0 (QUERY_AGG, whose 32-byte rows had neither order, limit nor the int64 fields, was
 * removed in protocol 11) */
int tsdbc_query_agg_ex(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, int64_t interval_ms,
                       const tsdb_query_opts *opts, tsdb_bucket **out, size_t *nout)
{
    if (interval_ms < 0) {
        *out = NULL;
        *nout = 0;
        return TSDB_ERR_INVALID;
    }
    return buckets_req(c, series, start, end, interval_ms, NULL, opts, 0, (void **)out, nout);
}

int tsdbc_query_bounds(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, const int64_t *bounds, size_t n,
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

static int group_req(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, const tsdb_group *group,
                     const tsdb_query_opts *opts, uint8_t fields, void **out, size_t *nout)
{
    bool local = group && group->timezone && strcmp(group->timezone, "UTC") != 0;
    if (!local)
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
    rc = tsdb_calendar_bounds(group->timezone, group->unit, group->count, lo, hi == INT64_MAX ? hi : hi + 1,
                              &bounds, &n);
    /* more local periods than one request carries: the status of more than the calendar computes, in every client */
    if (rc == 0 && n > (TSDB_MAX_FRAME - 64) / 8)
        rc = TSDB_ERR_TOO_MANY_BUCKETS;
    if (rc == 0)
        rc = bounds_req(c, series, start, end, bounds, n, opts, fields, out, nout);
    free(bounds);
    return rc;
}

int tsdbc_query_group(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, const tsdb_group *group,
                      const tsdb_query_opts *opts, tsdb_bucket **out, size_t *nout)
{
    return group_req(c, series, start, end, group, opts, 0, (void **)out, nout);
}

/* --------------------------------------------------------------- spread */

int tsdbc_query_spread(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, int64_t interval_ms,
                       const tsdb_query_opts *opts, tsdb_spread_bucket **out, size_t *nout)
{
    if (interval_ms < 0) {
        *out = NULL;
        *nout = 0;
        return TSDB_ERR_INVALID;
    }
    return buckets_req(c, series, start, end, interval_ms, NULL, opts, TSDB_SPREAD, (void **)out, nout);
}

int tsdbc_query_spread_group(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, const tsdb_group *group,
                             const tsdb_query_opts *opts, tsdb_spread_bucket **out, size_t *nout)
{
    return group_req(c, series, start, end, group, opts, TSDB_SPREAD, (void **)out, nout);
}

int tsdbc_query_spread_bounds(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, const int64_t *bounds,
                              size_t n, const tsdb_query_opts *opts, tsdb_spread_bucket **out, size_t *nout)
{
    return bounds_req(c, series, start, end, bounds, n, opts, TSDB_SPREAD, (void **)out, nout);
}

/* ------------------------------------------------- time-weighted averages */

static int twa_req(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, int64_t interval, uint8_t unit,
                   uint32_t count, const int64_t *bounds, size_t n, const tsdb_twa_opts *opts, tsdb_twa_bucket **out,
                   size_t *nout)
{
    *out = NULL;
    *nout = 0;
    if (n > (TSDB_MAX_FRAME - 64 - TSDB_TWA_HEADER) / 8)
        return TSDB_ERR_INVALID;
    tsdb_twa_opts def = {0};
    if (!opts)
        opts = &def;
    uint8_t q[TSDB_TWA_HEADER] = {0};
    uint64_t lim = opts->limit;
    uint32_t n32 = (uint32_t)n;
    memcpy(q, &series, 8);
    memcpy(q + 8, &start, 8);
    memcpy(q + 16, &end, 8);
    q[24] = (uint8_t)opts->method;
    q[25] = opts->order == TSDB_DESC;
    q[26] = unit;
    memcpy(q + 28, &opts->max_gap_ms, 8);
    memcpy(q + 36, &lim, 8);
    memcpy(q + 44, &interval, 8);
    memcpy(q + 52, &count, 4);
    memcpy(q + 56, &n32, 4);
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

int tsdbc_query_twa(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, int64_t interval_ms,
                    const tsdb_twa_opts *opts, tsdb_twa_bucket **out, size_t *nout)
{
    if (interval_ms < 0) {
        *out = NULL;
        *nout = 0;
        return TSDB_ERR_INVALID;
    }
    return twa_req(c, series, start, end, interval_ms, 0, 0, NULL, 0, opts, out, nout);
}

int tsdbc_query_twa_bounds(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, const int64_t *bounds, size_t n,
                           const tsdb_twa_opts *opts, tsdb_twa_bucket **out, size_t *nout)
{
    if (!bounds || n == 0) {
        *out = NULL;
        *nout = 0;
        return TSDB_ERR_INVALID;
    }
    return twa_req(c, series, start, end, 0, 0, 0, bounds, n, opts, out, nout);
}

int tsdbc_query_twa_group(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, const tsdb_group *group,
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
    rc = tsdb_calendar_bounds(group->timezone, group->unit, group->count, lo, hi == INT64_MAX ? hi : hi + 1,
                              &bounds, &n);
    if (rc == 0 && n > (TSDB_MAX_FRAME - 64 - TSDB_TWA_HEADER) / 8) /* as in group_req */
        rc = TSDB_ERR_TOO_MANY_BUCKETS;
    if (rc == 0)
        rc = tsdbc_query_twa_bounds(c, series, start, end, bounds, n, opts, out, nout);
    free(bounds);
    return rc;
}

/* ------------------------------------------------------------ value types */

int tsdbc_series_create(tsdbc *c, const char *name, tsdb_type type, const char *path, tsdb_sid *id)
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
    if (n != 8)
        return TSDB_ERR_CORRUPT;
    memcpy(id, b, 8);
    return 0;
}

int tsdbc_series_type(tsdbc *c, tsdb_sid id, tsdb_type *type)
{
    const uint8_t *b;
    size_t n;
    int rc = call(c, TSDB_OP_SERIES_TYPE, &id, 8, NULL, 0, &b, &n);
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
    c->timeout_ms = ms;
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
        if (st == 0 && n >= 4)
            memcpy(&theirs, b, 4);
        if (st == 0 && theirs >= 12) { /* u16 flags, u16 n, n x (u8 len, mechanism) */
            uint16_t nm;
            size_t at = 8;
            if (n < 8) {
                c->broken = true; /* a server of 12 that does not say whether it wants a login: answer nothing */
                return 1;
            }
            memcpy(&c->hello_flags, b + 4, 2);
            memcpy(&nm, b + 6, 2);
            for (uint16_t i = 0; i < nm && at < n && at + 1 + b[at] <= n; i++, at += 1 + b[at])
                c->scram |= b[at] == strlen(TSDB_SCRAM_SHA256) &&
                            memcmp(b + at + 1, TSDB_SCRAM_SHA256, strlen(TSDB_SCRAM_SHA256)) == 0;
        }
        c->server_version = theirs;
    }
    return c->server_version;
}

/* At connect: a server below TSDB_PROTOCOL_MIN reads every series id as a u32, so nothing could be sent to it
 * safely (protocol 11 widened them). */
static int check_server_version(tsdbc *c)
{
    uint32_t v = server_version(c);
    if (c->broken)
        return TSDB_ERR_IO;
    if (v < TSDB_PROTOCOL_MIN) {
        fprintf(stderr, "tsdbc: tsdbd speaks protocol %u; this client needs %u: upgrade the server\n", v,
                TSDB_PROTOCOL_MIN);
        return TSDB_ERR_INVALID;
    }
    return 0;
}

uint32_t tsdbc_server_version(tsdbc *c) { return tsdbc_wait(c) ? 0 : server_version(c); }

uint64_t tsdbc_bytes_sent(const tsdbc *c) { return c->sent; }

/* ------------------------------------------------------------- login */

#define SCRAM_ITER_MIN 4096         /* fewer is a downgrade a server could ask for: refused */
#define SCRAM_ITER_MAX 100000000    /* more would let a fake server pin the client's CPU */
#define KEY_CACHE 16

_Static_assert(TSDBC_PASSWORD_MAX == TSDB_PASSWORD_MAX, "one password rule");

#ifdef TSDBC_TEST_HOOKS
/* Only in the tests' build of this file (tests/client_hooks.h), never in libtsdbclient.a: the client nonce comes
 * from here when set (the RFC 7677 test replays a fixed exchange), and PBKDF2 runs are counted (the key cache). */
const char *tsdbc_test_nonce;
unsigned tsdbc_test_pbkdf2_runs;
#endif

/* PBKDF2 once per process: ClientKey and ServerKey by user, salt, iterations and password. The password is not kept:
 * an entry holds HMAC(salt, password) to tell one password from another. One computation per key at a time: the
 * connections of a pool start together, and every one that missed would otherwise run PBKDF2 (n times the CPU at
 * a high iteration count) and insert its own copy, evicting other users' keys from the ring. The first to miss
 * claims an entry as pending and computes outside the lock; the others wait for it. A pending entry is never
 * evicted, so a waiter finds it until it is done. */
typedef struct {
    bool used, pending;
    char user[256];
    uint8_t salt[64], pw_mac[32], client_key[32], server_key[32];
    size_t salt_len;
    uint32_t iterations;
} key_entry;
static pthread_mutex_t key_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t key_done = PTHREAD_COND_INITIALIZER;
static key_entry keys[KEY_CACHE];
static unsigned key_next;

static key_entry *key_find(const char *user, const uint8_t *salt, size_t salt_len, uint32_t iter, const uint8_t *mac)
{
    for (unsigned i = 0; i < KEY_CACHE; i++) {
        key_entry *e = &keys[i];
        if (e->used && e->iterations == iter && e->salt_len == salt_len && strcmp(e->user, user) == 0 &&
            memcmp(e->salt, salt, salt_len) == 0 && tsdb_ct_equal(e->pw_mac, mac, 32))
            return e;
    }
    return NULL;
}

static void scram_keys_cached(const char *user, const char *pw, size_t pwl, const uint8_t *salt, size_t salt_len,
                              uint32_t iter, uint8_t ck[32], uint8_t sk[32])
{
    uint8_t mac[32];
    tsdb_hmac_sha256(salt, salt_len, pw, pwl, mac);
    key_entry *mine = NULL;
    if (strlen(user) < sizeof keys[0].user && salt_len <= sizeof keys[0].salt) {
        pthread_mutex_lock(&key_mu);
        key_entry *e;
        while ((e = key_find(user, salt, salt_len, iter, mac)) && e->pending)
            pthread_cond_wait(&key_done, &key_mu); /* another login computes this key */
        if (e) {
            memcpy(ck, e->client_key, 32);
            memcpy(sk, e->server_key, 32);
            pthread_mutex_unlock(&key_mu);
            tsdb_wipe(mac, sizeof mac);
            return;
        }
        for (unsigned k = 0; k < KEY_CACHE && !mine; k++) { /* the next slot of the ring that nobody waits for */
            e = &keys[key_next++ % KEY_CACHE];
            if (!e->pending)
                mine = e;
        }
        if (mine) { /* else KEY_CACHE other keys are in flight: this one is computed without being cached */
            *mine = (key_entry){.used = true, .pending = true, .salt_len = salt_len, .iterations = iter};
            snprintf(mine->user, sizeof mine->user, "%s", user);
            memcpy(mine->salt, salt, salt_len);
            memcpy(mine->pw_mac, mac, 32);
        }
        pthread_mutex_unlock(&key_mu);
    }
    tsdb_scram_keys(pw, pwl, salt, salt_len, iter, ck, NULL, sk); /* outside the lock: milliseconds or more */
#ifdef TSDBC_TEST_HOOKS
    __atomic_add_fetch(&tsdbc_test_pbkdf2_runs, 1, __ATOMIC_RELAXED);
#endif
    if (mine) {
        pthread_mutex_lock(&key_mu);
        memcpy(mine->client_key, ck, 32);
        memcpy(mine->server_key, sk, 32);
        mine->pending = false;
        pthread_cond_broadcast(&key_done);
        pthread_mutex_unlock(&key_mu);
    }
    tsdb_wipe(mac, sizeof mac);
}

/* The password in the first line of a file: TSDB_ERR_IO (errno set) when it cannot be read, TSDB_ERR_INVALID when
 * the line breaks the password rule. */
static int read_password_file(const char *path, char pw[TSDBC_PASSWORD_MAX + 1])
{
    FILE *f = fopen(path, "re");
    if (!f)
        return TSDB_ERR_IO;
    int rc = tsdb_read_password(f, pw);
    int err = errno;
    fclose(f);
    errno = err;
    return rc == -1 ? TSDB_ERR_IO : rc ? TSDB_ERR_INVALID : 0;
}

/* A u16-length message from a reply body; NULL when the body is not exactly that. */
static const char *reply_msg(const uint8_t *b, size_t n, size_t *len)
{
    uint16_t l;
    if (n < 2 || (memcpy(&l, b, 2), n != 2u + l))
        return NULL;
    *len = l;
    return (const char *)b + 2;
}

/* SCRAM-SHA-256 (RFC 5802, RFC 7677) as the client: the user in n=, no channel binding ("n,,"). The server's
 * signature is checked before the connect returns: a server without this user's verifier cannot pass. */
static int scram(tsdbc *c, const char *user, const char *pw)
{
    size_t ul = strlen(user), pwl = strlen(pw);
    if (!ul || ul > 255 || strpbrk(user, ",="))
        return TSDB_ERR_INVALID; /* user names never hold these (USER_NAME_MAX is far below 255) */
    char cn[64];
#ifdef TSDBC_TEST_HOOKS
    if (tsdbc_test_nonce) {
        snprintf(cn, sizeof cn, "%s", tsdbc_test_nonce);
    } else
#endif
    {
        uint8_t r[18];
        if (tsdb_random(r, sizeof r) != 0)
            return TSDB_ERR_IO;
        tsdb_base64_encode(r, sizeof r, cn);
    }
    size_t cnl = strlen(cn);
    /* AUTH_START: u8 len, mechanism, u16 len, "n,," client-first-message-bare */
    char first[5 + 255 + 3 + sizeof cn];
    int fl = snprintf(first, sizeof first, "n,,n=%s,r=%s", user, cn);
    uint8_t start[1 + 13 + 2 + sizeof first];
    size_t ml = strlen(TSDB_SCRAM_SHA256);
    start[0] = (uint8_t)ml;
    memcpy(start + 1, TSDB_SCRAM_SHA256, ml);
    uint16_t fl16 = (uint16_t)fl;
    memcpy(start + 1 + ml, &fl16, 2);
    memcpy(start + 3 + ml, first, (size_t)fl);
    const uint8_t *b;
    size_t n, sl;
    int rc = call(c, TSDB_OP_AUTH_START, start, 3 + ml + (size_t)fl, NULL, 0, &b, &n);
    if (rc)
        return rc == TSDB_ERR_AUTH || rc == TSDB_ERR_INVALID ? TSDB_ERR_AUTH : rc;
    const char *sf = reply_msg(b, n, &sl);
    /* server-first-message: r=<client nonce><server nonce>,s=<salt>,i=<iterations>, nothing else */
    char server_first[600], nonce[300];
    uint8_t salt[64];
    size_t salt_len;
    if (!sf || sl >= sizeof server_first)
        return TSDB_ERR_AUTH;
    memcpy(server_first, sf, sl);
    server_first[sl] = '\0';
    const char *s = strstr(server_first, ",s="), *i = s ? strstr(s + 3, ",i=") : NULL;
    if (strncmp(server_first, "r=", 2) != 0 || !s || !i || (size_t)(s - server_first - 2) <= cnl ||
        (size_t)(s - server_first - 2) >= sizeof nonce || memcmp(server_first + 2, cn, cnl) != 0 ||
        tsdb_base64_decode(s + 3, (size_t)(i - s - 3), salt, sizeof salt, &salt_len) != 0 || salt_len == 0)
        return TSDB_ERR_AUTH;
    size_t nl = (size_t)(s - server_first - 2);
    memcpy(nonce, server_first + 2, nl);
    nonce[nl] = '\0';
    char *end;
    errno = 0;
    unsigned long it = strtoul(i + 3, &end, 10);
    if (*end || errno || i[3] < '1' || i[3] > '9' || it < SCRAM_ITER_MIN || it > SCRAM_ITER_MAX)
        return TSDB_ERR_AUTH;
    for (size_t k = 0; k < nl; k++)
        if (nonce[k] < 0x21 || nonce[k] > 0x7e || nonce[k] == ',')
            return TSDB_ERR_AUTH;
    uint8_t ck[32], sk[32], stored[32], sig[32], proof[32], ssig[32];
    scram_keys_cached(user, pw, pwl, salt, salt_len, (uint32_t)it, ck, sk);
    /* AuthMessage = client-first-message-bare "," server-first-message "," client-final-message-without-proof */
    char final[400 + tsdb_base64_len(32)];
    int wl = snprintf(final, sizeof final, "c=biws,r=%s", nonce);
    size_t am_len = (size_t)fl - 3 + 1 + sl + 1 + (size_t)wl;
    char *am = malloc(am_len);
    if (!am) {
        tsdb_wipe(ck, sizeof ck);
        return TSDB_ERR_NOMEM;
    }
    memcpy(am, first + 3, (size_t)fl - 3);
    am[fl - 3] = ',';
    memcpy(am + fl - 2, server_first, sl);
    am[fl - 2 + sl] = ',';
    memcpy(am + fl - 1 + sl, final, (size_t)wl);
    tsdb_sha256_buf(ck, 32, stored);
    tsdb_hmac_sha256(stored, 32, am, am_len, sig);
    for (int k = 0; k < 32; k++)
        proof[k] = ck[k] ^ sig[k];
    tsdb_hmac_sha256(sk, 32, am, am_len, ssig);
    free(am);
    tsdb_wipe(ck, sizeof ck);
    tsdb_wipe(sk, sizeof sk);
    memcpy(final + wl, ",p=", 3);
    wl += 3;
    wl += (int)tsdb_base64_encode(proof, 32, final + wl);
    tsdb_wipe(proof, sizeof proof);
    uint8_t cont[2 + sizeof final];
    uint16_t wl16 = (uint16_t)wl;
    memcpy(cont, &wl16, 2);
    memcpy(cont + 2, final, (size_t)wl);
    rc = call(c, TSDB_OP_AUTH_CONTINUE, cont, 2 + (size_t)wl, NULL, 0, &b, &n);
    if (rc)
        return rc == TSDB_ERR_AUTH || rc == TSDB_ERR_INVALID ? TSDB_ERR_AUTH : rc;
    /* server-final-message: v=<ServerSignature>; an e= (or anything else) fails */
    const char *v = reply_msg(b, n, &sl);
    uint8_t got[33];
    size_t gl;
    if (!v || sl < 2 || memcmp(v, "v=", 2) != 0 || tsdb_base64_decode(v + 2, sl - 2, got, sizeof got, &gl) != 0 ||
        gl != 32 || !tsdb_ct_equal(got, ssig, 32))
        return TSDB_ERR_AUTH;
    return 0;
}

/* After HELLO: logs in when the server asks for it, refuses a server that does not when the caller requires it. */
static int login(tsdbc *c, const tsdbc_options *o)
{
    bool wants = c->hello_flags & TSDB_HELLO_AUTH;
    if (!wants) {
        if (!o->require_auth)
            return 0; /* --no-auth: credentials, if any, are not needed */
        fprintf(stderr, "tsdbc: tsdbd does not ask for a login (--no-auth) and this client requires one\n");
        return TSDB_ERR_AUTH;
    }
    if (!o->user) {
        fprintf(stderr, "tsdbc: tsdbd requires a login: give a user and a password\n");
        return TSDB_ERR_AUTH;
    }
    if (!c->scram)
        return TSDB_ERR_AUTH; /* no mechanism this client speaks */
    char buf[TSDBC_PASSWORD_MAX + 1];
    const char *pw = o->password;
    if (!pw) {
        int rc = read_password_file(o->password_file, buf);
        if (rc) {
            fprintf(stderr, "tsdbc: password file %s: %s\n", o->password_file,
                    rc == TSDB_ERR_IO ? strerror(errno) : "no password in its first line (1 to 1023 bytes)");
            return TSDB_ERR_INVALID;
        }
        pw = buf;
    }
    int rc = scram(c, o->user, pw);
    tsdb_wipe(buf, sizeof buf);
    return rc;
}

/* ------------------------------------------------------- users, grants */

static void put_name16(uint8_t *p, const char *s, size_t n)
{
    uint16_t l = (uint16_t)n;
    memcpy(p, &l, 2);
    memcpy(p + 2, s, n);
}

/* u16 len, name and what follows it, sent as one request with no reply body. */
static int user_call(tsdbc *c, uint8_t op, const char *name, const void *rest, size_t rest_len)
{
    size_t nl = strlen(name);
    if (!nl || nl > 0xffff)
        return TSDB_ERR_INVALID;
    uint8_t *p = malloc(2 + nl + rest_len);
    if (!p)
        return TSDB_ERR_NOMEM;
    put_name16(p, name, nl);
    if (rest_len)
        memcpy(p + 2 + nl, rest, rest_len);
    int rc = call(c, op, p, 2 + nl + rest_len, NULL, 0, NULL, NULL);
    tsdb_wipe(p, 2 + nl + rest_len);
    free(p);
    return rc;
}

/* The verifier of a password with a fresh salt: u32 iterations, u8 salt_len, salt, StoredKey, ServerKey (A18). */
static int verifier(const char *password, uint32_t iterations, uint8_t *out, size_t *len)
{
    if (!password_ok(password))
        return TSDB_ERR_INVALID;
    uint32_t it = iterations ? iterations : SCRAM_ITER_MIN;
    if (it < SCRAM_ITER_MIN || it > SCRAM_ITER_MAX)
        return TSDB_ERR_INVALID;
    uint8_t salt[16];
    if (tsdb_random(salt, sizeof salt) != 0)
        return TSDB_ERR_IO;
    memcpy(out, &it, 4);
    out[4] = sizeof salt;
    memcpy(out + 5, salt, sizeof salt);
    tsdb_scram_keys(password, strlen(password), salt, sizeof salt, it, NULL, out + 5 + sizeof salt,
                    out + 5 + sizeof salt + 32);
    *len = 5 + sizeof salt + 64;
    return 0;
}

int tsdbc_user_create(tsdbc *c, const char *name, const char *password, uint32_t iterations, uint8_t flags)
{
    uint8_t v[1 + 5 + 16 + 64];
    size_t vl;
    int rc = verifier(password, iterations, v + 1, &vl);
    if (rc)
        return rc;
    v[0] = flags;
    rc = user_call(c, TSDB_OP_USER_CREATE, name, v, 1 + vl);
    tsdb_wipe(v, sizeof v);
    return rc;
}

int tsdbc_user_password(tsdbc *c, const char *name, const char *password, uint32_t iterations)
{
    uint8_t v[5 + 16 + 64];
    size_t vl;
    int rc = verifier(password, iterations, v, &vl);
    if (rc == 0)
        rc = user_call(c, TSDB_OP_USER_PASSWORD, name, v, vl);
    tsdb_wipe(v, sizeof v);
    return rc;
}

int tsdbc_user_drop(tsdbc *c, const char *name) { return user_call(c, TSDB_OP_USER_DROP, name, NULL, 0); }

int tsdbc_user_alter(tsdbc *c, const char *name, uint8_t flags)
{
    return user_call(c, TSDB_OP_USER_ALTER, name, &flags, 1);
}

static int grant_call(tsdbc *c, uint8_t op, const char *name, uint8_t kind, const char *selector, uint8_t rights)
{
    size_t sl = selector ? strlen(selector) : 0;
    if (sl > 0xffff)
        return TSDB_ERR_INVALID;
    uint8_t *g = malloc(4 + sl);
    if (!g)
        return TSDB_ERR_NOMEM;
    g[0] = kind, g[1] = rights;
    put_name16(g + 2, selector ? selector : "", sl);
    int rc = user_call(c, op, name, g, 4 + sl);
    free(g);
    return rc;
}

int tsdbc_grant(tsdbc *c, const char *name, uint8_t kind, const char *selector, uint8_t rights)
{
    return grant_call(c, TSDB_OP_GRANT, name, kind, selector, rights);
}

int tsdbc_revoke(tsdbc *c, const char *name, uint8_t kind, const char *selector, uint8_t rights)
{
    return grant_call(c, TSDB_OP_REVOKE, name, kind, selector, rights);
}

void tsdbc_users_free(tsdbc_user *u, size_t n)
{
    for (size_t i = 0; u && i < n; i++) {
        for (uint32_t j = 0; j < u[i].ngrants; j++)
            free(u[i].grants[j].selector);
        free(u[i].grants);
        free(u[i].name);
    }
    free(u);
}

static char *dup_bytes(const uint8_t *p, size_t n)
{
    char *s = malloc(n + 1);
    if (s) {
        memcpy(s, p, n);
        s[n] = '\0';
    }
    return s;
}

/* One user at p (u16 len, name, u8 flags, u32 g, g x (u8 kind, u8 rights, u16 len, selector)); the bytes after it,
 * NULL when malformed (CORRUPT) or out of memory (*nomem). */
static const uint8_t *read_user(const uint8_t *p, const uint8_t *end, tsdbc_user *u, bool *nomem)
{
    uint16_t l;
    uint32_t g;
    memset(u, 0, sizeof *u);
    if (end - p < 2 || (memcpy(&l, p, 2), end - p < 2 + l + 5))
        return NULL;
    if (!(u->name = dup_bytes(p + 2, l)))
        return *nomem = true, NULL;
    p += 2 + l;
    u->flags = p[0];
    memcpy(&g, p + 1, 4);
    p += 5;
    if ((size_t)(end - p) / 4 < g)
        return NULL;
    if (g && !(u->grants = calloc(g, sizeof *u->grants)))
        return *nomem = true, NULL;
    for (; u->ngrants < g; u->ngrants++) {
        tsdbc_user_grant *x = &u->grants[u->ngrants];
        if (end - p < 4 || (memcpy(&l, p + 2, 2), end - p - 4 < l))
            return NULL;
        x->kind = p[0], x->rights = p[1];
        if (!(x->selector = dup_bytes(p + 4, l)))
            return *nomem = true, NULL;
        p += 4 + l;
    }
    return p;
}

int tsdbc_whoami(tsdbc *c, tsdbc_user **out)
{
    *out = NULL;
    const uint8_t *b;
    size_t n;
    int rc = call(c, TSDB_OP_WHOAMI, NULL, 0, NULL, 0, &b, &n);
    if (rc)
        return rc;
    tsdbc_user *u = calloc(1, sizeof *u);
    bool nomem = false;
    if (!u)
        return TSDB_ERR_NOMEM;
    if (read_user(b, b + n, u, &nomem) != b + n) {
        tsdbc_users_free(u, 1);
        return nomem ? TSDB_ERR_NOMEM : TSDB_ERR_CORRUPT;
    }
    *out = u;
    return 0;
}

int tsdbc_user_list(tsdbc *c, tsdbc_user **out, size_t *nout)
{
    *out = NULL;
    *nout = 0;
    int rc = tsdbc_wait(c);
    if (rc || (rc = send_req(c, TSDB_OP_USER_LIST, NULL, 0, NULL, 0, NULL, 0, NULL, 0)) != 0)
        return rc;
    tsdbc_user *u = NULL;
    size_t nu = 0, cap = 0;
    for (;;) {
        int32_t st;
        const uint8_t *b;
        size_t len;
        if ((rc = read_reply(c, &st, &b, &len)) != 0)
            break;
        if (st) {
            rc = st;
            break;
        }
        uint32_t k;
        if (len < 5 || b[0] > 1) {
            c->broken = true;
            rc = TSDB_ERR_CORRUPT;
            break;
        }
        bool last = b[0];
        memcpy(&k, b + 1, 4);
        const uint8_t *p = b + 5, *end = b + len;
        if ((size_t)(end - p) / 7 < k) {
            c->broken = true;
            rc = TSDB_ERR_CORRUPT;
            break;
        }
        if (nu + k > cap) {
            size_t nc = cap ? cap * 2 : 16;
            while (nc < nu + k)
                nc *= 2;
            tsdbc_user *q = realloc(u, nc * sizeof *u);
            if (!q) {
                rc = TSDB_ERR_NOMEM;
                c->broken = !last; /* the rest of the stream stays unread */
                break;
            }
            u = q, cap = nc;
        }
        bool nomem = false;
        for (uint32_t i = 0; i < k && p; i++, nu++)
            p = read_user(p, end, &u[nu], &nomem);
        if (p != end) { /* nu counts the one read_user left half made: freed with the others */
            c->broken = nomem ? !last : true;
            rc = nomem ? TSDB_ERR_NOMEM : TSDB_ERR_CORRUPT;
            break;
        }
        if (last)
            break;
        stream_more(c);
    }
    if (rc) {
        tsdbc_users_free(u, nu);
        return rc;
    }
    *out = u;
    *nout = nu;
    return 0;
}

/* Whether inserts go packed: compression on. */
static bool packing(tsdbc *c) { return c->zlevel != 0; }

/* One packed frame: reshape, zstd, send (the buffers are free again on return). */
static int send_packed(tsdbc *c, const tsdb_sid *series, tsdb_sid s, const int64_t *ts, const tsdb_values *v,
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
    tsdb_sid sid = series ? TSDB_MANY_SERIES : s;
    uint32_t rl = (uint32_t)raw;
    memcpy(h, &m, 4);
    h[4] = (uint8_t)v->type;
    h[5] = (uint8_t)(TSDB_PACKED | (v->valid ? TSDB_VALID : 0));
    memcpy(h + 8, &sid, 8);
    memcpy(h + TSDB_VALUES_HEADER, &rl, 4);
    struct iovec parts[3] = {{0}, {h, sizeof h}, {c->zbuf, z}};
    return send_reqv(c, TSDB_OP_INSERT_VALUES, parts, 3);
}

static int insert_values_frames(tsdbc *c, const tsdb_sid *series, tsdb_sid s, const int64_t *ts,
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
    static const uint8_t pad[7];
    for (size_t off = 0; off < n || (n == 0 && off == 0); off += INSERT_FRAME_ROWS) {
        uint32_t m = (uint32_t)(n - off < INSERT_FRAME_ROWS ? n - off : INSERT_FRAME_ROWS);
        uint8_t h[TSDB_VALUES_HEADER] = {0};
        tsdb_sid sid = series ? TSDB_MANY_SERIES : s;
        memcpy(h, &m, 4);
        h[4] = (uint8_t)v->type;
        h[5] = v->valid ? TSDB_VALID : 0;
        memcpy(h + 8, &sid, 8);
        size_t vb = (size_t)m * aw, valid = v->valid ? ((size_t)m + 7) / 8 : 0;
        size_t end = TSDB_VALUES_HEADER + (size_t)m * 8 + vb + valid;
        struct iovec parts[7] = {{0},
                                 {h, sizeof h},
                                 {(void *)(ts + off), (size_t)m * 8},
                                 {(uint8_t *)v->values + off * aw, vb},
                                 {(void *)(v->valid ? v->valid + off / 8 : pad), valid}, /* off is a multiple of 8 */
                                 {(void *)pad, series ? ((end + 7) & ~(size_t)7) - end : 0},
                                 {(void *)(series ? series + off : NULL), series ? (size_t)m * 8 : 0}};
        if ((rc = send_reqv(c, TSDB_OP_INSERT_VALUES, parts, 7)) != 0)
            return rc;
        c->pending++;
        if (n == 0)
            break;
    }
    return wait ? tsdbc_wait(c) : 0;
}

int tsdbc_insert_values(tsdbc *c, const tsdb_sid *series, const int64_t *ts, const tsdb_values *values, size_t n)
{
    return insert_values_frames(c, series, 0, ts, values, n, true);
}

int tsdbc_insert_series_values(tsdbc *c, tsdb_sid series, const int64_t *ts, const tsdb_values *values, size_t n)
{
    return insert_values_frames(c, NULL, series, ts, values, n, true);
}

int tsdbc_insert_values_async(tsdbc *c, const tsdb_sid *series, const int64_t *ts, const tsdb_values *values,
                              size_t n)
{
    return insert_values_frames(c, series, 0, ts, values, n, false);
}

int tsdbc_insert_series_values_async(tsdbc *c, tsdb_sid series, const int64_t *ts, const tsdb_values *values,
                                     size_t n)
{
    return insert_values_frames(c, NULL, series, ts, values, n, false);
}

static int read_values(tsdbc *c, tsdb_values_fn fn, void *ctx);

int tsdbc_query_values(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, const tsdb_query_opts *opts,
                       tsdb_values_fn fn, void *ctx)
{
    int rc = tsdbc_wait(c);
    if (rc)
        return rc;
    uint8_t q[33];
    memcpy(q, &series, 8);
    memcpy(q + 8, &start, 8);
    memcpy(q + 16, &end, 8);
    q[24] = opts && opts->order == TSDB_DESC;
    uint64_t lim = opts ? opts->limit : 0;
    memcpy(q + 25, &lim, 8);
    if ((rc = send_req(c, TSDB_OP_QUERY_VALUES, q, 33, NULL, 0, NULL, 0, NULL, 0)) != 0)
        return rc;
    return read_values(c, fn, ctx);
}

int tsdbc_query_lttb(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, size_t threshold, tsdb_values_fn fn,
                     void *ctx)
{
    return tsdbc_query_lttb_ex(c, series, start, end, threshold, NULL, fn, ctx);
}

int tsdbc_query_lttb_ex(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, size_t threshold,
                        const tsdb_query_opts *opts, tsdb_values_fn fn, void *ctx)
{
    int rc = tsdbc_wait(c);
    if (rc)
        return rc;
    /* order and limit only when they change something: the 32-byte form is the plain one */
    bool ex = opts && (opts->order == TSDB_DESC || opts->limit);
    uint8_t q[41];
    uint64_t t = threshold, lim = opts ? opts->limit : 0;
    memcpy(q, &series, 8);
    memcpy(q + 8, &start, 8);
    memcpy(q + 16, &end, 8);
    memcpy(q + 24, &t, 8);
    q[32] = ex && opts->order == TSDB_DESC;
    memcpy(q + 33, &lim, 8);
    if ((rc = send_req(c, TSDB_OP_QUERY_LTTB, q, ex ? 41 : 32, NULL, 0, NULL, 0, NULL, 0)) != 0)
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
    int type = -1; /* of the first frame: the series has one, and a callback may size its buffer by it */
    for (;;) {
        int32_t st;
        const uint8_t *b;
        size_t len;
        if ((rc = read_reply(c, &st, &b, &len)) != 0)
            break;
        uint32_t n;
        if (len < 8 || b[1] > TSDB_STATE || (type >= 0 && b[1] != type) || (memcpy(&n, b + 4, 4), false)) {
            rc = TSDB_ERR_CORRUPT;
            c->broken = true;
            break;
        }
        type = b[1];
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

static int state_req(tsdbc *c, tsdb_sid series, const char *name, bool create, uint32_t *code)
{
    size_t len = strlen(name);
    if (len == 0 || len > 65535)
        return TSDB_ERR_INVALID;
    uint8_t h[11];
    uint16_t l16 = (uint16_t)len;
    memcpy(h, &series, 8);
    h[8] = create;
    memcpy(h + 9, &l16, 2);
    const uint8_t *b;
    size_t n;
    int rc = call(c, TSDB_OP_STATE, h, sizeof h, name, len, &b, &n);
    if (rc)
        return rc;
    if (n != 4)
        return TSDB_ERR_CORRUPT;
    memcpy(code, b, 4);
    return 0;
}

int tsdbc_state(tsdbc *c, tsdb_sid series, const char *name, uint32_t *code)
{
    return state_req(c, series, name, true, code);
}

int tsdbc_state_lookup(tsdbc *c, tsdb_sid series, const char *name, uint32_t *code)
{
    return state_req(c, series, name, false, code);
}

int tsdbc_state_name(tsdbc *c, tsdb_sid series, uint32_t code, char *buf, size_t len)
{
    uint8_t q[12];
    memcpy(q, &series, 8);
    memcpy(q + 8, &code, 4);
    const uint8_t *b;
    size_t n;
    int rc = call(c, TSDB_OP_STATE_NAME, q, sizeof q, NULL, 0, &b, &n);
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

int tsdbc_delete_range(tsdbc *c, tsdb_sid series, int64_t start, int64_t end)
{
    int rc = tsdbc_wait(c);
    if (rc)
        return rc;
    uint8_t b[24];
    memcpy(b, &series, 8);
    memcpy(b + 8, &start, 8);
    memcpy(b + 16, &end, 8);
    return call(c, TSDB_OP_DELETE_RANGE, b, sizeof b, NULL, 0, NULL, NULL);
}

/* One frame, unpacked: the server applies it as one tsdb_replace_range, all or nothing. */
int tsdbc_replace_range(tsdbc *c, tsdb_sid series, int64_t start, int64_t end, const int64_t *ts,
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
    h[4] = (uint8_t)v->type;
    h[5] = v->valid ? TSDB_VALID : 0;
    memcpy(h + 8, &series, 8);
    memcpy(h + 16, &start, 8);
    memcpy(h + 24, &end, 8);
    int rc = tsdbc_wait(c);
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

int tsdbc_set_retention(tsdbc *c, tsdb_sid series, int64_t limit_ms)
{
    int rc = tsdbc_wait(c);
    if (rc)
        return rc;
    uint8_t b[16];
    memcpy(b, &series, 8);
    memcpy(b + 8, &limit_ms, 8);
    return call(c, TSDB_OP_SET_RETENTION, b, sizeof b, NULL, 0, NULL, NULL);
}

int tsdbc_get_retention(tsdbc *c, tsdb_sid series, int64_t *limit_ms)
{
    int rc = tsdbc_wait(c);
    if (rc)
        return rc;
    const uint8_t *b;
    size_t n;
    rc = call(c, TSDB_OP_GET_RETENTION, &series, 8, NULL, 0, &b, &n);
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
static int multi_read(mrun *m, uint32_t id, uint8_t op, uint8_t fields, const tsdb_sid *series, size_t n, size_t base)
{
    tsdbc *c = m->c;
    bool values = op == TSDB_OP_QUERY_VALUES || op == TSDB_OP_QUERY_LTTB;
    size_t row = op == TSDB_OP_QUERY_TWA ? TSDB_TWA_ROW : fields & TSDB_SPREAD ? TSDB_SPREAD_ROW : TSDB_BUCKET_ROW;
    size_t next = 0;   /* the lowest index the next frame may have */
    bool open = false; /* series `next` has had frames, but not its last */
    uint8_t open_type = 0; /* the value type of those frames: a series has one, and callers size buffers by it */
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
            /* an error END carries the HA body after its u32 answered (tsdb_wire_error_shape) */
            bool ha = st == TSDB_ERR_MOVED || st == TSDB_ERR_UNAVAILABLE || st == TSDB_ERR_PARTIAL ||
                      st == TSDB_ERR_DROPPED;
            if (len < 4 || (len != 4 && !ha) || open)
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
            if (len != 8 + (size_t)k * (8 + aw) + valid || (st && !b[0]) || (open && b[1] != open_type))
                goto corrupt;
            open_type = b[1];
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

/* The template of q: the inner op's payload for series TSDB_MANY_SERIES, with 0 where per-series arrays take over.
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
    case TSDB_MULTI_VALUES: *op = TSDB_OP_QUERY_VALUES, len = 33; break;
    case TSDB_MULTI_LTTB: *op = TSDB_OP_QUERY_LTTB, len = 41; break;
    case TSDB_MULTI_BUCKETS:
    case TSDB_MULTI_SPREAD:
        if (nb)
            *op = TSDB_OP_QUERY_BOUNDS, len = 37 + nb * 8 + spread;
        else
            *op = TSDB_OP_QUERY_BUCKETS, len = 48 + spread;
        break;
    case TSDB_MULTI_TWA: *op = TSDB_OP_QUERY_TWA, len = TSDB_TWA_HEADER + nb * 8; break;
    default: return TSDB_ERR_INVALID;
    }
    uint8_t *t = calloc(len, 1);
    if (!t)
        return TSDB_ERR_NOMEM;
    uint64_t any = TSDB_MANY_SERIES;
    uint32_t cnt = g ? g->count : 0, n32 = (uint32_t)nb;
    memcpy(t, &any, 8);
    put64(t + 8, start);
    put64(t + 16, end);
    uint64_t lim = q->opts.limit;
    uint8_t desc = q->opts.order == TSDB_DESC;
    switch (*op) {
    case TSDB_OP_QUERY_VALUES:
        t[24] = desc;
        memcpy(t + 25, &lim, 8);
        break;
    case TSDB_OP_QUERY_LTTB: {
        uint64_t th = q->threshold;
        memcpy(t + 24, &th, 8);
        t[32] = desc;
        memcpy(t + 33, &lim, 8);
        break;
    }
    case TSDB_OP_QUERY_BOUNDS:
        t[24] = desc;
        memcpy(t + 25, &lim, 8);
        memcpy(t + 33, &n32, 4);
        memcpy(t + 37, bounds, nb * 8);
        if (spread)
            t[37 + nb * 8] = TSDB_SPREAD;
        break;
    case TSDB_OP_QUERY_BUCKETS:
        put64(t + 24, g ? 0 : interval);
        t[32] = g ? (uint8_t)g->unit : 0;
        memcpy(t + 33, &cnt, 4);
        t[37] = desc;
        memcpy(t + 38, &lim, 8);
        if (spread)
            t[48] = TSDB_SPREAD;
        break;
    case TSDB_OP_QUERY_TWA: {
        uint64_t tl = q->twa.limit;
        t[24] = (uint8_t)q->twa.method;
        t[25] = q->twa.order == TSDB_DESC;
        t[26] = nb || !g ? 0 : (uint8_t)g->unit;
        put64(t + 28, q->twa.max_gap_ms);
        memcpy(t + 36, &tl, 8);
        put64(t + 44, nb || g ? 0 : interval);
        uint32_t c2 = nb ? 0 : cnt;
        memcpy(t + 52, &c2, 4);
        memcpy(t + 56, &n32, 4);
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
static int multi_send_all(mrun *m, const tsdb_multi_query *q, const tsdb_sid *series, size_t n, const int64_t *bounds,
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
        parts[np++] = (struct iovec){(void *)(series + at), k * 8};
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
static int multi_single(mrun *m, const tsdb_multi_query *q, const tsdb_sid *series, size_t n)
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

static int multi_local(mrun *m, const tsdb_multi_query *q, const tsdb_sid *series, size_t n)
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
    rc = tsdb_calendar_bounds(q->group->timezone, q->group->unit, q->group->count, L, H == INT64_MAX ? H : H + 1,
                              &bounds, &nb);
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

int tsdbc_query_multi(tsdbc *c, const tsdb_multi_query *q, const tsdb_sid *series, size_t n,
                      const tsdb_multi_opts *mo, tsdb_multi_fn fn, void *ctx)
{
    int rc = tsdbc_wait(c);
    if (rc)
        return rc;
    if (!q || !fn || (n && !series) || (q->nbounds && !q->bounds) ||
        (q->trim & ~(TSDB_MULTI_TRIM_LO | TSDB_MULTI_TRIM_HI)))
        return TSDB_ERR_INVALID;
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
    /* The buffer is sized by the type of the series' first rows: rows of another (wider) type would not fit.
     * multi_read refuses such a reply already; this keeps the buffer safe whatever reaches it. */
    if (o->n && in->values.type != o->values.type)
        return k->err = TSDB_ERR_CORRUPT;
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

int tsdbc_query_multi_collect(tsdbc *c, const tsdb_multi_query *q, const tsdb_sid *series, size_t n,
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

/* ------------------------------------------------- nodes, TOPOLOGY and STATUS (protocol 13) */

static uint64_t shuffle_next(uint64_t *x)
{
    *x ^= *x << 13;
    *x ^= *x >> 7;
    *x ^= *x << 17;
    return *x;
}

int tsdbc_connect_nodes(const char *nodes, const tsdbc_options *o, tsdbc **out)
{
    *out = NULL;
    static const tsdbc_options no_options;
    if (!o)
        o = &no_options;
    if (!nodes)
        return TSDB_ERR_INVALID;
    char **list = NULL;
    size_t n = 0;
    int rc = 0;
    const char *p = nodes;
    while (*p) {
        const char *e = strchr(p, ',');
        size_t l = e ? (size_t)(e - p) : strlen(p);
        char host[256];
        int port;
        char *item = strndup(p, l);
        char **nl = item ? realloc(list, (n + 1) * sizeof *list) : NULL;
        if (!nl) {
            free(item);
            rc = TSDB_ERR_NOMEM;
            break;
        }
        list = nl;
        if (parse_node(item, host, sizeof host, &port) != 0) {
            free(item);
            rc = TSDB_ERR_INVALID;
            break;
        }
        list[n++] = item;
        p += l + (e ? 1 : 0);
        if (!e)
            break;
    }
    if (rc == 0 && n == 0)
        rc = TSDB_ERR_INVALID;
    /* shuffled once, so a fleet of clients spreads over the nodes */
    uint64_t x = (uint64_t)mono_ms() * 0x9e3779b97f4a7c15ull ^ (uint64_t)(uintptr_t)list ^ ((uint64_t)getpid() << 32);
    if (!x)
        x = 1;
    for (size_t i = n; rc == 0 && i > 1; i--) {
        size_t j = (size_t)(shuffle_next(&x) % i);
        char *t = list[i - 1];
        list[i - 1] = list[j];
        list[j] = t;
    }
    for (size_t i = 0; rc == 0 && i < n; i++) {
        char host[256];
        int port;
        parse_node(list[i], host, sizeof host, &port);
        tsdbc *c;
        int r = tsdbc_connect_opts(host, port, o, &c);
        if (r == 0) {
            c->nodes = list;
            c->nnodes = n;
            c->node_at = i;
            *out = c;
            return 0;
        }
        rc = r;
        if (r == TSDB_ERR_IO)
            rc = 0; /* this node is not there: the next one */
        if (r == TSDB_ERR_IO && i + 1 == n)
            rc = TSDB_ERR_IO;
    }
    for (size_t i = 0; i < n; i++)
        free(list[i]);
    free(list);
    return rc ? rc : TSDB_ERR_IO;
}

static void copy_addr(char *dst, const char *src, size_t n)
{
    memcpy(dst, src, n);
    dst[n] = 0;
}

int tsdbc_topology(tsdbc *c, tsdbc_topo **out)
{
    *out = NULL;
    if (tsdbc_server_version(c) < 13)
        return TSDB_ERR_INVALID; /* the op does not exist there */
    const uint8_t *b;
    size_t n;
    int rc = call(c, TSDB_OP_TOPOLOGY, NULL, 0, NULL, 0, &b, &n);
    if (rc)
        return rc;
    tsdb_topo_it it;
    if (tsdb_wire_topo_open(&it, b, n) != 0)
        return TSDB_ERR_CORRUPT;
    tsdbc_topo *t = calloc(1, sizeof *t);
    if (!t)
        return TSDB_ERR_NOMEM;
    t->gen = it.gen;
    t->nodes = calloc(it.nn ? it.nn : 1, sizeof *t->nodes);
    if (!t->nodes) {
        free(t);
        return TSDB_ERR_NOMEM;
    }
    tsdb_topo_node nd;
    int r;
    while ((r = tsdb_wire_topo_node(&it, &nd)) == 1) {
        tsdbc_node *o = &t->nodes[t->nnodes++];
        o->node_id = nd.node_id;
        o->alive = nd.alive;
        copy_addr(o->addr, nd.addr, nd.addr_len);
    }
    if (r < 0) {
        tsdbc_topology_free(t);
        return TSDB_ERR_CORRUPT;
    }
    t->vols = calloc(it.nv ? it.nv : 1, sizeof *t->vols);
    if (!t->vols) {
        tsdbc_topology_free(t);
        return TSDB_ERR_NOMEM;
    }
    tsdb_topo_vol v;
    while ((r = tsdb_wire_topo_vol(&it, &v)) == 1) {
        tsdbc_vol *o = &t->vols[t->nvols++];
        o->vol = v.vol;
        o->role = v.role;
        o->health = v.health;
        o->epoch = v.epoch;
        o->reason = v.reason;
        o->domain = v.domain;
        o->since_ms = v.since_ms;
        o->owner = v.owner;
        o->server = v.server;
        copy_addr(o->addr, v.addr, v.addr_len);
        o->path = strndup(v.path, v.path_len);
        if (!o->path) {
            tsdbc_topology_free(t);
            return TSDB_ERR_NOMEM;
        }
    }
    if (r < 0) {
        tsdbc_topology_free(t);
        return TSDB_ERR_CORRUPT;
    }
    *out = t;
    return 0;
}

void tsdbc_topology_free(tsdbc_topo *t)
{
    if (!t)
        return;
    for (size_t i = 0; i < t->nvols; i++)
        free(t->vols[i].path);
    free(t->vols);
    free(t->nodes);
    free(t);
}

int tsdbc_status(tsdbc *c, char **text)
{
    *text = NULL;
    if (tsdbc_server_version(c) < 13)
        return TSDB_ERR_INVALID;
    const uint8_t *b;
    size_t n;
    int rc = call(c, TSDB_OP_STATUS, NULL, 0, NULL, 0, &b, &n);
    if (rc)
        return rc;
    if (n < 4 || tsdb_wire_rd32(b) != n - 4)
        return TSDB_ERR_CORRUPT;
    *text = strndup((const char *)b + 4, n - 4);
    return *text ? 0 : TSDB_ERR_NOMEM;
}

/* ------------------------------------------------------- the newest value of many series (protocol 14) */

/* One QUERY_LATEST for ids[0 .. n-1] (n in 1 .. TSDB_LATEST_MAX_IDS), ends per id or NULL. The reply comes in frames
 * of up to 65,536 records in index order; records decode into out[]. A read-only request, so an UNAVAILABLE or MOVED
 * before the first frame is retried by read_reply; once a frame has been delivered it cannot be. */
static int latest_send(tsdbc *c, const tsdb_sid *ids, size_t n, const int64_t *ends, int64_t end, bool with_nulls,
                       int threads, tsdb_latest_result *out)
{
    uint8_t h[TSDB_LATEST_HEADER] = {(uint8_t)((with_nulls ? TSDB_LATEST_WITH_NULLS : 0) |
                                               (ends ? TSDB_LATEST_PER_END : 0))};
    uint16_t th = threads < 0 ? 0 : threads > 0xffff ? 0xffff : (uint16_t)threads;
    uint32_t n32 = (uint32_t)n;
    int64_t e = ends ? 0 : end;
    memcpy(h + 2, &th, 2);
    memcpy(h + 4, &n32, 4);
    memcpy(h + 8, &e, 8);
    struct iovec parts[4];
    int np = 1;
    parts[np++] = (struct iovec){h, sizeof h};
    if (ends)
        parts[np++] = (struct iovec){(void *)ends, n * 8};
    parts[np++] = (struct iovec){(void *)ids, n * 8};
    int rc = send_reqv(c, TSDB_OP_QUERY_LATEST, parts, np);
    if (rc)
        return rc;
    size_t next = 0;
    for (;;) {
        int32_t st;
        const uint8_t *b;
        size_t len;
        if ((rc = read_reply(c, &st, &b, &len)) != 0)
            return rc;
        if (len < TSDB_LATEST_PREFIX)
            goto corrupt;
        uint32_t first, m;
        memcpy(&first, b, 4);
        memcpy(&m, b + 4, 4);
        uint8_t last = b[8];
        if (b[9] || b[10] || b[11] || last > 1)
            goto corrupt;
        if (st) /* the request's own status: an empty frame (then, for the HA statuses, their body) */
            return m || !last ? (c->broken = true, TSDB_ERR_CORRUPT) : st;
        if (first != next || m > TSDB_LATEST_FRAME_IDS || m > n - next ||
            len != TSDB_LATEST_PREFIX + (size_t)m * TSDB_LATEST_REC || (last ? next + m != n : m == 0))
            goto corrupt;
        for (uint32_t i = 0; i < m; i++) {
            const uint8_t *r = b + TSDB_LATEST_PREFIX + (size_t)i * TSDB_LATEST_REC;
            tsdb_latest_result *o = &out[next + i];
            int32_t rs;
            int64_t ts;
            uint64_t v;
            memcpy(&rs, r, 4);
            memcpy(&ts, r + 8, 8);
            memcpy(&v, r + 16, 8);
            uint8_t kind = r[4], type = r[5];
            if (kind > TSDB_LATEST_NONE || type > TSDB_STATE || r[6] || r[7] ||
                (rs && (kind != TSDB_LATEST_NONE || type || ts || v)) || (kind != TSDB_LATEST_VALUE && v) ||
                (kind == TSDB_LATEST_NONE && ts) || (kind == TSDB_LATEST_NULL && !with_nulls))
                goto corrupt;
            *o = (tsdb_latest_result){.status = rs, .kind = kind, .type = type, .ts = ts};
            _Static_assert(sizeof o->v == 8, "the value union is the wire slot");
            memcpy(&o->v, &v, 8); /* little endian: float32, bool and state sit in the low bytes */
        }
        next += m;
        if (last)
            return 0;
        stream_more(c);
    }
corrupt:
    c->broken = true;
    return TSDB_ERR_CORRUPT;
}

int tsdbc_latest(tsdbc *c, const tsdb_sid *ids, size_t n, const tsdb_latest_opts *opts, tsdb_latest_result *out)
{
    int rc = tsdbc_wait(c);
    if (rc)
        return rc;
    if (n > TSDB_LATEST_MAX_IDS || (n && (!ids || !out)))
        return TSDB_ERR_INVALID;
    if (n == 0)
        return 0;
    if (server_version(c) < 14) /* an older server does not know the op: nothing is sent */
        return c->broken ? TSDB_ERR_IO : TSDB_ERR_INVALID;
    return latest_send(c, ids, n, opts ? opts->ends : NULL, opts ? opts->end : INT64_MAX,
                       opts && opts->with_nulls, opts ? opts->threads : 0, out);
}

/* ------------------------------------------------------- statistics (protocol 15) */

int tsdbc_series_stats(tsdbc *c, const tsdb_sid *ids, size_t n, int flags, tsdb_series_stat *out)
{
    int rc = tsdbc_wait(c);
    if (rc)
        return rc;
    if ((flags & ~TSDB_SSTAT_REQ_RESIDENT) || (n && (!ids || !out)))
        return TSDB_ERR_INVALID;
    if (n == 0)
        return 0;
    if (server_version(c) < 15) /* an older server does not know the op: nothing is sent */
        return c->broken ? TSDB_ERR_IO : TSDB_ERR_INVALID;
    _Static_assert(sizeof(tsdb_series_stat) == TSDB_STATS_SERIES_REC, "the record is the wire record");
    size_t chunk = (flags & TSDB_SSTAT_REQ_RESIDENT) ? TSDB_STATS_SERIES_RESIDENT_MAX : TSDB_STATS_SERIES_MAX_IDS;
    for (size_t at = 0; at < n;) {
        size_t m = n - at < chunk ? n - at : chunk;
        uint32_t h[2] = {(uint32_t)m, (flags & TSDB_SSTAT_REQ_RESIDENT) ? TSDB_STATS_RESIDENT : 0};
        const uint8_t *b;
        size_t len;
        if ((rc = call(c, TSDB_OP_STATS_SERIES, h, sizeof h, ids + at, m * 8, &b, &len)) != 0) {
            if (rc > 0)
                rc = TSDB_ERR_CORRUPT;
            return rc;
        }
        uint32_t done, z;
        if (len < 8)
            goto corrupt;
        memcpy(&done, b, 4);
        memcpy(&z, b + 4, 4);
        if (z || done == 0 || done > m || len != 8 + (size_t)done * TSDB_STATS_SERIES_REC)
            goto corrupt;
        memcpy(out + at, b + 8, (size_t)done * TSDB_STATS_SERIES_REC);
        at += done; /* the server's work budget may have stopped it early: ask again for the rest */
    }
    return 0;
corrupt:
    c->broken = true;
    return TSDB_ERR_CORRUPT;
}

void tsdbc_tenant_result_free(tsdbc_tenant_result *r)
{
    if (!r)
        return;
    for (size_t i = 0; i < r->nrows; i++) {
        free(r->rows[i].prefix);
        free(r->rows[i].hot);
    }
    free(r->rows);
    memset(r, 0, sizeof *r);
}

/* Decodes the rows of one STATS_TENANTS reply (header already read) into r; false when the frame is malformed. */
static bool tenant_rows(const uint8_t *b, size_t len, uint32_t count, int want, tsdbc_tenant_result *r)
{
    size_t o = TSDB_TSTATS_HEADER;
    for (uint32_t i = 0; i < count; i++) {
        if (len - o < TSDB_TSTATS_ROW_HEAD)
            return false;
        tsdbc_tenant_row row = {0};
        uint32_t rflags;
        uint16_t pl, nhot;
        memcpy(&row.status, b + o, 4);
        memcpy(&row.volume, b + o + 4, 4);
        memcpy(&rflags, b + o + 8, 4);
        memcpy(&pl, b + o + 12, 2);
        memcpy(&nhot, b + o + 14, 2);
        o += TSDB_TSTATS_ROW_HEAD;
        size_t padded = ((size_t)pl + 7) & ~(size_t)7;
        bool sum = row.volume == TSDB_TSTATS_VOLUME_SUM;
        size_t need = padded + TSDB_TSTATS_DATA_FIELDS * 8 +
                      (sum && (want & TSDB_TSTATS_CALLER) ? TSDB_TSTATS_CALLER_FIELDS * 8 : 0) +
                      (size_t)nhot * TSDB_TSTATS_HOT_REC;
        if (len - o < need || (!sum && nhot))
            return false;
        row.prefix = malloc((size_t)pl + 1);
        if (!row.prefix)
            return false;
        memcpy(row.prefix, b + o, pl);
        row.prefix[pl] = 0;
        row.prefix_len = pl;
        o += padded;
        row.st.flags = rflags;
        row.st.generation = r->generation;
        row.st.list_hash = r->list_hash;
        memcpy(&row.st.series, b + o, TSDB_TSTATS_DATA_FIELDS * 8);
        o += TSDB_TSTATS_DATA_FIELDS * 8;
        if (sum && (want & TSDB_TSTATS_CALLER)) {
            _Static_assert(sizeof(tsdbc_tenant_caller) == TSDB_TSTATS_CALLER_FIELDS * 8, "the wire's 20 counters");
            memcpy(&row.caller, b + o, sizeof row.caller);
            row.has_caller = 1;
            o += sizeof row.caller;
        }
        if (nhot) {
            row.hot = malloc((size_t)nhot * sizeof *row.hot);
            if (!row.hot) {
                free(row.prefix);
                return false;
            }
            for (uint16_t j = 0; j < nhot; j++, o += TSDB_TSTATS_HOT_REC) {
                row.hot[j].kind = b[o];
                memcpy(&row.hot[j].id, b + o + 8, 8);
                memcpy(&row.hot[j].estimate, b + o + 16, 8);
                memcpy(&row.hot[j].error, b + o + 24, 8);
            }
            row.nhot = nhot;
        }
        tsdbc_tenant_row *nr = realloc(r->rows, (r->nrows + 1) * sizeof *nr);
        if (!nr) {
            free(row.prefix);
            free(row.hot);
            return false;
        }
        r->rows = nr;
        r->rows[r->nrows++] = row;
    }
    return o == len;
}

int tsdbc_tenant_stats(tsdbc *c, const char *const *prefixes, size_t n, int flags, unsigned k, tsdbc_tenant_result *out)
{
    int rc = tsdbc_wait(c);
    if (rc)
        return rc;
    if (!out || (n && !prefixes) || n > 65535 || k > TSDB_TSTATS_MAX_K ||
        (flags & ~(TSDB_TSTATS_VOLUMES | TSDB_TSTATS_HOT | TSDB_TSTATS_CALLER)))
        return TSDB_ERR_INVALID;
    memset(out, 0, sizeof *out);
    size_t total = 0;
    for (size_t i = 0; i < n; i++) {
        size_t l = prefixes[i] ? strlen(prefixes[i]) : 0;
        if (l > 65535)
            return TSDB_ERR_INVALID;
        total += 2 + l;
    }
    if (server_version(c) < 15)
        return c->broken ? TSDB_ERR_IO : TSDB_ERR_INVALID;
    uint8_t *names = malloc(total + 1);
    if (!names)
        return TSDB_ERR_NOMEM;
    size_t o = 0;
    for (size_t i = 0; i < n; i++) {
        uint16_t l = prefixes[i] ? (uint16_t)strlen(prefixes[i]) : 0;
        memcpy(names + o, &l, 2);
        memcpy(names + o + 2, prefixes[i], l);
        o += 2 + l;
    }
    uint32_t cursor = 0;
    int restarts = 0;
    for (;;) {
        uint8_t h[16] = {0};
        uint16_t f = (uint16_t)flags, k16 = (uint16_t)k;
        uint32_t n32 = (uint32_t)n;
        memcpy(h, &f, 2);
        memcpy(h + 2, &k16, 2);
        memcpy(h + 4, &n32, 4);
        memcpy(h + 8, &cursor, 4);
        const uint8_t *b;
        size_t len;
        rc = call(c, TSDB_OP_STATS_TENANTS, h, sizeof h, names, total, &b, &len);
        if (rc) {
            if (rc > 0)
                rc = TSDB_ERR_CORRUPT;
            goto fail;
        }
        uint32_t count, rflags, next, gen;
        uint64_t hash, counted, listed;
        if (len < TSDB_TSTATS_HEADER)
            goto corrupt;
        memcpy(&count, b, 4);
        memcpy(&rflags, b + 4, 4);
        memcpy(&next, b + 8, 4);
        memcpy(&gen, b + 12, 4);
        memcpy(&hash, b + 16, 8);
        memcpy(&counted, b + 24, 8);
        memcpy(&listed, b + 32, 8);
        if (cursor && (gen != out->generation || hash != out->list_hash)) {
            /* the tenant list changed between two replies (a SIGHUP or a settle): a cursor into the old list means
             * nothing, so ask again from the start, a few times at most */
            tsdbc_tenant_result_free(out);
            if (++restarts > 3) {
                rc = TSDB_ERR_IO;
                goto fail;
            }
            cursor = 0;
            continue;
        }
        out->flags |= rflags;
        out->generation = gen;
        out->list_hash = hash;
        out->parts_counted = counted;
        out->parts_listed = listed;
        if (!tenant_rows(b, len, count, flags, out))
            goto corrupt;
        if (next == TSDB_TSTATS_DONE)
            break;
        if (next <= cursor || (count == 0 && next == cursor))
            goto corrupt;
        cursor = next;
    }
    free(names);
    return 0;
corrupt:
    c->broken = true;
    rc = TSDB_ERR_CORRUPT;
fail:
    free(names);
    tsdbc_tenant_result_free(out);
    return rc;
}

static uint64_t name_hash(const char *s) /* FNV-1a */
{
    uint64_t h = 1469598103934665603ull;
    for (; *s; s++)
        h = (h ^ (uint8_t)*s) * 1099511628211ull;
    return h | 1; /* 0 marks a free slot */
}

static struct name_ent *name_find(tsdbc *c, const char *name, uint64_t h)
{
    if (!c->nc_cap)
        return NULL;
    for (size_t i = h & (c->nc_cap - 1);; i = (i + 1) & (c->nc_cap - 1)) {
        struct name_ent *e = &c->nc[i];
        if (!e->h)
            return NULL;
        if (e->h == h && strcmp(e->name, name) == 0)
            return e;
    }
}

#define NAME_CACHE_MAX ((size_t)1 << 20) /* ids kept per connection */

static void name_put(tsdbc *c, const char *name, uint64_t h, tsdb_sid id)
{
    if (c->nc_n >= NAME_CACHE_MAX || name_find(c, name, h))
        return; /* a full cache stays as it is: later misses are asked again */
    if ((c->nc_n + 1) * 2 > c->nc_cap) {
        size_t cap = c->nc_cap ? c->nc_cap * 2 : 1024;
        struct name_ent *nc = calloc(cap, sizeof *nc);
        if (!nc)
            return;
        for (size_t i = 0; i < c->nc_cap; i++)
            if (c->nc[i].h) {
                size_t j = c->nc[i].h & (cap - 1);
                while (nc[j].h)
                    j = (j + 1) & (cap - 1);
                nc[j] = c->nc[i];
            }
        free(c->nc);
        c->nc = nc;
        c->nc_cap = cap;
    }
    char *copy = strdup(name);
    if (!copy)
        return;
    size_t j = h & (c->nc_cap - 1);
    while (c->nc[j].h)
        j = (j + 1) & (c->nc_cap - 1);
    c->nc[j] = (struct name_ent){copy, h, id};
    c->nc_n++;
}

/* Resolves the names that are not in the cache (st[i] == 1): the requests of a window go out together and their
 * replies are read in order, so that a cold list costs one round trip per window, not one per name. A name the lookup
 * refuses (NOTFOUND, DENIED, INVALID) is answered in its own record (st[i] = 2). UNAVAILABLE and MOVED, which a
 * lone request retries (read_reply), are asked again one at a time once the window is read. Any other status, or a
 * broken connection, fails the call. */
#define NAME_WINDOW 256
static int names_resolve(tsdbc *c, const char *const *names, size_t n, uint8_t *st, tsdb_sid *idv,
                         tsdb_latest_result *out)
{
    size_t win[NAME_WINDOW];
    for (size_t i = 0; i < n;) {
        size_t w = 0;
        while (i < n && w < NAME_WINDOW) {
            if (st[i] == 1)
                win[w++] = i;
            i++;
        }
        if (!w)
            continue;
        int rc = tsdbc_wait(c);
        if (rc)
            return rc;
        bool sent[NAME_WINDOW] = {false};
        size_t nsent = 0;
        for (size_t k = 0; k < w; k++) { /* an INVALID name (too long) is not sent */
            rc = series_send(c, names[win[k]], 0, NULL);
            if (rc == 0) {
                sent[k] = true;
                nsent++;
            } else if (rc == TSDB_ERR_INVALID) {
                out[win[k]] = (tsdb_latest_result){.status = TSDB_ERR_INVALID, .kind = TSDB_LATEST_NONE};
                st[win[k]] = 2;
            } else {
                if (nsent && !c->broken) /* replies of what was sent stay unread: the connection is out of step */
                    c->broken = true;
                return rc;
            }
        }
        int first = 0; /* the first hard error: the replies are all read first */
        bool again[NAME_WINDOW] = {false};
        for (size_t k = 0; k < w; k++) {
            if (!sent[k])
                continue;
            size_t at = win[k];
            tsdb_sid id = 0;
            int s = series_recv(c, &id);
            if (c->broken)
                return s ? s : TSDB_ERR_IO;
            if (s == 0) {
                name_put(c, names[at], name_hash(names[at]), id);
                idv[at] = id;
                st[at] = 0;
            } else if (s == TSDB_ERR_NOTFOUND || s == TSDB_ERR_DENIED || s == TSDB_ERR_INVALID) {
                out[at] = (tsdb_latest_result){.status = s, .kind = TSDB_LATEST_NONE};
                st[at] = 2;
            } else if (s == TSDB_ERR_UNAVAILABLE || s == TSDB_ERR_MOVED) {
                again[k] = true;
            } else if (!first) {
                first = s;
            }
        }
        if (first)
            return first;
        for (size_t k = 0; k < w; k++) {
            if (!again[k])
                continue;
            size_t at = win[k];
            tsdb_sid id;
            int s = tsdbc_series(c, names[at], 0, &id);
            if (s == 0) {
                name_put(c, names[at], name_hash(names[at]), id);
                idv[at] = id;
                st[at] = 0;
            } else if (s == TSDB_ERR_NOTFOUND || s == TSDB_ERR_DENIED || s == TSDB_ERR_INVALID) {
                out[at] = (tsdb_latest_result){.status = s, .kind = TSDB_LATEST_NONE};
                st[at] = 2;
            } else {
                return s;
            }
        }
    }
    return 0;
}

int tsdbc_latest_by_name(tsdbc *c, const char *const *names, size_t n, const tsdb_latest_opts *opts,
                         tsdb_latest_result *out)
{
    int rc = tsdbc_wait(c);
    if (rc)
        return rc;
    if (n > TSDB_LATEST_MAX_IDS || (n && (!names || !out)))
        return TSDB_ERR_INVALID;
    if (n == 0)
        return 0;
    if (server_version(c) < 14)
        return c->broken ? TSDB_ERR_IO : TSDB_ERR_INVALID;
    tsdb_sid *ids = malloc(n * sizeof *ids);
    size_t *at = malloc(n * sizeof *at); /* request slot -> index in names */
    uint8_t *st = malloc(n);             /* 0: resolved (ids[i] holds the id), 1: to look up, 2: answered in out[i] */
    tsdb_sid *idv = malloc(n * sizeof *idv);
    int64_t *ends = opts && opts->ends ? malloc(n * sizeof *ends) : NULL;
    if (!ids || !at || !st || !idv || (opts && opts->ends && !ends)) {
        free(ids);
        free(at);
        free(st);
        free(idv);
        free(ends);
        return TSDB_ERR_NOMEM;
    }
    bool miss = false;
    for (size_t i = 0; i < n; i++) {
        if (!names[i] || !*names[i]) {
            out[i] = (tsdb_latest_result){.status = TSDB_ERR_INVALID, .kind = TSDB_LATEST_NONE};
            st[i] = 2;
            continue;
        }
        struct name_ent *e = name_find(c, names[i], name_hash(names[i]));
        if (e) {
            idv[i] = e->id;
            st[i] = 0;
        } else {
            st[i] = 1;
            miss = true;
        }
    }
    if (miss)
        rc = names_resolve(c, names, n, st, idv, out);
    size_t k = 0;
    for (size_t i = 0; rc == 0 && i < n; i++) {
        if (st[i] != 0)
            continue;
        ids[k] = idv[i];
        at[k] = i;
        if (ends)
            ends[k] = opts->ends[i];
        k++;
    }
    if (rc == 0 && k) {
        tsdb_latest_result *r = malloc(k * sizeof *r);
        if (!r) {
            rc = TSDB_ERR_NOMEM;
        } else {
            rc = latest_send(c, ids, k, ends, opts ? opts->end : INT64_MAX, opts && opts->with_nulls,
                             opts ? opts->threads : 0, r);
            for (size_t j = 0; rc == 0 && j < k; j++)
                out[at[j]] = r[j];
            free(r);
        }
    }
    free(ids);
    free(at);
    free(st);
    free(idv);
    free(ends);
    return rc;
}
