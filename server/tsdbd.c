/*
 * tsdbd: serves a tsdb directory over TCP (protocol in tsdb_protocol.h).
 *
 * One thread runs the event loop: one recv and at most one send in flight per
 * connection, and an eventfd through which workers report. The loop runs on
 * io_uring, or on epoll where io_uring is not allowed (RHEL and AlmaLinux 10
 * disable it by default); --io auto (the default) tries io_uring and falls
 * back to epoll when the kernel refuses it. Only how bytes move differs: see
 * "event loop" below. Connections are accepted by a thread of their own with a
 * plain accept(), not through the ring: a pending io_uring accept keeps the
 * listening socket alive until the ring is torn down, which for a killed
 * process happens some time after its death, and meanwhile a restarted server
 * cannot bind and clients connect to a listener nobody serves. Database calls may block (fsync, decoding),
 * so complete request frames go to a pool of worker threads.
 *
 * A connection's requests run in order, one at a time, which keeps "last
 * write wins" for clients that pipeline. Workers append replies to the
 * connection's output buffer; a worker streaming a large result waits while
 * that buffer is over OUT_CAP, so a slow client cannot make the server buffer
 * without bound.
 *
 *   tsdbd --dir /data/sensors [--listen 127.0.0.1] [--port 7070] [--port-file FILE] [--workers N]
 *         [--wal-sync-ms 0] [--no-fsync] [--read-only | --follow] [--io auto|uring|epoll]
 *         [--verify-checksums] [--metrics-port N|auto [--metrics-bind ADDR]]
 *
 * --metrics-port serves Prometheus metrics (GET /metrics) on a port of its own, from a
 * thread of its own: see metrics.c. Off by default; binds to --listen unless --metrics-bind.
 *
 * --verify-checksums checks each granule's checksum before a query decodes it
 * (off by default: meant for storage without checksums of its own).
 *
 * --port 0 binds a port the kernel chooses; --port-file FILE then holds "PORT PID" (written to a temporary file
 * and renamed once the listener is up; an old FILE is removed at start), and the banner names the real port.
 * Scripts and tests start servers this way and never compute a port, so two of them cannot collide.
 * --metrics-port auto does the same for the metrics port, which the file then names as a third number.
 *
 * --follow serves queries from a directory another process writes (on this
 * host or another one, over NFS), following its parts and WAL as they change.
 *
 * Connections: up to --max-connections (default 10000) at once, each costing a
 * descriptor (the soft limit is raised to the hard one at start) and 16 KB
 * of receive buffer while idle. Requests run on --workers threads; requests
 * beyond that wait in their connection's queue.
 *
 * Signals: SIGTERM and SIGINT stop it cleanly (requests in progress finish,
 * the memtable is flushed, exit status 0); SIGHUP and SIGPIPE are ignored.
 *
 * Fail-stop: when a storage error puts the database in its failed state
 * (tsdb_failed: a failed WAL write or fsync and the like, after which it is
 * unknown what reached the disk) writes return TSDB_ERR_IO, which clients
 * already know. The server then logs one line, stops accepting connections,
 * lets the replies in flight go out for a moment and exits with status 1:
 * only a reopen recovers, so a supervisor restarts it (or a standby takes over).
 * After SIGKILL a new server can start on the same port and directory at once.
 *
 * There is no authentication: listen on a trusted interface only.
 */
#define _GNU_SOURCE
#include "tsdb.h"
#include "tsdb_protocol.h"
#include "../client/tsdb_pack.h"
#include "metrics.h"

#include <arpa/inet.h>
#include <errno.h>
#include <getopt.h>
#ifndef TSDBD_NO_URING /* make NO_URING=1: epoll only, no liburing needed at run time */
#include <liburing.h>
#endif
#include <zstd.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define RECV_MIN_FREE (16u << 10) /* per connection; grows for large frames */
#define BUF_KEEP (256u << 10)      /* buffers grown past this shrink back when idle */
#define OUT_CAP (8u << 20)
#define RAW_CHUNK_ROWS 16384

typedef struct buf {
    uint8_t *p;
    size_t len, cap;
} buf;

static int buf_reserve(buf *b, size_t extra)
{
    if (b->len + extra <= b->cap)
        return 0;
    size_t cap = b->cap ? b->cap : 4096;
    while (cap < b->len + extra)
        cap *= 2;
    uint8_t *p = realloc(b->p, cap);
    if (!p)
        return -1;
    b->p = p;
    b->cap = cap;
    return 0;
}

/* Gives memory back after a large frame or reply, keeping idle connections cheap. */
static void buf_shrink(buf *b)
{
    if (b->cap > BUF_KEEP && b->len <= BUF_KEEP / 2) {
        uint8_t *p = realloc(b->p, BUF_KEEP);
        if (p) {
            b->p = p;
            b->cap = BUF_KEEP;
        }
    }
}

typedef enum { EV_RECV, EV_SEND, EV_WAKE } ev_type;

struct conn;
typedef struct ev {
    ev_type type;
    struct conn *c;
} ev;

typedef struct req {
    struct req *next;
    struct conn *c;
    uint32_t id;
    uint8_t op;
    uint8_t *payload; /* malloc'd, so 16-byte aligned */
    size_t len;
} req;

typedef struct conn {
    struct conn *prev, *next; /* all connections, for shutdown (main thread) */
    int fd;
    ev recv_ev, send_ev;
    /* main thread only */
    buf in;
    req *q_head, *q_tail;
    bool busy;          /* a worker owns this connection's current request */
    bool recv_inflight, send_inflight, closed;
    bool eof;           /* the client sent everything it will: reply to it, then close */
    buf sending;
    size_t sent;
    bool readable, writable, rdhup; /* epoll: what the socket can do, as far as we know */
    bool in_ready, dead;            /* epoll: in the ready list; freed while in it */
    struct conn *ready_next;
    /* shared with the worker */
    pthread_mutex_t lock;
    pthread_cond_t cv;
    buf out;
    atomic_bool closing;
    bool notified;      /* an output event is queued for the main thread */
    tsdb_query_stats last_stats; /* of the previous query (LAST_STATS) */
} conn;

/* ------------------------------------------------------------ globals */

static tsdb *db;
#ifndef TSDBD_NO_URING
static struct io_uring ring;
#endif
static int wake_fd = -1;
static atomic_int stopping; /* lock-free, so the signal handler may store it; read by failwatch too */
#ifndef TSDBD_NO_URING
static ev wake_ev = {EV_WAKE, NULL};
#endif
static conn *conns;
static size_t nconns, max_conns = 10000;

/* work queue: requests ready to run */
static pthread_mutex_t work_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t work_cv = PTHREAD_COND_INITIALIZER;
static req *work_head, *work_tail;
static bool work_stop;

/* A worker streaming to a client that does not read waits for output space.
 * Enough such clients would hold every worker and stop the server for
 * everyone else, so a worker about to wait while no other is free starts an
 * extra one (they leave again when the queue is empty), and a client that
 * reads nothing for send_timeout_s loses its connection. Under work_lock. */
#define MAX_EXTRA_WORKERS 256
static int base_workers, running_workers, waiting_workers;
static pthread_cond_t workers_gone_cv = PTHREAD_COND_INITIALIZER;
static int send_timeout_s = 60;
static void *worker_main(void *arg);

static void worker_waits(int delta)
{
    pthread_mutex_lock(&work_lock);
    waiting_workers += delta;
    if (delta < 0)
        pthread_cond_broadcast(&work_cv); /* an idle extra may leave now that this one is free */
    if (delta > 0 && running_workers == waiting_workers && running_workers < base_workers + MAX_EXTRA_WORKERS &&
        !work_stop) {
        pthread_attr_t at;
        pthread_attr_init(&at);
        pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED);
        pthread_t t;
        if (pthread_create(&t, &at, worker_main, (void *)1) == 0)
            running_workers++;
        pthread_attr_destroy(&at);
    }
    pthread_mutex_unlock(&work_lock);
}

/* events from workers to the main thread */
typedef struct note {
    conn *c;
    bool done; /* the request finished (else: output is waiting) */
} note;
static pthread_mutex_t note_lock = PTHREAD_MUTEX_INITIALIZER;
static note *notes;
static size_t nnotes, notes_cap;

/* connections the accept thread took, for the main thread to set up */
static int *new_fds;
static size_t nnew, new_cap;

static void notify(conn *c, bool done)
{
    pthread_mutex_lock(&note_lock);
    if (nnotes == notes_cap) {
        notes_cap = notes_cap ? notes_cap * 2 : 256;
        notes = realloc(notes, notes_cap * sizeof *notes);
        if (!notes)
            abort();
    }
    notes[nnotes++] = (note){c, done};
    pthread_mutex_unlock(&note_lock);
    uint64_t one = 1;
    if (write(wake_fd, &one, 8) != 8) {
        /* the counter cannot overflow in practice; the main thread will see the note */
    }
}

/* ------------------------------------------------------- reply writing */

static inline void put32(uint8_t *p, uint32_t v) { memcpy(p, &v, 4); }

/* Appends a complete reply frame to c's output, waiting while the output is
 * over OUT_CAP. Returns false when the connection is going away. */
static bool reply(conn *c, uint32_t id, int32_t status, const void *a, size_t alen, const void *b, size_t blen,
                  const void *d, size_t dlen)
{
    size_t body = 4 + 1 + 4 + alen + blen + dlen;
    if (status)
        metrics_error(status);
    uint8_t hdr[13];
    put32(hdr, (uint32_t)body);
    put32(hdr + 4, id);
    hdr[8] = TSDB_OP_REPLY;
    memcpy(hdr + 9, &status, 4);
    pthread_mutex_lock(&c->lock);
    if (c->out.len > OUT_CAP && !atomic_load(&c->closing)) {
        worker_waits(1);
        struct timespec deadline;
        clock_gettime(CLOCK_REALTIME, &deadline);
        deadline.tv_sec += send_timeout_s;
        while (c->out.len > OUT_CAP && !atomic_load(&c->closing)) {
            if (pthread_cond_timedwait(&c->cv, &c->lock, &deadline) == ETIMEDOUT) {
                atomic_store(&c->closing, true); /* the client reads nothing: give the worker back */
                shutdown(c->fd, SHUT_RDWR);
            }
        }
        worker_waits(-1);
    }
    bool ok = !atomic_load(&c->closing);
    if (ok && buf_reserve(&c->out, 13 + alen + blen + dlen) != 0) {
        /* a lost reply would leave the client waiting forever: drop the connection instead */
        atomic_store(&c->closing, true);
        shutdown(c->fd, SHUT_RDWR);
        ok = false;
    }
    bool wake = false;
    if (ok) {
        memcpy(c->out.p + c->out.len, hdr, 13);
        c->out.len += 13;
        if (alen)
            memcpy(c->out.p + c->out.len, a, alen);
        c->out.len += alen;
        if (blen)
            memcpy(c->out.p + c->out.len, b, blen);
        c->out.len += blen;
        if (dlen)
            memcpy(c->out.p + c->out.len, d, dlen);
        c->out.len += dlen;
        wake = !c->notified;
        c->notified = true;
    }
    pthread_mutex_unlock(&c->lock);
    if (wake)
        notify(c, false);
    return ok;
}

static void reply_status(conn *c, uint32_t id, int32_t status) { reply(c, id, status, NULL, 0, NULL, 0, NULL, 0); }

/* ------------------------------------------------------------- requests */

typedef struct raw_out {
    conn *c;
    uint32_t id;
    int64_t *ts;
    float *v;
    uint32_t n;
    bool gone;
} raw_out;

static bool raw_flush(raw_out *o, bool last, int32_t status)
{
    uint8_t h[5];
    h[0] = last;
    put32(h + 1, o->n);
    bool ok = reply(o->c, o->id, status, h, 5, o->ts, (size_t)o->n * 8, o->v, (size_t)o->n * 4);
    o->n = 0;
    return ok;
}

static int raw_cb(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    raw_out *o = ctx;
    while (n) {
        size_t take = RAW_CHUNK_ROWS - o->n < n ? RAW_CHUNK_ROWS - o->n : n;
        memcpy(o->ts + o->n, ts, take * 8);
        memcpy(o->v + o->n, v, take * 4);
        o->n += (uint32_t)take;
        ts += take;
        v += take;
        n -= take;
        if (o->n == RAW_CHUNK_ROWS && !raw_flush(o, false, 0)) {
            o->gone = true;
            return 1; /* the client went away: stop the query */
        }
    }
    return 0;
}

/* QUERY_VALUES output: chunks of typed rows with a validity bitmap when they hold nulls. */
typedef struct values_out {
    conn *c;
    uint32_t id;
    uint8_t type, any_null;
    int64_t *ts;
    uint8_t *v; /* values, then room for the bitmap */
    uint32_t n;
    bool gone;
} values_out;

static bool values_flush(values_out *o, bool last, int32_t status)
{
    unsigned aw = tsdb_wire_width(o->type);
    size_t vb = (size_t)o->n * aw;
    uint8_t h[8] = {last, o->type, (uint8_t)(o->any_null ? TSDB_VALID : 0), 0};
    put32(h + 4, o->n);
    size_t valid = o->any_null ? ((size_t)o->n + 7) / 8 : 0;
    if (valid)
        memmove(o->v + vb, o->v + (size_t)RAW_CHUNK_ROWS * 8, valid); /* the bitmap right after the values */
    bool ok = reply(o->c, o->id, status, h, 8, o->ts, (size_t)o->n * 8, o->v, vb + valid);
    o->n = 0;
    o->any_null = 0;
    if (o->v) /* NULL for an error reply before any rows */
        memset(o->v + (size_t)RAW_CHUNK_ROWS * 8, 0, RAW_CHUNK_ROWS / 8);
    return ok;
}

static int values_cb(void *ctx, const int64_t *ts, const tsdb_values *col, size_t n)
{
    values_out *o = ctx;
    unsigned aw = tsdb_wire_width(o->type);
    uint8_t *bits = o->v + (size_t)RAW_CHUNK_ROWS * 8;
    for (size_t i = 0; i < n; i++) {
        bool known = !col->valid || ((col->valid[i >> 3] >> (i & 7)) & 1);
        o->ts[o->n] = ts[i];
        memcpy(o->v + (size_t)o->n * aw, (const uint8_t *)col->values + i * aw, aw);
        bits[o->n >> 3] |= (uint8_t)(known << (o->n & 7));
        o->any_null |= !known;
        if (++o->n == RAW_CHUNK_ROWS && !values_flush(o, false, 0)) {
            o->gone = true;
            return 1; /* the client went away: stop the query */
        }
    }
    return 0;
}

/* A packed INSERT_VALUES: unzstd, unshape into plain arrays, insert. */
static int insert_packed(const uint8_t *p, size_t len, uint32_t n, uint32_t s)
{
    unsigned type = p[8];
    bool valid = p[9] & TSDB_VALID, many = s == TSDB_MANY_SERIES;
    uint32_t raw_len;
    if (len < TSDB_VALUES_HEADER + 4)
        return TSDB_ERR_INVALID;
    memcpy(&raw_len, p + TSDB_VALUES_HEADER, 4);
    const uint8_t *z = p + TSDB_VALUES_HEADER + 4;
    size_t zlen = len - TSDB_VALUES_HEADER - 4;
    if (raw_len > tsdb_pack_raw_bound(n, type, valid, many) || ZSTD_getFrameContentSize(z, zlen) != raw_len)
        return TSDB_ERR_INVALID;
    unsigned w = tsdb_wire_width(type);
    uint8_t *raw = malloc(raw_len ? raw_len : 1);
    int64_t *ts = malloc((size_t)n * 8 + 8);
    uint8_t *vals = malloc((size_t)n * w + (size_t)n / 8 + 9); /* values, then the bitmap */
    uint32_t *series = many ? malloc((size_t)n * 4 + 4) : NULL;
    int rc = !raw || !ts || !vals || (many && !series) ? TSDB_ERR_NOMEM : 0;
    if (rc == 0) {
        size_t got = ZSTD_decompress(raw, raw_len, z, zlen);
        uint8_t *bits = vals + (size_t)n * w;
        if (ZSTD_isError(got) || got != raw_len ||
            tsdb_pack_unshape(raw, raw_len, n, type, valid, many, ts, vals, bits, series) != 0) {
            rc = TSDB_ERR_INVALID;
        } else {
            tsdb_values col = {(tsdb_type)type, vals, valid ? bits : NULL};
            rc = many ? tsdb_insert_values(db, series, ts, &col, n) : tsdb_insert_series_values(db, s, ts, &col, n);
        }
    }
    free(raw);
    free(ts);
    free(vals);
    free(series);
    return rc;
}

/* fields: 0 (bb: tsdb_bucket) or TSDB_SPREAD (bb: tsdb_spread_bucket, version 8), which appends the spread
 * to every row */
/* The most bucket rows of `row` bytes that one reply frame carries (after its count). A reply with more buckets is
 * refused (TSDB_ERR_TOO_MANY_BUCKETS) rather than built. A request with a larger limit computes at most one bucket
 * more than a frame carries (the limit it runs with). One without a limit runs as it does embedded, so it is
 * refused at once when its span exceeds the engine's bucket limit, and its work and memory follow the buckets
 * with rows (src/query.c bq_window); running it with the frame as its limit would instead walk a sparse span
 * window by window that the engine refuses as a whole. */
static size_t frame_rows(size_t row) { return (TSDB_MAX_FRAME - 9 - 4) / row; }
static size_t frame_limit(uint64_t lim, size_t row)
{
    size_t cap = frame_rows(row);
    return lim > cap ? cap + 1 : (size_t)lim;
}

static void reply_buckets(conn *c, uint32_t id, const void *bb, size_t nb, uint8_t fields)
{
    size_t row = fields & TSDB_SPREAD ? TSDB_SPREAD_ROW : TSDB_BUCKET_ROW;
    size_t stride = fields & TSDB_SPREAD ? sizeof(tsdb_spread_bucket) : sizeof(tsdb_bucket);
    if (nb > frame_rows(row)) {
        reply_status(c, id, TSDB_ERR_TOO_MANY_BUCKETS);
        return;
    }
    uint8_t *rows = malloc(nb * row + 1);
    if (!rows) {
        reply_status(c, id, TSDB_ERR_NOMEM);
        return;
    }
    for (size_t i = 0; i < nb; i++) {
        uint8_t *o = rows + i * row;
        const tsdb_bucket *b = (const tsdb_bucket *)((const uint8_t *)bb + i * stride);
        const tsdb_spread_bucket *sb = (const tsdb_spread_bucket *)b;
        memcpy(o, &b->start, 8);
        memcpy(o + 8, &b->count, 8);
        memcpy(o + 16, &b->sum, 8);
        memcpy(o + 24, &b->min, 8);
        memcpy(o + 32, &b->max, 8);
        memcpy(o + 40, &b->isum, 8);
        memcpy(o + 48, &b->imin, 8);
        memcpy(o + 56, &b->imax, 8);
        if (fields & TSDB_SPREAD) {
            memcpy(o + 64, &sb->range, 8);
            memcpy(o + 72, &sb->var_pop, 8);
            memcpy(o + 80, &sb->var_samp, 8);
            memcpy(o + 88, &sb->stddev_pop, 8);
            memcpy(o + 96, &sb->stddev_samp, 8);
        }
    }
    uint32_t n = (uint32_t)nb;
    reply(c, id, 0, &n, 4, rows, nb * row, NULL, 0);
    free(rows);
}

static void reply_twa(conn *c, uint32_t id, const tsdb_twa_bucket *b, size_t nb)
{
    uint8_t *rows = malloc(nb * TSDB_TWA_ROW + 1);
    if (!rows) {
        reply_status(c, id, TSDB_ERR_NOMEM);
        return;
    }
    for (size_t i = 0; i < nb; i++) {
        uint8_t *o = rows + i * TSDB_TWA_ROW;
        memcpy(o, &b[i].start, 8);
        memcpy(o + 8, &b[i].twa, 8);
        memcpy(o + 16, &b[i].area, 8);
        memcpy(o + 24, &b[i].covered_ms, 8);
        memcpy(o + 32, &b[i].count, 8);
    }
    uint32_t n = (uint32_t)nb;
    reply(c, id, 0, &n, 4, rows, nb * TSDB_TWA_ROW, NULL, 0);
    free(rows);
}

/* QUERY_TWA: one request for the interval, calendar and bounds forms */
static int handle_twa(conn *c, uint32_t id, const uint8_t *p, size_t len)
{
    uint32_t s, count, n;
    int64_t start, end, gap, interval;
    uint64_t lim;
    if (len < TSDB_TWA_HEADER || (memcpy(&n, p + 52, 4), len != TSDB_TWA_HEADER + (size_t)n * 8) || p[23])
        return TSDB_ERR_INVALID;
    memcpy(&s, p, 4);
    memcpy(&start, p + 4, 8);
    memcpy(&end, p + 12, 8);
    memcpy(&gap, p + 24, 8);
    memcpy(&lim, p + 32, 8);
    memcpy(&interval, p + 40, 8);
    memcpy(&count, p + 48, 4);
    tsdb_twa_opts o = {.method = (tsdb_twa_method)p[20], .max_gap_ms = gap, .order = p[21] ? TSDB_DESC : TSDB_ASC,
                       .limit = (size_t)lim};
    uint8_t unit = p[22];
    tsdb_twa_bucket *b = NULL;
    size_t nb = 0;
    int rc;
    if (n) {
        int64_t *bounds = malloc((size_t)n * 8); /* aligned copy */
        if (!bounds)
            return TSDB_ERR_NOMEM;
        memcpy(bounds, p + TSDB_TWA_HEADER, (size_t)n * 8);
        rc = unit || interval ? TSDB_ERR_INVALID : tsdb_query_twa_bounds(db, s, start, end, bounds, n, &o, &b, &nb);
        free(bounds);
    } else if (unit) {
        tsdb_group g = {(tsdb_unit)unit, count, NULL};
        rc = interval ? TSDB_ERR_INVALID : tsdb_query_twa_group(db, s, start, end, &g, &o, &b, &nb);
    } else {
        rc = tsdb_query_twa(db, s, start, end, interval, &o, &b, &nb);
    }
    if (rc == 0)
        reply_twa(c, id, b, nb);
    free(b);
    return rc;
}

static void handle(req *r)
{
    conn *c = r->c;
    const uint8_t *p = r->payload;
    size_t len = r->len;
    int rc = 0;
    switch (r->op) {
    case TSDB_OP_HELLO: {
        /* the version both sides speak: a client never sends what it does not know, so an older one hears
         * its own version back, as it did from the server it was written against */
        uint32_t v = TSDB_PROTOCOL_VERSION, theirs;
        if (len >= 4) {
            memcpy(&theirs, p, 4);
            if (theirs && theirs < v)
                v = theirs;
        }
        reply(c, r->id, 0, &v, 4, NULL, 0, NULL, 0);
        return;
    }
    case TSDB_OP_SERIES: {
        if (len < 3) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        uint16_t n, pn = 0;
        memcpy(&n, p + 1, 2);
        if (len >= 3u + n + 2)
            memcpy(&pn, p + 3 + n, 2);
        if (n == 0 || (len != 3u + n && len != 3u + n + 2 + pn) || memchr(p + 3, 0, n) ||
            (len > 3u + n && memchr(p + 5 + n, 0, pn))) {
            rc = TSDB_ERR_INVALID; /* strndup would cut a name at a NUL and use another series */
            break;
        }
        char *name = strndup((const char *)p + 3, n);
        char *path = len > 3u + n ? strndup((const char *)p + 5 + n, pn) : NULL;
        uint32_t id = 0;
        if (!name || (len > 3u + n && !path))
            rc = TSDB_ERR_NOMEM;
        else if (!p[0])
            rc = tsdb_series_lookup(db, name, &id);
        else
            rc = tsdb_series_at(db, name, path && *path ? path : NULL, &id);
        free(name);
        free(path);
        if (rc == 0) {
            reply(c, r->id, 0, &id, 4, NULL, 0, NULL, 0);
            return;
        }
        break;
    }
    case TSDB_OP_SERIES_NAME: {
        uint32_t id;
        if (len != 4) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&id, p, 4);
        const char *name = tsdb_series_name(db, id);
        if (!name) {
            rc = TSDB_ERR_NOTFOUND;
            break;
        }
        uint16_t n = (uint16_t)strlen(name);
        reply(c, r->id, 0, &n, 2, name, n, NULL, 0);
        return;
    }
    case TSDB_OP_INSERT: {
        uint32_t n, s;
        if (len < 8) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&n, p, 4);
        memcpy(&s, p + 4, 4);
        bool many = s == TSDB_MANY_SERIES;
        if (len != 8 + (size_t)n * (many ? 16 : 12)) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        const int64_t *ts = (const int64_t *)(p + 8);
        const float *v = (const float *)(p + 8 + (size_t)n * 8);
        rc = many ? tsdb_insert(db, (const uint32_t *)(p + 8 + (size_t)n * 12), ts, v, n)
                  : tsdb_insert_series(db, s, ts, v, n);
        break;
    }
    case TSDB_OP_QUERY_RAW: {
        raw_out o = {c, r->id, NULL, NULL, 0, false};
        if (len != 20 && len != 29) {
            raw_flush(&o, true, TSDB_ERR_INVALID); /* raw replies always carry `last` and `n` */
            return;
        }
        uint32_t s;
        int64_t start, end;
        tsdb_query_opts q = {TSDB_ASC, 0};
        memcpy(&s, p, 4);
        memcpy(&start, p + 4, 8);
        memcpy(&end, p + 12, 8);
        if (len == 29) {
            uint64_t lim;
            q.order = p[20] ? TSDB_DESC : TSDB_ASC;
            memcpy(&lim, p + 21, 8);
            q.limit = (size_t)lim;
        }
        o.ts = malloc(RAW_CHUNK_ROWS * 8);
        o.v = malloc(RAW_CHUNK_ROWS * 4);
        if (!o.ts || !o.v) {
            free(o.ts);
            free(o.v);
            o.ts = NULL;
            o.v = NULL;
            raw_flush(&o, true, TSDB_ERR_NOMEM);
            return;
        }
        rc = tsdb_query_raw_ex(db, s, start, end, &q, raw_cb, &o);
        if (!o.gone)
            raw_flush(&o, true, rc); /* the rest of the rows, or the error */
        free(o.ts);
        free(o.v);
        return;
    }
    case TSDB_OP_QUERY_AGG:
    case TSDB_OP_QUERY_GROUP: {
        uint32_t s;
        int64_t start, end, interval = 0;
        tsdb_bucket *b = NULL;
        size_t nb = 0;
        if (len < 20) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&s, p, 4);
        memcpy(&start, p + 4, 8);
        memcpy(&end, p + 12, 8);
        if (r->op == TSDB_OP_QUERY_AGG) {
            if (len != 28) {
                rc = TSDB_ERR_INVALID;
                break;
            }
            memcpy(&interval, p + 20, 8);
            rc = tsdb_query_agg(db, s, start, end, interval, &b, &nb);
        } else {
            uint16_t zl;
            if (len < 20 + 1 + 4 + 1 + 8 + 2 || (memcpy(&zl, p + 34, 2), len != 36u + zl) || memchr(p + 36, 0, zl)) {
                rc = TSDB_ERR_INVALID;
                break;
            }
            uint32_t count;
            uint64_t lim;
            memcpy(&count, p + 21, 4);
            memcpy(&lim, p + 26, 8);
            char *zone = zl ? strndup((const char *)p + 36, zl) : NULL;
            tsdb_group g = {(tsdb_unit)p[20], count, zone};
            tsdb_query_opts q = {p[25] ? TSDB_DESC : TSDB_ASC, frame_limit(lim, TSDB_AGG_ROW)};
            rc = tsdb_query_group_ex(db, s, start, end, p[20] ? &g : NULL, &q, &b, &nb);
            free(zone);
        }
        if (rc == 0 && nb > frame_rows(TSDB_AGG_ROW)) {
            free(b);
            rc = TSDB_ERR_TOO_MANY_BUCKETS;
            break;
        }
        if (rc == 0) {
            uint8_t *rows = malloc(nb * TSDB_AGG_ROW + 1);
            if (!rows) {
                free(b);
                rc = TSDB_ERR_NOMEM;
                break;
            }
            for (size_t i = 0; i < nb; i++) {
                uint8_t *o = rows + i * TSDB_AGG_ROW;
                memcpy(o, &b[i].start, 8);
                memcpy(o + 8, &b[i].count, 8);
                float mn = (float)b[i].min, mx = (float)b[i].max; /* version 1 rows: float32 */
                memcpy(o + 16, &b[i].sum, 8);
                memcpy(o + 24, &mn, 4);
                memcpy(o + 28, &mx, 4);
            }
            uint32_t n = (uint32_t)nb;
            reply(c, r->id, 0, &n, 4, rows, nb * TSDB_AGG_ROW, NULL, 0);
            free(rows);
            free(b);
            return;
        }
        break;
    }
    case TSDB_OP_SERIES_CREATE: {
        uint16_t n, pn;
        if (len < 5 || (memcpy(&n, p + 1, 2), len < 5u + n)) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&pn, p + 3 + n, 2);
        if (n == 0 || len != 5u + n + pn || memchr(p + 3, 0, n) || memchr(p + 5 + n, 0, pn)) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        char *name = strndup((const char *)p + 3, n);
        char *path = pn ? strndup((const char *)p + 5 + n, pn) : NULL;
        uint32_t id = 0;
        rc = !name || (pn && !path) ? TSDB_ERR_NOMEM : tsdb_series_create(db, name, (tsdb_type)p[0], path, &id);
        free(name);
        free(path);
        if (rc == 0) {
            reply(c, r->id, 0, &id, 4, NULL, 0, NULL, 0);
            return;
        }
        break;
    }
    case TSDB_OP_SERIES_TYPE: {
        uint32_t id;
        tsdb_type t;
        if (len != 4) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&id, p, 4);
        if ((rc = tsdb_series_type(db, id, &t)) == 0) {
            uint8_t t8 = (uint8_t)t;
            reply(c, r->id, 0, &t8, 1, NULL, 0, NULL, 0);
            return;
        }
        break;
    }
    case TSDB_OP_INSERT_VALUES: {
        uint32_t n, s;
        static const uint8_t zero[6];
        /* reserved bytes and flags must be zero, so they can mean something later */
        if (len < TSDB_VALUES_HEADER || p[8] > TSDB_STATE || (p[9] & ~(TSDB_VALID | TSDB_PACKED)) ||
            memcmp(p + 10, zero, 6)) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&n, p, 4);
        memcpy(&s, p + 4, 4);
        bool many = s == TSDB_MANY_SERIES;
        if (p[9] & TSDB_PACKED) {
            rc = insert_packed(p, len, n, s);
            break;
        }
        size_t vb = (size_t)n * tsdb_wire_width(p[8]), valid = (p[9] & TSDB_VALID) ? ((size_t)n + 7) / 8 : 0;
        size_t end = TSDB_VALUES_HEADER + (size_t)n * 8 + vb + valid, sofs = (end + 3) & ~(size_t)3;
        if (len != (many ? sofs + (size_t)n * 4 : end)) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        const int64_t *ts = (const int64_t *)(p + TSDB_VALUES_HEADER); /* the payload is 8-aligned */
        const uint8_t *vals = p + TSDB_VALUES_HEADER + (size_t)n * 8;
        tsdb_values col = {(tsdb_type)p[8], vals, valid ? vals + vb : NULL};
        rc = many ? tsdb_insert_values(db, (const uint32_t *)(p + sofs), ts, &col, n)
                  : tsdb_insert_series_values(db, s, ts, &col, n);
        break;
    }
    case TSDB_OP_QUERY_VALUES:
    case TSDB_OP_QUERY_LTTB: { /* the same chunks: LTTB's points are rows of the series */
        bool lttb = r->op == TSDB_OP_QUERY_LTTB;
        values_out o = {c, r->id, 0, 0, NULL, NULL, 0, false};
        if (lttb ? len != 28 && len != 37 : len != 20 && len != 29) { /* version 9: LTTB's order and limit */
            values_flush(&o, true, TSDB_ERR_INVALID);
            return;
        }
        uint32_t s;
        int64_t start, end;
        tsdb_query_opts q = {TSDB_ASC, 0};
        memcpy(&s, p, 4);
        memcpy(&start, p + 4, 8);
        memcpy(&end, p + 12, 8);
        uint64_t threshold = 0;
        if (lttb)
            memcpy(&threshold, p + 20, 8);
        if (len == 29 || len == 37) {
            uint64_t lim;
            size_t at = lttb ? 28 : 20;
            q.order = p[at] ? TSDB_DESC : TSDB_ASC;
            memcpy(&lim, p + at + 1, 8);
            q.limit = lim > SIZE_MAX ? SIZE_MAX : (size_t)lim;
        }
        tsdb_type t = TSDB_FLOAT32;
        tsdb_series_type(db, s, &t); /* an unknown series: no rows */
        o.type = (uint8_t)t;
        o.ts = malloc(RAW_CHUNK_ROWS * 8);
        o.v = calloc((size_t)RAW_CHUNK_ROWS * 8 + RAW_CHUNK_ROWS / 8, 1);
        if (!o.ts || !o.v) {
            free(o.ts);
            free(o.v);
            o.ts = NULL;
            o.v = NULL;
            values_flush(&o, true, TSDB_ERR_NOMEM);
            return;
        }
        if (!lttb)
            rc = tsdb_query_values(db, s, start, end, &q, values_cb, &o);
        else if (threshold > SIZE_MAX)
            rc = TSDB_ERR_INVALID;
        else
            rc = tsdb_query_lttb_ex(db, s, start, end, (size_t)threshold, &q, values_cb, &o);
        if (!o.gone)
            values_flush(&o, true, rc); /* the rest of the rows, or the error */
        free(o.ts);
        free(o.v);
        return;
    }
    case TSDB_OP_STATE: {
        uint32_t s, code;
        uint16_t n;
        if (len < 7 || (memcpy(&n, p + 5, 2), len != 7u + n) || n == 0 || memchr(p + 7, 0, n)) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&s, p, 4);
        char *name = strndup((const char *)p + 7, n);
        rc = !name ? TSDB_ERR_NOMEM : p[4] ? tsdb_state(db, s, name, &code) : tsdb_state_lookup(db, s, name, &code);
        free(name);
        if (rc == 0) {
            reply(c, r->id, 0, &code, 4, NULL, 0, NULL, 0);
            return;
        }
        break;
    }
    case TSDB_OP_STATE_NAME: {
        uint32_t s, code;
        if (len != 8) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&s, p, 4);
        memcpy(&code, p + 4, 4);
        const char *name = tsdb_state_name(db, s, code);
        if (!name) {
            rc = TSDB_ERR_NOTFOUND;
            break;
        }
        uint16_t n = (uint16_t)strlen(name);
        reply(c, r->id, 0, &n, 2, name, n, NULL, 0);
        return;
    }
    case TSDB_OP_QUERY_BUCKETS: {
        uint32_t s, count;
        int64_t start, end, interval;
        uint64_t lim;
        uint16_t zl;
        uint8_t fields = 0; /* version 8: an optional byte after the zone */
        if (len < 44 || (memcpy(&zl, p + 42, 2), len != 44u + zl && len != 45u + zl) || memchr(p + 44, 0, zl) ||
            (len == 45u + zl && ((fields = p[44 + zl]) & ~TSDB_SPREAD))) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&s, p, 4);
        memcpy(&start, p + 4, 8);
        memcpy(&end, p + 12, 8);
        memcpy(&interval, p + 20, 8);
        memcpy(&count, p + 29, 4);
        memcpy(&lim, p + 34, 8);
        tsdb_query_opts q = {p[33] ? TSDB_DESC : TSDB_ASC,
                             frame_limit(lim, fields & TSDB_SPREAD ? TSDB_SPREAD_ROW : TSDB_BUCKET_ROW)};
        tsdb_bucket *b = NULL;
        size_t nb = 0;
        char *zone = zl ? strndup((const char *)p + 44, zl) : NULL;
        tsdb_group g = {(tsdb_unit)p[28], count, zone};
        if (zl && !zone) {
            rc = TSDB_ERR_NOMEM;
        } else if (fields & TSDB_SPREAD) { /* the spread costs a second pass: only when asked for */
            tsdb_spread_bucket *sb = NULL;
            rc = p[28] == 0 ? tsdb_query_spread(db, s, start, end, interval, &q, &sb, &nb)
                            : tsdb_query_spread_group(db, s, start, end, &g, &q, &sb, &nb);
            b = (tsdb_bucket *)sb;
        } else if (p[28] == 0) {
            rc = tsdb_query_agg_ex(db, s, start, end, interval, &q, &b, &nb);
        } else {
            rc = tsdb_query_group_ex(db, s, start, end, &g, &q, &b, &nb);
        }
        free(zone);
        if (rc == 0) {
            reply_buckets(c, r->id, b, nb, fields);
            free(b);
            return;
        }
        free(b);
        break;
    }
    case TSDB_OP_LAST_STATS: {
        pthread_mutex_lock(&c->lock);
        tsdb_query_stats st = c->last_stats;
        pthread_mutex_unlock(&c->lock);
        uint64_t v[10] = {st.rows,  st.rows_decoded, st.rows_from_stats, st.memtable_rows,       st.bytes,
                          st.bytes_read, st.parts,  st.granules,        st.granules_from_stats, st.result_rows};
        reply(c, r->id, 0, &st.elapsed_s, 8, v, sizeof v, NULL, 0);
        return;
    }
    case TSDB_OP_QUERY_BOUNDS: {
        uint32_t s, n;
        int64_t start, end;
        uint64_t lim;
        uint8_t fields = 0; /* version 8: an optional byte after the bounds */
        if (len < 33 || (memcpy(&n, p + 29, 4), len != 33 + (size_t)n * 8 && len != 34 + (size_t)n * 8) || n == 0 ||
            (len == 34 + (size_t)n * 8 && ((fields = p[33 + (size_t)n * 8]) & ~TSDB_SPREAD))) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&s, p, 4);
        memcpy(&start, p + 4, 8);
        memcpy(&end, p + 12, 8);
        memcpy(&lim, p + 21, 8);
        tsdb_query_opts q = {p[20] ? TSDB_DESC : TSDB_ASC,
                             frame_limit(lim, fields & TSDB_SPREAD ? TSDB_SPREAD_ROW : TSDB_BUCKET_ROW)};
        int64_t *bounds = malloc((size_t)n * 8); /* aligned copy */
        tsdb_bucket *b = NULL;
        size_t nb = 0;
        if (!bounds) {
            rc = TSDB_ERR_NOMEM;
            break;
        }
        memcpy(bounds, p + 33, (size_t)n * 8);
        if (fields & TSDB_SPREAD) {
            tsdb_spread_bucket *sb = NULL;
            rc = tsdb_query_spread_bounds(db, s, start, end, bounds, n, &q, &sb, &nb);
            b = (tsdb_bucket *)sb;
        } else {
            rc = tsdb_query_bounds(db, s, start, end, bounds, n, &q, &b, &nb);
        }
        free(bounds);
        if (rc == 0) {
            reply_buckets(c, r->id, b, nb, fields);
            free(b);
            return;
        }
        free(b);
        break;
    }
    case TSDB_OP_QUERY_TWA:
        if ((rc = handle_twa(c, r->id, p, len)) == 0)
            return;
        break;
    case TSDB_OP_DELETE_RANGE: {
        uint32_t s, zero;
        int64_t start, end;
        if (len != 24 || (memcpy(&zero, p + 4, 4), zero)) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&s, p, 4);
        memcpy(&start, p + 8, 8);
        memcpy(&end, p + 16, 8);
        rc = tsdb_delete_range(db, s, start, end);
        break;
    }
    case TSDB_OP_REPLACE_RANGE: {
        uint32_t n, s;
        int64_t start, end;
        static const uint8_t zero[6];
        if (len < TSDB_REPLACE_HEADER || p[8] > TSDB_STATE || (p[9] & ~TSDB_VALID) || memcmp(p + 10, zero, 6)) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&n, p, 4);
        memcpy(&s, p + 4, 4);
        memcpy(&start, p + 16, 8);
        memcpy(&end, p + 24, 8);
        size_t vb = (size_t)n * tsdb_wire_width(p[8]), valid = (p[9] & TSDB_VALID) ? ((size_t)n + 7) / 8 : 0;
        if (len != TSDB_REPLACE_HEADER + (size_t)n * 8 + vb + valid) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        const int64_t *ts = (const int64_t *)(p + TSDB_REPLACE_HEADER); /* the payload is 8-aligned */
        const uint8_t *vals = p + TSDB_REPLACE_HEADER + (size_t)n * 8;
        tsdb_values col = {(tsdb_type)p[8], vals, valid ? vals + vb : NULL};
        rc = tsdb_replace_range(db, s, start, end, ts, &col, n);
        break;
    }
    case TSDB_OP_SET_RETENTION: {
        uint32_t s, zero;
        int64_t limit;
        if (len != 16 || (memcpy(&zero, p + 4, 4), zero)) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&s, p, 4);
        memcpy(&limit, p + 8, 8);
        rc = tsdb_set_retention(db, s, limit);
        break;
    }
    case TSDB_OP_GET_RETENTION: {
        uint32_t s;
        int64_t limit;
        if (len != 4) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&s, p, 4);
        if ((rc = tsdb_get_retention(db, s, &limit)) == 0) {
            reply(c, r->id, 0, &limit, 8, NULL, 0, NULL, 0);
            return;
        }
        break;
    }
    case TSDB_OP_COMMIT: rc = tsdb_commit(db); break;
    case TSDB_OP_FLUSH: rc = tsdb_flush(db); break;
    case TSDB_OP_OPTIMIZE: rc = tsdb_optimize(db); break;
    case TSDB_OP_STATS: {
        tsdb_stats st;
        tsdb_get_stats(db, &st);
        uint64_t v[8] = {st.parts, st.partitions, st.rows, st.bytes, st.merges, st.merged_rows,
                         st.passthrough_granules, st.memtable_rows};
        reply(c, r->id, 0, v, sizeof v, NULL, 0, NULL, 0);
        return;
    }
    default: rc = TSDB_ERR_INVALID; break;
    }
    if (rc == 0 && (r->op == TSDB_OP_INSERT || r->op == TSDB_OP_INSERT_VALUES)) {
        uint32_t n;
        memcpy(&n, p, 4); /* the length was checked: it has the row count */
        metrics_rows_inserted(n);
    }
    reply_status(c, r->id, rc);
}

static void *worker_main(void *arg)
{
    bool extra = arg != NULL; /* started while every worker waited: leaves when idle and another is free */
    for (;;) {
        pthread_mutex_lock(&work_lock);
        while (!work_head && !work_stop && (!extra || running_workers - waiting_workers <= 1))
            pthread_cond_wait(&work_cv, &work_lock);
        if (!work_head) {
            if (--running_workers == 0)
                pthread_cond_broadcast(&workers_gone_cv);
            pthread_mutex_unlock(&work_lock);
            return NULL;
        }
        req *r = work_head;
        work_head = r->next;
        if (!work_head)
            work_tail = NULL;
        pthread_mutex_unlock(&work_lock);
        int64_t began = metrics_request_begin();
        handle(r); /* even for a closed connection: the client sent it, and replies are dropped */
        metrics_request_end(r->op, began);
        if (r->op == TSDB_OP_QUERY_RAW || r->op == TSDB_OP_QUERY_AGG || r->op == TSDB_OP_QUERY_GROUP ||
            r->op == TSDB_OP_QUERY_VALUES || r->op == TSDB_OP_QUERY_BUCKETS || r->op == TSDB_OP_QUERY_BOUNDS ||
            r->op == TSDB_OP_QUERY_LTTB || r->op == TSDB_OP_QUERY_TWA) {
            tsdb_query_stats st;
            tsdb_last_query_stats(&st); /* this worker ran it */
            pthread_mutex_lock(&r->c->lock);
            r->c->last_stats = st;
            pthread_mutex_unlock(&r->c->lock);
        }
        conn *c = r->c;
        free(r->payload);
        free(r);
        notify(c, true);
    }
}

/* ---------------------------------------------------------- event loop */

/*
 * Two backends move the bytes; everything else (framing, dispatch, replies,
 * closing) is shared. With io_uring a recv or send is submitted to the ring and
 * completes later. With epoll, for kernels or policies without io_uring (RHEL
 * and AlmaLinux 10 ship it disabled: kernel.io_uring_disabled=2), the same
 * operation is wanted until the socket is ready and then done with a
 * non-blocking call. Both end in on_recv and on_send, and recv_inflight and
 * send_inflight mean "a recv / send is pending" in both, so the order of
 * replies, back-pressure and closing cannot differ between them.
 *
 * epoll is edge-triggered: a socket is added once, with EPOLLIN, EPOLLOUT and
 * EPOLLRDHUP, and never modified, so a connection changing between reading
 * and writing costs no epoll_ctl, and a client that hangs up while its request
 * runs is reported once rather than on every wait (level-triggered, EPOLLHUP
 * cannot be masked). Events only mark a connection readable or writable. A
 * recv or send that returns EAGAIN clears the mark; so does a short read, since
 * data arriving later raises a new edge, unless the peer has already shut
 * down (EPOLLRDHUP: no edge will announce the end of stream). Connections with
 * a wanted operation on a ready socket wait in the ready list, worked through
 * in rounds of at most one recv and one send each, like one batch of
 * completions: a client that sends without pause cannot hold the loop.
 * Events carry the descriptor, not the connection: an event for a connection
 * freed earlier in the same batch finds nothing in fd_conn, or the connection
 * that has the number since, for which it is a spurious wakeup.
 */
typedef enum { IO_AUTO, IO_URING, IO_EPOLL } io_kind;
static io_kind io = IO_AUTO;
static int epfd = -1;
static conn **fd_conn; /* epoll: connections by descriptor */
static size_t fd_conn_cap;
static conn *ready_head, *ready_tail; /* epoll: connections that can make progress */
static size_t nready;

#ifndef TSDBD_NO_URING
static struct io_uring_sqe *sqe_get(void)
{
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    if (!sqe) {
        io_uring_submit(&ring);
        sqe = io_uring_get_sqe(&ring);
    }
    return sqe;
}
#endif

/* epoll: queues c for the next round if a wanted operation can proceed. */
static void ep_want(conn *c)
{
    if (c->in_ready || !((c->recv_inflight && c->readable) || (c->send_inflight && c->writable)))
        return;
    c->in_ready = true;
    c->ready_next = NULL;
    if (ready_tail)
        ready_tail->ready_next = c;
    else
        ready_head = c;
    ready_tail = c;
    nready++;
}

static void submit_recv(conn *c)
{
    if (c->recv_inflight || c->closed)
        return;
    buf_shrink(&c->in);
    if (buf_reserve(&c->in, RECV_MIN_FREE) != 0)
        return;
    c->recv_inflight = true;
    if (io == IO_EPOLL) {
        ep_want(c);
        return;
    }
#ifndef TSDBD_NO_URING
    struct io_uring_sqe *sqe = sqe_get();
    io_uring_prep_recv(sqe, c->fd, c->in.p + c->in.len, c->in.cap - c->in.len, 0);
    io_uring_sqe_set_data(sqe, &c->recv_ev);
#endif
}

static void submit_send(conn *c)
{
    if (c->send_inflight || c->closed)
        return;
    if (c->sent == c->sending.len) {
        /* take whatever the worker produced since the last send */
        pthread_mutex_lock(&c->lock);
        buf t = c->sending;
        c->sending = c->out;
        c->out = t;
        c->out.len = 0;
        c->notified = false;
        pthread_cond_broadcast(&c->cv);
        pthread_mutex_unlock(&c->lock);
        c->sent = 0;
        if (c->sending.len == 0) {
            buf_shrink(&c->sending);
            return;
        }
    }
    c->send_inflight = true;
    if (io == IO_EPOLL) {
        ep_want(c);
        return;
    }
#ifndef TSDBD_NO_URING
    struct io_uring_sqe *sqe = sqe_get();
    io_uring_prep_send(sqe, c->fd, c->sending.p + c->sent, c->sending.len - c->sent, MSG_NOSIGNAL);
    io_uring_sqe_set_data(sqe, &c->send_ev);
#endif
}

static void dispatch(conn *c)
{
    if (c->busy || !c->q_head)
        return;
    req *r = c->q_head;
    c->q_head = r->next;
    if (!c->q_head)
        c->q_tail = NULL;
    r->next = NULL;
    c->busy = true;
    pthread_mutex_lock(&work_lock);
    if (work_tail)
        work_tail->next = r;
    else
        work_head = r;
    work_tail = r;
    pthread_cond_signal(&work_cv);
    pthread_mutex_unlock(&work_lock);
}

/* Closes the descriptor and frees c. A connection still in the epoll ready
 * list is only marked dead: ep_round frees it when it comes out. */
static void conn_free(conn *c)
{
    nconns--;
    metrics_conn_closed();
    if (c->prev)
        c->prev->next = c->next;
    else
        conns = c->next;
    if (c->next)
        c->next->prev = c->prev;
    while (c->q_head) {
        req *r = c->q_head;
        c->q_head = r->next;
        free(r->payload);
        free(r);
    }
    if (io == IO_EPOLL && (size_t)c->fd < fd_conn_cap)
        fd_conn[c->fd] = NULL; /* before close: the number may come back for the next connection */
    close(c->fd);
    free(c->in.p);
    free(c->out.p);
    free(c->sending.p);
    pthread_mutex_destroy(&c->lock);
    pthread_cond_destroy(&c->cv);
    if (c->in_ready)
        c->dead = true;
    else
        free(c);
}

/* Starts closing; the memory goes once nothing refers to it any more. Requests
 * already received still run, one by one, with their replies dropped: a client
 * that pipelines inserts and closes without reading the acknowledgements should
 * not lose them depending on how far the server had got. */
static void conn_close(conn *c)
{
    if (!c->closed) {
        c->closed = true;
        pthread_mutex_lock(&c->lock);
        atomic_store(&c->closing, true);
        pthread_cond_broadcast(&c->cv); /* a worker waiting for output space gives up */
        pthread_mutex_unlock(&c->lock);
        shutdown(c->fd, SHUT_RDWR); /* completes the outstanding recv (io_uring) */
        if (io == IO_EPOLL)
            c->recv_inflight = c->send_inflight = false; /* only wanted: nothing in the kernel to complete */
    }
    dispatch(c);
    if (!c->busy && !c->q_head && !c->recv_inflight && !c->send_inflight)
        conn_free(c);
}

/* After the client's end of stream: close once every request has been answered. */
static void finish_if_drained(conn *c)
{
    if (c->eof && !c->closed && !c->busy && !c->q_head && !c->send_inflight)
        conn_close(c);
}

/* Splits received bytes into requests. Returns false on a protocol error. */
static bool parse(conn *c)
{
    size_t off = 0;
    while (c->in.len - off >= 4) {
        uint32_t flen;
        memcpy(&flen, c->in.p + off, 4);
        if (flen < 5 || flen > TSDB_MAX_FRAME)
            return false;
        if (c->in.len - off < 4 + (size_t)flen) {
            /* make room for the whole frame up front */
            if (buf_reserve(&c->in, 4 + (size_t)flen - (c->in.len - off)) != 0)
                return false;
            break;
        }
        req *r = calloc(1, sizeof *r);
        if (!r)
            return false;
        r->c = c;
        memcpy(&r->id, c->in.p + off + 4, 4);
        r->op = c->in.p[off + 8];
        r->len = flen - 5;
        r->payload = malloc(r->len ? r->len : 1);
        if (!r->payload) {
            free(r);
            return false;
        }
        memcpy(r->payload, c->in.p + off + 9, r->len);
        if (c->q_tail)
            c->q_tail->next = r;
        else
            c->q_head = r;
        c->q_tail = r;
        off += 4 + (size_t)flen;
    }
    memmove(c->in.p, c->in.p + off, c->in.len - off);
    c->in.len -= off;
    return true;
}

/* A recv finished: res bytes arrived (0: end of stream), or -errno. */
static void on_recv(conn *c, ssize_t res)
{
    c->recv_inflight = false;
    if (res == 0 && !c->closed) { /* end of stream: what was received still gets its replies */
        if (getenv("TSDBD_DEBUG"))
            fprintf(stderr, "tsdbd: connection %d: recv end\n", c->fd);
        c->eof = true;
        finish_if_drained(c);
        return;
    }
    if (res < 0 || c->closed) {
        if (getenv("TSDBD_DEBUG") && !c->closed)
            fprintf(stderr, "tsdbd: connection %d: recv %s\n", c->fd, strerror((int)-res));
        conn_close(c);
        return;
    }
    c->in.len += (size_t)res;
    if (!parse(c)) {
        if (getenv("TSDBD_DEBUG"))
            fprintf(stderr, "tsdbd: connection %d: protocol error\n", c->fd);
        conn_close(c);
        return;
    }
    dispatch(c);
    submit_recv(c);
}

/* A send finished: res bytes went out, or -errno. */
static void on_send(conn *c, ssize_t res)
{
    c->send_inflight = false;
    if (res < 0 || c->closed) {
        if (getenv("TSDBD_DEBUG") && res < 0)
            fprintf(stderr, "tsdbd: connection %d: send %s\n", c->fd, strerror((int)-res));
        conn_close(c);
        return;
    }
    c->sent += (size_t)res;
    submit_send(c);
    finish_if_drained(c);
}

/* A new connection closed for want of memory or of epoll watches: the client
 * only sees it closed, so say why here, at most once a second (a shortage hits
 * every connection of a burst). Event loop thread only. */
static void drop_new_conn(int fd, const char *what, int err)
{
    static time_t last;
    time_t t = time(NULL);
    if (t != last) {
        last = t;
        fprintf(stderr, "tsdbd: connection refused: %s: %s\n", what, strerror(err));
    }
    close(fd);
}

static void on_accept(int fd)
{
    if (nconns >= max_conns) {
        close(fd); /* over the limit: the client sees the connection closed */
        metrics_conn_rejected();
        return;
    }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    conn *c = calloc(1, sizeof *c);
    if (!c) {
        drop_new_conn(fd, "connection", ENOMEM);
        return;
    }
    if (io == IO_EPOLL) {
        if ((size_t)fd >= fd_conn_cap) {
            size_t cap = fd_conn_cap ? fd_conn_cap : 1024;
            while (cap <= (size_t)fd)
                cap *= 2;
            conn **t = realloc(fd_conn, cap * sizeof *t);
            if (!t) {
                free(c);
                drop_new_conn(fd, "descriptor table", ENOMEM);
                return;
            }
            memset(t + fd_conn_cap, 0, (cap - fd_conn_cap) * sizeof *t);
            fd_conn = t;
            fd_conn_cap = cap;
        }
        struct epoll_event e = {.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET, .data.fd = fd};
        if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &e) != 0) {
            int err = errno;
            free(c);
            drop_new_conn(fd, "epoll_ctl", err); /* ENOSPC: fs.epoll.max_user_watches */
            return;
        }
        fd_conn[fd] = c;
        c->readable = c->writable = true; /* until a call says otherwise */
    }
    c->fd = fd;
    c->recv_ev = (ev){EV_RECV, c};
    c->send_ev = (ev){EV_SEND, c};
    pthread_mutex_init(&c->lock, NULL);
    pthread_cond_init(&c->cv, NULL);
    c->next = conns;
    if (conns)
        conns->prev = c;
    conns = c;
    nconns++;
    metrics_conn_opened();
    submit_recv(c);
}

static void on_wake(void)
{
    uint64_t v;
    if (read(wake_fd, &v, 8) < 0 && errno != EAGAIN)
        perror("tsdbd: eventfd");
    pthread_mutex_lock(&note_lock);
    note *batch = notes;
    size_t n = nnotes;
    notes = NULL;
    nnotes = notes_cap = 0;
    int *fds = new_fds;
    size_t nfds = nnew;
    new_fds = NULL;
    nnew = new_cap = 0;
    pthread_mutex_unlock(&note_lock);
    for (size_t i = 0; i < nfds; i++)
        on_accept(fds[i]);
    free(fds);
    for (size_t i = 0; i < n; i++) {
        conn *c = batch[i].c;
        if (batch[i].done)
            c->busy = false;
        if (c->closed) {
            conn_close(c);
            continue;
        }
        submit_send(c);
        if (batch[i].done)
            dispatch(c);
        finish_if_drained(c);
    }
    free(batch);
}

#ifndef TSDBD_NO_URING
static void uring_loop(void)
{
    struct io_uring_sqe *sqe = sqe_get();
    io_uring_prep_poll_multishot(sqe, wake_fd, POLLIN);
    io_uring_sqe_set_data(sqe, &wake_ev);
    while (!stopping) {
        io_uring_submit_and_wait(&ring, 1);
        struct io_uring_cqe *cqe;
        unsigned head, seen = 0;
        io_uring_for_each_cqe(&ring, head, cqe)
        {
            seen++;
            ev *e = io_uring_cqe_get_data(cqe);
            switch (e->type) {
            case EV_WAKE:
                on_wake();
                if (!(cqe->flags & IORING_CQE_F_MORE)) {
                    struct io_uring_sqe *s2 = sqe_get();
                    io_uring_prep_poll_multishot(s2, wake_fd, POLLIN);
                    io_uring_sqe_set_data(s2, &wake_ev);
                }
                break;
            case EV_RECV: on_recv(e->c, cqe->res); break;
            case EV_SEND: on_send(e->c, cqe->res); break;
            }
        }
        io_uring_cq_advance(&ring, seen);
    }
}
#endif

/* epoll: one round over the connections that were ready when it started.
 * in_ready stays set while one is handled, so it is not queued twice and a
 * close leaves the freeing to this loop. */
static void ep_round(void)
{
    for (size_t n = nready; n > 0 && ready_head; n--) {
        conn *c = ready_head;
        ready_head = c->ready_next;
        if (!ready_head)
            ready_tail = NULL;
        nready--;
        if (!c->dead && c->recv_inflight && c->readable) {
            size_t room = c->in.cap - c->in.len;
            ssize_t r = recv(c->fd, c->in.p + c->in.len, room, MSG_DONTWAIT);
            int err = errno;
            if (r < 0 && (err == EAGAIN || err == EWOULDBLOCK))
                c->readable = false;
            else if (r >= 0 || err != EINTR) {
                if (r == 0 || (r > 0 && (size_t)r < room && !c->rdhup))
                    c->readable = false;
                on_recv(c, r < 0 ? -err : r);
            }
        }
        if (!c->dead && c->send_inflight && c->writable) {
            ssize_t r = send(c->fd, c->sending.p + c->sent, c->sending.len - c->sent, MSG_NOSIGNAL | MSG_DONTWAIT);
            int err = errno;
            if (r < 0 && (err == EAGAIN || err == EWOULDBLOCK))
                c->writable = false;
            else if (r >= 0 || err != EINTR)
                on_send(c, r < 0 ? -err : r);
        }
        c->in_ready = false;
        if (c->dead)
            free(c);
        else
            ep_want(c);
    }
}

static void ep_loop(void)
{
    struct epoll_event evs[256];
    while (!stopping) {
        int n = epoll_wait(epfd, evs, 256, ready_head ? 0 : -1);
        if (n < 0) {
            if (errno != EINTR) {
                perror("tsdbd: epoll_wait");
                stopping = 1;
            }
            continue;
        }
        for (int i = 0; i < n; i++) {
            int fd = evs[i].data.fd;
            if (fd == wake_fd) {
                on_wake();
                continue;
            }
            conn *c = (size_t)fd < fd_conn_cap ? fd_conn[fd] : NULL;
            if (!c)
                continue;
            uint32_t m = evs[i].events;
            if (m & (EPOLLRDHUP | EPOLLHUP | EPOLLERR))
                c->rdhup = true; /* recv no longer blocks, and no further edge may come */
            if (m & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR))
                c->readable = true;
            if (m & (EPOLLOUT | EPOLLHUP | EPOLLERR))
                c->writable = true;
            ep_want(c);
        }
        ep_round();
    }
}

static size_t workers_now(void)
{
    pthread_mutex_lock(&work_lock);
    size_t n = (size_t)running_workers;
    pthread_mutex_unlock(&work_lock);
    return n;
}

/* Accepts connections until the listening socket is shut down. */
static void *accept_main(void *arg)
{
    int lfd = *(int *)arg;
    for (;;) {
        int fd = accept4(lfd, NULL, NULL, SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EINTR || errno == ECONNABORTED)
                continue;
            if (errno == EMFILE || errno == ENFILE || errno == ENOBUFS || errno == ENOMEM) {
                struct timespec d = {0, 10000000}; /* out of descriptors: let some close */
                nanosleep(&d, NULL);
                continue;
            }
            return NULL; /* EINVAL: shut down */
        }
        pthread_mutex_lock(&note_lock);
        if (nnew == new_cap) {
            new_cap = new_cap ? new_cap * 2 : 64;
            new_fds = realloc(new_fds, new_cap * sizeof *new_fds);
            if (!new_fds)
                abort();
        }
        new_fds[nnew++] = fd;
        pthread_mutex_unlock(&note_lock);
        uint64_t one = 1;
        if (write(wake_fd, &one, 8) != 8) {
        }
    }
}

static atomic_bool db_failed;

/* Polls tsdb_failed: a failure can come from any worker or from a background
 * thread of the library, with no request to report it. */
static void *failwatch_main(void *arg)
{
    int lfd = *(int *)arg;
    char msg[512];
    while (!stopping) {
        if (tsdb_failed(db, msg, sizeof msg)) {
            fprintf(stderr, "tsdbd: storage failure, exiting: %s\n", msg);
            atomic_store(&db_failed, true);
            shutdown(lfd, SHUT_RDWR); /* new clients go elsewhere; ends the accept thread */
            struct timespec grace = {0, 200000000}; /* replies in flight (TSDB_ERR_IO for writes) go out */
            nanosleep(&grace, NULL);
            stopping = 1;
            uint64_t one = 1;
            if (write(wake_fd, &one, 8) != 8) {
            }
            return NULL;
        }
        struct timespec d = {0, 20000000};
        nanosleep(&d, NULL);
    }
    return NULL;
}

static void on_signal(int sig)
{
    (void)sig;
    stopping = 1;
    uint64_t one = 1;
    if (write(wake_fd, &one, 8) != 8) {
    }
}

/* Binds and listens; *port_p is updated to the port actually bound, which is the kernel's choice for port 0. */
static int listen_on(const char *addr, int *port_p)
{
    int port = *port_p;
    int fd = socket(AF_INET6, SOCK_STREAM | SOCK_CLOEXEC, 0);
    struct sockaddr_in6 sa6 = {.sin6_family = AF_INET6, .sin6_port = htons((uint16_t)port)};
    struct sockaddr_in sa4 = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port)};
    struct sockaddr *sa;
    socklen_t salen;
    if (inet_pton(AF_INET, addr, &sa4.sin_addr) == 1) {
        close(fd);
        fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        sa = (struct sockaddr *)&sa4;
        salen = sizeof sa4;
    } else if (inet_pton(AF_INET6, addr, &sa6.sin6_addr) == 1) {
        sa = (struct sockaddr *)&sa6;
        salen = sizeof sa6;
    } else {
        fprintf(stderr, "tsdbd: bad listen address %s\n", addr);
        return -1;
    }
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    if (bind(fd, sa, salen) != 0 || listen(fd, 1024) != 0) {
        fprintf(stderr, "tsdbd: cannot listen on %s:%d: %s\n", addr, port, strerror(errno));
        close(fd);
        return -1;
    }
    struct sockaddr_storage bound;
    socklen_t bl = sizeof bound;
    if (getsockname(fd, (struct sockaddr *)&bound, &bl) == 0)
        *port_p = ntohs(bound.ss_family == AF_INET ? ((struct sockaddr_in *)&bound)->sin_port
                                                   : ((struct sockaddr_in6 *)&bound)->sin6_port);
    return fd;
}

/* "PORT PID [METRICS_PORT]\n", written to a temporary file and renamed once the listener is up, so a reader sees
 * all of it or none (a half-written pid would read as another server's, and the harness would refuse a good one). A stale file is removed at startup: whoever finds one now knows this server wrote it. */
static int write_port_file(const char *path, int port, int mport)
{
    char tmp[4096];
    if (snprintf(tmp, sizeof tmp, "%s.tmp.%d", path, (int)getpid()) >= (int)sizeof tmp)
        return -1;
    FILE *f = fopen(tmp, "w");
    if (!f)
        return -1;
    int ok = fprintf(f, "%d %d", port, (int)getpid()) > 0;
    if (mport)
        ok = (fprintf(f, " %d", mport) > 0 && ok);
    ok = (fputc('\n', f) != EOF && ok);
    ok = (fflush(f) == 0 && ok);
    ok = (fclose(f) == 0 && ok);
    if (!ok || rename(tmp, path) != 0) {
        unlink(tmp);
        return -1;
    }
    return 0;
}

int main(int argc, char **argv)
{
    const char *dir = NULL, *addr = "127.0.0.1", *port_file = NULL;
    int port = TSDB_DEFAULT_PORT, nworkers = (int)sysconf(_SC_NPROCESSORS_ONLN);
    tsdb_options o;
    tsdb_options_init(&o);
    static struct option opts[] = {{"dir", required_argument, 0, 'd'},        {"listen", required_argument, 0, 'l'},
                                   {"port", required_argument, 0, 'p'},       {"workers", required_argument, 0, 'w'},
                                   {"wal-sync-ms", required_argument, 0, 's'}, {"no-fsync", no_argument, 0, 'F'},
                                   {"read-only", no_argument, 0, 'r'},        {"follow", no_argument, 0, 'f'},
                                   {"refresh-ms", required_argument, 0, 'R'}, {"max-connections", required_argument, 0, 'm'},
                                   {"wal-max-batch-rows", required_argument, 0, 'B'},
                                   {"partition-ms", required_argument, 0, 'P'},
                                   {"send-timeout", required_argument, 0, 'T'},
                                   {"io", required_argument, 0, 'I'},
                                   {"verify-checksums", no_argument, 0, 'V'},
                                   {"metrics-port", required_argument, 0, 'M'},
                                   {"metrics-bind", required_argument, 0, 'b'},
                                   {"port-file", required_argument, 0, 'Q'},
                                   {0, 0, 0, 0}};
    int ch, metrics_port = 0, metrics_on = 0;
    const char *metrics_addr = NULL;
    while ((ch = getopt_long(argc, argv, "", opts, NULL)) != -1) {
        switch (ch) {
        case 'd': dir = optarg; break;
        case 'l': addr = optarg; break;
        case 'p': port = atoi(optarg); break; /* 0: any free port, told by --port-file and the banner */
        case 'Q': port_file = optarg; break;
        case 'w': nworkers = atoi(optarg); break;
        case 's': o.wal_sync_ms = atoi(optarg); break;
        case 'F': o.fsync = 0; break;
        case 'V': o.verify_checksums = 1; break;
        case 'M': {
            if (!strcmp(optarg, "auto")) { /* a port the kernel chooses; the port file names it */
                metrics_on = 1;
                metrics_port = 0;
                break;
            }
            char *end;
            long mp = strtol(optarg, &end, 10);
            if (!*optarg || *end || mp < 1 || mp > 65535)
                goto usage;
            metrics_on = 1;
            metrics_port = (int)mp;
            break;
        }
        case 'b': metrics_addr = optarg; break;
        case 'r': o.read_only = 1; break;
        case 'f': o.follow = 1; break;
        case 'R': o.refresh_ms = atoi(optarg); break;
        case 'm': max_conns = (size_t)atol(optarg); break;
        case 'B': o.wal_max_batch_rows = (size_t)atol(optarg); break;
        case 'P': o.partition_ms = atoll(optarg); break; /* only when the database is created */
        case 'T': send_timeout_s = atoi(optarg); break;
        case 'I':
            if (strcmp(optarg, "auto") == 0)
                io = IO_AUTO;
            else if (strcmp(optarg, "uring") == 0)
                io = IO_URING;
            else if (strcmp(optarg, "epoll") == 0)
                io = IO_EPOLL;
            else
                goto usage;
            break;
        default: goto usage;
        }
    }
    if (!dir || nworkers < 1 || send_timeout_s < 1 || metrics_port < 0 || metrics_port > 65535 ||
        (metrics_addr && !metrics_on)) {
    usage:
        fprintf(stderr, "usage: tsdbd --dir DIR [--listen ADDR] [--port %d|0] [--port-file FILE] [--workers N]\n"
                        "             [--wal-sync-ms MS]\n"
                        "             [--no-fsync] [--read-only | --follow [--refresh-ms 100]] [--max-connections 10000]\n"
                        "             [--send-timeout 60] [--io auto|uring|epoll] [--verify-checksums]\n"
                        "             [--metrics-port N|auto [--metrics-bind ADDR]]\n",
                TSDB_DEFAULT_PORT);
        return 2;
    }
    if (port_file)
        unlink(port_file);
    int rc = tsdb_open(dir, &o, &db);
    if (rc) {
        fprintf(stderr, "tsdbd: %s: %s\n", dir, tsdb_strerror(rc));
        return 1;
    }
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0 && rl.rlim_cur < rl.rlim_max) {
        rl.rlim_cur = rl.rlim_max; /* one descriptor per connection */
        setrlimit(RLIMIT_NOFILE, &rl);
    }
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0 && rl.rlim_cur != RLIM_INFINITY && max_conns + 64 > rl.rlim_cur) {
        max_conns = rl.rlim_cur > 64 ? rl.rlim_cur - 64 : 1;
        fprintf(stderr, "tsdbd: descriptor limit %llu: at most %zu connections\n", (unsigned long long)rl.rlim_cur,
                max_conns);
    }
    int lfd = listen_on(addr, &port);
    if (lfd < 0)
        return 1;
    int mfd = -1;
    if (metrics_on)
        metrics_enable(); /* before any connection: the hooks count from the start */
    if (metrics_on && (mfd = listen_on(metrics_addr ? metrics_addr : addr, &metrics_port)) < 0)
        return 1;
    wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
#ifdef TSDBD_NO_URING
    if (io == IO_URING) {
        fprintf(stderr, "tsdbd: io_uring: not in this build (NO_URING=1)\n");
        return 1;
    }
    io = IO_EPOLL;
#else
    if (io != IO_EPOLL) {
        struct io_uring_params params = {.flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN};
        if ((rc = io_uring_queue_init_params(4096, &ring, &params)) < 0) {
            params = (struct io_uring_params){0}; /* older kernels */
            rc = io_uring_queue_init_params(4096, &ring, &params);
        }
        /* Under auto every error falls back, the reason logged: EPERM is policy
         * (kernel.io_uring_disabled, seccomp), ENOSYS a kernel without it, EINVAL
         * one too old for what we ask, EACCES a security module, ENOMEM a locked
         * memory limit; epoll serves the same clients in all of these. */
        if (rc < 0 && io == IO_URING) {
            fprintf(stderr, "tsdbd: io_uring: %s\n", strerror(-rc));
            return 1;
        }
        if (rc < 0)
            fprintf(stderr, "tsdbd: io_uring unavailable (%s%s), using epoll\n", strerror(-rc),
                    rc == -EPERM ? ": see sysctl kernel.io_uring_disabled" : "");
        io = rc < 0 ? IO_EPOLL : IO_URING;
    }
#endif
    if (io == IO_EPOLL) {
        struct epoll_event e = {.events = EPOLLIN, .data.fd = wake_fd};
        if ((epfd = epoll_create1(EPOLL_CLOEXEC)) < 0 || epoll_ctl(epfd, EPOLL_CTL_ADD, wake_fd, &e) != 0) {
            fprintf(stderr, "tsdbd: epoll: %s\n", strerror(errno));
            return 1;
        }
    }
    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    signal(SIGPIPE, SIG_IGN); /* a client that went away is a send error, not a death */
    signal(SIGHUP, SIG_IGN);  /* nothing to reload; a closing terminal must not take the server down */
    pthread_t *workers = calloc((size_t)nworkers, sizeof *workers);
    base_workers = running_workers = nworkers;
    if (mfd >= 0) {
        metrics_cfg mc = {.db = db,
                          .listen_fd = mfd,
                          .io = io == IO_URING ? "io_uring" : "epoll",
                          .role = o.follow ? "follower" : o.read_only ? "read-only" : "writer",
                          .max_conns = max_conns,
                          .workers = workers_now};
        int mrc = metrics_start(&mc);
        if (mrc != 0) { /* the operator asked for it: a monitoring gap must not be silent */
            fprintf(stderr, "tsdbd: metrics thread: %s\n", strerror(-mrc));
            return 1;
        }
        fprintf(stderr, "tsdbd: metrics on %s:%d\n", metrics_addr ? metrics_addr : addr, metrics_port);
    }
    for (int i = 0; i < nworkers; i++)
        pthread_create(&workers[i], NULL, worker_main, NULL);

    pthread_t acceptor;
    pthread_create(&acceptor, NULL, accept_main, &lfd);
    pthread_t failwatch;
    pthread_create(&failwatch, NULL, failwatch_main, &lfd);
    fprintf(stderr, "tsdbd: serving %s%s on %s:%d, %d workers, up to %zu connections, simd %s, io %s\n", dir,
            o.follow ? " (following its writer)" : o.read_only ? " (read only)" : "", addr, port, nworkers,
            max_conns, tsdb_simd_level(), io == IO_URING ? "io_uring" : "epoll");
    if (port_file && write_port_file(port_file, port, metrics_on ? metrics_port : 0) != 0) { /* last: whoever reads it finds a server that serves */
        fprintf(stderr, "tsdbd: cannot write %s: %s\n", port_file, strerror(errno));
        return 1;
    }
#ifndef TSDBD_NO_URING
    if (io == IO_URING)
        uring_loop();
    else
#endif
        ep_loop();

    fprintf(stderr, "tsdbd: shutting down\n");
    shutdown(lfd, SHUT_RDWR); /* stops listening, and ends the accept thread's accept() */
    pthread_join(acceptor, NULL);
    pthread_join(failwatch, NULL);
    close(lfd);
    if (mfd >= 0)
        metrics_stop(); /* before tsdb_close: it reads the database */
    pthread_mutex_lock(&work_lock);
    work_stop = true;
    pthread_cond_broadcast(&work_cv);
    pthread_mutex_unlock(&work_lock);
    for (conn *c = conns; c; c = c->next) { /* wake workers waiting for output space */
        pthread_mutex_lock(&c->lock);
        atomic_store(&c->closing, true);
        pthread_cond_broadcast(&c->cv);
        pthread_mutex_unlock(&c->lock);
    }
    for (int i = 0; i < nworkers; i++)
        pthread_join(workers[i], NULL);
    free(workers);
    pthread_mutex_lock(&work_lock); /* and the extra ones, which are detached */
    while (running_workers > 0)
        pthread_cond_wait(&workers_gone_cv, &work_lock);
    pthread_mutex_unlock(&work_lock);
#ifndef TSDBD_NO_URING
    if (io == IO_URING)
        io_uring_queue_exit(&ring);
    else
#endif
        close(epfd);
    while (conns)
        conn_free(conns);
    while (ready_head) { /* only dead ones are left */
        conn *c = ready_head;
        ready_head = c->ready_next;
        free(c);
    }
    free(fd_conn);
    free(notes);
    for (size_t i = 0; i < nnew; i++)
        close(new_fds[i]);
    free(new_fds);
    rc = tsdb_close(db);
    if (atomic_load(&db_failed))
        return 1; /* close failed too, by design: nothing more was written */
    if (rc)
        fprintf(stderr, "tsdbd: close: %s\n", tsdb_strerror(rc));
    return rc ? 1 : 0;
}
