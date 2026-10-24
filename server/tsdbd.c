/*
 * tsdbd: serves a tsdb directory over TCP (protocol in tsdb_protocol.h).
 *
 * One thread runs an io_uring event loop: multishot accept, one recv and at
 * most one send in flight per connection, and a multishot poll on an eventfd
 * through which workers report. Database calls may block (fsync, decoding),
 * so complete request frames go to a pool of worker threads.
 *
 * A connection's requests run in order, one at a time, which keeps "last
 * write wins" for clients that pipeline. Workers append replies to the
 * connection's output buffer; a worker streaming a large result waits while
 * that buffer is over OUT_CAP, so a slow client cannot make the server buffer
 * without bound.
 *
 *   tsdbd --dir /data/sensors [--listen 127.0.0.1] [--port 7070] [--workers N]
 *         [--wal-sync-ms 0] [--no-fsync] [--read-only | --follow]
 *
 * --follow serves queries from a directory another process writes (on this
 * host or another one, over NFS), following its parts and WAL as they change.
 *
 * Connections: up to --max-connections (default 10000) at once, each costing a
 * descriptor (the soft limit is raised to the hard one at start) and 16 KB
 * of receive buffer while idle. Requests run on --workers threads; requests
 * beyond that wait in their connection's queue.
 *
 * There is no authentication: listen on a trusted interface only.
 */
#define _GNU_SOURCE
#include "tsdb.h"
#include "tsdb_protocol.h"

#include <arpa/inet.h>
#include <errno.h>
#include <getopt.h>
#include <liburing.h>
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
#include <sys/eventfd.h>
#include <sys/resource.h>
#include <sys/socket.h>
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

typedef enum { EV_ACCEPT, EV_RECV, EV_SEND, EV_WAKE } ev_type;

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
    /* shared with the worker */
    pthread_mutex_t lock;
    pthread_cond_t cv;
    buf out;
    atomic_bool closing;
    bool notified;      /* an output event is queued for the main thread */
} conn;

/* ------------------------------------------------------------ globals */

static tsdb *db;
static struct io_uring ring;
static int wake_fd = -1;
static volatile sig_atomic_t stopping;
static ev accept_ev = {EV_ACCEPT, NULL}, wake_ev = {EV_WAKE, NULL};
static conn *conns;
static size_t nconns, max_conns = 10000;

/* work queue: requests ready to run */
static pthread_mutex_t work_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t work_cv = PTHREAD_COND_INITIALIZER;
static req *work_head, *work_tail;
static bool work_stop;

/* events from workers to the main thread */
typedef struct note {
    conn *c;
    bool done; /* the request finished (else: output is waiting) */
} note;
static pthread_mutex_t note_lock = PTHREAD_MUTEX_INITIALIZER;
static note *notes;
static size_t nnotes, notes_cap;

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
    uint8_t hdr[13];
    put32(hdr, (uint32_t)body);
    put32(hdr + 4, id);
    hdr[8] = TSDB_OP_REPLY;
    memcpy(hdr + 9, &status, 4);
    pthread_mutex_lock(&c->lock);
    while (c->out.len > OUT_CAP && !atomic_load(&c->closing))
        pthread_cond_wait(&c->cv, &c->lock);
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

static void reply_buckets(conn *c, uint32_t id, const tsdb_bucket *b, size_t nb)
{
    uint8_t *rows = malloc(nb * TSDB_BUCKET_ROW + 1);
    if (!rows) {
        reply_status(c, id, TSDB_ERR_NOMEM);
        return;
    }
    for (size_t i = 0; i < nb; i++) {
        uint8_t *o = rows + i * TSDB_BUCKET_ROW;
        memcpy(o, &b[i].start, 8);
        memcpy(o + 8, &b[i].count, 8);
        memcpy(o + 16, &b[i].sum, 8);
        memcpy(o + 24, &b[i].min, 8);
        memcpy(o + 32, &b[i].max, 8);
        memcpy(o + 40, &b[i].isum, 8);
        memcpy(o + 48, &b[i].imin, 8);
        memcpy(o + 56, &b[i].imax, 8);
    }
    uint32_t n = (uint32_t)nb;
    reply(c, id, 0, &n, 4, rows, nb * TSDB_BUCKET_ROW, NULL, 0);
    free(rows);
}

static void handle(req *r)
{
    conn *c = r->c;
    const uint8_t *p = r->payload;
    size_t len = r->len;
    int rc = 0;
    switch (r->op) {
    case TSDB_OP_HELLO: {
        uint32_t v = TSDB_PROTOCOL_VERSION;
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
            tsdb_query_opts q = {p[25] ? TSDB_DESC : TSDB_ASC, (size_t)lim};
            rc = tsdb_query_group_ex(db, s, start, end, p[20] ? &g : NULL, &q, &b, &nb);
            free(zone);
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
        if (len < TSDB_VALUES_HEADER || p[8] > TSDB_STATE) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&n, p, 4);
        memcpy(&s, p + 4, 4);
        bool many = s == TSDB_MANY_SERIES;
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
    case TSDB_OP_QUERY_VALUES: {
        values_out o = {c, r->id, 0, 0, NULL, NULL, 0, false};
        if (len != 20 && len != 29) {
            values_flush(&o, true, TSDB_ERR_INVALID);
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
        rc = tsdb_query_values(db, s, start, end, &q, values_cb, &o);
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
        if (len < 44 || (memcpy(&zl, p + 42, 2), len != 44u + zl) || memchr(p + 44, 0, zl)) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        memcpy(&s, p, 4);
        memcpy(&start, p + 4, 8);
        memcpy(&end, p + 12, 8);
        memcpy(&interval, p + 20, 8);
        memcpy(&count, p + 29, 4);
        memcpy(&lim, p + 34, 8);
        tsdb_query_opts q = {p[33] ? TSDB_DESC : TSDB_ASC, (size_t)lim};
        tsdb_bucket *b = NULL;
        size_t nb = 0;
        if (p[28] == 0) {
            rc = tsdb_query_agg(db, s, start, end, interval, &b, &nb);
            if (rc == 0 && q.order == TSDB_DESC)
                for (size_t i = 0, j = nb ? nb - 1 : 0; i < j; i++, j--) {
                    tsdb_bucket x = b[i];
                    b[i] = b[j];
                    b[j] = x;
                }
            if (rc == 0 && q.limit && nb > q.limit)
                nb = q.limit;
        } else {
            char *zone = zl ? strndup((const char *)p + 44, zl) : NULL;
            tsdb_group g = {(tsdb_unit)p[28], count, zone};
            rc = zl && !zone ? TSDB_ERR_NOMEM : tsdb_query_group_ex(db, s, start, end, &g, &q, &b, &nb);
            free(zone);
        }
        if (rc == 0) {
            reply_buckets(c, r->id, b, nb);
            free(b);
            return;
        }
        free(b);
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
    reply_status(c, r->id, rc);
}

static void *worker_main(void *arg)
{
    (void)arg;
    for (;;) {
        pthread_mutex_lock(&work_lock);
        while (!work_head && !work_stop)
            pthread_cond_wait(&work_cv, &work_lock);
        if (!work_head) {
            pthread_mutex_unlock(&work_lock);
            return NULL;
        }
        req *r = work_head;
        work_head = r->next;
        if (!work_head)
            work_tail = NULL;
        pthread_mutex_unlock(&work_lock);
        handle(r); /* even for a closed connection: the client sent it, and replies are dropped */
        conn *c = r->c;
        free(r->payload);
        free(r);
        notify(c, true);
    }
}

/* ---------------------------------------------------------- event loop */

static struct io_uring_sqe *sqe_get(void)
{
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    if (!sqe) {
        io_uring_submit(&ring);
        sqe = io_uring_get_sqe(&ring);
    }
    return sqe;
}

static void submit_recv(conn *c)
{
    if (c->recv_inflight || c->closed)
        return;
    buf_shrink(&c->in);
    if (buf_reserve(&c->in, RECV_MIN_FREE) != 0)
        return;
    struct io_uring_sqe *sqe = sqe_get();
    io_uring_prep_recv(sqe, c->fd, c->in.p + c->in.len, c->in.cap - c->in.len, 0);
    io_uring_sqe_set_data(sqe, &c->recv_ev);
    c->recv_inflight = true;
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
    struct io_uring_sqe *sqe = sqe_get();
    io_uring_prep_send(sqe, c->fd, c->sending.p + c->sent, c->sending.len - c->sent, MSG_NOSIGNAL);
    io_uring_sqe_set_data(sqe, &c->send_ev);
    c->send_inflight = true;
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

static void conn_free(conn *c)
{
    nconns--;
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
    free(c->in.p);
    free(c->out.p);
    free(c->sending.p);
    pthread_mutex_destroy(&c->lock);
    pthread_cond_destroy(&c->cv);
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
        shutdown(c->fd, SHUT_RDWR); /* completes the outstanding recv */
    }
    dispatch(c);
    if (!c->busy && !c->q_head && !c->recv_inflight && !c->send_inflight) {
        close(c->fd);
        conn_free(c);
    }
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

static void on_accept(int fd)
{
    if (nconns >= max_conns) {
        close(fd); /* over the limit: the client sees the connection closed */
        return;
    }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    conn *c = calloc(1, sizeof *c);
    if (!c) {
        close(fd);
        return;
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
    pthread_mutex_unlock(&note_lock);
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

static void on_signal(int sig)
{
    (void)sig;
    stopping = 1;
    uint64_t one = 1;
    if (write(wake_fd, &one, 8) != 8) {
    }
}

static int listen_on(const char *addr, int port)
{
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
    return fd;
}

int main(int argc, char **argv)
{
    const char *dir = NULL, *addr = "127.0.0.1";
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
                                   {0, 0, 0, 0}};
    int ch;
    while ((ch = getopt_long(argc, argv, "", opts, NULL)) != -1) {
        switch (ch) {
        case 'd': dir = optarg; break;
        case 'l': addr = optarg; break;
        case 'p': port = atoi(optarg); break;
        case 'w': nworkers = atoi(optarg); break;
        case 's': o.wal_sync_ms = atoi(optarg); break;
        case 'F': o.fsync = 0; break;
        case 'r': o.read_only = 1; break;
        case 'f': o.follow = 1; break;
        case 'R': o.refresh_ms = atoi(optarg); break;
        case 'm': max_conns = (size_t)atol(optarg); break;
        case 'B': o.wal_max_batch_rows = (size_t)atol(optarg); break;
        case 'P': o.partition_ms = atoll(optarg); break; /* only when the database is created */
        default: goto usage;
        }
    }
    if (!dir || nworkers < 1) {
    usage:
        fprintf(stderr, "usage: tsdbd --dir DIR [--listen ADDR] [--port %d] [--workers N] [--wal-sync-ms MS]\n"
                        "             [--no-fsync] [--read-only | --follow [--refresh-ms 100]] [--max-connections 10000]\n",
                TSDB_DEFAULT_PORT);
        return 2;
    }
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
    int lfd = listen_on(addr, port);
    if (lfd < 0)
        return 1;
    wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    struct io_uring_params params = {.flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN};
    if (io_uring_queue_init_params(4096, &ring, &params) < 0) {
        params.flags = 0; /* older kernels */
        if ((rc = io_uring_queue_init_params(4096, &ring, &params)) < 0) {
            fprintf(stderr, "tsdbd: io_uring: %s\n", strerror(-rc));
            return 1;
        }
    }
    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    signal(SIGPIPE, SIG_IGN);
    pthread_t *workers = calloc((size_t)nworkers, sizeof *workers);
    for (int i = 0; i < nworkers; i++)
        pthread_create(&workers[i], NULL, worker_main, NULL);

    struct io_uring_sqe *sqe = sqe_get();
    io_uring_prep_multishot_accept(sqe, lfd, NULL, NULL, SOCK_CLOEXEC);
    io_uring_sqe_set_data(sqe, &accept_ev);
    sqe = sqe_get();
    io_uring_prep_poll_multishot(sqe, wake_fd, POLLIN);
    io_uring_sqe_set_data(sqe, &wake_ev);
    fprintf(stderr, "tsdbd: serving %s%s on %s:%d, %d workers, up to %zu connections, simd %s\n", dir,
            o.follow ? " (following its writer)" : o.read_only ? " (read only)" : "", addr, port, nworkers,
            max_conns, tsdb_simd_level());

    while (!stopping) {
        io_uring_submit_and_wait(&ring, 1);
        struct io_uring_cqe *cqe;
        unsigned head, seen = 0;
        io_uring_for_each_cqe(&ring, head, cqe)
        {
            seen++;
            ev *e = io_uring_cqe_get_data(cqe);
            int res = cqe->res;
            switch (e->type) {
            case EV_ACCEPT:
                if (res >= 0)
                    on_accept(res);
                if (!(cqe->flags & IORING_CQE_F_MORE)) { /* re-arm */
                    struct io_uring_sqe *s2 = sqe_get();
                    io_uring_prep_multishot_accept(s2, lfd, NULL, NULL, SOCK_CLOEXEC);
                    io_uring_sqe_set_data(s2, &accept_ev);
                }
                break;
            case EV_WAKE:
                on_wake();
                if (!(cqe->flags & IORING_CQE_F_MORE)) {
                    struct io_uring_sqe *s2 = sqe_get();
                    io_uring_prep_poll_multishot(s2, wake_fd, POLLIN);
                    io_uring_sqe_set_data(s2, &wake_ev);
                }
                break;
            case EV_RECV: {
                conn *c = e->c;
                c->recv_inflight = false;
                if (res == 0 && !c->closed) { /* end of stream: what was received still gets its replies */
                    if (getenv("TSDBD_DEBUG"))
                        fprintf(stderr, "tsdbd: connection %d: recv end\n", c->fd);
                    c->eof = true;
                    finish_if_drained(c);
                    break;
                }
                if (res < 0 || c->closed) {
                    if (getenv("TSDBD_DEBUG") && !c->closed)
                        fprintf(stderr, "tsdbd: connection %d: recv %s\n", c->fd, strerror(-res));
                    conn_close(c);
                    break;
                }
                c->in.len += (size_t)res;
                if (!parse(c)) {
                    if (getenv("TSDBD_DEBUG"))
                        fprintf(stderr, "tsdbd: connection %d: protocol error\n", c->fd);
                    conn_close(c);
                    break;
                }
                dispatch(c);
                submit_recv(c);
                break;
            }
            case EV_SEND: {
                conn *c = e->c;
                c->send_inflight = false;
                if (res < 0 || c->closed) {
                    if (getenv("TSDBD_DEBUG") && res < 0)
                        fprintf(stderr, "tsdbd: connection %d: send %s\n", c->fd, strerror(-res));
                    conn_close(c);
                    break;
                }
                c->sent += (size_t)res;
                submit_send(c);
                finish_if_drained(c);
                break;
            }
            }
        }
        io_uring_cq_advance(&ring, seen);
    }

    fprintf(stderr, "tsdbd: shutting down\n");
    /* The pending multishot accept holds its own reference to the socket, and the
     * ring is torn down asynchronously, after the process has exited: close() alone
     * leaves it listening for a moment, so a restarted server cannot bind and clients
     * connect to a listener nobody accepts from. shutdown() stops listening now. */
    shutdown(lfd, SHUT_RDWR);
    close(lfd);
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
    io_uring_queue_exit(&ring);
    while (conns) {
        close(conns->fd);
        conn_free(conns);
    }
    free(notes);
    rc = tsdb_close(db);
    if (rc)
        fprintf(stderr, "tsdbd: close: %s\n", tsdb_strerror(rc));
    return rc ? 1 : 0;
}
