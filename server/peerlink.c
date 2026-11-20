/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Data links (peerlink.h has the design and the wire). The file has four parts: bytes and hashes (a growing buffer,
 * the streaming xxh64, HMAC with the pads absorbed once per key), the frame codec (pl_msg_decode, pl_reader: what a
 * fuzzer reaches), the handshake (blocking, with deadlines, on the link's own thread) and the link threads.
 *
 * One thread runs one connection of one link: it owns the socket, the reader, the write buffer and the state of the
 * bulk file in flight in each direction. The driver's threads only touch the link's queue and job list (under
 * `plink.mu`) and the eventfd that wakes the thread. A frame queued with plink_send is copied; compressing, tagging
 * and writing happen on the link's thread, so a caller never waits for the CPU cost of its own frame.
 *
 * Bulk files (sender): BEGIN{file, total, meta}, the receiver answers RESUME{offset} (what it already holds; the
 * prefix is read back and hashed on both sides, so the end-to-end xxh64 covers resumed bytes too), CHUNKs from the
 * offset on at the node's token bucket rate, END{total, xxh64}, the receiver answers DONE{status}. A connection lost
 * in between puts the job back at the head of the list: the next connection starts at BEGIN again and the receiver
 * says where it is. Receiver: chunks must arrive in order and within the length BEGIN gave; anything else ends the
 * link.
 */
#define _GNU_SOURCE
#include "peerlink.h"

#include "tsdb_crypto.h"

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>
#include <zstd.h>

#include "../src/compat.h" /* macOS: accept4, SOCK_CLOEXEC; nothing on Linux */

int peerlink_mutant;

#define TAG 32
#define MS 1000000ull
#define HS_FRAME_MAX 512
#define HS_TR_MAX (HS_FRAME_MAX + 128)
#define WIRE_MAX (PL_FRAME_MAX + 128) /* the longest frame after its length field: type, header, payload, tag */
#define WB_HIGH (1u << 20)            /* the link thread encodes no more while this much waits to be written */
#define READ_BUF (256u << 10)
#define MAX_JOBS 64
#define MAX_HAND 64 /* connections that may be in their handshake at once, beyond the links' own */
#define COMPRESS_MIN 128
enum { F_HELLO = 1, F_HELLO_ACK = 2, F_AUTH = 3 };

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static void oom(void)
{
    fputs("tsdbd: peerlink: out of memory: stopping (fail-stop)\n", stderr);
    _exit(1);
}

/* ------------------------------------------------------------------ bytes */

struct buf {
    uint8_t *p;
    size_t off, len, cap; /* data is p[off, len) */
};

static void buf_reserve(struct buf *b, size_t extra)
{
    if (b->off && b->off == b->len)
        b->off = b->len = 0;
    if (b->len + extra <= b->cap)
        return;
    if (b->off) {
        memmove(b->p, b->p + b->off, b->len - b->off);
        b->len -= b->off;
        b->off = 0;
        if (b->len + extra <= b->cap)
            return;
    }
    size_t cap = b->cap ? b->cap : 4096;
    while (cap < b->len + extra)
        cap *= 2;
    uint8_t *p = realloc(b->p, cap);
    if (!p)
        oom();
    b->p = p;
    b->cap = cap;
}

static void buf_free(struct buf *b)
{
    free(b->p);
    *b = (struct buf){0};
}

static void put(struct buf *b, const void *d, size_t n)
{
    if (!n)
        return;
    buf_reserve(b, n);
    memcpy(b->p + b->len, d, n);
    b->len += n;
}

static void put8(struct buf *b, uint8_t v) { put(b, &v, 1); }

static void put32(struct buf *b, uint32_t v)
{
    uint8_t x[4] = {(uint8_t)v, (uint8_t)(v >> 8), (uint8_t)(v >> 16), (uint8_t)(v >> 24)};
    put(b, x, 4);
}

static void put64(struct buf *b, uint64_t v)
{
    put32(b, (uint32_t)v);
    put32(b, (uint32_t)(v >> 32));
}

static uint32_t le32(const uint8_t *p)
{
    return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24;
}

static uint64_t le64(const uint8_t *p) { return (uint64_t)le32(p) | (uint64_t)le32(p + 4) << 32; }

/* ------------------------------------------------------------------ xxh64, streaming */

#define XP1 11400714785074694791ull
#define XP2 14029467366897019727ull
#define XP3 1609587929392839161ull
#define XP4 9650029242287828579ull
#define XP5 2870177450012600261ull

static uint64_t rotl64(uint64_t x, int r) { return (x << r) | (x >> (64 - r)); }
static uint64_t xround(uint64_t acc, uint64_t in) { return rotl64(acc + in * XP2, 31) * XP1; }
static uint64_t xmerge(uint64_t h, uint64_t v) { return (h ^ xround(0, v)) * XP1 + XP4; }

void pl_xxh_init(struct pl_xxh *s)
{
    memset(s, 0, sizeof *s);
    s->v[0] = XP1 + XP2;
    s->v[1] = XP2;
    s->v[2] = 0;
    s->v[3] = 0 - XP1;
}

void pl_xxh_update(struct pl_xxh *s, const void *data, size_t n)
{
    const uint8_t *p = data;
    s->total += n;
    if (s->nbuf) {
        size_t take = 32 - s->nbuf < n ? 32 - s->nbuf : n;
        memcpy(s->buf + s->nbuf, p, take);
        s->nbuf += take;
        p += take;
        n -= take;
        if (s->nbuf < 32)
            return;
        for (int i = 0; i < 4; i++)
            s->v[i] = xround(s->v[i], le64(s->buf + 8 * i));
        s->nbuf = 0;
    }
    while (n >= 32) {
        for (int i = 0; i < 4; i++)
            s->v[i] = xround(s->v[i], le64(p + 8 * i));
        p += 32;
        n -= 32;
    }
    if (n) {
        memcpy(s->buf, p, n);
        s->nbuf = n;
    }
}

uint64_t pl_xxh_final(const struct pl_xxh *s)
{
    uint64_t h;
    if (s->total >= 32) {
        h = rotl64(s->v[0], 1) + rotl64(s->v[1], 7) + rotl64(s->v[2], 12) + rotl64(s->v[3], 18);
        for (int i = 0; i < 4; i++)
            h = xmerge(h, s->v[i]);
    } else {
        h = s->v[2] + XP5;
    }
    h += s->total;
    const uint8_t *p = s->buf, *end = s->buf + s->nbuf;
    for (; p + 8 <= end; p += 8)
        h = rotl64(h ^ xround(0, le64(p)), 27) * XP1 + XP4;
    if (p + 4 <= end) {
        h = rotl64(h ^ ((uint64_t)le32(p) * XP1), 23) * XP2 + XP3;
        p += 4;
    }
    for (; p < end; p++)
        h = rotl64(h ^ (*p * XP5), 11) * XP1;
    h ^= h >> 33;
    h *= XP2;
    h ^= h >> 29;
    h *= XP3;
    h ^= h >> 32;
    return h;
}

/* ------------------------------------------------------------------ HMAC */

/* HMAC-SHA256 with a key of at most 64 bytes: the two pad blocks are absorbed once, and a tag copies the states */
struct hk {
    tsdb_sha256 in, out;
};

static void hk_init(struct hk *k, const uint8_t *key, size_t klen)
{
    uint8_t kb[64] = {0}, pad[64];
    if (klen > 64)
        tsdb_sha256_buf(key, klen, kb);
    else
        memcpy(kb, key, klen);
    for (int i = 0; i < 64; i++)
        pad[i] = kb[i] ^ 0x36;
    tsdb_sha256_init(&k->in);
    tsdb_sha256_update(&k->in, pad, 64);
    for (int i = 0; i < 64; i++)
        pad[i] = kb[i] ^ 0x5c;
    tsdb_sha256_init(&k->out);
    tsdb_sha256_update(&k->out, pad, 64);
    tsdb_wipe(kb, sizeof kb);
    tsdb_wipe(pad, sizeof pad);
}

static void hk_mac(const struct hk *k, const void *a, size_t an, const void *b, size_t bn, const void *c, size_t cn,
                   const void *d, size_t dn, uint8_t out[32])
{
    tsdb_sha256 x = k->in;
    uint8_t ih[32];
    tsdb_sha256_update(&x, a, an);
    tsdb_sha256_update(&x, b, bn);
    tsdb_sha256_update(&x, c, cn);
    tsdb_sha256_update(&x, d, dn);
    tsdb_sha256_final(&x, ih);
    x = k->out;
    tsdb_sha256_update(&x, ih, 32);
    tsdb_sha256_final(&x, out);
}

/* the tag of one frame: its sequence number in that direction, its type and its payload (header and body) */
static void frame_tag(const struct hk *k, uint64_t seq, uint8_t type, const void *h, size_t hn, const void *p,
                      size_t pn, uint8_t out[32])
{
    uint8_t s[9];
    for (int i = 0; i < 8; i++)
        s[i] = (uint8_t)(seq >> (8 * i));
    s[8] = type;
    hk_mac(k, s, 9, h, hn, p, pn, "", 0, out);
}

/* the handshake's MAC and the link keys: HMAC(secret, label | transcript) */
static void hs_mac(const uint8_t *secret, size_t slen, const char *label, const uint8_t *tr, size_t trlen,
                   uint8_t out[32])
{
    struct hk k;
    hk_init(&k, secret, slen);
    hk_mac(&k, label, strlen(label), tr, trlen, "", 0, "", 0, out);
    tsdb_wipe(&k, sizeof k);
}

/* ------------------------------------------------------------------ the frame codec */

int pl_msg_decode(uint8_t type, const uint8_t *p, size_t n, struct pl_msg *m, uint8_t **own)
{
    memset(m, 0, sizeof *m);
    if (own)
        *own = NULL;
    if (type & 0x80) {
        type &= 0x7f;
        if (type == 0 || type > PL_USER_TYPE_MAX || n < 4 + 1 || !own)
            return -1;
        uint32_t raw = le32(p);
        if (raw == 0 || raw > PL_FRAME_MAX)
            return -1;
        uint8_t *d = malloc(raw);
        if (!d)
            return -1;
        size_t got = ZSTD_decompress(d, raw, p + 4, n - 4);
        if (ZSTD_isError(got) || (got != raw && peerlink_mutant != PL_MUT_ZSTDSIZE)) {
            free(d);
            return -1;
        }
        *own = d;
        p = d;
        n = raw;
    }
    m->type = type;
    m->p = p;
    m->n = n;
    if (type >= 1 && type <= PL_USER_TYPE_MAX)
        return n <= PL_FRAME_MAX ? 0 : -1;
    switch (type) {
    case PLT_BEGIN:
        if (n < 20)
            return -1;
        m->file = le64(p);
        m->a = le64(p + 8);
        m->b = le32(p + 16);
        if (m->b > PL_META_MAX || m->b != n - 20)
            return -1;
        m->data = p + 20;
        m->data_len = n - 20;
        return 0;
    case PLT_RESUME:
        if (n != 20)
            return -1;
        m->file = le64(p);
        m->status = (int32_t)le32(p + 8);
        m->a = le64(p + 12);
        return 0;
    case PLT_CHUNK:
        if (n <= 16 || n - 16 > PL_FRAME_MAX)
            return -1;
        m->file = le64(p);
        m->a = le64(p + 8);
        m->data = p + 16;
        m->data_len = n - 16;
        return 0;
    case PLT_END:
        if (n != 24)
            return -1;
        m->file = le64(p);
        m->a = le64(p + 8);
        m->b = le64(p + 16);
        return 0;
    case PLT_DONE:
        if (n != 12)
            return -1;
        m->file = le64(p);
        m->status = (int32_t)le32(p + 8);
        return 0;
    case PLT_PING:
    case PLT_PONG:
        return n == 0 ? 0 : -1;
    default:
        return -1;
    }
}

/* [u32 len][type][hdr][payload][tag] appended to b; `hdr` is the compression header or a control frame's fields */
static void frame_put(struct buf *b, const struct hk *k, uint64_t *seq, uint8_t type, const void *h, size_t hn,
                      const void *p, size_t pn)
{
    uint8_t t[TAG];
    frame_tag(k, (*seq)++, type, h, hn, p, pn, t);
    put32(b, (uint32_t)(1 + hn + pn + TAG));
    put8(b, type);
    put(b, h, hn);
    put(b, p, pn);
    put(b, t, TAG);
}

/* a driver frame, compressed when it pays; `zc` is the thread's zstd context (NULL: a one-off) */
static void frame_user(struct buf *b, const struct hk *k, uint64_t *seq, ZSTD_CCtx *zc, int compress, uint8_t type,
                       const void *p, size_t n)
{
    if (compress && n >= COMPRESS_MIN) {
        size_t bound = ZSTD_compressBound(n);
        uint8_t *z = malloc(bound);
        if (z) {
            size_t zn = zc ? ZSTD_compressCCtx(zc, z, bound, p, n, 1) : ZSTD_compress(z, bound, p, n, 1);
            if (!ZSTD_isError(zn) && zn + 4 + n / 8 < n) {
                uint8_t h[4] = {(uint8_t)n, (uint8_t)(n >> 8), (uint8_t)(n >> 16), (uint8_t)(n >> 24)};
                frame_put(b, k, seq, type | 0x80, h, 4, z, zn);
                free(z);
                return;
            }
            free(z);
        }
    }
    frame_put(b, k, seq, type, "", 0, p, n);
}

size_t pl_frame_encode(const uint8_t key[32], uint64_t *seq, int compress, uint8_t type, const void *p, size_t n,
                       uint8_t **out)
{
    struct hk k;
    struct buf b = {0};
    hk_init(&k, key, 32);
    frame_user(&b, &k, seq, NULL, compress, type, p, n);
    *out = b.p;
    return b.len;
}

struct pl_reader {
    struct hk key;
    int skip, bad;
    uint64_t seq;
    struct buf rb;
};

struct pl_reader *pl_reader_new(const uint8_t key[32], int skip_tag)
{
    struct pl_reader *r = calloc(1, sizeof *r);
    if (!r)
        return NULL;
    hk_init(&r->key, key, 32);
    r->skip = skip_tag;
    return r;
}

void pl_reader_free(struct pl_reader *r)
{
    if (!r)
        return;
    buf_free(&r->rb);
    free(r);
}

int pl_reader_feed(struct pl_reader *r, const uint8_t *p, size_t n, int (*fn)(void *arg, const struct pl_msg *m),
                   void *arg)
{
    if (r->bad)
        return -1;
    put(&r->rb, p, n);
    while (r->rb.len - r->rb.off >= 4) {
        const uint8_t *at = r->rb.p + r->rb.off;
        uint32_t len = le32(at);
        if (len < 1 + TAG || len > WIRE_MAX) {
            r->bad = 1;
            break;
        }
        if (r->rb.len - r->rb.off - 4 < len)
            break;
        const uint8_t *f = at + 4;
        size_t body = len - TAG;
        uint8_t t[TAG];
        frame_tag(&r->key, r->seq++, f[0], "", 0, f + 1, body - 1, t);
        if (!r->skip && peerlink_mutant != PL_MUT_NOTAG && !tsdb_ct_equal(t, f + body, TAG)) {
            r->bad = 1;
            break;
        }
        struct pl_msg m;
        uint8_t *own = NULL;
        if (pl_msg_decode(f[0], f + 1, body - 1, &m, &own) != 0) {
            r->bad = 1;
            break;
        }
        int rc = fn ? fn(arg, &m) : 0;
        free(own);
        if (rc < 0) {
            r->bad = 1;
            break;
        }
        r->rb.off += 4 + (size_t)len;
    }
    if (r->bad) {
        buf_free(&r->rb);
        return -1;
    }
    return 0;
}

/* ------------------------------------------------------------------ the links */

struct qf {
    struct qf *next;
    size_t n;
    uint8_t type, tracked; /* tracked: on_sent is owed for it (plink_send_tracked) */
    uint64_t token, deadline;
    uint8_t p[];
};

struct jobq {
    struct jobq *next;
    struct pl_job j;
    uint8_t meta[PL_META_MAX];
    int attempts;
};

struct plink {
    struct peerlink *pl;
    struct plink *next;
    int kind, dialed;
    uint32_t want_id;
    char addr[160];
    _Atomic uint32_t peer;
    void *arg;
    int evfd, evw;
    pthread_t thr; /* a dialed link's thread (accepted ones run on detached threads, counted in pl->helpers) */
    int thr_valid;
    /* under pl->mu: accepted links. adopt_seq bumps when a connection that passed the handshake claims the link
     * (never before: a peer that fails AUTH replaces nothing); the connection running it, and any older one waiting
     * for it to leave, sees the bump and leaves. running: a connection's thread is inside conn_run. */
    uint64_t adopt_seq;
    int running;

    pthread_mutex_t mu;
    pthread_cond_t cv;
    int up;   /* a connection is established (from before on_link(up) runs) */
    int open; /* the driver may send: set once on_link(up) has returned (that thread may send from inside it) */
    pthread_t io_thr;
    int io_set;
    uint64_t gen;
    struct qf *qh, *qt;
    size_t qbytes;
    struct jobq *jh, *jt;
    int njobs;
    uint64_t qdl_min;   /* the earliest deadline of a tracked frame in the queue (0: none; may be early, never late) */
    uint64_t reset_gen; /* plink_reset: the connection to end */
    _Atomic int stopped; /* plink_stop */
    _Atomic int paused;  /* plink_pause: the link's thread reads nothing from the socket meanwhile */
};

struct peerlink {
    struct pl_cfg cfg;
    uint8_t secret[1024];
    size_t secret_len;
    pthread_mutex_t mu;
    pthread_cond_t acv; /* on mu: an accepted link's claim changed, or the peerlink is closing */
    struct plink *links;
    _Atomic int closing; /* set under mu */
    _Atomic int helpers; /* threads that are not a dialed link's: the listener's helpers, adopt threads */
    int nhand;           /* under mu: adopt threads still in their handshake (bounded: MAX_HAND) */
    pthread_t lthr;
    int lfd, lthr_valid;
    pthread_mutex_t bmu; /* the bulk token bucket */
    uint64_t vt;
    _Atomic uint64_t rate;
    struct pl_stats st;
};

#define ST(pl, f, n) __atomic_add_fetch(&(pl)->st.f, (n), __ATOMIC_RELAXED)

/* the wake-up of a link's thread: a nonblocking pipe (evfd the read end), portable where eventfd is not */
static void wake(struct plink *l)
{
    char one = 1;
    if (write(l->evw, &one, 1) < 0) {
        /* EAGAIN: the pipe is full, so it is signalled */
    }
}

static void drain_evfd(struct plink *l)
{
    char v[64];
    while (read(l->evfd, v, sizeof v) > 0)
        ;
}

static struct plink *link_new(struct peerlink *pl, int kind, int dialed)
{
    struct plink *l = calloc(1, sizeof *l);
    if (!l)
        return NULL;
    l->pl = pl;
    l->kind = kind;
    l->dialed = dialed;
    int pp[2] = {-1, -1};
    if (pipe(pp) != 0) {
        free(l);
        return NULL;
    }
    for (int i = 0; i < 2; i++) {
        fcntl(pp[i], F_SETFL, fcntl(pp[i], F_GETFL) | O_NONBLOCK);
        fcntl(pp[i], F_SETFD, FD_CLOEXEC);
    }
    l->evfd = pp[0];
    l->evw = pp[1];
    pthread_condattr_t ca;
    pthread_condattr_init(&ca);
    pthread_condattr_setclock(&ca, CLOCK_MONOTONIC);
    pthread_cond_init(&l->cv, &ca);
    pthread_condattr_destroy(&ca);
    pthread_mutex_init(&l->mu, NULL);
    return l;
}

uint32_t plink_peer(const struct plink *l) { return atomic_load(&((struct plink *)l)->peer); }
int plink_kind(const struct plink *l) { return l->kind; }
void plink_set_arg(struct plink *l, void *arg) { l->arg = arg; }
void *plink_arg(const struct plink *l) { return l->arg; }

int plink_up(const struct plink *l)
{
    struct plink *m = (struct plink *)l;
    pthread_mutex_lock(&m->mu);
    int up = m->up;
    pthread_mutex_unlock(&m->mu);
    return up;
}

uint64_t plink_gen(const struct plink *l)
{
    struct plink *m = (struct plink *)l;
    pthread_mutex_lock(&m->mu);
    uint64_t g = m->gen;
    pthread_mutex_unlock(&m->mu);
    return g;
}

size_t plink_queued(struct plink *l)
{
    pthread_mutex_lock(&l->mu);
    size_t n = l->qbytes;
    pthread_mutex_unlock(&l->mu);
    return n;
}

static int send_impl2(struct plink *l, uint64_t gen, uint8_t type, const void *hdr, size_t hn, const void *p,
                      size_t n, int timeout_ms, int tracked, uint64_t token, uint64_t deadline)
{
    struct peerlink *pl = l->pl;
    if (type == 0 || type > PL_USER_TYPE_MAX || (!p && n) || (!hdr && hn))
        return PL_E_INVALID;
    if (n > PL_FRAME_MAX || hn > PL_FRAME_MAX - n)
        return PL_E_TOOBIG;
    struct qf *f = malloc(sizeof *f + hn + n);
    if (!f)
        oom();
    f->next = NULL;
    f->type = type;
    f->tracked = (uint8_t)tracked;
    f->token = token;
    f->deadline = tracked ? deadline : 0;
    if (hn)
        memcpy(f->p, hdr, hn);
    if (n)
        memcpy(f->p + hn, p, n);
    n += hn;
    f->n = n;
    size_t cost = n + 48, cap = pl->cfg.queue_bytes ? pl->cfg.queue_bytes : PL_QUEUE_BYTES;
    struct timespec dl;
    clock_gettime(CLOCK_MONOTONIC, &dl);
    dl.tv_sec += timeout_ms / 1000;
    dl.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
    if (dl.tv_nsec >= 1000000000L) {
        dl.tv_sec++;
        dl.tv_nsec -= 1000000000L;
    }
    int rc = PL_OK;
    pthread_mutex_lock(&l->mu);
    for (;;) {
        if (atomic_load(&pl->closing) || atomic_load(&l->stopped)) {
            rc = atomic_load(&pl->closing) ? PL_E_CLOSED : PL_E_DOWN;
            break;
        }
        /* sends open when on_link(up) has returned; the link's own thread may send from inside it */
        if (!l->open && !(l->up && l->io_set && pthread_equal(l->io_thr, pthread_self()))) {
            rc = PL_E_DOWN;
            break;
        }
        if (gen && l->gen != gen) {
            rc = PL_E_DOWN;
            break;
        }
        /* a frame larger than the whole queue still goes in when the queue is empty */
        if (peerlink_mutant == PL_MUT_NOBACKPRESSURE || l->qbytes == 0 || l->qbytes + cost <= cap)
            break;
        if (timeout_ms <= 0 || pthread_cond_timedwait(&l->cv, &l->mu, &dl) != 0) {
            rc = PL_E_FULL;
            break;
        }
    }
    if (rc == PL_OK) {
        if (l->qt)
            l->qt->next = f;
        else
            l->qh = f;
        l->qt = f;
        l->qbytes += cost;
        if (f->deadline && (!l->qdl_min || f->deadline < l->qdl_min))
            l->qdl_min = f->deadline;
    }
    pthread_mutex_unlock(&l->mu);
    if (rc != PL_OK) {
        free(f);
        return rc;
    }
    wake(l);
    return PL_OK;
}

static int send_impl(struct plink *l, uint64_t gen, uint8_t type, const void *p, size_t n, int timeout_ms)
{
    return send_impl2(l, gen, type, NULL, 0, p, n, timeout_ms, 0, 0, 0);
}

int plink_send(struct plink *l, uint8_t type, const void *p, size_t n, int timeout_ms)
{
    return send_impl(l, 0, type, p, n, timeout_ms);
}

int plink_sendv(struct plink *l, uint64_t gen, uint8_t type, const void *hdr, size_t hn, const void *p, size_t n,
                int timeout_ms, int tracked, uint64_t token, uint64_t deadline_ns)
{
    return send_impl2(l, gen, type, hdr, hn, p, n, timeout_ms, tracked, token, deadline_ns);
}

void plink_reset(struct plink *l)
{
    pthread_mutex_lock(&l->mu);
    if (l->up)
        l->reset_gen = l->gen;
    pthread_mutex_unlock(&l->mu);
    wake(l);
}

void plink_pause(struct plink *l, int paused)
{
    atomic_store(&l->paused, paused ? 1 : 0);
    wake(l);
}

int plink_unqueue(struct plink *l, uint64_t token)
{
    int found = 0;
    pthread_mutex_lock(&l->mu);
    for (struct qf **pp = &l->qh, *prev = NULL; *pp; prev = *pp, pp = &(*pp)->next) {
        struct qf *f = *pp;
        if (!f->tracked || f->token != token)
            continue;
        *pp = f->next;
        if (l->qt == f)
            l->qt = prev;
        l->qbytes -= f->n + 48;
        pthread_cond_broadcast(&l->cv);
        free(f); /* qdl_min may now be early, which it is allowed to be */
        found = 1;
        break;
    }
    pthread_mutex_unlock(&l->mu);
    return found;
}

void plink_stop(struct plink *l)
{
    if (!l->dialed)
        return;
    atomic_store(&l->stopped, 1);
    wake(l);
}

int plink_test_ctl(struct plink *l, uint8_t type, const void *p, size_t n)
{
    struct qf *f = malloc(sizeof *f + n);
    if (!f)
        oom();
    f->next = NULL;
    f->n = n;
    f->type = type;
    if (n)
        memcpy(f->p, p, n);
    pthread_mutex_lock(&l->mu);
    if (!l->open) {
        pthread_mutex_unlock(&l->mu);
        free(f);
        return PL_E_DOWN;
    }
    if (l->qt)
        l->qt->next = f;
    else
        l->qh = f;
    l->qt = f;
    l->qbytes += n + 48;
    pthread_mutex_unlock(&l->mu);
    wake(l);
    return PL_OK;
}

int plink_send_gen(struct plink *l, uint64_t gen, uint8_t type, const void *p, size_t n, int timeout_ms)
{
    return gen ? send_impl(l, gen, type, p, n, timeout_ms) : PL_E_INVALID;
}

int plink_bulk_send(struct plink *l, const struct pl_job *job)
{
    if (!job || !job->done || job->fd < 0 || job->meta_len > PL_META_MAX || (job->meta_len && !job->meta))
        return PL_E_INVALID;
    struct jobq *q = calloc(1, sizeof *q);
    if (!q)
        oom();
    q->j = *job;
    if (job->meta_len)
        memcpy(q->meta, job->meta, job->meta_len);
    q->j.meta = q->meta;
    if (!q->j.max_attempts)
        q->j.max_attempts = 5;
    int rc = PL_OK;
    pthread_mutex_lock(&l->mu);
    if (atomic_load(&l->pl->closing))
        rc = PL_E_CLOSED;
    else if (l->njobs >= MAX_JOBS)
        rc = PL_E_FULL;
    else {
        if (l->jt)
            l->jt->next = q;
        else
            l->jh = q;
        l->jt = q;
        l->njobs++;
    }
    pthread_mutex_unlock(&l->mu);
    if (rc != PL_OK) {
        free(q);
        return rc;
    }
    wake(l);
    return PL_OK;
}

/* ---- the token bucket: 0 when `n` bytes may go now (and are counted), else the nanoseconds to wait */
static uint64_t bucket_take(struct peerlink *pl, size_t n)
{
    uint64_t rate = atomic_load(&pl->rate);
    if (rate == PL_RATE_UNLIMITED || peerlink_mutant == PL_MUT_NOTHROTTLE)
        return 0;
    if (!rate)
        rate = PL_BULK_RATE_DEFAULT;
    uint64_t now = now_ns(), burst = 50 * MS, cost = (uint64_t)((double)n * 1e9 / (double)rate), wait = 0;
    pthread_mutex_lock(&pl->bmu);
    if (pl->vt <= now) {
        uint64_t floor = now > burst ? now - burst : 0;
        pl->vt = (pl->vt > floor ? pl->vt : floor) + cost;
    } else {
        wait = pl->vt - now;
    }
    pthread_mutex_unlock(&pl->bmu);
    return wait;
}

void peerlink_set_bulk_rate(struct peerlink *pl, uint64_t bytes_per_s) { atomic_store(&pl->rate, bytes_per_s); }

/* ---- sockets */

static int write_dl(int fd, const uint8_t *p, size_t n, uint64_t deadline)
{
    while (n) {
        ssize_t w = write(fd, p, n);
        if (w > 0) {
            p += w;
            n -= (size_t)w;
            continue;
        }
        if (w < 0 && errno == EINTR)
            continue;
        if (w < 0 && errno != EAGAIN)
            return -1;
        uint64_t now = now_ns();
        if (now >= deadline)
            return -1;
        struct pollfd pf = {fd, POLLOUT, 0};
        if (poll(&pf, 1, (int)((deadline - now) / MS) + 1) < 0 && errno != EINTR)
            return -1;
    }
    return 0;
}

static int read_dl(struct peerlink *pl, int fd, uint8_t *p, size_t n, uint64_t deadline)
{
    while (n) {
        ssize_t r = read(fd, p, n);
        if (r > 0) {
            p += r;
            n -= (size_t)r;
            continue;
        }
        if (r == 0)
            return -1;
        if (errno == EINTR)
            continue;
        if (errno != EAGAIN)
            return -1;
        uint64_t now = now_ns();
        if (now >= deadline || atomic_load(&pl->closing))
            return -1;
        struct pollfd pf = {fd, POLLIN, 0};
        uint64_t slice = (deadline - now) / MS + 1;
        if (poll(&pf, 1, (int)(slice > 100 ? 100 : slice)) < 0 && errno != EINTR)
            return -1;
    }
    return 0;
}

/* one handshake frame: its type and body (at most HS_FRAME_MAX) */
static int read_hs(struct peerlink *pl, int fd, uint64_t deadline, uint8_t *type, uint8_t *body, size_t *len)
{
    uint8_t h[5];
    if (read_dl(pl, fd, h, 4, deadline) != 0)
        return -1;
    uint32_t n = le32(h);
    if (n < 1 || n > HS_FRAME_MAX || read_dl(pl, fd, h + 4, 1, deadline) != 0)
        return -1;
    if (read_dl(pl, fd, body, n - 1, deadline) != 0)
        return -1;
    *type = h[4];
    *len = n - 1;
    return 0;
}

static int send_hs(int fd, uint8_t type, const uint8_t *body, size_t n, uint64_t deadline)
{
    uint8_t h[5] = {(uint8_t)(n + 1), (uint8_t)((n + 1) >> 8), 0, 0, type};
    uint8_t fr[HS_FRAME_MAX + 8];
    memcpy(fr, h, 5);
    memcpy(fr + 5, body, n);
    return write_dl(fd, fr, 5 + n, deadline);
}

static int dial_fd(struct peerlink *pl, const char *addr, uint64_t deadline)
{
    char host[128], port[16];
    const char *colon = strrchr(addr, ':');
    if (!colon || !colon[1] || (size_t)(colon - addr) >= sizeof host || strlen(colon + 1) >= sizeof port)
        return -1;
    size_t hl = (size_t)(colon - addr);
    memcpy(host, addr, hl);
    host[hl] = 0;
    if (hl >= 2 && host[0] == '[' && host[hl - 1] == ']') {
        memmove(host, host + 1, hl - 2);
        host[hl - 2] = 0;
    }
    snprintf(port, sizeof port, "%s", colon + 1);
    struct addrinfo hints = {.ai_family = AF_UNSPEC, .ai_socktype = SOCK_STREAM, .ai_flags = AI_NUMERICSERV};
    struct addrinfo *ai = NULL;
    if (getaddrinfo(host, port, &hints, &ai) != 0 || !ai)
        return -1;
    int fd = socket(ai->ai_family, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
    if (fd < 0) {
        freeaddrinfo(ai);
        return -1;
    }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    int rc = connect(fd, ai->ai_addr, ai->ai_addrlen);
    freeaddrinfo(ai);
    if (rc != 0 && errno != EINPROGRESS) {
        close(fd);
        return -1;
    }
    while (rc != 0) {
        uint64_t now = now_ns();
        if (now >= deadline || atomic_load(&pl->closing)) {
            close(fd);
            return -1;
        }
        struct pollfd pf = {fd, POLLOUT, 0};
        uint64_t slice = (deadline - now) / MS + 1;
        int r = poll(&pf, 1, (int)(slice > 100 ? 100 : slice));
        if (r < 0 && errno != EINTR) {
            close(fd);
            return -1;
        }
        if (r > 0) {
            int err = 0;
            socklen_t el = sizeof err;
            if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &el) != 0 || err) {
                close(fd);
                return -1;
            }
            break;
        }
    }
    return fd;
}

/* ---- the handshake */

struct hello {
    uint32_t id;
    uint8_t uuid[16], ident[16], nc[32];
    int kind;
};

/* the HELLO body: magic, version, id, uuid, ident, nonce, address (u16 length + bytes), kind; -1 when it is not */
static int hello_parse(const uint8_t *p, size_t n, struct hello *h)
{
    if (n < 4 + 1 + 4 + 16 + 16 + 32 + 2 + 1 || le32(p) != PL_CL_MAGIC || p[4] != PL_CL_VERSION)
        return -1;
    h->id = le32(p + 5);
    memcpy(h->uuid, p + 9, 16);
    memcpy(h->ident, p + 25, 16);
    memcpy(h->nc, p + 41, 32);
    size_t al = (size_t)p[73] | (size_t)p[74] << 8;
    if (n != 75 + al + 1 || memchr(p + 75, 0, al))
        return -1;
    h->kind = p[75 + al];
    return 0;
}

int peerlink_hello_kind(const uint8_t *hello, size_t len)
{
    struct hello h;
    if (hello_parse(hello, len, &h) != 0)
        return -1;
    return h.kind;
}

static int zero16(const uint8_t *u)
{
    for (int i = 0; i < 16; i++)
        if (u[i])
            return 0;
    return 1;
}

struct keys {
    struct hk in, out;
    uint32_t peer;
};

static void keys_derive(const struct peerlink *pl, int dialer, const uint8_t *tr, size_t trlen, struct keys *k)
{
    uint8_t ko[32], ki[32];
    hs_mac(pl->secret, pl->secret_len, dialer ? "tsdb-link-k>" : "tsdb-link-k<", tr, trlen, ko);
    hs_mac(pl->secret, pl->secret_len, dialer ? "tsdb-link-k<" : "tsdb-link-k>", tr, trlen, ki);
    hk_init(&k->out, ko, 32);
    hk_init(&k->in, ki, 32);
    tsdb_wipe(ko, 32);
    tsdb_wipe(ki, 32);
}

static int hs_dial(struct plink *l, int fd, struct keys *k)
{
    struct peerlink *pl = l->pl;
    uint64_t dl = now_ns() + (uint64_t)(pl->cfg.hs_ms ? pl->cfg.hs_ms : 5000) * MS;
    uint8_t tr[HS_TR_MAX], body[HS_FRAME_MAX], mac[32], want[32], type;
    size_t trlen = 0, n;
    struct buf b = {0};
    uint8_t nc[32];
    tsdb_random(nc, sizeof nc);
    put32(&b, PL_CL_MAGIC);
    put8(&b, PL_CL_VERSION);
    put32(&b, pl->cfg.self.id);
    put(&b, pl->cfg.self.uuid, 16);
    put(&b, pl->cfg.self.ident, 16);
    put(&b, nc, 32);
    size_t al = strnlen(pl->cfg.advertise, sizeof pl->cfg.advertise - 1);
    put8(&b, (uint8_t)al);
    put8(&b, (uint8_t)(al >> 8));
    put(&b, pl->cfg.advertise, al);
    put8(&b, (uint8_t)l->kind);
    memcpy(tr, b.p, b.len);
    trlen = b.len;
    int rc = send_hs(fd, F_HELLO, b.p, b.len, dl);
    buf_free(&b);
    if (rc != 0)
        return -1;
    if (read_hs(pl, fd, dl, &type, body, &n) != 0 || type != F_HELLO_ACK || n != 4 + 16 + 16 + 32 + 32)
        return -1;
    memcpy(tr + trlen, body, n - 32);
    trlen += n - 32;
    uint32_t id = le32(body);
    const uint8_t *uuid = body + 4;
    hs_mac(pl->secret, pl->secret_len, "tsdb-link-s", tr, trlen, want);
    if (!tsdb_ct_equal(want, body + n - 32, 32)) {
        ST(pl, auth_failures, 1);
        return -1;
    }
    if ((!zero16(pl->cfg.self.uuid) && !zero16(uuid) && memcmp(uuid, pl->cfg.self.uuid, 16)) ||
        (l->want_id && id != l->want_id))
        return -1;
    memcpy(tr + trlen, body + n - 32, 32);
    trlen += 32;
    hs_mac(pl->secret, pl->secret_len, "tsdb-link-c", tr, trlen, mac);
    if (send_hs(fd, F_AUTH, mac, 32, dl) != 0)
        return -1;
    keys_derive(pl, 1, tr, trlen, k);
    k->peer = id;
    return 0;
}

static int hs_accept(struct peerlink *pl, int fd, const uint8_t *hello, size_t hn, const struct pl_self *self,
                     struct keys *k)
{
    uint64_t dl = now_ns() + (uint64_t)(pl->cfg.hs_ms ? pl->cfg.hs_ms : 5000) * MS;
    uint8_t tr[HS_TR_MAX], body[HS_FRAME_MAX], mac[32], want[32], ns[32], type;
    size_t n;
    struct hello h;
    if (hn > HS_FRAME_MAX || hello_parse(hello, hn, &h) != 0)
        return -1;
    if (!zero16(self->uuid) && !zero16(h.uuid) && memcmp(h.uuid, self->uuid, 16) != 0 &&
        peerlink_mutant != PL_MUT_UUID)
        return -1;
    memcpy(tr, hello, hn);
    size_t trlen = hn;
    tsdb_random(ns, sizeof ns);
    struct buf b = {0};
    put32(&b, self->id);
    put(&b, self->uuid, 16);
    put(&b, self->ident, 16);
    put(&b, ns, 32);
    memcpy(tr + trlen, b.p, b.len);
    trlen += b.len;
    hs_mac(pl->secret, pl->secret_len, "tsdb-link-s", tr, trlen, mac);
    put(&b, mac, 32);
    int rc = send_hs(fd, F_HELLO_ACK, b.p, b.len, dl);
    buf_free(&b);
    if (rc != 0)
        return -1;
    memcpy(tr + trlen, mac, 32);
    trlen += 32;
    if (read_hs(pl, fd, dl, &type, body, &n) != 0 || type != F_AUTH || n != 32)
        return -1;
    hs_mac(pl->secret, pl->secret_len, "tsdb-link-c", tr, trlen, want);
    if (!tsdb_ct_equal(want, body, 32) && peerlink_mutant != PL_MUT_ACCAUTH) {
        ST(pl, auth_failures, 1);
        return -1;
    }
    keys_derive(pl, 0, tr, trlen, k);
    k->peer = h.id;
    return 0;
}

/* ---- one connection */

enum { J_WAITRESUME = 1, J_HASH, J_SEND, J_WAITDONE };
enum { HP_RX = 1, HP_TX };

struct conn {
    struct plink *l;
    struct peerlink *pl;
    int fd, dead;
    uint64_t my_seq; /* the adoption this thread serves (accepted links) */
    struct keys keys;
    uint64_t seq_out;
    struct pl_reader *rd;
    struct buf wb;
    ZSTD_CCtx *zc;
    uint64_t last_rx, last_ping;
    uint8_t *cbuf; /* a chunk's read buffer */

    struct { /* the file arriving. skip: the driver's data callback failed it: the chunks and END still in flight
              * for that file are ignored (the sender is told PL_E_IO and stops) */
        int active, skip;
        uint64_t file, total, next;
        struct pl_xxh h;
        int prefix_fd;
    } rx;

    /* the hash of a resume prefix, a megabyte per turn of the loop, so the link keeps answering PINGs meanwhile (a
     * multi-GB prefix on a slow volume takes longer than dead_ms) */
    struct {
        int active, kind;
        int fd;
        uint64_t n, off, base;
        struct pl_xxh *h;
    } hp;
    uint8_t *hbuf;
    int stalled; /* the socket did not take everything last turn: TCP is holding us back, not a silent peer */

    struct jobq *job; /* the file leaving */
    int jstate;
    uint64_t jnext, jsent, throttle_until;
    struct pl_xxh jh;

    /* tracked frames whose bytes are in wb, in order: [start, end) in the bytes this connection handed to wb (every
     * byte appended is at wdone + its distance from wb.off, so no appender needs to count) */
    uint64_t gen, wdone;
    struct trk {
        uint64_t token, start, end;
    } *trk;
    size_t trk_h, trk_n, trk_cap;
};

static void sent_cb(struct conn *c, uint64_t token, int written)
{
    if (c->pl->cfg.on_sent)
        c->pl->cfg.on_sent(c->pl->cfg.arg, c->l, token, c->gen, written);
}

/* the tracked frames whose first byte (the mutant: last byte) has reached the socket */
static void trk_written(struct conn *c)
{
    while (c->trk_h < c->trk_n) {
        struct trk *t = &c->trk[c->trk_h];
        if (peerlink_mutant == PL_MUT_QUEUEDASUNSENT ? t->end > c->wdone : t->start >= c->wdone)
            break;
        c->trk_h++;
        sent_cb(c, t->token, 1);
    }
    if (c->trk_h == c->trk_n)
        c->trk_h = c->trk_n = 0;
}

/* queued tracked frames past their deadline leave unwritten (l->mu held; the callbacks run after it is let go) */
static struct qf *q_expire(struct plink *l, uint64_t now)
{
    struct qf *gone = NULL, **pp = &l->qh, *last = NULL;
    uint64_t min = 0;
    while (*pp) {
        struct qf *f = *pp;
        if (f->tracked && f->deadline && f->deadline <= now) {
            *pp = f->next;
            l->qbytes -= f->n + 48;
            f->next = gone;
            gone = f;
            continue;
        }
        if (f->deadline && (!min || f->deadline < min))
            min = f->deadline;
        last = f;
        pp = &f->next;
    }
    l->qt = last;
    l->qdl_min = min;
    if (gone)
        pthread_cond_broadcast(&l->cv);
    return gone;
}

static void job_done(struct peerlink *pl, struct jobq *q, int status, uint64_t sent)
{
    if (status == PL_OK) {
        ST(pl, bulk_files, 1);
        ST(pl, bulk_bytes, q->j.length);
    }
    q->j.done(q->j.arg, q->j.file, status, sent);
    free(q);
}

static void send_ctl(struct conn *c, uint8_t type, const void *p, size_t n)
{
    frame_put(&c->wb, &c->keys.out, &c->seq_out, type, "", 0, p, n);
}

static void send_done(struct conn *c, uint64_t file, int32_t status)
{
    struct buf b = {0};
    put64(&b, file);
    put32(&b, (uint32_t)status);
    send_ctl(c, PLT_DONE, b.p, b.len);
    buf_free(&b);
}

static void rx_end(struct conn *c, int status)
{
    if (c->rx.active && c->pl->cfg.bulk.end)
        c->pl->cfg.bulk.end(c->pl->cfg.arg, c->l, c->rx.file, status);
    c->rx.active = 0;
}

static void rx_begin_done(struct conn *c, uint64_t file, int rc, uint64_t resume)
{
    struct buf b = {0};
    put64(&b, file);
    put32(&b, (uint32_t)(rc < 0 ? rc : 0));
    put64(&b, rc < 0 ? 0 : resume);
    send_ctl(c, PLT_RESUME, b.p, b.len);
    buf_free(&b);
    if (rc == 0)
        c->rx.next = resume;
}

static void hash_start(struct conn *c, int kind, int fd, uint64_t n, uint64_t base, struct pl_xxh *h)
{
    pl_xxh_init(h);
    c->hp.active = 1;
    c->hp.kind = kind;
    c->hp.fd = fd;
    c->hp.n = n;
    c->hp.off = 0;
    c->hp.base = base;
    c->hp.h = h;
}

/* one megabyte of the prefix; on the last one the file's turn goes on */
static void hash_step(struct conn *c)
{
    if (!c->hbuf && !(c->hbuf = malloc(1u << 20)))
        oom();
    size_t want = c->hp.n - c->hp.off < (1u << 20) ? (size_t)(c->hp.n - c->hp.off) : (1u << 20);
    ssize_t r = pread(c->hp.fd, c->hbuf, want, (off_t)(c->hp.base + c->hp.off));
    if (r < 0 && errno == EINTR)
        return;
    int bad = r <= 0;
    if (!bad) {
        pl_xxh_update(c->hp.h, c->hbuf, (size_t)r);
        c->hp.off += (uint64_t)r;
    }
    if (!bad && c->hp.off < c->hp.n)
        return;
    c->hp.active = 0;
    if (c->hp.kind == HP_RX) {
        if (bad) /* unreadable: start the file over */
            pl_xxh_init(&c->rx.h);
        rx_begin_done(c, c->rx.file, 0, bad ? 0 : c->hp.n);
    } else if (bad) {
        struct jobq *q = c->job;
        c->job = NULL;
        job_done(c->pl, q, PL_E_IO, 0);
        c->dead = 1;
    } else {
        c->jnext = c->hp.n;
        c->jstate = J_SEND;
    }
}

static int conn_msg(void *arg, const struct pl_msg *m)
{
    struct conn *c = arg;
    struct peerlink *pl = c->pl;
    c->last_rx = now_ns();
    if (m->type >= 1 && m->type <= PL_USER_TYPE_MAX) {
        ST(pl, frames_in, 1);
        ST(pl, bytes_in, m->n);
        if (pl->cfg.on_frame)
            pl->cfg.on_frame(pl->cfg.arg, c->l, m->type, m->p, m->n);
        return 0;
    }
    switch (m->type) {
    case PLT_PING:
        send_ctl(c, PLT_PONG, "", 0);
        return 0;
    case PLT_PONG:
        return 0;
    case PLT_BEGIN: {
        rx_end(c, PL_E_DOWN); /* a file left unfinished */
        c->rx.skip = 0;
        uint64_t resume = 0;
        int pfd = -1, rc = pl->cfg.bulk.open ? pl->cfg.bulk.open(pl->cfg.arg, c->l, m->file, m->data, m->data_len,
                                                                   m->a, &resume, &pfd)
                                             : PL_E_REFUSED;
        if (rc == 0 && resume > m->a && peerlink_mutant != PL_MUT_RESUMEBOUND)
            rc = PL_E_INVALID;
        pl_xxh_init(&c->rx.h);
        if (rc == 0) {
            c->rx.active = 1;
            c->rx.file = m->file;
            c->rx.total = m->a;
            c->rx.next = 0;
            c->rx.prefix_fd = pfd;
            if (resume > 0 && pfd >= 0) { /* RESUME goes out when the prefix is hashed */
                c->rx.next = resume;
                hash_start(c, HP_RX, pfd, resume, 0, &c->rx.h);
                return 0;
            }
            resume = 0;
        }
        rx_begin_done(c, m->file, rc, resume);
        return 0;
    }
    case PLT_CHUNK: {
        if (c->rx.skip && m->file == c->rx.file)
            return 0;
        if (!c->rx.active || c->hp.active || m->file != c->rx.file ||
            (m->a != c->rx.next && peerlink_mutant != PL_MUT_CHUNKOFF) || m->data_len > c->rx.total - c->rx.next)
            return -1;
        int rc = pl->cfg.bulk.data ? pl->cfg.bulk.data(pl->cfg.arg, c->l, m->file, m->a, m->data, m->data_len)
                                   : PL_E_IO;
        if (rc < 0) {
            send_done(c, m->file, PL_E_IO);
            rx_end(c, PL_E_IO);
            c->rx.skip = 1;
            return 0;
        }
        pl_xxh_update(&c->rx.h, m->data, m->data_len);
        c->rx.next += m->data_len;
        return 0;
    }
    case PLT_END: {
        if (c->rx.skip && m->file == c->rx.file)
            return 0;
        if (!c->rx.active || c->hp.active || m->file != c->rx.file || m->a != c->rx.total ||
            c->rx.next != c->rx.total)
            return -1;
        int st = pl_xxh_final(&c->rx.h) == m->b || peerlink_mutant == PL_MUT_ENDHASH ? PL_OK : PL_E_HASH;
        send_done(c, m->file, st);
        rx_end(c, st);
        return 0;
    }
    case PLT_RESUME: {
        struct jobq *q = c->job;
        if (!q || c->jstate != J_WAITRESUME || m->file != q->j.file)
            return -1;
        if (m->status < 0) {
            c->job = NULL;
            job_done(pl, q, PL_E_REFUSED, 0);
            return 0;
        }
        uint64_t off = m->a > q->j.length ? 0 : m->a;
        pl_xxh_init(&c->jh);
        c->jnext = 0;
        if (off) { /* SEND starts when the prefix is hashed */
            c->jstate = J_HASH;
            hash_start(c, HP_TX, q->j.fd, off, q->j.base, &c->jh);
            return 0;
        }
        c->jstate = J_SEND;
        return 0;
    }
    case PLT_DONE: {
        struct jobq *q = c->job;
        /* the receiver's verdict; in SEND it is a failure on its side (PL_E_IO): the transfer stops here, whatever
         * chunks are in flight are ignored over there */
        if (!q || (c->jstate != J_WAITDONE && c->jstate != J_SEND) || m->file != q->j.file)
            return -1;
        c->job = NULL;
        job_done(pl, q, m->status, c->jsent);
        return 0;
    }
    default:
        return -1;
    }
}

/* the next job off the list, when there is none in hand */
static void job_start(struct conn *c)
{
    struct plink *l = c->l;
    while (!c->job) {
        pthread_mutex_lock(&l->mu);
        struct jobq *q = l->jh;
        if (q) {
            l->jh = q->next;
            if (!l->jh)
                l->jt = NULL;
            l->njobs--;
        }
        pthread_mutex_unlock(&l->mu);
        if (!q)
            return;
        q->next = NULL;
        if (q->attempts++ >= q->j.max_attempts) {
            job_done(c->pl, q, PL_E_GIVEUP, 0);
            continue;
        }
        c->job = q;
        c->jstate = J_WAITRESUME;
        c->jsent = 0;
        struct buf b = {0};
        put64(&b, q->j.file);
        put64(&b, q->j.length);
        put32(&b, (uint32_t)q->j.meta_len);
        put(&b, q->meta, q->j.meta_len);
        send_ctl(c, PLT_BEGIN, b.p, b.len);
        buf_free(&b);
    }
}

/* chunks (or the END) while the write buffer has room; returns the ns to wait for the bucket, 0 when none */
static uint64_t job_send(struct conn *c)
{
    struct jobq *q = c->job;
    struct peerlink *pl = c->pl;
    uint32_t chunk = pl->cfg.bulk_chunk ? pl->cfg.bulk_chunk : PL_BULK_CHUNK;
    if (chunk > PL_FRAME_MAX - 16)
        chunk = PL_FRAME_MAX - 16;
    while (q && c->jstate == J_SEND && c->wb.len - c->wb.off < WB_HIGH) {
        if (c->jnext == q->j.length) {
            struct buf b = {0};
            put64(&b, q->j.file);
            put64(&b, q->j.length);
            put64(&b, pl_xxh_final(&c->jh));
            send_ctl(c, PLT_END, b.p, b.len);
            buf_free(&b);
            c->jstate = J_WAITDONE;
            break;
        }
        size_t n = q->j.length - c->jnext < chunk ? (size_t)(q->j.length - c->jnext) : chunk;
        uint64_t w = bucket_take(pl, n);
        if (w)
            return w;
        if (!c->cbuf && !(c->cbuf = malloc(chunk)))
            oom();
        size_t got = 0;
        while (got < n) {
            ssize_t r = pread(q->j.fd, c->cbuf + got, n - got, (off_t)(q->j.base + c->jnext + got));
            if (r < 0 && errno == EINTR)
                continue;
            if (r <= 0)
                break;
            got += (size_t)r;
        }
        if (got < n) { /* the file is shorter than the job says, or unreadable */
            c->job = NULL;
            job_done(pl, q, PL_E_IO, c->jsent);
            c->dead = 1; /* the receiver is mid-file: end the connection */
            return 0;
        }
        uint8_t h[16] = {0};
        for (int i = 0; i < 8; i++) {
            h[i] = (uint8_t)(q->j.file >> (8 * i));
            h[8 + i] = (uint8_t)(c->jnext >> (8 * i));
        }
        frame_put(&c->wb, &c->keys.out, &c->seq_out, PLT_CHUNK, h, 16, c->cbuf, n);
        pl_xxh_update(&c->jh, c->cbuf, n);
        c->jnext += n;
        c->jsent += n;
    }
    return 0;
}

static void conn_run(struct plink *l, int fd, struct keys *keys, uint64_t my_seq)
{
    struct peerlink *pl = l->pl;
    struct conn c = {.l = l, .pl = pl, .fd = fd, .my_seq = my_seq, .keys = *keys};
    c.zc = ZSTD_createCCtx();
    /* the reader checks the tags with the key the peer's frames carry: our `in` */
    c.rd = calloc(1, sizeof *c.rd);
    if (!c.rd || !c.zc)
        oom();
    c.rd->key = keys->in;
    uint8_t *rbuf = malloc(READ_BUF);
    if (!rbuf)
        oom();
    drain_evfd(l);
    /* no TCP_USER_TIMEOUT: Linux counts bytes held back by a shut window as unacknowledged, which would end a link
     * whose peer is merely slow. A silent idle peer is found by PING and dead_ms; a host that vanishes while we are held
     * back by it, by the kernel's own retransmission limit. */
    if (pl->cfg.sock_buf > 0) {
        setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &pl->cfg.sock_buf, sizeof pl->cfg.sock_buf);
        setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &pl->cfg.sock_buf, sizeof pl->cfg.sock_buf);
    }
    pthread_mutex_lock(&l->mu);
    l->up = 1;
    l->gen++;
    c.gen = l->gen;
    l->io_thr = pthread_self();
    l->io_set = 1;
    atomic_store(&l->peer, keys->peer);
    pthread_mutex_unlock(&l->mu);
    ST(pl, connects, 1);
    if (pl->cfg.on_link)
        pl->cfg.on_link(pl->cfg.arg, l, 1);
    pthread_mutex_lock(&l->mu);
    l->open = 1; /* the driver's own threads may send from here on (their frames follow on_link's) */
    pthread_mutex_unlock(&l->mu);
    c.last_rx = c.last_ping = now_ns();
    uint64_t ping_ns = (uint64_t)(pl->cfg.ping_ms ? pl->cfg.ping_ms : 1000) * MS;
    uint64_t dead_ns = (uint64_t)(pl->cfg.dead_ms ? pl->cfg.dead_ms : 5000) * MS;
    int poll_cap = (int)(ping_ns / MS / 2);
    if (poll_cap < 10)
        poll_cap = 10;

    while (!c.dead && !atomic_load(&pl->closing) && !atomic_load(&l->stopped)) {
        pthread_mutex_lock(&pl->mu);
        int kicked = !l->dialed && l->adopt_seq != my_seq;
        pthread_mutex_unlock(&pl->mu);
        if (kicked)
            break;
        uint64_t now = now_ns();
        pthread_mutex_lock(&l->mu);
        int reset = l->reset_gen == c.gen;
        struct qf *expired = l->qdl_min && l->qdl_min <= now ? q_expire(l, now) : NULL;
        pthread_mutex_unlock(&l->mu);
        while (expired) {
            struct qf *nx = expired->next;
            sent_cb(&c, expired->token, 0);
            free(expired);
            expired = nx;
        }
        if (reset) {
            ST(pl, drops, 1);
            break;
        }
        /* queued frames of the driver */
        while (c.wb.len - c.wb.off < WB_HIGH) {
            pthread_mutex_lock(&l->mu);
            struct qf *f = l->qh;
            if (f) {
                l->qh = f->next;
                if (!l->qh)
                    l->qt = NULL;
                l->qbytes -= f->n + 48;
                pthread_cond_broadcast(&l->cv);
            }
            pthread_mutex_unlock(&l->mu);
            if (!f)
                break;
            if (f->tracked && f->deadline && f->deadline <= now) {
                sent_cb(&c, f->token, 0);
                free(f);
                continue;
            }
            size_t before = c.wb.len, pend0 = c.wb.len - c.wb.off;
            frame_user(&c.wb, &c.keys.out, &c.seq_out, c.zc, l->kind != PL_KIND_BULK, f->type, f->p, f->n);
            if (f->tracked) {
                if (c.trk_n == c.trk_cap) {
                    if (c.trk_h) {
                        memmove(c.trk, c.trk + c.trk_h, (c.trk_n - c.trk_h) * sizeof *c.trk);
                        c.trk_n -= c.trk_h;
                        c.trk_h = 0;
                    } else {
                        c.trk_cap = c.trk_cap ? c.trk_cap * 2 : 64;
                        c.trk = realloc(c.trk, c.trk_cap * sizeof *c.trk);
                        if (!c.trk)
                            oom();
                    }
                }
                c.trk[c.trk_n++] = (struct trk){f->token, c.wdone + pend0, c.wdone + (c.wb.len - c.wb.off)};
            }
            ST(pl, frames_out, 1);
            ST(pl, bytes_out, c.wb.len - before);
            ST(pl, raw_out, f->n);
            free(f);
        }
        job_start(&c);
        if (c.hp.active)
            hash_step(&c);
        uint64_t bwait = 0;
        if (c.job && c.jstate == J_SEND) {
            if (c.throttle_until <= now)
                bwait = job_send(&c);
            else
                bwait = c.throttle_until - now;
            if (bwait)
                c.throttle_until = now + bwait;
        }
        int paused = atomic_load(&l->paused), pfresh = paused && peerlink_mutant != PL_MUT_PAUSEDEAD;
        if (c.stalled)
            c.last_rx = now; /* the peer's window is shut with our bytes waiting: it is slow, not silent */
        if (pfresh)
            c.last_rx = now; /* we do not read: the peer's silence is ours (its PINGs wait in the socket) */
        if (now - c.last_rx > dead_ns) {
            ST(pl, drops, 1);
            break;
        }
        /* paused, we still PING on our own clock: the peer hears from us and does not count us dead */
        if ((pfresh || now - c.last_rx > ping_ns) && now - c.last_ping > ping_ns) {
            send_ctl(&c, PLT_PING, "", 0);
            c.last_ping = now;
        }
        /* the buffer was filled to its mark: frames may be waiting behind it, and nothing wakes us for them */
        int full = c.wb.len - c.wb.off >= WB_HIGH;
        while (c.wb.len > c.wb.off) {
            ssize_t w = write(fd, c.wb.p + c.wb.off, c.wb.len - c.wb.off);
            if (w < 0 && errno == EINTR)
                continue;
            if (w < 0 && errno == EAGAIN)
                break;
            if (w <= 0) {
                c.dead = 1;
                break;
            }
            c.wb.off += (size_t)w;
            c.wdone += (uint64_t)w;
        }
        trk_written(&c);
        if (c.wb.off == c.wb.len)
            c.wb.off = c.wb.len = 0;
        c.stalled = c.wb.len > c.wb.off;
        if (c.dead)
            break;
        int tmo = poll_cap;
        if (bwait && (int)(bwait / MS) + 1 < tmo)
            tmo = (int)(bwait / MS) + 1;
        /* a file with more to send, not held by the bucket, and nothing left to write: fill the buffer again at
         * once (with data in the buffer the POLLOUT below wakes us when the socket drains) */
        if (c.wb.len == c.wb.off && (full || (!bwait && c.job && c.jstate == J_SEND)))
            tmo = 0;
        if (c.hp.active && !c.dead)
            tmo = 0; /* the next megabyte of a prefix */
        pthread_mutex_lock(&l->mu);
        uint64_t qdl = l->qdl_min;
        pthread_mutex_unlock(&l->mu);
        if (qdl) { /* a queued frame's deadline */
            uint64_t t = now_ns();
            int d = qdl <= t ? 0 : (int)((qdl - t) / MS) + 1;
            if (d < tmo)
                tmo = d;
        }
        /* paused: no POLLIN (a hangup or an error is still reported, and read) */
        struct pollfd pf[2] = {{fd, (short)((paused ? 0 : POLLIN) | (c.wb.len > c.wb.off ? POLLOUT : 0)), 0},
                               {l->evfd, POLLIN, 0}};
        /* room left in the write buffer: come back as soon as the socket drains */
        int pr = poll(pf, 2, tmo);
        if (pr < 0 && errno != EINTR)
            break;
        if (pr <= 0)
            continue;
        if (pf[1].revents & POLLIN)
            drain_evfd(l);
        if (pf[0].revents & (POLLIN | POLLHUP | POLLERR)) {
            ssize_t r = read(fd, rbuf, READ_BUF);
            if (r > 0) {
                c.last_rx = now_ns();
                int fr = pl_reader_feed(c.rd, rbuf, (size_t)r, conn_msg, &c);
                c.last_rx = now_ns(); /* our own callbacks' time is not the peer's silence */
                if (fr != 0) {
                    ST(pl, drops, 1);
                    break;
                }
            } else if (r == 0 || (errno != EAGAIN && errno != EINTR)) {
                ST(pl, drops, 1);
                break;
            }
        }
    }

    /* the connection ended: what was in flight goes back or is told */
    rx_end(&c, PL_E_DOWN);
    pthread_mutex_lock(&l->mu);
    l->up = 0;
    l->open = 0;
    l->io_set = 0;
    struct qf *f = l->qh;
    l->qh = l->qt = NULL;
    l->qbytes = 0;
    l->qdl_min = 0;
    pthread_cond_broadcast(&l->cv);
    pthread_mutex_unlock(&l->mu);
    /* the tracked frames still in wb: written if any of their bytes went (the mutant: all of them), else not; then
     * the queue, none of whose bytes went */
    for (; c.trk_h < c.trk_n; c.trk_h++) {
        struct trk *t = &c.trk[c.trk_h];
        sent_cb(&c, t->token,
                peerlink_mutant == PL_MUT_QUEUEDASUNSENT ? t->end <= c.wdone
                : peerlink_mutant == PL_MUT_WBWRITTEN    ? 1
                                                         : t->start < c.wdone);
    }
    free(c.trk);
    while (f) {
        struct qf *nx = f->next;
        if (f->tracked)
            sent_cb(&c, f->token, 0);
        free(f);
        f = nx;
    }
    if (c.job) {
        struct jobq *q = c.job;
        if (atomic_load(&pl->closing)) {
            job_done(pl, q, PL_E_CLOSED, c.jsent);
        } else {
            pthread_mutex_lock(&l->mu);
            q->next = l->jh;
            l->jh = q;
            if (!l->jt)
                l->jt = q;
            l->njobs++;
            pthread_mutex_unlock(&l->mu);
        }
    }
    if (pl->cfg.on_link)
        pl->cfg.on_link(pl->cfg.arg, l, 0);
    close(fd);
    pl_reader_free(c.rd);
    ZSTD_freeCCtx(c.zc);
    buf_free(&c.wb);
    free(c.cbuf);
    free(c.hbuf);
    free(rbuf);
    tsdb_wipe(&c.keys, sizeof c.keys);
}

/* a dialed link: connect, handshake, run, and again after a backoff, until the peerlink closes */
static void *dial_main(void *arg)
{
    struct plink *l = arg;
    struct peerlink *pl = l->pl;
    uint64_t lo = (uint64_t)(pl->cfg.backoff_min_ms ? pl->cfg.backoff_min_ms : 50) * MS;
    uint64_t hi = (uint64_t)(pl->cfg.backoff_max_ms ? pl->cfg.backoff_max_ms : 2000) * MS;
    uint64_t gap = lo;
    while (!atomic_load(&pl->closing) && !atomic_load(&l->stopped)) {
        uint64_t hs = (uint64_t)(pl->cfg.hs_ms ? pl->cfg.hs_ms : 5000) * MS;
        char addr[sizeof l->addr];
        pthread_mutex_lock(&pl->mu); /* plink_set_addr may change it */
        memcpy(addr, l->addr, sizeof addr);
        pthread_mutex_unlock(&pl->mu);
        int fd = dial_fd(pl, addr, now_ns() + hs);
        struct keys k;
        if (fd >= 0 && hs_dial(l, fd, &k) == 0) {
            gap = lo;
            conn_run(l, fd, &k, 0);
        } else if (fd >= 0) {
            close(fd);
        }
        /* a jittered gap, cut short by a close */
        uint64_t rnd;
        tsdb_random(&rnd, sizeof rnd);
        uint64_t until = now_ns() + gap / 2 + rnd % (gap / 2 + 1);
        while (!atomic_load(&pl->closing) && !atomic_load(&l->stopped) && now_ns() < until) {
            struct pollfd pf = {l->evfd, POLLIN, 0};
            poll(&pf, 1, 20);
            if (pf.revents & POLLIN)
                drain_evfd(l);
        }
        gap = gap * 2 > hi ? hi : gap * 2;
    }
    return NULL;
}

struct adopt {
    struct peerlink *pl;
    int fd;
    uint8_t hello[HS_FRAME_MAX];
    size_t hn;
    struct pl_self self;
};

/* The accepted link of (peer, kind), made when it is not there: called only for a peer that passed AUTH (pl->mu held),
 * so no one without the secret makes a link struct, a pipe or a claim. */
static struct plink *accepted_link(struct peerlink *pl, uint32_t peer, int kind)
{
    struct plink *l;
    for (l = pl->links; l; l = l->next)
        if (!l->dialed && l->kind == kind && atomic_load(&l->peer) == peer)
            return l;
    l = link_new(pl, kind, 0);
    if (!l)
        return NULL;
    atomic_store(&l->peer, peer);
    l->next = pl->links;
    pl->links = l;
    return l;
}

/* An accepted connection: the handshake first, on this thread, with nothing else touched; a peer that holds the secret
 * then takes the link over from the connection that runs it (which leaves when it is next at its loop's top) and from
 * a connection of the same peer that is waiting for the same. At most one connection runs and one waits per link
 * (an older waiting one is closed by the newer), and this thread never joins another: a connection stuck in a driver
 * callback (a volume on NFS that is away) holds its own thread and descriptor only, however often the peer redials. */
static void *adopt_main(void *arg)
{
    struct adopt *a = arg;
    struct peerlink *pl = a->pl;
    struct hello h;
    struct keys k = {0};
    int ok = !atomic_load(&pl->closing) && hello_parse(a->hello, a->hn, &h) == 0 &&
             hs_accept(pl, a->fd, a->hello, a->hn, &a->self, &k) == 0;
    pthread_mutex_lock(&pl->mu);
    pl->nhand--;
    struct plink *l = ok && !atomic_load(&pl->closing) ? accepted_link(pl, h.id, h.kind) : NULL;
    uint64_t my = 0;
    if (l) {
        my = peerlink_mutant == PL_MUT_NOKICK ? l->adopt_seq : ++l->adopt_seq;
        wake(l);
        pthread_cond_broadcast(&pl->acv);
        while (l->running && l->adopt_seq == my && !atomic_load(&pl->closing))
            pthread_cond_wait(&pl->acv, &pl->mu);
        if (l->adopt_seq != my || atomic_load(&pl->closing))
            l = NULL;
        else
            l->running = 1;
    }
    pthread_mutex_unlock(&pl->mu);
    if (l) {
        conn_run(l, a->fd, &k, my);
        pthread_mutex_lock(&pl->mu);
        l->running = 0;
        pthread_cond_broadcast(&pl->acv);
        pthread_mutex_unlock(&pl->mu);
    } else {
        close(a->fd);
    }
    tsdb_wipe(&k, sizeof k);
    free(a);
    atomic_fetch_sub(&pl->helpers, 1);
    return NULL;
}

int peerlink_adopt(struct peerlink *pl, int fd, const uint8_t *hello, size_t hello_len, const struct pl_self *self)
{
    struct hello h;
    if (hello_len > HS_FRAME_MAX || hello_parse(hello, hello_len, &h) != 0)
        return -1;
    if (h.kind <= PL_KIND_META || h.kind > PL_KIND_DATA_MAX) {
        fprintf(stderr, "peerlink: refused a link of kind %d from node %" PRIu32 " (unknown here)\n", h.kind, h.id);
        return -1;
    }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    struct adopt *a = calloc(1, sizeof *a);
    if (!a)
        return -1;
    a->pl = pl;
    a->fd = fd;
    memcpy(a->hello, hello, hello_len);
    a->hn = hello_len;
    a->self = *self;
    /* closing is checked under mu, where peerlink_free sets it: an adoption that gets past this is waited for */
    pthread_mutex_lock(&pl->mu);
    if (atomic_load(&pl->closing) || pl->nhand >= MAX_HAND) {
        pthread_mutex_unlock(&pl->mu);
        free(a);
        return -1;
    }
    pl->nhand++;
    atomic_fetch_add(&pl->helpers, 1);
    pthread_t t;
    pthread_attr_t at;
    pthread_attr_init(&at);
    pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED);
    int rc = pthread_create(&t, &at, adopt_main, a);
    pthread_attr_destroy(&at);
    if (rc != 0) {
        pl->nhand--;
        atomic_fetch_sub(&pl->helpers, 1);
        pthread_mutex_unlock(&pl->mu);
        free(a);
        return -1;
    }
    pthread_mutex_unlock(&pl->mu);
    return 0;
}

struct plink *peerlink_find(struct peerlink *pl, uint32_t peer, int kind)
{
    struct plink *l;
    pthread_mutex_lock(&pl->mu);
    for (l = pl->links; l; l = l->next)
        if (!l->dialed && l->kind == kind && atomic_load(&l->peer) == peer)
            break;
    pthread_mutex_unlock(&pl->mu);
    return l;
}

struct plink *peerlink_dial(struct peerlink *pl, uint32_t want_id, const char *addr, int kind)
{
    if (strlen(addr) >= sizeof((struct plink *)0)->addr || kind <= PL_KIND_META || kind > PL_KIND_DATA_MAX)
        return NULL;
    pthread_mutex_lock(&pl->mu);
    struct plink *l;
    if (atomic_load(&pl->closing)) {
        pthread_mutex_unlock(&pl->mu);
        return NULL;
    }
    if (peerlink_mutant == PL_MUT_SHAREDSOCKET && kind == PL_KIND_BULK) {
        /* the planted bug: the bulk link is the log link's socket */
        for (l = pl->links; l; l = l->next)
            if (l->dialed && l->kind == PL_KIND_LOG && l->want_id == want_id && !strcmp(l->addr, addr)) {
                pthread_mutex_unlock(&pl->mu);
                return l;
            }
        kind = PL_KIND_LOG;
    }
    if (peerlink_mutant == PL_MUT_KINDSHARED && kind == PL_KIND_FWD) {
        /* the planted bug: forwarded frames on the log link's socket */
        for (l = pl->links; l; l = l->next)
            if (l->dialed && l->kind == PL_KIND_LOG && l->want_id == want_id && !strcmp(l->addr, addr)) {
                pthread_mutex_unlock(&pl->mu);
                return l;
            }
    }
    l = link_new(pl, kind, 1);
    if (!l) {
        pthread_mutex_unlock(&pl->mu);
        return NULL;
    }
    l->want_id = want_id;
    snprintf(l->addr, sizeof l->addr, "%s", addr);
    atomic_store(&l->peer, want_id);
    if (pthread_create(&l->thr, NULL, dial_main, l) != 0) {
        pthread_mutex_unlock(&pl->mu);
        close(l->evfd);
        close(l->evw);
        free(l);
        return NULL;
    }
    l->thr_valid = 1;
    l->next = pl->links;
    pl->links = l;
    pthread_mutex_unlock(&pl->mu);
    return l;
}

int plink_set_addr(struct plink *l, const char *addr)
{
    struct peerlink *pl = l->pl;
    if (!l->dialed || strlen(addr) >= sizeof l->addr)
        return -1;
    pthread_mutex_lock(&pl->mu);
    snprintf(l->addr, sizeof l->addr, "%s", addr);
    pthread_mutex_unlock(&pl->mu);
    return 0;
}

/* ---- the standalone listener */

struct lhs {
    struct peerlink *pl;
    int fd;
};

static void *lhs_main(void *arg)
{
    struct lhs *h = arg;
    struct peerlink *pl = h->pl;
    uint8_t body[HS_FRAME_MAX], type;
    size_t n;
    int adopted = 0;
    if (read_hs(pl, h->fd, now_ns() + (uint64_t)(pl->cfg.hs_ms ? pl->cfg.hs_ms : 5000) * MS, &type, body, &n) == 0 &&
        type == F_HELLO)
        adopted = peerlink_adopt(pl, h->fd, body, n, &pl->cfg.self) == 0;
    if (!adopted)
        close(h->fd);
    free(h);
    atomic_fetch_sub(&pl->helpers, 1);
    return NULL;
}

static void *listen_main(void *arg)
{
    struct peerlink *pl = arg;
    while (!atomic_load(&pl->closing)) {
        struct pollfd pf = {pl->lfd, POLLIN, 0};
        if (poll(&pf, 1, 50) <= 0)
            continue;
        int fd = accept4(pl->lfd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0)
            continue;
        struct lhs *h = malloc(sizeof *h);
        pthread_t t;
        if (!h) {
            close(fd);
            continue;
        }
        h->pl = pl;
        h->fd = fd;
        atomic_fetch_add(&pl->helpers, 1);
        if (pthread_create(&t, NULL, lhs_main, h) != 0) {
            atomic_fetch_sub(&pl->helpers, 1);
            close(fd);
            free(h);
        } else {
            pthread_detach(t);
        }
    }
    return NULL;
}

int peerlink_listen(struct peerlink *pl, int port)
{
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
    if (fd < 0)
        return -1;
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port)};
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t sl = sizeof sa;
    if (bind(fd, (struct sockaddr *)&sa, sizeof sa) != 0 || listen(fd, 64) != 0 ||
        getsockname(fd, (struct sockaddr *)&sa, &sl) != 0) {
        close(fd);
        return -1;
    }
    pl->lfd = fd;
    if (pthread_create(&pl->lthr, NULL, listen_main, pl) != 0) {
        close(fd);
        return -1;
    }
    pl->lthr_valid = 1;
    return ntohs(sa.sin_port);
}

/* ---- lifetime */

struct peerlink *peerlink_new(const struct pl_cfg *cfg)
{
    if (!cfg || !cfg->secret || cfg->secret_len == 0 || cfg->secret_len > 1024)
        return NULL;
    struct peerlink *pl = calloc(1, sizeof *pl);
    if (!pl)
        return NULL;
    pl->cfg = *cfg;
    memcpy(pl->secret, cfg->secret, cfg->secret_len);
    pl->secret_len = cfg->secret_len;
    pl->cfg.secret = pl->secret;
    pl->lfd = -1;
    pthread_mutex_init(&pl->mu, NULL);
    pthread_cond_init(&pl->acv, NULL);
    pthread_mutex_init(&pl->bmu, NULL);
    atomic_store(&pl->rate, cfg->bulk_rate);
    return pl;
}

void peerlink_stats(struct peerlink *pl, struct pl_stats *st)
{
    uint64_t *d = (uint64_t *)st, *s = (uint64_t *)&pl->st;
    for (size_t i = 0; i < sizeof *st / 8; i++)
        d[i] = __atomic_load_n(&s[i], __ATOMIC_RELAXED);
}

void peerlink_free(struct peerlink *pl)
{
    if (!pl)
        return;
    pthread_mutex_lock(&pl->mu);
    atomic_store(&pl->closing, 1);
    pthread_cond_broadcast(&pl->acv);
    for (struct plink *l = pl->links; l; l = l->next) {
        wake(l);
        pthread_mutex_lock(&l->mu);
        pthread_cond_broadcast(&l->cv);
        pthread_mutex_unlock(&l->mu);
    }
    pthread_mutex_unlock(&pl->mu);
    if (pl->lthr_valid) {
        pthread_join(pl->lthr, NULL);
        close(pl->lfd);
    }
    while (atomic_load(&pl->helpers) > 0)
        usleep(1000);
    struct plink *l = pl->links;
    while (l) {
        struct plink *nx = l->next;
        if (l->thr_valid)
            pthread_join(l->thr, NULL);
        for (struct qf *f = l->qh; f;) {
            struct qf *n = f->next;
            free(f);
            f = n;
        }
        for (struct jobq *q = l->jh; q;) {
            struct jobq *n = q->next;
            job_done(pl, q, PL_E_CLOSED, 0);
            q = n;
        }
        close(l->evfd);
        close(l->evw);
        pthread_cond_destroy(&l->cv);
        pthread_mutex_destroy(&l->mu);
        free(l);
        l = nx;
    }
    pthread_mutex_destroy(&pl->mu);
    pthread_cond_destroy(&pl->acv);
    pthread_mutex_destroy(&pl->bmu);
    tsdb_wipe(pl->secret, sizeof pl->secret);
    free(pl);
}
