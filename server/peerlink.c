/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Data links (peerlink.h has the design and the wire). The file has four parts: bytes and hashes (a growing buffer,
 * the streaming xxh64, HMAC with the pads absorbed once per key for the handshake, the frames' check of plcheck.h), the
 * frame codec (pl_msg_decode, pl_reader: what a
 * fuzzer reaches), the handshake (blocking, with deadlines, on the link's own thread, always on TCP), the TCP transport
 * and the link threads.
 *
 * One thread runs one connection of one link: it owns the transport (pltransport.h: the TCP socket the handshake ran
 * on, the only one so far), the reader, the write buffer and the state of the bulk file in flight in each direction.
 * The driver's threads only touch the link's queue and job list (under `plink.mu`) and the eventfd that wakes the
 * thread. A frame queued with plink_send is copied; compressing, tagging
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
#include "plcheck.h"
#include "plrdma.h"
#include "pltransport.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <limits.h>
#include <net/if.h>
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
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>
#include <zstd.h>

#include "../src/compat.h" /* macOS: accept4, SOCK_CLOEXEC; nothing on Linux */

int peerlink_mutant;
static _Atomic int check_default; /* peerlink_set_check_level */
static _Atomic int g_allow_ipoib; /* peerlink_set_allow_ipoib */
static pthread_mutex_t g_rdma_mu = PTHREAD_MUTEX_INITIALIZER;
static struct pl_rdma_cfg g_rdma;
int (*peerlink_test_iftype)(const char *ifname);
int peerlink_test_rdma_hold_ready_ms;
int peerlink_test_rdma_abort_before_hello;
int peerlink_test_rdma_fail_after_hello;
int peerlink_test_rdma_skip_go;
int peerlink_test_rdma_bad_auth;
int peerlink_test_rdma_cm_bad_pd;
uint64_t peerlink_test_rdma_offers;
int peerlink_test_cut_end;
int32_t peerlink_test_done_status;
static uint64_t kind_bytes[PL_KIND_DATA_MAX + 1]; /* peerlink_kind_bytes: process-wide, so no peerlink is touched */
void (*peerlink_test_salts)(int dialer, uint64_t out, uint64_t in);

#define MS 1000000ull
#define HS_FRAME_MAX 512
#define HS_TR_MAX (HS_FRAME_MAX + 128)
#define WIRE_MAX (PL_FRAME_MAX + 128) /* the longest frame after its length field: type, header, payload, check */
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

/* one direction's frame check (plcheck.h): its type (the level the connection runs at) and the connection's salt for
 * that direction */
struct fck {
    int type;
    uint64_t salt;
};

/* the check of the frame of wire length `len` at f, the frame's number in its direction `seq`, seeded with salt ^ seq;
 * `body` is its type, header and payload (all before the check). PL_CHECK_FULL covers the body as it is on the wire;
 * PL_CHECK_HEADER the length, the type, the compression header and the first PL_CHECK_SPAN bytes of the payload as the
 * driver gave it, a[0..an) then b[0..bn) (a compressed frame's decompressed bytes: the reader has them only after the
 * decode). out: pl_check_len(k->type) bytes; nothing for PL_CHECK_NONE. */
static void frame_check(const struct fck *k, uint64_t seq, uint32_t len, const uint8_t *f, size_t body,
                        const uint8_t *a, size_t an, const uint8_t *b, size_t bn, uint8_t *out)
{
    uint64_t seed = peerlink_mutant == PL_MUT_NOSEQ    ? k->salt
                    : peerlink_mutant == PL_MUT_NOSALT ? seq
                    : peerlink_mutant == PL_MUT_SEQ8   ? k->salt ^ (uint8_t)seq
                                                       : k->salt ^ seq;
    int notype = peerlink_mutant == PL_MUT_NOTYPE;
    if (k->type == PL_CHECK_FULL)
        pl_check(seed, f + notype, body - (size_t)notype, out);
    else if (k->type == PL_CHECK_HEADER)
        pl_check_header(seed, len, notype ? 0 : f[0], f + 1, f[0] & 0x80 ? 4 : 0, a, an, b, bn, out);
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
    case PLT_RDMA_GO:
    case PLT_RDMA_ABORT:
        return n == 0 ? 0 : -1;
    case PLT_RDMA_OFFER:
        if (n != PLR_EP_LEN + 8)
            return -1;
        m->data = p;
        m->data_len = PLR_EP_LEN;
        m->a = le64(p + PLR_EP_LEN);
        return 0;
    case PLT_RDMA_ACCEPT:
        if (n != PLR_EP_LEN && n != PLR_EP_LEN + PLR_CM_ADDR_LEN)
            return -1;
        m->data = p;
        m->data_len = n;
        return 0;
    case PLT_RDMA_DECLINE:
        if (n != 1)
            return -1;
        m->a = p[0];
        return 0;
    case PLT_RDMA_HELLO:
    case PLT_RDMA_READY:
        if (n != 8 + 32)
            return -1;
        m->a = le64(p);
        m->data = p + 8;
        m->data_len = 32;
        return 0;
    default:
        return -1;
    }
}

/* [u32 len][type][hdr][payload][check] appended to b; `hdr` is the compression header (then lp, ln is the payload
 * before compression, for PL_CHECK_HEADER) or a control frame's fields (lp NULL); the check as long as its type says
 * (none: no bytes) */
static void frame_put(struct buf *b, const struct fck *k, uint64_t *seq, uint8_t type, const void *h, size_t hn,
                      const void *p, size_t pn, const void *lp, size_t ln)
{
    size_t cl = pl_check_len(k->type);
    uint8_t t[PL_CHECK_LEN_MAX];
    /* room for the whole frame first: a put that has to make room moves the buffer's data to its start
     * (buf_reserve), which would leave `at` pointing at other bytes */
    buf_reserve(b, 4 + 1 + hn + pn + cl);
    uint32_t len = (uint32_t)(1 + hn + pn + cl);
    put32(b, len);
    size_t at = b->len;
    put8(b, type);
    put(b, h, hn);
    put(b, p, pn);
    if (cl) {
        if (lp)
            frame_check(k, *seq, len, b->p + at, b->len - at, lp, ln, NULL, 0, t);
        else
            frame_check(k, *seq, len, b->p + at, b->len - at, h, hn, p, pn, t);
        put(b, t, cl);
    }
    (*seq)++;
}

/* a driver frame, compressed when it pays; `zc` is the thread's zstd context (NULL: a one-off) */
static void frame_user(struct buf *b, const struct fck *k, uint64_t *seq, ZSTD_CCtx *zc, int compress, uint8_t type,
                       const void *p, size_t n)
{
    if (compress && n >= COMPRESS_MIN) {
        size_t bound = ZSTD_compressBound(n);
        uint8_t *z = malloc(bound);
        if (z) {
            size_t zn = zc ? ZSTD_compressCCtx(zc, z, bound, p, n, 1) : ZSTD_compress(z, bound, p, n, 1);
            if (!ZSTD_isError(zn) && zn + 4 + n / 8 < n) {
                uint8_t h[4] = {(uint8_t)n, (uint8_t)(n >> 8), (uint8_t)(n >> 16), (uint8_t)(n >> 24)};
                frame_put(b, k, seq, type | 0x80, h, 4, z, zn, p, n);
                free(z);
                return;
            }
            free(z);
        }
    }
    frame_put(b, k, seq, type, "", 0, p, n, NULL, 0);
}

size_t pl_frame_encode(int check, uint64_t salt, uint64_t *seq, int compress, uint8_t type, const void *p, size_t n,
                       uint8_t **out)
{
    struct fck k = {check, salt};
    struct buf b = {0};
    frame_user(&b, &k, seq, NULL, compress, type, p, n);
    *out = b.p;
    return b.len;
}

struct pl_reader {
    struct fck key;
    int skip, bad;
    uint64_t seq;
    struct buf rb;
};

struct pl_reader *pl_reader_new(int check, uint64_t salt, int skip_tag)
{
    struct pl_reader *r = calloc(1, sizeof *r);
    if (!r)
        return NULL;
    r->key = (struct fck){check, salt};
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
        size_t cl = pl_check_len(r->key.type);
        if (len < 1 + cl || len > WIRE_MAX) {
            r->bad = 1;
            break;
        }
        if (r->rb.len - r->rb.off - 4 < len)
            break;
        const uint8_t *f = at + 4;
        size_t body = len - cl;
        uint8_t t[PL_CHECK_LEN_MAX];
        uint64_t seq = r->seq++;
        int verify = cl && !r->skip && peerlink_mutant != PL_MUT_NOTAG;
        size_t cmp = peerlink_mutant == PL_MUT_HALFCMP ? cl / 2 : cl;
        if (verify && r->key.type == PL_CHECK_FULL) { /* before the decode: the bytes as they are on the wire */
            frame_check(&r->key, seq, len, f, body, NULL, 0, NULL, 0, t);
            if (!tsdb_ct_equal(t, f + body, cmp)) {
                r->bad = 1;
                break;
            }
        }
        struct pl_msg m;
        uint8_t *own = NULL;
        if (pl_msg_decode(f[0], f + 1, body - 1, &m, &own) != 0) {
            r->bad = 1;
            break;
        }
        if (verify && r->key.type == PL_CHECK_HEADER) { /* after it: the payload's head as the driver gave it */
            frame_check(&r->key, seq, len, f, body, m.p, m.n, NULL, 0, t);
            if (!tsdb_ct_equal(t, f + body, cmp)) {
                free(own);
                r->bad = 1;
                break;
            }
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
    _Atomic int check;     /* the frame check type its connection runs (plcheck.h), 0 while down */
    _Atomic int transport; /* 0: down, 1: tcp, 2: rdma */
    void *arg;
    int evfd, evw;
    pthread_t thr; /* a dialed link's thread (accepted ones run on detached threads, counted in pl->helpers) */
    int thr_valid;
    /* RDMA upgrade backoff state (dialed links, owned by dial_main / conn_run thread) */
    int rdma_fails;
    int rdma_backed_off;
    int rdma_pool_idx;
    uint64_t rdma_backoff_ns;
    uint64_t rdma_until_ns;
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
    pthread_mutex_t bmu; /* the bulk token buckets, one per data kind (R4 D7: a kind of its own never waits behind
                          * another's files: a snapshot's on kind 5 does not hold a COMMIT's on kind 2) */
    uint64_t vt[PL_KIND_DATA_MAX + 1];
    _Atomic uint64_t rate;                          /* every kind without a rate of its own */
    _Atomic uint64_t krate[PL_KIND_DATA_MAX + 1]; /* 0: `rate` */
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
int plink_check(const struct plink *l) { return atomic_load(&((struct plink *)l)->check); }
int plink_is_rdma(const struct plink *l) { return atomic_load(&((struct plink *)l)->transport) == 2; }
const char *plink_transport(const struct plink *l)
{
    int tr = atomic_load(&((struct plink *)l)->transport);
    return tr == 2 ? "rdma" : tr == 1 ? "tcp" : "";
}
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

void plink_reset(struct plink *l) { plink_reset_gen(l, 0); }

void plink_reset_gen(struct plink *l, uint64_t gen)
{
    pthread_mutex_lock(&l->mu);
    if (l->up && (!gen || l->gen == gen))
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
static uint64_t bucket_take(struct peerlink *pl, int kind, size_t n)
{
    if (kind < 0 || kind > PL_KIND_DATA_MAX || peerlink_mutant == PL_MUT_KINDBUCKET)
        kind = 0; /* PL_MUT_KINDBUCKET: every kind in one bucket at the node-wide rate (r3d's) */
    uint64_t rate = peerlink_mutant == PL_MUT_KINDBUCKET ? 0 : atomic_load(&pl->krate[kind]);
    if (!rate)
        rate = atomic_load(&pl->rate);
    if (rate == PL_RATE_UNLIMITED || peerlink_mutant == PL_MUT_NOTHROTTLE)
        return 0;
    if (!rate)
        rate = PL_BULK_RATE_DEFAULT;
    uint64_t now = now_ns(), burst = 50 * MS, cost = (uint64_t)((double)n * 1e9 / (double)rate), wait = 0;
    pthread_mutex_lock(&pl->bmu);
    uint64_t *vt = &pl->vt[kind];
    if (*vt <= now) {
        uint64_t floor = now > burst ? now - burst : 0;
        *vt = (*vt > floor ? *vt : floor) + cost;
    } else {
        wait = *vt - now;
    }
    pthread_mutex_unlock(&pl->bmu);
    return wait;
}

void peerlink_set_bulk_rate(struct peerlink *pl, uint64_t bytes_per_s) { atomic_store(&pl->rate, bytes_per_s); }

int peerlink_set_kind_rate(struct peerlink *pl, int kind, uint64_t bytes_per_s)
{
    if (kind < PL_KIND_LOG || kind > PL_KIND_DATA_MAX)
        return PL_E_INVALID;
    atomic_store(&pl->krate[kind], bytes_per_s);
    return PL_OK;
}

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

/* ---- IPoIB refusal (docs/design-rdma.md §5.3, D12): no TCP over InfiniBand interfaces unless explicitly allowed */

#define PL_ARPHRD_INFINIBAND 32

void peerlink_set_allow_ipoib(int allow) { atomic_store(&g_allow_ipoib, allow ? 1 : 0); }

static int iface_arphrd(const char *ifname)
{
    int (*fn)(const char *) = __atomic_load_n(&peerlink_test_iftype, __ATOMIC_RELAXED);
    if (fn)
        return fn(ifname);
#ifdef __linux__
    char path[256], buf[32];
    if (snprintf(path, sizeof path, "/sys/class/net/%s/type", ifname) >= (int)sizeof path)
        return -1;
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return -1;
    ssize_t r = read(fd, buf, sizeof buf - 1);
    close(fd);
    if (r <= 0)
        return -1;
    buf[r] = 0;
    return atoi(buf);
#else
    (void)ifname;
    return -1;
#endif
}

static int sa_same_ip(const struct sockaddr *a, const struct sockaddr *b)
{
    if (!a || !b)
        return 0;
    if (a->sa_family == AF_INET && b->sa_family == AF_INET)
        return memcmp(&((const struct sockaddr_in *)a)->sin_addr, &((const struct sockaddr_in *)b)->sin_addr, 4) == 0;
    if (a->sa_family == AF_INET6 && b->sa_family == AF_INET6) {
        const struct sockaddr_in6 *a6 = (const struct sockaddr_in6 *)a;
        const struct sockaddr_in6 *b6 = (const struct sockaddr_in6 *)b;
        if (memcmp(&a6->sin6_addr, &b6->sin6_addr, 16) != 0)
            return 0;
        if (IN6_IS_ADDR_LINKLOCAL(&a6->sin6_addr) && a6->sin6_scope_id && b6->sin6_scope_id &&
            a6->sin6_scope_id != b6->sin6_scope_id)
            return 0;
        return 1;
    }
    if (a->sa_family == AF_INET6 && b->sa_family == AF_INET) {
        const struct in6_addr *v6 = &((const struct sockaddr_in6 *)a)->sin6_addr;
        if (IN6_IS_ADDR_V4MAPPED(v6))
            return memcmp(v6->s6_addr + 12, &((const struct sockaddr_in *)b)->sin_addr, 4) == 0;
    }
    if (a->sa_family == AF_INET && b->sa_family == AF_INET6) {
        const struct in6_addr *v6 = &((const struct sockaddr_in6 *)b)->sin6_addr;
        if (IN6_IS_ADDR_V4MAPPED(v6))
            return memcmp(v6->s6_addr + 12, &((const struct sockaddr_in *)a)->sin_addr, 4) == 0;
    }
    return 0;
}

int peerlink_check_ipoib_sa(const struct sockaddr *sa, char *err, size_t errlen)
{
    if (atomic_load(&g_allow_ipoib) || peerlink_mutant == PL_MUT_NOIPOIB)
        return 0;
    if (!sa || (sa->sa_family != AF_INET && sa->sa_family != AF_INET6))
        return 0;
    struct ifaddrs *iflist = NULL;
    if (getifaddrs(&iflist) != 0)
        return 0;
    int bad = 0;
    for (struct ifaddrs *ifa = iflist; ifa; ifa = ifa->ifa_next) {
        if (!ifa->ifa_addr || !ifa->ifa_name || !sa_same_ip(sa, ifa->ifa_addr))
            continue;
        if (iface_arphrd(ifa->ifa_name) == PL_ARPHRD_INFINIBAND) {
            if (err && errlen)
                snprintf(err, errlen,
                         "peer address uses IPoIB interface %s (ARPHRD_INFINIBAND): TCP over IPoIB is refused "
                         "unless cluster.peer.allow_ipoib=true (--peer-allow-ipoib)",
                         ifa->ifa_name);
            bad = 1;
            break;
        }
    }
    freeifaddrs(iflist);
    return bad ? -1 : 0;
}

int peerlink_check_ipoib_fd(int fd, char *err, size_t errlen)
{
    if (atomic_load(&g_allow_ipoib) || peerlink_mutant == PL_MUT_NOIPOIB)
        return 0;
    struct sockaddr_storage ss;
    socklen_t sl = sizeof ss;
    if (getsockname(fd, (struct sockaddr *)&ss, &sl) != 0)
        return 0;
    return peerlink_check_ipoib_sa((const struct sockaddr *)&ss, err, errlen);
}

int peerlink_check_ipoib_addr(const char *addr, char *err, size_t errlen)
{
    if (!addr || !*addr || atomic_load(&g_allow_ipoib) || peerlink_mutant == PL_MUT_NOIPOIB)
        return 0;
    char host[256];
    size_t n = strlen(addr);
    if (n >= sizeof host)
        return 0;
    memcpy(host, addr, n + 1);
    if (host[0] == '[') {
        char *rb = strchr(host, ']');
        if (!rb)
            return 0;
        size_t hl = (size_t)(rb - host - 1);
        memmove(host, host + 1, hl);
        host[hl] = 0;
    } else {
        char *c1 = strchr(host, ':'), *c2 = strrchr(host, ':');
        if (c1 && c1 == c2)
            *c1 = 0; /* strip :PORT from IPv4 or hostname (bare IPv6 has >= 2 colons) */
    }
    if (!*host)
        return 0;
    unsigned scope_id = 0;
    char *pct = strchr(host, '%');
    if (pct) {
        const char *zone = pct + 1;
        if (*zone) {
            char ifb[IF_NAMESIZE];
            const char *ifname = zone;
            int arphrd = iface_arphrd(ifname);
            if (arphrd < 0) {
                char *end = NULL;
                unsigned long idx = strtoul(zone, &end, 10);
                if (end != zone && !*end && idx > 0 && idx <= UINT_MAX && if_indextoname((unsigned)idx, ifb)) {
                    ifname = ifb;
                    arphrd = iface_arphrd(ifname);
                }
            }
            if (arphrd == PL_ARPHRD_INFINIBAND) {
                if (err && errlen)
                    snprintf(err, errlen,
                             "peer address uses IPoIB interface %s (ARPHRD_INFINIBAND): TCP over IPoIB is refused "
                             "unless cluster.peer.allow_ipoib=true (--peer-allow-ipoib)",
                             ifname);
                return -1;
            }
            scope_id = if_nametoindex(ifname);
        }
        *pct = 0;
    }
    struct sockaddr_in sa4 = {.sin_family = AF_INET};
    if (inet_pton(AF_INET, host, &sa4.sin_addr) == 1)
        return peerlink_check_ipoib_sa((const struct sockaddr *)&sa4, err, errlen);
    struct sockaddr_in6 sa6 = {.sin6_family = AF_INET6, .sin6_scope_id = scope_id};
    if (inet_pton(AF_INET6, host, &sa6.sin6_addr) == 1)
        return peerlink_check_ipoib_sa((const struct sockaddr *)&sa6, err, errlen);
    struct addrinfo hints = {.ai_family = AF_UNSPEC, .ai_socktype = SOCK_STREAM};
    struct addrinfo *ai = NULL;
    if (getaddrinfo(host, NULL, &hints, &ai) != 0 || !ai)
        return 0;
    int bad = 0;
    for (struct addrinfo *p = ai; p; p = p->ai_next) {
        if (peerlink_check_ipoib_sa(p->ai_addr, err, errlen) != 0) {
            bad = 1;
            break;
        }
    }
    freeaddrinfo(ai);
    return bad ? -1 : 0;
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
    char ierr[256];
    if (peerlink_check_ipoib_fd(fd, ierr, sizeof ierr) != 0) {
        fprintf(stderr, "peerlink: dial %s refused: %s\n", addr, ierr);
        close(fd);
        return -1;
    }
    return fd;
}

/* ---- the handshake */

/* ---- the TCP transport (pltransport.h): a connection's socket once its handshake passed, its bytes exactly as
 * before the seam (one read, one write per call) */

struct pl_tcp {
    struct pl_transport t;
    int fd;
};

static ssize_t tcp_read(struct pl_transport *t, void *p, size_t n) { return read(((struct pl_tcp *)t)->fd, p, n); }

static ssize_t tcp_writev(struct pl_transport *t, const struct iovec *iov, int n)
{
    int fd = ((struct pl_tcp *)t)->fd;
    return n == 1 ? write(fd, iov[0].iov_base, iov[0].iov_len) : writev(fd, iov, n);
}

static int tcp_poll_fds(struct pl_transport *t, int want, struct pollfd *pf, int *ready)
{
    pf[0] = (struct pollfd){((struct pl_tcp *)t)->fd,
                            (short)((want & PL_TR_IN ? POLLIN : 0) | (want & PL_TR_OUT ? POLLOUT : 0)), 0};
    *ready = 0; /* the kernel holds the bytes: poll says */
    return 1;
}

static int tcp_poll_ready(struct pl_transport *t, const struct pollfd *pf, int k)
{
    (void)t;
    (void)k;
    return (pf[0].revents & (POLLIN | POLLHUP | POLLERR) ? PL_TR_IN : 0) | (pf[0].revents & POLLOUT ? PL_TR_OUT : 0);
}

static void tcp_close(struct pl_transport *t)
{
    close(((struct pl_tcp *)t)->fd);
    free(t);
}

static const struct pl_tr_ops tcp_ops = {"tcp", tcp_read, tcp_writev, tcp_poll_fds, tcp_poll_ready, tcp_close};

/* the transport of a socket whose handshake passed; it owns fd from here on */
static struct pl_transport *tcp_new(int fd, int sock_buf)
{
    struct pl_tcp *t = calloc(1, sizeof *t);
    if (!t)
        oom();
    t->t.ops = &tcp_ops;
    t->fd = fd;
    /* no TCP_USER_TIMEOUT: Linux counts bytes held back by a shut window as unacknowledged, which would end a link
     * whose peer is merely slow. A silent idle peer is found by PING and dead_ms; a host that vanishes while we are held
     * back by it, by the kernel's own retransmission limit. */
    if (sock_buf > 0) {
        setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sock_buf, sizeof sock_buf);
        setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &sock_buf, sizeof sock_buf);
    }
    return &t->t;
}

struct hello {
    uint32_t id;
    uint8_t uuid[16], ident[16], nc[32];
    int kind;
    unsigned checks; /* the frame check types the dialer can run, bit (1 << type) each (plcheck.h) */
    unsigned caps;   /* the dialer's PL_CAP_* bits (unknown ones ignored) */
};

/* the HELLO body of a data link: magic, version, id, uuid, ident, nonce, address (u16 length + bytes), kind, the
 * check types offered (u8 bit set, from cluster version 8), the capability bits (u8, PL_CAP_*); -1 when it is not one, -2 when it is one of another cluster
 * version (peerlink_adopt says so; peerlink_hello_kind, which the fuzzer reaches, says nothing). */
static int hello_parse(const uint8_t *p, size_t n, struct hello *h)
{
    if (n < 4 + 1 + 4 + 16 + 16 + 32 + 2 + 1 + 1 + 1 || le32(p) != PL_CL_MAGIC)
        return -1;
    if (p[4] != PL_CL_VERSION)
        return -2;
    h->id = le32(p + 5);
    memcpy(h->uuid, p + 9, 16);
    memcpy(h->ident, p + 25, 16);
    memcpy(h->nc, p + 41, 32);
    size_t al = (size_t)p[73] | (size_t)p[74] << 8;
    if (n != 75 + al + 3 || memchr(p + 75, 0, al))
        return -1;
    h->kind = p[75 + al];
    h->checks = p[76 + al];
    h->caps = p[77 + al];
    return 0;
}

void peerlink_set_check_level(int level) { atomic_store(&check_default, level); }
int peerlink_check_parse(const char *s) { return pl_check_parse(s); }
const char *peerlink_check_name(int type) { return pl_check_name(type); }

static _Atomic uint64_t g_up[PL_NKINDS][4];    /* the process's connections up by kind and check type */
static _Atomic uint64_t g_up_tr[PL_NKINDS][2]; /* the process's connections up by kind and transport (0: tcp, 1: rdma) */
static _Atomic uint64_t g_rdma_fb[PL_RDMA_FB_MAX];

static const char *const kname_tab[PL_NKINDS] = {"meta", "log", "bulk", "fwd", "fwdq", "snap", "chunk", "chunkb"};

static const char *const rdma_fb_names[PL_RDMA_FB_MAX] = {
    [PL_RDMA_FB_NOT_BUILT] = "not_built",
    [PL_RDMA_FB_OFF] = "off",
    [PL_RDMA_FB_KIND] = "kind",
    [PL_RDMA_FB_SUBNET] = "subnet",
    [PL_RDMA_FB_POOL] = "pool",
    [PL_RDMA_FB_QP] = "qp",
    [PL_RDMA_FB_TIMEOUT] = "timeout",
    [PL_RDMA_FB_AUTH] = "auth",
    [PL_RDMA_FB_QP_ERROR] = "qp_error",
    [PL_RDMA_FB_BACKOFF] = "backoff",
    [PL_RDMA_FB_BAD] = "bad",
};

void peerlink_set_rdma(const struct pl_rdma_cfg *cfg)
{
    pthread_mutex_lock(&g_rdma_mu);
    if (cfg)
        g_rdma = *cfg;
    else
        memset(&g_rdma, 0, sizeof g_rdma);
    pthread_mutex_unlock(&g_rdma_mu);
}

static void rdma_cfg_get(const struct peerlink *pl, struct pl_rdma_cfg *out)
{
    if (pl->cfg.has_rdma) {
        *out = pl->cfg.rdma;
    } else {
        pthread_mutex_lock(&g_rdma_mu);
        *out = g_rdma;
        pthread_mutex_unlock(&g_rdma_mu);
    }
    if (!out->kinds)
        out->kinds = ((1u << (PL_KIND_DATA_MAX + 1)) - 1u) & ~1u; /* all data kinds 1..7, never META (0) */
}

static void up_count(int kind, int type, int d)
{
    if (kind >= 0 && kind < PL_NKINDS && type >= 1 && type <= 3)
        atomic_fetch_add_explicit(&g_up[kind][type], (uint64_t)(int64_t)d, memory_order_relaxed);
}

static void up_tr_count(int kind, int rdma, int d)
{
    if (kind >= 0 && kind < PL_NKINDS)
        atomic_fetch_add_explicit(&g_up_tr[kind][rdma ? 1 : 0], (uint64_t)(int64_t)d, memory_order_relaxed);
}

static void fb_count(int reason)
{
    if (reason >= 0 && reason < PL_RDMA_FB_MAX)
        atomic_fetch_add_explicit(&g_rdma_fb[reason], 1, memory_order_relaxed);
}

void peerlink_up_by_check(uint64_t out[PL_NKINDS][4])
{
    for (int k = 0; k < PL_NKINDS; k++)
        for (int t = 0; t < 4; t++)
            out[k][t] = atomic_load_explicit(&g_up[k][t], memory_order_relaxed);
}

void peerlink_up_by_transport(uint64_t out[PL_NKINDS][2])
{
    for (int k = 0; k < PL_NKINDS; k++)
        for (int t = 0; t < 2; t++)
            out[k][t] = atomic_load_explicit(&g_up_tr[k][t], memory_order_relaxed);
}

void peerlink_rdma_fallbacks(uint64_t out[PL_RDMA_FB_MAX])
{
    for (int i = 0; i < PL_RDMA_FB_MAX; i++)
        out[i] = atomic_load_explicit(&g_rdma_fb[i], memory_order_relaxed);
}

const char *peerlink_rdma_fb_name(int reason)
{
    return reason >= 0 && reason < PL_RDMA_FB_MAX && rdma_fb_names[reason] ? rdma_fb_names[reason] : "unknown";
}

int peerlink_links_status(char *buf, size_t n)
{
    uint64_t up[PL_NKINDS][4], tr[PL_NKINDS][2];
    peerlink_up_by_check(up);
    peerlink_up_by_transport(tr);
    size_t off = 0;
    if (n)
        buf[0] = 0;
    for (int k = 0; k < PL_NKINDS; k++) {
        if (!up[k][1] && !up[k][2] && !up[k][3])
            continue;
        int w = snprintf(buf + off, n - off,
                         "peer links %s: none %" PRIu64 " header %" PRIu64 " full %" PRIu64 " tcp %" PRIu64
                         " rdma %" PRIu64 "\n",
                         kname_tab[k], up[k][1], up[k][2], up[k][3], tr[k][0], tr[k][1]);
        if (w < 0 || (size_t)w >= n - off)
            return -1;
        off += (size_t)w;
    }
    return 0;
}

/* this end's frame check level (cfg.check, else the process's; plcheck.h) */
static int check_level(const struct peerlink *pl)
{
    int d = atomic_load(&check_default);
    return pl->cfg.check ? pl->cfg.check : d ? d : PL_CHECK_NONE;
}

/* the acceptor's choice among the check types a HELLO offers: the lowest it offers that this end runs at or above its
 * own level, so the stronger of the two ends' levels (each end offers the types at or above its own); 0 when none */
static int check_choose(unsigned offered, int level)
{
    unsigned both = offered & pl_check_offer(level);
    if (peerlink_mutant == PL_MUT_CHECKTYPE && !both)
        return level;
    for (int t = 1; t <= 7; t++)
        if (both & (1u << t))
            return t;
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
    struct fck in, out;
    uint8_t rdma_key[32]; /* connection-scoped HMAC key for RDMA_HELLO / RDMA_READY (design-rdma.md §5.1 step 7) */
    uint32_t peer;
    unsigned peer_caps; /* the PL_CAP_* bits the peer's HELLO or HELLO_ACK carried */
    int try_rdma;       /* dialer sent PL_CAP_RDMA_TRY and both sides speak PL_CAP_RDMA */
};

/* each direction's frame check: the negotiated type and a salt from the transcript under the secret (plcheck.h), so
 * that a frame of another connection fails its check here. A keyed type would take its key from the same place. */
static void keys_derive(const struct peerlink *pl, int dialer, int check, const uint8_t *tr, size_t trlen,
                        struct keys *k)
{
    uint8_t ko[32], ki[32];
    int same = peerlink_mutant == PL_MUT_SAMESALT;
    if (peerlink_mutant == PL_MUT_NOTRANSCRIPT)
        trlen = 0;
    hs_mac(pl->secret, pl->secret_len, dialer || same ? "tsdb-link-x>" : "tsdb-link-x<", tr, trlen, ko);
    hs_mac(pl->secret, pl->secret_len, dialer && !same ? "tsdb-link-x<" : "tsdb-link-x>", tr, trlen, ki);
    hs_mac(pl->secret, pl->secret_len, "tsdb-link-rdma", tr, trlen, k->rdma_key);
    k->out = (struct fck){check, le64(ko)};
    k->in = (struct fck){check, le64(ki)};
    if (peerlink_test_salts)
        peerlink_test_salts(dialer, k->out.salt, k->in.salt);
    tsdb_wipe(ko, 32);
    tsdb_wipe(ki, 32);
}

static int dial_can_try_rdma(const struct plink *l, const struct pl_rdma_cfg *rc, uint64_t now)
{
    if (!l->dialed || l->kind <= PL_KIND_META || l->kind > PL_KIND_DATA_MAX)
        return 0;
    if (rc->mode == PL_RDMA_OFF || rc->npools <= 0)
        return 0;
    if (rc->cm && !rc->cm_ops)
        return 0;
    if (!(rc->kinds & (1u << l->kind)))
        return 0;
    if (rc->mode != PL_RDMA_ON && peerlink_mutant != PL_MUT_RDMA_NOBACKOFF && l->rdma_until_ns && now < l->rdma_until_ns)
        return 0;
    return 1;
}

static int hs_dial(struct plink *l, int fd, struct keys *k)
{
    struct peerlink *pl = l->pl;
    uint64_t now = now_ns();
    uint64_t dl = now + (uint64_t)(pl->cfg.hs_ms ? pl->cfg.hs_ms : 5000) * MS;
    uint8_t tr[HS_TR_MAX], body[HS_FRAME_MAX], mac[32], want[32], type;
    size_t trlen = 0, n;
    struct pl_rdma_cfg rc;
    rdma_cfg_get(pl, &rc);
    int want_try = dial_can_try_rdma(l, &rc, now);
    unsigned my_caps = PL_CAPS | (want_try ? PL_CAP_RDMA_TRY : 0u);
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
    unsigned offer = pl_check_offer(check_level(pl));
    put8(&b, (uint8_t)offer);
    put8(&b, (uint8_t)my_caps);
    memcpy(tr, b.p, b.len);
    trlen = b.len;
    int s_rc = send_hs(fd, F_HELLO, b.p, b.len, dl);
    buf_free(&b);
    if (s_rc != 0)
        return -1;
    if (read_hs(pl, fd, dl, &type, body, &n) != 0 || type != F_HELLO_ACK || n != 4 + 16 + 16 + 32 + 1 + 1 + 32)
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
    int check = body[4 + 16 + 16 + 32];
    if ((check < 1 || check > 7 || !(offer & (1u << check))) && peerlink_mutant != PL_MUT_DIALANYTYPE) {
        fprintf(stderr, "peerlink: node %" PRIu32 " chose frame check type %d, which this server does not run at "
                "its level %s (it offered 0x%x)\n", id, check, pl_check_name(check_level(pl)), offer);
        return -1;
    }
    memcpy(tr + trlen, body + n - 32, 32);
    trlen += 32;
    hs_mac(pl->secret, pl->secret_len, "tsdb-link-c", tr, trlen, mac);
    if (send_hs(fd, F_AUTH, mac, 32, dl) != 0)
        return -1;
    keys_derive(pl, 1, check, tr, trlen, k);
    k->peer = id;
    k->peer_caps = body[4 + 16 + 16 + 32 + 1];
    k->try_rdma = want_try && (k->peer_caps & PL_CAP_RDMA);
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
    int check = check_choose(h.checks, check_level(pl));
    if (!check) {
        fprintf(stderr, "peerlink: refused a link from node %" PRIu32 ": it offers frame check types 0x%x, this server "
                "runs 0x%x at its level %s\n", h.id, h.checks, pl_check_offer(check_level(pl)),
                pl_check_name(check_level(pl)));
        return -1;
    }
    memcpy(tr, hello, hn);
    size_t trlen = hn;
    tsdb_random(ns, sizeof ns);
    struct buf b = {0};
    put32(&b, self->id);
    put(&b, self->uuid, 16);
    put(&b, self->ident, 16);
    put(&b, ns, 32);
    put8(&b, (uint8_t)check);
    put8(&b, (uint8_t)PL_CAPS);
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
    keys_derive(pl, 0, check, tr, trlen, k);
    k->peer = h.id;
    k->peer_caps = h.caps;
    k->try_rdma = !!(h.caps & PL_CAP_RDMA_TRY);
    return 0;
}

/* ---- one connection */

enum { J_WAITRESUME = 1, J_HASH, J_SEND, J_WAITDONE };
enum { HP_RX = 1, HP_TX };
enum {
    UP_IDLE = 0,
    UP_D_WAIT_ACCEPT, /* dialer sent OFFER on TCP; waiting for ACCEPT or DECLINE */
    UP_D_WAIT_CM,     /* dialer dialing rdma_cm (resolve_addr -> resolve_route -> connect -> ESTABLISHED) */
    UP_D_WAIT_READY,  /* dialer sent HELLO on QP; waiting for READY on QP */
    UP_A_WAIT_OFFER,  /* acceptor saw PL_CAP_RDMA_TRY; waiting for initial OFFER or ABORT on TCP */
    UP_A_WAIT_CM,     /* acceptor sent CM ACCEPT on TCP; waiting for authenticated CONNECT_REQUEST or ABORT */
    UP_A_WAIT_CM_EST, /* acceptor called rdma_accept; waiting for ESTABLISHED or ABORT on TCP */
    UP_A_WAIT_HELLO,  /* acceptor sent ACCEPT on TCP (or CM established); waiting for HELLO on QP or ABORT on TCP */
    UP_A_WAIT_GO,     /* acceptor sent READY on QP; waiting for GO on QP or ABORT on TCP */
};

struct dctl {
    struct dctl *next;
    uint8_t type;
    size_t n;
    uint8_t p[];
};

struct conn {
    struct plink *l;
    struct peerlink *pl;
    struct pl_transport *t; /* the bytes of the frames go here and come from here, nowhere else (pltransport.h) */
    int dead;
    uint64_t my_seq; /* the adoption this thread serves (accepted links) */
    struct keys keys;
    uint64_t seq_out;
    struct pl_reader *rd;
    struct buf wb;
    ZSTD_CCtx *zc;
    uint64_t last_rx, last_ping;
    uint8_t *cbuf; /* a chunk's read buffer */

    /* RDMA upgrade state machine (docs/design-rdma.md §5.1, §5.3) */
    struct pl_rdma_cfg rcfg;
    int up_state, settled, on_rdma, rdma_final_decline, rdma_stable_reset;
    int up_aborted_wait, up_declined_wait, up_fail_after_hello;
    struct plr_conn *up_conn;
    struct pl_transport *up_t;
    struct plr_pool *up_pool;
    struct plr_ep up_peer_ep;
    struct plr_cm_id *up_cm_id;      /* dialer's cm_id before plr_conn_new_cm takes ownership */
    struct plr_cm_token *up_cm_tok;  /* acceptor's pending auth token on cm_lis */
    uint8_t up_cm_pd[PLR_CM_PD_LEN]; /* dialer's private_data for rdma_connect */
    int up_cm_mode;                  /* 1: current upgrade attempt uses rdma_cm (Option C) */
    struct buf up_wb, up_rb;
    uint64_t up_nonce, up_self_seq, up_peer_seq, up_deadline, up_ready_at;
    uint64_t rdma_settled_ns, next_reoffer_ns;
    struct dctl *dctl_h, *dctl_t; /* control frames deferred while an upgrade is in flight */

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

/* A status in RESUME or DONE is PL_OK or one of the PL_E_* codes. The frames carry no check of their own by default
 * (plcheck.h): a flipped bit that makes any other value ends the connection, and the file begins again on the next one,
 * instead of reaching `done` as a status no caller knows. */
static int status_known(int32_t s)
{
    if (peerlink_mutant == PL_MUT_DONESTATUS)
        return 1;
    return s <= PL_OK && (s >= PL_E_GIVEUP || peerlink_mutant == PL_MUT_STATUSNEG);
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

static void send_ctl_now(struct conn *c, uint8_t type, const void *p, size_t n)
{
    frame_put(&c->wb, &c->keys.out, &c->seq_out, type, "", 0, p, n, NULL, 0);
}

static void send_ctl(struct conn *c, uint8_t type, const void *p, size_t n)
{
    if (c->up_state != UP_IDLE) {
        struct dctl *d = malloc(sizeof *d + n);
        if (!d)
            oom();
        d->next = NULL;
        d->type = type;
        d->n = n;
        if (n)
            memcpy(d->p, p, n);
        if (c->dctl_t)
            c->dctl_t->next = d;
        else
            c->dctl_h = d;
        c->dctl_t = d;
        return;
    }
    send_ctl_now(c, type, p, n);
}

static void dctl_flush(struct conn *c)
{
    struct dctl *d = c->dctl_h;
    c->dctl_h = c->dctl_t = NULL;
    while (d) {
        struct dctl *nx = d->next;
        send_ctl_now(c, d->type, d->p, d->n);
        free(d);
        d = nx;
    }
}

static void dctl_free(struct conn *c)
{
    struct dctl *d = c->dctl_h;
    c->dctl_h = c->dctl_t = NULL;
    while (d) {
        struct dctl *nx = d->next;
        free(d);
        d = nx;
    }
}

/* ---- RDMA upgrade helpers (docs/design-rdma.md §5.1, §5.3) */

static void up_rdma_mac(const uint8_t key[32], uint8_t type, uint64_t seq, uint64_t nonce, uint8_t out[32])
{
    uint8_t msg[1 + 8 + 8];
    msg[0] = type;
    for (int i = 0; i < 8; i++) {
        msg[1 + i] = (uint8_t)(seq >> (8 * i));
        msg[9 + i] = (uint8_t)(nonce >> (8 * i));
    }
    struct hk hk;
    hk_init(&hk, key, 32);
    hk_mac(&hk, msg, sizeof msg, "", 0, "", 0, "", 0, out);
    tsdb_wipe(&hk, sizeof hk);
}

/* Connection-derived HMAC tag carried in rdma_conn_param.private_data (Option C, design-rdma.md §5.5). */
static void up_rdma_cm_mac(const uint8_t key[32], uint64_t offer_seq, uint64_t nonce, uint8_t out[32])
{
    static const char dom[] = "tsdb-rdma-cm-req";
    uint8_t msg[16];
    for (int i = 0; i < 8; i++) {
        msg[i] = (uint8_t)(offer_seq >> (8 * i));
        msg[8 + i] = (uint8_t)(nonce >> (8 * i));
    }
    struct hk hk;
    hk_init(&hk, key, 32);
    hk_mac(&hk, dom, sizeof dom - 1, msg, sizeof msg, "", 0, "", 0, out);
    tsdb_wipe(&hk, sizeof hk);
}

static void up_cm_build_pd(const struct conn *c, uint8_t out[PLR_CM_PD_LEN])
{
    uint32_t magic = PLR_CM_PD_MAGIC;
    for (int i = 0; i < 4; i++)
        out[i] = (uint8_t)(magic >> (8 * i));
    for (int i = 0; i < 8; i++)
        out[4 + i] = (uint8_t)(c->up_nonce >> (8 * i));
    up_rdma_cm_mac(c->keys.rdma_key, c->up_self_seq, c->up_nonce, out + 12);
    int bp = __atomic_load_n(&peerlink_test_rdma_cm_bad_pd, __ATOMIC_RELAXED);
    if (bp == 1)
        out[12] ^= 0xff; /* corrupt HMAC tag */
    else if (bp == 2)
        out[4] ^= 0x01; /* corrupt nonce */
}

static int cm_ip_is_zero(const uint8_t ip[16])
{
    for (int i = 0; i < 16; i++)
        if (ip[i])
            return 0;
    return 1;
}

static void cm_fill_ip_from_tcp(const struct pl_transport *t, int peer, struct plr_cm_addr *a)
{
    if (!t || t->ops != &tcp_ops)
        return;
    int fd = ((const struct pl_tcp *)t)->fd;
    struct sockaddr_storage ss;
    socklen_t sl = sizeof ss;
    if ((peer ? getpeername(fd, (struct sockaddr *)&ss, &sl) : getsockname(fd, (struct sockaddr *)&ss, &sl)) != 0)
        return;
    if (ss.ss_family == AF_INET) {
        a->af = 4;
        memset(a->ip, 0, 16);
        memcpy(a->ip, &((const struct sockaddr_in *)&ss)->sin_addr, 4);
    } else if (ss.ss_family == AF_INET6) {
        const struct in6_addr *v6 = &((const struct sockaddr_in6 *)&ss)->sin6_addr;
        if (IN6_IS_ADDR_V4MAPPED(v6)) {
            a->af = 4;
            memset(a->ip, 0, 16);
            memcpy(a->ip, v6->s6_addr + 12, 4);
        } else {
            a->af = 6;
            memcpy(a->ip, v6, 16);
        }
    }
}

static void cm_wake_cb(void *arg) { wake((struct plink *)arg); }

static void up_cleanup(struct conn *c)
{
    if (c->up_cm_tok) {
        plr_cm_cancel(c->rcfg.cm_lis, c->up_cm_tok);
        c->up_cm_tok = NULL;
    }
    if (c->up_cm_id) {
        c->rcfg.cm_ops->destroy(c->up_cm_id);
        c->up_cm_id = NULL;
    }
    if (c->up_t) {
        c->up_t->ops->close(c->up_t); /* anchor is NULL, so c->t (TCP) is untouched */
        c->up_t = NULL;
        c->up_conn = NULL;
    } else if (c->up_conn) {
        plr_conn_free(c->up_conn);
        c->up_conn = NULL;
    }
    c->up_pool = NULL;
    c->up_cm_mode = 0;
    tsdb_wipe(c->up_cm_pd, sizeof c->up_cm_pd);
    c->up_ready_at = 0;
    c->up_fail_after_hello = 0;
    buf_free(&c->up_wb);
    buf_free(&c->up_rb);
}

static void conn_settle(struct conn *c, int rdma)
{
    struct plink *l = c->l;
    struct peerlink *pl = c->pl;
    int was_rdma = c->on_rdma;
    c->on_rdma = rdma;
    if (!c->settled) {
        c->settled = 1;
        pthread_mutex_lock(&l->mu);
        l->up = 1;
        l->gen++;
        c->gen = l->gen;
        l->io_thr = pthread_self();
        l->io_set = 1;
        atomic_store(&l->peer, c->keys.peer);
        atomic_store(&l->check, c->keys.out.type);
        atomic_store(&l->transport, rdma ? 2 : 1);
        pthread_mutex_unlock(&l->mu);
        ST(pl, connects, 1);
        if (c->keys.out.type >= 1 && c->keys.out.type <= 3)
            ST(pl, up_check[c->keys.out.type], 1);
        up_count(l->kind, c->keys.out.type, 1);
        up_tr_count(l->kind, rdma, 1);
        if (pl->cfg.on_link)
            pl->cfg.on_link(pl->cfg.arg, l, 1);
        pthread_mutex_lock(&l->mu);
        l->open = 1; /* the driver's own threads may send from here on (their frames follow on_link's) */
        pthread_mutex_unlock(&l->mu);
    } else if (rdma != was_rdma) {
        up_tr_count(l->kind, was_rdma, -1);
        up_tr_count(l->kind, rdma, 1);
        atomic_store(&l->transport, rdma ? 2 : 1);
    }
    dctl_flush(c);
}

static int acc_requires_rdma(const struct conn *c)
{
    return !c->l->dialed && c->rcfg.mode == PL_RDMA_ON && c->l->kind > PL_KIND_META &&
           c->l->kind <= PL_KIND_DATA_MAX && (c->rcfg.kinds & (1u << c->l->kind));
}

static int decl_to_fb(int reason)
{
    return reason == PL_RDMA_DECL_NOT_BUILT ? PL_RDMA_FB_NOT_BUILT
           : reason == PL_RDMA_DECL_OFF     ? PL_RDMA_FB_OFF
           : reason == PL_RDMA_DECL_KIND    ? PL_RDMA_FB_KIND
           : reason == PL_RDMA_DECL_SUBNET  ? PL_RDMA_FB_SUBNET
           : reason == PL_RDMA_DECL_POOL    ? PL_RDMA_FB_POOL
           : reason == PL_RDMA_DECL_QP      ? PL_RDMA_FB_QP
                                            : PL_RDMA_FB_BAD;
}

static void up_record_fail(struct conn *c, int fb_reason)
{
    fb_count(fb_reason);
    if (!c->l->dialed || c->rcfg.mode == PL_RDMA_ON)
        return;
    struct plink *l = c->l;
    l->rdma_fails++;
    if (l->rdma_fails >= 3 && peerlink_mutant != PL_MUT_RDMA_NOBACKOFF) {
        uint64_t bmin = (uint64_t)(c->rcfg.backoff_min_ms ? c->rcfg.backoff_min_ms : 60000u) * MS;
        uint64_t bmax = (uint64_t)(c->rcfg.backoff_max_ms ? c->rcfg.backoff_max_ms : 600000u) * MS;
        uint64_t prev = l->rdma_backoff_ns;
        l->rdma_backoff_ns = prev ? (prev * 2 > bmax ? bmax : prev * 2) : bmin;
        l->rdma_until_ns = now_ns() + l->rdma_backoff_ns;
        if (!l->rdma_backed_off || l->rdma_backoff_ns != prev)
            fprintf(stderr, "peerlink: rdma: peer %" PRIu32 " kind %s: %d failures in a row (%s), staying on TCP for %" PRIu64 " ms\n",
                    c->keys.peer, kname_tab[l->kind], l->rdma_fails, peerlink_rdma_fb_name(fb_reason),
                    (uint64_t)(l->rdma_backoff_ns / MS));
        l->rdma_backed_off = 1;
    }
}

static void up_schedule_reoffer(struct conn *c, uint64_t now)
{
    c->next_reoffer_ns = 0;
    if (!c->l->dialed || c->rdma_final_decline || c->rcfg.mode == PL_RDMA_OFF || c->rcfg.npools <= 0 ||
        !(c->rcfg.kinds & (1u << c->l->kind)) || !(c->keys.peer_caps & PL_CAP_RDMA) ||
        c->rcfg.retry_ms == UINT64_MAX)
        return;
    uint64_t rms = c->rcfg.retry_ms ? c->rcfg.retry_ms : 600000ull;
    uint64_t retry_ns = rms > (UINT64_MAX - now) / MS ? UINT64_MAX - now : rms * MS;
    uint64_t at = now + retry_ns;
    if (peerlink_mutant != PL_MUT_RDMA_NOBACKOFF && c->l->rdma_until_ns > at)
        at = c->l->rdma_until_ns;
    c->next_reoffer_ns = at;
}

static void up_dial_abort(struct conn *c, int fb_reason, int send_abort)
{
    int was_wait_accept = c->up_state == UP_D_WAIT_ACCEPT;
    up_cleanup(c);
    up_record_fail(c, fb_reason);
    if (c->rcfg.mode == PL_RDMA_ON || (c->rcfg.cm && peerlink_mutant == PL_MUT_RDMA_CM_NOFALLBACK)) {
        fprintf(stderr, "peerlink: rdma: mode on: peer %" PRIu32 " kind %s upgrade failed (%s): ending connection\n",
                c->keys.peer, kname_tab[c->l->kind], peerlink_rdma_fb_name(fb_reason));
        c->up_state = UP_IDLE;
        c->dead = 1;
        return;
    }
    if (send_abort) {
        send_ctl_now(c, PLT_RDMA_ABORT, "", 0);
        if (was_wait_accept)
            c->up_aborted_wait = 1;
    }
    c->up_state = UP_IDLE;
    up_schedule_reoffer(c, now_ns());
    conn_settle(c, 0);
}

static void up_dial_start_offer(struct conn *c, int initial)
{
    char err[256] = "no RDMA pool";
    c->up_aborted_wait = 0;
    int np = c->rcfg.npools;
    int start = np > 0 ? c->l->rdma_pool_idx % np : 0;
    struct plr_ep ep;
    if (c->rcfg.cm) {
        struct plr_pool *pool = NULL;
        for (int i = 0; i < np && !pool; i++) {
            int idx = (start + i) % np;
            if (c->rcfg.pools[idx]) {
                pool = c->rcfg.pools[idx];
                c->l->rdma_pool_idx = idx;
            }
        }
        if (!pool || !c->rcfg.cm_ops) {
            up_dial_abort(c, !c->rcfg.cm_ops ? PL_RDMA_FB_NOT_BUILT : PL_RDMA_FB_POOL, initial && c->keys.try_rdma);
            return;
        }
        plr_pool_ep(pool, PLR_EP_F_CM, &ep);
        c->up_pool = pool;
        c->up_cm_mode = 1;
    } else {
        struct plr_conn *conn = NULL;
        for (int i = 0; i < np && !conn; i++) {
            int idx = (start + i) % np;
            if (c->rcfg.pools[idx]) {
                conn = plr_conn_new(c->rcfg.pools[idx], err, sizeof err);
                if (conn)
                    c->l->rdma_pool_idx = idx;
            }
        }
        if (!conn) {
            fprintf(stderr, "peerlink: rdma: %s\n", err);
            up_dial_abort(c, PL_RDMA_FB_POOL, initial && c->keys.try_rdma);
            return;
        }
        plr_conn_ep(conn, &ep);
        c->up_conn = conn;
        c->up_cm_mode = 0;
    }
    uint8_t payload[PLR_EP_LEN + 8];
    plr_ep_encode(&ep, payload);
    tsdb_random(&c->up_nonce, sizeof c->up_nonce);
    for (int i = 0; i < 8; i++)
        payload[PLR_EP_LEN + i] = (uint8_t)(c->up_nonce >> (8 * i));
    c->up_self_seq = c->seq_out;
    send_ctl_now(c, PLT_RDMA_OFFER, payload, sizeof payload);
    __atomic_add_fetch(&peerlink_test_rdma_offers, 1, __ATOMIC_RELAXED);
    uint64_t otmo = (uint64_t)(c->rcfg.offer_timeout_ms ? c->rcfg.offer_timeout_ms : 2000u) * MS;
    c->up_deadline = now_ns() + otmo;
    c->up_state = UP_D_WAIT_ACCEPT;
}

static void up_encode_qp_probe(struct conn *c, uint8_t type, uint64_t at_seq, uint64_t nonce)
{
    uint8_t payload[8 + 32];
    uint64_t wn = nonce;
    int ba = __atomic_load_n(&peerlink_test_rdma_bad_auth, __ATOMIC_RELAXED);
    if ((ba == 3 && type == PLT_RDMA_HELLO) || (ba == 4 && type == PLT_RDMA_READY))
        wn ^= 1;
    for (int i = 0; i < 8; i++)
        payload[i] = (uint8_t)(wn >> (8 * i));
    up_rdma_mac(c->keys.rdma_key, type, at_seq, nonce, payload + 8);
    if ((ba == 1 && type == PLT_RDMA_HELLO) || (ba == 2 && type == PLT_RDMA_READY))
        payload[8] ^= 0xff;
    uint64_t tmp_seq = at_seq;
    frame_put(&c->up_wb, &c->keys.out, &tmp_seq, type, "", 0, payload, sizeof payload, NULL, 0);
    if (peerlink_mutant == PL_MUT_RDMA_SEQBUMP)
        c->seq_out++; /* planted bug: HELLO/READY consumes a stream seq number */
}

static int conn_msg(void *arg, const struct pl_msg *m);

static int up_switch_to_rdma(struct conn *c, int send_go)
{
    if (c->wb.len != c->wb.off || c->rd->rb.len != c->rd->rb.off) {
        c->dead = 1;
        return -1;
    }
    c->wb.off = c->wb.len = 0;
    c->rd->rb.off = c->rd->rb.len = 0;
    plr_conn_set_anchor(c->up_conn, c->t);
    c->t = c->up_t;
    c->up_t = NULL;
    c->up_conn = NULL;
    c->up_pool = NULL;
    c->up_cm_mode = 0;
    tsdb_wipe(c->up_cm_pd, sizeof c->up_cm_pd);
    c->up_state = UP_IDLE;
    c->up_ready_at = 0;
    c->next_reoffer_ns = 0;
    c->rdma_settled_ns = now_ns();
    c->rdma_stable_reset = 0;
    if (c->up_wb.len > c->up_wb.off)
        put(&c->wb, c->up_wb.p + c->up_wb.off, c->up_wb.len - c->up_wb.off);
    buf_free(&c->up_wb);
    if (send_go)
        send_ctl_now(c, PLT_RDMA_GO, "", 0);
    conn_settle(c, 1);
    if (c->up_rb.len > c->up_rb.off) {
        size_t nl = c->up_rb.len - c->up_rb.off;
        uint8_t *tmp = malloc(nl);
        if (!tmp)
            oom();
        memcpy(tmp, c->up_rb.p + c->up_rb.off, nl);
        buf_free(&c->up_rb);
        int fr = pl_reader_feed(c->rd, tmp, nl, conn_msg, c);
        free(tmp);
        if (fr != 0) {
            c->dead = 1;
            return -1;
        }
    } else {
        buf_free(&c->up_rb);
    }
    return 0;
}

static int up_handle_offer(struct conn *c, const struct pl_msg *m)
{
    if (c->l->dialed || (c->up_state != UP_A_WAIT_OFFER && !(c->up_state == UP_IDLE && !c->on_rdma)))
        return -1;
    c->up_declined_wait = 0;
    int decl = 0;
    struct plr_ep peer_ep;
    struct plr_pool *pool = NULL;
    if (c->rcfg.mode == PL_RDMA_OFF)
        decl = PL_RDMA_DECL_OFF;
    else if (c->rcfg.test_not_built)
        decl = PL_RDMA_DECL_NOT_BUILT;
#ifndef TSDB_RDMA
    else if (c->rcfg.npools <= 0)
        decl = PL_RDMA_DECL_NOT_BUILT;
#endif
    else if (!(c->rcfg.kinds & (1u << c->l->kind)))
        decl = PL_RDMA_DECL_KIND;
    else if (c->rcfg.npools <= 0)
        decl = PL_RDMA_DECL_POOL;
    else if (plr_ep_decode(&peer_ep, m->data, m->data_len) != 0)
        decl = PL_RDMA_DECL_BAD;
    else {
        for (int i = 0; i < c->rcfg.npools && !pool; i++)
            if (c->rcfg.pools[i] && plr_port_subnet_match(plr_pool_port(c->rcfg.pools[i]), &peer_ep))
                pool = c->rcfg.pools[i];
        if (!pool)
            decl = PL_RDMA_DECL_SUBNET;
    }
    if (!decl) {
        int want_cm = !!(peer_ep.flags & PLR_EP_F_CM);
        if (want_cm) {
            if (!c->rcfg.cm)
                decl = PL_RDMA_DECL_OFF;
            else if (!c->rcfg.cm_ops || !c->rcfg.cm_lis)
                decl = PL_RDMA_DECL_QP;
            else {
                struct plr_cm_addr my_addr;
                plr_cm_listener_addr(c->rcfg.cm_lis, &my_addr);
                int af_mismatch = 0;
                if (cm_ip_is_zero(my_addr.ip)) {
                    uint8_t lis_af = my_addr.af;
                    const struct plr_port *pp = plr_pool_port(pool);
                    if (pp && pp->cm_af && (!lis_af || pp->cm_af == lis_af)) {
                        my_addr.af = pp->cm_af;
                        memcpy(my_addr.ip, pp->cm_ip, 16);
                    } else {
                        struct plr_cm_addr tcp_addr = my_addr;
                        cm_fill_ip_from_tcp(c->t, 0, &tcp_addr);
                        if (!lis_af || tcp_addr.af == lis_af)
                            my_addr = tcp_addr;
                        else
                            af_mismatch = 1;
                    }
                }
                if (af_mismatch) {
                    decl = PL_RDMA_DECL_SUBNET;
                } else {
                    uint64_t offer_seq = c->rd->seq - 1;
                    uint8_t want_tag[32];
                    up_rdma_cm_mac(c->keys.rdma_key, offer_seq, m->a, want_tag);
                    struct plr_cm_token *tok = plr_cm_expect(c->rcfg.cm_lis, m->a, want_tag, cm_wake_cb, c->l);
                    tsdb_wipe(want_tag, sizeof want_tag);
                    if (!tok) {
                        decl = PL_RDMA_DECL_QP;
                    } else {
                        struct plr_ep my_ep;
                        plr_pool_ep(pool, PLR_EP_F_CM, &my_ep);
                        uint8_t acc_bytes[PLR_EP_LEN + PLR_CM_ADDR_LEN];
                        plr_ep_encode(&my_ep, acc_bytes);
                        plr_cm_addr_encode(&my_addr, acc_bytes + PLR_EP_LEN);
                        c->up_cm_tok = tok;
                        c->up_pool = pool;
                        c->up_peer_ep = peer_ep;
                        c->up_peer_seq = offer_seq;
                        c->up_nonce = m->a;
                        c->up_self_seq = c->seq_out; /* seq of RDMA_ACCEPT */
                        c->up_cm_mode = 1;
                        send_ctl_now(c, PLT_RDMA_ACCEPT, acc_bytes, sizeof acc_bytes);
                        uint64_t atmo = (uint64_t)(c->rcfg.accept_timeout_ms ? c->rcfg.accept_timeout_ms : 4000u) * MS;
                        c->up_deadline = now_ns() + atmo;
                        c->up_state = UP_A_WAIT_CM;
                        return 0;
                    }
                }
            }
        } else if (c->rcfg.cm) {
            decl = PL_RDMA_DECL_OFF;
        } else {
            char err[256];
            struct plr_conn *conn = plr_conn_new(pool, err, sizeof err);
            if (!conn) {
                fprintf(stderr, "peerlink: rdma: accept: %s\n", err);
                decl = PL_RDMA_DECL_POOL;
            } else if (plr_conn_start_opt(conn, &peer_ep, c->rcfg.sl[c->l->kind], c->rcfg.poll_us, err, sizeof err) != 0) {
                fprintf(stderr, "peerlink: rdma: accept: %s\n", err);
                plr_conn_free(conn);
                decl = PL_RDMA_DECL_QP;
            } else {
                struct plr_ep my_ep;
                uint8_t ep_bytes[PLR_EP_LEN];
                plr_conn_ep(conn, &my_ep);
                plr_ep_encode(&my_ep, ep_bytes);
                c->up_conn = conn;
                c->up_t = plr_conn_transport(conn, NULL);
                c->up_peer_seq = c->rd->seq - 1; /* seq of RDMA_OFFER */
                c->up_nonce = m->a;
                c->up_self_seq = c->seq_out; /* seq of RDMA_ACCEPT */
                c->up_cm_mode = 0;
                send_ctl_now(c, PLT_RDMA_ACCEPT, ep_bytes, sizeof ep_bytes);
                uint64_t atmo = (uint64_t)(c->rcfg.accept_timeout_ms ? c->rcfg.accept_timeout_ms : 4000u) * MS;
                c->up_deadline = now_ns() + atmo;
                c->up_state = UP_A_WAIT_HELLO;
                return 0;
            }
        }
    }
    uint8_t r = (uint8_t)decl;
    send_ctl_now(c, PLT_RDMA_DECLINE, &r, 1);
    c->up_state = UP_IDLE;
    if (acc_requires_rdma(c) || (c->rcfg.cm && peerlink_mutant == PL_MUT_RDMA_CM_NOFALLBACK)) {
        int fb = decl_to_fb(decl);
        fb_count(fb);
        fprintf(stderr, "peerlink: rdma: mode on: peer %" PRIu32 " kind %s declined upgrade (%s): ending connection\n",
                c->keys.peer, kname_tab[c->l->kind], peerlink_rdma_fb_name(fb));
        c->dead = 1;
        return 0;
    }
    c->up_declined_wait = 1;
    conn_settle(c, 0);
    return 0;
}

static int up_handle_accept(struct conn *c, const struct pl_msg *m)
{
    if (c->up_state == UP_IDLE && !c->on_rdma && c->up_aborted_wait) {
        c->up_aborted_wait = 0; /* late ACCEPT racing with dialer's timeout ABORT */
        return 0;
    }
    if (!c->l->dialed || c->up_state != UP_D_WAIT_ACCEPT || (!c->up_cm_mode && !c->up_conn))
        return -1;
    /* ACCEPT was received and consumed: any abort from here on must NOT set up_aborted_wait (no late ACCEPT pending). */
    c->up_state = c->up_cm_mode ? UP_D_WAIT_CM : UP_D_WAIT_READY;
    struct plr_ep peer_ep;
    char err[256];
    if (plr_ep_decode(&peer_ep, m->data, m->data_len) != 0) {
        up_dial_abort(c, PL_RDMA_FB_BAD, 1);
        return 0;
    }
    if (__atomic_load_n(&peerlink_test_rdma_abort_before_hello, __ATOMIC_RELAXED)) {
        up_dial_abort(c, PL_RDMA_FB_QP, 1);
        return 0;
    }
    if (c->up_cm_mode) {
        struct plr_cm_addr cm_addr;
        if (!(peer_ep.flags & PLR_EP_F_CM) || m->data_len != PLR_EP_LEN + PLR_CM_ADDR_LEN ||
            plr_cm_addr_decode(&cm_addr, m->data + PLR_EP_LEN, PLR_CM_ADDR_LEN) != 0) {
            up_dial_abort(c, PL_RDMA_FB_BAD, 1);
            return 0;
        }
        if (cm_ip_is_zero(cm_addr.ip))
            cm_fill_ip_from_tcp(c->t, 1, &cm_addr);
        up_cm_build_pd(c, c->up_cm_pd);
        int otmo_ms = (int)(c->rcfg.offer_timeout_ms ? c->rcfg.offer_timeout_ms : 2000u);
        c->up_cm_id = c->rcfg.cm_ops->dial(&cm_addr, otmo_ms, err, sizeof err);
        if (!c->up_cm_id) {
            fprintf(stderr, "peerlink: rdma-cm: dial: %s\n", err);
            up_dial_abort(c, PL_RDMA_FB_QP, 1);
            return 0;
        }
        c->up_peer_ep = peer_ep;
        c->up_peer_seq = c->rd->seq - 1; /* seq of RDMA_ACCEPT */
        c->up_state = UP_D_WAIT_CM;
        return 0;
    }
    if (m->data_len != PLR_EP_LEN || (peer_ep.flags & PLR_EP_F_CM)) {
        up_dial_abort(c, PL_RDMA_FB_BAD, 1);
        return 0;
    }
    if (plr_conn_start_opt(c->up_conn, &peer_ep, c->rcfg.sl[c->l->kind], c->rcfg.poll_us, err, sizeof err) != 0) {
        fprintf(stderr, "peerlink: rdma: dial: %s\n", err);
        up_dial_abort(c, PL_RDMA_FB_QP, 1);
        return 0;
    }
    c->up_t = plr_conn_transport(c->up_conn, NULL);
    c->up_peer_seq = c->rd->seq - 1; /* seq of RDMA_ACCEPT */
    up_encode_qp_probe(c, PLT_RDMA_HELLO, c->up_self_seq, c->up_nonce);
    c->up_fail_after_hello = __atomic_load_n(&peerlink_test_rdma_fail_after_hello, __ATOMIC_RELAXED);
    c->up_state = UP_D_WAIT_READY;
    return 0;
}

static int up_handle_decline(struct conn *c, const struct pl_msg *m)
{
    if (c->up_state == UP_IDLE && !c->on_rdma && c->up_aborted_wait) {
        c->up_aborted_wait = 0; /* late DECLINE racing with dialer's timeout ABORT */
        return 0;
    }
    if (!c->l->dialed || c->up_state != UP_D_WAIT_ACCEPT)
        return -1;
    uint8_t reason = (uint8_t)m->a;
    if (reason == PL_RDMA_DECL_SUBNET && c->rcfg.npools > 1)
        c->l->rdma_pool_idx = (c->l->rdma_pool_idx + 1) % c->rcfg.npools;
    int final = reason == PL_RDMA_DECL_NOT_BUILT || reason == PL_RDMA_DECL_OFF || reason == PL_RDMA_DECL_KIND;
    int fb = decl_to_fb(reason);
    up_cleanup(c);
    if (final) {
        fb_count(fb);
        c->rdma_final_decline = 1;
        if (c->rcfg.mode == PL_RDMA_ON || (c->rcfg.cm && peerlink_mutant == PL_MUT_RDMA_CM_NOFALLBACK)) {
            fprintf(stderr, "peerlink: rdma: mode on: peer %" PRIu32 " kind %s declined (%s): ending connection\n",
                    c->keys.peer, kname_tab[c->l->kind], peerlink_rdma_fb_name(fb));
            c->up_state = UP_IDLE;
            c->dead = 1;
            return 0;
        }
        c->up_state = UP_IDLE;
        c->next_reoffer_ns = 0;
        conn_settle(c, 0);
        return 0;
    }
    up_dial_abort(c, fb, 0); /* no ABORT needed after peer's DECLINE */
    return 0;
}

static int up_handle_abort(struct conn *c)
{
    if (c->l->dialed)
        return -1;
    if (c->up_state == UP_IDLE && !c->on_rdma && c->up_declined_wait) {
        c->up_declined_wait = 0; /* racing dialer timeout ABORT after acceptor sent DECLINE */
        return 0;
    }
    if (c->up_state != UP_A_WAIT_OFFER && c->up_state != UP_A_WAIT_CM && c->up_state != UP_A_WAIT_CM_EST &&
        c->up_state != UP_A_WAIT_HELLO && c->up_state != UP_A_WAIT_GO)
        return -1;
    up_cleanup(c);
    c->up_state = UP_IDLE;
    if (acc_requires_rdma(c) || (c->rcfg.cm && peerlink_mutant == PL_MUT_RDMA_CM_NOFALLBACK)) {
        fb_count(PL_RDMA_FB_QP);
        fprintf(stderr, "peerlink: rdma: mode on: peer %" PRIu32 " kind %s aborted upgrade: ending connection\n",
                c->keys.peer, kname_tab[c->l->kind]);
        c->dead = 1;
        return 0;
    }
    conn_settle(c, 0);
    return 0;
}

/* Advances Option C (rdma_cm) non-blocking CM events on dialer (UP_D_WAIT_CM) or acceptor (UP_A_WAIT_CM_EST). */
static void up_step_cm(struct conn *c)
{
    char err[256];
    for (;;) {
        int ev = PLR_CM_EV_NONE;
        if (c->up_state == UP_D_WAIT_CM) {
            if (c->up_conn) {
                ev = plr_conn_cm_poll(c->up_conn);
            } else if (c->up_cm_id) {
                struct plr_cm_ev cev;
                int r = c->rcfg.cm_ops->next_event(c->up_cm_id, &cev);
                ev = r < 0 ? -1 : r == 0 ? PLR_CM_EV_NONE : cev.type;
            }
            if (ev == PLR_CM_EV_NONE)
                return;
            if (ev == PLR_CM_EV_ADDR_RESOLVED && c->up_cm_id && !c->up_conn) {
                if (c->rcfg.cm_ops->set_sl)
                    c->rcfg.cm_ops->set_sl(c->up_cm_id, c->rcfg.sl[c->l->kind]);
                int otmo_ms = (int)(c->rcfg.offer_timeout_ms ? c->rcfg.offer_timeout_ms : 2000u);
                if (c->rcfg.cm_ops->resolve_route(c->up_cm_id, otmo_ms) != 0) {
                    fprintf(stderr, "peerlink: rdma-cm: resolve_route failed: %s\n", strerror(errno));
                    up_dial_abort(c, PL_RDMA_FB_QP, 1);
                    return;
                }
                continue;
            }
            if (ev == PLR_CM_EV_ROUTE_RESOLVED && c->up_cm_id && !c->up_conn) {
                c->up_conn = plr_conn_new_cm(c->up_pool, c->rcfg.cm_ops, c->up_cm_id, &c->up_peer_ep, c->rcfg.poll_us,
                                             err, sizeof err);
                if (!c->up_conn && errno == EXDEV && c->rcfg.npools > 1) {
                    /* Multi-HCA Ethernet: route resolved to another pool's device; try the remaining pools. */
                    for (int i = 0; i < c->rcfg.npools && !c->up_conn; i++) {
                        struct plr_pool *alt = c->rcfg.pools[i];
                        if (!alt || alt == c->up_pool)
                            continue;
                        c->up_conn = plr_conn_new_cm(alt, c->rcfg.cm_ops, c->up_cm_id, &c->up_peer_ep, c->rcfg.poll_us,
                                                     err, sizeof err);
                        if (c->up_conn) {
                            c->up_pool = alt;
                            c->l->rdma_pool_idx = i;
                        }
                    }
                }
                if (!c->up_conn) {
                    if (errno == EXDEV && c->rcfg.npools > 1)
                        c->l->rdma_pool_idx = (c->l->rdma_pool_idx + 1) % c->rcfg.npools;
                    fprintf(stderr, "peerlink: rdma-cm: dial: %s\n", err);
                    up_dial_abort(c, PL_RDMA_FB_QP, 1);
                    return;
                }
                c->up_cm_id = NULL; /* ownership transferred into c->up_conn */
                int bp = __atomic_load_n(&peerlink_test_rdma_cm_bad_pd, __ATOMIC_RELAXED);
                uint8_t pd_len = bp == 3 ? 0 : (uint8_t)PLR_CM_PD_LEN;
                if (plr_conn_cm_connect(c->up_conn, pd_len ? c->up_cm_pd : NULL, pd_len, err, sizeof err) != 0) {
                    fprintf(stderr, "peerlink: rdma-cm: %s\n", err);
                    up_dial_abort(c, PL_RDMA_FB_QP, 1);
                    return;
                }
                continue;
            }
            if (ev == PLR_CM_EV_ESTABLISHED && c->up_conn) {
                plr_conn_cm_established(c->up_conn);
                c->up_t = plr_conn_transport(c->up_conn, NULL);
                up_encode_qp_probe(c, PLT_RDMA_HELLO, c->up_self_seq, c->up_nonce);
                c->up_fail_after_hello = __atomic_load_n(&peerlink_test_rdma_fail_after_hello, __ATOMIC_RELAXED);
                c->up_state = UP_D_WAIT_READY;
                return;
            }
            if (ev == PLR_CM_EV_REJECTED) {
                up_dial_abort(c, PL_RDMA_FB_AUTH, 1);
                return;
            }
            up_dial_abort(c, PL_RDMA_FB_QP, 1);
            return;
        }
        if (c->up_state == UP_A_WAIT_CM_EST && c->up_conn) {
            ev = plr_conn_cm_poll(c->up_conn);
            if (ev == PLR_CM_EV_NONE)
                return;
            if (ev == PLR_CM_EV_ESTABLISHED) {
                plr_conn_cm_established(c->up_conn);
                c->up_t = plr_conn_transport(c->up_conn, NULL);
                c->up_state = UP_A_WAIT_HELLO;
                return;
            }
            up_cleanup(c); /* acceptor keeps waiting on TCP for ABORT or 4 s timeout */
            return;
        }
        return;
    }
}

/* Parses upgrade frames (RDMA_HELLO, RDMA_READY, RDMA_GO) arriving on c->up_t in c->up_rb. */
static int up_feed_qp(struct conn *c)
{
    size_t cl = pl_check_len(c->keys.in.type);
    while (c->up_state != UP_IDLE && c->up_rb.len - c->up_rb.off >= 4) {
        const uint8_t *at = c->up_rb.p + c->up_rb.off;
        uint32_t len = le32(at);
        if (len < 1 + cl || len > WIRE_MAX)
            return -1;
        if (c->up_rb.len - c->up_rb.off - 4 < len)
            break;
        const uint8_t *f = at + 4;
        size_t body = len - cl;
        uint8_t type = f[0];
        int is_probe = (c->up_state == UP_A_WAIT_HELLO && type == PLT_RDMA_HELLO) ||
                       (c->up_state == UP_D_WAIT_READY && type == PLT_RDMA_READY);
        int is_go = (c->up_state == UP_A_WAIT_GO && type == PLT_RDMA_GO);
        if (!is_probe && !is_go)
            return -1;
        uint64_t seq = is_probe ? c->up_peer_seq : c->rd->seq++;
        int verify = cl && peerlink_mutant != PL_MUT_NOTAG;
        size_t cmp = peerlink_mutant == PL_MUT_HALFCMP ? cl / 2 : cl;
        uint8_t t[PL_CHECK_LEN_MAX];
        if (verify && c->keys.in.type == PL_CHECK_FULL) {
            frame_check(&c->keys.in, seq, len, f, body, NULL, 0, NULL, 0, t);
            if (!tsdb_ct_equal(t, f + body, cmp))
                return -1;
        }
        struct pl_msg m;
        uint8_t *own = NULL;
        if (pl_msg_decode(type, f + 1, body - 1, &m, &own) != 0)
            return -1;
        if (verify && c->keys.in.type == PL_CHECK_HEADER) {
            frame_check(&c->keys.in, seq, len, f, body, m.p, m.n, NULL, 0, t);
            if (!tsdb_ct_equal(t, f + body, cmp)) {
                free(own);
                return -1;
            }
        }
        c->up_rb.off += 4 + (size_t)len;
        if (is_probe) {
            uint8_t want_mac[32];
            up_rdma_mac(c->keys.rdma_key, type, c->up_peer_seq, c->up_nonce, want_mac);
            int auth_ok = m.a == c->up_nonce && tsdb_ct_equal(want_mac, m.data, 32);
            free(own);
            if (!auth_ok && peerlink_mutant != PL_MUT_RDMA_NOAUTH) {
                fprintf(stderr, "peerlink: rdma: %s authentication or nonce mismatch: refusing upgrade\n",
                        type == PLT_RDMA_HELLO ? "RDMA_HELLO" : "RDMA_READY");
                if (c->up_state == UP_D_WAIT_READY) {
                    up_dial_abort(c, PL_RDMA_FB_AUTH, 1);
                    return 0;
                }
                return -1;
            }
            if (c->up_state == UP_A_WAIT_HELLO) {
                int hold_ms = __atomic_load_n(&peerlink_test_rdma_hold_ready_ms, __ATOMIC_RELAXED);
                if (hold_ms > 0) {
                    c->up_ready_at = now_ns() + (uint64_t)hold_ms * MS;
                    c->up_state = UP_A_WAIT_GO;
                } else {
                    up_encode_qp_probe(c, PLT_RDMA_READY, c->up_self_seq, c->up_nonce);
                    c->up_state = UP_A_WAIT_GO;
                    if (peerlink_mutant == PL_MUT_RDMA_SETTLEREADY)
                        return up_switch_to_rdma(c, 0); /* planted bug: settles on READY instead of GO */
                }
            } else { /* UP_D_WAIT_READY */
                if (__atomic_load_n(&peerlink_test_rdma_skip_go, __ATOMIC_RELAXED)) {
                    c->up_deadline = now_ns() + 15000ull * MS; /* test: wait so acceptor's 4 s timeout fires */
                    return 0;
                }
                return up_switch_to_rdma(c, 1);
            }
        } else { /* UP_A_WAIT_GO: RDMA_GO arrived */
            free(own);
            return up_switch_to_rdma(c, 0);
        }
    }
    return 0;
}

static void send_done(struct conn *c, uint64_t file, int32_t status)
{
    struct buf b = {0};
    put64(&b, file);
    int32_t bad = __atomic_exchange_n(&peerlink_test_done_status, 0, __ATOMIC_RELAXED);
    put32(&b, (uint32_t)(bad ? bad : status));
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
    int is_up = m->type == PLT_RDMA_OFFER || m->type == PLT_RDMA_ACCEPT || m->type == PLT_RDMA_DECLINE ||
                m->type == PLT_RDMA_ABORT;
    if (!is_up && (!c->settled || (c->up_state != UP_IDLE && c->up_state != UP_D_WAIT_ACCEPT)))
        return -1;
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
        if (st == PL_OK && c->l && c->l->kind == PL_KIND_SNAP &&
            __atomic_load_n(&peerlink_test_cut_end, __ATOMIC_RELAXED) > 0 &&
            __atomic_sub_fetch(&peerlink_test_cut_end, 1, __ATOMIC_RELAXED) == 0) {
            fprintf(stderr, "TEST: a kind-5 connection cut after the end of file %" PRIu64 ", before its DONE"
                    " (snapcutdone)\n", m->file);
            rx_end(c, st);
            return -1; /* the connection goes down: the sender begins the file again on the next one */
        }
        send_done(c, m->file, st);
        rx_end(c, st);
        return 0;
    }
    case PLT_RESUME: {
        struct jobq *q = c->job;
        if (!q || c->jstate != J_WAITRESUME || m->file != q->j.file || !status_known(m->status))
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
        if (!q || (c->jstate != J_WAITDONE && c->jstate != J_SEND) || m->file != q->j.file || !status_known(m->status))
            return -1;
        c->job = NULL;
        job_done(pl, q, m->status, c->jsent);
        return 0;
    }
    case PLT_RDMA_OFFER:
        return up_handle_offer(c, m);
    case PLT_RDMA_ACCEPT:
        return up_handle_accept(c, m);
    case PLT_RDMA_DECLINE:
        return up_handle_decline(c, m);
    case PLT_RDMA_ABORT:
        return up_handle_abort(c);
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
        uint64_t w = bucket_take(pl, c->l->kind, n);
        if (w)
            return w;
        if (c->l->kind >= 0 && c->l->kind <= PL_KIND_DATA_MAX)
            __atomic_add_fetch(&kind_bytes[c->l->kind], n, __ATOMIC_RELAXED);
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
        frame_put(&c->wb, &c->keys.out, &c->seq_out, PLT_CHUNK, h, 16, c->cbuf, n, NULL, 0);
        pl_xxh_update(&c->jh, c->cbuf, n);
        c->jnext += n;
        c->jsent += n;
    }
    return 0;
}

static void conn_run(struct plink *l, struct pl_transport *t, struct keys *keys, uint64_t my_seq)
{
    struct peerlink *pl = l->pl;
    struct conn c = {.l = l, .pl = pl, .t = t, .my_seq = my_seq, .keys = *keys};
    rdma_cfg_get(pl, &c.rcfg);
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
    c.last_rx = c.last_ping = now_ns();
    /* Decide initial RDMA upgrade state before calling on_link(up) (design-rdma.md §5.1):
     * - dialer with keys->try_rdma: send RDMA_OFFER, settle only at decision (GO or ABORT/DECLINE)
     * - dialer in mode ON that cannot offer (e.g. peer lacks PL_CAP_RDMA or no pool): end connection
     * - acceptor whose peer set PL_CAP_RDMA_TRY: wait in UP_A_WAIT_OFFER for OFFER/ABORT before on_link(up)
     * - otherwise: settle on TCP immediately and call on_link(up) now */
    if (l->dialed && keys->try_rdma) {
        up_dial_start_offer(&c, 1);
    } else if (l->dialed && c.rcfg.mode == PL_RDMA_ON && (c.rcfg.kinds & (1u << l->kind))) {
        int fb = !(keys->peer_caps & PL_CAP_RDMA) ? PL_RDMA_FB_NOT_BUILT
                 : (c.rcfg.cm && !c.rcfg.cm_ops)  ? PL_RDMA_FB_NOT_BUILT
                 : c.rcfg.npools <= 0             ? PL_RDMA_FB_POOL
                                                  : PL_RDMA_FB_BACKOFF;
        fb_count(fb);
        fprintf(stderr, "peerlink: rdma: mode on: peer %" PRIu32 " kind %s cannot upgrade (%s): ending connection\n",
                keys->peer, kname_tab[l->kind], peerlink_rdma_fb_name(fb));
        c.dead = 1;
    } else if (l->dialed && c.rcfg.mode == PL_RDMA_AUTO && c.rcfg.cm && !c.rcfg.cm_ops &&
               peerlink_mutant == PL_MUT_RDMA_CM_NOFALLBACK) {
        c.dead = 1;
    } else if (!l->dialed && keys->try_rdma) {
        uint64_t otmo = (uint64_t)(c.rcfg.offer_timeout_ms ? c.rcfg.offer_timeout_ms : 2000u) * MS;
        c.up_deadline = c.last_rx + otmo;
        c.up_state = UP_A_WAIT_OFFER;
    } else if (acc_requires_rdma(&c)) {
        int fb = !(keys->peer_caps & PL_CAP_RDMA) ? PL_RDMA_FB_NOT_BUILT : PL_RDMA_FB_OFF;
        fb_count(fb);
        fprintf(stderr, "peerlink: rdma: mode on: peer %" PRIu32 " kind %s did not offer RDMA: ending connection\n",
                keys->peer, kname_tab[l->kind]);
        c.dead = 1;
    } else {
        if (l->dialed && c.rcfg.mode == PL_RDMA_AUTO && c.rcfg.npools > 0 && (c.rcfg.kinds & (1u << l->kind)) &&
            (keys->peer_caps & PL_CAP_RDMA) && l->rdma_until_ns && c.last_rx < l->rdma_until_ns)
            fb_count(PL_RDMA_FB_BACKOFF);
        up_schedule_reoffer(&c, c.last_rx);
        conn_settle(&c, 0);
    }
    uint64_t ping_ns = (uint64_t)(pl->cfg.ping_ms ? pl->cfg.ping_ms : 1000) * MS;
    uint64_t dead_ns = (uint64_t)(pl->cfg.dead_ms ? pl->cfg.dead_ms : 5000) * MS;
    int poll_cap = (int)(ping_ns / MS / 2);
    if (poll_cap < 10)
        poll_cap = 10;
    int reset_seen = 0;

    while (!c.dead && !atomic_load(&pl->closing) && !atomic_load(&l->stopped)) {
        pthread_mutex_lock(&pl->mu);
        int kicked = !l->dialed && l->adopt_seq != my_seq;
        pthread_mutex_unlock(&pl->mu);
        if (kicked)
            break;
        uint64_t now = now_ns();
        pthread_mutex_lock(&l->mu);
        int reset = c.settled && l->reset_gen == c.gen;
        struct qf *expired = l->qdl_min && l->qdl_min <= now ? q_expire(l, now) : NULL;
        pthread_mutex_unlock(&l->mu);
        while (expired) {
            struct qf *nx = expired->next;
            sent_cb(&c, expired->token, 0);
            free(expired);
            expired = nx;
        }
        if (reset) {
            reset_seen = 1;
            ST(pl, drops, 1);
            break;
        }
        /* Staying on RDMA for stable_ms (60 s) resets the per-(peer, kind) failure count (design-rdma.md §5.3). */
        if (c.on_rdma && l->dialed && !c.rdma_stable_reset) {
            uint64_t stab = (uint64_t)(c.rcfg.stable_ms ? c.rcfg.stable_ms : 60000u) * MS;
            if (now - c.rdma_settled_ns >= stab) {
                if (l->rdma_backed_off)
                    fprintf(stderr, "peerlink: rdma: peer %" PRIu32 " kind %s stable on RDMA: backoff reset\n",
                            c.keys.peer, kname_tab[l->kind]);
                l->rdma_fails = 0;
                l->rdma_backoff_ns = 0;
                l->rdma_until_ns = 0;
                l->rdma_backed_off = 0;
                c.rdma_stable_reset = 1;
            }
        }
        /* Upgrade timers: delayed READY (test hook), dialer 2 s timeout, acceptor 4 s timeout, live re-offer. */
        if (c.up_state == UP_A_WAIT_GO && c.up_ready_at && now >= c.up_ready_at) {
            c.up_ready_at = 0;
            if (c.up_t) {
                up_encode_qp_probe(&c, PLT_RDMA_READY, c.up_self_seq, c.up_nonce);
                if (peerlink_mutant == PL_MUT_RDMA_SETTLEREADY)
                    up_switch_to_rdma(&c, 0);
            }
        }
        if (c.up_state == UP_A_WAIT_CM && c.up_cm_tok) {
            struct plr_cm_id *cid = plr_cm_take(c.rcfg.cm_lis, c.up_cm_tok);
            if (cid) {
                c.up_cm_tok = NULL;
                char err[256];
                c.up_conn = plr_conn_new_cm(c.up_pool, c.rcfg.cm_ops, cid, &c.up_peer_ep, c.rcfg.poll_us, err, sizeof err);
                if (!c.up_conn && errno == EXDEV && c.rcfg.npools > 1) {
                    for (int i = 0; i < c.rcfg.npools && !c.up_conn; i++) {
                        struct plr_pool *alt = c.rcfg.pools[i];
                        if (!alt || alt == c.up_pool)
                            continue;
                        c.up_conn = plr_conn_new_cm(alt, c.rcfg.cm_ops, cid, &c.up_peer_ep, c.rcfg.poll_us, err, sizeof err);
                        if (c.up_conn)
                            c.up_pool = alt;
                    }
                }
                if (!c.up_conn) {
                    fprintf(stderr, "peerlink: rdma-cm: accept: %s\n", err);
                    c.rcfg.cm_ops->reject(cid);
                } else if (plr_conn_cm_accept(c.up_conn, err, sizeof err) != 0) {
                    fprintf(stderr, "peerlink: rdma-cm: %s\n", err);
                    up_cleanup(&c);
                } else {
                    c.up_state = UP_A_WAIT_CM_EST;
                    up_step_cm(&c);
                }
            }
        }
        if (c.up_state != UP_IDLE && now >= c.up_deadline) {
            if (c.up_state == UP_D_WAIT_ACCEPT || c.up_state == UP_D_WAIT_CM || c.up_state == UP_D_WAIT_READY) {
                up_dial_abort(&c, PL_RDMA_FB_TIMEOUT, 1);
            } else {
                fprintf(stderr, "peerlink: rdma: acceptor timed out waiting for peer %" PRIu32 " kind %s: closing\n",
                        c.keys.peer, kname_tab[l->kind]);
                c.dead = 1;
            }
            if (c.dead)
                break;
        }
        if (c.up_state == UP_IDLE && c.settled && !c.on_rdma && c.next_reoffer_ns && now >= c.next_reoffer_ns) {
            c.next_reoffer_ns = 0;
            if (!c.up_aborted_wait && dial_can_try_rdma(l, &c.rcfg, now))
                up_dial_start_offer(&c, 0);
            else
                up_schedule_reoffer(&c, now);
            if (c.dead)
                break;
        }
        /* queued frames of the driver (held in l->qh while an upgrade is in flight: §5.1) */
        if (c.up_state == UP_IDLE && c.settled) {
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
        }
        uint64_t bwait = 0;
        if (c.up_state == UP_IDLE && c.settled && c.job && c.jstate == J_SEND) {
            if (c.throttle_until <= now)
                bwait = job_send(&c);
            else
                bwait = c.throttle_until - now;
            if (bwait)
                c.throttle_until = now + bwait;
        }
        int paused = atomic_load(&l->paused), pfresh = paused && peerlink_mutant != PL_MUT_PAUSEDEAD;
        if (c.stalled || c.up_state != UP_IDLE)
            c.last_rx = now; /* held back, or an upgrade has its own deadline (2 s / 4 s) */
        if (pfresh)
            c.last_rx = now; /* we do not read: the peer's silence is ours (its PINGs wait in the socket) */
        if (now - c.last_rx > dead_ns) {
            ST(pl, drops, 1);
            break;
        }
        /* paused, we still PING on our own clock: the peer hears from us and does not count us dead */
        if (c.up_state == UP_IDLE && c.settled && (pfresh || now - c.last_rx > ping_ns) && now - c.last_ping > ping_ns) {
            send_ctl(&c, PLT_PING, "", 0);
            c.last_ping = now;
        }
        /* the buffer was filled to its mark: frames may be waiting behind it, and nothing wakes us for them */
        int full = c.wb.len - c.wb.off >= WB_HIGH;
        while (c.wb.len > c.wb.off) {
            struct iovec iv = {c.wb.p + c.wb.off, c.wb.len - c.wb.off};
            ssize_t w = c.t->ops->writev(c.t, &iv, 1);
            if (w < 0 && errno == EINTR)
                continue;
            if (w < 0 && errno == EAGAIN)
                break;
            if (w <= 0) {
                c.dead = 1;
                break;
            }
            /* PL_MUT_WNOSHORT: a short write counted as the whole buffer (frames the transport never took: WRITTEN) */
            c.wdone += peerlink_mutant == PL_MUT_WNOSHORT ? (uint64_t)(c.wb.len - c.wb.off) : (uint64_t)w;
            c.wb.off += (size_t)w;
        }
        trk_written(&c);
        if (c.wb.off == c.wb.len)
            c.wb.off = c.wb.len = 0;
        c.stalled = c.wb.len > c.wb.off;
        /* Write upgrade probe frames (RDMA_HELLO / RDMA_READY) to the candidate QP transport */
        while (c.up_t && c.up_wb.len > c.up_wb.off && !c.dead) {
            struct iovec iv = {c.up_wb.p + c.up_wb.off, c.up_wb.len - c.up_wb.off};
            ssize_t w = c.up_t->ops->writev(c.up_t, &iv, 1);
            if (w < 0 && errno == EINTR)
                continue;
            if (w < 0 && errno == EAGAIN)
                break;
            if (w <= 0) {
                if (c.up_state == UP_D_WAIT_READY)
                    up_dial_abort(&c, PL_RDMA_FB_QP_ERROR, 1);
                else
                    up_cleanup(&c); /* acceptor keeps waiting on TCP for ABORT until 4 s deadline */
                break;
            }
            c.up_wb.off += (size_t)w;
        }
        if (c.up_wb.off == c.up_wb.len)
            c.up_wb.off = c.up_wb.len = 0;
        if (c.up_state == UP_D_WAIT_READY && c.up_fail_after_hello && c.up_wb.len == c.up_wb.off) {
            /* test hook: RDMA_HELLO was posted on the QP; now fall back to TCP via RDMA_ABORT */
            c.up_fail_after_hello = 0;
            up_dial_abort(&c, PL_RDMA_FB_QP_ERROR, 1);
        }
        if (c.dead)
            break;
        int tmo = poll_cap;
        if (bwait && (int)(bwait / MS) + 1 < tmo)
            tmo = (int)(bwait / MS) + 1;
        /* a file with more to send, not held by the bucket, and nothing left to write: fill the buffer again at
         * once (with data in the buffer the POLLOUT below wakes us when the socket drains) */
        if (c.up_state == UP_IDLE && c.settled && c.wb.len == c.wb.off &&
            (full || (!bwait && c.job && c.jstate == J_SEND)))
            tmo = 0;
        if (c.up_state == UP_IDLE && c.settled && c.hp.active && !c.dead)
            tmo = 0; /* the next megabyte of a prefix */
        if (c.up_state != UP_IDLE) {
            uint64_t t = now_ns();
            int d = c.up_deadline <= t ? 0 : (int)((c.up_deadline - t) / MS) + 1;
            if (d < tmo)
                tmo = d;
            if (c.up_ready_at) {
                int r = c.up_ready_at <= t ? 0 : (int)((c.up_ready_at - t) / MS) + 1;
                if (r < tmo)
                    tmo = r;
            }
        } else if (c.settled && !c.on_rdma && c.next_reoffer_ns) {
            uint64_t t = now_ns();
            uint64_t rem = c.next_reoffer_ns <= t ? 0 : (c.next_reoffer_ns - t) / MS + 1;
            if (rem < (uint64_t)tmo)
                tmo = (int)rem;
        }
        pthread_mutex_lock(&l->mu);
        uint64_t qdl = l->qdl_min;
        pthread_mutex_unlock(&l->mu);
        if (qdl) { /* a queued frame's deadline */
            uint64_t t = now_ns();
            int d = qdl <= t ? 0 : (int)((qdl - t) / MS) + 1;
            if (d < tmo)
                tmo = d;
        }
        /* paused: no reading (a hangup or an error is still reported, and read); bytes left in the write buffer:
         * come back as soon as the transport takes more */
        struct pollfd pf[PL_TR_POLL_MAX * 2 + 2];
        int ready = 0, want = ((paused && c.up_state == UP_IDLE) ? 0 : PL_TR_IN) | (c.wb.len > c.wb.off ? PL_TR_OUT : 0);
        int nt = c.t->ops->poll_fds(c.t, want, pf, &ready);
        if (nt < 0 || nt > PL_TR_POLL_MAX) {
            fprintf(stderr, "peerlink: the %s transport asked to poll %d descriptors (at most %d): connection ended\n",
                    c.t->ops->name, nt, PL_TR_POLL_MAX);
            break;
        }
        int nu = 0, uready = 0;
        if (c.up_t) {
            int uwant = PL_TR_IN | (c.up_wb.len > c.up_wb.off ? PL_TR_OUT : 0);
            nu = c.up_t->ops->poll_fds(c.up_t, uwant, pf + nt, &uready);
            if (nu < 0 || nu > PL_TR_POLL_MAX) {
                nu = 0;
                if (c.up_state == UP_D_WAIT_READY)
                    up_dial_abort(&c, PL_RDMA_FB_QP_ERROR, 1);
                else
                    up_cleanup(&c);
            }
        }
        int ncm = 0;
        if (c.up_state == UP_D_WAIT_CM || c.up_state == UP_A_WAIT_CM_EST) {
            int cm_fd = c.up_conn ? plr_conn_cm_fd(c.up_conn) : (c.up_cm_id ? c.rcfg.cm_ops->fd(c.up_cm_id) : -1);
            if (cm_fd >= 0) {
                pf[nt + nu] = (struct pollfd){cm_fd, POLLIN, 0};
                ncm = 1;
            }
        }
        pf[nt + nu + ncm] = (struct pollfd){l->evfd, POLLIN, 0};
        if ((ready || uready) && peerlink_mutant != PL_MUT_NOREADY)
            tmo = 0; /* the transport holds what we want already */
        int pr = poll(pf, (nfds_t)(nt + nu + ncm + 1), tmo);
        if (pr < 0 && errno != EINTR)
            break;
        if (pr <= 0 && !ready && !uready)
            continue;
        if (pr > 0 && (pf[nt + nu + ncm].revents & POLLIN))
            drain_evfd(l);
        if (pr > 0 && ncm && (pf[nt + nu].revents & (POLLIN | POLLERR | POLLHUP))) {
            up_step_cm(&c);
            if (c.dead)
                break;
        }
        int tr = ready | (pr > 0 ? c.t->ops->poll_ready(c.t, pf, nt) : 0);
        int utr = c.up_t ? (uready | (pr > 0 && nu > 0 ? c.up_t->ops->poll_ready(c.up_t, pf + nt, nu) : 0)) : 0;
        if (tr & PL_TR_IN) {
            ssize_t r = c.t->ops->read(c.t, rbuf, READ_BUF);
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
        if (c.up_t && !c.dead && (utr & PL_TR_IN)) {
            ssize_t r = c.up_t->ops->read(c.up_t, rbuf, READ_BUF);
            if (r > 0) {
                put(&c.up_rb, rbuf, (size_t)r);
                if (up_feed_qp(&c) != 0) {
                    if (c.up_state == UP_D_WAIT_READY)
                        up_dial_abort(&c, PL_RDMA_FB_QP_ERROR, 1);
                    else {
                        ST(pl, drops, 1);
                        break;
                    }
                }
            } else if (r == 0 || (errno != EAGAIN && errno != EINTR)) {
                if (c.up_state == UP_D_WAIT_READY)
                    up_dial_abort(&c, PL_RDMA_FB_QP_ERROR, 1);
                else
                    up_cleanup(&c); /* acceptor waits on TCP for ABORT or 4 s timeout */
            }
        }
    }

    /* If an RDMA connection dropped unexpectedly within stable_ms (60 s) of settling, count against backoff (§5.3). */
    if (c.on_rdma && l->dialed && !atomic_load(&pl->closing) && !atomic_load(&l->stopped) && !reset_seen) {
        uint64_t stab = (uint64_t)(c.rcfg.stable_ms ? c.rcfg.stable_ms : 60000u) * MS;
        if (!c.rdma_stable_reset && now_ns() - c.rdma_settled_ns < stab)
            up_record_fail(&c, PL_RDMA_FB_QP_ERROR);
    }
    up_cleanup(&c);
    dctl_free(&c);

    /* the connection ended: what was in flight goes back or is told */
    rx_end(&c, PL_E_DOWN);
    struct qf *f = NULL;
    if (c.settled) {
        pthread_mutex_lock(&l->mu);
        l->up = 0;
        l->open = 0;
        l->io_set = 0;
        if (!(peerlink_mutant == PL_MUT_RDMA_KEEPQUEUE && c.on_rdma)) {
            f = l->qh;
            l->qh = l->qt = NULL;
            l->qbytes = 0;
            l->qdl_min = 0;
        }
        pthread_cond_broadcast(&l->cv);
        pthread_mutex_unlock(&l->mu);
    }
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
    if (peerlink_mutant == PL_MUT_CLOSEFIRST)
        c.t->ops->close(c.t);
    if (c.settled) {
        if (c.keys.out.type >= 1 && c.keys.out.type <= 3)
            ST(pl, up_check[c.keys.out.type], (uint64_t)-1);
        if (peerlink_mutant != PL_MUT_UPDOWNLEAK)
            up_count(l->kind, c.keys.out.type, -1);
        up_tr_count(l->kind, c.on_rdma, -1);
        atomic_store(&l->check, 0);
        atomic_store(&l->transport, 0);
        if (pl->cfg.on_link)
            pl->cfg.on_link(pl->cfg.arg, l, 0);
    }
    if (peerlink_mutant != PL_MUT_CLOSEFIRST)
        c.t->ops->close(c.t); /* last (pltransport.h): the driver saw the down before a verbs transport's slots go */
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
            conn_run(l, tcp_new(fd, pl->cfg.sock_buf), &k, 0);
            tsdb_wipe(&k, sizeof k);
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
 * callback (a volume on NFS that is away) holds its own thread and descriptor only, however often the peer redials.
 * claim_run is the claim and the run, on transport t, which it owns (closed when the connection ends or is not run);
 * ok: the handshake passed; hand: the connection was counted in nhand. */
static void claim_run(struct peerlink *pl, int ok, int hand, uint32_t peer, int kind, struct pl_transport *t,
                      struct keys *k)
{
    pthread_mutex_lock(&pl->mu);
    if (hand || peerlink_mutant == PL_MUT_NHANDALL)
        pl->nhand--;
    struct plink *l = ok && !atomic_load(&pl->closing) ? accepted_link(pl, peer, kind) : NULL;
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
        conn_run(l, t, k, my);
        pthread_mutex_lock(&pl->mu);
        l->running = 0;
        pthread_cond_broadcast(&pl->acv);
        pthread_mutex_unlock(&pl->mu);
    } else {
        t->ops->close(t);
    }
}

static void *adopt_main(void *arg)
{
    struct adopt *a = arg;
    struct peerlink *pl = a->pl;
    struct hello h;
    struct keys k = {0};
    int ok = !atomic_load(&pl->closing) && hello_parse(a->hello, a->hn, &h) == 0 &&
             hs_accept(pl, a->fd, a->hello, a->hn, &a->self, &k) == 0;
    claim_run(pl, ok, 1, ok ? h.id : 0, ok ? h.kind : 0, tcp_new(a->fd, ok ? pl->cfg.sock_buf : 0), &k);
    tsdb_wipe(&k, sizeof k);
    free(a);
    atomic_fetch_sub(&pl->helpers, 1);
    return NULL;
}

/* test_peerlink's attach (peerlink.h): a connection on any transport, its keys from a transcript the two ends share */
struct tattach {
    struct peerlink *pl;
    uint32_t peer;
    int kind;
    struct pl_transport *t;
    struct keys k;
};

static void *tattach_main(void *arg)
{
    struct tattach *a = arg;
    struct peerlink *pl = a->pl;
    claim_run(pl, 1, 0, a->peer, a->kind, a->t, &a->k);
    tsdb_wipe(&a->k, sizeof a->k);
    free(a);
    atomic_fetch_sub(&pl->helpers, 1);
    return NULL;
}

int peerlink_test_attach(struct peerlink *pl, uint32_t peer, int kind, int dialer, const uint8_t *tr, size_t trlen,
                         struct pl_transport *t)
{
    struct tattach *a = kind > PL_KIND_META && kind <= PL_KIND_DATA_MAX ? calloc(1, sizeof *a) : NULL;
    if (!a) {
        t->ops->close(t);
        return -1;
    }
    a->pl = pl;
    a->peer = peer;
    a->kind = kind;
    a->t = t;
    keys_derive(pl, dialer, check_level(pl), tr, trlen, &a->k);
    a->k.peer = peer;
    a->k.peer_caps = PL_CAPS;
    pthread_mutex_lock(&pl->mu); /* as peerlink_adopt: an attach that gets past closing is waited for */
    int rc = -1;
    if (!atomic_load(&pl->closing)) {
        atomic_fetch_add(&pl->helpers, 1);
        pthread_t th;
        pthread_attr_t at;
        pthread_attr_init(&at);
        pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED);
        rc = pthread_create(&th, &at, tattach_main, a);
        pthread_attr_destroy(&at);
        if (rc != 0)
            atomic_fetch_sub(&pl->helpers, 1);
    }
    pthread_mutex_unlock(&pl->mu);
    if (rc != 0) {
        t->ops->close(t);
        tsdb_wipe(&a->k, sizeof a->k);
        free(a);
        return -1;
    }
    return 0;
}

int peerlink_adopt(struct peerlink *pl, int fd, const uint8_t *hello, size_t hello_len, const struct pl_self *self)
{
    struct hello h;
    int hp = hello_len > HS_FRAME_MAX ? -1 : hello_parse(hello, hello_len, &h);
    if (hp == -2) /* a peer of another version (tsdbd's cluster.c says it before it hands a connection over) */
        fprintf(stderr, "peerlink: refused a link of cluster version %u (this server speaks %u)\n", (unsigned)hello[4],
                (unsigned)PL_CL_VERSION);
    if (hp != 0)
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

uint64_t peerlink_kind_bytes(int kind)
{
    return kind >= 0 && kind <= PL_KIND_DATA_MAX ? __atomic_load_n(&kind_bytes[kind], __ATOMIC_RELAXED) : 0;
}

void peerlink_stats(struct peerlink *pl, struct pl_stats *st)
{
    uint64_t *d = (uint64_t *)st, *s = (uint64_t *)&pl->st;
    for (size_t i = 0; i < sizeof *st / 8; i++)
        d[i] = __atomic_load_n(&s[i], __ATOMIC_RELAXED);
    pthread_mutex_lock(&pl->mu);
    st->handshakes = (uint64_t)(int64_t)pl->nhand;
    pthread_mutex_unlock(&pl->mu);
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
