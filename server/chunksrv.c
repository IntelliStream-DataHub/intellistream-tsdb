/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The chunk server and its client (chunksrv.h has the design). Threads: the peerlink's link threads decode and queue
 * (a request is checked before it is queued, so a malformed one ends its link at once) and write the cells of a put
 * as they arrive (a write to a temporary file: page cache); `workers` threads answer the requests and commit the puts,
 * which sync, so no link thread waits on a disk for longer than a write. The request queue is bounded in bytes: a link
 * whose frames would pass the bound is paused (plink_pause: TCP pushes back on its peer) until the workers catch up.
 */
#include "chunksrv.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef __linux__
#include <sys/syscall.h>
#endif
#include <time.h>
#include <unistd.h>

#include "../include/tsdb.h"
#include "../src/chunk_priv.h"
#include "../src/internal.h"
#include "peerlink.h"

int chunksrv_mutant;

int chunksrv_mutant_parse(const char *name)
{
    static const char *const names[] = {"none", "chunknofence", "chunkselfraise", "putokearly", "nocellcheck",
                                        "storeforeign", "nometacheck", "sumsonly", "noreqcheck", "noopmatch",
                                        "norespcheck"};
    for (int i = 0; i < (int)(sizeof names / sizeof *names); i++)
        if (strcmp(name, names[i]) == 0)
            return i;
    return -1;
}

/* ---------------------------------------------------------------- wire */
static void put16(uint8_t *p, uint16_t v) { p[0] = (uint8_t)v, p[1] = (uint8_t)(v >> 8); }
static void put32(uint8_t *p, uint32_t v)
{
    for (int i = 0; i < 4; i++)
        p[i] = (uint8_t)(v >> (8 * i));
}
static void put64(uint8_t *p, uint64_t v)
{
    for (int i = 0; i < 8; i++)
        p[i] = (uint8_t)(v >> (8 * i));
}
static uint16_t get16(const uint8_t *p) { return (uint16_t)(p[0] | p[1] << 8); }
static uint32_t get32(const uint8_t *p)
{
    uint32_t v = 0;
    for (int i = 3; i >= 0; i--)
        v = v << 8 | p[i];
    return v;
}
static uint64_t get64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 7; i >= 0; i--)
        v = v << 8 | p[i];
    return v;
}

#define REQ_HDR 21u  /* rid 8, op 1, volume 4, epoch 8 */
#define RESP_HDR 21u /* rid 8, op 1, rc 4, check 8 (resp_check) */
#define ID_LEN 34u   /* vuuid 16, oid 16, pos 2 */

static uint64_t xh(const void *p, size_t n)
{
    struct pl_xxh h;
    pl_xxh_init(&h);
    pl_xxh_update(&h, p, n);
    return pl_xxh_final(&h);
}

/* An answer's check (review F4): an xxh64 over rid, op, rc and the body; a READ's over its body's length only, since its
 * cells are checked against the object's sums by the reader (chunkread.c) and hashing them again would be the per-byte
 * pass the links gave up. So a flipped rc (an error taken for success: a DEL counted done that never was), a LIST
 * page's names or an EPOCH answer's fence never reach the caller. */
static uint64_t resp_check(const uint8_t *p, size_t n)
{
    struct pl_xxh h;
    pl_xxh_init(&h);
    pl_xxh_update(&h, p, 13);
    if (p[8] == CS_OP_READ) {
        uint8_t l[8];
        put64(l, n - RESP_HDR);
        pl_xxh_update(&h, l, 8);
    } else {
        pl_xxh_update(&h, p + RESP_HDR, n - RESP_HDR);
    }
    return pl_xxh_final(&h);
}

void cs_resp_seal(uint8_t *p, size_t n)
{
    put64(p + 13, resp_check(p, n));
}

static size_t req_body_len(uint8_t op)
{
    switch (op) {
    case CS_OP_READ: return ID_LEN + 12;
    case CS_OP_SUMS: return ID_LEN + 12;
    case CS_OP_LIST: return 1 + 16 + 2 + 4;
    case CS_OP_DEL: return ID_LEN;
    case CS_OP_EPOCH: return 0;
    default: return (size_t)-1;
    }
}

size_t cs_req_encode(const struct cs_req *r, uint8_t *out, size_t cap)
{
    size_t bl = req_body_len(r->op);
    if (bl == (size_t)-1 || cap < REQ_HDR + bl + CS_CHECK_LEN)
        return 0;
    put64(out, r->rid);
    out[8] = r->op;
    put32(out + 9, r->volume);
    put64(out + 13, r->epoch);
    uint8_t *b = out + REQ_HDR;
    if (r->op == CS_OP_READ || r->op == CS_OP_SUMS || r->op == CS_OP_DEL) {
        memcpy(b, r->vuuid, 16);
        memcpy(b + 16, r->oid, 16);
        put16(b + 32, r->pos);
        if (r->op != CS_OP_DEL) {
            put64(b + 34, r->off);
            put32(b + 42, r->len);
        }
    } else if (r->op == CS_OP_LIST) {
        b[0] = r->has_cursor;
        memcpy(b + 1, r->oid, 16);
        put16(b + 17, r->pos);
        put32(b + 19, r->len);
    }
    put64(out + REQ_HDR + bl, xh(out, REQ_HDR + bl));
    return REQ_HDR + bl + CS_CHECK_LEN;
}

int cs_req_decode(const uint8_t *p, size_t n, struct cs_req *r)
{
    memset(r, 0, sizeof *r);
    if (n < REQ_HDR)
        return -1;
    r->rid = get64(p);
    r->op = p[8];
    r->volume = get32(p + 9);
    r->epoch = get64(p + 13);
    size_t bl = req_body_len(r->op);
    if (bl == (size_t)-1 || n != REQ_HDR + bl + CS_CHECK_LEN)
        return -1;
    if (get64(p + REQ_HDR + bl) != xh(p, REQ_HDR + bl) && chunksrv_mutant != CS_MUT_NOREQCHECK)
        return -1;
    const uint8_t *b = p + REQ_HDR;
    if (r->op == CS_OP_READ || r->op == CS_OP_SUMS || r->op == CS_OP_DEL) {
        memcpy(r->vuuid, b, 16);
        memcpy(r->oid, b + 16, 16);
        r->pos = get16(b + 32);
        if (r->pos >= CHUNK_POS_MAX)
            return -1;
        if (r->op != CS_OP_DEL) {
            r->off = get64(b + 34);
            r->len = get32(b + 42);
            uint32_t max = r->op == CS_OP_READ ? CS_READ_MAX : CS_SUMS_MAX;
            if (r->len == 0 || r->len > max || r->off > CHUNK_OBJ_MAX)
                return -1;
        }
    } else if (r->op == CS_OP_LIST) {
        if (b[0] > 1)
            return -1;
        r->has_cursor = b[0];
        memcpy(r->oid, b + 1, 16);
        r->pos = get16(b + 17);
        r->len = get32(b + 19);
        if (r->len == 0 || r->len > CS_LIST_MAX || r->pos >= CHUNK_POS_MAX)
            return -1;
    }
    return 0;
}

int cs_resp_decode(const uint8_t *p, size_t n, struct cs_resp *r)
{
    memset(r, 0, sizeof *r);
    if (n < RESP_HDR)
        return -1;
    r->rid = get64(p);
    r->op = p[8];
    r->rc = (int32_t)get32(p + 9);
    r->body = p + RESP_HDR;
    r->body_len = n - RESP_HDR;
    if (get64(p + 13) != resp_check(p, n) && chunksrv_mutant != CS_MUT_NORESPCHECK)
        return -1;
    if (r->op < CS_OP_READ || r->op > CS_OP_PUT || r->rc > 0)
        return -1;
    if (r->rc != 0)
        return r->body_len == 0 ? 0 : -1;
    const uint8_t *b = r->body;
    size_t bl = r->body_len;
    switch (r->op) {
    case CS_OP_READ: return bl >= 1 && bl <= CS_READ_MAX ? 0 : -1;
    case CS_OP_SUMS:
        return bl >= 36 && get32(b + 32) <= CS_SUMS_MAX && bl == 36 + 8 * (size_t)get32(b + 32) ? 0 : -1;
    case CS_OP_LIST:
        return bl >= 5 && b[4] <= 1 && get32(b) <= CS_LIST_MAX && bl == 5 + CS_LIST_ENTRY * (size_t)get32(b) ? 0 : -1;
    case CS_OP_DEL: return bl == 0 ? 0 : -1;
    case CS_OP_EPOCH: return bl == 9 && b[8] <= 1 ? 0 : -1;
    case CS_OP_PUT: return bl == 8 ? 0 : -1;
    }
    return -1;
}

void cs_put_meta_encode(const struct cs_put_meta *m, uint8_t out[CS_PUT_META_LEN])
{
    put32(out, CS_PUT_MAGIC);
    put32(out + 4, m->volume);
    put64(out + 8, m->epoch);
    memcpy(out + 16, m->vuuid, 16);
    memcpy(out + 32, m->oid, 16);
    put16(out + 48, m->pos);
    put16(out + 50, m->g.k);
    put16(out + 52, m->g.m);
    put32(out + 54, m->g.cell);
    put64(out + 58, m->g.len);
    put64(out + 66, xh(out, 66));
}

int cs_put_meta_decode(const uint8_t *p, size_t n, struct cs_put_meta *m)
{
    memset(m, 0, sizeof *m);
    if (n != CS_PUT_META_LEN || get32(p) != CS_PUT_MAGIC)
        return -1;
    if (get64(p + 66) != xh(p, 66) && chunksrv_mutant != CS_MUT_NOMETACHECK)
        return -1;
    m->volume = get32(p + 4);
    m->epoch = get64(p + 8);
    memcpy(m->vuuid, p + 16, 16);
    memcpy(m->oid, p + 32, 16);
    m->pos = get16(p + 48);
    if (chunk_geom_make(&m->g, get16(p + 50), get16(p + 52), get32(p + 54), get64(p + 58)) != 0)
        return -1;
    return m->pos < chunk_npos(&m->g) ? 0 : -1;
}

uint64_t cs_put_total(const struct cs_put_meta *m)
{
    return 8 * chunk_pos_cells(&m->g, m->pos) + chunk_pos_len(&m->g, m->pos);
}

uint64_t cs_sums_hash(const uint64_t *sums, uint64_t n)
{
    struct pl_xxh h;
    pl_xxh_init(&h);
    for (uint64_t i = 0; i < n; i++) {
        uint8_t b[8];
        put64(b, sums[i]);
        pl_xxh_update(&h, b, 8);
    }
    return pl_xxh_final(&h);
}

uint64_t cs_put_answer_hash(const uint8_t meta[CS_PUT_META_LEN], uint64_t sums_hash)
{
    if (chunksrv_mutant == CS_MUT_SUMSONLY) /* planted: the identity the receiver took is not covered */
        return sums_hash;
    struct pl_xxh h;
    uint8_t b[8];
    put64(b, sums_hash);
    pl_xxh_init(&h);
    pl_xxh_update(&h, meta, CS_PUT_META_LEN);
    pl_xxh_update(&h, b, 8);
    return pl_xxh_final(&h);
}

/* the receive-side flip of the tests (chunksrv_test_flip) */
static _Atomic int flip_where;
static uint32_t flip_off;
static uint8_t flip_mask;
static bool flip_rehash;

void chunksrv_test_flip(int where, uint32_t off, uint8_t mask, bool rehash)
{
    flip_off = off, flip_mask = mask, flip_rehash = rehash;
    atomic_store(&flip_where, where);
}

/* p, n: a frame of kind `where` as received; a copy flipped once when the test armed it (to be freed), else NULL */
static uint8_t *flip_take(int where, const void *p, size_t n)
{
    int w = where;
    if (atomic_load_explicit(&flip_where, memory_order_relaxed) != where || flip_off >= n ||
        !atomic_compare_exchange_strong(&flip_where, &w, CS_FLIP_NONE))
        return NULL;
    uint8_t *c = malloc(n);
    if (!c)
        return NULL;
    memcpy(c, p, n);
    c[flip_off] ^= flip_mask;
    if (flip_rehash && where == CS_FLIP_META && n == CS_PUT_META_LEN)
        put64(c + 66, xh(c, 66));
    else if (flip_rehash && where == CS_FLIP_REQ && n > CS_CHECK_LEN)
        put64(c + n - CS_CHECK_LEN, xh(c, n - CS_CHECK_LEN));
    else if (flip_rehash && where == CS_FLIP_RESP && n >= RESP_HDR)
        cs_resp_seal(c, n);
    return c;
}

int chunksrv_st(int rc)
{
    switch (rc) {
    case 0: return CHUNK_ST_OK;
    case TSDB_ERR_NOTFOUND: return CHUNK_ST_ABSENT;
    case TSDB_ERR_CORRUPT: return CHUNK_ST_DAMAGED;
    case TSDB_ERR_IO: return CHUNK_ST_IOERR;
    case TSDB_ERR_UNAVAILABLE: return CHUNK_ST_DOWN;
    default: return -1;
    }
}

/* ---------------------------------------------------------------- the server */
#define CS_PEND_BUCKETS 512u

struct cs_job {
    struct cs_job *next;
    int commit;          /* 1: put is to be committed; 0: a request frame */
    struct cs_put *put;
    struct plink *l;
    uint64_t gen;
    uint32_t peer;
    size_t n;
    struct cs_req req;   /* decoded and checked once in on_frame_chunk */
};

struct cs_pend {
    struct cs_pend *next;
    pthread_cond_t cv; /* per-request CLOCK_MONOTONIC condvar */
    struct plink *link;
    uint64_t gen;
    uint64_t rid;
    uint8_t op; /* the answer's op must be this one */
    int refs; /* the caller, and a bulk job while it lives */
    bool done, aborted, job_done;
    int job_status;
    int32_t rc;
    uint8_t *body;
    size_t body_len;
    struct timespec job_at; /* when the job ended */
};

struct cs_peer {
    uint32_t id;
    char addr[128];
    struct plink *lc, *lb; /* the dialed CHUNK and CHUNKB links */
    bool gone;             /* the member left: its links are stopped (plink_stop) */
};

/* A CHUNKB callback as a job for the put thread of its peer: OPEN, DATA and END in the link's order. */
enum { BJ_OPEN, BJ_DATA, BJ_END };
struct cs_bjob {
    struct cs_bjob *next;
    int kind, status;
    uint64_t file, off, total;
    size_t n;
    uint8_t p[]; /* OPEN: the meta; DATA: the bytes */
};

/* The puts from one peer. CHUNKB's callbacks only queue; the put thread does the store I/O (the EPOCH raise, the cell
 * writes, the commit's syncs), so a slow disk never blocks the link's thread past dead_ms (peerlink.h:24-26). One put
 * at a time per peer, each in the order the link gave it; a queue above CS_PUT_QUEUE_BYTES pauses the link (TCP holds
 * the sender back), so neither memory nor open tmp files grow with a slow disk. */
struct cs_lctx {
    struct chunksrv *cs;
    uint32_t peer;
    struct cs_put *cur; /* the put thread's own */
    pthread_t thr;
    bool started, stop;
    pthread_mutex_t mu;
    pthread_cond_t cv;
    struct cs_bjob *h, *t;
    size_t bytes;
    struct plink *paused;
    _Atomic uint64_t refused; /* file + 1 of the put answered with a refusal: the rest of its bytes are refused */
    struct cs_lctx *next;
};

struct chunksrv {
    chunk_backend *be; /* NULL: no store on this member */
    int n;
    char **paths;
    _Atomic(chunksrv_epoch_fn) src;
    _Atomic(void *) src_arg;
    struct chunksrv_stats st;

    /* the network half (chunksrv_listen) */
    struct peerlink *pl;
    pthread_mutex_t qmu;
    pthread_cond_t qcv;
    struct cs_job *qh, *qt;
    size_t qbytes;
    struct plink **paused;
    size_t npaused, cpaused;
    bool closing;
    pthread_t *workers;
    int nworkers;
    pthread_mutex_t cmu; /* pending calls, peers, link contexts */
    struct cs_pend *pend[CS_PEND_BUCKETS];
    struct cs_peer *peers;
    size_t npeers, cpeers;
    struct cs_lctx *lctx;
    _Atomic uint64_t rid;
    bool listening;
};

static inline size_t pend_bucket(uint64_t rid)
{
    uint64_t h = rid * 0x9e3779b97f4a7c15ull;
    return (size_t)((h ^ (h >> 32)) & (CS_PEND_BUCKETS - 1u));
}

#define STAT(cs, f) __atomic_add_fetch(&(cs)->st.f, 1, __ATOMIC_RELAXED)
#define STATN(cs, f, v) __atomic_add_fetch(&(cs)->st.f, (v), __ATOMIC_RELAXED)
#define CS_QUEUE_BYTES (32u << 20)
#define CS_PUT_QUEUE_BYTES (16u << 20)
int chunksrv_test_put_delay_ms; /* test hook: the first cell of every put waits this long (a slow disk) */

struct chunksrv *chunksrv_open(const char *const *paths, int n, char *err, size_t errlen)
{
    struct chunksrv *cs = calloc(1, sizeof *cs);
    if (!cs) {
        snprintf(err, errlen, "out of memory");
        return NULL;
    }
    pthread_mutex_init(&cs->qmu, NULL);
    pthread_cond_init(&cs->qcv, NULL);
    pthread_mutex_init(&cs->cmu, NULL);
    uint64_t r;
    if (chunk_random(&r, sizeof r) != 0) { /* a restarted client's ids never meet a late answer to its previous life */
        snprintf(err, errlen, "cannot read random bytes");
        chunksrv_close(cs);
        return NULL;
    }
    atomic_store(&cs->rid, r);
    if (n > 0) {
        cs->paths = calloc((size_t)n, sizeof *cs->paths);
        for (int i = 0; cs->paths && i < n; i++)
            cs->paths[i] = strdup(paths[i]);
        cs->n = n;
        chunk_be_opts o = {.min_up = 0};
        int rc = chunk_be_open2(&cs->be, paths, n, NULL, &o);
        if (rc != 0) {
            snprintf(err, errlen, "the chunk stores cannot be opened: %s",
                     rc == CHUNK_ERR_BUSY ? "one is open elsewhere (another tsdbd owns it)"
                     : rc == TSDB_ERR_INVALID ? "a directory or store named twice"
                                              : "error");
            chunksrv_close(cs);
            return NULL;
        }
    }
    return cs;
}

void chunksrv_close(struct chunksrv *cs)
{
    if (!cs)
        return;
    if (cs->listening)
        chunksrv_unlisten(cs);
    if (cs->be)
        chunk_be_close(cs->be);
    for (int i = 0; cs->paths && i < cs->n; i++)
        free(cs->paths[i]);
    free(cs->paths);
    pthread_mutex_destroy(&cs->qmu);
    pthread_cond_destroy(&cs->qcv);
    pthread_mutex_destroy(&cs->cmu);
    free(cs);
}

chunk_backend *chunksrv_backend(struct chunksrv *cs) { return cs->be; }

int chunksrv_check_owner(struct chunksrv *cs, const uint8_t cluster[16], uint32_t node, char *err, size_t errlen)
{
    for (int i = 0; cs->be && i < cs->n; i++) {
        chunk_store *st = chunk_be_store(cs->be, i);
        if (!st)
            continue;
        chunk_store_owner o;
        chunk_store_get_owner(st, &o);
        if (chunksrv_mutant == CS_MUT_STOREFOREIGN) /* planted: any store is taken */
            continue;
        char want[33], have[33];
        chunk_oid_hex(cluster, want);
        if (!o.set) {
            snprintf(err, errlen,
                     "chunk store %s names no cluster and node; this is cluster %s node %" PRIu32
                     " (make it with tsdb chunk-store init PATH --cluster UUID --node ID)",
                     cs->paths[i], want, node);
            return -1;
        }
        chunk_oid_hex(o.cluster, have);
        if (memcmp(o.cluster, cluster, 16) != 0 || o.node != node) {
            snprintf(err, errlen,
                     "chunk store %s belongs to cluster %s node %" PRIu32 ", but this is cluster %s node %" PRIu32
                     " (a disk moved between members or clusters)",
                     cs->paths[i], have, o.node, want, node);
            return -1;
        }
    }
    return 0;
}

int chunksrv_stores(struct chunksrv *cs, int *up, bool log)
{
    int u = 0;
    for (int i = 0; cs->be && i < cs->n; i++) {
        int s = chunk_be_store_state(cs->be, i); /* a store failed in place is DOWN_IOERR there (E57) */
        u += s == CHUNK_STORE_UP;
        if (log)
            fprintf(stderr, "tsdbd: chunk store %s: %s\n", cs->paths[i], chunk_store_state_name(s));
    }
    if (up)
        *up = u;
    return cs->n;
}

void chunksrv_epoch_source(struct chunksrv *cs, chunksrv_epoch_fn fn, void *arg)
{
    atomic_store(&cs->src_arg, arg);
    atomic_store(&cs->src, fn);
}

/* Each counter is read on its own (relaxed, as STAT writes them): a plain struct copy races the workers. */
void chunksrv_get_stats(struct chunksrv *cs, struct chunksrv_stats *st)
{
#define LD(f) st->f = __atomic_load_n(&cs->st.f, __ATOMIC_RELAXED)
    LD(requests); LD(malformed); LD(puts_ok); LD(puts_refused); LD(put_bytes); LD(reads); LD(read_bytes); LD(fenced);
    LD(unsure_refused); LD(source_raises); LD(answer_mismatch);
#undef LD
}

static bool any_unsure(struct chunksrv *cs, uint32_t volume)
{
    for (int i = 0; i < cs->n; i++) {
        chunk_store *st = chunk_be_store(cs->be, i);
        if (st && chunk_store_epoch_unsure(st, volume))
            return true;
    }
    return false;
}

/* E14: a request raises the fence of its volume to its epoch first. An unsure store only from the authoritative source
 * (D9); the stores that are sure are raised all the same. 0, CHUNK_ERR_EPOCH_UNSURE (only for a put or a delete: a read
 * goes on), or the error of a store that could not store it. */
static int cs_see(struct chunksrv *cs, uint32_t volume, uint64_t e, bool mutating)
{
    if (any_unsure(cs, volume)) {
        chunksrv_epoch_fn fn = atomic_load(&cs->src);
        uint64_t auth = 0;
        if (fn && fn(atomic_load(&cs->src_arg), volume, &auth) == 1) {
            /* One fence across the member's stores (E14): a request epoch the sure stores took while this one was unsure
             * fences its writers here too, or a writer refused on a sure store would be taken on this one. */
            uint64_t to = auth, be_e = chunk_be_epoch(cs->be, volume);
            to = be_e > to ? be_e : to;
            for (int i = 0; i < cs->n; i++) {
                chunk_store *st = chunk_be_store(cs->be, i);
                uint64_t se = st && !chunk_store_epoch_unsure(st, volume) ? chunk_store_epoch(st, volume) : 0;
                to = se > to ? se : to;
            }
            int rc = chunk_be_see_epoch(cs->be, volume, to);
            if (rc != 0)
                return rc;
            STAT(cs, source_raises);
        } else if (chunksrv_mutant == CS_MUT_SELFRAISE) { /* planted: a request's epoch taken as authoritative */
            int rc = chunk_be_see_epoch(cs->be, volume, e);
            if (rc != 0)
                return rc;
        }
    }
    if (any_unsure(cs, volume)) {
        int rc = 0;
        for (int i = 0; i < cs->n && rc == 0; i++) {
            chunk_store *st = chunk_be_store(cs->be, i);
            int r = st ? chunk_store_see(st, volume, e) : 0;
            if (r != 0 && r != CHUNK_ERR_EPOCH_UNSURE)
                rc = r;
        }
        if (rc == 0 && mutating) {
            STAT(cs, unsure_refused);
            rc = CHUNK_ERR_EPOCH_UNSURE;
        }
        return rc;
    }
    return e > chunk_be_epoch(cs->be, volume) ? chunk_be_see_epoch(cs->be, volume, e) : 0;
}

/* the epoch a put or a delete is made at */
static uint64_t act_epoch(struct chunksrv *cs, uint32_t volume, uint64_t e)
{
    if (chunksrv_mutant == CS_MUT_NOFENCE) { /* planted: the store's own epoch, so a fenced writer passes */
        uint64_t f = chunk_be_epoch(cs->be, volume);
        return f > e ? f : e;
    }
    return e;
}

static void reply_rc(cs_reply_fn reply, void *arg, uint64_t rid, uint8_t op, int32_t rc, const void *body, size_t n)
{
    uint8_t stackbuf[256];
    uint8_t *b = RESP_HDR + n <= sizeof stackbuf ? stackbuf : malloc(RESP_HDR + n);
    if (!b)
        return; /* the requester times out: nothing was promised */
    put64(b, rid);
    b[8] = op;
    put32(b + 9, (uint32_t)rc);
    if (n)
        memcpy(b + RESP_HDR, body, n);
    cs_resp_seal(b, RESP_HDR + n);
    reply(arg, b, RESP_HDR + n);
    if (b != stackbuf)
        free(b);
}

static void ident_of(const struct cs_req *r, chunk_ident *id)
{
    memset(id, 0, sizeof *id);
    id->volume = r->volume;
    memcpy(id->vuuid, r->vuuid, 16);
    memcpy(id->oid, r->oid, 16);
    id->pos = r->pos;
}

static void do_read(struct chunksrv *cs, const struct cs_req *r, cs_reply_fn reply, void *arg)
{
    chunk_ident id;
    ident_of(r, &id);
    chunk_rd *rd = NULL;
    int rc = chunk_be_rd_open(cs->be, &id, &rd);
    if (rc == 0) {
        const chunk_hdr *h = chunk_rd_hdr(rd);
        if (r->off > h->chunk_len || r->len > h->chunk_len - r->off)
            rc = TSDB_ERR_INVALID;
    }
    uint8_t *b = rc == 0 ? malloc(RESP_HDR + r->len) : NULL;
    if (rc == 0 && !b)
        rc = TSDB_ERR_NOMEM;
    if (rc == 0)
        rc = chunk_rd_read(rd, r->off, b + RESP_HDR, r->len);
    if (rd)
        chunk_rd_close(rd);
    if (rc == 0) {
        put64(b, r->rid);
        b[8] = CS_OP_READ;
        put32(b + 9, 0);
        cs_resp_seal(b, RESP_HDR + r->len);
        reply(arg, b, RESP_HDR + r->len);
        STAT(cs, reads);
        STATN(cs, read_bytes, r->len);
    } else {
        reply_rc(reply, arg, r->rid, CS_OP_READ, rc, NULL, 0);
    }
    free(b);
}

static void do_sums(struct chunksrv *cs, const struct cs_req *r, cs_reply_fn reply, void *arg)
{
    chunk_ident id;
    ident_of(r, &id);
    chunk_rd *rd = NULL;
    int rc = chunk_be_rd_open(cs->be, &id, &rd);
    uint8_t *b = NULL;
    size_t bl = 0;
    if (rc == 0) {
        const chunk_hdr *h = chunk_rd_hdr(rd);
        uint64_t nc = chunk_pos_cells(&h->g, h->pos);
        if (r->off > nc)
            rc = TSDB_ERR_INVALID;
        else {
            uint64_t k = nc - r->off < r->len ? nc - r->off : r->len;
            bl = 36 + 8 * k;
            b = malloc(bl);
            if (!b)
                rc = TSDB_ERR_NOMEM;
            else {
                const uint64_t *s = chunk_rd_sums(rd);
                put64(b, h->epoch);
                put16(b + 8, h->g.k);
                put16(b + 10, h->g.m);
                put32(b + 12, h->g.cell);
                put64(b + 16, h->g.len);
                put64(b + 24, nc);
                put32(b + 32, (uint32_t)k);
                for (uint64_t i = 0; i < k; i++)
                    put64(b + 36 + 8 * i, s[r->off + i]);
            }
        }
    }
    if (rd)
        chunk_rd_close(rd);
    reply_rc(reply, arg, r->rid, CS_OP_SUMS, rc, b, rc == 0 ? bl : 0);
    free(b);
}

struct lvec {
    chunk_listed *v;
    size_t n, cap;
    int err;
};

static int lcollect(void *ctx, const chunk_listed *c)
{
    struct lvec *lv = ctx;
    if (lv->n == lv->cap) {
        size_t nc = lv->cap ? 2 * lv->cap : 256;
        chunk_listed *v = realloc(lv->v, nc * sizeof *v);
        if (!v) {
            lv->err = TSDB_ERR_NOMEM;
            return 1;
        }
        lv->v = v, lv->cap = nc;
    }
    lv->v[lv->n++] = *c;
    return 0;
}

static int lcmp_key(const uint8_t *oa, unsigned pa, const uint8_t *ob, unsigned pb)
{
    int c = memcmp(oa, ob, 16);
    return c ? c : (pa > pb) - (pa < pb);
}

static int lcmp(const void *a, const void *b)
{
    const chunk_listed *x = a, *y = b;
    int c = lcmp_key(x->oid, x->pos, y->oid, y->pos);
    return c ? c : (x->store > y->store) - (x->store < y->store);
}

static bool lsame(const chunk_listed *a, const chunk_listed *b) { return lcmp_key(a->oid, a->pos, b->oid, b->pos) == 0; }

/* A page is made from the names (one readdir per store, no chunk opened) and only its own entries' headers are read:
 * paging a volume costs one header per chunk in all, not one per chunk per page. The cursor names (oid, pos) and no
 * store, so a page never splits the copies of one (oid, pos) between two stores: it ends before them, or (when they
 * alone are more than the page holds, at most one a store) holds them all. */
static void do_list(struct chunksrv *cs, const struct cs_req *r, cs_reply_fn reply, void *arg)
{
    struct lvec lv = {0};
    int rc = chunk_be_list_names(cs->be, r->volume, lcollect, &lv);
    if (rc > 0 || lv.err)
        rc = lv.err ? lv.err : TSDB_ERR_IO;
    uint8_t *b = NULL;
    size_t bl = 0;
    if (rc == 0) {
        if (lv.n > 1)
            qsort(lv.v, lv.n, sizeof *lv.v, lcmp);
        size_t i = 0;
        if (r->has_cursor)
            while (i < lv.n && lcmp_key(lv.v[i].oid, lv.v[i].pos, r->oid, r->pos) <= 0)
                i++;
        size_t k = lv.n - i < r->len ? lv.n - i : r->len;
        if (k > 0 && i + k < lv.n && lsame(&lv.v[i + k - 1], &lv.v[i + k])) {
            size_t s = i + k - 1;
            while (s > i && lsame(&lv.v[s - 1], &lv.v[s]))
                s--;
            if (s > i)
                k = s - i;
            else
                while (i + k < lv.n && lsame(&lv.v[i + k - 1], &lv.v[i + k]))
                    k++;
        }
        bl = 5 + CS_LIST_ENTRY * k;
        b = malloc(bl);
        if (!b)
            rc = TSDB_ERR_NOMEM;
        else {
            put32(b, (uint32_t)k);
            b[4] = i + k < lv.n;
            for (size_t j = 0; j < k; j++) {
                const chunk_listed *nm = &lv.v[i + j];
                chunk_listed full, *c = &full;
                chunk_store *st = chunk_be_store(cs->be, nm->store);
                int lr = st ? chunk_store_list_one(st, r->volume, nm->oid, nm->pos, false, c) : TSDB_ERR_UNAVAILABLE;
                if (lr != 0) { /* removed since the names were read (ABSENT), or the store went */
                    memset(c, 0, sizeof *c);
                    memcpy(c->oid, nm->oid, 16);
                    c->pos = nm->pos;
                    c->status = lr == TSDB_ERR_NOTFOUND      ? CHUNK_ST_ABSENT
                                : lr == TSDB_ERR_UNAVAILABLE ? CHUNK_ST_DOWN
                                                             : CHUNK_ST_IOERR;
                }
                c->store = nm->store;
                uint8_t *e = b + 5 + CS_LIST_ENTRY * j;
                memcpy(e, c->oid, 16);
                put16(e + 16, c->pos);
                put64(e + 18, c->size);
                put64(e + 26, c->epoch);
                memcpy(e + 34, c->vuuid, 16);
                e[50] = (uint8_t)c->status;
                e[51] = (uint8_t)c->store;
            }
        }
    }
    free(lv.v);
    reply_rc(reply, arg, r->rid, CS_OP_LIST, rc, b, rc == 0 ? bl : 0);
    free(b);
}

static int chunksrv_handle_req(struct chunksrv *cs, uint32_t peer, const struct cs_req *r, cs_reply_fn reply, void *arg)
{
    (void)peer;
    STAT(cs, requests);
    if (!cs->be) {
        reply_rc(reply, arg, r->rid, r->op, TSDB_ERR_UNAVAILABLE, NULL, 0);
        return 0;
    }
    bool mutating = r->op == CS_OP_DEL;
    int rc = cs_see(cs, r->volume, r->epoch, mutating);
    if (rc != 0) {
        reply_rc(reply, arg, r->rid, r->op, rc, NULL, 0);
        return 0;
    }
    switch (r->op) {
    case CS_OP_READ: do_read(cs, r, reply, arg); break;
    case CS_OP_SUMS: do_sums(cs, r, reply, arg); break;
    case CS_OP_LIST: do_list(cs, r, reply, arg); break;
    case CS_OP_DEL: {
        chunk_ident id;
        ident_of(r, &id);
        rc = chunk_be_del(cs->be, &id, act_epoch(cs, r->volume, r->epoch));
        if (rc == CHUNK_ERR_FENCED)
            STAT(cs, fenced);
        reply_rc(reply, arg, r->rid, CS_OP_DEL, rc, NULL, 0);
        break;
    }
    case CS_OP_EPOCH: {
        uint8_t b[9];
        put64(b, chunk_be_epoch(cs->be, r->volume));
        b[8] = any_unsure(cs, r->volume);
        reply_rc(reply, arg, r->rid, CS_OP_EPOCH, 0, b, sizeof b);
        break;
    }
    }
    return 0;
}

int chunksrv_handle(struct chunksrv *cs, uint32_t peer, const uint8_t *p, size_t n, cs_reply_fn reply, void *arg)
{
    struct cs_req r;
    if (cs_req_decode(p, n, &r) != 0) {
        STAT(cs, malformed);
        return -1;
    }
    return chunksrv_handle_req(cs, peer, &r, reply, arg);
}

/* ---------------------------------------------------------------- puts */
struct cs_put {
    struct chunksrv *cs;
    uint64_t file;
    struct cs_put_meta m;
    chunk_wr *w;
    uint64_t total, got, ncells, cell_i;
    uint64_t *sums;
    uint8_t *cell;     /* the cell being assembled */
    uint64_t cell_have, cell_len;
    uint8_t meta[CS_PUT_META_LEN]; /* as received and checked: PUT_OK's hash covers it */
    int rc;            /* the first failure (answered) */
    bool answered;
    cs_reply_fn reply;
    void *arg;
};

static void put_answer(struct cs_put *p, int rc)
{
    if (p->answered)
        return;
    p->answered = true;
    uint8_t b[8] = {0};
    if (rc == 0)
        put64(b, cs_put_answer_hash(p->meta, cs_sums_hash(p->sums, p->ncells)));
    reply_rc(p->reply, p->arg, p->file, CS_OP_PUT, rc, b, rc == 0 ? 8 : 0);
    if (rc == 0) {
        STAT(p->cs, puts_ok);
    } else {
        STAT(p->cs, puts_refused);
        if (rc == CHUNK_ERR_FENCED)
            STAT(p->cs, fenced);
    }
}

static void put_free(struct cs_put *p)
{
    if (p->w)
        chunk_wr_abort(p->w);
    free(p->sums);
    free(p->cell);
    free(p);
}

int chunksrv_put_begin(struct chunksrv *cs, uint32_t peer, uint64_t file, const void *meta, size_t meta_len,
                       uint64_t total, cs_reply_fn reply, void *arg, struct cs_put **out)
{
    (void)peer;
    *out = NULL;
    struct cs_put *p = calloc(1, sizeof *p);
    if (!p)
        return TSDB_ERR_NOMEM;
    p->cs = cs, p->file = file, p->reply = reply, p->arg = arg;
    uint8_t *flipped = flip_take(CS_FLIP_META, meta, meta_len);
    if (flipped)
        meta = flipped;
    /* The meta's own check comes first: the fence raise below is durable, and nothing is raised from a meta that
     * fails it (the data's checks, the cells against their sums, come later and cannot vouch for the epoch). */
    int rc = cs_put_meta_decode(meta, meta_len, &p->m) == 0 && total == cs_put_total(&p->m) ? 0 : TSDB_ERR_INVALID;
    if (rc == 0)
        memcpy(p->meta, meta, CS_PUT_META_LEN);
    free(flipped);
    if (rc == 0 && !cs->be)
        rc = TSDB_ERR_UNAVAILABLE;
    if (rc == 0) {
        p->total = total;
        p->ncells = chunk_pos_cells(&p->m.g, p->m.pos);
        p->sums = malloc((p->ncells ? p->ncells : 1) * 8);
        p->cell = malloc(p->m.g.cell);
        if (!p->sums || !p->cell)
            rc = TSDB_ERR_NOMEM;
    }
    if (rc == 0)
        rc = cs_see(cs, p->m.volume, p->m.epoch, true);
    if (rc == 0) {
        chunk_ident id = {.volume = p->m.volume, .pos = p->m.pos};
        memcpy(id.vuuid, p->m.vuuid, 16);
        memcpy(id.oid, p->m.oid, 16);
        rc = chunk_be_wr_open(cs->be, -1, &id, &p->m.g, act_epoch(cs, p->m.volume, p->m.epoch), &p->w);
    }
    if (rc != 0) {
        put_answer(p, rc);
        put_free(p);
        return rc;
    }
    if (p->ncells)
        p->cell_len = chunk_pos_cell_len(&p->m.g, p->m.pos, 0);
    *out = p;
    return 0;
}

int chunksrv_put_data(struct cs_put *p, uint64_t off, const void *d, size_t n)
{
    if (p->rc)
        return p->rc;
    if (off != p->got || n > p->total - p->got) {
        p->rc = TSDB_ERR_INVALID;
        put_answer(p, p->rc);
        return p->rc;
    }
    const uint8_t *s = d;
    uint64_t sums_bytes = 8 * p->ncells;
    while (n > 0) {
        if (p->got < sums_bytes) { /* the sums table: 8 bytes a cell, any split */
            uint64_t k = sums_bytes - p->got < n ? sums_bytes - p->got : n;
            for (uint64_t i = 0; i < k; i++) {
                uint64_t at = p->got + i;
                uint64_t *v = &p->sums[at / 8];
                if (at % 8 == 0)
                    *v = 0;
                *v |= (uint64_t)s[i] << (8 * (at % 8));
            }
            p->got += k, s += k, n -= (size_t)k;
            continue;
        }
        if (p->cell_i == 0 && p->cell_have == 0 && chunksrv_test_put_delay_ms > 0) {
            int ms = chunksrv_test_put_delay_ms;
            nanosleep(&(struct timespec){ms / 1000, (long)(ms % 1000) * 1000000L}, NULL);
        }
        uint64_t k = p->cell_len - p->cell_have < n ? p->cell_len - p->cell_have : n;
        memcpy(p->cell + p->cell_have, s, k);
        p->cell_have += k, p->got += k, s += k, n -= (size_t)k;
        if (p->cell_have == p->cell_len) {
            int rc = chunksrv_mutant == CS_MUT_NOCELLCHECK /* planted: the sender's sum taken as it is */
                         ? chunk_wr_cell_trusted(p->w, p->cell, p->cell_len, p->sums[p->cell_i])
                         : chunk_wr_cell(p->w, p->cell, p->cell_len, true, p->sums[p->cell_i]);
            if (rc != 0) {
                p->rc = rc;
                put_answer(p, rc);
                return rc;
            }
            p->cell_i++;
            p->cell_have = 0;
            if (p->cell_i < p->ncells)
                p->cell_len = chunk_pos_cell_len(&p->m.g, p->m.pos, p->cell_i);
        }
    }
    return 0;
}

void chunksrv_put_end(struct cs_put *p, int status)
{
    if (status == 0 && p->rc == 0) {
        int rc = p->got == p->total && p->cell_i == p->ncells ? 0 : TSDB_ERR_INVALID;
        if (rc == 0) {
            if (chunksrv_mutant == CS_MUT_PUTOKEARLY) /* planted: acknowledged before it is durable */
                put_answer(p, 0);
            rc = chunk_wr_commit(p->w);
            p->w = NULL; /* commit releases the writer, whatever it returns */
        }
        if (rc == 0)
            STATN(p->cs, put_bytes, p->total);
        put_answer(p, rc);
    }
    put_free(p);
}

/* ---------------------------------------------------------------- on a peerlink */
static void q_push(struct chunksrv *cs, struct cs_job *j, struct plink *pause_l)
{
    pthread_mutex_lock(&cs->qmu);
    j->next = NULL;
    if (cs->qt)
        cs->qt->next = j;
    else
        cs->qh = j;
    cs->qt = j;
    cs->qbytes += j->n;
    if (pause_l && cs->qbytes > CS_QUEUE_BYTES) { /* TCP pushes back on the peer until the workers catch up */
        if (cs->npaused == cs->cpaused) {
            size_t nc = cs->cpaused ? 2 * cs->cpaused : 8;
            struct plink **v = realloc(cs->paused, nc * sizeof *v);
            if (v)
                cs->paused = v, cs->cpaused = nc;
        }
        if (cs->npaused < cs->cpaused) {
            cs->paused[cs->npaused++] = pause_l;
            plink_pause(pause_l, 1);
        }
    }
    pthread_cond_signal(&cs->qcv);
    pthread_mutex_unlock(&cs->qmu);
}

struct rctx {
    struct plink *l;
    uint64_t gen;
};

static void reply_link(void *arg, const uint8_t *p, size_t n)
{
    struct rctx *c = arg;
    plink_send_gen(c->l, c->gen, CS_T_RESP, p, n, 1000); /* a lost answer is the requester's timeout */
}

static void reply_put(void *arg, const uint8_t *p, size_t n)
{
    struct cs_lctx *x = arg;
    struct plink *l = peerlink_find(x->cs->pl, x->peer, PL_KIND_CHUNK);
    if (l)
        plink_send(l, CS_T_RESP, p, n, 1000);
}

static void *worker_main(void *arg)
{
    struct chunksrv *cs = arg;
    pthread_mutex_lock(&cs->qmu);
    for (;;) {
        while (!cs->qh && !cs->closing)
            pthread_cond_wait(&cs->qcv, &cs->qmu);
        if (cs->closing)
            break;
        struct cs_job *j = cs->qh;
        cs->qh = j->next;
        if (!cs->qh)
            cs->qt = NULL;
        cs->qbytes -= j->n;
        if (cs->npaused && cs->qbytes < CS_QUEUE_BYTES / 2) {
            for (size_t i = 0; i < cs->npaused; i++)
                plink_pause(cs->paused[i], 0);
            cs->npaused = 0;
        }
        pthread_mutex_unlock(&cs->qmu);
        if (j->commit) {
            chunksrv_put_end(j->put, 0);
        } else {
            struct rctx c = {.l = j->l, .gen = j->gen};
            chunksrv_handle_req(cs, j->peer, &j->req, reply_link, &c);
        }
        free(j);
        pthread_mutex_lock(&cs->qmu);
    }
    pthread_mutex_unlock(&cs->qmu);
    return NULL;
}

static void on_link_chunk(void *arg, struct plink *l, int up)
{
    if (up || plink_kind(l) != PL_KIND_CHUNK)
        return;
    struct chunksrv *cs = arg;
    uint64_t gen = plink_gen(l);
    pthread_mutex_lock(&cs->cmu);
    for (size_t b = 0; b < CS_PEND_BUCKETS; b++)
        for (struct cs_pend *q = cs->pend[b]; q; q = q->next)
            if (q->link == l && q->gen <= gen && !q->done && !q->aborted) {
                q->aborted = true;
                pthread_cond_signal(&q->cv);
            }
    pthread_mutex_unlock(&cs->cmu);
}

static void on_frame_chunk(struct chunksrv *cs, struct plink *l, int kind, uint8_t type, const uint8_t *p, size_t n);

static void on_frame(void *arg, struct plink *l, uint8_t type, const uint8_t *p, size_t n)
{
    struct chunksrv *cs = arg;
    int kind = plink_kind(l);
    uint8_t *flipped = kind == PL_KIND_CHUNK && (type == CS_T_REQ || type == CS_T_RESP)
                           ? flip_take(type == CS_T_REQ ? CS_FLIP_REQ : CS_FLIP_RESP, p, n)
                           : NULL;
    if (flipped)
        p = flipped;
    on_frame_chunk(cs, l, kind, type, p, n);
    free(flipped);
}

static void on_frame_chunk(struct chunksrv *cs, struct plink *l, int kind, uint8_t type, const uint8_t *p, size_t n)
{
    if (kind == PL_KIND_CHUNK && type == CS_T_REQ) {
        struct cs_req r;
        if (cs_req_decode(p, n, &r) != 0)
            goto bad;
        struct cs_job *j = malloc(sizeof *j);
        if (!j)
            return; /* the requester times out */
        *j = (struct cs_job){.l = l, .gen = plink_gen(l), .peer = plink_peer(l), .n = n, .req = r};
        q_push(cs, j, l);
        return;
    }
    if (kind == PL_KIND_CHUNK && type == CS_T_RESP) {
        struct cs_resp r;
        if (cs_resp_decode(p, n, &r) != 0)
            goto bad;
        pthread_mutex_lock(&cs->cmu);
        for (struct cs_pend *q = cs->pend[pend_bucket(r.rid)]; q; q = q->next)
            if (q->rid == r.rid && !q->done) {
                if (q->op != r.op && chunksrv_mutant != CS_MUT_NOOPMATCH) {
                    /* another call's answer (a changed rid or op): neither call takes it, the link ends. The
                     * call's op is copied under cmu: once it is let go of, the caller may free q */
                    unsigned qop = q->op;
                    pthread_mutex_unlock(&cs->cmu);
                    STAT(cs, answer_mismatch);
                    fprintf(stderr, "tsdbd: chunk link from node %" PRIu32 ": an answer of op %u for a call of op %u: "
                            "link ended\n", plink_peer(l), r.op, qop);
                    plink_reset(l);
                    return;
                }
                q->rc = r.rc;
                q->body = r.body_len ? malloc(r.body_len) : NULL;
                if (q->body)
                    memcpy(q->body, r.body, r.body_len);
                q->body_len = q->body ? r.body_len : 0;
                q->done = true;
                pthread_cond_signal(&q->cv);
                break;
            }
        pthread_mutex_unlock(&cs->cmu); /* an answer nobody waits for (a call that timed out) is dropped */
        return;
    }
bad:
    STAT(cs, malformed);
    fprintf(stderr, "tsdbd: chunk link from node %" PRIu32 ": a malformed frame (type %u, %zu bytes): link ended\n",
            plink_peer(l), type, n);
    plink_reset(l);
}

static void put_run(struct cs_lctx *x, struct cs_bjob *j)
{
    switch (j->kind) {
    case BJ_OPEN:
        if (x->cur) { /* the same file again after a reconnect, or a file after one that was cut: from the start */
            chunksrv_put_end(x->cur, PL_E_DOWN);
            x->cur = NULL;
        }
        if (chunksrv_put_begin(x->cs, x->peer, j->file, j->p, j->n, j->total, reply_put, x, &x->cur) != 0)
            atomic_store(&x->refused, j->file + 1);
        break;
    case BJ_DATA:
        if (x->cur && x->cur->file == j->file && chunksrv_put_data(x->cur, j->off, j->p, j->n) != 0)
            atomic_store(&x->refused, j->file + 1);
        break;
    case BJ_END:
        if (x->cur && x->cur->file == j->file) {
            chunksrv_put_end(x->cur, j->status);
            x->cur = NULL;
        }
        break;
    }
}

static void *put_main(void *arg)
{
    struct cs_lctx *x = arg;
    pthread_mutex_lock(&x->mu);
    for (;;) {
        while (!x->h && !x->stop)
            pthread_cond_wait(&x->cv, &x->mu);
        if (x->stop)
            break;
        struct cs_bjob *j = x->h;
        x->h = j->next;
        if (!x->h)
            x->t = NULL;
        x->bytes -= j->n;
        if (x->paused && x->bytes < CS_PUT_QUEUE_BYTES / 2) {
            plink_pause(x->paused, 0);
            x->paused = NULL;
        }
        pthread_mutex_unlock(&x->mu);
        put_run(x, j);
        free(j);
        pthread_mutex_lock(&x->mu);
    }
    pthread_mutex_unlock(&x->mu);
    return NULL;
}

/* The peer's put queue, made (with its thread) at the peer's first CHUNKB link; kept for the server's life. */
static struct cs_lctx *lctx_of(struct chunksrv *cs, struct plink *l)
{
    struct cs_lctx *x = plink_arg(l);
    if (x)
        return x;
    uint32_t peer = plink_peer(l); /* an accepted link is (peer, kind) for its life */
    pthread_mutex_lock(&cs->cmu);
    for (x = cs->lctx; x && x->peer != peer; x = x->next)
        ;
    if (!x && (x = calloc(1, sizeof *x)) != NULL) {
        x->cs = cs;
        x->peer = peer;
        pthread_mutex_init(&x->mu, NULL);
        pthread_cond_init(&x->cv, NULL);
        x->started = pthread_create(&x->thr, NULL, put_main, x) == 0;
        x->next = cs->lctx;
        cs->lctx = x;
    }
    pthread_mutex_unlock(&cs->cmu);
    if (x && !x->started)
        return NULL;
    if (x)
        plink_set_arg(l, x);
    return x;
}

static int b_push(struct plink *l, int kind, int status, uint64_t file, uint64_t off, uint64_t total, const void *p,
                  size_t n)
{
    struct cs_lctx *x = plink_arg(l);
    if (!x)
        return -1;
    struct cs_bjob *j = malloc(sizeof *j + n);
    if (!j)
        return -1;
    *j = (struct cs_bjob){.kind = kind, .status = status, .file = file, .off = off, .total = total, .n = n};
    if (n)
        memcpy(j->p, p, n);
    pthread_mutex_lock(&x->mu);
    if (x->t)
        x->t->next = j;
    else
        x->h = j;
    x->t = j;
    x->bytes += n;
    if (!x->paused && x->bytes > CS_PUT_QUEUE_BYTES) {
        x->paused = l;
        plink_pause(l, 1);
    }
    pthread_cond_signal(&x->cv);
    pthread_mutex_unlock(&x->mu);
    return 0;
}

static int b_open(void *arg, struct plink *l, uint64_t file, const void *meta, size_t meta_len, uint64_t total,
                  uint64_t *resume, int *prefix_fd)
{
    struct chunksrv *cs = arg;
    struct cs_lctx *x = plink_kind(l) == PL_KIND_CHUNKB ? lctx_of(cs, l) : NULL;
    if (!x || meta_len > PL_META_MAX)
        return -1;
    *resume = 0;
    *prefix_fd = -1;
    return b_push(l, BJ_OPEN, 0, file, 0, total, meta, meta_len);
}

static int b_data(void *arg, struct plink *l, uint64_t file, uint64_t off, const void *p, size_t n)
{
    (void)arg;
    struct cs_lctx *x = plink_arg(l);
    if (!x || atomic_load(&x->refused) == file + 1) /* answered already: the sender stops sending */
        return -1;
    return b_push(l, BJ_DATA, 0, file, off, 0, p, n);
}

static void b_end(void *arg, struct plink *l, uint64_t file, int status)
{
    (void)arg;
    if (b_push(l, BJ_END, status, file, 0, 0, NULL, 0) != 0) {
        struct cs_lctx *x = plink_arg(l);
        if (x) /* no memory for the job: the put is never answered, the sender times out */
            atomic_store(&x->refused, file + 1);
    }
}

int chunksrv_listen(struct chunksrv *cs, const struct chunksrv_net_cfg *cfg)
{
    if (cs->listening)
        return -1;
    cs->closing = false;
    cs->nworkers = cfg->workers > 0 ? cfg->workers : 4;
    cs->workers = calloc((size_t)cs->nworkers, sizeof *cs->workers);
    if (!cs->workers)
        return -1;
    int started = 0;
    for (; started < cs->nworkers; started++)
        if (pthread_create(&cs->workers[started], NULL, worker_main, cs) != 0)
            break;
    struct pl_cfg pc = {.secret = cfg->secret, .secret_len = cfg->secret_len, .arg = cs, .on_link = on_link_chunk,
                        .on_frame = on_frame, .bulk = {.open = b_open, .data = b_data, .end = b_end},
                        .bulk_rate = cfg->rate, .dead_ms = cfg->dead_ms, .ping_ms = cfg->ping_ms};
    pc.self.id = cfg->self_id;
    memcpy(pc.self.uuid, cfg->uuid, 16);
    memcpy(pc.self.ident, cfg->ident, 16);
    snprintf(pc.advertise, sizeof pc.advertise, "%s", cfg->advertise);
    cs->pl = started == cs->nworkers ? peerlink_new(&pc) : NULL;
    if (!cs->pl) {
        cs->nworkers = started;
        cs->listening = true;
        chunksrv_unlisten(cs);
        return -1;
    }
    cs->listening = true;
    return 0;
}

struct peerlink *chunksrv_peerlink(struct chunksrv *cs) { return cs->pl; }

void chunksrv_set_rate(struct chunksrv *cs, uint64_t bytes_per_s)
{
    if (cs->pl)
        peerlink_set_bulk_rate(cs->pl, bytes_per_s);
}

void chunksrv_unlisten(struct chunksrv *cs)
{
    if (!cs->listening)
        return;
    pthread_mutex_lock(&cs->qmu);
    cs->closing = true;
    pthread_cond_broadcast(&cs->qcv);
    pthread_mutex_unlock(&cs->qmu);
    for (int i = 0; i < cs->nworkers; i++)
        pthread_join(cs->workers[i], NULL);
    free(cs->workers);
    cs->workers = NULL;
    for (struct cs_lctx *x = cs->lctx; x; x = x->next) { /* their answers go out on the links: stopped first */
        pthread_mutex_lock(&x->mu);
        x->stop = true;
        pthread_cond_signal(&x->cv);
        pthread_mutex_unlock(&x->mu);
        if (x->started)
            pthread_join(x->thr, NULL);
        x->started = false;
    }
    if (cs->pl) /* the link threads end: nothing queues or calls back any more */
        peerlink_free(cs->pl);
    cs->pl = NULL;
    for (struct cs_job *j = cs->qh, *nx; j; j = nx) {
        nx = j->next;
        if (j->commit)
            chunksrv_put_end(j->put, PL_E_CLOSED);
        free(j);
    }
    cs->qh = cs->qt = NULL;
    cs->qbytes = 0;
    free(cs->paused);
    cs->paused = NULL;
    cs->npaused = cs->cpaused = 0;
    for (struct cs_lctx *x = cs->lctx, *nx; x; x = nx) { /* after the links: nothing queues any more */
        nx = x->next;
        for (struct cs_bjob *j = x->h, *jn; j; j = jn) {
            jn = j->next;
            free(j);
        }
        if (x->cur)
            chunksrv_put_end(x->cur, PL_E_CLOSED);
        pthread_mutex_destroy(&x->mu);
        pthread_cond_destroy(&x->cv);
        free(x);
    }
    cs->lctx = NULL;
    for (size_t b = 0; b < CS_PEND_BUCKETS; b++) {
        for (struct cs_pend *q = cs->pend[b], *nx; q; q = nx) { /* bulk jobs ended with the links (done ran) */
            nx = q->next;
            pthread_cond_destroy(&q->cv);
            free(q->body);
            free(q);
        }
        cs->pend[b] = NULL;
    }
    free(cs->peers);
    cs->peers = NULL;
    cs->npeers = cs->cpeers = 0;
    cs->listening = false;
}

int chunksrv_set_peer(struct chunksrv *cs, uint32_t id, const char *addr)
{
    if (!cs->pl || id == 0)
        return -1;
    pthread_mutex_lock(&cs->cmu);
    struct cs_peer *pp = NULL;
    for (size_t i = 0; i < cs->npeers; i++)
        if (cs->peers[i].id == id)
            pp = &cs->peers[i];
    int rc = 0;
    if (pp && pp->gone) {
        rc = -1; /* a member that left does not come back under its id */
    } else if (pp && strcmp(pp->addr, addr) != 0) {
        snprintf(pp->addr, sizeof pp->addr, "%s", addr);
        plink_set_addr(pp->lc, addr);
        plink_set_addr(pp->lb, addr);
    } else if (!pp) {
        if (cs->npeers == cs->cpeers) {
            size_t nc = cs->cpeers ? 2 * cs->cpeers : 8;
            struct cs_peer *v = realloc(cs->peers, nc * sizeof *v);
            if (!v) {
                pthread_mutex_unlock(&cs->cmu);
                return -1;
            }
            cs->peers = v, cs->cpeers = nc;
        }
        pp = &cs->peers[cs->npeers];
        memset(pp, 0, sizeof *pp);
        pp->id = id;
        snprintf(pp->addr, sizeof pp->addr, "%s", addr);
        pp->lc = peerlink_dial(cs->pl, id, addr, PL_KIND_CHUNK);
        pp->lb = peerlink_dial(cs->pl, id, addr, PL_KIND_CHUNKB);
        if (pp->lc && pp->lb)
            cs->npeers++;
        else
            rc = -1;
    }
    pthread_mutex_unlock(&cs->cmu);
    return rc;
}

void chunksrv_keep_peers(struct chunksrv *cs, const uint32_t *ids, size_t n)
{
    pthread_mutex_lock(&cs->cmu);
    for (size_t i = 0; i < cs->npeers; i++) {
        struct cs_peer *pp = &cs->peers[i];
        bool keep = false;
        for (size_t k = 0; k < n && !keep; k++)
            keep = ids[k] == pp->id;
        if (!keep && !pp->gone) {
            pp->gone = true;
            plink_stop(pp->lc);
            plink_stop(pp->lb);
        }
    }
    pthread_mutex_unlock(&cs->cmu);
}

static int peer_links(struct chunksrv *cs, uint32_t id, struct plink **lc, struct plink **lb)
{
    pthread_mutex_lock(&cs->cmu);
    int rc = -1;
    for (size_t i = 0; i < cs->npeers; i++)
        if (cs->peers[i].id == id && !cs->peers[i].gone) {
            *lc = cs->peers[i].lc;
            *lb = cs->peers[i].lb;
            rc = 0;
        }
    pthread_mutex_unlock(&cs->cmu);
    return rc;
}

static uint64_t now_ns(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000000000ull + (uint64_t)t.tv_nsec;
}

static struct cs_pend *pend_add(struct chunksrv *cs, uint64_t rid, int refs, uint8_t op)
{
    struct cs_pend *q = calloc(1, sizeof *q);
    if (!q)
        return NULL;
    tsdb_cond_init_mono(&q->cv);
    q->rid = rid;
    q->op = op;
    q->refs = refs;
    size_t b = pend_bucket(rid);
    pthread_mutex_lock(&cs->cmu);
    q->next = cs->pend[b];
    cs->pend[b] = q;
    pthread_mutex_unlock(&cs->cmu);
    return q;
}

/* under cmu */
static void pend_unref(struct chunksrv *cs, struct cs_pend *q)
{
    if (--q->refs > 0)
        return;
    size_t b = pend_bucket(q->rid);
    for (struct cs_pend **pp = &cs->pend[b]; *pp; pp = &(*pp)->next)
        if (*pp == q) {
            *pp = q->next;
            break;
        }
    pthread_cond_destroy(&q->cv);
    free(q->body);
    free(q);
}

/* wait on the request's own monotonic condvar until target_ns (CLOCK_MONOTONIC ns) */
static void wait_until_ns(struct chunksrv *cs, struct cs_pend *pd, uint64_t target_ns)
{
    uint64_t now = now_ns(), left = target_ns > now ? target_ns - now : 0;
    struct timespec t;
    tsdb_cond_deadline_ns(&t, (int64_t)left);
    pthread_cond_timedwait(&pd->cv, &cs->cmu, &t);
}

static int send_until(struct plink *l, const uint8_t *p, size_t n, uint64_t deadline, uint64_t *gen_out)
{
    for (;;) {
        uint64_t gen = plink_gen(l);
        int rc = gen ? plink_send_gen(l, gen, CS_T_REQ, p, n, 100) : PL_E_DOWN;
        if (rc == PL_OK) {
            if (gen_out)
                *gen_out = gen;
            return 0;
        }
        if (rc != PL_E_DOWN && rc != PL_E_FULL)
            return -1;
        if (now_ns() >= deadline)
            return -1;
        nanosleep(&(struct timespec){0, 10 * 1000000L}, NULL);
    }
}

int chunksrv_call(struct chunksrv *cs, uint32_t peer, const struct cs_req *r, int timeout_ms, int32_t *rc,
                  uint8_t **body, size_t *body_len)
{
    *body = NULL;
    *body_len = 0;
    struct plink *lc, *lb;
    if (!cs->pl || peer_links(cs, peer, &lc, &lb) != 0)
        return TSDB_ERR_UNAVAILABLE;
    uint64_t deadline = now_ns() + (uint64_t)(timeout_ms > 0 ? timeout_ms : 30000) * 1000000ull;
    struct cs_req q = *r;
    q.rid = atomic_fetch_add(&cs->rid, 1);
    uint8_t buf[REQ_HDR + 64];
    size_t n = cs_req_encode(&q, buf, sizeof buf);
    if (!n)
        return TSDB_ERR_INVALID;
    struct cs_pend *pd = pend_add(cs, q.rid, 1, q.op);
    if (!pd)
        return TSDB_ERR_NOMEM;
    uint64_t gen = 0;
    int ret = send_until(lc, buf, n, deadline, &gen) == 0 ? 0 : TSDB_ERR_UNAVAILABLE;
    pthread_mutex_lock(&cs->cmu);
    if (ret == 0) {
        pd->link = lc;
        pd->gen = gen;
        if (!pd->done) {
            /* reply_link binds its response to the connection generation that carried the request; once that
             * connection drops, no answer can arrive for this rid, so fail fast instead of hanging until deadline. */
            pthread_mutex_unlock(&cs->cmu);
            bool alive = plink_up(lc) && plink_gen(lc) == gen;
            pthread_mutex_lock(&cs->cmu);
            if (!alive && !pd->done)
                pd->aborted = true;
        }
    }
    while (ret == 0 && !pd->done && !pd->aborted && now_ns() < deadline)
        wait_until_ns(cs, pd, deadline);
    if (ret == 0 && !pd->done)
        ret = TSDB_ERR_UNAVAILABLE;
    if (ret == 0) {
        *rc = pd->rc;
        *body = pd->body;
        *body_len = pd->body_len;
        pd->body = NULL;
    }
    pend_unref(cs, pd);
    pthread_mutex_unlock(&cs->cmu);
    return ret;
}

struct jarg {
    struct chunksrv *cs;
    struct cs_pend *pd;
    int fd;
};

static void put_job_done(void *arg, uint64_t file, int status, uint64_t sent)
{
    (void)file, (void)sent;
    struct jarg *ja = arg;
    struct chunksrv *cs = ja->cs;
    close(ja->fd);
    pthread_mutex_lock(&cs->cmu);
    ja->pd->job_done = true;
    ja->pd->job_status = status;
    clock_gettime(CLOCK_MONOTONIC, &ja->pd->job_at);
    pthread_cond_signal(&ja->pd->cv);
    pend_unref(cs, ja->pd);
    pthread_mutex_unlock(&cs->cmu);
    free(ja);
}

int chunksrv_put_remote(struct chunksrv *cs, uint32_t peer, const struct cs_put_meta *m, int fd, uint64_t base,
                        uint64_t sum, int timeout_ms)
{
    struct plink *lc, *lb;
    if (!cs->pl || peer_links(cs, peer, &lc, &lb) != 0)
        return TSDB_ERR_UNAVAILABLE;
    uint64_t deadline = now_ns() + (uint64_t)(timeout_ms > 0 ? timeout_ms : 30000) * 1000000ull;
    while (!(plink_up(lc) && plink_up(lb)) && now_ns() < deadline) /* the answer comes back on the CHUNK link */
        nanosleep(&(struct timespec){0, 10 * 1000000L}, NULL);
    if (!(plink_up(lc) && plink_up(lb)))
        return TSDB_ERR_UNAVAILABLE;
    uint8_t meta[CS_PUT_META_LEN];
    cs_put_meta_encode(m, meta);
    struct jarg *ja = calloc(1, sizeof *ja);
    int dfd = fcntl(fd, F_DUPFD_CLOEXEC, 0);
    if (!ja || dfd < 0) {
        free(ja);
        if (dfd >= 0)
            close(dfd);
        return TSDB_ERR_NOMEM;
    }
    uint64_t rid = atomic_fetch_add(&cs->rid, 1);
    struct cs_pend *pd = pend_add(cs, rid, 2, CS_OP_PUT);
    if (!pd) {
        free(ja);
        close(dfd);
        return TSDB_ERR_NOMEM;
    }
    *ja = (struct jarg){.cs = cs, .pd = pd, .fd = dfd};
    struct pl_job job = {.file = rid, .fd = dfd, .base = base, .length = cs_put_total(m), .meta = meta,
                         .meta_len = sizeof meta, .done = put_job_done, .arg = ja};
    if (plink_bulk_send(lb, &job) != PL_OK) {
        close(dfd);
        free(ja);
        pthread_mutex_lock(&cs->cmu);
        pd->refs--; /* the job's: it never runs */
        pend_unref(cs, pd);
        pthread_mutex_unlock(&cs->cmu);
        return TSDB_ERR_UNAVAILABLE;
    }
    pthread_mutex_lock(&cs->cmu);
    int ret = 0;
    for (;;) {
        if (pd->done)
            break;
        uint64_t now = now_ns();
        if (now >= deadline) {
            ret = TSDB_ERR_UNAVAILABLE;
            break;
        }
        uint64_t wait_target = deadline;
        if (pd->job_done && pd->job_status != PL_OK) { /* the refusal's reason may still be on its way on CHUNK */
            uint64_t at = (uint64_t)pd->job_at.tv_sec * 1000000000ull + (uint64_t)pd->job_at.tv_nsec;
            uint64_t grace = at + 1000000000ull;
            if (now >= grace) {
                ret = pd->job_status == PL_E_REFUSED || pd->job_status == PL_E_IO ? TSDB_ERR_IO : TSDB_ERR_UNAVAILABLE;
                break;
            }
            if (grace < wait_target)
                wait_target = grace;
        }
        wait_until_ns(cs, pd, wait_target);
    }
    if (ret == 0) {
        if (pd->rc != 0)
            ret = pd->rc;
        else if (pd->body_len != 8 || get64(pd->body) != cs_put_answer_hash(meta, sum)) {
            fprintf(stderr, "tsdbd: chunk put to node %" PRIu32 ": the answer's sum is not the chunk's\n", peer);
            ret = TSDB_ERR_IO;
        }
    }
    pend_unref(cs, pd);
    pthread_mutex_unlock(&cs->cmu);
    return ret;
}

#define CS_STAGE_MEMFD_MAX (16u << 20)

static int stage_wr_full(int fd, const void *buf, size_t n)
{
    const uint8_t *p = buf;
    size_t off = 0;
    while (off < n) {
        ssize_t w = write(fd, p + off, n - off);
        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0)
            return TSDB_ERR_IO;
        off += (size_t)w;
    }
    return 0;
}

int chunksrv_stage(const char *dir, const chunk_geom *g, unsigned pos, const void *cells, const uint64_t *sums)
{
    uint64_t nc = chunk_pos_cells(g, pos), len = chunk_pos_len(g, pos);
    int fd = -1;
#if defined(__linux__) && defined(SYS_memfd_create)
#ifndef MFD_CLOEXEC
#define MFD_CLOEXEC 0x0001U
#endif
    uint64_t tot = 8 * nc + len;
    if (tot <= CS_STAGE_MEMFD_MAX)
        fd = (int)syscall(SYS_memfd_create, "tsdb-chunkput", (unsigned)MFD_CLOEXEC);
#endif
    if (fd < 0) {
        char path[4096];
        snprintf(path, sizeof path, "%s/chunkput.XXXXXX", dir);
        fd = mkstemp(path);
        if (fd < 0)
            return TSDB_ERR_IO;
        unlink(path);
        (void)fcntl(fd, F_SETFD, FD_CLOEXEC);
    }
    uint8_t sbuf[8 * 512];
    for (uint64_t i = 0; i < nc;) {
        uint64_t c = nc - i < 512 ? nc - i : 512;
        for (uint64_t j = 0; j < c; j++)
            put64(sbuf + 8 * j, sums[i + j]);
        if (stage_wr_full(fd, sbuf, (size_t)(8 * c)) != 0) {
            close(fd);
            return TSDB_ERR_IO;
        }
        i += c;
    }
    if (len > 0 && stage_wr_full(fd, cells, (size_t)len) != 0) {
        close(fd);
        return TSDB_ERR_IO;
    }
    return fd;
}
