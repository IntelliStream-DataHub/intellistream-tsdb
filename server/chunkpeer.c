/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The cold operations of a cluster ec volume over its stripe group (server/chunkpeer.h has the design). The context
 * holds the volume and its vuuid only: the stripe group is read from the copy at every put, open and listing (E9: a
 * position moves only by STRIPE_SET, and r8g checks at the marker that the version it put by is still the copy's). An
 * object handle copies what it reads through (the transport, the members, the sums) so that it never touches the
 * context after obj_open, as src/coldops.h requires.
 */
#define _GNU_SOURCE
#include "chunkpeer.h"

#include "../include/tsdb.h"
#include "../src/compat.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

int chunkpeer_mutant;
uint32_t chunkpeer_test_list_max = CS_LIST_MAX;

int chunkpeer_mutant_parse(const char *name)
{
    static const char *const names[] = {"none", "ownepoch", "nosumcmp", "staleraise", "delbeforegc", "sticky",
                                         "unsuredown"};
    for (size_t i = 0; i < sizeof names / sizeof *names; i++)
        if (!strcmp(name, names[i]))
            return (int)i;
    return -1;
}

static void put64(uint8_t *p, uint64_t v)
{
    for (int i = 0; i < 8; i++)
        p[i] = (uint8_t)(v >> (8 * i));
}
static uint64_t get64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 7; i >= 0; i--)
        v = v << 8 | p[i];
    return v;
}
static uint32_t get32(const uint8_t *p) { return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24; }
static uint16_t get16(const uint8_t *p) { return (uint16_t)(p[0] | p[1] << 8); }

static const char *err_name(int rc)
{
    switch (rc) {
    case CHUNK_ERR_FENCED: return "fenced: below the member's epoch";
    case CHUNK_ERR_EPOCH_UNSURE: return "the member's store is unsure of its epoch";
    case CHUNK_ERR_FULL: return "the member's store is full";
    case CHUNK_ERR_STORE_ID: return "the member's store has another id";
    case CHUNK_ERR_BUSY: return "the member's store is busy";
    case TSDB_ERR_UNAVAILABLE: return "the member does not answer";
    default: return tsdb_strerror(rc);
    }
}

static int timeout_of(const struct chunkpeer *cp) { return cp->timeout_ms > 0 ? cp->timeout_ms : 30000; }

/* ---------------------------------------------------------------- the epoch (D9) */
static uint64_t own_fence(const struct chunkpeer *cp, uint32_t volume)
{
    chunk_backend *be = cp->local ? chunksrv_backend(cp->local) : NULL;
    return be ? chunk_be_epoch(be, volume) : 0;
}

uint64_t chunkpeer_epoch(void *arg, uint32_t volume)
{
    struct chunkpeer *cp = arg;
    if (chunkpeer_mutant == CP_MUT_OWNEPOCH)
        return own_fence(cp, volume); /* planted: what the stores (the writer) last saw */
    struct chunkpeer_view v;
    return cp->view(cp->view_arg, volume, &v) == 0 ? v.epoch : 0;
}

int chunkpeer_epoch_source(void *arg, uint32_t volume, uint64_t *epoch)
{
    struct chunkpeer *cp = arg;
    if (chunkpeer_mutant == CP_MUT_OWNEPOCH) {
        uint64_t e = own_fence(cp, volume); /* planted: an unsure store raised to its own surviving slot */
        if (e == 0)
            return 0;
        *epoch = e;
        return 1;
    }
    struct chunkpeer_view v;
    if (cp->view(cp->view_arg, volume, &v) != 0 || v.epoch == 0)
        return 0;
    if (!v.caught_up && chunkpeer_mutant != CP_MUT_STALERAISE)
        return 0; /* another leader may have committed a higher epoch that this copy has not applied */
    *epoch = v.epoch;
    return 1;
}

/* ---------------------------------------------------------------- the context */
typedef struct pctx {
    struct chunkpeer *cp;
    uint32_t volume;
    uint8_t vuuid[16];
    atomic_bool unfenced;  /* a member did not hold the epoch when last asked: no put or delete (writable false) */
    atomic_bool del_said;  /* the refusal of deletes was logged */
    _Atomic uint64_t seen; /* the highest fence a member answered */
    _Atomic int64_t asked_ms; /* when the members were last asked again while unfenced (one asker a second) */
} pctx;

static bool p_writable(void *ctx);

int chunkpeer_make(void *arg, struct tier *t, uint32_t volume, const uint8_t vuuid[16], void **ctx)
{
    (void)t;
    pctx *p = calloc(1, sizeof *p);
    if (!p)
        return TSDB_ERR_NOMEM;
    p->cp = arg;
    p->volume = volume;
    memcpy(p->vuuid, vuuid, 16);
    *ctx = p;
    return 0;
}

static int view_of(pctx *p, struct chunkpeer_view *v)
{
    int rc = p->cp->view(p->cp->view_arg, p->volume, v);
    if (rc)
        return rc == TSDB_ERR_NOTFOUND ? TSDB_ERR_UNAVAILABLE : rc;
    if (v->nstripe == 0 || v->nstripe > CHUNK_POS_MAX)
        return TSDB_ERR_UNAVAILABLE;
    return 0;
}

static void see_fence(pctx *p, uint64_t e)
{
    uint64_t cur = atomic_load(&p->seen);
    while (e > cur && !atomic_compare_exchange_weak(&p->seen, &cur, e))
        ;
}

/* ---------------------------------------------------------------- put */
typedef struct stage {
    const chunk_geom *g;
    unsigned npos;
    int fd[CHUNK_POS_MAX];
} stage;

static int stage_cell(void *ctx, unsigned pos, uint64_t idx, const uint8_t *data, size_t len, uint64_t sum)
{
    (void)sum;
    stage *s = ctx;
    if (pos >= s->npos)
        return TSDB_ERR_INVALID;
    uint64_t off = 8 * chunk_pos_cells(s->g, pos) + idx * (uint64_t)s->g->cell;
    for (size_t done = 0; done < len;) {
        ssize_t w = pwrite(s->fd[pos], data + done, len - done, (off_t)(off + done));
        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0)
            return errno == ENOSPC ? CHUNK_ERR_FULL : TSDB_ERR_IO;
        done += (size_t)w;
    }
    return 0;
}

static int stage_sums(int fd, const uint64_t *sums, uint64_t n)
{
    uint8_t buf[8 * 512];
    for (uint64_t i = 0; i < n;) {
        uint64_t c = n - i < 512 ? n - i : 512;
        for (uint64_t j = 0; j < c; j++)
            put64(buf + 8 * j, sums[i + j]);
        if (pwrite(fd, buf, 8 * c, (off_t)(8 * i)) != (ssize_t)(8 * c))
            return TSDB_ERR_IO;
        i += c;
    }
    return 0;
}

static int p_put_stream(void *ctx, const uint8_t oid[16], const chunk_geom *g, uint64_t epoch,
                        const chunk_put_src *src, uint64_t *sums_out)
{
    pctx *p = ctx;
    struct chunkpeer *cp = p->cp;
    if (!p_writable(p))
        return TSDB_ERR_UNAVAILABLE;
    struct chunkpeer_view v;
    int rc = view_of(p, &v);
    if (rc)
        return rc;
    unsigned npos = chunk_npos(g);
    if (npos > v.nstripe || g->m != v.m || g->k > v.k) {
        fprintf(stderr, "tsdbd: volume %" PRIu32 ": an object of %u+%u positions does not fit the stripe group of the "
                        "metadata copy (%u+%u): the put is refused\n", p->volume, g->k, g->m, v.k, v.m);
        return TSDB_ERR_INVALID;
    }
    uint64_t *own = NULL;
    if (!sums_out && !(sums_out = own = malloc(chunk_sums_total(g) * sizeof *own)))
        return TSDB_ERR_NOMEM;
    stage s = {.g = g, .npos = npos};
    for (unsigned i = 0; i < npos; i++)
        s.fd[i] = -1;
    for (unsigned i = 0; i < npos && rc == 0; i++) {
        /* mkstemp and unlink, never O_TMPFILE: macOS has none, and NFS (tsdbd's root in production) refuses it */
        char path[PATH_MAX + 32];
        snprintf(path, sizeof path, "%s/chunkstage.XXXXXX", cp->stage_dir);
        s.fd[i] = mkstemp(path);
        if (s.fd[i] >= 0) {
            unlink(path);
            (void)fcntl(s.fd[i], F_SETFD, FD_CLOEXEC);
        } else {
            fprintf(stderr, "tsdbd: volume %" PRIu32 ": a put cannot be staged in %s: %s\n", p->volume, cp->stage_dir,
                    strerror(errno));
            rc = TSDB_ERR_IO;
        }
    }
    if (rc == 0)
        rc = chunk_encode_stream(g, src, stage_cell, &s, sums_out);
    for (unsigned i = 0; i < npos && rc == 0; i++)
        rc = stage_sums(s.fd[i], sums_out + chunk_sums_base(g, i), chunk_pos_cells(g, i));
    /* One position after the other: every one must be committed before the settle goes on (no degraded settle). */
    for (unsigned i = 0; i < npos && rc == 0; i++) {
        struct cs_put_meta m = {.volume = p->volume, .epoch = epoch, .pos = (uint16_t)i, .g = *g};
        memcpy(m.vuuid, p->vuuid, 16);
        memcpy(m.oid, oid, 16);
        uint64_t sum = cs_sums_hash(sums_out + chunk_sums_base(g, i), chunk_pos_cells(g, i)), answered = 0;
        uint8_t meta[CS_PUT_META_LEN];
        cs_put_meta_encode(&m, meta);
        rc = cp->net.put(cp->net.arg, v.stripe[i], &m, s.fd[i], 0, sum, timeout_of(cp), &answered);
        /* the answer covers the identity the member took (volume, epoch, position, oid) as well as the sums */
        if (rc == 0 && answered != cs_put_answer_hash(meta, sum) && chunkpeer_mutant != CP_MUT_NOSUMCMP) {
            fprintf(stderr, "tsdbd: volume %" PRIu32 ": member %" PRIu32 " answered the put of position %u with "
                            "another sum than the chunk's: the put fails\n", p->volume, v.stripe[i], i);
            rc = TSDB_ERR_IO;
        }
        if (rc)
            fprintf(stderr, "tsdbd: volume %" PRIu32 ": the put of position %u on member %" PRIu32 " failed (%s): "
                            "the settle fails\n", p->volume, i, v.stripe[i], err_name(rc));
        else if (epoch)
            see_fence(p, epoch);
    }
    for (unsigned i = 0; i < npos; i++)
        if (s.fd[i] >= 0)
            close(s.fd[i]);
    free(own);
    return rc;
}

/* ---------------------------------------------------------------- read */
struct cold_obj {
    struct chunkpeer_net net;
    int timeout_ms;
    uint32_t volume;
    uint64_t epoch;
    uint8_t vuuid[16], oid[16];
    chunk_geom g;
    unsigned npos;
    uint32_t node[CHUNK_POS_MAX];
    int st[CHUNK_POS_MAX];       /* the open's status of each position */
    uint64_t *sums[CHUNK_POS_MAX]; /* each position's own table (status OK) */
    uint64_t *rec;               /* the object record's sums, or NULL */
};

static void req_init(struct cs_req *r, uint8_t op, uint32_t volume, uint64_t epoch, const uint8_t vuuid[16],
                     const uint8_t oid[16], unsigned pos)
{
    memset(r, 0, sizeof *r);
    r->op = op;
    r->volume = volume;
    r->epoch = epoch;
    if (vuuid)
        memcpy(r->vuuid, vuuid, 16);
    if (oid)
        memcpy(r->oid, oid, 16);
    r->pos = (uint16_t)pos;
}

/* An answer's rc as a CHUNK_ST_* of a position: a member that did not answer is DOWN (it may well hold the chunk). */
static int st_of(int trc, int32_t rc)
{
    if (trc)
        return CHUNK_ST_DOWN;
    int st = chunksrv_st(rc);
    return st < 0 ? CHUNK_ST_IOERR : st;
}

/* Position pos's header and sums table: CHUNK_ST_OK with *g and *sums (malloc'ed) or a CHUNK_ST_*. With `expect`
 * the whole table is fetched and the header must name expect's geometry; without, one sum (the header alone). */
static int fetch_sums(const struct chunkpeer_net *net, int tmo, uint32_t node, uint32_t volume, uint64_t epoch,
                      const uint8_t vuuid[16], const uint8_t oid[16], unsigned pos, const chunk_geom *expect,
                      chunk_geom *g, uint64_t **sums)
{
    *sums = NULL;
    uint64_t total = expect ? chunk_pos_cells(expect, pos) : 1, got = 0;
    uint64_t *t = expect ? malloc((total ? total : 1) * sizeof *t) : NULL;
    if (expect && !t)
        return CHUNK_ST_IOERR;
    int st = CHUNK_ST_OK;
    do {
        struct cs_req r;
        req_init(&r, CS_OP_SUMS, volume, epoch, vuuid, oid, pos);
        r.off = got;
        r.len = (uint32_t)(total - got < CS_SUMS_MAX ? total - got : CS_SUMS_MAX);
        int32_t rc = 0;
        uint8_t *b = NULL;
        size_t bn = 0;
        int trc = net->call(net->arg, node, &r, tmo, &rc, &b, &bn);
        if ((st = st_of(trc, rc)) != CHUNK_ST_OK) {
            free(b);
            break;
        }
        if (bn < 36) {
            st = CHUNK_ST_DAMAGED;
            free(b);
            break;
        }
        chunk_geom h;
        uint32_t n = get32(b + 32);
        if (chunk_geom_make(&h, get16(b + 8), get16(b + 10), get32(b + 12), get64(b + 16)) != 0 ||
            (expect && (h.k != expect->k || h.m != expect->m || h.cell != expect->cell || h.len != expect->len)) ||
            get64(b + 24) != chunk_pos_cells(&h, pos) || n != r.len || bn != 36 + 8 * (size_t)n) {
            st = CHUNK_ST_DAMAGED; /* another encoding of the object, or an answer that does not parse */
            free(b);
            break;
        }
        *g = h;
        for (uint32_t i = 0; t && i < n; i++)
            t[got + i] = get64(b + 36 + 8 * (size_t)i);
        got += n;
        free(b);
    } while (expect && got < total);
    if (st == CHUNK_ST_OK && expect)
        *sums = t;
    else
        free(t);
    return st;
}

/* ---------------------------------------------------------------- asking the positions at once */
/* Every position is asked on a thread of its own, so that one member that hangs (paused, a stuck disk: the link is up
 * and nothing answers) costs one deadline, not one per position, and an open that has its k' positions stops waiting
 * for it after FAN_GRACE_MS. A thread the caller stopped waiting for finishes on its own: what it shares with the
 * caller is the fan, reference-counted, and the transport, which outlives every context (tsdbd's holds the chunk
 * server while a call is in it). It never touches a context or an object handle. */
#define FAN_GRACE_MS 200

typedef struct ask {
    int st; /* SUMS: CHUNK_ST_*; EPOCH: the call's rc (0, or the error) */
    chunk_geom g;
    uint64_t *sums;
    uint64_t fence;
    bool unsure;
} ask;

typedef struct fan fan;
typedef struct fan_job {
    fan *f;
    unsigned i;
} fan_job;

struct fan {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    int refs;
    unsigned n, done, ok;
    bool epoch_op; /* CS_OP_EPOCH at epoch, else CS_OP_SUMS (of the whole table with expect, else one sum) */
    struct chunkpeer_net net;
    int tmo;
    uint32_t volume;
    uint64_t epoch;
    uint8_t vuuid[16], oid[16];
    bool has_expect;
    chunk_geom expect;
    uint32_t node[CHUNK_POS_MAX];
    ask a[CHUNK_POS_MAX];
    bool fin[CHUNK_POS_MAX];
    fan_job job[CHUNK_POS_MAX];
};

static atomic_int fan_threads; /* asks still running, abandoned ones included (chunkpeer_quiesce) */

static void fan_one(fan *f, unsigned i, ask *out)
{
    memset(out, 0, sizeof *out);
    if (!f->epoch_op) {
        out->st = fetch_sums(&f->net, f->tmo, f->node[i], f->volume, f->epoch, f->vuuid, f->oid, i,
                             f->has_expect ? &f->expect : NULL, &out->g, &out->sums);
        return;
    }
    struct cs_req r;
    req_init(&r, CS_OP_EPOCH, f->volume, f->epoch, NULL, NULL, 0);
    int32_t arc = 0;
    uint8_t *b = NULL;
    size_t bn = 0;
    int rc = f->net.call(f->net.arg, f->node[i], &r, f->tmo, &arc, &b, &bn);
    if (rc == 0 && arc != 0)
        rc = arc;
    if (rc == 0 && bn != 9)
        rc = TSDB_ERR_IO;
    if (rc == 0) {
        out->fence = get64(b);
        out->unsure = b[8] != 0;
    }
    free(b);
    out->st = rc;
}

static bool fan_ok(const fan *f, const ask *a) { return f->epoch_op ? a->st == 0 : a->st == CHUNK_ST_OK; }

static void fan_unref_locked(fan *f) /* unlocks f->mu; frees f at the last reference */
{
    bool last = --f->refs == 0;
    pthread_mutex_unlock(&f->mu);
    if (!last)
        return;
    for (unsigned i = 0; i < f->n; i++)
        free(f->a[i].sums);
    pthread_cond_destroy(&f->cv);
    pthread_mutex_destroy(&f->mu);
    free(f);
}

static void fan_record(fan *f, unsigned i, ask *a)
{
    pthread_mutex_lock(&f->mu);
    f->a[i] = *a;
    f->fin[i] = true;
    f->done++;
    f->ok += fan_ok(f, a);
    pthread_cond_broadcast(&f->cv);
}

static void *fan_main(void *arg)
{
    fan_job *j = arg;
    fan *f = j->f;
    ask a;
    fan_one(f, j->i, &a);
    fan_record(f, j->i, &a);
    fan_unref_locked(f);
    atomic_fetch_sub(&fan_threads, 1);
    return NULL;
}

/* Asks positions 0..n-1 of f and waits until all answered, or `need` (> 0) are OK and FAN_GRACE_MS more passed (0
 * grace with need 1: the first OK), or, with stop_on_fail, one failed. out[i] gets each answer (its sums move to the
 * caller), `unfinished` where none came in time. Consumes the caller's reference on f. */
static void fan_run(fan *f, unsigned need, int grace_ms, bool stop_on_fail, ask *out, int unfinished)
{
    pthread_mutex_init(&f->mu, NULL);
    tsdb_cond_init_mono(&f->cv);
    f->refs = 1 + (int)f->n;
    for (unsigned i = 0; i < f->n; i++) {
        f->job[i] = (fan_job){f, i};
        pthread_attr_t at;
        pthread_t th;
        pthread_attr_init(&at);
        pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED);
        atomic_fetch_add(&fan_threads, 1);
        int e = pthread_create(&th, &at, fan_main, &f->job[i]);
        pthread_attr_destroy(&at);
        if (e != 0) { /* no thread: asked here, in turn */
            atomic_fetch_sub(&fan_threads, 1);
            ask a;
            fan_one(f, i, &a);
            fan_record(f, i, &a);
            f->refs--;
            pthread_mutex_unlock(&f->mu);
        }
    }
    pthread_mutex_lock(&f->mu);
    bool armed = false;
    struct timespec dl;
    while (f->done < f->n && !(stop_on_fail && f->done > f->ok)) {
        if (need && f->ok >= need) {
            if (!armed) {
                tsdb_cond_deadline_ms(&dl, grace_ms);
                armed = true;
            }
            if (pthread_cond_timedwait(&f->cv, &f->mu, &dl) == ETIMEDOUT)
                break;
            continue;
        }
        pthread_cond_wait(&f->cv, &f->mu);
    }
    for (unsigned i = 0; i < f->n; i++) {
        if (f->fin[i]) {
            out[i] = f->a[i];
            f->a[i].sums = NULL;
        } else {
            memset(&out[i], 0, sizeof out[i]);
            out[i].st = unfinished;
        }
    }
    fan_unref_locked(f);
}

static fan *fan_new(const struct chunkpeer *cp, uint32_t volume, uint64_t epoch, const uint32_t *nodes, unsigned n)
{
    fan *f = calloc(1, sizeof *f);
    if (!f)
        return NULL;
    f->net = cp->net;
    f->tmo = timeout_of(cp);
    f->volume = volume;
    f->epoch = epoch;
    f->n = n;
    memcpy(f->node, nodes, n * sizeof *nodes);
    return f;
}

void chunkpeer_quiesce(void)
{
    while (atomic_load(&fan_threads) > 0) {
        struct timespec ts = {0, 2 * 1000000L};
        nanosleep(&ts, NULL);
    }
}

static int src_read(void *ctx, unsigned pos, uint64_t off, void *buf, size_t len)
{
    struct cold_obj *o = ctx;
    if (pos >= o->npos || o->st[pos] != CHUNK_ST_OK)
        return pos < o->npos ? o->st[pos] : CHUNK_ST_ABSENT;
    for (size_t done = 0; done < len;) {
        struct cs_req r;
        req_init(&r, CS_OP_READ, o->volume, o->epoch, o->vuuid, o->oid, pos);
        r.off = off + done;
        r.len = (uint32_t)(len - done < CS_READ_MAX ? len - done : CS_READ_MAX);
        int32_t rc = 0;
        uint8_t *b = NULL;
        size_t bn = 0;
        int st = st_of(o->net.call(o->net.arg, o->node[pos], &r, o->timeout_ms, &rc, &b, &bn), rc);
        if (st == CHUNK_ST_OK && bn != r.len)
            st = CHUNK_ST_IOERR; /* a short read is an error, never the end */
        if (st == CHUNK_ST_OK)
            memcpy((uint8_t *)buf + done, b, bn);
        free(b);
        if (st != CHUNK_ST_OK)
            return st;
        done += r.len;
    }
    return 0;
}

static const uint64_t *src_sums(void *ctx, unsigned pos, int *status)
{
    struct cold_obj *o = ctx;
    if (pos >= o->npos || o->st[pos] != CHUNK_ST_OK) {
        *status = pos < o->npos ? o->st[pos] : CHUNK_ST_ABSENT;
        return NULL;
    }
    return o->sums[pos];
}

static void p_obj_close(cold_obj *o)
{
    if (!o)
        return;
    for (unsigned i = 0; i < CHUNK_POS_MAX; i++)
        free(o->sums[i]);
    free(o->rec);
    free(o);
}

/* An open with no position OK: NOTFOUND only when every position answered that it holds no such chunk; UNAVAILABLE
 * when one is away or failed to read (its chunk may be there: E15, never CORRUPT for that), CORRUPT when the rest
 * failed their checks (another encoding of the object, a header or table that does not parse). */
static int none_ok(const int *st, unsigned n)
{
    bool away = false, damaged = false;
    for (unsigned i = 0; i < n; i++) {
        away |= st[i] == CHUNK_ST_DOWN || st[i] == CHUNK_ST_IOERR;
        damaged |= st[i] == CHUNK_ST_DAMAGED;
    }
    return away ? TSDB_ERR_UNAVAILABLE : damaged ? TSDB_ERR_CORRUPT : TSDB_ERR_NOTFOUND;
}

static int p_obj_open(void *ctx, const uint8_t oid[16], const chunk_geom *expect, const uint64_t *rec_sums,
                      cold_obj **out)
{
    pctx *p = ctx;
    *out = NULL;
    struct chunkpeer_view v;
    int rc = view_of(p, &v);
    if (rc)
        return rc;
    struct cold_obj *o = calloc(1, sizeof *o);
    if (!o)
        return TSDB_ERR_NOMEM;
    o->net = p->cp->net;
    o->timeout_ms = timeout_of(p->cp);
    o->volume = p->volume;
    o->epoch = v.epoch;
    memcpy(o->vuuid, p->vuuid, 16);
    memcpy(o->oid, oid, 16);
    for (unsigned i = 0; i < v.nstripe; i++)
        o->node[i] = v.stripe[i];
    if (expect) {
        o->g = *expect;
    } else { /* the geometry from the first header found (positions are placed whatever k' is: E9) */
        fan *f = fan_new(p->cp, o->volume, o->epoch, o->node, v.nstripe);
        if (!f) {
            p_obj_close(o);
            return TSDB_ERR_NOMEM;
        }
        memcpy(f->vuuid, o->vuuid, 16);
        memcpy(f->oid, oid, 16);
        ask a[CHUNK_POS_MAX];
        fan_run(f, 1, 0, false, a, CHUNK_ST_DOWN);
        int st[CHUNK_POS_MAX];
        bool found = false;
        for (unsigned i = 0; i < v.nstripe; i++) {
            st[i] = a[i].st;
            if (a[i].st == CHUNK_ST_OK && !found) {
                o->g = a[i].g;
                found = true;
            }
        }
        if (!found) {
            p_obj_close(o);
            return none_ok(st, v.nstripe);
        }
    }
    o->npos = chunk_npos(&o->g);
    if (o->npos > v.nstripe) {
        p_obj_close(o);
        return TSDB_ERR_CORRUPT; /* an object record of more positions than the volume has members */
    }
    fan *f = fan_new(p->cp, o->volume, o->epoch, o->node, o->npos);
    if (!f) {
        p_obj_close(o);
        return TSDB_ERR_NOMEM;
    }
    memcpy(f->vuuid, o->vuuid, 16);
    memcpy(f->oid, oid, 16);
    f->has_expect = true;
    f->expect = o->g;
    ask a[CHUNK_POS_MAX];
    /* k' positions are enough to read every byte: one that does not answer by then is DOWN for this handle */
    fan_run(f, o->g.k, FAN_GRACE_MS, false, a, CHUNK_ST_DOWN);
    unsigned ok = 0;
    for (unsigned i = 0; i < o->npos; i++) {
        o->st[i] = a[i].st;
        o->sums[i] = a[i].sums;
        ok += o->st[i] == CHUNK_ST_OK;
    }
    if (ok == 0) {
        int rc0 = none_ok(o->st, o->npos);
        p_obj_close(o);
        return rc0;
    }
    if (rec_sums) {
        size_t n = chunk_sums_total(&o->g);
        if (!(o->rec = malloc(n * sizeof *o->rec))) {
            p_obj_close(o);
            return TSDB_ERR_NOMEM;
        }
        memcpy(o->rec, rec_sums, n * sizeof *o->rec);
    }
    *out = o;
    return 0;
}

static int p_obj_read(cold_obj *o, uint64_t off, uint64_t len, void *buf, unsigned flags, chunk_report *rep)
{
    chunk_source src = {.ctx = o, .read = src_read, .sums = src_sums};
    for (int i = 0; i < 16; i++)
        snprintf(src.name + 2 * i, sizeof src.name - 2 * (size_t)i, "%02x", o->oid[i]);
    return chunk_read_range(&src, &o->g, o->rec, off, len, buf, flags, rep);
}

static const chunk_geom *p_obj_geom(const cold_obj *o) { return &o->g; }

/* ---------------------------------------------------------------- delete (D13: only by MK_GC, r8k) */
static int p_del(void *ctx, const uint8_t oid[16], unsigned pos, uint64_t epoch, int reason)
{
    pctx *p = ctx;
    if (chunkpeer_mutant != CP_MUT_DELBEFOREGC) {
        if (!atomic_exchange(&p->del_said, true))
            fprintf(stderr, "tsdbd: volume %" PRIu32 ": direct chunk deletes on a cluster volume are refused "
                            "(chunks are deleted only by a committed MK_GC): %s refused\n", p->volume,
                    reason == COLD_DEL_SWEEP ? "open's sweep of orphans" : "direct delete");
        return TSDB_ERR_UNAVAILABLE;
    }
    struct chunkpeer_view v; /* planted: R7's delete, through to the member */
    int rc = view_of(p, &v);
    if (rc)
        return rc;
    if (pos >= v.nstripe)
        return TSDB_ERR_NOTFOUND;
    struct cs_req r;
    req_init(&r, CS_OP_DEL, p->volume, epoch, p->vuuid, oid, pos);
    int32_t arc = 0;
    uint8_t *b = NULL;
    size_t bn = 0;
    rc = p->cp->net.call(p->cp->net.arg, v.stripe[pos], &r, timeout_of(p->cp), &arc, &b, &bn);
    free(b);
    return rc ? rc : arc;
}

static int p_del_gc(void *ctx, const uint8_t oid[16], unsigned pos, uint64_t epoch, uint64_t mk_pos)
{
    pctx *p = ctx;
    if (mk_pos == 0 || epoch == 0)
        return TSDB_ERR_INVALID;
    if (!p_writable(p))
        return TSDB_ERR_UNAVAILABLE;
    struct chunkpeer_view v;
    int rc = view_of(p, &v);
    if (rc)
        return rc;
    if (pos >= v.nstripe)
        return TSDB_ERR_NOTFOUND;
    struct cs_req r;
    req_init(&r, CS_OP_DEL, p->volume, epoch, p->vuuid, oid, pos);
    int32_t arc = 0;
    uint8_t *b = NULL;
    size_t bn = 0;
    rc = p->cp->net.call(p->cp->net.arg, v.stripe[pos], &r, timeout_of(p->cp), &arc, &b, &bn);
    free(b);
    int res = rc ? rc : arc;
    if (res == 0)
        see_fence(p, epoch);
    return res;
}

/* ---------------------------------------------------------------- list */
static int list_entry(const uint8_t *e, chunk_listed *c, uint8_t vuuid[16])
{
    memset(c, 0, sizeof *c);
    memcpy(c->oid, e, 16);
    c->pos = get16(e + 16);
    c->size = get64(e + 18);
    c->epoch = get64(e + 26);
    memcpy(c->vuuid, e + 34, 16);
    if (vuuid)
        memcpy(vuuid, c->vuuid, 16);
    c->status = e[50];
    return e[50]; /* the entry's status */
}

/* One page of member `node`'s listing after the cursor: 0 with the body, or an error. */
static int list_page(pctx *p, uint32_t node, bool has_cursor, const uint8_t coid[16], unsigned cpos, uint32_t max,
                     uint8_t **body, size_t *bn, uint32_t *n, bool *more)
{
    struct cs_req r;
    req_init(&r, CS_OP_LIST, p->volume, 0, NULL, has_cursor ? coid : NULL, has_cursor ? cpos : 0);
    r.has_cursor = has_cursor;
    r.len = max;
    int32_t arc = 0;
    *body = NULL;
    int rc = p->cp->net.call(p->cp->net.arg, node, &r, timeout_of(p->cp), &arc, body, bn);
    if (rc == 0 && arc != 0)
        rc = arc;
    if (rc == 0 && (*bn < 5 || (*n = get32(*body)) > max || *bn != 5 + (size_t)*n * CS_LIST_ENTRY))
        rc = TSDB_ERR_IO; /* a listing cut short or garbled is an error, never the end */
    if (rc) {
        free(*body);
        *body = NULL;
        return rc;
    }
    *more = (*body)[4] != 0;
    return 0;
}

static int p_list_names(void *ctx, chunk_list_fn fn, void *fctx)
{
    pctx *p = ctx;
    struct chunkpeer_view v;
    int rc = view_of(p, &v);
    for (unsigned i = 0; i < v.nstripe && rc == 0; i++) {
        bool has = false, more = true;
        uint8_t coid[16] = {0};
        unsigned cpos = 0;
        while (more && rc == 0) {
            uint8_t *b;
            size_t bn;
            uint32_t n = 0;
            if ((rc = list_page(p, v.stripe[i], has, coid, cpos, chunkpeer_test_list_max, &b, &bn, &n, &more)) != 0)
                break;
            for (uint32_t j = 0; j < n && rc == 0; j++) {
                chunk_listed c;
                list_entry(b + 5 + (size_t)j * CS_LIST_ENTRY, &c, NULL);
                c.store = (int)v.stripe[i]; /* our token: the member (owner asks it) */
                rc = fn(fctx, &c);
                memcpy(coid, c.oid, 16);
                cpos = c.pos;
                has = true;
            }
            free(b);
            if (n == 0)
                break;
        }
    }
    return rc;
}

/* The (oid, pos) just before c's in a member's listing order (oid bytes, then pos): a one-entry page after it is c. */
static bool cursor_before(const chunk_listed *c, uint8_t oid[16], unsigned *pos)
{
    memcpy(oid, c->oid, 16);
    if (c->pos > 0) {
        *pos = c->pos - 1u;
        return true;
    }
    int i = 15;
    while (i >= 0 && oid[i] == 0)
        oid[i--] = 0xff;
    if (i < 0)
        return false; /* the first name there is: no cursor */
    oid[i]--;
    *pos = CHUNK_POS_MAX - 1; /* the highest position a cursor may name (cs_req_decode) */
    return true;
}

static int p_owner(void *ctx, const chunk_listed *c, uint8_t vuuid[16])
{
    pctx *p = ctx;
    uint8_t coid[16];
    unsigned cpos = 0;
    bool has = cursor_before(c, coid, &cpos);
    uint8_t *b;
    size_t bn;
    uint32_t n = 0;
    bool more;
    int rc = list_page(p, (uint32_t)c->store, has, coid, cpos, 4, &b, &bn, &n, &more);
    if (rc)
        return rc;
    rc = TSDB_ERR_NOTFOUND;
    for (uint32_t j = 0; j < n; j++) { /* the copies of one (oid, pos) are listed together */
        chunk_listed e;
        int st = list_entry(b + 5 + (size_t)j * CS_LIST_ENTRY, &e, vuuid);
        if (memcmp(e.oid, c->oid, 16) || e.pos != c->pos)
            break;
        if (st == CHUNK_ST_OK) {
            rc = 0;
            break;
        }
        rc = TSDB_ERR_CORRUPT;
    }
    free(b);
    return rc;
}

/* ---------------------------------------------------------------- epoch and state */
/* EPOCH at e on every stripe member at once (e 0: only asks): how many hold e (a fence at or above it, a store that is
 * sure) in *held, how many answered at all in *answered (NULL: not wanted); *first_bad the first that did not hold it
 * (its index), its answer in *bad. */
static int ask_all(pctx *p, uint64_t e, bool stop_on_fail, unsigned *held, unsigned *answered, unsigned *nstripe,
                   int *first_bad, ask *bad)
{
    struct chunkpeer_view v;
    int rc = view_of(p, &v);
    if (rc)
        return rc;
    fan *f = fan_new(p->cp, p->volume, e, v.stripe, v.nstripe);
    if (!f)
        return TSDB_ERR_NOMEM;
    f->epoch_op = true;
    ask a[CHUNK_POS_MAX];
    fan_run(f, 0, 0, stop_on_fail, a, TSDB_ERR_UNAVAILABLE);
    *held = 0;
    if (answered)
        *answered = 0;
    *nstripe = v.nstripe;
    *first_bad = -1;
    for (unsigned i = 0; i < v.nstripe; i++) {
        if (a[i].st == 0) {
            see_fence(p, a[i].fence);
            if (answered)
                (*answered)++;
        }
        if (a[i].st == 0 && a[i].fence >= e && !a[i].unsure) {
            (*held)++;
            continue;
        }
        if (*first_bad < 0) {
            *first_bad = (int)i;
            *bad = a[i];
            if (bad->st == 0)
                bad->st = bad->unsure ? CHUNK_ERR_EPOCH_UNSURE : CHUNK_ERR_FENCED;
        }
    }
    return 0;
}

/* At attach (a writer's open, a promotion, a replica's open). A member that is away or does not take e makes the
 * context unfenced: no put or delete until every member holds the copy's epoch (asked again by writable, at most once a
 * second). Never an error for members that are away: an attach that failed would refuse the volume until tsdbd
 * restarts, and a cluster's cold start opens its volumes before the chunk links are up. */
static int p_see_epoch(void *ctx, uint64_t e)
{
    pctx *p = ctx;
    unsigned held, n;
    int bad_i;
    ask bad;
    int rc = ask_all(p, e, false, &held, NULL, &n, &bad_i, &bad);
    if (rc)
        return rc;
    if (held == n) {
        atomic_store(&p->unfenced, false);
        return 0;
    }
    atomic_store(&p->unfenced, true);
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    atomic_store(&p->asked_ms, (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000);
    fprintf(stderr, "tsdbd: volume %" PRIu32 ": %u of %u stripe members do not hold epoch %" PRIu64 " (position %d: "
                    "%s): no put or delete of the volume until they do (asked again every second)\n",
            p->volume, n - held, n, e, bad_i, err_name(bad.st));
    return 0;
}

static uint64_t p_epoch(void *ctx) { return atomic_load(&((pctx *)ctx)->seen); }

static bool p_writable(void *ctx)
{
    pctx *p = ctx;
    if (!atomic_load(&p->unfenced))
        return true;
    if (chunkpeer_mutant == CP_MUT_STICKY)
        return false; /* planted: unfenced for the context's life (a promoted leader never settles again) */
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    int64_t now = (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000, last = atomic_load(&p->asked_ms);
    if (now - last < 1000 || !atomic_compare_exchange_strong(&p->asked_ms, &last, now))
        return false; /* asked within the last second, or another thread asks now */
    struct chunkpeer_view v;
    if (view_of(p, &v) != 0 || v.epoch == 0)
        return false;
    unsigned held, n;
    int bad_i;
    ask bad;
    if (ask_all(p, v.epoch, true, &held, NULL, &n, &bad_i, &bad) != 0 || held != n)
        return false;
    atomic_store(&p->unfenced, false);
    fprintf(stderr, "tsdbd: volume %" PRIu32 ": every stripe member holds epoch %" PRIu64 ": puts and deletes go "
                    "on\n", p->volume, v.epoch);
    return true;
}

/* Every member answered: an unsure store is UP (as the directory backend's is) and refuses puts itself; counting it
 * down made eccold's stripe_ok refuse every settle before its put while one store was unsure (ecbasic's D9 case). */
static bool p_all_up(void *ctx)
{
    unsigned held, up, n;
    int bad_i;
    ask bad;
    if (chunkpeer_mutant == CP_MUT_UNSUREDOWN)
        return ask_all(ctx, 0, true, &held, &up, &n, &bad_i, &bad) == 0 && held == n; /* planted: unsure is down */
    return ask_all(ctx, 0, true, &held, &up, &n, &bad_i, &bad) == 0 && up == n;
}

static uint64_t p_stripe_version(void *ctx)
{
    struct chunkpeer_view v;
    return view_of(ctx, &v) == 0 ? v.stripe_version : 0;
}

static bool p_gc_suspended(void *ctx)
{
    struct chunkpeer_view v;
    return view_of(ctx, &v) != 0 || v.gc_suspended != 0;
}

static unsigned p_nstripe(void *ctx)
{
    struct chunkpeer_view v;
    return view_of(ctx, &v) == 0 ? v.nstripe : 0;
}

static void p_free(void *ctx) { free(ctx); }

const cold_ops coldops_peer = {
    .name = "peer",
    .put_stream = p_put_stream,
    .obj_open = p_obj_open,
    .obj_read = p_obj_read,
    .obj_geom = p_obj_geom,
    .obj_close = p_obj_close,
    .del = p_del,
    .list_names = p_list_names,
    .owner = p_owner,
    .see_epoch = p_see_epoch,
    .epoch = p_epoch,
    .writable = p_writable,
    .all_up = p_all_up,
    .ctx_free = p_free,
    .stripe_version = p_stripe_version,
    .del_gc = p_del_gc,
    .gc_suspended = p_gc_suspended,
    .nstripe = p_nstripe,
};

/* ---------------------------------------------------------------- in-process delivery */
typedef struct reply {
    int n;
    int32_t rc;
    uint8_t op;
    uint8_t *body;
    size_t len;
} reply;

static void take_reply(void *arg, const uint8_t *p, size_t n)
{
    reply *r = arg;
    struct cs_resp a;
    if (cs_resp_decode(p, n, &a) != 0 || r->n++ > 0)
        return;
    r->rc = a.rc;
    r->op = a.op;
    if (a.body_len && (r->body = malloc(a.body_len)) != NULL) {
        memcpy(r->body, a.body, a.body_len);
        r->len = a.body_len;
    }
}

int chunkpeer_local_call(struct chunksrv *cs, uint32_t from, const struct cs_req *r, int32_t *rc, uint8_t **body,
                         size_t *body_len)
{
    *body = NULL;
    *body_len = 0;
    if (!cs)
        return TSDB_ERR_UNAVAILABLE;
    uint8_t buf[128];
    size_t n = cs_req_encode(r, buf, sizeof buf);
    reply a = {0};
    if (n == 0 || chunksrv_handle(cs, from, buf, n, take_reply, &a) != 0 || a.n != 1) {
        free(a.body);
        return TSDB_ERR_UNAVAILABLE;
    }
    *rc = a.rc;
    *body = a.body;
    *body_len = a.len;
    return 0;
}

int chunkpeer_local_put(struct chunksrv *cs, uint32_t from, const struct cs_put_meta *m, int fd, uint64_t base,
                        uint64_t *answered)
{
    if (!cs)
        return TSDB_ERR_UNAVAILABLE;
    static _Atomic uint64_t file_ids = 1ull << 62; /* apart from any peerlink's */
    uint8_t meta[CS_PUT_META_LEN];
    cs_put_meta_encode(m, meta);
    uint64_t total = cs_put_total(m);
    reply a = {0};
    struct cs_put *put = NULL;
    int rc = chunksrv_put_begin(cs, from, atomic_fetch_add(&file_ids, 1), meta, sizeof meta, total, take_reply, &a,
                                &put);
    if (rc == 0) {
        size_t cap = 1u << 20;
        uint8_t *buf = malloc(cap);
        int st = buf ? 0 : -1;
        for (uint64_t off = 0; off < total && st == 0 && rc == 0;) {
            size_t want = total - off < cap ? (size_t)(total - off) : cap;
            ssize_t got = pread(fd, buf, want, (off_t)(base + off));
            if (got < 0 && errno == EINTR)
                continue;
            if (got <= 0) {
                st = -1;
                break;
            }
            rc = chunksrv_put_data(put, off, buf, (size_t)got);
            off += (uint64_t)got;
        }
        free(buf);
        chunksrv_put_end(put, st == 0 && rc == 0 ? 0 : -1);
        if (st != 0 && rc == 0)
            rc = TSDB_ERR_IO;
    }
    if (a.n == 1 && rc == 0)
        rc = a.rc;
    else if (rc == 0)
        rc = TSDB_ERR_IO; /* committed without an answer: never taken as a success */
    if (rc == 0) {
        if (a.len != 8) {
            rc = TSDB_ERR_IO;
        } else {
            *answered = get64(a.body);
        }
    }
    free(a.body);
    return rc;
}
