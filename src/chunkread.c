/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Reading an object back from its chunks, with decode (design-cluster-data.md §3.1-3.2, E2, E10, E11, E26).
 *
 * chunk_read_range works one stripe at a time over a chunk_source (files here, memory in tests and in fuzz_ec, the
 * network in R8). The healthy path reads the cells a byte range needs from their data chunks. A cell that is absent,
 * cannot be read or fails its sum is an erasure: the stripe is rebuilt from any k' good cells of its k' + m, data
 * cells first, then parity, and a data cell past the end of the object counts as a good cell of zeros. Fewer than k'
 * good cells is TSDB_ERR_CORRUPT naming the object, never wrong bytes.
 *
 * What chooses the good cells is the encode-time sum of each cell (E26): a 64-bit xxh64 per cell, from the object
 * record when the caller has it (which also catches a chunk of another encoding that is self-consistent), else from
 * the chunk's own table. A wrong survivor decodes to a plausible wrong answer, so nothing is ever tried and checked
 * after the fact by a short sum. The geometry is the object's own (the caller's record, or the headers), never today's
 * settings (E2).
 *
 * Two modes. The default reads every cell it uses whole and checks it, which is what a scrub, a repair or the second
 * try of a reader wants. CHUNK_RR_RAW reads only the bytes asked for and, when a cell fails to read, the same byte
 * range of the others (Reed-Solomon works column by column, so a 10 KB granule costs 10 KB from each of k' cells, not
 * k' MiB): it cannot see rot, so its caller must check what it gets (a granule's crc, E11) and ask again without RAW
 * when that fails.
 */
#include "internal.h"
#include "chunk.h"
#include "chunk_priv.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifdef TSDB_EC
#include "ec/rs.h"
#endif

/* Bounded cached I/O worker pool for concurrent multi-chunk local preads in chunk_read_range.
 * Never spawns threads for single-chunk reads; falls back inline when saturated, draining, or forked. */
#define CR_POOL_MAX 8
#define CR_POOL_IDLE_MS 2000
#define CR_PAR_MIN_BYTES (32u << 10)

typedef struct cr_job {
    void (*fn)(void *);
    void *arg;
    struct cr_job *next;
} cr_job;

static pthread_mutex_t cr_pool_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cr_pool_cv;
static pthread_once_t cr_pool_once = PTHREAD_ONCE_INIT;
static atomic_bool cr_pool_inited;
static pid_t cr_pool_pid;
static cr_job *cr_pool_head, *cr_pool_tail;
static int cr_pool_idle, cr_pool_queued, cr_pool_total;
static bool cr_pool_draining;

static void cr_pool_init_once(void)
{
    tsdb_cond_init_mono(&cr_pool_cv);
    cr_pool_pid = getpid();
    atomic_store_explicit(&cr_pool_inited, true, memory_order_release);
}

static void *cr_pool_worker_main(void *arg)
{
    cr_job *j = arg;
    for (;;) {
        void (*fn)(void *) = j->fn;
        void *a = j->arg;
        fn(a);
        pthread_mutex_lock(&cr_pool_mu);
        if (cr_pool_head) {
            j = cr_pool_head;
            cr_pool_head = j->next;
            if (!cr_pool_head)
                cr_pool_tail = NULL;
            cr_pool_queued--;
            pthread_mutex_unlock(&cr_pool_mu);
            continue;
        }
        if (cr_pool_draining) {
            cr_pool_total--;
            pthread_cond_broadcast(&cr_pool_cv);
            pthread_mutex_unlock(&cr_pool_mu);
            return NULL;
        }
        cr_pool_idle++;
        struct timespec dl;
        tsdb_cond_deadline_ms(&dl, CR_POOL_IDLE_MS);
        while (!cr_pool_head && !cr_pool_draining) {
            if (pthread_cond_timedwait(&cr_pool_cv, &cr_pool_mu, &dl) == ETIMEDOUT)
                break;
        }
        cr_pool_idle--;
        if (cr_pool_head) {
            j = cr_pool_head;
            cr_pool_head = j->next;
            if (!cr_pool_head)
                cr_pool_tail = NULL;
            cr_pool_queued--;
            pthread_mutex_unlock(&cr_pool_mu);
            continue;
        }
        cr_pool_total--;
        pthread_cond_broadcast(&cr_pool_cv);
        pthread_mutex_unlock(&cr_pool_mu);
        return NULL;
    }
}

static int cr_pool_submit(cr_job *j)
{
    pthread_once(&cr_pool_once, cr_pool_init_once);
    if (getpid() != cr_pool_pid)
        return -1;
    j->next = NULL;
    pthread_mutex_lock(&cr_pool_mu);
    if (cr_pool_draining) {
        pthread_mutex_unlock(&cr_pool_mu);
        return -1;
    }
    if (cr_pool_idle > cr_pool_queued) {
        if (cr_pool_tail)
            cr_pool_tail->next = j;
        else
            cr_pool_head = j;
        cr_pool_tail = j;
        cr_pool_queued++;
        pthread_cond_signal(&cr_pool_cv);
        pthread_mutex_unlock(&cr_pool_mu);
        return 0;
    }
    if (cr_pool_total >= CR_POOL_MAX) {
        pthread_mutex_unlock(&cr_pool_mu);
        return -1;
    }
    cr_pool_total++;
    pthread_mutex_unlock(&cr_pool_mu);

    pthread_attr_t at;
    pthread_t th;
    pthread_attr_init(&at);
    pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED);
#ifdef __APPLE__
    pthread_attr_setstacksize(&at, TSDB_THREAD_STACK);
#endif
    int e = pthread_create(&th, &at, cr_pool_worker_main, j);
    pthread_attr_destroy(&at);
    if (e != 0) {
        pthread_mutex_lock(&cr_pool_mu);
        cr_pool_total--;
        pthread_cond_broadcast(&cr_pool_cv);
        pthread_mutex_unlock(&cr_pool_mu);
        return -1;
    }
    return 0;
}

__attribute__((destructor)) static void cr_pool_fini(void)
{
    if (!atomic_load_explicit(&cr_pool_inited, memory_order_acquire))
        return;
    if (getpid() != cr_pool_pid)
        return;
    pthread_mutex_lock(&cr_pool_mu);
    cr_pool_draining = true;
    pthread_cond_broadcast(&cr_pool_cv);
    while (cr_pool_total > 0)
        pthread_cond_wait(&cr_pool_cv, &cr_pool_mu);
    pthread_mutex_unlock(&cr_pool_mu);
}

static int obj_read(void *ctx, unsigned pos, uint64_t off, void *buf, size_t len);
static bool obj_healthy_pos_ready(const chunk_obj *o, unsigned pos);

enum { PS_UNKNOWN = 0, PS_GOOD, PS_BAD };

typedef struct rr {
    const chunk_source *src;
    const chunk_geom *g;
    const uint64_t *rec;
    chunk_report *rep;
    bool raw;
    uint64_t base[CHUNK_POS_MAX];
    uint8_t *slot[CHUNK_POS_MAX]; /* a buffer of one cell per position, made when first used, kept for the call */
    size_t cap;
    uint8_t zero_[1];
} rr;

static void note(rr *r, unsigned pos, int st)
{
    r->rep->st[pos] = (uint8_t)st;
    r->rep->bad_cells++;
}

static uint8_t *rr_slot(rr *r, unsigned pos)
{
    if (!r->slot[pos])
        r->slot[pos] = malloc(r->cap ? r->cap : 1);
    return r->slot[pos];
}

/* Reads cell `idx` of chunk `pos` whole into dst (len bytes) and checks it: 0 or a CHUNK_ST_* status. */
static int fetch_checked(rr *r, unsigned pos, uint64_t idx, size_t len, uint8_t *dst)
{
    uint64_t want;
    if (r->rec) {
        want = r->rec[r->base[pos] + idx];
    } else {
        int st = 0;
        const uint64_t *tab = r->src->sums(r->src->ctx, pos, &st);
        if (!tab)
            return st ? st : CHUNK_ST_IOERR;
        want = tab[idx];
    }
    int st = r->src->read(r->src->ctx, pos, idx * r->g->cell, dst, len);
    if (st)
        return st;
    if (xxh64(dst, len, 0) != want && chunk_mutant != CHUNK_MUT_NOSUMS) /* planted: sums not checked */
        return CHUNK_ST_DAMAGED;
    return 0;
}

static void copy_out(const uint8_t *cell_bytes, uint64_t x, uint64_t y, uint8_t *dst)
{
    memcpy(dst, cell_bytes + x, (size_t)(y - x));
}

/* The stripe `s` has needed cells that failed (bad[i]): rebuilds them from the others into dst. */
static int degrade(rr *r, uint64_t s, uint64_t off, const bool *bad, const bool *have, const uint64_t *xs,
                   const uint64_t *ys, uint8_t *buf)
{
#ifndef TSDB_EC
    (void)r, (void)s, (void)off, (void)bad, (void)have, (void)xs, (void)ys, (void)buf;
    return TSDB_ERR_INVALID;
#else
    const chunk_geom *g = r->g;
    unsigned k = g->k, m = g->m;
    uint64_t ncells = chunk_ncells(g), Ls = chunk_stripe_len(g, s);
    uint64_t x0 = Ls, y0 = 0;
    for (unsigned i = 0; i < k; i++)
        if (bad[i]) {
            if (xs[i] < x0)
                x0 = xs[i];
            if (ys[i] > y0)
                y0 = ys[i];
        }
    if (!r->raw)
        x0 = 0, y0 = Ls; /* whole cells, each checked */
    size_t w = (size_t)(y0 - x0);
    uint8_t *shards[CHUNK_POS_MAX];
    bool present[CHUNK_POS_MAX], isbad[CHUNK_POS_MAX];
    unsigned ngood = 0;
    memset(shards, 0, sizeof shards);
    memset(isbad, 0, sizeof isbad);
    for (unsigned i = 0; i < k; i++)
        isbad[i] = bad[i];
    for (unsigned p = 0; p < k + m; p++)
        present[p] = false;
    int sts[CHUNK_POS_MAX] = {0};
    for (unsigned p = 0; p < k + m && ngood < k; p++) { /* data first, then parity */
        if (isbad[p])
            continue;
        uint64_t c = p < k ? s * k + p : 0, cl = p < k ? chunk_cell_len(g, c) : Ls;
        uint8_t *b = rr_slot(r, p);
        if (!b)
            return TSDB_ERR_NOMEM;
        if (!r->raw && p < k && have[p]) { /* the direct pass read and checked it whole: only the padding is left */
            memset(b + cl, 0, w - (size_t)cl);
        } else if (p < k && c >= ncells) { /* beyond the object: zeros, no read */
            memset(b, 0, w);
            r->rep->st[p] = CHUNK_ST_VIRTUAL;
        } else if (r->raw) {
            size_t avail = x0 < cl ? (size_t)((y0 < cl ? y0 : cl) - x0) : 0;
            int st = avail ? r->src->read(r->src->ctx, p, s * g->cell + x0, b, avail) : 0;
            if (st) {
                sts[p] = st;
                note(r, p, st);
                continue;
            }
            memset(b + avail, 0, w - avail);
        } else {
            int st = fetch_checked(r, p, s, (size_t)cl, b);
            if (st) {
                sts[p] = st;
                note(r, p, st);
                continue;
            }
            memset(b + cl, 0, w - (size_t)cl);
        }
        shards[p] = b;
        present[p] = true;
        ngood++;
    }
    if (ngood < k) {
        unsigned absent = 0, damaged = 0, io = 0;
        for (unsigned p = 0; p < k + m; p++)
            absent += sts[p] == CHUNK_ST_ABSENT, damaged += sts[p] == CHUNK_ST_DAMAGED, io += sts[p] == CHUNK_ST_IOERR;
        unsigned nbad = 0, down = 0;
        for (unsigned p = 0; p < k + m; p++) {
            nbad += isbad[p] || sts[p];
            /* a position whose store is merely away: the cell may be fine (E15: unavailable, never lost) */
            down += (sts[p] ? sts[p] : isbad[p] ? r->rep->st[p] : 0) == CHUNK_ST_DOWN;
        }
        bool away = ngood + down >= k;
        if (!chunk_log_quiet)
            fprintf(stderr, "tsdb: object %s: stripe %llu has %u good chunks of the %u it needs (%u of %u positions "
                        "unusable: %u absent, %u damaged, %u unreadable, %u on stores that are down): %s\n",
                    r->src->name, (unsigned long long)s, ngood, k, nbad, k + m, absent, damaged, io, down,
                    away ? "unavailable while they are" : "not readable");
        return away ? TSDB_ERR_UNAVAILABLE : TSDB_ERR_CORRUPT;
    }
    /* the buffers the decode fills: every absent data position gets one of its own */
    for (unsigned p = 0; p < k + m; p++)
        if (!present[p]) {
            shards[p] = rr_slot(r, p);
            if (!shards[p])
                return TSDB_ERR_NOMEM;
        }
    iso_rs *rs = chunk_rs_get(k, m);
    if (!rs)
        return TSDB_ERR_NOMEM;
    if (iso_rs_decode_data(rs, shards, present, w) != ISO_OK)
        return TSDB_ERR_CORRUPT;
    for (unsigned i = 0; i < k; i++) {
        if (!bad[i])
            continue;
        uint64_t c = s * k + i;
        if (!r->raw && r->rec && xxh64(shards[i], (size_t)chunk_cell_len(g, c), 0) != r->rec[r->base[i] + s] &&
            chunk_mutant != CHUNK_MUT_NOSUMS) {
            if (!chunk_log_quiet)
                fprintf(stderr, "tsdb: object %s: stripe %llu rebuilt cell %u does not match its encode-time sum: the "
                            "chunks are not one encoding\n", r->src->name, (unsigned long long)s, i);
            return TSDB_ERR_CORRUPT;
        }
        copy_out(shards[i], xs[i] - x0, ys[i] - x0, buf + (c * g->cell + xs[i] - off));
        r->rep->decoded_cells++;
    }
    return 0;
#endif
}

typedef struct cr_pos_task {
    cr_job job;
    pthread_mutex_t *mu;
    pthread_cond_t *cv;
    unsigned *left;
    const chunk_obj *o;
    const chunk_geom *g;
    const uint64_t *rec;
    uint64_t base;
    uint64_t off, len;
    uint64_t s0, s1, c0, c1;
    uint8_t *buf;
    unsigned pos;
    size_t cap;
    bool raw;
    bool failed;
} cr_pos_task;

static void cr_pos_run(cr_pos_task *t)
{
    const chunk_geom *g = t->g;
    unsigned k = g->k, pos = t->pos;
    uint8_t *slot = NULL;
    if (!t->raw) {
        slot = malloc(t->cap ? t->cap : 1);
        if (!slot) {
            t->failed = true;
            return;
        }
    }
    const uint64_t *sums = NULL;
    if (!t->raw && !t->rec) {
        int st = 0;
        sums = chunk_obj_pos_sums(t->o, pos);
        if (!sums) {
            (void)st;
            free(slot);
            t->failed = true;
            return;
        }
    }
    bool failed = false;
    for (uint64_t s = t->s0; s <= t->s1 && !failed; s++) {
        uint64_t c = s * k + pos;
        if (c < t->c0 || c > t->c1)
            continue;
        uint64_t cs = c * g->cell, cl = chunk_cell_len(g, c);
        uint64_t a = t->off > cs ? t->off : cs;
        uint64_t b = t->off + t->len < cs + cl ? t->off + t->len : cs + cl;
        uint64_t x = a - cs, y = b - cs;
        uint8_t *dst = t->buf + (a - t->off);
        if (t->raw) {
            if (obj_read((void *)t->o, pos, s * g->cell + x, dst, (size_t)(y - x)) != 0)
                failed = true;
        } else {
            uint64_t want = t->rec ? t->rec[t->base + s] : sums[s];
            if (obj_read((void *)t->o, pos, s * g->cell, slot, (size_t)cl) != 0) {
                failed = true;
            } else if (xxh64(slot, (size_t)cl, 0) != want && chunk_mutant != CHUNK_MUT_NOSUMS) {
                failed = true;
            } else {
                copy_out(slot, x, y, dst);
            }
        }
    }
    free(slot);
    t->failed = failed;
}

static void cr_pos_work(void *arg)
{
    cr_pos_task *t = arg;
    cr_pos_run(t);
    pthread_mutex_lock(t->mu);
    (*t->left)--;
    if (*t->left == 0)
        pthread_cond_signal(t->cv);
    pthread_mutex_unlock(t->mu);
}

/* Parallel healthy fast path for local chunk_obj multi-chunk reads: runs one task per touched data position across
 * [s0, s1] (pos 0 inline, others on the cached I/O pool). Returns true when every cell succeeded; returns false to
 * fall back to the exact stripe-by-stripe degraded path (zero change to degrade/report/error semantics). */
static bool try_read_parallel(const chunk_obj *o, const chunk_geom *g, const uint64_t *rec_sums, uint64_t off,
                              uint64_t len, uint8_t *buf, bool raw, uint64_t c0, uint64_t c1)
{
    unsigned k = g->k;
    uint64_t s0 = c0 / k, s1 = c1 / k;
    unsigned pos_list[CHUNK_K_MAX], npos = 0;
    for (unsigned i = 0; i < k; i++) {
        uint64_t first = s0 * k + i;
        if (first < c0)
            first += k;
        if (first <= c1) {
            if (!obj_healthy_pos_ready(o, i))
                return false; /* missing/damaged data chunk: let stripe-by-stripe degrade() handle it */
            pos_list[npos++] = i;
        }
    }
    if (npos < 2)
        return false;

    pthread_mutex_t mu = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t cv = PTHREAD_COND_INITIALIZER;
    unsigned left = 0;
    cr_pos_task tasks[CHUNK_K_MAX];
    bool inline_run[CHUNK_K_MAX] = {false};
    size_t cap = g->cell < g->len ? g->cell : (size_t)g->len;

    for (unsigned idx = 0; idx < npos; idx++) {
        unsigned p = pos_list[idx];
        tasks[idx] = (cr_pos_task){
            .job = {.fn = cr_pos_work, .arg = &tasks[idx]},
            .mu = &mu,
            .cv = &cv,
            .left = &left,
            .o = o,
            .g = g,
            .rec = rec_sums,
            .base = chunk_sums_base(g, p),
            .off = off,
            .len = len,
            .s0 = s0,
            .s1 = s1,
            .c0 = c0,
            .c1 = c1,
            .buf = buf,
            .pos = p,
            .cap = cap,
            .raw = raw,
        };
    }

    inline_run[0] = true; /* calling thread handles first position inline */
    pthread_mutex_lock(&mu);
    for (unsigned idx = 1; idx < npos; idx++)
        left++;
    pthread_mutex_unlock(&mu);

    unsigned failed_submits = 0;
    for (unsigned idx = 1; idx < npos; idx++) {
        if (cr_pool_submit(&tasks[idx].job) != 0) {
            inline_run[idx] = true;
            failed_submits++;
        }
    }
    if (failed_submits > 0) {
        pthread_mutex_lock(&mu);
        left -= failed_submits;
        pthread_mutex_unlock(&mu);
    }

    for (unsigned idx = 0; idx < npos; idx++)
        if (inline_run[idx])
            cr_pos_run(&tasks[idx]);

    pthread_mutex_lock(&mu);
    while (left > 0)
        pthread_cond_wait(&cv, &mu);
    pthread_mutex_unlock(&mu);
    pthread_cond_destroy(&cv);
    pthread_mutex_destroy(&mu);

    for (unsigned idx = 0; idx < npos; idx++)
        if (tasks[idx].failed)
            return false;
    return true;
}

int chunk_read_range(const chunk_source *src, const chunk_geom *g, const uint64_t *rec_sums, uint64_t off,
                     uint64_t len, void *vbuf, unsigned flags, chunk_report *rep)
{
    chunk_report local;
    if (!rep)
        rep = &local;
    memset(rep, 0, sizeof *rep);
    if (!chunk_geom_valid(g) || off > g->len || len > g->len - off)
        return TSDB_ERR_INVALID;
    if (len == 0)
        return 0;
    uint8_t *buf = vbuf;
    unsigned k = g->k;
    uint64_t c0 = off / g->cell, c1 = (off + len - 1) / g->cell;
    bool raw = (flags & CHUNK_RR_RAW) != 0;
    if (src->read == obj_read && k > 1 && c1 > c0 && len >= CR_PAR_MIN_BYTES && chunk_fault_read == 0) {
        if (try_read_parallel((const chunk_obj *)src->ctx, g, rec_sums, off, len, buf, raw, c0, c1))
            return 0;
    }
    rr r;
    memset(&r, 0, sizeof r);
    r.src = src;
    r.g = g;
    r.rec = rec_sums;
    r.rep = rep;
    r.raw = raw;
    r.cap = g->cell < g->len ? g->cell : (size_t)g->len;
    for (unsigned p = 0; p < chunk_npos(g); p++)
        r.base[p] = chunk_sums_base(g, p);
    int rc = 0;
    for (uint64_t s = c0 / k; s <= c1 / k && rc == 0; s++) {
        bool bad[CHUNK_K_MAX] = {false}, have[CHUNK_K_MAX] = {false};
        uint64_t xs[CHUNK_K_MAX] = {0}, ys[CHUNK_K_MAX] = {0};
        bool any = false;
        for (unsigned i = 0; i < k; i++) {
            uint64_t c = s * k + i;
            if (c < c0 || c > c1)
                continue;
            uint64_t cs = c * g->cell, cl = chunk_cell_len(g, c);
            uint64_t a = off > cs ? off : cs, b = off + len < cs + cl ? off + len : cs + cl;
            xs[i] = a - cs;
            ys[i] = b - cs;
            uint8_t *dst = buf + (a - off);
            int st;
            if (r.raw) {
                st = src->read(src->ctx, i, s * g->cell + xs[i], dst, (size_t)(ys[i] - xs[i]));
            } else {
                uint8_t *slot = rr_slot(&r, i);
                st = slot ? fetch_checked(&r, i, s, (size_t)cl, slot) : CHUNK_ST_IOERR;
                if (st == 0) {
                    copy_out(slot, xs[i], ys[i], dst);
                    have[i] = true; /* whole and checked: a source for the others' rebuild */
                }
            }
            if (st) {
                note(&r, i, st);
                bad[i] = true;
                any = true;
            }
        }
        if (any)
            rc = degrade(&r, s, off, bad, have, xs, ys, buf);
    }
    for (unsigned p = 0; p < CHUNK_POS_MAX; p++)
        free(r.slot[p]);
    return rc;
}

/* ---------------------------------------------------------------- an object in a backend */
struct chunk_obj {
    chunk_backend *be;
    uint32_t volume;
    uint8_t vuuid[16], oid[16];
    chunk_geom g;
    pthread_mutex_t mu;               /* serializes lazy open of deferred parity positions */
    atomic_bool tried[CHUNK_POS_MAX]; /* release-stored once rd[p]/st[p] are populated */
    chunk_rd *rd[CHUNK_POS_MAX];
    int st[CHUNK_POS_MAX];
    const uint64_t *rec;
    char name[40];
};

static int st_of(int rc)
{
    return rc == TSDB_ERR_NOTFOUND ? CHUNK_ST_ABSENT : rc == TSDB_ERR_CORRUPT ? CHUNK_ST_DAMAGED : CHUNK_ST_IOERR;
}

static bool obj_healthy_pos_ready(const chunk_obj *o, unsigned pos)
{
    return pos < CHUNK_POS_MAX && atomic_load_explicit(&((chunk_obj *)o)->tried[pos], memory_order_acquire) &&
           o->rd[pos] != NULL;
}

static int obj_open_pos(chunk_obj *o, unsigned p, bool *have, int *worst)
{
    chunk_ident id = {.volume = o->volume, .pos = (uint16_t)p};
    memcpy(id.vuuid, o->vuuid, 16);
    memcpy(id.oid, o->oid, 16);
    chunk_rd *rd = NULL;
    int rc = chunk_be_rd_open(o->be, &id, &rd);
    if (rc == 0) {
        const chunk_geom *hg = &chunk_rd_hdr(rd)->g;
        if (!*have) { /* the geometry is the first valid chunk's: the object's own, recorded in every header */
            o->g = *hg;
            *have = true;
        }
        if (memcmp(hg, &o->g, sizeof *hg) != 0) { /* another encoding's chunk: not this object's */
            chunk_rd_close(rd);
            o->st[p] = CHUNK_ST_DAMAGED;
            return TSDB_ERR_CORRUPT;
        }
        o->rd[p] = rd;
        o->st[p] = CHUNK_ST_OK;
        return 0;
    }
    o->st[p] = st_of(rc);
    if (worst && (rc == TSDB_ERR_IO || (rc == TSDB_ERR_CORRUPT && *worst != TSDB_ERR_IO)))
        *worst = rc;
    return rc;
}

/* Lazily opens a deferred parity position (k .. k+m-1) on first use (degrade() or heal); lock-free once tried. */
static void obj_ensure_pos(chunk_obj *o, unsigned pos)
{
    if (pos >= CHUNK_POS_MAX || atomic_load_explicit(&o->tried[pos], memory_order_acquire))
        return;
    pthread_mutex_lock(&o->mu);
    if (!atomic_load_explicit(&o->tried[pos], memory_order_relaxed)) {
        if (pos < chunk_npos(&o->g)) {
            bool have = true;
            (void)obj_open_pos(o, pos, &have, NULL);
            if (!o->rd[pos] && o->st[pos] == CHUNK_ST_ABSENT &&
                chunk_be_store_state(o->be, chunk_be_pos_store(o->be, o->oid, pos)) != CHUNK_STORE_UP)
                o->st[pos] = CHUNK_ST_DOWN;
        } else {
            o->st[pos] = CHUNK_ST_ABSENT;
        }
        atomic_store_explicit(&o->tried[pos], true, memory_order_release);
    }
    pthread_mutex_unlock(&o->mu);
}

static int obj_read(void *ctx, unsigned pos, uint64_t off, void *buf, size_t len)
{
    chunk_obj *o = ctx;
    if (pos >= CHUNK_POS_MAX)
        return CHUNK_ST_IOERR;
    obj_ensure_pos(o, pos);
    if (!o->rd[pos])
        return o->st[pos] ? o->st[pos] : CHUNK_ST_ABSENT;
    return chunk_rd_read(o->rd[pos], off, buf, len) == 0 ? 0 : CHUNK_ST_IOERR;
}

static const uint64_t *obj_sums(void *ctx, unsigned pos, int *status)
{
    chunk_obj *o = ctx;
    if (pos >= CHUNK_POS_MAX) {
        *status = CHUNK_ST_ABSENT;
        return NULL;
    }
    obj_ensure_pos(o, pos);
    if (!o->rd[pos]) {
        *status = o->st[pos] ? o->st[pos] : CHUNK_ST_ABSENT;
        return NULL;
    }
    return chunk_rd_sums(o->rd[pos]);
}

int chunk_obj_open(chunk_backend *be, uint32_t volume, const uint8_t vuuid[16], const uint8_t oid[16],
                   const chunk_geom *expect, const uint64_t *rec_sums, chunk_obj **out)
{
    chunk_obj *o = calloc(1, sizeof *o);
    if (!o)
        return TSDB_ERR_NOMEM;
    o->be = be;
    o->volume = volume;
    memcpy(o->vuuid, vuuid, 16);
    memcpy(o->oid, oid, 16);
    o->rec = rec_sums;
    pthread_mutex_init(&o->mu, NULL);
    chunk_oid_hex(oid, o->name);
    bool have = expect != NULL;
    if (have)
        o->g = *expect;
    bool alive = false;
    int worst = TSDB_ERR_NOTFOUND;
    /* Healthy path: open only data positions 0 .. k-1; parity positions k .. k+m-1 open lazily in obj_ensure_pos()
     * when degrade() or heal actually needs them. If geometry is unknown (!have), scan until the first valid header
     * discovers o->g (then finish up to k-1). If all data positions failed (!alive), scan parity too so an object
     * surviving only on parity chunks still opens. */
    unsigned p = 0;
    for (; p < CHUNK_POS_MAX; p++) {
        if (have && alive && p >= o->g.k)
            break;
        if (have && p >= chunk_npos(&o->g))
            break;
        int rc = obj_open_pos(o, p, &have, &worst);
        atomic_store_explicit(&o->tried[p], true, memory_order_relaxed);
        if (rc == 0)
            alive = true;
    }
    bool away = false; /* a store that is not UP: what it holds may be what this object lacks */
    for (int i = 0; i < chunk_be_nstores(be); i++)
        away |= chunk_be_store_state(be, i) != CHUNK_STORE_UP;
    if (!alive || !have) {
        if (away && !chunk_log_quiet && worst != TSDB_ERR_NOTFOUND)
            fprintf(stderr, "tsdb: object %s: no chunk of it can be read while a store is down: unavailable\n", o->name);
        else if (worst != TSDB_ERR_NOTFOUND && !chunk_log_quiet)
            fprintf(stderr, "tsdb: object %s: no chunk of it can be read: not readable\n", o->name);
        chunk_obj_close(o);
        if (away)
            return TSDB_ERR_UNAVAILABLE;
        return worst == TSDB_ERR_NOTFOUND ? TSDB_ERR_NOTFOUND : TSDB_ERR_CORRUPT;
    }
    if (away)
        for (unsigned i = 0; i < p; i++)
            if (!o->rd[i] && o->st[i] == CHUNK_ST_ABSENT &&
                chunk_be_store_state(be, chunk_be_pos_store(be, oid, i)) != CHUNK_STORE_UP)
                o->st[i] = CHUNK_ST_DOWN; /* it may be there: it comes back with its store (or its heal) */
    if (chunk_mutant == CHUNK_MUT_SETTINGS) { /* planted: the layout taken from today's settings, not from the object */
        chunk_geom t;
        if (chunk_geom_derive(&t, chunk_settings_k, chunk_settings_m, o->g.len) == 0)
            o->g = t;
    }
    *out = o;
    return 0;
}

void chunk_obj_close(chunk_obj *o)
{
    if (!o)
        return;
    for (unsigned p = 0; p < CHUNK_POS_MAX; p++)
        chunk_rd_close(o->rd[p]);
    pthread_mutex_destroy(&o->mu);
    free(o);
}

const chunk_geom *chunk_obj_geom(const chunk_obj *o) { return &o->g; }

const uint64_t *chunk_obj_pos_sums(const chunk_obj *o, unsigned pos)
{
    if (pos >= CHUNK_POS_MAX)
        return NULL;
    obj_ensure_pos((chunk_obj *)o, pos);
    return o->rd[pos] ? chunk_rd_sums(o->rd[pos]) : NULL;
}

int chunk_obj_pos_status(const chunk_obj *o, unsigned pos)
{
    if (pos >= CHUNK_POS_MAX)
        return CHUNK_ST_ABSENT;
    obj_ensure_pos((chunk_obj *)o, pos);
    return o->rd[pos] ? CHUNK_ST_OK : o->st[pos];
}

uint64_t chunk_obj_epoch(const chunk_obj *o)
{
    uint64_t e = 0;
    unsigned n = chunk_npos(&o->g);
    for (unsigned p = 0; p < n; p++) {
        obj_ensure_pos((chunk_obj *)o, p);
        if (o->rd[p] && chunk_rd_hdr(o->rd[p])->epoch > e)
            e = chunk_rd_hdr(o->rd[p])->epoch;
    }
    return e;
}

int chunk_obj_source(chunk_obj *o, chunk_source *src)
{
    memset(src, 0, sizeof *src);
    src->ctx = o;
    memcpy(src->name, o->name, sizeof o->name);
    src->read = obj_read;
    src->sums = obj_sums;
    return 0;
}

int chunk_obj_read(chunk_obj *o, uint64_t off, uint64_t len, void *buf, unsigned flags, chunk_report *rep)
{
    chunk_source src;
    chunk_obj_source(o, &src);
    return chunk_read_range(&src, &o->g, o->rec, off, len, buf, flags, rep);
}
