/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R9 checked stripe fill worker (server/sfill.h; docs/design-cluster-data.md §5.3, E13, E14, E15; R9 plan D9-D11,
 * D18; track r9-fill).
 *
 * Threading model:
 *   - One coordinator thread per sfill instance fetches paged OBJLIST(volume, version < V, cursor) for each active
 *     filling job (never holding sf->mu across I/O) and pushes per-object tasks onto the priority task queue.
 *   - A worker pool of up to `rebuild_concurrency` (default 4, clamped 1..64) worker threads pops tasks in lowest-
 *     margin-first order (E13), runs the checked copy-before-decode pipeline, writes durably via chunk_wr_open_heal +
 *     chunk_wr_commit (tmp -> fsync -> rename -> dir-fsync), reads back and scrubs the written chunk, fires
 *     CP_FILL_WRITTEN, and notifies log replicas via notify_reopen (CS_OP_REOPEN -> eccache_reopen).
 *   - sfill_report() is strictly non-blocking (copies current job snapshots under sf->mu).
 *
 * Lock order: sf->mu is a leaf lock (never held across network I/O, disk I/O, pacing waits, or user callbacks).
 */
#include "sfill.h"
#include "../src/chunk_priv.h"

#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../include/tsdb.h"
#include "../src/chunk_priv.h"
#include "../src/compat.h"
#include "../src/internal.h"
#include "crashpt.h"
#include "pace.h"

#ifdef TSDB_EC
#include "../src/ec/rs.h"
#endif

#define SFILL_DEFAULT_CONCURRENCY 4u
#define SFILL_MAX_WORKERS 64u
#define SFILL_DEFAULT_PAGE 256u
#define SFILL_DEFAULT_RETRY_MS 300
#define SFILL_PACE_CAP_NS 500000000ull /* 500 ms cap per pace call so workers stay responsive to stop/shutdown */

_Atomic int sfill_mutant = SFILL_MUT_NONE;

#ifdef TSDB_TEST_HOOKS
#include <unistd.h>
_Atomic int sfill_test_readback_flip = 0;
uint32_t sfill_test_objlist_page = 0;
int sfill_test_retry_ms = 0;
static _Atomic int sfill_test_obj_delay_ms = 0;
static _Atomic int sfill_test_pause_after = -1;
static _Atomic int sfill_test_pause_seen = 0;
static char sfill_test_pause_path[512];
#endif

int sfill_mutant_parse(const char *name)
{
    if (!name || !*name || strcmp(name, "none") == 0)
        return SFILL_MUT_NONE;
    if (strcmp(name, "fillnocheck") == 0)
        return SFILL_MUT_NOCHECK;
    if (strcmp(name, "fillfewerk") == 0)
        return SFILL_MUT_FEWERK;
    if (strcmp(name, "fillsurvivortrust") == 0)
        return SFILL_MUT_SURVIVOR_TRUST;
    if (strcmp(name, "wholeearly") == 0)
        return SFILL_MUT_WHOLE_EARLY;
    return -1;
}

void sfill_set_mutant(int mut)
{
    atomic_store_explicit(&sfill_mutant, mut, memory_order_relaxed);
}

void sfill_objs_free(struct sfill_obj *objs, uint32_t n)
{
    if (!objs)
        return;
    for (uint32_t i = 0; i < n; i++)
        free(objs[i].sums);
    free(objs);
}

static inline bool pos_written(uint32_t positions, unsigned pos)
{
    return positions == 0 || (pos < 32u && (positions & (1u << pos)) != 0);
}

static uint64_t mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

static uint64_t wall_ms_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

static uint64_t mono_to_wall_ms(uint64_t mono_ts, uint64_t mono_now, uint64_t wall_now)
{
    uint64_t age = mono_now >= mono_ts ? mono_now - mono_ts : 0;
    return wall_now > age ? wall_now - age : 0;
}

/* ------------------------------------------------------------------ internal state */

enum sfill_obj_rc {
    SFILL_OBJ_OK = 0,
    SFILL_OBJ_UNAVAILABLE = 1,
    SFILL_OBJ_CORRUPT = 2,
    SFILL_OBJ_FAILED = 3,
    SFILL_OBJ_CANCELLED = 4,
};

#define SFILL_BUCKETS 4u

struct sfill_job {
    struct sfill_job *next;
    struct sfill_job_cfg cfg;
    uint64_t gen;          /* bumped on stripe_version change or removal */
    uint64_t done;         /* verified objects in current pass */
    uint64_t total;        /* objects discovered so far in current pass */
    uint64_t bytes_done;   /* chunk bytes verified in current pass (HBS_PROGRESS) */
    uint64_t bytes_total;  /* chunk bytes discovered so far in current pass (HBS_PROGRESS) */
    uint64_t start_ms;     /* monotonic ms when job started */
    uint64_t progress_ms;  /* monotonic ms when job last advanced */
    uint64_t failures;     /* failed passes or object attempts */
    uint8_t whole;         /* 1 once every object < stripe_version is durably held and verified */
    bool removed;          /* sfill_stop() called: hidden from reports and freed when inflight == 0 */
    bool listing;          /* coordinator is currently fetching one OBJLIST page */
    bool pass_active;      /* currently streaming pages of a verification/fill pass */
    bool pass_has_cursor;  /* pass_cursor holds the last oid of the previous page */
    bool pass_more;        /* more OBJLIST pages remain in the current pass */
    bool has_vuuid;        /* vuuid populated from the first page of the current pass */
    uint8_t pass_cursor[16];
    uint8_t vuuid[16];
    uint64_t pass_num;     /* 0 on initial pass (incl. post-crash restart), >0 on retry passes */
    uint64_t pass_gen;     /* gen when current pass began */
    uint64_t round_pending;/* queued/running tasks for the current page */
    uint64_t round_errors; /* objects in current pass that did not succeed */
    uint64_t retry_at_ms;  /* monotonic ms before next pass attempt (0: ready now) */
    uint64_t pace_id;      /* active pace_job_begin_ex(PACE_RECV, ...) id, or 0 */
    uint64_t inflight;     /* tasks currently referencing this job */
};

struct sfill_task {
    struct sfill_task *next;
    struct sfill_job *job; /* NULL for ad-hoc reconcile tasks enqueued via sfill_enqueue_obj */
    uint64_t gen;
    uint32_t volume;
    uint32_t pos;
    uint32_t prev_node;
    int margin;
    bool first_pass;
    bool force_write;
    bool (*cancel_cb)(void *arg);
    void (*done_cb)(void *arg, int rc);
    void *done_arg;
    uint8_t vuuid[16];
    uint64_t obj_bytes;
    struct sfill_obj obj; /* owns obj.sums */
};

struct sfill {
    struct sfill_ops ops;
    bool has_ops;

    pthread_mutex_t pub_mu;  /* serializes publish_reports() -> on_report(); lock order: pub_mu -> mu */
    pthread_mutex_t mu;
    pthread_cond_t coord_cv; /* CLOCK_MONOTONIC */
    pthread_cond_t work_cv;  /* CLOCK_MONOTONIC */

    bool stopping;
    bool coord_started;
    pthread_t coord_thr;

    pthread_t workers[SFILL_MAX_WORKERS];
    uint32_t nworkers;
    uint32_t want_workers;

    struct sfill_job *jobs;
    struct sfill_task *q_head[SFILL_BUCKETS];
    struct sfill_task *q_tail[SFILL_BUCKETS];

    /* Stats (relaxed atomics, updated by workers without mu) */
    struct {
        atomic_uint_fast64_t copied;
        atomic_uint_fast64_t decoded;
        atomic_uint_fast64_t already_held;
        atomic_uint_fast64_t skipped_small;
        atomic_uint_fast64_t failed;
        atomic_uint_fast64_t unavailable;
        atomic_uint_fast64_t corrupt;
        atomic_uint_fast64_t survivor_bad;
        atomic_uint_fast64_t reopened;
        atomic_uint_fast64_t bytes_read;
        atomic_uint_fast64_t bytes_written;
    } st;
};

#define SF_STAT(sf, f) atomic_fetch_add_explicit(&(sf)->st.f, 1, memory_order_relaxed)
#define SF_STATN(sf, f, v) atomic_fetch_add_explicit(&(sf)->st.f, (v), memory_order_relaxed)

static void task_free(struct sfill_task *t)
{
    if (!t)
        return;
    free(t->obj.sums);
    free(t);
}

static bool job_cancelled(struct sfill *sf, struct sfill_job *j, uint64_t gen)
{
    pthread_mutex_lock(&sf->mu);
    bool cancel = sf->stopping || (j && (j->removed || j->gen != gen));
    pthread_mutex_unlock(&sf->mu);
    return cancel;
}

static bool task_cancelled(struct sfill *sf, const struct sfill_task *t)
{
    if (job_cancelled(sf, t->job, t->gen))
        return true;
    return t->cancel_cb && t->cancel_cb(t->done_arg);
}

static void publish_reports(struct sfill *sf)
{
    if (!sf->has_ops || !sf->ops.on_report)
        return;
    struct sfill_report buf[256];
    /* Serialize snapshot + delivery so an earlier whole=0 snapshot can never overwrite a newer whole=1 report. */
    pthread_mutex_lock(&sf->pub_mu);
    uint32_t n = sfill_report(sf, buf, 256);
    sf->ops.on_report(sf->ops.arg, buf, n);
    pthread_mutex_unlock(&sf->pub_mu);
}

static int retry_delay_ms(void)
{
#ifdef TSDB_TEST_HOOKS
    if (sfill_test_retry_ms > 0)
        return sfill_test_retry_ms;
#endif
    return SFILL_DEFAULT_RETRY_MS;
}

static uint32_t objlist_page_size(void)
{
#ifdef TSDB_TEST_HOOKS
    if (sfill_test_objlist_page > 0)
        return sfill_test_objlist_page;
#endif
    return SFILL_DEFAULT_PAGE;
}

/* ------------------------------------------------------------------ local chunk verification & write */

/* Returns true if local `be` already holds a complete, sum-verified chunk for `(id, obj)`. */
static bool local_chunk_intact(chunk_backend *be, const chunk_ident *id, const struct sfill_obj *obj)
{
    if (!be)
        return false;
    chunk_rd *rd = NULL;
    if (chunk_be_rd_open(be, id, &rd) != 0)
        return false;
    const chunk_hdr *h = chunk_rd_hdr(rd);
    uint64_t nc = chunk_pos_cells(&obj->g, id->pos);
    uint64_t base = chunk_sums_base(&obj->g, id->pos);
    bool ok = h->epoch == obj->writer_epoch && memcmp(&h->g, &obj->g, sizeof h->g) == 0 &&
              base + nc <= obj->nsums &&
              memcmp(chunk_rd_sums(rd), obj->sums + base, (size_t)nc * sizeof(uint64_t)) == 0;
    if (ok) {
        uint64_t nbad = 0;
        ok = chunk_rd_scrub(rd, &nbad) == 0 && nbad == 0;
    }
    chunk_rd_close(rd);
    return ok;
}

/* Read back and verify a just-committed chunk on `st`. */
static bool verify_written_chunk(chunk_store *st, const chunk_ident *id, const struct sfill_obj *obj,
                                 bool bypass_rec_sums)
{
#ifdef TSDB_TEST_HOOKS
    int f = atomic_load_explicit(&sfill_test_readback_flip, memory_order_relaxed);
    while (f > 0 && !atomic_compare_exchange_weak_explicit(&sfill_test_readback_flip, &f, f - 1,
                                                           memory_order_acq_rel, memory_order_relaxed))
        ;
    if (f > 0)
        (void)chunk_store_test_flip(st, id);
#endif
    chunk_rd *rd = NULL;
    if (chunk_rd_open(st, id, &rd) != 0)
        return false;
    const chunk_hdr *h = chunk_rd_hdr(rd);
    uint64_t nc = chunk_pos_cells(&obj->g, id->pos);
    uint64_t base = chunk_sums_base(&obj->g, id->pos);
    bool ok = h->epoch == obj->writer_epoch && memcmp(&h->g, &obj->g, sizeof h->g) == 0;
    if (ok && !bypass_rec_sums)
        ok = base + nc <= obj->nsums &&
             memcmp(chunk_rd_sums(rd), obj->sums + base, (size_t)nc * sizeof(uint64_t)) == 0;
    if (ok) {
        uint64_t nbad = 0;
        ok = chunk_rd_scrub(rd, &nbad) == 0 && nbad == 0;
    }
    chunk_rd_close(rd);
    return ok;
}

/* Open a local chunk writer for `(id, obj)` at `obj->writer_epoch`, fenced at `fence_epoch`. */
static int open_local_writer(chunk_backend *be, const chunk_ident *id, const struct sfill_obj *obj,
                             uint64_t fence_epoch, chunk_store **st_out, chunk_wr **w_out)
{
    *st_out = NULL;
    *w_out = NULL;
    if (!be || chunk_be_nstores(be) <= 0)
        return TSDB_ERR_UNAVAILABLE;
    uint64_t fe = fence_epoch >= obj->writer_epoch ? fence_epoch : obj->writer_epoch;
    if (fe == 0)
        fe = 1;
    int rc = chunk_be_prepare(be, id->volume, fe);
    if (rc != 0)
        return rc;
    int n = chunk_be_nstores(be);
    int base = chunk_be_place(be, id->oid, id->pos);
    int idx = base;
    for (int k = 0; k < n; k++) {
        int cand = (base + k) % n;
        if (chunk_be_store_state(be, cand) == CHUNK_STORE_UP && !chunk_be_store_draining(be, cand)) {
            idx = cand;
            break;
        }
    }
    chunk_store *st = chunk_be_open_store(be, idx);
    if (!st)
        return TSDB_ERR_UNAVAILABLE;
    rc = chunk_wr_open_heal(st, id, &obj->g, obj->writer_epoch, fe, w_out);
    if (rc != 0)
        return rc;
    *st_out = st;
    return 0;
}

/* ------------------------------------------------------------------ 1x copy from prev_node */

/* Attempts a 1x copy of position `pos` from `prev_node`, verifying every cell against obj->sums before writing.
 * Returns SFILL_OBJ_OK on verified durable copy, or non-zero to fall back to RS decode. */
static int try_copy_from_prev(struct sfill *sf, struct sfill_task *t, chunk_backend *be,
                              const chunk_ident *id, uint64_t vol_epoch)
{
    const struct sfill_obj *obj = &t->obj;
    const chunk_geom *g = &obj->g;
    uint64_t ncells = chunk_pos_cells(g, t->pos);
    uint64_t base = chunk_sums_base(g, t->pos);
    if (ncells == 0 || base + ncells > obj->nsums)
        return SFILL_OBJ_FAILED;

    uint8_t *buf = malloc(g->cell ? g->cell : 1);
    if (!buf)
        return SFILL_OBJ_FAILED;

    chunk_store *st = NULL;
    chunk_wr *w = NULL;
    int rc = open_local_writer(be, id, obj, vol_epoch, &st, &w);
    if (rc != 0) {
        free(buf);
        return SFILL_OBJ_UNAVAILABLE;
    }

    int res = SFILL_OBJ_OK;
    uint64_t total_written = 0;
    for (uint64_t c = 0; c < ncells; c++) {
        if (task_cancelled(sf, t)) {
            res = SFILL_OBJ_CANCELLED;
            break;
        }
        uint64_t clen = chunk_pos_cell_len(g, t->pos, c);
        uint64_t coff = c * (uint64_t)g->cell;
        (void)pace_take_capped(PACE_RECV, clen, t->margin, SFILL_PACE_CAP_NS);
        int stc = sf->ops.read_cell(sf->ops.arg, t->prev_node, t->volume, vol_epoch, t->vuuid, obj->oid, t->pos,
                                    coff, buf, (size_t)clen);
        if (stc != CHUNK_ST_OK) {
            res = SFILL_OBJ_FAILED;
            break;
        }
        SF_STATN(sf, bytes_read, clen);
        uint64_t want = obj->sums[base + c];
        if (xxh64(buf, (size_t)clen, 0) != want) {
            SF_STAT(sf, survivor_bad);
            res = SFILL_OBJ_CORRUPT;
            break;
        }
        if (chunk_wr_cell(w, buf, (size_t)clen, true, want) != 0) {
            res = SFILL_OBJ_FAILED;
            break;
        }
        total_written += clen;
    }
    free(buf);

    if (res != SFILL_OBJ_OK) {
        chunk_wr_abort(w);
        return res;
    }
    if (chunk_wr_commit(w) != 0)
        return SFILL_OBJ_FAILED;
    if (!verify_written_chunk(st, id, obj, false)) {
        char hex[33];
        chunk_oid_hex(obj->oid, hex);
        fprintf(stderr, "tsdbd: sfill: volume %" PRIu32 " pos %" PRIu32 " object %s: copied chunk failed read-back\n",
                t->volume, t->pos, hex);
        return SFILL_OBJ_FAILED;
    }
    SF_STATN(sf, bytes_written, total_written);
    SF_STAT(sf, copied);
    return SFILL_OBJ_OK;
}

/* ------------------------------------------------------------------ RS decode from k survivors */

static int decode_from_survivors(struct sfill *sf, struct sfill_task *t, chunk_backend *be,
                                 const chunk_ident *id, uint64_t vol_epoch,
                                 const uint32_t stripe[CHUNK_POS_MAX], unsigned nstripe)
{
#ifndef TSDB_EC
    (void)sf;
    (void)t;
    (void)be;
    (void)id;
    (void)vol_epoch;
    (void)stripe;
    (void)nstripe;
    return SFILL_OBJ_FAILED;
#else
    const struct sfill_obj *obj = &t->obj;
    const chunk_geom *g = &obj->g;
    unsigned k = g->k, m = g->m, npos = chunk_npos(g);
    if (m == 0 || npos > nstripe || t->pos >= npos)
        return SFILL_OBJ_CORRUPT;

    int mut = atomic_load_explicit(&sfill_mutant, memory_order_relaxed);
    iso_rs *rs = chunk_rs_get(k, m);
    if (!rs)
        return SFILL_OBJ_FAILED;

    uint64_t target_cells = chunk_pos_cells(g, t->pos);
    uint64_t target_base = chunk_sums_base(g, t->pos);
    uint64_t total_data_cells = chunk_ncells(g);
    if (target_cells == 0 || target_base + target_cells > obj->nsums)
        return SFILL_OBJ_CORRUPT;

    chunk_store *st = NULL;
    chunk_wr *w = NULL;
    int orc = open_local_writer(be, id, obj, vol_epoch, &st, &w);
    if (orc != 0)
        return SFILL_OBJ_UNAVAILABLE;

    /* Allocate one cell-sized shard buffer per position (reused across all stripes of this object). */
    size_t cell_cap = g->cell ? (size_t)g->cell : 1;
    uint8_t *shards[CHUNK_POS_MAX] = {NULL};
    for (unsigned p = 0; p < npos; p++) {
        shards[p] = malloc(cell_cap);
        if (!shards[p]) {
            for (unsigned j = 0; j < p; j++)
                free(shards[j]);
            chunk_wr_abort(w);
            return SFILL_OBJ_FAILED;
        }
    }

    iso_rs_plan *plan = NULL;
    uint32_t plan_mask = 0;
    bool data_only = t->pos < k;
    bool bypassed_rec_sums = (mut == SFILL_MUT_NOCHECK);
    int res = SFILL_OBJ_OK;
    uint64_t total_written = 0;

    for (uint64_t s = 0; s < target_cells; s++) {
        if (task_cancelled(sf, t)) {
            res = SFILL_OBJ_CANCELLED;
            break;
        }
        uint64_t Ls = chunk_stripe_len(g, s);
        bool present[CHUNK_POS_MAX];
        int sts[CHUNK_POS_MAX];
        memset(present, 0, sizeof present);
        memset(sts, 0, sizeof sts);
        unsigned ngood = 0;
        uint32_t pmask = 0;

        for (unsigned p = 0; p < npos && ngood < k; p++) {
            if (p == t->pos)
                continue;
            if (!pos_written(obj->positions, p)) {
                sts[p] = CHUNK_ST_UNWRITTEN;
                continue;
            }
            uint64_t cidx = p < k ? s * (uint64_t)k + p : 0;
            if (p < k && cidx >= total_data_cells) {
                /* Virtual zero cell beyond end of object (E7): no network read required. */
                memset(shards[p], 0, (size_t)Ls);
                present[p] = true;
                pmask |= 1u << p;
                ngood++;
                continue;
            }
            uint64_t clen = chunk_pos_cell_len(g, p, s);
            uint64_t coff = s * (uint64_t)g->cell;
            (void)pace_take_capped(PACE_RECV, clen, t->margin, SFILL_PACE_CAP_NS);
            int r = sf->ops.read_cell(sf->ops.arg, stripe[p], t->volume, vol_epoch, t->vuuid, obj->oid, p,
                                      coff, shards[p], (size_t)clen);
            if (r != CHUNK_ST_OK) {
                sts[p] = r;
                continue;
            }
            SF_STATN(sf, bytes_read, clen);

            uint64_t sbase = chunk_sums_base(g, p);
            if (sbase + s >= obj->nsums) {
                sts[p] = CHUNK_ST_DAMAGED;
                continue;
            }
            uint64_t want_s = obj->sums[sbase + s];
            if (mut != SFILL_MUT_SURVIVOR_TRUST && xxh64(shards[p], (size_t)clen, 0) != want_s) {
                SF_STAT(sf, survivor_bad);
                sts[p] = CHUNK_ST_DAMAGED;
                continue;
            }
            if (clen < Ls)
                memset(shards[p] + clen, 0, (size_t)(Ls - clen));
            present[p] = true;
            pmask |= 1u << p;
            ngood++;
        }

        if (ngood < k) {
            if (mut == SFILL_MUT_FEWERK && ngood > 0) {
                /* Planted mutant `fillfewerk`: fabricate zeros for missing survivors to reach k and decode anyway. */
                for (unsigned p = 0; p < npos && ngood < k; p++) {
                    if (p == t->pos || present[p])
                        continue;
                    memset(shards[p], 0, (size_t)Ls);
                    present[p] = true;
                    pmask |= 1u << p;
                    ngood++;
                }
                bypassed_rec_sums = true;
            } else {
                unsigned ndown = 0;
                for (unsigned p = 0; p < npos; p++)
                    if (p != t->pos && sts[p] == CHUNK_ST_DOWN)
                        ndown++;
                char hex[33];
                chunk_oid_hex(obj->oid, hex);
                bool away = ngood + ndown >= k;
                fprintf(stderr,
                        "tsdbd: sfill: volume %" PRIu32 " pos %" PRIu32 " object %s stripe %" PRIu64
                        ": only %u of %u good survivors (%u down): %s\n",
                        t->volume, t->pos, hex, s, ngood, k, ndown, away ? "unavailable" : "corrupt");
                res = away ? SFILL_OBJ_UNAVAILABLE : SFILL_OBJ_CORRUPT;
                break;
            }
        }

        if (!plan || plan_mask != pmask) {
            iso_rs_plan_free(plan);
            plan = iso_rs_plan_new(rs, present, data_only);
            if (!plan) {
                res = SFILL_OBJ_FAILED;
                break;
            }
            plan_mask = pmask;
        }
        iso_rs_plan_apply(plan, shards, (size_t)Ls);

        uint64_t out_len = chunk_pos_cell_len(g, t->pos, s);
        uint64_t want_out = obj->sums[target_base + s];
        uint64_t got_out = xxh64(shards[t->pos], (size_t)out_len, 0);
        if (!bypassed_rec_sums && got_out != want_out) {
            char hex[33];
            chunk_oid_hex(obj->oid, hex);
            fprintf(stderr,
                    "tsdbd: sfill: volume %" PRIu32 " pos %" PRIu32 " object %s stripe %" PRIu64
                    ": rebuilt cell sum mismatch (got %016" PRIx64 ", want %016" PRIx64 "): refusing to write\n",
                    t->volume, t->pos, hex, s, got_out, want_out);
            res = SFILL_OBJ_CORRUPT;
            break;
        }

        int wrc = bypassed_rec_sums ? chunk_wr_cell_trusted(w, shards[t->pos], (size_t)out_len, got_out)
                                    : chunk_wr_cell(w, shards[t->pos], (size_t)out_len, true, want_out);
        if (wrc != 0) {
            res = SFILL_OBJ_FAILED;
            break;
        }
        total_written += out_len;
    }

    iso_rs_plan_free(plan);
    for (unsigned p = 0; p < npos; p++)
        free(shards[p]);

    if (res != SFILL_OBJ_OK) {
        chunk_wr_abort(w);
        return res;
    }
    if (chunk_wr_commit(w) != 0)
        return SFILL_OBJ_FAILED;
    if (!verify_written_chunk(st, id, obj, bypassed_rec_sums)) {
        char hex[33];
        chunk_oid_hex(obj->oid, hex);
        fprintf(stderr, "tsdbd: sfill: volume %" PRIu32 " pos %" PRIu32 " object %s: rebuilt chunk failed read-back\n",
                t->volume, t->pos, hex);
        return SFILL_OBJ_FAILED;
    }
    SF_STATN(sf, bytes_written, total_written);
    SF_STAT(sf, decoded);
    return SFILL_OBJ_OK;
#endif
}

/* Execute one object fill task. */
static int run_object_task(struct sfill *sf, struct sfill_task *t)
{
    if (task_cancelled(sf, t))
        return SFILL_OBJ_CANCELLED;

#ifdef TSDB_TEST_HOOKS
    {
        int pa = atomic_load_explicit(&sfill_test_pause_after, memory_order_relaxed);
        if (pa >= 0 && sfill_test_pause_path[0]) {
            int idx = atomic_fetch_add_explicit(&sfill_test_pause_seen, 1, memory_order_relaxed);
            if (idx >= pa) {
                while (access(sfill_test_pause_path, F_OK) == 0 && !job_cancelled(sf, t->job, t->gen)) {
                    struct timespec ts = {0, 50 * 1000000L};
                    nanosleep(&ts, NULL);
                }
                if (job_cancelled(sf, t->job, t->gen))
                    return SFILL_OBJ_CANCELLED;
            }
        }
        int dms = atomic_load_explicit(&sfill_test_obj_delay_ms, memory_order_relaxed);
        if (dms > 0) {
            struct timespec ts = {dms / 1000, (long)(dms % 1000) * 1000000L};
            nanosleep(&ts, NULL);
            if (job_cancelled(sf, t->job, t->gen))
                return SFILL_OBJ_CANCELLED;
        }
    }
#endif

    const struct sfill_obj *obj = &t->obj;
    unsigned npos = chunk_npos(&obj->g);
    if (t->pos >= npos) {
        /* E7: small object with k' < k has no chunk at this position. */
        SF_STAT(sf, skipped_small);
        return SFILL_OBJ_OK;
    }

    uint64_t vol_epoch = 0;
    uint32_t stripe[CHUNK_POS_MAX] = {0};
    unsigned nstripe = 0;
    bool prev_up = false;
    if (sf->ops.vol_view(sf->ops.arg, t->volume, &vol_epoch, stripe, &nstripe, &prev_up, t->prev_node) != 0)
        return SFILL_OBJ_UNAVAILABLE;

    chunk_backend *be = sf->ops.local_be_hold ? sf->ops.local_be_hold(sf->ops.arg) : NULL;
    if (!be)
        return SFILL_OBJ_UNAVAILABLE;

    chunk_ident id = {.volume = t->volume, .pos = (uint16_t)t->pos};
    memcpy(id.vuuid, t->vuuid, 16);
    memcpy(id.oid, obj->oid, 16);

    /* Fast path (and crash-resume path after CP_FILL_WRITTEN): already durably held and sum-verified locally.
     * Notify log replicas on pass 0 only (covers crash-resume after CP_FILL_WRITTEN without spamming retries). */
    if (!t->force_write && local_chunk_intact(be, &id, obj)) {
        sf->ops.local_be_release(sf->ops.arg, be);
        SF_STAT(sf, already_held);
        if (t->first_pass && sf->ops.notify_reopen) {
            sf->ops.notify_reopen(sf->ops.arg, t->volume, obj->oid);
            SF_STAT(sf, reopened);
        }
        return SFILL_OBJ_OK;
    }

    int res = SFILL_OBJ_FAILED;
    /* E13: copy from prev (A) at 1x bandwidth when A is UP and held this position (passing all encode-time sums). */
    if (t->prev_node != 0 && prev_up && pos_written(obj->positions, t->pos))
        res = try_copy_from_prev(sf, t, be, &id, vol_epoch);

    /* Otherwise decode whole cells from k other written positions over PL_KIND_CHUNKR. */
    if (res != SFILL_OBJ_OK && res != SFILL_OBJ_CANCELLED)
        res = decode_from_survivors(sf, t, be, &id, vol_epoch, stripe, nstripe);

    sf->ops.local_be_release(sf->ops.arg, be);

    if (res == SFILL_OBJ_OK) {
        /* R9 crash point: chunk is durable and read-back verified on disk, before reporting progress/whole. */
        crash_point(CP_FILL_WRITTEN);
        if (sf->ops.notify_reopen) {
            sf->ops.notify_reopen(sf->ops.arg, t->volume, obj->oid);
            SF_STAT(sf, reopened);
        }
    } else if (res == SFILL_OBJ_UNAVAILABLE) {
        SF_STAT(sf, unavailable);
    } else if (res == SFILL_OBJ_CORRUPT) {
        SF_STAT(sf, corrupt);
    } else if (res == SFILL_OBJ_FAILED) {
        SF_STAT(sf, failed);
    }
    return res;
}

/* ------------------------------------------------------------------ worker & coordinator threads */

static inline unsigned margin_bucket(int margin)
{
    if (margin == PACE_MARGIN_BALANCE)
        return 3;
    if (margin <= 0)
        return 0;
    if (margin == 1)
        return 1;
    return 2;
}

static inline bool job_more_urgent(const struct sfill_job *a, const struct sfill_job *b)
{
    unsigned ba = margin_bucket(a->cfg.margin);
    unsigned bb = margin_bucket(b->cfg.margin);
    if (ba != bb)
        return ba < bb;
    return a->cfg.margin < b->cfg.margin;
}

/* Append task to its priority bucket in O(1) (sf->mu held). */
static void push_task_locked(struct sfill *sf, struct sfill_task *t)
{
    unsigned b = margin_bucket(t->margin);
    t->next = NULL;
    if (sf->q_tail[b])
        sf->q_tail[b]->next = t;
    else
        sf->q_head[b] = t;
    sf->q_tail[b] = t;
}

/* Pop the lowest-margin queued task in O(1) (sf->mu held). */
static struct sfill_task *pop_task_locked(struct sfill *sf)
{
    for (unsigned b = 0; b < SFILL_BUCKETS; b++) {
        struct sfill_task *t = sf->q_head[b];
        if (!t)
            continue;
        sf->q_head[b] = t->next;
        if (!sf->q_head[b])
            sf->q_tail[b] = NULL;
        t->next = NULL;
        return t;
    }
    return NULL;
}

/* Drop all unstarted queued tasks matching `job` (or all removed jobs if job == NULL; sf->mu held).
 * Ad-hoc tasks dropped on shutdown (`drop_all`) are appended to `*dropped_adhoc` so callers invoke done_cb outside mu. */
static void drop_tasks_locked(struct sfill *sf, struct sfill_job *job, bool drop_all,
                              struct sfill_task **dropped_adhoc)
{
    for (unsigned b = 0; b < SFILL_BUCKETS; b++) {
        struct sfill_task *prev = NULL;
        struct sfill_task *cur = sf->q_head[b];
        while (cur) {
            struct sfill_task *nx = cur->next;
            bool match = drop_all || (cur->job && (job ? cur->job == job : cur->job->removed));
            if (match) {
                if (prev)
                    prev->next = nx;
                else
                    sf->q_head[b] = nx;
                if (cur->job) {
                    if (cur->job->inflight > 0)
                        cur->job->inflight--;
                    if (cur->job->round_pending > 0)
                        cur->job->round_pending--;
                    task_free(cur);
                } else if (dropped_adhoc) {
                    cur->next = *dropped_adhoc;
                    *dropped_adhoc = cur;
                } else {
                    task_free(cur);
                }
            } else {
                prev = cur;
            }
            cur = nx;
        }
        sf->q_tail[b] = prev;
    }
}

/* Move queued tasks of `job` when its margin changes (sf->mu held). */
static void rebucket_job_tasks_locked(struct sfill *sf, struct sfill_job *job, int old_margin, int new_margin)
{
    unsigned ob = margin_bucket(old_margin);
    unsigned nb = margin_bucket(new_margin);
    if (ob == nb) {
        for (struct sfill_task *t = sf->q_head[ob]; t; t = t->next)
            if (t->job == job)
                t->margin = new_margin;
        return;
    }
    struct sfill_task *prev = NULL;
    struct sfill_task *cur = sf->q_head[ob];
    while (cur) {
        struct sfill_task *nx = cur->next;
        if (cur->job == job) {
            if (prev)
                prev->next = nx;
            else
                sf->q_head[ob] = nx;
            cur->margin = new_margin;
            push_task_locked(sf, cur);
        } else {
            prev = cur;
        }
        cur = nx;
    }
    sf->q_tail[ob] = prev;
}

static void gc_removed_jobs_locked(struct sfill *sf)
{
    for (struct sfill_job **pp = &sf->jobs; *pp;) {
        struct sfill_job *j = *pp;
        if (j->removed && j->inflight == 0 && !j->listing) {
            *pp = j->next;
            if (j->pace_id) {
                pace_job_end_ex(PACE_RECV, j->pace_id);
                j->pace_id = 0;
            }
            free(j);
        } else {
            pp = &j->next;
        }
    }
}

/* Complete a pass once its last page has drained (sf->mu held). Returns true if reports should be published. */
static bool finish_pass_locked(struct sfill *sf, struct sfill_job *j)
{
    j->pass_active = false;
    if (j->pace_id) {
        pace_job_end_ex(PACE_RECV, j->pace_id);
        j->pace_id = 0;
    }
    if (j->round_errors == 0 && j->done == j->total) {
        j->whole = 1;
        fprintf(stderr,
                "tsdbd: sfill: volume %" PRIu32 " pos %" PRIu32 " version %" PRIu64
                ": whole (%" PRIu64 "/%" PRIu64 " objects verified)\n",
                j->cfg.volume, j->cfg.pos, j->cfg.stripe_version, j->done, j->total);
        return true;
    }
    j->pass_num++;
    j->retry_at_ms = mono_ms() + (uint64_t)retry_delay_ms();
    pthread_cond_signal(&sf->coord_cv);
    return false;
}

static void *worker_main(void *arg)
{
    struct sfill *sf = arg;
    pthread_mutex_lock(&sf->mu);
    for (;;) {
        struct sfill_task *t = NULL;
        while (!sf->stopping && (t = pop_task_locked(sf)) == NULL)
            pthread_cond_wait(&sf->work_cv, &sf->mu);
        if (!t && sf->stopping)
            break;
        pthread_mutex_unlock(&sf->mu);

        int rc = run_object_task(sf, t);

        if (!t->job) {
            if (t->done_cb)
                t->done_cb(t->done_arg, rc == SFILL_OBJ_OK ? 0 : -1);
            task_free(t);
            pthread_mutex_lock(&sf->mu);
            continue;
        }

        bool should_publish = false;
        pthread_mutex_lock(&sf->mu);
        struct sfill_job *j = t->job;
        if (j->inflight > 0)
            j->inflight--;
        if (!j->removed && j->pass_active && j->pass_gen == t->gen && j->gen == t->gen) {
            if (rc == SFILL_OBJ_OK) {
                j->done++;
                j->bytes_done += t->obj_bytes;
                j->progress_ms = mono_ms();
                if (atomic_load_explicit(&sfill_mutant, memory_order_relaxed) == SFILL_MUT_WHOLE_EARLY)
                    j->whole = 1;
                should_publish = true;
            } else if (rc != SFILL_OBJ_CANCELLED) {
                j->round_errors++;
                j->failures++;
            }
            if (j->round_pending > 0)
                j->round_pending--;
            if (j->round_pending == 0) {
                if (j->pass_more) {
                    pthread_cond_signal(&sf->coord_cv);
                } else {
                    if (finish_pass_locked(sf, j))
                        should_publish = true;
                }
            }
        }
        if (j->inflight == 0 && !j->removed && !j->whole && !j->pass_active)
            pthread_cond_signal(&sf->coord_cv);
        gc_removed_jobs_locked(sf);
        pthread_mutex_unlock(&sf->mu);

        task_free(t);
        if (should_publish)
            publish_reports(sf);

        pthread_mutex_lock(&sf->mu);
    }
    pthread_mutex_unlock(&sf->mu);
    return NULL;
}

static void ensure_workers_locked(struct sfill *sf)
{
    uint32_t want = sf->want_workers;
    if (want == 0)
        want = SFILL_DEFAULT_CONCURRENCY;
    if (want > SFILL_MAX_WORKERS)
        want = SFILL_MAX_WORKERS;
    while (sf->nworkers < want && !sf->stopping) {
        pthread_attr_t at;
        pthread_attr_init(&at);
#ifdef __APPLE__
        pthread_attr_setstacksize(&at, TSDB_THREAD_STACK);
#endif
        int e = pthread_create(&sf->workers[sf->nworkers], &at, worker_main, sf);
        pthread_attr_destroy(&at);
        if (e != 0)
            break;
        sf->nworkers++;
    }
}

static void *coord_main(void *arg)
{
    struct sfill *sf = arg;
    pthread_mutex_lock(&sf->mu);
    while (!sf->stopping) {
        gc_removed_jobs_locked(sf);
        uint64_t now = mono_ms();
        uint64_t earliest_retry = UINT64_MAX;
        struct sfill_job *pick = NULL;

        /* Pick the most-urgent job ready for its next OBJLIST page (streaming page-by-page). */
        for (struct sfill_job *j = sf->jobs; j; j = j->next) {
            if (j->removed || j->whole || j->listing || j->round_pending > 0)
                continue;
            if (!j->pass_active) {
                if (j->inflight > 0)
                    continue;
                if (j->retry_at_ms > now) {
                    if (j->retry_at_ms < earliest_retry)
                        earliest_retry = j->retry_at_ms;
                    continue;
                }
            } else if (!j->pass_more) {
                continue;
            }
            if (!pick || job_more_urgent(j, pick))
                pick = j;
        }

        if (!pick) {
            if (earliest_retry == UINT64_MAX) {
                pthread_cond_wait(&sf->coord_cv, &sf->mu);
            } else {
                uint64_t wait_ms = earliest_retry > now ? earliest_retry - now : 1;
                struct timespec ts;
                tsdb_cond_deadline_ns(&ts, (int64_t)(wait_ms * 1000000ull));
                pthread_cond_timedwait(&sf->coord_cv, &sf->mu, &ts);
            }
            continue;
        }

        if (!pick->pass_active) {
            pick->pass_active = true;
            pick->pass_gen = pick->gen;
            pick->pass_has_cursor = false;
            memset(pick->pass_cursor, 0, 16);
            pick->pass_more = true;
            pick->has_vuuid = false;
            pick->done = 0;
            pick->total = 0;
            pick->bytes_done = 0;
            pick->bytes_total = 0;
            pick->round_errors = 0;
            if (pick->pace_id)
                pace_job_end_ex(PACE_RECV, pick->pace_id);
            pick->pace_id = pace_job_begin_ex(PACE_RECV, pick->cfg.margin, 0);
        }

        pick->listing = true;
        uint64_t gen = pick->gen;
        uint32_t vol = pick->cfg.volume;
        uint32_t pos = pick->cfg.pos;
        uint64_t sver = pick->cfg.stripe_version;
        uint32_t prev = pick->cfg.prev_node;
        int margin = pick->cfg.margin;
        bool has_cursor = pick->pass_has_cursor;
        uint8_t cursor[16];
        memcpy(cursor, pick->pass_cursor, 16);
        bool first_pass = (pick->pass_num == 0);
        uint32_t page = objlist_page_size();
        pthread_mutex_unlock(&sf->mu);

        uint8_t vu[16] = {0};
        struct sfill_obj *items = NULL;
        uint32_t got = 0;
        bool more = false;
        int lrc = sf->ops.objlist(sf->ops.arg, vol, sver, has_cursor, cursor, page, vu, &items, &got, &more);

        bool should_publish = false;
        pthread_mutex_lock(&sf->mu);
        pick->listing = false;
        if (sf->stopping || pick->removed || pick->gen != gen || !pick->pass_active || pick->pass_gen != gen) {
            sfill_objs_free(items, got);
            gc_removed_jobs_locked(sf);
            continue;
        }
        if (lrc != 0 || (got == 0 && more)) {
            sfill_objs_free(items, got);
            pick->pass_active = false;
            if (pick->pace_id) {
                pace_job_end_ex(PACE_RECV, pick->pace_id);
                pick->pace_id = 0;
            }
            pick->failures++;
            /* Keep pass_num unchanged on an OBJLIST failure so a post-crash restart still notifies reopen on pass 0
             * once the leader's OBJLIST becomes reachable. */
            pick->retry_at_ms = mono_ms() + (uint64_t)retry_delay_ms();
            continue;
        }
        if (!pick->has_vuuid) {
            memcpy(pick->vuuid, vu, 16);
            pick->has_vuuid = true;
        }
        pick->pass_more = more;
        if (got == 0) {
            free(items);
            if (finish_pass_locked(sf, pick))
                should_publish = true;
        } else {
            memcpy(pick->pass_cursor, items[got - 1].oid, 16);
            pick->pass_has_cursor = true;
            pick->total += got;
            ensure_workers_locked(sf);
            bool oom = false;
            for (uint32_t i = 0; i < got; i++) {
                struct sfill_task *t = calloc(1, sizeof *t);
                if (!t) {
                    oom = true;
                    break;
                }
                t->job = pick;
                t->gen = gen;
                t->volume = vol;
                t->pos = pos;
                t->prev_node = prev;
                t->margin = margin;
                t->first_pass = first_pass;
                memcpy(t->vuuid, pick->vuuid, 16);
                t->obj_bytes = chunk_geom_valid(&items[i].g) ? chunk_pos_len(&items[i].g, pos) : 0;
                pick->bytes_total += t->obj_bytes;
                t->obj = items[i];
                items[i].sums = NULL; /* transferred ownership */
                push_task_locked(sf, t);
                pick->round_pending++;
                pick->inflight++;
            }
            sfill_objs_free(items, got);
            if (oom) {
                pick->round_errors++;
                if (pick->round_pending == 0)
                    (void)finish_pass_locked(sf, pick);
            }
            if (pick->round_pending > 0) {
                if (atomic_load_explicit(&sfill_mutant, memory_order_relaxed) == SFILL_MUT_WHOLE_EARLY)
                    pick->whole = 1;
                should_publish = true;
                pthread_cond_broadcast(&sf->work_cv);
            }
        }
        pthread_mutex_unlock(&sf->mu);

        if (should_publish)
            publish_reports(sf);

        pthread_mutex_lock(&sf->mu);
    }
    gc_removed_jobs_locked(sf);
    pthread_mutex_unlock(&sf->mu);
    return NULL;
}

/* ------------------------------------------------------------------ public API */

static void load_env_mutant_once(void)
{
#ifdef TSDB_TEST_HOOKS
    const char *mu = getenv("TSDB_SFILL_MUTANT");
    if (mu && *mu) {
        int m = sfill_mutant_parse(mu);
        if (m > 0) {
            atomic_store_explicit(&sfill_mutant, m, memory_order_relaxed);
            fprintf(stderr, "tsdbd: sfill MUTANT %s (tests only)\n", mu);
        }
    }
    const char *th = getenv("TSDB_SFILL_TEST");
    if (th && *th) {
        char buf[1024];
        snprintf(buf, sizeof buf, "%s", th);
        char *sp = NULL;
        for (char *tok = strtok_r(buf, ",", &sp); tok; tok = strtok_r(NULL, ",", &sp)) {
            if (strncmp(tok, "obj_delay_ms=", 13) == 0) {
                atomic_store_explicit(&sfill_test_obj_delay_ms, atoi(tok + 13), memory_order_relaxed);
            } else if (strncmp(tok, "pause_after=", 12) == 0) {
                const char *col = strchr(tok + 12, ':');
                if (col && col[1]) {
                    atomic_store_explicit(&sfill_test_pause_after, atoi(tok + 12), memory_order_relaxed);
                    snprintf(sfill_test_pause_path, sizeof sfill_test_pause_path, "%s", col + 1);
                }
            }
        }
    }
#endif
}

struct sfill *sfill_new_with_ops(const struct sfill_ops *ops)
{
    static pthread_once_t once = PTHREAD_ONCE_INIT;
    pthread_once(&once, load_env_mutant_once);

    struct sfill *sf = calloc(1, sizeof *sf);
    if (!sf)
        return NULL;
    pthread_mutex_init(&sf->pub_mu, NULL);
    pthread_mutex_init(&sf->mu, NULL);
    tsdb_cond_init_mono(&sf->coord_cv);
    tsdb_cond_init_mono(&sf->work_cv);
    sf->want_workers = SFILL_DEFAULT_CONCURRENCY;
    if (ops) {
        sf->ops = *ops;
        sf->has_ops = ops->objlist && ops->read_cell && ops->vol_view && ops->local_be_hold && ops->local_be_release;
    }
    if (sf->has_ops) {
        if (pthread_create(&sf->coord_thr, NULL, coord_main, sf) != 0) {
            pthread_cond_destroy(&sf->work_cv);
            pthread_cond_destroy(&sf->coord_cv);
            pthread_mutex_destroy(&sf->mu);
            pthread_mutex_destroy(&sf->pub_mu);
            free(sf);
            return NULL;
        }
        sf->coord_started = true;
    }
    return sf;
}

struct sfill *sfill_new(void)
{
    return sfill_new_with_ops(NULL);
}

void sfill_free(struct sfill *sf)
{
    if (!sf)
        return;
    pthread_mutex_lock(&sf->mu);
    sf->stopping = true;
    /* Drop any queued (not yet started) tasks so workers exit promptly. */
    struct sfill_task *dropped = NULL;
    drop_tasks_locked(sf, NULL, true, &dropped);
    pthread_cond_broadcast(&sf->coord_cv);
    pthread_cond_broadcast(&sf->work_cv);
    uint32_t nw = sf->nworkers;
    bool cs = sf->coord_started;
    pthread_mutex_unlock(&sf->mu);

    for (struct sfill_task *t = dropped, *nx; t; t = nx) {
        nx = t->next;
        if (t->done_cb)
            t->done_cb(t->done_arg, -1);
        task_free(t);
    }

    if (cs)
        pthread_join(sf->coord_thr, NULL);
    for (uint32_t i = 0; i < nw; i++)
        pthread_join(sf->workers[i], NULL);

    for (struct sfill_job *j = sf->jobs, *nx; j; j = nx) {
        nx = j->next;
        if (j->pace_id)
            pace_job_end_ex(PACE_RECV, j->pace_id);
        free(j);
    }
    pthread_cond_destroy(&sf->work_cv);
    pthread_cond_destroy(&sf->coord_cv);
    pthread_mutex_destroy(&sf->mu);
    pthread_mutex_destroy(&sf->pub_mu);
    free(sf);
}

int sfill_start(struct sfill *sf, const struct sfill_job_cfg *cfg)
{
    if (!sf || !cfg || cfg->pos >= CHUNK_POS_MAX)
        return -1;
    uint32_t conc = cfg->concurrency ? cfg->concurrency : SFILL_DEFAULT_CONCURRENCY;
    if (conc > SFILL_MAX_WORKERS)
        conc = SFILL_MAX_WORKERS;

    bool changed = false;
    pthread_mutex_lock(&sf->mu);
    if (sf->stopping) {
        pthread_mutex_unlock(&sf->mu);
        return -1;
    }
    if (conc > sf->want_workers)
        sf->want_workers = conc;

    struct sfill_job *found = NULL;
    for (struct sfill_job *j = sf->jobs; j; j = j->next) {
        if (!j->removed && j->cfg.volume == cfg->volume && j->cfg.pos == cfg->pos) {
            found = j;
            break;
        }
    }
    if (found) {
        if (found->cfg.stripe_version != cfg->stripe_version) {
            found->cfg = *cfg;
            found->gen++;
            found->done = 0;
            found->total = 0;
            found->bytes_done = 0;
            found->bytes_total = 0;
            found->start_ms = found->progress_ms = mono_ms();
            found->whole = 0;
            found->pass_active = false;
            found->pass_num = 0;
            found->round_pending = 0;
            found->round_errors = 0;
            found->retry_at_ms = 0;
            if (found->pace_id) {
                pace_job_end_ex(PACE_RECV, found->pace_id);
                found->pace_id = 0;
            }
            /* Drop unstarted queued tasks from the old stripe_version; in-flight workers still drain via inflight. */
            drop_tasks_locked(sf, found, false, NULL);
            changed = true;
        } else {
            found->cfg.leader = cfg->leader;
            found->cfg.prev_node = cfg->prev_node;
            found->cfg.concurrency = cfg->concurrency;
            if (found->cfg.margin != cfg->margin) {
                int old_m = found->cfg.margin;
                found->cfg.margin = cfg->margin;
                rebucket_job_tasks_locked(sf, found, old_m, cfg->margin);
                if (found->pace_id)
                    pace_job_update_ex(PACE_RECV, found->pace_id, cfg->margin);
            }
        }
    } else {
        struct sfill_job *j = calloc(1, sizeof *j);
        if (!j) {
            pthread_mutex_unlock(&sf->mu);
            return -1;
        }
        j->cfg = *cfg;
        j->gen = 1;
        j->start_ms = j->progress_ms = mono_ms();
        j->next = sf->jobs;
        sf->jobs = j;
        changed = true;
    }
    if (changed)
        pthread_cond_signal(&sf->coord_cv);
    pthread_mutex_unlock(&sf->mu);

    if (changed)
        publish_reports(sf);
    return 0;
}

void sfill_stop(struct sfill *sf, uint32_t volume, uint32_t pos)
{
    if (!sf)
        return;
    bool removed_any = false;
    pthread_mutex_lock(&sf->mu);
    for (struct sfill_job *j = sf->jobs; j; j = j->next) {
        if (!j->removed && j->cfg.volume == volume && j->cfg.pos == pos) {
            j->removed = true;
            j->gen++;
            j->pass_active = false;
            if (j->pace_id) {
                pace_job_end_ex(PACE_RECV, j->pace_id);
                j->pace_id = 0;
            }
            removed_any = true;
        }
    }
    if (removed_any) {
        /* Drop any queued (unstarted) tasks for removed jobs. */
        drop_tasks_locked(sf, NULL, false, NULL);
        gc_removed_jobs_locked(sf);
        pthread_cond_signal(&sf->coord_cv);
    }
    pthread_mutex_unlock(&sf->mu);

    if (removed_any)
        publish_reports(sf);
}

int sfill_enqueue_obj(struct sfill *sf, uint32_t volume, uint32_t pos, uint32_t prev_node, int margin,
                      uint32_t concurrency, const uint8_t vuuid[16], struct sfill_obj *obj, bool force_write,
                      bool (*cancel_cb)(void *arg), void (*done_cb)(void *arg, int rc), void *done_arg)
{
    if (!sf || !sf->has_ops || !vuuid || !obj || pos >= CHUNK_POS_MAX)
        return -1;
    uint32_t conc = concurrency ? concurrency : SFILL_DEFAULT_CONCURRENCY;
    if (conc > SFILL_MAX_WORKERS)
        conc = SFILL_MAX_WORKERS;
    struct sfill_task *t = calloc(1, sizeof *t);
    if (!t)
        return -1;
    t->volume = volume;
    t->pos = pos;
    t->prev_node = prev_node;
    t->margin = margin;
    t->first_pass = true;
    t->force_write = force_write;
    t->cancel_cb = cancel_cb;
    t->done_cb = done_cb;
    t->done_arg = done_arg;
    memcpy(t->vuuid, vuuid, 16);
    t->obj = *obj;

    pthread_mutex_lock(&sf->mu);
    if (sf->stopping) {
        pthread_mutex_unlock(&sf->mu);
        free(t);
        return -1;
    }
    if (conc > sf->want_workers)
        sf->want_workers = conc;
    ensure_workers_locked(sf);
    obj->sums = NULL; /* transferred ownership */
    push_task_locked(sf, t);
    pthread_cond_signal(&sf->work_cv);
    pthread_mutex_unlock(&sf->mu);
    return 0;
}

uint32_t sfill_report(struct sfill *sf, struct sfill_report *out, uint32_t cap)
{
    if (!sf || !out || cap == 0)
        return 0;
    uint32_t n = 0;
    pthread_mutex_lock(&sf->mu);
    for (struct sfill_job *j = sf->jobs; j && n < cap; j = j->next) {
        if (j->removed)
            continue;
        out[n++] = (struct sfill_report){
            .volume = j->cfg.volume,
            .pos = j->cfg.pos,
            .stripe_version = j->cfg.stripe_version,
            .done = j->done,
            .total = j->total,
            .whole = j->whole,
            .failures = j->failures,
        };
    }
    pthread_mutex_unlock(&sf->mu);
    return n;
}

uint32_t sfill_get_progress(struct sfill *sf, struct sfill_progress *out, uint32_t cap)
{
    if (!sf || !out || cap == 0)
        return 0;
    uint64_t mono_now = mono_ms(), wall_now = wall_ms_now();
    uint32_t n = 0;
    pthread_mutex_lock(&sf->mu);
    for (struct sfill_job *j = sf->jobs; j && n < cap; j = j->next) {
        if (j->removed || j->whole)
            continue;
        uint64_t done = j->bytes_done;
        if (j->bytes_total > 0 && done > j->bytes_total)
            done = j->bytes_total;
        out[n++] = (struct sfill_progress){
            .volume = j->cfg.volume,
            .pos = j->cfg.pos,
            .bytes_done = done,
            .bytes_total = j->bytes_total,
            .started_ms = mono_to_wall_ms(j->start_ms, mono_now, wall_now),
            .last_progress_ms = mono_to_wall_ms(j->progress_ms, mono_now, wall_now),
        };
    }
    pthread_mutex_unlock(&sf->mu);
    return n;
}

void sfill_get_stats(struct sfill *sf, struct sfill_stats *out)
{
    if (!sf || !out)
        return;
    out->copied = atomic_load_explicit(&sf->st.copied, memory_order_relaxed);
    out->decoded = atomic_load_explicit(&sf->st.decoded, memory_order_relaxed);
    out->already_held = atomic_load_explicit(&sf->st.already_held, memory_order_relaxed);
    out->skipped_small = atomic_load_explicit(&sf->st.skipped_small, memory_order_relaxed);
    out->failed = atomic_load_explicit(&sf->st.failed, memory_order_relaxed);
    out->unavailable = atomic_load_explicit(&sf->st.unavailable, memory_order_relaxed);
    out->corrupt = atomic_load_explicit(&sf->st.corrupt, memory_order_relaxed);
    out->survivor_bad = atomic_load_explicit(&sf->st.survivor_bad, memory_order_relaxed);
    out->reopened = atomic_load_explicit(&sf->st.reopened, memory_order_relaxed);
    out->bytes_read = atomic_load_explicit(&sf->st.bytes_read, memory_order_relaxed);
    out->bytes_written = atomic_load_explicit(&sf->st.bytes_written, memory_order_relaxed);
}
