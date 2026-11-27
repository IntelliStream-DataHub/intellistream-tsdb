/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Local chunk moves between two stores of one backend (src/chunkmove.h; R13, r13-lmove; E50, §15.3, D18).
 */
#define _GNU_SOURCE
#include "chunkmove.h"

#include "chunk_priv.h"
#include "compat.h"
#include "internal.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define CHUNKMOVE_MAX_RATE_BPS (10ull << 30) /* ceiling avoids uint64 overflow in (bytes % rate) * 1e9 */
#define CHUNKMOVE_BURST_NS 50000000ull       /* 50 ms virtual-clock burst ceiling */
#define CHUNKMOVE_MAX_RETRIES 4

#ifdef TSDB_TEST_HOOKS
int chunkmove_mutant;
static pthread_mutex_t cm_hook_mu = PTHREAD_MUTEX_INITIALIZER;
static chunkmove_hook_fn cm_before_lock_fn;
static void *cm_before_lock_ctx;

int chunkmove_mutant_parse(const char *name)
{
    if (!name || !*name || strcmp(name, "none") == 0)
        return CHUNKMOVE_MUT_NONE;
    if (strcmp(name, "moveunlinkfirst") == 0)
        return CHUNKMOVE_MUT_UNLINKFIRST;
    if (strcmp(name, "movekeepwrong") == 0)
        return CHUNKMOVE_MUT_KEEPWRONG;
    return -1;
}

void chunkmove_set_before_lock_hook(chunkmove_hook_fn fn, void *ctx)
{
    pthread_mutex_lock(&cm_hook_mu);
    cm_before_lock_fn = fn;
    cm_before_lock_ctx = ctx;
    pthread_mutex_unlock(&cm_hook_mu);
}

static void cm_run_before_lock_hook(void)
{
    pthread_mutex_lock(&cm_hook_mu);
    chunkmove_hook_fn fn = cm_before_lock_fn;
    void *ctx = cm_before_lock_ctx;
    pthread_mutex_unlock(&cm_hook_mu);
    if (fn)
        fn(ctx);
}
#endif

static uint64_t cm_mono_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static uint64_t cm_effective_rate(uint64_t rate_bps)
{
    if (rate_bps == UINT64_MAX)
        return UINT64_MAX;
    uint64_t r = rate_bps > 0 ? rate_bps : ((uint64_t)CHUNKMOVE_DEFAULT_MB_S << 20);
    return r > CHUNKMOVE_MAX_RATE_BPS ? CHUNKMOVE_MAX_RATE_BPS : r;
}

void chunk_move_pacer_init(chunk_move_pacer *p, uint64_t rate_bps)
{
    if (!p)
        return;
    p->rate_bps = cm_effective_rate(rate_bps);
    p->next_ns = 0;
    p->init = false;
}

void chunk_move_pacer_set_rate(chunk_move_pacer *p, uint64_t rate_bps)
{
    if (!p)
        return;
    p->rate_bps = cm_effective_rate(rate_bps);
}

/* Advances the virtual-time token bucket by `bytes` and sleeps in <= 10 ms slices so cancellation is prompt. */
static int cm_pace_bytes(chunk_move_pacer *p, uint64_t bytes, int (*cancel)(void *), void *cancel_ctx)
{
    if (!p || p->rate_bps == UINT64_MAX || bytes == 0)
        return 0;
    uint64_t rate = cm_effective_rate(p->rate_bps);
    uint64_t now = cm_mono_ns();
    if (!p->init || now > p->next_ns + CHUNKMOVE_BURST_NS) {
        p->next_ns = now;
        p->init = true;
    }
    uint64_t dur_ns = (bytes / rate) * 1000000000ull + ((bytes % rate) * 1000000000ull) / rate;
    if (dur_ns == 0 && bytes > 0)
        dur_ns = 1;
    p->next_ns += dur_ns;
    while (p->next_ns > now) {
        if (cancel && cancel(cancel_ctx))
            return TSDB_ERR_STOPPED;
        uint64_t rem = p->next_ns - now;
        uint64_t slice = rem > 10000000ull ? 10000000ull : rem;
        struct timespec ts = {(time_t)(slice / 1000000000ull), (long)(slice % 1000000000ull)};
        nanosleep(&ts, NULL);
        now = cm_mono_ns();
    }
    return 0;
}

static bool vuuid_is_zero(const uint8_t v[16])
{
    uint8_t acc = 0;
    for (int i = 0; i < 16; i++)
        acc |= v[i];
    return acc == 0;
}

/* Reads back the committed chunk on `st_to` and verifies header, geometry, sums table, and every cell's xxh64. */
static int verify_target_chunk(chunk_store *st_to, const chunk_ident *id, const chunk_hdr *expect_hdr,
                               const uint64_t *expect_sums, uint64_t ncells)
{
    chunk_rd *trd = NULL;
    int rc = chunk_rd_open(st_to, id, &trd);
    if (rc != 0)
        return rc;
    const chunk_hdr *th = chunk_rd_hdr(trd);
    const uint64_t *tsums = chunk_rd_sums(trd);
    bool match = th->epoch == expect_hdr->epoch && th->pos == expect_hdr->pos &&
                 th->chunk_len == expect_hdr->chunk_len && memcmp(&th->g, &expect_hdr->g, sizeof th->g) == 0 &&
                 memcmp(th->vuuid, expect_hdr->vuuid, 16) == 0 && memcmp(th->oid, expect_hdr->oid, 16) == 0 &&
                 tsums != NULL;
    for (uint64_t i = 0; match && i < ncells; i++) {
        if (tsums[i] != expect_sums[i])
            match = false;
    }
    uint64_t nbad = 0;
    int s_rc = match ? chunk_rd_scrub(trd, &nbad) : TSDB_ERR_CORRUPT;
    chunk_rd_close(trd);
    if (!match)
        return TSDB_ERR_CORRUPT;
    if (s_rc != 0)
        return s_rc;
    return nbad == 0 ? 0 : TSDB_ERR_CORRUPT;
}

int chunk_move(chunk_backend *be, const chunk_ident *id, int from, int to, const chunk_move_opts *o)
{
    if (!be || !id)
        return TSDB_ERR_INVALID;
    if (be->read_only)
        return TSDB_ERR_READONLY;
    int bn = chunk_be_nstores(be);
    if (from < 0 || from >= bn || to < 0 || to >= bn || from == to || id->pos >= CHUNK_POS_MAX)
        return TSDB_ERR_INVALID;
    if (o && o->bytes_moved)
        *o->bytes_moved = 0;

    chunk_move_pacer local_pacer;
    chunk_move_pacer *pacer = NULL;
    if (o && o->pacer) {
        pacer = o->pacer;
    } else {
        chunk_move_pacer_init(&local_pacer, o ? o->rate_bps : 0);
        pacer = &local_pacer;
    }

    int (*cancel_fn)(void *) = o ? o->cancel : NULL;
    void *cancel_ctx = o ? o->cancel_ctx : NULL;

    chunk_store *st_from = chunk_be_store_ref(be, from);
    chunk_store *st_to = chunk_be_store_ref(be, to);
    if (!st_from || !st_to) {
        chunk_store_unref(st_from);
        chunk_store_unref(st_to);
        return TSDB_ERR_UNAVAILABLE;
    }
    if (chunk_store_failed(st_from) || chunk_store_failed(st_to)) {
        chunk_store_unref(st_from);
        chunk_store_unref(st_to);
        return TSDB_ERR_IO;
    }

    int rc = 0;
    for (int attempt = 0; attempt < CHUNKMOVE_MAX_RETRIES; attempt++) {
        if (cancel_fn && cancel_fn(cancel_ctx)) {
            rc = TSDB_ERR_STOPPED;
            break;
        }
        if (chunk_store_epoch_unsure(st_from, id->volume) || chunk_store_epoch_unsure(st_to, id->volume)) {
            rc = CHUNK_ERR_EPOCH_UNSURE;
            break;
        }

        chunk_rd *rd = NULL;
        bool any_vuuid = vuuid_is_zero(id->vuuid);
        rc = any_vuuid ? chunk_rd_open_any(st_from, id->volume, id->oid, id->pos, &rd)
                       : chunk_rd_open(st_from, id, &rd);
        if (rc != 0)
            break;

        dev_t src_dev = 0;
        ino_t src_ino = 0;
        rc = chunk_rd_stat(rd, &src_dev, &src_ino);
        if (rc != 0) {
            chunk_rd_close(rd);
            break;
        }

        chunk_hdr hdr = *chunk_rd_hdr(rd);
        uint64_t ncells = chunk_pos_cells(&hdr.g, hdr.pos);
        uint64_t *sums = malloc((ncells ? ncells : 1) * sizeof *sums);
        if (!sums) {
            chunk_rd_close(rd);
            rc = TSDB_ERR_NOMEM;
            break;
        }
        if (ncells > 0)
            memcpy(sums, chunk_rd_sums(rd), (size_t)ncells * sizeof *sums);

        chunk_ident full_id = *id;
        memcpy(full_id.vuuid, hdr.vuuid, 16);

        /* Step 1: Target gets the volume's fence FIRST (durably, before any chunk bytes are written to `to`). */
        uint64_t fence = chunk_be_epoch(be, full_id.volume);
        uint64_t fe = chunk_store_epoch(st_from, full_id.volume);
        uint64_t te = chunk_store_epoch(st_to, full_id.volume);
        if (fe > fence)
            fence = fe;
        if (te > fence)
            fence = te;
        if (hdr.epoch > fence)
            fence = hdr.epoch;

        rc = chunk_store_raise(st_to, full_id.volume, fence);
        if (rc != 0) {
            free(sums);
            chunk_rd_close(rd);
            break;
        }

        /* Step 2: Open tmp_* on `to` preserving hdr.epoch under `fence`, copy and verify every cell paced. */
        chunk_wr *w = NULL;
        rc = chunk_wr_open_heal(st_to, &full_id, &hdr.g, hdr.epoch, fence, &w);
        if (rc != 0) {
            free(sums);
            chunk_rd_close(rd);
            if (rc == CHUNK_ERR_FENCED)
                continue; /* volume fence moved up concurrently; refresh and retry */
            break;
        }
        chunk_wr_set_defer_loc(w);

        size_t buf_cap = hdr.g.cell < hdr.chunk_len ? hdr.g.cell : (size_t)hdr.chunk_len;
        uint8_t *buf = malloc(buf_cap ? buf_cap : 1);
        if (!buf) {
            chunk_wr_abort(w);
            free(sums);
            chunk_rd_close(rd);
            rc = TSDB_ERR_NOMEM;
            break;
        }

        uint64_t off = 0;
        for (uint64_t i = 0; i < ncells && rc == 0; i++) {
            if (cancel_fn && cancel_fn(cancel_ctx)) {
                rc = TSDB_ERR_STOPPED;
                break;
            }
            size_t cl = (size_t)chunk_pos_cell_len(&hdr.g, hdr.pos, i);
            rc = chunk_rd_read(rd, off, buf, cl);
            if (rc == 0)
                rc = chunk_wr_cell(w, buf, cl, true, sums[i]);
            if (rc == 0) {
                if (o && o->pace)
                    o->pace(o->pace_ctx, cl);
                rc = cm_pace_bytes(pacer, cl, cancel_fn, cancel_ctx);
            }
            off += cl;
        }
        free(buf);
        chunk_rd_close(rd);

        if (rc != 0) {
            chunk_wr_abort(w);
            free(sums);
            break;
        }

#ifdef TSDB_TEST_HOOKS
        bool unlinked_early = false;
        if (chunkmove_mutant == CHUNKMOVE_MUT_UNLINKFIRST) {
            /* Planted bug (moveunlinkfirst): unlink source BEFORE target tmp is fsynced and renamed! A power cut right
             * after this unlink leaves zero copies on disk. */
            uint64_t del_ep = chunk_store_epoch(st_from, full_id.volume);
            if (fence > del_ep)
                del_ep = fence;
            (void)chunk_store_del(st_from, &full_id, del_ep);
            unlinked_early = true;
        }
#endif

        /* Step 3: Write sums + footer and fsync tmp_* on `to` BEFORE taking per-oid lock. */
        rc = chunk_wr_sync(w);
        if (rc != 0) {
            chunk_wr_abort(w);
            free(sums);
            break;
        }

#ifdef TSDB_TEST_HOOKS
        cm_run_before_lock_hook();
#endif

        /* Step 4: Under per-(volume, oid) lock, check source still exists with the same inode (serializes cleanly
         * against concurrent MK_GC deletes and r12-repair replace-puts), rename + dir-fsync target, verify every cell
         * on target against sums, update local index, then unlink source and fsync source dir. */
        chunk_be_oid_lock(be, full_id.volume, full_id.oid);

        bool retry_moved = false;
        dev_t cur_dev = 0;
        ino_t cur_ino = 0;
#ifdef TSDB_TEST_HOOKS
        int stat_rc = unlinked_early ? 0 : chunk_store_stat_chunk(st_from, full_id.volume, full_id.oid, full_id.pos,
                                                                  &cur_dev, &cur_ino);
#else
        int stat_rc = chunk_store_stat_chunk(st_from, full_id.volume, full_id.oid, full_id.pos, &cur_dev, &cur_ino);
#endif
        if (stat_rc != 0) {
            /* Concurrent MK_GC delete removed the source (TSDB_ERR_NOTFOUND) or source I/O failed: never resurrect. */
            chunk_wr_abort(w);
            chunk_be_oid_unlock(be, full_id.volume, full_id.oid);
            free(sums);
            rc = stat_rc;
            break;
        }
#ifdef TSDB_TEST_HOOKS
        if (!unlinked_early && (cur_dev != src_dev || cur_ino != src_ino)) {
#else
        if (cur_dev != src_dev || cur_ino != src_ino) {
#endif
            /* Concurrent r12-repair replace-put created a new inode on `from`: drop stale copy and retry. */
            chunk_wr_abort(w);
            chunk_be_oid_unlock(be, full_id.volume, full_id.oid);
            free(sums);
            retry_moved = true;
            rc = TSDB_ERR_IO;
        }

        if (!retry_moved) {
            rc = chunk_wr_commit(w); /* frees w on every path; skips tail/fsync since already synced */
            if (rc == CHUNK_ERR_FENCED) {
                chunk_be_oid_unlock(be, full_id.volume, full_id.oid);
                free(sums);
                retry_moved = true;
            } else if (rc != 0) {
                chunk_be_oid_unlock(be, full_id.volume, full_id.oid);
                free(sums);
                break;
            }
        }

        if (!retry_moved) {
            /* Step 5: Verify every cell on `to` against the sums before updating index or touching source. */
            rc = verify_target_chunk(st_to, &full_id, &hdr, sums, ncells);
            if (rc != 0) {
                /* Drop the corrupt target copy under oid_mu and ensure local index still resolves to `from`. */
                uint64_t tdel_ep = chunk_store_epoch(st_to, full_id.volume);
                if (fence > tdel_ep)
                    tdel_ep = fence;
                (void)chunk_store_del(st_to, &full_id, tdel_ep);
                int primary = chunk_be_place(be, full_id.oid, full_id.pos);
                if (from == primary)
                    chunk_be_loc_del(be, full_id.volume, full_id.oid, full_id.pos);
                else
                    chunk_be_loc_set(be, full_id.volume, full_id.oid, full_id.pos, from);
                chunk_be_oid_unlock(be, full_id.volume, full_id.oid);
                free(sums);
                break;
            }

            /* Step 6: Update local index so readers immediately resolve to `to`. */
            chunk_be_loc_set(be, full_id.volume, full_id.oid, full_id.pos, to);

            /* Step 7: Unlink source on `from` and fsync its directory. */
#ifdef TSDB_TEST_HOOKS
            if (!unlinked_early) {
#endif
                uint64_t del_ep = chunk_store_epoch(st_from, full_id.volume);
                if (fence > del_ep)
                    del_ep = fence;
                int drc = chunk_store_del(st_from, &full_id, del_ep);
                if (drc != 0 && drc != TSDB_ERR_NOTFOUND)
                    rc = drc;
#ifdef TSDB_TEST_HOOKS
            }
#endif
            chunk_be_oid_unlock(be, full_id.volume, full_id.oid);
            free(sums);
            if (rc == 0 && o && o->bytes_moved)
                *o->bytes_moved = chunk_file_size(&hdr.g, hdr.pos);
            break;
        }
    }

    chunk_store_unref(st_from);
    chunk_store_unref(st_to);
    return rc;
}
