/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Volume-leader chunk repair (server/chunkrepair.h; R12, r12-repair; docs/design-cluster-data.md E11, E14, E25, E26,
 * §5.9; PLAN.md D5, D9).
 *
 * Lock order: cr->mu is a leaf lock (never held across network I/O, disk I/O, decode, or user callbacks).
 */
#include "chunkrepair.h"

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
#include "../src/compat.h"
#include "../src/internal.h"

#ifdef TSDB_EC
#include "../src/ec/rs.h"
#endif

#define CR_STAGE_MEMFD_MAX (16u << 20)

_Atomic int chunkrepair_mutant = CR_MUT_NONE;

#ifdef TSDB_TEST_HOOKS
static pthread_mutex_t cr_pause_mu = PTHREAD_MUTEX_INITIALIZER;
static char cr_pause_path[512];
static atomic_uint_fast64_t cr_pause_hits;
#endif

const char *chunkrepair_res_name(int res)
{
    switch (res) {
    case CHUNKREPAIR_RES_REPAIRED: return "repaired";
    case CHUNKREPAIR_RES_UNRECOVERABLE: return "unrecoverable";
    case CHUNKREPAIR_RES_UNAVAILABLE: return "unavailable";
    case CHUNKREPAIR_RES_NOT_LIVE: return "not_live";
    case CHUNKREPAIR_RES_FENCED: return "fenced";
    case CHUNKREPAIR_RES_FAILED: return "failed";
    case CHUNKREPAIR_RES_DROPPED: return "dropped";
    default: return "unknown";
    }
}

int chunkrepair_mutant_parse(const char *name)
{
    if (!name || !*name || strcmp(name, "none") == 0)
        return CR_MUT_NONE;
    if (strcmp(name, "repairnolive") == 0)
        return CR_MUT_NOLIVE;
    if (strcmp(name, "repairnosums") == 0)
        return CR_MUT_NOSUMS;
    if (strcmp(name, "repairownepoch") == 0)
        return CR_MUT_OWNEPOCH;
    return -1;
}

void chunkrepair_env_init(void)
{
#ifdef TSDB_TEST_HOOKS
    const char *m = getenv("TSDB_REPAIR_MUTANT");
    if (m && *m) {
        int v = chunkrepair_mutant_parse(m);
        if (v >= 0) {
            atomic_store_explicit(&chunkrepair_mutant, v, memory_order_release);
            fprintf(stderr, "tsdbd: chunkrepair MUTANT %s (tests only)\n", m);
        }
    }
    const char *t = getenv("TSDB_REPAIR_TEST");
    if (t && strncmp(t, "pause_before_put=", 17) == 0)
        chunkrepair_set_pause_path(t + 17);
#endif
}

#ifdef TSDB_TEST_HOOKS
void chunkrepair_set_pause_path(const char *path)
{
    pthread_mutex_lock(&cr_pause_mu);
    if (path && *path)
        snprintf(cr_pause_path, sizeof cr_pause_path, "%s", path);
    else
        cr_pause_path[0] = '\0';
    pthread_mutex_unlock(&cr_pause_mu);
}

uint64_t chunkrepair_pause_hits(void)
{
    return atomic_load_explicit(&cr_pause_hits, memory_order_acquire);
}
#endif

#define CR_DEFAULT_MAX_RETRIES 3u
#define CR_DEFAULT_RETRY_BASE_MS 500u
#define CR_DEFAULT_COOLDOWN_MS 30000u
#define CR_COOLDOWN_SLOTS 256u

static uint64_t cr_now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

struct cr_item {
    struct cr_item *next;
    uint32_t volume;
    uint8_t oid[16];
    unsigned pos;
    uint32_t target_node; /* 0 if not yet resolved */
    uint8_t retries;
    uint64_t ready_ms; /* 0 = ready immediately; monotonic ms backoff deadline */
};

struct cr_active {
    bool in_use;
    uint32_t volume;
    uint8_t oid[16];
    unsigned pos;
    uint32_t target_node;
};

struct cr_cool {
    uint32_t volume;
    uint8_t oid[16];
    unsigned pos;
    uint64_t until_ms;
};

struct chunkrepair {
    struct chunkrepair_ops ops;
    char stage_dir[PATH_MAX];
    unsigned nworkers;
    size_t qmax;
    unsigned max_retries;
    uint32_t retry_base_ms;
    uint32_t cooldown_ms;

    pthread_mutex_t mu;
    pthread_cond_t cv;
    pthread_cond_t idle_cv;
    atomic_bool stopping;

    struct cr_item *qh, *qt;
    size_t qlen;
    size_t nactive;
    struct cr_active active[CHUNKREPAIR_MAX_WORKERS];
    struct cr_cool cool[CR_COOLDOWN_SLOTS];
    unsigned cool_next;
    pthread_t workers[CHUNKREPAIR_MAX_WORKERS];

    atomic_uint_fast64_t results[CHUNKREPAIR_RES_COUNT];
    atomic_uint_fast64_t bytes_repaired;
    atomic_uint_fast64_t deduped;
};

static bool cool_active_locked(const struct chunkrepair *cr, uint32_t volume, const uint8_t oid[16], unsigned pos,
                               uint64_t now)
{
    if (cr->cooldown_ms == 0)
        return false;
    for (unsigned i = 0; i < CR_COOLDOWN_SLOTS; i++) {
        const struct cr_cool *c = &cr->cool[i];
        if (c->until_ms > now && c->volume == volume && c->pos == pos && memcmp(c->oid, oid, 16) == 0)
            return true;
    }
    return false;
}

static void cool_record_locked(struct chunkrepair *cr, uint32_t volume, const uint8_t oid[16], unsigned pos,
                               uint64_t now)
{
    if (cr->cooldown_ms == 0)
        return;
    uint64_t until = now + cr->cooldown_ms;
    unsigned slot = cr->cool_next;
    for (unsigned i = 0; i < CR_COOLDOWN_SLOTS; i++) {
        struct cr_cool *c = &cr->cool[i];
        if (c->volume == volume && c->pos == pos && memcmp(c->oid, oid, 16) == 0) {
            c->until_ms = until;
            return;
        }
        if (c->until_ms <= now)
            slot = i;
    }
    cr->cool[slot].volume = volume;
    memcpy(cr->cool[slot].oid, oid, 16);
    cr->cool[slot].pos = pos;
    cr->cool[slot].until_ms = until;
    if (slot == cr->cool_next)
        cr->cool_next = (cr->cool_next + 1u) % CR_COOLDOWN_SLOTS;
}

static inline void cr_put64(uint8_t *p, uint64_t v)
{
    for (int i = 0; i < 8; i++)
        p[i] = (uint8_t)(v >> (8 * i));
}

static bool pos_written(uint32_t positions, unsigned pos)
{
    if (positions == 0)
        return true;
    return pos < 32 && ((positions & (1u << pos)) != 0);
}

static int pwrite_full(int fd, const void *buf, size_t n, uint64_t off)
{
    const uint8_t *p = buf;
    size_t done = 0;
    while (done < n) {
        ssize_t w = pwrite(fd, p + done, n - done, (off_t)(off + done));
        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0)
            return TSDB_ERR_IO;
        done += (size_t)w;
    }
    return 0;
}

static int stage_open_fd(const char *stage_dir, uint64_t total)
{
    int fd = -1;
#if defined(__linux__) && defined(SYS_memfd_create)
#ifndef MFD_CLOEXEC
#define MFD_CLOEXEC 0x0001U
#endif
    if (total <= CR_STAGE_MEMFD_MAX)
        fd = (int)syscall(SYS_memfd_create, "tsdb-repairput", (unsigned)MFD_CLOEXEC);
#endif
    if (fd < 0) {
        char path[PATH_MAX + 32];
        const char *dir = stage_dir && stage_dir[0] ? stage_dir : "/tmp";
        snprintf(path, sizeof path, "%s/chunkrepair.XXXXXX", dir);
        fd = mkstemp(path);
        if (fd < 0)
            return TSDB_ERR_IO;
        unlink(path);
        (void)fcntl(fd, F_SETFD, FD_CLOEXEC);
    }
    return fd;
}

static bool node_busy_locked(const struct chunkrepair *cr, uint32_t node, unsigned skip_slot)
{
    if (node == 0)
        return false;
    for (unsigned i = 0; i < cr->nworkers; i++) {
        if (i == skip_slot)
            continue;
        if (cr->active[i].in_use && cr->active[i].target_node == node)
            return true;
    }
    return false;
}

/* Reserve `node` for `slot` under cr->mu so at most one repair targets a given storage node at a time. */
static bool reserve_node(struct chunkrepair *cr, unsigned slot, uint32_t node)
{
    pthread_mutex_lock(&cr->mu);
    while (!atomic_load_explicit(&cr->stopping, memory_order_acquire) && node_busy_locked(cr, node, slot))
        pthread_cond_wait(&cr->cv, &cr->mu);
    bool ok = !atomic_load_explicit(&cr->stopping, memory_order_acquire);
    if (ok)
        cr->active[slot].target_node = node;
    pthread_mutex_unlock(&cr->mu);
    return ok;
}

#ifdef TSDB_EC
static int decode_and_stage(struct chunkrepair *cr, uint32_t volume, unsigned target_pos,
                            const struct chunkrepair_obj *obj, const struct chunkrepair_vol_view *vv, int *fd_out,
                            uint64_t *sum_hash_out, uint64_t *bytes_out)
{
    *fd_out = -1;
    *sum_hash_out = 0;
    *bytes_out = 0;

    const chunk_geom *g = &obj->g;
    unsigned k = g->k, m = g->m, npos = chunk_npos(g);
    if (m == 0 || npos > vv->nstripe || target_pos >= npos)
        return CHUNKREPAIR_RES_FAILED;

    iso_rs *rs = chunk_rs_get(k, m);
    if (!rs)
        return CHUNKREPAIR_RES_FAILED;

    uint64_t target_cells = chunk_pos_cells(g, target_pos);
    uint64_t target_base = chunk_sums_base(g, target_pos);
    uint64_t total_data_cells = chunk_ncells(g);
    uint64_t pos_len = chunk_pos_len(g, target_pos);
    if (target_cells == 0 || target_base + target_cells > obj->nsums)
        return CHUNKREPAIR_RES_FAILED;

    uint64_t sums_bytes = 8 * target_cells;
    uint64_t total_bytes = sums_bytes + pos_len;
    int fd = stage_open_fd(cr->stage_dir, total_bytes);
    if (fd < 0)
        return CHUNKREPAIR_RES_FAILED;

    uint64_t *pos_sums = malloc((size_t)target_cells * sizeof *pos_sums);
    size_t cell_cap = g->cell ? (size_t)g->cell : 1;
    uint8_t *shards[CHUNK_POS_MAX] = {NULL};
    bool alloc_ok = pos_sums != NULL;
    for (unsigned p = 0; alloc_ok && p < npos; p++) {
        shards[p] = malloc(cell_cap);
        if (!shards[p])
            alloc_ok = false;
    }
    if (!alloc_ok) {
        for (unsigned p = 0; p < npos; p++)
            free(shards[p]);
        free(pos_sums);
        close(fd);
        return CHUNKREPAIR_RES_FAILED;
    }

    int mut = atomic_load_explicit(&chunkrepair_mutant, memory_order_acquire);
    bool nosums = (mut == CR_MUT_NOSUMS);
    iso_rs_plan *plan = NULL;
    uint32_t plan_mask = 0;
    bool data_only = target_pos < k;
    int res = CHUNKREPAIR_RES_REPAIRED;
    uint64_t cell_off = 0;

    for (uint64_t s = 0; s < target_cells; s++) {
        if (atomic_load_explicit(&cr->stopping, memory_order_acquire)) {
            res = CHUNKREPAIR_RES_UNAVAILABLE;
            break;
        }
        uint64_t Ls = chunk_stripe_len(g, s);
        bool present[CHUNK_POS_MAX];
        /* Note: sts[p] is only populated for positions visited before reaching ngood == k; when ngood < k below the
         * loop ran through all npos entries so every non-target, non-good slot holds its actual failure status. */
        int sts[CHUNK_POS_MAX];
        memset(present, 0, sizeof present);
        memset(sts, 0, sizeof sts);
        unsigned ngood = 0;
        uint32_t pmask = 0;

        for (unsigned p = 0; p < npos && ngood < k; p++) {
            if (p == target_pos)
                continue;
            if (!pos_written(obj->positions, p)) {
                sts[p] = CHUNK_ST_UNWRITTEN;
                continue;
            }
            uint64_t cidx = p < k ? s * (uint64_t)k + p : 0;
            if (p < k && cidx >= total_data_cells) {
                /* Virtual zero cell beyond end of object (E7). */
                memset(shards[p], 0, (size_t)Ls);
                present[p] = true;
                pmask |= 1u << p;
                ngood++;
                continue;
            }
            uint32_t snode = vv->stripe[p];
            if (vv->stripe_filling[p]) {
                uint64_t barrier = vv->stripe_set_ver[p] ? vv->stripe_set_ver[p] : vv->stripe_version;
                bool pre_barrier = (barrier == 0 || obj->stripe_version < barrier);
                if (pre_barrier) {
                    if (vv->stripe_prev[p] != 0 && vv->stripe_prev_up[p]) {
                        snode = vv->stripe_prev[p];
                    } else if (!cr->ops.is_filled ||
                               !cr->ops.is_filled(cr->ops.arg, snode, volume, p, barrier, obj->oid)) {
                        sts[p] = CHUNK_ST_DOWN;
                        continue;
                    }
                }
            }
            if (snode == 0) {
                sts[p] = CHUNK_ST_DOWN;
                continue;
            }

            uint64_t clen = chunk_pos_cell_len(g, p, s);
            uint64_t coff = s * (uint64_t)g->cell;
            int r = cr->ops.read_cell(cr->ops.arg, snode, volume, vv->fence_epoch, obj->vuuid, obj->oid, p, coff,
                                      shards[p], (size_t)clen);
            if (r != CHUNK_ST_OK) {
                sts[p] = r;
                continue;
            }
            uint64_t sbase = chunk_sums_base(g, p);
            if (sbase + s >= obj->nsums) {
                sts[p] = CHUNK_ST_DAMAGED;
                continue;
            }
            if (!nosums && xxh64(shards[p], (size_t)clen, 0) != obj->sums[sbase + s]) {
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
            /* Only CHUNK_ST_DAMAGED (checksum failure) and CHUNK_ST_UNWRITTEN count as permanently lost; DOWN, ABSENT
             * (e.g., pending sfill/sreconcile), and IOERR are transient and count as unavailable, never unrecoverable. */
            unsigned ntransient = 0;
            for (unsigned p = 0; p < npos; p++)
                if (p != target_pos &&
                    (sts[p] == CHUNK_ST_DOWN || sts[p] == CHUNK_ST_ABSENT || sts[p] == CHUNK_ST_IOERR))
                    ntransient++;
            bool away = (ngood + ntransient >= k);
            char hex[33];
            chunk_oid_hex(obj->oid, hex);
            if (away) {
                fprintf(stderr,
                        "tsdbd: chunkrepair: volume %" PRIu32 " oid=%s pos=%u stripe=%" PRIu64
                        ": only %u of %u good survivors (%u transient): unavailable\n",
                        volume, hex, target_pos, s, ngood, k, ntransient);
                res = CHUNKREPAIR_RES_UNAVAILABLE;
            } else {
                fprintf(stderr,
                        "tsdbd: health: chunk repair unrecoverable volume=%" PRIu32 " oid=%s pos=%u stripe=%" PRIu64
                        " good=%u need=%u transient=%u\n",
                        volume, hex, target_pos, s, ngood, k, ntransient);
                res = CHUNKREPAIR_RES_UNRECOVERABLE;
            }
            break;
        }

        if (!plan || plan_mask != pmask) {
            iso_rs_plan_free(plan);
            plan = iso_rs_plan_new(rs, present, data_only);
            if (!plan) {
                res = CHUNKREPAIR_RES_FAILED;
                break;
            }
            plan_mask = pmask;
        }
        iso_rs_plan_apply(plan, shards, (size_t)Ls);

        uint64_t out_len = chunk_pos_cell_len(g, target_pos, s);
        uint64_t want_out = obj->sums[target_base + s];
        uint64_t got_out = xxh64(shards[target_pos], (size_t)out_len, 0);
        if (!nosums && got_out != want_out) {
            char hex[33];
            chunk_oid_hex(obj->oid, hex);
            fprintf(stderr,
                    "tsdbd: health: chunk repair unrecoverable volume=%" PRIu32 " oid=%s pos=%u stripe=%" PRIu64
                    ": rebuilt cell sum mismatch\n",
                    volume, hex, target_pos, s);
            res = CHUNKREPAIR_RES_UNRECOVERABLE;
            break;
        }
        pos_sums[s] = nosums ? got_out : want_out;

        if (pwrite_full(fd, shards[target_pos], (size_t)out_len, sums_bytes + cell_off) != 0) {
            res = CHUNKREPAIR_RES_FAILED;
            break;
        }
        cell_off += out_len;
    }

    iso_rs_plan_free(plan);
    for (unsigned p = 0; p < npos; p++)
        free(shards[p]);

    if (res == CHUNKREPAIR_RES_REPAIRED) {
        uint8_t sbuf[8 * 512];
        for (uint64_t i = 0; i < target_cells;) {
            uint64_t c = target_cells - i < 512 ? target_cells - i : 512;
            for (uint64_t j = 0; j < c; j++)
                cr_put64(sbuf + 8 * j, pos_sums[i + j]);
            if (pwrite_full(fd, sbuf, (size_t)(8 * c), 8 * i) != 0) {
                res = CHUNKREPAIR_RES_FAILED;
                break;
            }
            i += c;
        }
    }

    if (res == CHUNKREPAIR_RES_REPAIRED) {
        *sum_hash_out = cs_sums_hash(pos_sums, target_cells);
        *bytes_out = pos_len;
        *fd_out = fd;
    } else {
        close(fd);
    }
    free(pos_sums);
    return res;
}
#else
static int decode_and_stage(struct chunkrepair *cr, uint32_t volume, unsigned target_pos,
                            const struct chunkrepair_obj *obj, const struct chunkrepair_vol_view *vv, int *fd_out,
                            uint64_t *sum_hash_out, uint64_t *bytes_out)
{
    (void)cr;
    (void)volume;
    (void)target_pos;
    (void)obj;
    (void)vv;
    *fd_out = -1;
    *sum_hash_out = 0;
    *bytes_out = 0;
    return CHUNKREPAIR_RES_FAILED;
}
#endif

#ifdef TSDB_TEST_HOOKS
static void maybe_pause_before_put(struct chunkrepair *cr, uint32_t volume, const uint8_t oid[16], unsigned pos)
{
    char path[512];
    pthread_mutex_lock(&cr_pause_mu);
    snprintf(path, sizeof path, "%s", cr_pause_path);
    pthread_mutex_unlock(&cr_pause_mu);
    if (!path[0] || access(path, F_OK) != 0)
        return;
    char hex[33];
    chunk_oid_hex(oid, hex);
    fprintf(stderr, "tsdbd: chunkrepair: paused before put volume=%" PRIu32 " oid=%s pos=%u\n", volume, hex, pos);
    atomic_fetch_add_explicit(&cr_pause_hits, 1, memory_order_release);
    while (access(path, F_OK) == 0 && !atomic_load_explicit(&cr->stopping, memory_order_acquire)) {
        struct timespec ts = {0, 20 * 1000000L};
        nanosleep(&ts, NULL);
    }
    fprintf(stderr, "tsdbd: chunkrepair: resumed before put volume=%" PRIu32 " oid=%s pos=%u\n", volume, hex, pos);
}
#endif

static int repair_one(struct chunkrepair *cr, unsigned slot, const struct cr_item *it)
{
    /* Step 1: check live / not-retired / not-pending-MK_GC before decoding (D9). */
    struct chunkrepair_obj obj;
    memset(&obj, 0, sizeof obj);
    int rc = cr->ops.lookup_live(cr->ops.arg, it->volume, it->oid, &obj);
    if (rc != 0) {
        free(obj.sums);
        if (rc == TSDB_ERR_NOTFOUND)
            return CHUNKREPAIR_RES_NOT_LIVE;
        fprintf(stderr, "tsdbd: chunkrepair: lookup_live volume=%" PRIu32 " pos=%u failed (%d)\n", it->volume, it->pos,
                rc);
        if (rc == TSDB_ERR_UNAVAILABLE)
            return CHUNKREPAIR_RES_UNAVAILABLE;
        return CHUNKREPAIR_RES_FAILED;
    }

    unsigned npos = chunk_npos(&obj.g);
    if (it->pos >= npos || !pos_written(obj.positions, it->pos)) {
        free(obj.sums);
        return CHUNKREPAIR_RES_NOT_LIVE;
    }

    /* Step 2: load current volume view (pinned fence epoch + stripe map). */
    struct chunkrepair_vol_view vv;
    memset(&vv, 0, sizeof vv);
    rc = cr->ops.vol_view(cr->ops.arg, it->volume, &vv);
    if (rc != 0 || !vv.is_leader || vv.fence_epoch == 0 || it->pos >= vv.nstripe) {
        fprintf(stderr, "tsdbd: chunkrepair: vol_view volume=%" PRIu32 " pos=%u unavailable (rc=%d lead=%d ep=%" PRIu64 ")\n",
                it->volume, it->pos, rc, (int)vv.is_leader, vv.fence_epoch);
        free(obj.sums);
        return CHUNKREPAIR_RES_UNAVAILABLE;
    }
    if (vv.stripe_filling[it->pos]) {
        uint64_t barrier = vv.stripe_set_ver[it->pos] ? vv.stripe_set_ver[it->pos] : vv.stripe_version;
        if (barrier == 0 || obj.stripe_version < barrier) {
            /* Position is currently being filled by sfill; leave it to sfill. */
            free(obj.sums);
            return CHUNKREPAIR_RES_UNAVAILABLE;
        }
    }
    uint32_t target_node = vv.stripe[it->pos];
    if (target_node == 0 || (it->target_node != 0 && it->target_node != target_node)) {
        /* Report was for an old holder before a stripe change; do not repair onto a different node. */
        free(obj.sums);
        return CHUNKREPAIR_RES_UNAVAILABLE;
    }
    if (target_node != it->target_node && !reserve_node(cr, slot, target_node)) {
        free(obj.sums);
        return CHUNKREPAIR_RES_UNAVAILABLE;
    }

    /* Step 3: decode target position from k verified survivors and check every rebuilt cell (E26). */
    int fd = -1;
    uint64_t sum_hash = 0, bytes = 0;
    int res = decode_and_stage(cr, it->volume, it->pos, &obj, &vv, &fd, &sum_hash, &bytes);
    if (res != CHUNKREPAIR_RES_REPAIRED) {
        free(obj.sums);
        return res;
    }

#ifdef TSDB_TEST_HOOKS
    maybe_pause_before_put(cr, it->volume, it->oid, it->pos);
    if (atomic_load_explicit(&cr->stopping, memory_order_acquire)) {
        close(fd);
        free(obj.sums);
        return CHUNKREPAIR_RES_UNAVAILABLE;
    }
#endif

    /* Step 4: re-check live/not-retired/not-pending-MK_GC under per-oid GC lock and send replace-put (D9, E14). */
    int mut = atomic_load_explicit(&chunkrepair_mutant, memory_order_acquire);
    uint64_t hdr_epoch = (mut == CR_MUT_OWNEPOCH) ? vv.fence_epoch : obj.writer_epoch;
    if (hdr_epoch == 0)
        hdr_epoch = 1;
    struct cs_put_meta meta;
    memset(&meta, 0, sizeof meta);
    meta.volume = it->volume;
    meta.epoch = vv.fence_epoch;
    memcpy(meta.vuuid, obj.vuuid, 16);
    memcpy(meta.oid, obj.oid, 16);
    meta.pos = (uint16_t)it->pos;
    meta.g = obj.g;
    meta.flags = CS_PUT_F_REPLACE;
    meta.hdr_epoch = hdr_epoch;

    rc = cr->ops.check_and_put(cr->ops.arg, target_node, &meta, fd, 0, sum_hash, mut == CR_MUT_NOLIVE);
    close(fd);
    free(obj.sums);

    if (rc == 0) {
        atomic_fetch_add_explicit(&cr->bytes_repaired, bytes, memory_order_relaxed);
        char hex[33];
        chunk_oid_hex(it->oid, hex);
        fprintf(stderr,
                "tsdbd: chunkrepair: repaired volume=%" PRIu32 " oid=%s pos=%u node=%" PRIu32 " hdr_epoch=%" PRIu64
                " fence=%" PRIu64 " bytes=%" PRIu64 "\n",
                it->volume, hex, it->pos, target_node, hdr_epoch, vv.fence_epoch, bytes);
        if (cr->ops.notify_reopen)
            cr->ops.notify_reopen(cr->ops.arg, it->volume, it->oid);
        return CHUNKREPAIR_RES_REPAIRED;
    }
    if (rc == TSDB_ERR_NOTFOUND)
        return CHUNKREPAIR_RES_NOT_LIVE;
    if (rc == CHUNK_ERR_FENCED)
        return CHUNKREPAIR_RES_FENCED;
    fprintf(stderr, "tsdbd: chunkrepair: put volume=%" PRIu32 " pos=%u node=%" PRIu32 " failed (%d)\n", it->volume,
            it->pos, target_node, rc);
    if (rc == TSDB_ERR_UNAVAILABLE)
        return CHUNKREPAIR_RES_UNAVAILABLE;
    return CHUNKREPAIR_RES_FAILED;
}

struct cr_warg {
    struct chunkrepair *cr;
    unsigned slot;
};

static bool pop_for_slot(struct chunkrepair *cr, unsigned slot, struct cr_item *out)
{
    pthread_mutex_lock(&cr->mu);
    for (;;) {
        if (atomic_load_explicit(&cr->stopping, memory_order_acquire)) {
            pthread_mutex_unlock(&cr->mu);
            return false;
        }
        uint64_t now = cr_now_ms();
        uint64_t next_ready = 0;
        struct cr_item *prev = NULL, *cur = cr->qh;
        while (cur) {
            if (!node_busy_locked(cr, cur->target_node, slot)) {
                if (cur->ready_ms <= now)
                    break;
                if (next_ready == 0 || cur->ready_ms < next_ready)
                    next_ready = cur->ready_ms;
            }
            prev = cur;
            cur = cur->next;
        }
        if (cur) {
            if (prev)
                prev->next = cur->next;
            else
                cr->qh = cur->next;
            if (cr->qt == cur)
                cr->qt = prev;
            cr->qlen--;
            *out = *cur;
            out->next = NULL;
            free(cur);

            cr->active[slot].in_use = true;
            cr->active[slot].volume = out->volume;
            memcpy(cr->active[slot].oid, out->oid, 16);
            cr->active[slot].pos = out->pos;
            cr->active[slot].target_node = out->target_node;
            cr->nactive++;
            pthread_mutex_unlock(&cr->mu);
            return true;
        }
        if (next_ready > now) {
            struct timespec dl;
            tsdb_cond_deadline_ms(&dl, (int)(next_ready - now > 60000 ? 60000 : next_ready - now));
            pthread_cond_timedwait(&cr->cv, &cr->mu, &dl);
        } else {
            pthread_cond_wait(&cr->cv, &cr->mu);
        }
    }
}

static void *worker_main(void *arg)
{
    struct cr_warg *wa = arg;
    struct chunkrepair *cr = wa->cr;
    unsigned slot = wa->slot;
    free(wa);

    struct cr_item it;
    while (pop_for_slot(cr, slot, &it)) {
        int res = repair_one(cr, slot, &it);
        bool will_retry = (res == CHUNKREPAIR_RES_UNAVAILABLE || res == CHUNKREPAIR_RES_FAILED) &&
                          it.retries < cr->max_retries && !atomic_load_explicit(&cr->stopping, memory_order_acquire);

        pthread_mutex_lock(&cr->mu);
        cr->active[slot].in_use = false;
        cr->active[slot].target_node = 0;
        cr->nactive--;
        bool requeued = false;
        if (will_retry && cr->qlen < cr->qmax && !atomic_load_explicit(&cr->stopping, memory_order_acquire)) {
            bool dup = false;
            for (struct cr_item *c = cr->qh; c; c = c->next)
                if (c->volume == it.volume && c->pos == it.pos && memcmp(c->oid, it.oid, 16) == 0) {
                    dup = true;
                    break;
                }
            if (!dup) {
                struct cr_item *ri = calloc(1, sizeof *ri);
                if (ri) {
                    ri->volume = it.volume;
                    memcpy(ri->oid, it.oid, 16);
                    ri->pos = it.pos;
                    ri->target_node = it.target_node;
                    ri->retries = (uint8_t)(it.retries + 1);
                    uint64_t delay = (uint64_t)cr->retry_base_ms << (it.retries < 6 ? it.retries : 6);
                    ri->ready_ms = cr_now_ms() + delay;
                    if (cr->qt)
                        cr->qt->next = ri;
                    else
                        cr->qh = ri;
                    cr->qt = ri;
                    cr->qlen++;
                    requeued = true;
                }
            } else {
                requeued = true;
            }
        }
        if (!requeued && (res == CHUNKREPAIR_RES_UNRECOVERABLE || res == CHUNKREPAIR_RES_UNAVAILABLE ||
                          res == CHUNKREPAIR_RES_FAILED))
            cool_record_locked(cr, it.volume, it.oid, it.pos, cr_now_ms());
        pthread_cond_broadcast(&cr->cv);
        if (cr->qlen == 0 && cr->nactive == 0)
            pthread_cond_broadcast(&cr->idle_cv);
        pthread_mutex_unlock(&cr->mu);

        if (!requeued) {
            if (res >= 0 && res < CHUNKREPAIR_RES_COUNT)
                atomic_fetch_add_explicit(&cr->results[res], 1, memory_order_relaxed);
            if (cr->ops.on_done)
                cr->ops.on_done(cr->ops.arg, it.volume, it.oid, it.pos, res);
        }
    }
    return NULL;
}

struct chunkrepair *chunkrepair_new(const struct chunkrepair_cfg *cfg, const struct chunkrepair_ops *ops)
{
    if (!ops || !ops->lookup_live || !ops->vol_view || !ops->read_cell || !ops->check_and_put)
        return NULL;
    struct chunkrepair *cr = calloc(1, sizeof *cr);
    if (!cr)
        return NULL;
    cr->ops = *ops;
    if (cfg && cfg->stage_dir[0])
        snprintf(cr->stage_dir, sizeof cr->stage_dir, "%s", cfg->stage_dir);
    else
        snprintf(cr->stage_dir, sizeof cr->stage_dir, "/tmp");
    cr->nworkers = (cfg && cfg->nworkers > 0) ? cfg->nworkers : CHUNKREPAIR_DEFAULT_WORKERS;
    if (cr->nworkers > CHUNKREPAIR_MAX_WORKERS)
        cr->nworkers = CHUNKREPAIR_MAX_WORKERS;
    cr->qmax = (cfg && cfg->queue_max > 0) ? cfg->queue_max : CHUNKREPAIR_QUEUE_MAX;
    cr->max_retries = (cfg && cfg->max_retries < 0)
                          ? 0u
                          : ((cfg && cfg->max_retries > 0) ? (unsigned)cfg->max_retries : CR_DEFAULT_MAX_RETRIES);
    cr->retry_base_ms = (cfg && cfg->retry_base_ms > 0) ? cfg->retry_base_ms : CR_DEFAULT_RETRY_BASE_MS;
    cr->cooldown_ms = (cfg && cfg->cooldown_ms < 0)
                          ? 0u
                          : ((cfg && cfg->cooldown_ms > 0) ? (uint32_t)cfg->cooldown_ms : CR_DEFAULT_COOLDOWN_MS);

    pthread_mutex_init(&cr->mu, NULL);
    tsdb_cond_init_mono(&cr->cv);
    pthread_cond_init(&cr->idle_cv, NULL);
    atomic_store_explicit(&cr->stopping, false, memory_order_relaxed);

    unsigned started = 0;
    for (; started < cr->nworkers; started++) {
        struct cr_warg *wa = malloc(sizeof *wa);
        if (!wa)
            break;
        wa->cr = cr;
        wa->slot = started;
        if (pthread_create(&cr->workers[started], NULL, worker_main, wa) != 0) {
            free(wa);
            break;
        }
    }
    if (started < cr->nworkers) {
        cr->nworkers = started;
        chunkrepair_free(cr);
        return NULL;
    }
    return cr;
}

void chunkrepair_free(struct chunkrepair *cr)
{
    if (!cr)
        return;
    pthread_mutex_lock(&cr->mu);
    atomic_store_explicit(&cr->stopping, true, memory_order_release);
    pthread_cond_broadcast(&cr->cv);
    pthread_cond_broadcast(&cr->idle_cv);
    pthread_mutex_unlock(&cr->mu);

    for (unsigned i = 0; i < cr->nworkers; i++)
        pthread_join(cr->workers[i], NULL);

    for (struct cr_item *it = cr->qh, *nx; it; it = nx) {
        nx = it->next;
        free(it);
    }
    pthread_cond_destroy(&cr->idle_cv);
    pthread_cond_destroy(&cr->cv);
    pthread_mutex_destroy(&cr->mu);
    free(cr);
}

int chunkrepair_enqueue(struct chunkrepair *cr, uint32_t volume, const uint8_t oid[16], unsigned pos,
                        uint32_t target_node)
{
    if (!cr || !oid || pos >= CHUNK_POS_MAX)
        return TSDB_ERR_INVALID;
    pthread_mutex_lock(&cr->mu);
    if (atomic_load_explicit(&cr->stopping, memory_order_acquire)) {
        pthread_mutex_unlock(&cr->mu);
        return TSDB_ERR_INVALID;
    }
    for (unsigned i = 0; i < cr->nworkers; i++) {
        if (cr->active[i].in_use && cr->active[i].volume == volume && cr->active[i].pos == pos &&
            memcmp(cr->active[i].oid, oid, 16) == 0) {
            atomic_fetch_add_explicit(&cr->deduped, 1, memory_order_relaxed);
            pthread_mutex_unlock(&cr->mu);
            return 0;
        }
    }
    for (struct cr_item *cur = cr->qh; cur; cur = cur->next) {
        if (cur->volume == volume && cur->pos == pos && memcmp(cur->oid, oid, 16) == 0) {
            if (cur->target_node == 0 && target_node != 0)
                cur->target_node = target_node;
            atomic_fetch_add_explicit(&cr->deduped, 1, memory_order_relaxed);
            pthread_mutex_unlock(&cr->mu);
            return 0;
        }
    }
    if (cool_active_locked(cr, volume, oid, pos, cr_now_ms())) {
        atomic_fetch_add_explicit(&cr->deduped, 1, memory_order_relaxed);
        pthread_mutex_unlock(&cr->mu);
        return 0;
    }
    if (cr->qlen >= cr->qmax) {
        atomic_fetch_add_explicit(&cr->results[CHUNKREPAIR_RES_DROPPED], 1, memory_order_relaxed);
        pthread_mutex_unlock(&cr->mu);
        return TSDB_ERR_UNAVAILABLE;
    }
    struct cr_item *it = calloc(1, sizeof *it);
    if (!it) {
        atomic_fetch_add_explicit(&cr->results[CHUNKREPAIR_RES_DROPPED], 1, memory_order_relaxed);
        pthread_mutex_unlock(&cr->mu);
        return TSDB_ERR_NOMEM;
    }
    it->volume = volume;
    memcpy(it->oid, oid, 16);
    it->pos = pos;
    it->target_node = target_node;
    if (cr->qt)
        cr->qt->next = it;
    else
        cr->qh = it;
    cr->qt = it;
    cr->qlen++;
    pthread_cond_broadcast(&cr->cv);
    pthread_mutex_unlock(&cr->mu);
    return 0;
}

void chunkrepair_get_stats(struct chunkrepair *cr, struct chunkrepair_stats *out)
{
    if (!out)
        return;
    memset(out, 0, sizeof *out);
    if (!cr)
        return;
    for (int i = 0; i < CHUNKREPAIR_RES_COUNT; i++)
        out->results[i] = atomic_load_explicit(&cr->results[i], memory_order_relaxed);
    out->bytes_repaired = atomic_load_explicit(&cr->bytes_repaired, memory_order_relaxed);
    out->deduped = atomic_load_explicit(&cr->deduped, memory_order_relaxed);
    pthread_mutex_lock(&cr->mu);
    out->queue_depth = cr->qlen + cr->nactive;
    pthread_mutex_unlock(&cr->mu);
}

size_t chunkrepair_queue_depth(struct chunkrepair *cr)
{
    if (!cr)
        return 0;
    pthread_mutex_lock(&cr->mu);
    size_t d = cr->qlen + cr->nactive;
    pthread_mutex_unlock(&cr->mu);
    return d;
}

void chunkrepair_wait_idle(struct chunkrepair *cr)
{
    if (!cr)
        return;
    pthread_mutex_lock(&cr->mu);
    while (!atomic_load_explicit(&cr->stopping, memory_order_acquire) && (cr->qlen > 0 || cr->nactive > 0))
        pthread_cond_wait(&cr->idle_cv, &cr->mu);
    pthread_mutex_unlock(&cr->mu);
}
