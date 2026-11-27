/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Unit tests for server/sreconcile.c (R9 track r9-reconcile; docs/design-cluster-data.md §5.2, E10, E14, E27, E28;
 * R9 plan D14, D15).
 *
 * Covers:
 *   1. Two-way OBJLIST vs CHUNK_LIST diff: fills ONLY missing (degraded) chunk, leaves existing intact chunks
 *      untouched (exact inode + exact one-chunk byte count). Kills `backfullcopy` and `backskipmissing`.
 *   2. Store answered with another store id: logs once with store path and both ids, treats every chunk on that
 *      store as missing (even if a stale file exists), refills all, and persists new expected id across restarts.
 *   3. Store answered empty: refills every object from survivors, bit-exact.
 *   4. Store answered not at all (DOWN): logs once with store path and expected id vs none, treats chunks as missing.
 *   5. CHUNK_LIST \ OBJLIST: oids on committed MK_GC pending list left to r8k (not deleted, not reported as orphan);
 *      oids neither in OBJLIST nor pending MK_GC reported as orphans and NEVER deleted here.
 *   6. sreconcile_note_missing + sreconcile_drain_noted: queues degraded position until holder returns UP.
 *   7. HBS_FILL progress reporting (version = current stripe_version, done/total, cleared on completion) and
 *      sreconcile_stop_pos.
 *
 * Usage: test_sreconcile [--mutant NAME] [SCRATCH]
 */
#define _GNU_SOURCE
#include "internal.h"
#include "chunk.h"
#include "chunk_priv.h"
#include "pace.h"
#include "sfill.h"
#include "sreconcile.h"

#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#ifndef TSDB_EC
int main(void)
{
    printf("test_sreconcile: SKIPPED (this build has no erasure codec: NO_EC=1)\n");
    return 0;
}
#else

static int fails;
static char root_dir[PATH_MAX];
static const uint8_t VUUID[16] = {0xa1, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
                                  0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x09};

#define CHECK(cond, ...)                                                                                              \
    do {                                                                                                              \
        if (!(cond)) {                                                                                                \
            fprintf(stderr, "FAIL %s:%d: %s — ", __FILE__, __LINE__, #cond);                                          \
            fprintf(stderr, __VA_ARGS__);                                                                             \
            fprintf(stderr, "\n");                                                                                    \
            fails++;                                                                                                  \
        }                                                                                                             \
    } while (0)

static int rm_one(const char *p, const struct stat *sb, int type, struct FTW *ftw)
{
    (void)sb, (void)type, (void)ftw;
    return remove(p);
}

static void rm_tree(const char *path)
{
    (void)nftw(path, rm_one, 16, FTW_DEPTH | FTW_PHYS);
}

static uint64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

static void sleep_ms(int ms)
{
    struct timespec ts = {ms / 1000, (long)(ms % 1000) * 1000000L};
    nanosleep(&ts, NULL);
}

/* ------------------------------------------------------------------ fake objects & cluster environment */

struct mock_obj {
    uint8_t oid[16];
    chunk_geom g;
    uint32_t positions;
    uint64_t writer_epoch;
    uint64_t stripe_version;
    uint64_t nsums;
    uint64_t *sums;
    uint8_t *pos_bytes[CHUNK_POS_MAX];
    uint64_t pos_len[CHUNK_POS_MAX];
};

struct mock_env {
    pthread_mutex_t mu;
    uint32_t volume;
    uint32_t pos;
    uint64_t vol_epoch;
    uint64_t stripe_version;
    uint32_t stripe[CHUNK_POS_MAX];
    unsigned nstripe;
    int margin;
    bool held_whole;

    chunk_backend *be;
    int nstores;
    char store_paths[2][PATH_MAX];
    int store_state_override[2]; /* -1: use real backend state */
    bool store_disk_ok_override[2];
    bool use_disk_override[2];
    uint8_t store_live_id_override[16];
    uint8_t store_disk_id_override[16];
    bool use_id_override;
    int force_place_store; /* -1: use oid[0] % nstores */

    struct mock_obj objs[16];
    uint32_t nobjs;
    uint32_t objlist_calls;
    uint32_t objlist_max_page;
    int fail_objlist_once;

    uint8_t gc_pending[16][16];
    uint32_t ngc_pending;
    bool gc_truncated;
    bool hide_local_exists;

    bool holder_up;
    uint32_t holder_nudged;
    bool note_on_first_nudge;
    uint8_t concurrent_note_oid[16];

    /* Progress report snapshots observed via on_report */
    struct sfill_report seen_rep[32];
    uint32_t nseen_rep;
    bool saw_active_report;
    bool slow_read;
};

struct cap_ctx {
    const chunk_geom *g;
    struct mock_obj *o;
};

static int cap_cell(void *ctx, unsigned pos, uint64_t idx, const uint8_t *data, size_t len, uint64_t sum)
{
    (void)sum;
    struct cap_ctx *c = ctx;
    memcpy(c->o->pos_bytes[pos] + idx * (uint64_t)c->g->cell, data, len);
    return 0;
}

static void mock_obj_build(struct mock_obj *o, uint8_t tag, unsigned k, unsigned m, uint32_t cell, uint64_t obj_len,
                           uint64_t epoch, uint64_t sver, uint32_t positions, uint64_t seed)
{
    memset(o, 0, sizeof *o);
    for (int i = 0; i < 16; i++)
        o->oid[i] = (uint8_t)(tag * 19 + i + 1);
    o->oid[0] = tag;
    if (chunk_geom_make(&o->g, k, m, cell, obj_len) != 0)
        abort();
    o->positions = positions;
    o->writer_epoch = epoch;
    o->stripe_version = sver;
    o->nsums = chunk_sums_total(&o->g);
    o->sums = calloc((size_t)o->nsums, sizeof(uint64_t));
    uint8_t *raw = malloc((size_t)obj_len);
    if (!o->sums || !raw)
        abort();
    uint64_t r = seed ^ 0x517cc1b727220a95ull;
    for (uint64_t i = 0; i < obj_len; i++) {
        r ^= r << 13;
        r ^= r >> 7;
        r ^= r << 17;
        raw[i] = (uint8_t)r;
    }
    unsigned npos = chunk_npos(&o->g);
    for (unsigned p = 0; p < npos; p++) {
        o->pos_len[p] = chunk_pos_len(&o->g, p);
        o->pos_bytes[p] = malloc((size_t)(o->pos_len[p] ? o->pos_len[p] : 1));
        if (!o->pos_bytes[p])
            abort();
    }
    struct cap_ctx cc = {.g = &o->g, .o = o};
    if (chunk_encode(&o->g, raw, cap_cell, &cc, o->sums) != 0)
        abort();
    free(raw);
}

static void mock_obj_free(struct mock_obj *o)
{
    free(o->sums);
    for (unsigned p = 0; p < CHUNK_POS_MAX; p++)
        free(o->pos_bytes[p]);
    memset(o, 0, sizeof *o);
}

static void mock_env_init_n(struct mock_env *e, const char *const *sdirs, int nstores, uint32_t vol, uint32_t pos,
                            uint64_t sver)
{
    memset(e, 0, sizeof *e);
    pthread_mutex_init(&e->mu, NULL);
    e->volume = vol;
    e->pos = pos;
    e->vol_epoch = 5;
    e->stripe_version = sver;
    e->nstripe = 6;
    for (unsigned p = 0; p < 6; p++)
        e->stripe[p] = 10 + p;
    e->margin = 1;
    e->held_whole = true;
    e->nstores = nstores;
    e->force_place_store = -1;
    for (int i = 0; i < 2; i++)
        e->store_state_override[i] = -1;

    for (int i = 0; i < nstores; i++) {
        snprintf(e->store_paths[i], sizeof e->store_paths[i], "%s", sdirs[i]);
        rm_tree(sdirs[i]);
        uint8_t sid[16];
        if (chunk_store_init(sdirs[i], NULL, sid) != 0)
            abort();
    }
    if (chunk_be_open(&e->be, sdirs, nstores, NULL) != 0)
        abort();
    if (chunk_be_see_epoch(e->be, vol, e->vol_epoch) != 0)
        abort();
}

static void mock_env_init(struct mock_env *e, const char *sdir, uint32_t vol, uint32_t pos, uint64_t sver)
{
    const char *paths[1] = {sdir};
    mock_env_init_n(e, paths, 1, vol, pos, sver);
}

static void mock_env_free(struct mock_env *e)
{
    for (uint32_t i = 0; i < e->nobjs; i++)
        mock_obj_free(&e->objs[i]);
    if (e->be)
        chunk_be_close(e->be);
    pthread_mutex_destroy(&e->mu);
}

static void mock_put_local(struct mock_env *e, const struct mock_obj *o, unsigned pos)
{
    chunk_ident id = {.volume = e->volume, .pos = (uint16_t)pos};
    memcpy(id.vuuid, VUUID, 16);
    memcpy(id.oid, o->oid, 16);
    uint64_t base = chunk_sums_base(&o->g, pos);
    if (chunk_be_put(e->be, 0, &id, &o->g, o->writer_epoch, o->pos_bytes[pos], o->sums + base) != 0)
        abort();
}

static ino_t chunk_inode(const char *sdir, uint32_t vol, const uint8_t oid[16], unsigned pos)
{
    char hex[33], fp[PATH_MAX + 128];
    chunk_oid_hex(oid, hex);
    snprintf(fp, sizeof fp, "%s/s%" PRIu32 "/o%s.%u.chunk", sdir, vol, hex, pos);
    struct stat st;
    return stat(fp, &st) == 0 ? st.st_ino : 0;
}

static bool chunk_exists_on_disk(const char *sdir, uint32_t vol, const uint8_t oid[16], unsigned pos)
{
    return chunk_inode(sdir, vol, oid, pos) != 0;
}

/* ------------------------------------------------------------------ sfill_ops & sreconcile_ops callbacks */

static int op_objlist(void *arg, uint32_t volume, uint64_t max_version, bool has_cursor,
                      const uint8_t cursor_oid[16], uint32_t max_entries,
                      uint8_t vuuid_out[16], struct sfill_obj **items_out, uint32_t *n_out, bool *more_out)
{
    struct mock_env *e = arg;
    *items_out = NULL;
    *n_out = 0;
    *more_out = false;
    memcpy(vuuid_out, VUUID, 16);
    pthread_mutex_lock(&e->mu);
    e->objlist_calls++;
    if (e->fail_objlist_once > 0) {
        e->fail_objlist_once--;
        pthread_mutex_unlock(&e->mu);
        return TSDB_ERR_UNAVAILABLE;
    }
    if (volume != e->volume) {
        pthread_mutex_unlock(&e->mu);
        return TSDB_ERR_NOTFOUND;
    }
    uint32_t matched[16];
    uint32_t nm = 0;
    for (uint32_t i = 0; i < e->nobjs; i++) {
        if (max_version != 0 && e->objs[i].stripe_version >= max_version)
            continue;
        if (has_cursor && memcmp(e->objs[i].oid, cursor_oid, 16) <= 0)
            continue;
        matched[nm++] = i;
    }
    uint32_t cap = max_entries;
    if (e->objlist_max_page > 0 && e->objlist_max_page < cap)
        cap = e->objlist_max_page;
    uint32_t take = nm < cap ? nm : cap;
    *more_out = take < nm;
    struct sfill_obj *out = take ? calloc(take, sizeof *out) : NULL;
    for (uint32_t k = 0; k < take; k++) {
        const struct mock_obj *o = &e->objs[matched[k]];
        memcpy(out[k].oid, o->oid, 16);
        out[k].g = o->g;
        out[k].positions = o->positions;
        out[k].writer_epoch = o->writer_epoch;
        out[k].stripe_version = o->stripe_version;
        out[k].nsums = o->nsums;
        out[k].sums = malloc((size_t)o->nsums * sizeof(uint64_t));
        memcpy(out[k].sums, o->sums, (size_t)o->nsums * sizeof(uint64_t));
    }
    pthread_mutex_unlock(&e->mu);
    *items_out = out;
    *n_out = take;
    return 0;
}

static int op_read_cell(void *arg, uint32_t node, uint32_t volume, uint64_t epoch,
                        const uint8_t vuuid[16], const uint8_t oid[16], unsigned pos,
                        uint64_t off, void *buf, size_t len)
{
    (void)node, (void)epoch, (void)vuuid;
    struct mock_env *e = arg;
    if (e->slow_read)
        sleep_ms(30);
    pthread_mutex_lock(&e->mu);
    if (volume != e->volume || pos >= CHUNK_POS_MAX) {
        pthread_mutex_unlock(&e->mu);
        return CHUNK_ST_ABSENT;
    }
    for (uint32_t i = 0; i < e->nobjs; i++) {
        if (memcmp(e->objs[i].oid, oid, 16) == 0) {
            if (off + len > e->objs[i].pos_len[pos]) {
                pthread_mutex_unlock(&e->mu);
                return CHUNK_ST_IOERR;
            }
            memcpy(buf, e->objs[i].pos_bytes[pos] + off, len);
            pthread_mutex_unlock(&e->mu);
            return CHUNK_ST_OK;
        }
    }
    pthread_mutex_unlock(&e->mu);
    return CHUNK_ST_ABSENT;
}

static int op_sfill_vol_view(void *arg, uint32_t volume, uint64_t *epoch_out,
                             uint32_t stripe_out[CHUNK_POS_MAX], unsigned *nstripe_out,
                             bool *prev_up_out, uint32_t prev_node)
{
    (void)prev_node;
    struct mock_env *e = arg;
    pthread_mutex_lock(&e->mu);
    if (volume != e->volume) {
        pthread_mutex_unlock(&e->mu);
        return -1;
    }
    *epoch_out = e->vol_epoch;
    *nstripe_out = e->nstripe;
    memcpy(stripe_out, e->stripe, e->nstripe * sizeof(uint32_t));
    *prev_up_out = false;
    pthread_mutex_unlock(&e->mu);
    return 0;
}

static chunk_backend *op_be_hold(void *arg)
{
    struct mock_env *e = arg;
    bool any_up = false;
    pthread_mutex_lock(&e->mu);
    for (int i = 0; i < e->nstores; i++) {
        if (e->store_state_override[i] != CHUNK_STORE_DOWN_MISSING)
            any_up = true;
    }
    pthread_mutex_unlock(&e->mu);
    return any_up ? e->be : NULL;
}

static void op_be_release(void *arg, chunk_backend *be)
{
    (void)arg, (void)be;
}

struct lcollect_ctx {
    chunk_listed *v;
    uint32_t n, cap;
};

static int lcollect_cb(void *ctx, const chunk_listed *c)
{
    struct lcollect_ctx *s = ctx;
    if (s->n == s->cap) {
        uint32_t nc = s->cap ? s->cap * 2 : 16;
        chunk_listed *nv = realloc(s->v, nc * sizeof *nv);
        if (!nv)
            return TSDB_ERR_NOMEM;
        s->v = nv;
        s->cap = nc;
    }
    s->v[s->n++] = *c;
    return 0;
}

static int op_local_list(void *arg, uint32_t volume, bool has_cursor, const uint8_t cursor_oid[16],
                         uint16_t cursor_pos, uint32_t max_entries, chunk_listed **items_out, uint32_t *n_out,
                         bool *more_out)
{
    (void)has_cursor, (void)cursor_oid, (void)cursor_pos, (void)max_entries;
    struct mock_env *e = arg;
    *items_out = NULL;
    *n_out = 0;
    *more_out = false;
    bool any_up = false;
    pthread_mutex_lock(&e->mu);
    for (int i = 0; i < e->nstores; i++)
        if (e->store_state_override[i] != CHUNK_STORE_DOWN_MISSING)
            any_up = true;
    pthread_mutex_unlock(&e->mu);
    if (!any_up)
        return TSDB_ERR_UNAVAILABLE;
    struct lcollect_ctx lc = {0};
    int rc = chunk_be_list(e->be, volume, false, lcollect_cb, &lc);
    if (rc != 0) {
        free(lc.v);
        return rc;
    }
    *items_out = lc.v;
    *n_out = lc.n;
    return 0;
}

static bool op_local_exists(void *arg, uint32_t volume, const uint8_t oid[16], unsigned pos)
{
    struct mock_env *e = arg;
    pthread_mutex_lock(&e->mu);
    bool hide = e->hide_local_exists;
    int ns = e->nstores;
    char paths[2][PATH_MAX];
    for (int i = 0; i < ns; i++)
        snprintf(paths[i], sizeof paths[i], "%s", e->store_paths[i]);
    pthread_mutex_unlock(&e->mu);
    if (hide)
        return false;
    for (int i = 0; i < ns; i++) {
        if (chunk_exists_on_disk(paths[i], volume, oid, pos))
            return true;
    }
    return false;
}

static int op_gc_pending(void *arg, uint32_t volume, uint8_t (**oids_out)[16], uint32_t *n_out, bool *truncated_out)
{
    struct mock_env *e = arg;
    *oids_out = NULL;
    *n_out = 0;
    if (truncated_out)
        *truncated_out = false;
    pthread_mutex_lock(&e->mu);
    if (volume != e->volume) {
        pthread_mutex_unlock(&e->mu);
        return TSDB_ERR_NOTFOUND;
    }
    uint32_t n = e->ngc_pending;
    uint8_t (*out)[16] = n ? malloc(16 * (size_t)n) : NULL;
    if (n && out)
        memcpy(out, e->gc_pending, 16 * (size_t)n);
    bool trunc = e->gc_truncated;
    pthread_mutex_unlock(&e->mu);
    *oids_out = out;
    *n_out = n;
    if (truncated_out)
        *truncated_out = trunc;
    return 0;
}

static int op_rec_vol_view(void *arg, uint32_t volume, uint32_t pos, struct sreconcile_vol_view *out)
{
    struct mock_env *e = arg;
    memset(out, 0, sizeof *out);
    pthread_mutex_lock(&e->mu);
    if (volume != e->volume || pos != e->pos) {
        pthread_mutex_unlock(&e->mu);
        return -1;
    }
    out->held_whole = e->held_whole;
    out->stripe_version = e->stripe_version;
    out->leader = 10;
    out->margin = e->margin;
    out->concurrency = 2;
    pthread_mutex_unlock(&e->mu);
    return 0;
}

static int op_list_held(void *arg, struct sreconcile_held_pos **out, uint32_t *n_out)
{
    struct mock_env *e = arg;
    pthread_mutex_lock(&e->mu);
    if (!e->held_whole) {
        pthread_mutex_unlock(&e->mu);
        *out = NULL;
        *n_out = 0;
        return 0;
    }
    struct sreconcile_held_pos *arr = calloc(1, sizeof *arr);
    arr[0].volume = e->volume;
    arr[0].pos = e->pos;
    arr[0].stripe_version = e->stripe_version;
    pthread_mutex_unlock(&e->mu);
    *out = arr;
    *n_out = 1;
    return 0;
}

static int op_store_count(void *arg)
{
    struct mock_env *e = arg;
    return e->nstores;
}

static int op_store_info(void *arg, int idx, struct sreconcile_store_info *out)
{
    struct mock_env *e = arg;
    if (idx < 0 || idx >= e->nstores)
        return -1;
    memset(out, 0, sizeof *out);
    pthread_mutex_lock(&e->mu);
    snprintf(out->path, sizeof out->path, "%s", e->store_paths[idx]);
    out->state = e->store_state_override[idx] >= 0 ? e->store_state_override[idx] : chunk_be_store_state(e->be, idx);
    if (e->use_id_override && idx == 0) {
        memcpy(out->live_id, e->store_live_id_override, 16);
        memcpy(out->disk_id, e->store_disk_id_override, 16);
        out->disk_ok = e->use_disk_override[idx] ? e->store_disk_ok_override[idx] : true;
    } else {
        chunk_store *st = chunk_be_store(e->be, idx);
        if (st && out->state == CHUNK_STORE_UP)
            memcpy(out->live_id, chunk_store_id(st), 16);
        out->disk_ok = e->use_disk_override[idx] ? e->store_disk_ok_override[idx]
                                                 : (chunk_store_meta_read(e->store_paths[idx], out->disk_id, NULL) == 0);
    }
    pthread_mutex_unlock(&e->mu);
    return 0;
}

static int op_place_store(void *arg, const uint8_t oid[16], unsigned pos)
{
    struct mock_env *e = arg;
    if (e->force_place_store >= 0)
        return e->force_place_store;
    if (e->nstores <= 1)
        return 0;
    return chunk_be_place(e->be, oid, pos);
}

static int op_notify_holder(void *arg, uint32_t volume, uint32_t pos)
{
    struct mock_env *e = arg;
    bool inject = false;
    uint8_t coid[16] = {0};
    pthread_mutex_lock(&e->mu);
    bool up = e->holder_up && volume == e->volume && pos == e->pos;
    if (up) {
        e->holder_nudged++;
        if (e->note_on_first_nudge) {
            e->note_on_first_nudge = false;
            inject = true;
            memcpy(coid, e->concurrent_note_oid, 16);
        }
    }
    pthread_mutex_unlock(&e->mu);
    if (inject)
        sreconcile_note_missing(volume, coid, pos);
    return up ? 1 : 0;
}

static void op_on_report(void *arg, const struct sfill_report *reps, uint32_t nreps)
{
    struct mock_env *e = arg;
    pthread_mutex_lock(&e->mu);
    if (nreps > 0) {
        e->saw_active_report = true;
        uint32_t take = nreps < 32 ? nreps : 32;
        memcpy(e->seen_rep, reps, take * sizeof *reps);
        e->nseen_rep = take;
    }
    pthread_mutex_unlock(&e->mu);
}

static struct sfill_ops make_sfill_ops(struct mock_env *e)
{
    return (struct sfill_ops){
        .objlist = op_objlist,
        .read_cell = op_read_cell,
        .vol_view = op_sfill_vol_view,
        .local_be_hold = op_be_hold,
        .local_be_release = op_be_release,
        .arg = e,
    };
}

static struct sreconcile_ops make_rec_ops(struct mock_env *e)
{
    return (struct sreconcile_ops){
        .objlist = op_objlist,
        .local_list = op_local_list,
        .local_exists = op_local_exists,
        .gc_pending = op_gc_pending,
        .vol_view = op_rec_vol_view,
        .list_held = op_list_held,
        .store_count = op_store_count,
        .store_info = op_store_info,
        .place_store = op_place_store,
        .notify_holder = op_notify_holder,
        .on_report = op_on_report,
        .arg = e,
    };
}

static bool wait_passes_ok(struct sreconcile *rec, uint64_t want, uint64_t timeout_ms)
{
    uint64_t t0 = now_ms();
    for (;;) {
        struct sreconcile_stats st;
        sreconcile_get_stats(rec, &st);
        if (st.passes_ok >= want)
            return true;
        if (now_ms() - t0 > timeout_ms)
            return false;
        sleep_ms(10);
    }
}

/* ------------------------------------------------------------------ Case 1: two-way diff, missing + existing + GC + orphan */

static void test_diff_and_two_way(void)
{
    char sdir[PATH_MAX + 64], stdir[PATH_MAX + 64];
    snprintf(sdir, sizeof sdir, "%s/c1_store", root_dir);
    snprintf(stdir, sizeof stdir, "%s/c1_state", root_dir);
    rm_tree(stdir);
    mkdir(stdir, 0755);

    struct mock_env env;
    mock_env_init(&env, sdir, 1, 2, 7);
    env.slow_read = true; /* allow HBS_FILL report observation while filling */

    /* 3 committed objects on leader: obj 0 and obj 1 already held locally, obj 2 settled degraded (pos 2 missing). */
    mock_obj_build(&env.objs[0], 1, 4, 2, 65536, 400000, 5, 7, 0x3fu, 101);
    mock_obj_build(&env.objs[1], 2, 4, 2, 65536, 450000, 5, 7, 0x3fu, 202);
    mock_obj_build(&env.objs[2], 3, 4, 2, 65536, 380000, 5, 7, 0x3fu & ~(1u << 2), 303);
    env.nobjs = 3;

    /* Put obj 0 and obj 1 locally. */
    mock_put_local(&env, &env.objs[0], 2);
    mock_put_local(&env, &env.objs[1], 2);
    ino_t ino0 = chunk_inode(sdir, 1, env.objs[0].oid, 2);
    ino_t ino1 = chunk_inode(sdir, 1, env.objs[1].oid, 2);

    /* Add 2 extra local chunks NOT in leader's OBJLIST:
     *   - extra_gc: on committed MK_GC pending list -> left to r8k (not deleted, not reported as orphan)
     *   - extra_orphan: neither in OBJLIST nor in MK_GC pending -> reported as orphan, NEVER deleted */
    struct mock_obj extra_gc, extra_orphan;
    mock_obj_build(&extra_gc, 90, 4, 2, 65536, 200000, 5, 7, 0x3fu, 909);
    mock_obj_build(&extra_orphan, 91, 4, 2, 65536, 200000, 5, 7, 0x3fu, 919);
    mock_put_local(&env, &extra_gc, 2);
    mock_put_local(&env, &extra_orphan, 2);
    memcpy(env.gc_pending[0], extra_gc.oid, 16);
    env.ngc_pending = 1;

    struct sfill_ops sops = make_sfill_ops(&env);
    struct sreconcile_ops rops = make_rec_ops(&env);
    struct sfill *sf = sfill_new_with_ops(&sops);
    struct sreconcile *rec = sreconcile_new(sf, &rops, stdir);

    sreconcile_on_start(sf);
    CHECK(wait_passes_ok(rec, 1, 10000), "c1: reconcile pass did not complete");

    struct sreconcile_stats rst;
    struct sfill_stats fst;
    sreconcile_get_stats(rec, &rst);
    sfill_get_stats(sf, &fst);

    /* Missing degraded obj 2 must be filled (kills backskipmissing). */
    CHECK(chunk_exists_on_disk(sdir, 1, env.objs[2].oid, 2), "c1: missing obj 2 chunk was not filled");
    CHECK(rst.filled == 1, "c1: expected exactly 1 filled object, got %" PRIu64, rst.filled);
    CHECK(rst.already_present == 2, "c1: expected 2 already_present, got %" PRIu64, rst.already_present);
    CHECK(rst.bytes_filled == env.objs[2].pos_len[2],
          "c1: expected bytes_filled=%" PRIu64 " (one chunk), got %" PRIu64, env.objs[2].pos_len[2], rst.bytes_filled);
    CHECK(fst.bytes_written == env.objs[2].pos_len[2],
          "c1: expected sfill bytes_written=%" PRIu64 ", got %" PRIu64, env.objs[2].pos_len[2], fst.bytes_written);

    /* Existing chunks must keep their exact inodes (kills backfullcopy). */
    CHECK(chunk_inode(sdir, 1, env.objs[0].oid, 2) == ino0, "c1: existing obj 0 chunk was rewritten");
    CHECK(chunk_inode(sdir, 1, env.objs[1].oid, 2) == ino1, "c1: existing obj 1 chunk was rewritten");

    /* Two-way check: gc_pending left to r8k, orphan reported and NEVER deleted. */
    CHECK(rst.gc_pending == 1, "c1: expected gc_pending==1, got %" PRIu64, rst.gc_pending);
    CHECK(rst.orphans_reported == 1, "c1: expected orphans_reported==1, got %" PRIu64, rst.orphans_reported);
    CHECK(chunk_exists_on_disk(sdir, 1, extra_gc.oid, 2), "c1: gc_pending chunk was deleted directly");
    CHECK(chunk_exists_on_disk(sdir, 1, extra_orphan.oid, 2), "c1: orphan chunk was deleted by reconcile");

    /* HBS_FILL progress report must carry version == current stripe_version (7) and clear when done. */
    CHECK(env.saw_active_report, "c1: never observed active HBS_FILL progress report");
    if (env.nseen_rep > 0) {
        CHECK(env.seen_rep[0].volume == 1 && env.seen_rep[0].pos == 2 && env.seen_rep[0].stripe_version == 7 &&
                  env.seen_rep[0].total == 1 && env.seen_rep[0].whole == 0,
              "c1: bad HBS_FILL report (vol=%" PRIu32 " pos=%" PRIu32 " ver=%" PRIu64 " total=%" PRIu64 " whole=%u)",
              env.seen_rep[0].volume, env.seen_rep[0].pos, env.seen_rep[0].stripe_version, env.seen_rep[0].total,
              env.seen_rep[0].whole);
    }
    struct sfill_report cur[4];
    CHECK(sreconcile_report(rec, cur, 4) == 0, "c1: reconcile report not cleared after completion");

    sreconcile_stop(rec);
    sfill_free(sf);
    sreconcile_free(rec);
    mock_obj_free(&extra_gc);
    mock_obj_free(&extra_orphan);
    mock_env_free(&env);
}

/* ------------------------------------------------------------------ Case 2: store id mismatch & empty store */

static void test_store_id_and_empty(void)
{
    char sdir[PATH_MAX + 64], stdir[PATH_MAX + 64];
    snprintf(sdir, sizeof sdir, "%s/c2_store", root_dir);
    snprintf(stdir, sizeof stdir, "%s/c2_state", root_dir);
    rm_tree(stdir);
    mkdir(stdir, 0755);

    struct mock_env env;
    mock_env_init(&env, sdir, 2, 1, 3);
    mock_obj_build(&env.objs[0], 10, 4, 2, 65536, 300000, 5, 3, 0x3fu, 404);
    mock_obj_build(&env.objs[1], 11, 4, 2, 65536, 320000, 5, 3, 0x3fu, 505);
    env.nobjs = 2;
    mock_put_local(&env, &env.objs[0], 1);
    mock_put_local(&env, &env.objs[1], 1);

    /* First start: records store_id ID1 into stdir/chunk_stores; both chunks already present. */
    struct sfill_ops sops = make_sfill_ops(&env);
    struct sreconcile_ops rops = make_rec_ops(&env);
    struct sfill *sf = sfill_new_with_ops(&sops);
    struct sreconcile *rec = sreconcile_new(sf, &rops, stdir);
    sreconcile_on_start_ex(rec);
    CHECK(wait_passes_ok(rec, 1, 10000), "c2a: initial pass did not complete");
    sreconcile_stop(rec);
    sfill_free(sf);
    sreconcile_free(rec);

    /* Simulate store replaced with another store id ID2 (empty fresh store) and restart. */
    chunk_be_close(env.be);
    env.be = NULL;
    rm_tree(sdir);
    uint8_t new_id[16];
    CHECK(chunk_store_init(sdir, NULL, new_id) == 0, "c2b: re-init store");
    const char *paths[1] = {sdir};
    CHECK(chunk_be_open(&env.be, paths, 1, NULL) == 0, "c2b: reopen store");
    CHECK(chunk_be_see_epoch(env.be, 2, env.vol_epoch) == 0, "c2b: see_epoch");

    /* Plant obj 0 on the new store too: because the store id changed, EVEN obj 0 must be treated as missing
     * (force_write = true) and rewritten! */
    mock_put_local(&env, &env.objs[0], 1);
    ino_t old_ino0 = chunk_inode(sdir, 2, env.objs[0].oid, 1);

    sf = sfill_new_with_ops(&sops);
    rec = sreconcile_new(sf, &rops, stdir);
    sreconcile_on_start_ex(rec);
    CHECK(wait_passes_ok(rec, 1, 10000), "c2b: replacement-store reconcile did not complete");

    struct sreconcile_stats rst;
    sreconcile_get_stats(rec, &rst);
    CHECK(rst.store_id_mismatches == 1, "c2b: expected store_id_mismatches==1, got %" PRIu64, rst.store_id_mismatches);
    CHECK(rst.filled == 2, "c2b: expected both objects refilled on new store id, got %" PRIu64, rst.filled);
    CHECK(chunk_exists_on_disk(sdir, 2, env.objs[0].oid, 1) && chunk_exists_on_disk(sdir, 2, env.objs[1].oid, 1),
          "c2b: missing refilled chunk(s)");
    CHECK(chunk_inode(sdir, 2, env.objs[0].oid, 1) != old_ino0,
          "c2b: chunk on mismatched-id store was not rewritten");

    /* Subsequent return with the same new store id: 0 mismatches, 0 refills (all already present). */
    sreconcile_on_return(rec);
    CHECK(wait_passes_ok(rec, 2, 10000), "c2c: second pass did not complete");
    sreconcile_get_stats(rec, &rst);
    CHECK(rst.store_id_mismatches == 1, "c2c: mismatch logged more than once (%" PRIu64 ")", rst.store_id_mismatches);
    if (atomic_load(&sreconcile_mutant) == SRECONCILE_MUT_NONE)
        CHECK(rst.filled == 2, "c2c: unexpected refill on intact return (filled=%" PRIu64 ")", rst.filled);

    sreconcile_stop(rec);
    sfill_free(sf);
    sreconcile_free(rec);
    mock_env_free(&env);
}

/* ------------------------------------------------------------------ Case 3: store not answering (DOWN) */

static void test_store_down(void)
{
    char sdir[PATH_MAX + 64], stdir[PATH_MAX + 64];
    snprintf(sdir, sizeof sdir, "%s/c3_store", root_dir);
    snprintf(stdir, sizeof stdir, "%s/c3_state", root_dir);
    rm_tree(stdir);
    mkdir(stdir, 0755);

    struct mock_env env;
    mock_env_init(&env, sdir, 3, 0, 1);
    mock_obj_build(&env.objs[0], 20, 4, 2, 65536, 260000, 5, 1, 0x3fu, 606);
    env.nobjs = 1;
    env.store_state_override[0] = CHUNK_STORE_DOWN_MISSING;
    env.use_disk_override[0] = true;
    env.store_disk_ok_override[0] = false;

    struct sfill_ops sops = make_sfill_ops(&env);
    struct sreconcile_ops rops = make_rec_ops(&env);
    struct sfill *sf = sfill_new_with_ops(&sops);
    struct sreconcile *rec = sreconcile_new(sf, &rops, stdir);

    sreconcile_on_start_ex(rec);
    uint64_t t0 = now_ms();
    struct sreconcile_stats rst;
    for (;;) {
        sreconcile_get_stats(rec, &rst);
        if (rst.store_down_events >= 1)
            break;
        if (now_ms() - t0 > 5000)
            break;
        sleep_ms(10);
    }
    /* Wait briefly to confirm all-down backs off without retry-storming OBJLIST or sfill. */
    sleep_ms(80);
    sreconcile_get_stats(rec, &rst);
    pthread_mutex_lock(&env.mu);
    uint32_t ol_while_down = env.objlist_calls;
    pthread_mutex_unlock(&env.mu);
    CHECK(rst.store_down_events == 1, "c3: expected store_down_events==1 (logged once), got %" PRIu64,
          rst.store_down_events);
    CHECK(ol_while_down == 0, "c3: all-down store must not spam OBJLIST (got %" PRIu32 " calls)", ol_while_down);
    CHECK(rst.passes_ok == 0, "c3: down store must not succeed (passes_ok=%" PRIu64 ")", rst.passes_ok);

    /* Bring store back UP -> reconcile retries and fills cleanly! */
    pthread_mutex_lock(&env.mu);
    env.store_state_override[0] = -1;
    env.use_disk_override[0] = false;
    pthread_mutex_unlock(&env.mu);
    sreconcile_on_return(rec);
    CHECK(wait_passes_ok(rec, 1, 10000), "c3: recovery after store UP did not complete");
    if (atomic_load(&sreconcile_mutant) == SRECONCILE_MUT_NONE)
        CHECK(chunk_exists_on_disk(sdir, 3, env.objs[0].oid, 0), "c3: chunk not filled after store returned UP");

    sreconcile_stop(rec);
    sfill_free(sf);
    sreconcile_free(rec);
    mock_env_free(&env);
}

/* ------------------------------------------------------------------ Case 5: review edge cases */

static bool file_contains(const char *path, const char *needle)
{
    FILE *f = fopen(path, "r");
    if (!f)
        return false;
    char buf[4096];
    size_t n = fread(buf, 1, sizeof buf - 1, f);
    fclose(f);
    buf[n] = '\0';
    return strstr(buf, needle) != NULL;
}

static void test_review_edge_cases(void)
{
    if (atomic_load(&sreconcile_mutant) != SRECONCILE_MUT_NONE)
        return;

    /* 5a. Replacement store_id MUST NOT persist to chunk_stores if the reconcile pass fails; it must persist only
     * after the retry pass finishes cleanly (and must keep forcing refill on retry). Also tests multi-page OBJLIST
     * streaming (objlist_max_page = 2 across 4 objects) and pre-enqueue GC filter (object 3 on gc_pending is skipped). */
    {
        char sdir[PATH_MAX + 64], stdir[PATH_MAX + 64], cspath[PATH_MAX + 128];
        snprintf(sdir, sizeof sdir, "%s/c5a_store", root_dir);
        snprintf(stdir, sizeof stdir, "%s/c5a_state", root_dir);
        snprintf(cspath, sizeof cspath, "%s/chunk_stores", stdir);
        rm_tree(stdir);
        mkdir(stdir, 0755);

        struct mock_env env;
        mock_env_init(&env, sdir, 5, 1, 4);
        uint8_t id1[16];
        memcpy(id1, chunk_store_id(chunk_be_store(env.be, 0)), 16);
        char id1_hex[33];
        chunk_oid_hex(id1, id1_hex);

        for (int i = 0; i < 4; i++)
            mock_obj_build(&env.objs[i], (uint8_t)(30 + i), 4, 2, 65536, 200000, 5, 4, 0x3fu, 700 + i);
        env.nobjs = 4;
        /* Mark obj 3 as already on committed MK_GC pending list -> must NOT be resurrected during reconcile. */
        memcpy(env.gc_pending[0], env.objs[3].oid, 16);
        env.ngc_pending = 1;
        env.objlist_max_page = 2; /* force multi-page streaming */

        struct sfill_ops sops = make_sfill_ops(&env);
        struct sreconcile_ops rops = make_rec_ops(&env);
        struct sfill *sf = sfill_new_with_ops(&sops);
        struct sreconcile *rec = sreconcile_new(sf, &rops, stdir);

        /* First start records id1. */
        sreconcile_on_start_ex(rec);
        CHECK(wait_passes_ok(rec, 1, 10000), "c5a: initial pass did not complete");
        CHECK(file_contains(cspath, id1_hex), "c5a: chunk_stores missing id1 after initial pass");
        CHECK(chunk_exists_on_disk(sdir, 5, env.objs[0].oid, 1) &&
                  chunk_exists_on_disk(sdir, 5, env.objs[1].oid, 1) &&
                  chunk_exists_on_disk(sdir, 5, env.objs[2].oid, 1),
              "c5a: multi-page streaming missed an object");
        CHECK(!chunk_exists_on_disk(sdir, 5, env.objs[3].oid, 1),
              "c5a: object on committed MK_GC pending list was resurrected");

        sreconcile_stop(rec);
        sfill_free(sf);
        sreconcile_free(rec);

        /* Replace store with id2, plant obj 0 with old inode, and fail OBJLIST once so pass 1 fails. */
        chunk_be_close(env.be);
        env.be = NULL;
        rm_tree(sdir);
        uint8_t id2[16];
        CHECK(chunk_store_init(sdir, NULL, id2) == 0, "c5a: reinit store");
        char id2_hex[33];
        chunk_oid_hex(id2, id2_hex);
        const char *paths[1] = {sdir};
        CHECK(chunk_be_open(&env.be, paths, 1, NULL) == 0, "c5a: reopen store");
        CHECK(chunk_be_see_epoch(env.be, 5, env.vol_epoch) == 0, "c5a: see_epoch");
        mock_put_local(&env, &env.objs[0], 1);
        ino_t planted_ino = chunk_inode(sdir, 5, env.objs[0].oid, 1);

        env.fail_objlist_once = 2; /* fail first 2 attempts so we can inspect chunk_stores mid-failure */
        sf = sfill_new_with_ops(&sops);
        rec = sreconcile_new(sf, &rops, stdir);
        sreconcile_on_start_ex(rec);

        /* Wait for at least one failed pass while mismatch was detected. */
        uint64_t t0 = now_ms();
        struct sreconcile_stats rst;
        for (;;) {
            sreconcile_get_stats(rec, &rst);
            if (rst.passes_err >= 1)
                break;
            if (now_ms() - t0 > 5000)
                break;
            sleep_ms(5);
        }
        /* While pass has NOT succeeded yet, chunk_stores MUST still carry id1 (not id2). */
        if (rst.passes_ok == 0) {
            CHECK(file_contains(cspath, id1_hex) && !file_contains(cspath, id2_hex),
                  "c5a: replacement store_id persisted before reconcile completed");
        }
        /* Once retries succeed, obj 0 must have been rewritten (replace_mask survived retry!) and id2 persisted. */
        CHECK(wait_passes_ok(rec, 1, 10000), "c5a: replacement retry did not complete");
        CHECK(chunk_inode(sdir, 5, env.objs[0].oid, 1) != planted_ino,
              "c5a: replace_mask lost across retry (obj 0 was not rewritten)");
        CHECK(file_contains(cspath, id2_hex), "c5a: id2 not persisted after clean completion");

        sreconcile_stop(rec);
        sfill_free(sf);
        sreconcile_free(rec);
        mock_env_free(&env);
    }

    /* 5b. False-orphan suppression: (i) local_exists==false (concurrent GC delete) suppresses orphan report;
     * (ii) gc_truncated==true suppresses orphan report. */
    {
        char sdir[PATH_MAX + 64], stdir[PATH_MAX + 64];
        snprintf(sdir, sizeof sdir, "%s/c5b_store", root_dir);
        snprintf(stdir, sizeof stdir, "%s/c5b_state", root_dir);
        rm_tree(stdir);
        mkdir(stdir, 0755);

        struct mock_env env;
        mock_env_init(&env, sdir, 6, 0, 1);
        struct mock_obj extra;
        mock_obj_build(&extra, 95, 4, 2, 65536, 200000, 5, 1, 0x3fu, 959);
        mock_put_local(&env, &extra, 0);
        env.hide_local_exists = true;

        struct sfill_ops sops = make_sfill_ops(&env);
        struct sreconcile_ops rops = make_rec_ops(&env);
        struct sfill *sf = sfill_new_with_ops(&sops);
        struct sreconcile *rec = sreconcile_new(sf, &rops, stdir);

        sreconcile_on_start_ex(rec);
        CHECK(wait_passes_ok(rec, 1, 10000), "c5b(i): pass did not complete");
        struct sreconcile_stats rst;
        sreconcile_get_stats(rec, &rst);
        CHECK(rst.orphans_reported == 0, "c5b(i): vanished chunk reported as false orphan (%" PRIu64 ")",
              rst.orphans_reported);

        /* Now expose local_exists, but mark gc_pending truncated -> still 0 false orphans. */
        pthread_mutex_lock(&env.mu);
        env.hide_local_exists = false;
        env.gc_truncated = true;
        pthread_mutex_unlock(&env.mu);
        sreconcile_on_return(rec);
        CHECK(wait_passes_ok(rec, 2, 10000), "c5b(ii): pass did not complete");
        sreconcile_get_stats(rec, &rst);
        CHECK(rst.orphans_reported == 0, "c5b(ii): truncated gc_pending reported false orphan (%" PRIu64 ")",
              rst.orphans_reported);

        sreconcile_stop(rec);
        sfill_free(sf);
        sreconcile_free(rec);
        mock_obj_free(&extra);
        mock_env_free(&env);
    }

    /* 5c. Partial store down (2 stores: store 1 DOWN, store 0 UP): store 0's objects still reconcile, store 1's
     * objects are counted in missing_on_down_store without sfill errors, and once store 1 comes UP everything
     * finishes cleanly. */
    {
        char sdir0[PATH_MAX + 64], sdir1[PATH_MAX + 64], stdir[PATH_MAX + 64];
        snprintf(sdir0, sizeof sdir0, "%s/c5c_store0", root_dir);
        snprintf(sdir1, sizeof sdir1, "%s/c5c_store1", root_dir);
        snprintf(stdir, sizeof stdir, "%s/c5c_state", root_dir);
        rm_tree(stdir);
        mkdir(stdir, 0755);

        const char *paths[2] = {sdir0, sdir1};
        struct mock_env env;
        mock_env_init_n(&env, paths, 2, 7, 2, 1);

        /* Build objects until we have at least one placed on store 0 and one placed on store 1. */
        int got0 = -1, got1 = -1;
        for (uint8_t tag = 40; tag < 80 && (got0 < 0 || got1 < 0); tag++) {
            struct mock_obj tmp;
            mock_obj_build(&tmp, tag, 4, 2, 65536, 200000, 5, 1, 0x3fu, 1000 + tag);
            int p = chunk_be_place(env.be, tmp.oid, 2);
            if (p == 0 && got0 < 0) {
                env.objs[env.nobjs++] = tmp;
                got0 = (int)env.nobjs - 1;
            } else if (p == 1 && got1 < 0) {
                env.objs[env.nobjs++] = tmp;
                got1 = (int)env.nobjs - 1;
            } else {
                mock_obj_free(&tmp);
            }
        }
        CHECK(got0 >= 0 && got1 >= 0, "c5c: failed to place objects across both stores");

        env.store_state_override[1] = CHUNK_STORE_DOWN_MISSING;
        env.use_disk_override[1] = true;
        env.store_disk_ok_override[1] = false;

        struct sfill_ops sops = make_sfill_ops(&env);
        struct sreconcile_ops rops = make_rec_ops(&env);
        struct sfill *sf = sfill_new_with_ops(&sops);
        struct sreconcile *rec = sreconcile_new(sf, &rops, stdir);

        sreconcile_on_start_ex(rec);
        uint64_t t0 = now_ms();
        struct sreconcile_stats rst;
        for (;;) {
            sreconcile_get_stats(rec, &rst);
            if (rst.missing_on_down_store >= 1 && rst.filled >= 1)
                break;
            if (now_ms() - t0 > 5000)
                break;
            sleep_ms(10);
        }
        CHECK(rst.filled >= 1, "c5c: UP store 0 object was not filled while store 1 was DOWN");
        CHECK(rst.missing_on_down_store >= 1, "c5c: DOWN store 1 object not counted in missing_on_down_store");
        CHECK(rst.failed == 0, "c5c: DOWN store 1 object must not cause sfill failures (got %" PRIu64 ")", rst.failed);
        CHECK(rst.passes_ok == 0, "c5c: partial-down pass must not report passes_ok");

        /* Wait 100 ms (> 4x retry_ms): parked job must NOT re-stream OBJLIST while store 1 stays DOWN. */
        sleep_ms(100);
        pthread_mutex_lock(&env.mu);
        uint32_t ol_parked = env.objlist_calls;
        pthread_mutex_unlock(&env.mu);
        CHECK(ol_parked == 1, "c5c: partial-down job re-streamed OBJLIST while store 1 stayed down (%" PRIu32 " calls)",
              ol_parked);

        /* Bring store 1 back UP (no explicit on_return!): lightweight store poll wakes job and finishes cleanly. */
        pthread_mutex_lock(&env.mu);
        env.store_state_override[1] = -1;
        env.use_disk_override[1] = false;
        pthread_mutex_unlock(&env.mu);
        CHECK(wait_passes_ok(rec, 1, 10000), "c5c: recovery after store 1 UP did not complete");
        CHECK(chunk_exists_on_disk(sdir0, 7, env.objs[got0].oid, 2) &&
                  chunk_exists_on_disk(sdir1, 7, env.objs[got1].oid, 2),
              "c5c: both stores must hold their chunks after recovery");

        sreconcile_stop(rec);
        sfill_free(sf);
        sreconcile_free(rec);
        mock_env_free(&env);
    }

    /* 5d. Damaged chunk_stores (bad XXH64 checksum): must NOT be silently overwritten. */
    {
        char sdir[PATH_MAX + 64], stdir[PATH_MAX + 64], cspath[PATH_MAX + 128];
        snprintf(sdir, sizeof sdir, "%s/c5d_store", root_dir);
        snprintf(stdir, sizeof stdir, "%s/c5d_state", root_dir);
        snprintf(cspath, sizeof cspath, "%s/chunk_stores", stdir);
        rm_tree(stdir);
        mkdir(stdir, 0755);

        const char *corrupt_marker = "0102030405060708090a0b0c0d0e0f10 /bogus/path\nXXH64 deadbeefdeadbeef\n";
        FILE *f = fopen(cspath, "w");
        CHECK(f != NULL, "c5d: create corrupt chunk_stores");
        if (f) {
            fwrite(corrupt_marker, 1, strlen(corrupt_marker), f);
            fclose(f);
        }

        struct mock_env env;
        mock_env_init(&env, sdir, 8, 0, 1);
        struct sfill_ops sops = make_sfill_ops(&env);
        struct sreconcile_ops rops = make_rec_ops(&env);
        struct sfill *sf = sfill_new_with_ops(&sops);
        struct sreconcile *rec = sreconcile_new(sf, &rops, stdir);

        sreconcile_on_start_ex(rec);
        CHECK(wait_passes_ok(rec, 1, 10000), "c5d: pass did not complete");
        CHECK(file_contains(cspath, "deadbeefdeadbeef"),
              "c5d: damaged chunk_stores was silently overwritten!");

        sreconcile_stop(rec);
        sfill_free(sf);
        sreconcile_free(rec);
        mock_env_free(&env);
    }

    /* 5e. disk_diverged while running: logs mismatch once and waits for restart (never force-fills into old handle). */
    {
        char sdir[PATH_MAX + 64], stdir[PATH_MAX + 64];
        snprintf(sdir, sizeof sdir, "%s/c5e_store", root_dir);
        snprintf(stdir, sizeof stdir, "%s/c5e_state", root_dir);
        rm_tree(stdir);
        mkdir(stdir, 0755);

        struct mock_env env;
        mock_env_init(&env, sdir, 9, 0, 1);
        mock_obj_build(&env.objs[0], 50, 4, 2, 65536, 200000, 5, 1, 0x3fu, 5000);
        env.nobjs = 1;
        env.use_id_override = true;
        memset(env.store_live_id_override, 0x11, 16);
        memset(env.store_disk_id_override, 0x22, 16);

        struct sfill_ops sops = make_sfill_ops(&env);
        struct sreconcile_ops rops = make_rec_ops(&env);
        struct sfill *sf = sfill_new_with_ops(&sops);
        struct sreconcile *rec = sreconcile_new(sf, &rops, stdir);

        sreconcile_on_start_ex(rec);
        uint64_t t0 = now_ms();
        struct sreconcile_stats rst;
        for (;;) {
            sreconcile_get_stats(rec, &rst);
            if (rst.store_id_mismatches >= 1)
                break;
            if (now_ms() - t0 > 5000)
                break;
            sleep_ms(10);
        }
        sleep_ms(80);
        sreconcile_get_stats(rec, &rst);
        pthread_mutex_lock(&env.mu);
        uint32_t ol_div = env.objlist_calls;
        pthread_mutex_unlock(&env.mu);
        CHECK(rst.store_id_mismatches == 1, "c5e: expected 1 mismatch log, got %" PRIu64, rst.store_id_mismatches);
        CHECK(rst.filled == 0 && ol_div == 0,
              "c5e: disk_diverged must wait for restart without force-filling (filled=%" PRIu64 " ol=%" PRIu32 ")",
              rst.filled, ol_div);

        sreconcile_stop(rec);
        sfill_free(sf);
        sreconcile_free(rec);
        mock_env_free(&env);
    }

    /* 5f. Concurrent note added during notify_holder must NOT be cleared by the in-flight nudge. */
    {
        char sdir[PATH_MAX + 64], stdir[PATH_MAX + 64];
        snprintf(sdir, sizeof sdir, "%s/c5f_store", root_dir);
        snprintf(stdir, sizeof stdir, "%s/c5f_state", root_dir);
        rm_tree(stdir);
        mkdir(stdir, 0755);

        struct mock_env env;
        mock_env_init(&env, sdir, 10, 2, 1);
        env.holder_up = true;
        env.note_on_first_nudge = true;
        memset(env.concurrent_note_oid, 0xbb, 16);

        sreconcile_test_reset_missing();
        struct sfill_ops sops = make_sfill_ops(&env);
        struct sreconcile_ops rops = make_rec_ops(&env);
        struct sfill *sf = sfill_new_with_ops(&sops);
        struct sreconcile *rec = sreconcile_new(sf, &rops, stdir);

        uint8_t oid1[16];
        memset(oid1, 0xaa, 16);
        sreconcile_note_missing(10, oid1, 2);

        /* Kick reconcile_main: first nudge delivers oid1 and injects oid2 concurrently -> oid2 must stay pending
         * and get nudged on a second drain (never lost by clearing all entries for (vol, pos)). */
        sreconcile_kick_noted(rec);
        uint64_t t0 = now_ms();
        for (;;) {
            pthread_mutex_lock(&env.mu);
            uint32_t nudged = env.holder_nudged;
            pthread_mutex_unlock(&env.mu);
            if (nudged >= 2 || now_ms() - t0 > 5000)
                break;
            if (nudged >= 1)
                sreconcile_kick_noted(rec);
            sleep_ms(10);
        }
        pthread_mutex_lock(&env.mu);
        uint32_t nudged_final = env.holder_nudged;
        pthread_mutex_unlock(&env.mu);
        CHECK(nudged_final == 2, "c5f: concurrent note was lost! expected 2 nudges, got %" PRIu32, nudged_final);

        sreconcile_stop(rec);
        sfill_free(sf);
        sreconcile_free(rec);
        mock_env_free(&env);
    }

    /* 5g. Mid-pass sreconcile_stop_pos cancels remaining queued/in-flight tasks cleanly. */
    {
        char sdir[PATH_MAX + 64], stdir[PATH_MAX + 64];
        snprintf(sdir, sizeof sdir, "%s/c5g_store", root_dir);
        snprintf(stdir, sizeof stdir, "%s/c5g_state", root_dir);
        rm_tree(stdir);
        mkdir(stdir, 0755);

        struct mock_env env;
        mock_env_init(&env, sdir, 11, 1, 1);
        env.slow_read = true;
        env.objlist_max_page = 2;
        for (int i = 0; i < 6; i++)
            mock_obj_build(&env.objs[i], (uint8_t)(60 + i), 4, 2, 65536, 200000, 5, 1, 0x3fu, 6000 + i);
        env.nobjs = 6;

        struct sfill_ops sops = make_sfill_ops(&env);
        struct sreconcile_ops rops = make_rec_ops(&env);
        struct sfill *sf = sfill_new_with_ops(&sops);
        struct sreconcile *rec = sreconcile_new(sf, &rops, stdir);

        sreconcile_on_start_ex(rec);
        /* Wait until pass starts queuing, then transfer position away (stop_pos + held_whole=false). */
        uint64_t t0 = now_ms();
        for (;;) {
            struct sreconcile_stats rst;
            sreconcile_get_stats(rec, &rst);
            if (rst.missing_queued > 0 || now_ms() - t0 > 5000)
                break;
            sleep_ms(5);
        }
        pthread_mutex_lock(&env.mu);
        env.held_whole = false;
        pthread_mutex_unlock(&env.mu);
        sreconcile_stop_pos(rec, 11, 1);

        sreconcile_stop(rec);
        sfill_free(sf);
        sreconcile_free(rec);
        struct sreconcile_stats rst;
        /* Count written chunks on disk: must be strictly fewer than all 6 (cancelled mid-pass!). */
        int written = 0;
        for (int i = 0; i < 6; i++)
            if (chunk_exists_on_disk(sdir, 11, env.objs[i].oid, 1))
                written++;
        (void)rst;
        CHECK(written < 6, "c5g: stop_pos did not cancel mid-pass reconcile (all %d/6 chunks written)", written);
        mock_env_free(&env);
    }
}

/* ------------------------------------------------------------------ Case 4: sreconcile_note_missing + drain */

static void test_note_missing_and_stop(void)
{
    char sdir[PATH_MAX + 64], stdir[PATH_MAX + 64];
    snprintf(sdir, sizeof sdir, "%s/c4_store", root_dir);
    snprintf(stdir, sizeof stdir, "%s/c4_state", root_dir);
    rm_tree(stdir);
    mkdir(stdir, 0755);

    struct mock_env env;
    mock_env_init(&env, sdir, 4, 3, 2);
    env.holder_up = false;

    struct sfill_ops sops = make_sfill_ops(&env);
    struct sreconcile_ops rops = make_rec_ops(&env);
    struct sfill *sf = sfill_new_with_ops(&sops);
    struct sreconcile *rec = sreconcile_new(sf, &rops, stdir);

    sreconcile_test_reset_missing();
    uint8_t oid[16] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    sreconcile_note_missing(4, oid, 3);
    CHECK(sreconcile_missing_count() == 1, "c4: missing_count != 1");

    /* While holder is DOWN, drain keeps it queued (0 nudges). */
    sreconcile_drain_noted(rec);
    CHECK(env.holder_nudged == 0, "c4: nudged while holder DOWN");

    /* Once holder returns UP, drain nudges it exactly once and clears pending entry. */
    env.holder_up = true;
    sreconcile_drain_noted(rec);
    sreconcile_drain_noted(rec);
    CHECK(env.holder_nudged == 1, "c4: expected exactly 1 nudge on return, got %" PRIu32, env.holder_nudged);

    sreconcile_stop_pos(rec, 4, 3);
    sreconcile_stop(rec);
    sfill_free(sf);
    sreconcile_free(rec);
    mock_env_free(&env);
}

int main(int argc, char **argv)
{
    const char *mutant = NULL;
    const char *scratch = "/tmp";
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mutant") == 0 && i + 1 < argc)
            mutant = argv[++i];
        else
            scratch = argv[i];
    }
    if (mutant) {
        int m = sreconcile_mutant_parse(mutant);
        if (m < 0) {
            fprintf(stderr, "test_sreconcile: unknown mutant '%s'\n", mutant);
            return 2;
        }
        sreconcile_set_mutant(m);
    }

    snprintf(root_dir, sizeof root_dir, "%s/tsdb_sreconcile_%d", scratch, (int)getpid());
    rm_tree(root_dir);
    if (mkdir(root_dir, 0755) != 0 && errno != EEXIST)
        return 2;

    sreconcile_test_retry_ms = 20;
    sfill_test_retry_ms = 20;

    printf("test_sreconcile%s%s\n", mutant ? " mutant " : "", mutant ? mutant : "");
    test_diff_and_two_way();
    test_store_id_and_empty();
    test_store_down();
    test_note_missing_and_stop();
    test_review_edge_cases();

    rm_tree(root_dir);
    if (fails) {
        fprintf(stderr, "test_sreconcile: %d failure(s)\n", fails);
        return 1;
    }
    printf("test_sreconcile: PASS\n");
    return 0;
}
#endif
