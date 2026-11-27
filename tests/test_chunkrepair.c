/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Unit tests and mutant suite for volume-leader chunk repair (server/chunkrepair.[ch]; R12, r12-repair).
 *
 * Covers:
 *   1. Deduplication of queued & active `(volume, oid, pos)`, bounded queue drop (`dropped`), and strict
 *      one-repair-per-store serialization without head-of-line blocking across distinct stores.
 *   2. Data and parity position repair from `k` verified survivors against real in-process `chunksrv` stores,
 *      preserving `hdr.epoch == writer_epoch < fence_epoch` (catches `repairownepoch`), and invoking `notify_reopen`.
 *   3. Second rotten survivor alongside damaged target (skips bad survivor via `obj.sums`, repairs target bit-exact;
 *      catches `repairnosums`) and `< k` good (`m + 1` bad -> `unrecoverable`, writes nothing).
 *   4. Deposed leader fenced by storage server (`CHUNK_ERR_FENCED`, `fenced` counter, old chunk untouched).
 *   5. Concurrent drop / MK_GC race via `pause_before_put`: object retired/deleted before put -> `not_live`,
 *      zero chunks resurrected (catches `repairnolive`).
 *   6. Read-path integration (`chunkpeer` `on_damaged` callback triggers `chunkrepair` on bad survivor read).
 */
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "chunk.h"
#include "chunk_priv.h"
#include "chunkpeer.h"
#include "chunkrepair.h"
#include "chunksrv.h"
#include "internal.h"
#include "tsdb.h"

static int fails;
#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);                                               \
            fails++;                                                                                                   \
        }                                                                                                              \
    } while (0)

#ifndef TSDB_EC
int main(void)
{
    printf("test_chunkrepair: SKIPPED (no erasure codec)\n");
    return 0;
}
#else

static char S[PATH_MAX - 128];
static int seq;

static void fresh(const char *tag, char *out, size_t cap)
{
    snprintf(out, cap, "%s/%.32s_%d", S, tag, ++seq);
    CHECK(mkdir(out, 0755) == 0);
}

/* ---- Test 1: dedupe, bounded queue, one-repair-per-store serialization ---- */
struct ser_ctx {
    pthread_mutex_t mu;
    int active_on[8];
    int max_on[8];
    int total_started[8];
    atomic_bool hold_node1;
    atomic_int node1_entered;
    atomic_int node2_entered;
};

static int ser_lookup(void *arg, uint32_t volume, const uint8_t oid[16], struct chunkrepair_obj *out)
{
    (void)volume;
    struct ser_ctx *ctx = arg;
    (void)ctx;
    memset(out, 0, sizeof *out);
    memcpy(out->oid, oid, 16);
    CHECK(chunk_geom_make(&out->g, 2, 1, 4096, 8192) == 0);
    out->writer_epoch = 1;
    out->nsums = chunk_sums_total(&out->g);
    out->sums = calloc((size_t)out->nsums, sizeof(uint64_t));
    return 0;
}

static int ser_view(void *arg, uint32_t volume, struct chunkrepair_vol_view *out)
{
    (void)volume;
    struct ser_ctx *ctx = arg;
    memset(out, 0, sizeof *out);
    out->fence_epoch = 2;
    out->k = 2;
    out->m = 1;
    out->nstripe = 3;
    out->stripe[0] = 1;
    out->stripe[1] = 2;
    out->stripe[2] = 3;
    out->is_leader = true;
    (void)ctx;
    return 0;
}

static int ser_read(void *arg, uint32_t node, uint32_t volume, uint64_t epoch, const uint8_t vuuid[16],
                    const uint8_t oid[16], unsigned pos, uint64_t off, void *buf, size_t len)
{
    (void)node, (void)volume, (void)epoch, (void)vuuid, (void)oid, (void)pos, (void)off;
    struct ser_ctx *ctx = arg;
    /* Observe per-target-node concurrency via oid[0] (encodes target node in this test). */
    unsigned target = oid[0] < 8 ? oid[0] : 0;
    if (pos == 2) { /* called once per repair */
        pthread_mutex_lock(&ctx->mu);
        ctx->active_on[target]++;
        if (ctx->active_on[target] > ctx->max_on[target])
            ctx->max_on[target] = ctx->active_on[target];
        ctx->total_started[target]++;
        pthread_mutex_unlock(&ctx->mu);
        if (target == 1) {
            atomic_fetch_add(&ctx->node1_entered, 1);
            while (atomic_load(&ctx->hold_node1))
                usleep(5000);
        } else if (target == 2) {
            atomic_fetch_add(&ctx->node2_entered, 1);
        }
        pthread_mutex_lock(&ctx->mu);
        ctx->active_on[target]--;
        pthread_mutex_unlock(&ctx->mu);
    }
    memset(buf, 0, len);
    return CHUNK_ST_DOWN; /* fast exit as unavailable after concurrency check */
}

static int ser_put(void *arg, uint32_t node, const struct cs_put_meta *m, int fd, uint64_t base, uint64_t sum,
                   bool skip_live)
{
    (void)arg, (void)node, (void)m, (void)fd, (void)base, (void)sum, (void)skip_live;
    return TSDB_ERR_UNAVAILABLE;
}

static void test_queue_and_serialization(void)
{
    struct ser_ctx ctx;
    memset(&ctx, 0, sizeof ctx);
    pthread_mutex_init(&ctx.mu, NULL);
    atomic_store(&ctx.hold_node1, true);

    struct chunkrepair_cfg cfg = {.nworkers = 4, .queue_max = 3, .max_retries = -1};
    snprintf(cfg.stage_dir, sizeof cfg.stage_dir, "%s", S);
    struct chunkrepair_ops ops = {
        .lookup_live = ser_lookup,
        .vol_view = ser_view,
        .read_cell = ser_read,
        .check_and_put = ser_put,
        .arg = &ctx,
    };
    struct chunkrepair *cr = chunkrepair_new(&cfg, &ops);
    CHECK(cr != NULL);

    uint8_t oid1[16] = {1, 1}, oid2[16] = {1, 2}, oid3[16] = {1, 3}, oid4[16] = {1, 4}, oid5[16] = {1, 5};
    uint8_t oid_n2[16] = {2, 1};

    /* Enqueue oid1 targeting node 1 (starts running and holds on hold_node1). */
    CHECK(chunkrepair_enqueue(cr, 1, oid1, 0, 1) == 0);
    for (int i = 0; i < 2000 && atomic_load(&ctx.node1_entered) == 0; i++)
        usleep(5000);
    CHECK(atomic_load(&ctx.node1_entered) == 1);

    /* Duplicate enqueue of active oid1 must be deduped (returns 0, doesn't consume queue slot). */
    CHECK(chunkrepair_enqueue(cr, 1, oid1, 0, 1) == 0);

    /* Fill queue (max 3) with oid2, oid3, oid4 all targeting node 1; duplicate oid2 must dedupe. */
    CHECK(chunkrepair_enqueue(cr, 1, oid2, 0, 1) == 0);
    CHECK(chunkrepair_enqueue(cr, 1, oid2, 0, 1) == 0);
    CHECK(chunkrepair_enqueue(cr, 1, oid3, 0, 1) == 0);
    /* Enqueue oid_n2 targeting node 2: another worker must pick it up immediately even while node 1 is blocked! */
    CHECK(chunkrepair_enqueue(cr, 1, oid_n2, 1, 2) == 0);
    for (int i = 0; i < 2000 && atomic_load(&ctx.node2_entered) == 0; i++)
        usleep(5000);
    CHECK(atomic_load(&ctx.node2_entered) == 1);

    /* Fill remaining 3rd queue slot with oid4, then 4th queued item oid5 must be DROPPED (TSDB_ERR_UNAVAILABLE). */
    CHECK(chunkrepair_enqueue(cr, 1, oid4, 0, 1) == 0);
    CHECK(chunkrepair_enqueue(cr, 1, oid5, 0, 1) == TSDB_ERR_UNAVAILABLE);

    /* Release node 1 and wait for all queued repairs to drain. */
    atomic_store(&ctx.hold_node1, false);
    chunkrepair_wait_idle(cr);

    struct chunkrepair_stats st;
    chunkrepair_get_stats(cr, &st);
    CHECK(ctx.max_on[1] == 1); /* strict one-repair-per-store serialization */
    CHECK(ctx.total_started[1] == 4);
    CHECK(ctx.total_started[2] == 1);
    CHECK(st.deduped >= 2);
    CHECK(st.results[CHUNKREPAIR_RES_DROPPED] == 1);
    CHECK(st.queue_depth == 0);

    chunkrepair_free(cr);
    pthread_mutex_destroy(&ctx.mu);
}

/* ---- Real 6-node 4+2 chunksrv harness for Tests 2-6 ---- */
#define NSTORES 6
struct repair_env {
    struct chunksrv *srv[NSTORES];
    char sdir[NSTORES][PATH_MAX + 32];
    uint32_t volume;
    uint8_t vuuid[16];
    uint8_t oid[16];
    chunk_geom g;
    uint8_t *obj_data;
    uint64_t *sums;
    uint64_t nsums;
    uint64_t writer_epoch;
    atomic_uint_fast64_t fence_epoch;
    atomic_bool is_leader;
    atomic_bool is_live;
    pthread_mutex_t gc_mu;
    atomic_int reopen_calls;
    atomic_int put_attempts;
    atomic_int inject_absent_mask;   /* bit p set -> env_read_cell returns CHUNK_ST_ABSENT */
    atomic_int inject_down_countdown;/* >0: env_read_cell returns CHUNK_ST_DOWN and decrements */
};

static int env_lookup_live(void *arg, uint32_t volume, const uint8_t oid[16], struct chunkrepair_obj *out)
{
    struct repair_env *e = arg;
    if (!atomic_load(&e->is_leader))
        return TSDB_ERR_UNAVAILABLE;
    if (volume != e->volume || memcmp(oid, e->oid, 16) != 0)
        return TSDB_ERR_NOTFOUND;
    pthread_mutex_lock(&e->gc_mu);
    bool live = atomic_load(&e->is_live);
    pthread_mutex_unlock(&e->gc_mu);
    if (!live)
        return TSDB_ERR_NOTFOUND;
    memset(out, 0, sizeof *out);
    memcpy(out->vuuid, e->vuuid, 16);
    memcpy(out->oid, e->oid, 16);
    out->g = e->g;
    out->writer_epoch = e->writer_epoch;
    out->stripe_version = 1;
    out->nsums = e->nsums;
    out->sums = malloc((size_t)e->nsums * sizeof(uint64_t));
    if (!out->sums)
        return TSDB_ERR_NOMEM;
    memcpy(out->sums, e->sums, (size_t)e->nsums * sizeof(uint64_t));
    return 0;
}

static int env_vol_view(void *arg, uint32_t volume, struct chunkrepair_vol_view *out)
{
    struct repair_env *e = arg;
    if (volume != e->volume || !atomic_load(&e->is_leader))
        return TSDB_ERR_UNAVAILABLE;
    memset(out, 0, sizeof *out);
    out->fence_epoch = atomic_load(&e->fence_epoch);
    out->stripe_version = 1;
    out->k = e->g.k;
    out->m = e->g.m;
    out->nstripe = NSTORES;
    for (unsigned p = 0; p < NSTORES; p++)
        out->stripe[p] = p + 1; /* node ids 1..6 */
    out->is_leader = true;
    return 0;
}

static int env_read_cell(void *arg, uint32_t node, uint32_t volume, uint64_t epoch, const uint8_t vuuid[16],
                         const uint8_t oid[16], unsigned pos, uint64_t off, void *buf, size_t len)
{
    struct repair_env *e = arg;
    if (node < 1 || node > NSTORES)
        return CHUNK_ST_DOWN;
    if (pos < 31 && (atomic_load(&e->inject_absent_mask) & (1 << pos)))
        return CHUNK_ST_ABSENT;
    int dc = atomic_load(&e->inject_down_countdown);
    while (dc > 0) {
        if (atomic_compare_exchange_weak(&e->inject_down_countdown, &dc, dc - 1))
            return CHUNK_ST_DOWN;
    }
    struct cs_req r;
    memset(&r, 0, sizeof r);
    r.rid = 1;
    r.op = CS_OP_READ;
    r.volume = volume;
    r.epoch = epoch;
    memcpy(r.vuuid, vuuid, 16);
    memcpy(r.oid, oid, 16);
    r.pos = (uint16_t)pos;
    r.off = off;
    r.len = (uint32_t)len;
    int32_t rc = 0;
    uint8_t *body = NULL;
    size_t bl = 0;
    int trc = chunkpeer_local_call(e->srv[node - 1], 99, &r, &rc, &body, &bl);
    if (trc != 0) {
        free(body);
        return CHUNK_ST_DOWN;
    }
    int st = chunksrv_st(rc);
    if (st == CHUNK_ST_OK) {
        if (bl != len)
            st = CHUNK_ST_DAMAGED;
        else
            memcpy(buf, body, len);
    }
    free(body);
    return st < 0 ? CHUNK_ST_IOERR : st;
}

static int env_check_and_put(void *arg, uint32_t node, const struct cs_put_meta *m, int fd, uint64_t base, uint64_t sum,
                             bool skip_live)
{
    struct repair_env *e = arg;
    if (node < 1 || node > NSTORES)
        return TSDB_ERR_UNAVAILABLE;
    pthread_mutex_lock(&e->gc_mu);
    int rc = 0;
    if (!skip_live) {
        if (!atomic_load(&e->is_leader))
            rc = TSDB_ERR_UNAVAILABLE;
        else if (!atomic_load(&e->is_live))
            rc = TSDB_ERR_NOTFOUND;
    }
    if (rc == 0) {
        atomic_fetch_add(&e->put_attempts, 1);
        uint64_t answered = 0;
        rc = chunkpeer_local_put(e->srv[node - 1], 99, m, fd, base, &answered);
        if (rc == 0) {
            uint8_t enc[CS_PUT_META_MAX];
            size_t ml = cs_put_meta_encode(m, enc);
            if (answered != cs_put_answer_hash_len(enc, ml, sum))
                rc = TSDB_ERR_IO;
        }
    }
    pthread_mutex_unlock(&e->gc_mu);
    return rc;
}

static void env_notify_reopen(void *arg, uint32_t volume, const uint8_t oid[16])
{
    (void)volume, (void)oid;
    struct repair_env *e = arg;
    atomic_fetch_add(&e->reopen_calls, 1);
}

struct enc_cap {
    uint8_t *buf[NSTORES];
    uint64_t off[NSTORES];
};

static int cap_cell(void *ctx, unsigned pos, uint64_t idx, const uint8_t *data, size_t len, uint64_t sum)
{
    (void)idx, (void)sum;
    struct enc_cap *ec = ctx;
    memcpy(ec->buf[pos] + ec->off[pos], data, len);
    ec->off[pos] += len;
    return 0;
}

static void env_init(struct repair_env *e, const char *tag)
{
    memset(e, 0, sizeof *e);
    pthread_mutex_init(&e->gc_mu, NULL);
    e->volume = 1;
    for (int i = 0; i < 16; i++) {
        e->vuuid[i] = (uint8_t)(0xa0 + i);
        e->oid[i] = (uint8_t)(0x10 + i);
    }
    e->writer_epoch = 3;
    atomic_store(&e->fence_epoch, 7);
    atomic_store(&e->is_leader, true);
    atomic_store(&e->is_live, true);

    CHECK(chunk_geom_make(&e->g, 4, 2, 4096, 4096 * 4 * 3 + 777) == 0);
    e->obj_data = malloc((size_t)e->g.len);
    CHECK(e->obj_data != NULL);
    uint64_t r = 0x123456789abcdef0ull;
    for (uint64_t i = 0; i < e->g.len; i++) {
        r ^= r << 13, r ^= r >> 7, r ^= r << 17;
        e->obj_data[i] = (uint8_t)r;
    }
    e->nsums = chunk_sums_total(&e->g);
    e->sums = calloc((size_t)e->nsums, sizeof(uint64_t));
    CHECK(e->sums != NULL);

    struct enc_cap ec;
    memset(&ec, 0, sizeof ec);
    for (unsigned p = 0; p < NSTORES; p++)
        ec.buf[p] = malloc((size_t)chunk_pos_len(&e->g, p));
    CHECK(chunk_encode(&e->g, e->obj_data, cap_cell, &ec, e->sums) == 0);

    char dir[PATH_MAX];
    fresh(tag, dir, sizeof dir);
    for (unsigned p = 0; p < NSTORES; p++) {
        snprintf(e->sdir[p], sizeof e->sdir[p], "%s/st%u", dir, p);
        CHECK(chunk_store_init(e->sdir[p], NULL, NULL) == 0);
        const char *pp[1] = {e->sdir[p]};
        char err[256] = "";
        e->srv[p] = chunksrv_open(pp, 1, err, sizeof err);
        CHECK(e->srv[p] != NULL);
        chunk_store *st = chunk_be_store(chunksrv_backend(e->srv[p]), 0);
        chunk_ident id = {.volume = e->volume, .pos = (uint16_t)p};
        memcpy(id.vuuid, e->vuuid, 16);
        memcpy(id.oid, e->oid, 16);
        CHECK(chunk_store_put(st, &id, &e->g, e->writer_epoch, ec.buf[p], e->sums + chunk_sums_base(&e->g, p)) == 0);
        CHECK(chunk_store_raise(st, e->volume, atomic_load(&e->fence_epoch)) == 0);
        free(ec.buf[p]);
    }
}

static void env_destroy(struct repair_env *e)
{
    for (unsigned p = 0; p < NSTORES; p++)
        chunksrv_close(e->srv[p]);
    free(e->obj_data);
    free(e->sums);
    pthread_mutex_destroy(&e->gc_mu);
}

static void verify_chunk_intact(struct repair_env *e, unsigned pos, uint64_t want_epoch)
{
    chunk_store *st = chunk_be_store(chunksrv_backend(e->srv[pos]), 0);
    chunk_ident id = {.volume = e->volume, .pos = (uint16_t)pos};
    memcpy(id.vuuid, e->vuuid, 16);
    memcpy(id.oid, e->oid, 16);
    chunk_rd *rd = NULL;
    CHECK(chunk_rd_open(st, &id, &rd) == 0);
    if (!rd)
        return;
    CHECK(chunk_rd_hdr(rd)->epoch == want_epoch);
    uint64_t nc = chunk_pos_cells(&e->g, pos);
    uint64_t base = chunk_sums_base(&e->g, pos);
    CHECK(memcmp(chunk_rd_sums(rd), e->sums + base, (size_t)nc * sizeof(uint64_t)) == 0);
    uint64_t nbad = 1;
    CHECK(chunk_rd_scrub(rd, &nbad) == 0 && nbad == 0);
    chunk_rd_close(rd);
}

static void flip_chunk(struct repair_env *e, unsigned pos)
{
    chunk_store *st = chunk_be_store(chunksrv_backend(e->srv[pos]), 0);
    chunk_ident id = {.volume = e->volume, .pos = (uint16_t)pos};
    memcpy(id.vuuid, e->vuuid, 16);
    memcpy(id.oid, e->oid, 16);
    CHECK(chunk_store_test_flip(st, &id) == 0);
}

static void test_repair_all_paths(void)
{
    struct repair_env e;
    env_init(&e, "repair");

    struct chunkrepair_cfg cfg = {
        .nworkers = 2,
        .queue_max = 64,
        .max_retries = 2,
        .retry_base_ms = 20,
        .cooldown_ms = -1,
    };
    snprintf(cfg.stage_dir, sizeof cfg.stage_dir, "%s", S);
    struct chunkrepair_ops ops = {
        .lookup_live = env_lookup_live,
        .vol_view = env_vol_view,
        .read_cell = env_read_cell,
        .check_and_put = env_check_and_put,
        .notify_reopen = env_notify_reopen,
        .arg = &e,
    };
    struct chunkrepair *cr = chunkrepair_new(&cfg, &ops);
    CHECK(cr != NULL);

    /* Case A: data pos 0 and parity pos 4 damaged; leader fence=7 > writer_epoch=3.
     * Both must be repaired with hdr.epoch == 3 (catches repairownepoch). */
    flip_chunk(&e, 0);
    flip_chunk(&e, 4);
    CHECK(chunkrepair_enqueue(cr, e.volume, e.oid, 0, 1) == 0);
    CHECK(chunkrepair_enqueue(cr, e.volume, e.oid, 4, 5) == 0);
    chunkrepair_wait_idle(cr);
    verify_chunk_intact(&e, 0, e.writer_epoch);
    verify_chunk_intact(&e, 4, e.writer_epoch);
    CHECK(atomic_load(&e.reopen_calls) >= 2);
    struct chunkrepair_stats st;
    chunkrepair_get_stats(cr, &st);
    CHECK(st.results[CHUNKREPAIR_RES_REPAIRED] == 2);
    CHECK(st.bytes_repaired == chunk_pos_len(&e.g, 0) + chunk_pos_len(&e.g, 4));

    /* Case B: second rotten survivor (pos 0 and pos 1 both damaged). Honest code skips bad pos 1 using obj.sums,
     * decodes exact pos 0 from pos 2..5 (catches repairnosums). */
    flip_chunk(&e, 0);
    flip_chunk(&e, 1);
    CHECK(chunkrepair_enqueue(cr, e.volume, e.oid, 0, 1) == 0);
    chunkrepair_wait_idle(cr);
    verify_chunk_intact(&e, 0, e.writer_epoch);
    /* Repair pos 1 back to clean too. */
    CHECK(chunkrepair_enqueue(cr, e.volume, e.oid, 1, 2) == 0);
    chunkrepair_wait_idle(cr);
    verify_chunk_intact(&e, 1, e.writer_epoch);

    /* Case C: m + 1 = 3 damaged positions (0, 1, 2) -> unrecoverable, writes nothing. */
    flip_chunk(&e, 0);
    flip_chunk(&e, 1);
    flip_chunk(&e, 2);
    uint64_t unrec_before = 0;
    chunkrepair_get_stats(cr, &st);
    unrec_before = st.results[CHUNKREPAIR_RES_UNRECOVERABLE];
    CHECK(chunkrepair_enqueue(cr, e.volume, e.oid, 0, 1) == 0);
    chunkrepair_wait_idle(cr);
    chunkrepair_get_stats(cr, &st);
    CHECK(st.results[CHUNKREPAIR_RES_UNRECOVERABLE] == unrec_before + 1);
    /* Pos 0 must still be damaged (nothing written). */
    {
        chunk_store *s0 = chunk_be_store(chunksrv_backend(e.srv[0]), 0);
        chunk_ident id = {.volume = e.volume, .pos = 0};
        memcpy(id.vuuid, e.vuuid, 16);
        memcpy(id.oid, e.oid, 16);
        chunk_rd *rd = NULL;
        CHECK(chunk_rd_open(s0, &id, &rd) == 0);
        uint64_t nbad = 0;
        CHECK(rd && chunk_rd_scrub(rd, &nbad) == 0 && nbad >= 1);
        chunk_rd_close(rd);
    }
    /* Restore pos 1 and 2 directly so pos 0 is repairable again for next tests. */
    flip_chunk(&e, 1); /* XOR twice restores original byte! */
    flip_chunk(&e, 2);
    verify_chunk_intact(&e, 1, e.writer_epoch);
    verify_chunk_intact(&e, 2, e.writer_epoch);

    /* Case D: deposed leader fenced (store fence raised to 9 while leader's fence is 7 -> E14 FENCED). */
    CHECK(chunk_store_raise(chunk_be_store(chunksrv_backend(e.srv[0]), 0), e.volume, 9) == 0);
    CHECK(chunkrepair_enqueue(cr, e.volume, e.oid, 0, 1) == 0);
    chunkrepair_wait_idle(cr);
    chunkrepair_get_stats(cr, &st);
    CHECK(st.results[CHUNKREPAIR_RES_FENCED] == 1);

    /* Advance leader fence to 9 and repair pos 0 back to clean. */
    atomic_store(&e.fence_epoch, 9);
    CHECK(chunkrepair_enqueue(cr, e.volume, e.oid, 0, 1) == 0);
    chunkrepair_wait_idle(cr);
    verify_chunk_intact(&e, 0, e.writer_epoch);

    /* Case E: survivor ABSENT (not DAMAGED) -> MUST count unavailable, NOT unrecoverable. */
    flip_chunk(&e, 0);
    atomic_store(&e.inject_absent_mask, (1 << 1) | (1 << 2) | (1 << 3)); /* 3 absent -> only 2 good of 4 */
    chunkrepair_get_stats(cr, &st);
    uint64_t unrec_e = st.results[CHUNKREPAIR_RES_UNRECOVERABLE];
    uint64_t unavail_e = st.results[CHUNKREPAIR_RES_UNAVAILABLE];
    CHECK(chunkrepair_enqueue(cr, e.volume, e.oid, 0, 1) == 0);
    chunkrepair_wait_idle(cr);
    atomic_store(&e.inject_absent_mask, 0);
    chunkrepair_get_stats(cr, &st);
    CHECK(st.results[CHUNKREPAIR_RES_UNRECOVERABLE] == unrec_e);
    CHECK(st.results[CHUNKREPAIR_RES_UNAVAILABLE] == unavail_e + 1);

    /* Case F: transient DOWN on first attempt recovers automatically via backoff retry. */
    atomic_store(&e.inject_down_countdown, 3); /* first attempt sees 3 DOWN -> unavailable -> retries and succeeds */
    CHECK(chunkrepair_enqueue(cr, e.volume, e.oid, 0, 1) == 0);
    chunkrepair_wait_idle(cr);
    verify_chunk_intact(&e, 0, e.writer_epoch);

#ifdef TSDB_TEST_HOOKS
    /* Case G: drop / MK_GC race (D9): pause_before_put -> wait deterministically for pause -> retire & delete pos 0 ->
     * release -> leader MUST re-check live/GC before put (0 put attempts sent, 0 chunks resurrected; catches
     * repairnolive). */
    char pausef[PATH_MAX + 32];
    snprintf(pausef, sizeof pausef, "%s/pause_before_put", S);
    int pfd = open(pausef, O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC, 0644);
    CHECK(pfd >= 0);
    if (pfd >= 0)
        close(pfd);
    chunkrepair_set_pause_path(pausef);

    uint64_t hits0 = chunkrepair_pause_hits();
    flip_chunk(&e, 0);
    CHECK(chunkrepair_enqueue(cr, e.volume, e.oid, 0, 1) == 0);
    for (int i = 0; i < 2000 && chunkrepair_pause_hits() == hits0; i++)
        usleep(5000);
    CHECK(chunkrepair_pause_hits() > hits0);

    /* Simulate committed MK_GC deleting all chunks while repair is paused before put. */
    pthread_mutex_lock(&e.gc_mu);
    atomic_store(&e.is_live, false);
    chunk_ident id0 = {.volume = e.volume, .pos = 0};
    memcpy(id0.vuuid, e.vuuid, 16);
    memcpy(id0.oid, e.oid, 16);
    CHECK(chunk_store_del(chunk_be_store(chunksrv_backend(e.srv[0]), 0), &id0, 9) == 0);
    pthread_mutex_unlock(&e.gc_mu);

    int puts_before = atomic_load(&e.put_attempts);
    unlink(pausef);
    chunkrepair_wait_idle(cr);
    chunkrepair_set_pause_path(NULL);

    CHECK(atomic_load(&e.put_attempts) == puts_before); /* leader must never even issue put for dead object */
    chunk_rd *rd = NULL;
    CHECK(chunk_rd_open(chunk_be_store(chunksrv_backend(e.srv[0]), 0), &id0, &rd) == TSDB_ERR_NOTFOUND);
    if (rd)
        chunk_rd_close(rd);
    chunkrepair_get_stats(cr, &st);
    CHECK(st.results[CHUNKREPAIR_RES_NOT_LIVE] >= 1);
#endif

    chunkrepair_free(cr);
    env_destroy(&e);
}

/* ---- Test 6: read-path bad survivor triggers chunkrepair via chunkpeer.on_damaged ---- */
struct rp_ctx {
    struct repair_env *env;
    struct chunkrepair *cr;
};

static int rp_call(void *arg, uint32_t node, const struct cs_req *r, int tmo, int32_t *rc, uint8_t **body, size_t *bl)
{
    (void)tmo;
    struct rp_ctx *c = arg;
    if (node < 1 || node > NSTORES)
        return TSDB_ERR_UNAVAILABLE;
    return chunkpeer_local_call(c->env->srv[node - 1], 99, r, rc, body, bl);
}

static int rp_put(void *arg, uint32_t node, const struct cs_put_meta *m, int fd, uint64_t base, uint64_t sum, int tmo,
                  uint64_t *answered)
{
    (void)sum, (void)tmo;
    struct rp_ctx *c = arg;
    if (node < 1 || node > NSTORES)
        return TSDB_ERR_UNAVAILABLE;
    return chunkpeer_local_put(c->env->srv[node - 1], 99, m, fd, base, answered);
}

static int rp_view(void *arg, uint32_t volume, struct chunkpeer_view *v)
{
    struct rp_ctx *c = arg;
    if (volume != c->env->volume)
        return TSDB_ERR_NOTFOUND;
    memset(v, 0, sizeof *v);
    v->caught_up = 1;
    v->epoch = atomic_load(&c->env->fence_epoch);
    v->stripe_version = 1;
    v->k = c->env->g.k;
    v->m = c->env->g.m;
    v->nstripe = NSTORES;
    for (unsigned p = 0; p < NSTORES; p++)
        v->stripe[p] = p + 1;
    return 0;
}

static void rp_on_damaged(void *arg, uint32_t volume, const uint8_t oid[16], unsigned pos, uint32_t node)
{
    struct rp_ctx *c = arg;
    (void)chunkrepair_enqueue(c->cr, volume, oid, pos, node);
}

static void test_read_triggers_repair(void)
{
    struct repair_env e;
    env_init(&e, "readrep");

    struct chunkrepair_cfg cfg = {
        .nworkers = 2,
        .queue_max = 64,
        .max_retries = 2,
        .retry_base_ms = 20,
        .cooldown_ms = -1,
    };
    snprintf(cfg.stage_dir, sizeof cfg.stage_dir, "%s", S);
    struct chunkrepair_ops ops = {
        .lookup_live = env_lookup_live,
        .vol_view = env_vol_view,
        .read_cell = env_read_cell,
        .check_and_put = env_check_and_put,
        .notify_reopen = env_notify_reopen,
        .arg = &e,
    };
    struct chunkrepair *cr = chunkrepair_new(&cfg, &ops);
    CHECK(cr != NULL);

    struct rp_ctx rpc = {.env = &e, .cr = cr};
    struct chunkpeer cp;
    memset(&cp, 0, sizeof cp);
    cp.net = (struct chunkpeer_net){.call = rp_call, .put = rp_put, .arg = &rpc};
    cp.view = rp_view;
    cp.view_arg = &rpc;
    cp.on_damaged = rp_on_damaged;
    cp.on_damaged_arg = &rpc;
    snprintf(cp.stage_dir, sizeof cp.stage_dir, "%s", S);

    /* Flip data pos 0 on disk, then read the object through coldops_peer. */
    flip_chunk(&e, 0);
    void *cctx = NULL;
    CHECK(chunkpeer_make(&cp, NULL, e.volume, e.vuuid, &cctx) == 0);
    cold_obj *obj = NULL;
    CHECK(coldops_peer.obj_open(cctx, e.oid, &e.g, e.sums, &obj) == 0);
    uint8_t *got = malloc((size_t)e.g.len);
    chunk_report rep;
    memset(&rep, 0, sizeof rep);
    CHECK(coldops_peer.obj_read(obj, 0, e.g.len, got, 0, &rep) == 0);
    CHECK(memcmp(got, e.obj_data, (size_t)e.g.len) == 0);
    CHECK(rep.st[0] == CHUNK_ST_DAMAGED);
    free(got);
    coldops_peer.obj_close(obj);
    coldops_peer.ctx_free(cctx);
    chunkpeer_quiesce();

    /* Read hook must have enqueued and repaired pos 0 cleanly. */
    chunkrepair_wait_idle(cr);
    verify_chunk_intact(&e, 0, e.writer_epoch);
    struct chunkrepair_stats st;
    chunkrepair_get_stats(cr, &st);
    CHECK(st.results[CHUNKREPAIR_RES_REPAIRED] >= 1);

    chunkrepair_free(cr);
    env_destroy(&e);
}

static void test_cooldown(void)
{
    struct repair_env e;
    env_init(&e, "cooldown");

    struct chunkrepair_cfg cfg = {
        .nworkers = 1,
        .queue_max = 16,
        .max_retries = -1,
        .cooldown_ms = 200,
    };
    snprintf(cfg.stage_dir, sizeof cfg.stage_dir, "%s", S);
    struct chunkrepair_ops ops = {
        .lookup_live = env_lookup_live,
        .vol_view = env_vol_view,
        .read_cell = env_read_cell,
        .check_and_put = env_check_and_put,
        .arg = &e,
    };
    struct chunkrepair *cr = chunkrepair_new(&cfg, &ops);
    CHECK(cr != NULL);

    /* Make pos 0 unrecoverable (3 damaged positions). */
    flip_chunk(&e, 0);
    flip_chunk(&e, 1);
    flip_chunk(&e, 2);
    CHECK(chunkrepair_enqueue(cr, e.volume, e.oid, 0, 1) == 0);
    chunkrepair_wait_idle(cr);
    struct chunkrepair_stats st;
    chunkrepair_get_stats(cr, &st);
    CHECK(st.results[CHUNKREPAIR_RES_UNRECOVERABLE] == 1);
    uint64_t dedup0 = st.deduped;

    /* Immediate re-enqueue must be suppressed by cooldown without re-decoding. */
    CHECK(chunkrepair_enqueue(cr, e.volume, e.oid, 0, 1) == 0);
    chunkrepair_wait_idle(cr);
    chunkrepair_get_stats(cr, &st);
    CHECK(st.results[CHUNKREPAIR_RES_UNRECOVERABLE] == 1);
    CHECK(st.deduped == dedup0 + 1);

    /* After cooldown expires, a fresh enqueue runs again. */
    usleep(250 * 1000);
    CHECK(chunkrepair_enqueue(cr, e.volume, e.oid, 0, 1) == 0);
    chunkrepair_wait_idle(cr);
    chunkrepair_get_stats(cr, &st);
    CHECK(st.results[CHUNKREPAIR_RES_UNRECOVERABLE] == 2);

    chunkrepair_free(cr);
    env_destroy(&e);
}

int main(int argc, char **argv)
{
    const char *base = "/tmp", *mutant = NULL;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mutant") == 0 && i + 1 < argc)
            mutant = argv[++i];
        else
            base = argv[i];
    }
    if (mutant) {
        int m = chunkrepair_mutant_parse(mutant);
        if (m <= 0) {
            fprintf(stderr, "test_chunkrepair: unknown mutant %s\n", mutant);
            return 2;
        }
        atomic_store(&chunkrepair_mutant, m);
    }
    char tmpl[sizeof S];
    snprintf(tmpl, sizeof tmpl, "%s/tsdb-chunkrepair-XXXXXX", base);
    if (!mkdtemp(tmpl)) {
        perror("mkdtemp");
        return 1;
    }
    snprintf(S, sizeof S, "%s", tmpl);
    printf("test_chunkrepair%s%s\n", mutant ? " mutant " : "", mutant ? mutant : "");

    test_queue_and_serialization();
    test_repair_all_paths();
    test_read_triggers_repair();
    test_cooldown();

    if (fails == 0) {
        char cmd[PATH_MAX + 32];
        snprintf(cmd, sizeof cmd, "rm -rf '%s'", S);
        if (system(cmd) != 0)
            fprintf(stderr, "test_chunkrepair: could not remove %s\n", S);
    }
    printf("test_chunkrepair: %s (%d failures)\n", fails ? "FAILED" : "ok", fails);
    return fails ? 1 : 0;
}
#endif
