/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Unit tests for server/sfill.c (R9 track r9-fill; docs/design-cluster-data.md §5.3, E13-E15; D9-D11, D18).
 *
 * Uses a deterministic in-memory fake source backed by real chunk_encode() outputs and a real on-disk chunk_backend
 * for the local filling store, verifying:
 *   1. Copy-before-decode (1x copy from UP prev, zero reads from other positions).
 *   2. Copy fallback to RS decode when prev holds a rotted cell.
 *   3. RS decode (both data and parity positions) with rotted survivor fallback (catches `fillsurvivortrust`).
 *   4. Rebuilt-cell encode-time sum verification refusing mismatched encodings (catches `fillnocheck`).
 *   5. Fewer-than-k survivors refused per E15 (unavailable vs corrupt) and retried on recovery (catches `fillfewerk`).
 *   6. Partial `positions` bitmap (E10 degraded settle: unwritten positions never requested).
 *   7. Paged OBJLIST pagination, stripe_version < V filtering, small k' < k skip, empty volume, OBJLIST retry.
 *   8. Read-back verification failure detection + idempotent restart resume (already_held + reopen notify).
 *   9. Pacing via server/pace.h and concurrent worker pool + sfill_stop cleanup.
 *
 * Usage: test_sfill [--mutant NAME] [SCRATCH]
 */
#define _XOPEN_SOURCE 700
#include "internal.h"
#include "chunk.h"
#include "chunk_priv.h"
#include "pace.h"
#include "sfill.h"

#include <errno.h>
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
    printf("test_sfill: SKIPPED (this build has no erasure codec: NO_EC=1)\n");
    return 0;
}
#else

static int fails;
static char root_dir[PATH_MAX];
static const uint8_t VUUID[16] = {0x9f, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
                                  0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0x01};

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

/* ------------------------------------------------------------------ fake object & cluster source */

struct fake_obj {
    uint8_t oid[16];
    chunk_geom g;
    uint32_t positions;
    uint64_t writer_epoch;
    uint64_t stripe_version;
    uint64_t nsums;
    uint64_t *sums;
    uint8_t *pos_bytes[CHUNK_POS_MAX]; /* original encoded bytes of each position */
    uint64_t pos_len[CHUNK_POS_MAX];
    /* Fault injection per position */
    int pos_status[CHUNK_POS_MAX]; /* 0 = OK, else CHUNK_ST_* returned on read */
    bool pos_rot[CHUNK_POS_MAX];   /* flip byte 0 of cell 0 when served */
    bool prev_rot;                 /* flip byte 0 of cell 0 when served from prev_node */
};

#define FAKE_MAX_OBJS 32
#define FAKE_MAX_REOPENS 128

struct fake_src {
    pthread_mutex_t mu;
    pthread_cond_t read_cv;
    bool read_block;
    bool read_entered;
    uint32_t volume;
    uint64_t vol_epoch;
    uint32_t stripe[CHUNK_POS_MAX];
    unsigned nstripe;
    uint32_t prev_node;
    bool prev_up;

    chunk_backend *local_be;
    struct fake_obj objs[FAKE_MAX_OBJS];
    uint32_t nobjs;

    int objlist_fail_left;
    uint64_t objlist_calls;
    uint64_t read_calls_node[64]; /* indexed by node id < 64 */
    uint64_t read_calls_pos[CHUNK_POS_MAX];

    uint8_t reopened[FAKE_MAX_REOPENS][16];
    uint32_t nreopened;

    struct sfill_report last_rep[16];
    uint32_t nlast_rep;
    uint64_t report_calls;
};

struct enc_cap {
    const chunk_geom *g;
    struct fake_obj *o;
};

static int cap_cell(void *ctx, unsigned pos, uint64_t idx, const uint8_t *data, size_t len, uint64_t sum)
{
    (void)sum;
    struct enc_cap *c = ctx;
    uint64_t off = idx * (uint64_t)c->g->cell;
    memcpy(c->o->pos_bytes[pos] + off, data, len);
    return 0;
}

static void fake_obj_build(struct fake_obj *o, uint8_t tag, unsigned k, unsigned m, uint32_t cell, uint64_t obj_len,
                           uint64_t epoch, uint64_t stripe_ver, uint32_t positions, uint64_t seed)
{
    memset(o, 0, sizeof *o);
    for (int i = 0; i < 16; i++)
        o->oid[i] = (uint8_t)(tag * 13 + i + 1);
    o->oid[0] = tag; /* deterministic sort order */
    if (chunk_geom_make(&o->g, k, m, cell, obj_len) != 0)
        abort();
    o->positions = positions;
    o->writer_epoch = epoch;
    o->stripe_version = stripe_ver;
    o->nsums = chunk_sums_total(&o->g);
    o->sums = calloc((size_t)o->nsums, sizeof(uint64_t));
    uint8_t *raw = malloc((size_t)obj_len);
    if (!o->sums || !raw)
        abort();
    uint64_t x = seed ? seed : 0x123456789abcdef1ull;
    for (uint64_t i = 0; i < obj_len; i++) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        raw[i] = (uint8_t)x;
    }
    unsigned npos = chunk_npos(&o->g);
    for (unsigned p = 0; p < npos; p++) {
        o->pos_len[p] = chunk_pos_len(&o->g, p);
        o->pos_bytes[p] = calloc(1, (size_t)(o->pos_len[p] ? o->pos_len[p] : 1));
        if (!o->pos_bytes[p])
            abort();
    }
    struct enc_cap cap = {.g = &o->g, .o = o};
    if (chunk_encode(&o->g, raw, cap_cell, &cap, o->sums) != 0)
        abort();
    free(raw);
}

static void fake_obj_free(struct fake_obj *o)
{
    free(o->sums);
    for (unsigned p = 0; p < CHUNK_POS_MAX; p++)
        free(o->pos_bytes[p]);
    memset(o, 0, sizeof *o);
}

static void fake_src_init(struct fake_src *fs, uint32_t vol, uint64_t epoch, unsigned nstripe, uint32_t prev_node,
                          bool prev_up, chunk_backend *be)
{
    memset(fs, 0, sizeof *fs);
    pthread_mutex_init(&fs->mu, NULL);
    pthread_cond_init(&fs->read_cv, NULL);
    fs->volume = vol;
    fs->vol_epoch = epoch;
    fs->nstripe = nstripe;
    for (unsigned p = 0; p < nstripe; p++)
        fs->stripe[p] = p + 10; /* stripe members 10..10+nstripe-1 */
    fs->prev_node = prev_node;
    fs->prev_up = prev_up;
    fs->local_be = be;
}

static void fake_src_free(struct fake_src *fs)
{
    for (uint32_t i = 0; i < fs->nobjs; i++)
        fake_obj_free(&fs->objs[i]);
    pthread_cond_destroy(&fs->read_cv);
    pthread_mutex_destroy(&fs->mu);
}

static int fobj_oid_cmp(const void *a, const void *b)
{
    const struct fake_obj *const *xa = a;
    const struct fake_obj *const *xb = b;
    return memcmp((*xa)->oid, (*xb)->oid, 16);
}

static int fake_objlist(void *arg, uint32_t volume, uint64_t max_version, bool has_cursor,
                        const uint8_t cursor_oid[16], uint32_t max_entries,
                        uint8_t vuuid_out[16], struct sfill_obj **items_out, uint32_t *n_out, bool *more_out)
{
    struct fake_src *fs = arg;
    *items_out = NULL;
    *n_out = 0;
    *more_out = false;
    pthread_mutex_lock(&fs->mu);
    fs->objlist_calls++;
    if (volume != fs->volume) {
        pthread_mutex_unlock(&fs->mu);
        return TSDB_ERR_NOTFOUND;
    }
    if (fs->objlist_fail_left > 0) {
        fs->objlist_fail_left--;
        pthread_mutex_unlock(&fs->mu);
        return TSDB_ERR_UNAVAILABLE;
    }
    memcpy(vuuid_out, VUUID, 16);
    const struct fake_obj *cand[FAKE_MAX_OBJS];
    uint32_t nc = 0;
    for (uint32_t i = 0; i < fs->nobjs; i++) {
        if (max_version != 0 && fs->objs[i].stripe_version >= max_version)
            continue;
        if (has_cursor && memcmp(fs->objs[i].oid, cursor_oid, 16) <= 0)
            continue;
        cand[nc++] = &fs->objs[i];
    }
    if (nc > 1)
        qsort(cand, nc, sizeof *cand, fobj_oid_cmp);
    uint32_t take = nc < max_entries ? nc : max_entries;
    *more_out = take < nc;
    struct sfill_obj *out = take ? calloc(take, sizeof *out) : NULL;
    for (uint32_t i = 0; i < take; i++) {
        memcpy(out[i].oid, cand[i]->oid, 16);
        out[i].g = cand[i]->g;
        out[i].positions = cand[i]->positions;
        out[i].writer_epoch = cand[i]->writer_epoch;
        out[i].stripe_version = cand[i]->stripe_version;
        out[i].nsums = cand[i]->nsums;
        out[i].sums = malloc((size_t)cand[i]->nsums * sizeof(uint64_t));
        memcpy(out[i].sums, cand[i]->sums, (size_t)cand[i]->nsums * sizeof(uint64_t));
    }
    pthread_mutex_unlock(&fs->mu);
    *items_out = out;
    *n_out = take;
    return 0;
}

static int fake_read_cell(void *arg, uint32_t node, uint32_t volume, uint64_t epoch,
                          const uint8_t vuuid[16], const uint8_t oid[16], unsigned pos,
                          uint64_t off, void *buf, size_t len)
{
    (void)epoch;
    struct fake_src *fs = arg;
    pthread_mutex_lock(&fs->mu);
    if (fs->read_block) {
        fs->read_entered = true;
        pthread_cond_broadcast(&fs->read_cv);
        while (fs->read_block)
            pthread_cond_wait(&fs->read_cv, &fs->mu);
    }
    if (node < 64)
        fs->read_calls_node[node]++;
    if (pos < CHUNK_POS_MAX)
        fs->read_calls_pos[pos]++;
    if (volume != fs->volume || memcmp(vuuid, VUUID, 16) != 0) {
        pthread_mutex_unlock(&fs->mu);
        return CHUNK_ST_ABSENT;
    }
    const struct fake_obj *o = NULL;
    for (uint32_t i = 0; i < fs->nobjs; i++)
        if (memcmp(fs->objs[i].oid, oid, 16) == 0) {
            o = &fs->objs[i];
            break;
        }
    if (!o || pos >= chunk_npos(&o->g) || off + len > o->pos_len[pos]) {
        pthread_mutex_unlock(&fs->mu);
        return CHUNK_ST_ABSENT;
    }
    bool is_prev = (fs->prev_node != 0 && node == fs->prev_node);
    if (is_prev) {
        if (!fs->prev_up) {
            pthread_mutex_unlock(&fs->mu);
            return CHUNK_ST_DOWN;
        }
        memcpy(buf, o->pos_bytes[pos] + off, len);
        if (o->prev_rot && off == 0 && len > 0)
            ((uint8_t *)buf)[0] ^= 0x5a;
        pthread_mutex_unlock(&fs->mu);
        return CHUNK_ST_OK;
    }
    if (o->pos_status[pos] != 0) {
        int st = o->pos_status[pos];
        pthread_mutex_unlock(&fs->mu);
        return st;
    }
    memcpy(buf, o->pos_bytes[pos] + off, len);
    if (o->pos_rot[pos] && off == 0 && len > 0)
        ((uint8_t *)buf)[0] ^= 0xa5;
    pthread_mutex_unlock(&fs->mu);
    return CHUNK_ST_OK;
}

static int fake_vol_view(void *arg, uint32_t volume, uint64_t *epoch_out,
                         uint32_t stripe_out[CHUNK_POS_MAX], unsigned *nstripe_out,
                         bool *prev_up_out, uint32_t prev_node)
{
    struct fake_src *fs = arg;
    pthread_mutex_lock(&fs->mu);
    if (volume != fs->volume) {
        pthread_mutex_unlock(&fs->mu);
        return -1;
    }
    *epoch_out = fs->vol_epoch;
    *nstripe_out = fs->nstripe;
    memcpy(stripe_out, fs->stripe, fs->nstripe * sizeof(uint32_t));
    *prev_up_out = (prev_node != 0 && prev_node == fs->prev_node && fs->prev_up);
    pthread_mutex_unlock(&fs->mu);
    return 0;
}

static chunk_backend *fake_be_hold(void *arg) { return ((struct fake_src *)arg)->local_be; }
static void fake_be_release(void *arg, chunk_backend *be) { (void)arg, (void)be; }

static void fake_notify_reopen(void *arg, uint32_t volume, const uint8_t oid[16])
{
    (void)volume;
    struct fake_src *fs = arg;
    pthread_mutex_lock(&fs->mu);
    if (fs->nreopened < FAKE_MAX_REOPENS)
        memcpy(fs->reopened[fs->nreopened++], oid, 16);
    pthread_mutex_unlock(&fs->mu);
}

static void fake_on_report(void *arg, const struct sfill_report *reps, uint32_t nreps)
{
    struct fake_src *fs = arg;
    pthread_mutex_lock(&fs->mu);
    fs->report_calls++;
    uint32_t n = nreps < 16 ? nreps : 16;
    if (n > 0)
        memcpy(fs->last_rep, reps, n * sizeof reps[0]);
    fs->nlast_rep = n;
    pthread_mutex_unlock(&fs->mu);
}

static struct sfill_ops make_ops(struct fake_src *fs)
{
    return (struct sfill_ops){
        .objlist = fake_objlist,
        .read_cell = fake_read_cell,
        .vol_view = fake_vol_view,
        .local_be_hold = fake_be_hold,
        .local_be_release = fake_be_release,
        .notify_reopen = fake_notify_reopen,
        .on_report = fake_on_report,
        .arg = fs,
    };
}

static chunk_backend *make_local_be(const char *tag, char *dir_out, size_t cap)
{
    static int seq;
    int nw = snprintf(dir_out, cap, "%s/%s-%d", root_dir, tag, seq++);
    if (nw < 0 || (size_t)nw >= cap)
        abort();
    mkdir(dir_out, 0755);
    char s0[PATH_MAX + 32];
    int sw = snprintf(s0, sizeof s0, "%s/st0", dir_out);
    if (sw < 0 || (size_t)sw >= sizeof s0)
        abort();
    if (chunk_store_init(s0, NULL, NULL) != 0)
        abort();
    const char *paths[1] = {s0};
    chunk_backend *be = NULL;
    if (chunk_be_open(&be, paths, 1, NULL) != 0)
        abort();
    return be;
}

static bool wait_whole(struct sfill *sf, uint32_t vol, uint32_t pos, int timeout_ms, struct sfill_report *out)
{
    uint64_t t0 = now_ms();
    for (;;) {
        struct sfill_report reps[16];
        uint32_t n = sfill_report(sf, reps, 16);
        for (uint32_t i = 0; i < n; i++) {
            if (reps[i].volume == vol && reps[i].pos == pos) {
                if (out)
                    *out = reps[i];
                if (reps[i].whole)
                    return true;
            }
        }
        if ((int)(now_ms() - t0) >= timeout_ms)
            return false;
        sleep_ms(10);
    }
}

static void verify_chunk_matches_orig(chunk_backend *be, uint32_t vol, unsigned pos, const struct fake_obj *o,
                                      const char *ctx)
{
    chunk_ident id = {.volume = vol, .pos = (uint16_t)pos};
    memcpy(id.vuuid, VUUID, 16);
    memcpy(id.oid, o->oid, 16);
    chunk_rd *rd = NULL;
    int rc = chunk_be_rd_open(be, &id, &rd);
    CHECK(rc == 0, "%s: chunk_be_rd_open failed (%d)", ctx, rc);
    if (rc != 0)
        return;
    const chunk_hdr *h = chunk_rd_hdr(rd);
    CHECK(h->epoch == o->writer_epoch, "%s: writer_epoch mismatch (got %" PRIu64 ", want %" PRIu64 ")", ctx, h->epoch,
          o->writer_epoch);
    CHECK(h->chunk_len == o->pos_len[pos], "%s: chunk_len mismatch", ctx);
    uint8_t *got = malloc((size_t)(o->pos_len[pos] ? o->pos_len[pos] : 1));
    CHECK(chunk_rd_read(rd, 0, got, (size_t)o->pos_len[pos]) == 0, "%s: chunk_rd_read failed", ctx);
    CHECK(memcmp(got, o->pos_bytes[pos], (size_t)o->pos_len[pos]) == 0,
          "%s: rebuilt chunk bytes differ from original encoded chunk", ctx);
    uint64_t nbad = 99;
    CHECK(chunk_rd_scrub(rd, &nbad) == 0 && nbad == 0, "%s: chunk_rd_scrub bad=%" PRIu64, ctx, nbad);
    free(got);
    chunk_rd_close(rd);
}

static bool chunk_exists(chunk_backend *be, uint32_t vol, unsigned pos, const uint8_t oid[16])
{
    chunk_ident id = {.volume = vol, .pos = (uint16_t)pos};
    memcpy(id.vuuid, VUUID, 16);
    memcpy(id.oid, oid, 16);
    chunk_rd *rd = NULL;
    if (chunk_be_rd_open(be, &id, &rd) == 0) {
        chunk_rd_close(rd);
        return true;
    }
    return false;
}

/* ------------------------------------------------------------------ test cases */

/* 1. Copy-before-decode (1x copy from UP prev, zero reads from other stripe members). */
static void test_copy_from_prev(void)
{
    char dir[PATH_MAX];
    chunk_backend *be = make_local_be("copy", dir, sizeof dir);
    struct fake_src fs;
    fake_src_init(&fs, 1, 5, 6, /*prev=*/42, /*prev_up=*/true, be);
    fake_obj_build(&fs.objs[0], 1, 4, 2, 4096, 4096 * 10 + 333, 3, 1, 0, 101);
    fs.nobjs = 1;

    struct sfill_ops ops = make_ops(&fs);
    struct sfill *sf = sfill_new_with_ops(&ops);
    struct sfill_job_cfg cfg = {.volume = 1, .pos = 2, .stripe_version = 2, .prev_node = 42, .leader = 10,
                                .ec_k = 4, .ec_m = 2, .margin = 1, .concurrency = 2};
    CHECK(sfill_start(sf, &cfg) == 0, "copy: sfill_start");
    struct sfill_report rep = {0};
    CHECK(wait_whole(sf, 1, 2, 5000, &rep), "copy: did not reach whole");
    CHECK(rep.done == 1 && rep.total == 1 && rep.whole == 1 && rep.stripe_version == 2,
          "copy: report done=%" PRIu64 " total=%" PRIu64 " whole=%u", rep.done, rep.total, rep.whole);

    struct sfill_stats st;
    sfill_get_stats(sf, &st);
    CHECK(st.copied == 1 && st.decoded == 0, "copy: expected copied=1 decoded=0, got %" PRIu64 "/%" PRIu64,
          st.copied, st.decoded);
    CHECK(fs.read_calls_node[42] > 0, "copy: prev_node not read");
    for (unsigned p = 0; p < 6; p++)
        CHECK(fs.read_calls_node[10 + p] == 0, "copy: non-prev node %u was read during 1x copy", 10 + p);
    CHECK(fs.nreopened == 1 && memcmp(fs.reopened[0], fs.objs[0].oid, 16) == 0, "copy: notify_reopen missing");
    struct sfill_progress pr = {0};
    CHECK(sfill_get_progress(sf, &pr, 1) == 0, "copy: completed (whole) job must be omitted from sfill_get_progress");
    verify_chunk_matches_orig(be, 1, 2, &fs.objs[0], "copy");

    sfill_free(sf);
    fake_src_free(&fs);
    chunk_be_close(be);
}

/* 2. Copy fallback when prev holds a rotted cell -> falls back to RS decode and writes exact bytes. */
static void test_copy_rotted_prev_fallback(void)
{
    char dir[PATH_MAX];
    chunk_backend *be = make_local_be("copyrot", dir, sizeof dir);
    struct fake_src fs;
    fake_src_init(&fs, 1, 5, 6, /*prev=*/42, /*prev_up=*/true, be);
    fake_obj_build(&fs.objs[0], 2, 4, 2, 4096, 4096 * 9 + 77, 2, 1, 0, 202);
    fs.objs[0].prev_rot = true;
    fs.nobjs = 1;

    struct sfill_ops ops = make_ops(&fs);
    struct sfill *sf = sfill_new_with_ops(&ops);
    struct sfill_job_cfg cfg = {.volume = 1, .pos = 1, .stripe_version = 2, .prev_node = 42, .leader = 10,
                                .ec_k = 4, .ec_m = 2, .margin = 1, .concurrency = 2};
    CHECK(sfill_start(sf, &cfg) == 0, "copyrot: sfill_start");
    CHECK(wait_whole(sf, 1, 1, 5000, NULL), "copyrot: did not reach whole");

    struct sfill_stats st;
    sfill_get_stats(sf, &st);
    CHECK(st.copied == 0 && st.decoded == 1 && st.survivor_bad >= 1,
          "copyrot: copied=%" PRIu64 " decoded=%" PRIu64 " bad=%" PRIu64, st.copied, st.decoded, st.survivor_bad);
    verify_chunk_matches_orig(be, 1, 1, &fs.objs[0], "copyrot");

    sfill_free(sf);
    fake_src_free(&fs);
    chunk_be_close(be);
}

/* 3. RS decode of data AND parity positions with a rotted survivor (catches `fillsurvivortrust`). */
static void test_decode_rotted_survivor(void)
{
    /* 3a: Rebuild data position 1 while survivor position 0 is rotted -> must reject pos 0 and use parity! */
    for (unsigned target_pos = 1; target_pos <= 5; target_pos += 4) {
        char dir[PATH_MAX];
        chunk_backend *be = make_local_be(target_pos == 1 ? "decdata" : "decpar", dir, sizeof dir);
        struct fake_src fs;
        fake_src_init(&fs, 2, 7, 6, /*prev=*/42, /*prev_up=*/false, be);
        fake_obj_build(&fs.objs[0], 3, 4, 2, 4096, 4096 * 11 + 511, 4, 1, 0, 303 + target_pos);
        fs.objs[0].pos_rot[0] = true; /* rot surviving data position 0! */
        fs.nobjs = 1;

        struct sfill_ops ops = make_ops(&fs);
        struct sfill *sf = sfill_new_with_ops(&ops);
        struct sfill_job_cfg cfg = {.volume = 2, .pos = target_pos, .stripe_version = 2, .prev_node = 42,
                                    .leader = 10, .ec_k = 4, .ec_m = 2, .margin = 1, .concurrency = 2};
        CHECK(sfill_start(sf, &cfg) == 0, "dec(%u): sfill_start", target_pos);
        CHECK(wait_whole(sf, 2, target_pos, 3000, NULL),
              "dec(%u): failed to rebuild around rotted survivor (fillsurvivortrust?)", target_pos);

        struct sfill_stats st;
        sfill_get_stats(sf, &st);
        CHECK(st.decoded == 1 && st.survivor_bad >= 1, "dec(%u): decoded=%" PRIu64 " survivor_bad=%" PRIu64,
              target_pos, st.decoded, st.survivor_bad);
        verify_chunk_matches_orig(be, 2, target_pos, &fs.objs[0], target_pos == 1 ? "decdata" : "decpar");

        sfill_free(sf);
        fake_src_free(&fs);
        chunk_be_close(be);
    }
}

/* 4. Rebuilt-cell sum verification refuses a mismatched encoding (catches `fillnocheck`). */
static void test_rebuilt_sum_mismatch_refused(void)
{
    char dir[PATH_MAX];
    chunk_backend *be = make_local_be("nocheck", dir, sizeof dir);
    struct fake_src fs;
    fake_src_init(&fs, 3, 5, 6, /*prev=*/42, /*prev_up=*/false, be);

    /* Object A and Object B have identical geometry but different payloads. Build fs.objs[0] with survivors'
     * bytes & sums from B, but target pos 2's encode-time sums from A: every survivor passes its own sum check,
     * yet RS-decoded pos 2 will NOT match obj->sums[pos 2]. Honest code MUST refuse to write! */
    struct fake_obj objA;
    fake_obj_build(&objA, 4, 4, 2, 4096, 4096 * 6 + 99, 2, 1, 0, 404);
    fake_obj_build(&fs.objs[0], 4, 4, 2, 4096, 4096 * 6 + 99, 2, 1, 0, 505);
    uint64_t base2 = chunk_sums_base(&objA.g, 2);
    uint64_t nc2 = chunk_pos_cells(&objA.g, 2);
    memcpy(fs.objs[0].sums + base2, objA.sums + base2, (size_t)nc2 * sizeof(uint64_t));
    fake_obj_free(&objA);
    fs.nobjs = 1;

    struct sfill_ops ops = make_ops(&fs);
    struct sfill *sf = sfill_new_with_ops(&ops);
    struct sfill_job_cfg cfg = {.volume = 3, .pos = 2, .stripe_version = 2, .prev_node = 42, .leader = 10,
                                .ec_k = 4, .ec_m = 2, .margin = 1, .concurrency = 1};
    CHECK(sfill_start(sf, &cfg) == 0, "nocheck: sfill_start");

    /* Wait until at least one attempt ran, and assert it did NOT mark whole and did NOT write any chunk. */
    uint64_t t0 = now_ms();
    struct sfill_stats st;
    struct sfill_report rep = {0};
    for (;;) {
        sfill_get_stats(sf, &st);
        sfill_report(sf, &rep, 1);
        if (st.corrupt >= 1 || rep.whole == 1 || now_ms() - t0 > 2000)
            break;
        sleep_ms(10);
    }
    CHECK(rep.whole == 0 && rep.done == 0, "nocheck: mismatched encoding must NOT reach whole (fillnocheck?)");
    CHECK(st.corrupt >= 1 && st.decoded == 0, "nocheck: expected corrupt>=1 decoded=0, got %" PRIu64 "/%" PRIu64,
          st.corrupt, st.decoded);
    CHECK(!chunk_exists(be, 3, 2, fs.objs[0].oid), "nocheck: corrupt chunk must NOT be written to disk");

    sfill_free(sf);
    fake_src_free(&fs);
    chunk_be_close(be);
}

/* 5. Fewer than k good survivors refused per E15 (unavailable vs corrupt), then retried on recovery (catches `fillfewerk`). */
static void test_fewer_than_k_and_retry(void)
{
    char dir[PATH_MAX];
    chunk_backend *be = make_local_be("fewerk", dir, sizeof dir);
    struct fake_src fs;
    fake_src_init(&fs, 4, 6, 6, /*prev=*/42, /*prev_up=*/false, be);
    fake_obj_build(&fs.objs[0], 5, 4, 2, 4096, 4096 * 8 + 17, 2, 1, 0, 606);
    /* Filling pos 0: make pos 2 and pos 3 DOWN -> only 3 survivors (< k = 4) available! */
    fs.objs[0].pos_status[2] = CHUNK_ST_DOWN;
    fs.objs[0].pos_status[3] = CHUNK_ST_DOWN;
    fs.nobjs = 1;

    struct sfill_ops ops = make_ops(&fs);
    struct sfill *sf = sfill_new_with_ops(&ops);
    struct sfill_job_cfg cfg = {.volume = 4, .pos = 0, .stripe_version = 2, .prev_node = 42, .leader = 10,
                                .ec_k = 4, .ec_m = 2, .margin = 0, .concurrency = 1};
    CHECK(sfill_start(sf, &cfg) == 0, "fewerk: sfill_start");

    uint64_t t0 = now_ms();
    struct sfill_stats st;
    struct sfill_report rep = {0};
    for (;;) {
        sfill_get_stats(sf, &st);
        sfill_report(sf, &rep, 1);
        if (st.unavailable >= 1 || rep.whole == 1 || now_ms() - t0 > 2000)
            break;
        sleep_ms(10);
    }
    CHECK(rep.whole == 0 && rep.done == 0, "fewerk: must NOT mark whole with < k survivors (fillfewerk?)");
    CHECK(st.unavailable >= 1 && st.decoded == 0, "fewerk: expected unavailable>=1 decoded=0, got %" PRIu64 "/%" PRIu64,
          st.unavailable, st.decoded);
    CHECK(!chunk_exists(be, 4, 0, fs.objs[0].oid), "fewerk: must NOT write chunk with < k survivors");

    /* Switch pos 2 & 3 from DOWN to ABSENT -> must classify as CORRUPT (not unavailable) per E15. */
    pthread_mutex_lock(&fs.mu);
    fs.objs[0].pos_status[2] = CHUNK_ST_ABSENT;
    fs.objs[0].pos_status[3] = CHUNK_ST_ABSENT;
    pthread_mutex_unlock(&fs.mu);
    t0 = now_ms();
    for (;;) {
        sfill_get_stats(sf, &st);
        if (st.corrupt >= 1 || now_ms() - t0 > 2000)
            break;
        sleep_ms(10);
    }
    CHECK(st.corrupt >= 1, "fewerk: ABSENT < k should be classified corrupt per E15");

    /* Now recover pos 2 -> 4 good survivors {1, 2, 4, 5} -> automatic retry must succeed! */
    pthread_mutex_lock(&fs.mu);
    fs.objs[0].pos_status[2] = CHUNK_ST_OK;
    pthread_mutex_unlock(&fs.mu);
    CHECK(wait_whole(sf, 4, 0, 3000, &rep), "fewerk: did not succeed after survivor recovered");
    CHECK(rep.done == 1 && rep.total == 1 && rep.whole == 1, "fewerk: bad final report");
    verify_chunk_matches_orig(be, 4, 0, &fs.objs[0], "fewerk-recovered");

    sfill_free(sf);
    fake_src_free(&fs);
    chunk_be_close(be);
}

/* 6. Partial `positions` bitmap (E10 degraded settle): unwritten positions never requested. */
static void test_partial_positions_bitmap(void)
{
    /* 6a: pos 2 was unwritten (mask 0x3b = all 6 except bit 2); filling pos 0 must NEVER read pos 2. */
    {
        char dir[PATH_MAX];
        chunk_backend *be = make_local_be("posmaskA", dir, sizeof dir);
        struct fake_src fs;
        fake_src_init(&fs, 5, 4, 6, /*prev=*/42, /*prev_up=*/false, be);
        fake_obj_build(&fs.objs[0], 6, 4, 2, 4096, 4096 * 7 + 123, 2, 1, /*positions=*/0x3bu, 707);
        fs.nobjs = 1;

        struct sfill_ops ops = make_ops(&fs);
        struct sfill *sf = sfill_new_with_ops(&ops);
        struct sfill_job_cfg cfg = {.volume = 5, .pos = 0, .stripe_version = 2, .prev_node = 42, .leader = 10,
                                    .ec_k = 4, .ec_m = 2, .margin = 1, .concurrency = 1};
        CHECK(sfill_start(sf, &cfg) == 0, "posmaskA: sfill_start");
        CHECK(wait_whole(sf, 5, 0, 3000, NULL), "posmaskA: did not reach whole");
        CHECK(fs.read_calls_pos[2] == 0, "posmaskA: unwritten pos 2 was requested %" PRIu64 " times",
              fs.read_calls_pos[2]);
        verify_chunk_matches_orig(be, 5, 0, &fs.objs[0], "posmaskA");

        sfill_free(sf);
        fake_src_free(&fs);
        chunk_be_close(be);
    }

    /* 6b: filling the unwritten position itself (pos 2) with prev UP must skip copy from prev and decode! */
    {
        char dir[PATH_MAX];
        chunk_backend *be = make_local_be("posmaskB", dir, sizeof dir);
        struct fake_src fs;
        fake_src_init(&fs, 5, 4, 6, /*prev=*/42, /*prev_up=*/true, be);
        fake_obj_build(&fs.objs[0], 7, 4, 2, 4096, 4096 * 7 + 123, 2, 1, /*positions=*/0x3bu, 808);
        fs.nobjs = 1;

        struct sfill_ops ops = make_ops(&fs);
        struct sfill *sf = sfill_new_with_ops(&ops);
        struct sfill_job_cfg cfg = {.volume = 5, .pos = 2, .stripe_version = 2, .prev_node = 42, .leader = 10,
                                    .ec_k = 4, .ec_m = 2, .margin = 1, .concurrency = 1};
        CHECK(sfill_start(sf, &cfg) == 0, "posmaskB: sfill_start");
        CHECK(wait_whole(sf, 5, 2, 3000, NULL), "posmaskB: did not reach whole");
        CHECK(fs.read_calls_node[42] == 0, "posmaskB: prev_node should not be read for unwritten position");
        struct sfill_stats st;
        sfill_get_stats(sf, &st);
        CHECK(st.decoded == 1 && st.copied == 0, "posmaskB: expected decoded=1 copied=0");
        verify_chunk_matches_orig(be, 5, 2, &fs.objs[0], "posmaskB");

        sfill_free(sf);
        fake_src_free(&fs);
        chunk_be_close(be);
    }
}

/* 7. Paged OBJLIST, stripe_version < V filtering, small k'<k object skip, OBJLIST retry, empty volume. */
static void test_pagination_filter_and_empty(void)
{
    sfill_test_objlist_page = 2; /* force multi-page cursor pagination! */
    char dir[PATH_MAX];
    chunk_backend *be = make_local_be("page", dir, sizeof dir);
    struct fake_src fs;
    fake_src_init(&fs, 6, 8, 6, /*prev=*/42, /*prev_up=*/false, be);
    fs.objlist_fail_left = 2; /* fail first 2 OBJLIST calls -> must retry and NEVER mark whole early */

    /* 4 regular objects with stripe_version < 5, 1 tiny object (k'=1, m=2 -> npos=3 < target pos=4),
     * plus 2 objects with stripe_version >= 5 (must be excluded by version < V). */
    for (uint8_t i = 0; i < 4; i++)
        fake_obj_build(&fs.objs[fs.nobjs++], (uint8_t)(10 + i), 4, 2, 4096, 4096 * 5 + 100 + i, 2, 1 + i, 0, 900 + i);
    fake_obj_build(&fs.objs[fs.nobjs++], 14, 1, 2, 4096, 1024, 2, 2, 0, 910); /* small k'=1+2=3; pos=4 skipped */
    fake_obj_build(&fs.objs[fs.nobjs++], 15, 4, 2, 4096, 4096 * 5, 3, 5, 0, 911); /* ver == V: excluded */
    fake_obj_build(&fs.objs[fs.nobjs++], 16, 4, 2, 4096, 4096 * 5, 3, 6, 0, 912); /* ver > V: excluded */

    struct sfill_ops ops = make_ops(&fs);
    struct sfill *sf = sfill_new_with_ops(&ops);
    struct sfill_job_cfg cfg = {.volume = 6, .pos = 4, .stripe_version = 5, .prev_node = 42, .leader = 10,
                                .ec_k = 4, .ec_m = 2, .margin = 2, .concurrency = 4};
    CHECK(sfill_start(sf, &cfg) == 0, "page: sfill_start");
    struct sfill_report rep = {0};
    CHECK(wait_whole(sf, 6, 4, 5000, &rep), "page: did not reach whole");
    CHECK(rep.done == 5 && rep.total == 5 && rep.whole == 1,
          "page: done=%" PRIu64 " total=%" PRIu64 " whole=%u", rep.done, rep.total, rep.whole);
    CHECK(fs.objlist_calls >= 5, "page: expected >= 5 OBJLIST calls (2 fails + 3 pages), got %" PRIu64,
          fs.objlist_calls);

    struct sfill_stats st;
    sfill_get_stats(sf, &st);
    CHECK(st.decoded == 4 && st.skipped_small == 1, "page: decoded=%" PRIu64 " skipped_small=%" PRIu64,
          st.decoded, st.skipped_small);
    for (uint32_t i = 0; i < 4; i++)
        verify_chunk_matches_orig(be, 6, 4, &fs.objs[i], "page");
    CHECK(!chunk_exists(be, 6, 4, fs.objs[5].oid), "page: ver==V object should not be filled");
    CHECK(!chunk_exists(be, 6, 4, fs.objs[6].oid), "page: ver>V object should not be filled");

    /* Verify pacing counted bytes on PACE_RECV. */
    struct pace_stats ps;
    pace_stats(&ps);
    CHECK(ps.taken_bytes[PACE_RECV] > 0, "page: pace PACE_RECV taken_bytes should be > 0");

    /* sfill_stop removes the job from reports and progress immediately. */
    struct sfill_progress pr = {0};
    sfill_stop(sf, 6, 4);
    CHECK(sfill_report(sf, &rep, 1) == 0, "page: sfill_stop did not remove report");
    CHECK(sfill_get_progress(sf, &pr, 1) == 0, "page: sfill_stop did not remove progress");

    sfill_free(sf);
    fake_src_free(&fs);
    chunk_be_close(be);

    /* Empty volume (0 objects < V) -> immediately whole with done=0, total=0. */
    be = make_local_be("empty", dir, sizeof dir);
    fake_src_init(&fs, 7, 2, 6, 42, false, be);
    ops = make_ops(&fs);
    sf = sfill_new_with_ops(&ops);
    cfg = (struct sfill_job_cfg){.volume = 7, .pos = 0, .stripe_version = 2, .prev_node = 42, .leader = 10,
                                 .ec_k = 4, .ec_m = 2, .margin = 1, .concurrency = 2};
    CHECK(sfill_start(sf, &cfg) == 0, "empty: sfill_start");
    CHECK(wait_whole(sf, 7, 0, 3000, &rep), "empty: did not reach whole");
    CHECK(rep.done == 0 && rep.total == 0 && rep.whole == 1, "empty: report mismatch");

    sfill_free(sf);
    fake_src_free(&fs);
    chunk_be_close(be);
    sfill_test_objlist_page = 0;
}

/* 8. Read-back verification failure triggers retry, and restart resumes idempotently via already_held. */
static void test_readback_verify_and_resume(void)
{
    char dir[PATH_MAX];
    chunk_backend *be = make_local_be("readback", dir, sizeof dir);
    struct fake_src fs;
    fake_src_init(&fs, 8, 4, 6, /*prev=*/42, /*prev_up=*/false, be);
    fake_obj_build(&fs.objs[0], 20, 4, 2, 4096, 4096 * 6 + 55, 3, 1, 0, 1001);
    fs.nobjs = 1;

    /* Flip one byte after commit before read-back once -> first pass fails read-back, retry writes clean chunk. */
    atomic_store(&sfill_test_readback_flip, 1);
    struct sfill_ops ops = make_ops(&fs);
    struct sfill *sf = sfill_new_with_ops(&ops);
    struct sfill_job_cfg cfg = {.volume = 8, .pos = 3, .stripe_version = 2, .prev_node = 42, .leader = 10,
                                .ec_k = 4, .ec_m = 2, .margin = 1, .concurrency = 1};
    CHECK(sfill_start(sf, &cfg) == 0, "readback: sfill_start");
    CHECK(wait_whole(sf, 8, 3, 5000, NULL), "readback: did not recover on retry");

    struct sfill_stats st;
    sfill_get_stats(sf, &st);
    CHECK(st.failed >= 1 && st.decoded == 1, "readback: expected failed>=1 decoded=1, got %" PRIu64 "/%" PRIu64,
          st.failed, st.decoded);
    verify_chunk_matches_orig(be, 8, 3, &fs.objs[0], "readback");
    sfill_free(sf);

    /* Simulate restart after CP_FILL_WRITTEN (including a transient initial OBJLIST failure while peer links come up):
     * start a fresh sfill against the same local chunk store. It must detect the chunk is already intact
     * (already_held == 1), do 0 remote reads, and still notify reopen on pass 0 once OBJLIST succeeds. */
    memset(fs.read_calls_node, 0, sizeof fs.read_calls_node);
    fs.nreopened = 0;
    fs.objlist_fail_left = 1;
    sf = sfill_new_with_ops(&ops);
    CHECK(sfill_start(sf, &cfg) == 0, "resume: sfill_start");
    CHECK(wait_whole(sf, 8, 3, 3000, NULL), "resume: did not reach whole");
    sfill_get_stats(sf, &st);
    CHECK(st.already_held == 1 && st.decoded == 0 && st.copied == 0,
          "resume: expected already_held=1, got held=%" PRIu64 " dec=%" PRIu64 " cop=%" PRIu64,
          st.already_held, st.decoded, st.copied);
    for (int i = 0; i < 64; i++)
        CHECK(fs.read_calls_node[i] == 0, "resume: unexpected remote read on node %d", i);
    CHECK(fs.nreopened == 1, "resume: notify_reopen should still fire on resume");

    sfill_free(sf);
    fake_src_free(&fs);
    chunk_be_close(be);
}

/* 9. Changing stripe_version while a worker is actively running an object must not deadlock the job. */
static void test_version_change_mid_object(void)
{
    char dir[PATH_MAX];
    chunk_backend *be = make_local_be("verchg", dir, sizeof dir);
    struct fake_src fs;
    fake_src_init(&fs, 9, 4, 6, /*prev=*/42, /*prev_up=*/false, be);
    fake_obj_build(&fs.objs[0], 30, 4, 2, 4096, 4096 * 6 + 33, 2, 1, 0, 1101);
    fake_obj_build(&fs.objs[1], 31, 4, 2, 4096, 4096 * 6 + 44, 2, 2, 0, 1102); /* stripe_version == 2 (< V=3) */
    fs.nobjs = 2;
    fs.read_block = true;

    struct sfill_ops ops = make_ops(&fs);
    struct sfill *sf = sfill_new_with_ops(&ops);
    struct sfill_job_cfg cfg = {.volume = 9, .pos = 2, .stripe_version = 2, .prev_node = 42, .leader = 10,
                                .ec_k = 4, .ec_m = 2, .margin = 1, .concurrency = 2};
    CHECK(sfill_start(sf, &cfg) == 0, "verchg: sfill_start V=2");

    /* Wait until a worker has entered read_cell for obj 0 at V=2. */
    pthread_mutex_lock(&fs.mu);
    uint64_t t0 = now_ms();
    while (!fs.read_entered && now_ms() - t0 < 3000) {
        struct timespec dl;
        clock_gettime(CLOCK_REALTIME, &dl);
        dl.tv_nsec += 20 * 1000000L;
        if (dl.tv_nsec >= 1000000000L) {
            dl.tv_sec++;
            dl.tv_nsec -= 1000000000L;
        }
        pthread_cond_timedwait(&fs.read_cv, &fs.mu, &dl);
    }
    CHECK(fs.read_entered, "verchg: worker did not enter read_cell");
    pthread_mutex_unlock(&fs.mu);

    /* While obj 0 is actively in flight (V=2), sfill_get_progress must report bytes_total == pos_len[2]. */
    struct sfill_progress pr = {0};
    uint32_t npr = sfill_get_progress(sf, &pr, 1);
    CHECK(npr == 1 && pr.volume == 9 && pr.pos == 2 && pr.bytes_done == 0 && pr.bytes_total == fs.objs[0].pos_len[2] &&
              pr.started_ms > 0 && pr.last_progress_ms >= pr.started_ms,
          "verchg: live sfill_get_progress mismatch (n=%u done=%" PRIu64 " total=%" PRIu64 " want=%" PRIu64 ")", npr,
          pr.bytes_done, pr.bytes_total, fs.objs[0].pos_len[2]);

    /* Bump stripe_version to 3 while worker is in-flight on V=2, then unblock the read! */
    cfg.stripe_version = 3;
    CHECK(sfill_start(sf, &cfg) == 0, "verchg: sfill_start V=3");

    pthread_mutex_lock(&fs.mu);
    fs.read_block = false;
    pthread_cond_broadcast(&fs.read_cv);
    pthread_mutex_unlock(&fs.mu);

    struct sfill_report rep = {0};
    CHECK(wait_whole(sf, 9, 2, 5000, &rep), "verchg: job stalled after mid-object stripe_version bump");
    CHECK(rep.stripe_version == 3 && rep.whole == 1 && rep.done == 2 && rep.total == 2,
          "verchg: unexpected report ver=%" PRIu64 " whole=%u done=%" PRIu64 "/%" PRIu64,
          rep.stripe_version, rep.whole, rep.done, rep.total);
    verify_chunk_matches_orig(be, 9, 2, &fs.objs[0], "verchg0");
    verify_chunk_matches_orig(be, 9, 2, &fs.objs[1], "verchg1");

    sfill_free(sf);
    fake_src_free(&fs);
    chunk_be_close(be);
}

int main(int argc, char **argv)
{
    const char *scratch = NULL;
    const char *mutant = NULL;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mutant") == 0 && i + 1 < argc) {
            mutant = argv[++i];
        } else if (argv[i][0] != '-') {
            scratch = argv[i];
        }
    }
    if (mutant) {
        int m = sfill_mutant_parse(mutant);
        if (m < 0) {
            fprintf(stderr, "test_sfill: unknown mutant '%s'\n", mutant);
            return 2;
        }
        sfill_set_mutant(m);
    }
    if (!scratch || !*scratch)
        scratch = getenv("TMPDIR");
    if (!scratch || !*scratch)
        scratch = "/tmp";
    snprintf(root_dir, sizeof root_dir, "%s/tsdb_sfill_%d", scratch, (int)getpid());
    mkdir(root_dir, 0755);

    sfill_test_retry_ms = 20;
    pace_reset();
    pace_configure(&(struct pace_cfg){.rebuild_bytes_s = 1000ull << 20, .balance_bytes_s = 0});

    printf("test_sfill%s%s\n", mutant ? " mutant " : "", mutant ? mutant : "");
    test_copy_from_prev();
    test_copy_rotted_prev_fallback();
    test_decode_rotted_survivor();
    test_rebuilt_sum_mismatch_refused();
    test_fewer_than_k_and_retry();
    test_partial_positions_bitmap();
    test_pagination_filter_and_empty();
    test_readback_verify_and_resume();
    test_version_change_mid_object();

    rm_tree(root_dir);
    if (fails > 0) {
        fprintf(stderr, "test_sfill: %d failure(s)\n", fails);
        return 1;
    }
    printf("test_sfill: PASS\n");
    return 0;
}
#endif
