/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Tests for local chunk moves, start-time dedupe, and concurrent delete/repair serialization (R13, r13-lmove;
 * docs/design-cluster-data.md E50, §15.3, PLAN.md D18).
 *
 * Covers:
 *   1. Basic chunk_move: target gets the volume's fence first, header preserves writer epoch, cells verified against
 *      sums, local location index updated, source unlinked, reads whole, damaged source refused.
 *   2. Pacing and prompt cancellation (leaving source intact and no leftover tmp file).
 *   3. Start-time dedupe in chunk_be_open2: preferred store (capacity over fast / drain target over draining) kept
 *      (catches mutant `movekeepwrong`), damaged copy always dropped even if on preferred store, corrupt-header copy
 *      dropped, all-damaged copies left intact.
 *   4. Concurrent races: deterministic delete-vs-move (leaves nothing), deterministic r12-repair replace-put-vs-move
 *      (detects changed inode and moves repaired bytes), and multi-threaded stress race (moves + deletes + reads).
 *   5. crashfs power-loss sweep at every step of chunk_move: always 1 or 2 whole copies (never 0; catches mutant
 *      `moveunlinkfirst`), target EPOCH >= volume fence whenever target holds the chunk, and start-time dedupe keeps
 *      the preferred copy (catches `movekeepwrong`).
 *
 * Works identically with and without TSDB_EC (NO_EC=1) because chunk puts/reads/moves use xxh64 cell sums directly.
 */
#define _GNU_SOURCE
#include "chunk.h"
#include "chunk_priv.h"
#include "chunkmove.h"
#include "internal.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static int fails;
static char ROOT[PATH_MAX];
static char SELF[PATH_MAX];
static char SHIM[PATH_MAX + 32];

#define CHECK(cond, ...)                                                                         \
    do {                                                                                         \
        if (!(cond)) {                                                                           \
            fprintf(stderr, "FAIL %s:%d: %s — ", __FILE__, __LINE__, #cond);                     \
            fprintf(stderr, __VA_ARGS__);                                                        \
            fprintf(stderr, "\n");                                                               \
            fails++;                                                                             \
        }                                                                                        \
    } while (0)

static int rm_cb(const char *p, const struct stat *sb, int tf, struct FTW *fb)
{
    (void)sb;
    (void)tf;
    (void)fb;
    return remove(p);
}

static void rmtree(const char *dir) { nftw(dir, rm_cb, 16, FTW_DEPTH | FTW_PHYS); }

static uint64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

static void fill_pattern(uint8_t *buf, size_t n, uint64_t seed)
{
    uint64_t x = seed ? seed : 1;
    for (size_t i = 0; i < n; i++) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        buf[i] = (uint8_t)x;
    }
}

static void make_id(chunk_ident *id, uint32_t vol, unsigned tag, unsigned pos)
{
    memset(id, 0, sizeof *id);
    id->volume = vol;
    id->pos = (uint16_t)pos;
    for (int i = 0; i < 16; i++) {
        id->vuuid[i] = (uint8_t)(0xA0 + i);
        id->oid[i] = (uint8_t)(tag * 37 + (unsigned)i * 7 + 1);
    }
}

/* Reads every cell of a chunk in store `st` and checks exact bytes against `expect` (if non-NULL). */
static int verify_store_chunk(chunk_store *st, const chunk_ident *id, const uint8_t *expect, size_t len,
                              uint64_t *epoch_out)
{
    chunk_rd *rd = NULL;
    int rc = chunk_rd_open(st, id, &rd);
    if (rc != 0)
        return rc;
    uint64_t nbad = 0;
    rc = chunk_rd_scrub(rd, &nbad);
    if (rc == 0 && nbad != 0)
        rc = TSDB_ERR_CORRUPT;
    if (rc == 0 && epoch_out)
        *epoch_out = chunk_rd_hdr(rd)->epoch;
    if (rc == 0 && expect) {
        if (chunk_rd_hdr(rd)->chunk_len != len) {
            rc = TSDB_ERR_CORRUPT;
        } else {
            uint8_t *buf = malloc(len ? len : 1);
            if (!buf)
                rc = TSDB_ERR_NOMEM;
            else {
                rc = chunk_rd_read(rd, 0, buf, len);
                if (rc == 0 && memcmp(buf, expect, len) != 0)
                    rc = TSDB_ERR_CORRUPT;
                free(buf);
            }
        }
    }
    chunk_rd_close(rd);
    return rc;
}

static int count_tmp_files(const char *store_dir, uint32_t vol)
{
    char vdir[PATH_MAX + 64];
    snprintf(vdir, sizeof vdir, "%s/s%" PRIu32, store_dir, vol);
    DIR *d = opendir(vdir);
    if (!d)
        return 0;
    int n = 0;
    struct dirent *e;
    while ((e = readdir(d)) != NULL)
        if (strncmp(e->d_name, "tmp_", 4) == 0)
            n++;
    closedir(d);
    return n;
}

/* ---------------------------------------------------------------- 1. basic move */
static void test_basic_move(void)
{
    char d[PATH_MAX + 32], s0[PATH_MAX + 64], s1[PATH_MAX + 64];
    snprintf(d, sizeof d, "%s/basic", ROOT);
    snprintf(s0, sizeof s0, "%s/st0", d);
    snprintf(s1, sizeof s1, "%s/st1", d);
    CHECK(mkdir(d, 0755) == 0, "mkdir basic");
    CHECK(chunk_store_init(s0, NULL, NULL) == 0, "init st0");
    CHECK(chunk_store_init(s1, NULL, NULL) == 0, "init st1");

    /* Put a chunk on st0 at epoch 3 and raise st0's fence to 7 BEFORE st1 has any volume directory/EPOCH. */
    chunk_geom g;
    CHECK(chunk_geom_make(&g, 2, 1, 4096, 4096 * 6 + 321) == 0, "geom");
    chunk_ident id;
    make_id(&id, 1, 1, 0);
    size_t clen = (size_t)chunk_pos_len(&g, id.pos);
    uint8_t *cells = malloc(clen);
    fill_pattern(cells, clen, 1001);

    chunk_store *solo = NULL;
    CHECK(chunk_store_open(s0, NULL, &solo) == 0, "open solo st0");
    CHECK(chunk_store_put(solo, &id, &g, 3, cells, NULL) == 0, "put on st0");
    CHECK(chunk_store_raise(solo, 1, 7) == 0, "raise st0 fence to 7");
    chunk_store_close(solo);

    const char *pp[2] = {s0, s1};
    int pref[2] = {0, 30};
    chunk_be_opts bo = {.min_up = 2, .strict = true, .store_pref = pref};
    chunk_backend *be = NULL;
    CHECK(chunk_be_open2(&be, pp, 2, NULL, &bo) == 0, "open be");
    CHECK(chunk_store_epoch(chunk_be_store(be, 1), 1) == 0, "st1 has no epoch before move");

    uint64_t moved = 0;
    chunk_move_opts mo = {.rate_bps = UINT64_MAX, .bytes_moved = &moved};
    CHECK(chunk_move(be, &id, 0, 1, &mo) == 0, "chunk_move 0->1");
    CHECK(moved == chunk_file_size(&g, id.pos), "bytes_moved=%" PRIu64, moved);

    /* Target got volume fence (7) and preserved original writer epoch (3). */
    CHECK(chunk_store_epoch(chunk_be_store(be, 1), 1) == 7, "st1 fence raised to 7 (got %" PRIu64 ")",
          chunk_store_epoch(chunk_be_store(be, 1), 1));
    uint64_t hdr_ep = 0;
    CHECK(verify_store_chunk(chunk_be_store(be, 1), &id, cells, clen, &hdr_ep) == 0, "st1 chunk whole");
    CHECK(hdr_ep == 3, "writer epoch preserved (got %" PRIu64 ")", hdr_ep);
    CHECK(verify_store_chunk(chunk_be_store(be, 0), &id, NULL, 0, NULL) == TSDB_ERR_NOTFOUND, "st0 source unlinked");
    CHECK(chunk_be_loc_get(be, 1, id.oid, id.pos) == 1, "local index points to st1");

    chunk_rd *rd = NULL;
    int at = -1;
    CHECK(chunk_be_rd_open_at(be, &id, &rd, &at) == 0 && at == 1, "be read hits st1 (at=%d)", at);
    chunk_rd_close(rd);

    /* Fenced delete below volume fence must still be refused on st1; delete at fence 7 succeeds. */
    CHECK(chunk_be_del(be, &id, 6) == CHUNK_ERR_FENCED, "del below fence refused");
    CHECK(chunk_be_del(be, &id, 7) == 0, "del at fence succeeds");
    CHECK(verify_store_chunk(chunk_be_store(be, 1), &id, NULL, 0, NULL) == TSDB_ERR_NOTFOUND, "gone after del");

    /* Move with zeroed vuuid (drain driver pattern) preserves source header's vuuid. */
    chunk_ident id2;
    make_id(&id2, 1, 2, 1);
    size_t clen2 = (size_t)chunk_pos_len(&g, id2.pos);
    uint8_t *cells2 = malloc(clen2);
    fill_pattern(cells2, clen2, 2002);
    CHECK(chunk_be_put(be, 0, &id2, &g, 8, cells2, NULL) == 0, "put id2 on st0");
    chunk_ident id2_any = {.volume = 1, .pos = id2.pos};
    memcpy(id2_any.oid, id2.oid, 16);
    CHECK(chunk_move(be, &id2_any, 0, 1, &mo) == 0, "chunk_move with zero vuuid");
    CHECK(verify_store_chunk(chunk_be_store(be, 1), &id2, cells2, clen2, &hdr_ep) == 0 && hdr_ep == 8,
          "zero-vuuid move preserved vuuid & epoch");
    CHECK(verify_store_chunk(chunk_be_store(be, 0), &id2, NULL, 0, NULL) == TSDB_ERR_NOTFOUND, "id2 unlinked on st0");

    /* Damaged source must NOT be copied or unlinked. */
    chunk_ident id3;
    make_id(&id3, 1, 3, 0);
    CHECK(chunk_be_put(be, 0, &id3, &g, 8, cells, NULL) == 0, "put id3 on st0");
    CHECK(chunk_store_test_flip(chunk_be_store(be, 0), &id3) == 0, "flip cell byte on st0");
    CHECK(chunk_move(be, &id3, 0, 1, &mo) == TSDB_ERR_CORRUPT, "damaged source rejected");
    CHECK(chunk_store_stat_chunk(chunk_be_store(be, 0), 1, id3.oid, id3.pos, NULL, NULL) == 0,
          "damaged source left intact on st0");
    CHECK(chunk_store_stat_chunk(chunk_be_store(be, 1), 1, id3.oid, id3.pos, NULL, NULL) == TSDB_ERR_NOTFOUND,
          "nothing written to st1 on damaged source");

    chunk_be_close(be);
    free(cells);
    free(cells2);
    printf("  basic move, fence-first, local index, zero-vuuid, damaged-source refusal: ok\n");
}

/* ---------------------------------------------------------------- 2. pacing & cancellation */
struct cancel_after_ctx {
    int left;
    int cancelled;
};

static void cancel_pace_cb(void *vctx, uint64_t bytes)
{
    (void)bytes;
    struct cancel_after_ctx *c = vctx;
    if (c->left > 0 && --c->left == 0)
        c->cancelled = 1;
}

static int cancel_check_cb(void *vctx) { return ((struct cancel_after_ctx *)vctx)->cancelled; }

static void test_pacing_and_cancel(void)
{
    char d[PATH_MAX + 32], s0[PATH_MAX + 64], s1[PATH_MAX + 64];
    snprintf(d, sizeof d, "%s/pace", ROOT);
    snprintf(s0, sizeof s0, "%s/st0", d);
    snprintf(s1, sizeof s1, "%s/st1", d);
    CHECK(mkdir(d, 0755) == 0, "mkdir pace");
    CHECK(chunk_store_init(s0, NULL, NULL) == 0, "init st0");
    CHECK(chunk_store_init(s1, NULL, NULL) == 0, "init st1");

    const char *pp[2] = {s0, s1};
    chunk_backend *be = NULL;
    CHECK(chunk_be_open(&be, pp, 2, NULL) == 0, "open be");

    chunk_geom g;
    CHECK(chunk_geom_make(&g, 1, 0, 16384, 16384 * 4) == 0, "geom 64KiB");
    chunk_ident id1, id2;
    make_id(&id1, 2, 10, 0);
    make_id(&id2, 2, 11, 0);
    size_t clen = (size_t)chunk_pos_len(&g, 0);
    uint8_t *cells = malloc(clen);
    fill_pattern(cells, clen, 3003);
    CHECK(chunk_be_put(be, 0, &id1, &g, 1, cells, NULL) == 0, "put id1");
    CHECK(chunk_be_put(be, 0, &id2, &g, 1, cells, NULL) == 0, "put id2");

    /* 64 KiB at 256 KiB/s -> ~250 ms. */
    chunk_move_pacer pacer;
    chunk_move_pacer_init(&pacer, 256u * 1024u);
    chunk_move_opts mo = {.pacer = &pacer};
    uint64_t t0 = now_ms();
    CHECK(chunk_move(be, &id1, 0, 1, &mo) == 0, "paced move");
    uint64_t dt = now_ms() - t0;
    CHECK(dt >= 150, "paced move elapsed %" PRIu64 " ms (want >= 150 ms)", dt);

    /* Cancellation after 1 cell aborts cleanly, leaves source intact and zero tmp files on target. */
    struct cancel_after_ctx cc = {.left = 1, .cancelled = 0};
    chunk_move_opts cmo = {.rate_bps = UINT64_MAX,
                           .cancel = cancel_check_cb,
                           .cancel_ctx = &cc,
                           .pace = cancel_pace_cb,
                           .pace_ctx = &cc};
    CHECK(chunk_move(be, &id2, 0, 1, &cmo) == TSDB_ERR_STOPPED, "cancelled move returns STOPPED");
    CHECK(verify_store_chunk(chunk_be_store(be, 0), &id2, cells, clen, NULL) == 0, "source intact after cancel");
    CHECK(verify_store_chunk(chunk_be_store(be, 1), &id2, NULL, 0, NULL) == TSDB_ERR_NOTFOUND,
          "target absent after cancel");
    CHECK(count_tmp_files(s1, 2) == 0, "no leftover tmp_* on target after cancel");

    chunk_be_close(be);
    free(cells);
    printf("  pacing (%" PRIu64 " ms for 64 KiB @ 256 KiB/s) and cancellation: ok\n", dt);
}

/* ---------------------------------------------------------------- 3. start-time dedupe */
static void test_open_dedupe(void)
{
    char d[PATH_MAX + 32], s0[PATH_MAX + 64], s1[PATH_MAX + 64];
    snprintf(d, sizeof d, "%s/dedupe", ROOT);
    snprintf(s0, sizeof s0, "%s/fast", d);
    snprintf(s1, sizeof s1, "%s/cap", d);
    CHECK(mkdir(d, 0755) == 0, "mkdir dedupe");
    CHECK(chunk_store_init(s0, NULL, NULL) == 0, "init fast");
    CHECK(chunk_store_init(s1, NULL, NULL) == 0, "init cap");

    chunk_geom g;
    CHECK(chunk_geom_make(&g, 2, 1, 4096, 4096 * 4 + 99) == 0, "geom");
    chunk_ident id_both, id_dmg_cap, id_dmg_fast, id_hdr_cap, id_both_dmg;
    make_id(&id_both, 3, 20, 0);
    make_id(&id_dmg_cap, 3, 21, 0);
    make_id(&id_dmg_fast, 3, 22, 0);
    make_id(&id_hdr_cap, 3, 23, 0);
    make_id(&id_both_dmg, 3, 24, 0);
    size_t clen = (size_t)chunk_pos_len(&g, 0);
    uint8_t *cells = malloc(clen);
    fill_pattern(cells, clen, 4004);

    /* Put identical copies onto both stores directly (simulating a crash after target commit, before source unlink). */
    chunk_store *st0 = NULL, *st1 = NULL;
    CHECK(chunk_store_open(s0, NULL, &st0) == 0 && chunk_store_open(s1, NULL, &st1) == 0, "open solo stores");
    chunk_ident *all_ids[5] = {&id_both, &id_dmg_cap, &id_dmg_fast, &id_hdr_cap, &id_both_dmg};
    for (int i = 0; i < 5; i++) {
        CHECK(chunk_store_put(st0, all_ids[i], &g, 4, cells, NULL) == 0, "put fast %d", i);
        CHECK(chunk_store_put(st1, all_ids[i], &g, 4, cells, NULL) == 0, "put cap %d", i);
    }
    /* Flip cells / truncate headers as planned. */
    CHECK(chunk_store_test_flip(st1, &id_dmg_cap) == 0, "flip cap copy");
    CHECK(chunk_store_test_flip(st0, &id_dmg_fast) == 0, "flip fast copy");
    CHECK(chunk_store_test_flip(st0, &id_both_dmg) == 0, "flip both fast");
    CHECK(chunk_store_test_flip(st1, &id_both_dmg) == 0, "flip both cap");
    chunk_store_close(st0);
    chunk_store_close(st1);

    /* Corrupt header of id_hdr_cap on cap store by truncating it to 10 bytes. */
    char hex[33], badpath[PATH_MAX + 128];
    chunk_oid_hex(id_hdr_cap.oid, hex);
    snprintf(badpath, sizeof badpath, "%s/s3/o%s.0.chunk", s1, hex);
    CHECK(truncate(badpath, 10) == 0, "truncate cap header");

    /* Reopen as a backend with pref={0 (fast), 30 (cap)} -> triggers start-time dedupe! */
    const char *pp[2] = {s0, s1};
    int pref[2] = {0, 30};
    chunk_be_opts bo = {.min_up = 2, .strict = true, .store_pref = pref};
    chunk_backend *be = NULL;
    CHECK(chunk_be_open2(&be, pp, 2, NULL, &bo) == 0, "open be with dedupe");

    /* 1. Both whole -> capacity copy kept, fast copy unlinked (catches movekeepwrong). */
    CHECK(verify_store_chunk(chunk_be_store(be, 1), &id_both, cells, clen, NULL) == 0,
          "dedupe kept capacity copy (movekeepwrong?)");
    CHECK(verify_store_chunk(chunk_be_store(be, 0), &id_both, NULL, 0, NULL) == TSDB_ERR_NOTFOUND,
          "dedupe unlinked fast copy (movekeepwrong?)");
    {
        int expect_loc = chunk_be_place(be, id_both.oid, 0) == 1 ? -1 : 1;
        CHECK(chunk_be_loc_get(be, 3, id_both.oid, 0) == expect_loc, "index matches off-primary policy for cap");
        chunk_rd *rd = NULL;
        int got_st = -1;
        CHECK(chunk_be_rd_open_at(be, &id_both, &rd, &got_st) == 0 && got_st == 1, "read resolves id_both on cap");
        chunk_rd_close(rd);
    }

    /* 2. Damaged cap + whole fast -> damaged cap dropped, whole fast kept. */
    CHECK(verify_store_chunk(chunk_be_store(be, 0), &id_dmg_cap, cells, clen, NULL) == 0,
          "dedupe kept whole fast copy over damaged cap");
    CHECK(verify_store_chunk(chunk_be_store(be, 1), &id_dmg_cap, NULL, 0, NULL) == TSDB_ERR_NOTFOUND,
          "dedupe dropped damaged cap copy");

    /* 3. Damaged fast + whole cap -> damaged fast dropped, whole cap kept. */
    CHECK(verify_store_chunk(chunk_be_store(be, 1), &id_dmg_fast, cells, clen, NULL) == 0,
          "dedupe kept whole cap copy over damaged fast");
    CHECK(verify_store_chunk(chunk_be_store(be, 0), &id_dmg_fast, NULL, 0, NULL) == TSDB_ERR_NOTFOUND,
          "dedupe dropped damaged fast copy");

    /* 4. Corrupt header on cap + whole fast -> corrupt cap dropped, whole fast kept. */
    CHECK(verify_store_chunk(chunk_be_store(be, 0), &id_hdr_cap, cells, clen, NULL) == 0,
          "dedupe kept whole fast copy over corrupt-header cap");
    CHECK(chunk_store_stat_chunk(chunk_be_store(be, 1), 3, id_hdr_cap.oid, 0, NULL, NULL) == TSDB_ERR_NOTFOUND,
          "dedupe dropped corrupt-header cap copy");

    /* 5. Both damaged -> neither silently deleted. */
    CHECK(chunk_store_stat_chunk(chunk_be_store(be, 0), 3, id_both_dmg.oid, 0, NULL, NULL) == 0,
          "all-damaged fast copy retained");
    CHECK(chunk_store_stat_chunk(chunk_be_store(be, 1), 3, id_both_dmg.oid, 0, NULL, NULL) == 0,
          "all-damaged cap copy retained");

    chunk_be_close(be);
    free(cells);
    printf("  start-time dedupe (preferred kept, damaged/corrupt dropped, all-damaged retained): ok\n");
}

/* ---------------------------------------------------------------- 4. concurrent delete & repair races */
#ifdef TSDB_TEST_HOOKS
struct del_race_ctx {
    chunk_backend *be;
    chunk_ident id;
    uint64_t epoch;
    int fired;
    int del_rc;
};

static void del_race_hook(void *vctx)
{
    struct del_race_ctx *c = vctx;
    if (c->fired++)
        return;
    c->del_rc = chunk_be_del(c->be, &c->id, c->epoch);
}

struct rep_race_ctx {
    chunk_backend *be;
    chunk_ident id;
    chunk_geom g;
    uint64_t hdr_epoch, fence_epoch;
    const uint8_t *new_cells;
    int fired;
    int rep_rc;
};

static void rep_race_hook(void *vctx)
{
    struct rep_race_ctx *c = vctx;
    if (c->fired)
        return;
    c->fired = 1;
    chunk_wr *w = NULL;
    chunk_store *st = NULL;
    int rc = chunk_be_wr_open_replace(c->be, &c->id, &c->g, c->hdr_epoch, c->fence_epoch, &w, &st);
    if (rc == 0) {
        const uint8_t *p = c->new_cells;
        for (uint64_t i = 0; i < chunk_pos_cells(&c->g, c->id.pos) && rc == 0; i++) {
            size_t cl = (size_t)chunk_pos_cell_len(&c->g, c->id.pos, i);
            rc = chunk_wr_cell(w, p, cl, false, 0);
            p += cl;
        }
        if (rc == 0)
            rc = chunk_wr_commit(w);
        else
            chunk_wr_abort(w);
    }
    chunk_store_unref(st);
    c->rep_rc = rc;
}
#endif

struct stress_ctx {
    chunk_backend *be;
    chunk_geom g;
    int nchunks;
    chunk_ident *ids;
    uint8_t **cells;
    size_t clen;
    atomic_int stop;
    atomic_int rd_errs;
};

static void *stress_mover(void *arg)
{
    struct stress_ctx *s = arg;
    chunk_move_opts mo = {.rate_bps = UINT64_MAX};
    for (int i = 0; i < s->nchunks; i++) {
        int rc = chunk_move(s->be, &s->ids[i], 0, 1, &mo);
        if (rc != 0 && rc != TSDB_ERR_NOTFOUND)
            atomic_fetch_add(&s->rd_errs, 1);
    }
    return NULL;
}

static void *stress_deleter(void *arg)
{
    struct stress_ctx *s = arg;
    for (int i = 1; i < s->nchunks; i += 2) {
        int rc = chunk_be_del(s->be, &s->ids[i], 5);
        if (rc != 0)
            atomic_fetch_add(&s->rd_errs, 1);
    }
    return NULL;
}

static void *stress_reader(void *arg)
{
    struct stress_ctx *s = arg;
    uint8_t *buf = malloc(s->clen ? s->clen : 1);
    while (!atomic_load(&s->stop)) {
        for (int i = 0; i < s->nchunks; i += 2) {
            chunk_rd *rd = NULL;
            int rc = chunk_be_rd_open(s->be, &s->ids[i], &rd);
            if (rc != 0) {
                atomic_fetch_add(&s->rd_errs, 1);
                continue;
            }
            uint64_t nbad = 0;
            if (chunk_rd_scrub(rd, &nbad) != 0 || nbad != 0 ||
                chunk_rd_read(rd, 0, buf, s->clen) != 0 || memcmp(buf, s->cells[i], s->clen) != 0)
                atomic_fetch_add(&s->rd_errs, 1);
            chunk_rd_close(rd);
        }
    }
    free(buf);
    return NULL;
}

static void test_concurrent_races(void)
{
    char d[PATH_MAX + 32], s0[PATH_MAX + 64], s1[PATH_MAX + 64];
    snprintf(d, sizeof d, "%s/races", ROOT);
    snprintf(s0, sizeof s0, "%s/st0", d);
    snprintf(s1, sizeof s1, "%s/st1", d);
    CHECK(mkdir(d, 0755) == 0, "mkdir races");
    CHECK(chunk_store_init(s0, NULL, NULL) == 0, "init st0");
    CHECK(chunk_store_init(s1, NULL, NULL) == 0, "init st1");

    const char *pp[2] = {s0, s1};
    int pref[2] = {0, 30};
    chunk_be_opts bo = {.min_up = 2, .strict = true, .store_pref = pref};
    chunk_backend *be = NULL;
    CHECK(chunk_be_open2(&be, pp, 2, NULL, &bo) == 0, "open be");

    chunk_geom g;
    CHECK(chunk_geom_make(&g, 2, 1, 4096, 4096 * 4) == 0, "geom");
    size_t clen = (size_t)chunk_pos_len(&g, 0);
    uint8_t *cells_a = malloc(clen), *cells_b = malloc(clen);
    fill_pattern(cells_a, clen, 5005);
    fill_pattern(cells_b, clen, 6006);

#ifdef TSDB_TEST_HOOKS
    /* 4a. Deterministic delete racing move -> leaves NOTHING on either store! */
    chunk_ident id_del;
    make_id(&id_del, 4, 30, 0);
    CHECK(chunk_be_put(be, 0, &id_del, &g, 5, cells_a, NULL) == 0, "put id_del");
    struct del_race_ctx dc = {.be = be, .id = id_del, .epoch = 5, .fired = 0, .del_rc = -1};
    chunkmove_set_before_lock_hook(del_race_hook, &dc);
    chunk_move_opts mo = {.rate_bps = UINT64_MAX};
    int mrc = chunk_move(be, &id_del, 0, 1, &mo);
    chunkmove_set_before_lock_hook(NULL, NULL);
    CHECK(dc.fired == 1 && dc.del_rc == 0, "del hook fired (fired=%d rc=%d)", dc.fired, dc.del_rc);
    CHECK(mrc == TSDB_ERR_NOTFOUND, "move aborted with NOTFOUND when deleted concurrently (rc=%d)", mrc);
    CHECK(chunk_store_stat_chunk(chunk_be_store(be, 0), 4, id_del.oid, 0, NULL, NULL) == TSDB_ERR_NOTFOUND,
          "deleted chunk absent on st0");
    CHECK(chunk_store_stat_chunk(chunk_be_store(be, 1), 4, id_del.oid, 0, NULL, NULL) == TSDB_ERR_NOTFOUND,
          "deleted chunk NOT resurrected on st1");
    CHECK(count_tmp_files(s1, 4) == 0, "no tmp left on st1 after delete race");

    /* 4b. Deterministic r12-repair replace-put racing move -> move retries and copies repaired bytes B! */
    chunk_ident id_rep;
    make_id(&id_rep, 4, 31, 0);
    CHECK(chunk_be_put(be, 0, &id_rep, &g, 5, cells_a, NULL) == 0, "put id_rep (A)");
    struct rep_race_ctx rc_ctx = {
        .be = be, .id = id_rep, .g = g, .hdr_epoch = 5, .fence_epoch = 6, .new_cells = cells_b, .fired = 0, .rep_rc = -1};
    chunkmove_set_before_lock_hook(rep_race_hook, &rc_ctx);
    mrc = chunk_move(be, &id_rep, 0, 1, &mo);
    chunkmove_set_before_lock_hook(NULL, NULL);
    CHECK(rc_ctx.fired == 1 && rc_ctx.rep_rc == 0, "replace hook fired (fired=%d rc=%d)", rc_ctx.fired, rc_ctx.rep_rc);
    CHECK(mrc == 0, "move retried after replace and succeeded (rc=%d)", mrc);
    CHECK(verify_store_chunk(chunk_be_store(be, 1), &id_rep, cells_b, clen, NULL) == 0,
          "target holds repaired payload B (not stale A)");
    CHECK(chunk_store_stat_chunk(chunk_be_store(be, 0), 4, id_rep.oid, 0, NULL, NULL) == TSDB_ERR_NOTFOUND,
          "source unlinked after replace+move");
#endif

    /* 4c. Multi-threaded stress race: moves + deletes on odd chunks + reads on even chunks. */
    enum { NSTRESS = 40 };
    chunk_ident ids[NSTRESS];
    uint8_t *bufs[NSTRESS];
    for (int i = 0; i < NSTRESS; i++) {
        make_id(&ids[i], 5, (unsigned)(100 + i), 0);
        bufs[i] = malloc(clen);
        fill_pattern(bufs[i], clen, (uint64_t)(7000 + i));
        CHECK(chunk_be_put(be, 0, &ids[i], &g, 5, bufs[i], NULL) == 0, "put stress %d", i);
    }

    struct stress_ctx sc = {.be = be, .g = g, .nchunks = NSTRESS, .ids = ids, .cells = bufs, .clen = clen};
    atomic_init(&sc.stop, 0);
    atomic_init(&sc.rd_errs, 0);
    pthread_t tm, td, tr;
    CHECK(pthread_create(&tr, NULL, stress_reader, &sc) == 0, "spawn reader");
    CHECK(pthread_create(&tm, NULL, stress_mover, &sc) == 0, "spawn mover");
    CHECK(pthread_create(&td, NULL, stress_deleter, &sc) == 0, "spawn deleter");
    pthread_join(tm, NULL);
    pthread_join(td, NULL);
    atomic_store(&sc.stop, 1);
    pthread_join(tr, NULL);

    CHECK(atomic_load(&sc.rd_errs) == 0, "stress race errors=%d", atomic_load(&sc.rd_errs));
    for (int i = 0; i < NSTRESS; i++) {
        if (i % 2 == 1) {
            /* Deleted concurrently -> must exist on NEITHER store! */
            CHECK(chunk_store_stat_chunk(chunk_be_store(be, 0), 5, ids[i].oid, 0, NULL, NULL) == TSDB_ERR_NOTFOUND,
                  "stress odd %d absent on st0", i);
            CHECK(chunk_store_stat_chunk(chunk_be_store(be, 1), 5, ids[i].oid, 0, NULL, NULL) == TSDB_ERR_NOTFOUND,
                  "stress odd %d NOT resurrected on st1", i);
        } else {
            CHECK(verify_store_chunk(chunk_be_store(be, 1), &ids[i], bufs[i], clen, NULL) == 0,
                  "stress even %d whole on st1", i);
            CHECK(chunk_store_stat_chunk(chunk_be_store(be, 0), 5, ids[i].oid, 0, NULL, NULL) == TSDB_ERR_NOTFOUND,
                  "stress even %d unlinked on st0", i);
        }
        free(bufs[i]);
    }

    chunk_be_close(be);
    free(cells_a);
    free(cells_b);
    printf("  concurrent delete/repair races and multi-threaded stress: ok\n");
}

/* ---------------------------------------------------------------- 5. unsure EPOCH beside sure volume + loc_tab scoping */
static void test_unsure_and_scoping(void)
{
    char dir[PATH_MAX + 32], s0[PATH_MAX + 64], s1[PATH_MAX + 64];
    snprintf(dir, sizeof dir, "%s/unsure", ROOT);
    snprintf(s0, sizeof s0, "%s/st0", dir);
    snprintf(s1, sizeof s1, "%s/st1", dir);
    CHECK(mkdir(dir, 0755) == 0, "mkdir unsure");
    CHECK(chunk_store_init(s0, NULL, NULL) == 0, "init st0");
    CHECK(chunk_store_init(s1, NULL, NULL) == 0, "init st1");

    chunk_geom g;
    CHECK(chunk_geom_make(&g, 2, 1, 4096, 4096 * 2 + 55) == 0, "geom");
    size_t clen = (size_t)chunk_pos_len(&g, 0);
    uint8_t *cells = malloc(clen);
    fill_pattern(cells, clen, 7777);

    chunk_ident id_unsure, id_sure;
    make_id(&id_unsure, 10, 1, 0);
    make_id(&id_sure, 11, 2, 0);

    /* Put both on st0 and raise EPOCH twice so both 4 KiB EPOCH slots are populated. */
    chunk_store *st0 = NULL;
    CHECK(chunk_store_open(s0, NULL, &st0) == 0, "open st0");
    CHECK(chunk_store_put(st0, &id_unsure, &g, 1, cells, NULL) == 0, "put unsure vol 10");
    CHECK(chunk_store_raise(st0, 10, 2) == 0, "raise vol 10 to 2");
    CHECK(chunk_store_put(st0, &id_sure, &g, 1, cells, NULL) == 0, "put sure vol 11");
    CHECK(chunk_store_raise(st0, 11, 2) == 0, "raise vol 11 to 2");
    chunk_store_close(st0);

    /* Corrupt page 0 of st0/s10/EPOCH so vol 10 opens UNSURE while vol 11 remains SURE. */
    char ep_path[PATH_MAX + 96];
    snprintf(ep_path, sizeof ep_path, "%s/s10/EPOCH", s0);
    {
        int fd = open(ep_path, O_RDWR);
        uint8_t b = 0;
        CHECK(fd >= 0 && pread(fd, &b, 1, 0) == 1, "read EPOCH slot 0");
        b ^= 0xffu;
        CHECK(fd >= 0 && pwrite(fd, &b, 1, 0) == 1 && close(fd) == 0, "corrupt EPOCH slot 0");
    }

    const char *pp[2] = {s0, s1};
    int pref[2] = {0, 30};
    chunk_be_opts bo = {.min_up = 2, .strict = true, .store_pref = pref};
    chunk_backend *be = NULL;
    CHECK(chunk_be_open2(&be, pp, 2, NULL, &bo) == 0, "reopen backend");
    CHECK(chunk_store_epoch_unsure(chunk_be_store(be, 0), 10), "vol 10 is unsure on st0");
    CHECK(!chunk_store_epoch_unsure(chunk_be_store(be, 0), 11), "vol 11 is sure on st0");

    /* Moving vol 10 returns CHUNK_ERR_EPOCH_UNSURE and leaves st1 empty; vol 11 moves cleanly! */
    chunk_move_opts mo = {.rate_bps = UINT64_MAX};
    CHECK(chunk_move(be, &id_unsure, 0, 1, &mo) == CHUNK_ERR_EPOCH_UNSURE, "unsure vol 10 refused with EPOCH_UNSURE");
    CHECK(verify_store_chunk(chunk_be_store(be, 0), &id_unsure, cells, clen, NULL) == 0, "vol 10 intact on st0");
    CHECK(verify_store_chunk(chunk_be_store(be, 1), &id_unsure, NULL, 0, NULL) == TSDB_ERR_NOTFOUND, "vol 10 absent on st1");

    CHECK(chunk_move(be, &id_sure, 0, 1, &mo) == 0, "sure vol 11 moves cleanly beside unsure vol 10");
    CHECK(verify_store_chunk(chunk_be_store(be, 1), &id_sure, cells, clen, NULL) == 0, "vol 11 whole on st1");
    CHECK(verify_store_chunk(chunk_be_store(be, 0), &id_sure, NULL, 0, NULL) == TSDB_ERR_NOTFOUND, "vol 11 unlinked on st0");

    /* Once vol 10's epoch is raised authoritatively, its move succeeds too. */
    CHECK(chunk_be_see_epoch(be, 10, 5) == 0, "authoritative raise clears unsure vol 10");
    CHECK(chunk_move(be, &id_unsure, 0, 1, &mo) == 0, "vol 10 moves cleanly once sure");
    CHECK(verify_store_chunk(chunk_be_store(be, 1), &id_unsure, cells, clen, NULL) == 0, "vol 10 whole on st1");
    CHECK(chunk_store_epoch(chunk_be_store(be, 1), 10) >= 5, "vol 10 fence >= 5 on st1");
    chunk_be_close(be);

    /* Verify loc_tab stays completely empty (0 entries) for normal non-moved primary puts/reads (no RAM bloat). */
    chunk_be_opts bo_plain = {.min_up = 2, .strict = true};
    CHECK(chunk_be_open2(&be, pp, 2, NULL, &bo_plain) == 0, "open plain backend without store_pref");
    CHECK(atomic_load(&be->loc_cnt) == 0, "plain backend starts with loc_cnt == 0");
    chunk_ident id_p;
    make_id(&id_p, 12, 99, 0);
    int primary = chunk_be_place(be, id_p.oid, id_p.pos);
    CHECK(chunk_be_put(be, primary, &id_p, &g, 1, cells, NULL) == 0, "put on primary store");
    CHECK(atomic_load(&be->loc_cnt) == 0, "primary put leaves loc_cnt == 0");
    chunk_rd *rd = NULL;
    CHECK(chunk_be_rd_open_at(be, &id_p, &rd, NULL) == 0, "read primary chunk");
    chunk_rd_close(rd);
    CHECK(atomic_load(&be->loc_cnt) == 0, "primary read leaves loc_cnt == 0");
    chunk_be_close(be);

    free(cells);
    printf("  unsure EPOCH beside sure volume and zero-overhead loc_tab scoping: ok\n");
}

/* ---------------------------------------------------------------- 6. crashfs power-loss sweep */
static int crashfs_child(const char *dir)
{
    char s0[PATH_MAX + 64], s1[PATH_MAX + 64];
    snprintf(s0, sizeof s0, "%s/st0", dir);
    snprintf(s1, sizeof s1, "%s/st1", dir);
    const char *pp[2] = {s0, s1};
    int pref[2] = {0, 30};
    chunk_be_opts bo = {.min_up = 2, .strict = true, .store_pref = pref};
    chunk_backend *be = NULL;
    if (chunk_be_open2(&be, pp, 2, NULL, &bo) != 0)
        return 2;
    chunk_ident id;
    make_id(&id, 1, 77, 0);
    chunk_move_opts mo = {.rate_bps = UINT64_MAX};
    int rc = chunk_move(be, &id, 0, 1, &mo);
    chunk_be_close(be);
    return rc == 0 ? 0 : 3;
}

static int run_crashfs_child(const char *dir, int warmup, int seed, const char *mut_name)
{
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512], w[16], sd[16];
        snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0:detect_leaks=0",
                 getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
        setenv("ASAN_OPTIONS", asan, 1);
        snprintf(w, sizeof w, "%d", warmup);
        snprintf(sd, sizeof sd, "%d", seed);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", dir, 1);
        setenv("CRASHFS_PROB", "1", 1);
        setenv("CRASHFS_WARMUP", w, 1);
        setenv("CRASHFS_AT_EXIT", "1", 1);
        setenv("CRASHFS_TEAR", "1", 1);
        setenv("CRASHFS_SEED", sd, 1);
        int null = open("/dev/null", O_WRONLY);
        if (null >= 0) {
            dup2(null, 1);
            dup2(null, 2);
            close(null);
        }
        if (mut_name && *mut_name)
            execl(SELF, "test_chunkmove", "--mutant", mut_name, "--child", dir, (char *)NULL);
        else
            execl(SELF, "test_chunkmove", "--child", dir, (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid)
        return -1;
    if (WIFEXITED(st))
        return WEXITSTATUS(st);
    return 100 + WTERMSIG(st);
}

static void setup_crash_case(const char *dir, const chunk_geom *g, const chunk_ident *id, const uint8_t *cells)
{
    rmtree(dir);
    char s0[PATH_MAX + 64], s1[PATH_MAX + 64];
    snprintf(s0, sizeof s0, "%s/st0", dir);
    snprintf(s1, sizeof s1, "%s/st1", dir);
    CHECK(mkdir(dir, 0755) == 0, "mkdir crash case");
    CHECK(chunk_store_init(s0, NULL, NULL) == 0, "init st0");
    CHECK(chunk_store_init(s1, NULL, NULL) == 0, "init st1");
    chunk_store *st0 = NULL;
    CHECK(chunk_store_open(s0, NULL, &st0) == 0, "open st0");
    CHECK(chunk_store_put(st0, id, g, 3, cells, NULL) == 0, "put on st0");
    CHECK(chunk_store_raise(st0, 1, 5) == 0, "raise st0 fence to 5");
    chunk_store_close(st0);
}

static void test_crashfs(const char *mut_name)
{
    if (access(SHIM, R_OK) != 0) {
        if (getenv("TSDB_REQUIRE_CRASHFS") && strcmp(getenv("TSDB_REQUIRE_CRASHFS"), "1") == 0) {
            CHECK(false, "TSDB_REQUIRE_CRASHFS=1 set but %s missing", SHIM);
            return;
        }
        printf("  crashfs power-loss sweep: skipped (%s not built)\n", SHIM);
        return;
    }

    chunk_geom g;
    CHECK(chunk_geom_make(&g, 2, 1, 4096, 4096 * 4 + 77) == 0, "geom");
    chunk_ident id;
    make_id(&id, 1, 77, 0);
    size_t clen = (size_t)chunk_pos_len(&g, 0);
    uint8_t *cells = malloc(clen);
    fill_pattern(cells, clen, 9009);

    char cdir[PATH_MAX + 32], s0[PATH_MAX + 64], s1[PATH_MAX + 64];
    snprintf(cdir, sizeof cdir, "%s/crash", ROOT);
    snprintf(s0, sizeof s0, "%s/st0", cdir);
    snprintf(s1, sizeof s1, "%s/st1", cdir);

    int f0 = fails;
    int steps = 0, seen_two = 0, seen_s1_only = 0, seen_s0_only = 0;
    for (int w = 0; w < 200; w++) {
        setup_crash_case(cdir, &g, &id, cells);
        int ex = run_crashfs_child(cdir, w, 100 + w, mut_name);
        steps++;

        /* Step A: inspect raw stores INDIVIDUALLY (before dedupe) to guarantee 1 or 2 whole copies, NEVER 0. */
        chunk_store *st0 = NULL, *st1 = NULL;
        int rc0 = chunk_store_open(s0, NULL, &st0);
        int rc1 = chunk_store_open(s1, NULL, &st1);
        CHECK(rc0 == 0 && rc1 == 0, "step %d (ex=%d): stores reopen individually (rc0=%d rc1=%d)", w, ex, rc0, rc1);
        if (rc0 != 0 || rc1 != 0) {
            chunk_store_close(st0);
            chunk_store_close(st1);
            break;
        }
        uint64_t ep0 = 0, ep1 = 0;
        int v0 = verify_store_chunk(st0, &id, cells, clen, &ep0);
        int v1 = verify_store_chunk(st1, &id, cells, clen, &ep1);
        int nwhole = (v0 == 0) + (v1 == 0);
        CHECK(nwhole >= 1, "step %d (ex=%d): ZERO WHOLE COPIES AFTER POWER LOSS (v0=%d v1=%d; moveunlinkfirst!)",
              w, ex, v0, v1);
        if (v1 == 0) {
            CHECK(ep1 == 3, "step %d: target writer epoch=%" PRIu64 " (want 3)", w, ep1);
            CHECK(chunk_store_epoch(st1, 1) >= 5,
                  "step %d: target holds chunk with EPOCH=%" PRIu64 " < volume fence 5!", w,
                  chunk_store_epoch(st1, 1));
        }
        if (v0 == 0 && v1 == 0)
            seen_two++;
        else if (v1 == 0)
            seen_s1_only++;
        else if (v0 == 0)
            seen_s0_only++;
        chunk_store_close(st0);
        chunk_store_close(st1);

        /* Step B: reopen together with pref={0 (fast), 30 (capacity)} -> start-time dedupe must keep wanted copy! */
        const char *pp[2] = {s0, s1};
        int pref[2] = {0, 30};
        chunk_be_opts bo = {.min_up = 2, .strict = true, .store_pref = pref};
        chunk_backend *be = NULL;
        CHECK(chunk_be_open2(&be, pp, 2, NULL, &bo) == 0, "step %d: reopen backend", w);
        if (be) {
            int a0 = verify_store_chunk(chunk_be_store(be, 0), &id, cells, clen, NULL);
            int a1 = verify_store_chunk(chunk_be_store(be, 1), &id, cells, clen, NULL);
            CHECK((a0 == 0) + (a1 == 0) == 1, "step %d: expected exactly 1 copy after dedupe (a0=%d a1=%d)", w, a0, a1);
            if (v1 == 0) {
                CHECK(a1 == 0 && a0 == TSDB_ERR_NOTFOUND,
                      "step %d: dedupe did not keep preferred capacity copy (a0=%d a1=%d; movekeepwrong!)", w, a0, a1);
            }
            /* Step C: if still on st0, completing the move after recovery must succeed cleanly. */
            if (a0 == 0) {
                chunk_move_opts mo = {.rate_bps = UINT64_MAX};
                CHECK(chunk_move(be, &id, 0, 1, &mo) == 0, "step %d: retry move after crash", w);
                CHECK(verify_store_chunk(chunk_be_store(be, 1), &id, cells, clen, NULL) == 0,
                      "step %d: chunk whole on st1 after retry", w);
                CHECK(verify_store_chunk(chunk_be_store(be, 0), &id, NULL, 0, NULL) == TSDB_ERR_NOTFOUND,
                      "step %d: source unlinked after retry", w);
            }
            chunk_be_close(be);
        }

        if (fails > f0)
            break;
        if (ex == 0) {
            /* Child finished every step; run 6 more post-exit rollback seeds to verify final fsyncs. */
            for (int r = 0; r < 6 && fails == f0; r++) {
                setup_crash_case(cdir, &g, &id, cells);
                int rex = run_crashfs_child(cdir, 10000, 500 + r, mut_name);
                CHECK(rex == 0, "post-exit seed %d: child exited %d", r, rex);
                chunk_store *rs0 = NULL, *rs1 = NULL;
                CHECK(chunk_store_open(s0, NULL, &rs0) == 0 && chunk_store_open(s1, NULL, &rs1) == 0,
                      "post-exit seed %d: reopen", r);
                if (rs0 && rs1) {
                    int rv0 = verify_store_chunk(rs0, &id, cells, clen, NULL);
                    int rv1 = verify_store_chunk(rs1, &id, cells, clen, NULL);
                    CHECK(rv1 == 0 && rv0 == TSDB_ERR_NOTFOUND,
                          "post-exit seed %d: target not durable or source came back (rv0=%d rv1=%d)", r, rv0, rv1);
                    CHECK(chunk_store_epoch(rs1, 1) >= 5, "post-exit seed %d: target EPOCH lost", r);
                }
                chunk_store_close(rs0);
                chunk_store_close(rs1);
            }
            break;
        }
    }

    CHECK(seen_s0_only > 0 && seen_two > 0 && seen_s1_only > 0,
          "crashfs sweep hit all move phases (s0_only=%d two=%d s1_only=%d over %d steps)", seen_s0_only, seen_two,
          seen_s1_only, steps);
    free(cells);
    printf("  crashfs power-loss sweep (%d steps: s0_only=%d, two=%d, s1_only=%d + 6 post-exit seeds): ok\n", steps,
           seen_s0_only, seen_two, seen_s1_only);
}

int main(int argc, char **argv)
{
    const char *scratch = "/tmp";
    const char *mut_name = NULL;
    const char *child_dir = NULL;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mutant") == 0 && i + 1 < argc) {
            mut_name = argv[++i];
        } else if (strcmp(argv[i], "--child") == 0 && i + 1 < argc) {
            child_dir = argv[++i];
        } else {
            scratch = argv[i];
        }
    }

#ifdef TSDB_TEST_HOOKS
    if (mut_name) {
        int m = chunkmove_mutant_parse(mut_name);
        if (m < 0) {
            fprintf(stderr, "test_chunkmove: unknown mutant '%s'\n", mut_name);
            return 2;
        }
        chunkmove_mutant = m;
    }
#else
    if (mut_name) {
        fprintf(stderr, "test_chunkmove: mutants require TSDB_TEST_HOOKS\n");
        return 2;
    }
#endif

    if (child_dir)
        return crashfs_child(child_dir);

    if (!realpath(argv[0], SELF))
        snprintf(SELF, sizeof SELF, "%s", argv[0]);
    const char *slash = strrchr(SELF, '/');
    snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", slash ? (int)(slash - SELF) : 1, slash ? SELF : ".");

    snprintf(ROOT, sizeof ROOT, "%s/tsdb_chunkmove.XXXXXX", scratch);
    if (!mkdtemp(ROOT)) {
        fprintf(stderr, "test_chunkmove: mkdtemp %s: %s\n", ROOT, strerror(errno));
        return 1;
    }

    printf("test_chunkmove%s%s\n", mut_name ? " --mutant " : "", mut_name ? mut_name : "");
    test_basic_move();
    test_pacing_and_cancel();
    test_open_dedupe();
    test_concurrent_races();
    test_unsure_and_scoping();
    test_crashfs(mut_name);

    rmtree(ROOT);
    if (fails) {
        fprintf(stderr, "test_chunkmove: %d failure(s)\n", fails);
        return 1;
    }
    printf("test_chunkmove: OK\n");
    return 0;
}
