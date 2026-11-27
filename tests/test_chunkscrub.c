/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Unit, fault-injection (outagefs EIO), pacing, mid-pass restart/SIGKILL, and local-DOWN decode-around tests for the
 * per-store chunk scrubber (src/chunkscrub.[ch]; R12, r12-chunk).
 *
 * Usage: test_chunkscrub [--mutant scrubskipcell|scrubeioclean|scrubrestart] [SCRATCH]
 */
#define _GNU_SOURCE
#include "chunk.h"
#include "chunk_priv.h"
#include "chunkscrub.h"
#include "internal.h"
#include "watchdog.h"

#ifndef TSDB_EC
#include <stdio.h>
int main(void)
{
    printf("test_chunkscrub: SKIPPED (this build has no erasure codec: it is x86-64 only)\n");
    return 0;
}
#else

#ifdef __linux__
#include "outagefs_ctl.h"
#include <sys/prctl.h>
#endif

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static int failures;
static char scratch_base[PATH_MAX / 2];
static const char *g_self_exe;

#define CHECK(cond, ...)                                                                                               \
    do {                                                                                                               \
        if (!(cond)) {                                                                                                 \
            fprintf(stderr, "FAIL %s:%d: %s: ", __FILE__, __LINE__, #cond);                                            \
            fprintf(stderr, __VA_ARGS__);                                                                              \
            fputc('\n', stderr);                                                                                       \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

static void safe_kill(pid_t pid, int sig)
{
    if (pid <= 0)
        return;
    (void)kill(pid, sig);
}

static uint64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

static void rm_rf(const char *path)
{
    struct stat st;
    if (lstat(path, &st) != 0)
        return;
    if (!S_ISDIR(st.st_mode)) {
        (void)unlink(path);
        return;
    }
    DIR *d = opendir(path);
    if (d) {
        struct dirent *e;
        while ((e = readdir(d)) != NULL) {
            if (strcmp(e->d_name, ".") == 0 || strcmp(e->d_name, "..") == 0)
                continue;
            char sub[PATH_MAX];
            int n = snprintf(sub, sizeof sub, "%s/%s", path, e->d_name);
            if (n > 0 && (size_t)n < sizeof sub)
                rm_rf(sub);
        }
        closedir(d);
    }
    (void)rmdir(path);
}

static void mk_fresh(const char *tag, char *out, size_t cap)
{
    static int seq;
    snprintf(out, cap, "%s/cscrub-%d-%s-%d", scratch_base, (int)getpid(), tag, seq++);
    rm_rf(out);
    if (mkdir(out, 0755) != 0) {
        perror("mkdir");
        exit(1);
    }
}

static const uint32_t TEST_VOL = 7;
static const uint8_t TEST_VUUID[16] = {0xc5, 0x01, 0x02, 0x03, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

struct test_obj {
    uint8_t oid[16];
    chunk_geom g;
    uint8_t *data;
    int pos_on_st0; /* position placed on store 0 (always >= 0 for 3-store 2+1) */
};

struct test_world {
    char dir[PATH_MAX];
    char spaths[3][PATH_MAX + 32];
    chunk_backend *be;
    struct test_obj *objs;
    int nobjs;
    uint64_t st0_total_bytes;
};

static void world_make(struct test_world *w, const char *tag, int nobjs, uint32_t cell, uint64_t obj_len)
{
    memset(w, 0, sizeof *w);
    mk_fresh(tag, w->dir, sizeof w->dir);
    const char *pp[3];
    for (int i = 0; i < 3; i++) {
        snprintf(w->spaths[i], sizeof w->spaths[i], "%s/st%d", w->dir, i);
        CHECK(chunk_store_init(w->spaths[i], NULL, NULL) == 0, "init st%d", i);
        pp[i] = w->spaths[i];
    }
    CHECK(chunk_be_open(&w->be, pp, 3, NULL) == 0, "chunk_be_open %s", tag);
    w->objs = calloc((size_t)nobjs, sizeof *w->objs);
    w->nobjs = nobjs;
    CHECK(w->objs != NULL, "calloc objs");

    for (int i = 0; i < nobjs; i++) {
        struct test_obj *o = &w->objs[i];
        memset(o->oid, 0, 16);
        o->oid[0] = (uint8_t)(i >> 8);
        o->oid[1] = (uint8_t)(i & 0xff);
        o->oid[2] = 0xa5;
        o->oid[15] = (uint8_t)(i * 37 + 11);
        CHECK(chunk_geom_make(&o->g, 2, 1, cell, obj_len) == 0, "geom %d", i);
        o->data = malloc((size_t)obj_len);
        CHECK(o->data != NULL, "malloc data %d", i);
        uint64_t rng = (uint64_t)(i + 1) * 0x9e3779b97f4a7c15ull;
        for (uint64_t j = 0; j < obj_len; j++) {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            o->data[j] = (uint8_t)rng;
        }
        CHECK(chunk_be_put_object(w->be, TEST_VOL, TEST_VUUID, o->oid, &o->g, 1, o->data, ~0u, NULL) == 0,
              "put object %d", i);
        o->pos_on_st0 = -1;
        for (unsigned p = 0; p < chunk_npos(&o->g); p++) {
            if (chunk_be_pos_store(w->be, o->oid, p) == 0) {
                o->pos_on_st0 = (int)p;
                w->st0_total_bytes += chunk_file_size(&o->g, p);
            }
        }
        CHECK(o->pos_on_st0 >= 0, "object %d placed on store 0", i);
    }
}

static void world_free(struct test_world *w)
{
    if (w->be)
        chunk_be_close(w->be);
    for (int i = 0; i < w->nobjs; i++)
        free(w->objs[i].data);
    free(w->objs);
    rm_rf(w->dir);
}

static void chunk_path(const struct test_world *w, int store, const struct test_obj *o, unsigned pos, char *out,
                       size_t cap)
{
    char hex[33];
    chunk_oid_hex(o->oid, hex);
    snprintf(out, cap, "%s/s%" PRIu32 "/o%s.%u.chunk", w->spaths[store], TEST_VOL, hex, pos);
}

static int flip_file_byte(const char *path, uint64_t off)
{
    int fd = open(path, O_RDWR | O_CLOEXEC);
    if (fd < 0)
        return -1;
    uint8_t b = 0;
    if (pread(fd, &b, 1, (off_t)off) != 1) {
        close(fd);
        return -1;
    }
    b ^= 0x5au;
    int rc = (pwrite(fd, &b, 1, (off_t)off) == 1 && fsync(fd) == 0) ? 0 : -1;
    close(fd);
    return rc;
}

/* Report collector for unit tests */
struct rep_log {
    pthread_mutex_t mu;
    chunk_scrub_damage items[256];
    int count;
    int fail_first; /* return TSDB_ERR_UNAVAILABLE for first fail_first calls */
    int attempts;
    int out_calls;
    uint64_t out_day_dmg;
    atomic_bool paused;
};

static int on_damage_rec(void *ctx, const chunk_scrub_damage *dmg)
{
    struct rep_log *rl = ctx;
    pthread_mutex_lock(&rl->mu);
    rl->attempts++;
    if (rl->fail_first > 0) {
        rl->fail_first--;
        pthread_mutex_unlock(&rl->mu);
        return TSDB_ERR_UNAVAILABLE;
    }
    if (rl->count < 256)
        rl->items[rl->count++] = *dmg;
    pthread_mutex_unlock(&rl->mu);
    return 0;
}

static void on_out_rec(void *ctx, int store_idx, uint64_t day_damaged)
{
    (void)store_idx;
    struct rep_log *rl = ctx;
    pthread_mutex_lock(&rl->mu);
    rl->out_calls++;
    rl->out_day_dmg = day_damaged;
    pthread_mutex_unlock(&rl->mu);
}

static bool is_paused_rec(void *ctx, int store_idx)
{
    (void)store_idx;
    struct rep_log *rl = ctx;
    return atomic_load(&rl->paused);
}

static int count_reports_for(struct rep_log *rl, const uint8_t oid[16], uint16_t pos, uint64_t *bmap_out, int *err_out)
{
    int n = 0;
    pthread_mutex_lock(&rl->mu);
    for (int i = 0; i < rl->count; i++) {
        if (rl->items[i].pos == pos && memcmp(rl->items[i].oid, oid, 16) == 0) {
            n++;
            if (bmap_out)
                *bmap_out = rl->items[i].bad_cells;
            if (err_out)
                *err_out = rl->items[i].err;
        }
    }
    pthread_mutex_unlock(&rl->mu);
    return n;
}

static bool wait_passes(chunk_scrub *sc, uint64_t want, uint64_t timeout_ms)
{
    uint64_t t0 = now_ms();
    while (now_ms() - t0 < timeout_ms) {
        chunk_scrub_stats st;
        chunk_scrub_get_stats(sc, &st);
        if (st.passes_done >= want)
            return true;
        usleep(10000);
    }
    return false;
}

/* 1. 50 objects: flip byte in data cell (last cell), parity cell (last cell), sums table, footer -> each found within
 * one pass and reported exactly once. */
static void test_flips_and_once(void)
{
    struct test_world w;
    world_make(&w, "flips", 50, 16384, 2 * 16384 * 3 + 1234);

    struct rep_log rl = {.mu = PTHREAD_MUTEX_INITIALIZER};
    chunk_scrub_cfg cfg = {
        .store_idx = 0,
        .scrub_days = 14,
        .scrub_mb_s = 200,
        .scrub_out_count = 8,
        .pass_interval_ms = 60000,
        .be = w.be,
        .on_damage = on_damage_rec,
        .on_out = on_out_rec,
        .ctx = &rl,
    };

    /* Clean pass first. */
    chunk_scrub *sc = chunk_scrub_start(chunk_be_store(w.be, 0), &cfg);
    CHECK(sc != NULL, "chunk_scrub_start clean");
    CHECK(wait_passes(sc, 1, 15000), "clean pass completed");
    chunk_scrub_stats st;
    chunk_scrub_get_stats(sc, &st);
    CHECK(st.chunks_total == 50 && st.damaged_total == 0 && st.bytes_total == w.st0_total_bytes,
          "clean pass chunks=%" PRIu64 " dmg=%" PRIu64 " bytes=%" PRIu64 " want=%" PRIu64, st.chunks_total,
          st.damaged_total, st.bytes_total, w.st0_total_bytes);
    CHECK(rl.count == 0, "clean pass 0 reports");
    chunk_scrub_stop(sc);

    /* Pick 4 distinct objects on store 0: data cell, parity cell, sums table, footer. */
    int idx_data = -1, idx_parity = -1, idx_sums = -1, idx_ftr = -1;
    for (int i = 0; i < w.nobjs; i++) {
        if (w.objs[i].pos_on_st0 < 2 && idx_data < 0)
            idx_data = i;
        else if (w.objs[i].pos_on_st0 == 2 && idx_parity < 0)
            idx_parity = i;
        else if (idx_sums < 0 && i != idx_data && i != idx_parity)
            idx_sums = i;
        else if (idx_ftr < 0 && i != idx_data && i != idx_parity && i != idx_sums)
            idx_ftr = i;
    }
    CHECK(idx_data >= 0 && idx_parity >= 0 && idx_sums >= 0 && idx_ftr >= 0, "picked 4 distinct targets");

    char p[PATH_MAX + 96];
    /* Flip last cell of data chunk (catches scrubskipcell!). */
    const struct test_obj *od = &w.objs[idx_data];
    uint64_t d_ncells = chunk_pos_cells(&od->g, (unsigned)od->pos_on_st0);
    uint64_t d_last_len = chunk_pos_cell_len(&od->g, (unsigned)od->pos_on_st0, d_ncells - 1);
    uint64_t d_last_off = CHUNK_HDR_BYTES + chunk_pos_len(&od->g, (unsigned)od->pos_on_st0) - d_last_len;
    chunk_path(&w, 0, od, (unsigned)od->pos_on_st0, p, sizeof p);
    CHECK(flip_file_byte(p, d_last_off + d_last_len / 2) == 0, "flip data last cell");

    /* Flip last cell of parity chunk. */
    const struct test_obj *op = &w.objs[idx_parity];
    uint64_t p_ncells = chunk_pos_cells(&op->g, (unsigned)op->pos_on_st0);
    uint64_t p_last_len = chunk_pos_cell_len(&op->g, (unsigned)op->pos_on_st0, p_ncells - 1);
    uint64_t p_last_off = CHUNK_HDR_BYTES + chunk_pos_len(&op->g, (unsigned)op->pos_on_st0) - p_last_len;
    chunk_path(&w, 0, op, (unsigned)op->pos_on_st0, p, sizeof p);
    CHECK(flip_file_byte(p, p_last_off + p_last_len / 2) == 0, "flip parity last cell");

    /* Flip sums table byte. */
    const struct test_obj *os = &w.objs[idx_sums];
    uint64_t sums_off = CHUNK_HDR_BYTES + chunk_pos_len(&os->g, (unsigned)os->pos_on_st0);
    chunk_path(&w, 0, os, (unsigned)os->pos_on_st0, p, sizeof p);
    CHECK(flip_file_byte(p, sums_off + 3) == 0, "flip sums table");

    /* Flip footer byte. */
    const struct test_obj *of = &w.objs[idx_ftr];
    uint64_t fsize = chunk_file_size(&of->g, (unsigned)of->pos_on_st0);
    chunk_path(&w, 0, of, (unsigned)of->pos_on_st0, p, sizeof p);
    CHECK(flip_file_byte(p, fsize - 4) == 0, "flip footer");

    /* Remove SCRUB so we run one fresh pass and check each of the 4 is reported exactly once. */
    char spath[PATH_MAX + 64];
    snprintf(spath, sizeof spath, "%s/SCRUB", w.spaths[0]);
    (void)unlink(spath);

    sc = chunk_scrub_start(chunk_be_store(w.be, 0), &cfg);
    CHECK(sc != NULL, "chunk_scrub_start flips");
    CHECK(wait_passes(sc, 1, 15000), "flips pass completed");
    chunk_scrub_get_stats(sc, &st);
    chunk_scrub_stop(sc);

    uint64_t bm_d = 0, bm_p = 0, bm_s = 0, bm_f = 0;
    int er_d = 0, er_p = 0, er_s = 0, er_f = 0;
    int cd = count_reports_for(&rl, od->oid, (uint16_t)od->pos_on_st0, &bm_d, &er_d);
    int cp = count_reports_for(&rl, op->oid, (uint16_t)op->pos_on_st0, &bm_p, &er_p);
    int cs = count_reports_for(&rl, os->oid, (uint16_t)os->pos_on_st0, &bm_s, &er_s);
    int cf = count_reports_for(&rl, of->oid, (uint16_t)of->pos_on_st0, &bm_f, &er_f);

    CHECK(st.damaged_total == 4 && rl.count == 4, "expected 4 damaged reports, got dmg=%" PRIu64 " rep=%d",
          st.damaged_total, rl.count);
    CHECK(cd == 1 && er_d == TSDB_ERR_CORRUPT && (bm_d & (1ull << (d_ncells - 1))) != 0,
          "data last cell: cnt=%d err=%d bmap=0x%" PRIx64, cd, er_d, bm_d);
    CHECK(cp == 1 && er_p == TSDB_ERR_CORRUPT && (bm_p & (1ull << (p_ncells - 1))) != 0,
          "parity last cell: cnt=%d err=%d bmap=0x%" PRIx64, cp, er_p, bm_p);
    CHECK(cs == 1 && er_s == TSDB_ERR_CORRUPT, "sums flip: cnt=%d err=%d", cs, er_s);
    CHECK(cf == 1 && er_f == TSDB_ERR_CORRUPT, "footer flip: cnt=%d err=%d", cf, er_f);
    CHECK(chunk_be_store_state(w.be, 0) == CHUNK_STORE_UP, "4 <= 8 so store still UP");

    printf("  flips_and_once: ok (50 objects, 4 flips reported once each)\n");
    world_free(&w);
}

/* 2. EIO under liboutagefs.so is damage, NEVER counted clean (catches scrubeioclean!). */
static void test_eio_damage(void)
{
#ifdef __linux__
    const char *ctl = getenv("OUTAGEFS_CTL");
    const char *edir = outagefs_dir("eio");
    if (ctl && edir) {
        char sdir[PATH_MAX + 32];
        snprintf(sdir, sizeof sdir, "%s/eio_st", edir);
        rm_rf(sdir);
        CHECK(chunk_store_init(sdir, NULL, NULL) == 0, "eio store init");
        const char *pp[1] = {sdir};
        chunk_backend *be = NULL;
        CHECK(chunk_be_open(&be, pp, 1, NULL) == 0, "eio be open");

        chunk_geom g;
        CHECK(chunk_geom_make(&g, 2, 1, 16384, 2 * 16384 * 2) == 0, "eio geom");
        uint8_t *data = calloc(1, (size_t)g.len);
        CHECK(data != NULL, "eio data");
        for (int i = 0; i < 16; i++) {
            uint8_t oid[16] = {0xe1, (uint8_t)i, 0, 1};
            CHECK(chunk_be_put_object(be, TEST_VOL, TEST_VUUID, oid, &g, 1, data, 1u, NULL) == 0, "eio put %d", i);
        }
        free(data);

        /* First: arm EIO before listing starts -> readdir EIO must never complete a clean pass (passes_done == 0). */
        char cmd[PATH_MAX + 96], out[256];
        snprintf(cmd, sizeof cmd, "eio %s", edir);
        CHECK(outagefs_cmd(ctl, cmd, out, sizeof out) == 0, "outagefs eio (list): %s", out);
        struct rep_log rl0 = {.mu = PTHREAD_MUTEX_INITIALIZER};
        chunk_scrub_cfg cfg0 = {
            .store_idx = 0,
            .scrub_mb_s = 200,
            .scrub_out_count = 100,
            .pass_interval_ms = 60000,
            .on_damage = on_damage_rec,
            .ctx = &rl0,
        };
        chunk_scrub *sc0 = chunk_scrub_start(chunk_be_store(be, 0), &cfg0);
        CHECK(sc0 != NULL, "eio list scrub start");
        usleep(150000);
        chunk_scrub_stats st0 = {0};
        chunk_scrub_get_stats(sc0, &st0);
        chunk_scrub_stop(sc0);
        CHECK(st0.passes_done == 0, "list EIO must not count as a clean pass (passes=%" PRIu64 ")", st0.passes_done);

        snprintf(cmd, sizeof cmd, "heal %s", edir);
        CHECK(outagefs_cmd(ctl, cmd, out, sizeof out) == 0, "outagefs heal (list): %s", out);

        /* Second: let listing succeed and chunk 0 scrub, then arm EIO mid-pass so remaining chunks hit real
         * openat/pread EIO via liboutagefs.so -> every EIO must be reported as TSDB_ERR_IO damage, never clean! */
        struct rep_log rl = {.mu = PTHREAD_MUTEX_INITIALIZER};
        chunk_scrub_cfg cfg = {
            .store_idx = 0,
            .rate_bps_override = 256ull * 1024ull, /* ~256 KiB/s so chunk 0..15 takes ~2 s */
            .scrub_out_count = 100,
            .pass_interval_ms = 60000,
            .on_damage = on_damage_rec,
            .ctx = &rl,
        };
        chunk_scrub *sc = chunk_scrub_start(chunk_be_store(be, 0), &cfg);
        CHECK(sc != NULL, "eio scrub start");
        uint64_t t0 = now_ms();
        chunk_scrub_stats st = {0};
        while (now_ms() - t0 < 10000) {
            chunk_scrub_get_stats(sc, &st);
            if (st.chunks_total >= 1)
                break;
            usleep(5000);
        }
        CHECK(st.chunks_total >= 1 && st.chunks_total < 14, "mid-pass before EIO: chunks=%" PRIu64, st.chunks_total);
        uint64_t before_eio = st.chunks_total;
        snprintf(cmd, sizeof cmd, "eio %s", edir);
        CHECK(outagefs_cmd(ctl, cmd, out, sizeof out) == 0, "outagefs eio (mid-pass): %s", out);
        CHECK(wait_passes(sc, 1, 15000), "mid-pass EIO finished pass");
        chunk_scrub_get_stats(sc, &st);
        chunk_scrub_stop(sc);

        snprintf(cmd, sizeof cmd, "heal %s", edir);
        CHECK(outagefs_cmd(ctl, cmd, out, sizeof out) == 0, "outagefs heal (mid-pass): %s", out);

        int io_reports = 0;
        for (int i = 0; i < rl.count; i++)
            if (rl.items[i].err == TSDB_ERR_IO)
                io_reports++;
        uint64_t min_expected = st.chunks_total > before_eio + 1 ? st.chunks_total - before_eio - 1 : 1;
        CHECK(st.chunks_total == 16 && st.damaged_total >= min_expected && (uint64_t)io_reports == st.damaged_total,
              "outagefs EIO must be damage (never clean): chunks=%" PRIu64 " before=%" PRIu64 " dmg=%" PRIu64
              " io_reports=%d",
              st.chunks_total, before_eio, st.damaged_total, io_reports);

        chunk_be_close(be);
        rm_rf(sdir);
        printf("  eio_damage (outagefs): ok (%" PRIu64 " EIO chunks reported as TSDB_ERR_IO damage)\n",
               st.damaged_total);
        return;
    }
#endif
    /* Fallback when outagefs is unavailable (e.g. non-Linux): inject chunk_fault_read EIO. */
    struct test_world w;
    world_make(&w, "eiofb", 6, 16384, 2 * 16384 * 2);
    struct rep_log rl = {.mu = PTHREAD_MUTEX_INITIALIZER};
    chunk_scrub_cfg cfg = {
        .store_idx = 0,
        .scrub_mb_s = 200,
        .scrub_out_count = 100,
        .pass_interval_ms = 60000,
        .on_damage = on_damage_rec,
        .ctx = &rl,
    };
    chunk_fault_read = 3;
    chunk_scrub *sc = chunk_scrub_start(chunk_be_store(w.be, 0), &cfg);
    CHECK(sc != NULL, "eio fallback start");
    CHECK(wait_passes(sc, 1, 10000), "eio fallback pass");
    chunk_scrub_stats st;
    chunk_scrub_get_stats(sc, &st);
    chunk_scrub_stop(sc);
    chunk_fault_read = 0;
    CHECK(st.damaged_total >= 1 && rl.count >= 1 && rl.items[0].err == TSDB_ERR_IO,
          "eio fallback: dmg=%" PRIu64 " rep=%d", st.damaged_total, rl.count);
    world_free(&w);
    printf("  eio_damage (fault hook): ok\n");
}

/* 3. Pace holds within 20% of scrub_mb_s, and pause halts scrubbing without post-pause burst. */
static void test_pace_and_pause(void)
{
    struct test_world w;
    /* 50 objects x ~160 KiB per chunk on store 0 = ~8.0 MiB -> ~2.0 s at 4 MiB/s. */
    world_make(&w, "pace", 50, 32768, 2 * 32768 * 5);

    struct rep_log rl = {.mu = PTHREAD_MUTEX_INITIALIZER};
    atomic_init(&rl.paused, true);

    chunk_scrub_cfg cfg = {
        .store_idx = 0,
        .scrub_days = 14,
        .scrub_mb_s = 4,
        .scrub_out_count = 8,
        .pass_interval_ms = 60000,
        .on_damage = on_damage_rec,
        .is_paused = is_paused_rec,
        .ctx = &rl,
    };

    chunk_scrub *sc = chunk_scrub_start(chunk_be_store(w.be, 0), &cfg);
    CHECK(sc != NULL, "pace scrub start");

    /* While paused, 0 chunks should be scrubbed. */
    usleep(250000);
    chunk_scrub_stats st;
    chunk_scrub_get_stats(sc, &st);
    CHECK(st.chunks_total == 0 && st.bytes_total == 0, "paused: chunks=%" PRIu64 " bytes=%" PRIu64, st.chunks_total,
          st.bytes_total);

    /* Unpause and measure exact pass duration at 4 MiB/s. */
    uint64_t t0 = now_ms();
    atomic_store(&rl.paused, false);
    CHECK(wait_passes(sc, 1, 15000), "paced pass completed");
    uint64_t dt_ms = now_ms() - t0;
    chunk_scrub_get_stats(sc, &st);
    chunk_scrub_stop(sc);

    double rate_mb_s = dt_ms > 0 ? ((double)st.bytes_total / (1024.0 * 1024.0)) / ((double)dt_ms / 1000.0) : 0.0;
    CHECK(st.chunks_total == 50 && st.bytes_total == w.st0_total_bytes, "paced chunks=%" PRIu64 " bytes=%" PRIu64,
          st.chunks_total, st.bytes_total);
    CHECK(rate_mb_s <= 4.0 * 1.20,
          "pace cap violated: %.2f MiB/s (want <= 4.00 + 20%%, bytes=%" PRIu64 ", dt=%" PRIu64 " ms)",
          rate_mb_s, st.bytes_total, dt_ms);

    printf("  pace_and_pause: ok (%.2f MiB/s vs target 4.00 MiB/s in %" PRIu64 " ms)\n", rate_mb_s, dt_ms);
    world_free(&w);
}

/* 4. Mid-pass close/restart AND mid-pass SIGKILL/restart resume from SCRUB cursor (< 1.2x one pass). */
static void test_restart_resume(void)
{
    struct test_world w;
    /* 50 objects x ~160 KiB = ~8.0 MiB on store 0. */
    world_make(&w, "restart", 50, 32768, 2 * 32768 * 5);

    /* Part A: clean stop mid-pass + restart (catches scrubrestart!). */
    chunk_scrub_cfg cfg = {
        .store_idx = 0,
        .scrub_days = 14,
        .scrub_mb_s = 4,
        .scrub_out_count = 8,
        .pass_interval_ms = 60000,
        .save_every_chunks = 2,
    };
    chunk_scrub *sc = chunk_scrub_start(chunk_be_store(w.be, 0), &cfg);
    CHECK(sc != NULL, "restart run 1");
    uint64_t t0 = now_ms();
    chunk_scrub_stats st1 = {0};
    while (now_ms() - t0 < 10000) {
        chunk_scrub_get_stats(sc, &st1);
        if (st1.chunks_total >= 22)
            break;
        usleep(10000);
    }
    chunk_scrub_stop(sc);
    CHECK(st1.chunks_total >= 20 && st1.chunks_total < 45, "stopped mid-pass at %" PRIu64 " chunks", st1.chunks_total);

    /* Restart on the same store: must resume from SCRUB cursor and finish the pass. */
    cfg.scrub_mb_s = 200;
    sc = chunk_scrub_start(chunk_be_store(w.be, 0), &cfg);
    CHECK(sc != NULL, "restart run 2");
    CHECK(wait_passes(sc, 1, 10000), "restart run 2 pass done");
    chunk_scrub_stats st2 = {0};
    chunk_scrub_get_stats(sc, &st2);
    chunk_scrub_stop(sc);

    uint64_t total_read = st1.bytes_total + st2.bytes_total;
    double ratio = w.st0_total_bytes > 0 ? (double)total_read / (double)w.st0_total_bytes : 99.0;
    CHECK(ratio < 1.2, "restart bytes read ratio %.3f >= 1.2 (run1=%" PRIu64 " run2=%" PRIu64 " pass=%" PRIu64 ")",
          ratio, st1.bytes_total, st2.bytes_total, w.st0_total_bytes);

    /* Part B: corrupt SCRUB file -> logs and restarts full pass cleanly (never fatal). */
    char spath[PATH_MAX + 64];
    snprintf(spath, sizeof spath, "%s/SCRUB", w.spaths[0]);
    int fd = open(spath, O_WRONLY | O_TRUNC | O_CREAT | O_CLOEXEC, 0644);
    CHECK(fd >= 0, "open SCRUB to corrupt");
    if (fd >= 0) {
        (void)!write(fd, "tsdb-chunk-scrub 1\ncorrupt garbage\n", 35);
        close(fd);
    }
    sc = chunk_scrub_start(chunk_be_store(w.be, 0), &cfg);
    CHECK(sc != NULL, "corrupt SCRUB restart");
    CHECK(wait_passes(sc, 1, 10000), "corrupt SCRUB full pass");
    chunk_scrub_stats st3 = {0};
    chunk_scrub_get_stats(sc, &st3);
    chunk_scrub_stop(sc);
    CHECK(st3.chunks_total == 50 && st3.bytes_total == w.st0_total_bytes,
          "corrupt SCRUB restarted full pass: chunks=%" PRIu64, st3.chunks_total);

    /* Part C: real fork() + SIGKILL mid-pass (store closed in parent first so flock transfers cleanly!). */
    char s0path[PATH_MAX + 32];
    snprintf(s0path, sizeof s0path, "%s", w.spaths[0]);
    uint64_t pass_bytes = w.st0_total_bytes;
    chunk_be_close(w.be);
    w.be = NULL;
    (void)unlink(spath);

    int pipefd[2];
    CHECK(pipe(pipefd) == 0, "pipe for sigkill child");
    char fdstr[32];
    snprintf(fdstr, sizeof fdstr, "%d", pipefd[1]);
    pid_t pid = fork();
    if (pid == 0) {
        close(pipefd[0]);
        execl(g_self_exe, g_self_exe, "--kill-child", s0path, fdstr, (char *)NULL);
        _exit(127);
    }
    close(pipefd[1]);
    uint64_t child_bytes = 0;
    CHECK(pid > 0, "fork sigkill child");
    struct pollfd pfd = {.fd = pipefd[0], .events = POLLIN};
    int pr = poll(&pfd, 1, 15000);
    ssize_t rn = pr > 0 ? read(pipefd[0], &child_bytes, sizeof child_bytes) : -1;
    close(pipefd[0]);
    CHECK(pr > 0 && rn == (ssize_t)sizeof child_bytes && child_bytes > 0, "child reported mid-pass bytes (pr=%d rn=%zd)",
          pr, rn);
    /* Wait briefly for save_every_chunks=2 SCRUB flush, then SIGKILL immediately. */
    usleep(50000);
    safe_kill(pid, SIGKILL);
    int wstatus = 0;
    (void)waitpid(pid, &wstatus, 0);
    CHECK(WIFSIGNALED(wstatus) && WTERMSIG(wstatus) == SIGKILL, "child killed with SIGKILL");

    chunk_store *pst = NULL;
    CHECK(chunk_store_open(s0path, NULL, &pst) == 0, "reopen store after SIGKILL");
    if (pst) {
        sc = chunk_scrub_start(pst, &cfg);
        CHECK(sc != NULL, "resume after SIGKILL");
        CHECK(wait_passes(sc, 1, 10000), "post-SIGKILL pass done");
        chunk_scrub_stats st_k = {0};
        chunk_scrub_get_stats(sc, &st_k);
        chunk_scrub_stop(sc);
        chunk_store_close(pst);

        double kratio = (double)(child_bytes + st_k.bytes_total) / (double)pass_bytes;
        CHECK(kratio < 1.2, "SIGKILL resume ratio %.3f >= 1.2 (child=%" PRIu64 " parent=%" PRIu64 " pass=%" PRIu64 ")",
              kratio, child_bytes, st_k.bytes_total, pass_bytes);
        printf("  restart_resume: ok (stop ratio=%.2fx, SIGKILL ratio=%.2fx, corrupt SCRUB ok)\n", ratio, kratio);
    }
    world_free(&w);
}

static int run_kill_child(const char *s0path, int pipe_wfd)
{
    th_watchdog("test_chunkscrub-child", 30);
#ifdef __linux__
    (void)prctl(PR_SET_PDEATHSIG, SIGKILL);
#endif
    chunk_store *cst = NULL;
    if (chunk_store_open(s0path, NULL, &cst) != 0)
        return 2;
    chunk_scrub_cfg ccfg = {
        .store_idx = 0,
        .scrub_days = 14,
        .scrub_mb_s = 4,
        .pass_interval_ms = 60000,
        .save_every_chunks = 2,
    };
    chunk_scrub *csc = chunk_scrub_start(cst, &ccfg);
    if (!csc)
        return 3;
    for (;;) {
        chunk_scrub_stats cst_s;
        chunk_scrub_get_stats(csc, &cst_s);
        if (cst_s.chunks_total >= 20) {
            uint64_t b = cst_s.bytes_total;
            (void)!write(pipe_wfd, &b, sizeof b);
            break;
        }
        usleep(10000);
    }
    /* Wait until parent sends SIGKILL (no clean stop). */
    for (;;)
        usleep(100000);
}

static int count_store0_cb(void *ctx, const chunk_listed *c)
{
    int *cnt = ctx;
    if (c->store == 0)
        (*cnt)++;
    return 0;
}

/* 5. UTC-day damage count > scrub_out_count marks store DOWN locally, suspends walking the failing store so it cannot
 *    flood logs or leaders, and reads decode around it; also verifies unreachable leader retry and that
 *    chunk_be_store/list_names skip the DOWN store. */
static void test_out_count_and_retry(void)
{
    struct test_world w;
    world_make(&w, "outdown", 16, 16384, 2 * 16384 * 2 + 500);

    /* Corrupt 14 chunks on store 0 (> default scrub_out_count = 8): once day_damaged hits 9, store must be marked
     * DOWN_DAMAGED and the scrubber must stop walking instead of continuing through the remaining 5 corrupt chunks! */
    for (int i = 0; i < 14; i++) {
        char p[PATH_MAX + 96];
        chunk_path(&w, 0, &w.objs[i], (unsigned)w.objs[i].pos_on_st0, p, sizeof p);
        CHECK(flip_file_byte(p, CHUNK_HDR_BYTES + 10) == 0, "corrupt chunk %d", i);
    }

    struct rep_log rl = {.mu = PTHREAD_MUTEX_INITIALIZER, .fail_first = 2};
    chunk_scrub_cfg cfg = {
        .store_idx = 0,
        .scrub_days = 14,
        .scrub_mb_s = 200,
        .scrub_out_count = 8,
        .pass_interval_ms = 60000,
        .be = w.be,
        .on_damage = on_damage_rec,
        .on_out = on_out_rec,
        .ctx = &rl,
    };

    chunk_scrub *sc = chunk_scrub_start(chunk_be_store(w.be, 0), &cfg);
    CHECK(sc != NULL, "outdown scrub start");
    /* Wait for marked_down and delivery of all 9 already-recorded reports. */
    uint64_t rt0 = now_ms();
    chunk_scrub_stats st = {0};
    while (now_ms() - rt0 < 10000) {
        chunk_scrub_get_stats(sc, &st);
        pthread_mutex_lock(&rl.mu);
        int got = rl.count;
        pthread_mutex_unlock(&rl.mu);
        if (st.marked_down && got >= 9)
            break;
        usleep(20000);
    }
    /* Brief settle to verify scrubber stays suspended and does NOT keep walking remaining corrupt chunks! */
    usleep(250000);
    chunk_scrub_get_stats(sc, &st);
    chunk_scrub_stop(sc);

    CHECK(st.damaged_total == 9 && st.day_damaged == 9,
          "scrubber must stop at day_damaged=9 (not walk all 14): day_damaged=%" PRIu64 " total=%" PRIu64,
          st.day_damaged, st.damaged_total);
    CHECK(st.marked_down && rl.out_calls == 1 && rl.out_day_dmg == 9,
          "marked_down=%d out_calls=%d out_day_dmg=%" PRIu64, st.marked_down, rl.out_calls, rl.out_day_dmg);
    CHECK(chunk_be_store_state(w.be, 0) == CHUNK_STORE_DOWN_DAMAGED, "store 0 state=%d (want DOWN_DAMAGED=%d)",
          chunk_be_store_state(w.be, 0), CHUNK_STORE_DOWN_DAMAGED);
    CHECK(chunk_be_store(w.be, 0) == NULL, "chunk_be_store(0) must be NULL while DOWN_DAMAGED");
    int st0_listed = 0;
    CHECK(chunk_be_list_names(w.be, TEST_VOL, count_store0_cb, &st0_listed) == 0 && st0_listed == 0,
          "chunk_be_list_names must skip DOWN_DAMAGED store 0 (listed=%d)", st0_listed);
    /* The 2 initially failed reports must have been retried and delivered! */
    CHECK(rl.count == 9 && rl.attempts >= 11, "retry delivered all 9 reports (count=%d attempts=%d)", rl.count,
          rl.attempts);

    /* Reads on the 3-store backend MUST decode around locally-DOWN store 0 bit-exact for all 16 objects! */
    for (int i = 0; i < w.nobjs; i++) {
        const struct test_obj *o = &w.objs[i];
        chunk_obj *co = NULL;
        CHECK(chunk_obj_open(w.be, TEST_VOL, TEST_VUUID, o->oid, &o->g, NULL, &co) == 0, "obj_open %d", i);
        if (co) {
            uint8_t *buf = malloc((size_t)o->g.len);
            chunk_report rep;
            memset(&rep, 0, sizeof rep);
            int rc = chunk_obj_read(co, 0, o->g.len, buf, 0, &rep);
            CHECK(rc == 0 && memcmp(buf, o->data, (size_t)o->g.len) == 0, "decode around DOWN store obj %d rc=%d", i,
                  rc);
            if ((unsigned)o->pos_on_st0 < o->g.k) {
                CHECK(rep.st[o->pos_on_st0] == CHUNK_ST_DOWN && rep.decoded_cells > 0,
                      "data pos %d on DOWN store: st=%u decoded=%u", o->pos_on_st0, rep.st[o->pos_on_st0],
                      rep.decoded_cells);
            }
            free(buf);
            chunk_obj_close(co);
        }
    }

    printf("  out_count_and_retry: ok (9 > 8 -> DOWN_DAMAGED & suspended, store skipped, all 16 decoded around)\n");
    world_free(&w);
}

/* 6. Regression test: multi-volume transient list error on vol A must NOT advance cursor into vol B or skip vol A. */
static void test_multivol_transient_err(void)
{
    char dir[PATH_MAX];
    static int seq;
    snprintf(dir, sizeof dir, "%s/multivol-%d", scratch_base, seq++);
    CHECK(mkdir(dir, 0755) == 0, "mkdir multivol");
    char sp[PATH_MAX + 32];
    snprintf(sp, sizeof sp, "%s/st0", dir);
    CHECK(chunk_store_init(sp, NULL, NULL) == 0, "init multivol store");

    chunk_store *st = NULL;
    CHECK(chunk_store_open(sp, NULL, &st) == 0, "open multivol store");
    if (!st)
        return;

    /* Put 5 chunks in volume 7 and 5 chunks in volume 8; flip a byte in volume 7's chunk #2. */
    uint8_t cell[16384];
    memset(cell, 0x3cu, sizeof cell);
    chunk_geom g = {.k = 2, .m = 1, .cell = sizeof cell, .len = (uint64_t)sizeof cell * 2};
    uint8_t bad_oid[16] = {0};
    for (uint32_t vol = 7; vol <= 8; vol++) {
        for (int i = 0; i < 5; i++) {
            uint8_t oid[16] = {0};
            oid[0] = (uint8_t)vol;
            oid[1] = (uint8_t)i;
            chunk_ident id = {.volume = vol, .pos = 0};
            memcpy(id.vuuid, TEST_VUUID, 16);
            memcpy(id.oid, oid, 16);
            CHECK(chunk_store_put(st, &id, &g, 1, cell, NULL) == 0, "put vol %u #%d", vol, i);
            if (vol == 7 && i == 2)
                memcpy(bad_oid, oid, 16);
        }
    }
    char bad_hex[33], bad_path[PATH_MAX + 96];
    chunk_oid_hex(bad_oid, bad_hex);
    snprintf(bad_path, sizeof bad_path, "%s/s7/o%s.0.chunk", sp, bad_hex);
    CHECK(flip_file_byte(bad_path, CHUNK_HDR_BYTES + 16) == 0, "flip vol 7 chunk #2");

    /* Arm one-shot transient list failure on volume 7. Round 1 must abort without touching vol 8 or advancing cursor;
     * round 2 must scrub all 10 chunks across both volumes and detect the corrupt chunk in volume 7! */
    struct rep_log rl = {.mu = PTHREAD_MUTEX_INITIALIZER};
    chunk_scrub_cfg cfg = {
        .store_idx = 0,
        .scrub_days = 14,
        .scrub_mb_s = 200,
        .scrub_out_count = 100,
        .pass_interval_ms = 100,
        .on_damage = on_damage_rec,
        .ctx = &rl,
    };
    chunkscrub_fault_list_vol = 7;
    chunk_scrub *sc = chunk_scrub_start(st, &cfg);
    CHECK(sc != NULL, "multivol scrub start");
    CHECK(wait_passes(sc, 1, 10000), "multivol retry pass completed");
    chunk_scrub_stats st_s;
    chunk_scrub_get_stats(sc, &st_s);
    chunk_scrub_stop(sc);
    chunk_store_close(st);

    CHECK(st_s.chunks_total == 10 && st_s.damaged_total == 1,
          "multivol must scrub all 10 chunks and catch vol 7 damage (chunks=%" PRIu64 " dmg=%" PRIu64 ")",
          st_s.chunks_total, st_s.damaged_total);
    CHECK(rl.count == 1 && rl.items[0].volume == 7 && memcmp(rl.items[0].oid, bad_oid, 16) == 0,
          "multivol reported vol 7 bad chunk (count=%d vol=%" PRIu32 ")", rl.count,
          rl.count > 0 ? rl.items[0].volume : 0u);
    printf("  multivol_transient_err: ok (vol 7 error aborted pass, retry scrubbed both vols and caught vol 7 rot)\n");
}

static uint64_t fake_wall_cb(void *ctx)
{
    _Atomic uint64_t *clk = ctx;
    return atomic_load(clk);
}

/* 7. Regression test: scrub_days schedule (pass_interval_ms = 0) and last_pass_s / age_s across 3 passes.
 *    Verifies pass_start_s is captured when each pass actually begins (after inter-pass sleep), not at the end of the
 *    previous pass — so pass 3 waits a full scrub_days window instead of running continuously. */
static void test_schedule_and_age(void)
{
    struct test_world w;
    world_make(&w, "sched", 4, 16384, 2 * 16384 * 2);

    const uint64_t day = 86400ull;
    const uint64_t T0 = 1700000000ull;
    _Atomic uint64_t vclock;
    atomic_init(&vclock, T0);

    chunk_scrub_cfg cfg = {
        .store_idx = 0,
        .scrub_days = 14,
        .scrub_mb_s = 200,
        .scrub_out_count = 8,
        .pass_interval_ms = 0, /* exercise real scrub_days scheduling */
        .wall_s = fake_wall_cb,
        .ctx = &vclock,
    };

    chunk_scrub *sc = chunk_scrub_start(chunk_be_store(w.be, 0), &cfg);
    CHECK(sc != NULL, "sched scrub start");
    CHECK(wait_passes(sc, 1, 10000), "sched pass 1 done");

    chunk_scrub_stats st;
    chunk_scrub_get_stats(sc, &st);
    CHECK(st.passes_done == 1 && st.last_pass_s == T0 && st.age_s == 0,
          "pass 1: passes=%" PRIu64 " last_pass_s=%" PRIu64 " (want %" PRIu64 ") age=%" PRIu64, st.passes_done,
          st.last_pass_s, T0, st.age_s);

    /* Advance to 10 s before due: pass 2 must NOT start yet and age_s reflects elapsed time. */
    atomic_store(&vclock, T0 + 14 * day - 10);
    chunk_scrub_update_cfg(sc, 14, 0, 0);
    usleep(200000);
    chunk_scrub_get_stats(sc, &st);
    CHECK(st.passes_done == 1 && st.age_s == 14 * day - 10,
          "before due: passes=%" PRIu64 " age=%" PRIu64, st.passes_done, st.age_s);

    /* Advance past due to T1: pass 2 starts and finishes with last_pass_s == T1 (NOT T0!). */
    const uint64_t T1 = T0 + 14 * day + 100;
    atomic_store(&vclock, T1);
    chunk_scrub_update_cfg(sc, 14, 0, 0);
    CHECK(wait_passes(sc, 2, 10000), "sched pass 2 done");
    chunk_scrub_get_stats(sc, &st);
    CHECK(st.passes_done == 2 && st.last_pass_s == T1 && st.age_s == 0,
          "pass 2: passes=%" PRIu64 " last_pass_s=%" PRIu64 " (want %" PRIu64 ") age=%" PRIu64, st.passes_done,
          st.last_pass_s, T1, st.age_s);

    /* Advance only 1 hour past T1: pass 3 MUST wait a full 14 days (catches premature pass_start_s bug!). */
    atomic_store(&vclock, T1 + 3600);
    chunk_scrub_update_cfg(sc, 14, 0, 0);
    usleep(300000);
    chunk_scrub_get_stats(sc, &st);
    CHECK(st.passes_done == 2 && st.last_pass_s == T1 && st.age_s == 3600,
          "after pass 2 (+1h): passes=%" PRIu64 " (want 2) last_pass_s=%" PRIu64 " age=%" PRIu64, st.passes_done,
          st.last_pass_s, st.age_s);

    /* Restart at T1 + 3600 (rolling restart mid-window): must sleep until T1 + 14d instead of immediately re-scrubbing! */
    chunk_scrub_stop(sc);
    sc = chunk_scrub_start(chunk_be_store(w.be, 0), &cfg);
    CHECK(sc != NULL, "sched restart mid-window");
    usleep(300000);
    chunk_scrub_get_stats(sc, &st);
    CHECK(st.passes_done == 0 && st.chunks_total == 0 && st.last_pass_s == T1 && st.age_s == 3600,
          "rolling restart mid-window must wait: passes=%" PRIu64 " chunks=%" PRIu64 " lp=%" PRIu64 " age=%" PRIu64,
          st.passes_done, st.chunks_total, st.last_pass_s, st.age_s);

    /* Advance past T1 + 14d to T2: pass runs and updates last_pass_s == T2. */
    const uint64_t T2 = T1 + 14 * day + 5;
    atomic_store(&vclock, T2);
    chunk_scrub_update_cfg(sc, 14, 0, 0);
    CHECK(wait_passes(sc, 1, 10000), "sched pass 3 done after restart");
    chunk_scrub_get_stats(sc, &st);
    CHECK(st.passes_done == 1 && st.last_pass_s == T2 && st.age_s == 0,
          "pass 3: passes=%" PRIu64 " last_pass_s=%" PRIu64 " (want %" PRIu64 ") age=%" PRIu64, st.passes_done,
          st.last_pass_s, T2, st.age_s);

    chunk_scrub_stop(sc);
    world_free(&w);

    /* Empty store: clean poll keeps age_s == 0 without writing SCRUB every 2 s. */
    char edir[PATH_MAX];
    snprintf(edir, sizeof edir, "%s/empty-sched", scratch_base);
    CHECK(mkdir(edir, 0755) == 0, "mkdir empty-sched");
    char esp[PATH_MAX + 32];
    snprintf(esp, sizeof esp, "%s/st0", edir);
    CHECK(chunk_store_init(esp, NULL, NULL) == 0, "init empty store");
    chunk_store *est = NULL;
    CHECK(chunk_store_open(esp, NULL, &est) == 0, "open empty store");
    if (est) {
        atomic_store(&vclock, T0);
        chunk_scrub_cfg ecfg = {
            .store_idx = 0,
            .scrub_days = 14,
            .scrub_mb_s = 50,
            .pass_interval_ms = 50,
            .wall_s = fake_wall_cb,
            .ctx = &vclock,
        };
        chunk_scrub *esc = chunk_scrub_start(est, &ecfg);
        CHECK(esc != NULL, "empty scrub start");
        atomic_store(&vclock, T0 + 30 * day); /* 30 days later: empty store must NOT go stale! */
        usleep(200000);
        chunk_scrub_get_stats(esc, &st);
        chunk_scrub_stop(esc);
        chunk_store_close(est);
        CHECK(st.age_s == 0, "empty store age_s must stay 0 (got %" PRIu64 ")", st.age_s);
        char esf[PATH_MAX + 64];
        snprintf(esf, sizeof esf, "%s/SCRUB", esp);
        CHECK(access(esf, F_OK) != 0, "empty store should not fsync SCRUB on idle polls");
    }
    printf("  schedule_and_age: ok (3 passes at 14d cadence, rolling restart wait, empty store age=0)\n");
}

/* 8. Regression test (E15): FD exhaustion (EMFILE) on openat returns TSDB_ERR_NOMEM for scrub (transient, never damage)
 *    while chunk_be_rd_open returns TSDB_ERR_IO (never TSDB_ERR_NOTFOUND / CHUNK_ST_ABSENT). */
static void test_fd_exhaustion_e15(void)
{
    struct test_world w;
    world_make(&w, "emfile", 2, 16384, 2 * 16384);

    struct rlimit orig_rl;
    if (getrlimit(RLIMIT_NOFILE, &orig_rl) != 0)
        return;

    /* Find the lowest unused fd so setting rlim_cur = probe makes the very next openat fail with EMFILE. */
    int probe = open("/dev/null", O_RDONLY | O_CLOEXEC);
    CHECK(probe >= 3, "probe fd=%d", probe);
    if (probe < 3) {
        world_free(&w);
        return;
    }
    close(probe);

    const struct test_obj *o = &w.objs[0];
    chunk_ident id = {.volume = TEST_VOL, .pos = (uint16_t)o->pos_on_st0};
    memcpy(id.vuuid, TEST_VUUID, 16);
    memcpy(id.oid, o->oid, 16);
    chunk_store *st0 = chunk_be_store(w.be, 0);

    struct rlimit tight = {.rlim_cur = (rlim_t)probe, .rlim_max = orig_rl.rlim_max};
    int be_rc = 0, scrub_rc = 0;
    chunk_rd *rd1 = NULL, *rd2 = NULL;
    if (setrlimit(RLIMIT_NOFILE, &tight) == 0) {
        be_rc = chunk_be_rd_open(w.be, &id, &rd1);
        scrub_rc = chunk_rd_open_any(st0, TEST_VOL, o->oid, (unsigned)o->pos_on_st0, &rd2);
        (void)setrlimit(RLIMIT_NOFILE, &orig_rl);
    } else {
        world_free(&w);
        return;
    }
    if (rd1)
        chunk_rd_close(rd1);
    if (rd2)
        chunk_rd_close(rd2);

    CHECK(be_rc == TSDB_ERR_IO, "EMFILE chunk_be_rd_open must return TSDB_ERR_IO (not NOTFOUND), got %d", be_rc);
    CHECK(scrub_rc == TSDB_ERR_NOMEM, "EMFILE chunk_rd_open_any must return TSDB_ERR_NOMEM (transient), got %d",
          scrub_rc);

    /* Confirm normal open still works once limit is restored. */
    chunk_rd *rd3 = NULL;
    CHECK(chunk_be_rd_open(w.be, &id, &rd3) == 0 && rd3 != NULL, "chunk_be_rd_open after restore");
    if (rd3)
        chunk_rd_close(rd3);

    world_free(&w);
    printf("  fd_exhaustion_e15: ok (EMFILE -> be_rd_open=IO, scrub_open=NOMEM)\n");
}

/* 9. Live settings update (R14, r14-settings): chunk_scrub_update_cfg accelerates a 1 MB/s pass to 200 MB/s live
 *    and lowers scrub_out_count live from 8 to 3. */
static void test_live_update_cfg(void)
{
    struct test_world w;
    /* 12 chunks x 512 KiB = 6 MiB on store 0 -> ~6 s at 1 MB/s, < 200 ms at 200 MB/s. */
    world_make(&w, "livecfg", 12, 128u << 10, 512u << 10);
    struct rep_log rl = {.mu = PTHREAD_MUTEX_INITIALIZER};

    chunk_scrub_cfg cfg = {
        .store_idx = 0,
        .scrub_days = 14,
        .scrub_mb_s = 1,
        .scrub_out_count = 8,
        .pass_interval_ms = 50,
        .be = w.be,
        .on_damage = on_damage_rec,
        .on_out = on_out_rec,
        .ctx = &rl,
    };
    chunk_scrub *sc = chunk_scrub_start(chunk_be_store(w.be, 0), &cfg);
    CHECK(sc != NULL, "livecfg: chunk_scrub_start");

    /* Wait until scrubbing has started at 1 MB/s, then raise scrub_mb_s live to 200 and lower scrub_out_count to 3. */
    chunk_scrub_stats st;
    memset(&st, 0, sizeof st);
    for (uint64_t t0 = now_ms(); now_ms() - t0 < 5000; usleep(25000)) {
        chunk_scrub_get_stats(sc, &st);
        if (st.bytes_total > 0)
            break;
    }
    CHECK(st.bytes_total > 0 && st.passes_done == 0,
          "livecfg: expected active slow pass before update (bytes=%" PRIu64 " passes=%" PRIu64 ")", st.bytes_total,
          st.passes_done);

    uint64_t t_upd = now_ms();
    chunk_scrub_update_cfg(sc, 7, 200, 3);
    CHECK(wait_passes(sc, 1, 3000), "livecfg: pass 1 did not finish quickly after scrub_mb_s 1->200");
    uint64_t dt_ms = now_ms() - t_upd;

    /* Flip 4 chunks (exceeds new scrub_out_count=3, below old 8) and verify store goes DOWN_DAMAGED live. */
    for (size_t i = 0; i < 4; i++) {
        char p[PATH_MAX + 96];
        chunk_path(&w, 0, &w.objs[i], (unsigned)w.objs[i].pos_on_st0, p, sizeof p);
        CHECK(flip_file_byte(p, CHUNK_HDR_BYTES + 10) == 0, "livecfg: corrupt chunk %zu", i);
    }
    for (uint64_t t0 = now_ms(); now_ms() - t0 < 5000; usleep(25000)) {
        chunk_scrub_get_stats(sc, &st);
        if (st.marked_down)
            break;
    }
    CHECK(st.marked_down, "livecfg: lowered scrub_out_count=3 did not mark store DOWN after 4 damaged chunks");

    chunk_scrub_stop(sc);
    world_free(&w);
    printf("  live_update_cfg: ok (1->200 MB/s finished in %" PRIu64 " ms; out_count 8->3 triggered on 4 damaged)\n",
           dt_ms);
}

int main(int argc, char **argv)
{
    g_self_exe = argv[0];
    if (argc == 4 && strcmp(argv[1], "--kill-child") == 0)
        return run_kill_child(argv[2], atoi(argv[3]));

    const char *mut = NULL;
    const char *dir_arg = NULL;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mutant") == 0 && i + 1 < argc)
            mut = argv[++i];
        else if (argv[i][0] != '-')
            dir_arg = argv[i];
        else {
            fprintf(stderr, "usage: test_chunkscrub [--mutant scrubskipcell|scrubeioclean|scrubrestart] [SCRATCH]\n");
            return 2;
        }
    }

    const char *base = dir_arg && *dir_arg ? dir_arg : getenv("TMPDIR");
    if (!base || !*base)
        base = "/tmp";
    if (mkdir(base, 0755) != 0 && errno != EEXIST) {
        perror("mkdir base");
        return 1;
    }

#ifdef __linux__
    if (!getenv("OUTAGEFS_CTL")) {
        if (!getenv("OUTAGEFS_LIB")) {
            char sib[PATH_MAX];
            const char *sl = strrchr(argv[0], '/');
            if (sl) {
                snprintf(sib, sizeof sib, "%.*s/liboutagefs.so", (int)(sl - argv[0]), argv[0]);
                if (access(sib, R_OK) == 0)
                    setenv("OUTAGEFS_LIB", sib, 1);
            }
        }
        const char *lib = getenv("OUTAGEFS_LIB") ? getenv("OUTAGEFS_LIB") : "build/liboutagefs.so";
        if (access(lib, R_OK) == 0) {
            const char *names[] = {"eio"};
            if (outagefs_reexec(argv, base, names, 1) != 0)
                return 1;
        }
    }
#endif

    th_watchdog("test_chunkscrub", 300);

    if (mut) {
        int m = chunkscrub_mutant_parse(mut);
        if (m <= 0) {
            fprintf(stderr, "test_chunkscrub: unknown mutant %s\n", mut);
            return 2;
        }
        chunkscrub_mutant = m;
    }

    snprintf(scratch_base, sizeof scratch_base, "%s/tsdb_chunkscrub.%d", base, (int)getpid());
    rm_rf(scratch_base);
    if (mkdir(scratch_base, 0755) != 0) {
        perror("mkdir scratch");
        return 1;
    }

    test_flips_and_once();
    test_eio_damage();
    test_pace_and_pause();
    test_restart_resume();
    test_out_count_and_retry();
    test_multivol_transient_err();
    test_schedule_and_age();
    test_fd_exhaustion_e15();
    test_live_update_cfg();

    rm_rf(scratch_base);
#ifdef __linux__
    const char *edir = outagefs_dir("eio");
    if (edir) {
        char parent[PATH_MAX];
        snprintf(parent, sizeof parent, "%s", edir);
        char *sl = strrchr(parent, '/');
        if (sl) {
            *sl = '\0';
            rm_rf(parent);
        }
    }
#endif

    if (failures) {
        fprintf(stderr, "test_chunkscrub: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_chunkscrub: ok\n");
    return 0;
}
#endif
