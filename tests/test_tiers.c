/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Tests for TIERS file, root META db_id, store META db= binding, tsdb_tier_add(_dir),
 * concurrent OFD locking, running-writer safety, crashfs power-loss sweeps, and mutants
 * (docs/design-cluster-data.md §15.7, E55, E58; R15 D2, D3, D18).
 *
 *   ./build/test_tiers [scratch dir]
 *   ./build/test_tiers --mutant tiersnosync|nodbid|samedev [scratch dir]
 *   ./build/test_tiers --child add1|add2 DBDIR STOREDIR ACKFILE [--mutant NAME]
 */
#define _GNU_SOURCE
#include "tiers.h"
#include "chunk.h"
#include "internal.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static int failures;
static int saved_err = -1;
static int cap_rd = -1;
static char S[PATH_MAX], SELF[PATH_MAX], SHIM[PATH_MAX + 32];
static const char *mutant_name = NULL;

static void fail_at(const char *file, int line, const char *what)
{
    char buf[512];
    int n = snprintf(buf, sizeof buf, "%s:%d: CHECK failed: %s\n", file, line, what);
    if (write(saved_err >= 0 ? saved_err : 2, buf, (size_t)n) < 0)
        failures++;
    failures++;
}

#define CHECK(cond)                             \
    do {                                        \
        if (!(cond))                            \
            fail_at(__FILE__, __LINE__, #cond); \
    } while (0)

static int sh(const char *fmt, ...)
{
    char cmd[4096];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(cmd, sizeof cmd, fmt, ap);
    va_end(ap);
    return system(cmd);
}

static void cap_begin(void)
{
    fflush(stderr);
    int pf[2];
    saved_err = dup(2);
    if (pipe(pf) == 0) {
        fcntl(pf[0], F_SETFL, O_NONBLOCK);
        fcntl(pf[0], F_SETFD, FD_CLOEXEC);
        fcntl(pf[1], F_SETFD, FD_CLOEXEC);
        cap_rd = pf[0];
        dup2(pf[1], 2);
        close(pf[1]);
    }
}

static void cap_end(char *out, size_t cap)
{
    fflush(stderr);
    if (saved_err >= 0) {
        dup2(saved_err, 2);
        close(saved_err);
        saved_err = -1;
    }
    size_t n = 0;
    if (cap_rd >= 0) {
        char junk[4096];
        bool room = out != NULL && cap > 0;
        for (;;) {
            ssize_t r = read(cap_rd, room ? out + n : junk, room ? cap - 1 - n : sizeof junk);
            if (r <= 0)
                break;
            if (room) {
                n += (size_t)r;
                room = (n + 1 < cap);
            }
        }
        close(cap_rd);
        cap_rd = -1;
    }
    if (out && cap > 0)
        out[n] = '\0';
}

static void fresh_dir(const char *name, char *out, size_t cap)
{
    snprintf(out, cap, "%s/%s", S, name);
    CHECK(sh("rm -rf '%s' && mkdir -p '%s'", out, out) == 0);
}

#ifdef TSDB_EC
static void make_id(uint8_t out[16], uint8_t base)
{
    for (int i = 0; i < 16; i++)
        out[i] = (uint8_t)(base + i);
}
#endif

static void init_db_closed(const char *dir)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.stats = -1;
    o.latest_cache = 0;
    tsdb *db = NULL;
    CHECK(tsdb_open(dir, &o, &db) == 0 && db != NULL);
    if (db)
        CHECK(tsdb_close(db) == 0);
}

static int apply_mutant(const char *name)
{
#ifdef TSDB_TEST_HOOKS
    if (!name)
        return 0;
    if (strcmp(name, "tiersnosync") == 0) {
        tiers_test_mutant = TIERS_MUT_NOSYNC;
        return 0;
    }
    if (strcmp(name, "nodbid") == 0) {
        tiers_test_mutant = TIERS_MUT_NODBID;
        return 0;
    }
    if (strcmp(name, "samedev") == 0) {
        tiers_test_mutant = TIERS_MUT_SAMEDEV;
        return 0;
    }
#else
    (void)name;
#endif
    return -1;
}

#ifndef TSDB_EC
static void test_no_ec(void)
{
    char dbdir[PATH_MAX + 32], stdir[PATH_MAX + 32], log[2048];
    fresh_dir("noec-db", dbdir, sizeof dbdir);
    snprintf(stdir, sizeof stdir, "%s/noec-store", S);
    init_db_closed(dbdir);

    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.stats = -1;
    o.latest_cache = 0;
    tsdb *db = NULL;
    CHECK(tsdb_open(dbdir, &o, &db) == 0 && db != NULL);

    cap_begin();
    CHECK(tsdb_tier_add(db, stdir, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_INVALID);
    cap_end(log, sizeof log);
    CHECK(strstr(log, "not built with the erasure codec") != NULL);

    cap_begin();
    CHECK(tsdb_tier_add_dir(dbdir, stdir, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_INVALID);
    cap_end(log, sizeof log);
    CHECK(strstr(log, "not built with the erasure codec") != NULL);

    if (db)
        CHECK(tsdb_close(db) == 0);
}
#else

/* ---------------------------------------------------------------- 1. Records round-trip */

static void test_records_roundtrip(void)
{
    char dir[PATH_MAX + 32];
    fresh_dir("rec-rt", dir, sizeof dir);
    int dfd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    CHECK(dfd >= 0);

    /* Absent TIERS -> empty view with exists=false and defaults. */
    tiers_view v;
    CHECK(tiers_load(dfd, false, true, &v) == 0);
    CHECK(!v.exists && v.nstores == 0 && v.nvols == 0);
    tiers_settings eff;
    tiers_view_settings_for(&v, TIERS_VOL_ALL, &eff);
    CHECK(eff.tier_after_s == TIERS_AFTER_S_DEFAULT && eff.tier_high_pct == TIERS_HIGH_PCT_DEFAULT &&
          eff.young_high_pct == TIERS_YOUNG_HIGH_PCT_DEFAULT && eff.rebuild_mb_s == TIERS_REBUILD_MB_S_DEFAULT &&
          eff.scrub_mb_s == TIERS_SCRUB_MB_S_DEFAULT && eff.scrub_days == TIERS_SCRUB_DAYS_DEFAULT &&
          eff.k == 0 && eff.m == 0);
    tiers_view_free(&v);

    /* Append every store class + single_copy, global + per-volume settings, LOST, REPLACED_BY, chained replacement. */
    uint8_t id0[16], id1[16], id2[16], id3[16], id4[16], id5[16];
    make_id(id0, 0x10);
    make_id(id1, 0x20);
    make_id(id2, 0x30);
    make_id(id3, 0x40);
    make_id(id4, 0x50);
    make_id(id5, 0x60);

    CHECK(tiers_append_store(dfd, true, id0, TSDB_TIER_CLASS_UNKNOWN, false, "/mnt/t0") == 0);
    CHECK(tiers_append_store(dfd, true, id1, TSDB_TIER_CLASS_NVME, true, "/mnt/t1") == 0);
    CHECK(tiers_append_store(dfd, true, id2, TSDB_TIER_CLASS_SSD, false, "/mnt/t2") == 0);
    CHECK(tiers_append_store(dfd, true, id3, TSDB_TIER_CLASS_HDD, true, "/mnt/t3") == 0);

    /* Duplicate store id or duplicate path refused. */
    CHECK(tiers_append_store(dfd, true, id0, TSDB_TIER_CLASS_HDD, false, "/mnt/t99") == TSDB_ERR_INVALID);
    CHECK(tiers_append_store(dfd, true, id4, TSDB_TIER_CLASS_HDD, false, "/mnt/t2") == TSDB_ERR_INVALID);

    /* Database-wide SETTINGS */
    tiers_settings gs;
    memset(&gs, 0, sizeof gs);
    gs.set_mask = TSDB_TIER_SET_REDUNDANCY | TSDB_TIER_SET_AFTER | TSDB_TIER_SET_HIGH_PCT |
                  TSDB_TIER_SET_YOUNG_HIGH_PCT | TSDB_TIER_SET_REBUILD_MB_S | TSDB_TIER_SET_SCRUB_MB_S |
                  TSDB_TIER_SET_SCRUB_DAYS;
    gs.k = 4;
    gs.m = 2;
    gs.tier_after_s = 123456;
    gs.tier_high_pct = 72;
    gs.young_high_pct = 86;
    gs.rebuild_mb_s = 180;
    gs.scrub_mb_s = 35;
    gs.scrub_days = 10;
    CHECK(tiers_append_settings(dfd, true, TIERS_VOL_ALL, &gs) == 0);

    /* Per-volume SETTINGS override (volume 2: only after_s and single-copy 1+0) */
    tiers_settings vs;
    memset(&vs, 0, sizeof vs);
    vs.set_mask = TSDB_TIER_SET_REDUNDANCY | TSDB_TIER_SET_AFTER;
    vs.k = 1;
    vs.m = 0;
    vs.tier_after_s = 3600;
    CHECK(tiers_append_settings(dfd, true, 2, &vs) == 0);

    /* Invalid m=0 with k!=1 refused */
    tiers_settings bads;
    memset(&bads, 0, sizeof bads);
    bads.set_mask = TSDB_TIER_SET_REDUNDANCY;
    bads.k = 2;
    bads.m = 0;
    CHECK(tiers_append_settings(dfd, true, TIERS_VOL_ALL, &bads) == TSDB_ERR_INVALID);

    /* STATE: id1 LOST -> REPLACED_BY id4 at /mnt/t1 (reusing mount point), then id4 LOST -> REPLACED_BY id5 */
    CHECK(tiers_append_state(dfd, true, id1, TIERS_STATE_LOST, 0, false, NULL, NULL) == 0);
    /* Replacing id2 before it is LOST is refused */
    CHECK(tiers_append_state(dfd, true, id2, TIERS_STATE_REPLACED_BY, TSDB_TIER_CLASS_SSD, false, id4, "/mnt/t2new") ==
          TSDB_ERR_INVALID);
    CHECK(tiers_append_state(dfd, true, id1, TIERS_STATE_REPLACED_BY, TSDB_TIER_CLASS_HDD, false, id4, "/mnt/t1") == 0);
    /* Chained replacement on slot 1 */
    CHECK(tiers_append_state(dfd, true, id4, TIERS_STATE_LOST, 0, false, NULL, NULL) == 0);
    CHECK(tiers_append_state(dfd, true, id4, TIERS_STATE_REPLACED_BY, TSDB_TIER_CLASS_SSD, true, id5, "/mnt/t1b") == 0);
    /* id3 LOST and left lost */
    CHECK(tiers_append_state(dfd, true, id3, TIERS_STATE_LOST, 0, false, NULL, NULL) == 0);

    /* Reload and verify every field */
    CHECK(tiers_load(dfd, false, true, &v) == 0);
    CHECK(v.exists && v.nstores == 4 && v.nvols == 1);
    CHECK(memcmp(v.stores[0].id, id0, 16) == 0 && v.stores[0].cls == TSDB_TIER_CLASS_UNKNOWN &&
          !v.stores[0].single_copy && v.stores[0].state == TIERS_STATE_ACTIVE && strcmp(v.stores[0].path, "/mnt/t0") == 0);
    CHECK(memcmp(v.stores[1].id, id4, 16) == 0 && memcmp(v.stores[1].replaced_by, id5, 16) == 0 &&
          v.stores[1].cls == TSDB_TIER_CLASS_SSD && v.stores[1].single_copy &&
          v.stores[1].state == TIERS_STATE_REPLACED_BY && strcmp(v.stores[1].path, "/mnt/t1b") == 0);
    CHECK(memcmp(v.stores[2].id, id2, 16) == 0 && v.stores[2].cls == TSDB_TIER_CLASS_SSD &&
          !v.stores[2].single_copy && v.stores[2].state == TIERS_STATE_ACTIVE && strcmp(v.stores[2].path, "/mnt/t2") == 0);
    CHECK(memcmp(v.stores[3].id, id3, 16) == 0 && v.stores[3].cls == TSDB_TIER_CLASS_HDD &&
          v.stores[3].single_copy && v.stores[3].state == TIERS_STATE_LOST && strcmp(v.stores[3].path, "/mnt/t3") == 0);

    /* Effective settings for volume 0 (global) and volume 2 (overridden) */
    tiers_view_settings_for(&v, 0, &eff);
    CHECK(eff.k == 4 && eff.m == 2 && eff.tier_after_s == 123456 && eff.tier_high_pct == 72 &&
          eff.young_high_pct == 86 && eff.rebuild_mb_s == 180 && eff.scrub_mb_s == 35 && eff.scrub_days == 10);
    tiers_view_settings_for(&v, 2, &eff);
    CHECK(eff.k == 1 && eff.m == 0 && eff.tier_after_s == 3600 && eff.tier_high_pct == 72 &&
          eff.young_high_pct == 86 && eff.rebuild_mb_s == 180 && eff.scrub_mb_s == 35 && eff.scrub_days == 10);

    tiers_view_free(&v);
    close(dfd);
    printf("  TIERS record round-trip (STORE, STATE, SETTINGS, per-volume) ok\n");
}

/* ---------------------------------------------------------------- 2. Torn tail & corruption */

static void test_torn_tail_and_corrupt(void)
{
    char dir[PATH_MAX + 32], tpath[PATH_MAX + 64], log[2048];
    fresh_dir("torn-corrupt", dir, sizeof dir);
    snprintf(tpath, sizeof tpath, "%s/%s", dir, TIERS_FILE);
    int dfd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    CHECK(dfd >= 0);

    uint8_t id0[16], id1[16], id2[16], id3[16];
    make_id(id0, 0x11);
    make_id(id1, 0x22);
    make_id(id2, 0x33);
    make_id(id3, 0x44);

    /* Build a canonical 3-record TIERS file image in memory to slice at every byte. */
    uint8_t full[4096];
    size_t off1 = 0, off2 = 0, off3 = 0;
    int n1 = tiers_encode_store(0, id0, TSDB_TIER_CLASS_HDD, false, "/mnt/store-alpha", full, sizeof full);
    CHECK(n1 > 0);
    off1 = (size_t)n1;

    tiers_settings gs;
    memset(&gs, 0, sizeof gs);
    gs.set_mask = TSDB_TIER_SET_AFTER | TSDB_TIER_SET_HIGH_PCT;
    gs.tier_after_s = 86400;
    gs.tier_high_pct = 81;
    int n2 = tiers_encode_settings(off1, TIERS_VOL_ALL, &gs, full + off1, sizeof full - off1);
    CHECK(n2 > 0);
    off2 = off1 + (size_t)n2;

    int n3 = tiers_encode_store(off2, id1, TSDB_TIER_CLASS_SSD, true, "/mnt/store-beta", full + off2, sizeof full - off2);
    CHECK(n3 > 0);
    off3 = off2 + (size_t)n3;

    /* Torn tail at EVERY byte cut of record 3: 1 .. n3 - 1. */
    for (size_t cut = 1; cut < (size_t)n3; cut++) {
        int fd = open(tpath, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        CHECK(fd >= 0 && write(fd, full, off2 + cut) == (ssize_t)(off2 + cut));
        if (fd >= 0)
            close(fd);

        /* Read-only load sees 2 records, leaves file untouched. */
        tiers_view v;
        CHECK(tiers_load(dfd, false, true, &v) == 0);
        CHECK(v.nstores == 1 && v.global.tier_after_s == 86400 && v.file_end == off2);
        tiers_view_free(&v);
        struct stat st;
        CHECK(stat(tpath, &st) == 0 && (size_t)st.st_size == off2 + cut);

        /* Writer load cuts torn tail back to off2. */
        cap_begin();
        CHECK(tiers_load(dfd, true, true, &v) == 0);
        cap_end(log, sizeof log);
        CHECK(v.nstores == 1 && v.global.tier_after_s == 86400 && v.file_end == off2);
        CHECK(strstr(log, "cutting a torn tail") != NULL);
        tiers_view_free(&v);
        CHECK(stat(tpath, &st) == 0 && (size_t)st.st_size == off2);

        /* Appending a new record after the cut succeeds and reloads cleanly. */
        CHECK(tiers_append_store(dfd, true, id2, TSDB_TIER_CLASS_NVME, false, "/mnt/store-gamma") == 0);
        CHECK(tiers_load(dfd, false, true, &v) == 0);
        CHECK(v.nstores == 2 && memcmp(v.stores[1].id, id2, 16) == 0);
        tiers_view_free(&v);
    }

    /* Zero-filled torn tail (crashfs tear pattern) -> cut cleanly. */
    {
        uint8_t torn[4096];
        memcpy(torn, full, off2);
        memset(torn + off2, 0, (size_t)n3);
        int fd = open(tpath, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        CHECK(fd >= 0 && write(fd, torn, off3) == (ssize_t)off3);
        if (fd >= 0)
            close(fd);
        tiers_view v;
        cap_begin();
        CHECK(tiers_load(dfd, true, true, &v) == 0);
        cap_end(log, sizeof log);
        CHECK(v.nstores == 1 && v.file_end == off2);
        tiers_view_free(&v);
        struct stat st;
        CHECK(stat(tpath, &st) == 0 && (size_t)st.st_size == off2);
    }

    /* Flipped byte BEFORE a good record MUST fail with TSDB_ERR_CORRUPT (both cut=false and cut=true). */
    size_t flip_offsets[] = {0, 4, 8, off1 - 9, off1 - 1, off1, off1 + 5, off2 - 1};
    for (size_t i = 0; i < sizeof flip_offsets / sizeof flip_offsets[0]; i++) {
        uint8_t bad[4096];
        memcpy(bad, full, off3);
        bad[flip_offsets[i]] ^= 0x04;
        int fd = open(tpath, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        CHECK(fd >= 0 && write(fd, bad, off3) == (ssize_t)off3);
        if (fd >= 0)
            close(fd);

        tiers_view v;
        cap_begin();
        CHECK(tiers_load(dfd, false, true, &v) == TSDB_ERR_CORRUPT);
        cap_end(log, sizeof log);
        CHECK(strstr(log, TIERS_FILE) != NULL);

        cap_begin();
        CHECK(tiers_load(dfd, true, true, &v) == TSDB_ERR_CORRUPT);
        cap_end(log, sizeof log);
        CHECK(strstr(log, TIERS_FILE) != NULL);

        /* Must NOT truncate a damaged file! */
        struct stat st;
        CHECK(stat(tpath, &st) == 0 && (size_t)st.st_size == off3);
    }

    /* Read error / non-regular TIERS (directory named TIERS) -> TSDB_ERR_IO with stderr line. */
    CHECK(unlink(tpath) == 0 && mkdir(tpath, 0755) == 0);
    tiers_view v;
    cap_begin();
    CHECK(tiers_load(dfd, false, true, &v) == TSDB_ERR_IO);
    cap_end(log, sizeof log);
    CHECK(strstr(log, TIERS_FILE) != NULL);
    CHECK(rmdir(tpath) == 0);

    (void)id3;
    close(dfd);
    printf("  TIERS torn tail (every byte cut) & corrupt-before-good & read-error ok\n");
}

/* ---------------------------------------------------------------- 3. Validation, db_id & binding */

static void test_add_validation_and_binding(void)
{
    char db1_dir[PATH_MAX + 32], db2_dir[PATH_MAX + 32], db3_dir[PATH_MAX + 32];
    char s0[PATH_MAX + 64], s1[PATH_MAX + 64], s2[PATH_MAX + 64], s3[PATH_MAX + 64];
    char s_db2[PATH_MAX + 64], v1_dir[PATH_MAX + 64], nonempty[PATH_MAX + 64], unbound[PATH_MAX + 64];
    char log[4096];

    fresh_dir("val-db1", db1_dir, sizeof db1_dir);
    fresh_dir("val-db2", db2_dir, sizeof db2_dir);
    fresh_dir("val-db3", db3_dir, sizeof db3_dir);
    snprintf(s0, sizeof s0, "%s/val-s0", S);
    snprintf(s1, sizeof s1, "%s/val-s1", S);
    snprintf(s2, sizeof s2, "%s/val-s2", S);
    snprintf(s3, sizeof s3, "%s/val-s3", S);
    snprintf(s_db2, sizeof s_db2, "%s/val-s-db2", S);
    snprintf(v1_dir, sizeof v1_dir, "%s/val-v1", S);
    fresh_dir("val-nonempty", nonempty, sizeof nonempty);
    snprintf(unbound, sizeof unbound, "%s/val-unbound", S);
    sh("rm -rf '%s' '%s' '%s' '%s' '%s' '%s' '%s'", s0, s1, s2, s3, s_db2, v1_dir, unbound);

    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.stats = -1;
    o.latest_cache = 0;

    tsdb *db1 = NULL, *db2 = NULL, *db3 = NULL;
    CHECK(tsdb_open(db1_dir, &o, &db1) == 0 && db1 != NULL);
    CHECK(tsdb_open(db2_dir, &o, &db2) == 0 && db2 != NULL);
    CHECK(tsdb_open(db3_dir, &o, &db3) == 0 && db3 != NULL);

    /* Fresh database has no db_id yet. */
    uint8_t db1_id[16], db2_id[16], got_id[16];
    CHECK(!tiers_tsdb_db_id(db1, db1_id));

    /* Stubs return TSDB_ERR_INVALID and log "not built". */
    tsdb_tier_settings st_cfg;
    memset(&st_cfg, 0, sizeof st_cfg);
    tsdb_tier_info st_info;
    memset(&st_info, 0, sizeof st_info);
    cap_begin();
    CHECK(tsdb_tier_set(db1, &st_cfg) == TSDB_ERR_INVALID);
    cap_end(log, sizeof log);
    CHECK(strstr(log, "not built") != NULL);
    cap_begin();
    CHECK(tsdb_tier_lost(db1, "s0", NULL, 0) == TSDB_ERR_INVALID);
    cap_end(log, sizeof log);
    CHECK(strstr(log, "not built") != NULL);
    cap_begin();
    CHECK(tsdb_tier_salvage(db1, 0, 1, 0) == TSDB_ERR_INVALID);
    cap_end(log, sizeof log);
    CHECK(strstr(log, "not built") != NULL);
    cap_begin();
    CHECK(tsdb_tier_status(db1, &st_info) == TSDB_ERR_INVALID);
    cap_end(log, sizeof log);
    CHECK(strstr(log, "not built") != NULL);
    tsdb_tier_info_free(&st_info);

    /* Path, class and flag validation refusals. */
    CHECK(tsdb_tier_add(NULL, s0, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_INVALID);
    CHECK(tsdb_tier_add(db1, NULL, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_INVALID);
    CHECK(tsdb_tier_add(db1, "", "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_INVALID);
    CHECK(tsdb_tier_add(db1, "relative/path", "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_INVALID);
    CHECK(tsdb_tier_add(db1, "/tmp/bad\nline", "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_INVALID);
    CHECK(tsdb_tier_add(db1, "/tmp/./bad", "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_INVALID);
    CHECK(tsdb_tier_add(db1, "/tmp/../bad", "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_INVALID);
    cap_begin();
    CHECK(tsdb_tier_add(db1, s0, "tape", TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_INVALID);
    cap_end(log, sizeof log);
    CHECK(tsdb_tier_add(db1, s0, "hdd", 0x80u) == TSDB_ERR_INVALID);

    /* Same-device refusal WITHOUT TSDB_TIER_ALLOW_SHARED_DEV (kills mutant samedev!). */
    cap_begin();
    CHECK(tsdb_tier_add(db1, s0, "hdd", 0) == TSDB_ERR_INVALID);
    cap_end(log, sizeof log);
    CHECK(strstr(log, "same device") != NULL);
    CHECK(access(s0, F_OK) != 0); /* refused add cleaned up its mkdir */

    /* First real add WITH TSDB_TIER_ALLOW_SHARED_DEV. */
    CHECK(tsdb_tier_add(db1, s0, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);
    CHECK(tiers_tsdb_db_id(db1, db1_id));

    /* Verify root META db_id survives close & reopen. */
    CHECK(tsdb_close(db1) == 0);
    db1 = NULL;
    CHECK(tsdb_open(db1_dir, &o, &db1) == 0 && db1 != NULL);
    CHECK(tiers_tsdb_db_id(db1, got_id) && memcmp(got_id, db1_id, 16) == 0);

    /* Add remaining classes and single-copy flag. */
    CHECK(tsdb_tier_add(db1, s1, "nvme", TSDB_TIER_SINGLE_COPY | TSDB_TIER_ALLOW_SHARED_DEV) == 0);
    CHECK(tsdb_tier_add(db1, s2, NULL, TSDB_TIER_ALLOW_SHARED_DEV) == 0);
    CHECK(tsdb_tier_add(db1, s3, "ssd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);

    /* Load TIERS and verify stores + open s0 with chunk_store_open_db (kills mutant nodbid!). */
    int dfd1 = open(db1_dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    CHECK(dfd1 >= 0);
    tiers_view v1;
    CHECK(tiers_load(dfd1, false, true, &v1) == 0);
    CHECK(v1.exists && v1.nstores == 4);
    CHECK(v1.stores[0].cls == TSDB_TIER_CLASS_HDD && !v1.stores[0].single_copy);
    CHECK(v1.stores[1].cls == TSDB_TIER_CLASS_NVME && v1.stores[1].single_copy);
    CHECK(v1.stores[2].cls == TSDB_TIER_CLASS_UNKNOWN && !v1.stores[2].single_copy);
    CHECK(v1.stores[3].cls == TSDB_TIER_CLASS_SSD && !v1.stores[3].single_copy);

    chunk_store *cst = NULL;
    CHECK(chunk_store_open_db(s0, v1.stores[0].id, db1_id, &cst) == 0 && cst != NULL);
    if (cst) {
        chunk_store_owner ow;
        chunk_store_get_owner(cst, &ow);
        CHECK(ow.db_set && memcmp(ow.db_id, db1_id, 16) == 0);
        chunk_store_close(cst);
        cst = NULL;
    }

    /* Nesting, volume overlap and duplicate store refusals. */
    uint32_t vol_idx = 0;
    CHECK(tsdb_volume_add(db1, v1_dir, &vol_idx) == 0 && vol_idx == 1);
    /* Verify new volume META did NOT copy root's db_id=, and volume dir is refused as root of tsdb_tier_add_dir. */
    int vfd = open(v1_dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    CHECK(vfd >= 0);
    bool v_has_dbid = true;
    uint8_t tmp_id[16];
    CHECK(tiers_db_id_read(vfd, &v_has_dbid, tmp_id) == 0 && !v_has_dbid);
    close(vfd);
    char bogus_s[PATH_MAX + 64];
    snprintf(bogus_s, sizeof bogus_s, "%s/val-bogus-on-vol", S);
    cap_begin();
    CHECK(tsdb_tier_add_dir(v1_dir, bogus_s, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_INVALID);
    cap_end(NULL, 0);
    CHECK(access(bogus_s, F_OK) != 0);

    /* Damaged TIERS fails tsdb_open with TSDB_ERR_CORRUPT; torn tail in TIERS is cut by writer tsdb_open. */
    {
        char cor_db[PATH_MAX + 32], cor_s0[PATH_MAX + 64], cor_s1[PATH_MAX + 64], cor_tpath[PATH_MAX + 64];
        fresh_dir("val-open-tiers", cor_db, sizeof cor_db);
        snprintf(cor_s0, sizeof cor_s0, "%s/val-open-s0", S);
        snprintf(cor_s1, sizeof cor_s1, "%s/val-open-s1", S);
        snprintf(cor_tpath, sizeof cor_tpath, "%s/%s", cor_db, TIERS_FILE);
        sh("rm -rf '%s' '%s'", cor_s0, cor_s1);
        init_db_closed(cor_db);
        CHECK(tsdb_tier_add_dir(cor_db, cor_s0, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);
        CHECK(tsdb_tier_add_dir(cor_db, cor_s1, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);

        struct stat tst;
        CHECK(stat(cor_tpath, &tst) == 0 && tst.st_size > 32);
        off_t good_sz = tst.st_size;
        /* Append 13 bytes of torn garbage -> writer open cuts tail cleanly. */
        int tfd = open(cor_tpath, O_WRONLY | O_APPEND | O_CLOEXEC);
        CHECK(tfd >= 0 && write(tfd, "torn_tail_123", 13) == 13);
        if (tfd >= 0)
            close(tfd);
        tsdb *cdb = NULL;
        cap_begin();
        CHECK(tsdb_open(cor_db, &o, &cdb) == 0 && cdb != NULL);
        cap_end(log, sizeof log);
        CHECK(strstr(log, "cutting a torn tail") != NULL);
        CHECK(stat(cor_tpath, &tst) == 0 && tst.st_size == good_sz);
        if (cdb)
            CHECK(tsdb_close(cdb) == 0);

        /* Flip byte in record 0 before valid record 1 -> tsdb_open MUST fail with TSDB_ERR_CORRUPT. */
        tfd = open(cor_tpath, O_RDWR | O_CLOEXEC);
        CHECK(tfd >= 0);
        uint8_t b = 0;
        CHECK(pread(tfd, &b, 1, 8) == 1);
        b ^= 0x08;
        CHECK(pwrite(tfd, &b, 1, 8) == 1);
        close(tfd);
        cdb = NULL;
        cap_begin();
        CHECK(tsdb_open(cor_db, &o, &cdb) == TSDB_ERR_CORRUPT && cdb == NULL);
        cap_end(log, sizeof log);
        CHECK(strstr(log, TIERS_FILE) != NULL);
    }

    /* Stripped db_id= while TIERS has stores -> BOTH tsdb_open (writer & read-only) and tsdb_tier_add_dir MUST fail
     * with TSDB_ERR_CORRUPT (never silently mint a new db_id). Also verify duplicate db_id= and parts_v8= refusals. */
    {
        char ndb[PATH_MAX + 32], ns0[PATH_MAX + 64], ns1[PATH_MAX + 64], mpath[PATH_MAX + 64];
        fresh_dir("val-strip-dbid", ndb, sizeof ndb);
        snprintf(ns0, sizeof ns0, "%s/val-strip-s0", S);
        snprintf(ns1, sizeof ns1, "%s/val-strip-s1", S);
        snprintf(mpath, sizeof mpath, "%s/META", ndb);
        sh("rm -rf '%s' '%s'", ns0, ns1);
        init_db_closed(ndb);

        /* parts_v8=1 in root META refuses tier add until optimize clears it. */
        char orig_meta[1024];
        int mfd = open(mpath, O_RDONLY | O_CLOEXEC);
        ssize_t mn = mfd >= 0 ? read(mfd, orig_meta, sizeof orig_meta - 1) : -1;
        CHECK(mn > 0);
        if (mfd >= 0)
            close(mfd);
        orig_meta[mn > 0 ? mn : 0] = '\0';
        mfd = open(mpath, O_WRONLY | O_APPEND | O_CLOEXEC);
        CHECK(mfd >= 0 && write(mfd, "parts_v8=1\n", 11) == 11);
        if (mfd >= 0)
            close(mfd);
        cap_begin();
        CHECK(tsdb_tier_add_dir(ndb, ns0, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_INVALID);
        cap_end(log, sizeof log);
        CHECK(strstr(log, "parts_v8=") != NULL);
        /* Restore original META without parts_v8=1 and add store 0. */
        mfd = open(mpath, O_WRONLY | O_TRUNC | O_CLOEXEC);
        CHECK(mfd >= 0 && write(mfd, orig_meta, (size_t)mn) == mn);
        if (mfd >= 0)
            close(mfd);
        CHECK(tsdb_tier_add_dir(ndb, ns0, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);

        /* Duplicate db_id= line in root META -> TSDB_ERR_CORRUPT. */
        char with_id[1024];
        mfd = open(mpath, O_RDONLY | O_CLOEXEC);
        ssize_t wn = mfd >= 0 ? read(mfd, with_id, sizeof with_id - 1) : -1;
        CHECK(wn > 0);
        if (mfd >= 0)
            close(mfd);
        with_id[wn > 0 ? wn : 0] = '\0';
        mfd = open(mpath, O_WRONLY | O_APPEND | O_CLOEXEC);
        CHECK(mfd >= 0 && write(mfd, "db_id=0102030405060708090a0b0c0d0e0f10\n", 39) == 39);
        if (mfd >= 0)
            close(mfd);
        tsdb *cdb = NULL;
        cap_begin();
        CHECK(tsdb_open(ndb, &o, &cdb) == TSDB_ERR_CORRUPT && cdb == NULL);
        cap_end(log, sizeof log);
        CHECK(strstr(log, "duplicate db_id=") != NULL);

        /* Strip db_id= back to orig_meta while TIERS still lists ns0 -> writer open, read-only open, and 2nd add
         * MUST ALL fail with TSDB_ERR_CORRUPT and never mint a replacement db_id! */
        mfd = open(mpath, O_WRONLY | O_TRUNC | O_CLOEXEC);
        CHECK(mfd >= 0 && write(mfd, orig_meta, (size_t)mn) == mn);
        if (mfd >= 0)
            close(mfd);

        cap_begin();
        CHECK(tsdb_open(ndb, &o, &cdb) == TSDB_ERR_CORRUPT && cdb == NULL);
        cap_end(log, sizeof log);
        CHECK(strstr(log, "no db_id=") != NULL);

        tsdb_options ro2 = o;
        ro2.read_only = 1;
        cap_begin();
        CHECK(tsdb_open(ndb, &ro2, &cdb) == TSDB_ERR_CORRUPT && cdb == NULL);
        cap_end(log, sizeof log);
        CHECK(strstr(log, "no db_id=") != NULL);

        cap_begin();
        CHECK(tsdb_tier_add_dir(ndb, ns1, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_CORRUPT);
        cap_end(log, sizeof log);
        CHECK(strstr(log, "no db_id=") != NULL);
        CHECK(access(ns1, F_OK) != 0);

        /* Verify META still has no db_id= (not silently minted!). */
        int nfd = open(ndb, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        bool nhas = true;
        uint8_t nid[16];
        CHECK(nfd >= 0 && tiers_db_id_read(nfd, &nhas, nid) == 0 && !nhas);
        if (nfd >= 0)
            close(nfd);
    }

    char sub[PATH_MAX + 96];
    snprintf(sub, sizeof sub, "%s/inside-root", db1_dir);
    cap_begin();
    CHECK(tsdb_tier_add(db1, sub, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_INVALID);
    cap_end(NULL, 0);
    snprintf(sub, sizeof sub, "%s/inside-vol", v1_dir);
    cap_begin();
    CHECK(tsdb_tier_add(db1, sub, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_INVALID);
    cap_end(NULL, 0);
    snprintf(sub, sizeof sub, "%s/inside-store", s0);
    cap_begin();
    CHECK(tsdb_tier_add(db1, sub, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_INVALID);
    cap_end(NULL, 0);
    /* Same store twice */
    cap_begin();
    CHECK(tsdb_tier_add(db1, s0, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_INVALID);
    cap_end(NULL, 0);

    /* Non-empty directory refusal. */
    char stray[PATH_MAX + 96];
    snprintf(stray, sizeof stray, "%s/hello.txt", nonempty);
    int sfd = open(stray, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    CHECK(sfd >= 0 && write(sfd, "hi", 2) == 2);
    if (sfd >= 0)
        close(sfd);
    cap_begin();
    CHECK(tsdb_tier_add(db1, nonempty, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_INVALID);
    cap_end(log, sizeof log);
    CHECK(strstr(log, "not empty") != NULL);
    CHECK(access(stray, F_OK) == 0);

    /* Unbound chunk store refused by tsdb_tier_add AND by chunk_store_open_db. */
    uint8_t ub_sid[16];
    CHECK(chunk_store_init(unbound, NULL, ub_sid) == 0);
    cap_begin();
    CHECK(tsdb_tier_add(db1, unbound, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_INVALID);
    cap_end(NULL, 0);
    cap_begin();
    CHECK(chunk_store_open_db(unbound, ub_sid, db1_id, &cst) == CHUNK_ERR_STORE_ID);
    cap_end(log, sizeof log);
    CHECK(strstr(log, "no db= binding") != NULL);

    /* Cross-database store binding refusal naming BOTH db_ids! */
    CHECK(tsdb_tier_add(db2, s_db2, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);
    CHECK(tiers_tsdb_db_id(db2, db2_id));
    CHECK(memcmp(db1_id, db2_id, 16) != 0);

    char hex1[33], hex2[33];
    chunk_oid_hex(db1_id, hex1);
    chunk_oid_hex(db2_id, hex2);

    cap_begin();
    CHECK(tsdb_tier_add(db2, s0, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_INVALID);
    cap_end(log, sizeof log);
    CHECK(strstr(log, hex1) != NULL && strstr(log, hex2) != NULL);

    /* Also when target db3 has no db_id yet: refuses naming db1's id. */
    cap_begin();
    CHECK(tsdb_tier_add(db3, s0, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_INVALID);
    cap_end(log, sizeof log);
    CHECK(strstr(log, hex1) != NULL);

    /* Opening db1's store with db2's expected db_id refuses naming BOTH db_ids! */
    cap_begin();
    CHECK(chunk_store_open_db(s0, v1.stores[0].id, db2_id, &cst) == CHUNK_ERR_STORE_ID);
    cap_end(log, sizeof log);
    CHECK(strstr(log, hex1) != NULL && strstr(log, hex2) != NULL);

    /* Read-only handle refuses tsdb_tier_add. */
    tsdb_options ro = o;
    ro.read_only = 1;
    tsdb *db1_ro = NULL;
    CHECK(tsdb_open(db1_dir, &ro, &db1_ro) == 0 && db1_ro != NULL);
    CHECK(tsdb_tier_add(db1_ro, s0, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_READONLY);
    CHECK(tsdb_close(db1_ro) == 0);

    tiers_view_free(&v1);
    close(dfd1);
    CHECK(tsdb_close(db1) == 0);
    CHECK(tsdb_close(db2) == 0);
    CHECK(tsdb_close(db3) == 0);
    printf("  tsdb_tier_add validation, db_id, same-device & cross-db binding ok\n");
}

/* ---------------------------------------------------------------- 4. Concurrent adds (OFD lock) */

static void test_concurrent_adds(void)
{
    char dbdir[PATH_MAX + 32];
    fresh_dir("conc-db", dbdir, sizeof dbdir);
    init_db_closed(dbdir);

    int go_pipe[2], ready_pipe[2];
    CHECK(pipe(go_pipe) == 0 && pipe(ready_pipe) == 0);

    pid_t pids[2];
    for (int p = 0; p < 2; p++) {
        pids[p] = fork();
        if (pids[p] == 0) {
            close(go_pipe[1]);
            close(ready_pipe[0]);
            char c = 'R';
            if (write(ready_pipe[1], &c, 1) != 1)
                _exit(10);
            close(ready_pipe[1]);
            if (read(go_pipe[0], &c, 1) != 1)
                _exit(11);
            close(go_pipe[0]);
            for (int k = 0; k < 4; k++) {
                char sp[PATH_MAX + 64];
                snprintf(sp, sizeof sp, "%s/conc-s-%d-%d", S, p, k);
                int rc = tsdb_tier_add_dir(dbdir, sp, k % 2 ? "hdd" : "ssd", TSDB_TIER_ALLOW_SHARED_DEV);
                if (rc != 0)
                    _exit(20 + k);
            }
            _exit(0);
        }
        CHECK(pids[p] > 0);
    }
    close(go_pipe[0]);
    close(ready_pipe[1]);
    char rbuf[2];
    CHECK(read(ready_pipe[0], &rbuf[0], 1) == 1 && read(ready_pipe[0], &rbuf[1], 1) == 1);
    close(ready_pipe[0]);
    CHECK(write(go_pipe[1], "GG", 2) == 2);
    close(go_pipe[1]);

    for (int p = 0; p < 2; p++) {
        int st = 0;
        CHECK(waitpid(pids[p], &st, 0) == pids[p] && WIFEXITED(st) && WEXITSTATUS(st) == 0);
    }

    int dfd = open(dbdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    CHECK(dfd >= 0);
    bool has_db = false;
    uint8_t db_id[16];
    CHECK(tiers_db_id_read(dfd, &has_db, db_id) == 0 && has_db);
    tiers_view v;
    CHECK(tiers_load(dfd, false, true, &v) == 0);
    CHECK(v.exists && v.nstores == 8);
    for (size_t i = 0; i < v.nstores; i++) {
        chunk_store *cst = NULL;
        CHECK(chunk_store_open_db(v.stores[i].path, v.stores[i].id, db_id, &cst) == 0 && cst != NULL);
        if (cst)
            chunk_store_close(cst);
    }
    tiers_view_free(&v);
    close(dfd);
    printf("  concurrent tsdb_tier_add_dir from 2 processes (VOLUMES OFD lock) ok\n");
}

/* ---------------------------------------------------------------- 5. Beside a running writer */

typedef struct writer_ctx {
    tsdb *db;
    tsdb_sid sid;
    atomic_bool stop;
    atomic_int err;
    uint64_t written;
} writer_ctx;

static void *writer_thread(void *arg)
{
    writer_ctx *w = arg;
    int64_t ts = 1700000000000LL;
    while (!atomic_load(&w->stop)) {
        float val = (float)(w->written % 1000);
        tsdb_values col = {TSDB_FLOAT32, &val, NULL};
        int rc = tsdb_insert_series_values(w->db, w->sid, &ts, &col, 1);
        if (rc != 0) {
            atomic_store(&w->err, rc);
            break;
        }
        ts += 1000;
        w->written++;
    }
    return NULL;
}

static void test_beside_running_writer(void)
{
    char dbdir[PATH_MAX + 32];
    fresh_dir("live-db", dbdir, sizeof dbdir);

    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 1;
    o.stats = -1;
    o.latest_cache = 0;
    tsdb *db = NULL;
    CHECK(tsdb_open(dbdir, &o, &db) == 0 && db != NULL);

    tsdb_sid sid = 0;
    CHECK(tsdb_series(db, "sensor.temp", &sid) == 0);

    writer_ctx wc = {.db = db, .sid = sid, .stop = false, .err = 0, .written = 0};
    pthread_t th;
    CHECK(pthread_create(&th, NULL, writer_thread, &wc) == 0);

    /* Add 4 stores from outside (tsdb_tier_add_dir) and 2 via the open writer handle while inserts run. */
    for (int i = 0; i < 6; i++) {
        char sp[PATH_MAX + 64];
        snprintf(sp, sizeof sp, "%s/live-s%d", S, i);
        if (i < 4)
            CHECK(tsdb_tier_add_dir(dbdir, sp, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);
        else
            CHECK(tsdb_tier_add(db, sp, "ssd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);
    }

    atomic_store(&wc.stop, true);
    pthread_join(th, NULL);
    CHECK(atomic_load(&wc.err) == 0 && wc.written > 0);
    CHECK(tsdb_close(db) == 0);

    /* Reopen and verify all rows + db_id + all 6 stores open cleanly. */
    CHECK(tsdb_open(dbdir, &o, &db) == 0 && db != NULL);
    uint8_t db_id[16];
    CHECK(tiers_tsdb_db_id(db, db_id));
    tsdb_bucket *b = NULL;
    size_t nb = 0;
    CHECK(tsdb_query_agg(db, sid, TSDB_TS_MIN, TSDB_TS_MAX, 0, &b, &nb) == 0);
    CHECK(nb == 1 && b[0].count == wc.written);
    free(b);
    CHECK(tsdb_close(db) == 0);

    int dfd = open(dbdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    CHECK(dfd >= 0);
    tiers_view v;
    CHECK(tiers_load(dfd, false, true, &v) == 0);
    CHECK(v.exists && v.nstores == 6);
    for (size_t i = 0; i < v.nstores; i++) {
        chunk_store *cst = NULL;
        CHECK(chunk_store_open_db(v.stores[i].path, v.stores[i].id, db_id, &cst) == 0 && cst != NULL);
        if (cst)
            chunk_store_close(cst);
    }
    tiers_view_free(&v);
    close(dfd);
    printf("  tsdb_tier_add(_dir) beside a running writer (%llu rows) ok\n", (unsigned long long)wc.written);
}

/* ---------------------------------------------------------------- 6. Crashfs power-loss sweeps */

static void write_ack(const char *ackfile, const char *text)
{
    int fd = open(ackfile, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd >= 0) {
        if (write(fd, text, strlen(text)) < 0)
            _exit(2);
        close(fd);
    }
}

static bool read_ack(const char *ackfile, char *buf, size_t cap)
{
    FILE *f = fopen(ackfile, "r");
    if (!f)
        return false;
    size_t n = fread(buf, 1, cap - 1, f);
    fclose(f);
    buf[n] = '\0';
    return n > 0;
}

static int run_crash_child(const char *mode, const char *root_dir, const char *dbdir, const char *stdir,
                           const char *ackfile, int warmup, int seed)
{
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512];
        const char *prev = getenv("ASAN_OPTIONS");
        snprintf(asan, sizeof asan, "%s%sverify_asan_link_order=0:detect_leaks=0",
                 prev && *prev ? prev : "", prev && *prev ? ":" : "");
        setenv("ASAN_OPTIONS", asan, 1);
        char w[16], sd[16];
        snprintf(w, sizeof w, "%d", warmup);
        snprintf(sd, sizeof sd, "%d", seed);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", root_dir, 1);
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
        if (mutant_name)
            execl(SELF, "test_tiers", "--child", mode, dbdir, stdir, ackfile, "--mutant", mutant_name, (char *)NULL);
        else
            execl(SELF, "test_tiers", "--child", mode, dbdir, stdir, ackfile, (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

static void child_entry(const char *mode, const char *dbdir, const char *stdir, const char *ackfile)
{
    if (strcmp(mode, "add1") == 0 || strcmp(mode, "add2") == 0) {
        const char *cls = strcmp(mode, "add1") == 0 ? "hdd" : "ssd";
        if (tsdb_tier_add_dir(dbdir, stdir, cls, TSDB_TIER_ALLOW_SHARED_DEV) != 0)
            _exit(4);
        write_ack(ackfile, "ok");
        exit(0); /* exit() (not _exit): CRASHFS_AT_EXIT rolls back anything not synced! */
    }
    _exit(3);
}

static void sweep_tier_add(const char *mode, size_t base_stores)
{
    char wrap[PATH_MAX + 32], dbdir[PATH_MAX + 64], s0[PATH_MAX + 64], target[PATH_MAX + 64];
    char ackf[PATH_MAX + 32], ackbuf[64], msg[256];
    snprintf(ackf, sizeof ackf, "%s/pl-%s.ack", S, mode);

    int crashes = 0;
    bool finished = false;
    for (int k = 0; k < 400 && !finished; k++) {
        fresh_dir("pl-wrap", wrap, sizeof wrap);
        snprintf(dbdir, sizeof dbdir, "%s/db", wrap);
        snprintf(s0, sizeof s0, "%s/store0", wrap);
        snprintf(target, sizeof target, "%s/store_target", wrap);
        CHECK(mkdir(dbdir, 0755) == 0);
        init_db_closed(dbdir);
        if (base_stores == 1)
            CHECK(tsdb_tier_add_dir(dbdir, s0, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);
        unlink(ackf);

        int code = run_crash_child(mode, wrap, dbdir, target, ackf, k, k + 17);
        CHECK(code == 99 || code == 0);
        if (code != 99 && code != 0)
            break;
        crashes += (code == 99);
        finished = (code == 0);

        bool acked = read_ack(ackf, ackbuf, sizeof ackbuf);

        /* Database MUST open cleanly after every power cut. */
        tsdb_options o;
        tsdb_options_init(&o);
        o.merge_threads = 0;
        o.stats = -1;
        o.latest_cache = 0;
        tsdb *db = NULL;
        cap_begin();
        int orc = tsdb_open(dbdir, &o, &db);
        cap_end(NULL, 0);
        if (orc != 0) {
            snprintf(msg, sizeof msg, "%s step %d: tsdb_open failed after power loss (rc=%d)", mode, k, orc);
            fail_at(__FILE__, __LINE__, msg);
            continue;
        }
        uint8_t db_id[16];
        bool has_db_id = tiers_tsdb_db_id(db, db_id);
        CHECK(tsdb_close(db) == 0);

        /* Load TIERS (cutting any torn tail). */
        int dfd = open(dbdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        CHECK(dfd >= 0);
        tiers_view v;
        cap_begin();
        int lrc = tiers_load(dfd, true, true, &v);
        cap_end(NULL, 0);
        close(dfd);

        if (lrc != 0) {
            snprintf(msg, sizeof msg, "%s step %d: tiers_load failed after power loss (rc=%d)", mode, k, lrc);
            fail_at(__FILE__, __LINE__, msg);
            continue;
        }

        if (acked) {
            if (!has_db_id || v.nstores != base_stores + 1) {
                snprintf(msg, sizeof msg, "%s step %d: acknowledged add lost (has_db_id=%d nstores=%zu want=%zu)",
                         mode, k, has_db_id, v.nstores, base_stores + 1);
                fail_at(__FILE__, __LINE__, msg);
            }
        } else {
            if (v.nstores != base_stores && v.nstores != base_stores + 1) {
                snprintf(msg, sizeof msg, "%s step %d: unexpected nstores=%zu (base=%zu)", mode, k, v.nstores,
                         base_stores);
                fail_at(__FILE__, __LINE__, msg);
            }
        }

        /* INVARIANT: every store recorded in TIERS MUST have root db_id and MUST open via chunk_store_open_db! */
        if (v.nstores > 0 && !has_db_id) {
            snprintf(msg, sizeof msg, "%s step %d: TIERS has %zu store(s) but root META lacks db_id", mode, k,
                     v.nstores);
            fail_at(__FILE__, __LINE__, msg);
        }
        for (size_t i = 0; i < v.nstores && has_db_id; i++) {
            chunk_store *cst = NULL;
            cap_begin();
            int src = chunk_store_open_db(v.stores[i].path, v.stores[i].id, db_id, &cst);
            cap_end(NULL, 0);
            if (src != 0 || !cst) {
                snprintf(msg, sizeof msg, "%s step %d: TIERS store %zu (%s) failed to open (rc=%d)", mode, k, i,
                         v.stores[i].path, src);
                fail_at(__FILE__, __LINE__, msg);
            } else {
                chunk_store_close(cst);
            }
        }

        /* If power cut happened before TIERS recorded target, retrying tsdb_tier_add_dir MUST succeed! */
        size_t got_n = v.nstores;
        tiers_view_free(&v);
        if (!acked && got_n == base_stores) {
            int rrc = tsdb_tier_add_dir(dbdir, target, base_stores == 0 ? "hdd" : "ssd", TSDB_TIER_ALLOW_SHARED_DEV);
            if (rrc != 0) {
                snprintf(msg, sizeof msg, "%s step %d: retry tsdb_tier_add_dir after crash failed (rc=%d)", mode, k,
                         rrc);
                fail_at(__FILE__, __LINE__, msg);
            } else {
                int rfd = open(dbdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
                CHECK(rfd >= 0);
                uint8_t rdb[16];
                bool rhas = false;
                CHECK(tiers_db_id_read(rfd, &rhas, rdb) == 0 && rhas);
                tiers_view rv;
                CHECK(tiers_load(rfd, false, true, &rv) == 0 && rv.nstores == base_stores + 1);
                close(rfd);
                for (size_t i = 0; i < rv.nstores; i++) {
                    chunk_store *cst = NULL;
                    CHECK(chunk_store_open_db(rv.stores[i].path, rv.stores[i].id, rdb, &cst) == 0 && cst != NULL);
                    if (cst)
                        chunk_store_close(cst);
                }
                tiers_view_free(&rv);
            }
        }
    }
    CHECK(finished && crashes > 3);
}

static void test_power_loss(void)
{
    if (access(SHIM, R_OK) != 0) {
        if (getenv("TSDB_REQUIRE_CRASHFS") && strcmp(getenv("TSDB_REQUIRE_CRASHFS"), "1") == 0) {
            fail_at(__FILE__, __LINE__, "TSDB_REQUIRE_CRASHFS=1 set but libcrashfs.so missing");
            return;
        }
        printf("  power loss: skipped (no %s)\n", SHIM);
        return;
    }
    sweep_tier_add("add1", 0);
    sweep_tier_add("add2", 1);
    printf("  crashfs power loss (and retry) at every step of tsdb_tier_add (1st & 2nd store) ok\n");
}
#endif /* TSDB_EC */

int main(int argc, char **argv)
{
#ifdef TSDB_EC
    if (argc >= 6 && strcmp(argv[1], "--child") == 0) {
        if (argc >= 8 && strcmp(argv[6], "--mutant") == 0) {
            if (apply_mutant(argv[7]) != 0)
                _exit(5);
        }
        child_entry(argv[2], argv[3], argv[4], argv[5]);
    }
#endif

    const char *base = "/tmp";
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mutant") == 0 && i + 1 < argc) {
            mutant_name = argv[++i];
            if (apply_mutant(mutant_name) != 0) {
                fprintf(stderr, "test_tiers: unknown mutant '%s'\n", mutant_name);
                return 2;
            }
        } else if (argv[i][0] != '-') {
            base = argv[i];
        } else {
            fprintf(stderr, "usage: %s [--mutant tiersnosync|nodbid|samedev] [scratch]\n", argv[0]);
            return 2;
        }
    }

    if (!realpath(argv[0], SELF))
        snprintf(SELF, sizeof SELF, "%s", argv[0]);
    char dirbuf[PATH_MAX];
    snprintf(dirbuf, sizeof dirbuf, "%s", SELF);
    char *sl = strrchr(dirbuf, '/');
    if (sl)
        *sl = '\0';
    else
        snprintf(dirbuf, sizeof dirbuf, ".");
    snprintf(SHIM, sizeof SHIM, "%s/libcrashfs.so", dirbuf);

    snprintf(S, sizeof S, "%s/test_tiers_%ld", base, (long)getpid());
    if (sh("rm -rf '%s' && mkdir -p '%s'", S, S) != 0) {
        fprintf(stderr, "test_tiers: cannot make scratch dir %s\n", S);
        return 2;
    }

#ifndef TSDB_EC
    test_no_ec();
    sh("rm -rf '%s'", S);
    if (failures) {
        fprintf(stderr, "test_tiers: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_tiers: skipped (no TSDB_EC)\n");
    return 0;
#else
    printf("test_tiers%s%s:\n", mutant_name ? " --mutant " : "", mutant_name ? mutant_name : "");
    test_records_roundtrip();
    test_torn_tail_and_corrupt();
    test_add_validation_and_binding();
    test_concurrent_adds();
    test_beside_running_writer();
    test_power_loss();

    sh("rm -rf '%s'", S);
    if (failures) {
        fprintf(stderr, "test_tiers: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_tiers: ok\n");
    return 0;
#endif
}
