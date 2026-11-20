/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The tier of a tsdb handle (src/tier.c, R7's r7-tier): one chunk backend per handle, shared by every engine.
 *
 *   noec     a build without TSDB_EC refuses a volume whose META says requires=tier (the one case that also runs there)
 *   volumes  two volumes with one tier: META gets vuuid= and requires=tier once, each volume its own uuid and EPOCH
 *            in every store; a reopen of one volume while the other serves; a feed enable keeps the word
 *   down     a store missing at open is DOWN and read around (min_up met), the open fails when it is not met; the
 *            tier halts (settles, deletes) with one line
 *   lost     every store missing, or replaced by an empty directory: the open fails with a line per store, nothing is
 *            made (the root's requires=tier says the tier holds data; the stores alone look new)
 *   fence    a store that cannot take a new volume's epoch (its directory not writable) halts the tier, with one line
 *   unsure   a damaged EPOCH slot (a torn store looks the same) and then an open: puts work
 *   refuse   requires=tier without a tier, without TSDB_TEST_TIER_I_KNOW=1, read-only and following; a tier on a
 *            cluster volume; a running follower that meets an .ec name stops
 *   follow   a follower whose writer's MANIFEST names an .ec part, through eng_refresh: one line, the engine failed
 *   reval    revalidation counts an .ec part and its .part as one, settled_ and retired_ files as none
 *   power    power loss (crashfs) at every step of the first open with a tier: META has both lines or neither, and the
 *            uuid never changes once an open with the tier returned; then with the stores under crashfs too: every
 *            store is UP after the next open (made, or found)
 *   eio      a store fails in service (crashfs, EIO on its files): the tier halts with one line, reads go on
 *
 * Mutants (--mutant NAME, src/tier.h): tierperengine, tierunsure, tierstrict, tierfresh, tierfence (each must fail
 * here), tiernoec (must fail in a NO_EC build). Usage: test_tier [--mutant NAME] [SCRATCH]
 */
#define _GNU_SOURCE
#include "chunk.h"
#include "chunk_priv.h"
#include "feed.h"
#include "tier.h"
#include "tsdb.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static int fails;
#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);                                               \
            fails++;                                                                                                   \
        }                                                                                                              \
    } while (0)

static char S[PATH_MAX], SELF[PATH_MAX], SHIM[PATH_MAX + 32];
static uint8_t OBJ[3000]; /* the bytes of every object; filled before anything, a child too */

static void sh(const char *fmt, const char *a)
{
    char cmd[3 * PATH_MAX];
    snprintf(cmd, sizeof cmd, fmt, a, a, a);
    if (system(cmd) != 0)
        fprintf(stderr, "test_tier: '%s' failed\n", cmd);
}

static void fresh(const char *d) { sh("rm -rf '%s' && mkdir -p '%s'", d); }

static char *slurp(const char *path)
{
    static char buf[4096];
    buf[0] = '\0';
    int fd = open(path, O_RDONLY);
    if (fd < 0)
        return buf;
    ssize_t r = read(fd, buf, sizeof buf - 1);
    close(fd);
    buf[r > 0 ? r : 0] = '\0';
    return buf;
}

static void append(const char *path, const char *text)
{
    int fd = open(path, O_WRONLY | O_APPEND);
    if (fd >= 0) {
        if (write(fd, text, strlen(text)) != (ssize_t)strlen(text))
            fprintf(stderr, "test_tier: append to %s failed\n", path);
        close(fd);
    }
}

/* stderr into a file between cap_begin and cap_end, which returns it (slurp's buffer) and echoes it to the real one */
static char CAP[PATH_MAX + 16];
static int cap_saved = -1;

static void cap_begin(void)
{
    fflush(stderr);
    snprintf(CAP, sizeof CAP, "%s/stderr.cap", S);
    int fd = open(CAP, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    cap_saved = dup(2);
    if (fd >= 0) {
        dup2(fd, 2);
        close(fd);
    }
}

static const char *cap_end(void)
{
    fflush(stderr);
    if (cap_saved >= 0) {
        dup2(cap_saved, 2);
        close(cap_saved);
        cap_saved = -1;
    }
    const char *t = slurp(CAP);
    fputs(t, stderr);
    return t;
}

static int count(const char *hay, const char *needle)
{
    int n = 0;
    for (const char *q = hay; (q = strstr(q, needle)) != NULL; q += strlen(needle))
        n++;
    return n;
}

#ifdef TSDB_EC
/* META of dir: whether it says requires=tier, and its vuuid (32 hex, "" without) */
static bool meta_tier(const char *dir, char vu[33])
{
    char p[PATH_MAX + 8];
    snprintf(p, sizeof p, "%s/META", dir);
    const char *t = slurp(p);
    unsigned feats = 0;
    meta_requires_parse(t, &feats, NULL, 0);
    const char *v = strstr(t, "vuuid=");
    vu[0] = '\0';
    if (v)
        snprintf(vu, 33, "%.32s", v + 6);
    return feats & META_REQ_TIER;
}

static void tier_env(const char *stores, int n, int k, int m)
{
    char spec[4 * PATH_MAX] = "", *q = spec;
    q += sprintf(q, "k=%d,m=%d,dirs=", k, m);
    for (int i = 0; i < n; i++)
        q += sprintf(q, "%s%s/st%d", i ? ":" : "", stores, i);
    setenv("TSDB_TEST_TIER", spec, 1);
    setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);
}
#endif

static void no_tier_env(void)
{
    unsetenv("TSDB_TEST_TIER");
    unsetenv("TSDB_TEST_TIER_I_KNOW");
}

static int open_db(const char *dir, tsdb **db, int read_only)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.read_only = read_only;
    *db = NULL;
    return tsdb_open(dir, &o, db);
}

#ifdef TSDB_EC
/* The engine level: a tier opened by hand and one engine of it (the backend is then reachable). */
typedef struct {
    tier *t;
    eng *db;
} direct;

static int direct_open(const char *dir, uint32_t vol, direct *d)
{
    tsdb_options o;
    tsdb_options_init(&o);
    d->t = NULL;
    d->db = NULL;
    int rc = tier_open(dir, &o, &d->t);
    if (rc)
        return rc;
    tier_want(d->t, vol);
    rc = eng_open(dir, &o, &d->db);
    tier_want(NULL, 0);
    if (rc) {
        tier_put(d->t);
        d->t = NULL;
        d->db = NULL;
    }
    return rc;
}

static void direct_close(direct *d)
{
    if (d->db)
        eng_close(d->db);
    tier_put(d->t);
    d->db = NULL;
    d->t = NULL;
}

static const uint8_t OID[16] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

static int put_obj(direct *d, const uint8_t oid[16])
{
    chunk_geom g;
    int rc = chunk_geom_make(&g, 2, 1, 512, sizeof OBJ);
    return rc ? rc : chunk_be_put_object(tier_backend(d->t), d->db->tier_vol, tier_vuuid(d->db), oid, &g, 1, OBJ, ~0u,
                                         NULL);
}

static int get_obj(direct *d, const uint8_t oid[16])
{
    chunk_obj *ob = NULL;
    int rc = chunk_obj_open(tier_backend(d->t), d->db->tier_vol, tier_vuuid(d->db), oid, NULL, NULL, &ob);
    if (rc)
        return rc;
    uint8_t back[sizeof OBJ];
    chunk_report rep;
    memset(&rep, 0, sizeof rep);
    rc = chunk_obj_read(ob, 0, sizeof back, back, 0, &rep);
    chunk_obj_close(ob);
    return rc ? rc : memcmp(back, OBJ, sizeof OBJ) == 0 ? 0 : TSDB_ERR_CORRUPT;
}

#endif

/* ------------------------------------------------------------------------------------------------------------ noec */

static void t_noec(void)
{
    char dir[PATH_MAX + 32], meta[PATH_MAX + 40];
    snprintf(dir, sizeof dir, "%s/noec", S);
    fresh(dir);
    no_tier_env();
    tsdb *db;
    CHECK(open_db(dir, &db, 0) == 0);
    tsdb_close(db);
    snprintf(meta, sizeof meta, "%s/META", dir);
    append(meta, "requires=tier\n");
    /* without TSDB_EC: no build of that kind reads such a volume; with it: no tier is configured. Refused either way. */
    CHECK(open_db(dir, &db, 0) != 0);
    CHECK(strstr(slurp(meta), "requires=tier\n") != NULL); /* a refused open leaves META as it was */
    printf("  noec: requires=tier is refused by a build without the codec and by an open without a tier\n");
}

#ifdef TSDB_EC
/* --------------------------------------------------------------------------------------------------------- volumes */

static void t_volumes(void)
{
    char dir[PATH_MAX + 32], vol[PATH_MAX + 32], stores[PATH_MAX + 32], p[PATH_MAX + 64];
    snprintf(dir, sizeof dir, "%s/vols/db", S);
    snprintf(vol, sizeof vol, "%s/vols/v1", S);
    snprintf(stores, sizeof stores, "%s/vols/stores", S);
    snprintf(p, sizeof p, "%s/vols", S);
    fresh(p);
    mkdir(stores, 0755);
    tier_env(stores, 3, 2, 1);
    tsdb *db;
    int rc = open_db(dir, &db, 0);
    CHECK(rc == 0);
    if (rc)
        return;
    uint32_t idx = 0;
    rc = tsdb_volume_add(db, vol, &idx);
    CHECK(rc == 0 && idx == 1); /* tierperengine: the second engine's own backend finds the stores BUSY */
    tsdb_sid a = 0, b = 0;
    CHECK(tsdb_series_create(db, "a", TSDB_FLOAT32, NULL, &a) == 0);
    CHECK(rc != 0 || tsdb_series_create(db, "b", TSDB_FLOAT32, vol, &b) == 0);
    char u0[33], u1[33];
    CHECK(meta_tier(dir, u0) && strlen(u0) == 32);
    CHECK(rc != 0 || (meta_tier(vol, u1) && strlen(u1) == 32 && strcmp(u0, u1) != 0));
    if (fails) {
        snprintf(p, sizeof p, "%s/META", vol);
        fprintf(stderr, "volume META: [%s] u0 %s u1 %s\n", slurp(p), u0, u1);
    }
    for (int i = 0; i < 3; i++) {
        snprintf(p, sizeof p, "%s/st%d/s0/EPOCH", stores, i);
        CHECK(access(p, F_OK) == 0);
        snprintf(p, sizeof p, "%s/st%d/s1/EPOCH", stores, i);
        CHECK(rc != 0 || access(p, F_OK) == 0);
    }
    if (rc == 0) {
        /* a reopen of volume 1 (the outage path) while the root serves: the new engine takes its own reference to
         * the same backend, no second open of the stores */
        CHECK(tsdb_append(db, b, 1000, 1.0f) == 0);
        CHECK(tsdb_volume_quiesce(db, 1) == 0);
        CHECK(tsdb_append(db, a, 1000, 2.0f) == 0);
        CHECK(tsdb_volume_idle(db, 1, 5000) == 0);
        tsdb_volume_report rep;
        memset(&rep, 0, sizeof rep);
        CHECK(tsdb_volume_reopen(db, 1, &rep) == 0 && !rep.diverged);
        CHECK(tsdb_append(db, a, 2000, 3.0f) == 0);
        CHECK(tsdb_append(db, b, 2000, 4.0f) == 0);
        char after[33];
        CHECK(meta_tier(vol, after) && strcmp(after, u1) == 0); /* the volume's uuid, made at its add, kept */
    }
    /* a feed enable rewrites the requires= line: it keeps "tier" */
    tsdb_feed_options fo;
    memset(&fo, 0, sizeof fo);
    CHECK(tsdb_feed_enable(db, &fo) == 0);
    tsdb_close(db);
    snprintf(p, sizeof p, "%s/META", dir);
    unsigned feats = 0;
    meta_requires_parse(slurp(p), &feats, NULL, 0);
    CHECK((feats & (META_REQ_TIER | META_REQ_FEED)) == (META_REQ_TIER | META_REQ_FEED));
    /* the uuids are made once */
    CHECK(open_db(dir, &db, 0) == 0);
    tsdb_close(db);
    char w0[33];
    CHECK(meta_tier(dir, w0) && strcmp(w0, u0) == 0);
    printf("  volumes: two volumes, one backend, uuids made once, a reopen while the other serves, feed keeps tier\n");
}

/* ------------------------------------------------------------------------------------------------------------ down */

static void t_down(void)
{
    char dir[PATH_MAX + 32], stores[PATH_MAX + 32], p[PATH_MAX + 64], q[PATH_MAX + 64];
    snprintf(p, sizeof p, "%s/down", S);
    fresh(p);
    snprintf(dir, sizeof dir, "%s/down/db", S);
    snprintf(stores, sizeof stores, "%s/down/stores", S);
    mkdir(stores, 0755);
    tier_env(stores, 3, 2, 1);
    direct d;
    CHECK(direct_open(dir, 0, &d) == 0);
    if (!d.db)
        return;
    CHECK(tier_writable(d.t));
    CHECK(put_obj(&d, OID) == 0 && get_obj(&d, OID) == 0);
    direct_close(&d);
    snprintf(p, sizeof p, "%s/st1", stores);
    snprintf(q, sizeof q, "%s/st1.gone", stores);
    CHECK(rename(p, q) == 0);
    cap_begin();
    int rc = direct_open(dir, 0, &d); /* tierstrict: a missing store fails the open */
    bool w1 = rc == 0 && tier_writable(d.t), w2 = rc == 0 && tier_writable(d.t);
    const char *err = cap_end();
    CHECK(rc == 0);
    CHECK(count(err, "settles and chunk deletes stop") == 1); /* one line, however often it is asked */
    if (rc == 0) {
        CHECK(chunk_be_store_state(tier_backend(d.t), 1) == CHUNK_STORE_DOWN_MISSING);
        CHECK(!w1 && !w2);
        CHECK(get_obj(&d, OID) == 0); /* read around: decoded from the two left */
        direct_close(&d);
    }
    /* the store is not made again: a tier that has raised a volume treats a store without META as a lost disk */
    CHECK(access(p, F_OK) != 0);
    char p2[PATH_MAX + 64], q2[PATH_MAX + 64];
    snprintf(p2, sizeof p2, "%s/st2", stores);
    snprintf(q2, sizeof q2, "%s/st2.gone", stores);
    CHECK(rename(p2, q2) == 0);
    CHECK(direct_open(dir, 0, &d) == TSDB_ERR_UNAVAILABLE && !d.t); /* 1 of 3 up, 2 needed */
    CHECK(rename(q2, p2) == 0 && rename(q, p) == 0);
    CHECK(direct_open(dir, 0, &d) == 0); /* back at the next open */
    if (d.db) {
        CHECK(tier_writable(d.t) && get_obj(&d, OID) == 0);
        direct_close(&d);
    }
    printf("  down: a missing store is DOWN and read around, the tier halts once; too few stores fail the open\n");
}

/* ------------------------------------------------------------------------------------------------------------ lost */

static void t_lost(void)
{
    char dir[PATH_MAX + 32], stores[PATH_MAX + 32], p[PATH_MAX + 64], q[PATH_MAX + 64];
    snprintf(p, sizeof p, "%s/lost", S);
    fresh(p);
    snprintf(dir, sizeof dir, "%s/lost/db", S);
    snprintf(stores, sizeof stores, "%s/lost/stores", S);
    mkdir(stores, 0755);
    tier_env(stores, 3, 2, 1);
    direct d;
    CHECK(direct_open(dir, 0, &d) == 0);
    if (!d.db)
        return;
    CHECK(put_obj(&d, OID) == 0);
    direct_close(&d);
    for (int i = 0; i < 3; i++) { /* the mounts are not there: empty directories where the stores were */
        snprintf(p, sizeof p, "%s/st%d", stores, i);
        snprintf(q, sizeof q, "%s/st%d.real", stores, i);
        CHECK(rename(p, q) == 0 && mkdir(p, 0755) == 0);
    }
    for (int round = 0; round < 2; round++) { /* empty directories, then no directories at all */
        cap_begin();
        int rc = direct_open(dir, 0, &d);
        tsdb *db = NULL;
        int rc2 = open_db(dir, &db, 0);
        const char *err = cap_end();
        CHECK(rc != 0 && !d.t); /* tierfresh: the stores are made again and the open succeeds */
        CHECK(rc2 != 0 && !db);
        if (db)
            tsdb_close(db);
        if (d.t)
            direct_close(&d);
        CHECK(count(err, "has no META, but the tier holds data") == 6);
        for (int i = 0; i < 3; i++) {
            snprintf(p, sizeof p, "%s/st%d/META", stores, i);
            CHECK(access(p, F_OK) != 0); /* nothing made */
            snprintf(p, sizeof p, "%s/st%d", stores, i);
            CHECK(round == 0 || access(p, F_OK) != 0);
            if (round == 0)
                CHECK(rmdir(p) == 0);
        }
    }
    for (int i = 0; i < 3; i++) {
        snprintf(p, sizeof p, "%s/st%d", stores, i);
        snprintf(q, sizeof q, "%s/st%d.real", stores, i);
        CHECK(rename(q, p) == 0);
    }
    CHECK(direct_open(dir, 0, &d) == 0); /* the mounts are back */
    if (d.db) {
        CHECK(tier_writable(d.t) && get_obj(&d, OID) == 0);
        direct_close(&d);
    }
    printf("  lost: every store missing or empty: the open fails with a line per store, nothing is made\n");
}

/* ----------------------------------------------------------------------------------------------------------- fence */

static void t_fence(void)
{
    if (geteuid() == 0) {
        printf("  fence: skipped (root writes into a directory of mode 0555)\n");
        return;
    }
    char dir[PATH_MAX + 32], vol[PATH_MAX + 32], stores[PATH_MAX + 32], p[PATH_MAX + 64];
    snprintf(p, sizeof p, "%s/fence", S);
    fresh(p);
    snprintf(dir, sizeof dir, "%s/fence/db", S);
    snprintf(vol, sizeof vol, "%s/fence/v1", S);
    snprintf(stores, sizeof stores, "%s/fence/stores", S);
    mkdir(stores, 0755);
    tier_env(stores, 3, 2, 1);
    direct d;
    CHECK(direct_open(dir, 0, &d) == 0);
    if (!d.db)
        return;
    CHECK(tier_writable(d.t) && put_obj(&d, OID) == 0);
    snprintf(p, sizeof p, "%s/st1", stores);
    CHECK(chmod(p, 0555) == 0); /* store 1 stays UP, but s1/ cannot be made in it */
    tsdb_options o;
    tsdb_options_init(&o);
    eng *e = NULL;
    cap_begin();
    tier_want(d.t, 1);
    int rc = eng_open(vol, &o, &e);
    tier_want(NULL, 0);
    bool w = tier_writable(d.t);
    const char *err = cap_end();
    CHECK(rc == 0); /* the other two hold the fence */
    CHECK(!w);      /* tierfence: the tier stays writable with store 1 unfenced */
    CHECK(count(err, "tier store 1 (") == 1 && count(err, "did not take the volume's epoch") == 1);
    CHECK(chunk_be_store_state(tier_backend(d.t), 1) == CHUNK_STORE_UP);
    snprintf(p, sizeof p, "%s/st1/s1", stores);
    CHECK(access(p, F_OK) != 0);
    CHECK(get_obj(&d, OID) == 0); /* reads go on */
    if (e)
        eng_close(e);
    direct_close(&d);
    snprintf(p, sizeof p, "%s/st1", stores);
    CHECK(chmod(p, 0755) == 0);
    printf("  fence: a store UP that cannot take a volume's epoch halts the tier with one line\n");
}

/* ---------------------------------------------------------------------------------------------------------- unsure */

static void t_unsure(void)
{
    char dir[PATH_MAX + 32], stores[PATH_MAX + 32], p[PATH_MAX + 64];
    snprintf(p, sizeof p, "%s/unsure", S);
    fresh(p);
    snprintf(dir, sizeof dir, "%s/unsure/db", S);
    snprintf(stores, sizeof stores, "%s/unsure/stores", S);
    mkdir(stores, 0755);
    tier_env(stores, 3, 2, 1);
    direct d;
    CHECK(direct_open(dir, 0, &d) == 0);
    if (!d.db)
        return;
    direct_close(&d);
    /* one slot of store 0's EPOCH damaged (a torn store and rot look alike): its volume opens unsure */
    snprintf(p, sizeof p, "%s/st0/s0/EPOCH", stores);
    int fd = open(p, O_WRONLY);
    CHECK(fd >= 0 && pwrite(fd, "garbage-garbage-garbage-garbage!", 32, 4096) == 32);
    if (fd >= 0)
        close(fd);
    CHECK(direct_open(dir, 0, &d) == 0);
    if (!d.db)
        return;
    chunk_store *st = chunk_be_store(tier_backend(d.t), 0);
    CHECK(st && !chunk_store_epoch_unsure(st, 0)); /* tierunsure: nobody raised it */
    uint8_t oid[16];
    memcpy(oid, OID, 16);
    oid[0] = 0x77;
    CHECK(put_obj(&d, oid) == 0 && get_obj(&d, oid) == 0);
    direct_close(&d);
    printf("  unsure: a damaged EPOCH slot is cleared by the open's epoch 1: puts work\n");
}

/* ---------------------------------------------------------------------------------------------------------- refuse */

static void t_refuse(void)
{
    char dir[PATH_MAX + 32], stores[PATH_MAX + 32], p[PATH_MAX + 64];
    snprintf(p, sizeof p, "%s/refuse", S);
    fresh(p);
    snprintf(dir, sizeof dir, "%s/refuse/db", S);
    snprintf(stores, sizeof stores, "%s/refuse/stores", S);
    mkdir(stores, 0755);
    tier_env(stores, 3, 2, 1);
    tsdb *db;
    CHECK(open_db(dir, &db, 0) == 0);
    tsdb_close(db);
    no_tier_env();
    CHECK(open_db(dir, &db, 0) == TSDB_ERR_INVALID); /* no tier */
    cap_begin();
    int rro = open_db(dir, &db, 1);
    const char *err = cap_end();
    CHECK(rro == TSDB_ERR_INVALID); /* read-only: refused for that reason, not for the missing tier */
    CHECK(count(err, "a follower or a read-only open does not read erasure-coded parts") == 1);
    tsdb_options o;
    tsdb_options_init(&o);
    o.read_only = 1;
    o.follow = 1;
    CHECK(tsdb_open(dir, &o, &db) == TSDB_ERR_INVALID); /* a follower (D7) */
    tier_env(stores, 3, 2, 1);
    unsetenv("TSDB_TEST_TIER_I_KNOW");
    CHECK(open_db(dir, &db, 0) == TSDB_ERR_INVALID);
    tier_env(stores, 3, 2, 1);
    CHECK(tsdb_open(dir, &o, &db) == TSDB_ERR_INVALID); /* a tier on a follower */
    CHECK(open_db(dir, &db, 0) == 0);
    tsdb_close(db);

    /* a tier on a cluster volume (D1): refused with the tier, opened without it */
    char cv[PATH_MAX + 32], meta[PATH_MAX + 40];
    snprintf(cv, sizeof cv, "%s/refuse/cvol", S);
    no_tier_env();
    CHECK(open_db(cv, &db, 0) == 0);
    tsdb_close(db);
    snprintf(meta, sizeof meta, "%s/META", cv);
    append(meta, "requires=cluster\n");
    tsdb_options_init(&o);
    eng *e = NULL;
    meta_cluster_open_allow(true);
    CHECK(eng_open(cv, &o, &e) == 0);
    if (e)
        eng_close(e);
    tier_env(stores, 3, 2, 1);
    direct d;
    CHECK(direct_open(cv, 2, &d) == TSDB_ERR_INVALID);
    meta_cluster_open_allow(false);

    /* a running follower that meets an .ec name stops: its engine fails with one line */
    no_tier_env();
    char plain[PATH_MAX + 32];
    snprintf(plain, sizeof plain, "%s/refuse/plain", S);
    CHECK(open_db(plain, &db, 0) == 0);
    tsdb_close(db);
    tsdb_options_init(&o);
    o.read_only = 1;
    e = NULL;
    CHECK(eng_open(plain, &o, &e) == 0);
    if (e) {
        CHECK(eng_check(e) == 0);
        CHECK(tier_follower_ec(e, "p0_1_1_0.ec") != 0 && eng_check(e) != 0);
        eng_close(e);
    }
    printf("  refuse: no tier, read-only, follower, no I_KNOW, a cluster volume, a follower meeting .ec\n");
}

/* ----------------------------------------------------------------------------------------------------------- reval */

static void t_reval(void)
{
    eng *neu = calloc(1, sizeof *neu);
    part *pp[3];
    const char *names[3] = {"p5_1_4_1.ec", "p5_5_5_0.part", "p6_1_1_0.part"};
    const uint64_t lo[3] = {1, 5, 1}, hi[3] = {4, 5, 1};
    for (int i = 0; i < 3; i++) {
        pp[i] = calloc(1, sizeof *pp[i]);
        snprintf(pp[i]->name, sizeof pp[i]->name, "%s", names[i]);
        pp[i]->partition = i < 2 ? 5 : 6;
        pp[i]->min_block = lo[i], pp[i]->max_block = hi[i];
    }
    neu->parts = pp;
    neu->nparts = 3;
    reval_part x[4];
    memset(x, 0, sizeof x);
    const char *was[4] = {"p5_1_4_1.part", "p5_5_5_0.ec", "p6_1_1_0.part", "p6_2_2_0.ec"};
    const uint64_t wlo[4] = {1, 5, 1, 2};
    for (int i = 0; i < 4; i++) {
        snprintf(x[i].name, sizeof x[i].name, "%s", was[i]);
        x[i].partition = i < 2 ? 5 : 6;
        x[i].min_block = x[i].max_block = wlo[i];
        if (i == 0)
            x[i].max_block = 4;
        x[i].rows = 10;
    }
    uint64_t lost = 0, rows = 0;
    reval_parts(neu, x, 4, &lost, &rows); /* only p6_2_2_0.ec is gone */
    CHECK(lost == 1 && rows == 10);
    /* by name alone too: an .ec and its .part are one part even where the block test would not say so */
    pp[0]->partition = 9;
    lost = rows = 0;
    reval_parts(neu, x, 1, &lost, &rows);
    CHECK(lost == 0);
    snprintf(pp[0]->name, sizeof pp[0]->name, "settled_p5_1_4_1.part");
    lost = rows = 0;
    reval_parts(neu, x, 1, &lost, &rows);
    CHECK(lost == 1);
    for (int i = 0; i < 3; i++)
        free(pp[i]);
    free(neu);
    printf("  reval: .ec and .part are one part; settled_ is none\n");
}

/* ----------------------------------------------------------------------------------------------- power, eio: child */

static int child(int argc, char **argv)
{
    /* --child MODE DIR STORES ACK */
    if (argc < 6)
        return 2;
    const char *mode = argv[2], *dir = argv[3], *stores = argv[4], *ack = argv[5];
    tier_env(stores, 3, 2, 1);
    if (strcmp(mode, "power") == 0) {
        tsdb *db;
        if (open_db(dir, &db, 0) != 0)
            _exit(3);
        int fd = open(ack, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0 || fsync(fd) != 0)
            _exit(4);
        close(fd);
        tsdb_close(db);
        _exit(0);
    }
    /* eio: the tier opens, puts an object; whatever fails, an object put before stays readable */
    direct d;
    int rc = direct_open(dir, 0, &d);
    if (rc) {
        fprintf(stderr, "test_tier: the open under eio failed: %s\n", tsdb_strerror(rc));
        _exit(5);
    }
    uint8_t oid[16];
    memcpy(oid, OID, 16);
    oid[0] = 0x55;
    int put = put_obj(&d, oid);
    bool w = tier_writable(d.t);
    int get = get_obj(&d, OID);
    fprintf(stderr, "test_tier: put %s, read %s\n", tsdb_strerror(put), tsdb_strerror(get));
    char line[64];
    int n = snprintf(line, sizeof line, "%d %d %d\n", put == 0, w, get == 0);
    int fd = open(ack, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0 || write(fd, line, (size_t)n) != n)
        _exit(6);
    close(fd);
    _exit(0);
}

static int run_child(const char *mode, const char *dir, const char *stores, const char *ack, const char *root, int w,
                     const char *mark, const char *errf)
{
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512], ws[16];
        snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0:detect_leaks=0",
                 getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
        setenv("ASAN_OPTIONS", asan, 1);
        snprintf(ws, sizeof ws, "%d", w);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", root, 1);
        setenv("CRASHFS_PROB", "1", 1);
        setenv("CRASHFS_WARMUP", ws, 1);
        setenv("CRASHFS_AT_EXIT", "1", 1);
        setenv("CRASHFS_TEAR", "1", 1);
        setenv("CRASHFS_SEED", ws, 1);
        if (strcmp(mode, "eio") == 0) {
            setenv("CRASHFS_MODE", "eio", 1);
            setenv("CRASHFS_MARK", mark, 1);
        }
        int null = open("/dev/null", O_WRONLY);
        int ef = open(errf, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (null >= 0)
            dup2(null, 1);
        if (ef >= 0)
            dup2(ef, 2);
        execl(SELF, "test_tier", "--child", mode, dir, stores, ack, (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

/* ----------------------------------------------------------------------------------------------------------- power */

static void t_power(bool stores_too)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  power: skipped (no %s)\n", SHIM);
        return;
    }
    char base[PATH_MAX + 32], dir[PATH_MAX + 32], stores[PATH_MAX + 32], ack[PATH_MAX + 32], errf[PATH_MAX + 32];
    snprintf(base, sizeof base, "%s/power", S);
    snprintf(dir, sizeof dir, "%s/power/db", S);
    snprintf(stores, sizeof stores, "%s/power/stores", S);
    snprintf(ack, sizeof ack, "%s/power-ack", S); /* outside the crashfs root: an ack whose name a cut took is lost */
    snprintf(errf, sizeof errf, "%s/power-err", S);
    int w, cuts = 0, f0 = fails;
    for (w = 0; w < 2000; w++) {
        fresh(base);
        unlink(ack);
        mkdir(stores, 0755);
        tsdb *db;
        no_tier_env();
        CHECK(open_db(dir, &db, 0) == 0); /* the database exists: the cut falls into the tier's first open */
        tsdb_close(db);
        int rc = run_child("power", dir, stores, ack, stores_too ? base : dir, w, "", errf);
        bool acked = access(ack, F_OK) == 0;
        char u[33], u2[33];
        bool req = meta_tier(dir, u);
        CHECK(req == (u[0] != '\0')); /* both lines or neither */
        CHECK(!acked || (req && strlen(u) == 32));
        tier_env(stores, 3, 2, 1);
        CHECK(open_db(dir, &db, 0) == 0);
        if (db)
            tsdb_close(db);
        CHECK(meta_tier(dir, u2) && strlen(u2) == 32 && (!req || strcmp(u, u2) == 0));
        if (stores_too) { /* the stores a cut left half made are finished, never DOWN: the root's word came after them */
            direct d;
            CHECK(direct_open(dir, 0, &d) == 0);
            for (int i = 0; d.t && i < 3; i++)
                CHECK(chunk_be_store_state(tier_backend(d.t), i) == CHUNK_STORE_UP);
            CHECK(d.t && tier_writable(d.t) && put_obj(&d, OID) == 0 && get_obj(&d, OID) == 0);
            if (d.t)
                direct_close(&d);
        }
        if (fails > f0) {
            fprintf(stderr, "  power: at step %d (child exit %d)\n", w, rc);
            return;
        }
        if (rc == 0)
            break;
        cuts += rc == 99;
    }
    printf("  power: a power cut at each of %d steps of the first open with a tier%s: META whole, the uuid kept%s\n",
           cuts, stores_too ? ", the stores too" : "", stores_too ? ", every store UP after" : "");
}

/* ------------------------------------------------------------------------------------------------------------- eio */

static void t_eio(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  eio: skipped (no %s)\n", SHIM);
        return;
    }
    char base[PATH_MAX + 32], dir[PATH_MAX + 32], stores[PATH_MAX + 32], st0[PATH_MAX + 32], ack[PATH_MAX + 32],
        mark[PATH_MAX + 32], errf[PATH_MAX + 32];
    snprintf(base, sizeof base, "%s/eio", S);
    snprintf(dir, sizeof dir, "%s/eio/db", S);
    snprintf(stores, sizeof stores, "%s/eio/stores", S);
    snprintf(st0, sizeof st0, "%s/eio/stores/st0", S);
    snprintf(ack, sizeof ack, "%s/eio/ack", S);
    snprintf(mark, sizeof mark, "%s/eio/mark", S);
    snprintf(errf, sizeof errf, "%s/eio/err", S);
    int faults = 0, halts = 0, w, f0 = fails;
    for (w = 0; w < 200; w++) {
        fresh(base);
        mkdir(stores, 0755);
        tier_env(stores, 3, 2, 1);
        direct d;
        CHECK(direct_open(dir, 0, &d) == 0);
        if (!d.db)
            return;
        CHECK(put_obj(&d, OID) == 0);
        direct_close(&d);
        CHECK(direct_open(dir, 0, &d) == 0);
        CHECK(d.db && get_obj(&d, OID) == 0); /* a new handle reads it without crashfs */
        direct_close(&d);
        unlink(ack);
        unlink(mark);
        int rc = run_child("eio", dir, stores, ack, st0, w, mark, errf);
        int put = -1, wr = -1, get = -1;
        CHECK(rc == 0 && sscanf(slurp(ack), "%d %d %d", &put, &wr, &get) == 3);
        bool fault = access(mark, F_OK) == 0;
        /* an EIO on a tmp_ chunk fails just its put; one on a name's directory fsync or on EPOCH fails the store */
        bool failed = strstr(slurp(errf), "acknowledges nothing more") != NULL;
        CHECK(!failed || fault);
        CHECK(get == 1);                     /* reads go on */
        CHECK(failed ? wr == 0 : wr == 1);   /* the tier halts on a store failed in service, and only then */
        CHECK(!failed || strstr(slurp(errf), "settles and chunk deletes stop") != NULL);
        CHECK(fault || put == 1);
        if (fails > f0) {
            fprintf(stderr, "  eio: at step %d (exit %d, fault %d, put %d, writable %d, read %d)\n", w, rc, fault, put, wr,
                    get);
            return;
        }
        if (!fault)
            break;
        faults++;
        halts += failed;
    }
    CHECK(halts > 0);
    printf("  eio: an I/O error at each of %d steps of store 0 (%d failed the store): the tier halts with one line "
           "then, reads go on\n", faults, halts);
}
#endif

/* ---------------------------------------------------------------------------------------------------------- follow */

static int count_fn(void *ctx, const int64_t *ts, const void *v, size_t n)
{
    (void)ts;
    (void)v;
    *(size_t *)ctx += n;
    return 0;
}

/* A running follower meets an .ec name in its writer's MANIFEST (the writer lists its part under that name for one
 * MANIFEST, beside a hard link of that name, as test_ecnames does): the refresh fails with the engine's one line and no
 * "keeping the previous view", the engine is failed (tsdb_failed), and the view is the one before. */
static void t_follow(void)
{
    char dir[PATH_MAX + 32];
    snprintf(dir, sizeof dir, "%s/follow", S);
    fresh(dir);
    no_tier_env();
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    eng *db = NULL, *f = NULL;
    CHECK(eng_open(dir, &o, &db) == 0);
    if (!db)
        return;
    uint32_t s = 0;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    int64_t ts[100];
    float v[100];
    for (int i = 0; i < 100; i++)
        ts[i] = 1000 * (int64_t)i, v[i] = (float)i;
    CHECK(eng_insert_series(db, TSDB_FLOAT32, s, ts, v, 100) == 0);
    CHECK(eng_flush(db) == 0);
    tsdb_options fo;
    tsdb_options_init(&fo);
    fo.follow = 1;
    fo.refresh_ms = 1000000; /* refreshed by hand */
    fo.create_volumes = 0;
    CHECK(eng_open(dir, &fo, &f) == 0);
    if (f) {
        size_t n0 = 0, n1 = 0;
        CHECK(eng_query_raw_ex(f, s, INT64_MIN, INT64_MAX, false, 0, count_fn, &n0) == 0 && n0 == 100);
        pthread_mutex_lock(&db->lock);
        part *p = db->nparts == 1 ? db->parts[0] : NULL;
        char real[96] = "", ecname[96] = "";
        if (p) {
            snprintf(real, sizeof real, "%s", p->name);
            snprintf(ecname, sizeof ecname, "%.*s.ec", (int)(strlen(real) - 5), real);
            snprintf(p->name, sizeof p->name, "%s", ecname);
        }
        pthread_mutex_unlock(&db->lock);
        CHECK(p != NULL);
        if (p) {
            CHECK(linkat(db->dirfd, real, db->dirfd, ecname, 0) == 0);
            CHECK(manifest_publish(db) == 0);
            pthread_mutex_lock(&db->lock);
            snprintf(p->name, sizeof p->name, "%s", real);
            pthread_mutex_unlock(&db->lock);
            CHECK(eng_check(f) == 0);
            cap_begin();
            int r1 = eng_refresh(f), r2 = eng_refresh(f);
            const char *err = cap_end();
            CHECK(r1 != 0 && r2 != 0);
            CHECK(eng_check(f) != 0); /* the engine failed: tsdb_failed, and tsdbd exits */
            CHECK(count(err, ecname) == 1 && count(err, "keeping the previous view") == 0);
            CHECK(eng_query_raw_ex(f, s, INT64_MIN, INT64_MAX, false, 0, count_fn, &n1) == 0 && n1 == n0);
            CHECK(manifest_publish(db) == 0);
            CHECK(unlinkat(db->dirfd, ecname, 0) == 0);
        }
        eng_close(f);
    }
    CHECK(eng_close(db) == 0);
    printf("  follow: a follower meeting an .ec name in MANIFEST stops with one line, its engine failed\n");
}

int main(int argc, char **argv)
{
    for (size_t i = 0; i < sizeof OBJ; i++) /* before the child too: it compares what it reads with OBJ */
        OBJ[i] = (uint8_t)(i * 131 + 7);
#ifdef TSDB_EC
    if (argc > 1 && strcmp(argv[1], "--child") == 0)
        return child(argc, argv);
#endif
    const char *mutant = NULL;
    int ai = 1;
    if (argc > 2 && strcmp(argv[1], "--mutant") == 0) {
        mutant = argv[2];
        ai = 3;
    }
    if (mutant) {
        if (strcmp(mutant, "tierperengine") == 0)
            tier_mutant = TIER_MUT_PERENGINE;
        else if (strcmp(mutant, "tierunsure") == 0)
            tier_mutant = TIER_MUT_UNSURE;
        else if (strcmp(mutant, "tiernoec") == 0)
            tier_mutant = TIER_MUT_NOEC;
        else if (strcmp(mutant, "tierstrict") == 0)
            tier_mutant = TIER_MUT_STRICT;
        else if (strcmp(mutant, "tierfresh") == 0)
            tier_mutant = TIER_MUT_FRESH;
        else if (strcmp(mutant, "tierfence") == 0)
            tier_mutant = TIER_MUT_FENCE;
        else {
            fprintf(stderr, "unknown mutant %s\n", mutant);
            return 2;
        }
    }
    const char *scratch = argc > ai ? argv[ai] : "/tmp";
    snprintf(S, sizeof S, "%s/test_tier.%d", scratch, (int)getpid());
    if (!realpath(argv[0], SELF))
        snprintf(SELF, sizeof SELF, "%s", argv[0]);
    char *slash = strrchr(SELF, '/');
    snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", slash ? (int)(slash - SELF) : 1, slash ? SELF : ".");
    fresh(S);
    printf("test_tier%s%s\n", mutant ? " --mutant " : "", mutant ? mutant : "");
    t_noec();
#ifdef TSDB_EC
    t_volumes();
    t_down();
    t_lost();
    t_fence();
    t_unsure();
    t_refuse();
    t_reval();
    t_power(false);
    t_power(true);
    t_eio();
#else
    printf("  volumes, down, lost, fence, unsure, refuse, reval, power, eio: skipped (no TSDB_EC in this build)\n");
#endif
    t_follow();
    if (!fails)
        sh("rm -rf '%s'", S);
    printf("%s\n", fails ? "test_tier: FAILED" : "test_tier: ok");
    return fails ? 1 : 0;
}
