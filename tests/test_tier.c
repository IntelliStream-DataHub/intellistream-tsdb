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
 *            (r8b) and the attach's epoch is its provider's, never the stores' own fence: an unsure store is raised
 *            to it, puts and deletes carry it, a provider with none fails the attach
 *   unsure   a damaged EPOCH slot (a torn store looks the same) and then an open: puts work
 *   ops      (r8b) two volumes attached with different cold ops: each volume's puts and reads reach its own stores
 *   refuse   requires=tier without a tier, without TSDB_TEST_TIER_I_KNOW=1, read-only and following; a tier on a
 *            cluster volume; a running follower that meets an .ec name stops
 *   follow   a follower whose writer's MANIFEST names an .ec part, through eng_refresh: one line, the engine failed
 *   reval    revalidation counts an .ec part and its .part as one, settled_ and retired_ files as none
 *   power    power loss (crashfs) at every step of the first open with a tier: META has both lines or neither, and the
 *            uuid never changes once an open with the tier returned; then with the stores under crashfs too: every
 *            store is UP after the next open (made, or found)
 *   eio      a store fails in service (crashfs, EIO on its files): the tier halts with one line, reads go on
 *
 * Mutants (--mutant NAME, src/tier.h): tierperengine, tierunsure, tierstrict, tierfresh, tierfence, selfepoch (each must fail
 * here: selfepoch in fence), tiernoec (must fail in a NO_EC build). Usage: test_tier [--mutant NAME] [SCRATCH]
 */
#define _GNU_SOURCE
#include "chunk.h"
#include "chunk_priv.h"
#include "coldops.h"
#include "eccache.h"
#include "ecgc.h"
#include "ecpart.h"
#include "feed.h"
#include "outagefs_ctl.h"
#include "settle.h"
#include "tier.h"
#include "tierfill.h"
#include "tiers.h"
#include "tsdb.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#ifdef __linux__
#include <sys/syscall.h>
#endif
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

static char S[PATH_MAX], SELF[PATH_MAX], SHIM[PATH_MAX + 32], SHIM_OUTAGE[PATH_MAX + 32];
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
#ifndef TSDB_EC
    /* A database whose TIERS lists stores must also be refused on a NO_EC=1 build naming requires=tier and the codec. */
    char tdir[PATH_MAX + 32];
    snprintf(tdir, sizeof tdir, "%s/noec-tiers", S);
    fresh(tdir);
    CHECK(open_db(tdir, &db, 0) == 0);
    tsdb_close(db);
    int dfd = open(tdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    CHECK(dfd >= 0);
    if (dfd >= 0) {
        uint8_t dbid[16], sid[16];
        memset(sid, 0x33, sizeof sid);
        CHECK(tiers_db_id_ensure(dfd, true, dbid) == 0);
        CHECK(tiers_append_store(dfd, true, sid, TSDB_TIER_CLASS_HDD, false, "/noec-dummy-store") == 0);
        close(dfd);
    }
    cap_begin();
    int rct = open_db(tdir, &db, 0);
    const char *terr = cap_end();
    CHECK(rct == TSDB_ERR_INVALID && !db);
    CHECK(strstr(terr, "requires=tier") != NULL && strstr(terr, "not built with the erasure codec") != NULL);
#endif
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

static void t_fence_provider(void);

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
    t_fence_provider();
}


/* ------------------------------------------------------------------------------------------------- fence: provider */

static uint64_t prov_epoch(void *arg, uint32_t volume)
{
    (void)volume;
    return *(const uint64_t *)arg;
}

static int direct_open_with(const char *dir, uint32_t vol, const tier_attach *a, direct *d)
{
    tsdb_options o;
    tsdb_options_init(&o);
    d->t = NULL;
    d->db = NULL;
    int rc = tier_open(dir, &o, &d->t);
    if (rc)
        return rc;
    tier_want_attach(d->t, vol, a);
    rc = eng_open(dir, &o, &d->db);
    tier_want(NULL, 0);
    if (rc) {
        tier_put(d->t);
        d->t = NULL;
        d->db = NULL;
    }
    return rc;
}

typedef struct {
    const uint8_t *b;
} msrc;

static int msrc_fill(void *ctx, uint64_t s, uint64_t off, uint8_t *buf, size_t len)
{
    (void)s;
    memcpy(buf, ((msrc *)ctx)->b + off, len);
    return 0;
}

/* A put of OBJ through the volume's ops at its provider's epoch (what a settle does). */
static int ops_put(coldvol *cv, const uint8_t oid[16])
{
    chunk_geom g;
    int rc = chunk_geom_make(&g, 2, 1, 512, sizeof OBJ);
    uint64_t *sums = malloc(chunk_sums_total(&g) * sizeof *sums);
    msrc m = {OBJ};
    chunk_put_src src = {.ctx = &m, .fill = msrc_fill};
    if (!rc && sums)
        rc = cv->ops->put_stream(cv->ctx, oid, &g, coldvol_epoch(cv), &src, sums);
    free(sums);
    return sums ? rc : TSDB_ERR_NOMEM;
}

static int ops_get(coldvol *cv, const uint8_t oid[16])
{
    cold_obj *ob = NULL;
    int rc = cv->ops->obj_open(cv->ctx, oid, NULL, NULL, &ob);
    if (rc)
        return rc;
    uint8_t back[sizeof OBJ];
    chunk_report rep;
    memset(&rep, 0, sizeof rep);
    rc = cv->ops->obj_read(ob, 0, sizeof back, back, 0, &rep);
    cv->ops->obj_close(ob);
    return rc ? rc : memcmp(back, OBJ, sizeof OBJ) == 0 ? 0 : TSDB_ERR_CORRUPT;
}

/* The epoch of an attach is its provider's, never the stores' own fence (what a writer last saw): a volume whose EPOCH
 * lost a slot in one store is raised to the provider's epoch 3 in every store, and puts and deletes carry it. The
 * selfepoch mutant gives the stores' fence (1) as the authoritative epoch: the unsure store is cleared at 1. */
static void t_fence_provider(void)
{
    char dir[PATH_MAX + 32], stores[PATH_MAX + 32], p[PATH_MAX + 64];
    snprintf(p, sizeof p, "%s/fenceprov", S);
    fresh(p);
    snprintf(dir, sizeof dir, "%s/fenceprov/db", S);
    snprintf(stores, sizeof stores, "%s/fenceprov/stores", S);
    mkdir(stores, 0755);
    tier_env(stores, 3, 2, 1);
    direct d;
    CHECK(direct_open(dir, 0, &d) == 0); /* the default provider: epoch 1 */
    if (!d.db)
        return;
    CHECK(tier_epoch(d.t, 0) == 1 && put_obj(&d, OID) == 0);
    direct_close(&d);
    snprintf(p, sizeof p, "%s/st0/s0/EPOCH", stores); /* one slot damaged: store 0's volume opens unsure */
    int fd = open(p, O_WRONLY);
    CHECK(fd >= 0 && pwrite(fd, "garbage-garbage-garbage-garbage!", 32, 4096) == 32);
    if (fd >= 0)
        close(fd);
    uint64_t zero = 0, three = 3;
    tier_attach a = {.epoch = prov_epoch, .epoch_arg = &zero};
    cap_begin();
    int rc = direct_open_with(dir, 0, &a, &d);
    const char *err = cap_end();
    CHECK(rc == TSDB_ERR_UNAVAILABLE && count(err, "is not known") == 1); /* no epoch now: no attach */
    if (d.db)
        direct_close(&d);
    a.epoch_arg = &three;
    CHECK(direct_open_with(dir, 0, &a, &d) == 0);
    if (!d.db)
        return;
    CHECK(tier_epoch(d.t, 0) == 3);
    for (int i = 0; i < 3; i++) {
        chunk_store *st = chunk_be_store(tier_backend(d.t), i);
        CHECK(st && chunk_store_epoch(st, 0) == 3 && !chunk_store_epoch_unsure(st, 0)); /* selfepoch: 1 */
    }
    coldvol *cv = tier_cold(d.db);
    uint8_t oid[16];
    memcpy(oid, OID, 16);
    oid[0] = 0x31;
    CHECK(cv && ops_put(cv, oid) == 0 && ops_get(cv, oid) == 0);
    chunk_ident id = {.volume = 0, .pos = 0};
    memcpy(id.vuuid, tier_vuuid(d.db), 16);
    memcpy(id.oid, oid, 16);
    chunk_rd *r = NULL;
    CHECK(chunk_be_rd_open(tier_backend(d.t), &id, &r) == 0 && r && chunk_rd_hdr(r)->epoch == 3);
    if (r)
        chunk_rd_close(r);
    CHECK(cv && cv->ops->del(cv->ctx, oid, 0, 1, COLD_DEL_GC) != 0); /* below the fence */
    CHECK(cv && cv->ops->del(cv->ctx, oid, 0, coldvol_epoch(cv), COLD_DEL_GC) == 0);
    direct_close(&d);
    printf("  fence: the attach's epoch is its provider's (3), never the stores' own fence: an unsure store is raised "
           "to it, puts and deletes carry it; a provider with none fails the attach\n");
}

/* ---------------------------------------------------------------------------------------------------------------- ops */

static int count_files(const char *path)
{
    DIR *dd = opendir(path);
    if (!dd)
        return 0;
    int n = 0;
    struct dirent *e;
    while ((e = readdir(dd)) != NULL)
        n += e->d_name[0] != '.' && strcmp(e->d_name, "EPOCH") != 0;
    closedir(dd);
    return n;
}

/* Two volumes of one tier attached with different ops: the root with the directory ops over the tier's stores, volume
 * 1 with the directory ops over a backend of its own (as r8f gives a cluster volume ops over its stripe group). Each
 * volume's puts reach its own stores, the cache's lookup by volume (tier_cold_get) finds each volume's ops, and the
 * contexts are freed with the last reference (ASan). */
static void t_ops(void)
{
    char dir[PATH_MAX + 32], vol[PATH_MAX + 32], stores[PATH_MAX + 32], other[PATH_MAX + 32], p[PATH_MAX + 64];
    snprintf(p, sizeof p, "%s/ops", S);
    fresh(p);
    snprintf(dir, sizeof dir, "%s/ops/db", S);
    snprintf(vol, sizeof vol, "%s/ops/v1", S);
    snprintf(stores, sizeof stores, "%s/ops/stores", S);
    snprintf(other, sizeof other, "%s/ops/other", S);
    mkdir(stores, 0755);
    mkdir(other, 0755);
    tier_env(stores, 3, 2, 1);
    char op[3][PATH_MAX + 64];
    const char *opp[3];
    for (int i = 0; i < 3; i++) {
        snprintf(op[i], sizeof op[i], "%s/o%d", other, i);
        uint8_t id[16];
        CHECK(chunk_store_init(op[i], NULL, id) == 0);
        opp[i] = op[i];
    }
    chunk_backend *be2 = NULL;
    chunk_be_opts bo = {.min_up = 3};
    CHECK(chunk_be_open2(&be2, opp, 3, NULL, &bo) == 0);
    direct d;
    CHECK(direct_open(dir, 0, &d) == 0);
    if (!d.db || !be2) {
        if (d.db)
            direct_close(&d);
        if (be2)
            chunk_be_close(be2);
        return;
    }
    tsdb_options o;
    tsdb_options_init(&o);
    eng *e = NULL;
    tier_attach a = {.ops = &coldops_dir, .make = coldops_dir_make, .make_arg = be2};
    tier_want_attach(d.t, 1, &a);
    int rc = eng_open(vol, &o, &e);
    tier_want(NULL, 0);
    CHECK(rc == 0 && e);
    if (e) {
        uint8_t o0[16], o1[16];
        memcpy(o0, OID, 16);
        memcpy(o1, OID, 16);
        o0[0] = 0xa0, o1[0] = 0xa1;
        CHECK(ops_put(tier_cold(d.db), o0) == 0 && ops_put(tier_cold(e), o1) == 0);
        int in0 = 0, in1 = 0, cross = 0;
        for (int i = 0; i < 3; i++) {
            snprintf(p, sizeof p, "%s/st%d/s0", stores, i);
            in0 += count_files(p);
            snprintf(p, sizeof p, "%s/st%d/s1", stores, i);
            cross += count_files(p);
            snprintf(p, sizeof p, "%s/s1", op[i]);
            in1 += count_files(p);
            snprintf(p, sizeof p, "%s/s0", op[i]);
            cross += count_files(p);
        }
        CHECK(in0 == 3 && in1 == 3 && cross == 0); /* k' + m = 3 chunks each, each in its own volume's stores */
        coldvol *c0 = tier_cold_get(d.t, 0), *c1 = tier_cold_get(d.t, 1);
        CHECK(c0 == tier_cold(d.db) && c1 == tier_cold(e));
        CHECK(c0 && c1 && ops_get(c0, o0) == 0 && ops_get(c1, o1) == 0);
        CHECK(c0 && c1 && ops_get(c0, o1) != 0 && ops_get(c1, o0) != 0);
        uint8_t u[16];
        eccache_vuuid(d.t, 1, u);
        CHECK(memcmp(u, tier_vuuid(e), 16) == 0 && memcmp(tier_vuuid(e), tier_vuuid(d.db), 16) != 0);
        tier_cold_put(c0);
        tier_cold_put(c1);
        eng_close(e);
        c1 = tier_cold_get(d.t, 1); /* the registry keeps a closed volume's ops for the deletes queued by it */
        CHECK(c1 && ops_get(c1, o1) == 0);
        tier_cold_put(c1);
    }
    direct_close(&d);
    chunk_be_close(be2);
    printf("  ops: two volumes with different ops: each volume's puts and reads reach its own stores\n");
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
    CHECK(rro == TSDB_ERR_INVALID); /* read-only without TIERS: refused naming requires=tier and no tier */
    CHECK(count(err, "no tier is configured") == 1);
    tsdb_options o;
    tsdb_options_init(&o);
    o.read_only = 1;
    o.follow = 1;
    CHECK(tsdb_open(dir, &o, &db) == TSDB_ERR_INVALID); /* follower without TIERS */
    tier_env(stores, 3, 2, 1);
    unsetenv("TSDB_TEST_TIER_I_KNOW");
    CHECK(open_db(dir, &db, 0) == TSDB_ERR_INVALID);
    tier_env(stores, 3, 2, 1);
    CHECK(tsdb_open(dir, &o, &db) == TSDB_ERR_INVALID); /* TSDB_TEST_TIER without TIERS on a follower */
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

#ifdef TSDB_EC
static int64_t rh_now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void rh_sleep_ms(int ms)
{
    struct timespec d = {ms / 1000, (long)(ms % 1000) * 1000000L};
    nanosleep(&d, NULL);
}

static int rh_unlink_unshimmed(const char *path)
{
#ifdef __linux__
    return (int)syscall(SYS_unlinkat, AT_FDCWD, path, 0);
#else
    return unlink(path);
#endif
}

typedef struct {
    size_t n;
    double sum;
} rh_sum;

static int rh_sum_cb(void *ctx, const int64_t *ts, const void *v, size_t n)
{
    (void)ts;
    rh_sum *s = ctx;
    const float *fv = v;
    for (size_t i = 0; i < n; i++)
        s->sum += fv[i];
    s->n += n;
    return 0;
}

static int rh_find_ec_oid(eng *db, int64_t part_key, uint8_t oid_out[16])
{
    int found = 0;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++) {
        part *p = db->parts[i];
        if (p->partition != part_key)
            continue;
        struct ecpart *ep = atomic_load_explicit(&p->ec, memory_order_acquire);
        if (ep && !ep->test_mark) {
            memcpy(oid_out, ep->rec.oid, 16);
            found = 1;
            break;
        }
    }
    pthread_mutex_unlock(&db->lock);
    return found;
}

static int rh_settle_part(eng *db, int64_t part_key)
{
    for (int attempt = 0; attempt < 20; attempt++) {
        part *sel[16];
        size_t n = 0;
        bool has_ec = false;
        pthread_mutex_lock(&db->lock);
        for (size_t i = 0; i < db->nparts && n < 16; i++) {
            part *p = db->parts[i];
            if (p->partition != part_key)
                continue;
            if (atomic_load_explicit(&p->ec, memory_order_acquire)) {
                has_ec = true;
            } else {
                part_ref(p);
                sel[n++] = p;
            }
        }
        pthread_mutex_unlock(&db->lock);
        if (!n)
            return has_ec ? 0 : TSDB_ERR_NOTFOUND;
        size_t settled = 0;
        int rc = eng_settle(db, part_key, sel, n, &settled);
        for (size_t i = 0; i < n; i++)
            part_unref(sel[i]);
        if (rc == 0 && settled == n)
            return 0;
        if (rc == 0 || rc == TSDB_ERR_NOTFOUND) {
            rh_sleep_ms(30);
            continue;
        }
        return rc;
    }
    uint8_t tmp[16];
    return rh_find_ec_oid(db, part_key, tmp) ? 0 : TSDB_ERR_IO;
}

static int rh_of_cmd(const char *ctl, const char *verb, const char *dir)
{
    char line[PATH_MAX + 64], out[256];
    snprintf(line, sizeof line, "%s %s", verb, dir);
    return outagefs_cmd(ctl, line, out, sizeof out);
}

typedef struct {
    eng *db;
    uint32_t sid;
    int64_t t0, t1;
    atomic_bool done;
    int rc;
    rh_sum got;
} rh_qarg;

static void *rh_query_thread(void *arg)
{
    rh_qarg *q = arg;
    q->got = (rh_sum){0, 0};
    q->rc = eng_query_raw_ex(q->db, q->sid, q->t0, q->t1, false, 0, rh_sum_cb, &q->got);
    atomic_store_explicit(&q->done, true, memory_order_release);
    return NULL;
}

typedef struct {
    direct *d;
    uint32_t sid;
    atomic_bool stop;
    atomic_int errs;
    atomic_int reads;
} rh_stress_arg;

static void *rh_stress_reader(void *arg)
{
    rh_stress_arg *a = arg;
    while (!atomic_load_explicit(&a->stop, memory_order_acquire)) {
        eccache_test_drop_entries(a->d->t);
        rh_sum got = {0, 0};
        int rc = eng_query_raw_ex(a->d->db, a->sid, 0, 1000, false, 0, rh_sum_cb, &got);
        if (rc != 0 || got.n != 200 || (int64_t)(got.sum + 0.5) != 19900)
            atomic_fetch_add(&a->errs, 1);
        atomic_fetch_add(&a->reads, 1);
    }
    return NULL;
}

/* Regression helper for A4: opens/closes a second volume (v1) concurrently with store attach+sweep. */
typedef struct {
    tier *t;
    const char *dir;
    atomic_bool stop;
    atomic_int errs;
    atomic_int cycles;
} rh_openclose_arg;

static void *rh_openclose_thread(void *arg)
{
    rh_openclose_arg *a = arg;
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.partition_ms = 1000;
    o.partition_classes = 0;
    o.create_volumes = 0;
    while (!atomic_load_explicit(&a->stop, memory_order_acquire)) {
        eng *db = NULL;
        tier_want(a->t, 1);
        int rc = eng_open(a->dir, &o, &db);
        tier_want(NULL, 0);
        if (rc != 0 || !db) {
            atomic_fetch_add(&a->errs, 1);
            break;
        }
        uint32_t sid = 0;
        if (eng_series_lookup(db, "v1.s", &sid) == 0) {
            rh_sum got = {0, 0};
            if (eng_query_raw_ex(db, sid, 0, 1000, false, 0, rh_sum_cb, &got) != 0 || got.n != 100)
                atomic_fetch_add(&a->errs, 1);
        }
        eng_close(db);
        atomic_fetch_add(&a->cycles, 1);
    }
    return NULL;
}

/* Regression helper for A5: pauses an in-flight eng_settle inside settle_fill_hook while attach+sweep tries to run. */
static atomic_bool rh_hook_entered;
static atomic_bool rh_hook_release;

static void rh_settle_pause_hook(eng *db, const part *p, uint64_t stripe)
{
    (void)db;
    (void)p;
    if (stripe != 0)
        return;
    atomic_store_explicit(&rh_hook_entered, true, memory_order_release);
    while (!atomic_load_explicit(&rh_hook_release, memory_order_acquire))
        rh_sleep_ms(10);
}

typedef struct {
    eng *db;
    int64_t pk;
    int rc;
} rh_settle_arg;

static void *rh_settle_thread(void *arg)
{
    rh_settle_arg *a = arg;
    a->rc = rh_settle_part(a->db, a->pk);
    return NULL;
}

/* Child for crashfs power-loss test across store attach + missed-delete sweep. */
static int child_health_power(const char *dbdir, int victim)
{
    atomic_store(&ecgc_test_nosweep, false);
    tier_health_test_opts(0, -1, 0, 0); /* probes off; drive attach deterministically via domain_state */
    no_tier_env();
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.partition_ms = 1000;
    o.partition_classes = 0;
    o.create_volumes = 0;
    direct d;
    if (tier_open(dbdir, &o, &d.t) != 0 || !d.t)
        return 3;
    tier_want(d.t, 0);
    int rc = eng_open(dbdir, &o, &d.db);
    tier_want(NULL, 0);
    if (rc != 0 || !d.db) {
        tier_put(d.t);
        return 4;
    }
    chunk_backend *be = tier_backend(d.t);
    tier_health_domain_state(d.t, victim, true);
    tier_health_domain_state(d.t, victim, false);
    int64_t dl = rh_now_ms() + 10000;
    while ((chunk_be_store_state(be, victim) != CHUNK_STORE_UP || !tier_writable(d.t)) && rh_now_ms() < dl)
        rh_sleep_ms(20);
    bool ok = chunk_be_store_state(be, victim) == CHUNK_STORE_UP && tier_writable(d.t);
    tier_quiesce(d.t);
    direct_close(&d);
    tier_health_test_opts(0, 0, 0, 0);
    return ok ? 0 : 5;
}

/* Child process under liboutagefs.so: tests runtime EIO during queries, stall detection within tier_stall_s + 1 s,
 * reads decoding around DOWN stores, attach healing missing chunks (kills attachnoheal), missed-delete sweep, and
 * settles pausing while DOWN and resuming after recovery. */
static int child_health(int argc, char **argv)
{
    /* --child health BASE STORES_BASE MUTANT */
    if (argc < 6)
        return 2;
    const char *base = argv[3], *stbase = argv[4], *mut = argv[5];
    if (strcmp(mut, "downnoread") == 0) {
        tier_mutant = TIER_MUT_DOWNNOREAD;
        chunk_mutant = CHUNK_MUT_DOWNNOREAD;
    } else if (strcmp(mut, "attachnoheal") == 0) {
        tier_mutant = TIER_MUT_ATTACHNOHEAL;
        chunk_mutant = CHUNK_MUT_ATTACHNOHEAL;
    }
    const char *ctl = getenv("OUTAGEFS_CTL");
    if (!ctl || !*ctl)
        return 3;

    /* Enable real orphan sweep for missed-delete verification. */
    atomic_store(&ecgc_test_nosweep, false);
    atomic_store(&settle_poll_ms, 100);
    tier_health_test_opts(1500, 150, 3, 500);

    char dbdir[PATH_MAX + 64], st[3][PATH_MAX + 64];
    snprintf(dbdir, sizeof dbdir, "%s/db", base);
    for (int i = 0; i < 3; i++)
        snprintf(st[i], sizeof st[i], "%s/st%d", stbase, i);

    no_tier_env();
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.partition_ms = 1000;
    o.partition_classes = 0;
    o.obsolete_part_lifetime_s = 0;
    o.create_volumes = 0;
    tsdb *tdb = NULL;
    if (tsdb_open(dbdir, &o, &tdb) != 0)
        return 4;
    tsdb_close(tdb);
    for (int i = 0; i < 3; i++)
        if (tsdb_tier_add_dir(dbdir, st[i], "hdd", TSDB_TIER_ALLOW_SHARED_DEV) != 0)
            return 5;

    setenv("TSDB_TEST_TIER", "grace_s=0,keep_s=0", 1);
    setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);

    direct d;
    if (tier_open(dbdir, &o, &d.t) != 0 || !d.t)
        return 6;
    tier_want(d.t, 0);
    int rc = eng_open(dbdir, &o, &d.db);
    tier_want(NULL, 0);
    if (rc != 0 || !d.db) {
        tier_put(d.t);
        return 7;
    }
    chunk_backend *be = tier_backend(d.t);
    uint32_t sid = 0;
    if (eng_series(d.db, "rh.s", TSDB_FLOAT32, &sid) != 0)
        return 8;

    /* Insert 2 partitions: p0 ([0,1000)), p1 ([1000,2000)) -> flush and settle both before faulting. */
    int64_t ts[200];
    float vals[200];
    for (int p = 0; p < 2; p++) {
        for (int i = 0; i < 200; i++) {
            ts[i] = (int64_t)p * 1000 + i * 4;
            vals[i] = (float)(p * 1000 + i);
        }
        if (eng_insert_series(d.db, TSDB_FLOAT32, sid, ts, vals, 200) != 0)
            return 9;
    }
    if (eng_flush(d.db) != 0)
        return 10;
    int64_t pk0 = 0, pk1 = d.db->cls_kmul;
    if (rh_settle_part(d.db, pk0) != 0 || rh_settle_part(d.db, pk1) != 0)
        return 11;
    uint8_t oid0[16], oid1[16];
    if (!rh_find_ec_oid(d.db, pk0, oid0) || !rh_find_ec_oid(d.db, pk1, oid1))
        return 12;

    /* Also put an extra object belonging to volume 0 (oid_missed), whose chunks on st0/st2 we will delete while st1
     * is DOWN so st1 misses the delete and keeps an orphan chunk until re-attach sweeps it. */
    uint8_t oid_missed[16];
    memcpy(oid_missed, OID, 16);
    oid_missed[0] = 0x99;
    if (put_obj(&d, oid_missed) != 0)
        return 13;

    /* Pick stores guaranteed to hold pos 0 and pos 1 of oid1 regardless of derived k' (k' >= 1, m = 1). */
    int s_eio = chunk_be_place(be, oid1, 0);
    int s_hang = chunk_be_place(be, oid1, 1);

    /* 1. Inject EIO on s_eio during queries: queries stay right via decode, s_eio becomes DOWN_IOERR, volume serves,
     *    and settles pause (!tier_writable, 0 settles while DOWN). */
    if (rh_of_cmd(ctl, "eio", st[s_eio]) != 0)
        return 14;
    for (int r = 0; r < 10; r++) {
        eccache_test_drop(d.t);
        rh_sum got = {0, 0};
        if (eng_query_raw_ex(d.db, sid, 1000, 2000, false, 0, rh_sum_cb, &got) != 0 || got.n != 200 ||
            (int64_t)(got.sum + 0.5) != 219900)
            return 15;
    }
    int64_t dl = rh_now_ms() + 6000;
    while (chunk_be_store_state(be, s_eio) != CHUNK_STORE_DOWN_IOERR && rh_now_ms() < dl)
        rh_sleep_ms(30);
    if (chunk_be_store_state(be, s_eio) != CHUNK_STORE_DOWN_IOERR)
        return 16;
    if (tier_writable(d.t))
        return 17;

    /* Volume keeps serving writes while s_eio is DOWN; new partition p3 flushed, settles MUST NOT run while DOWN. */
    uint64_t sd_before = atomic_load(&settle_done);
    for (int i = 0; i < 200; i++) {
        ts[i] = 3000 + i * 4;
        vals[i] = (float)(3000 + i);
    }
    if (eng_insert_series(d.db, TSDB_FLOAT32, sid, ts, vals, 200) != 0 || eng_flush(d.db) != 0)
        return 18;
    rh_sleep_ms(400);
    if (atomic_load(&settle_done) != sd_before)
        return 19; /* no settle may run while a store is DOWN */

    /* While s_eio is DOWN:
     * (a) Remove oid1's pos-0 chunk file on s_eio so attach MUST heal/rebuild it (kills attachnoheal).
     * (b) Delete oid_missed via chunk_be_del: deletes on the other stores, skipped on DOWN s_eio (missed delete!).
     * (c) Drop partition 0 via eng_drop_before(1000): retires p0's .ec part; ecgc holds it until s_eio recovers. */
    char hex1[33], cpath1[PATH_MAX + 128];
    chunk_oid_hex(oid1, hex1);
    snprintf(cpath1, sizeof cpath1, "%s/s0/o%s.0.chunk", st[s_eio], hex1);
    if (rh_unlink_unshimmed(cpath1) != 0)
        return 20;

    unsigned posm_on_seio = 0;
    for (unsigned p = 0; p < 3; p++)
        if (chunk_be_place(be, oid_missed, p) == s_eio)
            posm_on_seio = p;
    char hexm[33], cpathm[PATH_MAX + 128];
    chunk_oid_hex(oid_missed, hexm);
    snprintf(cpathm, sizeof cpathm, "%s/s0/o%s.%u.chunk", st[s_eio], hexm, posm_on_seio);
    for (unsigned p = 0; p < 3; p++) {
        chunk_ident cid = {.volume = 0, .pos = (uint16_t)p};
        memcpy(cid.vuuid, tier_vuuid(d.db), 16);
        memcpy(cid.oid, oid_missed, 16);
        (void)chunk_be_del(be, &cid, 1); /* deletes UP stores; s_eio is DOWN so it misses the delete */
    }
    if (eng_drop_before(d.db, 1000) != 0)
        return 21; /* retires p0; keeps p1, p3 */

    /* 2. Heal s_eio: probe passes N times -> queued attach heals oid1, sweeps oid_missed, promotes s_eio to UP,
     *    and resumes settles + ecgc. */
    if (rh_of_cmd(ctl, "heal", st[s_eio]) != 0)
        return 22;
    dl = rh_now_ms() + 15000;
    while (chunk_be_store_state(be, s_eio) != CHUNK_STORE_UP && rh_now_ms() < dl)
        rh_sleep_ms(30);
    if (chunk_be_store_state(be, s_eio) != CHUNK_STORE_UP)
        return 23;
    dl = rh_now_ms() + 5000;
    while (!tier_writable(d.t) && rh_now_ms() < dl)
        rh_sleep_ms(30);
    if (!tier_writable(d.t))
        return 24;

    /* Verify attach healed oid1 on s_eio (kills attachnoheal: file must exist and pass scrub). */
    if (access(cpath1, F_OK) != 0) {
        fprintf(stderr, "child_health: oid1 chunk on store %d was NOT healed (%s missing)\n", s_eio, cpath1);
        return 25;
    }
    {
        chunk_store *seio_ref = chunk_be_store_ref(be, s_eio);
        chunk_ident cid1 = {.volume = 0, .pos = 0};
        memcpy(cid1.vuuid, tier_vuuid(d.db), 16);
        memcpy(cid1.oid, oid1, 16);
        chunk_rd *rd = NULL;
        uint64_t bad = 1;
        int crc = seio_ref ? chunk_rd_open(seio_ref, &cid1, &rd) : TSDB_ERR_UNAVAILABLE;
        if (crc == 0) {
            crc = chunk_rd_scrub(rd, &bad);
            chunk_rd_close(rd);
        }
        chunk_store_unref(seio_ref);
        if (crc != 0 || bad != 0)
            return 26;
    }

    /* Verify missed chunk delete (oid_missed on s_eio) was swept upon re-attach. */
    if (access(cpathm, F_OK) == 0) {
        fprintf(stderr, "child_health: missed delete chunk %s was NOT swept on attach\n", cpathm);
        return 27;
    }

    /* Verify settles resume (p3 settles) and p1 still reads exact rows. */
    dl = rh_now_ms() + 10000;
    while (atomic_load(&settle_done) == sd_before && rh_now_ms() < dl)
        rh_sleep_ms(50);
    if (atomic_load(&settle_done) == sd_before)
        return 28;

    /* 3. Hang s_hang: probe detects stall and sets s_hang DOWN_STALLED within tier_stall_s + 1 s (1500 + 1000 = 2500 ms).
     *    New reads skip s_hang and decode around it quickly while s_hang is STILL hung! */
    int64_t t_hang = rh_now_ms();
    if (rh_of_cmd(ctl, "hang", st[s_hang]) != 0)
        return 29;
    dl = t_hang + 6000;
    while (chunk_be_store_state(be, s_hang) != CHUNK_STORE_DOWN_STALLED && rh_now_ms() < dl)
        rh_sleep_ms(20);
    int64_t detect_ms = rh_now_ms() - t_hang;
    if (chunk_be_store_state(be, s_hang) != CHUNK_STORE_DOWN_STALLED || detect_ms > 2500) {
        fprintf(stderr, "child_health: hang detect state=%d in %lld ms (limit 2500 ms)\n",
                chunk_be_store_state(be, s_hang), (long long)detect_ms);
        (void)rh_of_cmd(ctl, "heal", st[s_hang]);
        return 30;
    }

    /* Drop cache and run a query on p1 while s_hang is STILL hung: must complete before heal (kills downnoread). */
    eccache_test_drop(d.t);
    rh_qarg qa = {.db = d.db, .sid = sid, .t0 = 1000, .t1 = 2000, .rc = -1};
    atomic_init(&qa.done, false);
    pthread_t qt;
    if (pthread_create(&qt, NULL, rh_query_thread, &qa) != 0) {
        (void)rh_of_cmd(ctl, "heal", st[s_hang]);
        return 31;
    }
    int64_t qstart = rh_now_ms();
    while (!atomic_load_explicit(&qa.done, memory_order_acquire) && rh_now_ms() - qstart < 3000)
        rh_sleep_ms(10);
    bool q_fast = atomic_load_explicit(&qa.done, memory_order_acquire);
    int64_t q_ms = rh_now_ms() - qstart;

    /* Heal s_hang so any stuck probe/query thread unblocks cleanly before exit. */
    if (rh_of_cmd(ctl, "heal", st[s_hang]) != 0) {
        pthread_join(qt, NULL);
        return 32;
    }
    pthread_join(qt, NULL);
    if (!q_fast || qa.rc != 0 || qa.got.n != 200 || (int64_t)(qa.got.sum + 0.5) != 219900) {
        fprintf(stderr, "child_health: query while store %d hung: fast=%d ms=%lld rc=%d n=%zu sum=%.1f\n", s_hang,
                q_fast, (long long)q_ms, qa.rc, qa.got.n, qa.got.sum);
        return 33;
    }

    /* Wait for s_hang to attach and return to UP after heal. */
    dl = rh_now_ms() + 15000;
    while ((chunk_be_store_state(be, s_hang) != CHUNK_STORE_UP || !tier_writable(d.t)) && rh_now_ms() < dl)
        rh_sleep_ms(30);
    if (chunk_be_store_state(be, s_hang) != CHUNK_STORE_UP || !tier_writable(d.t))
        return 34;

    /* 3b. Regression: a store that already failed in service (st->failed = 1, so state reports DOWN_IOERR while
     *     be->st[i] is still open) and then hangs under outagefs must still be detached by the stall monitor to
     *     DOWN_STALLED so reads decode around it instead of blocking on the hung mount. */
    chunk_store *fs = chunk_be_open_store(be, s_hang);
    if (!fs)
        return 35;
    int64_t t_fh = rh_now_ms();
    if (rh_of_cmd(ctl, "hang", st[s_hang]) != 0)
        return 36;
    chunk_store_test_fail(fs); /* in-memory only; hang is already active so no healthy probe can preempt */
    if (chunk_be_store_state(be, s_hang) != CHUNK_STORE_DOWN_IOERR || chunk_be_open_store(be, s_hang) == NULL) {
        (void)rh_of_cmd(ctl, "heal", st[s_hang]);
        return 37;
    }
    dl = t_fh + 6000;
    while ((chunk_be_store_state(be, s_hang) != CHUNK_STORE_DOWN_STALLED || chunk_be_open_store(be, s_hang) != NULL) &&
           rh_now_ms() < dl)
        rh_sleep_ms(20);
    int64_t fh_ms = rh_now_ms() - t_fh;
    if (chunk_be_store_state(be, s_hang) != CHUNK_STORE_DOWN_STALLED || chunk_be_open_store(be, s_hang) != NULL ||
        fh_ms > 2500) {
        fprintf(stderr, "child_health: failed+hang detect state=%d open=%d in %lld ms (limit 2500 ms)\n",
                chunk_be_store_state(be, s_hang), chunk_be_open_store(be, s_hang) != NULL, (long long)fh_ms);
        (void)rh_of_cmd(ctl, "heal", st[s_hang]);
        return 38;
    }
    eccache_test_drop(d.t);
    rh_qarg qa2 = {.db = d.db, .sid = sid, .t0 = 1000, .t1 = 2000, .rc = -1};
    atomic_init(&qa2.done, false);
    if (pthread_create(&qt, NULL, rh_query_thread, &qa2) != 0) {
        (void)rh_of_cmd(ctl, "heal", st[s_hang]);
        return 39;
    }
    qstart = rh_now_ms();
    while (!atomic_load_explicit(&qa2.done, memory_order_acquire) && rh_now_ms() - qstart < 3000)
        rh_sleep_ms(10);
    bool q2_fast = atomic_load_explicit(&qa2.done, memory_order_acquire);
    if (rh_of_cmd(ctl, "heal", st[s_hang]) != 0) {
        pthread_join(qt, NULL);
        return 40;
    }
    pthread_join(qt, NULL);
    if (!q2_fast || qa2.rc != 0 || qa2.got.n != 200 || (int64_t)(qa2.got.sum + 0.5) != 219900) {
        fprintf(stderr, "child_health: query on failed+hung store %d: fast=%d rc=%d n=%zu sum=%.1f\n", s_hang, q2_fast,
                qa2.rc, qa2.got.n, qa2.got.sum);
        return 41;
    }
    dl = rh_now_ms() + 15000;
    while ((chunk_be_store_state(be, s_hang) != CHUNK_STORE_UP || !tier_writable(d.t)) && rh_now_ms() < dl)
        rh_sleep_ms(30);
    if (chunk_be_store_state(be, s_hang) != CHUNK_STORE_UP || !tier_writable(d.t))
        return 42;

    /* 4. Close tier while attach_th is blocked inside a hung outagefs store: tier_quiesce must return within the
     *    2 s deadline (detaching attach_th with attach_owns_ref), then healing the mount lets attach_th unblock,
     *    abort before chunk_be_attach, and drop its final tier_put() ref cleanly (verified under ASan and TSan). */
    uint64_t att_before = atomic_load_explicit(&tier_health_attach_attempts, memory_order_relaxed);
    if (rh_of_cmd(ctl, "hang", st[s_hang]) != 0)
        return 43;
    tier_health_domain_state(d.t, s_hang, true);
    tier_health_domain_state(d.t, s_hang, false); /* attach_th enters chunk_store_meta_read and blocks on hang */
    dl = rh_now_ms() + 5000;
    while (atomic_load_explicit(&tier_health_attach_attempts, memory_order_relaxed) == att_before && rh_now_ms() < dl)
        rh_sleep_ms(10);
    if (atomic_load_explicit(&tier_health_attach_attempts, memory_order_relaxed) == att_before) {
        (void)rh_of_cmd(ctl, "heal", st[s_hang]);
        return 44;
    }
    int64_t qclose0 = rh_now_ms();
    tier_quiesce(d.t);
    direct_close(&d);
    int64_t qclose_ms = rh_now_ms() - qclose0;
    /* Confirm the detach path actually ran: detached attach_th still holds attach_owns_ref, so tier_find_by_dir
     * still sees the tier until heal lets attach_th exit and run its final tier_put(). */
    tier *still = tier_find_by_dir(dbdir);
    bool was_detached = still != NULL;
    if (still)
        tier_put(still);
    if (rh_of_cmd(ctl, "heal", st[s_hang]) != 0)
        return 45;
    dl = rh_now_ms() + 5000;
    while (rh_now_ms() < dl) {
        tier *probe_t = tier_find_by_dir(dbdir);
        if (!probe_t)
            break;
        tier_put(probe_t);
        rh_sleep_ms(10);
    }
    tier *after = tier_find_by_dir(dbdir);
    if (after)
        tier_put(after);
    if (!was_detached || after != NULL || qclose_ms > 5500) {
        fprintf(stderr, "child_health: hung attach close detached=%d freed=%d ms=%lld (limit 5500 ms)\n", was_detached,
                after == NULL, (long long)qclose_ms);
        return 46;
    }
    no_tier_env();
    tier_health_test_opts(0, 0, 0, 0);
    return 0;
}

static void init_empty_db(const char *dir);
static int run_child(const char *mode, const char *dir, const char *stores, const char *ack, const char *root, int w,
                     const char *mark, const char *errf);

static void t_runtime_health(void)
{
    char base[PATH_MAX + 32], dbdir[PATH_MAX + 48], st[3][PATH_MAX + 64];
    snprintf(base, sizeof base, "%s/r15health", S);
    fresh(base);
    snprintf(dbdir, sizeof dbdir, "%s/db", base);
    for (int i = 0; i < 3; i++)
        snprintf(st[i], sizeof st[i], "%s/st%d", base, i);

    no_tier_env();
    init_empty_db(dbdir);
    for (int i = 0; i < 3; i++)
        CHECK(tsdb_tier_add_dir(dbdir, st[i], "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);

    /* (A) Abandon-flag check + direct chunk_be_set_down + concurrent reader stress + downnoread mutant check.
     *     Probes are disabled for A1-A3 so manual chunk_be_attach never races the background attach worker. */
    tier_health_test_opts(0, -1, 0, 0);
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.partition_ms = 1000;
    o.partition_classes = 0;
    o.create_volumes = 0;
    direct d;
    CHECK(tier_open(dbdir, &o, &d.t) == 0 && d.t != NULL);
    if (!d.t)
        return;
    tier_want(d.t, 0);
    CHECK(eng_open(dbdir, &o, &d.db) == 0 && d.db != NULL);
    tier_want(NULL, 0);
    if (!d.db) {
        tier_put(d.t);
        return;
    }
    chunk_backend *be = tier_backend(d.t);
    uint32_t sid = 0;
    CHECK(eng_series(d.db, "s", TSDB_FLOAT32, &sid) == 0);
    int64_t ts[200];
    float vals[200];
    for (int i = 0; i < 200; i++) {
        ts[i] = i * 4;
        vals[i] = (float)i;
    }
    CHECK(eng_insert_series(d.db, TSDB_FLOAT32, sid, ts, vals, 200) == 0);
    CHECK(eng_flush(d.db) == 0);
    CHECK(rh_settle_part(d.db, 0) == 0);
    uint8_t oid0[16];
    CHECK(rh_find_ec_oid(d.db, 0, oid0) == 1);

    /* A1: abandon flag set -> seam returns TSDB_ERR_UNAVAILABLE; cleared -> returns exact rows. */
    eccache_test_drop(d.t);
    volatile int ab = 1;
    tsdb_set_abandon_flag(&ab);
    rh_sum got = {0, 0};
    CHECK(eng_query_raw_ex(d.db, sid, 0, 1000, false, 0, rh_sum_cb, &got) == TSDB_ERR_UNAVAILABLE);
    {
        chunk_obj *ob = NULL;
        CHECK(chunk_obj_open(be, 0, tier_vuuid(d.db), oid0, NULL, NULL, &ob) == 0 && ob);
        if (ob) {
            uint8_t tmp[64];
            CHECK(chunk_obj_read(ob, 0, sizeof tmp, tmp, 0, NULL) == TSDB_ERR_UNAVAILABLE);
            chunk_obj_close(ob);
        }
    }
    ab = 0;
    tsdb_set_abandon_flag(NULL);
    got = (rh_sum){0, 0};
    CHECK(eng_query_raw_ex(d.db, sid, 0, 1000, false, 0, rh_sum_cb, &got) == 0 && got.n == 200 &&
          (int64_t)(got.sum + 0.5) == 19900);

    /* A2: invalid args to chunk_be_set_down refused; valid UP -> DOWN_STALLED skips DOWN store on BOTH already-open
     *     and newly-opened chunk_obj handles (kills downnoread deterministically!). */
    CHECK(chunk_be_set_down(be, -1, CHUNK_STORE_DOWN_IOERR) == TSDB_ERR_INVALID);
    CHECK(chunk_be_set_down(be, 99, CHUNK_STORE_DOWN_IOERR) == TSDB_ERR_INVALID);
    CHECK(chunk_be_set_down(be, 1, CHUNK_STORE_DOWN_MISSING) == TSDB_ERR_INVALID);
    {
        chunk_obj *already = NULL;
        CHECK(chunk_obj_open(be, 0, tier_vuuid(d.db), oid0, NULL, NULL, &already) == 0 && already);
        CHECK(chunk_be_set_down(be, 1, CHUNK_STORE_DOWN_STALLED) == 0);
        CHECK(chunk_be_store_state(be, 1) == CHUNK_STORE_DOWN_STALLED);
        /* Verify chunk_be_set_down did NOT close dirfd/release flock synchronously on the caller's thread (MUST stay
         * parked until chunk_be_attach releases it on the attach thread). */
        {
            chunk_store *dup = NULL;
            CHECK(chunk_store_open_quiet(st[1], NULL, &dup) == CHUNK_ERR_BUSY && dup == NULL);
        }
        /* Idempotent on already-DOWN store. */
        CHECK(chunk_be_set_down(be, 1, CHUNK_STORE_DOWN_IOERR) == 0);
        CHECK(chunk_be_store_state(be, 1) == CHUNK_STORE_DOWN_STALLED);

        unsigned pos_on_1 = 0;
        for (unsigned p = 0; p < 3; p++)
            if (chunk_be_place(be, oid0, p) == 1)
                pos_on_1 = p;
        const chunk_geom *gm = already ? chunk_obj_geom(already) : NULL;
        if (already && gm) {
            uint8_t *buf = malloc((size_t)gm->len);
            chunk_report rep;
            memset(&rep, 0, sizeof rep);
            CHECK(buf && chunk_obj_read(already, 0, gm->len, buf, 0, &rep) == 0);
            /* Must decode around store 1 (if pos_on_1 < k, data read sees DOWN and decodes; if pos_on_1 == k,
             * test newly-opened object or a position whose data cell is on store 1). */
            if (pos_on_1 < gm->k)
                CHECK(rep.st[pos_on_1] == CHUNK_ST_DOWN && rep.decoded_cells > 0);
            free(buf);
            chunk_obj_close(already);
        }
        /* Pick the store holding data position 0 of oid0 to guarantee a data position is on the DOWN store. */
        int data0_st = chunk_be_place(be, oid0, 0);
        if (data0_st != 1) {
            /* Bring store 1 back up first, then set data0_st DOWN so data pos 0 is definitely on the DOWN store. */
            chunk_heal_opts ho = {0};
            if (chunk_mutant != CHUNK_MUT_DOWNNOREAD)
                CHECK(chunk_be_attach(be, 1, st[1], NULL, &ho, NULL) == 0);
            chunk_obj *ob2 = NULL;
            CHECK(chunk_obj_open(be, 0, tier_vuuid(d.db), oid0, NULL, NULL, &ob2) == 0 && ob2);
            CHECK(chunk_be_set_down(be, data0_st, CHUNK_STORE_DOWN_STALLED) == 0);
            if (ob2) {
                uint8_t buf[128];
                chunk_report rep;
                memset(&rep, 0, sizeof rep);
                CHECK(chunk_obj_read(ob2, 0, sizeof buf, buf, 0, &rep) == 0);
                CHECK(rep.st[0] == CHUNK_ST_DOWN && rep.decoded_cells > 0); /* kills downnoread */
                chunk_obj_close(ob2);
            }
            if (chunk_mutant != CHUNK_MUT_DOWNNOREAD)
                CHECK(chunk_be_attach(be, data0_st, st[data0_st], NULL, &ho, NULL) == 0);
        } else {
            chunk_obj *ob2 = NULL;
            CHECK(chunk_obj_open(be, 0, tier_vuuid(d.db), oid0, NULL, NULL, &ob2) == 0 && ob2);
            if (ob2) {
                uint8_t buf[128];
                chunk_report rep;
                memset(&rep, 0, sizeof rep);
                CHECK(chunk_obj_read(ob2, 0, sizeof buf, buf, 0, &rep) == 0);
                CHECK(rep.st[0] == CHUNK_ST_DOWN && rep.decoded_cells > 0); /* kills downnoread */
                chunk_obj_close(ob2);
            }
            chunk_heal_opts ho = {0};
            if (chunk_mutant != CHUNK_MUT_DOWNNOREAD)
                CHECK(chunk_be_attach(be, 1, st[1], NULL, &ho, NULL) == 0);
        }
    }

    /* A3: Concurrent readers + repeated runtime set_down/attach (TSan/ASan safety check). */
    if (chunk_mutant != CHUNK_MUT_DOWNNOREAD) {
        rh_stress_arg sa = {.d = &d, .sid = sid};
        atomic_init(&sa.stop, false);
        atomic_init(&sa.errs, 0);
        atomic_init(&sa.reads, 0);
        pthread_t rths[4];
        for (int i = 0; i < 4; i++)
            CHECK(pthread_create(&rths[i], NULL, rh_stress_reader, &sa) == 0);
        for (int round = 0; round < 6; round++) {
            int victim = round % 3;
            CHECK(chunk_be_set_down(be, victim, (round & 1) ? CHUNK_STORE_DOWN_IOERR : CHUNK_STORE_DOWN_STALLED) == 0);
            eccache_reopen_all(d.t);
            rh_sleep_ms(25);
            chunk_heal_opts ho = {0};
            int arc = TSDB_ERR_IO;
            for (int try = 0; try < 50 && arc != 0; try++) {
                arc = chunk_be_attach(be, victim, st[victim], NULL, &ho, NULL);
                if (arc == CHUNK_ERR_BUSY)
                    rh_sleep_ms(10);
            }
            CHECK(arc == 0);
            eccache_reopen_all(d.t);
            rh_sleep_ms(15);
        }
        atomic_store_explicit(&sa.stop, true, memory_order_release);
        for (int i = 0; i < 4; i++)
            pthread_join(rths[i], NULL);
        CHECK(atomic_load(&sa.errs) == 0 && atomic_load(&sa.reads) > 0);
    }

    /* A4: concurrent engine open/close (v1) during store DOWN -> queued attach + sweep (verifies zero UAF on
     *     db->lock/parts/ecgc during close and zero live chunks deleted during load_parts). */
    if (chunk_mutant != CHUNK_MUT_DOWNNOREAD && chunk_mutant != CHUNK_MUT_ATTACHNOHEAL) {
        bool prev_nosweep = atomic_exchange(&ecgc_test_nosweep, false);
        char v1dir[PATH_MAX + 48];
        snprintf(v1dir, sizeof v1dir, "%s/v1", base);
        eng *v1 = NULL;
        tier_want(d.t, 1);
        CHECK(eng_open(v1dir, &o, &v1) == 0 && v1 != NULL);
        tier_want(NULL, 0);
        if (v1) {
            uint32_t v1sid = 0;
            CHECK(eng_series(v1, "v1.s", TSDB_FLOAT32, &v1sid) == 0);
            int64_t ts1[100];
            float fv1[100];
            for (int i = 0; i < 100; i++)
                ts1[i] = i * 5, fv1[i] = (float)i;
            CHECK(eng_insert_series(v1, TSDB_FLOAT32, v1sid, ts1, fv1, 100) == 0);
            CHECK(eng_flush(v1) == 0);
            CHECK(rh_settle_part(v1, 0) == 0);
            eng_close(v1);

            rh_openclose_arg oca = {.t = d.t, .dir = v1dir};
            atomic_init(&oca.stop, false);
            atomic_init(&oca.errs, 0);
            atomic_init(&oca.cycles, 0);
            pthread_t octh;
            CHECK(pthread_create(&octh, NULL, rh_openclose_thread, &oca) == 0);
            for (int round = 0; round < 6; round++) {
                tier_health_domain_state(d.t, 1, true);
                rh_sleep_ms(15);
                tier_health_domain_state(d.t, 1, false);
                int64_t dl = rh_now_ms() + 10000;
                while (chunk_be_store_state(be, 1) != CHUNK_STORE_UP && rh_now_ms() < dl)
                    rh_sleep_ms(10);
                CHECK(chunk_be_store_state(be, 1) == CHUNK_STORE_UP);
            }
            atomic_store_explicit(&oca.stop, true, memory_order_release);
            pthread_join(octh, NULL);
            CHECK(atomic_load(&oca.errs) == 0 && atomic_load(&oca.cycles) > 0);
            /* Verify v1's settled .ec part still has all chunks intact and reads exact rows after attach+sweep. */
            tier_want(d.t, 1);
            CHECK(eng_open(v1dir, &o, &v1) == 0 && v1 != NULL);
            tier_want(NULL, 0);
            if (v1) {
                eccache_test_drop(d.t);
                uint32_t v1s2 = 0;
                rh_sum got1 = {0, 0};
                CHECK(eng_series_lookup(v1, "v1.s", &v1s2) == 0);
                CHECK(eng_query_raw_ex(v1, v1s2, 0, 1000, false, 0, rh_sum_cb, &got1) == 0 && got1.n == 100);
                eng_close(v1);
            }
        }

        /* A5: in-flight settle paused inside settle_fill_hook while store 1 goes DOWN and recovers. Attach+sweep MUST
         *     wait for the in-flight settle (any_engine_busy + S1-then-S2 sweep ordering) and never delete its chunk. */
        int64_t ts2[200];
        float fv2[200];
        for (int i = 0; i < 200; i++)
            ts2[i] = d.db->partition_ms + i * 4, fv2[i] = (float)(1000 + i);
        CHECK(eng_insert_series(d.db, TSDB_FLOAT32, sid, ts2, fv2, 200) == 0);
        CHECK(eng_flush(d.db) == 0);
        int64_t pk1 = d.db->cls_kmul;
        atomic_store_explicit(&rh_hook_entered, false, memory_order_release);
        atomic_store_explicit(&rh_hook_release, false, memory_order_release);
        atomic_store_explicit(&settle_fill_hook, rh_settle_pause_hook, memory_order_release);
        rh_settle_arg sarg = {.db = d.db, .pk = pk1, .rc = -1};
        pthread_t sth;
        CHECK(pthread_create(&sth, NULL, rh_settle_thread, &sarg) == 0);
        int64_t dl = rh_now_ms() + 5000;
        while (!atomic_load_explicit(&rh_hook_entered, memory_order_acquire) && rh_now_ms() < dl)
            rh_sleep_ms(10);
        CHECK(atomic_load_explicit(&rh_hook_entered, memory_order_acquire));
        /* Drive store 1 DOWN and immediately request attach while settle is still paused (p->settling == true). */
        tier_health_domain_state(d.t, 1, true);
        tier_health_domain_state(d.t, 1, false);
        rh_sleep_ms(300);
        /* Attach worker MUST defer while settle is in flight (store 1 stays DOWN until settle finishes). */
        CHECK(chunk_be_store_state(be, 1) == CHUNK_STORE_DOWN_STALLED);
        /* Verify chunk_be_set_down did NOT close dirfd/release flock synchronously on the caller's thread (MUST stay
         * parked until chunk_be_attach releases it on the attach thread). */
        {
            chunk_store *dup = NULL;
            CHECK(chunk_store_open_quiet(st[1], NULL, &dup) == CHUNK_ERR_BUSY && dup == NULL);
        }
        atomic_store_explicit(&rh_hook_release, true, memory_order_release);
        pthread_join(sth, NULL);
        atomic_store_explicit(&settle_fill_hook, NULL, memory_order_release);
        CHECK(sarg.rc == 0);
        /* Wait for store 1 to attach & sweep after settle completed. */
        dl = rh_now_ms() + 10000;
        while ((chunk_be_store_state(be, 1) != CHUNK_STORE_UP || !tier_writable(d.t)) && rh_now_ms() < dl)
            rh_sleep_ms(20);
        CHECK(chunk_be_store_state(be, 1) == CHUNK_STORE_UP && tier_writable(d.t));
        /* Partition 1 MUST be settled, read exact 200 rows, and keep every encoded chunk intact (none swept!). */
        eccache_test_drop(d.t);
        rh_sum got2 = {0, 0};
        CHECK(eng_query_raw_ex(d.db, sid, d.db->partition_ms, d.db->partition_ms + 1000, false, 0, rh_sum_cb, &got2) ==
                  0 &&
              got2.n == 200 && (int64_t)(got2.sum + 0.5) == 219900);
        uint8_t oid1_chk[16];
        unsigned npos1 = 0;
        pthread_mutex_lock(&d.db->lock);
        for (size_t i = 0; i < d.db->nparts; i++) {
            if (d.db->parts[i]->partition != pk1)
                continue;
            struct ecpart *ep = atomic_load_explicit(&d.db->parts[i]->ec, memory_order_acquire);
            if (ep && !ep->test_mark) {
                memcpy(oid1_chk, ep->rec.oid, 16);
                npos1 = (unsigned)ep->rec.k + (unsigned)ep->rec.m;
                break;
            }
        }
        pthread_mutex_unlock(&d.db->lock);
        CHECK(npos1 >= 2);
        for (unsigned pos = 0; pos < npos1; pos++) {
            int sidx = chunk_be_place(be, oid1_chk, pos);
            chunk_store *sref = chunk_be_store_ref(be, sidx);
            chunk_ident cid = {.volume = 0, .pos = (uint16_t)pos};
            memcpy(cid.vuuid, tier_vuuid(d.db), 16);
            memcpy(cid.oid, oid1_chk, 16);
            chunk_rd *rd = NULL;
            uint64_t bad = 1;
            int crc = sref ? chunk_rd_open(sref, &cid, &rd) : TSDB_ERR_UNAVAILABLE;
            if (crc == 0) {
                crc = chunk_rd_scrub(rd, &bad);
                chunk_rd_close(rd);
            }
            chunk_store_unref(sref);
            CHECK(crc == 0 && bad == 0);
        }
        atomic_store(&ecgc_test_nosweep, prev_nosweep);
    }

    tier_quiesce(d.t);
    direct_close(&d);
    tier_health_test_opts(0, 0, 0, 0);

    /* A6: 4+2 tier (6 stores) with one store missing (DOWN_MISSING): taking another store DOWN and recovering it
     *     leaves it in REPLACING without spin-retrying full-backend chunk_be_settle at 5 Hz while the first store
     *     stays gone. */
    if (chunk_mutant != CHUNK_MUT_DOWNNOREAD && chunk_mutant != CHUNK_MUT_ATTACHNOHEAL) {
        char base6[PATH_MAX + 32], db6[PATH_MAX + 48], st6[6][PATH_MAX + 64], st5_gone[PATH_MAX + 80];
        snprintf(base6, sizeof base6, "%s/r15health_nospin", S);
        fresh(base6);
        snprintf(db6, sizeof db6, "%s/db", base6);
        for (int i = 0; i < 6; i++)
            snprintf(st6[i], sizeof st6[i], "%s/st%d", base6, i);
        snprintf(st5_gone, sizeof st5_gone, "%s/st5.gone", base6);
        tier_env(base6, 6, 4, 2);
        /* Open once to create stores & write 1 object, then hide st5 so reopen sees st5 DOWN_MISSING. */
        direct d6;
        tier_health_test_opts(0, -1, 0, 0);
        CHECK(tier_open(db6, &o, &d6.t) == 0 && d6.t);
        if (d6.t) {
            tier_want(d6.t, 0);
            CHECK(eng_open(db6, &o, &d6.db) == 0 && d6.db);
            tier_want(NULL, 0);
            if (d6.db)
                CHECK(put_obj(&d6, OID) == 0);
            tier_quiesce(d6.t);
            direct_close(&d6);
        }
        CHECK(rename(st6[5], st5_gone) == 0);

        /* Reopen with fast probes (100 ms, ok_need 2): st5 is DOWN_MISSING, st0..st4 are UP (5 >= 4 min_up). */
        tier_health_test_opts(1500, 100, 2, 500);
        CHECK(tier_open(db6, &o, &d6.t) == 0 && d6.t);
        if (d6.t) {
            tier_want(d6.t, 0);
            CHECK(eng_open(db6, &o, &d6.db) == 0 && d6.db);
            tier_want(NULL, 0);
            chunk_backend *be6 = tier_backend(d6.t);
            CHECK(chunk_be_store_state(be6, 5) == CHUNK_STORE_DOWN_MISSING);
            uint64_t att0 = atomic_load(&tier_health_attach_attempts);
            tier_health_domain_state(d6.t, 1, true);
            CHECK(chunk_be_store_state(be6, 1) == CHUNK_STORE_DOWN_STALLED);
            tier_health_domain_state(d6.t, 1, false);
            int64_t dl = rh_now_ms() + 5000;
            while (chunk_be_store_state(be6, 1) != CHUNK_STORE_REPLACING && rh_now_ms() < dl)
                rh_sleep_ms(20);
            CHECK(chunk_be_store_state(be6, 1) == CHUNK_STORE_REPLACING);
            /* Wait 800 ms with 100 ms probes running: attach worker MUST NOT spin calling chunk_be_settle! */
            rh_sleep_ms(800);
            uint64_t att_delta = atomic_load(&tier_health_attach_attempts) - att0;
            CHECK(att_delta >= 1 && att_delta <= 2);
            tier_quiesce(d6.t);
            direct_close(&d6);
        }
        no_tier_env();
        tier_health_test_opts(0, 0, 0, 0);
    }

    /* (B) Outagefs child: real EIO, hang/stall detection, queued attach/heal, missed deletes swept, settles resume. */
    if (access(SHIM_OUTAGE, R_OK) != 0) {
        printf("  runtime_health: abandon+set_down+concurrency ok (outagefs part skipped: no %s)\n", SHIM_OUTAGE);
        return;
    }
    /* Skip spawning outagefs child if downnoread already failed above (keeps mutant runs fast). */
    if (chunk_mutant == CHUNK_MUT_DOWNNOREAD && fails > 0)
        return;

    char obase[PATH_MAX + 32], ost[PATH_MAX + 48], ctl[PATH_MAX + 64], roots[4 * PATH_MAX];
    snprintf(obase, sizeof obase, "%s/r15health_of", S);
    fresh(obase);
    snprintf(ost, sizeof ost, "%s/stores", obase);
    mkdir(ost, 0755);
    for (int i = 0; i < 3; i++) {
        char sd[PATH_MAX + 64];
        snprintf(sd, sizeof sd, "%s/st%d", ost, i);
        mkdir(sd, 0755);
    }
    snprintf(ctl, sizeof ctl, "%s/ctl", obase);
    snprintf(roots, sizeof roots, "%s/st0:%s/st1:%s/st2", ost, ost, ost);
    const char *mname = tier_mutant == TIER_MUT_DOWNNOREAD ? "downnoread"
                        : tier_mutant == TIER_MUT_ATTACHNOHEAL ? "attachnoheal" : "none";
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512];
        snprintf(asan, sizeof asan, "verify_asan_link_order=0:detect_leaks=0%s%.400s",
                 getenv("ASAN_OPTIONS") ? ":" : "", getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
        setenv("ASAN_OPTIONS", asan, 1);
        setenv("LD_PRELOAD", SHIM_OUTAGE, 1);
        setenv("OUTAGEFS_ROOTS", roots, 1);
        setenv("OUTAGEFS_CTL", ctl, 1);
        execl(SELF, "test_tier", "--child", "health", obase, ost, mname, (char *)NULL);
        _exit(127);
    }
    int st_w = -1;
    CHECK(pid > 0 && waitpid(pid, &st_w, 0) == pid && WIFEXITED(st_w) && WEXITSTATUS(st_w) == 0);
    if (pid > 0 && (!WIFEXITED(st_w) || WEXITSTATUS(st_w) != 0))
        fprintf(stderr, "  runtime_health: child exited with status %d (sig %d)\n",
                WIFEXITED(st_w) ? WEXITSTATUS(st_w) : -1, WIFSIGNALED(st_w) ? WTERMSIG(st_w) : 0);

    /* (C) Crashfs power-loss at every sync step of attach + missed-delete sweep: reopening after every cut keeps
     *     every live .ec part readable with exact rows and zero corruption. */
    if (access(SHIM, R_OK) == 0 && chunk_mutant != CHUNK_MUT_DOWNNOREAD && chunk_mutant != CHUNK_MUT_ATTACHNOHEAL) {
        char pbase[PATH_MAX + 32], pdb[PATH_MAX + 48], pst[3][PATH_MAX + 64];
        char pack[PATH_MAX + 48], perr[PATH_MAX + 48];
        snprintf(pbase, sizeof pbase, "%s/r15health_pwr", S);
        snprintf(pdb, sizeof pdb, "%s/db", pbase);
        for (int i = 0; i < 3; i++)
            snprintf(pst[i], sizeof pst[i], "%s/st%d", pbase, i);
        snprintf(pack, sizeof pack, "%s/r15health_pwr.ack", S);
        snprintf(perr, sizeof perr, "%s/r15health_pwr.err", S);
        int cuts = 0, f0 = fails;
        for (int w = 0; w < 200; w++) {
            fresh(pbase);
            no_tier_env();
            tier_health_test_opts(0, -1, 0, 0);
            tsdb *tdb = NULL;
            CHECK(tsdb_open(pdb, &o, &tdb) == 0 && tdb);
            if (tdb)
                tsdb_close(tdb);
            for (int i = 0; i < 3; i++)
                CHECK(tsdb_tier_add_dir(pdb, pst[i], "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);
            direct pd;
            CHECK(tier_open(pdb, &o, &pd.t) == 0 && pd.t);
            tier_want(pd.t, 0);
            CHECK(eng_open(pdb, &o, &pd.db) == 0 && pd.db);
            tier_want(NULL, 0);
            uint32_t psid = 0;
            CHECK(eng_series(pd.db, "hp.s", TSDB_FLOAT32, &psid) == 0);
            int64_t pts[100];
            float pfv[100];
            for (int i = 0; i < 100; i++)
                pts[i] = i * 5, pfv[i] = (float)i;
            CHECK(eng_insert_series(pd.db, TSDB_FLOAT32, psid, pts, pfv, 100) == 0);
            CHECK(eng_flush(pd.db) == 0);
            CHECK(rh_settle_part(pd.db, 0) == 0);
            uint8_t poid[16], omiss[16];
            CHECK(rh_find_ec_oid(pd.db, 0, poid) == 1);
            memcpy(omiss, OID, 16);
            omiss[0] = 0x77;
            CHECK(put_obj(&pd, omiss) == 0);
            int victim = chunk_be_place(tier_backend(pd.t), poid, 0); /* pos 0 is always present on victim */
            tier_quiesce(pd.t);
            direct_close(&pd);
            /* Remove poid's pos-0 chunk on victim so attach must rebuild it, and leave omiss so sweep deletes it. */
            char phex[33], pcpath[PATH_MAX + 128], vstr[16];
            chunk_oid_hex(poid, phex);
            snprintf(pcpath, sizeof pcpath, "%s/s0/o%s.0.chunk", pst[victim], phex);
            CHECK(unlink(pcpath) == 0);
            snprintf(vstr, sizeof vstr, "%d", victim);
            int crc = run_child("health_power", pdb, pbase, vstr, pst[victim], w, "", perr);
            /* Reopen after every power cut (rc == 99) or clean finish (rc == 0) and verify exact query rows. */
            CHECK(crc == 0 || crc == 99);
            CHECK(tier_open(pdb, &o, &pd.t) == 0 && pd.t);
            tier_want(pd.t, 0);
            CHECK(eng_open(pdb, &o, &pd.db) == 0 && pd.db);
            tier_want(NULL, 0);
            uint32_t psid2 = 0;
            rh_sum pgot = {0, 0};
            CHECK(eng_series_lookup(pd.db, "hp.s", &psid2) == 0);
            CHECK(eng_query_raw_ex(pd.db, psid2, 0, 1000, false, 0, rh_sum_cb, &pgot) == 0 && pgot.n == 100 &&
                  (int64_t)(pgot.sum + 0.5) == 4950);
            tier_quiesce(pd.t);
            direct_close(&pd);
            tier_health_test_opts(0, 0, 0, 0);
            if (fails > f0) {
                fprintf(stderr, "  runtime_health power: failed at step %d (child exit %d)\n", w, crc);
                break;
            }
            if (crc == 0)
                break;
            cuts += crc == 99;
        }
        CHECK(cuts > 0);
        printf("  runtime_health: attach+sweep power loss clean across %d crashfs cuts\n", cuts);
    }

    printf("  runtime_health: abandon flag, set_down concurrency, open/close + in-flight settle races, outagefs EIO + "
           "hang + attach heal + missed-delete sweep + settle resume\n");
}
#endif

/* ----------------------------------------------------------------------------------------------- power, eio: child */

static int child_full_hang(int argc, char **argv);

static int child(int argc, char **argv)
{
    /* --child MODE DIR STORES ACK */
    if (argc < 6)
        return 2;
    const char *mode = argv[2], *dir = argv[3], *stores = argv[4], *ack = argv[5];
    if (strcmp(mode, "health") == 0)
        return child_health(argc, argv);
    if (strcmp(mode, "health_power") == 0)
        return child_health_power(dir, atoi(ack));
    if (strcmp(mode, "full_hang") == 0)
        return child_full_hang(argc, argv);
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
        setenv("TSDB_TEST_TIER_HEALTH", "probe=0", 1);
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

/* --------------------------------------------------------------------------------------------------- R15: TIERS */

static int put_obj_km(direct *d, const uint8_t oid[16], unsigned k, unsigned m)
{
    chunk_geom g;
    int rc = chunk_geom_make(&g, k, m, 512, sizeof OBJ);
    return rc ? rc : chunk_be_put_object(tier_backend(d->t), d->db->tier_vol, tier_vuuid(d->db), oid, &g, 1, OBJ, ~0u,
                                         NULL);
}

static void init_empty_db(const char *dir)
{
    tsdb *db = NULL;
    CHECK(open_db(dir, &db, 0) == 0 && db != NULL);
    if (db)
        tsdb_close(db);
}

/* R15 r15-open: open from TIERS with strict store_id & db_id checks, LOST store handling, REPLACED_BY, and min_up.
 * Kills planted mutants `dbidskip` (foreign db_id / missing db= binding with matching store_id) and `lostopen`
 * (LOST store opened as UP instead of NULL / DOWN_MISSING). */
static void t_tiers_open(void)
{
    char base[PATH_MAX + 32], db1[PATH_MAX + 48], db2[PATH_MAX + 48];
    char st[3][PATH_MAX + 64], s1_bak[PATH_MAX + 80], s1_rep[PATH_MAX + 80], db2_st[PATH_MAX + 64];
    snprintf(base, sizeof base, "%s/r15open", S);
    fresh(base);
    snprintf(db1, sizeof db1, "%s/db1", base);
    snprintf(db2, sizeof db2, "%s/db2", base);
    snprintf(db2_st, sizeof db2_st, "%s/db2_s0", base);
    for (int i = 0; i < 3; i++)
        snprintf(st[i], sizeof st[i], "%s/st%d", base, i);
    snprintf(s1_bak, sizeof s1_bak, "%s/st1.bak", base);
    snprintf(s1_rep, sizeof s1_rep, "%s/st1.rep", base);

    no_tier_env();
    init_empty_db(db1);
    init_empty_db(db2);
    for (int i = 0; i < 3; i++)
        CHECK(tsdb_tier_add_dir(db1, st[i], "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);
    CHECK(tsdb_tier_add_dir(db2, db2_st, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);

    int dfd1 = open(db1, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    int dfd2 = open(db2, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    CHECK(dfd1 >= 0 && dfd2 >= 0);
    tiers_view tv1;
    uint8_t db1_id[16], db2_id[16];
    bool has1 = false, has2 = false;
    CHECK(tiers_load(dfd1, false, false, &tv1) == 0 && tv1.nstores == 3);
    CHECK(tiers_db_id_read(dfd1, &has1, db1_id) == 0 && has1);
    CHECK(tiers_db_id_read(dfd2, &has2, db2_id) == 0 && has2);
    CHECK(memcmp(db1_id, db2_id, 16) != 0);
    close(dfd2);
    uint8_t s1_id[16], s2_id[16];
    memcpy(s1_id, tv1.stores[1].id, 16);
    memcpy(s2_id, tv1.stores[2].id, 16);
    tiers_view_free(&tv1);

    /* Normal open from TIERS: 3 stores -> default 2+1, all UP, put & read succeed. */
    direct d;
    CHECK(direct_open(db1, 0, &d) == 0 && d.t && d.db);
    if (!d.db) {
        close(dfd1);
        return;
    }
    CHECK(tier_config(d.t)->k == 2 && tier_config(d.t)->m == 1);
    CHECK(tier_writable(d.t));
    CHECK(put_obj_km(&d, OID, 2, 1) == 0 && get_obj(&d, OID) == 0);
    direct_close(&d);

    /* 1. Missing store is strictly refused (naming the store), and NOT re-created. */
    CHECK(rename(st[1], s1_bak) == 0);
    cap_begin();
    int rc = direct_open(db1, 0, &d);
    const char *err = cap_end();
    CHECK(rc == TSDB_ERR_NOTFOUND && !d.t);
    CHECK(strstr(err, st[1]) != NULL);
    CHECK(access(st[1], F_OK) != 0);
    if (d.t)
        direct_close(&d);

    /* 2. Wrong store_id (same db_id, freshly minted store_id) is refused naming the store. */
    chunk_store_owner ow1;
    memset(&ow1, 0, sizeof ow1);
    ow1.db_set = true;
    memcpy(ow1.db_id, db1_id, 16);
    CHECK(chunk_store_init2(st[1], NULL, &ow1, NULL) == 0);
    cap_begin();
    rc = direct_open(db1, 0, &d);
    err = cap_end();
    CHECK(rc == TSDB_ERR_INVALID && !d.t);
    CHECK(strstr(err, st[1]) != NULL);
    if (d.t)
        direct_close(&d);
    sh("rm -rf '%s'", st[1]);

    /* 3. Foreign db_id WITH matching store_id -> refused naming store and BOTH db_ids (kills `dbidskip`). */
    chunk_store_owner ow2;
    memset(&ow2, 0, sizeof ow2);
    ow2.db_set = true;
    memcpy(ow2.db_id, db2_id, 16);
    CHECK(chunk_store_init2(st[1], s1_id, &ow2, NULL) == 0);
    char hex1[33], hex2[33];
    chunk_oid_hex(db1_id, hex1);
    chunk_oid_hex(db2_id, hex2);
    cap_begin();
    rc = direct_open(db1, 0, &d);
    err = cap_end();
    CHECK(rc == TSDB_ERR_INVALID && !d.t);
    CHECK(strstr(err, st[1]) != NULL && strstr(err, hex1) != NULL && strstr(err, hex2) != NULL);
    if (d.t)
        direct_close(&d);
    sh("rm -rf '%s'", st[1]);

    /* 4. Unbound store (no db= line) WITH matching store_id -> refused naming store and expected db_id (kills `dbidskip`). */
    CHECK(chunk_store_init(st[1], s1_id, NULL) == 0);
    cap_begin();
    rc = direct_open(db1, 0, &d);
    err = cap_end();
    CHECK(rc == TSDB_ERR_INVALID && !d.t);
    CHECK(strstr(err, st[1]) != NULL && strstr(err, "no db= binding") != NULL && strstr(err, hex1) != NULL);
    if (d.t)
        direct_close(&d);
    sh("rm -rf '%s'", st[1]);

    /* Restore original st[1] -> opens cleanly again. */
    CHECK(rename(s1_bak, st[1]) == 0);
    CHECK(direct_open(db1, 0, &d) == 0 && d.t && tier_writable(d.t) && get_obj(&d, OID) == 0);
    direct_close(&d);

    /* 5. LOST store: mark st[1] LOST in TIERS.
     *    (a) Even while st[1] still sits on disk, it MUST open as DOWN_MISSING (NULL), NOT UP (kills `lostopen`),
     *        reads decode around it, and settles wait (!tier_writable). */
    CHECK(tiers_append_state(dfd1, true, s1_id, TIERS_STATE_LOST, 0, false, NULL, NULL) == 0);
    CHECK(direct_open(db1, 0, &d) == 0 && d.t && d.db);
    if (d.t) {
        CHECK(chunk_be_store_state(tier_backend(d.t), 1) == CHUNK_STORE_DOWN_MISSING);
        CHECK(chunk_be_store(tier_backend(d.t), 1) == NULL);
        CHECK(!tier_writable(d.t)); /* settles wait */
        CHECK(get_obj(&d, OID) == 0); /* reads decode around the LOST store */
        direct_close(&d);
    }

    /*    (b) With st[1] removed from disk, open MUST still succeed (also kills `lostopen`). */
    CHECK(rename(st[1], s1_bak) == 0);
    CHECK(direct_open(db1, 0, &d) == 0 && d.t && d.db);
    if (d.t) {
        CHECK(chunk_be_store_state(tier_backend(d.t), 1) == CHUNK_STORE_DOWN_MISSING);
        CHECK(!tier_writable(d.t));
        CHECK(get_obj(&d, OID) == 0);
        direct_close(&d);
    }

    /* 6. REPLACED_BY: create replacement store bound to db1_id, record REPLACED_BY -> opens UP with new id. */
    uint8_t rep_id[16];
    CHECK(chunk_store_init2(s1_rep, NULL, &ow1, rep_id) == 0);
    CHECK(tiers_append_state(dfd1, true, s1_id, TIERS_STATE_REPLACED_BY, TSDB_TIER_CLASS_HDD, false, rep_id, s1_rep) == 0);
    CHECK(direct_open(db1, 0, &d) == 0 && d.t && d.db);
    if (d.t) {
        CHECK(chunk_be_store_state(tier_backend(d.t), 1) == CHUNK_STORE_UP);
        chunk_store *rs = chunk_be_store(tier_backend(d.t), 1);
        CHECK(rs != NULL && memcmp(chunk_store_id(rs), rep_id, 16) == 0);
        CHECK(tier_writable(d.t));
        CHECK(get_obj(&d, OID) == 0);
        direct_close(&d);
    }

    /* 7. Too many LOST stores (2 of 3 LOST for 2+1 -> 1 < min_up=2): open fails with TSDB_ERR_UNAVAILABLE. */
    CHECK(tiers_append_state(dfd1, true, rep_id, TIERS_STATE_LOST, 0, false, NULL, NULL) == 0);
    CHECK(tiers_append_state(dfd1, true, s2_id, TIERS_STATE_LOST, 0, false, NULL, NULL) == 0);
    cap_begin();
    rc = direct_open(db1, 0, &d);
    err = cap_end();
    CHECK(rc == TSDB_ERR_UNAVAILABLE && !d.t);
    CHECK(strstr(err, "1 of 3") != NULL && strstr(err, "2 are needed") != NULL);
    if (d.t)
        direct_close(&d);

    close(dfd1);
    printf("  tiers_open: missing, wrong store_id, foreign/unbound db_id, LOST, REPLACED_BY, min_up\n");
}

/* R15 r15-open: D4 defaults for 2, 3, 4, 5, 6, 8 stores, 1 non-single-copy store, and single-copy m = 0. */
static void t_tiers_defaults(void)
{
    no_tier_env();
    static const struct {
        int n, k, m;
    } cases[] = {{2, 1, 1}, {3, 2, 1}, {4, 3, 1}, {5, 4, 1}, {6, 4, 2}, {8, 4, 2}};
    for (size_t ci = 0; ci < sizeof cases / sizeof cases[0]; ci++) {
        char base[PATH_MAX + 32], dbdir[PATH_MAX + 48], stdir[PATH_MAX + 64];
        snprintf(base, sizeof base, "%s/r15def_%d", S, cases[ci].n);
        fresh(base);
        snprintf(dbdir, sizeof dbdir, "%s/db", base);
        init_empty_db(dbdir);
        for (int i = 0; i < cases[ci].n; i++) {
            snprintf(stdir, sizeof stdir, "%s/st%d", base, i);
            CHECK(tsdb_tier_add_dir(dbdir, stdir, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);
        }
        direct d;
        CHECK(direct_open(dbdir, 0, &d) == 0 && d.t && d.db);
        if (d.t && d.db) {
            const tier_settings *ts = tier_eng_settings(d.db);
            CHECK(tier_config(d.t)->k == cases[ci].k && tier_config(d.t)->m == cases[ci].m);
            CHECK(ts && ts->from_tiers && ts->k == cases[ci].k && ts->m == cases[ci].m);
            CHECK(ts->tier_after_s == TIERS_AFTER_S_DEFAULT && ts->grace_s == (int64_t)TIERS_AFTER_S_DEFAULT);
            CHECK(ts->tier_high_pct == TIERS_HIGH_PCT_DEFAULT && ts->young_high_pct == TIERS_YOUNG_HIGH_PCT_DEFAULT);
            CHECK(ts->rebuild_mb_s == TIERS_REBUILD_MB_S_DEFAULT && ts->scrub_mb_s == TIERS_SCRUB_MB_S_DEFAULT &&
                  ts->scrub_days == TIERS_SCRUB_DAYS_DEFAULT);
            CHECK(put_obj_km(&d, OID, (unsigned)cases[ci].k, (unsigned)cases[ci].m) == 0 && get_obj(&d, OID) == 0);
            direct_close(&d);
        }
    }

    /* 1 non-single-copy store: opens cleanly with no active tier yet (waits for 2nd store). */
    {
        char base[PATH_MAX + 32], dbdir[PATH_MAX + 48], st0[PATH_MAX + 64];
        snprintf(base, sizeof base, "%s/r15one_plain", S);
        fresh(base);
        snprintf(dbdir, sizeof dbdir, "%s/db", base);
        snprintf(st0, sizeof st0, "%s/st0", base);
        init_empty_db(dbdir);
        CHECK(tsdb_tier_add_dir(dbdir, st0, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);
        tsdb_options o;
        tsdb_options_init(&o);
        tier *t = (tier *)0x1;
        CHECK(tier_open(dbdir, &o, &t) == 0 && t == NULL);
    }

    /* Single-copy (m = 0): 1 store and 2 stores, all --single-copy -> 1+0, min_up = all stores. */
    for (int n = 1; n <= 2; n++) {
        char base[PATH_MAX + 32], dbdir[PATH_MAX + 48], stdir[PATH_MAX + 64];
        snprintf(base, sizeof base, "%s/r15sc_%d", S, n);
        fresh(base);
        snprintf(dbdir, sizeof dbdir, "%s/db", base);
        init_empty_db(dbdir);
        for (int i = 0; i < n; i++) {
            snprintf(stdir, sizeof stdir, "%s/st%d", base, i);
            CHECK(tsdb_tier_add_dir(dbdir, stdir, "hdd", TSDB_TIER_SINGLE_COPY | TSDB_TIER_ALLOW_SHARED_DEV) == 0);
        }
        direct d;
        CHECK(direct_open(dbdir, 0, &d) == 0 && d.t && d.db);
        if (d.t && d.db) {
            CHECK(tier_config(d.t)->k == 1 && tier_config(d.t)->m == 0);
            CHECK(tier_eng_settings(d.db)->k == 1 && tier_eng_settings(d.db)->m == 0);
            CHECK(put_obj_km(&d, OID, 1, 0) == 0 && get_obj(&d, OID) == 0);
            direct_close(&d);
        }
        /* Marking 1 store LOST violates min_up = n (all stores for m = 0) -> open fails with UNAVAILABLE. */
        int dfd = open(dbdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        tiers_view tv;
        CHECK(dfd >= 0 && tiers_load(dfd, false, false, &tv) == 0 && tv.nstores == (size_t)n);
        CHECK(tiers_append_state(dfd, true, tv.stores[0].id, TIERS_STATE_LOST, 0, false, NULL, NULL) == 0);
        tiers_view_free(&tv);
        close(dfd);
        CHECK(direct_open(dbdir, 0, &d) == TSDB_ERR_UNAVAILABLE && !d.t);
    }

    /* Mixed single-copy + non-single-copy (2 stores): default 1+1; SETTINGS requesting m = 0 is refused. */
    {
        char base[PATH_MAX + 32], dbdir[PATH_MAX + 48], st0[PATH_MAX + 64], st1[PATH_MAX + 64];
        snprintf(base, sizeof base, "%s/r15mix", S);
        fresh(base);
        snprintf(dbdir, sizeof dbdir, "%s/db", base);
        snprintf(st0, sizeof st0, "%s/st0", base);
        snprintf(st1, sizeof st1, "%s/st1", base);
        init_empty_db(dbdir);
        CHECK(tsdb_tier_add_dir(dbdir, st0, "hdd", TSDB_TIER_SINGLE_COPY | TSDB_TIER_ALLOW_SHARED_DEV) == 0);
        CHECK(tsdb_tier_add_dir(dbdir, st1, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);
        direct d;
        CHECK(direct_open(dbdir, 0, &d) == 0 && d.t && tier_config(d.t)->k == 1 && tier_config(d.t)->m == 1);
        if (d.t)
            direct_close(&d);
        int dfd = open(dbdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        tiers_settings s;
        memset(&s, 0, sizeof s);
        s.set_mask = TSDB_TIER_SET_REDUNDANCY;
        s.k = 1;
        s.m = 0;
        CHECK(dfd >= 0 && tiers_append_settings(dfd, true, TIERS_VOL_ALL, &s) == 0);
        close(dfd);
        cap_begin();
        int rc = direct_open(dbdir, 0, &d);
        const char *err = cap_end();
        CHECK(rc == TSDB_ERR_INVALID && !d.t && strstr(err, "single-copy") != NULL);
    }
    printf("  tiers_defaults: D4 defaults (2,3,4,5,6,8), 1-store wait, single-copy m=0 and mixed refusal\n");
}

/* R15 r15-open: per-volume SETTINGS override, cache_bytes option, TSDB_TEST_TIER beside TIERS, and refusals. */
static void t_tiers_settings(void)
{
    char base[PATH_MAX + 32], dbdir[PATH_MAX + 48], vol1[PATH_MAX + 48], stdir[PATH_MAX + 64];
    snprintf(base, sizeof base, "%s/r15set", S);
    fresh(base);
    snprintf(dbdir, sizeof dbdir, "%s/db", base);
    snprintf(vol1, sizeof vol1, "%s/v1", base);
    no_tier_env();
    init_empty_db(dbdir);
    for (int i = 0; i < 6; i++) {
        snprintf(stdir, sizeof stdir, "%s/st%d", base, i);
        CHECK(tsdb_tier_add_dir(dbdir, stdir, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);
    }

    int dfd = open(dbdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    CHECK(dfd >= 0);
    tiers_settings gs, vs1;
    memset(&gs, 0, sizeof gs);
    gs.set_mask = TSDB_TIER_SET_REDUNDANCY | TSDB_TIER_SET_AFTER | TSDB_TIER_SET_HIGH_PCT |
                  TSDB_TIER_SET_YOUNG_HIGH_PCT | TSDB_TIER_SET_REBUILD_MB_S | TSDB_TIER_SET_SCRUB_MB_S |
                  TSDB_TIER_SET_SCRUB_DAYS;
    gs.k = 3;
    gs.m = 2;
    gs.tier_after_s = 1000;
    gs.tier_high_pct = 75;
    gs.young_high_pct = 88;
    gs.rebuild_mb_s = 150;
    gs.scrub_mb_s = 40;
    gs.scrub_days = 7;
    CHECK(tiers_append_settings(dfd, true, TIERS_VOL_ALL, &gs) == 0);

    memset(&vs1, 0, sizeof vs1);
    vs1.set_mask = TSDB_TIER_SET_REDUNDANCY | TSDB_TIER_SET_AFTER | TSDB_TIER_SET_HIGH_PCT;
    vs1.k = 4;
    vs1.m = 1;
    vs1.tier_after_s = 300;
    vs1.tier_high_pct = 65;
    CHECK(tiers_append_settings(dfd, true, 1, &vs1) == 0);
    close(dfd);

    /* Per-process cache size option + per-volume SETTINGS override. */
    tsdb_tier_set_cache_bytes(32u << 20);
    direct d;
    CHECK(direct_open(dbdir, 0, &d) == 0 && d.t && d.db);
    if (d.t && d.db) {
        const tier_settings *s0 = tier_eng_settings(d.db);
        CHECK(tier_config(d.t)->cache_mb == 32);
        CHECK(s0 && s0->from_tiers && s0->k == 3 && s0->m == 2 && s0->tier_after_s == 1000 && s0->grace_s == 1000 &&
              s0->tier_high_pct == 75 && s0->young_high_pct == 88 && s0->rebuild_mb_s == 150 && s0->scrub_mb_s == 40 &&
              s0->scrub_days == 7 && s0->cache_mb == 32);
        tsdb_options o;
        tsdb_options_init(&o);
        eng *e1 = NULL;
        tier_want(d.t, 1);
        CHECK(eng_open(vol1, &o, &e1) == 0 && e1 != NULL);
        tier_want(NULL, 0);
        if (e1) {
            const tier_settings *s1 = tier_eng_settings(e1);
            CHECK(s1 && s1->from_tiers && s1->k == 4 && s1->m == 1 && s1->tier_after_s == 300 && s1->grace_s == 300 &&
                  s1->tier_high_pct == 65 && s1->young_high_pct == 88 && s1->rebuild_mb_s == 150 &&
                  s1->scrub_mb_s == 40 && s1->scrub_days == 7);
            eng_close(e1);
        }
        direct_close(&d);
    }
    tsdb_tier_set_cache_bytes(0);

    /* TSDB_TEST_TIER beside TIERS: dirs= refused (with or without I_KNOW), timing keys need I_KNOW=1. */
    setenv("TSDB_TEST_TIER", "k=2,m=1,dirs=/a:/b:/c", 1);
    setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);
    cap_begin();
    int rc = direct_open(dbdir, 0, &d);
    const char *err = cap_end();
    CHECK(rc == TSDB_ERR_INVALID && !d.t && strstr(err, "dirs=") != NULL);

    unsetenv("TSDB_TEST_TIER_I_KNOW");
    setenv("TSDB_TEST_TIER", "dirs=/a:/b:/c", 1);
    cap_begin();
    rc = direct_open(dbdir, 0, &d);
    err = cap_end();
    CHECK(rc == TSDB_ERR_INVALID && !d.t && strstr(err, "dirs=") != NULL);

    setenv("TSDB_TEST_TIER", "grace_s=5,keep_s=10,eager=1", 1);
    unsetenv("TSDB_TEST_TIER_I_KNOW");
    cap_begin();
    rc = direct_open(dbdir, 0, &d);
    err = cap_end();
    CHECK(rc == TSDB_ERR_INVALID && !d.t && strstr(err, "TSDB_TEST_TIER_I_KNOW=1") != NULL);

    setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);
    CHECK(direct_open(dbdir, 0, &d) == 0 && d.t && d.db);
    if (d.t && d.db) {
        const tier_settings *s0 = tier_eng_settings(d.db);
        CHECK(s0 && s0->k == 3 && s0->m == 2 && s0->grace_s == 5 && s0->tier_after_s == 5 && s0->keep_s == 10 &&
              s0->eager);
        direct_close(&d);
    }
    no_tier_env();

    /* Read-only / follower open on a TIERS DB (root META has requires=tier) succeeds (R15 D13, r15-follow). */
    tsdb *db = NULL;
    CHECK(open_db(dbdir, &db, 1) == 0 && db != NULL);
    if (db)
        tsdb_close(db);
    db = NULL;
    tsdb_options fo;
    tsdb_options_init(&fo);
    fo.follow = 1;
    CHECK(tsdb_open(dbdir, &fo, &db) == 0 && db != NULL);
    if (db)
        tsdb_close(db);

    printf("  tiers_settings: per-volume SETTINGS, cache_bytes, TSDB_TEST_TIER rules, ro/follow open\n");
}

/* ---------------------------------------------------------------------------------------------- R15: follower */

static int tf_sum_cb(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    return rh_sum_cb(ctx, ts, v, n);
}

static int tf_query_sum(tsdb *db, tsdb_sid sid, int64_t t0, int64_t t1, rh_sum *out)
{
    *out = (rh_sum){0, 0};
    return tsdb_query_raw(db, sid, t0, t1, tf_sum_cb, out);
}

static int tf_count_ec_files(const char *dir)
{
    DIR *dd = opendir(dir);
    if (!dd)
        return 0;
    int n = 0;
    struct dirent *e;
    while ((e = readdir(dd)) != NULL)
        if (ec_file_name(e->d_name))
            n++;
    closedir(dd);
    return n;
}

static int tf_count_retired_ec(const char *dir)
{
    DIR *dd = opendir(dir);
    if (!dd)
        return 0;
    int n = 0;
    struct dirent *e;
    while ((e = readdir(dd)) != NULL)
        if (ec_retired_name(e->d_name))
            n++;
    closedir(dd);
    return n;
}

static void init_db_part_ms(const char *dir, int64_t part_ms)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.partition_ms = part_ms;
    o.partition_classes = 0;
    o.create_volumes = 0;
    tsdb *db = NULL;
    CHECK(tsdb_open(dir, &o, &db) == 0 && db != NULL);
    if (db)
        tsdb_close(db);
}

/* R15 r15-follow (D13): read-only chunk store open (no flock, no EPOCH write, no tmp cleanup, no delete, store_id &
 * db_id verified), ecpart_follower_open through the follower's own eccache, snapshot survival across settle and drop
 * within obsolete_part_lifetime_s, and follower startup refusals for missing/wrong-id/foreign-db stores.
 * Kills planted mutants `followflock` and `followdel`. */
static void t_follower(void)
{
    char base[PATH_MAX + 32];
    snprintf(base, sizeof base, "%s/r15follow", S);
    fresh(base);
    no_tier_env();
    tier_health_test_opts(0, -1, 0, 0);

    /* (1) Low-level read-only store & backend contract. */
    {
        char db1[PATH_MAX + 48], st[3][PATH_MAX + 64], tmpf[PATH_MAX + 96], epf[PATH_MAX + 96];
        snprintf(db1, sizeof db1, "%s/ro_db", base);
        for (int i = 0; i < 3; i++)
            snprintf(st[i], sizeof st[i], "%s/ro_st%d", base, i);
        init_empty_db(db1);
        for (int i = 0; i < 3; i++)
            CHECK(tsdb_tier_add_dir(db1, st[i], "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);

        int dfd = open(db1, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        tiers_view tv;
        uint8_t db1_id[16], wrong_id[16];
        bool has_db = false;
        CHECK(dfd >= 0 && tiers_load(dfd, false, false, &tv) == 0 && tv.nstores == 3);
        CHECK(tiers_db_id_read(dfd, &has_db, db1_id) == 0 && has_db);
        close(dfd);
        memcpy(wrong_id, db1_id, 16);
        wrong_id[0] ^= 0xff;

        /* Open writer (holds exclusive flock on all 3 stores) and write an object. */
        direct w;
        CHECK(direct_open(db1, 0, &w) == 0 && w.t && w.db);
        if (w.t && w.db) {
            CHECK(put_obj_km(&w, OID, 2, 1) == 0);
            /* Create a stray tmp_* file in s0/ and snapshot EPOCH bytes before read-only open. */
            snprintf(tmpf, sizeof tmpf, "%s/s0/tmp_keep_me", st[0]);
            int tfd = open(tmpf, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            CHECK(tfd >= 0);
            if (tfd >= 0) {
                CHECK(write(tfd, "keep", 4) == 4);
                close(tfd);
            }
            snprintf(epf, sizeof epf, "%s/s0/EPOCH", st[0]);
            uint8_t ep_before[8192];
            int efd = open(epf, O_RDONLY);
            CHECK(efd >= 0 && pread(efd, ep_before, sizeof ep_before, 0) == (ssize_t)sizeof ep_before);
            if (efd >= 0)
                close(efd);

            /* Read-only open succeeds while writer holds flock (kills followflock). */
            chunk_store *rost = NULL;
            CHECK(chunk_store_open_flags(st[0], tv.stores[0].id, db1_id, 0x2u, &rost) == TSDB_ERR_INVALID && !rost);
            CHECK(chunk_store_open_flags(st[0], tv.stores[0].id, db1_id, CHUNK_OPEN_RDONLY, &rost) == 0 && rost);
            if (rost) {
                /* Find which position of OID is placed on store 0 and read it. */
                unsigned p0 = 0;
                for (unsigned p = 0; p < 3; p++)
                    if (chunk_be_place(tier_backend(w.t), OID, p) == 0)
                        p0 = p;
                chunk_ident cid = {.volume = 0, .pos = (uint16_t)p0};
                memcpy(cid.vuuid, tier_vuuid(w.db), 16);
                memcpy(cid.oid, OID, 16);
                chunk_rd *rd = NULL;
                CHECK(chunk_rd_open(rost, &cid, &rd) == 0 && rd != NULL);
                if (rd)
                    chunk_rd_close(rd);
                /* No tmp_* cleanup, no EPOCH write, all mutating calls refused with TSDB_ERR_READONLY. */
                CHECK(access(tmpf, F_OK) == 0);
                CHECK(chunk_store_raise(rost, 0, 99) == TSDB_ERR_READONLY);
                CHECK(chunk_store_del(rost, &cid, 1) == TSDB_ERR_READONLY);
                chunk_geom g;
                CHECK(chunk_geom_make(&g, 2, 1, 512, sizeof OBJ) == 0);
                chunk_wr *wr = NULL;
                CHECK(chunk_wr_open(rost, &cid, &g, 1, &wr) == TSDB_ERR_READONLY && !wr);
                uint8_t ep_after[8192];
                efd = open(epf, O_RDONLY);
                CHECK(efd >= 0 && pread(efd, ep_after, sizeof ep_after, 0) == (ssize_t)sizeof ep_after);
                if (efd >= 0)
                    close(efd);
                CHECK(memcmp(ep_before, ep_after, sizeof ep_before) == 0);
                chunk_store_close(rost);
            }
            /* Wrong store_id and wrong db_id are refused on read-only open. */
            CHECK(chunk_store_open_flags(st[0], wrong_id, db1_id, CHUNK_OPEN_RDONLY, &rost) == CHUNK_ERR_STORE_ID &&
                  !rost);
            CHECK(chunk_store_open_flags(st[0], tv.stores[0].id, wrong_id, CHUNK_OPEN_RDONLY, &rost) ==
                      CHUNK_ERR_STORE_ID &&
                  !rost);

            /* Read-only backend works while writer holds flock and refuses all writes/deletes/epoch raises. */
            const char *paths[3] = {st[0], st[1], st[2]};
            uint8_t ids[48];
            for (int i = 0; i < 3; i++)
                memcpy(ids + 16 * i, tv.stores[i].id, 16);
            chunk_backend *robe = NULL;
            chunk_be_opts bo = {.min_up = 2, .read_only = true, .strict = true};
            CHECK(chunk_be_open2(&robe, paths, 3, ids, &bo) == 0 && robe != NULL);
            if (robe) {
                chunk_obj *ob = NULL;
                CHECK(chunk_obj_open(robe, 0, tier_vuuid(w.db), OID, NULL, NULL, &ob) == 0 && ob != NULL);
                if (ob) {
                    uint8_t back[sizeof OBJ];
                    CHECK(chunk_obj_read(ob, 0, sizeof back, back, 0, NULL) == 0 &&
                          memcmp(back, OBJ, sizeof OBJ) == 0);
                    chunk_obj_close(ob);
                }
                chunk_ident cid = {.volume = 0, .pos = 0};
                memcpy(cid.vuuid, tier_vuuid(w.db), 16);
                memcpy(cid.oid, OID, 16);
                chunk_geom g;
                CHECK(chunk_geom_make(&g, 2, 1, 512, sizeof OBJ) == 0);
                CHECK(chunk_be_see_epoch(robe, 0, 5) == TSDB_ERR_READONLY);
                CHECK(chunk_be_del(robe, &cid, 1) == TSDB_ERR_READONLY);
                CHECK(chunk_be_put_object(robe, 0, tier_vuuid(w.db), OID, &g, 1, OBJ, ~0u, NULL) == TSDB_ERR_READONLY);
                CHECK(chunk_be_heal(robe, 0, 0, NULL, NULL) == TSDB_ERR_READONLY);
                CHECK(chunk_be_repair(robe, &cid, NULL, NULL) == TSDB_ERR_READONLY);
                CHECK(chunk_be_settle(robe, 0, NULL, NULL) == TSDB_ERR_READONLY);
                CHECK(chunk_be_attach(robe, 0, paths[0], NULL, NULL, NULL) == TSDB_ERR_READONLY);
                CHECK(chunk_be_replace(robe, 0, paths[0], NULL, NULL, NULL) == TSDB_ERR_READONLY);
                chunk_be_close(robe);
            }
            direct_close(&w);

            /* Even on read-only directory/file permissions (simulating ro NFS export), read-only open reads cleanly. */
            if (geteuid() != 0) {
                char s0d[PATH_MAX + 80];
                snprintf(s0d, sizeof s0d, "%s/s0", st[0]);
                int c1 = chmod(epf, 0444);
                int c2 = chmod(s0d, 0555);
                int c3 = chmod(st[0], 0555);
                chunk_backend *robe2 = NULL;
                int ro_rc = (c1 == 0 && c2 == 0 && c3 == 0) ? chunk_be_open2(&robe2, paths, 3, ids, &bo) : -1;
                if (robe2)
                    chunk_be_close(robe2);
                int r1 = chmod(st[0], 0755);
                int r2 = chmod(s0d, 0755);
                int r3 = chmod(epf, 0644);
                CHECK(c1 == 0 && c2 == 0 && c3 == 0);
                CHECK(ro_rc == 0 && robe2 != NULL);
                CHECK(r1 == 0 && r2 == 0 && r3 == 0);
            }
            unlink(tmpf);
        }
        tiers_view_free(&tv);
    }

    /* (2) Follower opens BEFORE writer starts (kills followflock in follower-first order, tests lazy vuuid pickup),
     *     then writer inserts, settles across root + volume 1, and follower + plain read-only open match writer. */
    {
        char db2[PATH_MAX + 48], vol1[PATH_MAX + 48], st[3][PATH_MAX + 64];
        snprintf(db2, sizeof db2, "%s/e2e_db", base);
        snprintf(vol1, sizeof vol1, "%s/e2e_v1", base);
        for (int i = 0; i < 3; i++)
            snprintf(st[i], sizeof st[i], "%s/e2e_st%d", base, i);
        init_db_part_ms(db2, 1000);
        for (int i = 0; i < 3; i++)
            CHECK(tsdb_tier_add_dir(db2, st[i], "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);

        /* Open follower FIRST (before writer has written vuuid= or requires=tier to META). */
        tsdb_options fo;
        tsdb_options_init(&fo);
        fo.follow = 1;
        fo.refresh_ms = 50;
        tsdb *f = NULL;
        CHECK(tsdb_open(db2, &fo, &f) == 0 && f != NULL);

        /* Now open writer while follower is open (kills followflock!). */
        setenv("TSDB_TEST_TIER", "grace_s=0,keep_s=3600", 1);
        setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);
        int prev_poll = atomic_exchange(&settle_poll_ms, 50);
        tsdb_options wo;
        tsdb_options_init(&wo);
        wo.merge_threads = 0;
        wo.partition_ms = 1000;
        wo.partition_classes = 0;
        wo.obsolete_part_lifetime_s = 60;
        wo.flush_interval_ms = 50;
        tsdb *w = NULL;
        CHECK(tsdb_open(db2, &wo, &w) == 0 && w != NULL);
        if (w && f) {
            uint32_t vidx = 0;
            CHECK(tsdb_volume_add(w, vol1, &vidx) == 0 && vidx == 1);
            tsdb_sid s0 = 0, s1 = 0;
            CHECK(tsdb_series_create(w, "tf.s0", TSDB_FLOAT32, NULL, &s0) == 0);
            CHECK(tsdb_series_create(w, "tf.s1", TSDB_FLOAT32, vol1, &s1) == 0);
            int64_t ts[200];
            float vals[200];
            for (int p = 0; p < 3; p++) {
                for (int i = 0; i < 200; i++)
                    ts[i] = (int64_t)p * 1000 + i * 4, vals[i] = (float)(p * 1000 + i);
                CHECK(tsdb_insert_series(w, s0, ts, vals, 200) == 0);
            }
            for (int i = 0; i < 200; i++)
                ts[i] = i * 4, vals[i] = (float)(5000 + i);
            CHECK(tsdb_insert_series(w, s1, ts, vals, 200) == 0);
            CHECK(tsdb_flush(w) == 0);

            /* Wait for eager settle thread to produce .ec parts in both root and volume 1. */
            int64_t dl = rh_now_ms() + 10000;
            while ((tf_count_ec_files(db2) < 3 || tf_count_ec_files(vol1) < 1) && rh_now_ms() < dl)
                rh_sleep_ms(30);
            CHECK(tf_count_ec_files(db2) >= 3 && tf_count_ec_files(vol1) >= 1);

            /* Wait for follower to refresh and see both series and all settled rows. */
            tsdb_sid fs0 = 0, fs1 = 0;
            dl = rh_now_ms() + 10000;
            while (rh_now_ms() < dl) {
                (void)tsdb_refresh(f);
                if (tsdb_series_lookup(f, "tf.s0", &fs0) == 0 && tsdb_series_lookup(f, "tf.s1", &fs1) == 0) {
                    rh_sum cs0 = {0, 0}, cs1 = {0, 0};
                    if (tf_query_sum(f, fs0, 0, 3000, &cs0) == 0 && cs0.n == 600 &&
                        tf_query_sum(f, fs1, 0, 1000, &cs1) == 0 && cs1.n == 200)
                        break;
                }
                rh_sleep_ms(30);
            }
            rh_sum ws0, ws1, fs0_res, fs1_res;
            CHECK(tf_query_sum(w, s0, 0, 3000, &ws0) == 0 && ws0.n == 600);
            CHECK(tf_query_sum(w, s1, 0, 1000, &ws1) == 0 && ws1.n == 200);
            CHECK(tsdb_series_lookup(f, "tf.s0", &fs0) == 0 && tsdb_series_lookup(f, "tf.s1", &fs1) == 0);
            CHECK(tf_query_sum(f, fs0, 0, 3000, &fs0_res) == 0 && fs0_res.n == ws0.n && fs0_res.sum == ws0.sum);
            CHECK(tf_query_sum(f, fs1, 0, 1000, &fs1_res) == 0 && fs1_res.n == ws1.n && fs1_res.sum == ws1.sum);

            /* Plain read-only open (follow = 0) also reads settled .ec parts cleanly while writer is open. */
            no_tier_env();
            tsdb *ro = NULL;
            CHECK(open_db(db2, &ro, 1) == 0 && ro != NULL);
            if (ro) {
                tsdb_sid rs0 = 0, rs1 = 0;
                rh_sum r0 = {0, 0}, r1 = {0, 0};
                CHECK(tsdb_series_lookup(ro, "tf.s0", &rs0) == 0 && tsdb_series_lookup(ro, "tf.s1", &rs1) == 0);
                CHECK(tf_query_sum(ro, rs0, 0, 3000, &r0) == 0 && r0.n == ws0.n && r0.sum == ws0.sum);
                CHECK(tf_query_sum(ro, rs1, 0, 1000, &r1) == 0 && r1.n == ws1.n && r1.sum == ws1.sum);
                tsdb_close(ro);
            }
        }
        if (f)
            tsdb_close(f);
        if (w)
            tsdb_close(w);
        no_tier_env();
        atomic_store(&settle_poll_ms, prev_poll);
    }

    /* (3) Snapshot survival across settle + drop within obsolete_part_lifetime_s (kills followdel),
     *     follower's own eccache, retired_ fallback in ecpart_follower_open, and cleanup after lifetime. */
    char db3[PATH_MAX + 48], st3[3][PATH_MAX + 64];
    snprintf(db3, sizeof db3, "%s/snap_db", base);
    for (int i = 0; i < 3; i++)
        snprintf(st3[i], sizeof st3[i], "%s/snap_st%d", base, i);
    init_db_part_ms(db3, 1000);
    for (int i = 0; i < 3; i++)
        CHECK(tsdb_tier_add_dir(db3, st3[i], "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);

    {
        tsdb_options wo, fo;
        tsdb_options_init(&wo);
        wo.merge_threads = 0;
        wo.partition_ms = 1000;
        wo.partition_classes = 0;
        wo.obsolete_part_lifetime_s = 60;
        wo.flush_interval_ms = 50;
        wo.create_volumes = 0;

        direct wd = {0}, fd = {0};
        CHECK(tier_open(db3, &wo, &wd.t) == 0 && wd.t != NULL);
        if (wd.t) {
            tier_want(wd.t, 0);
            CHECK(eng_open(db3, &wo, &wd.db) == 0 && wd.db != NULL);
            tier_want(NULL, 0);
        }
        uint32_t sid = 0;
        if (wd.db) {
            CHECK(eng_series(wd.db, "snap.s", TSDB_FLOAT32, &sid) == 0);
            int64_t ts[200];
            float vals[200];
            for (int p = 0; p < 3; p++) {
                for (int i = 0; i < 200; i++)
                    ts[i] = (int64_t)p * 1000 + i * 4, vals[i] = (float)(p * 1000 + i);
                CHECK(eng_insert_series(wd.db, TSDB_FLOAT32, sid, ts, vals, 200) == 0);
            }
            CHECK(eng_flush(wd.db) == 0);
            /* Settle partition 0 only; partitions 1 and 2 remain .part files for now. */
            CHECK(rh_settle_part(wd.db, 0) == 0);
            CHECK(tf_count_ec_files(db3) == 1);

            /* Open follower with manual refresh (refresh_ms huge). */
            tsdb_options_init(&fo);
            fo.read_only = 1;
            fo.follow = 1;
            fo.refresh_ms = 1000000;
            fo.create_volumes = 0;
            CHECK(tier_open(db3, &fo, &fd.t) == 0 && fd.t != NULL);
            if (fd.t) {
                CHECK(fd.t->read_only && !tier_writable(fd.t) && fd.t->gc == NULL && fd.t->settle == NULL);
                tier_want(fd.t, 0);
                CHECK(eng_open(db3, &fo, &fd.db) == 0 && fd.db != NULL);
                tier_want(NULL, 0);
            }
        }
        if (wd.db && fd.db) {
            /* Follower reads partition 0 (.ec) through its OWN eccache. */
            eccache_test_drop(fd.t);
            eccache_counters cc0, cc1;
            eccache_get_counters(fd.t, &cc0);
            rh_sum got0 = {0, 0};
            CHECK(eng_query_raw_ex(fd.db, sid, 0, 1000, false, 0, rh_sum_cb, &got0) == 0 && got0.n == 200 &&
                  (int64_t)(got0.sum + 0.5) == 19900);
            eccache_get_counters(fd.t, &cc1);
            CHECK(cc1.obj_opens > cc0.obj_opens && cc1.misses > cc0.misses);

            /* Hold a snapshot on the follower while writer settles partition 1 AND drops partition 0! */
            size_t nsnap = 0;
            part **snap = db_snapshot(fd.db, INT64_MIN, INT64_MAX, &nsnap);
            CHECK(snap != NULL && nsnap == 3);
            int64_t pk1 = wd.db->cls_kmul;
            CHECK(rh_settle_part(wd.db, pk1) == 0);
            CHECK(eng_drop_before(wd.db, 1000) == 0);
            CHECK(tf_count_retired_ec(db3) == 1);

            /* Drive writer's graveyard purge + synchronous GC worker: within obsolete_part_lifetime_s (60 s), chunks
             * MUST stay! (Planted mutant `followdel` ignores ge->deadline and deletes p0's chunks right here.) */
            graveyard_purge(wd.db, false);
            ecgc_tier_work(wd.t);
            graveyard_purge(wd.db, false);
            ecgc_tier_work(wd.t);

            /* Drop follower's eccache so it MUST fetch p0's chunks from disk (not from cache). */
            eccache_test_drop(fd.t);
            rh_sum sp0 = {0, 0}, sp1 = {0, 0};
            CHECK(eng_query_raw_ex(fd.db, sid, 0, 1000, false, 0, rh_sum_cb, &sp0) == 0 && sp0.n == 200 &&
                  (int64_t)(sp0.sum + 0.5) == 19900);
            CHECK(eng_query_raw_ex(fd.db, sid, 1000, 2000, false, 0, rh_sum_cb, &sp1) == 0 && sp1.n == 200 &&
                  (int64_t)(sp1.sum + 0.5) == 219900);

            /* Also verify ecpart_follower_open falls back to retired_<name>.ec if writer just retired it. */
            char ret_ec[128] = "";
            DIR *dd = opendir(db3);
            if (dd) {
                struct dirent *de;
                while ((de = readdir(dd)) != NULL)
                    if (ec_retired_name(de->d_name)) {
                        snprintf(ret_ec, sizeof ret_ec, "%s", de->d_name + 8);
                        break;
                    }
                closedir(dd);
            }
            CHECK(ret_ec[0] != '\0');
            if (ret_ec[0]) {
                part *rp = NULL;
                CHECK(ecpart_follower_open(fd.db, ret_ec, &rp) == 0 && rp != NULL);
                if (rp)
                    part_unref(rp);
            }

            db_release(snap, nsnap);

            /* Follower refreshes: p0 is gone, p1 is now .ec and reads cleanly via follower's eccache. */
            CHECK(eng_refresh(fd.db) == 0);
            eccache_test_drop(fd.t);
            rh_sum af0 = {0, 0}, af1 = {0, 0};
            CHECK(eng_query_raw_ex(fd.db, sid, 0, 1000, false, 0, rh_sum_cb, &af0) == 0 && af0.n == 0);
            CHECK(eng_query_raw_ex(fd.db, sid, 1000, 2000, false, 0, rh_sum_cb, &af1) == 0 && af1.n == 200 &&
                  (int64_t)(af1.sum + 0.5) == 219900);
        }
        direct_close(&fd);
        if (wd.t)
            tier_quiesce(wd.t);
        direct_close(&wd);

        /* Reopen writer with 1 s lifetime: verify retired_p0.ec is cleaned up once the lifetime expires.
         * ("Still kept before deadline" was already asserted above under the 60 s lifetime, immune to load stalls.) */
        if (ecgc_mutant != GC_MUT_FOLLOWDEL && chunk_mutant != CHUNK_MUT_FOLLOW_FLOCK) {
            wo.obsolete_part_lifetime_s = 1;
            CHECK(tier_open(db3, &wo, &wd.t) == 0 && wd.t != NULL);
            if (wd.t) {
                tier_want(wd.t, 0);
                CHECK(eng_open(db3, &wo, &wd.db) == 0 && wd.db != NULL);
                tier_want(NULL, 0);
            }
            if (wd.db) {
                int64_t dl = rh_now_ms() + 10000;
                while (tf_count_retired_ec(db3) > 0 && rh_now_ms() < dl) {
                    graveyard_purge(wd.db, false);
                    ecgc_tier_work(wd.t);
                    rh_sleep_ms(50);
                }
                CHECK(tf_count_retired_ec(db3) == 0); /* cleaned up once lifetime expired */
            }
            if (wd.t)
                tier_quiesce(wd.t);
            direct_close(&wd);
        }
    }

    /* (4) Follower store refusals: missing non-LOST store, wrong store_id, foreign db_id, unbound store,
     *     LOST store decode-around, and too many LOST stores. */
    {
        char st1_bak[PATH_MAX + 80];
        snprintf(st1_bak, sizeof st1_bak, "%s/snap_st1.bak", base);
        int dfd = open(db3, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        tiers_view tv;
        uint8_t db3_id[16], other_db_id[16], s1_id[16], s2_id[16];
        bool has3 = false;
        CHECK(dfd >= 0 && tiers_load(dfd, false, false, &tv) == 0 && tv.nstores == 3);
        CHECK(tiers_db_id_read(dfd, &has3, db3_id) == 0 && has3);
        memcpy(s1_id, tv.stores[1].id, 16);
        memcpy(s2_id, tv.stores[2].id, 16);
        memcpy(other_db_id, db3_id, 16);
        other_db_id[0] ^= 0x55;
        tiers_view_free(&tv);

        tsdb_options fo;
        tsdb_options_init(&fo);
        fo.follow = 1;
        tsdb *f = NULL;

        /* 4a. Missing non-LOST store -> refused naming the store. */
        CHECK(rename(st3[1], st1_bak) == 0);
        cap_begin();
        int rc = tsdb_open(db3, &fo, &f);
        const char *err = cap_end();
        CHECK(rc == TSDB_ERR_NOTFOUND && !f && strstr(err, st3[1]) != NULL);
        if (f) {
            tsdb_close(f);
            f = NULL;
        }

        /* 4b. Wrong store_id (same db_id) -> refused naming the store. */
        chunk_store_owner ow_same = {.db_set = true};
        memcpy(ow_same.db_id, db3_id, 16);
        CHECK(chunk_store_init2(st3[1], NULL, &ow_same, NULL) == 0);
        cap_begin();
        rc = tsdb_open(db3, &fo, &f);
        err = cap_end();
        CHECK(rc == TSDB_ERR_INVALID && !f && strstr(err, st3[1]) != NULL);
        if (f) {
            tsdb_close(f);
            f = NULL;
        }
        sh("rm -rf '%s'", st3[1]);

        /* 4c. Foreign db_id (matching store_id) -> refused naming store and BOTH db_ids. */
        chunk_store_owner ow_other = {.db_set = true};
        memcpy(ow_other.db_id, other_db_id, 16);
        CHECK(chunk_store_init2(st3[1], s1_id, &ow_other, NULL) == 0);
        char hex_want[33], hex_have[33];
        chunk_oid_hex(db3_id, hex_want);
        chunk_oid_hex(other_db_id, hex_have);
        cap_begin();
        rc = tsdb_open(db3, &fo, &f);
        err = cap_end();
        CHECK(rc == TSDB_ERR_INVALID && !f && strstr(err, st3[1]) != NULL && strstr(err, hex_want) != NULL &&
              strstr(err, hex_have) != NULL);
        if (f) {
            tsdb_close(f);
            f = NULL;
        }
        sh("rm -rf '%s'", st3[1]);

        /* 4d. Unbound store (no db= line, matching store_id) -> refused naming store and expected db_id. */
        CHECK(chunk_store_init(st3[1], s1_id, NULL) == 0);
        cap_begin();
        rc = tsdb_open(db3, &fo, &f);
        err = cap_end();
        CHECK(rc == TSDB_ERR_INVALID && !f && strstr(err, st3[1]) != NULL && strstr(err, hex_want) != NULL);
        if (f) {
            tsdb_close(f);
            f = NULL;
        }
        sh("rm -rf '%s'", st3[1]);

        /* 4e. LOST store (missing from disk, marked LOST in TIERS): follower opens and decodes around it. */
        CHECK(tiers_append_state(dfd, true, s1_id, TIERS_STATE_LOST, 0, false, NULL, NULL) == 0);
        CHECK(tsdb_open(db3, &fo, &f) == 0 && f != NULL);
        if (f) {
            tsdb_sid fs = 0;
            rh_sum got = {0, 0};
            CHECK(tsdb_series_lookup(f, "snap.s", &fs) == 0);
            CHECK(tf_query_sum(f, fs, 1000, 2000, &got) == 0 && got.n == 200 && (int64_t)(got.sum + 0.5) == 219900);
            tsdb_close(f);
            f = NULL;
        }

        /* 4f. Too many LOST stores (< min_up): follower refuses with TSDB_ERR_UNAVAILABLE. */
        CHECK(tiers_append_state(dfd, true, s2_id, TIERS_STATE_LOST, 0, false, NULL, NULL) == 0);
        CHECK(tsdb_open(db3, &fo, &f) == TSDB_ERR_UNAVAILABLE && !f);
        close(dfd);
    }

    tier_health_test_opts(0, 0, 0, 0);
    printf("  follower: ro stores (no flock/EPOCH/tmp/del), follower eccache reads, snapshot across settle+drop, "
           "missing/wrong-id/foreign-db refusals, LOST decode\n");
}

/* ---------------------------------------------------------------------------------------------------------- r15-full */

static atomic_int full_relieve_hits;
static atomic_bool full_pause_entered;
static atomic_bool full_pause_release;

static void full_relieve_hook(eng *db, const part *p, uint64_t stripe)
{
    (void)db;
    (void)p;
    if (stripe == SETTLE_HOOK_PREPARED && atomic_fetch_add(&full_relieve_hits, 1) == 0)
        ecpolicy_set_test_fill_pct(85); /* simulate settle freeing fast disk below young_high_pct - 2 (88%) */
}

static void full_pause_hook(eng *db, const part *p, uint64_t stripe)
{
    (void)db;
    (void)p;
    if (stripe == SETTLE_HOOK_PREPARED) {
        atomic_store_explicit(&full_pause_entered, true, memory_order_release);
        while (!atomic_load_explicit(&full_pause_release, memory_order_acquire))
            rh_sleep_ms(10);
    }
}

/* Child under liboutagefs.so: opens two tiered volumes (vol0, vol1) on separate prefixes sharing one tier, hangs
 * vol0's directory so vol0's tierfill sampler blocks inside statvfs(vol0), and verifies:
 *   1. vol1's eng_close completes immediately (< 1 s) while vol0's sampler is hung (zero t->eng_active held across I/O);
 *   2. vol0's eng_close completes within the 500 ms detach deadline (< 1.5 s) instead of hanging pthread_join;
 *   3. tier_quiesce + tier_put complete cleanly and healing vol0 lets the detached sampler unref/free cleanly. */
static int child_full_hang(int argc, char **argv)
{
    if (argc < 6)
        return 2;
    const char *v0dir = argv[3], *v1dir = argv[4], *st0dir = argv[5];
    const char *ctl = getenv("OUTAGEFS_CTL");
    if (!ctl || !*ctl)
        return 3;
    no_tier_env();
    tier_health_test_opts(0, -1, 0, 0); /* isolate tierfill sampler I/O */
    ecpolicy_set_test_fill_pct(-1);     /* force real statvfs so outagefs hang blocks vol0's sampler */
    int prev_poll = atomic_exchange(&settle_poll_ms, 20);

    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    tier *t = NULL;
    if (tier_open(v0dir, &o, &t) != 0 || !t)
        return 4;
    eng *e0 = NULL, *e1 = NULL;
    tier_want(t, 0);
    int rc0 = eng_open(v0dir, &o, &e0);
    tier_want(NULL, 0);
    tier_want(t, 1);
    int rc1 = eng_open(v1dir, &o, &e1);
    tier_want(NULL, 0);
    if (rc0 != 0 || !e0 || rc1 != 0 || !e1) {
        if (e1)
            eng_close(e1);
        if (e0)
            eng_close(e0);
        tier_quiesce(t);
        tier_put(t);
        return 5;
    }

    /* Hang vol0's directory: vol0's tierfill sampler enters statvfs(v0dir) and blocks inside liboutagefs.so. */
    if (rh_of_cmd(ctl, "hang", v0dir) != 0)
        return 6;
    bool hung_seen = false;
    int64_t dl = rh_now_ms() + 3000;
    while (rh_now_ms() < dl) {
        char out[1024];
        if (outagefs_cmd(ctl, "state", out, sizeof out) == 0) {
            const char *p = strstr(out, v0dir);
            const char *nl = p ? strchr(p, '\n') : NULL;
            const char *h = p ? strstr(p, "hung=") : NULL;
            if (h && (!nl || h < nl) && strtoul(h + 5, NULL, 10) > 0) {
                hung_seen = true;
                break;
            }
        }
        rh_sleep_ms(15);
    }
    if (!hung_seen) {
        (void)rh_of_cmd(ctl, "heal", v0dir);
        return 7;
    }

    /* 1. Close vol1 while vol0's sampler is STILL stuck in statvfs(v0dir): MUST complete fast (no eng_active held). */
    int64_t c1_t0 = rh_now_ms();
    int c1_rc = eng_close(e1);
    int64_t c1_ms = rh_now_ms() - c1_t0;
    if (c1_rc != 0 || c1_ms > 10000) {
        fprintf(stderr, "child_full_hang: eng_close(vol1) rc=%d ms=%lld (limit 10000)\n", c1_rc, (long long)c1_ms);
        (void)rh_of_cmd(ctl, "heal", v0dir);
        return 8;
    }

    /* 2. Stop vol0's sampler while v0dir is STILL hung: tierfill_eng_stop must detach after ~500 ms instead of
     *    hanging pthread_join, then heal v0dir and close vol0/tier cleanly (detached worker drops its ref safely). */
    int64_t s0_t0 = rh_now_ms();
    tierfill_eng_stop(e0);
    int64_t s0_ms = rh_now_ms() - s0_t0;
    (void)rh_of_cmd(ctl, "heal", v0dir);
    tier_quiesce(t);
    int c0_rc = eng_close(e0);
    tier_put(t);
    rh_sleep_ms(100); /* let detached sampler wake from statvfs and drop its final worker_unref cleanly */
    if (c0_rc != 0 || s0_ms > 10000) {
        atomic_store(&settle_poll_ms, prev_poll);
        tier_health_test_opts(0, 0, 0, 0);
        fprintf(stderr, "child_full_hang: tierfill_eng_stop(vol0) ms=%lld (limit 10000) close_rc=%d\n",
                (long long)s0_ms, c0_rc);
        return 9;
    }

    /* 3. Regression: hang a tier store root (st0dir) after the 1 s fill-sample interval has elapsed; an idle tiered
     *    volume's settle thread must NOT call fstatvfs() on stores (reads cached fill_pct only), so settle rounds
     *    keep advancing and tsdb_close() finishes promptly while st0dir is still hung. */
    tsdb *sdb = NULL;
    if (open_db(v0dir, &sdb, 0) != 0 || !sdb) {
        atomic_store(&settle_poll_ms, prev_poll);
        tier_health_test_opts(0, 0, 0, 0);
        return 10;
    }
    rh_sleep_ms(1150); /* expire any 1 s store fill_sample_ms cache from open */
    if (rh_of_cmd(ctl, "hang", st0dir) != 0) {
        tsdb_close(sdb);
        atomic_store(&settle_poll_ms, prev_poll);
        tier_health_test_opts(0, 0, 0, 0);
        return 11;
    }
    uint64_t r0 = atomic_load(&settle_rounds);
    int64_t rdl = rh_now_ms() + 10000;
    while (atomic_load(&settle_rounds) < r0 + 3 && rh_now_ms() < rdl)
        rh_sleep_ms(15);
    bool rounds_ok = atomic_load(&settle_rounds) >= r0 + 3;
    int64_t sc_t0 = rh_now_ms();
    int sc_rc = tsdb_close(sdb);
    int64_t sc_ms = rh_now_ms() - sc_t0;
    (void)rh_of_cmd(ctl, "heal", st0dir);
    atomic_store(&settle_poll_ms, prev_poll);
    tier_health_test_opts(0, 0, 0, 0);
    if (!rounds_ok || sc_rc != 0 || sc_ms > 10000) {
        fprintf(stderr, "child_full_hang: hung-store close rounds_ok=%d rc=%d ms=%lld (limit 10000)\n",
                (int)rounds_ok, sc_rc, (long long)sc_ms);
        return 12;
    }
    return 0;
}

static void sleep_ms_tier(int ms)
{
    struct timespec d = {ms / 1000, (long)(ms % 1000) * 1000000L};
    nanosleep(&d, NULL);
}

static void wait_settle_rounds_tier(uint64_t delta)
{
    uint64_t target = atomic_load(&settle_rounds) + delta;
    for (int i = 0; i < 400 && atomic_load(&settle_rounds) < target; i++)
        sleep_ms_tier(10);
}

/* R15 r15-full: FULL hysteresis before ENOSPC (inserts/appends/replaces refused at >= 90%, hysteresis at 88..89%,
 * deletes/drops/queries pass, settle relieves FULL -> inserts resume); store 95% put cap (96% refuses regular puts and
 * defers settles, rebuild put passes, real fstatvfs sampled <= 1/s). Kills mutants fullnoflag and storeput96. */
static void t_full(void)
{
    char base[PATH_MAX + 32], dbdir[PATH_MAX + 48], stdir[3][PATH_MAX + 64];
    snprintf(base, sizeof base, "%s/r15full", S);
    fresh(base);
    snprintf(dbdir, sizeof dbdir, "%s/db", base);
    no_tier_env();

    /* Part A: volume FULL before ENOSPC, hysteresis, deletes/drops pass, store 96% defers settle, settle relieves FULL */
    init_empty_db(dbdir);
    for (int i = 0; i < 3; i++) {
        snprintf(stdir[i], sizeof stdir[i], "%s/st%d", base, i);
        CHECK(tsdb_tier_add_dir(dbdir, stdir[i], "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);
    }

    int prev_poll = atomic_exchange(&settle_poll_ms, 20);
    ecpolicy_set_test_fill_pct(50); /* below tier_high_pct (80) and young_high_pct (90) */
    chunk_store_set_test_fill_pct(-1);
    chunk_store_set_test_fill_path(NULL, -1);

    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    tsdb *db = NULL;
    CHECK(tsdb_open(dbdir, &o, &db) == 0 && db != NULL);
    if (db) {
        tsdb_sid s1 = 0;
        CHECK(tsdb_series_create(db, "full.s1", TSDB_FLOAT32, NULL, &s1) == 0);

        const int64_t DAY = 86400000LL;
        int64_t now_ms = (int64_t)time(NULL) * 1000LL;
        int64_t yday_ts = (now_ms / DAY - 1) * DAY + 1000; /* finished key (< 7d old): settles only at >= tier_high_pct */
        int64_t today_ts = (now_ms / DAY) * DAY + 1000;    /* active key (phi > now): never settles */

        int64_t yts[8];
        float yv[8];
        for (int i = 0; i < 8; i++)
            yts[i] = yday_ts + i * 1000, yv[i] = (float)(10 + i);
        CHECK(tsdb_insert_series(db, s1, yts, yv, 8) == 0);
        CHECK(tsdb_append(db, s1, today_ts, 1.0f) == 0);
        CHECK(tsdb_append(db, s1, today_ts + 1000, 2.0f) == 0);
        CHECK(tsdb_flush(db) == 0);

        wait_settle_rounds_tier(3);
        CHECK(!tierfill_vol_full(db, 0, NULL));

        /* Hold store 0 at 96% while raising volume fill to 90% so the settle waits (counted) and volume stays FULL. */
        chunk_store_set_test_fill_path(stdir[0], 96);
        uint64_t sdone0 = atomic_load(&settle_done);
        ecpolicy_set_test_fill_pct(90);

        bool became_full = false;
        int64_t full_since = 0;
        for (int i = 0; i < 200; i++) {
            if (tierfill_vol_full(db, 0, &full_since)) {
                became_full = true;
                break;
            }
            sleep_ms_tier(10);
        }
        /* Note: under --mutant fullnoflag, tierfill_vol_full returns false and writes below succeed (killing it). */
        CHECK(became_full && full_since > 0);

        /* Settle must be waiting because store 0 is at 96% (> 95% cap). Kills --mutant storeput96. */
        eng *e0 = tsdb_test_engine(db, 0);
        CHECK(e0 != NULL && e0->tier != NULL);
        if (e0 && e0->tier) {
            bool saw_wait = false;
            for (int i = 0; i < 200; i++) {
                if (eng_tier_settle_is_waiting(e0) && eng_tier_settles_waiting(e0) > 0 &&
                    ecsettle_tier_waiting_now(e0->tier) >= 1) {
                    saw_wait = true;
                    break;
                }
                sleep_ms_tier(10);
            }
            CHECK(saw_wait);
            CHECK(atomic_load(&settle_done) == sdone0);
        }

        /* Every insert, append, and replace route must return TSDB_ERR_UNAVAILABLE before touching the engine. */
        int64_t t1 = today_ts + 5000;
        float v1 = 99.0f;
        tsdb_values tv1 = {.type = TSDB_FLOAT32, .values = &v1, .valid = NULL};
        uint32_t fv = 99;
        CHECK(tsdb_append(db, s1, t1, v1) == TSDB_ERR_UNAVAILABLE);
        CHECK(tierfill_take_last_refused(&fv) && fv == 0);
        CHECK(tsdb_insert_series(db, s1, &t1, &v1, 1) == TSDB_ERR_UNAVAILABLE);
        CHECK(tsdb_insert(db, &s1, &t1, &v1, 1) == TSDB_ERR_UNAVAILABLE);
        CHECK(tsdb_insert_values(db, &s1, &t1, &tv1, 1) == TSDB_ERR_UNAVAILABLE);
        CHECK(tsdb_insert_series_values(db, s1, &t1, &tv1, 1) == TSDB_ERR_UNAVAILABLE);
        CHECK(tsdb_replace_range(db, s1, today_ts, today_ts + 2000, &t1, &tv1, 1) == TSDB_ERR_UNAVAILABLE);
        CHECK(tierfill_vol_refusals(db, 0) >= 6);

        /* Unknown series ID on a FULL volume must still get TSDB_ERR_INVALID, not TSDB_ERR_UNAVAILABLE. */
        tsdb_sid bogus = TSDB_SID(0, 99999u);
        CHECK(tsdb_append(db, bogus, t1, v1) == TSDB_ERR_INVALID);

        /* Verify none of the refused writes reached the engine. */
        tsdb_bucket *b = NULL;
        size_t nb = 0;
        CHECK(tsdb_query_agg(db, s1, today_ts + 4000, today_ts + 10000, 0, &b, &nb) == 0 && nb == 0);
        free(b);

        /* Hysteresis: 89% and 88% stay FULL (must drop below young_high_pct - 2 = 88%, i.e. <= 87%). */
        ecpolicy_set_test_fill_pct(89);
        wait_settle_rounds_tier(4);
        CHECK(tierfill_vol_full(db, 0, NULL));
        CHECK(tsdb_append(db, s1, t1, v1) == TSDB_ERR_UNAVAILABLE);

        ecpolicy_set_test_fill_pct(88);
        wait_settle_rounds_tier(4);
        CHECK(tierfill_vol_full(db, 0, NULL));
        CHECK(tsdb_append(db, s1, t1, v1) == TSDB_ERR_UNAVAILABLE);

        /* Deletes (including replace_range with n == 0) and drops MUST pass while FULL (D7). */
        CHECK(tsdb_delete_range(db, s1, today_ts, today_ts + 500) == 0);
        CHECK(tsdb_replace_range(db, s1, today_ts + 2000, today_ts + 2500, NULL, NULL, 0) == 0);
        CHECK(tsdb_drop_before(db, yday_ts - 10 * DAY) == 0);
        /* Queries still pass while FULL (1 row left in today's range after deleting today_ts). */
        b = NULL;
        nb = 0;
        CHECK(tsdb_query_agg(db, s1, today_ts, today_ts + 3000, 0, &b, &nb) == 0 && nb == 1 && b[0].count == 1);
        free(b);

        /* Lift the store 96% cap: yesterday's key settles, hook drops fill to 85% -> FULL clears -> inserts resume! */
        atomic_store(&full_relieve_hits, 0);
        atomic_store(&settle_fill_hook, full_relieve_hook);
        chunk_store_set_test_fill_path(NULL, -1);

        bool settled = false;
        for (int i = 0; i < 400; i++) {
            if (atomic_load(&settle_done) > sdone0 && !tierfill_vol_full(db, 0, NULL)) {
                settled = true;
                break;
            }
            sleep_ms_tier(10);
        }
        atomic_store(&settle_fill_hook, NULL);
        CHECK(settled && atomic_load(&full_relieve_hits) >= 1);
        if (e0 && e0->tier)
            CHECK(!eng_tier_settle_is_waiting(e0) && ecsettle_tier_waiting_now(e0->tier) == 0);

        CHECK(tsdb_append(db, s1, today_ts + 5000, 42.0f) == 0);
        b = NULL;
        nb = 0;
        CHECK(tsdb_query_agg(db, s1, today_ts + 4000, today_ts + 6000, 0, &b, &nb) == 0 && nb == 1 && b[0].count == 1);
        free(b);

        CHECK(tsdb_close(db) == 0);

        /* Reopening on a volume already >= young_high_pct must seed FULL synchronously before any background tick. */
        ecpolicy_set_test_fill_pct(92);
        tsdb *db2 = NULL;
        CHECK(tsdb_open(dbdir, &o, &db2) == 0 && db2 != NULL);
        if (db2) {
            CHECK(tierfill_vol_full(db2, 0, NULL));
            CHECK(tsdb_append(db2, s1, today_ts + 6000, 43.0f) == TSDB_ERR_UNAVAILABLE);
            CHECK(tsdb_close(db2) == 0);
        }
        ecpolicy_set_test_fill_pct(50);
    }

    /* Part B: Store 95% put cap (scoped to single-server tiers via chunk_be_set_put_cap) — unscoped backend is not
     * capped; capped backend passes at 95%, refuses regular puts (CHUNK_ERR_FULL) at 96%, allows rebuild puts at 96%,
     * cached fs bytes does zero statvfs syscalls, and real fstatvfs is rate-limited to <= 1/s per store. */
    {
        char sbase[PATH_MAX + 48], sp[3][PATH_MAX + 64];
        snprintf(sbase, sizeof sbase, "%s/cap", base);
        fresh(sbase);
        const char *paths[3];
        for (int i = 0; i < 3; i++) {
            snprintf(sp[i], sizeof sp[i], "%s/s%d", sbase, i);
            uint8_t id[16];
            CHECK(chunk_store_init(sp[i], NULL, id) == 0);
            paths[i] = sp[i];
        }
        chunk_backend *be = NULL;
        CHECK(chunk_be_open(&be, paths, 3, NULL) == 0 && be != NULL);
        if (be) {
            CHECK(chunk_be_see_epoch(be, 0, 1) == 0);
            uint8_t vuuid[16] = {0xaa, 0xbb, 0xcc, 1};
            uint8_t oid0[16] = {0x10, 0x20, 0x30, 0};
            uint8_t oid1[16] = {0x10, 0x20, 0x30, 1};
            uint8_t oid2[16] = {0x10, 0x20, 0x30, 2};
            chunk_geom g;
            CHECK(chunk_geom_make(&g, 2, 1, 512, sizeof OBJ) == 0);

            /* Before enabling the single-server tier cap, a plain chunk backend (e.g. cluster chunksrv) is uncapped. */
            chunk_store_set_test_fill_path(sp[0], 96);
            CHECK(chunk_be_put_object(be, 0, vuuid, oid0, &g, 1, OBJ, ~0u, NULL) == 0);

            /* Enable single-server 95% cap. */
            chunk_be_set_put_cap(be, CHUNK_STORE_PUT_MAX_PCT);

            /* At 95%: regular put succeeds. */
            chunk_store_set_test_fill_path(sp[0], 95);
            CHECK(chunk_be_put_object(be, 0, vuuid, oid1, &g, 1, OBJ, ~0u, NULL) == 0);

            /* At 96%: regular put targeting store 0 MUST be refused with CHUNK_ERR_FULL (kills storeput96). */
            chunk_store_set_test_fill_path(sp[0], 96);
            CHECK(chunk_be_put_object(be, 0, vuuid, oid2, &g, 1, OBJ, ~0u, NULL) == CHUNK_ERR_FULL);
            CHECK(chunk_be_any_store_full(be));

            /* Remove oid1's chunk on store 0 while store 0 is STILL at 96%, then rebuild via chunk_be_heal:
             * rebuild puts use chunk_wr_open_heal (heal=true) and MUST pass above 95%! */
            unsigned pos_on_0 = 0;
            for (unsigned p = 0; p < 3; p++)
                if (chunk_be_place(be, oid1, p) == 0)
                    pos_on_0 = p;
            char hex[33], cpath[PATH_MAX + 128];
            chunk_oid_hex(oid1, hex);
            snprintf(cpath, sizeof cpath, "%s/s0/o%s.%u.chunk", sp[0], hex, pos_on_0);
            CHECK(unlink(cpath) == 0);

            chunk_heal_opts ho = {.epoch = 1, .scrub = false};
            chunk_heal_report rep;
            memset(&rep, 0, sizeof rep);
            CHECK(chunk_be_heal(be, 0, 0, &ho, &rep) == 0);
            CHECK(rep.rebuilt_missing == 1 && rep.failed == 0);
            struct stat sb;
            CHECK(stat(cpath, &sb) == 0 && sb.st_size > 0);

            /* Cached fs bytes snapshot (used by metrics) must read cached values with zero fstatvfs syscalls, and
             * real fstatvfs on the put path must be rate-limited to <= 1/s per store. */
            chunk_store_set_test_fill_path(NULL, -1);
            chunk_store_set_test_fill_pct(-1);
            uint64_t u = 0, fr = 0, tot = 0;
            chunk_store *st0 = chunk_be_store(be, 0);
            uint64_t sc_before = atomic_load(&chunk_store_fill_samples);
            CHECK(st0 != NULL && chunk_store_fs_bytes_cached(st0, &u, &fr, &tot) == 0 && tot > 0);
            CHECK(atomic_load(&chunk_store_fill_samples) == sc_before);

            int64_t t_start = rh_now_ms();
            uint64_t s0_samples = atomic_load(&chunk_store_fill_samples);
            for (int i = 0; i < 20; i++) {
                uint8_t oidn[16] = {0x55, (uint8_t)i, 0, 1};
                CHECK(chunk_be_put_object(be, 0, vuuid, oidn, &g, 1, OBJ, ~0u, NULL) == 0);
            }
            uint64_t s1_samples = atomic_load(&chunk_store_fill_samples);
            int64_t elapsed_ms = rh_now_ms() - t_start;
            uint64_t max_allowed = (uint64_t)(3 * ((elapsed_ms / 1000) + 2));
            CHECK(s1_samples - s0_samples <= max_allowed && s1_samples - s0_samples < 60);

            chunk_be_close(be);
        }
    }

    /* Part C: Dedicated sampler thread flips FULL within ~1 s even while the settle thread is blocked encoding a part. */
    {
        char sbase[PATH_MAX + 48], sdb[PATH_MAX + 64], sst[3][PATH_MAX + 80];
        snprintf(sbase, sizeof sbase, "%s/sampler_blocked", base);
        fresh(sbase);
        snprintf(sdb, sizeof sdb, "%s/db", sbase);
        init_empty_db(sdb);
        for (int i = 0; i < 3; i++) {
            snprintf(sst[i], sizeof sst[i], "%s/st%d", sbase, i);
            CHECK(tsdb_tier_add_dir(sdb, sst[i], "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);
        }
        ecpolicy_set_test_fill_pct(50);
        tsdb *db = NULL;
        CHECK(tsdb_open(sdb, &o, &db) == 0 && db != NULL);
        if (db) {
            tsdb_sid s1 = 0;
            CHECK(tsdb_series_create(db, "blk.s1", TSDB_FLOAT32, NULL, &s1) == 0);
            const int64_t DAY = 86400000LL;
            int64_t now_ms = (int64_t)time(NULL) * 1000LL;
            int64_t old_ts = (now_ms / DAY - 10) * DAY + 1000; /* > 7d old: due for settle immediately */
            int64_t ots[8];
            float ov[8];
            for (int i = 0; i < 8; i++)
                ots[i] = old_ts + i * 1000, ov[i] = (float)i;
            atomic_store_explicit(&full_pause_entered, false, memory_order_release);
            atomic_store_explicit(&full_pause_release, false, memory_order_release);
            atomic_store_explicit(&settle_fill_hook, full_pause_hook, memory_order_release);
            CHECK(tsdb_insert_series(db, s1, ots, ov, 8) == 0);
            CHECK(tsdb_flush(db) == 0);
            /* Wait until the settle thread is blocked inside full_pause_hook (cannot call tierfill_update). */
            int64_t dl = rh_now_ms() + 5000;
            while (!atomic_load_explicit(&full_pause_entered, memory_order_acquire) && rh_now_ms() < dl)
                sleep_ms_tier(10);
            CHECK(atomic_load_explicit(&full_pause_entered, memory_order_acquire));
            CHECK(!tierfill_vol_full(db, 0, NULL));

            /* Raise fill to 91% while settle thread is stuck: dedicated sampler thread MUST flip FULL within ~1 s! */
            int64_t flip0 = rh_now_ms();
            ecpolicy_set_test_fill_pct(91);
            bool flipped = false;
            while (rh_now_ms() - flip0 < 1500) {
                if (tierfill_vol_full(db, 0, NULL)) {
                    flipped = true;
                    break;
                }
                sleep_ms_tier(10);
            }
            CHECK(flipped);
            CHECK(tsdb_append(db, s1, now_ms, 1.0f) == TSDB_ERR_UNAVAILABLE);

            atomic_store_explicit(&full_pause_release, true, memory_order_release);
            atomic_store_explicit(&settle_fill_hook, NULL, memory_order_release);
            CHECK(tsdb_close(db) == 0);
        }
        ecpolicy_set_test_fill_pct(50);
    }

    /* Part D: Regression for hung NFS volume isolation — under liboutagefs.so, hanging vol0's statvfs must NOT block
     * eng_close of vol1 (no t->eng_active held across statvfs) nor hang eng_close of vol0 (detached on deadline). */
    if (access(SHIM_OUTAGE, R_OK) == 0 && tierfill_mutant == TIERFILL_MUT_NONE && chunk_mutant == CHUNK_MUT_NONE) {
        char hbase[PATH_MAX + 48], v0d[PATH_MAX + 64], v1d[PATH_MAX + 64], hst[3][PATH_MAX + 80];
        char hctl[PATH_MAX + 64], hroots[3 * PATH_MAX + 240];
        snprintf(hbase, sizeof hbase, "%s/hang_iso", base);
        fresh(hbase);
        snprintf(v0d, sizeof v0d, "%s/v0", hbase);
        snprintf(v1d, sizeof v1d, "%s/v1", hbase);
        init_empty_db(v0d);
        init_empty_db(v1d);
        for (int i = 0; i < 3; i++) {
            snprintf(hst[i], sizeof hst[i], "%s/st%d", hbase, i);
            CHECK(tsdb_tier_add_dir(v0d, hst[i], "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);
        }
        snprintf(hctl, sizeof hctl, "%s/ctl", hbase);
        snprintf(hroots, sizeof hroots, "%s:%s:%s", v0d, v1d, hst[0]);
        pid_t pid = fork();
        if (pid == 0) {
            char asan[512];
            snprintf(asan, sizeof asan, "verify_asan_link_order=0:detect_leaks=0%s%.400s",
                     getenv("ASAN_OPTIONS") ? ":" : "", getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
            setenv("ASAN_OPTIONS", asan, 1);
            setenv("LD_PRELOAD", SHIM_OUTAGE, 1);
            setenv("OUTAGEFS_ROOTS", hroots, 1);
            setenv("OUTAGEFS_CTL", hctl, 1);
            execl(SELF, "test_tier", "--child", "full_hang", v0d, v1d, hst[0], (char *)NULL);
            _exit(127);
        }
        int st_w = -1;
        CHECK(pid > 0 && waitpid(pid, &st_w, 0) == pid && WIFEXITED(st_w) && WEXITSTATUS(st_w) == 0);
        if (pid > 0 && (!WIFEXITED(st_w) || WEXITSTATUS(st_w) != 0))
            fprintf(stderr, "  full: hang_iso child exited with status %d (sig %d)\n",
                    WIFEXITED(st_w) ? WEXITSTATUS(st_w) : -1, WIFSIGNALED(st_w) ? WTERMSIG(st_w) : 0);
    } else if (tierfill_mutant == TIERFILL_MUT_NONE && chunk_mutant == CHUNK_MUT_NONE) {
        printf("  full: hang_iso skipped (%s not built)\n", SHIM_OUTAGE);
    }

    ecpolicy_set_test_fill_pct(-1);
    chunk_store_set_test_fill_pct(-1);
    chunk_store_set_test_fill_path(NULL, -1);
    atomic_store(&settle_poll_ms, prev_poll);
    printf("  full: FULL hysteresis (90%%/<88%%), deletes/drops pass, settle relieves, 95%% store cap & rebuild pass, "
           "sampler isolation\n");
}

/* Helper for tests/test_config.sh: --mk-tier-db DBDIR STORE[:CLASS]... (creates DB and adds stores to TIERS). */
static int mk_tier_db(int argc, char **argv)
{
    if (argc < 4)
        return 2;
    const char *dbdir = argv[2];
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.stats = -1;
    o.latest_cache = 0;
    tsdb *db = NULL;
    if (tsdb_open(dbdir, &o, &db) != 0)
        return 1;
    tsdb_close(db);
    for (int i = 3; i < argc; i++) {
        char path[PATH_MAX];
        const char *spec = argv[i], *slash = strrchr(spec, '/'), *colon = strrchr(spec, ':');
        const char *cls = NULL;
        size_t plen = strlen(spec);
        if (colon && (!slash || colon > slash)) {
            plen = (size_t)(colon - spec);
            cls = colon + 1;
        }
        if (plen >= sizeof path)
            return 2;
        memcpy(path, spec, plen);
        path[plen] = '\0';
        if (tsdb_tier_add_dir(dbdir, path, cls, TSDB_TIER_ALLOW_SHARED_DEV) != 0)
            return 1;
    }
    return 0;
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
    /* the objects here are put straight into the stores, named by no .ec file: a reopen's sweep (R7 D6) would delete
     * them as orphans (eio reads one back after a reopen); test_ecgc tests the sweep. Before the child too. */
    atomic_store(&ecgc_test_nosweep, true);
    for (size_t i = 0; i < sizeof OBJ; i++) /* before the child too: it compares what it reads with OBJ */
        OBJ[i] = (uint8_t)(i * 131 + 7);
#ifdef TSDB_EC
    if (argc > 1 && strcmp(argv[1], "--child") == 0)
        return child(argc, argv);
    if (argc > 1 && strcmp(argv[1], "--mk-tier-db") == 0)
        return mk_tier_db(argc, argv);
#else
    if (argc > 1 && strcmp(argv[1], "--mk-tier-db") == 0)
        return 77;
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
        else if (strcmp(mutant, "selfepoch") == 0)
            tier_mutant = TIER_MUT_SELFEPOCH;
        else if (strcmp(mutant, "dbidskip") == 0)
            tier_mutant = TIER_MUT_DBIDSKIP;
        else if (strcmp(mutant, "lostopen") == 0)
            tier_mutant = TIER_MUT_LOSTOPEN;
        else if (strcmp(mutant, "downnoread") == 0) {
            tier_mutant = TIER_MUT_DOWNNOREAD;
            chunk_mutant = CHUNK_MUT_DOWNNOREAD;
        } else if (strcmp(mutant, "attachnoheal") == 0) {
            tier_mutant = TIER_MUT_ATTACHNOHEAL;
            chunk_mutant = CHUNK_MUT_ATTACHNOHEAL;
        } else if (strcmp(mutant, "fullnoflag") == 0) {
            tierfill_mutant = TIERFILL_MUT_FULLNOFLAG;
        } else if (strcmp(mutant, "storeput96") == 0) {
            chunk_mutant = CHUNK_MUT_STOREPUT96;
        } else if (strcmp(mutant, "followflock") == 0) {
            tier_mutant = TIER_MUT_FOLLOWFLOCK;
            chunk_mutant = CHUNK_MUT_FOLLOW_FLOCK;
        } else if (strcmp(mutant, "followdel") == 0) {
            tier_mutant = TIER_MUT_FOLLOWDEL;
            ecgc_mutant = GC_MUT_FOLLOWDEL;
        } else {
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
    snprintf(SHIM_OUTAGE, sizeof SHIM_OUTAGE, "%.*s/liboutagefs.so", slash ? (int)(slash - SELF) : 1,
             slash ? SELF : ".");
    fresh(S);
    printf("test_tier%s%s\n", mutant ? " --mutant " : "", mutant ? mutant : "");
    t_noec();
#ifdef TSDB_EC
    t_volumes();
    t_down();
    t_lost();
    t_fence();
    t_unsure();
    t_ops();
    t_refuse();
    t_reval();
    t_power(false);
    t_power(true);
    t_eio();
    t_tiers_open();
    t_tiers_defaults();
    t_tiers_settings();
    t_follower();
    t_runtime_health();
    t_full();
#else
    printf("  volumes, down, lost, fence, unsure, ops, refuse, reval, power, eio, tiers_open, tiers_defaults, "
           "tiers_settings, follower, runtime_health, full: skipped (no TSDB_EC in this build)\n");
#endif
    t_follow();
    if (!fails)
        sh("rm -rf '%s'", S);
    printf("%s\n", fails ? "test_tier: FAILED" : "test_tier: ok");
    return fails ? 1 : 0;
}
