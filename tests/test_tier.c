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
#include "feed.h"
#include "tier.h"
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

    /* Read-only / follower open on a tiered DB (root META has requires=tier) is refused. */
    tsdb *db = NULL;
    CHECK(open_db(dbdir, &db, 1) == TSDB_ERR_INVALID && !db);
    tsdb_options fo;
    tsdb_options_init(&fo);
    fo.follow = 1;
    CHECK(tsdb_open(dbdir, &fo, &db) == TSDB_ERR_INVALID && !db);

    printf("  tiers_settings: per-volume SETTINGS, cache_bytes, TSDB_TEST_TIER rules, ro/follow refusal\n");
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
    t_ops();
    t_refuse();
    t_reval();
    t_power(false);
    t_power(true);
    t_eio();
    t_tiers_open();
    t_tiers_defaults();
    t_tiers_settings();
#else
    printf("  volumes, down, lost, fence, unsure, ops, refuse, reval, power, eio, tiers_open, tiers_defaults, "
           "tiers_settings: skipped (no TSDB_EC in this build)\n");
#endif
    t_follow();
    if (!fails)
        sh("rm -rf '%s'", S);
    printf("%s\n", fails ? "test_tier: FAILED" : "test_tier: ok");
    return fails ? 1 : 0;
}
