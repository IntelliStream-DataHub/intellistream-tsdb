/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The tier of a tsdb handle (src/tier.h has the design). r7-iface: the configuration's parser (pure), the decision
 * of TSDB_TEST_TIER / TSDB_TEST_TIER_I_KNOW, a tier that is a reference count around its configuration, and the
 * engine's attach and detach, which call each owner's hooks (src/tier.h). r7-tier adds the backend (chunk_be_open2,
 * epoch 1, vuuid=, requires=tier and the refusals of D1 and D7) here; r7-seam's cache, r7-gc's queue and r7-settle's
 * thread live in their own files, behind the hooks, never here.
 */
#include "tier.h"
#include "chunk.h"
#include "chunk_priv.h"
#include "eccache.h"
#include "feed.h"
#include "ecgc.h"
#include "eckeep.h"
#include "ecpart.h"
#include "ecswap.h"
#include "settle.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/random.h>
#include <unistd.h>

int r7_mutant;
int tier_mutant;

static void tier_halt(tier *t, int i, const char *why);

void tier_cfg_free(tier_cfg *c)
{
    if (!c)
        return;
    for (int i = 0; i < c->ndirs; i++)
        free(c->dirs[i]);
    free(c->dirs);
    memset(c, 0, sizeof *c);
}

/* A decimal number of [lo, hi] spanning exactly [s, e): no sign, no blanks, no leading "0x" */
static bool tier_num(const char *s, const char *e, uint64_t lo, uint64_t hi, uint64_t *out)
{
    if (s == e || e - s > 20)
        return false;
    uint64_t v = 0;
    for (const char *q = s; q < e; q++) {
        if (*q < '0' || *q > '9')
            return false;
        unsigned d = (unsigned)(*q - '0');
        if (v > (UINT64_MAX - d) / 10)
            return false;
        v = v * 10 + d;
    }
    if (v < lo || v > hi)
        return false;
    *out = v;
    return true;
}

#define TIER_ERR(...) (snprintf(err, errlen, __VA_ARGS__), rc = TSDB_ERR_INVALID)

static int tier_dirs(const char *s, const char *e, tier_cfg *c, char *err, size_t errlen)
{
    int rc = 0;
    if (s == e)
        return TIER_ERR("dirs is empty");
    for (const char *p = s; p <= e && rc == 0;) {
        const char *q = memchr(p, ':', (size_t)(e - p));
        if (!q)
            q = e;
        size_t len = (size_t)(q - p);
        if (len == 0)
            return TIER_ERR("dirs has an empty path");
        if (*p != '/')
            return TIER_ERR("dirs: %.*s is not an absolute path", (int)len, p);
        if (len >= PATH_MAX)
            return TIER_ERR("dirs: a path is longer than PATH_MAX");
        if (c->ndirs == TIER_DIRS_MAX)
            return TIER_ERR("dirs names more than %d stores", TIER_DIRS_MAX);
        for (int i = 0; i < c->ndirs; i++)
            if (strlen(c->dirs[i]) == len && memcmp(c->dirs[i], p, len) == 0)
                return TIER_ERR("dirs names %.*s twice", (int)len, p);
        if (!c->dirs && !(c->dirs = calloc(TIER_DIRS_MAX, sizeof *c->dirs)))
            return TSDB_ERR_NOMEM;
        if (!(c->dirs[c->ndirs] = strndup(p, len)))
            return TSDB_ERR_NOMEM;
        c->ndirs++;
        if (q == e)
            break;
        p = q + 1;
        if (p == e)
            return TIER_ERR("dirs ends with ':'");
    }
    return rc;
}

int tier_cfg_parse(const char *spec, tier_cfg *c, char *err, size_t errlen)
{
    char dummy[1];
    if (!err || !errlen)
        err = dummy, errlen = sizeof dummy;
    err[0] = '\0';
    memset(c, 0, sizeof *c);
    c->grace_s = TIER_GRACE_S_DEFAULT;
    c->keep_s = TIER_KEEP_S_DEFAULT;
    c->coarse_s = TIER_COARSE_S_DEFAULT;
    int rc = 0;
    enum { K_K, K_M, K_DIRS, K_GRACE, K_KEEP, K_COARSE, K_CACHE, K_EAGER, K_N };
    static const char *const keys[K_N] = {"k", "m", "dirs", "grace_s", "keep_s", "coarse_s", "cache_mb", "eager"};
    bool seen[K_N] = {false};
    if (!spec || !*spec) {
        TIER_ERR("the setting is empty");
        goto out;
    }
    for (const char *p = spec; rc == 0;) {
        const char *end = strchr(p, ',');
        if (!end)
            end = p + strlen(p);
        const char *eq = memchr(p, '=', (size_t)(end - p));
        if (!eq) {
            TIER_ERR("'%.*s' is not key=value", (int)(end - p), p);
            break;
        }
        int k = 0;
        while (k < K_N && !(strlen(keys[k]) == (size_t)(eq - p) && memcmp(keys[k], p, (size_t)(eq - p)) == 0))
            k++;
        if (k == K_N) {
            TIER_ERR("unknown key '%.*s'", (int)(eq - p), p);
            break;
        }
        if (seen[k]) {
            TIER_ERR("%s is given twice", keys[k]);
            break;
        }
        seen[k] = true;
        const char *v = eq + 1;
        uint64_t n = 0;
        switch (k) {
        case K_K:
            if (!tier_num(v, end, 1, CHUNK_K_MAX, &n))
                TIER_ERR("k must be 1 .. %d", CHUNK_K_MAX);
            c->k = (int)n;
            break;
        case K_M:
            if (!tier_num(v, end, 1, CHUNK_M_MAX, &n))
                TIER_ERR("m must be 1 .. %d", CHUNK_M_MAX);
            c->m = (int)n;
            break;
        case K_DIRS:
            rc = tier_dirs(v, end, c, err, errlen);
            break;
        case K_GRACE:
        case K_KEEP:
        case K_COARSE:
            if (!tier_num(v, end, 0, TIER_SECONDS_MAX, &n))
                TIER_ERR("%s must be 0 .. %lld seconds", keys[k], (long long)TIER_SECONDS_MAX);
            *(k == K_GRACE ? &c->grace_s : k == K_KEEP ? &c->keep_s : &c->coarse_s) = (int64_t)n;
            break;
        case K_CACHE:
            if (!tier_num(v, end, 0, TIER_CACHE_MB_MAX, &n))
                TIER_ERR("cache_mb must be 0 .. %llu", (unsigned long long)TIER_CACHE_MB_MAX);
            c->cache_mb = n;
            break;
        case K_EAGER:
            if (!tier_num(v, end, 0, 1, &n))
                TIER_ERR("eager must be 0 or 1");
            c->eager = n == 1;
            break;
        }
        if (!*end)
            break;
        p = end + 1;
        if (!*p)
            TIER_ERR("the setting ends with ','");
    }
    if (rc == 0) {
        for (int k = K_K; k <= K_DIRS; k++)
            if (!seen[k]) {
                TIER_ERR("%s is required", keys[k]);
                break;
            }
    }
    if (rc == 0 && c->ndirs < c->k + c->m)
        TIER_ERR("dirs names %d stores, fewer than k + m = %d", c->ndirs, c->k + c->m);
out:
    if (rc) {
        if (rc == TSDB_ERR_NOMEM)
            snprintf(err, errlen, "out of memory");
        tier_cfg_free(c);
        return rc;
    }
    c->set = true;
    return 0;
}

/* tier_from_strings, its lines naming the database (dir; NULL: none) */
static int tier_decide(const char *dir, const char *spec, const char *iknow, tier_cfg *c)
{
    memset(c, 0, sizeof *c);
    if (!spec || !*spec)
        return 0;
    const char *sep = dir ? ": " : "";
    dir = dir ? dir : "";
    if ((!iknow || strcmp(iknow, "1") != 0) && r7_mutant != R7_MUT_TIERLOOSE) {
        fprintf(stderr, "tsdb: %s%sTSDB_TEST_TIER is set but TSDB_TEST_TIER_I_KNOW=1 is not: the tier is a test "
                        "feature that writes requires=tier into META, which older builds refuse; the open fails (unset "
                        "TSDB_TEST_TIER, or set TSDB_TEST_TIER_I_KNOW=1 for a test)\n", dir, sep);
        return TSDB_ERR_INVALID;
    }
    char err[256];
    int rc = tier_cfg_parse(spec, c, err, sizeof err);
    if (rc)
        fprintf(stderr, "tsdb: %s%sTSDB_TEST_TIER: %s: the open fails\n", dir, sep, err);
    return rc;
}

int tier_from_strings(const char *spec, const char *iknow, tier_cfg *c) { return tier_decide(NULL, spec, iknow, c); }

int tier_from_env(tier_cfg *c)
{
    return tier_from_strings(getenv("TSDB_TEST_TIER"), getenv("TSDB_TEST_TIER_I_KNOW"), c);
}

#ifdef TSDB_EC
/* Whether the store at path holds a volume (an s<number> directory), or might: anything but a clean "no" says yes. */
static bool store_has_volumes(const char *path)
{
    DIR *d = opendir(path);
    if (!d)
        return errno != ENOENT;
    bool has = false;
    struct dirent *e;
    errno = 0;
    while ((e = readdir(d)) != NULL)
        if (e->d_name[0] == 's' && e->d_name[1] >= '0' && e->d_name[1] <= '9') {
            has = true;
            break;
        }
    if (!e && errno)
        has = true;
    closedir(d);
    return has;
}

/* Whether the database's root META says requires=tier, or might: anything but a clean "no" (no META yet, or META
 * without the word) says yes. */
static bool root_has_tier(const char *dir)
{
    char p[PATH_MAX + 8];
    snprintf(p, sizeof p, "%s/META", dir);
    int fd = open(p, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return errno != ENOENT;
    char txt[512];
    ssize_t r = read(fd, txt, sizeof txt - 1);
    close(fd);
    if (r < 0)
        return true;
    txt[r] = '\0';
    unsigned feats;
    meta_requires_parse(txt, &feats, NULL, 0);
    return feats & META_REQ_TIER;
}

/* The backend. A tier is new while the root's META does not say requires=tier (the root's attach writes it after the
 * stores were made and before any store holds a volume) and none of its stores holds a volume: the stores without
 * META are made first (chunk_store_init: the directory, one level of it, and META, all synced), so a power loss
 * between two of them is finished by the next open. Once the root says requires=tier, or a volume has been raised in
 * any store, a store without META is a lost disk (a mount that is not there, a disk replaced): DOWN, never made again
 * here (its chunks are gone; a heal or replace makes it, R15). The stores alone cannot tell: all of them missing or
 * empty look like a new tier. */
static int tier_be_open(tier *t, const char *dir)
{
    int n = t->cfg.ndirs;
    const char *const *paths = (const char *const *)t->cfg.dirs;
    bool fresh = tier_mutant == TIER_MUT_FRESH || !root_has_tier(dir);
    char meta[PATH_MAX + 8];
    for (int i = 0; i < n && fresh; i++) {
        snprintf(meta, sizeof meta, "%s/META", paths[i]);
        if (access(meta, F_OK) == 0 && store_has_volumes(paths[i]))
            fresh = false;
    }
    for (int i = 0; i < n && fresh; i++) {
        snprintf(meta, sizeof meta, "%s/META", paths[i]);
        if (access(meta, F_OK) == 0 || errno != ENOENT)
            continue;
        uint8_t id[16];
        int rc = chunk_store_init(paths[i], NULL, id);
        if (rc) {
            fprintf(stderr, "tsdb: %s: the tier's chunk store %d (%s) cannot be made (%s): the open fails\n", dir, i,
                    paths[i], tsdb_strerror(rc));
            return rc;
        }
    }
    for (int i = 0; i < n && !fresh; i++) {
        snprintf(meta, sizeof meta, "%s/META", paths[i]);
        if (access(meta, F_OK) != 0 && errno == ENOENT)
            fprintf(stderr, "tsdb: %s: the tier's chunk store %d (%s) has no META, but the tier holds data: a lost "
                            "disk or a mount that is not there; it is DOWN and not made again\n", dir, i, paths[i]);
    }
    chunk_be_opts o = {.min_up = n - t->cfg.m};
    int rc = tier_mutant == TIER_MUT_STRICT ? chunk_be_open(&t->be, paths, n, NULL)
                                            : chunk_be_open2(&t->be, paths, n, NULL, &o);
    if (rc) {
        t->be = NULL;
        fprintf(stderr, "tsdb: %s: the tier's chunk stores do not open (%s; %d of %d must be up): the open fails\n",
                dir, tsdb_strerror(rc), o.min_up, n);
    }
    return rc;
}
#endif

int tier_open(const char *dir, const tsdb_options *opt, tier **out)
{
    *out = NULL;
    tier_cfg c;
    int rc = tier_decide(dir, getenv("TSDB_TEST_TIER"), getenv("TSDB_TEST_TIER_I_KNOW"), &c);
    if (rc || !c.set)
        return rc;
#ifndef TSDB_EC
    (void)opt;
    fprintf(stderr, "tsdb: %s: TSDB_TEST_TIER is set, but this build has no erasure codec (TSDB_EC): the open fails\n",
            dir);
    tier_cfg_free(&c);
    return TSDB_ERR_INVALID;
#else
    if (opt && (opt->read_only || opt->follow)) {
        /* the writer holds the stores (flock), and a follower does not read .ec parts (D7) */
        fprintf(stderr, "tsdb: %s: TSDB_TEST_TIER is set on a read-only open or a follower: only a writer opens a "
                        "tier: the open fails\n", dir);
        tier_cfg_free(&c);
        return TSDB_ERR_INVALID;
    }
    tier *t = calloc(1, sizeof *t);
    if (!t) {
        tier_cfg_free(&c);
        return TSDB_ERR_NOMEM;
    }
    atomic_init(&t->refs, 1);
    atomic_init(&t->halted, false);
    t->cfg = c;
    if (tier_mutant != TIER_MUT_PERENGINE && (rc = tier_be_open(t, dir)) != 0) {
        tier_put(t);
        return rc;
    }
    if ((rc = eccache_tier_open(t)) != 0 || (rc = ecgc_tier_open(t)) != 0 || (rc = ecsettle_tier_open(t)) != 0) {
        tier_put(t); /* every close, the failed owner's included */
        return rc;
    }
    /* the cache reads through this backend, never one of its own (its stores' flocks would make that one BUSY) */
    eccache_set_backend(t, t->be);
    *out = t;
    return 0;
#endif
}

tier *tier_get(tier *t)
{
    if (t)
        atomic_fetch_add_explicit(&t->refs, 1, memory_order_relaxed);
    return t;
}

void tier_put(tier *t)
{
    if (!t || atomic_fetch_sub_explicit(&t->refs, 1, memory_order_acq_rel) != 1)
        return;
    ecsettle_tier_close(t);
    ecgc_tier_close(t);
    eccache_tier_close(t);
#ifdef TSDB_EC
    if (t->be)
        chunk_be_close(t->be);
#endif
    tier_cfg_free(&t->cfg);
    free(t);
}

void tier_quiesce(tier *t)
{
    if (t)
        ecsettle_tier_quiesce(t);
}

const tier_cfg *tier_config(const tier *t) { return t ? &t->cfg : NULL; }

uint32_t tier_refs(const tier *t) { return t ? atomic_load(&((tier *)t)->refs) : 0; }

static __thread tier *want_tier;
static __thread uint32_t want_vol;

void tier_want(tier *t, uint32_t vol)
{
    want_tier = t;
    want_vol = vol;
}

static int hexval(char c) { return c >= '0' && c <= '9' ? c - '0' : c >= 'a' && c <= 'f' ? c - 'a' + 10 : -1; }

/* META's vuuid= line: 1 and *u when there is one, 0 when there is none, TSDB_ERR_CORRUPT when it is not 32 lowercase
 * hex digits. */
static int meta_vuuid(const char *txt, uint8_t u[16])
{
    for (const char *l = txt; *l;) {
        if (strncmp(l, "vuuid=", 6) == 0) {
            const char *h = l + 6;
            for (int i = 0; i < 16; i++) {
                int a = hexval(h[2 * i]), b = a < 0 ? -1 : hexval(h[2 * i + 1]);
                if (b < 0)
                    return TSDB_ERR_CORRUPT;
                u[i] = (uint8_t)(a << 4 | b);
            }
            return h[32] == '\n' || h[32] == '\0' ? 1 : TSDB_ERR_CORRUPT;
        }
        const char *e = strchr(l, '\n');
        if (!e)
            break;
        l = e + 1;
    }
    return 0;
}

static bool random_bytes(uint8_t *b, size_t n)
{
    size_t got = 0;
    while (got < n) {
        ssize_t r = getrandom(b + got, n - got, 0);
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0)
            return false;
        got += (size_t)r;
    }
    return true;
}

/* META, for the refusals of every engine (with a tier or not) and the tier's vuuid=. */
static int tier_meta_read(eng *db, char *buf, size_t cap)
{
    int fd = openat(db->dirfd, "META", O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return TSDB_ERR_IO;
    ssize_t r = read(fd, buf, cap - 1);
    close(fd);
    if (r <= 0)
        return TSDB_ERR_CORRUPT;
    buf[r] = '\0';
    return 0;
}

/* The engine's META under a tier: vuuid= made once, requires=tier written before anything can name an .ec file (one
 * rewrite, so a power loss leaves META with both or with neither; an open after it makes them again). */
static int tier_meta_write(eng *db, const char *txt, uint8_t u[16])
{
    unsigned feats;
    meta_requires_parse(txt, &feats, NULL, 0);
    int rc = meta_vuuid(txt, u);
    if (rc < 0) {
        fprintf(stderr, "tsdb: %s: META's vuuid= line is damaged: the open fails\n", db->dir);
        return rc;
    }
    if (rc == 1 && (feats & META_REQ_TIER))
        return 0;
    if (rc == 0 && !random_bytes(u, 16)) {
        fprintf(stderr, "tsdb: %s: no random bytes for the volume's uuid: the open fails\n", db->dir);
        return TSDB_ERR_IO;
    }
    char line[48];
    int n = snprintf(line, sizeof line, "vuuid=");
    for (int i = 0; i < 16; i++)
        n += snprintf(line + n, sizeof line - (size_t)n, "%02x", u[i]);
    snprintf(line + n, sizeof line - (size_t)n, "\n");
    if ((rc = meta_tier_require(db->dirfd, db->opt.fsync, line)) != 0)
        fprintf(stderr, "tsdb: %s: META could not be given requires=tier and vuuid= (%s): the open fails\n", db->dir,
                tsdb_strerror(rc));
    return rc;
}

int tier_engine_attach(eng *db)
{
    tier *t = want_tier;
    uint32_t vol = want_vol;
    want_tier = NULL; /* taken once: an engine opened inside this open (none today) does not attach to it */
    char txt[512];
    int rc = tier_meta_read(db, txt, sizeof txt);
    if (rc)
        return rc; /* meta_load_or_create read it a moment ago */
    unsigned feats;
    meta_requires_parse(txt, &feats, NULL, 0);
    bool ro = db->opt.read_only || db->opt.follow;
    if (feats & META_REQ_TIER) {
#ifndef TSDB_EC
        if (tier_mutant == TIER_MUT_NOEC)
            return 0; /* planted: the volume opens as if its .ec parts were not there */
        fprintf(stderr, "tsdb: %s: META says requires=tier (the volume may hold erasure-coded parts), but this "
                        "build has no erasure codec (TSDB_EC): the open fails\n", db->dir);
        return TSDB_ERR_INVALID;
#endif
        if (ro) {
            fprintf(stderr, "tsdb: %s: META says requires=tier: a follower or a read-only open does not read "
                            "erasure-coded parts: the open fails\n", db->dir);
            return TSDB_ERR_INVALID;
        }
        if (!t) {
            fprintf(stderr, "tsdb: %s: META says requires=tier, but no tier is configured (TSDB_TEST_TIER with "
                            "TSDB_TEST_TIER_I_KNOW=1): the open fails\n", db->dir);
            return TSDB_ERR_INVALID;
        }
    }
    if (!t)
        return 0;
    if ((feats & (META_REQ_CLUSTER | META_REQ_REPLICA)) || db->replica_req) {
        fprintf(stderr, "tsdb: %s: a tier on a cluster volume (META requires=cluster or replica) is not built yet "
                        "(R8): the open fails\n", db->dir);
        return TSDB_ERR_INVALID;
    }
    if (ro) { /* tier_open refuses these; an engine opened read-only under a writer's tier is the same mistake */
        fprintf(stderr, "tsdb: %s: a read-only open under a tier: the open fails\n", db->dir);
        return TSDB_ERR_INVALID;
    }
    struct ecstate *es = calloc(1, sizeof *es);
    if (!es)
        return TSDB_ERR_NOMEM;
    db->ecstate = es;
    db->tier = tier_get(t);
    db->tier_vol = vol;
    if ((rc = tier_meta_write(db, txt, es->vuuid)) != 0)
        return rc; /* the open fails; its eng_close detaches */
    db->tier_req = true; /* the one place it is set: META says requires=tier now, also at the first attach (E55) */
    if ((rc = eccache_set_vuuid(t, vol, es->vuuid)) != 0)
        return rc; /* the cache's readers name chunks by the same uuid as their writers */
    chunk_backend *be = t->be;
    if (tier_mutant == TIER_MUT_PERENGINE) {
        chunk_be_opts o = {.min_up = t->cfg.ndirs - t->cfg.m};
        if ((rc = chunk_be_open2(&es->mut_be, (const char *const *)t->cfg.dirs, t->cfg.ndirs, NULL, &o)) != 0) {
            es->mut_be = NULL;
            fprintf(stderr, "tsdb: %s: the tier's chunk stores do not open (%s): the open fails\n", db->dir,
                    tsdb_strerror(rc));
            return rc;
        }
        be = es->mut_be;
    }
    /* D4: the chunk epoch is the constant 1, given as the authoritative one at every open: it clears a volume whose
     * EPOCH lost a slot (a torn store), which would refuse every put. The call does not reach every store: the
     * backend's fence is the highest epoch any store holds, so a store behind it (a power cut before its EPOCH was
     * stored, or a store that failed to take it at an earlier open) is not raised, and not every error fails the store
     * (a mkdir of s<N> refused leaves it UP). So each store UP is raised by itself where it does not hold the epoch:
     * one that still does not halts the tier (no put may reach a store without its fence); none holding it fails the
     * open. */
    if (be && tier_mutant != TIER_MUT_UNSURE) {
        rc = chunk_be_see_epoch(be, vol, 1);
        if (rc == TSDB_ERR_NOMEM)
            return rc;
        int held = tier_mutant == TIER_MUT_FENCE; /* planted: the call's word taken for every store */
        for (int i = 0; i < chunk_be_nstores(be) && tier_mutant != TIER_MUT_FENCE; i++) {
            chunk_store *st = chunk_be_store(be, i);
            if (chunk_be_store_state(be, i) != CHUNK_STORE_UP || !st || chunk_store_failed(st))
                continue; /* tier_writable halts for these */
            bool has = chunk_store_epoch(st, vol) >= 1 && !chunk_store_epoch_unsure(st, vol);
            if (!has && chunk_store_raise(st, vol, 1) == 0)
                has = chunk_store_epoch(st, vol) >= 1 && !chunk_store_epoch_unsure(st, vol);
            if (has)
                held++;
            else
                tier_halt(t, i, "did not take the volume's epoch at its open");
        }
        if (held == 0) {
            fprintf(stderr, "tsdb: %s: no chunk store of the tier holds the epoch of volume %u (%s): the open fails\n",
                    db->dir, vol, rc ? tsdb_strerror(rc) : "none is up and not failed");
            return rc ? rc : TSDB_ERR_IO;
        }
    }
    if (be)
        (void)tier_writable(t); /* a store DOWN at open: the line now, not at the first settle */
    if ((rc = ecgc_eng_open(db)) != 0 || (rc = eckeep_eng_open(db)) != 0 || (rc = ecsettle_eng_open(db)) != 0 ||
        (rc = ecswap_eng_open(db)) != 0)
        return rc; /* the open fails; its eng_close detaches */
    return 0;
}

struct chunk_backend *tier_backend(const tier *t) { return t ? t->be : NULL; }

const uint8_t *tier_vuuid(const eng *db) { return db->ecstate ? db->ecstate->vuuid : NULL; }

static void tier_halt(tier *t, int i, const char *why)
{
    if (!atomic_exchange(&t->halted, true))
        fprintf(stderr, "tsdb: tier store %d (%s) %s: settles and chunk deletes stop for this tier until it is opened "
                        "again; reads go on\n", i, t->cfg.dirs[i], why);
}

bool tier_writable(tier *t)
{
    if (!t || !t->be)
        return false;
    if (atomic_load(&t->halted))
        return false;
    for (int i = 0; i < chunk_be_nstores(t->be); i++) {
        int st = chunk_be_store_state(t->be, i);
        if (st == CHUNK_STORE_UP)
            continue;
        char why[64];
        snprintf(why, sizeof why, "is %s", chunk_store_state_name(st));
        tier_halt(t, i, why);
        return false;
    }
    return true;
}

int tier_follower_ec(eng *db, const char *name)
{
    char what[160];
    snprintf(what, sizeof what, "reading %s, an erasure-coded part MANIFEST names (followers do not read them)",
             name);
    eng_fail(db, what, ENOTSUP);
    return TSDB_ERR_IO; /* follow_refresh passes it up: CORRUPT or INVALID would add "keeping the previous view" */
}

void tier_engine_detach(eng *db)
{
    if (db->ecstate) {
        ecswap_eng_close(db);
        ecsettle_eng_close(db);
        eckeep_eng_close(db);
        ecgc_eng_close(db);
    }
    if (db->ecstate && db->ecstate->mut_be)
        chunk_be_close(db->ecstate->mut_be);
    free(db->ecstate);
    db->ecstate = NULL;
    tier_put(db->tier);
    db->tier = NULL;
}
