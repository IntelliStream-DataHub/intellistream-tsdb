/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The tier of a tsdb handle (src/tier.h has the design). r7-iface: the configuration's parser (pure), the decision
 * of TSDB_TEST_TIER / TSDB_TEST_TIER_I_KNOW, a tier that is a reference count around its configuration, and the
 * engine's attach and detach, which call each owner's hooks (src/tier.h). r7-tier adds the backend (chunk_be_open2,
 * epoch 1, vuuid=, requires=tier and the refusals of D1 and D7) here; r7-seam's cache, r7-gc's queue and r7-settle's
 * thread live in their own files, behind the hooks, never here.
 */
#include "eccold.h"
#include "tier.h"
#include "chunk.h"
#include "chunk_priv.h"
#include "coldops.h"
#include "eccache.h"
#include "feed.h"
#include "ecgc.h"
#include "eckeep.h"
#include "ecpart.h"
#include "ecswap.h"
#include "settle.h"
#include "tierctl.h"
#include "tierfill.h"
#include "tiersalv.h"

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
static _Atomic uint64_t tier_cache_mb_opt;

static inline int tier_mut(void)
{
#ifdef TSDB_TEST_HOOKS
    return tier_mutant;
#else
    return 0;
#endif
}

#ifdef TSDB_TEST_HOOKS
static pthread_once_t tier_mut_once = PTHREAD_ONCE_INIT;
static int tier_mut_rc;

static void tier_mut_init_once(void)
{
    const char *m = getenv("TSDB_TIER_MUTANT");
    if (!m || !*m)
        return;
    if (strcmp(m, "downnoread") == 0) {
        tier_mutant = TIER_MUT_DOWNNOREAD;
        chunk_mutant = CHUNK_MUT_DOWNNOREAD;
    } else if (strcmp(m, "fullnoflag") == 0) {
        tierfill_mutant = TIERFILL_MUT_FULLNOFLAG;
    } else if (strcmp(m, "lostnoreplace") == 0) {
        tierctl_mutant = TIERCTL_MUT_LOSTNOREPLACE;
    } else if (strcmp(m, "salvagelatest") == 0) {
        tiersalv_mutant = TIERSALV_MUT_LATEST;
    } else {
        fprintf(stderr, "tsdb: TSDB_TIER_MUTANT=%s: no such mutant\n", m);
        tier_mut_rc = -1;
        return;
    }
    fprintf(stderr, "tsdb: TSDB_TIER_MUTANT=%s (tests only)\n", m);
}
#endif

int tier_mutant_env_init(void)
{
#ifdef TSDB_TEST_HOOKS
    pthread_once(&tier_mut_once, tier_mut_init_once);
    return tier_mut_rc;
#else
    return 0;
#endif
}

void tsdb_tier_set_cache_bytes(size_t bytes)
{
    uint64_t mb = 0;
    if (bytes > 0) {
        mb = bytes < (1u << 20) ? 1u : (uint64_t)(bytes >> 20);
        if (mb > TIER_CACHE_MB_MAX)
            mb = TIER_CACHE_MB_MAX;
    }
    atomic_store_explicit(&tier_cache_mb_opt, mb, memory_order_relaxed);
}

bool tier_default_geom(int nstores, bool all_single_copy, int *k_out, int *m_out)
{
    if (nstores <= 0)
        return false;
    if (all_single_copy) {
        if (k_out)
            *k_out = 1;
        if (m_out)
            *m_out = 0;
        return true;
    }
    if (nstores < 2)
        return false;
    int k, m;
    if (nstores == 2) {
        k = 1;
        m = 1;
    } else if (nstores <= 5) {
        k = nstores - 1;
        m = 1;
    } else {
        k = 4;
        m = 2;
    }
    if (k_out)
        *k_out = k;
    if (m_out)
        *m_out = m;
    return true;
}


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

#if defined(TSDB_EC) && defined(TSDB_TEST_HOOKS)
/* Timing-only overrides from TSDB_TEST_TIER beside a TIERS file that names stores (R15 D16). */
typedef struct tier_env_override {
    bool any;
    bool has_grace, has_keep, has_coarse, has_cache, has_eager;
    int64_t grace_s, keep_s, coarse_s;
    uint64_t cache_mb;
    bool eager;
} tier_env_override;

static int tier_env_override_parse(const char *dir, const char *spec, const char *iknow, tier_env_override *ov)
{
    memset(ov, 0, sizeof *ov);
    if (!spec || !*spec)
        return 0;
    const char *sep = dir ? ": " : "";
    dir = dir ? dir : "";
    enum { K_GRACE, K_KEEP, K_COARSE, K_CACHE, K_EAGER, K_N };
    static const char *const keys[K_N] = {"grace_s", "keep_s", "coarse_s", "cache_mb", "eager"};
    bool seen[K_N] = {false};
    /* First scan: validate key=value framing and always refuse dirs= (and k=/m=) beside TIERS, even without I_KNOW=1. */
    char bad_geom = '\0';
    for (const char *p = spec;;) {
        const char *end = strchr(p, ',');
        if (!end)
            end = p + strlen(p);
        const char *eq = memchr(p, '=', (size_t)(end - p));
        if (!eq) {
            fprintf(stderr, "tsdb: %s%sTSDB_TEST_TIER: '%.*s' is not key=value: the open fails\n", dir, sep,
                    (int)(end - p), p);
            return TSDB_ERR_INVALID;
        }
        size_t kl = (size_t)(eq - p);
        if (kl == 4 && memcmp(p, "dirs", 4) == 0) {
            fprintf(stderr, "tsdb: %s%sTSDB_TEST_TIER names dirs= beside %s: stores come from %s, not "
                            "TSDB_TEST_TIER: the open fails\n", dir, sep, TIERS_FILE, TIERS_FILE);
            return TSDB_ERR_INVALID;
        }
        if (kl == 1 && (p[0] == 'k' || p[0] == 'm') && !bad_geom)
            bad_geom = p[0];
        if (!*end)
            break;
        p = end + 1;
        if (!*p) {
            fprintf(stderr, "tsdb: %s%sTSDB_TEST_TIER: the setting ends with ',': the open fails\n", dir, sep);
            return TSDB_ERR_INVALID;
        }
    }
    if (bad_geom) {
        fprintf(stderr, "tsdb: %s%sTSDB_TEST_TIER names %c= beside %s: geometry comes from %s, not "
                        "TSDB_TEST_TIER: the open fails\n", dir, sep, bad_geom, TIERS_FILE, TIERS_FILE);
        return TSDB_ERR_INVALID;
    }
    if ((!iknow || strcmp(iknow, "1") != 0) && r7_mutant != R7_MUT_TIERLOOSE) {
        fprintf(stderr, "tsdb: %s%sTSDB_TEST_TIER is set beside %s without TSDB_TEST_TIER_I_KNOW=1: timing "
                        "overrides apply only with TSDB_TEST_TIER_I_KNOW=1: the open fails\n", dir, sep, TIERS_FILE);
        return TSDB_ERR_INVALID;
    }
    for (const char *p = spec;;) {
        const char *end = strchr(p, ',');
        if (!end)
            end = p + strlen(p);
        const char *eq = memchr(p, '=', (size_t)(end - p));
        size_t kl = (size_t)(eq - p);
        int k = 0;
        while (k < K_N && !(strlen(keys[k]) == kl && memcmp(keys[k], p, kl) == 0))
            k++;
        if (k == K_N) {
            fprintf(stderr, "tsdb: %s%sTSDB_TEST_TIER: unknown key '%.*s' beside %s: the open fails\n", dir, sep,
                    (int)kl, p, TIERS_FILE);
            return TSDB_ERR_INVALID;
        }
        if (seen[k]) {
            fprintf(stderr, "tsdb: %s%sTSDB_TEST_TIER: %s is given twice: the open fails\n", dir, sep, keys[k]);
            return TSDB_ERR_INVALID;
        }
        seen[k] = true;
        const char *v = eq + 1;
        uint64_t n = 0;
        switch (k) {
        case K_GRACE:
        case K_KEEP:
        case K_COARSE:
            if (!tier_num(v, end, 0, TIER_SECONDS_MAX, &n)) {
                fprintf(stderr, "tsdb: %s%sTSDB_TEST_TIER: %s must be 0 .. %lld seconds: the open fails\n", dir, sep,
                        keys[k], (long long)TIER_SECONDS_MAX);
                return TSDB_ERR_INVALID;
            }
            if (k == K_GRACE) {
                ov->has_grace = true;
                ov->grace_s = (int64_t)n;
            } else if (k == K_KEEP) {
                ov->has_keep = true;
                ov->keep_s = (int64_t)n;
            } else {
                ov->has_coarse = true;
                ov->coarse_s = (int64_t)n;
            }
            break;
        case K_CACHE:
            if (!tier_num(v, end, 0, TIER_CACHE_MB_MAX, &n)) {
                fprintf(stderr, "tsdb: %s%sTSDB_TEST_TIER: cache_mb must be 0 .. %llu: the open fails\n", dir, sep,
                        (unsigned long long)TIER_CACHE_MB_MAX);
                return TSDB_ERR_INVALID;
            }
            ov->has_cache = true;
            ov->cache_mb = n;
            break;
        case K_EAGER:
            if (!tier_num(v, end, 0, 1, &n)) {
                fprintf(stderr, "tsdb: %s%sTSDB_TEST_TIER: eager must be 0 or 1: the open fails\n", dir, sep);
                return TSDB_ERR_INVALID;
            }
            ov->has_eager = true;
            ov->eager = n == 1;
            break;
        }
        if (!*end)
            break;
        p = end + 1;
    }
    ov->any = true;
    return 0;
}
#endif

#ifdef TSDB_TEST_HOOKS
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
#endif

int tier_from_strings(const char *spec, const char *iknow, tier_cfg *c)
{
#ifdef TSDB_TEST_HOOKS
    return tier_decide(NULL, spec, iknow, c);
#else
    (void)iknow;
    memset(c, 0, sizeof *c);
    return spec && *spec ? TSDB_ERR_INVALID : 0;
#endif
}

int tier_from_env(tier_cfg *c)
{
#ifdef TSDB_TEST_HOOKS
    return tier_from_strings(getenv("TSDB_TEST_TIER"), getenv("TSDB_TEST_TIER_I_KNOW"), c);
#else
    memset(c, 0, sizeof *c);
    return 0;
#endif
}

#if defined(TSDB_EC) && defined(TSDB_TEST_HOOKS)
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
    } else {
        chunk_be_set_put_cap(t->be, CHUNK_STORE_PUT_MAX_PCT);
    }
    return rc;
}
#endif

#ifdef TSDB_EC
/* Open the directory chunk backend strictly from TIERS (R15 r15-open, r15-follow, design-cluster-data.md §15.7, D3,
 * D4, D9, D13):
 * - At open, every non-LOST store in TIERS must exist with its recorded store_id and db_id (strict chunk_be_open +
 *   owner db_id check; an unrecorded missing/down store fails open naming it until declared LOST).
 * - With read_only (followers and read-only opens, D13): stores are opened with CHUNK_OPEN_RDONLY (no flock, no EPOCH
 *   write, no tmp_* cleanup, no delete), still enforcing strict presence of every non-LOST store and verifying
 *   store_id and db_id so a follower on shared storage refuses a missing or foreign-db store naming it.
 * - LOST stores are passed as NULL (CHUNK_STORE_DOWN_MISSING), REPLACED_BY stores by their replacement path & id.
 * - Fewer than min_up active stores fails the open (TSDB_ERR_UNAVAILABLE). */
static int tier_be_open_from_tiers(tier *t, const char *dir, int min_up, bool read_only, bool salvage_mode)
{
    int n = (int)t->tview.nstores;
    if (n <= 0 || n > TIERS_STORES_MAX)
        return TSDB_ERR_INVALID;
    for (int i = 0; i < TIERS_STORES_MAX; i++)
        t->tiers_to_be[i] = -1;
    const char *paths[TIERS_STORES_MAX];
    uint8_t ids[TIERS_STORES_MAX * 16];
    int out_n = 0;
    int active = 0;
    for (int i = 0; i < n; i++) {
        const tiers_store *st = &t->tview.stores[i];
        if (st->state == TIERS_STATE_REMOVED)
            continue; /* R15 r15-shrink: REMOVED stores are skipped at reopen (backend opens n - 1 stores) */
        int be_i = out_n++;
        t->tiers_to_be[i] = be_i;
        if (st->state == TIERS_STATE_LOST || st->state == TIERS_STATE_SHRINKING) {
            if (st->state == TIERS_STATE_LOST && tier_mut() == TIER_MUT_LOSTOPEN) {
                /* Planted mutant: opens a LOST store as UP instead of passing NULL (DOWN_MISSING). */
                paths[be_i] = st->path;
                memcpy(ids + 16 * be_i, st->id, 16);
                active++;
            } else {
                paths[be_i] = NULL;
                memset(ids + 16 * be_i, 0, 16);
            }
        } else if (st->state == TIERS_STATE_REPLACED_BY) {
            paths[be_i] = st->path;
            memcpy(ids + 16 * be_i, st->replaced_by, 16);
            active++;
        } else {
            paths[be_i] = st->path;
            memcpy(ids + 16 * be_i, st->id, 16);
            active++;
        }
    }
    if (out_n <= 0)
        return TSDB_ERR_UNAVAILABLE;
    if (!salvage_mode && active < min_up) {
        fprintf(stderr, "tsdb: %s: %d of %d tier stores in %s are active, %d are needed: the open fails\n", dir,
                active, out_n, TIERS_FILE, min_up);
        return TSDB_ERR_UNAVAILABLE;
    }
    int rc;
    if (salvage_mode) {
        chunk_be_opts bo = {.min_up = 0, .read_only = false, .strict = false};
        rc = chunk_be_open2(&t->be, paths, out_n, ids, &bo);
    } else if (read_only) {
        chunk_be_opts bo = {.min_up = min_up, .read_only = true, .strict = true};
        rc = chunk_be_open2(&t->be, paths, out_n, ids, &bo);
    } else {
        rc = chunk_be_open(&t->be, paths, out_n, ids);
    }
    if (rc) {
        t->be = NULL;
        int mapped = rc == CHUNK_ERR_BUSY ? TSDB_ERR_LOCKED : rc == CHUNK_ERR_STORE_ID ? TSDB_ERR_INVALID : rc;
        fprintf(stderr, "tsdb: %s: tier stores from %s failed to open (%s): the open fails\n", dir, TIERS_FILE,
                rc == CHUNK_ERR_STORE_ID ? "store id mismatch"
                                         : rc == CHUNK_ERR_BUSY ? "store is locked by another process"
                                                                : tsdb_strerror(mapped));
        return mapped;
    }
    if (tier_mut() != TIER_MUT_DBIDSKIP) {
        for (int i = 0; i < out_n; i++) {
            chunk_store *st = chunk_be_store(t->be, i);
            if (!st)
                continue; /* LOST or SHRINKING store (DOWN_MISSING) */
            chunk_store_owner ow;
            chunk_store_get_owner(st, &ow);
            if (!ow.db_set) {
                char want_hex[33];
                chunk_oid_hex(t->db_id, want_hex);
                fprintf(stderr, "tsdb: %s: tier store %d (%s) META has no db= binding, expected database %s: "
                                "the open fails\n", dir, i, paths[i], want_hex);
                chunk_be_close(t->be);
                t->be = NULL;
                return TSDB_ERR_INVALID;
            }
            if (memcmp(ow.db_id, t->db_id, 16) != 0) {
                char have_hex[33], want_hex[33];
                chunk_oid_hex(ow.db_id, have_hex);
                chunk_oid_hex(t->db_id, want_hex);
                fprintf(stderr, "tsdb: %s: tier store %d (%s) belongs to database %s, not %s: the open fails\n", dir,
                        i, paths[i], have_hex, want_hex);
                chunk_be_close(t->be);
                t->be = NULL;
                return TSDB_ERR_INVALID;
            }
        }
    }
    if (!read_only)
        chunk_be_set_put_cap(t->be, CHUNK_STORE_PUT_MAX_PCT);
    /* R15 (r15-lost, r15-shrink): mark LOST/SHRINKING slots for E15 TSDB_ERR_IO, exclude SHRINKING from new
     * placements, and open any unfinished replacement (REBUILDING marker present) in CHUNK_STORE_REPLACING. */
    for (int i = 0; i < n; i++) {
        const tiers_store *st = &t->tview.stores[i];
        int be_i = t->tiers_to_be[i];
        if (be_i < 0)
            continue;
        if (st->state == TIERS_STATE_LOST && tier_mut() != TIER_MUT_LOSTOPEN) {
            chunk_be_set_lost(t->be, be_i, true);
        } else if (st->state == TIERS_STATE_SHRINKING) {
            chunk_be_set_lost(t->be, be_i, true);
            chunk_be_set_excluded(t->be, be_i, true);
        } else if (!read_only && st->state == TIERS_STATE_REPLACED_BY && tierctl_store_rebuilding(st->path)) {
            (void)chunk_be_set_replacing(t->be, be_i);
            tier_ensure_paused(t, be_i, "is rebuilding");
        }
    }
    return 0;
}

static int validate_tiers_redundancy(const char *dir, const tiers_settings *s, int nstores, bool all_single_copy)
{
    if (!(s->set_mask & TSDB_TIER_SET_REDUNDANCY))
        return 0;
    if (s->m == 0) {
        if (s->k != 1 || !all_single_copy) {
            fprintf(stderr, "tsdb: %s: %s requests %u+0 redundancy, but m = 0 requires k = 1 and every store "
                            "to be single-copy: the open fails\n", dir, TIERS_FILE, (unsigned)s->k);
            return TSDB_ERR_INVALID;
        }
        return 0;
    }
    if (s->k < 1 || s->k > CHUNK_K_MAX || s->m > CHUNK_M_MAX || (int)s->k + (int)s->m > nstores) {
        fprintf(stderr, "tsdb: %s: %s requests %u+%u redundancy, but only %d store(s) are recorded: the open fails\n",
                dir, TIERS_FILE, (unsigned)s->k, (unsigned)s->m, nstores);
        return TSDB_ERR_INVALID;
    }
    return 0;
}
#endif

static tier_cluster_fn cluster_fn; /* set once before any tsdb_open (tier_set_cluster), read-only after */
static void *cluster_arg;
static uint64_t cluster_cache_mb;

/* Registry of open single-server tiers keyed by database root path (allows tsdbd's --health-domain and STATUS to find
 * the active tier without adding public symbols to libtsdb.so or editing src/volumes.c). */
static pthread_mutex_t tier_reg_mu = PTHREAD_MUTEX_INITIALIZER;
static tier **tier_reg;
static size_t ntier_reg;

#ifdef TSDB_EC
static void tier_set_dir(tier *t, const char *dir)
{
    if (!t || !dir || !*dir)
        return;
    char canon[PATH_MAX];
    char *d = strdup(realpath(dir, canon) ? canon : dir);
    if (!d)
        return;
    free(t->dir);
    t->dir = d;
}

static void tier_reg_add(tier *t)
{
    if (!t || !t->dir)
        return;
    pthread_mutex_lock(&tier_reg_mu);
    bool present = false;
    for (size_t i = 0; i < ntier_reg; i++)
        if (tier_reg[i] == t) {
            present = true;
            break;
        }
    if (!present) {
        tier **nt = realloc(tier_reg, (ntier_reg + 1) * sizeof *nt);
        if (nt) {
            tier_reg = nt;
            tier_reg[ntier_reg++] = t;
        }
    }
    pthread_mutex_unlock(&tier_reg_mu);
}
#endif

static void tier_reg_remove(tier *t)
{
    pthread_mutex_lock(&tier_reg_mu);
    for (size_t i = 0; i < ntier_reg; i++) {
        if (tier_reg[i] == t) {
            tier_reg[i] = tier_reg[--ntier_reg];
            if (ntier_reg == 0) {
                free(tier_reg);
                tier_reg = NULL;
            }
            break;
        }
    }
    pthread_mutex_unlock(&tier_reg_mu);
}

tier *tier_try_get(tier *t)
{
    if (!t)
        return NULL;
    unsigned r = atomic_load_explicit(&t->refs, memory_order_relaxed);
    while (r > 0) {
        if (atomic_compare_exchange_weak_explicit(&t->refs, &r, r + 1, memory_order_acquire, memory_order_relaxed))
            return t;
    }
    return NULL;
}

tier *tier_find_by_dir(const char *dir)
{
    if (!dir || !*dir)
        return NULL;
    char canon[PATH_MAX];
    const char *want = realpath(dir, canon) ? canon : dir;
    tier *found = NULL;
    pthread_mutex_lock(&tier_reg_mu);
    for (size_t i = 0; i < ntier_reg; i++) {
        tier *t = tier_reg[i];
        if (!t->dir)
            continue;
        /* tier_reg_add canonicalized t->dir outside tier_reg_mu, so pure string compare here (no I/O under lock). */
        if (strcmp(t->dir, want) == 0 || strcmp(t->dir, dir) == 0) {
            found = tier_try_get(t);
            if (found)
                break;
        }
    }
    pthread_mutex_unlock(&tier_reg_mu);
    return found;
}

int tier_store_count(const tier *t)
{
#ifdef TSDB_EC
    return (t && t->be) ? chunk_be_nstores(t->be) : 0;
#else
    (void)t;
    return 0;
#endif
}

bool tier_store_path(const tier *t, int i, char *buf, size_t cap)
{
    if (!t || !buf || cap == 0 || i < 0)
        return false;
#ifdef TSDB_EC
    if (t->be && chunk_be_store_path_copy(t->be, i, buf, cap))
        return true;
#endif
    if (i < t->cfg.ndirs && t->cfg.dirs[i]) {
        snprintf(buf, cap, "%s", t->cfg.dirs[i]);
        return true;
    }
    buf[0] = '\0';
    return false;
}

int tier_store_state(const tier *t, int i)
{
#ifdef TSDB_EC
    return (t && t->be) ? chunk_be_store_state(t->be, i) : -1;
#else
    (void)t;
    (void)i;
    return -1;
#endif
}

void tier_set_cluster(tier_cluster_fn fn, void *arg, uint64_t cache_mb)
{
    cluster_fn = fn;
    cluster_arg = arg;
    cluster_cache_mb = cache_mb;
}

static int tier_open_impl(const char *dir, const tsdb_options *opt, bool salvage_mode, tier **out)
{
    *out = NULL;
    if (tier_mutant_env_init() != 0)
        return TSDB_ERR_INVALID;
    tiers_view tv;
    tiers_view_init(&tv);
    bool has_db_id = false;
    uint8_t db_id[16];
    memset(db_id, 0, sizeof db_id);
    if (dir && *dir) {
        int dirfd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (dirfd >= 0) {
            /* Read-only parse here (before the root/VOLUMES lock); tsdb_open cuts any torn tail under the VOLUMES OFD
             * write lock once the root engine is open. */
            int trc = tiers_load(dirfd, false, false, &tv);
            if (trc) {
                close(dirfd);
                tiers_view_free(&tv);
                return trc;
            }
            if (tv.nstores > 0) {
                int drc = tiers_db_id_read(dirfd, &has_db_id, db_id);
                if (drc != 0 && drc != TSDB_ERR_NOTFOUND) {
                    close(dirfd);
                    tiers_view_free(&tv);
                    return drc;
                }
                if (!has_db_id) {
                    fprintf(stderr, "tsdb: %s: %s lists %zu store(s) but root META has no db_id=: the open fails\n",
                            dir, TIERS_FILE, tv.nstores);
                    close(dirfd);
                    tiers_view_free(&tv);
                    return TSDB_ERR_CORRUPT;
                }
            }
            close(dirfd);
        }
    }

    if (tv.nstores > 0) {
#ifndef TSDB_EC
        (void)opt;
        fprintf(stderr, "tsdb: %s: %s lists %zu store(s) (requires=tier), but this build was not built with the "
                        "erasure codec (TSDB_EC): the open fails\n", dir, TIERS_FILE, tv.nstores);
        tiers_view_free(&tv);
        return TSDB_ERR_INVALID;
#else
        if (cluster_fn) {
            fprintf(stderr, "tsdb: %s: %s lists %zu store(s), but a cluster server cannot open a single-server tier: "
                            "the open fails\n", dir, TIERS_FILE, tv.nstores);
            tiers_view_free(&tv);
            return TSDB_ERR_INVALID;
        }
#ifdef TSDB_TEST_HOOKS
        tier_env_override ov;
        int orc = tier_env_override_parse(dir, getenv("TSDB_TEST_TIER"), getenv("TSDB_TEST_TIER_I_KNOW"), &ov);
        if (orc) {
            tiers_view_free(&tv);
            return orc;
        }
#endif
        bool ro = opt && (opt->read_only || opt->follow);
        int n = (int)tv.nstores;
        int n_open = 0, n_elig = 0;
        bool all_single_copy = true;
        for (int i = 0; i < n; i++) {
            if (tv.stores[i].state == TIERS_STATE_REMOVED)
                continue;
            n_open++;
            if (tv.stores[i].state != TIERS_STATE_SHRINKING) {
                n_elig++;
                if (!tv.stores[i].single_copy)
                    all_single_copy = false;
            }
        }
        if (n_elig == 0)
            all_single_copy = false;
        if (n_open == 0) {
            fprintf(stderr, "tsdb: %s: %s has no non-removed stores (%d store(s) all REMOVED): the open fails\n", dir,
                    TIERS_FILE, n);
            tiers_view_free(&tv);
            return TSDB_ERR_CORRUPT;
        }
        int vrc = validate_tiers_redundancy(dir, &tv.global, n_elig, all_single_copy);
        for (size_t i = 0; i < tv.nvols && vrc == 0; i++)
            vrc = validate_tiers_redundancy(dir, &tv.vols[i].s, n_elig, all_single_copy);
        if (vrc) {
            tiers_view_free(&tv);
            return vrc;
        }
        int k = 0, m = 0;
        if (tv.global.set_mask & TSDB_TIER_SET_REDUNDANCY) {
            k = (int)tv.global.k;
            m = (int)tv.global.m;
        } else if (!tier_default_geom(n_elig, all_single_copy, &k, &m)) {
            /* One non-single-copy store added so far: no active tier yet until a second store is added. */
#ifdef TSDB_TEST_HOOKS
            if (ov.any && !ro)
                fprintf(stderr, "tsdb: %s: %s has only %d non-single-copy store (tier not active yet); "
                                "TSDB_TEST_TIER timing overrides ignored\n", dir, TIERS_FILE, n_elig);
#endif
            tiers_view_free(&tv);
            return 0;
        }
        int min_m = m;
        for (size_t i = 0; i < tv.nvols; i++)
            if ((tv.vols[i].s.set_mask & TSDB_TIER_SET_REDUNDANCY) && (int)tv.vols[i].s.m < min_m)
                min_m = (int)tv.vols[i].s.m;
        int min_up = n_elig - min_m;
        if (min_up < 1)
            min_up = 1;

        tier_cfg c = {0};
        c.set = true;
        c.k = k;
        c.m = m;
        c.ndirs = n_open;
        c.dirs = calloc((size_t)n_open, sizeof *c.dirs);
        if (!c.dirs) {
            tiers_view_free(&tv);
            return TSDB_ERR_NOMEM;
        }
        for (int i = 0, j = 0; i < n; i++) {
            if (tv.stores[i].state == TIERS_STATE_REMOVED)
                continue;
            c.dirs[j] = strdup(tv.stores[i].path);
            if (!c.dirs[j]) {
                tier_cfg_free(&c);
                tiers_view_free(&tv);
                return TSDB_ERR_NOMEM;
            }
            j++;
        }
        c.grace_s = (int64_t)tv.global.tier_after_s;
        c.keep_s = TIER_KEEP_S_DEFAULT;
        c.coarse_s = TIER_COARSE_S_DEFAULT;
        c.cache_mb = atomic_load_explicit(&tier_cache_mb_opt, memory_order_relaxed);
        c.eager = false;
#ifdef TSDB_TEST_HOOKS
        if (!ro) {
            if (ov.has_grace)
                c.grace_s = ov.grace_s;
            if (ov.has_keep)
                c.keep_s = ov.keep_s;
            if (ov.has_coarse)
                c.coarse_s = ov.coarse_s;
            if (ov.has_eager)
                c.eager = ov.eager;
        }
        if (ov.has_cache)
            c.cache_mb = ov.cache_mb;
#endif
        tier *t = calloc(1, sizeof *t);
        if (!t) {
            tier_cfg_free(&c);
            tiers_view_free(&tv);
            return TSDB_ERR_NOMEM;
        }
        atomic_init(&t->refs, 1);
        atomic_init(&t->halted, false);
        atomic_init(&t->fence_halted, false);
        pthread_mutex_init(&t->cmu, NULL);
        pthread_cond_init(&t->eng_cv, NULL);
        t->cfg = c;
        t->from_tiers = true;
        t->read_only = ro;
        memcpy(t->db_id, db_id, 16);
        t->tview = tv; /* ownership transferred; tier_put frees it */
#ifdef TSDB_TEST_HOOKS
        t->ov_grace = !ro && ov.has_grace;
#endif
        int open_rc = tier_be_open_from_tiers(t, dir, min_up, ro, salvage_mode);
        if (open_rc != 0 || (open_rc = eccache_tier_open(t)) != 0) {
            tier_put(t);
            return open_rc;
        }
        eccache_set_backend(t, t->be);
        if (salvage_mode) {
            atomic_store(&t->halted, true);
            if ((open_rc = ecgc_tier_open(t)) != 0) {
                tier_put(t);
                return open_rc;
            }
        } else if (!ro) {
            tier_set_dir(t, dir);
            if ((open_rc = ecgc_tier_open(t)) != 0 || (open_rc = ecsettle_tier_open(t)) != 0 ||
                (open_rc = tier_health_open(t)) != 0 || (open_rc = tierctl_open(t)) != 0) {
                tier_put(t);
                return open_rc;
            }
            tier_reg_add(t);
        }
        *out = t;
        return 0;
#endif
    }
    tiers_view_free(&tv);
    if (salvage_mode)
        return 0;

    tier_cfg c = {0};
#ifdef TSDB_TEST_HOOKS
    int rc = tier_decide(dir, getenv("TSDB_TEST_TIER"), getenv("TSDB_TEST_TIER_I_KNOW"), &c);
    if (rc)
        return rc;
#endif
    if (!c.set) {
#ifdef TSDB_EC
        /* r8f: a cluster tsdbd's tier, for its ec volumes only (no stores: their cold ops reach the stripe group) */
        if (!cluster_fn || (opt && (opt->read_only || opt->follow)))
            return 0;
        c.cache_mb = cluster_cache_mb;
#else
        (void)dir;
        (void)opt;
        return 0;
#endif
    }
#ifndef TSDB_EC
#ifdef TSDB_TEST_HOOKS
    (void)opt;
    fprintf(stderr, "tsdb: %s: TSDB_TEST_TIER is set, but this build has no erasure codec (TSDB_EC): the open fails\n",
            dir);
    tier_cfg_free(&c);
    return TSDB_ERR_INVALID;
#else
    return 0;
#endif
#else
#ifdef TSDB_TEST_HOOKS
    if (c.set && opt && (opt->read_only || opt->follow)) {
        /* the writer holds the stores (flock), and a follower does not read .ec parts (D7) */
        fprintf(stderr, "tsdb: %s: TSDB_TEST_TIER is set on a read-only open or a follower: only a writer opens a "
                        "tier: the open fails\n", dir);
        tier_cfg_free(&c);
        return TSDB_ERR_INVALID;
    }
#endif
    if (c.set && c.cache_mb == 0)
        c.cache_mb = atomic_load_explicit(&tier_cache_mb_opt, memory_order_relaxed);
    tier *t = calloc(1, sizeof *t);
    if (!t) {
        tier_cfg_free(&c);
        return TSDB_ERR_NOMEM;
    }
    atomic_init(&t->refs, 1);
    atomic_init(&t->halted, false);
    atomic_init(&t->fence_halted, false);
    pthread_mutex_init(&t->cmu, NULL);
    pthread_cond_init(&t->eng_cv, NULL);
    t->cfg = c;
    int open_rc = 0;
#ifdef TSDB_TEST_HOOKS
    if (c.set && tier_mutant != TIER_MUT_PERENGINE && (open_rc = tier_be_open(t, dir)) != 0) {
        tier_put(t);
        return open_rc;
    }
#else
    (void)dir;
#endif
    if ((open_rc = eccache_tier_open(t)) != 0 || (open_rc = ecgc_tier_open(t)) != 0 ||
        (open_rc = ecsettle_tier_open(t)) != 0) {
        tier_put(t); /* every close, the failed owner's included */
        return open_rc;
    }
    /* the cache reads through this backend, never one of its own (its stores' flocks would make that one BUSY) */
    eccache_set_backend(t, t->be);
    if (t->be) {
        tier_set_dir(t, dir);
        if ((open_rc = tier_health_open(t)) != 0 || (open_rc = tierctl_open(t)) != 0) {
            tier_put(t);
            return open_rc;
        }
        tier_reg_add(t);
    }
    *out = t;
    return 0;
#endif
}

int tier_open(const char *dir, const tsdb_options *opt, tier **out)
{
    return tier_open_impl(dir, opt, false, out);
}

int tier_open_salvage(const char *dir, const tsdb_options *opt, tier **out)
{
    return tier_open_impl(dir, opt, true, out);
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
    tier_reg_remove(t);
    if (tierctl_close(t))
        return; /* a detached tier worker is still stuck in the kernel using t/be/cache: leave allocated */
    if (tier_health_close(t))
        return; /* a detached health worker is still stuck in the kernel using t/be/cache: leave allocated */
    ecsettle_tier_close(t);
    ecgc_tier_close(t);
    eccache_tier_close(t);
    for (size_t i = 0; i < t->ncvs; i++)
        tier_cold_put(t->cvs[i]); /* before the backend: a directory context names it */
    free(t->cvs);
    free(t->engs);
    free(t->dir);
    pthread_cond_destroy(&t->eng_cv);
    pthread_mutex_destroy(&t->cmu);
#ifdef TSDB_EC
    if (t->be)
        chunk_be_close(t->be);
#endif
    tiers_view_free(&t->tview);
    tier_cfg_free(&t->cfg);
    free(t);
}

void tier_quiesce(tier *t)
{
    if (t) {
        tier_health_quiesce(t);
        tierctl_quiesce(t);
        ecsettle_tier_quiesce(t);
        ecgc_tier_quiesce(t);
    }
}

const tier_cfg *tier_config(const tier *t) { return t ? &t->cfg : NULL; }

uint32_t tier_refs(const tier *t) { return t ? atomic_load(&((tier *)t)->refs) : 0; }

static __thread tier *want_tier;
static __thread uint32_t want_vol;
static __thread tier_attach want_attach;

void tier_want(tier *t, uint32_t vol)
{
    want_tier = t;
    want_vol = vol;
    memset(&want_attach, 0, sizeof want_attach);
}

void tier_want_attach(tier *t, uint32_t vol, const tier_attach *a)
{
    tier_want(t, vol);
    if (a)
        want_attach = *a;
}

static int hexval(char c) { return c >= '0' && c <= '9' ? c - '0' : c >= 'a' && c <= 'f' ? c - 'a' + 10 : -1; }

/* META's vuuid= line: 1 and *u when there is one, 0 when there is none, TSDB_ERR_CORRUPT when it is not 32 lowercase
 * hex digits. */
static int meta_hexline(const char *txt, const char *key, uint8_t u[16])
{
    size_t kl = strlen(key);
    for (const char *l = txt; *l;) {
        if (strncmp(l, key, kl) == 0) {
            const char *h = l + kl;
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

static int meta_vuuid(const char *txt, uint8_t u[16]) { return meta_hexline(txt, "vuuid=", u); }

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
    if (rc == 0 && db->cluster_req) { /* r8f: a cluster volume's vuuid is its metadata uuid (cvolfs's volume_uuid=) */
        if ((rc = meta_hexline(txt, "volume_uuid=", u)) != 1) {
            fprintf(stderr, "tsdb: %s: META of a cluster volume has no valid volume_uuid= line: the open fails\n",
                    db->dir);
            return rc < 0 ? rc : TSDB_ERR_CORRUPT;
        }
        rc = 0;
    } else if (rc == 0 && !random_bytes(u, 16)) {
        fprintf(stderr, "tsdb: %s: no random bytes for the volume's uuid: the open fails\n", db->dir);
        return TSDB_ERR_IO;
    }
    char line[48];
    size_t n = 0;
    int r = snprintf(line, sizeof line, "vuuid=");
    if (r > 0)
        n = (size_t)r < sizeof line ? (size_t)r : sizeof line - 1;
    for (int i = 0; i < 16 && n + 1 < sizeof line; i++) {
        r = snprintf(line + n, sizeof line - n, "%02x", u[i]);
        if (r > 0)
            n += (size_t)r < sizeof line - n ? (size_t)r : sizeof line - n - 1;
    }
    if (n + 1 < sizeof line)
        snprintf(line + n, sizeof line - n, "\n");
    if ((rc = meta_tier_require(db->dirfd, db->opt.fsync, line)) != 0)
        fprintf(stderr, "tsdb: %s: META could not be given requires=tier and vuuid= (%s): the open fails\n", db->dir,
                tsdb_strerror(rc));
    return rc;
}


void tier_settings_from_cfg(const tier_cfg *c, tier_settings *s)
{
    memset(s, 0, sizeof *s);
    tier_settings_set_km(s, c->k, c->m);
    s->grace_s = c->grace_s;
    s->keep_s = c->keep_s;
    s->coarse_s = c->coarse_s;
    s->cache_mb = c->cache_mb;
    s->eager = c->eager;
}

coldvol *tier_cold_ref(coldvol *cv)
{
    if (cv)
        atomic_fetch_add_explicit(&cv->refs, 1, memory_order_relaxed);
    return cv;
}

void tier_cold_put(coldvol *cv)
{
    if (!cv || atomic_fetch_sub_explicit(&cv->refs, 1, memory_order_acq_rel) != 1)
        return;
    if (cv->ops && cv->ctx)
        cv->ops->ctx_free(cv->ctx);
    free(cv);
}

/* The registry takes a reference to cv as the volume's entry; the one it replaces (a retired engine's) is put. */
static int cold_register(tier *t, coldvol *cv)
{
    coldvol *old = NULL;
    int rc = 0;
    pthread_mutex_lock(&t->cmu);
    size_t i = 0;
    while (i < t->ncvs && t->cvs[i]->volume != cv->volume)
        i++;
    if (i == t->ncvs) {
        coldvol **n = realloc(t->cvs, (t->ncvs + 1) * sizeof *n);
        if (!n)
            rc = TSDB_ERR_NOMEM; /* the open fails: a reader of the volume would find no ops */
        else
            t->cvs = n, t->cvs[t->ncvs++] = tier_cold_ref(cv);
    } else {
        old = t->cvs[i];
        t->cvs[i] = tier_cold_ref(cv);
    }
    pthread_mutex_unlock(&t->cmu);
    tier_cold_put(old); /* outside cmu: the last put frees the context */
    return rc;
}

coldvol *tier_cold_get(tier *t, uint32_t volume)
{
    if (!t)
        return NULL;
    coldvol *cv = NULL;
    pthread_mutex_lock(&t->cmu);
    for (size_t i = 0; i < t->ncvs; i++)
        if (t->cvs[i]->volume == volume)
            cv = tier_cold_ref(t->cvs[i]);
    pthread_mutex_unlock(&t->cmu);
    return cv;
}

coldvol *tier_cold(const eng *db) { return db->ecstate ? db->ecstate->cold : NULL; }

uint64_t coldvol_epoch(const coldvol *cv)
{
    if (!cv)
        return 0;
    uint64_t le = atomic_load_explicit(&((coldvol *)cv)->lead_epoch, memory_order_acquire);
    if (le)
        return le;
    return cv->epoch_fn ? cv->epoch_fn(cv->epoch_arg, cv->volume) : 1; /* D4: the constant 1 on a single server */
}

uint64_t tier_epoch(tier *t, uint32_t volume)
{
    coldvol *cv = tier_cold_get(t, volume);
    uint64_t e = coldvol_epoch(cv);
    tier_cold_put(cv);
    return e;
}

const tier_settings *tier_eng_settings(const eng *db)
{
    coldvol *cv = tier_cold(db);
    return cv ? &cv->set : NULL;
}

bool tier_cold_writable(const eng *db)
{
    coldvol *cv = tier_cold(db);
    return cv && cv->ops->writable(cv->ctx);
}

/* A cluster volume (META requires=cluster or replica): eccold_may_attach (src/eccold.h, r8g fills it) decides whether one
 * attaches a tier; until then R7's refusal stands. */
static bool tier_is_cluster_volume(eng *db)
{
    char txt[512];
    unsigned feats = 0;
    if (tier_meta_read(db, txt, sizeof txt) == 0)
        meta_requires_parse(txt, &feats, NULL, 0);
    return (feats & (META_REQ_CLUSTER | META_REQ_REPLICA)) || db->replica_req;
}

static void tier_fill_vol_settings(tier *t, uint32_t vol, tier_settings *out)
{
    if (t->from_tiers) {
        tiers_settings vs;
        int def_k = 0, def_m = 0;
        bool has_def = false;
        size_t tv_nstores = 0;
        bool all_sc = false;
        pthread_mutex_lock(&t->cmu);
        tiers_view_settings_for(&t->tview, vol, &vs);
        tv_nstores = 0;
        all_sc = true;
        for (size_t i = 0; i < t->tview.nstores; i++) {
            if (t->tview.stores[i].state == TIERS_STATE_REMOVED || t->tview.stores[i].state == TIERS_STATE_SHRINKING)
                continue;
            tv_nstores++;
            if (!t->tview.stores[i].single_copy)
                all_sc = false;
        }
        if (tv_nstores == 0)
            all_sc = false;
        has_def = tier_default_geom((int)tv_nstores, all_sc, &def_k, &def_m);
        pthread_mutex_unlock(&t->cmu);
        tier_settings_from_cfg(&t->cfg, out);
        out->from_tiers = true;
        if (vs.set_mask & TSDB_TIER_SET_REDUNDANCY) {
            bool ok = vs.m == 0 ? (vs.k == 1 && all_sc)
                                : (vs.k >= 1 && vs.k <= CHUNK_K_MAX && vs.m <= CHUNK_M_MAX &&
                                   (int)vs.k + (int)vs.m <= (int)tv_nstores);
            if (ok)
                tier_settings_set_km(out, (int)vs.k, (int)vs.m);
            else if (has_def)
                tier_settings_set_km(out, def_k, def_m);
        } else if (has_def) {
            tier_settings_set_km(out, def_k, def_m);
        }
        out->tier_after_s = t->ov_grace ? (uint64_t)t->cfg.grace_s : vs.tier_after_s;
        out->grace_s = t->ov_grace ? t->cfg.grace_s : (int64_t)vs.tier_after_s;
        out->tier_high_pct = vs.tier_high_pct;
        out->young_high_pct = vs.young_high_pct;
        out->rebuild_mb_s = vs.rebuild_mb_s;
        out->scrub_mb_s = vs.scrub_mb_s;
        out->scrub_days = vs.scrub_days;
    } else {
        tier_settings_from_cfg(&t->cfg, out);
    }
}

/* R15 D13 (r15-follow): attach a read-only coldvol for follower/read-only reads through the follower's eccache
 * without writing META, touching EPOCH, or starting any writer/GC/settle state. */
static int follower_attach_cold(eng *db, tier *t, uint32_t vol, const uint8_t vuuid[16])
{
    coldvol *cv = calloc(1, sizeof *cv);
    if (!cv)
        return TSDB_ERR_NOMEM;
    atomic_init(&cv->refs, 1);
    cv->volume = vol;
    memcpy(cv->vuuid, vuuid, 16);
    tier_fill_vol_settings(t, vol, &cv->set);
    int rc = coldops_dir_make(NULL, t, vol, cv->vuuid, &cv->ctx);
    if (rc != 0) {
        free(cv);
        return rc;
    }
    cv->ops = &coldops_dir;
    rc = cold_register(t, cv);
    if (rc != 0) {
        tier_cold_put(cv);
        return rc;
    }
    tier_cold_put(db->ecstate->cold);
    db->ecstate->cold = cv;
    return 0;
}

int tier_follower_ensure_vuuid(eng *db)
{
    if (!db || !db->tier || !db->tier->read_only || !db->ecstate || db->ecstate->has_vuuid)
        return 0;
    char txt[512];
    int rc = tier_meta_read(db, txt, sizeof txt);
    if (rc)
        return rc;
    unsigned feats = 0;
    meta_requires_parse(txt, &feats, NULL, 0);
    uint8_t u[16];
    int vrc = meta_vuuid(txt, u);
    if (vrc != 1 || !(feats & META_REQ_TIER)) {
        fprintf(stderr, "tsdb: %s: erasure-coded part opened on a follower, but META lacks requires=tier or a valid "
                        "vuuid=: the open fails\n", db->dir);
        return TSDB_ERR_CORRUPT;
    }
    rc = follower_attach_cold(db, db->tier, db->tier_vol, u);
    if (rc == 0) {
        memcpy(db->ecstate->vuuid, u, 16);
        db->ecstate->has_vuuid = true;
        db->tier_req = true;
    }
    return rc;
}

int tier_engine_attach(eng *db)
{
    tier *t = want_tier;
    uint32_t vol = want_vol;
    tier_attach at = want_attach;
    want_tier = NULL; /* taken once: an engine opened inside this open (none today) does not attach to it */
    memset(&want_attach, 0, sizeof want_attach);
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
        if (!t) {
            if (ro) {
                fprintf(stderr, "tsdb: %s: META says requires=tier, but no tier is configured (a follower or a "
                                "read-only open needs %s to read erasure-coded parts): the open fails\n",
                        db->dir, TIERS_FILE);
            } else {
#ifdef TSDB_TEST_HOOKS
                fprintf(stderr, "tsdb: %s: META says requires=tier, but no tier is configured (TSDB_TEST_TIER with "
                                "TSDB_TEST_TIER_I_KNOW=1): the open fails\n", db->dir);
#else
                fprintf(stderr, "tsdb: %s: META says requires=tier, but no tier is configured: the open fails\n",
                        db->dir);
#endif
            }
            return TSDB_ERR_INVALID;
        }
    }
    if (!t)
        return 0;
    tier_settings cset;
    if (!t->cfg.set) { /* r8f: a cluster tsdbd's tier takes its ec volumes only, with the hook's ops */
        if (!tier_is_cluster_volume(db) || !eccold_may_attach(db)) {
            if (feats & META_REQ_TIER) {
                fprintf(stderr, "tsdb: %s: META says requires=tier, but the volume is not a cluster ec volume and no "
                                "tier is configured for it: the open fails\n", db->dir);
                return TSDB_ERR_INVALID;
            }
            return 0;
        }
        if (!at.ops) {
            if (!cluster_fn)
                return TSDB_ERR_INVALID;
            memset(&cset, 0, sizeof cset);
            if ((rc = cluster_fn(cluster_arg, vol, &at, &cset)) != 0) {
                fprintf(stderr, "tsdb: %s: the cold side of cluster ec volume %u cannot be attached (%s): the open "
                                "fails\n", db->dir, vol, tsdb_strerror(rc));
                return rc;
            }
            at.settings = &cset;
        }
    } else if (t->from_tiers && tier_is_cluster_volume(db)) {
        fprintf(stderr, "tsdb: %s: cluster volume cannot attach to a single-server tier (%s): the open fails\n",
                db->dir, TIERS_FILE);
        return TSDB_ERR_INVALID;
    } else if (tier_is_cluster_volume(db) && !eccold_may_attach(db)) { /* eccold.h: asked for cluster volumes only */
        fprintf(stderr, "tsdb: %s: cluster volume has no cold-tier hooks attached (!eccold_may_attach): the open "
                        "fails\n", db->dir);
        return TSDB_ERR_INVALID;
    }
    if (ro && !t->read_only) { /* an engine opened read-only under a writer's tier is a mistake */
        fprintf(stderr, "tsdb: %s: a read-only open under a writer's tier: the open fails\n", db->dir);
        return TSDB_ERR_INVALID;
    }
    if (!ro && t->read_only) { /* a writer engine cannot attach to a read-only tier */
        fprintf(stderr, "tsdb: %s: a writer open under a read-only tier: the open fails\n", db->dir);
        return TSDB_ERR_INVALID;
    }
    if (ro && t->read_only) {
        /* R15 D13 (r15-follow): attach read-only without writing META, raising EPOCH, or starting GC/settle/keep. */
        struct ecstate *es = calloc(1, sizeof *es);
        if (!es)
            return TSDB_ERR_NOMEM;
        db->ecstate = es;
        db->tier = tier_get(t);
        db->tier_vol = vol;
        db->tier_req = (feats & META_REQ_TIER) != 0;
        int vrc = meta_vuuid(txt, es->vuuid);
        if (vrc < 0) {
            fprintf(stderr, "tsdb: %s: META's vuuid= line is damaged: the open fails\n", db->dir);
            return vrc;
        }
        if (vrc == 0) {
            if (feats & META_REQ_TIER) {
                fprintf(stderr, "tsdb: %s: META says requires=tier but has no vuuid= line: the open fails\n", db->dir);
                return TSDB_ERR_CORRUPT;
            }
            /* Writer has not yet attached a tier to this volume; vuuid= will be loaded lazily on first .ec part. */
            return 0;
        }
        rc = follower_attach_cold(db, t, vol, es->vuuid);
        if (rc == 0)
            es->has_vuuid = true;
        return rc;
    }
    struct ecstate *es = calloc(1, sizeof *es);
    coldvol *cv = calloc(1, sizeof *cv);
    if (!es || !cv) {
        free(es);
        free(cv);
        return TSDB_ERR_NOMEM;
    }
    db->ecstate = es;
    db->tier = tier_get(t);
    db->tier_vol = vol;
    atomic_init(&cv->refs, 1);
    es->cold = cv; /* the engine's reference: tier_engine_detach puts it */
    cv->volume = vol;
    cv->epoch_fn = at.epoch;
    cv->epoch_arg = at.epoch_arg;
    atomic_init(&cv->lead_epoch, (at.lead_epoch && db->leader_open) ? at.lead_epoch : 0);
    if (at.settings)
        cv->set = *at.settings;
    else
        tier_fill_vol_settings(t, vol, &cv->set);
    if ((rc = tier_meta_write(db, txt, es->vuuid)) != 0)
        return rc; /* the open fails; its eng_close detaches */
    es->has_vuuid = true;
    db->tier_req = true; /* the one place it is set: META says requires=tier now, also at the first attach (E55) */
    memcpy(cv->vuuid, es->vuuid, 16);
    void *make_arg = at.make_arg;
    if (tier_mutant == TIER_MUT_PERENGINE) {
        chunk_be_opts o = {.min_up = t->cfg.ndirs - t->cfg.m};
        if ((rc = chunk_be_open2(&es->mut_be, (const char *const *)t->cfg.dirs, t->cfg.ndirs, NULL, &o)) != 0) {
            es->mut_be = NULL;
            fprintf(stderr, "tsdb: %s: the tier's chunk stores do not open (%s): the open fails\n", db->dir,
                    tsdb_strerror(rc));
            return rc;
        }
        at.ops = NULL, make_arg = es->mut_be; /* planted: the volume's ops over a backend of its own */
    }
    const cold_ops *ops = at.ops ? at.ops : &coldops_dir;
    if ((rc = (at.ops ? at.make : coldops_dir_make)(make_arg, t, vol, cv->vuuid, &cv->ctx)) != 0)
        return rc;
    cv->ops = ops; /* only now: tier_cold_put frees a context only with its ops */
    /* The epoch comes from the volume's provider (D4: the constant 1 on a single server), given as the authoritative
     * one at every open: it clears a volume whose EPOCH lost a slot (a torn store), which would refuse every put. Never
     * the stores' own fence, which is what a writer last saw (selfepoch plants exactly that). */
    uint64_t e = coldvol_epoch(cv);
    if (tier_mutant == TIER_MUT_SELFEPOCH && (e = ops->epoch(cv->ctx)) == 0)
        e = 1;
    if (e == 0) {
        fprintf(stderr, "tsdb: %s: the epoch of volume %u is not known (its provider has none now): the open fails\n",
                db->dir, vol);
        return TSDB_ERR_UNAVAILABLE;
    }
    if ((rc = ops->see_epoch(cv->ctx, e)) != 0) {
        if (rc == TSDB_ERR_NOMEM)
            return rc;
        fprintf(stderr, "tsdb: %s: no chunk store of the tier holds the epoch of volume %u (%s): the open fails\n",
                db->dir, vol, tsdb_strerror(rc));
        return rc;
    }
    if (t->be)
        (void)tier_writable(t); /* a store DOWN at open: the line now, not at the first settle */
    if ((rc = ecgc_eng_open(db)) != 0 || (rc = eckeep_eng_open(db)) != 0 || (rc = ecsettle_eng_open(db)) != 0 ||
        (rc = ecswap_eng_open(db)) != 0)
        return rc; /* the open fails; its eng_close detaches */
    if (!db->ec_cluster) {
        /* Pre-reserve t->engs[] capacity BEFORE cold_register() so OOM cannot leave a dangling coldvol entry.
         * The engine is only listed in t->engs[] once load_parts and WAL replay complete (tier_engine_ready). */
        pthread_mutex_lock(&t->cmu);
        size_t need = t->nengs + t->eng_opening + 1;
        if (need > t->eng_cap) {
            size_t nc = t->eng_cap ? t->eng_cap * 2 : 8;
            while (nc < need)
                nc *= 2;
            eng **ne = realloc(t->engs, nc * sizeof *ne);
            if (!ne) {
                pthread_mutex_unlock(&t->cmu);
                return TSDB_ERR_NOMEM;
            }
            t->engs = ne;
            t->eng_cap = nc;
        }
        pthread_mutex_unlock(&t->cmu);
    }
    /* Last: the registry is what the cache's readers, the gc worker (deletes queued by an engine of the volume that
     * closed since) and tier_epoch find by volume number, so it takes only a context whose epoch was seen. An attach
     * that failed above leaves the volume's previous entry in place (none of those lookups runs before the open
     * returns but the gc worker, which keeps an item it cannot delete yet). */
    rc = cold_register(t, cv);
    if (rc == 0 && !at.settings && t->from_tiers) {
        /* Refresh live TIERS settings under t->cmu after cold_register so any update applied while attach ran is seen. */
        pthread_mutex_lock(&t->cmu);
        tiers_settings vs;
        tiers_view_settings_for(&t->tview, vol, &vs);
        size_t tv_elig = 0;
        bool all_sc = true;
        for (size_t i = 0; i < t->tview.nstores; i++) {
            if (t->tview.stores[i].state == TIERS_STATE_REMOVED || t->tview.stores[i].state == TIERS_STATE_SHRINKING)
                continue;
            tv_elig++;
            if (!t->tview.stores[i].single_copy)
                all_sc = false;
        }
        if (tv_elig == 0)
            all_sc = false;
        int def_k = 0, def_m = 0;
        bool has_def = tier_default_geom((int)tv_elig, all_sc, &def_k, &def_m);
        if (vs.set_mask & TSDB_TIER_SET_REDUNDANCY) {
            bool ok = vs.m == 0 ? (vs.k == 1 && all_sc)
                                : (vs.k >= 1 && vs.k <= CHUNK_K_MAX && vs.m <= CHUNK_M_MAX &&
                                   (int)vs.k + (int)vs.m <= (int)tv_elig);
            if (ok)
                tier_settings_set_km(&cv->set, (int)vs.k, (int)vs.m);
        } else if (has_def) {
            tier_settings_set_km(&cv->set, def_k, def_m);
        }
        if (!t->ov_grace) {
            atomic_store_explicit(&cv->set.tier_after_s, vs.tier_after_s, memory_order_relaxed);
            atomic_store_explicit(&cv->set.grace_s, (int64_t)vs.tier_after_s, memory_order_relaxed);
        }
        atomic_store_explicit(&cv->set.tier_high_pct, vs.tier_high_pct, memory_order_relaxed);
        atomic_store_explicit(&cv->set.young_high_pct, vs.young_high_pct, memory_order_relaxed);
        atomic_store_explicit(&cv->set.rebuild_mb_s, vs.rebuild_mb_s, memory_order_relaxed);
        atomic_store_explicit(&cv->set.scrub_mb_s, vs.scrub_mb_s, memory_order_relaxed);
        atomic_store_explicit(&cv->set.scrub_days, vs.scrub_days, memory_order_relaxed);
        pthread_mutex_unlock(&t->cmu);
    }
    if (rc == 0 && !db->ec_cluster) {
        pthread_mutex_lock(&t->cmu);
        t->eng_opening++;
        db->ecstate->tier_opening = true;
        pthread_mutex_unlock(&t->cmu);
    }
    return rc;
}

void tier_engine_ready(eng *db)
{
    if (!db || !db->tier || !db->ecstate)
        return;
    /* Seed FULL state synchronously before eng_open returns so a volume opened/reopened at >= young_high_pct
     * refuses inserts immediately, then start the per-volume 1 s sampler thread (isolated per volume). */
    if (!db->tier->read_only) {
        tierfill_eng_seed(db);
        (void)tierfill_eng_start(db);
    }
    tier *t = db->tier;
    pthread_mutex_lock(&t->cmu);
    if (db->ecstate->tier_opening) {
        db->ecstate->tier_opening = false;
        if (t->eng_opening > 0)
            t->eng_opening--;
        if (!db->ecstate->tier_registered && t->nengs < t->eng_cap) {
            t->engs[t->nengs++] = db;
            db->ecstate->tier_registered = true;
        }
        pthread_cond_broadcast(&t->eng_cv);
    }
    pthread_mutex_unlock(&t->cmu);
}

void tier_engine_unreg(eng *db)
{
    if (!db || !db->tier || !db->ecstate)
        return;
    tierfill_eng_stop(db);
    tier *t = db->tier;
    bool was_reg = false;
    pthread_mutex_lock(&t->cmu);
    if (db->ecstate->tier_opening) {
        db->ecstate->tier_opening = false;
        if (t->eng_opening > 0)
            t->eng_opening--;
        pthread_cond_broadcast(&t->eng_cv);
    }
    if (db->ecstate->tier_registered) {
        for (size_t i = 0; i < t->nengs; i++)
            if (t->engs[i] == db) {
                t->engs[i] = t->engs[--t->nengs];
                break;
            }
        db->ecstate->tier_registered = false;
        was_reg = true;
        while (t->eng_active > 0)
            pthread_cond_wait(&t->eng_cv, &t->cmu);
    }
    pthread_mutex_unlock(&t->cmu);
    if (was_reg)
        tierctl_eng_stop(db);
}

struct chunk_backend *tier_backend(const tier *t) { return t ? t->be : NULL; }

const uint8_t *tier_vuuid(const eng *db) { return (db->ecstate && db->ecstate->has_vuuid) ? db->ecstate->vuuid : NULL; }

static void tier_pause_store(tier *t, int i, const char *why)
{
    if (!atomic_exchange(&t->halted, true)) {
        char pbuf[PATH_MAX];
        const char *p = tier_store_path(t, i, pbuf, sizeof pbuf) ? pbuf : "-";
        fprintf(stderr, "tsdb: tier store %d (%s) %s: settles and chunk deletes stop for this tier until stores "
                        "recover; reads go on\n", i, p, why);
    }
}

void tier_halt_store(tier *t, int i, const char *why)
{
    atomic_store(&t->fence_halted, true);
    tier_pause_store(t, i, why);
}

void tier_ensure_paused(tier *t, int store, const char *why)
{
    if (!t)
        return;
    tier_pause_store(t, store, why ? why : "is DOWN");
}

void tier_store_set_down(tier *t, int store, int reason)
{
#ifdef TSDB_EC
    if (!t || !t->be)
        return;
    int prev = chunk_be_store_state(t->be, store);
    if (chunk_be_set_down(t->be, store, reason) != 0)
        return;
    if (prev != CHUNK_STORE_UP || chunk_be_store_state(t->be, store) != CHUNK_STORE_UP) {
        char why[64];
        snprintf(why, sizeof why, "is %s", chunk_store_state_name(reason));
        tier_pause_store(t, store, why);
    }
    if (prev == CHUNK_STORE_UP || prev == CHUNK_STORE_REPLACING || prev == CHUNK_STORE_DOWN_IOERR) {
        /* Do not close cached chunk fds synchronously here (may be called on a health-monitor callback thread where
         * close() on a hung mount could block): signal the attach worker to reopen/evict stale handles off-thread. */
        tier_health_kick_reopen(t);
    }
#else
    (void)t;
    (void)store;
    (void)reason;
#endif
}

void tier_resume_if_all_up(tier *t)
{
#ifdef TSDB_EC
    if (!t || !t->be || atomic_load(&t->fence_halted))
        return;
    int n = chunk_be_nstores(t->be);
    int elig = 0;
    for (int i = 0; i < n; i++) {
        if (chunk_be_store_excluded(t->be, i))
            continue;
        elig++;
        if (chunk_be_store_state(t->be, i) != CHUNK_STORE_UP)
            return;
    }
    if (elig == 0)
        return;
    if (atomic_exchange(&t->halted, false))
        fprintf(stderr, "tsdb: tier: all %d active stores are UP: settles and chunk deletes resume\n", elig);
    ecgc_kick_tier(t);
#else
    (void)t;
#endif
}

bool tier_writable(tier *t)
{
    if (!t || !t->be || t->read_only)
        return false;
    if (atomic_load(&t->halted))
        return false;
    int elig = 0;
    for (int i = 0; i < chunk_be_nstores(t->be); i++) {
        if (chunk_be_store_excluded(t->be, i))
            continue;
        elig++;
        int st = chunk_be_store_state(t->be, i);
        if (st == CHUNK_STORE_UP)
            continue;
        char why[64];
        snprintf(why, sizeof why, "is %s", chunk_store_state_name(st));
        tier_pause_store(t, i, why);
        return false;
    }
    return elig > 0;
}

bool tier_gc_deletable(tier *t)
{
#ifdef TSDB_EC
    if (!t || !t->be || t->read_only || atomic_load(&t->fence_halted))
        return false;
    /* Respect health attach/reopen/sweep windows and non-LOST halts: only bypass t->halted when at least one
     * store is permanently LOST/excluded (which keeps t->halted set to pause settles) and every other store is UP. */
    if (tier_health_busy(t))
        return false;
    int bn = chunk_be_nstores(t->be);
    bool any_lost = false;
    for (int i = 0; i < bn; i++) {
        if (chunk_be_store_is_lost(t->be, i) || chunk_be_store_excluded(t->be, i)) {
            any_lost = true;
            continue; /* permanently LOST/excluded: unreachable chunks are clean no-ops */
        }
        if (chunk_be_store_state(t->be, i) != CHUNK_STORE_UP)
            return false;
    }
    if (!any_lost && atomic_load(&t->halted))
        return false;
    return true;
#else
    (void)t;
    return false;
#endif
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
    tier_engine_unreg(db); /* idempotent: covers failed-open paths where eng_close did not reach unreg */
    if (db->ecstate) {
        ecswap_eng_close(db);
        ecsettle_eng_close(db);
        eckeep_eng_close(db);
        ecgc_eng_close(db);
    }
    if (db->ecstate)
        tier_cold_put(db->ecstate->cold); /* the context goes with the last reference (the registry's, a delete's) */
    if (db->ecstate && db->ecstate->mut_be)
        chunk_be_close(db->ecstate->mut_be);
    free(db->ecstate);
    db->ecstate = NULL;
    tier_put(db->tier);
    db->tier = NULL;
}
