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
#include "eccache.h"
#include "ecgc.h"
#include "eckeep.h"
#include "ecpart.h"
#include "ecswap.h"
#include "settle.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int r7_mutant;

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

int tier_from_strings(const char *spec, const char *iknow, tier_cfg *c)
{
    memset(c, 0, sizeof *c);
    if (!spec || !*spec)
        return 0;
    if ((!iknow || strcmp(iknow, "1") != 0) && r7_mutant != R7_MUT_TIERLOOSE) {
        fprintf(stderr, "tsdb: TSDB_TEST_TIER is set but TSDB_TEST_TIER_I_KNOW=1 is not: the tier is a test feature "
                        "that writes requires=tier into META, which older builds refuse; the open fails (unset "
                        "TSDB_TEST_TIER, or set TSDB_TEST_TIER_I_KNOW=1 for a test)\n");
        return TSDB_ERR_INVALID;
    }
    char err[256];
    int rc = tier_cfg_parse(spec, c, err, sizeof err);
    if (rc)
        fprintf(stderr, "tsdb: TSDB_TEST_TIER: %s: the open fails\n", err);
    return rc;
}

int tier_from_env(tier_cfg *c)
{
    return tier_from_strings(getenv("TSDB_TEST_TIER"), getenv("TSDB_TEST_TIER_I_KNOW"), c);
}

int tier_open(const char *dir, const tsdb_options *opt, tier **out)
{
    (void)opt;
    *out = NULL;
    tier_cfg c;
    int rc = tier_from_env(&c);
    if (rc || !c.set)
        return rc;
#ifndef TSDB_EC
    fprintf(stderr, "tsdb: %s: TSDB_TEST_TIER is set, but this build has no erasure codec (TSDB_EC): the open fails\n",
            dir);
    tier_cfg_free(&c);
    return TSDB_ERR_INVALID;
#else
    (void)dir;
    tier *t = calloc(1, sizeof *t);
    if (!t) {
        tier_cfg_free(&c);
        return TSDB_ERR_NOMEM;
    }
    atomic_init(&t->refs, 1);
    t->cfg = c;
    if ((rc = eccache_tier_open(t)) != 0 || (rc = ecgc_tier_open(t)) != 0 || (rc = ecsettle_tier_open(t)) != 0) {
        tier_put(t); /* every close, the failed owner's included */
        return rc;
    }
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

int tier_engine_attach(eng *db)
{
    tier *t = want_tier;
    want_tier = NULL; /* taken once: an engine opened inside this open (none today) does not attach to it */
    if (!t)
        return 0;
    struct ecstate *es = calloc(1, sizeof *es);
    if (!es)
        return TSDB_ERR_NOMEM;
    db->ecstate = es;
    db->tier = tier_get(t);
    db->tier_vol = want_vol;
    int rc;
    if ((rc = ecgc_eng_open(db)) != 0 || (rc = eckeep_eng_open(db)) != 0 || (rc = ecsettle_eng_open(db)) != 0 ||
        (rc = ecswap_eng_open(db)) != 0)
        return rc; /* the open fails; its eng_close detaches */
    return 0;
}

void tier_engine_detach(eng *db)
{
    if (db->ecstate) {
        ecswap_eng_close(db);
        ecsettle_eng_close(db);
        eckeep_eng_close(db);
        ecgc_eng_close(db);
    }
    free(db->ecstate);
    db->ecstate = NULL;
    tier_put(db->tier);
    db->tier = NULL;
}
