/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The tier: one per tsdb handle (R7, docs/design-cluster-data.md; the plan's D3, D4, D11). It holds the chunk backend
 * (one chunk_be_open2 over the configured stores, min_up = stores - m: r7-tier), the granule cache and the LRU of
 * open objects (r7-seam), and the settle thread (D11: nice 19, one settle at a time, beside the merge threads, which
 * merge_threads = 0 does not stop: r7-settle). A tier is reference counted: tsdb_open holds one (tier_open), each
 * engine one from its open to its close (tier_engine_attach, tier_engine_detach), retired engines included (a reopen,
 * src/volumes.c, closes the old engine through eng_retire, maybe after tsdb_close when a multi call's cut keeps it):
 * whoever lets go last closes it. tsdb_close calls tier_quiesce before it closes the volumes, so no settle runs on an
 * engine being closed.
 *
 * The configuration is a test feature until R15 (D3): the environment variable TSDB_TEST_TIER, read by tsdb_open,
 *
 *     k=2,m=1,dirs=/a:/b:/c,grace_s=3600,keep_s=86400,coarse_s=604800,cache_mb=0,eager=0
 *
 * acts only with TSDB_TEST_TIER_I_KNOW=1 beside it; set without it, the open fails with a line: a stray variable must
 * not lead an open to write requires=tier into META, which older builds and builds without TSDB_EC refuse. k (1 ..
 * CHUNK_K_MAX), m (1 .. CHUNK_M_MAX) and dirs (absolute paths split by ':', at least k + m, no repeats, at most
 * TIER_DIRS_MAX) are required; the rest default as above (cache_mb 0: 10% of RAM, r7-seam; eager=1: jepsen's policy,
 * r7-settle). Unknown keys, repeated keys, empty values, signs, numbers out of range and trailing commas are refused.
 *
 * Owners: r7-iface wrote this header, tier_cfg_parse and tier_from_env (pure parsing) and the plain reference count in
 * src/tier.c; r7-tier fills tier_open, tier_engine_attach (refusals, chunk_be_see_epoch(be, vol, 1)) and the rest.
 */
#ifndef TSDB_TIER_H
#define TSDB_TIER_H

#include "internal.h"

#define TIER_DIRS_MAX 64
#define TIER_SECONDS_MAX 315360000LL /* ten years: every *_s stays far from overflow in ms */
#define TIER_CACHE_MB_MAX (1ull << 30)

typedef struct tier_cfg {
    bool set;     /* a tier was configured; everything below is meaningful only then */
    int k, m;
    int ndirs;
    char **dirs;  /* ndirs strings, owned (tier_cfg_free) */
    int64_t grace_s, keep_s, coarse_s;
    uint64_t cache_mb;
    bool eager;
} tier_cfg;

#define TIER_GRACE_S_DEFAULT 3600
#define TIER_KEEP_S_DEFAULT 86400
#define TIER_COARSE_S_DEFAULT 604800

/* Parses spec (the grammar above) into *c (c->set true). TSDB_ERR_INVALID with what is wrong in err (always
 * NUL-terminated when errlen > 0), TSDB_ERR_NOMEM. Pure: no environment, no files. */
int tier_cfg_parse(const char *spec, tier_cfg *c, char *err, size_t errlen);
void tier_cfg_free(tier_cfg *c);
/* The decision of tier_from_env from the two variables' values (NULL: unset): spec NULL or "" is no tier (c->set
 * false, 0); spec set and iknow other than "1" is refused; else tier_cfg_parse. A refusal prints one line naming the
 * reason on stderr and returns TSDB_ERR_INVALID. */
int tier_from_strings(const char *spec, const char *iknow, tier_cfg *c);
/* tier_from_strings(getenv("TSDB_TEST_TIER"), getenv("TSDB_TEST_TIER_I_KNOW"), c) */
int tier_from_env(tier_cfg *c);

typedef struct tier tier;

/* tsdb_open's: *out NULL when no tier is configured, else a tier holding the caller's reference. A configuration the
 * build cannot serve (no TSDB_EC) is refused with a line. dir: the database, for messages. */
int tier_open(const char *dir, const tsdb_options *opt, tier **out);
tier *tier_get(tier *t);  /* another reference; NULL stays NULL */
void tier_put(tier *t);   /* the last one closes the tier (its settle thread, cache, backend) */
void tier_quiesce(tier *t); /* tsdb_close, before the volumes close: no settle runs from here on */
const tier_cfg *tier_config(const tier *t);
uint32_t tier_refs(const tier *t); /* for tests */

/* The engine side. tier_want(t, vol) on the opening thread names the tier and volume number the next engine opened
 * on that thread attaches to (eng_open_sched, through tier_engine_attach after META is read); the opener clears it
 * with tier_want(NULL, 0) after the open, whatever it returned. tier_engine_attach takes the wish (once) and a
 * reference into db->tier; tier_engine_detach (eng_close) gives it back. */
void tier_want(tier *t, uint32_t vol);
int tier_engine_attach(eng *db);
void tier_engine_detach(eng *db);

/* The per-engine state of R7's files, db->ecstate (made by tier_engine_attach when the engine has a tier). One pointer
 * per owner, each defined in its own file, so no two tracks edit one struct. */
struct ecstate {
    struct ecgc_eng *gc;         /* r7-gc: the delete queue (src/ecgc.c) */
    struct eckeep_eng *keep;     /* r7-keep: settled_* digests (src/eckeep.c) */
    struct ecsettle_eng *settle; /* r7-settle: policy state (src/settle.c) */
    struct ecswap_eng *swap;     /* r7-swap (src/ecswap.c) */
};

#endif
