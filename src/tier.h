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

/* A tier. Its owners' state hangs off one pointer each, made by the owner's open hook and freed by its close hook (in
 * the owner's own file), so no two tracks edit this struct or tier.c's open and close: tier_open calls
 * eccache_tier_open, ecgc_tier_open, ecsettle_tier_open in that order (a failure: the tier is put, which runs every
 * close), the last tier_put calls the closes in the reverse order, each also for an owner whose open never ran or
 * failed (its pointer is then NULL), and tier_quiesce calls ecsettle_tier_quiesce. r7-tier adds the backend's
 * fields. */
typedef struct tier {
    atomic_uint refs;
    tier_cfg cfg;
    struct eccache_tier *cache;   /* r7-seam: the granule cache and the open objects (src/eccache.c) */
    struct ecgc_tier *gc;         /* r7-gc: the delete queue the settle thread works (src/ecgc.c) */
    struct ecsettle_tier *settle; /* r7-settle: the settle thread (src/settle.c) */
    /* r7-tier: the backend over cfg.dirs, opened once by tier_open (chunk_be_open2, min_up = stores - m) and closed by
     * the last tier_put: a store flocks its directory, so one backend per engine would make the second volume BUSY.
     * halted: a store was not UP (DOWN at open, or failed in service), or did not take a volume's epoch at its open:
     * settles and chunk deletes stop for the tier until the next handle open, which opens the stores again; reads go
     * on (tier_writable). One line when it halts, never more. */
    struct chunk_backend *be;
    atomic_bool halted;
} tier;

/* tsdb_open's: *out NULL when no tier is configured, else a tier holding the caller's reference. A configuration the
 * build cannot serve (no TSDB_EC) is refused with a line. dir: the database, for messages. */
int tier_open(const char *dir, const tsdb_options *opt, tier **out);
tier *tier_get(tier *t);  /* another reference; NULL stays NULL */
void tier_put(tier *t);   /* the last one closes the tier (its settle thread, cache, backend) */
void tier_quiesce(tier *t); /* tsdb_close, before the volumes close: no settle runs from here on */
const tier_cfg *tier_config(const tier *t);
uint32_t tier_refs(const tier *t); /* for tests */

/* r7-tier. The backend (NULL without a tier; reads go through it whatever the stores' states). */
struct chunk_backend *tier_backend(const tier *t);
/* Whether settles and chunk deletes may go on: every store UP and none failed in service (chunk_store_failed, which
 * chunk_be_store_state reports as DOWN_IOERR). The first false logs one line naming the store and its state; from then
 * on the tier stays halted until the handle is opened again. r7-settle and r7-gc ask it before each settle and each
 * batch of deletes (a put to a DOWN store is refused anyway; a delete would skip it and leave its chunks behind). */
bool tier_writable(tier *t);
/* The volume's uuid (META's vuuid=, made once by the first open with a tier), for chunk idents; NULL without a tier. */
const uint8_t *tier_vuuid(const eng *db);
/* A follower's MANIFEST names an .ec part (ecpart_follower_open): the follower stops (eng_fail, one line; the code is
 * TSDB_ERR_IO, which follow_refresh passes up without its "broken part, keeping the previous view" line): it cannot
 * read the part (D7). What "stops" means: tsdb_failed reports it, and every refresh that meets an .ec name fails, so the
 * view does not move past the last MANIFEST without one; tsdbd sees tsdb_failed and exits. An embedded follower's queries
 * still answer from that last view (no query path checks eng_fail), as a follower that lags would: rows the writer
 * has since settled are the same rows, and the parts it maps stay readable while they are retired. */
int tier_follower_ec(eng *db, const char *name);
/* Planted bugs (tests/test_tier.c --mutant NAME; the Makefile requires each to fail it): each engine opens its own
 * backend (the second volume is BUSY); no chunk_be_see_epoch at attach (an unsure EPOCH stays unsure); a build without
 * TSDB_EC opens a requires=tier volume; the strict chunk_be_open (a missing store fails the open); a tier is new when
 * no store holds a volume, whatever the root's META says (stores all missing or empty are made again); no check that
 * every store UP holds the volume's epoch after the raise (one that could not store it stays UP, unfenced). */
enum { TIER_MUT_NONE = 0, TIER_MUT_PERENGINE, TIER_MUT_UNSURE, TIER_MUT_NOEC, TIER_MUT_STRICT, TIER_MUT_FRESH,
       TIER_MUT_FENCE };
extern int tier_mutant;

/* The engine side. tier_want(t, vol) on the opening thread names the tier and volume number the next engine opened
 * on that thread attaches to (eng_open_sched, through tier_engine_attach after META is read); the opener clears it
 * with tier_want(NULL, 0) after the open, whatever it returned. tier_engine_attach takes the wish (once) and a
 * reference into db->tier; tier_engine_detach (eng_close) gives it back. */
void tier_want(tier *t, uint32_t vol);
int tier_engine_attach(eng *db);
void tier_engine_detach(eng *db);

/* The per-engine state of R7's files, db->ecstate (made by tier_engine_attach when the engine has a tier). One pointer
 * per owner, each defined in its own file, so no two tracks edit one struct. tier_engine_attach calls ecgc_eng_open,
 * eckeep_eng_open, ecsettle_eng_open, ecswap_eng_open in that order, right after META is read (before recovery and
 * load_parts); a failure fails the open, whose eng_close runs tier_engine_detach, which calls the four closes in the
 * reverse order, each also for an owner whose open never ran or failed (its pointer NULL), then frees ecstate. */
struct ecstate {
    struct ecgc_eng *gc;         /* r7-gc: the delete queue (src/ecgc.c) */
    struct eckeep_eng *keep;     /* r7-keep: settled_* digests (src/eckeep.c) */
    struct ecsettle_eng *settle; /* r7-settle: policy state (src/settle.c) */
    struct ecswap_eng *swap;     /* r7-swap (src/ecswap.c) */
    uint8_t vuuid[16];           /* r7-tier: META's vuuid= (tier_vuuid) */
    struct chunk_backend *mut_be; /* r7-tier: the backend of the tierperengine mutant only */
};

#endif
