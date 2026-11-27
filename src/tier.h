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
#include "tierfill.h"
#include "tiers.h"

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
    _Atomic(struct tier_health *) health; /* r15-health: probes & domain hooks (freed only in tier_put) */
    _Atomic(struct tierctl *) ctl;        /* r15-lost: single tier worker, TIERS re-read, paced rebuild */
    /* r7-tier: the backend over cfg.dirs, opened once by tier_open (chunk_be_open2, min_up = stores - m) and closed by
     * the last tier_put: a store flocks its directory, so one backend per engine would make the second volume BUSY.
     * halted: settles and chunk deletes are paused while any store is not UP (cleared when all stores return to UP,
     * unless fence_halted is set by an unfenced UP store at open). One line per pause. */
    struct chunk_backend *be;
    atomic_bool halted;
    atomic_bool fence_halted;
    /* r8b-coldops: the registry of attached volumes (tier_cold_get), one reference each; cmu is a leaf */
    pthread_mutex_t cmu;
    pthread_cond_t eng_cv;
    struct coldvol **cvs;
    size_t ncvs;
    /* r15-health: attached single-server engines (under cmu), for heal live() and missed-delete sweep */
    eng **engs;
    size_t nengs, eng_cap;
    size_t eng_opening; /* engines between tier_engine_attach and tier_engine_ready (parts not loaded yet) */
    unsigned eng_active;
    char *dir; /* database root path (canonical if available), for tier_find_by_dir */
    /* R15 (r15-open, r15-follow): loaded TIERS view & db_id when opened from TIERS, read-only follower flag, plus
     * TSDB_TEST_TIER grace override flag */
    bool from_tiers;
    bool read_only;
    uint8_t db_id[16];
    tiers_view tview;
    bool ov_grace;
} tier;

/* Default geometry by store count (R15 D4, D5): all_single_copy -> 1+0; else 2 -> 1+1, 3..5 -> (n-1)+1, 6+ -> 4+2.
 * Returns false when nstores is too small (<= 0, or 1 non-single-copy store). */
bool tier_default_geom(int nstores, bool all_single_copy, int *k_out, int *m_out);

/* tsdb_open's: *out NULL when no tier is configured, else a tier holding the caller's reference. A configuration the
 * build cannot serve (no TSDB_EC) is refused with a line. dir: the database, for messages. */
int tier_open(const char *dir, const tsdb_options *opt, tier **out);
tier *tier_get(tier *t);  /* another reference; NULL stays NULL */
tier *tier_try_get(tier *t); /* another reference iff refs > 0, else NULL */
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
/* A follower without a tier attached meets an .ec part in MANIFEST (ecpart_follower_open): the follower stops
 * (eng_fail, one line; the code is TSDB_ERR_IO, which follow_refresh passes up without its "broken part, keeping the
 * previous view" line). Followers of a TIERS database instead open a read-only tier (R15 D13: stores on shared
 * storage, opened without flock/EPOCH writes/deletes, store ids and db_id checked; missing non-LOST store or wrong
 * db_id refused at start) and read .ec parts through their own eccache. A read-only tier keeps the TIERS store set
 * seen at open and does not run the health or attach worker: a store replaced/removed later or hitting runtime EIO
 * stays DOWN until the follower reopens, while reads decode around it as long as k positions remain. */
int tier_follower_ec(eng *db, const char *name);
/* R15 D13 (r15-follow): ensures a read-only/follower engine that opened before the writer wrote vuuid= into META has
 * loaded vuuid= and registered its read-only coldvol before opening an .ec part. No-op once loaded or on writers. */
int tier_follower_ensure_vuuid(eng *db);
/* Planted bugs (tests/test_tier.c --mutant NAME; the Makefile requires each to fail it): each engine opens its own
 * backend (the second volume is BUSY); no chunk_be_see_epoch at attach (an unsure EPOCH stays unsure); a build without
 * TSDB_EC opens a requires=tier volume; the strict chunk_be_open (a missing store fails the open); a tier is new when
 * no store holds a volume, whatever the root's META says (stores all missing or empty are made again); no check that
 * every store UP holds the volume's epoch after the raise (one that could not store it stays UP, unfenced). */
enum { TIER_MUT_NONE = 0, TIER_MUT_PERENGINE, TIER_MUT_UNSURE, TIER_MUT_NOEC, TIER_MUT_STRICT, TIER_MUT_FRESH,
       TIER_MUT_FENCE,
       TIER_MUT_SELFEPOCH, /* selfepoch: the epoch provider is bypassed at attach: the stores' own fence (the writer's
                            * epoch) is given as the authoritative one, so an unsure store is raised by it */
       TIER_MUT_DBIDSKIP,  /* dbidskip: skips db_id verification when opening stores from TIERS */
       TIER_MUT_LOSTOPEN,  /* lostopen: opens a LOST store as UP instead of passing NULL (DOWN_MISSING) */
       TIER_MUT_DOWNNOREAD,   /* downnoread: reads are still sent to a DOWN store */
       TIER_MUT_ATTACHNOHEAL, /* attachnoheal: returning store marked UP without healing */
       TIER_MUT_FOLLOWFLOCK,  /* followflock: label for CHUNK_MUT_FOLLOW_FLOCK (follower takes exclusive store flock) */
       TIER_MUT_FOLLOWDEL     /* followdel: label for GC_MUT_FOLLOWDEL (writer deletes retired chunks immediately) */ };
extern int tier_mutant;

/* R15 (r15-health): per-store probes, runtime store DOWN, queued attach/heal worker, and tsdbd --health-domain hooks. */
int tier_health_open(tier *t);
void tier_health_quiesce(tier *t);
bool tier_health_close(tier *t); /* returns true if a detached attach worker still references t */
void tier_health_kick_reopen(tier *t);
tier *tier_find_by_dir(const char *dir);
int tier_store_count(const tier *t);
bool tier_store_path(const tier *t, int i, char *buf, size_t cap);
int tier_store_state(const tier *t, int i);
void tier_store_set_down(tier *t, int store, int reason);
void tier_ensure_paused(tier *t, int store, const char *why);
void tier_resume_if_all_up(tier *t);
void tier_health_domain_state(tier *t, int store, bool down);
void tier_health_test_opts(int stall_ms, int every_ms, int ok_need, int fast_ms);
extern atomic_uint_fast64_t tier_health_attach_attempts;
/* R15 (r15-lost): tier_health <-> tierctl single-worker coordination */
struct chunk_heal_opts;
void tier_health_set_wake(tier *t, void (*fn)(void *), void *ctx);
bool tier_health_take_reopen(tier *t);
bool tier_health_has_pending(tier *t);
bool tier_health_run_one(tier *t, const struct chunk_heal_opts *ho_tpl, int64_t *wait_ms_out);
bool tier_health_any_settling(tier *t);
int tier_health_live_oids(tier *t, uint8_t (**oids_out)[16], size_t *n_out, bool *complete_out);
void tier_health_store_lost(tier *t, int store);
void tier_health_store_replaced(tier *t, int store, const char *new_path, const uint8_t new_id[16]);
int tier_health_store_added(tier *t, int store, const char *path, const uint8_t id[16], bool active);

/* r8b-coldops (E22). A volume's settings: the tier's configuration on a single server (tier_settings_from_cfg); r8f
 * gives a cluster volume its own from tsdbd's keys (D22). cache_mb is the tier's (one cache per handle), read only by
 * tier_open. Live-updatable TIERS fields are atomic so tierctl's TIERS re-read never races with settle/ecpolicy. */
typedef struct tier_settings {
    _Atomic int k, m;
    _Atomic uint32_t km_packed; /* (k << 16) | m: single atomic word for tear-free live redundancy updates */
    _Atomic int64_t grace_s, keep_s, coarse_s;
    uint64_t cache_mb;
    bool eager;
    /* r8g-cut, a cluster leader's settle (src/eccold.c; ec.settle_force_s, ec.settle_wait_max_s): how long past
     * grace_s the policy waits for the memtable to let go of the key's rows by itself before the gate flushes it, and
     * how long a settle waits for every position to be UP under the stripe version it put by. 0: the defaults
     * (ECCOLD_FORCE_S_DEFAULT, ECCOLD_WAIT_MAX_S_DEFAULT). */
    int64_t force_s, wait_max_s;
    /* R15 (r15-open, D6, D7, D12, D16, D20): settings from TIERS (from_tiers false when not opened from TIERS). */
    bool from_tiers;
    _Atomic uint64_t tier_after_s;
    _Atomic uint32_t tier_high_pct;
    _Atomic uint32_t young_high_pct;
    _Atomic uint32_t rebuild_mb_s;
    _Atomic uint32_t scrub_mb_s;
    _Atomic uint32_t scrub_days;
} tier_settings;
static inline void tier_settings_set_km(tier_settings *s, int k, int m)
{
    uint32_t packed = ((uint32_t)(uint16_t)k << 16) | (uint32_t)(uint16_t)m;
    atomic_store_explicit(&s->km_packed, packed, memory_order_release);
    atomic_store_explicit(&s->k, k, memory_order_relaxed);
    atomic_store_explicit(&s->m, m, memory_order_relaxed);
}
static inline void tier_settings_get_km(const tier_settings *s, int *k_out, int *m_out)
{
    uint32_t packed = atomic_load_explicit(&((tier_settings *)s)->km_packed, memory_order_acquire);
    if (packed != 0) {
        *k_out = (int)(packed >> 16);
        *m_out = (int)(packed & 0xffffu);
    } else {
        *k_out = atomic_load_explicit(&((tier_settings *)s)->k, memory_order_relaxed);
        *m_out = atomic_load_explicit(&((tier_settings *)s)->m, memory_order_relaxed);
    }
}
void tier_settings_from_cfg(const tier_cfg *c, tier_settings *s);

/* The epoch provider of an attached volume: the epoch every put and delete of the volume carries and the one an unsure
 * store is raised by (never the writer's own: E14). 0 is "not known now": the attach fails, a settle or delete waits.
 * NULL in tier_attach: the constant 1 (R7's D4). Called from any thread, also after the engine that attached with it
 * closed: the tier's registry keeps a volume's latest coldvol (with fn and arg) until the volume's next successful
 * attach or tier_close, and the gc worker asks it for the epoch of deletes queued by a closed engine. So arg outlives
 * the tier (or at least every coldvol that names it), never just the engine.
 * The epoch is seen by the positions (ops->see_epoch) only at attach. A provider whose epoch moves while the engine
 * runs (r8f: a promotion) must have see_epoch called again by whoever moves it before a put or delete carries the new
 * one; until then such writes rely on a store raising itself on a put above its EPOCH (src/chunk.h), which an unsure
 * store refuses. */
typedef uint64_t (*tier_epoch_fn)(void *arg, uint32_t volume);

/* What a volume attaches with (tier_want_attach). Every field may be zero: the directory ops over the tier's stores
 * (coldops_dir_make with arg NULL), epoch 1, the tier's settings. make is called once per attach, after META's vuuid=
 * is known; the context it makes belongs to the attached volume's coldvol (freed by ops->ctx_free with the last
 * reference). The ops table itself must outlive the tier (a static table): the cache keeps it in open objects past
 * the context and the engine (src/eccache.c), so r8f never allocates ops per volume. */
struct cold_ops;
typedef struct tier_attach {
    const struct cold_ops *ops;
    int (*make)(void *arg, tier *t, uint32_t volume, const uint8_t vuuid[16], void **ctx);
    void *make_arg;
    tier_epoch_fn epoch;
    void *epoch_arg;
    const tier_settings *settings; /* copied at attach */
    uint64_t lead_epoch;           /* R8 (r8f F4, r8k-gc): if non-zero, pins this leader engine's own lead epoch */
} tier_attach;

/* R8 (r8f): a cluster tsdbd's ec volumes. Set once before tsdb_open (server/chunkpeer_cluster.c). With no
 * TSDB_TEST_TIER, tier_open then makes a tier with no stores of its own (cfg.set false, be NULL, the cache's budget
 * cache_mb): only cluster ec volumes (META requires=cluster and ec) attach to it, each with what fn fills in (the peer
 * cold ops, the copy's epoch provider, *s: the settings with the copy's k and m, which the attach copies); every other
 * volume opens without a tier. fn returns 0, or an error that fails the volume's open. Without TSDB_EC it is ignored
 * (a volume whose META says requires=tier is then refused at open, as on any build without the codec). */
typedef int (*tier_cluster_fn)(void *arg, uint32_t volume, tier_attach *a, tier_settings *s);
void tier_set_cluster(tier_cluster_fn fn, void *arg, uint64_t cache_mb);

/* One attached volume's cold side: its ops and their context, epoch provider and settings. Reference counted: the
 * engine holds one (db->ecstate->cold), the tier's registry one for the volume's latest successful attach (what
 * tier_cold_get finds: a reopen's new engine replaces the entry once its attach has seen the epoch, the retired engine
 * keeps its own; a failed attach leaves the entry as it was), and a lookup one while it calls an op. A queued delete
 * holds none: it names the volume and finds the entry when it runs (src/ecgc.c). A volume's vuuid never changes under
 * one tier (META's vuuid= is made once), so a delete through the latest entry's context carries the vuuid the item was
 * queued with. */
typedef struct coldvol {
    atomic_uint refs;
    uint32_t volume;
    uint8_t vuuid[16];
    const struct cold_ops *ops;
    void *ctx;
    tier_epoch_fn epoch_fn;
    void *epoch_arg;
    _Atomic uint64_t lead_epoch; /* R8 (r8f F4): non-zero on a cluster leader: puts/deletes always use this epoch */
    tier_settings set;
} coldvol;
/* The engine's (no reference taken: valid while it is attached); NULL without a tier. */
coldvol *tier_cold(const eng *db);
/* The volume's latest attach in the tier, with a reference (tier_cold_put), or NULL. */
coldvol *tier_cold_get(tier *t, uint32_t volume);
coldvol *tier_cold_ref(coldvol *cv);
void tier_cold_put(coldvol *cv);
/* The volume's epoch from its provider (0: unknown, or no such volume attached). */
uint64_t tier_epoch(tier *t, uint32_t volume);
uint64_t coldvol_epoch(const coldvol *cv);
/* The engine's settings (NULL without a tier). */
const tier_settings *tier_eng_settings(const eng *db);
/* Whether the engine's volume may settle and delete now (its ops' writable). */
bool tier_cold_writable(const eng *db);
/* coldops_dir.c: store i did not take the volume's epoch (or is not UP): the tier halts, one line (tier_writable). */
void tier_halt_store(tier *t, int i, const char *why);

/* The engine side. tier_want(t, vol) on the opening thread names the tier and volume number the next engine opened
 * on that thread attaches to (eng_open_sched, through tier_engine_attach after META is read); the opener clears it
 * with tier_want(NULL, 0) after the open, whatever it returned. tier_engine_attach takes the wish (once) and a
 * reference into db->tier; tier_engine_detach (eng_close) gives it back. */
void tier_want(tier *t, uint32_t vol);
/* tier_want with what the volume attaches with (a: copied; NULL is tier_want): r8f's cluster volumes. */
void tier_want_attach(tier *t, uint32_t vol, const tier_attach *a);
int tier_engine_attach(eng *db);
void tier_engine_ready(eng *db);
void tier_engine_unreg(eng *db);
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
    tierfill_state fill;         /* r15-full: atomic FULL hysteresis flag & refusal count (src/tierfill.c) */
    uint8_t vuuid[16];           /* r7-tier: META's vuuid= (tier_vuuid) */
    bool has_vuuid;              /* r15-follow: true once vuuid= has been read and coldvol registered */
    struct coldvol *cold;        /* r8b-coldops: the volume's ops, epoch provider and settings (a reference) */
    struct chunk_backend *mut_be; /* r7-tier: the backend of the tierperengine mutant only */
    bool tier_opening;           /* r15-health: counted in tier->eng_opening until tier_engine_ready */
    bool tier_registered;        /* r15-health: listed in tier->engs[] until tier_engine_unreg */
};

#endif
