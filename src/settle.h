/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The settle of parts into objects and the settle thread of a tier (R7, r7-settle; design-cluster-data.md E5-E7, E26,
 * §2.3 steps 1-2 and 6, §2.4; the plan's D1, D2, D4, D11: nice 19, one settle at a time, beside the merge threads,
 * which merge_threads = 0 does not stop). src/settle.c has the design of a settle, src/ecpolicy.c the policy that
 * chooses what settles and when.
 *
 * Hooks of src/tier.h: t->settle is made by ecsettle_tier_open (tier_open), stopped by ecsettle_tier_quiesce
 * (tsdb_close, before the volumes close: no settle runs from there on) and freed by ecsettle_tier_close (the last
 * tier_put; also after a failed or missing open). db->ecstate->settle: ecsettle_eng_open, ecsettle_eng_close
 * (tier_engine_attach, tier_engine_detach). The engine's own two (src/db.c): ecsettle_eng_ready at the end of a
 * writer's open (the thread may take the engine from then on), ecsettle_eng_stop in eng_halt and eng_close once the
 * merge threads are joined and before the parts go (a settle of the engine is abandoned and waited for).
 */
#ifndef TSDB_SETTLE_H
#define TSDB_SETTLE_H
#include "internal.h"
struct tier;
int ecsettle_tier_open(struct tier *t);
void ecsettle_tier_quiesce(struct tier *t);
void ecsettle_tier_close(struct tier *t);
int ecsettle_eng_open(eng *db);
void ecsettle_eng_close(eng *db);
void ecsettle_eng_ready(eng *db);
void ecsettle_eng_stop(eng *db);

/* Settles the parts sel[0 .. n) of the partition key `partition` (the settle thread's, and a test hook): each part is
 * pinned (`settling`, under db->lock), encoded in one pass into an object of the tier's chunk store, its .ec file
 * written, and all of them swapped in by one ec_swap_commit. Parts that are not listed, not of this key, merging,
 * settling, .ec, damaged, without granules or carried over from format 8 are skipped (left as they are); a key that
 * a drop is retiring is skipped whole. 0: every pinned part is now an .ec part (the count in *settled when not NULL).
 * TSDB_ERR_NOTFOUND: nothing to settle. TSDB_ERR_INVALID: no tier, or a cluster or replica volume (D1: R7 settles
 * only on one server) that is not the leader of a cluster ec volume (R8, D21: src/eccold.c settles there, with a gate
 * and a MK_COLD marker; TSDB_ERR_INDETERMINATE when the leader was deposed after the marker went out). TSDB_ERR_READONLY. TSDB_ERR_UNAVAILABLE: a store is DOWN or failed (tier_writable), or the
 * backend refused a put. TSDB_ERR_STOPPED: the engine is closing or halting, or a drop or a merge took a part
 * meanwhile. TSDB_ERR_CORRUPT: a granule, the index or the footer did not match its checksum, or rows contradict
 * CLASSES: the part is marked damaged (part_mark_payload) and nothing is written. TSDB_ERR_IO, TSDB_ERR_NOMEM.
 * Whatever the result, no part stays `settling`, and the chunks of an object that was not swapped in are handed to
 * r7-gc (an engine failure after the intent leaves them to the next open, which may roll the swap forward). The
 * caller holds no engine lock. On one server jr_serial, flush_lock and cls_rw are never taken; on a cluster leader
 * eccold_settle takes jr_serial through lw_enter (the gate, like a large write) after the puts and holds it across a
 * memtable flush (flush_lock, cls_rw), the commit-point wait, the marker and the swap (publish_lock > latest_rw >
 * db->lock), so the lock order there is jr_serial > flush_lock > ... as a large write's. */
int eng_settle(eng *db, int64_t partition, part **sel, size_t n, size_t *settled);

/* Re-encodes the contiguous sources sel[0 .. n) of partition `partition` (at least one .ec, optional young .part files)
 * into a single .ec object (or an empty .part when tombstones or retention remove every row), verifying the logical
 * digest (ec_logical_digest_frozen) before swapping via ec_swap_reencode. An .ec source that is DOWN (UNAVAILABLE) or
 * CORRUPT aborts the re-encode without marking the part damaged or bumping merge_fail_seq. 0: swapped (*reencoded set
 * to the number of input parts combined when non-NULL). */
int eng_reencode(eng *db, int64_t partition, part **sel, size_t n, size_t *reencoded);

/* The two halves of a settle of one part, for src/eccold.c (a leader puts the object before its gate and writes the
 * .ec file, whose record carries the cut, after it). ecsettle_put: the geometry, a fresh oid, the record (r owned by
 * the caller from 0 on: ec_record_free) and the put of the object under the volume's epoch (into *epoch when not
 * NULL); a failure hands what was put to r7-gc and leaves r freed. ecsettle_index: the tmp_*.ec of r and the .ec part
 * opened from it (pending names the temporary); a failure removes the temporary and hands the object to r7-gc.
 * ecsettle_gc_object: hands an object no .ec part names to r7-gc. */
struct ec_record;
int ecsettle_put(eng *db, part *p, struct ec_record *r, uint64_t *epoch);
int ecsettle_put_pos(eng *db, part *p, struct ec_record *r, uint64_t *epoch, uint32_t posmask,
                     uint32_t *written_mask_out);
int ecsettle_index(eng *db, part *p, const struct ec_record *r, part **out);
void ecsettle_gc_object(eng *db, const struct ec_record *r);
/* whether db is closing or halting (a settle in progress gives up: STOPPED) */
bool ecsettle_stopping(eng *db);
/* R9 (E10): per-engine degraded-settle wait state (src/eccold.c), owned by ecsettle_eng. */
typedef struct eccold_deg_wait {
    uint32_t down_mask;    /* stripe-position bitmap observed down when down_since_ms started */
    int64_t down_since_ms; /* monotonic ms when down_mask was first seen down (0: not waiting) */
    int64_t last_seen_ms;  /* monotonic ms of the most recent check that observed down_mask */
    bool settled;          /* true after down_mask already waited wait_max_s and settled degraded */
    bool one_logged;       /* logged the wait for 1 position down */
    bool two_logged;       /* logged that >= 2 positions are down (never degraded) */
    _Atomic bool waiting;  /* last eccold_settle returned UNAVAILABLE only to wait: do not 1-hour backoff */
} eccold_deg_wait;
eccold_deg_wait *ecsettle_deg_wait(eng *db);

/* ------------------------------------------------------------ the policy (src/ecpolicy.c, E5, R15 D6) */
/* Picks what settles next in db (under db->lock, which the caller holds): the key into *key and its parts, referenced,
 * into sel[0 .. return value) (at most cap; the caller passes them to eng_settle and unrefs them). 0: nothing is due.
 * now_ms: eng_now_ms. */
size_t ecpolicy_pick_locked(eng *db, int64_t now_ms, int64_t *key, part **sel, size_t cap);
/* R15 (D6, E55): sample db's volume filesystem integer used % (floor of used * 100 / total, using f_bavail) via
 * fstatvfs(db->dirfd) at most once per second per engine. Called outside db->lock (never on the append path). */
uint32_t ecpolicy_sample_fill(eng *db);
/* Record an externally sampled fill % (from tierfill's per-volume sampler) into db's cached fill_pct (and
 * fill_sample_ms when real_sample is true, so real statvfs stays <= 1/s across both settle and sampler threads). */
void ecpolicy_record_fill(eng *db, uint32_t pct, bool real_sample);
/* True if at least ECPOLICY_FILL_SAMPLE_MS (1 s) has elapsed since db's last real statvfs/fstatvfs sample. */
bool ecpolicy_fill_due(const eng *db);
/* Active test override for volume fill % (0..100), or -1 when real statvfs/fstatvfs should be used. */
int ecpolicy_test_fill_override(void);
/* Last sampled (or test-overridden) used % of db's volume filesystem (0..100). Lock-free, no syscall. */
uint32_t ecpolicy_fill_pct(const eng *db);
/* R15 (D6, E55): monotonic counter of settle-thread rounds in which a due settle was deferred on a single-server tier
 * because not all directory stores (always >= k'+m; placement hashes across every store) were UP (DOWN or REPLACING),
 * a store exceeded the 95% regular-put cap, or the tier was halted/unwritable. */
uint64_t eng_tier_settles_waiting(const eng *db);
uint64_t ecsettle_tier_waiting(const struct tier *t);
/* R15 r15-full: current number of tiered volumes on `t` with a due settle waiting (stores down, full, or halted).
 * Exported as the Prometheus gauge tsdb_tier_settles_waiting. */
uint64_t ecsettle_tier_waiting_now(const struct tier *t);
/* True while db's most recent settle round on a single-server tier found a due settle blocked by store availability
 * or a halted/unwritable tier. */
bool eng_tier_settle_is_waiting(const eng *db);
/* R14 (r14-progress, D23): lock-safe settle backlog getter for one engine (non-blocking, takes db->lock briefly).
 * Sets *keys_waiting to partition keys due to settle and *oldest_wait_s to seconds past settle eligibility. */
void ecpolicy_backlog(eng *db, uint32_t *keys_waiting, uint64_t *oldest_wait_s);
/* R10 (E6): picks a contiguous range of parts of one key to re-encode (caller holds no engine lock): > ec_objects_max
 * combinable .ec objects, or tombstones in newer combinable parts covering > 10% of .ec rows (evaluated outside
 * db->lock); bounded by max_part_bytes and MAX_MERGE_PARTS; skips keys with a pending eckeep digest or recent
 * re-encode failure. Referenced parts into sel. */
size_t ecpolicy_pick_reencode(eng *db, int64_t now_ms, int64_t *key, part **sel, size_t cap);
/* tests: the cluster branch takes every key as too big for one marker (it says so and marks the key failed) */
extern bool ecpolicy_test_cap_hit;
/* tests (active only under TSDB_TEST_HOOKS): override the filesystem fill % (0..100; < 0 restores real fstatvfs). */
void ecpolicy_set_test_fill_pct(int pct);
/* tests: count of real fstatvfs() calls performed by ecpolicy_sample_fill (proves <= 1/s rate limit). */
extern atomic_uint_fast64_t ecpolicy_fill_samples;
/* A settle of key failed (not STOPPED): the policy leaves the key alone for an hour. */
void ecpolicy_failed(eng *db, int64_t key, int64_t now_ms);
/* A settle happened (eager mode counts them: at most ECPOLICY_EAGER_PER_MIN a minute per engine). */
void ecpolicy_settled(eng *db, int64_t now_ms);
/* A re-encode of key failed (not STOPPED): per-key backoff for ECPOLICY_BACKOFF_MS. */
void ecpolicy_reencode_failed(eng *db, int64_t key, int64_t now_ms);
/* A re-encode happened (eager mode counts them: at most ECPOLICY_EAGER_REENCODE_PER_MIN a minute per engine). */
void ecpolicy_reencoded(eng *db, int64_t now_ms);
#define ECPOLICY_BACKOFF_MS (3600 * 1000LL)
#define ECPOLICY_FILL_SAMPLE_MS 1000LL
#define ECPOLICY_EAGER_PER_MIN 30
#define ECPOLICY_EAGER_REENCODE_PER_MIN 15
#define ECPOLICY_EAGER_MIN_BYTES (1u << 20)
#define ECPOLICY_OBJECTS_MAX_DEFAULT 4
#define ECPOLICY_TOMB_PCT 10
#define ECPOLICY_FAILS 64 /* keys remembered as failed per engine (the oldest is forgotten first) */
/* E6: max combinable .ec objects per key before re-encode triggers (default ECPOLICY_OBJECTS_MAX_DEFAULT = 4). */
extern atomic_uint ecpolicy_objects_max;

/* The policy's per-engine state (db->ecstate->settle; src/settle.c makes it, src/ecpolicy.c uses it under pmu) */
typedef struct ecpolicy_state {
    pthread_mutex_t pmu; /* a leaf */
    struct { int64_t key, at_ms; } fail[ECPOLICY_FAILS];
    size_t nfail;
    struct { int64_t key, at_ms; } rfail[ECPOLICY_FAILS];
    size_t nrfail;
    int64_t eager_win_ms; /* eager: the minute being counted, and its settles */
    int eager_n;
    int64_t eager_rwin_ms; /* eager: the minute being counted for re-encodes */
    int eager_rn;
} ecpolicy_state;
ecpolicy_state *ecpolicy_of(eng *db);

/* ------------------------------------------------------------ tests */
/* The thread's poll interval in ms (1000; tests set it lower before the open). */
extern atomic_int settle_poll_ms;
/* tests: while set, the settle thread skips automatic re-encodes (explicit eng_reencode still runs). */
extern atomic_bool settle_reencode_pause;
/* Runs in a settle's fill, for every stripe: after the stripe's bytes were copied out of the map and before they are
 * checked (with the settletwopass mutant: between the check, which reads the map, and the copy). */
extern void (*_Atomic settle_fill_hook)(eng *db, const part *p, uint64_t stripe);
/* The hook's stripe once more, before the first stripe: the early copy of the header, index and footer is taken and
 * checked, nothing of the object is copied yet. */
#define SETTLE_HOOK_PREPARED UINT64_MAX
/* Counters for tests: settles and re-encodes that swapped, failed (CORRUPT and the others but STOPPED), abandoned
 * (STOPPED), and the settle thread's rounds. */
extern atomic_uint_fast64_t settle_done, settle_failed, settle_abandoned, settle_rounds;
extern atomic_uint_fast64_t reencode_done, reencode_failed, reencode_abandoned;
/* A failure planted in settle_one (tests): the call after settle_test_fail_skip passing ones fails at the step
 * settle_test_fail_at names with TSDB_ERR_IO, as the real step's failure would leave it: PUT after a put that committed
 * every position (a failure while committing), INDEX in place of ec_index_write (no tmp .ec), OPEN in place of
 * part_open_ec (the tmp .ec written). 0: none. */
enum { SETTLE_FAIL_NONE = 0, SETTLE_FAIL_PUT, SETTLE_FAIL_INDEX, SETTLE_FAIL_OPEN };
extern atomic_int settle_test_fail_at, settle_test_fail_skip;

/* planted bugs (tests/test_settle.c --mutant NAME, or TSDB_SETTLE_MUTANT=NAME in the environment; the Makefile requires
 * each to fail the test): ecrankdown: in memory only, an .ec part ranks below every other part of its partition (its
 * block numbers taken down after the swap; no name changes, so the open never fails on it); settlenocheck: no granule
 * crc, no index or footer comparison, no class check; settletwopass: the checks read the map, the encode a second copy;
 * settlebusy: the thread settles a key while a merge of it is selectable; coarsewhole: a coarse key settles all its
 * parts once one is due */
enum { SETTLE_MUT_NONE = 0, SETTLE_MUT_RANKDOWN, SETTLE_MUT_NOCHECK, SETTLE_MUT_TWOPASS, SETTLE_MUT_BUSY,
       SETTLE_MUT_COARSEWHOLE,
       /* the chunk handoffs of the failure paths (int-h; test_settle --case gcpaths, gcpower): gcskipput, gcskipindex,
        * gcskipopen: settle_one hands no object to r7-gc after a failed put, index write, open of the .ec; gcskipelse:
        * eng_settle keeps the objects of the parts settled before a later one failed; gcskipswap: nor those of a swap
        * refused with nothing written; gcskipdiscard: swap_discard (a STOPPED swap) hands none over; gccommitted: a
        * swap that committed hands its objects over all the same; gcdouble: a NOMEM intent write is cleaned up by
        * the swap and by the caller (REVIEW F7); gcearly: swap_discard hands the objects over before the directory
        * sync that makes the temporaries' unlinks durable (a power cut then rolls the intent forward onto deleted
        * chunks); nullnosync: no such sync after a failed intent write (whose name another thread's sync may have
        * made durable); syncfailgo: the objects go to r7-gc although that sync failed (the engine failed, so the
        * unlinks may never reach the disk); nohaltstop: eng_halt does not stop the engine's settle (a settle of a
        * halted engine, a volume being reopened, would go on beside the new engine: test_settle --case halt) */
       SETTLE_MUT_GCSKIPPUT, SETTLE_MUT_GCSKIPINDEX, SETTLE_MUT_GCSKIPOPEN, SETTLE_MUT_GCSKIPELSE, SETTLE_MUT_GCSKIPSWAP,
       SETTLE_MUT_GCSKIPDISCARD, SETTLE_MUT_GCCOMMITTED, SETTLE_MUT_GCDOUBLE, SETTLE_MUT_GCEARLY,
       SETTLE_MUT_NULLNOSYNC, SETTLE_MUT_SYNCFAILGO, SETTLE_MUT_NOHALTSTOP,
       /* R10 re-encode mutants (tests/test_settle.c --mutant NAME): reencodedroptomb: drops tombstones covering older
        * parts during re-encode; reencoderankdown: ranks the re-encoded output below other parts in memory;
        * reencodemarkunavail: marks an .ec source damaged on TSDB_ERR_UNAVAILABLE during re-encode */
       SETTLE_MUT_DROPTOMB, SETTLE_MUT_RERANKDOWN, SETTLE_MUT_MARKUNAVAIL,
       /* R15 settle policy mutants (tests/test_settle.c --case r15policy --mutant NAME): afterignored: uses grace_s
        * instead of tier_after_s on a TIERS-opened tier; highnotoldest: picks the newest eligible key instead of the
        * oldest under tier_high_pct; settledegraded: settles on a single-server tier while a store is DOWN/REPLACING */
       SETTLE_MUT_AFTERIGNORED, SETTLE_MUT_HIGHNOTOLDEST, SETTLE_MUT_SETTLEDEGRADED };
extern int settle_mutant;
int settle_mutant_parse(const char *name); /* -1 when unknown */
/* For tests: the streaming xxh64 the fill hashes the object with, fed lens[0..n) bytes of buf in turn (it must equal
 * xxh64(buf, sum of lens, 0) however the bytes are split) */
uint64_t settle_xxh64_pieces(const uint8_t *buf, const size_t *lens, size_t n);
#endif
