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
 * only on one server). TSDB_ERR_READONLY. TSDB_ERR_UNAVAILABLE: a store is DOWN or failed (tier_writable), or the
 * backend refused a put. TSDB_ERR_STOPPED: the engine is closing or halting, or a drop or a merge took a part
 * meanwhile. TSDB_ERR_CORRUPT: a granule, the index or the footer did not match its checksum, or rows contradict
 * CLASSES: the part is marked damaged (part_mark_payload) and nothing is written. TSDB_ERR_IO, TSDB_ERR_NOMEM.
 * Whatever the result, no part stays `settling`, and the chunks of an object that was not swapped in are handed to
 * r7-gc (an engine failure after the intent leaves them to the next open, which may roll the swap forward). The
 * caller holds no engine lock; jr_serial, flush_lock and cls_rw are never taken. */
int eng_settle(eng *db, int64_t partition, part **sel, size_t n, size_t *settled);

/* ------------------------------------------------------------ the policy (src/ecpolicy.c, E5) */
/* Picks what settles next in db (under db->lock, which the caller holds): the key into *key and its parts, referenced,
 * into sel[0 .. return value) (at most cap; the caller passes them to eng_settle and unrefs them). 0: nothing is due.
 * now_ms: eng_now_ms. */
size_t ecpolicy_pick_locked(eng *db, int64_t now_ms, int64_t *key, part **sel, size_t cap);
/* A settle of key failed (not STOPPED): the policy leaves the key alone for an hour. */
void ecpolicy_failed(eng *db, int64_t key, int64_t now_ms);
/* A settle happened (eager mode counts them: at most ECPOLICY_EAGER_PER_MIN a minute per engine). */
void ecpolicy_settled(eng *db, int64_t now_ms);
#define ECPOLICY_BACKOFF_MS (3600 * 1000LL)
#define ECPOLICY_EAGER_PER_MIN 30
#define ECPOLICY_EAGER_MIN_BYTES (1u << 20)
#define ECPOLICY_FAILS 64 /* keys remembered as failed per engine (the oldest is forgotten first) */

/* The policy's per-engine state (db->ecstate->settle; src/settle.c makes it, src/ecpolicy.c uses it under pmu) */
typedef struct ecpolicy_state {
    pthread_mutex_t pmu; /* a leaf */
    struct { int64_t key, at_ms; } fail[ECPOLICY_FAILS];
    size_t nfail;
    int64_t eager_win_ms; /* eager: the minute being counted, and its settles */
    int eager_n;
} ecpolicy_state;
ecpolicy_state *ecpolicy_of(eng *db);

/* ------------------------------------------------------------ tests */
/* The thread's poll interval in ms (1000; tests set it lower before the open). */
extern atomic_int settle_poll_ms;
/* Runs in a settle's fill, for every stripe: after the stripe's bytes were copied out of the map and before they are
 * checked (with the settletwopass mutant: between the check, which reads the map, and the copy). */
extern void (*_Atomic settle_fill_hook)(eng *db, const part *p, uint64_t stripe);
/* The hook's stripe once more, before the first stripe: the early copy of the header, index and footer is taken and
 * checked, nothing of the object is copied yet. */
#define SETTLE_HOOK_PREPARED UINT64_MAX
/* Counters for tests: settles that swapped, failed (CORRUPT and the others but STOPPED), abandoned (STOPPED), and the
 * settle thread's rounds. */
extern atomic_uint_fast64_t settle_done, settle_failed, settle_abandoned, settle_rounds;

/* planted bugs (tests/test_settle.c --mutant NAME, or TSDB_SETTLE_MUTANT=NAME in the environment; the Makefile requires
 * each to fail the test): ecrankdown: in memory only, an .ec part ranks below every other part of its partition (its
 * block numbers taken down after the swap; no name changes, so the open never fails on it); settlenocheck: no granule
 * crc, no index or footer comparison, no class check; settletwopass: the checks read the map, the encode a second copy;
 * settlebusy: the thread settles a key while a merge of it is selectable; coarsewhole: a coarse key settles all its
 * parts once one is due */
enum { SETTLE_MUT_NONE = 0, SETTLE_MUT_RANKDOWN, SETTLE_MUT_NOCHECK, SETTLE_MUT_TWOPASS, SETTLE_MUT_BUSY,
       SETTLE_MUT_COARSEWHOLE };
extern int settle_mutant;
int settle_mutant_parse(const char *name); /* -1 when unknown */
/* For tests: the streaming xxh64 the fill hashes the object with, fed lens[0..n) bytes of buf in turn (it must equal
 * xxh64(buf, sum of lens, 0) however the bytes are split) */
uint64_t settle_xxh64_pieces(const uint8_t *buf, const size_t *lens, size_t n);
#endif
