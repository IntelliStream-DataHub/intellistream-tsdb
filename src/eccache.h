/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The seam between an .ec part's granules and the chunk store (R7, r7-seam; design-cluster-data.md E11, §3.1-3.3):
 * the granule cache of the tsdb handle's tier and the LRU of open objects, reached through p->ec (its tier: one per
 * handle, so one cache per handle).
 *
 * A granule's block bytes are a cache entry keyed by (oid, granule offset). A reader pins the entry and decodes from
 * it (never a copy): a pinned entry is never evicted or freed. A miss reads the bytes with CHUNK_RR_RAW (only the
 * granule's own bytes, from the same range of the others when a chunk is bad) and checks xxh64 against the index's
 * crc, always (E11: an .ec granule is checked whatever verify_checksums says); a mismatch reads again without RAW,
 * whole cells against the object record's sums (src/chunk.h), and checks again. A second mismatch, or an object that
 * has lost more than m chunks of a stripe, is TSDB_ERR_CORRUPT with one line naming volume, partition, part and
 * granule; a read that needs a store that is DOWN and cannot be made around it is TSDB_ERR_UNAVAILABLE (the bytes are
 * away, not lost). Never wrong bytes, and never part_mark_payload: an .ec part is never merged (r7-skip), and an
 * unreadable granule there says nothing about the part's index.
 *
 * The cache's mutex is a leaf (AGENTS.md "Lock order"): it is never held across I/O, an object's open or close, or a
 * decode. Entries live under a byte budget (cache_mb of TSDB_TEST_TIER, 0: 10% of RAM), LRU; a granule larger than
 * the budget is read, used by its reader and dropped. Objects (chunk_obj, one fd per chunk found) live under an fd
 * budget, LRU; an object a reader holds is never closed under it. Two readers that miss the same granule or object
 * read it once: the second waits for the first (on the cache's cv) and then hits. Every fill and open broadcasts on
 * that one cv, so each wakes every waiter (cheap at tens of threads; per-entry waits if it ever shows up).
 * The budget is per tier, so per handle: an embedded process with several handles has one cache each.
 *
 * No read here runs under db->lock, latest_rw, buf_lock or publish_lock: every caller of granule_read reads a
 * snapshot's granules with no engine lock held (the audit is in tests/test_seam.c's header).
 *
 * Residency (statistics): the cached bytes of the part's object in a range (eccache_resident), never its map.
 *
 * Prefetch (R8 D19, r8p-prefetch): a miss that continues a sequential walk (the granule after the last one a miss read
 * or read ahead, in the same series and part) reads the next N granules of that series with it, in one range read of
 * the object; N starts at 4 and doubles with each miss that continues the walk, up to a cap (32). The granules read
 * ahead are entries like any other, LOADING and pinned by the reader that reads them, so a second reader waits for them
 * instead of reading them again, and each is checked against its index crc before it turns READY: one that fails is
 * dropped silently (its own miss later reads it alone, with the second read without RAW, and refuses it as CORRUPT),
 * and a range read that fails drops the whole window and reads the demanded granule alone, as before. Bytes read ahead
 * and not yet used are held under a quarter of the budget, never more than 4 MiB per read; making room evicts from the
 * cold end, never a pinned entry. The window stops at a granule that is cached or loading, at another series, at a gap
 * between offsets and at the end of the part's granules. Counters: prefetched (granules read ahead and cached), used
 * (of those, hit), wasted (left the cache unused), dropped (failed their check or their read). The walks are tracked in
 * a few slots per cache (oid, the next index, N), so concurrent scans of other objects or series do not reset each
 * other unless more than 16 run at once.
 */
#ifndef TSDB_ECCACHE_H
#define TSDB_ECCACHE_H

#include "internal.h"

/* What a cache entry costs beyond its granule's bytes: struct ce, BLK_SLACK, two allocator headers, table slots.
 * The budget counts it (eccache.c ce_cost). */
#define ECCACHE_ENTRY_OVERHEAD 224

struct chunk_backend;
struct ec_record;

/* granule_read for an .ec part (p->ec set): the same contract (src/internal.h). A test-marked part (eng_test_mark_ec)
 * still reads its map. */
int ec_granule_read(codec_ctx *c, const part *p, const granule *g, int type, int64_t *ts, void *v, size_t cap,
                    bool verify);
/* The bytes an .ec part counts as its file in part_walk (statistics: the four byte columns add up to it): the
 * object's length. Runs under db->lock as well (stats_walk, eng_foreach_part): a number the part keeps. */
uint64_t eccache_part_bytes(const part *p);
/* Bytes of the object's [off, off + len) that are in the cache (the residency of the statistics). A test-marked part:
 * the page cache's residency of its map. */
uint64_t eccache_resident(const part *p, uint64_t off, uint64_t len);

/* The tier's hooks (src/tier.h): t->cache. */
int eccache_tier_open(struct tier *t);
void eccache_tier_close(struct tier *t);

/* The backend the cache reads from: the tier's, handed over by tier_open (owned by the tier, never closed here, and
 * open until the tier closes, after eccache_tier_close: readers use it without a reference). The cache never opens one
 * of its own: a store flocks its directory, so one beside the tier's would be BUSY (one backend per handle). Without
 * one (only test_tier's tierperengine mutant) every read is TSDB_ERR_UNAVAILABLE. Tests may hand another over while no
 * reader runs. */
void eccache_set_backend(struct tier *t, struct chunk_backend *be);
/* The backend (NULL with *rc = TSDB_ERR_UNAVAILABLE when none was handed over), which the directory ops read through,
 * and the volume's uuid (its latest attach's, tier_cold_get; all zero for a volume never attached). */
struct chunk_backend *eccache_backend(struct tier *t, int *rc);
void eccache_vuuid(struct tier *t, uint32_t volume, uint8_t out[16]);
/* r7-gc: the object's chunks are gone; its granules leave the cache (a read in flight finishes and is not cached) and
 * it is closed once no reader holds it (an object that is still opening included). Costs the object's entries only. */
void eccache_forget(struct tier *t, uint32_t volume, const uint8_t oid[16]);
/* Heal, replace and settle (chunkheal.c write new files) and a store coming back: the object is closed once no reader
 * holds it and the next reader opens it again (its cached granules stay: they were checked). A read that sees the
 * object degraded (a cell failed or rebuilt, a position away, an error) does the same by itself. */
void eccache_reopen(struct tier *t, uint32_t volume, const uint8_t oid[16]);

/* ------------------------------------------------------------ tests */
typedef struct eccache_counters {
    uint64_t hits, misses, waits;   /* granule lookups; waits: a miss another reader was already filling */
    uint64_t bytes, entries;        /* what the cache holds now */
    uint64_t evictions, uncached;   /* entries evicted; granules used once and dropped (larger than the budget) */
    uint64_t retries, corrupt;      /* second reads without RAW; granules refused as CORRUPT */
    uint64_t obj_opens, obj_closes; /* chunk_obj_open / chunk_obj_close */
    uint64_t objs_open, fds_open, fds_peak; /* now, and the highest count of fds the open objects held */
    uint64_t budget, fd_budget;
    uint64_t reads;                 /* range reads of objects (every obj_read: misses, second reads, windows) */
    uint64_t pf_reads;              /* of those, reads that carried granules ahead of the one asked for */
    uint64_t prefetched, pf_used, pf_wasted, pf_dropped; /* granules read ahead: cached, hit, left unused, refused */
    uint64_t pf_bytes, pf_peak, pf_budget; /* bytes read ahead and not yet used (cost, as bytes), highest, budget */
    uint64_t pf_call_peak;          /* the longest window read, in bytes (since eccache_test_prefetch_call) */
} eccache_counters;
void eccache_get_counters(struct tier *t, eccache_counters *out);
/* Budgets for tests (bytes; fds). The fd budget counts the chunk fds of the open objects (k' + m each at most). */
void eccache_test_budget(struct tier *t, uint64_t bytes);
void eccache_test_fd_budget(struct tier *t, uint64_t fds);
/* The prefetch cap in granules per read (0: off; the default is ECCACHE_PF_CAP); resets the walks. */
#define ECCACHE_PF_CAP 32
void eccache_test_prefetch(struct tier *t, unsigned cap);
/* The bytes of one window's range read at most (0: the default, 4 MiB); resets pf_call_peak. */
void eccache_test_prefetch_call(struct tier *t, uint64_t bytes);
/* While above 0, each window's range read fails (UNAVAILABLE) without reading, and the count goes down by one. */
extern _Atomic int eccache_test_pf_fail;
/* Pins the READY entry of (oid, off) as a reader does (NULL: none); eccache_test_unpin lets go of it. */
void *eccache_test_pin(struct tier *t, const uint8_t oid[16], uint64_t off);
void eccache_test_unpin(struct tier *t, void *entry);
/* Empties the cache of what no reader holds and closes every object no reader holds (a cold cache). */
void eccache_test_drop(struct tier *t);
/* Empties the cache of what no reader holds; the objects stay open. */
void eccache_test_drop_entries(struct tier *t);
/* Called (when set) after an object's open, before it is listed open, and after a granule's fill, before it is listed
 * READY; for a window read ahead, after its granules are chosen, before they are listed (PFPLAN), and after its range
 * read, before its granules are listed READY (PFREAD): a test forgets the object there to run its race without
 * threads. No lock of the cache is held. */
enum { ECC_HOOK_OPENED = 1, ECC_HOOK_FILLED, ECC_HOOK_PFPLAN, ECC_HOOK_PFREAD };
extern void (*eccache_test_hook)(struct tier *t, int where, const uint8_t oid[16]);

/* r7-seam's test hook (r7-keep, r7-gc, r7-oracle use it): encodes the bytes of p, a listed .part of db, into the
 * tier's chunk store as a new object (oid random, the tier's k and m, epoch 1) and attaches it: p->ec set (the one
 * store into it, under db->lock, as eng_test_mark_ec), so from then on every granule of p is read through the seam.
 * In memory only: a reopen lists the .part again. rec (may be NULL) gets a copy of the record (its sums malloc'ed).
 * TSDB_ERR_NOTFOUND (not listed), TSDB_ERR_LOCKED (merging), TSDB_ERR_INVALID (no tier, already .ec, or a part with
 * no granules), the backend's errors. Tests that attach run with merge_threads = 0 until r7-skip. */
int ec_test_attach(eng *db, part *p, struct ec_record *rec);
/* The same with the object encoded from src (p->map_len bytes; NULL: p->map): a test plants an object whose record
 * matches its chunks but whose bytes are not the part's. */
int ec_test_attach_from(eng *db, part *p, const uint8_t *src, struct ec_record *rec);
/* The same for an object already in the store (rec from an earlier ec_test_attach, before a reopen): nothing is
 * written. */
int ec_test_attach_object(eng *db, part *p, const struct ec_record *rec);

/* Planted bugs (tests/test_seam.c --mutant NAME; the Makefile requires each to fail it). */
enum {
    ECC_MUT_NONE = 0,
    ECC_MUT_NOVERIFY, /* seamnoverify: a miss's bytes are not checked against the granule's crc */
    ECC_MUT_NORETRY,  /* seamnoretry: a mismatch is CORRUPT at once, never read again without RAW */
    ECC_MUT_KEYOFF,   /* cachekeyoff: entries keyed by the offset alone, so two objects collide */
    ECC_MUT_NORECHECK, /* seamnorecheck: the whole-cell read's bytes are not checked against the crc */
    ECC_MUT_RETRYERR,  /* seamretryerr: a RAW read that failed is read again whole (double I/O while a store is away) */
    ECC_MUT_KEEPSTALE, /* seamkeepstale: an object a read saw degraded stays open (a healed chunk is never read) */
    ECC_MUT_FORGETNOOP, /* forgetnoop: eccache_forget does nothing */
    ECC_MUT_PFNOCHECK,  /* prefetchnocheck: granules read ahead turn READY without their crc check */
    ECC_MUT_PFEVICTPINNED, /* prefetchevictpinned: the room made for a window evicts pinned entries too */
    ECC_MUT_PFNODOUBLE,    /* prefetchnodouble: the window stays at 4 granules, never grows to the cap */
    ECC_MUT_PFDUPKEY,      /* prefetchdupkey: a window does not stop at a granule already cached (two entries, one key) */
    ECC_MUT_PFFORGETLIVE,  /* prefetchforgetlive: a window forgotten during its read is cached all the same */
    ECC_MUT_PFNOFALLBACK,  /* prefetchnofallback: a window's failed read fails the granule asked for */
};
extern int eccache_mutant;
#endif
