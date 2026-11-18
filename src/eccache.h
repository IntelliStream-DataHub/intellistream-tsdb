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
 * read it once: the second waits for the first (on the cache's cv) and then hits.
 *
 * No read here runs under db->lock, latest_rw, buf_lock or publish_lock: every caller of granule_read reads a
 * snapshot's granules with no engine lock held (the audit is in tests/test_seam.c's header).
 *
 * Residency (statistics): the cached bytes of the part's object in a range (eccache_resident), never its map.
 */
#ifndef TSDB_ECCACHE_H
#define TSDB_ECCACHE_H

#include "internal.h"

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

/* The backend the cache reads from. r7-tier hands its per-handle backend over once it is open (owned by the tier, not
 * closed here). Until that is merged, and whenever none was handed over, the cache opens one itself at its first use
 * from the tier's configuration (chunk_be_open2 over dirs, min_up = stores - m) and closes it with the tier; a failed
 * open is tried again at most once a second (its error is the reader's). */
void eccache_set_backend(struct tier *t, struct chunk_backend *be);
/* The volume's uuid of chunk identities (META vuuid=, r7-tier); all zero until set. */
void eccache_set_vuuid(struct tier *t, uint32_t volume, const uint8_t vuuid[16]);
/* The backend (opened on first use as above; NULL with *rc set when it cannot be) and the volume's uuid. */
struct chunk_backend *eccache_backend(struct tier *t, int *rc);
void eccache_vuuid(struct tier *t, uint32_t volume, uint8_t out[16]);
/* r7-gc: the object's chunks are gone; its granules leave the cache and it is closed once no reader holds it. */
void eccache_forget(struct tier *t, uint32_t volume, const uint8_t oid[16]);

/* ------------------------------------------------------------ tests */
typedef struct eccache_counters {
    uint64_t hits, misses, waits;   /* granule lookups; waits: a miss another reader was already filling */
    uint64_t bytes, entries;        /* what the cache holds now */
    uint64_t evictions, uncached;   /* entries evicted; granules used once and dropped (larger than the budget) */
    uint64_t retries, corrupt;      /* second reads without RAW; granules refused as CORRUPT */
    uint64_t obj_opens, obj_closes; /* chunk_obj_open / chunk_obj_close */
    uint64_t objs_open, fds_open, fds_peak; /* now, and the highest count of fds the open objects held */
    uint64_t budget, fd_budget;
} eccache_counters;
void eccache_get_counters(struct tier *t, eccache_counters *out);
/* Budgets for tests (bytes; fds). The fd budget counts the chunk fds of the open objects (k' + m each at most). */
void eccache_test_budget(struct tier *t, uint64_t bytes);
void eccache_test_fd_budget(struct tier *t, uint64_t fds);
/* Empties the cache of what no reader holds and closes every object no reader holds (a cold cache). */
void eccache_test_drop(struct tier *t);

/* r7-seam's test hook (r7-keep, r7-gc, r7-oracle use it): encodes the bytes of p, a listed .part of db, into the
 * tier's chunk store as a new object (oid random, the tier's k and m, epoch 1) and attaches it: p->ec set (the one
 * store into it, under db->lock, as eng_test_mark_ec), so from then on every granule of p is read through the seam.
 * In memory only: a reopen lists the .part again. rec (may be NULL) gets a copy of the record (its sums malloc'ed).
 * TSDB_ERR_NOTFOUND (not listed), TSDB_ERR_LOCKED (merging), TSDB_ERR_INVALID (no tier, already .ec, or a part with
 * no granules), the backend's errors. Tests that attach run with merge_threads = 0 until r7-skip. */
int ec_test_attach(eng *db, part *p, struct ec_record *rec);
/* The same for an object already in the store (rec from an earlier ec_test_attach, before a reopen): nothing is
 * written. */
int ec_test_attach_object(eng *db, part *p, const struct ec_record *rec);

/* Planted bugs (tests/test_seam.c --mutant NAME; the Makefile requires each to fail it). */
enum {
    ECC_MUT_NONE = 0,
    ECC_MUT_NOVERIFY, /* seamnoverify: a miss's bytes are not checked against the granule's crc */
    ECC_MUT_NORETRY,  /* seamnoretry: a mismatch is CORRUPT at once, never read again without RAW */
    ECC_MUT_KEYOFF,   /* cachekeyoff: entries keyed by the offset alone, so two objects collide */
};
extern int eccache_mutant;
#endif
