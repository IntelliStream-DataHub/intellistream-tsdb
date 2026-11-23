/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Settled (.ec) parts: R7 of docs/design-cluster-data.md (§2.2, §2.3; the plan's D1-D11). On one server the object is
 * the part file unchanged (E7): a settle encodes the bytes of p<K>_<A>_<B>_<L>.part into the chunk store and swaps the
 * part for p<K>_<A>_<B>_<L>.ec, which keeps its source's name and block range (D2), so a settle changes how a part's
 * bytes are stored and nothing else: same rows, same rank, same key.
 *
 * The .ec file (written by ec_index_write as tmp_<pid>_<n>.ec, fsynced, then renamed by the swap's intent v4):
 *
 *   [0, R)          the source part's header, granule index, tombstones and 64-byte footer, verbatim: the bytes the
 *                   source's footer hash covers (src/part.c part_hash), so gx_* and gx_load read the index in place,
 *                   unchanged (AGENTS.md "The granule index"). Granule offsets (GF_OFF) stay offsets into the
 *                   object (the source file), whose granule bytes are not here: they are read from the chunk store
 *                   (ec_granule_read, src/eccache.h). R = hdr_len + index_len + tombstone bytes + 64; the source's
 *                   header and footer place them exactly as in the part (footer.index_off is the object's offset of
 *                   the index; the .ec file holds the index at hdr_len).
 *   [R, R + n)      the object record (ec_record, encoded below; n = ec_record_size).
 *   [R + n, +64)    the trailer (EC_TRAILER_BYTES): it names the record's place and hashes it, and hashes itself.
 *
 * The record's encoding, little-endian, every field at a fixed offset (EC_REC_*), version EC_RECORD_VERSION:
 *
 *     0  u32  version             EC_RECORD_VERSION; a decoder refuses another
 *     4  u32  class_check         EC_CLS_*: the class check of §2.3 step 2, run on the rows at settle (an .ec part's
 *                                 granules are never decoded by cls_check_parts: the open trusts this)
 *     8  u8[16] oid               random, from the settle (the object's name in the chunk store)
 *    24  i64  key                 the partition key
 *    32  u64  min_block           the source's block range (equal to the name's)
 *    40  u64  max_block
 *    48  u32  level
 *    52  u16  k                   k' of the object's geometry (chunk_geom), 1 .. CHUNK_K_MAX
 *    54  u16  m                   1 .. CHUNK_M_MAX
 *    56  u32  cell_bytes
 *    60  u32  positions           R9: a mask of the positions written (E10's degraded settle); 0 until then: all of them
 *    64  u64  object_len          the source file's length
 *    72  u64  object_xxh64        xxh64 (seed 0) of the whole object
 *    80  u64  writer_epoch        the chunk epoch the chunks were written under (D4: 1 in R7)
 *    88  i64  settle_ms           when the swap was decided, eng_now_ms (r7-keep counts settle_keep_s from it, the
 *                                 policy settle_coarse_s: never from an mtime, which a rename does not change)
 *    96  u64  ppos                R8: the settle's cut Z (the log end its rows are taken at, below its MK_COLD
 *                                 marker); 0 but in a cluster ec volume
 *   104  u64  stripe_version      R8: the stripe list's version; 0 but in a cluster ec volume
 *   112  u8[96] source            the source part's name, NUL padded (p<K>_<A>_<B>_<L>.part)
 *   208  u64  nsums               chunk_sums_total(geometry): one encode-time xxh64 per cell of every position (E26)
 *   216  u64  sums[nsums]
 *
 * The trailer, the file's last 64 bytes:
 *
 *     0  u8[8] magic              EC_TRAILER_MAGIC
 *     8  u32  trailer version     EC_TRAILER_VERSION
 *    12  u32  record version      the record's own version, readable without decoding it
 *    16  u64  record_off          R above
 *    24  u64  record_len          n above
 *    32  u64  record_xxh64        xxh64 (seed 0) of the record's bytes
 *    40  u8[16] reserved          zero
 *    56  u64  trailer_xxh64       xxh64 (seed 0) of the trailer's bytes [0, 56)
 *
 * Owners: r7-iface wrote this layout, the names (src/part.c) and the stubs here; r7-ecfile writes the codec, the writer
 * and part_open_ec in src/ecpart.c. eng_test_mark_ec is r7-iface's test hook (r7-skip, r7-oracle) and stays.
 */
#ifndef TSDB_ECPART_H
#define TSDB_ECPART_H

#include "internal.h"

#define EC_RECORD_VERSION 1u
#define EC_TRAILER_VERSION 1u
#define EC_TRAILER_MAGIC "TSDB-EC\n"
#define EC_TRAILER_BYTES 64

enum {
    EC_REC_VERSION = 0, EC_REC_CLASS = 4, EC_REC_OID = 8, EC_REC_KEY = 24, EC_REC_MIN_BLOCK = 32,
    EC_REC_MAX_BLOCK = 40, EC_REC_LEVEL = 48, EC_REC_K = 52, EC_REC_M = 54, EC_REC_CELL = 56, EC_REC_POSITIONS = 60,
    EC_REC_OBJ_LEN = 64, EC_REC_OBJ_HASH = 72, EC_REC_EPOCH = 80, EC_REC_SETTLE_MS = 88, EC_REC_PPOS = 96,
    EC_REC_STRIPE_VERSION = 104, EC_REC_SOURCE = 112, EC_REC_NSUMS = 208, EC_REC_SUMS = 216,
    EC_REC_FIXED = EC_REC_SUMS, /* the bytes before the sums */
};
enum {
    EC_TR_MAGIC = 0, EC_TR_VERSION = 8, EC_TR_REC_VERSION = 12, EC_TR_REC_OFF = 16, EC_TR_REC_LEN = 24,
    EC_TR_REC_HASH = 32, EC_TR_RESERVED = 40, EC_TR_HASH = 56,
};

/* The class check's result (§2.3 step 2). A record that says EC_CLS_FAILED fails the open (r7-ecfile). */
enum { EC_CLS_NONE = 0, EC_CLS_PASSED = 1, EC_CLS_FAILED = 2 };

typedef struct ec_record {
    uint32_t version;
    uint32_t class_check;
    uint8_t oid[16];
    int64_t key;
    uint64_t min_block, max_block;
    uint32_t level;
    uint16_t k, m;
    uint32_t cell_bytes;
    uint32_t positions; /* R9 (E10); 0 */
    uint64_t object_len, object_xxh64;
    uint64_t writer_epoch;
    int64_t settle_ms;
    uint64_t ppos, stripe_version; /* R8: only in a cluster ec volume (record_valid); else 0 */
    char source[96];
    uint64_t nsums;
    uint64_t *sums; /* nsums entries, owned (ec_record_free) */
} ec_record;

/* The in-memory state of an .ec part (part.ec). r7-ecfile fills rec at the open; r7-seam reaches the handle's cache
 * and open objects through tier (the engine's, referenced as long as the part is: one per handle). test_mark: a part
 * that eng_test_mark_ec marked: its bytes are still in its map and every R7 stub reads it as a .part. */
struct ecpart {
    ec_record rec;
    struct tier *tier;
    uint32_t volume;
    bool test_mark;
    /* r7-ecfile: where the open found the record in the .ec file and its hash (ec_index_intact checks it again) */
    uint64_t rec_off, rec_len, rec_hash;
};

/* ------------------------------------------------------------ r7-ecfile: prototypes (src/ecpart.c) */
size_t ec_record_size(const ec_record *r); /* EC_REC_FIXED + 8 * nsums */
/* Writes r into buf[0 .. ec_record_size); TSDB_ERR_INVALID when cap is short or a field is out of range. */
int ec_record_encode(const ec_record *r, uint8_t *buf, size_t cap);
/* Safe on any bytes; refuses another version (TSDB_ERR_CORRUPT) and allocates r->sums. */
int ec_record_decode(const uint8_t *buf, size_t len, ec_record *r);
/* The same for a volume that may say more: `cluster_ec` (an erasure-coded cluster volume's engine, eng.ec_cluster)
 * admits ppos and stripe_version != 0 (R8); without it, as the two above, both must be 0. positions != 0 is refused
 * either way (E10 is R9's). */
int ec_record_encode_as(const ec_record *r, uint8_t *buf, size_t cap, bool cluster_ec);
int ec_record_decode_as(const uint8_t *buf, size_t len, ec_record *r, bool cluster_ec);
void ec_record_free(ec_record *r);
/* src (a listed .part) + r -> tmp_<pid>_<n>.ec in the engine's directory, fsynced; its name into tmp. */
int ec_index_write(eng *db, const part *src, const ec_record *r, char tmp[96]);
/* Opens an .ec file as a part (p->ec set, p->verify true: E11): the source's header and footer hash, the trailer,
 * the record's hash, and the record against the footer and the name (rows, time range, block range). */
int part_open_ec(eng *db, const char *file, const char *name, part **out);

/* r7-ecfile's planted bugs (tests/test_ecpart.c --mutant NAME; the Makefile requires each to fail it): the open checks
 * no hash (neither the source's nor the record's); the open leaves wal_seq 0 (a WAL segment replays into a partition an
 * .ec part holds); load_parts leaves .ec files alone (ecpart_load lists nothing); the open leaves fno_lo, fno_hi 0
 * (fno_next does not count the .ec part's numbers); the open does not check where the index places each granule
 * (overlapping granules, or one past the object's data, are opened) */
enum { EC_MUT_NONE = 0, EC_MUT_NOHASH, EC_MUT_NOWALSEQ, EC_MUT_SKIPNAME, EC_MUT_NOFNO, EC_MUT_NOLAYOUT };
extern int ecpart_mutant;
/* Test hook: ecpart_load takes every volume as one whose META says requires=tier (r7-tier writes the word, and refuses
 * it without a configured tier; tests of the .ec file itself run without one). */
extern atomic_bool ec_test_assume_tier;

/* ------------------------------------------------------------ the engine's calls into src/ecpart.c */
/* load_parts met `name` (ec_file_name): *out the .ec part to list. A volume whose META lacks requires=tier (written
 * before the first .ec name; an .ec name without it is damage), or both an .ec part and its source with no intent
 * naming them, fail the open with a line (TSDB_ERR_CORRUPT): load_parts never leaves a part out quietly. */
int ecpart_load(eng *db, const char *name, bool read_only, part **out);
/* A follower's MANIFEST names an .ec part: the follower stops with one line (tier_follower_ec, D7). */
int ecpart_follower_open(eng *db, const char *name, part **out);
/* part_index_intact of an .ec part: its record's hash, and the source's header, index, tombstones and footer against
 * the hash the open checked (a test-marked part: part_map_intact, its map is a whole part). */
bool ec_index_intact(const part *p);
/* cls_check_parts for an .ec part: its granules are never decoded there, so the record's class check stands for them
 * (the index checks still run): TSDB_ERR_CORRUPT with a line when it says FAILED, or, with partition classes on, did
 * not run (a settle writes EC_CLS_PASSED); 0 otherwise. */
int ec_cls_record(eng *db, const part *p);
void ecpart_free(struct ecpart *e);

/* ------------------------------------------------------------ test hook (r7-iface's; r7-skip, r7-oracle) */
enum { EC_MARK_EC = 1, EC_MARK_SETTLING = 2 };
/* Flags a listed part of db in memory, nothing on disk: EC_MARK_EC gives it a struct ecpart with test_mark (once;
 * never taken back), EC_MARK_SETTLING sets p->settling (under db->lock; without the flag it is cleared).
 * TSDB_ERR_NOTFOUND when p is not listed, TSDB_ERR_LOCKED when a merge has it (`merging`: the merge would meet an .ec
 * source and fail), TSDB_ERR_NOMEM. The mark is the one store into p->ec while the part is listed: p->ec is atomic, so
 * the readers that run beside it (the statistics' sweeper and sampler, queries, merges) see NULL or the whole ecpart.
 * Merges still select a marked part until r7-skip's barriers: tests that mark run with merge_threads = 0. */
int eng_test_mark_ec(eng *db, part *p, unsigned flags);

/* ------------------------------------------------------------ call counters (tests/test_ecnames.c) */
/* Every stub above and in src/{eccache,ecgc,eckeep,ecswap,settle,tier}.c bumps its counter on entry, so the test sees
 * that each dispatch in a shared file reaches it (a stub does what the old code did: without the counters nothing
 * could tell a missing branch). Whoever fills a stub keeps its EC_CALLED line. Relaxed increments, on R7's paths only
 * (a .part never reaches one). */
enum {
    EC_CALL_GRANULE_READ,   /* ec_granule_read (part.c granule_read) */
    EC_CALL_INDEX_INTACT,   /* ec_index_intact (part.c part_index_intact) */
    EC_CALL_CLS_RECORD,     /* ec_cls_record (classes.c cls_check_parts) */
    EC_CALL_LOAD,           /* ecpart_load (db.c load_parts) */
    EC_CALL_FOLLOWER_OPEN,  /* ecpart_follower_open (manifest.c open_listed_part) */
    EC_CALL_PART_BYTES,     /* eccache_part_bytes (stats.c part_walk, db.c stats_walk and eng_foreach_part) */
    EC_CALL_RESIDENT,       /* eccache_resident (stats.c sscx_run, stats_hot.c sample_part) */
    EC_CALL_SWAP_RECOVER,   /* ecswap_recover (part.c intents_recover) */
    EC_CALL_GC_LOAD,        /* ecgc_load_retired (load_parts) */
    EC_CALL_GC_RETIRE,      /* ecgc_retire_locked (manifest.c db_retire_locked) */
    EC_CALL_GC_PURGE,       /* ecgc_purge_entry (manifest.c graveyard_purge) */
    EC_CALL_GC_LAST_UNREF,  /* ecgc_last_unref (part.c part_unref) */
    EC_CALL_GC_SWEEP,       /* ecgc_open_sweep (db.c eng_open_sched) */
    EC_CALL_KEEP_LOAD,      /* eckeep_load_settled (load_parts) */
    EC_CALL_KEEP_PURGE,     /* eckeep_purge_entry (graveyard_purge) */
    EC_CALL_ENG_OPEN,       /* each of the four per-engine opens (tier_engine_attach): 4 per engine with a tier */
    EC_CALL_ENG_CLOSE,      /* each of the four per-engine closes (tier_engine_detach) */
    EC_CALL_TIER_OPEN,      /* each of the three per-tier opens (tier_open) */
    EC_CALL_TIER_CLOSE,     /* each of the three per-tier closes (the last tier_put) */
    EC_CALL_TIER_QUIESCE,   /* ecsettle_tier_quiesce (tier_quiesce, tsdb_close) */
    EC_CALL_N
};
extern atomic_uint_fast64_t ec_calls[EC_CALL_N];
#define EC_CALLED(k) atomic_fetch_add_explicit(&ec_calls[(k)], 1, memory_order_relaxed)

#endif
