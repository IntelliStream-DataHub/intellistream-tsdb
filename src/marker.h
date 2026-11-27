/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * WAL marker payloads (format 9, docs/design-cdc.md 2.1, design-ha-modes.md 2.1, design-cluster.md 4.1).
 *
 * A marker record (WAL kind 1) holds a payload of `u16 type, u16 flags, u32 span` and a body. The ledger of shared
 * mode, the change feed and the cluster's replication log all name the same events, so their payloads are defined
 * here once and have one encoder and one decoder (M15). Nothing writes a marker but tests yet: the backends
 * (feed, ledger, cluster) come later and change no format.
 *
 * Every integer is little endian, the body is a multiple of 4 bytes and canonical: padding and reserved fields are
 * zero, so decode followed by encode gives the same bytes (the fuzz harness checks that, and the decoder refuses
 * anything else). A decoder never reads past the length it is given and never allocates: strings and the file list
 * point into the payload it decoded.
 *
 * The types the format knows by number (a replay or a follower skips a marker whose type it does not apply, and keeps
 * the numbering: `span` numbers belong to the record either way):
 *   COMMIT, DROP, RETENTION, EPOCH   payloads defined below
 *   CLASS, SERIES, STATE, NOW        payloads defined below (the replica engine applies them, src/replica.c)
 *   FEED_SLOT                        number reserved for the change feed (its payload comes with the feed)
 *   SLOT                             number reserved for the cluster (design-cluster.md 4.1; its payload comes with it)
 *   COLD, GC, DIGEST                 payloads defined below (R8, docs/design-cluster-data.md 2.3, E14, E26): a settle's
 *                                    cut, a chunk deletion, a settle's logical digest. GC and DIGEST (12, 13) belong to a
 *                                    volume whose META says requires=ec, COLD to one with ec or tier (E17 and its
 *                                    amendment for E55): an open of any other refuses them, and so does the writer
 *                                    (mk_type_admitted), where every older binary refuses the word itself
 *   14, 15                           MK_DROP_OBJ and MK_UNSETTLE of the design (docs/design-cluster-data.md, the
 *                                    format table; R10 gives them bodies), behind the same word: this binary cannot
 *                                    apply them, so a volume that holds one is refused, never replayed past them
 * A type this binary does not know at all is MK_UNKNOWN: the record is intact and skipped.
 */
#ifndef TSDB_MARKER_H
#define TSDB_MARKER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

enum mk_type {
    MK_UNKNOWN = 0,
    MK_COMMIT = 1,
    MK_DROP = 2,
    MK_RETENTION = 3,
    MK_EPOCH = 4,
    MK_FEED_SLOT = 5, /* reserved */
    MK_CLASS = 6,     /* reserved: the cluster's kinds */
    MK_SERIES = 7,
    MK_STATE = 8,
    MK_SLOT = 9,
    MK_NOW = 10,
    MK_COLD = 11,
    MK_GC = 12,
    MK_DIGEST = 13,
    MK_TYPE_LAST = MK_DIGEST /* the types above this one are unknown to this binary */
};
/* Numbers the design gives R10's ec markers (MK_DROP_OBJ, MK_UNSETTLE): unknown here, and admitted nowhere. */
#define MK_EC_LATER_LO 14
#define MK_EC_LATER_HI 15

#define MK_HDR 8 /* type, flags, span: the start of every marker payload (WAL_MARKER_HDR) */
#define MK_NAME_MAX 255
#define MK_SNAME_MAX 32767 /* a series or state name (series.c MAX_NAME) */
#define MK_FILES_MAX 4096
#define MK_BUCKETS 4096 /* the fence's table (LW_BUCKETS) */
#define MK_NO_PARTITION INT64_MIN
/* R8's bodies (COLD, GC, DIGEST): at most MK_FILES_MAX objects, files and oids each, and an object record of at most
 * MK_REC_MAX bytes (an ec_record of thousands of cell sums; src/ecpart.h). The whole payload stays below the WAL's
 * 2^27 bytes (mk_encode) and, on a leader, one peer frame (the leader's own check). */
#define MK_REC_MAX (1u << 20)
#define MK_OID 16 /* an object id (ec_record.oid) */

/* COMMIT: a direct commit (insert, replace or delete that goes to parts, not through the WAL rows). */
enum mk_op { MK_OP_INSERT = 1, MK_OP_REPLACE = 2, MK_OP_DELETE = 3 };
/* the keys it covers, in the form of the last-write-wins fence (lw_fence): none, one series and one range, or a
 * table of MK_BUCKETS buckets of series, each with the range of its keys (lo > hi: no keys) */
enum mk_keys { MK_KEYS_NONE = 0, MK_KEYS_RANGE = 1, MK_KEYS_TABLE = 2 };

typedef struct mk_file {
    const char *name; /* final name, not NUL terminated (name_len bytes, no '/' and no NUL) */
    uint32_t name_len;
    uint64_t size, xxh64, header_check; /* the file's size, its xxh64, the part's header checksum */
    uint8_t kind; /* COLD's files: enum mk_file_kind; 0 in a COMMIT (decode refuses anything else there) */
} mk_file;

/* what a file of a COLD marker is: an .ec index file of an object, or a part of Y (design-cluster-data.md 2.3 step 4:
 * the late parts of the key, all with ppos <= Z) */
enum mk_file_kind { MK_FILE_EC = 1, MK_FILE_Y = 2 };

typedef struct mk_commit {
    uint8_t op;        /* enum mk_op */
    uint8_t keys_form; /* enum mk_keys */
    uint32_t nfiles;   /* at most MK_FILES_MAX */
    uint64_t flush_through; /* F: segment the commit flushes through first */
    uint64_t rows, events, reserve;
    uint32_t series;  /* MK_KEYS_RANGE: the one series (a volume's local id) */
    int64_t lo, hi;   /* MK_KEYS_RANGE */
    const uint8_t *table; /* MK_KEYS_TABLE: MK_BUCKETS x (lo, hi), 16 bytes each (mk_table_get, mk_table_put) */
    /* the file list, in its encoded form (iterate with mk_file_next) */
    const uint8_t *files;
    size_t files_len;
} mk_commit;

typedef struct mk_drop {
    uint64_t wm_gen;
    int64_t dropped_before;
    uint64_t block;      /* the block number of the drop's part (F17) */
    int64_t partition;   /* MK_NO_PARTITION: everything before dropped_before; else the one partition's key */
} mk_drop;

typedef struct mk_retention {
    uint32_t series;     /* local id */
    int64_t limit_ms;
    uint64_t record_off; /* offset of its record in RETENTION */
} mk_retention;

typedef struct mk_epoch {
    uint64_t epoch;
    const char *owner; /* not NUL terminated */
    uint32_t owner_len;
} mk_epoch;

/* CLASS: a series switches to a partition class at `at` (a leader's decision; a replica applies it at the first
 * instant that is safe on its own state, never earlier than `at`). record_off is where the record goes in CLASSES. */
typedef struct mk_class {
    uint32_t series;     /* local id */
    uint32_t cls;        /* 0 .. 3 */
    int64_t at;          /* INT64_MIN: from the beginning of time (a fresh series' decision) */
    uint64_t record_off;
} mk_class;

/* SERIES and STATE: the next entry of the dictionary. `id` is the dictionary index (a state's code is id + 1). */
typedef struct mk_series {
    uint32_t id;
    uint32_t vtype; /* TSDB_FLOAT32 .. TSDB_STATE */
    const char *name; /* not NUL terminated; 1 .. MK_SNAME_MAX bytes, no NUL */
    uint32_t name_len;
} mk_series;

typedef struct mk_state {
    uint32_t id;
    const char *name;
    uint32_t name_len;
} mk_state;

/* NOW: the leader's retention clock, in ms */
typedef struct mk_now {
    int64_t now_ms;
} mk_now;

/* COLD (R8, design-cluster-data.md 2.3 steps 4-6): the settle of partition key `key` cut at log position Z. The
 * leader's memtable held no row of the key above `flush_through` (F, as a COMMIT's: the leader's wal_flushed after
 * its rotate at the cut; a replica flushes through F only). The object records are opaque here (ec_record_encode's
 * bytes, decoded and checked by their consumer), each with its oid; the files are the .ec and Y files to list; the
 * retired oids are objects the settle replaces (R10's re-encode; written empty in R8, and a decode refuses a list). Iterate with mk_cold_obj_next,
 * mk_cold_file_next and mk_oid_at (the retired list). */
typedef struct mk_cold {
    uint64_t z, flush_through, stripe_version;
    int64_t key;
    uint32_t nobjs, nfiles, nretired; /* each at most MK_FILES_MAX */
    const uint8_t *objs;              /* encoded (mk_cold_obj_put), objs_len bytes */
    size_t objs_len;
    const uint8_t *files;             /* encoded (mk_file_put, kind set), files_len bytes */
    size_t files_len;
    const uint8_t *retired;           /* nretired x MK_OID bytes */
} mk_cold;

/* one object of a COLD marker, as mk_cold_obj_next gives it: rec points into the payload */
typedef struct mk_cold_obj {
    uint8_t oid[MK_OID];
    const uint8_t *rec;
    uint32_t rec_len; /* 1 .. MK_REC_MAX */
} mk_cold_obj;

/* GC (R8, E14, D13; R9 D12): chunks of these objects go (retired objects, orphans), their deletion is done everywhere
 * and the pending-delete list forgets them, or a released stripe position on a former holder goes (MK_GC_RELEASE /
 * MK_GC_RELEASE_DONE: {rel_vol, rel_pos, rel_node, rel_ver}). */
#define MK_REL_POS_MAX 32u /* matches CHUNK_POS_MAX / META_STRIPE_MAX */
enum mk_gc_reason {
    MK_GC_RETIRED = 1,
    MK_GC_ORPHAN = 2,
    MK_GC_DONE = 3,
    MK_GC_RELEASE = 4,
    MK_GC_RELEASE_DONE = 5,
};
typedef struct mk_gc {
    uint32_t reason;     /* enum mk_gc_reason */
    uint32_t noids;      /* RETIRED/ORPHAN/DONE: 1..MK_FILES_MAX; RELEASE/RELEASE_DONE: 0 */
    const uint8_t *oids; /* noids x MK_OID bytes */
    uint32_t rel_vol;    /* MK_GC_RELEASE / MK_GC_RELEASE_DONE: volume number */
    uint32_t rel_pos;    /* MK_GC_RELEASE / MK_GC_RELEASE_DONE: stripe position (< MK_REL_POS_MAX) */
    uint32_t rel_node;   /* MK_GC_RELEASE / MK_GC_RELEASE_DONE: released member id (> 0) */
    uint64_t rel_ver;    /* MK_GC_RELEASE / MK_GC_RELEASE_DONE: stripe_version since which node left pos (> 0) */
} mk_gc;

/* DIGEST (R8, E26, D7): the leader's logical digest of key `key` as of Z, over the objects named: the visible rows
 * (`rows`), the series that have any (`series`), and an xxh64 over them in (series, ts) order */
typedef struct mk_digest {
    int64_t key;
    uint64_t z, rows, series, xxh64;
    int64_t now_ms; /* the clock of the digest's retention cutoffs (a replica freezes its own at it) */
    uint32_t noids; /* at most MK_FILES_MAX */
    const uint8_t *oids;
} mk_digest;

/* mk.flags of a MK_DIGEST: the leader hashed its set's tombstone term (a listed part of the key lay below the set) */
#define MKD_TOMBS 1u

typedef struct mk {
    uint16_t type;  /* enum mk_type, or the raw number of an unknown one (then mk_type_name gives NULL) */
    uint16_t flags;
    uint32_t span;  /* the numbers the record takes (at least 1) */
    union {
        mk_commit commit;
        mk_drop drop;
        mk_retention retention;
        mk_epoch epoch;
        mk_class cls;
        mk_series series;
        mk_state state;
        mk_now now;
        mk_cold cold;
        mk_gc gc;
        mk_digest digest;
    } u;
    const uint8_t *body; /* every type: the raw body (an unknown or reserved type has nothing else) */
    size_t body_len;
} mk;

/* Name of a known type ("COMMIT"), NULL for a number the format does not know. */
const char *mk_type_name(uint16_t type);
/* Whether the format knows the number (defined or reserved): an unknown one is skipped by replay all the same, but
 * a binary asked to open a volume with requires= for it would refuse. */
bool mk_type_known(uint16_t type);
/* Whether a volume admits a marker of this type (E17), at its open's replay and at its writer (a marker the volume's
 * next open would refuse is never logged): GC and DIGEST only where META says requires=ec (`ec`), COLD where it says
 * ec or tier (`tier`), 14 and 15 nowhere (R10's, MK_EC_LATER_*); every other type, unknown ones included, as before
 * (applied or skipped). */
bool mk_type_admitted(uint16_t type, bool ec, bool tier);
/* The span a marker of this type and content must have: events for COMMIT (at least 1), else 1. */
uint32_t mk_span(const mk *m);

/* Encodes a marker payload (the bytes after a record header). Gives a malloc'd buffer, or TSDB_ERR_INVALID for
 * content that decode would refuse (a name that is too long, a span that does not match, a table of the wrong
 * size). Types without a payload definition (reserved ones) take m->body as it is. `files` of a COMMIT is the
 * already encoded list (mk_file_put), table as MK_BUCKETS 16-byte pairs. */
int mk_encode(const mk *m, uint8_t **out, size_t *len);
/* Decodes and validates a payload of n bytes (n is the record's len): TSDB_ERR_CORRUPT for any deviation (length,
 * padding, a field out of range, a span that does not match the type). Never reads past p + n. */
int mk_decode(const uint8_t *p, size_t n, mk *m);

/* The encoded file list: appends one entry to buf (of room for mk_file_size bytes) and gives its length. */
size_t mk_file_size(const mk_file *f);
size_t mk_file_put(uint8_t *buf, const mk_file *f);
/* Reads the next entry of a validated list at *pos (decode has checked them all). Returns false at the end. */
bool mk_file_next(const mk_commit *c, size_t *pos, mk_file *f);
/* COLD's lists. An object: mk_cold_obj_size bytes, mk_cold_obj_put writes it (gives its length). The files are
 * mk_file entries with their kind set. mk_cold_obj_next and mk_cold_file_next read a validated list at *pos (false at
 * the end); mk_oid_at gives the i-th oid of a list (COLD's retired, GC's, DIGEST's). */
size_t mk_cold_obj_size(uint32_t rec_len);
size_t mk_cold_obj_put(uint8_t *buf, const uint8_t oid[MK_OID], const uint8_t *rec, uint32_t rec_len);
bool mk_cold_obj_next(const mk_cold *c, size_t *pos, mk_cold_obj *o);
bool mk_cold_file_next(const mk_cold *c, size_t *pos, mk_file *f);
const uint8_t *mk_oid_at(const uint8_t *oids, uint32_t i);
/* Planted bugs of the R8 bodies (tests/test_marker.c --mutant NAME; the Makefile requires each to fail it). */
enum { MK_MUT_NONE = 0, MK_MUT_COLD_BOUNDS /* mkcoldbounds: R8's counts unbounded */,
       MK_MUT_ADMIT_ALL /* mkadmitall: an open without requires=ec admits GC and DIGEST */,
       MK_MUT_PPOS_LOOSE /* ecpposloose: record_valid takes ppos and stripe_version on any volume */,
       MK_MUT_WRITE_ANY /* mkwriteany: the WAL writer logs a marker its volume does not admit */,
       MK_MUT_RETIRED /* mkretired: COLD's decoder takes retired oids, which R8 cannot honour */ };
extern int mk_test_mutant;
/* A bucket of a MK_KEYS_TABLE (MK_BUCKETS x 16 bytes, not aligned): read and written through these. */
void mk_table_get(const uint8_t *table, uint32_t bucket, int64_t *lo, int64_t *hi);
void mk_table_put(uint8_t *table, uint32_t bucket, int64_t lo, int64_t hi);

#endif
