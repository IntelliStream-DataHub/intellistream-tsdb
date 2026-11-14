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
 *   SLOT, COLD                       numbers reserved for the cluster (design-cluster.md 4.1; payloads come with it)
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
    MK_TYPE_LAST = MK_COLD /* the types above this one are unknown to format 9 */
};

#define MK_HDR 8 /* type, flags, span: the start of every marker payload (WAL_MARKER_HDR) */
#define MK_NAME_MAX 255
#define MK_SNAME_MAX 32767 /* a series or state name (series.c MAX_NAME) */
#define MK_FILES_MAX 4096
#define MK_BUCKETS 4096 /* the fence's table (LW_BUCKETS) */
#define MK_NO_PARTITION INT64_MIN

/* COMMIT: a direct commit (insert, replace or delete that goes to parts, not through the WAL rows). */
enum mk_op { MK_OP_INSERT = 1, MK_OP_REPLACE = 2, MK_OP_DELETE = 3 };
/* the keys it covers, in the form of the last-write-wins fence (lw_fence): none, one series and one range, or a
 * table of MK_BUCKETS buckets of series, each with the range of its keys (lo > hi: no keys) */
enum mk_keys { MK_KEYS_NONE = 0, MK_KEYS_RANGE = 1, MK_KEYS_TABLE = 2 };

typedef struct mk_file {
    const char *name; /* final name, not NUL terminated (name_len bytes, no '/' and no NUL) */
    uint32_t name_len;
    uint64_t size, xxh64, header_check; /* the file's size, its xxh64, the part's header checksum */
} mk_file;

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
    } u;
    const uint8_t *body; /* every type: the raw body (an unknown or reserved type has nothing else) */
    size_t body_len;
} mk;

/* Name of a known type ("COMMIT"), NULL for a number the format does not know. */
const char *mk_type_name(uint16_t type);
/* Whether the format knows the number (defined or reserved): an unknown one is skipped by replay all the same, but
 * a binary asked to open a volume with requires= for it would refuse. */
bool mk_type_known(uint16_t type);
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
/* A bucket of a MK_KEYS_TABLE (MK_BUCKETS x 16 bytes, not aligned): read and written through these. */
void mk_table_get(const uint8_t *table, uint32_t bucket, int64_t *lo, int64_t *hi);
void mk_table_put(uint8_t *table, uint32_t bucket, int64_t lo, int64_t hi);

#endif
