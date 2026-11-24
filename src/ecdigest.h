/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The logical digest of a settle (design-cluster-data.md E26, §2.3 step 7; the R8 plan's D6, D7, D25). Replicas merge
 * on their own (E3, C9), so their files differ: what they must agree on is the rows. The digest of a set of parts of
 * one key (plain parts and .ec parts alike: .ec granules come through the read seam, so R10 can digest objects) is
 *  - per series, in series order, the rows of the set visible under its retention cutoff, in ts order: the set's own
 *    tombstones applied and last write wins among its parts, exactly as a query over those parts alone answers them
 *    (eng_query_raw_parts), each row as (series u32, ts i64, the value slot as stored: nulls are their reserved
 *    value), then (series, rows of the series);
 *  - then, when a listed part of the key lies below the set (an older object: the set's tombstones still hide rows
 *    there, and a merge keeps them only then, src/merge.c), a canonical form of the set's tombstones: per series the
 *    union of their ranges clipped to [max(cutoff, key start), key end), never the stored ranges (merges split, clip
 *    and drop them differently);
 * hashed by xxh64 over 64 KiB pieces of that byte stream, each piece seeded with the hash of the ones before (the
 * pieces are cut from the stream, never from a reader's batches, so the value does not depend on the layout), with the
 * total rows and the number of series that have any. Equal for any layout of the same rows: merged or not, any
 * granule cuts, rows of an expired series one twin's merge dropped and the other still stores.
 *
 * The clock and the limits are the log's as of the MK_DIGEST: the leader appends MK_NOW{t} and MK_DIGEST in one batch
 * (jr_markers) and digests at t holding ret_lock (no RETENTION marker between its digest and its marker); a replica's
 * clock at the marker is that t (the NOW markers in log order), and ecdigest_apply freezes the cutoffs of the series
 * of its retired set that clip the key (cutoff above the key's start; usually none) with the marker's expectation.
 *
 * Leader (ecdigest_leader, from src/eccold.c after the swap, the gate released, Y still pinned): digests I ∪ Y and
 * appends the marker; its own settled_ files keep R7's byte digest (src/eckeep.c). A failed append is like any
 * marker's: deposed -> INDETERMINATE and dropped, else the engine failed.
 * Replica: the COLD's apply hands its retired parts (settled_<own part>) to eckeep bound to (key, z)
 * (eckeep_retired); ecdigest_apply binds the marker to them, writes the job file digest_<key>_<z> (checksummed,
 * tmp + fdatasync + rename + directory fsync: a restart finds the expectation again, as the marker is not applied
 * twice) and queues it on eckeep's thread (throttled like the byte digest), which opens the settled_ files as parts,
 * digests them and compares: a match lets eckeep release them at settle_keep_s; a mismatch keeps them and the volume
 * goes DIVERGED (ec_diverge, EC_DIV_DIGEST). The job file goes after its last file is released (unlinked, directory
 * synced). Open: a replica whose COLD was applied but whose leader was deposed before its MK_DIGEST holds those files
 * until an operator acts (logged), never released unchecked.
 *
 * Builds with and without TSDB_EC (the embedded crates copy every .c file of src/).
 */
#ifndef TSDB_ECDIGEST_H
#define TSDB_ECDIGEST_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "marker.h"

struct eng;
struct part;

typedef struct ec_ldigest {
    uint64_t rows, series, xxh64;
} ec_ldigest;

/* a series' frozen cutoff (ecdigest_apply): rows before it are not visible */
typedef struct ecd_cut {
    uint32_t series;
    int64_t cutoff;
} ecd_cut;

/* The logical digest of set[0 .. n) (parts and .ec parts of key `key`, referenced by the caller) at the retention
 * clock now_ms (the limits are the engine's now). */
int ec_logical_digest(struct eng *db, struct part **set, size_t n, int64_t key, int64_t now_ms, ec_ldigest *out);
/* The same with frozen cutoffs: cuts[0 .. ncuts) sorted by series; a series not in it has none. */
int ec_logical_digest_frozen(struct eng *db, struct part **set, size_t n, int64_t key, const ecd_cut *cuts,
                             size_t ncuts, ec_ldigest *out);
/* The cutoffs at now_ms of the series of set[0 .. n) that clip key `key` (above its start), sorted; *cuts malloc'd. */
int ecd_freeze(struct eng *db, struct part **set, size_t n, int64_t key, int64_t now_ms, ecd_cut **cuts,
               size_t *ncuts);

/* A leader after a settle's swap: the digest of set[0 .. n) (I and Y) and MK_NOW + MK_DIGEST {key, z, the oids}. 0,
 * TSDB_ERR_INDETERMINATE (deposed), or another error (the engine failed, or nothing was appended). */
int ecdigest_leader(struct eng *db, int64_t key, uint64_t z, struct part **set, size_t n, const uint8_t *oids,
                    size_t noids);
/* A committed MK_DIGEST at log position pos (m decoded): 0 once its comparison is queued (or done). */
int ecdigest_apply(struct eng *db, uint64_t pos, const mk *m);

/* planted bugs (each must fail tests/test_ecdigest.c, test_keep or test_cluster ecdigest; the Makefile requires it) */
enum {
    ECDIGEST_MUT_NONE = 0,
    ECDIGEST_MUT_BYTES,       /* digestbytes: the hash of the part files' bytes (differs across layouts) */
    ECDIGEST_MUT_NOTOMB,      /* digestnotomb: no tombstone term */
    ECDIGEST_MUT_SKIP,        /* digestskip: a replica's comparison is skipped (every job matches) */
    ECDIGEST_MUT_NORETENTION, /* digestnoretention: no cutoff (expired rows one twin still stores are hashed) */
};
extern int ecdigest_mutant;
int ecdigest_mutant_parse(const char *name); /* -1 for an unknown name */

#endif
