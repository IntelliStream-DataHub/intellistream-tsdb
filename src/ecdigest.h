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
 * The clock and the limits are the log's as of the MK_DIGEST: the leader takes its clock t, the limits (ret_gen) and
 * whether a listed part of the key lies below the set under ret_lock, digests at t with the lock let go, and appends
 * the DIGEST carrying t (`now_ms`) and that bit (mk.flags MKD_TOMBS) under ret_lock again only if ret_gen is unchanged
 * (else it digests again; the last try holds the lock throughout), so no RETENTION marker lies between its digest and
 * its marker; the commit wait runs with no lock. No MK_NOW goes with it: the leader's periodic NOW markers could land
 * between and move a replica's clock. A replica freezes its cutoffs at the marker's now_ms for every series of its
 * retired set, those with tombstones only included, and takes the tombstone term from MKD_TOMBS, never from its own
 * listing (a drop or TTL pass between the apply and the job would change it).
 *
 * Leader (ecdigest_leader, from src/eccold.c after the swap, the gate released, Y still pinned): digests I ∪ Y and
 * appends the marker; its own settled_ files keep R7's byte digest (src/eckeep.c). A failed append is like any
 * marker's: deposed -> INDETERMINATE and dropped, else the engine failed.
 * Replica: the COLD's apply binds its retired parts (settled_<own part>) to (key, z) in an unarmed job file
 * digest_<key>_<z> written after its renames (ecdigest_cold_bind; the decide path of a replay binds them again,
 * ecdigest_cold_rebind), so a restart holds them, never ORPHAN. ecdigest_apply arms it (ECDJ_ARMED: the expectation,
 * the frozen cutoffs, TOMBS; checksummed, tmp + fdatasync + rename + directory fsync: the marker is not applied twice)
 * and queues it on eckeep's thread, which checks every granule checksum of the copies (rot, D25), opens them as parts,
 * digests and compares: a match is written to the job (ECDJ_MATCHED) before eckeep releases the copies at
 * settle_keep_s (a restart releases the rest without a second digest); a mismatch, or a digest that fails on copies
 * whose checksums hold, keeps them and the volume goes DIVERGED (ec_diverge, EC_DIV_DIGEST); rot or a copy missing with
 * no match recorded is ROT: counted lost (eckeep_copies_lost), the copies released at their deadline, the .ec parts
 * served. A DIGEST of a later Z of the key supersedes every unarmed job of the key below it (ECDJ_SUPERSEDED: its
 * leader was deposed before its DIGEST, which can never come): ORPHAN, released at the deadline unchecked and counted
 * (eckeep_copies_undigested). The job file goes after its last copy is released (unlinked, directory synced).
 *
 * Builds with and without TSDB_EC (the embedded crates copy every .c file of src/).
 */
#ifndef TSDB_ECDIGEST_H
#define TSDB_ECDIGEST_H

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "marker.h"

struct eng;
struct part;

typedef struct ec_ldigest {
    uint64_t rows, series, xxh64;
    bool tombs; /* the tombstone term was hashed (the leader's decision travels as MKD_TOMBS) */
} ec_ldigest;

/* a series' frozen cutoff (ecdigest_apply): rows before it are not visible */
typedef struct ecd_cut {
    uint32_t series;
    int64_t cutoff;
} ecd_cut;

/* The logical digest of set[0 .. n) (parts and .ec parts of key `key`, referenced by the caller) at the retention
 * clock now_ms (the limits are the engine's now). */
int ec_logical_digest(struct eng *db, struct part **set, size_t n, int64_t key, int64_t now_ms, ec_ldigest *out);
/* The same with frozen cutoffs: cuts[0 .. ncuts) sorted by series (a series not in it has none), and the tombstone term
 * hashed exactly when `tombs` (the leader's MKD_TOMBS: whether a part lies below the set is listing state, which a
 * replica's drop or retention pass changes before its job runs). */
int ec_logical_digest_frozen(struct eng *db, struct part **set, size_t n, int64_t key, const ecd_cut *cuts,
                             size_t ncuts, bool tombs, ec_ldigest *out);
/* The cutoffs at now_ms of the series of set[0 .. n) that clip key `key` (above its start), sorted; *cuts malloc'd.
 * The series of the set's tombstones count too: one whose rows are all elsewhere still has its term clipped. */
int ecd_freeze(struct eng *db, struct part **set, size_t n, int64_t key, int64_t now_ms, ecd_cut **cuts,
               size_t *ncuts);

/* A leader after a settle's swap: the digest of set[0 .. n) (I and Y) and MK_NOW + MK_DIGEST {key, z, the oids}. 0,
 * TSDB_ERR_INDETERMINATE (deposed), or another error (the engine failed, or nothing was appended). */
int ecdigest_leader(struct eng *db, int64_t key, uint64_t z, struct part **set, size_t n, const uint8_t *oids,
                    size_t noids);
/* A committed MK_DIGEST at log position pos (m decoded): 0 once its comparison is queued (or done). */
int ecdigest_apply(struct eng *db, uint64_t pos, const mk *m);

/* A replica's job: the expectation of one MK_DIGEST and the retired files it covers, durable as `file` */
#define ECDJ_ARMED 1u      /* its MK_DIGEST was applied: want, cuts and TOMBS hold */
#define ECDJ_TOMBS 2u      /* the DIGEST's MKD_TOMBS */
#define ECDJ_MATCHED 4u    /* the copies matched: a restart releases the rest without a second digest */
#define ECDJ_SUPERSEDED 8u /* unarmed, and a DIGEST of a later Z of the key was applied: released unchecked */
#define ECD_REFUSED 1      /* ecdigest_job_check: the copies pass their checksums and cannot be digested */
#define ECD_ARMED 2        /* eckeep_job_names: the job is armed already (its DIGEST is applied again) */

typedef struct ecd_job {
    char file[64]; /* digest_<key>_<z> */
    int64_t key;
    uint64_t z;
    uint32_t flags; /* ECDJ_* */
    ec_ldigest want;
    uint8_t oid[16]; /* the marker's first object (ec_diverge names it) */
    ecd_cut *cuts;
    size_t ncuts;
    char (*names)[256]; /* settled_<own part> */
    size_t nnames;
} ecd_job;
void ecd_job_free(ecd_job *j);
bool ecdigest_job_file(const char *name); /* digest_<key>_<z>, or its .tmp */
/* eckeep's thread: the digest of j's files (opened as parts, every granule's checksum checked: a replica's retired
 * copy is read whole once). 0: *got computed; TSDB_ERR_NOTFOUND: *bad files are gone (the job is not matched, so
 * lost); TSDB_ERR_CORRUPT: *bad copies fail their checksums (rot, D25); ECD_REFUSED: they pass them and the digest
 * refuses them (a series SERIES does not know, timestamps out of order: a divergence, not rot); other: try later. */
int ecdigest_job_check(struct eng *db, const ecd_job *j, ec_ldigest *got, size_t *bad);
/* the job file (tmp, fdatasync, rename, directory fsync: fail-stop), replacing an earlier form of it */
int ecdigest_job_write(struct eng *db, const ecd_job *j);
/* A replica's COLD apply after its renames (src/ecapply.c): its retired copies bound to (key, z) in an unarmed job
 * file, so that a restart never binds them to another COLD's DIGEST. A COLD found done at a replay
 * (ecapply_cold_decide) binds the copies of its key that no job names (a crash before the job was durable: earlier
 * COLDs' copies are named by theirs). */
void ecdigest_cold_bind(struct eng *db, int64_t key, uint64_t z, struct part *const *own, size_t nown);
void ecdigest_cold_rebind(struct eng *db, int64_t key, uint64_t z);
/* eckeep's thread at its start: every digest_* file of the directory to eckeep_add_job; a damaged one is logged and
 * kept (its files stay held) */
void ecdigest_load_jobs(struct eng *db);
/* the job file's unlink and the directory fsync after it (the release of its last file was synced before) */
void ecdigest_job_remove(struct eng *db, const char *file);
/* D25: whether settled_ file `name` (a leader's kept copy whose object matched its record) fails its own checksums
 * (footer, index, a granule): rot of the copy, never DIVERGED */
bool ecdigest_copy_rotted(struct eng *db, const char *name);

/* planted bugs (each must fail tests/test_ecdigest.c, test_keep or test_cluster ecdigest; the Makefile requires it) */
enum {
    ECDIGEST_MUT_NONE = 0,
    ECDIGEST_MUT_BYTES,       /* digestbytes: the hash of the part files' bytes (differs across layouts) */
    ECDIGEST_MUT_NOTOMB,      /* digestnotomb: no tombstone term */
    ECDIGEST_MUT_SKIP,        /* digestnocompare: a replica's comparison is skipped (every job matches) */
    ECDIGEST_MUT_NORETENTION, /* digestnoretention: no cutoff (expired rows one twin still stores are hashed) */
    ECDIGEST_MUT_RELEASENOSYNC, /* releasenosync: a release unlinks without the directory fsync (src/eckeep.c) */
    ECDIGEST_MUT_ROTDIVERGES, /* rotdiverges: rot of a kept copy is taken for DIVERGED (src/eckeep.c) */
    ECDIGEST_MUT_BELOWTRUE,   /* digestbelowtrue: the tombstone term is hashed with nothing listed below the set */
    ECDIGEST_MUT_NOTS,        /* digestnots: the rows' timestamps are not hashed */
    ECDIGEST_MUT_FREEZEROWS,  /* digestfreezerows: ecd_freeze takes the series with rows only, not the tombstones' */
    ECDIGEST_MUT_LOCALBELOW,  /* digestlocalbelow: a replica's job decides the tombstone term from its own listing */
    ECDIGEST_MUT_JOBNOSYNC,   /* digestjobnosync: a job file is renamed without its fdatasync */
    ECDIGEST_MUT_NOCRCWALK,   /* digestnocrcwalk: a job does not check its copies' granule checksums */
    ECDIGEST_MUT_ROTMATCHES,  /* digestrotmatches: a job's rot (or a missing copy) counts as a match */
    ECDIGEST_MUT_NOJOBREMOVE, /* digestnojobremove: a job file stays after its last copy was released */
    ECDIGEST_MUT_ANYZ,        /* digestanyz: a DIGEST takes every retired copy of its key, bound to its Z or not */
    ECDIGEST_MUT_NOSUPERSEDE, /* digestnosupersede: copies whose DIGEST can no longer come are held forever */
    ECDIGEST_MUT_CORRUPTROT,  /* digestcorruptrot: a digest that fails on a copy with valid checksums counts as rot */
};
extern int ecdigest_mutant;
/* test hook: the next replica job check hashes the value of its first row changed by one bit, as a retired copy whose
 * rows differ from the leader's with valid checksums would (test_ecapply digest, test_cluster ecdigest) */
extern atomic_bool ecdigest_test_alter;
int ecdigest_mutant_parse(const char *name); /* -1 for an unknown name */

#endif
