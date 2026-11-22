/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * DIVERGED made durable (design-cluster-data.md E26, the R8 plan's D8, D23). A volume whose settled data does not match
 * what it replaced (a settle's digest, src/eckeep.c; a part above Z at a cold apply, r8h) must stay refused across a
 * restart: before this file the flag was memory only, and a restarted engine served the .ec until its digest failed
 * again, which waits while any store is not UP.
 *
 * The file DIVERGED in the volume's directory: append-only records of DIV_REC (48) bytes, little-endian:
 *   0 u32 check   the low 32 bits of xxh64 over bytes 4..47, seeded with the record's offset (a record copied to
 *                 another offset is damage, as in RETENTION)
 *   4 u32 why     EC_DIV_*
 *   8 i64 key     the partition key (0: none)
 *  16 u8[16] oid  the object (zeros with flags bit 0 clear: none)
 *  32 i64 ms      wall clock of the write
 *  40 u32 flags   bit 0: oid present
 *  44 u32 zero
 * ec_diverge appends a record (once per (key, oid, why): a restart's digest that fails again adds nothing), fdatasyncs
 * it and, when it is the file's first record, fsyncs the directory (eng_sync_dir), and only then sets db->diverged: what
 * acts on the flag (repld's FAIL, the ISR shrink, a failover) must find it again after a restart. A failed write or
 * sync is fail-stop (eng_fail); the flag is set all the same. The open (ecdiverge_open, before any thread of the
 * engine starts, writers and readers alike) reads every record and sets the flag when there is one (a writer's WAL
 * open then fsyncs the directory, before any thread acts: the record's writer may have died before its own); a
 * bad record with
 * a valid one after it, or a read error, fails the open (TSDB_ERR_CORRUPT / TSDB_ERR_IO); a torn tail (a write the
 * crash cut, never acknowledged: the flag was not set) is confirmed by a second read (tail_confirm) and cut by the
 * next append. Lock order: the file's mutex > dir_sync_lock > fail_lock; it is taken under no engine lock.
 *
 * Clearing is an operator step only (`tsdb DIR volume diverged --clear`, ecdiverge_cli): refused while a writer holds
 * the directory's LOCK, and on a cluster volume (META requires=cluster) until R4's rebuild of a replica exists (D23:
 * a diverged replica back in the ISR could be promoted with its diverged rows); a META that cannot be read refuses.
 * The LOCK is taken before the records are read, so every record removed was logged. tsdbd reports the flag through
 * tsdb_volume_diverged (src/cvol.h). tests/test_diverge.c tests the file (power loss, EIO, damage) and
 * tests/fuzz/fuzz_diverge.c its reader.
 *
 * Builds with and without TSDB_EC (the embedded crates copy every .c file of src/).
 */
#ifndef TSDB_ECDIVERGE_H
#define TSDB_ECDIVERGE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

struct eng;
struct ecdiverge; /* the DIVERGED file's state, eng.diverge */

enum {
    EC_DIV_DIGEST = 1, /* a settle's digest did not match */
    EC_DIV_PPOS = 2,   /* a part above Z at a cold apply */
    EC_DIV_TEST = 3    /* planted by a test hook (tsdbd's TSDB_REPLD_TEST=plantdiverge) */
};

/* The volume diverged at key `key` (oid: the object, may be NULL) for `why` (EC_DIV_*): durable, then the flag. 0, or
 * TSDB_ERR_IO when the write failed (the engine failed; the flag is set). Never under an engine lock (it writes). */
int ec_diverge(struct eng *db, int64_t key, const uint8_t *oid, int why);
/* The open's read of DIVERGED (after META, before any thread starts); ecdiverge_close frees it. */
int ecdiverge_open(struct eng *db);
void ecdiverge_close(struct eng *db);
/* `tsdb DIR volume diverged [--clear]` on the volume directory `dir`, with no engine open: prints its records to out
 * and returns how many there are; with `clear` removes the file (logging each record it removes to stderr), refused
 * with TSDB_ERR_LOCKED while a writer holds the directory and TSDB_ERR_INVALID on a cluster volume. */
int ecdiverge_cli(const char *dir, bool clear, FILE *out);

/* Planted bugs (tests): divergemem keeps the flag in memory only (test_keep's restart case must fail), clearcluster
 * lets --clear through on a cluster volume (test_keep's clearcli case must fail); the rest fail tests/test_diverge.c:
 * nodirsync, nofdatasync (power loss), nofail (EIO not
 * fail-stop), nocut, nocorrupt (damage taken for a torn tail), seed0 (a record moved to another offset is valid),
 * notailconfirm (a misread tail cut), readerrend (a read error taken for the end), metaopen (--clear with an
 * unreadable META counts as no cluster volume). */
enum {
    ECDIV_MUT_NONE,
    ECDIV_MUT_MEM,
    ECDIV_MUT_CLEARCLUSTER,
    ECDIV_MUT_NODIRSYNC,
    ECDIV_MUT_NOFDATASYNC,

    ECDIV_MUT_NOFAIL,
    ECDIV_MUT_NOCUT,
    ECDIV_MUT_NOCORRUPT,
    ECDIV_MUT_SEED0,
    ECDIV_MUT_NOTAILCONFIRM,
    ECDIV_MUT_READERREND,
    ECDIV_MUT_METAOPEN
};
extern int ecdiverge_mutant;
/* Test hook: nonzero flips the last byte the open's first read of DIVERGED returns (a misread page). */
extern int ecdiverge_test_misread;
/* The fuzz harness's entry: the records of a DIVERGED file of `len` bytes (no engine), as the open reads them. 0, or
 * TSDB_ERR_CORRUPT; *n the records. */
int ecdiverge_parse(const uint8_t *buf, size_t len, size_t *n);


#endif
