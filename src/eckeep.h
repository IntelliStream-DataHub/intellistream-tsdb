/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Settled sources (R7, r7-keep; design-cluster-data.md E26, §2.3 step 7; the plan's D5): settled_<part>.part, the
 * source a settle's swap renamed (src/ecswap.c), is kept for settle_keep_s (the tier's keep_s) counted from the .ec
 * record's settle time, never from the file's mtime (a rename keeps the source's write time, so an mtime deadline would
 * fall due at once after a restart), and for as long as its digest has not matched. GY_SETTLED in the graveyard.
 *
 * The digest, one per settled_ file, after each swap and again for every settled_ file after each open (before any is
 * released): the object read whole through chunk_obj_read without CHUNK_RR_RAW (every cell checked against the
 * record's sums, a bad one rebuilt from the others), its xxh64 compared with the record's object_xxh64, and the
 * settled_ file read and hashed too: both must equal the record's. A store that is not UP holds the digest (it waits
 * and tries again): it never "matches" by reading around a position that is away. A match releases the file at its
 * deadline. A mismatch, an object that cannot be read whole, or a file of another length holds the file for good, logs
 * one line naming volume, partition, oid and both files, and sets db->diverged: the route() family of src/volumes.c then
 * answers TSDB_ERR_UNAVAILABLE for the volume, across restarts too (the DIVERGED file, src/ecdiverge.h, R8 D8), until
 * an operator runs `tsdb DIR volume diverged --clear`. An in-process reopen (tsdb_volume_reopen, tsdb_cvol_replace)
 * inherits the flag as well.
 A settled_ file whose
 * .ec part is no longer listed (a drop took the key) has no object to compare with: its rows are gone from the volume
 * anyway, and it is released keep_s after it was found so.
 *
 * The object is read a whole number of stripes (k' cells) at a time, so no cell is read and checked twice and a damaged
 * stripe is rebuilt once. The digests run on one thread per engine (nice 19, throttled to eckeep_digest_mb_s of reads,
 * both files counted, per engine),
 * started by the first swap or the first graveyard purge after the open (never during load_parts, which lists parts
 * without db->lock) and stopped by eckeep_eng_stop, which eng_close and eng_halt call right after the merge threads
 * (the thread holds part references and takes db->lock). Its mutex is a leaf: never held across I/O or an engine lock.
 */
#ifndef TSDB_ECKEEP_H
#define TSDB_ECKEEP_H

#include "internal.h"

/* load_parts met settled_<p...>.part (settled_part_name): a writer adds a held graveyard entry and a pending digest; a
 * read-only open leaves the file alone. TSDB_ERR_NOMEM fails the open. */
int eckeep_load_settled(eng *db, const char *name, bool read_only);
/* graveyard_purge, for a GY_SETTLED entry (db->lock held): true when it leaves the list (the file was unlinked: its
 * digest matched, or its .ec part was gone, and its deadline passed). `all` changes nothing: a settled_ file is never
 * released before its digest and its deadline. Starts the digest thread when it has not started yet. */
bool eckeep_purge_entry(eng *db, const graveyard_entry *e, bool all, int64_t now);
/* ec_swap_commit, after a swap is durable (publish_lock released, no engine lock held): settled_<in[i]->name> for
 * each i is held and queued for its digest. Out of memory: the file stays on disk unreleased until the next open. */
void eckeep_swapped(eng *db, part *const *in, size_t n);

/* The engine's hooks (src/tier.h): db->ecstate->keep. eckeep_eng_stop (eng_close, eng_halt) joins the digest thread;
 * once stopped it never starts again. Each does nothing without a tier. */
int eckeep_eng_open(eng *db);
void eckeep_eng_stop(eng *db);
void eckeep_eng_close(eng *db);

/* What a settled_ file's digest says (eckeep_status) */
enum { ECKEEP_PENDING = 0, ECKEEP_MATCHED, ECKEEP_DIVERGED, ECKEEP_ORPHAN };
/* The state of settled_ file `name`, or TSDB_ERR_NOTFOUND (not held: released, or never seen). */
int eckeep_status(eng *db, const char *name);
/* tests: waits up to timeout_ms for `name` to leave ECKEEP_PENDING; its state (or TSDB_ERR_NOTFOUND). */
int eckeep_test_wait(eng *db, const char *name, int64_t timeout_ms);
/* tests: digests started since the open (each try counts, a store that was not UP included) */
uint64_t eckeep_test_tries(eng *db);
/* tests: whether the engine's digest thread was told to stop (eckeep_eng_stop): it never runs again */
bool eckeep_test_stopped(eng *db);
/* tests: while set the thread starts no digest */
extern atomic_bool eckeep_test_pause;
/* MB per second the digest reads (the object and the file together), per engine: n tiered volumes read up to n times
 * this; 0: no limit. Set before engines open. */
extern unsigned eckeep_digest_mb_s;

/* planted bugs (tests/test_keep.c --mutant NAME; the Makefile requires each to fail it) */
enum {
    ECKEEP_MUT_NONE = 0,
    ECKEEP_MUT_DIGESTSKIP,   /* digestskip: every digest "matches" without reading anything */
    ECKEEP_MUT_DIGESTRAW,    /* digestraw: the object is read with CHUNK_RR_RAW (a bad cell's bytes are hashed) */
    ECKEEP_MUT_DIGESTNOHOLD, /* digestnohold: a diverged file is released at its deadline like a matched one */
    ECKEEP_MUT_KEEPMTIME,    /* keepmtime: the deadline counts from the settled_ file's mtime */
    ECKEEP_MUT_NOOBJCMP,     /* noobjcmp: the object's xxh64 is not compared with the record's (only the file's is) */
    ECKEEP_MUT_OPENWAIT,     /* openwait: an object that cannot be opened with every store UP waits, never DIVERGED */
    ECKEEP_MUT_NOLENCHECK,   /* nolencheck: a settled_ file of another length is not refused (its first bytes hashed) */
    ECKEEP_MUT_ORPHANNOW,    /* orphannow: an orphan is released at once, not keep_s after it was found */
    ECKEEP_MUT_NOHALTSTOP,   /* nohaltstop: eng_halt leaves the digest thread running (src/db.c) */
};
extern int eckeep_mutant;
#endif
