/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The log position (`ppos`) of every part of an erasure-coded cluster volume (design-cluster-data.md 2.3, the R8 plan's
 * D2-D3): a per-volume checksummed, append-only PPOS file, one record per part name, appended and fdatasynced before the
 * part's name is visible (a flush, parts_commit, a merge, a settle's swap), compacted at open; a listed part without a
 * record fails the open. A flush part's ppos comes from a per-memtable map base partition -> highest fno, fed at the
 * three append sites (ppos_note, under buf_lock). Lock order: publish_lock > ppos_mu; ppos_mu is held across its own
 * append and fdatasync and never taken under db->lock, latest_rw or buf_lock.
 *
 * The file: 32-byte records {u64 xxh64 of the part's name (0 taken as 1), u64 ppos, u32 flags (PPOS_F_*), u32 0,
 * u64 xxh64 of the first 24 bytes seeded with the record's offset}. A torn tail (a prefix of an append that never
 * finished, or garbage after it) is read again (tail_confirm) and cut by a writer's open; a bad record with a valid
 * one after it fails the open (TSDB_ERR_CORRUPT, a line). The last record of a name wins. At open, after load_parts,
 * every listed part (.ec ones too) must have a record, else TSDB_ERR_CORRUPT naming the part; records of names no
 * longer listed are dropped by a compaction (PPOS.tmp, fdatasync, rename, directory fsync) once they are more than
 * half. A failed write or sync of a record is fail-stop (eng_fail), and the file takes no more records.
 *
 * Lock order (for AGENTS.md): publish_lock > ppos_mu (struct ppos.mu: its append, fdatasync and, on creation, the
 * directory fsync) > ppos's map mutex (a leaf: the name -> ppos index, never held across I/O; ppos_of takes only
 * it). ppos_mu is never taken under db->lock, latest_rw or buf_lock: records go out before those are taken (a flush
 * and parts_commit before the intent's renames, a merge before its output is named in pw_finish, a swap from
 * ecapply_swap_pre before the intent's commit point).
 *
 * The per-memtable map is right only because rows reach a memtable at the three append sites alone, each noted with
 * its position under buf_lock (the replay: its own memtable, alone), and because a flush part of key K holds rows of
 * K's days only: its ppos, the highest position noted for any of those days, over-approximates (rows of other series
 * and classes in those days count) and never under-approximates. A settle of a key (r8g) relies on its fence, which
 * covers every series bucket over the key's time range (D3), so no row of the key enters a memtable between the cut
 * and the flush the swap waits for. A map that could not grow (an allocation under buf_lock failed) is marked lost,
 * and a flush then gives each of its parts the memtable's fno_hi: too high, never too low.
 *
 * "Unknown" is PPOS_UNKNOWN (UINT64_MAX), the top of the range, never 0: a part nobody knows the position of (a large
 * insert, delete or replace with no COMMIT armed, as a plain writer's) must never pass r8h's "retire a part of P only
 * with ppos <= Z", and a merge's max over its sources keeps it unknown. Its flag (PPOS_F_UNKNOWN) survives the open's
 * compaction, as every record's flags do.
 *
 * For r8h: the compaction keeps the records of listed names only, so a `settled_` source (GY_SETTLED) loses its record
 * at the next compacting open; a reverse swap that lists it again must ppos_set it first. For r8f: an attach that adds
 * `ec` to a volume with parts must ppos_set each before the next open, which otherwise fails.
 *
 * Bounds: records and index entries are only added while the engine runs (32 bytes of file and 34 to 68 of index per
 * part made: a flush, a COMMIT part, a merge output, a swap); the open's compaction drops the dead ones. A volume that
 * makes 10,000 parts a day grows by about 320 KB of file and at most 680 KB of index a day until its next open.
 *
 * Directory syncs: PPOS's creation and the compaction's rename are each followed by a directory fsync of their own
 * (eng_sync_dir), so no later name that relies on a record can reach the disk without PPOS's name.
 *
 * Single-server and non-ec volumes have no PPOS file: db->ppos stays NULL, the notes and records do nothing and
 * answer 0, and a lookup answers TSDB_ERR_INVALID.
 *
 * Builds with and without TSDB_EC (the embedded crates copy every .c file of src/).
 */
#ifndef TSDB_PPOS_H
#define TSDB_PPOS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

struct eng;
struct memtable;
struct ppos;    /* the volume's PPOS state (r8d), eng.ppos */
struct ppos_mt; /* a memtable's map base partition -> highest fno (r8d), memtable.ppos */

/* At the open of a cluster ec volume (eng.ec_cluster): reads and compacts PPOS, sets db->ppos. 0 and nothing else on
 * any other volume. */
int ppos_open(struct eng *db);
void ppos_close(struct eng *db);
/* n rows at ts (a base partition) were appended to memtable m (the active one; the replay's own) at log positions from
 * fno on: under buf_lock (the replay: alone), at every append site (mem_insert, the replay, a replica's apply_rows),
 * through eng_ppos_note (internal.h), which calls it only when db->ppos. */
void ppos_note(struct eng *db, struct memtable *m, int64_t ts, size_t n, uint64_t fno);
/* Around a flush's part writes and commit (flush_waiting, the replay's flush of its memtable): a ppos_record with pos 0
 * between the two takes each part's position from m's map. Only when db->ppos. */
void ppos_flush_begin(struct eng *db, struct memtable *m);
void ppos_flush_end(struct eng *db, struct memtable *m);
/* m's map (memtable.ppos), freed with m (mt_unref). */
void ppos_mt_free(struct memtable *m);
/* The records of the parts about to be named (names[0 .. n)), each at log position pos (0: from the memtable's map),
 * appended and synced before any of the names is visible; 0 and nothing written unless db->ppos. pos 0 means the map
 * only inside a flush window (ppos_flush_begin/end); outside one (a large write with no COMMIT armed) it records
 * PPOS_UNKNOWN, as does pos PPOS_UNKNOWN itself (a merge with an unknown source). */
int ppos_record(struct eng *db, const char *const *names, size_t n, uint64_t pos);
/* The record of one part (an .ec part at its marker's Z; 0 is taken as unknown); the ppos of a listed part, which is
 * PPOS_UNKNOWN when nobody knew it: a caller treats that as above every position. */
int ppos_set(struct eng *db, const char *name, uint64_t pos);
int ppos_of(struct eng *db, const char *name, uint64_t *pos);

#define PPOS_UNKNOWN UINT64_MAX
/* record flags: how the position was found (information only) */
enum { PPOS_F_GIVEN = 1, PPOS_F_MAP = 2, PPOS_F_FALLBACK = 4, PPOS_F_UNKNOWN = 8, PPOS_F_ALL = 15 };
/* planted bugs (tests/test_ppos.c --mutant; also TSDB_PPOS_MUTANT in the test's children) */
enum {
    PPOS_MUT_NONE,
    PPOS_MUT_AFTER,   /* pposafter: records written only at the next record or the close, after the names */
    PPOS_MUT_NOSYNC,  /* pposnosync: no fdatasync of a record */
    PPOS_MUT_FLUSHHI, /* pposflushhi: a flush part takes its memtable's fno_hi, not its days' */
    PPOS_MUT_REPLAY,  /* pposreplay: rows the open's replay adds are not noted */
    PPOS_MUT_UNKNOWN0,  /* pposunknown0: an unknown position recorded as 0, the bottom of the range */
    PPOS_MUT_FALLBACK0, /* pposfallback0: a flush whose map is lost records 0, not its memtable's fno_hi */
    PPOS_MUT_NOFAIL,    /* pposnofail: a failed write or sync of a record does not fail the engine */
    PPOS_MUT_NOBADGOOD, /* pposnobadgood: the open cuts a bad record before a valid one as a torn tail */
    PPOS_MUT_FLAGS,     /* pposflags: the open's compaction writes every record as PPOS_F_GIVEN */
    PPOS_MUT_SWAPNONE,  /* pposswapnone: a settle's swap records nothing for its .ec parts (test_eccold) */
    PPOS_MUT_SWAPAFTER, /* pposswapafter: a swap's .ec records only after its renames (test_eccold swappower) */
};
extern int ppos_mutant;
/* test helpers: the number of records the file holds (after the open's compaction); a name's flags (-1: no record);
 * ppos_test_lose_map: a memtable's map acts as if its allocation failed; ppos_test_fail_errno: the next append fails
 * with that errno (then 0 again) */
size_t ppos_test_records(struct eng *db);
int ppos_test_flags(struct eng *db, const char *name);
extern int ppos_test_lose_map, ppos_test_fail_errno;

#endif
