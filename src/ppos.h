/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The log position (`ppos`) of every part of an erasure-coded cluster volume (design-cluster-data.md 2.3, the R8 plan's
 * D2-D3): a per-volume checksummed, append-only PPOS file, one record per part name, appended and fdatasynced before the
 * part's name is visible (a flush, parts_commit, a merge, a settle's swap), compacted at open; a listed part without a
 * record fails the open. A flush part's ppos comes from a per-memtable map base partition -> highest fno, fed at the
 * three append sites (ppos_note, under buf_lock). Lock order: publish_lock > ppos_mu; ppos_mu is held across its own
 * append and fdatasync and never taken under db->lock, latest_rw or buf_lock.
 *
 * STUB (r8a1-mk): filled by r8d-ppos. Until then no volume has a PPOS file: db->ppos stays NULL, the notes and records
 * do nothing and answer 0, and a lookup answers TSDB_ERR_INVALID ("not built yet").
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
 * appended and synced before any of the names is visible; 0 and nothing written unless db->ppos. */
int ppos_record(struct eng *db, const char *const *names, size_t n, uint64_t pos);
/* The record of one part (an .ec part at its marker's Z); the ppos of a listed part. */
int ppos_set(struct eng *db, const char *name, uint64_t pos);
int ppos_of(struct eng *db, const char *name, uint64_t *pos);

#endif
