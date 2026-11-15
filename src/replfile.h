/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The REPL file of a replica directory (docs/design-cluster.md 4.3): what a replica must keep across crashes and
 * power loss and nothing else holds: its instance id (`rid`), its open generation, its fence and `led`, the epoch
 * history, the log start and the commit floor, with the identity it belongs to (volume uuid, cluster uuid, node,
 * incarnation).
 *
 * The file is FNO's scheme (src/fno.c): two checksummed 4 KiB slots on pages of their own, written alternately, each
 * save overwriting the older generation and fdatasync'ed before replfile_save returns; the newer valid generation
 * wins. A torn write reaches one page, so the other holds the state before it, and nothing relied on the newer state
 * before the save returned. Rot of a slot after its save looks the same, so a torn load is "unsure" (see replfile_load).
 * A damaged slot is logged and the other is used (the next save overwrites it); both bad is
 * TSDB_ERR_CORRUPT. A failed write or sync is fail-stop for the volume (the caller's): the handle then refuses every
 * save and never retries, because a retried fsync can return 0 after the kernel dropped the pages.
 *
 * Fields that never change in the life of a directory (volume uuid, cluster uuid, node, rid) are checked at every save,
 * and the ones that never go back (open_gen, fence, led, floor, log_start) too: a save that breaks either is refused
 * (TSDB_ERR_INVALID) with nothing written and the handle still good.
 *
 * Why src/ and not server/: the replica driver of the engine (src/replica.c) feeds `struct repl_durable` from it, and
 * the tests and the fuzz harness link libtsdb. It uses only the engine's checksum and sync shims.
 */
#ifndef TSDB_REPLFILE_H
#define TSDB_REPLFILE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "repl.h"

#define REPLFILE_NAME "REPL"
#define REPLFILE_PAGE 4096
#define REPLFILE_OPEN_GEN_BELT (1ull << 32) /* what a torn load adds to open_gen */
#define REPLFILE_MAX_HIST 247 /* epoch entries one slot holds (16 bytes each after a 128 byte head and a checksum) */

struct replfile_state {
    uint8_t volume_uuid[16];
    uint8_t cluster_uuid[16];
    uint32_t node;                /* this replica's node id */
    uint64_t incarnation;         /* of the server run that last saved */
    uint64_t rid;                 /* replica instance, never 0, drawn at create and never changed */
    uint64_t open_gen;            /* raised (and saved) at every open */
    uint64_t open_nonce;          /* the draw of the open that saved open_gen (0: none, as in a fresh directory): two opens
                                   * that saved one generation differ here (server/cvol.c); free to change at a save */
    uint64_t fence;               /* the highest epoch this replica has saved */
    uint64_t led;                 /* the highest epoch it has led */
    uint64_t floor;               /* the commit floor */
    uint64_t log_start;           /* first retained position (0 or 1: all of the log) */
    struct repl_epoch *hist;      /* malloc'd by create/load/from_durable; replfile_state_free */
    uint32_t nhist;
    bool unsure;                  /* fence, led, floor, log_start may be older than what was acknowledged: set by a torn
                                   * load, stored by every save, and cleared only by one that sets led = fence */
};

struct replfile;

/* A rid for a fresh directory: 64 random bits, never 0. 0 on failure (no randomness). */
uint64_t replfile_new_rid(void);

/* Makes REPL in the directory `dirfd` (the caller's tmp directory, which it renames into place afterwards) with both
 * slots holding *st: written to REPL.tmp, synced, renamed, and the directory synced when `sync` is set. If st->rid is
 * 0 a new one is drawn and stored in *st. An existing REPL is TSDB_ERR_INVALID (a directory has one rid for life).
 * *out is the open handle (generation 1). TSDB_ERR_IO, _NOMEM, _INVALID (a state that cannot be stored). */
int replfile_create(int dirfd, struct replfile_state *st, bool sync, struct replfile **out);

/* Reads REPL. TSDB_ERR_NOTFOUND: no file. TSDB_ERR_CORRUPT: both slots bad (logged). TSDB_ERR_NOMEM: never taken for
 * damage. *torn is set when one slot was bad and the other used (logged). *st is the newer valid slot (its history
 * malloc'd).
 *
 * A torn load is "unsure", not "the state before an unacknowledged save": rot or a misdirected write of the newest slot
 * after its save returned looks exactly like a torn write, and then fence, led, floor and log_start are older than
 * what the replica acknowledged (a restarted leader could resume an epoch it led, a deposed leader's records pass the
 * fence, log_start names segments that are gone). So the caller must treat them as unknown: no leading and no fence
 * answer until it has the volume's current epoch from the metadata group and has saved fence = led = that epoch, and
 * log_start comes from the oldest segment on disk. Only open_gen is dealt with here: it is raised by
 * REPLFILE_OPEN_GEN_BELT (a gap, never a repeat, as FNO_BELT), so that a registration with the metadata group
 * (gen > recorded) cannot collide with one the lost save carried; the handle holds saves to that value.
 * st->unsure is set by a torn load, and the next save stores it: after a restart the load is clean and the older
 * slot's values would look trusted otherwise. *torn is the damage found on this load only; st->unsure also holds for
 * a REPL an earlier open saved after a torn load. replfile_save clears the flag only in a save with led = fence. */
int replfile_load(int dirfd, struct replfile **out, struct replfile_state *st, bool *torn);

/* Stores *st in the older slot and fdatasyncs it (`sync` of create/load is not kept: a save always syncs). Returns 0
 * only once the state is durable. TSDB_ERR_IO: the write or the sync failed, which is fail-stop for the caller; the
 * handle refuses every later save with TSDB_ERR_IO. TSDB_ERR_INVALID: nothing written (identity changed, a value went
 * back, led above fence, a history epoch above fence, log_start - 1 above floor, a history that does not fit: the entries wholly below position log_start - 1 are dropped from the stored copy first, and
 * if more than REPLFILE_MAX_HIST remain the save is refused). */
int replfile_save(struct replfile *rf, const struct replfile_state *st);
/* The generation of the slot last read or written (tests). */
uint64_t replfile_gen(const struct replfile *rf);
bool replfile_failed(const struct replfile *rf);
void replfile_close(struct replfile *rf);

void replfile_state_free(struct replfile_state *st);

/* The core's view: the fields of *d that REPL holds. log_end is the driver's own scan of the log, not REPL's.
 * d->hist points into *st (valid while *st is). The entries that start beyond log_end are
 * left out: they name positions the log does not have (the epoch was saved, the power went before its marker). */
void replfile_to_durable(const struct replfile_state *st, uint64_t log_end, struct repl_durable *d);
/* The other way: rid, fence, led, floor, log_start and the history of *d into *st (the identity fields stay). */
int replfile_from_durable(struct replfile_state *st, const struct repl_durable *d);

/* Pure encoding, for the fuzz harness and the tests: one slot page. replfile_encode needs a page of REPLFILE_PAGE
 * bytes; TSDB_ERR_INVALID when the state cannot be encoded (nhist too large, rid 0, history out of order).
 * replfile_parse takes the bytes it is given (any n) and is true only for a slot that is whole and well formed. */
int replfile_encode(uint8_t *page, const struct replfile_state *st, uint64_t gen);
bool replfile_parse(const uint8_t *page, size_t n, struct replfile_state *st, uint64_t *gen);

/* The two pages of a file (got bytes, a short file counts as bad pages) as a load reads them: the newer valid slot into
 * *st, *slot, *gen, *torn. TSDB_ERR_CORRUPT: both bad; TSDB_ERR_NOMEM: an allocation failed (not damage). For the
 * fuzz harness and the tests; load adds the open generation belt on top. */
int replfile_pick(const uint8_t *pages, size_t got, struct replfile_state *st, uint64_t *gen, int *slot, bool *torn);

/* Test hook: planted bugs, which tests/test_replfile.c requires to fail it. */
enum { REPLFILE_MUT_NONE, REPLFILE_MUT_NOSYNC, REPLFILE_MUT_SAMESLOT, REPLFILE_MUT_NOCHECKSUM };
extern int replfile_test_mutant;
extern int replfile_test_nomem;    /* a slot with history fails its allocation */
extern int replfile_test_eio_read; /* load's read fails with EIO (a read error is never end of file) */

#endif
