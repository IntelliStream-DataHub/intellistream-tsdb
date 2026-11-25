/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Snapshots of a leader's volume (design-cluster.md 8.1, R4 D3-D6): the make side and the install. snapshot.c has the
 * design.
 * Internal: engine and tsdbd only, never exported from libtsdb.so.
 */
#ifndef TSDB_SNAPSHOT_H
#define TSDB_SNAPSHOT_H

#include <limits.h>
#include <stddef.h>
#include <stdint.h>

struct eng;

/* One entry of the epoch history: `epoch` begins at position `first` (repl.h's struct repl_epoch) */
struct snap_epoch {
    uint64_t epoch, first;
};

/* What a make cut and wrote. The caller zeroes it and sets the inputs. */
struct snap_info {
    /* in: the leader's epoch history (the driver's core, any moment after the make began: entries that begin past P are
     * left out), ascending by first; may be empty (a test leader: P's epoch is then 0) */
    const struct snap_epoch *hist;
    size_t nhist;
    /* out */
    uint64_t p;        /* the cut: the commit point, the last position of a record (0: nothing committed) */
    uint64_t p_epoch;  /* the epoch of the history entry that holds P (0: none) */
    uint64_t p_seq;    /* the segment that holds P (0: no segment) */
    uint64_t s_seq;    /* the oldest segment in the snapshot (0: none) */
    uint64_t s_first;  /* its first position (0: none): the replica's log start */
    uint64_t done;     /* RSTATE's `done` at the cut */
    size_t nparts, nsegs, nstage, nkeep, nfiles;
    uint64_t bytes;    /* the bytes a receiver gets: every file's size once per inode (a keep_ file that is a listed
                        * part's link counts once), SNAPMAN's left out */
    char path[PATH_MAX]; /* the snapshot directory */
};

/* The kinds of files a SNAPMAN names */
enum snap_kind { SNAP_PART = 1, SNAP_SEG = 2, SNAP_STAGE = 3, SNAP_KEEP = 4, SNAP_DICT = 5, SNAP_RSTATE = 6, SNAP_META = 7 };

#define SNAP_MAN "SNAPMAN"
#define SNAP_MAN_VERSION 2u
#define SNAP_PREFIX "snap_"
/* A make that a merge, a flush or a retirement overtook (a part, a segment or a COMMIT's file gone between the cut and
 * its link): nothing is wrong, the caller makes the snapshot again (it removed the directory) */
#define SNAP_RETRY TSDB_ERR_UNAVAILABLE

/* Makes a snapshot of the leader engine `db` (eng_leader_attach) in the directory `name` inside the volume directory
 * (a single path element that begins with "snap_"; it must not exist). 0 and `out` filled; on any error the directory
 * is removed, nothing else changed (SNAP_RETRY: make it again). The leader goes on writing meanwhile. */
int snap_make(struct eng *db, const char *name, struct snap_info *out);
/* Removes a snapshot directory (every file, the directory, its parent synced): 0, or TSDB_ERR_IO */
int snap_release(const char *path);
/* The open of a writer or a replica: removes every snap_* directory of the volume directory (no make survives its
 * engine), and syncs the directory once one went. */
int snap_sweep(struct eng *db);

/* A SNAPMAN, read back (the verify of the tests, r4-snapinst's install) */
struct snap_file {
    char name[128];
    uint8_t kind; /* enum snap_kind */
    uint64_t size, xxh64;
    uint32_t link; /* 0, or 1 + the index of an earlier entry that names the same file (a hard link of it: a keep_
                    * file and the listed part a COMMIT published are one inode); size and xxh64 are that entry's */
};
struct snap_man {
    uint32_t version;
    int64_t volume; /* META's volume=, -1: none */
    uint8_t uuid[16]; /* META's vuuid=, zeros: none */
    uint64_t p, p_epoch, p_seq, s_seq, s_first, done;
    struct snap_epoch *hist;
    size_t nhist;
    struct snap_file *files;
    size_t nfiles;
};
/* 0, TSDB_ERR_NOTFOUND (no SNAPMAN), TSDB_ERR_CORRUPT (a bad one: also a name that is ".", "..", SNAPMAN's own or
 * holds '/' or NUL, a name twice, an unknown kind, a link that is not to an earlier entry of the same size and hash),
 * TSDB_ERR_IO, TSDB_ERR_NOMEM. The checksum proves the file whole, not who wrote it. */
int snap_man_read(const char *dir, struct snap_man *m);
void snap_man_free(struct snap_man *m);
/* xxh64 (seed 0, the engine's xxh64) of a whole file read in pieces, and its size */
int snap_xxh64_fd(int fd, uint64_t *size, uint64_t *xxh);
/* tests: the streaming hash of buf fed in pieces of `step` bytes (equal to xxh64(buf, len, 0)) */
uint64_t snap_test_xxh64(const void *buf, size_t len, size_t step);

/* Test hooks (tests/test_snapshot.c): called at each step with no lock held; `arg` is the part name at SNAP_AT_PART
 * (before its link; every part is linked before any is read), SNAP_AT_SEGLIST comes after the segments are listed */
enum { SNAP_AT_CUT = 1, SNAP_AT_PART = 2, SNAP_AT_LINKED = 3, SNAP_AT_DICTS = 4, SNAP_AT_SEGS = 5, SNAP_AT_SYNCED = 6,
       SNAP_AT_DONE = 7, SNAP_AT_SEGLIST = 8 };
extern void (*snap_test_hook)(struct eng *db, int at, const char *arg);
/* Called inside the cut, under flush_lock and publish_lock, once P, the part list and the COMMITs' files are read: the
 * test notes what the leader had there, and its verify requires the snapshot to hold all of it. `parts` is the list the
 * make took (NULL under SNAP_MUT_AFTERP): db->parts may differ by now, a merge commit takes no publish_lock */
extern void (*snap_test_cut)(struct eng *db, uint64_t p, const char (*parts)[96], size_t nparts);
/* Planted bugs (tests/test_snapshot.c --mutant NAME; the Makefile requires each to fail it) */
enum snap_mutant { SNAP_MUT_NONE = 0, SNAP_MUT_AFTERP, SNAP_MUT_DICTSHORT, SNAP_MUT_NOKEEP, SNAP_MUT_NOSTAGE,
                   SNAP_MUT_NORETIRED, SNAP_MUT_NODIRSYNC, SNAP_MUT_NONEEDSEQ, SNAP_MUT_NOSEGRETRY,
                   /* the install's */
                   SNAP_MUT_LOWERFENCE, SNAP_MUT_RAISELED, SNAP_MUT_KEEPUNSURE, SNAP_MUT_SWAPFIRST,
                   SNAP_MUT_INSTNODIRSYNC,
                   /* a leader's snapshot that lists one part fewer (tsdbd: TSDB_REPLD_MUTANT=snapdroppart; jepsen-cluster-selftest) */
                   SNAP_MUT_DROPPART };
extern int snap_mutant;

/* ---- the install (r4-snapinst; snapshot.c's second half has the design) ----
 *
 * A received copy is installed as volume `number`'s replica directory under `root` (the cluster volumes' root, where
 * v<N> lies): snap_install(root, number, "<root>/v<N>.snap", id, keep) checks every file against SNAPMAN (size,
 * xxh64; a link entry missing is made from the entry it names), refuses a file SNAPMAN does not name, checks every
 * part's header, index, footer and granule checksums, then writes META (requires=cluster,replica, volume=number, the
 * identity's volume_uuid and cluster), FNO = P + 1, REPL (floor P, log start S or P + 1, the history from S on, and the
 * promises of D6 below), SNAPDONE, each synced, the directory synced; then swaps: v<N> -> v<N>.old (when there),
 * v<N>.snap -> v<N>, the root synced, v<N>.old removed, the root synced. 0: v<N> is the snapshot's and opens with
 * eng_open_replica(floor P). On an error before SNAPDONE v<N> is untouched and v<N>.snap is left for the caller to
 * remove (snap_release) or for snap_leftovers; a retry on the same v<N>.snap is TSDB_ERR_CORRUPT once the install wrote
 * a file SNAPMAN does not name (FNO, REPL), which is not damage: release it and fetch again. An error after SNAPDONE
 * (the swap's) leaves the outcome to snap_leftovers, which finishes the swap: the caller neither releases v<N>.snap
 * nor opens v<N> before it ran. snap_leftovers runs before any install (a start does it first): v<N>.old without v<N>
 * is refused (TSDB_ERR_INVALID), it holds the only copy of the old directory and snap_leftovers puts it back.
 *
 * `keep` is the REPL of the directory being replaced; the caller reads it only after its driver stopped saving REPL
 * (the volume closed: a FENCE saved between the read and the close would be lost). Kept: rid, open generation and
 * nonce (the reopen raises it), fence = max(keep's, P's epoch) (never lowered), led as it was (never raised: led =
 * fence is what clears `unsure`), unsure, with the open generation moved by REPLFILE_OPEN_GEN_BELT. A snapshot whose P
 * is below keep's floor is refused (TSDB_ERR_INVALID). keep NULL: a fresh learner: id->rid, generation 0, fence P's
 * epoch, led 0. id: the identity REPL carries (and keep must match). */
#define SNAP_DONE "SNAPDONE"
struct replfile_state;
struct snap_ident {
    uint8_t volume_uuid[16];
    uint8_t cluster_uuid[16];
    uint32_t node;
    uint64_t incarnation;
    uint64_t rid; /* a fresh learner's (the metadata record's), never 0; with keep, keep's rid is used */
};
int snap_install(const char *root, uint32_t number, const char *staged, const struct snap_ident *id,
                 const struct replfile_state *keep);
/* What a crash left of an install of volume `number` (the start of cvolfs_prepare, before v<N> is looked at):
 * v<N>.snap with SNAPDONE is finished (v<N> -> v<N>.old when there, v<N>.snap -> v<N>); v<N>.snap without it is removed;
 * v<N>.old beside a v<N> is removed, without one it is renamed back. Every step synced. 0 or TSDB_ERR_IO. */
int snap_leftovers(const char *root, uint32_t number);
/* tests: called after the verify, after SNAPDONE and after the swap (whatever their result), and between the swap's two
 * renames when there was an old v<N> */
enum { SNAP_INST_VERIFIED = 1, SNAP_INST_DONE = 2, SNAP_INST_SWAPPED = 3, SNAP_INST_HALF = 4 };
extern void (*snap_inst_hook)(int at);

#endif
