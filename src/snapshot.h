/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Snapshots of a leader's volume (design-cluster.md 8.1, R4 D3/D4): the make side. snapshot.c's header has the design.
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
    uint64_t bytes;    /* every file's size, SNAPMAN's left out */
    char path[PATH_MAX]; /* the snapshot directory */
};

/* The kinds of files a SNAPMAN names */
enum snap_kind { SNAP_PART = 1, SNAP_SEG = 2, SNAP_STAGE = 3, SNAP_KEEP = 4, SNAP_DICT = 5, SNAP_RSTATE = 6, SNAP_META = 7 };

#define SNAP_MAN "SNAPMAN"
#define SNAP_MAN_VERSION 1u
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
/* 0, TSDB_ERR_NOTFOUND (no SNAPMAN), TSDB_ERR_CORRUPT (a bad one), TSDB_ERR_IO, TSDB_ERR_NOMEM */
int snap_man_read(const char *dir, struct snap_man *m);
void snap_man_free(struct snap_man *m);
/* xxh64 (seed 0, the engine's xxh64) of a whole file read in pieces, and its size */
int snap_xxh64_fd(int fd, uint64_t *size, uint64_t *xxh);
/* tests: the streaming hash of buf fed in pieces of `step` bytes (equal to xxh64(buf, len, 0)) */
uint64_t snap_test_xxh64(const void *buf, size_t len, size_t step);

/* Test hooks (tests/test_snapshot.c): called at each step with no lock held; `arg` is the part name at SNAP_AT_PART */
enum { SNAP_AT_CUT = 1, SNAP_AT_PART = 2, SNAP_AT_LINKED = 3, SNAP_AT_DICTS = 4, SNAP_AT_SEGS = 5, SNAP_AT_SYNCED = 6,
       SNAP_AT_DONE = 7 };
extern void (*snap_test_hook)(struct eng *db, int at, const char *arg);
/* Planted bugs (tests/test_snapshot.c --mutant NAME; the Makefile requires each to fail it) */
enum snap_mutant { SNAP_MUT_NONE = 0, SNAP_MUT_AFTERP, SNAP_MUT_DICTSHORT, SNAP_MUT_NOKEEP, SNAP_MUT_NOSTAGE,
                   SNAP_MUT_NORETIRED, SNAP_MUT_NODIRSYNC };
extern int snap_mutant;

#endif
