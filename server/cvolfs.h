/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The disk side of tsdbd's cluster volume manager (server/cvol.c, steps 1 and 2 of its header): making a replica
 * directory <root>/v<N> whole, and reading, checking and raising its open generation in REPL (src/replfile.c).
 * Split out so that tests/test_cvolfs.c can cut the power at every file-system step of it (crashfs) without a server.
 */
#ifndef TSDBD_CVOLFS_H
#define TSDBD_CVOLFS_H

#include <stdint.h>

/* What the metadata group's record and this server say about one replica of a volume. */
struct cvolfs_rec {
    uint32_t number;
    char name[64];
    uint8_t uuid[16], cluster[16]; /* the volume's and the cluster's: from the record (META's volume_uuid=, cluster=) */
    uint32_t node;                 /* this server's node id: from the cluster thread's push (`self`) */
    uint64_t incarnation;          /* of this server run (REPL says which run saved last); see cvol.c */
    uint64_t rid;                  /* this server's replica instance, from the record */
    uint64_t open_gen, open_nonce; /* the last REPLICA_OPEN the group committed for it */
    int stale_first;               /* test switch: the record is a copy one open behind (TSDB_CLUSTER_TEST) */
};

/* What a prepare saved: the open to report in the heartbeat. */
struct cvolfs_open {
    uint64_t rid, gen, nonce;
    int unsure; /* REPL was torn on load: fence, led, floor, log start and history are not trusted (none is used yet) */
};

/* Makes <root>/v<N> whole or not at all, if it does not exist yet; then loads its REPL, checks it against `r` (identity,
 * rid, generation, nonce: an older copy of the directory, another instance, another directory), and saves generation + 1
 * under a new random nonce (REPL's fdatasync) before returning it in *out. 0, or -1 with a line on stderr naming the
 * path: the volume is then not opened (a failed save is fail-stop for the volume: the handle is dropped, never
 * retried; a missing REPL, one with no valid slot, or an I/O error are refusals as well). */
int cvolfs_prepare(const char *root, const struct cvolfs_rec *r, struct cvolfs_open *out);

/* One log line in tsdbd's "volumes" format, on stderr. */
void cvolfs_say(const char *fmt, ...) __attribute__((format(printf, 1, 2)));

/* Test hook: planted bugs, which tests/test_cvolfs.c and tests/test_cluster.c (scenario volumes) require to fail. */
enum {
    CVOLFS_MUT_NONE,
    CVOLFS_MUT_SAMERID,      /* a new instance id at every open (REPL refuses to save it: the volume is not opened) */
    CVOLFS_MUT_REPLNONCE,    /* the nonce in REPL is not compared */
    CVOLFS_MUT_NONONCE,      /* the nonce is reported but not saved (REPL keeps 0) */
    CVOLFS_MUT_TRUSTUNSURE,  /* a torn REPL is trusted: no skip of a belt in the open generation */
    CVOLFS_MUT_NODIRSYNC     /* the new directory is renamed into place without the sync of the root (the rename can be lost after the ack) */
};
extern int cvolfs_test_mutant;

#endif
