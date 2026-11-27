/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The disk side of tsdbd's cluster volume manager (server/cvol.c, steps 1 and 2 of its header): making a replica
 * directory <root>/v<N> whole, and reading, checking and raising its open generation in REPL (src/replfile.c).
 * Split out so that tests/test_cvolfs.c can cut the power at every file-system step of it (crashfs) without a server.
 */
#ifndef TSDBD_CVOLFS_H
#define TSDBD_CVOLFS_H

#include <limits.h>
#include <stddef.h>
#include <stdint.h>

/* One local disk candidate for multi-disk log replica placement and scanning (R13, E48, E51, D14). */
struct cvolfs_disk {
    char path[PATH_MAX];
    uint8_t store_id[16];
    int disk_class;      /* DISK_CLASS_NVME(1), DISK_CLASS_SSD(2), DISK_CLASS_HDD(3) */
    uint8_t state;       /* enum meta_store_state (META_STORE_UP = 0) */
    int present;         /* 1 if mounted/present on this start */
    int recovering;      /* 1 while disk health domain is RECOVERING (existing v<N> reopen allowed, no new placement) */
    int have_usage;      /* 1: use size_bytes/used_bytes directly (tests); 0: statvfs(path) */
    uint64_t size_bytes; /* total filesystem bytes (when have_usage != 0) */
    uint64_t used_bytes; /* used filesystem bytes (when have_usage != 0) */
    uint32_t nreplicas;  /* log replicas currently placed on this disk */
    double wal_bps;      /* sum of 1-hour WAL bytes/s of log replicas on this disk */
};

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
    uint32_t young_move_pct;       /* E51: projected use mark (default 80 when 0) */
    uint8_t log_on_hdd;            /* E50: 1 allows hdd only when no fast disk is UP */
};

/* What a prepare saved: the open to report in the heartbeat. */
struct cvolfs_open {
    uint64_t rid, gen, nonce;
    int unsure; /* REPL is unsure (torn on load now or on an earlier open: replfile.h): fence, led, floor, log start and
                 * history are not trusted (none is used yet) */
    char disk_root[PATH_MAX]; /* disk root holding v<N> */
    uint8_t store_id[16];     /* disk store_id holding v<N> (all zeros when unknown/single-root) */
};

/* 1 if disk `d` should be scanned for v<N> and siblings (UP present fast disk, or UP present hdd with log_on_hdd). */
int cvolfs_disk_scannable(const struct cvolfs_disk *d, int log_on_hdd);

/* E51 disk picker for a new v<N>: returns index into disks[0..ndisks) or -1 if no eligible disk exists below
 * young_move_pct. Rules: UP present fast disks only; hdd allowed only when log_on_hdd != 0 and no fast disk is UP.
 * Rejects any disk whose projected use >= young_move_pct (a brand-new empty v<N> adds 0 initial bytes; callers with a
 * non-zero projection include it in used_bytes); minimizes replicas weighted by 1-hour WAL bytes/s, tie-breaking nvme
 * before ssd, then lower fill %, then lower index. */
int cvolfs_pick_disk(const struct cvolfs_disk *disks, int ndisks, uint32_t young_move_pct, int log_on_hdd);

/* Removes a leftover <root>/v<N>.tmp directory (from an interrupted make); 0 or -1 with a line on stderr. */
int cvolfs_clean_tmp(const char *root, uint32_t number);

/* Multi-disk prepare (R13, E48, E51, D14): scans every scannable disk for v<N> and its siblings (snapinst_leftovers,
 * v<N>.tmp leftovers per disk); refuses if >= 2 disks hold v<N> naming both paths; places a new v<N> on the disk
 * cvolfs_pick_disk chooses (reading format lines from <meta_root>/META); loads, checks, and raises REPL. */
int cvolfs_prepare_disks(const char *meta_root, const struct cvolfs_disk *disks, int ndisks,
                         const struct cvolfs_rec *r, struct cvolfs_open *out);

/* Single-root wrapper (meta_root = root, one UP ssd disk at root). */
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
    CVOLFS_MUT_TORNSTALE,    /* a torn REPL is never judged an older copy (only its belt-raised generation counts) */
    CVOLFS_MUT_NODIRSYNC,    /* the new directory is renamed into place without the sync of the root (lost after the ack) */
    CVOLFS_MUT_STALEOPEN,    /* cvolset_prepare leaves a v<N> of another rid in place (refused: never joins again) */
    CVOLFS_MUT_REMOVENODIRSYNC, /* a removal's rename is not followed by the root's fsync */
    CVOLFS_MUT_REMOVEDOPEN,  /* cvolset_close does not close a removed replica: it keeps serving */
    CVOLFS_MUT_MAKEMISSING,  /* a missing v<N> with a recorded open is made again (fresh) instead of refused */
    CVOLFS_MUT_LOSTASSTALE,  /* a reset's old v<N> is set aside as stale-<rid>, not lost-<rid> (test_cluster resetlost) */
    CVOLFS_MUT_FIRSTDISK,    /* R13 E51: always pick disk 0 (first eligible disk) for a new v<N> */
    CVOLFS_MUT_DUPOK         /* R13 D14: accept duplicate v<N> across two disks instead of refusing */
};
extern int cvolfs_test_mutant;

#endif
