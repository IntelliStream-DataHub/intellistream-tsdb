/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Cluster server disk table, start checks, binding, and HBS_DISKS heartbeat section (R13, r13-start;
 * docs/design-cluster-data.md §15.1, E47-E49, E58, PLAN.md D11-D13).
 *
 * Lifecycle in tsdbd --cluster (non-witness):
 *   1. Option parsing collects --disk PATH[:CLASS] (disks_parse_spec), --role both|compute|storage (disks_parse_role),
 *      --allow-shared-device, and optionally --dir DIR (added as a disk using its DISK metadata class without /sys
 *      detection).
 *   2. disks_prepare() runs before tsdb_open() and cluster_start(): loads <state-dir>/DISKS, loads each disk's DISK
 *      metadata (refusing uninitialised/unmounted paths, marking known missing stores DOWN), checks duplicate paths
 *      and store ids, and verifies st_dev against /, --state-dir, and every other present disk (unless
 *      --allow-shared-device).
 *   3. disks_start() runs right after cluster_start() (before chunksrv_cluster_start()): verifies no live tsdbd holds
 *      a disk, checks cluster/node ownership and DESTROYED state against the metadata copy, enforces explicit :CLASS
 *      against metadata (or DISK), runs /sys detection (logging and counting tsdb_disk_class_mismatch without refusing),
 *      binds unbound disks on first start via disk_bind(), assigns disk uses and <disk>/chunks/ stores, persists
 *      <state-dir>/DISKS, and registers HBS_DISKS (section 12) and Prometheus metrics via creport.
 *   4. On a server not yet in a cluster at start, disks_bind_late() binds unbound disks once cluster_self() reports
 *      have_node (called from chunksrv_cluster's thread), persists <state-dir>/DISKS, and triggers a heartbeat.
 *   5. disks_stop() unregisters HBS_DISKS and metrics and releases disk directory locks on shutdown.
 */
#ifndef TSDBD_DISKS_H
#define TSDBD_DISKS_H

#include <limits.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "meta.h"
#include "../include/tsdb.h"
#include "../src/disk.h"

/* Heartbeat section ID registered via creport_section() (1..9 built-in, 10 HBS_PROGRESS, 11 CREPORT_SEC_CHUNKSCRUB). */
#define HBS_DISKS 12
#define DISKS_MAX META_DISKS_MAX

struct disk_spec {
    char path[PATH_MAX];
    int stated_class; /* DISK_CLASS_NONE (0) when omitted, else DISK_CLASS_NVME/SSD/HDD */
    int from_dir;     /* 1 when implicitly derived from --dir */
};

struct disks_cfg {
    const struct disk_spec *specs;
    int nspecs;
    const char *state_dir;
    uint8_t role;            /* enum meta_role */
    int allow_shared_device; /* --allow-shared-device */
};

/* Decoded HBS_DISKS payload stored in heard->ext (read by cluster.c's propose_disks and build_place_members). */
struct hbs_disk_row {
    uint8_t store_id[16];
    uint8_t disk_class; /* enum meta_disk_class */
    uint8_t uses;       /* enum meta_disk_uses bitmask */
    uint8_t state;      /* enum meta_store_state */
    uint8_t have_usage; /* 1 when young_bytes/chunk_bytes are reported (r13-fill or test knob) */
    uint64_t size_bytes;
    uint64_t sync_us;
    uint64_t young_bytes; /* bytes in young-tier uses on this disk (valid when have_usage != 0) */
    uint64_t chunk_bytes; /* bytes in chunk use on this disk (valid when have_usage != 0) */
};

struct hbs_disks {
    uint8_t role;                /* enum meta_role: declared --role */
    uint8_t allow_shared_device; /* 1 while --allow-shared-device is set */
    uint8_t drain_no_space;      /* 1 while any draining store stopped because no target store has room */
    uint32_t class_mismatches;   /* local /sys detection disagreements */
    uint32_t ndisks;
    struct hbs_disk_row disks[];
};

/* Parse PATH[:nvme|ssd|hdd] into `out`. Returns 0, or -1 with message in `err`. */
int disks_parse_spec(const char *arg, struct disk_spec *out, char *err, size_t errlen);
/* Parse "both", "compute", or "storage" into `*role_out`. Returns 0 or -1. */
int disks_parse_role(const char *arg, uint8_t *role_out);

/* Phase 1 (before tsdb_open and cluster_start): load DISK files, check st_dev and missing stores. */
int disks_prepare(const struct disks_cfg *cfg, char *err, size_t errlen);
/* Primary data directory chosen from prepared disks when --dir was omitted (pinned primary from DISKS first, else
 * present nvme -> ssd -> hdd; never a non-present disk). */
const char *disks_primary_dir(void);

/* Register HBS_DISKS (section 12) encoder/decoder/formatter with creport (safe on witnesses too). */
int disks_creport_register(void);
/* Phase 2 (after cluster_start, before chunksrv_cluster_start): ownership/DESTROYED/class checks, binding,
 * chunk-store path selection, HBS_DISKS and metrics registration. */
int disks_start(char *err, size_t errlen);
/* Late binding once a newly-started server joins a cluster (called off the cluster thread). */
int disks_bind_late(const uint8_t cluster_uuid[16], uint32_t node_id, char *err, size_t errlen);
/* Fill `paths[0..cap)` with the `<disk>/chunks` directories this server serves and store count in `*n_out`. */
void disks_chunk_paths(const char **paths, int cap, int *n_out);
/* Like disks_chunk_paths, also filling optional per-slot dedupe preference ranks `prefs[0..cap)` (capacity > fast >
 * draining) and initial `draining[0..cap)` flags (R13 r13-lmove; E50). */
void disks_chunk_layout(const char **paths, int *prefs, bool *draining, int cap, int *n_out);
/* Mark a local disk's chunk store as draining (or clear it) at runtime (prepared for r13-dev's `disk out`/`disk class`)
 * and wake the local drain driver. Returns 0 on success, -1 if store_id is unknown or has no chunk store. */
int disks_mark_draining(const uint8_t store_id[16], int draining);
/* Start/stop the local chunk drain driver (R13 r13-lmove; after chunksrv_cluster_start, before chunksrv_cluster_stop). */
int disks_drain_start(void);
void disks_drain_stop(void);

/* Read-only snapshot of one configured disk (for multi-disk cvolfs/scrubd; R13 r13-cvolfs). */
struct disk_info {
    char path[PATH_MAX];
    uint8_t store_id[16];
    int disk_class; /* DISK_CLASS_NVME / DISK_CLASS_SSD / DISK_CLASS_HDD */
    uint8_t uses;   /* enum meta_disk_uses bitmask */
    uint8_t state;  /* enum meta_store_state */
    int present;    /* 1 if DISK loaded and present on this start */
    int recovering; /* 1 while disk health domain is RECOVERING (existing v<N> reopen allowed) */
    uint64_t size_bytes;
};
/* Copy up to `cap` local disk entries into `out`. Returns count copied (0 before disks_prepare). Thread-safe. */
int disks_list(struct disk_info *out, int cap);

/* Per-disk O_DIRECT health probes & fault isolation (R13, r13-fault; E57, §15.6, PLAN.md D16).
 * Callback runs off g_mu and off health->mu; must not block. `old_state` / `new_state` are TSDB_DOM_*. */
typedef void (*disk_probe_cb)(void *ctx, int disk_idx, const uint8_t store_id[16], const char *path, int disk_class,
                              uint8_t uses, int old_state, int new_state);

int disk_probe_start(disk_probe_cb cb, void *ctx, char *err, size_t errlen);
void disk_probe_set_stall_s(uint32_t disk_stall_s);
void disk_probe_now_store(const uint8_t store_id[16]);
void disk_probe_now_chunk(int chunk_slot);
void disk_probe_set_up(int disk_idx);
int disk_probe_state(int disk_idx);
int disk_probe_info_by_store(const uint8_t store_id[16], tsdb_domain_info *out);
void disk_probe_chunk_failed(int chunk_slot, int err_code);
void disk_probe_stop(void);

/* Unregister HBS_DISKS / metrics and release disk locks. */
void disks_stop(void);

#ifdef TSDB_TEST_HOOKS
enum {
    DISKS_MUT_NONE = 0,
    DISKS_MUT_NODEVCHECK = 1,    /* nodevcheck: st_dev not compared */
    DISKS_MUT_CLASSSILENT = 2,   /* classsilent: stated class mismatch accepted silently */
    DISKS_MUT_MISSINGASNEW = 3,  /* missingasnew: a known store missing its DISK is initialised as new */
    DISKS_MUT_STALLEXIT = 4,     /* stallexit: exit(1) when a fast disk probe goes DOWN */
    DISKS_MUT_STALLNORESIGN = 5, /* stallnoresign: keep leaders on a DOWN fast disk instead of deposing */
    DISKS_MUT_STOREEIOEXIT = 6,  /* storeeioexit: exit(1) when a chunk store fails EIO/ENOSPC or goes DOWN */
};
int disks_mutant_parse(const char *name);
int disks_mutant(void);
#endif

#endif /* TSDBD_DISKS_H */
