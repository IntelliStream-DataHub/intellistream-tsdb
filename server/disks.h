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

/* Per-disk fill/mark flags carried in HBS_DISKS v3 (R13 r13-fill; E30, E50, E53, §15.4). */
#define HBS_DISK_F_USAGE      1u  /* usage byte counters are valid */
#define HBS_DISK_F_WARN       2u  /* young_fill_pct >= young_warn_pct (75% default) */
#define HBS_DISK_F_MOVE       4u  /* young_fill_pct >= young_move_pct (80%: no new placement + move needed) */
#define HBS_DISK_F_FULL       8u  /* young_fill_pct >= young_high_pct (90% with 2-point hysteresis) */
#define HBS_DISK_F_CHUNK_FULL 16u /* chunk store past fill mark (settles wait; rebuilds allowed up to 95%) */

/* Per-disk device-lifecycle flags carried in HBS_DISKS v3 e[94] (R13 r13-dev; E12, E27, E54, §5.5, §15.6). */
#define HBS_DISK_DF_FAILED        1u /* disk/chunk failed in service (EIO/ENOSPC -> failstop_out_s) */
#define HBS_DISK_DF_DAMAGED       2u /* r12-chunk daily damage > scrub_out_count -> immediate OUT */
#define HBS_DISK_DF_DRAINING      4u /* local r13-lmove drain in progress */
#define HBS_DISK_DF_NO_LOCAL_ROOM 8u /* OUT chunk disk has no local capacity room -> move stripe positions off node */

struct cluster_copy;

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
    uint32_t adopt_old;      /* --adopt OLD (R13 r13-adopt; 0: off) */
};

/* Decoded HBS_DISKS payload stored in heard->ext (read by cluster.c's propose_disks and build_place_members). */
struct hbs_disk_row {
    uint8_t store_id[16];
    uint8_t disk_class; /* enum meta_disk_class */
    uint8_t uses;       /* enum meta_disk_uses bitmask */
    uint8_t state;      /* enum meta_store_state */
    uint8_t have_usage; /* 1 when usage bytes are reported (r13-fill or test knob) */
    uint8_t fill_flags; /* HBS_DISK_F_* bitmask */
    uint8_t dev_flags;  /* HBS_DISK_DF_* bitmask (R13 r13-dev) */
    uint8_t young_fill_pct;
    uint8_t chunk_fill_pct;
    uint64_t size_bytes;
    uint64_t sync_us;
    uint64_t young_bytes;    /* total young-tier bytes on this disk (young_data+retained+retired+local+index) */
    uint64_t chunk_bytes;    /* bytes in chunk use on this disk */
    uint64_t retained_bytes; /* retained WAL segments (E29) */
    uint64_t retired_bytes;  /* retired settle parts kept settle_keep_s (E26) */
    uint64_t local_bytes;    /* local copies (ec_keep_local_h) */
    uint64_t index_bytes;    /* .ec index files */
    uint64_t young_peak_7d;  /* 7-day peak of young-tier bytes (shared disk young reserve, §15.4) */
};

struct hbs_disks {
    uint8_t role;                /* enum meta_role: declared --role */
    uint8_t allow_shared_device; /* 1 while --allow-shared-device is set */
    uint8_t drain_no_space;      /* 1 while any draining store stopped because no target store has room */
    uint32_t class_mismatches;   /* local /sys detection disagreements */
    uint32_t adopt_old;          /* R13 r13-adopt: non-zero while waiting to adopt member adopt_old */
    uint32_t adopt_chunk_stores; /* number of pending-adoption chunk stores waiting on adopt_old */
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

/* Read-only snapshot of one configured disk (for multi-disk cvolfs/scrubd; R13 r13-cvolfs/r13-fill). */
struct disk_info {
    char path[PATH_MAX];
    uint8_t store_id[16];
    int disk_class; /* DISK_CLASS_NVME / DISK_CLASS_SSD / DISK_CLASS_HDD */
    uint8_t uses;   /* enum meta_disk_uses bitmask */
    uint8_t state;  /* enum meta_store_state */
    int present;    /* 1 if DISK loaded and present on this start */
    int recovering; /* 1 while disk health domain is RECOVERING (existing v<N> reopen allowed) */
    int have_usage; /* 1 when young_room_bytes/young_used_bytes are populated by disk_usage_* */
    uint64_t size_bytes;
    uint64_t young_room_bytes; /* size_bytes (or size_bytes - chunk_bytes on shared disks) */
    uint64_t young_used_bytes; /* effective young-tier bytes counted against young_room_bytes */
    uint8_t young_fill_pct;
    uint8_t chunk_fill_pct;
    uint8_t fill_flags; /* HBS_DISK_F_* */
};
/* Copy up to `cap` local disk entries into `out`. Returns count copied (0 before disks_prepare). Thread-safe. */
int disks_list(struct disk_info *out, int cap);

/* Per-disk bytes-by-use accounting, 75/80/90% fill marks, per-volume FULL, and chunk-store fill mark (R13 r13-fill;
 * E30, E50, E53, §15.4). Samples every 10 s (or faster under test) off any engine lock.
 * Callbacks decouple disks.o (also linked into tsdb CLI) from cvol.o / repld.o. */
struct disk_usage_hooks {
    int (*vol_disk)(uint32_t vol, char *root_out, size_t cap, uint8_t store_id_out[16]);
    uint32_t (*vols_on_disk)(const uint8_t store_id[16], uint32_t *vols_out, uint32_t cap);
    void (*wake_vol)(uint32_t vol);
};
int disk_usage_start(tsdb *db, const struct disk_usage_hooks *hooks);
void disk_usage_stop(void);
void disk_usage_kick(void);
int disks_usage_active(void);
int disks_any_full(void);
int disks_vol_full(uint32_t vol);
uint8_t disks_vol_fill_pct(uint32_t vol);
int disks_chunk_store_can_put(int chunk_slot, int is_rebuild);

struct disk_fill_snap {
    uint8_t store_id[16];
    int disk_class;
    uint8_t uses;
    uint8_t fill_flags;
    uint8_t young_fill_pct;
    uint8_t chunk_fill_pct;
    uint64_t size_bytes;
    uint64_t young_data_bytes; /* pure young data: active WAL + young parts + metadata */
    uint64_t retained_bytes;
    uint64_t retired_bytes;
    uint64_t local_bytes;
    uint64_t index_bytes;
    uint64_t chunk_bytes;
    uint64_t young_peak_7d;
};
int disks_usage_snapshot(struct disk_fill_snap *out, int cap);

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

/* R13 (r13-dev): device OUT, return-before-OUT attach, damage-triggered OUT, replace, and class change hooks. */
void disks_chunk_damaged_out(int chunk_slot, uint64_t day_damaged);
void disks_sync_copy(const struct cluster_copy *c);
int disks_chunk_attach_async(int disk_idx);
int disks_slot_is_out(int chunk_slot);
int disks_pick_rebuild_store(int lost_slot);
void disks_on_replacing_settled(uint64_t settled_mask);
void disks_set_reconcile_kick(void (*fn)(void));

/* R13 (r13-adopt): return pending adoption target OLD (0 if none) and optional pending chunk store count. */
uint32_t disks_adopt_info(uint32_t *chunk_stores_out);
/* R13 (r13-adopt): 1 if this server currently has store_id UP locally (lock-free-friendly helper for cnode_duty). */
int disks_has_up_store(const uint8_t store_id[16]);
/* R13 (r13-adopt): retry pending adoption against `c` without rerunning unrelated disks_sync_copy() work. */
void disks_adopt_retry(const struct cluster_copy *c);

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
    DISKS_MUT_FULLALL = 7,       /* fullall: mark EVERY volume on the node FULL when any disk is FULL */
    DISKS_MUT_FULLNORELEASE = 8, /* fullnorelease: do not early-release matched retired parts at 90% */
    DISKS_MUT_FULLMERGEON = 9,   /* fullmergeon: do not hold non-shrinking merges at 90% */
    DISKS_MUT_DEVOUTNOHOLD = 10, /* devoutnohold: ignore member hold when marking a device OUT */
    DISKS_MUT_REBUILDTOFAST = 11,/* rebuildtofast: allow capacity-store OUT rebuild onto a fast (NVMe/SSD) disk */
    DISKS_MUT_DAMAGENOOUT = 12,  /* damagenoout: ignore r12-chunk damage > scrub_out_count instead of marking OUT */
    DISKS_MUT_ADOPT_HEARD = 13,  /* adoptheard: accept --adopt / node replace adoption while OLD is still heard */
    DISKS_MUT_ADOPT_LOGDIR = 14, /* adoptlogdir: carry over v<N> log dir on adopted fast disk instead of .stale-<rid> */
    DISKS_MUT_ADOPT_NOREBIND = 15, /* adoptnorebind: skip atomic DISK rewrite to NEW during adoption */
};
int disks_mutant_parse(const char *name);
int disks_mutant(void);
int disks_mutant_env(void);
#endif

#endif /* TSDBD_DISKS_H */
