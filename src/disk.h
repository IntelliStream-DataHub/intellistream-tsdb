/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Disk class detection and durable DISK metadata (docs/design-cluster-data.md §15.1, E47, E58, D11).
 *
 * Each --disk directory holds a checksummed DISK file created once by `tsdb disk init PATH[:CLASS]`:
 *   - magic ("TSDBDSK1"), version (1), class (nvme/ssd/hdd), random 16-byte store_id, measured fdatasync p50
 *     latency in microseconds, cluster_uuid (16 bytes) and node_id (uint32_t) (both zero until bound), and an
 *     xxh64 checksum over the preceding bytes.
 *   - written atomically via DISK.tmp -> fsync -> rename -> dir fsync.
 *   - capacity disks (hdd, or fast disks when explicitly asked for chunks) also create <path>/chunks/ via
 *     chunk_store_init under the same store_id (when built with TSDB_EC) before DISK is renamed into place.
 *   - disk_bind writes (cluster_uuid, node_id) into <path>/DISK once atomically; re-binding to a different
 *     identity is refused. <path>/DISK is the authoritative cluster/node binding for --disk directories
 *     (D11/D12); <path>/chunks/META carries the matching store_id (unbound) and is not rewritten by disk_bind
 *     (r13-start enforces disk ownership via <path>/DISK).
 *
 * Class detection (disk_detect / disk_detect_dev) walks /sys (or a test sysroot):
 *   1. stat/fstat -> st_dev (MAJ:MIN); anonymous devices (major == 0: NFS, ZFS, tmpfs, overlay, btrfs) refused;
 *      <sysroot>/dev/block/MAJ:MIN resolved; a partition (has `partition` file) replaced by its parent disk.
 *   2. Any device with a non-empty `slaves/` directory (dm-crypt, LVM, md, dm-cache, bcache) is replaced
 *      recursively by all of its slaves.
 *   3. Leaf classification: loop/virtual devices refused; `nvme*` must have controller transport `pcie` in
 *      <sysroot>/class/nvme/<ctrl>/transport (under native NVMe multipath `nvme-subsys*`, every controller path
 *      must be `pcie`; tcp/rdma/other or zero controllers refused); otherwise `queue/rotational` 0 -> ssd,
 *      1 -> hdd.
 *   4. All leaves must agree on one class; mixed classes or missing local devices are refused with §15.1's
 *      diagnostic message.
 */
#ifndef TSDB_DISK_H
#define TSDB_DISK_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define DISK_META_FILE    "DISK"
#define DISK_META_VERSION 1u
#define DISK_META_BYTES   72u
#define DISK_SYNC_WARN_US 2000u /* 2 ms p50 threshold on fast classes (nvme, ssd) */
#define DISK_SYNC_SAMPLES 100u  /* 100 fdatasyncs of 4 KiB */

typedef enum disk_class {
    DISK_CLASS_NONE = 0,
    DISK_CLASS_NVME = 1,
    DISK_CLASS_SSD  = 2,
    DISK_CLASS_HDD  = 3,
} disk_class;

typedef struct disk_meta {
    uint32_t version;
    int disk_class;           /* DISK_CLASS_NVME, DISK_CLASS_SSD, DISK_CLASS_HDD */
    uint8_t store_id[16];     /* random 16-byte store id, never all-zero */
    uint64_t sync_p50_us;     /* measured p50 latency of 100 4 KiB fdatasyncs, in microseconds */
    uint8_t cluster_uuid[16]; /* all-zero until bound */
    uint32_t node_id;         /* 0 until bound */
    bool bound;               /* true iff bound to (cluster_uuid, node_id) */
} disk_meta;

const char *disk_class_name(int cls);
int disk_class_parse(const char *name);
bool disk_class_is_fast(int cls);

/* Pure encoding/decoding of the 72-byte DISK metadata image (for disk_load/init/bind and fuzz_disk). */
int disk_meta_encode(const disk_meta *d, uint8_t out[DISK_META_BYTES]);
int disk_meta_parse(const uint8_t *buf, size_t n, disk_meta *out);

/* §15.1 class detection for a path via <sysroot> (defaults to "/sys" when NULL).
 * On success returns 0 and sets *cls_out. On failure returns a negative TSDB_ERR_* and writes a diagnostic
 * sentence into `why` (if non-NULL and why_cap > 0). */
int disk_detect(const char *path, const char *sysroot, int *cls_out, char *why, size_t why_cap);

/* Same detection starting from an explicit (maj, min) device number (used by disk_detect and tests on fake-sys). */
int disk_detect_dev(unsigned maj, unsigned min, const char *label, const char *sysroot, int *cls_out, char *why,
                    size_t why_cap);

/* Initialise a disk directory.
 * - `class_or_0`: explicit DISK_CLASS_NVME/SSD/HDD, or DISK_CLASS_NONE (0) to auto-detect via `sysroot`.
 * - `want_chunks`: create <path>/chunks/ with the same store_id (also always done for hdd when TSDB_EC is built).
 * - Refuses non-empty directories, directories already holding DISK, and symlinked paths.
 * - Measures 100 fdatasyncs of 4 KiB and warns on stderr if p50 > 2000 us on a fast class.
 * - Writes DISK atomically (tmp -> fsync -> rename -> dir fsync). */
int disk_init2(const char *path, int class_or_0, const char *sysroot, bool want_chunks, disk_meta *out, char *why,
               size_t why_cap);
int disk_init(const char *path, int class_or_0, const char *sysroot, disk_meta *out);

/* Load and verify <path>/DISK. Returns TSDB_ERR_NOTFOUND if absent, TSDB_ERR_CORRUPT (logging `path` to stderr)
 * on bad format or checksum, TSDB_ERR_IO on I/O error. */
int disk_load(const char *path, disk_meta *out);

/* Bind <path>/DISK to (cluster_uuid, node_id) atomically. Write-once: idempotent if already bound to the same
 * identity (syncing DISK and its directory before returning 0 so a prior unsynced rename is made durable);
 * refuses re-binding to a different cluster_uuid or node_id (TSDB_ERR_INVALID). Returns TSDB_ERR_LOCKED if the
 * disk directory is locked by another process.
 * Note: <path>/DISK is the authoritative owner record for --disk directories (D11/D12); <path>/chunks/META
 * retains its matching store_id (unbound) and is not rewritten here (r13-start validates ownership via DISK). */
int disk_bind(const char *path, const uint8_t cluster_uuid[16], uint32_t node_id);

#ifdef TSDB_TEST_HOOKS
/* Planted mutants, which tests/test_disks.c requires to fail it. */
enum {
    DISK_MUT_NONE = 0,
    DISK_MUT_ROTATIONAL_FLIP, /* rotational read inverted: fake-sys SSD/HDD cases fail */
    DISK_MUT_BIND_TWICE,      /* second bind to a different identity accepted */
    DISK_MUT_NOSYNC,          /* directory fsync omitted after rename: crashfs case fails */
};
extern int disk_test_mutant;
extern unsigned disk_test_sync_samples; /* 0 = default (DISK_SYNC_SAMPLES = 100) */
extern uint64_t disk_test_sync_us;      /* 0 = real measurement; >0 = override p50_us for warning test */
#endif

#endif
