/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Disk class detection (/sys) and durable checksummed DISK metadata file (docs/design-cluster-data.md §15.1, D11).
 */
#include "internal.h"
#include "disk.h"
#include "chunk.h"

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#ifndef __APPLE__
#include <sys/sysmacros.h>
#endif
#include <time.h>
#include <unistd.h>

#define DISK_MAGIC "TSDBDSK1"
#define DISK_PROBE_TMP "DISK.probe.tmp"
#define DISK_META_TMP "DISK.tmp"
#define DISK_DETECT_MAX_DEPTH 16
#define DISK_DETECT_MAX_LEAVES 256

#ifdef TSDB_TEST_HOOKS
int disk_test_mutant = DISK_MUT_NONE;
unsigned disk_test_sync_samples = 0;
uint64_t disk_test_sync_us = 0;
#endif

/* ---------------------------------------------------------------- helpers */

static void set_why(char *why, size_t cap, const char *fmt, ...)
{
    if (!why || cap == 0)
        return;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(why, cap, fmt, ap);
    va_end(ap);
}

static uint32_t get_le32(const uint8_t *p)
{
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static void put_le32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)v;
    p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16);
    p[3] = (uint8_t)(v >> 24);
}

static uint64_t get_le64(const uint8_t *p)
{
    return (uint64_t)get_le32(p) | ((uint64_t)get_le32(p + 4) << 32);
}

static void put_le64(uint8_t *p, uint64_t v)
{
    put_le32(p, (uint32_t)v);
    put_le32(p + 4, (uint32_t)(v >> 32));
}

static bool is_zero16(const uint8_t b[16])
{
    uint8_t acc = 0;
    for (int i = 0; i < 16; i++)
        acc |= b[i];
    return acc == 0;
}

static int wr_full(int fd, const void *buf, size_t n)
{
    const uint8_t *p = buf;
    size_t put = 0;
    while (put < n) {
        ssize_t w = write(fd, p + put, n - put);
        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0)
            return TSDB_ERR_IO;
        put += (size_t)w;
    }
    return 0;
}

static int pwrite_full(int fd, const void *buf, size_t n, off_t off)
{
    const uint8_t *p = buf;
    size_t put = 0;
    while (put < n) {
        ssize_t w = pwrite(fd, p + put, n - put, off + (off_t)put);
        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0)
            return TSDB_ERR_IO;
        put += (size_t)w;
    }
    return 0;
}

static int lock_dir_nb(int dirfd)
{
    while (flock(dirfd, LOCK_EX | LOCK_NB) != 0) {
        if (errno == EINTR)
            continue;
        if (errno == EWOULDBLOCK || errno == EAGAIN)
            return TSDB_ERR_LOCKED;
        return TSDB_ERR_IO;
    }
    return 0;
}

static int sync_dir_fd(int dirfd)
{
#ifdef TSDB_TEST_HOOKS
    if (disk_test_mutant == DISK_MUT_NOSYNC)
        return 0;
#endif
    return tsdb_fsync(dirfd) == 0 ? 0 : TSDB_ERR_IO;
}

static int sync_parent_dir(const char *path)
{
#ifdef TSDB_TEST_HOOKS
    if (disk_test_mutant == DISK_MUT_NOSYNC)
        return 0;
#endif
    char *p = strdup(path);
    if (!p)
        return TSDB_ERR_NOMEM;
    char *slash = strrchr(p, '/');
    const char *dir = ".";
    if (slash && slash != p) {
        *slash = '\0';
        dir = p;
    } else if (slash) {
        dir = "/";
    }
    int fd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    int rc = fd >= 0 && tsdb_fsync(fd) == 0 ? 0 : TSDB_ERR_IO;
    if (fd >= 0)
        close(fd);
    free(p);
    return rc;
}

/* Strip trailing slashes (except "/") so O_NOFOLLOW catches symlinked target directories too. */
static int strip_slashes(const char *path, char **copy_out, const char **p_out)
{
    if (!path || !*path)
        return TSDB_ERR_INVALID;
    size_t plen = strlen(path);
    while (plen > 1 && path[plen - 1] == '/')
        plen--;
    if (path[plen] == '\0') {
        *copy_out = NULL;
        *p_out = path;
        return 0;
    }
    char *c = malloc(plen + 1);
    if (!c)
        return TSDB_ERR_NOMEM;
    memcpy(c, path, plen);
    c[plen] = '\0';
    *copy_out = c;
    *p_out = c;
    return 0;
}

/* ---------------------------------------------------------------- classes */

const char *disk_class_name(int cls)
{
    switch (cls) {
    case DISK_CLASS_NVME:
        return "nvme";
    case DISK_CLASS_SSD:
        return "ssd";
    case DISK_CLASS_HDD:
        return "hdd";
    default:
        return "unknown";
    }
}

int disk_class_parse(const char *name)
{
    if (!name)
        return DISK_CLASS_NONE;
    if (strcmp(name, "nvme") == 0)
        return DISK_CLASS_NVME;
    if (strcmp(name, "ssd") == 0)
        return DISK_CLASS_SSD;
    if (strcmp(name, "hdd") == 0)
        return DISK_CLASS_HDD;
    return DISK_CLASS_NONE;
}

bool disk_class_is_fast(int cls)
{
    return cls == DISK_CLASS_NVME || cls == DISK_CLASS_SSD;
}

/* ---------------------------------------------------------------- DISK encoding */

int disk_meta_encode(const disk_meta *d, uint8_t out[DISK_META_BYTES])
{
    if (!d || !out)
        return TSDB_ERR_INVALID;
    uint32_t ver = d->version ? d->version : DISK_META_VERSION;
    if (ver != DISK_META_VERSION)
        return TSDB_ERR_INVALID;
    if (d->disk_class != DISK_CLASS_NVME && d->disk_class != DISK_CLASS_SSD && d->disk_class != DISK_CLASS_HDD)
        return TSDB_ERR_INVALID;
    if (is_zero16(d->store_id))
        return TSDB_ERR_INVALID;
    bool cu_zero = is_zero16(d->cluster_uuid);
    if (d->bound) {
        if (cu_zero || d->node_id == 0)
            return TSDB_ERR_INVALID;
    } else {
        if (!cu_zero || d->node_id != 0)
            return TSDB_ERR_INVALID;
    }

    memset(out, 0, DISK_META_BYTES);
    memcpy(out, DISK_MAGIC, 8);
    put_le32(out + 8, ver);
    put_le32(out + 12, (uint32_t)d->disk_class);
    memcpy(out + 16, d->store_id, 16);
    put_le64(out + 32, d->sync_p50_us);
    memcpy(out + 40, d->cluster_uuid, 16);
    put_le32(out + 56, d->node_id);
    put_le32(out + 60, 0u); /* reserved */
    put_le64(out + 64, xxh64(out, 64, 0));
    return 0;
}

int disk_meta_parse(const uint8_t *buf, size_t n, disk_meta *out)
{
    if (!buf || n != DISK_META_BYTES)
        return TSDB_ERR_CORRUPT;
    if (memcmp(buf, DISK_MAGIC, 8) != 0)
        return TSDB_ERR_CORRUPT;
    uint64_t want = xxh64(buf, 64, 0);
    if (get_le64(buf + 64) != want)
        return TSDB_ERR_CORRUPT;

    uint32_t ver = get_le32(buf + 8);
    uint32_t cls = get_le32(buf + 12);
    uint64_t p50 = get_le64(buf + 32);
    uint32_t node = get_le32(buf + 56);
    uint32_t res = get_le32(buf + 60);

    if (ver != DISK_META_VERSION || res != 0)
        return TSDB_ERR_CORRUPT;
    if (cls != DISK_CLASS_NVME && cls != DISK_CLASS_SSD && cls != DISK_CLASS_HDD)
        return TSDB_ERR_CORRUPT;
    if (is_zero16(buf + 16))
        return TSDB_ERR_CORRUPT;
    bool cu_zero = is_zero16(buf + 40);
    if ((node == 0) != cu_zero)
        return TSDB_ERR_CORRUPT;

    if (out) {
        memset(out, 0, sizeof *out);
        out->version = ver;
        out->disk_class = (int)cls;
        memcpy(out->store_id, buf + 16, 16);
        out->sync_p50_us = p50;
        memcpy(out->cluster_uuid, buf + 40, 16);
        out->node_id = node;
        out->bound = (node != 0);
    }
    return 0;
}

/* ---------------------------------------------------------------- /sys class detection (§15.1) */

static int read_sys_line(const char *path, char *buf, size_t cap)
{
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return -1;
    ssize_t r = read(fd, buf, cap - 1);
    close(fd);
    if (r <= 0)
        return -1;
    buf[r] = '\0';
    while (r > 0 && (buf[r - 1] == '\n' || buf[r - 1] == '\r' || buf[r - 1] == ' ' || buf[r - 1] == '\t'))
        buf[--r] = '\0';
    return r > 0 ? 0 : -1;
}

static const char *base_name(const char *path)
{
    const char *s = strrchr(path, '/');
    return s ? s + 1 : path;
}

static bool has_prefix(const char *s, const char *pfx)
{
    return strncmp(s, pfx, strlen(pfx)) == 0;
}

/* True iff `s` is strictly "nvme" followed by one or more ASCII digits (e.g. "nvme0", "nvme12"),
 * not a namespace ("nvme0n1") or subsystem ("nvme-subsys0"). */
static bool is_nvme_ctrl_name(const char *s)
{
    if (!has_prefix(s, "nvme") || !isdigit((unsigned char)s[4]))
        return false;
    for (size_t i = 4; s[i]; i++) {
        if (!isdigit((unsigned char)s[i]))
            return false;
    }
    return true;
}

typedef struct detect_ctx {
    const char *sysroot;
    const char *label;
    int cls;        /* DISK_CLASS_NONE until first leaf */
    int nleaves;
    bool mixed;
    bool no_local;
    char detail[128];
} detect_ctx;

static int check_nvme_ctrl_pcie(detect_ctx *ctx, const char *ctrl)
{
    char p[PATH_MAX + 64], val[64];
    if (snprintf(p, sizeof p, "%s/class/nvme/%s/transport", ctx->sysroot, ctrl) >= (int)sizeof p ||
        read_sys_line(p, val, sizeof val) != 0) {
        ctx->no_local = true;
        return -1;
    }
    if (strcmp(val, "pcie") != 0) {
        ctx->no_local = true;
        snprintf(ctx->detail, sizeof ctx->detail, "NVMe transport %s", val);
        return -1;
    }
    return 0;
}

/* Native NVMe multipath (nvme_core.multipath=Y, default on RHEL/AlmaLinux 9+):
 * <devdir>/device resolves to .../nvme-subsysX. Inspect all controllers backing this namespace:
 *   1. Prefer <devdir>/multipath/ (kernel 6.x+: per-namespace controller paths nvmeXcYnZ whose
 *      `device` symlink points to the real controller nvmeY).
 *   2. Otherwise inspect controller entries (`nvme<digits>`) directly inside `subsys_dir` (kernel 4.15/5.x+).
 * Require at least one controller and require EVERY controller's transport to be `pcie` (fail closed). */
static int check_nvme_multipath_pcie(detect_ctx *ctx, const char *devdir, const char *subsys_dir)
{
    char mpath[PATH_MAX + 32];
    int nctrl = 0;

    if (snprintf(mpath, sizeof mpath, "%s/multipath", devdir) < (int)sizeof mpath) {
        DIR *d = opendir(mpath);
        if (d) {
            for (;;) {
                errno = 0;
                struct dirent *de = readdir(d);
                if (!de) {
                    if (errno != 0) {
                        closedir(d);
                        ctx->no_local = true;
                        return -1;
                    }
                    break;
                }
                if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
                    continue;
                char cdev[PATH_MAX + 320], resolved[PATH_MAX];
                const char *ctrl = NULL;
                if (snprintf(cdev, sizeof cdev, "%s/%s/device", mpath, de->d_name) < (int)sizeof cdev &&
                    realpath(cdev, resolved)) {
                    const char *b = base_name(resolved);
                    if (is_nvme_ctrl_name(b))
                        ctrl = b;
                }
                if (!ctrl) {
                    if (snprintf(cdev, sizeof cdev, "%s/%s", mpath, de->d_name) < (int)sizeof cdev &&
                        realpath(cdev, resolved)) {
                        const char *b = base_name(resolved);
                        if (is_nvme_ctrl_name(b))
                            ctrl = b;
                    }
                }
                if (!ctrl || check_nvme_ctrl_pcie(ctx, ctrl) != 0) {
                    closedir(d);
                    ctx->no_local = true;
                    return -1;
                }
                nctrl++;
            }
            closedir(d);
            if (nctrl > 0)
                return 0;
        } else if (errno != ENOENT && errno != ENOTDIR) {
            ctx->no_local = true;
            return -1;
        }
    }

    DIR *sd = opendir(subsys_dir);
    if (!sd) {
        ctx->no_local = true;
        return -1;
    }
    for (;;) {
        errno = 0;
        struct dirent *de = readdir(sd);
        if (!de) {
            if (errno != 0) {
                closedir(sd);
                ctx->no_local = true;
                return -1;
            }
            break;
        }
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;
        if (!is_nvme_ctrl_name(de->d_name))
            continue;
        if (check_nvme_ctrl_pcie(ctx, de->d_name) != 0) {
            closedir(sd);
            return -1;
        }
        nctrl++;
    }
    closedir(sd);
    if (nctrl == 0) {
        ctx->no_local = true;
        return -1;
    }
    return 0;
}

static int check_nvme_leaf_pcie(detect_ctx *ctx, const char *devdir, const char *name)
{
    char linkp[PATH_MAX + 32], resolved[PATH_MAX];
    if (snprintf(linkp, sizeof linkp, "%s/device", devdir) >= (int)sizeof linkp) {
        ctx->no_local = true;
        return -1;
    }
    struct stat lst;
    if (lstat(linkp, &lst) == 0) {
        if (!realpath(linkp, resolved)) {
            ctx->no_local = true;
            return -1;
        }
        const char *b = base_name(resolved);
        if (is_nvme_ctrl_name(b))
            return check_nvme_ctrl_pcie(ctx, b);
        if (has_prefix(b, "nvme-subsys"))
            return check_nvme_multipath_pcie(ctx, devdir, resolved);
        /* Unexpected device target: fail closed. */
        ctx->no_local = true;
        return -1;
    }
    if (errno != ENOENT) {
        ctx->no_local = true;
        return -1;
    }
    /* No <devdir>/device symlink (minimal/legacy sysfs): derive nvme<digits> from namespace name. */
    if (!has_prefix(name, "nvme") || !isdigit((unsigned char)name[4])) {
        ctx->no_local = true;
        return -1;
    }
    size_t i = 4;
    while (isdigit((unsigned char)name[i]))
        i++;
    char ctrl[64];
    if (i >= sizeof ctrl) {
        ctx->no_local = true;
        return -1;
    }
    memcpy(ctrl, name, i);
    ctrl[i] = '\0';
    return check_nvme_ctrl_pcie(ctx, ctrl);
}

static int classify_leaf(detect_ctx *ctx, const char *devdir)
{
    const char *name = base_name(devdir);
    char p[PATH_MAX + 64], val[64];

    /* Refuse loop and virtual/non-local block devices. */
    if (has_prefix(name, "loop") || has_prefix(name, "ram") || has_prefix(name, "zram") ||
        has_prefix(name, "nbd") || has_prefix(name, "nullb") || has_prefix(name, "dm-") ||
        (strncmp(name, "md", 2) == 0 && isdigit((unsigned char)name[2]))) {
        ctx->no_local = true;
        return -1;
    }
    if (snprintf(p, sizeof p, "%s/loop", devdir) < (int)sizeof p && access(p, F_OK) == 0) {
        ctx->no_local = true;
        return -1;
    }

    int leaf_cls = DISK_CLASS_NONE;
    if (has_prefix(name, "nvme")) {
        if (check_nvme_leaf_pcie(ctx, devdir, name) != 0)
            return -1;
        leaf_cls = DISK_CLASS_NVME;
    } else {
        if (snprintf(p, sizeof p, "%s/queue/rotational", devdir) >= (int)sizeof p ||
            read_sys_line(p, val, sizeof val) != 0) {
            ctx->no_local = true;
            return -1;
        }
        if (strcmp(val, "0") != 0 && strcmp(val, "1") != 0) {
            ctx->no_local = true;
            return -1;
        }
        bool rot = (val[0] == '1');
#ifdef TSDB_TEST_HOOKS
        if (disk_test_mutant == DISK_MUT_ROTATIONAL_FLIP)
            rot = !rot;
#endif
        leaf_cls = rot ? DISK_CLASS_HDD : DISK_CLASS_SSD;
    }

    ctx->nleaves++;
    if (ctx->cls == DISK_CLASS_NONE) {
        ctx->cls = leaf_cls;
    } else if (ctx->cls != leaf_cls) {
        ctx->mixed = true;
        return -1;
    }
    return 0;
}

static int walk_block_dev(detect_ctx *ctx, const char *devpath, int depth)
{
    if (depth > DISK_DETECT_MAX_DEPTH || ctx->nleaves >= DISK_DETECT_MAX_LEAVES) {
        ctx->no_local = true;
        return -1;
    }
    char resolved[PATH_MAX];
    if (!realpath(devpath, resolved)) {
        ctx->no_local = true;
        return -1;
    }

    /* Step 1: a partition is replaced by its parent disk. */
    char partp[PATH_MAX + 32];
    if (snprintf(partp, sizeof partp, "%s/partition", resolved) < (int)sizeof partp && access(partp, F_OK) == 0) {
        char *slash = strrchr(resolved, '/');
        if (!slash || slash == resolved) {
            ctx->no_local = true;
            return -1;
        }
        *slash = '\0';
    }

    /* Step 2: a device with non-empty slaves/ (dm-crypt, LVM, md, dm-cache, bcache) is replaced recursively
     * by all of its slaves. */
    char slavesp[PATH_MAX + 32];
    int nslaves = 0;
    if (snprintf(slavesp, sizeof slavesp, "%s/slaves", resolved) < (int)sizeof slavesp) {
        DIR *d = opendir(slavesp);
        if (d) {
            for (;;) {
                errno = 0;
                struct dirent *de = readdir(d);
                if (!de) {
                    if (errno != 0) {
                        closedir(d);
                        ctx->no_local = true;
                        return -1;
                    }
                    break;
                }
                if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
                    continue;
                nslaves++;
                char child[PATH_MAX + 288];
                if (snprintf(child, sizeof child, "%s/%s", slavesp, de->d_name) >= (int)sizeof child) {
                    closedir(d);
                    ctx->no_local = true;
                    return -1;
                }
                if (walk_block_dev(ctx, child, depth + 1) != 0) {
                    closedir(d);
                    return -1;
                }
            }
            closedir(d);
        } else if (errno != ENOENT && errno != ENOTDIR) {
            ctx->no_local = true;
            return -1;
        }
    }
    if (nslaves > 0)
        return 0;

    return classify_leaf(ctx, resolved);
}

int disk_detect_dev(unsigned maj, unsigned min, const char *label, const char *sysroot, int *cls_out, char *why,
                    size_t why_cap)
{
    const char *lbl = label && *label ? label : "(disk)";
    const char *root = sysroot && *sysroot ? sysroot : "/sys";

    /* Step 1: anonymous device numbers (major 0: NFS, ZFS, tmpfs, overlay, btrfs) have no local block device. */
    if (maj == 0) {
        set_why(why, why_cap, "%s: cannot tell its class (no local block device); add :nvme, :ssd or :hdd", lbl);
        return TSDB_ERR_INVALID;
    }

    char devlink[PATH_MAX + 64];
    if (snprintf(devlink, sizeof devlink, "%s/dev/block/%u:%u", root, maj, min) >= (int)sizeof devlink) {
        set_why(why, why_cap, "%s: cannot tell its class (no local block device); add :nvme, :ssd or :hdd", lbl);
        return TSDB_ERR_INVALID;
    }

    detect_ctx ctx;
    memset(&ctx, 0, sizeof ctx);
    ctx.sysroot = root;
    ctx.label = lbl;

    if (walk_block_dev(&ctx, devlink, 0) != 0 || ctx.nleaves == 0 || ctx.cls == DISK_CLASS_NONE) {
        if (ctx.mixed) {
            set_why(why, why_cap, "%s: cannot tell its class (devices of different kinds); add :nvme, :ssd or :hdd",
                    lbl);
        } else if (ctx.detail[0]) {
            set_why(why, why_cap,
                    "%s: cannot tell its class (no local block device: %s); add :nvme, :ssd or :hdd", lbl, ctx.detail);
        } else {
            set_why(why, why_cap, "%s: cannot tell its class (no local block device); add :nvme, :ssd or :hdd", lbl);
        }
        return TSDB_ERR_INVALID;
    }

    if (cls_out)
        *cls_out = ctx.cls;
    return 0;
}

int disk_detect(const char *path, const char *sysroot, int *cls_out, char *why, size_t why_cap)
{
    if (!path || !*path) {
        set_why(why, why_cap, "empty disk path");
        return TSDB_ERR_INVALID;
    }
    struct stat st;
    if (stat(path, &st) != 0) {
        int e = errno;
        set_why(why, why_cap, "%s: %s", path, strerror(e));
        return e == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    }
    unsigned maj = (unsigned)major(st.st_dev);
    unsigned min = (unsigned)minor(st.st_dev);
    return disk_detect_dev(maj, min, path, sysroot, cls_out, why, why_cap);
}

/* ---------------------------------------------------------------- fdatasync benchmark */

static int cmp_u64(const void *a, const void *b)
{
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}

static int measure_sync_p50(int dirfd, uint64_t *p50_us)
{
    unsigned n = DISK_SYNC_SAMPLES;
#ifdef TSDB_TEST_HOOKS
    if (disk_test_sync_us > 0) {
        *p50_us = disk_test_sync_us;
        return 0;
    }
    if (disk_test_sync_samples > 0 && disk_test_sync_samples <= DISK_SYNC_SAMPLES)
        n = disk_test_sync_samples;
#endif
    unlinkat(dirfd, DISK_PROBE_TMP, 0);
    int fd = openat(dirfd, DISK_PROBE_TMP, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0644);
    if (fd < 0)
        return TSDB_ERR_IO;

    uint8_t buf[4096];
    memset(buf, 0xa5, sizeof buf);
    uint64_t samples[DISK_SYNC_SAMPLES];
    int rc = 0;

    for (unsigned i = 0; i < n; i++) {
        put_le64(buf, (uint64_t)i + 1u);
        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);
        if (pwrite_full(fd, buf, sizeof buf, 0) != 0 || tsdb_fdatasync(fd) != 0) {
            rc = TSDB_ERR_IO;
            break;
        }
        clock_gettime(CLOCK_MONOTONIC, &t1);
        int64_t ds = (int64_t)t1.tv_sec - (int64_t)t0.tv_sec;
        int64_t dn = (int64_t)t1.tv_nsec - (int64_t)t0.tv_nsec;
        int64_t ns = ds * 1000000000LL + dn;
        samples[i] = ns > 0 ? (uint64_t)(ns / 1000LL) : 0u;
    }

    close(fd);
    unlinkat(dirfd, DISK_PROBE_TMP, 0);
    if (rc != 0)
        return rc;

    qsort(samples, n, sizeof samples[0], cmp_u64);
    *p50_us = samples[n / 2];
    return 0;
}

/* ---------------------------------------------------------------- load / init / bind */

static int disk_load_fd(int dirfd, const char *path, disk_meta *out)
{
    int fd = openat(dirfd, DISK_META_FILE, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;

    uint8_t buf[DISK_META_BYTES + 1];
    size_t got = 0;
    int rc = 0;
    for (;;) {
        ssize_t r = read(fd, buf + got, sizeof buf - got);
        if (r < 0 && errno == EINTR)
            continue;
        if (r < 0) {
            rc = TSDB_ERR_IO;
            break;
        }
        if (r == 0)
            break;
        got += (size_t)r;
        if (got == sizeof buf) {
            rc = TSDB_ERR_CORRUPT;
            break;
        }
    }
    close(fd);
    if (rc == 0)
        rc = disk_meta_parse(buf, got, out);
    if (rc == TSDB_ERR_CORRUPT)
        fprintf(stderr, "tsdb: %s/%s: corrupt disk metadata (bad format or checksum)\n", path, DISK_META_FILE);
    return rc;
}

static int disk_write_atomic(int dirfd, const disk_meta *d, bool *renamed_out)
{
    if (renamed_out)
        *renamed_out = false;
    uint8_t raw[DISK_META_BYTES];
    int rc = disk_meta_encode(d, raw);
    if (rc != 0)
        return rc;

    unlinkat(dirfd, DISK_META_TMP, 0);
    int fd = openat(dirfd, DISK_META_TMP, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0644);
    if (fd < 0)
        return TSDB_ERR_IO;
    if (wr_full(fd, raw, sizeof raw) != 0 || tsdb_fsync(fd) != 0) {
        close(fd);
        unlinkat(dirfd, DISK_META_TMP, 0);
        return TSDB_ERR_IO;
    }
    if (close(fd) != 0) {
        unlinkat(dirfd, DISK_META_TMP, 0);
        return TSDB_ERR_IO;
    }
    if (renameat(dirfd, DISK_META_TMP, dirfd, DISK_META_FILE) != 0) {
        unlinkat(dirfd, DISK_META_TMP, 0);
        return TSDB_ERR_IO;
    }
    if (renamed_out)
        *renamed_out = true;
    return sync_dir_fd(dirfd);
}

/* Check whether <cfd>/META is an unbound chunk_store_init leftover (no cluster=/node= owner).
 * Sets *unbound_out = true only if empty or unbound tsdb chunk store META; returns TSDB_ERR_IO on read error. */
static int chunk_meta_is_unbound_leftover(int cfd, bool *unbound_out, bool *bound_out)
{
    *unbound_out = false;
    *bound_out = false;
    int fd = openat(cfd, "META", O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? 0 : TSDB_ERR_IO;
    char buf[256];
    size_t got = 0;
    for (;;) {
        ssize_t r = read(fd, buf + got, sizeof buf - 1 - got);
        if (r < 0 && errno == EINTR)
            continue;
        if (r < 0) {
            close(fd);
            return TSDB_ERR_IO;
        }
        if (r == 0)
            break;
        got += (size_t)r;
        if (got == sizeof buf - 1) {
            close(fd);
            return 0; /* too large to be an unbound META leftover */
        }
    }
    close(fd);
    buf[got] = '\0';
    if (got == 0) {
        *unbound_out = true;
        return 0;
    }
    if (strncmp(buf, "tsdb chunk store ", 17) != 0)
        return 0;
    if (strstr(buf, "\ncluster=") != NULL || strstr(buf, "\nnode=") != NULL) {
        *bound_out = true;
        return 0;
    }
    *unbound_out = true;
    return 0;
}

/* Inspect <dirfd>/chunks: sets *ok_out = true iff it is a real directory containing exclusively
 * unbound chunk_store_init leftovers (unbound META and/or META.tmp regular files, no volume subdirs
 * or bound chunk stores). Returns TSDB_ERR_IO on filesystem/read error. */
static int chunks_is_init_leftover(int dirfd, bool *ok_out, bool *bound_out)
{
    *ok_out = false;
    *bound_out = false;
    int cfd = openat(dirfd, "chunks", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (cfd < 0)
        return (errno == ENOENT || errno == ENOTDIR || errno == ELOOP) ? 0 : TSDB_ERR_IO;
    DIR *d = fdopendir(cfd);
    if (!d) {
        close(cfd);
        return TSDB_ERR_IO;
    }
    bool ok = true;
    int rc = 0;
    for (;;) {
        errno = 0;
        struct dirent *de = readdir(d);
        if (!de) {
            if (errno != 0)
                rc = TSDB_ERR_IO;
            break;
        }
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;
        if (strcmp(de->d_name, "META") != 0 && strcmp(de->d_name, "META.tmp") != 0) {
            ok = false;
            break;
        }
        struct stat st;
        if (fstatat(cfd, de->d_name, &st, AT_SYMLINK_NOFOLLOW) != 0) {
            if (errno != ENOENT)
                rc = TSDB_ERR_IO;
            ok = false;
            break;
        }
        if (!S_ISREG(st.st_mode)) {
            ok = false;
            break;
        }
        if (strcmp(de->d_name, "META") == 0) {
            bool unbound = false, bound = false;
            rc = chunk_meta_is_unbound_leftover(cfd, &unbound, &bound);
            if (rc != 0 || !unbound) {
                *bound_out = bound;
                ok = false;
                break;
            }
        }
    }
    closedir(d);
    if (rc == 0)
        *ok_out = ok;
    return rc;
}

static void remove_leftover_chunks(int dirfd)
{
    int cfd = openat(dirfd, "chunks", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (cfd >= 0) {
        unlinkat(cfd, "META", 0);
        unlinkat(cfd, "META.tmp", 0);
        close(cfd);
    }
    unlinkat(dirfd, "chunks", AT_REMOVEDIR);
}

/* Refuse directories that already hold DISK or any non-leftover files.
 * If no DISK exists and the only entries are leftovers from an interrupted disk_init (DISK.probe.tmp,
 * DISK.tmp, and an empty/unbound-META-only chunks/ dir), clean them up so a retry succeeds. */
static int prepare_empty_dir(int dirfd, const char *path, char *why, size_t why_cap)
{
    if (faccessat(dirfd, DISK_META_FILE, F_OK, AT_SYMLINK_NOFOLLOW) == 0) {
        set_why(why, why_cap, "%s: already initialised (%s exists)", path, DISK_META_FILE);
        return TSDB_ERR_INVALID;
    }
    int dupfd = dup(dirfd);
    if (dupfd < 0)
        return TSDB_ERR_IO;
    DIR *d = fdopendir(dupfd);
    if (!d) {
        close(dupfd);
        return TSDB_ERR_IO;
    }
    rewinddir(d);

    bool has_probe = false, has_tmp = false, has_chunks = false;
    for (;;) {
        errno = 0;
        struct dirent *de = readdir(d);
        if (!de) {
            if (errno != 0) {
                int err = errno;
                closedir(d);
                set_why(why, why_cap, "%s: readdir failed: %s", path, strerror(err));
                return TSDB_ERR_IO;
            }
            break;
        }
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;
        if (strcmp(de->d_name, DISK_PROBE_TMP) == 0 || strcmp(de->d_name, DISK_META_TMP) == 0) {
            struct stat st;
            if (fstatat(dirfd, de->d_name, &st, AT_SYMLINK_NOFOLLOW) != 0) {
                int err = errno;
                closedir(d);
                set_why(why, why_cap, "%s: stat %s failed: %s", path, de->d_name, strerror(err));
                return TSDB_ERR_IO;
            }
            if (S_ISREG(st.st_mode)) {
                if (strcmp(de->d_name, DISK_PROBE_TMP) == 0)
                    has_probe = true;
                else
                    has_tmp = true;
                continue;
            }
        } else if (strcmp(de->d_name, "chunks") == 0) {
            bool ok = false, bound = false;
            int crc = chunks_is_init_leftover(dirfd, &ok, &bound);
            if (crc != 0) {
                closedir(d);
                set_why(why, why_cap, "%s: failed inspecting chunks directory", path);
                return crc;
            }
            if (bound) {
                closedir(d);
                set_why(why, why_cap, "%s: directory contains a bound chunk store (chunks/META)", path);
                return TSDB_ERR_INVALID;
            }
            if (ok) {
                has_chunks = true;
                continue;
            }
        }
        set_why(why, why_cap, "%s: directory is not empty (contains %s)", path, de->d_name);
        closedir(d);
        return TSDB_ERR_INVALID;
    }
    closedir(d);

    if (has_probe)
        unlinkat(dirfd, DISK_PROBE_TMP, 0);
    if (has_tmp)
        unlinkat(dirfd, DISK_META_TMP, 0);
    if (has_chunks)
        remove_leftover_chunks(dirfd);
    return 0;
}

int disk_init2(const char *path, int class_or_0, const char *sysroot, bool want_chunks, disk_meta *out, char *why,
               size_t why_cap)
{
    if (why && why_cap > 0)
        why[0] = '\0';
    char *pcopy = NULL;
    const char *p = NULL;
    int rc = strip_slashes(path, &pcopy, &p);
    if (rc != 0) {
        set_why(why, why_cap, "empty disk path");
        return rc;
    }

    if (class_or_0 != DISK_CLASS_NONE && class_or_0 != DISK_CLASS_NVME && class_or_0 != DISK_CLASS_SSD &&
        class_or_0 != DISK_CLASS_HDD) {
        set_why(why, why_cap, "%s: invalid disk class %d", p, class_or_0);
        free(pcopy);
        return TSDB_ERR_INVALID;
    }

#ifndef TSDB_EC
    if (want_chunks) {
        set_why(why, why_cap,
                "%s: cannot initialise chunks store: this binary was not built with the erasure codec", p);
        free(pcopy);
        return TSDB_ERR_INVALID;
    }
#endif

    bool made = (mkdir(p, 0755) == 0);
    if (!made && errno != EEXIST) {
        int e = errno;
        set_why(why, why_cap, "%s: cannot create directory: %s", p, strerror(e));
        free(pcopy);
        return TSDB_ERR_IO;
    }

    int dirfd = open(p, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (dirfd < 0) {
        int e = errno;
        set_why(why, why_cap, "%s: cannot open directory: %s", p, strerror(e));
        if (made)
            rmdir(p);
        free(pcopy);
        return e == ELOOP || e == ENOTDIR ? TSDB_ERR_INVALID : TSDB_ERR_IO;
    }

    bool locked = false;
    bool prepared = false;
    bool made_chunks = false;
    bool renamed = false;

    rc = lock_dir_nb(dirfd);
    if (rc != 0) {
        if (rc == TSDB_ERR_LOCKED)
            set_why(why, why_cap, "%s: disk directory is busy (locked by another process)", p);
        else
            set_why(why, why_cap, "%s: failed locking directory", p);
        goto done;
    }
    locked = true;

    rc = prepare_empty_dir(dirfd, p, why, why_cap);
    if (rc != 0)
        goto done;
    prepared = true;

    int cls = class_or_0;
    if (cls == DISK_CLASS_NONE) {
        struct stat st;
        if (fstat(dirfd, &st) != 0) {
            int e = errno;
            set_why(why, why_cap, "%s: fstat failed: %s", p, strerror(e));
            rc = TSDB_ERR_IO;
            goto done;
        }
        rc = disk_detect_dev((unsigned)major(st.st_dev), (unsigned)minor(st.st_dev), p, sysroot, &cls, why, why_cap);
        if (rc != 0)
            goto done;
    }

    uint64_t p50 = 0;
    rc = measure_sync_p50(dirfd, &p50);
    if (rc != 0) {
        set_why(why, why_cap, "%s: failed measuring fdatasync latency", p);
        goto done;
    }

    if (disk_class_is_fast(cls) && p50 > DISK_SYNC_WARN_US) {
        fprintf(stderr,
                "tsdb: warning: %s (%s): fdatasync p50 is %" PRIu64 " us (above %u us): "
                "disk may lack power-loss protection or class may be wrong\n",
                p, disk_class_name(cls), p50, DISK_SYNC_WARN_US);
    }

    disk_meta d;
    memset(&d, 0, sizeof d);
    d.version = DISK_META_VERSION;
    d.disk_class = cls;
    d.sync_p50_us = p50;
    d.bound = false;
    do {
        if (chunk_random(d.store_id, sizeof d.store_id) != 0) {
            set_why(why, why_cap, "%s: failed generating random store id", p);
            rc = TSDB_ERR_IO;
            goto done;
        }
    } while (is_zero16(d.store_id));

#ifdef TSDB_EC
    if (cls == DISK_CLASS_HDD || want_chunks) {
        char cpath[PATH_MAX + 32];
        if (snprintf(cpath, sizeof cpath, "%s/chunks", p) >= (int)sizeof cpath) {
            set_why(why, why_cap, "%s: path too long for chunks directory", p);
            rc = TSDB_ERR_INVALID;
            goto done;
        }
        rc = chunk_store_init(cpath, d.store_id, NULL);
        if (rc != 0) {
            remove_leftover_chunks(dirfd);
            set_why(why, why_cap, "%s: failed initialising chunk store at %s", p, cpath);
            goto done;
        }
        made_chunks = true;
    }
#else
    if (cls == DISK_CLASS_HDD)
        fprintf(stderr, "tsdb: note: %s (hdd): skipping chunks/ store (binary built without the erasure codec)\n", p);
#endif

    rc = disk_write_atomic(dirfd, &d, &renamed);
    if (rc != 0) {
        if (renamed)
            set_why(why, why_cap, "%s: %s was renamed but syncing directory failed (may not be durable)", p,
                    DISK_META_FILE);
        else
            set_why(why, why_cap, "%s: failed writing %s", p, DISK_META_FILE);
        goto done;
    }
    if (made) {
        rc = sync_parent_dir(p);
        if (rc != 0) {
            set_why(why, why_cap, "%s: %s was renamed but syncing parent directory failed (may not be durable)", p,
                    DISK_META_FILE);
            goto done;
        }
    }
    if (out)
        *out = d;

done:
    if (rc != 0 && locked && prepared && !renamed) {
        if (made_chunks)
            remove_leftover_chunks(dirfd);
        unlinkat(dirfd, DISK_PROBE_TMP, 0);
        unlinkat(dirfd, DISK_META_TMP, 0);
    }
    close(dirfd);
    if (rc != 0 && locked && !renamed && made)
        rmdir(p);
    free(pcopy);
    return rc;
}

int disk_init(const char *path, int class_or_0, const char *sysroot, disk_meta *out)
{
    char why[512];
    int rc = disk_init2(path, class_or_0, sysroot, false, out, why, sizeof why);
    if (rc != 0 && why[0])
        fprintf(stderr, "tsdb: %s\n", why);
    return rc;
}

int disk_load(const char *path, disk_meta *out)
{
    char *pcopy = NULL;
    const char *p = NULL;
    int rc = strip_slashes(path, &pcopy, &p);
    if (rc != 0)
        return rc;

    int dirfd = open(p, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dirfd < 0) {
        int e = errno;
        free(pcopy);
        if (e == ENOENT)
            return TSDB_ERR_NOTFOUND;
        return e == ENOTDIR ? TSDB_ERR_INVALID : TSDB_ERR_IO;
    }
    rc = disk_load_fd(dirfd, p, out);
    close(dirfd);
    free(pcopy);
    return rc;
}

int disk_bind(const char *path, const uint8_t cluster_uuid[16], uint32_t node_id)
{
    if (!cluster_uuid || is_zero16(cluster_uuid) || node_id == 0)
        return TSDB_ERR_INVALID;

    char *pcopy = NULL;
    const char *p = NULL;
    int rc = strip_slashes(path, &pcopy, &p);
    if (rc != 0)
        return rc;

    int dirfd = open(p, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (dirfd < 0) {
        int e = errno;
        free(pcopy);
        if (e == ENOENT)
            return TSDB_ERR_NOTFOUND;
        return e == ELOOP || e == ENOTDIR ? TSDB_ERR_INVALID : TSDB_ERR_IO;
    }

    rc = lock_dir_nb(dirfd);
    if (rc != 0) {
        if (rc == TSDB_ERR_LOCKED)
            fprintf(stderr, "tsdb: %s: disk directory is busy (locked by another process)\n", p);
        goto done;
    }

    disk_meta cur;
    rc = disk_load_fd(dirfd, p, &cur);
    if (rc != 0)
        goto done;

    if (cur.bound) {
        if (memcmp(cur.cluster_uuid, cluster_uuid, 16) == 0 && cur.node_id == node_id) {
            /* Ensure an earlier rename that crashed or failed before dir fsync is durable
             * before acknowledging this idempotent bind. */
            int dfd = openat(dirfd, DISK_META_FILE, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
            if (dfd < 0) {
                rc = TSDB_ERR_IO;
                goto done;
            }
            int s1 = tsdb_fsync(dfd);
            close(dfd);
            rc = s1 == 0 ? sync_dir_fd(dirfd) : TSDB_ERR_IO;
            goto done;
        }
#ifdef TSDB_TEST_HOOKS
        if (disk_test_mutant != DISK_MUT_BIND_TWICE)
#endif
        {
            char old_hex[33], new_hex[33];
            chunk_oid_hex(cur.cluster_uuid, old_hex);
            chunk_oid_hex(cluster_uuid, new_hex);
            fprintf(stderr,
                    "tsdb: %s/%s: already bound to cluster %s node %" PRIu32
                    ", refusing re-bind to cluster %s node %" PRIu32 "\n",
                    p, DISK_META_FILE, old_hex, cur.node_id, new_hex, node_id);
            rc = TSDB_ERR_INVALID;
            goto done;
        }
    }

    memcpy(cur.cluster_uuid, cluster_uuid, 16);
    cur.node_id = node_id;
    cur.bound = true;
    rc = disk_write_atomic(dirfd, &cur, NULL);

done:
    close(dirfd);
    free(pcopy);
    return rc;
}

int disk_rebind_locked(int dirfd, const char *path, const uint8_t cluster_uuid[16], uint32_t old_node_id,
                       uint32_t new_node_id)
{
    if (dirfd < 0 || !path || !cluster_uuid || is_zero16(cluster_uuid) || old_node_id == 0 || new_node_id == 0 ||
        old_node_id == new_node_id)
        return TSDB_ERR_INVALID;

    disk_meta cur;
    int rc = disk_load_fd(dirfd, path, &cur);
    if (rc != 0)
        return rc;

    if (!cur.bound) {
        fprintf(stderr, "tsdb: %s/%s: unbound disk cannot be adopted (expected node %" PRIu32 ")\n", path,
                DISK_META_FILE, old_node_id);
        return TSDB_ERR_INVALID;
    }
    if (memcmp(cur.cluster_uuid, cluster_uuid, 16) != 0) {
        char have_hex[33], want_hex[33];
        chunk_oid_hex(cur.cluster_uuid, have_hex);
        chunk_oid_hex(cluster_uuid, want_hex);
        fprintf(stderr,
                "tsdb: %s/%s: belongs to cluster %s node %" PRIu32 ", refusing adoption into cluster %s\n", path,
                DISK_META_FILE, have_hex, cur.node_id, want_hex);
        return TSDB_ERR_INVALID;
    }
    if (cur.node_id == new_node_id) {
        /* Idempotent retry after a crash between rename and dir fsync: make durable before acknowledging. */
        int dfd = openat(dirfd, DISK_META_FILE, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
        if (dfd < 0)
            return TSDB_ERR_IO;
        int s1 = tsdb_fsync(dfd);
        close(dfd);
        return s1 == 0 ? sync_dir_fd(dirfd) : TSDB_ERR_IO;
    }
    if (cur.node_id != old_node_id) {
        fprintf(stderr, "tsdb: %s/%s: bound to node %" PRIu32 ", expected node %" PRIu32 " for adoption\n", path,
                DISK_META_FILE, cur.node_id, old_node_id);
        return TSDB_ERR_INVALID;
    }

    cur.node_id = new_node_id;
    return disk_write_atomic(dirfd, &cur, NULL);
}

int disk_rebind(const char *path, const uint8_t cluster_uuid[16], uint32_t old_node_id, uint32_t new_node_id)
{
    if (!cluster_uuid || is_zero16(cluster_uuid) || old_node_id == 0 || new_node_id == 0 || old_node_id == new_node_id)
        return TSDB_ERR_INVALID;

    char *pcopy = NULL;
    const char *p = NULL;
    int rc = strip_slashes(path, &pcopy, &p);
    if (rc != 0)
        return rc;

    int dirfd = open(p, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (dirfd < 0) {
        int e = errno;
        free(pcopy);
        if (e == ENOENT)
            return TSDB_ERR_NOTFOUND;
        return e == ELOOP || e == ENOTDIR ? TSDB_ERR_INVALID : TSDB_ERR_IO;
    }

    rc = lock_dir_nb(dirfd);
    if (rc != 0) {
        if (rc == TSDB_ERR_LOCKED)
            fprintf(stderr, "tsdb: %s: disk directory is busy (locked by another process)\n", p);
        goto done;
    }

    rc = disk_rebind_locked(dirfd, p, cluster_uuid, old_node_id, new_node_id);

done:
    close(dirfd);
    free(pcopy);
    return rc;
}
