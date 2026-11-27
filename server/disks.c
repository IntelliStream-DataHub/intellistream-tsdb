/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Cluster server disk table, start checks (E58), binding, chunk store selection, and HBS_DISKS (R13, r13-start).
 */
#define _GNU_SOURCE
#include "disks.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

#include "chunksrv.h"
#include "cluster.h"
#include "creport.h"
#include "../client/tsdb_crypto.h"
#include "../src/chunk.h"
#include "../src/chunk_priv.h"
#include "../src/chunkmove.h"
#include "../src/compat.h"
#include "../src/cvol.h"
#include "../src/internal.h"

#define HBS_DISKS_VER      3u
#define HBS_DISKS_VER_V2   2u
#define HBS_DISKS_VER_V1   1u
#define HBS_DISKS_HDR      8u
#define HBS_DISKS_ENTRY_V1 36u
#define HBS_DISKS_ENTRY_V2 52u
#define HBS_DISKS_ENTRY    96u
#define DISK_PEAK_HOURS    168u /* 7 days of hourly young-tier peaks (§15.4) */

struct known_disk {
    uint8_t store_id[16];
    uint8_t disk_class;
    uint8_t uses;
    uint64_t size_bytes;
    uint64_t sync_us;
    char path[PATH_MAX];
};

struct disk_entry {
    char path[PATH_MAX];
    char chunks_path[PATH_MAX];
    int stated_class;
    int from_dir; /* 1: only named by --dir (skips /sys detection, logs --dir warning) */
    int is_dir;   /* 1: engine directory (--dir, even if also named by --disk; missing DISK always refuses) */
    int present; /* 1: DISK loaded; 0: known store missing (reported DOWN) */
    disk_meta meta;
    int effective_class;
    uint8_t uses;
    uint8_t state;
    uint64_t size_bytes;
    uint64_t sync_us;
    dev_t dev;
    int has_chunks;
    int class_mismatch;
    int detected_class;
    int lock_fd;          /* exclusive flock on bound disk directory, -1 when not held */
    int dom_idx;          /* tsdb_health domain index, -1 when not probed */
    int chunk_slot;       /* 0-based index in chunksrv's chunk_backend, -1 if none */
    int probe_down;       /* 1 while health domain is DOWN or RECOVERING */
    int probe_recovering; /* 1 while health domain is RECOVERING */
    int io_down;          /* 1 after chunk store EIO/ENOSPC or probe DOWN (sticky until restart/replace) */
    /* R13 r13-lmove: local chunk drain state. */
    int draining;                /* 1 while this chunk store is being drained locally (no new puts) */
    int drain_no_space;          /* 1 while drain stopped because no target store has room */
    int drain_no_space_logged;   /* rate-limit log message while stuck with no room */
    int drain_started_logged;    /* log once when chunks are actually seen to drain on this store */
    uint64_t drain_bytes_left;   /* remaining chunk bytes on this draining store (HBS_PROGRESS) */
    uint64_t drain_chunks_moved; /* chunks moved off this store since start */
    uint64_t drain_bytes_moved;  /* bytes moved off this store since start */
    uint64_t drain_done_logged;  /* drain_chunks_moved value when completion was last logged */
    /* R13 r13-fill: per-disk bytes-by-use accounting, 7-day peak, and 75/80/90% fill hysteresis (E30, E50, E53). */
    int usage_have;
    uint64_t usage_raw_size;     /* raw statvfs total bytes */
    uint64_t usage_fs_used;      /* raw statvfs (f_blocks - f_bavail) * unit */
    uint64_t usage_raw_young;    /* raw pure young data: active WAL, young parts, metadata */
    uint64_t usage_raw_retained; /* raw retained WAL segments (E29) */
    uint64_t usage_raw_retired;  /* raw retired settle parts (settle_keep_s, E26) */
    uint64_t usage_raw_local;    /* raw local copies (ec_keep_local_h) */
    uint64_t usage_raw_index;    /* raw .ec index files */
    uint64_t usage_raw_chunks;   /* raw chunk store bytes */
    uint64_t usage_size;         /* effective size (statvfs or virtual size override) */
    uint64_t usage_young_data;   /* effective pure young data (incl. any phantom/override delta) */
    uint64_t usage_eff_young;    /* effective total young-tier bytes (for marks & HBS_DISKS young_bytes) */
    uint64_t usage_eff_chunks;   /* effective chunk bytes (for marks & HBS_DISKS chunk_bytes) */
    uint64_t usage_young_room;   /* room available to young tier (size or size - eff_chunks on shared disks) */
    uint64_t usage_peak_7d;      /* 7-day peak of young-tier bytes (shared disk young reserve) */
    uint64_t peak_hour_val[DISK_PEAK_HOURS];
    uint64_t peak_hour_stamp[DISK_PEAK_HOURS];
    uint8_t usage_young_pct;
    uint8_t usage_chunk_pct;
    uint8_t usage_fill_flags; /* HBS_DISK_F_* */
    int usage_full;           /* 1 while in FULL hysteresis state (>= young_high_pct until < young_high_pct - 2) */
    int usage_chunk_full;     /* 1 while past cluster chunk fill mark or shared young reserve */
    int usage_chunk_rb_full;  /* 1 while chunk store >= 95% (rebuild hard cap) */
};

static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_probe_cv = PTHREAD_COND_INITIALIZER; /* waited on by disk_probe_stop until probe_users == 0 */
static atomic_int g_disks_started; /* fast-path guard so heartbeat/metrics never block on disks_start() I/O */
static atomic_int g_disks_usage_active;
static atomic_int g_disks_any_full;

static struct {
    int prepared;
    int started;
    int bound_all;
    char state_dir[PATH_MAX];
    uint8_t role;
    int allow_shared;
    uint32_t class_mismatches;
    int ndisks;
    struct disk_entry disks[DISKS_MAX];
    int nknown;
    struct known_disk known[DISKS_MAX];
    int have_primary_id;
    uint8_t primary_id[16];
    /* R13 (r13-fault): per-disk health probe monitor (src/health.c). */
    tsdb_health *probe_h;
    int probe_users;    /* threads currently inside a tsdb_health_* call on probe_h */
    int probe_stopping; /* 1 once disk_probe_stop begins draining probe_users */
    disk_probe_cb probe_cb;
    void *probe_ctx;
    int probe_down_override; /* 1 if TSDBD_TEST_HEALTH explicitly set down= */
    int probe_down_ms;       /* current down_ms configured on probe_h */
    /* R13 r13-lmove: background local chunk drain driver. */
    int drain_started;
    int drain_thr_running;
    int drain_stop;
    int drain_kick;
    int drain_no_space_any;
    pthread_t drain_thr;
    pthread_cond_t drain_cv;
    /* R13 r13-fill: per-disk refcounted detachable usage sampler workers (10 s default, fast under test). */
    tsdb *usage_db;
    struct disk_usage_hooks usage_hooks;
    int usage_started;
    struct disk_usage_worker *usage_workers[DISKS_MAX];
    int nusage_workers;
#ifdef TSDB_TEST_HOOKS
    int mutant;
#endif
} G;

/* ---------------------------------------------------------------- helpers */

static void put_le32(uint8_t *p, uint32_t v)
{
    for (int i = 0; i < 4; i++)
        p[i] = (uint8_t)(v >> (8 * i));
}

static void put_le64(uint8_t *p, uint64_t v)
{
    for (int i = 0; i < 8; i++)
        p[i] = (uint8_t)(v >> (8 * i));
}

static uint32_t get_le32(const uint8_t *p)
{
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static uint64_t get_le64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 7; i >= 0; i--)
        v = (v << 8) | p[i];
    return v;
}

static void clean_path(const char *in, char *out, size_t cap)
{
    size_t len = strlen(in);
    while (len > 1 && in[len - 1] == '/')
        len--;
    if (len >= cap)
        len = cap - 1;
    memcpy(out, in, len);
    out[len] = '\0';
}

static int parse_hex16(const char *s, uint8_t out[16])
{
    if (!s || strlen(s) != 32)
        return -1;
    for (int i = 0; i < 16; i++) {
        unsigned v = 0;
        for (int k = 0; k < 2; k++) {
            char c = s[2 * i + k];
            unsigned d;
            if (c >= '0' && c <= '9')
                d = (unsigned)(c - '0');
            else if (c >= 'a' && c <= 'f')
                d = (unsigned)(c - 'a' + 10);
            else if (c >= 'A' && c <= 'F')
                d = (unsigned)(c - 'A' + 10);
            else
                return -1;
            v = (v << 4) | d;
        }
        out[i] = (uint8_t)v;
    }
    return 0;
}

static const char *uses_str(uint8_t uses)
{
    if ((uses & META_DISK_USE_LOG) && (uses & META_DISK_USE_CHUNK))
        return "log,chunk";
    if (uses & META_DISK_USE_LOG)
        return "log";
    if (uses & META_DISK_USE_CHUNK)
        return "chunk";
    return "none";
}

static int same_path(const char *a, const char *b)
{
    char ca[PATH_MAX], cb[PATH_MAX];
    clean_path(a, ca, sizeof ca);
    clean_path(b, cb, sizeof cb);
    if (strcmp(ca, cb) == 0)
        return 1;
    char ra[PATH_MAX], rb[PATH_MAX];
    if (realpath(ca, ra) && realpath(cb, rb))
        return strcmp(ra, rb) == 0;
    return 0;
}

static uint64_t fs_size_bytes(const char *path)
{
    struct statvfs sv;
    if (statvfs(path, &sv) != 0)
        return 1;
    uint64_t unit = sv.f_frsize ? (uint64_t)sv.f_frsize : (uint64_t)sv.f_bsize;
    uint64_t sz = (uint64_t)sv.f_blocks * unit;
    return sz > 0 ? sz : 1;
}

static int stat_state_dev(const char *state_dir, dev_t *dev_out)
{
    struct stat st;
    if (stat(state_dir, &st) == 0) {
        *dev_out = st.st_dev;
        return 0;
    }
    char buf[PATH_MAX];
    clean_path(state_dir, buf, sizeof buf);
    for (;;) {
        char *sl = strrchr(buf, '/');
        if (!sl) {
            if (stat(".", &st) == 0) {
                *dev_out = st.st_dev;
                return 0;
            }
            return -1;
        }
        if (sl == buf) {
            if (stat("/", &st) == 0) {
                *dev_out = st.st_dev;
                return 0;
            }
            return -1;
        }
        *sl = '\0';
        if (stat(buf, &st) == 0) {
            *dev_out = st.st_dev;
            return 0;
        }
    }
}

static void close_all_locks_locked(void)
{
    for (int i = 0; i < G.ndisks; i++) {
        if (G.disks[i].lock_fd >= 0) {
            close(G.disks[i].lock_fd);
            G.disks[i].lock_fd = -1;
        }
    }
}

static int try_lock_disk(const char *path, int *fd_out, char *err, size_t errlen)
{
    int fd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) {
        snprintf(err, errlen, "disk %s: cannot open directory: %s", path, strerror(errno));
        return -1;
    }
    while (flock(fd, LOCK_EX | LOCK_NB) != 0) {
        if (errno == EINTR)
            continue;
        int e = errno;
        close(fd);
        if (e == EWOULDBLOCK || e == EAGAIN)
            snprintf(err, errlen, "disk %s is open elsewhere (another tsdbd owns it)", path);
        else
            snprintf(err, errlen, "disk %s: flock failed: %s", path, strerror(e));
        return -1;
    }
    *fd_out = fd;
    return 0;
}

/* ---------------------------------------------------------------- <state-dir>/DISKS local registry */

static void known_load(const char *state_dir)
{
    G.nknown = 0;
    G.have_primary_id = 0;
    memset(G.primary_id, 0, sizeof G.primary_id);
    char p[PATH_MAX + 32];
    if (snprintf(p, sizeof p, "%s/DISKS", state_dir) >= (int)sizeof p)
        return;
    FILE *f = fopen(p, "r");
    if (!f)
        return;
    char line[PATH_MAX + 160];
    if (!fgets(line, sizeof line, f) || strncmp(line, "tsdb disks 1", 12) != 0) {
        fclose(f);
        return;
    }
    while (fgets(line, sizeof line, f)) {
        size_t l = strlen(line);
        while (l > 0 && (line[l - 1] == '\n' || line[l - 1] == '\r'))
            line[--l] = '\0';
        if (strncmp(line, "primary ", 8) == 0) {
            uint8_t pid[16];
            if (parse_hex16(line + 8, pid) == 0) {
                memcpy(G.primary_id, pid, 16);
                G.have_primary_id = 1;
            }
            continue;
        }
        if (G.nknown >= DISKS_MAX)
            continue;
        char hex[40];
        unsigned cls = 0, uses = 0;
        unsigned long long sz = 0, sus = 0;
        int off = 0;
        if (sscanf(line, "%32s %u %u %llu %llu %n", hex, &cls, &uses, &sz, &sus, &off) != 5 || off <= 0 ||
            !line[off])
            continue;
        uint8_t sid[16];
        if (parse_hex16(hex, sid) != 0)
            continue;
        if (cls < META_DISK_NVME || cls > META_DISK_CLASS_LAST || uses == 0 || (uses & ~META_DISK_USE_ALL))
            continue;
        struct known_disk *k = &G.known[G.nknown++];
        memcpy(k->store_id, sid, 16);
        k->disk_class = (uint8_t)cls;
        k->uses = (uint8_t)uses;
        k->size_bytes = sz > 0 ? (uint64_t)sz : 1;
        k->sync_us = (uint64_t)sus;
        clean_path(line + off, k->path, sizeof k->path);
    }
    fclose(f);
}

static const struct known_disk *known_find_path(const char *path)
{
    for (int i = 0; i < G.nknown; i++) {
        if (same_path(G.known[i].path, path))
            return &G.known[i];
    }
    return NULL;
}

static void known_save_snapshot(const char *state_dir, const struct known_disk *rows, int nrows, int have_primary,
                                const uint8_t primary_id[16])
{
    if (!state_dir || !*state_dir)
        return;
    char tmp[PATH_MAX + 48], dst[PATH_MAX + 32];
    if (snprintf(tmp, sizeof tmp, "%s/DISKS.tmp", state_dir) >= (int)sizeof tmp ||
        snprintf(dst, sizeof dst, "%s/DISKS", state_dir) >= (int)sizeof dst)
        return;
    unlink(tmp);
    int fd = open(tmp, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600);
    if (fd < 0) {
        fprintf(stderr, "tsdbd: disks: warning: cannot create %s: %s\n", tmp, strerror(errno));
        return;
    }
    FILE *f = fdopen(fd, "w");
    if (!f) {
        fprintf(stderr, "tsdbd: disks: warning: fdopen %s failed: %s\n", tmp, strerror(errno));
        close(fd);
        unlink(tmp);
        return;
    }
    int werr = (fprintf(f, "tsdb disks 1\n") < 0);
    if (!werr && have_primary) {
        char phex[33];
        chunk_oid_hex(primary_id, phex);
        if (fprintf(f, "primary %s\n", phex) < 0)
            werr = 1;
    }
    for (int i = 0; i < nrows && !werr; i++) {
        char hex[33];
        chunk_oid_hex(rows[i].store_id, hex);
        if (fprintf(f, "%s %u %u %" PRIu64 " %" PRIu64 " %s\n", hex, (unsigned)rows[i].disk_class,
                    (unsigned)rows[i].uses, rows[i].size_bytes > 0 ? rows[i].size_bytes : (uint64_t)1,
                    rows[i].sync_us, rows[i].path) < 0)
            werr = 1;
    }
    int ok = (!werr && fflush(f) == 0 && tsdb_fsync(fileno(f)) == 0);
    int saved_errno = ok ? 0 : errno;
    if (fclose(f) != 0) {
        if (ok)
            saved_errno = errno;
        ok = 0;
    }
    if (!ok || rename(tmp, dst) != 0) {
        if (ok)
            saved_errno = errno;
        fprintf(stderr, "tsdbd: disks: warning: cannot persist %s: %s\n", dst,
                saved_errno ? strerror(saved_errno) : "write failed");
        unlink(tmp);
        return;
    }
    int dfd = open(state_dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd >= 0) {
        if (tsdb_fsync(dfd) != 0)
            fprintf(stderr, "tsdbd: disks: warning: fsync dir %s failed: %s\n", state_dir, strerror(errno));
        close(dfd);
    }
}

static void persist_known_from_state(void)
{
    struct known_disk rows[DISKS_MAX];
    int nrows = 0;
    char sdir[PATH_MAX];
    int have_primary = 0;
    uint8_t primary_id[16] = {0};
    pthread_mutex_lock(&g_mu);
    snprintf(sdir, sizeof sdir, "%s", G.state_dir);
    have_primary = G.have_primary_id;
    if (have_primary)
        memcpy(primary_id, G.primary_id, 16);
    for (int i = 0; i < G.ndisks && nrows < DISKS_MAX; i++) {
        const struct disk_entry *d = &G.disks[i];
        if (!d->path[0])
            continue;
        if (d->present && !d->meta.bound)
            continue;
        struct known_disk *r = &rows[nrows++];
        memcpy(r->store_id, d->meta.store_id, 16);
        r->disk_class = (uint8_t)d->effective_class;
        r->uses = d->uses ? d->uses : META_DISK_USE_LOG;
        r->size_bytes = d->size_bytes > 0 ? d->size_bytes : 1;
        r->sync_us = d->sync_us;
        snprintf(r->path, sizeof r->path, "%s", d->path);
    }
    /* Preserve prior known paths not in current command line so an omitted path stays known. */
    for (int i = 0; i < G.nknown && nrows < DISKS_MAX; i++) {
        int dup = 0;
        for (int j = 0; j < nrows; j++) {
            if (same_path(rows[j].path, G.known[i].path) ||
                memcmp(rows[j].store_id, G.known[i].store_id, 16) == 0) {
                dup = 1;
                break;
            }
        }
        if (!dup)
            rows[nrows++] = G.known[i];
    }
    G.nknown = nrows;
    if (nrows > 0)
        memcpy(G.known, rows, (size_t)nrows * sizeof rows[0]);
    pthread_mutex_unlock(&g_mu);
    known_save_snapshot(sdir, rows, nrows, have_primary, primary_id);
}

/* ---------------------------------------------------------------- parsing & mutants */

#ifdef TSDB_TEST_HOOKS
int disks_mutant_parse(const char *name)
{
    if (!name || !*name)
        return DISKS_MUT_NONE;
    if (strcmp(name, "nodevcheck") == 0)
        return DISKS_MUT_NODEVCHECK;
    if (strcmp(name, "classsilent") == 0)
        return DISKS_MUT_CLASSSILENT;
    if (strcmp(name, "missingasnew") == 0)
        return DISKS_MUT_MISSINGASNEW;
    if (strcmp(name, "stallexit") == 0)
        return DISKS_MUT_STALLEXIT;
    if (strcmp(name, "stallnoresign") == 0)
        return DISKS_MUT_STALLNORESIGN;
    if (strcmp(name, "storeeioexit") == 0)
        return DISKS_MUT_STOREEIOEXIT;
    if (strcmp(name, "fullall") == 0)
        return DISKS_MUT_FULLALL;
    if (strcmp(name, "fullnorelease") == 0)
        return DISKS_MUT_FULLNORELEASE;
    if (strcmp(name, "fullmergeon") == 0)
        return DISKS_MUT_FULLMERGEON;
    return -1;
}

int disks_mutant(void)
{
    pthread_mutex_lock(&g_mu);
    int m = G.mutant;
    pthread_mutex_unlock(&g_mu);
    return m;
}
#endif

int disks_parse_spec(const char *arg, struct disk_spec *out, char *err, size_t errlen)
{
    if (!arg || !*arg || !out) {
        if (err && errlen)
            snprintf(err, errlen, "empty --disk specification");
        return -1;
    }
    memset(out, 0, sizeof *out);
    const char *slash = strrchr(arg, '/');
    const char *colon = strrchr(slash ? slash + 1 : arg, ':');
    if (colon) {
        size_t plen = (size_t)(colon - arg);
        if (plen == 0 || plen >= sizeof out->path) {
            if (err && errlen)
                snprintf(err, errlen, "--disk '%s': invalid path", arg);
            return -1;
        }
        int cls = disk_class_parse(colon + 1);
        if (cls == DISK_CLASS_NONE) {
            if (err && errlen)
                snprintf(err, errlen, "--disk '%s': unknown class '%s' (expected nvme, ssd or hdd)", arg, colon + 1);
            return -1;
        }
        char raw[PATH_MAX];
        memcpy(raw, arg, plen);
        raw[plen] = '\0';
        clean_path(raw, out->path, sizeof out->path);
        out->stated_class = cls;
    } else {
        if (strlen(arg) >= sizeof out->path) {
            if (err && errlen)
                snprintf(err, errlen, "--disk: path too long");
            return -1;
        }
        clean_path(arg, out->path, sizeof out->path);
        out->stated_class = DISK_CLASS_NONE;
    }
    return 0;
}

int disks_parse_role(const char *arg, uint8_t *role_out)
{
    if (!arg || !role_out)
        return -1;
    if (strcmp(arg, "both") == 0) {
        *role_out = META_ROLE_BOTH;
        return 0;
    }
    if (strcmp(arg, "compute") == 0) {
        *role_out = META_ROLE_COMPUTE;
        return 0;
    }
    if (strcmp(arg, "storage") == 0) {
        *role_out = META_ROLE_STORAGE;
        return 0;
    }
    return -1;
}

/* ---------------------------------------------------------------- HBS_DISKS heartbeat section & metrics */

static uint64_t sat_add64(uint64_t a, uint64_t b)
{
    return a > UINT64_MAX - b ? UINT64_MAX : a + b;
}

#ifdef TSDB_TEST_HOOKS
/* Per-disk usage override parsed from TSDB_DISK_USAGE_FILE=<path>.
 * Supported line formats (<target> is "nvme", "ssd", "hdd", "all", or 32-char hex store_id):
 *   <target> <young_pct> <chunk_pct>             (r13-place: override young/chunk bytes to % of size)
 *   <target> bytes <young_bytes> <chunk_bytes>   (r13-place: override young/chunk bytes directly)
 *   <target> phantom <add_young> <add_chunks>    (r13-fill: add phantom bytes on top of sampled bytes)
 *   <target> size <virtual_size_bytes>           (r13-fill: virtual disk capacity cap for real-fill tests) */
struct disk_usage_ov {
    int have_pct;
    int have_bytes;
    int have_phantom;
    int have_size;
    uint64_t young_pct;
    uint64_t chunk_pct;
    uint64_t young_bytes;
    uint64_t chunk_bytes;
    uint64_t phantom_young;
    uint64_t phantom_chunks;
    uint64_t virt_size;
};

static int load_disk_usage_file(const uint8_t (*sids)[16], const int *classes, uint32_t n, struct disk_usage_ov *ovs)
{
    memset(ovs, 0, (size_t)n * sizeof *ovs);
    const char *path = getenv("TSDB_DISK_USAGE_FILE");
    if (!path || !*path || n == 0)
        return 0;
    FILE *f = fopen(path, "r");
    if (!f)
        return 0;
    int matched_any = 0;
    char line[256];
    while (fgets(line, sizeof line, f)) {
        char target[64], mode[32];
        unsigned long long a = 0, b = 0;
        int kind = 0; /* 1=pct, 2=bytes, 3=phantom, 4=size */
        if (sscanf(line, "%63s %31s %llu %llu", target, mode, &a, &b) == 4) {
            if (strcmp(mode, "bytes") == 0)
                kind = 2;
            else if (strcmp(mode, "phantom") == 0)
                kind = 3;
        } else if (sscanf(line, "%63s %31s %llu", target, mode, &a) == 3 && strcmp(mode, "size") == 0) {
            kind = 4;
        } else if (sscanf(line, "%63s %llu %llu", target, &a, &b) == 3) {
            kind = 1;
        }
        if (kind == 0)
            continue;
        uint8_t sid[16];
        int is_sid = (parse_hex16(target, sid) == 0);
        int cls = strcmp(target, "all") == 0 ? -1 : disk_class_parse(target);
        if (!is_sid && cls == DISK_CLASS_NONE)
            continue;
        for (uint32_t i = 0; i < n; i++) {
            int match = is_sid ? (memcmp(sids[i], sid, 16) == 0) : (cls == -1 || classes[i] == cls);
            if (!match)
                continue;
            matched_any = 1;
            if (kind == 1) {
                ovs[i].have_pct = 1;
                ovs[i].have_bytes = 0;
                ovs[i].young_pct = a > 100 ? 100 : (uint64_t)a;
                ovs[i].chunk_pct = b > 100 ? 100 : (uint64_t)b;
            } else if (kind == 2) {
                ovs[i].have_bytes = 1;
                ovs[i].have_pct = 0;
                ovs[i].young_bytes = (uint64_t)a;
                ovs[i].chunk_bytes = (uint64_t)b;
            } else if (kind == 3) {
                ovs[i].have_phantom = 1;
                ovs[i].phantom_young = sat_add64(ovs[i].phantom_young, (uint64_t)a);
                ovs[i].phantom_chunks = sat_add64(ovs[i].phantom_chunks, (uint64_t)b);
            } else if (kind == 4 && a > 0) {
                ovs[i].have_size = 1;
                ovs[i].virt_size = (uint64_t)a;
            }
        }
    }
    fclose(f);
    return matched_any;
}

static void apply_disk_usage_file(struct hbs_disk_row *rows, uint32_t nrows, const uint64_t *raw_size,
                                  const uint64_t *raw_young, const uint64_t *raw_chunks)
{
    if (nrows == 0)
        return;
    uint8_t sids[DISKS_MAX][16];
    int classes[DISKS_MAX];
    struct disk_usage_ov ovs[DISKS_MAX];
    for (uint32_t i = 0; i < nrows; i++) {
        memcpy(sids[i], rows[i].store_id, 16);
        classes[i] = (int)rows[i].disk_class;
    }
    if (!load_disk_usage_file((const uint8_t (*)[16])sids, classes, nrows, ovs))
        return;
    for (uint32_t i = 0; i < nrows; i++) {
        const struct disk_usage_ov *o = &ovs[i];
        if (!o->have_pct && !o->have_bytes && !o->have_phantom && !o->have_size)
            continue;
        struct hbs_disk_row *r = &rows[i];
        if (o->have_size && o->virt_size > 0)
            r->size_bytes = o->virt_size;
        else if (raw_size[i] > 0)
            r->size_bytes = raw_size[i];
        r->have_usage = 1;
        r->fill_flags |= HBS_DISK_F_USAGE;
        if (o->have_bytes) {
            r->young_bytes = o->young_bytes;
            r->chunk_bytes = o->chunk_bytes;
        } else if (o->have_pct) {
            r->young_bytes = o->young_pct == 0
                                 ? 0
                                 : (uint64_t)(((__uint128_t)r->size_bytes * o->young_pct + 99u) / 100u);
            if (r->young_bytes > r->size_bytes)
                r->young_bytes = r->size_bytes;
            r->chunk_bytes = o->chunk_pct == 0
                                 ? 0
                                 : (uint64_t)(((__uint128_t)r->size_bytes * o->chunk_pct + 99u) / 100u);
            if (r->chunk_bytes > r->size_bytes)
                r->chunk_bytes = r->size_bytes;
        } else if (o->have_phantom) {
            r->young_bytes = sat_add64(raw_young[i], o->phantom_young);
            r->chunk_bytes = sat_add64(raw_chunks[i], o->phantom_chunks);
        }
        uint64_t room = ((r->uses & META_DISK_USE_LOG) && (r->uses & META_DISK_USE_CHUNK))
                            ? (r->size_bytes > r->chunk_bytes ? r->size_bytes - r->chunk_bytes : 0)
                            : r->size_bytes;
        uint64_t yp = (r->uses & META_DISK_USE_LOG)
                          ? (room > 0 ? (uint64_t)(((__uint128_t)r->young_bytes * 100u) / room) : 100u)
                          : 0u;
        uint64_t cp = r->size_bytes > 0 ? (uint64_t)(((__uint128_t)r->chunk_bytes * 100u) / r->size_bytes) : 100u;
        r->young_fill_pct = (uint8_t)(yp > 100u ? 100u : yp);
        r->chunk_fill_pct = (uint8_t)(cp > 100u ? 100u : cp);
        uint32_t wp = 75u, mp = 80u;
        const struct cluster_copy *cpv = cluster_copy_get();
        if (cpv) {
            if (cpv->young_warn_pct >= 1 && cpv->young_warn_pct <= 99)
                wp = cpv->young_warn_pct;
            if (cpv->young_move_pct >= 1 && cpv->young_move_pct <= 99)
                mp = cpv->young_move_pct;
            cluster_copy_put(cpv);
        }
        r->fill_flags &= (uint8_t)~(HBS_DISK_F_WARN | HBS_DISK_F_MOVE);
        if ((r->uses & META_DISK_USE_LOG) && r->young_fill_pct >= wp)
            r->fill_flags |= HBS_DISK_F_WARN;
        if ((r->uses & META_DISK_USE_LOG) && r->young_fill_pct >= mp)
            r->fill_flags |= HBS_DISK_F_MOVE;
    }
}
#endif

static ssize_t disks_sec_encode(uint8_t *buf, size_t cap)
{
    if (!atomic_load_explicit(&g_disks_started, memory_order_acquire))
        return 0;
    struct hbs_disk_row rows[DISKS_MAX];
#ifdef TSDB_TEST_HOOKS
    uint64_t raw_size[DISKS_MAX] = {0}, raw_young[DISKS_MAX] = {0}, raw_chunks[DISKS_MAX] = {0};
#endif
    uint32_t nrows = 0, mismatches = 0;
    uint8_t role = META_ROLE_BOTH;
    int allow_shared = 0, drain_no_space = 0, started = 0;

    pthread_mutex_lock(&g_mu);
    started = G.started;
    role = G.role;
    allow_shared = G.allow_shared;
    drain_no_space = G.drain_no_space_any;
    mismatches = G.class_mismatches;
    for (int i = 0; started && i < G.ndisks && nrows < DISKS_MAX; i++) {
        const struct disk_entry *d = &G.disks[i];
        /* Never emit unbound disks: disk_bind must succeed before STORE_ADD can be proposed (§15.1). */
        if (d->present && !d->meta.bound)
            continue;
        uint32_t idx = nrows++;
        struct hbs_disk_row *r = &rows[idx];
        memset(r, 0, sizeof *r);
        memcpy(r->store_id, d->meta.store_id, 16);
        r->disk_class = (uint8_t)d->effective_class;
        r->uses = d->uses ? d->uses : META_DISK_USE_LOG;
        r->state = d->state;
        r->size_bytes = d->usage_have && d->usage_size > 0 ? d->usage_size : (d->size_bytes > 0 ? d->size_bytes : 1);
        r->sync_us = d->sync_us;
        if (d->usage_have) {
            r->have_usage = 1;
            r->fill_flags = d->usage_fill_flags | HBS_DISK_F_USAGE;
            r->young_fill_pct = d->usage_young_pct;
            r->chunk_fill_pct = d->usage_chunk_pct;
            r->young_bytes = d->usage_eff_young;
            r->chunk_bytes = d->usage_eff_chunks;
            r->retained_bytes = d->usage_raw_retained;
            r->retired_bytes = d->usage_raw_retired;
            r->local_bytes = d->usage_raw_local;
            r->index_bytes = d->usage_raw_index;
            r->young_peak_7d = d->usage_peak_7d;
        }
#ifdef TSDB_TEST_HOOKS
        raw_size[idx] = d->usage_raw_size > 0 ? d->usage_raw_size : (d->size_bytes > 0 ? d->size_bytes : 1);
        raw_young[idx] = sat_add64(
            sat_add64(d->usage_raw_young, d->usage_raw_retained),
            sat_add64(sat_add64(d->usage_raw_retired, d->usage_raw_local), d->usage_raw_index));
        raw_chunks[idx] = d->usage_raw_chunks;
#endif
    }
    pthread_mutex_unlock(&g_mu);

    if (!started)
        return 0;
    if (nrows == 0 && role == META_ROLE_BOTH && !allow_shared && !drain_no_space && mismatches == 0)
        return 0;
#ifdef TSDB_TEST_HOOKS
    apply_disk_usage_file(rows, nrows, raw_size, raw_young, raw_chunks);
#endif
    size_t need = HBS_DISKS_HDR + (size_t)nrows * HBS_DISKS_ENTRY;
    if (need > cap)
        return -1;
    buf[0] = HBS_DISKS_VER;
    buf[1] = role;
    buf[2] = (uint8_t)((allow_shared ? 1u : 0u) | (drain_no_space ? 2u : 0u));
    buf[3] = (uint8_t)nrows;
    put_le32(buf + 4, mismatches);
    for (uint32_t i = 0; i < nrows; i++) {
        uint8_t *e = buf + HBS_DISKS_HDR + (size_t)i * HBS_DISKS_ENTRY;
        memcpy(e, rows[i].store_id, 16);
        e[16] = rows[i].disk_class;
        e[17] = rows[i].uses;
        e[18] = rows[i].state;
        e[19] = rows[i].have_usage ? (uint8_t)(rows[i].fill_flags | HBS_DISK_F_USAGE) : 0u;
        put_le64(e + 20, rows[i].size_bytes);
        put_le64(e + 28, rows[i].sync_us);
        put_le64(e + 36, rows[i].young_bytes);
        put_le64(e + 44, rows[i].chunk_bytes);
        put_le64(e + 52, rows[i].retained_bytes);
        put_le64(e + 60, rows[i].retired_bytes);
        put_le64(e + 68, rows[i].local_bytes);
        put_le64(e + 76, rows[i].index_bytes);
        put_le64(e + 84, rows[i].young_peak_7d);
        e[92] = rows[i].young_fill_pct;
        e[93] = rows[i].chunk_fill_pct;
        e[94] = 0;
        e[95] = 0;
    }
    return (ssize_t)need;
}

static int disks_sec_decode(uint32_t member_id, const uint8_t *data, size_t len, void **out_val)
{
    (void)member_id;
    if (!data || !out_val || len < HBS_DISKS_HDR)
        return -1;
    uint8_t ver = data[0];
    size_t esz;
    if (ver == HBS_DISKS_VER)
        esz = HBS_DISKS_ENTRY;
    else if (ver == HBS_DISKS_VER_V2)
        esz = HBS_DISKS_ENTRY_V2;
    else if (ver == HBS_DISKS_VER_V1)
        esz = HBS_DISKS_ENTRY_V1;
    else
        return -1;
    uint8_t role = data[1];
    uint8_t flags = data[2];
    uint32_t n = data[3];
    if (role > META_ROLE_LAST || (flags & ~3u) || n > DISKS_MAX)
        return -1;
    if (len != HBS_DISKS_HDR + (size_t)n * esz)
        return -1;
    struct hbs_disks *v = calloc(1, sizeof *v + (size_t)n * sizeof v->disks[0]);
    if (!v)
        return -1;
    v->role = role;
    v->allow_shared_device = (uint8_t)(flags & 1u);
    v->drain_no_space = (uint8_t)((flags >> 1) & 1u);
    v->class_mismatches = get_le32(data + 4);
    v->ndisks = n;
    for (uint32_t i = 0; i < n; i++) {
        const uint8_t *e = data + HBS_DISKS_HDR + (size_t)i * esz;
        struct hbs_disk_row *r = &v->disks[i];
        memcpy(r->store_id, e, 16);
        int nz = 0;
        for (int b = 0; b < 16; b++)
            nz |= r->store_id[b];
        r->disk_class = e[16];
        r->uses = e[17];
        r->state = e[18];
        if (!nz || r->disk_class < META_DISK_NVME || r->disk_class > META_DISK_CLASS_LAST ||
            r->uses == 0 || (r->uses & ~META_DISK_USE_ALL) || r->state > META_STORE_STATE_LAST) {
            free(v);
            return -1;
        }
        if (ver == HBS_DISKS_VER_V1) {
            if (e[19] != 0) {
                free(v);
                return -1;
            }
            r->have_usage = 0;
        } else if (ver == HBS_DISKS_VER_V2) {
            if (e[19] > 1u) {
                free(v);
                return -1;
            }
            r->have_usage = e[19];
            r->fill_flags = e[19] ? HBS_DISK_F_USAGE : 0u;
        } else {
            if (e[19] & ~(HBS_DISK_F_USAGE | HBS_DISK_F_WARN | HBS_DISK_F_MOVE | HBS_DISK_F_FULL |
                          HBS_DISK_F_CHUNK_FULL)) {
                free(v);
                return -1;
            }
            r->fill_flags = e[19];
            r->have_usage = (e[19] & HBS_DISK_F_USAGE) != 0;
        }
        r->size_bytes = get_le64(e + 20);
        r->sync_us = get_le64(e + 28);
        if (ver >= HBS_DISKS_VER_V2) {
            r->young_bytes = get_le64(e + 36);
            r->chunk_bytes = get_le64(e + 44);
        }
        if (ver == HBS_DISKS_VER) {
            r->retained_bytes = get_le64(e + 52);
            r->retired_bytes = get_le64(e + 60);
            r->local_bytes = get_le64(e + 68);
            r->index_bytes = get_le64(e + 76);
            r->young_peak_7d = get_le64(e + 84);
            r->young_fill_pct = e[92] > 100u ? 100u : e[92];
            r->chunk_fill_pct = e[93] > 100u ? 100u : e[93];
        }
        if (r->size_bytes == 0) {
            free(v);
            return -1;
        }
    }
    *out_val = v;
    return 0;
}

static void disks_sec_free(void *val) { free(val); }

static size_t disks_sec_format(uint32_t member_id, const void *val, char *buf, size_t cap)
{
    if (!val || !buf || cap == 0)
        return 0;
    const struct hbs_disks *v = val;
    size_t off = 0;
    if (v->allow_shared_device && off + 1 < cap) {
        int n = snprintf(buf + off, cap - off, "disks node=%" PRIu32 " warn=allow_shared_device\n", member_id);
        if (n > 0)
            off += (size_t)n < cap - off ? (size_t)n : cap - off - 1;
    }
    if (v->class_mismatches > 0 && off + 1 < cap) {
        int n = snprintf(buf + off, cap - off, "disks node=%" PRIu32 " warn=class_mismatch count=%" PRIu32 "\n",
                         member_id, v->class_mismatches);
        if (n > 0)
            off += (size_t)n < cap - off ? (size_t)n : cap - off - 1;
    }
    if (v->drain_no_space && off + 1 < cap) {
        int n = snprintf(buf + off, cap - off, "disks node=%" PRIu32 " warn=drain_no_space\n", member_id);
        if (n > 0)
            off += (size_t)n < cap - off ? (size_t)n : cap - off - 1;
    }
    for (uint32_t i = 0; i < v->ndisks && off + 1 < cap; i++) {
        const struct hbs_disk_row *r = &v->disks[i];
        char hex[33];
        chunk_oid_hex(r->store_id, hex);
        if ((r->fill_flags & HBS_DISK_F_FULL) && off + 1 < cap) {
            int wn = snprintf(buf + off, cap - off, "disks node=%" PRIu32 " disk=%s warn=full pct=%u\n", member_id, hex,
                              (unsigned)r->young_fill_pct);
            if (wn > 0)
                off += (size_t)wn < cap - off ? (size_t)wn : cap - off - 1;
        } else if ((r->fill_flags & HBS_DISK_F_MOVE) && off + 1 < cap) {
            int wn = snprintf(buf + off, cap - off, "disks node=%" PRIu32 " disk=%s warn=move_needed pct=%u\n",
                              member_id, hex, (unsigned)r->young_fill_pct);
            if (wn > 0)
                off += (size_t)wn < cap - off ? (size_t)wn : cap - off - 1;
        } else if ((r->fill_flags & HBS_DISK_F_WARN) && off + 1 < cap) {
            int wn = snprintf(buf + off, cap - off, "disks node=%" PRIu32 " disk=%s warn=filling pct=%u\n", member_id,
                              hex, (unsigned)r->young_fill_pct);
            if (wn > 0)
                off += (size_t)wn < cap - off ? (size_t)wn : cap - off - 1;
        }
        if ((r->fill_flags & HBS_DISK_F_CHUNK_FULL) && off + 1 < cap) {
            int wn = snprintf(buf + off, cap - off, "disks node=%" PRIu32 " disk=%s warn=chunk_full pct=%u\n",
                              member_id, hex, (unsigned)r->chunk_fill_pct);
            if (wn > 0)
                off += (size_t)wn < cap - off ? (size_t)wn : cap - off - 1;
        }
        if (off + 1 >= cap)
            break;
        int n;
        if (r->have_usage)
            n = snprintf(buf + off, cap - off,
                         "disk node=%" PRIu32 " id=%s class=%s uses=%s state=%s size=%" PRIu64 " sync_us=%" PRIu64
                         " young_bytes=%" PRIu64 " chunk_bytes=%" PRIu64 " retained_bytes=%" PRIu64
                         " retired_bytes=%" PRIu64 " local_bytes=%" PRIu64 " index_bytes=%" PRIu64
                         " young_pct=%u chunk_pct=%u\n",
                         member_id, hex, meta_disk_class_name(r->disk_class), uses_str(r->uses),
                         meta_store_state_name(r->state), r->size_bytes, r->sync_us, r->young_bytes, r->chunk_bytes,
                         r->retained_bytes, r->retired_bytes, r->local_bytes, r->index_bytes,
                         (unsigned)r->young_fill_pct, (unsigned)r->chunk_fill_pct);
        else
            n = snprintf(buf + off, cap - off,
                         "disk node=%" PRIu32 " id=%s class=%s uses=%s state=%s size=%" PRIu64 " sync_us=%" PRIu64 "\n",
                         member_id, hex, meta_disk_class_name(r->disk_class), uses_str(r->uses),
                         meta_store_state_name(r->state), r->size_bytes, r->sync_us);
        if (n > 0)
            off += (size_t)n < cap - off ? (size_t)n : cap - off - 1;
    }
    buf[off] = '\0';
    return off;
}

static void disks_collect_mismatch(void *ctx, struct creport_sink *sink)
{
    (void)ctx;
    if (!atomic_load_explicit(&g_disks_started, memory_order_acquire))
        return;
    pthread_mutex_lock(&g_mu);
    uint32_t mm = G.class_mismatches;
    int started = G.started;
    pthread_mutex_unlock(&g_mu);
    if (started)
        creport_sink_put_u64(sink, NULL, 0, mm);
}

static void disks_collect_shared(void *ctx, struct creport_sink *sink)
{
    (void)ctx;
    if (!atomic_load_explicit(&g_disks_started, memory_order_acquire))
        return;
    pthread_mutex_lock(&g_mu);
    int sh = G.allow_shared;
    int started = G.started;
    pthread_mutex_unlock(&g_mu);
    if (started)
        creport_sink_put_u64(sink, NULL, 0, sh ? 1u : 0u);
}

static void disks_collect_state(void *ctx, struct creport_sink *sink)
{
    (void)ctx;
    if (!atomic_load_explicit(&g_disks_started, memory_order_acquire))
        return;
    struct {
        uint8_t store_id[16];
        int disk_class;
        uint8_t state;
    } snap[DISKS_MAX];
    int n = 0;
    pthread_mutex_lock(&g_mu);
    if (G.started) {
        for (int i = 0; i < G.ndisks && n < DISKS_MAX; i++) {
            const struct disk_entry *d = &G.disks[i];
            int nz = 0;
            for (int b = 0; b < 16; b++)
                nz |= d->meta.store_id[b];
            if (!nz)
                continue;
            memcpy(snap[n].store_id, d->meta.store_id, 16);
            snap[n].disk_class = d->effective_class;
            snap[n].state = d->state;
            n++;
        }
    }
    pthread_mutex_unlock(&g_mu);
    for (int i = 0; i < n; i++) {
        char hex[33];
        chunk_oid_hex(snap[i].store_id, hex);
        struct creport_label lbl[2] = {
            {"store", hex},
            {"class", meta_disk_class_name(snap[i].disk_class)},
        };
        creport_sink_put_u64(sink, lbl, 2, snap[i].state);
    }
}

static void disks_collect_bytes(void *ctx, struct creport_sink *sink)
{
    (void)ctx;
    if (!atomic_load_explicit(&g_disks_started, memory_order_acquire))
        return;
    struct disk_fill_snap snap[DISKS_MAX];
    int n = disks_usage_snapshot(snap, DISKS_MAX);
    static const char *const use_names[6] = {"young", "retained", "retired", "local", "index", "chunks"};
    for (int i = 0; i < n; i++) {
        char hex[33];
        chunk_oid_hex(snap[i].store_id, hex);
        const char *cls = meta_disk_class_name(snap[i].disk_class);
        uint64_t vals[6] = {
            snap[i].young_data_bytes, snap[i].retained_bytes, snap[i].retired_bytes,
            snap[i].local_bytes,      snap[i].index_bytes,    snap[i].chunk_bytes,
        };
        for (int u = 0; u < 6; u++) {
            struct creport_label lbl[3] = {
                {"store", hex},
                {"class", cls},
                {"use", use_names[u]},
            };
            creport_sink_put_u64(sink, lbl, 3, vals[u]);
        }
    }
}

static void disks_collect_size(void *ctx, struct creport_sink *sink)
{
    (void)ctx;
    if (!atomic_load_explicit(&g_disks_started, memory_order_acquire))
        return;
    struct {
        uint8_t store_id[16];
        int disk_class;
        uint64_t size_bytes;
    } snap[DISKS_MAX];
    int n = 0;
    pthread_mutex_lock(&g_mu);
    if (G.started) {
        for (int i = 0; i < G.ndisks && n < DISKS_MAX; i++) {
            const struct disk_entry *d = &G.disks[i];
            int nz = 0;
            for (int b = 0; b < 16; b++)
                nz |= d->meta.store_id[b];
            if (!nz)
                continue;
            memcpy(snap[n].store_id, d->meta.store_id, 16);
            snap[n].disk_class = d->effective_class;
            snap[n].size_bytes = d->usage_have && d->usage_size > 0 ? d->usage_size : d->size_bytes;
            n++;
        }
    }
    pthread_mutex_unlock(&g_mu);
    for (int i = 0; i < n; i++) {
        char hex[33];
        chunk_oid_hex(snap[i].store_id, hex);
        struct creport_label lbl[2] = {
            {"store", hex},
            {"class", meta_disk_class_name(snap[i].disk_class)},
        };
        creport_sink_put_u64(sink, lbl, 2, snap[i].size_bytes);
    }
}

/* ---------------------------------------------------------------- detection helper */

static void run_detect_check(struct disk_entry *d)
{
    if (!d->present || d->from_dir)
        return;
    int det_cls = DISK_CLASS_NONE;
    int got = 0;
#ifdef TSDB_TEST_HOOKS
    const char *ov = getenv("TSDB_DISK_DETECT_OVERRIDE");
    if (ov && *ov) {
        det_cls = disk_class_parse(ov);
        if (det_cls != DISK_CLASS_NONE)
            got = 1;
    }
    const char *sysroot = getenv("TSDB_DISK_SYSROOT");
    if (sysroot && !*sysroot)
        sysroot = NULL;
#else
    const char *sysroot = NULL;
#endif
    if (!got) {
        if (disk_detect(d->path, sysroot, &det_cls, NULL, 0) == 0 && det_cls != DISK_CLASS_NONE)
            got = 1;
    }
    if (got && det_cls != d->effective_class) {
        d->class_mismatch = 1;
        d->detected_class = det_cls;
        G.class_mismatches++;
        fprintf(stderr,
                "tsdbd: warning: disk %s: detected class %s disagrees with recorded class %s "
                "(not refusing; see tsdb_disk_class_mismatch)\n",
                d->path, disk_class_name(det_cls), disk_class_name(d->effective_class));
    }
}

/* ---------------------------------------------------------------- phase 1: disks_prepare */

int disks_prepare(const struct disks_cfg *cfg, char *err, size_t errlen)
{
    if (!cfg || !cfg->state_dir || !*cfg->state_dir || cfg->nspecs <= 0 || cfg->nspecs > DISKS_MAX) {
        snprintf(err, errlen, "invalid disk configuration (need 1..%d disks and --state-dir)", DISKS_MAX);
        return -1;
    }
    pthread_mutex_lock(&g_mu);
    atomic_store_explicit(&g_disks_started, 0, memory_order_release);
    close_all_locks_locked();
    memset(&G, 0, sizeof G);
    for (int i = 0; i < DISKS_MAX; i++) {
        G.disks[i].lock_fd = -1;
        G.disks[i].dom_idx = -1;
        G.disks[i].chunk_slot = -1;
    }
    snprintf(G.state_dir, sizeof G.state_dir, "%s", cfg->state_dir);
    G.role = cfg->role;
    G.allow_shared = cfg->allow_shared_device ? 1 : 0;

#ifdef TSDB_TEST_HOOKS
    const char *mu = getenv("TSDB_DISKS_MUTANT");
    if (mu && *mu) {
        int m = disks_mutant_parse(mu);
        if (m < 0) {
            snprintf(err, errlen, "TSDB_DISKS_MUTANT=%s: no such mutant", mu);
            pthread_mutex_unlock(&g_mu);
            return -1;
        }
        G.mutant = m;
        eng_set_disk_fill_mutants(m == DISKS_MUT_FULLNORELEASE, m == DISKS_MUT_FULLMERGEON);
        fprintf(stderr, "tsdbd: disks: MUTANT %s (tests only)\n", mu);
    } else {
        eng_set_disk_fill_mutants(0, 0);
    }
#endif

    known_load(G.state_dir);

    /* Deduplicate: an explicit --disk wins over an implicit --dir entry for the same path (preserving is_dir so
     * missing DISK on --dir is still refused); two explicit --disk naming the same path are refused. */
    struct disk_spec dedup[DISKS_MAX];
    int dedup_is_dir[DISKS_MAX];
    int ndedup = 0;
    for (int i = 0; i < cfg->nspecs; i++) {
        int dup = -1;
        for (int j = 0; j < ndedup; j++) {
            if (same_path(dedup[j].path, cfg->specs[i].path)) {
                dup = j;
                break;
            }
        }
        if (dup >= 0) {
            if (!cfg->specs[i].from_dir && !dedup[dup].from_dir) {
                snprintf(err, errlen, "disk %s: named twice", cfg->specs[i].path);
                pthread_mutex_unlock(&g_mu);
                return -1;
            }
            if (!cfg->specs[i].from_dir && dedup[dup].from_dir)
                dedup[dup] = cfg->specs[i];
            dedup_is_dir[dup] |= cfg->specs[i].from_dir ? 1 : 0;
            continue;
        }
        dedup_is_dir[ndedup] = cfg->specs[i].from_dir ? 1 : 0;
        dedup[ndedup++] = cfg->specs[i];
    }

    struct stat root_st;
    int have_root = (stat("/", &root_st) == 0);
    dev_t state_dev = 0;
    int have_state = (stat_state_dev(G.state_dir, &state_dev) == 0);

    uint8_t peek_uuid[16] = {0};
    uint32_t peek_id = 0;
    int peek_have_node = cluster_peek_node(cfg->state_dir, peek_uuid, &peek_id);

    for (int i = 0; i < ndedup; i++) {
        struct disk_entry *d = &G.disks[G.ndisks];
        clean_path(dedup[i].path, d->path, sizeof d->path);
        d->stated_class = dedup[i].stated_class;
        d->from_dir = dedup[i].from_dir;
        d->is_dir = dedup_is_dir[i];
        d->lock_fd = -1;

        disk_meta dm;
        int rc = disk_load(d->path, &dm);
        if (rc == TSDB_ERR_NOTFOUND) {
            if (d->is_dir) {
                /* E58: never let tsdb_open() create a fresh root database on an unmounted --dir mount point. */
                snprintf(err, errlen,
                         "disk %s (--dir): no %s metadata (refusing to open an unmounted engine directory)", d->path,
                         DISK_META_FILE);
                pthread_mutex_unlock(&g_mu);
                return -1;
            }
            const struct known_disk *kd = known_find_path(d->path);
            if (!kd) {
                snprintf(err, errlen,
                         "disk %s: no %s metadata (unmounted mount point or not initialised; run tsdb disk init)",
                         d->path, DISK_META_FILE);
                pthread_mutex_unlock(&g_mu);
                return -1;
            }
#ifdef TSDB_TEST_HOOKS
            if (G.mutant == DISKS_MUT_MISSINGASNEW) {
                /* Planted bug: treat a known missing store as brand new by re-initialising DISK. */
                char why[256];
                if (disk_init2(d->path, kd->disk_class, NULL, (kd->uses & META_DISK_USE_CHUNK) != 0, &dm, why,
                               sizeof why) != 0) {
                    snprintf(err, errlen, "disk %s: mutant missingasnew init failed: %s", d->path, why);
                    pthread_mutex_unlock(&g_mu);
                    return -1;
                }
                goto loaded_ok;
            }
#endif
            /* Known store whose path lacks DISK or is gone: reported DOWN, never made new (E58). */
            d->present = 0;
            d->state = META_STORE_DOWN;
            memcpy(d->meta.store_id, kd->store_id, 16);
            d->meta.disk_class = kd->disk_class;
            d->meta.sync_p50_us = kd->sync_us;
            d->effective_class = kd->disk_class;
            d->uses = kd->uses;
            d->size_bytes = kd->size_bytes > 0 ? kd->size_bytes : 1;
            d->sync_us = kd->sync_us;
            char hex[33];
            chunk_oid_hex(kd->store_id, hex);
            fprintf(stderr, "tsdbd: disks: %s: known store %s is missing (no %s), reporting DOWN\n", d->path, hex,
                    DISK_META_FILE);
            G.ndisks++;
            continue;
        }
        if (rc != 0) {
            snprintf(err, errlen, "disk %s: cannot load %s: %s", d->path, DISK_META_FILE, tsdb_strerror(rc));
            pthread_mutex_unlock(&g_mu);
            return -1;
        }
#ifdef TSDB_TEST_HOOKS
    loaded_ok:
#endif
        /* Probe flock and bound ownership before tsdb_open() so a foreign or live-busy disk never opens the engine. */
        {
            int lfd = -1;
            if (try_lock_disk(d->path, &lfd, err, errlen) != 0) {
                pthread_mutex_unlock(&g_mu);
                return -1;
            }
            close(lfd);
        }
        if (dm.bound) {
            if (peek_have_node) {
                if (memcmp(dm.cluster_uuid, peek_uuid, 16) != 0 || dm.node_id != peek_id) {
                    char have_hex[33], want_hex[33];
                    chunk_oid_hex(dm.cluster_uuid, have_hex);
                    chunk_oid_hex(peek_uuid, want_hex);
                    snprintf(err, errlen,
                             "disk %s belongs to cluster %s node %" PRIu32 ", but this is cluster %s node %" PRIu32,
                             d->path, have_hex, dm.node_id, want_hex, peek_id);
                    pthread_mutex_unlock(&g_mu);
                    return -1;
                }
            } else {
                char have_hex[33];
                chunk_oid_hex(dm.cluster_uuid, have_hex);
                snprintf(err, errlen,
                         "disk %s belongs to cluster %s node %" PRIu32 ", but this server is in no cluster yet",
                         d->path, have_hex, dm.node_id);
                pthread_mutex_unlock(&g_mu);
                return -1;
            }
        }
        /* Pin the engine directory's store_id on first start (or verify it matches the recorded primary in DISKS)
         * so omitting --dir or switching --dir across restarts never opens a different disk as the engine root. */
        if (d->is_dir) {
            if (G.have_primary_id) {
                if (memcmp(dm.store_id, G.primary_id, 16) != 0) {
                    char have_hex[33], want_hex[33];
                    chunk_oid_hex(dm.store_id, have_hex);
                    chunk_oid_hex(G.primary_id, want_hex);
                    snprintf(err, errlen, "disk %s (--dir): store %s disagrees with recorded primary engine disk %s",
                             d->path, have_hex, want_hex);
                    pthread_mutex_unlock(&g_mu);
                    return -1;
                }
            } else {
                memcpy(G.primary_id, dm.store_id, 16);
                G.have_primary_id = 1;
            }
        }
        /* Duplicate store_id across two different paths. */
        for (int j = 0; j < G.ndisks; j++) {
            if (memcmp(G.disks[j].meta.store_id, dm.store_id, 16) == 0) {
                char hex[33];
                chunk_oid_hex(dm.store_id, hex);
                snprintf(err, errlen, "disk %s and disk %s share store id %s", G.disks[j].path, d->path, hex);
                pthread_mutex_unlock(&g_mu);
                return -1;
            }
        }
        struct stat st;
        if (stat(d->path, &st) != 0) {
            snprintf(err, errlen, "disk %s: stat failed: %s", d->path, strerror(errno));
            pthread_mutex_unlock(&g_mu);
            return -1;
        }
        d->present = 1;
        d->state = META_STORE_UP;
        d->meta = dm;
        d->effective_class = dm.disk_class;
        d->sync_us = dm.sync_p50_us;
        d->size_bytes = fs_size_bytes(d->path);
        d->dev = st.st_dev;

        /* E58 st_dev check: refuse /, --state-dir, or another present --disk unless --allow-shared-device. */
        int check_dev = !G.allow_shared;
#ifdef TSDB_TEST_HOOKS
        if (G.mutant == DISKS_MUT_NODEVCHECK)
            check_dev = 0;
#endif
        if (check_dev) {
            if (have_root && d->dev == root_st.st_dev) {
                snprintf(err, errlen,
                         "disk %s: on the same device (st_dev) as / (pass --allow-shared-device for tests or "
                         "one-disk hosts)",
                         d->path);
                pthread_mutex_unlock(&g_mu);
                return -1;
            }
            if (have_state && d->dev == state_dev) {
                snprintf(err, errlen,
                         "disk %s: on the same device (st_dev) as --state-dir %s (pass --allow-shared-device for "
                         "tests or one-disk hosts)",
                         d->path, G.state_dir);
                pthread_mutex_unlock(&g_mu);
                return -1;
            }
            for (int j = 0; j < G.ndisks; j++) {
                if (G.disks[j].present && G.disks[j].dev == d->dev) {
                    snprintf(err, errlen,
                             "disk %s: on the same device (st_dev) as disk %s (pass --allow-shared-device for "
                             "tests or one-disk hosts)",
                             d->path, G.disks[j].path);
                    pthread_mutex_unlock(&g_mu);
                    return -1;
                }
            }
        }
        if (d->from_dir) {
            fprintf(stderr,
                    "tsdbd: warning: --dir %s used as a cluster disk (class %s, skipping device class detection)\n",
                    d->path, disk_class_name(d->effective_class));
        }
        G.ndisks++;
    }

    if (G.allow_shared) {
        fprintf(stderr,
                "tsdbd: warning: --allow-shared-device is set: device isolation checks are disabled "
                "(health warns while set)\n");
    }

    G.prepared = 1;
    pthread_mutex_unlock(&g_mu);
    return 0;
}

const char *disks_primary_dir(void)
{
    static char out[PATH_MAX];
    out[0] = '\0';
    pthread_mutex_lock(&g_mu);
    /* If a primary engine disk was already recorded in <state-dir>/DISKS, keep it pinned and refuse if missing. */
    if (G.have_primary_id) {
        for (int i = 0; i < G.ndisks; i++) {
            if (memcmp(G.disks[i].meta.store_id, G.primary_id, 16) == 0) {
                if (G.disks[i].present && G.disks[i].path[0])
                    snprintf(out, sizeof out, "%s", G.disks[i].path);
                break;
            }
        }
        if (!out[0]) {
            char phex[33];
            chunk_oid_hex(G.primary_id, phex);
            fprintf(stderr,
                    "tsdbd: disks: primary engine disk %s is missing or not passed (refusing to open another disk as "
                    "engine root)\n",
                    phex);
        }
        pthread_mutex_unlock(&g_mu);
        return out[0] ? out : NULL;
    }
    /* First start without --dir: pick present nvme, then present ssd, then present hdd; never a non-present disk. */
    const int order[] = {DISK_CLASS_NVME, DISK_CLASS_SSD, DISK_CLASS_HDD};
    for (size_t k = 0; k < 3 && !out[0]; k++) {
        for (int i = 0; i < G.ndisks; i++) {
            if (G.disks[i].present && G.disks[i].effective_class == order[k] && G.disks[i].path[0]) {
                snprintf(out, sizeof out, "%s", G.disks[i].path);
                memcpy(G.primary_id, G.disks[i].meta.store_id, 16);
                G.have_primary_id = 1;
                break;
            }
        }
    }
    pthread_mutex_unlock(&g_mu);
    return out[0] ? out : NULL;
}

int disks_creport_register(void)
{
    return creport_section(HBS_DISKS, disks_sec_encode, disks_sec_decode, disks_sec_free, disks_sec_format);
}

/* ---------------------------------------------------------------- phase 2: disks_start */

static void compute_uses_locked(void)
{
    int has_cap = 0;
    for (int i = 0; i < G.ndisks; i++) {
        if (G.disks[i].state != META_STORE_DESTROYED && G.disks[i].effective_class == DISK_CLASS_HDD) {
            has_cap = 1;
            break;
        }
    }
    for (int i = 0; i < G.ndisks; i++) {
        struct disk_entry *d = &G.disks[i];
        d->has_chunks = 0;
        d->draining = 0;
        d->chunks_path[0] = '\0';
        /* Disk uses follow hardware layout (E48/E50): HDD holds chunks; fast disks hold log replicas, and also
         * chunks on a server without capacity disks. Preserve recorded metadata/DISKS uses for missing (!present)
         * entries so DOWN slots keep their recorded chunk-store assignment. */
        if (d->present || d->uses == 0) {
            if (d->effective_class == DISK_CLASS_HDD) {
                d->uses = META_DISK_USE_CHUNK;
            } else if (has_cap) {
                d->uses = META_DISK_USE_LOG;
            } else {
                d->uses = META_DISK_USE_LOG | META_DISK_USE_CHUNK;
            }
        }
        if (d->present && !has_cap && (d->uses & META_DISK_USE_CHUNK) == 0 && d->effective_class != DISK_CLASS_HDD)
            d->uses = META_DISK_USE_LOG | META_DISK_USE_CHUNK;
        /* Only configured disk paths (d->path[0] != '\0') occupy chunk-backend slots: a temporarily missing --disk
         * keeps a NULL DOWN_MISSING slot at its index, while a retired/replaced metadata-only id (no path) does not
         * grow be->n or break hash%n puts. */
        if ((d->uses & META_DISK_USE_CHUNK) && G.role != META_ROLE_COMPUTE && d->state != META_STORE_DESTROYED &&
            d->state != META_STORE_OUT && d->path[0] != '\0')
            d->has_chunks = 1;
        if (d->has_chunks && d->present)
            snprintf(d->chunks_path, sizeof d->chunks_path, "%s/chunks", d->path);
        /* R13 r13-lmove (E50): when capacity disks are added to an all-flash server, any fast disk that already holds
         * a `<disk>/chunks/` store is still opened so reads stay whole and start-time dedupe sees both stores, while
         * marked draining (no new puts; background drain moves its chunks locally onto capacity disks). */
        if (!d->has_chunks && d->present && has_cap && G.role != META_ROLE_COMPUTE &&
            d->state != META_STORE_DESTROYED && d->state != META_STORE_OUT && d->path[0] != '\0') {
            char cpath[PATH_MAX];
            if (snprintf(cpath, sizeof cpath, "%s/chunks", d->path) < (int)sizeof cpath) {
                uint8_t cid[16];
                if (chunk_store_meta_read(cpath, cid, NULL) == 0) {
                    d->has_chunks = 1;
                    d->draining = 1;
                    snprintf(d->chunks_path, sizeof d->chunks_path, "%s", cpath);
                }
            }
        }
    }
}

static int ensure_chunk_store(const struct disk_entry *d, const struct cluster_self *me, char *err, size_t errlen)
{
    uint8_t cid[16];
    chunk_store_owner ow;
    int rc = chunk_store_meta_read(d->chunks_path, cid, &ow);
    if (rc == TSDB_ERR_NOTFOUND) {
        rc = chunk_store_init(d->chunks_path, d->meta.store_id, NULL);
        if (rc != 0) {
            snprintf(err, errlen, "disk %s: cannot initialise chunk store at %s: %s", d->path, d->chunks_path,
                     tsdb_strerror(rc));
            return -1;
        }
        return 0;
    }
    if (rc != 0) {
        snprintf(err, errlen, "disk %s: cannot read chunk store %s/META: %s", d->path, d->chunks_path,
                 tsdb_strerror(rc));
        return -1;
    }
    if (memcmp(cid, d->meta.store_id, 16) != 0) {
        char h1[33], h2[33];
        chunk_oid_hex(d->meta.store_id, h1);
        chunk_oid_hex(cid, h2);
        snprintf(err, errlen, "disk %s: %s store id %s disagrees with %s/META store id %s", d->path, DISK_META_FILE, h1,
                 d->chunks_path, h2);
        return -1;
    }
    if (ow.set && me && me->have_node &&
        (memcmp(ow.cluster, me->uuid, 16) != 0 || ow.node != me->id)) {
        char have[33], want[33];
        chunk_oid_hex(ow.cluster, have);
        chunk_oid_hex(me->uuid, want);
        snprintf(err, errlen,
                 "chunk store %s belongs to cluster %s node %" PRIu32 ", but this is cluster %s node %" PRIu32,
                 d->chunks_path, have, ow.node, want, me->id);
        return -1;
    }
    return 0;
}

int disks_start(char *err, size_t errlen)
{
    struct cluster_self me;
    int have_self = (cluster_self(&me) == 0 && me.have_node);
    const struct cluster_copy *c = cluster_copy_get();

    pthread_mutex_lock(&g_mu);
    if (!G.prepared) {
        pthread_mutex_unlock(&g_mu);
        if (c)
            cluster_copy_put(c);
        tsdb_wipe(&me, sizeof me);
        snprintf(err, errlen, "disks_start called before disks_prepare");
        return -1;
    }

    int all_bound = have_self ? 1 : 0;

    for (int i = 0; i < G.ndisks; i++) {
        struct disk_entry *d = &G.disks[i];
        int meta_cls = DISK_CLASS_NONE;

        /* Look up store_id across all members in the metadata copy (live copy or initial persisted copy). */
        uint32_t rec_node = 0;
        struct meta_disk rec_disk;
        int have_rec = 0;
        if (c) {
            for (uint32_t m = 0; m < c->nmembers && !have_rec; m++) {
                const struct cluster_copy_member *mb = &c->members[m];
                for (uint32_t k = 0; k < mb->ndisks && k < META_DISKS_MAX; k++) {
                    if (memcmp(mb->disks[k].store_id, d->meta.store_id, 16) == 0) {
                        rec_node = mb->id;
                        rec_disk = mb->disks[k];
                        have_rec = 1;
                        break;
                    }
                }
            }
        }
        if (!have_rec)
            have_rec = cluster_initial_disk_lookup(d->meta.store_id, &rec_node, &rec_disk);

        int lfd = -1;
        if (d->present) {
            /* Probe exclusive flock first so a disk held by another live tsdbd always reports BUSY. */
            if (try_lock_disk(d->path, &lfd, err, errlen) != 0)
                goto fail;
        }

        if (have_rec) {
            if (d->present && have_self && rec_node != me.id) {
                close(lfd);
                char hex[33];
                chunk_oid_hex(d->meta.store_id, hex);
                snprintf(err, errlen,
                         "disk %s (store %s): metadata assigns it to node %" PRIu32 ", but this is node %" PRIu32,
                         d->path, hex, rec_node, me.id);
                goto fail;
            }
            if (d->present && rec_disk.state == META_STORE_DESTROYED) {
                close(lfd);
                char hex[33];
                chunk_oid_hex(d->meta.store_id, hex);
                snprintf(err, errlen, "disk %s (store %s): metadata marks this store DESTROYED", d->path, hex);
                goto fail;
            }
            meta_cls = rec_disk.disk_class;
            if (!d->present) {
                d->effective_class = rec_disk.disk_class;
                d->uses = rec_disk.uses;
                d->size_bytes = rec_disk.size_bytes > 0 ? rec_disk.size_bytes : 1;
                d->sync_us = rec_disk.sync_us;
                if (rec_disk.state == META_STORE_DESTROYED)
                    d->state = META_STORE_DESTROYED;
            }
        }

        if (!d->present)
            continue;

        /* Ownership check against DISK metadata (E58). */
        if (have_self) {
            if (d->meta.bound) {
                if (memcmp(d->meta.cluster_uuid, me.uuid, 16) != 0 || d->meta.node_id != me.id) {
                    close(lfd);
                    char have_hex[33], want_hex[33];
                    chunk_oid_hex(d->meta.cluster_uuid, have_hex);
                    chunk_oid_hex(me.uuid, want_hex);
                    snprintf(err, errlen,
                             "disk %s belongs to cluster %s node %" PRIu32 ", but this is cluster %s node %" PRIu32,
                             d->path, have_hex, d->meta.node_id, want_hex, me.id);
                    goto fail;
                }
                d->lock_fd = lfd; /* already bound to us: keep the exclusive flock */
            } else {
                close(lfd); /* release briefly so disk_bind() can lock and atomically update DISK */
            }
        } else {
            close(lfd);
            if (d->meta.bound) {
                char have_hex[33];
                chunk_oid_hex(d->meta.cluster_uuid, have_hex);
                snprintf(err, errlen,
                         "disk %s belongs to cluster %s node %" PRIu32 ", but this server is in no cluster yet",
                         d->path, have_hex, d->meta.node_id);
                goto fail;
            }
        }

        /* Authoritative class: metadata record if committed, else DISK's class (§15.1). */
        int auth_cls = meta_cls != DISK_CLASS_NONE ? meta_cls : d->meta.disk_class;
        d->effective_class = auth_cls;

        /* Explicit :CLASS mismatch is refused naming both (unless mutant classsilent). */
        int check_cls = 1;
#ifdef TSDB_TEST_HOOKS
        if (G.mutant == DISKS_MUT_CLASSSILENT)
            check_cls = 0;
#endif
        if (check_cls && d->stated_class != DISK_CLASS_NONE && d->stated_class != auth_cls) {
            snprintf(err, errlen, "disk %s: stated class %s disagrees with %s class %s", d->path,
                     disk_class_name(d->stated_class), meta_cls != DISK_CLASS_NONE ? "metadata" : DISK_META_FILE,
                     disk_class_name(auth_cls));
            goto fail;
        }

        /* Detection runs at every start: disagreement is logged and counted, never refused (§15.1). */
        run_detect_check(d);

        /* First start binds unbound disks atomically via disk_bind(). */
        if (have_self && !d->meta.bound) {
            int brc = disk_bind(d->path, me.uuid, me.id);
            if (brc != 0) {
                snprintf(err, errlen, "disk %s: failed binding %s: %s", d->path, DISK_META_FILE, tsdb_strerror(brc));
                goto fail;
            }
            memcpy(d->meta.cluster_uuid, me.uuid, 16);
            d->meta.node_id = me.id;
            d->meta.bound = true;
            char shex[33], chex[33];
            chunk_oid_hex(d->meta.store_id, shex);
            chunk_oid_hex(me.uuid, chex);
            fprintf(stderr, "tsdbd: disks: bound %s (store %s, class %s) to cluster %s node %" PRIu32 "\n", d->path,
                    shex, disk_class_name(d->effective_class), chex, me.id);
            if (try_lock_disk(d->path, &d->lock_fd, err, errlen) != 0)
                goto fail;
        }
    }

    /* Include any non-DESTROYED store recorded on this member in metadata that was omitted from --disk (DOWN). */
    if (have_self) {
        struct meta_disk self_disks[META_DISKS_MAX];
        uint32_t nself = 0;
        const struct cluster_copy_member *self_mb = c ? cluster_copy_member(c, me.id) : NULL;
        if (self_mb && self_mb->ndisks > 0) {
            nself = self_mb->ndisks < META_DISKS_MAX ? self_mb->ndisks : META_DISKS_MAX;
            memcpy(self_disks, self_mb->disks, nself * sizeof self_disks[0]);
        } else {
            nself = cluster_initial_self_disks(self_disks, META_DISKS_MAX);
        }
        for (uint32_t k = 0; k < nself; k++) {
            const struct meta_disk *md = &self_disks[k];
            if (md->state == META_STORE_DESTROYED || md->state == META_STORE_OUT)
                continue;
            int found = 0;
            for (int i = 0; i < G.ndisks; i++) {
                if (memcmp(G.disks[i].meta.store_id, md->store_id, 16) == 0) {
                    found = 1;
                    break;
                }
            }
            if (!found && G.ndisks < DISKS_MAX) {
                struct disk_entry *d = &G.disks[G.ndisks++];
                memset(d, 0, sizeof *d);
                d->lock_fd = -1;
                d->dom_idx = -1;
                d->chunk_slot = -1;
                d->present = 0;
                d->state = META_STORE_DOWN;
                memcpy(d->meta.store_id, md->store_id, 16);
                d->meta.disk_class = md->disk_class;
                d->effective_class = md->disk_class;
                d->uses = md->uses;
                d->size_bytes = md->size_bytes > 0 ? md->size_bytes : 1;
                d->sync_us = md->sync_us;
                char shex[33];
                chunk_oid_hex(md->store_id, shex);
                fprintf(stderr, "tsdbd: disks: known store %s not passed at start, reporting DOWN\n", shex);
            }
        }
    }

    compute_uses_locked();

    /* Ensure <disk>/chunks/ exists and matches DISK store_id for every present chunk-serving disk. */
    for (int i = 0; i < G.ndisks; i++) {
        if (G.disks[i].has_chunks && G.disks[i].present) {
            if (ensure_chunk_store(&G.disks[i], have_self ? &me : NULL, err, errlen) != 0)
                goto fail;
        }
    }

    G.bound_all = all_bound;
    G.started = 1;
    atomic_store_explicit(&g_disks_started, 1, memory_order_release);
    pthread_mutex_unlock(&g_mu);

    if (c)
        cluster_copy_put(c);
    tsdb_wipe(&me, sizeof me);

    if (have_self)
        persist_known_from_state();

    (void)disks_creport_register();
    (void)creport_metric_collector("tsdb_disk_class_mismatch", CREPORT_GAUGE,
                                   "Number of local disks whose detected /sys class disagrees with the recorded class.",
                                   disks_collect_mismatch, NULL);
    (void)creport_metric_collector("tsdb_disk_shared_device", CREPORT_GAUGE,
                                   "1 while --allow-shared-device is active on this server (health warning), else 0.",
                                   disks_collect_shared, NULL);
    (void)creport_metric_collector("tsdb_disk_state", CREPORT_GAUGE,
                                   "Local disk state per store: 0 up, 1 down, 2 out, 3 destroyed.",
                                   disks_collect_state, NULL);
    (void)creport_metric_collector("tsdb_disk_bytes", CREPORT_GAUGE,
                                   "Local disk bytes by use (young, retained, retired, local, index, chunks).",
                                   disks_collect_bytes, NULL);
    (void)creport_metric_collector("tsdb_disk_size_bytes", CREPORT_GAUGE,
                                   "Total capacity in bytes of each local cluster disk.",
                                   disks_collect_size, NULL);
    if (have_self)
        cluster_beat_soon();
    return 0;

fail:
    G.prepared = 0;
    close_all_locks_locked();
    pthread_mutex_unlock(&g_mu);
    if (c)
        cluster_copy_put(c);
    tsdb_wipe(&me, sizeof me);
    return -1;
}

int disks_bind_late(const uint8_t cluster_uuid[16], uint32_t node_id, char *err, size_t errlen)
{
    /* Snapshot unbound present paths under lock, bind off-lock, then update state. */
    struct {
        int idx;
        char path[PATH_MAX];
        uint8_t store_id[16];
        int cls;
    } todo[DISKS_MAX];
    int ntodo = 0;

    pthread_mutex_lock(&g_mu);
    if (!G.started || G.bound_all) {
        pthread_mutex_unlock(&g_mu);
        return 0;
    }
    for (int i = 0; i < G.ndisks; i++) {
        struct disk_entry *d = &G.disks[i];
        if (!d->present)
            continue;
        if (d->meta.bound) {
            if (memcmp(d->meta.cluster_uuid, cluster_uuid, 16) != 0 || d->meta.node_id != node_id) {
                char have_hex[33], want_hex[33];
                chunk_oid_hex(d->meta.cluster_uuid, have_hex);
                chunk_oid_hex(cluster_uuid, want_hex);
                snprintf(err, errlen,
                         "disk %s belongs to cluster %s node %" PRIu32 ", but this is cluster %s node %" PRIu32,
                         d->path, have_hex, d->meta.node_id, want_hex, node_id);
                pthread_mutex_unlock(&g_mu);
                return -1;
            }
            continue;
        }
        todo[ntodo].idx = i;
        snprintf(todo[ntodo].path, sizeof todo[ntodo].path, "%s", d->path);
        memcpy(todo[ntodo].store_id, d->meta.store_id, 16);
        todo[ntodo].cls = d->effective_class;
        ntodo++;
    }
    pthread_mutex_unlock(&g_mu);

    int lock_fds[DISKS_MAX];
    for (int t = 0; t < ntodo; t++)
        lock_fds[t] = -1;

    for (int t = 0; t < ntodo; t++) {
        int rc = disk_bind(todo[t].path, cluster_uuid, node_id);
        if (rc != 0) {
            for (int k = 0; k < t; k++)
                if (lock_fds[k] >= 0)
                    close(lock_fds[k]);
            snprintf(err, errlen, "disk %s: failed late binding %s: %s", todo[t].path, DISK_META_FILE,
                     tsdb_strerror(rc));
            return -1;
        }
        if (try_lock_disk(todo[t].path, &lock_fds[t], err, errlen) != 0) {
            for (int k = 0; k < t; k++)
                if (lock_fds[k] >= 0)
                    close(lock_fds[k]);
            return -1;
        }
        char shex[33], chex[33];
        chunk_oid_hex(todo[t].store_id, shex);
        chunk_oid_hex(cluster_uuid, chex);
        fprintf(stderr, "tsdbd: disks: bound %s (store %s, class %s) to cluster %s node %" PRIu32 "\n", todo[t].path,
                shex, disk_class_name(todo[t].cls), chex, node_id);
    }

    pthread_mutex_lock(&g_mu);
    if (!G.started) {
        for (int t = 0; t < ntodo; t++)
            if (lock_fds[t] >= 0)
                close(lock_fds[t]);
        pthread_mutex_unlock(&g_mu);
        return 0;
    }
    for (int t = 0; t < ntodo; t++) {
        struct disk_entry *d = &G.disks[todo[t].idx];
        memcpy(d->meta.cluster_uuid, cluster_uuid, 16);
        d->meta.node_id = node_id;
        d->meta.bound = true;
        if (d->lock_fd >= 0)
            close(d->lock_fd);
        d->lock_fd = lock_fds[t];
    }
    G.bound_all = 1;
    pthread_mutex_unlock(&g_mu);

    persist_known_from_state();
    cluster_beat_soon();
    return 0;
}

void disks_chunk_layout(const char **paths, int *prefs, bool *draining, int cap, int *n_out)
{
    int idx[DISKS_MAX];
    int rank[DISKS_MAX];
    int n = 0;
    pthread_mutex_lock(&g_mu);
    for (int i = 0; i < G.ndisks; i++)
        G.disks[i].chunk_slot = -1;
    /* Order chunk slots by their persisted position in <state-dir>/DISKS (by path) so argv reordering does not
     * shift hash%n store placement, with newly added disks appended in argv order. */
    for (int i = 0; i < G.ndisks && n < DISKS_MAX; i++) {
        if (!G.disks[i].has_chunks)
            continue;
        int r = DISKS_MAX + i;
        for (int k = 0; k < G.nknown; k++) {
            if (same_path(G.known[k].path, G.disks[i].path)) {
                r = k;
                break;
            }
        }
        idx[n] = i;
        rank[n] = r;
        n++;
    }
    for (int a = 1; a < n; a++) {
        int ti = idx[a], tr = rank[a], b = a - 1;
        while (b >= 0 && rank[b] > tr) {
            idx[b + 1] = idx[b];
            rank[b + 1] = rank[b];
            b--;
        }
        idx[b + 1] = ti;
        rank[b + 1] = tr;
    }
    int out = 0;
    for (int a = 0; a < n && out < cap; a++) {
        struct disk_entry *d = &G.disks[idx[a]];
        d->chunk_slot = out;
        if (prefs) {
            prefs[out] = d->draining                             ? 0
                         : d->effective_class == DISK_CLASS_HDD  ? 30
                         : d->effective_class == DISK_CLASS_SSD  ? 20
                                                                 : 10;
        }
        if (draining)
            draining[out] = d->draining != 0;
        /* Known-missing configured chunk disk keeps a NULL DOWN_MISSING slot so hash%n placement stays stable and
         * reads answer UNAVAILABLE instead of NOTFOUND. */
        if (d->present && d->chunks_path[0])
            paths[out++] = d->chunks_path;
        else
            paths[out++] = NULL;
    }
    pthread_mutex_unlock(&g_mu);
    if (n_out)
        *n_out = out;
}

void disks_chunk_paths(const char **paths, int cap, int *n_out)
{
    disks_chunk_layout(paths, NULL, NULL, cap, n_out);
}

int disks_list(struct disk_info *out, int cap)
{
    if (!out || cap <= 0)
        return 0;
    int n = 0;
    pthread_mutex_lock(&g_mu);
    if (G.prepared || G.started) {
        for (int i = 0; i < G.ndisks && n < cap; i++) {
            const struct disk_entry *d = &G.disks[i];
            struct disk_info *di = &out[n++];
            memset(di, 0, sizeof *di);
            snprintf(di->path, sizeof di->path, "%s", d->path);
            memcpy(di->store_id, d->meta.store_id, 16);
            di->disk_class = d->effective_class;
            di->uses = d->uses;
            di->state = d->state;
            di->present = d->present;
            di->recovering = d->probe_recovering;
            di->have_usage = d->usage_have;
            di->size_bytes = d->usage_have && d->usage_size > 0 ? d->usage_size : d->size_bytes;
            di->young_room_bytes = d->usage_young_room;
            di->young_used_bytes = d->usage_eff_young;
            di->young_fill_pct = d->usage_young_pct;
            di->chunk_fill_pct = d->usage_chunk_pct;
            di->fill_flags = d->usage_fill_flags;
        }
    }
    pthread_mutex_unlock(&g_mu);
    return n;
}

/* ---------------------------------------------------------------- per-disk health probes (R13, r13-fault; E57, D16) */

static void disk_probe_test_options(tsdb_health_options *o, int *down_override)
{
    *down_override = 0;
#ifdef TSDB_TEST_HOOKS
    const char *s = getenv("TSDBD_TEST_HEALTH");
    if (!s || !*s)
        return;
    char *copy = strdup(s);
    if (!copy)
        return;
    char *save = NULL;
    for (char *kv = strtok_r(copy, ",", &save); kv; kv = strtok_r(NULL, ",", &save)) {
        char *eq = strchr(kv, '=');
        if (!eq)
            continue;
        *eq = '\0';
        int val = atoi(eq + 1);
        if (val <= 0)
            continue;
        if (strcmp(kv, "down") == 0) {
            o->down_ms = val;
            *down_override = 1;
        } else if (strcmp(kv, "read") == 0) {
            o->read_every_ms = val;
        } else if (strcmp(kv, "write") == 0) {
            o->write_every_ms = val;
        } else if (strcmp(kv, "recover") == 0) {
            o->recover_ms = val;
        } else if (strcmp(kv, "flap") == 0) {
            o->flap_recover_ms = val;
        } else if (strcmp(kv, "fast") == 0) {
            o->fast_ms = val;
        } else if (strcmp(kv, "spawn") == 0) {
            o->spawn_ms = val;
        }
    }
    free(copy);
#else
    (void)o;
#endif
}

/* Acquire a non-recursive-safe reference to G.probe_h (under g_mu); returns NULL once stopping. */
static tsdb_health *probe_hold_locked(void)
{
    if (!G.probe_h || G.probe_stopping)
        return NULL;
    G.probe_users++;
    return G.probe_h;
}

static void probe_release(void)
{
    pthread_mutex_lock(&g_mu);
    if (--G.probe_users == 0 && G.probe_stopping)
        pthread_cond_broadcast(&g_probe_cv);
    pthread_mutex_unlock(&g_mu);
}

static void disk_on_health_change(void *ctx, int dom_idx, int old_state, int new_state)
{
    (void)ctx;
    int disk_idx = -1;
    uint8_t store_id[16] = {0};
    char path[PATH_MAX] = "";
    int disk_class = DISK_CLASS_NONE;
    uint8_t uses = 0;
    int chunk_slot = -1;
    uint8_t new_store_state = META_STORE_UP;
    int state_changed = 0;
    int down_ms = 10000;
    int chunk_reason = CHUNK_STORE_DOWN_IOERR;
    disk_probe_cb cb = NULL;
    void *cb_ctx = NULL;
#ifdef TSDB_TEST_HOOKS
    int mut = DISKS_MUT_NONE;
#endif

    pthread_mutex_lock(&g_mu);
    tsdb_health *h = probe_hold_locked();
    if (!h) {
        pthread_mutex_unlock(&g_mu);
        return;
    }
    cb = G.probe_cb;
    cb_ctx = G.probe_ctx;
    down_ms = G.probe_down_ms > 0 ? G.probe_down_ms : 10000;
#ifdef TSDB_TEST_HOOKS
    mut = G.mutant;
#endif
    pthread_mutex_unlock(&g_mu);

    /* Guard against a late UP callback from tsdb_health_set_up racing a newer DOWN from the monitor thread. */
    if (new_state == TSDB_DOM_UP && tsdb_health_state(h, dom_idx) != TSDB_DOM_UP) {
        probe_release();
        return;
    }
    if (new_state == TSDB_DOM_DOWN) {
        tsdb_domain_info di;
        if (tsdb_health_info(h, dom_idx, &di) == 0 && (di.stuck_ms >= down_ms || di.probes_err == 0))
            chunk_reason = CHUNK_STORE_DOWN_STALLED;
    }
    probe_release();

    pthread_mutex_lock(&g_mu);
    for (int i = 0; i < G.ndisks; i++) {
        if (G.disks[i].dom_idx != dom_idx)
            continue;
        struct disk_entry *d = &G.disks[i];
        disk_idx = i;
        memcpy(store_id, d->meta.store_id, 16);
        snprintf(path, sizeof path, "%s", d->path);
        disk_class = d->effective_class;
        uses = d->uses;
        chunk_slot = d->chunk_slot;
        uint8_t prev = d->state;
        if (new_state == TSDB_DOM_DOWN) {
            d->probe_down = 1;
            d->probe_recovering = 0;
            if (d->state == META_STORE_UP)
                d->state = META_STORE_DOWN;
            /* A chunk-only disk that went DOWN missed writes/deletes and stays DOWN until replace/restart; a fast
             * log+chunk disk recovers its log state on UP while its chunk backend slot stays DOWN_STALLED. */
            if (d->chunk_slot >= 0 && !(d->uses & META_DISK_USE_LOG))
                d->io_down = 1;
        } else if (new_state == TSDB_DOM_RECOVERING) {
            d->probe_down = 1;
            d->probe_recovering = 1;
        } else if (new_state == TSDB_DOM_UP) {
            d->probe_down = 0;
            d->probe_recovering = 0;
            if (d->present && !d->io_down && d->state == META_STORE_DOWN)
                d->state = META_STORE_UP;
        }
        new_store_state = d->state;
        state_changed = (d->state != prev);
        break;
    }
    pthread_mutex_unlock(&g_mu);

    if (disk_idx < 0)
        return;

    static const char *const hnames[4] = {"up", "degraded", "down", "recovering"};
    fprintf(stderr, "tsdbd: disks: disk %s (%s): probe %s -> %s (store state=%s)\n", path,
            meta_disk_class_name(disk_class),
            old_state >= 0 && old_state <= 3 ? hnames[old_state] : "?",
            new_state >= 0 && new_state <= 3 ? hnames[new_state] : "?",
            meta_store_state_name(new_store_state));

    if (new_state == TSDB_DOM_DOWN) {
#ifdef TSDB_TEST_HOOKS
        if (mut == DISKS_MUT_STALLEXIT && (uses & META_DISK_USE_LOG)) {
            fprintf(stderr, "tsdbd: disks: MUTANT stallexit: exiting 1 on DOWN fast disk %s\n", path);
            fflush(stderr);
            _exit(1);
        }
        if (mut == DISKS_MUT_STOREEIOEXIT && (uses & META_DISK_USE_CHUNK)) {
            fprintf(stderr, "tsdbd: disks: MUTANT storeeioexit: exiting 1 on DOWN chunk disk %s\n", path);
            fflush(stderr);
            _exit(1);
        }
#endif
        if (chunk_slot >= 0) {
            struct chunksrv *cs = chunksrv_cluster_hold();
            if (cs) {
                chunk_backend *be = chunksrv_backend(cs);
                if (be)
                    (void)chunk_be_set_down(be, chunk_slot, chunk_reason);
                chunksrv_cluster_release();
            }
        }
    }

    if (state_changed)
        cluster_beat_soon();

    if (cb)
        cb(cb_ctx, disk_idx, store_id, path, disk_class, uses, old_state, new_state);
}

static void disk_on_settings(void *arg, uint64_t version, const struct meta_settings *s)
{
    (void)arg;
    (void)version;
    if (s && s->disk_stall_s > 0)
        disk_probe_set_stall_s(s->disk_stall_s);
}

int disk_probe_start(disk_probe_cb cb, void *ctx, char *err, size_t errlen)
{
    uint32_t stall_s = 10;
    const struct cluster_copy *cp = cluster_copy_get();
    if (cp) {
        if (cp->disk_stall_s > 0)
            stall_s = cp->disk_stall_s;
        cluster_copy_put(cp);
    }

    char node_lbl[64];
    struct cluster_self me;
    if (cluster_self(&me) == 0 && me.have_node)
        snprintf(node_lbl, sizeof node_lbl, "n%" PRIu32, me.id);
    else
        snprintf(node_lbl, sizeof node_lbl, "local");
    tsdb_wipe(&me, sizeof me);

    tsdb_health_options o;
    memset(&o, 0, sizeof o);
    o.down_ms = (int)(stall_s * 1000u);
    o.on_change = disk_on_health_change;
    o.node = node_lbl;
    int down_ov = 0;
    disk_probe_test_options(&o, &down_ov);

    tsdb_health *h = tsdb_health_open(&o);
    if (!h) {
        if (err && errlen)
            snprintf(err, errlen, "cannot start per-disk health probe monitor");
        return -1;
    }

    struct {
        int disk_idx;
        int dom_idx;
        char path[PATH_MAX];
    } todo[DISKS_MAX];
    int ntodo = 0;

    pthread_mutex_lock(&g_mu);
    if (G.probe_h) {
        pthread_mutex_unlock(&g_mu);
        tsdb_health_close(h);
        return 0;
    }
    G.probe_h = h;
    G.probe_users = 0;
    G.probe_stopping = 0;
    G.probe_cb = cb;
    G.probe_ctx = ctx;
    G.probe_down_override = down_ov;
    G.probe_down_ms = o.down_ms > 0 ? o.down_ms : 10000;
    for (int i = 0; i < G.ndisks && ntodo < DISKS_MAX; i++) {
        struct disk_entry *d = &G.disks[i];
        d->dom_idx = -1;
        if (!d->present || d->state == META_STORE_DESTROYED || !d->path[0])
            continue;
        d->dom_idx = ntodo;
        todo[ntodo].disk_idx = i;
        todo[ntodo].dom_idx = ntodo;
        snprintf(todo[ntodo].path, sizeof todo[ntodo].path, "%s", d->path);
        ntodo++;
    }
    pthread_mutex_unlock(&g_mu);

    for (int k = 0; k < ntodo; k++) {
        int idx = tsdb_health_add_domain(h, todo[k].path, todo[k].path);
        if (idx != todo[k].dom_idx) {
            if (err && errlen)
                snprintf(err, errlen, "disk %s: cannot start health probe", todo[k].path);
            disk_probe_stop();
            return -1;
        }
    }
    (void)cluster_add_settings_watch(disk_on_settings, NULL);
    return 0;
}

void disk_probe_set_stall_s(uint32_t disk_stall_s)
{
    uint32_t s = disk_stall_s > 0 ? disk_stall_s : 10u;
    int ms = s > 3600u ? 3600000 : (int)(s * 1000u);
    pthread_mutex_lock(&g_mu);
    tsdb_health *h = probe_hold_locked();
    int ov = G.probe_down_override;
    if (h && !ov)
        G.probe_down_ms = ms;
    pthread_mutex_unlock(&g_mu);
    if (!h)
        return;
    if (!ov)
        tsdb_health_set_down_ms(h, ms);
    probe_release();
}

void disk_probe_now_store(const uint8_t store_id[16])
{
    if (!store_id)
        return;
    int dom_idx = -1;
    pthread_mutex_lock(&g_mu);
    tsdb_health *h = probe_hold_locked();
    for (int i = 0; h && i < G.ndisks; i++) {
        if (memcmp(G.disks[i].meta.store_id, store_id, 16) == 0) {
            dom_idx = G.disks[i].dom_idx;
            break;
        }
    }
    pthread_mutex_unlock(&g_mu);
    if (!h)
        return;
    if (dom_idx >= 0)
        tsdb_health_probe_now(h, dom_idx);
    probe_release();
}

void disk_probe_now_chunk(int chunk_slot)
{
    if (chunk_slot < 0)
        return;
    int dom_idx = -1;
    pthread_mutex_lock(&g_mu);
    tsdb_health *h = probe_hold_locked();
    for (int i = 0; h && i < G.ndisks; i++) {
        if (G.disks[i].chunk_slot == chunk_slot) {
            dom_idx = G.disks[i].dom_idx;
            break;
        }
    }
    pthread_mutex_unlock(&g_mu);
    if (!h)
        return;
    if (dom_idx >= 0)
        tsdb_health_probe_now(h, dom_idx);
    probe_release();
}

void disk_probe_set_up(int disk_idx)
{
    int dom_idx = -1;
    pthread_mutex_lock(&g_mu);
    tsdb_health *h = probe_hold_locked();
    if (h && disk_idx >= 0 && disk_idx < G.ndisks)
        dom_idx = G.disks[disk_idx].dom_idx;
    pthread_mutex_unlock(&g_mu);
    if (!h)
        return;
    if (dom_idx >= 0)
        tsdb_health_set_up(h, dom_idx);
    probe_release();
}

int disk_probe_state(int disk_idx)
{
    int dom_idx = -1;
    pthread_mutex_lock(&g_mu);
    tsdb_health *h = probe_hold_locked();
    if (h && disk_idx >= 0 && disk_idx < G.ndisks)
        dom_idx = G.disks[disk_idx].dom_idx;
    pthread_mutex_unlock(&g_mu);
    if (!h)
        return -1;
    int st = dom_idx >= 0 ? tsdb_health_state(h, dom_idx) : -1;
    probe_release();
    return st;
}

int disk_probe_info_by_store(const uint8_t store_id[16], tsdb_domain_info *out)
{
    if (!store_id || !out)
        return TSDB_ERR_NOTFOUND;
    int dom_idx = -1;
    pthread_mutex_lock(&g_mu);
    tsdb_health *h = probe_hold_locked();
    for (int i = 0; h && i < G.ndisks; i++) {
        if (memcmp(G.disks[i].meta.store_id, store_id, 16) == 0) {
            dom_idx = G.disks[i].dom_idx;
            break;
        }
    }
    pthread_mutex_unlock(&g_mu);
    if (!h)
        return TSDB_ERR_NOTFOUND;
    int rc = dom_idx >= 0 ? tsdb_health_info(h, dom_idx, out) : TSDB_ERR_NOTFOUND;
    probe_release();
    return rc;
}

void disk_probe_chunk_failed(int chunk_slot, int err_code)
{
    if (chunk_slot < 0)
        return;
    int dom_idx = -1;
    int changed = 0;
    int is_log = 0;
    char path[PATH_MAX] = "";
#ifdef TSDB_TEST_HOOKS
    int mut = DISKS_MUT_NONE;
#endif
    pthread_mutex_lock(&g_mu);
    tsdb_health *h = probe_hold_locked();
#ifdef TSDB_TEST_HOOKS
    mut = G.mutant;
#endif
    for (int i = 0; i < G.ndisks; i++) {
        if (G.disks[i].chunk_slot != chunk_slot)
            continue;
        struct disk_entry *d = &G.disks[i];
        is_log = (d->uses & META_DISK_USE_LOG) != 0;
        /* Only take the whole disk DOWN in HBS_DISKS/cvolfs when it is a chunk-only disk: on a fast log+chunk disk
         * (no capacity disks), keep d->state governed by the disk's health probe so ENOSPC or a chunks/-only error
         * never leaves healthy log volumes unscannable while chunksrv already marks the chunk slot DOWN_IOERR. */
        if (!is_log) {
            d->io_down = 1;
            if (d->state == META_STORE_UP) {
                d->state = META_STORE_DOWN;
                changed = 1;
            }
        }
        dom_idx = d->dom_idx;
        snprintf(path, sizeof path, "%s", d->path);
        break;
    }
    pthread_mutex_unlock(&g_mu);
    if (h) {
        if (dom_idx >= 0)
            tsdb_health_probe_now(h, dom_idx);
        probe_release();
    }

    if (changed || is_log) {
        fprintf(stderr, "tsdbd: disks: chunk store %d (%s) failed (%s): %s\n", chunk_slot, path[0] ? path : "-",
                err_code == CHUNK_ERR_FULL ? "ENOSPC" : tsdb_strerror(err_code),
                is_log ? "chunk store marked DOWN_IOERR (log disk remains governed by probe)" : "marking disk DOWN");
    }
#ifdef TSDB_TEST_HOOKS
    if (mut == DISKS_MUT_STOREEIOEXIT) {
        fprintf(stderr, "tsdbd: disks: MUTANT storeeioexit: exiting 1 on chunk store %d (%s) failure\n", chunk_slot,
                path[0] ? path : "-");
        fflush(stderr);
        _exit(1);
    }
#endif
    if (changed)
        cluster_beat_soon();
}

void disk_probe_stop(void)
{
    cluster_del_settings_watch(disk_on_settings, NULL);
    pthread_mutex_lock(&g_mu);
    if (!G.probe_h) {
        pthread_mutex_unlock(&g_mu);
        return;
    }
    G.probe_stopping = 1;
    while (G.probe_users > 0)
        pthread_cond_wait(&g_probe_cv, &g_mu);
    tsdb_health *h = G.probe_h;
    G.probe_h = NULL;
    G.probe_stopping = 0;
    G.probe_cb = NULL;
    G.probe_ctx = NULL;
    for (int i = 0; i < G.ndisks; i++)
        G.disks[i].dom_idx = -1;
    pthread_mutex_unlock(&g_mu);

    int nd = tsdb_health_domains(h);
    for (int d = 0; d < nd; d++) {
        if (tsdb_health_state(h, d) != TSDB_DOM_UP)
            return; /* a probe thread may be stuck inside a hung kernel call; process exits with it */
    }
    tsdb_health_close(h);
}

/* ---------------------------------------------------------------- local chunk drain driver (R13, r13-lmove; E50, §15.3) */

struct drain_item {
    uint32_t vol;
    uint8_t oid[16];
    uint16_t pos;
    uint64_t size;
};

struct drain_list {
    struct drain_item *v;
    size_t n, cap;
    uint32_t cur_vol;
    uint64_t total_bytes;
    int err;
};

static int drain_list_cb(void *ctx, const chunk_listed *c)
{
    struct drain_list *dl = ctx;
    if (dl->n == dl->cap) {
        size_t nc = dl->cap ? dl->cap * 2 : 64;
        struct drain_item *nv = realloc(dl->v, nc * sizeof *nv);
        if (!nv) {
            dl->err = TSDB_ERR_NOMEM;
            return 1;
        }
        dl->v = nv;
        dl->cap = nc;
    }
    struct drain_item *it = &dl->v[dl->n++];
    it->vol = dl->cur_vol;
    memcpy(it->oid, c->oid, 16);
    it->pos = c->pos;
    it->size = c->size;
    dl->total_bytes += c->size;
    return 0;
}

static int drain_is_stopping(void *ctx)
{
    (void)ctx;
    pthread_mutex_lock(&g_mu);
    int st = G.drain_stop;
    pthread_mutex_unlock(&g_mu);
    return st;
}

static void drain_recompute_no_space_locked(void)
{
    int any = 0;
    for (int i = 0; i < G.ndisks; i++) {
        if (G.disks[i].draining && G.disks[i].drain_no_space) {
            any = 1;
            break;
        }
    }
    G.drain_no_space_any = any;
}

/* Picks the eligible UP non-draining target chunk store with the most free room below its fill mark (E27/E50/§15.3).
 * Never drains onto a fast disk while any UP non-draining capacity (HDD) store exists on this server. */
static int drain_pick_target(chunk_backend *be, int src_slot, uint64_t need_bytes)
{
    struct {
        int slot;
        int cls;
    } cands[DISKS_MAX];
    int nc = 0;
    int have_hdd = 0;

    pthread_mutex_lock(&g_mu);
    for (int i = 0; i < G.ndisks && nc < DISKS_MAX; i++) {
        const struct disk_entry *d = &G.disks[i];
        if (!d->present || !d->has_chunks || d->draining || d->chunk_slot < 0 || d->chunk_slot == src_slot)
            continue;
        if (d->state != META_STORE_UP || d->io_down || d->probe_down)
            continue;
        cands[nc].slot = d->chunk_slot;
        cands[nc].cls = d->effective_class;
        if (d->effective_class == DISK_CLASS_HDD)
            have_hdd = 1;
        nc++;
    }
    pthread_mutex_unlock(&g_mu);

    int best = -1;
    uint64_t best_free = 0;
    for (int i = 0; i < nc; i++) {
        if (have_hdd && cands[i].cls != DISK_CLASS_HDD)
            continue;
        int slot = cands[i].slot;
        if (chunk_be_store_state(be, slot) != CHUNK_STORE_UP)
            continue;
        chunk_store *st = chunk_be_store_ref(be, slot);
        if (!st || chunk_store_failed(st)) {
            chunk_store_unref(st);
            continue;
        }
        chunk_store_fs_refresh(st);
        uint64_t used = 0, avail = 0, total = 0;
        int frc = chunk_store_fs_bytes_cached(st, &used, &avail, &total);
        chunk_store_unref(st);
        if (frc != 0 || total == 0 || avail <= need_bytes)
            continue;
        uint64_t proj_used = used <= UINT64_MAX - need_bytes ? used + need_bytes : UINT64_MAX;
        uint64_t proj_pct = (uint64_t)(((__uint128_t)proj_used * 100u) / total);
        if (proj_pct >= CHUNK_STORE_PUT_MAX_PCT)
            continue;
        if (best < 0 || avail > best_free) {
            best = slot;
            best_free = avail;
        }
    }
    return best;
}

static uint64_t drain_current_rate_bps(void)
{
#ifdef TSDB_TEST_HOOKS
    const char *ov = getenv("TSDB_DRAIN_RATE_BPS");
    if (ov && *ov) {
        unsigned long long v = strtoull(ov, NULL, 10);
        if (v > 0)
            return (uint64_t)v;
    }
#endif
    uint32_t mb_s = 0;
    const struct cluster_copy *c = cluster_copy_get();
    if (c) {
        mb_s = c->balance_mb_s > 0 ? c->balance_mb_s : c->rebuild_mb_s;
        cluster_copy_put(c);
    }
    return mb_s > 0 ? ((uint64_t)mb_s << 20) : ((uint64_t)CHUNKMOVE_DEFAULT_MB_S << 20);
}

static void disks_store_extra_cb(void *arg, uint32_t store, uint64_t *scrub_age_s, uint64_t *drain_bytes_left)
{
    (void)arg;
    (void)scrub_age_s;
    if (!drain_bytes_left)
        return;
    *drain_bytes_left = 0;
    if (!atomic_load_explicit(&g_disks_started, memory_order_acquire))
        return;
    pthread_mutex_lock(&g_mu);
    for (int i = 0; i < G.ndisks; i++) {
        if (G.disks[i].chunk_slot == (int)store) {
            *drain_bytes_left = G.disks[i].drain_bytes_left;
            break;
        }
    }
    pthread_mutex_unlock(&g_mu);
}

static void disks_collect_drain_bytes(void *ctx, struct creport_sink *sink)
{
    (void)ctx;
    if (!atomic_load_explicit(&g_disks_started, memory_order_acquire))
        return;
    struct {
        uint8_t store_id[16];
        int cls;
        uint64_t left;
    } snap[DISKS_MAX];
    int n = 0;
    pthread_mutex_lock(&g_mu);
    if (G.started) {
        for (int i = 0; i < G.ndisks && n < DISKS_MAX; i++) {
            const struct disk_entry *d = &G.disks[i];
            if (!d->draining && d->drain_bytes_left == 0)
                continue;
            memcpy(snap[n].store_id, d->meta.store_id, 16);
            snap[n].cls = d->effective_class;
            snap[n].left = d->drain_bytes_left;
            n++;
        }
    }
    pthread_mutex_unlock(&g_mu);
    for (int i = 0; i < n; i++) {
        char hex[33];
        chunk_oid_hex(snap[i].store_id, hex);
        struct creport_label lbl[2] = {{"store", hex}, {"class", meta_disk_class_name(snap[i].cls)}};
        creport_sink_put_u64(sink, lbl, 2, snap[i].left);
    }
}

static void disks_collect_drain_moved(void *ctx, struct creport_sink *sink)
{
    (void)ctx;
    if (!atomic_load_explicit(&g_disks_started, memory_order_acquire))
        return;
    struct {
        uint8_t store_id[16];
        int cls;
        uint64_t moved;
    } snap[DISKS_MAX];
    int n = 0;
    pthread_mutex_lock(&g_mu);
    if (G.started) {
        for (int i = 0; i < G.ndisks && n < DISKS_MAX; i++) {
            const struct disk_entry *d = &G.disks[i];
            if (!d->draining && d->drain_chunks_moved == 0)
                continue;
            memcpy(snap[n].store_id, d->meta.store_id, 16);
            snap[n].cls = d->effective_class;
            snap[n].moved = d->drain_chunks_moved;
            n++;
        }
    }
    pthread_mutex_unlock(&g_mu);
    for (int i = 0; i < n; i++) {
        char hex[33];
        chunk_oid_hex(snap[i].store_id, hex);
        struct creport_label lbl[2] = {{"store", hex}, {"class", meta_disk_class_name(snap[i].cls)}};
        creport_sink_put_u64(sink, lbl, 2, snap[i].moved);
    }
}

static void disks_collect_drain_no_space(void *ctx, struct creport_sink *sink)
{
    (void)ctx;
    if (!atomic_load_explicit(&g_disks_started, memory_order_acquire))
        return;
    pthread_mutex_lock(&g_mu);
    int ns = G.drain_no_space_any;
    int started = G.started;
    pthread_mutex_unlock(&g_mu);
    if (started)
        creport_sink_put_u64(sink, NULL, 0, ns ? 1u : 0u);
}

/* Rate-limit per-chunk / per-volume drain error logs (owned exclusively by the single drain thread). */
enum { DRAIN_ERR_LOG_SLOTS = 64 };
struct drain_err_ent {
    int used;
    int slot;
    uint32_t vol;
    uint8_t oid[16];
    uint16_t pos;
    int rc;
    uint64_t last_ns;
};

static uint64_t drain_now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static bool drain_should_log_err(struct drain_err_ent tab[DRAIN_ERR_LOG_SLOTS], size_t *next_evict, int slot,
                                 uint32_t vol, const uint8_t oid[16], uint16_t pos, int rc)
{
    uint64_t now = drain_now_ns();
    const uint64_t cooldown_ns = 300ULL * 1000000000ULL;
    for (size_t i = 0; i < DRAIN_ERR_LOG_SLOTS; i++) {
        if (!tab[i].used)
            continue;
        if (tab[i].slot == slot && tab[i].vol == vol && tab[i].pos == pos && tab[i].rc == rc &&
            memcmp(tab[i].oid, oid, 16) == 0) {
            if (now - tab[i].last_ns < cooldown_ns)
                return false;
            tab[i].last_ns = now;
            return true;
        }
    }
    size_t idx = *next_evict % DRAIN_ERR_LOG_SLOTS;
    for (size_t i = 0; i < DRAIN_ERR_LOG_SLOTS; i++) {
        if (!tab[i].used) {
            idx = i;
            break;
        }
    }
    *next_evict = (idx + 1u) % DRAIN_ERR_LOG_SLOTS;
    tab[idx].used = 1;
    tab[idx].slot = slot;
    tab[idx].vol = vol;
    memcpy(tab[idx].oid, oid, 16);
    tab[idx].pos = pos;
    tab[idx].rc = rc;
    tab[idx].last_ns = now;
    return true;
}

static bool vol_in_list(const uint32_t *vols, size_t n, uint32_t vol)
{
    for (size_t i = 0; i < n; i++)
        if (vols[i] == vol)
            return true;
    return false;
}

static void *disks_drain_main(void *arg)
{
    (void)arg;
    chunk_move_pacer pacer;
    chunk_move_pacer_init(&pacer, drain_current_rate_bps());
    struct drain_err_ent err_log[DRAIN_ERR_LOG_SLOTS];
    memset(err_log, 0, sizeof err_log);
    size_t err_evict = 0;
    int64_t backoff_ms = 250;

    for (;;) {
        if (drain_is_stopping(NULL))
            break;

        struct chunksrv *cs = chunksrv_cluster_hold();
        if (!cs) {
            pthread_mutex_lock(&g_mu);
            if (!G.drain_stop) {
                struct timespec ts;
                tsdb_cond_deadline_ns(&ts, 100 * 1000000LL);
                pthread_cond_timedwait(&G.drain_cv, &g_mu, &ts);
            }
            int stop = G.drain_stop;
            pthread_mutex_unlock(&g_mu);
            if (stop)
                break;
            continue;
        }
        chunk_backend *be = chunksrv_backend(cs);
        if (!be) {
            chunksrv_cluster_release();
            pthread_mutex_lock(&g_mu);
            if (!G.drain_stop) {
                struct timespec ts;
                tsdb_cond_deadline_ns(&ts, 500 * 1000000LL);
                pthread_cond_timedwait(&G.drain_cv, &g_mu, &ts);
            }
            int stop = G.drain_stop;
            pthread_mutex_unlock(&g_mu);
            if (stop)
                break;
            continue;
        }

        /* Snapshot draining disk slots under g_mu. */
        struct {
            int disk_idx;
            int slot;
            char path[PATH_MAX];
        } todo[DISKS_MAX];
        int ntodo = 0;
        pthread_mutex_lock(&g_mu);
        for (int i = 0; i < G.ndisks && ntodo < DISKS_MAX; i++) {
            const struct disk_entry *d = &G.disks[i];
            if (d->draining && d->present && d->has_chunks && d->chunk_slot >= 0 && d->state == META_STORE_UP &&
                !d->io_down && !d->probe_down) {
                todo[ntodo].disk_idx = i;
                todo[ntodo].slot = d->chunk_slot;
                snprintf(todo[ntodo].path, sizeof todo[ntodo].path, "%s", d->path);
                ntodo++;
            }
        }
        pthread_mutex_unlock(&g_mu);

        bool moved_any = false;

        for (int t = 0; t < ntodo && !drain_is_stopping(NULL); t++) {
            int di = todo[t].disk_idx;
            int src_slot = todo[t].slot;
            if (chunk_be_store_state(be, src_slot) != CHUNK_STORE_UP)
                continue;
            chunk_store *st_src = chunk_be_store_ref(be, src_slot);
            if (!st_src || chunk_store_failed(st_src)) {
                chunk_store_unref(st_src);
                continue;
            }
            uint32_t *vols = NULL;
            uint64_t *eps = NULL;
            size_t nv = 0;
            struct drain_list dl = {0};
            int list_rc = chunk_store_volumes(st_src, &vols, &eps, &nv);
            if (list_rc == 0) {
                for (size_t vi = 0; vi < nv && dl.err == 0; vi++) {
                    dl.cur_vol = vols[vi];
                    int lrc = chunk_store_list(st_src, vols[vi], false, drain_list_cb, &dl);
                    if (lrc != 0) {
                        list_rc = lrc;
                        break;
                    }
                }
            }
            if (list_rc == 0 && dl.err != 0)
                list_rc = dl.err;
            free(vols);
            free(eps);
            chunk_store_unref(st_src);

            /* Never treat a listing/OOM error as an empty store or "drain complete": keep previous metrics. */
            if (list_rc != 0) {
                static const uint8_t zero_oid[16] = {0};
                if (drain_should_log_err(err_log, &err_evict, src_slot, 0, zero_oid, 0, list_rc)) {
                    fprintf(stderr,
                            "tsdbd: disks: local drain of store %d (%s): listing failed (%d); will retry\n",
                            src_slot, todo[t].path, list_rc);
                }
                free(dl.v);
                continue;
            }

            bool beat = false;
            bool log_start = false;
            bool log_done = false;
            uint64_t done_chunks = 0, done_bytes = 0;
            pthread_mutex_lock(&g_mu);
            struct disk_entry *de = &G.disks[di];
            uint64_t prev_left = de->drain_bytes_left;
            int prev_ns = G.drain_no_space_any;
            de->drain_bytes_left = dl.total_bytes;
            if (dl.n == 0) {
                de->drain_no_space = 0;
                de->drain_no_space_logged = 0;
                drain_recompute_no_space_locked();
                if (prev_left > 0 || prev_ns != G.drain_no_space_any)
                    beat = true;
                if (de->drain_chunks_moved > 0 && de->drain_done_logged != de->drain_chunks_moved) {
                    de->drain_done_logged = de->drain_chunks_moved;
                    done_chunks = de->drain_chunks_moved;
                    done_bytes = de->drain_bytes_moved;
                    log_done = true;
                }
            } else {
                if (!de->drain_started_logged) {
                    de->drain_started_logged = 1;
                    log_start = true;
                }
                if (prev_left == 0)
                    beat = true;
            }
            pthread_mutex_unlock(&g_mu);
            if (log_start) {
                fprintf(stderr,
                        "tsdbd: disks: local drain of store %d (%s) started (%zu chunks, %" PRIu64 " bytes)\n",
                        src_slot, todo[t].path, dl.n, dl.total_bytes);
            }
            if (log_done) {
                fprintf(stderr,
                        "tsdbd: disks: local drain of store %d (%s) complete (%" PRIu64 " chunks, %" PRIu64
                        " bytes moved)\n",
                        src_slot, todo[t].path, done_chunks, done_bytes);
            }
            if (beat)
                cluster_beat_soon();
            if (dl.n == 0) {
                free(dl.v);
                continue;
            }

            uint32_t unsure_vols[64];
            size_t n_unsure = 0;
            uint64_t left = dl.total_bytes;
            for (size_t k = 0; k < dl.n && !drain_is_stopping(NULL); k++) {
                pthread_mutex_lock(&g_mu);
                int still_draining = G.disks[di].draining;
                pthread_mutex_unlock(&g_mu);
                if (!still_draining)
                    break;
                if (vol_in_list(unsure_vols, n_unsure, dl.v[k].vol))
                    continue;

                chunk_move_pacer_set_rate(&pacer, drain_current_rate_bps());
                int dst_slot = drain_pick_target(be, src_slot, dl.v[k].size);
                if (dst_slot < 0) {
                    bool do_log = false, do_beat = false;
                    pthread_mutex_lock(&g_mu);
                    int prev_ns = G.drain_no_space_any;
                    G.disks[di].drain_no_space = 1;
                    G.disks[di].drain_bytes_left = left;
                    drain_recompute_no_space_locked();
                    if (!G.disks[di].drain_no_space_logged) {
                        G.disks[di].drain_no_space_logged = 1;
                        do_log = true;
                    }
                    if (prev_ns != G.drain_no_space_any)
                        do_beat = true;
                    pthread_mutex_unlock(&g_mu);
                    if (do_log) {
                        fprintf(stderr,
                                "tsdbd: disks: local drain of store %d (%s) stopped: no target chunk store has room "
                                "(%" PRIu64 " bytes remaining)\n",
                                src_slot, todo[t].path, left);
                    }
                    if (do_beat)
                        cluster_beat_soon();
                    break;
                }

                chunk_ident id = {.volume = dl.v[k].vol, .pos = dl.v[k].pos};
                memcpy(id.oid, dl.v[k].oid, 16);
                uint64_t moved_b = 0;
                chunk_move_opts mo = {
                    .pacer = &pacer,
                    .cancel = drain_is_stopping,
                    .bytes_moved = &moved_b,
                };

                chunksrv_scrub_pause_all(cs, 1);
                int mrc = chunk_move(be, &id, src_slot, dst_slot, &mo);
                chunksrv_scrub_pause_all(cs, -1);

                if (mrc == 0) {
                    moved_any = true;
                    left = left > dl.v[k].size ? left - dl.v[k].size : 0;
                    bool do_beat = false;
                    pthread_mutex_lock(&g_mu);
                    int prev_ns = G.drain_no_space_any;
                    G.disks[di].drain_no_space = 0;
                    G.disks[di].drain_no_space_logged = 0;
                    G.disks[di].drain_bytes_left = left;
                    G.disks[di].drain_chunks_moved++;
                    G.disks[di].drain_bytes_moved += moved_b;
                    drain_recompute_no_space_locked();
                    if (prev_ns != G.drain_no_space_any)
                        do_beat = true;
                    pthread_mutex_unlock(&g_mu);
                    if (do_beat)
                        cluster_beat_soon();
                } else if (mrc == TSDB_ERR_NOTFOUND) {
                    left = left > dl.v[k].size ? left - dl.v[k].size : 0;
                    pthread_mutex_lock(&g_mu);
                    G.disks[di].drain_bytes_left = left;
                    pthread_mutex_unlock(&g_mu);
                } else if (mrc == CHUNK_ERR_FULL) {
                    bool do_log = false, do_beat = false;
                    pthread_mutex_lock(&g_mu);
                    int prev_ns = G.drain_no_space_any;
                    G.disks[di].drain_no_space = 1;
                    G.disks[di].drain_bytes_left = left;
                    drain_recompute_no_space_locked();
                    if (!G.disks[di].drain_no_space_logged) {
                        G.disks[di].drain_no_space_logged = 1;
                        do_log = true;
                    }
                    if (prev_ns != G.drain_no_space_any)
                        do_beat = true;
                    pthread_mutex_unlock(&g_mu);
                    if (do_log) {
                        fprintf(stderr,
                                "tsdbd: disks: local drain of store %d (%s) stopped: target store %d full "
                                "(%" PRIu64 " bytes remaining)\n",
                                src_slot, todo[t].path, dst_slot, left);
                    }
                    if (do_beat)
                        cluster_beat_soon();
                    break;
                } else if (mrc == CHUNK_ERR_EPOCH_UNSURE) {
                    /* Skip this unsure volume for the rest of the pass so all other sure volumes finish draining. */
                    if (n_unsure < 64 && !vol_in_list(unsure_vols, n_unsure, dl.v[k].vol))
                        unsure_vols[n_unsure++] = dl.v[k].vol;
                    static const uint8_t zero_oid[16] = {0};
                    if (drain_should_log_err(err_log, &err_evict, src_slot, dl.v[k].vol, zero_oid, 0, mrc)) {
                        fprintf(stderr,
                                "tsdbd: disks: local drain of store %d: skipping volume %" PRIu32
                                " while EPOCH is unsure (other volumes continue)\n",
                                src_slot, dl.v[k].vol);
                    }
                } else if (mrc == TSDB_ERR_STOPPED) {
                    break;
                } else {
                    if (drain_should_log_err(err_log, &err_evict, src_slot, dl.v[k].vol, dl.v[k].oid, dl.v[k].pos,
                                             mrc)) {
                        char hex[33];
                        chunk_oid_hex(dl.v[k].oid, hex);
                        fprintf(stderr,
                                "tsdbd: disks: local drain of store %d: vol %" PRIu32 " oid %s pos %u failed (%d); "
                                "leaving source intact\n",
                                src_slot, dl.v[k].vol, hex, (unsigned)dl.v[k].pos, mrc);
                    }
                }
            }
            free(dl.v);
        }

        chunksrv_cluster_release();

        /* If we moved chunks in this pass, reset backoff and re-scan right away to confirm 0 remaining. */
        if (moved_any && !drain_is_stopping(NULL)) {
            backoff_ms = 250;
            continue;
        }

        pthread_mutex_lock(&g_mu);
        if (!G.drain_stop && !G.drain_kick) {
            struct timespec ts;
            tsdb_cond_deadline_ns(&ts, backoff_ms * 1000000LL);
            pthread_cond_timedwait(&G.drain_cv, &g_mu, &ts);
        }
        int kicked = G.drain_kick;
        G.drain_kick = 0;
        int stop = G.drain_stop;
        pthread_mutex_unlock(&g_mu);
        if (stop)
            break;
        if (kicked) {
            backoff_ms = 250;
        } else {
            backoff_ms *= 2;
            if (backoff_ms > 10000)
                backoff_ms = 10000;
        }
    }
    return NULL;
}

int disks_mark_draining(const uint8_t store_id[16], int draining)
{
    if (!store_id)
        return -1;
    int slot = -1;
    int spawn = 0;
    pthread_mutex_lock(&g_mu);
    for (int i = 0; i < G.ndisks; i++) {
        if (memcmp(G.disks[i].meta.store_id, store_id, 16) != 0)
            continue;
        if (!G.disks[i].has_chunks || G.disks[i].chunk_slot < 0) {
            pthread_mutex_unlock(&g_mu);
            return -1;
        }
        slot = G.disks[i].chunk_slot;
        G.disks[i].draining = draining ? 1 : 0;
        if (!draining) {
            G.disks[i].drain_no_space = 0;
            G.disks[i].drain_no_space_logged = 0;
            G.disks[i].drain_bytes_left = 0;
            drain_recompute_no_space_locked();
        }
        if (draining && G.drain_started && !G.drain_stop && !G.drain_thr_running)
            spawn = 1;
        if (G.drain_started) {
            G.drain_kick = 1;
            pthread_cond_broadcast(&G.drain_cv);
        }
        break;
    }
    if (spawn) {
        if (pthread_create(&G.drain_thr, NULL, disks_drain_main, NULL) == 0)
            G.drain_thr_running = 1;
    }
    pthread_mutex_unlock(&g_mu);
    if (slot < 0)
        return -1;
    struct chunksrv *cs = chunksrv_cluster_hold();
    if (cs) {
        chunk_backend *be = chunksrv_backend(cs);
        if (be)
            chunk_be_set_draining(be, slot, draining != 0);
        chunksrv_cluster_release();
    }
    cluster_beat_soon();
    return 0;
}

int disks_drain_start(void)
{
    pthread_mutex_lock(&g_mu);
    if (!G.started || G.drain_started) {
        pthread_mutex_unlock(&g_mu);
        return 0;
    }
    tsdb_cond_init_mono(&G.drain_cv);
    G.drain_stop = 0;
    G.drain_no_space_any = 0;
    G.drain_started = 1;
    int any_draining = 0;
    for (int i = 0; i < G.ndisks; i++) {
        if (G.disks[i].draining && G.disks[i].present && G.disks[i].has_chunks) {
            any_draining = 1;
            break;
        }
    }
    if (any_draining) {
        if (pthread_create(&G.drain_thr, NULL, disks_drain_main, NULL) != 0) {
            G.drain_started = 0;
            pthread_cond_destroy(&G.drain_cv);
            pthread_mutex_unlock(&g_mu);
            return -1;
        }
        G.drain_thr_running = 1;
    }
    pthread_mutex_unlock(&g_mu);

    creport_progress_set_store_extra(disks_store_extra_cb, NULL);
    (void)creport_metric_collector("tsdb_disk_drain_bytes_left", CREPORT_GAUGE,
                                   "Remaining chunk bytes to drain locally off this disk.",
                                   disks_collect_drain_bytes, NULL);
    (void)creport_metric_collector("tsdb_disk_drain_chunks_moved_total", CREPORT_COUNTER,
                                   "Total chunks moved locally off this draining disk since process start.",
                                   disks_collect_drain_moved, NULL);
    (void)creport_metric_collector("tsdb_disk_drain_no_space", CREPORT_GAUGE,
                                   "1 while any local chunk store drain stopped because no target store has room.",
                                   disks_collect_drain_no_space, NULL);
    return 0;
}

void disks_drain_stop(void)
{
    pthread_mutex_lock(&g_mu);
    if (!G.drain_started) {
        pthread_mutex_unlock(&g_mu);
        return;
    }
    G.drain_stop = 1;
    pthread_cond_broadcast(&G.drain_cv);
    int running = G.drain_thr_running;
    pthread_t thr = G.drain_thr;
    G.drain_thr_running = 0;
    G.drain_started = 0;
    pthread_mutex_unlock(&g_mu);

    if (running)
        pthread_join(thr, NULL);

    pthread_mutex_lock(&g_mu);
    pthread_cond_destroy(&G.drain_cv);
    pthread_mutex_unlock(&g_mu);

    creport_progress_set_store_extra(NULL, NULL);
    creport_metric_collector_del("tsdb_disk_drain_bytes_left");
    creport_metric_collector_del("tsdb_disk_drain_chunks_moved_total");
    creport_metric_collector_del("tsdb_disk_drain_no_space");
}

/* ---------------------------------------------------------------- per-disk usage & fill marks (R13, r13-fill; E30, E50, E53, §15.4) */

struct raw_disk_uses {
    uint64_t size_bytes;
    uint64_t fs_used;
    uint64_t young;
    uint64_t retained;
    uint64_t retired;
    uint64_t local;
    uint64_t index;
    uint64_t chunks;
};

static int has_suffix(const char *s, const char *suf)
{
    size_t sl = strlen(s), fl = strlen(suf);
    return sl >= fl && strcmp(s + sl - fl, suf) == 0;
}

static int parse_wal_seq(const char *name, uint64_t *seq_out)
{
    if (strncmp(name, "wal_", 4) != 0 || !has_suffix(name, ".log"))
        return 0;
    char *end = NULL;
    errno = 0;
    unsigned long long v = strtoull(name + 4, &end, 10);
    if (errno != 0 || !end || strcmp(end, ".log") != 0)
        return 0;
    *seq_out = (uint64_t)v;
    return 1;
}

/* Walk a single v<N> (or sibling) directory off any engine lock and classify bytes by use. */
static void walk_vol_dir(int parent_fd, const char *dname, struct raw_disk_uses *u)
{
    int dfd = openat(parent_fd, dname, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (dfd < 0)
        return;
    DIR *dir = fdopendir(dfd);
    if (!dir) {
        close(dfd);
        return;
    }
    struct wal_item {
        uint64_t seq;
        uint64_t bytes;
    } stack_wals[32];
    struct wal_item *wals = stack_wals;
    size_t nwals = 0, wal_cap = 32;
    uint64_t max_wal_seq = 0;

    for (struct dirent *e; (e = readdir(dir)) != NULL;) {
        const char *nm = e->d_name;
        if (nm[0] == '.' && (nm[1] == '\0' || (nm[1] == '.' && nm[2] == '\0')))
            continue;
        struct stat st;
        if (fstatat(dfd, nm, &st, AT_SYMLINK_NOFOLLOW) != 0 || !S_ISREG(st.st_mode))
            continue;
        /* Skip keep_ hard links of listed parts so the same inode is not counted twice. */
        if (strncmp(nm, "keep_", 5) == 0 && st.st_nlink > 1)
            continue;
        uint64_t sz = st.st_size > 0 ? (uint64_t)st.st_size : 0;
        if (strncmp(nm, "settled_", 8) == 0 || strncmp(nm, "retired_", 8) == 0 || strncmp(nm, "digest_", 7) == 0) {
            u->retired = sat_add64(u->retired, sz);
        } else if (strncmp(nm, "local_", 6) == 0) {
            u->local = sat_add64(u->local, sz);
        } else if (nm[0] == 'p' && (has_suffix(nm, ".ec") || has_suffix(nm, ".ec.tmp"))) {
            u->index = sat_add64(u->index, sz);
        } else if (strncmp(nm, "fwal_", 5) == 0 && has_suffix(nm, ".log")) {
            u->retained = sat_add64(u->retained, sz);
        } else {
            uint64_t wseq = 0;
            if (parse_wal_seq(nm, &wseq)) {
                if (wseq > max_wal_seq)
                    max_wal_seq = wseq;
                if (nwals == wal_cap) {
                    size_t ncap = wal_cap * 2;
                    struct wal_item *nw = (wals == stack_wals) ? malloc(ncap * sizeof *nw) : realloc(wals, ncap * sizeof *nw);
                    if (nw) {
                        if (wals == stack_wals)
                            memcpy(nw, stack_wals, nwals * sizeof *nw);
                        wals = nw;
                        wal_cap = ncap;
                    } else {
                        u->young = sat_add64(u->young, sz);
                        continue;
                    }
                }
                wals[nwals].seq = wseq;
                wals[nwals].bytes = sz;
                nwals++;
            } else {
                u->young = sat_add64(u->young, sz);
            }
        }
    }
    closedir(dir);

    for (size_t i = 0; i < nwals; i++) {
        if (wals[i].seq < max_wal_seq)
            u->retained = sat_add64(u->retained, wals[i].bytes);
        else
            u->young = sat_add64(u->young, wals[i].bytes);
    }
    if (wals != stack_wals)
        free(wals);
}

static uint64_t walk_dir_bytes(const char *path, int depth)
{
    if (!path || !*path || depth > 3)
        return 0;
    int dfd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd < 0)
        return 0;
    DIR *dir = fdopendir(dfd);
    if (!dir) {
        close(dfd);
        return 0;
    }
    uint64_t sum = 0;
    for (struct dirent *e; (e = readdir(dir)) != NULL;) {
        const char *nm = e->d_name;
        if (nm[0] == '.' && (nm[1] == '\0' || (nm[1] == '.' && nm[2] == '\0')))
            continue;
        struct stat st;
        if (fstatat(dfd, nm, &st, AT_SYMLINK_NOFOLLOW) != 0)
            continue;
        if (S_ISREG(st.st_mode)) {
            if (st.st_size > 0)
                sum = sat_add64(sum, (uint64_t)st.st_size);
        } else if (S_ISDIR(st.st_mode) && depth < 3) {
            char sub[PATH_MAX];
            if (snprintf(sub, sizeof sub, "%s/%s", path, nm) < (int)sizeof sub)
                sum = sat_add64(sum, walk_dir_bytes(sub, depth + 1));
        }
    }
    closedir(dir);
    return sum;
}

static void walk_disk_raw(const char *path, uint8_t uses, int has_chunks, const char *chunks_path, int allow_shared,
                          struct raw_disk_uses *u)
{
    memset(u, 0, sizeof *u);
    struct statvfs sv;
    if (statvfs(path, &sv) == 0 && sv.f_blocks > 0) {
        uint64_t unit = sv.f_frsize ? (uint64_t)sv.f_frsize : (uint64_t)sv.f_bsize;
        uint64_t total = (uint64_t)sv.f_blocks * unit;
        uint64_t avail = (uint64_t)sv.f_bavail * unit;
        u->size_bytes = total > 0 ? total : 1;
        u->fs_used = total > avail ? total - avail : 0;
    } else {
        u->size_bytes = 1;
    }
    if (uses & META_DISK_USE_LOG) {
        int dfd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (dfd >= 0) {
            DIR *dir = fdopendir(dfd);
            if (dir) {
                for (struct dirent *e; (e = readdir(dir)) != NULL;) {
                    const char *nm = e->d_name;
                    if (nm[0] != 'v' || nm[1] < '0' || nm[1] > '9')
                        continue;
                    /* Skip background-deleting .removed-* siblings so already-removed replicas never inflate fill. */
                    if (strstr(nm, ".removed-") != NULL)
                        continue;
                    struct stat st;
                    if (fstatat(dfd, nm, &st, AT_SYMLINK_NOFOLLOW) == 0 && S_ISDIR(st.st_mode))
                        walk_vol_dir(dfd, nm, u);
                }
                closedir(dir);
            } else {
                close(dfd);
            }
        }
    }
    if (has_chunks && chunks_path && chunks_path[0]) {
        if (!allow_shared) {
            /* Dedicated disk (production): derive chunk bytes in O(1) from statvfs fs_used minus walked young-tier
             * bytes instead of walking potentially millions of chunk files every 10 s. */
            uint64_t non_chunk = sat_add64(sat_add64(u->young, u->retained),
                                           sat_add64(sat_add64(u->retired, u->local), u->index));
            u->chunks = u->fs_used > non_chunk ? u->fs_used - non_chunk : 0;
        } else {
            u->chunks = walk_dir_bytes(chunks_path, 0);
        }
    }
}

/* Cluster-wide chunk fill mark in ppm per §5.6 / E53 (matching server/place.c:chunk_fill_mark_ppm). */
static uint64_t compute_cluster_chunk_mark_ppm(const struct cluster_copy *cp)
{
    if (!cp || cp->nmembers == 0)
        return 900000ull;
    int all_racked = 1;
    uint64_t total_cap = 0;
    uint32_t nstorage = 0;
    for (uint32_t i = 0; i < cp->nmembers; i++) {
        const struct cluster_copy_member *mb = &cp->members[i];
        if (mb->state >= META_OUT || mb->role == META_ROLE_COMPUTE || mb->ndisks == 0)
            continue;
        uint64_t mcap = 0;
        for (uint32_t d = 0; d < mb->ndisks && d < META_DISKS_MAX; d++) {
            const struct meta_disk *dk = &mb->disks[d];
            if (dk->state == META_STORE_UP && (dk->uses & META_DISK_USE_CHUNK))
                mcap = sat_add64(mcap, dk->size_bytes);
        }
        if (mcap == 0)
            continue;
        nstorage++;
        total_cap = sat_add64(total_cap, mcap);
        if (!mb->rack[0])
            all_racked = 0;
    }
    if (total_cap == 0 || nstorage <= 1)
        return 900000ull;
    uint64_t max_dom = 0;
    for (uint32_t i = 0; i < cp->nmembers; i++) {
        const struct cluster_copy_member *mb = &cp->members[i];
        if (mb->state >= META_OUT || mb->role == META_ROLE_COMPUTE || mb->ndisks == 0)
            continue;
        uint64_t dcap = 0;
        if (all_racked) {
            int seen = 0;
            for (uint32_t j = 0; j < i; j++) {
                if (cp->members[j].state < META_OUT && cp->members[j].role != META_ROLE_COMPUTE &&
                    cp->members[j].ndisks > 0 && strcmp(cp->members[j].rack, mb->rack) == 0) {
                    seen = 1;
                    break;
                }
            }
            if (seen)
                continue;
            for (uint32_t j = i; j < cp->nmembers; j++) {
                const struct cluster_copy_member *mj = &cp->members[j];
                if (mj->state >= META_OUT || mj->role == META_ROLE_COMPUTE || mj->ndisks == 0 ||
                    strcmp(mj->rack, mb->rack) != 0)
                    continue;
                for (uint32_t d = 0; d < mj->ndisks && d < META_DISKS_MAX; d++) {
                    const struct meta_disk *dk = &mj->disks[d];
                    if (dk->state == META_STORE_UP && (dk->uses & META_DISK_USE_CHUNK))
                        dcap = sat_add64(dcap, dk->size_bytes);
                }
            }
        } else {
            for (uint32_t d = 0; d < mb->ndisks && d < META_DISKS_MAX; d++) {
                const struct meta_disk *dk = &mb->disks[d];
                if (dk->state == META_STORE_UP && (dk->uses & META_DISK_USE_CHUNK))
                    dcap = sat_add64(dcap, dk->size_bytes);
            }
        }
        if (dcap > max_dom)
            max_dom = dcap;
    }
    if (max_dom >= total_cap)
        return 900000ull;
    uint64_t ppm = (uint64_t)(((__uint128_t)950000ull * (total_cap - max_dom)) / total_cap);
    return ppm > 950000ull ? 950000ull : ppm;
}

static uint64_t usage_now_hour(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ((uint64_t)ts.tv_sec / 3600ull) + 1ull; /* >= 1 so 0 means empty slot */
}

static uint64_t update_peak_7d_locked(struct disk_entry *d, uint64_t cur_young, uint64_t now_hr)
{
    uint32_t slot = (uint32_t)(now_hr % DISK_PEAK_HOURS);
    if (d->peak_hour_stamp[slot] != now_hr) {
        d->peak_hour_stamp[slot] = now_hr;
        d->peak_hour_val[slot] = cur_young;
    } else if (cur_young > d->peak_hour_val[slot]) {
        d->peak_hour_val[slot] = cur_young;
    }
    uint64_t peak = cur_young;
    for (uint32_t k = 0; k < DISK_PEAK_HOURS; k++) {
        if (d->peak_hour_stamp[k] != 0 && now_hr >= d->peak_hour_stamp[k] &&
            now_hr - d->peak_hour_stamp[k] < DISK_PEAK_HOURS && d->peak_hour_val[k] > peak)
            peak = d->peak_hour_val[k];
    }
    d->usage_peak_7d = peak;
    return peak;
}

/* Per-disk detachable usage sampler worker (R13 r13-fill; modeled on src/tierfill.c).
 * Each present disk runs on its own 256 KiB worker thread so a stalled/hung disk can NEVER stall fill accounting,
 * FULL transitions, or shutdown for any other disk. Non-UP / probe-down / io-down disks are skipped before touching
 * the filesystem; disk_usage_stop() waits up to 500 ms and detaches any kernel-stuck worker cleanly. */
struct disk_usage_worker {
    atomic_int refs;
    pthread_mutex_t mu; /* pure leaf mutex: NEVER held across statvfs, directory walk, g_mu, or engine calls */
    pthread_cond_t cv;
    pthread_t th;
    int disk_idx;
    uint8_t store_id[16];
    char path[PATH_MAX];
    char chunks_path[PATH_MAX];
    tsdb *db; /* cleared to NULL under mu by disk_usage_stop() before db can be closed */
    struct disk_usage_hooks hooks;
    bool stop;
    bool kick;
    bool done;
};

static void usage_worker_unref(struct disk_usage_worker *w)
{
    if (!w || atomic_fetch_sub_explicit(&w->refs, 1, memory_order_acq_rel) != 1)
        return;
    pthread_cond_destroy(&w->cv);
    pthread_mutex_destroy(&w->mu);
    free(w);
}

static void disk_usage_sample_one(struct disk_usage_worker *w)
{
    int allow_shared = 0, has_chunks = 0;
#ifdef TSDB_TEST_HOOKS
    int eff_class = 0;
#endif
    uint8_t uses = 0;
    uint8_t store_id[16];
    int eligible = 0;

    pthread_mutex_lock(&g_mu);
    if (G.started && w->disk_idx >= 0 && w->disk_idx < G.ndisks) {
        const struct disk_entry *d = &G.disks[w->disk_idx];
        /* E57 / r13-fault: NEVER touch a missing, DOWN, probe-down, or io-down disk. */
        if (d->present && d->path[0] && d->state == META_STORE_UP && !d->io_down && !d->probe_down &&
            memcmp(d->meta.store_id, w->store_id, 16) == 0) {
            allow_shared = G.allow_shared;
            has_chunks = d->has_chunks;
#ifdef TSDB_TEST_HOOKS
            eff_class = d->effective_class;
#endif
            uses = d->uses ? d->uses : META_DISK_USE_LOG;
            memcpy(store_id, d->meta.store_id, 16);
            eligible = 1;
        }
    }
    pthread_mutex_unlock(&g_mu);
    if (!eligible)
        return;

    /* Walk this single disk off g_mu and off any engine lock. */
    struct raw_disk_uses ru;
    walk_disk_raw(w->path, uses, has_chunks, w->chunks_path, allow_shared, &ru);

#ifdef TSDB_TEST_HOOKS
    uint8_t sids1[1][16];
    int cls1[1] = {eff_class};
    struct disk_usage_ov ov1[1];
    memcpy(sids1[0], store_id, 16);
    (void)load_disk_usage_file((const uint8_t (*)[16])sids1, cls1, 1, ov1);
#endif

    uint32_t warn_pct = 75u, move_pct = 80u, high_pct = 90u;
    uint64_t chunk_mark_ppm = 900000ull;
    const struct cluster_copy *cp = cluster_copy_get();
    if (cp) {
        if (cp->young_warn_pct >= 1 && cp->young_warn_pct <= 99)
            warn_pct = cp->young_warn_pct;
        if (cp->young_move_pct >= 1 && cp->young_move_pct <= 99)
            move_pct = cp->young_move_pct;
        if (cp->young_high_pct >= 10 && cp->young_high_pct <= 99)
            high_pct = cp->young_high_pct;
        chunk_mark_ppm = compute_cluster_chunk_mark_ppm(cp);
        cluster_copy_put(cp);
    }
    uint32_t clear_pct = high_pct >= 3u ? high_pct - 2u : high_pct;
    uint64_t now_hr = usage_now_hour();

    int full = 0, full_changed = 0, any_flag_changed = 0, need_beat = 0;
    uint8_t out_ypct = 0, out_flags = 0;

    pthread_mutex_lock(&g_mu);
    if (!G.started || w->disk_idx < 0 || w->disk_idx >= G.ndisks) {
        pthread_mutex_unlock(&g_mu);
        return;
    }
    struct disk_entry *d = &G.disks[w->disk_idx];
    if (!d->present || memcmp(d->meta.store_id, store_id, 16) != 0) {
        pthread_mutex_unlock(&g_mu);
        return;
    }

    uint64_t raw_young_total =
        sat_add64(sat_add64(ru.young, ru.retained), sat_add64(sat_add64(ru.retired, ru.local), ru.index));
    uint64_t eff_size = ru.size_bytes > 0 ? ru.size_bytes : (d->size_bytes > 0 ? d->size_bytes : 1);
    uint64_t eff_young = raw_young_total;
    uint64_t eff_chunks = ru.chunks;
    uint64_t young_data = ru.young;
    int has_ov = 0;

#ifdef TSDB_TEST_HOOKS
    const struct disk_usage_ov *ov = &ov1[0];
    if (ov->have_size && ov->virt_size > 0) {
        eff_size = ov->virt_size;
        has_ov = 1;
    }
    if (ov->have_bytes) {
        eff_young = ov->young_bytes;
        eff_chunks = ov->chunk_bytes;
        uint64_t other = sat_add64(sat_add64(ru.retained, ru.retired), sat_add64(ru.local, ru.index));
        young_data = eff_young > other ? eff_young - other : 0;
        has_ov = 1;
    } else if (ov->have_pct) {
        eff_young = ov->young_pct == 0 ? 0 : (uint64_t)(((__uint128_t)eff_size * ov->young_pct + 99u) / 100u);
        if (eff_young > eff_size)
            eff_young = eff_size;
        eff_chunks = ov->chunk_pct == 0 ? 0 : (uint64_t)(((__uint128_t)eff_size * ov->chunk_pct + 99u) / 100u);
        if (eff_chunks > eff_size)
            eff_chunks = eff_size;
        uint64_t other = sat_add64(sat_add64(ru.retained, ru.retired), sat_add64(ru.local, ru.index));
        young_data = eff_young > other ? eff_young - other : 0;
        has_ov = 1;
    } else if (ov->have_phantom) {
        eff_young = sat_add64(raw_young_total, ov->phantom_young);
        eff_chunks = sat_add64(ru.chunks, ov->phantom_chunks);
        young_data = sat_add64(ru.young, ov->phantom_young);
        has_ov = 1;
    }
#endif
    /* On a dedicated real disk (not --allow-shared-device and no test override), include real filesystem overhead
     * (statvfs fs_used) so ENOSPC can never arrive before young_high_pct. */
    if (!allow_shared && !has_ov) {
        if ((d->uses & META_DISK_USE_LOG) && !(d->uses & META_DISK_USE_CHUNK)) {
            if (ru.fs_used > eff_young)
                eff_young = ru.fs_used;
        } else if ((d->uses & META_DISK_USE_LOG) && (d->uses & META_DISK_USE_CHUNK)) {
            uint64_t non_chunk = ru.fs_used > eff_chunks ? ru.fs_used - eff_chunks : 0;
            if (non_chunk > eff_young)
                eff_young = non_chunk;
        } else if (d->uses & META_DISK_USE_CHUNK) {
            if (ru.fs_used > eff_chunks)
                eff_chunks = ru.fs_used;
        }
    }

    uint64_t peak_7d = update_peak_7d_locked(d, eff_young, now_hr);
    int shared = (d->uses & META_DISK_USE_LOG) && (d->uses & META_DISK_USE_CHUNK);
    uint64_t young_room = shared ? (eff_size > eff_chunks ? eff_size - eff_chunks : 0) : eff_size;
    uint64_t ypct = (d->uses & META_DISK_USE_LOG)
                        ? (young_room > 0 ? (uint64_t)(((__uint128_t)eff_young * 100u) / young_room) : 100u)
                        : 0u;
    if (ypct > 100u)
        ypct = 100u;
    uint64_t cppm = eff_size > 0 ? (uint64_t)(((__uint128_t)eff_chunks * 1000000ull) / eff_size) : 1000000ull;
    if (cppm > 1000000ull)
        cppm = 1000000ull;
    uint64_t cpct = cppm / 10000ull;
    if (cpct > 100u)
        cpct = 100u;

    int prev_full = d->usage_full;
    uint8_t prev_flags = d->usage_fill_flags;
    int next_full = prev_full;
    if (!(d->uses & META_DISK_USE_LOG)) {
        next_full = 0;
    } else if (!prev_full && ypct >= high_pct) {
        next_full = 1;
    } else if (prev_full && ypct < clear_pct) {
        next_full = 0;
    }

    int chunk_full = 0;
    int chunk_rb_full = 0;
    if (d->uses & META_DISK_USE_CHUNK) {
        if (cppm >= chunk_mark_ppm)
            chunk_full = 1;
        if (!chunk_full && shared) {
            /* E50 / §15.4: young reserve on a shared disk = max(10% of size, 2 * 7-day peak). */
            uint64_t reserve = eff_size / 10u;
            uint64_t p2 = peak_7d > (UINT64_MAX / 2u) ? UINT64_MAX : peak_7d * 2u;
            if (p2 > reserve)
                reserve = p2;
            if (eff_size <= eff_chunks || (eff_size - eff_chunks) < reserve)
                chunk_full = 1;
        }
        if (cppm >= 950000ull)
            chunk_rb_full = 1;
    }

    uint8_t flags = HBS_DISK_F_USAGE;
    if ((d->uses & META_DISK_USE_LOG) && ypct >= warn_pct)
        flags |= HBS_DISK_F_WARN;
    if ((d->uses & META_DISK_USE_LOG) && ypct >= move_pct)
        flags |= HBS_DISK_F_MOVE;
    if (next_full)
        flags |= HBS_DISK_F_FULL;
    if (chunk_full)
        flags |= HBS_DISK_F_CHUNK_FULL;

    d->usage_have = 1;
    d->usage_raw_size = ru.size_bytes > 0 ? ru.size_bytes : 1;
    d->usage_fs_used = ru.fs_used;
    d->usage_raw_young = ru.young;
    d->usage_raw_retained = ru.retained;
    d->usage_raw_retired = ru.retired;
    d->usage_raw_local = ru.local;
    d->usage_raw_index = ru.index;
    d->usage_raw_chunks = ru.chunks;
    d->usage_size = eff_size;
    d->usage_young_data = young_data;
    d->usage_eff_young = eff_young;
    d->usage_eff_chunks = eff_chunks;
    d->usage_young_room = young_room;
    d->usage_young_pct = (uint8_t)ypct;
    d->usage_chunk_pct = (uint8_t)cpct;
    d->usage_fill_flags = flags;
    d->usage_full = next_full;
    d->usage_chunk_full = chunk_full;
    d->usage_chunk_rb_full = chunk_rb_full;

    int any_full = 0;
    for (int i = 0; i < G.ndisks; i++)
        if (G.disks[i].present && G.disks[i].usage_full)
            any_full = 1;
    atomic_store_explicit(&g_disks_any_full, any_full, memory_order_release);

    full = next_full;
    full_changed = (next_full != prev_full);
    any_flag_changed = (flags != prev_flags);
    out_ypct = (uint8_t)ypct;
    out_flags = flags;
    need_beat = any_flag_changed;
    pthread_mutex_unlock(&g_mu);

    /* Log transitions and propagate per-volume disk_full state to open engines off g_mu. */
    if (full_changed) {
        if (full)
            fprintf(stderr,
                    "tsdbd: disks: disk %s young fill %u%% >= young_high_pct %" PRIu32
                    "%%: disk FULL (volumes on this disk refusing inserts with WHY_FULL)\n",
                    w->path, (unsigned)out_ypct, high_pct);
        else
            fprintf(stderr,
                    "tsdbd: disks: disk %s young fill %u%% < %" PRIu32
                    "%%: disk FULL cleared (resuming writes and merges)\n",
                    w->path, (unsigned)out_ypct, clear_pct);
    } else if (any_flag_changed) {
        if (out_flags & HBS_DISK_F_MOVE)
            fprintf(stderr,
                    "tsdbd: disks: disk %s young fill %u%% >= young_move_pct %" PRIu32
                    "%%: move needed (no new replicas placed here)\n",
                    w->path, (unsigned)out_ypct, move_pct);
        else if (out_flags & HBS_DISK_F_WARN)
            fprintf(stderr,
                    "tsdbd: disks: disk %s young fill %u%% >= young_warn_pct %" PRIu32 "%%: filling warning\n",
                    w->path, (unsigned)out_ypct, warn_pct);
    }

    pthread_mutex_lock(&w->mu);
    tsdb *db = w->stop ? NULL : w->db;
    struct disk_usage_hooks hooks = w->hooks;
    pthread_mutex_unlock(&w->mu);

    if (db && hooks.vols_on_disk) {
        uint32_t *vols = malloc(CLUSTER_REPORT_VOLS * sizeof *vols);
        if (vols) {
            uint32_t nv = hooks.vols_on_disk(store_id, vols, CLUSTER_REPORT_VOLS);
            for (uint32_t k = 0; k < nv; k++) {
                pthread_mutex_lock(&w->mu);
                int stopping = w->stop || !w->db;
                pthread_mutex_unlock(&w->mu);
                if (stopping)
                    break;
                struct eng *eng = tsdb_cvol_ref(db, vols[k]);
                if (eng) {
                    eng_set_disk_full(eng, full != 0);
                    tsdb_cvol_unref(&eng);
                }
                if (full_changed && hooks.wake_vol)
                    hooks.wake_vol(vols[k]);
            }
            free(vols);
        }
    }
    if (need_beat)
        cluster_beat_soon();
}

static int64_t disk_usage_interval_ms(void)
{
#ifdef TSDB_TEST_HOOKS
    const char *ms = getenv("TSDB_DISK_USAGE_MS");
    if (ms && *ms) {
        int v = atoi(ms);
        if (v >= 50 && v <= 10000)
            return v;
    }
    const char *uf = getenv("TSDB_DISK_USAGE_FILE");
    if (uf && *uf)
        return 100;
#endif
    return 10000; /* §15.4 / TASK-r13-fill: sample every 10 s off any engine lock */
}

static void *disk_usage_worker_main(void *arg)
{
    struct disk_usage_worker *w = arg;
    tsdb_thread_name("tsdb-diskuse");
    for (;;) {
        pthread_mutex_lock(&w->mu);
        bool stop = w->stop || !w->db;
        w->kick = false;
        pthread_mutex_unlock(&w->mu);
        if (stop)
            break;

        disk_usage_sample_one(w);

        int64_t wait_ms = disk_usage_interval_ms();
        pthread_mutex_lock(&w->mu);
        if (!w->stop && !w->kick) {
            struct timespec dl;
            tsdb_cond_deadline_ns(&dl, wait_ms * 1000000LL);
            pthread_cond_timedwait(&w->cv, &w->mu, &dl);
        }
        stop = w->stop || !w->db;
        pthread_mutex_unlock(&w->mu);
        if (stop)
            break;
    }
    pthread_mutex_lock(&w->mu);
    w->done = true;
    pthread_cond_broadcast(&w->cv);
    pthread_mutex_unlock(&w->mu);
    usage_worker_unref(w);
    return NULL;
}

int disk_usage_start(tsdb *db, const struct disk_usage_hooks *hooks)
{
    pthread_mutex_lock(&g_mu);
    if (!G.started || G.usage_started) {
        pthread_mutex_unlock(&g_mu);
        return 0;
    }
    G.usage_db = db;
    if (hooks)
        G.usage_hooks = *hooks;
    else
        memset(&G.usage_hooks, 0, sizeof G.usage_hooks);
    G.nusage_workers = 0;

    pthread_attr_t at;
    pthread_attr_init(&at);
    pthread_attr_setstacksize(&at, 256 * 1024);

    for (int i = 0; i < G.ndisks && G.nusage_workers < DISKS_MAX; i++) {
        const struct disk_entry *d = &G.disks[i];
        if (!d->present || !d->path[0])
            continue;
        struct disk_usage_worker *w = calloc(1, sizeof *w);
        if (!w)
            continue;
        atomic_init(&w->refs, 2); /* owner + worker thread */
        pthread_mutex_init(&w->mu, NULL);
        tsdb_cond_init_mono(&w->cv);
        w->disk_idx = i;
        memcpy(w->store_id, d->meta.store_id, 16);
        snprintf(w->path, sizeof w->path, "%s", d->path);
        snprintf(w->chunks_path, sizeof w->chunks_path, "%s", d->chunks_path);
        w->db = db;
        w->hooks = G.usage_hooks;
        if (pthread_create(&w->th, &at, disk_usage_worker_main, w) != 0) {
            atomic_store(&w->refs, 1);
            usage_worker_unref(w);
            continue;
        }
        G.usage_workers[G.nusage_workers++] = w;
    }
    pthread_attr_destroy(&at);

    G.usage_started = 1;
    atomic_store_explicit(&g_disks_usage_active, 1, memory_order_release);
    pthread_mutex_unlock(&g_mu);
    return 0;
}

void disk_usage_stop(void)
{
    struct disk_usage_worker *workers[DISKS_MAX];
    int nw = 0;

    pthread_mutex_lock(&g_mu);
    if (!G.usage_started) {
        pthread_mutex_unlock(&g_mu);
        return;
    }
    nw = G.nusage_workers;
    for (int i = 0; i < nw; i++) {
        workers[i] = G.usage_workers[i];
        G.usage_workers[i] = NULL;
    }
    G.nusage_workers = 0;
    G.usage_started = 0;
    G.usage_db = NULL;
    memset(&G.usage_hooks, 0, sizeof G.usage_hooks);
    atomic_store_explicit(&g_disks_usage_active, 0, memory_order_release);
    atomic_store_explicit(&g_disks_any_full, 0, memory_order_release);
    pthread_mutex_unlock(&g_mu);

    /* Signal every per-disk worker first so all healthy workers exit in parallel. */
    for (int i = 0; i < nw; i++) {
        struct disk_usage_worker *w = workers[i];
        pthread_mutex_lock(&w->mu);
        w->stop = true;
        w->db = NULL;
        pthread_cond_broadcast(&w->cv);
        pthread_mutex_unlock(&w->mu);
    }

    /* Bounded join: wait up to 500 ms per worker; detach any worker hung inside kernel I/O on a stalled disk. */
    struct timespec dl;
    tsdb_cond_deadline_ns(&dl, 500 * 1000000LL);
    for (int i = 0; i < nw; i++) {
        struct disk_usage_worker *w = workers[i];
        pthread_mutex_lock(&w->mu);
        while (!w->done) {
            if (pthread_cond_timedwait(&w->cv, &w->mu, &dl) != 0)
                break;
        }
        bool join = w->done;
        if (!join)
            pthread_detach(w->th); /* worker cannot exit until it takes w->mu; safe while !w->done */
        pthread_mutex_unlock(&w->mu);
        if (join)
            pthread_join(w->th, NULL);
        usage_worker_unref(w);
    }
}

void disk_usage_kick(void)
{
    if (!atomic_load_explicit(&g_disks_usage_active, memory_order_acquire))
        return;
    struct disk_usage_worker *workers[DISKS_MAX];
    int nw = 0;
    pthread_mutex_lock(&g_mu);
    if (G.usage_started) {
        nw = G.nusage_workers;
        for (int i = 0; i < nw; i++) {
            workers[i] = G.usage_workers[i];
            atomic_fetch_add_explicit(&workers[i]->refs, 1, memory_order_relaxed);
        }
    }
    pthread_mutex_unlock(&g_mu);
    for (int i = 0; i < nw; i++) {
        pthread_mutex_lock(&workers[i]->mu);
        workers[i]->kick = true;
        pthread_cond_broadcast(&workers[i]->cv);
        pthread_mutex_unlock(&workers[i]->mu);
        usage_worker_unref(workers[i]);
    }
}

int disks_usage_active(void) { return atomic_load_explicit(&g_disks_usage_active, memory_order_acquire) != 0; }
int disks_any_full(void) { return atomic_load_explicit(&g_disks_any_full, memory_order_acquire) != 0; }

int disks_vol_full(uint32_t vol)
{
    if (!atomic_load_explicit(&g_disks_any_full, memory_order_acquire))
        return 0;
#ifdef TSDB_TEST_HOOKS
    if (disks_mutant() == DISKS_MUT_FULLALL)
        return 1; /* planted: mark every volume on the node FULL whenever any disk is FULL */
#endif
    int (*vol_disk_fn)(uint32_t, char *, size_t, uint8_t[16]) = NULL;
    pthread_mutex_lock(&g_mu);
    vol_disk_fn = G.usage_hooks.vol_disk;
    pthread_mutex_unlock(&g_mu);
    if (!vol_disk_fn)
        return 0;
    uint8_t sid[16] = {0};
    char root[PATH_MAX] = "";
    if (vol_disk_fn(vol, root, sizeof root, sid) != 0)
        return 0;
    int nz = 0;
    for (int b = 0; b < 16; b++)
        nz |= sid[b];
    int full = 0;
    pthread_mutex_lock(&g_mu);
    for (int i = 0; i < G.ndisks; i++) {
        const struct disk_entry *d = &G.disks[i];
        int match = nz ? (memcmp(d->meta.store_id, sid, 16) == 0) : (root[0] && same_path(d->path, root));
        if (match) {
            full = d->usage_full != 0;
            break;
        }
    }
    pthread_mutex_unlock(&g_mu);
    return full;
}

uint8_t disks_vol_fill_pct(uint32_t vol)
{
    int (*vol_disk_fn)(uint32_t, char *, size_t, uint8_t[16]) = NULL;
    pthread_mutex_lock(&g_mu);
    vol_disk_fn = G.usage_hooks.vol_disk;
    pthread_mutex_unlock(&g_mu);
    if (!vol_disk_fn)
        return 0xff;
    uint8_t sid[16] = {0};
    char root[PATH_MAX] = "";
    if (vol_disk_fn(vol, root, sizeof root, sid) != 0)
        return 0xff;
    int nz = 0;
    for (int b = 0; b < 16; b++)
        nz |= sid[b];
    uint8_t pct = 0xff;
    pthread_mutex_lock(&g_mu);
    for (int i = 0; i < G.ndisks; i++) {
        const struct disk_entry *d = &G.disks[i];
        int match = nz ? (memcmp(d->meta.store_id, sid, 16) == 0) : (root[0] && same_path(d->path, root));
        if (match && d->usage_have) {
            pct = d->usage_young_pct;
            break;
        }
    }
    pthread_mutex_unlock(&g_mu);
    return pct;
}

int disks_chunk_store_can_put(int chunk_slot, int is_rebuild)
{
    if (chunk_slot < 0 || !atomic_load_explicit(&g_disks_started, memory_order_acquire))
        return 1;
    int allow = 1;
    pthread_mutex_lock(&g_mu);
    for (int i = 0; i < G.ndisks; i++) {
        const struct disk_entry *d = &G.disks[i];
        if (d->chunk_slot != chunk_slot)
            continue;
        if (d->usage_have)
            allow = is_rebuild ? !d->usage_chunk_rb_full : !d->usage_chunk_full;
        break;
    }
    pthread_mutex_unlock(&g_mu);
    return allow;
}

int disks_usage_snapshot(struct disk_fill_snap *out, int cap)
{
    if (!out || cap <= 0 || !atomic_load_explicit(&g_disks_started, memory_order_acquire))
        return 0;
    int n = 0;
    pthread_mutex_lock(&g_mu);
    if (G.started) {
        for (int i = 0; i < G.ndisks && n < cap; i++) {
            const struct disk_entry *d = &G.disks[i];
            int nz = 0;
            for (int b = 0; b < 16; b++)
                nz |= d->meta.store_id[b];
            if (!nz || !d->usage_have)
                continue;
            struct disk_fill_snap *s = &out[n++];
            memset(s, 0, sizeof *s);
            memcpy(s->store_id, d->meta.store_id, 16);
            s->disk_class = d->effective_class;
            s->uses = d->uses ? d->uses : META_DISK_USE_LOG;
            s->fill_flags = d->usage_fill_flags;
            s->young_fill_pct = d->usage_young_pct;
            s->chunk_fill_pct = d->usage_chunk_pct;
            s->size_bytes = d->usage_size > 0 ? d->usage_size : d->size_bytes;
            s->young_data_bytes = d->usage_young_data;
            s->retained_bytes = d->usage_raw_retained;
            s->retired_bytes = d->usage_raw_retired;
            s->local_bytes = d->usage_raw_local;
            s->index_bytes = d->usage_raw_index;
            s->chunk_bytes = d->usage_eff_chunks;
            s->young_peak_7d = d->usage_peak_7d;
        }
    }
    pthread_mutex_unlock(&g_mu);
    return n;
}

void disks_stop(void)
{
    disk_usage_stop();
    disks_drain_stop();
    disk_probe_stop();
    atomic_store_explicit(&g_disks_started, 0, memory_order_release);
    eng_set_disk_fill_mutants(0, 0);
    pthread_mutex_lock(&g_mu);
    int was_started = G.started;
    G.started = 0;
    G.prepared = 0;
    G.bound_all = 0;
    close_all_locks_locked();
    pthread_mutex_unlock(&g_mu);
    if (was_started) {
        creport_section_unregister(HBS_DISKS);
        creport_metric_collector_del("tsdb_disk_class_mismatch");
        creport_metric_collector_del("tsdb_disk_shared_device");
        creport_metric_collector_del("tsdb_disk_state");
        creport_metric_collector_del("tsdb_disk_bytes");
        creport_metric_collector_del("tsdb_disk_size_bytes");
    }
}
