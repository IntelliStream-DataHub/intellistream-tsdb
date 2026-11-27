/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Cluster server disk table, start checks (E58), binding, chunk store selection, and HBS_DISKS (R13, r13-start).
 */
#define _GNU_SOURCE
#include "disks.h"

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

#include "cluster.h"
#include "creport.h"
#include "../client/tsdb_crypto.h"
#include "../src/chunk.h"
#include "../src/compat.h"
#include "../src/internal.h"

#define HBS_DISKS_VER   1u
#define HBS_DISKS_HDR   8u
#define HBS_DISKS_ENTRY 36u

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
    int lock_fd; /* exclusive flock on bound disk directory, -1 when not held */
};

static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static atomic_int g_disks_started; /* fast-path guard so heartbeat/metrics never block on disks_start() I/O */

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
    return -1;
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

static ssize_t disks_sec_encode(uint8_t *buf, size_t cap)
{
    if (!atomic_load_explicit(&g_disks_started, memory_order_acquire))
        return 0;
    struct hbs_disk_row rows[DISKS_MAX];
    uint32_t nrows = 0, mismatches = 0;
    uint8_t role = META_ROLE_BOTH;
    int allow_shared = 0, started = 0;

    pthread_mutex_lock(&g_mu);
    started = G.started;
    role = G.role;
    allow_shared = G.allow_shared;
    mismatches = G.class_mismatches;
    for (int i = 0; started && i < G.ndisks && nrows < DISKS_MAX; i++) {
        const struct disk_entry *d = &G.disks[i];
        /* Never emit unbound disks: disk_bind must succeed before STORE_ADD can be proposed (§15.1). */
        if (d->present && !d->meta.bound)
            continue;
        struct hbs_disk_row *r = &rows[nrows++];
        memcpy(r->store_id, d->meta.store_id, 16);
        r->disk_class = (uint8_t)d->effective_class;
        r->uses = d->uses ? d->uses : META_DISK_USE_LOG;
        r->state = d->state;
        r->size_bytes = d->size_bytes > 0 ? d->size_bytes : 1;
        r->sync_us = d->sync_us;
    }
    pthread_mutex_unlock(&g_mu);

    if (!started)
        return 0;
    if (nrows == 0 && role == META_ROLE_BOTH && !allow_shared && mismatches == 0)
        return 0;
    size_t need = HBS_DISKS_HDR + (size_t)nrows * HBS_DISKS_ENTRY;
    if (need > cap)
        return -1;
    buf[0] = HBS_DISKS_VER;
    buf[1] = role;
    buf[2] = allow_shared ? 1u : 0u;
    buf[3] = (uint8_t)nrows;
    put_le32(buf + 4, mismatches);
    for (uint32_t i = 0; i < nrows; i++) {
        uint8_t *e = buf + HBS_DISKS_HDR + (size_t)i * HBS_DISKS_ENTRY;
        memcpy(e, rows[i].store_id, 16);
        e[16] = rows[i].disk_class;
        e[17] = rows[i].uses;
        e[18] = rows[i].state;
        e[19] = 0;
        put_le64(e + 20, rows[i].size_bytes);
        put_le64(e + 28, rows[i].sync_us);
    }
    return (ssize_t)need;
}

static int disks_sec_decode(uint32_t member_id, const uint8_t *data, size_t len, void **out_val)
{
    (void)member_id;
    if (!data || !out_val || len < HBS_DISKS_HDR)
        return -1;
    if (data[0] != HBS_DISKS_VER)
        return -1;
    uint8_t role = data[1];
    uint8_t flags = data[2];
    uint32_t n = data[3];
    if (role > META_ROLE_LAST || (flags & ~1u) || n > DISKS_MAX)
        return -1;
    if (len != HBS_DISKS_HDR + (size_t)n * HBS_DISKS_ENTRY)
        return -1;
    struct hbs_disks *v = calloc(1, sizeof *v + (size_t)n * sizeof v->disks[0]);
    if (!v)
        return -1;
    v->role = role;
    v->allow_shared_device = (uint8_t)(flags & 1u);
    v->class_mismatches = get_le32(data + 4);
    v->ndisks = n;
    for (uint32_t i = 0; i < n; i++) {
        const uint8_t *e = data + HBS_DISKS_HDR + (size_t)i * HBS_DISKS_ENTRY;
        struct hbs_disk_row *r = &v->disks[i];
        memcpy(r->store_id, e, 16);
        int nz = 0;
        for (int b = 0; b < 16; b++)
            nz |= r->store_id[b];
        r->disk_class = e[16];
        r->uses = e[17];
        r->state = e[18];
        if (!nz || e[19] != 0 || r->disk_class < META_DISK_NVME || r->disk_class > META_DISK_CLASS_LAST ||
            r->uses == 0 || (r->uses & ~META_DISK_USE_ALL) || r->state > META_STORE_STATE_LAST) {
            free(v);
            return -1;
        }
        r->size_bytes = get_le64(e + 20);
        r->sync_us = get_le64(e + 28);
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
    for (uint32_t i = 0; i < v->ndisks && off + 1 < cap; i++) {
        const struct hbs_disk_row *r = &v->disks[i];
        char hex[33];
        chunk_oid_hex(r->store_id, hex);
        int n = snprintf(buf + off, cap - off,
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
    for (int i = 0; i < DISKS_MAX; i++)
        G.disks[i].lock_fd = -1;
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
        fprintf(stderr, "tsdbd: disks: MUTANT %s (tests only)\n", mu);
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

void disks_chunk_paths(const char **paths, int cap, int *n_out)
{
    int idx[DISKS_MAX];
    int rank[DISKS_MAX];
    int n = 0;
    pthread_mutex_lock(&g_mu);
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
        const struct disk_entry *d = &G.disks[idx[a]];
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

void disks_stop(void)
{
    atomic_store_explicit(&g_disks_started, 0, memory_order_release);
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
    }
}
