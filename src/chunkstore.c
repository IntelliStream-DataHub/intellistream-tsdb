/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Chunk stores: one directory per disk (design-cluster-data.md §2.2, §5.5, §5.7, E14, E27), and the directory backend
 * over N of them on one machine.
 *
 *   <store>/                     flock'ed exclusively from open to close (a second opener is CHUNK_ERR_BUSY): two owners
 *                                would each keep their own epoch in memory, and one would write it back lower.
 *   <store>/META                 "tsdb chunk store 1", the store id, a hash: made once, checked at every open against
 *                                the id the caller was told. A disk that comes back as another one is a lost disk.
 *   <store>/s<volume>/EPOCH      the highest epoch of the volume this store has seen in any request: two checksummed
 *                                slots on pages of their own, the older generation overwritten and synced (src/fno.c's
 *                                scheme), so a torn store leaves the other slot, and nothing acted on the newer value
 *                                before its sync. Both slots bad, or none beside chunks, fails the open. One slot bad (a torn
 *                                store and rot look alike; the epoch may have been the bad slot's) opens the volume
 *                                "unsure": puts and deletes are refused until the owner raises EPOCH to the epoch the
 *                                metadata group holds (chunk_store_raise), so a fenced writer cannot pass.
 *   <store>/s<volume>/o<oid>.<pos>.chunk   a chunk file (src/chunk.c); tmp_* files are what a writer died holding
 *                                and are removed at open.
 *
 * Fencing (E14): a put or delete below the volume's epoch is refused; a higher one raises EPOCH first, and a delete is
 * also refused below the epoch the chunk's own header records. Reads are never fenced. The check is made when the
 * writer opens and again, under the epoch lock, at the rename: a chunk is put at the instant of its rename, and an
 * epoch raised before that instant refuses it.
 *
 * Durability: a chunk is written as tmp_*, fsync'ed, renamed and the directory fsync'ed before commit() returns, so
 * after a power loss it is whole or absent. A failed write or fsync of a file nothing relies on (the tmp file) fails
 * just the call. A failed directory fsync after a name became visible or went away, or any failed write or sync of
 * EPOCH, is fail-stop for the store: it acknowledges nothing more (a retried fsync can return 0 after the kernel
 * dropped the pages, so it proves nothing), reads go on, and the owner reopens it to replay from what is on disk.
 */
#include "internal.h"
#include "chunk.h"
#include "chunk_priv.h"
#include "chunkmove.h"
#include "chunkscrub.h"

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
#include <time.h>
#include <unistd.h>

#define EPOCH_PAGE 4096
#define EPOCH_MAGIC "TSDBEPO1"

typedef struct vol_state {
    uint32_t vol;
    int dirfd;
    int efd;
    uint64_t epoch, gen;
    int slot;
    bool unsure; /* one EPOCH slot was bad at open: epoch may be lower than the fence this store once held */
} vol_state;

struct chunk_store {
    atomic_uint refs; /* 1 from store_open; chunk_store_ref/unref; last unref closes fds and frees */
    bool read_only;   /* R15 D13: opened with CHUNK_OPEN_RDONLY (no flock, no EPOCH, no tmp cleanup, no write/del) */
    char *path;
    int dirfd;
    uint8_t id[16];
    chunk_store_owner owner; /* what META says (D11); unset for a store made without one */
    pthread_mutex_t mu; /* the epochs, and the instant of a rename or an unlink */
    vol_state *v;
    size_t nv, capv;
    atomic_int failed;
    atomic_uint seq;
    chunk_backend *owner_be; /* immutable once published into owner_be->st[owner_idx]; NULL for standalone stores */
    int owner_idx;
    struct chunk_store *next_pending; /* intrusive LIFO link while parked in be->pending_close[i] (guarded by st_mu) */
    atomic_int_fast64_t fill_sample_ms;   /* R15 r15-full: monotonic ms of last fstatvfs sample (<= 1/s per store) */
    atomic_uint_fast32_t fill_pct;        /* R15 r15-full: last sampled store filesystem used % (0..100) */
    atomic_uint_fast64_t fill_used_bytes; /* R15 r15-full: cached fs used bytes */
    atomic_uint_fast64_t fill_free_bytes; /* R15 r15-full: cached fs unprivileged available bytes */
    atomic_uint_fast64_t fill_total_bytes;/* R15 r15-full: cached fs total bytes */
    atomic_uint put_max_pct;              /* R15 r15-full: >0 caps regular puts at this % (0: off for cluster stores) */
};

/* ---------------------------------------------------------------- small helpers */
static bool is_storage_io_errno(int e)
{
    return e == EIO || e == ESTALE || e == ENODEV || e == ENXIO || e == ETIMEDOUT || e == EHOSTDOWN ||
           e == EHOSTUNREACH;
}

static int rd_full_ex(int fd, void *buf, size_t n, uint64_t off, bool *real_io)
{
    uint8_t *p = buf;
    size_t got = 0;
    if (real_io)
        *real_io = false;
    if (chunk_fault_read > 0 && --chunk_fault_read == 0) { /* test hook */
        if (real_io)
            *real_io = true;
        return TSDB_ERR_IO;
    }
    while (got < n) {
        ssize_t r = pread(fd, p + got, n - got, (off_t)(off + got));
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0) {
            if (r < 0 && real_io && is_storage_io_errno(errno))
                *real_io = true;
            if (chunk_mutant == CHUNK_MUT_EOF) { /* planted: a short read taken for the end of the file */
                memset(p + got, 0, n - got);
                return 0;
            }
            return TSDB_ERR_IO; /* a read error or a file shorter than it was when it was opened: never the end */
        }
        got += (size_t)r;
    }
    return 0;
}

static int rd_full(int fd, void *buf, size_t n, uint64_t off)
{
    return rd_full_ex(fd, buf, n, off, NULL);
}

static int wr_full(int fd, const void *buf, size_t n)
{
    const uint8_t *p = buf;
    size_t put = 0;
    while (put < n) {
        ssize_t w = write(fd, p + put, n - put);
        if (w < 0 && errno == EINTR)
            continue;
        if (w < 0)
            return errno == ENOSPC ? CHUNK_ERR_FULL : TSDB_ERR_IO;
        if (w == 0)
            return CHUNK_ERR_FULL;
        put += (size_t)w;
    }
    return 0;
}

static int sync_fd(int fd)
{
    if (chunk_mutant == CHUNK_MUT_NOSYNC) /* planted: a chunk named before its bytes are durable */
        return 0;
    return tsdb_fsync(fd);
}

static void store_fail(chunk_store *st, const char *what, int err)
{
    if (atomic_exchange(&st->failed, 1) == 0)
        fprintf(stderr, "tsdb: chunk store %s: %s failed (%s): the store acknowledges nothing more until it is "
                        "reopened\n", st->path, what, strerror(err));
    if (st->owner_be) {
        chunk_backend *be = st->owner_be;
        pthread_mutex_lock(&be->st_mu);
        if (atomic_load_explicit(&be->st[st->owner_idx], memory_order_relaxed) == st)
            atomic_store_explicit(&be->st_failed[st->owner_idx], true, memory_order_release);
        pthread_mutex_unlock(&be->st_mu);
    }
}

void chunk_store_bind_be(chunk_store *st, chunk_backend *be, int idx)
{
    if (!st)
        return;
    st->owner_be = be;
    st->owner_idx = idx;
    if (be)
        atomic_store_explicit(&st->put_max_pct, atomic_load_explicit(&be->put_max_pct, memory_order_acquire),
                              memory_order_release);
}

static int hex_val(int c)
{
    return c >= '0' && c <= '9' ? c - '0' : c >= 'a' && c <= 'f' ? c - 'a' + 10 : -1;
}

static bool hex_parse(const char *s, size_t nchars, uint8_t *out)
{
    for (size_t i = 0; i < nchars; i += 2) {
        int a = hex_val(s[i]), b = hex_val(s[i + 1]);
        if (a < 0 || b < 0)
            return false;
        out[i / 2] = (uint8_t)(a << 4 | b);
    }
    return true;
}

static void chunk_name(char *out, size_t cap, const uint8_t oid[16], unsigned pos)
{
    char hex[33];
    chunk_oid_hex(oid, hex);
    snprintf(out, cap, "o%s.%u.chunk", hex, pos);
}

/* ---------------------------------------------------------------- META */
static void meta_text(char *out, size_t cap, const uint8_t id[16], const chunk_store_owner *ow)
{
    char hex[33], body[256];
    chunk_oid_hex(id, hex);
    int r = snprintf(body, sizeof body, "tsdb chunk store %d\nid=%s\n", CHUNK_STORE_VERSION, hex);
    size_t n = r > 0 ? ((size_t)r < sizeof body ? (size_t)r : sizeof body - 1) : 0;
    if (ow && ow->set && n + 1 < sizeof body) { /* D11: the cluster and the node the store belongs to (a disk moved between them is refused) */
        char ch[33];
        chunk_oid_hex(ow->cluster, ch);
        r = snprintf(body + n, sizeof body - n, "cluster=%s\nnode=%" PRIu32 "\n", ch, ow->node);
        if (r > 0)
            n += (size_t)r < sizeof body - n ? (size_t)r : sizeof body - n - 1;
    }
    if (ow && ow->db_set && n + 1 < sizeof body) { /* R15 D3: single-server database binding */
        char dh[33];
        chunk_oid_hex(ow->db_id, dh);
        r = snprintf(body + n, sizeof body - n, "db=%s\n", dh);
        if (r > 0)
            n += (size_t)r < sizeof body - n ? (size_t)r : sizeof body - n - 1;
    }
    snprintf(out, cap, "%ssum=%016" PRIx64 "\n", body, xxh64(body, n, 0));
}

static int meta_read(int dirfd, uint8_t id[16], chunk_store_owner *ow)
{
    int fd = openat(dirfd, "META", O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    char buf[320];
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
            rc = TSDB_ERR_CORRUPT; /* longer than any META */
            break;
        }
    }
    close(fd);
    if (rc)
        return rc;
    /* strict: the first line, "id=" and 32 hex digits, then optionally "cluster=" + "node=", then optionally "db=" and
     * 32 hex digits; the text is re-made from those and compared whole, so the sum line, the newlines and the length
     * are all checked at once */
    static const char first[] = "tsdb chunk store 1\nid=", cl[] = "cluster=", nd[] = "node=", dbl[] = "db=";
    uint8_t t[16];
    chunk_store_owner o = {0};
    char want[320];
    if (got < sizeof first - 1 + 33 || memcmp(buf, first, sizeof first - 1) != 0 ||
        !hex_parse(buf + sizeof first - 1, 32, t) || buf[sizeof first - 1 + 32] != '\n')
        return TSDB_ERR_CORRUPT;
    size_t at = sizeof first - 1 + 33;
    if (got > at + sizeof cl - 1 + 33 && memcmp(buf + at, cl, sizeof cl - 1) == 0) {
        if (!hex_parse(buf + at + sizeof cl - 1, 32, o.cluster) || buf[at + sizeof cl - 1 + 32] != '\n')
            return TSDB_ERR_CORRUPT;
        at += sizeof cl - 1 + 33;
        if (got < at + sizeof nd - 1 || memcmp(buf + at, nd, sizeof nd - 1) != 0)
            return TSDB_ERR_CORRUPT;
        uint64_t v = 0;
        size_t i = at + sizeof nd - 1, d0 = i;
        while (i < got && buf[i] >= '0' && buf[i] <= '9' && i - d0 < 10)
            v = v * 10 + (uint64_t)(buf[i++] - '0');
        if (i == d0 || v > UINT32_MAX || i >= got || buf[i] != '\n')
            return TSDB_ERR_CORRUPT;
        o.node = (uint32_t)v;
        o.set = true;
        at = i + 1;
    }
    if (got >= at + sizeof dbl - 1 + 33 && memcmp(buf + at, dbl, sizeof dbl - 1) == 0) {
        if (!hex_parse(buf + at + sizeof dbl - 1, 32, o.db_id) || buf[at + sizeof dbl - 1 + 32] != '\n')
            return TSDB_ERR_CORRUPT;
        o.db_set = true;
    }
    meta_text(want, sizeof want, t, &o);
    if (strlen(want) != got || memcmp(want, buf, got) != 0)
        return TSDB_ERR_CORRUPT;
    memcpy(id, t, 16);
    if (ow)
        *ow = o;
    return 0;
}

int chunk_store_meta_read(const char *path, uint8_t id_out[16], chunk_store_owner *ow_out)
{
    if (!path || !*path)
        return TSDB_ERR_INVALID;
    int dirfd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dirfd < 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    uint8_t id[16];
    chunk_store_owner ow = {0};
    int rc = meta_read(dirfd, id, &ow);
    close(dirfd);
    if (rc == 0) {
        if (id_out)
            memcpy(id_out, id, 16);
        if (ow_out)
            *ow_out = ow;
    }
    return rc;
}

static int sync_dir_path_parent(const char *path)
{
    char *p = strdup(path);
    if (!p)
        return TSDB_ERR_NOMEM;
    char *slash = strrchr(p, '/');
    const char *dir = ".";
    if (slash && slash != p) {
        *slash = 0;
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

int chunk_store_init(const char *path, const uint8_t *id, uint8_t id_out[16])
{
    return chunk_store_init2(path, id, NULL, id_out);
}

int chunk_store_init2(const char *path, const uint8_t *id, const chunk_store_owner *owner, uint8_t id_out[16])
{
    if (!path || !*path)
        return TSDB_ERR_INVALID;
    /* Strip trailing slashes (except "/") so O_NOFOLLOW catches symlinked target directories too. */
    size_t plen = strlen(path);
    while (plen > 1 && path[plen - 1] == '/')
        plen--;
    char *pcopy = NULL;
    const char *p = path;
    if (path[plen] != '\0') {
        pcopy = malloc(plen + 1);
        if (!pcopy)
            return TSDB_ERR_NOMEM;
        memcpy(pcopy, path, plen);
        pcopy[plen] = '\0';
        p = pcopy;
    }
    bool made = mkdir(p, 0755) == 0;
    if (!made && errno != EEXIST) {
        free(pcopy);
        return TSDB_ERR_IO;
    }
    int dirfd = open(p, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (dirfd < 0) {
        free(pcopy);
        return TSDB_ERR_IO;
    }
    int rc = 0;
    if (faccessat(dirfd, "META", F_OK, AT_SYMLINK_NOFOLLOW) == 0) {
        close(dirfd);
        free(pcopy);
        return TSDB_ERR_INVALID; /* already a store: its id is not replaced */
    }
    uint8_t sid[16];
    if (id)
        memcpy(sid, id, 16);
    else if (chunk_random(sid, 16) != 0) {
        close(dirfd);
        free(pcopy);
        return TSDB_ERR_IO;
    }
    char text[320];
    meta_text(text, sizeof text, sid, owner);
    /* a temporary file is always a new inode: one a failed write or sync left behind may have had its pages dropped
     * (the kernel reports that once), so it is never reused */
    unlinkat(dirfd, "META.tmp", 0);
    int fd = openat(dirfd, "META.tmp", O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0644);
    if (fd < 0)
        rc = TSDB_ERR_IO;
    else {
        if (wr_full(fd, text, strlen(text)) != 0 || tsdb_fsync(fd) != 0)
            rc = TSDB_ERR_IO;
        close(fd);
    }
    if (rc == 0 && renameat(dirfd, "META.tmp", dirfd, "META") != 0)
        rc = TSDB_ERR_IO;
    if (rc != 0)
        unlinkat(dirfd, "META.tmp", 0);
    if (rc == 0 && tsdb_fsync(dirfd) != 0)
        rc = TSDB_ERR_IO;
    close(dirfd);
    if (rc == 0 && made)
        rc = sync_dir_path_parent(p);
    free(pcopy);
    if (rc == 0 && id_out)
        memcpy(id_out, sid, 16);
    return rc;
}

/* ---------------------------------------------------------------- EPOCH */
typedef struct epoch_slot {
    uint64_t epoch, gen;
    bool ok;
} epoch_slot;

static void epoch_fill(uint8_t *page, uint64_t epoch, uint64_t gen)
{
    memset(page, 0, EPOCH_PAGE);
    memcpy(page, EPOCH_MAGIC, 8);
    chunk_put_le64(page + 8, epoch);
    chunk_put_le64(page + 16, gen);
    chunk_put_le64(page + 24, xxh64(page, 24, 0));
}

static epoch_slot epoch_parse(const uint8_t *page)
{
    epoch_slot s = {0, 0, false};
    if (memcmp(page, EPOCH_MAGIC, 8) == 0 && chunk_le64(page + 24) == xxh64(page, 24, 0)) {
        s.epoch = chunk_le64(page + 8);
        s.gen = chunk_le64(page + 16);
        s.ok = true;
    }
    return s;
}

/* readdir where NULL is the end only when errno stayed 0 (db.c does the same): *err is the error that ended it, else 0. */
static struct dirent *next_entry(DIR *d, int *err)
{
    errno = 0;
    struct dirent *e;
    if (chunk_fault_readdir > 0 && --chunk_fault_readdir == 0) { /* test hook: readdir fails the way a bad disk does */
        e = NULL;
        errno = EIO;
    } else
        e = readdir(d);
    *err = e ? 0 : errno;
    return e;
}

/* Does the volume directory hold chunks (names o...)? Removes tmp_* files. 0, or TSDB_ERR_IO when the scan failed: a
 * failing readdir must not make a volume whose EPOCH is missing look new (its fence would restart at 0). */
static int dir_scan_volume(int dirfd, bool *has_out, bool *tmp_removed)
{
    int dup_fd = dup(dirfd);
    DIR *d = dup_fd >= 0 ? fdopendir(dup_fd) : NULL;
    if (!d) {
        if (dup_fd >= 0)
            close(dup_fd);
        return TSDB_ERR_IO;
    }
    rewinddir(d); /* the dup shares its offset with dirfd (AGENTS.md "Directory scans") */
    bool has = false;
    struct dirent *e;
    int err;
    while ((e = next_entry(d, &err)) != NULL) {
        if (strncmp(e->d_name, "tmp_", 4) == 0) {
            if (tmp_removed && unlinkat(dirfd, e->d_name, 0) == 0)
                *tmp_removed = true;
        } else if (e->d_name[0] == 'o') {
            has = true;
        }
    }
    closedir(d);
    if (err)
        return TSDB_ERR_IO;
    *has_out = has;
    return 0;
}

/* Finds or opens the volume's state; mu held. create: make the directory and its EPOCH if there are none. */
static int vol_get(chunk_store *st, uint32_t vol, bool create, vol_state **out)
{
    for (size_t i = 0; i < st->nv; i++)
        if (st->v[i].vol == vol) {
            *out = &st->v[i];
            return 0;
        }
    if (st->read_only) {
        if (create)
            return TSDB_ERR_READONLY;
        char name[32];
        snprintf(name, sizeof name, "s%u", vol);
        int vfd = openat(st->dirfd, name, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (vfd < 0)
            return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
        if (st->nv == st->capv) {
            size_t nc = st->capv ? 2 * st->capv : 4;
            vol_state *nv = realloc(st->v, nc * sizeof *nv);
            if (!nv) {
                close(vfd);
                return TSDB_ERR_NOMEM;
            }
            st->v = nv;
            st->capv = nc;
        }
        vol_state *v = &st->v[st->nv++];
        *v = (vol_state){.vol = vol, .dirfd = vfd, .efd = -1, .epoch = 0, .gen = 0, .slot = 0, .unsure = false};
        *out = v;
        return 0;
    }
    char name[32];
    snprintf(name, sizeof name, "s%u", vol);
    bool made = false;
    if (create && mkdirat(st->dirfd, name, 0755) == 0)
        made = true;
    else if (create && errno != EEXIST)
        return TSDB_ERR_IO;
    int vfd = openat(st->dirfd, name, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (vfd < 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    if (made && tsdb_fsync(st->dirfd) != 0) {
        close(vfd);
        store_fail(st, "directory fsync of a new volume directory", errno);
        return TSDB_ERR_IO;
    }
    bool removed = false, has = false;
    int srv = dir_scan_volume(vfd, &has, &removed);
    if (srv) {
        fprintf(stderr, "tsdb: chunk store %s: cannot list %s: the open fails\n", st->path, name);
        close(vfd);
        return srv;
    }
    int efd = openat(vfd, "EPOCH", O_RDWR | O_CLOEXEC);
    uint8_t pages[2 * EPOCH_PAGE];
    epoch_slot s[2] = {{0, 0, false}, {0, 0, false}};
    if (efd < 0 && errno != ENOENT) {
        close(vfd);
        return TSDB_ERR_IO;
    }
    if (efd < 0) {
        if (has) {
            fprintf(stderr, "tsdb: chunk store %s: %s has chunks but no EPOCH: the open fails (the fence it held is "
                            "lost)\n", st->path, name);
            close(vfd);
            return TSDB_ERR_CORRUPT;
        }
        if (!create) {
            close(vfd);
            return TSDB_ERR_NOTFOUND;
        }
        memset(pages, 0, sizeof pages);
        epoch_fill(pages, 0, 1);
        epoch_fill(pages + EPOCH_PAGE, 0, 0);
        unlinkat(vfd, "tmp_EPOCH", 0); /* a new inode every time (see chunk_store_init) */
        int tfd = openat(vfd, "tmp_EPOCH", O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
        int rc = tfd < 0 ? TSDB_ERR_IO : 0;
        if (tfd >= 0) {
            if (wr_full(tfd, pages, sizeof pages) != 0 || tsdb_fsync(tfd) != 0)
                rc = TSDB_ERR_IO;
            close(tfd);
        }
        if (rc == 0 && renameat(vfd, "tmp_EPOCH", vfd, "EPOCH") != 0)
            rc = TSDB_ERR_IO;
        if (rc != 0)
            unlinkat(vfd, "tmp_EPOCH", 0);
        if (rc == 0 && tsdb_fsync(vfd) != 0) {
            store_fail(st, "directory fsync of EPOCH", errno);
            rc = TSDB_ERR_IO;
        }
        efd = rc == 0 ? openat(vfd, "EPOCH", O_RDWR | O_CLOEXEC) : -1;
        if (efd < 0) {
            close(vfd);
            return TSDB_ERR_IO;
        }
    }
    size_t got = 0;
    memset(pages, 0, sizeof pages);
    while (got < sizeof pages) {
        ssize_t r = pread(efd, pages + got, sizeof pages - got, (off_t)got);
        if (r < 0 && errno == EINTR)
            continue;
        if (r < 0) {
            close(efd);
            close(vfd);
            return TSDB_ERR_IO;
        }
        if (r == 0)
            break; /* a short file: the slots it lacks are bad */
        got += (size_t)r;
    }
    s[0] = epoch_parse(pages);
    s[1] = epoch_parse(pages + EPOCH_PAGE);
    if (!s[0].ok && !s[1].ok) {
        fprintf(stderr, "tsdb: chunk store %s: %s/EPOCH has no valid slot: the fence of the volume is lost, the open "
                        "fails\n", st->path, name);
        close(efd);
        close(vfd);
        return TSDB_ERR_CORRUPT;
    }
    int best = !s[0].ok || (s[1].ok && s[1].gen > s[0].gen) ? 1 : 0;
    bool unsure = !s[0].ok || !s[1].ok;
    if (unsure)
        fprintf(stderr, "tsdb: chunk store %s: %s/EPOCH slot %d is damaged: the fence may be lower than it was (epoch "
                        "%" PRIu64 " read); puts and deletes of the volume are refused until its epoch is raised\n",
                st->path, name, best ? 0 : 1, s[best].epoch);
    if (st->nv == st->capv) {
        size_t nc = st->capv ? 2 * st->capv : 4;
        vol_state *nv = realloc(st->v, nc * sizeof *nv);
        if (!nv) {
            close(efd);
            close(vfd);
            return TSDB_ERR_NOMEM;
        }
        st->v = nv;
        st->capv = nc;
    }
    vol_state *v = &st->v[st->nv++];
    *v = (vol_state){.vol = vol, .dirfd = vfd, .efd = efd, .epoch = s[best].epoch, .gen = s[best].gen, .slot = best,
                  .unsure = unsure};
    *out = v;
    return 0;
}

/* Raises the volume's EPOCH to e and syncs it; mu held. Fail-stop on any failure. */
static int epoch_store(chunk_store *st, vol_state *v, uint64_t e)
{
    uint8_t page[EPOCH_PAGE];
    int target = 1 - v->slot; /* the older generation */
    epoch_fill(page, e, v->gen + 1);
    ssize_t w = pwrite(v->efd, page, EPOCH_PAGE, (off_t)target * EPOCH_PAGE);
    int err = w == EPOCH_PAGE ? 0 : (w >= 0 ? ENOSPC : errno);
    if (err == 0 && chunk_mutant != CHUNK_MUT_EPOCH_NOSYNC && tsdb_fdatasync(v->efd) != 0) /* planted: no sync */
        err = errno ? errno : EIO;
    if (err) {
        store_fail(st, "EPOCH write", err);
        return TSDB_ERR_IO;
    }
    v->slot = target;
    v->gen++;
    v->epoch = e;
    return 0;
}

/* ---------------------------------------------------------------- the store */
static int scan_volumes(chunk_store *st)
{
    int dup_fd = dup(st->dirfd);
    DIR *d = dup_fd >= 0 ? fdopendir(dup_fd) : NULL;
    if (!d) {
        if (dup_fd >= 0)
            close(dup_fd);
        return TSDB_ERR_IO;
    }
    rewinddir(d);
    int rc = 0, err = 0;
    struct dirent *e;
    while (rc == 0 && (e = next_entry(d, &err)) != NULL) {
        char *end;
        if (e->d_name[0] != 's' || e->d_name[1] < '0' || e->d_name[1] > '9')
            continue;
        unsigned long vol = strtoul(e->d_name + 1, &end, 10);
        char back[32];
        snprintf(back, sizeof back, "s%lu", vol);
        if (*end || vol > 0xffffffffu || strcmp(back, e->d_name) != 0)
            continue;
        vol_state *v;
        rc = vol_get(st, (uint32_t)vol, false, &v);
        if (rc == TSDB_ERR_NOTFOUND) /* a directory a crash left before its EPOCH, empty: nothing to carry */
            rc = 0;
    }
    closedir(d);
    if (rc == 0 && err) {
        fprintf(stderr, "tsdb: chunk store %s: cannot list the store directory: the open fails\n", st->path);
        rc = TSDB_ERR_IO;
    }
    return rc;
}

static int store_open(const char *path, const uint8_t *expect_id, const uint8_t *expect_db, bool read_only, bool quiet,
                      chunk_store **out)
{
    *out = NULL;
    int dirfd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dirfd < 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    if (!read_only || chunk_mutant == CHUNK_MUT_FOLLOW_FLOCK) {
        if (flock(dirfd, LOCK_EX | LOCK_NB) != 0) { /* held by the open file description: a second open here conflicts too */
            int busy = errno == EWOULDBLOCK;
            if (busy && !quiet)
                fprintf(stderr, "tsdb: chunk store %s is open elsewhere (two owners would let its EPOCH go back)\n", path);
            close(dirfd);
            return busy ? CHUNK_ERR_BUSY : TSDB_ERR_IO;
        }
    }
    uint8_t id[16];
    chunk_store_owner ow;
    int rc = meta_read(dirfd, id, &ow);
    if (rc == TSDB_ERR_CORRUPT && !quiet)
        fprintf(stderr, "tsdb: chunk store %s: META is damaged: the open fails\n", path);
    if (rc == 0 && expect_id && memcmp(expect_id, id, 16) != 0) {
        char a[33], b[33];
        chunk_oid_hex(id, a);
        chunk_oid_hex(expect_id, b);
        if (!quiet)
            fprintf(stderr, "tsdb: chunk store %s: its META says store %s, but it should be %s: not that disk (lost)\n",
                path, a, b);
        rc = CHUNK_ERR_STORE_ID;
    }
    if (rc == 0 && expect_db) {
        if (!ow.db_set) {
            char b[33];
            chunk_oid_hex(expect_db, b);
            if (!quiet)
                fprintf(stderr, "tsdb: chunk store %s: its META has no db= binding, but expected database %s: refused\n",
                        path, b);
            rc = CHUNK_ERR_STORE_ID;
        } else if (memcmp(ow.db_id, expect_db, 16) != 0) {
            char a[33], b[33];
            chunk_oid_hex(ow.db_id, a);
            chunk_oid_hex(expect_db, b);
            if (!quiet)
                fprintf(stderr, "tsdb: chunk store %s: its META binds it to database %s, not %s: refused\n",
                        path, a, b);
            rc = CHUNK_ERR_STORE_ID;
        }
    }
    if (rc) {
        close(dirfd);
        return rc;
    }
    chunk_store *st = calloc(1, sizeof *st);
    if (!st) {
        close(dirfd);
        return TSDB_ERR_NOMEM;
    }
    atomic_init(&st->refs, 1);
    st->read_only = read_only;
    st->path = strdup(path);
    st->dirfd = dirfd;
    memcpy(st->id, id, 16);
    st->owner = ow;
    pthread_mutex_init(&st->mu, NULL);
    pthread_mutex_lock(&st->mu);
    rc = !st->path ? TSDB_ERR_NOMEM : read_only ? 0 : scan_volumes(st);
    pthread_mutex_unlock(&st->mu);
    if (rc) {
        chunk_store_close(st);
        return rc;
    }
    /* Seed cached filesystem byte counts once while open already touches the store directory, so /metrics reads
     * cached atomics only without ever issuing fstatvfs on a potentially hanging store. */
    chunk_store_fs_refresh(st);
    *out = st;
    return 0;
}

int chunk_store_open(const char *path, const uint8_t *expect_id, chunk_store **out)
{
    return store_open(path, expect_id, NULL, false, false, out);
}

int chunk_store_open_quiet(const char *path, const uint8_t *expect_id, chunk_store **out)
{
    return store_open(path, expect_id, NULL, false, true, out);
}

int chunk_store_open_db(const char *path, const uint8_t *expect_id, const uint8_t *expect_db, chunk_store **out)
{
    return store_open(path, expect_id, expect_db, false, false, out);
}

int chunk_store_open_db_quiet(const char *path, const uint8_t *expect_id, const uint8_t *expect_db, chunk_store **out)
{
    return store_open(path, expect_id, expect_db, false, true, out);
}

int chunk_store_open_flags(const char *path, const uint8_t *expect_id, const uint8_t *expect_db, unsigned flags,
                           chunk_store **out)
{
    *out = NULL;
    if (flags & ~(unsigned)CHUNK_OPEN_RDONLY)
        return TSDB_ERR_INVALID;
    return store_open(path, expect_id, expect_db, (flags & CHUNK_OPEN_RDONLY) != 0, false, out);
}

int chunk_store_open_ro_quiet(const char *path, const uint8_t *expect_id, chunk_store **out)
{
    return store_open(path, expect_id, NULL, true, true, out);
}

static void store_destroy(chunk_store *st)
{
    for (size_t i = 0; i < st->nv; i++) {
        close(st->v[i].dirfd);
        if (st->v[i].efd >= 0)
            close(st->v[i].efd);
    }
    free(st->v);
    if (st->dirfd >= 0)
        close(st->dirfd);
    pthread_mutex_destroy(&st->mu);
    free(st->path);
    free(st);
}

chunk_store *chunk_store_ref(chunk_store *st)
{
    if (st)
        atomic_fetch_add_explicit(&st->refs, 1, memory_order_relaxed);
    return st;
}

void chunk_store_unref(chunk_store *st)
{
    if (st && atomic_fetch_sub_explicit(&st->refs, 1, memory_order_acq_rel) == 1)
        store_destroy(st);
}

void chunk_store_close(chunk_store *st) { chunk_store_unref(st); }

const uint8_t *chunk_store_id(const chunk_store *st) { return st->id; }
int chunk_store_dirfd(const chunk_store *st) { return st ? st->dirfd : -1; }
const char *chunk_store_path(const chunk_store *st) { return st && st->path ? st->path : ""; }
/* Test hooks (tests/test_chunk_heal.c): a store that failed in service, as an EIO on a directory fsync makes it (that path
 * is test_chunk.c's), and one flipped byte in the first cell of a chunk, as a write that landed wrong. */
void chunk_store_test_fail(chunk_store *st) { store_fail(st, "test hook", EIO); }

int chunk_store_test_flip(chunk_store *st, const chunk_ident *id)
{
    char name[80];
    chunk_name(name, sizeof name, id->oid, id->pos);
    pthread_mutex_lock(&st->mu);
    vol_state *v;
    int rc = vol_get(st, id->volume, false, &v);
    int fd = rc == 0 ? openat(v->dirfd, name, O_RDWR | O_CLOEXEC) : -1;
    pthread_mutex_unlock(&st->mu);
    if (fd < 0)
        return TSDB_ERR_IO;
    uint8_t b;
    rc = pread(fd, &b, 1, CHUNK_HDR_BYTES) == 1 ? 0 : TSDB_ERR_IO;
    b ^= 0x5a;
    if (rc == 0 && pwrite(fd, &b, 1, CHUNK_HDR_BYTES) != 1)
        rc = TSDB_ERR_IO;
    if (rc == 0 && fsync(fd) != 0)
        rc = TSDB_ERR_IO;
    close(fd);
    return rc;
}

bool chunk_store_failed(const chunk_store *st) { return atomic_load(&((chunk_store *)st)->failed) != 0; }

uint64_t chunk_store_epoch(chunk_store *st, uint32_t volume)
{
    pthread_mutex_lock(&st->mu);
    vol_state *v;
    uint64_t e = vol_get(st, volume, false, &v) == 0 ? v->epoch : 0;
    pthread_mutex_unlock(&st->mu);
    return e;
}

int chunk_store_raise(chunk_store *st, uint32_t volume, uint64_t e)
{
    if (st->read_only)
        return TSDB_ERR_READONLY;
    pthread_mutex_lock(&st->mu);
    vol_state *v;
    int rc = atomic_load(&st->failed) ? TSDB_ERR_IO : vol_get(st, volume, true, &v);
    if (rc == 0 && (e > v->epoch || (v->unsure && e >= v->epoch))) {
        rc = epoch_store(st, v, e); /* an unsure volume: also at its own epoch, which rewrites the damaged slot */
        if (rc == 0 && e >= v->epoch)
            v->unsure = false;
    }
    pthread_mutex_unlock(&st->mu);
    return rc;
}

int chunk_store_see(chunk_store *st, uint32_t volume, uint64_t e)
{
    if (st->read_only)
        return TSDB_ERR_READONLY;
    pthread_mutex_lock(&st->mu);
    vol_state *v;
    int rc = atomic_load(&st->failed) ? TSDB_ERR_IO : vol_get(st, volume, true, &v);
    if (rc == 0 && v->unsure)
        rc = CHUNK_ERR_EPOCH_UNSURE; /* never written from a request: both slots good would make it sure at a reopen */
    else if (rc == 0 && e > v->epoch)
        rc = epoch_store(st, v, e);
    pthread_mutex_unlock(&st->mu);
    return rc;
}

void chunk_store_get_owner(const chunk_store *st, chunk_store_owner *out) { *out = st->owner; }

bool chunk_store_epoch_unsure(chunk_store *st, uint32_t volume)
{
    pthread_mutex_lock(&st->mu);
    vol_state *v;
    bool u = vol_get(st, volume, false, &v) == 0 && v->unsure;
    pthread_mutex_unlock(&st->mu);
    return u;
}

int chunk_store_volumes(chunk_store *st, uint32_t **vols, uint64_t **epochs, size_t *n)
{
    pthread_mutex_lock(&st->mu);
    *n = st->nv;
    *vols = malloc((st->nv + 1) * sizeof **vols);
    *epochs = malloc((st->nv + 1) * sizeof **epochs);
    if (*vols && *epochs)
        for (size_t i = 0; i < st->nv; i++)
            (*vols)[i] = st->v[i].vol, (*epochs)[i] = st->v[i].epoch;
    pthread_mutex_unlock(&st->mu);
    if (!*vols || !*epochs) {
        free(*vols);
        free(*epochs);
        return TSDB_ERR_NOMEM;
    }
    return 0;
}

/* ---------------------------------------------------------------- store fill cap (R15 D7, §15.7) */
_Atomic uint_fast64_t chunk_store_fill_samples;

static int64_t store_mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

#ifdef TSDB_TEST_HOOKS
static pthread_mutex_t store_fill_test_mu = PTHREAD_MUTEX_INITIALIZER;
static int store_fill_test_global = -1;
static char store_fill_test_path[PATH_MAX];
static int store_fill_test_path_pct = -1;
static pthread_once_t store_fill_env_once = PTHREAD_ONCE_INIT;

static void store_fill_env_init(void)
{
    const char *v = getenv("TSDB_TEST_STORE_FILL_PCT");
    if (!v || !*v)
        return;
    char *end = NULL;
    errno = 0;
    long n = strtol(v, &end, 10);
    if (errno == 0 && end != v && *end == '\0' && n >= 0 && n <= 100) {
        pthread_mutex_lock(&store_fill_test_mu);
        if (store_fill_test_global < 0)
            store_fill_test_global = (int)n;
        pthread_mutex_unlock(&store_fill_test_mu);
    }
}

void chunk_store_set_test_fill_pct(int pct)
{
    pthread_once(&store_fill_env_once, store_fill_env_init);
    pthread_mutex_lock(&store_fill_test_mu);
    store_fill_test_global = pct < 0 ? -1 : pct > 100 ? 100 : pct;
    pthread_mutex_unlock(&store_fill_test_mu);
}

void chunk_store_set_test_fill_path(const char *path, int pct)
{
    pthread_once(&store_fill_env_once, store_fill_env_init);
    pthread_mutex_lock(&store_fill_test_mu);
    if (!path || !*path || pct < 0) {
        store_fill_test_path[0] = '\0';
        store_fill_test_path_pct = -1;
    } else {
        snprintf(store_fill_test_path, sizeof store_fill_test_path, "%s", path);
        store_fill_test_path_pct = pct > 100 ? 100 : pct;
    }
    pthread_mutex_unlock(&store_fill_test_mu);
}

static int store_test_fill_override(const char *path)
{
    pthread_once(&store_fill_env_once, store_fill_env_init);
    pthread_mutex_lock(&store_fill_test_mu);
    int ov = store_fill_test_global;
    if (store_fill_test_path_pct >= 0 && path && strcmp(path, store_fill_test_path) == 0)
        ov = store_fill_test_path_pct;
    pthread_mutex_unlock(&store_fill_test_mu);
    return ov;
}

static inline bool store_put_cap_active(void) { return chunk_mutant != CHUNK_MUT_STOREPUT96; }
#else
void chunk_store_set_test_fill_pct(int pct) { (void)pct; }
void chunk_store_set_test_fill_path(const char *path, int pct) { (void)path, (void)pct; }
static inline int store_test_fill_override(const char *path) { (void)path; return -1; }
static inline bool store_put_cap_active(void) { return true; }
#endif

static uint32_t store_sample_fs(chunk_store *st)
{
    int ov = store_test_fill_override(st->path);
    if (ov >= 0)
        return (uint32_t)ov; /* never clobber the cached real fstatvfs reading with a test override */
    int64_t mono = store_mono_ms();
    int64_t last = atomic_load_explicit(&st->fill_sample_ms, memory_order_relaxed);
    if (last != 0 && mono >= last && mono - last < 1000)
        return atomic_load_explicit(&st->fill_pct, memory_order_relaxed);
    uint32_t pct = atomic_load_explicit(&st->fill_pct, memory_order_relaxed);
    struct statvfs sv;
    if (st->dirfd >= 0 && fstatvfs(st->dirfd, &sv) == 0 && sv.f_blocks > 0) {
        uint64_t total_blks = (uint64_t)sv.f_blocks;
        uint64_t avail_blks = (uint64_t)sv.f_bavail < total_blks ? (uint64_t)sv.f_bavail : total_blks;
        uint64_t used_blks = total_blks - avail_blks;
        pct = used_blks > UINT64_MAX / 100u ? (uint32_t)(used_blks / (total_blks / 100u))
                                            : (uint32_t)((used_blks * 100u) / total_blks);
        if (pct > 100u)
            pct = 100u;
        uint64_t bsz = sv.f_frsize ? (uint64_t)sv.f_frsize : (uint64_t)sv.f_bsize;
        atomic_store_explicit(&st->fill_total_bytes, total_blks * bsz, memory_order_relaxed);
        atomic_store_explicit(&st->fill_used_bytes, used_blks * bsz, memory_order_relaxed);
        atomic_store_explicit(&st->fill_free_bytes, avail_blks * bsz, memory_order_relaxed);
        atomic_fetch_add_explicit(&chunk_store_fill_samples, 1, memory_order_relaxed);
    }
    atomic_store_explicit(&st->fill_pct, pct, memory_order_relaxed);
    atomic_store_explicit(&st->fill_sample_ms, mono ? mono : 1, memory_order_relaxed);
    return pct;
}

void chunk_store_fs_refresh(chunk_store *st)
{
    if (!st || st->dirfd < 0)
        return;
    (void)store_sample_fs(st);
}

int chunk_store_fs_bytes_cached(chunk_store *st, uint64_t *used_out, uint64_t *free_out, uint64_t *total_out)
{
    if (!st)
        return -1;
    uint64_t total = atomic_load_explicit(&st->fill_total_bytes, memory_order_relaxed);
    uint64_t used = atomic_load_explicit(&st->fill_used_bytes, memory_order_relaxed);
    uint64_t avail = atomic_load_explicit(&st->fill_free_bytes, memory_order_relaxed);
    int ov = store_test_fill_override(st->path);
    if (ov >= 0) {
        if (total == 0)
            total = 100ull << 30;
        used = (uint64_t)(((__uint128_t)total * (uint32_t)ov) / 100u);
        avail = total >= used ? total - used : 0;
    }
    if (total == 0)
        return -1;
    if (used_out)
        *used_out = used;
    if (free_out)
        *free_out = avail;
    if (total_out)
        *total_out = total;
    return 0;
}

void chunk_be_set_put_cap(chunk_backend *be, unsigned max_pct)
{
    if (!be)
        return;
    atomic_store_explicit(&be->put_max_pct, max_pct, memory_order_release);
    int n = atomic_load_explicit(&be->n, memory_order_acquire);
    for (int i = 0; i < n; i++) {
        chunk_store *st = chunk_be_raw_store_ref(be, i);
        if (st) {
            atomic_store_explicit(&st->put_max_pct, max_pct, memory_order_release);
            chunk_store_unref(st);
        }
    }
}

static uint32_t store_cached_fill_pct(const chunk_store *st)
{
    int ov = store_test_fill_override(st->path);
    if (ov >= 0)
        return (uint32_t)ov;
    return atomic_load_explicit(&st->fill_pct, memory_order_relaxed);
}

bool chunk_be_any_store_full(chunk_backend *be)
{
    if (!be || !store_put_cap_active())
        return false;
    unsigned cap = atomic_load_explicit(&be->put_max_pct, memory_order_acquire);
    if (cap == 0)
        return false;
    /* Read-only cached fill % (zero syscalls): the detached per-store probe threads (tier_health.c) and real puts
     * (wr_open) refresh fill_pct. Calling fstatvfs() here from the shared settle thread would hang eng_close/tsdb_close
     * whenever a tier store on NFS stalls. */
    int n = atomic_load_explicit(&be->n, memory_order_acquire);
    for (int i = 0; i < n; i++) {
        chunk_store *st = chunk_be_store_ref(be, i);
        bool full = st && store_cached_fill_pct(st) > cap;
        chunk_store_unref(st);
        if (full)
            return true;
    }
    return false;
}

/* ---------------------------------------------------------------- writing */
struct chunk_wr {
    chunk_store *st;
    chunk_ident id;
    chunk_geom g;
    uint64_t epoch; /* the header's */
    uint64_t fence; /* checked against the volume's epoch at open and at the rename: the writer's own, which is the
                     * header's except for a rebuilt chunk (it keeps the epoch the object was written at) */
    int vdirfd, fd;
    char tmp[96], name[80];
    uint8_t hdr[CHUNK_HDR_BYTES];
    uint64_t ncells, next, written;
    uint64_t *sums;
    int err;          /* a failed cell poisons the writer: commit refuses */
    bool replace_only;/* R12 (D9): refuse open and commit unless target chunk file already exists */
    bool synced;      /* R13 r13-lmove: tail already written and tmp fd already fsynced via chunk_wr_sync() */
    bool defer_loc;   /* R13 r13-lmove: caller (chunk_move) publishes loc_tab only after read-back verification */
};

void chunk_wr_set_defer_loc(chunk_wr *w)
{
    if (w)
        w->defer_loc = true;
}

static int wr_open(chunk_store *st, const chunk_ident *id, const chunk_geom *g, uint64_t epoch, uint64_t fence,
                   bool heal, bool replace_only, chunk_wr **out)
{
    if (st->read_only)
        return TSDB_ERR_READONLY;
    if (!chunk_geom_valid(g) || id->pos >= chunk_npos(g))
        return TSDB_ERR_INVALID;
    /* R15 D7 / §15.7: regular puts on single-server tiers (put_max_pct > 0) stop when integer filesystem fill %
     * (floor of used * 100 / total, using f_bavail) exceeds 95%; rebuild/heal puts pass. Sample outside st->mu, then
     * enforce after failed/fence/unsure checks inside st->mu so fenced or failed stores still report
     * CHUNK_ERR_FENCED/IO rather than CHUNK_ERR_FULL. */
    unsigned cap = atomic_load_explicit(&st->put_max_pct, memory_order_acquire);
    bool cap_full = !heal && cap > 0 && store_put_cap_active() && store_sample_fs(st) > cap;
    char target_name[80];
    chunk_name(target_name, sizeof target_name, id->oid, id->pos);
    pthread_mutex_lock(&st->mu);
    vol_state *v;
    int rc = atomic_load(&st->failed) ? TSDB_ERR_IO : vol_get(st, id->volume, !replace_only, &v);
    if (rc == 0 && fence < v->epoch && chunk_mutant != CHUNK_MUT_NOFENCE)
        rc = CHUNK_ERR_FENCED;
    if (rc == 0 && v->unsure)
        rc = CHUNK_ERR_EPOCH_UNSURE; /* a slot of EPOCH was damaged: no epoch is trusted until the owner raises it */
    if (rc == 0 && cap_full)
        rc = CHUNK_ERR_FULL;
    if (rc == 0 && fence > v->epoch && !(heal && chunk_mutant == CHUNK_MUT_REPLACE_LATE)) /* planted: raised later */
        rc = epoch_store(st, v, fence); /* before it acts on a higher one (E14) */
    if (rc == 0 && replace_only && faccessat(v->dirfd, target_name, F_OK, 0) != 0)
        rc = errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    int vdirfd = rc == 0 ? v->dirfd : -1;
    pthread_mutex_unlock(&st->mu);
    if (rc)
        return rc;
    chunk_wr *w = calloc(1, sizeof *w);
    if (!w)
        return TSDB_ERR_NOMEM;
    w->st = chunk_store_ref(st);
    w->id = *id;
    w->g = *g;
    w->epoch = epoch;
    w->fence = fence;
    w->replace_only = replace_only;
    w->vdirfd = vdirfd;
    w->fd = -1;
    w->ncells = chunk_pos_cells(g, id->pos);
    w->sums = malloc((w->ncells ? w->ncells : 1) * sizeof *w->sums);
    if (!w->sums) {
        chunk_store_unref(w->st);
        free(w);
        return TSDB_ERR_NOMEM;
    }
    chunk_hdr h;
    memset(&h, 0, sizeof h);
    memcpy(h.vuuid, id->vuuid, 16);
    memcpy(h.oid, id->oid, 16);
    h.epoch = epoch;
    h.g = *g;
    h.pos = id->pos;
    h.chunk_len = chunk_pos_len(g, id->pos);
    chunk_hdr_encode(&h, w->hdr);
    chunk_name(w->name, sizeof w->name, id->oid, id->pos);
    char hex[33];
    chunk_oid_hex(id->oid, hex);
    snprintf(w->tmp, sizeof w->tmp, "tmp_o%s.%u.%u", hex, id->pos, atomic_fetch_add(&st->seq, 1));
    w->fd = openat(vdirfd, w->tmp, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    if (w->fd < 0) {
        int e = errno;
        chunk_store_unref(w->st);
        free(w->sums);
        free(w);
        if (e == ENOSPC || e == EDQUOT)
            return CHUNK_ERR_FULL;
        if (e == EMFILE || e == ENFILE || e == ENOMEM)
            return TSDB_ERR_NOMEM;
        return TSDB_ERR_IO;
    }
    rc = wr_full(w->fd, w->hdr, CHUNK_HDR_BYTES);
    if (rc) {
        chunk_wr_abort(w);
        return rc;
    }
    *out = w;
    return 0;
}

int chunk_wr_open(chunk_store *st, const chunk_ident *id, const chunk_geom *g, uint64_t epoch, chunk_wr **out)
{
    return wr_open(st, id, g, epoch, epoch, false, false, out);
}

int chunk_wr_open_heal(chunk_store *st, const chunk_ident *id, const chunk_geom *g, uint64_t hdr_epoch,
                       uint64_t fence_epoch, chunk_wr **out)
{
    return wr_open(st, id, g, hdr_epoch, fence_epoch, true, false, out);
}

static int wr_cell(chunk_wr *w, const void *data, size_t len, uint64_t sum)
{
    if (w->err)
        return w->err;
    if (w->next >= w->ncells || len != chunk_pos_cell_len(&w->g, w->id.pos, w->next))
        return w->err = TSDB_ERR_INVALID;
    if (chunk_fault_write > 0 && --chunk_fault_write == 0) /* test hook: the write fails the way a bad disk's does */
        return w->err = TSDB_ERR_IO;
    int rc = wr_full(w->fd, data, len);
    if (rc)
        return w->err = rc;
    w->sums[w->next++] = sum;
    w->written += len;
    return 0;
}

int chunk_wr_cell(chunk_wr *w, const void *data, size_t len, bool sum_given, uint64_t sum)
{
    uint64_t have = xxh64(data, len, 0);
    if (sum_given && have != sum && chunk_mutant != CHUNK_MUT_NOSUMS) /* a chunk server checks what it receives (E26) */
        return w->err = TSDB_ERR_CORRUPT;
    return wr_cell(w, data, len, have);
}

int chunk_wr_cell_trusted(chunk_wr *w, const void *data, size_t len, uint64_t sum)
{
    return wr_cell(w, data, len, sum);
}

void chunk_wr_abort(chunk_wr *w)
{
    if (!w)
        return;
    if (w->fd >= 0) {
        close(w->fd);
        unlinkat(w->vdirfd, w->tmp, 0);
    }
    chunk_store_unref(w->st);
    free(w->sums);
    free(w);
}

int chunk_wr_sync(chunk_wr *w)
{
    if (!w)
        return TSDB_ERR_INVALID;
    if (w->synced)
        return 0;
    int rc = w->err;
    if (rc == 0 && w->next != w->ncells)
        rc = TSDB_ERR_INVALID; /* cells missing */
    if (rc == 0) {
        size_t tl = 8 * (size_t)w->ncells + CHUNK_FTR_BYTES;
        uint8_t *tail = malloc(tl);
        if (!tail)
            rc = TSDB_ERR_NOMEM;
        else {
            for (uint64_t i = 0; i < w->ncells; i++)
                chunk_put_le64(tail + 8 * i, w->sums[i]);
            chunk_ftr_make(tail + 8 * w->ncells, w->hdr, tail, w->ncells);
            rc = wr_full(w->fd, tail, tl);
            free(tail);
        }
    }
    if (rc == 0 && sync_fd(w->fd) != 0)
        rc = TSDB_ERR_IO; /* nothing named yet: just this call fails, and the file is dropped */
    if (rc == 0)
        w->synced = true;
    else
        w->err = rc;
    return rc;
}

int chunk_wr_commit(chunk_wr *w)
{
    chunk_store *st = w->st;
    int rc = w->synced ? w->err : chunk_wr_sync(w);
    if (rc) {
        chunk_wr_abort(w);
        return rc;
    }
    /* R13 r13-lmove: serialize replace-put commits (r12-repair) against concurrent chunk_move and MK_GC deletes on
     * the same (volume, oid) so a repair never races a move or resurrects a deleted chunk. */
    chunk_backend *be = st->owner_be;
    bool take_oid = w->replace_only && be != NULL;
    if (take_oid)
        chunk_be_oid_lock(be, w->id.volume, w->id.oid);
    pthread_mutex_lock(&st->mu);
    vol_state *v;
    if (atomic_load(&st->failed))
        rc = TSDB_ERR_IO;
    else if ((rc = vol_get(st, w->id.volume, true, &v)) == 0 && w->fence < v->epoch &&
             chunk_mutant != CHUNK_MUT_NOFENCE)
        rc = CHUNK_ERR_FENCED; /* the epoch moved on while the chunk was being written */
    if (rc == 0 && w->replace_only && faccessat(w->vdirfd, w->name, F_OK, 0) != 0)
        rc = errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO; /* D9: deleted before commit; never resurrect */
    if (rc == 0 && renameat(w->vdirfd, w->tmp, w->vdirfd, w->name) != 0)
        rc = TSDB_ERR_IO;
    pthread_mutex_unlock(&st->mu);
    if (rc) {
        if (take_oid)
            chunk_be_oid_unlock(be, w->id.volume, w->id.oid);
        chunk_wr_abort(w);
        return rc;
    }
    close(w->fd);
    w->fd = -1; /* named: the abort path must not unlink it */
    if (tsdb_fsync(w->vdirfd) != 0) {
        store_fail(st, "directory fsync after a chunk was named", errno);
        rc = TSDB_ERR_IO;
    }
    if (rc == 0 && be && !w->defer_loc) {
        int primary = chunk_be_place(be, w->id.oid, w->id.pos);
        if (st->owner_idx != primary)
            chunk_be_loc_set(be, w->id.volume, w->id.oid, w->id.pos, st->owner_idx);
        else if (atomic_load_explicit(&be->loc_cnt, memory_order_acquire) > 0)
            chunk_be_loc_del(be, w->id.volume, w->id.oid, w->id.pos);
    }
    if (take_oid)
        chunk_be_oid_unlock(be, w->id.volume, w->id.oid);
    chunk_store_unref(st);
    free(w->sums);
    free(w);
    return rc;
}

int chunk_store_put(chunk_store *st, const chunk_ident *id, const chunk_geom *g, uint64_t epoch, const void *cells,
                    const uint64_t *sums)
{
    chunk_wr *w;
    int rc = chunk_wr_open(st, id, g, epoch, &w);
    if (rc)
        return rc;
    const uint8_t *p = cells;
    for (uint64_t i = 0; i < chunk_pos_cells(g, id->pos) && rc == 0; i++) {
        size_t cl = (size_t)chunk_pos_cell_len(g, id->pos, i);
        rc = chunk_wr_cell(w, p, cl, sums != NULL, sums ? sums[i] : 0);
        p += cl;
    }
    if (rc) {
        chunk_wr_abort(w);
        return rc;
    }
    return chunk_wr_commit(w);
}

int chunk_store_del(chunk_store *st, const chunk_ident *id, uint64_t epoch)
{
    if (st->read_only)
        return TSDB_ERR_READONLY;
    char name[80];
    chunk_name(name, sizeof name, id->oid, id->pos);
    pthread_mutex_lock(&st->mu);
    vol_state *v;
    int rc = atomic_load(&st->failed) ? TSDB_ERR_IO : vol_get(st, id->volume, false, &v);
    if (rc == 0 && epoch < v->epoch && chunk_mutant != CHUNK_MUT_NOFENCE)
        rc = CHUNK_ERR_FENCED;
    if (rc == 0 && v->unsure)
        rc = CHUNK_ERR_EPOCH_UNSURE;
    if (rc == 0 && epoch > v->epoch)
        rc = epoch_store(st, v, epoch);
    if (rc == 0) {
        int fd = openat(v->dirfd, name, O_RDONLY | O_CLOEXEC);
        if (fd < 0)
            rc = errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
        else {
            uint8_t hb[CHUNK_HDR_BYTES];
            chunk_hdr h;
            struct stat sb;
            /* A header that does not parse says nothing of its writer: such a chunk may go (it is an erasure), and so
               may a file too short to hold one. A header that could not be READ proves nothing: the error stands. */
            if (fstat(fd, &sb) != 0)
                rc = TSDB_ERR_IO;
            else if ((uint64_t)sb.st_size >= CHUNK_HDR_BYTES) {
                int hr = rd_full(fd, hb, sizeof hb, 0);
                if (hr != 0)
                    rc = hr;
                else if (chunk_hdr_decode(hb, &h) == 0 && memcmp(h.vuuid, id->vuuid, 16) != 0)
                    rc = TSDB_ERR_NOTFOUND; /* another volume's chunk (two databases on one set of stores): not ours */
                else if (chunk_hdr_decode(hb, &h) == 0 && epoch < h.epoch && chunk_mutant != CHUNK_MUT_NOFENCE)
                    rc = CHUNK_ERR_FENCED; /* a delete below the chunk's own writer epoch (E14) */
            }
            close(fd);
        }
    }
    if (rc == 0 && unlinkat(v->dirfd, name, 0) != 0)
        rc = errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    int vfd = rc == 0 ? v->dirfd : -1;
    pthread_mutex_unlock(&st->mu);
    if (rc == 0 && tsdb_fsync(vfd) != 0) { /* the name may come back after a power loss: nothing more is acknowledged */
        store_fail(st, "directory fsync after a chunk was removed", errno);
        rc = TSDB_ERR_IO;
    }
    return rc;
}

/* ---------------------------------------------------------------- reading */
struct chunk_rd {
    int fd;
    chunk_hdr h;
    uint64_t *sums;
    uint64_t ncells;
};

/* Opens the chunk file at fd for reading. want_vuuid NULL: any (a listing does not know the volume's uuid). */
static int rd_load_ex(int fd, const uint8_t oid[16], unsigned pos, const uint8_t *want_vuuid, bool full, chunk_rd **out,
                      chunk_hdr *hdr_out, bool *real_io)
{
    if (real_io)
        *real_io = false;
    struct stat sb;
    if (fstat(fd, &sb) != 0) {
        if (real_io && is_storage_io_errno(errno))
            *real_io = true;
        return TSDB_ERR_IO;
    }
    uint64_t size = (uint64_t)sb.st_size;
    if (size < CHUNK_HDR_BYTES + CHUNK_FTR_BYTES)
        return TSDB_ERR_CORRUPT;
    uint8_t hb[CHUNK_HDR_BYTES];
    int rc = rd_full_ex(fd, hb, sizeof hb, 0, real_io);
    chunk_hdr h;
    if (rc)
        return rc;
    if (chunk_hdr_decode(hb, &h) != 0 || memcmp(h.oid, oid, 16) != 0 || h.pos != pos ||
        (want_vuuid && memcmp(h.vuuid, want_vuuid, 16) != 0) || size != chunk_file_size(&h.g, h.pos))
        return TSDB_ERR_CORRUPT; /* not parseable, or another chunk's file (misdirected), or not the size it says */
    if (hdr_out)
        *hdr_out = h;
    if (!full)
        return 0;
    uint64_t nc = chunk_pos_cells(&h.g, h.pos);
    size_t tl = 8 * (size_t)nc + CHUNK_FTR_BYTES;
    uint8_t *tail = malloc(tl);
    if (!tail)
        return TSDB_ERR_NOMEM;
    rc = rd_full_ex(fd, tail, tl, CHUNK_HDR_BYTES + h.chunk_len, real_io);
    if (rc == 0 && chunk_ftr_check(hb, tail, nc, tail + 8 * nc) != 0)
        rc = TSDB_ERR_CORRUPT;
    if (rc) {
        free(tail);
        return rc;
    }
    chunk_rd *r = calloc(1, sizeof *r);
    uint64_t *sums = malloc((nc ? nc : 1) * sizeof *sums);
    if (!r || !sums) {
        free(r);
        free(sums);
        free(tail);
        return TSDB_ERR_NOMEM;
    }
    for (uint64_t i = 0; i < nc; i++)
        sums[i] = chunk_le64(tail + 8 * i);
    free(tail);
    r->fd = fd;
    r->h = h;
    r->sums = sums;
    r->ncells = nc;
    *out = r;
    return 0;
}

static int rd_load(int fd, const uint8_t oid[16], unsigned pos, const uint8_t *want_vuuid, bool full, chunk_rd **out,
                   chunk_hdr *hdr_out)
{
    return rd_load_ex(fd, oid, pos, want_vuuid, full, out, hdr_out, NULL);
}

static int rd_open_impl(chunk_store *st, uint32_t volume, const uint8_t oid[16], unsigned pos,
                        const uint8_t *want_vuuid, bool scrub_mode, chunk_rd **out, bool *real_io)
{
    if (real_io)
        *real_io = false;
    pthread_mutex_lock(&st->mu);
    vol_state *v;
    int rc = vol_get(st, volume, false, &v);
    int vfd = rc == 0 ? v->dirfd : -1;
    pthread_mutex_unlock(&st->mu);
    if (rc)
        return rc;
    char name[80];
    chunk_name(name, sizeof name, oid, pos);
    int fd = openat(vfd, name, O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        int e = errno;
        if (e == ENOENT)
            return TSDB_ERR_NOTFOUND;
        if (scrub_mode && (e == EMFILE || e == ENFILE || e == ENOMEM))
            return TSDB_ERR_NOMEM; /* transient resource pressure, not disk damage */
        if (real_io && is_storage_io_errno(e))
            *real_io = true;
        return TSDB_ERR_IO;
    }
    rc = rd_load_ex(fd, oid, pos, want_vuuid, true, out, NULL, real_io);
    if (rc)
        close(fd);
    return rc;
}

static int chunk_rd_open_ex(chunk_store *st, const chunk_ident *id, chunk_rd **out, bool *real_io)
{
    return rd_open_impl(st, id->volume, id->oid, id->pos, id->vuuid, false, out, real_io);
}

int chunk_rd_open(chunk_store *st, const chunk_ident *id, chunk_rd **out)
{
    return chunk_rd_open_ex(st, id, out, NULL);
}

int chunk_rd_open_any(chunk_store *st, uint32_t volume, const uint8_t oid[16], unsigned pos, chunk_rd **out)
{
    return rd_open_impl(st, volume, oid, pos, NULL, true, out, NULL);
}

void chunk_rd_close(chunk_rd *r)
{
    if (!r)
        return;
    close(r->fd);
    free(r->sums);
    free(r);
}

const chunk_hdr *chunk_rd_hdr(const chunk_rd *r) { return &r->h; }
const uint64_t *chunk_rd_sums(const chunk_rd *r) { return r->sums; }

int chunk_rd_stat(const chunk_rd *r, dev_t *dev_out, ino_t *ino_out)
{
    if (!r || r->fd < 0)
        return TSDB_ERR_INVALID;
    struct stat sb;
    if (fstat(r->fd, &sb) != 0)
        return TSDB_ERR_IO;
    if (dev_out)
        *dev_out = sb.st_dev;
    if (ino_out)
        *ino_out = sb.st_ino;
    return 0;
}

int chunk_store_stat_chunk(chunk_store *st, uint32_t volume, const uint8_t oid[16], unsigned pos, dev_t *dev_out,
                           ino_t *ino_out)
{
    if (!st || pos >= CHUNK_POS_MAX)
        return TSDB_ERR_INVALID;
    pthread_mutex_lock(&st->mu);
    vol_state *v;
    int rc = atomic_load(&st->failed) ? TSDB_ERR_IO : vol_get(st, volume, false, &v);
    int vfd = rc == 0 ? v->dirfd : -1;
    pthread_mutex_unlock(&st->mu);
    if (rc != 0)
        return rc;
    char name[80];
    chunk_name(name, sizeof name, oid, pos);
    struct stat sb;
    if (fstatat(vfd, name, &sb, AT_SYMLINK_NOFOLLOW) != 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    if (dev_out)
        *dev_out = sb.st_dev;
    if (ino_out)
        *ino_out = sb.st_ino;
    return 0;
}

int chunk_rd_read_ex(chunk_rd *r, uint64_t off, void *buf, size_t len, bool *real_io)
{
    if (real_io)
        *real_io = false;
    if (off > r->h.chunk_len || len > r->h.chunk_len - off)
        return TSDB_ERR_INVALID;
    return rd_full_ex(r->fd, buf, len, CHUNK_HDR_BYTES + off, real_io);
}

int chunk_rd_read(chunk_rd *r, uint64_t off, void *buf, size_t len)
{
    return chunk_rd_read_ex(r, off, buf, len, NULL);
}

int chunk_rd_scrub_ex(chunk_rd *r, uint64_t *bad, uint64_t *bad_bitmap, uint64_t *bytes_out,
                      chunk_scrub_cell_cb on_cell, void *ctx)
{
    uint64_t nbad = 0, bmap = 0, off = 0, total = 0;
    size_t cap = r->h.g.cell < r->h.chunk_len ? r->h.g.cell : (size_t)r->h.chunk_len;
    uint8_t *buf = malloc(cap ? cap : 1);
    if (!buf)
        return TSDB_ERR_NOMEM;
    int rc = 0;
    uint64_t limit = r->ncells;
#ifdef TSDB_TEST_HOOKS
    if (chunkscrub_mutant == CHUNKSCRUB_MUT_SKIPCELL && limit > 0) /* planted bug: skips the last cell */
        limit--;
#endif
    for (uint64_t i = 0; i < limit && rc == 0; i++) {
        size_t cl = (size_t)chunk_pos_cell_len(&r->h.g, r->h.pos, i);
        rc = chunk_rd_read(r, off, buf, cl);
        if (rc == 0) {
            total += cl;
            if (xxh64(buf, cl, 0) != r->sums[i]) {
                nbad++;
                bmap |= (i < 63 ? (1ull << i) : (1ull << 63));
            }
            if (on_cell) {
                int cbrc = on_cell(ctx, cl);
                if (cbrc != 0)
                    rc = cbrc;
            }
        }
        off += cl;
    }
    free(buf);
    if (bad)
        *bad = nbad;
    if (bad_bitmap)
        *bad_bitmap = bmap;
    if (bytes_out)
        *bytes_out = total;
    return rc;
}

int chunk_rd_scrub(chunk_rd *r, uint64_t *bad)
{
    return chunk_rd_scrub_ex(r, bad, NULL, NULL, NULL, NULL);
}

/* What store_list says of one chunk file (c->oid and c->pos set): 0, or 1 when it is not there (removed under a scan) */
static int list_stat(int vfd, const char *n, bool verify, chunk_listed *c)
{
    int fd = openat(vfd, n, O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        if (errno == ENOENT)
            return 1;
        c->status = CHUNK_ST_IOERR; /* unreadable */
        return 0;
    }
    struct stat sb;
    chunk_hdr h;
    chunk_rd *r = NULL;
    c->size = fstat(fd, &sb) == 0 ? (uint64_t)sb.st_size : 0;
    int lr = rd_load(fd, c->oid, c->pos, NULL, verify, &r, &h);
    c->status = lr == 0 ? CHUNK_ST_OK : lr == TSDB_ERR_IO ? CHUNK_ST_IOERR : CHUNK_ST_DAMAGED;
    if (lr == 0) {
        c->epoch = h.epoch;
        memcpy(c->vuuid, h.vuuid, 16);
    }
    if (r)
        chunk_rd_close(r);
    else
        close(fd);
    return 0;
}

/* names_only: what the file name says (oid, pos), no file opened: the sweep of orphans (src/ecgc.c) reads a header
 * only for an object that nothing names, so that an open costs one readdir per store, not one open per chunk */
static int store_list(chunk_store *st, uint32_t volume, bool verify, bool names_only, chunk_list_fn fn, void *ctx)
{
    pthread_mutex_lock(&st->mu);
    vol_state *v;
    int rc = vol_get(st, volume, false, &v);
    int vfd = rc == 0 ? v->dirfd : -1;
    pthread_mutex_unlock(&st->mu);
    if (rc == TSDB_ERR_NOTFOUND)
        return 0;
    if (rc)
        return rc;
    int dup_fd = dup(vfd);
    DIR *d = dup_fd >= 0 ? fdopendir(dup_fd) : NULL;
    if (!d) {
        if (dup_fd >= 0)
            close(dup_fd);
        return TSDB_ERR_IO;
    }
    rewinddir(d);
    struct dirent *e;
    int derr = 0;
    while (rc == 0 && (e = next_entry(d, &derr)) != NULL) {
        chunk_listed c;
        memset(&c, 0, sizeof c);
        const char *n = e->d_name;
        char *end;
        if (n[0] != 'o' || strlen(n) < 1 + 32 + 1 + 1 + 6 || !hex_parse(n + 1, 32, c.oid) || n[33] != '.')
            continue;
        unsigned long pos = strtoul(n + 34, &end, 10);
        if (pos >= CHUNK_POS_MAX || strcmp(end, ".chunk") != 0)
            continue;
        c.pos = (uint16_t)pos;
        if (names_only) {
            rc = fn(ctx, &c);
            continue;
        }
        if (list_stat(vfd, n, verify, &c) != 0)
            continue; /* removed under the scan */
        rc = fn(ctx, &c);
    }
    closedir(d);
    return rc == 0 && derr ? TSDB_ERR_IO : rc; /* a failing readdir is not the end of the directory */
}

int chunk_store_list(chunk_store *st, uint32_t volume, bool verify, chunk_list_fn fn, void *ctx)
{
    return store_list(st, volume, verify, false, fn, ctx);
}

int chunk_store_list_names(chunk_store *st, uint32_t volume, chunk_list_fn fn, void *ctx)
{
    return store_list(st, volume, false, true, fn, ctx);
}

int chunk_store_list_one(chunk_store *st, uint32_t volume, const uint8_t oid[16], unsigned pos, bool verify,
                         chunk_listed *out)
{
    memset(out, 0, sizeof *out);
    if (pos >= CHUNK_POS_MAX)
        return TSDB_ERR_INVALID;
    pthread_mutex_lock(&st->mu);
    vol_state *v;
    int rc = vol_get(st, volume, false, &v);
    int vfd = rc == 0 ? v->dirfd : -1;
    pthread_mutex_unlock(&st->mu);
    if (rc)
        return rc;
    memcpy(out->oid, oid, 16);
    out->pos = (uint16_t)pos;
    char name[80];
    chunk_name(name, sizeof name, oid, pos);
    return list_stat(vfd, name, verify, out) ? TSDB_ERR_NOTFOUND : 0;
}

/* ---------------------------------------------------------------- the directory backend */
int chunk_store_peek(chunk_store *st, uint32_t volume, const uint8_t oid[16], unsigned pos, chunk_hdr *h)
{
    pthread_mutex_lock(&st->mu);
    vol_state *v;
    int rc = vol_get(st, volume, false, &v);
    int vfd = rc == 0 ? v->dirfd : -1;
    pthread_mutex_unlock(&st->mu);
    if (rc)
        return rc;
    char name[80];
    chunk_name(name, sizeof name, oid, pos);
    int fd = openat(vfd, name, O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        if (errno == ENOENT)
            return TSDB_ERR_NOTFOUND;
        if (errno == EMFILE || errno == ENFILE || errno == ENOMEM)
            return TSDB_ERR_NOMEM; /* transient resource pressure, not disk damage */
        return TSDB_ERR_IO;
    }
    rc = rd_load(fd, oid, pos, NULL, false, NULL, h);
    close(fd);
    return rc;
}

const char *chunk_store_state_name(int s)
{
    switch (s) {
    case CHUNK_STORE_UP: return "UP";
    case CHUNK_STORE_DOWN_MISSING: return "DOWN_MISSING";
    case CHUNK_STORE_DOWN_OTHER_ID: return "DOWN_OTHER_ID";
    case CHUNK_STORE_DOWN_DAMAGED: return "DOWN_DAMAGED";
    case CHUNK_STORE_DOWN_IOERR: return "DOWN_IOERR";
    case CHUNK_STORE_DOWN_STALLED: return "DOWN_STALLED";
    case CHUNK_STORE_REPLACING: return "REPLACING";
    default: return "?";
    }
}

static struct fence *fence_find(chunk_backend *be, uint32_t vol, bool add)
{
    for (size_t i = 0; i < be->nf; i++)
        if (be->f[i].vol == vol)
            return &be->f[i];
    if (!add)
        return NULL;
    if (be->nf == be->capf) {
        size_t nc = be->capf ? 2 * be->capf : 8;
        struct fence *nf = realloc(be->f, nc * sizeof *nf);
        if (!nf)
            return NULL;
        be->f = nf;
        be->capf = nc;
    }
    be->f[be->nf] = (struct fence){vol, 0};
    return &be->f[be->nf++];
}

chunk_store *chunk_be_raw_store(const chunk_backend *be, int i)
{
    int n = be ? atomic_load_explicit(&be->n, memory_order_acquire) : 0;
    return be && i >= 0 && i < n ? atomic_load_explicit(&be->st[i], memory_order_acquire) : NULL;
}

chunk_store *chunk_be_store_ref(const chunk_backend *be, int i)
{
    int n = be ? atomic_load_explicit(&be->n, memory_order_acquire) : 0;
    if (!be || i < 0 || i >= n)
        return NULL;
    chunk_backend *mbe = (chunk_backend *)be;
    pthread_mutex_lock(&mbe->st_mu);
    int s = atomic_load_explicit(&mbe->state[i], memory_order_acquire);
    chunk_store *st = (s == CHUNK_STORE_UP || s == CHUNK_STORE_REPLACING || chunk_mutant == CHUNK_MUT_DOWNNOREAD)
                          ? atomic_load_explicit(&mbe->st[i], memory_order_relaxed)
                          : NULL;
    if (st)
        chunk_store_ref(st);
    pthread_mutex_unlock(&mbe->st_mu);
    return st;
}

chunk_store *chunk_be_raw_store_ref(const chunk_backend *be, int i)
{
    int n = be ? atomic_load_explicit(&be->n, memory_order_acquire) : 0;
    if (!be || i < 0 || i >= n)
        return NULL;
    chunk_backend *mbe = (chunk_backend *)be;
    pthread_mutex_lock(&mbe->st_mu);
    chunk_store *st = atomic_load_explicit(&mbe->st[i], memory_order_relaxed);
    if (st)
        chunk_store_ref(st);
    pthread_mutex_unlock(&mbe->st_mu);
    return st;
}

chunk_store *chunk_be_open_store(const chunk_backend *be, int i)
{
    int n = be ? atomic_load_explicit(&be->n, memory_order_acquire) : 0;
    if (!be || i < 0 || i >= n)
        return NULL;
    int s = atomic_load_explicit(&be->state[i], memory_order_acquire);
    if (s != CHUNK_STORE_UP && s != CHUNK_STORE_REPLACING)
        return NULL;
    return atomic_load_explicit(&be->st[i], memory_order_acquire);
}

chunk_store *chunk_be_store(chunk_backend *be, int i) { return chunk_be_open_store(be, i); }

int chunk_be_store_state(const chunk_backend *be, int i)
{
    int n = be ? atomic_load_explicit(&be->n, memory_order_acquire) : 0;
    if (!be || i < 0 || i >= n)
        return -1;
    int s = atomic_load_explicit(&be->state[i], memory_order_acquire);
    if ((s == CHUNK_STORE_UP || s == CHUNK_STORE_REPLACING) &&
        atomic_load_explicit(&be->st_failed[i], memory_order_acquire))
        return CHUNK_STORE_DOWN_IOERR;
    return s;
}

int chunk_be_mark_down(chunk_backend *be, int i, const chunk_store *expected, int state)
{
    int n = be ? atomic_load_explicit(&be->n, memory_order_acquire) : 0;
    if (!be || i < 0 || i >= n ||
        (state != CHUNK_STORE_DOWN_DAMAGED && state != CHUNK_STORE_DOWN_IOERR))
        return 0;
    pthread_mutex_lock(&be->st_mu);
    int changed = 0;
    chunk_store *cur = atomic_load_explicit(&be->st[i], memory_order_acquire);
    if ((!expected || cur == expected) && !atomic_load_explicit(&be->st_failed[i], memory_order_acquire) &&
        (!cur || !chunk_store_failed(cur))) {
        int exp_state = CHUNK_STORE_UP;
        changed = atomic_compare_exchange_strong(&be->state[i], &exp_state, state) ? 1 : 0;
    }
    pthread_mutex_unlock(&be->st_mu);
    return changed;
}

void chunk_be_set_lost(chunk_backend *be, int i, bool lost)
{
    int n = be ? atomic_load_explicit(&be->n, memory_order_acquire) : 0;
    if (!be || i < 0 || i >= n)
        return;
    atomic_store_explicit(&be->st_lost[i], lost, memory_order_release);
}

bool chunk_be_store_is_lost(const chunk_backend *be, int i)
{
    int n = be ? atomic_load_explicit(&be->n, memory_order_acquire) : 0;
    if (!be || i < 0 || i >= n)
        return false;
    return atomic_load_explicit(&be->st_lost[i], memory_order_acquire);
}

void chunk_be_set_excluded(chunk_backend *be, int i, bool excluded)
{
    int n = be ? atomic_load_explicit(&be->n, memory_order_acquire) : 0;
    if (!be || i < 0 || i >= n || !be->st_excluded)
        return;
    atomic_store_explicit(&be->st_excluded[i], excluded, memory_order_release);
}

bool chunk_be_store_excluded(const chunk_backend *be, int i)
{
    int n = be ? atomic_load_explicit(&be->n, memory_order_acquire) : 0;
    if (!be || i < 0 || i >= n || !be->st_excluded)
        return false;
    return atomic_load_explicit(&be->st_excluded[i], memory_order_acquire);
}

int chunk_be_active_count(const chunk_backend *be)
{
    int n = be ? atomic_load_explicit(&be->n, memory_order_acquire) : 0;
    int active = 0;
    for (int i = 0; i < n; i++)
        if (!be->st_excluded || !atomic_load_explicit(&be->st_excluded[i], memory_order_acquire))
            active++;
    return active;
}

int chunk_be_mark_lost(chunk_backend *be, int i)
{
    int n = be ? atomic_load_explicit(&be->n, memory_order_acquire) : 0;
    if (!be || i < 0 || i >= n)
        return TSDB_ERR_INVALID;
    pthread_mutex_lock(&be->st_mu);
    atomic_store_explicit(&be->st_lost[i], true, memory_order_release);
    atomic_store_explicit(&be->state[i], CHUNK_STORE_DOWN_MISSING, memory_order_release);
    atomic_store_explicit(&be->st_failed[i], false, memory_order_release);
    chunk_store *old = atomic_load_explicit(&be->st[i], memory_order_relaxed);
    atomic_store_explicit(&be->st[i], NULL, memory_order_release);
    if (old) {
        old->next_pending = be->pending_close[i];
        be->pending_close[i] = old;
    }
    pthread_mutex_unlock(&be->st_mu);
    return 0;
}

int chunk_be_set_replacing(chunk_backend *be, int i)
{
    int n = be ? atomic_load_explicit(&be->n, memory_order_acquire) : 0;
    if (!be || i < 0 || i >= n)
        return TSDB_ERR_INVALID;
    pthread_mutex_lock(&be->st_mu);
    chunk_store *st = atomic_load_explicit(&be->st[i], memory_order_relaxed);
    if (!st) {
        pthread_mutex_unlock(&be->st_mu);
        return TSDB_ERR_UNAVAILABLE;
    }
    atomic_store_explicit(&be->st_lost[i], false, memory_order_release);
    atomic_store_explicit(&be->state[i], CHUNK_STORE_REPLACING, memory_order_release);
    pthread_mutex_unlock(&be->st_mu);
    return 0;
}

bool chunk_be_store_path_copy(const chunk_backend *be, int i, char *buf, size_t cap)
{
    int n = be ? atomic_load_explicit(&be->n, memory_order_acquire) : 0;
    if (!be || i < 0 || i >= n || !buf || cap == 0)
        return false;
    chunk_backend *mbe = (chunk_backend *)be;
    bool ok = false;
    pthread_mutex_lock(&mbe->st_mu);
    if (mbe->path[i]) {
        snprintf(buf, cap, "%s", mbe->path[i]);
        ok = true;
    } else {
        buf[0] = '\0';
    }
    pthread_mutex_unlock(&mbe->st_mu);
    return ok;
}

void chunk_be_release_down(chunk_backend *be, int i)
{
    int n = be ? atomic_load_explicit(&be->n, memory_order_acquire) : 0;
    if (!be || i < 0 || i >= n)
        return;
    pthread_mutex_lock(&be->st_mu);
    chunk_store *list = be->pending_close ? be->pending_close[i] : NULL;
    if (be->pending_close)
        be->pending_close[i] = NULL;
    pthread_mutex_unlock(&be->st_mu);
    while (list) {
        chunk_store *nxt = list->next_pending;
        list->next_pending = NULL;
        chunk_store_unref(list);
        list = nxt;
    }
}

int chunk_be_set_down(chunk_backend *be, int i, int reason)
{
    int n = be ? atomic_load_explicit(&be->n, memory_order_acquire) : 0;
    if (!be || i < 0 || i >= n)
        return TSDB_ERR_INVALID;
    if (reason != CHUNK_STORE_DOWN_IOERR && reason != CHUNK_STORE_DOWN_STALLED)
        return TSDB_ERR_INVALID;
    bool changed = false;
    char pbuf[PATH_MAX];
    pbuf[0] = '\0';
    pthread_mutex_lock(&be->st_mu);
    int cur = atomic_load_explicit(&be->state[i], memory_order_relaxed);
    if (cur == CHUNK_STORE_UP || cur == CHUNK_STORE_REPLACING) {
        atomic_store_explicit(&be->state[i], reason, memory_order_release);
        atomic_store_explicit(&be->st_failed[i], false, memory_order_release);
        chunk_store *old = atomic_load_explicit(&be->st[i], memory_order_relaxed);
        if (chunk_mutant != CHUNK_MUT_DOWNNOREAD) {
            atomic_store_explicit(&be->st[i], NULL, memory_order_release);
            /* Park old on pending_close[i] instead of unreffing/closing here: close(dirfd) releases an NFSv4 flock
             * and can block indefinitely on a dead server, which would stall domain/monitor/reader threads. The
             * attach worker (or chunk_be_close) calls chunk_be_release_down() right before reopening. */
            if (old) {
                old->next_pending = be->pending_close[i];
                be->pending_close[i] = old;
            }
        }
        snprintf(pbuf, sizeof pbuf, "%s", be->path[i] ? be->path[i] : "-");
        if (reason == CHUNK_STORE_DOWN_STALLED)
            atomic_fetch_add_explicit(&be->stall_gen, 1, memory_order_release);
        changed = true;
    }
    pthread_mutex_unlock(&be->st_mu);
    if (changed)
        fprintf(stderr, "tsdb: chunk backend: store %d (%s) is %s\n", i, pbuf, chunk_store_state_name(reason));
    return 0;
}

uint64_t chunk_be_stall_gen(const chunk_backend *be)
{
    return be ? atomic_load_explicit(&be->stall_gen, memory_order_acquire) : 0;
}

void chunk_be_set_ioerr_cb(chunk_backend *be, void (*fn)(void *ctx, int store), void *ctx)
{
    if (!be)
        return;
    pthread_mutex_lock(&be->st_mu);
    be->on_ioerr = fn;
    be->ioerr_ctx = ctx;
    if (!fn) {
        while (be->ioerr_active > 0)
            pthread_cond_wait(&be->ioerr_cv, &be->st_mu);
    }
    pthread_mutex_unlock(&be->st_mu);
}

void chunk_be_note_ioerr(chunk_backend *be, int store)
{
    int n = be ? atomic_load_explicit(&be->n, memory_order_acquire) : 0;
    if (!be || store < 0 || store >= n)
        return;
    pthread_mutex_lock(&be->st_mu);
    void (*fn)(void *, int) = be->on_ioerr;
    void *ctx = be->ioerr_ctx;
    if (fn)
        be->ioerr_active++;
    pthread_mutex_unlock(&be->st_mu);
    if (fn) {
        fn(ctx, store);
        pthread_mutex_lock(&be->st_mu);
        if (--be->ioerr_active == 0)
            pthread_cond_broadcast(&be->ioerr_cv);
        pthread_mutex_unlock(&be->st_mu);
    }
}

/* ---------------------------------------------------------------- per-oid lock, draining, local index & open dedupe */

static size_t be_oid_stripe(uint32_t volume, const uint8_t oid[16])
{
    uint64_t h = xxh64(oid, 16, (uint64_t)volume);
    return (size_t)(h & (CHUNK_OID_STRIPES - 1u));
}

void chunk_be_oid_lock(chunk_backend *be, uint32_t volume, const uint8_t oid[16])
{
    if (be && oid)
        pthread_mutex_lock(&be->oid_mu[be_oid_stripe(volume, oid)]);
}

void chunk_be_oid_unlock(chunk_backend *be, uint32_t volume, const uint8_t oid[16])
{
    if (be && oid)
        pthread_mutex_unlock(&be->oid_mu[be_oid_stripe(volume, oid)]);
}

void chunk_be_set_draining(chunk_backend *be, int i, bool draining)
{
    int n = be ? atomic_load_explicit(&be->n, memory_order_acquire) : 0;
    if (!be || i < 0 || i >= n || !be->st_draining)
        return;
    atomic_store_explicit(&be->st_draining[i], draining, memory_order_release);
}

bool chunk_be_store_draining(const chunk_backend *be, int i)
{
    int n = be ? atomic_load_explicit(&be->n, memory_order_acquire) : 0;
    if (!be || i < 0 || i >= n || !be->st_draining)
        return false;
    return atomic_load_explicit(&be->st_draining[i], memory_order_acquire);
}

static uint64_t loc_hash(uint32_t vol, const uint8_t oid[16], unsigned pos)
{
    uint64_t h = xxh64(oid, 16, ((uint64_t)vol << 16) | (uint64_t)pos);
    return h ? h : 1;
}

static bool loc_eq(const chunk_loc_ent *e, uint32_t vol, const uint8_t oid[16], unsigned pos)
{
    return e->vol == vol && e->pos == (uint16_t)pos && memcmp(e->oid, oid, 16) == 0;
}

static int loc_rehash_locked(chunk_backend *be, size_t ncap)
{
    chunk_loc_ent *ntab = calloc(ncap, sizeof *ntab);
    if (!ntab)
        return TSDB_ERR_NOMEM;
    size_t mask = ncap - 1u;
    size_t cnt = 0;
    for (size_t i = 0; i < be->loc_cap; i++) {
        if (be->loc_tab[i].state != 1)
            continue;
        size_t s = (size_t)loc_hash(be->loc_tab[i].vol, be->loc_tab[i].oid, be->loc_tab[i].pos) & mask;
        while (ntab[s].state == 1)
            s = (s + 1u) & mask;
        ntab[s] = be->loc_tab[i];
        cnt++;
    }
    free(be->loc_tab);
    be->loc_tab = ntab;
    be->loc_cap = ncap;
    be->loc_n = cnt;
    be->loc_used = cnt;
    return 0;
}

int chunk_be_loc_get(const chunk_backend *be, uint32_t volume, const uint8_t oid[16], unsigned pos)
{
    if (!be || !oid || pos >= CHUNK_POS_MAX)
        return -1;
    if (atomic_load_explicit(&be->loc_cnt, memory_order_acquire) == 0)
        return -1;
    chunk_backend *mbe = (chunk_backend *)be;
    int store = -1;
    pthread_mutex_lock(&mbe->loc_mu);
    if (mbe->loc_cap > 0 && mbe->loc_n > 0) {
        size_t mask = mbe->loc_cap - 1u;
        size_t s = (size_t)loc_hash(volume, oid, pos) & mask;
        for (size_t p = 0; p < mbe->loc_cap; p++) {
            const chunk_loc_ent *e = &mbe->loc_tab[s];
            if (e->state == 0)
                break;
            if (e->state == 1 && loc_eq(e, volume, oid, pos)) {
                store = e->store;
                break;
            }
            s = (s + 1u) & mask;
        }
    }
    pthread_mutex_unlock(&mbe->loc_mu);
    return store;
}

void chunk_be_loc_set(chunk_backend *be, uint32_t volume, const uint8_t oid[16], unsigned pos, int store)
{
    if (!be || !oid || pos >= CHUNK_POS_MAX || store < 0)
        return;
    pthread_mutex_lock(&be->loc_mu);
    if (be->loc_cap == 0 || (be->loc_used + 1u) * 10u >= be->loc_cap * 7u) {
        size_t ncap = be->loc_cap ? be->loc_cap : 64u;
        if (be->loc_cap > 0 && (be->loc_n + 1u) * 10u >= be->loc_cap * 6u)
            ncap *= 2u;
        if (loc_rehash_locked(be, ncap) != 0) {
            pthread_mutex_unlock(&be->loc_mu);
            return;
        }
    }
    size_t mask = be->loc_cap - 1u;
    size_t s = (size_t)loc_hash(volume, oid, pos) & mask;
    size_t first_tomb = SIZE_MAX;
    for (size_t p = 0; p < be->loc_cap; p++) {
        chunk_loc_ent *e = &be->loc_tab[s];
        if (e->state == 0) {
            size_t ins = first_tomb != SIZE_MAX ? first_tomb : s;
            if (be->loc_tab[ins].state == 0)
                be->loc_used++;
            be->loc_tab[ins].vol = volume;
            be->loc_tab[ins].pos = (uint16_t)pos;
            be->loc_tab[ins].state = 1;
            be->loc_tab[ins].store = store;
            memcpy(be->loc_tab[ins].oid, oid, 16);
            be->loc_n++;
            atomic_store_explicit(&be->loc_cnt, be->loc_n, memory_order_release);
            break;
        }
        if (e->state == 1 && loc_eq(e, volume, oid, pos)) {
            e->store = store;
            break;
        }
        if (e->state == 2 && first_tomb == SIZE_MAX)
            first_tomb = s;
        s = (s + 1u) & mask;
    }
    pthread_mutex_unlock(&be->loc_mu);
}

void chunk_be_loc_del(chunk_backend *be, uint32_t volume, const uint8_t oid[16], unsigned pos)
{
    if (!be || !oid || pos >= CHUNK_POS_MAX)
        return;
    if (atomic_load_explicit(&be->loc_cnt, memory_order_acquire) == 0)
        return;
    pthread_mutex_lock(&be->loc_mu);
    if (be->loc_cap > 0 && be->loc_n > 0) {
        size_t mask = be->loc_cap - 1u;
        size_t s = (size_t)loc_hash(volume, oid, pos) & mask;
        for (size_t p = 0; p < be->loc_cap; p++) {
            chunk_loc_ent *e = &be->loc_tab[s];
            if (e->state == 0)
                break;
            if (e->state == 1 && loc_eq(e, volume, oid, pos)) {
                e->state = 2;
                be->loc_n--;
                atomic_store_explicit(&be->loc_cnt, be->loc_n, memory_order_release);
                break;
            }
            s = (s + 1u) & mask;
        }
    }
    pthread_mutex_unlock(&be->loc_mu);
}

/* Unlinks a duplicate or damaged chunk file during start-time dedupe without touching EPOCH (safe even on an unsure
 * volume). Fail-stops the store if directory fsync fails. */
static int store_unlink_dedupe(chunk_store *st, uint32_t vol, const uint8_t oid[16], unsigned pos)
{
    char name[80];
    chunk_name(name, sizeof name, oid, pos);
    pthread_mutex_lock(&st->mu);
    vol_state *v;
    int rc = atomic_load(&st->failed) ? TSDB_ERR_IO : vol_get(st, vol, false, &v);
    int vfd = -1;
    if (rc == 0) {
        if (unlinkat(v->dirfd, name, 0) != 0)
            rc = errno == ENOENT ? 0 : TSDB_ERR_IO;
        else
            vfd = v->dirfd;
    }
    pthread_mutex_unlock(&st->mu);
    if (rc == 0 && vfd >= 0 && tsdb_fsync(vfd) != 0) {
        store_fail(st, "directory fsync after start-time dedupe unlink", errno);
        rc = TSDB_ERR_IO;
    }
    return rc;
}

typedef struct dedupe_ent {
    uint8_t oid[16];
    uint16_t pos;
    int store;
} dedupe_ent;

typedef struct dedupe_vec {
    dedupe_ent *v;
    size_t n, cap;
    int store;
    int err;
} dedupe_vec;

static int dedupe_add(void *vctx, const chunk_listed *c)
{
    dedupe_vec *dv = vctx;
    if (dv->n == dv->cap) {
        size_t nc = dv->cap ? 2 * dv->cap : 128;
        dedupe_ent *nv = realloc(dv->v, nc * sizeof *nv);
        if (!nv) {
            dv->err = TSDB_ERR_NOMEM;
            return 1;
        }
        dv->v = nv;
        dv->cap = nc;
    }
    memcpy(dv->v[dv->n].oid, c->oid, 16);
    dv->v[dv->n].pos = c->pos;
    dv->v[dv->n].store = dv->store;
    dv->n++;
    return 0;
}

static int dedupe_cmp(const void *a, const void *b)
{
    const dedupe_ent *x = a, *y = b;
    int c = memcmp(x->oid, y->oid, 16);
    if (c != 0)
        return c;
    if (x->pos != y->pos)
        return (x->pos > y->pos) - (x->pos < y->pos);
    return (x->store > y->store) - (x->store < y->store);
}

static bool dedupe_store_better(const chunk_backend *be, int cand, int best, const uint8_t oid[16], unsigned pos)
{
    int pc = be->store_pref ? be->store_pref[cand] : 0;
    int pb = be->store_pref ? be->store_pref[best] : 0;
#ifdef TSDB_TEST_HOOKS
    bool invert = chunkmove_mutant == CHUNKMOVE_MUT_KEEPWRONG;
#else
    bool invert = false;
#endif
    if (pc != pb)
        return invert ? (pc < pb) : (pc > pb);
    int placed = chunk_be_place(be, oid, pos);
    bool c_pl = (cand == placed);
    bool b_pl = (best == placed);
    if (c_pl != b_pl)
        return invert ? !c_pl : c_pl;
    return invert ? (cand < best) : (cand > best);
}

enum { DEDUPE_ABSENT = 0, DEDUPE_WHOLE = 1, DEDUPE_DAMAGED = 2, DEDUPE_IOERR = 3 };

/* R13 r13-lmove (E50, §15.3): scan open stores at backend open (only when store_pref is supplied, so single-server
 * tiers on NFS pay zero startup scan), and whenever the same (volume, oid, pos) exists in two or more stores verify
 * every copy against its encode-time cell sums, keep the whole copy on the preferred store (capacity over fast / drain
 * target over draining), drop damaged copies and non-preferred duplicates (unlink + dir fsync). Single-copy chunks are
 * NOT inserted into loc_tab (keeping normal backends at 0 bytes / 0 lock contention); only off-primary winners are. */
static void be_open_dedupe(chunk_backend *be)
{
    if (!be || be->read_only || !be->has_store_pref)
        return;
    int bn = atomic_load_explicit(&be->n, memory_order_relaxed);
    int up = 0;
    for (int i = 0; i < bn; i++)
        if (atomic_load(&be->st[i]))
            up++;
    if (up < 2 || be->nf == 0)
        return;

    for (size_t vi = 0; vi < be->nf; vi++) {
        uint32_t vol = be->f[vi].vol;
        dedupe_vec dv = {0};
        bool list_err = false;
        for (int i = 0; i < bn; i++) {
            chunk_store *st = atomic_load(&be->st[i]);
            if (!st)
                continue;
            dv.store = i;
            int rc = store_list(st, vol, false, true, dedupe_add, &dv);
            if (rc != 0 || dv.err != 0) {
                list_err = true;
                break;
            }
        }
        if (list_err || dv.n == 0) {
            free(dv.v);
            continue;
        }
        if (dv.n > 1)
            qsort(dv.v, dv.n, sizeof *dv.v, dedupe_cmp);

        for (size_t a = 0; a < dv.n;) {
            size_t b = a + 1;
            while (b < dv.n && dv.v[b].pos == dv.v[a].pos && memcmp(dv.v[b].oid, dv.v[a].oid, 16) == 0)
                b++;
            if (b - a == 1) {
                a = b;
                continue;
            }

            /* Duplicate chunk across >= 2 stores: verify every copy's header, footer, sums table, and all cells. */
            size_t m = b - a;
            int *cls = calloc(m, sizeof *cls);
            uint8_t (*vuuids)[16] = calloc(m, sizeof *vuuids);
            if (!cls || !vuuids) {
                free(cls);
                free(vuuids);
                a = b;
                continue;
            }
            int winner = -1;
            for (size_t k = 0; k < m; k++) {
                int sidx = dv.v[a + k].store;
                chunk_store *st = atomic_load(&be->st[sidx]);
                if (!st) {
                    cls[k] = DEDUPE_ABSENT;
                    continue;
                }
                chunk_rd *rd = NULL;
                int orc = chunk_rd_open_any(st, vol, dv.v[a].oid, dv.v[a].pos, &rd);
                if (orc == 0) {
                    uint64_t nbad = 0;
                    int src = chunk_rd_scrub(rd, &nbad);
                    memcpy(vuuids[k], chunk_rd_hdr(rd)->vuuid, 16);
                    chunk_rd_close(rd);
                    if (src == 0 && nbad == 0)
                        cls[k] = DEDUPE_WHOLE;
                    else if (src == 0)
                        cls[k] = DEDUPE_DAMAGED;
                    else
                        cls[k] = DEDUPE_IOERR;
                } else if (orc == TSDB_ERR_CORRUPT) {
                    cls[k] = DEDUPE_DAMAGED;
                } else if (orc == TSDB_ERR_NOTFOUND) {
                    cls[k] = DEDUPE_ABSENT;
                } else {
                    cls[k] = DEDUPE_IOERR;
                }
                if (cls[k] == DEDUPE_WHOLE) {
                    if (winner < 0 || dedupe_store_better(be, sidx, dv.v[a + (size_t)winner].store, dv.v[a].oid,
                                                          dv.v[a].pos))
                        winner = (int)k;
                }
            }

            char hex[33];
            chunk_oid_hex(dv.v[a].oid, hex);
            if (winner >= 0) {
                int wstore = dv.v[a + (size_t)winner].store;
                for (size_t k = 0; k < m; k++) {
                    if ((int)k == winner)
                        continue;
                    if (cls[k] != DEDUPE_DAMAGED &&
                        !(cls[k] == DEDUPE_WHOLE && memcmp(vuuids[k], vuuids[winner], 16) == 0))
                        continue; /* never drop an unreadable (IOERR) or foreign-vuuid copy */
                    int kstore = dv.v[a + k].store;
                    chunk_store *st = atomic_load(&be->st[kstore]);
                    if (!st)
                        continue;
                    if (store_unlink_dedupe(st, vol, dv.v[a].oid, dv.v[a].pos) == 0) {
                        fprintf(stderr, "tsdb: chunk dedupe: vol %" PRIu32 " oid %s pos %u: kept store %d, unlinked %s "
                                        "copy on store %d\n",
                                vol, hex, (unsigned)dv.v[a].pos, wstore,
                                cls[k] == DEDUPE_DAMAGED ? "damaged" : "duplicate", kstore);
                    }
                }
                if (wstore != chunk_be_place(be, dv.v[a].oid, dv.v[a].pos))
                    chunk_be_loc_set(be, vol, dv.v[a].oid, dv.v[a].pos, wstore);
            } else {
                fprintf(stderr, "tsdb: chunk dedupe: vol %" PRIu32 " oid %s pos %u: %zu copies found, none whole "
                                "(leaving intact)\n",
                        vol, hex, (unsigned)dv.v[a].pos, m);
            }
            free(cls);
            free(vuuids);
            a = b;
        }
        free(dv.v);
    }
}

/* The state a failed open puts a store in, or -1 when the open itself must fail. */
static int down_state(int rc)
{
    switch (rc) {
    case TSDB_ERR_NOTFOUND: return CHUNK_STORE_DOWN_MISSING;
    case CHUNK_ERR_STORE_ID: return CHUNK_STORE_DOWN_OTHER_ID;
    case TSDB_ERR_CORRUPT: return CHUNK_STORE_DOWN_DAMAGED;
    case TSDB_ERR_IO: return CHUNK_STORE_DOWN_IOERR;
    default: return -1; /* BUSY (another owner), no memory, an invalid argument */
    }
}

static const char *down_reason(int rc)
{
    switch (rc) {
    case TSDB_ERR_NOTFOUND: return "the directory or its META is not there (a lost disk)";
    case CHUNK_ERR_STORE_ID: return "its META names another store (a disk that came back as another one)";
    case TSDB_ERR_CORRUPT: return "its META or EPOCH is damaged";
    case TSDB_ERR_IO: return "an I/O error";
    default: return "no path (the caller knows it is down)";
    }
}

int chunk_be_open2(chunk_backend **out, const char *const *paths, int n, const uint8_t *ids, const chunk_be_opts *o)
{
    if (n < 1)
        return TSDB_ERR_INVALID;
    chunk_backend *be = calloc(1, sizeof *be);
    if (!be)
        return TSDB_ERR_NOMEM;
    int cap = n > 64 ? n : 64;
    be->cap = cap;
    be->st = calloc((size_t)cap, sizeof *be->st);
    be->state = calloc((size_t)cap, sizeof *be->state);
    be->st_failed = calloc((size_t)cap, sizeof *be->st_failed);
    be->st_lost = calloc((size_t)cap, sizeof *be->st_lost);
    be->st_excluded = calloc((size_t)cap, sizeof *be->st_excluded);
    be->st_draining = calloc((size_t)cap, sizeof *be->st_draining);
    be->store_pref = calloc((size_t)cap, sizeof *be->store_pref);
    be->pending_close = calloc((size_t)cap, sizeof *be->pending_close);
    be->path = calloc((size_t)cap, sizeof *be->path);
    for (size_t i = 0; i < CHUNK_OID_STRIPES; i++)
        pthread_mutex_init(&be->oid_mu[i], NULL);
    pthread_mutex_init(&be->mu, NULL);
    pthread_mutex_init(&be->st_mu, NULL);
    pthread_mutex_init(&be->loc_mu, NULL);
    atomic_init(&be->loc_cnt, 0);
    pthread_cond_init(&be->ioerr_cv, NULL);
    int rc = be->st && be->state && be->st_failed && be->st_lost && be->st_excluded && be->st_draining &&
                     be->store_pref && be->pending_close && be->path
                 ? 0
                 : TSDB_ERR_NOMEM;
    if (rc == 0 && o && o->store_pref) {
        be->has_store_pref = true;
        for (int i = 0; i < n; i++)
            be->store_pref[i] = o->store_pref[i];
    }
    bool ro = o && o->read_only;
    bool strict = !o || o->strict;
    be->read_only = ro;
    atomic_init(&be->n, rc == 0 ? n : 0); /* chunk_be_close closes what is open (NULL for the stores not opened) */
    int rcs[64]; /* why a store is down, for the message when too few are up */
    int up = 0;
    for (int i = 0; i < n && rc == 0; i++) {
        if (i < 64)
            rcs[i] = 0;
        if (paths[i])
            be->path[i] = strdup(paths[i]);
        if (!paths[i]) {
            atomic_store(&be->state[i], CHUNK_STORE_DOWN_MISSING);
            if (i < 64)
                rcs[i] = -1;
            fprintf(stderr, "tsdb: chunk backend: store %d has no path: it is DOWN (%s)\n", i, down_reason(-1));
            continue; /* down: the index stays */
        }
        chunk_store *st = NULL;
        int r = ro ? chunk_store_open_ro_quiet(paths[i], ids ? ids + 16 * i : NULL, &st)
                   : chunk_store_open_quiet(paths[i], ids ? ids + 16 * i : NULL, &st);
        if (r == CHUNK_ERR_STORE_ID && chunk_mutant == CHUNK_MUT_OPEN_ID_UP) /* planted: any disk will do */
            r = ro ? chunk_store_open_ro_quiet(paths[i], NULL, &st) : chunk_store_open_quiet(paths[i], NULL, &st);
        if (r == CHUNK_ERR_BUSY && chunk_mutant == CHUNK_MUT_OPEN_BUSY_DOWN) /* planted: another owner's store is down */
            r = TSDB_ERR_NOTFOUND;
        if (r == 0) {
            chunk_store_bind_be(st, be, i);
            atomic_store_explicit(&be->st_failed[i], false, memory_order_release);
            atomic_store_explicit(&be->st[i], st, memory_order_release);
            atomic_store(&be->state[i], CHUNK_STORE_UP);
            up++;
            for (int j = 0; j < i && rc == 0; j++) { /* two stores with one id are one disk reached twice (or a copy) */
                chunk_store *sj = atomic_load(&be->st[j]);
                if (sj && memcmp(chunk_store_id(sj), chunk_store_id(st), 16) == 0) {
                    fprintf(stderr, "tsdb: chunk backend: %s and %s are the same store (one id)\n", paths[j], paths[i]);
                    rc = TSDB_ERR_INVALID;
                }
            }
            continue;
        }
        int ds = down_state(r);
        if (ds < 0) {
            if (r == CHUNK_ERR_BUSY)
                fprintf(stderr, "tsdb: chunk backend: store %d (%s) is open elsewhere (two owners would let its EPOCH "
                                "go back): the open fails\n", i, paths[i]);
            rc = r;
            break;
        }
        atomic_store(&be->state[i], ds);
        if (i < 64)
            rcs[i] = r;
        fprintf(stderr, "tsdb: chunk backend: store %d (%s) is %s: %s\n", i, paths[i], chunk_store_state_name(ds),
                down_reason(r));
    }
    int need = o ? o->min_up : up; /* NULL or strict: every store that was named must open (what chunk_be_open means) */
    if (strict) {
        for (int i = 0; i < n && rc == 0; i++)
            if (paths[i] && atomic_load(&be->state[i]) != CHUNK_STORE_UP) {
                rc = i < 64 ? rcs[i] : TSDB_ERR_IO; /* the code the store's own open gave, as before */
                if (rc == 0)
                    rc = TSDB_ERR_IO;
            }
    } else if (rc == 0 && up < need) {
        char names[512] = "";
        size_t len = 0;
        for (int i = 0; i < n && len + 80 < sizeof names; i++)
            if (atomic_load(&be->state[i]) != CHUNK_STORE_UP) {
                int r = snprintf(names + len, sizeof names - len, "%s#%d %s (%s)", len ? ", " : "", i,
                                 be->path[i] ? be->path[i] : "-",
                                 chunk_store_state_name(atomic_load(&be->state[i])));
                if (r > 0)
                    len += (size_t)r < sizeof names - len ? (size_t)r : sizeof names - len - 1;
            }
        fprintf(stderr, "tsdb: chunk backend: %d of %d stores are up, %d are needed: down: %s\n", up, n, need, names);
        rc = TSDB_ERR_UNAVAILABLE;
    }
    int bn = atomic_load_explicit(&be->n, memory_order_relaxed);
    for (int i = 0; !be->read_only && i < bn && rc == 0; i++) { /* the fence: the maximum over the stores present */
        chunk_store *st = atomic_load(&be->st[i]);
        if (!st)
            continue;
        uint32_t *vols;
        uint64_t *eps;
        size_t nv;
        if ((rc = chunk_store_volumes(st, &vols, &eps, &nv)) != 0)
            break;
        for (size_t j = 0; j < nv; j++) {
            struct fence *f = fence_find(be, vols[j], true);
            if (!f)
                rc = TSDB_ERR_NOMEM;
            else if (eps[j] > f->epoch)
                f->epoch = eps[j];
        }
        free(vols);
        free(eps);
    }
    if (rc) {
        chunk_be_close(be);
        return rc;
    }
    be_open_dedupe(be);
    *out = be;
    return 0;
}

int chunk_be_open(chunk_backend **out, const char *const *paths, int n, const uint8_t *ids)
{
    return chunk_be_open2(out, paths, n, ids, NULL);
}

void chunk_be_close(chunk_backend *be)
{
    if (!be)
        return;
    chunk_rpool *rp = atomic_load_explicit(&be->rpool, memory_order_acquire);
    if (rp)
        chunk_rpool_destroy(rp);
    int bn = atomic_load_explicit(&be->n, memory_order_relaxed);
    for (int i = 0; i < bn; i++) {
        chunk_store_unref(atomic_load(&be->st[i])); /* NULL for a store opened or set as down */
        chunk_be_release_down(be, i);
        free(be->path[i]);
    }
    for (int i = 0; i < be->nretired; i++)
        chunk_store_unref(be->retired[i]);
    pthread_cond_destroy(&be->ioerr_cv);
    pthread_mutex_destroy(&be->loc_mu);
    pthread_mutex_destroy(&be->st_mu);
    pthread_mutex_destroy(&be->mu);
    for (size_t i = 0; i < CHUNK_OID_STRIPES; i++)
        pthread_mutex_destroy(&be->oid_mu[i]);
    free(be->st);
    free(be->state);
    free(be->st_failed);
    free(be->st_lost);
    free(be->st_excluded);
    free(be->st_draining);
    free(be->store_pref);
    free(be->pending_close);
    free(be->path);
    free(be->retired);
    free(be->loc_tab);
    free(be->f);
    free(be);
}

int chunk_be_nstores(const chunk_backend *be) { return atomic_load_explicit(&be->n, memory_order_acquire); }

int chunk_be_add_store(chunk_backend *be, const char *path, const uint8_t *expect_id, const uint8_t *expect_db,
                       bool replacing)
{
    if (!be || !path)
        return TSDB_ERR_INVALID;
    if (be->read_only)
        return TSDB_ERR_READONLY;
    int idx = atomic_load_explicit(&be->n, memory_order_acquire);
    if (idx >= be->cap)
        return TSDB_ERR_INVALID;
    chunk_store *ns = NULL;
    int rc = chunk_store_open_db_quiet(path, expect_id, expect_db, &ns);
    if (rc != 0)
        return rc;
    for (int j = 0; j < idx; j++) {
        chunk_store *sj = chunk_be_store_ref(be, j);
        bool dup = sj && memcmp(chunk_store_id(sj), chunk_store_id(ns), 16) == 0;
        chunk_store_unref(sj);
        if (dup) {
            chunk_store_close(ns);
            return TSDB_ERR_INVALID;
        }
    }
    /* Raise every known volume's fence in the newly opened store before publishing it. */
    uint32_t *vols = NULL;
    size_t nv = 0;
    rc = chunk_be_volumes_list(be, &vols, &nv);
    for (size_t j = 0; rc == 0 && j < nv; j++) {
        uint64_t ep = chunk_be_epoch(be, vols[j]);
        if (ep > 0)
            rc = chunk_store_raise(ns, vols[j], ep);
    }
    free(vols);
    if (rc != 0) {
        chunk_store_close(ns);
        return rc;
    }
    char *pcopy = strdup(path);
    if (!pcopy) {
        chunk_store_close(ns);
        return TSDB_ERR_NOMEM;
    }
    pthread_mutex_lock(&be->mu);
    int cur_n = atomic_load_explicit(&be->n, memory_order_relaxed);
    if (cur_n >= be->cap) {
        pthread_mutex_unlock(&be->mu);
        free(pcopy);
        chunk_store_close(ns);
        return TSDB_ERR_INVALID;
    }
    pthread_mutex_lock(&be->st_mu);
    be->path[cur_n] = pcopy;
    chunk_store_bind_be(ns, be, cur_n);
    atomic_store_explicit(&be->st_failed[cur_n], chunk_store_failed(ns), memory_order_release);
    atomic_store_explicit(&be->st_lost[cur_n], false, memory_order_release);
    atomic_store_explicit(&be->st_excluded[cur_n], false, memory_order_release);
    atomic_store_explicit(&be->state[cur_n], replacing ? CHUNK_STORE_REPLACING : CHUNK_STORE_UP, memory_order_release);
    atomic_store_explicit(&be->st[cur_n], ns, memory_order_release);
    atomic_store_explicit(&be->n, cur_n + 1, memory_order_release);
    pthread_mutex_unlock(&be->st_mu);
    pthread_mutex_unlock(&be->mu);
    return 0;
}

int chunk_be_add_lost(chunk_backend *be, const char *path)
{
    if (!be)
        return TSDB_ERR_INVALID;
    if (be->read_only)
        return TSDB_ERR_READONLY;
    char *pcopy = path ? strdup(path) : NULL;
    if (path && !pcopy)
        return TSDB_ERR_NOMEM;
    pthread_mutex_lock(&be->mu);
    int cur_n = atomic_load_explicit(&be->n, memory_order_relaxed);
    if (cur_n >= be->cap) {
        pthread_mutex_unlock(&be->mu);
        free(pcopy);
        return TSDB_ERR_INVALID;
    }
    pthread_mutex_lock(&be->st_mu);
    be->path[cur_n] = pcopy;
    atomic_store_explicit(&be->st_failed[cur_n], false, memory_order_release);
    atomic_store_explicit(&be->st_lost[cur_n], true, memory_order_release);
    atomic_store_explicit(&be->st_excluded[cur_n], false, memory_order_release);
    atomic_store_explicit(&be->state[cur_n], CHUNK_STORE_DOWN_MISSING, memory_order_release);
    atomic_store_explicit(&be->st[cur_n], NULL, memory_order_release);
    atomic_store_explicit(&be->n, cur_n + 1, memory_order_release);
    pthread_mutex_unlock(&be->st_mu);
    pthread_mutex_unlock(&be->mu);
    return 0;
}

uint64_t chunk_be_epoch(chunk_backend *be, uint32_t volume)
{
    pthread_mutex_lock(&be->mu);
    struct fence *f = fence_find(be, volume, false);
    uint64_t e = f ? f->epoch : 0;
    pthread_mutex_unlock(&be->mu);
    return e;
}

static int see_epoch_impl(chunk_backend *be, uint32_t volume, uint64_t e, bool allow_zero_open)
{
    if (be->read_only)
        return TSDB_ERR_READONLY;
    pthread_mutex_lock(&be->mu);
    struct fence *f = fence_find(be, volume, true);
    int rc = f ? 0 : TSDB_ERR_NOMEM;
    int bn = atomic_load_explicit(&be->n, memory_order_acquire);
    bool unsure = false; /* a store whose EPOCH lost a slot needs this call even at the epoch it already holds */
    for (int i = 0; f && i < bn; i++) {
        chunk_store *st = chunk_be_store_ref(be, i);
        unsure |= st && chunk_store_epoch_unsure(st, volume);
        chunk_store_unref(st);
    }
    if (f && (e > f->epoch || unsure)) {
        if (e > f->epoch)
            f->epoch = e; /* in memory first: a store that fails to sync it still does not act below it */
        int ok = 0;
        for (int i = 0; i < bn; i++) {
            chunk_store *st = chunk_be_store_ref(be, i);
            if (!st || chunk_store_failed(st)) {
                chunk_store_unref(st);
                continue; /* a store that acknowledges nothing needs no fence: it is a device DOWN (E27) */
            }
            int r = chunk_store_raise(st, volume, e);
            chunk_store_unref(st);
            if (r == 0)
                ok++;
            else if (rc == 0)
                rc = r;
        }
        if (ok == 0 && rc == 0 && !allow_zero_open)
            rc = TSDB_ERR_IO; /* every store has failed: nothing holds the epoch durably */
    }
    pthread_mutex_unlock(&be->mu);
    return rc;
}

int chunk_be_see_epoch(chunk_backend *be, uint32_t volume, uint64_t e)
{
    return see_epoch_impl(be, volume, e, false);
}

int chunk_be_see_epoch_for_attach(chunk_backend *be, uint32_t volume, uint64_t e)
{
    return see_epoch_impl(be, volume, e, true);
}

/* The fence before a put or delete at epoch e: refused below it, raised (in every store) above it. A store whose EPOCH
 * lost a slot refuses until chunk_be_see_epoch (the authoritative call) has been made: a writer's own epoch must not
 * stand in for the fence that was lost. */
int chunk_be_prepare(chunk_backend *be, uint32_t volume, uint64_t e)
{
    if (be->read_only)
        return TSDB_ERR_READONLY;
    uint64_t cur = chunk_be_epoch(be, volume);
    if (e < cur && chunk_mutant != CHUNK_MUT_NOFENCE)
        return CHUNK_ERR_FENCED;
    int bn = chunk_be_nstores(be);
    for (int i = 0; i < bn; i++) {
        chunk_store *st = chunk_be_store_ref(be, i);
        bool u = st && chunk_store_epoch_unsure(st, volume);
        chunk_store_unref(st);
        if (u)
            return CHUNK_ERR_EPOCH_UNSURE;
    }
    return e > cur ? chunk_be_see_epoch(be, volume, e) : 0;
}

static chunk_store *be_target_ref(chunk_backend *be, int store, const chunk_ident *id, int *err)
{
    if (store < 0)
        store = chunk_be_place(be, id->oid, id->pos);
    *err = 0;
    if (store >= chunk_be_nstores(be)) {
        *err = TSDB_ERR_INVALID;
        return NULL;
    }
    chunk_store *st = chunk_be_store_ref(be, store);
    if (!st)
        *err = TSDB_ERR_UNAVAILABLE; /* a store that is DOWN */
    return st;
}

int chunk_be_place_n(const uint8_t oid[16], unsigned pos, int n)
{
    if (n <= 0)
        return 0;
    return (int)((xxh64(oid, 16, 0) + pos) % (uint64_t)n);
}

int chunk_be_infer_place_n(const chunk_backend *be, const uint8_t oid[16], unsigned np, const int *obs_store,
                           const unsigned *obs_pos, size_t nobs, int target_store)
{
    int bn = chunk_be_nstores(be);
    if (bn <= 0 || nobs == 0 || !obs_store || !obs_pos)
        return bn;
    /* R15 r15-shrink: if any slot is excluded (SHRINKING/REMOVED live) and every observed chunk already matches
     * active-store placement, return -1 so chunk_be_place_for_obj maps missing positions through chunk_be_place(). */
    bool any_excl = false;
    for (int i = 0; i < bn; i++)
        if (chunk_be_store_excluded(be, i)) {
            any_excl = true;
            break;
        }
    if (any_excl) {
        bool match_active = true;
        for (size_t q = 0; q < nobs; q++) {
            if (chunk_be_place(be, oid, obs_pos[q]) != obs_store[q]) {
                match_active = false;
                break;
            }
        }
        if (match_active)
            return -1;
    }
    int min_n = np >= 2 ? (int)np : 1;
    if (min_n > bn)
        min_n = bn;
    uint64_t h = xxh64(oid, 16, 0);
    bool seen_pos[CHUNK_POS_MAX] = {false};
    for (size_t q = 0; q < nobs; q++)
        if (obs_pos[q] < CHUNK_POS_MAX)
            seen_pos[obs_pos[q]] = true;

    int best_n = -1;
    unsigned best_up_miss = UINT_MAX;
    for (int cand = bn; cand >= min_n; cand--) {
        bool ok = true;
        for (size_t q = 0; q < nobs; q++) {
            if ((int)((h + obs_pos[q]) % (uint64_t)cand) != obs_store[q]) {
                ok = false;
                break;
            }
        }
        if (!ok)
            continue;
        unsigned up_miss = 0;
        for (unsigned p = 0; p < np && p < CHUNK_POS_MAX; p++) {
            if (seen_pos[p])
                continue;
            int s = (int)((h + p) % (uint64_t)cand);
            if (s != target_store && !chunk_be_store_is_lost(be, s) &&
                chunk_be_store_state(be, s) == CHUNK_STORE_UP)
                up_miss++;
        }
        if (best_n < 0 || up_miss < best_up_miss) {
            best_n = cand;
            best_up_miss = up_miss;
        }
    }
    return best_n > 0 ? best_n : bn;
}

int chunk_be_place(const chunk_backend *be, const uint8_t oid[16], unsigned pos)
{
    int bn = chunk_be_nstores(be);
    if (bn <= 0)
        return 0;
    int elig_s[64];
    int *elig = bn <= 64 ? elig_s : malloc((size_t)bn * sizeof *elig);
    if (!elig)
        return chunk_be_place_n(oid, pos, bn);
    int ne = 0;
    bool any_excl = false;
    for (int i = 0; i < bn; i++) {
        if (chunk_be_store_excluded(be, i)) {
            any_excl = true;
            continue;
        }
        elig[ne++] = i;
    }
    int res;
    if (!any_excl)
        res = chunk_be_place_n(oid, pos, bn);
    else if (ne == 0)
        res = 0;
    else
        res = elig[(xxh64(oid, 16, 0) + pos) % (uint64_t)ne];
    if (elig != elig_s)
        free(elig);
    return res;
}

int chunk_be_place_for_obj(const chunk_backend *be, const uint8_t oid[16], unsigned pos, int place_n)
{
    return place_n < 0 ? chunk_be_place(be, oid, pos) : chunk_be_place_n(oid, pos, place_n);
}

int chunk_be_pos_store(const chunk_backend *be, const uint8_t oid[16], unsigned pos) { return chunk_be_place(be, oid, pos); }

int chunk_be_wr_open(chunk_backend *be, int store, const chunk_ident *id, const chunk_geom *g, uint64_t epoch,
                     chunk_wr **out)
{
    int rc = chunk_be_prepare(be, id->volume, epoch);
    if (rc)
        return rc;
    chunk_store *st = be_target_ref(be, store, id, &rc);
    if (!st)
        return rc;
    rc = chunk_wr_open(st, id, g, epoch, out);
    chunk_store_unref(st);
    return rc;
}

/* Returns 1 if the chunk file exists on `st`, 0 if absent (ENOENT / no volume dir), or TSDB_ERR_IO on I/O error. */
static int store_has_chunk_file(chunk_store *st, const chunk_ident *id)
{
    pthread_mutex_lock(&st->mu);
    vol_state *v;
    int rc = atomic_load(&st->failed) ? TSDB_ERR_IO : vol_get(st, id->volume, false, &v);
    int vfd = rc == 0 ? v->dirfd : -1;
    pthread_mutex_unlock(&st->mu);
    if (rc == TSDB_ERR_NOTFOUND)
        return 0;
    if (rc != 0)
        return rc;
    char name[80];
    chunk_name(name, sizeof name, id->oid, id->pos);
    if (faccessat(vfd, name, F_OK, 0) == 0)
        return 1;
    return errno == ENOENT ? 0 : TSDB_ERR_IO;
}

int chunk_be_store_has_chunk(chunk_backend *be, int store, uint32_t volume, const uint8_t oid[16], unsigned pos)
{
    if (!be || !oid || pos >= CHUNK_POS_MAX)
        return TSDB_ERR_INVALID;
    chunk_store *st = chunk_be_store_ref(be, store);
    if (!st)
        return TSDB_ERR_UNAVAILABLE;
    chunk_ident id = {.volume = volume, .pos = (uint16_t)pos};
    memcpy(id.oid, oid, 16);
    int rc = store_has_chunk_file(st, &id);
    chunk_store_unref(st);
    return rc;
}

int chunk_be_wr_open_replace(chunk_backend *be, const chunk_ident *id, const chunk_geom *g, uint64_t hdr_epoch,
                             uint64_t fence_epoch, chunk_wr **out, chunk_store **st_out)
{
    if (st_out)
        *st_out = NULL;
    int rc = chunk_be_prepare(be, id->volume, fence_epoch);
    if (rc)
        return rc;
    int bn = chunk_be_nstores(be);
    int hint = chunk_be_loc_get(be, id->volume, id->oid, id->pos);
    int primary = bn > 0 ? chunk_be_place(be, id->oid, id->pos) : -1;
    chunk_store *st = NULL;
    bool saw_down = false, saw_ioerr = false;
    if (hint >= 0 && hint < bn) {
        chunk_store *cand = chunk_be_store_ref(be, hint);
        if (!cand) {
            saw_down = true;
        } else {
            int h = store_has_chunk_file(cand, id);
            if (h == 1)
                st = cand;
            else {
                if (h < 0)
                    saw_ioerr = true;
                chunk_store_unref(cand);
            }
        }
    }
    if (!st && primary >= 0 && primary < bn && primary != hint) {
        chunk_store *cand = chunk_be_store_ref(be, primary);
        if (!cand) {
            saw_down = true;
        } else {
            int h = store_has_chunk_file(cand, id);
            if (h == 1)
                st = cand;
            else {
                if (h < 0)
                    saw_ioerr = true;
                chunk_store_unref(cand);
            }
        }
    }
    for (int i = 0; !st && i < bn; i++) {
        if (i == primary || i == hint)
            continue;
        chunk_store *cand = chunk_be_store_ref(be, i);
        if (!cand) {
            saw_down = true;
        } else {
            int h = store_has_chunk_file(cand, id);
            if (h == 1)
                st = cand;
            else {
                if (h < 0)
                    saw_ioerr = true;
                chunk_store_unref(cand);
            }
        }
    }
    if (!st) {
        /* D9: never create a chunk on replace-put; preserve I/O error (retryable) over DOWN (UNAVAILABLE) over
         * genuinely absent on every open store (already deleted -> NOTFOUND). */
        if (saw_ioerr)
            return TSDB_ERR_IO;
        return saw_down ? TSDB_ERR_UNAVAILABLE : TSDB_ERR_NOTFOUND;
    }
    rc = wr_open(st, id, g, hdr_epoch, fence_epoch, true, true, out);
    if (rc == 0 && st_out)
        *st_out = st;
    else
        chunk_store_unref(st);
    return rc;
}

int chunk_be_put(chunk_backend *be, int store, const chunk_ident *id, const chunk_geom *g, uint64_t epoch,
                 const void *cells, const uint64_t *sums)
{
    int rc = chunk_be_prepare(be, id->volume, epoch);
    if (rc)
        return rc;
    chunk_store *st = be_target_ref(be, store, id, &rc);
    if (!st)
        return rc;
    rc = chunk_store_put(st, id, g, epoch, cells, sums);
    chunk_store_unref(st);
    return rc;
}

int chunk_be_rd_open_at(chunk_backend *be, const chunk_ident *id, chunk_rd **out, int *store_out)
{
    if (store_out)
        *store_out = -1;
    int worst = TSDB_ERR_NOTFOUND;
    bool saw_down = false;
    bool saw_nomem = false;
    int bn = chunk_be_nstores(be);
    int hint = chunk_be_loc_get(be, id->volume, id->oid, id->pos);
    int primary = bn > 0 ? chunk_be_place(be, id->oid, id->pos) : -1;
    if (hint >= 0 && hint < bn) {
        chunk_store *st = chunk_be_store_ref(be, hint);
        if (!st) {
            saw_down = true;
        } else {
            bool real_io = false;
            int rc = chunk_rd_open_ex(st, id, out, &real_io);
            chunk_store_unref(st);
            if (rc == 0) {
                if (store_out)
                    *store_out = hint;
                return 0;
            }
            if (rc == TSDB_ERR_IO && real_io)
                chunk_be_note_ioerr(be, hint);
            if (rc == TSDB_ERR_NOMEM)
                saw_nomem = true;
            else if (rc == TSDB_ERR_IO || (rc == TSDB_ERR_CORRUPT && worst != TSDB_ERR_IO))
                worst = rc;
        }
    }
    if (primary >= 0 && primary < bn && primary != hint) {
        chunk_store *st = chunk_be_store_ref(be, primary);
        if (!st) {
            saw_down = true;
        } else {
            bool real_io = false;
            int rc = chunk_rd_open_ex(st, id, out, &real_io);
            chunk_store_unref(st);
            if (rc == 0) {
                if (hint >= 0)
                    chunk_be_loc_del(be, id->volume, id->oid, id->pos);
                if (store_out)
                    *store_out = primary;
                return 0;
            }
            if (rc == TSDB_ERR_IO && real_io)
                chunk_be_note_ioerr(be, primary);
            if (rc == TSDB_ERR_NOMEM)
                saw_nomem = true;
            else if (rc == TSDB_ERR_IO || (rc == TSDB_ERR_CORRUPT && worst != TSDB_ERR_IO))
                worst = rc;
        }
    }
    for (int i = 0; i < bn; i++) {
        if (i == primary || i == hint)
            continue;
        chunk_store *st = chunk_be_store_ref(be, i);
        if (!st) {
            saw_down = true;
            continue;
        }
        bool real_io = false;
        int rc = chunk_rd_open_ex(st, id, out, &real_io);
        chunk_store_unref(st);
        if (rc == 0) {
            chunk_be_loc_set(be, id->volume, id->oid, id->pos, i);
            if (store_out)
                *store_out = i;
            return 0;
        }
        if (rc == TSDB_ERR_IO && real_io)
            chunk_be_note_ioerr(be, i);
        if (rc == TSDB_ERR_NOMEM)
            saw_nomem = true;
        else if (rc == TSDB_ERR_IO || (rc == TSDB_ERR_CORRUPT && worst != TSDB_ERR_IO))
            worst = rc;
    }
    if (worst == TSDB_ERR_NOTFOUND) {
        if (saw_nomem)
            return TSDB_ERR_NOMEM;
        if (saw_down)
            return TSDB_ERR_UNAVAILABLE;
    }
    return worst;
}

int chunk_be_rd_open(chunk_backend *be, const chunk_ident *id, chunk_rd **out)
{
    return chunk_be_rd_open_at(be, id, out, NULL);
}

int chunk_be_del(chunk_backend *be, const chunk_ident *id, uint64_t epoch)
{
    if (be->read_only)
        return TSDB_ERR_READONLY;
    /* R13 r13-lmove: serialize against concurrent chunk_move and r12-repair replace-puts on the same (volume, oid)
     * so a delete racing a move never leaves a resurrected copy on the target store. */
    chunk_be_oid_lock(be, id->volume, id->oid);
    int rc = chunk_be_prepare(be, id->volume, epoch);
    if (rc) {
        chunk_be_oid_unlock(be, id->volume, id->oid);
        return rc;
    }
    bool found = false;
    int bn = chunk_be_nstores(be);
    for (int i = 0; i < bn; i++) {
        chunk_store *st = chunk_be_store_ref(be, i);
        if (!st)
            continue;
        rc = chunk_store_del(st, id, epoch);
        chunk_store_unref(st);
        if (rc == 0)
            found = true;
        else if (rc != TSDB_ERR_NOTFOUND) {
            chunk_be_oid_unlock(be, id->volume, id->oid);
            return rc;
        }
    }
    chunk_be_loc_del(be, id->volume, id->oid, id->pos);
    chunk_be_oid_unlock(be, id->volume, id->oid);
    return found ? 0 : TSDB_ERR_NOTFOUND;
}

typedef struct list_ctx {
    chunk_list_fn fn;
    void *ctx;
    int store;
} list_ctx;

static int list_one(void *ctx, const chunk_listed *c)
{
    list_ctx *l = ctx;
    chunk_listed t = *c;
    t.store = l->store;
    return l->fn(l->ctx, &t);
}

static int be_list(chunk_backend *be, uint32_t volume, bool verify, bool names_only, chunk_list_fn fn, void *ctx)
{
    int bn = chunk_be_nstores(be);
    for (int i = 0; i < bn; i++) {
        chunk_store *st = chunk_be_store_ref(be, i);
        if (!st)
            continue;
        list_ctx l = {fn, ctx, i};
        int rc = store_list(st, volume, verify, names_only, list_one, &l);
        chunk_store_unref(st);
        if (rc)
            return rc;
    }
    return 0;
}

int chunk_be_list(chunk_backend *be, uint32_t volume, bool verify, chunk_list_fn fn, void *ctx)
{
    return be_list(be, volume, verify, false, fn, ctx);
}

int chunk_be_list_names(chunk_backend *be, uint32_t volume, chunk_list_fn fn, void *ctx)
{
    return be_list(be, volume, false, true, fn, ctx);
}

/* ---------------------------------------------------------------- put an object */
#ifdef TSDB_EC
typedef struct put_ctx {
    chunk_wr *w[CHUNK_POS_MAX];
} put_ctx;

static int put_cell(void *ctx, unsigned pos, uint64_t idx, const uint8_t *data, size_t len, uint64_t sum)
{
    (void)idx;
    chunk_wr *w = ((put_ctx *)ctx)->w[pos];
    return w ? chunk_wr_cell_trusted(w, data, len, sum) : 0;
}
#endif

int chunk_be_put_stream(chunk_backend *be, uint32_t volume, const uint8_t vuuid[16], const uint8_t oid[16],
                        const chunk_geom *g, uint64_t epoch, const chunk_put_src *src, unsigned posmask,
                        uint64_t *sums_out)
{
#ifndef TSDB_EC
    (void)be, (void)volume, (void)vuuid, (void)oid, (void)g, (void)epoch, (void)src, (void)posmask, (void)sums_out;
    return TSDB_ERR_INVALID; /* no erasure codec in this build: nothing is opened */
#else
    if (be->read_only)
        return TSDB_ERR_READONLY;
    if (!chunk_geom_valid(g) || !src || !src->fill)
        return TSDB_ERR_INVALID;
    put_ctx pc;
    memset(&pc, 0, sizeof pc);
    unsigned np = chunk_npos(g);
    int bn = chunk_be_nstores(be);
    if (bn <= 0)
        return TSDB_ERR_INVALID;
    int elig_s[64];
    int *elig = bn <= 64 ? elig_s : malloc((size_t)bn * sizeof *elig);
    if (!elig)
        return TSDB_ERR_NOMEM;
    int ne = 0;
    bool any_excl = false;
    for (int i = 0; i < bn; i++) {
        if (chunk_be_store_excluded(be, i)) {
            any_excl = true;
            continue;
        }
        elig[ne++] = i;
    }
    int nplace = any_excl ? ne : bn;
    if (nplace <= 0) {
        if (elig != elig_s)
            free(elig);
        return TSDB_ERR_UNAVAILABLE;
    }
    int base = (int)(xxh64(oid, 16, 0) % (uint64_t)nplace), rc = 0;
    for (unsigned p = 0; p < np && rc == 0; p++) {
        if (!(posmask >> p & 1))
            continue;
        chunk_ident id = {.volume = volume, .pos = (uint16_t)p};
        memcpy(id.vuuid, vuuid, 16);
        memcpy(id.oid, oid, 16);
        int sidx = any_excl ? elig[(base + (int)p) % ne] : (base + (int)p) % bn;
        rc = chunk_be_wr_open(be, sidx, &id, g, epoch, &pc.w[p]);
    }
    if (elig != elig_s)
        free(elig);
    if (rc == 0)
        rc = chunk_encode_stream(g, src, put_cell, &pc, sums_out);
    for (unsigned p = 0; p < np; p++) {
        if (!pc.w[p])
            continue;
        if (rc == 0)
            rc = chunk_wr_commit(pc.w[p]);
        else
            chunk_wr_abort(pc.w[p]);
        pc.w[p] = NULL; /* commit frees its writer on every path */
        if (rc)
            for (unsigned q = p + 1; q < np; q++) {
                chunk_wr_abort(pc.w[q]);
                pc.w[q] = NULL;
            }
    }
    return rc;
#endif
}

static int mem_fill(void *ctx, uint64_t s, uint64_t off, uint8_t *buf, size_t len)
{
    (void)s;
    memcpy(buf, (const uint8_t *)ctx + off, len);
    return 0;
}

int chunk_be_put_object(chunk_backend *be, uint32_t volume, const uint8_t vuuid[16], const uint8_t oid[16],
                        const chunk_geom *g, uint64_t epoch, const uint8_t *obj, unsigned posmask, uint64_t *sums_out)
{
    chunk_put_src src = {(void *)(uintptr_t)obj, mem_fill};
    return chunk_be_put_stream(be, volume, vuuid, oid, g, epoch, &src, posmask, sums_out);
}
