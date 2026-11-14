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
    char *path;
    int dirfd;
    uint8_t id[16];
    pthread_mutex_t mu; /* the epochs, and the instant of a rename or an unlink */
    vol_state *v;
    size_t nv, capv;
    atomic_int failed;
    atomic_uint seq;
};

/* ---------------------------------------------------------------- small helpers */
static int rd_full(int fd, void *buf, size_t n, uint64_t off)
{
    uint8_t *p = buf;
    size_t got = 0;
    if (chunk_fault_read > 0 && --chunk_fault_read == 0) /* test hook */
        return TSDB_ERR_IO;
    while (got < n) {
        ssize_t r = pread(fd, p + got, n - got, (off_t)(off + got));
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0) {
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
static void meta_text(char *out, size_t cap, const uint8_t id[16])
{
    char hex[33], body[128];
    chunk_oid_hex(id, hex);
    int n = snprintf(body, sizeof body, "tsdb chunk store %d\nid=%s\n", CHUNK_STORE_VERSION, hex);
    snprintf(out, cap, "%ssum=%016" PRIx64 "\n", body, xxh64(body, (size_t)n, 0));
}

static int meta_read(int dirfd, uint8_t id[16])
{
    int fd = openat(dirfd, "META", O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    char buf[256];
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
    /* strict: the first line, "id=" and 32 hex digits; the text is re-made from that id and compared whole, so the
     * sum line, the newlines and the length are all checked at once */
    static const char first[] = "tsdb chunk store 1\nid=";
    uint8_t t[16];
    char want[256];
    if (got < sizeof first - 1 + 32 || memcmp(buf, first, sizeof first - 1) != 0 ||
        !hex_parse(buf + sizeof first - 1, 32, t))
        return TSDB_ERR_CORRUPT;
    meta_text(want, sizeof want, t);
    if (strlen(want) != got || memcmp(want, buf, got) != 0)
        return TSDB_ERR_CORRUPT;
    memcpy(id, t, 16);
    return 0;
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
    if (!path || !*path)
        return TSDB_ERR_INVALID;
    bool made = mkdir(path, 0755) == 0;
    if (!made && errno != EEXIST)
        return TSDB_ERR_IO;
    int dirfd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dirfd < 0)
        return TSDB_ERR_IO;
    int rc = 0;
    if (faccessat(dirfd, "META", F_OK, AT_SYMLINK_NOFOLLOW) == 0) {
        close(dirfd);
        return TSDB_ERR_INVALID; /* already a store: its id is not replaced */
    }
    uint8_t sid[16];
    if (id)
        memcpy(sid, id, 16);
    else
        chunk_random(sid, 16);
    char text[256];
    meta_text(text, sizeof text, sid);
    /* a temporary file is always a new inode: one a failed write or sync left behind may have had its pages dropped
     * (the kernel reports that once), so it is never reused */
    unlinkat(dirfd, "META.tmp", 0);
    int fd = openat(dirfd, "META.tmp", O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
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
        rc = sync_dir_path_parent(path);
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

static int store_open(const char *path, const uint8_t *expect_id, bool quiet, chunk_store **out)
{
    int dirfd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dirfd < 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    if (flock(dirfd, LOCK_EX | LOCK_NB) != 0) { /* held by the open file description: a second open here conflicts too */
        int busy = errno == EWOULDBLOCK;
        if (busy && !quiet)
            fprintf(stderr, "tsdb: chunk store %s is open elsewhere (two owners would let its EPOCH go back)\n", path);
        close(dirfd);
        return busy ? CHUNK_ERR_BUSY : TSDB_ERR_IO;
    }
    uint8_t id[16];
    int rc = meta_read(dirfd, id);
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
    if (rc) {
        close(dirfd);
        return rc;
    }
    chunk_store *st = calloc(1, sizeof *st);
    if (!st) {
        close(dirfd);
        return TSDB_ERR_NOMEM;
    }
    st->path = strdup(path);
    st->dirfd = dirfd;
    memcpy(st->id, id, 16);
    pthread_mutex_init(&st->mu, NULL);
    pthread_mutex_lock(&st->mu);
    rc = st->path ? scan_volumes(st) : TSDB_ERR_NOMEM;
    pthread_mutex_unlock(&st->mu);
    if (rc) {
        chunk_store_close(st);
        return rc;
    }
    *out = st;
    return 0;
}

int chunk_store_open(const char *path, const uint8_t *expect_id, chunk_store **out)
{
    return store_open(path, expect_id, false, out);
}

int chunk_store_open_quiet(const char *path, const uint8_t *expect_id, chunk_store **out)
{
    return store_open(path, expect_id, true, out);
}

void chunk_store_close(chunk_store *st)
{
    if (!st)
        return;
    for (size_t i = 0; i < st->nv; i++) {
        close(st->v[i].dirfd);
        close(st->v[i].efd);
    }
    free(st->v);
    if (st->dirfd >= 0)
        close(st->dirfd);
    pthread_mutex_destroy(&st->mu);
    free(st->path);
    free(st);
}

const uint8_t *chunk_store_id(const chunk_store *st) { return st->id; }
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
    int err; /* a failed cell poisons the writer: commit refuses */
};

static int wr_open(chunk_store *st, const chunk_ident *id, const chunk_geom *g, uint64_t epoch, uint64_t fence,
                   bool heal, chunk_wr **out)
{
    if (!chunk_geom_valid(g) || id->pos >= chunk_npos(g))
        return TSDB_ERR_INVALID;
    pthread_mutex_lock(&st->mu);
    vol_state *v;
    int rc = atomic_load(&st->failed) ? TSDB_ERR_IO : vol_get(st, id->volume, true, &v);
    if (rc == 0 && fence < v->epoch && chunk_mutant != CHUNK_MUT_NOFENCE)
        rc = CHUNK_ERR_FENCED;
    if (rc == 0 && v->unsure)
        rc = CHUNK_ERR_EPOCH_UNSURE; /* a slot of EPOCH was damaged: no epoch is trusted until the owner raises it */
    if (rc == 0 && fence > v->epoch && !(heal && chunk_mutant == CHUNK_MUT_REPLACE_LATE)) /* planted: raised later */
        rc = epoch_store(st, v, fence); /* before it acts on a higher one (E14) */
    int vdirfd = rc == 0 ? v->dirfd : -1;
    pthread_mutex_unlock(&st->mu);
    if (rc)
        return rc;
    chunk_wr *w = calloc(1, sizeof *w);
    if (!w)
        return TSDB_ERR_NOMEM;
    w->st = st;
    w->id = *id;
    w->g = *g;
    w->epoch = epoch;
    w->fence = fence;
    w->vdirfd = vdirfd;
    w->fd = -1;
    w->ncells = chunk_pos_cells(g, id->pos);
    w->sums = malloc((w->ncells ? w->ncells : 1) * sizeof *w->sums);
    if (!w->sums) {
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
        free(w->sums);
        free(w);
        return errno == ENOSPC ? CHUNK_ERR_FULL : TSDB_ERR_IO;
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
    return wr_open(st, id, g, epoch, epoch, false, out);
}

int chunk_wr_open_heal(chunk_store *st, const chunk_ident *id, const chunk_geom *g, uint64_t hdr_epoch,
                       uint64_t fence_epoch, chunk_wr **out)
{
    return wr_open(st, id, g, hdr_epoch, fence_epoch, true, out);
}

static int wr_cell(chunk_wr *w, const void *data, size_t len, uint64_t sum)
{
    if (w->err)
        return w->err;
    if (w->next >= w->ncells || len != chunk_pos_cell_len(&w->g, w->id.pos, w->next))
        return w->err = TSDB_ERR_INVALID;
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
    free(w->sums);
    free(w);
}

int chunk_wr_commit(chunk_wr *w)
{
    chunk_store *st = w->st;
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
    if (rc) {
        chunk_wr_abort(w);
        return rc;
    }
    pthread_mutex_lock(&st->mu);
    vol_state *v;
    if (atomic_load(&st->failed))
        rc = TSDB_ERR_IO;
    else if ((rc = vol_get(st, w->id.volume, true, &v)) == 0 && w->fence < v->epoch &&
             chunk_mutant != CHUNK_MUT_NOFENCE)
        rc = CHUNK_ERR_FENCED; /* the epoch moved on while the chunk was being written */
    if (rc == 0 && renameat(w->vdirfd, w->tmp, w->vdirfd, w->name) != 0)
        rc = TSDB_ERR_IO;
    pthread_mutex_unlock(&st->mu);
    if (rc) {
        chunk_wr_abort(w);
        return rc;
    }
    close(w->fd);
    w->fd = -1; /* named: the abort path must not unlink it */
    if (tsdb_fsync(w->vdirfd) != 0) {
        store_fail(st, "directory fsync after a chunk was named", errno);
        rc = TSDB_ERR_IO;
    }
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
static int rd_load(int fd, const uint8_t oid[16], unsigned pos, const uint8_t *want_vuuid, bool full, chunk_rd **out,
                   chunk_hdr *hdr_out)
{
    struct stat sb;
    if (fstat(fd, &sb) != 0)
        return TSDB_ERR_IO;
    uint64_t size = (uint64_t)sb.st_size;
    if (size < CHUNK_HDR_BYTES + CHUNK_FTR_BYTES)
        return TSDB_ERR_CORRUPT;
    uint8_t hb[CHUNK_HDR_BYTES];
    int rc = rd_full(fd, hb, sizeof hb, 0);
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
    rc = rd_full(fd, tail, tl, CHUNK_HDR_BYTES + h.chunk_len);
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

int chunk_rd_open(chunk_store *st, const chunk_ident *id, chunk_rd **out)
{
    pthread_mutex_lock(&st->mu);
    vol_state *v;
    int rc = vol_get(st, id->volume, false, &v);
    int vfd = rc == 0 ? v->dirfd : -1;
    pthread_mutex_unlock(&st->mu);
    if (rc)
        return rc;
    char name[80];
    chunk_name(name, sizeof name, id->oid, id->pos);
    int fd = openat(vfd, name, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    rc = rd_load(fd, id->oid, id->pos, id->vuuid, true, out, NULL);
    if (rc)
        close(fd);
    return rc;
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

int chunk_rd_read(chunk_rd *r, uint64_t off, void *buf, size_t len)
{
    if (off > r->h.chunk_len || len > r->h.chunk_len - off)
        return TSDB_ERR_INVALID;
    return rd_full(r->fd, buf, len, CHUNK_HDR_BYTES + off);
}

int chunk_rd_scrub(chunk_rd *r, uint64_t *bad)
{
    uint64_t nbad = 0, off = 0;
    size_t cap = r->h.g.cell < r->h.chunk_len ? r->h.g.cell : (size_t)r->h.chunk_len;
    uint8_t *buf = malloc(cap ? cap : 1);
    if (!buf)
        return TSDB_ERR_NOMEM;
    int rc = 0;
    for (uint64_t i = 0; i < r->ncells && rc == 0; i++) {
        size_t cl = (size_t)chunk_pos_cell_len(&r->h.g, r->h.pos, i);
        rc = chunk_rd_read(r, off, buf, cl);
        if (rc == 0 && xxh64(buf, cl, 0) != r->sums[i])
            nbad++;
        off += cl;
    }
    free(buf);
    *bad = nbad;
    return rc;
}

int chunk_store_list(chunk_store *st, uint32_t volume, bool verify, chunk_list_fn fn, void *ctx)
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
        int fd = openat(vfd, n, O_RDONLY | O_CLOEXEC);
        if (fd < 0) {
            c.status = errno == ENOENT ? CHUNK_ST_ABSENT : CHUNK_ST_IOERR; /* removed under the scan, or unreadable */
            if (c.status == CHUNK_ST_ABSENT)
                continue;
        } else {
            struct stat sb;
            chunk_hdr h;
            chunk_rd *r = NULL;
            c.size = fstat(fd, &sb) == 0 ? (uint64_t)sb.st_size : 0;
            int lr = rd_load(fd, c.oid, c.pos, NULL, verify, &r, &h);
            c.status = lr == 0 ? CHUNK_ST_OK : lr == TSDB_ERR_IO ? CHUNK_ST_IOERR : CHUNK_ST_DAMAGED;
            if (lr == 0)
                c.epoch = h.epoch;
            if (r)
                chunk_rd_close(r);
            else
                close(fd);
        }
        rc = fn(ctx, &c);
    }
    closedir(d);
    return rc == 0 && derr ? TSDB_ERR_IO : rc; /* a failing readdir is not the end of the directory */
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
    if (fd < 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
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

chunk_store *chunk_be_open_store(const chunk_backend *be, int i)
{
    return i >= 0 && i < be->n ? atomic_load_explicit(&be->st[i], memory_order_acquire) : NULL;
}

chunk_store *chunk_be_store(chunk_backend *be, int i) { return chunk_be_open_store(be, i); }

int chunk_be_store_state(const chunk_backend *be, int i)
{
    if (i < 0 || i >= be->n)
        return -1;
    int s = atomic_load(&be->state[i]);
    chunk_store *st = chunk_be_open_store(be, i);
    if (st && chunk_store_failed(st)) /* a store that failed in service acknowledges nothing: it is a device DOWN (E27) */
        return CHUNK_STORE_DOWN_IOERR;
    return s;
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
    be->st = calloc((size_t)n, sizeof *be->st);
    be->state = calloc((size_t)n, sizeof *be->state);
    be->path = calloc((size_t)n, sizeof *be->path);
    pthread_mutex_init(&be->mu, NULL);
    int rc = be->st && be->state && be->path ? 0 : TSDB_ERR_NOMEM;
    be->n = rc == 0 ? n : 0; /* chunk_be_close closes what is open (NULL for the stores not opened) */
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
        int r = chunk_store_open_quiet(paths[i], ids ? ids + 16 * i : NULL, &st);
        if (r == CHUNK_ERR_STORE_ID && chunk_mutant == CHUNK_MUT_OPEN_ID_UP) /* planted: any disk will do */
            r = chunk_store_open_quiet(paths[i], NULL, &st);
        if (r == CHUNK_ERR_BUSY && chunk_mutant == CHUNK_MUT_OPEN_BUSY_DOWN) /* planted: another owner's store is down */
            r = TSDB_ERR_NOTFOUND;
        if (r == 0) {
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
    int need = o ? o->min_up : up; /* NULL: every store that was named must open (what chunk_be_open means) */
    if (!o) {
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
            if (atomic_load(&be->state[i]) != CHUNK_STORE_UP)
                len += (size_t)snprintf(names + len, sizeof names - len, "%s#%d %s (%s)", len ? ", " : "", i,
                                        be->path[i] ? be->path[i] : "-",
                                        chunk_store_state_name(atomic_load(&be->state[i])));
        fprintf(stderr, "tsdb: chunk backend: %d of %d stores are up, %d are needed: down: %s\n", up, n, need, names);
        rc = TSDB_ERR_UNAVAILABLE;
    }
    for (int i = 0; i < be->n && rc == 0; i++) { /* the fence: the maximum over the stores present */
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
    for (int i = 0; i < be->n; i++) {
        chunk_store_close(atomic_load(&be->st[i])); /* NULL for a store opened as down */
        free(be->path[i]);
    }
    for (int i = 0; i < be->nretired; i++)
        chunk_store_close(be->retired[i]);
    pthread_mutex_destroy(&be->mu);
    free(be->st);
    free(be->state);
    free(be->path);
    free(be->retired);
    free(be->f);
    free(be);
}

int chunk_be_nstores(const chunk_backend *be) { return be->n; }

uint64_t chunk_be_epoch(chunk_backend *be, uint32_t volume)
{
    pthread_mutex_lock(&be->mu);
    struct fence *f = fence_find(be, volume, false);
    uint64_t e = f ? f->epoch : 0;
    pthread_mutex_unlock(&be->mu);
    return e;
}

int chunk_be_see_epoch(chunk_backend *be, uint32_t volume, uint64_t e)
{
    pthread_mutex_lock(&be->mu);
    struct fence *f = fence_find(be, volume, true);
    int rc = f ? 0 : TSDB_ERR_NOMEM;
    bool unsure = false; /* a store whose EPOCH lost a slot needs this call even at the epoch it already holds */
    for (int i = 0; f && i < be->n; i++) {
        chunk_store *st = chunk_be_open_store(be, i);
        unsure |= st && chunk_store_epoch_unsure(st, volume);
    }
    if (f && (e > f->epoch || unsure)) {
        if (e > f->epoch)
            f->epoch = e; /* in memory first: a store that fails to sync it still does not act below it */
        int ok = 0;
        for (int i = 0; i < be->n; i++) {
            chunk_store *st = chunk_be_open_store(be, i);
            if (!st || chunk_store_failed(st))
                continue; /* a store that acknowledges nothing needs no fence: it is a device DOWN (E27) */
            int r = chunk_store_raise(st, volume, e);
            if (r == 0)
                ok++;
            else if (rc == 0)
                rc = r;
        }
        if (ok == 0 && rc == 0)
            rc = TSDB_ERR_IO; /* every store has failed: nothing holds the epoch durably */
    }
    pthread_mutex_unlock(&be->mu);
    return rc;
}

/* The fence before a put or delete at epoch e: refused below it, raised (in every store) above it. A store whose EPOCH
 * lost a slot refuses until chunk_be_see_epoch (the authoritative call) has been made: a writer's own epoch must not
 * stand in for the fence that was lost. */
int chunk_be_prepare(chunk_backend *be, uint32_t volume, uint64_t e)
{
    uint64_t cur = chunk_be_epoch(be, volume);
    if (e < cur && chunk_mutant != CHUNK_MUT_NOFENCE)
        return CHUNK_ERR_FENCED;
    for (int i = 0; i < be->n; i++) {
        chunk_store *st = chunk_be_open_store(be, i);
        if (st && chunk_store_epoch_unsure(st, volume))
            return CHUNK_ERR_EPOCH_UNSURE;
    }
    return e > cur ? chunk_be_see_epoch(be, volume, e) : 0;
}

static chunk_store *be_target(chunk_backend *be, int store, const chunk_ident *id, int *err)
{
    if (store < 0)
        store = chunk_be_place(be, id->oid, id->pos);
    *err = 0;
    if (store >= be->n) {
        *err = TSDB_ERR_INVALID;
        return NULL;
    }
    chunk_store *st = chunk_be_open_store(be, store);
    if (!st)
        *err = TSDB_ERR_UNAVAILABLE; /* a store that is DOWN */
    return st;
}

int chunk_be_place(const chunk_backend *be, const uint8_t oid[16], unsigned pos)
{
    return (int)((xxh64(oid, 16, 0) + pos) % (uint64_t)be->n);
}

int chunk_be_pos_store(const chunk_backend *be, const uint8_t oid[16], unsigned pos) { return chunk_be_place(be, oid, pos); }

int chunk_be_wr_open(chunk_backend *be, int store, const chunk_ident *id, const chunk_geom *g, uint64_t epoch,
                     chunk_wr **out)
{
    int rc = chunk_be_prepare(be, id->volume, epoch);
    if (rc)
        return rc;
    chunk_store *st = be_target(be, store, id, &rc);
    return st ? chunk_wr_open(st, id, g, epoch, out) : rc;
}

int chunk_be_put(chunk_backend *be, int store, const chunk_ident *id, const chunk_geom *g, uint64_t epoch,
                 const void *cells, const uint64_t *sums)
{
    int rc = chunk_be_prepare(be, id->volume, epoch);
    if (rc)
        return rc;
    chunk_store *st = be_target(be, store, id, &rc);
    return st ? chunk_store_put(st, id, g, epoch, cells, sums) : rc;
}

int chunk_be_rd_open(chunk_backend *be, const chunk_ident *id, chunk_rd **out)
{
    int worst = TSDB_ERR_NOTFOUND;
    for (int i = 0; i < be->n; i++) {
        chunk_store *st = chunk_be_open_store(be, i);
        if (!st)
            continue;
        int rc = chunk_rd_open(st, id, out);
        if (rc == 0)
            return 0;
        if (rc == TSDB_ERR_IO || (rc == TSDB_ERR_CORRUPT && worst != TSDB_ERR_IO))
            worst = rc;
    }
    return worst;
}

int chunk_be_del(chunk_backend *be, const chunk_ident *id, uint64_t epoch)
{
    int rc = chunk_be_prepare(be, id->volume, epoch);
    if (rc)
        return rc;
    bool found = false;
    for (int i = 0; i < be->n; i++) {
        chunk_store *st = chunk_be_open_store(be, i);
        if (!st)
            continue;
        rc = chunk_store_del(st, id, epoch);
        if (rc == 0)
            found = true;
        else if (rc != TSDB_ERR_NOTFOUND)
            return rc;
    }
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

int chunk_be_list(chunk_backend *be, uint32_t volume, bool verify, chunk_list_fn fn, void *ctx)
{
    for (int i = 0; i < be->n; i++) {
        chunk_store *st = chunk_be_open_store(be, i);
        if (!st)
            continue;
        list_ctx l = {fn, ctx, i};
        int rc = chunk_store_list(st, volume, verify, list_one, &l);
        if (rc)
            return rc;
    }
    return 0;
}

/* ---------------------------------------------------------------- put an object */
typedef struct put_ctx {
    chunk_wr *w[CHUNK_POS_MAX];
} put_ctx;

static int put_cell(void *ctx, unsigned pos, uint64_t idx, const uint8_t *data, size_t len, uint64_t sum)
{
    (void)idx;
    chunk_wr *w = ((put_ctx *)ctx)->w[pos];
    return w ? chunk_wr_cell_trusted(w, data, len, sum) : 0;
}

int chunk_be_put_object(chunk_backend *be, uint32_t volume, const uint8_t vuuid[16], const uint8_t oid[16],
                        const chunk_geom *g, uint64_t epoch, const uint8_t *obj, unsigned posmask, uint64_t *sums_out)
{
    if (!chunk_geom_valid(g))
        return TSDB_ERR_INVALID;
    put_ctx pc;
    memset(&pc, 0, sizeof pc);
    unsigned np = chunk_npos(g);
    int base = chunk_be_place(be, oid, 0), rc = 0;
    for (unsigned p = 0; p < np && rc == 0; p++) {
        if (!(posmask >> p & 1))
            continue;
        chunk_ident id = {.volume = volume, .pos = (uint16_t)p};
        memcpy(id.vuuid, vuuid, 16);
        memcpy(id.oid, oid, 16);
        rc = chunk_be_wr_open(be, (base + (int)p) % be->n, &id, g, epoch, &pc.w[p]);
    }
    if (rc == 0)
        rc = chunk_encode(g, obj, put_cell, &pc, sums_out);
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
}
