/*
 * crashfs: an LD_PRELOAD shim that simulates losing power, in the spirit of
 * Jepsen's LazyFS and of ALICE.
 *
 * For files under $CRASHFS_ROOT it keeps what a disk would really hold:
 *  - file data is durable up to the size it had at its last fsync/fdatasync;
 *    regions rewritten in place since then are dirty;
 *  - creating, renaming and removing a directory entry is durable only after
 *    an fsync of that directory.
 * On a "power loss" it truncates every file to its durable size, zeroes the
 * dirty regions (harsher than a real disk, which keeps old or new pages),
 * undoes every directory change that was not made durable, newest first, and
 * exits at once.
 *
 * A power loss happens with probability $CRASHFS_PROB at each tracked
 * operation (after $CRASHFS_WARMUP of them), or at the next one after
 * SIGUSR2. Every tracked operation runs under one lock, so no thread can
 * write while the state is being rolled back.
 *
 * With CRASHFS_MODE=eio the fault is an I/O error instead, the way Linux
 * reports a failed writeback ("fsyncgate", PostgreSQL 2018): at the point
 * where a power loss would happen, the next write, pwrite, fsync or fdatasync
 * of a tracked file fails with EIO (a write after storing half of its bytes).
 * From then on that file or directory lies, in the worst way Linux allows:
 * its syncs return 0 but make nothing durable, because the kernel dropped the
 * pages and cleared the error. When the process exits, everything not made
 * durable is rolled back as in a power loss, so a write acknowledged after
 * the error, on the strength of a sync that lied, is lost and a checker sees
 * it. One fault per process; SIGUSR2 after it is a power loss at the next
 * tracked operation, for a process that carries on. With $CRASHFS_EIO_ONLY, only files whose name
 * starts with it fail (e.g. "wal_"). If $CRASHFS_MARK is set, the path of the
 * file that failed is written there, so a harness knows the fault happened.
 *
 * With CRASHFS_TEAR=1 a power loss keeps what a real disk may keep rather
 * than the least it must (each choice drawn from $CRASHFS_SEED):
 *  - of a file's unsynced appends, a random prefix: none, all, or up to a
 *    512-byte sector boundary (the page cache writes back in any order, but
 *    a sector is written whole);
 *  - or, at random, nothing after the durable end but garbage up to the end
 *    of the sector that holds it (that sector was being written when the
 *    power went; the bytes before the durable end were synced and stay);
 *  - of the sectors rewritten in place since the last sync, each one old or
 *    new at random (MANIFEST is rewritten in place);
 *  - of the directory operations not made durable, a random subset, in the
 *    order they were made for each name: once one on a name is undone,
 *    every later one on that name (or inside it) is undone too.
 */
#define _GNU_SOURCE
#include <dirent.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define MAX_FDS 65536
#define MAX_FILES 65536
#define MAX_OPS 1000000
#define MAX_DIRTY 16

typedef struct file {
    dev_t dev;
    ino_t ino;
    char path[PATH_MAX];
    off_t durable;
    struct {
        off_t off, len;
    } dirty[MAX_DIRTY];
    int ndirty;
    bool used;
    bool lying; /* after an injected EIO: syncs succeed without making anything durable */
    struct old_sector *old; /* CRASHFS_TEAR: durable sectors rewritten since the last sync, as they were */
    size_t nold, capold;
} file;

#define SECTOR 512
struct old_sector {
    off_t off;
    unsigned char data[SECTOR];
};

typedef enum { OP_CREATE, OP_RENAME, OP_UNLINK, OP_MKDIR } op_kind;

typedef struct dirop {
    op_kind kind;
    char dir[PATH_MAX];   /* the directory whose fsync makes it durable */
    char a[PATH_MAX];     /* created / renamed from / removed */
    char b[PATH_MAX];     /* renamed to */
    char saved[PATH_MAX]; /* a file this op replaced or removed, kept aside */
    bool done;            /* made durable */
} dirop;

static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static char root[PATH_MAX];
static size_t rootlen;
static bool active;
static double prob;
static long warmup, ops;
static uint64_t rng;
static volatile sig_atomic_t crash_requested;
static bool tear; /* CRASHFS_TEAR=1 */
static unsigned tear_prefix, tear_garbage, tear_old, tear_kept; /* what the power loss kept, for the message */
static bool eio_mode, eio_done; /* CRASHFS_MODE=eio, and the error was injected */
static char lying_dir[PATH_MAX]; /* a directory whose syncs lie after the error */
static char mark[PATH_MAX], eio_only[64];
static char *fdpath[MAX_FDS];
static file *files;
static size_t nfiles;
static dirop *dops;
static size_t ndops;
static unsigned saved_counter;

#define REAL(name) static __typeof__(name) *real_##name
REAL(open);
REAL(openat);
REAL(write);
REAL(pwrite);
REAL(fsync);
REAL(fdatasync);
REAL(rename);
REAL(renameat);
REAL(unlink);
REAL(unlinkat);
REAL(close);
REAL(mkdir);
REAL(mkdirat);
REAL(ftruncate);

static void on_usr2(int sig)
{
    (void)sig;
    crash_requested = 1;
}

/* 16 GB of address space, touched only as far as used. Not calloc: a sanitizer's allocator
 * clears it all, which takes seconds when the shim is preloaded into a sanitizer build. */
static void *zeroed(size_t size)
{
    void *p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    return p == MAP_FAILED ? NULL : p;
}

__attribute__((constructor)) static void init(void)
{
#define LOAD(name) real_##name = dlsym(RTLD_NEXT, #name)
    LOAD(open);
    LOAD(openat);
    LOAD(write);
    LOAD(pwrite);
    LOAD(fsync);
    LOAD(fdatasync);
    LOAD(rename);
    LOAD(renameat);
    LOAD(unlink);
    LOAD(unlinkat);
    LOAD(close);
    LOAD(mkdir);
    LOAD(mkdirat);
    LOAD(ftruncate);
    const char *r = getenv("CRASHFS_ROOT");
    if (!r || !realpath(r, root))
        return;
    rootlen = strlen(root);
    prob = getenv("CRASHFS_PROB") ? atof(getenv("CRASHFS_PROB")) : 0;
    warmup = getenv("CRASHFS_WARMUP") ? atol(getenv("CRASHFS_WARMUP")) : 0;
    rng = getenv("CRASHFS_SEED") ? strtoull(getenv("CRASHFS_SEED"), NULL, 10) : (uint64_t)getpid();
    rng = rng * 6364136223846793005ull + 1;
    files = zeroed(MAX_FILES * sizeof *files);
    dops = zeroed(MAX_OPS * sizeof *dops);
    eio_mode = getenv("CRASHFS_MODE") && !strcmp(getenv("CRASHFS_MODE"), "eio");
    if (getenv("CRASHFS_EIO_ONLY"))
        snprintf(eio_only, sizeof eio_only, "%s", getenv("CRASHFS_EIO_ONLY"));
    if (getenv("CRASHFS_MARK"))
        snprintf(mark, sizeof mark, "%s", getenv("CRASHFS_MARK"));
    tear = getenv("CRASHFS_TEAR") && atoi(getenv("CRASHFS_TEAR")) != 0;
    signal(SIGUSR2, on_usr2);
    active = files && dops;
}

static double urand(void)
{
    rng ^= rng << 13;
    rng ^= rng >> 7;
    rng ^= rng << 17;
    return (double)(rng >> 11) / 9007199254740992.0;
}

/* ------------------------------------------------------------- tracking */

static bool tracked(const char *p) { return active && strncmp(p, root, rootlen) == 0; }

static void path_of_at(int dirfd, const char *name, char *out)
{
    if (name[0] == '/' || dirfd == AT_FDCWD) {
        if (name[0] == '/')
            snprintf(out, PATH_MAX, "%s", name);
        else {
            char cwd[PATH_MAX];
            snprintf(out, PATH_MAX, "%s/%s", getcwd(cwd, sizeof cwd) ? cwd : ".", name);
        }
        return;
    }
    char link[64], dir[PATH_MAX];
    snprintf(link, sizeof link, "/proc/self/fd/%d", dirfd);
    ssize_t n = readlink(link, dir, sizeof dir - 1);
    dir[n > 0 ? n : 0] = '\0';
    snprintf(out, PATH_MAX, "%s/%s", dir, name);
}

static void dir_of(const char *p, char *out)
{
    snprintf(out, PATH_MAX, "%s", p);
    char *s = strrchr(out, '/');
    if (s && s != out)
        *s = '\0';
}

static file *file_get(const char *path, bool create_new)
{
    struct stat st;
    if (stat(path, &st) != 0)
        return NULL;
    for (size_t i = 0; i < nfiles; i++)
        if (files[i].used && files[i].dev == st.st_dev && files[i].ino == st.st_ino) {
            snprintf(files[i].path, PATH_MAX, "%s", path);
            return &files[i];
        }
    if (nfiles == MAX_FILES || !S_ISREG(st.st_mode))
        return NULL;
    file *f = &files[nfiles++];
    memset(f, 0, sizeof *f);
    f->used = true;
    f->dev = st.st_dev;
    f->ino = st.st_ino;
    snprintf(f->path, PATH_MAX, "%s", path);
    f->durable = create_new ? 0 : st.st_size; /* files from before we started are durable */
    return f;
}

/* CRASHFS_TEAR: keeps the durable content of the sectors in [off, end) that
 * are about to change, once per sync, so a power loss can bring them back. */
static void keep_old(file *f, int fd, off_t off, off_t end)
{
    if (!tear || f->lying)
        return;
    if (end > f->durable)
        end = f->durable;
    for (off_t s = off / SECTOR * SECTOR; s < end; s += SECTOR) {
        bool have = false;
        for (size_t i = 0; i < f->nold && !have; i++)
            have = f->old[i].off == s;
        if (have)
            continue;
        if (f->nold == f->capold) {
            size_t cap = f->capold ? 2 * f->capold : 16;
            struct old_sector *o = realloc(f->old, cap * sizeof *o);
            if (!o)
                return;
            f->old = o;
            f->capold = cap;
        }
        struct old_sector *o = &f->old[f->nold++];
        o->off = s;
        memset(o->data, 0, SECTOR);
        if (pread(fd, o->data, SECTOR, s) < 0) {
        }
    }
}

static void add_dirop(op_kind k, const char *a, const char *b, const char *saved)
{
    if (ndops == MAX_OPS)
        return;
    dirop *d = &dops[ndops++];
    memset(d, 0, sizeof *d);
    d->kind = k;
    dir_of(k == OP_RENAME ? b : a, d->dir);
    snprintf(d->a, PATH_MAX, "%s", a);
    if (b)
        snprintf(d->b, PATH_MAX, "%s", b);
    if (saved)
        snprintf(d->saved, PATH_MAX, "%s", saved);
}

/* Moves an existing file aside so that undoing can bring it back. */
static bool keep_aside(const char *path, char *saved)
{
    struct stat st;
    if (lstat(path, &st) != 0)
        return false;
    char dir[PATH_MAX];
    dir_of(path, dir);
    snprintf(saved, PATH_MAX, "%s/.crashfs_saved_%d_%u", dir, (int)getpid(), saved_counter++);
    return real_rename(path, saved) == 0;
}

/* ---------------------------------------------------------- power loss */

static void rm_tree(const char *p)
{
    struct stat st;
    if (lstat(p, &st) != 0)
        return;
    if (S_ISDIR(st.st_mode)) {
        DIR *d = opendir(p);
        struct dirent *e;
        while (d && (e = readdir(d))) {
            if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, ".."))
                continue;
            char c[PATH_MAX * 2];
            snprintf(c, sizeof c, "%s/%s", p, e->d_name);
            rm_tree(c);
        }
        if (d)
            closedir(d);
        rmdir(p);
    } else {
        real_unlink(p);
    }
}

/* CRASHFS_TEAR: what a disk may keep of a file (see the header). */
static void tear_file(file *f, int fd, off_t size)
{
    /* sectors rewritten in place: old or new, each; past the current end
     * (an unsynced truncate) the old ones come back */
    for (size_t i = 0; i < f->nold; i++) {
        struct old_sector *o = &f->old[i];
        off_t len = f->durable - o->off < SECTOR ? f->durable - o->off : SECTOR;
        if (o->off + len > size || urand() < 0.5) {
            tear_old++;
            if (real_pwrite(fd, o->data, (size_t)len, o->off) < 0) {
            }
        }
    }
    if (size <= f->durable)
        return;
    off_t keep;
    off_t sector_end = (f->durable + SECTOR - 1) / SECTOR * SECTOR;
    if (f->durable % SECTOR && urand() < 0.25) {
        /* the sector holding the durable end was being written: garbage after that end */
        keep = sector_end < size ? sector_end : size;
        tear_garbage++;
        unsigned char junk[SECTOR];
        for (off_t k = 0; k < keep - f->durable; k++)
            junk[k] = (unsigned char)(urand() * 256);
        if (real_pwrite(fd, junk, (size_t)(keep - f->durable), f->durable) < 0) {
        }
    } else {
        /* cut at the durable end, at a sector boundary after it, or nowhere: one of them at random */
        off_t first = f->durable / SECTOR * SECTOR + SECTOR;
        off_t nb = first < size ? (size - 1 - first) / SECTOR + 1 : 0; /* boundaries strictly inside */
        off_t k = (off_t)(urand() * (double)(nb + 2));
        keep = k == 0 ? f->durable : k > nb ? size : first + (k - 1) * SECTOR;
        if (keep > f->durable)
            tear_prefix++;
    }
    if (keep < size && real_ftruncate(fd, keep) != 0) {
    }
}

/* CRASHFS_TEAR: names whose undone operations force later ones on them (or inside them) to be undone too. */
static bool name_cut(char **cut, size_t ncut, const char *name)
{
    if (!name[0])
        return false;
    for (size_t i = 0; i < ncut; i++) {
        size_t n = strlen(cut[i]);
        if (!strncmp(cut[i], name, n) && (name[n] == '\0' || name[n] == '/'))
            return true;
    }
    return false;
}

static void tear_dirops(void)
{
    char **cut = malloc((2 * ndops + 1) * sizeof *cut);
    size_t ncut = 0;
    if (!cut)
        return;
    /* oldest first: each not durable op is kept at random, unless an earlier one on its names was undone */
    for (size_t i = 0; i < ndops; i++) {
        dirop *d = &dops[i];
        if (d->done)
            continue;
        char srcdir[PATH_MAX];
        dir_of(d->a, srcdir);
        bool forced = name_cut(cut, ncut, d->a) || name_cut(cut, ncut, d->b) || name_cut(cut, ncut, srcdir) ||
                      name_cut(cut, ncut, d->dir);
        if (!forced && urand() < 0.5) {
            d->done = true; /* reached the disk after all */
            tear_kept++;
            continue;
        }
        cut[ncut++] = d->a;
        if (d->b[0])
            cut[ncut++] = d->b;
    }
    free(cut);
}

/* Rolls the tracked tree back to what was made durable. Called with the lock held. */
static void rollback(void)
{
    /* file data first, by each file's current name */
    for (size_t i = 0; i < nfiles; i++) {
        file *f = &files[i];
        struct stat st;
        if (!f->used || stat(f->path, &st) != 0 || st.st_ino != f->ino)
            continue;
        int fd = real_open(f->path, O_WRONLY);
        if (fd < 0)
            continue;
        if (tear) {
            tear_file(f, fd, st.st_size);
            real_close(fd);
            continue;
        }
        for (int k = 0; k < f->ndirty; k++) { /* rewritten in place, never synced */
            off_t end = f->dirty[k].off + f->dirty[k].len;
            if (end > f->durable)
                end = f->durable;
            static const char zeros[4096];
            for (off_t o = f->dirty[k].off; o < end; o += (off_t)sizeof zeros) {
                size_t n = (size_t)(end - o < (off_t)sizeof zeros ? end - o : (off_t)sizeof zeros);
                if (real_pwrite(fd, zeros, n, o) < 0)
                    break;
            }
        }
        if (st.st_size > f->durable)
            real_ftruncate(fd, f->durable);
        real_close(fd);
    }
    /* then directory entries, newest first */
    if (tear)
        tear_dirops();
    for (size_t i = ndops; i-- > 0;) {
        dirop *d = &dops[i];
        if (d->done)
            continue;
        switch (d->kind) {
        case OP_CREATE: real_unlink(d->a); break;
        case OP_MKDIR: rm_tree(d->a); break;
        case OP_RENAME:
            real_rename(d->b, d->a);
            if (d->saved[0])
                real_rename(d->saved, d->b);
            break;
        case OP_UNLINK:
            if (d->saved[0])
                real_rename(d->saved, d->a);
            break;
        }
    }
    /* entries made durable that were kept aside: really gone now */
    for (size_t i = 0; i < ndops; i++)
        if (dops[i].done && dops[i].saved[0])
            rm_tree(dops[i].saved);
}

__attribute__((noreturn)) static void power_loss(void)
{
    rollback();
    char msg[200];
    int n = tear ? snprintf(msg, sizeof msg,
                            "crashfs: power loss (torn: %u unsynced prefixes kept, %u garbage tails, %u old sectors "
                            "back, %u unsynced directory operations kept)\n",
                            tear_prefix, tear_garbage, tear_old, tear_kept)
                 : snprintf(msg, sizeof msg, "crashfs: power loss\n");
    if (real_write(2, msg, (size_t)n) < 0) {
    }
    _exit(99);
}

/* Called with the lock held, before each tracked operation. In eio mode a
 * fault only becomes due here; eio_now() injects it at the next write or sync. */
static void maybe_crash(void)
{
    ops++;
    if (eio_mode) {
        if (eio_done && crash_requested) /* a process that carried on after the error loses power */
            power_loss();
        if (!eio_done && !crash_requested && ops > warmup && prob > 0 && urand() < prob)
            crash_requested = 1;
        return;
    }
    if (crash_requested || (ops > warmup && prob > 0 && urand() < prob))
        power_loss();
}

/* Called with the lock held by a write or sync of fd: true if it must fail
 * with EIO now. Marks the file (or directory) as lying from here on. */
static bool eio_now(int fd, const char *what)
{
    if (!eio_mode || eio_done || !crash_requested)
        return false;
    const char *base = strrchr(fdpath[fd], '/') ? strrchr(fdpath[fd], '/') + 1 : fdpath[fd];
    if (eio_only[0] && strncmp(base, eio_only, strlen(eio_only)) != 0)
        return false;
    eio_done = true;
    crash_requested = 0;
    struct stat st;
    if (fstat(fd, &st) == 0 && S_ISDIR(st.st_mode)) {
        snprintf(lying_dir, sizeof lying_dir, "%s", fdpath[fd]);
    } else {
        file *f = file_get(fdpath[fd], false);
        if (f)
            f->lying = true;
    }
    char msg[PATH_MAX + 64];
    int n = snprintf(msg, sizeof msg, "crashfs: EIO on %s of %s\n", what, fdpath[fd]);
    if (real_write(2, msg, (size_t)n) < 0) {
    }
    if (mark[0]) {
        int m = real_open(mark, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (m >= 0) {
            n = snprintf(msg, sizeof msg, "%s\n", fdpath[fd]);
            if (real_write(m, msg, (size_t)n) < 0) {
            }
            real_close(m);
        }
    }
    return true;
}

/* After an injected EIO the process may exit on its own (fail-stop) or be
 * stopped: either way, what its lying syncs did not make durable is gone. */
__attribute__((destructor)) static void fini(void)
{
    if (!eio_done)
        return;
    pthread_mutex_lock(&lock); /* never released: no thread writes after this */
    rollback();
    static const char msg[] = "crashfs: at exit, unsynced data lost\n";
    if (real_write(2, msg, sizeof msg - 1) < 0) {
    }
}

/* --------------------------------------------------------------- hooks */

static int do_open(int dirfd, const char *name, int flags, mode_t mode, bool at)
{
    char path[PATH_MAX];
    path_of_at(at ? dirfd : AT_FDCWD, name, path);
    if (!tracked(path))
        return at ? real_openat(dirfd, name, flags, mode) : real_open(name, flags, mode);
    pthread_mutex_lock(&lock);
    maybe_crash();
    struct stat st;
    bool existed = stat(path, &st) == 0;
    if ((flags & O_TRUNC) && existed && S_ISREG(st.st_mode)) {
        file *f = file_get(path, false);
        if (f && f->durable > 0) { /* truncating durable data: keep it until synced */
            char saved[PATH_MAX];
            if (keep_aside(path, saved)) {
                add_dirop(OP_UNLINK, path, NULL, saved);
                f->used = false;
                existed = false;
            }
        }
    }
    int fd = at ? real_openat(dirfd, name, flags, mode) : real_open(name, flags, mode);
    if (fd >= 0) {
        if ((flags & O_CREAT) && !existed) {
            add_dirop(OP_CREATE, path, NULL, NULL);
            file_get(path, true);
        } else {
            file_get(path, false);
        }
        if (fd < MAX_FDS) {
            free(fdpath[fd]);
            fdpath[fd] = strdup(path);
        }
    }
    pthread_mutex_unlock(&lock);
    return fd;
}

int open(const char *name, int flags, ...)
{
    mode_t mode = 0;
    if (flags & (O_CREAT | O_TMPFILE)) {
        va_list ap;
        va_start(ap, flags);
        mode = (mode_t)va_arg(ap, int);
        va_end(ap);
    }
    return do_open(AT_FDCWD, name, flags, mode, false);
}

int openat(int dirfd, const char *name, int flags, ...)
{
    mode_t mode = 0;
    if (flags & (O_CREAT | O_TMPFILE)) {
        va_list ap;
        va_start(ap, flags);
        mode = (mode_t)va_arg(ap, int);
        va_end(ap);
    }
    return do_open(dirfd, name, flags, mode, true);
}

int open64(const char *name, int flags, ...) __attribute__((alias("open")));
int openat64(int dirfd, const char *name, int flags, ...) __attribute__((alias("openat")));

static bool fd_tracked(int fd) { return fd >= 0 && fd < MAX_FDS && fdpath[fd]; }

ssize_t write(int fd, const void *buf, size_t n)
{
    if (!fd_tracked(fd))
        return real_write(fd, buf, n);
    pthread_mutex_lock(&lock);
    maybe_crash();
    if (eio_now(fd, "write")) {
        if (n > 1 && real_write(fd, buf, n / 2) < 0) { /* a torn record */
        }
        pthread_mutex_unlock(&lock);
        errno = EIO;
        return -1;
    }
    ssize_t r = real_write(fd, buf, n); /* tsdb only appends with write() */
    pthread_mutex_unlock(&lock);
    return r;
}

ssize_t pwrite(int fd, const void *buf, size_t n, off_t off)
{
    if (!fd_tracked(fd))
        return real_pwrite(fd, buf, n, off);
    pthread_mutex_lock(&lock);
    maybe_crash();
    file *f = file_get(fdpath[fd], false);
    bool fail = eio_now(fd, "pwrite");
    if (fail)
        n = n > 1 ? n / 2 : 0;
    if (f && off < f->durable) { /* a rewrite of durable data */
        keep_old(f, fd, off, off + (off_t)n);
        if (f->ndirty < MAX_DIRTY) {
            f->dirty[f->ndirty].off = off;
            f->dirty[f->ndirty].len = (off_t)n;
            f->ndirty++;
        } else {
            f->dirty[MAX_DIRTY - 1].len = f->durable; /* everything from there on */
        }
    }
    ssize_t r = real_pwrite(fd, buf, n, off);
    pthread_mutex_unlock(&lock);
    if (fail) {
        errno = EIO;
        return -1;
    }
    return r;
}

ssize_t pwrite64(int fd, const void *buf, size_t n, off_t off) __attribute__((alias("pwrite")));

static int do_sync(int fd, bool data_only)
{
    if (!fd_tracked(fd))
        return data_only ? real_fdatasync(fd) : real_fsync(fd);
    pthread_mutex_lock(&lock);
    maybe_crash();
    if (eio_now(fd, data_only ? "fdatasync" : "fsync")) { /* the pages are dropped: nothing becomes durable */
        pthread_mutex_unlock(&lock);
        errno = EIO;
        return -1;
    }
    int r = data_only ? real_fdatasync(fd) : real_fsync(fd);
    if (r == 0) {
        struct stat st;
        if (fstat(fd, &st) == 0 && S_ISDIR(st.st_mode)) {
            if (lying_dir[0] && !strcmp(lying_dir, fdpath[fd])) {
                pthread_mutex_unlock(&lock);
                return 0;
            }
            /* the directory's entries are durable now */
            for (size_t i = 0; i < ndops; i++)
                if (!dops[i].done && !strcmp(dops[i].dir, fdpath[fd])) {
                    dops[i].done = true;
                    if (dops[i].kind == OP_RENAME && dops[i].a[0]) {
                        /* the old name is gone for good too, if it was in another dir */
                    }
                }
        } else {
            file *f = file_get(fdpath[fd], false);
            if (f && !f->lying) {
                f->durable = st.st_size;
                f->ndirty = 0;
                f->nold = 0;
            }
        }
    }
    pthread_mutex_unlock(&lock);
    return r;
}

int fsync(int fd) { return do_sync(fd, false); }
int fdatasync(int fd) { return do_sync(fd, true); }

static int do_rename(int ofd, const char *oldp, int nfd, const char *newp, bool at)
{
    char a[PATH_MAX], b[PATH_MAX];
    path_of_at(at ? ofd : AT_FDCWD, oldp, a);
    path_of_at(at ? nfd : AT_FDCWD, newp, b);
    if (!tracked(a) && !tracked(b))
        return at ? real_renameat(ofd, oldp, nfd, newp) : real_rename(oldp, newp);
    pthread_mutex_lock(&lock);
    maybe_crash();
    char saved[PATH_MAX] = "";
    struct stat st;
    if (stat(b, &st) == 0 && S_ISREG(st.st_mode)) {
        /* replacing an existing file: keep the old one for an undo */
        char hard[PATH_MAX];
        char dir[PATH_MAX];
        dir_of(b, dir);
        snprintf(hard, PATH_MAX, "%s/.crashfs_saved_%d_%u", dir, (int)getpid(), saved_counter++);
        if (link(b, hard) == 0)
            snprintf(saved, PATH_MAX, "%s", hard);
    }
    int r = at ? real_renameat(ofd, oldp, nfd, newp) : real_rename(oldp, newp);
    if (r == 0) {
        add_dirop(OP_RENAME, a, b, saved[0] ? saved : NULL);
        file_get(b, false);
    } else if (saved[0]) {
        real_unlink(saved);
    }
    pthread_mutex_unlock(&lock);
    return r;
}

int rename(const char *a, const char *b) { return do_rename(AT_FDCWD, a, AT_FDCWD, b, false); }
int renameat(int ofd, const char *a, int nfd, const char *b) { return do_rename(ofd, a, nfd, b, true); }

static int do_unlink(int dirfd, const char *name, int flags, bool at)
{
    char p[PATH_MAX];
    path_of_at(at ? dirfd : AT_FDCWD, name, p);
    if (!tracked(p) || (flags & AT_REMOVEDIR))
        return at ? real_unlinkat(dirfd, name, flags) : real_unlink(name);
    pthread_mutex_lock(&lock);
    maybe_crash();
    char saved[PATH_MAX];
    int r;
    if (keep_aside(p, saved)) {
        add_dirop(OP_UNLINK, p, NULL, saved);
        r = 0;
    } else {
        r = at ? real_unlinkat(dirfd, name, flags) : real_unlink(name);
    }
    pthread_mutex_unlock(&lock);
    return r;
}

int unlink(const char *name) { return do_unlink(AT_FDCWD, name, 0, false); }
int unlinkat(int dirfd, const char *name, int flags) { return do_unlink(dirfd, name, flags, true); }

int mkdir(const char *p, mode_t mode)
{
    char a[PATH_MAX];
    path_of_at(AT_FDCWD, p, a);
    if (!tracked(a))
        return real_mkdir(p, mode);
    pthread_mutex_lock(&lock);
    maybe_crash();
    int r = real_mkdir(p, mode);
    if (r == 0)
        add_dirop(OP_MKDIR, a, NULL, NULL);
    pthread_mutex_unlock(&lock);
    return r;
}

int mkdirat(int dirfd, const char *p, mode_t mode)
{
    char a[PATH_MAX];
    path_of_at(dirfd, p, a);
    if (!tracked(a))
        return real_mkdirat(dirfd, p, mode);
    pthread_mutex_lock(&lock);
    maybe_crash();
    int r = real_mkdirat(dirfd, p, mode);
    if (r == 0)
        add_dirop(OP_MKDIR, a, NULL, NULL);
    pthread_mutex_unlock(&lock);
    return r;
}

int ftruncate(int fd, off_t len)
{
    if (!fd_tracked(fd))
        return real_ftruncate(fd, len);
    pthread_mutex_lock(&lock);
    maybe_crash();
    file *f = file_get(fdpath[fd], false);
    if (f && len < f->durable)
        keep_old(f, fd, len, f->durable);
    if (f && len < f->durable && f->ndirty < MAX_DIRTY) { /* cutting durable data, unsynced */
        f->dirty[f->ndirty].off = len;
        f->dirty[f->ndirty].len = f->durable - len;
        f->ndirty++;
    }
    int r = real_ftruncate(fd, len);
    pthread_mutex_unlock(&lock);
    return r;
}

int ftruncate64(int fd, off_t len) __attribute__((alias("ftruncate")));

int close(int fd)
{
    if (fd >= 0 && fd < MAX_FDS && fdpath[fd]) {
        pthread_mutex_lock(&lock);
        free(fdpath[fd]);
        fdpath[fd] = NULL;
        pthread_mutex_unlock(&lock);
    }
    return real_close(fd);
}
