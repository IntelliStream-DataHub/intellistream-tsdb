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
} file;

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
    files = calloc(MAX_FILES, sizeof *files);
    dops = calloc(MAX_OPS, sizeof *dops);
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

__attribute__((noreturn)) static void power_loss(void)
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
    static const char msg[] = "crashfs: power loss\n";
    if (real_write(2, msg, sizeof msg - 1) < 0) {
    }
    _exit(99);
}

/* Called with the lock held, before each tracked operation. */
static void maybe_crash(void)
{
    ops++;
    if (crash_requested || (ops > warmup && prob > 0 && urand() < prob))
        power_loss();
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
    if (f && off < f->durable) { /* a rewrite of durable data */
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
    return r;
}

ssize_t pwrite64(int fd, const void *buf, size_t n, off_t off) __attribute__((alias("pwrite")));

static int do_sync(int fd, bool data_only)
{
    if (!fd_tracked(fd))
        return data_only ? real_fdatasync(fd) : real_fsync(fd);
    pthread_mutex_lock(&lock);
    maybe_crash();
    int r = data_only ? real_fdatasync(fd) : real_fsync(fd);
    if (r == 0) {
        struct stat st;
        if (fstat(fd, &st) == 0 && S_ISDIR(st.st_mode)) {
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
            if (f) {
                f->durable = st.st_size;
                f->ndirty = 0;
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
