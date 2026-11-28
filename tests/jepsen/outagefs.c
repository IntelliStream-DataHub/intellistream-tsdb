/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * outagefs: an LD_PRELOAD shim that takes a storage domain away, the way an NFS server does (ha.md §15.8; the
 * "crashfs control socket" of that section, built as its own shim so that it stacks with crashfs: preload both).
 *
 * $OUTAGEFS_ROOTS is a colon-separated list of directory prefixes (the "domains": two filesystems of one server and
 * the filesystem of another one). Every descriptor opened under a prefix is remembered, and each call on such a
 * descriptor or path goes through the prefix's current fault:
 *
 *   hang        the call blocks until the prefix is healed (a hard NFS mount that lost its server). A thread inside
 *               the call is lost, as it is in the kernel, until heal.
 *   eio         every call fails with EIO (a degraded pool); estale, etimedout likewise.
 *   eiodrop     the kernel's writeback failure (fsyncgate): a write succeeds and is thrown away; the first fsync of
 *               a descriptor fails with EIO, every later one returns 0 (the dirty pages are gone, the error was
 *               reported once). Everything else works. Data an engine acknowledges after retrying such an fsync
 *               is not on the storage.
 *   enospc      calls that add data or names fail with ENOSPC; reads and fsync of what exists work (a full pool).
 *   slow MS     every call takes MS milliseconds more.
 *   rate MB/s   cap read and write throughput on the prefix to MB/s megabytes per second (0 disables).
 *   fill PCT    override statvfs/fstatvfs on the prefix to report PCT% used (0..100; -1 disables).
 *   heal        no fault (also clears slow, rate and fill).
 *   snap        copies the prefix's tree to a snapshot directory.
 *   rollback    puts the snapshot back in place: files that exist in it get its bytes (same inode, so open
 *               descriptors see them: tsdb's NFS client would see ESTALE for a file that went away, SIGBUS in a
 *               mapping that now lies beyond the end), files it lacks are removed, files it has and the tree lacks
 *               are recreated. What `zpool import -F` or the loss of a SLOG does to acknowledged writes.
 *
 * $OUTAGEFS_CTL is a unix socket path the shim listens on from a thread of its own. A client writes one line
 * ("hang /mnt/a\n") and reads lines back, the last "ok\n" or "err why\n", once the fault is in force. "state"
 * answers with one line per prefix first. `outagefs_cmd` in tests/outagefs_ctl.h sends one.
 *
 * A hang that nobody ends does not outlive its harness: a held call returns EIO once the parent process of the
 * process that loaded the shim is gone (getppid() changed), once the control socket's file is gone, or after
 * $OUTAGEFS_HOLD_MAX_S seconds (default 900). Without that a server whose test was killed sits in the shim
 * forever, deaf to --exit-with-parent and SIGTERM (its event loop is inside the held call).
 *
 * Not covered: page faults on mapped files (ha.md §15.8's userfaultfd handler); a mapping of a hung prefix is not
 * stopped, and an mmap call is not gated. Rollback is the way to get a SIGBUS.
 */
#define _GNU_SOURCE
#include <dirent.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <unistd.h>

#define MAX_FDS 65536
#define MAX_ROOTS 64

enum fault { F_NONE, F_HANG, F_EIO, F_ESTALE, F_ETIMEDOUT, F_ENOSPC, F_EIODROP };

static struct {
    char path[PATH_MAX];
    size_t len;
    enum fault fault;
    int slow_ms;
    double rate_mb_s;     /* MB/s throughput cap for read/write (0 = unlimited) */
    double rate_bps;      /* bytes/s (rate_mb_s * 1024 * 1024) */
    int64_t rate_next_ns; /* monotonic ns when next byte slot is available */
    int fill_pct;         /* statvfs/fstatvfs used % override (0..100, -1 = real) */
    char snap[PATH_MAX];
    unsigned long hung; /* threads inside a hang right now */
} roots[MAX_ROOTS];
static int nroots;
static pthread_mutex_t mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cv = PTHREAD_COND_INITIALIZER;
static signed char fdroot[MAX_FDS]; /* 0: not ours, else index + 1 */
static bool fdpoison[MAX_FDS];      /* eiodrop: this descriptor's failure was reported */
/* The file a descriptor was tracked for (round 14): a descriptor closed by a call the shim does not see (closedir of a
 * dup'ed directory descriptor) leaves its entry, and the next socket or pipe of that number would be failed or held as
 * a file of the dead volume. root_of_fd checks it still is that file. */
static dev_t fddev[MAX_FDS];
static ino_t fdino[MAX_FDS];
static __thread bool bypass;        /* the control thread's own file work */
static pid_t parent0;               /* getppid() when the shim loaded: the harness */
static char ctl_path[PATH_MAX];     /* the control socket's file, "" without one */
static int hold_max_ms = 900 * 1000;
static volatile bool abandoned;     /* the harness is gone: a hang is over, as an error */

/* The real functions, found on first use: a call can come before the constructor (the dynamic loader's own). */
#define REAL(ret, name, args) static ret(*real_##name) args
REAL(int, open, (const char *, int, ...));
REAL(int, openat, (int, const char *, int, ...));
REAL(int, creat, (const char *, mode_t));
REAL(ssize_t, read, (int, void *, size_t));
REAL(ssize_t, pread, (int, void *, size_t, off_t));
REAL(ssize_t, readv, (int, const struct iovec *, int));
REAL(ssize_t, preadv, (int, const struct iovec *, int, off_t));
REAL(ssize_t, write, (int, const void *, size_t));
REAL(ssize_t, pwrite, (int, const void *, size_t, off_t));
REAL(ssize_t, writev, (int, const struct iovec *, int));
REAL(ssize_t, pwritev, (int, const struct iovec *, int, off_t));
REAL(int, fsync, (int));
REAL(int, fdatasync, (int));
REAL(int, syncfs, (int));
REAL(int, ftruncate, (int, off_t));
REAL(int, close, (int));
REAL(int, dup, (int));
REAL(int, dup2, (int, int));
REAL(int, dup3, (int, int, int));
REAL(int, fcntl, (int, int, ...));
REAL(int, flock, (int, int));
REAL(int, rename, (const char *, const char *));
REAL(int, renameat, (int, const char *, int, const char *));
REAL(int, unlink, (const char *));
REAL(int, unlinkat, (int, const char *, int));
REAL(int, mkdir, (const char *, mode_t));
REAL(int, mkdirat, (int, const char *, mode_t));
REAL(int, fstat, (int, struct stat *));
REAL(int, stat, (const char *, struct stat *));
REAL(int, lstat, (const char *, struct stat *));
REAL(int, fstatat, (int, const char *, struct stat *, int));
REAL(int, statvfs, (const char *, struct statvfs *));
REAL(int, fstatvfs, (int, struct statvfs *));
REAL(struct dirent *, readdir, (DIR *));
REAL(int, fallocate, (int, int, off_t, off_t));
REAL(int, posix_fallocate, (int, off_t, off_t));

static void resolve(void)
{
    static bool done;
    if (done)
        return;
#define LOAD(name) real_##name = dlsym(RTLD_NEXT, #name)
    LOAD(open);
    LOAD(openat);
    LOAD(creat);
    LOAD(read);
    LOAD(pread);
    LOAD(readv);
    LOAD(preadv);
    LOAD(write);
    LOAD(pwrite);
    LOAD(writev);
    LOAD(pwritev);
    LOAD(fsync);
    LOAD(fdatasync);
    LOAD(syncfs);
    LOAD(ftruncate);
    LOAD(close);
    LOAD(dup);
    LOAD(dup2);
    LOAD(dup3);
    LOAD(fcntl);
    LOAD(flock);
    LOAD(rename);
    LOAD(renameat);
    LOAD(unlink);
    LOAD(unlinkat);
    LOAD(mkdir);
    LOAD(mkdirat);
    LOAD(fstat);
    LOAD(stat);
    LOAD(lstat);
    LOAD(fstatat);
    LOAD(statvfs);
    LOAD(fstatvfs);
    LOAD(readdir);
    LOAD(fallocate);
    LOAD(posix_fallocate);
    done = true;
}

/* ------------------------------------------------------------- the fault */

/* The longest matching prefix (index + 1) of an absolute path, 0 if none. A prefix matches whole path components. */
static int root_of_path(const char *p)
{
    if (!p || p[0] != '/')
        return 0;
    int best = 0;
    size_t best_len = 0;
    for (int i = 0; i < nroots; i++)
        if (!strncmp(p, roots[i].path, roots[i].len) && (p[roots[i].len] == '/' || p[roots[i].len] == 0) &&
            roots[i].len >= best_len) {
            best = i + 1;
            best_len = roots[i].len;
        }
    return best;
}

static int root_of_fd(int fd)
{
    if (fd < 0 || fd >= MAX_FDS || !fdroot[fd])
        return 0;
    struct stat st;
    if (real_fstat(fd, &st) == 0 && st.st_dev == fddev[fd] && st.st_ino == fdino[fd])
        return fdroot[fd];
    fdroot[fd] = 0; /* closed behind the shim's back */
    return 0;
}

static int root_of_at(int dirfd, const char *name)
{
    if (!name)
        return 0;
    if (name[0] == '/')
        return root_of_path(name);
    return dirfd == AT_FDCWD ? 0 : root_of_fd(dirfd);
}

/* Applies the fault of root r (index + 1) to a call that adds data (grows) or only reads. 0: go on, else -errno. */
static int gate(int r, bool grows)
{
    if (!r || bypass)
        return 0;
    int i = r - 1, rc = 0, slow;
    pthread_mutex_lock(&mu);
    int waited_ms = 0;
    while (roots[i].fault == F_HANG && !abandoned) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += 100 * 1000 * 1000;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000L;
        }
        roots[i].hung++;
        int w = pthread_cond_timedwait(&cv, &mu, &ts);
        roots[i].hung--;
        if (w == ETIMEDOUT) {
            waited_ms += 100;
            struct stat st;
            if ((parent0 > 1 && getppid() != parent0) || waited_ms >= hold_max_ms ||
                (ctl_path[0] && real_stat(ctl_path, &st) != 0 && errno == ENOENT))
                abandoned = true;
        }
    }
    switch (roots[i].fault) {
    case F_HANG: rc = -EIO; break; /* abandoned */
    case F_EIO: rc = -EIO; break;
    case F_ESTALE: rc = -ESTALE; break;
    case F_ETIMEDOUT: rc = -ETIMEDOUT; break;
    case F_ENOSPC: rc = grows ? -ENOSPC : 0; break;
    default: break;
    }
    slow = roots[i].slow_ms;
    pthread_mutex_unlock(&mu);
    if (slow > 0)
        usleep((useconds_t)slow * 1000);
    return rc;
}

/* Per-root read/write throughput cap (`rate MB/s`): reserves byte time under mu, sleeps outside mu in short slices
 * so heal / `rate 0` or abandonment wakes promptly. */
static void rate_gate(int r, size_t nbytes)
{
    if (!r || bypass || nbytes == 0)
        return;
    int i = r - 1;
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    int64_t now = (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
    pthread_mutex_lock(&mu);
    double bps = roots[i].rate_bps;
    if (bps <= 0.0) {
        pthread_mutex_unlock(&mu);
        return;
    }
    double cost_d = (double)nbytes * 1e9 / bps;
    int64_t cost_ns = cost_d > 6e10 ? 60000000000LL : cost_d < 1.0 ? 1 : (int64_t)cost_d;
    int64_t base = roots[i].rate_next_ns;
    if (base < now)
        base = now;
    int64_t done = base + cost_ns;
    roots[i].rate_next_ns = done;
    pthread_mutex_unlock(&mu);
    int64_t wait_ns = done - now;
    while (wait_ns > 0 && !abandoned) {
        int64_t chunk = wait_ns > 50000000LL ? 50000000LL : wait_ns;
        struct timespec sl = {.tv_sec = chunk / 1000000000LL, .tv_nsec = chunk % 1000000000LL};
        nanosleep(&sl, NULL);
        if (parent0 > 1 && getppid() != parent0) {
            abandoned = true;
            break;
        }
        pthread_mutex_lock(&mu);
        bool off = roots[i].rate_bps <= 0.0;
        pthread_mutex_unlock(&mu);
        if (off)
            break;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        now = (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
        wait_ns = done - now;
    }
}

/* Applies `fill PCT` override to a statvfs result when configured on root `r`. */
static void apply_fill(int r, struct statvfs *sv)
{
    if (!r || bypass || !sv)
        return;
    pthread_mutex_lock(&mu);
    int pct = roots[r - 1].fill_pct;
    pthread_mutex_unlock(&mu);
    if (pct < 0)
        return;
    if (pct > 100)
        pct = 100;
    if (sv->f_blocks == 0)
        sv->f_blocks = 1000000;
    if (sv->f_frsize == 0)
        sv->f_frsize = 4096;
    if (sv->f_bsize == 0)
        sv->f_bsize = sv->f_frsize;
    uint64_t total = (uint64_t)sv->f_blocks;
    /* floor((total - avail) * 100 / total) == pct without uint64 overflow. */
    uint64_t used = (total / 100u) * (uint64_t)pct + ((total % 100u) * (uint64_t)pct + 99u) / 100u;
    if (used > total)
        used = total;
    fsblkcnt_t avail = (fsblkcnt_t)(total - used);
    sv->f_bavail = avail;
    sv->f_bfree = avail;
}

/* eiodrop: is the root's fault one that throws writes away? */
static bool dropping(int r)
{
    if (!r || bypass)
        return false;
    pthread_mutex_lock(&mu);
    bool d = roots[r - 1].fault == F_EIODROP;
    pthread_mutex_unlock(&mu);
    return d;
}

/* fsync of a descriptor: under eiodrop the first one fails, the rest succeed. 0: go on to the real call. */
static int sync_gate(int fd)
{
    int r = root_of_fd(fd), g = gate(r, false);
    if (g)
        return g;
    if (dropping(r) && fd >= 0 && fd < MAX_FDS) {
        if (!fdpoison[fd]) {
            fdpoison[fd] = true;
            return -EIO;
        }
        return 1; /* reported once: success without syncing anything */
    }
    return 0;
}

#define GATE(r, grows)           \
    do {                         \
        int g_ = gate(r, grows); \
        if (g_) {                \
            errno = -g_;         \
            return -1;           \
        }                        \
    } while (0)

/* --------------------------------------------------------------- opens */

static void track(int fd, int r)
{
    if (fd >= 0 && fd < MAX_FDS) {
        struct stat st;
        if (r && real_fstat(fd, &st) == 0) {
            fddev[fd] = st.st_dev;
            fdino[fd] = st.st_ino;
        } else {
            r = 0;
        }
        fdroot[fd] = (signed char)r;
        fdpoison[fd] = false;
    }
}

static int do_open(int dirfd, const char *name, int flags, mode_t mode, bool at)
{
    resolve();
    int r = at ? root_of_at(dirfd, name) : root_of_path(name);
    GATE(r, (flags & (O_CREAT | O_TRUNC)) != 0);
    int fd = at ? real_openat(dirfd, name, flags, mode) : real_open(name, flags, mode);
    if (fd >= 0)
        track(fd, r);
    return fd;
}

#define OPEN_MODE(flags, mode)                            \
    mode_t mode = 0;                                      \
    if ((flags) & (O_CREAT | O_TMPFILE)) {                \
        va_list ap_;                                      \
        va_start(ap_, flags);                             \
        mode = va_arg(ap_, mode_t);                       \
        va_end(ap_);                                      \
    }

int open(const char *name, int flags, ...)
{
    OPEN_MODE(flags, mode)
    return do_open(AT_FDCWD, name, flags, mode, false);
}
int open64(const char *name, int flags, ...)
{
    OPEN_MODE(flags, mode)
    return do_open(AT_FDCWD, name, flags, mode, false);
}
int openat(int dirfd, const char *name, int flags, ...)
{
    OPEN_MODE(flags, mode)
    return do_open(dirfd, name, flags, mode, true);
}
int openat64(int dirfd, const char *name, int flags, ...)
{
    OPEN_MODE(flags, mode)
    return do_open(dirfd, name, flags, mode, true);
}
/* the fortified forms (open without a mode argument) */
int __open_2(const char *name, int flags) { return do_open(AT_FDCWD, name, flags, 0, false); }
int __open64_2(const char *name, int flags) { return do_open(AT_FDCWD, name, flags, 0, false); }
int __openat_2(int dirfd, const char *name, int flags) { return do_open(dirfd, name, flags, 0, true); }
int __openat64_2(int dirfd, const char *name, int flags) { return do_open(dirfd, name, flags, 0, true); }
int creat(const char *name, mode_t mode) { return do_open(AT_FDCWD, name, O_CREAT | O_WRONLY | O_TRUNC, mode, false); }
int creat64(const char *name, mode_t mode) { return creat(name, mode); }

int close(int fd)
{
    resolve();
    if (fd >= 0 && fd < MAX_FDS)
        fdroot[fd] = 0;
    return real_close(fd);
}

int dup(int fd)
{
    resolve();
    int n = real_dup(fd);
    if (n >= 0)
        track(n, root_of_fd(fd));
    return n;
}
int dup2(int fd, int to)
{
    resolve();
    int n = real_dup2(fd, to);
    if (n >= 0)
        track(n, root_of_fd(fd));
    return n;
}
int dup3(int fd, int to, int flags)
{
    resolve();
    int n = real_dup3(fd, to, flags);
    if (n >= 0)
        track(n, root_of_fd(fd));
    return n;
}

/* fcntl takes one pointer-sized argument at most for every command tsdb uses (F_OFD_SETLK's struct flock *). */
int fcntl(int fd, int cmd, ...)
{
    resolve();
    va_list ap;
    va_start(ap, cmd);
    void *arg = va_arg(ap, void *);
    va_end(ap);
    bool lock_cmd = cmd == F_SETLK || cmd == F_SETLKW || cmd == F_GETLK || cmd == F_OFD_SETLK ||
                    cmd == F_OFD_SETLKW || cmd == F_OFD_GETLK;
    if (lock_cmd)
        GATE(root_of_fd(fd), true);
    int rc = real_fcntl(fd, cmd, arg);
    if (rc >= 0 && (cmd == F_DUPFD || cmd == F_DUPFD_CLOEXEC))
        track(rc, root_of_fd(fd));
    return rc;
}
int fcntl64(int fd, int cmd, ...)
{
    va_list ap;
    va_start(ap, cmd);
    void *arg = va_arg(ap, void *);
    va_end(ap);
    return fcntl(fd, cmd, arg);
}

int flock(int fd, int op)
{
    resolve();
    GATE(root_of_fd(fd), true);
    return real_flock(fd, op);
}

/* ---------------------------------------------------------------- data */

ssize_t read(int fd, void *buf, size_t n)
{
    resolve();
    int r = root_of_fd(fd);
    GATE(r, false);
    ssize_t got = real_read(fd, buf, n);
    if (got > 0)
        rate_gate(r, (size_t)got);
    return got;
}
ssize_t pread(int fd, void *buf, size_t n, off_t off)
{
    resolve();
    int r = root_of_fd(fd);
    GATE(r, false);
    ssize_t got = real_pread(fd, buf, n, off);
    if (got > 0)
        rate_gate(r, (size_t)got);
    return got;
}
ssize_t pread64(int fd, void *buf, size_t n, off_t off) { return pread(fd, buf, n, off); }
/* glibc's fortified headers (Ubuntu builds at -O1 and up) turn read and pread into these when the buffer's size is
 * known: without them such calls would bypass the gate. The check itself is the real function's business. */
ssize_t __read_chk(int fd, void *buf, size_t n, size_t buflen)
{
    if (n > buflen)
        abort();
    return read(fd, buf, n);
}
ssize_t __pread_chk(int fd, void *buf, size_t n, off_t off, size_t buflen)
{
    if (n > buflen)
        abort();
    return pread(fd, buf, n, off);
}
ssize_t __pread64_chk(int fd, void *buf, size_t n, off_t off, size_t buflen)
{
    if (n > buflen)
        abort();
    return pread(fd, buf, n, off);
}
ssize_t write(int fd, const void *buf, size_t n)
{
    resolve();
    int r = root_of_fd(fd);
    GATE(r, true);
    if (dropping(r)) {
        rate_gate(r, n);
        return (ssize_t)n;
    }
    ssize_t put = real_write(fd, buf, n);
    if (put > 0)
        rate_gate(r, (size_t)put);
    return put;
}
ssize_t pwrite(int fd, const void *buf, size_t n, off_t off)
{
    resolve();
    int r = root_of_fd(fd);
    GATE(r, true);
    if (dropping(r)) {
        rate_gate(r, n);
        return (ssize_t)n;
    }
    ssize_t put = real_pwrite(fd, buf, n, off);
    if (put > 0)
        rate_gate(r, (size_t)put);
    return put;
}
ssize_t pwrite64(int fd, const void *buf, size_t n, off_t off) { return pwrite(fd, buf, n, off); }
static size_t iov_total(const struct iovec *iov, int iovcnt)
{
    size_t tot = 0;
    for (int i = 0; i < iovcnt; i++)
        tot += iov[i].iov_len;
    return tot;
}
ssize_t readv(int fd, const struct iovec *iov, int iovcnt)
{
    resolve();
    int r = root_of_fd(fd);
    GATE(r, false);
    ssize_t got = real_readv(fd, iov, iovcnt);
    if (got > 0)
        rate_gate(r, (size_t)got);
    return got;
}
ssize_t preadv(int fd, const struct iovec *iov, int iovcnt, off_t off)
{
    resolve();
    int r = root_of_fd(fd);
    GATE(r, false);
    ssize_t got = real_preadv(fd, iov, iovcnt, off);
    if (got > 0)
        rate_gate(r, (size_t)got);
    return got;
}
ssize_t preadv64(int fd, const struct iovec *iov, int iovcnt, off_t off) { return preadv(fd, iov, iovcnt, off); }
ssize_t writev(int fd, const struct iovec *iov, int iovcnt)
{
    resolve();
    int r = root_of_fd(fd);
    GATE(r, true);
    if (dropping(r)) {
        size_t tot = iov_total(iov, iovcnt);
        rate_gate(r, tot);
        return (ssize_t)tot;
    }
    ssize_t put = real_writev(fd, iov, iovcnt);
    if (put > 0)
        rate_gate(r, (size_t)put);
    return put;
}
ssize_t pwritev(int fd, const struct iovec *iov, int iovcnt, off_t off)
{
    resolve();
    int r = root_of_fd(fd);
    GATE(r, true);
    if (dropping(r)) {
        size_t tot = iov_total(iov, iovcnt);
        rate_gate(r, tot);
        return (ssize_t)tot;
    }
    ssize_t put = real_pwritev(fd, iov, iovcnt, off);
    if (put > 0)
        rate_gate(r, (size_t)put);
    return put;
}
ssize_t pwritev64(int fd, const struct iovec *iov, int iovcnt, off_t off) { return pwritev(fd, iov, iovcnt, off); }
/* fsync of data already written does not need space: it passes under enospc */
int fsync(int fd)
{
    resolve();
    int g = sync_gate(fd);
    if (g) {
        if (g < 0) {
            errno = -g;
            return -1;
        }
        return 0;
    }
    return real_fsync(fd);
}
int fdatasync(int fd)
{
    resolve();
    int g = sync_gate(fd);
    if (g) {
        if (g < 0) {
            errno = -g;
            return -1;
        }
        return 0;
    }
    return real_fdatasync(fd);
}
int syncfs(int fd)
{
    resolve();
    GATE(root_of_fd(fd), false);
    return real_syncfs(fd);
}
int ftruncate(int fd, off_t len)
{
    resolve();
    GATE(root_of_fd(fd), true);
    if (dropping(root_of_fd(fd)))
        return 0;
    return real_ftruncate(fd, len);
}
int ftruncate64(int fd, off_t len) { return ftruncate(fd, len); }
int fallocate(int fd, int mode, off_t off, off_t len)
{
    resolve();
    GATE(root_of_fd(fd), true);
    return real_fallocate(fd, mode, off, len);
}
int posix_fallocate(int fd, off_t off, off_t len)
{
    resolve();
    int g = gate(root_of_fd(fd), true);
    return g ? -g : real_posix_fallocate(fd, off, len);
}

/* ------------------------------------------------------------ namespace */

int rename(const char *a, const char *b)
{
    resolve();
    GATE(root_of_path(a), true);
    GATE(root_of_path(b), true);
    return real_rename(a, b);
}
int renameat(int of, const char *a, int nf, const char *b)
{
    resolve();
    GATE(root_of_at(of, a), true);
    GATE(root_of_at(nf, b), true);
    return real_renameat(of, a, nf, b);
}
int unlink(const char *p)
{
    resolve();
    GATE(root_of_path(p), false);
    return real_unlink(p);
}
int unlinkat(int d, const char *p, int flags)
{
    resolve();
    GATE(root_of_at(d, p), false);
    return real_unlinkat(d, p, flags);
}
int mkdir(const char *p, mode_t m)
{
    resolve();
    GATE(root_of_path(p), true);
    return real_mkdir(p, m);
}
int mkdirat(int d, const char *p, mode_t m)
{
    resolve();
    GATE(root_of_at(d, p), true);
    return real_mkdirat(d, p, m);
}
int fstat(int fd, struct stat *st)
{
    resolve();
    GATE(root_of_fd(fd), false);
    return real_fstat(fd, st);
}
int fstat64(int fd, struct stat64 *st) { return fstat(fd, (struct stat *)st); }
int stat(const char *p, struct stat *st)
{
    resolve();
    GATE(root_of_path(p), false);
    return real_stat(p, st);
}
int stat64(const char *p, struct stat64 *st) { return stat(p, (struct stat *)st); }
int lstat(const char *p, struct stat *st)
{
    resolve();
    GATE(root_of_path(p), false);
    return real_lstat(p, st);
}
int lstat64(const char *p, struct stat64 *st) { return lstat(p, (struct stat *)st); }
int fstatat(int d, const char *p, struct stat *st, int flags)
{
    resolve();
    GATE(root_of_at(d, p), false);
    return real_fstatat(d, p, st, flags);
}
int fstatat64(int d, const char *p, struct stat64 *st, int flags)
{
    return fstatat(d, p, (struct stat *)st, flags);
}
int statvfs(const char *p, struct statvfs *sv)
{
    resolve();
    int r = root_of_path(p);
    GATE(r, false);
    int rc = real_statvfs(p, sv);
    if (rc == 0)
        apply_fill(r, sv);
    return rc;
}
int statvfs64(const char *p, struct statvfs64 *sv) { return statvfs(p, (struct statvfs *)sv); }
int fstatvfs(int fd, struct statvfs *sv)
{
    resolve();
    int r = root_of_fd(fd);
    GATE(r, false);
    int rc = real_fstatvfs(fd, sv);
    if (rc == 0)
        apply_fill(r, sv);
    return rc;
}
int fstatvfs64(int fd, struct statvfs64 *sv) { return fstatvfs(fd, (struct statvfs *)sv); }

/* getdents is not a symbol glibc's readdir goes through; readdir itself is. A failure is end of directory with
 * errno set, which is what tsdb's scans must not take for a complete listing. */
struct dirent *readdir(DIR *d)
{
    resolve();
    int g = gate(root_of_fd(dirfd(d)), false);
    if (g) {
        errno = -g;
        return NULL;
    }
    return real_readdir(d);
}
struct dirent64 *readdir64(DIR *d) { return (struct dirent64 *)readdir(d); }

/* --------------------------------------------------------------- control */

/* The file work of snap and rollback runs on the control thread with `bypass` set: no fault applies to it, so a
 * rollback of a hung prefix works. Plain libc below; none of it comes back through the wrappers above. */
static int copy_file(const char *from, const char *to, bool inplace)
{
    int in = real_open(from, O_RDONLY | O_CLOEXEC);
    if (in < 0)
        return -1;
    int out = real_open(to, O_WRONLY | O_CREAT | (inplace ? 0 : O_TRUNC) | O_CLOEXEC, 0644);
    if (out < 0) {
        real_close(in);
        return -1;
    }
    char buf[65536];
    ssize_t n;
    int rc = 0;
    off_t at = 0;
    while ((n = real_read(in, buf, sizeof buf)) > 0) {
        if (real_pwrite(out, buf, (size_t)n, at) != n) {
            rc = -1;
            break;
        }
        at += n;
    }
    if (n < 0)
        rc = -1;
    /* in place: same inode, the length of the snapshot's file (shrinks a file that grew since) */
    if (rc == 0 && real_ftruncate(out, at) != 0)
        rc = -1;
    real_close(in);
    real_close(out);
    return rc;
}

static void rm_tree(const char *p)
{
    DIR *d = opendir(p);
    struct dirent *e;
    while (d && (e = real_readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, ".."))
            continue;
        char q[PATH_MAX];
        snprintf(q, sizeof q, "%s/%s", p, e->d_name);
        struct stat st;
        if (real_lstat(q, &st) == 0 && S_ISDIR(st.st_mode))
            rm_tree(q);
        else
            real_unlink(q);
    }
    if (d)
        closedir(d);
    rmdir(p);
}

/* Makes `to` hold exactly what `from` holds; the files `to` already has keep their inodes. */
static int sync_tree(const char *from, const char *to)
{
    DIR *d = opendir(from);
    if (!d)
        return -1;
    real_mkdir(to, 0755);
    int rc = 0;
    struct dirent *e;
    while ((e = real_readdir(d)) && rc == 0) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, ".."))
            continue;
        char a[PATH_MAX], b[PATH_MAX];
        snprintf(a, sizeof a, "%s/%s", from, e->d_name);
        snprintf(b, sizeof b, "%s/%s", to, e->d_name);
        struct stat st;
        if (real_lstat(a, &st) != 0)
            continue;
        if (S_ISDIR(st.st_mode))
            rc = sync_tree(a, b);
        else if (S_ISREG(st.st_mode))
            rc = copy_file(a, b, true);
    }
    closedir(d);
    /* remove what the snapshot lacks */
    d = opendir(to);
    if (!d)
        return -1;
    while ((e = real_readdir(d))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, ".."))
            continue;
        char a[PATH_MAX], b[PATH_MAX];
        snprintf(a, sizeof a, "%s/%s", from, e->d_name);
        snprintf(b, sizeof b, "%s/%s", to, e->d_name);
        struct stat st, sb;
        if (real_lstat(a, &st) == 0 || real_lstat(b, &sb) != 0)
            continue;
        if (S_ISDIR(sb.st_mode))
            rm_tree(b);
        else
            real_unlink(b);
    }
    closedir(d);
    return rc;
}

static const char *fault_name(enum fault f)
{
    static const char *n[] = {"none", "hang", "eio", "estale", "etimedout", "enospc", "eiodrop"};
    return n[f];
}

static void reply(int c, const char *fmt, ...)
{
    char buf[4096];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf, sizeof buf, fmt, ap);
    va_end(ap);
    if (n > 0)
        (void)real_write(c, buf, (size_t)(n < (int)sizeof buf ? n : (int)sizeof buf - 1));
}

static void control_line(int c, char *line)
{
    char *cmd = strtok(line, " \t\r\n"), *pfx = strtok(NULL, " \t\r\n"), *arg = strtok(NULL, " \t\r\n");
    if (!cmd) {
        reply(c, "err empty\n");
        return;
    }
    if (!strcmp(cmd, "state")) {
        pthread_mutex_lock(&mu);
        for (int i = 0; i < nroots; i++)
            reply(c, "%s %s slow=%d rate=%.2f fill=%d hung=%lu\n", roots[i].path, fault_name(roots[i].fault),
                  roots[i].slow_ms, roots[i].rate_mb_s, roots[i].fill_pct, roots[i].hung);
        pthread_mutex_unlock(&mu);
        reply(c, "ok\n");
        return;
    }
    int r = pfx ? root_of_path(pfx) : 0;
    if (!r || roots[r - 1].len != strlen(pfx)) {
        reply(c, "err unknown prefix\n");
        return;
    }
    int i = r - 1;
    enum fault f = F_NONE;
    bool is_fault = true;
    if (!strcmp(cmd, "hang"))
        f = F_HANG;
    else if (!strcmp(cmd, "eio"))
        f = F_EIO;
    else if (!strcmp(cmd, "estale"))
        f = F_ESTALE;
    else if (!strcmp(cmd, "etimedout"))
        f = F_ETIMEDOUT;
    else if (!strcmp(cmd, "enospc"))
        f = F_ENOSPC;
    else if (!strcmp(cmd, "eiodrop"))
        f = F_EIODROP;
    else if (!strcmp(cmd, "heal"))
        f = F_NONE;
    else
        is_fault = false;
    if (is_fault) {
        pthread_mutex_lock(&mu);
        roots[i].fault = f;
        if (f == F_NONE) {
            roots[i].slow_ms = 0;
            roots[i].rate_mb_s = 0.0;
            roots[i].rate_bps = 0.0;
            roots[i].rate_next_ns = 0;
            roots[i].fill_pct = -1;
        }
        pthread_cond_broadcast(&cv);
        pthread_mutex_unlock(&mu);
        reply(c, "ok\n");
    } else if (!strcmp(cmd, "slow") && arg) {
        pthread_mutex_lock(&mu);
        roots[i].slow_ms = atoi(arg);
        pthread_mutex_unlock(&mu);
        reply(c, "ok\n");
    } else if (!strcmp(cmd, "rate") && arg) {
        char *ep = NULL;
        errno = 0;
        double mb = strtod(arg, &ep);
        if (errno || !ep || ep == arg || *ep != '\0' || !(mb >= 0.0) || mb > 1e6) {
            reply(c, "err bad rate\n");
            return;
        }
        pthread_mutex_lock(&mu);
        roots[i].rate_mb_s = mb;
        roots[i].rate_bps = mb > 0.0 ? mb * (1024.0 * 1024.0) : 0.0;
        roots[i].rate_next_ns = 0;
        pthread_mutex_unlock(&mu);
        reply(c, "ok\n");
    } else if (!strcmp(cmd, "fill") && arg) {
        int pct = -1;
        if (strcmp(arg, "off") != 0) {
            char *ep = NULL;
            errno = 0;
            long v = strtol(arg, &ep, 10);
            if (errno || !ep || ep == arg || *ep != '\0' || v < -1 || v > 100) {
                reply(c, "err bad fill\n");
                return;
            }
            pct = (int)v;
        }
        pthread_mutex_lock(&mu);
        roots[i].fill_pct = pct;
        pthread_mutex_unlock(&mu);
        reply(c, "ok\n");
    } else if (!strcmp(cmd, "snap") || !strcmp(cmd, "rollback")) {
        bypass = true;
        int rc;
        if (!strcmp(cmd, "snap")) {
            snprintf(roots[i].snap, sizeof roots[i].snap, "%s.snap%d", getenv("OUTAGEFS_CTL"), i);
            /* a fresh snapshot: sync_tree makes it exactly the prefix's tree */
            rc = sync_tree(roots[i].path, roots[i].snap);
        } else if (!roots[i].snap[0]) {
            reply(c, "err no snapshot\n");
            bypass = false;
            return;
        } else {
            rc = sync_tree(roots[i].snap, roots[i].path);
        }
        bypass = false;
        reply(c, rc == 0 ? "ok\n" : "err %s\n", strerror(errno));
    } else {
        reply(c, "err unknown command\n");
    }
}

static void *control_main(void *arg)
{
    int ls = (int)(intptr_t)arg;
    bypass = true;
    for (;;) {
        int c = accept(ls, NULL, NULL);
        if (c < 0)
            continue;
        char line[512];
        size_t n = 0;
        ssize_t r;
        while (n < sizeof line - 1 && (r = real_read(c, line + n, 1)) == 1 && line[n] != '\n')
            n++;
        line[n] = 0;
        control_line(c, line);
        real_close(c);
    }
    return NULL;
}

static void new_parent(void) { parent0 = getppid(); }

__attribute__((constructor)) static void init(void)
{
    resolve();
    parent0 = getppid();
    pthread_atfork(NULL, NULL, new_parent); /* a forked child's harness is the process that forked it */
    const char *rs = getenv("OUTAGEFS_ROOTS"), *ctl = getenv("OUTAGEFS_CTL");
    if (!rs || !ctl)
        return;
    if (getenv("OUTAGEFS_HOLD_MAX_S"))
        hold_max_ms = atoi(getenv("OUTAGEFS_HOLD_MAX_S")) * 1000;
    snprintf(ctl_path, sizeof ctl_path, "%s", ctl);
    for (int i = 0; i < MAX_ROOTS; i++)
        roots[i].fill_pct = -1;
    char *copy = strdup(rs);
    for (char *p = strtok(copy, ":"); p && nroots < MAX_ROOTS; p = strtok(NULL, ":")) {
        if (!realpath(p, roots[nroots].path))
            snprintf(roots[nroots].path, sizeof roots[nroots].path, "%s", p);
        roots[nroots].len = strlen(roots[nroots].path);
        nroots++;
    }
    free(copy);
    struct sockaddr_un sa = {.sun_family = AF_UNIX};
    if (strlen(ctl) >= sizeof sa.sun_path)
        return;
    strcpy(sa.sun_path, ctl);
    real_unlink(ctl);
    int ls = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (ls < 0 || bind(ls, (struct sockaddr *)&sa, sizeof sa) != 0 || listen(ls, 8) != 0)
        return;
    pthread_t t;
    pthread_attr_t at;
    pthread_attr_init(&at);
    pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED);
    pthread_create(&t, &at, control_main, (void *)(intptr_t)ls);
}
