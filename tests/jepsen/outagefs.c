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
 *   enospc      calls that add data or names fail with ENOSPC; reads and fsync of what exists work (a full pool).
 *   slow MS     every call takes MS milliseconds more.
 *   heal        no fault.
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
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#define MAX_FDS 65536
#define MAX_ROOTS 16

enum fault { F_NONE, F_HANG, F_EIO, F_ESTALE, F_ETIMEDOUT, F_ENOSPC };

static struct {
    char path[PATH_MAX];
    size_t len;
    enum fault fault;
    int slow_ms;
    char snap[PATH_MAX];
    unsigned long hung; /* threads inside a hang right now */
} roots[MAX_ROOTS];
static int nroots;
static pthread_mutex_t mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cv = PTHREAD_COND_INITIALIZER;
static signed char fdroot[MAX_FDS]; /* 0: not ours, else index + 1 */
static __thread bool bypass;        /* the control thread's own file work */

/* The real functions, found on first use: a call can come before the constructor (the dynamic loader's own). */
#define REAL(ret, name, args) static ret(*real_##name) args
REAL(int, open, (const char *, int, ...));
REAL(int, openat, (int, const char *, int, ...));
REAL(int, creat, (const char *, mode_t));
REAL(ssize_t, read, (int, void *, size_t));
REAL(ssize_t, pread, (int, void *, size_t, off_t));
REAL(ssize_t, write, (int, const void *, size_t));
REAL(ssize_t, pwrite, (int, const void *, size_t, off_t));
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
    LOAD(write);
    LOAD(pwrite);
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
    LOAD(readdir);
    LOAD(fallocate);
    LOAD(posix_fallocate);
    done = true;
}

/* ------------------------------------------------------------- the fault */

/* The prefix (index + 1) of an absolute path, 0 if none. A prefix matches whole path components. */
static int root_of_path(const char *p)
{
    if (!p || p[0] != '/')
        return 0;
    for (int i = 0; i < nroots; i++)
        if (!strncmp(p, roots[i].path, roots[i].len) && (p[roots[i].len] == '/' || p[roots[i].len] == 0))
            return i + 1;
    return 0;
}

static int root_of_fd(int fd) { return fd >= 0 && fd < MAX_FDS ? fdroot[fd] : 0; }

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
    while (roots[i].fault == F_HANG) {
        roots[i].hung++;
        pthread_cond_wait(&cv, &mu);
        roots[i].hung--;
    }
    switch (roots[i].fault) {
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
    if (fd >= 0 && fd < MAX_FDS)
        fdroot[fd] = (signed char)r;
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
    GATE(root_of_fd(fd), false);
    return real_read(fd, buf, n);
}
ssize_t pread(int fd, void *buf, size_t n, off_t off)
{
    resolve();
    GATE(root_of_fd(fd), false);
    return real_pread(fd, buf, n, off);
}
ssize_t pread64(int fd, void *buf, size_t n, off_t off) { return pread(fd, buf, n, off); }
ssize_t write(int fd, const void *buf, size_t n)
{
    resolve();
    GATE(root_of_fd(fd), true);
    return real_write(fd, buf, n);
}
ssize_t pwrite(int fd, const void *buf, size_t n, off_t off)
{
    resolve();
    GATE(root_of_fd(fd), true);
    return real_pwrite(fd, buf, n, off);
}
ssize_t pwrite64(int fd, const void *buf, size_t n, off_t off) { return pwrite(fd, buf, n, off); }
/* fsync of data already written does not need space: it passes under enospc */
int fsync(int fd)
{
    resolve();
    GATE(root_of_fd(fd), false);
    return real_fsync(fd);
}
int fdatasync(int fd)
{
    resolve();
    GATE(root_of_fd(fd), false);
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
    static const char *n[] = {"none", "hang", "eio", "estale", "etimedout", "enospc"};
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
            reply(c, "%s %s slow=%d hung=%lu\n", roots[i].path, fault_name(roots[i].fault), roots[i].slow_ms,
                  roots[i].hung);
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
    else if (!strcmp(cmd, "heal"))
        f = F_NONE;
    else
        is_fault = false;
    if (is_fault) {
        pthread_mutex_lock(&mu);
        roots[i].fault = f;
        if (f == F_NONE)
            roots[i].slow_ms = 0;
        pthread_cond_broadcast(&cv);
        pthread_mutex_unlock(&mu);
        reply(c, "ok\n");
    } else if (!strcmp(cmd, "slow") && arg) {
        pthread_mutex_lock(&mu);
        roots[i].slow_ms = atoi(arg);
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

__attribute__((constructor)) static void init(void)
{
    resolve();
    const char *rs = getenv("OUTAGEFS_ROOTS"), *ctl = getenv("OUTAGEFS_CTL");
    if (!rs || !ctl)
        return;
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
