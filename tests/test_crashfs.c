/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * test_crashfs: the power-loss shim (tests/jepsen/crashfs.c) itself, for the calls it must track.
 *
 * writev and pwritev of a file under CRASHFS_ROOT are writes like write and pwrite: a power loss can happen at one,
 * one waits for the lock while a power loss rolls back, and a rewrite in place made with one is lost (or put back)
 * at the power loss. The Raft log appends with pwritev; when the shim passed it through, a node's append ran on while
 * another thread's power loss truncated the log, left a hole of zeros under a later record, read the hole back and
 * stopped with "entry N does not decode" instead of losing power (jepsen-cluster meta,classes power 4, epoll).
 *
 * A file made on the inode of a removed one is a new file: a tmp file that met an injected EIO (its syncs lie from then on)
 * and was unlinked durably frees its inode (the kept-aside copy goes at the directory's fsync); a new file given that inode
 * inherited the lie, its fsync never counted, and the power loss at exit cut it to 0 bytes (test_chunk's eio sweep: an
 * acknowledged chunk). `reuse` makes that file and requires its bytes after the power loss; `reuse-open`, `reuse-unseen`
 * and `reuse-close` each leave one of the shim's three belts alone in the way (crashfs.c, its head). On tmpfs (/tmp here)
 * no inode is reused: the cases then run under /var/tmp, and say SKIPPED where no inode comes back.
 *
 *   test_crashfs [scratch dir]        (needs build/libcrashfs.so beside the binary)
 */
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/wait.h>
#include <unistd.h>
#ifdef __linux__
#include <linux/magic.h>
#include <sys/syscall.h>
#include <sys/vfs.h>
#endif

static char SELF[PATH_MAX], SHIM[PATH_MAX + 32];
static int failures;

#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #c);                                      \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

/* The child's steps under the shim: 512 bytes of 'A' synced, then (by `how`) a rewrite of them with 'B' through
 * pwritev ("pwritev"), an append of 'B' through writev ("writev") or nothing ("none"). */
static int child_reuse(const char *file, const char *how);
static int child_rmdir(const char *dir, const char *how);
static int child_tear_link(const char *dir, const char *how);
static int child_rename_hl(const char *dir, const char *how);
static int child_unlink_append(const char *dir, const char *how);
static int child(const char *file, const char *how)
{
    if (!strncmp(how, "reuse", 5))
        return child_reuse(file, how);
    if (!strncmp(how, "rmdir", 5))
        return child_rmdir(file, how);
    if (!strncmp(how, "tear-link", 9))
        return child_tear_link(file, how);
    if (!strncmp(how, "rename-", 7))
        return child_rename_hl(file, how);
    if (!strncmp(how, "ul-", 3))
        return child_unlink_append(file, how);
    int fd = open(file, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0)
        return 3;
    char a[512], b[512];
    memset(a, 'A', sizeof a);
    memset(b, 'B', sizeof b);
    char d[PATH_MAX];
    snprintf(d, sizeof d, "%s", file);
    *strrchr(d, '/') = '\0';
    int dfd = open(d, O_RDONLY | O_DIRECTORY);
    if (dfd < 0 || pwrite(fd, a, sizeof a, 0) != (ssize_t)sizeof a || fsync(fd) != 0 || fsync(dfd) != 0)
        return 4; /* the name too is durable: only the rewrite below is not */
    struct iovec iov[2] = {{b, 200}, {b + 200, 312}};
    if (!strcmp(how, "pwritev") && pwritev(fd, iov, 2, 0) != 512)
        return 5;
    if (!strcmp(how, "writev") && (lseek(fd, 0, SEEK_END) != 512 || writev(fd, iov, 2) != 512))
        return 6;
    return 7; /* returns through exit(): CRASHFS_AT_EXIT loses the power there */
}

/* `file` (prefix "x": the only name CRASHFS_EIO_ONLY lets fail) meets the EIO and dies; then new files y<i> are made until
 * one has its inode, which is written and synced with its name (renamed to z first where its birth was unseen). By `how`,
 * one belt of the shim (tests/jepsen/crashfs.c, its head) or two stand between the dead file's record and the new one:
 *   reuse         x unlinked through the shim and its directory synced; y made through the shim's open (both belts)
 *   reuse-open    x removed by a raw unlink, unseen; y made through the shim's open (the renew on create alone)
 *   reuse-unseen  x unlinked durably; y made by a raw open, unseen, as mkstemp does (the drop at the unlink alone)
 *   reuse-close   x unlinked durably while open, then written (the EIO) and closed; y made by a raw open (the drop at
 *                 the close alone)
 * Prints the name of the file that must keep its 512 bytes. 7: done, 8: the file system gave none of 256 new files the
 * freed inode. */
#ifdef __linux__
static int child_reuse(const char *file, const char *how)
{
    char d[PATH_MAX];
    snprintf(d, sizeof d, "%s", file);
    *strrchr(d, '/') = '\0';
    bool raw_unlink = !strcmp(how, "reuse-open"), late = !strcmp(how, "reuse-close");
    bool unseen = !strcmp(how, "reuse-unseen") || late;
    int dfd = open(d, O_RDONLY | O_DIRECTORY);
    int fd = open(file, O_RDWR | O_CREAT | O_TRUNC, 0644);
    struct stat st;
    if (dfd < 0 || fd < 0 || fstat(fd, &st) != 0)
        return 3;
    char b[512];
    memset(b, 'B', sizeof b);
    if (late && (unlink(file) != 0 || fsync(dfd) != 0))
        return 5;
    if (pwrite(fd, b, sizeof b, 0) >= 0 || errno != EIO)
        return 4; /* the shim's EIO is due at the first write of x */
    close(fd);
    if (raw_unlink ? syscall(SYS_unlinkat, AT_FDCWD, file, 0) != 0 : !late && (unlink(file) != 0 || fsync(dfd) != 0))
        return 5;
    for (int i = 0; i < 256; i++) {
        char y[PATH_MAX + 16], z[PATH_MAX + 16];
        snprintf(y, sizeof y, "%s/y%d", d, i);
        snprintf(z, sizeof z, "%s/z", d);
        int yf = unseen ? (int)syscall(SYS_openat, AT_FDCWD, y, O_RDWR | O_CREAT | O_EXCL, 0644)
                        : open(y, O_RDWR | O_CREAT | O_EXCL, 0644);
        struct stat yst;
        if (yf < 0 || fstat(yf, &yst) != 0)
            return 6;
        if (yst.st_ino != st.st_ino) {
            close(yf);
            continue;
        }
        if (pwrite(yf, b, sizeof b, 0) != (ssize_t)sizeof b || fsync(yf) != 0)
            return 6;
        close(yf);
        if (unseen && rename(y, z) != 0)
            return 6;
        if (fsync(dfd) != 0)
            return 6;
        printf("%s\n", unseen ? z : y);
        fflush(stdout);
        return 7;
    }
    return 8;
}

#else
static int child_reuse(const char *file, const char *how)
{
    (void)file, (void)how;
    return 3;
}
#endif

static int make_synced_file(const char *path, int fill)
{
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0)
        return -1;
    char buf[512];
    memset(buf, fill, sizeof buf);
    if (pwrite(fd, buf, sizeof buf, 0) != (ssize_t)sizeof buf || fsync(fd) != 0) {
        close(fd);
        return -1;
    }
    close(fd);
    return 0;
}

static int sync_dir(const char *dir)
{
    int dfd = open(dir, O_RDONLY | O_DIRECTORY);
    if (dfd < 0)
        return -1;
    int rc = fsync(dfd);
    close(dfd);
    return rc;
}

static bool has_saved_artifact(const char *dir)
{
    DIR *d = opendir(dir);
    if (!d)
        return true;
    bool found = false;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        if (!strncmp(e->d_name, ".crashfs_saved_", 15)) {
            found = true;
            break;
        }
    }
    closedir(d);
    return found;
}

static int child_rmdir(const char *dir, const char *how)
{
    if (!strcmp(how, "rmdir-tear")) {
        char d1[PATH_MAX + 32], a[PATH_MAX + 48], out[PATH_MAX + 32];
        snprintf(d1, sizeof d1, "%s/d1", dir);
        snprintf(a, sizeof a, "%s/d1/a", dir);
        snprintf(out, sizeof out, "%s/out", dir);
        if (mkdir(d1, 0755) != 0 || make_synced_file(a, 'A') != 0)
            return 3;
        if (sync_dir(d1) != 0 || sync_dir(dir) != 0)
            return 4;
        if (rename(a, out) != 0 || rmdir(d1) != 0)
            return 5;
        return 7;
    }
    if (!strcmp(how, "rmdir-fork")) {
        char d1[PATH_MAX + 32];
        snprintf(d1, sizeof d1, "%s/d1", dir);
        if (mkdir(d1, 0755) != 0 || sync_dir(dir) != 0)
            return 3;
        if (rmdir(d1) != 0)
            return 4;
        pid_t p = fork();
        if (p < 0)
            return 5;
        if (p == 0)
            exit(0); /* normal exit runs fini() in forked child; must not delete parent's saved dir */
        int st = 0;
        if (waitpid(p, &st, 0) != p || !WIFEXITED(st) || WEXITSTATUS(st) != 0)
            return 6;
        raise(SIGUSR2);
        int fd = open(dir, O_RDONLY | O_DIRECTORY);
        if (fd >= 0)
            close(fd);
        return 7; /* unreachable: power_loss() exits with 99 */
    }
    if (!strcmp(how, "rmdir-sib")) {
        char sib[PATH_MAX + 32];
        snprintf(sib, sizeof sib, "%s2", dir);
        if (rmdir(sib) != 0)
            return 4;
        return 7;
    }
    char d1[PATH_MAX + 32], d2[PATH_MAX + 32], d3[PATH_MAX + 32], d3in[PATH_MAX + 48];
    char d4[PATH_MAX + 32], d5[PATH_MAX + 32], d5sl[PATH_MAX + 34], f[PATH_MAX + 32];
    snprintf(d1, sizeof d1, "%s/d1", dir);
    snprintf(d2, sizeof d2, "%s/d2", dir);
    snprintf(d3, sizeof d3, "%s/d3", dir);
    snprintf(d3in, sizeof d3in, "%s/d3/inside", dir);
    snprintf(d4, sizeof d4, "%s/d4", dir);
    snprintf(d5, sizeof d5, "%s/d5", dir);
    snprintf(d5sl, sizeof d5sl, "%s/d5/", dir);
    snprintf(f, sizeof f, "%s/f", dir);
    if (mkdir(d1, 0755) != 0 || mkdir(d2, 0755) != 0 || mkdir(d3, 0755) != 0 || mkdir(d3in, 0755) != 0 ||
        mkdir(d4, 0755) != 0 || mkdir(d5, 0755) != 0 || make_synced_file(f, 'A') != 0)
        return 3;
    if (sync_dir(d3) != 0 || sync_dir(dir) != 0)
        return 4;
    if (rmdir(d3) == 0 || (errno != ENOTEMPTY && errno != EEXIST))
        return 5;
    if (rmdir(f) == 0 || errno != ENOTDIR)
        return 6;
    if (rmdir(d4) != 0 || sync_dir(dir) != 0)
        return 8;
    int dfd = open(dir, O_RDONLY | O_DIRECTORY);
    if (dfd < 0)
        return 9;
    if (rmdir(d1) != 0 || unlinkat(dfd, "d2", AT_REMOVEDIR) != 0 || rmdir(d5sl) != 0) {
        close(dfd);
        return 10;
    }
    close(dfd);
    return 7;
}

static int child_tear_link(const char *dir, const char *how)
{
    if (!strcmp(how, "tear-link-dren")) {
        char d1[PATH_MAX + 32], d2[PATH_MAX + 32], a1[PATH_MAX + 48], l1[PATH_MAX + 48], a2[PATH_MAX + 48];
        snprintf(d1, sizeof d1, "%s/d1", dir);
        snprintf(d2, sizeof d2, "%s/d2", dir);
        snprintf(a1, sizeof a1, "%s/a", d1);
        snprintf(l1, sizeof l1, "%s/l", d1);
        snprintf(a2, sizeof a2, "%s/a", d2);
        if (mkdir(d1, 0755) != 0 || make_synced_file(a1, 'A') != 0)
            return 3;
        if (sync_dir(d1) != 0 || sync_dir(dir) != 0)
            return 4;
        if (link(a1, l1) != 0 || rename(d1, d2) != 0 || sync_dir(dir) != 0 || unlink(a2) != 0)
            return 5;
        return 7;
    }
    if (!strcmp(how, "tear-link-xdir")) {
        char s1[PATH_MAX + 32], s2[PATH_MAX + 32], a[PATH_MAX + 48], l[PATH_MAX + 48];
        snprintf(s1, sizeof s1, "%s/s1", dir);
        snprintf(s2, sizeof s2, "%s/s2", dir);
        snprintf(a, sizeof a, "%s/a", s1);
        snprintf(l, sizeof l, "%s/l", s2);
        if (mkdir(s1, 0755) != 0 || mkdir(s2, 0755) != 0 || make_synced_file(a, 'A') != 0)
            return 3;
        if (sync_dir(s1) != 0 || sync_dir(s2) != 0 || sync_dir(dir) != 0)
            return 4;
        if (link(a, l) != 0 || unlink(a) != 0)
            return 5;
        return 7;
    }
    char a[PATH_MAX + 32], l[PATH_MAX + 32];
    snprintf(a, sizeof a, "%s/a", dir);
    snprintf(l, sizeof l, "%s/l", dir);
    if (make_synced_file(a, 'A') != 0 || sync_dir(dir) != 0)
        return 3;
    if (link(a, l) != 0 || unlink(a) != 0)
        return 5;
    return 7;
}

static int child_rename_hl(const char *dir, const char *how)
{
    if (!strcmp(how, "rename-sym")) {
        char a[PATH_MAX + 32], s[PATH_MAX + 32];
        snprintf(a, sizeof a, "%s/a", dir);
        snprintf(s, sizeof s, "%s/s", dir);
        if (make_synced_file(a, 'A') != 0 || symlink(a, s) != 0 || sync_dir(dir) != 0)
            return 3;
        if (rename(s, a) != 0)
            return 4;
        return 7;
    }
    char a[PATH_MAX + 32], b[PATH_MAX + 32];
    snprintf(a, sizeof a, "%s/a", dir);
    snprintf(b, sizeof b, "%s/b", dir);
    if (make_synced_file(a, 'A') != 0 || link(a, b) != 0 || sync_dir(dir) != 0)
        return 3;
    if (rename(a, b) != 0)
        return 4;
    if (has_saved_artifact(dir))
        return 5;
    if (sync_dir(dir) != 0)
        return 6;
    if (has_saved_artifact(dir))
        return 8;
    return 7;
}

static int child_unlink_append(const char *dir, const char *how)
{
    char a[PATH_MAX + 32], b[PATH_MAX + 32], tail[512];
    snprintf(a, sizeof a, "%s/a", dir);
    snprintf(b, sizeof b, "%s/b", dir);
    memset(tail, 'B', sizeof tail);
    if (make_synced_file(a, 'A') != 0 || link(a, b) != 0 || sync_dir(dir) != 0)
        return 3;
    if (!strcmp(how, "ul-open")) {
        int fd = open(a, O_WRONLY | O_APPEND);
        if (fd < 0 || unlink(a) != 0 || sync_dir(dir) != 0)
            return 4;
        if (write(fd, tail, sizeof tail) != (ssize_t)sizeof tail)
            return 5;
        return 7;
    }
    int fb = open(b, O_WRONLY | O_APPEND);
    int fa = open(a, O_RDONLY);
    if (fb < 0 || fa < 0)
        return 4;
    close(fa);
    if (unlink(a) != 0 || sync_dir(dir) != 0) {
        close(fb);
        return 5;
    }
    if (write(fb, tail, sizeof tail) != (ssize_t)sizeof tail) {
        close(fb);
        return 6;
    }
    close(fb);
    return 7;
}

static int run_ext(const char *dir, const char *file, const char *how, const char *prob, int warmup, bool tear,
                   unsigned long seed, int undo_one, bool at_exit)
{
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512], w[16], sd[32], u[16];
        snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0", getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
        setenv("ASAN_OPTIONS", asan, 1);
        snprintf(w, sizeof w, "%d", warmup);
        snprintf(sd, sizeof sd, "%lu", seed);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", dir, 1);
        setenv("CRASHFS_PROB", prob, 1);
        setenv("CRASHFS_WARMUP", w, 1);
        setenv("CRASHFS_AT_EXIT", at_exit ? "1" : "0", 1);
        setenv("CRASHFS_SEED", sd, 1);
        if (tear)
            setenv("CRASHFS_TEAR", "1", 1);
        else
            unsetenv("CRASHFS_TEAR");
        if (undo_one >= 0) {
            snprintf(u, sizeof u, "%d", undo_one);
            setenv("CRASHFS_UNDO_ONE", u, 1);
        } else {
            unsetenv("CRASHFS_UNDO_ONE");
        }
        if (!strncmp(how, "reuse", 5)) {
            setenv("CRASHFS_MODE", "eio", 1);
            setenv("CRASHFS_EIO_ONLY", "x", 1);
            char out[PATH_MAX + 16];
            snprintf(out, sizeof out, "%s/reuse.out", dir);
            int o = open(out, O_WRONLY | O_CREAT | O_TRUNC, 0644); /* before the exec: the shim never sees it opened */
            if (o >= 0)
                dup2(o, 1);
        }
        int null = open("/dev/null", O_WRONLY);
        if (null >= 0)
            dup2(null, 2);
        execl(SELF, "test_crashfs", "--child", file, how, (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

static int run(const char *dir, const char *file, const char *how, const char *prob, int warmup)
{
    return run_ext(dir, file, how, prob, warmup, false, 1, -1, true);
}

/* A rewrite in place through pwritev is not durable without a sync: the power loss at exit takes it back. */
static void rewrite_lost(const char *dir)
{
    char f[PATH_MAX + 16];
    snprintf(f, sizeof f, "%s/rw", dir);
    CHECK(run(dir, f, "pwritev", "0", 0) == 7);
    char got[512];
    memset(got, 0, sizeof got);
    int fd = open(f, O_RDONLY);
    CHECK(fd >= 0 && pread(fd, got, sizeof got, 0) == (ssize_t)sizeof got);
    if (fd >= 0)
        close(fd);
    int bs = 0;
    for (size_t i = 0; i < sizeof got; i++)
        bs += got[i] == 'B';
    if (bs) {
        fprintf(stderr, "pwritev: %d bytes of the unsynced rewrite survived the power loss\n", bs);
        failures++;
    }
}

/* writev counts as an operation: with the warmup set to the operations of the run without it, the power fails at it */
static void writev_counts(const char *dir)
{
    char f[PATH_MAX + 16];
    snprintf(f, sizeof f, "%s/wv", dir);
    int k = -1;
    for (int w = 0; w < 64 && k < 0; w++)
        if (run(dir, f, "none", "1", w) == 7)
            k = w;
    CHECK(k > 0);
    if (k <= 0)
        return;
    int rc = run(dir, f, "writev", "1", k);
    if (rc != 99) {
        fprintf(stderr, "writev: the power did not fail at it (the child ended with %d, warmup %d)\n", rc, k);
        failures++;
    }
}

/* The file on the reused inode keeps its synced bytes through the power loss at exit, for each way of `how`. tmpfs never
 * reuses an inode number, so where the scratch directory is one the cases run in a directory of their own under /var/tmp;
 * where no inode comes back they say that they are skipped. */
#ifdef __linux__
static void inode_reuse_one(const char *top, const char *how)
{
    char dir[PATH_MAX + 32], f[PATH_MAX + 48];
    snprintf(dir, sizeof dir, "%s/%s", top, how);
    if (mkdir(dir, 0755) != 0) {
        fprintf(stderr, "%s: mkdir %s: %s\n", how, dir, strerror(errno));
        failures++;
        return;
    }
    snprintf(f, sizeof f, "%s/x", dir);
    int rc = run(dir, f, how, "1", 0);
    if (rc == 8) {
        printf("test_crashfs: %s SKIPPED: the file system of %s gave the freed inode to none of 256 new files\n", how, dir);
        return;
    }
    if (rc != 7) {
        fprintf(stderr, "%s: the child ended with %d\n", how, rc);
        failures++;
        return;
    }
    char of[PATH_MAX + 64], yn[PATH_MAX + 64] = "";
    snprintf(of, sizeof of, "%s/reuse.out", dir);
    FILE *o = fopen(of, "r");
    if (!o || !fgets(yn, sizeof yn, o))
        yn[0] = '\0';
    if (o)
        fclose(o);
    yn[strcspn(yn, "\n")] = '\0';
    struct stat st;
    if (!yn[0] || stat(yn, &st) != 0 || st.st_size != 512) {
        fprintf(stderr, "%s: %s, made on the inode of a removed file that met an EIO and synced with its name, has %lld "
                        "bytes after the power loss, not 512\n",
                how, yn, !yn[0] || stat(yn, &st) != 0 ? -1LL : (long long)st.st_size);
        failures++;
    }
}

static void inode_reuse(const char *dir)
{
    struct statfs sf;
    char own[PATH_MAX] = "";
    const char *top = dir;
    if (statfs(dir, &sf) == 0 && sf.f_type == TMPFS_MAGIC) {
        snprintf(own, sizeof own, "/var/tmp/test_crashfs_reuse.XXXXXX");
        if (!mkdtemp(own)) {
            printf("test_crashfs: reuse SKIPPED: %s is tmpfs (no inode is reused) and /var/tmp has no room: %s\n", dir,
                   strerror(errno));
            return;
        }
        top = own;
    }
    const char *hows[] = {"reuse", "reuse-open", "reuse-unseen", "reuse-close"};
    for (size_t i = 0; i < sizeof hows / sizeof *hows; i++)
        inode_reuse_one(top, hows[i]);
    if (own[0]) {
        char cmd[PATH_MAX + 16];
        snprintf(cmd, sizeof cmd, "rm -rf '%s'", own);
        if (system(cmd) != 0) {
        }
    }
}

#else
static void inode_reuse(const char *dir) { (void)dir; }
#endif

static bool check_file_512a(const char *path)
{
    char got[1024];
    int fd = open(path, O_RDONLY);
    if (fd < 0)
        return false;
    ssize_t n = read(fd, got, sizeof got);
    close(fd);
    if (n != 512)
        return false;
    for (int i = 0; i < 512; i++)
        if (got[i] != 'A')
            return false;
    return true;
}

static void rmdir_rollback(const char *top)
{
    char dir[PATH_MAX + 32];
    snprintf(dir, sizeof dir, "%s/rmdir_rb", top);
    CHECK(mkdir(dir, 0755) == 0);
    CHECK(run_ext(dir, dir, "rmdir", "0", 0, false, 1, -1, true) == 7);
    char d1[PATH_MAX + 48], d2[PATH_MAX + 48], d3in[PATH_MAX + 64], d4[PATH_MAX + 48], d5[PATH_MAX + 48], f[PATH_MAX + 48];
    snprintf(d1, sizeof d1, "%s/d1", dir);
    snprintf(d2, sizeof d2, "%s/d2", dir);
    snprintf(d3in, sizeof d3in, "%s/d3/inside", dir);
    snprintf(d4, sizeof d4, "%s/d4", dir);
    snprintf(d5, sizeof d5, "%s/d5", dir);
    snprintf(f, sizeof f, "%s/f", dir);
    struct stat st;
    CHECK(stat(d1, &st) == 0 && S_ISDIR(st.st_mode));
    CHECK(stat(d2, &st) == 0 && S_ISDIR(st.st_mode));
    CHECK(stat(d5, &st) == 0 && S_ISDIR(st.st_mode));
    CHECK(stat(d3in, &st) == 0 && S_ISDIR(st.st_mode));
    CHECK(stat(d4, &st) != 0 && errno == ENOENT);
    CHECK(check_file_512a(f));
    CHECK(!has_saved_artifact(dir));

    /* normal exit without CRASHFS_AT_EXIT cleans up unsynced .crashfs_saved_* entries */
    snprintf(dir, sizeof dir, "%s/rmdir_fini", top);
    CHECK(mkdir(dir, 0755) == 0);
    CHECK(run_ext(dir, dir, "rmdir", "0", 0, false, 1, -1, false) == 7);
    snprintf(d1, sizeof d1, "%s/d1", dir);
    snprintf(d2, sizeof d2, "%s/d2", dir);
    snprintf(d3in, sizeof d3in, "%s/d3/inside", dir);
    snprintf(d4, sizeof d4, "%s/d4", dir);
    snprintf(d5, sizeof d5, "%s/d5", dir);
    snprintf(f, sizeof f, "%s/f", dir);
    CHECK(stat(d1, &st) != 0 && errno == ENOENT);
    CHECK(stat(d2, &st) != 0 && errno == ENOENT);
    CHECK(stat(d4, &st) != 0 && errno == ENOENT);
    CHECK(stat(d5, &st) != 0 && errno == ENOENT);
    CHECK(stat(d3in, &st) == 0 && S_ISDIR(st.st_mode));
    CHECK(check_file_512a(f));
    CHECK(!has_saved_artifact(dir));

    /* CRASHFS_TEAR: undoing rename(d1/a, out) must also undo rmdir(d1) via name_inside_cut */
    for (int u = 0; u <= 2; u++) {
        snprintf(dir, sizeof dir, "%s/rmdir_tear_u%d", top, u);
        CHECK(mkdir(dir, 0755) == 0);
        CHECK(run_ext(dir, dir, "rmdir-tear", "0", 0, true, 1, u, true) == 7);
        char a[PATH_MAX + 64], out[PATH_MAX + 48];
        snprintf(d1, sizeof d1, "%s/d1", dir);
        snprintf(a, sizeof a, "%s/d1/a", dir);
        snprintf(out, sizeof out, "%s/out", dir);
        if (u == 0) {
            CHECK(stat(d1, &st) == 0 && S_ISDIR(st.st_mode));
            CHECK(check_file_512a(a));
            CHECK(access(out, F_OK) != 0 && errno == ENOENT);
            CHECK(!has_saved_artifact(d1));
        } else if (u == 1) {
            CHECK(stat(d1, &st) == 0 && S_ISDIR(st.st_mode));
            CHECK(access(a, F_OK) != 0 && errno == ENOENT);
            CHECK(check_file_512a(out));
        } else {
            CHECK(stat(d1, &st) != 0 && errno == ENOENT);
            CHECK(check_file_512a(out));
        }
        CHECK(!has_saved_artifact(dir));
    }

    /* forked child normal exit must not delete parent's .crashfs_saved_* backup */
    snprintf(dir, sizeof dir, "%s/rmdir_fork", top);
    CHECK(mkdir(dir, 0755) == 0);
    CHECK(run_ext(dir, dir, "rmdir-fork", "0", 0, false, 1, -1, false) == 99);
    snprintf(d1, sizeof d1, "%s/d1", dir);
    CHECK(stat(d1, &st) == 0 && S_ISDIR(st.st_mode));
    CHECK(!has_saved_artifact(dir));

    /* sibling path sharing prefix with CRASHFS_ROOT must not be tracked */
    char sib[PATH_MAX + 32];
    snprintf(dir, sizeof dir, "%s/rmdir_sib", top);
    snprintf(sib, sizeof sib, "%s/rmdir_sib2", top);
    CHECK(mkdir(dir, 0755) == 0 && mkdir(sib, 0755) == 0);
    CHECK(run_ext(dir, dir, "rmdir-sib", "0", 0, false, 1, -1, true) == 7);
    CHECK(stat(sib, &st) != 0 && errno == ENOENT);
    CHECK(!has_saved_artifact(top));
}

static void check_tear_link_one(const char *dir, const char *how, int mode, unsigned long sd, int undo_one)
{
    CHECK(mkdir(dir, 0755) == 0);
    CHECK(run_ext(dir, dir, how, "0", 0, true, sd, undo_one, true) == 7);
    char a[PATH_MAX + 64], l[PATH_MAX + 64];
    snprintf(a, sizeof a, mode == 1 ? "%s/s1/a" : mode == 2 ? "%s/d2/a" : "%s/a", dir);
    snprintf(l, sizeof l, mode == 1 ? "%s/s2/l" : mode == 2 ? "%s/d2/l" : "%s/l", dir);
    bool ha = access(a, F_OK) == 0, hl = access(l, F_OK) == 0;
    if (undo_one == 0)
        CHECK(ha && !hl);
    else if (undo_one == 1)
        CHECK(ha && hl);
    else if (undo_one == 2)
        CHECK(!ha && hl);
    else
        CHECK(ha || hl);
    CHECK(!ha || check_file_512a(a));
    CHECK(!hl || check_file_512a(l));
    CHECK(!has_saved_artifact(dir));
    if (mode == 1) {
        char s1[PATH_MAX + 64], s2[PATH_MAX + 64];
        snprintf(s1, sizeof s1, "%s/s1", dir);
        snprintf(s2, sizeof s2, "%s/s2", dir);
        CHECK(!has_saved_artifact(s1));
        CHECK(!has_saved_artifact(s2));
    } else if (mode == 2) {
        char d2[PATH_MAX + 64];
        snprintf(d2, sizeof d2, "%s/d2", dir);
        CHECK(!has_saved_artifact(d2));
    }
}

static void tear_link_unlink(const char *top)
{
    const char *hows[] = {"tear-link", "tear-link-xdir", "tear-link-dren"};
    for (int h = 0; h < 3; h++) {
        for (int u = 0; u <= 2; u++) {
            char dir[PATH_MAX + 48];
            snprintf(dir, sizeof dir, "%s/tl_%d_u%d", top, h, u);
            check_tear_link_one(dir, hows[h], h, 1, u);
        }
        for (unsigned long sd = 1; sd <= 32; sd++) {
            char dir[PATH_MAX + 48];
            snprintf(dir, sizeof dir, "%s/tl_%d_s%lu", top, h, sd);
            check_tear_link_one(dir, hows[h], h, sd, -1);
        }
    }
}

static void rename_hardlink(const char *top)
{
    for (int ae = 0; ae <= 1; ae++) {
        char dir[PATH_MAX + 32];
        snprintf(dir, sizeof dir, "%s/rhl_%d", top, ae);
        CHECK(mkdir(dir, 0755) == 0);
        CHECK(run_ext(dir, dir, "rename-hl", "0", 0, false, 1, -1, ae != 0) == 7);
        char a[PATH_MAX + 48], b[PATH_MAX + 48];
        snprintf(a, sizeof a, "%s/a", dir);
        snprintf(b, sizeof b, "%s/b", dir);
        CHECK(check_file_512a(a));
        CHECK(check_file_512a(b));
        CHECK(!has_saved_artifact(dir));

        /* rename(symlink_to_a, a) is NOT a no-op: lstat differs from stat */
        snprintf(dir, sizeof dir, "%s/rsym_%d", top, ae);
        CHECK(mkdir(dir, 0755) == 0);
        CHECK(run_ext(dir, dir, "rename-sym", "0", 0, false, 1, -1, ae != 0) == 7);
        snprintf(a, sizeof a, "%s/a", dir);
        snprintf(b, sizeof b, "%s/s", dir);
        struct stat sa, sb;
        if (ae) {
            CHECK(lstat(a, &sa) == 0 && S_ISREG(sa.st_mode));
            CHECK(check_file_512a(a));
            CHECK(lstat(b, &sb) == 0 && S_ISLNK(sb.st_mode));
        } else {
            CHECK(lstat(a, &sa) == 0 && S_ISLNK(sa.st_mode));
            CHECK(lstat(b, &sb) != 0 && errno == ENOENT);
        }
        CHECK(!has_saved_artifact(dir));
    }
}

static void unlink_first_append(const char *top)
{
    const char *hows[] = {"ul-open", "ul-closed"};
    for (size_t i = 0; i < 2; i++) {
        char dir[PATH_MAX + 32];
        snprintf(dir, sizeof dir, "%s/%s", top, hows[i]);
        CHECK(mkdir(dir, 0755) == 0);
        CHECK(run_ext(dir, dir, hows[i], "0", 0, false, 1, -1, true) == 7);
        char a[PATH_MAX + 48], b[PATH_MAX + 48];
        snprintf(a, sizeof a, "%s/a", dir);
        snprintf(b, sizeof b, "%s/b", dir);
        CHECK(access(a, F_OK) != 0 && errno == ENOENT);
        CHECK(check_file_512a(b));
    }
}

int main(int argc, char **argv)
{
    if (argc == 4 && !strcmp(argv[1], "--child"))
        exit(child(argv[2], argv[3]));
#ifdef __APPLE__
    printf("test_crashfs: skipped (crashfs is Linux only)\n");
    return 0;
#endif
    const char *base = argc > 1 ? argv[1] : "/tmp";
    if (!realpath("/proc/self/exe", SELF)) {
        perror("realpath");
        return 1;
    }
    snprintf(SHIM, sizeof SHIM, "%s", SELF);
    char *slash = strrchr(SHIM, '/');
    snprintf(slash + 1, sizeof SHIM - (size_t)(slash + 1 - SHIM), "libcrashfs.so");
    if (access(SHIM, R_OK) != 0) {
        fprintf(stderr, "test_crashfs: %s is missing (make it first)\n", SHIM);
        return 1;
    }
    char dir[PATH_MAX];
    snprintf(dir, sizeof dir, "%s/test_crashfs.XXXXXX", base);
    if (!mkdtemp(dir)) {
        perror("mkdtemp");
        return 1;
    }
    rewrite_lost(dir);
    writev_counts(dir);
    inode_reuse(dir);
    rmdir_rollback(dir);
    tear_link_unlink(dir);
    rename_hardlink(dir);
    unlink_first_append(dir);
    char cmd[PATH_MAX + 16];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (system(cmd) != 0) {
    }
    if (failures) {
        fprintf(stderr, "test_crashfs: %d failures\n", failures);
        return 1;
    }
    printf("test_crashfs: ok\n");
    return 0;
}
