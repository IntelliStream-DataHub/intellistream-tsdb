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
 *   test_crashfs [scratch dir]        (needs build/libcrashfs.so beside the binary)
 */
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/wait.h>
#include <unistd.h>

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
static int child(const char *file, const char *how)
{
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

static int run(const char *dir, const char *file, const char *how, const char *prob, int warmup)
{
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512], w[16];
        snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0", getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
        setenv("ASAN_OPTIONS", asan, 1);
        snprintf(w, sizeof w, "%d", warmup);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", dir, 1);
        setenv("CRASHFS_PROB", prob, 1);
        setenv("CRASHFS_WARMUP", w, 1);
        setenv("CRASHFS_AT_EXIT", "1", 1);
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
