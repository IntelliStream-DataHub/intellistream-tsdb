/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * crashfs (tests/jepsen/crashfs.c) with CRASHFS_TEAR keeps the durable content of a sector it is about to change, so
 * a power loss can bring it back. It once read that content through the CALLER's descriptor, and tsdb opens CLASSES
 * write-only (src/classes.c): the pread failed with EBADF, the zeros it had set up were kept, and the next power loss
 * wrote them over synced records (jepsen's classes workload, 312 zero bytes where 13 records had been fsynced: a
 * false loss, VERIFY11 (B)). A simulator must never do worse than a disk may: a power loss gives back old or new bytes
 * for what was rewritten or cut, never bytes nobody wrote.
 *
 * Each case runs a child under LD_PRELOAD=libcrashfs.so (CRASHFS_TEAR=1, a seed of its own, no random loss; the child
 * raises SIGUSR2 itself and the loss happens at its next file operation: exit status 99), then reads the file:
 *  - cut:     a file of synced content opened O_WRONLY and truncated below its durable end (the torn-tail cut of
 *             cls_cut_tail): every byte the file has is the synced content, and it is not shorter than the cut;
 *  - rewrite: two whole sectors of a synced file rewritten in place through an O_WRONLY descriptor: each sector is
 *             all old or all new, the rest and the size are untouched;
 *  - own:     the same cut on a file the child made and fsynced itself.
 * Usage: test_crashfs_old LIBCRASHFS.so DIR [SEEDS]
 */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define LEN 2700 /* not a multiple of 512: the last sector is short */
#define SECT 512

static int failures;

#define CHECK(c, ...)                                                                                                  \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "FAIL: %s:%d: ", __FILE__, __LINE__);                                                      \
            fprintf(stderr, __VA_ARGS__);                                                                              \
            fprintf(stderr, "\n");                                                                                     \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

/* the synced content: no zero byte in it, so a zeroed sector shows */
static void content(unsigned char *b, unsigned long seed, unsigned char salt)
{
    unsigned long x = seed * 2654435761u + salt + 1;
    for (int i = 0; i < LEN; i++) {
        x = x * 6364136223846793005ull + 1442695040888963407ull;
        b[i] = (unsigned char)(1 + (x >> 33) % 255);
    }
}

static int write_all(int fd, const unsigned char *b, size_t n)
{
    size_t got = 0;
    while (got < n) {
        ssize_t r = write(fd, b + got, n - got);
        if (r <= 0)
            return -1;
        got += (size_t)r;
    }
    return 0;
}

static int make_synced(const char *path, const unsigned char *b)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0 || write_all(fd, b, LEN) != 0 || fsync(fd) != 0)
        return -1;
    close(fd);
    char dir[4400];
    snprintf(dir, sizeof dir, "%s", path);
    *strrchr(dir, '/') = '\0';
    int dfd = open(dir, O_RDONLY | O_DIRECTORY);
    if (dfd < 0 || fsync(dfd) != 0)
        return -1;
    close(dfd);
    return 0;
}

/* the plug: the power goes at the next file operation, which this one is */
static void pull(const char *dir)
{
    raise(SIGUSR2);
    char other[4400];
    snprintf(other, sizeof other, "%s/other", dir);
    int fd = open(other, O_WRONLY | O_CREAT, 0644);
    if (fd >= 0)
        (void)!write(fd, "x", 1);
    _exit(3); /* no loss */
}

static int child(const char *kind, const char *dir, unsigned long seed)
{
    char path[4400];
    snprintf(path, sizeof path, "%s/f", dir);
    unsigned char b[LEN];
    content(b, seed, 0);
    if (!strcmp(kind, "own") && make_synced(path, b) != 0)
        return 2;
    int fd = open(path, O_WRONLY); /* write-only, as tsdb opens CLASSES */
    if (fd < 0)
        return 2;
    if (!strcmp(kind, "rewrite")) {
        unsigned char n[LEN];
        content(n, seed, 1);
        if (pwrite(fd, n + SECT, 2 * SECT, SECT) != 2 * SECT)
            return 2;
    } else {
        off_t cut = (off_t)(24 + (seed * 37) % (LEN - 48)); /* anywhere, sector boundaries and not */
        if (ftruncate(fd, cut) != 0)
            return 2;
    }
    pull(dir);
    return 3;
}

static int run_case(const char *so, const char *kind, const char *dir, unsigned long seed)
{
    char path[4400];
    snprintf(path, sizeof path, "%s/f", dir);
    unsigned char b[LEN], n[LEN];
    content(b, seed, 0);
    content(n, seed, 1);
    if (mkdir(dir, 0755) != 0 && errno != EEXIST)
        return -1;
    if (strcmp(kind, "own") && make_synced(path, b) != 0) /* from before the shim: durable at its size */
        return -1;
    pid_t pid = fork();
    if (pid < 0)
        return -1;
    if (pid == 0) {
        int nul = open("/dev/null", O_WRONLY);
        if (nul >= 0)
            dup2(nul, 2); /* the shim says what it lost on stderr */
        char ld[4300], root[4300], sd[64], *argv[6];
        snprintf(ld, sizeof ld, "LD_PRELOAD=%s", so);
        snprintf(root, sizeof root, "CRASHFS_ROOT=%s", dir);
        snprintf(sd, sizeof sd, "CRASHFS_SEED=%lu", seed);
        char sdl[64];
        snprintf(sdl, sizeof sdl, "%lu", seed);
        char *envp[] = {ld, root, sd, "CRASHFS_TEAR=1", "CRASHFS_PROB=0", NULL};
        argv[0] = "test_crashfs_old";
        argv[1] = "--child";
        argv[2] = (char *)kind;
        argv[3] = (char *)dir;
        argv[4] = sdl;
        argv[5] = NULL;
        execve("/proc/self/exe", argv, envp);
        _exit(2);
    }
    int st = 0;
    if (waitpid(pid, &st, 0) != pid)
        return -1;
    CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 99, "%s seed %lu: the child ended with status 0x%x, not the loss's 99", kind,
          seed, st);
    unsigned char got[LEN + 64];
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        CHECK(0, "%s seed %lu: the file is gone", kind, seed);
        return 0;
    }
    ssize_t sz = read(fd, got, sizeof got);
    close(fd);
    if (!strcmp(kind, "rewrite")) {
        CHECK(sz == LEN, "rewrite seed %lu: size %zd, wanted %d", seed, sz, LEN);
        for (int s = 0; s * SECT < LEN && sz == LEN; s++) {
            int a = s * SECT, e = a + SECT < LEN ? a + SECT : LEN;
            int old = memcmp(got + a, b + a, (size_t)(e - a)) == 0;
            int nw = s == 1 || s == 2 ? memcmp(got + a, n + a, (size_t)(e - a)) == 0 : 0;
            CHECK(old || nw, "rewrite seed %lu: sector %d is neither the synced content nor the rewrite (first byte 0x%02x)",
                  seed, s, got[a]);
        }
    } else {
        off_t cut = (off_t)(24 + (seed * 37) % (LEN - 48));
        CHECK(sz >= cut && sz <= LEN, "%s seed %lu: size %zd, wanted %lld..%d", kind, seed, sz, (long long)cut, LEN);
        for (ssize_t i = 0; i < sz && i < LEN; i++)
            if (got[i] != b[i]) {
                CHECK(0, "%s seed %lu: byte %zd is 0x%02x, the synced content has 0x%02x (cut at %lld)", kind, seed, i,
                      got[i], b[i], (long long)cut);
                break;
            }
    }
    return 0;
}

int main(int argc, char **argv)
{
    if (argc >= 5 && !strcmp(argv[1], "--child"))
        return child(argv[2], argv[3], strtoul(argv[4], NULL, 10));
    if (argc < 3) {
        fprintf(stderr, "usage: %s LIBCRASHFS.so DIR [SEEDS]\n", argv[0]);
        return 2;
    }
    unsigned long seeds = argc > 3 ? strtoul(argv[3], NULL, 10) : 120;
    char base[2048];
    snprintf(base, sizeof base, "%s/crashfs_old.%d", argv[2], (int)getpid());
    if (mkdir(base, 0755) != 0)
        return 2;
    static const char *kinds[] = {"cut", "rewrite", "own"};
    for (unsigned long seed = 1; seed <= seeds && failures < 10; seed++)
        for (int k = 0; k < 3; k++) {
            char dir[3000];
            snprintf(dir, sizeof dir, "%s/%s%lu", base, kinds[k], seed);
            if (run_case(argv[1], kinds[k], dir, seed) != 0) {
                fprintf(stderr, "FAIL: could not set up %s seed %lu\n", kinds[k], seed);
                return 2;
            }
            if (failures >= 10)
                break;
        }
    char cmd[4300];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", base);
    if (system(cmd) != 0) {
    }
    if (failures) {
        fprintf(stderr, "test_crashfs_old: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_crashfs_old: %lu seeds x 3 cases: a power loss gives back the synced content\n", seeds);
    return 0;
}
