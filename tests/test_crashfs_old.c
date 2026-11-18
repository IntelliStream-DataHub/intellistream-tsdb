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
 * and in every mode (CRASHFS_TEAR=1, none, REWRITE=old; round 13: the default mode gives old bytes, never zeros, for what was rewritten):
 *  - rename_sync:    a file written and fsynced, renamed (directory synced), then appended to and fsynced through the descriptor that
 *                    is still open on it: all 200 bytes are durable (VERIFY12: the shim credited the sync to the old name, and a
 *                    power loss cut the fsynced append: a false loss)
 *  - rename_newfile: the same, with a new file made at the old name before the fsync (the sync was credited to that file)
 *  - maxdirty:       17 one-byte rewrites of a synced 8 KiB file between two syncs: no byte nobody rewrote changes (a list of 16
 *                    regions once zeroed everything from the last one on), and a rewritten byte is old or new, never blank
 *                    (CRASHFS_REWRITE=zero, the harsh mode, zeroes only what was rewritten)
 * and round 14 (VERIFY13 finding 2: open(O_TRUNC) of a durable file was a removal and a new file, so that the new content, fsynced
 * without a directory sync, was lost to the OLD content, and an open(O_WRONLY | O_TRUNC) with no O_CREAT of an existing file failed
 * with ENOENT), in every mode:
 *  - otrunc_sync:     a synced file of 1000 bytes opened O_TRUNC | O_CREAT, 500 new bytes written and fsynced (no sync of the directory): the
 *                     file holds exactly the 500 new bytes after the loss (never the old 1000)
 *  - otrunc_nocreat:  the same without O_CREAT: the open must succeed (the file exists), and the same bytes remain
 *  - otrunc_pre:      the same on a file that was there before the shim was loaded (made by this parent), no O_CREAT
 *  - otrunc_nosync:   truncated and written, never synced: the old 1000 bytes (CRASHFS_TEAR: the old bytes, or a prefix of the new ones)
 *  - otrunc_dirsync:  truncated, written, the directory synced but the file not: the old content is not back (the truncation committed
 *                     with the directory's transaction): at most a prefix of the new bytes
 *  - otrunc_ident:    no loss: a descriptor opened before the truncation sees the new content and the inode is the same
 *  - unlink_nosync, write_then_unlink: a file unlinked without a directory sync is back with what was synced (a soft
 *                     check: bytes that were written and not synced may come back too, as on a disk): the synced bytes, never others
 * Usage: test_crashfs_old LIBCRASHFS.so DIR [SEEDS]
 */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdbool.h>
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

static int run_case(const char *so, const char *kind, const char *dir, unsigned long seed, int tear)
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
        char *envp[] = {ld, root, sd, tear ? "CRASHFS_TEAR=1" : "CRASHFS_X=1", "CRASHFS_PROB=0", NULL};
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

/* the attack cases of round 13 (VERIFY12 (B), chk/cfattack.c): contents of 8 KiB, A for the synced bytes, B for the rewrites */
static unsigned char CA[8192], CB[8192];
static void cfill(unsigned char *b, int n, unsigned char base)
{
    for (int i = 0; i < n; i++)
        b[i] = (unsigned char)(base + i % 200 + 1);
}
static void wr(int fd, const void *b, size_t n)
{
    if (write(fd, b, n) != (ssize_t)n)
        _exit(6);
}
static void dsync(const char *dir)
{
    int d = open(dir, O_RDONLY | O_DIRECTORY);
    if (d >= 0) {
        fsync(d);
        close(d);
    }
}
static void np(const char *dir, const char *n, char *out) { snprintf(out, 4400, "%s/%s", dir, n); }

static int child2(const char *kind, const char *dir)
{
    char a[4400], b[4400];
    cfill(CA, sizeof CA, 10);
    cfill(CB, sizeof CB, 77);
    if (!strcmp(kind, "rename_sync") || !strcmp(kind, "rename_newfile")) {
        np(dir, "a", a);
        np(dir, "b", b);
        int fd = open(a, O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (fd < 0)
            return 2;
        wr(fd, CA, 100);
        fsync(fd);
        dsync(dir);
        rename(a, b);
        dsync(dir);
        if (!strcmp(kind, "rename_newfile")) {
            int n = open(a, O_CREAT | O_WRONLY | O_TRUNC, 0644);
            wr(n, CB, 50);
            dsync(dir);
        }
        wr(fd, CA + 100, 100);
        fsync(fd);
    } else if (!strncmp(kind, "otrunc_", 7) || !strcmp(kind, "unlink_nosync") || !strcmp(kind, "write_then_unlink")) {
        np(dir, "a", a);
        int fd;
        if (!strcmp(kind, "otrunc_pre")) { /* made by the parent before the shim */
            fd = open(a, O_WRONLY | O_TRUNC);
            if (fd < 0)
                return 9;
            wr(fd, CB, 500);
            fsync(fd);
            pull(dir);
            return 3;
        }
        fd = open(a, O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (fd < 0)
            return 2;
        wr(fd, CA, 1000);
        fsync(fd);
        dsync(dir);
        if (!strcmp(kind, "unlink_nosync")) {
            unlink(a);
            wr(fd, CA + 1000, 100); /* appended to the unlinked file, not synced */
            pull(dir);
            return 3;
        }
        if (!strcmp(kind, "write_then_unlink")) {
            wr(fd, CA + 1000, 100);
            close(fd);
            unlink(a);
            pull(dir);
            return 3;
        }
        close(fd);
        if (!strcmp(kind, "otrunc_ident")) { /* no loss: the same file, truncated */
            int fd0 = open(a, O_RDONLY);
            struct stat s0, s1;
            fd = open(a, O_WRONLY | O_TRUNC);
            if (fd0 < 0 || fd < 0 || fstat(fd0, &s0) != 0 || stat(a, &s1) != 0 || s0.st_ino != s1.st_ino)
                return 7;
            wr(fd, CB, 500);
            fsync(fd);
            unsigned char r[600];
            if (pread(fd0, r, sizeof r, 0) != 500 || memcmp(r, CB, 500) != 0)
                return 8;
            return 0;
        }
        fd = open(a, !strcmp(kind, "otrunc_sync") ? O_WRONLY | O_TRUNC | O_CREAT : O_WRONLY | O_TRUNC, 0644);
        if (fd < 0)
            return 9;
        wr(fd, CB, 500);
        if (!strcmp(kind, "otrunc_dirsync"))
            dsync(dir);
        else if (strcmp(kind, "otrunc_nosync"))
            fsync(fd);
    } else { /* maxdirty */
        np(dir, "m", a);
        int fd = open(a, O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (fd < 0)
            return 2;
        wr(fd, CA, 8192);
        fsync(fd);
        dsync(dir);
        for (int i = 0; i < 17; i++)
            if (pwrite(fd, CB, 1, (off_t)i * 64) != 1)
                return 2;
    }
    pull(dir);
    return 3;
}

static long slurp(const char *p, unsigned char *buf, size_t cap)
{
    int fd = open(p, O_RDONLY);
    if (fd < 0)
        return -1;
    long n = read(fd, buf, cap);
    close(fd);
    return n;
}

/* mode: "tear", "plain", "old" or "zero" */
static void run_case2(const char *so, const char *kind, const char *mode, const char *dir, unsigned long seed)
{
    if (mkdir(dir, 0755) != 0 && errno != EEXIST) {
        failures++;
        return;
    }
    if (!strcmp(kind, "otrunc_pre")) { /* there before the shim: durable at its size */
        char pa[4400];
        np(dir, "a", pa);
        int pf = open(pa, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (pf < 0 || write(pf, CA, 1000) != 1000 || fsync(pf) != 0) {
            failures++;
            return;
        }
        close(pf);
        dsync(dir);
    }
    pid_t pid = fork();
    if (pid == 0) {
        int nul = open("/dev/null", O_WRONLY);
        if (nul >= 0)
            dup2(nul, 2);
        char ld[4300], root[4300], sd[64], *argv[5];
        snprintf(ld, sizeof ld, "LD_PRELOAD=%s", so);
        snprintf(root, sizeof root, "CRASHFS_ROOT=%s", dir);
        snprintf(sd, sizeof sd, "CRASHFS_SEED=%lu", seed);
        const char *m = !strcmp(mode, "tear") ? "CRASHFS_TEAR=1" : !strcmp(mode, "old") ? "CRASHFS_REWRITE=old"
                        : !strcmp(mode, "zero") ? "CRASHFS_REWRITE=zero" : "CRASHFS_X=1";
        char *envp[] = {ld, root, sd, (char *)m, "CRASHFS_PROB=0", NULL};
        argv[0] = "test_crashfs_old";
        argv[1] = "--child2";
        argv[2] = (char *)kind;
        argv[3] = (char *)dir;
        argv[4] = NULL;
        execve("/proc/self/exe", argv, envp);
        _exit(2);
    }
    int st = 0;
    if (pid < 0 || waitpid(pid, &st, 0) != pid) {
        failures++;
        return;
    }
    bool noloss = !strcmp(kind, "otrunc_ident");
    CHECK(WIFEXITED(st) && WEXITSTATUS(st) == (noloss ? 0 : 99), "%s/%s seed %lu: the child ended with status 0x%x, not %s", kind, mode, seed, st,
          noloss ? "0 (an open(O_TRUNC) of an existing file failed, or a descriptor opened before it saw other content, or the inode changed)"
                 : "the loss's 99 (an open(O_TRUNC) without O_CREAT of an existing file failed?)");
    static unsigned char got[16384];
    char p[4400];
    long n;
    if (noloss)
        return;
    if (!strncmp(kind, "otrunc_", 7) || !strcmp(kind, "unlink_nosync") || !strcmp(kind, "write_then_unlink")) {
        np(dir, "a", p);
        n = slurp(p, got, sizeof got);
        bool tr = !strcmp(mode, "tear");
        if (!strcmp(kind, "otrunc_sync") || !strcmp(kind, "otrunc_nocreat") || !strcmp(kind, "otrunc_pre")) {
            CHECK(n == 500 && memcmp(got, CB, 500) == 0, "%s/%s seed %lu: a has %ld bytes %s, the 500 fsynced after the truncation were wanted (the old 1000 are back?)",
                  kind, mode, seed, n, n > 0 && got[0] == CA[0] ? "(the OLD content)" : "");
        } else if (!strcmp(kind, "otrunc_nosync")) {
            bool old = n == 1000 && memcmp(got, CA, 1000) == 0, pre = n >= 0 && n <= 500 && memcmp(got, CB, (size_t)n) == 0;
            CHECK(old || (tr && pre), "%s/%s seed %lu: a has %ld bytes that are neither the old 1000 nor (CRASHFS_TEAR) a prefix of the new 500", kind, mode, seed, n);
        } else if (!strcmp(kind, "otrunc_dirsync")) {
            CHECK(n >= 0 && n <= 500 && memcmp(got, CB, (size_t)n) == 0, "%s/%s seed %lu: a has %ld bytes: the truncation committed with the directory's sync, "
                  "so only a prefix of the new 500 may be there (the old content is back?)", kind, mode, seed, n);
        } else { /* unlink_nosync, write_then_unlink: the synced 1000 bytes, then the unsynced ones may be there too (soft) */
            CHECK((tr && n == -1) /* CRASHFS_TEAR may keep the unlink */ || (n >= 1000 && n <= 1100 && memcmp(got, CA, (size_t)n) == 0), "%s/%s seed %lu: a has %ld bytes, the synced 1000 (and perhaps the unsynced 100) of the same content were wanted",
                  kind, mode, seed, n);
        }
        return;
    }
    if (!strcmp(kind, "rename_sync") || !strcmp(kind, "rename_newfile")) {
        np(dir, "b", p);
        n = slurp(p, got, sizeof got);
        CHECK(n == 200 && memcmp(got, CA, 200) == 0, "%s/%s seed %lu: b has %ld bytes, 200 were fsynced (through the descriptor opened before the rename)",
              kind, mode, seed, n);
        if (!strcmp(kind, "rename_newfile")) {
            np(dir, "a", p);
            n = slurp(p, got, sizeof got);
            CHECK(n == 50 || n == 0 || n == -1, "%s/%s seed %lu: a (made at the old name, never synced) has %ld bytes", kind, mode, seed, n);
        }
        return;
    }
    np(dir, "m", p);
    n = slurp(p, got, sizeof got);
    CHECK(n == 8192, "maxdirty/%s seed %lu: the file has %ld bytes, 8192", mode, seed, n);
    for (long i = 0; i < n && n == 8192; i++) {
        int rew = i % 64 == 0 && i / 64 < 17;
        if (!rew && got[i] != CA[i]) {
            CHECK(0, "maxdirty/%s seed %lu: byte %ld, which nobody rewrote, is %d (synced %d)", mode, seed, i, got[i], CA[i]);
            break;
        }
        if (rew && strcmp(mode, "zero") && got[i] != CA[i] && got[i] != CB[0]) {
            CHECK(0, "maxdirty/%s seed %lu: rewritten byte %ld is %d, neither the synced %d nor the new %d", mode, seed, i, got[i], CA[i], CB[0]);
            break;
        }
    }
}

int main(int argc, char **argv)
{
    if (argc >= 5 && !strcmp(argv[1], "--child"))
        return child(argv[2], argv[3], strtoul(argv[4], NULL, 10));
    if (argc >= 4 && !strcmp(argv[1], "--child2"))
        return child2(argv[2], argv[3]);
    if (argc < 3) {
        fprintf(stderr, "usage: %s LIBCRASHFS.so DIR [SEEDS]\n", argv[0]);
        return 2;
    }
    int san = 0; /* a sanitizer's runtime must come first in the library list: with crashfs preloaded the child refuses to start */
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
    san = 1;
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
    san = 1;
#endif
#endif
    if (san) {
        printf("test_crashfs_old: skipped: crashfs cannot be preloaded into a sanitizer build\n");
        return 0;
    }
    cfill(CA, sizeof CA, 10);
    cfill(CB, sizeof CB, 77);
    unsigned long seeds = argc > 3 ? strtoul(argv[3], NULL, 10) : 120;
    char base[2048];
    snprintf(base, sizeof base, "%s/crashfs_old.%d", argv[2], (int)getpid());
    if (mkdir(base, 0755) != 0)
        return 2;
    static const char *kinds[] = {"cut", "rewrite", "own"};
    for (unsigned long seed = 1; seed <= seeds && failures < 10; seed++) {
        for (int tear = 0; tear < 2; tear++) /* the default mode (old bytes back) and the tear mode (old or new, each sector) */
            for (int k = 0; k < 3; k++) {
                char dir[3000];
                snprintf(dir, sizeof dir, "%s/%s%d_%lu", base, kinds[k], tear, seed);
                if (run_case(argv[1], kinds[k], dir, seed, tear) != 0) {
                    fprintf(stderr, "FAIL: could not set up %s seed %lu\n", kinds[k], seed);
                    return 2;
                }
            }
        static const char *k2[] = {"rename_sync", "rename_newfile", "maxdirty", "otrunc_sync", "otrunc_nocreat", "otrunc_pre", "otrunc_nosync",
                                   "otrunc_dirsync", "otrunc_ident", "unlink_nosync", "write_then_unlink"};
        static const char *m2[] = {"plain", "tear", "old", "zero"};
        for (int k = 0; k < (int)(sizeof k2 / sizeof k2[0]); k++)
            for (int m = 0; m < 4; m++) {
                if (m == 3 && k != 2 && k < 3)
                    continue; /* the harsh mode zeroes what was rewritten: nothing to say about a rename */
                char dir[3000];
                snprintf(dir, sizeof dir, "%s/%s-%s%lu", base, k2[k], m2[m], seed);
                run_case2(argv[1], k2[k], m2[m], dir, seed);
            }
    }
    char cmd[4300];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", base);
    if (system(cmd) != 0) {
    }
    if (failures) {
        fprintf(stderr, "test_crashfs_old: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_crashfs_old: %lu seeds x (cut, rewrite, own in two modes; rename_sync, rename_newfile, maxdirty, the open(O_TRUNC) and unlink cases in every mode): a power loss gives back the synced content\n", seeds);
    return 0;
}
