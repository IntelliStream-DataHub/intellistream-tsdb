/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The disk side of tsdbd's cluster volume manager (server/cvolfs.c: make <root>/v<N>, keep its open generation and nonce
 * in REPL through src/replfile.c). Tests:
 *  - basic: a new volume is made whole (META, REPL, FNO, no v<N>.tmp), every open saves generation + 1 under a new
 *    nonce, the rid and the identity are kept, the nonce that was reported is the one in REPL;
 *  - refusals, each with a line that names the path: a record above the directory's generation (an older copy), the
 *    recorded generation saved by another open (same generation, other nonce), another rid, another volume uuid,
 *    cluster or node, a missing REPL, one with no valid slot, a stub text REPL, a failed save (EFBIG: fail-stop for
 *    the volume, the old state stays), a v<N>.tmp left by a crash (removed, the volume made);
 *  - a damaged newest REPL slot is "unsure": the open skips a belt, a record at the generation the lost save may have
 *    carried is not mistaken for this open's, and what the open saved loads clean;
 *  - power lost (crashfs, unsynced data torn) at every file-system step of making v<N> and of three opens, and again
 *    at a step of the resumed run: v<N> is absent or whole, never a stranded v<N>.tmp after the next prepare, REPL is
 *    the last acknowledged open, the one in flight or the acknowledged one under a belt, never a nonce that is not
 *    the one reported, and the next prepare is accepted under a record at the acknowledged open.
 *
 *   ./build/test_cvolfs [scratch dir]
 *   ./build/test_cvolfs --mutant nosync|nodirsync|nononce|trustunsure|tornstale|samerid [scratch dir]    must fail
 *   ./build/test_cvolfs --child DIR ACKFILE      (internal: a process the shim kills)
 */
#define _GNU_SOURCE
#include "cvolfs.h"
#include "internal.h"
#include "replfile.h"

#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static int failures;
static int saved_err = -1;
static char S[PATH_MAX], SELF[PATH_MAX], SHIM[PATH_MAX + 32];

static void fail_at(const char *file, int line, const char *what)
{
    char buf[512];
    int n = snprintf(buf, sizeof buf, "%s:%d: CHECK failed: %s\n", file, line, what);
    if (write(saved_err >= 0 ? saved_err : 2, buf, (size_t)n) < 0)
        failures++;
    failures++;
}
#define CHECK(cond)                             \
    do {                                        \
        if (!(cond))                            \
            fail_at(__FILE__, __LINE__, #cond); \
    } while (0)

static int sh(const char *fmt, ...)
{
    char cmd[4096];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(cmd, sizeof cmd, fmt, ap);
    va_end(ap);
    return system(cmd);
}

/* stderr into a pipe until cap_end(): the log lines are expected, and the tests look at them (a pipe, not a file: the
 * failed-save test sets a file size limit of 0, which a file would hit too) */
static int cap_rd = -1;
static void cap_begin(void)
{
    fflush(stderr);
    int pf[2];
    saved_err = dup(2);
    if (pipe2(pf, O_CLOEXEC | O_NONBLOCK) == 0) {
        cap_rd = pf[0];
        dup2(pf[1], 2);
        close(pf[1]);
    }
}

static void cap_end(char *out, size_t cap)
{
    fflush(stderr);
    if (saved_err >= 0) {
        dup2(saved_err, 2); /* closes the write end: the pipe ends at what was written */
        close(saved_err);
        saved_err = -1;
    }
    size_t n = 0;
    if (cap_rd >= 0) {
        char junk[4096];
        bool room = out != NULL;
        for (ssize_t r; (r = read(cap_rd, room ? out + n : junk, room ? cap - 1 - n : sizeof junk)) > 0;) {
            n += room ? (size_t)r : 0;
            room = out && n + 1 < cap;
        }
        close(cap_rd);
        cap_rd = -1;
    }
    if (out)
        out[n] = 0;
}

static void fresh(const char *name, char *dir, size_t cap)
{
    snprintf(dir, cap, "%s/%s", S, name);
    sh("rm -rf '%s'; mkdir -p '%s'", dir, dir);
    char meta[PATH_MAX + 32];
    snprintf(meta, sizeof meta, "%s/META", dir);
    FILE *f = fopen(meta, "w");
    CHECK(f != NULL);
    if (f) {
        fputs("version=9\npartition_ms=86400000\ngranule_rows=8192\npartition_classes=1\nsomething=else\n", f);
        fclose(f);
    }
}

static struct cvolfs_rec rec(uint32_t number, uint64_t open_gen, uint64_t open_nonce)
{
    struct cvolfs_rec r;
    memset(&r, 0, sizeof r);
    r.number = number;
    snprintf(r.name, sizeof r.name, "vol%u", number);
    for (int i = 0; i < 16; i++)
        r.uuid[i] = (uint8_t)(0x10 + i), r.cluster[i] = (uint8_t)(0xa0 + i);
    r.node = 3;
    r.incarnation = 77;
    r.rid = 0xfeedfacecafebeefull;
    r.open_gen = open_gen;
    r.open_nonce = open_nonce;
    return r;
}

/* REPL of root/v<N> as a load gives it */
static int load_repl(const char *root, uint32_t n, struct replfile_state *st, bool *torn)
{
    char v[PATH_MAX + 32];
    snprintf(v, sizeof v, "%s/v%u", root, n);
    int fd = open(v, O_RDONLY | O_DIRECTORY);
    if (fd < 0)
        return TSDB_ERR_NOTFOUND;
    struct replfile *rf;
    int rc = replfile_load(fd, &rf, st, torn);
    close(fd);
    replfile_close(rf);
    return rc;
}

static bool exists(const char *root, const char *rel)
{
    char p[PATH_MAX * 2];
    snprintf(p, sizeof p, "%s/%s", root, rel);
    return access(p, F_OK) == 0;
}

static void poke_slot(const char *root, uint32_t n, int slot, off_t off)
{
    char p[PATH_MAX * 2];
    snprintf(p, sizeof p, "%s/v%u/REPL", root, n);
    int fd = open(p, O_RDWR);
    uint8_t b = 0;
    CHECK(fd >= 0 && pread(fd, &b, 1, (off_t)slot * REPLFILE_PAGE + off) == 1);
    b ^= 0x40;
    CHECK(fd >= 0 && pwrite(fd, &b, 1, (off_t)slot * REPLFILE_PAGE + off) == 1);
    if (fd >= 0)
        close(fd);
}

/* which slot of REPL holds the newer generation (0 or 1) */
static int newest_slot(const char *root, uint32_t n)
{
    char p[PATH_MAX * 2];
    snprintf(p, sizeof p, "%s/v%u/REPL", root, n);
    int fd = open(p, O_RDONLY);
    uint64_t g[2] = {0, 0};
    CHECK(fd >= 0);
    for (int i = 0; i < 2 && fd >= 0; i++)
        CHECK(pread(fd, &g[i], 8, (off_t)i * REPLFILE_PAGE + 8) == 8);
    if (fd >= 0)
        close(fd);
    return g[1] > g[0];
}

static void test_basic(void)
{
    char root[PATH_MAX + 32], log[8192];
    fresh("basic", root, sizeof root);
    struct cvolfs_open o1, o2;
    struct cvolfs_rec r = rec(1, 0, 0);
    cap_begin();
    CHECK(cvolfs_prepare(root, &r, &o1) == 0);
    cap_end(log, sizeof log);
    CHECK(o1.gen == 1 && o1.nonce != 0 && o1.rid == r.rid && !o1.unsure);
    CHECK(exists(root, "v1/META") && exists(root, "v1/REPL") && exists(root, "v1/FNO") && !exists(root, "v1.tmp"));
    CHECK(strstr(log, "made ") != NULL);
    char meta[2048];
    snprintf(meta, sizeof meta, "%s/v1/META", root);
    FILE *f = fopen(meta, "r");
    size_t n = f ? fread(meta, 1, sizeof meta - 1, f) : 0;
    meta[n] = 0;
    if (f)
        fclose(f);
    CHECK(strstr(meta, "version=9\n") && strstr(meta, "partition_ms=86400000\n") && strstr(meta, "\nvolume=1\n") &&
          strstr(meta, "volume_uuid=101112131415161718191a1b1c1d1e1f\n") && strstr(meta, "requires=cluster,replica\n") &&
          !strstr(meta, "something=else"));
    struct replfile_state st;
    bool torn;
    CHECK(load_repl(root, 1, &st, &torn) == 0 && !torn);
    CHECK(st.open_gen == 1 && st.open_nonce == o1.nonce && st.rid == r.rid && st.node == 3 && st.incarnation == 77);
    CHECK(memcmp(st.volume_uuid, r.uuid, 16) == 0 && memcmp(st.cluster_uuid, r.cluster, 16) == 0);
    replfile_state_free(&st);

    /* the next open, under the record of the first */
    r = rec(1, 1, o1.nonce);
    r.incarnation = 78;
    cap_begin();
    CHECK(cvolfs_prepare(root, &r, &o2) == 0);
    cap_end(log, sizeof log);
    CHECK(o2.gen == 2 && o2.nonce != 0 && o2.nonce != o1.nonce && o2.rid == o1.rid);
    CHECK(strstr(log, "made ") == NULL);
    CHECK(load_repl(root, 1, &st, &torn) == 0 && !torn && st.open_gen == 2 && st.open_nonce == o2.nonce &&
          st.incarnation == 78);
    replfile_state_free(&st);
    /* a record that is ahead of the directory by one open of this very copy (the test switch's lowered push) */
    r = rec(1, 2, 0x1111);
    r.stale_first = 1;
    CHECK(cvolfs_prepare(root, &r, &o1) == 0 && o1.gen == 3);
    printf("  a new volume is made whole and every open saves its generation and nonce\n");
}

static int refused(const char *root, const struct cvolfs_rec *r, const char *needle)
{
    struct cvolfs_open o;
    char log[8192];
    cap_begin();
    int rc = cvolfs_prepare(root, r, &o);
    cap_end(log, sizeof log);
    if (rc == 0) {
        fprintf(stderr, "expected a refusal naming \"%s\"\n", needle);
        return 0;
    }
    if (!strstr(log, needle) || !strstr(log, root)) {
        fprintf(stderr, "the refusal lacks \"%s\" or the path:\n%s", needle, log);
        return 0;
    }
    return 1;
}

static void test_refusals(void)
{
    char root[PATH_MAX + 32], log[8192];
    struct cvolfs_open o, o2;
    fresh("refuse", root, sizeof root);
    struct cvolfs_rec r = rec(1, 0, 0);
    CHECK(cvolfs_prepare(root, &r, &o) == 0);
    /* the directory is at generation 1; copy it to start each case from it */
    char snap[PATH_MAX + 32];
    snprintf(snap, sizeof snap, "%s/v1.snap", root);
    CHECK(sh("cp -a '%s/v1' '%s'", root, snap) == 0);
#define RESTORE() CHECK(sh("rm -rf '%s/v1' && cp -a '%s' '%s/v1'", root, snap, root) == 0)

    /* an older copy: the group holds generation 2, the directory is at 1 */
    r = rec(1, 2, 0x55);
    CHECK(refused(root, &r, "older copy of the directory"));
    /* the recorded generation saved by another open: same generation, another nonce */
    r = rec(1, 1, o.nonce ^ 1);
    CHECK(refused(root, &r, "in another open than the one the metadata group recorded"));
    /* another instance, volume, cluster, node */
    r = rec(1, 1, o.nonce), r.rid++;
    CHECK(refused(root, &r, "another instance of the directory"));
    r = rec(1, 1, o.nonce), r.uuid[2]++;
    CHECK(refused(root, &r, "another volume, cluster or node"));
    r = rec(1, 1, o.nonce), r.cluster[2]++;
    CHECK(refused(root, &r, "another volume, cluster or node"));
    r = rec(1, 1, o.nonce), r.node++;
    CHECK(refused(root, &r, "another volume, cluster or node"));
    /* nothing above wrote: the same record as the directory's own opens it */
    r = rec(1, 1, o.nonce);
    CHECK(cvolfs_prepare(root, &r, &o2) == 0 && o2.gen == 2);
    RESTORE();

    /* a missing REPL in a directory that has data, one with no valid slot, a stub of the old text format */
    char p[PATH_MAX + 64];
    snprintf(p, sizeof p, "%s/v1/REPL", root);
    CHECK(unlink(p) == 0);
    CHECK(refused(root, &r, "REPL is missing"));
    int fd = open(p, O_WRONLY | O_CREAT | O_EXCL, 0644);
    static const char stub[] = "rid=feedfacecafebeef\ngen=1\nnonce=0000000000000007\n";
    CHECK(fd >= 0 && write(fd, stub, sizeof stub - 1) == (ssize_t)sizeof stub - 1);
    if (fd >= 0)
        close(fd);
    CHECK(refused(root, &r, "has no valid slot"));
    RESTORE();
    poke_slot(root, 1, 0, 100);
    poke_slot(root, 1, 1, 100);
    CHECK(refused(root, &r, "has no valid slot"));
    RESTORE();

    /* a failed save (EFBIG) fails the open for the volume, names the path, and leaves the old state: the next open,
     * once the disk works, opens under the same record */
    struct rlimit old, lim;
    getrlimit(RLIMIT_FSIZE, &old);
    void (*oldsig)(int) = signal(SIGXFSZ, SIG_IGN);
    lim = old;
    lim.rlim_cur = 0; /* no write can reach the file */
    CHECK(setrlimit(RLIMIT_FSIZE, &lim) == 0);
    CHECK(refused(root, &r, "cannot save open generation 2"));
    setrlimit(RLIMIT_FSIZE, &old);
    signal(SIGXFSZ, oldsig);
    struct replfile_state st;
    bool torn;
    CHECK(load_repl(root, 1, &st, &torn) == 0 && !torn && st.open_gen == 1 && st.open_nonce == o.nonce);
    replfile_state_free(&st);
    CHECK(cvolfs_prepare(root, &r, &o2) == 0 && o2.gen == 2);

    /* a v<N>.tmp left by a crash, with any of the files, is removed and the volume made */
    fresh("refuse2", root, sizeof root);
    char t[PATH_MAX + 64];
    snprintf(t, sizeof t, "%s/v2.tmp", root);
    CHECK(mkdir(t, 0755) == 0);
    static const char *left[] = {"META", "REPL", "REPL.tmp", "FNO", "FNO.tmp"};
    for (size_t k = 0; k < sizeof left / sizeof left[0]; k++) {
        char f[PATH_MAX + 96];
        snprintf(f, sizeof f, "%s/%s", t, left[k]);
        FILE *h = fopen(f, "w");
        CHECK(h && fputs("left by a crash\n", h) >= 0);
        if (h)
            fclose(h);
    }
    r = rec(2, 0, 0);
    cap_begin();
    CHECK(cvolfs_prepare(root, &r, &o) == 0);
    cap_end(log, sizeof log);
    CHECK(o.gen == 1 && !exists(root, "v2.tmp") && exists(root, "v2/FNO"));
    printf("  refusals name the path: older copy, another open, another instance or identity, REPL missing, stub or "
           "damaged, a failed save; a leftover v<N>.tmp is removed\n");
}

/* The slot a crash tears is the newest one: the open is unsure and skips a belt */
static void test_unsure(void)
{
    char root[PATH_MAX + 32], log[8192];
    fresh("unsure", root, sizeof root);
    struct cvolfs_open o1, o2, o3;
    struct cvolfs_rec r = rec(1, 0, 0);
    CHECK(cvolfs_prepare(root, &r, &o1) == 0); /* generation 1: the slot of the first save is the newest */
    r = rec(1, 1, o1.nonce);
    CHECK(cvolfs_prepare(root, &r, &o2) == 0 && o2.gen == 2 && !o2.unsure);
    int newest = newest_slot(root, 1);
    poke_slot(root, 1, newest, 80); /* generation 2's slot is damaged: the load falls back to generation 1 */
    /* the group recorded generation 2 (this open's, which the damage hides): the open that follows must not be
     * taken for it, and must not reach a generation the lost save may have carried */
    r = rec(1, 2, o2.nonce);
    cap_begin();
    CHECK(cvolfs_prepare(root, &r, &o3) == 0);
    cap_end(log, sizeof log);
    CHECK(o3.unsure && strstr(log, "was damaged") != NULL && strstr(log, root) != NULL);
    CHECK(o3.gen >= 2 + REPLFILE_OPEN_GEN_BELT && o3.nonce != o2.nonce && o3.nonce != 0);
    struct replfile_state st;
    bool torn;
    CHECK(load_repl(root, 1, &st, &torn) == 0 && !torn && st.open_gen == o3.gen && st.open_nonce == o3.nonce);
    replfile_state_free(&st);
    /* what it saved is a clean REPL: the next open under the record of this one is an ordinary one */
    /* clean to the load, but still unsure: the fence, led and log start it holds are those of the older slot (F2) */
    CHECK(load_repl(root, 1, &st, &torn) == 0 && !torn && st.unsure);
    replfile_state_free(&st);
    r = rec(1, o3.gen, o3.nonce);
    struct cvolfs_open o4;
    CHECK(cvolfs_prepare(root, &r, &o4) == 0 && o4.gen == o3.gen + 1 && o4.unsure);
    CHECK(load_repl(root, 1, &st, &torn) == 0 && !torn && st.unsure);
    replfile_state_free(&st);

    /* the save of the record followed a torn load, then the newest slot rots: the older slot is a belt and one open
     * behind the record. The current copy: accepted, and reported above the record */
    fresh("unsure3", root, sizeof root);
    r = rec(1, 0, 0);
    CHECK(cvolfs_prepare(root, &r, &o1) == 0);
    r = rec(1, 1, o1.nonce);
    CHECK(cvolfs_prepare(root, &r, &o2) == 0);
    poke_slot(root, 1, newest_slot(root, 1), 80);
    r = rec(1, 2, o2.nonce);
    cap_begin();
    CHECK(cvolfs_prepare(root, &r, &o3) == 0 && o3.unsure); /* saved o3.gen = 1 + belt + 1 over the damaged slot */
    cap_end(NULL, 0);
    poke_slot(root, 1, newest_slot(root, 1), 80);
    r = rec(1, o3.gen, o3.nonce);
    cap_begin();
    CHECK(cvolfs_prepare(root, &r, &o4) == 0 && o4.unsure && o4.gen == o3.gen + 1);
    cap_end(NULL, 0);

    /* the older copy is still refused beside it: a directory at generation 1 (both slots whole) under a record at 2,
     * and the same generation under another nonce */
    fresh("unsure2", root, sizeof root);
    r = rec(1, 0, 0);
    CHECK(cvolfs_prepare(root, &r, &o1) == 0);
    r = rec(1, 2, 0x77);
    CHECK(refused(root, &r, "older copy of the directory"));
    r = rec(1, 1, o1.nonce ^ 8);
    CHECK(refused(root, &r, "in another open than the one the metadata group recorded"));

    /* the stale copy whose newest slot is damaged (F1): a copy of v1 taken at generation 3, put back under a record at
     * 10. Its slots hold 2 and 3 (clean copy: refused above); with the newest damaged only 2 is left, and the belt
     * the load adds must not hide that it is far behind. And the same generation as the record under another nonce */
    fresh("unsure4", root, sizeof root);
    char copy[PATH_MAX + 64];
    snprintf(copy, sizeof copy, "%s.copy", root);
    uint64_t g = 0, n = 0;
    struct cvolfs_open o;
    for (int i = 1; i <= 10; i++) {
        r = rec(1, g, n);
        CHECK(cvolfs_prepare(root, &r, &o) == 0);
        g = o.gen, n = o.nonce;
        if (i == 3)
            sh("rm -rf '%s'; cp -a '%s/v1' '%s'", copy, root, copy);
    }
    sh("rm -rf '%s/v1'; cp -a '%s' '%s/v1'", root, copy, root);
    poke_slot(root, 1, newest_slot(root, 1), 80);
    r = rec(1, g, n);
    CHECK(refused(root, &r, "older copy of the directory"));
    /* one open behind (rot of the newest slot of the current copy looks the same): let in, unsure, above the record */
    sh("rm -rf '%s/v1'; cp -a '%s' '%s/v1'", root, copy, root);
    poke_slot(root, 1, newest_slot(root, 1), 80);
    r = rec(1, 3, 0);
    cap_begin();
    CHECK(cvolfs_prepare(root, &r, &o) == 0 && o.unsure && o.gen == 3 + REPLFILE_OPEN_GEN_BELT);
    cap_end(NULL, 0);
    /* a torn slot at the record's generation under another nonce: refused */
    sh("rm -rf '%s/v1'; cp -a '%s' '%s/v1'", root, copy, root);
    poke_slot(root, 1, newest_slot(root, 1), 80);
    r = rec(1, 2, 0x5151);
    CHECK(refused(root, &r, "in another open than the one the metadata group recorded"));
    sh("rm -rf '%s'", copy);
    printf("  a damaged newest REPL slot is unsure, and stays so after the save; the open skips a belt; an older copy "
           "is refused, damaged or not\n");
}

/* ------------------------------------------------------------ power loss */

#define NOPENS 3

static void ack(const char *file, uint64_t gen, uint64_t nonce)
{
    int fd = open(file, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    char line[64];
    int len = snprintf(line, sizeof line, "%" PRIu64 " %" PRIu64 "\n", gen, nonce);
    if (fd >= 0) {
        if (write(fd, line, (size_t)len) != len)
            _exit(2);
        close(fd);
    }
}

/* the last acknowledged open: 0 when none */
static int last_ack(const char *file, uint64_t *gen, uint64_t *nonce)
{
    FILE *f = fopen(file, "r");
    uint64_t g, n;
    int count = 0;
    *gen = *nonce = 0;
    while (f && fscanf(f, "%" SCNu64 " %" SCNu64, &g, &n) == 2)
        *gen = g, *nonce = n, count++;
    if (f)
        fclose(f);
    return count;
}

/* The manager's life: opens of v1, each under the record of the last open it was told about. Every open that
 * returned is acknowledged. A run after a crash goes on from the last acknowledgement. */
static void child_run(const char *dir, const char *ackfile)
{
    uint64_t gen, nonce;
    last_ack(ackfile, &gen, &nonce);
    for (int k = 0; k < NOPENS; k++) {
        struct cvolfs_rec r = rec(1, gen, nonce);
        struct cvolfs_open o;
        if (cvolfs_prepare(dir, &r, &o) != 0)
            _exit(4);
        ack(ackfile, o.gen, o.nonce);
        gen = o.gen, nonce = o.nonce;
    }
    _exit(0);
}

static int run_child(const char *dir, const char *ackfile, int warmup, int seed)
{
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512];
        snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0", getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
        setenv("ASAN_OPTIONS", asan, 1);
        char w[16], sd[16];
        snprintf(w, sizeof w, "%d", warmup);
        snprintf(sd, sizeof sd, "%d", seed);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", dir, 1);
        setenv("CRASHFS_PROB", "1", 1);
        setenv("CRASHFS_WARMUP", w, 1);
        setenv("CRASHFS_AT_EXIT", "1", 1);
        setenv("CRASHFS_TEAR", "1", 1);
        setenv("CRASHFS_SEED", sd, 1);
        int null = open(getenv("TSDB_CVOLFS_CHILDLOG") ? getenv("TSDB_CVOLFS_CHILDLOG") : "/dev/null",
                        O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (null >= 0) {
            dup2(null, 1);
            dup2(null, 2);
        }
        execl(SELF, "test_cvolfs", "--child", dir, ackfile, (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

/* After a power loss: v1 is absent or whole, and what REPL holds is the last acknowledged open, the one in flight, or
 * the acknowledged one under a belt. Then the next prepare, under a record at the last acknowledged open, is accepted
 * and leaves no v1.tmp. */
static void check_after(const char *dir, const char *ackfile, int k)
{
    uint64_t agen, anonce;
    int acks = last_ack(ackfile, &agen, &anonce);
    char m[300];
    if (exists(dir, "v1")) {
        if (!exists(dir, "v1/META") || !exists(dir, "v1/REPL") || !exists(dir, "v1/FNO")) {
            snprintf(m, sizeof m, "step %d: v1 exists without all of META, REPL and FNO", k);
            fail_at(__FILE__, __LINE__, m);
        }
        /* META's content, not only its name: an unsynced file can be empty or torn after the rename (power loss) */
        char mp[PATH_MAX * 2], mb[1024];
        snprintf(mp, sizeof mp, "%s/v1/META", dir);
        FILE *mf = fopen(mp, "r");
        size_t mn = mf ? fread(mb, 1, sizeof mb - 1, mf) : 0;
        if (mf)
            fclose(mf);
        mb[mn] = 0;
        if (!strstr(mb, "version=9\n") || !strstr(mb, "\nvolume=1\n") || !strstr(mb, "\nvolume_uuid=") ||
            !strstr(mb, "\ncluster=") || !strstr(mb, "\nrequires=cluster,replica\n")) {
            snprintf(m, sizeof m, "step %d: v1/META is empty or torn (%zu bytes)", k, mn);
            fail_at(__FILE__, __LINE__, m);
        }
        struct replfile_state st;
        bool torn;
        cap_begin();
        int rc = load_repl(dir, 1, &st, &torn);
        cap_end(NULL, 0);
        if (rc != 0) {
            snprintf(m, sizeof m, "step %d: REPL of the existing v1 does not load (%d)", k, rc);
            fail_at(__FILE__, __LINE__, m);
        } else {
            uint64_t g = st.open_gen % REPLFILE_OPEN_GEN_BELT;
            bool ok = (g == agen && (torn ? st.open_gen == agen + REPLFILE_OPEN_GEN_BELT : st.open_nonce == anonce)) ||
                      (g == agen + 1 && !torn && st.open_nonce != 0 && st.open_nonce != anonce) ||
                      (agen == 0 && g == 0 && !torn && st.open_nonce == 0); /* made, not opened yet */
            if (!ok) {
                snprintf(m, sizeof m,
                         "step %d: REPL holds generation %" PRIu64 " nonce %" PRIx64 "%s with the open %" PRIu64
                         " nonce %" PRIx64 " acknowledged",
                         k, st.open_gen, st.open_nonce, torn ? " (torn)" : "", agen, anonce);
                fail_at(__FILE__, __LINE__, m);
            }
            replfile_state_free(&st);
        }
    } else if (acks) {
        snprintf(m, sizeof m, "step %d: v1 is gone with %d opens acknowledged", k, acks);
        fail_at(__FILE__, __LINE__, m);
    }
    struct cvolfs_rec r = rec(1, agen, anonce);
    struct cvolfs_open o;
    cap_begin();
    int rc = cvolfs_prepare(dir, &r, &o);
    cap_end(NULL, 0);
    if (rc != 0) {
        snprintf(m, sizeof m, "step %d: the next prepare under the acknowledged open (%" PRIu64 ") was refused", k, agen);
        fail_at(__FILE__, __LINE__, m);
        return;
    }
    struct replfile_state st;
    bool torn;
    CHECK(!exists(dir, "v1.tmp"));
    CHECK(o.gen > agen && o.nonce != 0);
    CHECK(load_repl(dir, 1, &st, &torn) == 0 && !torn && st.open_gen == o.gen && st.open_nonce == o.nonce);
    replfile_state_free(&st);
}

static void test_power_loss(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  power loss: skipped (no %s)\n", SHIM);
        return;
    }
    char dir[PATH_MAX + 32], ackf[PATH_MAX + 32];
    snprintf(ackf, sizeof ackf, "%s/pl.ack", S);
    int crashes = 0, again = 0;
    bool finished = false;
    for (int k = 0; k < 600 && !finished; k++) {
        fresh("pl-dir", dir, sizeof dir);
        unlink(ackf);
        int code = run_child(dir, ackf, k, k + 1);
        CHECK(code == 99 || code == 0);
        if (code != 99 && code != 0)
            break;
        crashes += code == 99;
        finished = code == 0;
        if (code == 99 && k % 2 == 0) {
            /* the resumed run loses power too, at a step of its own: through the removal of a v1.tmp, the make, an open */
            int c2 = run_child(dir, ackf, (k * 7) % 31, k + 1000);
            /* crashfs's unlink moves a file aside until the process ends (to undo it at a crash), so the rmdir of a
             * leftover v1.tmp that follows its unlinks fails there with ENOTEMPTY: that run ends with 4 and the
             * leftover. So a power loss inside "remove a leftover v1.tmp, then make v1" is not exercised under the
             * shim (a loss at an unlink, the rmdir or the make that follows it); check_after's prepare, on the real
             * file system, is what shows the leftover removed and the make done */
            char t1[PATH_MAX + 40];
            snprintf(t1, sizeof t1, "%s/v1.tmp", dir);
            CHECK(c2 == 99 || c2 == 0 || (c2 == 4 && access(t1, F_OK) == 0));
            again++;
        }
        check_after(dir, ackf, k);
    }
    CHECK(finished && crashes > 30);
    printf("  power lost at each of %d steps of making v1 and %d opens (%d resumed runs lost it again): v1 absent or "
           "whole, REPL the acknowledged open or the one in flight, the next open accepted, no v1.tmp left\n", crashes,
           NOPENS, again);
}

static void set_mutant(const char *m)
{
    if (!strcmp(m, "nosync"))
        replfile_test_mutant = REPLFILE_MUT_NOSYNC;
    else if (!strcmp(m, "nodirsync"))
        cvolfs_test_mutant = CVOLFS_MUT_NODIRSYNC;
    else if (!strcmp(m, "nononce"))
        cvolfs_test_mutant = CVOLFS_MUT_NONONCE;
    else if (!strcmp(m, "trustunsure"))
        cvolfs_test_mutant = CVOLFS_MUT_TRUSTUNSURE;
    else if (!strcmp(m, "tornstale"))
        cvolfs_test_mutant = CVOLFS_MUT_TORNSTALE;
    else if (!strcmp(m, "samerid"))
        cvolfs_test_mutant = CVOLFS_MUT_SAMERID;
    else {
        fprintf(stderr, "unknown mutant %s\n", m);
        exit(2);
    }
    setenv("TSDB_CVOLFS_MUTANT", m, 1); /* the processes the shim kills run the same bug */
}

int main(int argc, char **argv)
{
    if (argc >= 4 && strcmp(argv[1], "--child") == 0) {
        if (getenv("TSDB_CVOLFS_MUTANT"))
            set_mutant(getenv("TSDB_CVOLFS_MUTANT"));
        child_run(argv[2], argv[3]);
        return 0;
    }
    const char *scratch = "/tmp", *mutant = NULL;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mutant") == 0 && i + 1 < argc)
            mutant = argv[++i];
        else
            scratch = argv[i];
    }
    char tmpl[PATH_MAX];
    snprintf(tmpl, sizeof tmpl, "%s/test-cvolfs.XXXXXX", scratch);
    if (!mkdtemp(tmpl)) {
        perror("mkdtemp");
        return 1;
    }
    snprintf(S, sizeof S, "%s", tmpl);
    if (!realpath(argv[0], SELF))
        snprintf(SELF, sizeof SELF, "%s", argv[0]);
    char *slash = strrchr(SELF, '/');
    snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", slash ? (int)(slash - SELF) : 1, slash ? SELF : ".");
    if (mutant)
        set_mutant(mutant);
    if (!mutant) {
        test_basic();
        test_refusals();
        test_unsure();
    } else if (cvolfs_test_mutant == CVOLFS_MUT_SAMERID) {
        test_basic();
        test_refusals();
    } else if (cvolfs_test_mutant == CVOLFS_MUT_NONONCE || cvolfs_test_mutant == CVOLFS_MUT_TRUSTUNSURE ||
               cvolfs_test_mutant == CVOLFS_MUT_TORNSTALE) {
        test_basic();
        test_unsure();
    }
    if (!mutant || replfile_test_mutant == REPLFILE_MUT_NOSYNC || cvolfs_test_mutant == CVOLFS_MUT_NODIRSYNC ||
        cvolfs_test_mutant == CVOLFS_MUT_NONONCE)
        test_power_loss();
    sh("rm -rf '%s'", S);
    if (failures)
        fprintf(stderr, "%d cvolfs checks FAILED\n", failures);
    else
        printf("cvolfs tests passed\n");
    return failures ? 1 : 0;
}
