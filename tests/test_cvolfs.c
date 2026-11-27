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
 *  - setpower: power lost at every step of a replica's removal (v<N> renamed v<N>.removed-<rid>) and of a prepare that
 *    sets a v<N> of another rid aside (stale, or lost under a reset) and makes the fresh one: the old directory is v<N>
 *    or set aside, never neither, an acknowledged step holds, the next start completes the change.
 *
 *   ./build/test_cvolfs [scratch dir]
 *   ./build/test_cvolfs --mutant nosync|nodirsync|nononce|trustunsure|tornstale|samerid|removenodirsync [dir]  must fail
 *   ./build/test_cvolfs --child DIR ACKFILE      (internal: a process the shim kills)
 *   ./build/test_cvolfs --setchild DIR ACKFILE removed|stale|lost      (internal: the same for setpower)
 */
#define _GNU_SOURCE
#include "cluster.h"
#include "cvolfs.h"
#include "cvolset.h"
#include "disk.h"
#include "internal.h"
#include "meta.h"
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
        fprintf(f, "version=%d\npartition_ms=86400000\ngranule_rows=8192\npartition_classes=1\nsomething=else\n",
                FORMAT_VERSION);
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
    char vline[32];
    snprintf(vline, sizeof vline, "version=%d\n", FORMAT_VERSION);
    CHECK(strstr(meta, vline) && strstr(meta, "partition_ms=86400000\n") && strstr(meta, "\nvolume=1\n") &&
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
    /* the directory is at generation 1; copy it to start each case from it (never under v1.snap or v1.old: the
     * prepare's snapinst_leftovers takes those for what an install left and removes them) */
    char snap[PATH_MAX + 32];
    snprintf(snap, sizeof snap, "%s/v1.saved", root);
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
    /* another rid that the group never saw opened (an ADD after a REMOVE while this server was down): the old directory
     * is set aside as stale and a fresh one made; the refusal above holds only for a rid recorded as opened here */
    {
        struct cvolfs_rec n = rec(1, 0, 0);
        char aside[64];
        snprintf(aside, sizeof aside, "v1.stale-%016" PRIx64, n.rid);
        n.rid++;
        CHECK(cvolfs_prepare(root, &n, &o2) == 0 && o2.gen == 1);
        CHECK(exists(root, aside));
        CHECK(sh("rm -rf '%s/%s'", root, aside) == 0);
        RESTORE();
    }

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

static int run_child(const char *dir, const char *ackfile, int warmup, int seed, const char *kind)
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
        if (kind)
            execl(SELF, "test_cvolfs", "--setchild", dir, ackfile, kind, (char *)NULL);
        else
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
        char vline[32];
        snprintf(vline, sizeof vline, "version=%d\n", FORMAT_VERSION);
        if (!strstr(mb, vline) || !strstr(mb, "\nvolume=1\n") || !strstr(mb, "\nvolume_uuid=") ||
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
        int code = run_child(dir, ackf, k, k + 1, NULL);
        CHECK(code == 99 || code == 0);
        if (code != 99 && code != 0)
            break;
        crashes += code == 99;
        finished = code == 0;
        if (code == 99 && k % 2 == 0) {
            /* the resumed run loses power too, at a step of its own: through the removal of a v1.tmp, the make, an open */
            int c2 = run_child(dir, ackf, (k * 7) % 31, k + 1000, NULL);
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

/* setpower (r4-setchg): power lost at every step of the replica set changes' directory moves. "removed": the removal
 * (cvolset_setaside "removed", what cvolset_mgr's remove_dir does once the copy shows the removal); "stale": the
 * prepare of a record naming another rid (the old v1 set aside as v1.stale-<rid>, a fresh v1 made and opened); "lost":
 * the same under a replica reset (v1.lost-<rid>). The old replica's directory is there after the loss as v1 or under
 * its set-aside name, never neither and never both; a v1 is whole; an acknowledged step holds (the removal's rename,
 * the fresh replica's open); and the next start (the removal again, or the prepare under the acknowledged open) ends
 * with the change made. */
#define RID_A UINT64_C(0x0a1b2c3d4e5f6071) /* the old replica; rec()'s rid is the new one */

static void set_child(const char *dir, const char *ackfile, const char *kind)
{
    char to[PATH_MAX + 64];
    if (!strcmp(kind, "removed")) {
        if (exists(dir, "v1") && cvolset_setaside(dir, 1, "removed", RID_A, to, sizeof to) != 0)
            _exit(4);
        ack(ackfile, 1, 0);
        exit(0); /* not _exit: the shim's power loss at exit is the check that the rename was made durable */
    }
    uint64_t gen, nonce;
    last_ack(ackfile, &gen, &nonce);
    for (int k = 0; k < 2; k++) {
        struct cvolfs_rec r = rec(1, gen, nonce);
        if (!strcmp(kind, "lost"))
            cvolset_note(1, CVS_RESET, r.rid); /* the push that names the new rid, again at every start */
        struct cvolfs_open o;
        if (cvolfs_prepare(dir, &r, &o) != 0)
            _exit(4);
        ack(ackfile, o.gen, o.nonce);
        gen = o.gen, nonce = o.nonce;
    }
    exit(0);
}

static bool whole_at(const char *dir, const char *v, uint64_t *rid)
{
    char a[PATH_MAX + 64], b[PATH_MAX + 64], c[PATH_MAX + 64], vd[PATH_MAX * 2];
    snprintf(a, sizeof a, "%s/META", v), snprintf(b, sizeof b, "%s/REPL", v), snprintf(c, sizeof c, "%s/FNO", v);
    snprintf(vd, sizeof vd, "%s/%s", dir, v);
    return exists(dir, a) && exists(dir, b) && exists(dir, c) && cvolset_rid_of(vd, rid) == 0;
}

static void check_set_after(const char *dir, const char *ackfile, const char *kind, int k)
{
    char aside[64], m[400];
    snprintf(aside, sizeof aside, "v1.%s-%016" PRIx64, kind, RID_A);
    uint64_t agen, anonce, rid = 0, arid = 0;
    int acks = last_ack(ackfile, &agen, &anonce);
    bool v1 = exists(dir, "v1"), v1whole = v1 && whole_at(dir, "v1", &rid), v1old = v1whole && rid == RID_A;
    bool as = exists(dir, aside);
    if (v1 && !v1whole) {
        snprintf(m, sizeof m, "%s, step %d: v1 exists without all of META, REPL (with a rid) and FNO", kind, k);
        fail_at(__FILE__, __LINE__, m);
    }
    if (v1whole && rid != RID_A && rid != rec(1, 0, 0).rid) {
        snprintf(m, sizeof m, "%s, step %d: v1 holds replica %016" PRIx64 ", neither the old nor the new", kind, k, rid);
        fail_at(__FILE__, __LINE__, m);
    }
    if (v1old == as) {
        snprintf(m, sizeof m, "%s, step %d: the old replica's directory is %s", kind, k,
                 as ? "both v1 and set aside" : "neither v1 nor set aside");
        fail_at(__FILE__, __LINE__, m);
    }
    if (as && !(whole_at(dir, aside, &arid) && arid == RID_A)) {
        snprintf(m, sizeof m, "%s, step %d: %s is not the old replica whole", kind, k, aside);
        fail_at(__FILE__, __LINE__, m);
    }
    if (acks && (!strcmp(kind, "removed") ? v1 : v1old)) {
        snprintf(m, sizeof m, "%s, step %d: acknowledged, and v1 still holds the old replica after the power loss", kind, k);
        fail_at(__FILE__, __LINE__, m);
    }
    /* the next start */
    char to[PATH_MAX + 64];
    if (!strcmp(kind, "removed")) {
        if (v1) {
            cap_begin();
            CHECK(cvolset_setaside(dir, 1, "removed", RID_A, to, sizeof to) == 0);
            cap_end(NULL, 0);
        }
        CHECK(!exists(dir, "v1") && exists(dir, aside));
        return;
    }
    struct cvolfs_rec r = rec(1, agen, anonce);
    if (!strcmp(kind, "lost"))
        cvolset_note(1, CVS_RESET, r.rid);
    struct cvolfs_open o;
    cap_begin();
    int rc = cvolfs_prepare(dir, &r, &o);
    cap_end(NULL, 0);
    if (rc != 0) {
        snprintf(m, sizeof m, "%s, step %d: the next start's prepare under the acknowledged open was refused", kind, k);
        fail_at(__FILE__, __LINE__, m);
        return;
    }
    cvolset_noted(1, CVS_RESET, NULL, 1); /* none left for the next round */
    CHECK(!exists(dir, "v1.tmp"));
    CHECK(whole_at(dir, "v1", &rid) && rid == r.rid && o.gen > agen);
    CHECK(whole_at(dir, aside, &arid) && arid == RID_A);
}

static void test_set_power(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  set power loss: skipped (no %s)\n", SHIM);
        return;
    }
    static const char *const kinds[] = {"removed", "stale", "lost"};
    char dir[PATH_MAX + 32], ackf[PATH_MAX + 32];
    snprintf(ackf, sizeof ackf, "%s/sp.ack", S);
    for (int i = 0; i < 3; i++) {
        int crashes = 0, whole = 0;
        bool finished = false;
        /* a loss at each step in turn, then 16 runs that end whole and lose power at their exit (a random subset of
         * what was not made durable undone: an unsynced rename is undone in about half of them) */
        for (int k = 0; k < 300 && whole < 16; k++) {
            int warm = finished ? 1000000 : k;
            fresh("sp-dir", dir, sizeof dir);
            unlink(ackf);
            struct cvolfs_rec ra = rec(1, 0, 0);
            ra.rid = RID_A;
            struct cvolfs_open o;
            cap_begin();
            int rc = cvolfs_prepare(dir, &ra, &o);
            cap_end(NULL, 0);
            CHECK(rc == 0);
            int code = run_child(dir, ackf, warm, k + 1, kinds[i]);
            CHECK(code == 99 || code == 0);
            if (code != 99 && code != 0)
                break;
            crashes += code == 99;
            if (code == 0)
                finished = true, whole++;
            check_set_after(dir, ackf, kinds[i], k);
        }
        CHECK(finished && whole == 16 && crashes >= 2);
        printf("  set power loss, %s: lost at each of %d steps and at the exit of %d whole runs: the old replica's "
               "directory v1 or set aside, never neither, the acknowledged step held, the next start made the change\n",
               kinds[i], crashes, whole);
    }
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
    else if (!strcmp(m, "removenodirsync"))
        cvolfs_test_mutant = CVOLFS_MUT_REMOVENODIRSYNC;
    else if (!strcmp(m, "cvolfirstdisk"))
        cvolfs_test_mutant = CVOLFS_MUT_FIRSTDISK;
    else if (!strcmp(m, "cvoldupok"))
        cvolfs_test_mutant = CVOLFS_MUT_DUPOK;
    else {
        fprintf(stderr, "unknown mutant %s\n", m);
        exit(2);
    }
    setenv("TSDB_CVOLFS_MUTANT", m, 1); /* the processes the shim kills run the same bug */
}

/* ------------------------------------------------------------ multi-disk (R13 E48, E51, D14) */

static void fresh_empty(const char *name, char *dir, size_t cap)
{
    snprintf(dir, cap, "%s/%s", S, name);
    sh("rm -rf '%s'; mkdir -p '%s'", dir, dir);
}

static struct cvolfs_disk mk_disk(const char *path, int cls, uint8_t tag)
{
    struct cvolfs_disk d;
    memset(&d, 0, sizeof d);
    snprintf(d.path, sizeof d.path, "%s", path);
    for (int i = 0; i < 16; i++)
        d.store_id[i] = (uint8_t)(tag + i);
    d.disk_class = cls;
    d.state = META_STORE_UP;
    d.present = 1;
    return d;
}

static void test_two_disks(void)
{
    char d0[PATH_MAX + 32], d1[PATH_MAX + 32], dh[PATH_MAX + 32], log[8192];
    fresh("two-d0", d0, sizeof d0);
    fresh_empty("two-d1", d1, sizeof d1); /* second disk has NO root META: reads format lines from d0/META */
    fresh_empty("two-dh", dh, sizeof dh);

    struct cvolfs_disk disks[2];
    disks[0] = mk_disk(d0, DISK_CLASS_NVME, 0x10);
    disks[1] = mk_disk(d1, DISK_CLASS_NVME, 0x20);
    disks[0].have_usage = disks[1].have_usage = 1;
    disks[0].size_bytes = disks[1].size_bytes = 1000000;
    disks[0].used_bytes = disks[1].used_bytes = 100000; /* 10% */

    /* 1. Make v1 -> d0, then with d0 holding 1 replica, make v2 -> MUST land on d1 (catches cvolfirstdisk). */
    struct cvolfs_rec r1 = rec(1, 0, 0), r2 = rec(2, 0, 0);
    struct cvolfs_open o1, o2;
    cap_begin();
    CHECK(cvolfs_prepare_disks(d0, disks, 2, &r1, &o1) == 0);
    cap_end(log, sizeof log);
    CHECK(strcmp(o1.disk_root, d0) == 0 && memcmp(o1.store_id, disks[0].store_id, 16) == 0);
    CHECK(exists(d0, "v1/REPL") && !exists(d1, "v1"));

    disks[0].nreplicas = 1;
    cap_begin();
    CHECK(cvolfs_prepare_disks(d0, disks, 2, &r2, &o2) == 0);
    cap_end(log, sizeof log);
    CHECK(strcmp(o2.disk_root, d1) == 0 && memcmp(o2.store_id, disks[1].store_id, 16) == 0);
    CHECK(exists(d1, "v2/META") && exists(d1, "v2/REPL") && exists(d1, "v2/FNO") && !exists(d0, "v2"));

    /* 2. Restart (zero in-memory counts): v2 is found on d1, generation raised there; leftover v2.tmp on d0 cleaned. */
    disks[0].nreplicas = disks[1].nreplicas = 0;
    char tmp0[PATH_MAX + 64];
    snprintf(tmp0, sizeof tmp0, "%s/v2.tmp", d0);
    CHECK(mkdir(tmp0, 0755) == 0);
    r2 = rec(2, o2.gen, o2.nonce);
    struct cvolfs_open o2b;
    cap_begin();
    CHECK(cvolfs_prepare_disks(d0, disks, 2, &r2, &o2b) == 0);
    cap_end(log, sizeof log);
    CHECK(o2b.gen == 2 && strcmp(o2b.disk_root, d1) == 0 && memcmp(o2b.store_id, disks[1].store_id, 16) == 0);
    CHECK(!exists(d0, "v2") && !exists(d0, "v2.tmp"));

    /* 3. Duplicate v2 on BOTH d0 and d1 -> MUST refuse naming both paths (catches cvoldupok). */
    CHECK(sh("cp -a '%s/v2' '%s/v2'", d1, d0) == 0);
    r2 = rec(2, o2b.gen, o2b.nonce);
    struct cvolfs_open o2c;
    cap_begin();
    int dup_rc = cvolfs_prepare_disks(d0, disks, 2, &r2, &o2c);
    cap_end(log, sizeof log);
    CHECK(dup_rc != 0);
    CHECK(strstr(log, "duplicate") != NULL && strstr(log, d0) != NULL && strstr(log, d1) != NULL);
    /* Remove the duplicate on d0 -> v2 opens cleanly on d1 again. */
    CHECK(sh("rm -rf '%s/v2'", d0) == 0);
    cap_begin();
    CHECK(cvolfs_prepare_disks(d0, disks, 2, &r2, &o2c) == 0 && o2c.gen == 3 && strcmp(o2c.disk_root, d1) == 0);
    cap_end(NULL, 0);

    /* 3b. Existing v2 on a DOWN disk is NOT invisible: refuse naming d1/v2 and never create a duplicate on d0. */
    disks[1].state = META_STORE_DOWN;
    r2 = rec(2, 0, 0);
    cap_begin();
    int down_rc0 = cvolfs_prepare_disks(d0, disks, 2, &r2, &o2c);
    cap_end(log, sizeof log);
    CHECK(down_rc0 != 0 && strstr(log, d1) != NULL && !exists(d0, "v2"));
    r2 = rec(2, o2c.gen, o2c.nonce);
    cap_begin();
    int down_rc1 = cvolfs_prepare_disks(d0, disks, 2, &r2, &o2c);
    cap_end(log, sizeof log);
    CHECK(down_rc1 != 0 && strstr(log, d1) != NULL && !exists(d0, "v2"));
    disks[1].state = META_STORE_UP;

    /* 4. E51 placement rules: WAL bytes/s weighting, young_move_pct, nvme before ssd, log_on_hdd. */
    /* 4a. WAL bytes/s weighting: 1 hot replica (10 MB/s) on d0 vs 2 idle replicas on d1 -> picks d1. */
    disks[0].nreplicas = 1, disks[0].wal_bps = 10.0 * 1024 * 1024;
    disks[1].nreplicas = 2, disks[1].wal_bps = 0.0;
    CHECK(cvolfs_pick_disk(disks, 2, 80, 0) == 1);

    /* 4b. Projected use >= young_move_pct (85% >= 80%): skipped even with 0 replicas in favor of d1 at 20%. */
    disks[0].nreplicas = 0, disks[0].wal_bps = 0.0, disks[0].used_bytes = 850000;
    disks[1].nreplicas = 3, disks[1].wal_bps = 5.0 * 1024 * 1024, disks[1].used_bytes = 200000;
    CHECK(cvolfs_pick_disk(disks, 2, 80, 0) == 1);
    /* When ALL eligible disks are at or above young_move_pct, placement MUST refuse (-1) and create no v<N>. */
    disks[1].used_bytes = 820000; /* 82% >= 80% */
    if (cvolfs_test_mutant != CVOLFS_MUT_FIRSTDISK) {
        cap_begin();
        CHECK(cvolfs_pick_disk(disks, 2, 80, 0) == -1);
        cap_end(log, sizeof log);
        CHECK(strstr(log, "young_move_pct") != NULL);
        struct cvolfs_rec r4 = rec(4, 0, 0);
        r4.young_move_pct = 80;
        struct cvolfs_open o4;
        cap_begin();
        CHECK(cvolfs_prepare_disks(d0, disks, 2, &r4, &o4) != 0);
        cap_end(NULL, 0);
        CHECK(!exists(d0, "v4") && !exists(d1, "v4"));
    }

    /* 4c. nvme before ssd on tie: d0 is ssd (0 replicas), d1 is nvme (0 replicas) -> picks d1. */
    disks[0] = mk_disk(d0, DISK_CLASS_SSD, 0x10);
    disks[1] = mk_disk(d1, DISK_CLASS_NVME, 0x20);
    disks[0].have_usage = disks[1].have_usage = 1;
    disks[0].size_bytes = disks[1].size_bytes = 1000000;
    disks[0].used_bytes = disks[1].used_bytes = 100000;
    CHECK(cvolfs_pick_disk(disks, 2, 80, 0) == 1);

    /* 4d. log_on_hdd: HDD only allowed when NO fast disk is UP. */
    struct cvolfs_disk hdd_only[1] = {mk_disk(dh, DISK_CLASS_HDD, 0x30)};
    hdd_only[0].have_usage = 1, hdd_only[0].size_bytes = 1000000, hdd_only[0].used_bytes = 100000;
    CHECK(cvolfs_pick_disk(hdd_only, 1, 80, 0) == -1);
    CHECK(cvolfs_pick_disk(hdd_only, 1, 80, 1) == 0);
    /* Make v3 on HDD when log_on_hdd=1 and no fast disk is UP; refused when log_on_hdd=0. */
    struct cvolfs_rec r3 = rec(3, 0, 0);
    struct cvolfs_open o3;
    cap_begin();
    CHECK(cvolfs_prepare_disks(d0, hdd_only, 1, &r3, &o3) != 0);
    cap_end(NULL, 0);
    r3.log_on_hdd = 1;
    cap_begin();
    CHECK(cvolfs_prepare_disks(d0, hdd_only, 1, &r3, &o3) == 0 && strcmp(o3.disk_root, dh) == 0);
    cap_end(NULL, 0);
    /* With an UP fast disk beside an HDD (even with log_on_hdd=1 and 5 replicas on fast disk), HDD is never picked. */
    struct cvolfs_disk mix[2] = {hdd_only[0], mk_disk(d1, DISK_CLASS_SSD, 0x20)};
    mix[1].have_usage = 1, mix[1].size_bytes = 1000000, mix[1].used_bytes = 100000, mix[1].nreplicas = 5;
    CHECK(cvolfs_pick_disk(mix, 2, 80, 1) == 1);
    /* If that fast disk is DOWN and log_on_hdd=1, HDD IS picked; if log_on_hdd=0, refused. */
    mix[1].state = META_STORE_DOWN;
    CHECK(cvolfs_pick_disk(mix, 2, 80, 0) == -1);
    CHECK(cvolfs_pick_disk(mix, 2, 80, 1) == 0);
    printf("  two disks: E51 placement (replicas, WAL bps, young_move_pct, nvme>ssd, log_on_hdd), restart/find, "
           "tmp cleanup, duplicate refused naming both paths\n");
}

static void child2_run(const char *d0, const char *d1, const char *ackfile)
{
    uint64_t gen, nonce;
    last_ack(ackfile, &gen, &nonce);
    struct cvolfs_disk disks[2] = {mk_disk(d0, DISK_CLASS_NVME, 0x10), mk_disk(d1, DISK_CLASS_NVME, 0x20)};
    disks[0].have_usage = disks[1].have_usage = 1;
    disks[0].size_bytes = disks[1].size_bytes = 1000000;
    disks[0].used_bytes = disks[1].used_bytes = 100000;
    disks[0].nreplicas = 1; /* force E51 to place new v1 on second disk d1 */
    for (int k = 0; k < NOPENS; k++) {
        struct cvolfs_rec r = rec(1, gen, nonce);
        struct cvolfs_open o;
        if (cvolfs_prepare_disks(d0, disks, 2, &r, &o) != 0)
            _exit(4);
        ack(ackfile, o.gen, o.nonce);
        gen = o.gen, nonce = o.nonce;
    }
    exit(0);
}

static void set_child2(const char *d1, const char *ackfile)
{
    char to[PATH_MAX + 64];
    if (exists(d1, "v1") && cvolset_setaside(d1, 1, "removed", RID_A, to, sizeof to) != 0)
        _exit(4);
    ack(ackfile, 1, 0);
    exit(0);
}

static int run_child2(const char *d0, const char *d1, const char *ackfile, int warmup, int seed, int is_remove)
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
        setenv("CRASHFS_ROOT", d1, 1);
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
        if (is_remove)
            execl(SELF, "test_cvolfs", "--setchild2", d1, ackfile, (char *)NULL);
        else
            execl(SELF, "test_cvolfs", "--child2", d0, d1, ackfile, (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

/* crashfs power loss at every step of make and remove on a second disk (d1, no META of its own). */
static void test_second_disk_power(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  second-disk power loss: skipped (no %s)\n", SHIM);
        return;
    }
    char d0[PATH_MAX + 32], d1[PATH_MAX + 32], ackf[PATH_MAX + 32];
    snprintf(ackf, sizeof ackf, "%s/pl2.ack", S);
    int crashes = 0, whole = 0;
    bool finished = false;
    for (int k = 0; k < 600 && whole < 8; k++) {
        int warm = finished ? 1000000 : k;
        fresh("pl2-d0", d0, sizeof d0);
        fresh_empty("pl2-d1", d1, sizeof d1);
        unlink(ackf);
        int code = run_child2(d0, d1, ackf, warm, k + 1, 0);
        CHECK(code == 99 || code == 0);
        if (code != 99 && code != 0)
            break;
        crashes += code == 99;
        if (code == 0)
            finished = true, whole++;
        CHECK(!exists(d0, "v1"));
        /* Verify d1/v1 state and recover via multi-disk cvolfs_prepare_disks. */
        uint64_t agen, anonce;
        int acks = last_ack(ackf, &agen, &anonce);
        if (exists(d1, "v1")) {
            CHECK(exists(d1, "v1/META") && exists(d1, "v1/REPL") && exists(d1, "v1/FNO"));
        } else {
            CHECK(!acks);
        }
        struct cvolfs_disk disks[2] = {mk_disk(d0, DISK_CLASS_NVME, 0x10), mk_disk(d1, DISK_CLASS_NVME, 0x20)};
        disks[0].have_usage = disks[1].have_usage = 1;
        disks[0].size_bytes = disks[1].size_bytes = 1000000;
        disks[0].used_bytes = disks[1].used_bytes = 100000;
        disks[0].nreplicas = 1;
        struct cvolfs_rec r = rec(1, agen, anonce);
        struct cvolfs_open o;
        cap_begin();
        int rc = cvolfs_prepare_disks(d0, disks, 2, &r, &o);
        cap_end(NULL, 0);
        CHECK(rc == 0 && strcmp(o.disk_root, d1) == 0 && o.gen > agen && !exists(d1, "v1.tmp") && !exists(d0, "v1"));
    }
    CHECK(finished && crashes > 20 && whole == 8);
    printf("  second-disk power loss (make): lost at %d steps + %d exit runs: d1/v1 absent or whole, next prepare on d1\n",
           crashes, whole);

    /* Remove on second disk under crashfs at every step + 16 whole runs with exit power loss. */
    crashes = 0, whole = 0, finished = false;
    for (int k = 0; k < 300 && whole < 16; k++) {
        int warm = finished ? 1000000 : k;
        fresh("sp2-d0", d0, sizeof d0);
        fresh_empty("sp2-d1", d1, sizeof d1);
        unlink(ackf);
        struct cvolfs_disk disks[2] = {mk_disk(d0, DISK_CLASS_NVME, 0x10), mk_disk(d1, DISK_CLASS_NVME, 0x20)};
        disks[0].have_usage = disks[1].have_usage = 1;
        disks[0].size_bytes = disks[1].size_bytes = 1000000;
        disks[0].used_bytes = disks[1].used_bytes = 100000;
        disks[0].nreplicas = 1;
        struct cvolfs_rec ra = rec(1, 0, 0);
        ra.rid = RID_A;
        struct cvolfs_open o;
        cap_begin();
        CHECK(cvolfs_prepare_disks(d0, disks, 2, &ra, &o) == 0 && strcmp(o.disk_root, d1) == 0);
        cap_end(NULL, 0);
        int code = run_child2(d0, d1, ackf, warm, k + 1, 1);
        CHECK(code == 99 || code == 0);
        if (code != 99 && code != 0)
            break;
        crashes += code == 99;
        if (code == 0)
            finished = true, whole++;
        check_set_after(d1, ackf, "removed", k);
    }
    CHECK(finished && whole == 16 && crashes >= 2);
    printf("  second-disk power loss (remove): lost at %d steps + %d exit runs: d1/v1 or d1/v1.removed-<rid>, never neither\n",
           crashes, whole);
}

/* unnamed (review r4-iface F2): the volume manager reports a volume as no longer named (CVS_UNNAMED, behind which
 * r4-setchg sets a directory aside) only from a push that shows it: a push the cluster thread cut at CLUSTER_REPORT_VOLS
 * (the lowest numbers) says nothing of the numbers past its last record */
static void test_unnamed(void)
{
    static struct cluster_vol_rec recs[CLUSTER_REPORT_VOLS];
    for (uint32_t i = 0; i < CLUSTER_REPORT_VOLS; i++)
        recs[i].number = 2 * (i + 1); /* 2, 4, ..., 512 */
    const uint32_t full = CLUSTER_REPORT_VOLS, last = 2 * CLUSTER_REPORT_VOLS;
    CHECK(!cvolset_unnamed(recs, full, 4));        /* named */
    CHECK(cvolset_unnamed(recs, full, 3));         /* below the last of a full push: absent means not named */
    CHECK(!cvolset_unnamed(recs, full, last + 1)); /* past the last of a full push: perhaps cut, never "unnamed" */
    CHECK(!cvolset_unnamed(recs, full, 900));
    CHECK(cvolset_unnamed(recs, 10, 900));         /* a push below the limit is whole */
    CHECK(cvolset_unnamed(recs, 0, 1));            /* an empty push names nothing */
    CHECK(!cvolset_unnamed(recs, 10, 20));
    printf("  unnamed: a cut push says nothing past its last record\n");
}

/* R9 (D6): v<N>.fault-<rid> set aside for a faulted replica is recognized at prepare: never opened, never deleted
 * automatically, logs one clear line naming its path, and does not block making a fresh v<N>. */
static void test_fault_aside(void)
{
    char root[PATH_MAX + 32], fdir[PATH_MAX + 96], ffile[PATH_MAX + 128], log[8192];
    fresh("fault-aside", root, sizeof root);
    snprintf(fdir, sizeof fdir, "%s/v1.fault-0123456789abcdef", root);
    CHECK(mkdir(fdir, 0755) == 0);
    snprintf(ffile, sizeof ffile, "%s/evidence.txt", fdir);
    FILE *f = fopen(ffile, "w");
    CHECK(f && fputs("keep me\n", f) >= 0);
    if (f)
        fclose(f);
    struct cvolfs_rec r = rec(1, 0, 0);
    struct cvolfs_open o;
    cap_begin();
    CHECK(cvolfs_prepare(root, &r, &o) == 0);
    cap_end(log, sizeof log);
    CHECK(o.gen == 1 && exists(root, "v1/REPL"));
    CHECK(exists(root, "v1.fault-0123456789abcdef") && exists(root, "v1.fault-0123456789abcdef/evidence.txt"));
    CHECK(strstr(log, fdir) != NULL && strstr(log, "never deleted automatically") != NULL);
    /* A second prepare of v1 must not log the same v1.fault-<rid> path a second time. */
    r = rec(1, o.gen, o.nonce);
    cap_begin();
    CHECK(cvolfs_prepare(root, &r, &o) == 0);
    cap_end(log, sizeof log);
    CHECK(strstr(log, fdir) == NULL);
    CHECK(exists(root, "v1.fault-0123456789abcdef/evidence.txt"));
    printf("  fault-aside: v1.fault-<rid> logged once with path, never opened or deleted automatically\n");
}

int main(int argc, char **argv)
{
    if (argc >= 4 && strcmp(argv[1], "--child") == 0) {
        if (getenv("TSDB_CVOLFS_MUTANT"))
            set_mutant(getenv("TSDB_CVOLFS_MUTANT"));
        child_run(argv[2], argv[3]);
        return 0;
    }
    if (argc >= 5 && strcmp(argv[1], "--setchild") == 0) {
        if (getenv("TSDB_CVOLFS_MUTANT"))
            set_mutant(getenv("TSDB_CVOLFS_MUTANT"));
        set_child(argv[2], argv[3], argv[4]);
        return 0;
    }
    if (argc >= 5 && strcmp(argv[1], "--child2") == 0) {
        if (getenv("TSDB_CVOLFS_MUTANT"))
            set_mutant(getenv("TSDB_CVOLFS_MUTANT"));
        child2_run(argv[2], argv[3], argv[4]);
        return 0;
    }
    if (argc >= 4 && strcmp(argv[1], "--setchild2") == 0) {
        if (getenv("TSDB_CVOLFS_MUTANT"))
            set_mutant(getenv("TSDB_CVOLFS_MUTANT"));
        set_child2(argv[2], argv[3]);
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
        test_unnamed();
        test_fault_aside();
        test_two_disks();
    } else if (cvolfs_test_mutant == CVOLFS_MUT_SAMERID) {
        test_basic();
        test_refusals();
    } else if (cvolfs_test_mutant == CVOLFS_MUT_NONONCE || cvolfs_test_mutant == CVOLFS_MUT_TRUSTUNSURE ||
               cvolfs_test_mutant == CVOLFS_MUT_TORNSTALE) {
        test_basic();
        test_unsure();
    } else if (cvolfs_test_mutant == CVOLFS_MUT_FIRSTDISK || cvolfs_test_mutant == CVOLFS_MUT_DUPOK) {
        test_two_disks();
    }
    if (!mutant || replfile_test_mutant == REPLFILE_MUT_NOSYNC || cvolfs_test_mutant == CVOLFS_MUT_NODIRSYNC ||
        cvolfs_test_mutant == CVOLFS_MUT_NONONCE)
        test_power_loss();
    if (!mutant || cvolfs_test_mutant == CVOLFS_MUT_REMOVENODIRSYNC)
        test_set_power();
    if (!mutant || cvolfs_test_mutant == CVOLFS_MUT_NODIRSYNC || cvolfs_test_mutant == CVOLFS_MUT_REMOVENODIRSYNC)
        test_second_disk_power();
    sh("rm -rf '%s'", S);
    if (failures)
        fprintf(stderr, "%d cvolfs checks FAILED\n", failures);
    else
        printf("cvolfs tests passed\n");
    return failures ? 1 : 0;
}
