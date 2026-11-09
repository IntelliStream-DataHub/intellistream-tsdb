/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * `tsdb DIR upgrade` (tsdb_upgrade): a database that release 0.9.0 wrote (disk format 8; tests/data/v8-fixture.tar,
 * made by tests/data/make-v8-fixture.sh with the 0.9.0 binary: a root and a volume, every value type, parts of
 * several partitions, tombstones, an empty part, retired parts, states, a retention limit, and the export of it that
 * 0.9.0 made, v8-fixture.tsdbdump) is carried over in place:
 *  - the open refuses it before and takes it after, and every series answers as the export says (a reference
 *    database imported from that export is compared series by series);
 *  - what is not clean is refused before anything is written: a WAL segment, an intent, a tmp part, a writer's lock;
 *  - parts keep their 24-byte headers (META parts_v8=1) and are read as they are, until merges rewrite them; then the
 *    allowance leaves META, and a part with the old header is damage in a volume that does not allow it;
 *  - power lost at every step of the upgrade (crashfs): the root says 8 until everything else is done, the open
 *    refuses or works, a second run finishes it, and the answers are the same.
 *
 *   ./build/test_upgrade [scratch dir]          (from the source directory: it reads tests/data)
 *   ./build/test_upgrade --mutant v8hash48      the mutation it must catch: a v8 header hashed as 48 bytes
 */
#define _GNU_SOURCE
#include "internal.h"

#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static int failures;
#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                              \
        }                                                                            \
    } while (0)

#define FIXTURE "tests/data/v8-fixture.tar"
#define DUMP "tests/data/v8-fixture.tsdbdump"
static const char *const names[] = {"f32", "f64", "i64", "flag", "mode", "gone", "v1f32", "v1i64"};
#define NNAMES 8

static char S[PATH_MAX], CLI[PATH_MAX + 16], SHIM[PATH_MAX + 32];
static char oldvol[PATH_MAX]; /* the volume path the fixture was made with */

static int sh(const char *fmt, ...)
{
    char cmd[4096];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(cmd, sizeof cmd, fmt, ap);
    va_end(ap);
    return system(cmd);
}

static void spit(const char *path, const char *text, size_t n)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0 || write(fd, text, n) != (ssize_t)n)
        failures++;
    if (fd >= 0)
        close(fd);
}

static char *slurp(const char *path, size_t *n)
{
    FILE *f = fopen(path, "rb");
    if (!f)
        return NULL;
    char *b = malloc(1 << 16);
    *n = b ? fread(b, 1, (1 << 16) - 1, f) : 0;
    fclose(f);
    if (b)
        b[*n] = '\0';
    return b;
}

/* The fixture unpacked into top/db and top/vol1, VOLUMES naming the volume where it now is. */
static void unpack(const char *top)
{
    sh("rm -rf '%s'; mkdir -p '%s' && tar -xf " FIXTURE " -C '%s'", top, top, top);
    char path[PATH_MAX + 32], vol[PATH_MAX + 16];
    snprintf(path, sizeof path, "%s/db/VOLUMES", top);
    size_t n;
    char *old = slurp(path, &n);
    if (!old || n < 10 || sscanf(old + 9, "%4095[^\n]", oldvol) != 1) {
        fprintf(stderr, "cannot read the fixture's VOLUMES\n");
        exit(1);
    }
    free(old);
    snprintf(vol, sizeof vol, "%s/vol1", top);
    char line[PATH_MAX + 32];
    int len = snprintf(line, sizeof line, "%08x %s\n", (uint32_t)xxh64(vol, strlen(vol), 1), vol);
    spit(path, line, (size_t)len);
}

static int meta_version(const char *dir)
{
    char p[PATH_MAX + 16];
    snprintf(p, sizeof p, "%s/META", dir);
    size_t n;
    char *t = slurp(p, &n);
    char *v = t ? strstr(t, "version=") : NULL;
    int r = v ? atoi(v + 8) : -1;
    free(t);
    return r;
}

static bool meta_has(const char *dir, const char *what)
{
    char p[PATH_MAX + 16];
    snprintf(p, sizeof p, "%s/META", dir);
    size_t n;
    char *t = slurp(p, &n);
    bool r = t && strstr(t, what);
    free(t);
    return r;
}

/* Parts of both directories by the version their header says (offset 8): how many are of format 8 and of 9. */
static void part_versions(const char *top, int *v8, int *v9)
{
    *v8 = *v9 = 0;
    const char *dirs[] = {"db", "vol1"};
    for (int d = 0; d < 2; d++) {
        char p[PATH_MAX + 16];
        snprintf(p, sizeof p, "%s/%s", top, dirs[d]);
        DIR *dir = opendir(p);
        struct dirent *e;
        while (dir && (e = readdir(dir))) {
            if (e->d_name[0] != 'p' || !strstr(e->d_name, ".part"))
                continue;
            char f[PATH_MAX + 300];
            snprintf(f, sizeof f, "%s/%s", p, e->d_name);
            int fd = open(f, O_RDONLY);
            uint32_t ver = 0;
            if (fd >= 0 && pread(fd, &ver, 4, 8) == 4)
                *(ver == 8 ? v8 : v9) += ver == 8 || ver == 9;
            if (fd >= 0)
                close(fd);
        }
        if (dir)
            closedir(dir);
    }
}

/* every series' rows, as the CLI prints them, in files under out/ */
static void dump_rows(const char *db, const char *out)
{
    sh("mkdir -p '%s'", out);
    for (int i = 0; i < NNAMES; i++)
        sh("'%s' '%s' query %s 1970-01-01 2100-01-01 raw > '%s/%s.txt' 2>/dev/null", CLI, db, names[i], out, names[i]);
}

static bool same_rows(const char *a, const char *b, size_t *rows)
{
    bool same = true;
    *rows = 0;
    for (int i = 0; i < NNAMES; i++) {
        char pa[PATH_MAX + 64], pb[PATH_MAX + 64];
        snprintf(pa, sizeof pa, "%s/%s.txt", a, names[i]);
        snprintf(pb, sizeof pb, "%s/%s.txt", b, names[i]);
        size_t na, nb;
        char *ta = slurp(pa, &na), *tb = slurp(pb, &nb);
        (void)ta, (void)tb;
        free(ta);
        free(tb);
        if (sh("cmp -s '%s' '%s'", pa, pb) != 0)
            same = false;
        struct stat st;
        *rows += stat(pa, &st) == 0 ? (size_t)st.st_size : 0;
    }
    return same;
}

static char ref_rows[PATH_MAX + 32];

/* The reference: the export 0.9.0 made, imported into a new database; its rows are what the upgraded one must give. */
static void make_reference(void)
{
    char ref[PATH_MAX + 16], vol[PATH_MAX + 16];
    snprintf(ref, sizeof ref, "%s/ref/db", S);
    snprintf(vol, sizeof vol, "%s/ref/vol1", S);
    snprintf(ref_rows, sizeof ref_rows, "%s/ref/rows", S);
    sh("rm -rf '%s/ref'; mkdir -p '%s'", S, vol);
    int rc = sh("'%s' '%s' import " DUMP " --volume '%s=%s' --create-volumes >/dev/null 2>&1", CLI, ref, oldvol, vol);
    CHECK(rc == 0);
    dump_rows(ref, ref_rows);
    size_t rows;
    CHECK(same_rows(ref_rows, ref_rows, &rows) && rows > 100000); /* the reference has the rows */
}

static tsdb_options opts(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.create_volumes = 0;
    return o;
}

static void check_equal_to_reference(const char *top, const char *what)
{
    char db[PATH_MAX + 16], rows[PATH_MAX + 32];
    snprintf(db, sizeof db, "%s/db", top);
    snprintf(rows, sizeof rows, "%s/rows", top);
    dump_rows(db, rows);
    size_t n;
    bool same = same_rows(ref_rows, rows, &n);
    if (!same)
        fprintf(stderr, "  %s: the rows differ from the 0.9.0 export\n", what);
    CHECK(same);
}

static void test_refusals(void)
{
    char top[PATH_MAX + 16], db[PATH_MAX + 16], vol[PATH_MAX + 16], cks[PATH_MAX + 32], cks2[PATH_MAX + 32];
    snprintf(top, sizeof top, "%s/refuse", S);
    snprintf(db, sizeof db, "%s/db", top);
    snprintf(vol, sizeof vol, "%s/vol1", top);
    snprintf(cks, sizeof cks, "%s/before.cksum", S);
    snprintf(cks2, sizeof cks2, "%s/after.cksum", S);
    unpack(top);
    /* the new release refuses a format-8 database, writers and readers, and names the way over */
    tsdb *t;
    tsdb_options o = opts();
    CHECK(tsdb_open(db, &o, &t) == TSDB_ERR_CORRUPT);
    o.read_only = 1;
    CHECK(tsdb_open(db, &o, &t) == TSDB_ERR_CORRUPT);
    struct {
        const char *dir, *file, *why;
    } bad[] = {{"db", "wal_0000000000000001.log", "a WAL segment, even an empty one"},
               {"vol1", "wal_0000000000000003.log", "a WAL segment in a volume"},
               {"db", "txn_12_3.intent", "an intent"},
               {"vol1", "tmp_12_3.part", "a part that was being written"}};
    for (size_t i = 0; i < sizeof bad / sizeof *bad; i++) {
        char p[PATH_MAX + 64];
        snprintf(p, sizeof p, "%s/%s/%s", top, bad[i].dir, bad[i].file);
        spit(p, "", 0);
        sh("cd '%s' && find . -type f ! -name LOCK | sort | xargs cksum > '%s'", top, cks);
        fprintf(stderr, "  (a refusal is logged next: %s)\n", bad[i].why);
        CHECK(tsdb_upgrade(db) == TSDB_ERR_INVALID);
        sh("cd '%s' && find . -type f ! -name LOCK | sort | xargs cksum > '%s'", top, cks2);
        CHECK(sh("cmp -s '%s' '%s'", cks, cks2) == 0); /* nothing was written, nothing removed */
        CHECK(meta_version(db) == 8 && meta_version(vol) == 8);
        unlink(p);
    }
    /* retired parts are normal after a clean close: no reason to refuse (the fixture has them) */
    CHECK(sh("ls '%s'/retired_p*.part >/dev/null", db) == 0);
    /* a writer's lock */
    char lock[PATH_MAX + 32];
    snprintf(lock, sizeof lock, "%s/LOCK", vol);
    int lfd = open(lock, O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    CHECK(lfd >= 0 && flock(lfd, LOCK_EX | LOCK_NB) == 0);
    fprintf(stderr, "  (a refusal is logged next: a writer's lock)\n");
    CHECK(tsdb_upgrade(db) == TSDB_ERR_LOCKED);
    CHECK(meta_version(db) == 8 && meta_version(vol) == 8);
    close(lfd);
    /* a damaged MANIFEST is not carried over */
    char man[PATH_MAX + 32];
    snprintf(man, sizeof man, "%s/MANIFEST", db);
    size_t n;
    char *m = slurp(man, &n);
    CHECK(m && n > 20);
    if (m && n > 20) {
        m[10] ^= 1;
        spit(man, m, n);
        fprintf(stderr, "  (a refusal is logged next: MANIFEST)\n");
        CHECK(tsdb_upgrade(db) == TSDB_ERR_CORRUPT);
        CHECK(meta_version(db) == 8 && meta_version(vol) == 8);
    }
    free(m);
    sh("rm -rf '%s'", top);
    printf("  upgrade refuses a WAL segment, an intent, a tmp part, a writer's lock and a damaged MANIFEST, and "
           "writes nothing\n");
}

/* A damaged META with very many `version=` lines (each grows by a byte when it is rewritten) must not overflow the
 * buffer that rewrites it: the upgrade finishes and the META says 9. Before the fix the output was sized
 * strlen + 64 and this wrote tens of kilobytes past it (a crash here, a heap-buffer-overflow under ASan). */
static void test_meta_many_version_lines(void)
{
    char top[PATH_MAX + 16], db[PATH_MAX + 16], mp[PATH_MAX + 32];
    snprintf(top, sizeof top, "%s/metalines", S);
    snprintf(db, sizeof db, "%s/db", top);
    snprintf(mp, sizeof mp, "%s/META", db);
    unpack(top);
    size_t n;
    char *m = slurp(mp, &n);
    CHECK(m && n > 0);
    enum { LINES = 40000 };
    char *big = malloc(n + (size_t)LINES * 9 + 1);
    if (m && big) {
        memcpy(big, m, n);
        size_t k = n;
        for (int i = 0; i < LINES; i++, k += 9)
            memcpy(big + k, "version=\n", 9);
        spit(mp, big, k);
        CHECK(tsdb_upgrade(db) == 2);
        CHECK(meta_version(db) == 9);
        CHECK(tsdb_upgrade(db) == 0);
    }
    free(big);
    free(m);
    sh("rm -rf '%s'", top);
    printf("  a META with 40000 version= lines is upgraded without writing past its buffer\n");
}

static void test_upgrade_and_merge(bool mutant)
{
    char top[PATH_MAX + 16], db[PATH_MAX + 16], vol[PATH_MAX + 16];
    snprintf(top, sizeof top, "%s/up", S);
    snprintf(db, sizeof db, "%s/db", top);
    snprintf(vol, sizeof vol, "%s/vol1", top);
    unpack(top);
    int v8, v9;
    part_versions(top, &v8, &v9);
    CHECK(v8 > 5 && v9 == 0);
    CHECK(tsdb_upgrade(db) == 2); /* the volume and the root */
    CHECK(meta_version(db) == 9 && meta_version(vol) == 9 && meta_has(db, "parts_v8=1") && meta_has(vol, "parts_v8=1"));
    char f[PATH_MAX + 16];
    snprintf(f, sizeof f, "%s/FNO", db);
    struct stat st;
    CHECK(stat(f, &st) == 0 && st.st_size == 8192);
    snprintf(f, sizeof f, "%s/FNO", vol);
    CHECK(stat(f, &st) == 0 && st.st_size == 8192);
    CHECK(sh("head -1 '%s/MANIFEST' | grep -q '^tsdb-manifest 9$' && grep -q '^fno_next 1$' '%s/MANIFEST'", db, db) ==
          0);
    part_versions(top, &v8, &v9);
    CHECK(v8 > 5 && v9 == 0); /* the parts are as they were */
    CHECK(tsdb_upgrade(db) == 0); /* nothing left to do */

    /* the open reads the parts as they are, and every series answers as 0.9.0's export says */
    tsdb *t;
    tsdb_options o = opts();
    o.merge_idle_s = -1; /* the fixture's parts are old, so an idle merge could rewrite them before the close */
    if (tsdb_open(db, &o, &t) != 0) {
        CHECK(false && "the upgraded database opens");
        return;
    }
    CHECK(tsdb_close(t) == 0);
    check_equal_to_reference(top, "after the upgrade");
    CHECK(meta_has(db, "parts_v8=1")); /* parts with the old header are still there */

    /* the merges rewrite them (tsdb_optimize takes a part alone too), and the allowance goes from META */
    CHECK(sh("'%s' '%s' optimize >/dev/null 2>&1", CLI, db) == 0);
    part_versions(top, &v8, &v9);
    CHECK(v8 == 0 && v9 > 3);
    CHECK(!meta_has(db, "parts_v8=") && !meta_has(vol, "parts_v8="));
    check_equal_to_reference(top, "after the merges");
    CHECK(tsdb_open(db, &o, &t) == 0 && tsdb_close(t) == 0);
    check_equal_to_reference(top, "reopened");
    sh("rm -rf '%s'", top);
    printf("  the fixture of 0.9.0 is upgraded, read as it is, merged into format 9 and reopened: every series "
           "answers as its export says\n");
    (void)mutant;
}

/* A part with the old header where META does not allow it is damage, not an old part. */
static void test_v8_part_not_allowed(void)
{
    char top[PATH_MAX + 16], db[PATH_MAX + 16];
    snprintf(top, sizeof top, "%s/noallow", S);
    snprintf(db, sizeof db, "%s/db", top);
    unpack(top);
    CHECK(tsdb_upgrade(db) == 2);
    char meta[PATH_MAX + 16];
    snprintf(meta, sizeof meta, "%s/META", db);
    size_t n;
    char *m = slurp(meta, &n);
    CHECK(m != NULL);
    char *p = m ? strstr(m, "parts_v8=1\n") : NULL;
    CHECK(p != NULL);
    if (p) {
        memmove(p, p + 11, n - (size_t)(p - m) - 11);
        spit(meta, m, n - 11);
        tsdb *t;
        tsdb_options o = opts();
        fprintf(stderr, "  (a broken part is logged next)\n");
        CHECK(tsdb_open(db, &o, &t) == TSDB_ERR_CORRUPT);
    }
    free(m);
    sh("rm -rf '%s'", top);
    printf("  a part with a format-8 header in a volume that does not allow it fails the open\n");
}

/* Power lost at every step of the upgrade and after it. */
static void test_power_loss(void)
{
    if (access(CLI, X_OK) != 0 || access(SHIM, R_OK) != 0) {
        printf("  power loss during the upgrade: skipped (no %s or %s)\n", CLI, SHIM);
        return;
    }
    char top[PATH_MAX + 16], db[PATH_MAX + 16], vol[PATH_MAX + 16];
    snprintf(top, sizeof top, "%s/pl", S);
    snprintf(db, sizeof db, "%s/db", top);
    snprintf(vol, sizeof vol, "%s/vol1", top);
    int crashes = 0, finished = 0, half = 0;
    for (int k = 0; k < 400 && !finished; k++) {
        unpack(top);
        char warm[16];
        snprintf(warm, sizeof warm, "%d", k);
        pid_t pid = fork();
        if (pid == 0) {
            char asan[512];
            snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0",
                     getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
            setenv("ASAN_OPTIONS", asan, 1);
            setenv("LD_PRELOAD", SHIM, 1);
            setenv("CRASHFS_ROOT", top, 1);
            setenv("CRASHFS_PROB", "1", 1);
            setenv("CRASHFS_WARMUP", warm, 1);
            setenv("CRASHFS_AT_EXIT", "1", 1);
            setenv("CRASHFS_TEAR", "1", 1);
            setenv("CRASHFS_SEED", warm, 1);
            int null = open("/dev/null", O_WRONLY);
            if (null >= 0) {
                dup2(null, 1);
                dup2(null, 2);
            }
            execl(CLI, "tsdb", db, "upgrade", (char *)NULL);
            _exit(127);
        }
        int st = -1;
        CHECK(pid > 0 && waitpid(pid, &st, 0) == pid && WIFEXITED(st));
        int code = WIFEXITED(st) ? WEXITSTATUS(st) : -1;
        CHECK(code == 99 || code == 0);
        if (code != 99 && code != 0)
            break;
        crashes += code == 99;
        finished = code == 0;
        /* the root is written last: if it says 9, the volume says 9 */
        int rv = meta_version(db), vv = meta_version(vol);
        CHECK(rv == 8 || rv == 9);
        CHECK(rv != 9 || vv == 9);
        half += rv == 8 && vv == 9;
        tsdb *t;
        tsdb_options o = opts();
        fprintf(stderr, "  (a refusal may be logged next: step %d)\n", k);
        int rc = tsdb_open(db, &o, &t);
        CHECK(rc == (rv == 9 ? 0 : TSDB_ERR_CORRUPT)); /* upgraded, or refused: never half open */
        if (rc == 0)
            CHECK(tsdb_close(t) == 0);
        /* run again: whatever the crash left, the second run finishes it */
        int n = tsdb_upgrade(db);
        CHECK(n >= 0 && n <= 2);
        CHECK(meta_version(db) == 9 && meta_version(vol) == 9);
        CHECK(tsdb_open(db, &o, &t) == 0 && tsdb_close(t) == 0);
        if (k % 8 == 0 || finished)
            check_equal_to_reference(top, "after power loss and a second run");
    }
    CHECK(finished && crashes > 10 && half > 0);
    sh("rm -rf '%s'", top);
    printf("  power lost at each of %d steps of the upgrade and after it (%d with the volume done, the root not): "
           "the open refuses or works, a second run finishes, the rows are the same\n", crashes, half);
}

/* An optional check with the binary of 0.9.0 itself (TSDB_V090_CLI): it refuses what the upgrade made, loudly. */
static void test_old_binary(void)
{
    const char *old = getenv("TSDB_V090_CLI");
    if (!old) {
        printf("  the 0.9.0 binary on an upgraded database: skipped (set TSDB_V090_CLI)\n");
        return;
    }
    char top[PATH_MAX + 16], db[PATH_MAX + 16], vol[PATH_MAX + 16];
    snprintf(top, sizeof top, "%s/old", S);
    snprintf(db, sizeof db, "%s/db", top);
    snprintf(vol, sizeof vol, "%s/vol1", top);
    unpack(top);
    /* a half-way upgrade: the volume at 9, the root at 8: the old binary opens the root and refuses the volume */
    CHECK(tsdb_upgrade(db) == 2);
    CHECK(sh("'%s' '%s' stats >/dev/null 2>&1", old, db) != 0);
    CHECK(sh("'%s' '%s' stats 2>&1 | grep -q 'disk format version 9'", old, db) == 0);
    CHECK(sh("'%s' '%s' stats 2>&1 | grep -q 'disk format version 9'", old, vol) == 0);
    sh("rm -rf '%s'", top);
    printf("  the 0.9.0 binary refuses an upgraded database, naming the versions\n");
}

int main(int argc, char **argv)
{
    const char *scratch = "/tmp";
    const char *mutant = NULL;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mutant") == 0 && i + 1 < argc)
            mutant = argv[++i];
        else
            scratch = argv[i];
    }
    char tmpl[PATH_MAX];
    snprintf(tmpl, sizeof tmpl, "%s/test-upgrade.XXXXXX", scratch);
    if (!mkdtemp(tmpl)) {
        perror("mkdtemp");
        return 1;
    }
    snprintf(S, sizeof S, "%s", tmpl);
    char self[PATH_MAX];
    snprintf(self, sizeof self, "%s", argv[0]);
    char *slash = strrchr(self, '/');
    snprintf(CLI, sizeof CLI, "%.*s/tsdb", slash ? (int)(slash - self) : 1, slash ? self : ".");
    snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", slash ? (int)(slash - self) : 1, slash ? self : ".");
    char abs[PATH_MAX];
    if (realpath(CLI, abs))
        snprintf(CLI, sizeof CLI, "%s", abs);
    if (realpath(SHIM, abs))
        snprintf(SHIM, sizeof SHIM, "%s", abs);
    if (mutant && strcmp(mutant, "v8hash48") == 0)
        eng_test_mutant = MUT_V8_HASH48;
    char probe[PATH_MAX + 16];
    snprintf(probe, sizeof probe, "%s/probe", S);
    unpack(probe); /* learns the volume path the fixture was made with */
    make_reference();
    test_upgrade_and_merge(mutant != NULL);
    if (!mutant) {
        test_refusals();
        test_meta_many_version_lines();
        test_v8_part_not_allowed();
        test_power_loss();
        test_old_binary();
    }
    sh("rm -rf '%s'", S);
    if (failures)
        fprintf(stderr, "%d upgrade checks FAILED\n", failures);
    else
        printf("upgrade tests passed\n");
    return failures ? 1 : 0;
}
