/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The open's recovery of an insert's intent (src/part.c intents_recover) under power loss at every step: an insert
 * over several partitions is all or nothing, also while the open that finishes it is cut.
 *
 * A fixture is a database whose last insert (PARTS partitions) stopped at its commit point: the intent is durable and
 * every part is still under its tmp_ name (or, in one case, one of them already renamed by an earlier open that was
 * cut). A child opens a copy under crashfs (tests/jepsen/crashfs.c) with CRASHFS_TEAR=1, which keeps a random subset
 * of the directory operations not yet synced (in order per name, any order across names), and loses power before its
 * w-th tracked operation, for every w and several seeds; the parent then opens the copy without the shim:
 *  - a whole intent with every part there was committed: every row is there, at every step; also when every part is
 *    under its final name already, renamed in the page cache only (a writer killed before its directory fsync, or an
 *    open killed or failed before its own): the child does those renames under the shim before it opens, and the
 *    open must sync them although it renames nothing itself (review H1);
 *  - an intent that is torn, or whose parts are not all there with none renamed, was not: no row is there;
 *  - after that open no tmp_ part and no intent is left.
 * And the insert itself (parts_commit: intent, its sync and the directory's, renames, the directory's sync, unlink),
 * cut at every step: all its rows or none at the next open, all of them once it returned 0. And a failed fsync of the
 * roll-forward (CRASHFS_MODE=eio): the open fails, keeps the intent, and the next open has every row.
 * A sweep that finds rows missing prints "ROWS WRONG", which the mutant run in the Makefile requires.
 * The bug this guards against (found in review, r7-swap M3): the roll-forward unlinked the intent before the
 * directory fsync that makes its renames durable, so a power cut could keep the unlink and lose a rename; the next
 * open found no intent and removed the tmp_ part that was left: the insert half there.
 *
 *   ./build/test_intents [scratch dir]
 *   ./build/test_intents --mutant unlinkfirst [scratch dir]   the old order: must fail
 *   ./build/test_intents --child DIR - | --child-insert DIR ACK  (internal: the open, or open and insert, the shim cuts)
 */
#define _GNU_SOURCE
#include "internal.h"

#include <dirent.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define PARTS 4
#define ROWS_PER_PART 40 /* PARTS x this >= wal_max_batch_rows: the insert goes straight to parts */
#define SEEDS 12

static int failures;
static char S[PATH_MAX - 64], SELF[PATH_MAX], SHIM[PATH_MAX];

#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #c);                                    \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

static void sh(const char *fmt, const char *a, const char *b)
{
    char cmd[3 * PATH_MAX];
    snprintf(cmd, sizeof cmd, fmt, a, b);
    if (system(cmd) != 0) {
        fprintf(stderr, "command failed: %s\n", cmd);
        failures++;
    }
}

static tsdb_options opts(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 1;
    o.wal_sync_ms = 0;
    o.merge_threads = 0;        /* nothing but the recovery changes the directory */
    o.partition_classes = 0;    /* one part per day */
    o.wal_max_batch_rows = 64;
    o.flush_interval_ms = 1000000;
    return o;
}

static int64_t rows_of(tsdb *db)
{
    tsdb_sid id;
    if (tsdb_series_lookup(db, "s", &id) != 0)
        return -1;
    tsdb_points p = {0};
    if (tsdb_query_points(db, id, INT64_MIN, INT64_MAX, &p) != 0)
        return -2;
    int64_t n = (int64_t)p.n;
    tsdb_points_free(&p);
    return n;
}

static int cmp_str(const void *a, const void *b) { return strcmp(*(char *const *)a, *(char *const *)b); }

/* The names in dir that start with prefix, sorted; returns how many (at most cap). */
static size_t names_of(const char *dir, const char *prefix, char out[][128], size_t cap)
{
    DIR *d = opendir(dir);
    char *v[64];
    size_t n = 0;
    struct dirent *e;
    while (d && (e = readdir(d)))
        if (strncmp(e->d_name, prefix, strlen(prefix)) == 0 && n < cap && n < 64)
            v[n++] = strdup(e->d_name);
    if (d)
        closedir(d);
    qsort(v, n, sizeof *v, cmp_str);
    for (size_t i = 0; i < n; i++) {
        snprintf(out[i], 128, "%s", v[i]);
        free(v[i]);
    }
    return n;
}

enum fixture { FX_FORWARD, FX_FORWARD_STARTED, FX_TORN, FX_MISSING, FX_INSERT, FX_UNSYNCED };
static const char *fx_name[] = {"roll forward", "roll forward, one part renamed already", "torn intent",
                                "a part gone, none renamed", "the insert itself (parts_commit)",
                                "roll forward, every part renamed but not synced (a killed writer)"};

static int insert_all(tsdb *db, tsdb_sid id)
{
    int64_t ts[PARTS * ROWS_PER_PART];
    float v[PARTS * ROWS_PER_PART];
    for (int i = 0; i < PARTS * ROWS_PER_PART; i++) {
        ts[i] = (int64_t)(i / ROWS_PER_PART) * 86400000 + (i % ROWS_PER_PART) * 1000;
        v[i] = (float)i;
    }
    return tsdb_insert_series(db, id, ts, v, PARTS * ROWS_PER_PART);
}

/* A database whose one insert over PARTS days stopped at its commit point (see the file's header). */
static void make_fixture(const char *dir, enum fixture fx)
{
    sh("rm -rf '%s'%s", dir, "");
    tsdb_options o = opts();
    tsdb *db = NULL;
    CHECK(tsdb_open(dir, &o, &db) == 0);
    if (!db)
        return;
    tsdb_sid id;
    CHECK(tsdb_series(db, "s", &id) == 0);
    if (fx == FX_INSERT) { /* the child inserts */
        tsdb_close(db);
        return;
    }
    CHECK(insert_all(db, id) == 0);
    tsdb_close(db);

    char parts[PARTS + 1][128];
    size_t n = names_of(dir, "p", parts, PARTS + 1);
    CHECK(n == PARTS);
    if (n != PARTS)
        return;
    char buf[4096], from[PATH_MAX + 800], to[PATH_MAX + 800];
    size_t len = (size_t)snprintf(buf, sizeof buf, "tsdb-intent 3 %d\n", PARTS);
    for (size_t i = 0; i < n; i++) {
        char tmp[64];
        snprintf(tmp, sizeof tmp, "tmp_1_%zu.part", i);
        len += (size_t)snprintf(buf + len, sizeof buf - len, "%s %s\n", tmp, parts[i]);
        bool keep = (fx == FX_FORWARD_STARTED && i == 1) || (fx == FX_MISSING && i == 2);
        snprintf(from, sizeof from, "%s/%s", dir, parts[i]);
        snprintf(to, sizeof to, "%s/%s", dir, tmp);
        if (fx == FX_MISSING && i == 2)
            CHECK(unlink(from) == 0); /* its tmp name was lost (a failed directory fsync), none renamed yet */
        else if (!keep)
            CHECK(rename(from, to) == 0);
    }
    len += (size_t)snprintf(buf + len, sizeof buf - len, "end %016" PRIx64 "\n", xxh64(buf, len, 0));
    if (fx == FX_TORN)
        buf[len - 3] ^= 1; /* the hash does not match: torn while it was written */
    snprintf(to, sizeof to, "%s/txn_1_100.intent", dir);
    int fd = open(to, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    CHECK(fd >= 0 && write(fd, buf, len) == (ssize_t)len && fsync(fd) == 0);
    if (fd >= 0)
        close(fd);
    sync();
}

/* The open the shim cuts: status 99 when the power went before the w-th tracked operation. */
static int run_child(const char *dir, const char *ack, int warmup, int seed, bool prerename, const char *eio_log)
{
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512], w[16], sd[16];
        /* no leak check in the child: the shim keeps the old contents of what it rolls back (keep_old) until the
         * process ends; the parent's opens of the same directories still run under it */
        snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0:detect_leaks=0",
                 getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
        setenv("ASAN_OPTIONS", asan, 1);
        snprintf(w, sizeof w, "%d", warmup);
        snprintf(sd, sizeof sd, "%d", seed);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", dir, 1);
        setenv("CRASHFS_PROB", "1", 1);
        setenv("CRASHFS_WARMUP", w, 1);
        setenv("CRASHFS_AT_EXIT", "1", 1); /* what the open did must be durable once it returned */
        setenv("CRASHFS_TEAR", "1", 1);
        setenv("CRASHFS_SEED", sd, 1);
        if (eio_log) { /* the next fsync after the w-th tracked operation fails instead (one fault) */
            setenv("CRASHFS_MODE", "eio", 1);
            setenv("CRASHFS_EIO_ON", "fsync", 1);
        }
        if (prerename)
            setenv("TSDB_INTENTS_PRERENAME", "1", 1);
        int null = open("/dev/null", O_WRONLY);
        int err = eio_log ? open(eio_log, O_WRONLY | O_CREAT | O_TRUNC, 0644) : null;
        if (null >= 0)
            dup2(null, 1);
        if (err >= 0)
            dup2(err, 2);
        execl(SELF, "test_intents", ack ? "--child-insert" : "--child", dir, ack ? ack : "-", (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

/* --child: the open alone; --child-insert: the open, then the insert, acknowledged in `ack` (outside the shim's root)
 * once it returned 0 */
static int child_main(const char *dir, const char *ack)
{
    if (getenv("TSDB_INTENTS_PRERENAME")) { /* what a killed writer leaves: every rename done, none synced */
        char p[PATH_MAX + 64], line[512], a[128], b[128];
        snprintf(p, sizeof p, "%s/txn_1_100.intent", dir);
        FILE *f = fopen(p, "r");
        if (!f)
            return 4;
        while (fgets(line, sizeof line, f))
            if (sscanf(line, "%127s %127s", a, b) == 2 && strncmp(a, "tmp_", 4) == 0) {
                char x[PATH_MAX + 200], y[PATH_MAX + 200];
                snprintf(x, sizeof x, "%s/%s", dir, a);
                snprintf(y, sizeof y, "%s/%s", dir, b);
                if (rename(x, y) != 0) {
                    fclose(f);
                    return 5;
                }
            }
        fclose(f);
    }
    tsdb_options o = opts();
    tsdb *db = NULL;
    if (tsdb_open(dir, &o, &db) != 0) {
        char p[PATH_MAX + 64];
        snprintf(p, sizeof p, "%s/txn_1_100.intent", dir);
        return access(p, F_OK) == 0 ? 2 : 6; /* a failed open keeps the intent */
    }
    tsdb_sid id;
    if (ack && tsdb_series_lookup(db, "s", &id) == 0 && insert_all(db, id) == 0) {
        int fd = open(ack, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd >= 0)
            close(fd);
    }
    int64_t n = rows_of(db);
    tsdb_close(db);
    return n >= 0 ? 0 : 3;
}

/* Every step of the open that recovers fixture fx, cut by a power loss; the next open must see `want` rows. */
static void sweep(enum fixture fx)
{
    char src[PATH_MAX], work[PATH_MAX];
    snprintf(src, sizeof src, "%s/src", S);
    snprintf(work, sizeof work, "%s/work", S);
    char ack[PATH_MAX + 8];
    snprintf(ack, sizeof ack, "%s/ack", S);
    make_fixture(src, fx);
    int64_t all = PARTS * ROWS_PER_PART,
            want = fx == FX_FORWARD || fx == FX_FORWARD_STARTED || fx == FX_UNSYNCED ? all : 0;
    bool rows_wrong = false;
    int bad = 0, steps = 0, none = 0, every = 0;
    for (int w = 0; w < 5000 && bad < 3; w++) {
        bool ended = false;
        for (int seed = 0; seed < SEEDS && bad < 3; seed++) {
            sh("rm -rf '%s'%s", work, "");
            char cmd[3 * PATH_MAX];
            snprintf(cmd, sizeof cmd, "cp -a '%s' '%s'", src, work);
            if (system(cmd) != 0) {
                failures++;
                return;
            }
            unlink(ack);
            int st = run_child(work, fx == FX_INSERT ? ack : NULL, w, seed, fx == FX_UNSYNCED, NULL);
            if (st != 99 && st != 0) {
                fprintf(stderr, "%s: step %d seed %d: the child exited with %d\n", fx_name[fx], w, seed, st);
                failures++;
                bad++;
                continue;
            }
            ended = ended || st == 0; /* the open and close ran whole; the power went at the exit */
            tsdb_options o = opts();
            tsdb *db = NULL;
            int rc = tsdb_open(work, &o, &db);
            int64_t n = db ? rows_of(db) : -1;
            if (db)
                tsdb_close(db);
            char left[4][128];
            size_t ntmp = names_of(work, "tmp_", left, 4), ntxn = names_of(work, "txn_", left, 4);
            if (fx == FX_INSERT) /* not acknowledged: all or none; acknowledged: all */
                want = access(ack, F_OK) == 0 || n == all ? all : 0;
            none += n == 0;
            every += n == all;
            rows_wrong = rows_wrong || n != want;
            if (rc != 0 || n != want || ntmp || ntxn) {
                fprintf(stderr,
                        "%s: power lost before operation %d (seed %d): the next open says %d, %" PRId64
                        " of %d rows (want %" PRId64 "), %zu tmp_ and %zu txn_ files left\n",
                        fx_name[fx], w, seed, rc, n, PARTS * ROWS_PER_PART, want, ntmp, ntxn);
                failures++;
                bad++;
            }
        }
        steps = w + 1;
        if (ended)
            break;
    }
    if (fx == FX_INSERT && (!none || !every)) { /* the sweep must see both sides of the commit point */
        fprintf(stderr, "%s: %d cuts left no row, %d every row: the sweep missed a side\n", fx_name[fx], none, every);
        failures++;
        bad++;
    }
    if (rows_wrong)
        printf("  %s: ROWS WRONG (part of the insert there)\n", fx_name[fx]);
    printf("  %s: power lost at each of %d steps (%d seeds each): %s\n", fx_name[fx], steps, SEEDS,
           bad                 ? "FAILED"
           : fx == FX_INSERT ? "all rows there or none, all once acknowledged"
           : want            ? "every row there"
                             : "no row there");
}

/* A failed fsync in the open's roll-forward (review L3): for every w, the first fsync after the w-th tracked operation
 * of the open fails with EIO (crashfs then lies about that file's syncs and rolls back at the exit what they did not
 * make durable). Every open that failed must have kept the intent; the next open, without the shim, has every row and
 * leaves no tmp_ or txn_ file. At least one w must hit the roll-forward's own fsync (its log line). */
static void eio_sweep(void)
{
    char src[PATH_MAX], work[PATH_MAX], log[PATH_MAX + 8];
    snprintf(src, sizeof src, "%s/src", S);
    snprintf(work, sizeof work, "%s/work", S);
    snprintf(log, sizeof log, "%s/eio.log", S);
    make_fixture(src, FX_FORWARD);
    int64_t all = PARTS * ROWS_PER_PART;
    int bad = 0, hit = 0, w;
    for (w = 0; w < 5000 && bad < 3; w++) {
        sh("rm -rf '%s'%s", work, "");
        char cmd[3 * PATH_MAX];
        snprintf(cmd, sizeof cmd, "cp -a '%s' '%s'", src, work);
        if (system(cmd) != 0) {
            failures++;
            return;
        }
        int st = run_child(work, NULL, w, 0, false, log);
        char line[1024];
        bool fault = false, ours = false;
        FILE *f = fopen(log, "r");
        while (f && fgets(line, sizeof line, f)) {
            fault = fault || strstr(line, "crashfs: EIO on") != NULL;
            ours = ours || strstr(line, "rolling insert intent txn_1_100.intent forward: fsync of the directory");
        }
        if (f)
            fclose(f);
        hit += ours;
        /* 6: the open failed with the intent gone, which only a fault after the recovery may leave */
        if (ours ? st != 2 : (st != 0 && st != 2 && st != 6) || (!fault && st != 0)) {
            fprintf(stderr, "a failed fsync: EIO after operation %d: the child exited with %d (%s)\n", w, st,
                    !ours ? "unexpected" : st == 6 ? "the failed open removed the intent" : "the open did not fail");
            failures++;
            bad++;
        }
        tsdb_options o = opts();
        tsdb *db = NULL;
        int rc = tsdb_open(work, &o, &db);
        int64_t n = db ? rows_of(db) : -1;
        if (db)
            tsdb_close(db);
        char left[4][128];
        size_t ntmp = names_of(work, "tmp_", left, 4), ntxn = names_of(work, "txn_", left, 4);
        if (rc != 0 || n != all || ntmp || ntxn) {
            fprintf(stderr,
                    "a failed fsync: EIO after operation %d: the next open says %d, %" PRId64 " of %" PRId64
                    " rows, %zu tmp_ and %zu txn_ files left\n",
                    w, rc, n, all, ntmp, ntxn);
            failures++;
            bad++;
        }
        if (!fault) /* the open and close ran whole before a fault came */
            break;
    }
    if (!hit) {
        fprintf(stderr, "a failed fsync: no step failed the roll-forward's fsync\n");
        failures++;
        bad++;
    }
    printf("  a failed fsync of the roll-forward, EIO at each of %d steps: %s\n", w + 1,
           bad ? "FAILED" : "the open fails and keeps the intent; every row there after");
}

int main(int argc, char **argv)
{
    if (argc == 4 && (strcmp(argv[1], "--child") == 0 || strcmp(argv[1], "--child-insert") == 0)) {
        if (getenv("TSDB_INTENTS_MUTANT"))
            intents_test_mutant = INTENTS_MUT_UNLINK_FIRST;
        return child_main(argv[2], strcmp(argv[1], "--child-insert") == 0 ? argv[3] : NULL);
    }
    const char *scratch = "/tmp";
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mutant") == 0 && i + 1 < argc) {
            if (strcmp(argv[++i], "unlinkfirst") != 0) {
                fprintf(stderr, "unknown mutant %s\n", argv[i]);
                return 2;
            }
            intents_test_mutant = INTENTS_MUT_UNLINK_FIRST;
            setenv("TSDB_INTENTS_MUTANT", "1", 1); /* the processes the shim cuts run the same bug */
        } else {
            scratch = argv[i];
        }
    }
    char tmpl[sizeof S];
    snprintf(tmpl, sizeof tmpl, "%.*s/test-intents.XXXXXX", (int)sizeof S - 32, scratch);
    if (!mkdtemp(tmpl)) {
        perror("mkdtemp");
        return 1;
    }
    snprintf(S, sizeof S, "%s", tmpl);
    if (!realpath(argv[0], SELF))
        snprintf(SELF, sizeof SELF, "%s", argv[0]);
    char *slash = strrchr(SELF, '/');
    snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", slash ? (int)(slash - SELF) : 1, slash ? SELF : ".");
    if (access(SHIM, R_OK) != 0) { /* crashfs is built on Linux only (macOS: docs/macos.md) */
#ifdef __linux__
        fprintf(stderr, "test_intents: %s is missing\n", SHIM);
        return 1;
#else
        printf("test_intents: skipped (no %s on this platform)\n", SHIM);
        rmdir(S);
        return intents_test_mutant ? 1 : 0; /* a mutant run must still fail */
#endif
    }
    printf("test_intents: an insert's intent recovered at the open, power lost at every step\n");
    sweep(FX_FORWARD);
    sweep(FX_FORWARD_STARTED);
    sweep(FX_UNSYNCED);
    if (!intents_test_mutant) {
        sweep(FX_TORN);
        sweep(FX_MISSING);
        sweep(FX_INSERT);
        eio_sweep();
    }
    if (failures == 0)
        sh("rm -rf '%s'%s", S, "");
    if (failures)
        printf("test_intents: %d FAILED\n", failures);
    else
        printf("test_intents: all passed\n");
    return failures != 0;
}
