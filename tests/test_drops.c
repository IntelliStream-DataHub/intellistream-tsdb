/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Drops are atomic (src/part.c, "atomic drops"): a drop of several parts, by tsdb_drop_before or by the TTL drop of
 * the merge thread, is written as a drop intent before its first rename, so a crash anywhere leaves the partition
 * whole or gone at the next open, never half, and never with an older value showing through.
 *
 * The fixture: partition 0 has three parts, A (2500 rows of value 1), B (2000 rows of value 2, over A's first 2000)
 * and C (1000 rows of value 3, over the middle of B's); partition 1 has one part (100 rows of value 9), which
 * tsdb_drop_before(1 day) keeps. Whole partition 0 reads 2500 rows (2, 3, 2, 1); any part of it retired alone shows.
 *  - kill: a child drops and _exits after the first (then the second) rename; the next open must show 0 rows.
 *    The bug (before the intent): the partition stayed half retired (B and C listed: 2000 rows).
 *  - ttl: the TTL drop renames in block order (oldest first; it used to go newest first), and a child killed after
 *    its first rename leaves no part of either partition listed at the next open.
 *  - power: tsdb_drop_before under crashfs with CRASHFS_TEAR, power lost before every tracked operation (the intent's
 *    write, its syncs, each rename, the directory fsync, the unlink): whole or gone, gone once acknowledged.
 *  - recover: an open that rolls a drop intent forward (one part already renamed) under the same power loss: gone.
 *  - torn: an intent without its end line: the drop never started, everything is there. damaged: an intent whose
 *    checksum is wrong after all its renames ran reads as torn: the renames stand (the partition is gone), the
 *    intent is removed (as an insert's intent, part.c).
 *  - ec: an .ec name in a drop intent is rolled forward to retired_<name>.ec (r7-gc's name).
 * Failures print HALF DROP, TTL ORDER WRONG or EC LEFT, which the mutant runs in the Makefile require.
 *
 *   ./build/test_drops [scratch dir]
 *   ./build/test_drops --mutant nointent|nosync|unlinkfirst|ttlreverse|skipec [scratch dir]   must fail
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

#define DAY 86400000LL
#define SEEDS 8

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

static tsdb_options opts(int merge_threads)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 1;
    o.wal_sync_ms = 0;
    o.merge_threads = merge_threads;
    o.partition_classes = 0; /* one partition per day */
    o.wal_max_batch_rows = 64;  /* every insert below goes straight to a part */
    o.flush_interval_ms = 1000000;
    return o;
}

static int put(tsdb *db, tsdb_sid id, int64_t t0, int from, int to, float v)
{
    int n = to - from;
    int64_t *ts = malloc((size_t)n * sizeof *ts);
    float *vs = malloc((size_t)n * sizeof *vs);
    for (int i = 0; i < n; i++) {
        ts[i] = t0 + (int64_t)(from + i) * 1000;
        vs[i] = v;
    }
    int rc = tsdb_insert_series(db, id, ts, vs, (size_t)n);
    free(ts);
    free(vs);
    return rc;
}

static void make_fixture(const char *dir)
{
    sh("rm -rf '%s'%s", dir, "");
    tsdb_options o = opts(0);
    tsdb *db = NULL;
    CHECK(tsdb_open(dir, &o, &db) == 0);
    if (!db)
        return;
    tsdb_sid id;
    CHECK(tsdb_series(db, "s", &id) == 0);
    CHECK(put(db, id, 0, 0, 2500, 1) == 0); /* 500 rows of it nothing overwrites: its retirement shows */
    CHECK(put(db, id, 0, 0, 2000, 2) == 0);
    CHECK(put(db, id, 0, 500, 1500, 3) == 0);
    CHECK(put(db, id, DAY, 0, 100, 9) == 0);
    tsdb_close(db);
}

static size_t count_prefix(const char *dir, const char *prefix, char out[][128], size_t cap)
{
    DIR *d = opendir(dir);
    size_t n = 0;
    struct dirent *e;
    while (d && (e = readdir(d)))
        if (strncmp(e->d_name, prefix, strlen(prefix)) == 0) {
            if (out && n < cap)
                snprintf(out[n], 128, "%.127s", e->d_name);
            n++;
        }
    if (d)
        closedir(d);
    return n;
}

static int cmp_str(const void *a, const void *b) { return strcmp((const char *)a, (const char *)b); }

enum verdict { V_GONE, V_WHOLE, V_HALF, V_ERR };
static const char *vname[] = {"gone", "whole", "HALF", "error"};

/* Opens dir (no shim) and judges partition 0; partition 1 must be whole (100 rows of 9). */
static enum verdict judge(const char *dir)
{
    tsdb_options o = opts(0);
    tsdb *db = NULL;
    if (tsdb_open(dir, &o, &db) != 0)
        return V_ERR;
    tsdb_sid id;
    enum verdict v = V_ERR;
    tsdb_points p = {0}, q = {0};
    if (tsdb_series_lookup(db, "s", &id) == 0 && tsdb_query_points(db, id, 0, DAY, &p) == 0 &&
        tsdb_query_points(db, id, DAY, 2 * DAY, &q) == 0) {
        bool day1 = q.n == 100;
        for (size_t i = 0; i < q.n; i++)
            day1 = day1 && q.values[i] == 9;
        bool whole = p.n == 2500;
        for (size_t i = 0; whole && i < p.n; i++)
            whole = p.ts[i] == (int64_t)i * 1000 && p.values[i] == (i >= 2000 ? 1 : i >= 500 && i < 1500 ? 3 : 2);
        v = !day1 ? V_ERR : p.n == 0 ? V_GONE : whole ? V_WHOLE : V_HALF;
        if (v != V_GONE && v != V_WHOLE)
            fprintf(stderr, "  partition 0: %zu rows (first value %g), partition 1: %zu rows\n", p.n,
                    p.n ? (double)p.values[0] : 0.0, q.n);
    }
    tsdb_points_free(&p);
    tsdb_points_free(&q);
    tsdb_close(db);
    if (count_prefix(dir, "txn_", NULL, 0) != 0) {
        fprintf(stderr, "  an intent is left after the open\n");
        return V_ERR;
    }
    return v;
}

/* ---------------------------------------------------------------- kill */

static size_t exit_at;
static void hook_exit(eng *db, const char *name, size_t at)
{
    (void)db;
    (void)name;
    if (at == exit_at)
        _exit(0);
}

static void kill_drop(size_t at)
{
    char work[PATH_MAX];
    snprintf(work, sizeof work, "%s/kill", S);
    make_fixture(work);
    pid_t pid = fork();
    if (pid == 0) {
        tsdb_options o = opts(0);
        tsdb *db = NULL;
        if (tsdb_open(work, &o, &db) != 0)
            _exit(2);
        exit_at = at;
        drop_test_hook = hook_exit;
        tsdb_drop_before(db, DAY);
        _exit(3); /* the hook did not fire */
    }
    int st = -1;
    CHECK(pid > 0 && waitpid(pid, &st, 0) == pid && WIFEXITED(st) && WEXITSTATUS(st) == 0);
    enum verdict v = judge(work);
    if (v == V_HALF)
        printf("  kill after rename %zu: HALF DROP\n", at + 1);
    CHECK(v == V_GONE);
    printf("  tsdb_drop_before killed after rename %zu: the next open finds the partition %s\n", at + 1, vname[v]);
}

/* ----------------------------------------------------------------- ttl */

static char seen[16][128];
static atomic_int nseen;
static void hook_record(eng *db, const char *name, size_t at)
{
    (void)db;
    (void)at;
    int i = atomic_load(&nseen);
    if (i < 16) {
        snprintf(seen[i], 128, "%s", name);
        atomic_store(&nseen, i + 1);
    }
}

static size_t listed_parts(const char *dir)
{
    char names[64][128];
    size_t n = count_prefix(dir, "p", names, 64), k = 0;
    for (size_t i = 0; i < n && i < 64; i++)
        k += part_file_name(names[i]);
    return k;
}

static int ttl_child(const char *dir, bool kill)
{
    tsdb_options o = opts(1);
    tsdb *db = NULL;
    if (tsdb_open(dir, &o, &db) != 0)
        return 2;
    exit_at = 0;
    drop_test_hook = kill ? hook_exit : hook_record;
    tsdb_sid id;
    if (tsdb_series_lookup(db, "s", &id) != 0 || tsdb_set_retention(db, id, DAY) != 0)
        return 4;
    atomic_store(&nseen, 0);
    for (int i = 0; i < 200 && (kill || atomic_load(&nseen) < 4); i++) /* killed: the hook ends the process */
        usleep(50000);
    drop_test_hook = NULL;
    tsdb_close(db);
    return 0;
}

static void ttl(void)
{
    char work[PATH_MAX];
    snprintf(work, sizeof work, "%s/ttl", S);
    make_fixture(work);
    CHECK(ttl_child(work, false) == 0);
    int n = atomic_load(&nseen);
    bool order = n == 4;
    for (int i = 1; i < n; i++) {
        int64_t pa, pb;
        uint64_t ba, bb;
        order = order && sscanf(seen[i - 1], "p%" SCNd64 "_%" SCNu64, &pa, &ba) == 2 &&
                sscanf(seen[i], "p%" SCNd64 "_%" SCNu64, &pb, &bb) == 2 && (pa < pb || (pa == pb && ba < bb));
    }
    if (!order) {
        printf("  TTL ORDER WRONG:");
        for (int i = 0; i < n; i++)
            printf(" %s", seen[i]);
        printf("\n");
    }
    CHECK(order);
    CHECK(listed_parts(work) == 0);
    printf("  the TTL drop retires %d parts in block order: %s\n", n, order ? "yes" : "no");

    make_fixture(work);
    pid_t pid = fork();
    if (pid == 0)
        _exit(ttl_child(work, true) == 0 ? 3 : 2); /* 0 only from the hook */
    int st = -1;
    CHECK(pid > 0 && waitpid(pid, &st, 0) == pid && WIFEXITED(st) && WEXITSTATUS(st) == 0);
    tsdb_options o = opts(0);
    tsdb *db = NULL;
    CHECK(tsdb_open(work, &o, &db) == 0); /* the roll-forward */
    if (db)
        tsdb_close(db);
    size_t left = listed_parts(work);
    if (left)
        printf("  TTL drop killed after its first rename: %zu parts still listed: HALF DROP\n", left);
    CHECK(left == 0);
    printf("  the TTL drop killed after its first rename: %zu parts listed at the next open\n", left);
}

/* --------------------------------------------------------------- power */

static int run_child(const char *mode, const char *dir, const char *ack, int warmup, int seed)
{
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512], w[16], sd[16];
        snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0:detect_leaks=0",
                 getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
        setenv("ASAN_OPTIONS", asan, 1);
        snprintf(w, sizeof w, "%d", warmup);
        snprintf(sd, sizeof sd, "%d", seed);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", dir, 1);
        setenv("CRASHFS_PROB", "1", 1);
        setenv("CRASHFS_WARMUP", w, 1);
        setenv("CRASHFS_AT_EXIT", "1", 1);
        setenv("CRASHFS_TEAR", "1", 1);
        setenv("CRASHFS_SEED", sd, 1);
        int null = open("/dev/null", O_WRONLY);
        if (null >= 0) {
            dup2(null, 1);
            dup2(null, 2);
        }
        execl(SELF, "test_drops", mode, dir, ack, (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

/* --child-drop DIR ACK: open, drop, acknowledge; --child-open DIR -: the open alone */
static int child_main(bool drop, const char *dir, const char *ack)
{
    tsdb_options o = opts(0);
    tsdb *db = NULL;
    if (tsdb_open(dir, &o, &db) != 0)
        return 2;
    if (drop && tsdb_drop_before(db, DAY) == 0) {
        int fd = open(ack, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd >= 0)
            close(fd);
    }
    tsdb_close(db);
    return 0;
}

static void write_intent(const char *dir, char names[][128], size_t n, bool torn, bool damaged)
{
    char buf[4096];
    size_t len = (size_t)snprintf(buf, sizeof buf, "tsdb-drop 1 %zu\n", n);
    for (size_t i = 0; i < n; i++)
        len += (size_t)snprintf(buf + len, sizeof buf - len, "%s\n", names[i]);
    if (!torn)
        len += (size_t)snprintf(buf + len, sizeof buf - len, "end %016" PRIx64 "\n", xxh64(buf, len, 0));
    if (damaged)
        buf[14] ^= 1; /* a byte of the first name */
    char p[PATH_MAX + 64];
    snprintf(p, sizeof p, "%s/txn_1_100.intent", dir);
    FILE *f = fopen(p, "w");
    CHECK(f && fwrite(buf, 1, len, f) == len);
    if (f)
        fclose(f);
}

static void retire_by_hand(const char *dir, const char *name)
{
    char a[PATH_MAX + 200], b[PATH_MAX + 200];
    snprintf(a, sizeof a, "%s/%s", dir, name);
    snprintf(b, sizeof b, "%s/retired_%s", dir, name);
    CHECK(rename(a, b) == 0);
}

/* the three part names of partition 0, sorted (block order: the blocks are single digits here) */
static size_t p0_names(const char *dir, char names[][128])
{
    size_t n = count_prefix(dir, "p0_", names, 8);
    qsort(names, n < 8 ? n : 8, 128, cmp_str);
    return n;
}

static void sweep(bool drop)
{
    char src[PATH_MAX], work[PATH_MAX], ack[PATH_MAX + 8];
    snprintf(src, sizeof src, "%s/src", S);
    snprintf(work, sizeof work, "%s/work", S);
    snprintf(ack, sizeof ack, "%s/ack", S);
    make_fixture(src);
    if (!drop) { /* a drop that wrote its intent and renamed one part, then died */
        char names[8][128];
        CHECK(p0_names(src, names) == 3);
        write_intent(src, names, 3, false, false);
        retire_by_hand(src, names[0]);
    }
    const char *what = drop ? "tsdb_drop_before" : "the open rolling a drop intent forward";
    int bad = 0, steps = 0, gone = 0, whole = 0;
    bool half = false;
    for (int w = 0; w < 5000 && bad < 3; w++) {
        bool ended = false;
        for (int seed = 0; seed < SEEDS && bad < 3; seed++) {
            sh("rm -rf '%s'%s", work, "");
            sh("cp -a '%s' '%s'", src, work);
            unlink(ack);
            int st = run_child(drop ? "--child-drop" : "--child-open", work, ack, w, seed);
            if (st != 99 && st != 0) {
                fprintf(stderr, "%s: step %d seed %d: the child exited with %d\n", what, w, seed, st);
                failures++;
                bad++;
                continue;
            }
            ended = ended || st == 0;
            bool acked = access(ack, F_OK) == 0;
            if (getenv("TSDB_DROPS_TRACE")) {
                char tr[64][128];
                size_t k = count_prefix(work, "", tr, 64);
                printf("w%d s%d st%d:", w, seed, st);
                for (size_t i = 0; i < k && i < 64; i++)
                    if (strstr(tr[i], "p0_") || strncmp(tr[i], "txn_", 4) == 0)
                        printf(" %s", tr[i]);
                printf("\n");
            }
            enum verdict v = judge(work);
            gone += v == V_GONE;
            whole += v == V_WHOLE;
            half = half || v == V_HALF;
            if (v == V_HALF || v == V_ERR || (v == V_WHOLE && (acked || !drop))) {
                fprintf(stderr, "%s: power lost before operation %d (seed %d): partition 0 %s%s\n", what, w, seed,
                        vname[v], acked ? " (acknowledged)" : "");
                failures++;
                bad++;
            }
        }
        steps = w + 1;
        if (ended)
            break;
    }
    if (drop && (!gone || !whole)) {
        fprintf(stderr, "%s: %d cuts left it gone, %d whole: the sweep missed a side\n", what, gone, whole);
        failures++;
    }
    if (half)
        printf("  %s: HALF DROP\n", what);
    printf("  %s: power lost at each of %d steps (%d seeds each; %d gone, %d whole): %s\n", what, steps, SEEDS, gone,
           whole,
           bad ? "FAILED" : drop ? "whole or gone, gone once acknowledged" : "gone");
}

/* ------------------------------------------------------- torn, damaged, ec */

static void crafted(void)
{
    char work[PATH_MAX], names[8][128];
    snprintf(work, sizeof work, "%s/crafted", S);

    make_fixture(work);
    CHECK(p0_names(work, names) == 3);
    write_intent(work, names, 3, true, false);
    enum verdict v = judge(work);
    CHECK(v == V_WHOLE);
    printf("  a torn drop intent: the drop never started, partition 0 %s\n", vname[v]);

    make_fixture(work);
    CHECK(p0_names(work, names) == 3);
    write_intent(work, names, 3, false, true);
    for (int i = 0; i < 3; i++)
        retire_by_hand(work, names[i]);
    v = judge(work);
    CHECK(v == V_GONE);
    printf("  a damaged drop intent after all its renames: read as torn, the renames stand: partition 0 %s\n",
           vname[v]);

    make_fixture(work);
    size_t n = p0_names(work, names);
    CHECK(n == 3);
    snprintf(names[n], 128, "p0_90_90_0.ec"); /* a stand-in: without a tier an .ec file is only a name to the open */
    char p[PATH_MAX + 64];
    snprintf(p, sizeof p, "%s/%s", work, names[n]);
    FILE *f = fopen(p, "w");
    CHECK(f != NULL);
    if (f)
        fclose(f);
    write_intent(work, names, n + 1, false, false);
    tsdb_options o = opts(0);
    tsdb *db = NULL;
    int rc = tsdb_open(work, &o, &db);
    if (db)
        tsdb_close(db);
    snprintf(p, sizeof p, "%s/retired_p0_90_90_0.ec", work);
    bool retired = access(p, F_OK) == 0;
    snprintf(p, sizeof p, "%s/p0_90_90_0.ec", work);
    bool left = access(p, F_OK) == 0;
    if (!retired || left || rc != 0)
        printf("  EC LEFT: the open said %d, retired_ %s, the .ec name %s\n", rc, retired ? "there" : "missing",
               left ? "still there" : "gone");
    CHECK(rc == 0 && retired && !left);
    printf("  an .ec name in a drop intent becomes retired_<name>.ec: %s\n", retired && !left ? "yes" : "no");
}

static int parse_mutant(const char *m)
{
    static const char *names[] = {"", "nointent", "nosync", "unlinkfirst", "ttlreverse", "skipec"};
    for (int i = 1; i < (int)(sizeof names / sizeof *names); i++)
        if (strcmp(m, names[i]) == 0)
            return i;
    return -1;
}

int main(int argc, char **argv)
{
    if (getenv("TSDB_DROP_MUTANT"))
        drop_test_mutant = parse_mutant(getenv("TSDB_DROP_MUTANT"));
    if (argc == 4 && (strcmp(argv[1], "--child-drop") == 0 || strcmp(argv[1], "--child-open") == 0))
        return child_main(strcmp(argv[1], "--child-drop") == 0, argv[2], argv[3]);
    const char *scratch = "/tmp";
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mutant") == 0 && i + 1 < argc) {
            if ((drop_test_mutant = parse_mutant(argv[++i])) < 0) {
                fprintf(stderr, "unknown mutant %s\n", argv[i]);
                return 2;
            }
            setenv("TSDB_DROP_MUTANT", argv[i], 1); /* the processes the shim cuts run the same bug */
        } else {
            scratch = argv[i];
        }
    }
    char tmpl[sizeof S];
    snprintf(tmpl, sizeof tmpl, "%.*s/test-drops.XXXXXX", (int)sizeof S - 32, scratch);
    if (!mkdtemp(tmpl)) {
        perror("mkdtemp");
        return 1;
    }
    snprintf(S, sizeof S, "%s", tmpl);
    if (!realpath(argv[0], SELF))
        snprintf(SELF, sizeof SELF, "%s", argv[0]);
    char *slash = strrchr(SELF, '/');
    snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", slash ? (int)(slash - SELF) : 1, slash ? SELF : ".");
    printf("test_drops: a drop is whole or not at all after a crash\n");
    kill_drop(0);
    kill_drop(1);
    ttl();
    crafted();
    if (access(SHIM, R_OK) == 0) {
        sweep(true);
        sweep(false);
    } else {
#ifdef __linux__
        fprintf(stderr, "test_drops: %s is missing\n", SHIM);
        failures++;
#else
        printf("test_drops: the power-loss sweeps skipped (no %s on this platform)\n", SHIM);
#endif
    }
    if (failures == 0)
        sh("rm -rf '%s'%s", S, "");
    if (failures)
        printf("test_drops: %d FAILED\n", failures);
    else
        printf("test_drops: all passed\n");
    return failures != 0;
}
