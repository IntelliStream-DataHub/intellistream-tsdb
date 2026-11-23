/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * PPOS (src/ppos.h, r8d): the log position of every part of an erasure-coded cluster volume, durable before the
 * part's name. The oracle is the positions the append sites report (ppos_test_note, told of every row with its
 * number before ppos_note runs): a flush part's ppos is the highest position of a row in any day of its key, of the
 * memtable it flushed; a merge's the highest of its sources'; a part a plain writer's large insert, delete or replace
 * commits has no marker and records PPOS_UNKNOWN, the top of the range (a leader's COMMIT position is test_replica's
 * ppos cases).
 *
 *  - days: rows of two days in one memtable, each day's part gets its own day's highest (mutant pposflushhi);
 *  - classes: a class-1 series' 7-day key counts a class-0 row of one of its days (over, never under);
 *  - writes: a merge of flush parts records their maximum; a large insert, a delete and a replace record PPOS_UNKNOWN
 *    (mutant pposunknown0), a merge over them stays unknown, and the open's compaction keeps the flag (pposflags);
 *  - fallback: a memtable whose map could not be allocated gives its parts the memtable's fno_hi, at or above every
 *    row's position (mutant pposfallback0);
 *  - failstop: a failed write of a record fails the engine, which takes no more writes (mutant pposnofail);
 *  - badgood: a damaged record of a dead name before a valid one fails the open (mutant pposnobadgood);
 *  - replay: rows a process left in the WAL, replayed and flushed by the next open, get the positions the process
 *    gave them and the same ppos (mutant pposreplay);
 *  - kill: a flush, then the process dies: the next open finds every record (mutant pposafter);
 *  - open: a listed part without a record, a bad record before a good one, a torn tail, a compaction;
 *  - race: flushes, merges and ppos_set at once (run under TSan);
 *  - power: crashfs power loss at every step of rows, flushes, large inserts, deletes and merges, of an open that
 *    replays and of an open that compacts: the next open never finds a named part without its record (mutants
 *    pposnosync, pposafter).
 *
 * Usage: test_ppos [--mutant NAME] [SCRATCH]
 */
#define _GNU_SOURCE
#include "internal.h"
#include "ppos.h"
#include "feed.h"

#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

#define DAY 86400000LL
#define T0 (19675LL * DAY) /* 2023-11-14 */

static char S[3072], SELF[PATH_MAX], SHIM[PATH_MAX];

static void sh(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void sh(const char *fmt, ...)
{
    char cmd[4 * PATH_MAX];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(cmd, sizeof cmd, fmt, ap);
    va_end(ap);
    if (system(cmd) != 0)
        fprintf(stderr, "command failed: %s\n", cmd);
}

static int open_db(const char *dir, eng **db)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.wal_sync_ms = 0;
    o.wal_max_batch_rows = 64; /* more rows go straight to parts */
    o.stats = -1;
    meta_cluster_open_allow(true);
    *db = NULL;
    int rc = eng_open(dir, &o, db);
    meta_cluster_open_allow(false);
    return rc;
}

/* a fresh cluster ec volume (requires=cluster,ec, as r8f's attach writes it) */
static void fresh(char *dir, const char *name)
{
    snprintf(dir, PATH_MAX, "%s/%s", S, name);
    sh("rm -rf '%s' && mkdir -p '%s'", dir, dir);
    eng *db = NULL;
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    if (eng_open(dir, &o, &db) != 0) {
        fprintf(stderr, "open of %s failed\n", dir);
        exit(2);
    }
    eng_close(db);
    int fd = open(dir, O_RDONLY | O_DIRECTORY);
    if (fd < 0 || meta_require(fd, true, META_REQ_CLUSTER | META_REQ_EC) != 0)
        exit(2);
    close(fd);
}

/* ------------------------------------------------------------ the oracle */

static struct note {
    int64_t ts;
    uint64_t pos;
    int site;
} *notes;
static size_t nnotes, notecap;
static pthread_mutex_t note_mu = PTHREAD_MUTEX_INITIALIZER;

static void note_hook(eng *db, memtable *m, int site, const int64_t *ts, size_t n, uint64_t fno, const uint32_t *ix)
{
    (void)db, (void)m;
    pthread_mutex_lock(&note_mu);
    for (size_t j = 0; j < n; j++) {
        if (nnotes == notecap) {
            notecap = notecap ? 2 * notecap : 1024;
            notes = realloc(notes, notecap * sizeof *notes);
        }
        notes[nnotes++] = (struct note){ts[j], fno + (ix ? ix[j] : j), site};
    }
    pthread_mutex_unlock(&note_mu);
}

/* the highest position of a noted row in a day of key's range */
static uint64_t expect_key(eng *db, int64_t key)
{
    int64_t lo, hi;
    partition_range(db, key, &lo, &hi);
    int64_t dlo = floor_div(lo, db->partition_ms), dhi = floor_div(hi - 1, db->partition_ms);
    uint64_t e = 0;
    for (size_t i = 0; i < nnotes; i++) {
        int64_t d = floor_div(notes[i].ts, db->partition_ms);
        if (d >= dlo && d <= dhi && notes[i].pos > e)
            e = notes[i].pos;
    }
    return e;
}

/* names listed before a step, to tell its new parts */
typedef struct {
    char (*name)[128];
    uint64_t *pos;
    int64_t *key;
    size_t n;
} listing;

static void list_parts(eng *db, listing *l)
{
    free(l->name), free(l->pos), free(l->key);
    l->n = db->nparts;
    l->name = calloc(l->n + 1, sizeof *l->name);
    l->pos = calloc(l->n + 1, sizeof *l->pos);
    l->key = calloc(l->n + 1, sizeof *l->key);
    for (size_t i = 0; i < l->n; i++) {
        snprintf(l->name[i], sizeof l->name[i], "%s", db->parts[i]->name);
        l->key[i] = db->parts[i]->partition;
        CHECK(ppos_of(db, l->name[i], &l->pos[i]) == 0);
    }
}

static bool listed(const listing *l, const char *name)
{
    for (size_t i = 0; i < l->n; i++)
        if (strcmp(l->name[i], name) == 0)
            return true;
    return false;
}

static void free_listing(listing *l)
{
    free(l->name), free(l->pos), free(l->key);
    memset(l, 0, sizeof *l);
}

/* every part not in `before` (a flush's) has its key's highest noted position; the notes are then forgotten */
static int check_flush(eng *db, const listing *before, const char *what)
{
    int seen = 0;
    for (size_t i = 0; i < db->nparts; i++) {
        part *p = db->parts[i];
        if (listed(before, p->name))
            continue;
        uint64_t got = 0, want = expect_key(db, p->partition);
        CHECK(ppos_of(db, p->name, &got) == 0);
        if (got != want) {
            fprintf(stderr, "%s: part %s has ppos %" PRIu64 ", its days' rows reach %" PRIu64 "\n", what, p->name, got,
                    want);
            failures++;
        }
        seen++;
    }
    nnotes = 0;
    return seen;
}

static void rows(eng *db, uint32_t s, int64_t t0, int n, int step_ms)
{
    int64_t ts[256];
    float v[256];
    for (int i = 0; i < n; i++)
        ts[i] = t0 + (int64_t)i * step_ms, v[i] = (float)(i + s);
    CHECK(eng_insert_series(db, TSDB_FLOAT32, s, ts, v, (size_t)n) == 0);
}

static uint32_t series(eng *db, const char *name)
{
    uint32_t id = 0;
    CHECK(eng_series(db, name, TSDB_FLOAT32, &id) == 0);
    return id;
}

/* ------------------------------------------------------------ cases */

static void case_days(void)
{
    char dir[PATH_MAX];
    fresh(dir, "days");
    eng *db;
    CHECK(open_db(dir, &db) == 0);
    if (!db)
        return;
    ppos_test_note = note_hook;
    nnotes = 0;
    uint32_t a = series(db, "a"), b = series(db, "b");
    listing before = {0};
    list_parts(db, &before);
    rows(db, a, T0 + 1000, 5, 1000);         /* day 0 */
    rows(db, b, T0 + 5 * DAY + 1000, 5, 1000); /* day 5 */
    rows(db, a, T0 + 10000, 3, 1000);        /* day 0 again: its part's highest is here */
    rows(db, b, T0 + 2 * DAY, 2, 1000);      /* day 2 */
    uint64_t hi5 = 0, hi0 = 0;
    for (size_t i = 0; i < nnotes; i++) {
        int64_t d = floor_div(notes[i].ts, DAY) - T0 / DAY;
        if (d == 5)
            hi5 = notes[i].pos > hi5 ? notes[i].pos : hi5;
        if (d == 0)
            hi0 = notes[i].pos > hi0 ? notes[i].pos : hi0;
    }
    CHECK(hi5 != 0 && hi0 > hi5);
    CHECK(eng_flush(db) == 0);
    CHECK(check_flush(db, &before, "days") == 3);
    /* a second memtable: today's rows never raise the old day's part */
    list_parts(db, &before);
    rows(db, a, T0 + 5 * DAY + 100000, 4, 1000);
    rows(db, a, T0 + 9 * DAY, 4, 1000);
    CHECK(eng_flush(db) == 0);
    CHECK(check_flush(db, &before, "days, second flush") == 2);
    CHECK(ppos_test_records(db) == db->nparts);
    free_listing(&before);
    ppos_test_note = NULL;
    CHECK(eng_close(db) == 0);
    CHECK(open_db(dir, &db) == 0); /* every listed part has its record */
    if (db)
        eng_close(db);
    printf("  days: each flush part has the highest position of its own days' rows\n");
}

static void case_classes(void)
{
    char dir[PATH_MAX];
    fresh(dir, "classes");
    eng *db;
    CHECK(open_db(dir, &db) == 0);
    if (!db)
        return;
    ppos_test_note = note_hook;
    nnotes = 0;
    uint32_t a = series(db, "a"), b = series(db, "b");
    CHECK(cls_test_apply(db, b, 0, 1) == 0); /* b in 7-day keys (in memory: this volume is not reopened) */
    int64_t key = partition_of(db, b, T0);
    int64_t lo, hi;
    partition_range(db, key, &lo, &hi);
    CHECK(partition_class(db, key) == 1 && hi - lo == 7 * DAY);
    listing before = {0};
    list_parts(db, &before);
    rows(db, b, lo + 1000, 5, 1000);           /* b in the key's first day */
    rows(db, a, lo + 3 * DAY + 1000, 5, 1000); /* a, class 0, later, in the key's fourth day */
    uint64_t bmax = 0, amax = 0;
    for (size_t i = 0; i < nnotes; i++)
        if (notes[i].ts < lo + DAY)
            bmax = notes[i].pos > bmax ? notes[i].pos : bmax;
        else
            amax = notes[i].pos > amax ? notes[i].pos : amax;
    CHECK(eng_flush(db) == 0);
    uint64_t bp = 0;
    for (size_t i = 0; i < db->nparts; i++)
        if (db->parts[i]->partition == key)
            CHECK(ppos_of(db, db->parts[i]->name, &bp) == 0);
    CHECK(bp == amax && amax > bmax); /* a's row in one of b's days counts: over, never under */
    CHECK(check_flush(db, &before, "classes") == 2);
    free_listing(&before);
    ppos_test_note = NULL;
    eng_close(db);
    printf("  classes: a class-1 key counts a class-0 row of one of its days (over-approximated, never under)\n");
}

static void case_writes(void)
{
    char dir[PATH_MAX];
    fresh(dir, "writes");
    eng *db;
    CHECK(open_db(dir, &db) == 0);
    if (!db)
        return;
    ppos_test_note = note_hook;
    nnotes = 0;
    uint32_t a = series(db, "a");
    listing before = {0};
    list_parts(db, &before);
    rows(db, a, T0, 10, 1000);
    CHECK(eng_flush(db) == 0);
    CHECK(check_flush(db, &before, "writes") == 1);
    list_parts(db, &before);
    rows(db, a, T0 + 30000, 10, 1000);
    CHECK(eng_flush(db) == 0);
    CHECK(check_flush(db, &before, "writes, second flush") == 1);
    /* a merge of known positions: their maximum */
    list_parts(db, &before);
    uint64_t want = 0;
    for (size_t i = 0; i < before.n; i++)
        want = before.pos[i] > want ? before.pos[i] : want;
    CHECK(before.n == 2 && want != 0 && want != PPOS_UNKNOWN);
    CHECK(eng_optimize(db) == 0);
    CHECK(db->nparts == 1);
    uint64_t got = 0;
    if (db->nparts == 1) {
        CHECK(ppos_of(db, db->parts[0]->name, &got) == 0 && got == want);
        CHECK(ppos_test_flags(db, db->parts[0]->name) == PPOS_F_GIVEN);
    }
    list_parts(db, &before);
    rows(db, a, T0 + 100000, 200, 10); /* a large insert: no marker on a plain writer */
    CHECK(eng_delete_range(db, a, T0 + 2000, T0 + 4000) == 0);
    int64_t ts[100];
    float v[100];
    for (int i = 0; i < 100; i++)
        ts[i] = T0 + 50000 + i, v[i] = (float)i;
    CHECK(eng_replace_range(db, TSDB_FLOAT32, a, T0 + 50000, T0 + 60000, ts, v, 100) == 0);
    int nnew = 0;
    for (size_t i = 0; i < db->nparts; i++) {
        if (listed(&before, db->parts[i]->name))
            continue;
        got = 0;
        CHECK(ppos_of(db, db->parts[i]->name, &got) == 0);
        if (got != PPOS_UNKNOWN)
            fprintf(stderr, "writes: the part %s of a write with no marker has ppos %" PRIu64 ", not unknown\n",
                    db->parts[i]->name, got);
        CHECK(got == PPOS_UNKNOWN);
        CHECK(ppos_test_flags(db, db->parts[i]->name) == PPOS_F_UNKNOWN);
        nnew++;
    }
    CHECK(nnew >= 3);
    nnotes = 0;
    rows(db, a, T0 + 7000, 3, 1000);
    list_parts(db, &before);
    CHECK(eng_flush(db) == 0);
    CHECK(check_flush(db, &before, "writes, after") == 1);
    /* a merge over unknown sources: unknown (the maximum, the top of the range) */
    list_parts(db, &before);
    CHECK(before.n >= 5);
    CHECK(eng_optimize(db) == 0);
    CHECK(db->nparts == 1);
    want = PPOS_UNKNOWN;
    if (db->nparts == 1) {
        CHECK(ppos_of(db, db->parts[0]->name, &got) == 0 && got == want);
        CHECK(ppos_test_flags(db, db->parts[0]->name) == PPOS_F_UNKNOWN);
    }
    free_listing(&before);
    ppos_test_note = NULL;
    CHECK(eng_close(db) == 0);
    CHECK(open_db(dir, &db) == 0);
    if (db) {
        CHECK(ppos_test_records(db) == 1); /* the open compacted: the merged sources' records were most */
        if (db->nparts == 1) {
            CHECK(ppos_of(db, db->parts[0]->name, &got) == 0 && got == want);
            CHECK(ppos_test_flags(db, db->parts[0]->name) == PPOS_F_UNKNOWN);
        }
        eng_close(db);
    }
    CHECK(open_db(dir, &db) == 0); /* the compacted file itself keeps the flag */
    if (db) {
        if (db->nparts == 1)
            CHECK(ppos_test_flags(db, db->parts[0]->name) == PPOS_F_UNKNOWN);
        eng_close(db);
    }
    printf("  writes: a merge records its sources' maximum; a large insert, a delete and a replace record unknown on a "
           "plain writer, a merge over them too, and the open's compaction keeps the flag\n");
}

static int ppos_fd(const char *dir, int flags);
static off_t ppos_size(const char *dir);

/* the map could not be allocated: each flush part takes the memtable's fno_hi, never less than a row's position */
static void case_fallback(void)
{
    char dir[PATH_MAX];
    fresh(dir, "fallback");
    eng *db;
    CHECK(open_db(dir, &db) == 0);
    if (!db)
        return;
    ppos_test_note = note_hook;
    nnotes = 0;
    uint32_t a = series(db, "a"), b = series(db, "b");
    listing before = {0};
    list_parts(db, &before);
    __atomic_store_n(&ppos_test_lose_map, 1, __ATOMIC_RELAXED);
    rows(db, a, T0 + 1000, 5, 1000);
    rows(db, b, T0 + 3 * DAY, 5, 1000);
    uint64_t top = 0;
    for (size_t i = 0; i < nnotes; i++)
        top = notes[i].pos > top ? notes[i].pos : top;
    CHECK(top != 0);
    CHECK(eng_flush(db) == 0);
    __atomic_store_n(&ppos_test_lose_map, 0, __ATOMIC_RELAXED);
    int seen = 0;
    for (size_t i = 0; i < db->nparts; i++) {
        if (listed(&before, db->parts[i]->name))
            continue;
        uint64_t got = 0;
        CHECK(ppos_of(db, db->parts[i]->name, &got) == 0);
        if (got < top || got == PPOS_UNKNOWN)
            fprintf(stderr, "fallback: part %s has ppos %" PRIu64 ", below the memtable's highest %" PRIu64 "\n",
                    db->parts[i]->name, got, top);
        CHECK(got >= top && got != PPOS_UNKNOWN);
        CHECK(ppos_test_flags(db, db->parts[i]->name) == PPOS_F_FALLBACK);
        seen++;
    }
    CHECK(seen == 2);
    nnotes = 0;
    free_listing(&before);
    ppos_test_note = NULL;
    CHECK(eng_close(db) == 0);
    printf("  fallback: a memtable without its map gives its parts its fno_hi, never less than a row's position\n");
}

/* a failed write of a record: the engine fails and acknowledges nothing more; the next open finds every record */
static void case_failstop(void)
{
    char dir[PATH_MAX];
    fresh(dir, "failstop");
    eng *db;
    CHECK(open_db(dir, &db) == 0);
    if (!db)
        return;
    uint32_t a = series(db, "a");
    rows(db, a, T0, 5, 1000);
    CHECK(eng_flush(db) == 0);
    rows(db, a, T0 + DAY, 5, 1000);
    __atomic_store_n(&ppos_test_fail_errno, EIO, __ATOMIC_RELAXED);
    fprintf(stderr, "  (a failure is logged next: an injected EIO on PPOS)\n");
    CHECK(eng_flush(db) != 0);
    __atomic_store_n(&ppos_test_fail_errno, 0, __ATOMIC_RELAXED);
    CHECK(eng_check(db) != 0); /* fail-stop */
    int64_t ts = T0 + 2 * DAY;
    float v = 1;
    CHECK(eng_insert_series(db, TSDB_FLOAT32, a, &ts, &v, 1) != 0);
    CHECK(eng_flush(db) != 0);
    eng_close(db);
    CHECK(open_db(dir, &db) == 0); /* the WAL replays; every listed part has its record */
    if (db) {
        CHECK(db->nparts >= 1);
        eng_close(db);
    }
    printf("  failstop: a failed write of a record fails the engine, nothing more is acknowledged\n");
}

/* a bad record before a valid one fails the open even when both name dead parts (no listed part misses a record) */
static void case_badgood(void)
{
    char dir[PATH_MAX];
    fresh(dir, "badgood");
    eng *db;
    CHECK(open_db(dir, &db) == 0);
    if (!db)
        return;
    uint32_t a = series(db, "a");
    rows(db, a, T0, 5, 1000);
    CHECK(eng_flush(db) == 0);
    CHECK(db->nparts == 1);
    CHECK(ppos_set(db, "p99_1_1_0.ec", 5) == 0); /* names never listed: dead records */
    CHECK(ppos_set(db, "p99_2_2_0.ec", 6) == 0);
    CHECK(eng_close(db) == 0);
    CHECK(ppos_size(dir) == 96);
    char bak[PATH_MAX + 16];
    snprintf(bak, sizeof bak, "%s.bak", dir);
    sh("rm -rf '%s' && cp -a '%s' '%s'", bak, dir, bak);
    int fd = ppos_fd(dir, O_RDWR);
    uint8_t byte = 0;
    CHECK(fd >= 0 && pread(fd, &byte, 1, 32 + 9) == 1);
    byte ^= 1;
    CHECK(pwrite(fd, &byte, 1, 32 + 9) == 1);
    close(fd);
    fprintf(stderr, "  (a refusal is logged next: damage before a valid record)\n");
    CHECK(open_db(dir, &db) == TSDB_ERR_CORRUPT && db == NULL);
    CHECK(ppos_size(dir) == 96); /* nothing cut */
    sh("rm -rf '%s' && mv '%s' '%s'", dir, bak, dir);
    CHECK(open_db(dir, &db) == 0);
    if (db) {
        CHECK(ppos_test_records(db) == 1); /* two dead of three: compacted */
        eng_close(db);
    }
    printf("  badgood: a damaged dead record before a valid one fails the open\n");
}

/* a child that leaves rows in the WAL (or flushes, then dies) and writes the positions it noted */
static void child_rows(const char *dir, const char *ack, bool flush)
{
    eng *db;
    if (open_db(dir, &db) != 0)
        _exit(3);
    ppos_test_note = note_hook;
    uint32_t a = series(db, "a"), b = series(db, "b");
    rows(db, a, T0 + 1000, 5, 1000);
    rows(db, b, T0 + 3 * DAY, 5, 1000);
    rows(db, a, T0 + 20000, 5, 1000);
    if (flush && eng_flush(db) != 0)
        _exit(4);
    FILE *f = fopen(ack, "w");
    for (size_t i = 0; f && i < nnotes; i++)
        fprintf(f, "%" PRId64 " %" PRIu64 "\n", notes[i].ts, notes[i].pos);
    if (f)
        fclose(f);
    _exit(failures ? 5 : 0); /* no close: the WAL keeps the rows */
}

static int run_child(const char *kind, const char *dir, const char *ack, bool shim, int warmup, int seed)
{
    pid_t pid = fork();
    if (pid == 0) {
        if (shim) {
            char asan[512], w[16], sd[16];
            snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0",
                     getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
            setenv("ASAN_OPTIONS", asan, 1);
            snprintf(w, sizeof w, "%d", warmup);
            snprintf(sd, sizeof sd, "%d", seed);
            setenv("LD_PRELOAD", SHIM, 1);
            setenv("CRASHFS_ROOT", dir, 1);
            setenv("CRASHFS_PROB", warmup >= 0 ? "1" : "0", 1);
            setenv("CRASHFS_WARMUP", w, 1);
            setenv("CRASHFS_AT_EXIT", "1", 1);
            setenv("CRASHFS_TEAR", "1", 1);
            setenv("CRASHFS_SEED", sd, 1);
            int null = open("/dev/null", O_WRONLY);
            if (null >= 0)
                dup2(null, 2);
        }
        execl(SELF, "test_ppos", "--child", kind, dir, ack, (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

static void case_replay(void)
{
    char dir[PATH_MAX], ack[PATH_MAX + 16];
    fresh(dir, "replay");
    snprintf(ack, sizeof ack, "%s/replay.ack", S);
    CHECK(run_child("rows", dir, ack, false, -1, 0) == 0);
    struct note child[64];
    size_t nc = 0;
    FILE *f = fopen(ack, "r");
    while (f && nc < 64 && fscanf(f, "%" SCNd64 " %" SCNu64, &child[nc].ts, &child[nc].pos) == 2)
        nc++;
    if (f)
        fclose(f);
    CHECK(nc == 15);
    ppos_test_note = note_hook;
    nnotes = 0;
    eng *db;
    CHECK(open_db(dir, &db) == 0); /* replays the rows and flushes them */
    ppos_test_note = NULL;
    if (!db)
        return;
    CHECK(nnotes == nc);
    for (size_t i = 0; i < nnotes && i < nc; i++) /* the replay's positions are the ones mem_insert gave */
        CHECK(notes[i].site == PPOS_SITE_REPLAY && notes[i].ts == child[i].ts && notes[i].pos == child[i].pos);
    listing none = {0};
    CHECK(check_flush(db, &none, "replay") == 2);
    eng_close(db);
    printf("  replay: rows replayed at the open get the positions mem_insert gave them, and their parts the same "
           "ppos\n");
}

static void case_kill(void)
{
    char dir[PATH_MAX], ack[PATH_MAX + 16];
    fresh(dir, "kill");
    snprintf(ack, sizeof ack, "%s/kill.ack", S);
    CHECK(run_child("flushdie", dir, ack, false, -1, 0) == 0);
    eng *db;
    int rc = open_db(dir, &db);
    if (rc != 0)
        printf("  KILL: the open after a flush and a kill failed (%d)\n", rc);
    CHECK(rc == 0);
    if (db) {
        CHECK(db->nparts == 2);
        eng_close(db);
    }
    printf("  kill: a flush, then the process dies: the next open finds every record\n");
}

static int ppos_fd(const char *dir, int flags)
{
    char p[PATH_MAX + 8];
    snprintf(p, sizeof p, "%s/PPOS", dir);
    return open(p, flags);
}

static off_t ppos_size(const char *dir)
{
    char p[PATH_MAX + 8];
    snprintf(p, sizeof p, "%s/PPOS", dir);
    struct stat st;
    return stat(p, &st) == 0 ? st.st_size : -1;
}

static void case_open(void)
{
    char dir[PATH_MAX];
    fresh(dir, "open");
    eng *db;
    CHECK(open_db(dir, &db) == 0);
    if (!db)
        return;
    uint32_t a = series(db, "a");
    rows(db, a, T0, 5, 1000);
    rows(db, a, T0 + 2 * DAY, 5, 1000);
    CHECK(eng_flush(db) == 0);
    CHECK(db->nparts == 2);
    CHECK(eng_close(db) == 0);
    char bak[PATH_MAX + 16];
    snprintf(bak, sizeof bak, "%s.bak", dir);
    sh("rm -rf '%s' && cp -a '%s' '%s'", bak, dir, bak);
    CHECK(ppos_size(dir) == 64);
    /* a torn tail: cut */
    int fd = ppos_fd(dir, O_WRONLY | O_APPEND);
    CHECK(fd >= 0 && write(fd, "torn tail", 9) == 9);
    close(fd);
    CHECK(open_db(dir, &db) == 0);
    if (db)
        eng_close(db);
    CHECK(ppos_size(dir) == 64);
    /* the second record lost: the part it named is listed without one */
    char p[PATH_MAX + 8];
    snprintf(p, sizeof p, "%s/PPOS", dir);
    CHECK(truncate(p, 32) == 0);
    fprintf(stderr, "  (a refusal is logged next: a listed part without a record)\n");
    CHECK(open_db(dir, &db) == TSDB_ERR_CORRUPT && db == NULL);
    /* no file at all, with parts listed */
    unlink(p);
    fprintf(stderr, "  (a refusal is logged next: no PPOS, parts listed)\n");
    CHECK(open_db(dir, &db) == TSDB_ERR_CORRUPT);
    /* a bad first record before a good second one */
    sh("rm -rf '%s' && cp -a '%s.bak' '%s'", dir, dir, dir);
    fd = ppos_fd(dir, O_RDWR);
    uint8_t byte = 0;
    CHECK(fd >= 0 && pread(fd, &byte, 1, 9) == 1);
    byte ^= 1;
    CHECK(pwrite(fd, &byte, 1, 9) == 1);
    close(fd);
    fprintf(stderr, "  (a refusal is logged next: damage before a valid record)\n");
    CHECK(open_db(dir, &db) == TSDB_ERR_CORRUPT);
    sh("rm -rf '%s' && cp -a '%s.bak' '%s'", dir, dir, dir);
    CHECK(open_db(dir, &db) == 0);
    if (db)
        eng_close(db);
    /* a single-server volume and a non-ec cluster volume keep no PPOS */
    char d2[PATH_MAX];
    snprintf(d2, sizeof d2, "%s/plain", S);
    sh("rm -rf '%s' && mkdir -p '%s'", d2, d2);
    CHECK(open_db(d2, &db) == 0);
    if (db) {
        CHECK(db->ppos == NULL);
        uint32_t x = series(db, "x");
        rows(db, x, T0, 5, 1000);
        CHECK(eng_flush(db) == 0);
        uint64_t pp;
        CHECK(ppos_of(db, db->parts[0]->name, &pp) == TSDB_ERR_INVALID);
        CHECK(ppos_set(db, "p1_1_1_0.ec", 5) == TSDB_ERR_INVALID);
        eng_close(db);
    }
    CHECK(ppos_size(d2) == -1);
    printf("  open: a torn tail is cut, a listed part without a record, no file and damage before a valid record "
           "fail the open, a single-server volume has no PPOS\n");
}

/* ------------------------------------------------------------ race (TSan) */

static atomic_bool race_stop;
static void *race_flush(void *arg)
{
    eng *db = arg;
    uint32_t a = 0;
    eng_series(db, "ra", TSDB_FLOAT32, &a);
    for (int i = 0; i < 60; i++) {
        int64_t ts[8];
        float v[8];
        for (int j = 0; j < 8; j++)
            ts[j] = T0 + (int64_t)(i % 5) * DAY + i * 100 + j, v[j] = (float)j;
        if (eng_insert_series(db, TSDB_FLOAT32, a, ts, v, 8) != 0 || eng_flush(db) != 0)
            failures++;
    }
    atomic_store(&race_stop, true);
    return NULL;
}

static void *race_merge(void *arg)
{
    eng *db = arg;
    while (!atomic_load(&race_stop))
        if (eng_optimize(db) != 0)
            failures++;
    return NULL;
}

static void case_race(void)
{
    char dir[PATH_MAX];
    fresh(dir, "race");
    eng *db;
    CHECK(open_db(dir, &db) == 0);
    if (!db)
        return;
    atomic_store(&race_stop, false);
    pthread_t f, m;
    pthread_create(&f, NULL, race_flush, db);
    pthread_create(&m, NULL, race_merge, db);
    for (int i = 0; !atomic_load(&race_stop); i++) { /* a swap's ppos_set (r8h's ecapply_swap_pre), from a test caller */
        char nm[64];
        snprintf(nm, sizeof nm, "p%d_%d_%d_0.ec", 4 * (19675 + i % 5), i, i);
        CHECK(ppos_set(db, nm, (uint64_t)i + 1) == 0);
        uint64_t got = 0;
        CHECK(ppos_of(db, nm, &got) == 0 && got == (uint64_t)i + 1);
    }
    pthread_join(f, NULL);
    pthread_join(m, NULL);
    CHECK(eng_close(db) == 0);
    CHECK(open_db(dir, &db) == 0);
    if (db) {
        CHECK(ppos_test_records(db) == db->nparts);
        eng_close(db);
    }
    printf("  race: flushes, merges and ppos_set at once; the open then compacts\n");
}

/* ------------------------------------------------------------ power loss */

/* a history of every path that names parts: rows, flushes, a large insert, a delete, merges */
static void child_history(const char *dir)
{
    eng *db;
    if (open_db(dir, &db) != 0)
        _exit(3);
    uint32_t a = series(db, "a"), b = series(db, "b");
    for (int i = 0; i < 4; i++) {
        rows(db, a, T0 + (int64_t)(i % 3) * DAY + i * 10000, 5, 1000);
        rows(db, b, T0 + (int64_t)i * DAY + 5000, 5, 1000);
        if (eng_flush(db) != 0)
            _exit(4);
        rows(db, b, T0 + (int64_t)i * DAY + 50000, 100, 10); /* a large insert */
        if (eng_delete_range(db, a, T0 + i * 10000, T0 + i * 10000 + 2000) != 0)
            _exit(4);
        if (i % 2 && eng_optimize(db) != 0)
            _exit(4);
    }
    eng_close(db);
    _exit(0);
}

static void child_open(const char *dir)
{
    eng *db;
    if (open_db(dir, &db) != 0)
        _exit(3);
    eng_close(db);
    _exit(0);
}

/* the open that follows a power loss must succeed: no named part without its record */
static bool opens(const char *dir, int k, const char *what)
{
    eng *db;
    int rc = open_db(dir, &db);
    if (rc != 0) {
        printf("  POWER: after a power loss at step %d of %s the open fails (%d)\n", k, what, rc);
        failures++;
        return false;
    }
    eng_close(db);
    return true;
}

static void sweep(const char *what, const char *kind, void (*prep)(const char *dir), int max, int need)
{
    char dir[PATH_MAX], ack[PATH_MAX + 16];
    snprintf(ack, sizeof ack, "%s/pl.ack", S);
    int crashes = 0, finished = 0;
    for (int k = 0; k < max && !finished; k++) {
        fresh(dir, "pl");
        if (prep)
            prep(dir);
        int code = run_child(kind, dir, ack, true, k, k + 1);
        CHECK(code == 99 || code == 0);
        if (code != 99 && code != 0)
            break;
        crashes += code == 99;
        finished = code == 0;
        if (!opens(dir, k, what))
            break;
    }
    CHECK(finished && crashes >= need);
    printf("  power: lost at each of %d steps of %s: every named part keeps its record\n", crashes, what);
}

static void prep_rows(const char *dir)
{
    char ack[PATH_MAX + 16];
    snprintf(ack, sizeof ack, "%s/prep.ack", S);
    if (run_child("rows", dir, ack, false, -1, 0) != 0)
        failures++;
}

static void prep_dead(const char *dir)
{
    eng *db;
    if (open_db(dir, &db) != 0) {
        failures++;
        return;
    }
    uint32_t a = series(db, "a");
    for (int i = 0; i < 6; i++) {
        rows(db, a, T0 + i * 10000, 5, 1000);
        eng_flush(db);
    }
    eng_optimize(db); /* six records of seven dead */
    rows(db, a, T0 + DAY, 5, 1000);
    eng_close(db);
}

static void case_power(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  power: skipped (no %s)\n", SHIM);
        return;
    }
    sweep("rows, flushes, large inserts, deletes and merges", "history", NULL, 3000, 30);
    sweep("an open that replays the WAL", "open", prep_rows, 600, 5);
    sweep("an open that compacts PPOS", "open", prep_dead, 600, 5);
}

/* ------------------------------------------------------------ cost of the notes */

#include <math.h>
#include <time.h>

/* rows/s of small inserts (1000 rows, through the WAL and the memtable) into a volume; days: the rows' days cycle
 * over this many (1: the last-day cache always hits; 30: it misses 29 times a batch) */
static double bench_one(bool ec, int days, size_t total, size_t batch)
{
    char dir[PATH_MAX];
    if (ec) {
        fresh(dir, "bench_ec");
    } else {
        snprintf(dir, sizeof dir, "%s/bench_plain", S);
        sh("rm -rf '%s' && mkdir -p '%s'", dir, dir);
    }
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.wal_sync_ms = 100;
    o.stats = -1;
    eng *db = NULL;
    meta_cluster_open_allow(true);
    int rc = eng_open(dir, &o, &db);
    meta_cluster_open_allow(false);
    if (rc != 0)
        return 0;
    if ((db->ppos != NULL) != ec)
        failures++;
    uint32_t ids[16];
    for (int i = 0; i < 16; i++) {
        char nm[16];
        snprintf(nm, sizeof nm, "s%d", i);
        ids[i] = series(db, nm);
    }
    static int64_t ts[100000];
    static float v[100000];
    struct timespec a, b;
    clock_gettime(CLOCK_MONOTONIC, &a);
    for (size_t k = 0, n = 0; n < total; k++, n += batch) {
        for (size_t j = 0; j < batch; j++) { /* in time order over `days` days: leaves the cached day days - 1 times */
            ts[j] = T0 + (int64_t)(j * (size_t)days / batch) * DAY + (int64_t)(k / 16) * (int64_t)batch + (int64_t)j;
            v[j] = (float)j;
        }
        if (eng_insert_series(db, TSDB_FLOAT32, ids[k % 16], ts, v, batch) != 0) {
            failures++;
            break;
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &b);
    eng_close(db);
    sh("rm -rf '%s'", dir);
    return (double)total / ((double)(b.tv_sec - a.tv_sec) + (double)(b.tv_nsec - a.tv_nsec) / 1e9);
}

static void bench(int reps)
{
    const size_t total = 4000000;
    bench_one(false, 1, total, 1000); /* a warm-up, not counted */
    /* 1000-row inserts through the memtable over 1 and 30 days (the notes), then 100,000-row inserts straight to parts
     * with fsync on (each part's record appended and fdatasynced before its name) */
    for (int mode = 0; mode < 3; mode++) {
        int days = mode == 1 ? 30 : 1;
        size_t batch = mode == 2 ? 100000 : 1000, tot = mode == 2 ? 2000000 : total;
        double r[2][32] = {{0}};
        for (int i = 0; i < reps && i < 32; i++) /* interleaved */
            for (int ec = 0; ec < 2; ec++)
                r[ec][i] = bench_one(ec, days, tot, batch);
        for (int ec = 0; ec < 2; ec++) {
            double m = 0, sd = 0;
            for (int i = 0; i < reps; i++)
                m += r[ec][i] / reps;
            for (int i = 0; i < reps; i++)
                sd += (r[ec][i] - m) * (r[ec][i] - m) / (reps > 1 ? reps - 1 : 1);
            printf("  bench: %s, %zu-row inserts, rows in %d day(s): %.2f M rows/s (sd %.2f, %d runs)\n",
                   ec ? "ec cluster volume (PPOS on)" : "plain volume (PPOS off)", batch, days, m / 1e6, sqrt(sd) / 1e6,
                   reps);
        }
    }
}

static int mutant_of(const char *m)
{
    return strcmp(m, "pposafter") == 0     ? PPOS_MUT_AFTER
           : strcmp(m, "pposnosync") == 0  ? PPOS_MUT_NOSYNC
           : strcmp(m, "pposflushhi") == 0 ? PPOS_MUT_FLUSHHI
           : strcmp(m, "pposreplay") == 0  ? PPOS_MUT_REPLAY
           : strcmp(m, "pposunknown0") == 0  ? PPOS_MUT_UNKNOWN0
           : strcmp(m, "pposfallback0") == 0 ? PPOS_MUT_FALLBACK0
           : strcmp(m, "pposnofail") == 0    ? PPOS_MUT_NOFAIL
           : strcmp(m, "pposnobadgood") == 0 ? PPOS_MUT_NOBADGOOD
           : strcmp(m, "pposflags") == 0     ? PPOS_MUT_FLAGS
                                           : -1;
}

int main(int argc, char **argv)
{
    if (argc >= 5 && strcmp(argv[1], "--child") == 0) {
        if (strcmp(argv[2], "rows") == 0)
            child_rows(argv[3], argv[4], false);
        else if (strcmp(argv[2], "flushdie") == 0)
            child_rows(argv[3], argv[4], true);
        else if (strcmp(argv[2], "history") == 0)
            child_history(argv[3]);
        else if (strcmp(argv[2], "open") == 0)
            child_open(argv[3]);
        _exit(2);
    }
#ifndef TSDB_EC
    printf("test_ppos: skipped (built without TSDB_EC)\n");
    return 0;
#endif
    const char *scratch = "/tmp", *mutant = NULL;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mutant") == 0 && i + 1 < argc)
            mutant = argv[++i];
        else
            scratch = argv[i];
    }
    if (mutant) {
        int m = mutant_of(mutant);
        if (m < 0) {
            fprintf(stderr, "unknown mutant %s\n", mutant);
            return 2;
        }
        ppos_mutant = m;
        char e[8];
        snprintf(e, sizeof e, "%d", m);
        setenv("TSDB_PPOS_MUTANT", e, 1); /* the children run the same bug */
    }
    char tmpl[sizeof S];
    snprintf(tmpl, sizeof tmpl, "%s/test-ppos.XXXXXX", scratch);
    if (!mkdtemp(tmpl)) {
        perror("mkdtemp");
        return 1;
    }
    snprintf(S, sizeof S, "%s", tmpl);
    if (!realpath(argv[0], SELF))
        snprintf(SELF, sizeof SELF, "%s", argv[0]);
    char *slash = strrchr(SELF, '/');
    snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", slash ? (int)(slash - SELF) : 1, slash ? SELF : ".");
    printf("test_ppos%s%s\n", mutant ? " --mutant " : "", mutant ? mutant : "");
    if (getenv("TEST_PPOS_BENCH")) {
        bench(atoi(getenv("TEST_PPOS_BENCH")) > 0 ? atoi(getenv("TEST_PPOS_BENCH")) : 5);
        sh("rm -rf '%s'", S);
        return failures ? 1 : 0;
    }
    if (ppos_mutant == PPOS_MUT_NOSYNC) { /* only a power loss can see it */
        case_power();
    } else {
        case_days();
        case_classes();
        case_writes();
        case_fallback();
        case_failstop();
        case_badgood();
        case_replay();
        case_kill();
        case_open();
        case_race();
        if (!getenv("TEST_PPOS_NO_POWER"))
            case_power();
    }
    if (failures == 0)
        sh("rm -rf '%s'", S);
    printf(failures ? "test_ppos: %d FAILED\n" : "test_ppos: ok\n", failures);
    return failures ? 1 : 0;
}
