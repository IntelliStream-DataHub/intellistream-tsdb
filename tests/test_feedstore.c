/*
 * The change feed's disk side (step F1 of docs/design-cdc.md): the FEED and FEEDSLOTS files, requires=feed, enable
 * and disable, retained WAL segments (fwal_<seq>.log: renamed at flush, open and close instead of unlinked) and the
 * cleaner that removes old ones. Tests:
 *  - off: with the feed off nothing changes (no fwal_, no FEED, META without requires=);
 *  - retained: a flush renames the segment; a replayed segment is cut at its last valid record, then renamed; a
 *    segment with no record is unlinked; a marker-only active segment is renamed at close, not unlinked;
 *  - the regression of this step: an unlink (feed off) or rename (feed on) that fails at the open must hold
 *    wal_removed back (it used to be set to the active segment's number minus 1 whatever happened), and the segment
 *    is retried at the next flush; with the feed on the retained set stays contiguous;
 *  - a new segment never takes the number of a retained one, even when retention dropped every part;
 *  - enable and disable: idempotent, the word first and last, a stale fwal_ before an enable is removed, a crash
 *    in the middle (by the files each step leaves) is finished by the next open, two volumes follow the root;
 *    refused with wal_sync_ms = -1 and on read-only handles;
 *  - the cleaner: age, size, the free-space rule and the leftovers below the floor, never the newest segment; the
 *    floor is durable in FEEDSLOTS and never goes down; FEEDSLOTS halves are picked by gen and a torn one is not fatal;
 *  - a torn FNO at the open gives a new feed_id before the first FNO store; feed new-id raises the numbers.
 *
 *   ./build/test_feedstore [scratch dir]
 */
#define _GNU_SOURCE
#include "internal.h"

#include <dirent.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdarg.h>
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

#define T0 1700000000000LL
static char S[PATH_MAX];

static int sh(const char *fmt, ...)
{
    char cmd[4096];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(cmd, sizeof cmd, fmt, ap);
    va_end(ap);
    return system(cmd);
}

static void fresh(const char *name, char *dir, size_t cap)
{
    snprintf(dir, cap, "%s/%s", S, name);
    sh("rm -rf '%s'; mkdir -p '%s'", dir, dir);
}

static int open_opt(const char *dir, int wal_sync_ms, bool ro, eng **db)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.read_only = ro;
    o.wal_sync_ms = wal_sync_ms;
    return eng_open(dir, &o, db);
}
static int open_eng(const char *dir, eng **db) { return open_opt(dir, 0, false, db); }

static void rows(eng *db, uint32_t series, int64_t t0, size_t n)
{
    int64_t ts[64];
    float v[64];
    for (size_t i = 0; i < n; i += 64) {
        size_t k = n - i < 64 ? n - i : 64;
        for (size_t j = 0; j < k; j++)
            ts[j] = t0 + (int64_t)(i + j), v[j] = (float)(i + j);
        int rc = eng_insert_series(db, TSDB_FLOAT32, series, ts, v, k);
        if (rc != 0) {
            fprintf(stderr, "insert failed: %d\n", rc);
            failures++;
        }
    }
}

static uint64_t marker1(eng *db)
{
    mk m;
    memset(&m, 0, sizeof m);
    uint64_t fno = 0;
    m.type = MK_DROP;
    m.u.drop = (mk_drop){.wm_gen = 1, .dropped_before = 5, .block = 9, .partition = MK_NO_PARTITION};
    CHECK(wal_marker_mk(db, &m, &fno) == 0);
    return fno;
}

static bool exists(const char *dir, const char *name)
{
    char p[PATH_MAX + 64];
    snprintf(p, sizeof p, "%s/%s", dir, name);
    return access(p, F_OK) == 0;
}

static int count_prefix(const char *dir, const char *prefix)
{
    DIR *d = opendir(dir);
    int n = 0;
    struct dirent *e;
    while (d && (e = readdir(d)))
        if (strncmp(e->d_name, prefix, strlen(prefix)) == 0)
            n++;
    if (d)
        closedir(d);
    return n;
}

static char *slurp(const char *dir, const char *name, size_t *len)
{
    char p[PATH_MAX + 64];
    snprintf(p, sizeof p, "%s/%s", dir, name);
    FILE *f = fopen(p, "rb");
    if (!f)
        return NULL;
    size_t cap = 1 << 16, n = 0;
    char *b = malloc(cap + 1);
    size_t r;
    while ((r = fread(b + n, 1, cap - n, f)) > 0) {
        n += r;
        if (n == cap) {
            cap *= 2;
            b = realloc(b, cap + 1);
        }
    }
    fclose(f);
    b[n] = 0;
    if (len)
        *len = n;
    return b;
}

static bool meta_has_feed(const char *dir)
{
    char *t = slurp(dir, "META", NULL);
    bool f = false;
    if (t)
        meta_requires_parse(t, &f, NULL, 0);
    free(t);
    return f;
}

/* Walks a retained segment: every byte is a valid record (a sealed segment has no torn tail). *first and *last: the
 * numbers it covers (last = the last record's fno + span - 1). Returns the records, -1 when damaged. */
static int seg_walk(const char *dir, const char *name, uint64_t seq, uint64_t *first, uint64_t *last)
{
    size_t len;
    char *b = slurp(dir, name, &len);
    if (!b)
        return -1;
    size_t pos = 0;
    int n = 0;
    wal_rec r;
    size_t rec;
    *first = *last = 0;
    while (pos < len && (rec = wal_record((uint8_t *)b + pos, len - pos, seq, pos, &r)) != 0) {
        if (!n)
            *first = r.fno;
        *last = r.fno + r.span - 1;
        pos += rec;
        n++;
    }
    free(b);
    return pos == len ? n : -1;
}

static void first_seg(const char *dir, const char *prefix, char *name, size_t cap)
{
    DIR *d = opendir(dir);
    struct dirent *e;
    name[0] = 0;
    while (d && (e = readdir(d)))
        if (strncmp(e->d_name, prefix, strlen(prefix)) == 0 && (!name[0] || strcmp(e->d_name, name) < 0))
            snprintf(name, cap, "%.*s", (int)cap - 1, e->d_name);
    if (d)
        closedir(d);
}

/* A process that writes and dies without closing: what a kill -9 leaves (its WAL is synced per write). */
static void crash_writer(const char *dir, size_t nrows, int markers)
{
    pid_t p = fork();
    if (p == 0) {
        eng *db;
        uint32_t s;
        if (open_eng(dir, &db) != 0 || eng_series(db, "s", TSDB_FLOAT32, &s) != 0)
            _exit(2);
        rows(db, s, T0, nrows);
        for (int i = 0; i < markers; i++)
            marker1(db);
        _exit(0);
    }
    int st = 0;
    waitpid(p, &st, 0);
    CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 0);
}

/* ------------------------------------------------------------------ tests */

static void test_off(void)
{
    char dir[PATH_MAX + 32];
    fresh("off", dir, sizeof dir);
    eng *db;
    uint32_t s;
    CHECK(open_eng(dir, &db) == 0 && eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    rows(db, s, T0, 500);
    CHECK(eng_flush(db) == 0);
    CHECK(count_prefix(dir, "fwal_") == 0);
    CHECK(eng_close(db) == 0);
    CHECK(count_prefix(dir, "fwal_") == 0 && count_prefix(dir, "wal_") == 0);
    CHECK(!exists(dir, "FEED") && !exists(dir, "FEEDSLOTS") && !meta_has_feed(dir));
    CHECK(open_eng(dir, &db) == 0 && !atomic_load(&db->feed_on) && !db->has_cleaner);
    CHECK(eng_close(db) == 0);
    printf("  off: no fwal_, no FEED, no FEEDSLOTS, META without the word, no cleaner thread\n");
}

static void test_retained(void)
{
    char dir[PATH_MAX + 32], name[64];
    fresh("retained", dir, sizeof dir);
    eng *db;
    uint32_t s;
    CHECK(open_eng(dir, &db) == 0 && eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    feed_opts fo = {.retention_s = 100000};
    CHECK(eng_feed_enable(db, &fo) == 0 && atomic_load(&db->feed_on) && db->has_cleaner);
    CHECK(exists(dir, "FEED") && exists(dir, "FEEDSLOTS") && meta_has_feed(dir));
    CHECK(eng_feed_enable(db, &fo) == 0); /* idempotent */
    rows(db, s, T0, 300);
    CHECK(eng_flush(db) == 0);
    CHECK(count_prefix(dir, "fwal_") == 1 && !exists(dir, "wal_0000000000000001.log"));
    first_seg(dir, "fwal_", name, sizeof name);
    uint64_t seq = strtoull(name + 5, NULL, 16), f, l;
    CHECK(seq == 1 && seg_walk(dir, name, seq, &f, &l) > 0 && f == 1 && l == 300);
    /* a flush renamed it: the engine's own accounting is as if it were unlinked */
    CHECK(atomic_load(&db->wal_removed) >= 1);
    CHECK(eng_close(db) == 0);
    /* the close: the empty active segment (nothing ever written) is unlinked, not kept */
    CHECK(count_prefix(dir, "wal_") == 0 && count_prefix(dir, "fwal_") == 1);
    CHECK(open_eng(dir, &db) == 0 && atomic_load(&db->feed_on) && db->has_cleaner);
    CHECK(db->wal_seq == 2); /* after the retained segment, never on it */
    rows(db, s, T0 + 10000, 10);
    CHECK(eng_close(db) == 0); /* the final flush renames segment 2; the active one at close is empty */
    CHECK(count_prefix(dir, "fwal_") == 2 && count_prefix(dir, "wal_") == 0);
    printf("  retained: a flush renames the segment, a close unlinks only an empty one, numbers continue\n");
}

/* A replayed segment goes to fwal_ cut at its last valid record; one with no record is unlinked. */
static void test_replay(void)
{
    char dir[PATH_MAX + 32], name[64];
    fresh("replay", dir, sizeof dir);
    eng *db;
    CHECK(open_eng(dir, &db) == 0 && eng_feed_enable(db, NULL) == 0 && eng_close(db) == 0);
    crash_writer(dir, 200, 2);
    CHECK(exists(dir, "wal_0000000000000001.log"));
    char p[PATH_MAX + 64];
    snprintf(p, sizeof p, "%s/wal_0000000000000001.log", dir);
    struct stat sb;
    CHECK(stat(p, &sb) == 0);
    off_t good = sb.st_size;
    FILE *f = fopen(p, "ab");
    fwrite("\x07torn tail garbage", 1, 18, f); /* a write the process died in */
    fclose(f);
    CHECK(open_eng(dir, &db) == 0);
    CHECK(!exists(dir, "wal_0000000000000001.log") && exists(dir, "fwal_0000000000000001.log"));
    snprintf(p, sizeof p, "%s/fwal_0000000000000001.log", dir);
    CHECK(stat(p, &sb) == 0 && sb.st_size == good); /* the torn tail is cut before the rename */
    uint64_t first, last;
    CHECK(seg_walk(dir, "fwal_0000000000000001.log", 1, &first, &last) > 0 && first == 1 && last == 202);
    CHECK(db->wal_seq == 2);
    CHECK(eng_close(db) == 0);
    CHECK(count_prefix(dir, "wal_") == 0);
    /* a crash that left an empty segment only: nothing to retain */
    fresh("replay2", dir, sizeof dir);
    CHECK(open_eng(dir, &db) == 0 && eng_feed_enable(db, NULL) == 0 && eng_close(db) == 0);
    crash_writer(dir, 0, 0);
    first_seg(dir, "wal_", name, sizeof name);
    CHECK(name[0] != 0);
    CHECK(open_eng(dir, &db) == 0 && eng_close(db) == 0);
    CHECK(count_prefix(dir, "fwal_") == 0 && count_prefix(dir, "wal_") == 0);
    printf("  replay: a torn tail is cut before the rename; a segment with no record is unlinked\n");
}

static void test_marker_close(void)
{
    char dir[PATH_MAX + 32];
    fresh("marker", dir, sizeof dir);
    eng *db;
    CHECK(open_eng(dir, &db) == 0 && eng_feed_enable(db, NULL) == 0);
    marker1(db); /* a marker adds no rows: the active segment holds a record and has none */
    CHECK(eng_close(db) == 0);
    CHECK(exists(dir, "fwal_0000000000000001.log") && count_prefix(dir, "wal_") == 0);
    uint64_t f, l;
    CHECK(seg_walk(dir, "fwal_0000000000000001.log", 1, &f, &l) == 1 && f == 1 && l == 1);
    /* the same with the feed off: the segment is unlinked as before */
    fresh("marker_off", dir, sizeof dir);
    CHECK(open_eng(dir, &db) == 0);
    marker1(db);
    CHECK(eng_close(db) == 0);
    CHECK(count_prefix(dir, "fwal_") == 0 && count_prefix(dir, "wal_") == 0);
    printf("  close: a marker-only active segment is renamed (feed on), unlinked (feed off)\n");
}

static const char *fail_name;
static int fail_hook(const char *name) { return fail_name && strcmp(name, fail_name) == 0 ? EIO : 0; }

/* The bug of this step: a failed unlink at open did not hold wal_removed back (db.c set it to wal_seq - 1). */
static void test_open_stuck(void)
{
    char dir[PATH_MAX + 32];
    for (int feed = 0; feed < 2; feed++) {
        fresh("stuck", dir, sizeof dir);
        eng *db;
        if (feed)
            CHECK(open_eng(dir, &db) == 0 && eng_feed_enable(db, NULL) == 0 && eng_close(db) == 0);
        crash_writer(dir, 100, 0);
        CHECK(exists(dir, "wal_0000000000000001.log"));
        fail_name = "wal_0000000000000001.log";
        eng_test_retire_fail = fail_hook;
        CHECK(open_eng(dir, &db) == 0);
        eng_test_retire_fail = NULL;
        CHECK(exists(dir, "wal_0000000000000001.log")); /* it stayed */
        CHECK(db->wal_seq == 2);
        CHECK(atomic_load(&db->wal_removed) == 0); /* did not pass it (1 before the fix) */
        CHECK(atomic_load(&db->wal_removed_synced) == 0);
        CHECK(db->wal_unlink_next == 1);
        fail_name = NULL;
        uint32_t s;
        CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
        rows(db, s, T0 + 100000, 5);
        CHECK(eng_flush(db) == 0); /* the next flush retries it */
        CHECK(!exists(dir, "wal_0000000000000001.log"));
        CHECK(atomic_load(&db->wal_removed) >= 2 && db->wal_unlink_next == 0);
        if (feed)
            CHECK(exists(dir, "fwal_0000000000000001.log") && exists(dir, "fwal_0000000000000002.log"));
        CHECK(eng_close(db) == 0);
    }
    /* a rename that cannot happen (the name is taken) is a failed rename: logged, nothing after it is renamed */
    fresh("stuck2", dir, sizeof dir);
    eng *db;
    CHECK(open_eng(dir, &db) == 0 && eng_feed_enable(db, NULL) == 0 && eng_close(db) == 0);
    crash_writer(dir, 100, 0);
    {
        char p[PATH_MAX + 64];
        snprintf(p, sizeof p, "%s/fwal_0000000000000001.log", dir);
        FILE *f = fopen(p, "wb"); /* in the way */
        fputs("x", f);
        fclose(f);
    }
    CHECK(open_eng(dir, &db) == 0);
    CHECK(exists(dir, "wal_0000000000000001.log") && atomic_load(&db->wal_removed) == 0 && db->wal_unlink_next == 1);
    CHECK(eng_close(db) == 0);
    printf("  open: a failed unlink or rename holds wal_removed back and is retried at the next flush\n");
}

/* Retention dropped every part, the segments are gone from the engine's view: a new segment must not take the number
 * of a retained one (the rename would replace it). */
static void test_collision(void)
{
    char dir[PATH_MAX + 32];
    fresh("collide", dir, sizeof dir);
    eng *db;
    uint32_t s;
    CHECK(open_eng(dir, &db) == 0 && eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    feed_opts fo = {.retention_s = 100000};
    CHECK(eng_feed_enable(db, &fo) == 0);
    rows(db, s, T0, 100);
    CHECK(eng_flush(db) == 0);
    CHECK(eng_drop_before(db, T0 + 10 * 86400000LL) == 0); /* no part left */
    CHECK(eng_close(db) == 0);
    CHECK(count_prefix(dir, "fwal_") == 1);
    CHECK(open_eng(dir, &db) == 0);
    CHECK(db->wal_seq == 2); /* not 1: the retained fwal_1 holds that number (without the count it would be 1) */
    CHECK(eng_close(db) == 0);
    printf("  numbers of segments: a new segment never takes a retained one's, parts or not\n");
}

static void test_refusals(void)
{
    char dir[PATH_MAX + 32];
    fresh("refuse", dir, sizeof dir);
    eng *db;
    CHECK(open_opt(dir, -1, false, &db) == 0);
    CHECK(eng_feed_enable(db, NULL) == TSDB_ERR_INVALID && !exists(dir, "FEED") && !meta_has_feed(dir));
    CHECK(eng_close(db) == 0);
    CHECK(open_eng(dir, &db) == 0 && eng_feed_enable(db, NULL) == 0 && eng_close(db) == 0);
    CHECK(open_opt(dir, -1, false, &db) == TSDB_ERR_INVALID); /* a feed-on database never opens without the WAL */
    CHECK(open_opt(dir, 0, true, &db) == 0 && atomic_load(&db->feed_on));
    CHECK(eng_feed_enable(db, NULL) == TSDB_ERR_INVALID && eng_feed_disable(db) == TSDB_ERR_INVALID);
    CHECK(eng_close(db) == 0);
    printf("  refusals: wal_sync_ms = -1 (enable, and the open of a feed-on database), read-only handles\n");
}

static void test_enable_disable(void)
{
    char dir[PATH_MAX + 32], p[PATH_MAX + 64];
    fresh("endis", dir, sizeof dir);
    eng *db;
    uint32_t s;
    CHECK(open_eng(dir, &db) == 0 && eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    /* leftovers of an interrupted disable (no open finished it): removed before the new feed's files are made */
    snprintf(p, sizeof p, "%s/fwal_0000000000000007.log", dir);
    FILE *f = fopen(p, "wb");
    fputs("stale", f);
    fclose(f);
    CHECK(eng_feed_enable(db, NULL) == 0);
    CHECK(!exists(dir, "fwal_0000000000000007.log") && exists(dir, "FEED") && exists(dir, "FEEDSLOTS"));
    feed_cfg c1, c2;
    CHECK(feed_cfg_read(db->dirfd, &c1) == 0 && c1.retention_s == 86400 && c1.max_bytes == (64ull << 30) &&
          c1.min_free_pct == 10 && c1.feed_id != 0);
    fslots fs;
    CHECK(fslots_read(db->dirfd, &fs) == 0 && fs.gen == 1 && fs.floor_event == 0);
    rows(db, s, T0, 50);
    CHECK(eng_flush(db) == 0 && count_prefix(dir, "fwal_") == 1);
    CHECK(eng_feed_disable(db) == 0);
    CHECK(!atomic_load(&db->feed_on) && !db->has_cleaner);
    CHECK(!exists(dir, "FEED") && !exists(dir, "FEEDSLOTS") && count_prefix(dir, "fwal_") == 0 && !meta_has_feed(dir));
    rows(db, s, T0 + 1000, 50);
    CHECK(eng_flush(db) == 0 && count_prefix(dir, "fwal_") == 0 && count_prefix(dir, "wal_") <= 1); /* the active one */
    CHECK(eng_feed_disable(db) == 0); /* idempotent */
    /* a later enable makes a new feed_id and its floor is the open numbers' */
    CHECK(eng_feed_enable(db, NULL) == 0 && feed_cfg_read(db->dirfd, &c2) == 0 && c2.feed_id != c1.feed_id);
    CHECK(fslots_read(db->dirfd, &fs) == 0 && fs.floor_event == 100);
    CHECK(eng_close(db) == 0);
    /* crash states of an enable: the word without FEED (any step before FEED) is finished as OFF by an open that finds
     * the root without FEED, never as a half feed */
    const char *steps[] = {"word", "slots"};
    for (int k = 0; k < 2; k++) {
        fresh("endis2", dir, sizeof dir);
        CHECK(open_eng(dir, &db) == 0 && eng_close(db) == 0);
        feed_dir d = {open(dir, O_RDONLY | O_DIRECTORY), NULL, true};
        feed_cfg cfg;
        feed_cfg_defaults(&cfg, NULL);
        CHECK(feed_dir_enable(&d, &cfg, 0) == 0);
        CHECK(unlinkat(d.dirfd, "FEED", 0) == 0); /* FEED is the last step: a crash before it */
        if (k == 1)
            CHECK(unlinkat(d.dirfd, "FEEDSLOTS", 0) == 0);
        close(d.dirfd);
        CHECK(open_eng(dir, &db) == 0);
        CHECK(!atomic_load(&db->feed_on) && !meta_has_feed(dir) && !exists(dir, "FEEDSLOTS"));
        CHECK(eng_close(db) == 0);
        (void)steps;
    }
    /* a crash state of a disable: FEED gone, the word and FEEDSLOTS left (the root: finished as off) */
    fresh("endis3", dir, sizeof dir);
    CHECK(open_eng(dir, &db) == 0 && eng_feed_enable(db, NULL) == 0 && eng_close(db) == 0);
    snprintf(p, sizeof p, "%s/FEED", dir);
    CHECK(unlink(p) == 0);
    CHECK(open_eng(dir, &db) == 0 && !atomic_load(&db->feed_on) && !meta_has_feed(dir) && !exists(dir, "FEEDSLOTS"));
    CHECK(eng_close(db) == 0);
    /* FEED without the word cannot happen: the open fails and says so */
    fresh("endis4", dir, sizeof dir);
    CHECK(open_eng(dir, &db) == 0 && eng_feed_enable(db, NULL) == 0 && eng_close(db) == 0);
    {
        feed_dir d = {open(dir, O_RDONLY | O_DIRECTORY), NULL, true};
        CHECK(feed_dir_disable(&d) == 0);
        feed_cfg cfg;
        feed_cfg_defaults(&cfg, NULL);
        CHECK(feed_cfg_write(&d, &cfg) == 0);
        close(d.dirfd);
    }
    CHECK(open_eng(dir, &db) == TSDB_ERR_CORRUPT);
    printf("  enable and disable: idempotent, stale fwal_ removed first, half states finished at open, new feed_id\n");
}

/* The cleaner: age, size, the free-space rule, leftovers below the floor; never the newest segment; the floor is
 * durable before the unlink and never goes down. Segments are made with tiny buffers so every flush is one. */
static uint32_t free_pct_now = 100;
static int free_hook(void *ctx, const char *dir, uint32_t *pct)
{
    (void)ctx, (void)dir;
    *pct = free_pct_now;
    return 0;
}

static void wait_segs(const char *dir, int want, int ms)
{
    for (int i = 0; i < ms / 10 && count_prefix(dir, "fwal_") != want; i++)
        usleep(10000);
}

static void make_segs(eng *db, uint32_t s, int n, int64_t t0)
{
    for (int i = 0; i < n; i++) {
        rows(db, s, t0 + i * 1000, 10);
        CHECK(eng_flush(db) == 0);
    }
}

static void test_cleaner(void)
{
    char dir[PATH_MAX + 32];
    eng *db;
    uint32_t s;
    /* size: segments of 32 + 10 * 20 = 232 bytes (a record per insert call, rows join): the bound keeps the newest few */
    fresh("clean_size", dir, sizeof dir);
    CHECK(open_eng(dir, &db) == 0 && eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    feed_opts fo = {.retention_s = 100000, .max_bytes = 1000};
    CHECK(eng_feed_enable(db, &fo) == 0);
    make_segs(db, s, 8, T0);
    wait_segs(dir, 4, 5000);
    int left = count_prefix(dir, "fwal_");
    if (left < 3 || left > 5)
        sh("ls -l '%s'", dir);
    CHECK(left >= 3 && left <= 5); /* the bound holds about 1000 bytes: 4 segments of 232 */
    CHECK(exists(dir, "fwal_0000000000000008.log")); /* the newest is never removed */
    fslots fs;
    CHECK(fslots_read(db->dirfd, &fs) == 0 && fs.floor_event > 0 && fs.gen > 1);
    uint64_t floor1 = fs.floor_event;
    uint32_t chk = fs.floor_check;
    CHECK(chk != 0);
    /* the floor is exactly the last number of the newest removed segment, and the oldest kept segment starts after it */
    char name[64];
    first_seg(dir, "fwal_", name, sizeof name);
    uint64_t seq = strtoull(name + 5, NULL, 16), f, l;
    CHECK(seg_walk(dir, name, seq, &f, &l) > 0 && f == floor1 + 1);
    CHECK(eng_close(db) == 0);
    CHECK(open_eng(dir, &db) == 0);
    CHECK(fslots_read(db->dirfd, &fs) == 0 && fs.floor_event >= floor1); /* it survives the reopen, never lower */
    CHECK(db->feed_floor_event >= floor1);
    CHECK(eng_close(db) == 0);

    /* age: a segment whose last record is older than retention goes; with retention 1 s and a sleep, all but the newest */
    fresh("clean_age", dir, sizeof dir);
    CHECK(open_eng(dir, &db) == 0 && eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    fo = (feed_opts){.retention_s = 1};
    CHECK(eng_feed_enable(db, &fo) == 0);
    make_segs(db, s, 4, T0);
    CHECK(count_prefix(dir, "fwal_") >= 1);
    wait_segs(dir, 1, 6000);
    CHECK(count_prefix(dir, "fwal_") == 1 && exists(dir, "fwal_0000000000000004.log")); /* never the newest */
    CHECK(eng_close(db) == 0);

    /* free space: below the rule the oldest go whatever their age and size */
    fresh("clean_free", dir, sizeof dir);
    CHECK(open_eng(dir, &db) == 0 && eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    fo = (feed_opts){.retention_s = 100000, .min_free_pct = 10};
    CHECK(eng_feed_enable(db, &fo) == 0);
    make_segs(db, s, 6, T0);
    usleep(1500000);
    CHECK(count_prefix(dir, "fwal_") == 6); /* nothing yet: young and small */
    free_pct_now = 5;
    feed_cleaner_wake(db);
    wait_segs(dir, 1, 6000);
    CHECK(count_prefix(dir, "fwal_") == 1 && exists(dir, "fwal_0000000000000006.log"));
    free_pct_now = 100;
    CHECK(eng_close(db) == 0);

    /* a crash after the floor was made durable but before the unlinks: the segments below it go first, whatever
     * their age or size (they are leftovers: the floor already excludes them) */
    fresh("clean_left", dir, sizeof dir);
    CHECK(open_eng(dir, &db) == 0 && eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    fo = (feed_opts){.retention_s = 100000};
    CHECK(eng_feed_enable(db, &fo) == 0);
    make_segs(db, s, 5, T0);
    CHECK(eng_close(db) == 0);
    CHECK(count_prefix(dir, "fwal_") == 5);
    CHECK(open_eng(dir, &db) == 0);
    CHECK(count_prefix(dir, "fwal_") == 5);
    uint64_t last3;
    CHECK(seg_walk(dir, "fwal_0000000000000003.log", 3, &f, &last3) > 0);
    CHECK(fslots_set_floor(db, last3, 0x1234) == 0); /* what the cleaner does first: the floor, durable */
    feed_cleaner_wake(db);
    wait_segs(dir, 2, 5000);
    CHECK(count_prefix(dir, "fwal_") == 2 && exists(dir, "fwal_0000000000000004.log") &&
          exists(dir, "fwal_0000000000000005.log"));
    CHECK(fslots_set_floor(db, last3 - 5, 1) == 0 && db->feed_floor_event == last3 + 0); /* never down */
    CHECK(eng_close(db) == 0);
    printf("  cleaner: size, age, free space, leftovers below the floor; never the newest; the floor is durable\n");
}

int main(int argc, char **argv)
{
    snprintf(S, sizeof S, "%s", argc > 1 ? argv[1] : "/tmp");
    feed_test_free_hook(free_hook, NULL); /* the machine's own free space must not decide (the rule is tested alone) */
    test_off();
    test_retained();
    test_replay();
    test_marker_close();
    test_open_stuck();
    test_collision();
    test_refusals();
    test_enable_disable();
    test_cleaner();
    printf(failures ? "FAILED: %d\n" : "all ok\n", failures);
    return failures != 0;
}
