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
static char S[PATH_MAX], SELF[PATH_MAX], SHIM[PATH_MAX + 32];

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

static uint64_t fno_of(eng *db)
{
    pthread_mutex_lock(&db->buf_lock);
    uint64_t v = db->fno_next;
    pthread_mutex_unlock(&db->buf_lock);
    return v;
}

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

static int unlink_in(const char *dir, const char *name)
{
    char p[PATH_MAX + 64];
    snprintf(p, sizeof p, "%s/%s", dir, name);
    return unlink(p);
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

/* The same with the feed on: the killed writer leaves wal_1 and the feed's files. */
static void crash_writer_feed(const char *dir, size_t nrows, int markers)
{
    pid_t p = fork();
    if (p == 0) {
        eng *db;
        uint32_t s;
        if (open_eng(dir, &db) != 0 || eng_series(db, "s", TSDB_FLOAT32, &s) != 0 || eng_feed_enable(db, NULL) != 0)
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
    *pct = __atomic_load_n(&free_pct_now, __ATOMIC_RELAXED); /* the cleaner thread reads it */
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
    __atomic_store_n(&free_pct_now, 5, __ATOMIC_RELAXED);
    feed_cleaner_wake(db);
    wait_segs(dir, 1, 6000);
    CHECK(count_prefix(dir, "fwal_") == 1 && exists(dir, "fwal_0000000000000006.log"));
    __atomic_store_n(&free_pct_now, 100, __ATOMIC_RELAXED);
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


/* ---------------------------------------------------- volumes, numbers, FEEDSLOTS halves, power loss */

static int open_tsdb(const char *dir, tsdb **t)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    return tsdb_open(dir, &o, t);
}

static bool vol_on(const char *dir)
{
    return exists(dir, "FEED") && exists(dir, "FEEDSLOTS") && meta_has_feed(dir);
}

static bool vol_clean(const char *dir)
{
    return !exists(dir, "FEED") && !exists(dir, "FEEDSLOTS") && !meta_has_feed(dir) && count_prefix(dir, "fwal_") == 0;
}

/* The root decides: a volume follows it at the open, whatever its own files say (design-cdc.md 2.13). */
static void test_two_volumes(void)
{
    char root[PATH_MAX + 32], v1[PATH_MAX + 32], v2[PATH_MAX + 32];
    fresh("two", root, sizeof root);
    snprintf(v1, sizeof v1, "%s/vol1", S);
    snprintf(v2, sizeof v2, "%s/vol2", S);
    sh("rm -rf '%s' '%s'; mkdir -p '%s' '%s'", v1, v2, v1, v2);
    tsdb *t;
    uint32_t idx;
    CHECK(open_tsdb(root, &t) == 0 && tsdb_close(t) == 0);
    CHECK(tsdb_volume_add_dir(root, v1, &idx) == 0 && idx == 1);
    CHECK(open_tsdb(root, &t) == 0);
    tsdb_sid a, b;
    CHECK(tsdb_series_create(t, "a", TSDB_FLOAT32, NULL, &a) == 0 && tsdb_series_create(t, "b", TSDB_FLOAT32, v1, &b) == 0);
    CHECK(tsdb_feed_enable(t, NULL) == 0 && tsdb_feed_enable(t, NULL) == 0); /* idempotent */
    tsdb_feed_info i0, i1;
    CHECK(tsdb_feed_info_get(t, 0, &i0) == 0 && tsdb_feed_info_get(t, 1, &i1) == 0 && i0.on && i1.on &&
          i0.feed_id != i1.feed_id && i0.feed_id && i1.feed_id);
    CHECK(vol_on(root) && vol_on(v1));
    /* a volume added beside the open writer, with the feed on, starts with a feed of its own, before its VOLUMES line */
    CHECK(tsdb_volume_add_dir(root, v2, &idx) == 0 && idx == 2);
    tsdb_sid c;
    CHECK(tsdb_series_create(t, "c", TSDB_FLOAT32, v2, &c) == 0);
    tsdb_feed_info i2;
    CHECK(vol_on(v2) && tsdb_feed_info_get(t, 2, &i2) == 0 && i2.on && i2.feed_id != i0.feed_id);
    CHECK(tsdb_close(t) == 0);

    /* the crash states of a disable, root first: the root lost FEED, the volumes still have everything. The open
     * finishes the disable in every volume (the root's FEED is the commit point of the whole database) */
    CHECK(unlink_in(root, "FEED") == 0);
    CHECK(open_tsdb(root, &t) == 0);
    CHECK(tsdb_feed_info_get(t, 0, &i0) == 0 && !i0.on && tsdb_feed_info_get(t, 1, &i1) == 0 && !i1.on &&
          tsdb_feed_info_get(t, 2, &i2) == 0 && !i2.on);
    CHECK(tsdb_close(t) == 0);
    CHECK(vol_clean(root) && vol_clean(v1) && vol_clean(v2));

    /* the crash states of an enable, root first: the root has FEED, a volume has nothing (or only the word): the
     * open enables the volume, with a new feed_id of its own, before its WAL is replayed */
    CHECK(open_tsdb(root, &t) == 0 && tsdb_feed_enable(t, NULL) == 0 && tsdb_close(t) == 0);
    for (int k = 0; k < 3; k++) {
        feed_dir d = {open(v1, O_RDONLY | O_DIRECTORY), NULL, true};
        CHECK(d.dirfd >= 0 && feed_dir_disable(&d) == 0);
        if (k == 1) { /* only the word left: a disable that died before the last step ... or an enable that began */
            feed_cfg cfg;
            feed_cfg_defaults(&cfg, NULL);
            CHECK(feed_dir_enable(&d, &cfg, 0) == 0 && unlinkat(d.dirfd, "FEED", 0) == 0);
        }
        close(d.dirfd);
        CHECK(!exists(v1, "FEED") || k == 2);
        CHECK(open_tsdb(root, &t) == 0);
        tsdb_feed_info r, v;
        CHECK(tsdb_feed_info_get(t, 0, &r) == 0 && tsdb_feed_info_get(t, 1, &v) == 0 && r.on && v.on &&
              r.feed_id != v.feed_id);
        CHECK(tsdb_close(t) == 0);
        CHECK(vol_on(root) && vol_on(v1));
    }
    /* a volume that is on while the root is off is turned off */
    CHECK(open_tsdb(root, &t) == 0 && tsdb_feed_disable(t) == 0 && tsdb_close(t) == 0);
    CHECK(vol_clean(root) && vol_clean(v1));
    {
        feed_dir d = {open(v1, O_RDONLY | O_DIRECTORY), NULL, true};
        feed_cfg cfg;
        feed_cfg_defaults(&cfg, NULL);
        CHECK(feed_dir_enable(&d, &cfg, 0) == 0);
        close(d.dirfd);
    }
    CHECK(vol_on(v1) && open_tsdb(root, &t) == 0);
    CHECK(tsdb_feed_info_get(t, 1, &i1) == 0 && !i1.on);
    CHECK(tsdb_close(t) == 0 && vol_clean(v1));
    printf("  two volumes: enable and disable go root first; crash states are finished to the root's state at open; "
           "a volume added with the feed on gets one\n");
}

/* feed new-id: a new feed_id first, then the numbers 2^40 higher; a torn FNO at the open does the same */
static void test_new_id(void)
{
    char dir[PATH_MAX + 32], p[PATH_MAX + 64];
    fresh("newid", dir, sizeof dir);
    eng *db;
    uint32_t s;
    CHECK(open_eng(dir, &db) == 0 && eng_series(db, "s", TSDB_FLOAT32, &s) == 0 && eng_feed_enable(db, NULL) == 0);
    rows(db, s, T0, 20);
    feed_cfg c0, c1;
    CHECK(feed_cfg_read(db->dirfd, &c0) == 0);
    uint64_t n0 = fno_of(db);
    CHECK(eng_feed_new_id(db) == 0 && feed_cfg_read(db->dirfd, &c1) == 0 && c1.feed_id != c0.feed_id);
    CHECK(fno_of(db) >= n0 + FNO_BELT);
    rows(db, s, T0 + 100, 5);
    CHECK(eng_close(db) == 0);
    CHECK(open_eng(dir, &db) == 0 && fno_of(db) >= n0 + FNO_BELT); /* durable */
    CHECK(feed_cfg_read(db->dirfd, &c0) == 0 && c0.feed_id == c1.feed_id);
    CHECK(eng_close(db) == 0);
    /* a torn FNO slot at the open: the numbers skip ahead and the feed_id is new (consumers resync) */
    uint64_t before;
    CHECK(open_eng(dir, &db) == 0);
    before = fno_of(db);
    CHECK(eng_close(db) == 0);
    snprintf(p, sizeof p, "%s/FNO", dir);
    uint8_t junk[4096];
    memset(junk, 0xa5, sizeof junk);
    int fd = open(p, O_WRONLY);
    CHECK(fd >= 0 && pwrite(fd, junk, sizeof junk, 4096) == (ssize_t)sizeof junk);
    close(fd);
    fprintf(stderr, "  (a torn FNO slot is logged next)\n");
    CHECK(open_eng(dir, &db) == 0);
    CHECK(feed_cfg_read(db->dirfd, &c0) == 0 && c0.feed_id != c1.feed_id && fno_of(db) >= before);
    uint64_t torn_next = fno_of(db);
    CHECK(eng_close(db) == 0);
    /* with the feed off a torn slot changes no feed_id: there is none */
    printf("  new-id: a new feed_id, numbers 2^40 higher, durable; a torn FNO slot at the open renews the feed_id "
           "(next %" PRIu64 ")\n", torn_next);
}

/* FEEDSLOTS: the half with the higher gen wins, a torn half is ignored, both torn fails the open */
static void test_slot_halves(void)
{
    char dir[PATH_MAX + 32], p[PATH_MAX + 64];
    fresh("halves", dir, sizeof dir);
    eng *db;
    uint32_t s;
    CHECK(open_eng(dir, &db) == 0 && eng_series(db, "s", TSDB_FLOAT32, &s) == 0 && eng_feed_enable(db, NULL) == 0);
    CHECK(fslots_set_floor(db, 10, 0xaa) == 0 && fslots_set_floor(db, 20, 0xbb) == 0);
    CHECK(eng_close(db) == 0);
    fslots fs;
    snprintf(p, sizeof p, "%s/FEEDSLOTS", dir);
    struct stat st;
    CHECK(stat(p, &st) == 0 && st.st_size == 8192);
    uint8_t img[8192], junk[4096];
    memset(junk, 0xa5, sizeof junk);
    int fd = open(p, O_RDWR);
    CHECK(fd >= 0 && pread(fd, img, sizeof img, 0) == (ssize_t)sizeof img);
    int dfd = open(dir, O_RDONLY | O_DIRECTORY);
    CHECK(fslots_read(dfd, &fs) == 0 && fs.floor_event == 20 && fs.gen == 3);
    int newer = -1;
    for (int h = 0; h < 2; h++) { /* tear the half that holds the newest: the other one (gen 2, floor 10) is the anchor */
        CHECK(pwrite(fd, junk, sizeof junk, h * 4096) == (ssize_t)sizeof junk);
        CHECK(fslots_read(dfd, &fs) == 0);
        if (fs.floor_event == 10)
            newer = h;
        CHECK(pwrite(fd, img + h * 4096, 4096, h * 4096) == 4096); /* put it back */
    }
    CHECK(newer >= 0);
    CHECK(pwrite(fd, junk, sizeof junk, newer * 4096) == (ssize_t)sizeof junk);
    CHECK(open_eng(dir, &db) == 0 && db->feed_floor_event == 10); /* the torn newest is ignored, never fatal */
    CHECK(fslots_set_floor(db, 30, 1) == 0 && eng_close(db) == 0); /* the next write goes to the scratch half */
    CHECK(fslots_read(dfd, &fs) == 0 && fs.floor_event == 30 && fs.gen == 3);
    CHECK(pwrite(fd, junk, sizeof junk, 0) == (ssize_t)sizeof junk && pwrite(fd, junk, sizeof junk, 4096) == (ssize_t)sizeof junk);
    fprintf(stderr, "  (a refusal is logged next: both halves of FEEDSLOTS are torn)\n");
    CHECK(open_eng(dir, &db) == TSDB_ERR_CORRUPT);
    close(fd);
    close(dfd);
    printf("  FEEDSLOTS: the higher gen wins, a torn half is ignored, both torn fails the open\n");
}

/* Power loss while an open replays a segment that was never synced (wal_sync_ms > 0, or a kill -9): the replay
 * truncates, fdatasyncs, then renames it to fwal_. Were the sync missing, the rename could be durable while the data is
 * not, and a "sealed" fwal_ would end in a torn record. The child writes unsynced rows, drops its engine without a
 * close and opens the directory again, all under the crashfs shim; power is lost at the k-th tracked step. */
static void child_replay(const char *dir)
{
    /* the segment a killed writer left, as a process under the shim sees it: written, never synced (a kill -9 between
     * the write and the fdatasync, or the page cache of the crash before); its name is durable */
    char p[PATH_MAX + 64];
    snprintf(p, sizeof p, "%s/wal_0000000000000001.log", dir);
    size_t len = 0;
    char *img = slurp(dir, "wal_0000000000000001.log", &len);
    if (!img || len == 0 || unlink(p) != 0)
        _exit(2);
    int fd = open(p, O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd < 0 || write(fd, img, len) != (ssize_t)len)
        _exit(3);
    close(fd);
    int dfd = open(dir, O_RDONLY | O_DIRECTORY);
    if (dfd < 0 || fsync(dfd) != 0)
        _exit(4);
    close(dfd);
    eng *b;
    if (open_eng(dir, &b) != 0)
        _exit(5);
    _exit(failures ? 6 : 0);
}

static int run_child(const char *dir, int k, int seed)
{
    pid_t pid = fork();
    if (pid == 0) {
        char w[16];
        snprintf(w, sizeof w, "%d", k);
        char asan[512];
        snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0:detect_leaks=0",
                 getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
        setenv("ASAN_OPTIONS", asan, 1);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", dir, 1);
        setenv("CRASHFS_PROB", "1", 1);
        setenv("CRASHFS_WARMUP", w, 1);
        setenv("CRASHFS_TEAR", "1", 1);
        char sd[16];
        snprintf(sd, sizeof sd, "%d", seed);
        setenv("CRASHFS_SEED", sd, 1);
        int null = open("/dev/null", O_WRONLY);
        if (null >= 0)
            dup2(null, 1), dup2(null, 2);
        execl(SELF, "test_feedstore", "--child", "replay", dir, (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

/* After a recovery with the feed on: the offline check F2's reader relies on. fwal_ seqs above the floor contiguous, every
 * fwal_ valid to its last byte, numbers only increase across segments, the floor below the oldest kept number. */
static void check_feed_dir(const char *dir, const char *what, int k, uint64_t whole_last)
{
    DIR *d = opendir(dir);
    struct dirent *e;
    uint64_t seqs[256];
    int n = 0;
    while (d && (e = readdir(d)))
        if (fwal_file_name(e->d_name) && n < 256)
            seqs[n++] = strtoull(e->d_name + 5, NULL, 16);
    if (d)
        closedir(d);
    for (int i = 0; i < n; i++)
        for (int j = i + 1; j < n; j++)
            if (seqs[j] < seqs[i]) {
                uint64_t t = seqs[i];
                seqs[i] = seqs[j];
                seqs[j] = t;
            }
    uint64_t prev_last = 0;
    for (int i = 0; i < n; i++) {
        char name[64];
        uint64_t f, l;
        fwal_name(name, sizeof name, seqs[i]);
        if (i && seqs[i] != seqs[i - 1] + 1) {
            fprintf(stderr, "  %s, step %d: fwal_ seqs not contiguous at %" PRIu64 "\n", what, k, seqs[i]);
            failures++;
        }
        int recs = seg_walk(dir, name, seqs[i], &f, &l);
        if (recs < 0) {
            fprintf(stderr, "  %s, step %d: %s is torn or damaged\n", what, k, name);
            failures++;
            continue;
        }
        if (whole_last && seqs[i] == 1 && (recs <= 0 || l != whole_last)) {
            /* the replay accepted every record of it: a rename that outlived its data is a loss */
            fprintf(stderr, "  %s, step %d: %s ends at %" PRIu64 ", not at %" PRIu64 " (renamed before its data was "
                            "synced)\n", what, k, name, l, whole_last);
            failures++;
        }
        if (recs && f <= prev_last) {
            fprintf(stderr, "  %s, step %d: %s starts at %" PRIu64 ", not above %" PRIu64 "\n", what, k, name, f, prev_last);
            failures++;
        }
        if (recs)
            prev_last = l;
    }
}

/* The offline check jepsen runs (tests/jepsen/feedcheck.c) must see a lost segment, a torn retained segment, numbers that
 * go back and a floor above the oldest kept number: a check that passes everything proves nothing. */
int feedcheck_dir(const char *dir, bool crashed, char *why, size_t cap);

static void test_feedcheck(void)
{
    char dir[PATH_MAX + 32], bad[PATH_MAX + 32], why[512];
    fresh("feedcheck", dir, sizeof dir);
    eng *db;
    uint32_t s;
    CHECK(open_eng(dir, &db) == 0 && eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    feed_opts fo = {.retention_s = 100000};
    CHECK(eng_feed_enable(db, &fo) == 0);
    for (int i = 0; i < 4; i++) {
        rows(db, s, T0 + i * 100000, 50);
        CHECK(eng_flush(db) == 0);
    }
    CHECK(eng_close(db) == 0);
    CHECK(count_prefix(dir, "fwal_") == 4);
    CHECK(feedcheck_dir(dir, false, why, sizeof why) == 0);
    CHECK(feedcheck_dir(dir, true, why, sizeof why) == 0);
    fresh("feedcheck-bad", bad, sizeof bad);
    sh("rm -rf '%s' && cp -a '%s' '%s' && rm '%s/fwal_0000000000000002.log'", bad, dir, bad, bad);
    CHECK(feedcheck_dir(bad, false, why, sizeof why) != 0 && strstr(why, "contiguous"));
    sh("rm -rf '%s' && cp -a '%s' '%s' && truncate -s -3 '%s/fwal_0000000000000003.log'", bad, dir, bad, bad);
    CHECK(feedcheck_dir(bad, false, why, sizeof why) != 0 && strstr(why, "torn"));
    sh("rm -rf '%s' && cp -a '%s' '%s' && cp '%s/fwal_0000000000000002.log' '%s/fwal_0000000000000003.log'", bad, dir,
       bad, bad, bad);
    CHECK(feedcheck_dir(bad, false, why, sizeof why) != 0); /* a segment bound to another seq is not whole either */
    /* a floor at or above the first number kept */
    sh("rm -rf '%s' && cp -a '%s' '%s'", bad, dir, bad);
    CHECK(open_eng(bad, &db) == 0 && fslots_set_floor(db, 60, 1) == 0 && eng_close(db) == 0);
    CHECK(feedcheck_dir(bad, false, why, sizeof why) != 0 && strstr(why, "floor"));
    /* the floor at the end of segment 1 with segment 1 still there: a cleaner that died before its unlinks; fine */
    uint64_t f1, l1;
    CHECK(seg_walk(dir, "fwal_0000000000000001.log", 1, &f1, &l1) > 0);
    sh("rm -rf '%s' && cp -a '%s' '%s'", bad, dir, bad);
    CHECK(open_eng(bad, &db) == 0 && fslots_set_floor(db, l1, 1) == 0 && eng_close(db) == 0);
    CHECK(feedcheck_dir(bad, false, why, sizeof why) == 0);
    /* a gap above the floor still counts */
    sh("rm '%s/fwal_0000000000000003.log'", bad);
    CHECK(feedcheck_dir(bad, false, why, sizeof why) != 0 && strstr(why, "contiguous"));
    /* a torn newest wal_ is what a crash leaves: fine before the open, never in a retained one */
    sh("rm -rf '%s' && cp -a '%s' '%s' && cp '%s/fwal_0000000000000004.log' '%s/wal_0000000000000005.log' && "
       "truncate -s -3 '%s/wal_0000000000000005.log'", bad, dir, bad, bad, bad, bad);
    CHECK(feedcheck_dir(bad, true, why, sizeof why) == 0);
    CHECK(feedcheck_dir(bad, false, why, sizeof why) == 0); /* not counted as a name when none is expected */
    printf("  feedcheck: sees a lost segment, a torn one, a damaged one and a floor too high\n");
}

/* `feed info` of a handle without a cleaner (read-only): the floor from FEEDSLOTS, the segments from the files. */
static void test_info(void)
{
    char dir[PATH_MAX + 32];
    fresh("info", dir, sizeof dir);
    eng *db;
    uint32_t s;
    CHECK(open_eng(dir, &db) == 0 && eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    feed_opts fo = {.retention_s = 100000};
    CHECK(eng_feed_enable(db, &fo) == 0);
    for (int i = 0; i < 3; i++) {
        rows(db, s, T0 + i * 100000, 20);
        CHECK(eng_flush(db) == 0);
    }
    CHECK(fslots_set_floor(db, 7, 1) == 0 && eng_close(db) == 0);
    tsdb *t;
    tsdb_options o;
    tsdb_options_init(&o);
    o.read_only = 1;
    o.create_volumes = 0;
    tsdb_feed_info fi;
    CHECK(tsdb_open(dir, &o, &t) == 0 && tsdb_feed_info_get(t, 0, &fi) == 0);
    CHECK(fi.on && fi.floor_event == 7 && fi.retained_segments == 3 && fi.retained_bytes > 0 && fi.retention_s == 100000);
    tsdb_close(t);
    printf("  info: a read-only handle reports the floor and the retained segments\n");
}

static void test_power_replay(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  power loss at the replay: skipped (no %s)\n", SHIM);
        return;
    }
    char dir[PATH_MAX + 32], src[PATH_MAX + 32];
    int crashes = 0, finished = 0;
    fresh("pl-replay-src", src, sizeof src);
    crash_writer_feed(src, 60, 2);
    CHECK(exists(src, "wal_0000000000000001.log") && exists(src, "FEED"));
    uint64_t f0, want = 0;
    CHECK(seg_walk(src, "wal_0000000000000001.log", 1, &f0, &want) > 0 && want >= 60);
    /* the tear of unsynced bytes is random: each step is lost with several seeds */
    for (int k = 0; k < 300 && !finished; k++) {
        for (int seed = 1; seed <= 12 && !finished; seed++) {
            fresh("pl-replay", dir, sizeof dir);
            sh("rm -rf '%s' && cp -a '%s' '%s'", dir, src, dir);
            int code = run_child(dir, k, seed);
            CHECK(code == 99 || code == 0);
            if (code != 99 && code != 0)
                return;
            crashes += code == 99;
            finished = code == 0;
            check_feed_dir(dir, "replay", k, want);
            if (seed == 1) { /* and the directory opens, with its feed in order */
                eng *db;
                int rc = open_eng(dir, &db);
                CHECK(rc == 0);
                if (rc == 0) {
                    check_feed_dir(dir, "after the open", k, 0);
                    CHECK(eng_close(db) == 0);
                }
            }
        }
    }
    CHECK(finished && crashes > 10);
    printf("  power lost at each of %d steps of a replay of an unsynced segment: every fwal_ is whole, contiguous, "
           "increasing\n", crashes);
}

int main(int argc, char **argv)
{
    if (argc == 4 && strcmp(argv[1], "--child") == 0 && strcmp(argv[2], "replay") == 0) {
        feed_test_free_hook(free_hook, NULL);
        if (getenv("FEEDSTORE_MUTANT"))
            eng_test_mutant = MUT_FEED_NOSEAL;
        child_replay(argv[3]);
        return 6;
    }
    bool mutant = argc > 1 && strcmp(argv[1], "--mutant") == 0; /* a planted bug that the power-loss sweep must see */
    const char *self = argv[0];
    if (mutant) {
        argv++, argc--;
        setenv("FEEDSTORE_MUTANT", "noseal", 1);
    }
    snprintf(S, sizeof S, "%s", argc > 1 ? argv[1] : "/tmp");
    if (!realpath(self, SELF))
        snprintf(SELF, sizeof SELF, "%s", self);
    char *slash = strrchr(SELF, '/');
    snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", slash ? (int)(slash - SELF) : 1, slash ? SELF : ".");
    feed_test_free_hook(free_hook, NULL); /* the machine's own free space must not decide (the rule is tested alone) */
    if (mutant) { /* the replayed segment renamed without its fdatasync: only a power loss sees it */
        test_power_replay();
        printf(failures ? "FAILED: %d\n" : "all ok\n", failures);
        return failures != 0;
    }
    test_off();
    test_retained();
    test_replay();
    test_marker_close();
    test_open_stuck();
    test_collision();
    test_refusals();
    test_enable_disable();
    test_cleaner();
    test_two_volumes();
    test_new_id();
    test_slot_halves();
    test_feedcheck();
    test_info();
    test_power_replay();
    printf(failures ? "FAILED: %d\n" : "all ok\n", failures);
    return failures != 0;
}
