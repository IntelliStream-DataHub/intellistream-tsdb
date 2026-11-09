/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The numbers of the log (`fno`, docs/design-cdc.md 2.1) never go back across opens, and a marker is never rows
 * (format 9). The bound is FNO, not MANIFEST (src/fno.c). Tests:
 *  - numbers are exact across clean reopens, rows and markers alike (the mutation "numbers restart from the parts"
 *    must fail this);
 *  - the retention history: rows flushed, their segment gone, their part dropped, MANIFEST torn: the next number
 *    still comes from FNO (the mutation "the bound is MANIFEST's" must fail this);
 *  - a replay skips markers and counts their numbers; a follower does too; damage in a marker before valid records
 *    fails the open and in the last record is a torn tail (the mutation "a marker is replayed as rows" must fail);
 *  - FNO's slots: with one bad slot (a torn store, or damage to the newest slot, which cannot be told apart) the other
 *    is used and the numbers skip ahead by FNO_BELT; both bad or no file fails the open; damage to the newest slot
 *    of a history whose segments and parts are gone never hands out a number twice;
 *  - limits: a bound at the limit of the numbers (a slot, a segment's record) fails the open, never a torn tail;
 *    a record whose number goes back, or a segment that starts below the end of the one before it, is damage;
 *  - META requires= refuses the open of a root and of a volume, and a volume is not added to such a root;
 *  - a volume that never held a record and has no FNO (an add by 0.9.0) gets one at its first open;
 *  - power lost at every step (crashfs, tearing unsynced data): after rows, a flush, a drop and markers, and during
 *    the open that replays a segment of markers only, every number a caller was told is below the next one.
 *
 *   ./build/test_fno [scratch dir]                run from the source directory is not needed
 *   ./build/test_fno --mutant reuse|manifest|markers|nosync [scratch dir]    must fail (nosync: in the power-loss sweeps)
 *   ./build/test_fno --child KIND DIR ACKFILE     (internal: a process the shim kills)
 */
#define _GNU_SOURCE
#include "internal.h"

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

static int open_eng(const char *dir, bool ro, bool follow, eng **db)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.read_only = ro;
    o.follow = follow;
    o.refresh_ms = 20;
    return eng_open(dir, &o, db);
}

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

/* a DROP marker (one number) or a COMMIT with `events` numbers; gives the first number */
static uint64_t marker(eng *db, int events)
{
    mk m;
    memset(&m, 0, sizeof m);
    uint64_t fno = 0;
    if (events <= 1) {
        m.type = MK_DROP;
        m.u.drop = (mk_drop){.wm_gen = 1, .dropped_before = 5, .block = 9, .partition = MK_NO_PARTITION};
    } else {
        m.type = MK_COMMIT;
        m.u.commit = (mk_commit){.op = MK_OP_INSERT, .keys_form = MK_KEYS_NONE, .events = (uint64_t)events};
    }
    int rc = wal_marker_mk(db, &m, &fno);
    if (rc != 0) {
        fprintf(stderr, "marker failed: %d\n", rc);
        failures++;
    }
    return fno;
}

static uint64_t rows_of(eng *db)
{
    tsdb_stats st;
    eng_get_stats(db, &st);
    return st.rows + st.memtable_rows;
}

static void fresh(const char *name, char *dir, size_t cap)
{
    snprintf(dir, cap, "%s/%s", S, name);
    sh("rm -rf '%s'; mkdir -p '%s'", dir, dir); /* crashfs wants its root to exist when it starts */
}

/* ------------------------------------------------------------ in-process tests */

static void test_reopen(void)
{
    char dir[PATH_MAX + 32];
    fresh("reopen", dir, sizeof dir);
    eng *db;
    uint32_t s;
    CHECK(open_eng(dir, false, false, &db) == 0 && fno_of(db) == 1);
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    rows(db, s, T0, 100);
    CHECK(fno_of(db) == 101);
    CHECK(eng_close(db) == 0);
    CHECK(open_eng(dir, false, false, &db) == 0 && fno_of(db) == 101); /* exact: no gap, no reuse */
    CHECK(marker(db, 1) == 101 && marker(db, 3) == 102 && fno_of(db) == 105);
    rows(db, s, T0 + 1000, 5);
    CHECK(fno_of(db) == 110);
    CHECK(eng_close(db) == 0);
    CHECK(open_eng(dir, false, false, &db) == 0 && fno_of(db) == 110);
    CHECK(rows_of(db) == 105);
    CHECK(eng_close(db) == 0);
    /* only markers, then rows: the markers leave no part, the numbers still hold */
    fresh("reopen2", dir, sizeof dir);
    CHECK(open_eng(dir, false, false, &db) == 0);
    CHECK(marker(db, 1) == 1 && marker(db, 1) == 2);
    CHECK(eng_close(db) == 0);
    CHECK(open_eng(dir, false, false, &db) == 0 && fno_of(db) == 3);
    CHECK(eng_close(db) == 0);
    printf("  numbers are exact across clean reopens: rows, markers, markers only\n");
}

/* The retention history: rows take 1..1000, their flush syncs FNO and unlinks the segment, retention drops the
 * partition, nothing else is written, and the MANIFEST left behind is torn. Only FNO still has the bound. */
static void test_history(void)
{
    char dir[PATH_MAX + 32];
    fresh("history", dir, sizeof dir);
    eng *db;
    uint32_t s;
    CHECK(open_eng(dir, false, false, &db) == 0);
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    rows(db, s, T0, 1000);
    CHECK(eng_flush(db) == 0);
    CHECK(eng_drop_before(db, T0 + 10 * 86400000LL) == 0); /* the partition goes: no part, no row left */
    CHECK(eng_close(db) == 0);
    CHECK(sh("ls '%s'/wal_* 2>/dev/null | grep -q .", dir) != 0); /* no segment either */
    CHECK(sh("ls '%s'/p*.part 2>/dev/null | grep -q .", dir) != 0);
    char man[PATH_MAX + 64];
    snprintf(man, sizeof man, "%s/MANIFEST", dir);
    CHECK(truncate(man, 9) == 0); /* torn */
    CHECK(open_eng(dir, false, false, &db) == 0);
    uint64_t next = fno_of(db);
    CHECK(next == 1001);
    CHECK(marker(db, 1) == 1001);
    CHECK(eng_close(db) == 0);
    printf("  retention history: no segment, no part, MANIFEST torn: the next number is 1001, from FNO\n");
}

static void test_slots(void)
{
    char dir[PATH_MAX + 32], fno[PATH_MAX + 64];
    fresh("slots", dir, sizeof dir);
    eng *db;
    CHECK(open_eng(dir, false, false, &db) == 0 && eng_close(db) == 0);
    snprintf(fno, sizeof fno, "%s/FNO", dir);
    static const struct {
        int a_ok, b_ok;
        uint64_t an, ag, bn, bg;
        int rc;
        uint64_t want;
        const char *what;
    } cases[] = {
        {1, 0, 1000, 5, 0, 0, 0, 1000 + FNO_BELT, "slot 1 bad: slot 0 is used, the numbers skip ahead"},
        {0, 1, 0, 0, 2000, 3, 0, 2000 + FNO_BELT, "slot 0 bad: slot 1 is used, the numbers skip ahead"},
        {1, 1, 3000, 7, 3500, 8, 0, 3500, "both valid: the newer generation"},
        {1, 1, 4500, 9, 4000, 8, 0, 4500, "both valid: the newer generation, slot 0"},
        {0, 0, 0, 0, 0, 0, TSDB_ERR_CORRUPT, 0, "both torn: the open fails"},
    };
    for (size_t i = 0; i < sizeof cases / sizeof *cases; i++) {
        uint8_t pages[8192];
        memset(pages, 0xa5, sizeof pages); /* a torn slot: bytes that do not check out */
        if (cases[i].a_ok)
            fno_slot_page(pages, cases[i].an, cases[i].ag);
        if (cases[i].b_ok)
            fno_slot_page(pages + 4096, cases[i].bn, cases[i].bg);
        int fd = open(fno, O_WRONLY);
        CHECK(fd >= 0 && pwrite(fd, pages, sizeof pages, 0) == (ssize_t)sizeof pages);
        close(fd);
        if (cases[i].rc)
            fprintf(stderr, "  (a refusal is logged next: %s)\n", cases[i].what);
        int rc = open_eng(dir, false, false, &db);
        CHECK(rc == cases[i].rc);
        if (rc == 0) {
            CHECK(fno_of(db) == cases[i].want);
            CHECK(eng_close(db) == 0); /* writes the exact number back */
        }
    }
    /* a flipped bit in a valid slot is a bad slot, not a number */
    uint8_t pages[8192];
    memset(pages, 0, sizeof pages);
    fno_slot_page(pages, 777, 3);
    fno_slot_page(pages + 4096, 888, 4);
    pages[4096 + 8] ^= 1; /* the number in slot 1 */
    int fd = open(fno, O_WRONLY);
    CHECK(fd >= 0 && pwrite(fd, pages, sizeof pages, 0) == (ssize_t)sizeof pages);
    close(fd);
    CHECK(open_eng(dir, false, false, &db) == 0 && fno_of(db) == 777 + FNO_BELT);
    CHECK(eng_close(db) == 0);
    /* no file at all in a format-9 volume */
    CHECK(unlink(fno) == 0);
    fprintf(stderr, "  (a refusal is logged next: FNO missing)\n");
    CHECK(open_eng(dir, false, false, &db) == TSDB_ERR_CORRUPT);
    printf("  FNO: a bad slot is skipped and the numbers skip ahead, both bad or no file fails the open, the newer "
           "generation wins\n");
}

static int run_child(const char *kind, const char *dir, const char *ackfile, bool under_shim, int warmup, int seed);

/* Damage to the newest slot, found after its segments and parts are gone: the history of test_history, the newest
 * slot of FNO flipped in one bit. The number it held (1001) cannot be told from a tear, so the next one must be
 * above it all the same: a number handed out twice would let a feed consumer take new rows for old ones. */
static void test_damaged_newest(void)
{
    char dir[PATH_MAX + 32], fno[PATH_MAX + 64];
    fresh("newest", dir, sizeof dir);
    eng *db;
    uint32_t s;
    CHECK(open_eng(dir, false, false, &db) == 0);
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    rows(db, s, T0, 500);
    CHECK(eng_flush(db) == 0); /* FNO holds 501 */
    rows(db, s, T0 + 1000, 500);
    CHECK(eng_flush(db) == 0); /* and now 1001, in the other slot */
    CHECK(eng_drop_before(db, T0 + 10 * 86400000LL) == 0);
    CHECK(eng_close(db) == 0);
    CHECK(sh("ls '%s'/wal_* 2>/dev/null | grep -q .", dir) != 0);
    snprintf(fno, sizeof fno, "%s/FNO", dir);
    uint8_t pages[8192];
    int fd = open(fno, O_RDWR);
    CHECK(fd >= 0 && pread(fd, pages, sizeof pages, 0) == (ssize_t)sizeof pages);
    uint64_t g0, g1;
    memcpy(&g0, pages + 16, 8);
    memcpy(&g1, pages + 4096 + 16, 8);
    off_t newest = g1 > g0 ? 4096 : 0;
    uint8_t b = pages[newest + 8] ^ 1; /* one bit of the number in the newest slot */
    CHECK(pwrite(fd, &b, 1, newest + 8) == 1);
    close(fd);
    CHECK(open_eng(dir, false, false, &db) == 0);
    CHECK(fno_of(db) > 1001); /* never 501: that would hand out 501..1000 a second time */
    CHECK(marker(db, 1) > 1001);
    CHECK(eng_close(db) == 0);
    printf("  damage to the newest FNO slot, segments and parts gone: numbers skip ahead, none is handed out twice\n");
}

/* A WAL segment as the "mixed" child leaves it (records: 10 rows at 0, a marker at 232, ...). */
static void segment_of_mixed(const char *name, char *dir, size_t cap)
{
    char ack[PATH_MAX + 32];
    fresh(name, dir, cap);
    snprintf(ack, sizeof ack, "%s/%s.ack", S, name);
    unlink(ack);
    CHECK(run_child("mixed", dir, ack, false, -1, 0) == 0);
}

static void patch_file(const char *path, off_t at, const void *p, size_t n)
{
    int fd = open(path, O_RDWR);
    CHECK(fd >= 0 && pwrite(fd, p, n, at) == (ssize_t)n);
    if (fd >= 0)
        close(fd);
}

/* Records that pass every check of a record and still cannot come from a writer: the open fails and names the
 * damage (it is neither replayed nor cut as a torn tail). */
static void test_number_guards(void)
{
    char dir[PATH_MAX + 32], seg[PATH_MAX + 64], seg2[PATH_MAX + 64];
    eng *db;
    /* 1. inside a segment: the marker after the 10 rows (numbers 1..10) says it is number 5 */
    segment_of_mixed("guard1", dir, sizeof dir);
    snprintf(seg, sizeof seg, "%s/wal_0000000000000001.log", dir);
    struct stat st;
    int fd = open(seg, O_RDWR);
    CHECK(fd >= 0 && fstat(fd, &st) == 0);
    size_t whole = (size_t)st.st_size;
    uint8_t *rec = malloc(whole);
    CHECK(rec && pread(fd, rec, whole, 0) == (ssize_t)whole);
    close(fd);
    uint64_t five = 5;
    memcpy(rec + 232 + 16, &five, 8);
    wal_reseal(rec + 232, 1, 232);
    patch_file(seg, 0, rec, whole);
    free(rec);
    fprintf(stderr, "  (a refusal is logged next: a number that goes back inside a segment)\n");
    CHECK(open_eng(dir, false, false, &db) == TSDB_ERR_CORRUPT);
    /* 2. across segments: the second segment is a copy of the first, sealed for its own number, so it starts at
       number 1 where the first ends at 27 */
    segment_of_mixed("guard2", dir, sizeof dir);
    snprintf(seg, sizeof seg, "%s/wal_0000000000000001.log", dir);
    snprintf(seg2, sizeof seg2, "%s/wal_0000000000000002.log", dir);
    uint8_t *buf = NULL;
    size_t len = 0;
    fd = open(seg, O_RDONLY);
    CHECK(fd >= 0 && fstat(fd, &st) == 0);
    len = (size_t)st.st_size;
    buf = malloc(len);
    CHECK(buf && pread(fd, buf, len, 0) == (ssize_t)len);
    close(fd);
    for (size_t pos = 0; pos < len;) {
        wal_rec wr;
        size_t rl = wal_record(buf + pos, len - pos, 1, pos, &wr);
        CHECK(rl != 0);
        if (rl == 0)
            break;
        wal_reseal(buf + pos, 2, pos);
        pos += rl;
    }
    fd = open(seg2, O_WRONLY | O_CREAT | O_EXCL, 0644);
    CHECK(fd >= 0 && write(fd, buf, len) == (ssize_t)len);
    close(fd);
    free(buf);
    fprintf(stderr, "  (a refusal is logged next: a segment that starts below the end of the one before it)\n");
    CHECK(open_eng(dir, false, false, &db) == TSDB_ERR_CORRUPT);
    printf("  a record whose number goes back, or a segment below the one before it, fails the open\n");
}

/* The limit of the numbers: a bound at it is damage or a bug, and the writer must never number a record past what
 * its own replay accepts (it would cut its acknowledged rows as a torn tail at the next open). */
static void test_limits(void)
{
    char dir[PATH_MAX + 32], fno[PATH_MAX + 64], seg[PATH_MAX + 64];
    eng *db;
    uint32_t s;
    fresh("limits", dir, sizeof dir);
    CHECK(open_eng(dir, false, false, &db) == 0 && eng_close(db) == 0);
    snprintf(fno, sizeof fno, "%s/FNO", dir);
    uint8_t pages[8192];
    /* a slot at the limit fails the open, a slot below it (with room for what a run numbers) works */
    memset(pages, 0xa5, sizeof pages);
    fno_slot_page(pages, FNO_LIMIT - 2, 5);
    fno_slot_page(pages + 4096, FNO_LIMIT - 3, 4);
    patch_file(fno, 0, pages, sizeof pages);
    fprintf(stderr, "  (a refusal is logged next: FNO beyond the limit of the numbers)\n");
    CHECK(open_eng(dir, false, false, &db) == TSDB_ERR_CORRUPT);
    fno_slot_page(pages, FNO_BOUND_MAX - 100, 5);
    fno_slot_page(pages + 4096, FNO_BOUND_MAX - 101, 4);
    patch_file(fno, 0, pages, sizeof pages);
    CHECK(open_eng(dir, false, false, &db) == 0 && eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    rows(db, s, T0, 5);
    CHECK(fno_of(db) == FNO_BOUND_MAX - 95);
    CHECK(eng_close(db) == 0);
    CHECK(open_eng(dir, false, false, &db) == 0 && rows_of(db) == 5); /* all five rows came back */
    /* the numbers run out: the next write would end at the bound, so the engine fails (fail-stop) rather than number
       a record the next open refuses; what was acknowledged is all there at that open */
    CHECK(fno_of(db) == FNO_BOUND_MAX - 95);
    {
        mk m;
        memset(&m, 0, sizeof m);
        m.type = MK_COMMIT;
        m.u.commit = (mk_commit){.op = MK_OP_INSERT, .keys_form = MK_KEYS_NONE, .events = 95};
        CHECK(wal_marker_mk(db, &m, NULL) == TSDB_ERR_IO); /* 95 numbers would end at the bound */
        CHECK(eng_failed_msg(db, NULL, 0) != 0);
    }
    eng_close(db);
    fprintf(stderr, "  (a refusal is logged next: none; the reopen below must work)\n");
    CHECK(open_eng(dir, false, false, &db) == 0 && rows_of(db) == 5 && fno_of(db) == FNO_BOUND_MAX - 95);
    CHECK(eng_close(db) == 0);
    /* a record at the limit in a segment: the checks of the record pass, so it is damage, not a torn tail, and the
       segment stays */
    segment_of_mixed("limits2", dir, sizeof dir);
    snprintf(seg, sizeof seg, "%s/wal_0000000000000001.log", dir);
    struct stat st;
    int fd = open(seg, O_RDWR);
    CHECK(fd >= 0 && fstat(fd, &st) == 0);
    size_t whole = (size_t)st.st_size, last = 0;
    uint8_t *rec = malloc(whole);
    CHECK(rec && pread(fd, rec, whole, 0) == (ssize_t)whole);
    close(fd);
    for (size_t pos = 0, rl; pos < whole; pos += rl) { /* the last record: with valid records after it the replay's
                                                         own checks would fail the open, and not show the guard */
        wal_rec wr;
        rl = wal_record(rec + pos, whole - pos, 1, pos, &wr);
        CHECK(rl != 0);
        if (rl == 0)
            break;
        last = pos;
    }
    uint64_t far = FNO_LIMIT + 7;
    memcpy(rec + last + 16, &far, 8);
    wal_reseal(rec + last, 1, last);
    patch_file(seg, 0, rec, whole);
    free(rec);
    fprintf(stderr, "  (a refusal is logged next: a record beyond the limit of the numbers)\n");
    CHECK(open_eng(dir, false, false, &db) == TSDB_ERR_CORRUPT);
    CHECK(access(seg, F_OK) == 0);
    printf("  a bound at the limit of the numbers fails the open; numbers below it replay whole\n");
}

/* META requires=: a feature this binary does not know changes what the files mean. */
static void test_requires(void)
{
    char dir[PATH_MAX + 32], meta[PATH_MAX + 64], vol[PATH_MAX + 64], vmeta[PATH_MAX + 96];
    eng *db;
    tsdb *t;
    uint32_t idx;
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    fresh("requires", dir, sizeof dir);
    CHECK(tsdb_open(dir, &o, &t) == 0);
    tsdb_close(t);
    snprintf(meta, sizeof meta, "%s/META", dir);
    CHECK(sh("cp '%s' '%s.saved' && echo 'requires=feed-v2' >> '%s'", meta, meta, meta) == 0);
    fprintf(stderr, "  (a refusal is logged next: requires=feed-v2 on a root)\n");
    CHECK(open_eng(dir, false, false, &db) == TSDB_ERR_INVALID);
    CHECK(open_eng(dir, true, false, &db) == TSDB_ERR_INVALID);
    CHECK(open_eng(dir, true, true, &db) == TSDB_ERR_INVALID);
    /* a root that requires something, or is of another format, is given no volume: it would get one made by these
       rules that the root's own release cannot use */
    snprintf(vol, sizeof vol, "%s/requires-vol2", S);
    CHECK(tsdb_volume_add_dir(dir, vol, &idx) == TSDB_ERR_INVALID);
    CHECK(sh("test ! -e '%s/META'", vol) == 0);
    CHECK(sh("cp '%s.saved' '%s' && sed -i 's/^version=.*/version=10/' '%s'", meta, meta, meta) == 0);
    CHECK(tsdb_volume_add_dir(dir, vol, &idx) == TSDB_ERR_INVALID);
    CHECK(sh("test ! -e '%s/META'", vol) == 0);
    /* a volume that requires something: the open of the database refuses */
    CHECK(sh("cp '%s.saved' '%s'", meta, meta) == 0);
    snprintf(vol, sizeof vol, "%s/requires-vol1", S);
    CHECK(tsdb_volume_add_dir(dir, vol, &idx) == 0 && idx == 1);
    snprintf(vmeta, sizeof vmeta, "%s/META", vol);
    CHECK(sh("grep -q '^volume=1' '%s'", vmeta) == 0);
    CHECK(sh("echo 'requires=feed-v2' >> '%s'", vmeta) == 0);
    fprintf(stderr, "  (a refusal is logged next: requires=feed-v2 on a volume)\n");
    CHECK(tsdb_open(dir, &o, &t) != 0);
    printf("  META requires= refuses the open of a root and of a volume; no volume is added to such a root\n");
}

/* A volume made by an add that left no FNO (0.9.0's, on an upgraded database): nothing there ever had a number, so
 * its first open makes FNO(1). */
static void test_empty_volume(void)
{
    char dir[PATH_MAX + 32], vol[PATH_MAX + 64], path[PATH_MAX + 96];
    tsdb *t;
    uint32_t idx;
    fresh("emptyvol", dir, sizeof dir);
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    CHECK(tsdb_open(dir, &o, &t) == 0);
    tsdb_close(t);
    snprintf(vol, sizeof vol, "%s/emptyvol-vol1", S);
    CHECK(tsdb_volume_add_dir(dir, vol, &idx) == 0 && idx == 1);
    snprintf(path, sizeof path, "%s/FNO", vol);
    CHECK(access(path, F_OK) == 0 && unlink(path) == 0); /* what 0.9.0's add leaves */
    CHECK(tsdb_open(dir, &o, &t) == 0);
    CHECK(access(path, F_OK) == 0);
    if (t)
        tsdb_close(t);
    printf("  a volume with no FNO that never held a record gets one at its first open\n");
}

/* Writes records into a directory the way a writer that died leaves them: a child that does not close. */
static void child_run(const char *kind, const char *dir, const char *ackfile);

static int run_child(const char *kind, const char *dir, const char *ackfile, bool under_shim, int warmup, int seed)
{
    pid_t pid = fork();
    if (pid == 0) {
        if (under_shim) {
            char asan[512];
            snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0",
                     getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
            setenv("ASAN_OPTIONS", asan, 1);
            char w[16], sd[16];
            snprintf(w, sizeof w, "%d", warmup);
            snprintf(sd, sizeof sd, "%d", seed);
            setenv("LD_PRELOAD", SHIM, 1);
            setenv("CRASHFS_ROOT", dir, 1);
            setenv("CRASHFS_PROB", warmup >= 0 ? "1" : "0", 1);
            setenv("CRASHFS_WARMUP", w, 1);
            setenv("CRASHFS_AT_EXIT", "1", 1);
            setenv("CRASHFS_TEAR", "1", 1);
            setenv("CRASHFS_SEED", sd, 1);
        }
        int null = open("/dev/null", O_WRONLY);
        if (null >= 0) {
            dup2(null, 1);
            dup2(null, 2);
        }
        execl(SELF, "test_fno", "--child", kind, dir, ackfile, (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

static uint64_t max_ack(const char *ackfile)
{
    FILE *f = fopen(ackfile, "r");
    uint64_t a, mx = 0;
    while (f && fscanf(f, "%" SCNu64, &a) == 1)
        mx = a > mx ? a : mx;
    if (f)
        fclose(f);
    return mx;
}

static void test_replay_markers(void)
{
    char dir[PATH_MAX + 32], ack[PATH_MAX + 32];
    fresh("markers", dir, sizeof dir);
    snprintf(ack, sizeof ack, "%s/markers.ack", S);
    unlink(ack);
    CHECK(run_child("mixed", dir, ack, false, -1, 0) == 0);
    /* rows 1..10, DROP (11), rows 12..21, COMMIT with 3 events (22..24), rows 25..26: 22 rows, next 27 */
    eng *db;
    int rc = open_eng(dir, false, false, &db);
    CHECK(rc == 0);
    if (rc == 0) {
        CHECK(rows_of(db) == 22); /* markers are not rows */
        CHECK(fno_of(db) == 27);
        CHECK(eng_close(db) == 0);
    }
    /* a follower: the rows on both sides of a marker, no marker as a row */
    fresh("markers-follow", dir, sizeof dir);
    CHECK(run_child("mixed", dir, ack, false, -1, 0) == 0);
    rc = open_eng(dir, true, true, &db);
    CHECK(rc == 0);
    if (rc == 0) {
        for (int i = 0; i < 100 && rows_of(db) != 22; i++) {
            usleep(20000);
            eng_refresh(db);
        }
        CHECK(rows_of(db) == 22);
        CHECK(eng_close(db) == 0);
    }
    /* damage in a marker with valid records after it fails the open and names the segment; in the last record it is
       a torn tail */
    fresh("markers-damage", dir, sizeof dir);
    CHECK(run_child("mixed", dir, ack, false, -1, 0) == 0);
    char seg[PATH_MAX + 64];
    snprintf(seg, sizeof seg, "%s/wal_0000000000000001.log", dir);
    int fd = open(seg, O_RDWR);
    CHECK(fd >= 0);
    off_t marker_at = 32 + 10 * 20, payload = marker_at + 32 + 8 + 4; /* the second row of the marker's body */
    uint8_t b;
    CHECK(fd >= 0 && pread(fd, &b, 1, payload) == 1);
    b ^= 0x40;
    CHECK(fd >= 0 && pwrite(fd, &b, 1, payload) == 1);
    close(fd);
    fprintf(stderr, "  (a refusal is logged next: a damaged marker before valid records)\n");
    CHECK(open_eng(dir, false, false, &db) == TSDB_ERR_CORRUPT);
    printf("  a replay and a follower skip markers and count their numbers; a damaged marker before valid records "
           "fails the open\n");
}

/* ------------------------------------------------------------ the children */

static void ack(const char *file, uint64_t n)
{
    int fd = open(file, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    char line[32];
    int len = snprintf(line, sizeof line, "%" PRIu64 "\n", n);
    if (fd >= 0) {
        if (write(fd, line, (size_t)len) != len)
            _exit(2);
        close(fd);
    }
}

static void child_run(const char *kind, const char *dir, const char *ackfile)
{
    eng *db;
    if (open_eng(dir, false, false, &db) != 0)
        _exit(3);
    uint32_t s = 0;
    if (strcmp(kind, "mixed") == 0) { /* rows, a marker, rows, a marker of 3, rows; no close */
        if (eng_series(db, "s", TSDB_FLOAT32, &s) != 0)
            _exit(4);
        rows(db, s, T0, 10);
        marker(db, 1);
        rows(db, s, T0 + 100, 10);
        marker(db, 3);
        rows(db, s, T0 + 200, 2);
        _exit(0);
    }
    if (strcmp(kind, "history") == 0) { /* rows, flush, drop, markers: every acknowledged number is written down */
        if (eng_series(db, "s", TSDB_FLOAT32, &s) != 0)
            _exit(4);
        for (int i = 0; i < 4; i++) {
            rows(db, s, T0 + i * 1000, 50);
            ack(ackfile, fno_of(db) - 1); /* the insert returned: its rows are numbered and synced */
        }
        if (eng_flush(db) != 0)
            _exit(5);
        if (eng_drop_before(db, T0 + 10 * 86400000LL) != 0)
            _exit(5);
        for (int i = 0; i < 3; i++) {
            uint64_t f = marker(db, i == 1 ? 3 : 1);
            ack(ackfile, f + (i == 1 ? 2 : 0));
        }
        _exit(0);
    }
    if (strcmp(kind, "markers") == 0) { /* only markers, acknowledged, and no close */
        for (int i = 0; i < 4; i++) {
            uint64_t f = marker(db, i == 2 ? 2 : 1);
            ack(ackfile, f + (i == 2 ? 1 : 0));
        }
        _exit(0);
    }
    if (strcmp(kind, "open") == 0) { /* nothing but the open (the replay of what is there) and a clean close */
        eng_close(db);
        _exit(0);
    }
    _exit(6);
}

/* After a power loss: the open works and the next number is above every number a caller was told. */
static bool bound_holds(const char *dir, const char *ackfile, int k, const char *what)
{
    eng *db;
    int rc = open_eng(dir, false, false, &db);
    if (rc != 0) {
        fprintf(stderr, "  %s, step %d: the open failed: %d\n", what, k, rc);
        failures++;
        return false;
    }
    uint64_t next = fno_of(db), mx = max_ack(ackfile);
    bool ok = next > mx;
    if (!ok) {
        fprintf(stderr, "  %s, step %d: the next number is %" PRIu64 ", not above the acknowledged %" PRIu64 "\n", what,
                k, next, mx);
        failures++;
    }
    /* and what it hands out now is above them as well */
    uint64_t f = marker(db, 1);
    CHECK(f > mx);
    CHECK(eng_close(db) == 0);
    return ok;
}

static void test_power_loss(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  power loss: skipped (no %s)\n", SHIM);
        return;
    }
    char dir[PATH_MAX + 32], ackf[PATH_MAX + 32];
    snprintf(ackf, sizeof ackf, "%s/pl.ack", S);
    int crashes = 0, finished = 0;
    /* rows, a flush, a drop, markers: power lost at each step, with unsynced data torn at random */
    for (int k = 0; k < 600 && !finished; k++) {
        fresh("pl-history", dir, sizeof dir);
        unlink(ackf);
        int code = run_child("history", dir, ackf, true, k, k + 1);
        CHECK(code == 99 || code == 0);
        if (code != 99 && code != 0)
            break;
        crashes += code == 99;
        finished = code == 0;
        bound_holds(dir, ackf, k, "history");
    }
    CHECK(finished && crashes > 30);
    int c1 = crashes;
    /* a segment of markers only, left by a process that did not close, replayed by an open that loses power at each
       of its steps (the replay unlinks the segment: FNO must hold its end first) */
    crashes = finished = 0;
    for (int k = 0; k < 400 && !finished; k++) {
        fresh("pl-open", dir, sizeof dir);
        unlink(ackf);
        CHECK(run_child("markers", dir, ackf, false, -1, 0) == 0);
        int code = run_child("open", dir, ackf, true, k, k + 7);
        CHECK(code == 99 || code == 0);
        if (code != 99 && code != 0)
            break;
        crashes += code == 99;
        finished = code == 0;
        bound_holds(dir, ackf, k, "open");
    }
    CHECK(finished && crashes > 5);
    printf("  power lost at each of %d steps of rows, a flush, a drop and markers, and at each of %d steps of the "
           "open that replays a segment of markers: the next number is above every acknowledged one\n", c1, crashes);
}

static void set_mutant(const char *mutant)
{
    eng_test_mutant = strcmp(mutant, "reuse") == 0      ? MUT_FNO_REUSE
                      : strcmp(mutant, "manifest") == 0 ? MUT_FNO_MANIFEST_ONLY
                      : strcmp(mutant, "markers") == 0  ? MUT_MARKER_ROWS
                      : strcmp(mutant, "nosync") == 0   ? MUT_FNO_NOSYNC
                                                        : MUT_NONE;
    if (eng_test_mutant == MUT_NONE) {
        fprintf(stderr, "unknown mutant %s\n", mutant);
        exit(2);
    }
    setenv("TSDB_FNO_MUTANT", mutant, 1); /* the processes the shim kills run the same bug */
}

int main(int argc, char **argv)
{
    if (argc >= 5 && strcmp(argv[1], "--child") == 0) {
        if (getenv("TSDB_FNO_MUTANT"))
            set_mutant(getenv("TSDB_FNO_MUTANT"));
        child_run(argv[2], argv[3], argv[4]);
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
    snprintf(tmpl, sizeof tmpl, "%s/test-fno.XXXXXX", scratch);
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
    if (eng_test_mutant == MUT_FNO_NOSYNC) { /* only the power-loss sweeps can see this one: a clean close is fine */
        test_power_loss();
    } else {
        test_reopen();
        test_history();
        test_replay_markers();
        if (!mutant) {
            test_slots();
            test_damaged_newest();
            test_number_guards();
            test_limits();
            test_requires();
            test_empty_volume();
            test_power_loss();
        }
    }
    sh("rm -rf '%s'", S);
    if (failures)
        fprintf(stderr, "%d fno checks FAILED\n", failures);
    else
        printf("fno tests passed\n");
    return failures ? 1 : 0;
}
