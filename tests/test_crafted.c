/*
 * Crafted files and extreme inputs: what someone with write access to the
 * data directory (or a shared NFS mount) can put there, and inputs at the
 * edges of the API. The checksums are no authentication (xxh64, unkeyed), so
 * every case here recomputes them where the file has one: the code must
 * refuse the content, not rely on the hash. Each case that crashed before its
 * fix runs in a child process, so a crash is a failure, not the end of the run.
 *
 *   ./build/test_crafted [scratch dir]
 */
#define _GNU_SOURCE
#include "internal.h"
#include "tsdb.h"
#include "tsdb_calendar.h"

#include <dirent.h>
#include <fcntl.h>
#include <ftw.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static int failures;

#define CHECK(cond)                                                                          \
    do {                                                                                     \
        if (!(cond)) {                                                                       \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond);         \
            failures++;                                                                      \
        }                                                                                    \
    } while (0)
#define OK(expr)                                                                             \
    do {                                                                                     \
        int rc_ = (expr);                                                                    \
        if (rc_ != 0) {                                                                      \
            fprintf(stderr, "%s:%d: %s returned %d\n", __FILE__, __LINE__, #expr, rc_);      \
            exit(1);                                                                         \
        }                                                                                    \
    } while (0)

static int rm_one(const char *p, const struct stat *s, int flag, struct FTW *f)
{
    (void)s, (void)flag, (void)f;
    return remove(p);
}
static void rm_rf(const char *dir) { nftw(dir, rm_one, 16, FTW_DEPTH | FTW_PHYS); }

static char *slurp(const char *path, size_t *len)
{
    FILE *f = fopen(path, "rb");
    if (!f)
        return NULL;
    fseek(f, 0, SEEK_END);
    long n = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *buf = malloc((size_t)n + 1);
    if (fread(buf, 1, (size_t)n, f) != (size_t)n) {
        free(buf);
        buf = NULL;
    }
    fclose(f);
    if (buf)
        buf[n] = '\0';
    *len = (size_t)n;
    return buf;
}

static void spit(const char *path, const void *buf, size_t len)
{
    FILE *f = fopen(path, "wb");
    if (!f || fwrite(buf, 1, len, f) != len) {
        fprintf(stderr, "cannot write %s\n", path);
        exit(1);
    }
    fclose(f);
}

static bool find_part(const char *dir, char *name, size_t len)
{
    DIR *d = opendir(dir);
    struct dirent *e;
    bool found = false;
    while (d && (e = readdir(d)))
        if (e->d_name[0] == 'p' && strstr(e->d_name, ".part") && !found) {
            snprintf(name, len, "%s", e->d_name);
            found = true;
        }
    if (d)
        closedir(d);
    return found;
}

static tsdb_options opts(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.granule_rows = 64;
    o.fsync = 0;
    o.merge_threads = 0;
    o.flush_interval_ms = 1000000;
    return o;
}

/* Runs fn in a child: true when it exited 0 (a crash or a failed CHECK there is false). */
static bool in_child(void (*fn)(const char *), const char *arg)
{
    fflush(stderr);
    pid_t pid = fork();
    if (pid == 0) {
        alarm(60); /* a loop that never ends fails too */
        failures = 0;
        fn(arg);
        _exit(failures ? 1 : 0);
    }
    int st;
    if (pid < 0 || waitpid(pid, &st, 0) != pid)
        return false;
    if (WIFSIGNALED(st))
        fprintf(stderr, "  child died of signal %d\n", WTERMSIG(st));
    return WIFEXITED(st) && WEXITSTATUS(st) == 0;
}

/* A database with one series of 1000 rows in one part (and the MANIFEST listing it). */
static void make_db(const char *dir)
{
    rm_rf(dir);
    tsdb_options o = opts();
    tsdb *db;
    uint32_t id;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series(db, "s1", &id));
    int64_t ts[1000];
    float v[1000];
    for (int i = 0; i < 1000; i++) {
        ts[i] = 1700000000000LL + i * 1000;
        v[i] = (float)i;
    }
    OK(tsdb_insert_series(db, id, ts, v, 1000));
    OK(tsdb_flush(db));
    OK(tsdb_close(db));
}

/* ------------------------------------------------------------ MANIFEST */

/* MANIFEST with its "parts" line replaced and `extra` part lines added, checksum recomputed. */
static void craft_manifest(const char *dir, const char *parts_line, int extra, bool twice)
{
    char path[512];
    snprintf(path, sizeof path, "%s/MANIFEST", dir);
    size_t len;
    char *text = slurp(path, &len);
    CHECK(text != NULL);
    if (!text)
        return;
    size_t cap = len + 64 + (size_t)extra * 32;
    char *out = malloc(cap);
    size_t n = 0;
    for (char *line = text, *next; line && *line; line = next) {
        next = strchr(line, '\n');
        if (next)
            *next++ = '\0';
        if (!strncmp(line, "checksum ", 9))
            break;
        if (twice && !strncmp(line, "wal_active ", 11))
            continue; /* one header line fewer: the second count is read as a header too, not as a part name */
        if (!strncmp(line, "parts ", 6)) {
            n += (size_t)snprintf(out + n, cap - n, "%s\n", parts_line);
            if (twice)
                n += (size_t)snprintf(out + n, cap - n, "%s\n", parts_line);
            continue;
        }
        n += (size_t)snprintf(out + n, cap - n, "%s\n", line);
    }
    for (int i = 0; i < extra; i++)
        n += (size_t)snprintf(out + n, cap - n, "p0_%d_%d_0.part\n", i + 100, i + 100);
    n += (size_t)snprintf(out + n, cap - n, "checksum %016llx\n", (unsigned long long)xxh64(out, n, 0));
    spit(path, out, n);
    free(out);
    free(text);
}

static void open_follower_refused(const char *dir)
{
    tsdb_options o = opts();
    o.read_only = 1;
    tsdb *db;
    int rc = tsdb_open(dir, &o, &db);
    CHECK(rc == TSDB_ERR_CORRUPT);
    if (rc == 0)
        tsdb_close(db);
}

static void test_manifest(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/crafted_manifest", base);
    /* SIZE_MAX parts: calloc(SIZE_MAX + 1) was calloc(0), then a pointer per line was stored past it */
    make_db(dir);
    craft_manifest(dir, "parts 18446744073709551615", 2000, false);
    CHECK(in_child(open_follower_refused, dir));
    make_db(dir);
    craft_manifest(dir, "parts -1", 2000, false); /* %zu takes a sign */
    CHECK(in_child(open_follower_refused, dir));
    /* a second count would allocate again over the first array */
    make_db(dir);
    craft_manifest(dir, "parts 1", 0, true);
    CHECK(in_child(open_follower_refused, dir));
    rm_rf(dir);
}

/* --------------------------------------------------------------- parts */

static char part_dir[512];

static int count_cb(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    (void)ts, (void)v;
    *(long *)ctx += (long)n;
    return 0;
}

static void query_rows(const char *dir, int *rc, long *rows)
{
    tsdb_options o = opts();
    tsdb *db;
    uint32_t id;
    *rc = tsdb_open(dir, &o, &db);
    *rows = -1;
    if (*rc)
        return;
    OK(tsdb_series_lookup(db, "s1", &id));
    long n = 0;
    *rc = tsdb_query_raw(db, id, INT64_MIN, INT64_MAX, count_cb, &n);
    if (*rc == 0)
        *rows = n;
    tsdb_close(db);
}

static void open_refused(const char *dir)
{
    int rc;
    long rows;
    query_rows(dir, &rc, &rows);
    CHECK(rc == TSDB_ERR_CORRUPT);
}

static void query_refused(const char *dir)
{
    int rc;
    long rows;
    query_rows(dir, &rc, &rows);
    CHECK(rc == TSDB_ERR_CORRUPT);
}

/* The footer and index of the part, read and written back with a recomputed hash. */
static void rehash_part(const char *path)
{
    size_t len;
    char *buf = slurp(path, &len);
    part_footer f;
    memcpy(&f, buf + len - sizeof f, sizeof f);
    uint64_t index_end = f.index_off + (uint64_t)f.ngranules * sizeof(granule);
    uint64_t x = xxh64(buf, sizeof(part_header), 0);
    x = xxh64(buf + f.index_off, (size_t)f.ngranules * sizeof(granule), x);
    size_t tlen = len - sizeof f - (size_t)index_end;
    if (tlen)
        x = xxh64(buf + index_end, tlen, x);
    f.index_hash = xxh64(&f, offsetof(part_footer, index_hash), x);
    memcpy(buf + len - sizeof f, &f, sizeof f);
    spit(path, buf, len);
    free(buf);
}

/* a second part in the crafted part's partition, after its rows, then a merge of the two */
static void merge_refused(const char *dir)
{
    tsdb_options o = opts();
    tsdb *db;
    uint32_t id;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series_lookup(db, "s1", &id));
    int64_t ts[100];
    float v[100];
    for (int i = 0; i < 100; i++) {
        ts[i] = 1700000000000LL + 2000000 + i * 1000;
        v[i] = (float)i;
    }
    OK(tsdb_insert_series(db, id, ts, v, 100));
    OK(tsdb_flush(db));
    CHECK(tsdb_optimize(db) == TSDB_ERR_CORRUPT); /* was TSDB_ERR_IO: write(2) from the unmapped address */
    tsdb_close(db);
}

/* One series of 40 rows in granules of 16, 16 and 8 rows (one part). */
static void make_db_3_granules(const char *dir)
{
    rm_rf(dir);
    tsdb_options o = opts();
    o.granule_rows = 16;
    o.wal_max_batch_rows = 4; /* straight to a part */
    tsdb *db;
    uint32_t id;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series(db, "s1", &id));
    int64_t ts[40];
    float v[40];
    for (int i = 0; i < 40; i++) {
        ts[i] = 1700000000000LL + i * 1000;
        v[i] = (float)(i * 3 % 17);
    }
    OK(tsdb_insert_series(db, id, ts, v, 40));
    OK(tsdb_close(db));
}

/* a crafted index may be refused or answered, but never overflow a buffer (ASan) */
static void query_safe(const char *dir)
{
    int rc;
    long rows;
    query_rows(dir, &rc, &rows);
    CHECK(rc == 0 || rc == TSDB_ERR_CORRUPT);
}

static void test_parts(const char *base)
{
    snprintf(part_dir, sizeof part_dir, "%s/crafted_part", base);
    char name[256], path[1024];

    /* index_off near 2^64: index_end wrapped to 0, the check passed, and the hash read before the mapping
       (no hash needs forging: it crashed before the comparison) */
    make_db(part_dir);
    CHECK(find_part(part_dir, name, sizeof name));
    snprintf(path, sizeof path, "%s/%s", part_dir, name);
    size_t len;
    char *buf = slurp(path, &len);
    part_footer f;
    memcpy(&f, buf + len - sizeof f, sizeof f);
    f.ngranules = 1u << 24;
    f.index_off = (uint64_t)0 - (uint64_t)sizeof(granule) * f.ngranules;
    memcpy(buf + len - sizeof f, &f, sizeof f);
    spit(path, buf, len);
    free(buf);
    CHECK(in_child(open_refused, part_dir));

    /* a granule's off + length wrapping below index_off, the index hash recomputed: the query read 8 GB before
       the mapping */
    make_db(part_dir);
    CHECK(find_part(part_dir, name, sizeof name));
    snprintf(path, sizeof path, "%s/%s", part_dir, name);
    buf = slurp(path, &len);
    memcpy(&f, buf + len - sizeof f, sizeof f);
    granule g;
    memcpy(&g, buf + f.index_off, sizeof g);
    g.ts_len = g.val_len = 0xffffffffu;
    g.off = (uint64_t)0 - ((1ull << 33) - 2) + 40;
    memcpy(buf + f.index_off, &g, sizeof g);
    spit(path, buf, len);
    free(buf);
    rehash_part(path);
    CHECK(in_child(query_refused, part_dir));
    /* the same granule in a merge, which copies whole granules without decoding them: it copied from 8 GB before
       the mapping (a shorter one goes through memcpy) */
    CHECK(in_child(merge_refused, part_dir));

    /* the 8-row granule first in the index (entries 0 and 2 swapped, hash recomputed): the raw query sizes its decode
       buffers by the largest granule it reads; sized by the first, it decoded 16 rows into room for 8 */
    make_db_3_granules(part_dir);
    CHECK(find_part(part_dir, name, sizeof name));
    snprintf(path, sizeof path, "%s/%s", part_dir, name);
    buf = slurp(path, &len);
    memcpy(&f, buf + len - sizeof f, sizeof f);
    granule gs[3];
    CHECK(f.ngranules == 3);
    if (f.ngranules == 3) {
        memcpy(gs, buf + f.index_off, sizeof gs);
        CHECK(gs[0].count == 16 && gs[1].count == 16 && gs[2].count == 8);
        memcpy(buf + f.index_off, &gs[2], sizeof(granule));
        memcpy(buf + f.index_off + 2 * sizeof(granule), &gs[0], sizeof(granule));
    }
    spit(path, buf, len);
    free(buf);
    rehash_part(path);
    CHECK(in_child(query_safe, part_dir));
    rm_rf(part_dir);
}

/* ------------------------------------------------------------- intents */

static void write_intent(const char *dir, const char *lines, int count)
{
    char text[1024], path[512];
    int n = snprintf(text, sizeof text, "tsdb-intent 3 %d\n%s", count, lines);
    n += snprintf(text + n, sizeof text - (size_t)n, "end %016llx\n",
                  (unsigned long long)xxh64(text, (size_t)n, 0));
    snprintf(path, sizeof path, "%s/txn_99_99.intent", dir); /* a name parts_commit writes: others are not read */
    spit(path, text, (size_t)n);
}

/* An intent of two renames (the fewest a writer makes) in a database from make_db: the crafted one, whose tmp
   side exists, and one already done (the database's own part), so recovery rolls forward rather than back.
   Afterwards the database must still open with its rows, and a `fin` that did not exist must not appear. */
static void one_intent(const char *dir, const char *tmp, const char *fin)
{
    char path[1024], line[600];
    snprintf(path, sizeof path, "%s/%s", dir, fin);
    bool existed = access(path, F_OK) == 0;
    if (strchr(tmp, '/') == NULL) {
        snprintf(path, sizeof path, "%s/%s", dir, tmp);
        spit(path, "garbage", 7);
    }
    char part[256];
    CHECK(find_part(dir, part, sizeof part));
    snprintf(line, sizeof line, "%s %s\ntmp_7_7.part %s\n", tmp, fin, part);
    write_intent(dir, line, 2);
    int rc;
    long rows;
    query_rows(dir, &rc, &rows);
    CHECK(rc == 0 && rows == 1000);
    snprintf(path, sizeof path, "%s/%s", dir, fin);
    CHECK(existed || access(path, F_OK) != 0);
}

static void test_intents(const char *base)
{
    char dir[512], path[1024];
    snprintf(dir, sizeof dir, "%s/crafted_intent", base);

    /* an intent naming SERIES: garbage was renamed over it (CATALOG still knows the names, so only the bytes
       show it) */
    make_db(dir);
    snprintf(path, sizeof path, "%s/SERIES", dir);
    size_t n0, n1;
    char *before = slurp(path, &n0);
    one_intent(dir, "tmp_x", "SERIES");
    char *after = slurp(path, &n1);
    CHECK(before && after && n0 == n1 && memcmp(before, after, n0) == 0);
    free(before);
    free(after);

    /* any other file of the directory: a name that is no part */
    make_db(dir);
    snprintf(path, sizeof path, "%s/victim", dir);
    spit(path, "precious", 8);
    one_intent(dir, "tmp_x", "victim");
    after = slurp(path, &n1);
    CHECK(after && n1 == 8 && memcmp(after, "precious", 8) == 0);
    free(after);

    /* MANIFEST: the open must neither fail nor lose the part */
    make_db(dir);
    one_intent(dir, "tmp_1_2.part", "MANIFEST");

    /* names a writer never makes: something after ".part" on either side */
    make_db(dir);
    one_intent(dir, "tmp_1_2.partX", "p0_900_900_0.part");
    make_db(dir);
    one_intent(dir, "tmp_1_2.part", "p0_901_901_0.part.x");

    /* a tmp side that leaves the directory */
    make_db(dir);
    char sub[600];
    snprintf(sub, sizeof sub, "%s/tmp_", dir);
    mkdir(sub, 0755);
    snprintf(path, sizeof path, "%s/outside", base);
    spit(path, "outside", 7);
    char part[256], lines[512];
    CHECK(find_part(dir, part, sizeof part));
    snprintf(lines, sizeof lines, "tmp_/../../outside p0_900_900_0.part\ntmp_7_7.part %s\n", part);
    write_intent(dir, lines, 2);
    int rc;
    long rows;
    query_rows(dir, &rc, &rows);
    CHECK(rc == 0 && rows == 1000);
    CHECK(access(path, F_OK) == 0); /* not pulled in */
    snprintf(path, sizeof path, "%s/p0_900_900_0.part", dir);
    CHECK(access(path, F_OK) != 0);
    snprintf(path, sizeof path, "%s/outside", base);
    unlink(path);
    rm_rf(dir);
}

/* ---------------------------------------------------------- dictionary */

#define DICT_MAX_NAMES ((1u << 31) - 1) /* as in series.c */

static void dict_full(const char *dir)
{
    int dfd = open(dir, O_DIRECTORY | O_RDONLY);
    series_dict d;
    OK(series_open(&d, dfd, "SERIES", false, false));
    uint32_t id;
    OK(series_get(&d, "a", true, TSDB_FLOAT32, &id));
    OK(series_get(&d, "b", true, TSDB_FLOAT32, &id));
    struct stat before, after;
    fstat(d.fd, &before);
    /* as if 2^31 - 1 names were there: one more is refused before its record is written (the table of twice
       as many slots wrapped its uint32 size at 2^30 names, and a record written first made every later open
       fail on it) */
    uint32_t real = atomic_load(&d.count);
    atomic_store(&d.count, DICT_MAX_NAMES);
    CHECK(series_get(&d, "c", true, TSDB_FLOAT32, &id) == TSDB_ERR_INVALID);
    fstat(d.fd, &after);
    CHECK(after.st_size == before.st_size);
    atomic_store(&d.count, real);
    /* existing names still resolve, and the seed is not the old public constant */
    CHECK(series_get(&d, "b", false, 0, &id) == 0 && id == 1);
    series_dict d2;
    int dfd2 = open(dir, O_DIRECTORY | O_RDONLY);
    OK(series_open(&d2, dfd2, "SERIES", true, false));
#ifndef MAIN_COMPAT /* builds against the code before the fix, to show the other checks fail there */
    CHECK(d2.seed != d.seed); /* random per open: 2^-64 to collide */
#endif
    CHECK(series_get(&d2, "a", false, 0, &id) == 0 && id == 0);
    /* and the table is placed by it: "a", the first name, sits in its own slot */
    CHECK(d.table[xxh64("a", 1, d.seed) & (d.tcap - 1)] == 1);
    /* a file that holds more names than a create may make is damage: refused at load, never stored past the end */
    OK(series_get(&d, "c", true, TSDB_FLOAT32, &id));
    uint32_t real2 = atomic_load(&d2.count);
    atomic_store(&d2.count, DICT_MAX_NAMES);
    CHECK(series_reload(&d2, dfd2) == TSDB_ERR_CORRUPT);
    atomic_store(&d2.count, real2);
    series_close(&d2);
    series_close(&d);
    close(dfd2);
    close(dfd);
}

static void test_dict(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/crafted_dict", base);
    rm_rf(dir);
    mkdir(dir, 0755);
    CHECK(in_child(dict_full, dir));
    rm_rf(dir);
}

/* ------------------------------------------------- extreme timestamps */

static void test_extremes(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/crafted_extremes", base);
    rm_rf(dir);
    tsdb_options o = opts();
    o.wal_max_batch_rows = 4; /* 3 rows: WAL and memtable; 5: straight to parts */
    tsdb *db;
    uint32_t a, b;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series(db, "a", &a));
    OK(tsdb_series(db, "b", &b));
    float v[5] = {1, 2, 3, 4, 5};
    /* a row at INT64_MAX: no half-open range reaches it, so it could never be read, counted or deleted */
    int64_t small[3] = {INT64_MIN, 1000, INT64_MAX};
    CHECK(tsdb_insert_series(db, a, small, v, 3) == TSDB_ERR_INVALID);
    CHECK(tsdb_append(db, a, INT64_MAX, 1) == TSDB_ERR_INVALID);
    int64_t sorted[5] = {INT64_MIN, 1000, 2000, 3000, INT64_MAX};
    CHECK(tsdb_insert_series(db, a, sorted, v, 5) == TSDB_ERR_INVALID);
    int64_t unsorted[5] = {3000, INT64_MAX, 1000, INT64_MIN, 2000};
    CHECK(tsdb_insert_series(db, a, unsorted, v, 5) == TSDB_ERR_INVALID);
    uint32_t ser[5] = {b, a, b, a, a};
    CHECK(tsdb_insert(db, ser, unsorted, v, 5) == TSDB_ERR_INVALID);
    uint32_t ser_sorted[5] = {a, a, a, b, b};
    int64_t ts_sorted[5] = {1, 2, INT64_MAX, 1, 2};
    CHECK(tsdb_insert(db, ser_sorted, ts_sorted, v, 5) == TSDB_ERR_INVALID);
    /* each series in time order, all in the last partition: the counting sort (bucket_write) takes it */
    uint32_t ser_bucket[5] = {b, a, b, a, a};
    int64_t ts_bucket[5] = {INT64_MAX - 4, INT64_MAX - 3, INT64_MAX - 2, INT64_MAX - 1, INT64_MAX};
    CHECK(tsdb_insert(db, ser_bucket, ts_bucket, v, 5) == TSDB_ERR_INVALID);
    tsdb_bucket *bk;
    size_t nb;
    OK(tsdb_query_group(db, a, INT64_MIN, INT64_MAX, NULL, &bk, &nb));
    CHECK(nb == 0 || bk[0].count == 0); /* all or nothing: none of the refused rows */
    free(bk);

    /* the extremes that are allowed: local grouping over an open range used to fail (TSDB_ERR_INVALID) once
       one row lay beyond the calendar's 200,000 years */
    int64_t ok3[3] = {INT64_MIN, 1000, INT64_MAX - 5};
    OK(tsdb_insert_series(db, a, ok3, v, 3));
    int64_t ok5[5] = {INT64_MIN, -5000, 1000, 86400000LL * 3, INT64_MAX - 1};
    OK(tsdb_insert_series(db, b, ok5, v, 5));
    tsdb_group g = {TSDB_DAY, 1, "+01:00"};
    for (int s = 0; s < 2; s++) {
        uint32_t id = s ? b : a;
        uint64_t want = s ? 5 : 3, total = 0;
        int rc = tsdb_query_group_local(db, id, INT64_MIN, INT64_MAX, &g, NULL, &bk, &nb);
        CHECK(rc == 0);
        for (size_t i = 0; rc == 0 && i < nb; i++)
            total += bk[i].count;
        CHECK(total == want);
        CHECK(rc != 0 || (nb > 0 && bk[0].start == INT64_MIN));
        free(bk);
        tsdb_spread_bucket *sb;
        CHECK(tsdb_query_spread_group_local(db, id, INT64_MIN, INT64_MAX, &g, NULL, &sb, &nb) == 0);
        free(sb);
        tsdb_twa_bucket *tb;
        CHECK(tsdb_query_twa_group_local(db, id, INT64_MIN, INT64_MAX, &g, NULL, &tb, &nb) == 0);
        free(tb);
    }
    /* the inner rows still have their own days: INT64_MIN joins the first, INT64_MAX - 1 the last (+01:00) */
    OK(tsdb_query_group_local(db, b, INT64_MIN, INT64_MAX, &g, NULL, &bk, &nb));
    CHECK(nb >= 2 && bk[0].start == INT64_MIN && bk[0].count == 3);
    CHECK(nb >= 2 && bk[nb - 1].start == 86400000LL * 3 - 3600000 && bk[nb - 1].count == 2);
    free(bk);
    /* only rows beyond the limit, on one side */
    uint32_t c;
    OK(tsdb_series(db, "c", &c));
    int64_t far[2] = {INT64_MAX - 10, INT64_MAX - 5};
    OK(tsdb_insert_series(db, c, far, v, 2));
    CHECK(tsdb_query_group_local(db, c, INT64_MIN, INT64_MAX, &g, NULL, &bk, &nb) == 0);
    CHECK(nb == 1 && bk[0].count == 2);
    free(bk);
    OK(tsdb_close(db));
    rm_rf(dir);
}

/* A WAL that holds a row at INT64_MAX, as a build before the refusal could write it (or a crafted segment):
   replay and the flush at close keep it, as that build did, instead of failing every open. */
static int count_slots(void *ctx, const int64_t *ts, const void *v, size_t n)
{
    (void)ts, (void)v;
    *(long *)ctx += (long)n;
    return 0;
}

static void wal_unreachable_write(const char *dir)
{
    tsdb_options o = opts();
    eng *db;
    uint32_t id;
    OK(eng_open(dir, &o, &db));
    OK(eng_series(db, "a", TSDB_FLOAT32, &id));
    int64_t ts[2] = {INT64_MAX, 1000};
    float v[2] = {1, 2};
    OK(mem_insert(db, NULL, id, ts, v, sizeof *v, 2)); /* past write_rows, which refuses it */
    _exit(failures ? 1 : 0); /* no close: the rows stay in the WAL only */
}

static void wal_unreachable_open(const char *dir)
{
    tsdb_options o = opts();
    eng *db;
    uint32_t id;
    for (int round = 0; round < 2; round++) { /* replay, flush at close; then from the part */
        int rc = eng_open(dir, &o, &db);
        CHECK(rc == 0);
        if (rc)
            return;
        OK(eng_series_lookup(db, "a", &id));
        long n = 0;
        OK(eng_query_raw_ex(db, id, INT64_MIN, INT64_MAX, false, 0, count_slots, &n));
        CHECK(n == 1);
        CHECK(eng_close(db) == 0);
    }
}

static void test_wal_unreachable(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/crafted_wal_max", base);
    rm_rf(dir);
    CHECK(in_child(wal_unreachable_write, dir));
    CHECK(in_child(wal_unreachable_open, dir));
    rm_rf(dir);
}

/* A patas header naming a ring slot before the first value: the decoder read
 * out[i - 1 - (i - 1 - ref) % RING] with an index that wrapped below zero. */
static void patas_ring(const char *unused)
{
    (void)unused;
    uint8_t blk[6 + BLK_SLACK] = {0x01, 0x02, 0x03, 0x04};
    uint16_t h = 5 << 9; /* ref 5 for the second value, no bytes, no shift */
    memcpy(blk + 4, &h, 2);
    uint32_t out[2];
    CHECK(patas_decode(blk, 6, 2, out) == TSDB_ERR_CORRUPT);
    exit(failures ? 1 : 0);
}

static void test_codecs(void)
{
    CHECK(in_child(patas_ring, NULL));
}

/* The part writer refuses rows out of (series, ts) order and writes nothing: a series run going back in time
 * (what a placement that read two class histories once wrote), a repeated timestamp, a series before the last,
 * within a granule and across granules. Before, it wrote them: queries binary-search the runs. */
static void test_writer_order(const char *base)
{
    char dir[600];
    snprintf(dir, sizeof dir, "%s/order", base);
    tsdb_options o = opts();
    o.granule_rows = 4;
    eng *db;
    OK(eng_open(dir, &o, &db));
    static const struct {
        uint32_t s1;
        int64_t a[3];
        uint32_t s2;
        int64_t b[3];
    } cases[] = {
        {1, {10, 20, 30}, 1, {5, 6, 7}},     /* back in time across calls */
        {1, {10, 20, 30}, 1, {30, 31, 32}},  /* a repeated ts */
        {2, {10, 20, 30}, 1, {40, 50, 60}},  /* a series before the last */
        {1, {10, 30, 20}, 2, {1, 2, 3}},     /* within a granule */
    };
    float v[3] = {1, 2, 3};
    for (size_t c = 0; c < sizeof cases / sizeof *cases; c++) {
        part_writer w;
        OK(pw_begin(&w, db, 0, 0));
        int rc = pw_add(&w, cases[c].s1, TSDB_FLOAT32, cases[c].a, v, 3);
        if (rc == 0) {
            rc = pw_add(&w, cases[c].s2, TSDB_FLOAT32, cases[c].b, v, 3);
            part *p = NULL;
            if (rc == 0)
                rc = pw_finish(&w, 1, 1, 0, &p);
            else
                pw_abort(&w);
            if (p)
                part_unref(p);
        } else {
            pw_abort(&w);
        }
        CHECK(rc == TSDB_ERR_CORRUPT);
    }
    part_writer w; /* and a good one still goes through */
    OK(pw_begin(&w, db, 0, 0));
    int64_t t1[3] = {10, 20, 30}, t2[3] = {40, 50, 60};
    OK(pw_add(&w, 1, TSDB_FLOAT32, t1, v, 3));
    OK(pw_add(&w, 1, TSDB_FLOAT32, t2, v, 3));
    pw_abort(&w);
    CHECK(eng_close(db) == 0);
}

int main(int argc, char **argv)
{
    /* a directory of this run's own: the fixed names below collided between worktrees testing at once */
    char base[400];
    snprintf(base, sizeof base, "%s/tsdb-crafted-%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    rm_rf(base);
    if (mkdir(base, 0755) != 0) {
        perror(base);
        return 1;
    }
    test_manifest(base);
    test_parts(base);
    test_intents(base);
    test_dict(base);
    test_extremes(base);
    test_wal_unreachable(base);
    test_writer_order(base);
    test_codecs();
    if (failures) {
        fprintf(stderr, "test_crafted: %d failures\n", failures);
        return 1;
    }
    rm_rf(base);
    printf("test_crafted: ok\n");
    return 0;
}
