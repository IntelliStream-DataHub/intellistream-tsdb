/*
 * Damage on disk is never silent (docs/design.md, "Damage on disk").
 *
 * Each test builds a small database, damages one file the way a crash or a
 * failing disk does (a cut tail, zeros past the end, a flipped byte in the
 * middle), and checks what the next open, query or follower refresh makes of
 * it: a torn tail, which a crash leaves, is cut and the open goes on with
 * every record before it; damage with valid records after it fails with
 * TSDB_ERR_CORRUPT and keeps the file, because cutting there would drop
 * acknowledged rows without a word. A valid record moved to another offset
 * counts as damage; a read error or a short read (injected through pread) is
 * never taken for the end of a file.
 *
 *   ./build/test_damage [scratch dir]
 */
#define _GNU_SOURCE
#include "internal.h"
#include "tsdb.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <ftw.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <time.h>
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
            fprintf(stderr, "%s:%d: %s = %s\n", __FILE__, __LINE__, #expr, tsdb_strerror(rc_)); \
            failures++;                                                                      \
        }                                                                                    \
    } while (0)

#define RC(expr, want)                                                                       \
    do {                                                                                     \
        int rc_ = (expr);                                                                    \
        if (rc_ != (want)) {                                                                 \
            fprintf(stderr, "%s:%d: %s = %s, expected %s\n", __FILE__, __LINE__, #expr,      \
                    tsdb_strerror(rc_), tsdb_strerror(want));                                \
            failures++;                                                                      \
        }                                                                                    \
    } while (0)

static const int64_t T0 = 1700000000000;

/* ------------------------------------------------------------- files */

static int rm_one(const char *p, const struct stat *s, int flag, struct FTW *f)
{
    (void)s, (void)flag, (void)f;
    return remove(p);
}

static void rm_rf(const char *dir) { nftw(dir, rm_one, 16, FTW_DEPTH | FTW_PHYS); }

static void path(char *buf, size_t len, const char *dir, const char *name)
{
    if ((size_t)snprintf(buf, len, "%s/%s", dir, name) >= len)
        abort();
}

static off_t file_size(const char *dir, const char *name)
{
    char p[1024];
    path(p, sizeof p, dir, name);
    struct stat st;
    return stat(p, &st) == 0 ? st.st_size : -1;
}

/* The first file in dir whose name starts with prefix and ends with suffix. */
static bool find_file(const char *dir, const char *prefix, const char *suffix, char *out, size_t len)
{
    DIR *d = opendir(dir);
    if (!d)
        return false;
    struct dirent *e;
    bool found = false;
    while (!found && (e = readdir(d)) != NULL) {
        size_t n = strlen(e->d_name), s = strlen(suffix);
        if (strncmp(e->d_name, prefix, strlen(prefix)) == 0 && n >= s && strcmp(e->d_name + n - s, suffix) == 0) {
            snprintf(out, len, "%s", e->d_name);
            found = true;
        }
    }
    closedir(d);
    return found;
}

static void file_pread(const char *dir, const char *name, off_t off, void *buf, size_t len)
{
    char p[1024];
    path(p, sizeof p, dir, name);
    int fd = open(p, O_RDONLY);
    CHECK(fd >= 0 && pread(fd, buf, len, off) == (ssize_t)len);
    if (fd >= 0)
        close(fd);
}

static void file_pwrite(const char *dir, const char *name, off_t off, const void *buf, size_t len)
{
    char p[1024];
    path(p, sizeof p, dir, name);
    int fd = open(p, O_WRONLY);
    CHECK(fd >= 0 && pwrite(fd, buf, len, off) == (ssize_t)len);
    if (fd >= 0)
        close(fd);
}

static void flip(const char *dir, const char *name, off_t off)
{
    uint8_t b;
    file_pread(dir, name, off, &b, 1);
    b ^= 0x10;
    file_pwrite(dir, name, off, &b, 1);
}

static void cut(const char *dir, const char *name, off_t len)
{
    char p[1024];
    path(p, sizeof p, dir, name);
    CHECK(truncate(p, len) == 0);
}

static void append_zeros(const char *dir, const char *name, size_t n)
{
    uint8_t *z = calloc(1, n);
    file_pwrite(dir, name, file_size(dir, name), z, n);
    free(z);
}

/* The library's pread calls land here (the test binary's definition wins over
 * libc's): while armed, reads of files whose name starts with fault_name fail
 * with EIO, or find the file ending after 8 bytes (a short read); or the next one only returns its last 8 bytes
 * garbled (a misdirected read, which a second read does not repeat). */
enum { FAULT_NONE, FAULT_EIO, FAULT_SHORT, FAULT_GARBLE };
static atomic_int fault_mode, fault_fired;
static char fault_name[32];

static void arm_read_fault(int mode, const char *name)
{
    snprintf(fault_name, sizeof fault_name, "%s", name);
    atomic_store(&fault_fired, 0);
    atomic_store(&fault_mode, mode);
}

ssize_t pread(int fd, void *buf, size_t n, off_t off)
{
    int mode = atomic_load(&fault_mode);
    if (mode != FAULT_NONE) {
        char link[64], target[PATH_MAX];
        snprintf(link, sizeof link, "/proc/self/fd/%d", fd);
        ssize_t l = readlink(link, target, sizeof target - 1);
        if (l > 0) {
            target[l] = '\0';
            const char *base = strrchr(target, '/');
            base = base ? base + 1 : target;
            if (strncmp(base, fault_name, strlen(fault_name)) == 0) {
                atomic_fetch_add(&fault_fired, 1);
                if (mode == FAULT_GARBLE && atomic_exchange(&fault_mode, FAULT_NONE) == FAULT_GARBLE) {
                    ssize_t r = syscall(SYS_pread64, fd, buf, n, off);
                    for (ssize_t i = r > 8 ? r - 8 : 0; i < r; i++)
                        ((uint8_t *)buf)[i] ^= 0xa5;
                    return r;
                }
                if (mode == FAULT_EIO) {
                    errno = EIO;
                    return -1;
                }
                if (off >= 8)
                    return 0;
                if (n > (size_t)(8 - off))
                    n = (size_t)(8 - off);
            }
        }
    }
    return syscall(SYS_pread64, fd, buf, n, off);
}

/* the fortified variant, which _FORTIFY_SOURCE builds call where the buffer's size is known */
ssize_t __pread_chk(int fd, void *buf, size_t n, off_t off, size_t buflen);
ssize_t __pread_chk(int fd, void *buf, size_t n, off_t off, size_t buflen)
{
    if (n > buflen)
        abort();
    return pread(fd, buf, n, off);
}

/* ------------------------------------------------------------- database */

static tsdb_options opts(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.granule_rows = 64;
    o.fsync = 0;
    o.merge_threads = 0; /* a merge would read (and check) the damaged part on its own */
    o.query_threads = 2;
    o.flush_interval_ms = 1000000;
    o.wal_sync_ms = 0;
    return o;
}

static long count_rows(tsdb *db, tsdb_sid id, int *rc)
{
    tsdb_points pts = {0};
    *rc = tsdb_query_points(db, id, 0, INT64_MAX, &pts);
    long n = *rc == 0 ? (long)pts.n : -1;
    tsdb_points_free(&pts);
    return n;
}

static long rows_of(const char *dir, const char *series)
{
    tsdb_options o = opts();
    tsdb *db;
    if (tsdb_open(dir, &o, &db) != 0)
        return -2;
    tsdb_sid id;
    long n = -3;
    int rc;
    if (tsdb_series_lookup(db, series, &id) == 0)
        n = count_rows(db, id, &rc);
    tsdb_close(db);
    return n;
}

static int try_open(const char *dir)
{
    tsdb_options o = opts();
    tsdb *db;
    int rc = tsdb_open(dir, &o, &db);
    if (rc == 0)
        tsdb_close(db);
    return rc;
}

/* ------------------------------------------------------------- SERIES */

static size_t rec_size(size_t name_len) { return (8 + name_len + 7) & ~(size_t)7; }

static void make_series(const char *dir, const char *const *names, int n)
{
    rm_rf(dir);
    tsdb_options o = opts();
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    for (int i = 0; i < n; i++) {
        tsdb_sid id;
        OK(tsdb_series(db, names[i], &id));
        CHECK(id == TSDB_SID(0, i));
    }
    OK(tsdb_close(db));
}

static void check_names(const char *dir, const char *const *names, int n, int present)
{
    tsdb_options o = opts();
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    for (int i = 0; i < n; i++) {
        tsdb_sid id;
        int rc = tsdb_series_lookup(db, names[i], &id);
        if (i < present)
            CHECK(rc == 0 && id == TSDB_SID(0, i));
        else
            CHECK(rc == TSDB_ERR_NOTFOUND);
    }
    OK(tsdb_close(db));
}

static void test_series(const char *scratch)
{
    char dir[600];
    snprintf(dir, sizeof dir, "%s/damage-series", scratch);
    static const char *const names[] = {"pump.inlet.pressure", "pump.outlet.pressure", "pump.speed"};
    size_t off1 = rec_size(strlen(names[0])), off2 = off1 + rec_size(strlen(names[1]));
    size_t end = off2 + rec_size(strlen(names[2]));

    make_series(dir, names, 3);
    CHECK(file_size(dir, "SERIES") == (off_t)end);

    /* a torn last record (a crash during the append): cut, the others stay */
    cut(dir, "SERIES", (off_t)off2 + 5);
    check_names(dir, names, 3, 2);
    CHECK(file_size(dir, "SERIES") == (off_t)off2);

    /* zeros past the end (the size reached the disk before the data): cut */
    make_series(dir, names, 3);
    append_zeros(dir, "SERIES", 4096);
    check_names(dir, names, 3, 3);
    CHECK(file_size(dir, "SERIES") == (off_t)end);

    /* a flipped bit in the name of the middle record, a valid one after it */
    make_series(dir, names, 3);
    flip(dir, "SERIES", (off_t)off1 + 10);
    RC(try_open(dir), TSDB_ERR_CORRUPT);
    CHECK(file_size(dir, "SERIES") == (off_t)end); /* kept as it was */

    /* a damaged length field in the middle record */
    make_series(dir, names, 3);
    flip(dir, "SERIES", (off_t)off1 + 4);
    RC(try_open(dir), TSDB_ERR_CORRUPT);

    /* the first record zeroed: no ids may shift onto other names */
    make_series(dir, names, 3);
    uint8_t z[8] = {0};
    file_pwrite(dir, "SERIES", 0, z, sizeof z);
    RC(try_open(dir), TSDB_ERR_CORRUPT);
    printf("  SERIES: torn tail and zero tail cut, damage before valid records fails the open\n");
    rm_rf(dir);
}

/* ------------------------------------------------------------- WAL */

enum { WAL_RECS = 10, WAL_RECROWS = 5 };
#define WAL_RECLEN (WAL_HDR + WAL_RECROWS * WAL_ROW)

/* A child writes WAL_RECS inserts of WAL_RECROWS rows (one WAL record each)
 * and dies without closing: the rows live only in the WAL. */
static void crash_with_wal(const char *dir, char *wal, size_t len)
{
    rm_rf(dir);
    pid_t pid = fork();
    if (pid == 0) {
        tsdb_options o = opts();
        tsdb *db;
        tsdb_sid id;
        if (tsdb_open(dir, &o, &db) || tsdb_series(db, "wal.series", &id))
            _exit(1);
        for (int r = 0; r < WAL_RECS; r++) {
            int64_t ts[WAL_RECROWS];
            float v[WAL_RECROWS];
            for (int i = 0; i < WAL_RECROWS; i++) {
                ts[i] = T0 + (r * WAL_RECROWS + i) * 1000;
                v[i] = (float)(r * WAL_RECROWS + i);
            }
            if (tsdb_insert_series(db, id, ts, v, WAL_RECROWS))
                _exit(1);
        }
        _exit(0); /* no tsdb_close: a crash */
    }
    int status;
    waitpid(pid, &status, 0);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    CHECK(find_file(dir, "wal_", ".log", wal, len));
    CHECK(file_size(dir, wal) == WAL_RECS * WAL_RECLEN);
}

static void test_wal(const char *scratch)
{
    char dir[600], wal[256];
    snprintf(dir, sizeof dir, "%s/damage-wal", scratch);

    crash_with_wal(dir, wal, sizeof wal);
    CHECK(rows_of(dir, "wal.series") == WAL_RECS * WAL_RECROWS);

    /* the last record torn: the rows before it come back */
    crash_with_wal(dir, wal, sizeof wal);
    cut(dir, wal, WAL_RECS * WAL_RECLEN - 30);
    CHECK(rows_of(dir, "wal.series") == (WAL_RECS - 1) * WAL_RECROWS);

    /* a cut inside the header of the last record */
    crash_with_wal(dir, wal, sizeof wal);
    cut(dir, wal, (WAL_RECS - 1) * WAL_RECLEN + 6);
    CHECK(rows_of(dir, "wal.series") == (WAL_RECS - 1) * WAL_RECROWS);

    /* zeros past the last record */
    crash_with_wal(dir, wal, sizeof wal);
    append_zeros(dir, wal, 4096);
    CHECK(rows_of(dir, "wal.series") == WAL_RECS * WAL_RECROWS);

    /* record 3 of 10 damaged (a row byte, then a header byte): the open fails, the segment stays */
    for (int where = 0; where < 2; where++) {
        crash_with_wal(dir, wal, sizeof wal);
        flip(dir, wal, 2 * WAL_RECLEN + (where == 0 ? WAL_HDR + 7 : 1));
        RC(try_open(dir), TSDB_ERR_CORRUPT);
        CHECK(file_size(dir, wal) == WAL_RECS * WAL_RECLEN);
        RC(try_open(dir), TSDB_ERR_CORRUPT); /* and again: nothing was cut or replayed */
        CHECK(file_size(dir, wal) == WAL_RECS * WAL_RECLEN);
        /* moving the segment away accepts the loss */
        char from[1024], to[1100];
        path(from, sizeof from, dir, wal);
        snprintf(to, sizeof to, "%s/../%s.moved", dir, wal);
        CHECK(rename(from, to) == 0);
        CHECK(rows_of(dir, "wal.series") == 0);
        unlink(to);
    }
    printf("  WAL: torn tail and zero tail ignored, a damaged record before valid ones fails the open\n");
    rm_rf(dir);
}

/* A follower that meets a damaged WAL record with valid ones after it does
 * not stall in silence: once the damage outlasts the time NFS may take to
 * deliver appends in order, refreshes fail with TSDB_ERR_CORRUPT. */
static void test_wal_follower(const char *scratch)
{
    char dir[600], wal[256];
    snprintf(dir, sizeof dir, "%s/damage-walfollow", scratch);
    rm_rf(dir);
    tsdb_options o = opts();
    tsdb *w, *f;
    tsdb_sid id;
    OK(tsdb_open(dir, &o, &w));
    OK(tsdb_series(w, "follow.series", &id));
    int64_t ts[WAL_RECROWS];
    float v[WAL_RECROWS];
    for (int r = 0; r < 6; r++) {
        for (int i = 0; i < WAL_RECROWS; i++) {
            ts[i] = T0 + (r * WAL_RECROWS + i) * 1000;
            v[i] = (float)i;
        }
        if (r == 3) {
            /* the follower has records 0-2 */
            tsdb_options fo = opts();
            fo.follow = 1;
            fo.read_only = 1;
            fo.refresh_ms = 20;
            OK(tsdb_open(dir, &fo, &f));
            OK(tsdb_refresh(f));
            int rc;
            CHECK(count_rows(f, id, &rc) == 3 * WAL_RECROWS);
        }
        OK(tsdb_insert_series(w, id, ts, v, WAL_RECROWS));
    }
    CHECK(find_file(dir, "wal_", ".log", wal, sizeof wal));
    CHECK(file_size(dir, wal) == 6 * WAL_RECLEN);
    flip(dir, wal, 3 * WAL_RECLEN + WAL_HDR + 2); /* record 3, which the follower has not read */

    /* within the patience a refresh is quiet (the appends may still be on their way) */
    int rc = tsdb_refresh(f);
    CHECK(rc == 0);
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    double waited = 0;
    while ((rc = tsdb_refresh(f)) == 0 && waited < 10) {
        usleep(50000);
        clock_gettime(CLOCK_MONOTONIC, &t1);
        waited = (double)(t1.tv_sec - t0.tv_sec) + (double)(t1.tv_nsec - t0.tv_nsec) / 1e9;
    }
    RC(rc, TSDB_ERR_CORRUPT);
    CHECK(waited >= 1.5); /* patience: max(2 s, 20 refreshes) */
    tsdb_stats st;
    OK(tsdb_get_stats(f, &st));
    CHECK(st.follow_errors > 0);
    CHECK(count_rows(f, id, &rc) == 3 * WAL_RECROWS); /* what it had stays queryable */
    printf("  WAL follower: damage reported after %.1f s, %llu failed refreshes\n", waited,
           (unsigned long long)st.follow_errors);
    OK(tsdb_close(f));
    OK(tsdb_close(w));
    rm_rf(dir);
}

/* ------------------------------------------------------------- parts */

enum { PART_ROWS = 1000 };

static void make_part(const char *dir, char *part, size_t len)
{
    rm_rf(dir);
    tsdb_options o = opts();
    tsdb *db;
    tsdb_sid id;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series(db, "part.series", &id));
    int64_t ts[PART_ROWS];
    float v[PART_ROWS];
    for (int i = 0; i < PART_ROWS; i++) {
        ts[i] = T0 + i * 1000;
        v[i] = (float)(i % 37) * 0.25f;
    }
    OK(tsdb_insert_series(db, id, ts, v, PART_ROWS));
    OK(tsdb_close(db));
    CHECK(find_file(dir, "p", ".part", part, len));
}

static part_footer read_footer(const char *dir, const char *part)
{
    part_footer ft;
    file_pread(dir, part, file_size(dir, part) - (off_t)sizeof ft, &ft, sizeof ft);
    return ft;
}

static void query_part(const char *dir, int verify, int *rc, uint64_t *checks)
{
    tsdb_options o = opts();
    o.verify_checksums = verify;
    tsdb *db;
    tsdb_sid id;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series_lookup(db, "part.series", &id));
    uint64_t before = atomic_load(&granule_checks);
    count_rows(db, id, rc);
    *checks = atomic_load(&granule_checks) - before;
    OK(tsdb_close(db));
}

static void test_parts(const char *scratch)
{
    char dir[600], part[256];
    snprintf(dir, sizeof dir, "%s/damage-part", scratch);
    int rc;
    uint64_t checks;

    /* undamaged: verify on checks every granule, off checks none */
    make_part(dir, part, sizeof part);
    query_part(dir, 1, &rc, &checks);
    CHECK(rc == 0 && checks == (PART_ROWS + 63) / 64);
    query_part(dir, 0, &rc, &checks);
    CHECK(rc == 0 && checks == 0);

    /* a flipped byte in a granule: caught by verify_checksums, and not paid for without it (whatever the
     * decoder then makes of it). In the middle of the data: the last bytes before the index may be the padding
     * to 8 (format 7's smaller zstd frames left 3 of them here) */
    make_part(dir, part, sizeof part);
    part_footer ft = read_footer(dir, part);
    flip(dir, part, (off_t)(sizeof(part_header) + ft.index_off) / 2);
    query_part(dir, 1, &rc, &checks);
    RC(rc, TSDB_ERR_CORRUPT);
    CHECK(checks > 0);
    query_part(dir, 0, &rc, &checks);
    CHECK(checks == 0);

    /* a flipped byte in the index, in the footer's max_ts, in the header's wal_seq:
     * the open fails and names the part */
    static const char *const what[] = {"index", "footer max_ts", "header wal_seq"};
    for (int k = 0; k < 3; k++) {
        make_part(dir, part, sizeof part);
        ft = read_footer(dir, part);
        off_t size = file_size(dir, part);
        off_t at = k == 0   ? (off_t)ft.index_off + 9
                   : k == 1 ? size - (off_t)sizeof ft + (off_t)offsetof(part_footer, max_ts) + 1
                            : (off_t)offsetof(part_header, wal_seq);
        flip(dir, part, at);
        if (try_open(dir) != TSDB_ERR_CORRUPT) {
            fprintf(stderr, "%s:%d: a part with a damaged %s opens\n", __FILE__, __LINE__, what[k]);
            failures++;
        }
        CHECK(file_size(dir, part) == size); /* not renamed, not cut */
        /* renaming it to .broken accepts the loss */
        char from[1024], to[1100];
        path(from, sizeof from, dir, part);
        snprintf(to, sizeof to, "%s.broken", from);
        CHECK(rename(from, to) == 0);
        CHECK(rows_of(dir, "part.series") == 0);
    }
    printf("  parts: granule damage caught with verify_checksums (%d granules checked), index/header/footer at open\n",
           (PART_ROWS + 63) / 64);
    rm_rf(dir);
}

/* A follower that finds a new part broken keeps its previous view, counts and
 * logs the part once, and its refreshes fail with TSDB_ERR_CORRUPT. */
static void test_part_follower(const char *scratch)
{
    char dir[600], first[256], part[256] = "";
    snprintf(dir, sizeof dir, "%s/damage-partfollow", scratch);
    make_part(dir, first, sizeof first);
    tsdb_options o = opts();
    tsdb *w, *f = NULL;
    tsdb_sid id;
    OK(tsdb_open(dir, &o, &w));
    OK(tsdb_series_lookup(w, "part.series", &id));
    tsdb_options ro = opts();
    ro.read_only = 1; /* no polling thread: refreshes happen here */
    OK(tsdb_open(dir, &ro, &f));
    OK(tsdb_append(w, id, T0 + 2 * PART_ROWS * 1000, 1.0f));
    OK(tsdb_flush(w));
    DIR *d = opendir(dir);
    struct dirent *e;
    while (d && (e = readdir(d)) != NULL) {
        size_t n = strlen(e->d_name);
        if (e->d_name[0] == 'p' && n > 5 && strcmp(e->d_name + n - 5, ".part") == 0 && strcmp(e->d_name, first) != 0)
            snprintf(part, sizeof part, "%s", e->d_name);
    }
    if (d)
        closedir(d);
    CHECK(part[0] != '\0');
    if (f && part[0]) {
        flip(dir, part, (off_t)read_footer(dir, part).index_off + 9);
        int rc;
        for (int i = 0; i < 2; i++)
            RC(tsdb_refresh(f), TSDB_ERR_CORRUPT);
        tsdb_stats st;
        OK(tsdb_get_stats(f, &st));
        CHECK(st.broken_parts == 1 && st.follow_errors >= 2);
        CHECK(count_rows(f, id, &rc) == PART_ROWS); /* the previous view */
    }
    if (f)
        OK(tsdb_close(f));
    OK(tsdb_close(w));
    printf("  parts: a follower keeps its previous view of a broken part and reports it\n");
    rm_rf(dir);
}

/* ------------------------------------------------------------- MANIFEST */

struct restore {
    const char *dir;
    uint8_t *data;
    size_t len;
    int delay_ms;
};

static void *restore_main(void *arg)
{
    struct restore *r = arg;
    usleep((useconds_t)r->delay_ms * 1000);
    file_pwrite(r->dir, "MANIFEST", 0, r->data, r->len);
    return NULL;
}

/* A follower opening on a MANIFEST caught mid-rewrite (its first sector
 * torn) waits for the rewrite rather than starting empty; one that stays
 * damaged fails the open. The writer itself rebuilds MANIFEST at open. */
static void test_manifest(const char *scratch)
{
    char dir[600];
    snprintf(dir, sizeof dir, "%s/damage-manifest", scratch);
    char part[256];
    make_part(dir, part, sizeof part);
    off_t len = file_size(dir, "MANIFEST");
    CHECK(len > 0);
    if (len <= 0)
        return;
    uint8_t *orig = malloc((size_t)len);
    file_pread(dir, "MANIFEST", 0, orig, (size_t)len);
    size_t torn = (size_t)len < 512 ? (size_t)len : 512;
    uint8_t *junk = malloc(torn);
    for (size_t i = 0; i < torn; i++)
        junk[i] = (uint8_t)(orig[i] ^ 0x5a);

    tsdb_options fo = opts();
    fo.follow = 1;
    fo.read_only = 1;
    fo.refresh_ms = 20;
    tsdb *f;
    tsdb_sid id;
    int rc;

    /* torn for 300 ms, then the rewrite completes: the follower sees the rows */
    file_pwrite(dir, "MANIFEST", 0, junk, torn);
    struct restore r = {dir, orig, (size_t)len, 300};
    pthread_t th;
    pthread_create(&th, NULL, restore_main, &r);
    OK(tsdb_open(dir, &fo, &f));
    pthread_join(th, NULL);
    if (f) {
        OK(tsdb_series_lookup(f, "part.series", &id));
        CHECK(count_rows(f, id, &rc) == PART_ROWS);
        OK(tsdb_close(f));
    }

    /* torn for good: the follower's open fails rather than show an empty database */
    file_pwrite(dir, "MANIFEST", 0, junk, torn);
    f = NULL;
    RC(tsdb_open(dir, &fo, &f), TSDB_ERR_CORRUPT);
    if (f)
        tsdb_close(f);

    /* torn after a follower opened, and its refreshes come slower than refresh_ms:
     * the failure is still logged (once) when the patience runs out */
    file_pwrite(dir, "MANIFEST", 0, orig, (size_t)len);
    tsdb_options ro = opts();
    ro.read_only = 1; /* no polling thread: refreshes happen here */
    ro.refresh_ms = 20;
    f = NULL;
    OK(tsdb_open(dir, &ro, &f));
    if (f) {
        file_pwrite(dir, "MANIFEST", 0, junk, torn);
        char log[700];
        snprintf(log, sizeof log, "%s.stderr", dir);
        fflush(stderr);
        int saved = dup(2), lfd = open(log, O_RDWR | O_CREAT | O_TRUNC, 0644);
        dup2(lfd, 2);
        int rc0 = tsdb_refresh(f);
        usleep(2300 * 1000); /* past the patience (2 s) and one refresh interval more */
        int rc1 = tsdb_refresh(f), rc2 = tsdb_refresh(f);
        fflush(stderr);
        dup2(saved, 2);
        close(saved);
        char text[4096] = "";
        ssize_t got = pread(lfd, text, sizeof text - 1, 0);
        text[got > 0 ? got : 0] = '\0';
        close(lfd);
        unlink(log);
        RC(rc0, 0);
        RC(rc1, TSDB_ERR_CORRUPT);
        RC(rc2, TSDB_ERR_CORRUPT);
        const char *line = strstr(text, "MANIFEST has failed its checks");
        CHECK(line && !strstr(line + 1, "MANIFEST has failed its checks"));
        file_pwrite(dir, "MANIFEST", 0, orig, (size_t)len);
        OK(tsdb_refresh(f));
        OK(tsdb_close(f));
    }

    /* the writer rebuilds it from the parts */
    file_pwrite(dir, "MANIFEST", 0, junk, torn);
    CHECK(rows_of(dir, "part.series") == PART_ROWS);
    printf("  MANIFEST: a follower waits out a torn rewrite, and fails the open on one that stays torn\n");
    free(orig);
    free(junk);
    rm_rf(dir);
}

/* ------------------------------------------------- misdirected writes */

/* Two parts of one series (PART_ROWS rows each, inserted straight to parts), the database left open: the caller
 * damages a part in place, as a misdirected write meant for another file would, after the open checked it. */
static tsdb *open_two_parts(const char *dir, int verify, tsdb_sid *id, char *part, size_t len)
{
    rm_rf(dir);
    tsdb_options o = opts();
    o.verify_checksums = verify;
    o.wal_max_batch_rows = 100; /* straight to parts */
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series(db, "md.series", id));
    int64_t ts[PART_ROWS];
    float v[PART_ROWS];
    for (int k = 0; k < 2; k++) {
        for (int i = 0; i < PART_ROWS; i++) {
            ts[i] = T0 + (k * PART_ROWS + i) * 1000;
            v[i] = (float)(i % 37) * 0.25f;
        }
        OK(tsdb_insert_series(db, *id, ts, v, PART_ROWS));
    }
    CHECK(find_file(dir, "p", ".part", part, len));
    return db;
}

/* A misdirected write into the index of a part the server has mapped (found by jepsen --nemesis misdirect): the
 * open checked the index, the readers trust it, and the mapping shows what reached the file since. A run table
 * entry made gix_run_of return UINT32_MAX and a merge read rser[UINT32_MAX] (SIGSEGV). Now every entry number
 * taken from the run table stays in the index, and with verify_checksums the index is read from the copy that was
 * checked, so the answers do not change at all. */
static void test_index_after_open(const char *scratch)
{
    char dir[600], part[256];
    snprintf(dir, sizeof dir, "%s/damage-index-live", scratch);
    for (int verify = 0; verify <= 1; verify++) {
        tsdb_sid id;
        tsdb *db = open_two_parts(dir, verify, &id, part, sizeof part);
        int rc;
        CHECK(count_rows(db, id, &rc) == 2 * PART_ROWS);
        part_footer ft = read_footer(dir, part);
        uint8_t junk[4096];
        memset(junk, 0xa5, sizeof junk);
        size_t n = ft.index_len < sizeof junk ? (size_t)ft.index_len : sizeof junk;
        file_pwrite(dir, part, (off_t)ft.index_off, junk, n);
        long rows = count_rows(db, id, &rc); /* without verify: anything but a crash */
        if (verify)
            CHECK(rc == 0 && rows == 2 * PART_ROWS);
        rc = tsdb_optimize(db); /* a merge of both parts walks the run table */
        if (verify) {
            CHECK(rc == 0);
            CHECK(count_rows(db, id, &rc) == 2 * PART_ROWS && rc == 0);
        }
        tsdb_close(db);
    }
    printf("  a part's index damaged after the open: no crash; with verify_checksums the answers stay right\n");
    rm_rf(dir);
}

/* SERIES damaged so that no valid record follows the first bad one is cut as a torn tail at the open: a misdirected
 * write once overwrote all four records of jepsen's SERIES with WAL bytes, the open cut the file to nothing, and
 * every series and its rows were gone without an error. A part that holds a series SERIES no longer has fails the
 * open now. */
static void test_series_cut_under_parts(const char *scratch)
{
    char dir[600], part[256];
    snprintf(dir, sizeof dir, "%s/damage-series-cut", scratch);
    for (int whole = 0; whole <= 1; whole++) {
        tsdb_sid id;
        tsdb *db = open_two_parts(dir, 0, &id, part, sizeof part);
        tsdb_sid id2;
        OK(tsdb_series(db, "md.other", &id2)); /* the last record, with rows in a part */
        int64_t ts[PART_ROWS];
        float v[PART_ROWS];
        for (int i = 0; i < PART_ROWS; i++)
            ts[i] = T0 + i, v[i] = 1;
        OK(tsdb_insert_series(db, id2, ts, v, PART_ROWS));
        OK(tsdb_close(db));
        off_t size = file_size(dir, "SERIES");
        off_t from = whole ? 0 : size - (off_t)rec_size(strlen("md.other"));
        uint8_t junk[256];
        for (size_t i = 0; i < sizeof junk; i++)
            junk[i] = (uint8_t)(i * 37 + 11);
        file_pwrite(dir, "SERIES", from, junk, (size_t)(size - from));
        RC(try_open(dir), TSDB_ERR_CORRUPT);
    }
    printf("  SERIES cut below series that parts hold: the open fails\n");
    rm_rf(dir);
}

/* src/volumes.c: switch a series' class at *at (tests/test_classes.c) */
int tsdb_test_class_switch(tsdb *t, tsdb_sid id, int cls, const int64_t *at);

/* A read at the open that returns other bytes at the end of the file than the disk holds (a misdirected read, found
 * by jepsen --nemesis misdirect) made the last records of the WAL, a dictionary, RETENTION or CLASSES look torn,
 * and the open cut them: acknowledged writes lost to a fault a second read does not repeat. The tail is read again
 * before it is cut; reads that differ fail the open, and the next open finds everything. */
static void test_misread_tail(const char *scratch)
{
    char dir[600], wal[256];
    snprintf(dir, sizeof dir, "%s/damage-misread", scratch);

    crash_with_wal(dir, wal, sizeof wal);
    arm_read_fault(FAULT_GARBLE, "wal_");
    RC(try_open(dir), TSDB_ERR_IO);
    CHECK(atomic_load(&fault_fired) == 1);
    CHECK(rows_of(dir, "wal.series") == WAL_RECS * WAL_RECROWS);

    static const char *const names[] = {"pump.inlet.pressure", "pump.outlet.pressure", "pump.speed"};
    make_series(dir, names, 3);
    off_t size = file_size(dir, "SERIES");
    arm_read_fault(FAULT_GARBLE, "SERIES");
    RC(try_open(dir), TSDB_ERR_IO);
    CHECK(atomic_load(&fault_fired) == 1 && file_size(dir, "SERIES") == size);
    check_names(dir, names, 3, 3);

    rm_rf(dir);
    tsdb_options o = opts();
    tsdb *db;
    tsdb_sid id;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series(db, "kept", &id));
    OK(tsdb_set_retention(db, id, 7 * 86400000LL));
    OK(tsdb_set_retention(db, id, 9 * 86400000LL));
    int64_t t1 = T0 + 86400000LL, t2 = T0 + 2 * 86400000LL;
    OK(tsdb_test_class_switch(db, id, 2, &t1));
    OK(tsdb_test_class_switch(db, id, 3, &t2));
    OK(tsdb_close(db));
    off_t rsize = file_size(dir, "RETENTION"), csize = file_size(dir, "CLASSES");
    arm_read_fault(FAULT_GARBLE, "RETENTION");
    RC(try_open(dir), TSDB_ERR_IO);
    CHECK(atomic_load(&fault_fired) == 1);
    arm_read_fault(FAULT_GARBLE, "CLASSES");
    RC(try_open(dir), TSDB_ERR_IO);
    CHECK(atomic_load(&fault_fired) == 1);
    int64_t limit = 0;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_get_retention(db, id, &limit));
    CHECK(limit == 9 * 86400000LL);
    OK(tsdb_close(db));
    CHECK(file_size(dir, "RETENTION") == rsize && file_size(dir, "CLASSES") == csize);
    printf("  a misread tail of the WAL, SERIES, RETENTION or CLASSES at the open: read again, nothing cut\n");
    rm_rf(dir);
}

/* src/db.c: in an open that lists the parts itself, just before it does */
void tsdb_test_before_parts(void (*fn)(void *), void *arg);

/* A writer opens, creates a series and writes a part of it between a reader's read of SERIES and its listing of
 * the parts (a reader opens without MANIFEST only before the first writer published one). */
static void writer_in_between(void *arg)
{
    const char *dir = arg;
    tsdb_test_before_parts(NULL, NULL); /* not in the writer's own open */
    tsdb_options o = opts();
    o.wal_max_batch_rows = 100; /* straight to a part */
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid id;
    OK(tsdb_series(db, "race.late", &id));
    int64_t ts[PART_ROWS];
    float v[PART_ROWS];
    for (int i = 0; i < PART_ROWS; i++)
        ts[i] = T0 + i, v[i] = 2;
    OK(tsdb_insert_series(db, id, ts, v, PART_ROWS));
    OK(tsdb_close(db));
}

/* The check that parts hold only series SERIES has must not refuse a reader that raced a writer: it reads SERIES
 * again before it judges. Nor may it compare one volume's parts with another's SERIES: ids are local per volume. */
static void test_series_check_races(const char *scratch)
{
    char dir[600], part[256], vol[700];
    snprintf(dir, sizeof dir, "%s/damage-series-race", scratch);
    tsdb_sid id;
    OK(tsdb_close(open_two_parts(dir, 0, &id, part, sizeof part)));
    char mf[700];
    snprintf(mf, sizeof mf, "%s/MANIFEST", dir);
    OK(unlink(mf));
    tsdb_test_before_parts(writer_in_between, dir);
    tsdb_options ro = opts();
    ro.read_only = 1;
    tsdb *r = NULL;
    OK(tsdb_open(dir, &ro, &r));
    tsdb_test_before_parts(NULL, NULL);
    if (r) {
        tsdb_sid late;
        int rc;
        OK(tsdb_series_lookup(r, "race.late", &late));
        CHECK(count_rows(r, late, &rc) == PART_ROWS && rc == 0);
        tsdb_close(r);
    }

    /* volumes: the second has more series than the root, each with rows in a part */
    rm_rf(dir);
    snprintf(vol, sizeof vol, "%s-vol", dir);
    rm_rf(vol);
    tsdb_options o = opts();
    o.wal_max_batch_rows = 100;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    int64_t ts[PART_ROWS];
    float v[PART_ROWS];
    for (int i = 0; i < PART_ROWS; i++)
        ts[i] = T0 + i, v[i] = 3;
    OK(tsdb_series(db, "root.only", &id));
    OK(tsdb_insert_series(db, id, ts, v, PART_ROWS));
    for (int s = 0; s < 3; s++) {
        char name[32];
        snprintf(name, sizeof name, "vol.s%d", s);
        OK(tsdb_series_at(db, name, vol, &id));
        OK(tsdb_insert_series(db, id, ts, v, PART_ROWS));
    }
    OK(tsdb_close(db));
    OK(try_open(dir));
    off_t size = file_size(vol, "SERIES");
    size_t rs = rec_size(strlen("vol.s2"));
    uint8_t junk[64] = {0x5a};
    file_pwrite(vol, "SERIES", size - (off_t)rs, junk, rs);
    RC(try_open(dir), TSDB_ERR_CORRUPT);
    printf("  that check reads SERIES again for a reader that raced a writer, and judges each volume by its own\n");
    rm_rf(dir);
    rm_rf(vol);
}

/* A child makes "wal.a" in the root and three series in vol (or "wal.b" in the root when vol is NULL), writes rows
 * of each, and dies without closing: the rows live only in the WAL of their directory. */
static void crash_with_series(const char *dir, const char *vol)
{
    rm_rf(dir);
    if (vol)
        rm_rf(vol);
    pid_t pid = fork();
    if (pid == 0) {
        tsdb_options o = opts();
        tsdb *db;
        tsdb_sid id;
        int64_t ts[WAL_RECROWS];
        float v[WAL_RECROWS];
        for (int i = 0; i < WAL_RECROWS; i++)
            ts[i] = T0 + i * 1000, v[i] = (float)i;
        if (tsdb_open(dir, &o, &db) || tsdb_series(db, "wal.a", &id) || tsdb_insert_series(db, id, ts, v, WAL_RECROWS))
            _exit(1);
        for (int s = 0; s < (vol ? 3 : 1); s++) {
            char name[32];
            snprintf(name, sizeof name, vol ? "vol.s%d" : "wal.b", s);
            if ((vol ? tsdb_series_at(db, name, vol, &id) : tsdb_series(db, name, &id)) ||
                tsdb_insert_series(db, id, ts, v, WAL_RECROWS))
                _exit(1);
        }
        _exit(0); /* no tsdb_close: a crash */
    }
    int status;
    waitpid(pid, &status, 0);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

static void junk_last_series(const char *dir, const char *name)
{
    off_t size = file_size(dir, "SERIES");
    size_t rs = rec_size(strlen(name));
    uint8_t junk[64];
    for (size_t i = 0; i < sizeof junk; i++)
        junk[i] = (uint8_t)(i * 37 + 11);
    file_pwrite(dir, "SERIES", size - (off_t)rs, junk, rs);
}

/* Damage to the last SERIES record looks like a torn append, and the open cut it; a series whose rows were only in
 * the WAL then had them skipped by the replay as rows "of a series never created": acknowledged writes gone without
 * a word. A series' record is synced before its id is handed out, so such rows mean damage: the open fails, every
 * time (the WAL segment stays), and in each volume against that volume's SERIES. */
static void test_series_cut_under_wal(const char *scratch)
{
    char dir[600], vol[700], wal[256];
    snprintf(dir, sizeof dir, "%s/damage-series-wal", scratch);
    snprintf(vol, sizeof vol, "%s-vol", dir);

    crash_with_series(dir, NULL);
    CHECK(rows_of(dir, "wal.b") == WAL_RECROWS); /* undamaged: replayed */

    for (int how = 0; how < 2; how++) {
        crash_with_series(dir, NULL);
        CHECK(find_file(dir, "wal_", ".log", wal, sizeof wal));
        off_t wal_len = file_size(dir, wal);
        if (how == 0)
            junk_last_series(dir, "wal.b");
        else
            cut(dir, "SERIES", file_size(dir, "SERIES") - 3); /* a torn last record */
        RC(try_open(dir), TSDB_ERR_CORRUPT);
        RC(try_open(dir), TSDB_ERR_CORRUPT); /* and again: the segment was neither replayed nor cut */
        CHECK(file_size(dir, wal) == wal_len);
    }

    /* volumes: the volume has more series than the root, all with rows only in its WAL */
    crash_with_series(dir, vol);
    OK(try_open(dir));
    crash_with_series(dir, vol);
    junk_last_series(vol, "vol.s2");
    RC(try_open(dir), TSDB_ERR_CORRUPT);
    printf("  SERIES cut below series with rows only in the WAL: the open fails, in each volume\n");
    rm_rf(dir);
    rm_rf(vol);
}

/* A writer with rows of a new series in its WAL opens between a reader's read of SERIES and its replay of the WAL
 * (a reader opens without MANIFEST only before the first writer published one): the reader reads SERIES again. */
static tsdb *wal_writer;
static void wal_writer_in_between(void *arg)
{
    const char *dir = arg;
    tsdb_test_before_parts(NULL, NULL);
    tsdb_options o = opts();
    tsdb_sid id;
    int64_t ts[WAL_RECROWS];
    float v[WAL_RECROWS];
    for (int i = 0; i < WAL_RECROWS; i++)
        ts[i] = T0 + i * 1000, v[i] = 4;
    OK(tsdb_open(dir, &o, &wal_writer));
    OK(tsdb_series(wal_writer, "race.wal", &id));
    OK(tsdb_insert_series(wal_writer, id, ts, v, WAL_RECROWS)); /* kept open: the rows stay in its WAL */
}

struct series_maker {
    tsdb *w;
    int n;
    atomic_int done;
};

static void *series_maker_main(void *arg)
{
    struct series_maker *sm = arg;
    int64_t ts[3] = {T0, T0 + 1000, T0 + 2000};
    float v[3] = {1, 2, 3};
    for (int s = 0; s < sm->n; s++) {
        char name[32];
        tsdb_sid id;
        snprintf(name, sizeof name, "race.f%d", s);
        OK(tsdb_series(sm->w, name, &id));
        OK(tsdb_insert_series(sm->w, id, ts, v, 3));
    }
    atomic_store(&sm->done, 1);
    return NULL;
}

/* Followers take WAL rows of a series the writer created a moment ago: they read SERIES again before they judge,
 * and never report damage for it. One whose SERIES stays short of a WAL row's series keeps the rows it showed,
 * shows none of that record or later ones, and reports the damage once it outlasts the patience for NFS; once
 * SERIES is whole again, the rows appear. */
static void test_series_wal_follower(const char *scratch)
{
    char dir[600];
    snprintf(dir, sizeof dir, "%s/damage-series-walfollow", scratch);

    /* a reader without MANIFEST that raced a writer */
    crash_with_series(dir, NULL);
    OK(try_open(dir)); /* the rows to parts */
    char mf[700];
    snprintf(mf, sizeof mf, "%s/MANIFEST", dir);
    OK(unlink(mf));
    tsdb_test_before_parts(wal_writer_in_between, dir);
    tsdb_options ro = opts();
    ro.read_only = 1;
    tsdb *r = NULL;
    OK(tsdb_open(dir, &ro, &r));
    tsdb_test_before_parts(NULL, NULL);
    if (r) {
        tsdb_sid late;
        int rc;
        OK(tsdb_series_lookup(r, "race.wal", &late));
        CHECK(count_rows(r, late, &rc) == WAL_RECROWS && rc == 0);
        tsdb_close(r);
    }
    if (wal_writer)
        OK(tsdb_close(wal_writer));
    wal_writer = NULL;

    /* followers that refresh and open while the writer creates series */
    rm_rf(dir);
    tsdb_options o = opts();
    tsdb_options fo = opts();
    fo.follow = 1;
    fo.read_only = 1;
    fo.refresh_ms = 20;
    struct series_maker sm = {.n = 3000};
    tsdb *f;
    OK(tsdb_open(dir, &o, &sm.w));
    OK(tsdb_open(dir, &fo, &f));
    pthread_t th;
    CHECK(pthread_create(&th, NULL, series_maker_main, &sm) == 0);
    int refreshes = 0, opens = 0;
    while (!atomic_load(&sm.done)) {
        OK(tsdb_refresh(f));
        refreshes++;
        if (refreshes % 4 == 0) {
            tsdb *f2;
            OK(tsdb_open(dir, &fo, &f2));
            OK(tsdb_refresh(f2));
            OK(tsdb_close(f2));
            opens++;
        }
    }
    pthread_join(th, NULL);
    OK(tsdb_refresh(f));
    tsdb_stats st;
    OK(tsdb_get_stats(f, &st));
    CHECK(st.follow_errors == 0);
    for (int s = 0; s < sm.n; s++) {
        char name[32];
        tsdb_sid id;
        int rc;
        snprintf(name, sizeof name, "race.f%d", s);
        OK(tsdb_series_lookup(f, name, &id));
        CHECK(count_rows(f, id, &rc) == 3);
    }
    OK(tsdb_close(f));
    printf("  followers: %d refreshes and %d opens while %d series were created, no damage reported\n", refreshes,
           opens, sm.n);

    /* SERIES short of a series with rows in the WAL */
    tsdb_sid a, b;
    int64_t ts[WAL_RECROWS];
    float v[WAL_RECROWS];
    for (int i = 0; i < WAL_RECROWS; i++)
        ts[i] = T0 + i * 1000, v[i] = 5;
    OK(tsdb_series(sm.w, "follow.a", &a));
    OK(tsdb_insert_series(sm.w, a, ts, v, WAL_RECROWS));
    OK(tsdb_open(dir, &fo, &f));
    OK(tsdb_refresh(f));
    off_t size = file_size(dir, "SERIES");
    OK(tsdb_series(sm.w, "follow.b", &b));
    size_t rs = rec_size(strlen("follow.b"));
    uint8_t good[64];
    file_pread(dir, "SERIES", size, good, rs);
    junk_last_series(dir, "follow.b");
    OK(tsdb_insert_series(sm.w, b, ts, v, WAL_RECROWS));
    OK(tsdb_insert_series(sm.w, a, (int64_t[]){T0 + 100000}, (float[]){6}, 1)); /* after it: not shown either */
    int rc = tsdb_refresh(f);
    CHECK(rc == 0); /* within the patience */
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    double waited = 0;
    while ((rc = tsdb_refresh(f)) == 0 && waited < 10) {
        usleep(50000);
        clock_gettime(CLOCK_MONOTONIC, &t1);
        waited = (double)(t1.tv_sec - t0.tv_sec) + (double)(t1.tv_nsec - t0.tv_nsec) / 1e9;
    }
    RC(rc, TSDB_ERR_CORRUPT);
    CHECK(waited >= 1.5);
    OK(tsdb_get_stats(f, &st));
    CHECK(st.follow_errors > 0);
    tsdb_sid fb;
    CHECK(count_rows(f, a, &rc) == WAL_RECROWS); /* what it showed stays */
    CHECK(tsdb_series_lookup(f, "follow.b", &fb) == TSDB_ERR_NOTFOUND);
    file_pwrite(dir, "SERIES", size, good, rs); /* whole again */
    OK(tsdb_refresh(f));
    OK(tsdb_series_lookup(f, "follow.b", &fb));
    CHECK(count_rows(f, fb, &rc) == WAL_RECROWS);
    CHECK(count_rows(f, a, &rc) == WAL_RECROWS + 1);
    printf("  follower: SERIES short of a WAL row's series reported after %.1f s, the rows kept for later\n", waited);
    OK(tsdb_close(f));
    OK(tsdb_close(sm.w));
    rm_rf(dir);
}

/* ------------------------------------------------------------- shifted records */

/* A valid record at another offset (stale data, a misdirected write) is
 * damage, not a record: the checks are seeded with the offset. */
static void test_shifted(const char *scratch)
{
    char dir[600], other[600], wal[256];
    snprintf(dir, sizeof dir, "%s/damage-shift", scratch);
    snprintf(other, sizeof other, "%s/damage-shift-other", scratch);

    /* WAL: record 5 copied over record 2 would replay rows twice and lose record 2's */
    crash_with_wal(dir, wal, sizeof wal);
    uint8_t rec[WAL_RECLEN];
    file_pread(dir, wal, 5 * WAL_RECLEN, rec, sizeof rec);
    file_pwrite(dir, wal, 2 * WAL_RECLEN, rec, sizeof rec);
    RC(try_open(dir), TSDB_ERR_CORRUPT);

    /* SERIES: a record of another dictionary, valid at its own offset, copied to
     * another one (names of one length, so the records line up) */
    static const char *const names[] = {"eq.aaaa", "eq.bbbb", "eq.cccc", "eq.dddd"};
    static const char *const others[] = {"eq.aaaa", "eq.qqqq"};
    size_t rs = rec_size(strlen(names[0]));
    make_series(other, others, 2);
    make_series(dir, names, 4);
    uint8_t srec[64];
    file_pread(other, "SERIES", (off_t)rs, srec, rs);
    file_pwrite(dir, "SERIES", 2 * (off_t)rs, srec, rs);
    RC(try_open(dir), TSDB_ERR_CORRUPT);
    printf("  WAL and SERIES: a valid record moved to another offset fails the open\n");
    rm_rf(dir);
    rm_rf(other);
}

/* ------------------------------------------------------------- read errors */

/* A read error, or a file that reads shorter than its size, is never taken
 * for the end of the file: the open (or the follower's refresh) fails. */
static void test_read_errors(const char *scratch)
{
    char dir[600], wal[256];
    snprintf(dir, sizeof dir, "%s/damage-eio", scratch);

    crash_with_wal(dir, wal, sizeof wal);
    for (int mode = FAULT_EIO; mode <= FAULT_SHORT; mode++) {
        arm_read_fault(mode, "wal_");
        RC(try_open(dir), TSDB_ERR_IO);
        atomic_store(&fault_mode, FAULT_NONE);
        CHECK(atomic_load(&fault_fired) > 0);
        CHECK(file_size(dir, wal) == WAL_RECS * WAL_RECLEN); /* nothing cut */
    }
    CHECK(rows_of(dir, "wal.series") == WAL_RECS * WAL_RECROWS);

    static const char *const names[] = {"pump.inlet.pressure", "pump.outlet.pressure", "pump.speed"};
    make_series(dir, names, 3);
    off_t size = file_size(dir, "SERIES");
    for (int mode = FAULT_EIO; mode <= FAULT_SHORT; mode++) {
        arm_read_fault(mode, "SERIES");
        RC(try_open(dir), TSDB_ERR_IO);
        atomic_store(&fault_mode, FAULT_NONE);
        CHECK(atomic_load(&fault_fired) > 0);
        CHECK(file_size(dir, "SERIES") == size);
    }
    check_names(dir, names, 3, 3);

    /* a follower that meets rows of a new series and cannot read SERIES: the
     * refresh fails, and the rows come once it can (not dropped meanwhile) */
    rm_rf(dir);
    tsdb_options o = opts();
    tsdb *w, *f = NULL;
    tsdb_sid a, b;
    OK(tsdb_open(dir, &o, &w));
    OK(tsdb_series(w, "eio.a", &a));
    OK(tsdb_append(w, a, T0, 1.0f));
    tsdb_options ro = opts();
    ro.read_only = 1; /* no polling thread: refreshes happen here */
    OK(tsdb_open(dir, &ro, &f));
    if (f) {
        OK(tsdb_refresh(f));
        OK(tsdb_series(w, "eio.b", &b));
        OK(tsdb_append(w, b, T0, 2.0f));
        arm_read_fault(FAULT_EIO, "SERIES");
        RC(tsdb_refresh(f), TSDB_ERR_IO);
        atomic_store(&fault_mode, FAULT_NONE);
        CHECK(atomic_load(&fault_fired) > 0);
        OK(tsdb_refresh(f));
        tsdb_sid fb;
        int rc;
        OK(tsdb_series_lookup(f, "eio.b", &fb));
        CHECK(fb == b && count_rows(f, b, &rc) == 1);
        OK(tsdb_close(f));
    }
    OK(tsdb_close(w));
    printf("  read errors and short reads of the WAL and SERIES fail the open, and a follower's refresh\n");
    rm_rf(dir);
}

/* ------------------------------------------------------------- codecs */

/* With verify_checksums off a damaged granule reaches the decoders: they may
 * return wrong values, but must not overflow or read out of bounds (make asan
 * runs this with UBSan stopping at the first report). */
size_t decimal_bp_encode(const float *in, size_t n, uint8_t *out);
int decimal_bp_decode(const uint8_t *in, size_t len, size_t n, float *out);
size_t decimal_encode(const float *in, size_t n, uint8_t *out);
int decimal_decode(const uint8_t *in, size_t len, size_t n, float *out);

static uint64_t rnd_state = 88172645463325252ull;
static uint64_t rnd(void)
{
    rnd_state ^= rnd_state << 13;
    rnd_state ^= rnd_state >> 7;
    rnd_state ^= rnd_state << 17;
    return rnd_state;
}

static void test_codecs(void)
{
    /* the decimal codecs' first integer near INT64_MAX, then rising deltas */
    enum { N = 256 };
    float in[N], out[N];
    for (int i = 0; i < N; i++)
        in[i] = (float)i * 0.5f;
    static uint8_t enc[2 * N * 4 + 256];
    const int64_t big = INT64_MAX - 3;
    size_t len = decimal_bp_encode(in, N, enc);
    CHECK(len > 10);
    if (len > 10) {
        memcpy(enc + 2, &big, 8);
        decimal_bp_decode(enc, len, N, out); /* any result but UB */
    }
    len = decimal_encode(in, N, enc);
    CHECK(len > 10);
    if (len > 10) {
        memcpy(enc + 2, &big, 8);
        decimal_decode(enc, len, N, out);
    }

    /* flipped bits in encoded blocks of every type and codec */
    codec_ctx *c = codec_new();
    long blocks = 0, errors = 0;
    static const size_t ns[] = {7, 100, 1000};
    for (int ni = 0; ni < 3; ni++) {
        size_t n = ns[ni], cap = block_bound(n) + 64;
        int64_t *ts = malloc(n * 8), *ts2 = malloc(n * 8);
        uint8_t *v = malloc(n * 8), *v2 = malloc(n * 8), *e = malloc(cap), *d = malloc(cap);
        for (int type = 0; type <= 4; type++)
            for (int vc = 0; vc <= 2; vc++)
                for (int level = 0; level <= 3; level += 3) {
                    int64_t t = T0;
                    for (size_t i = 0; i < n; i++) {
                        t += 1000 + (int64_t)(rnd() % 3);
                        ts[i] = t;
                        if (type == TSDB_FLOAT32) {
                            float x = (float)(i % 50) * 0.5f;
                            memcpy(v + 4 * i, &x, 4);
                        } else if (type == TSDB_FLOAT64) {
                            double x = (double)(i % 50) * 0.25;
                            memcpy(v + 8 * i, &x, 8);
                        } else if (type == TSDB_INT64) {
                            int64_t x = (int64_t)(rnd() % 1000);
                            memcpy(v + 8 * i, &x, 8);
                        } else {
                            uint32_t x = (uint32_t)(i / 10 % 2);
                            memcpy(v + 4 * i, &x, 4);
                        }
                    }
                    size_t lt = block_encode_ts(c, level, ts, n, e);
                    size_t lv = block_encode_vals(c, level, vc, type, v, n, e + lt);
                    CHECK(lt > 0 && lv > 0);
                    if (!lt || !lv)
                        continue;
                    for (int it = 0; it < 300; it++) {
                        memcpy(d, e, lt + lv);
                        for (int k = 1 + (int)(rnd() % 3); k > 0; k--)
                            d[rnd() % (lt + lv)] ^= (uint8_t)(1u << (rnd() % 8));
                        if (rnd() % 8 == 0)
                            d[rnd() % (lt + lv)] = (uint8_t)rnd();
                        /* exact-size copies so that ASan sees overreads; a part has >= 16 bytes after any block */
                        uint8_t *bt = calloc(1, lt + 16), *bv = calloc(1, lv + 16);
                        memcpy(bt, d, lt);
                        memcpy(bv, d + lt, lv);
                        int r1 = block_decode_ts(c, bt, lt, n, ts2);
                        int r2 = block_decode_vals(c, type, bv, lv, n, v2);
                        blocks++;
                        errors += r1 || r2;
                        free(bt);
                        free(bv);
                    }
                }
        free(ts);
        free(ts2);
        free(v);
        free(v2);
        free(e);
        free(d);
    }
    codec_free(c);
    printf("  codecs: %ld damaged blocks decoded without UB (%ld rejected)\n", blocks, errors);
}

/* Counts the entries of dir (but . and ..), to see that a refused open made nothing. */
static int entries(const char *dir)
{
    DIR *d = opendir(dir);
    int n = 0;
    for (struct dirent *e; d && (e = readdir(d));)
        n += strcmp(e->d_name, ".") && strcmp(e->d_name, "..");
    if (d)
        closedir(d);
    return n;
}

/* A database of another disk format (7: series ids from a CATALOG, no volume numbers in META) is refused at open,
 * by writers and readers, with a message that names both versions, and nothing in it is touched. */
static void test_format_version(const char *scratch)
{
    char dir[600];
    snprintf(dir, sizeof dir, "%s/damage-format", scratch);
    rm_rf(dir);
    tsdb_options o = opts();
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid id;
    OK(tsdb_series(db, "s", &id));
    OK(tsdb_append(db, id, 1000, 1.0f));
    OK(tsdb_close(db));
    char meta[512], text[512];
    off_t len = file_size(dir, "META");
    CHECK(len > 0 && len < (off_t)sizeof meta);
    file_pread(dir, "META", 0, meta, (size_t)len);
    meta[len] = '\0';
    char *v = strstr(meta, "version=");
    CHECK(v && atoi(v + 8) == FORMAT_VERSION && FORMAT_VERSION == 8);
    if (!v)
        return;
    v[8] = '7';
    file_pwrite(dir, "META", 0, meta, (size_t)len);
    int before = entries(dir);
    for (int ro = 0; ro < 2; ro++) {
        char log[700];
        snprintf(log, sizeof log, "%s.stderr", dir);
        fflush(stderr);
        int saved = dup(2), lfd = open(log, O_RDWR | O_CREAT | O_TRUNC, 0644);
        dup2(lfd, 2);
        o.read_only = ro;
        db = NULL;
        int rc = tsdb_open(dir, &o, &db);
        fflush(stderr);
        dup2(saved, 2);
        close(saved);
        char got[1024] = "";
        ssize_t n = pread(lfd, got, sizeof got - 1, 0);
        got[n > 0 ? n : 0] = '\0';
        close(lfd);
        unlink(log);
        RC(rc, TSDB_ERR_CORRUPT);
        if (db)
            tsdb_close(db);
        CHECK(strstr(got, "disk format version 7;") && strstr(got, "reads version 8 only"));
    }
    CHECK(entries(dir) == before);
    file_pread(dir, "META", 0, text, (size_t)len);
    CHECK(memcmp(text, meta, (size_t)len) == 0 && file_size(dir, "META") == len);
    rm_rf(dir);
    printf("  a format-7 database is refused at open, naming both versions, untouched\n");
}

int main(int argc, char **argv)
{
    const char *scratch = argc > 1 ? argv[1] : "/tmp";
    char base[512];
    snprintf(base, sizeof base, "%s/tsdb-damage-%d", scratch, (int)getpid());
    rm_rf(base);
    mkdir(base, 0755);
    test_series(base);
    test_wal(base);
    test_wal_follower(base);
    test_shifted(base);
    test_index_after_open(base);
    test_series_cut_under_parts(base);
    test_series_check_races(base);
    test_series_cut_under_wal(base);
    test_series_wal_follower(base);
    test_misread_tail(base);
    test_read_errors(base);
    test_parts(base);
    test_part_follower(base);
    test_manifest(base);
    test_codecs();
    test_format_version(base);
    rm_rf(base);
    if (failures) {
        fprintf(stderr, "%d damage checks FAILED\n", failures);
        return 1;
    }
    printf("damage tests passed\n");
    return 0;
}
