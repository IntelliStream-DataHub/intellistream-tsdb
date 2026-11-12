/* SPDX-License-Identifier: AGPL-3.0-only */
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
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "portable.h"

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
        char target[PATH_MAX];
        ssize_t l = fd_path(fd, target, sizeof target);
        if (l > 0) {
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
#ifdef __APPLE__
    return REAL(pread)(fd, buf, n, off);
#else
    return syscall(SYS_pread64, fd, buf, n, off);
#endif
}

#ifndef __APPLE__
/* the fortified variant, which _FORTIFY_SOURCE builds call where the buffer's size is known */
ssize_t __pread_chk(int fd, void *buf, size_t n, off_t off, size_t buflen);
ssize_t __pread_chk(int fd, void *buf, size_t n, off_t off, size_t buflen)
{
    if (n > buflen)
        abort();
    return pread(fd, buf, n, off);
}
#endif

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
    o.pin_index = verify;
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
 * taken from the run table stays in the index, and with pin_index the index is read from the copy that was
 * checked, so the answers do not change at all. (That copy was verify_checksums' at first; it is its own option
 * now, since it costs memory per granule and the clamps alone keep reads in bounds.) */
static void test_index_after_open(const char *scratch)
{
    char dir[600], part[256];
    snprintf(dir, sizeof dir, "%s/damage-index-live", scratch);
    for (int verify = 0; verify <= 1; verify++) { /* verify: pin_index (and verify_checksums) */
        tsdb_sid id;
        tsdb *db = open_two_parts(dir, verify, &id, part, sizeof part);
        int rc;
        CHECK(count_rows(db, id, &rc) == 2 * PART_ROWS);
        tsdb_volume_stats vs;
        OK(tsdb_get_volume_stats(db, 0, &vs));
        CHECK(verify ? vs.pinned_bytes >= 8 * vs.granules : vs.pinned_bytes == 0);
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
    /* verify_checksums alone no longer pins the index: that costs memory per granule, and is pin_index's */
    tsdb_options o = opts();
    o.verify_checksums = 1;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_volume_stats vs;
    OK(tsdb_get_volume_stats(db, 0, &vs));
    CHECK(vs.pinned_bytes == 0 && vs.granules > 0);
    tsdb_close(db);
    printf("  a part's index damaged after the open: no crash; with pin_index the answers stay right\n");
    rm_rf(dir);
}

/* A merge copies a merged source's granules verbatim, statistics included, from the index in the mapping. Damage
 * that reached the index after the open (without pin_index) was written into the new part under a fresh hash: wrong
 * aggregates for good, which no later check could see. The merge checks each source's index hash first now, and
 * fails rather than copy; the damaged part stays, and the next open refuses it. */
static void test_merge_index_damage(const char *scratch)
{
    char dir[600], part[256];
    snprintf(dir, sizeof dir, "%s/damage-merge-index", scratch);
    rm_rf(dir);
    tsdb_options o = opts();
    o.wal_max_batch_rows = 100; /* straight to parts */
    tsdb *db;
    tsdb_sid id;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series(db, "merge.a", &id));
    static int64_t ts[PART_ROWS];
    static float v[PART_ROWS];
    for (int k = 0; k < 3; k++) {
        for (int i = 0; i < PART_ROWS; i++)
            ts[i] = T0 + (k * PART_ROWS + i) * 1000, v[i] = (float)(i % 37);
        OK(tsdb_insert_series(db, id, ts, v, PART_ROWS));
        if (k == 1)
            OK(tsdb_optimize(db)); /* a merged part: its granules are copied verbatim by the next merge */
    }
    CHECK(find_file(dir, "p", "_1.part", part, sizeof part));
    part_footer ft = read_footer(dir, part);
    flip(dir, part, (off_t)(ft.index_off + ft.index_len - 8)); /* the last granule entry's statistics */
    RC(tsdb_optimize(db), TSDB_ERR_CORRUPT);
    char again[256];
    CHECK(find_file(dir, part, "", again, sizeof again)); /* still there, not replaced by a laundered copy */
    tsdb_close(db);
    printf("  a merge does not copy statistics from an index damaged after the open\n");
    rm_rf(dir);
}

/* A granule's count changed in the mapping between two loads of it (pin_index off, the default), found by the 0.9.0
 * review (secrel F1): a raw query sized its buffers from the counts of its window, then decoded the count it loaded
 * again into them, and a merge bounded its loops by one load and its decoded stamps by another: a heap overflow
 * from file bytes, where tsdb.h promises wrong answers at most. Every decode is bounded by the buffer it writes now,
 * and a merge refuses a count that changed. The hook flips the top bit of an entry's count in the file exactly
 * between the loads (IX_*); the mapping shows it at once, as it shows a misdirected write. */
static atomic_int ix_at = -1, ix_fired, ix_flip_it = 1;
static off_t ix_off; /* where the bit is: the file, the offset, which bit */
static char ix_name[96];
static uint8_t ix_mask;

static void ix_hook(const part *p, uint32_t i, int at)
{
    if (at != atomic_load(&ix_at) || atomic_exchange(&ix_fired, 1))
        return;
    const gix_field *c = &p->ix.f[GF_COUNT];
    CHECK(c->width > 0); /* counts that differ: the field has bits */
    unsigned bit = c->bit + c->width - 1u;
    ix_off = (off_t)(p->ix.e - p->map) + (off_t)i * p->ix.stride + c->byte + bit / 8;
    ix_mask = (uint8_t)(1u << (bit % 8));
    snprintf(ix_name, sizeof ix_name, "%s", p->name);
    if (!c->width || !atomic_load(&ix_flip_it))
        return;
    uint32_t before = gx_count(p, i);
    int fd = openat(p->dirfd, p->name, O_RDWR | O_CLOEXEC);
    uint8_t b = 0;
    CHECK(fd >= 0 && pread(fd, &b, 1, ix_off) == 1);
    b ^= ix_mask;
    CHECK(fd >= 0 && pwrite(fd, &b, 1, ix_off) == 1);
    if (fd >= 0)
        close(fd);
    CHECK(gx_count(p, i) != before); /* the readers see it */
}

/* ix.a: one granule of 10 rows, ix.b: one of 40, in one part; tomb: a delete touches ix.a's granule, so the query
 * decodes it through decode_visible rather than a source (src_fill). ix.a's count is the smaller: its top bit is 0,
 * and the flip raises it above the buffer sized for 10 rows. */
static tsdb *ix_query_db(const char *dir, bool tomb, tsdb_sid *a)
{
    rm_rf(dir);
    tsdb_options o = opts();
    o.partition_classes = 0;
    o.wal_max_batch_rows = 1; /* straight to a part */
    tsdb *db;
    tsdb_sid b;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series(db, "ix.a", a));
    OK(tsdb_series(db, "ix.b", &b));
    tsdb_sid ids[50];
    int64_t ts[50];
    float v[50];
    for (int i = 0; i < 50; i++)
        ids[i] = i < 10 ? *a : b, ts[i] = T0 + (i < 10 ? i : i - 10) * 1000, v[i] = 1.0f;
    OK(tsdb_insert(db, ids, ts, v, 50));
    if (tomb)
        OK(tsdb_delete_range(db, *a, T0 + 5000, T0 + 5001));
    return db;
}

/* The merge path: a merged part holds ix.a (60 rows, one granule across an hour edge, written with hour cuts off)
 * and ix.c (64 rows); its count and longest gap leave the rows of each hour open, so a merge with hour cuts on
 * decodes its stamps (pw_copy_fits) and copies it (pw_copy_granule) with the hour count taken from them. */
static void ix_merge(const char *dir, int at, bool flip)
{
    rm_rf(dir);
    unsigned hc = atomic_load(&hour_cut_min);
    atomic_store(&hour_cut_min, 0);
    tsdb_options o = opts();
    o.partition_classes = 0;
    o.wal_max_batch_rows = 1;
    tsdb *db;
    tsdb_sid a, b, c;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series(db, "ix.a", &a));
    OK(tsdb_series(db, "ix.b", &b));
    OK(tsdb_series(db, "ix.c", &c));
    int64_t e = (T0 / 3600000 + 10) * 3600000; /* an hour edge inside a day */
    int64_t ta[60], tc[64];
    float va[60], vc[64];
    for (int i = 0; i < 60; i++) /* 15 rows in the hour before e, 45 after it, 30 min 20 s apart */
        ta[i] = i < 15 ? e - 1800000 + i * 20000 : e + 300000 + (i - 15) * 20000, va[i] = (float)i;
    for (int i = 0; i < 64; i++)
        tc[i] = e + 1800000 + i * 1000, vc[i] = 1.0f;
    OK(tsdb_insert_series(db, a, ta, va, 30));
    OK(tsdb_insert_series(db, c, tc, vc, 64));
    OK(tsdb_insert_series(db, a, ta + 30, va + 30, 30));
    OK(tsdb_optimize(db));
    OK(tsdb_insert_series(db, b, tc, vc, 10));
    atomic_store(&hour_cut_min, 20);
    atomic_store(&ix_flip_it, flip);
    atomic_store(&ix_fired, 0);
    atomic_store(&ix_at, at);
    RC(tsdb_optimize(db), flip ? TSDB_ERR_CORRUPT : 0);
    CHECK(atomic_load(&ix_fired));
    atomic_store(&ix_at, -1);
    atomic_store(&ix_flip_it, 1);
    atomic_store(&hour_cut_min, hc);
    tsdb_close(db);
}

static atomic_int ix_stop;
static uint8_t *ix_byte;

static void *ix_flipper(void *arg)
{
    (void)arg;
    while (!atomic_load_explicit(&ix_stop, memory_order_relaxed))
        *(volatile uint8_t *)ix_byte ^= ix_mask;
    return NULL;
}

static void test_index_count_changes(const char *scratch)
{
    char dir[600];
    snprintf(dir, sizeof dir, "%s/damage-index-count", scratch);
    atomic_store(&index_reload_hook, ix_hook);
    for (int tomb = 0; tomb <= 1; tomb++) {
        tsdb_sid a;
        tsdb *db = ix_query_db(dir, tomb, &a);
        int rc;
        CHECK(count_rows(db, a, &rc) == 10 - tomb && rc == 0);
        atomic_store(&ix_fired, 0);
        atomic_store(&ix_at, tomb ? IX_VISIBLE : IX_SRC);
        count_rows(db, a, &rc);
        CHECK(atomic_load(&ix_fired));
        RC(rc, TSDB_ERR_CORRUPT);
        atomic_store(&ix_at, -1);
        tsdb_close(db);
    }
    ix_merge(dir, IX_FITS, false); /* the path is taken, and without a change the merge goes through */
    ix_merge(dir, IX_COPY, false);
    ix_merge(dir, IX_FITS, true);
    ix_merge(dir, IX_COPY, true);

    /* and racing: a thread toggles the bit while queries run (what ASan caught in seconds before) */
    for (int tomb = 0; tomb <= 1; tomb++) {
        tsdb_sid a;
        tsdb *db = ix_query_db(dir, tomb, &a);
        atomic_store(&ix_flip_it, 0); /* the hook only finds the bit */
        atomic_store(&ix_fired, 0);
        atomic_store(&ix_at, tomb ? IX_VISIBLE : IX_SRC);
        int rc;
        count_rows(db, a, &rc);
        atomic_store(&ix_at, -1);
        atomic_store(&ix_flip_it, 1);
        CHECK(atomic_load(&ix_fired));
        char p[900];
        path(p, sizeof p, dir, ix_name);
        int fd = open(p, O_RDWR | O_CLOEXEC);
        off_t size = fd >= 0 ? lseek(fd, 0, SEEK_END) : 0;
        uint8_t *m = fd >= 0 ? mmap(NULL, (size_t)size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0) : MAP_FAILED;
        CHECK(m != MAP_FAILED && ix_off < size);
        if (m != MAP_FAILED && ix_off < size) {
            ix_byte = m + ix_off;
            atomic_store(&ix_stop, 0);
            pthread_t t;
            pthread_create(&t, NULL, ix_flipper, NULL);
            long wrong = 0; /* answers and TSDB_ERR_CORRUPT are both right here; anything else is not */
            for (int i = 0; i < 20000; i++) {
                count_rows(db, a, &rc);
                wrong += rc != 0 && rc != TSDB_ERR_CORRUPT;
            }
            atomic_store(&ix_stop, 1);
            pthread_join(t, NULL);
            CHECK(wrong == 0);
            munmap(m, (size_t)size);
        }
        if (fd >= 0)
            close(fd);
        tsdb_close(db);
    }
    atomic_store(&index_reload_hook, NULL);
    printf("  a granule count that changes in the mapping between two loads is refused, never decoded past a buffer\n");
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

static void crash_with_series(const char *dir, const char *vol);

/* Every file of dir, by name and content (order-free): equal before and after means nothing was changed. */
static uint64_t dir_digest(const char *dir)
{
    DIR *d = opendir(dir);
    uint64_t sum = 0;
    struct dirent *e;
    while (d && (e = readdir(d)) != NULL) {
        char p[1024];
        struct stat st;
        path(p, sizeof p, dir, e->d_name);
        if (stat(p, &st) != 0 || !S_ISREG(st.st_mode) || strcmp(e->d_name, "LOCK") == 0)
            continue;
        uint8_t *b = malloc((size_t)st.st_size + 1);
        int fd = open(p, O_RDONLY);
        CHECK(b && fd >= 0 && pread(fd, b, (size_t)st.st_size, 0) == (ssize_t)st.st_size);
        sum += xxh64(e->d_name, strlen(e->d_name), 0) * 31 + xxh64(b, (size_t)st.st_size, (uint64_t)st.st_size);
        if (fd >= 0)
            close(fd);
        free(b);
    }
    if (d)
        closedir(d);
    return sum;
}

static void append_junk(const char *dir, const char *name, size_t n)
{
    char p[1024];
    path(p, sizeof p, dir, name);
    int fd = open(p, O_WRONLY | O_CREAT | O_APPEND, 0644);
    uint8_t z[64] = {0};
    CHECK(fd >= 0 && n <= sizeof z && write(fd, z, n) == (ssize_t)n);
    if (fd >= 0)
        close(fd);
}

/* The open cut a "torn" SERIES tail (and those of STATES and CLASSES) before the checks that then found the cut
 * record was acknowledged and refused the open: the only copy of the damaged record was gone, so one flipped bit,
 * which the operator could have flipped back, became a database that never opens again. The cuts wait for every
 * check now: a refused open leaves each file byte for byte as it was, and once SERIES is repaired the open works. */
static void test_refused_open_keeps_files(const char *scratch)
{
    char dir[600], wal[256];
    snprintf(dir, sizeof dir, "%s/damage-refused-keeps", scratch);
    for (int in_wal = 0; in_wal <= 1; in_wal++) {
        const char *last = in_wal ? "wal.b" : "sensor_3";
        if (in_wal) {
            crash_with_series(dir, NULL); /* wal.a and wal.b, with rows only in the WAL */
            CHECK(find_file(dir, "wal_", ".log", wal, sizeof wal));
            append_junk(dir, wal, 13); /* a torn append after the rows */
        } else {
            rm_rf(dir);
            tsdb_options o = opts();
            tsdb *db;
            OK(tsdb_open(dir, &o, &db));
            tsdb_sid sid;
            uint32_t code;
            OK(tsdb_series_create(db, "machine.mode", TSDB_STATE, NULL, &sid)); /* STATES gets a record */
            OK(tsdb_state(db, sid, "running", &code));
            for (int s = 0; s < 4; s++) {
                char name[32];
                tsdb_sid id;
                snprintf(name, sizeof name, "sensor_%d", s);
                OK(tsdb_series(db, name, &id));
                int64_t ts[100];
                float v[100];
                for (int i = 0; i < 100; i++)
                    ts[i] = T0 + i * 1000, v[i] = (float)i;
                OK(tsdb_insert_series(db, id, ts, v, 100));
            }
            OK(tsdb_flush(db));
            OK(tsdb_close(db));
        }
        append_junk(dir, "STATES", 5);
        append_junk(dir, "CLASSES", 10);
        off_t size = file_size(dir, "SERIES");
        off_t at = size - (off_t)rec_size(strlen(last)) + 8; /* the last record's name */
        flip(dir, "SERIES", at);
        uint64_t before = dir_digest(dir);
        RC(try_open(dir), TSDB_ERR_CORRUPT);
        CHECK(file_size(dir, "SERIES") == size);
        CHECK(dir_digest(dir) == before);
        flip(dir, "SERIES", at); /* the repair: possible because the record is still there */
        OK(try_open(dir));
        CHECK(rows_of(dir, last) == (in_wal ? WAL_RECROWS : 100));
    }
    printf("  a refused open changes no file: SERIES repaired afterwards, the open works\n");
    rm_rf(dir);
}

static int open_reader(const char *dir, int follow, tsdb **out)
{
    tsdb_options o = opts();
    o.read_only = 1;
    o.follow = follow;
    return tsdb_open(dir, &o, out);
}

/* Four series with 100 rows each, straight into parts, in dir (or vol, a volume) by a writer left open. */
static tsdb *writer_with_parts(const char *dir, const char *vol, int first, int n)
{
    tsdb_options o = opts();
    o.wal_max_batch_rows = 10; /* inserts go straight to parts */
    o.fsync = 1;               /* a missing record is damage only after a writer that synced it */
    tsdb *db = NULL;
    OK(tsdb_open(dir, &o, &db));
    for (int s = first; db && s < first + n; s++) {
        char name[32];
        tsdb_sid id;
        snprintf(name, sizeof name, "sensor_%d", s);
        OK(vol ? tsdb_series_at(db, name, vol, &id) : tsdb_series(db, name, &id));
        int64_t ts[100];
        float v[100];
        for (int i = 0; i < 100; i++)
            ts[i] = T0 + i * 1000, v[i] = (float)i;
        OK(tsdb_insert_series(db, id, ts, v, 100));
    }
    return db;
}

/* The follower gap: a reader or follower with MANIFEST listed parts naming a series its SERIES lacks (the last
 * record damaged at rest while the writer ran) and served the series as never made: open fine, lookup NOTFOUND, no
 * error and no log line, while the writer kept serving it. Now the part is "broken" like one that fails its
 * checksum: the open waits out the patience and fails, a running follower keeps its previous view and its refresh
 * fails, and both recover once SERIES is repaired. */
static void test_series_gap_follower(const char *scratch)
{
    char dir[600], vol[700];
    snprintf(dir, sizeof dir, "%s/damage-series-gap", scratch);
    snprintf(vol, sizeof vol, "%s-vol", dir);
    rm_rf(dir);
    tsdb *w = writer_with_parts(dir, NULL, 0, 4), *f;
    off_t at = file_size(dir, "SERIES") - (off_t)rec_size(strlen("sensor_3")) + 8;
    flip(dir, "SERIES", at);
    for (int follow = 0; follow <= 1; follow++) {
        int rc = open_reader(dir, follow, &f);
        RC(rc, TSDB_ERR_CORRUPT);
        if (rc == 0)
            tsdb_close(f);
    }
    flip(dir, "SERIES", at);
    tsdb_sid id;
    OK(open_reader(dir, 1, &f));
    OK(tsdb_series_lookup(f, "sensor_3", &id));

    /* a running follower: the damage reaches SERIES between the record's append and the part that uses it */
    tsdb_sid id4;
    OK(tsdb_series(w, "sensor_4", &id4));
    at = file_size(dir, "SERIES") - (off_t)rec_size(strlen("sensor_4")) + 8;
    flip(dir, "SERIES", at);
    int64_t ts[100];
    float v[100];
    for (int i = 0; i < 100; i++)
        ts[i] = T0 + i * 1000, v[i] = (float)i;
    OK(tsdb_insert_series(w, id4, ts, v, 100));
    int rc = 0;
    for (int i = 0; i < 600 && rc == 0; i++) { /* the patience is 2 s */
        rc = tsdb_refresh(f);
        usleep(10000);
    }
    RC(rc, TSDB_ERR_CORRUPT);
    tsdb_stats st;
    OK(tsdb_get_stats(f, &st));
    CHECK(st.broken_parts >= 1);
    RC(tsdb_series_lookup(f, "sensor_4", &id), TSDB_ERR_NOTFOUND); /* the previous view */
    flip(dir, "SERIES", at);
    OK(tsdb_refresh(f));
    OK(tsdb_series_lookup(f, "sensor_4", &id));
    int qrc;
    CHECK(count_rows(f, id, &qrc) == 100 && qrc == 0);
    tsdb_close(f);
    tsdb_close(w);

    /* a volume is judged by its own SERIES */
    rm_rf(dir);
    rm_rf(vol);
    w = writer_with_parts(dir, vol, 0, 4);
    at = file_size(vol, "SERIES") - (off_t)rec_size(strlen("sensor_3")) + 8;
    flip(vol, "SERIES", at);
    rc = open_reader(dir, 1, &f);
    if (rc == 0) { /* volumes open when first needed: the lookup must not say "no such series" */
        CHECK(tsdb_series_lookup(f, "sensor_3", &id) != TSDB_ERR_NOTFOUND);
        tsdb_close(f);
    } else {
        RC(rc, TSDB_ERR_CORRUPT);
    }
    tsdb_close(w);
    printf("  a follower's new part naming a series SERIES lacks: the open fails, a running follower keeps its view\n");
    rm_rf(dir);
    rm_rf(vol);
}

/* src/volumes.c: cls_observe on every part of the root volume, as the merge thread does on an idle partition */
int tsdb_test_observe(tsdb *t);

/* Whether CLASSES has a record for local series id s. */
static bool classes_has(const char *dir, uint32_t s)
{
    off_t size = file_size(dir, "CLASSES");
    bool found = false;
    for (off_t off = 0; size > 0 && off + 24 <= size; off += 24) {
        uint8_t rec[24];
        file_pread(dir, "CLASSES", off, rec, sizeof rec);
        uint32_t id;
        memcpy(&id, rec + 4, 4);
        found |= id == s;
    }
    return found;
}

/* The measurement of an idle partition's series rates (cls_observe) reads the run table in the mapping, where damage
 * after the open put a series id SERIES lacks: it wrote and synced a class record for that id, which a series made
 * later would inherit. Runs naming a series beyond SERIES, or out of order, are skipped. */
static void test_observe_damaged_runs(const char *scratch)
{
    char dir[600], pname[256];
    snprintf(dir, sizeof dir, "%s/damage-observe", scratch);
    for (int damage = 0; damage <= 1; damage++) {
        rm_rf(dir);
        tsdb_options o = opts();
        o.wal_max_batch_rows = 10; /* straight to parts */
        tsdb *db;
        tsdb_sid id;
        OK(tsdb_open(dir, &o, &db));
        OK(tsdb_series(db, "observe.a", &id));
        static int64_t ts[600];
        static float v[600];
        for (int i = 0; i < 600; i++) /* fast: class 0 */
            ts[i] = T0 + i * 1000, v[i] = 1;
        OK(tsdb_insert_series(db, id, ts, v, 600));
        const int64_t day2 = 19677LL * 86400000; /* two days on: the base partition 19677, key 4 x 19677 */
        for (int i = 0; i < 100; i++) /* then slow, in a class 0 partition: a switch is due */
            ts[i] = day2 + i * 800000, v[i] = 2;
        OK(tsdb_insert_series(db, id, ts, v, 100));
        CHECK(find_file(dir, "p78708_", ".part", pname, sizeof pname));
        if (damage) {
            int dfd = open(dir, O_RDONLY | O_DIRECTORY);
            part *pt = NULL;
            OK(part_open(dfd, pname, false, &pt));
            if (pt && pt->ix.nruns == 1) {
                uint32_t bogus = 1000;
                file_pwrite(dir, pname, (off_t)((const uint8_t *)pt->ix.rser - pt->map), &bogus, 4);
            }
            CHECK(pt && pt->ix.nruns == 1);
            if (pt)
                part_unref(pt);
            close(dfd);
        }
        RC(tsdb_test_observe(db), damage ? TSDB_ERR_CORRUPT : 0); /* the index is checked first now */
        if (damage)
            CHECK(!classes_has(dir, 1000));
        else
            CHECK(classes_has(dir, (uint32_t)TSDB_SID_LOCAL(id))); /* the setup reaches a switch */
        tsdb_close(db);
    }
    printf("  rates measured from a damaged run table write no class record for a series SERIES lacks\n");
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
    OK(tsdb_series(sm.w, "follow.n", &b));
    size_t rs = rec_size(strlen("follow.n"));
    uint8_t good[64];
    file_pread(dir, "SERIES", size, good, rs);
    junk_last_series(dir, "follow.n");
    OK(tsdb_insert_series(sm.w, b, ts, v, WAL_RECROWS));
    /* A writer without fsync leaves the order its files reach a follower on NFS to the writeback, so a missing record
     * is not damage within the usual patience: it was reported as "SERIES is damaged" after 2 s. It gets a minute. */
    int rc = 0;
    for (int i = 0; i < 30 && rc == 0; i++) {
        rc = tsdb_refresh(f);
        usleep(100000);
    }
    OK(rc);
    file_pwrite(dir, "SERIES", size, good, rs);
    OK(tsdb_refresh(f));
    OK(tsdb_series_lookup(f, "follow.n", &b));

    /* a writer with fsync: after the usual patience it is damage */
    OK(tsdb_close(sm.w));
    tsdb_options so = opts();
    so.fsync = 1;
    OK(tsdb_open(dir, &so, &sm.w));
    OK(tsdb_refresh(f));
    size = file_size(dir, "SERIES");
    OK(tsdb_series(sm.w, "follow.b", &b));
    rs = rec_size(strlen("follow.b"));
    file_pread(dir, "SERIES", size, good, rs);
    junk_last_series(dir, "follow.b");
    OK(tsdb_insert_series(sm.w, b, ts, v, WAL_RECROWS));
    OK(tsdb_insert_series(sm.w, a, (int64_t[]){T0 + 100000}, (float[]){6}, 1)); /* after it: not shown either */
    rc = tsdb_refresh(f);
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
static void test_format_version_of(const char *scratch, int old)
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
    CHECK(v && atoi(v + 8) == FORMAT_VERSION && FORMAT_VERSION == 9);
    if (!v)
        return;
    v[8] = (char)('0' + old);
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
        char want[64];
        snprintf(want, sizeof want, "disk format version %d;", old);
        CHECK(strstr(got, want) && strstr(got, "reads version 9 only"));
        /* the previous format has a way over, named in the message; older ones only export and import */
        CHECK((old == 8) == (strstr(got, "tsdb DIR upgrade") != NULL));
    }
    CHECK(entries(dir) == before);
    file_pread(dir, "META", 0, text, (size_t)len);
    CHECK(memcmp(text, meta, (size_t)len) == 0 && file_size(dir, "META") == len);
    rm_rf(dir);
    printf("  a format-%d database is refused at open, naming both versions, untouched\n", old);
}

static void test_format_version(const char *scratch)
{
    test_format_version_of(scratch, 7);
    test_format_version_of(scratch, 8);
}

/* ---------------------------------------------------- second review (REVIEW2) */

#define DAY_MS 86400000LL

static int64_t wall_ms(void) { return (int64_t)time(NULL) * 1000; }

static void insert_at(tsdb *db, tsdb_sid id, int64_t t0, int n)
{
    int64_t *ts = malloc((size_t)n * sizeof *ts);
    float *v = malloc((size_t)n * sizeof *v);
    for (int i = 0; i < n; i++)
        ts[i] = t0 + i * 1000, v[i] = (float)i;
    OK(tsdb_insert_series(db, id, ts, v, (size_t)n));
    free(ts);
    free(v);
}

/* N1 (rv2.c). The last SERIES record, damaged at rest, looks like a torn append and was cut: the next series got its
 * id and inherited what other records said of it, here the retention limit of a series made for decommissioning,
 * which hid and then dropped the new series' rows. A RETENTION or CLASSES record, or the count of a MANIFEST whose
 * writer synced, proves the record was acknowledged: the open fails and changes nothing. STATES the same through
 * MANIFEST. Without fsync MANIFEST proves nothing, and the record is cut as before. */
static void test_records_prove_series(const char *scratch)
{
    char dir[600];
    snprintf(dir, sizeof dir, "%s/damage-records-prove", scratch);
    const char *what[] = {"RETENTION", "CLASSES", "MANIFEST's series", "MANIFEST's states"};
    for (int proof = 0; proof < 4; proof++) {
        rm_rf(dir);
        tsdb_options o = opts();
        o.fsync = 1;
        tsdb *db;
        tsdb_sid a, b, st;
        OK(tsdb_open(dir, &o, &db));
        OK(tsdb_series(db, "a", &a));
        OK(tsdb_series(db, "decommissioned", &b));
        insert_at(db, a, wall_ms() - 10 * DAY_MS, 100);
        if (proof == 0)
            OK(tsdb_set_retention(db, b, DAY_MS));
        if (proof == 1) {
            OK(tsdb_test_class_switch(db, b, 1, NULL));
            CHECK(classes_has(dir, (uint32_t)b));
        }
        if (proof == 3) {
            uint32_t code;
            OK(tsdb_series_create(db, "machine.mode", TSDB_STATE, NULL, &st));
            OK(tsdb_state(db, st, "running", &code));
            insert_at(db, a, wall_ms() - 9 * DAY_MS, 10);
        }
        OK(tsdb_flush(db)); /* publishes MANIFEST with the counts */
        tsdb_close(db);
        if (proof < 2) {
            char m[700];
            path(m, sizeof m, dir, "MANIFEST");
            CHECK(unlink(m) == 0); /* the record alone proves it */
        }
        const char *file = proof == 3 ? "STATES" : "SERIES";
        off_t size = file_size(dir, file);
        flip(dir, file, size - 1); /* the last record's name: one bit, at rest */
        if (try_open(dir) != TSDB_ERR_CORRUPT) {
            fprintf(stderr, "FAIL: %s did not keep the open from cutting the last %s record\n", what[proof], file);
            failures++;
        }
        CHECK(file_size(dir, file) == size); /* nothing cut */
        flip(dir, file, size - 1);
        OK(try_open(dir));
    }

    /* a writer without fsync: its MANIFEST counts records a power loss may take, so the record is cut */
    rm_rf(dir);
    tsdb_options o = opts();
    tsdb *db;
    tsdb_sid a, b;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series(db, "a", &a));
    OK(tsdb_series(db, "decommissioned", &b));
    insert_at(db, a, wall_ms() - 10 * DAY_MS, 100);
    OK(tsdb_flush(db));
    tsdb_close(db);
    flip(dir, "SERIES", file_size(dir, "SERIES") - 1);
    OK(try_open(dir));
    CHECK(rows_of(dir, "decommissioned") == -3); /* cut: no such series */
    printf("  a SERIES or STATES record cut as torn while RETENTION, CLASSES or MANIFEST names it: the open fails\n");
    rm_rf(dir);
}

/* the byte offset of the run table's series ids in a part's index, and the part's footer */
static size_t rser_offset(const char *dir, const char *part, part_footer *ft)
{
    *ft = read_footer(dir, part);
    uint8_t *ib = calloc(1, ft->index_len + 8);
    file_pread(dir, part, (off_t)ft->index_off, ib, ft->index_len);
    gix x;
    size_t off = 0;
    if (gix_parse(ib, ft->index_len, ft->ngranules, &x) == 0 && x.nruns)
        off = (size_t)((const uint8_t *)x.rser - ib);
    else
        CHECK(!"gix_parse");
    free(ib);
    return off;
}

/* A RETENTION or CLASSES record (checksum sealed, as only a crafted file has it) that names series 2^32 - 1: the
 * tables these records fill grew to the id before the open compared it with SERIES, 32 GB (RETENTION) or 4 GB
 * (CLASSES) of zeroed memory, and a reader kept them (the 0.9.0 review, secrel L1). A record of a series SERIES
 * lacks is never applied now: the writer's open fails on it as before, a reader leaves it for a later refresh. The
 * opens run in a child with 3 GB of address space, where such a table fails to allocate (not under ASan or TSan,
 * which reserve terabytes of it). */
static void test_records_huge_id(const char *scratch)
{
    char dir[600];
    snprintf(dir, sizeof dir, "%s/damage-records-huge-id", scratch);
    for (int cls = 0; cls <= 1; cls++) {
        rm_rf(dir);
        tsdb_options o = opts();
        tsdb *db;
        tsdb_sid a;
        OK(tsdb_open(dir, &o, &db));
        OK(tsdb_series(db, "a", &a));
        insert_at(db, a, T0, 10);
        OK(tsdb_flush(db));
        tsdb_close(db);
        const char *file = cls ? "CLASSES" : "RETENTION";
        off_t at = file_size(dir, file);
        if (at < 0)
            at = 0;
        uint8_t rec[24] = {0};
        size_t n = cls ? 24 : 16;
        uint32_t s = UINT32_MAX;
        int64_t v = cls ? INT64_MIN : 86400000; /* CLASSES: from (then class 0 at +16); RETENTION: the limit */
        memcpy(rec + 4, &s, 4);
        memcpy(rec + 8, &v, 8);
        uint32_t check = (uint32_t)xxh64(rec + 4, n - 4, (uint64_t)at);
        memcpy(rec, &check, 4);
        char p[700];
        path(p, sizeof p, dir, file);
        int fd = open(p, O_WRONLY | O_CREAT | O_CLOEXEC, 0644);
        CHECK(fd >= 0 && pwrite(fd, rec, n, at) == (ssize_t)n);
        if (fd >= 0)
            close(fd);
        fflush(NULL);
        pid_t pid = fork();
        if (pid == 0) {
#if !defined(__SANITIZE_ADDRESS__) && !defined(__SANITIZE_THREAD__)
            struct rlimit rl = {3ull << 30, 3ull << 30};
            if (setrlimit(RLIMIT_AS, &rl) != 0)
                _exit(3);
#endif
            int w = try_open(dir); /* the writer: SERIES lacks the series, the open fails, nothing cut */
            if (w != TSDB_ERR_CORRUPT)
                fprintf(stderr, "  %s naming series 2^32 - 1: the writer's open gave %s\n", file, tsdb_strerror(w));
            tsdb_options ro = opts();
            ro.read_only = 1;
            int r = tsdb_open(dir, &ro, &db);
            long rows = -1;
            if (r == 0) {
                tsdb_sid id;
                int qrc;
                if (tsdb_series_lookup(db, "a", &id) == 0)
                    rows = count_rows(db, id, &qrc);
                tsdb_close(db);
            } else {
                fprintf(stderr, "  %s naming series 2^32 - 1: a reader's open gave %s\n", file, tsdb_strerror(r));
            }
            _exit(w == TSDB_ERR_CORRUPT && r == 0 && rows == 10 ? 0 : 1);
        }
        int status = 0;
        CHECK(pid > 0 && waitpid(pid, &status, 0) == pid);
        CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
        CHECK(file_size(dir, file) == at + (off_t)n); /* kept for the operator */
    }
    printf("  a RETENTION or CLASSES record of series 2^32 - 1 grows no table: the writer's open fails, a reader's not\n");
    rm_rf(dir);
}

/* N2 (rv.c part A). A retention change recomputed when each part expires from series ids read in the mapping: an
 * index damaged after the open renamed the run of a series without retention to one with a 1-day limit, and the
 * merge thread retired the whole partition, for good. The drop checks the index first now, and counts the part. */
static void test_retention_damaged_index(const char *scratch)
{
    char dir[600], part[256], again[256];
    snprintf(dir, sizeof dir, "%s/damage-retention-index", scratch);
    rm_rf(dir);
    tsdb_options o = opts();
    o.merge_threads = 1;
    o.merge_idle_s = 100000;
    o.wal_max_batch_rows = 100; /* straight to parts */
    o.partition_classes = 0;
    tsdb *db;
    tsdb_sid a, b;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series(db, "keep.forever", &a));
    OK(tsdb_series(db, "short.lived", &b));
    insert_at(db, a, (wall_ms() / DAY_MS - 30) * DAY_MS + 3600000, 640);
    CHECK(find_file(dir, "p", ".part", part, sizeof part));
    part_footer ft;
    size_t roff = rser_offset(dir, part, &ft);
    off_t at = (off_t)(ft.index_off + roff);
    uint32_t was, one = 1;
    file_pread(dir, part, at, &was, 4);
    CHECK(was == 0);
    file_pwrite(dir, part, at, &one, 4); /* the run of keep.forever now names short.lived */
    OK(tsdb_set_retention(db, b, DAY_MS));
    tsdb_stats st = {0};
    for (int i = 0; i < 40 && st.broken_parts == 0; i++) {
        usleep(100000);
        OK(tsdb_get_stats(db, &st));
    }
    CHECK(st.broken_parts == 1);
    usleep(1500000); /* the merge thread looks again every second: still kept, counted once */
    OK(tsdb_get_stats(db, &st));
    CHECK(st.broken_parts == 1);
    CHECK(find_file(dir, part, "", again, sizeof again));
    file_pwrite(dir, part, at, &was, 4);
    int rc;
    CHECK(count_rows(db, a, &rc) == 640 && rc == 0);
    tsdb_close(db);
    o.merge_threads = 0;
    OK(tsdb_open(dir, &o, &db)); /* repaired: the next open takes it */
    CHECK(count_rows(db, a, &rc) == 640 && rc == 0);
    tsdb_close(db);
    printf("  a retention drop checks the part's index first: damaged since the open, the partition stays\n");
    rm_rf(dir);
}

/* merge-wait (found by jepsen --nemesis misdirect without verify_checksums, 0.9.0). An insert waits while its
 * partition has more than PARTS_TO_DELAY_INSERT parts (db_wait_merges), and the wait had no end: merges that fail
 * on a granule whose payload is damaged (the merge checks every granule's hash; the open does not) were retried
 * every second, the count never fell, and every insert into the partition, so every worker of tsdbd, waited for
 * ever. Now a merge that cannot decode a granule marks its part (once, counted in broken_parts, left out of
 * merges), the wait ends for a partition with a marked part, and for any other partition whose merges make no
 * progress it ends after wait_stall_ms. The rows are published before the wait: the insert still succeeds. */
#define MW_PARTS (PARTS_TO_DELAY_INSERT + 5)
#define MW_ROWS 200
#define MW_T0 ((T0 / DAY_MS + 1) * DAY_MS) /* a day start: all the parts, 61,000 s of rows, in one partition */

static void mw_build(const char *dir, tsdb_sid *id)
{
    rm_rf(dir);
    tsdb_options o = opts(); /* no merge threads: no wait, and nothing merges while the parts pile up */
    o.wal_max_batch_rows = 100; /* straight to parts */
    o.partition_classes = 0;    /* one base partition */
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series(db, "mw.series", id));
    int64_t ts[MW_ROWS];
    float v[MW_ROWS];
    for (int k = 0; k < MW_PARTS; k++) {
        for (int i = 0; i < MW_ROWS; i++)
            ts[i] = MW_T0 + ((int64_t)k * MW_ROWS + i) * 1000, v[i] = (float)(i % 37);
        OK(tsdb_insert_series(db, *id, ts, v, MW_ROWS));
    }
    OK(tsdb_close(db));
}

struct mw_insert {
    tsdb *db;
    tsdb_sid id;
    int rc;
    atomic_int done;
};

static void *mw_insert_main(void *arg)
{
    struct mw_insert *m = arg;
    int64_t ts[MW_ROWS];
    float v[MW_ROWS];
    for (int i = 0; i < MW_ROWS; i++)
        ts[i] = MW_T0 + ((int64_t)MW_PARTS * MW_ROWS + i) * 1000, v[i] = 1.0f;
    m->rc = tsdb_insert_series(m->db, m->id, ts, v, MW_ROWS);
    atomic_store(&m->done, 1);
    return NULL;
}

/* One more insert into the full partition; it must return within `limit_s`, with success. The old code never
 * returned: the test says so and leaves (a hung call cannot be cancelled, and tsdb_close would wait for it). */
static void mw_insert_returns(tsdb *db, tsdb_sid id, int limit_s, const char *what)
{
    struct mw_insert m = {.db = db, .id = id};
    pthread_t t;
    CHECK(pthread_create(&t, NULL, mw_insert_main, &m) == 0);
    for (int i = 0; i < limit_s * 10 && !atomic_load(&m.done); i++)
        usleep(100000);
    if (!atomic_load(&m.done)) {
        fprintf(stderr, "%s:%d: %s: the insert still waits for merges after %d s\n", __FILE__, __LINE__, what,
                limit_s);
        _exit(1);
    }
    pthread_join(t, NULL);
    OK(m.rc);
}

static void test_wait_merges_damaged(const char *scratch)
{
    char dir[600];
    snprintf(dir, sizeof dir, "%s/damage-merge-wait", scratch);
    for (int verify = 0; verify <= 1; verify++) {
        tsdb_sid id;
        mw_build(dir, &id);
        /* the payload of every part: a flipped byte in the middle of its data (the same place in each: they are
         * alike). Every merge fails, whichever parts it picks. */
        DIR *d = opendir(dir);
        CHECK(d != NULL);
        int n = 0;
        struct dirent *e;
        while (d && (e = readdir(d)) != NULL) {
            size_t l = strlen(e->d_name);
            if (e->d_name[0] != 'p' || l < 5 || strcmp(e->d_name + l - 5, ".part") != 0)
                continue;
            part_footer ft = read_footer(dir, e->d_name);
            flip(dir, e->d_name, (off_t)(sizeof(part_header) + ft.index_off) / 2);
            n++;
        }
        if (d)
            closedir(d);
        CHECK(n == MW_PARTS);
        tsdb_options o = opts();
        o.merge_threads = 1;
        o.partition_classes = 0;
        o.wal_max_batch_rows = 100; /* the insert goes to a part, and waits */
        o.verify_checksums = verify; /* the merge checks granule hashes in either case */
        tsdb *db;
        OK(tsdb_open(dir, &o, &db));
        /* every part is damaged, but each merge marks only the one it met: until all are marked a pair is left
         * to merge, so the wait ends by the stall rule (merges that fail count as no progress) */
        int was = atomic_load(&wait_stall_ms);
        atomic_store(&wait_stall_ms, 3000);
        mw_insert_returns(db, id, 30, verify ? "verify_checksums on" : "verify_checksums off");
        atomic_store(&wait_stall_ms, was);
        tsdb_stats st = {0};
        for (int i = 0; i < 50 && st.broken_parts == 0; i++) { /* the merge thread marks it on its first try */
            usleep(100000);
            OK(tsdb_get_stats(db, &st));
        }
        CHECK(st.broken_parts >= 1);
        usleep(2500000); /* retried every second before: counted once per part now, and never more than the parts */
        tsdb_stats st2 = {0};
        OK(tsdb_get_stats(db, &st2));
        CHECK(st2.broken_parts <= MW_PARTS + 1);
        RC(tsdb_optimize(db), TSDB_ERR_CORRUPT); /* reports the partition it left alone */
        tsdb_close(db);
    }
    printf("  an insert into a partition whose merges fail on a damaged payload does not wait for ever\n");
    rm_rf(dir);
}

/* The other half: no part is damaged, but no pair can merge (max_part_bytes), so the count never falls. The wait
 * ends after wait_stall_ms (shortened here) with no merge running, and the insert succeeds. */
static void test_wait_merges_stalled(const char *scratch)
{
    char dir[600];
    snprintf(dir, sizeof dir, "%s/damage-merge-stall", scratch);
    tsdb_sid id;
    mw_build(dir, &id);
    tsdb_options o = opts();
    o.merge_threads = 1;
    o.partition_classes = 0;
    o.wal_max_batch_rows = 100; /* the insert goes to a part, and waits */
    o.max_part_bytes = 1;       /* no range of two parts fits */
    o.merge_idle_s = 1000000;   /* and no solo rewrite */
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    int was = atomic_load(&wait_stall_ms);
    atomic_store(&wait_stall_ms, 1500);
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    mw_insert_returns(db, id, 30, "parts that cannot pair");
    clock_gettime(CLOCK_MONOTONIC, &t1);
    CHECK((t1.tv_sec - t0.tv_sec) * 1000 + (t1.tv_nsec - t0.tv_nsec) / 1000000 >= 1400); /* it did wait */
    /* and the stall is remembered: the next insert into the partition goes on at once, instead of paying
     * wait_stall_ms again (it was paid by every call, and k times by an insert that wrote k parts of it) */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    mw_insert_returns(db, id, 30, "a partition declared stalled");
    mw_insert_returns(db, id, 30, "a partition declared stalled, again");
    clock_gettime(CLOCK_MONOTONIC, &t1);
    CHECK((t1.tv_sec - t0.tv_sec) * 1000 + (t1.tv_nsec - t0.tv_nsec) / 1000000 < 700);
    atomic_store(&wait_stall_ms, was);
    int rc;
    CHECK(count_rows(db, id, &rc) == (long)(MW_PARTS + 1) * MW_ROWS && rc == 0); /* and the rows are all there */
    tsdb_close(db);
    printf("  an insert stops waiting for merges that cannot run (parts that cannot pair), after wait_stall_ms,"
           " and the next ones do not wait again\n");
    rm_rf(dir);
}

/* Merges that fail without marking a part (a full disk, EIO on the merge output before it has a name: the test hook
 * merge_fault_ms sleeps, then fails) are retried every second. The wait counted only the time between merges, so
 * with merges that take long it ended after about wait_stall_ms times the length of a merge (tens of minutes at
 * 4 GiB outputs): a failed merge now counts as no progress, and the wait ends near wait_stall_ms after it. */
static void test_wait_merges_failing(const char *scratch)
{
    char dir[600];
    snprintf(dir, sizeof dir, "%s/damage-merge-failing", scratch);
    tsdb_sid id;
    mw_build(dir, &id);
    tsdb_options o = opts();
    o.merge_threads = 1;
    o.partition_classes = 0;
    o.wal_max_batch_rows = 100;
    o.merge_idle_s = 1000000;
    tsdb *db;
    int was = atomic_load(&wait_stall_ms);
    atomic_store(&wait_stall_ms, 2500);
    /* Every merge runs 5 s, then fails; 1 s back-off between them. Set before the open: the merge thread starts
     * with it and sees MW_PARTS parts at once, so a hook set after the open lost a race under load (a starved
     * test thread let one real merge of about 100 parts through, the partition fell under the delay limit and
     * the insert returned at once: ms >= 4000 failed in about 1 run in 18 at 128 busy loops). */
    atomic_store(&merge_fault_ms, 5000);
    OK(tsdb_open(dir, &o, &db));
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    /* first failure after 5 s, then 2.5 s: about 8 s. The old clock gained at most 1 s per 6 s cycle: 15 s or more */
    mw_insert_returns(db, id, 12, "merges that keep failing");
    clock_gettime(CLOCK_MONOTONIC, &t1);
    long ms = (t1.tv_sec - t0.tv_sec) * 1000 + (t1.tv_nsec - t0.tv_nsec) / 1000000;
    CHECK(ms >= 4000); /* it did wait for the first merge */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    mw_insert_returns(db, id, 30, "merges that keep failing, the next insert");
    clock_gettime(CLOCK_MONOTONIC, &t1);
    CHECK((t1.tv_sec - t0.tv_sec) * 1000 + (t1.tv_nsec - t0.tv_nsec) / 1000000 < 700);
    atomic_store(&merge_fault_ms, 0);
    atomic_store(&wait_stall_ms, was);
    tsdb_close(db);
    printf("  an insert stops waiting for merges that keep failing, near wait_stall_ms\n");
    rm_rf(dir);
}

/* merge-barrier. A damaged part used to take its whole partition out of merges, so the part count grew without
 * limit until the part was restored. Merges combine adjacent block ranges only, so the ranges below and above a
 * damaged part can still be merged, each alone: the damaged part's tombstones keep hiding the lower blocks (the
 * lower range gets a block number below it), and the tombstones of the upper range keep hiding its rows (the output
 * keeps the union of its sources' tombstones, as a listed older part, the damaged one, exists). A merge across the
 * barrier is never made: it would put a lower block's rows into an output newer than the damaged part's tombstones.
 *
 * The partition, in block order (1 s apart; t(i) = MB_T0 + i s, values are small integers so that sums are exact
 * in every layout; 9 parts): L1 rows [0,150) = 1   L2 rows [100,400) = 2   L3 delete [200,230)   a replace of
 * [300,600): its tombstone, and D = its rows [580,600) = 4 (the part damaged below: a flipped byte in its payload)
 * U1 rows [250,350) = 5   U2 delete [320,330) (it hides only U1's rows inside the upper merge: L2's rows there are
 * already hidden by the replace's tombstone [300,600), a block below the barrier; test_merge_barrier_upper_tomb and
 * test_merge_barrier_bottom are the cases where an upper tombstone alone hides rows below it)
 * U3 rows [0,50) = 9   U4 rows [380,470) = 6.
 * Queries cover [0,580): D's rows [580,600) are out of range and never decoded. */
#define MB_T0 MW_T0
#define MB_T(i) (MB_T0 + (int64_t)(i) * 1000)
#define MB_END MB_T(580)

static void mb_rows(tsdb *db, tsdb_sid id, int a, int b, float val)
{
    int64_t ts[300];
    float v[300];
    int n = b - a;
    CHECK(n <= 300);
    for (int i = 0; i < n; i++)
        ts[i] = MB_T(a + i), v[i] = val;
    OK(tsdb_insert_series(db, id, ts, v, (size_t)n));
}

static void mb_build(const char *dir, tsdb_sid *id)
{
    rm_rf(dir);
    tsdb_options o = opts();
    o.wal_max_batch_rows = 10; /* the inserts go straight to parts: one each (every one has more than 10 rows) */
    o.partition_classes = 0;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series(db, "mb.series", id));
    mb_rows(db, *id, 0, 150, 1);
    mb_rows(db, *id, 100, 400, 2);
    OK(tsdb_delete_range(db, *id, MB_T(200), MB_T(230)));
    float v4[20];
    int64_t ts4[20]; /* the replace: the tombstone part first, then the rows */
    for (int i = 0; i < 20; i++)
        ts4[i] = MB_T(580 + i), v4[i] = 4;
    tsdb_values col = {.type = TSDB_FLOAT32, .values = v4, .valid = NULL};
    OK(tsdb_replace_range(db, *id, MB_T(300), MB_T(600), ts4, &col, 20));
    mb_rows(db, *id, 250, 350, 5);
    OK(tsdb_delete_range(db, *id, MB_T(320), MB_T(330)));
    mb_rows(db, *id, 0, 50, 9);
    mb_rows(db, *id, 380, 470, 6);
    OK(tsdb_close(db));
}

struct mb_names {
    char name[16][96];
    int64_t rows[16];
    int n;
};

static void mb_collect(void *ctx, const tsdb_part_info *info)
{
    struct mb_names *c = ctx;
    if (c->n < 16) {
        snprintf(c->name[c->n], sizeof c->name[0], "%s", info->name);
        c->rows[c->n] = (int64_t)info->rows;
    }
    c->n++;
}

/* The damaged part is the one with the 20 rows of the replace (the replace's tombstone is a part of its own, the
 * one before it). */
static void mb_damage(const char *dir, char *name, size_t len)
{
    tsdb_options o = opts();
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    struct mb_names pn = {.n = 0};
    OK(tsdb_foreach_part(db, mb_collect, &pn));
    CHECK(pn.n == 9);
    for (int i = 0; i < pn.n && i < 16 && pn.n != 9; i++)
        fprintf(stderr, "  part %s: %lld rows\n", pn.name[i], (long long)pn.rows[i]);
    int d = -1;
    for (int i = 0; i < pn.n && i < 16; i++)
        if (pn.rows[i] == 20)
            d = i;
    CHECK(d == 4);
    snprintf(name, len, "%s", d >= 0 ? pn.name[d] : "");
    OK(tsdb_close(db));
    part_footer ft = read_footer(dir, name);
    flip(dir, name, (off_t)(sizeof(part_header) + ft.index_off) / 2);
}

struct mb_answers {
    tsdb_points pts;
    tsdb_bucket *agg[5];
    size_t nagg[5];
    int rc;
};

static void mb_ask(tsdb *db, tsdb_sid id, struct mb_answers *a)
{
    memset(a, 0, sizeof *a);
    int rc = tsdb_query_points(db, id, MB_T0, MB_END, &a->pts);
    const int64_t iv[3] = {0, 60000, 7000};
    for (int i = 0; i < 3 && rc == 0; i++)
        rc = tsdb_query_agg(db, id, MB_T0, MB_END, iv[i], &a->agg[i], &a->nagg[i]);
    if (rc == 0) {
        tsdb_group g = {.unit = TSDB_HOUR, .count = 1};
        rc = tsdb_query_group(db, id, MB_T0, MB_END, &g, &a->agg[3], &a->nagg[3]);
    }
    if (rc == 0) {
        tsdb_query_opts q = {.order = TSDB_DESC, .limit = 4};
        rc = tsdb_query_agg_ex(db, id, MB_T0, MB_END, 30000, &q, &a->agg[4], &a->nagg[4]);
    }
    a->rc = rc;
}

static void mb_free(struct mb_answers *a)
{
    tsdb_points_free(&a->pts);
    for (int i = 0; i < 5; i++)
        free(a->agg[i]);
}

static bool mb_same(const struct mb_answers *x, const struct mb_answers *y)
{
    if (x->rc || y->rc || x->pts.n != y->pts.n || x->pts.n == 0)
        return false;
    for (size_t i = 0; i < x->pts.n; i++)
        if (x->pts.ts[i] != y->pts.ts[i] || x->pts.values[i] != y->pts.values[i])
            return false;
    for (int k = 0; k < 5; k++) {
        if (x->nagg[k] != y->nagg[k] || x->nagg[k] == 0)
            return false;
        for (size_t i = 0; i < x->nagg[k]; i++) {
            const tsdb_bucket *p = &x->agg[k][i], *q = &y->agg[k][i];
            if (p->start != q->start || p->count != q->count || p->sum != q->sum || p->min != q->min ||
                p->max != q->max)
                return false;
        }
    }
    return true;
}

/* L1..L3 and the replace's tombstone become one part, U1..U4 another, the damaged part stays between them. */
static void mb_check_layout(tsdb *db, const char *dparts)
{
    struct mb_names pn = {.n = 0};
    OK(tsdb_foreach_part(db, mb_collect, &pn));
    CHECK(pn.n == 3);
    if (pn.n == 3) {
        CHECK(strcmp(pn.name[1], dparts) == 0);
        CHECK(pn.rows[1] == 20);
        CHECK(strcmp(pn.name[0], dparts) != 0 && strcmp(pn.name[2], dparts) != 0);
    }
}

static void test_merge_barrier(const char *scratch)
{
    char dir[600], dname[96];
    snprintf(dir, sizeof dir, "%s/damage-merge-barrier", scratch);
    for (int mode = 0; mode < 3; mode++) { /* optimize, then the merge thread; either one with verify_checksums */
        tsdb_sid id;
        mb_build(dir, &id);
        struct mb_answers before;
        tsdb_options o = opts();
        o.partition_classes = 0;
        tsdb *db;
        OK(tsdb_open(dir, &o, &db));
        mb_ask(db, id, &before);
        OK(tsdb_close(db));
        CHECK(before.rc == 0 && before.pts.n > 300);
        mb_damage(dir, dname, sizeof dname);
        o.verify_checksums = mode == 2;
        if (mode == 0 || mode == 2) {
            OK(tsdb_open(dir, &o, &db));
            RC(tsdb_optimize(db), TSDB_ERR_CORRUPT); /* the damaged part is named: said so, the rest is merged */
        } else {
            o.merge_threads = 1;
            o.merge_idle_s = 1; /* idle: every range may merge, and a rewrite of a single part too */
            OK(tsdb_open(dir, &o, &db));
            for (int i = 0; i < 400; i++) { /* the thread meets the damaged part in a merge, marks it, goes on */
                struct mb_names pn = {.n = 0};
                OK(tsdb_foreach_part(db, mb_collect, &pn));
                if (pn.n == 3)
                    break;
                usleep(100000);
            }
        }
        mb_check_layout(db, dname);
        struct mb_answers after;
        mb_ask(db, id, &after);
        CHECK(mb_same(&before, &after));
        tsdb_stats st;
        OK(tsdb_get_stats(db, &st));
        CHECK(st.broken_parts == 1);
        OK(tsdb_close(db));
        /* and from disk: the parts a merge named, and the damaged one, give the same answers after a reopen */
        o.merge_threads = 0;
        OK(tsdb_open(dir, &o, &db));
        mb_check_layout(db, dname);
        struct mb_answers again;
        mb_ask(db, id, &again);
        CHECK(mb_same(&before, &again));
        OK(tsdb_close(db));
        mb_free(&before);
        mb_free(&after);
        mb_free(&again);
    }
    printf("  a damaged part is a merge barrier: the ranges below and above merge (optimize and the merge thread),"
           " none across it, answers unchanged\n");
    rm_rf(dir);
}

static void mb_partition_parts(void *ctx, const tsdb_part_info *info)
{
    int *n = ctx;
    (*n)++;
    (void)info;
}

/* The only tombstone that hides rows below the barrier is in the upper range: a delete over L1/L2 rows [50,150),
 * after D. The upper merge must keep it (its output's `older` is true because D and the lower parts are listed);
 * with `older` false the merge drops the tombstone and 100 deleted rows come back. Both merge paths. */
static void test_merge_barrier_upper_tomb(const char *scratch)
{
    char dir[600], dname[96];
    snprintf(dir, sizeof dir, "%s/mb-upper-tomb", scratch);
    for (int mode = 0; mode < 2; mode++) {
        rm_rf(dir);
        tsdb_options o = opts();
        o.wal_max_batch_rows = 10;
        o.partition_classes = 0;
        tsdb *db;
        tsdb_sid id;
        OK(tsdb_open(dir, &o, &db));
        OK(tsdb_series(db, "mb.series", &id));
        mb_rows(db, id, 0, 100, 1);   /* L1 */
        mb_rows(db, id, 100, 200, 2); /* L2 */
        mb_rows(db, id, 200, 260, 3); /* L3 */
        mb_rows(db, id, 580, 600, 4); /* D (damaged below; its rows are out of the queried range) */
        mb_rows(db, id, 300, 350, 5); /* U1 */
        OK(tsdb_delete_range(db, id, MB_T(50), MB_T(150))); /* U2: hides L1/L2 rows [50,150), below the barrier */
        mb_rows(db, id, 400, 450, 6); /* U3 */
        mb_rows(db, id, 450, 480, 7); /* U4 */
        OK(tsdb_close(db));
        OK(tsdb_open(dir, &o, &db));
        struct mb_answers before;
        mb_ask(db, id, &before);
        struct mb_names pn = {.n = 0};
        OK(tsdb_foreach_part(db, mb_collect, &pn));
        CHECK(pn.n == 8);
        int d = -1;
        for (int i = 0; i < pn.n && i < 16; i++)
            if (pn.rows[i] == 20)
                d = i;
        CHECK(d == 3);
        snprintf(dname, sizeof dname, "%s", d >= 0 ? pn.name[d] : "");
        OK(tsdb_close(db));
        part_footer ft = read_footer(dir, dname);
        flip(dir, dname, (off_t)(sizeof(part_header) + ft.index_off) / 2);
        if (mode == 0) {
            OK(tsdb_open(dir, &o, &db));
            RC(tsdb_optimize(db), TSDB_ERR_CORRUPT);
        } else {
            o.merge_threads = 1;
            o.merge_idle_s = 1;
            OK(tsdb_open(dir, &o, &db));
            for (int i = 0; i < 400; i++) {
                struct mb_names q = {.n = 0};
                OK(tsdb_foreach_part(db, mb_collect, &q));
                if (q.n == 3)
                    break;
                usleep(100000);
            }
        }
        struct mb_names q = {.n = 0};
        OK(tsdb_foreach_part(db, mb_collect, &q));
        CHECK(q.n == 3);
        struct mb_answers after;
        mb_ask(db, id, &after);
        CHECK(before.rc == 0 && after.rc == 0);
        CHECK(before.pts.n == after.pts.n);
        CHECK(mb_same(&before, &after));
        OK(tsdb_close(db));
        mb_free(&before);
        mb_free(&after);
    }
    rm_rf(dir);
    printf("  merge barrier: an upper tombstone keeps hiding rows below the barrier\n");
}

/* The damaged part is the lowest of its partition and only an upper tombstone hides some of its rows. The upper
 * merge must keep that tombstone because D is listed below it (`older` must count a damaged part). The damage is
 * then undone (a misdirected read: part_mark_payload keeps the mark in memory only) and the reopen reads D again:
 * rows [0,50) must stay hidden. */
static void test_merge_barrier_bottom(const char *scratch)
{
    char dir[600], dname[96];
    snprintf(dir, sizeof dir, "%s/mb-bottom", scratch);
    rm_rf(dir);
    tsdb_options o = opts();
    o.wal_max_batch_rows = 10;
    o.partition_classes = 0;
    tsdb *db;
    tsdb_sid id;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series(db, "mb.series", &id));
    mb_rows(db, id, 0, 100, 4);   /* D */
    mb_rows(db, id, 200, 300, 5); /* U1 */
    OK(tsdb_delete_range(db, id, MB_T(0), MB_T(50))); /* U2: hides D's rows [0,50) */
    mb_rows(db, id, 300, 350, 6); /* U3 */
    OK(tsdb_close(db));
    OK(tsdb_open(dir, &o, &db));
    struct mb_answers before;
    mb_ask(db, id, &before);
    struct mb_names pn = {.n = 0};
    OK(tsdb_foreach_part(db, mb_collect, &pn));
    CHECK(pn.n == 4);
    snprintf(dname, sizeof dname, "%s", pn.name[0]);
    OK(tsdb_close(db));
    part_footer ft = read_footer(dir, dname);
    off_t off = (off_t)(sizeof(part_header) + ft.index_off) / 2;
    flip(dir, dname, off);
    OK(tsdb_open(dir, &o, &db));
    RC(tsdb_optimize(db), TSDB_ERR_CORRUPT);
    struct mb_names q = {.n = 0};
    OK(tsdb_foreach_part(db, mb_collect, &q));
    CHECK(q.n == 2);
    OK(tsdb_close(db));
    flip(dir, dname, off); /* the damage was a misread: the part is whole again */
    OK(tsdb_open(dir, &o, &db));
    struct mb_answers after;
    mb_ask(db, id, &after);
    CHECK(before.rc == 0 && after.rc == 0);
    CHECK(mb_same(&before, &after));
    OK(tsdb_close(db));
    mb_free(&before);
    mb_free(&after);
    rm_rf(dir);
    printf("  merge barrier: an upper tombstone keeps hiding the damaged part's rows\n");
}

/* With a part marked, a stream of inserts into the partition keeps its part count bounded again (before, it grew
 * with every insert: nothing merged, and the wait for merges was skipped for a partition with a marked part). */
static void test_merge_barrier_bounded(const char *scratch)
{
    char dir[600], dname[96];
    snprintf(dir, sizeof dir, "%s/damage-merge-barrier-stream", scratch);
    tsdb_sid id;
    mb_build(dir, &id);
    mb_damage(dir, dname, sizeof dname);
    tsdb_options o = opts();
    o.partition_classes = 0;
    o.wal_max_batch_rows = 100;
    o.merge_threads = 1;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    RC(tsdb_optimize(db), TSDB_ERR_CORRUPT); /* marks it (one merge across it fails first), the rest merges */
    tsdb_stats st;
    OK(tsdb_get_stats(db, &st));
    CHECK(st.broken_parts == 1);
    int worst = 0;
    int was = atomic_load(&wait_stall_ms);
    atomic_store(&wait_stall_ms, 5000);
    for (int k = 0; k < 900; k++) { /* each insert is a part of its own */
        int64_t ts[120];
        float v[120];
        for (int i = 0; i < 120; i++)
            ts[i] = MB_T(1000 + k) + i, v[i] = 1.0f; /* 1 ms apart: a second's rows, in the same partition */
        OK(tsdb_insert_series(db, id, ts, v, 120));
        int n = 0;
        OK(tsdb_foreach_part(db, mb_partition_parts, &n));
        worst = MAX(worst, n);
    }
    atomic_store(&wait_stall_ms, was);
    CHECK(worst <= PARTS_TO_DELAY_INSERT + 5);
    int rc;
    tsdb_points pts = {0};
    rc = tsdb_query_points(db, id, MB_T(1000), INT64_MAX, &pts);
    CHECK(rc == 0 && pts.n == 900 * 120);
    tsdb_points_free(&pts);
    OK(tsdb_close(db));
    printf("  with a damaged part in the partition a stream of inserts keeps its part count bounded"
           " (at most %d parts)\n", worst);
    rm_rf(dir);
}

struct part_count {
    int64_t partition;
    int n;
};

static void count_partition(void *ctx, const tsdb_part_info *info)
{
    struct part_count *c = ctx;
    c->n += info->partition == c->partition;
}

/* n3 (rv.c part B). A merge that failed the index check was retried every second, forever: two log lines a second,
 * no count, and since retention runs only when no merge is selected, expired partitions were never dropped. The
 * part is marked now: logged and counted once, left out of merge selection, and the rest goes on. */
static void test_damaged_part_no_starvation(const char *scratch)
{
    char dir[600], log[700], p1[256], pfx[64], left[256];
    snprintf(dir, sizeof dir, "%s/damage-merge-starve", scratch);
    snprintf(log, sizeof log, "%s/damage-merge-starve.log", scratch);
    rm_rf(dir);
    tsdb_options o = opts();
    o.merge_threads = 1;
    o.merge_idle_s = 2; /* in whole seconds: not before the damage below */
    o.wal_max_batch_rows = 100;
    o.partition_classes = 0;
    tsdb *db;
    tsdb_sid a, c;
    OK(tsdb_open(dir, &o, &db));
    OK(tsdb_series(db, "damaged", &a));
    OK(tsdb_series(db, "expiring", &c));
    int64_t now = wall_ms(), yday = (now / DAY_MS - 1) * DAY_MS + 3600000, old = (now / DAY_MS - 30) * DAY_MS + 3600000;
    insert_at(db, a, yday, 640);
    insert_at(db, a, yday + 640 * 1000, 640);
    snprintf(pfx, sizeof pfx, "p%lld_", (long long)(yday / DAY_MS));
    CHECK(find_file(dir, pfx, ".part", p1, sizeof p1));
    part_footer ft = read_footer(dir, p1);
    fflush(stderr);
    int saved = dup(2), lf = open(log, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    CHECK(saved >= 0 && lf >= 0 && dup2(lf, 2) == 2);
    flip(dir, p1, (off_t)(ft.index_off + ft.index_len - 8));
    insert_at(db, c, old, 640);
    snprintf(pfx, sizeof pfx, "p%lld_", (long long)(old / DAY_MS));
    usleep(4000000); /* the partition of yesterday is idle: its merge fails on the index check */
    OK(tsdb_set_retention(db, c, DAY_MS));
    struct part_count pc = {old / DAY_MS, 1};
    for (int i = 0; i < 60 && pc.n; i++) { /* the part list, not the files: a rewrite renames them */
        usleep(100000);
        pc.n = 0;
        OK(tsdb_foreach_part(db, count_partition, &pc));
    }
    CHECK(pc.n == 0); /* the expired partition went, though a merge keeps failing elsewhere */
    CHECK(!find_file(dir, pfx, ".part", left, sizeof left));
    RC(tsdb_optimize(db), TSDB_ERR_CORRUPT); /* the damaged partition is left out, and said so */
    tsdb_stats st;
    OK(tsdb_get_stats(db, &st));
    tsdb_close(db);
    fflush(stderr);
    dup2(saved, 2);
    close(saved);
    close(lf);
    CHECK(st.broken_parts == 1);
    FILE *f = fopen(log, "r");
    int lines = 0;
    char line[2048];
    while (f && fgets(line, sizeof line, f))
        lines += strstr(line, "no longer matches the checksum") != NULL;
    if (f)
        fclose(f);
    if (lines != 1) {
        fprintf(stderr, "FAIL: the damaged index was reported %d times, not once\n", lines);
        failures++;
    }
    printf("  a part that fails the merge's index check is reported once and left out; retention drops go on\n");
    unlink(log);
    rm_rf(dir);
}

/* Copies what a follower in dst may see of the writer's directory src: parts (linked) and the small files, SERIES
 * only up to `series_bytes` (its last records still in the writer's page cache, say), and no WAL. */
static void mirror(const char *src, const char *dst, off_t series_bytes)
{
    mkdir(dst, 0755);
    DIR *d = opendir(src);
    struct dirent *e;
    while (d && (e = readdir(d)) != NULL) {
        const char *n = e->d_name;
        char from[1024], to[1024];
        path(from, sizeof from, src, n);
        path(to, sizeof to, dst, n);
        if (n[0] == '.' || !strncmp(n, "wal_", 4) || !strcmp(n, "LOCK") || !strncmp(n, "tmp", 3) ||
            !strncmp(n, "retired_", 8))
            continue;
        if (n[0] == 'p' && strstr(n, ".part")) {
            if (link(from, to) != 0 && errno != EEXIST)
                CHECK(!"link");
            continue;
        }
        off_t len = file_size(src, n);
        if (len < 0 || e->d_type == DT_DIR) {
            fprintf(stderr, "mirror: %s skipped (size %lld)\n", n, (long long)len);
            continue;
        }
        if (!strcmp(n, "SERIES") && series_bytes < len)
            len = series_bytes;
        uint8_t *buf = malloc((size_t)len + 1);
        file_pread(src, n, 0, buf, (size_t)len);
        int fd = open(to, O_WRONLY | O_CREAT, 0644);
        CHECK(fd >= 0 && pwrite(fd, buf, (size_t)len, 0) == (ssize_t)len && ftruncate(fd, len) == 0);
        if (fd >= 0)
            close(fd);
        free(buf);
    }
    if (d)
        closedir(d);
}

/* n4. A follower's patience for a series SERIES lacks ran from the start of the gap, which a writer creating series
 * steadily keeps open for good (each MANIFEST counts a record or two not yet visible): after the patience every
 * refresh failed, and the view froze. It runs per record now, from when the first missing one went missing; a
 * writer without fsync has its views taken meanwhile. In one process: the follower reads a copy of the writer's
 * files whose SERIES lags three records behind. */
static void test_follower_steady_creation(const char *scratch)
{
    char dir[600], fdir[700];
    snprintf(dir, sizeof dir, "%s/damage-steady", scratch);
    snprintf(fdir, sizeof fdir, "%s/damage-steady-follower", scratch);
    for (int sync = 0; sync <= 1; sync++) {
        rm_rf(dir);
        rm_rf(fdir);
        tsdb_options o = opts();
        o.fsync = sync;
        o.wal_max_batch_rows = 10; /* straight to parts */
        o.partition_classes = 0;
        tsdb *w, *f;
        tsdb_sid s0, id;
        OK(tsdb_open(dir, &o, &w));
        OK(tsdb_series(w, "s0000", &s0));
        int64_t t = T0;
        insert_at(w, s0, t, 20);
        t += 20 * 1000;
        mirror(dir, fdir, file_size(dir, "SERIES"));
        OK(open_reader(fdir, 0, &f));
        int rc;
        long first = count_rows(f, s0, &rc);
        CHECK(first == 20);
        int fails = 0, n = 1;
        long seen = first;
        /* 3.6 s, longer than the 2 s patience; each record goes missing for about 0.3 s */
        for (int tick = 0; tick < 36; tick++, n++) {
            char name[32];
            snprintf(name, sizeof name, "s%04d", n); /* records of 16 bytes */
            OK(tsdb_series(w, name, &id));
            insert_at(w, id, T0, 20);
            insert_at(w, s0, t, 20);
            t += 20 * 1000;
            OK(tsdb_flush(w));
            mirror(dir, fdir, MAX(16, file_size(dir, "SERIES") - 3 * 16)); /* never below what it read */
            fails += tsdb_refresh(f) != 0;
            seen = count_rows(f, s0, &rc);
            usleep(100000);
        }
        if (fails) {
            fprintf(stderr, "FAIL: steady creation (fsync %d): %d refreshes failed\n", sync, fails);
            failures++;
        }
        if (!sync)
            CHECK(seen >= first + 30 * 20); /* the views went on: rows of s0 kept arriving */
        /* one record that stays missing is still reported after the patience (a syncing writer's) */
        rc = 0;
        for (int i = 0; sync && i < 40 && rc == 0; i++) {
            usleep(100000);
            rc = tsdb_refresh(f);
        }
        if (sync)
            RC(rc, TSDB_ERR_CORRUPT);
        mirror(dir, fdir, file_size(dir, "SERIES"));
        OK(tsdb_refresh(f));
        CHECK(count_rows(f, s0, &rc) == 20 + 36 * 20);
        tsdb_close(f);
        tsdb_close(w);
    }
    printf("  a writer creating series steadily: followers neither fail nor freeze; a record missing for good is "
           "reported\n");
    rm_rf(dir);
    rm_rf(fdir);
}

struct seg_crash {
    tsdb *db;
    tsdb_sid id;
};

static void insert_then_die(void *arg)
{
    struct seg_crash *c = arg;
    for (int r = 0; r < 3; r++)
        insert_at(c->db, c->id, T0 + (100 + r * 10) * 1000, 5);
    _exit(0); /* a crash in the middle of the flush: segment 1 not in parts, segment 2 with records */
}

/* A child writes three records to WAL segment 1, starts a flush (segment 2 created, segment 1 synced to its end),
 * writes three records to segment 2 while the flush places its rows, and dies before the flush lists a part. */
static void crash_with_two_segments(const char *dir)
{
    rm_rf(dir);
    pid_t pid = fork();
    if (pid == 0) {
        alarm(20);
        tsdb_options o = opts();
        o.fsync = 1;
        struct seg_crash c;
        if (tsdb_open(dir, &o, &c.db) || tsdb_series(c.db, "wal.series", &c.id))
            _exit(1);
        for (int r = 0; r < 3; r++)
            insert_at(c.db, c.id, T0 + r * 10 * 1000, 5);
        tsdb_test_midplace(c.db, insert_then_die, &c);
        tsdb_flush(c.db);
        _exit(2);
    }
    int status;
    waitpid(pid, &status, 0);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

/* n5. The torn end of a WAL segment that a later segment with records follows was cut like a torn append, losing
 * acknowledged rows: a power loss cannot do that, since each segment is synced to its end before the next takes
 * rows (wal_check_order). Refused now, with nothing changed; a torn end of the newest segment is still cut. */
static void test_wal_torn_before_later(const char *scratch)
{
    char dir[600], s1[64], s2[64];
    snprintf(dir, sizeof dir, "%s/damage-wal-order", scratch);
    snprintf(s1, sizeof s1, "wal_%016x.log", 1);
    snprintf(s2, sizeof s2, "wal_%016x.log", 2);
    crash_with_two_segments(dir);
    CHECK(file_size(dir, s1) == 3 * (WAL_HDR + 5 * WAL_ROW) && file_size(dir, s2) == 3 * (WAL_HDR + 5 * WAL_ROW));
    CHECK(rows_of(dir, "wal.series") == 30); /* the control: both replayed */

    crash_with_two_segments(dir);
    off_t len = file_size(dir, s1);
    cut(dir, s1, len - 7); /* the last record of segment 1 torn */
    RC(try_open(dir), TSDB_ERR_CORRUPT);
    CHECK(file_size(dir, s1) == len - 7 && file_size(dir, s2) == len);
    char m[700];
    path(m, sizeof m, dir, s1);
    CHECK(unlink(m) == 0); /* what the message says to do to accept the loss */
    CHECK(rows_of(dir, "wal.series") == 15);

    crash_with_two_segments(dir);
    cut(dir, s2, file_size(dir, s2) - 7); /* the newest segment's end: a torn append */
    CHECK(rows_of(dir, "wal.series") == 25);
    printf("  a torn WAL segment followed by one with records fails the open; the newest one's torn end is cut\n");
    rm_rf(dir);
}

/* i6. A follower that read the last SERIES record kept its name after the writer cut that record (damaged at rest,
 * taken for a torn tail) and handed its id to a new name: SERIES shorter than what it read, or the same size with
 * another record there. A reload reads the last record again now, and the refresh reports it after the patience. */
static void test_follower_series_rewritten(const char *scratch)
{
    char dir[600];
    snprintf(dir, sizeof dir, "%s/damage-series-rewritten", scratch);
    rm_rf(dir);
    tsdb_options o = opts(); /* no fsync: MANIFEST does not prove the last record, and the writer cuts it */
    o.wal_max_batch_rows = 10;
    tsdb *w, *f;
    tsdb_sid id;
    OK(tsdb_open(dir, &o, &w));
    for (int s = 0; s < 4; s++) {
        char name[32];
        snprintf(name, sizeof name, "sensor_%d", s);
        OK(tsdb_series(w, name, &id));
        if (s < 3) /* sensor_3 has no rows: nothing else proves its record */
            insert_at(w, id, T0, 100);
    }
    OK(tsdb_flush(w));
    OK(open_reader(dir, 0, &f));
    OK(tsdb_series_lookup(f, "sensor_3", &id));
    tsdb_close(w);
    flip(dir, "SERIES", file_size(dir, "SERIES") - 1);
    OK(tsdb_open(dir, &o, &w)); /* cuts sensor_3 */
    int rc = 0;
    for (int i = 0; i < 40 && rc == 0; i++) {
        usleep(100000);
        rc = tsdb_refresh(f);
    }
    RC(rc, TSDB_ERR_CORRUPT); /* shorter than what it read */
    OK(tsdb_series(w, "sensor_X", &id)); /* the same id, in a record of the same size */
    OK(tsdb_flush(w));
    RC(tsdb_refresh(f), TSDB_ERR_CORRUPT);
    tsdb_close(f);
    OK(open_reader(dir, 0, &f)); /* a new reader reads what is there */
    RC(tsdb_series_lookup(f, "sensor_3", &id), TSDB_ERR_NOTFOUND);
    OK(tsdb_series_lookup(f, "sensor_X", &id));
    tsdb_close(f);
    tsdb_close(w);
    printf("  a follower notices SERIES cut below, or rewritten at, the last record it read\n");
    rm_rf(dir);
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
    test_refused_open_keeps_files(base);
    test_series_gap_follower(base);
    test_records_prove_series(base);
    test_records_huge_id(base);
    test_retention_damaged_index(base);
    test_damaged_part_no_starvation(base);
    test_wait_merges_damaged(base);
    test_wait_merges_stalled(base);
    test_wait_merges_failing(base);
    test_merge_barrier(base);
    test_merge_barrier_upper_tomb(base);
    test_merge_barrier_bottom(base);
    test_merge_barrier_bounded(base);
    test_follower_steady_creation(base);
    test_wal_torn_before_later(base);
    test_follower_series_rewritten(base);
    test_observe_damaged_runs(base);
    test_merge_index_damage(base);
    test_index_count_changes(base);
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
