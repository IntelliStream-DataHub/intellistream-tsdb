/*
 * Fail-stop after a storage error (see tsdb_failed in tsdb.h and eng_fail in
 * src/internal.h).
 *
 * The library's write, pwrite, fsync, fdatasync and renameat land here: the
 * test binary's definitions win over libc's. A test arms one failure: the
 * next matching call (by operation and a substring of the file's path) fails
 * with EIO, once, as a real disk error does; the call after it succeeds. A
 * failing write can first write half its bytes (a torn record), and a failing
 * fdatasync can lose what was written since the last successful sync of that
 * file (truncating it there), as the kernel may drop the dirty pages of a
 * failed sync. Each test then checks that nothing was acknowledged that the
 * disk lacks: the writers whose rows went through the failed call get
 * TSDB_ERR_IO, every later write too, and a reopen shows exactly the rows
 * that were acknowledged.
 *
 *   ./build/test_failstop [scratch dir]
 */
#define _GNU_SOURCE
#include "tsdb.h"

#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

static int failures;

#ifdef OLD_CODE /* to show the tests fail on code from before fail-stop, which lacks tsdb_failed */
int tsdb_failed(tsdb *db, char *msg, size_t len)
{
    (void)db;
    if (msg && len)
        msg[0] = '\0';
    return 0;
}
#endif

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

/* ------------------------------------------------------- fault injection */

enum { F_WRITE = 1, F_PWRITE = 2, F_FSYNC = 4, F_FDATASYNC = 8, F_RENAME = 16 };

static pthread_mutex_t f_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t f_cv = PTHREAD_COND_INITIALIZER;
static int f_op;          /* the armed operation, 0: none */
static char f_match[64];  /* substring of the path; "/" with F_FSYNC: the database directory itself */
static const char *f_dir; /* the database directory */
static bool f_partial;    /* a failing write writes half its bytes first */
static bool f_drop;       /* a failing fdatasync loses the bytes since the last successful one */
static int f_fired;
static int h_op;          /* hold the next such call on a WAL segment until h_go */
static int h_begun, h_go;

/* sizes at the last successful sync, by inode */
static struct {
    dev_t dev;
    ino_t ino;
    off_t size;
} synced[64];
static size_t nsynced;

static void path_of(int fd, char *buf, size_t len)
{
    char link[64];
    snprintf(link, sizeof link, "/proc/self/fd/%d", fd);
    ssize_t n = readlink(link, buf, len - 1);
    buf[n > 0 ? n : 0] = '\0';
}

/* Decides, once per matching call, whether it fails; holds it first if asked. */
static bool inject(int op, int fd, const char *name)
{
    char path[512];
    if (name)
        snprintf(path, sizeof path, "%s", name);
    else
        path_of(fd, path, sizeof path);
    pthread_mutex_lock(&f_lock);
    bool fail = false;
    if (f_op == op) {
        if (op == F_FSYNC && strcmp(f_match, "/") == 0)
            fail = f_dir && strcmp(path, f_dir) == 0;
        else
            fail = strstr(path, f_match) != NULL;
        if (fail) {
            f_op = 0;
            f_fired++;
        }
    }
    if (!fail && h_op == op && strstr(path, "/wal_")) {
        h_op = 0;
        int me = ++h_begun;
        pthread_cond_broadcast(&f_cv);
        while (h_go < me)
            pthread_cond_wait(&f_cv, &f_lock);
    }
    pthread_mutex_unlock(&f_lock);
    return fail;
}

static off_t *synced_slot(int fd)
{
    struct stat st;
    if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode))
        return NULL;
    for (size_t i = 0; i < nsynced; i++)
        if (synced[i].dev == st.st_dev && synced[i].ino == st.st_ino)
            return &synced[i].size;
    if (nsynced == sizeof synced / sizeof *synced)
        nsynced = 0; /* old segments are gone */
    synced[nsynced].dev = st.st_dev;
    synced[nsynced].ino = st.st_ino;
    synced[nsynced].size = 0;
    return &synced[nsynced++].size;
}

static int sync_call(int op, long sysno, int fd)
{
    if (inject(op, fd, NULL)) {
        pthread_mutex_lock(&f_lock);
        off_t *s = f_drop ? synced_slot(fd) : NULL;
        if (s && ftruncate(fd, *s) != 0)
            fprintf(stderr, "ftruncate: %s\n", strerror(errno));
        pthread_mutex_unlock(&f_lock);
        errno = EIO;
        return -1;
    }
    int r = (int)syscall(sysno, fd);
    if (r == 0) {
        pthread_mutex_lock(&f_lock);
        struct stat st;
        off_t *s = synced_slot(fd);
        if (s && fstat(fd, &st) == 0)
            *s = st.st_size;
        pthread_mutex_unlock(&f_lock);
    }
    return r;
}

int fsync(int fd) { return sync_call(F_FSYNC, SYS_fsync, fd); }
int fdatasync(int fd) { return sync_call(F_FDATASYNC, SYS_fdatasync, fd); }

ssize_t write(int fd, const void *buf, size_t n)
{
    if (fd > 2 && inject(F_WRITE, fd, NULL)) {
        if (f_partial && n > 1)
            syscall(SYS_write, fd, buf, n / 2);
        errno = EIO;
        return -1;
    }
    return syscall(SYS_write, fd, buf, n);
}

ssize_t pwrite(int fd, const void *buf, size_t n, off_t off)
{
    if (inject(F_PWRITE, fd, NULL)) {
        errno = EIO;
        return -1;
    }
    return syscall(SYS_pwrite64, fd, buf, n, off);
}

int renameat(int olddirfd, const char *oldpath, int newdirfd, const char *newpath)
{
    if (inject(F_RENAME, -1, newpath)) {
        errno = EIO;
        return -1;
    }
    return (int)syscall(SYS_renameat, olddirfd, oldpath, newdirfd, newpath);
}

static void arm(int op, const char *match, bool partial, bool drop)
{
    pthread_mutex_lock(&f_lock);
    f_op = op;
    snprintf(f_match, sizeof f_match, "%s", match);
    f_partial = partial;
    f_drop = drop;
    f_fired = 0;
    pthread_mutex_unlock(&f_lock);
}

static int fired(void)
{
    pthread_mutex_lock(&f_lock);
    int n = f_fired;
    f_op = 0;
    pthread_mutex_unlock(&f_lock);
    return n;
}

/* ------------------------------------------------------------- helpers */

static char dir[600];

static tsdb_options opts(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 1;
    o.wal_sync_ms = 0;
    o.flush_interval_ms = 1000000; /* flushes only when a test asks */
    o.buffer_rows = 1u << 20;
    o.merge_threads = 0;
    o.wal_max_batch_rows = 64;
    return o;
}

static tsdb *open_fresh(const char *name, tsdb_options o)
{
    char cmd[700];
    snprintf(dir, sizeof dir, "%.500s", name);
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (system(cmd) != 0)
        failures++;
    tsdb *db = NULL;
    OK(tsdb_open(dir, &o, &db));
    char real[PATH_MAX];
    if (realpath(dir, real)) {
        pthread_mutex_lock(&f_lock);
        f_dir = strdup(real);
        pthread_mutex_unlock(&f_lock);
    }
    return db;
}

static tsdb *reopen(tsdb *db)
{
    tsdb_close(db); /* may report the failure */
    tsdb_options o = opts();
    tsdb *n = NULL;
    OK(tsdb_open(dir, &o, &n));
    return n;
}

static bool is_failed(tsdb *db, const char *expect)
{
    char msg[256];
    int rc = tsdb_failed(db, msg, sizeof msg);
    if (rc == TSDB_ERR_IO && expect && !strstr(msg, expect)) {
        fprintf(stderr, "failure message \"%s\" lacks \"%s\"\n", msg, expect);
        failures++;
    }
    return rc == TSDB_ERR_IO;
}

/* Exactly the timestamps want[0..n) are in the series, in order. */
static void expect_rows(tsdb *db, const char *series, const int64_t *want, size_t n)
{
    uint32_t id;
    OK(tsdb_series_lookup(db, series, &id));
    tsdb_points p = {0};
    OK(tsdb_query_points(db, id, INT64_MIN, INT64_MAX, &p));
    bool same = p.n == n;
    for (size_t i = 0; same && i < n; i++)
        same = p.ts[i] == want[i];
    if (!same) {
        fprintf(stderr, "series %s: want %zu rows, have %zu:", series, n, p.n);
        for (size_t i = 0; i < p.n && i < 20; i++)
            fprintf(stderr, " %lld", (long long)p.ts[i]);
        fprintf(stderr, "\n");
        failures++;
    }
    tsdb_points_free(&p);
}

static void sleep_ms(int ms)
{
    struct timespec d = {ms / 1000, (ms % 1000) * 1000000L};
    nanosleep(&d, NULL);
}

/* --------------------------------------------------------------- tests */

typedef struct {
    tsdb *db;
    uint32_t id;
    int64_t ts;
    int rc;
} appender;

static void *append_main(void *arg)
{
    appender *a = arg;
    a->rc = tsdb_append(a->db, a->id, a->ts, (float)a->ts);
    return NULL;
}

/* The bug the same-key tests found: writers waiting in one group commit whose
 * fdatasync failed were acknowledged by the next writer's successful sync. */
static void test_group_commit(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-group", base);
    tsdb *db = open_fresh(name, opts());
    uint32_t id;
    OK(tsdb_series(db, "s", &id));

    /* X syncs and is held in its fdatasync; A and D append meanwhile, so the
     * next writer's batch holds both their rows */
    pthread_mutex_lock(&f_lock);
    h_op = F_FDATASYNC;
    h_begun = h_go = 0;
    pthread_mutex_unlock(&f_lock);
    appender x = {db, id, 1, 0}, a = {db, id, 2, 0}, d = {db, id, 3, 0};
    pthread_t tx, ta, td;
    pthread_create(&tx, NULL, append_main, &x);
    pthread_mutex_lock(&f_lock);
    while (h_begun < 1)
        pthread_cond_wait(&f_cv, &f_lock);
    pthread_mutex_unlock(&f_lock);
    pthread_create(&ta, NULL, append_main, &a);
    pthread_create(&td, NULL, append_main, &d);
    for (int i = 0; i < 5000; i++) { /* both in the memtable: their rows are in the WAL buffer */
        tsdb_stats st;
        tsdb_get_stats(db, &st);
        if (st.memtable_rows == 3)
            break;
        sleep_ms(1);
    }
    sleep_ms(50); /* and, most likely, waiting for the writer */
    arm(F_FDATASYNC, "/wal_", false, true);
    pthread_mutex_lock(&f_lock);
    h_go = 1;
    pthread_cond_broadcast(&f_cv);
    pthread_mutex_unlock(&f_lock);
    pthread_join(tx, NULL);
    pthread_join(ta, NULL);
    pthread_join(td, NULL);
    CHECK(fired() == 1);
    OK(x.rc);
    CHECK(a.rc == TSDB_ERR_IO);
    CHECK(d.rc == TSDB_ERR_IO);
    CHECK(is_failed(db, "WAL fdatasync"));
    CHECK(tsdb_append(db, id, 4, 4) == TSDB_ERR_IO);
    CHECK(tsdb_commit(db) == TSDB_ERR_IO);
    CHECK(tsdb_flush(db) == TSDB_ERR_IO);
    uint32_t other;
    CHECK(tsdb_series(db, "new", &other) == TSDB_ERR_IO);
    OK(tsdb_series(db, "s", &other)); /* existing series are still found */
    int64_t one[] = {1};
    expect_rows(db, "s", one, 1); /* reads keep working */

    db = reopen(db);
    CHECK(!is_failed(db, NULL));
    int64_t acked[4];
    size_t n = 0;
    appender *all[] = {&x, &a, &d};
    for (size_t i = 0; i < 3; i++)
        if (all[i]->rc == 0)
            acked[n++] = all[i]->ts;
    expect_rows(db, "s", acked, n);
    OK(tsdb_append(db, id, 5, 5)); /* writable again */
    tsdb_close(db);
    printf("  group commit: waiters of a failed fdatasync are not acknowledged\n");
}

/* A failed write may leave half a record; nothing may follow it, or replay
 * (which stops there) would lose acknowledged rows. */
static void test_torn_write(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-torn", base);
    tsdb *db = open_fresh(name, opts());
    uint32_t id;
    OK(tsdb_series(db, "s", &id));
    OK(tsdb_append(db, id, 1, 1));
    arm(F_WRITE, "/wal_", true, false);
    CHECK(tsdb_append(db, id, 2, 2) == TSDB_ERR_IO);
    CHECK(fired() == 1);
    int rc3 = tsdb_append(db, id, 3, 3);
    CHECK(rc3 == TSDB_ERR_IO);
    CHECK(is_failed(db, "WAL write"));
    db = reopen(db);
    int64_t want[] = {1, 3};
    expect_rows(db, "s", want, rc3 == 0 ? 2 : 1);
    tsdb_close(db);
    printf("  torn WAL record: no write after it, replay keeps what came before\n");
}

/* The directory fsync that makes new part names durable fails: the insert is
 * not acknowledged, and the database takes no more writes. */
static void test_dir_fsync(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-dir", base);
    tsdb *db = open_fresh(name, opts());
    uint32_t id;
    OK(tsdb_series(db, "s", &id));
    int64_t ts[100];
    float v[100];
    for (int i = 0; i < 100; i++) {
        ts[i] = i;
        v[i] = (float)i;
    }
    OK(tsdb_insert_series(db, id, ts, v, 100)); /* a part, straight away */
    arm(F_FSYNC, "/", false, false);
    for (int i = 0; i < 100; i++)
        ts[i] = 1000 + i;
    CHECK(tsdb_insert_series(db, id, ts, v, 100) == TSDB_ERR_IO);
    CHECK(fired() == 1);
    CHECK(is_failed(db, "fsync of the directory"));
    CHECK(tsdb_insert_series(db, id, ts, v, 100) == TSDB_ERR_IO);
    CHECK(tsdb_append(db, id, 5000, 1) == TSDB_ERR_IO);
    CHECK(tsdb_optimize(db) == TSDB_ERR_IO);
    db = reopen(db); /* the unacknowledged part may or may not be there; the first one is */
    uint32_t sid;
    OK(tsdb_series_lookup(db, "s", &sid));
    tsdb_points p = {0};
    OK(tsdb_query_points(db, sid, 0, 100, &p));
    CHECK(p.n == 100);
    tsdb_points_free(&p);
    tsdb_close(db);
    printf("  failed directory fsync after publishing a part\n");
}

/* MANIFEST is rewritten in place: a failed rewrite may leave it torn. */
static void test_manifest(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-manifest", base);
    tsdb *db = open_fresh(name, opts());
    uint32_t id;
    OK(tsdb_series(db, "s", &id));
    OK(tsdb_append(db, id, 1, 1));
    arm(F_PWRITE, "/MANIFEST", false, false);
    tsdb_flush(db); /* the rows are durable in the WAL either way */
    CHECK(fired() == 1);
    CHECK(is_failed(db, "MANIFEST"));
    CHECK(tsdb_append(db, id, 2, 2) == TSDB_ERR_IO);
    CHECK(tsdb_flush(db) == TSDB_ERR_IO);
    db = reopen(db);
    int64_t want[] = {1};
    expect_rows(db, "s", want, 1);
    tsdb_close(db);
    printf("  failed MANIFEST rewrite\n");
}

/* A torn series dictionary record ends the dictionary for the next open. */
static void test_series_dict(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-series", base);
    tsdb *db = open_fresh(name, opts());
    uint32_t a, b, c;
    OK(tsdb_series(db, "a", &a));
    OK(tsdb_append(db, a, 1, 1));
    arm(F_WRITE, "/SERIES", true, false);
    CHECK(tsdb_series(db, "b", &b) == TSDB_ERR_IO);
    CHECK(fired() == 1);
    int rc = tsdb_series(db, "c", &c);
    CHECK(rc == TSDB_ERR_IO);
    CHECK(is_failed(db, "series dictionary"));
    if (rc == 0)
        OK(tsdb_append(db, c, 7, 7));
    db = reopen(db);
    int64_t want[] = {1};
    expect_rows(db, "a", want, 1);
    if (rc == 0) { /* acknowledged: must be there */
        int64_t seven[] = {7};
        expect_rows(db, "c", seven, 1);
    }
    tsdb_close(db);
    printf("  failed series dictionary append\n");
}

/* CATALOG maps series to volumes; a torn record shifts every later one. */
static void test_catalog(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-catalog", base);
    tsdb *db = open_fresh(name, opts());
    uint32_t a, b, c;
    OK(tsdb_series(db, "a", &a));
    arm(F_WRITE, "/CATALOG", true, false);
    CHECK(tsdb_series(db, "b", &b) == TSDB_ERR_IO);
    CHECK(fired() == 1);
    int rc = tsdb_series(db, "c", &c);
    CHECK(rc == TSDB_ERR_IO);
    CHECK(is_failed(db, "CATALOG"));
    if (rc == 0)
        OK(tsdb_append(db, c, 7, 7));
    db = reopen(db);
    uint32_t got;
    OK(tsdb_series_lookup(db, "a", &got));
    if (rc == 0) {
        int64_t seven[] = {7};
        expect_rows(db, "c", seven, 1);
    }
    tsdb_close(db);
    printf("  failed CATALOG append\n");
}

/* wal_sync_ms > 0: the flusher's sync fails with no writer waiting; the next
 * write learns of it. */
static void test_background_sync(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-async", base);
    tsdb_options o = opts();
    o.wal_sync_ms = 20;
    tsdb *db = open_fresh(name, o);
    uint32_t id;
    OK(tsdb_series(db, "s", &id));
    OK(tsdb_append(db, id, 1, 1));
    OK(tsdb_commit(db));
    arm(F_FDATASYNC, "/wal_", false, true);
    OK(tsdb_append(db, id, 2, 2)); /* not durable yet: wal_sync_ms promises no more */
    for (int i = 0; i < 2000 && !is_failed(db, NULL); i++)
        sleep_ms(1);
    CHECK(is_failed(db, "WAL fdatasync"));
    CHECK(tsdb_append(db, id, 3, 3) == TSDB_ERR_IO);
    CHECK(tsdb_commit(db) == TSDB_ERR_IO);
    db = reopen(db);
    int64_t want[] = {1};
    expect_rows(db, "s", want, 1);
    tsdb_close(db);
    printf("  failed background WAL sync\n");
}

int main(int argc, char **argv)
{
    const char *base = argc > 1 ? argv[1] : "/tmp";
    char root[300];
    snprintf(root, sizeof root, "%s/tsdb-failstop-%d", base, (int)getpid());
    mkdir(root, 0755);
    printf("fail-stop after storage errors\n");
    test_group_commit(root);
    test_torn_write(root);
    test_dir_fsync(root);
    test_manifest(root);
    test_series_dict(root);
    test_catalog(root);
    test_background_sync(root);
    if (!failures) {
        char cmd[512];
        snprintf(cmd, sizeof cmd, "rm -rf '%s'", root);
        if (system(cmd) != 0)
            failures++;
    }
    printf(failures ? "FAILED: %d checks\n" : "all tests passed\n", failures);
    return failures != 0;
}
