/* SPDX-License-Identifier: AGPL-3.0-only */
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

#include <dirent.h>
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

#include "portable.h"

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

enum { F_WRITE = 1, F_PWRITE = 2, F_FSYNC = 4, F_FDATASYNC = 8, F_RENAME = 16, F_UNLINK = 32 };

static pthread_mutex_t f_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t f_cv = PTHREAD_COND_INITIALIZER;
static int f_op;          /* the armed operation, 0: none */
static char f_match[64];  /* substring of the path; "/" with F_FSYNC: the database directory itself */
static char *f_dir; /* the database directory */
static bool f_partial;    /* a failing write writes half its bytes first */
static bool f_drop;       /* a failing fdatasync loses the bytes since the last successful one */
static int f_fired;
static int f_skip;        /* matching calls to let through before the one that fails */
static int h_op;          /* hold the next such call on a path with h_match until h_go */
static char h_match[64];
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
    ssize_t n = fd_path(fd, buf, len);
    if (n <= 0)
        buf[0] = '\0';
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
        if (fail && f_skip > 0) {
            f_skip--;
            fail = false;
        } else if (fail) {
            f_op = 0;
            f_fired++;
        }
    }
    if (h_op == op && strstr(path, h_match)) { /* held even when it is to fail */
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

/* the real fsync or fdatasync (Linux: the system call, macOS: the next definition of the name) */
static int real_sync(long sysno, int fd)
{
#ifdef __APPLE__
    return sysno == SYS_fsync ? REAL(fsync)(fd) : REAL(fdatasync)(fd);
#else
    return (int)syscall(sysno, fd);
#endif
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
    int r = real_sync(sysno, fd);
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

static ssize_t real_write(int fd, const void *buf, size_t n)
{
#ifdef __APPLE__
    return REAL(write)(fd, buf, n);
#else
    return syscall(SYS_write, fd, buf, n);
#endif
}

ssize_t write(int fd, const void *buf, size_t n)
{
    if (fd > 2 && inject(F_WRITE, fd, NULL)) {
        if (f_partial && n > 1)
            real_write(fd, buf, n / 2);
        errno = EIO;
        return -1;
    }
    return real_write(fd, buf, n);
}

ssize_t pwrite(int fd, const void *buf, size_t n, off_t off)
{
    if (inject(F_PWRITE, fd, NULL)) {
        errno = EIO;
        return -1;
    }
#ifdef __APPLE__
    return REAL(pwrite)(fd, buf, n, off);
#else
    return syscall(SYS_pwrite64, fd, buf, n, off);
#endif
}

int renameat(int olddirfd, const char *oldpath, int newdirfd, const char *newpath)
{
    if (inject(F_RENAME, -1, newpath)) {
        errno = EIO;
        return -1;
    }
#ifdef __APPLE__
    return REAL(renameat)(olddirfd, oldpath, newdirfd, newpath);
#else
    return (int)syscall(SYS_renameat, olddirfd, oldpath, newdirfd, newpath);
#endif
}

int unlinkat(int dirfd, const char *path, int flags)
{
    if (inject(F_UNLINK, -1, path)) {
        errno = EIO;
        return -1;
    }
#ifdef __APPLE__
    return REAL(unlinkat)(dirfd, path, flags);
#else
    return (int)syscall(SYS_unlinkat, dirfd, path, flags);
#endif
}

static void arm(int op, const char *match, bool partial, bool drop)
{
    pthread_mutex_lock(&f_lock);
    f_op = op;
    snprintf(f_match, sizeof f_match, "%s", match);
    f_partial = partial;
    f_drop = drop;
    f_fired = 0;
    f_skip = 0;
    pthread_mutex_unlock(&f_lock);
}

static void arm_after(int skip, int op, const char *match)
{
    arm(op, match, false, false);
    pthread_mutex_lock(&f_lock);
    f_skip = skip;
    pthread_mutex_unlock(&f_lock);
}

static void hold(int op, const char *match)
{
    pthread_mutex_lock(&f_lock);
    h_op = op;
    snprintf(h_match, sizeof h_match, "%s", match);
    h_begun = h_go = 0;
    pthread_mutex_unlock(&f_lock);
}

static void wait_held(void)
{
    pthread_mutex_lock(&f_lock);
    while (h_begun < 1)
        pthread_cond_wait(&f_cv, &f_lock);
    pthread_mutex_unlock(&f_lock);
}

static void release(void)
{
    pthread_mutex_lock(&f_lock);
    h_go = 1;
    pthread_cond_broadcast(&f_cv);
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

/* whether the armed call has failed, leaving it armed */
static bool fired_yet(void)
{
    pthread_mutex_lock(&f_lock);
    bool yes = f_fired > 0;
    pthread_mutex_unlock(&f_lock);
    return yes;
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

/* Base partitions only: a test that counts partitions or names their parts (p1_) must not see a slow series
 * move to a longer class, nor the CLASSES file that move creates */
static tsdb_options opts_days(void)
{
    tsdb_options o = opts();
    o.partition_classes = 0;
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
        free(f_dir);
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
    /* the per-volume flag that tsdbd --metrics-port reports as tsdb_failed agrees */
    int any = 0;
    for (uint32_t i = 0; i < tsdb_volume_count(db); i++) {
        tsdb_volume_stats vs;
        if (tsdb_get_volume_stats(db, i, &vs) == 0)
            any |= vs.failed;
    }
    if (any != (rc == TSDB_ERR_IO)) {
        fprintf(stderr, "tsdb_get_volume_stats: failed=%d, tsdb_failed=%d\n", any, rc);
        failures++;
    }
    return rc == TSDB_ERR_IO;
}

/* Exactly the timestamps want[0..n) are in the series, in order. */
static void expect_rows(tsdb *db, const char *series, const int64_t *want, size_t n)
{
    tsdb_sid id;
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

/* files in the database directory whose names match the grep pattern */
static int count_files(const char *pattern)
{
    char cmd[800], out[64] = "";
    snprintf(cmd, sizeof cmd, "ls '%s' | grep -c '%s'", dir, pattern);
    FILE *f = popen(cmd, "r");
    if (f) {
        if (!fgets(out, sizeof out, f))
            out[0] = '\0';
        pclose(f);
    }
    return atoi(out);
}

static void sleep_ms(int ms)
{
    struct timespec d = {ms / 1000, (ms % 1000) * 1000000L};
    nanosleep(&d, NULL);
}

/* --------------------------------------------------------------- tests */

typedef struct {
    tsdb *db;
    tsdb_sid id;
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
    tsdb_sid id;
    OK(tsdb_series(db, "s", &id));

    /* X syncs and is held in its fdatasync; A and D append meanwhile, so the
     * next writer's batch holds both their rows */
    hold(F_FDATASYNC, "/wal_");
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
    tsdb_sid other;
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
    tsdb_sid id;
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
    tsdb_sid id;
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
    tsdb_sid sid;
    OK(tsdb_series_lookup(db, "s", &sid));
    tsdb_points p = {0};
    OK(tsdb_query_points(db, sid, 0, 100, &p));
    CHECK(p.n == 100);
    tsdb_points_free(&p);
    tsdb_close(db);
    printf("  failed directory fsync after publishing a part\n");
}

/* A small write into the keys of a large write that has not published yet waits for its publish (lw_fence). When
 * that publish fails the engine, the small write must be refused: with an asynchronous WAL nothing after the wait
 * checked, so it went into the memtable and returned 0 from a failed engine, and its row was gone after a reopen. */
void tsdb_test_precommit(tsdb *t, void (*fn)(void *), void *arg); /* the library's test hook (src/internal.h) */

typedef struct {
    appender small; /* a key of the large write */
    pthread_t th;
    bool started;
    int done_early;
} fence_fail;

static void *fence_small_main(void *arg)
{
    appender *a = arg;
    int64_t ts = a->ts;
    float v = 7;
    int rc = tsdb_insert_series(a->db, a->id, &ts, &v, 1);
    __atomic_store_n(&a->rc, rc, __ATOMIC_SEQ_CST);
    return NULL;
}

static void fence_fail_window(void *arg)
{
    fence_fail *x = arg;
    x->small.rc = 1; /* running */
    x->started = pthread_create(&x->th, NULL, fence_small_main, &x->small) == 0;
    sleep_ms(300);
    x->done_early = __atomic_load_n(&x->small.rc, __ATOMIC_SEQ_CST) != 1;
    arm(F_FSYNC, "/", false, false); /* the directory fsync of this publish */
}

static void test_fence_failure(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-fence", base);
    tsdb_options o = opts();
    o.wal_sync_ms = 100; /* asynchronous: no WAL commit refuses the write */
    tsdb *db = open_fresh(name, o);
    tsdb_sid id;
    OK(tsdb_series(db, "s", &id));
    int64_t ts[100];
    float v[100];
    for (int i = 0; i < 100; i++) {
        ts[i] = i;
        v[i] = 1;
    }
    fence_fail x = {.small = {db, id, 50, 0}};
    tsdb_test_precommit(db, fence_fail_window, &x);
    CHECK(tsdb_insert_series(db, id, ts, v, 100) == TSDB_ERR_IO); /* large: 100 >= wal_max_batch_rows */
    tsdb_test_precommit(db, NULL, NULL);
    CHECK(fired() == 1);
    CHECK(is_failed(db, "fsync of the directory"));
    CHECK(x.started);
    if (x.started)
        pthread_join(x.th, NULL);
    CHECK(!x.done_early); /* it waited for the large write */
    if (x.small.rc != TSDB_ERR_IO) {
        fprintf(stderr, "a small write that waited for a large write was acknowledged (%d) after its publish failed "
                        "the engine\n", x.small.rc);
        failures++;
    }
    tsdb_close(db);
    printf("  a small write waiting for a large write whose publish fails\n");
}

/* A flush fails on a part file: an error that fails only the call, the
 * memtable waits to be retried. A large insert after it flushes the memtables
 * first, so older small writes cannot shadow its part: before the fix, the
 * retry wrote only the failed memtable and left the active one, which queries
 * read as newer than every part (the jepsen eio nemesis found this). */
static void test_flush_retry(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-flush", base);
    tsdb *db = open_fresh(name, opts());
    tsdb_sid id;
    OK(tsdb_series(db, "s", &id));
    int64_t ts[100];
    float v[100];
    for (int i = 0; i < 100; i++)
        ts[i] = i;
    for (int i = 0; i < 100; i++)
        v[i] = 1;
    OK(tsdb_insert_series(db, id, ts, v, 10)); /* memtable */
    arm(F_FDATASYNC, "/tmp_", false, false);
    CHECK(tsdb_flush(db) == TSDB_ERR_IO);
    CHECK(fired() == 1);
    CHECK(!is_failed(db, NULL));
    for (int i = 0; i < 100; i++)
        v[i] = 2;
    OK(tsdb_insert_series(db, id, ts, v, 10)); /* the new active memtable */
    for (int i = 0; i < 100; i++)
        v[i] = 3;
    OK(tsdb_insert_series(db, id, ts, v, 100)); /* a part, newer than both */
    for (int round = 0; round < 2; round++) {
        tsdb_points p = {0};
        tsdb_sid sid;
        OK(tsdb_series_lookup(db, "s", &sid));
        OK(tsdb_query_points(db, sid, 0, 100, &p));
        CHECK(p.n == 100);
        int stale = 0;
        for (size_t i = 0; i < p.n; i++)
            stale += p.values[i] != 3;
        if (stale)
            fprintf(stderr, "%s: %d of %zu rows older than the last write\n", round ? "after reopen" : "live", stale, p.n);
        CHECK(stale == 0);
        tsdb_points_free(&p);
        if (round == 0)
            db = reopen(db);
    }
    tsdb_close(db);
    printf("  failed flush, then a large insert over the same keys\n");
}

/* MANIFEST is rewritten in place: a failed rewrite may leave it torn. */
static void test_manifest(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-manifest", base);
    tsdb *db = open_fresh(name, opts());
    tsdb_sid id;
    OK(tsdb_series(db, "s", &id));
    OK(tsdb_append(db, id, 1, 1));
    arm(F_PWRITE, "/MANIFEST", false, false);
    CHECK(tsdb_flush(db) == TSDB_ERR_IO); /* the rows are durable in the WAL either way */
    CHECK(fired() == 1);
    CHECK(is_failed(db, "MANIFEST"));
    CHECK(tsdb_append(db, id, 2, 2) == TSDB_ERR_IO);
    CHECK(tsdb_flush(db) == TSDB_ERR_IO);
    db = reopen(db);
    int64_t want[] = {1};
    expect_rows(db, "s", want, 1);
    tsdb_close(db);
    printf("  failed MANIFEST rewrite\n");

    /* The rewrite after a flush's parts are listed fails (the one naming the new segment succeeded): the flushed
     * segment stays, since MANIFEST on disk still names it and a follower may be tailing it. */
    snprintf(name, sizeof name, "%s/failstop-manifest-flush", base);
    db = open_fresh(name, opts());
    OK(tsdb_series(db, "s", &id));
    OK(tsdb_append(db, id, 1, 1));
    arm_after(1, F_PWRITE, "/MANIFEST");
    CHECK(tsdb_flush(db) == TSDB_ERR_IO);
    CHECK(fired() == 1);
    CHECK(is_failed(db, "MANIFEST"));
    CHECK(count_files("^wal_") == 2);
    db = reopen(db);
    expect_rows(db, "s", want, 1);
    tsdb_close(db);
    printf("  failed MANIFEST rewrite after a flush\n");

    /* The same failure while publishing an insert's parts: they are committed
     * (renamed, directory synced) and listed before MANIFEST is rewritten, so
     * the insert succeeded and the parts must stay. Discarding them freed
     * parts that queries still found in the list. */
    snprintf(name, sizeof name, "%s/failstop-manifest-insert", base);
    db = open_fresh(name, opts());
    OK(tsdb_series(db, "s", &id));
    int64_t ts[100];
    float v[100];
    for (int i = 0; i < 100; i++) {
        ts[i] = 10 + i;
        v[i] = (float)i;
    }
    arm(F_PWRITE, "/MANIFEST", false, false);
    OK(tsdb_insert_series(db, id, ts, v, 100)); /* a part, straight away */
    CHECK(fired() == 1);
    CHECK(is_failed(db, "MANIFEST"));
    tsdb_points p = {0};
    OK(tsdb_query_points(db, id, 0, 1000, &p));
    CHECK(p.n == 100 && p.ts[0] == 10 && p.values[99] == 99);
    tsdb_points_free(&p);
    CHECK(tsdb_insert_series(db, id, ts, v, 100) == TSDB_ERR_IO);
    db = reopen(db);
    expect_rows(db, "s", ts, 100);
    tsdb_close(db);
    printf("  failed MANIFEST rewrite after an insert's commit\n");
}

/* A torn series dictionary record ends the dictionary for the next open. */
static void test_series_dict(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-series", base);
    tsdb *db = open_fresh(name, opts());
    tsdb_sid a, b, c;
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

/* The dictionaries' test hook (src/internal.h, DICT_AT_*): fails the add of one name in memory, as if memory ran
 * out after its record was written, and holds the creator whose append failed before it fails the engine. */
void tsdb_test_dict_fault(tsdb *t, int (*fn)(void *arg, int point, const char *name), void *arg);
enum { AT_ADD = 1, AT_FAILED = 2 };
static char df_name[32];     /* the name the hook acts on */
static int df_fail_add;      /* the next add of it fails */
static int df_hold, df_held, df_go; /* hold its creator after the failed append */

static int dict_fault(void *arg, int point, const char *name)
{
    (void)arg;
    int fail = 0;
    pthread_mutex_lock(&f_lock);
    if (strcmp(name, df_name) == 0) {
        if (point == AT_ADD && df_fail_add) {
            df_fail_add = 0;
            fail = 1;
        } else if (point == AT_FAILED && df_hold) {
            df_hold = 0;
            df_held = 1;
            pthread_cond_broadcast(&f_cv);
            while (!df_go)
                pthread_cond_wait(&f_cv, &f_lock);
        }
    }
    pthread_mutex_unlock(&f_lock);
    return fail;
}

static void dict_fault_arm(const char *name, bool fail_add, bool hold_failed)
{
    pthread_mutex_lock(&f_lock);
    snprintf(df_name, sizeof df_name, "%s", name);
    df_fail_add = fail_add;
    df_hold = hold_failed;
    df_held = df_go = 0;
    pthread_mutex_unlock(&f_lock);
}

/* The record of a new name is durable, but adding it to the table in memory fails (no memory). The next name
 * would get its id in memory and the one after it in the file: after a reopen, its rows would be another
 * series'. Such a dictionary takes no more names, and the engine fails, as after a torn record. */
static void test_series_dict_add(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-series-add", base);
    tsdb *db = open_fresh(name, opts());
    tsdb_sid a, b, c;
    OK(tsdb_series(db, "a", &a));
    OK(tsdb_append(db, a, 1, 1));
    tsdb_test_dict_fault(db, dict_fault, NULL);
    dict_fault_arm("b", true, false);
    int rb = tsdb_series(db, "b", &b);
    CHECK(rb == TSDB_ERR_IO);
    CHECK(is_failed(db, "series dictionary"));
    int rc = tsdb_series(db, "c", &c);
    CHECK(rc == TSDB_ERR_IO);
    if (rc == 0)
        OK(tsdb_append(db, c, 7, 7));
    tsdb_test_dict_fault(db, NULL, NULL);
    db = reopen(db);
    int64_t want[] = {1};
    expect_rows(db, "a", want, 1);
    if (rc == 0) { /* acknowledged: must be there, under its own name */
        int64_t seven[] = {7};
        expect_rows(db, "c", seven, 1);
    }
    tsdb_close(db);
    printf("  a name whose record is durable but not in memory fails the engine\n");
}

struct creator {
    tsdb *db;
    const char *name;
    tsdb_sid id;
    int rc;
};

static void *create_main(void *arg)
{
    struct creator *c = arg;
    c->rc = tsdb_series(c->db, c->name, &c->id);
    return NULL;
}

/* A creator refused because an earlier append failed (the dictionary is broken) must not fail the engine itself:
 * it knows no cause, and the first failure wins the message. The creator whose append failed reports its own. */
static void test_series_dict_cause(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-series-cause", base);
    tsdb *db = open_fresh(name, opts());
    tsdb_sid a;
    OK(tsdb_series(db, "a", &a));
    tsdb_test_dict_fault(db, dict_fault, NULL);
    dict_fault_arm("b", true, true);
    struct creator cb = {.db = db, .name = "b", .rc = 1}, cc = {.db = db, .name = "c", .rc = 1};
    pthread_t tb, tc;
    CHECK(pthread_create(&tb, NULL, create_main, &cb) == 0);
    pthread_mutex_lock(&f_lock);
    while (!df_held)
        pthread_cond_wait(&f_cv, &f_lock);
    pthread_mutex_unlock(&f_lock);
    CHECK(pthread_create(&tc, NULL, create_main, &cc) == 0); /* refused: the dictionary is broken */
    pthread_join(tc, NULL);
    CHECK(cc.rc == TSDB_ERR_IO);
    pthread_mutex_lock(&f_lock);
    df_go = 1;
    pthread_cond_broadcast(&f_cv);
    pthread_mutex_unlock(&f_lock);
    pthread_join(tb, NULL);
    CHECK(cb.rc == TSDB_ERR_IO);
    CHECK(is_failed(db, strerror(ENOMEM)));
    tsdb_test_dict_fault(db, NULL, NULL);
    tsdb_close(db);
    printf("  the creator whose append failed names the cause of the failure\n");
}

/* A volume add writes the volume's META (its number, which ids carry) before the VOLUMES line names the
 * directory. A failed write leaves no line and fails just the add: nothing relies on that META yet, so the writer
 * goes on, and the add tried again completes the directory, under the number it gets then. */
static void test_volume_meta(const char *base)
{
    char name[600], vol[700], cmd[800];
    snprintf(name, sizeof name, "%s/failstop-volmeta", base);
    snprintf(vol, sizeof vol, "%s/failstop-volmeta-v1", base);
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", vol);
    if (system(cmd) != 0)
        failures++;
    tsdb *db = open_fresh(name, opts());
    tsdb_sid a, b;
    OK(tsdb_series(db, "a", &a));
    OK(tsdb_append(db, a, 1, 1));
    arm(F_WRITE, "-v1/META.tmp", true, false);
    CHECK(tsdb_series_at(db, "b", vol, &b) == TSDB_ERR_IO);
    CHECK(fired() == 1);
    CHECK(tsdb_volume_count(db) == 1);
    CHECK(tsdb_failed(db, NULL, 0) == 0);
    OK(tsdb_append(db, a, 2, 2));
    OK(tsdb_series_at(db, "b", vol, &b));
    CHECK(b == TSDB_SID(1, 0) && tsdb_volume_count(db) == 2);
    OK(tsdb_append(db, b, 7, 7));
    db = reopen(db);
    int64_t one_two[] = {1, 2}, seven[] = {7};
    expect_rows(db, "a", one_two, 2);
    expect_rows(db, "b", seven, 1);
    tsdb_close(db);
    if (system(cmd) != 0)
        failures++;
    printf("  failed volume META write fails just the add\n");
}

struct dir_adder {
    const char *root, *vol;
    uint32_t index;
    int rc;
};

static void *dir_add_main(void *arg)
{
    struct dir_adder *a = arg;
    a->rc = tsdb_volume_add_dir(a->root, a->vol, &a->index);
    return NULL;
}

/* Two adds of one directory at once (two CLIs; or one directory reached by two paths, from two hosts) each write
 * its META outside the VOLUMES lock. With one shared META.tmp, the second add's rename took the file the first was
 * still writing, and the first one's rename then failed (or, had it died, left META empty). Each writes a temporary
 * of its own now: both adds succeed with the same number, and META says it. */
static void test_volume_meta_race(const char *base)
{
    char name[600], vol[700], cmd[800], meta[800];
    snprintf(name, sizeof name, "%s/failstop-volrace", base);
    snprintf(vol, sizeof vol, "%s/failstop-volrace-v1", base);
    snprintf(meta, sizeof meta, "%s/META", vol);
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", vol);
    if (system(cmd) != 0)
        failures++;
    tsdb *db = open_fresh(name, opts());
    OK(tsdb_close(db));
    struct dir_adder a = {.root = name, .vol = vol, .rc = 1}, b = {.root = name, .vol = vol, .rc = 1};
    hold(F_WRITE, "-v1/META.tmp");
    pthread_t ta, tb;
    CHECK(pthread_create(&ta, NULL, dir_add_main, &a) == 0);
    wait_held(); /* a has created its temporary and is about to write it */
    CHECK(pthread_create(&tb, NULL, dir_add_main, &b) == 0);
    pthread_join(tb, NULL);
    release();
    pthread_join(ta, NULL);
    CHECK(a.rc == 0 && b.rc == 0);
    CHECK(a.index == 1 && b.index == 1);
    char txt[256] = {0};
    FILE *f = fopen(meta, "r");
    if (f) {
        if (fread(txt, 1, sizeof txt - 1, f) == 0)
            txt[0] = '\0';
        fclose(f);
    }
    CHECK(strstr(txt, "volume=1\n") != NULL);
    tsdb_options o = opts();
    db = NULL;
    OK(tsdb_open(name, &o, &db));
    if (db) {
        CHECK(tsdb_volume_count(db) == 2);
        tsdb_sid s;
        OK(tsdb_series_at(db, "s", vol, &s));
        CHECK(s == TSDB_SID(1, 0));
        OK(tsdb_close(db));
    }
    if (system(cmd) != 0)
        failures++;
    printf("  two adds of one volume at once each write their own META temporary\n");
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
    tsdb_sid id;
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
    CHECK(tsdb_append(db, id, 4, 4) == TSDB_ERR_IO); /* the WAL reports the error once; the engine keeps it */
    db = reopen(db);
    int64_t want[] = {1};
    expect_rows(db, "s", want, 1);
    tsdb_close(db);
    printf("  failed background WAL sync\n");
}

/* ------------------------------------------------- tests after review */

static int64_t count_rows(tsdb *db, const char *series)
{
    tsdb_sid id;
    if (tsdb_series_lookup(db, series, &id) != 0)
        return 0;
    tsdb_points p = {0};
    OK(tsdb_query_points(db, id, INT64_MIN, INT64_MAX, &p));
    int64_t n = (int64_t)p.n;
    tsdb_points_free(&p);
    return n;
}

/* n rows from ts0 on; with split, the second half one day later (another partition) */
static int insert_n(tsdb *db, tsdb_sid id, int64_t ts0, int n, bool split)
{
    int64_t ts[200];
    float v[200];
    for (int i = 0; i < n; i++) {
        ts[i] = ts0 + i + (split && i >= n / 2 ? 86400000 : 0);
        v[i] = (float)i;
    }
    return tsdb_insert_series(db, id, ts, v, (size_t)n);
}

typedef struct {
    tsdb *db;
    tsdb_sid series;
    const char *name;
    uint32_t code;
    int rc;
} stater;

static void *state_main(void *arg)
{
    stater *s = arg;
    s->rc = tsdb_state(s->db, s->series, s->name, &s->code);
    return NULL;
}

/* A creator waiting for the dictionary lock while another one's append fails
 * must not append after the torn record: the open would end the dictionary
 * there and give its acknowledged code to another name. SERIES is guarded by
 * the volume's create lock, STATES only by the dictionary's. */
static void test_state_race(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-states", base);
    tsdb *db = open_fresh(name, opts());
    tsdb_sid mode;
    uint32_t idle;
    OK(tsdb_series_create(db, "mode", TSDB_STATE, NULL, &mode));
    OK(tsdb_state(db, mode, "idle", &idle));
    arm(F_WRITE, "/STATES", true, false);
    hold(F_WRITE, "/STATES");
    stater a = {db, mode, "a", 0, 0}, b = {db, mode, "b", 0, 0};
    pthread_t ta, tb;
    pthread_create(&ta, NULL, state_main, &a);
    wait_held(); /* a holds the dictionary lock in its failing write */
    pthread_create(&tb, NULL, state_main, &b);
    sleep_ms(100); /* b has passed the engine's check and waits for the lock */
    release();
    pthread_join(ta, NULL);
    pthread_join(tb, NULL);
    CHECK(a.rc == TSDB_ERR_IO);
    CHECK(fired() == 1);
    CHECK(b.rc == TSDB_ERR_IO);
    CHECK(is_failed(db, "state dictionary"));
    db = reopen(db);
    uint32_t got;
    OK(tsdb_state_lookup(db, mode, "idle", &got));
    CHECK(got == idle);
    if (b.rc == 0) { /* acknowledged: must be there, with its code */
        CHECK(tsdb_state_lookup(db, mode, "b", &got) == 0 && got == b.code);
    }
    tsdb_close(db);
    printf("  failed state dictionary append with a second creator waiting\n");
}

/* The name of the first file in dir that starts with prefix and ends with suffix; false if none. */
static bool find_file(const char *prefix, const char *suffix, char *out, size_t cap)
{
    DIR *d = opendir(dir);
    struct dirent *e;
    bool found = false;
    while (d && !found && (e = readdir(d))) {
        size_t l = strlen(e->d_name), sl = strlen(suffix);
        if (strncmp(e->d_name, prefix, strlen(prefix)) == 0 && l >= sl && strcmp(e->d_name + l - sl, suffix) == 0) {
            snprintf(out, cap, "%s/%s", dir, e->d_name);
            found = true;
        }
    }
    if (d)
        closedir(d);
    return found;
}

/* A failed insert over two partitions whose intent is left on disk, the database closed. */
static void intent_left(const char *name)
{
    tsdb *db = open_fresh(name, opts_days());
    tsdb_sid id;
    OK(tsdb_series(db, "s", &id));
    arm(F_FSYNC, "/", false, false);
    CHECK(insert_n(db, id, 0, 100, true) == TSDB_ERR_IO);
    CHECK(fired() == 1);
    tsdb_close(db);
    char path[PATH_MAX];
    CHECK(find_file("txn_", ".intent", path, sizeof path));
}

static int64_t rows_after_open(void)
{
    tsdb_options o = opts();
    tsdb *db = NULL;
    OK(tsdb_open(dir, &o, &db));
    if (!db)
        return -1;
    int64_t n = count_rows(db, "s");
    tsdb_close(db);
    return n;
}

/* One insert over two partitions commits through an intent: all or nothing. */
static void test_intent(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-intent", base);

    /* the directory fsync that makes the intent durable fails: committed or not, never half */
    tsdb *db = open_fresh(name, opts_days());
    tsdb_sid id;
    OK(tsdb_series(db, "s", &id));
    arm(F_FSYNC, "/", false, false);
    CHECK(insert_n(db, id, 0, 100, true) == TSDB_ERR_IO);
    CHECK(fired() == 1);
    CHECK(is_failed(db, "intent"));
    CHECK(insert_n(db, id, 1000, 100, true) == TSDB_ERR_IO);
    db = reopen(db);
    int64_t n = count_rows(db, "s");
    CHECK(n == 0 || n == 100);
    tsdb_close(db);

    /* the rename of the second partition's part fails after the intent is durable: the open rolls it forward,
     * so its tmp file must still be there */
    db = open_fresh(name, opts_days());
    OK(tsdb_series(db, "s", &id));
    arm(F_RENAME, "p1_", false, false); /* renameat gets the bare name */
    CHECK(insert_n(db, id, 0, 100, true) == TSDB_ERR_IO);
    CHECK(fired() == 1);
    CHECK(is_failed(db, "rename of a new part"));
    db = reopen(db);
    CHECK(count_rows(db, "s") == 100);
    tsdb_close(db);

    /* after the failed directory fsync the intent's name survived a power loss, one tmp name did not: rolling
     * the other one forward would publish half the insert */
    char path[PATH_MAX];
    intent_left(name);
    CHECK(find_file("tmp_", ".part", path, sizeof path) && unlink(path) == 0);
    CHECK(rows_after_open() == 0);
    CHECK(!find_file("txn_", ".intent", path, sizeof path) && !find_file("tmp_", "", path, sizeof path));

    /* an intent damaged in the middle (torn: its sectors not all written) rolls the insert back whole */
    intent_left(name);
    CHECK(find_file("txn_", ".intent", path, sizeof path));
    int fd = open(path, O_RDWR);
    CHECK(fd >= 0 && pwrite(fd, "X", 1, 20) == 1);
    close(fd);
    CHECK(rows_after_open() == 0);
    CHECK(!find_file("txn_", ".intent", path, sizeof path) && !find_file("tmp_", "", path, sizeof path));

    /* cut short (the end line lost) or empty: rolled back whole as well */
    for (int cut = 0; cut < 2; cut++) {
        intent_left(name);
        CHECK(find_file("txn_", ".intent", path, sizeof path));
        struct stat st;
        CHECK(stat(path, &st) == 0 && truncate(path, cut ? st.st_size - 10 : 0) == 0);
        CHECK(rows_after_open() == 0);
    }

    /* damage that still parses (its two renames swapped): only the checksum catches it, and it rolls back */
    intent_left(name);
    CHECK(find_file("txn_", ".intent", path, sizeof path));
    {
        char text[1024], out[1024];
        FILE *fp = fopen(path, "r");
        size_t len = fp ? fread(text, 1, sizeof text - 1, fp) : 0;
        if (fp)
            fclose(fp);
        text[len] = '\0';
        char *l1 = strchr(text, '\n'), *l2 = l1 ? strchr(l1 + 1, '\n') : NULL, *l3 = l2 ? strchr(l2 + 1, '\n') : NULL;
        CHECK(l3 != NULL);
        if (l3) {
            /* "head\nA\nB\nend..." -> "head\nB\nA\nend..." */
            int n = snprintf(out, sizeof out, "%.*s%.*s\n%.*s%s", (int)(l1 + 1 - text), text, (int)(l3 - l2 - 1), l2 + 1,
                             (int)(l2 - l1), l1 + 1, l3 + 1);
            CHECK(n == (int)len && strcmp(out, text) != 0);
            fp = fopen(path, "w");
            CHECK(fp && fwrite(out, 1, len, fp) == len);
            if (fp)
                fclose(fp);
        }
    }
    CHECK(rows_after_open() == 0);
    CHECK(!find_file("txn_", ".intent", path, sizeof path) && !find_file("tmp_", "", path, sizeof path));

    /* an intent that is whole is rolled forward */
    intent_left(name);
    CHECK(rows_after_open() == 100);
    printf("  failed commit of an insert over two partitions: all or nothing\n");
    printf("  a torn intent, or one whose parts are not all there, rolls the insert back\n");
}

/* CLASSES maps a series' rows to partitions, the WAL's on replay included: a failed write or sync of a record
 * (or of the directory after the file was made) fails the engine, and the open cuts a torn record. A slow fresh
 * series' first large insert writes its record (from the beginning of time) before any part. */
static void test_classes_file(const char *base)
{
    static const struct {
        int op;
        const char *match, *msg;
    } cases[] = {
        {F_FSYNC, "/", "creating CLASSES"},
        {F_PWRITE, "/CLASSES", "CLASSES"},
        {F_FDATASYNC, "/CLASSES", "CLASSES"},
    };
    for (size_t k = 0; k < sizeof cases / sizeof cases[0]; k++) {
        char name[600];
        snprintf(name, sizeof name, "%s/failstop-classes-%zu", base, k);
        tsdb *db = open_fresh(name, opts());
        tsdb_sid id, slow;
        OK(tsdb_series(db, "s", &id));
        OK(tsdb_series(db, "slow", &slow));
        OK(insert_n(db, id, 0, 100, false)); /* fast: class 0, no record */
        int64_t ts[100];
        float v[100];
        for (int i = 0; i < 100; i++)
            ts[i] = (int64_t)i * 600000, v[i] = (float)i; /* a row in 10 minutes */
        arm(cases[k].op, cases[k].match, false, false);
        CHECK(tsdb_insert_series(db, slow, ts, v, 100) == TSDB_ERR_IO);
        CHECK(fired() == 1);
        CHECK(is_failed(db, cases[k].msg));
        CHECK(insert_n(db, id, 1000, 10, false) == TSDB_ERR_IO);
        db = reopen(db);
        CHECK(count_rows(db, "s") == 100);
        CHECK(count_rows(db, "slow") == 0); /* failed before any part was written */
        OK(insert_n(db, slow, 0, 100, false)); /* the series gets its class now */
        CHECK(count_rows(db, "slow") == 100);
        tsdb_close(db);
    }
    printf("  failed write or sync of CLASSES\n");
}

static int64_t stat_parts(tsdb *db)
{
    tsdb_stats st;
    OK(tsdb_get_stats(db, &st));
    return (int64_t)st.parts;
}

static tsdb *two_parts(const char *name)
{
    tsdb *db = open_fresh(name, opts());
    tsdb_sid id;
    OK(tsdb_series(db, "s", &id));
    OK(insert_n(db, id, 0, 100, false));
    OK(insert_n(db, id, 100, 100, false));
    CHECK(stat_parts(db) == 2);
    return db;
}

typedef struct {
    tsdb *db;
    int rc;
} optimizer;

static void *optimize_main(void *arg)
{
    optimizer *o = arg;
    o->rc = tsdb_optimize(o->db);
    return NULL;
}

static void test_merge(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-merge", base);

    /* the fsync that makes the merged part's name durable fails: the sources are not retired */
    tsdb *db = two_parts(name);
    arm(F_FSYNC, "/", false, false);
    CHECK(tsdb_optimize(db) == TSDB_ERR_IO);
    CHECK(fired() == 1);
    CHECK(is_failed(db, "merged part"));
    db = reopen(db);
    CHECK(count_rows(db, "s") == 200);
    tsdb_close(db);

    /* renaming a replaced source to retired_ fails: still listed after a reopen, so fail-stop */
    db = two_parts(name);
    arm(F_RENAME, "retired_", false, false);
    CHECK(tsdb_optimize(db) == TSDB_ERR_IO);
    CHECK(fired() == 1);
    CHECK(is_failed(db, "retired_"));
    db = reopen(db);
    CHECK(count_rows(db, "s") == 200);
    tsdb_close(db);

    /* the engine fails while a merge is in flight: its output is neither named nor listed */
    db = two_parts(name);
    tsdb_sid id;
    OK(tsdb_series_lookup(db, "s", &id));
    hold(F_FDATASYNC, "/tmp_");
    optimizer o = {db, 0};
    pthread_t t;
    pthread_create(&t, NULL, optimize_main, &o);
    wait_held();
    arm(F_WRITE, "/wal_", false, false);
    CHECK(tsdb_append(db, id, 5000, 1) == TSDB_ERR_IO);
    release();
    pthread_join(t, NULL);
    CHECK(o.rc == TSDB_ERR_IO);
    CHECK(stat_parts(db) == 2);
    CHECK(count_files("^p.*\\.part$") == 2);
    db = reopen(db);
    CHECK(count_rows(db, "s") == 200);
    tsdb_close(db);
    printf("  failed merges, and a merge in flight when the engine fails\n");
}

/* A failed merge backs off for a second before it tries again. tsdbd closes the database right after a storage
 * failure, often with a merge failed by it, and has to be gone within a second: close must not wait it out. */
static void test_merge_backoff(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-backoff", base);
    tsdb *db = two_parts(name);
    tsdb_close(db);
    tsdb_options o = opts();
    o.merge_threads = 1;
    o.merge_idle_s = 1; /* merges the two parts a second after they were written */
    arm(F_FDATASYNC, "/tmp_", false, false); /* the merged part's data: the merge fails, the engine does not */
    OK(tsdb_open(dir, &o, &db));
    for (int i = 0; i < 2500 && !fired_yet(); i++)
        sleep_ms(2);
    CHECK(fired() == 1);
    sleep_ms(50); /* into the back-off */
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    tsdb_close(db);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double ms = (double)(t1.tv_sec - t0.tv_sec) * 1e3 + (double)(t1.tv_nsec - t0.tv_nsec) / 1e6;
    CHECK(ms < 300);
    db = reopen(NULL);
    CHECK(count_rows(db, "s") == 200);
    tsdb_close(db);
    printf("  close does not wait for a failed merge's back-off (%.0f ms)\n", ms);
}

/* A flush opens a new WAL segment; its name must be durable before rows go there. */
static void test_wal_segment(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-segment", base);
    tsdb *db = open_fresh(name, opts());
    tsdb_sid id;
    OK(tsdb_series(db, "s", &id));
    OK(tsdb_append(db, id, 1, 1));
    arm(F_FSYNC, "/", false, false);
    CHECK(tsdb_flush(db) == TSDB_ERR_IO);
    CHECK(fired() == 1);
    CHECK(is_failed(db, "WAL segment"));
    CHECK(tsdb_append(db, id, 2, 2) == TSDB_ERR_IO);
    db = reopen(db);
    int64_t want[] = {1};
    expect_rows(db, "s", want, 1);
    tsdb_close(db);
    printf("  failed directory fsync after creating a WAL segment\n");
}

void tsdb_test_wal_state(tsdb *t, size_t *nwtail, uint64_t *removed, uint64_t *removed_synced);

static int wal_files(const char *dir)
{
    DIR *d = opendir(dir);
    int n = 0;
    for (struct dirent *e; d && (e = readdir(d));)
        n += strncmp(e->d_name, "wal_", 4) == 0;
    if (d)
        closedir(d);
    return n;
}

/* A flushed segment whose unlink fails stays, and wal_removed must not pass it: empty merge outputs (and the
 * bound of a class switch) are dropped once every segment up to theirs is unlinked, and this one would replay at
 * the next open. Before the fix the result of unlinkat was ignored and wal_removed moved on. The next flush
 * removes it. */
static void test_wal_unlink(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-unlink", base);
    tsdb *db = open_fresh(name, opts());
    tsdb_sid id;
    OK(tsdb_series(db, "s", &id));
    size_t nt;
    uint64_t r0, r1, r2, sy;
    tsdb_test_wal_state(db, &nt, &r0, &sy);
    OK(tsdb_append(db, id, 1, 1));
    arm(F_UNLINK, "wal_", false, false);
    printf("  (a log line about removing a WAL segment is expected next)\n");
    OK(tsdb_flush(db)); /* not fail-stop: nothing relies on the unlink */
    CHECK(fired() == 1);
    tsdb_test_wal_state(db, &nt, &r1, &sy);
    CHECK(r1 == r0);
    int stuck = wal_files(name);
    CHECK(stuck == 2); /* the flushed segment and the new one */
    OK(tsdb_append(db, id, 2, 2));
    OK(tsdb_flush(db));
    tsdb_test_wal_state(db, &nt, &r2, &sy);
    CHECK(r2 >= r0 + 2);
    CHECK(wal_files(name) == 1);
    db = reopen(db);
    int64_t want[] = {1, 2};
    expect_rows(db, "s", want, 2);
    tsdb_close(db);
    printf("  failed unlink of a flushed WAL segment\n");
}

/* A large insert's part is renamed while another thread's directory fsync
 * fails. Its own fsync after that returns 0, but proves nothing: the kernel
 * reports a writeback error once per open file, the database shares one
 * directory fd, and the failed entries may never reach the disk
 * ("fsyncgate"). Before the fix the insert was acknowledged after the failure
 * (the jepsen eio nemesis lost such an insert after a WAL segment's directory
 * fsync failed in a flush). */
typedef struct {
    tsdb *db;
    tsdb_sid id;
    int rc;
} inserter;

static void *insert_main(void *arg)
{
    inserter *a = arg;
    a->rc = insert_n(a->db, a->id, 0, 100, false);
    return NULL;
}

static void test_dir_fsync_race(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-dirrace", base);
    tsdb *db = open_fresh(name, opts());
    tsdb_sid id;
    OK(tsdb_series(db, "s", &id));
    hold(F_RENAME, "_0.part"); /* the large insert, before it names its part */
    inserter a = {db, id, -1};
    pthread_t t;
    pthread_create(&t, NULL, insert_main, &a);
    wait_held();
    OK(tsdb_append(db, id, 1000, 1)); /* something to flush */
    OK(tsdb_commit(db));
    arm(F_FSYNC, "/", false, false);
    CHECK(tsdb_flush(db) == TSDB_ERR_IO); /* the new WAL segment's directory fsync */
    CHECK(fired() == 1);
    release();
    pthread_join(t, NULL);
    CHECK(a.rc == TSDB_ERR_IO);
    CHECK(is_failed(db, "WAL segment"));
    db = reopen(db);
    int64_t n = count_rows(db, "s");
    CHECK(n == 1 || n == 101); /* the unacknowledged part may or may not be there */
    tsdb_close(db);
    printf("  a directory fsync after another one failed\n");
}

static void test_drop_before(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-drop", base);
    tsdb *db = open_fresh(name, opts());
    tsdb_sid id;
    OK(tsdb_series(db, "s", &id));
    OK(insert_n(db, id, 0, 100, false));
    OK(insert_n(db, id, 86400000, 100, false));
    arm(F_FSYNC, "/", false, false);
    CHECK(tsdb_drop_before(db, 86400000) == TSDB_ERR_IO);
    CHECK(fired() == 1);
    CHECK(is_failed(db, "dropped"));
    db = reopen(db);
    int64_t n = count_rows(db, "s");
    CHECK(n == 100 || n == 200);
    tsdb_close(db);
    printf("  failed directory fsync after dropping partitions\n");
}

/* ------------------------------------------------ ha.md section 13: B2, B3 */

static void set_dir(const char *path)
{
    char real[PATH_MAX];
    snprintf(dir, sizeof dir, "%.500s", path);
    pthread_mutex_lock(&f_lock);
    free(f_dir);
    f_dir = strdup(realpath(dir, real) ? real : dir);
    pthread_mutex_unlock(&f_lock);
}

static void rm_tree(const char *path)
{
    char cmd[700];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", path);
    if (system(cmd) != 0)
        failures++;
}

extern int (*mt_test_reserve_fail)(uint32_t id); /* src/memtable.c: nonzero fails the memtable's growth for series id */
static uint32_t reserve_fail_id;
static int reserve_fail_hits;

static int reserve_fail(uint32_t id)
{
    if (id != reserve_fail_id)
        return 0;
    reserve_fail_hits++;
    return 1;
}

/* A small write the memtable has no room for (malloc failed) is refused before it reaches the WAL. It used to be
 * appended to the WAL buffer first: the refused write's earlier rows stayed in the memtable (visible at once), and
 * the next write's sync made the whole of it durable, so a crash replayed it. Nothing of it may show, now or after
 * a crash, and the engine goes on (an allocation failure is no storage error). */
static void test_memtable_room(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-mtroom", base);
    tsdb *db = open_fresh(name, opts());
    tsdb_sid a, b, c;
    OK(tsdb_series(db, "a", &a));
    OK(tsdb_series(db, "b", &b));
    OK(tsdb_series(db, "c", &c));
    const tsdb_sid ids[4] = {a, a, b, c};
    const int64_t ts[4] = {1, 2, 3, 4};
    const float v[4] = {1, 2, 3, 4};
    reserve_fail_id = (uint32_t)TSDB_SID_LOCAL(c), reserve_fail_hits = 0;
    mt_test_reserve_fail = reserve_fail;
    int rc = tsdb_insert(db, ids, ts, v, 4);
    mt_test_reserve_fail = NULL;
    if (reserve_fail_hits != 1 || rc != TSDB_ERR_NOMEM) {
        fprintf(stderr, "memtable room: the planted failure fired %d times, the insert gave %s\n", reserve_fail_hits,
                tsdb_strerror(rc));
        failures++;
    }
    const int64_t later = 100;
    const float lv = 100;
    OK(tsdb_insert_series(db, a, &later, &lv, 1)); /* syncs the WAL: whatever the refused write left there too */
    if (is_failed(db, NULL)) {
        fprintf(stderr, "memtable room: the engine failed\n");
        failures++;
    }
    expect_rows(db, "a", &later, 1);
    expect_rows(db, "b", NULL, 0);
    expect_rows(db, "c", NULL, 0);
    /* a crash: the directory as it is while open, WAL and all */
    char cmd[2000], crash[620];
    snprintf(crash, sizeof crash, "%s.crash", name);
    snprintf(cmd, sizeof cmd, "rm -rf '%s' && cp -a '%s' '%s'", crash, name, crash);
    if (system(cmd) != 0)
        failures++;
    tsdb_close(db);
    tsdb_options o = opts();
    db = NULL;
    OK(tsdb_open(crash, &o, &db));
    if (db) {
        expect_rows(db, "a", &later, 1);
        expect_rows(db, "b", NULL, 0);
        expect_rows(db, "c", NULL, 0);
        tsdb_close(db);
    }
    rm_tree(crash);
}

/* The room a batch needs counts every row of a series that comes more than once: series b holds 63 rows of its 64,
 * so one more row fits and two do not. Ids in ascending order take one capacity check per series, other orders
 * count row by row; each must refuse the batch (the growth fails) with nothing of it visible, and no engine fail
 * (the mt_add after the WAL append would have to fail then). */
static void test_memtable_room_runs(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-mtroom-runs", base);
    tsdb *db = open_fresh(name, opts());
    tsdb_sid a, b;
    OK(tsdb_series(db, "a", &a));
    OK(tsdb_series(db, "b", &b));
    OK(insert_n(db, b, 0, 63, false));
    const tsdb_sid order[2][3] = {{a, b, b}, {b, a, b}}; /* ascending (a run of b), and not */
    for (int c = 0; c < 2; c++) {
        const int64_t ts[3] = {1000 + c, 2000 + c, 3000 + c};
        const float v[3] = {1, 2, 3};
        reserve_fail_id = (uint32_t)TSDB_SID_LOCAL(b), reserve_fail_hits = 0;
        mt_test_reserve_fail = reserve_fail;
        int rc = tsdb_insert(db, order[c], ts, v, 3);
        mt_test_reserve_fail = NULL;
        if (reserve_fail_hits != 1 || rc != TSDB_ERR_NOMEM) {
            fprintf(stderr, "memtable room, order %d: the planted failure fired %d times, the insert gave %s\n", c,
                    reserve_fail_hits, tsdb_strerror(rc));
            failures++;
        }
        CHECK(count_rows(db, "a") == 0);
        CHECK(count_rows(db, "b") == 63);
        if (is_failed(db, NULL)) {
            fprintf(stderr, "memtable room, order %d: the engine failed\n", c);
            failures++;
        }
    }
    const int64_t ts[3] = {5000, 6000, 7000};
    const float v[3] = {1, 2, 3};
    OK(tsdb_insert(db, order[1], ts, v, 3)); /* no failure planted: all of it is applied */
    CHECK(count_rows(db, "a") == 1);
    CHECK(count_rows(db, "b") == 65);
    tsdb_close(db);
}

extern int (*mt_test_flush_fail)(void); /* src/memtable.c: 0 flushes, INT_MIN skips the flush, else its error */
static pthread_t flush_fail_main;
static int flush_fail_hits;

/* The flusher thread is "slow" (its flush does nothing), the writer's own backpressure flush fails with NOMEM */
static int flush_fail(void)
{
    if (!pthread_equal(pthread_self(), flush_fail_main))
        return INT_MIN;
    flush_fail_hits++;
    return TSDB_ERR_NOMEM;
}

static uint64_t memtable_rows_of(tsdb *db)
{
    tsdb_volume_stats vs;
    memset(&vs, 0, sizeof vs);
    OK(tsdb_get_volume_stats(db, 0, &vs));
    return vs.memtable_rows;
}

/* The backpressure flush (a memtable at twice buffer_rows) failing is never the result of a write that is applied.
 * It used to run after the append: the write that took the memtable to the limit returned the flush's error
 * although its rows were visible and in the WAL (a crash replays them), an error a client takes for a refused
 * write. Now it runs before the next write's append: that write is refused, definitely (not visible, not in the
 * WAL), the memtable stops growing while flushes fail, and the first write after they work again flushes and is
 * applied. */
static void test_backpressure_flush(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-backpressure", base);
    tsdb_options o = opts();
    o.buffer_rows = 200; /* writes of 50 rows (below wal_max_batch_rows): the limit 2 x 200 comes at the 8th */
    tsdb *db = open_fresh(name, o);
    tsdb_sid id;
    OK(tsdb_series(db, "s", &id));
    flush_fail_main = pthread_self(), flush_fail_hits = 0;
    __atomic_store_n(&mt_test_flush_fail, flush_fail, __ATOMIC_RELAXED);
    for (int k = 0; k < 8; k++) { /* the 8th reaches the limit: applied, so a success whatever the flush would say */
        int rc = insert_n(db, id, k * 50, 50, false);
        if (rc != 0) {
            fprintf(stderr, "backpressure: write %d gave %s, it is applied\n", k, tsdb_strerror(rc));
            failures++;
        }
    }
    CHECK(count_rows(db, "s") == 400);
    CHECK(memtable_rows_of(db) == 400);
    for (int k = 8; k < 13; k++) { /* flushes keep failing: each write is refused whole, the memtable stays */
        int rc = insert_n(db, id, k * 50, 50, false);
        if (rc != TSDB_ERR_NOMEM) {
            fprintf(stderr, "backpressure: write %d gave %s, want NOMEM\n", k, tsdb_strerror(rc));
            failures++;
        }
        CHECK(count_rows(db, "s") == 400);
        CHECK(memtable_rows_of(db) == 400);
    }
    CHECK(flush_fail_hits == 5);
    if (is_failed(db, NULL)) {
        fprintf(stderr, "backpressure: the engine failed\n");
        failures++;
    }
    /* a crash: nothing of the refused writes is in the WAL */
    char cmd[2000], crash[620];
    snprintf(crash, sizeof crash, "%s.crash", name);
    snprintf(cmd, sizeof cmd, "rm -rf '%s' && cp -a '%s' '%s'", crash, name, crash);
    if (system(cmd) != 0)
        failures++;
    tsdb *cdb = NULL;
    tsdb_options co = opts();
    OK(tsdb_open(crash, &co, &cdb));
    if (cdb) {
        CHECK(count_rows(cdb, "s") == 400);
        tsdb_close(cdb);
    }
    rm_tree(crash);
    __atomic_store_n(&mt_test_flush_fail, NULL, __ATOMIC_RELAXED); /* they work again: the next write flushes the 400 rows, then is applied */
    OK(insert_n(db, id, 8 * 50, 50, false));
    CHECK(count_rows(db, "s") == 450);
    CHECK(memtable_rows_of(db) == 50);
    tsdb_close(db);
}

/* A series that comes again after others ({a, b, a}, a with one row of room), and one series given as a run of 2
 * rows (tsdb_insert_series): each counts every row of a series, and each is refused whole when the growth fails. */
static void test_memtable_room_more(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-mtroom-more", base);
    tsdb *db = open_fresh(name, opts());
    tsdb_sid a, b;
    OK(tsdb_series(db, "a", &a));
    OK(tsdb_series(db, "b", &b));
    OK(insert_n(db, a, 0, 63, false));
    {
        const tsdb_sid order[3] = {a, b, a};
        const int64_t ts[3] = {1000, 2000, 3000};
        const float v[3] = {1, 2, 3};
        reserve_fail_id = (uint32_t)TSDB_SID_LOCAL(a), reserve_fail_hits = 0;
        mt_test_reserve_fail = reserve_fail;
        int rc = tsdb_insert(db, order, ts, v, 3);
        mt_test_reserve_fail = NULL;
        if (reserve_fail_hits != 1 || rc != TSDB_ERR_NOMEM) {
            fprintf(stderr, "memtable room more {a,b,a}: hits %d, %s\n", reserve_fail_hits, tsdb_strerror(rc));
            failures++;
        }
        CHECK(count_rows(db, "a") == 63);
        CHECK(count_rows(db, "b") == 0);
        CHECK(!is_failed(db, NULL));
    }
    {
        reserve_fail_id = (uint32_t)TSDB_SID_LOCAL(a), reserve_fail_hits = 0;
        mt_test_reserve_fail = reserve_fail;
        int rc = insert_n(db, a, 5000, 2, false);
        mt_test_reserve_fail = NULL;
        if (reserve_fail_hits != 1 || rc != TSDB_ERR_NOMEM) {
            fprintf(stderr, "memtable room more, one series of 2: hits %d, %s\n", reserve_fail_hits, tsdb_strerror(rc));
            failures++;
        }
        CHECK(count_rows(db, "a") == 63);
        CHECK(!is_failed(db, NULL));
    }
    /* the counts of the refused writes are gone: a's 64th row fits, then the next growth works */
    OK(insert_n(db, a, 6000, 1, false));
    CHECK(count_rows(db, "a") == 64);
    const tsdb_sid order[3] = {a, b, a};
    const int64_t ts[3] = {7000, 8000, 9000};
    const float v[3] = {1, 2, 3};
    OK(tsdb_insert(db, order, ts, v, 3));
    CHECK(count_rows(db, "a") == 66);
    CHECK(count_rows(db, "b") == 1);
    tsdb_close(db);
}

/* What a backpressure flush hook does on the writer's thread (the writer's own flush, before its append) */
enum { BP_COUNT, BP_FAIL_ENGINE, BP_ABANDON };
static int bp_mode;
static int bp_hits;
static tsdb *bp_db;
static volatile int bp_flag;

static int bp_hook(void)
{
    if (!pthread_equal(pthread_self(), flush_fail_main))
        return INT_MIN; /* the flusher thread: slow */
    bp_hits++;
    if (bp_mode == BP_FAIL_ENGINE) {
        arm(F_FDATASYNC, "/wal_", false, false);
        (void)tsdb_commit(bp_db); /* syncs the rows of the 8 writes: fails, the engine is failed */
    } else if (bp_mode == BP_ABANDON) {
        bp_flag = 1; /* the request is abandoned while its flush runs */
    }
    return INT_MIN; /* the flush itself does nothing */
}

static int64_t count_after_crash(const char *name, const char *series)
{
    char cmd[2000], crash[620];
    snprintf(crash, sizeof crash, "%s.crash", name);
    snprintf(cmd, sizeof cmd, "rm -rf '%s' && cp -a '%s' '%s'", crash, name, crash);
    if (system(cmd) != 0)
        failures++;
    tsdb *cdb = NULL;
    tsdb_options co = opts();
    int64_t n = -1;
    OK(tsdb_open(crash, &co, &cdb));
    if (cdb) {
        n = count_rows(cdb, series);
        tsdb_close(cdb);
    }
    rm_tree(crash);
    return n;
}

/* The write that finds the memtable over its limit starts a flush before its append, and what happens while that
 * runs counts: with wal_sync_ms > 0 nothing after the append checks, so a write whose engine failed or whose request
 * was abandoned meanwhile must be refused before it, appending nothing (not in the memtable, not in the WAL: a
 * crash does not bring it back). A request already abandoned starts no flush at all. */
static void test_backpressure_guards(const char *base)
{
    static const char *const what[] = {"flush hook runs, nothing happens", "engine failed by the flush",
                                       "request abandoned during the flush", "request abandoned before"};
    for (int c = 0; c < 4; c++) {
        char name[600];
        snprintf(name, sizeof name, "%s/failstop-bpguards%d", base, c);
        tsdb_options o = opts();
        o.buffer_rows = 200;
        o.wal_sync_ms = 10000; /* the WAL is synced by the timer: the write is acknowledged at its append */
        tsdb *db = open_fresh(name, o);
        tsdb_sid id;
        OK(tsdb_series(db, "s", &id));
        bp_db = db, bp_hits = 0, bp_flag = 0;
        bp_mode = c == 1 ? BP_FAIL_ENGINE : c == 2 ? BP_ABANDON : BP_COUNT;
        flush_fail_main = pthread_self();
        __atomic_store_n(&mt_test_flush_fail, bp_hook, __ATOMIC_RELAXED);
        tsdb_set_abandon_flag(&bp_flag);
        for (int k = 0; k < 8; k++)
            OK(insert_n(db, id, k * 50, 50, false)); /* the memtable reaches 2 x buffer_rows with the 8th */
        CHECK(count_rows(db, "s") == 400);
        CHECK(bp_hits == 0);
        if (c == 3)
            bp_flag = 1;
        int rc = insert_n(db, id, 400, 50, false);
        int want = c == 0 ? 0 : c == 1 ? TSDB_ERR_IO : TSDB_ERR_ABANDONED;
        if (rc != want) {
            fprintf(stderr, "backpressure guards (%s): the write gave %s, want %s\n", what[c], tsdb_strerror(rc),
                    tsdb_strerror(want));
            failures++;
        }
        if (bp_hits != (c == 3 ? 0 : 1)) {
            fprintf(stderr, "backpressure guards (%s): the flush hook ran %d times\n", what[c], bp_hits);
            failures++;
        }
        int64_t rows = c == 0 ? 450 : 400; /* a refused write is in neither the memtable nor the WAL */
        CHECK(count_rows(db, "s") == rows);
        CHECK(memtable_rows_of(db) == (uint64_t)rows);
        CHECK(is_failed(db, c == 1 ? "WAL fdatasync" : NULL) == (c == 1));
        if (c == 1)
            CHECK(fired() == 1);
        __atomic_store_n(&mt_test_flush_fail, NULL, __ATOMIC_RELAXED);
        tsdb_set_abandon_flag(NULL);
        if (c == 1)
            db = reopen(db); /* the failed engine: only a reopen replays from disk */
        else
            OK(tsdb_commit(db));
        CHECK(count_rows(db, "s") == rows);
        tsdb_close(db);
        CHECK(count_after_crash(name, "s") == rows);
    }
}

/* B2: a failure while the parts of one insert are written, synced or named is never a success, and the insert
 * leaves nothing: not a part in the live database, not one after a reopen, not a tmp or intent file. Failures at
 * each write and sync of the first and of the second partition's part. */
static void test_partial_insert(const char *base)
{
    char name[600];
    snprintf(name, sizeof name, "%s/failstop-partial", base);
    static const struct {
        int op;
        const char *match;
        int skip;
        const char *what;
    } cases[] = {
        {F_FDATASYNC, "tmp_", 0, "sync of the first part"},   {F_FDATASYNC, "tmp_", 1, "sync of the second part"},
        {F_WRITE, "tmp_", 0, "write of the first part"},      {F_WRITE, "tmp_", 1, "write of the second part"},
        {F_FDATASYNC, "txn_", 0, "sync of the intent"},       {F_WRITE, "txn_", 0, "write of the intent"},
        {F_RENAME, "p0_", 0, "rename of the first part"},    {F_RENAME, "p1_", 0, "rename of the second part"},
    };
    for (size_t c = 0; c < sizeof cases / sizeof *cases; c++) {
        tsdb *db = open_fresh(name, opts_days());
        tsdb_sid id;
        OK(tsdb_series(db, "s", &id));
        arm_after(cases[c].skip, cases[c].op, cases[c].match);
        int rc = insert_n(db, id, 0, 100, true);
        int hit = fired();
        if (hit != 1) {
            fprintf(stderr, "partial insert, %s: the fault did not fire (%d)\n", cases[c].what, hit);
            failures++;
            tsdb_close(db);
            continue;
        }
        if (rc == 0) {
            fprintf(stderr, "partial insert, %s: reported as success\n", cases[c].what);
            failures++;
        }
        int64_t live = count_rows(db, "s");
        if (live != 0 && live != 100) {
            fprintf(stderr, "partial insert, %s: %lld rows live\n", cases[c].what, (long long)live);
            failures++;
        }
        if (rc != 0 && live != 0) {
            fprintf(stderr, "partial insert, %s: failed but %lld rows are live\n", cases[c].what, (long long)live);
            failures++;
        }
        db = reopen(db);
        int64_t n = count_rows(db, "s");
        if (n != 0 && n != 100) {
            fprintf(stderr, "partial insert, %s: %lld rows after a reopen (half an insert)\n", cases[c].what,
                    (long long)n);
            failures++;
        }
        tsdb_close(db);
        if (rc != 0 && n != 0 && cases[c].op != F_RENAME) { /* a rename fails after the intent's commit point */
            fprintf(stderr, "partial insert, %s: a failed insert came back after a reopen\n", cases[c].what);
            failures++;
        }
        if (n == 0 && (count_files("^tmp_") || count_files("^txn_"))) {
            fprintf(stderr, "partial insert, %s: files left behind\n", cases[c].what);
            failures++;
        }
    }
    printf("  an insert whose part or intent fails to write, sync or name is not a success and not half there\n");
}

/* B3: no fsync result is ignored. Every fsync and fdatasync of a scenario fails in turn (the n-th one, n = 0, 1,
 * ...; the sweep ends when the scenario makes fewer): the call that made it must return an error, an open must
 * fail rather than succeed on a directory it could not make durable. */
typedef int (*sweep_fn)(const char *name, int round, bool *fired_out);

static int sweep_insert(const char *name, int round, bool *fired_out)
{
    tsdb *db = open_fresh(name, opts_days());
    tsdb_sid id;
    OK(tsdb_series(db, "s", &id));
    OK(tsdb_append(db, id, 5, 5));
    arm_after(round & 0xff, (round >> 8) ? F_FDATASYNC : F_FSYNC, "");
    int rc = insert_n(db, id, 1000, 100, true);
    *fired_out = fired() > 0;
    tsdb_close(db);
    return rc;
}

static int sweep_flush(const char *name, int round, bool *fired_out)
{
    tsdb *db = open_fresh(name, opts_days());
    tsdb_sid id;
    OK(tsdb_series(db, "s", &id));
    for (int i = 0; i < 20; i++)
        OK(tsdb_append(db, id, 100 + i, i));
    arm_after(round & 0xff, (round >> 8) ? F_FDATASYNC : F_FSYNC, "");
    int rc = tsdb_flush(db);
    *fired_out = fired() > 0;
    tsdb_close(db);
    return rc;
}

static int sweep_merge(const char *name, int round, bool *fired_out)
{
    tsdb *db = two_parts(name);
    arm_after(round & 0xff, (round >> 8) ? F_FDATASYNC : F_FSYNC, "");
    int rc = tsdb_optimize(db);
    *fired_out = fired() > 0;
    tsdb_close(db);
    return rc;
}

static int sweep_delete(const char *name, int round, bool *fired_out)
{
    tsdb *db = two_parts(name);
    tsdb_sid id;
    OK(tsdb_series_lookup(db, "s", &id));
    arm_after(round & 0xff, (round >> 8) ? F_FDATASYNC : F_FSYNC, "");
    int rc = tsdb_delete_range(db, id, 10, 50);
    *fired_out = fired() > 0;
    tsdb_close(db);
    return rc;
}

static void run_sweep(const char *what, sweep_fn fn, const char *name)
{
    for (int kind = 0; kind < 2; kind++) {
        for (int n = 0; n < 60; n++) {
            bool hit = false;
            int rc = fn(name, (kind << 8) | n, &hit);
            if (!hit)
                break;
            if (rc == 0) {
                fprintf(stderr, "%s: the %s number %d failed and the call returned success\n", what,
                        kind ? "fdatasync" : "fsync", n);
                failures++;
            }
        }
    }
}

/* An open that finds work to redo (an intent to roll forward, a WAL segment to replay, tmp files to remove) makes
 * the result durable before it goes on; if that fails the open fails. `prepare` leaves such a directory in `src`. */
static void run_open_sweep(const char *what, const char *src, const char *work)
{
    for (int kind = 0; kind < 2; kind++) {
        for (int n = 0; n < 60; n++) {
            char cmd[1400];
            rm_tree(work);
            snprintf(cmd, sizeof cmd, "cp -a '%s' '%s'", src, work);
            if (system(cmd) != 0) {
                failures++;
                return;
            }
            set_dir(work);
            arm_after(n, kind ? F_FDATASYNC : F_FSYNC, "");
            tsdb_options o = opts_days();
            tsdb *db = NULL;
            int rc = tsdb_open(work, &o, &db);
            bool hit = fired() > 0;
            if (db)
                tsdb_close(db);
            if (!hit)
                break;
            if (rc == 0) {
                fprintf(stderr, "%s: open: the %s number %d failed and the open succeeded\n", what,
                        kind ? "fdatasync" : "fsync", n);
                failures++;
            }
        }
    }
}

static void test_fsync_results(const char *base)
{
    char name[600], src[600], work[600];
    snprintf(name, sizeof name, "%s/failstop-sweep", base);
    run_sweep("insert", sweep_insert, name);
    run_sweep("flush", sweep_flush, name);
    run_sweep("merge", sweep_merge, name);
    run_sweep("delete", sweep_delete, name);
    printf("  every failed fsync of an insert, a flush, a merge and a delete reaches the caller\n");

    /* creating a database */
    for (int kind = 0; kind < 2; kind++) {
        for (int n = 0; n < 20; n++) {
            rm_tree(name);
            mkdir(name, 0755);
            set_dir(name);
            arm_after(n, kind ? F_FDATASYNC : F_FSYNC, "");
            tsdb_options o = opts_days();
            tsdb *db = NULL;
            int rc = tsdb_open(name, &o, &db);
            bool hit = fired() > 0;
            if (db)
                tsdb_close(db);
            if (!hit)
                break;
            if (rc == 0) {
                fprintf(stderr, "create: the %s number %d failed and the open succeeded\n",
                        kind ? "fdatasync" : "fsync", n);
                failures++;
            }
        }
    }

    /* a leftover intent, rolled forward at the open */
    snprintf(src, sizeof src, "%s/failstop-sweep-src", base);
    snprintf(work, sizeof work, "%s/failstop-sweep-work", base);
    intent_left(src);
    run_open_sweep("intent roll forward", src, work);

    /* a WAL segment to replay: the directory copied while the database is open (a crash) */
    rm_tree(src);
    tsdb *db = open_fresh(src, opts_days());
    tsdb_sid id;
    OK(tsdb_series(db, "s", &id));
    for (int i = 0; i < 20; i++)
        OK(tsdb_append(db, id, 100 + i, i));
    {
        char cmd[1400];
        snprintf(cmd, sizeof cmd, "cp -a '%s' '%s.crash'", src, src);
        if (system(cmd) != 0)
            failures++;
    }
    tsdb_close(db);
    char crash[620];
    snprintf(crash, sizeof crash, "%s.crash", src);
    run_open_sweep("WAL replay", crash, work);
    rm_tree(crash);
    printf("  every failed fsync while a database is created or opened reaches the open\n");
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
    test_fence_failure(root);
    test_flush_retry(root);
    test_manifest(root);
    test_series_dict(root);
    test_series_dict_add(root);
    test_series_dict_cause(root);
    test_volume_meta(root);
    test_volume_meta_race(root);
    test_background_sync(root);
    test_state_race(root);
    test_intent(root);
    test_classes_file(root);
    test_merge(root);
    test_merge_backoff(root);
    test_wal_segment(root);
    test_wal_unlink(root);
    test_drop_before(root);
    test_dir_fsync_race(root);
    test_partial_insert(root);
    test_memtable_room(root);
    test_memtable_room_runs(root);
    test_backpressure_flush(root);
    test_memtable_room_more(root);
    test_backpressure_guards(root);
    test_fsync_results(root);
    if (!failures) {
        char cmd[512];
        snprintf(cmd, sizeof cmd, "rm -rf '%s'", root);
        if (system(cmd) != 0)
            failures++;
    }
    printf(failures ? "FAILED: %d checks\n" : "all tests passed\n", failures);
    return failures != 0;
}
