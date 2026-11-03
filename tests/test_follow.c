/*
 * One writer process, one follower process, one directory.
 *
 * The writer appends (WAL, fsync'd), writes large batches that overwrite
 * earlier timestamps, merges, retires parts (with a short lifetime) and
 * creates series while the follower queries continuously. The follower must
 * never see duplicates or disorder, must see acknowledged appends within a
 * few refresh intervals, and must end up with exactly the writer's data.
 */
#define _GNU_SOURCE
#include "tsdb.h"

#include <fcntl.h>
#include <math.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "portable.h"

#define NS 4
#define T0 1700000000000LL
/* internal test hook: switch a series to class cls at the next boundary after its rows (or *at when later) */
int tsdb_test_class_switch(tsdb *t, tsdb_sid series, int cls, const int64_t *at);

typedef struct shared {
    atomic_int phase;          /* 1 appends, 2 batches, 3 done */
    atomic_llong acked[NS + 1]; /* newest acknowledged append per series */
    long long final_count[NS + 1];
    double final_sum[NS + 1];
    atomic_int follower_done;
    atomic_int writer_failed;
} shared;

static int failures;
#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                              \
        }                                                                            \
    } while (0)

static double now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + ts.tv_nsec * 1e-9;
}

static void writer(const char *dir, shared *sh)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.granule_rows = 256;
    o.partition_ms = 3600000;
    o.obsolete_part_lifetime_s = 2;
    o.flush_interval_ms = 400;
    o.wal_max_batch_rows = 5000;
    tsdb *db;
    if (tsdb_open(dir, &o, &db)) {
        sh->writer_failed = 1;
        return;
    }
    tsdb_sid id;
    char name[32];
    for (int s = 0; s < NS; s++) {
        snprintf(name, sizeof name, "sensor.%d", s);
        tsdb_series(db, name, &id);
    }
    /* phase 1: single appends through the WAL */
    sh->phase = 1;
    double t0 = now();
    long long i = 0;
    while (now() - t0 < 1.5) {
        for (int s = 0; s < NS; s++) {
            long long ts = T0 + i * 1000;
            if (tsdb_append(db, TSDB_SID(0, s), ts, (float)(i % 1000) / 10.0f)) {
                sh->writer_failed = 1;
                return;
            }
            sh->acked[s] = ts; /* durable and visible to followers from now on */
        }
        i++;
    }
    /* phase 2: large batches overwriting earlier rows, merges, a new series */
    sh->phase = 2;
    enum { B = 20000 };
    int64_t *ts = malloc(B * sizeof *ts);
    float *v = malloc(B * sizeof *v);
    for (int round = 0; round < 12; round++) {
        for (int s = 0; s < NS; s++) {
            for (int k = 0; k < B; k++) {
                ts[k] = T0 + ((long long)round * 7000 + k) * 1000; /* overlaps the appends and earlier rounds */
                v[k] = (float)(round * 100 + s);
            }
            if (tsdb_insert_series(db, TSDB_SID(0, s), ts, v, B)) {
                sh->writer_failed = 1;
                return;
            }
        }
        if (round == 5) {
            tsdb_series(db, "sensor.late", &id); /* the follower has to discover it */
            int64_t t = T0;
            float x = 42;
            tsdb_append(db, id, t, x);
        }
        if (round % 4 == 3)
            tsdb_optimize(db); /* merges retire parts the follower may be reading */
        usleep(50000);
    }
    tsdb_flush(db);
    tsdb_optimize(db);
    for (uint32_t s = 0; s <= NS; s++) {
        tsdb_bucket *b;
        size_t nb;
        sh->final_count[s] = 0;
        sh->final_sum[s] = 0;
        if (tsdb_query_agg(db, TSDB_SID(0, s), INT64_MIN, INT64_MAX, 0, &b, &nb) == 0 && nb == 1) {
            sh->final_count[s] = (long long)b[0].count;
            sh->final_sum[s] = b[0].sum;
        }
        free(b);
    }
    sh->phase = 3;
    while (!sh->follower_done)
        usleep(10000);
    tsdb_close(db);
    free(ts);
    free(v);
}

static bool query_ok(tsdb *db, uint32_t s, size_t *rows, double *sum)
{
    tsdb_points p;
    if (tsdb_query_points(db, TSDB_SID(0, s), INT64_MIN, INT64_MAX, &p))
        return false;
    bool ok = true;
    double total = 0;
    for (size_t i = 0; i < p.n; i++) {
        if (i && p.ts[i] <= p.ts[i - 1])
            ok = false; /* a duplicate or disorder */
        total += p.values[i];
    }
    *rows = p.n;
    *sum = total;
    tsdb_points_free(&p);
    return ok;
}

/* A follower sees typed rows and nulls from the writer's WAL, and state names
 * created after it opened; then the same from parts once they are flushed. */
typedef struct typed_seen {
    size_t n, nulls;
    int64_t last;
} typed_seen;

static int typed_cb(void *ctx, const int64_t *ts, const tsdb_values *col, size_t n)
{
    typed_seen *t = ctx;
    for (size_t i = 0; i < n; i++) {
        bool known = !col->valid || ((col->valid[i >> 3] >> (i & 7)) & 1);
        t->nulls += !known;
        if (known && col->type == TSDB_INT64)
            t->last = ((const int64_t *)col->values)[i];
        (void)ts;
    }
    t->n += n;
    return 0;
}

static void test_typed_follow(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/tsdb-follow-typed-%d", base, (int)getpid());
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.flush_interval_ms = 600000; /* the rows stay in the WAL until the explicit flush */
    tsdb *w;
    if (tsdb_open(dir, &o, &w) != 0) {
        CHECK(!"open writer");
        return;
    }
    tsdb_sid cnt, mode;
    CHECK(tsdb_series_create(w, "typed.count", TSDB_INT64, NULL, &cnt) == 0);
    tsdb_options fo;
    tsdb_options_init(&fo);
    fo.follow = 1;
    fo.refresh_ms = 20;
    tsdb *f = NULL;
    CHECK(tsdb_open(dir, &fo, &f) == 0);
    if (!f) {
        tsdb_close(w);
        return;
    }
    /* created after the follower opened */
    CHECK(tsdb_series_create(w, "typed.mode", TSDB_STATE, NULL, &mode) == 0);
    uint32_t idle;
    CHECK(tsdb_state(w, mode, "idle", &idle) == 0);
    int64_t ts[100], iv[100];
    uint8_t valid[13];
    memset(valid, 0xff, sizeof valid);
    for (int k = 0; k < 100; k++) {
        ts[k] = T0 + k * 1000;
        iv[k] = 1000000000000LL + k;
        if (k % 10 == 0)
            valid[k >> 3] &= (uint8_t)~(1u << (k & 7));
    }
    CHECK(tsdb_insert_series_values(w, cnt, ts, &(tsdb_values){TSDB_INT64, iv, valid}, 100) == 0);
    uint32_t codes[3] = {idle, idle, idle};
    CHECK(tsdb_insert_series_values(w, mode, ts, &(tsdb_values){TSDB_STATE, codes, NULL}, 3) == 0);
    for (int round = 0; round < 2; round++) {
        typed_seen t = {0, 0, 0};
        for (int k = 0; k < 200 && t.n < 100; k++) { /* a few refreshes */
            usleep(10000);
            t = (typed_seen){0, 0, 0};
            CHECK(tsdb_query_values(f, cnt, INT64_MIN, INT64_MAX, NULL, typed_cb, &t) == 0);
        }
        CHECK(t.n == 100 && t.nulls == 10 && t.last == iv[99]);
        tsdb_bucket *b;
        size_t nb;
        CHECK(tsdb_query_agg(f, cnt, INT64_MIN, INT64_MAX, 0, &b, &nb) == 0);
        CHECK(nb == 1 && b[0].count == 90 && b[0].imax == iv[99]);
        free(b);
        tsdb_sid fmode;
        uint32_t fidle;
        CHECK(tsdb_series_lookup(f, "typed.mode", &fmode) == 0 && fmode == mode);
        CHECK(tsdb_state_lookup(f, fmode, "idle", &fidle) == 0 && fidle == idle);
        const char *name = tsdb_state_name(f, fmode, idle);
        CHECK(name && strcmp(name, "idle") == 0);
        if (round == 0)
            CHECK(tsdb_flush(w) == 0); /* now from parts */
    }
    tsdb_close(f);
    tsdb_close(w);
    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    CHECK(system(cmd) == 0);
}

static void count_part_cls(void *ctx, const tsdb_part_info *p)
{
    ((uint64_t *)ctx)[p->cls & 3] += p->rows;
}

/* A follower across a partition class switch: it reads the switch from CLASSES with the MANIFEST that lists the
 * parts of the new class, and answers exactly what the writer answers, rows written into the past after the switch
 * (they go to the old class's partitions) included; so does a follower opened after it. */
static void test_class_follow(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/tsdb-follow-cls-%d", base, (int)getpid());
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.merge_threads = 0;
    tsdb *w, *r = NULL, *r2 = NULL;
    tsdb_sid id;
    if (tsdb_open(dir, &o, &w) != 0) {
        CHECK(!"open writer");
        return;
    }
    enum { N = 4 * 1440 }; /* four days at a minute, then four more after the switch */
    static int64_t ts[2 * N];
    static float v[2 * N];
    for (int i = 0; i < 2 * N; i++) {
        ts[i] = T0 + (int64_t)i * 60000;
        v[i] = (float)i;
    }
    CHECK(tsdb_series(w, "s", &id) == 0 && tsdb_test_class_switch(w, id, 0, NULL) == 0);
    CHECK(tsdb_insert_series(w, id, ts, v, N) == 0 && tsdb_flush(w) == 0);
    tsdb_options ro;
    tsdb_options_init(&ro);
    ro.read_only = 1;
    CHECK(tsdb_open(dir, &ro, &r) == 0);
    if (!r) {
        tsdb_close(w);
        return;
    }
    int c = -1;
    CHECK(tsdb_series_class(r, id, &c) == 0 && c == 0);
    CHECK(tsdb_test_class_switch(w, id, 3, NULL) == 0);
    /* rows from the next day (the default partition) after the newest row on are class 3 */
    int64_t T = (ts[N - 1] / 86400000 + 1) * 86400000;
    int n0 = 0;
    while (ts[n0] < T)
        n0++;
    CHECK(tsdb_insert_series(w, id, ts + N, v + N, N) == 0); /* after the switch: class 3 */
    float old[100];
    for (int i = 0; i < 100; i++)
        old[i] = v[1000 + i] = -1.0f - (float)i; /* into the past: class 0, newer than the first rows */
    CHECK(tsdb_insert_series(w, id, ts + 1000, old, 100) == 0 && tsdb_flush(w) == 0);
    CHECK(tsdb_refresh(r) == 0);
    CHECK(tsdb_series_class(r, id, &c) == 0 && c == 3);
    CHECK(tsdb_optimize(w) == 0); /* the follower still maps the parts it replaced */
    CHECK(tsdb_open(dir, &ro, &r2) == 0);
    for (int round = 0; round < 2; round++) {
        tsdb *rd[3] = {w, r, r2};
        uint64_t rows[3][4];
        for (int k = 0; k < 3; k++) {
            if (!rd[k])
                continue;
            if (k)
                CHECK(tsdb_refresh(rd[k]) == 0);
            tsdb_points p;
            CHECK(tsdb_query_points(rd[k], id, INT64_MIN, INT64_MAX, &p) == 0);
            bool same = p.n == 2 * N;
            for (size_t i = 0; same && i < p.n; i++)
                same = p.ts[i] == ts[i] && p.values[i] == v[i];
            CHECK(same);
            tsdb_points_free(&p);
            memset(rows[k], 0, sizeof rows[k]);
            CHECK(tsdb_foreach_part(rd[k], count_part_cls, rows[k]) == 0);
            CHECK(rows[k][0] == (uint64_t)n0 && rows[k][3] == (uint64_t)(2 * N - n0) && !rows[k][1] && !rows[k][2]);
            CHECK(tsdb_series_class(rd[k], id, &c) == 0 && c == 3);
        }
    }
    if (r2)
        tsdb_close(r2);
    tsdb_close(r);
    tsdb_close(w);
    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    CHECK(system(cmd) == 0);
    printf("  a follower reads a class switch with the parts that use it\n");
}

static void *republish(void *arg)
{
    usleep(100000);
    tsdb_sid other;
    CHECK(tsdb_series(arg, "other", &other) == 0 && tsdb_append(arg, other, T0, 1.0f) == 0 && tsdb_flush(arg) == 0);
    return NULL;
}

/* A reader opening on a MANIFEST older than CLASSES: after that MANIFEST, a delete merged a series' newest rows
 * away, and a switch then gave its class from a T below rows the old MANIFEST still lists (in parts of the old
 * class). The open check of rows against classes failed the reader's open; it reads MANIFEST again now, and a
 * reader that opens on a damaged database (test_classes: opencheck) still fails, once MANIFEST stays the same. */
static void test_stale_manifest_classes(const char *base)
{
    char dir[512], mpath[600];
    snprintf(dir, sizeof dir, "%s/tsdb-follow-stale-%d", base, (int)getpid());
    snprintf(mpath, sizeof mpath, "%s/MANIFEST", dir);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.merge_threads = 0;
    o.wal_max_batch_rows = 16; /* the rows go straight to parts */
    tsdb *w, *r = NULL;
    tsdb_sid id;
    if (tsdb_open(dir, &o, &w) != 0) {
        CHECK(!"open writer");
        return;
    }
    enum { N = 3 * 1440 }; /* three days at a minute */
    static int64_t ts[N];
    static float v[N];
    for (int i = 0; i < N; i++)
        ts[i] = T0 + (int64_t)i * 60000, v[i] = (float)i;
    CHECK(tsdb_series(w, "s", &id) == 0 && tsdb_test_class_switch(w, id, 0, NULL) == 0);
    CHECK(tsdb_insert_series(w, id, ts, v, N) == 0 && tsdb_flush(w) == 0);
    /* the MANIFEST a lagging reader sees */
    char *old = NULL;
    size_t olen = 0;
    FILE *f = fopen(mpath, "rb");
    CHECK(f != NULL);
    if (f) {
        old = malloc(1 << 20);
        olen = fread(old, 1, 1 << 20, f);
        fclose(f);
    }
    int64_t d1 = (T0 / 86400000 + 1) * 86400000;
    CHECK(tsdb_delete_range(w, id, d1, INT64_MAX) == 0 && tsdb_optimize(w) == 0);
    CHECK(tsdb_test_class_switch(w, id, 3, NULL) == 0); /* from d1: the newest row left is before it */
    f = fopen(mpath, "r+b");
    CHECK(f != NULL && olen > 0);
    if (f) {
        CHECK(fwrite(old, 1, olen, f) == olen && ftruncate(fileno(f), (off_t)olen) == 0);
        fclose(f);
    }
    tsdb_options ro;
    tsdb_options_init(&ro);
    ro.read_only = 1;
    pthread_t th; /* the writer's next MANIFEST, a moment after the reader read the old one */
    CHECK(pthread_create(&th, NULL, republish, w) == 0);
    int rc = tsdb_open(dir, &ro, &r);
    pthread_join(th, NULL);
    if (rc)
        fprintf(stderr, "a reader on a MANIFEST older than CLASSES: open failed (%d)\n", rc);
    CHECK(rc == 0);
    if (r) {
        tsdb_points p;
        CHECK(tsdb_query_points(r, id, INT64_MIN, INT64_MAX, &p) == 0 && p.n == (size_t)(d1 - T0) / 60000 + 1);
        tsdb_points_free(&p);
        tsdb_close(r);
    }
    free(old);
    tsdb_close(w);
    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    CHECK(system(cmd) == 0);
    printf("  a reader opens on a MANIFEST older than a class switch\n");
}

/* The library's fstat calls land here (the test binary's definition wins over
 * libc's): the next one can report a file larger than it is, which is what a
 * reader sees when a MANIFEST shrinks while it is rewritten in place. */
static atomic_int stat_grow;

int fstat(int fd, struct stat *st)
{
#ifdef __APPLE__
    int rc = REAL(fstat)(fd, st);
#else
    int rc = fstatat(fd, "", st, AT_EMPTY_PATH);
#endif
    int grow = atomic_exchange(&stat_grow, 0);
    if (rc == 0)
        st->st_size += grow;
    return rc;
}

/* Regression: a MANIFEST read while the writer rewrote it (shorter than the
 * size a moment before) made a reader's refresh fail with "out of memory": the
 * error was chosen after the buffer was freed. It is retried at the next poll. */
static void test_torn_manifest(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/tsdb-follow-torn-%d", base, (int)getpid());
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    tsdb *w, *r = NULL;
    tsdb_sid id;
    if (tsdb_open(dir, &o, &w) != 0) {
        CHECK(!"open writer");
        return;
    }
    CHECK(tsdb_series(w, "s", &id) == 0 && tsdb_append(w, id, T0, 1.0f) == 0 && tsdb_flush(w) == 0);
    tsdb_options ro;
    tsdb_options_init(&ro);
    ro.read_only = 1; /* no polling thread: refreshes happen here */
    CHECK(tsdb_open(dir, &ro, &r) == 0);
    if (r) {
        atomic_store(&stat_grow, 1); /* the MANIFEST's fstat: the first one of a refresh */
        CHECK(tsdb_refresh(r) == 0);
        CHECK(atomic_load(&stat_grow) == 0);
        CHECK(tsdb_append(w, id, T0 + 1, 2.0f) == 0 && tsdb_flush(w) == 0);
        CHECK(tsdb_refresh(r) == 0); /* the next one catches up */
        tsdb_points p;
        CHECK(tsdb_query_points(r, id, T0, T0 + 2, &p) == 0 && p.n == 2);
        tsdb_points_free(&p);
        tsdb_close(r);
    }
    tsdb_close(w);
    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    CHECK(system(cmd) == 0);
    printf("  a MANIFEST torn by a rewrite is read again at the next refresh\n");
}

static uint64_t manifest_version(const char *dir)
{
    char path[600], text[256];
    snprintf(path, sizeof path, "%s/MANIFEST", dir);
    FILE *f = fopen(path, "r");
    unsigned long long v = 0;
    if (f) {
        size_t n = fread(text, 1, sizeof text - 1, f);
        text[n] = '\0';
        char *line = strstr(text, "\nversion ");
        if (line)
            v = strtoull(line + 9, NULL, 10);
        fclose(f);
    }
    return v;
}

/* Every writer counts its MANIFEST versions from 1 again. Writers that die before they flush (kill -9, power loss)
 * each publish the same versions at their open: one for the replay of the segment the last one left, one for the
 * open. A follower that saw the second of two such writers holds its version and a WAL tail of its segment; the next
 * writer publishes that version too, with other parts and a new active segment. The follower must take that
 * MANIFEST, or it shows nothing the new writer appends until the writer happens to publish again (jepsen power:
 * "the follower does not catch up"). */
static void test_writer_restart(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/tsdb-follow-restart-%d", base, (int)getpid());
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.merge_threads = 0; /* no merge publishes a version of its own */
    tsdb *w = NULL, *r = NULL;
    tsdb_sid id = 0;
    for (int i = 0; i < 2; i++) { /* writers with one row in their WAL each, dead without a flush or a close */
        pid_t pid = fork();
        if (pid == 0)
            _exit(tsdb_open(dir, &o, &w) != 0 || tsdb_series(w, "s", &id) != 0 ||
                  tsdb_append(w, id, T0 + i, 1.0f) != 0);
        int st;
        CHECK(pid > 0 && waitpid(pid, &st, 0) == pid && WIFEXITED(st) && WEXITSTATUS(st) == 0);
    }
    tsdb_options ro;
    tsdb_options_init(&ro);
    ro.read_only = 1; /* no polling thread: refreshes happen here */
    CHECK(tsdb_open(dir, &ro, &r) == 0);
    if (r) {
        uint64_t seen = manifest_version(dir);
        tsdb_points p;
        CHECK(tsdb_series_lookup(r, "s", &id) == 0);
        CHECK(tsdb_query_points(r, id, T0, T0 + 3, &p) == 0 && p.n == 2); /* a part, and the last one's WAL */
        tsdb_points_free(&p);
        CHECK(tsdb_open(dir, &o, &w) == 0);
        if (w) {
            CHECK(manifest_version(dir) == seen); /* the case under test: the same version, another MANIFEST */
            CHECK(tsdb_append(w, id, T0 + 2, 2.0f) == 0);
            CHECK(tsdb_refresh(r) == 0);
            CHECK(tsdb_query_points(r, id, T0, T0 + 3, &p) == 0);
            CHECK(p.n == 3); /* the new writer's row, from its new segment */
            tsdb_points_free(&p);
            tsdb_close(w);
        }
        tsdb_close(r);
    }
    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    CHECK(system(cmd) == 0);
    printf("  a restarted writer's MANIFEST with the version the follower has is still read\n");
}

/* A series the writer creates after the follower opened, with no rows yet (no MANIFEST change, no WAL row):
 * the follower finds it by name and by id, from SERIES alone. */
static void test_rowless_series(const char *base)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/tsdb-follow-rowless-%d", base, (int)getpid());
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    tsdb *w, *r = NULL;
    tsdb_sid first, late, late2 = 0, got = 0;
    if (tsdb_open(dir, &o, &w) != 0) {
        CHECK(!"open writer");
        return;
    }
    CHECK(tsdb_series(w, "first", &first) == 0 && tsdb_append(w, first, T0, 1.0f) == 0 && tsdb_flush(w) == 0);
    tsdb_options ro;
    tsdb_options_init(&ro);
    ro.read_only = 1; /* no polling thread */
    CHECK(tsdb_open(dir, &ro, &r) == 0);
    if (r) {
        CHECK(tsdb_series(w, "late", &late) == 0 && tsdb_series(w, "late2", &late2) == 0);
        CHECK(tsdb_series_lookup(r, "late", &got) == 0 && got == late); /* by name */
        const char *name = tsdb_series_name(r, late2);                   /* by id */
        CHECK(name && strcmp(name, "late2") == 0);
        CHECK(tsdb_series_lookup(r, "never", &got) == TSDB_ERR_NOTFOUND);
        tsdb_close(r);
    }
    tsdb_close(w);
    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    CHECK(system(cmd) == 0);
    printf("  a series created after the follower opened, with no rows, is found by name and by id\n");
}

int main(int argc, char **argv)
{
    char dir[512];
    snprintf(dir, sizeof dir, "%s/tsdb-follow-%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (system(cmd) != 0)
        return 1;
    shared *sh = mmap(NULL, sizeof *sh, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    memset(sh, 0, sizeof *sh);
    pid_t pid = fork();
    if (pid == 0) {
        writer(dir, sh);
        _exit(sh->writer_failed);
    }

    while (sh->phase == 0 && !sh->writer_failed)
        usleep(1000);
    tsdb_options o;
    tsdb_options_init(&o);
    o.follow = 1;
    o.refresh_ms = 20;
    tsdb *db = NULL;
    for (int k = 0; k < 200 && tsdb_open(dir, &o, &db) != 0; k++)
        usleep(10000);
    CHECK(db != NULL);
    if (!db)
        return 1;

    /* while the writer appends: acknowledged rows show up within a few refreshes */
    int fresh_checks = 0, fresh_ok = 0;
    size_t queries = 0;
    bool ordered = true;
    while (sh->phase == 1) {
        long long want = sh->acked[0];
        usleep(100000); /* five refresh intervals */
        tsdb_points p;
        if (tsdb_query_points(db, TSDB_SID(0, 0), want, want + 1, &p) == 0) {
            fresh_checks++;
            fresh_ok += p.n == 1;
            tsdb_points_free(&p);
        }
    }
    /* while it overwrites, merges and retires: every snapshot is consistent */
    while (sh->phase == 2) {
        for (uint32_t s = 0; s < NS; s++) {
            size_t rows;
            double sum;
            ordered &= query_ok(db, s, &rows, &sum);
            queries++;
        }
        tsdb_bucket *b;
        size_t nb;
        if (tsdb_query_agg(db, TSDB_SID(0, 1), INT64_MIN, INT64_MAX, 60000, &b, &nb) == 0)
            free(b);
    }
    CHECK(ordered);
    CHECK(fresh_checks > 3 && fresh_ok == fresh_checks);

    /* after: the follower converges to exactly the writer's data */
    bool converged = false;
    double t0 = now();
    while (!converged && now() - t0 < 5) {
        converged = true;
        for (uint32_t s = 0; s <= NS; s++) {
            size_t rows = 0;
            double sum = 0;
            if (s == NS) {
                tsdb_sid id;
                if (tsdb_series_lookup(db, "sensor.late", &id) != 0 || id != TSDB_SID(0, NS)) {
                    converged = false;
                    break;
                }
            }
            if (!query_ok(db, s, &rows, &sum) || (long long)rows != sh->final_count[s] ||
                fabs(sum - sh->final_sum[s]) > 1e-6 * fmax(1, fabs(sum))) {
                converged = false;
                break;
            }
        }
        if (!converged)
            usleep(20000);
    }
    CHECK(converged);
    tsdb_stats st;
    tsdb_get_stats(db, &st);
    printf("  follower: %d freshness checks, %zu consistent queries during merges, %llu parts now\n", fresh_checks,
           queries, (unsigned long long)st.parts);
    tsdb_close(db);
    sh->follower_done = 1;
    int status;
    waitpid(pid, &status, 0);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    if (system(cmd) != 0)
        return 1;
    test_typed_follow(argc > 1 ? argv[1] : "/tmp");
    test_torn_manifest(argc > 1 ? argv[1] : "/tmp");
    test_rowless_series(argc > 1 ? argv[1] : "/tmp");
    test_writer_restart(argc > 1 ? argv[1] : "/tmp");
    test_class_follow(argc > 1 ? argv[1] : "/tmp");
    test_stale_manifest_classes(argc > 1 ? argv[1] : "/tmp");
    if (failures) {
        printf("FAILED: %d checks\n", failures);
        return 1;
    }
    printf("follower tests passed\n");
    return 0;
}
