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

#include <math.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define NS 4
#define T0 1700000000000LL

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
    uint32_t id;
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
            if (tsdb_append(db, (uint32_t)s, ts, (float)(i % 1000) / 10.0f)) {
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
            if (tsdb_insert_series(db, (uint32_t)s, ts, v, B)) {
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
        if (tsdb_query_agg(db, s, INT64_MIN, INT64_MAX, 0, &b, &nb) == 0 && nb == 1) {
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
    if (tsdb_query_points(db, s, INT64_MIN, INT64_MAX, &p))
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
    uint32_t cnt, mode;
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
        uint32_t fmode, fidle;
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
        if (tsdb_query_points(db, 0, want, want + 1, &p) == 0) {
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
        if (tsdb_query_agg(db, 1, INT64_MIN, INT64_MAX, 60000, &b, &nb) == 0)
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
                uint32_t id;
                if (tsdb_series_lookup(db, "sensor.late", &id) != 0 || id != NS) {
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
    if (failures) {
        printf("FAILED: %d checks\n", failures);
        return 1;
    }
    printf("follower tests passed\n");
    return 0;
}
