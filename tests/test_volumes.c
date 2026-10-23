/* Volumes: series stored in directories of their own, routed by the root. */
#define _GNU_SOURCE
#include "tsdb.h"

#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int failures;
#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                              \
        }                                                                            \
    } while (0)
#define OK(expr)                                                                                    \
    do {                                                                                            \
        int rc_ = (expr);                                                                           \
        if (rc_ != 0) {                                                                             \
            fprintf(stderr, "%s:%d: %s -> %s\n", __FILE__, __LINE__, #expr, tsdb_strerror(rc_)); \
            exit(1);                                                                                \
        }                                                                                           \
    } while (0)

#define NS 6
#define T0 1700000000000LL
#define SPAN 20000

/* the expected value at each (series, time slot); NAN-free: 0 means unset */
static float want[NS][SPAN];

static uint64_t rng = 12345;
static uint64_t rnd(void)
{
    rng ^= rng << 13;
    rng ^= rng >> 7;
    rng ^= rng << 17;
    return rng;
}

static void check_all(tsdb *db, const uint32_t *ids)
{
    for (int s = 0; s < NS; s++) {
        tsdb_points p;
        OK(tsdb_query_points(db, ids[s], INT64_MIN, INT64_MAX, &p));
        size_t k = 0;
        bool ok = true;
        for (int i = 0; i < SPAN; i++) {
            if (want[s][i] == 0)
                continue;
            if (k >= p.n || p.ts[k] != T0 + (int64_t)i * 1000 || p.values[k] != want[s][i])
                ok = false;
            k++;
        }
        CHECK(ok && k == p.n);
        tsdb_bucket *b;
        size_t nb;
        OK(tsdb_query_agg(db, ids[s], INT64_MIN, INT64_MAX, 0, &b, &nb));
        CHECK(nb == (k ? 1u : 0u) && (!nb || b[0].count == k));
        free(b);
        tsdb_points_free(&p);
    }
}

typedef struct wjob {
    tsdb *db;
    uint32_t id;
    int s;
} wjob;

static void *vol_writer(void *arg)
{
    wjob *j = arg;
    for (int i = 0; i < 2000; i++) {
        float v = (float)(j->s * 1000 + i + 1);
        int slot = i * 7 % SPAN;
        if (tsdb_append(j->db, j->id, T0 + (int64_t)slot * 1000, v) == 0)
            want[j->s][slot] = v;
    }
    return NULL;
}

int main(int argc, char **argv)
{
    const char *base = argc > 1 ? argv[1] : "/tmp";
    char root[512], va[512], vb[512], other[512], cmd[2200];
    snprintf(root, sizeof root, "%s/tsdb-vol-%d/root", base, (int)getpid());
    snprintf(va, sizeof va, "%s/tsdb-vol-%d/mount_a/sensors", base, (int)getpid());
    snprintf(vb, sizeof vb, "%s/tsdb-vol-%d/mount_b/sensors", base, (int)getpid());
    snprintf(other, sizeof other, "%s/tsdb-vol-%d/other", base, (int)getpid());
    snprintf(cmd, sizeof cmd, "rm -rf '%s/tsdb-vol-%d'", base, (int)getpid());
    if (system(cmd) != 0)
        return 1;
    char mk[1200];
    snprintf(mk, sizeof mk, "mkdir -p '%s/tsdb-vol-%d/mount_a' '%s/tsdb-vol-%d/mount_b'", base, (int)getpid(), base,
             (int)getpid());
    if (system(mk) != 0)
        return 1;

    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.granule_rows = 128;
    o.partition_ms = 3600000;
    o.wal_max_batch_rows = 500;
    tsdb *db;
    OK(tsdb_open(root, &o, &db));
    const char *where[NS] = {NULL, va, vb, va, NULL, vb};
    uint32_t ids[NS];
    char name[32];
    for (int s = 0; s < NS; s++) {
        snprintf(name, sizeof name, "sensor.%d", s);
        OK(tsdb_series_at(db, name, where[s], &ids[s]));
        CHECK(ids[s] == (uint32_t)s);
    }
    CHECK(tsdb_volume_count(db) == 3);
    uint32_t again;
    OK(tsdb_series_at(db, "sensor.1", vb, &again)); /* exists: stays where it is */
    CHECK(again == ids[1] && strstr(tsdb_series_path(db, again), "mount_a") != NULL);
    CHECK(strcmp(tsdb_series_name(db, ids[5]), "sensor.5") == 0);

    /* batches mixing volumes, unsorted, with duplicates: last write wins */
    for (int round = 0; round < 30; round++) {
        size_t n = 1 + rnd() % 2000;
        uint32_t *s = malloc(n * sizeof *s);
        int64_t *t = malloc(n * sizeof *t);
        float *v = malloc(n * sizeof *v);
        for (size_t i = 0; i < n; i++) {
            int ser = (int)(rnd() % NS), slot = (int)(rnd() % SPAN);
            s[i] = ids[ser];
            t[i] = T0 + (int64_t)slot * 1000;
            v[i] = (float)(round * 10000 + (int)i + 1);
        }
        OK(tsdb_insert(db, s, t, v, n));
        for (size_t i = 0; i < n; i++) /* in order: a later duplicate replaces an earlier one */
            for (int ser = 0; ser < NS; ser++)
                if (s[i] == ids[ser])
                    want[ser][(t[i] - T0) / 1000] = v[i];
        free(s);
        free(t);
        free(v);
    }
    check_all(db, ids);

    /* parallel writers, each to its own series (its own volume's WAL) */
    pthread_t th[NS];
    wjob jobs[NS];
    for (int s = 0; s < NS; s++) {
        jobs[s] = (wjob){db, ids[s], s};
        pthread_create(&th[s], NULL, vol_writer, &jobs[s]);
    }
    for (int s = 0; s < NS; s++)
        pthread_join(th[s], NULL);
    check_all(db, ids);
    OK(tsdb_optimize(db));
    check_all(db, ids);
    tsdb_stats st;
    tsdb_get_stats(db, &st);
    printf("  %u series in %u volumes, %llu parts\n", tsdb_series_count(db), tsdb_volume_count(db),
           (unsigned long long)st.parts);

    /* a follower of the root sees all volumes, and a volume created after it opened */
    tsdb_options fo = o;
    fo.follow = 1;
    fo.refresh_ms = 20;
    tsdb *f;
    OK(tsdb_open(root, &fo, &f));
    check_all(f, ids);
    char vc[600];
    snprintf(vc, sizeof vc, "%s/tsdb-vol-%d/mount_c", base, (int)getpid());
    uint32_t late;
    OK(tsdb_series_at(db, "late.on_c", vc, &late));
    int64_t lt = T0;
    float lv = 7;
    OK(tsdb_insert_series(db, late, &lt, &lv, 1));
    uint32_t seen = 0;
    int rc = TSDB_ERR_NOTFOUND;
    for (int k = 0; k < 100 && rc; k++) {
        rc = tsdb_series_lookup(f, "late.on_c", &seen);
        if (rc)
            usleep(20000);
    }
    CHECK(rc == 0 && seen == late);
    tsdb_points p = {0};
    for (int k = 0; k < 100 && p.n == 0; k++) {
        tsdb_points_free(&p);
        OK(tsdb_query_points(f, seen, INT64_MIN, INT64_MAX, &p));
        if (!p.n)
            usleep(20000);
    }
    CHECK(p.n == 1 && p.values[0] == 7);
    tsdb_points_free(&p);
    OK(tsdb_close(f));
    OK(tsdb_close(db));

    /* reopen: same ids, names, paths and data */
    OK(tsdb_open(root, &o, &db));
    CHECK(tsdb_series_count(db) == NS + 1 && tsdb_volume_count(db) == 4);
    for (int s = 0; s < NS; s++) {
        uint32_t id;
        snprintf(name, sizeof name, "sensor.%d", s);
        OK(tsdb_series_lookup(db, name, &id));
        CHECK(id == ids[s]);
    }
    check_all(db, ids);
    OK(tsdb_close(db));

    /* crash between creating a series in its volume and recording it: adopted at open */
    char cat[600];
    snprintf(cat, sizeof cat, "%s/CATALOG", root);
    struct stat cs;
    CHECK(stat(cat, &cs) == 0 && cs.st_size == (NS + 1) * 8);
    CHECK(truncate(cat, cs.st_size - 8) == 0); /* "late.on_c" is no longer recorded */
    OK(tsdb_open(root, &o, &db));
    uint32_t back;
    OK(tsdb_series_lookup(db, "late.on_c", &back));
    CHECK(back == late);
    OK(tsdb_query_points(db, back, INT64_MIN, INT64_MAX, &p));
    CHECK(p.n == 1);
    tsdb_points_free(&p);

    /* a volume belongs to one database */
    tsdb *db2;
    OK(tsdb_open(other, &o, &db2));
    uint32_t x;
    CHECK(tsdb_series_at(db2, "steal", va, &x) == TSDB_ERR_INVALID);
    OK(tsdb_close(db2));
    OK(tsdb_close(db));

    if (system(cmd) != 0)
        return 1;
    if (failures) {
        printf("FAILED: %d checks\n", failures);
        return 1;
    }
    printf("volume tests passed\n");
    return 0;
}
