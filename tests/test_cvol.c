/* SPDX-License-Identifier: AGPL-3.0-only */
/* Cluster volumes in a tsdb handle (src/cvol.h; design-cluster.md 3.1, DECISION D2 of the R3 plan).
 *
 *  mode     a root that holds series, or a VOLUMES line, is refused as a cluster node's (naming them); a local
 *           volume add and a series in the root are refused on a cluster node
 *  open     a held volume opens from v<N> only when its META says volume=N; a directory whose META requires
 *           "cluster" is refused by an open outside the cluster path (tsdb_open)
 *  route    series are made and found by the volume's cluster name; a declared volume this server does not hold
 *           answers UNAVAILABLE (creation, query, insert), an unknown name NOTFOUND; a replica refuses writes
 *  promote  a replica volume becomes the writer (replica_promote through tsdb_cvol_replace) while reader threads
 *           query it: every answer is the rows or UNAVAILABLE, never another error; a name pointer handed out by the
 *           old engine stays valid (asan); the writer then takes rows
 *
 *   ./build/test_cvol [scratch dir]
 */
#define _GNU_SOURCE
#include "cvol.h"
#include "internal.h"
#include "replica.h"

#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
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
#define CHECK_EQ(a, b)                                                                                        \
    do {                                                                                                      \
        long long a_ = (long long)(a), b_ = (long long)(b);                                                   \
        if (a_ != b_) {                                                                                       \
            fprintf(stderr, "%s:%d: CHECK failed: %s == %s (%lld vs %lld)\n", __FILE__, __LINE__, #a, #b, a_, b_); \
            failures++;                                                                                       \
        }                                                                                                     \
    } while (0)

#define T0 1700000000000LL
#define NR 200
static char S[PATH_MAX / 2];

static void sh(const char *fmt, const char *a)
{
    char cmd[4 * PATH_MAX];
    snprintf(cmd, sizeof cmd, fmt, a, a, a);
    if (system(cmd) != 0) {
        fprintf(stderr, "failed: %s\n", cmd);
        exit(2);
    }
}

static tsdb_options opts(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.wal_sync_ms = 0;
    o.create_volumes = 0;
    return o;
}

/* A cluster volume's directory as tsdbd makes it, with series s0, s1 and NR rows each: an engine makes it, then META
 * gets the number and the features (the order tsdbd uses does not matter to the open). */
static void make_vol(const char *dir, uint32_t number, bool rows)
{
    tsdb_options o = opts();
    eng *db;
    if (eng_open(dir, &o, &db) != 0) {
        fprintf(stderr, "cannot create %s\n", dir);
        exit(2);
    }
    for (int s = 0; s < 2; s++) {
        uint32_t id;
        char nm[8];
        snprintf(nm, sizeof nm, "s%d", s);
        if (eng_series(db, nm, TSDB_FLOAT32, &id) != 0 || id != (uint32_t)s)
            exit(2);
        uint32_t ids[NR];
        int64_t ts[NR];
        float v[NR];
        for (int i = 0; i < NR; i++)
            ids[i] = id, ts[i] = T0 + i * 1000, v[i] = (float)(s * 1000 + i);
        if (rows && eng_insert(db, TSDB_FLOAT32, ids, ts, v, NR) != 0)
            exit(2);
    }
    eng_close(db);
    char p[PATH_MAX];
    snprintf(p, sizeof p, "%s/META", dir);
    sh("sed -i '/^volume=/d' '%s/META'", dir); /* the engine wrote volume=0 */
    FILE *f = fopen(p, "a");
    if (!f)
        exit(2);
    fprintf(f, "volume=%u\nrequires=cluster,replica\n", number);
    fclose(f);
}

static int count_fn(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    (void)ts, (void)v;
    *(size_t *)ctx += n;
    return 0;
}

static int qcount(tsdb *t, tsdb_sid id, size_t *n)
{
    *n = 0;
    return tsdb_query_raw(t, id, T0 - 1, T0 + 10 * NR * 1000LL, count_fn, n);
}

static void test_mode(void)
{
    char d[PATH_MAX];
    tsdb_options o = opts();
    tsdb *t;
    /* a root with a series */
    snprintf(d, sizeof d, "%s/root-series", S);
    sh("rm -rf '%s' && mkdir -p '%s'", d);
    CHECK_EQ(tsdb_open(d, &o, &t), 0);
    tsdb_sid id;
    CHECK_EQ(tsdb_series(t, "old", &id), 0);
    CHECK_EQ(tsdb_cvol_mode(t), TSDB_ERR_INVALID);
    tsdb_close(t);
    /* a root with a VOLUMES line */
    snprintf(d, sizeof d, "%s/root-vols", S);
    sh("rm -rf '%s' '%s.v' && mkdir -p '%s'", d);
    CHECK_EQ(tsdb_open(d, &o, &t), 0);
    char v[PATH_MAX];
    snprintf(v, sizeof v, "%s.v", d);
    uint32_t idx;
    CHECK_EQ(tsdb_volume_add(t, v, &idx), 0);
    CHECK_EQ(tsdb_cvol_mode(t), TSDB_ERR_INVALID);
    tsdb_close(t);
}

struct reader {
    tsdb *t;
    tsdb_sid id;
    atomic_bool stop;
    atomic_long ok, unavailable, other;
};

static void *reader_main(void *arg)
{
    struct reader *r = arg;
    while (!atomic_load(&r->stop)) {
        size_t n;
        int rc = qcount(r->t, r->id, &n);
        tsdb_type ty;
        int trc = tsdb_series_type(r->t, r->id, &ty);
        if (rc == 0 && n == NR && (trc == 0 || trc == TSDB_ERR_UNAVAILABLE))
            atomic_fetch_add(&r->ok, 1);
        else if (rc == TSDB_ERR_UNAVAILABLE && (trc == 0 || trc == TSDB_ERR_UNAVAILABLE))
            atomic_fetch_add(&r->unavailable, 1);
        else if (atomic_fetch_add(&r->other, 1) == 0)
            fprintf(stderr, "  reader: query %d (%zu rows), type %d\n", rc, n, trc);
    }
    return NULL;
}

static void replace_pause(void *arg)
{
    (void)arg;
    usleep(20000); /* readers meet the empty slot */
}

static void test_cluster(void)
{
    char root[PATH_MAX], v3[PATH_MAX], v5[PATH_MAX], v9[PATH_MAX];
    snprintf(root, sizeof root, "%s/node", S);
    sh("rm -rf '%s' && mkdir -p '%s'", root);
    snprintf(v3, sizeof v3, "%s/v3", root);
    snprintf(v5, sizeof v5, "%s/v5", root);
    snprintf(v9, sizeof v9, "%s/v9", root);
    make_vol(v3, 3, true);
    make_vol(v5, 5, true);
    make_vol(v9, 8, false); /* META names another number */

    tsdb_options o = opts();
    tsdb *x;
    CHECK(tsdb_open(v3, &o, &x) != 0); /* never opened alone (its META's number refuses it first) */
    eng *e = NULL;
    CHECK_EQ(eng_open(v3, &o, &e), TSDB_ERR_INVALID); /* requires=cluster: only through the cluster path */

    tsdb *t;
    CHECK_EQ(tsdb_open(root, &o, &t), 0);
    if (!t)
        exit(2);
    CHECK_EQ(tsdb_cvol_declare(t, 3, "alpha"), TSDB_ERR_INVALID); /* not a cluster handle yet */
    CHECK_EQ(tsdb_cvol_mode(t), 0);
    uint32_t idx;
    CHECK_EQ(tsdb_volume_add(t, v9, &idx), TSDB_ERR_INVALID);
    tsdb_sid id;
    CHECK_EQ(tsdb_series_create(t, "r", TSDB_FLOAT32, NULL, &id), TSDB_ERR_INVALID); /* no series in the root */
    CHECK_EQ(tsdb_series(t, "r", &id), TSDB_ERR_INVALID);
    CHECK_EQ(tsdb_cvol_declare(t, 3, "alpha"), 0);
    CHECK_EQ(tsdb_cvol_declare(t, 3, "alpha"), 0);
    CHECK_EQ(tsdb_cvol_declare(t, 3, "other"), TSDB_ERR_INVALID); /* names never change */
    CHECK_EQ(tsdb_cvol_declare(t, 4, "alpha"), TSDB_ERR_INVALID); /* nor are given twice */
    CHECK_EQ(tsdb_cvol_declare(t, 5, "rep"), 0);
    CHECK_EQ(tsdb_cvol_declare(t, 7, "beta"), 0);
    CHECK_EQ(tsdb_cvol_declare(t, 9, "nine"), 0);
    CHECK_EQ(tsdb_cvol_declare(t, 65535, "big"), TSDB_ERR_INVALID);
    CHECK_EQ(tsdb_cvol_open(t, 4, v3, false, 0), TSDB_ERR_INVALID); /* not declared */
    CHECK_EQ(tsdb_cvol_open(t, 9, v9, false, 0), TSDB_ERR_CORRUPT);
    CHECK_EQ(tsdb_cvol_open(t, 3, v3, false, 0), 0);
    CHECK_EQ(tsdb_cvol_open(t, 3, v3, false, 0), TSDB_ERR_INVALID); /* open already */
    CHECK_EQ(tsdb_cvol_open(t, 5, v5, true, 0), 0);
    CHECK_EQ(tsdb_cvol_role(t, 3), CVOL_WRITER);
    CHECK_EQ(tsdb_cvol_role(t, 5), CVOL_REPLICA);
    CHECK_EQ(tsdb_cvol_role(t, 7), CVOL_NONE);
    uint32_t num;
    CHECK_EQ(tsdb_cvol_find(t, "beta", &num), 0);
    CHECK_EQ(num, 7);

    /* route by the volume's name */
    tsdb_sid a0, a2, r0;
    CHECK_EQ(tsdb_series_at(t, "s0", "alpha", &a0), 0);
    CHECK_EQ(a0, TSDB_SID(3, 0));
    CHECK_EQ(tsdb_series_create(t, "s2", TSDB_FLOAT32, "alpha", &a2), 0);
    CHECK_EQ(a2, TSDB_SID(3, 2));
    CHECK_EQ(tsdb_series_at(t, "s0", "rep", &r0), 0); /* names are per volume */
    CHECK_EQ(r0, TSDB_SID(5, 0));
    CHECK_EQ(tsdb_series_at(t, "x", "beta", &id), TSDB_ERR_UNAVAILABLE); /* declared, held elsewhere */
    CHECK_EQ(tsdb_series_at(t, "x", "gamma", &id), TSDB_ERR_NOTFOUND);
    CHECK_EQ(tsdb_series_create(t, "new", TSDB_FLOAT32, "rep", &id), TSDB_ERR_READONLY); /* a replica */
    size_t n;
    CHECK_EQ(qcount(t, a0, &n), 0);
    CHECK_EQ(n, NR);
    CHECK_EQ(qcount(t, r0, &n), 0);
    CHECK_EQ(n, NR);
    int64_t ts = T0 + NR * 1000;
    float val = 1;
    CHECK_EQ(tsdb_insert(t, &a2, &ts, &val, 1), 0);
    CHECK_EQ(qcount(t, a2, &n), 0);
    CHECK_EQ(n, 1);
    CHECK(tsdb_insert(t, &r0, &ts, &val, 1) != 0); /* a replica takes records only */
    tsdb_sid b0 = TSDB_SID(7, 0), mix[2] = {a2, b0};
    int64_t tss[2] = {ts + 1, ts + 1};
    float vs[2] = {2, 2};
    CHECK_EQ(qcount(t, b0, &n), TSDB_ERR_UNAVAILABLE);
    CHECK_EQ(tsdb_insert(t, &b0, &ts, &val, 1), TSDB_ERR_UNAVAILABLE);
    CHECK_EQ(tsdb_insert(t, mix, tss, vs, 2), TSDB_ERR_UNAVAILABLE);
    CHECK_EQ(qcount(t, TSDB_SID(6, 0), &n), 0); /* no such volume: nothing */
    CHECK_EQ(n, 0);
    CHECK_EQ(tsdb_flush(t), 0); /* the holes are skipped */

    /* promote volume 5 under readers */
    const char *nm = tsdb_series_name(t, r0);
    CHECK(nm && strcmp(nm, "s0") == 0);
    struct eng *rep = tsdb_cvol_ref(t, 5);
    CHECK(rep != NULL);
    if (!rep)
        exit(2);
    uint64_t pos = 0;
    CHECK_EQ(replica_epoch_begin(rep, 1, "node-a", &pos), 0);
    CHECK_EQ(replica_sync(rep), 0);
    CHECK_EQ(replica_apply(rep, pos), 0);
    tsdb_cvol_unref(&rep);
    struct reader rd = {.t = t, .id = r0};
    pthread_t th[4];
    for (int i = 0; i < 4; i++)
        pthread_create(&th[i], NULL, reader_main, &rd);
    usleep(20000);
    tsdb_cvol_test_replace_hook = replace_pause;
    CHECK_EQ(tsdb_cvol_promote(t, 3, pos), TSDB_ERR_INVALID); /* a writer already */
    CHECK_EQ(tsdb_cvol_promote(t, 5, pos), 0);
    tsdb_cvol_test_replace_hook = NULL;
    usleep(20000);
    atomic_store(&rd.stop, true);
    for (int i = 0; i < 4; i++)
        pthread_join(th[i], NULL);
    fprintf(stderr, "  promote under readers: %ld answers, %ld unavailable, %ld other\n", atomic_load(&rd.ok),
            atomic_load(&rd.unavailable), atomic_load(&rd.other));
    CHECK_EQ(atomic_load(&rd.other), 0);
    CHECK(atomic_load(&rd.ok) > 0);
    CHECK(atomic_load(&rd.unavailable) > 0);
    CHECK_EQ(tsdb_cvol_role(t, 5), CVOL_WRITER);
    CHECK(strcmp(nm, "s0") == 0); /* the old engine's string, kept */
    CHECK_EQ(tsdb_insert(t, &r0, &ts, &val, 1), 0);
    CHECK_EQ(qcount(t, r0, &n), 0);
    CHECK_EQ(n, NR + 1);
    CHECK_EQ(tsdb_series_create(t, "new", TSDB_FLOAT32, "rep", &id), 0);
    CHECK_EQ(id, TSDB_SID(5, 2));
    CHECK_EQ(tsdb_close(t), 0);
}

int main(int argc, char **argv)
{
    const char *base = argc > 1 ? argv[1] : "/tmp";
    snprintf(S, sizeof S, "%s/test_cvol.%d", base, (int)getpid());
    sh("rm -rf '%s' && mkdir -p '%s'", S);
    test_mode();
    test_cluster();
    if (failures) {
        fprintf(stderr, "test_cvol: %d FAILED (%s kept)\n", failures, S);
        return 1;
    }
    sh("rm -rf '%s'", S);
    printf("test_cvol: OK\n");
    return 0;
}
