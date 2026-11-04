/* Tests: range deletes, replaces and retention.
 *
 * The core is model based: random inserts (small ones through the WAL, large
 * ones straight into parts), deletes, replaces, flushes, merges and reopens of
 * a few int64 series, checked against a reference array after every step, and
 * the same sequence killed at a random point in a child process: the database
 * must then hold the state after the last acknowledged operation or after the
 * one in flight, never a mix. Then: granule statistics around tombstones,
 * followers, and retention with a clock that jumps. */
#include "internal.h"

#include <dirent.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
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
#define OK(expr)                                                                                 \
    do {                                                                                         \
        int rc_ = (expr);                                                                        \
        if (rc_ != 0) {                                                                          \
            fprintf(stderr, "%s:%d: %s -> %s\n", __FILE__, __LINE__, #expr, tsdb_strerror(rc_)); \
            exit(1);                                                                             \
        }                                                                                        \
    } while (0)

static const char *scratch = "/tmp";

static uint64_t rnd(uint64_t *s)
{
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    return *s;
}

/* kill(-1) would reach every process of the user */
static void kill_child(pid_t pid)
{
    if (pid <= 0) {
        fprintf(stderr, "refusing to signal pid %d\n", (int)pid);
        exit(1);
    }
    kill(pid, SIGKILL);
}

static void rmrf(const char *dir)
{
    char cmd[1200];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (system(cmd) != 0)
        exit(1);
}

/* ------------------------------------------------------------------ model */

#define NS 3
#define SID(s) TSDB_SID(0, TSDB_SID_LOCAL(s)) /* every series here is in the database directory: an index or an id */
#define D 16000 /* timestamps 0..D-1: eight partitions of 2000 ms */
#define ABSENT INT64_MAX
#define NULLV INT64_MIN

typedef struct {
    int64_t v[NS][D];
} model;

enum { OP_SMALL, OP_LARGE, OP_DELETE, OP_REPLACE, OP_FLUSH, OP_OPTIMIZE, OP_REOPEN, OP_KINDS };

typedef struct {
    int kind;
    uint32_t s;
    int64_t a, b;
    size_t n;
    int64_t ts[2000];
    int64_t v[2000];
    uint8_t valid[250];
} op;

static void gen_op(uint64_t *r, op *o, bool reopen)
{
    uint64_t k = rnd(r) % 100;
    o->kind = k < 30 ? OP_SMALL : k < 45 ? OP_LARGE : k < 65 ? OP_DELETE : k < 80 ? OP_REPLACE : k < 88 ? OP_FLUSH
              : k < 95                                                                                ? OP_OPTIMIZE
                                                                                                      : OP_REOPEN;
    if (o->kind == OP_REOPEN && !reopen)
        o->kind = OP_FLUSH;
    o->s = (uint32_t)(rnd(r) % NS);
    int64_t w = 1 + (int64_t)(rnd(r) % (rnd(r) % 4 == 0 ? D : 3000));
    o->a = (int64_t)(rnd(r) % D);
    o->b = MIN(D, o->a + w);
    if (rnd(r) % 10 == 0) /* sometimes far outside the data */
        o->b = INT64_MAX;
    size_t span = (size_t)(MIN(o->b, D) - o->a);
    o->n = o->kind == OP_SMALL ? 1 + rnd(r) % 40 : o->kind == OP_LARGE ? 300 + rnd(r) % 1700
           : o->kind == OP_REPLACE                                     ? rnd(r) % MIN(span + 1, 1200)
                                                                       : 0;
    memset(o->valid, 0xff, sizeof o->valid);
    for (size_t i = 0; i < o->n; i++) {
        o->ts[i] = o->kind == OP_REPLACE ? o->a + (int64_t)(rnd(r) % span) : (int64_t)(rnd(r) % D);
        if (o->kind == OP_LARGE && i)
            o->ts[i] = (o->ts[i - 1] + 1 + (int64_t)(rnd(r) % 8)) % D;
        o->v[i] = (int64_t)(rnd(r) % 2000001) - 1000000;
        if (rnd(r) % 50 == 0)
            o->valid[i / 8] &= (uint8_t)~(1u << (i % 8));
    }
}

static void model_apply(model *m, const op *o)
{
    if (o->kind == OP_DELETE || o->kind == OP_REPLACE)
        for (int64_t t = o->a; t < MIN(o->b, D); t++)
            m->v[o->s][t] = ABSENT;
    if (o->kind == OP_SMALL || o->kind == OP_LARGE || o->kind == OP_REPLACE)
        for (size_t i = 0; i < o->n; i++)
            m->v[o->s][o->ts[i]] = o->valid[i / 8] >> (i % 8) & 1 ? o->v[i] : NULLV;
}

static tsdb_options test_opts(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.partition_ms = 2000;
    o.granule_rows = 64;
    o.wal_max_batch_rows = 200;
    o.merge_idle_s = -1;
    return o;
}

static const tsdb_options *reopen_opts; /* NULL: test_opts() */

static void db_apply(tsdb **db, const char *dir, const op *o)
{
    tsdb_values v = {TSDB_INT64, o->v, o->valid};
    switch (o->kind) {
    case OP_SMALL:
    case OP_LARGE:
        OK(tsdb_insert_series_values(*db, SID(o->s), o->ts, &v, o->n));
        break;
    case OP_DELETE:
        OK(tsdb_delete_range(*db, SID(o->s), o->a, o->b));
        break;
    case OP_REPLACE:
        OK(tsdb_replace_range(*db, SID(o->s), o->a, o->b, o->ts, &v, o->n));
        break;
    case OP_FLUSH:
        OK(tsdb_flush(*db));
        break;
    case OP_OPTIMIZE:
        OK(tsdb_optimize(*db));
        break;
    case OP_REOPEN: {
        OK(tsdb_close(*db));
        tsdb_options opt = reopen_opts ? *reopen_opts : test_opts();
        OK(tsdb_open(dir, &opt, db));
        break;
    }
    }
}

static void create_series(tsdb *db)
{
    for (uint32_t s = 0; s < NS; s++) {
        char name[16];
        tsdb_sid id;
        snprintf(name, sizeof name, "s%u", s);
        OK(tsdb_series_create(db, name, TSDB_INT64, NULL, &id));
        CHECK(id == SID(s));
    }
}

typedef struct {
    int64_t ts[D];
    int64_t v[D];
    size_t n;
} rows;

static int collect(void *ctx, const int64_t *ts, const tsdb_values *v, size_t n)
{
    rows *r = ctx;
    for (size_t i = 0; i < n && r->n < D; i++, r->n++) {
        r->ts[r->n] = ts[i];
        r->v[r->n] = !v->valid || v->valid[i / 8] >> (i % 8) & 1 ? ((const int64_t *)v->values)[i] : NULLV;
    }
    return 0;
}

/* whether the database's series s matches the model; quiet: only report */
static bool series_matches(tsdb *db, const model *m, uint32_t s, bool quiet)
{
    static rows r;
    r.n = 0;
    if (tsdb_query_values(db, SID(s), INT64_MIN, INT64_MAX, NULL, collect, &r) != 0)
        return false;
    size_t k = 0;
    for (int64_t t = 0; t < D; t++) {
        if (m->v[s][t] == ABSENT)
            continue;
        if (k >= r.n || r.ts[k] != t || r.v[k] != m->v[s][t]) {
            if (!quiet)
                fprintf(stderr, "  series %u ts %" PRId64 ": expected %" PRId64 ", got %s ts %" PRId64 " = %" PRId64
                        "\n", s, t, m->v[s][t], k < r.n ? "" : "nothing,", k < r.n ? r.ts[k] : 0,
                        k < r.n ? r.v[k] : 0);
            return false;
        }
        k++;
    }
    if (k != r.n && !quiet)
        fprintf(stderr, "  series %u: %zu extra rows, first ts %" PRId64 "\n", s, r.n - k, r.ts[k]);
    return k == r.n;
}

/* aggregates and descending limited queries over random ranges */
static void check_queries(tsdb *db, const model *m, uint64_t *r)
{
    for (int q = 0; q < 6; q++) {
        uint32_t s = (uint32_t)(rnd(r) % NS);
        int64_t a = (int64_t)(rnd(r) % D), b = a + 1 + (int64_t)(rnd(r) % D);
        int64_t interval = q % 2 ? 0 : 500;
        tsdb_bucket *bk = NULL;
        size_t nb = 0;
        OK(tsdb_query_agg(db, SID(s), a, b, interval, &bk, &nb));
        size_t j = 0;
        for (int64_t lo = interval ? a - a % interval : a; lo < MIN(b, D); lo = interval ? lo + interval : D) {
            uint64_t cnt = 0, rowsin = 0;
            int64_t sum = 0, mn = INT64_MAX, mx = INT64_MIN;
            for (int64_t t = MAX(lo, a); t < MIN(MIN(b, D), interval ? lo + interval : D); t++) {
                int64_t v = m->v[s][t];
                if (v == ABSENT)
                    continue;
                rowsin++;
                if (v == NULLV)
                    continue;
                cnt++;
                sum += v;
                mn = MIN(mn, v);
                mx = MAX(mx, v);
            }
            if (!rowsin)
                continue;
            bool ok = j < nb && bk[j].count == cnt && (!cnt || (bk[j].isum == sum && bk[j].imin == mn &&
                                                                  bk[j].imax == mx));
            if (!ok)
                fprintf(stderr, "  agg series %u [%" PRId64 ",%" PRId64 ") bucket %" PRId64 ": count %" PRIu64
                        " sum %" PRId64 " expected, got %" PRIu64 " %" PRId64 "\n", s, a, b, lo, cnt, sum,
                        j < nb ? bk[j].count : 0, j < nb ? bk[j].isum : 0);
            CHECK(ok);
            j++;
        }
        CHECK(j == nb);
        free(bk);

        static rows rr;
        rr.n = 0;
        tsdb_query_opts qo = {TSDB_DESC, 1 + rnd(r) % 100};
        OK(tsdb_query_values(db, SID(s), a, b, &qo, collect, &rr));
        size_t k = 0;
        for (int64_t t = MIN(b, D) - 1; t >= a && k < qo.limit; t--) {
            if (m->v[s][t] == ABSENT)
                continue;
            bool ok = k < rr.n && rr.ts[k] == t && rr.v[k] == m->v[s][t];
            CHECK(ok);
            if (!ok)
                break;
            k++;
        }
        CHECK(k == rr.n || failures);
    }
}

static int mixed_parts(const char *dir);
static int model_mixed; /* model steps that saw a part with both rows and tombstones */

static void test_model(uint64_t seed, int steps)
{
    char dir[1024];
    snprintf(dir, sizeof dir, "%s/tsdb_delete_model", scratch);
    rmrf(dir);
    tsdb_options opt = test_opts();
    opt.merge_threads = seed % 2 ? 2 : 0;
    if (seed % 2) { /* background merges of a few parts at a time: tombstones kept by partial merges */
        opt.merge_idle_s = 1;
        opt.max_part_bytes = 12000;
    }
    reopen_opts = &opt;
    tsdb *db;
    OK(tsdb_open(dir, &opt, &db));
    create_series(db);
    static model m;
    for (uint32_t s = 0; s < NS; s++)
        for (int64_t t = 0; t < D; t++)
            m.v[s][t] = ABSENT;
    static op o;
    uint64_t r = seed, qr = seed * 31 + 7;
    int start_failures = failures;
    for (int i = 0; i < steps && failures == start_failures; i++) {
        gen_op(&r, &o, true);
        if (seed % 2 && o.kind == OP_OPTIMIZE)
            o.kind = OP_FLUSH; /* parts pile up: level merges of the newer, smaller ones leave older ones out */
        db_apply(&db, dir, &o);
        model_apply(&m, &o);
        for (uint32_t s = 0; s < NS; s++)
            if (!series_matches(db, &m, s, false)) {
                fprintf(stderr, "model seed %" PRIu64 " step %d (op %d series %u [%" PRId64 ",%" PRId64
                        ") n %zu)\n", seed, i, o.kind, o.s, o.a, o.b, o.n);
                failures++;
                break;
            }
        check_queries(db, &m, &qr);
        if (seed % 2)
            model_mixed += mixed_parts(dir) > 0;
    }
    if (getenv("TSDB_DELETE_VERBOSE")) {
        tsdb_stats st;
        tsdb_get_stats(db, &st);
        printf("model seed %" PRIu64 ": %" PRIu64 " background merges, %d steps with mixed parts\n", seed, st.merges,
               model_mixed);
    }
    OK(tsdb_optimize(db));
    for (uint32_t s = 0; s < NS; s++)
        CHECK(series_matches(db, &m, s, false));
    OK(tsdb_close(db));
    reopen_opts = NULL;
}

/* A child runs the sequence and reports each acknowledged operation through a
 * pipe; it is killed at a random moment. The reopened database must hold the
 * model after the acknowledged operations, or after one more. */
static void test_crash(uint64_t seed)
{
    char dir[1024];
    snprintf(dir, sizeof dir, "%s/tsdb_delete_crash", scratch);
    rmrf(dir);
    int pfd[2];
    if (pipe(pfd) != 0)
        exit(1);
    pid_t pid = fork();
    if (pid == 0) {
        close(pfd[0]);
        tsdb_options opt = test_opts();
        tsdb *db;
        OK(tsdb_open(dir, &opt, &db));
        create_series(db);
        static op o;
        uint64_t r = seed;
        for (int i = 0;; i++) {
            gen_op(&r, &o, true);
            db_apply(&db, dir, &o);
            if (write(pfd[1], "x", 1) != 1)
                _exit(1);
        }
    }
    close(pfd[1]);
    struct timespec d = {0, (long)(20 + seed * 7919 % 400) * 1000000};
    nanosleep(&d, NULL);
    kill_child(pid);
    int status;
    waitpid(pid, &status, 0);
    size_t acked = 0;
    char buf[256];
    ssize_t got;
    while ((got = read(pfd[0], buf, sizeof buf)) > 0)
        acked += (size_t)got;
    close(pfd[0]);

    static model m, m2;
    for (uint32_t s = 0; s < NS; s++)
        for (int64_t t = 0; t < D; t++)
            m.v[s][t] = ABSENT;
    static op o;
    uint64_t r = seed;
    for (size_t i = 0; i < acked; i++) {
        gen_op(&r, &o, true);
        model_apply(&m, &o);
    }
    m2 = m;
    gen_op(&r, &o, true);
    model_apply(&m2, &o);

    if (getenv("TSDB_DELETE_VERBOSE"))
        fprintf(stderr, "crash seed %" PRIu64 ": %zu acknowledged\n", seed, acked);
    tsdb_options opt = test_opts();
    tsdb *db;
    int rc = tsdb_open(dir, &opt, &db);
    if (rc == TSDB_ERR_NOTFOUND || (rc == 0 && tsdb_series_count(db) < NS)) { /* killed before the series existed */
        if (rc == 0)
            tsdb_close(db);
        return;
    }
    OK(rc);
    for (int pass = 0; pass < 2; pass++) {
        bool a = true, b = true;
        for (uint32_t s = 0; s < NS; s++) {
            a = a && series_matches(db, &m, s, true);
            b = b && series_matches(db, &m2, s, true);
        }
        if (!a && !b) {
            fprintf(stderr, "crash seed %" PRIu64 " after %zu acknowledged ops (next op %d series %u), pass %d:\n",
                    seed, acked, o.kind, o.s, pass);
            for (uint32_t s = 0; s < NS; s++)
                series_matches(db, &m, s, false);
            failures++;
            break;
        }
        OK(tsdb_optimize(db)); /* and once more after merging the recovered state */
    }
    OK(tsdb_close(db));
}

/* ------------------------------------------------------- deterministic */

/* WAL replay around deletes: rows in the memtable before a delete, rows written
 * after it into the same WAL segment, then SIGKILL. The delete flushed the older
 * rows into parts, so replay must neither bring them back nor skip the newer
 * ones in the partitions where the delete left a tombstone part. */
static void wr_put(tsdb *db, uint32_t s, int64_t a, int64_t b, int64_t add)
{
    int64_t ts[100], v[100];
    size_t n = 0;
    for (int64_t t = a; t < b; t += 100, n++) {
        ts[n] = t;
        v[n] = add + t;
    }
    tsdb_values col = {TSDB_INT64, v, NULL};
    OK(tsdb_insert_series_values(db, SID(s), ts, &col, n));
}

static void wr_check(tsdb *db, const int64_t *exp0, const char *when)
{
    static rows r;
    for (uint32_t s = 0; s < 2; s++) {
        r.n = 0;
        OK(tsdb_query_values(db, SID(s), INT64_MIN, INT64_MAX, NULL, collect, &r));
        size_t k = 0;
        bool ok = true;
        for (int64_t t = 0; t < 10000; t += 100) {
            int64_t want = s ? t : exp0[t / 100];
            if (want == NULLV)
                continue;
            if (ok && (k >= r.n || r.ts[k] != t || r.v[k] != want)) {
                fprintf(stderr, "  ts %lld: want %lld, row %zu has ts %lld value %lld\n", (long long)t,
                        (long long)want, k, k < r.n ? (long long)r.ts[k] : -1LL, k < r.n ? (long long)r.v[k] : -1LL);
                ok = false;
            }
            k++;
        }
        if (!ok || k != r.n) {
            fprintf(stderr, "FAIL wal replay (%s): series %u has %zu rows, %zu expected\n", when, s, r.n, k);
            failures++;
        }
    }
}

static void test_wal_replay(void)
{
    char dir[1024];
    snprintf(dir, sizeof dir, "%s/tsdb_delete_wal", scratch);
    rmrf(dir);
    tsdb_options opt = test_opts();
    opt.wal_sync_ms = 0; /* every write durable on return */
    pid_t pid = fork();
    if (pid == 0) {
        tsdb *db;
        OK(tsdb_open(dir, &opt, &db));
        create_series(db);
        wr_put(db, 0, 0, 10000, 0); /* five partitions, all in the memtable */
        wr_put(db, 1, 0, 10000, 0);
        OK(tsdb_delete_range(db, SID(0), 2000, 6000));
        wr_put(db, 0, 3000, 4000, 1000000);   /* after the delete: visible */
        wr_put(db, 0, 7000, 8000, 2000000);   /* overwrites outside the range */
        int64_t ts = 8500, v = 3000000;
        tsdb_values col = {TSDB_INT64, &v, NULL};
        OK(tsdb_replace_range(db, SID(0), 8000, 9000, &ts, &col, 1));
        wr_put(db, 0, 8800, 8900, 4000000);   /* after the replace */
        raise(SIGKILL);
        _exit(1);
    }
    int st;
    waitpid(pid, &st, 0);
    CHECK(WIFSIGNALED(st) && WTERMSIG(st) == SIGKILL);
    int64_t exp0[100];
    for (int i = 0; i < 100; i++) {
        int64_t t = i * 100;
        exp0[i] = t >= 2000 && t < 6000 ? NULLV : t;
        if (t >= 3000 && t < 4000)
            exp0[i] = 1000000 + t;
        if (t >= 7000 && t < 8000)
            exp0[i] = 2000000 + t;
        if (t >= 8000 && t < 9000)
            exp0[i] = t == 8500 ? 3000000 : t == 8800 ? 4000000 + t : NULLV;
    }
    tsdb *db;
    OK(tsdb_open(dir, &opt, &db));
    wr_check(db, exp0, "replayed");
    OK(tsdb_close(db));
    OK(tsdb_open(dir, &opt, &db)); /* the replay's own parts must not replay again */
    wr_check(db, exp0, "reopened");
    OK(tsdb_optimize(db));
    wr_check(db, exp0, "merged");
    OK(tsdb_close(db));
    rmrf(dir);
}

static int64_t agg_count(tsdb *db, uint32_t s, int64_t a, int64_t b, int64_t *sum, tsdb_query_stats *st)
{
    tsdb_bucket *bk = NULL;
    size_t nb = 0;
    OK(tsdb_query_agg(db, SID(s), a, b, 0, &bk, &nb));
    tsdb_last_query_stats(st);
    int64_t c = nb ? (int64_t)bk[0].count : 0;
    *sum = nb ? bk[0].isum : 0;
    free(bk);
    return c;
}

/* Granules a tombstone overlaps are decoded; once merged, statistics answer again. */
static void test_stats(void)
{
    char dir[1024];
    snprintf(dir, sizeof dir, "%s/tsdb_delete_stats", scratch);
    rmrf(dir);
    tsdb_options opt = test_opts();
    opt.partition_ms = 86400000;
    opt.merge_threads = 0;
    tsdb *db;
    OK(tsdb_open(dir, &opt, &db));
    create_series(db);
    enum { N = 100000 };
    int64_t *ts = malloc(N * sizeof *ts), *v = malloc(N * sizeof *v);
    for (int i = 0; i < N; i++)
        ts[i] = i, v[i] = i;
    tsdb_values tv = {TSDB_INT64, v, NULL};
    OK(tsdb_insert_series_values(db, SID(1), ts, &tv, N));
    OK(tsdb_optimize(db));
    tsdb_query_stats st;
    int64_t sum;
    CHECK(agg_count(db, 1, 0, N, &sum, &st) == N);
    CHECK(st.granules > 0 && st.granules_from_stats == st.granules);

    /* deleting another series' range, or this series' range outside its rows, costs no decoding */
    OK(tsdb_delete_range(db, SID(0), 0, N));
    OK(tsdb_delete_range(db, SID(1), N + 10, N + 1000));
    CHECK(agg_count(db, 1, 0, N, &sum, &st) == N);
    CHECK(st.granules_from_stats == st.granules);

    OK(tsdb_delete_range(db, SID(1), 1000, 2000));
    int64_t want = (int64_t)N * (N - 1) / 2 - (1000 + 1999) * 1000 / 2;
    CHECK(agg_count(db, 1, 0, N, &sum, &st) == N - 1000 && sum == want);
    CHECK(st.granules - st.granules_from_stats <= 1000 / 64 + 2 && st.granules_from_stats > 0);
    /* rows of granules decoded around a tombstone count once, as rows_decoded: no memtable holds any */
    CHECK(st.memtable_rows == 0);
    {
        static rows rr;
        rr.n = 0;
        OK(tsdb_query_values(db, SID(1), 0, 3000, NULL, collect, &rr));
        tsdb_last_query_stats(&st);
        CHECK(rr.n == 2000 && st.memtable_rows == 0 && st.rows_decoded > 0);
        tsdb_twa_opts to = {.method = TSDB_TWA_STEP};
        tsdb_twa_bucket *tb = NULL;
        size_t ntb = 0;
        OK(tsdb_query_twa(db, SID(1), 0, N, 0, &to, &tb, &ntb));
        tsdb_last_query_stats(&st);
        CHECK(ntb == 1 && st.memtable_rows == 0 && st.rows_decoded > 0);
        free(tb);
    }
    /* a later insert into the deleted range is visible; the delete hides only what came before it */
    int64_t t1 = 1500, v1 = 7;
    tsdb_values one = {TSDB_INT64, &v1, NULL};
    OK(tsdb_insert_series_values(db, SID(1), &t1, &one, 1));
    CHECK(agg_count(db, 1, 0, N, &sum, &st) == N - 999 && sum == want + 7);
    OK(tsdb_flush(db));
    CHECK(agg_count(db, 1, 0, N, &sum, &st) == N - 999 && sum == want + 7);

    OK(tsdb_optimize(db));
    CHECK(agg_count(db, 1, 0, N, &sum, &st) == N - 999 && sum == want + 7);
    CHECK(st.granules_from_stats == st.granules);
    tsdb_stats ds;
    OK(tsdb_get_stats(db, &ds));
    CHECK(ds.parts == 1 && ds.rows == N - 999);

    /* replace: the old range goes, the new rows stay, outside rows are refused */
    int64_t rts[3] = {50000, 50001, 50500}, rv[3] = {1, 2, 3};
    tsdb_values rtv = {TSDB_INT64, rv, NULL};
    CHECK(tsdb_replace_range(db, SID(1), 50000, 50500, rts, &rtv, 3) == TSDB_ERR_INVALID);
    OK(tsdb_replace_range(db, SID(1), 50000, 51000, rts, &rtv, 3));
    CHECK(agg_count(db, 1, 50000, 51000, &sum, &st) == 3 && sum == 6);
    CHECK(agg_count(db, 1, 0, N, &sum, &st) == N - 999 - 997);
    OK(tsdb_close(db));
    OK(tsdb_open(dir, &opt, &db));
    CHECK(agg_count(db, 1, 50000, 51000, &sum, &st) == 3 && sum == 6);
    CHECK(agg_count(db, 1, 1000, 2000, &sum, &st) == 1 && sum == 7);
    OK(tsdb_close(db));
    free(ts);
    free(v);
}

/* A follower sees deletes as they are committed. */
static void test_follower(void)
{
    char dir[1024];
    snprintf(dir, sizeof dir, "%s/tsdb_delete_follow", scratch);
    rmrf(dir);
    tsdb_options opt = test_opts();
    tsdb *db, *fo;
    OK(tsdb_open(dir, &opt, &db));
    create_series(db);
    int64_t ts[100], v[100];
    for (int i = 0; i < 100; i++)
        ts[i] = i * 100, v[i] = i;
    tsdb_values tv = {TSDB_INT64, v, NULL};
    OK(tsdb_insert_series_values(db, SID(2), ts, &tv, 100));
    OK(tsdb_flush(db));
    tsdb_options fopt = test_opts();
    fopt.follow = 1;
    fopt.refresh_ms = 20;
    OK(tsdb_open(dir, &fopt, &fo));
    int64_t sum;
    tsdb_query_stats st;
    CHECK(agg_count(fo, 2, 0, D, &sum, &st) == 100);
    OK(tsdb_delete_range(db, SID(2), 0, 5000));
    int64_t n = -1;
    for (int i = 0; i < 300 && n != 50; i++) {
        struct timespec d = {0, 10000000};
        nanosleep(&d, NULL);
        n = agg_count(fo, 2, 0, D, &sum, &st);
    }
    CHECK(n == 50);
    OK(tsdb_optimize(db));
    for (int i = 0; i < 30; i++) {
        struct timespec d = {0, 10000000};
        nanosleep(&d, NULL);
        CHECK(agg_count(fo, 2, 0, D, &sum, &st) == 50);
    }
    OK(tsdb_close(fo));
    OK(tsdb_close(db));
}

/* ------------------------------------------------------------ retention */

static int64_t fake_now;
static int64_t fake_clock(void *arg)
{
    (void)arg;
    return __atomic_load_n(&fake_now, __ATOMIC_SEQ_CST);
}

/* rows of series s in [a, b) that a values query returns, in either order; their first and last timestamps */
static size_t values_span(tsdb *db, uint32_t s, int64_t a, int64_t b, tsdb_order order, int64_t *first, int64_t *last)
{
    static rows rr;
    rr.n = 0;
    tsdb_query_opts qo = {order, 0};
    OK(tsdb_query_values(db, SID(s), a, b, &qo, collect, &rr));
    *first = rr.n ? rr.ts[0] : -1;
    *last = rr.n ? rr.ts[rr.n - 1] : -1;
    return rr.n;
}

/* series s holds exactly the rows [from, D) to every values query */
static bool values_from(tsdb *db, uint32_t s, int64_t from)
{
    int64_t f, l;
    bool ok = values_span(db, s, 0, D, TSDB_ASC, &f, &l) == (size_t)(D - from) && f == from && l == D - 1;
    ok &= values_span(db, s, 0, D, TSDB_DESC, &f, &l) == (size_t)(D - from) && f == D - 1 && l == from;
    ok &= values_span(db, s, 0, from + 1, TSDB_DESC, &f, &l) == 1 && f == from;
    ok &= from == 0 || values_span(db, s, 0, from, TSDB_ASC, &f, &l) == 0;
    return ok;
}

static void test_retention(void)
{
    char dir[1024];
    snprintf(dir, sizeof dir, "%s/tsdb_delete_retention", scratch);
    rmrf(dir);
    tsdb_options opt = test_opts();
    opt.merge_threads = 0;
    opt.clock_ms = fake_clock;
    fake_now = 20000;
    tsdb *db;
    OK(tsdb_open(dir, &opt, &db));
    create_series(db);
    int64_t ts[D], v[D];
    for (int i = 0; i < D; i++)
        ts[i] = i, v[i] = 1;
    tsdb_values tv = {TSDB_INT64, v, NULL};
    for (tsdb_sid s = 0; s < NS; s++)
        OK(tsdb_insert_series_values(db, SID(s), ts, &tv, D));
    int64_t lim = -1, sum;
    tsdb_query_stats st;
    OK(tsdb_get_retention(db, SID(0), &lim));
    CHECK(lim == 0);
    CHECK(tsdb_set_retention(db, SID(0), -5) == TSDB_ERR_INVALID);
    CHECK(tsdb_set_retention(db, SID(99), 5) != 0);
    OK(tsdb_set_retention(db, SID(0), 10000)); /* cutoff 10000 */
    OK(tsdb_set_retention(db, SID(1), 30000)); /* cutoff -10000: keeps everything */
    CHECK(agg_count(db, 0, 0, D, &sum, &st) == D - 10000);
    CHECK(agg_count(db, 1, 0, D, &sum, &st) == D);
    CHECK(values_from(db, 0, 10000) && values_from(db, 1, 0));
    CHECK(agg_count(db, 2, 0, D, &sum, &st) == D);
    fake_now = 21000; /* forward: one more second expires */
    CHECK(agg_count(db, 0, 0, D, &sum, &st) == D - 11000);
    CHECK(values_from(db, 0, 11000));
    fake_now = 5000; /* back: nothing comes back */
    CHECK(agg_count(db, 0, 0, D, &sum, &st) == D - 11000);
    OK(tsdb_close(db));
    fake_now = 21000;
    OK(tsdb_open(dir, &opt, &db));
    OK(tsdb_get_retention(db, SID(0), &lim));
    CHECK(lim == 10000);
    CHECK(agg_count(db, 0, 0, D, &sum, &st) == D - 11000);
    CHECK(values_from(db, 0, 11000));
    OK(tsdb_optimize(db)); /* merges drop the expired rows for good */
    CHECK(values_from(db, 0, 11000));
    OK(tsdb_set_retention(db, SID(0), 0));
    CHECK(agg_count(db, 0, 0, D, &sum, &st) == D - 11000);
    CHECK(values_from(db, 0, 11000));
    CHECK(agg_count(db, 1, 0, D, &sum, &st) == D);
    OK(tsdb_close(db));

    /* in the background: a part holding expired rows is rewritten alone once old enough, and partitions
     * every series of which has expired are dropped whole */
    opt.merge_threads = 1;
    opt.retention_merge_s = 1;
    OK(tsdb_open(dir, &opt, &db));
    OK(tsdb_optimize(db));
    tsdb_stats ds;
    OK(tsdb_get_stats(db, &ds));
    CHECK(ds.partitions == 8 && ds.rows == (uint64_t)(D - 11000) + 2 * D);
    fake_now = 26000;
    /* series 0 keeps its rows from 11000 on (no limit now, the rest merged away), cutoffs: series 1 6000,
     * series 2 3000: partition 0 goes whole, the rows before the cutoffs in the others are rewritten away */
    OK(tsdb_set_retention(db, SID(1), 20000));
    OK(tsdb_set_retention(db, SID(2), 23000));
    const uint64_t want = (D - 11000) + (D - 6000) + (D - 3000);
    for (int i = 0; i < 100; i++) {
        OK(tsdb_get_stats(db, &ds));
        if (ds.partitions == 7 && ds.rows == want)
            break;
        struct timespec d = {0, 50000000};
        nanosleep(&d, NULL);
    }
    if (ds.partitions != 7 || ds.rows != want)
        fprintf(stderr, "  retention: %" PRIu64 " partitions, %" PRIu64 " rows\n", ds.partitions, ds.rows);
    CHECK(ds.partitions == 7 && ds.rows == want);
    CHECK(agg_count(db, 2, 0, D, &sum, &st) == D - 3000);
    CHECK(agg_count(db, 1, 0, D, &sum, &st) == D - 6000);
    OK(tsdb_close(db));
}

/* Parts in dir that hold both granules and tombstones: what a merge that left an older part of the partition out
 * writes (it must keep the tombstones, and they must not hide its own rows). */
static int mixed_parts(const char *dir)
{
    DIR *d = opendir(dir);
    int n = 0;
    struct dirent *e;
    while (d && (e = readdir(d)) != NULL) {
        size_t l = strlen(e->d_name);
        if (l < 6 || strcmp(e->d_name + l - 5, ".part") != 0 || strncmp(e->d_name, "retired_", 8) == 0)
            continue;
        char path[1400];
        snprintf(path, sizeof path, "%s/%s", dir, e->d_name);
        int fd = open(path, O_RDONLY | O_CLOEXEC);
        struct stat st;
        part_footer f;
        if (fd >= 0 && fstat(fd, &st) == 0 && st.st_size >= (off_t)sizeof f &&
            pread(fd, &f, sizeof f, st.st_size - (off_t)sizeof f) == (ssize_t)sizeof f && f.ngranules > 0 &&
            f.index_off + f.index_len + sizeof f < (uint64_t)st.st_size) /* a trailer: tombstones */
            n++;
        if (fd >= 0)
            close(fd);
    }
    if (d)
        closedir(d);
    return n;
}

static void wait_ms(int ms)
{
    struct timespec d = {ms / 1000, (long)(ms % 1000) * 1000000};
    nanosleep(&d, NULL);
}

/* A background merge of a replace's parts (tombstones + new rows) that leaves the big old part out
 * (max_part_bytes): the merged part keeps the tombstones, which still hide the old part's rows and not its own. */
static void test_partial_merge(void)
{
    char dir[1024];
    snprintf(dir, sizeof dir, "%s/tsdb_delete_partial", scratch);
    rmrf(dir);
    tsdb_options opt;
    tsdb_options_init(&opt);
    opt.partition_ms = 1000000000;
    opt.merge_threads = 1;
    opt.merge_idle_s = 1;
    opt.max_part_bytes = 200000;
    tsdb *db;
    OK(tsdb_open(dir, &opt, &db));
    tsdb_sid s;
    OK(tsdb_series_create(db, "a", TSDB_INT64, NULL, &s));
    enum { N = 100000 };
    static int64_t ts[N], v[N];
    uint64_t r = 88172645463325252ull;
    for (int i = 0; i < N; i++)
        ts[i] = i, v[i] = 1 | (int64_t)(rnd(&r) & 0xffffff) << 1;
    tsdb_values tv = {TSDB_INT64, v, NULL};
    OK(tsdb_insert_series_values(db, s, ts, &tv, N));
    OK(tsdb_flush(db));
    int64_t want = 0;
    for (int i = N / 2; i < N; i++)
        want += v[i];
    int64_t rts[100], rv[100];
    for (int i = 0; i < 100; i++)
        rts[i] = i, rv[i] = 7, want += 7;
    tsdb_values rtv = {TSDB_INT64, rv, NULL};
    OK(tsdb_replace_range(db, s, 0, N / 2, rts, &rtv, 100));
    tsdb_stats st;
    for (int i = 0; i < 100; i++) {
        tsdb_get_stats(db, &st);
        if (st.parts == 2 && mixed_parts(dir) == 1)
            break;
        wait_ms(100);
    }
    CHECK(mixed_parts(dir) == 1); /* the merge happened, and left the old part out */
    tsdb_query_stats qs;
    int64_t sum;
    CHECK(agg_count(db, s, 0, N, &sum, &qs) == N / 2 + 100);
    CHECK(sum == want);
    OK(tsdb_close(db));
    OK(tsdb_open(dir, &opt, &db));
    CHECK(agg_count(db, s, 0, N, &sum, &qs) == N / 2 + 100);
    OK(tsdb_optimize(db));
    CHECK(agg_count(db, s, 0, N, &sum, &qs) == N / 2 + 100);
    CHECK(sum == want);
    OK(tsdb_close(db));
}

/* Deletes whose ends fall exactly on the first or last row of a granule, a part or a partition. */
static void test_edges(void)
{
    static const struct {
        int64_t a, b, left; /* delete [a, b) of rows 0..5999 (granules of 64, partitions of 2000) */
    } c[] = {
        {5999, 7000, 5999}, /* start at the last row of the last part */
        {1999, 2500, 5499}, /* start at the last row of a partition */
        {1500, 2000, 5500}, /* end at the first row of the next partition */
        {2000, 4000, 4000}, /* exactly one partition */
        {1000, 1001, 5999}, /* one row */
        {63, 64, 5999},     /* the last row of a granule */
        {127, 300, 5827},   /* from the last row of a granule */
        {50, 64, 5986},     /* up to the first row of the next granule */
        {-5, 0, 6000},      /* up to the first row */
    };
    char dir[1024];
    snprintf(dir, sizeof dir, "%s/tsdb_delete_edges", scratch);
    for (size_t k = 0; k < sizeof c / sizeof c[0]; k++) {
        rmrf(dir);
        tsdb_options opt = test_opts();
        opt.merge_threads = 0;
        tsdb *db;
        OK(tsdb_open(dir, &opt, &db));
        tsdb_sid s;
        OK(tsdb_series_create(db, "a", TSDB_INT64, NULL, &s));
        static int64_t ts[6000], v[6000];
        for (int i = 0; i < 6000; i++)
            ts[i] = i, v[i] = 1;
        tsdb_values tv = {TSDB_INT64, v, NULL};
        OK(tsdb_insert_series_values(db, s, ts, &tv, 6000));
        OK(tsdb_flush(db));
        OK(tsdb_delete_range(db, s, c[k].a, c[k].b));
        tsdb_query_stats qs;
        int64_t sum, q = agg_count(db, s, -10, 10000, &sum, &qs);
        OK(tsdb_optimize(db));
        int64_t m = agg_count(db, s, -10, 10000, &sum, &qs);
        if (q != c[k].left || m != c[k].left || sum != c[k].left) {
            fprintf(stderr, "edges: delete [%" PRId64 ", %" PRId64 ") left %" PRId64 ", after merge %" PRId64
                    ", want %" PRId64 "\n", c[k].a, c[k].b, q, m, c[k].left);
            failures++;
        }
        OK(tsdb_close(db));
    }
}

/* An empty part left by a merge (all rows deleted or expired) anchors WAL replay: while a WAL segment from before
 * the delete may come back (an unlink that was not durable), it keeps that segment's deleted rows out. Once no
 * such segment can exist, the anchor and its partition go: nothing is left behind for good. */
static void test_anchors(void)
{
    char dir[1024], seg[1400], aside[1400];
    snprintf(dir, sizeof dir, "%s/tsdb_delete_anchor", scratch);
    snprintf(aside, sizeof aside, "%s.wal", dir);
    rmrf(dir);
    tsdb_options opt = test_opts();
    opt.merge_threads = 0; /* nothing may retire the anchor before the segment comes back */
    tsdb *db;
    OK(tsdb_open(dir, &opt, &db));
    tsdb_sid s;
    OK(tsdb_series_create(db, "a", TSDB_INT64, NULL, &s));
    int64_t ts[100], v[100];
    for (int i = 0; i < 100; i++)
        ts[i] = i, v[i] = 1;
    tsdb_values tv = {TSDB_INT64, v, NULL};
    OK(tsdb_insert_series_values(db, s, ts, &tv, 100)); /* small: in the WAL */
    DIR *d = opendir(dir);
    struct dirent *e;
    seg[0] = 0;
    while (d && (e = readdir(d)) != NULL)
        if (strncmp(e->d_name, "wal_", 4) == 0)
            snprintf(seg, sizeof seg, "%s/%s", dir, e->d_name);
    if (d)
        closedir(d);
    char cmd[3000];
    snprintf(cmd, sizeof cmd, "cp '%s' '%s'", seg, aside);
    CHECK(seg[0] && system(cmd) == 0);
    OK(tsdb_delete_range(db, s, 0, 100));
    OK(tsdb_optimize(db));
    OK(tsdb_close(db));
    CHECK(rename(aside, seg) == 0); /* the segment's unlink was lost */
    OK(tsdb_open(dir, &opt, &db));
    tsdb_query_stats qs;
    int64_t sum;
    CHECK(agg_count(db, s, 0, 100, &sum, &qs) == 0);
    OK(tsdb_close(db));

    /* with merges on, the anchor goes once its WAL segment is gone */
    opt.merge_threads = 1;
    OK(tsdb_open(dir, &opt, &db));
    tsdb_stats st;
    for (int i = 0; i < 50; i++) {
        tsdb_get_stats(db, &st);
        if (st.partitions == 0)
            break;
        wait_ms(100);
    }
    CHECK(st.partitions == 0 && st.parts == 0);
    CHECK(agg_count(db, s, 0, 100, &sum, &qs) == 0);
    OK(tsdb_close(db));

    /* retention: the rows expire before the end of their partition; the k = 1 rewrite leaves an anchor, which must
     * not keep the expired partition forever */
    rmrf(dir);
    opt = test_opts();
    opt.merge_threads = 1;
    opt.retention_merge_s = 1;
    opt.clock_ms = fake_clock;
    fake_now = 1000;
    OK(tsdb_open(dir, &opt, &db));
    OK(tsdb_series_create(db, "a", TSDB_INT64, NULL, &s));
    static int64_t ts2[1000], v2[1000];
    for (int i = 0; i < 1000; i++)
        ts2[i] = i, v2[i] = 1;
    tv.values = v2;
    OK(tsdb_insert_series_values(db, s, ts2, &tv, 1000));
    OK(tsdb_flush(db));
    OK(tsdb_set_retention(db, s, 1000));
    __atomic_store_n(&fake_now, 2500, __ATOMIC_SEQ_CST); /* rows expired, their partition [0, 2000) not yet */
    for (int i = 0; i < 60; i++) {
        tsdb_get_stats(db, &st);
        if (st.rows == 0)
            break;
        wait_ms(100);
    }
    CHECK(st.rows == 0); /* rewritten into an anchor (which may go at once) */
    __atomic_store_n(&fake_now, 100000, __ATOMIC_SEQ_CST);
    for (int i = 0; i < 60; i++) {
        tsdb_get_stats(db, &st);
        if (st.partitions == 0)
            break;
        wait_ms(100);
    }
    CHECK(st.partitions == 0 && st.parts == 0);
    OK(tsdb_close(db));
}

/* Last write wins in real time. A large insert, replace or delete flushes the memtable, writes its parts and
 * publishes them; memtable rows rank above every part. A small write (WAL + memtable) to one of its keys that was
 * acknowledged between that flush and the publish would therefore keep winning over the large write although it
 * came first: a read sees the small write and the large write not yet (at another of its keys), the large write is
 * acknowledged, and reads still show the small write, which no order of the three explains. The small write must
 * instead wait until the large write is visible and then win as the newer one. The precommit hook runs in the
 * window; the small writes run in threads of their own, since the overlapping one blocks there. A small write that
 * touches none of the large write's keys must not wait. */
enum { LWW_K = 1000, LWW_K2 = 2000, LWW_FAR = 9000, LWW_SMALL = 7, LWW_OLD = 1, LWW_NEW = 2, LWW_ABSENT = -1 };

typedef struct {
    tsdb *db;
    tsdb_sid s;
    int64_t ts, v;
    pthread_mutex_t mu;
    pthread_cond_t cv;
    int done, rc;
    pthread_t th;
} lww_write;

typedef struct {
    tsdb *db;
    tsdb_sid s, other;
    lww_write small, apart, later; /* on a key of the large write; on another series; on s past its keys */
    int in_window, apart_in_window, later_in_window;
    int64_t seen_k, seen_k2; /* read in the window once the small write was acknowledged */
} lww;

static void *lww_write_main(void *arg)
{
    lww_write *w = arg;
    tsdb_values col = {TSDB_INT64, &w->v, NULL};
    int rc = tsdb_insert_series_values(w->db, w->s, &w->ts, &col, 1);
    pthread_mutex_lock(&w->mu);
    w->rc = rc;
    w->done = 1;
    pthread_cond_broadcast(&w->cv);
    pthread_mutex_unlock(&w->mu);
    return NULL;
}

static void lww_start(lww_write *w, tsdb *db, tsdb_sid s, int64_t ts, int64_t v)
{
    w->db = db, w->s = s, w->ts = ts, w->v = v, w->done = 0, w->rc = 0;
    pthread_mutex_init(&w->mu, NULL);
    pthread_cond_init(&w->cv, NULL);
    if (pthread_create(&w->th, NULL, lww_write_main, w) != 0) {
        fprintf(stderr, "pthread_create\n");
        exit(1);
    }
}

/* whether the write was acknowledged within ms */
static int lww_wait(lww_write *w, int ms)
{
    struct timespec dl;
    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += ms / 1000;
    dl.tv_nsec += (long)(ms % 1000) * 1000000;
    if (dl.tv_nsec >= 1000000000L)
        dl.tv_sec++, dl.tv_nsec -= 1000000000L;
    pthread_mutex_lock(&w->mu);
    while (!w->done && pthread_cond_timedwait(&w->cv, &w->mu, &dl) == 0)
        ;
    int done = w->done;
    pthread_mutex_unlock(&w->mu);
    return done;
}

static int lww_join(lww_write *w)
{
    pthread_join(w->th, NULL);
    pthread_mutex_destroy(&w->mu);
    pthread_cond_destroy(&w->cv);
    return w->rc;
}

static int64_t lww_read(tsdb *db, tsdb_sid s, int64_t k)
{
    static rows r;
    r.n = 0;
    OK(tsdb_query_values(db, s, k, k + 1, NULL, collect, &r));
    return r.n ? r.v[0] : LWW_ABSENT;
}

static void lww_window(void *arg)
{
    lww *x = arg;
    lww_start(&x->apart, x->db, x->other, LWW_K, LWW_SMALL);
    lww_start(&x->later, x->db, x->s, LWW_FAR, LWW_SMALL);
    lww_start(&x->small, x->db, x->s, LWW_K, LWW_SMALL);
    x->apart_in_window = lww_wait(&x->apart, 3000);
    x->later_in_window = lww_wait(&x->later, 3000);
    x->in_window = lww_wait(&x->small, 300);
    if (x->in_window) {
        x->seen_k = lww_read(x->db, x->s, LWW_K);
        x->seen_k2 = lww_read(x->db, x->s, LWW_K2);
    }
}

static void test_lww_realtime(void)
{
    /* a large insert of one series, a replace, a delete, and large inserts of two series (the fence's buckets),
     * their rows interleaved (time-major) or one series after the other */
    static const char *const kinds[] = {"insert", "replace", "delete", "insert of two series, interleaved",
                                        "insert of two series, one after the other"};
    for (int kind = 0; kind < 5; kind++) {
        char dir[1024];
        snprintf(dir, sizeof dir, "%s/tsdb_delete_lww", scratch);
        rmrf(dir);
        tsdb_options opt = test_opts();
        tsdb *db;
        OK(tsdb_open(dir, &opt, &db));
        lww x;
        memset(&x, 0, sizeof x);
        x.db = db;
        OK(tsdb_series_create(db, "a", TSDB_INT64, NULL, &x.s));
        OK(tsdb_series_create(db, "b", TSDB_INT64, NULL, &x.other));
        tsdb_sid c, ids[300];
        OK(tsdb_series_create(db, "c", TSDB_INT64, NULL, &c));
        int64_t ts[300], v[300];
        for (int i = 0; i < 60; i++)
            ts[i] = i * 100, v[i] = LWW_OLD;
        tsdb_values col = {TSDB_INT64, v, NULL};
        OK(tsdb_insert_series_values(db, x.s, ts, &col, 60)); /* small: into the memtable, which L flushes */
        for (int i = 0; i < 300; i++)
            ts[i] = i * 20, v[i] = LWW_NEW; /* [0, 6000): LWW_K and LWW_K2 among them */
        if (kind == 3) /* the keys of a: every 40 ms, LWW_K and LWW_K2 among them */
            for (int i = 0; i < 300; i++)
                ids[i] = i % 2 ? c : x.s, ts[i] = i / 2 * 40;
        if (kind == 4)
            for (int i = 0; i < 300; i++)
                ids[i] = i < 150 ? x.s : c, ts[i] = i % 150 * 40;
        tsdb_test_precommit(db, lww_window, &x);
        if (kind == 0)
            OK(tsdb_insert_series_values(db, x.s, ts, &col, 300)); /* large: 300 >= wal_max_batch_rows */
        else if (kind == 1)
            OK(tsdb_replace_range(db, x.s, 0, 6000, ts, &col, 300));
        else if (kind == 2)
            OK(tsdb_delete_range(db, x.s, 0, 6000));
        else
            OK(tsdb_insert_values(db, ids, ts, &col, 300));
        int64_t large = kind == 2 ? LWW_ABSENT : LWW_NEW;
        tsdb_test_precommit(db, NULL, NULL);
        int rc = lww_join(&x.small), rca = lww_join(&x.apart), rcl = lww_join(&x.later);
        if (rc || rca || rcl) {
            fprintf(stderr, "lww %s: small writes failed: %s / %s / %s\n", kinds[kind], tsdb_strerror(rc),
                    tsdb_strerror(rca), tsdb_strerror(rcl));
            failures++;
        }
        int64_t fin = lww_read(db, x.s, LWW_K);
        /* acknowledged in the window, the small write is older than the large one, which must win; acknowledged
         * after the large write was published, it is newer and wins */
        int64_t want = x.in_window ? large : LWW_SMALL;
        if (fin != want) {
            fprintf(stderr,
                    "lww %s: the small write to ts %d was acknowledged %s the large write was published (reads "
                    "then: ts %d = %" PRId64 ", ts %d = %" PRId64 "); after the large write was acknowledged ts %d "
                    "reads %" PRId64 ", want %" PRId64 "\n",
                    kinds[kind], LWW_K, x.in_window ? "before" : "after", LWW_K, x.seen_k, LWW_K2, x.seen_k2, LWW_K,
                    fin, want);
            failures++;
        }
        if (x.in_window) { /* the anomaly itself: the small write won while it was older */
            fprintf(stderr, "lww %s: a small write to a key of an unpublished large write was acknowledged\n",
                    kinds[kind]);
            failures++;
        }
        if (!x.apart_in_window || !x.later_in_window) {
            fprintf(stderr, "lww %s: a small write to a key the large write does not touch waited for it (%s)\n",
                    kinds[kind], x.apart_in_window ? "same series, later time" : "another series");
            failures++;
        }
        /* the WAL replay ranks the same way */
        OK(tsdb_close(db));
        OK(tsdb_open(dir, &opt, &db));
        int64_t again = lww_read(db, x.s, LWW_K), far = lww_read(db, x.s, LWW_FAR);
        if (again != fin || far != LWW_SMALL || lww_read(db, x.other, LWW_K) != LWW_SMALL) {
            fprintf(stderr, "lww %s: after a reopen ts %d reads %" PRId64 " (before %" PRId64 "), ts %d %" PRId64 "\n",
                    kinds[kind], LWW_K, again, fin, LWW_FAR, far);
            failures++;
        }
        OK(tsdb_close(db));
    }
}

/* A large write that publishes wakes only the small writes its own fence held: a small write waiting for large
 * write A sleeps through the publish of B on another series. With one condition variable for every fence, each
 * publish woke every waiter, which scanned its rows against every fence again under buf_lock. */
typedef struct {
    tsdb *db;
    tsdb_sid a, b;
    int calls;
    lww_write small;
    int small_early, small_after_b, rc_b;
    uint64_t wakeups_after_b;
} lww_two;

static int lww_large(tsdb *db, tsdb_sid s)
{
    int64_t ts[300], v[300];
    for (int i = 0; i < 300; i++)
        ts[i] = i * 20, v[i] = LWW_NEW;
    tsdb_values col = {TSDB_INT64, v, NULL};
    return tsdb_insert_series_values(db, s, ts, &col, 300); /* large: 300 >= wal_max_batch_rows */
}

static void *lww_large_b(void *arg)
{
    lww_two *x = arg;
    x->rc_b = lww_large(x->db, x->b);
    return NULL;
}

static void lww_two_window(void *arg)
{
    lww_two *x = arg;
    if (__atomic_fetch_add(&x->calls, 1, __ATOMIC_SEQ_CST) != 0)
        return; /* B's: it publishes at once */
    lww_start(&x->small, x->db, x->a, LWW_K, LWW_SMALL);
    x->small_early = lww_wait(&x->small, 300);
    pthread_t th;
    if (pthread_create(&th, NULL, lww_large_b, x) != 0) {
        fprintf(stderr, "pthread_create\n");
        exit(1);
    }
    pthread_join(th, NULL);
    x->small_after_b = lww_wait(&x->small, 100);
    x->wakeups_after_b = tsdb_test_fence_wakeups(x->db);
}

/* One run; returns the wake-ups before A published (a spurious return from pthread_cond_wait counts too, which
 * glibc allows and almost never produces: the caller tries again before it calls it a failure) */
static uint64_t lww_wakeups_once(void)
{
    char dir[1024];
    snprintf(dir, sizeof dir, "%s/tsdb_delete_lww_two", scratch);
    rmrf(dir);
    tsdb_options opt = test_opts();
    opt.partition_classes = 0; /* B classifies nothing while A holds the class history frozen */
    tsdb *db;
    OK(tsdb_open(dir, &opt, &db));
    lww_two x;
    memset(&x, 0, sizeof x);
    x.db = db;
    OK(tsdb_series_create(db, "a", TSDB_INT64, NULL, &x.a));
    OK(tsdb_series_create(db, "b", TSDB_INT64, NULL, &x.b));
    tsdb_test_precommit(db, lww_two_window, &x);
    OK(lww_large(db, x.a));
    tsdb_test_precommit(db, NULL, NULL);
    OK(lww_join(&x.small));
    OK(x.rc_b);
    if (x.small_early || x.small_after_b) {
        fprintf(stderr, "lww wake-ups: the small write did not wait for A\n");
        failures++;
    }
    if (lww_read(db, x.a, LWW_K) != LWW_SMALL || lww_read(db, x.b, LWW_K) != LWW_NEW) {
        fprintf(stderr, "lww wake-ups: wrong values after both large writes\n");
        failures++;
    }
    OK(tsdb_close(db));
    return x.wakeups_after_b;
}

static void test_lww_wakeups(void)
{
    uint64_t woke = 0;
    for (int attempt = 0; attempt < 3 && (attempt == 0 || woke != 0); attempt++)
        woke = lww_wakeups_once();
    if (woke != 0) {
        fprintf(stderr, "lww wake-ups: the small write waiting for A woke %" PRIu64 " times before A published, in "
                        "each of 3 tries\n", woke);
        failures++;
    }
}

/* The write wait hook may be changed or cleared while a write waits: the -1s (the waiter's and the large write's)
 * go to the hook that got the +1s. Before, the large write's lw_leave and the waiter called whatever hook was set
 * by then: a NULL one crashed, another one got -1s it never had +1s for. */
static long lww_hook_count[2];

static void lww_hook(void *ctx, int delta) { __atomic_add_fetch((long *)ctx, delta, __ATOMIC_SEQ_CST); }

typedef struct {
    tsdb *db;
    tsdb_sid s;
    int round; /* 0: the hook is cleared in the window, 1: replaced */
    lww_write small;
    int small_early;
    long during;
} lww_hk;

static void lww_hook_window(void *arg)
{
    lww_hk *x = arg;
    lww_start(&x->small, x->db, x->s, LWW_K, LWW_SMALL);
    x->small_early = lww_wait(&x->small, 300);
    x->during = __atomic_load_n(&lww_hook_count[0], __ATOMIC_SEQ_CST); /* the waiter and this large write */
    tsdb_set_write_wait_hook(x->db, x->round ? lww_hook : NULL, x->round ? &lww_hook_count[1] : NULL);
}

static void test_lww_hook_change(void)
{
    for (int round = 0; round < 2; round++) {
        char dir[1024];
        snprintf(dir, sizeof dir, "%s/tsdb_delete_lww_hook", scratch);
        rmrf(dir);
        tsdb_options opt = test_opts();
        tsdb *db;
        OK(tsdb_open(dir, &opt, &db));
        lww_hook_count[0] = lww_hook_count[1] = 0;
        tsdb_set_write_wait_hook(db, lww_hook, &lww_hook_count[0]);
        lww_hk x;
        memset(&x, 0, sizeof x);
        x.db = db, x.round = round;
        OK(tsdb_series_create(db, "a", TSDB_INT64, NULL, &x.s));
        tsdb_test_precommit(db, lww_hook_window, &x);
        OK(lww_large(db, x.s));
        tsdb_test_precommit(db, NULL, NULL);
        OK(lww_join(&x.small));
        long c0 = __atomic_load_n(&lww_hook_count[0], __ATOMIC_SEQ_CST);
        long c1 = __atomic_load_n(&lww_hook_count[1], __ATOMIC_SEQ_CST);
        if (x.small_early || x.during != 2 || c0 != 0 || c1 != 0) {
            fprintf(stderr, "lww hook %s while a write waited: %s, %ld counted in the window (want 2); the counts "
                            "end at %ld and %ld (want 0 and 0)\n", round ? "replaced" : "cleared",
                    x.small_early ? "it did not wait" : "it waited", x.during, c0, c1);
            failures++;
        }
        if (lww_read(db, x.s, LWW_K) != LWW_SMALL) {
            fprintf(stderr, "lww hook: the small write did not win\n");
            failures++;
        }
        OK(tsdb_close(db));
    }
}

/* The precommit test hook reaches volumes added after it was set (tsdbd's TSDBD_TEST_HOLD_LARGE_MS sets it at start;
 * a volume added later used to run its large writes without it, so a test there did not test the window). */
static void lww_count_hook(void *arg) { ++*(int *)arg; }

static void test_lww_hook_new_volume(void)
{
    char dir[1024], vol[1100];
    snprintf(dir, sizeof dir, "%s/tsdb_delete_lww_vol", scratch);
    snprintf(vol, sizeof vol, "%s.volume", dir);
    rmrf(dir);
    rmrf(vol);
    tsdb_options opt = test_opts();
    tsdb *db;
    OK(tsdb_open(dir, &opt, &db));
    int calls = 0;
    tsdb_test_precommit(db, lww_count_hook, &calls);
    uint32_t index;
    OK(tsdb_volume_add(db, vol, &index));
    tsdb_sid s;
    OK(tsdb_series_create(db, "v", TSDB_INT64, vol, &s));
    OK(lww_large(db, s));
    tsdb_test_precommit(db, NULL, NULL);
    if (calls != 1) {
        fprintf(stderr, "lww: the precommit hook ran %d times for a large insert into a volume added later\n", calls);
        failures++;
    }
    OK(tsdb_close(db));
    rmrf(vol);
}

/* An insert nested in another large write's window (here in the precommit hook, on the same thread) must not
 * rewrite the outer fence's table. Before, both tables were the thread's one scratch buffer: the nested insert
 * reset it to its own series, and a small write into the outer write's keys was acknowledged before its publish. */
typedef struct {
    tsdb *db;
    tsdb_sid a, b, c, d;
    int depth;
    lww_write small;
    int in_window, rc_nested;
} lww_nest;

/* a large insert of two series, interleaved (a table of buckets): s1 and s2 every 40 ms from t0 */
static int lww_large2(tsdb *db, tsdb_sid s1, tsdb_sid s2, int64_t t0)
{
    tsdb_sid ids[300];
    int64_t ts[300], v[300];
    for (int i = 0; i < 300; i++)
        ids[i] = i % 2 ? s2 : s1, ts[i] = t0 + i / 2 * 40, v[i] = LWW_NEW;
    tsdb_values col = {TSDB_INT64, v, NULL};
    return tsdb_insert_values(db, ids, ts, &col, 300);
}

static void lww_nest_window(void *arg)
{
    lww_nest *x = arg;
    if (x->depth++)
        return; /* the nested insert's own window */
    x->rc_nested = lww_large2(x->db, x->c, x->d, 100000);
    lww_start(&x->small, x->db, x->a, LWW_K, LWW_SMALL);
    x->in_window = lww_wait(&x->small, 300);
}

static void test_lww_nested(void)
{
    char dir[1024];
    snprintf(dir, sizeof dir, "%s/tsdb_delete_lww_nest", scratch);
    rmrf(dir);
    tsdb_options opt = test_opts();
    opt.partition_classes = 0; /* the nested insert classifies nothing while the outer one holds the history */
    tsdb *db;
    OK(tsdb_open(dir, &opt, &db));
    lww_nest x;
    memset(&x, 0, sizeof x);
    x.db = db;
    OK(tsdb_series_create(db, "a", TSDB_INT64, NULL, &x.a));
    OK(tsdb_series_create(db, "b", TSDB_INT64, NULL, &x.b));
    OK(tsdb_series_create(db, "c", TSDB_INT64, NULL, &x.c));
    OK(tsdb_series_create(db, "d", TSDB_INT64, NULL, &x.d));
    tsdb_test_precommit(db, lww_nest_window, &x);
    OK(lww_large2(db, x.a, x.b, 0)); /* LWW_K is a key of a */
    tsdb_test_precommit(db, NULL, NULL);
    OK(lww_join(&x.small));
    OK(x.rc_nested);
    if (x.in_window || lww_read(db, x.a, LWW_K) != LWW_SMALL) {
        fprintf(stderr, "lww nested: after an insert nested in the window, a small write into the outer insert's "
                        "keys was acknowledged before its publish\n");
        failures++;
    }
    OK(tsdb_close(db));
}

int main(int argc, char **argv)
{
    /* a directory of this run's own: the fixed names below collided between worktrees testing at once */
    static char own[600];
    snprintf(own, sizeof own, "%s/tsdb-delete-%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    rmrf(own);
    if (mkdir(own, 0755) != 0) {
        perror(own);
        return 1;
    }
    scratch = own;
    int rounds = getenv("TSDB_DELETE_ROUNDS") ? atoi(getenv("TSDB_DELETE_ROUNDS")) : 6;
    test_lww_realtime();
    test_lww_wakeups();
    test_lww_hook_change();
    test_lww_nested();
    test_lww_hook_new_volume();
    test_stats();
    test_wal_replay();
    test_follower();
    test_retention();
    test_partial_merge();
    test_edges();
    test_anchors();
    for (int i = 0; i < rounds && !failures; i++)
        test_model(1000 + (uint64_t)i, 150);
    if (rounds >= 2 && !failures && model_mixed == 0) {
        fprintf(stderr, "model: no merge ever kept tombstones next to rows (partial merges untested)\n");
        failures++;
    }
    if (getenv("TSDB_DELETE_VERBOSE"))
        printf("model: %d steps saw a part with rows and tombstones\n", model_mixed);
    for (int i = 0; i < 4 * rounds && !failures; i++)
        test_crash(5000 + (uint64_t)i);
    if (failures) {
        fprintf(stderr, "test_delete: %d failures\n", failures);
        return 1;
    }
    rmrf(own);
    printf("test_delete: ok\n");
    return 0;
}
