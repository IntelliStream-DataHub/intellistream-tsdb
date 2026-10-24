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

#include <fcntl.h>
#include <inttypes.h>
#include <signal.h>
#include <stdlib.h>
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

static void db_apply(tsdb **db, const char *dir, const op *o)
{
    tsdb_values v = {TSDB_INT64, o->v, o->valid};
    switch (o->kind) {
    case OP_SMALL:
    case OP_LARGE:
        OK(tsdb_insert_series_values(*db, o->s, o->ts, &v, o->n));
        break;
    case OP_DELETE:
        OK(tsdb_delete_range(*db, o->s, o->a, o->b));
        break;
    case OP_REPLACE:
        OK(tsdb_replace_range(*db, o->s, o->a, o->b, o->ts, &v, o->n));
        break;
    case OP_FLUSH:
        OK(tsdb_flush(*db));
        break;
    case OP_OPTIMIZE:
        OK(tsdb_optimize(*db));
        break;
    case OP_REOPEN: {
        OK(tsdb_close(*db));
        tsdb_options opt = test_opts();
        OK(tsdb_open(dir, &opt, db));
        break;
    }
    }
}

static void create_series(tsdb *db)
{
    for (uint32_t s = 0; s < NS; s++) {
        char name[16];
        uint32_t id;
        snprintf(name, sizeof name, "s%u", s);
        OK(tsdb_series_create(db, name, TSDB_INT64, NULL, &id));
        CHECK(id == s);
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
    if (tsdb_query_values(db, s, INT64_MIN, INT64_MAX, NULL, collect, &r) != 0)
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
        OK(tsdb_query_agg(db, s, a, b, interval, &bk, &nb));
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
        OK(tsdb_query_values(db, s, a, b, &qo, collect, &rr));
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

static void test_model(uint64_t seed, int steps)
{
    char dir[1024];
    snprintf(dir, sizeof dir, "%s/tsdb_delete_model", scratch);
    rmrf(dir);
    tsdb_options opt = test_opts();
    opt.merge_threads = seed % 2 ? 2 : 0;
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
    }
    OK(tsdb_optimize(db));
    for (uint32_t s = 0; s < NS; s++)
        CHECK(series_matches(db, &m, s, false));
    OK(tsdb_close(db));
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
    OK(tsdb_insert_series_values(db, s, ts, &col, n));
}

static void wr_check(tsdb *db, const int64_t *exp0, const char *when)
{
    static rows r;
    for (uint32_t s = 0; s < 2; s++) {
        r.n = 0;
        OK(tsdb_query_values(db, s, INT64_MIN, INT64_MAX, NULL, collect, &r));
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
        OK(tsdb_delete_range(db, 0, 2000, 6000));
        wr_put(db, 0, 3000, 4000, 1000000);   /* after the delete: visible */
        wr_put(db, 0, 7000, 8000, 2000000);   /* overwrites outside the range */
        int64_t ts = 8500, v = 3000000;
        tsdb_values col = {TSDB_INT64, &v, NULL};
        OK(tsdb_replace_range(db, 0, 8000, 9000, &ts, &col, 1));
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
    OK(tsdb_query_agg(db, s, a, b, 0, &bk, &nb));
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
    OK(tsdb_insert_series_values(db, 1, ts, &tv, N));
    OK(tsdb_optimize(db));
    tsdb_query_stats st;
    int64_t sum;
    CHECK(agg_count(db, 1, 0, N, &sum, &st) == N);
    CHECK(st.granules > 0 && st.granules_from_stats == st.granules);

    /* deleting another series' range, or this series' range outside its rows, costs no decoding */
    OK(tsdb_delete_range(db, 0, 0, N));
    OK(tsdb_delete_range(db, 1, N + 10, N + 1000));
    CHECK(agg_count(db, 1, 0, N, &sum, &st) == N);
    CHECK(st.granules_from_stats == st.granules);

    OK(tsdb_delete_range(db, 1, 1000, 2000));
    int64_t want = (int64_t)N * (N - 1) / 2 - (1000 + 1999) * 1000 / 2;
    CHECK(agg_count(db, 1, 0, N, &sum, &st) == N - 1000 && sum == want);
    CHECK(st.granules - st.granules_from_stats <= 1000 / 64 + 2 && st.granules_from_stats > 0);
    /* a later insert into the deleted range is visible; the delete hides only what came before it */
    int64_t t1 = 1500, v1 = 7;
    tsdb_values one = {TSDB_INT64, &v1, NULL};
    OK(tsdb_insert_series_values(db, 1, &t1, &one, 1));
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
    CHECK(tsdb_replace_range(db, 1, 50000, 50500, rts, &rtv, 3) == TSDB_ERR_INVALID);
    OK(tsdb_replace_range(db, 1, 50000, 51000, rts, &rtv, 3));
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
    OK(tsdb_insert_series_values(db, 2, ts, &tv, 100));
    OK(tsdb_flush(db));
    tsdb_options fopt = test_opts();
    fopt.follow = 1;
    fopt.refresh_ms = 20;
    OK(tsdb_open(dir, &fopt, &fo));
    int64_t sum;
    tsdb_query_stats st;
    CHECK(agg_count(fo, 2, 0, D, &sum, &st) == 100);
    OK(tsdb_delete_range(db, 2, 0, 5000));
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
    for (uint32_t s = 0; s < NS; s++)
        OK(tsdb_insert_series_values(db, s, ts, &tv, D));
    int64_t lim = -1, sum;
    tsdb_query_stats st;
    OK(tsdb_get_retention(db, 0, &lim));
    CHECK(lim == 0);
    CHECK(tsdb_set_retention(db, 0, -5) == TSDB_ERR_INVALID);
    CHECK(tsdb_set_retention(db, 99, 5) != 0);
    OK(tsdb_set_retention(db, 0, 10000)); /* cutoff 10000 */
    OK(tsdb_set_retention(db, 1, 30000)); /* cutoff -10000: keeps everything */
    CHECK(agg_count(db, 0, 0, D, &sum, &st) == D - 10000);
    CHECK(agg_count(db, 1, 0, D, &sum, &st) == D);
    CHECK(agg_count(db, 2, 0, D, &sum, &st) == D);
    fake_now = 21000; /* forward: one more second expires */
    CHECK(agg_count(db, 0, 0, D, &sum, &st) == D - 11000);
    fake_now = 5000; /* back: nothing comes back */
    CHECK(agg_count(db, 0, 0, D, &sum, &st) == D - 11000);
    OK(tsdb_close(db));
    fake_now = 21000;
    OK(tsdb_open(dir, &opt, &db));
    OK(tsdb_get_retention(db, 0, &lim));
    CHECK(lim == 10000);
    CHECK(agg_count(db, 0, 0, D, &sum, &st) == D - 11000);
    OK(tsdb_optimize(db)); /* merges drop the expired rows for good */
    OK(tsdb_set_retention(db, 0, 0));
    CHECK(agg_count(db, 0, 0, D, &sum, &st) == D - 11000);
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
    OK(tsdb_set_retention(db, 1, 20000));
    OK(tsdb_set_retention(db, 2, 23000));
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

int main(int argc, char **argv)
{
    if (argc > 1)
        scratch = argv[1];
    int rounds = getenv("TSDB_DELETE_ROUNDS") ? atoi(getenv("TSDB_DELETE_ROUNDS")) : 6;
    test_stats();
    test_wal_replay();
    test_follower();
    test_retention();
    for (int i = 0; i < rounds && !failures; i++)
        test_model(1000 + (uint64_t)i, 150);
    for (int i = 0; i < 4 * rounds && !failures; i++)
        test_crash(5000 + (uint64_t)i);
    if (failures) {
        fprintf(stderr, "test_delete: %d failures\n", failures);
        return 1;
    }
    printf("test_delete: ok\n");
    return 0;
}
