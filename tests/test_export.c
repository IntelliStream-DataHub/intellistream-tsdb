/* tsdb export / import (tools/dump.c, docs/export-format.md), through the tsdb binary as a user runs it:
 *
 * - a round trip keeps every type bit for bit: nulls, NaN payloads, -0.0, the int64 extremes, the ends of the time
 *   axis, bools, state codes (mapped through their names into another dictionary), retention and volumes; deletes
 *   are applied; every query class (raw, agg, group, spread, TWA, LTTB) answers the same on the copy;
 * - filters (--series, --from, --to), existing series (refused, --skip-existing, --merge, --replace);
 * - a damaged stream (truncated anywhere, any flipped byte) fails the import loudly: from a file nothing is
 *   written, from a pipe the series being read is removed again and the rest are whole; a newer stream version
 *   and an unknown record kind are refused with a clear message;
 * - a stream written by the first release (tests/data/export-v1.tsdbdump) still imports: the format is stable;
 * - an export taken while a writer keeps rewriting every series sees each series at one moment;
 * - export and import run in constant memory (anonymous RSS sampled from /proc; parts are mmap'd).
 *
 *   ./build/test_export [scratch dir]       (the tsdb binary beside it, or $TSDB_CLI)
 *   ./build/test_export --write-golden FILE  writes the golden stream again (only for a new stream version) */
#define _GNU_SOURCE
#include "tsdb.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <math.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
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

#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#define SANITIZED 1 /* shadow memory is anonymous: the memory bounds mean nothing, and big runs are slow */
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
#define SANITIZED 1
#endif
#endif
#ifndef SANITIZED
#define SANITIZED 0
#endif

#define T0 1700000000000LL
static char cli[PATH_MAX], base[PATH_MAX], errfile[PATH_MAX + 16];

/* Every path lives until exit (a ring of buffers handed `src` to a later path: export read the wrong directory).
 * The list keeps them reachable, so LeakSanitizer does not count them. */
static char **paths;
static size_t npaths;

static char *path(const char *name)
{
    char **more = realloc(paths, (npaths + 1) * sizeof *paths);
    char *p = malloc(strlen(base) + strlen(name) + 2);
    if (!more || !p) {
        perror("path");
        exit(1);
    }
    paths = more;
    sprintf(p, "%s/%s", base, name);
    return paths[npaths++] = p;
}

/* ------------------------------------------------------------------ running the tsdb binary */

static long rss_anon_kb(pid_t pid)
{
    char f[64], line[256];
    snprintf(f, sizeof f, "/proc/%d/status", (int)pid);
    FILE *s = fopen(f, "r");
    long kb = -1;
    while (s && fgets(line, sizeof line, s))
        if (sscanf(line, "RssAnon: %ld", &kb) == 1)
            break;
    if (s)
        fclose(s);
    return kb;
}

/* Runs the CLI with stdin from `in` and stdout to `out` (NULL: /dev/null), stderr to errfile; returns its exit
 * status and, with peak, the most anonymous memory it held. */
static int run(const char *in, const char *out, long *peak, const char *const *args)
{
    fflush(NULL);
    /* Until execv the child is a copy of this process, whose anonymous memory (the rows it built) counts as the
     * child's: sample only once the close-on-exec pipe reports the exec (EOF). */
    int gate[2];
    if (pipe2(gate, O_CLOEXEC) != 0) {
        perror("pipe2");
        exit(1);
    }
    pid_t pid = fork();
    if (pid == 0) {
        close(gate[0]);
        int i = open(in ? in : "/dev/null", O_RDONLY);
        int o = open(out ? out : "/dev/null", O_WRONLY | O_CREAT | O_TRUNC, 0644);
        int e = open(errfile, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (i < 0 || o < 0 || e < 0)
            _exit(126);
        dup2(i, 0);
        dup2(o, 1);
        dup2(e, 2);
        execv(cli, (char *const *)args);
        _exit(127);
    }
    if (pid <= 0) {
        perror("fork");
        exit(1);
    }
    close(gate[1]);
    char c;
    while (read(gate[0], &c, 1) < 0 && errno == EINTR)
        ;
    close(gate[0]);
    int st;
    long max = 0;
    for (;;) {
        pid_t w = waitpid(pid, &st, peak ? WNOHANG : 0);
        if (w == pid)
            break;
        if (w < 0 && errno != EINTR) {
            perror("waitpid");
            exit(1);
        }
        long kb = rss_anon_kb(pid);
        if (kb > max)
            max = kb;
        usleep(2000);
    }
    if (peak)
        *peak = max;
    return WIFEXITED(st) ? WEXITSTATUS(st) : 128 + WTERMSIG(st);
}
#define RUN(in, out, ...) run(in, out, NULL, (const char *const[]){cli, __VA_ARGS__, NULL})
#define RUN_PEAK(peak, in, out, ...) run(in, out, peak, (const char *const[]){cli, __VA_ARGS__, NULL})

static int sh(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static int sh(const char *fmt, ...)
{
    char cmd[4 * PATH_MAX];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(cmd, sizeof cmd, fmt, ap);
    va_end(ap);
    fflush(NULL);
    int st = system(cmd);
    return WIFEXITED(st) ? WEXITSTATUS(st) : 128;
}

static char *slurp(const char *f, size_t *len)
{
    FILE *s = fopen(f, "rb");
    if (!s)
        return calloc(1, 1);
    fseek(s, 0, SEEK_END);
    long n = ftell(s);
    fseek(s, 0, SEEK_SET);
    char *p = malloc((size_t)n + 1);
    size_t got = fread(p, 1, (size_t)n, s);
    p[got] = 0;
    fclose(s);
    if (len)
        *len = got;
    return p;
}

static void spit(const char *f, const void *p, size_t n)
{
    FILE *s = fopen(f, "wb");
    if (!s || fwrite(p, 1, n, s) != n || fclose(s) != 0) {
        perror(f);
        exit(1);
    }
}

static bool err_has(const char *what)
{
    char *e = slurp(errfile, NULL);
    bool ok = strstr(e, what) != NULL;
    if (!ok)
        fprintf(stderr, "  (stderr does not mention \"%s\": %s)\n", what, e);
    free(e);
    return ok;
}

static tsdb *open_db(const char *dir, bool ro)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.read_only = ro;
    o.merge_threads = 0;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    return db;
}

/* ------------------------------------------------------------------ reading a series whole */

typedef struct col {
    size_t n, cap;
    int64_t *ts;
    uint64_t *v; /* the value's bits, zero-extended */
    uint8_t *ok;
    tsdb_type type;
} col;

static int col_add(void *arg, const int64_t *ts, const tsdb_values *v, size_t n)
{
    col *c = arg;
    if (c->n + n > c->cap) {
        c->cap = (c->n + n) * 2;
        c->ts = realloc(c->ts, c->cap * 8);
        c->v = realloc(c->v, c->cap * 8);
        c->ok = realloc(c->ok, c->cap);
    }
    unsigned w = v->type == TSDB_BOOL ? 1 : v->type == TSDB_FLOAT32 || v->type == TSDB_STATE ? 4 : 8;
    for (size_t i = 0; i < n; i++) {
        c->ts[c->n] = ts[i];
        c->v[c->n] = 0;
        memcpy(&c->v[c->n], (const uint8_t *)v->values + i * w, w);
        c->ok[c->n] = !v->valid || (v->valid[i >> 3] >> (i & 7) & 1);
        if (!c->ok[c->n])
            c->v[c->n] = 0;
        c->n++;
    }
    c->type = v->type;
    return 0;
}

static col read_series(tsdb *db, uint32_t id, int64_t a, int64_t b)
{
    col c = {0};
    OK(tsdb_series_type(db, id, &c.type));
    OK(tsdb_query_values(db, id, a, b, NULL, col_add, &c));
    return c;
}

static void col_free(col *c)
{
    free(c->ts);
    free(c->v);
    free(c->ok);
}

static uint32_t lookup(tsdb *db, const char *name)
{
    uint32_t id = UINT32_MAX;
    if (tsdb_series_lookup(db, name, &id) != 0)
        return UINT32_MAX;
    return id;
}

/* The series `name` reads the same in both databases over [a, b): type, retention, every row bit for bit, state
 * codes by name. */
static bool same_series(tsdb *s, tsdb *d, const char *name, int64_t a, int64_t b)
{
    uint32_t is = lookup(s, name), id = lookup(d, name);
    if (is == UINT32_MAX || id == UINT32_MAX) {
        fprintf(stderr, "  series %s missing (%u %u)\n", name, is, id);
        return false;
    }
    int64_t rs = -1, rd = -2;
    OK(tsdb_get_retention(s, is, &rs));
    OK(tsdb_get_retention(d, id, &rd));
    col x = read_series(s, is, a, b), y = read_series(d, id, a, b);
    bool same = x.type == y.type && rs == rd && x.n == y.n;
    for (size_t i = 0; same && i < x.n; i++) {
        same = x.ts[i] == y.ts[i] && x.ok[i] == y.ok[i];
        if (same && x.ok[i] && x.type == TSDB_STATE) {
            const char *nx = tsdb_state_name(s, is, (uint32_t)x.v[i]), *ny = tsdb_state_name(d, id, (uint32_t)y.v[i]);
            same = nx && ny && strcmp(nx, ny) == 0;
        } else if (same && x.ok[i]) {
            same = x.v[i] == y.v[i];
        }
        if (!same)
            fprintf(stderr, "  series %s row %zu: ts %" PRId64 "/%" PRId64 " ok %d/%d value %#" PRIx64 "/%#" PRIx64 "\n",
                    name, i, x.ts[i], y.ts[i], x.ok[i], y.ok[i], x.v[i], y.v[i]);
    }
    if (x.type != y.type || rs != rd || x.n != y.n)
        fprintf(stderr, "  series %s: type %d/%d retention %" PRId64 "/%" PRId64 " rows %zu/%zu\n", name, x.type,
                y.type, rs, rd, x.n, y.n);
    col_free(&x);
    col_free(&y);
    return same;
}

static size_t rows_of(tsdb *db, const char *name)
{
    uint32_t id = lookup(db, name);
    if (id == UINT32_MAX)
        return 0;
    col c = read_series(db, id, TSDB_TS_MIN, TSDB_TS_MAX);
    size_t n = c.n;
    col_free(&c);
    return n;
}

/* ------------------------------------------------------------------ the source database */

static uint64_t rng = 88172645463325252ull;
static uint64_t next(void)
{
    rng ^= rng << 13;
    rng ^= rng >> 7;
    rng ^= rng << 17;
    return rng;
}

static void insert(tsdb *db, uint32_t id, tsdb_type type, const int64_t *ts, const void *v, const uint8_t *valid,
                   size_t n)
{
    tsdb_values col = {type, v, valid};
    OK(tsdb_insert_series_values(db, id, ts, &col, n));
}

static const char *const state_names[] = {"idle", "run", "stop", "tab\there", "\xc3\xbcnicode", "a,b"};
#define BIG 200000 /* rows of the large float32 series: several chunks and batches */

/* Every type with its edge values, in the database directory and in volume `vol` (NULL: no volumes); `golden`
 * keeps it small and fully deterministic for the stream checked into tests/data. */
static void build_source(const char *dir, const char *vol, bool golden)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    uint32_t id;
    size_t n = golden ? 300 : BIG;
    int64_t *ts = malloc(n * 8);
    float *f = malloc(n * 4);
    double *d = malloc(n * 8);
    int64_t *iv = malloc(n * 8);
    uint8_t *b = malloc(n);
    uint32_t *st = malloc(n * 4);
    uint8_t *valid = malloc(n / 8 + 1);

    /* float32: NaN payloads, -0.0, infinities, the smallest denormal, nulls every 97th row */
    OK(tsdb_series_create(db, "temp/a", TSDB_FLOAT32, NULL, &id));
    memset(valid, 0, n / 8 + 1);
    for (size_t i = 0; i < n; i++) {
        ts[i] = T0 + (int64_t)i * 1000;
        f[i] = (float)(20.0 + (double)(next() % 10000) / 100.0);
        uint32_t bits = 0;
        switch (i % 11) {
        case 1: bits = 0x7fc00001u; break; /* a quiet NaN with a payload */
        case 2: bits = 0xffc12345u; break; /* a negative one */
        case 3: bits = 0x7f800001u; break; /* a signalling NaN */
        case 4: bits = 0x80000000u; break; /* -0.0 */
        case 5: bits = 0x7f800000u; break; /* inf */
        case 6: bits = 0x00000001u; break; /* the smallest denormal */
        }
        if (bits)
            memcpy(&f[i], &bits, 4);
        if (i % 97 != 5)
            valid[i >> 3] |= (uint8_t)(1u << (i & 7));
    }
    insert(db, id, TSDB_FLOAT32, ts, f, valid, n);
    if (!golden)
        OK(tsdb_delete_range(db, id, T0 + 1000 * 1000, T0 + 2000 * 1000)); /* the export has these rows deleted */

    /* float64 in the volume, with a retention limit of 100 years */
    OK(tsdb_series_create(db, "f64", TSDB_FLOAT64, vol, &id));
    OK(tsdb_set_retention(db, id, 100LL * 365 * 86400000));
    memset(valid, 0, n / 8 + 1);
    for (size_t i = 0; i < n; i++) {
        uint64_t bits;
        d[i] = (double)(int64_t)(next() % 2000001 - 1000000) / 1000.0;
        switch (i % 9) {
        case 1: bits = 0x7ff0000000000001ull; memcpy(&d[i], &bits, 8); break;
        case 2: bits = 0xfff8deadbeef0001ull; memcpy(&d[i], &bits, 8); break;
        case 3: bits = 0x8000000000000000ull; memcpy(&d[i], &bits, 8); break;
        case 4: d[i] = 4.9e-324; break;
        case 5: d[i] = -INFINITY; break;
        case 6: d[i] = 1.7976931348623157e308; break;
        }
        if (i % 13 != 0)
            valid[i >> 3] |= (uint8_t)(1u << (i & 7));
    }
    insert(db, id, TSDB_FLOAT64, ts, d, valid, n);

    /* int64: the extremes (INT64_MIN is null), at the ends of the time axis too */
    OK(tsdb_series_create(db, "i64", TSDB_INT64, NULL, &id));
    int64_t ets[] = {TSDB_TS_MIN, -86400000, -1, 0, 1, T0, TSDB_TS_MAX - 1};
    int64_t evs[] = {INT64_MIN + 1, INT64_MAX, -1, 0, 0, 42, 7};
    uint8_t ev = 0x77; /* row 3 (0) is null */
    insert(db, id, TSDB_INT64, ets, evs, &ev, 7);
    memset(valid, 0, n / 8 + 1);
    for (size_t i = 0; i < n; i++) {
        iv[i] = (int64_t)next();
        if (iv[i] == INT64_MIN)
            iv[i] = 0;
        if (i % 7)
            valid[i >> 3] |= (uint8_t)(1u << (i & 7));
    }
    for (size_t i = 0; i < n; i++)
        ts[i] = T0 + 5 + (int64_t)i * 1000;
    insert(db, id, TSDB_INT64, ts, iv, valid, n);

    /* bool */
    OK(tsdb_series_create(db, "door", TSDB_BOOL, vol, &id));
    for (size_t i = 0; i < n; i++)
        b[i] = (next() >> 7) & 1;
    insert(db, id, TSDB_BOOL, ts, b, valid, n);

    /* states: names created in another order than they first appear, so the target's codes differ */
    uint32_t code[6], other;
    OK(tsdb_series_create(db, "mode", TSDB_STATE, NULL, &id));
    OK(tsdb_state(db, id, "never used", &other));
    for (int k = 5; k >= 0; k--)
        OK(tsdb_state(db, id, state_names[k], &code[k]));
    for (size_t i = 0; i < n; i++)
        st[i] = code[(i / 3) % 6];
    insert(db, id, TSDB_STATE, ts, st, valid, n);
    if (vol) {
        uint32_t id2, c2;
        OK(tsdb_series_create(db, "mode2", TSDB_STATE, vol, &id2));
        OK(tsdb_state(db, id2, "far", &c2));
        for (size_t i = 0; i < n; i++)
            st[i] = c2;
        insert(db, id2, TSDB_STATE, ts, st, NULL, n);
    }

    /* an empty series keeps its type and retention */
    OK(tsdb_series_create(db, "empty", TSDB_FLOAT64, NULL, &id));
    OK(tsdb_set_retention(db, id, 3600000));

    /* a few rows through the WAL into the memtable, overwriting part rows (the last write wins) */
    uint32_t a;
    OK(tsdb_series_lookup(db, "temp/a", &a));
    float last[3] = {-1.0f, -2.0f, -3.0f};
    int64_t lts[3] = {T0, T0 + 1000, T0 + (int64_t)n * 1000};
    insert(db, a, TSDB_FLOAT32, lts, last, NULL, 3);
    OK(tsdb_close(db));
    free(ts), free(f), free(d), free(iv), free(b), free(st), free(valid);
}

static const char *const all_series[] = {"temp/a", "f64", "i64", "door", "mode", "mode2", "empty"};
#define NALL 7

/* ------------------------------------------------------------------ query classes */

static bool near(double a, double b)
{
    if (isnan(a) || isnan(b))
        return isnan(a) && isnan(b);
    if (a == b)
        return true;
    return fabs(a - b) <= 1e-9 * fmax(fabs(a), fabs(b));
}

static bool same_bucket(const tsdb_bucket *x, const tsdb_bucket *y)
{
    return x->start == y->start && x->count == y->count && near(x->sum, y->sum) && near(x->min, y->min) &&
           near(x->max, y->max) && x->isum == y->isum && x->imin == y->imin && x->imax == y->imax;
}

static int lttb_add(void *arg, const int64_t *ts, const tsdb_values *v, size_t n) { return col_add(arg, ts, v, n); }

static void same_queries(tsdb *s, tsdb *d, const char *name)
{
    uint32_t is = lookup(s, name), id = lookup(d, name);
    CHECK(is != UINT32_MAX && id != UINT32_MAX);
    if (is == UINT32_MAX || id == UINT32_MAX)
        return;
    int64_t a = TSDB_TS_MIN, b = TSDB_TS_MAX, a2 = T0 - 3600000, b2 = T0 + (int64_t)BIG * 1000 + 3600000;
    /* agg: one bucket over everything, and by minute */
    for (int k = 0; k < 2; k++) {
        tsdb_bucket *x = NULL, *y = NULL;
        size_t nx = 0, ny = 0;
        OK(tsdb_query_agg(s, is, k ? a2 : a, k ? b2 : b, k ? 60000 : 0, &x, &nx));
        OK(tsdb_query_agg(d, id, k ? a2 : a, k ? b2 : b, k ? 60000 : 0, &y, &ny));
        CHECK(nx == ny && nx > 0);
        for (size_t i = 0; i < nx && i < ny; i++)
            if (!same_bucket(&x[i], &y[i])) {
                fprintf(stderr, "  %s agg bucket %zu differs\n", name, i);
                failures++;
                break;
            }
        free(x), free(y);
    }
    /* group by hour */
    tsdb_group g = {TSDB_HOUR, 1, NULL};
    tsdb_bucket *x = NULL, *y = NULL;
    size_t nx = 0, ny = 0;
    OK(tsdb_query_group(s, is, a2, b2, &g, &x, &nx));
    OK(tsdb_query_group(d, id, a2, b2, &g, &y, &ny));
    CHECK(nx == ny && nx > 0);
    for (size_t i = 0; i < nx && i < ny; i++)
        if (!same_bucket(&x[i], &y[i])) {
            fprintf(stderr, "  %s group bucket %zu differs\n", name, i);
            failures++;
            break;
        }
    free(x), free(y);
    /* spread by 10 minutes */
    tsdb_spread_bucket *sx = NULL, *sy = NULL;
    OK(tsdb_query_spread(s, is, a2, b2, 600000, NULL, &sx, &nx));
    OK(tsdb_query_spread(d, id, a2, b2, 600000, NULL, &sy, &ny));
    CHECK(nx == ny && nx > 0);
    for (size_t i = 0; i < nx && i < ny; i++)
        if (!same_bucket(&sx[i].b, &sy[i].b) || !near(sx[i].range, sy[i].range) ||
            !near(sx[i].var_pop, sy[i].var_pop) || !near(sx[i].stddev_samp, sy[i].stddev_samp)) {
            fprintf(stderr, "  %s spread bucket %zu differs\n", name, i);
            failures++;
            break;
        }
    free(sx), free(sy);
    /* TWA by hour, step and linear */
    for (int m = 0; m < 2; m++) {
        tsdb_twa_opts to = {.method = m ? TSDB_TWA_LINEAR : TSDB_TWA_STEP, .max_gap_ms = 5000};
        tsdb_twa_bucket *tx = NULL, *ty = NULL;
        OK(tsdb_query_twa(s, is, a2, b2, 3600000, &to, &tx, &nx));
        OK(tsdb_query_twa(d, id, a2, b2, 3600000, &to, &ty, &ny));
        CHECK(nx == ny && nx > 0);
        for (size_t i = 0; i < nx && i < ny; i++)
            if (tx[i].start != ty[i].start || !near(tx[i].twa, ty[i].twa) || !near(tx[i].area, ty[i].area) ||
                tx[i].covered_ms != ty[i].covered_ms || tx[i].count != ty[i].count) {
                fprintf(stderr, "  %s twa bucket %zu differs\n", name, i);
                failures++;
                break;
            }
        free(tx), free(ty);
    }
    /* LTTB: the same points */
    col lx = {0}, ly = {0};
    OK(tsdb_query_lttb(s, is, a2, b2, 500, lttb_add, &lx));
    OK(tsdb_query_lttb(d, id, a2, b2, 500, lttb_add, &ly));
    CHECK(lx.n == ly.n && lx.n > 0);
    for (size_t i = 0; i < lx.n && i < ly.n; i++)
        if (lx.ts[i] != ly.ts[i] || lx.ok[i] != ly.ok[i] || lx.v[i] != ly.v[i]) {
            fprintf(stderr, "  %s lttb point %zu differs\n", name, i);
            failures++;
            break;
        }
    col_free(&lx), col_free(&ly);
}

/* ------------------------------------------------------------------ the tests */

static void test_round_trip(void)
{
    const char *src = path("src"), *vol = path("vol1");
    build_source(src, vol, false);
    char *file = path("all.dump");
    CHECK(RUN(NULL, NULL, src, "export", "-o", file) == 0);
    struct stat st;
    CHECK(stat(file, &st) == 0 && st.st_size > 0);
    CHECK(access(path("all.dump.partial"), F_OK) != 0);

    /* into a fresh database, the volume moved to another directory (added on the way) */
    char map[2 * PATH_MAX + 16];
    snprintf(map, sizeof map, "%s=%s", vol, path("vol2"));
    const char *dst = path("dst");
    CHECK(RUN(NULL, NULL, dst, "import", file, "--volume", map) != 0); /* vol2 is not a volume yet */
    CHECK(err_has("not a volume"));
    CHECK(RUN(NULL, NULL, dst, "import", file, "--volume", map, "--create-volumes") == 0);
    tsdb *s = open_db(src, true), *d = open_db(dst, true);
    CHECK(tsdb_series_count(d) == NALL);
    for (int i = 0; i < NALL; i++)
        CHECK(same_series(s, d, all_series[i], TSDB_TS_MIN, TSDB_TS_MAX));
    CHECK(strcmp(tsdb_series_path(d, lookup(d, "f64")), path("vol2")) == 0);
    CHECK(strcmp(tsdb_series_path(d, lookup(d, "temp/a")), dst) == 0);
    /* deletes were applied and the memtable's rows won */
    CHECK(rows_of(d, "temp/a") == BIG - 1000 + 1);
    col c = read_series(d, lookup(d, "temp/a"), T0, T0 + 1);
    CHECK(c.n == 1 && c.ok[0] && c.v[0] == 0xbf800000u);
    col_free(&c);
    const char *qs[] = {"temp/a", "f64", "i64", "door"};
    for (int i = 0; i < 4; i++)
        same_queries(s, d, qs[i]);
    tsdb_close(d);

    /* a pipe both ways (no look-ahead: verified as it goes) */
    /* each destination gets its own volume: vol2 belongs to dst now, and another database may not take it */
    const char *dst2 = path("dst2");
    snprintf(map, sizeof map, "%s=%s", vol, path("vol3"));
    CHECK(sh("'%s' '%s' export | '%s' '%s' import --volume '%s' --create-volumes 2>%s", cli, src, cli, dst2, map,
             errfile) == 0);
    d = open_db(dst2, true);
    for (int i = 0; i < NALL; i++)
        CHECK(same_series(s, d, all_series[i], TSDB_TS_MIN, TSDB_TS_MAX));
    tsdb_close(d);

    /* uncompressed streams too; --check reads a stream without a database */
    char *raw = path("raw.dump");
    CHECK(RUN(NULL, NULL, src, "export", "-o", raw, "--zstd-level", "0") == 0);
    CHECK(RUN(NULL, NULL, path("nodb"), "import", "--check", raw) == 0);
    CHECK(err_has("7 series"));
    CHECK(access(path("nodb"), F_OK) != 0);
    const char *dst3 = path("dst3");
    snprintf(map, sizeof map, "%s=%s", vol, path("vol4"));
    CHECK(RUN(raw, NULL, dst3, "import", "--volume", map, "--create-volumes") == 0); /* from stdin */
    d = open_db(dst3, true);
    for (int i = 0; i < NALL; i++)
        CHECK(same_series(s, d, all_series[i], TSDB_TS_MIN, TSDB_TS_MAX));
    tsdb_close(d);
    tsdb_close(s);

    /* the CSV import is still there, from stdin and from a file */
    spit(path("in.csv"), "x,1000,1.5\nx,2000,null\n", 23);
    CHECK(RUN(path("in.csv"), NULL, path("csv"), "import") == 0);
    CHECK(RUN(NULL, NULL, path("csv"), "import", path("in.csv")) == 0);
    d = open_db(path("csv"), true);
    CHECK(rows_of(d, "x") == 2);
    tsdb_close(d);
    CHECK(RUN(path("in.csv"), NULL, path("csv"), "import", "--merge") != 0); /* export options on CSV: refused */
    printf("round trip: ok\n");
}

static void test_filters_and_existing(void)
{
    const char *src = path("src"), *f = path("some.dump");
    char from[32], to[32];
    snprintf(from, sizeof from, "%lld", T0 + 50000 * 1000LL);
    snprintf(to, sizeof to, "%lld", T0 + 60000 * 1000LL);
    CHECK(RUN(NULL, NULL, src, "export", "-o", f, "--series", "mode*", "--series", "i64", "--from", from, "--to", to) ==
          0);
    const char *dst = path("filtered");
    char map[PATH_MAX + 16];
    snprintf(map, sizeof map, "%s=", path("vol1")); /* into the database directory */
    CHECK(RUN(NULL, NULL, dst, "import", f, "--volume", map) == 0);
    tsdb *s = open_db(src, true), *d = open_db(dst, true);
    CHECK(tsdb_series_count(d) == 3);
    CHECK(lookup(d, "temp/a") == UINT32_MAX);
    int64_t a = T0 + 50000 * 1000LL, b = T0 + 60000 * 1000LL;
    CHECK(same_series(s, d, "mode", a, b) && same_series(s, d, "mode2", a, b) && same_series(s, d, "i64", a, b));
    CHECK(rows_of(d, "mode") == 10000);
    CHECK(strcmp(tsdb_series_path(d, lookup(d, "mode2")), dst) == 0);
    tsdb_close(d);

    /* existing series: refused by default (nothing written), skipped, merged, or the range replaced */
    d = open_db(dst, false);
    uint32_t id = lookup(d, "i64");
    int64_t extra_ts[2] = {T0 + 50000 * 1000LL + 1, T0 + 70000 * 1000LL}; /* inside and outside the range */
    int64_t extra_v[2] = {-5, -6};
    insert(d, id, TSDB_INT64, extra_ts, extra_v, NULL, 2);
    tsdb_close(d);
    CHECK(RUN(NULL, NULL, dst, "import", f, "--volume", map) != 0);
    CHECK(err_has("exists"));
    CHECK(err_has("nothing was written"));
    d = open_db(dst, true);
    CHECK(rows_of(d, "i64") == 10002);
    tsdb_close(d);
    CHECK(RUN(NULL, NULL, dst, "import", f, "--volume", map, "--skip-existing") == 0);
    CHECK(err_has("3 skipped"));
    CHECK(RUN(NULL, NULL, dst, "import", f, "--volume", map, "--merge") == 0);
    d = open_db(dst, true);
    CHECK(rows_of(d, "i64") == 10002);
    tsdb_close(d);
    CHECK(RUN(NULL, NULL, dst, "import", f, "--volume", map, "--replace") == 0);
    d = open_db(dst, true);
    CHECK(rows_of(d, "i64") == 10001); /* the row inside the range is gone, the one after it stays */
    CHECK(same_series(s, d, "i64", a, b));
    tsdb_close(d);
    /* a series of another type is refused whatever the option */
    d = open_db(path("typed"), false);
    OK(tsdb_series_create(d, "mode", TSDB_INT64, NULL, &id));
    tsdb_close(d);
    CHECK(RUN(NULL, NULL, path("typed"), "import", f, "--volume", map, "--merge") != 0);
    CHECK(err_has("exists as int64"));
    tsdb_close(s);
    printf("filters and existing series: ok\n");
}

static uint32_t crc32c(const uint8_t *p, size_t n)
{
    uint32_t c = ~0u;
    while (n--) {
        c ^= *p++;
        for (int k = 0; k < 8; k++)
            c = c & 1 ? (c >> 1) ^ 0x82f63b78u : c >> 1;
    }
    return ~c;
}

static uint32_t le32(const uint8_t *p) { return p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24; }
static void st32(uint8_t *p, uint32_t v)
{
    for (int i = 0; i < 4; i++)
        p[i] = (uint8_t)(v >> (8 * i));
}

/* An import of `file` into a fresh database must fail and write nothing (from a file), or leave only whole series
 * (from a pipe). */
static int damaged_no;
static void refused(const char *file, const char *why, bool pipe)
{
    char dst[PATH_MAX + 64];
    snprintf(dst, sizeof dst, "%s/damaged%d", base, damaged_no++);
    char map[PATH_MAX + 16];
    snprintf(map, sizeof map, "%s=", path("vol1"));
    int rc = pipe ? sh("cat '%s' | '%s' '%s' import --volume '%s' 2>%s", file, cli, dst, map, errfile)
                  : RUN(NULL, NULL, dst, "import", file, "--volume", map);
    if (rc == 0) {
        fprintf(stderr, "  %s: accepted\n", why);
        failures++;
        return;
    }
    /* A file that is not an export stream (cut inside the magic) is refused as CSV before any database is opened,
     * so there is none to read: nothing was written. (A read-only open of a missing directory is an error, not an
     * empty database.) */
    struct stat st;
    if (stat(dst, &st) != 0 && errno == ENOENT)
        return;
    tsdb *d = open_db(dst, true), *s = open_db(path("src"), true);
    if (!pipe && tsdb_series_count(d) != 0) {
        fprintf(stderr, "  %s: %u series written\n", why, tsdb_series_count(d));
        failures++;
    }
    for (uint32_t i = 0; i < tsdb_series_count(d); i++) {
        const char *name = tsdb_series_name(d, i);
        if (rows_of(d, name) != 0 && !same_series(s, d, name, TSDB_TS_MIN, TSDB_TS_MAX)) {
            fprintf(stderr, "  %s: series %s half imported\n", why, name);
            failures++;
        }
    }
    tsdb_close(d);
    tsdb_close(s);
}

static void test_damage(void)
{
    size_t n;
    uint8_t *good = (uint8_t *)slurp(path("all.dump"), &n);
    char *bad = path("bad.dump");
    char why[64];
    uint32_t hlen = le32(good + 12);
    size_t first = 16 + hlen + 4; /* the first record */
    /* truncated: in the header, in a record header, in a payload, before the trailer, one byte short */
    size_t cuts[] = {0, 5, 8, 14, 20, first, first + 7, first + 40, n / 3, n / 2, n - 40, n - 1};
    for (size_t i = 0; i < sizeof cuts / sizeof *cuts; i++) {
        spit(bad, good, cuts[i]);
        snprintf(why, sizeof why, "cut at %zu", cuts[i]);
        refused(bad, why, false);
    }
    /* a byte flipped anywhere, or one appended */
    for (int i = 0; i < 40; i++) {
        size_t at = i < 8 ? (size_t)i * 3 : i < 12 ? first + (size_t)(i - 8) * 4 : (size_t)(next() % n);
        good[at] ^= 0x10;
        spit(bad, good, n);
        good[at] ^= 0x10;
        snprintf(why, sizeof why, "byte %zu flipped", at);
        refused(bad, why, false);
    }
    uint8_t *longer = malloc(n + 1);
    memcpy(longer, good, n);
    longer[n] = 0;
    spit(bad, longer, n + 1);
    refused(bad, "a byte appended", false);
    free(longer);
    CHECK(RUN(NULL, NULL, path("nodb"), "import", "--check", bad) != 0);

    /* from a pipe: damage in a later series stops the import; that series is removed again, earlier ones are whole */
    for (int i = 0; i < 6; i++) {
        size_t at = n / 2 + (size_t)i * (n / 13);
        good[at] ^= 0x01;
        spit(bad, good, n);
        good[at] ^= 0x01;
        snprintf(why, sizeof why, "pipe, byte %zu flipped", at);
        refused(bad, why, true);
        CHECK(err_has("damaged") || err_has("checksum"));
    }
    spit(bad, good, n / 2);
    refused(bad, "pipe, cut in half", true);
    CHECK(err_has("truncated"));

    /* a newer stream version: refused, saying so */
    uint8_t *v = malloc(n);
    memcpy(v, good, n);
    st32(v + 8, 2);
    st32(v + 16 + hlen, crc32c(v, 16 + hlen));
    spit(bad, v, n);
    CHECK(RUN(NULL, NULL, path("newer"), "import", bad) != 0);
    CHECK(err_has("version 2") && err_has("newer tsdb"));
    /* a record kind this version does not know (checksum fixed up) */
    memcpy(v, good, n);
    v[first] = 9;
    uint32_t len = le32(v + first + 4);
    uint32_t crc = crc32c(v + first, 12);
    {
        /* crc over the 12 header bytes then the payload: continue the bitwise CRC */
        uint8_t *tmp = malloc(12 + len);
        memcpy(tmp, v + first, 12);
        memcpy(tmp + 12, v + first + 16, len);
        crc = crc32c(tmp, 12 + len);
        free(tmp);
    }
    st32(v + first + 12, crc);
    spit(bad, v, n);
    CHECK(RUN(NULL, NULL, path("newer"), "import", bad) != 0);
    CHECK(err_has("kind 9"));
    /* not a stream at all */
    CHECK(RUN(NULL, NULL, path("nodb"), "import", "--check", path("in.csv")) != 0);
    CHECK(err_has("not a tsdb export stream"));
    free(v);
    free(good);
    printf("damaged streams: ok\n");
}

/* The stream of the first release imports, and reads exactly what build_source(golden) writes. */
static void test_golden(const char *golden)
{
    if (access(golden, R_OK) != 0) {
        fprintf(stderr, "  %s missing\n", golden);
        failures++;
        return;
    }
    const char *src = path("golden-src"), *dst = path("golden-dst");
    rng = 88172645463325252ull;
    build_source(src, NULL, true);
    CHECK(RUN(NULL, NULL, dst, "import", golden) == 0);
    tsdb *s = open_db(src, true), *d = open_db(dst, true);
    CHECK(tsdb_series_count(d) == 6);
    for (int i = 0; i < NALL; i++)
        if (strcmp(all_series[i], "mode2") != 0)
            CHECK(same_series(s, d, all_series[i], TSDB_TS_MIN, TSDB_TS_MAX));
    tsdb_close(s);
    tsdb_close(d);
    printf("golden stream: ok\n");
}

/* A writer rewrites every series with one value per generation (one insert each: WAL-sized and part-sized) while
 * exports run; each exported series must hold one generation whole. */
#define CS 6
static void test_concurrent(void)
{
    const char *dir = path("live"), *stop = path("live.stop"), *ready = path("live.ready");
    pid_t pid = fork();
    if (pid == 0) {
        tsdb_options o;
        tsdb_options_init(&o);
        o.flush_interval_ms = 50;
        tsdb *db;
        OK(tsdb_open(dir, &o, &db));
        uint32_t id[CS];
        char name[16];
        size_t rows[CS];
        for (int s = 0; s < CS; s++) {
            snprintf(name, sizeof name, "s%d", s);
            OK(tsdb_series_create(db, name, TSDB_FLOAT64, NULL, &id[s]));
            rows[s] = s % 2 ? 3000 : 70000;
        }
        int64_t *ts = malloc(70000 * 8);
        double *v = malloc(70000 * 8);
        for (int g = 1; access(stop, F_OK) != 0 && g < 100000; g++) {
            for (int s = 0; s < CS; s++) {
                for (size_t i = 0; i < rows[s]; i++)
                    ts[i] = T0 + (int64_t)i * 10, v[i] = g;
                tsdb_values c = {TSDB_FLOAT64, v, NULL};
                OK(tsdb_insert_series_values(db, id[s], ts, &c, rows[s]));
            }
            if (g == 3)
                close(open(ready, O_CREAT | O_WRONLY, 0644));
        }
        tsdb_close(db);
        _exit(0);
    }
    for (int i = 0; i < 3000 && access(ready, F_OK) != 0; i++)
        usleep(10000);
    CHECK(access(ready, F_OK) == 0);
    int mixed = 0;
    for (int round = 0; round < 6; round++) {
        char *f = path("live.dump"), dst[PATH_MAX + 64];
        snprintf(dst, sizeof dst, "%s/live-copy%d", base, round);
        CHECK(RUN(NULL, NULL, dir, "export", "-o", f) == 0);
        CHECK(RUN(NULL, NULL, dst, "import", f) == 0);
        tsdb *d = open_db(dst, true);
        CHECK(tsdb_series_count(d) == CS);
        for (int s = 0; s < CS; s++) {
            char name[16];
            snprintf(name, sizeof name, "s%d", s);
            col c = read_series(d, lookup(d, name), TSDB_TS_MIN, TSDB_TS_MAX);
            bool one = c.n == (s % 2 ? 3000u : 70000u);
            for (size_t i = 1; one && i < c.n; i++)
                one = c.v[i] == c.v[0] && c.ok[i];
            if (!one) {
                fprintf(stderr, "  round %d series %s: %zu rows, not one generation\n", round, name, c.n);
                mixed++;
            }
            col_free(&c);
        }
        tsdb_close(d);
    }
    CHECK(mixed == 0);
    close(open(stop, O_CREAT | O_WRONLY, 0644));
    int st;
    if (pid > 0)
        waitpid(pid, &st, 0);
    CHECK(pid > 0 && WIFEXITED(st) && WEXITSTATUS(st) == 0);
    printf("export while written: ok\n");
}

/* Anonymous memory of export and import does not grow with the database. */
static void big_db(const char *dir, size_t rows)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    uint32_t id[4];
    for (int s = 0; s < 4; s++) {
        char name[16];
        snprintf(name, sizeof name, "big%d", s);
        OK(tsdb_series_create(db, name, s % 2 ? TSDB_FLOAT64 : TSDB_INT64, NULL, &id[s]));
    }
    size_t step = 1 << 20;
    int64_t *ts = malloc(step * 8), *v = malloc(step * 8);
    for (int s = 0; s < 4; s++)
        for (size_t at = 0; at < rows / 4; at += step) {
            size_t k = rows / 4 - at < step ? rows / 4 - at : step;
            for (size_t i = 0; i < k; i++) {
                ts[i] = T0 + (int64_t)(at + i) * 1000;
                double x = (double)(next() % 100000) / 10.0;
                if (s % 2)
                    memcpy(&v[i], &x, 8);
                else
                    v[i] = (int64_t)(next() % 1000000);
            }
            tsdb_values c = {s % 2 ? TSDB_FLOAT64 : TSDB_INT64, v, NULL};
            OK(tsdb_insert_series_values(db, id[s], ts, &c, k));
        }
    free(ts);
    free(v);
    OK(tsdb_close(db));
}

static void test_memory(void)
{
    const char *big = getenv("TSDB_EXPORT_BIG_ROWS");
    size_t rows[2] = {1000000, big ? strtoull(big, NULL, 10) : SANITIZED ? 2000000 : 8000000};
    long ex[2], im[2];
    for (int k = 0; k < 2; k++) {
        char src[PATH_MAX + 64], dst[PATH_MAX + 64], f[PATH_MAX + 64];
        snprintf(src, sizeof src, "%s/mem-src%d", base, k);
        snprintf(dst, sizeof dst, "%s/mem-dst%d", base, k);
        snprintf(f, sizeof f, "%s/mem%d.dump", base, k);
        big_db(src, rows[k]);
        struct timespec t0, t1, t2;
        clock_gettime(CLOCK_MONOTONIC, &t0);
        CHECK(RUN_PEAK(&ex[k], NULL, NULL, src, "export", "-o", f) == 0);
        clock_gettime(CLOCK_MONOTONIC, &t1);
        CHECK(RUN_PEAK(&im[k], NULL, NULL, dst, "import", f) == 0);
        clock_gettime(CLOCK_MONOTONIC, &t2);
        tsdb *d = open_db(dst, true);
        CHECK(rows_of(d, "big1") == rows[k] / 4);
        tsdb_close(d);
        struct stat st;
        stat(f, &st);
        printf("  %zu rows: export %.2f s, %ld kB anonymous; import %.2f s, %ld kB; %.2f bytes/row\n", rows[k],
               (double)(t1.tv_sec - t0.tv_sec) + (double)(t1.tv_nsec - t0.tv_nsec) / 1e9, ex[k],
               (double)(t2.tv_sec - t1.tv_sec) + (double)(t2.tv_nsec - t1.tv_nsec) / 1e9, im[k],
               (double)st.st_size / (double)rows[k]);
        unlink(f);
    }
    /* the larger database may not need more than a few MB more (allocator noise), and neither is large */
    CHECK(SANITIZED || (ex[1] < ex[0] + 16384 && ex[1] < 128 * 1024));
    CHECK(SANITIZED || (im[1] < im[0] + 32768 && im[1] < 256 * 1024));
    printf("constant memory: ok\n");
}

int main(int argc, char **argv)
{
    const char *cli_env = getenv("TSDB_CLI");
    if (cli_env)
        snprintf(cli, sizeof cli, "%s", cli_env);
    else {
        char self[PATH_MAX - 16];
        ssize_t n = readlink("/proc/self/exe", self, sizeof self - 1);
        self[n > 0 ? n : 0] = 0;
        char *slash = strrchr(self, '/');
        if (slash)
            *slash = 0;
        snprintf(cli, sizeof cli, "%s/tsdb", self);
    }
    if (argc == 3 && strcmp(argv[1], "--write-golden") == 0) {
        char tmpl[] = "/tmp/tsdb-golden.XXXXXX";
        snprintf(base, sizeof base, "%s", mkdtemp(tmpl));
        snprintf(errfile, sizeof errfile, "%s/stderr", base);
        build_source(path("src"), NULL, true);
        int rc = RUN(NULL, NULL, path("src"), "export", "-o", argv[2]);
        sh("rm -rf '%s'", base);
        return rc;
    }
    char tmpl[PATH_MAX];
    snprintf(tmpl, sizeof tmpl, "%s/tsdb-export.XXXXXX", argc > 1 ? argv[1] : "/tmp");
    if (!mkdtemp(tmpl)) {
        perror(tmpl);
        return 1;
    }
    snprintf(base, sizeof base, "%s", tmpl);
    snprintf(errfile, sizeof errfile, "%s/stderr", base);
    if (access(cli, X_OK) != 0) {
        fprintf(stderr, "test_export: no tsdb binary at %s (set TSDB_CLI)\n", cli);
        return 1;
    }
    const char *g = getenv("TSDB_GOLDEN") ? getenv("TSDB_GOLDEN") : "tests/data/export-v1.tsdbdump";
    char golden[2 * PATH_MAX + 2], cwd[PATH_MAX];
    if (g[0] != '/' && getcwd(cwd, sizeof cwd))
        snprintf(golden, sizeof golden, "%s/%s", cwd, g);
    else
        snprintf(golden, sizeof golden, "%s", g);
    test_round_trip();
    test_filters_and_existing();
    test_damage();
    test_golden(golden);
    test_concurrent();
    test_memory();
    if (!failures)
        sh("rm -rf '%s'", base);
    printf("%s (%d failures)\n", failures ? "FAIL" : "all export tests passed", failures);
    return failures ? 1 : 0;
}
