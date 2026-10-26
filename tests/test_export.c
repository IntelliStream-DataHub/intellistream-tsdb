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

/* strcmp that a NULL (a series missing) makes false instead of a crash */
static bool same_str(const char *a, const char *b) { return a && b && strcmp(a, b) == 0; }

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
    CHECK(same_str(tsdb_series_path(d, lookup(d, "f64")), path("vol2")));
    CHECK(same_str(tsdb_series_path(d, lookup(d, "temp/a")), dst));
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
    CHECK(same_str(tsdb_series_path(d, lookup(d, "mode2")), dst));
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

/* ------------------------------------------------------------------ crafted streams */

/* Streams written here byte by byte (checksums right), each with one defect, so that every check of the reader is
 * seen to refuse it: the export of a real database never has these defects. */
typedef struct sbuf {
    uint8_t *p;
    size_t n, cap;
} sbuf;

static void sb_put(sbuf *b, const void *p, size_t n)
{
    if (b->n + n > b->cap) {
        b->cap = (b->n + n) * 2 + 64;
        b->p = realloc(b->p, b->cap);
        if (!b->p)
            exit(1);
    }
    memcpy(b->p + b->n, p, n);
    b->n += n;
}

static void sb_le(sbuf *b, uint64_t v, int bytes)
{
    uint8_t x[8];
    for (int i = 0; i < bytes; i++)
        x[i] = (uint8_t)(v >> (8 * i));
    sb_put(b, x, (size_t)bytes);
}

static void sb_str(sbuf *b, const char *s)
{
    sb_le(b, strlen(s), 4);
    sb_put(b, s, strlen(s));
}

static void sb_varint(sbuf *b, uint64_t v)
{
    while (v >= 0x80) {
        uint8_t x = (uint8_t)(v | 0x80);
        sb_put(b, &x, 1);
        v >>= 7;
    }
    uint8_t x = (uint8_t)v;
    sb_put(b, &x, 1);
}

typedef struct crafter {
    sbuf out, rec;
    uint32_t seq;
    uint64_t series, rows, s_rows, s_nulls;
    uint32_t s_chunks;
} crafter;

static void cr_header(crafter *c, uint32_t flags, int64_t from, int64_t to)
{
    sbuf p = {0};
    sb_le(&p, 5, 4);
    sb_le(&p, flags, 4);
    sb_le(&p, 0, 8);
    sb_le(&p, (uint64_t)from, 8);
    sb_le(&p, (uint64_t)to, 8);
    sb_le(&p, 4, 2);
    sb_put(&p, "test", 4);
    sb_put(&c->out, "TSDBDUMP", 8);
    sb_le(&c->out, 1, 4);
    sb_le(&c->out, p.n, 4);
    sb_put(&c->out, p.p, p.n);
    sb_le(&c->out, crc32c(c->out.p, c->out.n), 4);
    free(p.p);
}

static void cr_record(crafter *c, uint8_t kind, uint8_t flags)
{
    sbuf r = {0};
    uint8_t h[16] = {kind, flags};
    st32(h + 4, (uint32_t)c->rec.n);
    st32(h + 8, c->seq++);
    sb_put(&r, h, 12);
    sb_put(&r, c->rec.p, c->rec.n);
    st32(h + 12, crc32c(r.p, r.n));
    sb_put(&c->out, h, 16);
    sb_put(&c->out, c->rec.p, c->rec.n);
    c->rec.n = 0;
    free(r.p);
}

static void cr_series(crafter *c, const char *name, const char *vol, tsdb_type type, int64_t retention)
{
    sb_str(&c->rec, name);
    sb_str(&c->rec, vol);
    sb_le(&c->rec, type, 1);
    sb_le(&c->rec, (uint64_t)retention, 8);
    cr_record(c, 1, 0);
    c->s_rows = c->s_nulls = 0;
    c->s_chunks = 0;
}

static void cr_state(crafter *c, uint32_t code, const char *name)
{
    sb_le(&c->rec, 1, 4);
    sb_le(&c->rec, code, 4);
    sb_str(&c->rec, name);
    cr_record(c, 2, 0);
}

/* rows uncompressed: values of `w` bytes each; valid NULL: no nulls */
static void cr_chunk(crafter *c, unsigned w, const int64_t *ts, const void *vals, const uint8_t *valid, uint32_t rows,
                     uint8_t flags)
{
    sbuf raw = {0};
    uint8_t f = (uint8_t)(flags | (valid ? 1 : 0));
    sb_put(&raw, &f, 1);
    for (uint32_t i = 0; i < rows; i++)
        sb_varint(&raw, i ? (uint64_t)ts[i] - (uint64_t)ts[i - 1] : (uint64_t)ts[0] << 1 ^ (uint64_t)(ts[0] >> 63));
    for (unsigned k = 0; k < w; k++)
        for (uint32_t i = 0; i < rows; i++)
            sb_put(&raw, (const uint8_t *)vals + (size_t)i * w + k, 1);
    if (valid)
        sb_put(&raw, valid, (rows + 7) / 8);
    sb_le(&c->rec, rows, 4);
    sb_le(&c->rec, raw.n, 4);
    sb_le(&c->rec, 0, 1);
    sb_put(&c->rec, raw.p, raw.n);
    cr_record(c, 3, 0);
    free(raw.p);
    c->s_rows += rows;
    c->s_chunks++;
    for (uint32_t i = 0; valid && i < rows; i++)
        c->s_nulls += !(valid[i >> 3] >> (i & 7) & 1);
}

static void cr_end(crafter *c, int64_t rows_delta)
{
    sb_le(&c->rec, c->s_rows + (uint64_t)rows_delta, 8);
    sb_le(&c->rec, c->s_nulls, 8);
    sb_le(&c->rec, c->s_chunks, 4);
    cr_record(c, 4, 0);
    c->series++;
    c->rows += c->s_rows;
}

static void cr_finish(crafter *c, const char *file)
{
    sb_le(&c->rec, c->series, 8);
    sb_le(&c->rec, c->rows, 8);
    sb_le(&c->rec, c->seq, 8);
    cr_record(c, 5, 0);
    spit(file, c->out.p, c->out.n);
    free(c->out.p);
    free(c->rec.p);
}

/* One defect each; all zero: a good stream of a float32 series "f" (2 chunks), a bool "b" and a state "s". */
typedef struct knobs {
    uint32_t hdr_flags, chunk_flags, state_code;
    uint8_t rec_flags;
    int64_t end_delta, retention;
    bool seq_skip, unordered, unordered_chunks, out_of_range, before_range, null_f32, null_i64, bool2, dup, unnamed;
    bool optional_rec, unknown_rec;
    const char *vol;
} knobs;

#define CR_FROM (T0 - 1000000)
#define CR_TO (T0 + 1000000)

static void craft(const char *file, const knobs *k)
{
    crafter c = {0};
    cr_header(&c, k->hdr_flags, CR_FROM, CR_TO);
    cr_series(&c, "f", k->vol ? k->vol : "", TSDB_FLOAT32, k->retention);
    int64_t ts[3] = {T0, T0 + 1000, T0 + 2000};
    float fv[3] = {1.5f, -2.0f, 3.25f};
    if (k->unordered)
        ts[2] = T0 + 500;
    if (k->before_range)
        ts[0] = CR_FROM - 1;
    if (k->null_f32) {
        uint32_t bits = TSDB_NULL_F32_BITS;
        memcpy(&fv[1], &bits, 4);
    }
    cr_chunk(&c, 4, ts, fv, NULL, 3, (uint8_t)k->chunk_flags);
    if (k->optional_rec || k->unknown_rec) {
        sb_le(&c.rec, 42, 4);
        cr_record(&c, 9, k->optional_rec ? 1 : 0);
    }
    /* past the range only in the last chunk of f, where no later row is out of order because of it */
    int64_t ts2[2] = {k->unordered_chunks ? T0 + 1500 : T0 + 3000, k->out_of_range ? CR_TO : T0 + 4000};
    uint8_t valid = 0x2; /* the first row null */
    cr_chunk(&c, 4, ts2, fv, &valid, 2, 0);
    if (k->seq_skip)
        c.seq++;
    cr_end(&c, k->end_delta);
    cr_series(&c, "b", "", TSDB_BOOL, 0);
    uint8_t bv[3] = {1, 0, (uint8_t)(k->bool2 ? 2 : 1)};
    cr_chunk(&c, 1, ts, bv, NULL, 3, 0);
    cr_end(&c, 0);
    cr_series(&c, "s", "", TSDB_STATE, 0);
    uint32_t code = k->state_code ? k->state_code : 3;
    if (!k->unnamed)
        cr_state(&c, code, "on");
    uint32_t sv[3] = {code, code, code};
    cr_chunk(&c, 4, ts, sv, NULL, 3, 0);
    cr_end(&c, 0);
    cr_series(&c, "i", "", TSDB_INT64, 0);
    int64_t iv[3] = {INT64_MAX, k->null_i64 ? INT64_MIN : INT64_MIN + 1, 0};
    cr_chunk(&c, 8, ts, iv, NULL, 3, 0);
    cr_end(&c, 0);
    if (k->dup) {
        cr_series(&c, "b", "", TSDB_BOOL, 0);
        cr_chunk(&c, 1, ts, bv, NULL, 3, 0);
        cr_end(&c, 0);
    }
    cr_finish(&c, file);
}

static int crafted_no;

/* The crafted stream with defect k is refused from a file, naming `what`, and nothing is written. */
static void craft_refused(const knobs *k, const char *what, const char *why)
{
    char *f = path("crafted.dump"), dst[PATH_MAX + 64];
    craft(f, k);
    snprintf(dst, sizeof dst, "%s/crafted%d", base, crafted_no++);
    if (RUN(NULL, NULL, dst, "import", f) == 0) {
        fprintf(stderr, "  crafted (%s): accepted\n", why);
        failures++;
        return;
    }
    if (!err_has(what)) {
        fprintf(stderr, "  crafted (%s): refused for another reason\n", why);
        failures++;
    }
    struct stat st;
    if (stat(dst, &st) == 0) {
        tsdb *d = open_db(dst, true);
        if (tsdb_series_count(d) != 0) {
            fprintf(stderr, "  crafted (%s): %u series written\n", why, tsdb_series_count(d));
            failures++;
        }
        tsdb_close(d);
    }
}

static void test_crafted(void)
{
    char *f = path("crafted.dump");
    /* the good one imports, values intact */
    craft(f, &(knobs){0});
    const char *good = path("crafted-good");
    CHECK(RUN(NULL, NULL, good, "import", f) == 0);
    tsdb *d = open_db(good, true);
    CHECK(rows_of(d, "f") == 5 && rows_of(d, "b") == 3 && rows_of(d, "s") == 3 && rows_of(d, "i") == 3);
    col c = read_series(d, lookup(d, "s"), TSDB_TS_MIN, TSDB_TS_MAX);
    CHECK(c.n == 3 && same_str(tsdb_state_name(d, lookup(d, "s"), (uint32_t)c.v[0]), "on"));
    col_free(&c);
    c = read_series(d, lookup(d, "f"), TSDB_TS_MIN, TSDB_TS_MAX);
    CHECK(c.n == 5 && c.ok[0] && !c.ok[3] && c.ok[4]);
    col_free(&c);
    tsdb_close(d);
    /* an optional record of an unknown kind is skipped */
    craft(f, &(knobs){.optional_rec = true});
    CHECK(RUN(NULL, NULL, path("crafted-opt"), "import", f) == 0);

    craft_refused(&(knobs){.unknown_rec = true}, "kind 9", "unknown kind");
    craft_refused(&(knobs){.hdr_flags = 2}, "header flags", "header flags");
    craft_refused(&(knobs){.chunk_flags = 2}, "newer tsdb", "chunk flags");
    craft_refused(&(knobs){.end_delta = 1}, "the stream says", "END rows");
    craft_refused(&(knobs){.seq_skip = true}, "out of sequence", "sequence");
    craft_refused(&(knobs){.unordered = true}, "out of order", "times out of order in a chunk");
    craft_refused(&(knobs){.unordered_chunks = true}, "out of order", "times out of order across chunks");
    craft_refused(&(knobs){.out_of_range = true}, "out of range", "time past the header's range");
    craft_refused(&(knobs){.before_range = true}, "out of range", "time before the header's range");
    craft_refused(&(knobs){.null_f32 = true}, "reserved for null", "float32 null bits as a value");
    craft_refused(&(knobs){.null_i64 = true}, "reserved for null", "INT64_MIN as a value");
    craft_refused(&(knobs){.bool2 = true}, "reserved for null", "bool 2");
    craft_refused(&(knobs){.dup = true}, "twice", "a series twice");
    craft_refused(&(knobs){.unnamed = true}, "without a name", "a state code without a name");

    /* a record flag this version does not know (bit 1) */
    craft(f, &(knobs){0});
    {
        size_t n;
        uint8_t *s = (uint8_t *)slurp(f, &n);
        size_t first = 16 + le32(s + 12) + 4, len = le32(s + first + 4);
        s[first + 1] = 2;
        uint8_t *tmp = malloc(12 + len);
        memcpy(tmp, s + first, 12);
        memcpy(tmp + 12, s + first + 16, len);
        st32(s + first + 12, crc32c(tmp, 12 + len));
        free(tmp);
        spit(path("recflag.dump"), s, n);
        CHECK(RUN(NULL, NULL, path("recflag"), "import", path("recflag.dump")) != 0);
        CHECK(err_has("flags 0x2"));
        /* the magic damaged, the NUL bytes of the version intact: refused, not read as CSV */
        s[0] = 'X';
        spit(path("magic.dump"), s, n);
        CHECK(RUN(NULL, NULL, path("magic"), "import", path("magic.dump")) != 0);
        CHECK(err_has("nor CSV"));
        CHECK(access(path("magic"), F_OK) != 0);
        free(s);
    }

    /* state codes anywhere in u32: memory by the names, not by the code */
    long peak = 0;
    craft(f, &(knobs){.state_code = 0x0FFFFFFF});
    CHECK(RUN_PEAK(&peak, NULL, NULL, path("bigcode1"), "import", f) == 0);
    CHECK(SANITIZED || peak < 64 * 1024);
    long peak2 = 0;
    craft(f, &(knobs){.state_code = 0xFFFFFFFF});
    CHECK(RUN_PEAK(&peak2, NULL, NULL, path("bigcode2"), "import", f) == 0);
    CHECK(SANITIZED || peak2 < 64 * 1024);
    d = open_db(path("bigcode2"), true);
    c = read_series(d, lookup(d, "s"), TSDB_TS_MIN, TSDB_TS_MAX);
    CHECK(c.n == 3 && same_str(tsdb_state_name(d, lookup(d, "s"), (uint32_t)c.v[2]), "on"));
    col_free(&c);
    tsdb_close(d);
    printf("  state codes 0x0FFFFFFF / 0xFFFFFFFF: import peak %ld / %ld kB anonymous\n", peak, peak2);

    /* retention: rows already past it are reported; --no-retention imports them without it */
    craft(f, &(knobs){.retention = 1000});
    CHECK(RUN(NULL, NULL, path("ret1"), "import", f) == 0);
    CHECK(err_has("series f: 5 of its 5 rows are older than its retention"));
    CHECK(RUN(NULL, NULL, path("ret2"), "import", f, "--no-retention") == 0);
    d = open_db(path("ret2"), true);
    int64_t keep = -1;
    CHECK(rows_of(d, "f") == 5 && tsdb_get_retention(d, lookup(d, "f"), &keep) == 0 && keep == 0);
    tsdb_close(d);

    /* --replace: from a file only; sets the stream's retention (or keeps the target's with --no-retention); a
     * series larger than one batch is deleted, then written */
    craft(f, &(knobs){.retention = 365LL * 24 * 3600 * 1000 * 100});
    const char *rp = path("replace");
    d = open_db(rp, false);
    uint32_t id;
    OK(tsdb_series_create(d, "f", TSDB_FLOAT32, NULL, &id));
    int64_t old_ts[2] = {T0 + 500, CR_TO + 5};
    float old_v[2] = {9, 9};
    insert(d, id, TSDB_FLOAT32, old_ts, old_v, NULL, 2);
    tsdb_close(d);
    CHECK(sh("cat '%s' | '%s' '%s' import --replace 2>%s", f, cli, rp, errfile) != 0);
    CHECK(err_has("needs the stream in a file"));
    CHECK(RUN(NULL, NULL, rp, "import", f, "--replace", "--no-verify") != 0);
    CHECK(err_has("--no-verify"));
    d = open_db(rp, true);
    CHECK(rows_of(d, "f") == 2 && tsdb_series_count(d) == 1);
    tsdb_close(d);
    CHECK(RUN(NULL, NULL, rp, "import", f, "--replace", "--no-retention") == 0);
    d = open_db(rp, true);
    CHECK(rows_of(d, "f") == 6); /* the row inside the range replaced, the one after it kept */
    CHECK(tsdb_get_retention(d, lookup(d, "f"), &keep) == 0 && keep == 0);
    tsdb_close(d);
    d = open_db(rp, false); /* the row inside the range again: the batched path must delete it too */
    insert(d, lookup(d, "f"), TSDB_FLOAT32, old_ts, old_v, NULL, 1);
    CHECK(rows_of(d, "f") == 7);
    tsdb_close(d);
    CHECK(RUN(NULL, NULL, rp, "import", f, "--replace", "--batch-rows", "2") == 0);
    d = open_db(rp, true);
    CHECK(rows_of(d, "f") == 6);
    CHECK(tsdb_get_retention(d, lookup(d, "f"), &keep) == 0 && keep == 365LL * 24 * 3600 * 1000 * 100);
    tsdb_close(d);

    /* a pipe that breaks inside a series of several batches: the whole series before it stay, that one is emptied
     * again and named; a rerun with --merge completes it */
    {
        crafter cr = {0};
        cr_header(&cr, 0, CR_FROM, CR_TO);
        cr_series(&cr, "first", "", TSDB_FLOAT32, 0);
        int64_t t1[3] = {T0, T0 + 1, T0 + 2};
        float v1[3] = {1, 2, 3};
        cr_chunk(&cr, 4, t1, v1, NULL, 3, 0);
        cr_end(&cr, 0);
        cr_series(&cr, "big", "", TSDB_FLOAT32, 0);
        int64_t tb[1000];
        float vb[1000];
        for (int ch = 0; ch < 6; ch++) {
            for (int i = 0; i < 1000; i++) {
                tb[i] = T0 + ch * 1000 + i;
                vb[i] = (float)(ch * 1000 + i);
            }
            cr_chunk(&cr, 4, tb, vb, NULL, 1000, 0);
        }
        size_t whole = cr.out.n;
        cr_end(&cr, 0);
        cr_finish(&cr, path("pipe-good.dump"));
        size_t n;
        uint8_t *s = (uint8_t *)slurp(path("pipe-good.dump"), &n);
        s[whole - 5] ^= 1; /* in the last chunk: its checksum fails */
        spit(path("pipe-bad.dump"), s, n);
        free(s);
        const char *pd = path("pipe-dst");
        CHECK(sh("cat '%s' | '%s' '%s' import --batch-rows 1000 2>%s", path("pipe-bad.dump"), cli, pd, errfile) != 0);
        CHECK(err_has("series big, which this import created, is left empty") && err_has("--merge"));
        CHECK(err_has("1 series before it"));
        d = open_db(pd, true);
        CHECK(rows_of(d, "first") == 3 && lookup(d, "big") != UINT32_MAX && rows_of(d, "big") == 0);
        tsdb_close(d);
        CHECK(sh("cat '%s' | '%s' '%s' import --merge --batch-rows 1000 2>%s", path("pipe-good.dump"), cli, pd,
                 errfile) == 0);
        d = open_db(pd, true);
        CHECK(rows_of(d, "first") == 3 && rows_of(d, "big") == 6000);
        tsdb_close(d);
    }

    /* empty and short inputs are refused, not "imported" */
    spit(path("empty.dump"), "", 0);
    CHECK(RUN(NULL, NULL, path("empty"), "import", path("empty.dump")) != 0);
    CHECK(err_has("is empty"));
    CHECK(RUN("/dev/null", NULL, path("empty"), "import") != 0);
    CHECK(err_has("is empty"));
    spit(path("short.dump"), "TSDBD", 5);
    CHECK(RUN(NULL, NULL, path("empty"), "import", path("short.dump")) != 0);
    CHECK(err_has("shorter than a stream header"));
    CHECK(access(path("empty"), F_OK) != 0);
    /* CSV whose first series happens to start with the magic: still CSV */
    spit(path("magic.csv"), "TSDBDUMPster,1000,1.5\n", 22);
    CHECK(RUN(NULL, NULL, path("magiccsv"), "import", path("magic.csv")) == 0);
    d = open_db(path("magiccsv"), true);
    CHECK(rows_of(d, "TSDBDUMPster") == 1);
    tsdb_close(d);
    printf("crafted streams: ok\n");
}

/* Volume paths are checked before anything is written; --volume-root maps a whole tree. */
static void test_volumes(void)
{
    char *f = path("vol.dump"), map[2 * PATH_MAX + 16];
    craft(f, &(knobs){.vol = "/old/mnt/vol"});
    /* not a volume of the target: the message names both paths and the ways out */
    CHECK(RUN(NULL, NULL, path("v0"), "import", f) != 0);
    CHECK(err_has("volume /old/mnt/vol, which is not") && err_has("--volume /old/mnt/vol=NEW") &&
          err_has("--volume-root"));
    snprintf(map, sizeof map, "/old/mnt/vol=%s", path("v0vol"));
    CHECK(RUN(NULL, NULL, path("v0"), "import", f, "--volume", map) != 0);
    CHECK(err_has(path("v0vol")) && err_has("(--volume /old/mnt/vol=NEW"));
    /* invalid maps */
    CHECK(RUN(NULL, NULL, path("v1"), "import", f, "--volume", "/old/mnt/vol=rel/dir", "--create-volumes") == 2);
    CHECK(RUN(NULL, NULL, path("v1"), "import", f, "--volume", "/old/mnt/vol=/a/../b", "--create-volumes") == 2);
    CHECK(access(path("v1"), F_OK) != 0);
    /* paths that cannot be added: refused before anything is written */
    spit(path("afile"), "x", 1);
    snprintf(map, sizeof map, "/old/mnt/vol=%s/sub", path("afile"));
    CHECK(RUN(NULL, NULL, path("v2"), "import", f, "--volume", map, "--create-volumes") != 0);
    CHECK(err_has("cannot be added"));
    snprintf(map, sizeof map, "/old/mnt/vol=%s", path("src")); /* another database */
    CHECK(RUN(NULL, NULL, path("v2"), "import", f, "--volume", map, "--create-volumes") != 0);
    CHECK(err_has("holds tsdb files"));
    snprintf(map, sizeof map, "/old/mnt/vol=%s/inside", path("v2")); /* inside the target */
    CHECK(RUN(NULL, NULL, path("v2"), "import", f, "--volume", map, "--create-volumes") != 0);
    CHECK(err_has("inside the database directory"));
    tsdb *d = open_db(path("v2"), true);
    CHECK(tsdb_series_count(d) == 0);
    tsdb_close(d);
    /* trailing slashes are the same path */
    snprintf(map, sizeof map, "/old/mnt/vol/=%s/", path("v3vol"));
    CHECK(RUN(NULL, NULL, path("v3"), "import", f, "--volume", map, "--create-volumes") == 0);
    d = open_db(path("v3"), true);
    CHECK(same_str(tsdb_series_path(d, lookup(d, "f")), path("v3vol")));
    CHECK(same_str(tsdb_series_path(d, lookup(d, "b")), path("v3")));
    tsdb_close(d);
    /* --volume-root: every volume under OLD to the same place under NEW (here /old/mnt/vol -> NEW/mnt/vol) */
    sh("mkdir -p '%s/mnt'", path("root4"));
    snprintf(map, sizeof map, "/old=%s", path("root4"));
    CHECK(RUN(NULL, NULL, path("v4"), "import", f, "--volume-root", map, "--create-volumes") == 0);
    d = open_db(path("v4"), true);
    CHECK(same_str(tsdb_series_path(d, lookup(d, "f")), path("root4/mnt/vol")));
    tsdb_close(d);

    /* two state series of one volume share its dictionary: each stream names its own codes */
    const char *src = path("shared-src");
    d = open_db(src, false);
    uint32_t a, b, on, off;
    OK(tsdb_series_create(d, "s1", TSDB_STATE, NULL, &a));
    OK(tsdb_series_create(d, "s2", TSDB_STATE, NULL, &b));
    OK(tsdb_state(d, a, "on", &on));
    OK(tsdb_state(d, a, "off", &off));
    int64_t ts[2] = {T0, T0 + 1};
    uint32_t v[2] = {on, off};
    insert(d, a, TSDB_STATE, ts, v, NULL, 2);
    insert(d, b, TSDB_STATE, ts, v, NULL, 2);
    /* names long enough that one chunk's names exceed a STATES record of this exporter: split */
    uint32_t lg;
    OK(tsdb_series_create(d, "long", TSDB_STATE, NULL, &lg));
    enum { NLONG = 60, LLEN = 20000 };
    int64_t lts[NLONG];
    uint32_t lv[NLONG];
    char *nm = malloc(LLEN + 1);
    for (int i = 0; i < NLONG; i++) {
        memset(nm, 'a' + i % 26, LLEN);
        snprintf(nm, LLEN, "%02d", i);
        nm[2] = 'x';
        nm[LLEN] = 0;
        OK(tsdb_state(d, lg, nm, &lv[i]));
        lts[i] = T0 + i;
    }
    free(nm);
    insert(d, lg, TSDB_STATE, lts, lv, NULL, NLONG);
    tsdb_close(d);
    char *sf = path("shared.dump");
    CHECK(RUN(NULL, NULL, src, "export", "-o", sf) == 0);
    size_t n;
    uint8_t *s = (uint8_t *)slurp(sf, &n);
    int states = 0;
    for (size_t at = 16 + le32(s + 12) + 4; at + 16 <= n; at += 16 + le32(s + at + 4))
        states += s[at] == 2;
    free(s);
    CHECK(states >= 4); /* s1, s2, and long in two or more */
    CHECK(RUN(NULL, NULL, path("shared-dst"), "import", sf) == 0);
    tsdb *x = open_db(src, true);
    d = open_db(path("shared-dst"), true);
    CHECK(same_series(x, d, "s1", TSDB_TS_MIN, TSDB_TS_MAX) && same_series(x, d, "s2", TSDB_TS_MIN, TSDB_TS_MAX) &&
          same_series(x, d, "long", TSDB_TS_MIN, TSDB_TS_MAX));
    tsdb_close(d);
    tsdb_close(x);

    /* export never overwrites a FILE.partial it did not make, unless --force */
    char *out = path("p.dump"), *part = path("p.dump.partial");
    spit(part, "keep", 4);
    CHECK(RUN(NULL, NULL, src, "export", "-o", out) != 0);
    CHECK(err_has("exists") && access(out, F_OK) != 0);
    s = (uint8_t *)slurp(part, &n);
    CHECK(n == 4);
    free(s);
    CHECK(RUN(NULL, NULL, src, "export", "-o", out, "--force") == 0);
    CHECK(access(out, F_OK) == 0 && access(part, F_OK) != 0);
    printf("volumes, shared dictionaries, partial files: ok\n");
}

/* ------------------------------------------------------------------ syncing a new database */

/* Runs the CLI under the crashfs shim (tests/jepsen/crashfs.c, beside the CLI) with the CRASHFS_* settings in env
 * (name, value, ..., NULL). */
static int run_crashfs(const char *const *env, const char *const *args)
{
    char shim[PATH_MAX + 16], abs[PATH_MAX], self[PATH_MAX];
    const char *slash = strrchr(cli, '/');
    snprintf(shim, sizeof shim, "%.*s/libcrashfs.so", slash ? (int)(slash - cli) : 1, slash ? cli : ".");
    ssize_t n = readlink("/proc/self/exe", self, sizeof self - 1);
    self[n > 0 ? n : 0] = 0;
    if (!realpath(shim, abs) && strrchr(self, '/')) { /* a TSDB_CLI elsewhere: the shim beside this test */
        snprintf(shim, sizeof shim, "%.*s/libcrashfs.so", (int)(strrchr(self, '/') - self), self);
    }
    if (!realpath(shim, abs)) {
        fprintf(stderr, "  no crashfs shim at %s (make %s)\n", shim, shim);
        return -1;
    }
    const char *old = getenv("ASAN_OPTIONS");
    char *saved = old ? strdup(old) : NULL, asan[1024];
    snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0", saved ? saved : "");
    setenv("ASAN_OPTIONS", asan, 1);
    setenv("LD_PRELOAD", abs, 1);
    for (int i = 0; env[i]; i += 2)
        setenv(env[i], env[i + 1], 1);
    int rc = run(NULL, NULL, NULL, args);
    unsetenv("LD_PRELOAD");
    for (int i = 0; env[i]; i += 2)
        unsetenv(env[i]);
    if (saved)
        setenv("ASAN_OPTIONS", saved, 1);
    else
        unsetenv("ASAN_OPTIONS");
    free(saved);
    return rc;
}

static bool err_lacks(const char *what)
{
    char *e = slurp(errfile, NULL);
    bool lacks = !e || !strstr(e, what);
    if (!lacks)
        fprintf(stderr, "  (stderr mentions \"%s\": %s)\n", what, e);
    free(e);
    return lacks;
}

/* The crafted stream's rows, read back after a power loss: every series whole, f in the volume. Opened as the
 * writer, which recovers: MANIFEST, rewritten in place for followers, may be torn, and a read-only open waits for
 * a writer to publish it again. */
static void crafted_whole(const char *dir, const char *vol)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    tsdb *d;
    int rc = tsdb_open(dir, &o, &d);
    if (rc)
        fprintf(stderr, "  %s after the power loss: %s\n", dir, tsdb_strerror(rc));
    CHECK(rc == 0);
    if (rc)
        return;
    CHECK(rows_of(d, "f") == 5 && rows_of(d, "b") == 3 && rows_of(d, "s") == 3 && rows_of(d, "i") == 3);
    CHECK(same_str(tsdb_series_path(d, lookup(d, "f")), vol));
    tsdb_close(d);
}

/* An import into a new database writes unsynced and syncs every file system once at the end; it says it is done
 * only after that sync, and a failed sync fails the import. An existing database keeps every sync. */
static void test_sync(void)
{
    char *f = path("sync.dump"), *top = path("crash"), map[2 * PATH_MAX + 16];
    craft(f, &(knobs){.vol = "/old/mnt/vol"});
    sh("mkdir -p '%s'", top);
    /* power lost the moment import exits 0: all of it is on disk, the volume it created included */
    const char *loss[] = {"CRASHFS_ROOT", top, "CRASHFS_AT_EXIT", "1", NULL};
    char *db = path("crash/new"), *vol = path("crash/newvol");
    snprintf(map, sizeof map, "/old/mnt/vol=%s", vol);
    CHECK(run_crashfs(loss, (const char *const[]){cli, db, "import", f, "--volume", map, "--create-volumes", NULL}) ==
          0);
    CHECK(err_has("imported 4 series") && err_has("unsynced data lost"));
    crafted_whole(db, vol);
    /* the same into an existing database, synced by the engine as it goes */
    db = path("crash/existing"), vol = path("crash/existingvol");
    tsdb_close(open_db(db, false));
    snprintf(map, sizeof map, "/old/mnt/vol=%s", vol);
    CHECK(run_crashfs(loss, (const char *const[]){cli, db, "import", f, "--volume", map, "--create-volumes", NULL}) ==
          0);
    CHECK(err_has("imported 4 series") && err_has("unsynced data lost"));
    crafted_whole(db, vol);
    /* the final sync fails (EIO) on the database directory, then on a volume: the import fails and says so */
    const char *eio[] = {"CRASHFS_ROOT", top, "CRASHFS_MODE", "eio", "CRASHFS_PROB", "1", "CRASHFS_EIO_ONLY",
                         "syncfail", "CRASHFS_EIO_ON", "syncfs", NULL};
    for (int k = 0; k < 2; k++) {
        db = path(k ? "crash/new2" : "crash/syncfail-db");
        vol = path(k ? "crash/syncfail-vol" : "crash/new2vol");
        snprintf(map, sizeof map, "/old/mnt/vol=%s", vol);
        CHECK(run_crashfs(eio, (const char *const[]){cli, db, "import", f, "--volume", map, "--create-volumes",
                                                     NULL}) == 1);
        CHECK(err_has("crashfs: EIO on syncfs") && err_has("Input/output error") &&
              err_has("not on disk: delete it and import again") && err_lacks("imported "));
    }
    printf("new databases synced at the end: ok\n");
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
    test_crafted();
    test_volumes();
    test_sync();
    test_golden(golden);
    test_concurrent();
    test_memory();
    if (!failures)
        sh("rm -rf '%s'", base);
    printf("%s (%d failures)\n", failures ? "FAIL" : "all export tests passed", failures);
    return failures ? 1 : 0;
}
