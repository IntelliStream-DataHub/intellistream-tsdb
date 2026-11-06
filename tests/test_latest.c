/* tsdb_latest (docs/design-latest.md, step 1: the slow path alone).
 *
 * The oracle is a full scan: the answer for a series and an end bound is the last non-null row (with_nulls: the last
 * row) of tsdb_query_values over everything, ascending, below the bound, bit for bit. It is compared after every
 * step of random sequences of small and large inserts (duplicate timestamps, nulls, NaN payloads, int64 beyond 2^53,
 * timestamp 0 and negative ones), deletes, replaces, retention with a test clock, drop_before, class switches,
 * flushes, merges, optimize, reopen and a crash (fork, kill), in several option sets (many small granules, many
 * unmerged parts, no WAL, partition classes on and off), with ids above 2^32 in two volumes, never-written series,
 * series that hold only nulls, and random end bounds. Directed cases with expectations written by hand come first
 * (a null over the newest value, a null at a newer timestamp, a delete of the newest row, retention, missing ids);
 * a concurrency case (writers, mergers, lookups; the thread sanitizer runs it) follows.
 *
 * `test_latest DIR bench` measures the slow path at 10, 100 and 1,000 partitions (docs/design-latest.md, U4). */
#define _GNU_SOURCE
#include "tsdb.h"

#include <ftw.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            exit(1);                                                                 \
        }                                                                            \
    } while (0)
#define OK(expr)                                                                                          \
    do {                                                                                                  \
        int rc_ = (expr);                                                                                 \
        if (rc_ != 0) {                                                                                   \
            fprintf(stderr, "%s:%d: %s = %d (%s)\n", __FILE__, __LINE__, #expr, rc_, tsdb_strerror(rc_)); \
            exit(1);                                                                                      \
        }                                                                                                 \
    } while (0)

int tsdb_test_class_switch(tsdb *t, tsdb_sid series, int cls, const int64_t *at); /* internal test hook */

static char root[600];

static int rm_cb(const char *p, const struct stat *st, int flag, struct FTW *f)
{
    (void)st, (void)flag, (void)f;
    return remove(p);
}
static void rmrf(const char *dir) { nftw(dir, rm_cb, 16, FTW_DEPTH | FTW_PHYS); }

static int64_t clock_now;
static int64_t test_clock(void *arg)
{
    (void)arg;
    return __atomic_load_n(&clock_now, __ATOMIC_SEQ_CST);
}

static uint64_t rs = 0x9e3779b97f4a7c15ull;
static uint64_t rnd(void)
{
    rs ^= rs << 13;
    rs ^= rs >> 7;
    rs ^= rs << 17;
    return rs;
}

#define HOUR 3600000LL
#define DAY (24 * HOUR)

/* ------------------------------------------------------------------ rows and the oracle */

typedef struct rowset {
    int64_t *ts;
    uint64_t *x; /* slot bits */
    uint8_t *nul;
    size_t n, cap;
} rowset;

static void rs_free(rowset *r)
{
    free(r->ts);
    free(r->x);
    free(r->nul);
    memset(r, 0, sizeof *r);
}

static uint64_t slot_of(tsdb_type t, const tsdb_values *c, size_t i)
{
    switch (t) {
    case TSDB_FLOAT32: { uint32_t b; memcpy(&b, (const float *)c->values + i, 4); return b; }
    case TSDB_FLOAT64: { uint64_t b; memcpy(&b, (const double *)c->values + i, 8); return b; }
    case TSDB_INT64: return (uint64_t)((const int64_t *)c->values)[i];
    case TSDB_BOOL: return ((const uint8_t *)c->values)[i];
    default: return ((const uint32_t *)c->values)[i];
    }
}

static int collect_cb(void *ctx, const int64_t *ts, const tsdb_values *v, size_t n)
{
    rowset *r = ctx;
    if (r->n + n > r->cap) {
        r->cap = (r->n + n) * 2;
        r->ts = realloc(r->ts, r->cap * sizeof *r->ts);
        r->x = realloc(r->x, r->cap * sizeof *r->x);
        r->nul = realloc(r->nul, r->cap);
        CHECK(r->ts && r->x && r->nul);
    }
    for (size_t i = 0; i < n; i++) {
        r->ts[r->n + i] = ts[i];
        bool known = !v->valid || (v->valid[i >> 3] >> (i & 7) & 1);
        r->nul[r->n + i] = !known;
        r->x[r->n + i] = known ? slot_of(v->type, v, i) : 0;
    }
    r->n += n;
    return 0;
}

static void scan(tsdb *db, tsdb_sid id, rowset *r)
{
    r->n = 0;
    OK(tsdb_query_values(db, id, TSDB_TS_MIN, TSDB_TS_MAX, NULL, collect_cb, r));
}

typedef struct expect {
    int kind;
    int64_t ts;
    uint64_t x;
} expect;

static expect oracle(const rowset *r, int64_t end, bool with_nulls)
{
    for (size_t i = r->n; i-- > 0;) {
        if (r->ts[i] >= end)
            continue;
        if (r->nul[i] && !with_nulls)
            continue;
        return (expect){r->nul[i] ? TSDB_LATEST_NULL : TSDB_LATEST_VALUE, r->ts[i], r->x[i]};
    }
    return (expect){TSDB_LATEST_NONE, 0, 0};
}

static uint64_t result_bits(const tsdb_latest_result *o)
{
    switch (o->type) {
    case TSDB_FLOAT32: { uint32_t b; memcpy(&b, &o->v.f32, 4); return b; }
    case TSDB_FLOAT64: { uint64_t b; memcpy(&b, &o->v.f64, 8); return b; }
    case TSDB_INT64: return (uint64_t)o->v.i64;
    case TSDB_BOOL: return o->v.b;
    default: return o->v.state;
    }
}

static void same(const tsdb_latest_result *o, const expect *e, tsdb_sid id, const char *what, int step)
{
    bool ok = o->status == 0 && o->kind == e->kind;
    if (ok && e->kind != TSDB_LATEST_NONE)
        ok = o->ts == e->ts;
    if (ok && e->kind == TSDB_LATEST_VALUE)
        ok = result_bits(o) == e->x;
    if (ok && e->kind == TSDB_LATEST_NONE)
        ok = o->ts == 0 && result_bits(o) == 0;
    if (!ok) {
        fprintf(stderr, "step %d (%s): series %llx: status %d kind %d ts %lld bits %llx; want kind %d ts %lld bits %llx\n",
                step, what, (unsigned long long)id, o->status, o->kind, (long long)o->ts,
                (unsigned long long)result_bits(o), e->kind, (long long)e->ts, (unsigned long long)e->x);
        exit(1);
    }
}

/* ------------------------------------------------------------------ the world */

#define MAXS 24
typedef struct world {
    tsdb *db;
    tsdb_options opt;
    char dir[640], vol2[640];
    tsdb_sid id[MAXS];
    tsdb_type type[MAXS];
    uint32_t code[MAXS][5]; /* state codes */
    size_t ns;
    tsdb_sid never;         /* created, never written */
    tsdb_sid onlynull;      /* only nulls written */
    int step;
    bool classes;
} world;

static void make_series(world *w)
{
    static const tsdb_type tys[] = {TSDB_FLOAT32, TSDB_FLOAT64, TSDB_INT64, TSDB_BOOL, TSDB_STATE};
    char name[64];
    w->ns = 0;
    for (int i = 0; i < 12; i++) {
        tsdb_type t = tys[i % 5];
        snprintf(name, sizeof name, "latest.s%d", i);
        OK(tsdb_series_create(w->db, name, t, i % 3 == 2 ? w->vol2 : NULL, &w->id[w->ns]));
        w->type[w->ns] = t;
        if (t == TSDB_STATE)
            for (int c = 0; c < 5; c++) {
                char sn[16];
                snprintf(sn, sizeof sn, "st%d", c);
                OK(tsdb_state(w->db, w->id[w->ns], sn, &w->code[w->ns][c]));
            }
        w->ns++;
    }
    OK(tsdb_series_create(w->db, "latest.never", TSDB_FLOAT64, NULL, &w->never));
    OK(tsdb_series_create(w->db, "latest.onlynull", TSDB_INT64, w->vol2, &w->onlynull));
}

static void reopen(world *w)
{
    OK(tsdb_close(w->db));
    OK(tsdb_open(w->dir, &w->opt, &w->db));
}

/* a value generator: slot bits of type t (never the null) and the column to hand to the insert */
typedef struct batch {
    int64_t *ts;
    uint8_t *v; /* 8 bytes per row */
    uint8_t *valid;
    size_t n;
    tsdb_type t;
} batch;

static void batch_alloc(batch *b, size_t n, tsdb_type t)
{
    b->ts = calloc(n, sizeof *b->ts);
    b->v = calloc(n, 8);
    b->valid = calloc((n + 7) / 8, 1);
    b->n = n;
    b->t = t;
    CHECK(b->ts && b->v && b->valid);
}
static void batch_free(batch *b)
{
    free(b->ts);
    free(b->v);
    free(b->valid);
}

static void put_value(world *w, size_t si, batch *b, size_t i, int nullp)
{
    tsdb_type t = b->t;
    uint64_t r = rnd();
    bool known = (int)(r % 100) >= nullp;
    r = rnd();
    if (known)
        b->valid[i >> 3] |= (uint8_t)(1u << (i & 7));
    switch (t) {
    case TSDB_FLOAT32: {
        static const uint32_t sp[] = {0x00000000u, 0x80000000u, 0x7fc00001u, 0x7f800000u, 0xff800000u, 0x3fc00000u,
                                      0x40200000u};
        uint32_t bits;
        if (r % 4 == 0)
            bits = sp[(r >> 8) % 7];
        else {
            float f = (float)((int64_t)(r >> 8) % 100000) / 10.0f;
            memcpy(&bits, &f, 4);
        }
        memcpy(b->v + 4 * i, &bits, 4);
        break;
    }
    case TSDB_FLOAT64: {
        static const uint64_t sp[] = {0, 0x8000000000000000ull, 0x7ff8000000000001ull, 0x7ff0000000000000ull,
                                      0x7e37e43c8800759cull};
        uint64_t bits;
        if (r % 4 == 0)
            bits = sp[(r >> 8) % 5];
        else {
            double f = (double)((int64_t)(r >> 8) % 100000) / 100.0;
            memcpy(&bits, &f, 8);
        }
        memcpy(b->v + 8 * i, &bits, 8);
        break;
    }
    case TSDB_INT64: {
        static const int64_t sp[] = {0, 1, -1, INT64_MIN + 1, INT64_MAX, (1ll << 53) + 1, -(1ll << 53) - 1};
        int64_t x = r % 3 == 0 ? sp[(r >> 8) % 7] : (int64_t)(r >> 8) % 1000;
        memcpy(b->v + 8 * i, &x, 8);
        break;
    }
    case TSDB_BOOL: b->v[i] = (uint8_t)((r >> 8) & 1); break;
    default: {
        uint32_t c = w->code[si][(r >> 8) % 5];
        memcpy(b->v + 4 * i, &c, 4);
        break;
    }
    }
}

static int64_t rnd_ts(const world *w)
{
    uint64_t r = rnd();
    int64_t t;
    switch (r % 12) {
    case 0: t = 0; break;
    case 1: t = -(int64_t)((r >> 8) % 48) * HOUR; break;
    case 2: t = (int64_t)((r >> 8) % 400) * DAY; break;
    case 3: t = (int64_t)((r >> 8) % 20) * 28 * DAY + (int64_t)((r >> 20) % 5) * HOUR; break;
    default: t = (int64_t)((r >> 8) % (24 * 14)) * HOUR; break;
    }
    if ((r >> 40) % 3 == 0)
        t += (int64_t)((r >> 44) % 3) * 60000;
    (void)w;
    return t;
}

static void do_insert(world *w, size_t si, bool large, int nullp)
{
    uint64_t r = rnd();
    size_t n = large ? 70 + r % 200 : 1 + r % 12;
    batch b;
    batch_alloc(&b, n, w->type[si]);
    bool dense = r % 3 == 0;
    int64_t start = rnd_ts(w);
    for (size_t i = 0; i < n; i++) {
        b.ts[i] = dense ? start + (int64_t)i * 1000 : rnd_ts(w);
        put_value(w, si, &b, i, nullp);
    }
    tsdb_values vals = {b.t, b.v, b.valid};
    OK(tsdb_insert_series_values(w->db, w->id[si], b.ts, &vals, n));
    batch_free(&b);
}

static void do_replace(world *w, size_t si, int nullp)
{
    int64_t a = rnd_ts(w), len = (int64_t)(rnd() % 30) * HOUR;
    size_t n = rnd() % 8;
    batch b;
    batch_alloc(&b, n + 1, w->type[si]);
    int64_t step = len > 0 ? len / (int64_t)(n + 1) + 1 : 1;
    for (size_t i = 0; i < n; i++) { /* sorted, distinct, inside [a, a + len] */
        b.ts[i] = a + (int64_t)i * step;
        put_value(w, si, &b, i, nullp);
    }
    tsdb_values vals = {b.t, b.v, b.valid};
    int rc = tsdb_replace_range(w->db, w->id[si], a, a + len + 1 + (int64_t)n * step, b.ts, &vals, n);
    CHECK(rc == 0 || rc == TSDB_ERR_INVALID);
    batch_free(&b);
}

static void do_delete(world *w, size_t si)
{
    uint64_t r = rnd();
    int64_t a = r % 5 == 0 ? TSDB_TS_MIN : rnd_ts(w);
    int64_t b = r % 7 == 0 ? TSDB_TS_MAX : a + (int64_t)((r >> 8) % 60) * HOUR;
    /* often delete the newest rows: start just below the newest timestamp */
    if (r % 3 == 0) {
        rowset rr = {0};
        scan(w->db, w->id[si], &rr);
        if (rr.n) {
            size_t k = rr.n - 1 - (r >> 16) % (rr.n < 4 ? rr.n : 4);
            a = rr.ts[k];
            b = r % 2 ? TSDB_TS_MAX : rr.ts[rr.n - 1] + 1;
        }
        rs_free(&rr);
    }
    OK(tsdb_delete_range(w->db, w->id[si], a, b));
}

static void do_null_over_newest(world *w, size_t si)
{
    /* a null at the timestamp of the newest row, or of a non-null one near it: the user's rule */
    rowset rr = {0};
    scan(w->db, w->id[si], &rr);
    if (rr.n) {
        size_t k = rr.n - 1 - rnd() % (rr.n < 5 ? rr.n : 5);
        batch b;
        batch_alloc(&b, 1, w->type[si]);
        b.ts[0] = rr.ts[k];
        put_value(w, si, &b, 0, 100);
        tsdb_values vals = {b.t, b.v, b.valid};
        OK(tsdb_insert_series_values(w->db, w->id[si], b.ts, &vals, 1));
        batch_free(&b);
    }
    rs_free(&rr);
}

static void check_series(world *w, tsdb_sid id, const char *what)
{
    rowset rr = {0};
    scan(w->db, id, &rr);
    tsdb_latest_result o;
    expect e = oracle(&rr, TSDB_TS_MAX, false);
    OK(tsdb_latest(w->db, &id, 1, NULL, &o));
    same(&o, &e, id, what, w->step);
    tsdb_latest_opts op = {.end = TSDB_TS_MAX, .with_nulls = 1};
    e = oracle(&rr, TSDB_TS_MAX, true);
    OK(tsdb_latest(w->db, &id, 1, &op, &o));
    same(&o, &e, id, "with nulls", w->step);
    for (int k = 0; k < 4; k++) {
        int64_t end;
        switch (rnd() % 5) {
        case 0: end = rr.n ? rr.ts[rnd() % rr.n] : 0; break;
        case 1: end = rr.n ? rr.ts[rnd() % rr.n] + 1 : 1; break;
        case 2: end = rnd_ts(w); break;
        case 3: end = TSDB_TS_MIN; break;
        default: end = rr.n ? rr.ts[rr.n - 1] : 5; break;
        }
        bool wn = rnd() & 1;
        tsdb_latest_opts eo = {.end = end, .with_nulls = wn};
        e = oracle(&rr, end, wn);
        OK(tsdb_latest(w->db, &id, 1, &eo, &o));
        same(&o, &e, id, "end bound", w->step);
    }
    rs_free(&rr);
}

/* every series in one call (several threads, a per-series end, with ids that do not exist among them) */
static void check_all(world *w)
{
    tsdb_sid ids[MAXS + 8];
    size_t n = 0;
    for (size_t i = 0; i < w->ns; i++)
        ids[n++] = w->id[i];
    ids[n++] = w->never;
    ids[n++] = w->onlynull;
    size_t real = n;
    ids[n++] = TSDB_SID_NONE;
    ids[n++] = TSDB_SID(0, 100000);
    ids[n++] = TSDB_SID(9, 0);
    ids[n++] = ((tsdb_sid)1 << 50) | w->id[0];
    rowset *rr = calloc(real, sizeof *rr);
    for (size_t i = 0; i < real; i++)
        scan(w->db, ids[i], &rr[i]);
    for (int variant = 0; variant < 4; variant++) {
        int64_t ends[MAXS + 8];
        for (size_t i = 0; i < n; i++)
            ends[i] = i < real && rr[i].n ? rr[i].ts[rnd() % rr[i].n] + (int64_t)(rnd() % 3) - 1 : (int64_t)rnd_ts(w);
        bool wn = variant & 1;
        tsdb_latest_opts op = {.end = TSDB_TS_MAX, .ends = (variant & 2) ? ends : NULL, .with_nulls = wn,
                               .threads = variant == 3 ? 4 : 1};
        tsdb_latest_result out[MAXS + 8];
        memset(out, 0x5a, sizeof out);
        OK(tsdb_latest(w->db, ids, n, &op, out));
        for (size_t i = 0; i < real; i++) {
            expect e = oracle(&rr[i], (variant & 2) ? ends[i] : TSDB_TS_MAX, wn);
            same(&out[i], &e, ids[i], "all", w->step);
        }
        for (size_t i = real; i < n; i++) {
            CHECK(out[i].status == TSDB_ERR_NOTFOUND);
            CHECK(out[i].kind == TSDB_LATEST_NONE && out[i].type == 0 && out[i].ts == 0 && result_bits(&out[i]) == 0);
        }
    }
    for (size_t i = 0; i < real; i++)
        rs_free(&rr[i]);
    free(rr);
}

static void check_world(world *w)
{
    for (size_t i = 0; i < w->ns; i++)
        check_series(w, w->id[i], "series");
    check_series(w, w->never, "never");
    check_series(w, w->onlynull, "onlynull");
    check_all(w);
}

static void world_open(world *w, const char *name, tsdb_options opt, bool classes)
{
    memset(w, 0, sizeof *w);
    snprintf(w->dir, sizeof w->dir, "%s/%s", root, name);
    snprintf(w->vol2, sizeof w->vol2, "%s/%s_v2", root, name);
    rmrf(w->dir);
    rmrf(w->vol2);
    opt.clock_ms = test_clock;
    opt.create_volumes = 1;
    w->opt = opt;
    w->classes = classes;
    OK(tsdb_open(w->dir, &w->opt, &w->db));
    uint32_t vi;
    OK(tsdb_volume_add(w->db, w->vol2, &vi));
    make_series(w);
}

static void random_run(const char *name, tsdb_options opt, bool classes, int steps, uint64_t seed)
{
    world w;
    rs = seed;
    clock_now = 20 * DAY;
    world_open(&w, name, opt, classes);
    /* only nulls into onlynull */
    {
        batch b;
        batch_alloc(&b, 20, TSDB_INT64);
        for (size_t i = 0; i < 20; i++)
            b.ts[i] = (int64_t)i * HOUR; /* valid bits stay clear: nulls */
        tsdb_values vals = {TSDB_INT64, b.v, b.valid};
        OK(tsdb_insert_series_values(w.db, w.onlynull, b.ts, &vals, 20));
        batch_free(&b);
    }
    for (w.step = 0; w.step < steps; w.step++) {
        size_t si = rnd() % w.ns;
        uint64_t r = rnd() % 100;
        int nullp = (int)(rnd() % 4) * 15; /* 0, 15, 30, 45 */
        if (r < 30)
            do_insert(&w, si, false, nullp);
        else if (r < 45)
            do_insert(&w, si, true, nullp);
        else if (r < 53)
            do_null_over_newest(&w, si);
        else if (r < 60)
            do_delete(&w, si);
        else if (r < 65)
            do_replace(&w, si, nullp);
        else if (r < 70) {
            clock_now += (int64_t)(rnd() % 3) * DAY;
            if (rnd() % 8 == 0)
                clock_now -= DAY; /* the clock steps back: retention's now never does */
            int64_t lim = rnd() % 3 == 0 ? 0 : (int64_t)(5 + rnd() % 25) * DAY;
            OK(tsdb_set_retention(w.db, w.id[si], lim));
        } else if (r < 73)
            OK(tsdb_flush(w.db));
        else if (r < 76)
            OK(tsdb_commit(w.db));
        else if (r < 80)
            OK(tsdb_optimize(w.db));
        else if (r < 82 && w.classes)
            (void)tsdb_test_class_switch(w.db, w.id[si], (int)(rnd() % 4), NULL);
        else if (r < 83 && rnd() % 3 == 0)
            OK(tsdb_drop_before(w.db, (int64_t)(rnd() % 30) * DAY - 5 * DAY));
        else if (r < 86)
            reopen(&w);
        else
            do_insert(&w, si, rnd() % 4 == 0, nullp);
        if (w.step % 3 == 0 || r >= 86)
            check_world(&w);
        else
            check_series(&w, w.id[si], "step");
    }
    check_world(&w);
    OK(tsdb_close(w.db));
}

/* ------------------------------------------------------------------ directed cases */

static tsdb *open_plain(const char *name, tsdb_options *o)
{
    char dir[700];
    snprintf(dir, sizeof dir, "%s/%s", root, name);
    rmrf(dir);
    tsdb *db;
    OK(tsdb_open(dir, o, &db));
    return db;
}

static tsdb_latest_result one(tsdb *db, tsdb_sid id, int64_t end, int with_nulls)
{
    tsdb_latest_opts op = {.end = end, .with_nulls = with_nulls};
    tsdb_latest_result o;
    OK(tsdb_latest(db, &id, 1, &op, &o));
    CHECK(o.status == 0);
    return o;
}

static void put_f64(tsdb *db, tsdb_sid id, int64_t ts, double v, bool null)
{
    uint8_t valid = null ? 0 : 1;
    tsdb_values vals = {TSDB_FLOAT64, &v, &valid};
    OK(tsdb_insert_series_values(db, id, &ts, &vals, 1));
}

static void test_directed(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.clock_ms = test_clock;
    o.wal_max_batch_rows = 4;
    clock_now = 100 * DAY;
    tsdb *db = open_plain("latest_directed", &o);
    tsdb_sid s, empty, nulls;
    OK(tsdb_series_create(db, "d.s", TSDB_FLOAT64, NULL, &s));
    OK(tsdb_series_create(db, "d.empty", TSDB_FLOAT64, NULL, &empty));
    OK(tsdb_series_create(db, "d.nulls", TSDB_FLOAT64, NULL, &nulls));
    CHECK(s >> 32 && s < ((tsdb_sid)1 << 48));
    tsdb_latest_result r;

    r = one(db, empty, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_NONE && r.type == TSDB_FLOAT64 && r.ts == 0 && r.v.i64 == 0);
    put_f64(db, nulls, 10, 0, true);
    put_f64(db, nulls, 20, 0, true);
    r = one(db, nulls, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_NONE);
    r = one(db, nulls, TSDB_TS_MAX, 1);
    CHECK(r.kind == TSDB_LATEST_NULL && r.ts == 20);

    /* memtable rows */
    put_f64(db, s, 100, 1.0, false);
    put_f64(db, s, 200, 2.0, false);
    r = one(db, s, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 200 && r.v.f64 == 2.0);
    /* a null at a newer timestamp does not matter ... */
    put_f64(db, s, 300, 0, true);
    r = one(db, s, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 200 && r.v.f64 == 2.0);
    r = one(db, s, TSDB_TS_MAX, 1);
    CHECK(r.kind == TSDB_LATEST_NULL && r.ts == 300);
    /* ... a null at the newest value's timestamp hides it, in the memtable and over a part's row */
    put_f64(db, s, 200, 0, true);
    r = one(db, s, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 100 && r.v.f64 == 1.0);
    OK(tsdb_flush(db)); /* now the rows are in a part */
    r = one(db, s, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 100 && r.v.f64 == 1.0);
    put_f64(db, s, 100, 0, true); /* a null in the memtable over the part's value at 100 */
    r = one(db, s, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_NONE);
    put_f64(db, s, 100, 7.5, false); /* a value again at the nulled timestamp */
    r = one(db, s, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 100 && r.v.f64 == 7.5);
    /* end bounds are exclusive */
    r = one(db, s, 100, 0);
    CHECK(r.kind == TSDB_LATEST_NONE);
    r = one(db, s, 101, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 100);
    r = one(db, s, 300, 1);
    CHECK(r.kind == TSDB_LATEST_NULL && r.ts == 200);
    r = one(db, s, 301, 1);
    CHECK(r.kind == TSDB_LATEST_NULL && r.ts == 300);

    /* a delete of the newest rows */
    put_f64(db, s, 400, 4.0, false);
    put_f64(db, s, 500, 5.0, false);
    OK(tsdb_flush(db));
    r = one(db, s, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 500);
    OK(tsdb_delete_range(db, s, 450, 600));
    r = one(db, s, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 400 && r.v.f64 == 4.0);
    OK(tsdb_delete_range(db, s, 400, 401));
    r = one(db, s, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 100 && r.v.f64 == 7.5);
    OK(tsdb_delete_range(db, s, TSDB_TS_MIN, TSDB_TS_MAX));
    r = one(db, s, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_NONE);
    r = one(db, s, TSDB_TS_MAX, 1);
    CHECK(r.kind == TSDB_LATEST_NONE);

    /* retention: expired rows do not count, at once */
    put_f64(db, s, 90 * DAY, 9.0, false);
    put_f64(db, s, 95 * DAY, 9.5, false);
    OK(tsdb_flush(db));
    r = one(db, s, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 95 * DAY);
    OK(tsdb_set_retention(db, s, 3 * DAY)); /* now 100 d: keeps >= 97 d */
    r = one(db, s, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_NONE);
    clock_now = 99 * DAY;
    OK(tsdb_set_retention(db, s, 5 * DAY)); /* the clock does not go back: still 100 d - 5 d = 95 d */
    r = one(db, s, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 95 * DAY);
    clock_now = 100 * DAY;

    /* int64 beyond 2^53 and the bits of a NaN payload come back exactly */
    tsdb_sid si, sf;
    OK(tsdb_series_create(db, "d.i", TSDB_INT64, NULL, &si));
    OK(tsdb_series_create(db, "d.f", TSDB_FLOAT32, NULL, &sf));
    int64_t big = (1ll << 53) + 1, t1 = 5;
    tsdb_values vi = {TSDB_INT64, &big, NULL};
    OK(tsdb_insert_series_values(db, si, &t1, &vi, 1));
    uint32_t nanb = 0x7fc00001u;
    float nanf;
    memcpy(&nanf, &nanb, 4);
    tsdb_values vf = {TSDB_FLOAT32, &nanf, NULL};
    OK(tsdb_insert_series_values(db, sf, &t1, &vf, 1));
    for (int pass = 0; pass < 2; pass++) {
        r = one(db, si, TSDB_TS_MAX, 0);
        CHECK(r.kind == TSDB_LATEST_VALUE && r.v.i64 == big && r.type == TSDB_INT64);
        r = one(db, sf, TSDB_TS_MAX, 0);
        uint32_t got;
        memcpy(&got, &r.v.f32, 4);
        CHECK(r.kind == TSDB_LATEST_VALUE && got == nanb);
        OK(tsdb_flush(db));
    }

    /* ids: missing ones are NOTFOUND per record; n out of range */
    tsdb_sid ids[3] = {s, TSDB_SID_NONE, TSDB_SID(5, 1)};
    tsdb_latest_result out[3];
    OK(tsdb_latest(db, ids, 3, NULL, out));
    CHECK(out[0].status == 0 && out[1].status == TSDB_ERR_NOTFOUND && out[2].status == TSDB_ERR_NOTFOUND);
    CHECK(out[1].kind == TSDB_LATEST_NONE && out[2].kind == TSDB_LATEST_NONE);
    CHECK(tsdb_latest(db, ids, ((size_t)1 << 20) + 1, NULL, out) == TSDB_ERR_INVALID);
    OK(tsdb_latest(db, ids, 0, NULL, out));
    OK(tsdb_close(db));
}

/* ------------------------------------------------------------------ crash */

static void test_crash(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.clock_ms = test_clock;
    o.wal_max_batch_rows = 50;
    o.buffer_rows = 500;
    clock_now = 20 * DAY;
    char dir[700];
    snprintf(dir, sizeof dir, "%s/latest_crash", root);
    rmrf(dir);
    for (int round = 0; round < 4; round++) {
        pid_t pid = fork();
        CHECK(pid >= 0);
        if (pid == 0) {
            tsdb *db;
            OK(tsdb_open(dir, &o, &db));
            tsdb_sid id;
            if (tsdb_series_lookup(db, "c.a", &id) != 0)
                OK(tsdb_series_create(db, "c.a", TSDB_FLOAT64, NULL, &id));
            for (int64_t i = 0;; i++) {
                int64_t ts = (i * 7) % 5000 * 60000;
                double v = (double)i;
                uint8_t valid = i % 5 != 0;
                tsdb_values vals = {TSDB_FLOAT64, &v, &valid};
                (void)tsdb_insert_series_values(db, id, &ts, &vals, 1);
            }
        }
        struct timespec d = {0, (100 + round * 60) * 1000000L};
        nanosleep(&d, NULL);
        kill(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        tsdb *db;
        OK(tsdb_open(dir, &o, &db));
        tsdb_sid id;
        if (tsdb_series_lookup(db, "c.a", &id) == 0) {
            rowset rr = {0};
            scan(db, id, &rr);
            for (int wn = 0; wn < 2; wn++) {
                tsdb_latest_opts op = {.end = TSDB_TS_MAX, .with_nulls = wn};
                tsdb_latest_result out;
                OK(tsdb_latest(db, &id, 1, &op, &out));
                expect e = oracle(&rr, TSDB_TS_MAX, wn);
                same(&out, &e, id, "after a crash", round);
            }
            rs_free(&rr);
        }
        OK(tsdb_close(db));
    }
}

/* ------------------------------------------------------------------ concurrency */

#define CS 6
typedef struct conc {
    tsdb *db;
    tsdb_sid id[CS];
    atomic_bool stop;
    atomic_int_fast64_t written[CS]; /* newest timestamp acknowledged */
} conc;

static double conc_val(int64_t ts) { return (double)(ts * 3 + 1); }

typedef struct cwr {
    conc *c;
    int idx;
} cwr;

static void *conc_writer(void *arg)
{
    cwr *a = arg;
    conc *c = a->c;
    uint64_t seed = 1234567u + (uint64_t)a->idx * 77;
    int64_t t = 1000 * (a->idx + 1);
    for (int i = 0; i < 400; i++) {
        seed = seed * 6364136223846793005ull + 1442695040888963407ull;
        size_t n = (seed >> 33) % 7 == 0 ? 600 : 1 + (seed >> 40) % 4;
        int64_t ts[600];
        double v[600];
        uint8_t valid[75];
        memset(valid, 0xff, sizeof valid);
        for (size_t k = 0; k < n; k++) {
            ts[k] = t++;
            v[k] = conc_val(ts[k]);
        }
        tsdb_values vals = {TSDB_FLOAT64, v, valid};
        for (int s = a->idx; s < CS; s += 2) {
            OK(tsdb_insert_series_values(c->db, c->id[s], ts, &vals, n));
            atomic_store(&c->written[s], ts[n - 1]);
        }
        if (i % 50 == 0)
            OK(tsdb_flush(c->db));
    }
    return NULL;
}

static void *conc_merger(void *arg)
{
    conc *c = arg;
    while (!atomic_load(&c->stop)) {
        OK(tsdb_optimize(c->db));
        struct timespec d = {0, 3000000};
        nanosleep(&d, NULL);
    }
    return NULL;
}

static void *conc_reader(void *arg)
{
    conc *c = arg;
    int64_t last[CS] = {0};
    int64_t before[CS];
    while (!atomic_load(&c->stop)) {
        for (int s = 0; s < CS; s++)
            before[s] = atomic_load(&c->written[s]);
        tsdb_latest_result out[CS];
        tsdb_latest_opts op = {.end = TSDB_TS_MAX, .threads = 3};
        OK(tsdb_latest(c->db, c->id, CS, &op, out));
        for (int s = 0; s < CS; s++) {
            if (before[s] == 0) {
                continue;
            }
            /* a row acknowledged before the call is seen: the answer is at least that new, and a row written */
            CHECK(out[s].status == 0 && out[s].kind == TSDB_LATEST_VALUE);
            CHECK(out[s].ts >= before[s]);
            CHECK(out[s].v.f64 == conc_val(out[s].ts)); /* a row that was written */
            CHECK(out[s].ts >= last[s]);               /* never back in time (no delete here) */
            last[s] = out[s].ts;
        }
    }
    return NULL;
}

static void test_concurrent(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.wal_max_batch_rows = 100;
    o.buffer_rows = 2000;
    o.merge_threads = 2;
    o.granule_rows = 64;
    conc c = {0};
    c.db = open_plain("latest_conc", &o);
    char name[32];
    for (int i = 0; i < CS; i++) {
        snprintf(name, sizeof name, "cc.s%d", i);
        OK(tsdb_series_create(c.db, name, TSDB_FLOAT64, NULL, &c.id[i]));
    }
    pthread_t wt[2], mt, rt[3];
    cwr wa[2] = {{&c, 0}, {&c, 1}};
    for (int i = 0; i < 2; i++)
        CHECK(pthread_create(&wt[i], NULL, conc_writer, &wa[i]) == 0);
    CHECK(pthread_create(&mt, NULL, conc_merger, &c) == 0);
    for (int i = 0; i < 3; i++)
        CHECK(pthread_create(&rt[i], NULL, conc_reader, &c) == 0);
    for (int i = 0; i < 2; i++)
        pthread_join(wt[i], NULL);
    atomic_store(&c.stop, true);
    pthread_join(mt, NULL);
    for (int i = 0; i < 3; i++)
        pthread_join(rt[i], NULL);
    for (int s = 0; s < CS; s++) {
        rowset rr = {0};
        scan(c.db, c.id[s], &rr);
        tsdb_latest_result out;
        OK(tsdb_latest(c.db, &c.id[s], 1, NULL, &out));
        expect e = oracle(&rr, TSDB_TS_MAX, false);
        same(&out, &e, c.id[s], "after the concurrent run", 0);
        CHECK(out.ts == atomic_load(&c.written[s]));
        rs_free(&rr);
    }
    OK(tsdb_close(c.db));
}

/* ------------------------------------------------------------------ the slow path's timings */

static double now_s(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (double)t.tv_sec + (double)t.tv_nsec * 1e-9;
}

#define BS 200 /* series */
static double bench_call(tsdb *db, const tsdb_sid *ids, const tsdb_latest_opts *op, int reps)
{
    tsdb_latest_result out[BS];
    double t0 = now_s();
    for (int i = 0; i < reps; i++)
        OK(tsdb_latest(db, ids, BS, op, out));
    return (now_s() - t0) / reps / BS * 1e6; /* microseconds per id */
}

static void bench_one(int parts, int run, const char *dir_root)
{
    char dir[700];
    snprintf(dir, sizeof dir, "%s/latest_bench", dir_root);
    rmrf(dir);
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.wal_sync_ms = -1;
    o.partition_classes = 0;
    o.clock_ms = test_clock;
    clock_now = (int64_t)(parts + 5) * DAY;
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid ids[BS], expired;
    char name[48];
    for (int i = 0; i < BS; i++) {
        snprintf(name, sizeof name, "b.s%d", i);
        OK(tsdb_series_create(db, name, TSDB_FLOAT32, NULL, &ids[i]));
    }
    OK(tsdb_series_create(db, "b.expired", TSDB_FLOAT32, NULL, &expired));
    tsdb_sid none;
    OK(tsdb_series_create(db, "b.none", TSDB_FLOAT32, NULL, &none));
    enum { PER = 20 };
    int64_t *ts = malloc((size_t)BS * PER * sizeof *ts);
    float *v = malloc((size_t)BS * PER * sizeof *v);
    tsdb_sid *sid = malloc((size_t)BS * PER * sizeof *sid);
    for (int d = 0; d < parts; d++) {
        size_t k = 0;
        for (int i = 0; i < BS; i++)
            for (int j = 0; j < PER; j++, k++) {
                sid[k] = ids[i];
                ts[k] = (int64_t)d * DAY + j * 60000 + i;
                v[k] = (float)(d + j);
            }
        OK(tsdb_insert(db, sid, ts, v, k));
    }
    /* the series that went null for the last day: nulls in the newest partition only */
    {
        int64_t tn[PER];
        float vn[PER];
        uint8_t valid[(PER + 7) / 8] = {0};
        for (int j = 0; j < PER; j++)
            tn[j] = (int64_t)(parts - 1) * DAY + 3600000 + j * 60000, vn[j] = 0;
        tsdb_values vals = {TSDB_FLOAT32, vn, valid};
        for (int i = 0; i < BS / 2; i++)
            OK(tsdb_insert_series_values(db, ids[i], tn, &vals, PER));
    }
    /* a hot unmerged partition: 30 more small parts in the newest day (every insert of a few rows is a part) */
    for (int p = 0; p < 30; p++) {
        size_t k = 0;
        for (int i = 0; i < BS; i++, k++) {
            sid[k] = ids[i];
            ts[k] = (int64_t)(parts - 1) * DAY + 7200000 + p * 1000 + i;
            v[k] = (float)p;
        }
        OK(tsdb_insert(db, sid, ts, v, k));
        OK(tsdb_flush(db));
    }
    /* a series whose rows have all expired */
    {
        int64_t te = 0;
        float ve = 1;
        OK(tsdb_insert_series(db, expired, &te, &ve, 1));
        OK(tsdb_flush(db));
        OK(tsdb_set_retention(db, expired, DAY));
    }
    struct {
        const char *what;
        tsdb_latest_opts op;
        const tsdb_sid *ids;
    } cases[] = {
        {"newest, non-null (the hot partition, 30 parts)", {.end = TSDB_TS_MAX}, ids},
        {"newest with nulls", {.end = TSDB_TS_MAX, .with_nulls = 1}, ids},
        {"as of one day back", {.end = (int64_t)(parts - 1) * DAY}, ids},
        {"as of half way back", {.end = (int64_t)(parts / 2) * DAY}, ids},
        {"as of 10 partitions from the start", {.end = 10 * DAY}, ids},
        {"series with no row", {.end = TSDB_TS_MAX}, &none},
        {"series wholly expired", {.end = TSDB_TS_MAX}, &expired},
    };
    for (size_t c = 0; c < sizeof cases / sizeof cases[0]; c++) {
        tsdb_sid one_id[BS];
        const tsdb_sid *use = cases[c].ids;
        if (use != ids) {
            for (int i = 0; i < BS; i++)
                one_id[i] = use[0];
            use = one_id;
        }
        (void)bench_call(db, use, &cases[c].op, 2); /* warm */
        double us = bench_call(db, use, &cases[c].op, 20);
        printf("BENCH parts=%d run=%d case=\"%s\" us_per_id=%.2f\n", parts, run, cases[c].what, us);
    }
    free(ts);
    free(v);
    free(sid);
    OK(tsdb_close(db));
}

/* ------------------------------------------------------------------ main */

int main(int argc, char **argv)
{
    const char *scratch = argc > 1 ? argv[1] : "/tmp";
    snprintf(root, sizeof root, "%s/tsdb_latest_%d", scratch, (int)getpid());
    mkdir(root, 0755);
    if (argc > 2 && strcmp(argv[2], "bench") == 0) {
        int parts_list[] = {10, 100, 1000};
        for (int i = 0; i < 3; i++)
            for (int run = 0; run < 5; run++)
                bench_one(parts_list[i], run, root);
        rmrf(root);
        return 0;
    }
    int steps = argc > 2 ? atoi(argv[2]) : 260;
    test_directed();
    printf("directed ok\n");

    tsdb_options o;
    tsdb_options_init(&o);
    o.wal_max_batch_rows = 64;
    o.granule_rows = 32;
    o.merge_threads = 2;
    random_run("latest_r0", o, true, steps, 11);
    printf("classes, small granules, merges ok\n");

    tsdb_options_init(&o);
    o.partition_ms = 6 * HOUR;
    o.partition_classes = 0;
    o.wal_max_batch_rows = 64;
    o.granule_rows = 16;
    o.merge_threads = 0;
    random_run("latest_r1", o, false, steps, 22);
    printf("many unmerged parts ok\n");

    tsdb_options_init(&o);
    o.wal_sync_ms = -1;
    o.buffer_rows = 200;
    o.wal_max_batch_rows = 80;
    o.granule_rows = 24;
    o.merge_threads = 1;
    random_run("latest_r2", o, true, steps, 33);
    printf("no WAL ok\n");

    tsdb_options_init(&o);
    o.wal_sync_ms = 5;
    o.buffer_rows = 300;
    o.granule_rows = 64;
    o.wal_max_batch_rows = 90;
    o.merge_threads = 2;
    random_run("latest_r3", o, true, steps, 44);
    printf("periodic WAL ok\n");

    test_crash();
    printf("crash ok\n");
    test_concurrent();
    printf("concurrent ok\n");
    rmrf(root);
    printf("test_latest ok\n");
    return 0;
}
