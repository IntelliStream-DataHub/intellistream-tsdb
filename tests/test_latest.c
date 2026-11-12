/* SPDX-License-Identifier: AGPL-3.0-only */
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
 * a partial merge that leaves a part with rows and its own tombstones, a lookup while a flush holds a flushing memtable
 * under a newer active one (and a follower with two tails), and a follower against its own scan; a concurrency case
 * (writers, mergers, lookups; the thread sanitizer runs it) follows. The random option set "latest_r4" merges idle
 * partitions in pieces (max_part_bytes) and fails if no part with rows and tombstones ever existed.
 *
 * `test_latest DIR bench` measures the slow path at 10, 100 and 1,000 partitions (docs/design-latest.md, U4). */
#define _GNU_SOURCE
#include "internal.h" /* part_footer, tsdb_test_midplace */

#include <dirent.h>
#include <fcntl.h>
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
#include <sys/syscall.h>
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

/* Background merge threads read the clock (retention's now): every write is atomic too. */
static void set_clock(int64_t v) { __atomic_store_n(&clock_now, v, __ATOMIC_SEQ_CST); }
static void step_clock(int64_t d) { set_clock(__atomic_load_n(&clock_now, __ATOMIC_SEQ_CST) + d); }

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

/* Parts in dir that hold both granules and tombstones: what a merge writes that left an older part of the partition
 * out (it keeps the tombstones, which still hide that part's rows and must not hide its own). The state where
 * "a part's tombstones never hide its own rows" matters (a replace's parts merged without the old part below). */
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
    int mixed_steps; /* steps that saw a part with rows and tombstones (partial merges; see mixed_parts) */
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

/* the option set of the partial-merge run: parts idle for a second merge, in pieces of max_part_bytes */
#define PARTIAL(o) ((o).merge_idle_s > 0 && (o).merge_idle_s < 60)

static void random_run(const char *name, tsdb_options opt, bool classes, int steps, uint64_t seed)
{
    world w;
    rs = seed;
    set_clock(20 * DAY);
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
        if (getenv("LATEST_TRACE")) /* which op ran at which step, for a failing seed */
            fprintf(stderr, "step %d series %zu op %d nullp %d\n", w.step, si, (int)r, nullp);
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
            step_clock((int64_t)(rnd() % 3) * DAY);
            if (rnd() % 8 == 0)
                step_clock(-DAY); /* the clock steps back: retention's now never does */
            int64_t lim = rnd() % 3 == 0 ? 0 : (int64_t)(5 + rnd() % 25) * DAY;
            OK(tsdb_set_retention(w.db, w.id[si], lim));
        } else if (r < 73)
            OK(tsdb_flush(w.db));
        else if (r < 76)
            OK(tsdb_commit(w.db));
        else if (r < 80) /* a partial-merge run lets parts pile up: optimize would merge whole partitions */
            OK(PARTIAL(opt) ? tsdb_flush(w.db) : tsdb_optimize(w.db));
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
        if (PARTIAL(opt)) { /* partial merges: give the idle merges time, and see whether they left mixed parts */
            if (w.step % 12 == 11) {
                struct timespec d = {0, 120000000};
                nanosleep(&d, NULL);
            }
            if (mixed_parts(w.dir) + mixed_parts(w.vol2) > 0)
                w.mixed_steps++;
        }
    }
    check_world(&w);
    if (PARTIAL(opt)) {
        printf("  %d of %d steps had a part with rows and tombstones\n", w.mixed_steps, steps);
        CHECK(w.mixed_steps > 0 || steps < 100); /* else the run did not reach the state it is for */
    }
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
    set_clock(100 * DAY);
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
    set_clock(99 * DAY);
    OK(tsdb_set_retention(db, s, 5 * DAY)); /* the clock does not go back: still 100 d - 5 d = 95 d */
    r = one(db, s, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 95 * DAY);
    set_clock(100 * DAY);

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
    set_clock(20 * DAY);
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

static int conc_flush_every = 50;
static atomic_int conc_windows; /* flushes that went through the 3.2 window */
static void conc_hook(void *arg, int at)
{
    (void)arg;
    if (at == LH_FOLD_BEFORE_LIST)
        atomic_fetch_add(&conc_windows, 1);
}

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
        if (i % conc_flush_every == 0)
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
    atomic_store(&conc_windows, 0);
    tsdb_test_latest_hook(c.db, conc_hook, NULL);
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
    tsdb_test_latest_hook(c.db, NULL, NULL);
    CHECK(conc_flush_every == 50 || atomic_load(&conc_windows) > 20); /* the windows ran under the lookups */
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
    set_clock((int64_t)(parts + 5) * DAY);
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

/* ------------------------------------------------------------------ partial merges (M1) */

/* test_delete's partial merge: 100k rows, then a replace of [0, N/2) with 100 rows. A background merge of the
 * replace's parts that leaves the big old part out (max_part_bytes) writes a part with its own rows and the
 * tombstones next to them; those tombstones hide the old part's rows and never the part's own. The newest rows
 * below the small bounds lie in that mixed part. */
/* A retention limit lengthened while a merge runs (design-latest.md 3.5). The merge read the old limit and drops all
 * three rows (they are expired under it, as they would be without the change); the limit then goes from 3 d to 20 d,
 * the warmer fills the entry from the parts (the merge's sources are still listed: the row at 92 d), and the merge
 * lists. The listing marks the entries of its series that name a row in its range (lt_merge_marks): without it the
 * table keeps naming the row at 92 d, which no longer exists. The merge is listed (not discarded and run again:
 * discarding made every lengthening in the volume throw away every merge in flight, see test_merges_not_starved).
 * Regression: the merge thread read the discarded (freed) output afterwards (heap-use-after-free under `make asan`,
 * found by test_latest's random runs); the checks here are exact answers, and under ASan that nothing touches a freed
 * part. */
static void check_vs_scan(tsdb *db, tsdb_sid id, const char *what);
static _Atomic int rl_fired;
static tsdb *rl_db;
static tsdb_sid rl_id;
static void rl_hook(int at)
{
    if (at != 1 || atomic_exchange(&rl_fired, 1))
        return;
    uint64_t s0[6], s1[6];
    CHECK(tsdb_test_latest_stats(rl_db, s0));
    OK(tsdb_set_retention(rl_db, rl_id, 20 * DAY)); /* the cutoff goes from 97 d to 80 d */
    for (int i = 0; i < 500; i++) { /* the warmer fills the entry from the sources (fill_ok) */
        CHECK(tsdb_test_latest_stats(rl_db, s1));
        if (s1[3] > s0[3])
            break;
        usleep(10000);
    }
    CHECK(s1[3] > s0[3]);
}

static void test_retention_lengthened_in_merge(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 1; /* the merge thread read the freed output (tsdb_optimize did not) */
    o.merge_idle_s = 1; /* idle for 1 s: the thread merges the two parts */
    o.clock_ms = test_clock;
    o.partition_ms = 1000 * DAY;
    o.partition_classes = 0;
    set_clock(100 * DAY);
    tsdb *db = open_plain("latest_retlen", &o);
    tsdb_sid s;
    OK(tsdb_series_create(db, "rl.a", TSDB_FLOAT64, NULL, &s));
    OK(tsdb_set_retention(db, s, 3 * DAY)); /* keeps >= 97 d */
    put_f64(db, s, 90 * DAY, 1.0, false);
    put_f64(db, s, 91 * DAY, 2.0, false);
    OK(tsdb_flush(db));
    tsdb_latest_result r = one(db, s, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_NONE); /* all of it has expired */
    rl_db = db;
    rl_id = s;
    atomic_store(&rl_fired, 0);
    atomic_store(&merge_hook, rl_hook);
    put_f64(db, s, 92 * DAY, 3.0, false); /* the second part: the merge thread selects the two */
    OK(tsdb_flush(db));
    tsdb_stats st;
    for (int i = 0; i < 600 && (!atomic_load(&rl_fired) || (tsdb_get_stats(db, &st), st.merges < 1)); i++) {
        struct timespec d = {0, 50000000};
        nanosleep(&d, NULL);
    }
    atomic_store(&merge_hook, NULL);
    CHECK(atomic_load(&rl_fired) == 1);
    tsdb_get_stats(db, &st);
    CHECK(st.merges == 1); /* listed across the change, not discarded */
    check_vs_scan(db, s, "retention lengthened in a merge");
    r = one(db, s, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_NONE); /* the merge dropped them under the limit it read */
    OK(tsdb_close(db));
}

/* Lengthening the limit of OTHER series must not throw away a merge (design-latest.md 3.5 lists across a change). The
 * first version discarded every merge in flight in the volume at every lengthening, so while such calls kept coming
 * tsdb_optimize never finished (starve.c of the review: 8 s and 123 merges, 122 of them discarded, for one that takes
 * 0.07 s). Here a thread lengthens the limits of 64 other series in a loop until optimize returns or 20 s have gone. */
static _Atomic int ns_stop;
static _Atomic long ns_calls;
static tsdb *ns_db;
static tsdb_sid ns_other[64];
static void *ns_lengthen(void *arg)
{
    (void)arg;
    long i = 0;
    while (!atomic_load(&ns_stop)) {
        OK(tsdb_set_retention(ns_db, ns_other[i % 64], 1000 * DAY + i)); /* each call lowers that series' cutoff */
        atomic_store(&ns_calls, ++i);
    }
    return NULL;
}

static void *ns_watchdog(void *arg)
{
    (void)arg;
    for (int i = 0; i < 2000 && !atomic_load(&ns_stop); i++)
        usleep(10000);
    atomic_store(&ns_stop, 1);
    return NULL;
}

static void test_merges_not_starved(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0; /* tsdb_optimize merges */
    o.partition_classes = 0;
    ns_db = open_plain("latest_starve", &o);
    tsdb_sid s;
    OK(tsdb_series_create(ns_db, "ns.big", TSDB_FLOAT64, NULL, &s));
    for (int i = 0; i < 64; i++) {
        char nm[32];
        snprintf(nm, sizeof nm, "ns.o%d", i);
        OK(tsdb_series_create(ns_db, nm, TSDB_FLOAT64, NULL, &ns_other[i]));
        OK(tsdb_set_retention(ns_db, ns_other[i], 500 * DAY)); /* the loop's calls then all lower a cutoff */
    }
    enum { N = 200000, PARTS = 6 };
    int64_t *ts = malloc(N * sizeof *ts);
    double *v = malloc(N * sizeof *v);
    for (int p = 0; p < PARTS; p++) { /* PARTS parts in one partition: the day starts at 0 */
        for (int i = 0; i < N; i++) {
            ts[i] = (int64_t)i * PARTS + p;
            v[i] = i * 0.25 + p;
        }
        tsdb_values vals = {TSDB_FLOAT64, v, NULL};
        OK(tsdb_insert_series_values(ns_db, s, ts, &vals, N));
    }
    free(ts);
    free(v);
    tsdb_stats st0, st1;
    tsdb_get_stats(ns_db, &st0);
    CHECK(st0.parts >= 2);
    atomic_store(&ns_stop, 0);
    atomic_store(&ns_calls, 0);
    pthread_t th, wd;
    CHECK(pthread_create(&th, NULL, ns_lengthen, NULL) == 0);
    while (atomic_load(&ns_calls) < 10)
        usleep(1000);
    CHECK(pthread_create(&wd, NULL, ns_watchdog, NULL) == 0);
    OK(tsdb_optimize(ns_db));
    int timed_out = atomic_load(&ns_stop); /* the watchdog ended the run before optimize did */
    atomic_store(&ns_stop, 1);
    pthread_join(th, NULL);
    pthread_join(wd, NULL);
    CHECK(!timed_out);
    tsdb_get_stats(ns_db, &st1);
    CHECK(st1.parts == 1);
    CHECK(st1.merges - st0.merges <= 2); /* one merge, not one per lengthening */
    check_vs_scan(ns_db, s, "starve");
    OK(tsdb_close(ns_db));
}

/* ------------------------------------------------------------------ directed cases found by a mutation review */

/* An fdatasync of a WAL file that blocks once armed (T4), the way a slow disk would. The tests link libtsdb.a, so this
 * definition is the one the engine's tsdb_fdatasync reaches. */
static atomic_int sy_armed, sy_stalled, sy_release;
int fdatasync(int fd)
{
    if (atomic_load(&sy_armed)) {
        char p[64], buf[512];
        snprintf(p, sizeof p, "/proc/self/fd/%d", fd);
        ssize_t n = readlink(p, buf, sizeof buf - 1);
        if (n > 0) {
            buf[n] = 0;
            if (strstr(buf, "/wal_") && atomic_exchange(&sy_armed, 0)) {
                atomic_store(&sy_stalled, 1);
                while (!atomic_load(&sy_release))
                    usleep(1000);
            }
        }
    }
    return (int)syscall(SYS_fdatasync, fd);
}

/* T1: the active memtable rewrites a timestamp the flushing memtable holds: the active one is newer, so it wins the
 * tie (last write wins). Mutant: the flushing memtable wins ties. */
typedef struct {
    tsdb *db;
    tsdb_sid id;
    bool armed;
    int fired;
} dc1;
static void dc1_hook(void *arg)
{
    dc1 *x = arg;
    if (!x->armed)
        return;
    x->armed = false;
    x->fired++;
    put_f64(x->db, x->id, 30, 2.0, false);
    tsdb_latest_result r = one(x->db, x->id, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 30 && r.v.f64 == 2.0);
}

static void test_active_over_flushing(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    dc1 x = {0};
    x.db = open_plain("latest_dc1", &o);
    OK(tsdb_series_create(x.db, "dc1.a", TSDB_FLOAT64, NULL, &x.id));
    put_f64(x.db, x.id, 30, 8.0, false);
    tsdb_test_midplace(x.db, dc1_hook, &x);
    x.armed = true;
    OK(tsdb_flush(x.db));
    tsdb_test_midplace(x.db, NULL, NULL);
    CHECK(x.fired == 1);
    tsdb_latest_result r = one(x.db, x.id, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 30 && r.v.f64 == 2.0);
    OK(tsdb_close(x.db));
}

/* T2: a known entry below the drop floor whose row drop_before removed, looked up while the warmer holds its floor
 * walk (it has not stored yet): the lookup reads the entry as unknown and searches. Mutant: lookups read it as known. */
static atomic_int dc2_hold;
static void dc2_hook(void *arg, int at)
{
    (void)arg;
    if (at != LH_WARM_AFTER_CUT)
        return;
    while (atomic_load(&dc2_hold))
        usleep(1000);
}

static void test_drop_floor_lookup(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.partition_classes = 0;
    tsdb *db = open_plain("latest_dc2", &o);
    tsdb_sid id;
    OK(tsdb_series_create(db, "dc2.a", TSDB_FLOAT64, NULL, &id));
    put_f64(db, id, DAY + 5, 1.0, false);
    OK(tsdb_flush(db));
    tsdb_latest_result r = one(db, id, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == DAY + 5);
    atomic_store(&dc2_hold, 1);
    tsdb_test_latest_hook(db, dc2_hook, NULL);
    OK(tsdb_drop_before(db, 2 * DAY));
    r = one(db, id, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_NONE);
    atomic_store(&dc2_hold, 0);
    tsdb_test_latest_hook(db, NULL, NULL);
    OK(tsdb_close(db));
}

/* T3: a merge dropped the expired newest rows, then the limit is lengthened. The newest row then in the table (20 d + 1 h)
 * no longer exists: lt_retention_lowered must make the entry unknown, or the warmer (which never revisits known
 * entries) leaves it. Mutant: lt_retention_lowered does nothing. */
static void test_lengthened_after_merge(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.partition_classes = 0;
    o.clock_ms = test_clock;
    set_clock(100 * DAY);
    tsdb *db = open_plain("latest_dc3", &o);
    tsdb_sid id;
    OK(tsdb_series_create(db, "dc3.a", TSDB_FLOAT64, NULL, &id));
    put_f64(db, id, 10 * DAY, 1.0, false);
    OK(tsdb_flush(db));
    put_f64(db, id, 20 * DAY, 2.0, false);
    OK(tsdb_flush(db));
    put_f64(db, id, 20 * DAY + HOUR, 3.0, false);
    OK(tsdb_flush(db));
    OK(tsdb_set_retention(db, id, 50 * DAY)); /* every row is below 50 d */
    OK(tsdb_optimize(db));                     /* the merge drops them all */
    OK(tsdb_set_retention(db, id, 0));
    check_vs_scan(db, id, "lengthened after a merge");
    tsdb_latest_result r = one(db, id, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_NONE);
    OK(tsdb_close(db));
}

/* T4: sync mode, an appended row whose WAL fdatasync is still held: a query does not see it, so the lookup must not
 * either (the table's memtable read honours `visible`). Mutant: lookups ignore the WAL's durable position. */
typedef struct {
    tsdb *db;
    tsdb_sid id;
} dc4;
static void *dc4_writer(void *arg)
{
    dc4 *x = arg;
    put_f64(x->db, x->id, 20, 2.0, false);
    return NULL;
}

static void test_sync_visibility(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    dc4 x;
    x.db = open_plain("latest_dc4", &o);
    OK(tsdb_series_create(x.db, "dc4.a", TSDB_FLOAT64, NULL, &x.id));
    put_f64(x.db, x.id, 10, 1.0, false);
    atomic_store(&sy_release, 0);
    atomic_store(&sy_stalled, 0);
    atomic_store(&sy_armed, 1);
    pthread_t th;
    CHECK(pthread_create(&th, NULL, dc4_writer, &x) == 0);
    while (!atomic_load(&sy_stalled))
        usleep(1000);
    rowset rr = {0};
    scan(x.db, x.id, &rr);
    CHECK(rr.n == 1 && rr.ts[0] == 10); /* the query sees only the durable row */
    tsdb_latest_result r = one(x.db, x.id, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 10 && r.v.f64 == 1.0);
    rs_free(&rr);
    atomic_store(&sy_release, 1);
    pthread_join(th, NULL);
    r = one(x.db, x.id, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 20 && r.v.f64 == 2.0);
    OK(tsdb_close(x.db));
}

/* T5: the newest row exactly one below the cutoff has expired. Mutant: the cutoff test is off by one. */
static void test_row_below_cutoff(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.clock_ms = test_clock;
    set_clock(100 * DAY);
    tsdb *db = open_plain("latest_dc5", &o);
    tsdb_sid id;
    OK(tsdb_series_create(db, "dc5.a", TSDB_FLOAT64, NULL, &id));
    put_f64(db, id, 90 * DAY - 1, 1.0, false);
    OK(tsdb_flush(db));
    OK(tsdb_set_retention(db, id, 10 * DAY)); /* the cutoff is 90 d */
    tsdb_latest_result r = one(db, id, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_NONE);
    put_f64(db, id, 90 * DAY, 2.0, false); /* and one at the cutoff is live */
    OK(tsdb_flush(db));
    r = one(db, id, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 90 * DAY);
    OK(tsdb_close(db));
}

/* T6: the build's bound for state series (no time-weighted statistics, so a granule's newest non-null row is not known
 * from them). With the warmer off after the reopen, the lookups use the build's bounds: a granule that has nulls and
 * ends in a value (bound = its last ts), one that ends in a null, and an all-null one (known empty). Mutant: the bound is
 * always last ts - 1 (a memtable row at last ts - 1 then wins wrongly). */
static void test_build_state_bounds(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.partition_classes = 0;
    char dir[700];
    snprintf(dir, sizeof dir, "%s/latest_dc6", root);
    rmrf(dir);
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tsdb_sid id[3];
    uint32_t code;
    for (int i = 0; i < 3; i++) {
        char nm[32];
        snprintf(nm, sizeof nm, "dc6.s%d", i);
        OK(tsdb_series_create(db, nm, TSDB_STATE, NULL, &id[i]));
    }
    OK(tsdb_state(db, id[0], "up", &code));
    uint32_t vals[3][2] = {{0, code}, {code, 0}, {0, 0}}; /* null, up | up, null | null, null */
    uint8_t valid[3] = {0x2, 0x1, 0x0}; /* a bit per row */
    int64_t ts[2] = {10, 20};
    for (int i = 0; i < 3; i++) {
        tsdb_values v = {TSDB_STATE, vals[i], &valid[i]};
        OK(tsdb_insert_series_values(db, id[i], ts, &v, 2));
    }
    OK(tsdb_flush(db));
    OK(tsdb_close(db));
    tsdb_test_latest_nowarm(1);
    OK(tsdb_open(dir, &o, &db));
    tsdb_test_latest_nowarm(0);
    /* a memtable row just below the granule's last row: it beats the entry only if the entry's bound were below it */
    int64_t t19 = 19;
    uint32_t c19 = code;
    uint8_t v19 = 1;
    tsdb_values mv = {TSDB_STATE, &c19, &v19};
    OK(tsdb_insert_series_values(db, id[0], &t19, &mv, 1));
    for (int i = 0; i < 3; i++)
        check_vs_scan(db, id[i], "state build");
    tsdb_latest_result r = one(db, id[0], TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 20);
    r = one(db, id[1], TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 10);
    r = one(db, id[2], TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_NONE);
    OK(tsdb_close(db));
}

static void test_partial_merge(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.partition_ms = 1000000000;
    o.merge_threads = 1;
    o.merge_idle_s = 1;
    o.max_part_bytes = 200000;
    tsdb *db = open_plain("latest_partial", &o);
    char dir[700];
    snprintf(dir, sizeof dir, "%s/latest_partial", root);
    tsdb_sid s;
    OK(tsdb_series_create(db, "pm.a", TSDB_INT64, NULL, &s));
    enum { N = 100000 };
    static int64_t ts[N], v[N];
    uint64_t r = 88172645463325252ull;
    for (int i = 0; i < N; i++) {
        r ^= r << 13, r ^= r >> 7, r ^= r << 17;
        ts[i] = i, v[i] = 1 | (int64_t)(r & 0xffffff) << 1;
    }
    tsdb_values tv = {TSDB_INT64, v, NULL};
    OK(tsdb_insert_series_values(db, s, ts, &tv, N));
    OK(tsdb_flush(db));
    int64_t rts[100], rv[100];
    for (int i = 0; i < 100; i++)
        rts[i] = i, rv[i] = 7;
    tsdb_values rtv = {TSDB_INT64, rv, NULL};
    OK(tsdb_replace_range(db, s, 0, N / 2, rts, &rtv, 100));
    tsdb_stats st;
    for (int i = 0; i < 100; i++) {
        tsdb_get_stats(db, &st);
        if (st.parts == 2 && mixed_parts(dir) == 1)
            break;
        struct timespec d = {0, 100000000};
        nanosleep(&d, NULL);
    }
    CHECK(mixed_parts(dir) == 1); /* the merge happened, and left the old part out */
    struct { int64_t end; int64_t ts, v; } want[] = {
        {100, 99, 7}, {50, 49, 7}, {1, 0, 7}, {N / 2, 99, 7}, {N / 2 + 1, N / 2, v[N / 2]}, {TSDB_TS_MAX, N - 1, v[N - 1]},
    };
    for (size_t k = 0; k < sizeof want / sizeof *want; k++)
        for (int wn = 0; wn < 2; wn++) {
            tsdb_latest_result x = one(db, s, want[k].end, wn);
            if (x.kind != TSDB_LATEST_VALUE || x.ts != want[k].ts || x.v.i64 != want[k].v) {
                fprintf(stderr, "partial merge: end %lld: kind %d ts %lld v %lld, want ts %lld v %lld\n",
                        (long long)want[k].end, x.kind, (long long)x.ts, (long long)x.v.i64, (long long)want[k].ts,
                        (long long)want[k].v);
                exit(1);
            }
        }
    rowset rr = {0};
    scan(db, s, &rr);
    CHECK(rr.n == N / 2 + 100);
    for (int64_t end = 0; end < 130; end += 13) { /* and against the scan */
        expect e = oracle(&rr, end, false);
        tsdb_latest_opts eo = {.end = end};
        tsdb_latest_result x;
        OK(tsdb_latest(db, &s, 1, &eo, &x));
        same(&x, &e, s, "partial merge scan", 0);
    }
    rs_free(&rr);
    OK(tsdb_close(db));
}

/* ------------------------------------------------------------------ memtables against each other (L1) */

/* A lookup while a flush holds a flushing memtable, and the active one writes over the flushing rows' timestamps
 * (design 8, N1): newer memtable wins ties, a null in it hides the older value (and does not stop at the
 * flushing memtable's row), the flushing one wins over the parts. A follower refreshes in the same window, so it
 * reads two WAL tails and the parts. Each answer is also compared with a full scan of the same handle. */
typedef struct fw {
    tsdb *w, *f;
    tsdb_sid id;
    bool armed;
    int fired;
} fw;

static void check_vs_scan(tsdb *db, tsdb_sid id, const char *what)
{
    rowset rr = {0};
    scan(db, id, &rr);
    static const int64_t ends[] = {TSDB_TS_MAX, 45, 40, 35, 30, 25, 20, 15, 10, 5, TSDB_TS_MIN};
    for (size_t k = 0; k < sizeof ends / sizeof *ends; k++)
        for (int wn = 0; wn < 2; wn++) {
            expect e = oracle(&rr, ends[k], wn);
            tsdb_latest_opts eo = {.end = ends[k], .with_nulls = wn};
            tsdb_latest_result x;
            OK(tsdb_latest(db, &id, 1, &eo, &x));
            same(&x, &e, id, what, (int)k);
        }
    rs_free(&rr);
}

static void fw_hook(void *arg)
{
    fw *x = arg;
    if (!x->armed)
        return;
    x->armed = false;
    x->fired++;
    /* the flushing memtable holds ts 10 -> 7.0, 20 -> null, 30 -> 8.0; the parts 10 -> 5.0, 20 -> 6.0 */
    tsdb_latest_result r = one(x->w, x->id, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 30 && r.v.f64 == 8.0);
    r = one(x->w, x->id, 25, 0); /* its null at 20 hides the part's 6.0: the answer is its own 10 -> 7.0 */
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 10 && r.v.f64 == 7.0);
    /* the active memtable: 10 -> null, 20 -> 9.0, 30 -> null, 40 -> 1.0 */
    put_f64(x->w, x->id, 10, 0, true);
    put_f64(x->w, x->id, 20, 9.0, false);
    put_f64(x->w, x->id, 30, 0, true);
    put_f64(x->w, x->id, 40, 1.0, false);
    r = one(x->w, x->id, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 40 && r.v.f64 == 1.0);
    r = one(x->w, x->id, 40, 0); /* 30: null (active) over 8.0 (flushing): the answer is 20 -> 9.0 */
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 20 && r.v.f64 == 9.0);
    r = one(x->w, x->id, 40, 1);
    CHECK(r.kind == TSDB_LATEST_NULL && r.ts == 30);
    r = one(x->w, x->id, 20, 0); /* 10: null (active) over 7.0 (flushing) over 5.0 (part): nothing below */
    CHECK(r.kind == TSDB_LATEST_NONE);
    r = one(x->w, x->id, 20, 1);
    CHECK(r.kind == TSDB_LATEST_NULL && r.ts == 10);
    r = one(x->w, x->id, 21, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 20 && r.v.f64 == 9.0);
    check_vs_scan(x->w, x->id, "writer, flushing + active");
    OK(tsdb_refresh(x->f));
    check_vs_scan(x->f, x->id, "follower, two tails");
    r = one(x->f, x->id, 20, 0);
    CHECK(r.kind == TSDB_LATEST_NONE);
}

/* ------------------------------------------------------------------ the warmer */

typedef struct {
    tsdb *db;
    tsdb_sid id;
    bool armed;
    int fired;
} wm;

/* A publish between the warmer's cut and its store: a newer row, flushed. The fill (the row at 10) is then stale
 * and must be refused: the entry the flush made (30) is the right one. */
static void wm_hook(void *arg, int at)
{
    wm *x = arg;
    if (at != LH_WARM_BEFORE_STORE || !x->armed)
        return;
    x->armed = false;
    put_f64(x->db, x->id, 30, 99.0, false);
    OK(tsdb_flush(x->db));
    __atomic_add_fetch(&x->fired, 1, __ATOMIC_SEQ_CST);
}

static void wm_stats(tsdb *db, uint64_t s[6])
{
    CHECK(tsdb_test_latest_stats(db, s));
}

/* Waits until cond(stats) holds, at most 20 s. */
static bool wm_wait(tsdb *db, bool (*cond)(const uint64_t *s, uint64_t arg), uint64_t arg)
{
    for (int i = 0; i < 2000; i++) {
        uint64_t s[6];
        wm_stats(db, s);
        if (cond(s, arg))
            return true;
        usleep(10000);
    }
    return false;
}
static bool no_unknown(const uint64_t *s, uint64_t arg) { (void)arg; return s[5] == 0; }
static bool refused_at_least(const uint64_t *s, uint64_t arg) { return s[4] >= arg; }

static void test_warmer(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    tsdb *db = open_plain("latest_warmer", &o);
    enum { N = 300 };
    tsdb_sid id[N];
    for (int i = 0; i < N; i++) {
        char nm[32];
        snprintf(nm, sizeof nm, "wm.%d", i);
        OK(tsdb_series_create(db, nm, TSDB_FLOAT64, NULL, &id[i]));
        put_f64(db, id[i], 10, i, false);
        put_f64(db, id[i], 20, 1000 + i, false);
    }
    OK(tsdb_flush(db));
    /* the newest row of every series deleted: every entry is unknown until the warmer fills it */
    for (int i = 0; i < N; i++)
        OK(tsdb_delete_range(db, id[i], 20, TSDB_TS_MAX));
    CHECK(wm_wait(db, no_unknown, 0));
    uint64_t s0[6], s1[6];
    wm_stats(db, s0);
    CHECK(s0[3] >= N); /* filled by the warmer */
    for (int i = 0; i < N; i++) {
        tsdb_latest_result r = one(db, id[i], TSDB_TS_MAX, 0);
        CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 10 && r.v.f64 == (double)i);
    }
    wm_stats(db, s1);
    CHECK(s1[0] - s0[0] == N); /* every lookup answered from the table, none searched */
    CHECK(s1[1] == s0[1]);
    for (int i = 0; i < N; i += 37)
        check_vs_scan(db, id[i], "warmer, filled");

    /* a series deleted entirely: its fill is empty */
    tsdb_sid e;
    OK(tsdb_series_create(db, "wm.empty", TSDB_FLOAT64, NULL, &e));
    put_f64(db, e, 10, 1.0, false);
    OK(tsdb_flush(db));
    OK(tsdb_delete_range(db, e, TSDB_TS_MIN, TSDB_TS_MAX));
    CHECK(wm_wait(db, no_unknown, 0));
    wm_stats(db, s0);
    tsdb_latest_result r = one(db, e, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_NONE);
    wm_stats(db, s1);
    CHECK(s1[0] - s0[0] == 1 && s1[1] == s0[1]);

    /* a publish between the cut and the store refuses the batch */
    wm x = {.db = db};
    OK(tsdb_series_create(db, "wm.race", TSDB_FLOAT64, NULL, &x.id));
    put_f64(db, x.id, 10, 1.0, false);
    put_f64(db, x.id, 20, 2.0, false);
    OK(tsdb_flush(db));
    wm_stats(db, s0);
    tsdb_test_latest_hook(db, wm_hook, &x);
    x.armed = true;
    OK(tsdb_delete_range(db, x.id, 20, TSDB_TS_MAX));
    CHECK(wm_wait(db, refused_at_least, s0[4] + 1));
    CHECK(__atomic_load_n(&x.fired, __ATOMIC_SEQ_CST) == 1);
    CHECK(wm_wait(db, no_unknown, 0));
    tsdb_test_latest_hook(db, NULL, NULL);
    r = one(db, x.id, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 30 && r.v.f64 == 99.0);
    check_vs_scan(db, x.id, "warmer, refused");
    OK(tsdb_close(db));
}

/* Between the warmer's cut and its store: a retention limit (any write section bumps lgen), then a drop. Each must
 * refuse the batch; the entry stays an exact bound until the next fill. */
typedef struct {
    tsdb *db;
    tsdb_sid id;
    int action; /* 0 none, 1 set a retention, 2 drop before */
    int fired;
} wr;

static void wr_hook(void *arg, int at)
{
    wr *x = arg;
    if (at != LH_WARM_BEFORE_STORE || !x->action)
        return;
    int a = x->action;
    x->action = 0;
    if (a == 1)
        OK(tsdb_set_retention(x->db, x->id, 100000 * DAY));
    else
        OK(tsdb_drop_before(x->db, 15));
    __atomic_add_fetch(&x->fired, 1, __ATOMIC_SEQ_CST);
}

static void test_warmer_other_writers(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    wr x = {0};
    x.db = open_plain("latest_warmer2", &o);
    OK(tsdb_series_create(x.db, "wr.a", TSDB_FLOAT64, NULL, &x.id));
    put_f64(x.db, x.id, 10, 1.0, false);
    put_f64(x.db, x.id, 20, 2.0, false);
    OK(tsdb_flush(x.db));
    for (int a = 1; a <= 2; a++) {
        uint64_t s0[6];
        wm_stats(x.db, s0);
        tsdb_test_latest_hook(x.db, wr_hook, &x);
        x.action = a;
        OK(tsdb_delete_range(x.db, x.id, 20, TSDB_TS_MAX)); /* unknown: the warmer cuts and fills */
        CHECK(wm_wait(x.db, refused_at_least, s0[4] + 1));
        CHECK(__atomic_load_n(&x.fired, __ATOMIC_SEQ_CST) == a);
        CHECK(wm_wait(x.db, no_unknown, 0));
        tsdb_test_latest_hook(x.db, NULL, NULL);
        check_vs_scan(x.db, x.id, a == 1 ? "warmer, refused by a retention" : "warmer, refused by a drop");
        put_f64(x.db, x.id, 20, 2.0, false); /* the row is back for the next round */
        OK(tsdb_flush(x.db));
    }
    OK(tsdb_close(x.db));
}

static void test_flush_window(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    tsdb *w = open_plain("latest_flushwin", &o);
    char dir[700];
    snprintf(dir, sizeof dir, "%s/latest_flushwin", root);
    tsdb_options ro;
    tsdb_options_init(&ro);
    ro.read_only = 1;
    tsdb *f;
    OK(tsdb_open(dir, &ro, &f));
    fw x = {.w = w, .f = f};
    OK(tsdb_series_create(w, "fw.a", TSDB_FLOAT64, NULL, &x.id));
    put_f64(w, x.id, 10, 5.0, false);
    put_f64(w, x.id, 20, 6.0, false);
    OK(tsdb_flush(w)); /* the parts */
    put_f64(w, x.id, 10, 7.0, false);
    put_f64(w, x.id, 20, 0, true);
    put_f64(w, x.id, 30, 8.0, false);
    tsdb_test_midplace(w, fw_hook, &x);
    x.armed = true;
    OK(tsdb_flush(w));
    tsdb_test_midplace(w, NULL, NULL);
    CHECK(x.fired == 1); /* the hook ran while the memtable was flushing */
    check_vs_scan(w, x.id, "writer, after the flush");
    tsdb_latest_result r = one(w, x.id, 20, 0);
    CHECK(r.kind == TSDB_LATEST_NONE);
    r = one(w, x.id, TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 40 && r.v.f64 == 1.0);
    OK(tsdb_refresh(f));
    check_vs_scan(f, x.id, "follower, after the flush");
    OK(tsdb_close(f));
    OK(tsdb_close(w));
}

/* ------------------------------------------------------------------ the large-flush window (3.2) */

enum { WS_N = 7 };
typedef struct {
    tsdb *db;
    tsdb_sid id[WS_N];
    int mid, before_list;
} ww;

/* Runs on the flusher's thread, in the window: some of the stores are made, no lock is held, the flushing memtable is
 * listed. Every lookup must still give the full scan's answer. */
static void ww_hook(void *arg, int at)
{
    ww *x = arg;
    if (at != LH_WINDOW_MID && at != LH_FOLD_BEFORE_LIST)
        return;
    if (at == LH_WINDOW_MID)
        x->mid++;
    else
        x->before_list++;
    for (int i = 0; i < WS_N; i++)
        check_vs_scan(x->db, x->id[i], at == LH_WINDOW_MID ? "window, half stored" : "window, all stored");
}

static void test_window(void)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    ww x = {0};
    x.db = open_plain("latest_window", &o);
    for (int i = 0; i < WS_N; i++) {
        char nm[32];
        snprintf(nm, sizeof nm, "ww.%d", i);
        OK(tsdb_series_create(x.db, nm, TSDB_FLOAT64, NULL, &x.id[i]));
    }
    /* the parts: s0 10, s1 30, s2 20, s4 40 (deleted below: unknown), s5 20, s6 20 */
    put_f64(x.db, x.id[0], 10, 1.0, false);
    put_f64(x.db, x.id[1], 30, 2.0, false);
    put_f64(x.db, x.id[2], 20, 3.0, false);
    put_f64(x.db, x.id[4], 40, 4.0, false);
    put_f64(x.db, x.id[4], 5, 4.5, false);
    put_f64(x.db, x.id[5], 20, 5.0, false);
    put_f64(x.db, x.id[6], 20, 6.0, false);
    OK(tsdb_flush(x.db));
    OK(tsdb_delete_range(x.db, x.id[4], 40, TSDB_TS_MAX)); /* s4: unknown (39) until the warmer fills it */
    /* the flush: s0 a newer row (known to R), s1 an older one (the entry stays), s2 a null over its row (noted for
     * the write section), s3 a first row (empty to R), s4 a row above the bound (unknown to R), s5 the same
     * timestamp (R.ts >= k.ts), s6 a newer row and then a null beyond it (R below the null) */
    put_f64(x.db, x.id[0], 25, 7.0, false);
    put_f64(x.db, x.id[1], 20, 8.0, false);
    put_f64(x.db, x.id[2], 20, 0, true);
    put_f64(x.db, x.id[3], 10, 9.0, false);
    put_f64(x.db, x.id[4], 42, 10.0, false);
    put_f64(x.db, x.id[5], 20, 11.0, false);
    put_f64(x.db, x.id[6], 25, 12.0, false);
    put_f64(x.db, x.id[6], 28, 0, true);
    tsdb_test_latest_window(2);
    tsdb_test_latest_hook(x.db, ww_hook, &x);
    OK(tsdb_flush(x.db));
    tsdb_test_latest_hook(x.db, NULL, NULL);
    tsdb_test_latest_window(50000);
    CHECK(x.mid == 1 && x.before_list == 1); /* the window ran */
    for (int i = 0; i < WS_N; i++)
        check_vs_scan(x.db, x.id[i], "window, after");
    tsdb_latest_result r = one(x.db, x.id[2], TSDB_TS_MAX, 0); /* the null took 20: nothing below */
    CHECK(r.kind == TSDB_LATEST_NONE);
    r = one(x.db, x.id[6], TSDB_TS_MAX, 0);
    CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == 25 && r.v.f64 == 12.0);
    /* the warmer settles the unknown entry the null made */
    CHECK(wm_wait(x.db, no_unknown, 0));
    for (int i = 0; i < WS_N; i++)
        check_vs_scan(x.db, x.id[i], "window, warmed");
    OK(tsdb_close(x.db));
}

/* ------------------------------------------------------------------ a follower */

/* A read-only follower (WAL tails, flushing memtables, parts, deletes, optimize) against its own full scan, with
 * nulls over values at the same timestamps across the writer's memtable segments and parts. The follower's path is
 * this slow path for good (design D5). */
static void test_follower(int steps)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.wal_max_batch_rows = 8;
    o.granule_rows = 16;
    tsdb *w = open_plain("latest_follow", &o);
    char dir[700];
    snprintf(dir, sizeof dir, "%s/latest_follow", root);
    enum { NS = 4 };
    tsdb_sid id[NS];
    for (int i = 0; i < NS; i++) {
        char n[16];
        snprintf(n, sizeof n, "fo.s%d", i);
        OK(tsdb_series_create(w, n, TSDB_FLOAT64, NULL, &id[i]));
    }
    tsdb_options ro;
    tsdb_options_init(&ro);
    ro.read_only = 1;
    tsdb *f;
    OK(tsdb_open(dir, &ro, &f));
    rs = 12345;
    rowset rr = {0};
    long checks = 0;
    for (int step = 0; step < steps; step++) {
        int s = (int)(rnd() % NS);
        uint64_t r = rnd() % 100;
        if (r < 80) {
            int64_t ts = (int64_t)(rnd() % 300);
            double v = (double)(rnd() % 1000);
            uint8_t valid = rnd() % 3 != 0;
            tsdb_values vals = {TSDB_FLOAT64, &v, &valid};
            OK(tsdb_insert_series_values(w, id[s], &ts, &vals, 1));
        } else if (r < 85) {
            int64_t a = (int64_t)(rnd() % 300);
            OK(tsdb_delete_range(w, id[s], a, a + (int64_t)(rnd() % 40)));
        } else if (r < 90) {
            OK(tsdb_flush(w));
        } else if (r < 92) {
            OK(tsdb_optimize(w));
        }
        if (step % 7)
            continue;
        OK(tsdb_refresh(f));
        for (int k = 0; k < NS; k++) {
            scan(f, id[k], &rr);
            for (int e = 0; e < 4; e++) {
                int64_t end = e == 0 ? TSDB_TS_MAX : (int64_t)(rnd() % 320);
                for (int wn = 0; wn < 2; wn++) {
                    expect ex = oracle(&rr, end, wn);
                    tsdb_latest_opts lo = {.end = end, .with_nulls = wn};
                    tsdb_latest_result x;
                    OK(tsdb_latest(f, &id[k], 1, &lo, &x));
                    same(&x, &ex, id[k], "follower", step);
                    checks++;
                }
            }
        }
    }
    CHECK(checks > 1000);
    rs_free(&rr);
    OK(tsdb_close(f));
    OK(tsdb_close(w));
}

/* ------------------------------------------------------------------ main */

/* flush-tail: a publisher's fold lists are freed after the table's write lock is released. At 10^6 series one list is
 * 40 MB and its munmap (2 ms) held every lookup off. The hook runs at each free (LH_LF_FREE); the lock must not be
 * held. Mutant: lt_fold frees the lists itself (as before). */
typedef struct {
    tsdb *db;
    int frees, held;
} lfx;

static void lf_hook(void *arg, int at)
{
    lfx *x = arg;
    if (at != LH_LF_FREE)
        return;
    x->frees++;
    x->held += tsdb_test_latest_wr_held(x->db);
}

static void test_lf_free_unlocked(void)
{
    for (int mode = 0; mode < 3; mode++) { /* flush, flush through the window, a large insert's parts */
        tsdb_options o;
        tsdb_options_init(&o);
        o.merge_threads = 0;
        o.partition_classes = 0;
        o.buffer_rows = 100;
        tsdb_test_latest_window(mode == 1 ? 1 : 50000);
        tsdb *db = open_plain("latest_lf", &o);
        tsdb_sid id[4];
        for (int i = 0; i < 4; i++) {
            char name[32];
            snprintf(name, sizeof name, "lf.%d", i);
            OK(tsdb_series_create(db, name, TSDB_FLOAT64, NULL, &id[i]));
        }
        lfx x = {db, 0, 0};
        tsdb_test_latest_hook(db, lf_hook, &x);
        for (int i = 0; i < 4; i++) {
            if (mode < 2) {
                for (int k = 0; k < 20; k++)
                    put_f64(db, id[i], 1000 + k, 1.0 + k, false);
            } else {
                int64_t ts[500];
                double v[500];
                for (int k = 0; k < 500; k++) {
                    ts[k] = 1000 + k;
                    v[k] = k;
                }
                tsdb_values vals = {TSDB_FLOAT64, v, NULL};
                OK(tsdb_insert_series_values(db, id[i], ts, &vals, 500));
            }
        }
        if (mode < 2)
            OK(tsdb_flush(db));
        tsdb_test_latest_hook(db, NULL, NULL);
        CHECK(x.frees > 0);
        CHECK(x.held == 0);
        if (x.held || !x.frees)
            fprintf(stderr, "  mode %d: %d fold lists freed, %d under the table's write lock\n", mode, x.frees, x.held);
        tsdb_latest_result r = one(db, id[0], TSDB_TS_MAX, 0);
        CHECK(r.kind == TSDB_LATEST_VALUE && r.ts == (mode < 2 ? 1019 : 1499));
        OK(tsdb_close(db));
    }
    tsdb_test_latest_window(50000);
}

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
    test_retention_lengthened_in_merge();
    test_merges_not_starved();
    test_active_over_flushing();
    test_drop_floor_lookup();
    test_lengthened_after_merge();
    test_lf_free_unlocked();
    test_sync_visibility();
    test_row_below_cutoff();
    test_build_state_bounds();
    printf("retention lengthened in a merge ok\n");
    test_partial_merge();
    printf("partial merge ok\n");
    test_warmer();
    printf("warmer ok\n");
    test_warmer_other_writers();
    printf("warmer refused by retention and drop ok\n");
    test_flush_window();
    printf("flushing + active memtables, follower tails ok\n");
    test_window();
    printf("large-flush window ok\n");
    test_follower(1500);
    printf("follower ok\n");

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
    tsdb_test_latest_window(1); /* every flush through the 3.2 window */
    random_run("latest_r1", o, false, steps, 22);
    tsdb_test_latest_window(50000);
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
    tsdb_test_latest_window(2);
    random_run("latest_r3", o, true, steps, 44);
    tsdb_test_latest_window(50000);
    printf("periodic WAL ok\n");

    /* partial merges (merge_idle_s, a small max_part_bytes): parts with rows and their own tombstones */
    tsdb_options_init(&o);
    o.wal_max_batch_rows = 64;
    o.granule_rows = 32;
    o.merge_threads = 1;
    o.merge_idle_s = 1;
    o.max_part_bytes = 4000;
    random_run("latest_r4", o, false, steps, 55);
    printf("partial merges ok\n");

    /* no table at all: the slow path alone answers the same */
    tsdb_options_init(&o);
    o.latest_cache = 0;
    o.wal_max_batch_rows = 64;
    o.granule_rows = 32;
    o.merge_threads = 1;
    random_run("latest_r5", o, true, steps / 2, 66);
    printf("latest_cache = 0 ok\n");

    test_crash();
    printf("crash ok\n");
    test_concurrent();
    printf("concurrent ok\n");
    tsdb_test_latest_window(1); /* writers, merges and lookups with every flush a window */
    conc_flush_every = 4;
    test_concurrent();
    conc_flush_every = 50;
    tsdb_test_latest_window(50000);
    printf("concurrent with windows ok\n");
    rmrf(root);
    printf("test_latest ok\n");
    return 0;
}
