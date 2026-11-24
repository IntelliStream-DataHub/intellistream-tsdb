/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The logical digest (src/ecdigest.c, design-cluster-data.md E26, the R8 plan's D7): one value for the same rows in any
 * layout, another for other rows. Twins are separate databases written with the same rows and series ids:
 *  - layouts: one part, ten parts, ten parts merged, one part without hour cuts: one digest; a value changed, a row
 *    missing: another (mutant digestbytes hashes the files and must fail here);
 *  - retention: a series with a 1-day limit, half of its rows expired; one twin merged (its merge dropped them), the
 *    other still stores them: one digest at the clock; the frozen cutoffs (ecd_freeze) give the same
 *    (digestnoretention must fail);
 *  - tombstones over an older object (an .ec part below the set): twins with the same deletes, one merged, one not:
 *    one digest; a twin without the delete that hides only the object's rows: another (digestnotomb must fail);
 *  - a set mixing .ec and plain parts equals the same rows as plain parts.
 * Usage: test_ecdigest [--mutant NAME] [scratch dir]
 */
#include "internal.h"
#include "chunk.h"
#include "ecdigest.h"
#include "ecpart.h"
#include "ecswap.h"
#include "tier.h"

#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int failures;
#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);                                               \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

#ifdef TSDB_EC
#define K 2
#define M 1
#define NST 3
static const int64_t DAY = 86400000, HOUR = 3600000, T0 = 1704067200000; /* 2024-01-01 */
static char scratch[PATH_MAX];
static _Atomic int64_t clock_now = 1704067200000 + 2 * 86400000;

static int64_t tclock(void *arg)
{
    (void)arg;
    return atomic_load(&clock_now);
}

static void rm_rf(const char *path)
{
    char cmd[PATH_MAX * 2];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", path);
    if (system(cmd) != 0)
        fprintf(stderr, "rm -rf %s failed\n", path);
}

enum { SA, SB, SI, SR, NS }; /* a: dense (hour cuts), b: slow with a null, i: int64, r: float32 under retention */
static const char *const SNAME[NS] = {"a", "b", "i", "r"};
static const int STYPE[NS] = {TSDB_FLOAT32, TSDB_FLOAT32, TSDB_INT64, TSDB_FLOAT32};

typedef struct {
    char dir[PATH_MAX + 64], st[NST][PATH_MAX + 80];
    tsdb *t;
    eng *db;
    uint32_t s[NS];
} tw;

static int tw_open(tw *x, const char *name, bool tier)
{
    memset(x, 0, sizeof *x);
    snprintf(x->dir, sizeof x->dir, "%s/%s", scratch, name);
    rm_rf(x->dir);
    CHECK(mkdir(x->dir, 0755) == 0);
    if (tier) {
        char spec[8192];
        int n = snprintf(spec, sizeof spec, "k=%d,m=%d,keep_s=3600,cache_mb=16,dirs=", K, M);
        for (int i = 0; i < NST; i++) {
            snprintf(x->st[i], sizeof x->st[i], "%s/%s.st%d", scratch, name, i);
            rm_rf(x->st[i]);
            n += snprintf(spec + n, sizeof spec - (size_t)n, "%s%s", i ? ":" : "", x->st[i]);
        }
        setenv("TSDB_TEST_TIER", spec, 1);
        setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);
    }
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.partition_classes = 0;
    o.clock_ms = tclock;
    int rc = tsdb_open(x->dir, &o, &x->t);
    unsetenv("TSDB_TEST_TIER");
    unsetenv("TSDB_TEST_TIER_I_KNOW");
    CHECK(rc == 0);
    if (rc)
        return rc;
    x->db = tsdb_test_engine(x->t, 0);
    CHECK(x->db != NULL);
    for (int i = 0; i < NS; i++)
        CHECK(eng_series(x->db, SNAME[i], STYPE[i], &x->s[i]) == 0);
    return 0;
}

static void tw_close(tw *x)
{
    if (x->t)
        tsdb_close(x->t);
    x->t = NULL;
}

/* the rows of series i: (ts, value bits) */
static size_t nrows(int s) { return s == SA ? 14400 : s == SB ? 1440 : s == SI ? 720 : 1440; }
static int64_t row_ts(int s, size_t j)
{
    return s == SA ? T0 + HOUR + (int64_t)j * 1000 : s == SB ? T0 + (int64_t)j * 10000 : s == SI ? T0 + (int64_t)j * 120000
                                                                                     : T0 + (int64_t)j * 60000;
}
static uint64_t row_val(int s, size_t j)
{
    if (s == SB && j == 5)
        return type_null(TSDB_FLOAT32);
    if (s == SI)
        return (uint64_t)(int64_t)(j * 7 - 300);
    float f = (float)((j * 2654435761u) % 100003) / 13.0f + (float)s;
    uint32_t b;
    memcpy(&b, &f, 4);
    return b;
}

enum { W_SAME, W_CHANGE, W_MISSING };

/* every row of every series in nparts parts (row j of each series to part j % nparts), each part one flush */
static void tw_write(tw *x, int nparts, int how)
{
    int64_t *ts = malloc(16000 * sizeof *ts);
    uint8_t *v = malloc(16000 * 8);
    for (int p = 0; p < nparts; p++) {
        for (int s = 0; s < NS; s++) {
            size_t n = 0, w = type_width(STYPE[s]);
            for (size_t j = (size_t)p; j < nrows(s); j += (size_t)nparts) {
                if (how == W_MISSING && s == SB && j == 77)
                    continue;
                ts[n] = row_ts(s, j);
                uint64_t b = row_val(s, j);
                if (how == W_CHANGE && s == SA && j == 9000)
                    b ^= 1;
                memcpy(v + n * w, &b, w);
                n++;
            }
            if (n)
                CHECK(eng_insert_series(x->db, STYPE[s], x->s[s], ts, v, n) == 0);
        }
        CHECK(eng_flush(x->db) == 0);
    }
    free(ts);
    free(v);
}

/* the listed parts of key (with .ec parts or not), referenced */
static size_t tw_set(tw *x, int64_t key, bool with_ec, part ***out)
{
    eng *db = x->db;
    pthread_mutex_lock(&db->lock);
    part **s = calloc(db->nparts + 1, sizeof *s);
    size_t n = 0;
    for (size_t i = 0; i < db->nparts; i++)
        if (db->parts[i]->partition == key && (with_ec || !atomic_load(&db->parts[i]->ec))) {
            part_ref(db->parts[i]);
            s[n++] = db->parts[i];
        }
    pthread_mutex_unlock(&db->lock);
    *out = s;
    return n;
}

static void set_free(part **s, size_t n)
{
    for (size_t i = 0; i < n; i++)
        part_unref(s[i]);
    free(s);
}

static int64_t key0(tw *x)
{
    pthread_mutex_lock(&x->db->lock);
    int64_t k = x->db->nparts ? x->db->parts[0]->partition : -1;
    pthread_mutex_unlock(&x->db->lock);
    return k;
}

static ec_ldigest tw_digest(tw *x, bool with_ec)
{
    ec_ldigest d;
    memset(&d, 0, sizeof d);
    part **s;
    int64_t k = key0(x);
    size_t n = tw_set(x, k, with_ec, &s);
    CHECK(n > 0);
    CHECK(ec_logical_digest(x->db, s, n, k, atomic_load(&clock_now), &d) == 0);
    set_free(s, n);
    return d;
}

static bool same(ec_ldigest a, ec_ldigest b) { return a.rows == b.rows && a.series == b.series && a.xxh64 == b.xxh64; }

static size_t count_parts(tw *x, bool with_ec)
{
    part **s;
    size_t n = tw_set(x, key0(x), with_ec, &s);
    set_free(s, n);
    return n;
}

static uint32_t granules(tw *x)
{
    part **s;
    size_t n = tw_set(x, key0(x), true, &s);
    uint32_t g = 0;
    for (size_t i = 0; i < n; i++)
        g += s[i]->ng;
    set_free(s, n);
    return g;
}

/* ------------------------------------------------------------ layouts */

static void case_layouts(void)
{
    tw one, ten, merged, nocut, chg, miss;
    tw_open(&one, "one", false);
    tw_write(&one, 1, W_SAME);
    tw_open(&ten, "ten", false);
    tw_write(&ten, 10, W_SAME);
    tw_open(&merged, "merged", false);
    tw_write(&merged, 10, W_SAME);
    CHECK(eng_optimize(merged.db) == 0);
    unsigned was = atomic_load(&hour_cut_min);
    atomic_store(&hour_cut_min, 0); /* no hour cuts: other granules for the dense series */
    tw_open(&nocut, "nocut", false);
    tw_write(&nocut, 1, W_SAME);
    atomic_store(&hour_cut_min, was);
    tw_open(&chg, "chg", false);
    tw_write(&chg, 1, W_CHANGE);
    tw_open(&miss, "miss", false);
    tw_write(&miss, 1, W_MISSING);

    CHECK(count_parts(&ten, true) >= 10 && count_parts(&merged, true) < count_parts(&ten, true));
    CHECK(granules(&one) != granules(&nocut)); /* the layouts really differ */
    ec_ldigest d1 = tw_digest(&one, true), d10 = tw_digest(&ten, true), dm = tw_digest(&merged, true),
               dn = tw_digest(&nocut, true), dc = tw_digest(&chg, true), dx = tw_digest(&miss, true);
    uint64_t want = 0;
    for (int s = 0; s < NS; s++)
        want += nrows(s);
    CHECK(d1.rows == want && d1.series == NS);
    CHECK(same(d1, d10));
    CHECK(same(d1, dm));
    CHECK(same(d1, dn));
    CHECK(!same(d1, dc) && dc.rows == d1.rows); /* one value bit */
    CHECK(!same(d1, dx) && dx.rows == d1.rows - 1);
    printf("layouts: %" PRIu64 " rows, digest %016" PRIx64 " (1, 10, merged, no hour cuts)\n", d1.rows, d1.xxh64);
    tw_close(&one), tw_close(&ten), tw_close(&merged), tw_close(&nocut), tw_close(&chg), tw_close(&miss);
}

/* ------------------------------------------------------------ retention */

static void case_retention(void)
{
    atomic_store(&clock_now, T0 + DAY + DAY / 2); /* r's cutoff (1 day) is in the middle of its rows */
    tw a, b;
    tw_open(&a, "ret_a", false);
    tw_open(&b, "ret_b", false);
    CHECK(tsdb_set_retention(a.t, TSDB_SID(0, a.s[SR]), DAY) == 0);
    CHECK(tsdb_set_retention(b.t, TSDB_SID(0, b.s[SR]), DAY) == 0);
    tw_write(&a, 4, W_SAME);
    tw_write(&b, 4, W_SAME);
    CHECK(eng_optimize(a.db) == 0); /* a's merge drops r's expired rows; b still stores them */
    part **s;
    uint64_t ra = 0, rb = 0;
    size_t n = tw_set(&a, key0(&a), true, &s);
    for (size_t i = 0; i < n; i++)
        ra += s[i]->rows;
    set_free(s, n);
    n = tw_set(&b, key0(&b), true, &s);
    for (size_t i = 0; i < n; i++)
        rb += s[i]->rows;
    CHECK(ra < rb); /* the twins really store other rows */
    ec_ldigest da = tw_digest(&a, true), db = tw_digest(&b, true), df;
    CHECK(same(da, db));
    ecd_cut *cuts = NULL;
    size_t nc = 0;
    int64_t k = key0(&b);
    CHECK(ecd_freeze(b.db, s, n, k, atomic_load(&clock_now), &cuts, &nc) == 0);
    CHECK(nc == 1 && cuts && cuts[0].series == b.s[SR] && cuts[0].cutoff == T0 + DAY / 2);
    CHECK(ec_logical_digest_frozen(b.db, s, n, k, cuts, nc, db.tombs, &df) == 0);
    CHECK(same(df, db));
    free(cuts);
    set_free(s, n);
    printf("retention: stored %" PRIu64 " and %" PRIu64 " rows, %" PRIu64 " visible to both\n", ra, rb, da.rows);
    tw_close(&a), tw_close(&b);
    atomic_store(&clock_now, T0 + 2 * DAY);
}

/* ------------------------------------------------------------ settled parts */

/* settles the first listed .part with granules into an .ec part of the same rows (test_keep's settle_one) */
static int settle_first(tw *x)
{
    eng *db = x->db;
    part *p = NULL;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts && !p; i++)
        if (!atomic_load(&db->parts[i]->ec) && db->parts[i]->ng > 0) {
            p = db->parts[i];
            part_ref(p);
        }
    pthread_mutex_unlock(&db->lock);
    CHECK(p != NULL);
    if (!p)
        return TSDB_ERR_NOTFOUND;
    chunk_backend *be = tier_backend(db->tier);
    ec_record r;
    memset(&r, 0, sizeof r);
    chunk_geom g;
    int rc = chunk_geom_derive(&g, K, M, p->map_len);
    CHECK(rc == 0);
    r.version = EC_RECORD_VERSION;
    r.class_check = EC_CLS_PASSED;
    chunk_random(r.oid, sizeof r.oid);
    r.key = p->partition;
    r.min_block = p->min_block;
    r.max_block = p->max_block;
    r.level = p->level;
    r.k = (uint16_t)g.k;
    r.m = (uint16_t)g.m;
    r.cell_bytes = g.cell;
    r.object_len = p->map_len;
    r.object_xxh64 = xxh64(p->map, p->map_len, 0);
    r.writer_epoch = 1;
    r.settle_ms = eng_now_ms(db);
    snprintf(r.source, sizeof r.source, "%s", p->name);
    r.nsums = chunk_sums_total(&g);
    r.sums = malloc(r.nsums * sizeof *r.sums);
    CHECK(chunk_be_see_epoch(be, db->tier_vol, 1) == 0);
    rc = chunk_be_put_object(be, db->tier_vol, tier_vuuid(db), r.oid, &g, 1, p->map, ~0u, r.sums);
    CHECK(rc == 0);
    char tmp[96] = "", ecname[160];
    snprintf(ecname, sizeof ecname, "%.*s.ec", (int)(strlen(p->name) - 5), p->name);
    if (rc == 0)
        CHECK((rc = ec_index_write(db, p, &r, tmp)) == 0);
    part *e = NULL;
    if (rc == 0)
        CHECK((rc = part_open_ec(db, tmp, ecname, &e)) == 0 && e);
    if (rc == 0) {
        snprintf(e->pending, sizeof e->pending, "%.63s", tmp);
        CHECK((rc = ec_swap_commit(db, &p, &e, 1)) == 0);
        part_unref(e);
    }
    part_unref(p);
    free(r.sums);
    return rc;
}

/* rows of series a in [lo, hi) at 1 s, one flush */
static void write_range(tw *x, int64_t lo, int64_t hi)
{
    size_t n = (size_t)((hi - lo) / 1000);
    int64_t *ts = malloc(n * sizeof *ts);
    float *v = malloc(n * sizeof *v);
    for (size_t j = 0; j < n; j++)
        ts[j] = lo + (int64_t)j * 1000, v[j] = (float)((ts[j] / 1000) % 9973) / 3.0f;
    CHECK(eng_insert_series(x->db, TSDB_FLOAT32, x->s[SA], ts, v, n) == 0);
    CHECK(eng_flush(x->db) == 0);
    free(ts);
    free(v);
}

/* an object of [T0, T0 + 1 h), then plain parts of [T0 + 2 h, T0 + 4 h) and deletes: the one over the object's rows
 * (hides rows below the set: only the tombstone term sees it) unless `noobj`, the one over the set's own rows */
static void tomb_twin(tw *x, const char *name, bool noobj, bool merge)
{
    tw_open(x, name, true);
    write_range(x, T0, T0 + HOUR);
    CHECK(settle_first(x) == 0);
    write_range(x, T0 + 2 * HOUR, T0 + 3 * HOUR);
    write_range(x, T0 + 3 * HOUR, T0 + 4 * HOUR);
    if (!noobj)
        CHECK(eng_delete_range(x->db, x->s[SA], T0 + 600000, T0 + 1200000) == 0);
    CHECK(eng_delete_range(x->db, x->s[SA], T0 + 2 * HOUR + 600000, T0 + 3 * HOUR + 600000) == 0);
    if (merge)
        CHECK(eng_optimize(x->db) == 0); /* above the .ec barrier: the merged part keeps the union */
}

static void case_tombstones(void)
{
    tw a, b, c;
    tomb_twin(&a, "tomb_a", false, true);
    tomb_twin(&b, "tomb_b", false, false);
    tomb_twin(&c, "tomb_c", true, false);
    CHECK(count_parts(&a, false) < count_parts(&b, false)); /* a merged */
    CHECK(count_parts(&a, true) == count_parts(&a, false) + 1); /* the object below */
    ec_ldigest da = tw_digest(&a, false), db = tw_digest(&b, false), dc = tw_digest(&c, false);
    CHECK(same(da, db));
    CHECK(dc.rows == db.rows); /* the same rows in the set ... */
    CHECK(!same(dc, db));      /* ... but not the delete over the object's rows */
    printf("tombstones: %zu and %zu parts above the object, one digest; without the delete: another\n",
           count_parts(&a, false), count_parts(&b, false));
    tw_close(&a), tw_close(&b), tw_close(&c);
}

static void case_mixed(void)
{
    tw x;
    tw_open(&x, "mixed", true);
    write_range(&x, T0, T0 + HOUR);
    write_range(&x, T0 + HOUR, T0 + 2 * HOUR);
    write_range(&x, T0 + 30 * 60000, T0 + 90 * 60000); /* overlaps both: last write wins across .ec and plain */
    ec_ldigest plain = tw_digest(&x, true);
    CHECK(settle_first(&x) == 0);
    CHECK(count_parts(&x, true) == 3 && count_parts(&x, false) == 2);
    ec_ldigest mixed = tw_digest(&x, true);
    CHECK(same(plain, mixed));
    CHECK(plain.rows == 7200);
    printf("mixed: %" PRIu64 " rows, plain and .ec + plain agree\n", plain.rows);
    tw_close(&x);
}

/* A series whose rows are not in the set (only its delete is), under retention: the leader's live cutoff and a
 * replica's frozen ones (ecd_freeze over the same set) must give one digest (review F1; mutant digestfreezerows) */
static void case_tombonly(void)
{
    atomic_store(&clock_now, T0 + DAY + DAY / 4); /* r's cutoff T0 + 6 h at the delete */
    tw x;
    tw_open(&x, "rv_tombonly", true);
    CHECK(tsdb_set_retention(x.t, TSDB_SID(0, x.s[SR]), DAY) == 0);
    { /* r's rows and a's first hour in one flush: the object below the set */
        size_t n = 1000;
        int64_t *ts = malloc(n * sizeof *ts);
        float *v = malloc(n * sizeof *v);
        for (size_t j = 0; j < n; j++)
            ts[j] = T0 + 7 * HOUR + (int64_t)j * 60000, v[j] = (float)j;
        CHECK(eng_insert_series(x.db, TSDB_FLOAT32, x.s[SR], ts, v, n) == 0);
        free(ts);
        free(v);
    }
    write_range(&x, T0, T0 + HOUR);
    CHECK(settle_first(&x) == 0);
    write_range(&x, T0 + 2 * HOUR, T0 + 3 * HOUR);
    CHECK(eng_delete_range(x.db, x.s[SR], T0 + 8 * HOUR, T0 + 20 * HOUR) == 0);
    atomic_store(&clock_now, T0 + DAY + 12 * HOUR); /* the settle's clock: r's cutoff T0 + 12 h, inside the delete */
    part **s;
    int64_t k = key0(&x);
    size_t n = tw_set(&x, k, false, &s);
    ec_ldigest live, fr;
    CHECK(ec_logical_digest(x.db, s, n, k, atomic_load(&clock_now), &live) == 0);
    ecd_cut *cuts = NULL;
    size_t nc = 0;
    CHECK(ecd_freeze(x.db, s, n, k, atomic_load(&clock_now), &cuts, &nc) == 0);
    CHECK(ec_logical_digest_frozen(x.db, s, n, k, cuts, nc, live.tombs, &fr) == 0);
    printf("tombonly: %zu parts, %zu frozen cuts; live %" PRIu64 "/%" PRIu64 "/%016" PRIx64 " frozen %" PRIu64
           "/%" PRIu64 "/%016" PRIx64 " -> %s\n", n, nc, live.rows, live.series, live.xxh64, fr.rows, fr.series,
           fr.xxh64, same(live, fr) ? "SAME" : "DIFFERENT");
    CHECK(same(live, fr));
    free(cuts);
    set_free(s, n);
    tw_close(&x);
    atomic_store(&clock_now, T0 + 2 * DAY);
}

/* The digest of one fixed set (a replica's retired copies) before and after the key's parts are dropped: the job runs
 * on the keep thread at any later time (or after a restart), when the listing below the set is another; it takes the
 * leader's tombstone decision (MKD_TOMBS) and gives the leader's digest (review F2; mutant digestlocalbelow) */
static void case_dropafter(void)
{
    tw x;
    tomb_twin(&x, "rv_drop", false, false);
    part **s;
    int64_t k = key0(&x);
    size_t n = tw_set(&x, k, false, &s);
    ec_ldigest before, after;
    CHECK(ec_logical_digest(x.db, s, n, k, atomic_load(&clock_now), &before) == 0);
    CHECK(before.tombs);
    ecd_cut *cuts = NULL;
    size_t nc = 0;
    CHECK(ecd_freeze(x.db, s, n, k, atomic_load(&clock_now), &cuts, &nc) == 0);
    CHECK(eng_drop_before(x.db, T0 + 2 * DAY) == 0);
    CHECK(ec_logical_digest_frozen(x.db, s, n, k, cuts, nc, before.tombs, &after) == 0);
    free(cuts);
    printf("dropafter: %zu parts; before %016" PRIx64 " after %016" PRIx64 " -> %s\n", n, before.xxh64,
           after.xxh64, same(before, after) ? "SAME" : "DIFFERENT");
    CHECK(same(before, after));
    set_free(s, n);
    tw_close(&x);
}

/* Nothing listed below the set: its tombstones hide only its own rows, and a twin merged without them gives the same
 * digest (mutant digestbelowtrue: the term hashed anyway) */
static void case_nobelow(void)
{
    tw a, b;
    for (int m = 0; m < 2; m++) {
        tw *x = m ? &b : &a;
        tw_open(x, m ? "nobelow_b" : "nobelow_a", false);
        write_range(x, T0, T0 + HOUR);
        write_range(x, T0 + HOUR, T0 + 2 * HOUR);
        CHECK(eng_delete_range(x->db, x->s[SA], T0 + 600000, T0 + HOUR + 600000) == 0);
        if (m)
            CHECK(eng_optimize(x->db) == 0); /* the whole key: nothing older, the tombstones go */
    }
    ec_ldigest da = tw_digest(&a, false), db = tw_digest(&b, false);
    CHECK(!da.tombs && !db.tombs);
    CHECK(same(da, db));
    printf("nobelow: %zu and %zu parts, nothing below: one digest\n", count_parts(&a, false), count_parts(&b, false));
    tw_close(&a), tw_close(&b);
}

/* Two sets that differ only in one row's time give two digests (mutant digestnots) */
static void case_time(void)
{
    tw a, b;
    ec_ldigest d[2];
    for (int m = 0; m < 2; m++) {
        tw *x = m ? &b : &a;
        tw_open(x, m ? "time_b" : "time_a", false);
        int64_t ts[3] = {T0, T0 + 1000, T0 + (m ? 3000 : 2000)};
        float v[3] = {1, 2, 3};
        CHECK(eng_insert_series(x->db, TSDB_FLOAT32, x->s[SA], ts, v, 3) == 0);
        CHECK(eng_flush(x->db) == 0);
        d[m] = tw_digest(x, false);
    }
    CHECK(d[0].rows == 3 && d[1].rows == 3 && !same(d[0], d[1]));
    printf("time: a row moved in time changes the digest\n");
    tw_close(&a), tw_close(&b);
}
#endif

int main(int argc, char **argv)
{
#ifndef TSDB_EC
    (void)argc;
    (void)argv;
    printf("test_ecdigest: skipped (built without TSDB_EC)\n");
    return 0;
#else
    const char *base = getenv("TMPDIR") ? getenv("TMPDIR") : "/tmp";
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--mutant") && i + 1 < argc) {
            if ((ecdigest_mutant = ecdigest_mutant_parse(argv[++i])) < 0) {
                fprintf(stderr, "unknown mutant %s\n", argv[i]);
                return 2;
            }
        } else {
            base = argv[i];
        }
    }
    snprintf(scratch, sizeof scratch, "%s/test_ecdigest.XXXXXX", base);
    if (!mkdtemp(scratch)) {
        perror("mkdtemp");
        return 1;
    }
    case_layouts();
    case_retention();
    case_tombstones();
    case_mixed();
    case_tombonly();
    case_dropafter();
    case_nobelow();
    case_time();
    if (failures == 0)
        rm_rf(scratch);
    printf("test_ecdigest: %s (%d failures)\n", failures ? "FAILED" : "ok", failures);
    return failures ? 1 : 0;
#endif
}
