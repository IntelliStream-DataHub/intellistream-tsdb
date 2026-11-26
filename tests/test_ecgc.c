/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Retired .ec parts and their objects (src/ecgc.c, r7-gc). The objects are real: a settle by hand (chunk_be_put_object,
 * ec_index_write, part_open_ec, ec_swap_commit), so the .ec files are on disk and survive a reopen.
 *
 *   hold     obsolete_part_lifetime_s 0 and 1: a part a reader holds across its drop and deadline keeps its chunks and
 *            its reads succeed; once it lets go, the chunks go, then the retired name. Before the deadline nothing goes.
 *   sweep    an open queues the objects no file names (an abandoned settle's, one attached in memory only) and never a
 *            listed .ec's, a retired one's, or one a settle puts after the open; ecgc_queue_object.
 *   down     a store DOWN at the purge: the entry is kept, nothing unlinked; with the store back every chunk and the
 *            name go.
 *   foreign  another database on the same stores: its open's sweep and its deletes leave this one's object.
 *   unlisted a swap refused (its source dropped meanwhile) queues its object: the chunks go without a reopen.
 *   midround every store fails in service between two positions' deletes: the name stays; a reopen finishes.
 *   retry    a transient delete error keeps an entry, and the worker's own timer retries it.
 *   power    power lost (crashfs, torn at random) at every step of an open (its sweep) and a purge: a listed object
 *            keeps every chunk, a retired name stays until its object's chunks are all gone; a clean open then removes
 *            the retired object and the orphan.
 *
 * Mutants (--mutant NAME; the Makefile requires each to fail): gcunlinkfirst gcunrefleak gcnohold sweeplive
 * sweepinflight sweepforeign unlistednoop noretrytimer; noafter too is accepted, but survives (src/ecgc.h).
 */
#include "internal.h"
#include "chunk.h"
#include "chunk_priv.h"
#include "coldops.h"
#include "eccache.h"
#include "ecgc.h"
#include "ecgcmk.h"
#include "eckeep.h"
#include "ecpart.h"
#include "ecswap.h"
#include "marker.h"
#include "replica.h"
#include "settle.h"
#include "tier.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static int failures;
#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);                                               \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

static const struct {
    const char *name;
    int m;
} MUTANTS[] = {
    {"none", GC_MUT_NONE},
    {"gcunlinkfirst", GC_MUT_UNLINKFIRST},
    {"gcunrefleak", GC_MUT_UNREFLEAK},
    {"gcnohold", GC_MUT_NOHOLD},
    {"sweeplive", GC_MUT_SWEEPLIVE},
    {"sweepinflight", GC_MUT_SWEEPINFLIGHT},
    {"sweepforeign", GC_MUT_SWEEPFOREIGN},
    {"unlistednoop", GC_MUT_UNLISTEDNOOP},
    {"noretrytimer", GC_MUT_NORETRYTIMER},
    {"noafter", GC_MUT_NOAFTER},
    {"gcuncommitted", GC_MUT_UNCOMMITTED},
    {"gcnamedorphan", GC_MUT_NAMEDORPHAN},
    {"gcsuspendignored", GC_MUT_SUSPENDIGNORED},
    {"holdatretire", GC_MUT_HOLDATRET},
    {"gcreplicanoop", GC_MUT_REPLICANOOP},
};

static const char *mutant_name(int m)
{
    for (size_t i = 0; i < sizeof MUTANTS / sizeof *MUTANTS; i++)
        if (MUTANTS[i].m == m)
            return MUTANTS[i].name;
    return "none";
}

#ifdef TSDB_EC
#define K 2
#define M 1
#define NST 3
#define ROWS 20000
static const int64_t DAY = 86400000;
static char S[PATH_MAX], SELF[PATH_MAX], SHIM[PATH_MAX];
static bool quick;

static int sh(const char *fmt, ...)
{
    char cmd[4 * PATH_MAX];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(cmd, sizeof cmd, fmt, ap);
    va_end(ap);
    return system(cmd);
}

/* dir/db and dir/st0 .. st<NST-1>, each store initialized */
static void fresh(const char *what, char *dir, size_t cap)
{
    snprintf(dir, cap, "%s/%s", S, what);
    sh("rm -rf '%s'; mkdir -p '%s/db'", dir, dir);
    for (int i = 0; i < NST; i++) {
        char st[PATH_MAX + 16];
        snprintf(st, sizeof st, "%s/st%d", dir, i);
        CHECK(mkdir(st, 0755) == 0);
        uint8_t id[16];
        CHECK(chunk_store_init(st, NULL, id) == 0);
    }
}

typedef struct h {
    tier *t;
    eng *db;
    uint_fast64_t rounds; /* settle_rounds once the tier's settle thread was stopped */
} h;

static bool use_thread; /* the tier's worker works the queue (thread_case); else the test does */

/* the stores dir/st0.. and the database db (another database on the same stores: foreign_case) */
static int h_open_at(h *x, const char *dir, const char *db, int lifetime_s)
{
    char spec[8192];
    int n = snprintf(spec, sizeof spec, "k=%d,m=%d,cache_mb=64,dirs=", K, M);
    for (int i = 0; i < NST; i++)
        n += snprintf(spec + n, sizeof spec - (size_t)n, "%s%s/st%d", i ? ":" : "", dir, i);
    setenv("TSDB_TEST_TIER", spec, 1);
    setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);
    x->t = NULL;
    x->db = NULL;
    x->rounds = atomic_load(&settle_rounds);
    atomic_store(&ecgc_test_manual, !use_thread);
    int rc = tier_open(db, NULL, &x->t);
    unsetenv("TSDB_TEST_TIER");
    unsetenv("TSDB_TEST_TIER_I_KNOW");
    if (rc)
        return rc;
    /* The days written here are from 1970, due at once: the tier's settle thread (r7-settle) would settle them beside
     * the case's own steps, and its references (a swap's warmer cut, its own pins) hold an object the case expects gone
     * (midround failed so under TSan). Settles are test_settle's, with r7-gc's handoffs (gcpaths, gcpower). */
    ecsettle_tier_quiesce(x->t);
    x->rounds = atomic_load(&settle_rounds);
    /* So is each engine's digest thread (r7-keep): a swap queues its settled_ source, and the digest references the
     * .ec part across its reads of the object, so a drop and purge while it runs find the object held and queue
     * nothing (midround failed so under TSan). That reference is test_keep's (digestnohold). */
    atomic_store(&eckeep_test_pause, true);
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.partition_classes = 0; /* one key per day */
    o.obsolete_part_lifetime_s = lifetime_s;
    tier_want(x->t, 0);
    rc = eng_open(db, &o, &x->db);
    tier_want(NULL, 0);
    if (rc) {
        tier_put(x->t);
        x->t = NULL;
    }
    return rc;
}

static int h_open(h *x, const char *dir, int lifetime_s)
{
    char db[PATH_MAX + 8];
    snprintf(db, sizeof db, "%s/db", dir);
    return h_open_at(x, dir, db, lifetime_s);
}

static void h_close(h *x)
{
    CHECK(atomic_load(&settle_rounds) == x->rounds); /* no settle thread ran beside the case */
    if (x->db) {
        CHECK(eckeep_test_tries(x->db) == 0); /* nor a digest */
        eng_close(x->db);
    }
    tier_put(x->t);
    x->db = NULL;
    x->t = NULL;
}

/* purge, delete, purge: the retired names whose chunks went are unlinked */
static void h_gc(h *x)
{
    graveyard_purge(x->db, false);
    ecgc_tier_work(x->t);
    graveyard_purge(x->db, false);
}

static void write_day(eng *db, int d)
{
    uint32_t s;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    static int64_t ts[ROWS];
    static float v[ROWS];
    for (int i = 0; i < ROWS; i++)
        ts[i] = d * DAY + (int64_t)i * 3000, v[i] = (float)((d * 7919 + i) % 1013);
    CHECK(eng_insert_series(db, TSDB_FLOAT32, s, ts, v, ROWS) == 0);
    CHECK(eng_flush(db) == 0);
}

/* the one listed part of day d, referenced */
static part *day_part(eng *db, int d)
{
    part *r = NULL;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++)
        if (db->parts[i]->partition == d) {
            CHECK(r == NULL);
            r = db->parts[i];
        }
    if (r)
        part_ref(r);
    pthread_mutex_unlock(&db->lock);
    CHECK(r != NULL);
    return r;
}

/* the positions of each object made here (k' + m: a small object has a smaller k') */
static struct {
    uint8_t oid[16];
    int n;
} known[64];
static int nknown;

static void note(const uint8_t oid[16], int n)
{
    if (nknown < 64) {
        memcpy(known[nknown].oid, oid, 16);
        known[nknown++].n = n;
    }
}

static int npos(const uint8_t oid[16])
{
    for (int i = 0; i < nknown; i++)
        if (!memcmp(known[i].oid, oid, 16))
            return known[i].n;
    return -1;
}

/* an object of the tier from bytes (random oid): what a settle puts before any file names it */
static void put_object(h *x, const uint8_t *b, size_t len, uint8_t oid[16])
{
    chunk_geom g;
    CHECK(chunk_geom_derive(&g, K, M, len) == 0);
    uint64_t *sums = malloc(chunk_sums_total(&g) * sizeof *sums);
    chunk_random(oid, 16);
    CHECK(chunk_be_see_epoch(tier_backend(x->t), x->db->tier_vol, 1) == 0);
    CHECK(chunk_be_put_object(tier_backend(x->t), x->db->tier_vol, tier_vuuid(x->db), oid, &g, 1, b, ~0u, sums) == 0);
    note(oid, (int)(g.k + g.m));
    free(sums);
}

/* a settle by hand of day d's .part: the object, the .ec file, the swap. Its oid into oid; the .ec name into name.
 * drop_first: day d is dropped before the swap, which is then refused (the swap's rc is returned). */
static int settle_day_ex(h *x, int d, uint8_t oid[16], char name[96], bool drop_first)
{
    eng *db = x->db;
    part *p = day_part(db, d);
    int rc = TSDB_ERR_NOTFOUND;
    if (!p)
        return rc;
    chunk_geom g;
    CHECK(chunk_geom_derive(&g, K, M, p->map_len) == 0);
    ec_record r;
    memset(&r, 0, sizeof r);
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
    uint64_t ep = coldvol_epoch(tier_cold(db));
    if (ep == 0)
        ep = 1;
    r.writer_epoch = ep;
    r.settle_ms = eng_now_ms(db);
    snprintf(r.source, sizeof r.source, "%s", p->name);
    r.nsums = chunk_sums_total(&g);
    r.sums = malloc(r.nsums * sizeof *r.sums);
    CHECK(chunk_be_see_epoch(tier_backend(x->t), db->tier_vol, ep) == 0);
    CHECK(chunk_be_put_object(tier_backend(x->t), db->tier_vol, tier_vuuid(db), r.oid, &g, ep, p->map, ~0u, r.sums) ==
          0);
    char tmp[96];
    CHECK(ec_index_write(db, p, &r, tmp) == 0);
    snprintf(name, 96, "%.*s.ec", (int)(strlen(p->name) - 5), p->name);
    part *e = NULL;
    CHECK(part_open_ec(db, tmp, name, &e) == 0);
    if (e) {
        CHECK(part_set_pending(e, tmp) == 0);
        part *in[1] = {p}, *ec[1] = {e};
        if (drop_first)
            CHECK(eng_drop_before(db, (int64_t)(d + 1) * DAY) == 0);
        rc = ec_swap_commit(db, in, ec, 1);
        part_unref(e);
    }
    memcpy(oid, r.oid, 16);
    note(oid, (int)(g.k + g.m));
    free(r.sums);
    part_unref(p);
    return rc;
}

static void settle_day(h *x, int d, uint8_t oid[16], char name[96]) { CHECK(settle_day_ex(x, d, oid, name, false) == 0); }

/* chunk files of oid in every store */
static int chunks(const char *dir, const uint8_t oid[16])
{
    char hex[33];
    chunk_oid_hex(oid, hex);
    int n = 0;
    for (int i = 0; i < NST; i++) {
        char sd[PATH_MAX + 32];
        snprintf(sd, sizeof sd, "%s/st%d", dir, i);
        DIR *d1 = opendir(sd);
        struct dirent *a;
        while (d1 && (a = readdir(d1))) {
            if (a->d_name[0] != 's' || !strcmp(a->d_name, "st"))
                continue;
            char vd[2 * PATH_MAX];
            snprintf(vd, sizeof vd, "%s/%s", sd, a->d_name);
            DIR *d2 = opendir(vd);
            struct dirent *b;
            while (d2 && (b = readdir(d2)))
                n += strstr(b->d_name, hex) != NULL && strncmp(b->d_name, "tmp", 3) != 0;
            if (d2)
                closedir(d2);
        }
        if (d1)
            closedir(d1);
    }
    return n;
}

static bool exists(const char *dir, const char *name)
{
    char p[2 * PATH_MAX];
    snprintf(p, sizeof p, "%s/db/%s", dir, name);
    return access(p, F_OK) == 0;
}

/* every granule of p through its object (the cache emptied first): 0 and the rows, or the first error */
static int read_all(h *x, part *p, uint64_t *rows)
{
    eccache_test_drop(x->t);
    codec_ctx *cx = codec_new();
    static int64_t ts[8192 + 64];
    static float v[8192 + 64];
    int rc = 0;
    *rows = 0;
    for (uint32_t i = 0; i < p->ix.n && rc == 0; i++) {
        granule g;
        rc = granule_read_at(cx, p, i, TSDB_FLOAT32, ts, v, 8192 + 64, false, &g);
        if (rc == 0)
            *rows += gx_count(p, i);
    }
    codec_free(cx);
    return rc;
}

/* ------------------------------------------------------------ hold */

static void hold_case(int lifetime)
{
    char dir[PATH_MAX], name[96], rname[128];
    uint8_t oid[16], oid2[16];
    fresh("hold", dir, sizeof dir);
    h x;
    CHECK(h_open(&x, dir, lifetime) == 0);
    if (!x.db)
        return;
    write_day(x.db, 0);
    write_day(x.db, 1);
    write_day(x.db, 2);
    settle_day(&x, 0, oid, name);
    settle_day(&x, 1, oid2, name); /* name: day 1's */
    CHECK(chunks(dir, oid) == npos(oid));
    part *e = day_part(x.db, 0); /* a reader's reference, as a query's snapshot holds */
    CHECK(e && atomic_load(&e->ec) && !atomic_load(&e->ec)->test_mark);
    if (!e)
        return;
    snprintf(rname, sizeof rname, "retired_%s", e->name);
    CHECK(eng_drop_before(x.db, DAY) == 0);
    CHECK(exists(dir, rname));
    if (lifetime > 0)
        usleep((useconds_t)lifetime * 1000000 + 200000); /* the deadline passes while the reader holds the part */
    h_gc(&x);
    h_gc(&x);
    CHECK(chunks(dir, oid) == npos(oid)); /* held */
    CHECK(exists(dir, rname));
    uint64_t rows = 0;
    CHECK(read_all(&x, e, &rows) == 0 && rows == e->rows);
    part_unref(e);
    h_gc(&x);
    CHECK(chunks(dir, oid) == 0);
    CHECK(!exists(dir, rname));
    ecgc_counters c;
    ecgc_get_counters(x.t, &c);
    CHECK(c.unlinked == 1 && c.pending == 0 && c.holds == (ecgc_mutant == GC_MUT_HOLDATRET ? 0u : 1u));

    /* no reader: the deadline alone */
    char rname2[128];
    snprintf(rname2, sizeof rname2, "retired_%s", name);
    CHECK(eng_drop_before(x.db, 2 * DAY) == 0);
    CHECK(exists(dir, rname2));
    if (lifetime > 0) {
        h_gc(&x);
        CHECK(chunks(dir, oid2) == npos(oid2) && exists(dir, rname2)); /* not due */
        usleep((useconds_t)lifetime * 1000000 + 200000);
    }
    h_gc(&x);
    CHECK(chunks(dir, oid2) == 0 && !exists(dir, rname2));
    ecgc_get_counters(x.t, &c);
    CHECK(c.holds == 0);
    h_close(&x);
    printf("  hold, lifetime %d s: a held part's chunks outlive its drop and deadline, its reads succeed; then the "
           "chunks go, then the name\n", lifetime);
}

/* ------------------------------------------------------------ sweep */

static void sweep_case(void)
{
    char dir[PATH_MAX], na[96], nb[96];
    uint8_t oa[16], ob[16], orphan[16], attached[16], late[16], queued[16];
    fresh("sweep", dir, sizeof dir);
    h x;
    CHECK(h_open(&x, dir, 3600) == 0);
    if (!x.db)
        return;
    for (int d = 0; d < 4; d++)
        write_day(x.db, d);
    settle_day(&x, 0, ob, nb); /* retired below, kept for its lifetime */
    settle_day(&x, 2, oa, na); /* stays listed */
    CHECK(eng_drop_before(x.db, DAY) == 0);
    char rb[128];
    snprintf(rb, sizeof rb, "retired_%s", nb);
    CHECK(exists(dir, rb));
    static uint8_t junk[300000];
    for (size_t i = 0; i < sizeof junk; i++)
        junk[i] = (uint8_t)(i * 31 + 7);
    put_object(&x, junk, sizeof junk, orphan); /* an abandoned settle */
    part *p3 = day_part(x.db, 3);
    ec_record rec;
    memset(&rec, 0, sizeof rec);
    CHECK(p3 && ec_test_attach(x.db, p3, &rec) == 0); /* in memory only: an orphan after the reopen */
    memcpy(attached, rec.oid, 16);
    note(attached, rec.k + rec.m);
    free(rec.sums);
    part_unref(p3);
    h_close(&x);

    CHECK(h_open(&x, dir, 3600) == 0);
    if (!x.db)
        return;
    put_object(&x, junk, sizeof junk, late); /* a settle that began after the open */
    ecgc_counters c;
    ecgc_get_counters(x.t, &c);
    CHECK(c.sweeps == 1 && c.sweeps_skipped == 0);
    h_gc(&x);
    ecgc_get_counters(x.t, &c);
    CHECK(c.swept == 2);
    CHECK(chunks(dir, orphan) == 0 && chunks(dir, attached) == 0);
    CHECK(chunks(dir, oa) == npos(oa) && chunks(dir, ob) == npos(ob) && exists(dir, rb));
    CHECK(chunks(dir, late) == npos(late));
    /* the late settle abandoned: it queues its own object */
    CHECK(ecgc_queue_object(x.db, late, (unsigned)npos(late)) == 0);
    memcpy(queued, late, 16);
    ecgc_tier_work(x.t);
    CHECK(chunks(dir, queued) == 0);
    uint64_t rows = 0;
    part *pa = day_part(x.db, 2);
    CHECK(pa && read_all(&x, pa, &rows) == 0 && rows == ROWS);
    part_unref(pa);
    h_close(&x);
    printf("  sweep: an open queued %" PRIu64 " orphans (an abandoned settle's, an attached object's) and left the "
           "listed, the retired and a later settle's object\n", c.swept);
}

/* ------------------------------------------------------------ down */

static void down_case(void)
{
    char dir[PATH_MAX], name[96], rname[128];
    uint8_t oid[16];
    fresh("down", dir, sizeof dir);
    h x;
    CHECK(h_open(&x, dir, 0) == 0);
    if (!x.db)
        return;
    write_day(x.db, 0);
    write_day(x.db, 1);
    settle_day(&x, 0, oid, name);
    snprintf(rname, sizeof rname, "retired_%s", name);
    CHECK(eng_drop_before(x.db, DAY) == 0);
    h_close(&x); /* its close queued the object, which the tier's close left: the name stays */
    CHECK(exists(dir, rname) && chunks(dir, oid) == npos(oid));
    sh("mv '%s/st2' '%s/st2.away'", dir, dir);
    CHECK(h_open(&x, dir, 0) == 0);
    if (!x.db)
        return;
    h_gc(&x);
    h_gc(&x);
    ecgc_counters c;
    ecgc_get_counters(x.t, &c);
    CHECK(c.sweeps_skipped == 1 && c.retries >= 1 && c.deleted == 0);
    CHECK(exists(dir, rname));
    h_close(&x);
    sh("mv '%s/st2.away' '%s/st2'", dir, dir);
    CHECK(exists(dir, rname) && chunks(dir, oid) >= 1);
    CHECK(h_open(&x, dir, 0) == 0);
    if (!x.db)
        return;
    h_gc(&x);
    CHECK(chunks(dir, oid) == 0 && !exists(dir, rname));
    h_close(&x);
    printf("  down: a store DOWN keeps the entry and the name; back UP, every chunk goes, then the name\n");
}

/* ------------------------------------------------------------ thread */

/* the tier's own worker: a drop, and the chunks and the name go with no ecgc_tier_work from the test */
static void thread_case(void)
{
    char dir[PATH_MAX], name[96], rname[128];
    uint8_t oid[16];
    fresh("thread", dir, sizeof dir);
    use_thread = true;
    h x;
    CHECK(h_open(&x, dir, 0) == 0);
    use_thread = false;
    if (!x.db)
        return;
    write_day(x.db, 0);
    write_day(x.db, 1);
    settle_day(&x, 0, oid, name);
    snprintf(rname, sizeof rname, "retired_%s", name);
    CHECK(eng_drop_before(x.db, DAY) == 0);
    int ms = 0;
    for (; ms < 20000 && (chunks(dir, oid) != 0 || exists(dir, rname)); ms += 10) {
        graveyard_purge(x.db, false); /* the flusher's, sooner */
        usleep(10000);
    }
    CHECK(chunks(dir, oid) == 0 && !exists(dir, rname));
    h_close(&x);
    printf("  thread: the tier's worker deleted a dropped part's object, then the purge its name (%d ms)\n", ms);
}

/* ------------------------------------------------------------ foreign */

/* Two databases on one set of stores (an operator's mistake): each one's volume 0 lists the other's objects, which
 * neither names. B's sweep must leave A's object (its headers name A's vuuid), and so must a delete of it by B. */
static void foreign_case(void)
{
    char dir[PATH_MAX], name[96], dbb[PATH_MAX + 8];
    uint8_t oid[16];
    fresh("foreign", dir, sizeof dir);
    snprintf(dbb, sizeof dbb, "%s/dbB", dir);
    h a, b;
    CHECK(h_open(&a, dir, 3600) == 0);
    if (!a.db)
        return;
    write_day(a.db, 0);
    write_day(a.db, 1);
    settle_day(&a, 0, oid, name);
    h_close(&a);
    CHECK(chunks(dir, oid) == npos(oid));
    CHECK(h_open_at(&b, dir, dbb, 3600) == 0);
    if (!b.db)
        return;
    ecgc_counters c;
    ecgc_get_counters(b.t, &c);
    CHECK(c.sweeps == 1 && c.sweeps_skipped == 0 && c.swept == 0);
    h_gc(&b);
    CHECK(chunks(dir, oid) == npos(oid));
    CHECK(ecgc_queue_object(b.db, oid, (unsigned)npos(oid)) == 0); /* the backstop: deletes check the vuuid */
    ecgc_tier_work(b.t);
    CHECK(chunks(dir, oid) == npos(oid));
    h_close(&b);
    CHECK(h_open(&a, dir, 3600) == 0);
    if (!a.db)
        return;
    uint64_t rows = 0;
    part *p = day_part(a.db, 0);
    CHECK(p && atomic_load(&p->ec) && read_all(&a, p, &rows) == 0 && rows == ROWS);
    if (p)
        part_unref(p);
    h_close(&a);
    printf("  foreign: another database on the same stores neither swept nor deleted this one's object\n");
}

/* ------------------------------------------------------------ unlisted */

/* A swap refused because its source was dropped meanwhile hands its object to the queue: the chunks go with no reopen. */
static void unlisted_case(void)
{
    char dir[PATH_MAX], name[96];
    uint8_t oid[16];
    fresh("unlisted", dir, sizeof dir);
    h x;
    CHECK(h_open(&x, dir, 3600) == 0);
    if (!x.db)
        return;
    write_day(x.db, 0);
    write_day(x.db, 1);
    CHECK(settle_day_ex(&x, 0, oid, name, true) != 0);
    CHECK(chunks(dir, oid) == npos(oid) && !exists(dir, name));
    ecgc_tier_work(x.t);
    CHECK(chunks(dir, oid) == 0);
    h_close(&x);
    printf("  unlisted: a refused swap's object was deleted without a reopen\n");
}

/* ------------------------------------------------------------ midround, retry */

static void fail_all_stores(tier *t, unsigned pos)
{
    if (pos != 0)
        return;
    for (int i = 0; i < NST; i++) {
        chunk_store *st = chunk_be_store(tier_backend(t), i);
        if (st)
            chunk_store_test_fail(st);
    }
}

/* every store fails in service after position 0 is deleted: the name and the other chunks stay; after a reopen they go */
static void midround_case(void)
{
    char dir[PATH_MAX], name[96], rname[128];
    uint8_t oid[16];
    fresh("midround", dir, sizeof dir);
    h x;
    CHECK(h_open(&x, dir, 0) == 0);
    if (!x.db)
        return;
    write_day(x.db, 0);
    write_day(x.db, 1);
    settle_day(&x, 0, oid, name);
    snprintf(rname, sizeof rname, "retired_%s", name);
    CHECK(eng_drop_before(x.db, DAY) == 0);
    ecgc_test_after_del = fail_all_stores;
    h_gc(&x);
    ecgc_test_after_del = NULL;
    CHECK(exists(dir, rname) && chunks(dir, oid) == npos(oid) - 1);
    h_close(&x);
    CHECK(h_open(&x, dir, 0) == 0);
    if (!x.db)
        return;
    h_gc(&x);
    CHECK(chunks(dir, oid) == 0 && !exists(dir, rname));
    h_close(&x);
    printf("  midround: stores failing between two deletes kept the name; the next open finished the object\n");
}

/* a transient error keeps an entry: the worker tries it again by itself (its timer: nothing else kicks it) */
static void retry_case(void)
{
    char dir[PATH_MAX], name[96], rname[128];
    uint8_t oid[16];
    fresh("retry", dir, sizeof dir);
    use_thread = true;
    h x;
    CHECK(h_open(&x, dir, 0) == 0);
    use_thread = false;
    if (!x.db)
        return;
    write_day(x.db, 0);
    write_day(x.db, 1);
    settle_day(&x, 0, oid, name);
    snprintf(rname, sizeof rname, "retired_%s", name);
    atomic_store(&ecgc_test_del_fail, 1);
    CHECK(eng_drop_before(x.db, DAY) == 0);
    int ms = 0;
    for (; ms < 10000 && (chunks(dir, oid) != 0 || exists(dir, rname)); ms += 10) {
        graveyard_purge(x.db, false);
        usleep(10000);
    }
    ecgc_counters c;
    ecgc_get_counters(x.t, &c);
    CHECK(chunks(dir, oid) == 0 && !exists(dir, rname) && c.retries >= 1);
    atomic_store(&ecgc_test_del_fail, 0);
    h_close(&x);
    printf("  retry: an entry a transient error kept was deleted by the worker's own retry (%d ms)\n", ms);
}

/* ------------------------------------------------------------ power */

static char PBASE[PATH_MAX], PNAME[128];
static uint8_t P_RET[16], P_LIST[16], P_ORPH[16];

static void make_power_base(void)
{
    char name[96];
    fresh("pbase", PBASE, sizeof PBASE);
    h x;
    CHECK(h_open(&x, PBASE, 0) == 0);
    if (!x.db)
        return;
    write_day(x.db, 0);
    write_day(x.db, 1);
    settle_day(&x, 1, P_LIST, name);
    settle_day(&x, 0, P_RET, name);
    snprintf(PNAME, sizeof PNAME, "retired_%s", name);
    static uint8_t junk[200000];
    memset(junk, 0x5a, sizeof junk);
    put_object(&x, junk, sizeof junk, P_ORPH);
    CHECK(eng_drop_before(x.db, DAY) == 0);
    h_close(&x);
    CHECK(exists(PBASE, PNAME) && chunks(PBASE, P_RET) == npos(P_RET) && chunks(PBASE, P_ORPH) == npos(P_ORPH));
}

static int child(const char *dir)
{
    h x;
    if (h_open(&x, dir, 0) != 0)
        return 3;
    h_gc(&x);
    h_close(&x);
    return 0;
}

static int child_gcmk(const char *dir);

static int run_child_mode(const char *flag, const char *dir, int warmup, int seed)
{
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512], w[16], sd[16];
        snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0:detect_leaks=0",
                 getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
        setenv("ASAN_OPTIONS", asan, 1);
        snprintf(w, sizeof w, "%d", warmup);
        snprintf(sd, sizeof sd, "%d", seed);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", dir, 1);
        setenv("CRASHFS_PROB", "1", 1);
        setenv("CRASHFS_WARMUP", w, 1);
        setenv("CRASHFS_AT_EXIT", "1", 1);
        setenv("CRASHFS_TEAR", "1", 1);
        setenv("CRASHFS_SEED", sd, 1);
        int null = open("/dev/null", O_WRONLY);
        if (null >= 0) {
            dup2(null, 1);
            dup2(null, 2);
        }
        execl(SELF, "test_ecgc", flag, dir, "--mutant", mutant_name(ecgc_mutant), (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

static int run_child(const char *dir, int warmup, int seed) { return run_child_mode("--child", dir, warmup, seed); }

static void power_case(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  power loss: skipped (no %s)\n", SHIM);
        return;
    }
    make_power_base();
    char dir[PATH_MAX];
    snprintf(dir, sizeof dir, "%s/pl", S);
    int crashes = 0, seeds = quick ? 2 : 4;
    bool finished = false;
    for (int k = 0; k < 3000 && !finished; k++) {
        for (int sd = 0; sd < seeds; sd++) {
            sh("rm -rf '%s'; cp -a '%s' '%s'", dir, PBASE, dir);
            int code = run_child(dir, k, k * 7 + sd + 1);
            CHECK(code == 99 || code == 0);
            if (code != 99 && code != 0) {
                fprintf(stderr, "  (power loss at step %d: the child said %d)\n", k, code);
                k = 1 << 20;
                break;
            }
            crashes += code == 99;
            finished = code == 0;
            /* what the power left: the listed object whole; the retired name until its chunks are all gone */
            int lc = chunks(dir, P_LIST), rc = chunks(dir, P_RET);
            bool named = exists(dir, PNAME);
            CHECK(lc == npos(P_LIST));
            CHECK(named || rc == 0);
            if (lc != npos(P_LIST) || !(named || rc == 0)) {
                fprintf(stderr, "  (power loss at step %d, seed %d: listed %d chunks, retired %d chunks, name %d)\n",
                        k, sd, lc, rc, named);
                k = 1 << 20;
                break;
            }
            /* a clean open finishes the job */
            h x;
            CHECK(h_open(&x, dir, 0) == 0);
            if (x.db) {
                h_gc(&x);
                h_close(&x);
            }
            CHECK(chunks(dir, P_RET) == 0 && !exists(dir, PNAME) && chunks(dir, P_ORPH) == 0);
            CHECK(chunks(dir, P_LIST) == npos(P_LIST));
        }
    }
    CHECK(finished);
    printf("  power: lost at every step of an open (its sweep) and a purge, %d power losses: the listed object whole, "
           "the retired name kept until its chunks went\n", crashes);
}

/* ------------------------------------------------------------ reopen hold (r7-gc F5) + O(1) 10k holds */

static void reopen_hold_case(void)
{
    char dir[PATH_MAX], dbpath[PATH_MAX + 8], name[96], rname[128];
    uint8_t oid[16];
    fresh("reopen_hold", dir, sizeof dir);
    snprintf(dbpath, sizeof dbpath, "%s/db", dir);
    h x;
    CHECK(h_open(&x, dir, 0) == 0);
    if (!x.db)
        return;
    write_day(x.db, 0);
    write_day(x.db, 1);
    settle_day(&x, 0, oid, name);
    snprintf(rname, sizeof rname, "retired_%s", name);
    /* A reader on the OLD engine keeps a reference across a reopen: old_p is never retired on x.db! */
    part *old_p = day_part(x.db, 0);
    CHECK(old_p != NULL);
    if (!old_p) {
        h_close(&x);
        return;
    }
    eng_halt(x.db);
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.partition_classes = 0;
    o.obsolete_part_lifetime_s = 0;
    tier_want(x.t, 0);
    eng *neu = NULL;
    CHECK(eng_reopen(x.db, dbpath, &o, NULL, &neu, NULL, NULL) == 0 && neu != NULL);
    tier_want(NULL, 0);
    if (neu) {
        /* New engine drops and retires day 0: new_p reaches refcount 0, but old_p still holds oid! */
        CHECK(eng_drop_before(neu, DAY) == 0);
        CHECK(exists(dir, rname));
        graveyard_purge(neu, false);
        ecgc_tier_work(x.t);
        graveyard_purge(neu, false);
        CHECK(chunks(dir, oid) == npos(oid)); /* fails with holdatretire (r7-gc F5)! */
        CHECK(exists(dir, rname));
        uint64_t rows = 0;
        CHECK(read_all(&x, old_p, &rows) == 0 && rows == old_p->rows);
        /* Close old engine (releasing its list ref); old_p still holds the reader's ref. */
        eng_close(x.db);
        x.db = NULL;
        graveyard_purge(neu, false);
        ecgc_tier_work(x.t);
        graveyard_purge(neu, false);
        CHECK(chunks(dir, oid) == npos(oid));
        /* Reader lets go of old_p: now chunks and retired_ name go. */
        part_unref(old_p);
        graveyard_purge(neu, false);
        ecgc_tier_work(x.t);
        graveyard_purge(neu, false);
        CHECK(chunks(dir, oid) == 0 && !exists(dir, rname));
        eng_close(neu);
    } else {
        part_unref(old_p);
        eng_close(x.db);
        x.db = NULL;
    }

    /* O(1) hash check with 10,000 parts: hold, lookup, release in O(1) per op. */
    enum { NH = 10000 };
    part *pts = calloc(NH, sizeof *pts);
    uint8_t (*oids)[16] = malloc(NH * 16);
    CHECK(pts && oids);
    if (pts && oids) {
        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);
        for (size_t i = 0; i < NH; i++) {
            memset(oids[i], 0, 16);
            memcpy(oids[i], &i, sizeof i);
            struct ecpart *ep = calloc(1, sizeof *ep);
            ep->tier = x.t;
            ep->volume = 0;
            memcpy(ep->rec.oid, oids[i], 16);
            atomic_init(&pts[i].ec, ep);
            CHECK(ecgc_hold(x.t, &pts[i], 0, oids[i]) == 0);
        }
        ecgc_counters c;
        ecgc_get_counters(x.t, &c);
        CHECK(c.holds == NH);
        for (size_t i = 0; i < NH; i++)
            CHECK(ecgc_held(x.t, 0, oids[i]));
        for (size_t i = 0; i < NH; i++) {
            ecgc_last_unref(&pts[i]);
            if (ecgc_mutant != GC_MUT_UNREFLEAK)
                CHECK(!ecgc_held(x.t, 0, oids[i]));
        }
        clock_gettime(CLOCK_MONOTONIC, &t1);
        int64_t ms = (t1.tv_sec - t0.tv_sec) * 1000 + (t1.tv_nsec - t0.tv_nsec) / 1000000;
        ecgc_get_counters(x.t, &c);
        if (ecgc_mutant != GC_MUT_UNREFLEAK)
            CHECK(c.holds == 0);
        printf("  reopen_hold: old engine reader kept chunks across reopen & retire; %d O(1) holds/unrefs in %lld ms\n",
               NH, (long long)ms);
    }
    free(pts);
    free(oids);
    tier_put(x.t);
    x.t = NULL;
}

/* ------------------------------------------------------------ GCMK crashfs power loss */

static char GPBASE[PATH_MAX];
static uint8_t GP_OIDS[3][16];

static void make_gcmk_base(void)
{
    char name0[96], name1[96];
    fresh("gpbase", GPBASE, sizeof GPBASE);
    h x;
    CHECK(h_open(&x, GPBASE, 3600) == 0);
    if (!x.db)
        return;
    write_day(x.db, 0);
    write_day(x.db, 1);
    write_day(x.db, 2);
    settle_day(&x, 0, GP_OIDS[0], name0);
    settle_day(&x, 1, GP_OIDS[1], name1);
    chunk_random(GP_OIDS[2], 16);
    CHECK(eng_drop_before(x.db, DAY) == 0); /* day 0 retired_, day 1 still listed */
    h_close(&x);
}

static int child_gcmk(const char *dir)
{
    h x;
    atomic_store(&ecgc_test_nosweep, true);
    int rc = h_open(&x, dir, 3600);
    atomic_store(&ecgc_test_nosweep, false);
    if (rc != 0)
        return 3;
    uint8_t oids[3][16];
    /* Read oids from a small side file written by parent so child uses exact same oids */
    char opath[PATH_MAX + 32];
    snprintf(opath, sizeof opath, "%s/oids.bin", dir);
    int fd = open(opath, O_RDONLY);
    if (fd < 0 || read(fd, oids, sizeof oids) != (ssize_t)sizeof oids) {
        if (fd >= 0)
            close(fd);
        h_close(&x);
        return 4;
    }
    close(fd);
    mk m = {.type = MK_GC, .span = 1};
    m.u.gc = (mk_gc){.reason = MK_GC_RETIRED, .noids = 3, .oids = (const uint8_t *)oids};
    if (ecgcmk_apply(x.db, 10, &m) != 0) {
        h_close(&x);
        return 5;
    }
    mk d1 = {.type = MK_GC, .span = 1};
    d1.u.gc = (mk_gc){.reason = MK_GC_DONE, .noids = 2, .oids = (const uint8_t *)oids};
    if (ecgcmk_apply(x.db, 11, &d1) != 0) {
        h_close(&x);
        return 6;
    }
    mk d2 = {.type = MK_GC, .span = 1};
    d2.u.gc = (mk_gc){.reason = MK_GC_DONE, .noids = 1, .oids = (const uint8_t *)&oids[2]};
    if (ecgcmk_apply(x.db, 12, &d2) != 0) {
        h_close(&x);
        return 7;
    }
    h_close(&x);
    return 0;
}

static void gcmk_power_case(void)
{
    /* Mid-file corruption check first (works even without crashfs) */
    {
        char dir[PATH_MAX];
        fresh("gpcorr", dir, sizeof dir);
        h x;
        CHECK(h_open(&x, dir, 3600) == 0);
        if (x.db) {
            uint8_t o[2][16];
            chunk_random(o[0], 16);
            chunk_random(o[1], 16);
            mk m = {.type = MK_GC, .span = 1, .u.gc = {.reason = MK_GC_ORPHAN, .noids = 2, .oids = (const uint8_t *)o}};
            CHECK(ecgcmk_apply(x.db, 5, &m) == 0);
            CHECK(ecgcmk_pending_count(x.db) == 2 && ecgcmk_pending_has(x.db, o[0]) && ecgcmk_pending_has(x.db, o[1]));
            h_close(&x);
            /* Corrupt record 0 while record 1 stays valid -> open must return TSDB_ERR_CORRUPT */
            char gp[PATH_MAX + 32];
            snprintf(gp, sizeof gp, "%s/db/GCMK", dir);
            int fd = open(gp, O_RDWR);
            CHECK(fd >= 0);
            if (fd >= 0) {
                uint8_t b = 0;
                CHECK(pread(fd, &b, 1, 20) == 1);
                b ^= 0xff;
                CHECK(pwrite(fd, &b, 1, 20) == 1);
                close(fd);
            }
            CHECK(h_open(&x, dir, 3600) == TSDB_ERR_CORRUPT);
        }
    }
    if (access(SHIM, R_OK) != 0) {
        printf("  gcmk_power: skipped crashfs loop (no %s)\n", SHIM);
        return;
    }
    make_gcmk_base();
    char opath[PATH_MAX + 32];
    snprintf(opath, sizeof opath, "%s/oids.bin", GPBASE);
    int ofd = open(opath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    CHECK(ofd >= 0 && write(ofd, GP_OIDS, sizeof GP_OIDS) == (ssize_t)sizeof GP_OIDS);
    if (ofd >= 0)
        close(ofd);
    char dir[PATH_MAX];
    snprintf(dir, sizeof dir, "%s/gpl", S);
    int crashes = 0, seeds = quick ? 2 : 3;
    bool finished = false;
    for (int k = 0; k < 400 && !finished; k++) {
        for (int sd = 0; sd < seeds; sd++) {
            sh("rm -rf '%s'; cp -a '%s' '%s'", dir, GPBASE, dir);
            int code = run_child_mode("--child-gcmk", dir, k, k * 13 + sd + 1);
            CHECK(code == 99 || code == 0);
            if (code != 99 && code != 0) {
                fprintf(stderr, "  (gcmk power loss at step %d: child said %d)\n", k, code);
                k = 1 << 20;
                break;
            }
            crashes += code == 99;
            finished = code == 0;
            /* Clean reopen must succeed (torn tail truncated) and idempotently replay MK_GC + DONE */
            h x;
            atomic_store(&ecgc_test_nosweep, true);
            int orc = h_open(&x, dir, 3600);
            atomic_store(&ecgc_test_nosweep, false);
            CHECK(orc == 0);
            if (orc != 0 || !x.db) {
                k = 1 << 20;
                break;
            }
            mk m = {.type = MK_GC, .span = 1, .u.gc = {.reason = MK_GC_RETIRED, .noids = 3, .oids = (const uint8_t *)GP_OIDS}};
            CHECK(ecgcmk_apply(x.db, 10, &m) == 0);
            CHECK(ecgcmk_pending_count(x.db) == 3);
            mk d = {.type = MK_GC, .span = 1, .u.gc = {.reason = MK_GC_DONE, .noids = 3, .oids = (const uint8_t *)GP_OIDS}};
            CHECK(ecgcmk_apply(x.db, 12, &d) == 0);
            CHECK(ecgcmk_pending_count(x.db) == 0);
            h_close(&x);
        }
    }
    CHECK(finished);
    printf("  gcmk_power: %d crashfs power losses across GCMK append/done/compact + mid-file corruption check OK\n",
           crashes);
}

/* ------------------------------------------------------------ cluster MK_GC unit tests (kills all 4 cluster mutants) */

typedef struct mock_jr {
    atomic_bool withhold_commit;
    atomic_int mkgc_appends;
} mock_jr;

static int mj_admit(void *ctx, eng *db, uint64_t fno, uint32_t n)
{
    (void)ctx, (void)db, (void)fno, (void)n;
    return 0;
}
static void mj_bytes(void *ctx, eng *db, uint64_t seq, uint64_t off, const void *buf, size_t len, uint64_t first,
                     uint64_t last_fno)
{
    (void)first;
    mock_jr *m = ctx;
    uint64_t last = last_fno;
    for (size_t at = 0; at < len;) {
        wal_rec wr;
        size_t sz = wal_record((const uint8_t *)buf + at, len - at, seq, off + at, &wr);
        if (!sz)
            break;
        if (wr.kind == WAL_KIND_MARKER && wr.mtype == MK_GC)
            atomic_fetch_add(&m->mkgc_appends, 1);
        last = wr.fno + wr.span - 1;
        at += sz;
    }
    if (last && !atomic_load(&m->withhold_commit))
        eng_leader_commit(db, last);
}
static void mj_synced(void *ctx, eng *db, uint64_t fno) { (void)ctx, (void)db, (void)fno; }
static int mj_wait(void *ctx, eng *db, uint64_t pos)
{
    mock_jr *m = ctx;
    (void)pos;
    if (atomic_load(&m->withhold_commit)) {
        eng_leader_depose(db);
        return TSDB_ERR_INDETERMINATE;
    }
    return 0;
}
static int mj_ship(void *ctx, eng *db, const char *s, const char *n, uint64_t sz)
{
    (void)ctx, (void)db, (void)s, (void)n, (void)sz;
    return 0;
}

typedef struct mock_cops_state {
    atomic_bool suspended;
    _Atomic uint64_t copy_epoch;
} mock_cops_state;

static mock_cops_state MC;

static uint64_t mc_epoch_fn(void *arg, uint32_t vol)
{
    (void)arg, (void)vol;
    return atomic_load(&MC.copy_epoch);
}
static bool mc_suspended(void *ctx)
{
    (void)ctx;
    return atomic_load(&MC.suspended);
}
static int mc_del_gc(void *ctx, const uint8_t oid[16], unsigned pos, uint64_t epoch, uint64_t mk_pos)
{
    if (mk_pos == 0 || epoch == 0)
        return TSDB_ERR_INVALID;
    return coldops_dir.del(ctx, oid, pos, epoch, COLD_DEL_MKGC);
}
static cold_ops MC_OPS;

static void make_cluster_dir(const char *dir)
{
    char dbp[PATH_MAX + 8];
    snprintf(dbp, sizeof dbp, "%s/db", dir);
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.partition_classes = 0;
    o.wal_sync_ms = 0;
    eng *db = NULL;
    CHECK(eng_open(dbp, &o, &db) == 0 && db != NULL);
    if (db)
        eng_close(db);
    int fd = open(dbp, O_RDONLY | O_DIRECTORY);
    CHECK(fd >= 0 && meta_require(fd, true, META_REQ_CLUSTER | META_REQ_REPLICA | META_REQ_EC) == 0);
    if (fd >= 0)
        close(fd);
    char mp[PATH_MAX + 32];
    snprintf(mp, sizeof mp, "%s/META", dbp);
    FILE *mf = fopen(mp, "a");
    CHECK(mf != NULL);
    if (mf) {
        fputs("volume_uuid=7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b\n", mf);
        fclose(mf);
    }
}

static int open_cluster_leader(h *x, const char *dir, uint64_t lead_epoch, mock_jr *mj, struct jr_ops *jops)
{
    MC_OPS = coldops_dir;
    MC_OPS.del_gc = mc_del_gc;
    MC_OPS.gc_suspended = mc_suspended;
    atomic_store(&MC.copy_epoch, lead_epoch);
    char dbp[PATH_MAX + 8], spec[8192];
    snprintf(dbp, sizeof dbp, "%s/db", dir);
    int n = snprintf(spec, sizeof spec, "k=%d,m=%d,cache_mb=64,dirs=", K, M);
    for (int i = 0; i < NST; i++)
        n += snprintf(spec + n, sizeof spec - (size_t)n, "%s%s/st%d", i ? ":" : "", dir, i);
    setenv("TSDB_TEST_TIER", spec, 1);
    setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);
    atomic_store(&ecgc_test_manual, true);
    int rc = tier_open(dbp, NULL, &x->t);
    unsetenv("TSDB_TEST_TIER");
    unsetenv("TSDB_TEST_TIER_I_KNOW");
    if (rc)
        return rc;
    ecsettle_tier_quiesce(x->t);
    x->rounds = atomic_load(&settle_rounds);
    atomic_store(&eckeep_test_pause, true);
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.partition_classes = 0;
    o.wal_sync_ms = 0;
    o.obsolete_part_lifetime_s = 0;
    tier_attach at = {.ops = &MC_OPS, .make = coldops_dir_make, .epoch = mc_epoch_fn, .lead_epoch = lead_epoch};
    tier_want_attach(x->t, 0, &at);
    rc = eng_open_leader(dbp, &o, &x->db);
    tier_want(NULL, 0);
    if (rc) {
        tier_put(x->t);
        x->t = NULL;
        return rc;
    }
    *jops = (struct jr_ops){.ctx = mj, .admit = mj_admit, .bytes = mj_bytes, .synced = mj_synced, .wait = mj_wait,
                            .ship = mj_ship};
    rc = eng_leader_attach(x->db, jops);
    if (rc == 0)
        eng_leader_commit(x->db, x->db->fno_next - 1);
    return rc;
}

static void put_object_epoch(h *x, uint64_t epoch, const uint8_t *b, size_t len, uint8_t oid[16])
{
    chunk_geom g;
    CHECK(chunk_geom_derive(&g, K, M, len) == 0);
    uint64_t *sums = malloc(chunk_sums_total(&g) * sizeof *sums);
    chunk_random(oid, 16);
    CHECK(chunk_be_see_epoch(tier_backend(x->t), x->db->tier_vol, epoch) == 0);
    CHECK(chunk_be_put_object(tier_backend(x->t), x->db->tier_vol, tier_vuuid(x->db), oid, &g, epoch, b, ~0u, sums) ==
          0);
    note(oid, (int)(g.k + g.m));
    free(sums);
}

static void cluster_gc_case(void)
{
    meta_cluster_open_allow(true);
    atomic_store(&ecgc_test_orphan_grace_ms, 0);
    static uint8_t junk[200000];
    memset(junk, 0x3c, sizeof junk);

    /* 1. Uncommitted MK_GC must NEVER delete chunks (kills gcuncommitted). */
    {
        char dir[PATH_MAX], name[96];
        uint8_t oid[16];
        fresh("cg_uncom", dir, sizeof dir);
        make_cluster_dir(dir);
        mock_jr mj = {0};
        struct jr_ops jops = {0};
        h x = {0};
        CHECK(open_cluster_leader(&x, dir, 2, &mj, &jops) == 0);
        if (x.db) {
            write_day(x.db, 0);
            write_day(x.db, 1);
            settle_day(&x, 0, oid, name);
            CHECK(eng_drop_before(x.db, DAY) == 0);
            atomic_store(&mj.withhold_commit, true);
            ecgc_tier_work(x.t);
            CHECK(chunks(dir, oid) == npos(oid)); /* fails with gcuncommitted! */
            h_close(&x);
        }
    }

    /* 2. WAL record naming an object (MK_COLD, no listed .ec on disk) protects it from orphan sweep (kills gcnamedorphan). */
    {
        char dir[PATH_MAX];
        uint8_t oid_log[16], oid_orph[16];
        fresh("cg_wal", dir, sizeof dir);
        make_cluster_dir(dir);
        mock_jr mj = {0};
        struct jr_ops jops = {0};
        h x = {0};
        atomic_store(&MC.suspended, false);
        /* Epoch 1 writes two objects (no .ec files); then epoch 2 takes over. */
        CHECK(open_cluster_leader(&x, dir, 1, &mj, &jops) == 0);
        if (x.db) {
            put_object_epoch(&x, 1, junk, sizeof junk, oid_log);
            put_object_epoch(&x, 1, junk, sizeof junk, oid_orph);
            h_close(&x);
        }
        memset(&mj, 0, sizeof mj);
        CHECK(open_cluster_leader(&x, dir, 2, &mj, &jops) == 0);
        if (x.db) {
            CHECK(chunk_be_see_epoch(tier_backend(x.t), x.db->tier_vol, 2) == 0);
            /* Append an MK_COLD naming oid_log (no .ec file on disk) into the leader's WAL. */
            uint8_t obuf[64];
            uint8_t dummy_rec[8] = {1, 2, 3, 4, 5, 6, 7, 8};
            size_t olen = mk_cold_obj_put(obuf, oid_log, dummy_rec, sizeof dummy_rec);
            mk cm = {.type = MK_COLD, .span = 1};
            cm.u.cold = (mk_cold){.z = 1, .flush_through = 0, .stripe_version = 1, .key = 0, .nobjs = 1,
                                  .objs = obuf, .objs_len = olen};
            uint64_t cpos = 0;
            CHECK(jr_markers_append(x.db, &cm, 1, "test cold", &cpos) == 0 && cpos > 0);
            CHECK(jr_markers_wait(x.db, cpos, "test cold") == 0);
            ecgc_tier_work(x.t);
            CHECK(chunks(dir, oid_orph) == 0);
            CHECK(chunks(dir, oid_log) == npos(oid_log)); /* fails with gcnamedorphan! */
            CHECK(ecgcmk_pending_count(x.db) == 0);
            h_close(&x);
        }
    }

    /* 3. gc_suspended stops orphan GC while retired-object GC continues (kills gcsuspendignored). */
    {
        char dir[PATH_MAX], name[96];
        uint8_t oid_susp[16], oid_ret[16];
        fresh("cg_susp", dir, sizeof dir);
        make_cluster_dir(dir);
        mock_jr mj = {0};
        struct jr_ops jops = {0};
        h x = {0};
        atomic_store(&MC.suspended, true);
        CHECK(open_cluster_leader(&x, dir, 1, &mj, &jops) == 0);
        if (x.db) {
            put_object_epoch(&x, 1, junk, sizeof junk, oid_susp);
            h_close(&x);
        }
        memset(&mj, 0, sizeof mj);
        CHECK(open_cluster_leader(&x, dir, 2, &mj, &jops) == 0);
        if (x.db) {
            CHECK(chunk_be_see_epoch(tier_backend(x.t), x.db->tier_vol, 2) == 0);
            write_day(x.db, 0);
            write_day(x.db, 1);
            settle_day(&x, 0, oid_ret, name);
            CHECK(eng_drop_before(x.db, DAY) == 0);
            ecgc_tier_work(x.t);
            CHECK(chunks(dir, oid_ret) == 0);               /* retired GC runs while suspended */
            CHECK(chunks(dir, oid_susp) == npos(oid_susp)); /* orphan kept while suspended; fails gcsuspendignored! */
            atomic_store(&MC.suspended, false);
            ecgc_tier_work(x.t);
            CHECK(chunks(dir, oid_susp) == 0); /* resumed: orphan deleted */
            h_close(&x);
        }
    }

    /* 4. Replica ecgcmk_apply retires still-listed .ec, unlinks retired_, records GCMK (kills gcreplicanoop). */
    {
        char dir[PATH_MAX], name[96], dbp[PATH_MAX + 8];
        uint8_t oid[16];
        fresh("cg_rep", dir, sizeof dir);
        make_cluster_dir(dir);
        mock_jr mj = {0};
        struct jr_ops jops = {0};
        h x = {0};
        CHECK(open_cluster_leader(&x, dir, 2, &mj, &jops) == 0);
        if (x.db) {
            write_day(x.db, 0);
            settle_day(&x, 0, oid, name); /* .ec stays LISTED on disk */
            h_close(&x);
        }
        /* Reopen directory as a replica (TTL behind: .ec is still listed) and apply MK_GC. */
        snprintf(dbp, sizeof dbp, "%s/db", dir);
        char spec[8192];
        int n = snprintf(spec, sizeof spec, "k=%d,m=%d,cache_mb=64,dirs=", K, M);
        for (int i = 0; i < NST; i++)
            n += snprintf(spec + n, sizeof spec - (size_t)n, "%s%s/st%d", i ? ":" : "", dir, i);
        setenv("TSDB_TEST_TIER", spec, 1);
        setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);
        atomic_store(&ecgc_test_manual, true);
        tier *rt = NULL;
        CHECK(tier_open(dbp, NULL, &rt) == 0);
        unsetenv("TSDB_TEST_TIER");
        unsetenv("TSDB_TEST_TIER_I_KNOW");
        if (rt) {
            ecsettle_tier_quiesce(rt);
            tsdb_options o;
            tsdb_options_init(&o);
            o.merge_threads = 0;
            o.create_volumes = 0;
            o.partition_classes = 0;
            o.wal_sync_ms = 0;
            tier_want(rt, 0);
            eng *rep = NULL;
            CHECK(eng_open_replica(dbp, &o, 0, &rep) == 0 && rep != NULL);
            tier_want(NULL, 0);
            if (rep) {
                CHECK(exists(dir, name));
                mk gm = {.type = MK_GC, .span = 1, .u.gc = {.reason = MK_GC_RETIRED, .noids = 1, .oids = oid}};
                CHECK(ecgcmk_apply(rep, 50, &gm) == 0);
                CHECK(!exists(dir, name)); /* fails with gcreplicanoop! */
                char rname[128];
                snprintf(rname, sizeof rname, "retired_%s", name);
                CHECK(!exists(dir, rname));
                CHECK(ecgcmk_pending_has(rep, oid)); /* fails with gcreplicanoop! */
                mk dm = {.type = MK_GC, .span = 1, .u.gc = {.reason = MK_GC_DONE, .noids = 1, .oids = oid}};
                CHECK(ecgcmk_apply(rep, 51, &dm) == 0);
                CHECK(ecgcmk_pending_count(rep) == 0);
                eng_close(rep);
            }
            /* 4b. Empty MK_GC_DONE does not create GCMK; MK_GC_DONE also unlinks lingering retired_*.ec. */
            {
                char dir2[PATH_MAX], name2[96], rname2[128], dbp2[PATH_MAX + 8];
                uint8_t oid2[16];
                fresh("cg_done", dir2, sizeof dir2);
                make_cluster_dir(dir2);
                mock_jr mj2 = {0};
                struct jr_ops jops2 = {0};
                h x2 = {0};
                CHECK(open_cluster_leader(&x2, dir2, 2, &mj2, &jops2) == 0);
                if (x2.db) {
                    write_day(x2.db, 0);
                    write_day(x2.db, 1);
                    settle_day(&x2, 0, oid2, name2);
                    CHECK(eng_drop_before(x2.db, DAY) == 0); /* retires name2 -> retired_<name2>, GCMK not created yet */
                    snprintf(rname2, sizeof rname2, "retired_%s", name2);
                    CHECK(exists(dir2, rname2));
                    CHECK(!exists(dir2, "GCMK"));
                    h_close(&x2);
                }
                snprintf(dbp2, sizeof dbp2, "%s/db", dir2);
                tier_want(rt, 1);
                eng *rep2 = NULL;
                CHECK(eng_open_replica(dbp2, &o, 0, &rep2) == 0 && rep2 != NULL);
                tier_want(NULL, 0);
                if (rep2) {
                    CHECK(exists(dir2, rname2));
                    CHECK(!exists(dir2, "GCMK"));
                    mk dm = {.type = MK_GC, .span = 1, .u.gc = {.reason = MK_GC_DONE, .noids = 1, .oids = oid2}};
                    CHECK(ecgcmk_apply(rep2, 52, &dm) == 0);
                    CHECK(!exists(dir2, "GCMK"));   /* empty MK_GC_DONE must not compact/create a 0-byte GCMK */
                    CHECK(!exists(dir2, rname2));   /* lingering retired_*.ec unlinked on MK_GC_DONE */
                    eng_close(rep2);
                }
            }
            tier_put(rt);
        }
    }

    /* 5. FENCED stops GC + r8f F4 (deposed leader with caught-up copy still uses its own lead_epoch). */
    {
        char dir[PATH_MAX];
        uint8_t oid[16];
        fresh("cg_fence", dir, sizeof dir);
        make_cluster_dir(dir);
        mock_jr mj = {0};
        struct jr_ops jops = {0};
        h x = {0};
        CHECK(open_cluster_leader(&x, dir, 2, &mj, &jops) == 0);
        if (x.db) {
            put_object_epoch(&x, 2, junk, sizeof junk, oid);
            /* Simulate failover: metadata copy jumps to epoch 3 and chunk stores advance to epoch 3! */
            atomic_store(&MC.copy_epoch, 3);
            CHECK(coldvol_epoch(tier_cold(x.db)) == 2); /* r8f F4: still pinned to own lead epoch 2! */
            CHECK(chunk_be_see_epoch(tier_backend(x.t), x.db->tier_vol, 3) == 0);
            mk gm = {.type = MK_GC, .span = 1, .u.gc = {.reason = MK_GC_ORPHAN, .noids = 1, .oids = oid}};
            CHECK(ecgcmk_apply(x.db, 77, &gm) == 0);
            ecgc_tier_work(x.t);
            CHECK(ecgcmk_is_fenced(x.db));
            CHECK(chunks(dir, oid) == npos(oid)); /* refused FENCED, chunks intact */
            CHECK(ecgcmk_pending_has(x.db, oid)); /* kept on pending list */
            h_close(&x);
        }
    }
    atomic_store(&ecgc_test_orphan_grace_ms, -1);
    meta_cluster_open_allow(false);
    printf("  cluster_gc: uncommitted gate, WAL log scan, gc_suspended, replica apply, FENCED + r8f F4 OK\n");
}

static void run(void)
{
    hold_case(0);
    hold_case(1);
    reopen_hold_case();
    sweep_case();
    down_case();
    thread_case();
    foreign_case();
    unlisted_case();
    midround_case();
    retry_case();
    power_case();
    gcmk_power_case();
    cluster_gc_case();
}
#endif

int main(int argc, char **argv)
{
    const char *child_dir = NULL, *child_gcmk_dir = NULL, *scratch = NULL;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--mutant") && i + 1 < argc) {
            const char *m = argv[++i];
            bool ok = false;
            for (size_t j = 0; j < sizeof MUTANTS / sizeof *MUTANTS; j++)
                if (!strcmp(MUTANTS[j].name, m))
                    ecgc_mutant = MUTANTS[j].m, ok = true;
            if (!ok) {
                fprintf(stderr, "usage: test_ecgc [--quick] [--mutant NAME] [SCRATCH]\n");
                return 2;
            }
        } else if (!strcmp(argv[i], "--child") && i + 1 < argc) {
            child_dir = argv[++i];
        } else if (!strcmp(argv[i], "--child-gcmk") && i + 1 < argc) {
            child_gcmk_dir = argv[++i];
        } else if (!strcmp(argv[i], "--quick")) {
#ifdef TSDB_EC
            quick = true;
#endif
        } else {
            scratch = argv[i];
        }
    }
#ifndef TSDB_EC
    (void)child_dir;
    (void)child_gcmk_dir;
    (void)scratch;
    (void)mutant_name;
    printf("test_ecgc: skipped: no TSDB_EC in this build\n");
    return 0;
#else
    if (child_dir)
        return child(child_dir);
    if (child_gcmk_dir)
        return child_gcmk(child_gcmk_dir);
    if (!realpath(argv[0], SELF))
        snprintf(SELF, sizeof SELF, "%s", argv[0]);
    const char *slash = strrchr(SELF, '/');
    snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", slash ? (int)(slash - SELF) : 1, slash ? SELF : ".");
    snprintf(S, sizeof S, "%s/test_ecgc_%d", scratch ? scratch : "/tmp", (int)getpid());
    sh("rm -rf '%s'; mkdir -p '%s'", S, S);
    run();
    if (failures) {
        fprintf(stderr, "test_ecgc: %d failures (mutant %s)\n", failures, mutant_name(ecgc_mutant));
        return 1;
    }
    sh("rm -rf '%s'", S);
    printf("test_ecgc: ok\n");
    return 0;
#endif
}
