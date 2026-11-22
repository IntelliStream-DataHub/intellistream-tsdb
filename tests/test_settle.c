/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The settle of parts into objects (r7-settle: src/settle.c, src/ecpolicy.c; design-cluster-data.md E5-E7, E26).
 * Every case runs an engine with a tier of three directory stores (k=2, m=1) opened by hand (tier_open, tier_want,
 * eng_open), and most of them a second engine without a tier given the same writes: the oracle that never settled.
 * The clock is the test's (tsdb_options.clock_ms), so the policy's ages are set, never waited for.
 *
 *   basic     a key settled one part at a time (the newer one first: it must still win), the key settled whole, a
 *             second settle finds nothing, a v8 part is skipped, reopen; every query equals the oracle
 *   corrupt   a granule byte flipped in the source: CORRUPT, the part marked damaged, no .ec, no chunk left
 *   flip      a byte flipped through the fill hook after the stripe's copy: the object holds what was checked
 *   down      a store moved away: UNAVAILABLE, and the thread settles nothing
 *   policy    a day key settles after grace_s and its merges, not before; never while a merge is selectable
 *   coarse    a class-1 key part by part, not again within coarse_s, also across a restart
 *   backoff   a failed settle is not retried for an hour, then is
 *   eager     eager=1: only after a merge round, at most 30 settles a minute
 *   close     an engine closed during a slow encode: the settle is abandoned, nothing is left listed
 *   halt      an engine halted (a volume reopen) during a slow encode: the settle is abandoned before eng_halt returns
 *   threads   8 writers (appends, large inserts, deletes, replaces), merges, a drop and settles at once
 *   crash     TSDB_SETTLE_CRASH_AT after-put and after-ec-tmp, then power loss (crashfs) at every step
 *   gcpaths   every failure after the put (src/settle.h SETTLE_FAIL_*, src/ecswap.h ECSWAP_FAIL_*, a drop inside the
 *             swap): each failed object goes to r7-gc once, a gc round leaves exactly the listed objects in the
 *             stores, the key settles again, the rows equal the oracle's, a reopen's sweep finds nothing
 *   gcpower   a power cut in a refused swap's cleanup: with a drop's renames unsynced (stopped), after a failed
 *             intent write whose name another sync made durable (intentio), after a failed discard sync (syncfail):
 *             an intent that rolls forward finds whole objects, nothing is left behind; a drop of two parts (its
 *             intent durable) is finished at the open and its retired .ec parts collected, one of one part is not
 *   coldops   two volumes with recording ops and a provider (2, 3): settle by call and by the thread, a cold read, the
 *             gc's deletes and a reopen's sweep of volume 1 each go through volume 1's ops at its provider's epoch;
 *             provider 0 or ops not writable: nothing put or deleted; a failed attach keeps the registry's entry
 *
 * Mutants (--mutant NAME, src/settle.h; each must fail it): ecrankdown (basic), settlenocheck (corrupt), settletwopass
 * (flip), settlebusy (policy), coarsewhole (coarse); gcskipput gcskipindex gcskipopen gcskipelse gcskipswap
 * gcskipdiscard gccommitted gcdouble (gcpaths), gcearly nullnosync syncfailgo (gcpower), nohaltstop (halt). Usage: test_settle [--quick] [--case NAME]
 * [--mutant NAME] [SCRATCH]
 */
#include "internal.h"
#include "chunk.h"
#include "coldops.h"
#include "eccache.h"
#include "ecpart.h"
#include "ecgc.h"
#include "ecskip.h"
#include "ecswap.h"
#include "settle.h"
#include "tier.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
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

#ifndef TSDB_EC
int main(void)
{
    printf("test_settle: skipped (built without TSDB_EC)\n");
    return 0;
}
#else

static char S[PATH_MAX / 2], SELF[PATH_MAX], SHIM[PATH_MAX];
static bool quick;
static const char *mutant = "none";
static const char *only; /* --case NAME: that case alone */
static const int64_t DAY = 86400000, HOUR = 3600000;

/* the test's clock: every engine reads it (never goes back) */
static _Atomic int64_t NOW = 1000;
static int64_t clock_fn(void *arg)
{
    (void)arg;
    return atomic_load(&NOW);
}

static void sleep_ms(int ms)
{
    struct timespec ts = {ms / 1000, (long)(ms % 1000) * 1000000L};
    nanosleep(&ts, NULL);
}

/* ------------------------------------------------------------ directories, the tier */

typedef struct {
    char root[PATH_MAX - 64], db[PATH_MAX], st[3][PATH_MAX];
} where;

static void fresh(const char *what, where *w)
{
    static int seq;
    snprintf(w->root, sizeof w->root, "%s/settle-%d-%s-%d", S, (int)getpid(), what, seq++);
    char cmd[PATH_MAX + 32];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", w->root);
    if (system(cmd) != 0)
        CHECK(0);
    CHECK(mkdir(w->root, 0755) == 0);
    snprintf(w->db, sizeof w->db, "%s/db", w->root);
    CHECK(mkdir(w->db, 0755) == 0);
    for (int i = 0; i < 3; i++) {
        snprintf(w->st[i], sizeof w->st[i], "%s/st%d", w->root, i);
        CHECK(mkdir(w->st[i], 0755) == 0);
    }
}

static void tier_env(const where *w, int grace_s, int coarse_s, int eager)
{
    char spec[4 * PATH_MAX + 128];
    snprintf(spec, sizeof spec, "k=2,m=1,dirs=%s:%s:%s,grace_s=%d,coarse_s=%d,eager=%d", w->st[0], w->st[1], w->st[2],
             grace_s, coarse_s, eager);
    setenv("TSDB_TEST_TIER", spec, 1);
    setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);
}

typedef struct {
    int merge_threads, classes;
    size_t wal_max_batch_rows;
    int idle; /* merge_idle_s; 0: 1 */
} eopts;

typedef struct {
    tier *t;
    eng *db;
} tdb;

static void opts_of(const eopts *e, tsdb_options *o)
{
    tsdb_options_init(o);
    o->merge_threads = e->merge_threads;
    o->partition_classes = e->classes;
    o->create_volumes = 0;
    o->clock_ms = clock_fn;
    o->merge_idle_s = e->idle ? e->idle : 1;
    if (e->wal_max_batch_rows)
        o->wal_max_batch_rows = e->wal_max_batch_rows;
}

/* the engine of w->db with a tier (the environment's spec) */
static int t_open(const where *w, const eopts *e, tdb *d)
{
    tsdb_options o;
    opts_of(e, &o);
    d->t = NULL;
    d->db = NULL;
    int rc = tier_open(w->db, &o, &d->t);
    if (rc)
        return rc;
    tier_want(d->t, 0);
    rc = eng_open(w->db, &o, &d->db);
    tier_want(NULL, 0);
    if (rc) {
        tier_put(d->t);
        d->t = NULL;
        d->db = NULL;
    }
    return rc;
}

static int t_close(tdb *d)
{
    int rc = d->db ? eng_close(d->db) : 0;
    if (d->t)
        tier_put(d->t);
    d->db = NULL;
    d->t = NULL;
    return rc;
}

/* the oracle: an engine without a tier in w->root/oracle */
static eng *o_open(const where *w, const eopts *e)
{
    char p[PATH_MAX + 16];
    snprintf(p, sizeof p, "%s/oracle", w->root);
    mkdir(p, 0755);
    tsdb_options o;
    opts_of(e, &o);
    eng *db = NULL;
    CHECK(eng_open(p, &o, &db) == 0);
    return db;
}

static size_t chunk_files;
static int count_fn(const char *path, const struct stat *st, int type, struct FTW *f)
{
    (void)st;
    const char *b = path + f->base;
    if (type == FTW_F && strcmp(b, "META") && strcmp(b, "EPOCH") && strcmp(b, "LOCK") && strncmp(b, "tmp_", 4))
        chunk_files++;
    return 0;
}

/* chunk files in the three stores (META, EPOCH and the lock left out) */
static size_t chunks(const where *w)
{
    chunk_files = 0;
    for (int i = 0; i < 3; i++)
        nftw(w->st[i], count_fn, 16, FTW_PHYS);
    return chunk_files;
}

static size_t count_names(const char *dir, const char *prefix, const char *suffix)
{
    DIR *d = opendir(dir);
    if (!d)
        return 0;
    size_t n = 0, ls = strlen(suffix);
    for (struct dirent *e; (e = readdir(d));) {
        size_t l = strlen(e->d_name);
        if (!strncmp(e->d_name, prefix, strlen(prefix)) && l >= ls && !strcmp(e->d_name + l - ls, suffix))
            n++;
    }
    closedir(d);
    return n;
}

/* ------------------------------------------------------------ rows and the comparison */

enum { NOISY = 99 }; /* write_rows' salt for values that hardly compress */
static void write_rows(eng *db, uint32_t s, int64_t t0, int n, int64_t step, int salt)
{
    int64_t ts[512];
    float v[512];
    for (int i = 0; i < n; i += 512) {
        int k = n - i < 512 ? n - i : 512;
        for (int j = 0; j < k; j++)
            ts[j] = t0 + (int64_t)(i + j) * step,
            v[j] = salt == NOISY ? (float)(((uint32_t)(i + j) * 2654435761u >> 9) % 1000003) / 7
                                 : (float)((t0 / 1000 + i + j + salt * 7919) % 1009);
        CHECK(eng_insert_series(db, TSDB_FLOAT32, s, ts, v, (size_t)k) == 0);
    }
}

typedef struct {
    size_t n;
    double sum;
    uint64_t h;
} acc;

static int acc_fn(void *ctx, const int64_t *ts, const void *v, size_t n)
{
    acc *a = ctx;
    for (size_t i = 0; i < n; i++) {
        float f = ((const float *)v)[i];
        uint32_t b;
        memcpy(&b, &f, 4);
        a->sum += f;
        a->h += ((uint64_t)ts[i] * 0x9E3779B97F4A7C15ull) ^ ((uint64_t)b * 0xC2B2AE3D27D4EB4Full);
    }
    a->n += n;
    return 0;
}

static acc digest(eng *db, const char *name)
{
    acc r = {0, 0, 0};
    uint32_t s;
    if (eng_series_lookup(db, name, &s) != 0)
        return r;
    CHECK(eng_query_raw_ex(db, s, INT64_MIN + 1, INT64_MAX, false, 0, acc_fn, &r) == 0);
    return r;
}

static bool same(acc a, acc b) { return a.n == b.n && a.sum == b.sum && a.h == b.h; }

/* the listed parts of key, referenced (the caller unrefs) */
static size_t key_parts(eng *db, int64_t key, part **out, size_t cap)
{
    size_t n = 0;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts && n < cap; i++)
        if (db->parts[i]->partition == key) {
            part_ref(db->parts[i]);
            out[n++] = db->parts[i];
        }
    pthread_mutex_unlock(&db->lock);
    return n;
}

static void unref_all(part **p, size_t n)
{
    for (size_t i = 0; i < n; i++)
        part_unref(p[i]);
}

/* listed parts of key: how many, and how many of them .ec */
static void key_count(eng *db, int64_t key, size_t *all, size_t *ec)
{
    *all = *ec = 0;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++)
        if (db->parts[i]->partition == key) {
            (*all)++;
            *ec += part_is_ec(db->parts[i]);
        }
    pthread_mutex_unlock(&db->lock);
}

static int settle_key(eng *db, int64_t key, size_t *settled)
{
    part *p[64];
    size_t n = key_parts(db, key, p, 64);
    int rc = eng_settle(db, key, p, n, settled);
    unref_all(p, n);
    return rc;
}

/* waits up to ms for key to be all .ec (at least one part); whether it is */
static bool wait_settled(eng *db, int64_t key, int ms)
{
    for (int i = 0; i < ms / 10; i++) {
        size_t a, e;
        key_count(db, key, &a, &e);
        if (a && a == e)
            return true;
        sleep_ms(10);
    }
    return false;
}

/* waits for `rounds` more rounds of the settle thread */
static void wait_rounds(int rounds)
{
    uint64_t r0 = atomic_load(&settle_rounds);
    for (int i = 0; i < 1000 && atomic_load(&settle_rounds) < r0 + (uint64_t)rounds; i++)
        sleep_ms(10);
    CHECK(atomic_load(&settle_rounds) >= r0 + (uint64_t)rounds);
}

/* ------------------------------------------------------------ basic */

static void basic_case(void)
{
    where w;
    fresh("basic", &w);
    tier_env(&w, 3600, 86400, 0);
    eopts e = {.merge_threads = 0, .classes = 0};
    tdb d;
    CHECK(t_open(&w, &e, &d) == 0);
    eng *o = o_open(&w, &e);
    if (!d.db || !o)
        return;
    for (int k = 0; k < 2; k++) {
        eng *db = k ? o : d.db;
        uint32_t s;
        CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
        write_rows(db, s, 0, 20000, 2000, 0); /* day 0, part A */
        write_rows(db, s, DAY, 20000, 2000, 0);
        CHECK(eng_flush(db) == 0);
        write_rows(db, s, 0, 20000, 2000, 1); /* the same keys again, other values: part B wins over A */
        CHECK(eng_flush(db) == 0);
    }
    acc want = digest(o, "s");
    CHECK(want.n == 40000 && same(digest(d.db, "s"), want));
    part *p[8];
    size_t n = key_parts(d.db, 0, p, 8);
    CHECK(n == 2);
    if (n == 2) {
        part *a = p[0]->max_block < p[1]->max_block ? p[0] : p[1], *b = a == p[0] ? p[1] : p[0];
        /* a part carried over from format 8 is skipped (rewritten alone first) */
        uint32_t hl = a->hdr_len;
        a->hdr_len = 24;
        CHECK(eng_settle(d.db, 0, &a, 1, NULL) == TSDB_ERR_NOTFOUND);
        a->hdr_len = hl;
        /* the newer part first: as an .ec part it must still rank above A */
        uint64_t bh = xxh64(b->map, b->map_len, 0), bl = b->map_len, bmin = b->min_block, bmax = b->max_block;
        int64_t t0 = eng_now_ms(d.db);
        size_t got = 0;
        CHECK(eng_settle(d.db, 0, &b, 1, &got) == 0 && got == 1);
        int64_t t1 = eng_now_ms(d.db);
        CHECK(same(digest(d.db, "s"), want));
        /* its record: what the coarse pace counts from, D4's epoch, and the object's length and hash */
        part *q[8];
        size_t nq = key_parts(d.db, 0, q, 8), found = 0;
        for (size_t i = 0; i < nq; i++) {
            struct ecpart *ep = atomic_load(&q[i]->ec);
            if (!ep || q[i]->max_block != bmax)
                continue;
            found++;
            const ec_record *r = &ep->rec;
            CHECK(r->settle_ms >= t0 && r->settle_ms <= t1);
            CHECK(r->writer_epoch == 1);
            CHECK(r->key == 0 && r->min_block == bmin && r->max_block == bmax);
            CHECK(r->object_len == bl);
            CHECK(r->object_xxh64 == bh);
            CHECK(r->class_check == EC_CLS_PASSED);
        }
        CHECK(found == 1);
        unref_all(q, nq);
        CHECK(eng_settle(d.db, 0, &a, 1, &got) == 0 && got == 1);
        CHECK(same(digest(d.db, "s"), want));
        unref_all(p, n);
    }
    size_t all, ec;
    key_count(d.db, 0, &all, &ec);
    CHECK(all == 2 && ec == 2);
    CHECK(settle_key(d.db, 0, NULL) == TSDB_ERR_NOTFOUND); /* nothing left to settle */
    size_t got = 0;
    CHECK(settle_key(d.db, 1, &got) == 0 && got == 1);
    CHECK(same(digest(d.db, "s"), want));
    CHECK(count_names(w.db, "", ".ec") == 3);
    CHECK(chunks(&w) >= 6); /* three objects, at least k' + m = 2 positions each */
    /* newer rows over a settled key still win */
    uint32_t s;
    for (int k = 0; k < 2; k++) {
        eng *db = k ? o : d.db;
        CHECK(eng_series_lookup(db, "s", &s) == 0);
        write_rows(db, s, 5000, 1000, 4000, 2);
        CHECK(eng_flush(db) == 0);
    }
    want = digest(o, "s");
    CHECK(same(digest(d.db, "s"), want));
    CHECK(t_close(&d) == 0);
    CHECK(t_open(&w, &e, &d) == 0);
    if (d.db) {
        CHECK(same(digest(d.db, "s"), want));
        key_count(d.db, 1, &all, &ec);
        CHECK(all == 1 && ec == 1);
    }
    CHECK(t_close(&d) == 0);
    eng_close(o);
    printf("  basic: a key settled part by part (newer first) and whole, nothing left, v8 skipped, reopen: queries "
           "equal the oracle\n");
}

/* ------------------------------------------------------------ damage */

/* the file offset of a value byte of granule i of p */
static uint64_t gran_byte(const part *p, uint32_t i)
{
    granule g;
    gx_load(p, i, GW_LOC | GW_TIME, &g);
    return g.off + g.ts_len + g.val_len / 2;
}

static void flip(const char *dir, const char *name, uint64_t off)
{
    char path[PATH_MAX + 128];
    snprintf(path, sizeof path, "%s/%s", dir, name);
    int fd = open(path, O_RDWR);
    CHECK(fd >= 0);
    uint8_t b = 0;
    CHECK(pread(fd, &b, 1, (off_t)off) == 1);
    b ^= 0x5a;
    CHECK(pwrite(fd, &b, 1, (off_t)off) == 1);
    close(fd);
}

static void corrupt_case(void)
{
    where w;
    fresh("corrupt", &w);
    tier_env(&w, 3600, 86400, 0);
    eopts e = {.merge_threads = 0, .classes = 0};
    tdb d;
    CHECK(t_open(&w, &e, &d) == 0);
    if (!d.db)
        return;
    uint32_t s;
    CHECK(eng_series(d.db, "s", TSDB_FLOAT32, &s) == 0);
    write_rows(d.db, s, 0, 30000, 2000, 0);
    CHECK(eng_flush(d.db) == 0);
    part *p[4];
    size_t n = key_parts(d.db, 0, p, 4);
    CHECK(n == 1 && p[0]->ng >= 3);
    if (n == 1) {
        flip(w.db, p[0]->name, gran_byte(p[0], 2));
        uint64_t f0 = atomic_load(&settle_failed);
        CHECK(eng_settle(d.db, 0, p, 1, NULL) == TSDB_ERR_CORRUPT);
        CHECK(atomic_load(&settle_failed) == f0 + 1);
        CHECK(atomic_load(&p[0]->damaged));
        CHECK(!p[0]->settling);
        CHECK(eng_settle(d.db, 0, p, 1, NULL) == TSDB_ERR_NOTFOUND); /* damaged: skipped */
    }
    unref_all(p, n);
    CHECK(count_names(w.db, "", ".ec") == 0);
    CHECK(count_names(w.db, "tmp_", "") == 0);
    CHECK(chunks(&w) == 0);
    t_close(&d);
    printf("  corrupt: a granule's crc fails in the source: CORRUPT, the part damaged, no .ec, no chunk\n");
}

static _Atomic(const char *) flip_dir;
static _Atomic uint64_t flip_off;
static atomic_int flip_done;
static char flip_name[96];

static void flip_hook(eng *db, const part *p, uint64_t stripe)
{
    (void)db;
    if (stripe == 0 && !strcmp(p->name, flip_name) && atomic_exchange(&flip_done, 1) == 0)
        flip(atomic_load(&flip_dir), flip_name, atomic_load(&flip_off));
}

static void flip_case(void)
{
    where w;
    fresh("flip", &w);
    tier_env(&w, 3600, 86400, 0);
    eopts e = {.merge_threads = 0, .classes = 0};
    tdb d;
    CHECK(t_open(&w, &e, &d) == 0);
    if (!d.db)
        return;
    uint32_t s;
    CHECK(eng_series(d.db, "s", TSDB_FLOAT32, &s) == 0);
    write_rows(d.db, s, 0, 30000, 2000, 0);
    CHECK(eng_flush(d.db) == 0);
    acc want = digest(d.db, "s");
    part *p[4];
    size_t n = key_parts(d.db, 0, p, 4);
    CHECK(n == 1);
    if (n == 1) {
        snprintf(flip_name, sizeof flip_name, "%s", p[0]->name);
        atomic_store(&flip_dir, w.db);
        atomic_store(&flip_off, gran_byte(p[0], 1));
        atomic_store(&flip_done, 0);
        atomic_store(&settle_fill_hook, flip_hook);
        int rc = eng_settle(d.db, 0, p, 1, NULL);
        atomic_store(&settle_fill_hook, NULL);
        CHECK(atomic_load(&flip_done) == 1);
        /* one pass: what was checked is what was encoded, so the settle swaps and its object holds the rows as
         * they were; settletwopass encodes the flipped byte that its check never saw */
        CHECK(rc == 0);
        if (rc == 0)
            CHECK(same(digest(d.db, "s"), want));
    }
    unref_all(p, n);
    CHECK(t_close(&d) == 0);
    CHECK(t_open(&w, &e, &d) == 0);
    if (d.db)
        CHECK(same(digest(d.db, "s"), want));
    t_close(&d);
    printf("  flip: a byte of the map changed after the stripe was copied: the object holds the checked bytes\n");
}

/* ------------------------------------------------------------ stores DOWN */

/* ------------------------------------------------------------ each check before the encode, on its own */

static _Atomic(const char *) plant_dir;
static _Atomic uint64_t plant_off, plant_stripe;
static atomic_int plant_done;
static char plant_name[96];

/* flips the byte at plant_off of plant_name once, at the hook's stripe plant_stripe */
static void plant_hook(eng *db, const part *p, uint64_t stripe)
{
    (void)db;
    if (stripe == atomic_load(&plant_stripe) && !strcmp(p->name, plant_name) && atomic_exchange(&plant_done, 1) == 0)
        flip(atomic_load(&plant_dir), plant_name, atomic_load(&plant_off));
}

static void plant(const where *w, const part *p, uint64_t stripe, uint64_t off)
{
    snprintf(plant_name, sizeof plant_name, "%s", p->name);
    atomic_store(&plant_dir, w->db);
    atomic_store(&plant_off, off);
    atomic_store(&plant_stripe, stripe);
    atomic_store(&plant_done, 0);
    atomic_store(&settle_fill_hook, plant_hook);
}

/* eng_settle of key's one part: rc, and on a failure the key holds no .ec part and no chunk was named: each check
 * fails the fill, before the encode finishes (ec_index_write's own hash check comes after the chunks were put, so a
 * check that lets the damage through to it is caught here) */
static void settle_expect(const where *w, eng *db, int64_t key, int want, const char *what)
{
    part *p[4];
    size_t n = key_parts(db, key, p, 4);
    CHECK(n == 1);
    size_t c0 = chunks(w);
    int rc = n == 1 ? eng_settle(db, key, p, 1, NULL) : -999;
    atomic_store(&settle_fill_hook, NULL);
    if (rc != want) {
        fprintf(stderr, "FAIL %s: eng_settle %d, not %d\n", what, rc, want);
        failures++;
    }
    unref_all(p, n);
    if (want != 0) {
        size_t all, ec, c1 = chunks(w);
        key_count(db, key, &all, &ec);
        CHECK(ec == 0);
        if (c1 != c0) {
            fprintf(stderr, "FAIL %s: %zu chunks named before the settle failed (not in the fill)\n", what, c1 - c0);
            failures++;
        }
    }
}

/* the fill's streaming xxh64 against xxh64 over every length up to 300 (each tail of 8, 4 and single bytes) and
 * random splits, pieces of 0 bytes included: the .ec record keeps it as the object's hash */
static void hash_case(void)
{
    enum { N = 300 };
    uint8_t buf[N];
    uint64_t r = 0x9e3779b97f4a7c15ull;
    for (int i = 0; i < N; i++) {
        r = r * 6364136223846793005ull + 1442695040888963407ull;
        buf[i] = (uint8_t)(r >> 56);
    }
    size_t bad = 0;
    for (size_t len = 0; len <= N; len++) {
        size_t one = len;
        if (settle_xxh64_pieces(buf, &one, 1) != xxh64(buf, len, 0))
            bad++;
        for (int t = 0; t < 20; t++) {
            size_t lens[16], n = 0, left = len;
            while (left && n < 15) {
                r = r * 6364136223846793005ull + 1442695040888963407ull;
                size_t k = (size_t)(r >> 33) % (left + 1) / (t & 1 ? 1 : 4);
                lens[n++] = k;
                left -= k;
            }
            lens[n++] = left;
            if (settle_xxh64_pieces(buf, lens, n) != xxh64(buf, len, 0))
                bad++;
        }
    }
    if (bad) {
        fprintf(stderr, "FAIL the streaming xxh64 differs from xxh64 in %zu of %d cases\n", bad, 21 * (N + 1));
        failures++;
    }
    printf("  hash: the streaming xxh64 equals xxh64 at every length up to %d and any split\n", N);
}

static void checks_case(void)
{
    where w;
    fresh("checks", &w);
    tier_env(&w, 3600, 86400, 0);
    atomic_store(&NOW, 1000); /* nothing due: only the calls below settle */
    eopts e = {.merge_threads = 0, .classes = 0};
    tdb d;
    CHECK(t_open(&w, &e, &d) == 0);
    if (!d.db)
        return;
    eng *db = d.db;
    enum { KEYS = 7 };
    uint32_t s;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    for (int k = 0; k < KEYS; k++)
        write_rows(db, s, k * DAY, 200000, 200, NOISY); /* about 750 KB a key: several stripes */
    CHECK(eng_flush(db) == 0);
    CHECK(eng_optimize(db) == 0);
    part *p[KEYS][4];
    for (int k = 0; k < KEYS; k++) {
        size_t n = key_parts(db, k, p[k], 4);
        CHECK(n == 1);
        if (n != 1) {
            t_close(&d);
            return;
        }
        chunk_geom g;
        CHECK(chunk_geom_derive(&g, 2, 1, p[k][0]->map_len) == 0 && g.k == 2);
        CHECK(p[k][0]->map_len > (uint64_t)g.k * g.cell); /* more than one stripe */
    }
    const uint64_t foot_rows = offsetof(part_footer, rows) + 7;
    /* 0: a footer byte flipped before the settle: the hash the open checked fails (indexhash) */
    part *x = p[0][0];
    flip(w.db, x->name, x->map_len - sizeof(part_footer) + foot_rows);
    settle_expect(&w, db, 0, TSDB_ERR_CORRUPT, "a footer byte flipped");
    /* 1: the same, with the file's footer hash made to match: only the open's hash tells (openhash) */
    x = p[1][0];
    uint64_t fo = x->map_len - sizeof(part_footer);
    flip(w.db, x->name, fo + foot_rows);
    const uint8_t *m = x->map;
    uint64_t h = xxh64(m, x->hdr_len, 0);
    h = xxh64(m + x->index_off, x->index_len, h);
    if (fo > x->index_off + x->index_len)
        h = xxh64(m + x->index_off + x->index_len, fo - x->index_off - x->index_len, h);
    h = xxh64(m + fo, offsetof(part_footer, index_hash), h);
    char path[PATH_MAX + 128];
    snprintf(path, sizeof path, "%s/%s", w.db, x->name);
    int fd = open(path, O_RDWR);
    CHECK(fd >= 0 && pwrite(fd, &h, sizeof h, (off_t)(fo + offsetof(part_footer, index_hash))) == sizeof h);
    if (fd >= 0)
        close(fd);
    settle_expect(&w, db, 1, TSDB_ERR_CORRUPT, "a footer byte flipped and its hash rewritten");
    /* 2: a header byte flipped after the early copy, before the first stripe (hdrcmp) */
    plant(&w, p[2][0], SETTLE_HOOK_PREPARED, offsetof(part_header, wal_seq) + 7);
    settle_expect(&w, db, 2, TSDB_ERR_CORRUPT, "the header changed after its check");
    /* 3: a footer byte flipped after the first stripe was copied: the last stripe's copy differs (tailcmp) */
    x = p[3][0];
    plant(&w, x, 0, x->map_len - sizeof(part_footer) + foot_rows);
    settle_expect(&w, db, 3, TSDB_ERR_CORRUPT, "the footer changed after its check");
    /* 4: a byte flipped in the second stripe's share of the granule that straddles the first stripe edge (straddle) */
    x = p[4][0];
    chunk_geom g;
    CHECK(chunk_geom_derive(&g, 2, 1, x->map_len) == 0);
    uint64_t edge = (uint64_t)g.k * g.cell, at = 0;
    for (uint32_t i = 0; i < x->ng && !at; i++) {
        granule gr;
        gx_load(x, i, GW_LOC | GW_TIME, &gr);
        if (gr.off < edge && gr.off + gr.ts_len + gr.val_len > edge + 1)
            at = gr.off + gr.ts_len + gr.val_len - 1;
    }
    CHECK(at > edge);
    if (at)
        flip(w.db, x->name, at);
    settle_expect(&w, db, 4, TSDB_ERR_CORRUPT, "a straddling granule's second half flipped");
    /* 5: a part in a merge is refused before anything is encoded (nomerging: the swap's own check catches it after
     * a whole encode and put, STOPPED) */
    size_t c0 = chunks(&w);
    pthread_mutex_lock(&db->lock);
    p[5][0]->merging = true;
    pthread_mutex_unlock(&db->lock);
    settle_expect(&w, db, 5, TSDB_ERR_NOTFOUND, "a part in a merge");
    pthread_mutex_lock(&db->lock);
    p[5][0]->merging = false;
    pthread_mutex_unlock(&db->lock);
    CHECK(chunks(&w) == c0);
    /* 6 and 5: the same parts untouched settle */
    settle_expect(&w, db, 5, 0, "an untouched part");
    settle_expect(&w, db, 6, 0, "an untouched part");
    for (int k = 0; k < KEYS; k++)
        unref_all(p[k], 1);
    CHECK(count_names(w.db, "", ".ec") == 2);
    t_close(&d);
    printf("  checks: each check before the encode on its own (the open's hash, the footer's, the header and tail "
           "copies, a straddling granule) aborts; a merging part is refused before the encode\n");
}

/* ------------------------------------------------------------ a class contradiction */

static void class_case(void)
{
    where w;
    fresh("class", &w);
    tier_env(&w, 3600, 86400, 0);
    atomic_store(&NOW, 1000);
    eopts e = {.merge_threads = 0, .classes = 1};
    tdb d;
    CHECK(t_open(&w, &e, &d) == 0);
    if (!d.db)
        return;
    eng *db = d.db;
    uint32_t a, b;
    CHECK(eng_series(db, "a", TSDB_FLOAT32, &a) == 0 && eng_series(db, "b", TSDB_FLOAT32, &b) == 0);
    int64_t t0 = 0, t1 = 0; /* class 0 from 0 (a fresh series this slow would be classed coarser) */
    CHECK(eng_class_switch(db, a, 0, &t0) == 0 && eng_class_switch(db, b, 0, &t1) == 0 && t0 == 0 && t1 == 0);
    write_rows(db, a, 0, 20000, 2000, 0);
    write_rows(db, b, 2 * DAY, 20000, 2000, 0);
    CHECK(eng_flush(db) == 0);
    int64_t ka = partition_of(db, a, 0), kb = partition_of(db, b, 2 * DAY);
    CHECK(partition_class(db, ka) == 0 && partition_class(db, kb) == 0);
    /* a's rows all lie in class 1 by the history: the granules' bounds say so (noclass) */
    CHECK(cls_test_apply(db, a, 0, 1) == 0);
    settle_expect(&w, db, ka, TSDB_ERR_CORRUPT, "a series in another class");
    /* b is class 1 for 10 s inside its first granule, class 0 at both ends: only its decoded rows say so
     * (judgerows) */
    CHECK(cls_test_apply(db, b, 2 * DAY + 10000, 1) == 0 && cls_test_apply(db, b, 2 * DAY + 20000, 0) == 0);
    settle_expect(&w, db, kb, TSDB_ERR_CORRUPT, "rows of another class inside a granule");
    CHECK(count_names(w.db, "", ".ec") == 0);
    CHECK(chunks(&w) == 0);
    t_close(&d);
    printf("  class: a part that CLASSES contradicts (whole granules, rows inside one) aborts with no .ec\n");
}

static void down_case(void)
{
    where w;
    fresh("down", &w);
    tier_env(&w, 1, 86400, 0);
    eopts e = {.merge_threads = 0, .classes = 0};
    tdb d;
    CHECK(t_open(&w, &e, &d) == 0);
    if (!d.db)
        return;
    uint32_t s;
    CHECK(eng_series(d.db, "s", TSDB_FLOAT32, &s) == 0);
    write_rows(d.db, s, 0, 5000, 2000, 0);
    CHECK(eng_flush(d.db) == 0);
    acc want = digest(d.db, "s");
    CHECK(t_close(&d) == 0);
    char away[PATH_MAX + 16];
    snprintf(away, sizeof away, "%s.away", w.st[2]);
    CHECK(rename(w.st[2], away) == 0);
    CHECK(t_open(&w, &e, &d) == 0);
    if (d.db) {
        CHECK(settle_key(d.db, 0, NULL) == TSDB_ERR_UNAVAILABLE);
        atomic_store(&NOW, 30 * DAY); /* due, but the thread must not try */
        uint64_t f0 = atomic_load(&settle_failed), d0 = atomic_load(&settle_done);
        wait_rounds(5);
        CHECK(atomic_load(&settle_failed) == f0 && atomic_load(&settle_done) == d0);
        CHECK(same(digest(d.db, "s"), want));
        size_t all, ec;
        key_count(d.db, 0, &all, &ec);
        CHECK(ec == 0);
    }
    t_close(&d);
    CHECK(rename(away, w.st[2]) == 0);
    CHECK(chunks(&w) == 0);
    printf("  down: a store moved away: UNAVAILABLE, and the thread settles nothing\n");
}

/* ------------------------------------------------------------ the policy */

static void policy_case(void)
{
    /* a day key with merges on: due once its end is grace_s old and no merge of it is selectable */
    where w;
    fresh("policy", &w);
    tier_env(&w, 3600, 86400, 0);
    atomic_store(&NOW, 1000);
    eopts e = {.merge_threads = 1, .classes = 0};
    tdb d;
    CHECK(t_open(&w, &e, &d) == 0);
    if (!d.db)
        return;
    uint32_t s;
    CHECK(eng_series(d.db, "s", TSDB_FLOAT32, &s) == 0);
    write_rows(d.db, s, 0, 20000, 2000, 0);
    CHECK(eng_flush(d.db) == 0);
    write_rows(d.db, s, 1000, 20000, 2000, 1);
    CHECK(eng_flush(d.db) == 0);
    acc want = digest(d.db, "s");
    atomic_store(&NOW, DAY + HOUR - 60000); /* a minute short of grace */
    wait_rounds(5);
    size_t all, ec;
    key_count(d.db, 0, &all, &ec);
    CHECK(ec == 0);
    atomic_store(&NOW, DAY + HOUR + 1);
    CHECK(wait_settled(d.db, 0, 20000));
    key_count(d.db, 0, &all, &ec);
    CHECK(all == 1 && ec == 1); /* merged first, into one */
    CHECK(same(digest(d.db, "s"), want));
    t_close(&d);

    /* D11: no merge thread, nothing selectable: a day key settles after grace_s all the same; never while the day
     * still takes rows (NOW past its start + grace_s), never with a damaged part in the key */
    fresh("nomerge", &w);
    tier_env(&w, 3600, 86400, 0);
    atomic_store(&NOW, 1000);
    e.merge_threads = 0;
    CHECK(t_open(&w, &e, &d) == 0);
    if (!d.db)
        return;
    CHECK(eng_series(d.db, "s", TSDB_FLOAT32, &s) == 0);
    for (int k = 0; k < 2; k++) {
        write_rows(d.db, s, k * DAY, 5000, 2000, 0);
        CHECK(eng_flush(d.db) == 0);
        write_rows(d.db, s, k * DAY + 1000, 5000, 2000, 1);
        CHECK(eng_flush(d.db) == 0);
    }
    part *p[4];
    size_t n = key_parts(d.db, 1, p, 4);
    CHECK(n == 2);
    if (n)
        atomic_store(&p[0]->damaged, true);
    unref_all(p, n);
    atomic_store(&NOW, 3 * HOUR);
    wait_rounds(5);
    key_count(d.db, 0, &all, &ec);
    CHECK(all == 2 && ec == 0);
    atomic_store(&NOW, 2 * DAY + HOUR + 1);
    CHECK(wait_settled(d.db, 0, 20000));
    key_count(d.db, 0, &all, &ec);
    CHECK(all == 2 && ec == 2);
    wait_rounds(5);
    key_count(d.db, 1, &all, &ec);
    CHECK(all == 2 && ec == 0);
    t_close(&d);

    /* a merge thread whose idle round is far off: two level-0 parts are still to be rewritten, so the key waits
     * (settlebusy settles it) */
    fresh("busy", &w);
    tier_env(&w, 3600, 86400, 0);
    atomic_store(&NOW, 1000);
    e.merge_threads = 1;
    e.idle = 1000000;
    CHECK(t_open(&w, &e, &d) == 0);
    if (!d.db)
        return;
    CHECK(eng_series(d.db, "s", TSDB_FLOAT32, &s) == 0);
    write_rows(d.db, s, 0, 5000, 2000, 0);
    write_rows(d.db, s, DAY, 5000, 2000, 0); /* key 1: one level-0 part, which an idle round rewrites alone */
    CHECK(eng_flush(d.db) == 0);
    write_rows(d.db, s, 1000, 5000, 2000, 1);
    CHECK(eng_flush(d.db) == 0);
    atomic_store(&NOW, 30 * DAY);
    wait_rounds(10);
    key_count(d.db, 0, &all, &ec);
    CHECK(all == 2 && ec == 0);
    key_count(d.db, 1, &all, &ec);
    CHECK(all == 1 && ec == 0);
    t_close(&d);

    /* merge_idle_s = -1: no part is rewritten alone and nothing merged as idle, so nothing is waited for */
    fresh("noidle", &w);
    tier_env(&w, 3600, 86400, 0);
    atomic_store(&NOW, 1000);
    e.idle = -1;
    CHECK(t_open(&w, &e, &d) == 0);
    if (!d.db)
        return;
    CHECK(eng_series(d.db, "s", TSDB_FLOAT32, &s) == 0);
    write_rows(d.db, s, 0, 5000, 2000, 0);
    CHECK(eng_flush(d.db) == 0);
    write_rows(d.db, s, 1000, 5000, 2000, 1);
    CHECK(eng_flush(d.db) == 0);
    atomic_store(&NOW, 30 * DAY);
    CHECK(wait_settled(d.db, 0, 20000));
    t_close(&d);
    printf("  policy: a day key settles after grace_s and its merges, not before, never with a merge selectable or a "
           "damaged part; without merges (threads 0, idle off) it settles\n");
}

static void coarse_case(void)
{
    where w;
    fresh("coarse", &w);
    tier_env(&w, 4 * 86400, 7 * 86400, 0); /* grace 4 days: the first settle comes after coarse_s from 0 */
    atomic_store(&NOW, 1000);
    eopts e = {.merge_threads = 0, .classes = 1};
    tdb d;
    CHECK(t_open(&w, &e, &d) == 0);
    if (!d.db)
        return;
    uint32_t s;
    CHECK(eng_series(d.db, "s", TSDB_FLOAT32, &s) == 0);
    int64_t t0 = 0;
    CHECK(eng_class_switch(d.db, s, 1, &t0) == 0);
    write_rows(d.db, s, 0, 2000, 10000, 0); /* day 0: part 1 */
    CHECK(eng_flush(d.db) == 0);
    /* Nothing is due at 1000, part 1 alone included: two rounds run to the end any settle a round picks (it runs its
     * settle inside the round). Without this check coarsewhole, which picks part 1 here, is caught only when no round
     * falls between the two flushes: a record written after NOW moves on to 8 days holds the key back until 15 days,
     * which the checks below cannot tell from a right settle of part 1 (gate under load, 2026-11-21). */
    size_t all0, ec0;
    wait_rounds(2);
    key_count(d.db, 1, &all0, &ec0);
    CHECK(all0 == 1 && ec0 == 0);
    write_rows(d.db, s, 5 * DAY, 2000, 10000, 1); /* day 5, the same week: part 2 */
    CHECK(eng_flush(d.db) == 0);
    acc want = digest(d.db, "s");
    part *p[4];
    size_t n = key_parts(d.db, 1, p, 4);
    CHECK(n == 2);
    unref_all(p, n);
    if (n != 2) {
        t_close(&d);
        return;
    }
    wait_rounds(2);
    key_count(d.db, 1, &all0, &ec0);
    CHECK(all0 == 2 && ec0 == 0);
    atomic_store(&NOW, 8 * DAY); /* part 1 is due, part 2 is not */
    for (int i = 0; i < 2000; i++) {
        size_t all, ec;
        key_count(d.db, 1, &all, &ec);
        if (ec)
            break;
        sleep_ms(10);
    }
    wait_rounds(5);
    size_t all, ec;
    key_count(d.db, 1, &all, &ec);
    CHECK(all == 2 && ec == 1); /* coarsewhole settles both */
    CHECK(same(digest(d.db, "s"), want));
    atomic_store(&NOW, 11 * DAY); /* part 2 due now, but the last settle (at 8 days) is less than coarse_s old */
    wait_rounds(5);
    key_count(d.db, 1, &all, &ec);
    CHECK(ec == 1);
    CHECK(t_close(&d) == 0);
    CHECK(t_open(&w, &e, &d) == 0); /* a restart remembers it through the .ec record's settle time */
    if (!d.db)
        return;
    wait_rounds(5);
    key_count(d.db, 1, &all, &ec);
    CHECK(all == 2 && ec == 1);
    atomic_store(&NOW, 15 * DAY + 1000);
    CHECK(wait_settled(d.db, 1, 20000));
    CHECK(same(digest(d.db, "s"), want));
    t_close(&d);
    printf("  coarse: a class-1 key part by part, not again within coarse_s, also across a restart\n");
}

static void backoff_case(void)
{
    where w;
    fresh("backoff", &w);
    tier_env(&w, 3600, 86400, 0);
    atomic_store(&NOW, 1000);
    eopts e = {.merge_threads = 0, .classes = 0};
    tdb d;
    CHECK(t_open(&w, &e, &d) == 0);
    if (!d.db)
        return;
    uint32_t s;
    CHECK(eng_series(d.db, "s", TSDB_FLOAT32, &s) == 0);
    write_rows(d.db, s, 0, 5000, 2000, 0);
    CHECK(eng_flush(d.db) == 0);
    CHECK(eng_optimize(d.db) == 0); /* level 1: no merge selectable */
    /* puts fail while the volume's directories in the stores are read-only (an object takes k' + m of the three) */
    char vd[3][PATH_MAX + 16];
    struct stat sb[3];
    bool have = true;
    for (int i = 0; i < 3; i++) {
        snprintf(vd[i], sizeof vd[i], "%s/s0", w.st[i]);
        have &= stat(vd[i], &sb[i]) == 0;
    }
    CHECK(have);
    for (int i = 0; have && i < 3; i++)
        CHECK(chmod(vd[i], 0555) == 0);
    uint64_t f0 = atomic_load(&settle_failed);
    atomic_store(&NOW, 3 * DAY);
    for (int i = 0; i < 2000 && atomic_load(&settle_failed) == f0; i++)
        sleep_ms(10);
    CHECK(atomic_load(&settle_failed) == f0 + 1);
    for (int i = 0; have && i < 3; i++)
        CHECK(chmod(vd[i], sb[i].st_mode & 07777) == 0);
    atomic_store(&NOW, 3 * DAY + HOUR - 1000); /* within the hour: left alone */
    wait_rounds(5);
    size_t all, ec;
    key_count(d.db, 0, &all, &ec);
    CHECK(ec == 0 && atomic_load(&settle_failed) == f0 + 1);
    atomic_store(&NOW, 3 * DAY + HOUR + 1000);
    CHECK(wait_settled(d.db, 0, 20000));
    t_close(&d);
    printf("  backoff: a failed settle is not retried within the hour, then is\n");
}

static void eager_case(void)
{
    enum { KEYS = 34 };
    where w;
    fresh("eager", &w);
    tier_env(&w, 1, 86400, 1);
    atomic_store(&NOW, 1000);
    eopts e = {.merge_threads = 0, .classes = 0};
    tdb d;
    CHECK(t_open(&w, &e, &d) == 0);
    if (!d.db)
        return;
    uint32_t s;
    CHECK(eng_series(d.db, "s", TSDB_FLOAT32, &s) == 0);
    for (int k = 0; k < KEYS; k++)
        write_rows(d.db, s, k * DAY, 500, 2000, 0);
    CHECK(eng_flush(d.db) == 0);
    acc want = digest(d.db, "s");
    atomic_store(&NOW, 100 * DAY);
    wait_rounds(5); /* level 0 and small: not yet */
    CHECK(count_names(w.db, "", ".ec") == 0);
    CHECK(eng_optimize(d.db) == 0); /* a merge round on every key */
    for (int i = 0; i < 2000 && count_names(w.db, "", ".ec") < 30; i++)
        sleep_ms(10);
    wait_rounds(5);
    CHECK(count_names(w.db, "", ".ec") == ECPOLICY_EAGER_PER_MIN); /* the minute's budget */
    atomic_store(&NOW, 100 * DAY + 61000);
    for (int i = 0; i < 2000 && count_names(w.db, "", ".ec") < KEYS; i++)
        sleep_ms(10);
    CHECK(count_names(w.db, "", ".ec") == KEYS);
    CHECK(same(digest(d.db, "s"), want));
    write_rows(d.db, s, 100 * DAY + 60000, 500, 2, 0); /* its newest row less than grace_s old */
    CHECK(eng_flush(d.db) == 0);
    CHECK(eng_optimize(d.db) == 0);
    wait_rounds(5);
    CHECK(count_names(w.db, "", ".ec") == KEYS);
    t_close(&d);
    printf("  eager: nothing before a merge round, then %d a minute\n", ECPOLICY_EAGER_PER_MIN);
}

/* ------------------------------------------------------------ close during an encode */

static atomic_int slow_started;
static void slow_hook(eng *db, const part *p, uint64_t stripe)
{
    (void)db, (void)p, (void)stripe;
    atomic_store(&slow_started, 1);
    sleep_ms(300);
}

static void close_case(void)
{
    where w;
    fresh("close", &w);
    tier_env(&w, 3600, 86400, 0);
    atomic_store(&NOW, 1000);
    eopts e = {.merge_threads = 0, .classes = 0};
    tdb d;
    CHECK(t_open(&w, &e, &d) == 0);
    if (!d.db)
        return;
    uint32_t s;
    CHECK(eng_series(d.db, "s", TSDB_FLOAT32, &s) == 0);
    write_rows(d.db, s, 0, 400000, 200, NOISY); /* about 1.5 MB: several stripes */
    CHECK(eng_flush(d.db) == 0);
    CHECK(eng_optimize(d.db) == 0);
    acc want = digest(d.db, "s");
    atomic_store(&slow_started, 0);
    atomic_store(&settle_fill_hook, slow_hook);
    uint64_t a0 = atomic_load(&settle_abandoned), d0 = atomic_load(&settle_done);
    atomic_store(&NOW, 3 * DAY);
    for (int i = 0; i < 2000 && !atomic_load(&slow_started); i++)
        sleep_ms(10);
    CHECK(atomic_load(&slow_started));
    CHECK(eng_close(d.db) == 0); /* waits for the settle, which gives up */
    d.db = NULL;
    atomic_store(&settle_fill_hook, NULL);
    CHECK(atomic_load(&settle_abandoned) == a0 + 1 && atomic_load(&settle_done) == d0);
    t_close(&d);
    CHECK(chunks(&w) == 0); /* abandoned in the fill: no position was committed, nothing for the gc */
    atomic_store(&NOW, 1000 + 3 * DAY); /* the clock never goes back */
    CHECK(count_names(w.db, "", ".ec") == 0);
    CHECK(t_open(&w, &e, &d) == 0);
    if (d.db) {
        CHECK(same(digest(d.db, "s"), want));
        CHECK(wait_settled(d.db, 0, 20000)); /* and the next open settles it */
        CHECK(same(digest(d.db, "s"), want));
    }
    t_close(&d);
    printf("  close: an engine closed during a slow encode abandons the settle; the next open settles the key\n");
}

/* A halt (a volume about to be reopened: tsdb_volume_reopen) stops the engine's settle before it returns, as a close
 * does: a settle of the halted engine that went on would rename its sources and temporaries in a directory the new
 * engine owns. The fill sleeps 300 ms per stripe, so a halt that did not wait for the settle (nohaltstop) returns with
 * it still running. The stop order after it (settle, then the keep digest, then the parts) is the close's. */
static void halt_case(void)
{
    where w;
    fresh("halt", &w);
    tier_env(&w, 3600, 86400, 0);
    atomic_store(&NOW, 1000);
    eopts e = {.merge_threads = 0, .classes = 0};
    tdb d;
    CHECK(t_open(&w, &e, &d) == 0);
    if (!d.db)
        return;
    uint32_t s;
    CHECK(eng_series(d.db, "s", TSDB_FLOAT32, &s) == 0);
    write_rows(d.db, s, 0, 400000, 200, NOISY);
    CHECK(eng_flush(d.db) == 0);
    CHECK(eng_optimize(d.db) == 0);
    acc want = digest(d.db, "s");
    atomic_store(&slow_started, 0);
    atomic_store(&settle_fill_hook, slow_hook);
    uint64_t a0 = atomic_load(&settle_abandoned), d0 = atomic_load(&settle_done);
    atomic_store(&NOW, 3 * DAY);
    for (int i = 0; i < 2000 && !atomic_load(&slow_started); i++)
        sleep_ms(10);
    CHECK(atomic_load(&slow_started));
    eng_halt(d.db);
    bool stopped = atomic_load(&settle_abandoned) == a0 + 1 && atomic_load(&settle_done) == d0;
    if (!stopped)
        fprintf(stderr, "FAIL halt: eng_halt returned with the engine's settle still running\n");
    CHECK(stopped);
    atomic_store(&settle_fill_hook, NULL);
    CHECK(eng_close(d.db) == 0);
    d.db = NULL;
    t_close(&d);
    CHECK(chunks(&w) == 0 && count_names(w.db, "", ".ec") == 0 && count_names(w.db, "settled_", "") == 0);
    atomic_store(&NOW, 1000 + 3 * DAY);
    CHECK(t_open(&w, &e, &d) == 0);
    if (d.db)
        CHECK(same(digest(d.db, "s"), want));
    t_close(&d);
    printf("  halt: a halted engine's settle is abandoned before eng_halt returns\n");
}

/* ------------------------------------------------------------ 8 writers, merges, a drop, settles */

enum { WRITERS = 8 };
typedef struct {
    eng *db;
    int id;
    unsigned seed;
    int iters;
} wctx;

static void ops(eng *db, int id, unsigned seed, int iters)
{
    char name[16];
    snprintf(name, sizeof name, "w%d", id);
    uint32_t s;
    CHECK(eng_series(db, name, TSDB_FLOAT32, &s) == 0);
    int64_t ts[3000];
    float v[3000];
    for (int it = 0; it < iters; it++) {
        int op = rand_r(&seed) % 4, n = op == 1 ? 3000 : op == 3 ? 400 : 60;
        int64_t t0 = DAY + (int64_t)(rand_r(&seed) % (3 * 86400 - 3000)) * 1000;
        for (int j = 0; j < n; j++)
            ts[j] = t0 + (int64_t)j * 1000, v[j] = (float)(rand_r(&seed) % 100000) / 8;
        if (op == 0 || op == 1)
            CHECK(eng_insert_series(db, TSDB_FLOAT32, s, ts, v, (size_t)n) == 0);
        else if (op == 2)
            CHECK(eng_delete_range(db, s, t0, t0 + (int64_t)(rand_r(&seed) % 20000) * 1000) == 0);
        else
            CHECK(eng_replace_range(db, TSDB_FLOAT32, s, t0, t0 + (int64_t)n * 1000, ts, v, (size_t)n) == 0);
    }
}

static void *writer(void *arg)
{
    wctx *c = arg;
    ops(c->db, c->id, c->seed, c->iters);
    return NULL;
}

static void threads_case(void)
{
    where w;
    fresh("threads", &w);
    tier_env(&w, 1, 86400, 0);
    atomic_store(&NOW, 1000);
    eopts e = {.merge_threads = 2, .classes = 0, .wal_max_batch_rows = 1000};
    tdb d;
    CHECK(t_open(&w, &e, &d) == 0);
    eng *o = o_open(&w, &e);
    if (!d.db || !o)
        return;
    int iters = quick ? 40 : 150;
    for (int k = 0; k < 2; k++) {
        eng *db = k ? o : d.db;
        for (int i = 0; i < WRITERS; i++) {
            char name[16];
            uint32_t s;
            snprintf(name, sizeof name, "w%d", i);
            CHECK(eng_series(db, name, TSDB_FLOAT32, &s) == 0);
            write_rows(db, s, 0, 3000, 20000, i); /* day 0, which the drop takes */
        }
        CHECK(eng_flush(db) == 0);
    }
    CHECK(settle_key(d.db, 0, NULL) == 0); /* the drop meets an .ec key */
    atomic_store(&NOW, 30 * DAY); /* the thread settles too, wherever no merge is selectable */
    pthread_t th[WRITERS];
    wctx c[WRITERS];
    for (int i = 0; i < WRITERS; i++) {
        c[i] = (wctx){d.db, i, 1234u + (unsigned)i * 77u, iters};
        CHECK(pthread_create(&th[i], NULL, writer, &c[i]) == 0);
    }
    size_t settles = 0, stopped = 0;
    for (int r = 0; r < (quick ? 60 : 200); r++) {
        if (r == 20)
            CHECK(eng_drop_before(d.db, DAY) == 0);
        if (r % 7 == 0)
            CHECK(eng_flush(d.db) == 0);
        int64_t key = 1 + r % 3;
        int rc = settle_key(d.db, key, NULL);
        CHECK(rc == 0 || rc == TSDB_ERR_NOTFOUND || rc == TSDB_ERR_STOPPED);
        settles += rc == 0;
        stopped += rc == TSDB_ERR_STOPPED;
        sleep_ms(5);
    }
    for (int i = 0; i < WRITERS; i++)
        pthread_join(th[i], NULL);
    for (int i = 0; i < WRITERS; i++)
        ops(o, i, 1234u + (unsigned)i * 77u, iters);
    CHECK(eng_drop_before(o, DAY) == 0);
    for (int k = 1; k <= 3; k++)
        settle_key(d.db, k, NULL);
    CHECK(settles > 0);
    int bad = 0;
    for (int i = 0; i < WRITERS; i++) {
        char name[16];
        snprintf(name, sizeof name, "w%d", i);
        bad += !same(digest(d.db, name), digest(o, name));
    }
    CHECK(bad == 0);
    CHECK(t_close(&d) == 0);
    CHECK(t_open(&w, &e, &d) == 0);
    if (d.db)
        for (int i = 0; i < WRITERS; i++) {
            char name[16];
            snprintf(name, sizeof name, "w%d", i);
            CHECK(same(digest(d.db, name), digest(o, name)));
        }
    t_close(&d);
    eng_close(o);
    printf("  threads: %d writers, merges, a drop, %zu settles (%zu stopped): every series equals the oracle, also "
           "after a reopen\n",
           WRITERS, settles, stopped);
}

/* ------------------------------------------------------------ crash points and power loss */

/* grace_s for the crash and power cases: the settle thread never picks (merges are off, so their two level-0 parts
 * would be due at once, D11), only the settles the case makes run */
enum { QUIET_GRACE = 315360000 };
static where BASE;
static acc BWANT;

static void make_base(void)
{
    fresh("base", &BASE);
    tier_env(&BASE, QUIET_GRACE, 86400, 0);
    eopts e = {.merge_threads = 0, .classes = 0};
    tdb d;
    CHECK(t_open(&BASE, &e, &d) == 0);
    if (!d.db)
        return;
    uint32_t s;
    CHECK(eng_series(d.db, "s", TSDB_FLOAT32, &s) == 0);
    write_rows(d.db, s, 0, 20000, 2000, 0);
    CHECK(eng_flush(d.db) == 0);
    write_rows(d.db, s, 1000, 20000, 2000, 1);
    CHECK(eng_flush(d.db) == 0);
    BWANT = digest(d.db, "s");
    CHECK(t_close(&d) == 0);
}

/* a copy of BASE (the database and its stores: one root) */
static void copy_base(const char *what, where *w)
{
    fresh(what, w);
    char cmd[4 * PATH_MAX];
    snprintf(cmd, sizeof cmd, "rm -rf '%s' && cp -a '%s' '%s'", w->root, BASE.root, w->root);
    CHECK(system(cmd) == 0);
}

static int child_gcpower(const where *w);
static int child_settle(const char *root)
{
    where w;
    snprintf(w.root, sizeof w.root, "%s", root);
    snprintf(w.db, sizeof w.db, "%s/db", root);
    for (int i = 0; i < 3; i++)
        snprintf(w.st[i], sizeof w.st[i], "%s/st%d", root, i);
    const char *kind = getenv("TSDB_TEST_SETTLE_CHILD");
    if (kind && !strcmp(kind, "gcpower"))
        return child_gcpower(&w);
    tier_env(&w, QUIET_GRACE, 86400, 0);
    eopts e = {.merge_threads = 0, .classes = 0};
    tdb d;
    if (t_open(&w, &e, &d) != 0)
        return 3;
    int rc = settle_key(d.db, 0, NULL);
    int crc = t_close(&d);
    return rc == 0 && crc == 0 ? 0 : 4;
}

static const char *exit_mark; /* CRASHFS_EXIT_MARK of the next child under the shim, NULL: none */
static bool no_tear;          /* the next child's power loss drops everything not synced (no CRASHFS_TEAR) */
static pid_t child_pid;       /* the last child's pid */

static int run_child(const where *w, const char *crash_at, int warmup, int seed)
{
    pid_t pid = fork();
    if (pid == 0) {
        if (crash_at) {
            setenv("TSDB_SETTLE_CRASH_AT", crash_at, 1);
            /* the crash point leaves through exit(99) (a power cut under crashfs) with the engine still open, which
             * a leak check at exit would report (and change the status) */
            char asan[512];
            snprintf(asan, sizeof asan, "%s:detect_leaks=0", getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
            setenv("ASAN_OPTIONS", asan, 1);
        }
        if (exit_mark)
            setenv("CRASHFS_EXIT_MARK", exit_mark, 1);
        if (warmup >= 0) {
            char asan[512], wu[16], sd[16];
            snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0:detect_leaks=0",
                     getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
            setenv("ASAN_OPTIONS", asan, 1);
            snprintf(wu, sizeof wu, "%d", warmup);
            snprintf(sd, sizeof sd, "%d", seed);
            setenv("LD_PRELOAD", SHIM, 1);
            setenv("CRASHFS_ROOT", w->root, 1);
            setenv("CRASHFS_PROB", "1", 1);
            setenv("CRASHFS_WARMUP", wu, 1);
            setenv("CRASHFS_AT_EXIT", "1", 1);
            if (!no_tear)
                setenv("CRASHFS_TEAR", "1", 1);
            setenv("CRASHFS_SEED", sd, 1);
        }
        int null = open("/dev/null", O_WRONLY);
        if (null >= 0) {
            dup2(null, 1);
            dup2(null, 2);
        }
        execl(SELF, "test_settle", "--child", w->root, "--mutant", mutant, (char *)NULL);
        _exit(127);
    }
    child_pid = pid;
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

/* reopens w: the rows equal the base's, no temporary is left; 2: the key is all .ec, 1: all .part, -1 otherwise */
static size_t stores_exact(const where *w, eng *db, const char *what);
static int reopen_check(const where *w, const char *what)
{
    tier_env(w, QUIET_GRACE, 86400, 0);
    eopts e = {.merge_threads = 0, .classes = 0};
    tdb d;
    bool manual = atomic_exchange(&ecgc_test_manual, true); /* the test works the delete queue itself */
    int rc = t_open(w, &e, &d);
    atomic_store(&ecgc_test_manual, manual);
    CHECK(rc == 0);
    if (rc) {
        fprintf(stderr, "  (%s: the reopen said %d)\n", what, rc);
        return -1;
    }
    /* the open's sweep and a gc round: no chunk of an object nothing names is left, every listed .ec is whole */
    ecgc_tier_work(d.t);
    ecgc_tier_work(d.t);
    stores_exact(w, d.db, what);
    bool ok = same(digest(d.db, "s"), BWANT);
    CHECK(ok);
    if (!ok)
        fprintf(stderr, "  (%s: the rows differ)\n", what);
    size_t all, ec;
    key_count(d.db, 0, &all, &ec);
    int st = all && ec == all ? 2 : ec == 0 ? 1 : -1;
    CHECK(st > 0);
    CHECK(count_names(w->db, "tmp_", "") == 0);
    if (st == 1) { /* the settle can be done again */
        CHECK(settle_key(d.db, 0, NULL) == 0);
        CHECK(same(digest(d.db, "s"), BWANT));
    }
    t_close(&d);
    return st;
}

static void crash_case(void)
{
    make_base();
    where w;
    const char *points[] = {"after-put", "after-ec-tmp"};
    for (int i = 0; i < 2; i++) {
        copy_base("cp", &w);
        CHECK(run_child(&w, points[i], -1, 0) == 99);
        CHECK(reopen_check(&w, points[i]) == 1);
    }
    /* under crashfs (whose own power loss is never due here) a crash point must be a power cut: the hook leaves
     * through exit(99), so crashfs's destructor rolls back what was not synced and leaves its mark; jepsen's
     * settle-steps nemesis needs exactly that (a _exit there was a plain crash, and 488 runs cut nothing) */
    if (access(SHIM, R_OK) == 0) {
        char mark[PATH_MAX + 32], mp[PATH_MAX + 64];
        for (int i = 0; i < 2; i++) {
            copy_base("cpcut", &w);
            snprintf(mark, sizeof mark, "%s.cut", w.root);
            exit_mark = mark;
            CHECK(run_child(&w, points[i], 1000000000, 1) == 99);
            exit_mark = NULL;
            snprintf(mp, sizeof mp, "%s.%d", mark, (int)child_pid);
            bool cut = access(mp, F_OK) == 0;
            CHECK(cut);
            if (!cut)
                fprintf(stderr, "  (%s under crashfs: no power cut at the crash point, no %s)\n", points[i], mp);
            unlink(mp);
            CHECK(reopen_check(&w, points[i]) == 1);
        }
    }
    copy_base("cp", &w);
    CHECK(run_child(&w, NULL, -1, 0) == 0);
    CHECK(reopen_check(&w, "no crash") == 2);
    printf("  crash: after-put and after-ec-tmp leave the key as it was; the settle runs again\n");
    if (quick || access(SHIM, R_OK) != 0) {
        printf("  power loss: skipped (%s)\n", quick ? "--quick" : "no libcrashfs.so");
        return;
    }
    int crashes = 0, fwd = 0, back = 0, seeds = 4;
    bool finished = false;
    for (int k = 0; k < 4000 && !finished; k++)
        for (int sd = 0; sd < seeds; sd++) {
            copy_base("pl", &w);
            int code = run_child(&w, NULL, k, k * 7 + sd + 1);
            CHECK(code == 99 || code == 0);
            if (code != 99 && code != 0) {
                fprintf(stderr, "  (power loss at step %d: the child said %d)\n", k, code);
                k = 1 << 20;
                break;
            }
            crashes += code == 99;
            int st = reopen_check(&w, "power loss");
            if (code == 0) {
                finished = true;
                CHECK(st == 2);
            }
            fwd += st == 2;
            back += st == 1;
        }
    CHECK(finished && crashes > 20 && back > 0);
    printf("  power loss at each of %d steps of an open, a settle and a close (%d seeds each): the rows always equal "
           "the base's, the key all .part (%d) or all .ec (%d)\n",
           crashes / seeds, seeds, back, fwd);
}

/* ------------------------------------------------------------ the failure paths' chunks (int-h) */

/* chunk files per object in the three stores, by the oid's hex in the name (o<hex>.<pos>.chunk) */
enum { MAXOBJ = 256 };
static struct {
    char hex[33];
    unsigned n;
} objs[MAXOBJ];
static size_t nobjs;
static int obj_fn(const char *path, const struct stat *st, int type, struct FTW *f)
{
    (void)st;
    const char *b = path + f->base;
    if (type != FTW_F || b[0] != 'o' || strlen(b) < 34 || b[33] != '.' || !strstr(b, ".chunk"))
        return 0;
    for (size_t i = 0; i < nobjs; i++)
        if (!strncmp(objs[i].hex, b + 1, 32)) {
            objs[i].n++;
            return 0;
        }
    if (nobjs < MAXOBJ) {
        snprintf(objs[nobjs].hex, sizeof objs[nobjs].hex, "%.32s", b + 1);
        objs[nobjs++].n = 1;
    }
    return 0;
}

/* The stores hold exactly the objects of the listed .ec parts, each whole (k' + m chunk files), and nothing else.
 * Returns how many chunk files the listed parts need. */
static size_t stores_exact(const where *w, eng *db, const char *what)
{
    nobjs = 0;
    for (int i = 0; i < 3; i++)
        nftw(w->st[i], obj_fn, 16, FTW_PHYS);
    bool used[MAXOBJ] = {false};
    size_t need = 0;
    bool ok = true;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++) {
        struct ecpart *e = atomic_load(&db->parts[i]->ec);
        if (!e)
            continue;
        char hex[33];
        chunk_oid_hex(e->rec.oid, hex);
        unsigned want = (unsigned)e->rec.k + e->rec.m, have = 0;
        need += want;
        for (size_t j = 0; j < nobjs; j++)
            if (!strcmp(objs[j].hex, hex)) {
                used[j] = true;
                have = objs[j].n;
            }
        if (have != want) {
            fprintf(stderr, "FAIL %s: listed %s has %u of its %u chunks\n", what, db->parts[i]->name, have, want);
            ok = false;
        }
    }
    pthread_mutex_unlock(&db->lock);
    for (size_t j = 0; j < nobjs; j++)
        if (!used[j]) {
            fprintf(stderr, "FAIL %s: %u chunks of object %s that no listed part names\n", what, objs[j].n,
                    objs[j].hex);
            ok = false;
        }
    CHECK(ok);
    return need;
}

static void two_parts(eng *db, uint32_t s, int64_t key)
{
    write_rows(db, s, key * DAY, 20000, 2000, 0);
    CHECK(eng_flush(db) == 0);
    write_rows(db, s, key * DAY + 1000, 20000, 2000, 1);
    CHECK(eng_flush(db) == 0);
}

static eng *gc_drop_db;
static void *gc_drop_run(void *arg)
{
    (void)arg;
    return (void *)(intptr_t)eng_drop_before(gc_drop_db, DAY);
}
/* Key 0 goes while its swap waits: STOPPED. The hook runs under the swap's publish_lock, and a drop flushes (flush_lock,
 * which comes before publish_lock), so the drop runs on a thread of its own as a real one does, never inline: its
 * flush finds the memtable empty and needs no publish_lock, and its retire section waits for nothing the swap holds
 * here. */
static void gc_drop_hook(eng *db)
{
    (void)db;
    pthread_t t;
    void *ret = (void *)(intptr_t)-1;
    CHECK(pthread_create(&t, NULL, gc_drop_run, NULL) == 0);
    CHECK(pthread_join(t, &ret) == 0);
    CHECK((intptr_t)ret == 0);
}

/* One failure path on key `key`: settle it with the failure planted, expect rc; the failed objects' chunks must be in
 * the stores (the path did put them) until a gc round, which leaves exactly the listed objects; the rows equal the
 * oracle's; and the objects handed to r7-gc are exactly `handed` (one per object: never twice, F7). */
static void gc_path(const where *w, tdb *d, eng *o, const char *what, int64_t key, int rc_want, size_t handed,
                    size_t left)
{
    ecgc_counters c0, c1;
    ecgc_get_counters(d->t, &c0);
    size_t before = chunks(w);
    int rc = settle_key(d->db, key, NULL);
    if (rc != rc_want)
        fprintf(stderr, "FAIL %s: the settle said %d, not %d\n", what, rc, rc_want);
    CHECK(rc == rc_want);
    CHECK(atomic_load(&settle_test_fail_at) == SETTLE_FAIL_NONE && atomic_load(&ecswap_test_fail) == ECSWAP_FAIL_NONE);
    size_t all, ec;
    key_count(d->db, key, &all, &ec);
    CHECK(all == left && ec == 0);
    CHECK(count_names(w->db, "tmp_", "") == 0);
    CHECK(count_names(w->db, "txn_", ".intent") == 0); /* a failed intent write removes its intent too */
    bool teeth = chunks(w) > before; /* the path is reached after the put: else this case has no teeth */
    if (!teeth)
        fprintf(stderr, "FAIL %s: no chunk was put before the failure\n", what);
    CHECK(teeth);
    ecgc_tier_work(d->t);
    ecgc_get_counters(d->t, &c1);
    if (c1.queued - c0.queued != handed)
        fprintf(stderr, "FAIL %s: %" PRIu64 " objects handed to the gc, not %zu\n", what, c1.queued - c0.queued,
                handed);
    CHECK(c1.queued - c0.queued == handed);
    stores_exact(w, d->db, what);
    CHECK(same(digest(d->db, "s"), digest(o, "s")));
}

static void gcpaths_case(void)
{
    where w;
    fresh("gcpaths", &w);
    tier_env(&w, QUIET_GRACE, 86400, 0);
    atomic_store(&ecgc_test_manual, true); /* the test works the delete queue itself */
    eopts e = {.merge_threads = 0, .classes = 0};
    tdb d;
    CHECK(t_open(&w, &e, &d) == 0);
    eng *o = o_open(&w, &e);
    if (!d.db || !o) {
        atomic_store(&ecgc_test_manual, false);
        return;
    }
    uint32_t s, so;
    CHECK(eng_series(d.db, "s", TSDB_FLOAT32, &s) == 0 && eng_series(o, "s", TSDB_FLOAT32, &so) == 0);
    for (int64_t k = 0; k < 9; k++) {
        two_parts(d.db, s, k);
        two_parts(o, so, k);
    }
    atomic_store(&NOW, 30 * DAY);

    /* a drop of the key inside the swap (STOPPED): swap_discard unlinks, syncs, then hands over */
    gc_drop_db = d.db;
    ec_swap_hook = gc_drop_hook;
    CHECK(eng_drop_before(o, DAY) == 0);
    gc_path(&w, &d, o, "stopped", 0, TSDB_ERR_STOPPED, 2, 0);
    ec_swap_hook = NULL;
    CHECK(same(digest(d.db, "s"), digest(o, "s")));

    struct {
        const char *what;
        int settle_at, skip, swap, rc;
        size_t handed;
    } paths[] = {
        {"put", SETTLE_FAIL_PUT, 0, 0, TSDB_ERR_IO, 1},
        {"index", SETTLE_FAIL_INDEX, 0, 0, TSDB_ERR_IO, 1},
        {"open", SETTLE_FAIL_OPEN, 0, 0, TSDB_ERR_IO, 1},
        {"second", SETTLE_FAIL_OPEN, 1, 0, TSDB_ERR_IO, 2},    /* the first part's object too (eng_settle's else) */
        {"swapnomem", 0, 0, ECSWAP_FAIL_AT_NOMEM, TSDB_ERR_NOMEM, 2},
        {"intentnomem", 0, 0, ECSWAP_FAIL_INTENT_NOMEM, TSDB_ERR_NOMEM, 2}, /* F7: one owner, each object once */
        {"intentio", 0, 0, ECSWAP_FAIL_INTENT_IO, TSDB_ERR_IO, 2},
    };
    for (size_t i = 0; i < sizeof paths / sizeof *paths; i++) {
        int64_t key = 1 + (int64_t)i;
        atomic_store(&settle_test_fail_skip, paths[i].skip);
        atomic_store(&settle_test_fail_at, paths[i].settle_at);
        atomic_store(&ecswap_test_fail, paths[i].swap);
        gc_path(&w, &d, o, paths[i].what, key, paths[i].rc, paths[i].handed, 2);
        atomic_store(&settle_test_fail_at, SETTLE_FAIL_NONE);
        atomic_store(&ecswap_test_fail, ECSWAP_FAIL_NONE);
        /* the same key settled again: nothing a failed settle handed over takes any of its chunks */
        ecgc_counters c0, c1;
        ecgc_get_counters(d.t, &c0);
        CHECK(settle_key(d.db, key, NULL) == 0);
        ecgc_tier_work(d.t);
        ecgc_get_counters(d.t, &c1);
        CHECK(c1.queued == c0.queued); /* a committed swap hands nothing over (gccommitted) */
        CHECK(stores_exact(&w, d.db, paths[i].what) > 0);
        CHECK(same(digest(d.db, "s"), digest(o, "s")));
    }

    /* the reopen's sweep finds nothing to take: every chunk left is a listed part's */
    CHECK(t_close(&d) == 0);
    CHECK(t_open(&w, &e, &d) == 0);
    if (d.db) {
        ecgc_tier_work(d.t); /* the sweep's listing, if the open deferred it, and the queue */
        ecgc_tier_work(d.t);
        ecgc_counters c;
        ecgc_get_counters(d.t, &c);
        CHECK(c.sweeps >= 1 && c.sweeps_skipped == 0 && c.swept == 0); /* the sweep ran and found nothing */
        stores_exact(&w, d.db, "reopen");
        CHECK(same(digest(d.db, "s"), digest(o, "s")));
    }
    t_close(&d);
    atomic_store(&ecgc_test_manual, false);
    CHECK(eng_close(o) == 0);
    printf("  gcpaths: every failure after the put (put, index, open, a later part, the swap's NOMEM twice, its "
           "intent's IO, a drop: STOPPED) hands each object to the gc once; a gc round leaves exactly the listed "
           "objects; the key settles again; the reopen sweeps nothing\n");
}

/* The power-cut window of a refused swap (gcearly): a drop renames key 0's sources while the swap waits (its
 * directory sync held back by the hook), the swap is refused, and the power fails in swap_discard before its sync,
 * after the gc worked its queue. The drop's intent was synced before its first rename (drops are atomic), so the open
 * rolls the swap forward (the intent finds every temporary and source again: the .ec parts' chunks must still be
 * there) and then the drop, which retires the .ec parts in the sources' place: key 0 is gone, whole. */
static _Atomic int gcp_phase; /* 1: the drop is at its first rename (its intent synced), 2: the swap holds the lock */
static void gcp_drop_rename_hook(eng *db, const char *name, size_t at)
{
    if (at != 0)
        return;
    atomic_store(&gcp_phase, 1);
    for (int i = 0; i < 2000 && atomic_load(&gcp_phase) != 2; i++)
        sleep_ms(5);
    if (atomic_load(&gcp_phase) != 2)
        _exit(5);
}
static void *gcp_drop(void *arg)
{
    eng_drop_before(arg, DAY);
    return NULL;
}
static void gcp_swap_hook(eng *db)
{
    drop_test_hook = gcp_drop_rename_hook;
    pthread_t t;
    if (pthread_create(&t, NULL, gcp_drop, db) != 0)
        _exit(5);
    pthread_detach(t); /* the child ends in exit(99) with no join; syncfail lets the drop finish first (TSan: a leak) */
    /* the lock only once the drop's intent is synced (that sync takes it too): its renames then stay unsynced */
    for (int i = 0; i < 2000 && atomic_load(&gcp_phase) != 1; i++)
        sleep_ms(5);
    if (atomic_load(&gcp_phase) != 1)
        _exit(5);
    pthread_mutex_lock(&db->dir_sync_lock); /* the drop's sync after its renames waits behind it */
    atomic_store(&gcp_phase, 2);
    char dir[PATH_MAX];
    snprintf(dir, sizeof dir, "%s", db->dir);
    for (int i = 0; i < 1000 && count_names(dir, "retired_", "") == 0; i++)
        sleep_ms(5);
    if (count_names(dir, "retired_", "") == 0)
        _exit(5);
    sleep_ms(50); /* the drop's retire section ends; the swap's check waits for it anyway */
}
static void gcp_discard_hook(eng *db)
{
    ecgc_tier_work(db->tier); /* the worker's round, had it run now */
    exit(99);                 /* the power cut (crashfs at exit) */
}
/* intentio: another thread's directory sync lands while the failed intent is still named (REVIEW F2) */
static void gcp_intent_sync_hook(eng *db)
{
    if (eng_sync_dir(db, "test: another thread's directory sync") != 0)
        _exit(5);
}
/* syncfail: the engine fails between the unlinks and the discard's sync, so that sync fails (REVIEW F3); the drop's
 * sync, let go now, fails too: its renames stay unsynced */
static void gcp_fail_hook(eng *db)
{
    eng_fail(db, "test: the directory fsync of a refused swap's cleanup", EIO);
    pthread_mutex_unlock(&db->dir_sync_lock);
}
/* the gcpower variants: stopped (a drop inside the swap: the cut before the discard's sync), intentio (a failed intent
 * write whose name another sync made durable: the cut after the handoff), syncfail (the stopped swap's discard sync
 * fails: the cut after the gc round). stopped1 and syncfail1 merge key 0 to one part first: a drop of one part writes
 * no intent, so its rename stays unsynced and the swap's intent can roll forward with the source back; with two parts
 * the drop's intent is durable before its first rename and the open finishes the drop. */
static int child_gcpower(const where *w)
{
    char v[32];
    snprintf(v, sizeof v, "%s", getenv("TSDB_TEST_GCPOWER") ? getenv("TSDB_TEST_GCPOWER") : "");
    tier_env(w, QUIET_GRACE, 86400, 0);
    atomic_store(&ecgc_test_manual, true);
    eopts e = {.merge_threads = 0, .classes = 0};
    tdb d;
    if (!v[0] || t_open(w, &e, &d) != 0)
        return 3;
    size_t l = strlen(v), one[2];
    if (v[l - 1] == '1') {
        v[l - 1] = '\0';
        const int64_t k0 = 0;
        if (eng_optimize_keys(d.db, &k0, 1) != 0)
            return 4;
        key_count(d.db, 0, &one[0], &one[1]);
        if (one[0] != 1 || one[1] != 0)
            return 4;
    }
    if (!strcmp(v, "stopped")) {
        ec_swap_hook = gcp_swap_hook;
        ecswap_discard_hook = gcp_discard_hook;
    } else if (!strcmp(v, "intentio")) {
        atomic_store(&ecswap_test_fail, ECSWAP_FAIL_INTENT_IO);
        ecswap_intent_fail_hook = gcp_intent_sync_hook;
        ecswap_handed_hook = gcp_discard_hook;
    } else if (!strcmp(v, "syncfail")) {
        ec_swap_hook = gcp_swap_hook;
        ecswap_discard_hook = gcp_fail_hook;
        ecswap_handed_hook = gcp_discard_hook;
    } else
        return 3;
    int rc = settle_key(d.db, 0, NULL);
    if (!strcmp(v, "syncfail") && rc != 0) /* nothing handed over: the gc round finds nothing, then the cut */
        gcp_discard_hook(d.db);
    return 6; /* the hooks never returned here */
}

static void gcpower_case(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  gcpower: skipped (no libcrashfs.so)\n");
        return;
    }
    if (!BASE.root[0])
        make_base();
    const char *variants[] = {"stopped", "intentio", "syncfail", "stopped1", "syncfail1"};
    int seeds = quick ? 2 : 6, fwd = 0;
    for (int vi = 0; vi < 5; vi++)
    for (int seed = 1; seed <= seeds; seed++) {
        const char *v = variants[vi];
        bool drop = vi != 1, whole = vi == 0 || vi == 2; /* whole: the drop of both parts, through its intent */
        where w;
        copy_base("gcpower", &w);
        setenv("TSDB_TEST_SETTLE_CHILD", "gcpower", 1);
        setenv("TSDB_TEST_GCPOWER", v, 1);
        no_tear = seed == 1; /* every unsynced name change lost: the intent finds all it names and rolls forward */
        int code = run_child(&w, NULL, 1000000000, seed);
        no_tear = false;
        unsetenv("TSDB_TEST_SETTLE_CHILD");
        unsetenv("TSDB_TEST_GCPOWER");
        if (code != 99)
            fprintf(stderr, "FAIL gcpower %s seed %d: the child said %d\n", v, seed, code);
        CHECK(code == 99);
        /* the drop was never acknowledged: key 0 is whole or gone; listed .ec parts are whole objects */
        tier_env(&w, QUIET_GRACE, 86400, 0);
        atomic_store(&ecgc_test_manual, true);
        eopts e = {.merge_threads = 0, .classes = 0};
        tdb d;
        int rc = t_open(&w, &e, &d);
        CHECK(rc == 0);
        if (rc == 0) {
            ecgc_tier_work(d.t); /* the sweep's orphans (a swap rolled back) */
            ecgc_tier_work(d.t);
            acc a = digest(d.db, "s");
            /* whole: the drop's intent was durable before its first rename, so whatever of its renames the cut kept
             * (CRASHFS_TEAR) the open finishes it, never half (the swap rolled forward or back first); one part: its
             * rename survived the cut or did not */
            bool ok = whole ? a.n == 0 : same(a, BWANT) || (drop && a.n == 0);
            if (!ok)
                fprintf(stderr, "FAIL gcpower %s seed %d: %zu rows, not %s\n", v, seed, a.n,
                        whole ? "none (the drop finished)" : drop ? "the base's or none" : "the base's");
            CHECK(ok);
            size_t all, ec;
            key_count(d.db, 0, &all, &ec);
            fwd += all && ec == all;
            if (whole) { /* key 0 retired whole, the .ec parts of a swap rolled forward included */
                CHECK(all == 0);
                /* a rolled-forward swap's .ec parts, retired by the drop's roll-forward, are the gc's: once their
                 * time is up (here: purged regardless) their chunks and then their names go */
                graveyard_purge(d.db, true);
                ecgc_tier_work(d.t);
                graveyard_purge(d.db, true);
                CHECK(count_names(w.db, "retired_", ".ec") == 0);
            }
            if (seed == 1 && !whole && drop) /* nothing of the drop or the cleanup survived: the intent rolled forward */
                CHECK(all == 1 && ec == 1 && same(a, BWANT));
            if (seed == 1 && !drop) /* the discard's sync took the intent and the temporaries away for good */
                CHECK(all == 2 && ec == 0);
            stores_exact(&w, d.db, v);
            CHECK(count_names(w.db, "tmp_", "") == 0 && count_names(w.db, "txn_", ".intent") == 0);
        }
        t_close(&d);
        atomic_store(&ecgc_test_manual, false);
    }
    printf("  gcpower: a power cut in a refused swap's cleanup (a drop inside it, a failed intent write whose name "
           "another sync made durable, a failed discard sync) rolled %d of %d forward onto whole objects\n",
           fwd, 5 * seeds);
}

/* ------------------------------------------------------------ coldops: every consumer through its volume's ops */

/* A recording wrapper over the directory ops: what each volume's settle, cache, gc and sweep asked of its own ops, and
 * at which epoch. One rec per volume (its make_arg), shared by every context of the volume's attaches. */
typedef struct {
    uint64_t want; /* the epoch every put and delete must carry: the provider's */
    atomic_int puts, dels, opens, lists, seen, wrong;
    atomic_bool unwritable; /* writable() says false (a position away, as r8f's ops would) */
} rec;
typedef struct {
    void *in;
    rec *r;
} recctx;
static cold_ops REC; /* coldops_dir with the calls below in front; static: the ops outlive the tier */

static int r_put(void *ctx, const uint8_t oid[16], const chunk_geom *g, uint64_t epoch, const chunk_put_src *src,
                 uint64_t *sums)
{
    recctx *c = ctx;
    atomic_fetch_add(&c->r->puts, 1);
    if (epoch != c->r->want)
        atomic_fetch_add(&c->r->wrong, 1);
    return coldops_dir.put_stream(c->in, oid, g, epoch, src, sums);
}
static int r_open(void *ctx, const uint8_t oid[16], const chunk_geom *ex, const uint64_t *sums, cold_obj **out)
{
    recctx *c = ctx;
    atomic_fetch_add(&c->r->opens, 1);
    return coldops_dir.obj_open(c->in, oid, ex, sums, out);
}
static int r_del(void *ctx, const uint8_t oid[16], unsigned pos, uint64_t epoch, int reason)
{
    recctx *c = ctx;
    atomic_fetch_add(&c->r->dels, 1);
    if (epoch != c->r->want)
        atomic_fetch_add(&c->r->wrong, 1);
    return coldops_dir.del(c->in, oid, pos, epoch, reason);
}
static int r_list(void *ctx, chunk_list_fn fn, void *fctx)
{
    recctx *c = ctx;
    atomic_fetch_add(&c->r->lists, 1);
    return coldops_dir.list_names(c->in, fn, fctx);
}
static int r_owner(void *ctx, const chunk_listed *l, uint8_t vuuid[16])
{
    return coldops_dir.owner(((recctx *)ctx)->in, l, vuuid);
}
static int r_see(void *ctx, uint64_t e)
{
    recctx *c = ctx;
    atomic_fetch_add(&c->r->seen, 1);
    if (e != c->r->want)
        atomic_fetch_add(&c->r->wrong, 1);
    return coldops_dir.see_epoch(c->in, e);
}
static uint64_t r_epoch(void *ctx) { return coldops_dir.epoch(((recctx *)ctx)->in); }
static bool r_writable(void *ctx)
{
    recctx *c = ctx;
    return !atomic_load(&c->r->unwritable) && coldops_dir.writable(c->in);
}
static bool r_all_up(void *ctx) { return coldops_dir.all_up(((recctx *)ctx)->in); }
static void r_free(void *ctx)
{
    recctx *c = ctx;
    coldops_dir.ctx_free(c->in);
    free(c);
}
static int r_make(void *arg, tier *t, uint32_t volume, const uint8_t vuuid[16], void **ctx)
{
    recctx *c = calloc(1, sizeof *c);
    if (!c)
        return TSDB_ERR_NOMEM;
    c->r = arg;
    int rc = coldops_dir_make(NULL, t, volume, vuuid, &c->in); /* the tier's stores, as on a single server */
    if (rc)
        free(c);
    else
        *ctx = c;
    return rc;
}

static _Atomic uint64_t PROV[2]; /* the provider's epoch of volumes 0 and 1 (0: not known now) */
static uint64_t prov(void *arg, uint32_t volume)
{
    (void)arg;
    return volume < 2 ? atomic_load(&PROV[volume]) : 0;
}

static int rec_open(tier *t, const char *dir, uint32_t vol, rec *r, const tier_settings *set, eng **db)
{
    tsdb_options o;
    eopts e = {.merge_threads = 0, .classes = 0};
    opts_of(&e, &o);
    o.obsolete_part_lifetime_s = 0;
    tier_attach a = {.ops = &REC, .make = r_make, .make_arg = r, .epoch = prov, .settings = set};
    *db = NULL;
    tier_want_attach(t, vol, &a);
    int rc = eng_open(dir, &o, db);
    tier_want(NULL, 0);
    if (rc)
        *db = NULL;
    return rc;
}

/* chunk files of volume vol in w's stores */
static int vol_chunks(const where *w, uint32_t vol)
{
    int n = 0;
    for (int i = 0; i < 3; i++) {
        char p[PATH_MAX + 32];
        snprintf(p, sizeof p, "%s/s%u", w->st[i], vol);
        DIR *dd = opendir(p);
        struct dirent *de;
        while (dd && (de = readdir(dd)) != NULL)
            n += de->d_name[0] != '.' && strcmp(de->d_name, "EPOCH") && strncmp(de->d_name, "tmp_", 4);
        if (dd)
            closedir(dd);
    }
    return n;
}

typedef struct {
    const uint8_t *b;
} mem_src;
static int mem_fill(void *ctx, uint64_t s, uint64_t off, uint8_t *buf, size_t len)
{
    (void)s;
    memcpy(buf, ((mem_src *)ctx)->b + off, len);
    return 0;
}

static void gc_round(eng *db)
{
    graveyard_purge(db, false);
    ecgc_tier_work(db->tier);
    ecgc_tier_work(db->tier);
    graveyard_purge(db, false);
}

/* The .ec parts of key: whether each record carries epoch `want`, their oids forgotten by the cache */
static size_t ec_parts_at(eng *db, int64_t key, uint64_t want)
{
    part *p[16];
    size_t n = key_parts(db, key, p, 16), ok = 0;
    for (size_t i = 0; i < n; i++) {
        struct ecpart *ep = atomic_load(&p[i]->ec);
        if (ep && ep->rec.writer_epoch == want) {
            ok++;
            eccache_forget(db->tier, db->tier_vol, ep->rec.oid);
        }
    }
    unref_all(p, n);
    return ok;
}

/* Two volumes of one tier attached with recording ops over the tier's stores and a provider that says 2 for volume 0
 * and 3 for volume 1: a settle (by call and by the thread), a cold read, a retire and its gc delete, and a reopen's
 * sweep of an orphan on volume 1 each go through volume 1's ops at epoch 3, none through volume 0's; with the provider
 * at 0 a settle and a delete wait, and so they do while the ops say not writable. An attach that fails (provider 0)
 * leaves the volume's registry entry as it was. */
static void coldops_case(void)
{
    where w;
    fresh("coldops", &w);
    tier_env(&w, QUIET_GRACE, 86400, 0);
    atomic_store(&NOW, 1000);
    atomic_store(&ecgc_test_manual, true);
    REC = coldops_dir;
    REC.name = "rec", REC.put_stream = r_put, REC.obj_open = r_open, REC.del = r_del, REC.list_names = r_list;
    REC.owner = r_owner, REC.see_epoch = r_see, REC.epoch = r_epoch, REC.writable = r_writable;
    REC.all_up = r_all_up, REC.ctx_free = r_free;
    static rec r0, r1;
    memset(&r0, 0, sizeof r0);
    memset(&r1, 0, sizeof r1);
    r0.want = 2, r1.want = 3;
    atomic_store(&PROV[0], 2);
    atomic_store(&PROV[1], 3);
    tier_settings set1 = {.k = 2, .m = 1, .grace_s = 3600, .keep_s = 86400, .coarse_s = 86400};
    char v1[PATH_MAX + 16];
    snprintf(v1, sizeof v1, "%s/v1", w.root);
    eopts e = {.merge_threads = 0, .classes = 0};
    tsdb_options o;
    opts_of(&e, &o);
    tier *t = NULL;
    eng *d0 = NULL, *d1 = NULL;
    CHECK(tier_open(w.db, &o, &t) == 0);
    if (t) {
        CHECK(rec_open(t, w.db, 0, &r0, NULL, &d0) == 0);
        CHECK(rec_open(t, v1, 1, &r1, &set1, &d1) == 0);
    }
    if (!d0 || !d1)
        goto out;
    CHECK(atomic_load(&r0.seen) == 1 && atomic_load(&r1.seen) == 1);
    uint32_t s;
    CHECK(eng_series(d1, "s", TSDB_FLOAT32, &s) == 0);
    two_parts(d1, s, 0);
    write_rows(d1, s, DAY, 5000, 2000, 3);
    CHECK(eng_flush(d1) == 0);
    acc want = digest(d1, "s");

    /* the provider knows no epoch: no put (settle_noepoch0) */
    atomic_store(&PROV[1], 0);
    CHECK(settle_key(d1, 0, NULL) == TSDB_ERR_UNAVAILABLE);
    CHECK(atomic_load(&r1.puts) == 0);
    atomic_store(&PROV[1], 3);
    /* the volume's ops say not writable: no put */
    atomic_store(&r1.unwritable, true);
    CHECK(settle_key(d1, 0, NULL) == TSDB_ERR_UNAVAILABLE);
    CHECK(atomic_load(&r1.puts) == 0);
    atomic_store(&r1.unwritable, false);
    /* a settle by call: through volume 1's ops at 3 (settle_epoch1, settle_tierbe) */
    size_t got = 0, all, ec;
    CHECK(settle_key(d1, 0, &got) == 0 && got == 2);
    CHECK(atomic_load(&r1.puts) == 2 && ec_parts_at(d1, 0, 3) == 2);

    /* the thread: key 1 due (volume 1's grace_s, not the tier's), held while the ops say not writable
     * (settle_tierwritable), settled once they do */
    atomic_store(&r1.unwritable, true);
    uint64_t f0 = atomic_load(&settle_failed);
    atomic_store(&NOW, 2 * DAY + HOUR + 1);
    wait_rounds(5);
    key_count(d1, 1, &all, &ec);
    CHECK(ec == 0 && atomic_load(&r1.puts) == 2);
    CHECK(atomic_load(&settle_failed) == f0); /* the thread did not try (a failed try would back the key off) */
    atomic_store(&r1.unwritable, false);
    CHECK(wait_settled(d1, 1, 20000));
    CHECK(atomic_load(&r1.puts) == 3 && ec_parts_at(d1, 1, 3) == 1);

    /* a cold read (the cache forgot the objects above): volume 1's ops open them */
    int op0 = atomic_load(&r1.opens);
    CHECK(same(digest(d1, "s"), want));
    CHECK(atomic_load(&r1.opens) > op0);
    int c1 = vol_chunks(&w, 1);
    CHECK(c1 >= 6 && vol_chunks(&w, 0) == 0); /* three objects of k' + m >= 2 positions each */

    /* a drop retires the .ec parts; the gc deletes through volume 1's ops at 3 (gc_epoch1, gc_vol0), not while the
     * provider says 0 (gc_noepoch0) or the ops say not writable (gc_tierwritable) */
    CHECK(eng_drop_before(d1, 2 * DAY) == 0);
    atomic_store(&PROV[1], 0);
    gc_round(d1);
    CHECK(atomic_load(&r1.dels) == 0 && vol_chunks(&w, 1) == c1);
    atomic_store(&PROV[1], 3);
    atomic_store(&r1.unwritable, true);
    gc_round(d1);
    CHECK(atomic_load(&r1.dels) == 0 && vol_chunks(&w, 1) == c1);
    atomic_store(&r1.unwritable, false);
    gc_round(d1);
    CHECK(atomic_load(&r1.dels) == c1 && vol_chunks(&w, 1) == 0); /* one delete per position */

    /* an orphan of volume 1 (put through its ops, named by no part): the reopen's sweep lists volume 1's stores
     * (sweep_vol0) and deletes it through its ops */
    coldvol *cv = tier_cold(d1);
    chunk_geom g;
    uint8_t oid[16], buf[4096];
    memset(oid, 0x5a, sizeof oid);
    for (size_t i = 0; i < sizeof buf; i++)
        buf[i] = (uint8_t)(i * 31);
    CHECK(chunk_geom_make(&g, 2, 1, 512, sizeof buf) == 0);
    uint64_t sums[64];
    CHECK(chunk_sums_total(&g) <= 64);
    mem_src ms = {buf};
    chunk_put_src src = {.ctx = &ms, .fill = mem_fill};
    CHECK(cv->ops->put_stream(cv->ctx, oid, &g, coldvol_epoch(cv), &src, sums) == 0);
    CHECK(vol_chunks(&w, 1) == 3);
    CHECK(eng_close(d1) == 0);
    d1 = NULL;
    /* a failed attach (the provider knows no epoch) leaves the registry's entry as it was (F8) */
    coldvol *before = tier_cold_get(t, 1);
    atomic_store(&PROV[1], 0);
    CHECK(rec_open(t, v1, 1, &r1, &set1, &d1) == TSDB_ERR_UNAVAILABLE && !d1);
    coldvol *after = tier_cold_get(t, 1);
    CHECK(before && after == before);
    tier_cold_put(after);
    tier_cold_put(before);
    atomic_store(&PROV[1], 3);
    int l0 = atomic_load(&r0.lists), l1 = atomic_load(&r1.lists), dl = atomic_load(&r1.dels);
    CHECK(rec_open(t, v1, 1, &r1, &set1, &d1) == 0);
    if (d1) {
        ecgc_tier_work(t); /* the sweep's listing, if the open deferred it, and the queue */
        ecgc_tier_work(t);
        CHECK(atomic_load(&r1.lists) > l1 && atomic_load(&r0.lists) == l0);
        CHECK(atomic_load(&r1.dels) >= dl + 3 && vol_chunks(&w, 1) == 0);
    }
    /* volume 0's ops were asked nothing of volume 1's objects; every put and delete carried its provider's epoch */
    CHECK(atomic_load(&r0.puts) == 0 && atomic_load(&r0.dels) == 0 && atomic_load(&r0.opens) == 0);
    CHECK(atomic_load(&r0.wrong) == 0 && atomic_load(&r1.wrong) == 0);
out:
    if (d1)
        CHECK(eng_close(d1) == 0);
    if (d0)
        CHECK(eng_close(d0) == 0);
    if (t)
        tier_put(t);
    atomic_store(&ecgc_test_manual, false);
    printf("  coldops: settle (call, thread), cold read, gc delete, sweep of volume 1 through its own ops at its "
           "provider's epoch; provider 0 or not writable: nothing put or deleted; a failed attach keeps the entry\n");
}

int main(int argc, char **argv)
{
    const char *scratch = getenv("TMPDIR") ? getenv("TMPDIR") : "/tmp", *child = NULL;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--quick")) {
            quick = true;
        } else if (!strcmp(argv[i], "--mutant") && i + 1 < argc) {
            mutant = argv[++i];
        } else if (!strcmp(argv[i], "--case") && i + 1 < argc) {
            only = argv[++i];
        } else if (!strcmp(argv[i], "--child") && i + 1 < argc) {
            child = argv[++i];
        } else {
            scratch = argv[i];
        }
    }
    int m = settle_mutant_parse(mutant);
    if (m < 0) {
        fprintf(stderr, "test_settle: unknown mutant %s\n", mutant);
        return 2;
    }
    settle_mutant = m;
    atomic_store(&settle_poll_ms, 20);
    if (child)
        return child_settle(child);
    if (!realpath(argv[0], SELF))
        snprintf(SELF, sizeof SELF, "%s", argv[0]);
    char *slash = strrchr(SELF, '/');
    snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", slash ? (int)(slash - SELF) : 1, slash ? SELF : ".");
    snprintf(S, sizeof S, "%s", scratch);
    printf("test_settle%s%s\n", m ? " --mutant " : "", m ? mutant : "");
#define RUN(name) (!only || !strcmp(only, #name) ? name##_case() : (void)0)
    RUN(basic);
    RUN(corrupt);
    RUN(flip);
    RUN(hash);
    RUN(checks);
    RUN(class);
    RUN(down);
    RUN(policy);
    RUN(coarse);
    RUN(backoff);
    RUN(eager);
    RUN(close);
    RUN(halt);
    RUN(threads);
    RUN(crash);
    RUN(gcpaths);
    RUN(gcpower);
    RUN(coldops);
#undef RUN
    if (failures) {
        fprintf(stderr, "test_settle: %d failures\n", failures);
        return 1;
    }
    char cmd[PATH_MAX + 64];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'/settle-%d-*", S, (int)getpid());
    if (system(cmd) != 0)
        fprintf(stderr, "test_settle: could not remove the scratch directories\n");
    printf("test_settle: ok\n");
    return 0;
}
#endif
