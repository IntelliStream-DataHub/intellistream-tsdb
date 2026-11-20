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
 *   threads   8 writers (appends, large inserts, deletes, replaces), merges, a drop and settles at once
 *   crash     TSDB_SETTLE_CRASH_AT after-put and after-ec-tmp, then power loss (crashfs) at every step
 *
 * Mutants (--mutant NAME, src/settle.h; each must fail it): ecrankdown (basic), settlenocheck (corrupt), settletwopass
 * (flip), settlebusy (policy), coarsewhole (coarse). Usage: test_settle [--quick] [--mutant NAME] [SCRATCH]
 */
#include "internal.h"
#include "ecpart.h"
#include "ecskip.h"
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
    o->merge_idle_s = 1;
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
        size_t got = 0;
        CHECK(eng_settle(d.db, 0, &b, 1, &got) == 0 && got == 1);
        CHECK(same(digest(d.db, "s"), want));
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

    /* no merges: two level-0 parts keep a merge selectable, so the key never settles (settlebusy does) */
    fresh("busy", &w);
    tier_env(&w, 3600, 86400, 0);
    e.merge_threads = 0;
    CHECK(t_open(&w, &e, &d) == 0);
    if (!d.db)
        return;
    CHECK(eng_series(d.db, "s", TSDB_FLOAT32, &s) == 0);
    write_rows(d.db, s, 0, 5000, 2000, 0);
    CHECK(eng_flush(d.db) == 0);
    write_rows(d.db, s, 1000, 5000, 2000, 1);
    CHECK(eng_flush(d.db) == 0);
    atomic_store(&NOW, 30 * DAY);
    wait_rounds(10);
    key_count(d.db, 0, &all, &ec);
    CHECK(all == 2 && ec == 0);
    t_close(&d);
    printf("  policy: a day key settles after grace_s and its merges, not before, never with a merge selectable\n");
}

static void coarse_case(void)
{
    where w;
    fresh("coarse", &w);
    tier_env(&w, 3600, 7 * 86400, 0);
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
    write_rows(d.db, s, 3 * DAY, 2000, 10000, 1); /* day 3, the same week: part 2 */
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
    atomic_store(&NOW, DAY + HOUR); /* part 1 is due, part 2 is not */
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
    atomic_store(&NOW, 5 * DAY); /* part 2 due now, but the last settle is less than coarse_s old */
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
    atomic_store(&NOW, DAY + HOUR + 7 * DAY + 1000);
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

static where BASE;
static acc BWANT;

static void make_base(void)
{
    fresh("base", &BASE);
    tier_env(&BASE, 3600, 86400, 0);
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

static int child_settle(const char *root)
{
    where w;
    snprintf(w.root, sizeof w.root, "%s", root);
    snprintf(w.db, sizeof w.db, "%s/db", root);
    for (int i = 0; i < 3; i++)
        snprintf(w.st[i], sizeof w.st[i], "%s/st%d", root, i);
    tier_env(&w, 3600, 86400, 0);
    eopts e = {.merge_threads = 0, .classes = 0};
    tdb d;
    if (t_open(&w, &e, &d) != 0)
        return 3;
    int rc = settle_key(d.db, 0, NULL);
    int crc = t_close(&d);
    return rc == 0 && crc == 0 ? 0 : 4;
}

static const char *exit_mark; /* CRASHFS_EXIT_MARK of the next child under the shim, NULL: none */
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
static int reopen_check(const where *w, const char *what)
{
    tier_env(w, 3600, 86400, 0);
    eopts e = {.merge_threads = 0, .classes = 0};
    tdb d;
    int rc = t_open(w, &e, &d);
    CHECK(rc == 0);
    if (rc) {
        fprintf(stderr, "  (%s: the reopen said %d)\n", what, rc);
        return -1;
    }
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

int main(int argc, char **argv)
{
    const char *scratch = getenv("TMPDIR") ? getenv("TMPDIR") : "/tmp", *child = NULL;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--quick")) {
            quick = true;
        } else if (!strcmp(argv[i], "--mutant") && i + 1 < argc) {
            mutant = argv[++i];
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
    basic_case();
    corrupt_case();
    flip_case();
    down_case();
    policy_case();
    coarse_case();
    backoff_case();
    eager_case();
    close_case();
    threads_case();
    crash_case();
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
