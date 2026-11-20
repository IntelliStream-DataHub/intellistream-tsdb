/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R7's interfaces (r7-iface): the names of .ec parts, their temporaries, retired ones and settled sources, each
 * accepted by its own predicate only, with every near miss refused; TSDB_TEST_TIER's grammar on good and bad strings
 * and the refusal without TSDB_TEST_TIER_I_KNOW=1 (tsdb_open fails and makes nothing); the tier's references through
 * engine opens and closes; the stubs leave a database as today's code does (R7's names at open, a v4 intent, the
 * graveyard kinds, a volume add); eng_test_mark_ec and reads of a marked part. Each stub counts its calls
 * (ec_calls, src/ecpart.h), and every case checks that the dispatch in the shared file reached it: the stubs do what
 * the old code did, so the answers alone could not tell a missing branch. Also: the owners' open and close hooks of
 * the tier and the engine, the open's sweep, the byte totals, the CLASSES check of a marked part, a follower meeting
 * an .ec name, and tsdb upgrade refusing R7's names in a format-8 fixture (tests/data/v8-fixture.tar, from the
 * source tree).
 *
 * Usage: test_ecnames [--mutant nameloose|tierloose] [SCRATCH]. Without TSDB_EC (make NO_EC=1, the bindings) the
 * cases that need a tier say they are skipped; a tier configured there must be refused.
 */
#include "internal.h"
#include "ecgc.h"
#include "ecpart.h"
#include "tier.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
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

static char scratch[512];

/* the stubs' call counters (src/ecpart.h): a dispatch in a shared file must reach its stub */
static uint64_t calls(int k) { return (uint64_t)atomic_load(&ec_calls[k]); }
#define CALLS_SINCE(k, before) (calls(k) - (before)[k])
static void calls_now(uint64_t *out)
{
    for (int k = 0; k < EC_CALL_N; k++)
        out[k] = calls(k);
}

static void rm_rf(const char *path)
{
    DIR *d = opendir(path);
    if (d) {
        struct dirent *e;
        while ((e = readdir(d))) {
            if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, ".."))
                continue;
            char p[1024];
            snprintf(p, sizeof p, "%s/%s", path, e->d_name);
            struct stat st;
            if (lstat(p, &st) == 0 && S_ISDIR(st.st_mode))
                rm_rf(p);
            else
                unlink(p);
        }
        closedir(d);
    }
    rmdir(path);
}

static void fresh(char *out, size_t len, const char *what)
{
    snprintf(out, len, "%s/test_ecnames_%d_%s", scratch, (int)getpid(), what);
    rm_rf(out);
}

static bool exists(const char *dir, const char *name)
{
    char p[1024];
    snprintf(p, sizeof p, "%s/%s", dir, name);
    return access(p, F_OK) == 0;
}

static void touch(const char *dir, const char *name)
{
    char p[1024];
    snprintf(p, sizeof p, "%s/%s", dir, name);
    int fd = open(p, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    CHECK(fd >= 0);
    if (fd >= 0) {
        CHECK(write(fd, "not a part\n", 11) == 11);
        close(fd);
    }
}

/* ------------------------------------------------------------ names */

enum { N_PART = 1, N_PTMP = 2, N_EC = 4, N_ECTMP = 8, N_ECRET = 16, N_SETTLED = 32, N_INTENT = 64 };

static void names(void)
{
    static const struct {
        const char *name;
        unsigned want;
    } t[] = {
        {"p1_2_3_4.part", N_PART},
        {"p-1_2_3_4.part", N_PART},
        {"p1_2_3_4.ec", N_EC},
        {"p-1_2_3_4.ec", N_EC},
        {"p0_0_0_0.ec", N_EC},
        {"p12345678901234567890_1_1_0.ec", N_EC},       /* 20 digits */
        {"p123456789012345678901_1_1_0.ec", 0},         /* 21 */
        {"p1_2_3_4.ec2", 0},
        {"p1_2_3_4.e", 0},
        {"p1_2_3_4.ecc", 0},
        {"p1_2_3_4.ec.tmp", 0},
        {"p1_2_3_4.ec ", 0},
        {"p1_2_3_4ec", 0},
        {"p1_2_3_4.EC", 0},
        {"P1_2_3_4.ec", 0},
        {"p1_2_3.ec", 0},
        {"p1_2_3_4_5.ec", 0},
        {"pa_2_3_4.ec", 0},
        {"p1__3_4.ec", 0},
        {"p--1_2_3_4.ec", 0},
        {"p-_2_3_4.ec", 0},
        {"p+1_2_3_4.ec", 0},
        {"p1_2_3_4.", 0},
        {".ec", 0},
        {"p.ec", 0},
        {"xp1_2_3_4.ec", 0},
        {"tmp_1_2.ec", N_ECTMP},
        {"tmp_12345_678.ec", N_ECTMP},
        {"tmp_1_2.part", N_PTMP},
        {"tmp_1.ec", 0},
        {"tmp_1_2_3.ec", 0},
        {"tmp__2.ec", 0},
        {"tmp_1_.ec", 0},
        {"tmp_-1_2.ec", 0},
        {"tmp_1_2.ec.x", 0},
        {"tmp_1_2.e", 0},
        {"tmp_1_2ec", 0},
        {"tmp_a_2.ec", 0},
        {"Tmp_1_2.ec", 0},
        {"retired_p1_2_3_4.ec", N_ECRET},
        {"retired_p-1_2_3_4.ec", N_ECRET},
        {"retired_p1_2_3_4.part", 0}, /* a retired part: load_parts' own branch, not one of R7's predicates */
        {"retired_tmp_1_2.ec", 0},
        {"retired_p1_2_3.ec", 0},
        {"retired_p1_2_3_4.ec.x", 0},
        {"retiredp1_2_3_4.ec", 0},
        {"retired__p1_2_3_4.ec", 0},
        {"retired_", 0},
        {"retired_settled_p1_2_3_4.part", 0},
        {"settled_p1_2_3_4.part", N_SETTLED},
        {"settled_p-1_2_3_4.part", N_SETTLED},
        {"settled_p1_2_3_4.ec", 0},
        {"settled_tmp_1_2.part", 0},
        {"settled_p1_2_3.part", 0},
        {"settled_p1_2_3_4.part.x", 0},
        {"settledp1_2_3_4.part", 0},
        {"settled_", 0},
        {"Settled_p1_2_3_4.part", 0},
        {"settled_retired_p1_2_3_4.part", 0},
        {"txn_1_2.intent", N_INTENT},
        {"txn_1_2.ec", 0},
        {"", 0},
    };
    for (size_t i = 0; i < sizeof t / sizeof *t; i++) {
        const char *n = t[i].name;
        unsigned got = (part_file_name(n) ? N_PART : 0) | (part_tmp_name(n) ? N_PTMP : 0) |
                       (ec_file_name(n) ? N_EC : 0) | (ec_tmp_name(n) ? N_ECTMP : 0) |
                       (ec_retired_name(n) ? N_ECRET : 0) | (settled_part_name(n) ? N_SETTLED : 0) |
                       (intent_file_name(n) ? N_INTENT : 0);
        if (got != t[i].want) {
            fprintf(stderr, "FAIL names: '%s' matched %#x, want %#x\n", n, got, t[i].want);
            failures++;
        }
    }
}

/* ------------------------------------------------------------ TSDB_TEST_TIER */

static void cfg_good(void)
{
    tier_cfg c;
    char err[256];
    CHECK(tier_cfg_parse("k=2,m=1,dirs=/a:/b:/c", &c, err, sizeof err) == 0);
    CHECK(c.set && c.k == 2 && c.m == 1 && c.ndirs == 3);
    CHECK(c.ndirs == 3 && !strcmp(c.dirs[0], "/a") && !strcmp(c.dirs[1], "/b") && !strcmp(c.dirs[2], "/c"));
    CHECK(c.grace_s == TIER_GRACE_S_DEFAULT && c.keep_s == TIER_KEEP_S_DEFAULT && c.coarse_s == TIER_COARSE_S_DEFAULT);
    CHECK(c.cache_mb == 0 && !c.eager);
    tier_cfg_free(&c);
    CHECK(c.dirs == NULL && c.ndirs == 0 && !c.set);

    CHECK(tier_cfg_parse("eager=1,cache_mb=64,coarse_s=7,keep_s=5,grace_s=0,dirs=/x/y:/z:/w/v:/u,m=2,k=2", &c, err,
                         sizeof err) == 0);
    CHECK(c.k == 2 && c.m == 2 && c.ndirs == 4 && c.grace_s == 0 && c.keep_s == 5 && c.coarse_s == 7 &&
          c.cache_mb == 64 && c.eager);
    CHECK(c.ndirs == 4 && !strcmp(c.dirs[0], "/x/y") && !strcmp(c.dirs[3], "/u"));
    tier_cfg_free(&c);

    CHECK(tier_cfg_parse("k=16,m=4,dirs=/1:/2:/3:/4:/5:/6:/7:/8:/9:/10:/11:/12:/13:/14:/15:/16:/17:/18:/19:/20,"
                         "grace_s=315360000,eager=0",
                         &c, err, sizeof err) == 0);
    CHECK(c.k == 16 && c.m == 4 && c.ndirs == 20 && c.grace_s == TIER_SECONDS_MAX && !c.eager);
    tier_cfg_free(&c);

    CHECK(tier_cfg_parse("k=1,m=1,dirs=/a:/b:/c", &c, err, sizeof err) == 0); /* more stores than k + m */
    CHECK(c.ndirs == 3);
    tier_cfg_free(&c);
}

static void cfg_bad(void)
{
    static const char *const bad[] = {
        "",
        ",",
        "k=2",
        "m=1,dirs=/a:/b",
        "k=2,dirs=/a:/b:/c",
        "k=2,m=1",
        "k=2,m=1,dirs=/a:/b",            /* fewer than k + m */
        "k=0,m=1,dirs=/a:/b",
        "k=17,m=1,dirs=/a",
        "k=2,m=0,dirs=/a:/b",
        "k=2,m=5,dirs=/a:/b:/c:/d:/e:/f:/g",
        "k=-1,m=1,dirs=/a:/b",
        "k=+2,m=1,dirs=/a:/b:/c",
        "k= 2,m=1,dirs=/a:/b:/c",
        "k=2 ,m=1,dirs=/a:/b:/c",
        "k=0x2,m=1,dirs=/a:/b:/c",
        "k=2,m=1,dirs=/a:/b:/c,",
        ",k=2,m=1,dirs=/a:/b:/c",
        "k=2,,m=1,dirs=/a:/b:/c",
        "k=2,m=1,dirs=/a:/b:/c,k=2",
        "k=2,m=1,dirs=/a:/b:/c,dirs=/d:/e:/f",
        "k=2,m=1,dirs=",
        "k=,m=1,dirs=/a:/b:/c",
        "k=2,m=1,dirs=a:/b:/c",
        "k=2,m=1,dirs=/a:b:/c",
        "k=2,m=1,dirs=/a::/b:/c",
        "k=2,m=1,dirs=/a:/b:/c:",
        "k=2,m=1,dirs=:/a:/b:/c",
        "k=2,m=1,dirs=/a:/b:/a",
        "k=2,m=1,dirs=/a:/b:/c,grace_s=-1",
        "k=2,m=1,dirs=/a:/b:/c,grace_s=315360001",
        "k=2,m=1,dirs=/a:/b:/c,keep_s=99999999999999999999999",
        "k=2,m=1,dirs=/a:/b:/c,coarse_s=1.5",
        "k=2,m=1,dirs=/a:/b:/c,cache_mb=1073741825",
        "k=2,m=1,dirs=/a:/b:/c,eager=2",
        "k=2,m=1,dirs=/a:/b:/c,eager=yes",
        "k=2,m=1,dirs=/a:/b:/c,eager",
        "k=2,m=1,dirs=/a:/b:/c,turbo=1",
        "K=2,m=1,dirs=/a:/b:/c",
        "k=2,m=1,dirs=/a:/b:/c,=1",
        "k=2;m=1;dirs=/a:/b:/c",
        "k==2,m=1,dirs=/a:/b:/c",
    };
    for (size_t i = 0; i < sizeof bad / sizeof *bad; i++) {
        tier_cfg c;
        char err[256] = "x";
        int rc = tier_cfg_parse(bad[i], &c, err, sizeof err);
        if (rc != TSDB_ERR_INVALID || !err[0] || c.set || c.dirs) {
            fprintf(stderr, "FAIL tier_cfg_parse('%s') = %d (%s), want a refusal\n", bad[i], rc, err);
            failures++;
        }
        tier_cfg_free(&c);
    }
    /* more stores than TIER_DIRS_MAX */
    char big[TIER_DIRS_MAX * 8 + 64];
    size_t n = (size_t)snprintf(big, sizeof big, "k=1,m=1,dirs=");
    for (int i = 0; i <= TIER_DIRS_MAX; i++)
        n += (size_t)snprintf(big + n, sizeof big - n, "%s/d%d", i ? ":" : "", i);
    tier_cfg c;
    CHECK(tier_cfg_parse(big, &c, NULL, 0) == TSDB_ERR_INVALID && !c.set);
    tier_cfg_free(&c);
}

static void from_strings(void)
{
    tier_cfg c;
    const char *good = "k=2,m=1,dirs=/a:/b:/c";
    CHECK(tier_from_strings(NULL, NULL, &c) == 0 && !c.set);
    CHECK(tier_from_strings("", "1", &c) == 0 && !c.set);
    CHECK(tier_from_strings(NULL, "1", &c) == 0 && !c.set);
    CHECK(tier_from_strings(good, NULL, &c) == TSDB_ERR_INVALID && !c.set);
    CHECK(tier_from_strings(good, "", &c) == TSDB_ERR_INVALID && !c.set);
    CHECK(tier_from_strings(good, "0", &c) == TSDB_ERR_INVALID && !c.set);
    CHECK(tier_from_strings(good, "yes", &c) == TSDB_ERR_INVALID && !c.set);
    CHECK(tier_from_strings(good, "1 ", &c) == TSDB_ERR_INVALID && !c.set);
    CHECK(tier_from_strings(good, "11", &c) == TSDB_ERR_INVALID && !c.set);
    CHECK(tier_from_strings("k=2,m=1", "1", &c) == TSDB_ERR_INVALID && !c.set);
    CHECK(tier_from_strings(good, "1", &c) == 0 && c.set && c.k == 2);
    tier_cfg_free(&c);
    unsetenv("TSDB_TEST_TIER");
    unsetenv("TSDB_TEST_TIER_I_KNOW");
    CHECK(tier_from_env(&c) == 0 && !c.set);
    setenv("TSDB_TEST_TIER", good, 1);
    CHECK(tier_from_env(&c) == TSDB_ERR_INVALID && !c.set);
    setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);
    CHECK(tier_from_env(&c) == 0 && c.set);
    tier_cfg_free(&c);
    unsetenv("TSDB_TEST_TIER");
    unsetenv("TSDB_TEST_TIER_I_KNOW");
}

/* tsdb_open refuses a tier without the second variable, before any engine makes a directory or a file */
static void open_refusal(void)
{
    char dir[600], stores[600];
    fresh(dir, sizeof dir, "refuse");
    fresh(stores, sizeof stores, "stores");
    CHECK(mkdir(stores, 0755) == 0); /* a new tier makes each store directory, one level of it (chunk_store_init) */
    char spec[2048];
    snprintf(spec, sizeof spec, "k=2,m=1,dirs=%s/a:%s/b:%s/c", stores, stores, stores);
    setenv("TSDB_TEST_TIER", spec, 1);
    unsetenv("TSDB_TEST_TIER_I_KNOW");
    tsdb *t = NULL;
    CHECK(tsdb_open(dir, NULL, &t) == TSDB_ERR_INVALID && t == NULL);
    CHECK(access(dir, F_OK) != 0); /* nothing made: no directory, so no META to say requires=tier */
    setenv("TSDB_TEST_TIER", "k=2,m=1,dirs=relative:/b:/c", 1);
    setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);
    CHECK(tsdb_open(dir, NULL, &t) == TSDB_ERR_INVALID && t == NULL);
    CHECK(access(dir, F_OK) != 0);
    setenv("TSDB_TEST_TIER", spec, 1);
    uint64_t c0[EC_CALL_N];
    calls_now(c0);
    int rc = tsdb_open(dir, NULL, &t);
#ifdef TSDB_EC
    CHECK(rc == 0 && t);
    if (t) {
        tsdb_sid id;
        CHECK(tsdb_series(t, "s", &id) == 0);
        /* the tier's three owners opened; the root engine's four, and its sweep (a writer) */
        CHECK(CALLS_SINCE(EC_CALL_TIER_OPEN, c0) == 3 && CALLS_SINCE(EC_CALL_TIER_CLOSE, c0) == 0);
        CHECK(CALLS_SINCE(EC_CALL_ENG_OPEN, c0) == 4 && CALLS_SINCE(EC_CALL_GC_SWEEP, c0) == 1);
        CHECK(tsdb_close(t) == 0);
        /* tsdb_close quiesced the settle thread, the engine closed its owners, the last put closed the tier's */
        CHECK(CALLS_SINCE(EC_CALL_TIER_QUIESCE, c0) == 1 && CALLS_SINCE(EC_CALL_ENG_CLOSE, c0) == 4);
        CHECK(CALLS_SINCE(EC_CALL_TIER_CLOSE, c0) == 3);
    }
#else
    CHECK(rc == TSDB_ERR_INVALID && t == NULL); /* no erasure codec in this build: refused */
    printf("test_ecnames: open with a tier skipped: no TSDB_EC in this build (refused, as it must be)\n");
#endif
    unsetenv("TSDB_TEST_TIER");
    unsetenv("TSDB_TEST_TIER_I_KNOW");
    rm_rf(dir);
    rm_rf(stores);
}

static int open_eng(const char *dir, int lifetime_s, eng **db)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0; /* a marked part has no merge barrier before r7-skip */
    o.create_volumes = 0;
    o.obsolete_part_lifetime_s = lifetime_s;
    return eng_open(dir, &o, db);
}

/* the engine's references: one per open engine, given back at close; tier_want is taken once */
static void tier_refs_case(void)
{
#ifndef TSDB_EC
    printf("test_ecnames: tier references skipped: no TSDB_EC in this build\n");
#else
    char dir[600], dir2[600], stores[600], spec[1400];
    fresh(dir, sizeof dir, "refs");
    fresh(dir2, sizeof dir2, "refs2");
    fresh(stores, sizeof stores, "refstores");
    CHECK(mkdir(stores, 0755) == 0); /* tier_open opens the backend: the stores must be makeable */
    snprintf(spec, sizeof spec, "k=1,m=1,dirs=%s/a:%s/b", stores, stores);
    setenv("TSDB_TEST_TIER", spec, 1);
    setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);
    tier *t = NULL;
    CHECK(tier_open(dir, NULL, &t) == 0 && t);
    unsetenv("TSDB_TEST_TIER");
    unsetenv("TSDB_TEST_TIER_I_KNOW");
    if (!t)
        return;
    CHECK(tier_refs(t) == 1 && tier_config(t)->k == 1);
    uint64_t c0[EC_CALL_N];
    calls_now(c0);
    eng *a = NULL, *b = NULL;
    tier_want(t, 3);
    CHECK(open_eng(dir, 0, &a) == 0);
    CHECK(open_eng(dir2, 0, &b) == 0); /* the wish was taken by the first open */
    tier_want(NULL, 0);
    CHECK(a && a->tier == t && a->tier_vol == 3 && a->ecstate);
    CHECK(b && b->tier == NULL && b->ecstate == NULL);
    CHECK(tier_refs(t) == 2);
    /* only the engine with the tier ran its owners' opens and the sweep */
    CHECK(CALLS_SINCE(EC_CALL_ENG_OPEN, c0) == 4 && CALLS_SINCE(EC_CALL_GC_SWEEP, c0) == 1);
    eng_close(b);
    CHECK(CALLS_SINCE(EC_CALL_ENG_CLOSE, c0) == 0);
    eng_close(a);
    CHECK(tier_refs(t) == 1);
    CHECK(CALLS_SINCE(EC_CALL_ENG_CLOSE, c0) == 4);
    /* a read-only open of a tier volume is refused (requires=tier: only the writer, which holds the stores, reads .ec
     * parts) and gives the wish back unused */
    tsdb_options ro;
    tsdb_options_init(&ro);
    ro.read_only = 1;
    ro.create_volumes = 0;
    calls_now(c0);
    tier_want(t, 3);
    a = NULL;
    CHECK(eng_open(dir, &ro, &a) == TSDB_ERR_INVALID && !a);
    tier_want(NULL, 0);
    CHECK(CALLS_SINCE(EC_CALL_ENG_OPEN, c0) == 0 && CALLS_SINCE(EC_CALL_GC_SWEEP, c0) == 0);
    CHECK(tier_refs(t) == 1);
    tier_want(t, 1); /* an open that fails gives its reference back */
    eng *c = NULL;
    tsdb_options o;
    tsdb_options_init(&o);
    o.value_codec = 99;
    CHECK(eng_open(dir, &o, &c) != 0 && !c);
    touch(dir2, "p1_1_1_0.part"); /* a broken part: load_parts fails the open after the attach */
    calls_now(c0);
    CHECK(open_eng(dir2, 0, &c) == TSDB_ERR_CORRUPT && !c);
    tier_want(NULL, 0);
    CHECK(tier_refs(t) == 1);
    /* the failed open closed what it had opened, and swept nothing (it never got that far) */
    CHECK(CALLS_SINCE(EC_CALL_ENG_OPEN, c0) == 4 && CALLS_SINCE(EC_CALL_ENG_CLOSE, c0) == 4);
    CHECK(CALLS_SINCE(EC_CALL_GC_SWEEP, c0) == 0);
    calls_now(c0);
    tier_put(t);
    CHECK(CALLS_SINCE(EC_CALL_TIER_CLOSE, c0) == 3);
    rm_rf(dir);
    rm_rf(dir2);
    rm_rf(stores);
#endif
}

/* ------------------------------------------------------------ the stubs leave a database as today */

static void write_rows(eng *db, uint32_t s, int64_t t0, int n)
{
    int64_t ts[256];
    float v[256];
    for (int i = 0; i < n; i += 256) {
        int k = n - i < 256 ? n - i : 256;
        for (int j = 0; j < k; j++)
            ts[j] = t0 + (int64_t)(i + j) * 1000, v[j] = (float)((i + j) % 977);
        CHECK(eng_insert_series(db, TSDB_FLOAT32, s, ts, v, (size_t)k) == 0);
    }
}

typedef struct {
    size_t n;
    double sum;
} acc;

static int acc_fn(void *ctx, const int64_t *ts, const void *v, size_t n)
{
    (void)ts;
    acc *a = ctx;
    for (size_t i = 0; i < n; i++)
        a->sum += ((const float *)v)[i];
    a->n += n;
    return 0;
}

static acc read_all(eng *db, uint32_t s)
{
    acc a = {0, 0};
    CHECK(eng_query_raw_ex(db, s, INT64_MIN, INT64_MAX, false, 0, acc_fn, &a) == 0);
    return a;
}

static void part_info_fn(void *ctx, const tsdb_part_info *info)
{
    (void)ctx;
    (void)info;
}

static void stubs_case(void)
{
    char dir[600];
    fresh(dir, sizeof dir, "stubs");
    eng *db = NULL;
    CHECK(open_eng(dir, 3600, &db) == 0);
    if (!db)
        return;
    uint32_t s;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    int64_t day = 86400000;
    write_rows(db, s, 0, 20000);
    CHECK(eng_flush(db) == 0);
    write_rows(db, s, 2 * day, 20000);
    CHECK(eng_flush(db) == 0);
    acc before = read_all(db, s);
    CHECK(before.n == 40000);

    /* the hook: only listed parts; settling toggles; .ec once */
    pthread_mutex_lock(&db->lock);
    part *p0 = db->nparts >= 2 ? db->parts[0] : NULL, *p1 = db->nparts >= 2 ? db->parts[1] : NULL;
    pthread_mutex_unlock(&db->lock);
    CHECK(p0 && p1);
    if (!p0 || !p1) {
        eng_close(db);
        return;
    }
    part fake;
    memset(&fake, 0, sizeof fake);
    CHECK(eng_test_mark_ec(db, &fake, EC_MARK_EC) == TSDB_ERR_NOTFOUND && fake.ec == NULL);
    CHECK(eng_test_mark_ec(db, p0, EC_MARK_SETTLING) == 0 && p0->settling && !p0->ec);
    CHECK(eng_test_mark_ec(db, p0, 0) == 0 && !p0->settling);
    /* a part a merge has is refused: the merge would meet an .ec source */
    pthread_mutex_lock(&db->lock);
    p0->merging = true;
    pthread_mutex_unlock(&db->lock);
    CHECK(eng_test_mark_ec(db, p0, EC_MARK_EC) == TSDB_ERR_LOCKED && !p0->ec);
    pthread_mutex_lock(&db->lock);
    p0->merging = false;
    pthread_mutex_unlock(&db->lock);
    /* the statistics' sweeper must not walk the part while the counts below are taken */
    CHECK(stats_settle(db, 10000) == 0);
    atomic_store(&stats_pause, 1);
    CHECK(eng_test_mark_ec(db, p0, EC_MARK_EC | EC_MARK_SETTLING) == 0 && p0->ec && p0->ec->test_mark &&
          p0->settling);
    struct ecpart *e0 = p0->ec;
    CHECK(eng_test_mark_ec(db, p0, EC_MARK_EC) == 0 && p0->ec == e0 && !p0->settling);
    /* reads of a marked part go through ec_granule_read and give the same rows; p1 (a .part) never does */
    uint64_t c0[EC_CALL_N];
    calls_now(c0);
    acc after = read_all(db, s);
    CHECK(after.n == before.n && after.sum == before.sum);
    CHECK(CALLS_SINCE(EC_CALL_GRANULE_READ, c0) >= 1);
    calls_now(c0);
    CHECK(part_index_intact(p0) && part_checked(db, p0, "test"));
    CHECK(CALLS_SINCE(EC_CALL_INDEX_INTACT, c0) >= 1);
    calls_now(c0);
    CHECK(part_index_intact(p1));
    CHECK(CALLS_SINCE(EC_CALL_INDEX_INTACT, c0) == 0);
    /* the byte totals ask eccache_part_bytes for the marked part only */
    tsdb_stats st;
    calls_now(c0);
    CHECK(eng_get_stats(db, &st) == 0);
    CHECK(CALLS_SINCE(EC_CALL_PART_BYTES, c0) == 1);
    calls_now(c0);
    CHECK(eng_foreach_part(db, part_info_fn, NULL) == 0);
    CHECK(CALLS_SINCE(EC_CALL_PART_BYTES, c0) == 1);
    /* the residency of tsdb_series_stats asks the cache for the marked part's granules */
    sscx *x = sscx_open(db);
    CHECK(x != NULL);
    if (x) {
        tsdb_series_stat ss;
        memset(&ss, 0, sizeof ss);
        uint64_t probes = 0;
        calls_now(c0);
        sscx_run(x, &s, 1, &ss, &probes, true);
        CHECK(CALLS_SINCE(EC_CALL_RESIDENT, c0) >= 1);
        sscx_close(x);
    }
    atomic_store(&stats_pause, 0);

    /* a drop retires the marked part as GY_EC; the graveyard purges it through r7-gc's stub */
    char name0[128], retired0[160];
    snprintf(name0, sizeof name0, "%s", p0->name);
    snprintf(retired0, sizeof retired0, "retired_%s", p0->name);
    calls_now(c0);
    uint64_t unref0 = calls(EC_CALL_GC_LAST_UNREF);
    CHECK(eng_drop_before(db, day) == 0);
    CHECK(CALLS_SINCE(EC_CALL_GC_RETIRE, c0) == 1);
    CHECK(!exists(dir, name0) && exists(dir, retired0));
    pthread_mutex_lock(&db->lock);
    bool found = false;
    for (size_t i = 0; i < db->ngraveyard; i++)
        if (!strcmp(db->graveyard[i].name, retired0))
            found = db->graveyard[i].kind == GY_EC;
    pthread_mutex_unlock(&db->lock);
    CHECK(found);
    /* a settled source in the graveyard goes to r7-keep (src/eckeep.c), which releases one only after its digest
     * matched: never without a tier, as here, so the file stays (and is taken away by hand below) */
    touch(dir, "settled_p0_1_1_0.part");
    pthread_mutex_lock(&db->lock);
    CHECK(graveyard_add(db, "settled_p0_1_1_0.part", GY_SETTLED, 0) == 0);
    pthread_mutex_unlock(&db->lock);
    calls_now(c0);
    graveyard_purge(db, true);
    CHECK(!exists(dir, retired0) && exists(dir, "settled_p0_1_1_0.part"));
    CHECK(CALLS_SINCE(EC_CALL_GC_PURGE, c0) == 1 && CALLS_SINCE(EC_CALL_KEEP_PURGE, c0) == 1);
    char settled0[700];
    snprintf(settled0, sizeof settled0, "%s/settled_p0_1_1_0.part", dir);
    CHECK(unlink(settled0) == 0);
    acc dropped = read_all(db, s);
    CHECK(dropped.n == 20000);
    CHECK(eng_close(db) == 0); /* part_unref freed the marked part's ecpart (ASan) */
    CHECK(calls(EC_CALL_GC_LAST_UNREF) - unref0 == 1); /* r7-gc's hook: the marked part's last reference */

    /* R7's names at open: an .ec part fails the open (this build cannot read it: its rows must not vanish without a
     * word); retired_*.ec and settled_* go to their stubs, which leave them alone (no owner yet); tmp_*.ec is removed
     * as a part's temporary is; a v4 intent goes to ecswap_recover, which rolls it back as a torn one */
    touch(dir, "p999_1_1_0.ec");
    touch(dir, "retired_p999_2_2_0.ec");
    touch(dir, "settled_p999_3_3_0.part");
    touch(dir, "tmp_1_2.ec");
    touch(dir, "notes.ec");
    char intent[1024];
    snprintf(intent, sizeof intent, "%s/txn_1_7.intent", dir);
    FILE *f = fopen(intent, "w");
    CHECK(f != NULL);
    if (f) {
        fprintf(f, "tsdb-intent 4 1\nL tmp_1_2.ec p999_1_1_0.ec\nend 0000000000000000\n");
        fclose(f);
    }
    calls_now(c0);
    CHECK(open_eng(dir, 3600, &db) == TSDB_ERR_CORRUPT && !db);
    CHECK(CALLS_SINCE(EC_CALL_LOAD, c0) == 1 && CALLS_SINCE(EC_CALL_SWAP_RECOVER, c0) == 1);
    CHECK(exists(dir, "p999_1_1_0.ec") && !exists(dir, "txn_1_7.intent"));
    char ecpath[700];
    snprintf(ecpath, sizeof ecpath, "%s/p999_1_1_0.ec", dir);
    CHECK(unlink(ecpath) == 0);
    calls_now(c0);
    CHECK(open_eng(dir, 3600, &db) == 0);
    CHECK(CALLS_SINCE(EC_CALL_GC_LOAD, c0) == 1 && CALLS_SINCE(EC_CALL_KEEP_LOAD, c0) == 1);
    CHECK(CALLS_SINCE(EC_CALL_LOAD, c0) == 0 && CALLS_SINCE(EC_CALL_SWAP_RECOVER, c0) == 0);
    if (db) {
        CHECK(exists(dir, "retired_p999_2_2_0.ec") && exists(dir, "settled_p999_3_3_0.part") &&
              exists(dir, "notes.ec"));
        CHECK(!exists(dir, "tmp_1_2.ec") && !exists(dir, "txn_1_7.intent"));
        acc again = read_all(db, s);
        CHECK(again.n == dropped.n && again.sum == dropped.sum);
        CHECK(eng_close(db) == 0);
    }

    /* a volume add refuses a directory holding any of R7's names (engine_file_name) */
    static const char *const r7names[] = {"p5_1_1_0.ec", "tmp_3_4.ec", "retired_p5_1_1_0.ec", "settled_p5_1_1_0.part"};
    char root[600], vol[600];
    fresh(root, sizeof root, "root");
    fresh(vol, sizeof vol, "vol");
    tsdb *t = NULL;
    CHECK(tsdb_open(root, NULL, &t) == 0);
    if (t) {
        CHECK(tsdb_close(t) == 0);
        for (size_t i = 0; i < sizeof r7names / sizeof *r7names; i++) {
            rm_rf(vol);
            CHECK(mkdir(vol, 0755) == 0);
            touch(vol, r7names[i]);
            uint32_t index;
            int rc = tsdb_volume_add_dir(root, vol, &index);
            if (rc == 0) {
                fprintf(stderr, "FAIL a volume add took a directory holding %s\n", r7names[i]);
                failures++;
            }
        }
        rm_rf(vol);
        CHECK(mkdir(vol, 0755) == 0);
        touch(vol, "notes.ec"); /* a user's file */
        uint32_t index;
        CHECK(tsdb_volume_add_dir(root, vol, &index) == 0);
    }
    rm_rf(root);
    rm_rf(vol);
    rm_rf(dir);
}

/* cls_check_parts decodes a test-marked part's granules (it is a .part underneath) where a granule spans a stay in
 * another class (as tests/test_crafted.c's chkorder builds one), and asks ec_cls_record for every part with p->ec;
 * only a real .ec part skips the decode */
static void cls_case(void)
{
    char dir[600];
    fresh(dir, sizeof dir, "cls");
    eng *db = NULL;
    CHECK(open_eng(dir, 3600, &db) == 0);
    if (!db)
        return;
    uint32_t s;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    const int64_t D = db->partition_ms;
    int64_t t0 = INT64_MIN, t1 = D, t2 = 2 * D;
    CHECK(eng_class_switch(db, s, 1, &t0) == 0); /* class 1, class 0 for the second day, then class 1 again */
    CHECK(eng_class_switch(db, s, 0, &t1) == 0);
    CHECK(eng_class_switch(db, s, 1, &t2) == 0);
    float v[4] = {1, 2, 3, 4};
    int64_t ts[4] = {D - 2, 2 * D + 123457, 2 * D + 45678901, 6 * D + 1234567};
    part_writer w;
    part *pp = NULL;
    CHECK(pw_begin(&w, db, 1 /* class 1, base partition 0 */, 0) == 0);
    w.cut_ms = 0; /* one granule across the stay in class 0 */
    CHECK(pw_add(&w, s, TSDB_FLOAT32, ts, v, 4) == 0);
    CHECK(pw_finish_pending(&w, 0, 0, 1, &pp) == 0);
    CHECK(pp && parts_publish(db, &pp, 1) == 0);
    pthread_mutex_lock(&db->lock);
    part *p = db->nparts == 1 ? db->parts[0] : NULL;
    pthread_mutex_unlock(&db->lock);
    CHECK(p != NULL);
    uint64_t c0[EC_CALL_N];
    if (p) {
        calls_now(c0);
        CHECK(cls_check_parts(db) == 0);
        CHECK(CALLS_SINCE(EC_CALL_CLS_RECORD, c0) == 0 && CALLS_SINCE(EC_CALL_GRANULE_READ, c0) == 0);
        CHECK(eng_test_mark_ec(db, p, EC_MARK_EC) == 0);
        calls_now(c0);
        CHECK(cls_check_parts(db) == 0);
        CHECK(CALLS_SINCE(EC_CALL_CLS_RECORD, c0) == 1 && CALLS_SINCE(EC_CALL_GRANULE_READ, c0) >= 1);
        /* a merge's verbatim copy refuses an .ec source (its map has no granule bytes once r7-seam fills it);
         * nothing reaches the writer */
        part_writer mw;
        CHECK(pw_begin(&mw, db, 1, 0) == 0);
        CHECK(pw_copy_granule(&mw, p, 0) == TSDB_ERR_INVALID);
        CHECK(mw.ng == 0);
        pw_abort(&mw);
    }
    CHECK(eng_close(db) == 0);
    rm_rf(dir);
}

/* a follower whose writer's MANIFEST names an .ec part asks ecpart_follower_open, which refuses, and keeps its view */
static void follower_case(void)
{
    char dir[600];
    fresh(dir, sizeof dir, "follow");
    eng *db = NULL, *f = NULL;
    CHECK(open_eng(dir, 3600, &db) == 0);
    if (!db)
        return;
    uint32_t s;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    write_rows(db, s, 0, 20000);
    CHECK(eng_flush(db) == 0);
    tsdb_options fo;
    tsdb_options_init(&fo);
    fo.follow = 1;
    fo.refresh_ms = 1000000; /* refreshed by hand below */
    fo.create_volumes = 0;
    CHECK(eng_open(dir, &fo, &f) == 0);
    if (f) {
        acc a = read_all(f, s);
        CHECK(a.n == 20000);
        /* the writer lists its part under an .ec name for one MANIFEST, beside a real file of that name (a part's
         * bytes: part_open_as would take it, and the follower would serve the rows twice) */
        pthread_mutex_lock(&db->lock);
        part *p = db->nparts == 1 ? db->parts[0] : NULL;
        char real[96] = "", ecname[96] = "";
        if (p) {
            snprintf(real, sizeof real, "%s", p->name);
            snprintf(ecname, sizeof ecname, "%.*s.ec", (int)(strlen(real) - 5), real);
            snprintf(p->name, sizeof p->name, "%s", ecname);
        }
        pthread_mutex_unlock(&db->lock);
        CHECK(p != NULL);
        if (p) {
            CHECK(linkat(db->dirfd, real, db->dirfd, ecname, 0) == 0);
            CHECK(manifest_publish(db) == 0);
            pthread_mutex_lock(&db->lock);
            snprintf(p->name, sizeof p->name, "%s", real);
            pthread_mutex_unlock(&db->lock);
            uint64_t c0[EC_CALL_N];
            calls_now(c0);
            CHECK(eng_refresh(f) != 0); /* the previous view stays */
            CHECK(CALLS_SINCE(EC_CALL_FOLLOWER_OPEN, c0) == 1);
            acc b = read_all(f, s);
            CHECK(b.n == a.n && b.sum == a.sum);
            CHECK(manifest_publish(db) == 0);
            CHECK(unlinkat(db->dirfd, ecname, 0) == 0);
        }
        eng_close(f);
    }
    CHECK(eng_close(db) == 0);
    rm_rf(dir);
}

/* tsdb upgrade refuses a format-8 database holding any of R7's names (a newer release's files), before it writes
 * anything; the same fixture without one upgrades */
static void upgrade_case(void)
{
    static const char *const r7names[] = {"p5_1_1_0.ec", "tmp_3_4.ec", "retired_p5_1_1_0.ec", "settled_p5_1_1_0.part"};
    char top[600], cmd[2000], path[800], line[800];
    for (size_t i = 0; i <= sizeof r7names / sizeof *r7names; i++) {
        fresh(top, sizeof top, "upgrade");
        snprintf(cmd, sizeof cmd, "mkdir -p '%s' && tar -xf tests/data/v8-fixture.tar -C '%s'", top, top);
        if (system(cmd) != 0) {
            fprintf(stderr, "FAIL cannot unpack tests/data/v8-fixture.tar (run from the source tree)\n");
            failures++;
            return;
        }
        char vol[700], db[700];
        snprintf(vol, sizeof vol, "%s/vol1", top);
        snprintf(db, sizeof db, "%s/db", top);
        snprintf(path, sizeof path, "%s/VOLUMES", db);
        int len = snprintf(line, sizeof line, "%08x %s\n", (uint32_t)xxh64(vol, strlen(vol), 1), vol);
        FILE *f = fopen(path, "w");
        CHECK(f && fwrite(line, 1, (size_t)len, f) == (size_t)len);
        if (f)
            fclose(f);
        bool r7 = i < sizeof r7names / sizeof *r7names;
        if (r7)
            touch(i % 2 ? db : vol, r7names[i]); /* the volume and the root are both scanned */
        int rc = tsdb_upgrade(db);
        if (r7) {
            if (rc != TSDB_ERR_INVALID || exists(db, "FNO") || exists(vol, "FNO")) {
                fprintf(stderr, "FAIL upgrade with %s: rc %d, FNO written %d %d\n", r7names[i], rc, exists(db, "FNO"),
                        exists(vol, "FNO"));
                failures++;
            }
        } else {
            CHECK(rc == 2 && exists(db, "FNO") && exists(vol, "FNO")); /* both directories carried over */
        }
        rm_rf(top);
    }
}

int main(int argc, char **argv)
{
    int a = 1;
    if (a < argc && !strcmp(argv[a], "--mutant")) {
        if (a + 1 >= argc) {
            fprintf(stderr, "usage: test_ecnames [--mutant nameloose|tierloose] [SCRATCH]\n");
            return 2;
        }
        if (!strcmp(argv[a + 1], "nameloose"))
            r7_mutant = R7_MUT_NAMELOOSE;
        else if (!strcmp(argv[a + 1], "tierloose"))
            r7_mutant = R7_MUT_TIERLOOSE;
        else {
            fprintf(stderr, "test_ecnames: unknown mutant %s\n", argv[a + 1]);
            return 2;
        }
        a += 2;
    }
    snprintf(scratch, sizeof scratch, "%s", a < argc ? argv[a] : "/tmp");
    names();
    cfg_good();
    cfg_bad();
    from_strings();
    open_refusal();
    tier_refs_case();
    stubs_case();
    cls_case();
    follower_case();
    upgrade_case();
    if (failures) {
        fprintf(stderr, "test_ecnames: %d failures\n", failures);
        return 1;
    }
    printf("test_ecnames: ok\n");
    return 0;
}
