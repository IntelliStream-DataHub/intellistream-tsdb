/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R7's interfaces (r7-iface): the names of .ec parts, their temporaries, retired ones and settled sources, each
 * accepted by its own predicate only, with every near miss refused; TSDB_TEST_TIER's grammar on good and bad strings
 * and the refusal without TSDB_TEST_TIER_I_KNOW=1 (tsdb_open fails and makes nothing); the tier's references through
 * engine opens and closes; the stubs leave a database as today's code does (R7's names at open, a v4 intent, the
 * graveyard kinds, a volume add); eng_test_mark_ec and reads of a marked part.
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
    int rc = tsdb_open(dir, NULL, &t);
#ifdef TSDB_EC
    CHECK(rc == 0 && t);
    if (t) {
        tsdb_sid id;
        CHECK(tsdb_series(t, "s", &id) == 0);
        CHECK(tsdb_close(t) == 0);
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
    char dir[600], dir2[600];
    fresh(dir, sizeof dir, "refs");
    fresh(dir2, sizeof dir2, "refs2");
    setenv("TSDB_TEST_TIER", "k=1,m=1,dirs=/nonexistent/a:/nonexistent/b", 1);
    setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);
    tier *t = NULL;
    CHECK(tier_open(dir, NULL, &t) == 0 && t);
    unsetenv("TSDB_TEST_TIER");
    unsetenv("TSDB_TEST_TIER_I_KNOW");
    if (!t)
        return;
    CHECK(tier_refs(t) == 1 && tier_config(t)->k == 1);
    eng *a = NULL, *b = NULL;
    tier_want(t, 3);
    CHECK(open_eng(dir, 0, &a) == 0);
    CHECK(open_eng(dir2, 0, &b) == 0); /* the wish was taken by the first open */
    tier_want(NULL, 0);
    CHECK(a && a->tier == t && a->tier_vol == 3 && a->ecstate);
    CHECK(b && b->tier == NULL && b->ecstate == NULL);
    CHECK(tier_refs(t) == 2);
    eng_close(b);
    eng_close(a);
    CHECK(tier_refs(t) == 1);
    tier_want(t, 1); /* an open that fails gives its reference back */
    eng *c = NULL;
    tsdb_options o;
    tsdb_options_init(&o);
    o.value_codec = 99;
    CHECK(eng_open(dir, &o, &c) != 0 && !c);
    touch(dir2, "p1_1_1_0.part"); /* a broken part: load_parts fails the open after the attach */
    CHECK(open_eng(dir2, 0, &c) == TSDB_ERR_CORRUPT && !c);
    tier_want(NULL, 0);
    CHECK(tier_refs(t) == 1);
    tier_put(t);
    rm_rf(dir);
    rm_rf(dir2);
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
    CHECK(eng_test_mark_ec(db, p0, EC_MARK_EC | EC_MARK_SETTLING) == 0 && p0->ec && p0->ec->test_mark &&
          p0->settling);
    struct ecpart *e0 = p0->ec;
    CHECK(eng_test_mark_ec(db, p0, EC_MARK_EC) == 0 && p0->ec == e0 && !p0->settling);
    /* reads of a marked part go through ec_granule_read and give the same rows */
    acc after = read_all(db, s);
    CHECK(after.n == before.n && after.sum == before.sum);
    CHECK(part_index_intact(p0) && part_checked(db, p0, "test"));

    /* a drop retires the marked part as GY_EC; the graveyard purges it through r7-gc's stub */
    char name0[128], retired0[160];
    snprintf(name0, sizeof name0, "%s", p0->name);
    snprintf(retired0, sizeof retired0, "retired_%s", p0->name);
    CHECK(eng_drop_before(db, day) == 0);
    CHECK(!exists(dir, name0) && exists(dir, retired0));
    pthread_mutex_lock(&db->lock);
    bool found = false;
    for (size_t i = 0; i < db->ngraveyard; i++)
        if (!strcmp(db->graveyard[i].name, retired0))
            found = db->graveyard[i].kind == GY_EC;
    pthread_mutex_unlock(&db->lock);
    CHECK(found);
    graveyard_purge(db, true);
    CHECK(!exists(dir, retired0));
    acc dropped = read_all(db, s);
    CHECK(dropped.n == 20000);
    CHECK(eng_close(db) == 0); /* part_unref freed the marked part's ecpart (ASan) */

    /* R7's names at open: .ec, retired_*.ec and settled_* are left alone (no owner yet), tmp_*.ec removed as a
     * part's temporary is; a v4 intent is rolled back as a torn one (nothing writes one yet) */
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
    CHECK(open_eng(dir, 3600, &db) == 0);
    if (db) {
        CHECK(exists(dir, "p999_1_1_0.ec") && exists(dir, "retired_p999_2_2_0.ec") &&
              exists(dir, "settled_p999_3_3_0.part") && exists(dir, "notes.ec"));
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
    if (failures) {
        fprintf(stderr, "test_ecnames: %d failures\n", failures);
        return 1;
    }
    printf("test_ecnames: ok\n");
    return 0;
}
