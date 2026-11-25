/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The swap of a settle (src/ecswap.c, r7-swap): intent v4, ec_swap_commit with its input check under db->lock,
 * ecswap_recover. The .ec parts are made by hand until r7-settle: a copy of the source's file under tmp_<pid>_<n>.ec,
 * opened as a part named P.ec and marked like eng_test_mark_ec's parts (every R7 stub reads it as a .part), so reads
 * of a swapped key give the same rows. Until r7-ecfile an open that meets P.ec fails (ecpart_load's stub), so the
 * reopens after a swap judge the files the recovery left, not the rows.
 *
 *   basic      a swap: the files, the list in place, the same rows and latest rows, lgen bumped, MANIFEST; refusals
 *   drop       a drop completing inside ec_swap_hook: TSDB_ERR_STOPPED, nothing renamed, the temporaries and the
 *              intent gone, no row of the dropped key visible, before and after a reopen (lifetime 0 and 3600)
 *   recover    each rule of ecswap_recover on intents and files planted by hand
 *   crashpt    TSDB_SETTLE_CRASH_AT at every point (between-renames at each of its passes), then an open: the key is
 *              all .part and no .ec, or all .ec with every settled_ present; then power lost at each step of that
 *              open's recovery
 *   power      power lost (crashfs, torn at random) at every step of an open, a swap and a close, two seeds each
 *   logged     R8: a swap whose objects a committed COLD names is never handed to r7-gc when it is refused
 *   threads    16 threads query every key and look up the latest row while the keys are swapped one by one
 *
 * Usage: test_ecswap [--quick] [--mutant swapearly|swapnodropcheck|...|swaploggedgc] [SCRATCH]. Without
 * TSDB_EC (make NO_EC=1, the bindings) it says it is skipped.
 */
#include "internal.h"
#include "ecpart.h"
#include "ecswap.h"
#include "stats_priv.h"

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
static char S[PATH_MAX], SELF[PATH_MAX], SHIM[PATH_MAX];
static const struct {
    const char *name;
    int m;
} MUTANTS[] = {{"none", SWAP_MUT_NONE},
               {"swapearly", SWAP_MUT_EARLY},
               {"swappartial", SWAP_MUT_PARTIAL},
               {"swapnodirsync", SWAP_MUT_NODIRSYNC},
               {"swapnodropcheck", SWAP_MUT_NODROPCHECK},
               {"swapnolisted", SWAP_MUT_NOLISTED},
               {"swapnofailcheck", SWAP_MUT_NOFAILCHECK},
               {"swapnostats", SWAP_MUT_NOSTATS},
               {"swapmanifestrc", SWAP_MUT_MANIFESTRC},
               {"swapbackongone", SWAP_MUT_BACKONGONE},
               {"swaplostsettled", SWAP_MUT_LOSTSETTLED},
               {"swaploggedgc", SWAP_MUT_LOGGEDGC}};

static const char *mutant_name(int m)
{
    for (size_t i = 0; i < sizeof MUTANTS / sizeof *MUTANTS; i++)
        if (MUTANTS[i].m == m)
            return MUTANTS[i].name;
    return "none";
}
static bool quick;
static const int64_t DAY = 86400000;
enum { KEYS = 3, ROWS = 3000 };

static int sh(const char *fmt, ...)
{
    char cmd[4096];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(cmd, sizeof cmd, fmt, ap);
    va_end(ap);
    return system(cmd);
}

static void fresh(const char *what, char *out, size_t cap)
{
    snprintf(out, cap, "%s/%s", S, what);
    sh("rm -rf '%s'; mkdir -p '%s'", out, out);
}

static bool exists(const char *dir, const char *name)
{
    char p[PATH_MAX + 256];
    snprintf(p, sizeof p, "%s/%s", dir, name);
    return access(p, F_OK) == 0;
}

static int open_eng(const char *dir, int lifetime_s, eng **db)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0; /* no merge barrier for .ec parts before r7-skip */
    o.create_volumes = 0;
    o.partition_classes = 0; /* one key per day */
    o.obsolete_part_lifetime_s = lifetime_s;
    return eng_open(dir, &o, db);
}

static void write_rows(eng *db, uint32_t s, int64_t t0, int n, int64_t step)
{
    int64_t ts[256];
    float v[256];
    for (int i = 0; i < n; i += 256) {
        int k = n - i < 256 ? n - i : 256;
        for (int j = 0; j < k; j++)
            ts[j] = t0 + (int64_t)(i + j) * step, v[j] = (float)((t0 / 1000 + i + j) % 977);
        CHECK(eng_insert_series(db, TSDB_FLOAT32, s, ts, v, (size_t)k) == 0);
    }
}

/* KEYS days, two parts each (two flushes), series "s" (and "t" on day 2 only) */
static void populate(eng *db)
{
    uint32_t s, t;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    CHECK(eng_series(db, "t", TSDB_FLOAT32, &t) == 0);
    for (int d = 0; d < KEYS; d++)
        write_rows(db, s, d * DAY, ROWS, 2000);
    write_rows(db, t, 2 * DAY + 5, 100, 1000);
    CHECK(eng_flush(db) == 0);
    for (int d = 0; d < KEYS; d++)
        write_rows(db, s, d * DAY + 1000, ROWS, 2000);
    CHECK(eng_flush(db) == 0);
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

static acc read_range(eng *db, uint32_t s, int64_t a, int64_t b)
{
    acc r = {0, 0};
    CHECK(eng_query_raw_ex(db, s, a, b, false, 0, acc_fn, &r) == 0);
    return r;
}

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

static atomic_uint ec_counter;

/* An .ec part for the listed .part p, by hand (see the top): a copy of its file, fsynced, opened and marked */
static part *make_ec(eng *db, part *p)
{
    char tmp[64];
    snprintf(tmp, sizeof tmp, "tmp_%d_%u.ec", (int)getpid(), 900000 + atomic_fetch_add(&ec_counter, 1));
    int fd = openat(db->dirfd, tmp, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    CHECK(fd >= 0);
    if (fd < 0)
        return NULL;
    bool ok = write(fd, p->map, p->map_len) == (ssize_t)p->map_len && fsync(fd) == 0;
    CHECK(ok);
    close(fd);
    part *e = NULL;
    CHECK(part_open_as(db->dirfd, tmp, p->name, false, false, &e) == 0);
    if (!e)
        return NULL;
    e->verify = p->verify;
    snprintf(e->name, sizeof e->name, "%.*s.ec", (int)(strlen(p->name) - 5), p->name);
    snprintf(e->pending, sizeof e->pending, "%s", tmp);
    struct ecpart *ep = calloc(1, sizeof *ep);
    CHECK(ep != NULL);
    ep->test_mark = true;
    ep->tier = db->tier;
    ep->volume = db->tier_vol;
    atomic_store(&e->ec, ep);
    return e;
}

/* swaps every part of key; the names of its sources into names (stems without ".part") */
static int swap_key(eng *db, int64_t key, char names[][96], size_t *nnames)
{
    part *in[16], *ec[16];
    size_t n = key_parts(db, key, in, 16);
    CHECK(n >= 1);
    for (size_t i = 0; i < n; i++) {
        ec[i] = make_ec(db, in[i]);
        if (names)
            snprintf(names[i], 96, "%.*s", (int)(strlen(in[i]->name) - 5), in[i]->name);
    }
    if (nnames)
        *nnames = n;
    int rc = ec_swap_commit(db, in, ec, n);
    for (size_t i = 0; i < n; i++) {
        part_unref(in[i]);
        part_unref(ec[i]);
    }
    return rc;
}

static size_t count_names(const char *dir, const char *prefix, const char *suffix)
{
    DIR *d = opendir(dir);
    size_t n = 0;
    struct dirent *e;
    while (d && (e = readdir(d))) {
        size_t l = strlen(e->d_name), ls = strlen(suffix);
        n += strncmp(e->d_name, prefix, strlen(prefix)) == 0 && l >= ls && strcmp(e->d_name + l - ls, suffix) == 0;
    }
    if (d)
        closedir(d);
    return n;
}

/* The invariant of a swap of the sources stem[0 .. n): 1 when all are .part (no .ec, no settled_), 2 when all are
 * .ec with every settled_ present (no .part), 0 otherwise (with a line) */
static int key_state(const char *dir, char stem[][96], size_t n, const char *what)
{
    size_t parts = 0, ecs = 0, settled = 0;
    char nm[160];
    for (size_t i = 0; i < n; i++) {
        snprintf(nm, sizeof nm, "%s.part", stem[i]);
        parts += exists(dir, nm);
        snprintf(nm, sizeof nm, "%s.ec", stem[i]);
        ecs += exists(dir, nm);
        snprintf(nm, sizeof nm, "settled_%s.part", stem[i]);
        settled += exists(dir, nm);
    }
    if (parts == n && ecs == 0 && settled == 0)
        return 1;
    if (parts == 0 && ecs == n && settled == n)
        return 2;
    fprintf(stderr, "FAIL %s: of %zu sources %zu .part, %zu .ec, %zu settled_\n", what, n, parts, ecs, settled);
    return 0;
}

/* an open after a crash: recovery runs; until r7-ecfile a listed .ec name fails the open after it (CORRUPT) */
static int reopen_check(const char *dir, char stem[][96], size_t n, const char *what)
{
    eng *db = NULL;
    int rc = open_eng(dir, 3600, &db);
    CHECK(rc == 0 || rc == TSDB_ERR_CORRUPT);
    if (rc != 0 && rc != TSDB_ERR_CORRUPT)
        fprintf(stderr, "  (%s: open %d)\n", what, rc);
    if (db)
        CHECK(eng_close(db) == 0);
    int st = key_state(dir, stem, n, what);
    CHECK(st != 0);
    CHECK(count_names(dir, "tmp_", ".ec") == 0 && count_names(dir, "txn_", ".intent") == 0);
    return st;
}

/* ------------------------------------------------------------ basic */

static uint64_t lgen_of(eng *db)
{
    if (!db->lt)
        return 0;
    pthread_rwlock_rdlock(&db->lt->rw);
    uint64_t g = db->lt->lgen;
    pthread_rwlock_unlock(&db->lt->rw);
    return g;
}

static void basic_case(void)
{
    char dir[PATH_MAX];
    fresh("basic", dir, sizeof dir);
    eng *db = NULL;
    CHECK(open_eng(dir, 3600, &db) == 0);
    if (!db)
        return;
    populate(db);
    uint32_t s = 0, t = 1;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0 && eng_series(db, "t", TSDB_FLOAT32, &t) == 0);
    acc before = read_range(db, s, INT64_MIN, INT64_MAX), bt = read_range(db, t, INT64_MIN, INT64_MAX);
    CHECK(before.n == 2u * KEYS * ROWS && bt.n == 100);
    latest_row l0[2], l1[2];
    uint32_t ids[2] = {s, t};
    CHECK(eng_latest(db, ids, 2, NULL, INT64_MAX, false, l0) == 0);
    size_t nparts0 = db->nparts;

    /* refusals: nothing written */
    part *in[4], *ec[4];
    size_t n = key_parts(db, 2, in, 4);
    CHECK(n == 2);
    ec[0] = make_ec(db, in[0]);
    ec[1] = make_ec(db, in[1]);
    part *sw[2] = {ec[1], ec[0]};
    CHECK(ec_swap_commit(db, in, sw, 2) == TSDB_ERR_INVALID); /* another part's .ec */
    part *dup[2] = {in[0], in[0]};
    CHECK(ec_swap_commit(db, dup, ec, 2) == TSDB_ERR_INVALID);
    part *mixed[2] = {in[0], NULL};
    part *other[4];
    CHECK(key_parts(db, 1, other, 4) == 2);
    mixed[1] = other[0];
    CHECK(ec_swap_commit(db, mixed, ec, 2) == TSDB_ERR_INVALID); /* two keys */
    CHECK(count_names(dir, "txn_", ".intent") == 0 && count_names(dir, "tmp_", ".ec") == 2);
    /* a part in a merge is refused under db->lock, like a dropped one: nothing renamed, the temporaries gone */
    pthread_mutex_lock(&db->lock);
    in[1]->merging = true;
    pthread_mutex_unlock(&db->lock);
    CHECK(ec_swap_commit(db, in, ec, 2) == TSDB_ERR_STOPPED);
    pthread_mutex_lock(&db->lock);
    in[1]->merging = false;
    pthread_mutex_unlock(&db->lock);
    CHECK(count_names(dir, "txn_", ".intent") == 0 && count_names(dir, "tmp_", ".ec") == 0);
    CHECK(count_names(dir, "p2_", ".ec") == 0 && count_names(dir, "settled_", ".part") == 0);
    for (size_t i = 0; i < 2; i++) {
        part_unref(in[i]);
        part_unref(ec[i]);
        part_unref(other[i]);
    }
    CHECK(eng_check(db) == 0);

    /* a read-only engine beside the writer refuses */
    eng *rdb = NULL;
    tsdb_options o;
    tsdb_options_init(&o);
    o.read_only = true;
    o.partition_classes = 0;
    o.create_volumes = 0;
    int orc = eng_open(dir, &o, &rdb);
    if (orc != 0)
        fprintf(stderr, "  (read-only open: %d)\n", orc);
    CHECK(orc == 0);
    if (rdb) {
        part *ro[4], *re[4];
        CHECK(key_parts(rdb, 2, ro, 4) == 2);
        re[0] = ro[1];
        CHECK(ec_swap_commit(rdb, ro, re, 1) == TSDB_ERR_READONLY);
        part_unref(ro[0]);
        part_unref(ro[1]);
        CHECK(eng_close(rdb) == 0);
    }
    /* the swap of key 2, with the statistics' sweeper running (a sweeper that starts after it reconciles with the
     * list and would hide missing events) */
    CHECK(stats_settle(db, 60000) == 0);
    uint64_t g0 = lgen_of(db);
    char stem[4][96];
    size_t ns = 0;
    CHECK(swap_key(db, 2, stem, &ns) == 0 && ns == 2);
    CHECK(db->nparts == nparts0);
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++) {
        part *p = db->parts[i];
        CHECK((p->partition == 2) == (p->ec != NULL));
        CHECK(i == 0 || db->parts[i - 1]->partition <= p->partition);
        CHECK(!p->pending[0]);
    }
    pthread_mutex_unlock(&db->lock);
    CHECK(!db->lt || lgen_of(db) > g0);
    /* the statistics follow the swap (a third change of the list beside db_parts_insert/remove_locked): once the
     * sweeper has caught up, every listed part has its record, the .ec parts included */
    CHECK(stats_settle(db, 60000) == 0);
    struct estats *es = atomic_load(&db->stats);
    CHECK(es != NULL);
    if (es) {
        pthread_mutex_lock(&db->lock);
        pthread_mutex_lock(&es->mu);
        for (size_t i = 0; i < db->nparts; i++)
            if (!rec_find(es, db->parts[i])) {
                fprintf(stderr, "FAIL basic: no statistics record of listed part %s\n", db->parts[i]->name);
                failures++;
            }
        pthread_mutex_unlock(&es->mu);
        pthread_mutex_unlock(&db->lock);
    }
    CHECK(key_state(dir, stem, ns, "basic") == 2);
    CHECK(count_names(dir, "txn_", ".intent") == 0 && count_names(dir, "tmp_", ".ec") == 0);
    acc after = read_range(db, s, INT64_MIN, INT64_MAX), at = read_range(db, t, INT64_MIN, INT64_MAX);
    CHECK(after.n == before.n && after.sum == before.sum && at.n == bt.n && at.sum == bt.sum);
    CHECK(eng_latest(db, ids, 2, NULL, INT64_MAX, false, l1) == 0);
    for (int i = 0; i < 2; i++)
        CHECK(l1[i].status == l0[i].status && l1[i].ts == l0[i].ts && l1[i].slot == l0[i].slot);
    char mf[PATH_MAX + 16], buf[8192];
    snprintf(mf, sizeof mf, "%s/MANIFEST", dir);
    FILE *f = fopen(mf, "r");
    size_t got = f ? fread(buf, 1, sizeof buf - 1, f) : 0;
    if (f)
        fclose(f);
    buf[got] = '\0';
    char nm[160];
    snprintf(nm, sizeof nm, "%s.ec", stem[0]);
    CHECK(strstr(buf, nm) != NULL); /* followers learn of the swap (and stop at it: D7) */
    /* an .ec part is not swapped again */
    part *again[4];
    CHECK(key_parts(db, 2, again, 4) == 2);
    part *e2 = make_ec(db, key_parts(db, 1, other, 4) ? other[0] : NULL);
    part *one[1] = {again[0]};
    CHECK(ec_swap_commit(db, one, &e2, 1) == TSDB_ERR_INVALID);
    unlinkat(db->dirfd, e2->pending, 0);
    part_unref(e2);
    for (size_t i = 0; i < 2; i++) {
        part_unref(again[i]);
        part_unref(other[i]);
    }
    CHECK(eng_close(db) == 0);
    CHECK(reopen_check(dir, stem, ns, "basic reopen") == 2);

    printf("  a swap: each part replaced in place by its .ec, sources settled_, the same rows and latest rows, lgen "
           "bumped, MANIFEST names the .ec; bad arguments, a part in a merge and a read-only engine refused\n");
}

/* ------------------------------------------------------------ a drop inside the hook */

static void drop_hook(eng *db)
{
    CHECK(drop_retire(db, DAY) == 0); /* the drop's part after its flush: latest_rw and db->lock, no publish_lock */
}

static void drop_case(int lifetime)
{
    char dir[PATH_MAX];
    fresh(lifetime ? "drop" : "drop0", dir, sizeof dir);
    eng *db = NULL;
    CHECK(open_eng(dir, lifetime, &db) == 0);
    if (!db)
        return;
    populate(db);
    uint32_t s = 0;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    acc rest = read_range(db, s, DAY, INT64_MAX);
    char stem[4][96];
    size_t ns = 0;
    ec_swap_hook = drop_hook;
    int rc = swap_key(db, 0, stem, &ns);
    ec_swap_hook = NULL;
    CHECK(rc == TSDB_ERR_STOPPED);
    CHECK(eng_check(db) == 0);
    CHECK(read_range(db, s, INT64_MIN, DAY).n == 0);
    acc r2 = read_range(db, s, INT64_MIN, INT64_MAX);
    CHECK(r2.n == rest.n && r2.sum == rest.sum);
    CHECK(count_names(dir, "p0_", ".ec") == 0 && count_names(dir, "settled_", ".part") == 0);
    CHECK(count_names(dir, "tmp_", ".ec") == 0 && count_names(dir, "txn_", ".intent") == 0);
    CHECK(count_names(dir, "p0_", ".part") == 0);
    CHECK(count_names(dir, "retired_p0_", ".part") == (lifetime ? ns : 0));
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++)
        CHECK(db->parts[i]->partition != 0 && !db->parts[i]->ec);
    pthread_mutex_unlock(&db->lock);
    /* the engine goes on: the next key swaps */
    char st1[4][96];
    size_t n1 = 0;
    CHECK(swap_key(db, 1, st1, &n1) == 0);
    CHECK(key_state(dir, st1, n1, "drop, then key 1") == 2);
    CHECK(eng_close(db) == 0);
    CHECK(sh("rm -f '%s'/p1_*.ec", dir) == 0);
    for (size_t i = 0; i < n1; i++)
        sh("mv '%s/settled_%s.part' '%s/%s.part'", dir, st1[i], dir, st1[i]);
    CHECK(open_eng(dir, lifetime, &db) == 0);
    if (db) {
        CHECK(read_range(db, s, INT64_MIN, DAY).n == 0);
        acc r3 = read_range(db, s, INT64_MIN, INT64_MAX);
        CHECK(r3.n == rest.n && r3.sum == rest.sum);
        CHECK(eng_close(db) == 0);
    }
    printf("  a drop of the key inside the swap's hook (lifetime %d): TSDB_ERR_STOPPED, nothing renamed, temporaries "
           "and intent gone, no row of the key visible before or after a reopen\n", lifetime);
}

/* ------------------------------------------------------------ the contract around the check and the publish */

static int publish_nomem(eng *db)
{
    (void)manifest_publish(db);
    return TSDB_ERR_NOMEM;
}

static int64_t hook_key;

/* a merge of the key committed between the swap's choice and its check: its first source unlisted and retired */
static void merged_hook(eng *db)
{
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++)
        if (db->parts[i]->partition == hook_key) {
            db_retire_locked(db, db->parts[i]);
            break;
        }
    pthread_mutex_unlock(&db->lock);
}

static void failing_hook(eng *db) { eng_fail(db, "test_ecswap: the engine fails inside the swap's hook", EIO); }

static void contract_case(void)
{
    char dir[PATH_MAX], stem[4][96];
    size_t ns = 0;
    eng *db = NULL;

    /* a durable swap returns 0 even when MANIFEST's publish says out of memory: the .ec parts are listed */
    fresh("publish", dir, sizeof dir);
    CHECK(open_eng(dir, 3600, &db) == 0);
    if (db) {
        populate(db);
        ec_swap_publish = publish_nomem;
        int rc = swap_key(db, 1, stem, &ns);
        ec_swap_publish = manifest_publish;
        CHECK(rc == 0);
        CHECK(key_state(dir, stem, ns, "publish") == 2 && eng_check(db) == 0);
        pthread_mutex_lock(&db->lock);
        for (size_t i = 0; i < db->nparts; i++)
            CHECK((db->parts[i]->partition == 1) == (db->parts[i]->ec != NULL));
        pthread_mutex_unlock(&db->lock);
        CHECK(eng_close(db) == 0);
    }

    /* an input a merge unlisted before the check: refused, nothing renamed, the temporaries and the intent gone */
    fresh("merged", dir, sizeof dir);
    db = NULL;
    CHECK(open_eng(dir, 3600, &db) == 0);
    if (db) {
        populate(db);
        hook_key = 1;
        ec_swap_hook = merged_hook;
        int rc = swap_key(db, 1, stem, &ns);
        ec_swap_hook = NULL;
        if (rc != TSDB_ERR_STOPPED)
            fprintf(stderr, "  (merged: swap %d)\n", rc);
        CHECK(rc == TSDB_ERR_STOPPED && eng_check(db) == 0);
        CHECK(count_names(dir, "p1_", ".ec") == 0 && count_names(dir, "settled_", ".part") == 0);
        CHECK(count_names(dir, "tmp_", ".ec") == 0 && count_names(dir, "txn_", ".intent") == 0);
        CHECK(count_names(dir, "retired_p1_", ".part") == 1 && count_names(dir, "p1_", ".part") == 1);
        CHECK(eng_close(db) == 0);
    }

    /* the engine failed after the intent: nothing renamed (a failed engine names nothing), the files left for the
     * open, which finds the swap cut before its renames and rolls it forward (rows: once r7-ecfile's parts open) */
    fresh("failed", dir, sizeof dir);
    db = NULL;
    CHECK(open_eng(dir, 3600, &db) == 0);
    if (db) {
        populate(db);
        ec_swap_hook = failing_hook;
        int rc = swap_key(db, 1, stem, &ns);
        ec_swap_hook = NULL;
        CHECK(rc == TSDB_ERR_IO && eng_check(db) != 0);
        CHECK(count_names(dir, "txn_", ".intent") == 1 && count_names(dir, "tmp_", ".ec") == ns);
        CHECK(key_state(dir, stem, ns, "failed") == 1);
        eng_close(db); /* a failed engine's close reports it */
        CHECK(reopen_check(dir, stem, ns, "failed, reopened") == 2);
    }
    printf("  contract: a durable swap is 0 whatever MANIFEST's publish says; an input a merge unlisted is refused "
           "with nothing renamed; a failed engine renames nothing and the open rolls the swap forward\n");
}

/* ------------------------------------------------------------ a logged swap (R8) */

static int handed;
static void count_handed(eng *db) { (void)db, handed++; }

/* A swap whose objects a committed COLD marker names (ecapply_swap_pre's `logged`, forced by ecswap_test_logged): every
 * refusal after the hook fails the engine and hands no object to r7-gc (ecswap_handed_hook) nor back to the caller
 * (TSDB_ERR_IO, never NOMEM); the temporaries stay for the next open. A logged swap with no failure swaps. */
static void logged_case(void)
{
    static const struct {
        const char *name;
        int fail;  /* ECSWAP_FAIL_* */
        bool drop; /* a merge unlists a source inside the hook: the input check refuses */
        size_t intents;
    } C[] = {{"logged-atnomem", ECSWAP_FAIL_AT_NOMEM, false, 0},
             {"logged-intentnomem", ECSWAP_FAIL_INTENT_NOMEM, false, 0},
             {"logged-intentio", ECSWAP_FAIL_INTENT_IO, false, 0},
             {"logged-stopped", ECSWAP_FAIL_NONE, true, 1}};
    char dir[PATH_MAX], stem[4][96];
    size_t ns = 0;
    ecswap_handed_hook = count_handed;
    atomic_store(&ecswap_test_logged, true);
    for (size_t c = 0; c < sizeof C / sizeof *C; c++) {
        fresh(C[c].name, dir, sizeof dir);
        eng *db = NULL;
        CHECK(open_eng(dir, 3600, &db) == 0);
        if (!db)
            continue;
        populate(db);
        handed = 0;
        atomic_store(&ecswap_test_fail, C[c].fail);
        hook_key = 1;
        ec_swap_hook = C[c].drop ? merged_hook : NULL;
        int rc = swap_key(db, 1, stem, &ns);
        ec_swap_hook = NULL;
        atomic_store(&ecswap_test_fail, ECSWAP_FAIL_NONE);
        if (rc != TSDB_ERR_IO || handed != 0 || eng_check(db) == 0)
            fprintf(stderr, "  (%s: swap %d, handed %d, engine %s)\n", C[c].name, rc, handed,
                    eng_check(db) ? "failed" : "running");
        CHECK(rc == TSDB_ERR_IO && eng_check(db) != 0 && handed == 0);
        CHECK(count_names(dir, "tmp_", ".ec") == ns && count_names(dir, "txn_", ".intent") == C[c].intents);
        CHECK(count_names(dir, "p1_", ".ec") == 0 && count_names(dir, "settled_", ".part") == 0); /* none renamed */
        eng_close(db); /* a failed engine's close reports it */
    }
    /* the control: a logged swap that nothing refuses */
    fresh("logged-ok", dir, sizeof dir);
    eng *db = NULL;
    CHECK(open_eng(dir, 3600, &db) == 0);
    if (db) {
        populate(db);
        handed = 0;
        CHECK(swap_key(db, 1, stem, &ns) == 0 && eng_check(db) == 0 && handed == 0);
        CHECK(key_state(dir, stem, ns, "logged-ok") == 2);
        CHECK(eng_close(db) == 0);
    }
    atomic_store(&ecswap_test_logged, false);
    ecswap_handed_hook = NULL;
    printf("  logged (R8): a swap whose objects a committed COLD names, refused out of memory, at its intent or by "
           "its input check: the engine fails, no object handed to r7-gc, the temporaries kept\n");
}

/* ------------------------------------------------------------ recovery by hand */

static void put(const char *dir, const char *name, const char *text)
{
    char p[PATH_MAX + 256];
    snprintf(p, sizeof p, "%s/%s", dir, name);
    FILE *f = fopen(p, "w");
    CHECK(f != NULL);
    if (f) {
        fputs(text, f);
        fclose(f);
    }
}

static void put_intent(const char *dir, const char *body, bool seal)
{
    char buf[4096];
    size_t len = (size_t)snprintf(buf, sizeof buf, "%s", body);
    if (seal)
        snprintf(buf + len, sizeof buf - len, "end %016" PRIx64 "\n", xxh64(buf, len, 0));
    else
        snprintf(buf + len, sizeof buf - len, "end 0000000000000000\n");
    put(dir, "txn_7_1.intent", buf);
}

static const char *const IV4 = "tsdb-intent 4 2\nL tmp_7_1.ec p500_1_1_0.ec\nL tmp_7_2.ec p500_2_2_0.ec\n"
                               "R p500_1_1_0.part settled_p500_1_1_0.part\nR p500_2_2_0.part settled_p500_2_2_0.part\n";

/* plants files (a list of names, "+" for present) in a fresh database, the intent, and opens it */
static int plant_open(const char *what, const char *const *names, const char *body, bool seal, char *dir, size_t cap)
{
    fresh(what, dir, cap);
    eng *db = NULL;
    CHECK(open_eng(dir, 3600, &db) == 0);
    if (db)
        CHECK(eng_close(db) == 0);
    for (size_t i = 0; names[i]; i++)
        put(dir, names[i], "x");
    put_intent(dir, body, seal);
    db = NULL;
    int rc = open_eng(dir, 3600, &db);
    if (db)
        eng_close(db);
    CHECK(!exists(dir, "txn_7_1.intent") || rc != 0);
    return rc;
}

static void recover_case(void)
{
    char dir[PATH_MAX];
    uint64_t c0 = atomic_load(&ec_calls[EC_CALL_SWAP_RECOVER]);
    /* torn: the parts stay (they are not real parts: the open fails after recovery, at load_parts) */
    plant_open("rc-torn", (const char *const[]){"tmp_7_1.ec", "tmp_7_2.ec", NULL}, IV4, false, dir, sizeof dir);
    CHECK(!exists(dir, "txn_7_1.intent") && !exists(dir, "tmp_7_1.ec") && !exists(dir, "tmp_7_2.ec"));
    CHECK(atomic_load(&ec_calls[EC_CALL_SWAP_RECOVER]) == c0 + 1);
    /* complete, nothing renamed: forward */
    plant_open("rc-fwd", (const char *const[]){"tmp_7_1.ec", "tmp_7_2.ec", "p500_1_1_0.part", "p500_2_2_0.part", NULL},
               IV4, true, dir, sizeof dir);
    CHECK(exists(dir, "p500_1_1_0.ec") && exists(dir, "p500_2_2_0.ec") && exists(dir, "settled_p500_1_1_0.part") &&
          exists(dir, "settled_p500_2_2_0.part") && !exists(dir, "p500_1_1_0.part") && !exists(dir, "tmp_7_1.ec"));
    /* complete, half renamed, one .ec retired by a later drop: forward, the retired one counts as done */
    plant_open("rc-half", (const char *const[]){"retired_p500_1_1_0.ec", "tmp_7_2.ec", "settled_p500_1_1_0.part",
                                                "p500_2_2_0.part", NULL},
               IV4, true, dir, sizeof dir);
    CHECK(exists(dir, "retired_p500_1_1_0.ec") && !exists(dir, "p500_1_1_0.ec") && exists(dir, "p500_2_2_0.ec") &&
          exists(dir, "settled_p500_2_2_0.part") && !exists(dir, "p500_2_2_0.part"));
    /* a source retired although an .ec name is in place: no flow leaves this (a drop before the check renames
     * nothing, one after it retires the .ec parts): damage, the open fails, nothing renamed, the intent stays */
    int rc = plant_open("rc-dropped", (const char *const[]){"p500_1_1_0.ec", "tmp_7_2.ec", "retired_p500_1_1_0.part",
                                                            "retired_p500_2_2_0.part", NULL},
                        IV4, true, dir, sizeof dir);
    CHECK(rc == TSDB_ERR_CORRUPT && exists(dir, "txn_7_1.intent") && exists(dir, "p500_1_1_0.ec") &&
          exists(dir, "tmp_7_2.ec") && !exists(dir, "retired_p500_1_1_0.ec"));
    /* the swap done, its intent's unlink lost (before-unlink under a power cut), and a retired_ name of a source
     * beside its settled_ one: an older file of that name (the open's cover rule retired a merge output a crash left
     * unlisted, the merge named it again; jepsen tier300 classes settle-steps, 2026-11-20). Not damage: forward, the
     * intent goes (the fake parts then fail the open at load_parts, as in rc-released) */
    plant_open("rc-stale", (const char *const[]){"p500_1_1_0.ec", "p500_2_2_0.ec", "settled_p500_1_1_0.part",
                                                 "settled_p500_2_2_0.part", "retired_p500_1_1_0.part", NULL},
               IV4, true, dir, sizeof dir);
    CHECK(!exists(dir, "txn_7_1.intent") && exists(dir, "p500_1_1_0.ec") && exists(dir, "p500_2_2_0.ec") &&
          exists(dir, "settled_p500_1_1_0.part"));
    /* the same cut between the renames: the second source still P.part, both with an older retired_ copy: forward */
    plant_open("rc-stalehalf", (const char *const[]){"p500_1_1_0.ec", "tmp_7_2.ec", "settled_p500_1_1_0.part",
                                                     "p500_2_2_0.part", "retired_p500_1_1_0.part",
                                                     "retired_p500_2_2_0.part", NULL},
               IV4, true, dir, sizeof dir);
    CHECK(!exists(dir, "txn_7_1.intent") && exists(dir, "p500_2_2_0.ec") &&
          exists(dir, "settled_p500_2_2_0.part") && !exists(dir, "p500_2_2_0.part") && !exists(dir, "tmp_7_2.ec"));
    /* the swap done, then a drop with obsolete_part_lifetime_s = 0: the .ec parts retired and deleted at their last
     * reference, the intent's unlink lost to a power cut: forward (nothing left to do), the open succeeds */
    rc = plant_open("rc-dropafter", (const char *const[]){"settled_p500_1_1_0.part", "settled_p500_2_2_0.part", NULL},
                    IV4, true, dir, sizeof dir);
    CHECK(rc == 0 && !exists(dir, "txn_7_1.intent") && exists(dir, "settled_p500_1_1_0.part") &&
          exists(dir, "settled_p500_2_2_0.part") && !exists(dir, "p500_1_1_0.part"));
    /* the swap done, the settled_ sources released (r7-keep), the intent's unlink lost: the live .ec parts stay */
    plant_open("rc-released", (const char *const[]){"p500_1_1_0.ec", "p500_2_2_0.ec", NULL}, IV4, true, dir,
               sizeof dir);
    CHECK(exists(dir, "p500_1_1_0.ec") && exists(dir, "p500_2_2_0.ec") && !exists(dir, "retired_p500_1_1_0.ec") &&
          !exists(dir, "retired_p500_2_2_0.ec") && !exists(dir, "txn_7_1.intent"));
    /* a source gone (lifetime 0) */
    plant_open("rc-gone", (const char *const[]){"tmp_7_1.ec", "tmp_7_2.ec", "p500_2_2_0.part", NULL}, IV4, true, dir,
               sizeof dir);
    CHECK(!exists(dir, "tmp_7_1.ec") && !exists(dir, "p500_1_1_0.ec") && !exists(dir, "p500_2_2_0.ec") &&
          exists(dir, "p500_2_2_0.part"));
    /* a refused swap's cleanup cut before its intent went: the temporaries gone, nothing renamed: back */
    plant_open("rc-refused", (const char *const[]){"tmp_7_2.ec", "p500_1_1_0.part", "p500_2_2_0.part", NULL}, IV4,
               true, dir, sizeof dir);
    CHECK(exists(dir, "p500_1_1_0.part") && exists(dir, "p500_2_2_0.part") && !exists(dir, "p500_2_2_0.ec") &&
          !exists(dir, "settled_p500_2_2_0.part"));
    /* an .ec under none of its names after a rename: damage, the open fails and the intent stays */
    rc = plant_open("rc-lost", (const char *const[]){"p500_1_1_0.ec", "settled_p500_1_1_0.part", "p500_2_2_0.part",
                                                         NULL},
                        IV4, true, dir, sizeof dir);
    CHECK(rc == TSDB_ERR_CORRUPT && exists(dir, "txn_7_1.intent") && exists(dir, "p500_2_2_0.part"));
    /* crafted (the checksum holds): a rename outside the names a swap writes, two keys, a mismatched pair, a short
     * count: damage, the open fails, nothing renamed */
    static const char *const bad[] = {
        "tsdb-intent 4 1\nL tmp_7_1.ec MANIFEST\nR p500_1_1_0.part settled_p500_1_1_0.part\n",
        "tsdb-intent 4 1\nL tmp_7_1.ec p500_1_1_0.ec\nR p500_1_1_0.part SERIES\n",
        "tsdb-intent 4 1\nL tmp_7_1.ec p500_1_1_0.ec\nR p500_2_2_0.part settled_p500_2_2_0.part\n",
        "tsdb-intent 4 2\nL tmp_7_1.ec p500_1_1_0.ec\nL tmp_7_2.ec p501_2_2_0.ec\n"
        "R p500_1_1_0.part settled_p500_1_1_0.part\nR p501_2_2_0.part settled_p501_2_2_0.part\n",
        "tsdb-intent 4 2\nL tmp_7_1.ec p500_1_1_0.ec\nR p500_1_1_0.part settled_p500_1_1_0.part\n",
    };
    for (size_t i = 0; i < sizeof bad / sizeof *bad; i++) {
        rc = plant_open("rc-bad", (const char *const[]){"tmp_7_1.ec", "p500_1_1_0.part", "p500_2_2_0.part",
                                                        "p501_2_2_0.part", NULL},
                        bad[i], true, dir, sizeof dir);
        CHECK(rc == TSDB_ERR_CORRUPT && exists(dir, "txn_7_1.intent"));
        CHECK(exists(dir, "p500_1_1_0.part") && exists(dir, "p500_2_2_0.part") && !exists(dir, "p500_1_1_0.ec") &&
              !exists(dir, "MANIFEST.x") && count_names(dir, "settled_", ".part") == 0);
    }
    printf("  recovery: torn, forward, forward past a retired .ec, back when nothing was renamed (a source gone, a "
           "refused swap's cleanup), forward after a drop or a release that followed the swap or past an older "
           "retired_ copy of a source, damage (a retired "
           "source after a rename, a lost .ec, a crafted intent) fails the open\n");
}

/* ------------------------------------------------------------ children: crash points and power loss */

/* a database with the rows, made once and copied */
static char BASE[PATH_MAX];
static char BSTEM[4][96];
static size_t BN;

static void make_base(void)
{
    fresh("base", BASE, sizeof BASE);
    eng *db = NULL;
    CHECK(open_eng(BASE, 3600, &db) == 0);
    if (!db)
        return;
    populate(db);
    part *in[4];
    BN = key_parts(db, 1, in, 4);
    for (size_t i = 0; i < BN; i++) {
        snprintf(BSTEM[i], 96, "%.*s", (int)(strlen(in[i]->name) - 5), in[i]->name);
        part_unref(in[i]);
    }
    CHECK(eng_close(db) == 0);
}

static void copy_base(const char *what, char *dir, size_t cap)
{
    snprintf(dir, cap, "%s/%s", S, what);
    sh("rm -rf '%s'; cp -a '%s' '%s'", dir, BASE, dir);
}

/* child: open, swap key 1, close; 0 when the swap returned 0 */
static int child_swap(const char *dir)
{
    eng *db = NULL;
    if (open_eng(dir, 3600, &db) != 0)
        return 3;
    int rc = swap_key(db, 1, NULL, NULL);
    int crc = eng_close(db);
    return rc == 0 && crc == 0 ? 0 : 4;
}

static int child_open(const char *dir)
{
    eng *db = NULL;
    int rc = open_eng(dir, 3600, &db);
    if (db)
        eng_close(db);
    return rc == 0 || rc == TSDB_ERR_CORRUPT ? 0 : 3;
}

static int run_child(const char *kind, const char *dir, const char *crash_at, int warmup, int seed)
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
        if (warmup >= 0) {
            char asan[512], w[16], sd[16];
            /* the power fails at exit (CRASHFS_AT_EXIT) with the shim's kept sectors still allocated, which a leak
             * check at exit reports as a failure (exit 1, as test_chunk's children) */
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
        }
        int null = open("/dev/null", O_WRONLY);
        if (null >= 0) {
            dup2(null, 1);
            dup2(null, 2);
        }
        execl(SELF, "test_ecswap", "--child", kind, dir, "--mutant", mutant_name(ecswap_mutant), (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

static void crashpt_case(void)
{
    char dir[PATH_MAX], at[64];
    int fwd = 0, steps = 0;
    const char *points[] = {"after-intent", "before-unlink"};
    for (int p = 0; p < 2; p++) {
        copy_base("cp", dir, sizeof dir);
        CHECK(run_child("swap", dir, points[p], -1, 0) == 99);
        CHECK(count_names(dir, "txn_", ".intent") == 1);
        int st = reopen_check(dir, BSTEM, BN, points[p]);
        fwd += st == 2;
        CHECK(st == 2); /* a complete intent with every source present goes forward */
    }
    /* after each rename but the last: 2 * BN - 1 places */
    for (size_t k = 1; k < 2 * BN; k++) {
        copy_base("cp", dir, sizeof dir);
        snprintf(at, sizeof at, "between-renames:%zu", k);
        CHECK(run_child("swap", dir, at, -1, 0) == 99);
        int st = reopen_check(dir, BSTEM, BN, at);
        CHECK(st == 2);
        fwd += st == 2;
        /* power lost at each step of that recovery (torn at random, several seeds), then an open: the recovery's
         * renames must be durable before intents_recover unlinks the intent */
        if (access(SHIM, R_OK) != 0)
            continue;
        bool done = false;
        for (int w = 0; w < 400 && !done; w++) {
            for (int rs = 0; rs < (quick ? 3 : 6); rs++) {
                copy_base("cp", dir, sizeof dir);
                CHECK(run_child("swap", dir, at, -1, 0) == 99);
                int code = run_child("open", dir, NULL, w, w * 13 + rs + 11);
                CHECK(code == 99 || code == 0);
                if (code != 99 && code != 0) {
                    w = 1 << 20;
                    break;
                }
                done = code == 0;
                steps += code == 99;
                CHECK(reopen_check(dir, BSTEM, BN, "power loss in recovery") == 2);
            }
        }
        CHECK(done);
    }
    /* an armed point with a typo ends the process with status 2 */
    copy_base("cp", dir, sizeof dir);
    CHECK(run_child("swap", dir, "between-renamez", -1, 0) == 2);
    copy_base("cp", dir, sizeof dir);
    CHECK(run_child("swap", dir, "after-intent:0", -1, 0) == 2);
    printf("  crash points: after-intent, before-unlink and between each pair of the %zu renames: an open rolls the "
           "swap forward (%d), also when power fails at each of the steps of that open (%d power losses)\n", 2 * BN, fwd,
           steps);
}

static void power_case(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  power loss: skipped (no %s)\n", SHIM);
        return;
    }
    char dir[PATH_MAX];
    int crashes = 0, forward = 0, back = 0, seeds = quick ? 8 : 16;
    bool finished = false;
    for (int k = 0; k < 2000 && !finished; k++) {
        for (int sd = 0; sd < seeds; sd++) {
            copy_base("pl", dir, sizeof dir);
            int code = run_child("swap", dir, NULL, k, k * 7 + sd + 1);
            CHECK(code == 99 || code == 0);
            if (code != 99 && code != 0) {
                fprintf(stderr, "  (power loss at step %d: the child said %d)\n", k, code);
                k = 1 << 20;
                break;
            }
            crashes += code == 99;
            int st = reopen_check(dir, BSTEM, BN, "power loss");
            if (code == 0) {
                finished = true;
                CHECK(st == 2); /* acknowledged: durable */
            }
            forward += st == 2;
            back += st == 1;
        }
    }
    CHECK(finished && crashes > 20 && forward > 0 && back > 0);
    printf("  power lost at each of %d steps of an open, a swap and a close (%d seeds each): every reopen finds the "
           "key all .part (%d) or all .ec with its settled_ sources (%d); an acknowledged swap is durable\n",
           crashes / seeds, seeds, back, forward);
}

/* ------------------------------------------------------------ 16 threads across swaps */

enum { TKEYS = 24, READERS = 16 };
typedef struct {
    eng *db;
    uint32_t s;
    acc want, wantday[TKEYS];
    latest_row lwant;
    atomic_bool stop;
    atomic_long reads, bad;
} tctx;

static void *reader(void *arg)
{
    tctx *c = arg;
    unsigned r = (unsigned)(uintptr_t)&r;
    while (!atomic_load(&c->stop)) {
        r = r * 1103515245u + 12345u;
        int d = (int)(r >> 8) % (TKEYS + 2);
        acc a = {0, 0};
        bool ok;
        if (d < TKEYS) {
            ok = eng_query_raw_ex(c->db, c->s, d * DAY, (d + 1) * DAY, false, 0, acc_fn, &a) == 0 &&
                 a.n == c->wantday[d].n && a.sum == c->wantday[d].sum;
        } else if (d == TKEYS) {
            ok = eng_query_raw_ex(c->db, c->s, INT64_MIN, INT64_MAX, false, 0, acc_fn, &a) == 0 && a.n == c->want.n &&
                 a.sum == c->want.sum;
        } else {
            latest_row l;
            ok = eng_latest(c->db, &c->s, 1, NULL, INT64_MAX, false, &l) == 0 && l.status == c->lwant.status &&
                 l.ts == c->lwant.ts && l.slot == c->lwant.slot;
        }
        atomic_fetch_add(&c->reads, 1);
        if (!ok)
            atomic_fetch_add(&c->bad, 1);
    }
    return NULL;
}

static void threads_case(void)
{
    char dir[PATH_MAX];
    fresh("threads", dir, sizeof dir);
    static tctx c;
    memset(&c, 0, sizeof c);
    CHECK(open_eng(dir, 3600, &c.db) == 0);
    if (!c.db)
        return;
    CHECK(eng_series(c.db, "s", TSDB_FLOAT32, &c.s) == 0);
    for (int d = 0; d < TKEYS; d++)
        write_rows(c.db, c.s, d * DAY, 2000, 3000);
    CHECK(eng_flush(c.db) == 0);
    for (int d = 0; d < TKEYS; d++)
        write_rows(c.db, c.s, d * DAY + 1000, 2000, 3000);
    CHECK(eng_flush(c.db) == 0);
    c.want = read_range(c.db, c.s, INT64_MIN, INT64_MAX);
    for (int d = 0; d < TKEYS; d++)
        c.wantday[d] = read_range(c.db, c.s, d * DAY, (d + 1) * DAY);
    CHECK(eng_latest(c.db, &c.s, 1, NULL, INT64_MAX, false, &c.lwant) == 0);
    pthread_t th[READERS];
    for (int i = 0; i < READERS; i++)
        pthread_create(&th[i], NULL, reader, &c);
    /* The readers must run across every swap, at any speed: a fixed total (1000 reads) failed on a slow CI runner under
     * ASan with 719. Each swap waits for every reader to have read a few times since the one before, and starts while
     * they go on reading. */
    int swapped = 0, starved = 0;
    for (int d = TKEYS - 1; d >= 0; d--) { /* the newest key first: the latest row's part is swapped under readers */
        long r0 = atomic_load(&c.reads);
        for (int w = 0; w < 10000 && atomic_load(&c.reads) - r0 < 4 * READERS; w++)
            usleep(1000);
        starved += atomic_load(&c.reads) - r0 < 4 * READERS;
        swapped += swap_key(c.db, d, NULL, NULL) == 0;
    }
    long r1 = atomic_load(&c.reads);
    for (int w = 0; w < 10000 && atomic_load(&c.reads) - r1 < 4 * READERS; w++)
        usleep(1000);
    atomic_store(&c.stop, true);
    for (int i = 0; i < READERS; i++)
        pthread_join(th[i], NULL);
    CHECK(swapped == TKEYS);
    CHECK(atomic_load(&c.bad) == 0);
    CHECK(starved == 0 && atomic_load(&c.reads) - r1 >= 4 * READERS);
    pthread_mutex_lock(&c.db->lock);
    for (size_t i = 0; i < c.db->nparts; i++)
        CHECK(c.db->parts[i]->ec != NULL);
    pthread_mutex_unlock(&c.db->lock);
    CHECK(eng_close(c.db) == 0);
    printf("  %d readers (%ld queries and latest lookups) while %d keys were swapped one by one: every answer the "
           "same (%ld wrong)\n", READERS, (long)atomic_load(&c.reads), swapped, (long)atomic_load(&c.bad));
}

/* ------------------------------------------------------------ R10 re-encode swap */

/* Creates a test-marked .ec output part from contiguous sources in[0..nin) (merges into a pending .part, copies to
 * tmp_*.ec, opens with test_mark, unlinks the pending .part). */
static part *make_reencode_ec(eng *db, part **in, size_t nin)
{
    part *m = NULL;
    CHECK(merge_parts_reencode(db, in, nin, NULL, 0, false, &m) == 0 && m != NULL);
    if (!m)
        return NULL;
    char tmp[64];
    snprintf(tmp, sizeof tmp, "tmp_%d_%u.ec", (int)getpid(), 950000 + atomic_fetch_add(&ec_counter, 1));
    int fd = openat(db->dirfd, tmp, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    CHECK(fd >= 0);
    if (fd < 0) {
        unlinkat(db->dirfd, m->pending, 0);
        part_unref(m);
        return NULL;
    }
    bool ok = write(fd, m->map, m->map_len) == (ssize_t)m->map_len && fsync(fd) == 0;
    CHECK(ok);
    close(fd);
    part *e = NULL;
    CHECK(part_open_as(db->dirfd, tmp, m->name, false, false, &e) == 0 && e != NULL);
    unlinkat(db->dirfd, m->pending, 0);
    part_unref(m);
    if (!e)
        return NULL;
    char base[96];
    snprintf(base, sizeof base, "%s", e->name);
    snprintf(e->name, sizeof e->name, "%.*s.ec", (int)(strlen(base) - 5), base);
    snprintf(e->pending, sizeof e->pending, "%s", tmp);
    struct ecpart *ep = calloc(1, sizeof *ep);
    CHECK(ep != NULL);
    ep->test_mark = true;
    ep->tier = db->tier;
    ep->volume = db->tier_vol;
    atomic_store(&e->ec, ep);
    return e;
}

static void reencode_basic_case(void)
{
    char dir[PATH_MAX];
    fresh("re-basic", dir, sizeof dir);
    eng *db = NULL;
    CHECK(open_eng(dir, 3600, &db) == 0);
    if (!db)
        return;
    populate(db);
    uint32_t s = 0;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    /* Add a 3rd part on key 1 (overlapping timestamps so LWW matters) */
    write_rows(db, s, DAY + 500, ROWS, 2000);
    CHECK(eng_flush(db) == 0);
    acc before = read_range(db, s, INT64_MIN, INT64_MAX);
    latest_row l0;
    CHECK(eng_latest(db, &s, 1, NULL, INT64_MAX, false, &l0) == 0);

    /* Swap first 2 parts of key 1 to .ec, leaving the 3rd as a young .part */
    part *p[8];
    size_t n = key_parts(db, 1, p, 8);
    CHECK(n == 3);
    if (n == 3) {
        part *ec[2] = {make_ec(db, p[0]), make_ec(db, p[1])};
        CHECK(ec_swap_commit(db, p, ec, 2) == 0);
        part_unref(ec[0]);
        part_unref(ec[1]);
    }
    for (size_t i = 0; i < n; i++)
        part_unref(p[i]);

    /* Refusals: no .ec input (key 0 is all .part), wrong block range, two keys, read-only */
    part *k0[4], *k1[4];
    size_t n0 = key_parts(db, 0, k0, 4), n1 = key_parts(db, 1, k1, 4);
    CHECK(n0 == 2 && n1 == 3);
    if (n0 == 2 && n1 == 3) {
        part *out0 = make_reencode_ec(db, k0, 2);
        CHECK(ec_swap_reencode(db, k0, 2, out0) == TSDB_ERR_INVALID); /* no .ec source */
        unlinkat(db->dirfd, out0->pending, 0);
        part_unref(out0);

        part *out1 = make_reencode_ec(db, k1, 3);
        CHECK(ec_swap_reencode(db, k1, 2, out1) == TSDB_ERR_INVALID); /* block range mismatch */
        part *mix[2] = {k1[0], k0[0]};
        CHECK(ec_swap_reencode(db, mix, 2, out1) == TSDB_ERR_INVALID); /* two keys */
        /* Merging source -> TSDB_ERR_STOPPED, nothing renamed, tmp gone */
        pthread_mutex_lock(&db->lock);
        k1[2]->merging = true;
        pthread_mutex_unlock(&db->lock);
        CHECK(ec_swap_reencode(db, k1, 3, out1) == TSDB_ERR_STOPPED);
        pthread_mutex_lock(&db->lock);
        k1[2]->merging = false;
        pthread_mutex_unlock(&db->lock);
        CHECK(count_names(dir, "tmp_", ".ec") == 0 && count_names(dir, "txn_", ".intent") == 0);
        part_unref(out1);

        /* Real re-encode swap of 2 .ec + 1 .part -> 1 .ec */
        CHECK(stats_settle(db, 60000) == 0);
        uint64_t g0 = lgen_of(db);
        part *out = make_reencode_ec(db, k1, 3);
        char s0[128], s1[128], s2[128], fout[128];
        snprintf(s0, sizeof s0, "retired_%s", k1[0]->name);
        snprintf(s1, sizeof s1, "retired_%s", k1[1]->name);
        snprintf(s2, sizeof s2, "settled_%s", k1[2]->name);
        snprintf(fout, sizeof fout, "%s", out->name);
        CHECK(ec_swap_reencode(db, k1, 3, out) == 0);
        CHECK(!db->lt || lgen_of(db) > g0);
        CHECK(exists(dir, fout) && exists(dir, s0) && exists(dir, s1) && exists(dir, s2));
        CHECK(!exists(dir, k1[0]->name) && !exists(dir, k1[1]->name) && !exists(dir, k1[2]->name));
        CHECK(count_names(dir, "tmp_", ".ec") == 0 && count_names(dir, "txn_", ".intent") == 0);
        part *after_p[4];
        CHECK(key_parts(db, 1, after_p, 4) == 1 && after_p[0] == out);
        part_unref(after_p[0]);
        CHECK(stats_settle(db, 60000) == 0);
        struct estats *es = atomic_load(&db->stats);
        if (es) {
            pthread_mutex_lock(&db->lock);
            pthread_mutex_lock(&es->mu);
            for (size_t i = 0; i < db->nparts; i++)
                CHECK(rec_find(es, db->parts[i]) != NULL);
            pthread_mutex_unlock(&es->mu);
            pthread_mutex_unlock(&db->lock);
        }
        acc after = read_range(db, s, INT64_MIN, INT64_MAX);
        CHECK(after.n == before.n && after.sum == before.sum);
        latest_row l1;
        CHECK(eng_latest(db, &s, 1, NULL, INT64_MAX, false, &l1) == 0 && l1.status == l0.status && l1.ts == l0.ts &&
              l1.slot == l0.slot);
        part_unref(out);

        /* Empty .part output (ng == 0): delete all rows of key 1 and re-encode .ec + tombstone .part -> empty .part */
        CHECK(eng_delete_range(db, s, DAY, 2 * DAY) == 0);
        part *dk[4];
        size_t nd = key_parts(db, 1, dk, 4);
        CHECK(nd == 2);
        if (nd == 2) {
            part *empty = NULL;
            CHECK(merge_parts_reencode(db, dk, nd, NULL, 0, false, &empty) == 0 && empty && empty->ng == 0);
            if (empty) {
                CHECK(ec_swap_reencode(db, dk, nd, empty) == 0);
                CHECK(read_range(db, s, DAY, 2 * DAY).n == 0);
                part *chk[4];
                CHECK(key_parts(db, 1, chk, 4) == 1 && chk[0] == empty && !atomic_load(&chk[0]->ec));
                part_unref(chk[0]);
                part_unref(empty);
            }
            for (size_t i = 0; i < nd; i++)
                part_unref(dk[i]);
        }
    }
    for (size_t i = 0; i < n0; i++)
        part_unref(k0[i]);
    for (size_t i = 0; i < n1; i++)
        part_unref(k1[i]);
    CHECK(eng_close(db) == 0);
    printf("  re-encode basic: 2 .ec + 1 .part swapped for 1 .ec (retired_ for .ec, settled_ for .part), empty .part "
           "output, lt_merge_marks, stats, refusals\n");
}

static void reencode_drop_case(void)
{
    char dir[PATH_MAX];
    fresh("re-drop", dir, sizeof dir);
    eng *db = NULL;
    CHECK(open_eng(dir, 3600, &db) == 0);
    if (!db)
        return;
    populate(db);
    uint32_t s = 0;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    acc rest = read_range(db, s, DAY, INT64_MAX);
    part *k0[4];
    size_t n0 = key_parts(db, 0, k0, 4);
    CHECK(n0 == 2);
    if (n0 == 2) {
        part *e0 = make_ec(db, k0[0]);
        CHECK(ec_swap_commit(db, &k0[0], &e0, 1) == 0);
        part_unref(e0);
    }
    for (size_t i = 0; i < n0; i++)
        part_unref(k0[i]);

    n0 = key_parts(db, 0, k0, 4);
    CHECK(n0 == 2);
    if (n0 == 2) {
        part *out = make_reencode_ec(db, k0, 2);
        ec_swap_hook = drop_hook;
        int rc = ec_swap_reencode(db, k0, 2, out);
        ec_swap_hook = NULL;
        CHECK(rc == TSDB_ERR_STOPPED && eng_check(db) == 0);
        CHECK(read_range(db, s, INT64_MIN, DAY).n == 0);
        acc r2 = read_range(db, s, INT64_MIN, INT64_MAX);
        CHECK(r2.n == rest.n && r2.sum == rest.sum);
        CHECK(count_names(dir, "tmp_", ".ec") == 0 && count_names(dir, "txn_", ".intent") == 0);
        CHECK(!exists(dir, out->name));
        part_unref(out);
    }
    for (size_t i = 0; i < n0; i++)
        part_unref(k0[i]);
    CHECK(eng_close(db) == 0);
    printf("  re-encode drop: a drop inside the hook refuses with STOPPED, nothing renamed, no intent left\n");
}

static const char *const RE4 = "tsdb-intent 4 1 3\nL tmp_7_1.ec p500_1_3_2.ec\n"
                               "R p500_1_1_1.ec retired_p500_1_1_1.ec\n"
                               "R p500_2_2_1.ec retired_p500_2_2_1.ec\n"
                               "R p500_3_3_0.part settled_p500_3_3_0.part\n";

static void reencode_recover_case(void)
{
    char dir[PATH_MAX];
    /* torn: rolled back (tmp cleaned by load_parts when no fake .ec aborts load_parts first) */
    plant_open("re-torn", (const char *const[]){"tmp_7_1.ec", NULL}, RE4, false, dir, sizeof dir);
    CHECK(!exists(dir, "txn_7_1.intent") && !exists(dir, "tmp_7_1.ec") && !exists(dir, "p500_1_3_2.ec"));
    /* torn with a source present: intent unlinked, nothing renamed, source untouched */
    plant_open("re-tornsrc", (const char *const[]){"tmp_7_1.ec", "p500_1_1_1.ec", NULL}, RE4, false, dir, sizeof dir);
    CHECK(!exists(dir, "txn_7_1.intent") && exists(dir, "p500_1_1_1.ec") && !exists(dir, "p500_1_3_2.ec") &&
          !exists(dir, "retired_p500_1_1_1.ec"));
    /* complete, nothing renamed, all present: forward */
    plant_open("re-fwd", (const char *const[]){"tmp_7_1.ec", "p500_1_1_1.ec", "p500_2_2_1.ec", "p500_3_3_0.part", NULL},
               RE4, true, dir, sizeof dir);
    CHECK(!exists(dir, "txn_7_1.intent") && exists(dir, "p500_1_3_2.ec") && exists(dir, "retired_p500_1_1_1.ec") &&
          exists(dir, "retired_p500_2_2_1.ec") && exists(dir, "settled_p500_3_3_0.part") &&
          !exists(dir, "p500_1_1_1.ec") && !exists(dir, "p500_2_2_1.ec") && !exists(dir, "p500_3_3_0.part"));
    /* complete, L renamed, crash between R renames: forward */
    plant_open("re-half",
               (const char *const[]){"p500_1_3_2.ec", "retired_p500_1_1_1.ec", "p500_2_2_1.ec", "p500_3_3_0.part", NULL},
               RE4, true, dir, sizeof dir);
    CHECK(!exists(dir, "txn_7_1.intent") && exists(dir, "p500_1_3_2.ec") && exists(dir, "retired_p500_2_2_1.ec") &&
          exists(dir, "settled_p500_3_3_0.part") && !exists(dir, "p500_2_2_1.ec") && !exists(dir, "p500_3_3_0.part"));
    /* complete, L NOT renamed, .ec source retired (by a drop before the swap): MUST roll back, never resurrect */
    plant_open("re-dropbefore",
               (const char *const[]){"tmp_7_1.ec", "retired_p500_1_1_1.ec", "retired_p500_2_2_1.ec",
                                     "retired_p500_3_3_0.part", NULL},
               RE4, true, dir, sizeof dir);
    CHECK(!exists(dir, "txn_7_1.intent") && !exists(dir, "tmp_7_1.ec") && !exists(dir, "p500_1_3_2.ec") &&
          exists(dir, "retired_p500_1_1_1.ec") && exists(dir, "retired_p500_3_3_0.part"));
    /* complete, L retired by a drop after swap: forward (no-op) */
    plant_open("re-dropafter",
               (const char *const[]){"retired_p500_1_3_2.ec", "retired_p500_1_1_1.ec", "retired_p500_2_2_1.ec",
                                     "settled_p500_3_3_0.part", NULL},
               RE4, true, dir, sizeof dir);
    CHECK(!exists(dir, "txn_7_1.intent") && !exists(dir, "p500_1_3_2.ec") && exists(dir, "retired_p500_1_3_2.ec"));
    /* damage: L renamed, .part source under retired_ instead of settled_ */
    int rc = plant_open("re-partret",
                        (const char *const[]){"p500_1_3_2.ec", "retired_p500_1_1_1.ec", "retired_p500_2_2_1.ec",
                                              "retired_p500_3_3_0.part", NULL},
                        RE4, true, dir, sizeof dir);
    CHECK(rc == TSDB_ERR_CORRUPT && exists(dir, "txn_7_1.intent"));
    /* damage: .part source settled_, L missing while an .ec source is still at src */
    rc = plant_open("re-lostl",
                    (const char *const[]){"p500_1_1_1.ec", "retired_p500_2_2_1.ec", "settled_p500_3_3_0.part", NULL},
                    RE4, true, dir, sizeof dir);
    CHECK(rc == TSDB_ERR_CORRUPT && exists(dir, "txn_7_1.intent"));
    /* crafted re-encode intents: no .ec source, wrong block range, two keys, fin == src, wrong dst prefix */
    static const char *const rbad[] = {
        "tsdb-intent 4 1 2\nL tmp_7_1.ec p500_1_2_1.ec\n"
        "R p500_1_1_0.part settled_p500_1_1_0.part\nR p500_2_2_0.part settled_p500_2_2_0.part\n",
        "tsdb-intent 4 1 2\nL tmp_7_1.ec p500_1_5_2.ec\n"
        "R p500_1_1_1.ec retired_p500_1_1_1.ec\nR p500_2_2_1.ec retired_p500_2_2_1.ec\n",
        "tsdb-intent 4 1 2\nL tmp_7_1.ec p500_1_2_2.ec\n"
        "R p500_1_1_1.ec retired_p500_1_1_1.ec\nR p501_2_2_1.ec retired_p501_2_2_1.ec\n",
        "tsdb-intent 4 1 1\nL tmp_7_1.ec p500_1_1_1.ec\nR p500_1_1_1.ec retired_p500_1_1_1.ec\n",
        "tsdb-intent 4 1 1\nL tmp_7_1.ec p500_1_1_2.ec\nR p500_1_1_1.ec settled_p500_1_1_1.ec\n",
        "tsdb-intent 4 2 2\nL tmp_7_1.ec p500_1_2_2.ec\nL tmp_7_2.ec p500_1_2_3.ec\n"
        "R p500_1_1_1.ec retired_p500_1_1_1.ec\nR p500_2_2_1.ec retired_p500_2_2_1.ec\n",
    };
    for (size_t i = 0; i < sizeof rbad / sizeof *rbad; i++) {
        rc = plant_open("re-bad", (const char *const[]){"tmp_7_1.ec", "p500_1_1_1.ec", "p500_2_2_1.ec", NULL}, rbad[i],
                        true, dir, sizeof dir);
        CHECK(rc == TSDB_ERR_CORRUPT && exists(dir, "txn_7_1.intent"));
    }
    printf("  re-encode recovery: torn, forward, half-renamed, rollback over dropped .ec sources, drop after swap, "
           "damage and crafted intents refused\n");
}
#endif /* TSDB_EC */

int main(int argc, char **argv)
{
#ifndef TSDB_EC
    (void)argc;
    (void)argv;
    printf("test_ecswap: skipped (built without TSDB_EC)\n");
    return 0;
#else
    const char *scratch = getenv("TMPDIR") ? getenv("TMPDIR") : "/tmp", *child = NULL, *cdir = NULL;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--quick"))
            quick = true;
        else if (!strcmp(argv[i], "--mutant") && i + 1 < argc) {
            const char *m = argv[++i];
            ecswap_mutant = -1;
            for (size_t k = 0; k < sizeof MUTANTS / sizeof *MUTANTS; k++)
                if (!strcmp(m, MUTANTS[k].name))
                    ecswap_mutant = MUTANTS[k].m;
            if (ecswap_mutant < 0) {
                fprintf(stderr, "unknown mutant %s\n", m);
                return 2;
            }
        } else if (!strcmp(argv[i], "--child") && i + 2 < argc) {
            child = argv[++i];
            cdir = argv[++i];
        } else
            scratch = argv[i];
    }
    if (child)
        return !strcmp(child, "swap") ? child_swap(cdir) : child_open(cdir);
    char tmpl[PATH_MAX];
    snprintf(tmpl, sizeof tmpl, "%s/test-ecswap.XXXXXX", scratch);
    if (!mkdtemp(tmpl)) {
        perror("mkdtemp");
        return 1;
    }
    snprintf(S, sizeof S, "%s", tmpl);
    if (!realpath(argv[0], SELF))
        snprintf(SELF, sizeof SELF, "%s", argv[0]);
    char *slash = strrchr(SELF, '/');
    snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", slash ? (int)(slash - SELF) : 1, slash ? SELF : ".");
    printf("test_ecswap%s%s\n", ecswap_mutant ? " --mutant " : "", ecswap_mutant ? mutant_name(ecswap_mutant) : "");
    basic_case();
    contract_case();
    logged_case();
    drop_case(3600);
    drop_case(0);
    recover_case();
    reencode_basic_case();
    reencode_drop_case();
    reencode_recover_case();
    make_base();
    crashpt_case();
    power_case();
    threads_case();
    if (failures == 0)
        sh("rm -rf '%s'", S);
    printf(failures ? "test_ecswap: %d failures\n" : "test_ecswap: ok\n", failures);
    return failures != 0;
#endif
}
