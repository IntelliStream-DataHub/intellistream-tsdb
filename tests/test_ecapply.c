/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * A replica's apply of a committed MK_COLD (src/ecapply.c, r8h; design-cluster-data.md 2.3 step 6, the R8 plan's D5,
 * D6, D8) and the leader's side of it (the swap's RSTATE record, ecapply_cold_decide). The leader is test_eccold's: an
 * engine with a jr_ops of the test's own on a requires=cluster,replica,ec volume with a tier of three directory stores;
 * every byte it hands to the transport and every file it ships is recorded. A replica is a second directory with the
 * same series and the leader's META (its vuuid), opened with eng_open_replica on the leader's stores once the leader is
 * closed (the stores are flock'ed); the recorded log is appended, the files staged and the log applied.
 *
 * Cases: apply (the .ec part and Y listed, Y above it, the replica's own parts of P retired as settled_, the rows of P
 * unchanged, an overwrite in Y still winning; rows of another day written at the cut and after the marker, in the
 * marker's segment, survive a crash after the apply: the replay); above (a part of P with ppos above Z: TSDB_ERR_CORRUPT,
 * nothing renamed, DIVERGED, still after a restart); dropped (P dropped by the replica between the staging and the
 * intent: nothing listed, nothing retired); leader (a leader killed after its swap, or after the swap's intent and
 * before its renames, reopened as a replica: the COLD replayed is not applied again, every oid listed once, the .ec's
 * PPOS record there; or two COLDs and a clean close: the first's `done` is not lost under the second's `doing`); badft
 * (a COLD whose F is not below its own segment, through the seam ecapply_test_ft: TSDB_ERR_CORRUPT, nothing listed);
 * leaderpower (crashfs: the leader's power fails once its COLD marker committed, before the swap; reopened as a replica
 * it carries the COLD out from the stage_ links jr_cold made durable); power (crashfs power loss at every step of the
 * apply: the old parts or the new set, then the apply ends with the new set).
 *
 * Planted bugs (--mutant NAME, ecapply.h): ycold ecabovey pposignored v5nodirsync coldnoflushthrough leaderswapnors
 * pposafterswap;
 * the Makefile requires each to fail this test. An alarm ends a hung run with status 3.
 */
#define _GNU_SOURCE
#include "internal.h"
#include "chunk.h"
#include "coldops.h"
#include "tier.h"
#include "settle.h"
#include "ecpart.h"
#include "ecskip.h"
#include "ecgc.h"
#include "eccold.h"
#include "ecapply.h"
#include "feed.h"
#include "marker.h"
#include "ecswap.h"
#include "replica.h"
#include "ppos.h"

#include <dirent.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <sys/wait.h>

static int failures __attribute__((unused));
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
    printf("test_ecapply: skipped (built without TSDB_EC)\n");
    return 0;
}
#else

struct eng;
static void rx_bytes(void *ctx, struct eng *db, uint64_t seq, uint64_t off, const void *buf, size_t len,
                     uint64_t first, uint64_t last);
static int rx_ship(void *ctx, struct eng *db, const char *name, const char *mname, uint64_t size);

#define DAY_MS 86400000LL
#define T0 1700000000000LL
#define DAY0 ((T0 / DAY_MS) * DAY_MS)
#define KEY (DAY0 / DAY_MS)                 /* partition_classes 0: the key is the day */
#define CLK (DAY0 + 10 * DAY_MS)            /* the engine's clock: P ended 9 days ago */
#define GRACE_S (30 * 86400LL)              /* the settle thread never picks a key by itself */
#define QMAX 8192

static char S[PATH_MAX / 4];
static const char *mutant = "none";

static int64_t clock_fn(void *arg)
{
    (void)arg;
    return CLK;
}

static int64_t __attribute__((unused)) now_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (int64_t)t.tv_sec * 1000 + t.tv_nsec / 1000000;
}

static void msleep(int ms) { usleep((useconds_t)ms * 1000); }

/* ------------------------------------------------------------ the cold ops: the directory ops, DOWN and a stripe */

static _Atomic bool DOWN;          /* all_up says no */
static _Atomic uint64_t SV = 7;    /* the copy's stripe version */
static cold_ops WRAP;

static bool w_all_up(void *c) { return !atomic_load(&DOWN) && coldops_dir.all_up(c); }
static uint64_t w_sv(void *c)
{
    (void)c;
    return atomic_load(&SV);
}
static int w_make(void *arg, tier *t, uint32_t volume, const uint8_t vuuid[16], void **ctx)
{
    (void)arg;
    return coldops_dir_make(NULL, t, volume, vuuid, ctx);
}

/* ------------------------------------------------------------ the leader's transport (no replica) */

typedef struct xport {
    _Atomic bool manual; /* only the test moves the commit point */
    pthread_mutex_t mu;
    uint64_t adm_fno[1024];
    uint32_t adm_span[1024];
    size_t nadm;
    uint8_t *cold; /* the payload of the last COLD marker written, and its position */
    size_t ncold;
    uint64_t cold_fno;
} xport;

static int xp_ship(void *ctx, eng *db, const char *name, const char *mname, uint64_t size)
{
    (void)ctx, (void)mname;
    struct stat st;
    return fstatat(db->dirfd, name, &st, 0) == 0 && (uint64_t)st.st_size == size ? 0 : TSDB_ERR_IO;
}
static int xp_wait(void *ctx, eng *db, uint64_t last) { return (void)ctx, (void)db, (void)last, 0; }
static void xp_seg_done(void *ctx, eng *db, uint64_t seq) { (void)ctx, (void)db, (void)seq; }
static int xp_admit(void *ctx, eng *db, uint64_t fno, uint32_t span)
{
    xport *x = ctx;
    (void)db;
    pthread_mutex_lock(&x->mu);
    if (x->nadm < 1024)
        x->adm_fno[x->nadm] = fno, x->adm_span[x->nadm++] = span;
    pthread_mutex_unlock(&x->mu);
    return 0;
}
static void xp_bytes(void *ctx, eng *db, uint64_t seq, uint64_t off, const void *buf, size_t len, uint64_t first,
                     uint64_t last)
{
    (void)first;
    xport *x = ctx;
    for (size_t at = 0; at < len;) { /* the records written: keep the COLD marker's bytes for check_marker */
        wal_rec wr;
        size_t sz = wal_record((const uint8_t *)buf + at, len - at, seq, off + at, &wr);
        if (sz == 0)
            break;
        if (wr.kind == WAL_KIND_MARKER && wr.mtype == MK_COLD) {
            uint8_t *c = malloc(wr.n);
            if (c)
                memcpy(c, wr.payload, wr.n);
            pthread_mutex_lock(&x->mu);
            free(x->cold);
            x->cold = c, x->ncold = wr.n, x->cold_fno = wr.fno;
            pthread_mutex_unlock(&x->mu);
        }
        at += sz;
    }
    if (!atomic_load(&x->manual))
        eng_leader_commit(db, last);
}
static void xp_synced(void *ctx, eng *db, uint64_t last) { (void)ctx, (void)db, (void)last; }

static uint64_t ld_last(eng *db)
{
    pthread_mutex_lock(&db->buf_lock);
    uint64_t v = db->fno_next - 1;
    pthread_mutex_unlock(&db->buf_lock);
    return v;
}

/* whether the marker at pos was admitted as one record of span 1 */
static bool __attribute__((unused)) admitted_marker(xport *x, uint64_t pos)
{
    bool ok = false;
    pthread_mutex_lock(&x->mu);
    for (size_t i = 0; i < x->nadm; i++)
        ok |= x->adm_fno[i] == pos && x->adm_span[i] == 1;
    pthread_mutex_unlock(&x->mu);
    return ok;
}

/* ------------------------------------------------------------ a leader with a tier */

typedef struct lt {
    char root[PATH_MAX / 2], dir[PATH_MAX], st[3][PATH_MAX];
    tier *t;
    eng *db;
    xport x;
    struct jr_ops ops;
    tier_settings set;
    uint32_t s[8]; /* series s0 .. s7, local ids 0 .. 7: each in its own fence bucket */
} lt;

/* the paths, the tier's spec and settings of the leader in root (fresh, and a child that reopens it) */
static void lt_paths(lt *L, const char *root)
{
    memset(L, 0, sizeof *L);
    snprintf(L->root, sizeof L->root, "%s", root);
    snprintf(L->dir, sizeof L->dir, "%s/db", L->root);
    for (int i = 0; i < 3; i++)
        snprintf(L->st[i], sizeof L->st[i], "%s/st%d", L->root, i);
    char spec[4 * PATH_MAX];
    snprintf(spec, sizeof spec, "k=2,m=1,dirs=%s:%s:%s,grace_s=%lld", L->st[0], L->st[1], L->st[2],
             (long long)GRACE_S);
    setenv("TSDB_TEST_TIER", spec, 1);
    setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);
    pthread_mutex_init(&L->x.mu, NULL);
    L->set = (tier_settings){.k = 2, .m = 1, .grace_s = GRACE_S, .keep_s = 3600, .coarse_s = 86400, .cache_mb = 16,
                             .force_s = 3600, .wait_max_s = 1};
}

static void fresh(const char *what, lt *L)
{
    static int seq;
    char root[PATH_MAX / 2];
    snprintf(root, sizeof root, "%s/ecapply-%d-%s-%d", S, (int)getpid(), what, seq++);
    lt_paths(L, root);
    char cmd[PATH_MAX];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", L->root);
    CHECK(system(cmd) == 0);
    CHECK(mkdir(L->root, 0755) == 0);
    CHECK(mkdir(L->dir, 0755) == 0);
    for (int i = 0; i < 3; i++)
        CHECK(mkdir(L->st[i], 0755) == 0);
}

static void opts(tsdb_options *o)
{
    tsdb_options_init(o);
    o->merge_threads = 0;
    o->partition_classes = 0;
    o->create_volumes = 0;
    o->wal_sync_ms = 0;
    o->wal_max_batch_rows = 16; /* more rows than this go straight to parts: a COMMIT marker */
    o->clock_ms = clock_fn;
    o->obsolete_part_lifetime_s = 0;
}

/* first: a plain open makes the volume, then META says requires=cluster,replica,ec (a replicated cluster ec volume) */
static void make_cluster(lt *L)
{
    tsdb_options o;
    opts(&o);
    eng *db = NULL;
    CHECK(eng_open(L->dir, &o, &db) == 0);
    if (db) {
        for (int i = 0; i < 8; i++) {
            char nm[16];
            snprintf(nm, sizeof nm, "s%d", i);
            CHECK(eng_series(db, nm, TSDB_FLOAT32, &L->s[i]) == 0 && L->s[i] == (uint32_t)i);
        }
        CHECK(eng_close(db) == 0);
    }
    int fd = open(L->dir, O_RDONLY | O_DIRECTORY);
    CHECK(fd >= 0 && meta_require(fd, true, META_REQ_CLUSTER | META_REQ_REPLICA | META_REQ_EC) == 0);
    if (fd >= 0)
        close(fd);
}

static int lopen(lt *L)
{
    tsdb_options o;
    opts(&o);
    int rc = tier_open(L->dir, &o, &L->t);
    if (rc)
        return rc;
    tier_attach a = {.ops = &WRAP, .make = w_make, .settings = &L->set};
    tier_want_attach(L->t, 0, &a);
    rc = eng_open(L->dir, &o, &L->db);
    tier_want(NULL, 0);
    if (rc) {
        tier_put(L->t);
        L->t = NULL;
        L->db = NULL;
        return rc;
    }
    L->ops = (struct jr_ops){.ctx = &L->x, .ship = rx_ship, .wait = xp_wait, .seg_done = xp_seg_done,
                             .admit = xp_admit, .bytes = rx_bytes, .synced = xp_synced};
    rc = eng_leader_attach(L->db, &L->ops);
    if (rc == 0) /* the replicas hold what an earlier open wrote: the commit point at the log's end */
        eng_leader_commit(L->db, ld_last(L->db));
    return rc;
}

static void lclose(lt *L)
{
    free(L->x.cold);
    L->x.cold = NULL;
    if (L->db)
        eng_close(L->db); /* a deposed leader's close may say so: what the case checks it checked before */
    if (L->t)
        tier_put(L->t);
    L->db = NULL;
    L->t = NULL;
    char cmd[PATH_MAX];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", L->root);
    if (failures == 0 && system(cmd) != 0)
        CHECK(0);
}

static bool start(const char *what, lt *L)
{
    fresh(what, L);
    make_cluster(L);
    int rc = lopen(L);
    CHECK(rc == 0);
    if (rc)
        fprintf(stderr, "%s: the leader did not open (%d)\n", what, rc);
    return rc == 0;
}

/* n rows of series s from DAY0 + day*DAY_MS + t0 s, value base + i (n > 16: straight to parts) */
static int rows(eng *db, uint32_t s, int day, int t0, int n, float base)
{
    int64_t *ts = malloc((size_t)n * sizeof *ts);
    float *v = malloc((size_t)n * sizeof *v);
    for (int i = 0; i < n; i++) {
        ts[i] = DAY0 + day * DAY_MS + 1000LL * (t0 + i);
        v[i] = base + (float)i;
    }
    int rc = eng_insert_series(db, TSDB_FLOAT32, s, ts, v, (size_t)n);
    free(ts);
    free(v);
    return rc;
}

typedef struct qctx {
    int64_t ts[QMAX];
    float v[QMAX];
    size_t n;
} qctx;
static int qfn(void *c, const int64_t *ts, const void *vals, size_t n)
{
    qctx *q = c;
    const float *sl = vals;
    for (size_t i = 0; i < n && q->n < QMAX; i++, q->n++)
        q->ts[q->n] = ts[i], q->v[q->n] = sl[i];
    return 0;
}
static qctx *query(eng *db, uint32_t s, int day)
{
    qctx *q = calloc(1, sizeof *q);
    if (eng_query_raw_ex(db, s, DAY0 + day * DAY_MS, DAY0 + (day + 1) * DAY_MS - 1, false, 0, qfn, q) != 0)
        q->n = SIZE_MAX;
    return q;
}
/* series s on day `day` holds exactly n rows from t0 s, value base + i */
static bool holds(eng *db, uint32_t s, int day, int t0, int n, float base)
{
    qctx *q = query(db, s, day);
    bool ok = q->n == (size_t)n;
    for (int i = 0; ok && i < n; i++)
        ok = q->ts[i] == DAY0 + day * DAY_MS + 1000LL * (t0 + i) && q->v[i] == base + (float)i;
    if (!ok)
        fprintf(stderr, "series %u day %d: %zu rows, wanted %d from %d s at %g\n", s, day, q->n, n, t0, base);
    free(q);
    return ok;
}

/* I: a level-1 part of s0 in P (insert-written parts are never settled in a cluster, D18: optimize rewrites it) */
static void make_i(lt *L)
{
    CHECK(rows(L->db, L->s[0], 0, 0, 500, 1.0f) == 0);
    CHECK(eng_optimize(L->db) == 0);
}

static size_t parts_of(eng *db, int64_t key, part **out, size_t cap, bool ec)
{
    size_t n = 0;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++) {
        part *p = db->parts[i];
        if (p->partition == key && part_is_ec(p) == ec && n < cap) {
            part_ref(p);
            out[n++] = p;
        }
    }
    pthread_mutex_unlock(&db->lock);
    return n;
}
static void unref_all(part **p, size_t n)
{
    for (size_t i = 0; i < n; i++)
        part_unref(p[i]);
}

static int settle_key(eng *db, int64_t key)
{
    part *sel[64];
    size_t n = parts_of(db, key, sel, 64, false);
    int rc = eng_settle(db, key, sel, n, NULL);
    unref_all(sel, n);
    return rc;
}

static int chunks(const lt *L)
{
    int n = 0;
    for (int i = 0; i < 3; i++) {
        char p[PATH_MAX + 32];
        snprintf(p, sizeof p, "%s/s0", L->st[i]);
        DIR *dd = opendir(p);
        struct dirent *de;
        while (dd && (de = readdir(dd)) != NULL)
            n += de->d_name[0] != '.' && strcmp(de->d_name, "EPOCH") && strncmp(de->d_name, "tmp_", 4);
        if (dd)
            closedir(dd);
    }
    return n;
}
static bool __attribute__((unused)) chunks_go(lt *L, int want)
{
    for (int i = 0; i < 200; i++) {
        graveyard_purge(L->db, false);
        ecgc_tier_work(L->db->tier);
        if (chunks(L) == want)
            return true;
        msleep(50);
    }
    fprintf(stderr, "chunk files: %d, wanted %d\n", chunks(L), want);
    return false;
}

/* ------------------------------------------------------------ the recorded log and shipped files */

typedef struct lrec {
    uint64_t seq, off;
    uint8_t *b;
    size_t n;
} lrec;
typedef struct srec {
    char m[128];
    uint8_t *b;
    size_t n;
} srec;
static pthread_mutex_t RMU = PTHREAD_MUTEX_INITIALIZER;
static lrec *LOG;
static size_t NLOG, CLOG;
static srec *SHIP;
static size_t NSHIP, CSHIP;
static uint64_t LAST; /* the last position handed over */

static void rec_reset(void)
{
    for (size_t i = 0; i < NLOG; i++)
        free(LOG[i].b);
    for (size_t i = 0; i < NSHIP; i++)
        free(SHIP[i].b);
    NLOG = NSHIP = 0;
    LAST = 0;
}

static void rx_bytes(void *ctx, eng *db, uint64_t seq, uint64_t off, const void *buf, size_t len, uint64_t first,
                     uint64_t last)
{
    pthread_mutex_lock(&RMU);
    if (NLOG == CLOG) {
        CLOG = CLOG ? 2 * CLOG : 64;
        LOG = realloc(LOG, CLOG * sizeof *LOG);
    }
    uint8_t *b = malloc(len ? len : 1);
    memcpy(b, buf, len);
    LOG[NLOG++] = (lrec){.seq = seq, .off = off, .b = b, .n = len};
    if (last > LAST)
        LAST = last;
    pthread_mutex_unlock(&RMU);
    xp_bytes(ctx, db, seq, off, buf, len, first, last);
}

static int rx_ship(void *ctx, eng *db, const char *name, const char *mname, uint64_t size)
{
    int fd = openat(db->dirfd, name, O_RDONLY | O_CLOEXEC);
    uint8_t *b = malloc(size ? size : 1);
    ssize_t got = fd >= 0 ? pread(fd, b, size, 0) : -1;
    if (fd >= 0)
        close(fd);
    CHECK(got == (ssize_t)size);
    pthread_mutex_lock(&RMU);
    if (NSHIP == CSHIP) {
        CSHIP = CSHIP ? 2 * CSHIP : 16;
        SHIP = realloc(SHIP, CSHIP * sizeof *SHIP);
    }
    SHIP[NSHIP] = (srec){.b = b, .n = size};
    snprintf(SHIP[NSHIP].m, sizeof SHIP[NSHIP].m, "%s", mname);
    NSHIP++;
    pthread_mutex_unlock(&RMU);
    return xp_ship(ctx, db, name, mname, size);
}

/* ------------------------------------------------------------ a replica */

typedef struct rt {
    char dir[PATH_MAX];
    tier *t;
    eng *db;
} rt;

/* the leader closed (not removed: its stores and directory stay) */
static void lstop(lt *L)
{
    if (L->db)
        eng_close(L->db);
    if (L->t)
        tier_put(L->t);
    L->db = NULL;
    L->t = NULL;
    free(L->x.cold);
    L->x.cold = NULL;
}

static bool copy_file(const char *from, const char *to)
{
    char cmd[5 * PATH_MAX];
    snprintf(cmd, sizeof cmd, "cp '%s' '%s.tmp' && mv '%s.tmp' '%s'", from, to, to, to);
    return system(cmd) == 0;
}

/* the replica's directory: the leader's series (same names, same order: same local ids) and the leader's META (its
 * requires= and the vuuid its tier attach wrote): call with the leader open once */
static void make_replica(lt *L, rt *R)
{
    memset(R, 0, sizeof *R);
    snprintf(R->dir, sizeof R->dir, "%s/rep", L->root);
    CHECK(mkdir(R->dir, 0755) == 0);
    tsdb_options o;
    opts(&o);
    eng *db = NULL;
    CHECK(eng_open(R->dir, &o, &db) == 0);
    if (db) {
        for (int i = 0; i < 8; i++) {
            char nm[16];
            uint32_t id = 0;
            snprintf(nm, sizeof nm, "s%d", i);
            CHECK(eng_series(db, nm, TSDB_FLOAT32, &id) == 0 && id == (uint32_t)i);
        }
        CHECK(eng_close(db) == 0);
    }
    char a[PATH_MAX + 8], b[PATH_MAX + 8];
    snprintf(a, sizeof a, "%s/META", L->dir);
    snprintf(b, sizeof b, "%s/META", R->dir);
    CHECK(copy_file(a, b));
}

/* the replica in dir on the leader's stores (the leader must be closed) */
static int ropen(lt *L, rt *R, uint64_t floor)
{
    tsdb_options o;
    opts(&o);
    int rc = tier_open(R->dir, &o, &R->t);
    if (rc)
        return rc;
    tier_attach a = {.ops = &WRAP, .make = w_make, .settings = &L->set};
    tier_want_attach(R->t, 0, &a);
    rc = eng_open_replica(R->dir, &o, floor, &R->db);
    tier_want(NULL, 0);
    if (rc) {
        fprintf(stderr, "the replica %s did not open (%d)\n", R->dir, rc);
        tier_put(R->t);
        R->t = NULL;
        R->db = NULL;
    }
    return rc;
}

static void rstop(rt *R)
{
    if (R->db)
        eng_close(R->db);
    if (R->t)
        tier_put(R->t);
    R->db = NULL;
    R->t = NULL;
}

/* the recorded log appended, the shipped files staged, synced, applied through upto */
static int feed(rt *R, uint64_t upto)
{
    int rc = 0;
    for (size_t i = 0; rc == 0 && i < NLOG; i++)
        if ((rc = replica_append(R->db, LOG[i].seq, LOG[i].off, LOG[i].b, LOG[i].n)) != 0)
            fprintf(stderr, "replica_append %zu (seq %" PRIu64 " off %" PRIu64 "): %d\n", i, LOG[i].seq, LOG[i].off,
                    rc);
    for (size_t i = 0; rc == 0 && i < NSHIP; i++)
        if ((rc = replica_stage(R->db, SHIP[i].m, SHIP[i].b, SHIP[i].n)) != 0)
            fprintf(stderr, "replica_stage %s: %d\n", SHIP[i].m, rc);
    if (rc == 0 && (rc = replica_sync(R->db)) != 0)
        fprintf(stderr, "replica_sync: %d\n", rc);
    if (rc == 0)
        rc = replica_apply(R->db, upto);
    return rc;
}

/* names in dir that begin with prefix */
static int count_names(const char *dir, const char *prefix)
{
    int n = 0;
    DIR *d = opendir(dir);
    struct dirent *de;
    while (d && (de = readdir(d)) != NULL)
        n += strncmp(de->d_name, prefix, strlen(prefix)) == 0;
    if (d)
        closedir(d);
    return n;
}
static int settled_of_key(const char *dir)
{
    char pre[64];
    snprintf(pre, sizeof pre, "settled_p%" PRId64 "_", (int64_t)KEY);
    return count_names(dir, pre);
}

/* ------------------------------------------------------------ the leader's workload */

static _Atomic bool W_CUT; /* the settle's cut step writes rows of s1 into day 1 (the marker's segment) */
static _Atomic bool MARKER_EXIT; /* leaderpower's child: exit() (crashfs: the power fails) once the marker committed */
static void step_hook(eng *db, int step)
{
    if (step == ECCOLD_STEP_CUT && atomic_load(&W_CUT))
        CHECK(rows(db, 1, 1, 100, 3, 300.0f) == 0);
    if (step == ECCOLD_STEP_MARKER && atomic_load(&MARKER_EXIT)) {
        fflush(NULL);
        exit(99); /* not _exit: crashfs's destructor drops what was not synced */
    }
}

/* P: I (500 rows of s0 from 0 s, value 1 + i, a level-1 part), then in the memtable two of them overwritten (900, 901)
 * and 5 rows from 1000 s (100 + i): the cut flushes them into Y. Day 1: s1's 3 rows from 0 s (200 + i) before the cut
 * (flushed with it, a part of their own), with `full` 3 from 100 s written at the cut (300 + i) and 3 from 200 s after
 * the marker (400 + i): those two in the marker's segment. */
static void workload(lt *L, eccold_cut *c, bool full)
{
    make_i(L);
    CHECK(rows(L->db, L->s[0], 0, 0, 2, 900.0f) == 0);
    CHECK(rows(L->db, L->s[0], 0, 1000, 5, 100.0f) == 0);
    CHECK(rows(L->db, L->s[1], 1, 0, 3, 200.0f) == 0);
    atomic_store(&W_CUT, full);
    CHECK(settle_key(L->db, KEY) == 0);
    atomic_store(&W_CUT, false);
    eccold_test_last(c);
    CHECK(c->rc == 0 && c->nobjs == 1 && c->ny == 1);
    if (full)
        CHECK(rows(L->db, L->s[1], 1, 200, 3, 400.0f) == 0);
}

/* the answers the workload's rows must give */
static bool p_holds(eng *db)
{
    qctx *q = query(db, 0, 0);
    bool ok = q->n == 505;
    for (size_t i = 0; ok && i < 505; i++) {
        int64_t t = i < 500 ? (int64_t)i : 1000 + (int64_t)(i - 500);
        float v = i < 2 ? 900.0f + (float)i : i < 500 ? 1.0f + (float)i : 100.0f + (float)(i - 500);
        ok = q->ts[i] == DAY0 + 1000LL * t && q->v[i] == v;
        if (!ok)
            fprintf(stderr, "P row %zu: %" PRId64 " %g, wanted %" PRId64 " %g\n", i, q->ts[i], q->v[i],
                    (int64_t)(DAY0 + 1000LL * t), v);
    }
    if (q->n != 505)
        fprintf(stderr, "P: %zu rows, wanted 505\n", q->n);
    free(q);
    return ok;
}
static bool d1_holds(eng *db, bool full)
{
    qctx *q = query(db, 1, 1);
    size_t want = full ? 9 : 3;
    bool ok = q->n == want;
    for (size_t i = 0; ok && i < want; i++) {
        int64_t t = (int64_t)(i / 3) * 100 + (int64_t)(i % 3);
        float v = 200.0f + 100.0f * (float)(i / 3) + (float)(i % 3);
        ok = q->ts[i] == DAY0 + DAY_MS + 1000LL * t && q->v[i] == v;
    }
    if (!ok)
        fprintf(stderr, "day 1: %zu rows, wanted %zu\n", q->n, want);
    free(q);
    return ok;
}

/* what the replica must list after the apply: the .ec part, Y above it, nothing else of P */
static void check_listed(eng *db, const char *what)
{
    part *ec[8], *rest[8];
    size_t ne = parts_of(db, KEY, ec, 8, true), nr = parts_of(db, KEY, rest, 8, false);
    CHECK(ne == 1 && nr == 1);
    if (ne != 1 || nr != 1)
        fprintf(stderr, "%s: P lists %zu .ec parts and %zu others, wanted 1 and 1 (Y)\n", what, ne, nr);
    if (ne == 1 && nr == 1) {
        CHECK(ec[0]->max_block < rest[0]->min_block); /* Y above the .ec part */
        uint64_t pp = 0;
        CHECK(ppos_of(db, ec[0]->name, &pp) == 0 && pp != PPOS_UNKNOWN);
    }
    unref_all(ec, ne);
    unref_all(rest, nr);
}

/* ------------------------------------------------------------ apply: the replica, a crash after the apply, the replay */

static int child_status(pid_t pid)
{
    int ws = 0;
    if (pid <= 0 || waitpid(pid, &ws, 0) != pid)
        return -1;
    return WIFEXITED(ws) ? WEXITSTATUS(ws) : 128 + (WIFSIGNALED(ws) ? WTERMSIG(ws) : 0);
}

static void test_apply(void)
{
    lt L;
    rt R;
    rec_reset();
    if (!start("apply", &L))
        return;
    make_replica(&L, &R);
    eccold_cut c;
    workload(&L, &c, true);
    CHECK(p_holds(L.db) && d1_holds(L.db, true));
    uint64_t last = LAST;
    lstop(&L);
    fflush(NULL);
    pid_t pid = fork(); /* no engine open here: the child opens the replica, applies and dies without a close */
    if (pid == 0) {
        failures = 0;
        if (ropen(&L, &R, 0) != 0)
            _exit(2);
        int rc = feed(&R, last);
        CHECK(rc == 0);
        if (rc)
            fprintf(stderr, "apply: the replica's apply: %d\n", rc);
        CHECK(replica_applied_end(R.db) == last);
        check_listed(R.db, "apply");
        CHECK(settled_of_key(R.dir) >= 1);
        CHECK(p_holds(R.db) && d1_holds(R.db, true));
        CHECK(eng_check(R.db) == 0);
        fflush(NULL);
        _exit(failures ? 1 : 0);
    }
    int st = child_status(pid);
    CHECK(st == 0);
    if (st != 0)
        fprintf(stderr, "apply: the child's status %d\n", st);
    /* the replay after the crash: the rows of the marker's segment come back from the WAL */
    if (ropen(&L, &R, last) == 0) {
        CHECK(replica_apply(R.db, last) == 0);
        check_listed(R.db, "apply after the crash");
        CHECK(p_holds(R.db) && d1_holds(R.db, true));
        CHECK(settled_of_key(R.dir) >= 1);
        CHECK(count_names(R.dir, "txn_") == 0);
        rstop(&R);
    } else {
        CHECK(0);
    }
    /* and after a clean close */
    if (ropen(&L, &R, last) == 0) {
        check_listed(R.db, "apply after a close");
        CHECK(p_holds(R.db) && d1_holds(R.db, true));
        rstop(&R);
    } else {
        CHECK(0);
    }
    lclose(&L);
    printf("  apply: .ec and Y listed (Y above), own parts settled_, rows of P and of the marker's segment after a "
           "crash\n");
}

/* ------------------------------------------------------------ above: a part of P with ppos above Z */

static void test_above(void)
{
    lt L;
    rt R;
    rec_reset();
    if (!start("above", &L))
        return;
    make_replica(&L, &R);
    eccold_cut c;
    workload(&L, &c, false);
    uint64_t last = LAST, cpos = L.x.cold_fno;
    lstop(&L);
    CHECK(cpos > 0 && cpos == c.pos);
    if (ropen(&L, &R, 0) != 0) {
        CHECK(0);
        lclose(&L);
        return;
    }
    CHECK(feed(&R, cpos - 1) == 0);
    /* planted: one of the replica's own parts of P says it came after Z (a gate that failed) */
    part *own[8];
    size_t no = parts_of(R.db, KEY, own, 8, false);
    CHECK(no >= 1);
    if (no >= 1) {
        const char *nm[1] = {own[0]->name};
        pthread_mutex_lock(&R.db->publish_lock);
        CHECK(ppos_record(R.db, nm, 1, c.z + 1) == 0);
        pthread_mutex_unlock(&R.db->publish_lock);
    }
    unref_all(own, no);
    int rc = replica_apply(R.db, last);
    CHECK(rc != 0);
    CHECK(atomic_load(&R.db->diverged));
    part *ec[8], *rest[8];
    size_t ne = parts_of(R.db, KEY, ec, 8, true);
    CHECK(ne == 0);
    unref_all(ec, ne);
    size_t nr = parts_of(R.db, KEY, rest, 8, false);
    CHECK(nr >= no); /* nothing retired (the flush through F may have added one) */
    unref_all(rest, nr);
    CHECK(settled_of_key(R.dir) == 0);
    CHECK(count_names(R.dir, "txn_") == 0);
    rstop(&R);
    /* a restart: still DIVERGED */
    if (ropen(&L, &R, cpos - 1) == 0) {
        CHECK(atomic_load(&R.db->diverged));
        ne = parts_of(R.db, KEY, ec, 8, true);
        CHECK(ne == 0);
        unref_all(ec, ne);
        rstop(&R);
    } else {
        CHECK(0);
    }
    lclose(&L);
    printf("  above: a part of P above Z: refused, nothing renamed, DIVERGED also after a restart\n");
}

/* ------------------------------------------------------------ dropped: P dropped between the staging and the intent */

static int by_block(const void *a, const void *b)
{
    const part *x = *(part *const *)a, *y = *(part *const *)b;
    return x->min_block < y->min_block ? -1 : x->min_block > y->min_block;
}
static void drop_hook(eng *db, int step)
{
    if (step != ECAPPLY_STEP_STAGED)
        return;
    lt_sec s;
    lt_begin(db, &s);
    pthread_mutex_lock(&db->lock);
    part *ps[16];
    size_t n = 0;
    for (size_t i = 0; i < db->nparts && n < 16; i++)
        if (db->parts[i]->partition == KEY)
            ps[n++] = db->parts[i];
    qsort(ps, n, sizeof *ps, by_block);
    char intent[64] = "";
    size_t done = 0;
    int rc = drop_retire_parts_locked(db, ps, n, intent, &done, false);
    pthread_mutex_unlock(&db->lock);
    lt_end(db, &s);
    CHECK(n >= 1 && rc == 0 && done == n);
    if (rc == 0) {
        CHECK(eng_sync_dir(db, "test_ecapply's drop") == 0);
        if (intent[0])
            drop_intent_done(db, intent);
    }
}
static void test_dropped(void)
{
    lt L;
    rt R;
    rec_reset();
    if (!start("dropped", &L))
        return;
    make_replica(&L, &R);
    eccold_cut c;
    workload(&L, &c, false);
    uint64_t last = LAST;
    lstop(&L);
    if (ropen(&L, &R, 0) != 0) {
        CHECK(0);
        lclose(&L);
        return;
    }
    atomic_store(&ecapply_test_hook, drop_hook);
    CHECK(feed(&R, last) == 0);
    atomic_store(&ecapply_test_hook, NULL);
    part *ps[8];
    size_t ne = parts_of(R.db, KEY, ps, 8, true);
    CHECK(ne == 0);
    unref_all(ps, ne);
    size_t nr = parts_of(R.db, KEY, ps, 8, false);
    CHECK(nr == 0);
    unref_all(ps, nr);
    CHECK(settled_of_key(R.dir) == 0);
    CHECK(count_names(R.dir, "tmp_") == 0);
    CHECK(count_names(R.dir, "txn_") == 0);
    CHECK(replica_applied_end(R.db) == last);
    CHECK(eng_check(R.db) == 0);
    rstop(&R);
    lclose(&L);
    printf("  dropped: P dropped by the replica before the intent: nothing listed, nothing retired\n");
}

/* ------------------------------------------------------------ leader: its own COLD replayed after a crash */

static void swap_exit(eng *db) { (void)db, _exit(99); }

static void test_leader(void)
{
    static const char *const nm[] = {"leader-done", "leader-intent", "leader-two"};
    for (int at = 0; at < 3; at++) {
        lt L;
        rec_reset();
        if (!start(nm[at], &L))
            return;
        lstop(&L);
        fflush(NULL);
        pid_t pid = fork();
        if (pid == 0) {
            failures = 0;
            if (lopen(&L) != 0)
                _exit(2);
            eccold_cut c;
            if (at == 2) /* a second key, settled after P: its COLD's `doing` must not stand in for P's `done` */
                CHECK(rows(L.db, L.s[1], 1, 0, 300, 5.0f) == 0);
            make_i(&L);
            if (at == 1)
                ec_swap_hook = swap_exit;
            CHECK(settle_key(L.db, KEY) == 0);
            eccold_test_last(&c);
            CHECK(c.rc == 0 && c.nobjs == 1);
            if (at == 2) {
                CHECK(settle_key(L.db, KEY + 1) == 0);
                eccold_test_last(&c);
                CHECK(c.rc == 0 && c.nobjs == 1);
            }
            fflush(NULL);
            _exit(failures ? 1 : 0);
        }
        int st = child_status(pid);
        CHECK(st == (at == 1 ? 99 : 0));
        rt R = {0};
        snprintf(R.dir, sizeof R.dir, "%s", L.dir);
        if (ropen(&L, &R, 0) != 0) {
            CHECK(0);
            lclose(&L);
            return;
        }
        uint64_t end = replica_log_end(R.db);
        CHECK(replica_apply(R.db, end) == 0);
        CHECK(replica_applied_end(R.db) == end);
        part *ec[8], *rest[8];
        size_t ne = parts_of(R.db, KEY, ec, 8, true), nr = parts_of(R.db, KEY, rest, 8, false);
        CHECK(ne == 1 && nr == 0);
        if (ne != 1 || nr != 0)
            fprintf(stderr, "leader-%d: P lists %zu .ec parts and %zu others\n", at, ne, nr);
        for (size_t i = 0; i < ne; i++) {
            uint64_t pp = 0;
            CHECK(ppos_of(R.db, ec[i]->name, &pp) == 0 && pp != PPOS_UNKNOWN);
        }
        unref_all(ec, ne);
        unref_all(rest, nr);
        CHECK(holds(R.db, 0, 0, 0, 500, 1.0f));
        if (at == 2) {
            ne = parts_of(R.db, KEY + 1, ec, 8, true), nr = parts_of(R.db, KEY + 1, rest, 8, false);
            CHECK(ne == 1 && nr == 0);
            if (ne != 1 || nr != 0)
                fprintf(stderr, "leader-two: day 1 lists %zu .ec parts and %zu others\n", ne, nr);
            unref_all(ec, ne);
            unref_all(rest, nr);
            CHECK(holds(R.db, 1, 1, 0, 300, 5.0f));
        }
        CHECK(eng_check(R.db) == 0);
        rstop(&R);
        lclose(&L);
    }
    printf("  leader: killed after its swap or before its renames, or two COLDs and a close, reopened as a replica: no "
           "COLD applied again\n");
}

static char SHIM[PATH_MAX]; /* libcrashfs.so beside this binary */

/* ------------------------------------------------------------ badft: a COLD whose F is not below its own segment */

static void test_badft(void)
{
    lt L;
    rt R;
    rec_reset();
    if (!start("badft", &L))
        return;
    make_replica(&L, &R);
    eccold_cut c;
    workload(&L, &c, false);
    uint64_t last = LAST;
    lstop(&L);
    if (ropen(&L, &R, 0) != 0) {
        CHECK(0);
        lclose(&L);
        return;
    }
    atomic_store(&ecapply_test_ft, UINT64_C(1) << 62); /* the marker asks for a flush through a segment past its own */
    int rc = feed(&R, last);
    atomic_store(&ecapply_test_ft, 0);
    CHECK(rc == TSDB_ERR_CORRUPT);
    if (rc != TSDB_ERR_CORRUPT)
        fprintf(stderr, "badft: the apply said %d, wanted TSDB_ERR_CORRUPT\n", rc);
    part *ec[8];
    size_t ne = parts_of(R.db, KEY, ec, 8, true);
    CHECK(ne == 0); /* nothing carried out */
    unref_all(ec, ne);
    rstop(&R);
    lclose(&L);
    printf("  badft: a COLD that asks for a flush through its own segment or past it is refused, nothing listed\n");
}

/* ------------------------------------------------------------ leaderpower: the leader loses power after its COLD */

/* the child under crashfs: the leader settles P and the power fails once the marker is committed, before the swap (the
 * staged links of the marker's files must be durable by then: the marker names them for every member) */
static int leader_power_child(const char *root)
{
    lt L;
    lt_paths(&L, root);
    for (int i = 0; i < 8; i++)
        L.s[i] = (uint32_t)i;
    if (lopen(&L) != 0)
        _exit(2);
    make_i(&L);
    atomic_store(&MARKER_EXIT, true);
    int rc = settle_key(L.db, KEY);
    fprintf(stderr, "leaderpower: the settle returned %d without reaching its marker\n", rc);
    _exit(3);
}

static void test_leaderpower(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  leaderpower: skipped (no %s)\n", SHIM);
        return;
    }
    lt L;
    rec_reset();
    if (!start("leaderpower", &L))
        return;
    lstop(&L);
    fflush(NULL);
    char *argv[] = {"test_ecapply", S, "--leader-power-child", L.root, NULL};
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512];
        snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0", getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
        setenv("ASAN_OPTIONS", asan, 1);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", L.root, 1);
        setenv("CRASHFS_AT_EXIT", "1", 1);
        execv("/proc/self/exe", argv);
        _exit(127);
    }
    int st = child_status(pid);
    CHECK(st == 99);
    if (st != 99)
        fprintf(stderr, "leaderpower: the child's status %d\n", st);
    rt R = {0};
    snprintf(R.dir, sizeof R.dir, "%s", L.dir);
    int rc = ropen(&L, &R, 0);
    CHECK(rc == 0);
    if (rc) {
        fprintf(stderr, "leaderpower: the leader's directory does not open as a replica (%d)\n", rc);
        lclose(&L);
        return;
    }
    uint64_t end = replica_log_end(R.db);
    rc = replica_apply(R.db, end);
    CHECK(rc == 0);
    if (rc)
        fprintf(stderr, "leaderpower: the apply of its own COLD: %d\n", rc);
    part *ec[8], *rest[8];
    size_t ne = parts_of(R.db, KEY, ec, 8, true), nr = parts_of(R.db, KEY, rest, 8, false);
    CHECK(ne == 1 && nr == 0); /* the marker survived and was carried out from the staged links */
    if (ne != 1 || nr != 0)
        fprintf(stderr, "leaderpower: P lists %zu .ec parts and %zu others\n", ne, nr);
    unref_all(ec, ne);
    unref_all(rest, nr);
    CHECK(holds(R.db, 0, 0, 0, 500, 1.0f));
    CHECK(eng_check(R.db) == 0);
    rstop(&R);
    lclose(&L);
    printf("  leaderpower: power lost after the COLD marker, before the swap: reopened as a replica, it carries the COLD "
           "out from its links\n");
}

/* ------------------------------------------------------------ power: crashfs power loss at every step of the apply */

static void ack_path(char *p, size_t cap, const char *root, const char *what)
{
    const char *b = strrchr(root, '/');
    snprintf(p, cap, "%s/ack.%s.%s", S, b ? b + 1 : root, what);
}
static void power_ack(const char *root, const char *what)
{
    char p[PATH_MAX + 32];
    ack_path(p, sizeof p, root, what);
    int fd = open(p, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd >= 0)
        close(fd);
}
static bool power_acked(const char *root, const char *what)
{
    char p[PATH_MAX + 32];
    ack_path(p, sizeof p, root, what);
    return access(p, F_OK) == 0;
}
static void power_unack(const char *root, const char *what)
{
    char p[PATH_MAX + 32];
    ack_path(p, sizeof p, root, what);
    unlink(p);
}
/* the child under crashfs: the replica, applied through cpos - 1 and closed, applies the COLD and the rest */
static int power_child(const char *root, uint64_t last, uint64_t cpos)
{
    lt L;
    lt_paths(&L, root);
    rt R = {0};
    snprintf(R.dir, sizeof R.dir, "%s/rep", root);
    if (ropen(&L, &R, cpos - 1) != 0)
        _exit(2);
    if (replica_apply(R.db, last) != 0)
        _exit(5);
    power_ack(root, "applied");
    eng_close(R.db);
    tier_put(R.t);
    _exit(0);
}
static int power_run(const char *root, int warmup, int seed, uint64_t last, uint64_t cpos)
{
    fflush(NULL);
    char a[24], b[24];
    snprintf(a, sizeof a, "%" PRIu64, last);
    snprintf(b, sizeof b, "%" PRIu64, cpos);
    char *argv[] = {"test_ecapply", S, "--power-child", (char *)root, a, b, NULL};
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512], w[16], sd[16];
        snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0", getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
        setenv("ASAN_OPTIONS", asan, 1);
        snprintf(w, sizeof w, "%d", warmup);
        snprintf(sd, sizeof sd, "%d", seed);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", root, 1);
        setenv("CRASHFS_PROB", "1", 1);
        setenv("CRASHFS_WARMUP", w, 1);
        setenv("CRASHFS_AT_EXIT", "1", 1);
        setenv("CRASHFS_TEAR", "1", 1);
        setenv("CRASHFS_SEED", sd, 1);
        int null = getenv("TEST_ECAPPLY_POWER_LOG") ? -1 : open("/dev/null", O_WRONLY);
        if (null >= 0)
            dup2(null, 2);
        execv("/proc/self/exe", argv);
        _exit(127);
    }
    int ws = 0;
    if (pid < 0 || waitpid(pid, &ws, 0) != pid || !WIFEXITED(ws))
        return -1;
    return WEXITSTATUS(ws);
}
/* After a power loss at step k the replica opens with P's old parts listed (no .ec, no settled_) or the new set (the
 * .ec part, Y above it, every retired part under settled_), the rows of P the same either way; the apply then ends
 * with the new set. Each step starts from a copy of one template (leader settled, replica applied through cpos - 1). */
static void test_power(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  power: skipped (no %s)\n", SHIM);
        return;
    }
    lt L;
    rt R;
    rec_reset();
    if (!start("power", &L))
        return;
    make_replica(&L, &R);
    eccold_cut c;
    workload(&L, &c, false);
    uint64_t last = LAST, cpos = L.x.cold_fno;
    lstop(&L);
    if (ropen(&L, &R, 0) != 0) {
        CHECK(0);
        lclose(&L);
        return;
    }
    CHECK(feed(&R, cpos - 1) == 0);
    rstop(&R);
    char tmpl[PATH_MAX / 2];
    snprintf(tmpl, sizeof tmpl, "%s", L.root);
    int crashes = 0, applied = 0, newset = 0, f0 = failures;
    bool finished = false;
    int k0 = getenv("TEST_ECAPPLY_POWER_K") ? atoi(getenv("TEST_ECAPPLY_POWER_K")) : 0;
    /* several seeds per step: CRASHFS_TEAR keeps a random subset of the unsynced directory operations, and the window
     * that only the intent's directory fsync closes (renames kept, the intent's name lost) is a few steps wide */
    int seeds = getenv("TEST_ECAPPLY_POWER_SEEDS") ? atoi(getenv("TEST_ECAPPLY_POWER_SEEDS")) : 6;
    for (int ks = k0 * seeds; ks < 4000 * seeds && !finished && failures == f0; ks++) {
        int k = ks / seeds;
        alarm(240);
        char work[PATH_MAX / 2 + 8], cmd[PATH_MAX * 2];
        snprintf(work, sizeof work, "%s-w", tmpl);
        snprintf(cmd, sizeof cmd, "rm -rf '%s' && cp -a '%s' '%s'", work, tmpl, work);
        CHECK(system(cmd) == 0);
        power_unack(work, "applied");
        int code = power_run(work, k, ks + 1, last, cpos);
        CHECK(code == 99 || code == 0);
        if (code != 99 && code != 0) {
            fprintf(stderr, "power: the child at step %d exited %d\n", k, code);
            break;
        }
        crashes += code == 99;
        finished = code == 0 && ks % seeds == seeds - 1;
        bool ap = power_acked(work, "applied");
        applied += ap;
        power_unack(work, "applied");
        lt W;
        lt_paths(&W, work);
        rt Q = {0};
        snprintf(Q.dir, sizeof Q.dir, "%s/rep", work);
        if (ropen(&W, &Q, cpos - 1) != 0) {
            CHECK(0);
            fprintf(stderr, "power: after a power loss at step %d the replica does not open%s\n", k,
                    ap ? " (applied)" : "");
            break;
        }
        part *ec[8], *rest[8];
        size_t ne = parts_of(Q.db, KEY, ec, 8, true), nr = parts_of(Q.db, KEY, rest, 8, false);
        int st = settled_of_key(Q.dir);
        bool old = ne == 0 && st == 0 && nr >= 1, nw = ne == 1 && nr == 1 && st >= 1;
        CHECK(old || nw);
        CHECK(!ap || nw);
        if (!(old || nw) || (ap && !nw))
            fprintf(stderr, "power: step %d: %zu .ec parts, %zu others, %d settled_%s\n", k, ne, nr, st,
                    ap ? " (applied)" : "");
        newset += nw;
        unref_all(ec, ne);
        unref_all(rest, nr);
        CHECK(p_holds(Q.db));
        CHECK(replica_apply(Q.db, last) == 0);
        check_listed(Q.db, "power");
        CHECK(p_holds(Q.db) && d1_holds(Q.db, false));
        CHECK(count_names(Q.dir, "txn_") == 0);
        CHECK(eng_check(Q.db) == 0);
        rstop(&Q);
    }
    CHECK(finished && crashes >= 20 && newset > 0);
    char cmd[PATH_MAX * 2];
    snprintf(cmd, sizeof cmd, "rm -rf '%s-w'", tmpl);
    if (failures == f0 && system(cmd) != 0)
        CHECK(0);
    lclose(&L);
    printf("  power: power lost at each of %d steps of the apply (%d acknowledged, %d with the new set listed): the "
           "old parts or the new set, then the apply ends with the new set\n", crashes, applied, newset);
}

static void alarm_exit(int sig)
{
    (void)sig;
    static const char m[] = "test_ecapply: timed out (a hang)\n";
    if (write(2, m, sizeof m - 1) < 0) {
    }
    _exit(3);
}

int main(int argc, char **argv)
{
    const char *only = NULL, *power_root = NULL, *lpower_root = NULL;
    uint64_t p_last = 0, p_cpos = 0;
    snprintf(S, sizeof S, "/tmp");
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--mutant") && i + 1 < argc)
            mutant = argv[++i];
        else if (!strcmp(argv[i], "--case") && i + 1 < argc)
            only = argv[++i];
        else if (!strcmp(argv[i], "--power-child") && i + 3 < argc)
            power_root = argv[++i], p_last = strtoull(argv[++i], NULL, 10), p_cpos = strtoull(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--leader-power-child") && i + 1 < argc)
            lpower_root = argv[++i];
        else
            snprintf(S, sizeof S, "%s", argv[i]);
    }
    if (strcmp(mutant, "none")) {
        int m = ecapply_mutant_parse(mutant);
        if (m < 0) {
            fprintf(stderr, "test_ecapply: unknown mutant %s\n", mutant);
            return 2;
        }
        ecapply_mutant = m;
    }
    signal(SIGALRM, alarm_exit);
    alarm(240);
    meta_cluster_open_allow(true);
    WRAP = coldops_dir;
    WRAP.all_up = w_all_up;
    WRAP.stripe_version = w_sv;
    atomic_store(&eccold_test_hook, step_hook);
    leader_test_marker_hook = eccold_after_append;
    if (power_root)
        return power_child(power_root, p_last, p_cpos);
    if (lpower_root)
        return leader_power_child(lpower_root);
    char self[PATH_MAX];
    ssize_t sl = readlink("/proc/self/exe", self, sizeof self - 1);
    self[sl > 0 ? sl : 0] = '\0';
    char *slash = strrchr(self, '/');
    snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", slash ? (int)(slash - self) : 1, slash ? self : ".");
    printf("test_ecapply%s%s\n", strcmp(mutant, "none") ? ": mutant " : "", strcmp(mutant, "none") ? mutant : "");
    struct {
        const char *name;
        void (*fn)(void);
    } cases[] = {{"apply", test_apply}, {"above", test_above}, {"dropped", test_dropped}, {"leader", test_leader},
                 {"badft", test_badft}, {"leaderpower", test_leaderpower}, {"power", test_power}};
    for (size_t i = 0; i < sizeof cases / sizeof cases[0]; i++)
        if (!only || !strcmp(only, cases[i].name))
            cases[i].fn();
    rec_reset();
    if (failures) {
        fprintf(stderr, "test_ecapply: %d failures\n", failures);
        return 1;
    }
    printf("test_ecapply: ok\n");
    return 0;
}
#endif
