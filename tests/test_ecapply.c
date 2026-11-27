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
 * nothing renamed, DIVERGED, still after a restart; norecord: the same for a part with no PPOS record); multi (two
 * objects whose rows overlap and three Y, the marker listing them in reverse: numbered by the leader's blocks;
 * ecapply_cold_decide on that listing); twice (a second COLD of a settled key: the first .ec part kept, the second
 * above it); tamper (a marker whose Z is not the object records', with one object's .ec file named twice, or with a
 * damaged Y header check: refused, nothing listed); dropped (P dropped by the replica between the staging and the
 * intent: nothing listed, nothing retired); merging (a merge of the replica's own parts of P held across the apply: its
 * output retired, never listed); leader (a leader killed after its swap, or after the swap's intent and
 * before its renames, reopened as a replica: the COLD replayed is not applied again, every oid listed once, the .ec's
 * PPOS record there; or two COLDs and a clean close: the first's `done` is not lost under the second's `doing`); badft
 * (a COLD whose F is not below its own segment, through the seam ecapply_test_ft: TSDB_ERR_CORRUPT, nothing listed);
 * leaderpower (crashfs: the leader's power fails once its COLD marker committed, before the swap; reopened as a replica
 * it carries the COLD out from the stage_ links jr_cold made durable); power (crashfs power loss at every step of the
 * apply: the old parts or the new set, then the apply ends with the new set); recover (a durable v5 intent with nothing
 * renamed: rolled forward; with a temporary gone: rolled back and applied again; with an own part gone: rolled back,
 * DIVERGED); recpower (crashfs power loss at every step of the open that rolls a cut apply forward: it stands);
 * digest (r8j: the retired copies held until the DIGEST, its job file loaded at an open and MATCHED; a copy whose
 * rows differ: DIVERGED).
 *
 * Planted bugs (--mutant NAME, ecapply.h): ycold ecabovey pposignored v5nodirsync coldnoflushthrough leaderswapnors
 * pposafterswap mergekept notfoundzero noorder checkz seenskip noyhcheck decidesome recrollmissing recnodiverge
 * recnodirsync; r8j's digestnocompare (ecdigest.h) fails its digest case.
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
#include "ecdigest.h"
#include "eckeep.h"

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
    { /* a cluster volume's META names its metadata uuid (cvolfs writes volume_uuid=): a tier takes the
       * volume's vuuid from it (r8f) */
        char mp[PATH_MAX + 16];
        FILE *mf = snprintf(mp, sizeof mp, "%s/META", L->dir) < (int)sizeof mp ? fopen(mp, "a") : NULL;
        if (!mf || fputs("volume_uuid=5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a\n", mf) < 0 || fclose(mf) != 0)
            exit(2);
    }
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

static void above_case(const char *what, bool norecord)
{
    lt L;
    rt R;
    rec_reset();
    if (!start(what, &L))
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
    /* planted: one of the replica's own parts of P says it came after Z (a gate that failed), or has no record at all
     * (a listing path that forgot it: ppos_of then answers 0, which must count as unknown, above Z) */
    part *own[8];
    static char hidden[96];
    size_t no = parts_of(R.db, KEY, own, 8, false);
    CHECK(no >= 1);
    if (no >= 1 && norecord) {
        snprintf(hidden, sizeof hidden, "%s", own[0]->name);
        atomic_store(&ppos_test_hide, hidden);
    } else if (no >= 1) {
        const char *nm[1] = {own[0]->name};
        pthread_mutex_lock(&R.db->publish_lock);
        CHECK(ppos_record(R.db, nm, 1, c.z + 1) == 0);
        pthread_mutex_unlock(&R.db->publish_lock);
    }
    unref_all(own, no);
    int rc = replica_apply(R.db, last);
    atomic_store(&ppos_test_hide, NULL);
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
    printf("  %s: a part of P %s: refused, nothing renamed, DIVERGED also after a restart\n", what,
           norecord ? "with no PPOS record" : "above Z");
}
static void test_above(void)
{
    above_case("above", false);
    above_case("norecord", true);
}

/* ------------------------------------------------------------ multi, twice, tamper: several objects and Y parts */

/* the value series 0 holds at t s of day 0 after workload `m` (false: no row there) */
enum { RW_ONE, RW_MULTI, RW_TWICE };
static bool want_row(int m, int64_t t, float *v)
{
    if (!((t >= 0 && t < 500) || (t >= 1000 && t < 1005) || (m == RW_TWICE && t >= 2000 && t < 2020)))
        return false;
    if (t >= 2000)
        *v = 9000.0f + (float)(t - 2000);
    else if (t >= 1000)
        *v = 100.0f + (float)(t - 1000);
    else if (t < 2)
        *v = 900.0f + (float)t;
    else if (m == RW_TWICE && t >= 300 && t < 400)
        *v = 8000.0f + (float)(t - 300);
    else if (m == RW_MULTI && t >= 240 && t < 260)
        *v = 7000.0f + (float)(t - 240);
    else if (m == RW_MULTI && t >= 150 && t < 250)
        *v = 6000.0f + (float)(t - 150);
    else if (m == RW_MULTI && t >= 100 && t < 200)
        *v = 5000.0f + (float)(t - 100);
    else
        *v = 1.0f + (float)t;
    return true;
}
static bool rows_hold(eng *db, int m, const char *what)
{
    qctx *q = query(db, 0, 0);
    bool ok = q->n != SIZE_MAX;
    size_t i = 0;
    for (int64_t t = 0; ok && t < 2100; t++) {
        float v;
        if (!want_row(m, t, &v))
            continue;
        ok = i < q->n && q->ts[i] == DAY0 + 1000LL * t && q->v[i] == v;
        if (!ok)
            fprintf(stderr, "%s: row %zu is %s, wanted %" PRId64 " s at %g\n", what, i, i < q->n ? "another" : "missing",
                    t, v);
        i++;
    }
    if (ok && i != q->n) {
        ok = false;
        fprintf(stderr, "%s: %zu rows, wanted %zu\n", what, q->n, i);
    }
    free(q);
    return ok;
}
/* P lists wec .ec parts and wrest others (-1: any), every .ec part below every other part */
static void listed_n(eng *db, const char *what, size_t wec, int wrest)
{
    part *ec[8], *rest[8];
    size_t ne = parts_of(db, KEY, ec, 8, true), nr = parts_of(db, KEY, rest, 8, false);
    bool ok = ne == wec && (wrest < 0 || nr == (size_t)wrest);
    for (size_t i = 0; i < ne; i++)
        for (size_t j = 0; j < nr; j++)
            ok &= ec[i]->max_block < rest[j]->min_block;
    CHECK(ok);
    if (!ok)
        fprintf(stderr, "%s: P lists %zu .ec parts and %zu others, wanted %zu and %d, the .ec parts below\n", what, ne,
                nr, wec, wrest);
    unref_all(ec, ne);
    unref_all(rest, nr);
}

/* I of two level-1 parts, A (make_i) and B over A's rows 100-199 (5000 + i: B was rewritten alone while A was held
 * settling, a barrier), then Y: C over 150-249 (6000 + i), D over 240-259 (7000 + i), two parts inserts wrote, and the
 * memtable's rows (900, 901 at 0, 1 s; 100 + i from 1000 s), flushed at the cut. Each overlap is one only the leader's
 * order of the objects, and of Y above them, gets right. The marker as `tm` makes it. */
static void workload_multi(lt *L, eccold_cut *c, int tm)
{
    int64_t key = KEY;
    make_i(L);
    part *a[4];
    size_t na = parts_of(L->db, KEY, a, 4, false);
    CHECK(na == 1);
    pthread_mutex_lock(&L->db->lock);
    for (size_t i = 0; i < na; i++)
        a[i]->settling = true;
    pthread_mutex_unlock(&L->db->lock);
    CHECK(rows(L->db, L->s[0], 0, 100, 100, 5000.0f) == 0);
    CHECK(eng_optimize_keys(L->db, &key, 1) == 0);
    pthread_mutex_lock(&L->db->lock);
    for (size_t i = 0; i < na; i++)
        a[i]->settling = false;
    pthread_mutex_unlock(&L->db->lock);
    unref_all(a, na);
    CHECK(rows(L->db, L->s[0], 0, 150, 100, 6000.0f) == 0);
    CHECK(rows(L->db, L->s[0], 0, 240, 20, 7000.0f) == 0);
    CHECK(rows(L->db, L->s[0], 0, 0, 2, 900.0f) == 0);
    CHECK(rows(L->db, L->s[0], 0, 1000, 5, 100.0f) == 0);
    part *all[8];
    size_t n = parts_of(L->db, KEY, all, 8, false), lv = 0;
    for (size_t i = 0; i < n; i++)
        lv += all[i]->level > 0;
    unref_all(all, n);
    CHECK(n == 4 && lv == 2);
    if (n != 4 || lv != 2)
        fprintf(stderr, "multi: P has %zu parts, %zu of them merged, wanted 4 and 2\n", n, lv);
    atomic_store(&eccold_test_marker, tm);
    CHECK(settle_key(L->db, KEY) == 0);
    atomic_store(&eccold_test_marker, ECCOLD_TM_NONE);
    eccold_test_last(c);
    CHECK(c->rc == 0 && c->nobjs == 2 && c->ny == 3);
    if (c->nobjs != 2 || c->ny != 3)
        fprintf(stderr, "multi: the COLD has %u objects and %u Y parts, wanted 2 and 3\n", (unsigned)c->nobjs,
                (unsigned)c->ny);
}

/* Two objects and three Y in a marker that lists them in reverse: the replica numbers them by the leader's blocks.
 * The decision of a replayed COLD on that listing: both oids done, one of them damage, none not done. */
static void test_multi(void)
{
    lt L;
    rt R;
    rec_reset();
    if (!start("multi", &L))
        return;
    make_replica(&L, &R);
    eccold_cut c;
    workload_multi(&L, &c, ECCOLD_TM_REVERSE);
    CHECK(rows_hold(L.db, RW_MULTI, "multi: the leader"));
    uint64_t last = LAST;
    lstop(&L);
    if (ropen(&L, &R, 0) != 0) {
        CHECK(0);
        lclose(&L);
        return;
    }
    CHECK(feed(&R, last) == 0);
    listed_n(R.db, "multi", 2, 3);
    CHECK(rows_hold(R.db, RW_MULTI, "multi: the replica"));
    part *ec[4];
    size_t ne = parts_of(R.db, KEY, ec, 4, true);
    if (ne == 2) {
        uint8_t o[3 * MK_OID];
        memcpy(o, atomic_load(&ec[0]->ec)->rec.oid, MK_OID);
        memcpy(o + MK_OID, atomic_load(&ec[1]->ec)->rec.oid, MK_OID);
        memset(o + 2 * MK_OID, 0xa5, MK_OID);
        CHECK(ecapply_cold_decide(R.db, KEY, o, 2) == 1);
        CHECK(ecapply_cold_decide(R.db, KEY, o + MK_OID, 2) == TSDB_ERR_CORRUPT); /* one listed, one not */
        CHECK(ecapply_cold_decide(R.db, KEY, o + 2 * MK_OID, 1) == 0);
    }
    unref_all(ec, ne);
    rstop(&R);
    if (ropen(&L, &R, last) == 0) {
        listed_n(R.db, "multi after a close", 2, 3);
        CHECK(rows_hold(R.db, RW_MULTI, "multi after a close"));
        rstop(&R);
    } else {
        CHECK(0);
    }
    lclose(&L);
    printf("  multi: two objects and three Y in reverse marker order, numbered by the leader's blocks; decide\n");
}

/* A second COLD of a settled key (E6: late rows settle as a further object): the replica keeps the first .ec part,
 * lists the second above it, and the rows the second carries win */
static void test_twice(void)
{
    lt L;
    rt R;
    rec_reset();
    if (!start("twice", &L))
        return;
    make_replica(&L, &R);
    eccold_cut c, c2;
    int64_t key = KEY;
    workload(&L, &c, false);
    CHECK(rows(L.db, L.s[0], 0, 300, 100, 8000.0f) == 0);
    CHECK(rows(L.db, L.s[0], 0, 2000, 20, 9000.0f) == 0);
    CHECK(eng_optimize_keys(L.db, &key, 1) == 0); /* Y and the two inserts above the .ec part, a level-1 part */
    CHECK(settle_key(L.db, KEY) == 0);
    eccold_test_last(&c2);
    CHECK(c2.rc == 0 && c2.nobjs == 1 && c2.pos > c.pos);
    CHECK(rows_hold(L.db, RW_TWICE, "twice: the leader"));
    uint64_t last = LAST;
    lstop(&L);
    if (ropen(&L, &R, 0) != 0) {
        CHECK(0);
        lclose(&L);
        return;
    }
    CHECK(feed(&R, last) == 0);
    for (int pass = 0; pass < 2; pass++) {
        const char *what = pass ? "twice after a close" : "twice";
        part *ec[4];
        size_t ne = parts_of(R.db, KEY, ec, 4, true);
        CHECK(ne == 2);
        if (ne != 2)
            fprintf(stderr, "%s: P lists %zu .ec parts, wanted 2\n", what, ne);
        else
            CHECK(memcmp(atomic_load(&ec[0]->ec)->rec.oid, atomic_load(&ec[1]->ec)->rec.oid, MK_OID) != 0);
        unref_all(ec, ne);
        listed_n(R.db, what, 2, -1);
        CHECK(rows_hold(R.db, RW_TWICE, what));
        rstop(&R);
        if (pass == 0 && ropen(&L, &R, last) != 0) {
            CHECK(0);
            break;
        }
    }
    lclose(&L);
    printf("  twice: a second COLD of the key keeps the first .ec part and lists its object above it\n");
}

/* A marker with one field damaged is refused by the replica's checks, nothing listed or renamed */
static void tamper_one(int tm, const char *what)
{
    lt L;
    rt R;
    rec_reset();
    if (!start(what, &L))
        return;
    make_replica(&L, &R);
    eccold_cut c;
    workload_multi(&L, &c, tm);
    uint64_t last = LAST;
    lstop(&L);
    if (ropen(&L, &R, 0) != 0) {
        CHECK(0);
        lclose(&L);
        return;
    }
    int rc = feed(&R, last);
    CHECK(rc != 0);
    part *ec[4];
    size_t ne = parts_of(R.db, KEY, ec, 4, true);
    unref_all(ec, ne);
    CHECK(ne == 0 && settled_of_key(R.dir) == 0);
    if (rc == 0 || ne)
        fprintf(stderr, "%s: the replica applied a damaged COLD (%d, %zu .ec parts)\n", what, rc, ne);
    rstop(&R);
    lclose(&L);
}
static void test_tamper(void)
{
    tamper_one(ECCOLD_TM_Z, "tamper-z");
    tamper_one(ECCOLD_TM_DUPEC, "tamper-dupec");
    tamper_one(ECCOLD_TM_YHC, "tamper-yhc");
    printf("  tamper: Z not the objects', an object's .ec file twice, a Y header check: each refused, nothing listed\n");
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

/* ------------------------------------------------------------ merging: a merge of P in flight across the apply */

/* The replica's own merge of P's parts (eng_optimize_keys, as a purge runs it) is held once its output is written and
 * named, from the staging until the apply has retired its inputs: the apply marks them dropped, so the merge's commit
 * retires its output (merge_commit_locked's dropped branch) instead of listing rows of P beside the .ec part. */
static _Atomic int MH; /* 0 off, 1 armed, 2 held, 3 released */
static _Atomic int MG_RC;
static pthread_t MG_T;
static bool MG_ON;
static void mg_merge_hook(int at)
{
    int armed = 1;
    if (at != 1 || !atomic_compare_exchange_strong(&MH, &armed, 2))
        return;
    while (atomic_load(&MH) == 2)
        msleep(1);
}
static void *mg_thread(void *arg)
{
    int64_t key = KEY;
    atomic_store(&MG_RC, eng_optimize_keys(arg, &key, 1));
    return NULL;
}
static void merging_hook(eng *db, int step)
{
    if (step != ECAPPLY_STEP_STAGED || MG_ON)
        return;
    part *ps[8];
    size_t n = parts_of(db, KEY, ps, 8, false);
    CHECK(n >= 2); /* I and the replica's own flush of the cut's rows: a merge to make */
    unref_all(ps, n);
    atomic_store(&MG_RC, 1);
    atomic_store(&MH, 1);
    if (pthread_create(&MG_T, NULL, mg_thread, db) != 0) {
        atomic_store(&MH, 0);
        CHECK(0);
        return;
    }
    MG_ON = true;
    for (int i = 0; i < 10000 && atomic_load(&MH) != 2 && atomic_load(&MG_RC) == 1; i++)
        msleep(1);
    CHECK(atomic_load(&MH) == 2);
}
static void test_merging(void)
{
    lt L;
    rt R;
    rec_reset();
    if (!start("merging", &L))
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
    MG_ON = false;
    atomic_store(&merge_hook, mg_merge_hook);
    atomic_store(&ecapply_test_hook, merging_hook);
    int rc = feed(&R, last);
    atomic_store(&ecapply_test_hook, NULL);
    CHECK(rc == 0);
    atomic_store(&MH, 3);
    if (MG_ON)
        pthread_join(MG_T, NULL);
    atomic_store(&merge_hook, NULL);
    CHECK(MG_ON && atomic_load(&MG_RC) == 0);
    if (atomic_load(&MG_RC) != 0)
        fprintf(stderr, "merging: the merge: %d\n", atomic_load(&MG_RC));
    CHECK(replica_applied_end(R.db) == last);
    check_listed(R.db, "merging");
    CHECK(p_holds(R.db) && d1_holds(R.db, false));
    CHECK(settled_of_key(R.dir) >= 2);
    CHECK(eng_check(R.db) == 0);
    rstop(&R);
    if (ropen(&L, &R, last) == 0) {
        check_listed(R.db, "merging after a close");
        CHECK(p_holds(R.db) && d1_holds(R.db, false));
        rstop(&R);
    } else {
        CHECK(0);
    }
    lclose(&L);
    printf("  merging: a merge of P's own parts in flight across the apply: its output retired, never listed\n");
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
    char *argv[] = {"test_ecapply", S, "--leader-power-child", L.root, "--mutant", (char *)mutant, NULL};
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
/* the child under crashfs of recpower: the replica's open, which recovers the apply's intent, then a close */
static int recover_child(const char *root, uint64_t cpos)
{
    lt L;
    lt_paths(&L, root);
    rt R = {0};
    snprintf(R.dir, sizeof R.dir, "%s/rep", root);
    if (ropen(&L, &R, cpos - 1) != 0)
        _exit(2);
    power_ack(root, "applied");
    eng_close(R.db);
    tier_put(R.t);
    _exit(0);
}
static int power_run_as(const char *mode, const char *root, int warmup, int seed, uint64_t last, uint64_t cpos)
{
    fflush(NULL);
    char a[24], b[24];
    snprintf(a, sizeof a, "%" PRIu64, last);
    snprintf(b, sizeof b, "%" PRIu64, cpos);
    /* the planted bug goes to the child too: the apply that loses power runs there */
    char *argv[] = {"test_ecapply", S, (char *)mode, (char *)root, a, b, "--mutant", (char *)mutant, NULL};
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
static int power_run(const char *root, int warmup, int seed, uint64_t last, uint64_t cpos)
{
    return power_run_as("--power-child", root, warmup, seed, last, cpos);
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
    int keep0 = -1, stage0 = -1;
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
        /* the files of every shipped marker end as keep_ files whatever the step: a COLD at or below `done` whose
         * files were still staged (power lost between the apply and stage_to_keep) is kept by the open, never an orphan */
        int kp = count_names(Q.dir, "keep_"), sg = count_names(Q.dir, "stage_");
        if (keep0 < 0)
            keep0 = kp, stage0 = sg;
        CHECK(kp == keep0 && sg == stage0);
        if (kp != keep0 || sg != stage0)
            fprintf(stderr, "power: step %d: %d keep_ and %d stage_ files, other steps %d and %d\n", k, kp, sg, keep0,
                    stage0);
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
           "old parts or the new set, then the apply ends with the new set and %d keep_ files\n", crashes, applied, newset,
           keep0);
}

/* ------------------------------------------------------------ recover, recpower: the open's recovery of a v5 intent */

static void mid_exit(eng *db, int which)
{
    (void)db;
    if (which == ECAPPLY_CRASH_MID_INTENT) {
        fflush(NULL);
        _exit(99);
    }
}
/* The template of the recovery cases: the leader settled (one object, one Y), the replica applied through cpos - 1 and
 * closed, then killed in its apply of the COLD right after the .ec renames (cold-mid-intent): the intent durable, N
 * renamed, Y and R not. Without crashfs every byte the child wrote is on disk for the cases. */
static bool mid_template(lt *L, uint64_t *last, uint64_t *cpos)
{
    rt R;
    rec_reset();
    make_replica(L, &R);
    eccold_cut c;
    workload(L, &c, false);
    *last = LAST, *cpos = L->x.cold_fno;
    lstop(L);
    if (ropen(L, &R, 0) != 0)
        return false;
    CHECK(feed(&R, *cpos - 1) == 0);
    rstop(&R);
    fflush(NULL);
    pid_t pid = fork();
    if (pid == 0) {
        if (ropen(L, &R, *cpos - 1) != 0)
            _exit(2);
        atomic_store(&ecapply_crash_hook, mid_exit);
        (void)replica_apply(R.db, *last);
        _exit(5);
    }
    int st = child_status(pid);
    CHECK(st == 99);
    if (st != 99)
        fprintf(stderr, "recover: the apply's child exited %d, wanted the crash point (99)\n", st);
    char rep[PATH_MAX + 8];
    snprintf(rep, sizeof rep, "%s/rep", L->root);
    CHECK(count_names(rep, "txn_") == 1);
    return st == 99;
}
/* the intent in dir: its N and Y names renamed back to their temporaries (nothing renamed, as a cut right after the
 * intent leaves it); the first N temporary and the first R source to *tmp and *src */
static bool unrename(const char *dir, char *tmp, char *src, size_t cap)
{
    DIR *dd = opendir(dir);
    struct dirent *de;
    char in[PATH_MAX + 300] = "";
    while (dd && (de = readdir(dd)) != NULL)
        if (!strncmp(de->d_name, "txn_", 4))
            snprintf(in, sizeof in, "%s/%s", dir, de->d_name);
    if (dd)
        closedir(dd);
    FILE *f = in[0] ? fopen(in, "r") : NULL;
    if (!f)
        return false;
    char line[512];
    tmp[0] = src[0] = '\0';
    bool ok = true;
    while (fgets(line, sizeof line, f)) {
        char k, a[200], b[200], p[PATH_MAX + 300], q[PATH_MAX + 300];
        if (sscanf(line, "%c %199s %199s", &k, a, b) != 3 || (k != 'N' && k != 'Y' && k != 'R'))
            continue;
        if (k == 'R') {
            if (!src[0])
                snprintf(src, cap, "%s", a);
            continue;
        }
        if (k == 'N' && !tmp[0])
            snprintf(tmp, cap, "%s", a);
        snprintf(p, sizeof p, "%s/%s", dir, b);
        snprintf(q, sizeof q, "%s/%s", dir, a);
        if (access(p, F_OK) == 0 && rename(p, q) != 0)
            ok = false;
    }
    fclose(f);
    return ok && tmp[0] && src[0];
}
/* One copy of the template, its intent's renames undone and `gone` (0: nothing, 1: an N temporary, 2: an R source)
 * removed, opened: rolled forward (the new set), rolled back (the own parts listed, then the replay applies the COLD
 * again), or rolled back DIVERGED (an own part is gone: this member no longer holds the rows the COLD replaced). */
static void recover_one(const char *tmpl, int gone, uint64_t last, uint64_t cpos)
{
    static const char *const what[] = {"recover-forward", "recover-tmpgone", "recover-srcgone"};
    char work[PATH_MAX / 2 + 8], cmd[PATH_MAX * 2], rep[PATH_MAX], tmp[200], src[200], p[PATH_MAX + 256];
    snprintf(work, sizeof work, "%s-%d", tmpl, gone);
    snprintf(cmd, sizeof cmd, "rm -rf '%s' && cp -a '%s' '%s'", work, tmpl, work);
    CHECK(system(cmd) == 0);
    snprintf(rep, sizeof rep, "%s/rep", work);
    bool un = unrename(rep, tmp, src, sizeof tmp);
    CHECK(un);
    if (!un)
        return;
    if (gone) {
        snprintf(p, sizeof p, "%s/%s", rep, gone == 1 ? tmp : src);
        CHECK(unlink(p) == 0);
    }
    lt W;
    lt_paths(&W, work);
    rt Q = {0};
    snprintf(Q.dir, sizeof Q.dir, "%s", rep);
    if (ropen(&W, &Q, cpos - 1) != 0) {
        CHECK(0);
        fprintf(stderr, "%s: the replica does not open\n", what[gone]);
        return;
    }
    CHECK(count_names(rep, "txn_") == 0);
    part *ec[8];
    size_t ne = parts_of(Q.db, KEY, ec, 8, true);
    unref_all(ec, ne);
    int st = settled_of_key(rep);
    bool div = atomic_load(&Q.db->diverged);
    bool ok = gone == 0 ? ne == 1 && st >= 1 && !div : gone == 1 ? ne == 0 && st == 0 && !div : ne == 0 && div;
    CHECK(ok);
    if (!ok)
        fprintf(stderr, "%s: %zu .ec parts, %d settled_, %sDIVERGED after the open\n", what[gone], ne, st,
                div ? "" : "not ");
    if (gone < 2) {
        CHECK(p_holds(Q.db));
        CHECK(replica_apply(Q.db, last) == 0); /* RSTATE's `doing`: the decide, and the COLD again where rolled back */
        check_listed(Q.db, what[gone]);
        CHECK(p_holds(Q.db) && d1_holds(Q.db, false));
    }
    rstop(&Q);
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", work);
    if (!failures)
        CHECK(system(cmd) == 0);
}
static void test_recover(void)
{
    lt L;
    uint64_t last = 0, cpos = 0;
    if (!start("recover", &L))
        return;
    if (mid_template(&L, &last, &cpos))
        for (int gone = 0; gone < 3; gone++)
            recover_one(L.root, gone, last, cpos);
    lclose(&L);
    printf("  recover: a durable v5 intent with nothing renamed: rolled forward; a temporary gone: rolled back, applied "
           "again; an own part gone: rolled back, DIVERGED\n");
}
/* crashfs power loss at every step of the open that rolls a cut apply forward (cold-mid-intent: N renamed, Y and R
 * not): the apply stands at every step, the intent goes only once its renames are durable */
static void test_recpower(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  recpower: skipped (no %s)\n", SHIM);
        return;
    }
    lt L;
    uint64_t last = 0, cpos = 0;
    if (!start("recpower", &L))
        return;
    if (!mid_template(&L, &last, &cpos)) {
        lclose(&L);
        return;
    }
    int crashes = 0, f0 = failures, seeds = 6;
    bool finished = false;
    char work[PATH_MAX / 2 + 8], cmd[PATH_MAX * 2];
    snprintf(work, sizeof work, "%s-w", L.root);
    for (int ks = 0; ks < 400 * seeds && !finished && failures == f0; ks++) {
        int k = ks / seeds;
        alarm(240);
        snprintf(cmd, sizeof cmd, "rm -rf '%s' && cp -a '%s' '%s'", work, L.root, work);
        CHECK(system(cmd) == 0);
        int code = power_run_as("--recover-child", work, k, ks + 1, last, cpos);
        CHECK(code == 99 || code == 0);
        if (code != 99 && code != 0) {
            fprintf(stderr, "recpower: the child at step %d exited %d\n", k, code);
            break;
        }
        crashes += code == 99;
        finished = code == 0 && ks % seeds == seeds - 1;
        lt W;
        lt_paths(&W, work);
        rt Q = {0};
        snprintf(Q.dir, sizeof Q.dir, "%s/rep", work);
        if (ropen(&W, &Q, cpos - 1) != 0) {
            CHECK(0);
            fprintf(stderr, "recpower: after a power loss at step %d the replica does not open\n", k);
            break;
        }
        part *ec[8], *rest[8];
        size_t ne = parts_of(Q.db, KEY, ec, 8, true), nr = parts_of(Q.db, KEY, rest, 8, false);
        unref_all(ec, ne);
        unref_all(rest, nr);
        int st = settled_of_key(Q.dir);
        bool ok = ne == 1 && nr == 1 && st >= 1 && count_names(Q.dir, "txn_") == 0 && p_holds(Q.db);
        CHECK(ok);
        if (!ok)
            fprintf(stderr, "recpower: step %d seed %d: %zu .ec parts, %zu others, %d settled_\n", k, ks % seeds, ne, nr,
                    st);
        CHECK(replica_apply(Q.db, last) == 0);
        check_listed(Q.db, "recpower");
        CHECK(p_holds(Q.db) && d1_holds(Q.db, false));
        rstop(&Q);
    }
    CHECK(finished && crashes >= 3);
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", work);
    if (failures == f0)
        CHECK(system(cmd) == 0);
    lclose(&L);
    printf("  recpower: power lost at each of %d steps of the open that rolls a cut apply forward: it stands\n",
           crashes);
}

static void alarm_exit(int sig)
{
    (void)sig;
    static const char m[] = "test_ecapply: timed out (a hang)\n";
    if (write(2, m, sizeof m - 1) < 0) {
    }
    _exit(3);
}

/* ------------------------------------------------------------ digest: the replica's job (r8j, E26, D6-D7) */

/* the names of the replica's retired copies of KEY */
static size_t retired_names(const char *dir, char (*out)[256], size_t cap)
{
    char pre[64];
    snprintf(pre, sizeof pre, "settled_p%" PRId64 "_", (int64_t)KEY);
    size_t n = 0;
    DIR *d = opendir(dir);
    struct dirent *de;
    while (d && (de = readdir(d)) != NULL)
        if (strncmp(de->d_name, pre, strlen(pre)) == 0 && n < cap)
            snprintf(out[n++], 256, "%s", de->d_name);
    if (d)
        closedir(d);
    return n;
}

/* every retired copy of KEY in R's keep with status want (waits up to wait_ms for each) */
static bool copies_are(rt *R, int want, int64_t wait_ms, const char *what)
{
    char names[8][256];
    size_t n = retired_names(R->dir, names, 8);
    bool ok = n >= 1;
    for (size_t i = 0; i < n; i++) {
        int st = eckeep_test_wait(R->db, names[i], wait_ms);
        if (st != want) {
            fprintf(stderr, "%s: %s has keep status %d, wanted %d\n", what, names[i], st, want);
            ok = false;
        }
    }
    if (n == 0)
        fprintf(stderr, "%s: no retired copy of the key\n", what);
    return ok;
}

/* a job file's flags (ECDJ_*, the u32 after magic, key and z), -1 when there is none */
static long job_flags(const char *path)
{
    uint8_t b[28];
    int fd = open(path, O_RDONLY);
    if (fd < 0)
        return -1;
    ssize_t r = pread(fd, b, sizeof b, 0);
    close(fd);
    if (r != (ssize_t)sizeof b)
        return -1;
    uint32_t f;
    memcpy(&f, b + 24, 4);
    return f;
}

/* flips one byte inside the first granule of the retired copy `name` in R's directory: its checksum fails (rot) */
static bool rot_copy(rt *R, const char *name)
{
    part *p = NULL;
    if (part_open_as(R->db->dirfd, name, name + 8, false, false, &p) != 0 || p->ng == 0) {
        if (p)
            part_unref(p);
        return false;
    }
    granule gr;
    gx_load(p, 0, GW_LOC, &gr);
    off_t at = (off_t)gr.off + 1;
    part_unref(p);
    char path[PATH_MAX + 256];
    snprintf(path, sizeof path, "%s/%s", R->dir, name);
    int fd = open(path, O_RDWR);
    uint8_t c;
    bool ok = fd >= 0 && pread(fd, &c, 1, at) == 1;
    c ^= 0x40;
    ok = ok && pwrite(fd, &c, 1, at) == 1 && fsync(fd) == 0;
    if (fd >= 0)
        close(fd);
    return ok;
}

/* The replica's retired copies wait for the leader's DIGEST. Modes:
 * digest: applied through the COLD only, they are held, bound by the unarmed job file the COLD wrote (flags 0), also
 *   across a restart (never ORPHAN); the DIGEST applied with the keep thread paused arms the job (durable before the
 *   apply goes on), which a restart loads and digests: MATCHED (flag written), the volume not DIVERGED.
 * digestalter: the copy's rows differ from the leader's with valid checksums (hook ecdigest_test_alter): DIVERGED and
 *   the copies kept (mutant digestnocompare).
 * supersede: a DIGEST of a later Z of the key arrives first (the COLD's leader was deposed before its DIGEST): the
 *   copies are ORPHAN (released unchecked, counted undigested), never DIVERGED, also after a restart (mutants
 *   digestanyz: they are digested against the later DIGEST; digestnosupersede: held forever).
 * rot: one copy fails a granule checksum: ROT, counted lost, never DIVERGED (mutants digestnocrcwalk, digestrotmatches,
 *   digestcorruptrot is the other side: a digest refused with valid checksums is DIVERGED, not rot).
 * missing: a copy gone before the DIGEST, no match recorded: ROT, counted lost (digestrotmatches). */
static void test_digest(void)
{
    static const char *const modes[] = {"digest", "digestalter", "supersede", "rot", "missing"};
    for (int pass = 0; pass < 5; pass++) {
        const char *what = modes[pass];
        lt L;
        rt R;
        rec_reset();
        if (!start(what, &L))
            return;
        make_replica(&L, &R);
        eccold_cut c;
        workload(&L, &c, true);
        uint64_t last = LAST, cpos = L.x.cold_fno;
        lstop(&L);
        char job[64], jp[PATH_MAX + 64];
        snprintf(job, sizeof job, "digest_%" PRId64 "_%" PRIu64, (int64_t)KEY, c.z);
        snprintf(jp, sizeof jp, "%s/%s", R.dir, job);
        if (ropen(&L, &R, 0) != 0) {
            CHECK(0);
            lclose(&L);
            return;
        }
        if (pass != 1) {
            CHECK(feed(&R, cpos) == 0); /* through the COLD, not its DIGEST */
            CHECK(settled_of_key(R.dir) >= 1);
            CHECK(copies_are(&R, ECKEEP_PENDING, 300, "digest: before the DIGEST"));
            CHECK(job_flags(jp) == 0); /* bound at the COLD, unarmed */
            rstop(&R);
            if (ropen(&L, &R, cpos) != 0) {
                CHECK(0);
                lclose(&L);
                return;
            }
            CHECK(copies_are(&R, ECKEEP_PENDING, 300, "digest: held across a restart"));
        }
        char names[8][256];
        size_t n = retired_names(R.dir, names, 8);
        if (pass == 0) {
            atomic_store(&eckeep_test_pause, true);
            CHECK(replica_apply(R.db, last) == 0);
            CHECK(job_flags(jp) >= 0 && (job_flags(jp) & ECDJ_ARMED)); /* durable before the apply goes on */
            rstop(&R);
            atomic_store(&eckeep_test_pause, false);
            if (ropen(&L, &R, last) != 0) {
                CHECK(0);
                lclose(&L);
                return;
            }
            CHECK(copies_are(&R, ECKEEP_MATCHED, 20000, "digest: the job loaded at the open"));
            CHECK(job_flags(jp) >= 0 && (job_flags(jp) & ECDJ_MATCHED));
            CHECK(!atomic_load(&R.db->diverged));
            CHECK(p_holds(R.db) && d1_holds(R.db, true));
            /* the release: with keep_s 0 the open of a MATCHED job releases its copies without a second digest, and
             * the job file goes after the last one (mutant digestnojobremove) */
            rstop(&R);
            L.set.keep_s = 0;
            if (ropen(&L, &R, last) != 0) {
                CHECK(0);
                lclose(&L);
                return;
            }
            bool gone = false;
            for (int i = 0; i < 200 && !gone; i++) {
                if (n)
                    (void)eckeep_test_wait(R.db, names[0], 50);
                gone = settled_of_key(R.dir) == 0 && access(jp, F_OK) != 0;
                if (!gone)
                    usleep(50000);
            }
            if (!gone)
                fprintf(stderr, "digest: %d retired copies, job file %s after the release\n", settled_of_key(R.dir),
                        access(jp, F_OK) == 0 ? "still there" : "gone");
            CHECK(gone);
            CHECK(eckeep_test_tries(R.db) == 0); /* released as matched: no second digest */
            CHECK(!atomic_load(&R.db->diverged));
            CHECK(p_holds(R.db) && d1_holds(R.db, true));
        } else if (pass == 1) {
            atomic_store(&ecdigest_test_alter, true);
            CHECK(feed(&R, last) == 0);
            CHECK(access(jp, F_OK) == 0);
            CHECK(copies_are(&R, ECKEEP_DIVERGED, 20000, "digestalter"));
            atomic_store(&ecdigest_test_alter, false);
            CHECK(atomic_load(&R.db->diverged));
            CHECK(settled_of_key(R.dir) >= 1); /* kept */
        } else if (pass == 2) {
            /* the DIGEST of a later COLD of the key, whose expectation these copies cannot meet */
            mk m;
            memset(&m, 0, sizeof m);
            m.type = MK_DIGEST;
            m.span = 1;
            m.u.digest = (mk_digest){.key = KEY, .z = c.z + 1, .rows = 7, .series = 3, .xxh64 = 0x5eed,
                                     .now_ms = eng_now_ms(R.db)};
            CHECK(ecdigest_apply(R.db, last, &m) == 0);
            bool orphan = copies_are(&R, ECKEEP_ORPHAN, 20000, "supersede");
            CHECK(orphan);
            CHECK(!atomic_load(&R.db->diverged));
            CHECK(eckeep_copies_undigested(R.db) == n);
            CHECK(job_flags(jp) >= 0 && (job_flags(jp) & ECDJ_SUPERSEDED) && !(job_flags(jp) & ECDJ_ARMED));
            rstop(&R);
            if (ropen(&L, &R, cpos) != 0) {
                CHECK(0);
                lclose(&L);
                return;
            }
            CHECK(orphan && copies_are(&R, ECKEEP_ORPHAN, 20000, "supersede: after a restart"));
            CHECK(!atomic_load(&R.db->diverged));
        } else if (pass == 3) {
            CHECK(n >= 1 && rot_copy(&R, names[0]));
            uint64_t lost0 = eckeep_copies_lost(R.db);
            CHECK(replica_apply(R.db, last) == 0); /* the log is there: appended before the restart */
            CHECK(copies_are(&R, ECKEEP_ROT, 20000, "rot"));
            CHECK(!atomic_load(&R.db->diverged));
            CHECK(eckeep_copies_lost(R.db) == lost0 + 1);
        } else {
            CHECK(n >= 1);
            char path[PATH_MAX + 256];
            snprintf(path, sizeof path, "%s/%s", R.dir, names[0]);
            uint64_t lost0 = eckeep_copies_lost(R.db);
            CHECK(unlink(path) == 0);
            CHECK(replica_apply(R.db, last) == 0);
            int st = eckeep_test_wait(R.db, names[0], 20000);
            if (st != ECKEEP_ROT)
                fprintf(stderr, "missing: %s has keep status %d, wanted %d\n", names[0], st, ECKEEP_ROT);
            CHECK(st == ECKEEP_ROT);
            CHECK(n < 2 || copies_are(&R, ECKEEP_ROT, 20000, "missing: the others"));
            CHECK(!atomic_load(&R.db->diverged));
            CHECK(eckeep_copies_lost(R.db) == lost0 + 1);
        }
        rstop(&R);
        lclose(&L);
    }
    printf("  digest: retired copies bound at the COLD and held until the DIGEST, its job durable and loaded at an "
           "open, MATCHED; rows that differ: DIVERGED; a later DIGEST first: ORPHAN; rot or a missing copy: ROT, "
           "counted lost\n");
}

/* the child under crashfs (root = the replica's directory) of digestpower: the replica, applied through the COLD,
 * applies its DIGEST with the keep thread paused (the job is armed, never run) and loses power as it exits */
static int digest_power_child(const char *root, uint64_t last, uint64_t cpos)
{
    lt L;
    lt_paths(&L, root);
    rt R = {0};
    snprintf(R.dir, sizeof R.dir, "%s/rep", root);
    atomic_store(&eckeep_test_pause, true);
    if (ropen(&L, &R, cpos) != 0)
        _exit(2);
    if (replica_apply(R.db, last) != 0)
        _exit(5);
    power_ack(root, "applied");
    exit(0); /* the loss at exit is the shim's destructor; nothing closed, so no later sync covers the job file */
}

/* An applied DIGEST's job is durable before the apply goes on: power lost just after it, the open finds the job
 * armed and digests it (MATCHED), never holds the copies as unarmed (mutant digestjobnosync: the file is renamed
 * without its fdatasync, so the loss leaves it empty or torn) */
static void test_digestpower(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  digestpower: skipped (no %s)\n", SHIM);
        return;
    }
    lt L;
    rt R;
    rec_reset();
    if (!start("digestpower", &L))
        return;
    make_replica(&L, &R);
    eccold_cut c;
    workload(&L, &c, true);
    uint64_t last = LAST, cpos = L.x.cold_fno;
    lstop(&L);
    if (ropen(&L, &R, 0) != 0) {
        CHECK(0);
        lclose(&L);
        return;
    }
    CHECK(feed(&R, cpos) == 0);
    rstop(&R);
    char jp[PATH_MAX + 64];
    snprintf(jp, sizeof jp, "%s/digest_%" PRId64 "_%" PRIu64, R.dir, (int64_t)KEY, c.z);
    CHECK(job_flags(jp) == 0);
    power_unack(L.root, "applied");
    fflush(NULL);
    char a[24], b[24];
    snprintf(a, sizeof a, "%" PRIu64, last);
    snprintf(b, sizeof b, "%" PRIu64, cpos);
    char *argv[] = {"test_ecapply", S, "--digest-power-child", L.root, a, b, "--mutant", (char *)mutant, NULL};
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512];
        snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0", getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
        setenv("ASAN_OPTIONS", asan, 1);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", R.dir, 1); /* the replica only: the stores and the ack stay as written */
        setenv("CRASHFS_AT_EXIT", "1", 1);
        execv("/proc/self/exe", argv);
        _exit(127);
    }
    int st = child_status(pid);
    CHECK(st == 99 || st == 0);
    CHECK(power_acked(L.root, "applied"));
    if (!power_acked(L.root, "applied"))
        fprintf(stderr, "digestpower: the child's status %d, no ack\n", st);
    long f = job_flags(jp);
    if (f < 0 || !(f & ECDJ_ARMED))
        fprintf(stderr, "digestpower: after the power loss the job file's flags are %ld\n", f);
    CHECK(f >= 0 && (f & ECDJ_ARMED));
    if (ropen(&L, &R, last) != 0) {
        CHECK(0);
        lclose(&L);
        return;
    }
    CHECK(copies_are(&R, ECKEEP_MATCHED, 20000, "digestpower: the job after the power loss"));
    CHECK(!atomic_load(&R.db->diverged));
    rstop(&R);
    lclose(&L);
    printf("  digestpower: power lost just after the DIGEST's apply: its job is armed at the open and MATCHED\n");
}

/* R9 (E10): degraded MK_COLD record applied on a replica (k+m-1 positions accepted on cluster EC, k+m-2 and
 * single-server partial positions refused; replica reads back bit-exact with known erasure). */
static atomic_uint EA_DEG_UP = 0x0eu; /* pos 0 down on a 2+2 stripe */
static int ea_deg_up_mask(void *c, uint64_t req_epoch, uint32_t probe_mask, uint32_t *up_out, unsigned *npos_out)
{
    (void)c, (void)req_epoch;
    uint32_t u = atomic_load(&EA_DEG_UP);
    *npos_out = 4;
    *up_out = probe_mask ? (u & probe_mask) : u;
    return 0;
}

static void test_degraded(void)
{
    static int seq;
    char root[PATH_MAX / 2], ldir[PATH_MAX], rdir[PATH_MAX], st[4][PATH_MAX];
    snprintf(root, sizeof root, "%s/ecapply-%d-degraded-%d", S, (int)getpid(), seq++);
    snprintf(ldir, sizeof ldir, "%s/db", root);
    snprintf(rdir, sizeof rdir, "%s/rep", root);
    char cmd[PATH_MAX + 64];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", root);
    CHECK(system(cmd) == 0);
    CHECK(mkdir(root, 0755) == 0 && mkdir(ldir, 0755) == 0 && mkdir(rdir, 0755) == 0);
    for (int i = 0; i < 4; i++) {
        snprintf(st[i], sizeof st[i], "%s/st%d", root, i);
        CHECK(mkdir(st[i], 0755) == 0);
    }
    char spec[5 * PATH_MAX];
    snprintf(spec, sizeof spec, "k=2,m=2,dirs=%s:%s:%s:%s,grace_s=%lld", st[0], st[1], st[2], st[3], (long long)GRACE_S);
    setenv("TSDB_TEST_TIER", spec, 1);
    setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);

    tsdb_options o;
    opts(&o);
    eng *db = NULL;
    uint32_t s0 = 0;
    CHECK(eng_open(ldir, &o, &db) == 0 && db);
    CHECK(eng_series(db, "s0", TSDB_FLOAT32, &s0) == 0);
    CHECK(eng_close(db) == 0);
    int fd = open(ldir, O_RDONLY | O_DIRECTORY);
    CHECK(fd >= 0 && meta_require(fd, true, META_REQ_CLUSTER | META_REQ_REPLICA | META_REQ_EC) == 0);
    if (fd >= 0)
        close(fd);
    {
        char mp[PATH_MAX + 16];
        snprintf(mp, sizeof mp, "%s/META", ldir);
        FILE *mf = fopen(mp, "a");
        CHECK(mf && fputs("volume_uuid=7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c\n", mf) >= 0 && fclose(mf) == 0);
    }

    /* Open leader with 2+2 degraded cold ops */
    cold_ops dops = coldops_dir;
    dops.stripe_version = w_sv;
    dops.stripe_up_mask = ea_deg_up_mask;

    rec_reset();
    tier *t = NULL;
    CHECK(tier_open(ldir, &o, &t) == 0 && t);
    tier_settings set = {.k = 2, .m = 2, .grace_s = GRACE_S, .keep_s = 3600, .coarse_s = 86400, .cache_mb = 16,
                         .force_s = 3600, .wait_max_s = 1};
    tier_attach a = {.ops = &dops, .make = w_make, .settings = &set};
    tier_want_attach(t, 0, &a);
    db = NULL;
    CHECK(eng_open(ldir, &o, &db) == 0 && db);
    tier_want(NULL, 0);

    /* Initialize replica directory (FNO + SERIES) and copy leader's META (requires= and vuuid) */
    {
        eng *init_r = NULL;
        uint32_t rs0 = 0;
        CHECK(eng_open(rdir, &o, &init_r) == 0 && init_r);
        if (init_r) {
            CHECK(eng_series(init_r, "s0", TSDB_FLOAT32, &rs0) == 0 && rs0 == s0);
            CHECK(eng_close(init_r) == 0);
        }
        char a1[PATH_MAX + 32], a2[PATH_MAX + 32];
        snprintf(a1, sizeof a1, "%s/META", ldir), snprintf(a2, sizeof a2, "%s/META", rdir);
        CHECK(copy_file(a1, a2));
    }

    xport xp;
    memset(&xp, 0, sizeof xp);
    pthread_mutex_init(&xp.mu, NULL);
    struct jr_ops jops = {.ctx = &xp, .admit = xp_admit, .bytes = rx_bytes, .synced = xp_synced, .ship = rx_ship,
                          .wait = xp_wait, .seg_done = xp_seg_done};
    CHECK(eng_leader_attach(db, &jops) == 0);

    CHECK(rows(db, s0, 0, 0, 500, 1.0f) == 0);
    CHECK(eng_optimize(db) == 0);

    /* Trigger degraded settle on leader with pos 0 down */
    atomic_store(&EA_DEG_UP, 0x0eu);
    CHECK(settle_key(db, KEY) == TSDB_ERR_UNAVAILABLE);
    msleep(1100);
    CHECK(settle_key(db, KEY) == 0);
    uint32_t leader_pos = 0;
    {
        part *ec[4];
        size_t n = parts_of(db, KEY, ec, 4, true);
        CHECK(n == 1);
        if (n == 1) {
            struct ecpart *ep = atomic_load(&ec[0]->ec);
            leader_pos = ep->rec.positions;
            unsigned npos = (unsigned)ep->rec.k + (unsigned)ep->rec.m;
            CHECK(leader_pos != 0 && __builtin_popcount(leader_pos) == (int)(npos - 1) && !(leader_pos & 1u));
        }
        unref_all(ec, n);
    }
    uint64_t last = LAST;
    CHECK(eng_close(db) == 0);
    tier_put(t);

    /* Open replica over the same 4 stores and apply the recorded log (including degraded MK_COLD) */
    tier *rtier = NULL;
    eng *rdb = NULL;
    CHECK(tier_open(rdir, &o, &rtier) == 0 && rtier);
    tier_want_attach(rtier, 0, &a);
    CHECK(eng_open_replica(rdir, &o, 0, &rdb) == 0 && rdb);
    tier_want(NULL, 0);
    rt R = {.t = rtier, .db = rdb};
    snprintf(R.dir, sizeof R.dir, "%s", rdir);
    CHECK(feed(&R, last) == 0);

    /* Replica must list the degraded .ec part with the exact k'+m-1 positions bitmap and serve exact rows */
    {
        part *ec[4];
        size_t n = parts_of(rdb, KEY, ec, 4, true);
        CHECK(n == 1);
        if (n == 1) {
            struct ecpart *ep = atomic_load(&ec[0]->ec);
            CHECK(ep->rec.positions == leader_pos);
            /* Single-server decode of this degraded record must be refused; k+m-2 on cluster EC must be refused */
            size_t rl = ec_record_size(&ep->rec);
            uint8_t *rb = malloc(rl);
            CHECK(rb && ec_record_encode_as(&ep->rec, rb, rl, true) == 0);
            ec_record tmp;
            CHECK(ec_record_decode_as(rb, rl, &tmp, false) == TSDB_ERR_CORRUPT);
            uint32_t bad2 = leader_pos & ~(leader_pos & (uint32_t)(-(int32_t)leader_pos)); /* clear 1 more bit */
            memcpy(rb + EC_REC_POSITIONS, &bad2, 4);
            CHECK(ec_record_decode_as(rb, rl, &tmp, true) == TSDB_ERR_CORRUPT);
            free(rb);
        }
        unref_all(ec, n);
    }
    CHECK(holds(rdb, s0, 0, 0, 500, 1.0f));

    CHECK(eng_close(rdb) == 0);
    tier_put(rtier);
    free(xp.cold);
    pthread_mutex_destroy(&xp.mu);
    rec_reset();
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", root);
    CHECK(system(cmd) == 0);
    printf("  degraded: replica applied degraded MK_COLD (positions 0x%x), read back bit-exact, refused k+m-2 and "
           "single-server partial bitmap\n", leader_pos);
}

int main(int argc, char **argv)
{
    const char *only = NULL, *power_root = NULL, *lpower_root = NULL, *recover_root = NULL, *dpower_root = NULL;
    uint64_t p_last = 0, p_cpos = 0;
    snprintf(S, sizeof S, "/tmp");
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--mutant") && i + 1 < argc)
            mutant = argv[++i];
        else if (!strcmp(argv[i], "--case") && i + 1 < argc)
            only = argv[++i];
        else if (!strcmp(argv[i], "--power-child") && i + 3 < argc)
            power_root = argv[++i], p_last = strtoull(argv[++i], NULL, 10), p_cpos = strtoull(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--recover-child") && i + 3 < argc)
            recover_root = argv[++i], p_last = strtoull(argv[++i], NULL, 10), p_cpos = strtoull(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--digest-power-child") && i + 3 < argc)
            dpower_root = argv[++i], p_last = strtoull(argv[++i], NULL, 10), p_cpos = strtoull(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--leader-power-child") && i + 1 < argc)
            lpower_root = argv[++i];
        else
            snprintf(S, sizeof S, "%s", argv[i]);
    }
    if (strcmp(mutant, "none")) {
        int m = ecapply_mutant_parse(mutant), dm = m < 0 ? ecdigest_mutant_parse(mutant) : -1;
        if (dm > 0) { /* r8j's: digestskip (a replica's compare skipped) */
            ecdigest_mutant = dm;
            m = 0;
        }
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
    if (dpower_root)
        return digest_power_child(dpower_root, p_last, p_cpos);
    if (recover_root)
        return recover_child(recover_root, p_cpos);
    char self[PATH_MAX];
    ssize_t sl = readlink("/proc/self/exe", self, sizeof self - 1);
    self[sl > 0 ? sl : 0] = '\0';
    char *slash = strrchr(self, '/');
    snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", slash ? (int)(slash - self) : 1, slash ? self : ".");
    printf("test_ecapply%s%s\n", strcmp(mutant, "none") ? ": mutant " : "", strcmp(mutant, "none") ? mutant : "");
    if (strcmp(mutant, "none") && access(SHIM, R_OK) != 0) { /* a skipped power case would let a planted bug pass */
        fprintf(stderr, "test_ecapply: a mutant run needs %s (make %s)\n", SHIM, SHIM);
        return 2;
    }
    struct {
        const char *name;
        void (*fn)(void);
    } cases[] = {{"apply", test_apply}, {"above", test_above}, {"dropped", test_dropped}, {"merging", test_merging},
                 {"multi", test_multi}, {"twice", test_twice}, {"tamper", test_tamper}, {"leader", test_leader},
                 {"badft", test_badft}, {"leaderpower", test_leaderpower}, {"power", test_power},
                 {"recover", test_recover}, {"recpower", test_recpower}, {"digest", test_digest},
                 {"digestpower", test_digestpower}, {"degraded", test_degraded}};
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
