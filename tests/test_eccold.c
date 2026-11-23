/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * A leader's settle of a cluster ec volume (src/eccold.c, r8g-cut; design-cluster-data.md 2.3 steps 3-5, the R8
 * plan's D4, D18, D21, D22). The engine is a leader (a jr_ops of the test's own, as tests/test_replica.c's transport,
 * without a replica: its COMMIT and COLD markers commit as the bytes are handed over, or when the test moves the commit
 * point with `manual`) on a directory whose META says requires=cluster,ec, with a tier of three directory stores whose
 * cold ops the test wraps: it can say a position is DOWN and set the stripe version.
 *
 * Cases: the cut (L <= Z, F, the marker at its position, the .ec records' ppos = Z and stripe version, rows unchanged,
 * a reopen); the rotate (rows of P in the memtable go to a Y part, rows of another day to a part of their own); the
 * gate (a small write of a series not in the settled parts into P's day waits for the settle, one into another day
 * does not); the wait (Z only once the commit point passed L); a position DOWN before the gate (no fence: a small
 * write into P does not wait for it; the objects go to r7-gc) and at the re-check after it; the TTL pass skips a key
 * with a settling part; a deposed leader in the wait is INDETERMINATE and fails nothing; large inserts, replaces,
 * deletes and small writes during a settle all stay visible; the policy's cluster branch (whole keys after the grace,
 * never level 0, never a key about to expire, never on a diverged engine).
 *
 * swappower: crashfs power loss at every step of a settle, a drop of its .ec part and an open that compacts PPOS: the
 * leader reopens with a PPOS record for every listed part, the .ec included (src/ppos.h; the swap's records, written
 * by ecapply_swap_pre before its intent).
 *
 * Planted bugs (--mutant NAME, eccold.h): cutnogate cutnowait ttlsettling cutnorotate fencekeyonly stripewaitfenced;
 * ppos.h's pposswapnone (the swap records nothing for its .ec parts) and pposswapafter (only after its renames: only
 * a power loss sees it); the Makefile requires each to fail this test. An alarm ends a hung run with status 3 (a hang must not survive).
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
    printf("test_eccold: skipped (built without TSDB_EC)\n");
    return 0;
}
#else

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

static int64_t now_ms(void)
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
static bool admitted_marker(xport *x, uint64_t pos)
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
    snprintf(root, sizeof root, "%s/eccold-%d-%s-%d", S, (int)getpid(), what, seq++);
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
    L->ops = (struct jr_ops){.ctx = &L->x, .ship = xp_ship, .wait = xp_wait, .seg_done = xp_seg_done,
                             .admit = xp_admit, .bytes = xp_bytes, .synced = xp_synced};
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
static bool chunks_go(lt *L, int want)
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

static int fsum(eng *db, const char *name, uint64_t *size, uint64_t *sum)
{
    int fd = openat(db->dirfd, name, O_RDONLY | O_CLOEXEC);
    struct stat st;
    if (fd < 0 || fstat(fd, &st) != 0) {
        if (fd >= 0)
            close(fd);
        return -1;
    }
    uint8_t *b = malloc((size_t)st.st_size + 1);
    ssize_t k = b ? pread(fd, b, (size_t)st.st_size, 0) : -1;
    close(fd);
    if (k != st.st_size) {
        free(b);
        return -1;
    }
    *size = (uint64_t)st.st_size;
    *sum = xxh64(b, (size_t)st.st_size, 0);
    free(b);
    return 0;
}

/* The COLD marker as written to the log (the bytes the replicas get), decoded and compared with the leader: Z, F, the
 * stripe version, the key, each object's oid and record (the listed .ec part's, encoded), and each file's name, kind,
 * size, xxh64 and header check against the .ec and Y files on disk. whole: nothing was written into the key since,
 * so the .ec parts and Y are every part of the key (what a replica that installs the files lists). */
static void check_marker(lt *L, const eccold_cut *c, bool whole)
{
    pthread_mutex_lock(&L->x.mu);
    uint8_t *buf = L->x.cold;
    size_t len = L->x.ncold;
    uint64_t fno = L->x.cold_fno;
    L->x.cold = NULL;
    pthread_mutex_unlock(&L->x.mu);
    mk m;
    memset(&m, 0, sizeof m);
    CHECK(buf && fno == c->pos && mk_decode(buf, len, &m) == 0 && m.type == MK_COLD);
    if (!buf || m.type != MK_COLD) {
        free(buf);
        return;
    }
    CHECK(m.u.cold.z == c->z && m.u.cold.flush_through == c->f && m.u.cold.stripe_version == c->stripe_version);
    CHECK(m.u.cold.key == c->key && m.u.cold.nobjs == c->nobjs && m.u.cold.nfiles == c->nobjs + c->ny);
    part *ec[8], **rest = calloc(4096, sizeof *rest);
    size_t ne = parts_of(L->db, c->key, ec, 8, true), nr = rest ? parts_of(L->db, c->key, rest, 4096, false) : 0;
    CHECK(ne == c->nobjs);
    size_t pos = 0, k = 0;
    mk_cold_obj o;
    while (mk_cold_obj_next(&m.u.cold, &pos, &o)) {
        bool found = false;
        for (size_t i = 0; i < ne; i++) {
            struct ecpart *e = atomic_load(&ec[i]->ec);
            if (memcmp(o.oid, e->rec.oid, MK_OID) != 0)
                continue;
            found = true;
            CHECK(e->rec.ppos == c->z);
            size_t rl = ec_record_size(&e->rec);
            uint8_t *rb = malloc(rl);
            CHECK(rb && ec_record_encode_as(&e->rec, rb, rl, true) == 0 && o.rec_len == rl && !memcmp(o.rec, rb, rl));
            free(rb);
        }
        CHECK(found);
        k++;
    }
    CHECK(k == c->nobjs);
    mk_file f;
    size_t nec = 0, ny = 0;
    pos = 0;
    while (mk_cold_file_next(&m.u.cold, &pos, &f)) {
        char nm[128];
        snprintf(nm, sizeof nm, "%.*s", (int)f.name_len, f.name);
        uint64_t sz = 0, sum = 0;
        CHECK(fsum(L->db, nm, &sz, &sum) == 0 && sz == f.size && sum == f.xxh64);
        part **in = f.kind == MK_FILE_EC ? ec : rest;
        size_t nin = f.kind == MK_FILE_EC ? ne : nr;
        bool hit = false;
        for (size_t i = 0; i < nin; i++)
            if (!strcmp(in[i]->name, nm)) {
                hit = true;
                CHECK(f.header_check == in[i]->index_hash);
            }
        CHECK(hit && (f.kind == MK_FILE_EC || f.kind == MK_FILE_Y));
        nec += f.kind == MK_FILE_EC;
        ny += f.kind == MK_FILE_Y;
    }
    CHECK(nec == c->nobjs && ny == c->ny);
    if (whole)
        CHECK(nr == c->ny);
    unref_all(ec, ne);
    unref_all(rest, nr);
    free(rest);
    free(buf);
}

/* ------------------------------------------------------------ a writer on its own thread */

typedef struct wr {
    pthread_t th;
    eng *db;
    uint32_t s;
    int day, t0, n;
    float base;
    int rc;
    int64_t t_start, t_end;
    _Atomic bool done;
} wr;
static void *wr_run(void *a)
{
    wr *w = a;
    w->t_start = now_ms();
    w->rc = rows(w->db, w->s, w->day, w->t0, w->n, w->base);
    w->t_end = now_ms();
    atomic_store(&w->done, true);
    return NULL;
}
static void wr_start(wr *w, eng *db, uint32_t s, int day, int t0, int n, float base)
{
    *w = (wr){.db = db, .s = s, .day = day, .t0 = t0, .n = n, .base = base};
    atomic_init(&w->done, false);
    pthread_create(&w->th, NULL, wr_run, w);
}
static bool wr_done_within(wr *w, int ms)
{
    for (int i = 0; i < ms / 5 && !atomic_load(&w->done); i++)
        msleep(5);
    return atomic_load(&w->done);
}

/* the settle on its own thread */
typedef struct st {
    pthread_t th;
    eng *db;
    int64_t key;
    int rc;
    _Atomic bool done;
} st;
static void *st_run(void *a)
{
    st *s = a;
    s->rc = settle_key(s->db, s->key);
    atomic_store(&s->done, true);
    return NULL;
}
static void st_start(st *s, eng *db, int64_t key)
{
    *s = (st){.db = db, .key = key};
    atomic_init(&s->done, false);
    pthread_create(&s->th, NULL, st_run, s);
}

/* the case's hook (eccold_test_hook) */
static void (*_Atomic CASE_HOOK)(eng *db, int step);
static void hook(eng *db, int step)
{
    void (*h)(eng *, int) = atomic_load(&CASE_HOOK);
    if (h)
        h(db, step);
}

/* ------------------------------------------------------------ the cases */

/* the cut: L <= Z, F the flushed segment, the marker admitted at its position, the .ec record's ppos Z and stripe
 * version, the rows unchanged, and the same after a reopen */
static void test_cut(void)
{
    lt L;
    if (!start("cut", &L))
        return;
    make_i(&L);
    atomic_store(&SV, 7);
    CHECK(settle_key(L.db, KEY) == 0);
    eccold_cut c;
    eccold_test_last(&c);
    CHECK(c.rc == 0 && c.gated && c.key == KEY && c.nobjs == 1 && c.ny == 0 && !c.flushed);
    CHECK(c.l > 0 && c.z >= c.l && c.pos > c.z && c.stripe_version == 7);
    pthread_mutex_lock(&L.db->lock);
    uint64_t wf = L.db->wal_flushed;
    pthread_mutex_unlock(&L.db->lock);
    CHECK(c.f == wf);
    CHECK(admitted_marker(&L.x, c.pos));
    CHECK(atomic_load(&L.db->lead_commit) >= c.pos);
    part *ec[4];
    size_t n = parts_of(L.db, KEY, ec, 4, true);
    CHECK(n == 1);
    if (n == 1) {
        struct ecpart *e = atomic_load(&ec[0]->ec);
        CHECK(e->rec.ppos == c.z && e->rec.stripe_version == 7);
    }
    unref_all(ec, n);
    part *rest[4];
    size_t nr = parts_of(L.db, KEY, rest, 4, false);
    CHECK(nr == 0);
    unref_all(rest, nr);
    CHECK(holds(L.db, L.s[0], 0, 0, 500, 1.0f));
    CHECK(eng_check(L.db) == 0);
    /* a reopen lists the .ec part and answers the same */
    CHECK(eng_close(L.db) == 0);
    tier_put(L.t);
    L.db = NULL;
    L.t = NULL;
    int rc = lopen(&L);
    CHECK(rc == 0);
    if (rc) { /* nothing below may run on a leader that did not open */
        fprintf(stderr, "cut: the leader did not reopen after the settle (%d)\n", rc);
        lclose(&L);
        return;
    }
    n = parts_of(L.db, KEY, ec, 4, true);
    CHECK(n == 1);
    if (n == 1) {
        CHECK(atomic_load(&ec[0]->ec)->rec.ppos == c.z);
        uint64_t pp = 0; /* the .ec's PPOS record (src/ppos.h), written by the swap before its intent */
        CHECK(ppos_of(L.db, ec[0]->name, &pp) == 0 && pp == c.z);
    }
    unref_all(ec, n);
    CHECK(holds(L.db, L.s[0], 0, 0, 500, 1.0f));
    check_marker(&L, &c, true);
    lclose(&L);
    printf("  cut: L <= Z, F, the marker at its position, the .ec record's ppos Z and stripe version; a reopen\n");
}

/* the rotate: memtable rows of P go to a Y part through a flush at the cut, rows of another day to their own part */
static void test_rotate(void)
{
    lt L;
    if (!start("rotate", &L))
        return;
    make_i(&L);
    CHECK(rows(L.db, L.s[0], 0, 1000, 5, 100.0f) == 0); /* small: the memtable */
    CHECK(rows(L.db, L.s[1], 1, 0, 3, 200.0f) == 0);
    CHECK(settle_key(L.db, KEY) == 0);
    eccold_cut c;
    eccold_test_last(&c);
    CHECK(c.rc == 0 && c.flushed && c.ny == 1);
    char want[32];
    snprintf(want, sizeof want, "p%" PRId64 "_", (int64_t)KEY);
    CHECK(c.ny < 1 || strncmp(c.y[0], want, strlen(want)) == 0);
    pthread_mutex_lock(&L.db->lock);
    uint64_t wf = L.db->wal_flushed;
    bool mt = L.db->active && L.db->active->rows;
    pthread_mutex_unlock(&L.db->lock);
    CHECK(c.f == wf && c.f > 0);
    CHECK(!mt); /* the flush took every row; the other day's went to a part of their own key */
    part *o[4];
    size_t no = parts_of(L.db, KEY + 1, o, 4, false);
    CHECK(no == 1);
    unref_all(o, no);
    part *y[4];
    size_t ny = parts_of(L.db, KEY, y, 4, false); /* Y stays a .part (it is named by the marker, not settled) */
    CHECK(ny == 1);
    unref_all(y, ny);
    qctx *q = query(L.db, L.s[0], 0);
    CHECK(q->n == 505);
    for (int i = 0; q->n == 505 && i < 5; i++)
        CHECK(q->ts[500 + i] == DAY0 + 1000LL * (1000 + i) && q->v[500 + i] == 100.0f + (float)i);
    free(q);
    CHECK(holds(L.db, L.s[1], 1, 0, 3, 200.0f));
    CHECK(eng_check(L.db) == 0);
    {
        eccold_cut cm;
        eccold_test_last(&cm);
        CHECK(cm.ny > 0);
        check_marker(&L, &cm, true);
    }
    lclose(&L);
    printf("  rotate: P's memtable rows flushed at the cut into Y, another day's into its own part\n");
}

/* the gate: at GATED, a small write of s1 (no row in I) into P waits until the settle is done; one into day 2 does not */
static wr G_IN, G_OUT;
static _Atomic int G_BLOCKED, G_OUTFAST;
static void gate_hook(eng *db, int step)
{
    if (step != ECCOLD_STEP_GATED)
        return;
    wr_start(&G_IN, db, 1, 0, 2000, 1, 9.0f);
    wr_start(&G_OUT, db, 1, 2, 0, 1, 8.0f);
    atomic_store(&G_OUTFAST, wr_done_within(&G_OUT, 2000));
    msleep(300);
    atomic_store(&G_BLOCKED, !atomic_load(&G_IN.done));
}
static void test_gate(void)
{
    lt L;
    if (!start("gate", &L))
        return;
    make_i(&L);
    atomic_store(&CASE_HOOK, gate_hook);
    CHECK(settle_key(L.db, KEY) == 0);
    atomic_store(&CASE_HOOK, NULL);
    pthread_join(G_IN.th, NULL);
    pthread_join(G_OUT.th, NULL);
    CHECK(atomic_load(&G_BLOCKED));
    CHECK(atomic_load(&G_OUTFAST));
    CHECK(G_IN.rc == 0 && G_OUT.rc == 0);
    CHECK(holds(L.db, L.s[1], 0, 2000, 1, 9.0f));
    CHECK(holds(L.db, L.s[1], 2, 0, 1, 8.0f));
    CHECK(holds(L.db, L.s[0], 0, 0, 500, 1.0f));
    CHECK(eng_check(L.db) == 0);
    lclose(&L);
    printf("  gate: a small write of another series into P waits for the settle; one into another day does not\n");
}

/* the wait: the commit point held below the log end (a write of another day), Z is taken only once it passed L */
static lt *W_L;
static uint64_t W_LG, W_CUTCOMMIT;
static _Atomic bool W_RELEASED, W_STOP;
static _Atomic int W_RELEASED_AT_CUT = -1;
static pthread_t W_REL;
static void *releaser(void *a)
{
    eng *db = a;
    msleep(300);
    atomic_store(&W_RELEASED, true);
    while (!atomic_load(&W_STOP)) {
        eng_leader_commit(db, ld_last(db));
        msleep(2);
    }
    return NULL;
}
static wr W_W2;
static void wait_hook(eng *db, int step)
{
    if (step == ECCOLD_STEP_GATED) {
        W_LG = ld_last(db);
        wr_start(&W_W2, db, 2, 2, 0, 1, 9.0f); /* another day: outside the fence, appended, waits for the commit */
        for (int i = 0; i < 400 && ld_last(db) == W_LG; i++)
            msleep(5);
        pthread_create(&W_REL, NULL, releaser, db);
    } else if (step == ECCOLD_STEP_CUT) {
        W_CUTCOMMIT = atomic_load(&db->lead_commit);
        atomic_store(&W_RELEASED_AT_CUT, atomic_load(&W_RELEASED));
    }
}
static void test_wait(void)
{
    lt L;
    if (!start("wait", &L))
        return;
    W_L = &L;
    make_i(&L);
    atomic_store(&L.x.manual, true);
    uint64_t before = ld_last(L.db);
    wr w;
    wr_start(&w, L.db, L.s[1], 1, 0, 1, 5.0f); /* appended, waits for the commit point */
    for (int i = 0; i < 400 && ld_last(L.db) == before; i++)
        msleep(5);
    CHECK(ld_last(L.db) > before && !atomic_load(&w.done));
    atomic_store(&CASE_HOOK, wait_hook);
    CHECK(settle_key(L.db, KEY) == 0);
    atomic_store(&CASE_HOOK, NULL);
    atomic_store(&W_STOP, true);
    pthread_join(W_REL, NULL);
    atomic_store(&L.x.manual, false);
    eng_leader_commit(L.db, ld_last(L.db));
    pthread_join(w.th, NULL);
    pthread_join(W_W2.th, NULL);
    CHECK(w.rc == 0 && W_W2.rc == 0);
    CHECK(atomic_load(&W_RELEASED_AT_CUT) == 1);
    CHECK(W_CUTCOMMIT >= W_LG);
    eccold_cut c;
    eccold_test_last(&c);
    CHECK(c.rc == 0 && c.l == W_LG && c.z > c.l); /* the write into day 2 was appended after L, before Z */
    CHECK(eng_check(L.db) == 0);
    check_marker(&L, &c, true); /* the record's ppos and the marker's z are Z, not L */
    lclose(&L);
    printf("  wait: Z is taken only once the commit point passed the log end after the gate\n");
}

/* a position DOWN: before the gate the settle waits wait_max_s without a fence (a small write into P goes through at
 * once), gives up UNAVAILABLE and its objects go to r7-gc; DOWN at the re-check after the gate likewise; UP again: 0 */
static void down_hook(eng *db, int step)
{
    (void)db;
    if (step == ECCOLD_STEP_GATED)
        atomic_store(&DOWN, true);
}
static void putdown_hook(eng *db, int step)
{
    (void)db;
    if (step == ECCOLD_STEP_PUT)
        atomic_store(&DOWN, true);
}
static void test_down(void)
{
    lt L;
    if (!start("down", &L))
        return;
    make_i(&L);
    eccold_cut c;
    /* DOWN before the settle: given up at once, nothing put, the tier's thread does not wait */
    atomic_store(&DOWN, true);
    int64_t t0 = now_ms();
    CHECK(settle_key(L.db, KEY) == TSDB_ERR_UNAVAILABLE && now_ms() - t0 < 500 && chunks(&L) == 0);
    eccold_test_last(&c);
    CHECK(!c.gated && c.nobjs == 0);
    atomic_store(&DOWN, false);
    /* DOWN after the puts: waited for wait_max_s without a fence */
    atomic_store(&CASE_HOOK, putdown_hook);
    st s;
    t0 = now_ms();
    st_start(&s, L.db, KEY);
    msleep(150);
    wr w;
    wr_start(&w, L.db, L.s[1], 0, 3000, 1, 4.0f);
    pthread_join(w.th, NULL);
    pthread_join(s.th, NULL);
    atomic_store(&CASE_HOOK, NULL);
    int64_t took = now_ms() - t0;
    CHECK(w.rc == 0);
    CHECK(w.t_end - w.t_start < 500); /* stripewaitfenced: it waits for the whole stripe wait (1 s) */
    CHECK(s.rc == TSDB_ERR_UNAVAILABLE && took >= 900);
    eccold_test_last(&c);
    CHECK(c.rc == TSDB_ERR_UNAVAILABLE && !c.gated);
    atomic_store(&DOWN, false);
    CHECK(chunks_go(&L, 0)); /* nothing names the objects: r7-gc takes them */
    /* DOWN at the re-check after the gate */
    atomic_store(&CASE_HOOK, down_hook);
    CHECK(settle_key(L.db, KEY) == TSDB_ERR_UNAVAILABLE);
    atomic_store(&CASE_HOOK, NULL);
    eccold_test_last(&c);
    CHECK(c.gated && c.rc == TSDB_ERR_UNAVAILABLE);
    atomic_store(&DOWN, false);
    CHECK(chunks_go(&L, 0));
    part *ec[4];
    size_t n = parts_of(L.db, KEY, ec, 4, true);
    CHECK(n == 0);
    unref_all(ec, n);
    /* up again: the retry settles */
    CHECK(settle_key(L.db, KEY) == 0);
    n = parts_of(L.db, KEY, ec, 4, true);
    CHECK(n == 1);
    unref_all(ec, n);
    CHECK(holds(L.db, L.s[1], 0, 3000, 1, 4.0f));
    CHECK(holds(L.db, L.s[0], 0, 0, 500, 1.0f));
    CHECK(eng_check(L.db) == 0);
    lclose(&L);
    printf("  down: no fence while a position is DOWN, UNAVAILABLE after wait_max_s, the objects to r7-gc; the re-check\n");
}

/* the TTL pass between the cut and the swap leaves a key with a settling part alone */
static _Atomic int T_DROPPED = -1, T_SEL = -1;
static void ttl_hook(eng *db, int step)
{
    if (step != ECCOLD_STEP_CUT)
        return;
    part *sel[MAX_MERGE_PARTS];
    bool dropped = false, sync_wal = false;
    char intent[64];
    pthread_mutex_lock(&db->lock);
    db->last_ttl_ms = 0;
    size_t k = ttl_select_locked(db, sel, &dropped, &sync_wal, intent);
    for (size_t i = 0; i < k; i++)
        sel[i]->merging = false;
    pthread_mutex_unlock(&db->lock);
    unref_all(sel, k);
    atomic_store(&T_DROPPED, dropped);
    atomic_store(&T_SEL, (int)k);
}
static void test_ttl(void)
{
    lt L;
    if (!start("ttl", &L))
        return;
    make_i(&L);
    CHECK(eng_set_retention(L.db, L.s[0], 2 * DAY_MS) == 0); /* P ended 9 days before CLK */
    atomic_store(&CASE_HOOK, ttl_hook);
    int rc = settle_key(L.db, KEY);
    atomic_store(&CASE_HOOK, NULL);
    CHECK(rc == 0);
    CHECK(atomic_load(&T_DROPPED) == 0 && atomic_load(&T_SEL) == 0);
    CHECK(eng_check(L.db) == 0);
    lclose(&L);
    printf("  ttl: the TTL pass skips a key with a settling part in a cluster\n");
}

/* deposed in the wait for the commit point: INDETERMINATE, the engine not failed */
static void *deposer(void *a)
{
    msleep(200);
    eng_leader_depose(a);
    return NULL;
}
static pthread_t D_TH;
static void depose_hook(eng *db, int step)
{
    if (step == ECCOLD_STEP_GATED)
        pthread_create(&D_TH, NULL, deposer, db);
}
static void test_depose(void)
{
    lt L;
    if (!start("depose", &L))
        return;
    make_i(&L);
    atomic_store(&L.x.manual, true);
    uint64_t before = ld_last(L.db);
    wr w;
    wr_start(&w, L.db, L.s[1], 1, 0, 1, 5.0f);
    for (int i = 0; i < 400 && ld_last(L.db) == before; i++)
        msleep(5);
    atomic_store(&CASE_HOOK, depose_hook);
    int rc = settle_key(L.db, KEY);
    atomic_store(&CASE_HOOK, NULL);
    pthread_join(D_TH, NULL);
    pthread_join(w.th, NULL);
    CHECK(rc == TSDB_ERR_INDETERMINATE);
    CHECK(eng_check(L.db) == 0);
    part *ec[4];
    size_t n = parts_of(L.db, KEY, ec, 4, true);
    CHECK(n == 0);
    unref_all(ec, n);
    lclose(&L);
    printf("  depose: deposed in the wait for the commit point: INDETERMINATE, the engine not failed\n");
}

/* large inserts, replaces, deletes and small writes into P while the settle runs: every acknowledged write is seen */
typedef struct cw {
    pthread_t th;
    eng *db;
    int kind;
    _Atomic bool *stop;
    int last, errs;
} cw;
static void *cw_run(void *a)
{
    cw *c = a;
    for (int v = 1; !atomic_load(c->stop) || v < 4; v++) {
        int rc = 0;
        int64_t ts[40];
        float val[40];
        for (int i = 0; i < 40; i++)
            ts[i] = DAY0 + 1000LL * (5000 + i), val[i] = (float)(v * 100 + i);
        switch (c->kind) {
        case 0: /* large insert over the same 40 times of s2 */
            rc = eng_insert_series(c->db, TSDB_FLOAT32, 2, ts, val, 40);
            break;
        case 1: /* replace the range of s3 with 20 rows */
            rc = eng_replace_range(c->db, TSDB_FLOAT32, 3, ts[0], ts[39] + 1, ts, val, 20);
            break;
        case 2: /* 40 rows of s4, then the first 10 deleted */
            rc = eng_insert_series(c->db, TSDB_FLOAT32, 4, ts, val, 40);
            if (rc == 0)
                rc = eng_delete_range(c->db, 4, ts[0], ts[10]); /* [start, end) */
            break;
        default: /* one row of s5 at a new time */
            ts[0] = DAY0 + 1000LL * (6000 + v), val[0] = (float)v;
            rc = eng_insert_series(c->db, TSDB_FLOAT32, 5, ts, val, 1);
            break;
        }
        if (rc)
            c->errs++;
        else
            c->last = v;
        if (c->kind == 3 && v > 3000)
            break;
    }
    return NULL;
}
static bool model(eng *db, uint32_t s, int lo, int n, int v, int i0)
{
    qctx *q = calloc(1, sizeof *q);
    eng_query_raw_ex(db, s, DAY0 + 1000LL * 5000, DAY0 + 1000LL * 5999, false, 0, qfn, q);
    bool ok = q->n == (size_t)n;
    for (int i = 0; ok && i < n; i++)
        ok = q->ts[i] == DAY0 + 1000LL * (5000 + lo + i) && q->v[i] == (float)(v * 100 + i0 + i);
    if (!ok)
        fprintf(stderr, "series %u: %zu rows, wanted %d of version %d\n", s, q->n, n, v);
    free(q);
    return ok;
}
static void conc_hook(eng *db, int step)
{
    (void)db;
    if (step == ECCOLD_STEP_PUT || step == ECCOLD_STEP_GATED)
        msleep(150); /* before the gate the writers go on (late parts: Y); inside it they wait */
}
static void test_concurrent(void)
{
    lt L;
    if (!start("concurrent", &L))
        return;
    make_i(&L);
    _Atomic bool stop = false;
    cw c[4];
    for (int k = 0; k < 4; k++) {
        c[k] = (cw){.db = L.db, .kind = k, .stop = &stop};
        pthread_create(&c[k].th, NULL, cw_run, &c[k]);
    }
    msleep(50);
    atomic_store(&CASE_HOOK, conc_hook);
    int rc = settle_key(L.db, KEY);
    atomic_store(&CASE_HOOK, NULL);
    atomic_store(&stop, true);
    for (int k = 0; k < 4; k++)
        pthread_join(c[k].th, NULL);
    CHECK(rc == 0);
    for (int k = 0; k < 4; k++)
        CHECK(c[k].errs == 0);
    CHECK(model(L.db, 2, 0, 40, c[0].last, 0));
    CHECK(model(L.db, 3, 0, 20, c[1].last, 0));
    CHECK(model(L.db, 4, 10, 30, c[2].last, 10));
    qctx *q = calloc(1, sizeof *q);
    eng_query_raw_ex(L.db, 5, DAY0 + 1000LL * 6001, DAY0 + 1000LL * (6000 + c[3].last) + 1, false, 0, qfn, q);
    CHECK(q->n == (size_t)c[3].last);
    free(q);
    CHECK(holds(L.db, L.s[0], 0, 0, 500, 1.0f));
    CHECK(eng_check(L.db) == 0);
    eccold_cut cut;
    eccold_test_last(&cut);
    check_marker(&L, &cut, false);
    lclose(&L);
    printf("  concurrent: large inserts, replaces, deletes and small writes into P during a settle (Y: %zu parts, "
           "the gate held %lld us)\n",
           cut.ny, (long long)cut.hold_us);
}

/* the policy's cluster branch: a whole key once its end is past the grace by the replicated clock, never level 0,
 * never a key that expires within keep + retain + 1 h, never on a diverged engine */
static size_t pick(eng *db, int64_t now, int64_t *key)
{
    part *sel[64];
    pthread_mutex_lock(&db->lock);
    size_t n = ecpolicy_pick_locked(db, now, key, sel, 64);
    pthread_mutex_unlock(&db->lock);
    unref_all(sel, n);
    return n;
}
static void test_policy(void)
{
    lt L;
    if (!start("policy", &L))
        return;
    int64_t end = DAY0 + DAY_MS, g = GRACE_S * 1000, force = 3600 * 1000LL, key = -1;
    CHECK(rows(L.db, L.s[0], 0, 0, 500, 1.0f) == 0);
    CHECK(pick(L.db, end + g + force + 1000, &key) == 0); /* D18: only an insert-written part */
    CHECK(eng_optimize(L.db) == 0);
    CHECK(pick(L.db, end + g - 1000, &key) == 0); /* not past the grace */
    CHECK(pick(L.db, end + g + 1000, &key) == 1 && key == KEY);
    L.db->diverged = true;
    CHECK(pick(L.db, end + g + 1000, &key) == 0);
    L.db->diverged = false;
    /* rows that expire within keep_s + retain + 1 h of now are never settled */
    CHECK(eng_set_retention(L.db, L.s[0], 40 * DAY_MS) == 0);
    CHECK(pick(L.db, end + 40 * DAY_MS - 1000, &key) == 0);
    CHECK(pick(L.db, end + g + 1000, &key) == 1);
    /* the margin, keep_s 3600 s + retain 1800 s (no keep cap) + 1 h: a row that expires a minute past it is settled,
     * a minute before it not (margin0, noretain); ttl_any = DAY0 + R */
    int64_t now = end + g + 1000, margin = (3600 + 1800 + 3600) * 1000LL;
    CHECK(eng_set_retention(L.db, L.s[0], now + margin + 60000 - DAY0) == 0);
    CHECK(pick(L.db, now, &key) == 1);
    CHECK(eng_set_retention(L.db, L.s[0], now + margin - 60000 - DAY0) == 0);
    CHECK(pick(L.db, now, &key) == 0);
    CHECK(eng_set_retention(L.db, L.s[0], 40 * DAY_MS) == 0);
    CHECK(pick(L.db, now, &key) == 1);
    /* the natural flush: memtable rows of P hold the key back until grace + force_s (nonatflush) */
    CHECK(rows(L.db, L.s[1], 0, 2000, 5, 9.0f) == 0);
    CHECK(pick(L.db, now, &key) == 0);
    CHECK(pick(L.db, end + g + force + 1000, &key) == 1);
    /* more than one marker names: none, said once, and the key waits an hour */
    ecpolicy_test_cap_hit = true;
    CHECK(pick(L.db, end + g + force + 2000, &key) == 0);
    ecpolicy_test_cap_hit = false;
    CHECK(pick(L.db, end + g + force + 3000, &key) == 0);
    CHECK(pick(L.db, end + g + force + 3 * 3600 * 1000LL, &key) == 1);
    CHECK(eng_check(L.db) == 0);
    /* deposed: no settle (nodeposed) */
    eng_leader_depose(L.db);
    CHECK(settle_key(L.db, KEY) == TSDB_ERR_INVALID && chunks(&L) == 0);
    lclose(&L);
    printf("  policy: whole keys past the grace, never level 0, never about to expire, never diverged\n");
}

/* the swap refused after its COLD marker committed: the engine fails (the replicas carry the marker out), and once the
 * hook says a marker names the objects (r8h's ecapply_swap_pre; ecswap_test_logged until then) nothing goes to r7-gc */
static lt *CUR_L;           /* the case's leader, for a hook that counts chunk files */
static _Atomic int NC_HOOK; /* the chunk files when the hook ran */
static void swapfail_hook(eng *db, int step)
{
    (void)db;
    if (step == ECCOLD_STEP_MARKER) {
        atomic_store(&NC_HOOK, chunks(CUR_L));
        atomic_store(&ecswap_test_fail, ECSWAP_FAIL_INTENT_IO);
    }
}
static void test_swapfail(void)
{
    for (int logged = 0; logged < 2; logged++) {
        lt L;
        if (!start(logged ? "swapfail-logged" : "swapfail", &L))
            return;
        make_i(&L);
        CUR_L = &L;
        atomic_store(&ecswap_test_logged, logged != 0);
        atomic_store(&CASE_HOOK, swapfail_hook);
        int rc = settle_key(L.db, KEY);
        atomic_store(&CASE_HOOK, NULL);
        atomic_store(&ecswap_test_fail, ECSWAP_FAIL_NONE);
        atomic_store(&ecswap_test_logged, false);
        CHECK(rc == TSDB_ERR_IO);
        CHECK(eng_check(L.db) != 0); /* noswapfail: the leader would go on as if the marker had not been written */
        part *ec[4];
        size_t n = parts_of(L.db, KEY, ec, 4, true);
        CHECK(n == 0);
        unref_all(ec, n);
        if (logged) {
            for (int i = 0; i < 10; i++) {
                graveyard_purge(L.db, false);
                ecgc_tier_work(L.db->tier);
                msleep(20);
            }
            int nc = chunks(&L);
            CHECK(nc > 0 && nc == atomic_load(&NC_HOOK)); /* the object the committed marker names stays */
            if (nc != atomic_load(&NC_HOOK))
                fprintf(stderr, "swapfail: %d chunk files\n", nc);
        }
        lclose(&L);
    }
    printf("  swapfail: a swap refused after the marker committed fails the engine; a logged one keeps its object\n");
}

/* deposed right after the append (jr_cold's hook), before the commit: INDETERMINATE, the engine not failed, the
 * objects kept (a marker may name them: the reopen as a replica decides) */
static _Atomic int DA_SEEN = -1;
static void da_marker(eng *db, uint64_t fno)
{
    atomic_store(&DA_SEEN, atomic_load(&db->lead_commit) < fno);
    atomic_store(&NC_HOOK, chunks(CUR_L));
    eng_leader_depose(db);
}
static void test_deposeappend(void)
{
    lt L;
    if (!start("deposeappend", &L))
        return;
    make_i(&L);
    atomic_store(&L.x.manual, true);
    CUR_L = &L;
    atomic_store(&eccold_marker_hook, da_marker);
    int rc = settle_key(L.db, KEY);
    atomic_store(&eccold_marker_hook, NULL);
    CHECK(atomic_load(&DA_SEEN) == 1); /* the hook ran after the append and before the commit */
    CHECK(rc == TSDB_ERR_INDETERMINATE);
    CHECK(eng_check(L.db) == 0);
    part *ec[4];
    size_t n = parts_of(L.db, KEY, ec, 4, true);
    CHECK(n == 0);
    unref_all(ec, n);
    for (int i = 0; i < 10; i++) {
        graveyard_purge(L.db, false);
        ecgc_tier_work(L.db->tier);
        msleep(20);
    }
    int nc = chunks(&L);
    CHECK(nc > 0 && nc == atomic_load(&NC_HOOK)); /* indetgc: handed to r7-gc although a marker may name it */
    if (nc != atomic_load(&NC_HOOK))
        fprintf(stderr, "deposeappend: %d chunk files\n", nc);
    CHECK(holds(L.db, L.s[0], 0, 0, 500, 1.0f));
    lclose(&L);
    printf("  deposeappend: deposed between the append and the commit: INDETERMINATE, the object kept\n");
}

/* killed (as kill -9) right after the append, and right after the commit before the swap: the reopen as leader opens
 * and answers the same rows from the source part (the swap never ran) */
static void crash_marker(eng *db, uint64_t fno) { (void)db, (void)fno, _exit(99); }
static void crash_hook(eng *db, int step)
{
    (void)db;
    if (step == ECCOLD_STEP_MARKER)
        _exit(99);
}
/* the child of test_crash: reopen the leader in root and settle until the planted kill */
static int crash_child(int at, const char *root)
{
    lt L;
    lt_paths(&L, root);
    if (lopen(&L) != 0)
        return 2;
    if (at)
        atomic_store(&CASE_HOOK, crash_hook);
    else
        atomic_store(&eccold_marker_hook, crash_marker);
    settle_key(L.db, KEY);
    _exit(3);
}
static void test_crash(void)
{
    for (int at = 0; at < 2; at++) {
        lt L;
        if (!start(at ? "crash-commit" : "crash-append", &L))
            return;
        make_i(&L);
        CHECK(eng_close(L.db) == 0);
        tier_put(L.t);
        L.db = NULL;
        L.t = NULL;
        fflush(NULL);
        char mode[2] = {(char)('0' + at), 0};
        char *argv[] = {"test_eccold", S, "--crash-child", mode, L.root, NULL};
        pid_t pid = fork(); /* a new image: a fork of this threaded process alone could inherit a held lock */
        if (pid == 0) {
            execv("/proc/self/exe", argv);
            _exit(4);
        }
        int ws = 0;
        CHECK(pid > 0 && waitpid(pid, &ws, 0) == pid);
        CHECK(WIFEXITED(ws) && WEXITSTATUS(ws) == 99);
        if (!WIFEXITED(ws) || WEXITSTATUS(ws) != 99)
            fprintf(stderr, "crash: the child's status %#x\n", ws);
        int rc = lopen(&L);
        CHECK(rc == 0);
        if (rc) {
            fprintf(stderr, "crash: the leader did not open after the kill (%d)\n", rc);
            lclose(&L);
            return;
        }
        CHECK(eng_check(L.db) == 0);
        part *ec[4];
        size_t n = parts_of(L.db, KEY, ec, 4, true);
        CHECK(n == 0);
        unref_all(ec, n);
        CHECK(holds(L.db, L.s[0], 0, 0, 500, 1.0f));
        for (int i = 0; i < 10; i++) {
            graveyard_purge(L.db, false);
            ecgc_tier_work(L.db->tier);
            msleep(20);
        }
        /* not checked: the open's orphan sweep (ecgc.c) takes an object that only a COLD marker names, also one that
         * committed (crash-commit: the replicas list its .ec parts); rolling the COLD forward at the open is r8h's */
        printf("  crash-%s: chunk files after the reopen: %d\n", at ? "commit" : "append", chunks(&L));
        lclose(&L);
    }
    printf("  crash: killed after the append and after the commit: the leader reopens with the rows unchanged\n");
}

/* swappower: crashfs power loss at every step of a leader's settle of P (the swap's PPOS records, intent v4, renames),
 * of a drop of P's .ec part (retired_ names) and of the open that then compacts PPOS (the records of the dropped names
 * are dead, the one of a part of another day lives). The next open as leader must succeed: every listed part, the .ec
 * one included, has its PPOS record (src/ppos.h); P's rows are all there or, once a drop began, all gone. Mutant
 * pposswapafter (the .ec records after the renames) and the merged bug pposswapnone must fail it. */
static char SHIM[PATH_MAX];
/* S/ack.<root's last name>.what: no name crashfs takes as under CRASHFS_ROOT (a prefix match: never root.what) */
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
/* the child under crashfs (the acks lie outside its root, so they are what the child saw acknowledged) */
static int power_child(const char *root)
{
    lt L;
    lt_paths(&L, root);
    if (lopen(&L) != 0)
        _exit(2);
    if (settle_key(L.db, KEY) != 0)
        _exit(5);
    power_ack(root, "settled");
    if (eng_drop_before(L.db, DAY0 + DAY_MS) != 0)
        _exit(6);
    power_ack(root, "dropped");
    if (eng_close(L.db) != 0)
        _exit(7);
    tier_put(L.t);
    L.db = NULL, L.t = NULL;
    if (lopen(&L) != 0) /* PPOS compacts: three records of four are dead */
        _exit(8);
    if (eng_close(L.db) != 0)
        _exit(7);
    tier_put(L.t);
    _exit(0);
}
static int power_run(const char *root, int warmup)
{
    fflush(NULL);
    char *argv[] = {"test_eccold", S, "--power-child", (char *)root, NULL};
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512], w[16], sd[16];
        snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0", getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
        setenv("ASAN_OPTIONS", asan, 1);
        snprintf(w, sizeof w, "%d", warmup);
        snprintf(sd, sizeof sd, "%d", warmup + 1);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", root, 1);
        setenv("CRASHFS_PROB", "1", 1);
        setenv("CRASHFS_WARMUP", w, 1);
        setenv("CRASHFS_AT_EXIT", "1", 1);
        setenv("CRASHFS_TEAR", "1", 1);
        setenv("CRASHFS_SEED", sd, 1);
        int null = getenv("TEST_ECCOLD_POWER_LOG") ? -1 : open("/dev/null", O_WRONLY);
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
static void test_swappower(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  swappower: skipped (no %s)\n", SHIM);
        return;
    }
    int crashes = 0, settled = 0, dropped = 0, f0 = failures;
    bool finished = false;
    int k0 = getenv("TEST_ECCOLD_POWER_K") ? atoi(getenv("TEST_ECCOLD_POWER_K")) : 0; /* to look at one step */
    for (int k = k0; k < 4000 && !finished && failures == f0; k++) {
        alarm(240); /* per step: a hang still ends the run */
        lt L;
        if (!start("swappower", &L))
            return;
        make_i(&L);
        CHECK(rows(L.db, L.s[1], 2, 0, 500, 3.0f) == 0); /* a part of another day: its record outlives the compaction */
        CHECK(eng_close(L.db) == 0);
        tier_put(L.t);
        L.db = NULL, L.t = NULL;
        power_unack(L.root, "settled");
        power_unack(L.root, "dropped");
        int code = power_run(L.root, k);
        CHECK(code == 99 || code == 0);
        if (code != 99 && code != 0) {
            fprintf(stderr, "swappower: the child at step %d exited %d\n", k, code);
            lclose(&L);
            return;
        }
        crashes += code == 99;
        finished = code == 0;
        bool st = power_acked(L.root, "settled"), dr = power_acked(L.root, "dropped");
        settled += st, dropped += dr;
        power_unack(L.root, "settled");
        power_unack(L.root, "dropped");
        int rc = lopen(&L);
        CHECK(rc == 0);
        if (rc) {
            fprintf(stderr, "swappower: after a power loss at step %d the leader does not open (%d)%s%s\n", k, rc,
                    st ? ", settled" : "", dr ? ", dropped" : "");
            lclose(&L);
            return;
        }
        CHECK(eng_check(L.db) == 0);
        CHECK(holds(L.db, L.s[1], 2, 0, 500, 3.0f));
        qctx *q = query(L.db, L.s[0], 0);
        CHECK(q->n == (dr ? 0 : 500) || (st && q->n == 0)); /* a drop is all or nothing */
        if (!(q->n == (dr ? 0 : 500) || (st && q->n == 0)))
            fprintf(stderr, "swappower: step %d: %zu rows of P%s%s\n", k, q->n, st ? ", settled" : "",
                    dr ? ", dropped" : "");
        if (q->n == 500)
            CHECK(holds(L.db, L.s[0], 0, 0, 500, 1.0f));
        free(q);
        pthread_mutex_lock(&L.db->lock);
        for (size_t i = 0; i < L.db->nparts; i++) {
            uint64_t pp = 0;
            CHECK(ppos_of(L.db, L.db->parts[i]->name, &pp) == 0); /* the open checked it; so does this */
            struct ecpart *e = atomic_load(&L.db->parts[i]->ec);
            if (e)
                CHECK(pp == e->rec.ppos);
        }
        pthread_mutex_unlock(&L.db->lock);
        lclose(&L);
    }
    CHECK(finished && crashes >= 50 && settled > 0 && dropped > 0);
    printf("  swappower: power lost at each of %d steps of a settle, a drop of its .ec and a compacting open (%d after "
           "the settle, %d after the drop): the leader reopens, every listed part with its PPOS record\n",
           crashes, settled, dropped);
}

/* a drop of P between the put and the gate: STOPPED, nothing listed, the objects to r7-gc */
static void drop_hook(eng *db, int step)
{
    if (step == ECCOLD_STEP_PUT)
        CHECK(eng_drop_before(db, DAY0 + DAY_MS) == 0);
}
static void test_drop(void)
{
    lt L;
    if (!start("drop", &L))
        return;
    make_i(&L);
    atomic_store(&CASE_HOOK, drop_hook);
    int rc = settle_key(L.db, KEY);
    atomic_store(&CASE_HOOK, NULL);
    CHECK(rc == TSDB_ERR_STOPPED); /* inotlisted: the swap is refused after the marker and the engine fails */
    CHECK(eng_check(L.db) == 0);
    part *ec[4];
    size_t n = parts_of(L.db, KEY, ec, 4, true);
    CHECK(n == 0);
    unref_all(ec, n);
    CHECK(holds(L.db, L.s[0], 0, 0, 0, 1.0f));
    CHECK(chunks_go(&L, 0));
    lclose(&L);
    printf("  drop: P dropped between the put and the gate: STOPPED, the object to r7-gc\n");
}

/* a failed put, .ec write or .ec open: IO, the engine not failed, nothing listed, the objects to r7-gc; a retry settles */
static void test_indexfail(void)
{
    static const int at[] = {SETTLE_FAIL_PUT, SETTLE_FAIL_INDEX, SETTLE_FAIL_OPEN};
    for (size_t k = 0; k < sizeof at / sizeof at[0]; k++) {
        lt L;
        if (!start("indexfail", &L))
            return;
        make_i(&L);
        atomic_store(&settle_test_fail_skip, 0);
        atomic_store(&settle_test_fail_at, at[k]);
        int rc = settle_key(L.db, KEY);
        CHECK(atomic_load(&settle_test_fail_at) == SETTLE_FAIL_NONE); /* the step that failed took it */
        atomic_store(&settle_test_fail_at, SETTLE_FAIL_NONE);
        CHECK(rc == TSDB_ERR_IO);
        CHECK(eng_check(L.db) == 0);
        part *ec[4];
        size_t n = parts_of(L.db, KEY, ec, 4, true);
        CHECK(n == 0);
        unref_all(ec, n);
        CHECK(chunks_go(&L, 0));
        CHECK(settle_key(L.db, KEY) == 0);
        n = parts_of(L.db, KEY, ec, 4, true);
        CHECK(n == 1);
        unref_all(ec, n);
        CHECK(holds(L.db, L.s[0], 0, 0, 500, 1.0f));
        lclose(&L);
    }
    printf("  indexfail: a failed put, .ec write or open: IO, the objects to r7-gc, a retry settles\n");
}

/* a merge of a late part in flight at the cut: STOPPED (its output would be another Y); once it is done: settled */
static part *YM_P;
static void ym_hook(eng *db, int step)
{
    if (step != ECCOLD_STEP_PUT || !YM_P)
        return;
    pthread_mutex_lock(&db->lock);
    YM_P->merging = true;
    pthread_mutex_unlock(&db->lock);
}
static void test_ymerging(void)
{
    lt L;
    if (!start("ymerging", &L))
        return;
    make_i(&L);
    CHECK(rows(L.db, L.s[1], 0, 0, 20, 3.0f) == 0); /* a level-0 part of P: never settled (D18), so a Y */
    part *all[8];
    size_t na = parts_of(L.db, KEY, all, 8, false);
    YM_P = NULL;
    for (size_t i = 0; i < na; i++)
        if (all[i]->level == 0)
            YM_P = all[i];
    CHECK(na == 2 && YM_P);
    atomic_store(&CASE_HOOK, ym_hook);
    int rc = settle_key(L.db, KEY);
    atomic_store(&CASE_HOOK, NULL);
    CHECK(rc == TSDB_ERR_STOPPED); /* ymergeok */
    if (YM_P) {
        pthread_mutex_lock(&L.db->lock);
        YM_P->merging = false;
        pthread_mutex_unlock(&L.db->lock);
    }
    YM_P = NULL;
    unref_all(all, na);
    CHECK(eng_check(L.db) == 0);
    CHECK(chunks_go(&L, 0));
    CHECK(settle_key(L.db, KEY) == 0);
    eccold_cut c;
    eccold_test_last(&c);
    CHECK(c.rc == 0 && c.ny == 1);
    CHECK(holds(L.db, L.s[1], 0, 0, 20, 3.0f));
    CHECK(holds(L.db, L.s[0], 0, 0, 500, 1.0f));
    lclose(&L);
    printf("  ymerging: a late part merging at the cut: STOPPED; afterwards settled with it as Y\n");
}

static void alarm_exit(int sig)
{
    (void)sig;
    static const char m[] = "test_eccold: timed out (a hang)\n";
    if (write(2, m, sizeof m - 1) < 0) {
    }
    _exit(3);
}

int main(int argc, char **argv)
{
    const char *only = NULL, *crash_root = NULL, *power_root = NULL;
    int crash_at = -1;
    snprintf(S, sizeof S, "/tmp");
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--mutant") && i + 1 < argc)
            mutant = argv[++i];
        else if (!strcmp(argv[i], "--crash-child") && i + 2 < argc)
            crash_at = atoi(argv[++i]), crash_root = argv[++i];
        else if (!strcmp(argv[i], "--power-child") && i + 1 < argc)
            power_root = argv[++i];
        else if (!strcmp(argv[i], "--case") && i + 1 < argc)
            only = argv[++i];
        else
            snprintf(S, sizeof S, "%s", argv[i]);
    }
    /* the planted bugs of the swap's PPOS records (src/ecapply.c; ppos.h): the children inherit them through the
     * environment, which ppos_open reads */
    if (!strcmp(mutant, "pposswapnone") || !strcmp(mutant, "pposswapafter")) {
        char v[16];
        snprintf(v, sizeof v, "%d", !strcmp(mutant, "pposswapnone") ? PPOS_MUT_SWAPNONE : PPOS_MUT_SWAPAFTER);
        setenv("TSDB_PPOS_MUTANT", v, 1);
    } else if (strcmp(mutant, "none")) {
        int m = eccold_mutant_parse(mutant);
        if (m < 0) {
            fprintf(stderr, "test_eccold: unknown mutant %s\n", mutant);
            return 2;
        }
        eccold_mutant = m;
    }
    signal(SIGALRM, alarm_exit);
    alarm(240);
    meta_cluster_open_allow(true);
    WRAP = coldops_dir; /* the directory ops, with DOWN and the stripe version the test sets */
    WRAP.all_up = w_all_up;
    WRAP.stripe_version = w_sv;
    atomic_store(&eccold_test_hook, hook);
    leader_test_marker_hook = eccold_after_append; /* as tsdbd's repld.c: eccold_marker_hook right after the append */
    if (crash_root)
        return crash_child(crash_at, crash_root);
    if (power_root)
        return power_child(power_root);
    char self[PATH_MAX];
    ssize_t sl = readlink("/proc/self/exe", self, sizeof self - 1);
    self[sl > 0 ? sl : 0] = '\0';
    char *slash = strrchr(self, '/');
    snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", slash ? (int)(slash - self) : 1, slash ? self : ".");
    printf("test_eccold%s%s\n", strcmp(mutant, "none") ? ": mutant " : "", strcmp(mutant, "none") ? mutant : "");
    struct {
        const char *name;
        void (*fn)(void);
    } cases[] = {{"cut", test_cut},       {"rotate", test_rotate}, {"gate", test_gate},
                 {"wait", test_wait},     {"down", test_down},     {"ttl", test_ttl},
                 {"depose", test_depose}, {"concurrent", test_concurrent}, {"policy", test_policy},
                 {"swapfail", test_swapfail}, {"deposeappend", test_deposeappend}, {"crash", test_crash},
                 {"drop", test_drop},     {"indexfail", test_indexfail}, {"ymerging", test_ymerging},
                 {"swappower", test_swappower}};
    for (size_t i = 0; i < sizeof cases / sizeof cases[0]; i++)
        if (!only || !strcmp(only, cases[i].name))
            cases[i].fn();
    if (failures) {
        fprintf(stderr, "test_eccold: %d failures\n", failures);
        return 1;
    }
    printf("test_eccold: ok\n");
    return 0;
}
#endif
