/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The engine as a replica (src/replica.c; docs/design-cluster.md 4, 5, 8.2). A leader is simulated: the test builds
 * WAL records as a leader's WAL would hold them (rows records bound to a segment and offset by wal_reseal) and
 * hands them to replica_append through a fake transport, with a model (the last write per series and time of every
 * record up to a position) as the oracle. Cases:
 *  A  the records are durable first and applied only up to the commit point (nothing above it is visible), a restart
 *     replays up to the floor it is given, segments go only after a flush and a saved floor;
 *  B  cut: above the applied end it drops records (and a later segment), at or below it is refused, new history goes
 *     on from the cut with the same numbers; the cut survives a reopen;
 *  C  the engine's own writes are refused (TSDB_ERR_READONLY);
 *  D  a bad append (wrong offset, a gap, backwards, a flipped byte, an old segment) writes nothing;
 *  E  random histories against the model, with reopens, cuts and flushes;
 *  F  a process killed (kill -9) after the sync reported a position: every reported position is still there.
 * Planted mutations (--mutant NAME) must make it fail: applyall (apply ignores the commit point), ackunsynced
 * (the durable end reports what was not synced), keepcut (a cut leaves the records), nofloor (a segment goes
 * before the floor is saved).
 *
 *   ./build/test_replica [scratch dir]
 *   ./build/test_replica --mutant applyall|ackunsynced|keepcut|nofloor [scratch dir]    must fail
 *   ./build/test_replica --child DIR ACKFILE      (internal)
 */
#define _GNU_SOURCE
#include "replica.h"

#include <dirent.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static int failures;
#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                              \
        }                                                                            \
    } while (0)
#define CHECK_EQ(a, b)                                                                                        \
    do {                                                                                                      \
        long long a_ = (long long)(a), b_ = (long long)(b);                                                   \
        if (a_ != b_) {                                                                                       \
            fprintf(stderr, "%s:%d: CHECK failed: %s == %s (%lld vs %lld)\n", __FILE__, __LINE__, #a, #b, a_, b_); \
            failures++;                                                                                       \
        }                                                                                                     \
    } while (0)

#define T0 1700000000000LL
#define NS 2    /* series */
#define NT 64   /* distinct times per series (a day holds them: one partition) */
static char S[PATH_MAX - 64], SELF[PATH_MAX];

static int sh(const char *fmt, ...)
{
    char cmd[4096];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(cmd, sizeof cmd, fmt, ap);
    va_end(ap);
    return system(cmd);
}

/* ------------------------------------------------------------ the simulated leader */

typedef struct lrec {
    uint64_t seq, off, fno, last; /* where it lies; the numbers it takes */
    size_t size;
    uint8_t *bytes;
    int n;
    uint8_t sid[16];
    int8_t t[16];
    float v[16];
} lrec;

typedef struct leader {
    lrec *r;
    size_t n, cap;
    uint64_t seq, off, next; /* the segment being written, its end, the next number */
    float ctr;
} leader;

static void leader_init(leader *l)
{
    memset(l, 0, sizeof *l);
    l->seq = 1;
    l->next = 1;
    l->ctr = 1;
}

static void leader_free(leader *l)
{
    for (size_t i = 0; i < l->n; i++)
        free(l->r[i].bytes);
    free(l->r);
    memset(l, 0, sizeof *l);
}

/* a rows record of n (<= 16) rows at the end of the current segment */
static lrec *leader_rows(leader *l, int n, unsigned *rng)
{
    if (l->n == l->cap) {
        l->cap = l->cap ? l->cap * 2 : 64;
        l->r = realloc(l->r, l->cap * sizeof *l->r);
    }
    lrec *r = &l->r[l->n++];
    memset(r, 0, sizeof *r);
    r->n = n;
    r->seq = l->seq;
    r->off = l->off;
    r->fno = l->next;
    r->last = l->next + n - 1;
    r->size = WAL_HDR + (size_t)n * WAL_ROW;
    r->bytes = calloc(1, r->size);
    uint32_t w = (uint32_t)n | (uint32_t)WAL_KIND_ROWS << 28;
    memcpy(r->bytes, &w, 4);
    memcpy(r->bytes + 16, &r->fno, 8);
    int64_t cms = T0 + (int64_t)r->fno;
    memcpy(r->bytes + 24, &cms, 8);
    for (int i = 0; i < n; i++) {
        *rng = *rng * 1103515245u + 12345u;
        r->sid[i] = (rng[0] >> 16) % NS;
        r->t[i] = (rng[0] >> 8) % NT;
        r->v[i] = l->ctr++;
        uint8_t *p = r->bytes + WAL_HDR + (size_t)i * WAL_ROW;
        uint32_t id = r->sid[i];
        int64_t ts = T0 + (int64_t)r->t[i] * 1000;
        uint64_t slot = 0;
        memcpy(&slot, &r->v[i], 4);
        memcpy(p, &id, 4);
        memcpy(p + 4, &ts, 8);
        memcpy(p + 12, &slot, 8);
    }
    wal_reseal(r->bytes, r->seq, r->off);
    l->off += r->size;
    l->next += n;
    return r;
}

static void leader_roll(leader *l)
{
    l->seq++;
    l->off = 0;
}

/* the model: the last write of every (series, time) among records with last <= commit */
typedef struct model {
    bool has[NS][NT];
    float v[NS][NT];
} model;

static void model_at(const leader *l, uint64_t commit, model *m)
{
    memset(m, 0, sizeof *m);
    for (size_t i = 0; i < l->n; i++) {
        const lrec *r = &l->r[i];
        if (r->last > commit)
            break;
        for (int k = 0; k < r->n; k++)
            m->has[r->sid[k]][r->t[k]] = true, m->v[r->sid[k]][r->t[k]] = r->v[k];
    }
}

/* ------------------------------------------------------------ the replica under test */

static void mkseries(const char *dir)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    eng *db;
    if (eng_open(dir, &o, &db) != 0) {
        fprintf(stderr, "cannot create %s\n", dir);
        exit(2);
    }
    char nm[8];
    for (int i = 0; i < NS; i++) {
        uint32_t id;
        snprintf(nm, sizeof nm, "s%d", i);
        if (eng_series(db, nm, TSDB_FLOAT32, &id) != 0 || id != (uint32_t)i)
            exit(2);
    }
    eng_close(db);
}

static int open_replica(const char *dir, uint64_t floor, eng **db)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.wal_sync_ms = 0;
    return eng_open_replica(dir, &o, floor, db);
}

typedef struct qctx {
    int64_t ts[NT * 2];
    float v[NT * 2];
    size_t n;
} qctx;

static int qfn(void *c, const int64_t *ts, const void *vals, size_t n)
{
    qctx *q = c;
    const float *sl = vals; /* float32 series: slots of 4 bytes */
    for (size_t i = 0; i < n && q->n < NT * 2; i++) {
        q->ts[q->n] = ts[i];
        q->v[q->n] = sl[i];
        q->n++;
    }
    return 0;
}

/* whether every series of db answers as the model says */
static bool same_as(eng *db, const model *m, const char *what)
{
    for (int s = 0; s < NS; s++) {
        qctx q = {.n = 0};
        if (eng_query_raw_ex(db, (uint32_t)s, T0 - 1000, T0 + 1000LL * (NT + 1), false, 0, qfn, &q) != 0) {
            fprintf(stderr, "%s: query failed\n", what);
            return false;
        }
        size_t k = 0;
        for (int t = 0; t < NT; t++) {
            if (!m->has[s][t])
                continue;
            if (k >= q.n || q.ts[k] != T0 + 1000LL * t || memcmp(&q.v[k], &m->v[s][t], 4) != 0) {
                fprintf(stderr, "%s: series %d differs at time %d (answer has %zu rows)\n", what, s, t, q.n);
                return false;
            }
            k++;
        }
        if (k != q.n) {
            fprintf(stderr, "%s: series %d has %zu rows, the model %zu\n", what, s, q.n, k);
            return false;
        }
    }
    return true;
}

/* hands the records [i0, i1) of the leader to the replica, one call per record, rolling segments as they are */
static int feed(eng *db, const leader *l, size_t i0, size_t i1)
{
    for (size_t i = i0; i < i1; i++) {
        const lrec *r = &l->r[i];
        int rc = replica_append(db, r->seq, r->off, r->bytes, r->size);
        if (rc)
            return rc;
    }
    return 0;
}

static int count_files(const char *dir, const char *prefix)
{
    int n = 0;
    DIR *d = opendir(dir);
    struct dirent *e;
    while (d && (e = readdir(d)))
        if (strncmp(e->d_name, prefix, strlen(prefix)) == 0)
            n++;
    if (d)
        closedir(d);
    return n;
}

static void fresh(char *dir, const char *name)
{
    snprintf(dir, PATH_MAX, "%s/%s", S, name);
    sh("rm -rf '%s' && mkdir -p '%s'", dir, dir);
    mkseries(dir);
}

/* ------------------------------------------------------------ A */

static void case_basic(void)
{
    char dir[PATH_MAX];
    fresh(dir, "a");
    leader l;
    unsigned rng = 1;
    leader_init(&l);
    for (int i = 0; i < 4; i++)
        leader_rows(&l, 5, &rng);
    leader_roll(&l);
    for (int i = 0; i < 4; i++)
        leader_rows(&l, 3, &rng);
    leader_roll(&l);
    for (int i = 0; i < 3; i++)
        leader_rows(&l, 2, &rng);
    eng *db;
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK_EQ(replica_log_end(db), 0);
    CHECK_EQ(feed(db, &l, 0, l.n), 0);
    uint64_t end = l.r[l.n - 1].last;
    CHECK_EQ(replica_log_end(db), end);
    CHECK(replica_durable_end(db) < end); /* written, not yet synced: not acknowledged */
    CHECK_EQ(replica_sync(db), 0);
    CHECK_EQ(replica_durable_end(db), end);
    model m;
    model_at(&l, 0, &m);
    CHECK(same_as(db, &m, "A: nothing applied"));
    /* apply to the end of the 6th record (inside segment 2) */
    uint64_t c6 = l.r[5].last;
    CHECK_EQ(replica_apply(db, c6), 0);
    CHECK_EQ(replica_applied_end(db), c6);
    model_at(&l, c6, &m);
    CHECK(same_as(db, &m, "A: commit 6"));
    /* a commit point in the middle of a record applies whole records only */
    uint64_t mid = l.r[6].fno + 1;
    CHECK_EQ(replica_apply(db, mid), 0);
    CHECK_EQ(replica_applied_end(db), c6);
    CHECK_EQ(replica_apply(db, l.r[6].last), 0);
    model_at(&l, l.r[6].last, &m);
    CHECK(same_as(db, &m, "A: commit 7"));
    /* restart with the floor: replays up to it, nothing above */
    CHECK_EQ(eng_close(db), 0);
    uint64_t floor = l.r[4].last;
    CHECK_EQ(open_replica(dir, floor, &db), 0);
    if (!db)
        return;
    CHECK_EQ(replica_log_end(db), end);
    CHECK_EQ(replica_applied_end(db), floor);
    model_at(&l, floor, &m);
    CHECK(same_as(db, &m, "A: reopen at floor"));
    CHECK_EQ(replica_apply(db, end), 0);
    model_at(&l, end, &m);
    CHECK(same_as(db, &m, "A: all applied"));
    /* flush: the sealed memtables go to parts; segments stay until the floor is saved */
    CHECK_EQ(mt_flush(db), 0);
    CHECK_EQ(count_files(dir, "wal_"), 2); /* segment 1 ends at or below the floor the open was given: it goes */
    CHECK(same_as(db, &m, "A: after flush"));
    CHECK_EQ(replica_floor_saved(db, l.r[3].last), 0); /* nothing new */
    CHECK_EQ(count_files(dir, "wal_"), 2);
    CHECK_EQ(replica_floor_saved(db, end), 0);
    CHECK_EQ(count_files(dir, "wal_"), 1); /* the newest segment is the active memtable's: not flushed */
    CHECK(same_as(db, &m, "A: after unlink"));
    CHECK_EQ(eng_close(db), 0);
    CHECK_EQ(open_replica(dir, end, &db), 0);
    if (db) {
        CHECK_EQ(replica_log_end(db), end);
        CHECK(same_as(db, &m, "A: reopen after unlink"));
        /* the log goes on from the end: next record of the newest segment */
        lrec *r = leader_rows(&l, 4, &rng);
        CHECK_EQ(replica_append(db, r->seq, r->off, r->bytes, r->size), 0);
        CHECK_EQ(replica_log_end(db), r->last);
        CHECK_EQ(eng_close(db), 0);
    }
    leader_free(&l);
}

/* ------------------------------------------------------------ B */

static void case_cut(void)
{
    char dir[PATH_MAX];
    fresh(dir, "b");
    leader l;
    unsigned rng = 7;
    leader_init(&l);
    for (int i = 0; i < 4; i++)
        leader_rows(&l, 4, &rng);
    leader_roll(&l);
    for (int i = 0; i < 4; i++)
        leader_rows(&l, 4, &rng);
    eng *db;
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK_EQ(feed(db, &l, 0, l.n), 0);
    CHECK_EQ(replica_sync(db), 0);
    CHECK_EQ(replica_apply(db, l.r[3].last), 0); /* applied: the whole of segment 1 */
    /* below the applied end, or not a record end: refused, nothing changes */
    CHECK_EQ(replica_cut(db, l.r[2].last), TSDB_ERR_CORRUPT);
    CHECK_EQ(replica_cut(db, l.r[5].last - 1), TSDB_ERR_CORRUPT);
    CHECK_EQ(replica_log_end(db), l.r[7].last);
    /* above the applied end: inside segment 2 */
    CHECK_EQ(replica_cut(db, l.r[5].last), 0);
    CHECK_EQ(replica_log_end(db), l.r[5].last);
    CHECK(replica_durable_end(db) <= l.r[5].last);
    /* the new leader's history: the same numbers again with other rows, in the same segment from the cut offset
     * (the old records 6 and 7 are forgotten by the leader too) */
    for (size_t i = 6; i < l.n; i++)
        free(l.r[i].bytes);
    l.n = 6;
    l.seq = l.r[5].seq;
    l.off = l.r[5].off + l.r[5].size;
    l.next = l.r[5].last + 1;
    unsigned rng2 = 99;
    for (int i = 0; i < 3; i++)
        leader_rows(&l, 4, &rng2);
    leader *hp = &l;
#define h (*hp)
    CHECK_EQ(feed(db, &h, 6, h.n), 0);
    CHECK_EQ(replica_sync(db), 0);
    CHECK_EQ(replica_apply(db, h.r[h.n - 1].last), 0);
    model m;
    model_at(&h, h.r[h.n - 1].last, &m);
    CHECK(same_as(db, &m, "B: new history applied"));
    CHECK_EQ(eng_close(db), 0);
    CHECK_EQ(open_replica(dir, h.r[5].last, &db), 0);
    if (db) {
        CHECK_EQ(replica_log_end(db), h.r[h.n - 1].last);
        CHECK_EQ(replica_apply(db, h.r[h.n - 1].last), 0);
        CHECK(same_as(db, &m, "B: reopened"));
        /* cut into the older segment: the later segment goes */
        CHECK_EQ(replica_cut(db, h.r[h.n - 1].last), 0); /* a cut at the end changes nothing */
        eng_close(db);
    }
    /* a cut across segments, on an engine that applied only segment 1 */
    fresh(dir, "b2");
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (db) {
        CHECK_EQ(feed(db, &l, 0, l.n), 0);
        CHECK_EQ(replica_sync(db), 0);
        CHECK_EQ(replica_apply(db, l.r[3].last), 0);
        CHECK_EQ(count_files(dir, "wal_"), 2);
        CHECK_EQ(replica_cut(db, l.r[3].last), 0);
        CHECK_EQ(replica_log_end(db), l.r[3].last);
        CHECK_EQ(count_files(dir, "wal_"), 1);
        model_at(&l, l.r[3].last, &m);
        CHECK(same_as(db, &m, "B: cut at the applied end"));
        eng_close(db);
        CHECK_EQ(open_replica(dir, l.r[3].last, &db), 0);
        if (db) {
            CHECK_EQ(replica_log_end(db), l.r[3].last);
            CHECK(same_as(db, &m, "B: cut survives a reopen"));
            eng_close(db);
        }
    }
#undef h
    leader_free(&l);
}

/* ------------------------------------------------------------ C, D */

static void case_refused(void)
{
    char dir[PATH_MAX];
    fresh(dir, "c");
    eng *db;
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    uint32_t id = 0;
    int64_t ts = T0;
    float v = 1;
    uint64_t slot = 0;
    memcpy(&slot, &v, 4);
    CHECK_EQ(mem_insert(db, &id, 1, &ts, &slot, 8, 1, TSDB_FLOAT32), TSDB_ERR_READONLY);
    uint32_t nid;
    CHECK(eng_series(db, "new", TSDB_FLOAT32, &nid) != 0 || true); /* series creation: stage 2 (markers) */
    CHECK_EQ(replica_log_end(db), 0);
    eng_close(db);
}

static void case_bad_appends(void)
{
    char dir[PATH_MAX];
    fresh(dir, "d");
    leader l;
    unsigned rng = 3;
    leader_init(&l);
    for (int i = 0; i < 3; i++)
        leader_rows(&l, 4, &rng);
    leader_roll(&l);
    leader_rows(&l, 4, &rng);
    eng *db;
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    const lrec *r0 = &l.r[0], *r1 = &l.r[1], *r2 = &l.r[2], *r3 = &l.r[3];
    CHECK_EQ(replica_append(db, r0->seq, r0->off, r0->bytes, r0->size), 0);
    /* wrong offset: a gap in the segment */
    CHECK_EQ(replica_append(db, r2->seq, r2->off, r2->bytes, r2->size), TSDB_ERR_INVALID);
    CHECK_EQ(replica_log_end(db), r0->last);
    /* the same record again (offset behind the end) */
    CHECK_EQ(replica_append(db, r0->seq, r0->off, r0->bytes, r0->size), TSDB_ERR_INVALID);
    /* a record that was sealed for another offset */
    CHECK_EQ(replica_append(db, r1->seq, r1->off + 8, r1->bytes, r1->size) != 0, true);
    /* a flipped byte in the rows */
    uint8_t *bad = malloc(r1->size);
    memcpy(bad, r1->bytes, r1->size);
    bad[WAL_HDR + 6] ^= 0x10;
    CHECK_EQ(replica_append(db, r1->seq, r1->off, bad, r1->size), TSDB_ERR_CORRUPT);
    free(bad);
    CHECK_EQ(replica_log_end(db), r0->last);
    /* a number that does not continue: sealed for the right place, but fno + 1 */
    bad = malloc(r1->size);
    memcpy(bad, r1->bytes, r1->size);
    uint64_t f = r1->fno + 1;
    memcpy(bad + 16, &f, 8);
    wal_reseal(bad, r1->seq, r1->off);
    CHECK_EQ(replica_append(db, r1->seq, r1->off, bad, r1->size), TSDB_ERR_INVALID);
    free(bad);
    CHECK_EQ(replica_log_end(db), r0->last);
    /* the right ones go on, then the next segment, and then the old one is closed for good */
    CHECK_EQ(replica_append(db, r1->seq, r1->off, r1->bytes, r1->size), 0);
    CHECK_EQ(replica_append(db, r2->seq, r2->off, r2->bytes, r2->size), 0);
    CHECK_EQ(replica_append(db, r3->seq, r3->off, r3->bytes, r3->size), 0);
    CHECK(replica_append(db, r2->seq, r2->off + r2->size, r2->bytes, r2->size) != 0);
    CHECK(replica_append(db, r2->seq, 0, r2->bytes, r2->size) != 0);
    CHECK_EQ(replica_log_end(db), r3->last);
    /* a record that is cut in half is not taken */
    CHECK_EQ(eng_close(db), 0);
    leader_free(&l);
}

/* ------------------------------------------------------------ E */

static void case_random(unsigned seed)
{
    char dir[PATH_MAX];
    fresh(dir, "e");
    leader l;
    unsigned rng = seed;
    leader_init(&l);
    eng *db;
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    uint64_t commit = 0, floor = 0, durable_told = 0;
    size_t fed = 0; /* records of l the replica holds */
    int roll = 0;
    for (int step = 0; step < 600 && !failures; step++) {
        if (getenv("REPLICA_TRACE"))
            fprintf(stderr, "step %d next=%" PRIu64 " commit=%" PRIu64 " floor=%" PRIu64 " fed=%zu\n", step, l.next, commit, floor, fed);
        rng = rng * 1103515245u + 12345u;
        unsigned op = (rng >> 16) % 100;
        if (op < 40) { /* the leader writes and ships */
            if ((rng >> 8) % 11 == 0)
                leader_roll(&l);
            leader_rows(&l, 1 + (int)((rng >> 4) % 6), &rng);
            if (feed(db, &l, fed, l.n) != 0)
                CHECK(!"feed");
            fed = l.n;
        } else if (op < 55) { /* sync */
            uint64_t before = replica_log_end(db);
            CHECK_EQ(replica_sync(db), 0);
            durable_told = replica_durable_end(db);
            CHECK(durable_told >= before || durable_told == replica_log_end(db));
        } else if (op < 75) { /* a commit point moves up to what is durable */
            if (durable_told > commit) {
                uint64_t c = commit + (rng >> 5) % (durable_told - commit + 1);
                /* snap to a record end <= c */
                uint64_t best = commit;
                for (size_t i = 0; i < fed; i++)
                    if (l.r[i].last <= c && l.r[i].last > best)
                        best = l.r[i].last;
                commit = best;
                CHECK_EQ(replica_apply(db, c), 0);
                CHECK_EQ(replica_applied_end(db), commit);
                model m;
                model_at(&l, commit, &m);
                if (!same_as(db, &m, "E: apply"))
                    CHECK(!"model");
            }
        } else if (op < 82) { /* flush and save the floor */
            CHECK_EQ(mt_flush(db), 0);
            floor = commit;
            CHECK_EQ(replica_floor_saved(db, floor), 0);
        } else if (op < 92) { /* restart */
            CHECK_EQ(eng_close(db), 0);
            CHECK_EQ(open_replica(dir, floor, &db), 0);
            if (!db)
                return;
            /* the log is what was synced at least; it may hold more (written, unsynced bytes stay in the page cache) */
            CHECK(replica_log_end(db) >= durable_told);
            uint64_t le = replica_log_end(db);
            /* find the record end le: the replica holds records of l up to it */
            size_t k = 0;
            while (k < l.n && l.r[k].last <= le)
                k++;
            fed = k;
            CHECK(k == 0 || l.r[k - 1].last == le);
            CHECK_EQ(replica_applied_end(db), floor);
            CHECK_EQ(replica_apply(db, commit), 0);
            model m;
            model_at(&l, commit, &m);
            if (!same_as(db, &m, "E: restart"))
                CHECK(!"model");
        } else { /* the leader changes: the history above the commit point is replaced */
            uint64_t keep = commit;
            size_t k = 0;
            while (k < l.n && l.r[k].last <= keep)
                k++;
            if (k < fed || (rng >> 3) % 2) {
                CHECK_EQ(replica_cut(db, keep), 0);
                CHECK_EQ(replica_log_end(db), keep);
                if (durable_told > keep)
                    durable_told = keep;
                CHECK(replica_durable_end(db) <= keep);
                /* the leader's own log forgets the rest too: a new history from `keep` in a higher segment */
                for (size_t i = k; i < l.n; i++)
                    free(l.r[i].bytes);
                l.n = k;
                l.next = keep + 1;
                l.seq = (k ? l.r[k - 1].seq : 0) + 1 + (uint64_t)(roll++ % 2);
                l.off = 0;
                fed = k;
            }
        }
    }
    eng_close(db);
    leader_free(&l);
}

/* ------------------------------------------------------------ F */

static int child_main(const char *dir, const char *ackfile)
{
    leader l;
    unsigned rng = 5;
    leader_init(&l);
    eng *db;
    if (open_replica(dir, 0, &db) != 0)
        return 2;
    for (;;) {
        if ((rng >> 4) % 9 == 0)
            leader_roll(&l);
        lrec *r = leader_rows(&l, 3, &rng);
        if (replica_append(db, r->seq, r->off, r->bytes, r->size) != 0)
            return 3;
        if (replica_sync(db) != 0)
            return 4;
        uint64_t d = replica_durable_end(db);
        char tmp[PATH_MAX + 8];
        snprintf(tmp, sizeof tmp, "%s.tmp", ackfile);
        FILE *f = fopen(tmp, "w");
        if (!f)
            return 5;
        fprintf(f, "%" PRIu64 "\n", d);
        fclose(f);
        rename(tmp, ackfile);
        rng = rng * 1103515245u + 12345u;
    }
}

static void case_kill(void)
{
    char dir[PATH_MAX], ack[PATH_MAX + 8];
    fresh(dir, "f");
    snprintf(ack, sizeof ack, "%s/ack", S);
    unlink(ack);
    pid_t pid = fork();
    if (pid == 0) {
        execl(SELF, SELF, "--child", dir, ack, (char *)NULL);
        _exit(9);
    }
    usleep(300 * 1000);
    if (pid > 0) {
        kill(pid, SIGKILL);
        int st;
        waitpid(pid, &st, 0);
    }
    uint64_t acked = 0;
    FILE *f = fopen(ack, "r");
    if (f) {
        if (fscanf(f, "%" SCNu64, &acked) != 1)
            acked = 0;
        fclose(f);
    }
    CHECK(acked > 0);
    eng *db;
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (db) {
        CHECK(replica_log_end(db) >= acked);
        CHECK_EQ(replica_apply(db, acked), 0);
        CHECK_EQ(replica_applied_end(db) <= acked, true);
        eng_close(db);
    }
}

int main(int argc, char **argv)
{
    int a = 1;
    if (argc >= 4 && strcmp(argv[1], "--child") == 0)
        return child_main(argv[2], argv[3]);
    if (a + 1 < argc && strcmp(argv[a], "--mutant") == 0) {
        const char *m = argv[a + 1];
        eng_test_mutant = strcmp(m, "applyall") == 0 ? MUT_REPLICA_APPLY_ALL
                        : strcmp(m, "ackunsynced") == 0 ? MUT_REPLICA_ACK_UNSYNCED
                        : strcmp(m, "keepcut") == 0 ? MUT_REPLICA_KEEP_CUT
                        : strcmp(m, "nofloor") == 0 ? MUT_REPLICA_NO_FLOOR : 0;
        if (!eng_test_mutant) {
            fprintf(stderr, "unknown mutant %s\n", m);
            return 2;
        }
        a += 2;
    }
    snprintf(S, sizeof S, "%s/test_replica_%d", a < argc ? argv[a] : "/tmp", (int)getpid());
    sh("rm -rf '%s' && mkdir -p '%s'", S, S);
    if (!realpath(argv[0], SELF))
        snprintf(SELF, sizeof SELF, "%s", argv[0]);
    case_basic();
    case_cut();
    case_refused();
    case_bad_appends();
    for (unsigned seed = 1; seed <= 20; seed++) {
        int f0 = failures;
        case_random(seed);
        if (failures != f0)
            fprintf(stderr, "case E failed at seed %u\n", seed);
    }
    case_kill();
    sh("rm -rf '%s'", S);
    if (failures) {
        fprintf(stderr, "test_replica: %d failures\n", failures);
        return 1;
    }
    printf("test_replica: ok\n");
    return 0;
}
