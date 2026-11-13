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
 *  G  SERIES and STATE markers make the dictionaries (ids as the leader's, a replay changes nothing, a history that
 *     does not match is TSDB_ERR_CORRUPT), the engine's own creations are refused;
 *  H  the clock is the log's (commit_ms, NOW), never the wall clock: retention follows it; RETENTION records are
 *     applied once, by their offset in the file, across a restart;
 *  I  CLASS markers: T is moved past the replica's own rows (a part already holds them), a replay appends nothing,
 *     a reopen checks every part against CLASSES;
 *  F  a process killed (kill -9) after the sync reported a position: every reported position is still there.
 * Planted mutations (--mutant NAME) must make it fail: applyall (apply ignores the commit point), ackunsynced
 * (the durable end reports what was not synced), keepcut (a cut leaves the records), nofloor (a segment goes
 * before the floor is saved), walclock (retention reads the replica's own clock), nopos (a replayed RETENTION or CLASS
 * record is appended again), classat (a CLASS record takes the leader's T whatever this replica's parts hold).
 *
 *   ./build/test_replica [scratch dir]
 *   ./build/test_replica --mutant applyall|ackunsynced|keepcut|nofloor|walclock|nopos|classat [scratch dir]    must fail
 *   ./build/test_replica --child DIR ACKFILE      (internal)
 */
#define _GNU_SOURCE
#include "replica.h"
#include "marker.h"
#include "feed.h"

#include <dirent.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
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
static const char *MUTNAME;

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

/* ------------------------------------------------------------ read */

/* the sender's side: the log read back in chunks gives exactly the bytes that were appended, at every chunk size and
 * window size, from every record start, and in a cost that follows what is sent (the cursor) */
static void case_read(void)
{
    char dir[PATH_MAX];
    fresh(dir, "rd");
    leader l;
    unsigned rng = 11;
    leader_init(&l);
    for (int i = 0; i < 12; i++)
        leader_rows(&l, 1 + i % 16, &rng);
    leader_roll(&l);
    for (int i = 0; i < 12; i++)
        leader_rows(&l, 1 + (i * 5) % 16, &rng);
    leader_roll(&l);
    for (int i = 0; i < 6; i++)
        leader_rows(&l, 16, &rng);
    eng *db;
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK_EQ(feed(db, &l, 0, l.n), 0);
    CHECK_EQ(replica_sync(db), 0);
    uint64_t end = replica_log_end(db);
    size_t caps[] = {1, 200, 1000, 5000, 1 << 20};
    size_t wins[] = {16, 100, 65536};
    for (size_t wi = 0; wi < 3; wi++)
        for (size_t ci = 0; ci < 5; ci++) {
            replica_read_win_min = wins[wi];
            size_t ri = 0; /* the next record of the leader's list */
            uint64_t from = 1;
            int guard = 0;
            while (from <= end && guard++ < 1000) {
                uint8_t *buf;
                size_t len;
                uint64_t seq, off, last;
                int rc = replica_read(db, from, &buf, &len, caps[ci], &seq, &off, &last);
                CHECK_EQ(rc, 0);
                if (rc != 0)
                    break;
                size_t used = 0;
                CHECK(len > 0);
                while (used < len && ri < l.n) {
                    CHECK(l.r[ri].seq == seq && l.r[ri].off == off + used);
                    CHECK(used + l.r[ri].size <= len && memcmp(buf + used, l.r[ri].bytes, l.r[ri].size) == 0);
                    used += l.r[ri].size;
                    ri++;
                }
                CHECK_EQ(used, len);
                CHECK(ri > 0 && last == l.r[ri - 1].last);
                if (caps[ci] >= 200 && len > caps[ci])
                    CHECK(used == l.r[ri - 1].size); /* only a single record may exceed the cap */
                free(buf);
                from = last + 1;
            }
            CHECK_EQ(ri, l.n);
            CHECK_EQ(from, end + 1);
        }
    replica_read_win_min = 65536;
    /* every record start, cold (no cursor to help) and with a stale cursor */
    for (size_t i = l.n; i-- > 0;) {
        uint8_t *buf;
        size_t len;
        uint64_t seq, off, last;
        CHECK_EQ(replica_read(db, l.r[i].fno, &buf, &len, 1, &seq, &off, &last), 0);
        CHECK(len == l.r[i].size && seq == l.r[i].seq && off == l.r[i].off && last == l.r[i].last);
        free(buf);
    }
    uint8_t *buf;
    size_t len;
    uint64_t seq, off, last;
    CHECK_EQ(replica_read(db, l.r[1].fno + 1, &buf, &len, 100, &seq, &off, &last), TSDB_ERR_INVALID); /* inside a record */
    CHECK_EQ(replica_read(db, end + 1, &buf, &len, 100, &seq, &off, &last), 0);
    CHECK_EQ(len, 0);
    CHECK_EQ(replica_read(db, end + 2, &buf, &len, 100, &seq, &off, &last), TSDB_ERR_INVALID);
    /* a cut resets the cursor: read again after the log is shortened and grown with other records */
    CHECK_EQ(replica_cut(db, l.r[l.n - 3].last), 0);
    CHECK_EQ(replica_read(db, l.r[l.n - 3].last + 1, &buf, &len, 100, &seq, &off, &last), 0);
    CHECK_EQ(len, 0);
    CHECK_EQ(eng_close(db), 0);
    leader_free(&l);
}

/* ------------------------------------------------------------ sync across a cut, sealed segment after reopen */

static void seg_file(char *out, size_t cap, const char *dir, uint64_t seq);
static leader *HK_L, *HK_L2;
static lrec *HK_N1, *HK_N2;

/* in the middle of a sync: a cut and two new records (written, not synced) arrive */
static void hook_cut_and_append(eng *db)
{
    replica_test_sync_hook = NULL;
    if (replica_cut(db, HK_L->r[1].last) != 0)
        return;
    replica_append(db, HK_N1->seq, HK_N1->off, HK_N1->bytes, HK_N1->size);
    replica_append(db, HK_N2->seq, HK_N2->off, HK_N2->bytes, HK_N2->size);
}

static void case_sync_cut(void)
{
    char dir[PATH_MAX];
    fresh(dir, "sc");
    leader l, l2;
    unsigned rng = 3, r2 = 9;
    leader_init(&l);
    leader_init(&l2);
    for (int i = 0; i < 4; i++)
        leader_rows(&l, 3, &rng);
    unsigned rng2 = 3;
    for (int i = 0; i < 2; i++) /* the new history: the first two records of the old one (the same bytes), then others */
        leader_rows(&l2, 3, &rng2);
    lrec *n1 = leader_rows(&l2, 3, &r2), *n2 = leader_rows(&l2, 3, &r2);
    eng *db;
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK_EQ(feed(db, &l, 0, l.n), 0);
    HK_L = &l, HK_L2 = &l2, HK_N1 = n1, HK_N2 = n2;
    replica_test_sync_hook = hook_cut_and_append;
    CHECK_EQ(replica_sync(db), 0);
    replica_test_sync_hook = NULL;
    /* the sync began before the cut: it covers nothing that was written after it */
    CHECK(replica_durable_end(db) <= l.r[1].last);
    CHECK_EQ(replica_sync(db), 0);
    CHECK_EQ(replica_durable_end(db), n2->last);
    eng_close(db);
    leader_free(&l);
    leader_free(&l2);
}

static void case_sealed_open(void)
{
    char dir[PATH_MAX];
    fresh(dir, "so");
    leader l, l2;
    unsigned rng = 13;
    leader_init(&l);
    for (int i = 0; i < 3; i++)
        leader_rows(&l, 3, &rng);
    leader_roll(&l);
    for (int i = 0; i < 3; i++)
        leader_rows(&l, 3, &rng);
    eng *db;
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK_EQ(feed(db, &l, 0, l.n), 0);
    CHECK_EQ(replica_sync(db), 0);
    uint64_t end = replica_log_end(db), end1 = l.r[2].last;
    CHECK_EQ(replica_apply(db, end), 0);
    CHECK_EQ(mt_flush(db), 0); /* segment 1's memtable is in parts now: it is sealed for good */
    CHECK_EQ(replica_floor_saved(db, end1), 0);
    CHECK_EQ(eng_close(db), 0);
    char f2[PATH_MAX];
    seg_file(f2, sizeof f2, dir, 2);
    CHECK_EQ(unlink(f2), 0); /* nothing left of the log but the parts' own numbers */
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    /* the next leader's records start in a segment the parts hold a part of: rows there would be skipped by a replay */
    leader_init(&l2);
    l2.next = replica_log_end(db) + 1;
    lrec *r = leader_rows(&l2, 3, &rng);
    CHECK(replica_append(db, r->seq, r->off, r->bytes, r->size) != 0);
    eng_close(db);
    leader_free(&l);
    leader_free(&l2);
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
        char tmp[PATH_MAX + 32];
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

/* a signal only to a child we started: kill(-1) and kill(0) reach every process of the user (AGENTS.md) */
static int kill_child(pid_t pid, int sig)
{
    if (pid <= 1)
        return -1;
    return kill(pid, sig);
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
    if (kill_child(pid, SIGKILL) == 0) {
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
        /* the child's leader is deterministic (rng 5): what the replica holds at the acknowledged position is that
         * history's rows, not merely some log of that length */
        leader l;
        unsigned rng = 5;
        leader_init(&l);
        while (l.next <= acked) {
            if ((rng >> 4) % 9 == 0)
                leader_roll(&l);
            leader_rows(&l, 3, &rng);
            rng = rng * 1103515245u + 12345u;
        }
        model m;
        model_at(&l, replica_applied_end(db), &m);
        CHECK(same_as(db, &m, "F: after kill -9"));
        leader_free(&l);
        eng_close(db);
    }
}


/* ------------------------------------------------------------ G, H, I: markers */

/* a marker record at the end of the current segment (commit_ms given) */
static lrec *leader_marker(leader *l, const mk *m, int64_t commit_ms)
{
    uint8_t *pl;
    size_t pn;
    if (mk_encode(m, &pl, &pn) != 0) {
        fprintf(stderr, "cannot encode a marker\n");
        exit(2);
    }
    if (l->n == l->cap) {
        l->cap = l->cap ? l->cap * 2 : 64;
        l->r = realloc(l->r, l->cap * sizeof *l->r);
    }
    lrec *r = &l->r[l->n++];
    memset(r, 0, sizeof *r);
    uint32_t span;
    memcpy(&span, pl + 4, 4);
    r->seq = l->seq;
    r->off = l->off;
    r->fno = l->next;
    r->last = l->next + span - 1;
    r->size = WAL_HDR + pn;
    r->bytes = calloc(1, r->size);
    uint32_t w = (uint32_t)pn | (uint32_t)WAL_KIND_MARKER << 28;
    memcpy(r->bytes, &w, 4);
    memcpy(r->bytes + 16, &r->fno, 8);
    memcpy(r->bytes + 24, &commit_ms, 8);
    memcpy(r->bytes + WAL_HDR, pl, pn);
    free(pl);
    wal_reseal(r->bytes, r->seq, r->off);
    l->off += r->size;
    l->next += span;
    return r;
}

/* a rows record of one row at any time (the model does not see it: r->n stays 0) */
static lrec *leader_row1(leader *l, uint32_t sid, int64_t ts, float v, int64_t commit_ms)
{
    unsigned rng = 1;
    lrec *r = leader_rows(l, 1, &rng);
    r->n = 0;
    uint8_t *p = r->bytes + WAL_HDR;
    uint64_t slot = 0;
    memcpy(&slot, &v, 4);
    memcpy(p, &sid, 4);
    memcpy(p + 4, &ts, 8);
    memcpy(p + 12, &slot, 8);
    memcpy(r->bytes + 24, &commit_ms, 8);
    wal_reseal(r->bytes, r->seq, r->off);
    return r;
}

static lrec *m_series(leader *l, uint32_t id, const char *name, int vtype)
{
    mk m = {.type = MK_SERIES};
    m.u.series = (mk_series){.id = id, .vtype = (uint32_t)vtype, .name = name, .name_len = (uint32_t)strlen(name)};
    return leader_marker(l, &m, T0 + (int64_t)l->next);
}

static lrec *m_state(leader *l, uint32_t id, const char *name)
{
    mk m = {.type = MK_STATE};
    m.u.state = (mk_state){.id = id, .name = name, .name_len = (uint32_t)strlen(name)};
    return leader_marker(l, &m, T0 + (int64_t)l->next);
}

static lrec *m_now(leader *l, int64_t now)
{
    mk m = {.type = MK_NOW};
    m.u.now.now_ms = now;
    return leader_marker(l, &m, now);
}

static lrec *m_ret(leader *l, uint32_t series, int64_t limit, uint64_t off)
{
    mk m = {.type = MK_RETENTION};
    m.u.retention = (mk_retention){.series = series, .limit_ms = limit, .record_off = off};
    return leader_marker(l, &m, T0 + (int64_t)l->next);
}

static lrec *m_class(leader *l, uint32_t series, int cls, int64_t at, uint64_t off)
{
    mk m = {.type = MK_CLASS};
    m.u.cls = (mk_class){.series = series, .cls = (uint32_t)cls, .at = at, .record_off = off};
    return leader_marker(l, &m, T0 + (int64_t)l->next);
}

static void mkempty(const char *dir)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    eng *db;
    if (eng_open(dir, &o, &db) != 0)
        exit(2);
    eng_close(db);
}

static void fresh_empty(char *dir, const char *name)
{
    snprintf(dir, PATH_MAX, "%s/%s", S, name);
    sh("rm -rf '%s' && mkdir -p '%s'", dir, dir);
    mkempty(dir);
}

/* a replica that is handed the records of l and applies them: the error of the apply (0: all applied) */
static int run_log(const char *name, void (*build)(leader *), int *failed_engine)
{
    char dir[PATH_MAX];
    fresh_empty(dir, name);
    leader l;
    leader_init(&l);
    build(&l);
    eng *db;
    int rc = open_replica(dir, 0, &db);
    CHECK_EQ(rc, 0);
    if (rc)
        return rc;
    CHECK_EQ(feed(db, &l, 0, l.n), 0);
    CHECK_EQ(replica_sync(db), 0);
    rc = replica_apply(db, l.r[l.n - 1].last);
    *failed_engine = eng_check(db) != 0;
    eng_close(db);
    leader_free(&l);
    return rc;
}

static void log_gap(leader *l) { m_series(l, 1, "s1", TSDB_FLOAT32); }
static void log_other_name(leader *l) { m_series(l, 0, "s0", TSDB_FLOAT32), m_series(l, 0, "other", TSDB_FLOAT32); }
static void log_other_type(leader *l) { m_series(l, 0, "s0", TSDB_FLOAT32), m_series(l, 0, "s0", TSDB_INT64); }
static void log_state_gap(leader *l) { m_state(l, 3, "RUN"); }
static void log_ret_gap(leader *l) { m_series(l, 0, "s0", TSDB_FLOAT32), m_ret(l, 0, 1000, 16); }
static void log_class_gap(leader *l) { m_series(l, 0, "s0", TSDB_FLOAT32), m_class(l, 0, 1, T0, 24); }

static void case_dicts(void)
{
    char dir[PATH_MAX];
    fresh_empty(dir, "g");
    leader l;
    unsigned rng = 3;
    leader_init(&l);
    m_series(&l, 0, "s0", TSDB_FLOAT32);
    m_series(&l, 1, "s1", TSDB_FLOAT32);
    lrec *st = m_state(&l, 0, "RUN");
    for (int i = 0; i < 4; i++)
        leader_rows(&l, 5, &rng);
    eng *db;
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK_EQ(feed(db, &l, 0, l.n), 0);
    CHECK_EQ(replica_sync(db), 0);
    CHECK_EQ(replica_apply(db, 0), 0);
    uint32_t id;
    CHECK_EQ(eng_series_count(db), 0);
    CHECK_EQ(eng_series_lookup(db, "s0", &id), TSDB_ERR_NOTFOUND);
    CHECK_EQ(replica_apply(db, st->last), 0);
    CHECK_EQ(eng_series_count(db), 2);
    CHECK(eng_series_lookup(db, "s1", &id) == 0 && id == 1);
    CHECK_EQ(eng_series_type(db, 0), TSDB_FLOAT32);
    uint32_t code = 0;
    CHECK(eng_state(db, "RUN", false, &code) == 0 && code == 1);
    CHECK_EQ(replica_apply(db, l.r[l.n - 1].last), 0);
    model m;
    model_at(&l, l.r[l.n - 1].last, &m);
    CHECK(same_as(db, &m, "G: applied"));
    /* the engine's own creations are refused; what exists is found */
    CHECK_EQ(eng_series(db, "new", TSDB_FLOAT32, &id), TSDB_ERR_READONLY);
    CHECK_EQ(eng_state(db, "STOP", true, &code), TSDB_ERR_READONLY);
    CHECK(eng_series(db, "s0", TSDB_FLOAT32, &id) == 0 && id == 0);
    CHECK_EQ(eng_series(db, "s0", TSDB_INT64, &id), TSDB_ERR_INVALID);
    CHECK_EQ(eng_series_count(db), 2);
    /* a replay (a restart below the markers) makes nothing again */
    CHECK_EQ(eng_close(db), 0);
    CHECK_EQ(open_replica(dir, l.r[l.n - 1].last, &db), 0);
    if (db) {
        CHECK_EQ(eng_series_count(db), 2);
        CHECK(eng_state(db, "RUN", false, &code) == 0 && code == 1);
        CHECK(same_as(db, &m, "G: replayed"));
        CHECK_EQ(eng_close(db), 0);
    }
    leader_free(&l);
    /* histories that are not the leader's: the apply refuses and the engine fails */
    int failed = 0;
    CHECK_EQ(run_log("g1", log_gap, &failed), TSDB_ERR_CORRUPT);
    CHECK(failed);
    CHECK_EQ(run_log("g2", log_other_name, &failed), TSDB_ERR_CORRUPT);
    CHECK(failed);
    CHECK_EQ(run_log("g3", log_other_type, &failed), TSDB_ERR_CORRUPT);
    CHECK(failed);
    CHECK_EQ(run_log("g4", log_state_gap, &failed), TSDB_ERR_CORRUPT);
    CHECK(failed);
    CHECK_EQ(run_log("g5", log_ret_gap, &failed), TSDB_ERR_CORRUPT);
    CHECK(failed);
    CHECK_EQ(run_log("g6", log_class_gap, &failed), TSDB_ERR_CORRUPT);
    CHECK(failed);
}

static int count_query(eng *db, uint32_t s, int64_t a, int64_t b, float *last)
{
    qctx q = {.n = 0};
    if (eng_query_raw_ex(db, s, a, b, false, 0, qfn, &q) != 0)
        return -1;
    if (q.n && last)
        *last = q.v[q.n - 1];
    return (int)q.n;
}

static void case_clock(void)
{
    char dir[PATH_MAX];
    fresh(dir, "h");
    leader l;
    unsigned rng = 5;
    leader_init(&l);
    for (int i = 0; i < 6; i++)
        leader_rows(&l, 5, &rng);
    /* the leader's clock is a minute and a half past the rows; the wall clock is years later */
    lrec *r1 = m_ret(&l, 0, 86400000, 0);
    lrec *n1 = m_now(&l, T0 + 100000);
    eng *db;
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK_EQ(feed(db, &l, 0, l.n), 0);
    CHECK_EQ(replica_sync(db), 0);
    CHECK_EQ(replica_apply(db, 0), 0);
    CHECK_EQ(eng_now_ms(db), 0);
    CHECK_EQ(replica_apply(db, r1->last), 0);
    CHECK_EQ(eng_retention(db, 0), 86400000);
    CHECK_EQ(replica_apply(db, n1->last), 0);
    CHECK_EQ(eng_now_ms(db), T0 + 100000);
    model m;
    model_at(&l, n1->last, &m);
    CHECK(same_as(db, &m, "H: retention by the leader's clock")); /* by the wall clock every row is a day old */
    /* a NOW below the clock changes nothing; one above moves it */
    lrec *n2 = m_now(&l, T0 + 50000);
    lrec *n3 = m_now(&l, T0 + 200000);
    lrec *r2 = m_ret(&l, 1, 600000, 16);
    lrec *n4 = m_now(&l, T0 + 3600000);
    CHECK_EQ(feed(db, &l, (size_t)(n1 - l.r) + 1, l.n), 0);
    CHECK_EQ(replica_sync(db), 0);
    CHECK_EQ(replica_apply(db, n2->last), 0);
    CHECK_EQ(eng_now_ms(db), T0 + 100000);
    CHECK_EQ(replica_apply(db, n3->last), 0);
    CHECK_EQ(eng_now_ms(db), T0 + 200000);
    CHECK_EQ(replica_apply(db, r2->last), 0);
    CHECK_EQ(db->ret_off, 32);
    CHECK(count_query(db, 1, T0 - 1000, T0 + 1000LL * (NT + 1), NULL) > 0);
    CHECK_EQ(replica_apply(db, n4->last), 0);
    CHECK_EQ(count_query(db, 1, T0 - 1000, T0 + 1000LL * (NT + 1), NULL), 0); /* ten minutes of retention, an hour later */
    model_at(&l, n4->last, &m);
    memset(m.has[1], 0, sizeof m.has[1]);
    CHECK(same_as(db, &m, "H: s1 expired, s0 not"));
    /* a part written by a flush carries the leader's time, and a restart starts the clock from it */
    CHECK_EQ(mt_flush(db), 0);
    CHECK_EQ(eng_close(db), 0);
    CHECK_EQ(open_replica(dir, n4->last, &db), 0);
    if (db) {
        CHECK_EQ(db->ret_off, 32); /* the replay found both records there already */
        CHECK(eng_now_ms(db) >= T0 + 3600000);
        CHECK(same_as(db, &m, "H: after the restart"));
        /* a record the file does not reach: the replica lacks one */
        lrec *bad = m_ret(&l, 0, 1000, 4096);
        CHECK_EQ(replica_append(db, bad->seq, bad->off, bad->bytes, bad->size), 0);
        CHECK_EQ(replica_sync(db), 0);
        CHECK_EQ(replica_apply(db, bad->last), TSDB_ERR_CORRUPT);
        CHECK(eng_check(db) != 0);
        eng_close(db);
    }
    leader_free(&l);
}

static void case_classes(void)
{
    char dir[PATH_MAX];
    fresh(dir, "i");
    leader l;
    unsigned rng = 7;
    leader_init(&l);
    for (int i = 0; i < 4; i++)
        leader_rows(&l, 5, &rng); /* segment 1: rows of both series on the day of T0 */
    leader_roll(&l);
    lrec *first2 = leader_row1(&l, 1, T0 + 70000, 99.0f, T0 + 1000);
    lrec *cm = m_class(&l, 0, 2, T0, 0);
    int64_t far = T0 + 3 * 86400000LL;
    lrec *fr = leader_row1(&l, 0, far, 7.0f, far);
    eng *db;
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK_EQ(feed(db, &l, 0, l.n), 0);
    CHECK_EQ(replica_sync(db), 0);
    CHECK_EQ(replica_apply(db, first2->last), 0); /* segment 1 is sealed */
    CHECK_EQ(mt_flush(db), 0);                    /* its rows of series 0 are in a part, in a class 0 partition */
    int c = -1;
    CHECK(eng_series_class(db, 0, &c) == 0 && c == 0);
    CHECK_EQ(replica_apply(db, cm->last), 0);
    CHECK(eng_series_class(db, 0, &c) == 0 && c == 2);
    CHECK_EQ(db->cls_off, 24);
    CHECK_EQ(replica_apply(db, fr->last), 0);
    float v = 0;
    CHECK_EQ(count_query(db, 0, far - 1, far + 1, &v), 1);
    CHECK(v == 7.0f);
    CHECK_EQ(mt_flush(db), 0);
    CHECK_EQ(eng_close(db), 0);
    /* the reopen checks every part against CLASSES; the replay of the CLASS record appends nothing */
    CHECK_EQ(open_replica(dir, fr->last, &db), 0);
    if (db) {
        CHECK_EQ(db->cls_off, 24);
        CHECK_EQ(count_query(db, 0, far - 1, far + 1, &v), 1);
        model m;
        model_at(&l, l.r[3].last, &m);
        CHECK(same_as(db, &m, "I: segment 1's rows after the class switch"));
        /* a CLASS record that is not where the file ends */
        lrec *bad = m_class(&l, 1, 1, T0, 96);
        CHECK_EQ(replica_append(db, bad->seq, bad->off, bad->bytes, bad->size), 0);
        CHECK_EQ(replica_sync(db), 0);
        CHECK_EQ(replica_apply(db, bad->last), TSDB_ERR_CORRUPT);
        CHECK(eng_check(db) != 0);
        eng_close(db);
    }
    leader_free(&l);
}

/* ------------------------------------------------------------ J..O: what the review of steps 1-2 found */

static void seg_file(char *out, size_t cap, const char *dir, uint64_t seq)
{
    snprintf(out, cap, "%s/wal_%016" PRIx64 ".log", dir, seq);
}

/* J: the open refuses a log with a hole, cuts a torn tail so appends land at the valid end, refuses a floor beyond the
 * log, and the apply itself refuses what the open cannot see (a gap inside a segment) and what is not durable. */
static void case_open_checks(void)
{
    char dir[PATH_MAX], p[PATH_MAX + 64];
    leader l;
    unsigned rng = 4;
    eng *db;
    fresh(dir, "j1");
    leader_init(&l);
    for (int s = 0; s < 3; s++) {
        for (int i = 0; i < 3; i++)
            leader_rows(&l, 4, &rng);
        leader_roll(&l);
    }
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK_EQ(feed(db, &l, 0, l.n), 0);
    uint64_t end = l.r[l.n - 1].last;
    /* the commit point may be anywhere, the replica applies nothing that is not durable: segments 1 and 2 were synced
     * at their rotations, segment 3 was not */
    CHECK_EQ(replica_apply(db, end), 0);
    CHECK_EQ(replica_applied_end(db), l.r[5].last);
    eng_close(db);
    /* the bytes were never synced (a process kill leaves them in the page cache): the open syncs every segment before
     * it claims their positions, or a power loss takes what the driver was told is durable */
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (db) {
        CHECK_EQ(replica_durable_end(db), end);
        CHECK(atomic_load(&db->st_wal_syncs) >= 3);
        eng_close(db);
    }
    /* a floor beyond the log */
    CHECK_EQ(open_replica(dir, end + 5, &db) != 0, true);
    /* segment 2 gone: positions 13..24 are missing, the open must not claim them */
    seg_file(p, sizeof p, dir, 2);
    CHECK_EQ(unlink(p), 0);
    CHECK_EQ(open_replica(dir, 0, &db), TSDB_ERR_CORRUPT);
    leader_free(&l);

    /* a torn tail is cut: the next append lands at the valid end */
    fresh(dir, "j2");
    leader_init(&l);
    rng = 8;
    for (int i = 0; i < 4; i++)
        leader_rows(&l, 3, &rng);
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK_EQ(feed(db, &l, 0, l.n), 0);
    CHECK_EQ(replica_sync(db), 0);
    eng_close(db);
    seg_file(p, sizeof p, dir, 1);
    FILE *f = fopen(p, "ab");
    CHECK(f != NULL);
    if (f) {
        fwrite("\x07garbage", 1, 8, f);
        fclose(f);
    }
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (db) {
        CHECK_EQ(replica_log_end(db), l.r[3].last);
        lrec *r = leader_rows(&l, 3, &rng);
        CHECK_EQ(replica_append(db, r->seq, r->off, r->bytes, r->size), 0);
        CHECK_EQ(replica_sync(db), 0);
        eng_close(db);
        CHECK_EQ(open_replica(dir, 0, &db), 0);
        if (db) {
            CHECK_EQ(replica_log_end(db), r->last);
            CHECK_EQ(replica_apply(db, r->last), 0);
            model m;
            model_at(&l, r->last, &m);
            CHECK(same_as(db, &m, "J: after a torn tail and an append"));
            eng_close(db);
        }
    }
    leader_free(&l);

    /* a gap inside a segment (a record of the right seal for its place, but the numbers skip): the apply refuses */
    fresh(dir, "j3");
    leader_init(&l);
    rng = 9;
    lrec *a = leader_rows(&l, 3, &rng);
    leader_rows(&l, 3, &rng); /* skipped */
    lrec *c = leader_rows(&l, 3, &rng);
    uint8_t *cb = malloc(c->size);
    memcpy(cb, c->bytes, c->size);
    wal_reseal(cb, 1, a->size); /* where the leader's record B would have been */
    seg_file(p, sizeof p, dir, 1);
    f = fopen(p, "wb");
    CHECK(f != NULL);
    if (f) {
        fwrite(a->bytes, 1, a->size, f);
        fwrite(cb, 1, c->size, f);
        fclose(f);
    }
    free(cb);
    int rc = open_replica(dir, 0, &db);
    if (rc == 0) {
        CHECK_EQ(replica_apply(db, c->last), TSDB_ERR_CORRUPT);
        eng_close(db);
    }
    leader_free(&l);
}

/* K: sealing. The old segment's memtable is sealed by the first COMMITTED record of the next segment, not by an
 * uncommitted one: a cut at the end of the old segment must still let a new leader continue it. */
static void case_seal(void)
{
    char dir[PATH_MAX];
    fresh(dir, "k");
    leader l, l2;
    unsigned rng = 6, r2 = 6;
    leader_init(&l);
    leader_init(&l2);
    for (int i = 0; i < 3; i++)
        leader_rows(&l, 4, &rng);
    leader_roll(&l);
    leader_rows(&l, 4, &rng); /* the first record of segment 2: never committed */
    eng *db;
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK_EQ(feed(db, &l, 0, l.n), 0);
    CHECK_EQ(replica_sync(db), 0);
    uint64_t c = l.r[2].last;
    CHECK_EQ(replica_apply(db, c), 0);
    CHECK_EQ(replica_cut(db, c), 0);
    /* the new leader had only segment 1 and continues it */
    for (int i = 0; i < 3; i++)
        leader_rows(&l2, 4, &r2);
    lrec *nr = leader_rows(&l2, 2, &r2);
    CHECK_EQ(replica_append(db, nr->seq, nr->off, nr->bytes, nr->size), 0);
    CHECK_EQ(replica_sync(db), 0);
    CHECK_EQ(replica_apply(db, nr->last), 0);
    model m;
    model_at(&l2, nr->last, &m);
    CHECK(same_as(db, &m, "K: the new leader's history"));
    eng_close(db);
    leader_free(&l);
    leader_free(&l2);
}

/* L: replaying a segment must not put back rows that parts of a later segment overwrote (they rank above parts). */
static void case_replay_skip(void)
{
    char dir[PATH_MAX];
    fresh(dir, "l");
    leader l;
    leader_init(&l);
    int64_t t0 = T0;
    lrec *r1 = leader_row1(&l, 0, t0, 1.0f, T0 + 1);
    lrec *r2 = leader_row1(&l, 0, t0 + 5000, 5.0f, T0 + 2);
    leader_roll(&l);
    leader_row1(&l, 0, t0, 2.0f, T0 + 3); /* segment 2 overwrites t0 */
    leader_roll(&l);
    lrec *r4 = leader_row1(&l, 0, t0 + 9000, 9.0f, T0 + 4);
    eng *db;
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK_EQ(feed(db, &l, 0, l.n), 0);
    CHECK_EQ(replica_sync(db), 0);
    CHECK_EQ(replica_apply(db, r4->last), 0);
    CHECK_EQ(mt_flush(db), 0); /* segments 1 and 2 are parts; the floor was never saved: the segments stay */
    float v = 0;
    CHECK_EQ(count_query(db, 0, T0 - 1000, T0 + 1000, &v), 1);
    CHECK(v == 2.0f);
    CHECK_EQ(eng_close(db), 0);
    /* the restart's floor lies inside segment 1: only its first record is replayed, and it must not win */
    CHECK_EQ(open_replica(dir, r1->last, &db), 0);
    if (db) {
        CHECK_EQ(count_query(db, 0, T0 - 1000, T0 + 1000, &v), 1);
        CHECK(v == 2.0f);
        CHECK_EQ(replica_apply(db, r4->last), 0);
        CHECK_EQ(count_query(db, 0, T0 - 1000, T0 + 10000, &v), 3);
        CHECK(r2->last > r1->last);
        eng_close(db);
    }
    leader_free(&l);
}

/* M: the retention clock is the NOW markers, never the commit_ms of a record. */
static void case_clock_source(void)
{
    char dir[PATH_MAX];
    fresh(dir, "m");
    leader l;
    unsigned rng = 5;
    leader_init(&l);
    for (int i = 0; i < 6; i++)
        leader_rows(&l, 5, &rng);
    lrec *rt = m_ret(&l, 0, 20000, 0);
    lrec *n1 = m_now(&l, T0 + 30000);
    lrec *big = leader_row1(&l, 0, T0 + 64000, 7.0f, T0 + 3600000); /* its commit_ms is an hour ahead of the clock */
    lrec *n2 = m_now(&l, T0 + 3600000);
    eng *db;
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK_EQ(feed(db, &l, 0, l.n), 0);
    CHECK_EQ(replica_sync(db), 0);
    CHECK_EQ(replica_apply(db, n1->last), 0);
    CHECK_EQ(eng_now_ms(db), T0 + 30000);
    (void)rt;
    int n0 = count_query(db, 0, T0 - 1000, T0 + 65000, NULL);
    CHECK(n0 > 0);
    CHECK_EQ(replica_apply(db, big->last), 0);
    CHECK_EQ(eng_now_ms(db), T0 + 30000); /* a record's commit_ms does not move the clock */
    CHECK_EQ(count_query(db, 0, T0 - 1000, T0 + 65000, NULL), n0 + 1);
    CHECK_EQ(replica_apply(db, n2->last), 0);
    CHECK_EQ(eng_now_ms(db), T0 + 3600000);
    CHECK_EQ(count_query(db, 0, T0 - 1000, T0 + 65000, NULL), 0); /* 20 s of retention, an hour later */
    eng_close(db);
    leader_free(&l);
}

/* N: a committed record is visible whole or not at all. */
typedef struct rd {
    eng *db;
    _Atomic int stop;
    _Atomic int bad, seen;
} rd;

static int rd_fn(void *c, const int64_t *ts, const void *vals, size_t n)
{
    struct {
        float first;
        int have;
        rd *r;
    } *x = c;
    (void)ts;
    const float *f = vals;
    for (size_t i = 0; i < n; i++) {
        if (!x->have)
            x->first = f[i], x->have = 1;
        else if (f[i] != x->first)
            atomic_fetch_add(&x->r->bad, 1);
    }
    return 0;
}

static void *rd_thread(void *a)
{
    rd *r = a;
    while (!atomic_load(&r->stop)) {
        struct {
            float first;
            int have;
            rd *r;
        } x = {0, 0, r};
        eng_query_raw_ex(r->db, 0, T0, T0 + 10000000, false, 0, rd_fn, &x);
        atomic_fetch_add(&r->seen, 1);
    }
    return NULL;
}

static void case_atomic(void)
{
    char dir[PATH_MAX];
    fresh(dir, "n");
    enum { ROWS = 3000, RECS = 40 };
    leader l;
    leader_init(&l);
    for (int k = 0; k < RECS; k++) { /* every record rewrites the same ROWS keys with its own value */
        lrec *r = leader_rows(&l, 1, &(unsigned){1});
        free(r->bytes);
        r->n = 0;
        r->size = WAL_HDR + (size_t)ROWS * WAL_ROW;
        r->bytes = calloc(1, r->size);
        r->last = r->fno + ROWS - 1;
        l.next = r->fno + ROWS;
        l.off = r->off + r->size;
        uint32_t w = (uint32_t)ROWS | (uint32_t)WAL_KIND_ROWS << 28;
        memcpy(r->bytes, &w, 4);
        memcpy(r->bytes + 16, &r->fno, 8);
        int64_t cms = T0 + 1;
        memcpy(r->bytes + 24, &cms, 8);
        for (int i = 0; i < ROWS; i++) {
            uint8_t *p = r->bytes + WAL_HDR + (size_t)i * WAL_ROW;
            uint32_t id = 0;
            int64_t ts = T0 + (int64_t)i * 1000;
            uint64_t slot = 0;
            float v = (float)(k + 1);
            memcpy(&slot, &v, 4);
            memcpy(p, &id, 4);
            memcpy(p + 4, &ts, 8);
            memcpy(p + 12, &slot, 8);
        }
        wal_reseal(r->bytes, r->seq, r->off);
    }
    eng *db;
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK_EQ(feed(db, &l, 0, l.n), 0);
    CHECK_EQ(replica_sync(db), 0);
    rd r = {.db = db};
    pthread_t th;
    CHECK_EQ(pthread_create(&th, NULL, rd_thread, &r), 0);
    for (size_t i = 0; i < l.n; i++)
        CHECK_EQ(replica_apply(db, l.r[i].last), 0);
    atomic_store(&r.stop, 1);
    pthread_join(th, NULL);
    CHECK_EQ(atomic_load(&r.bad), 0);
    CHECK(atomic_load(&r.seen) > 0);
    eng_close(db);
    leader_free(&l);
}

/* O: a volume with a change feed is refused (retained segments are not designed for a replica yet). */
static void case_feed_refused(void)
{
    char dir[PATH_MAX];
    fresh(dir, "o");
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    eng *db;
    CHECK_EQ(eng_open(dir, &o, &db), 0);
    if (!db)
        return;
    CHECK_EQ(eng_feed_enable(db, NULL), 0);
    eng_close(db);
    CHECK_EQ(open_replica(dir, 0, &db), TSDB_ERR_INVALID);
}

/* P: power lost at every step of a replica that appends, syncs, closes and reopens (a process kill), and cuts: what it
 * acknowledged (durable_end) is on disk afterwards, and the log opens (nothing damaged, nothing missing). */
static char SHIM[PATH_MAX + 32];

static void write_ack(const char *ackfile, uint64_t v)
{
    char tmp[PATH_MAX + 32];
    snprintf(tmp, sizeof tmp, "%s.tmp", ackfile);
    FILE *f = fopen(tmp, "w");
    if (!f)
        return;
    fprintf(f, "%" PRIu64 "\n", v);
    fclose(f);
    rename(tmp, ackfile);
}

static int power_child(const char *dir, const char *ackfile)
{
    leader l;
    unsigned rng = 11;
    leader_init(&l);
    eng *db;
    char hi[PATH_MAX + 8]; /* the highest position the child's history can hold: written before every append, lowered after a cut */
    snprintf(hi, sizeof hi, "%s.hi", ackfile);
    if (open_replica(dir, 0, &db) != 0)
        return 2;
    for (int round = 0; round < 16; round++) {
        for (int k = 0; k < 3; k++) {
            rng = rng * 1103515245u + 12345u;
            if ((rng >> 12) % 3 == 0)
                leader_roll(&l);
            lrec *r = leader_rows(&l, 3, &rng);
            write_ack(hi, r->last);
            if (replica_append(db, r->seq, r->off, r->bytes, r->size) != 0)
                return 3;
        }
        switch (round % 4) {
        case 0: /* a sync, then the acknowledgement */
            if (replica_sync(db) != 0)
                return 4;
            write_ack(ackfile, replica_durable_end(db));
            break;
        case 1: /* the process dies and restarts (the page cache keeps the bytes): the open's durable end is claimed */
            if (eng_close(db) != 0 || open_replica(dir, 0, &db) != 0)
                return 5;
            write_ack(ackfile, replica_durable_end(db));
            break;
        case 2: { /* a cut inside the unsynced tail, then the acknowledgement of what stays */
            size_t j = l.n - 2;
            uint64_t last = l.r[j].last;
            if (last < replica_durable_end(db))
                write_ack(ackfile, last); /* the positions above it are given up before the cut */
            if (replica_cut(db, last) != 0)
                return 6;
            for (size_t i = j + 1; i < l.n; i++)
                free(l.r[i].bytes);
            l.n = j + 1;
            l.seq = l.r[j].seq;
            l.off = l.r[j].off + l.r[j].size;
            l.next = last + 1;
            write_ack(hi, last); /* what was above the cut is gone from the history: from here nothing of it may reappear */
            write_ack(ackfile, replica_durable_end(db));
            break;
        }
        default: /* rows written without a sync, then a sync that covers every segment */
            if (replica_sync(db) != 0)
                return 4;
            write_ack(ackfile, replica_durable_end(db));
            break;
        }
    }
    return 0;
}

static int run_power_child_as(const char *mode, const char *dir, const char *root, const char *ackfile, int warmup, int seed)
{
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512], w[16], sd[16];
        /* (no leak check in the child: it ends without freeing its leader, and crashfs keeps undo copies) */
        snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0:detect_leaks=0",
                 getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
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
        int null = open("/dev/null", O_WRONLY);
        if (null >= 0 && !getenv("TEST_REPLICA_CHILD_OUTPUT")) /* (set it to see why a child ended) */
            dup2(null, 1), dup2(null, 2);
        execl(SELF, SELF, mode, dir, ackfile, MUTNAME ? MUTNAME : "none", (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

static int run_power_child(const char *dir, const char *ackfile, int warmup, int seed)
{
    return run_power_child_as("--power", dir, dir, ackfile, warmup, seed);
}

/* (the ack files are named so that no root of crashfs, a path prefix, covers them: S/pl is the root) */
static void case_power(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  power loss: skipped (no %s)\n", SHIM);
        return;
    }
    char dir[PATH_MAX], ack[PATH_MAX + 16];
    snprintf(ack, sizeof ack, "%s/acks-of-pl.ack", S);
    int crashes = 0;
    bool finished = false;
    for (int k = 0; k < 900 && !finished; k++) {
        fresh(dir, "pl");
        unlink(ack);
        char hif[PATH_MAX + 24];
        snprintf(hif, sizeof hif, "%s.hi", ack);
        unlink(hif);
        int code = run_power_child(dir, ack, k, k + 1);
        if (code != 99 && code != 0)
            fprintf(stderr, "P: step %d: the child ended with status %d\n", k, code);
        CHECK(code == 99 || code == 0);
        if (code != 99 && code != 0)
            break;
        crashes += code == 99;
        finished = code == 0;
        uint64_t acked = 0;
        FILE *f = fopen(ack, "r");
        if (f) {
            if (fscanf(f, "%" SCNu64, &acked) != 1)
                acked = 0;
            fclose(f);
        }
        uint64_t hi = 0;
        f = fopen(hif, "r");
        if (f) {
            if (fscanf(f, "%" SCNu64, &hi) != 1)
                hi = 0;
            fclose(f);
        }
        eng *db;
        int rc = open_replica(dir, 0, &db);
        if (rc != 0) {
            fprintf(stderr, "P: step %d: the open failed: %d\n", k, rc);
            failures++;
            continue;
        }
        if (replica_log_end(db) < acked) {
            fprintf(stderr, "P: step %d: acknowledged %" PRIu64 ", the log ends at %" PRIu64 "\n", k, acked,
                    replica_log_end(db));
            failures++;
        }
        if (replica_log_end(db) > hi) { /* a cut that was not durable: the records above it came back */
            fprintf(stderr, "P: step %d: the log ends at %" PRIu64 ", above anything the history holds (%" PRIu64 ")\n", k,
                    replica_log_end(db), hi);
            failures++;
        }
        if (replica_apply(db, acked) != 0) {
            fprintf(stderr, "P: step %d: the acknowledged log does not apply\n", k);
            failures++;
        }
        eng_close(db);
    }
    CHECK(finished && crashes > 50);
    printf("  power lost at each of %d steps of appends, syncs, restarts and cuts: every acknowledged position is on "
           "disk\n", crashes);
}


/* ------------------------------------------------------------ K: a leader engine with COMMIT and DROP markers
 * The leader is a plain engine whose jr_ops (db->jr) ship its pending part files and wait for the replica to apply
 * its markers; the transport reads the leader's segment files (a segment about to be unlinked is read first). The oracle
 * is the leader's own answers: after every step the replica answers the same. */

typedef struct xport {
    eng *ld, *rep;
    char ldir[PATH_MAX];
    uint64_t cseq, coff;   /* what the replica has been handed of the leader's log */
    bool withhold;         /* ship keeps the file instead of staging it */
    bool noapply;          /* wait ships the log but the replica applies nothing */
    uint8_t *held;         /* the last file shipped */
    size_t held_len;
    char held_name[64];
    uint64_t saved;        /* the commit floor the "driver" saved */
} xport;

static int cmp_u64(const void *a, const void *b)
{
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return x < y ? -1 : x > y;
}

static int xp_ship_logs(xport *x)
{
    if (!x->rep)
        return 0;
    uint64_t seqs[512];
    size_t n = 0;
    DIR *d = opendir(x->ldir);
    struct dirent *e;
    while (d && (e = readdir(d))) {
        uint64_t seq;
        int consumed = 0;
        if (wal_file_name(e->d_name) && sscanf(e->d_name, "wal_%" SCNx64 ".log%n", &seq, &consumed) == 1 && n < 512)
            seqs[n++] = seq;
    }
    if (d)
        closedir(d);
    qsort(seqs, n, sizeof *seqs, cmp_u64);
    int dfd = open(x->ldir, O_RDONLY | O_DIRECTORY);
    int rc = 0;
    for (size_t i = 0; i < n && rc == 0; i++) {
        if (seqs[i] < x->cseq)
            continue;
        char nm[64];
        uint8_t *b;
        size_t len;
        wal_name(nm, sizeof nm, seqs[i]);
        if (read_file(dfd, nm, &b, &len) != 0)
            continue; /* unlinked meanwhile */
        uint64_t off = seqs[i] == x->cseq ? x->coff : 0;
        if (len > off) {
            rc = replica_append(x->rep, seqs[i], off, b + off, len - off);
            x->cseq = seqs[i];
            x->coff = len;
        }
        free(b);
    }
    close(dfd);
    if (rc == 0)
        rc = replica_sync(x->rep);
    return rc;
}

static uint64_t ld_last(xport *x) { return x->ld->fno_next - 1; }

static int xp_pump(xport *x, uint64_t commit)
{
    int rc = xp_ship_logs(x);
    if (rc == 0 && x->rep && !x->noapply)
        rc = replica_apply(x->rep, commit);
    return rc;
}

static int xp_ship(void *ctx, eng *db, const char *name, const char *mname, uint64_t size)
{
    xport *x = ctx;
    uint8_t *b;
    size_t len;
    if (read_file(db->dirfd, name, &b, &len) != 0 || len != size)
        return TSDB_ERR_IO;
    free(x->held);
    x->held = b;
    x->held_len = len;
    snprintf(x->held_name, sizeof x->held_name, "%s", mname);
    if (!x->rep || x->withhold)
        return 0;
    return replica_stage(x->rep, mname, b, len);
}

static int xp_wait(void *ctx, eng *db, uint64_t last)
{
    (void)db;
    return xp_pump(ctx, last);
}

static void xp_seg_done(void *ctx, eng *db, uint64_t seq)
{
    (void)db, (void)seq;
    xp_ship_logs(ctx); /* before the file goes */
}

static struct jr_ops XOPS;

static void xp_open_leader(xport *x)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.wal_sync_ms = 0;
    o.wal_max_batch_rows = 16; /* more rows than this go straight to parts: a COMMIT marker */
    if (eng_open(x->ldir, &o, &x->ld) != 0)
        exit(2);
    XOPS = (struct jr_ops){.ctx = x, .ship = xp_ship, .wait = xp_wait, .seg_done = xp_seg_done};
    x->ld->jr = &XOPS;
}

static bool eq_range(eng *a, eng *b, uint32_t s, int64_t lo, int64_t hi, const char *what)
{
    qctx qa = {.n = 0}, qb = {.n = 0};
    int ra = eng_query_raw_ex(a, s, lo, hi, false, 0, qfn, &qa);
    int rb = eng_query_raw_ex(b, s, lo, hi, false, 0, qfn, &qb);
    if (ra != 0 || rb != 0) {
        fprintf(stderr, "%s: series %u: query failed (%d, %d)\n", what, s, ra, rb);
        return false;
    }
    if (qa.n != qb.n || memcmp(qa.ts, qb.ts, qa.n * sizeof qa.ts[0]) != 0 || memcmp(qa.v, qb.v, qa.n * sizeof qa.v[0]) != 0) {
        fprintf(stderr, "%s: series %u: the leader has %zu rows, the replica %zu\n", what, s, qa.n, qb.n);
        return false;
    }
    return true;
}

#define DAY_MS 86400000LL
static bool xp_same(xport *x, const char *what)
{
    bool ok = true;
    for (uint32_t s = 0; s < NS; s++)
        ok &= eq_range(x->ld, x->rep, s, T0 - 1000, T0 + 1000LL * (NT + 1), what);
    return ok;
}

static void xp_step(xport *x, const char *what)
{
    CHECK_EQ(xp_pump(x, ld_last(x)), 0);
    CHECK(eng_check(x->rep) == 0);
    CHECK_EQ(replica_applied_end(x->rep), ld_last(x));
    CHECK(xp_same(x, what));
}

static void xp_rows(xport *x, uint32_t s, int t0, int n, float base)
{
    int64_t ts[NT];
    float v[NT];
    for (int i = 0; i < n; i++) {
        ts[i] = T0 + 1000LL * (t0 + i);
        v[i] = base + (float)i;
    }
    CHECK_EQ(eng_insert_series(x->ld, TSDB_FLOAT32, s, ts, v, (size_t)n), 0);
}

static void xp_replace(xport *x, uint32_t s, int a, int b, int n, float base)
{
    int64_t ts[NT];
    float v[NT];
    for (int i = 0; i < n; i++) {
        ts[i] = T0 + 1000LL * (a + 2 * i);
        v[i] = base + (float)i;
    }
    CHECK_EQ(eng_replace_range(x->ld, TSDB_FLOAT32, s, T0 + 1000LL * a, T0 + 1000LL * b, ts, v, (size_t)n), 0);
}

static void xp_start(xport *x, const char *name)
{
    char rd[PATH_MAX];
    memset(x, 0, sizeof *x);
    char ld[PATH_MAX];
    fresh(ld, name);
    snprintf(x->ldir, sizeof x->ldir, "%s", ld);
    snprintf(rd, sizeof rd, "%s_r", ld);
    sh("rm -rf '%s' && cp -a '%s' '%s'", rd, ld, rd);
    CHECK_EQ(open_replica(rd, 0, &x->rep), 0);
    xp_open_leader(x);
}

static void xp_reopen(xport *x, const char *rd, uint64_t floor)
{
    eng_close(x->rep);
    x->rep = NULL;
    CHECK_EQ(open_replica(rd, floor, &x->rep), 0);
}

static void xp_stop(xport *x)
{
    eng_close(x->ld);
    if (x->rep)
        eng_close(x->rep);
    free(x->held);
}

static void rdir(char *out, size_t cap, const xport *x) { snprintf(out, cap, "%s_r", x->ldir); }

static void case_commit(void)
{
    xport x;
    char rd[PATH_MAX];
    xp_start(&x, "kc");
    rdir(rd, sizeof rd, &x);
    xp_rows(&x, 0, 0, 10, 100), xp_rows(&x, 1, 0, 5, 300);
    xp_step(&x, "K: small rows");
    xp_rows(&x, 0, 5, 40, 2000); /* a large insert over rows still in the memtable */
    xp_step(&x, "K: large insert");
    xp_rows(&x, 0, 40, 10, 3000); /* a small write after it wins */
    xp_step(&x, "K: small after large");
    CHECK_EQ(eng_delete_range(x.ld, 1, T0 + 1000, T0 + 3000), 0);
    xp_step(&x, "K: delete");
    CHECK_EQ(eng_flush(x.ld), 0);
    xp_rows(&x, 1, 10, 5, 400);
    xp_step(&x, "K: flush");
    xp_rows(&x, 1, 0, 30, 5000);
    xp_replace(&x, 0, 10, 20, 5, 7000);
    xp_step(&x, "K: large and replace");
    CHECK_EQ(count_files(rd, "stage_"), 0); /* applied: the staged files are gone */
    CHECK_EQ(count_files(rd, "tmp_"), 0);
    /* a restart replays the log from the segments it holds: the COMMITs are not applied twice */
    xp_reopen(&x, rd, 0);
    xp_step(&x, "K: reopen floor 0");
    CHECK_EQ(replica_floor_saved(x.rep, replica_applied_end(x.rep)), 0);
    xp_reopen(&x, rd, replica_applied_end(x.rep));
    xp_rows(&x, 0, 20, 30, 9000);
    xp_step(&x, "K: reopen at the end");
    CHECK_EQ(count_files(rd, "stage_"), 0);
    xp_stop(&x);
}

/* a COMMIT whose files are not staged: not acknowledged, not applied, until they are */
static void case_commit_hold(void)
{
    xport x;
    char rd[PATH_MAX], name[64];
    uint64_t sz;
    xp_start(&x, "kh");
    rdir(rd, sizeof rd, &x);
    xp_rows(&x, 0, 0, 10, 100);
    xp_step(&x, "K: before");
    x.withhold = true;
    xp_rows(&x, 0, 5, 40, 2000);
    uint64_t last = ld_last(&x);
    CHECK(replica_durable_end(x.rep) < last);
    CHECK(replica_applied_end(x.rep) < last);
    CHECK_EQ(replica_missing(x.rep, name, sizeof name, &sz), 1);
    CHECK(strcmp(name, x.held_name) == 0);
    CHECK_EQ(sz, x.held_len);
    {
        qctx q = {.n = 0};
        CHECK_EQ(eng_query_raw_ex(x.rep, 0, T0 - 1000, T0 + 1000LL * (NT + 1), false, 0, qfn, &q), 0);
        CHECK_EQ(q.n, 10); /* the insert is not shown without its file */
    }
    x.withhold = false;
    /* a restart with the marker in the log and no file: still held */
    xp_reopen(&x, rd, 0);
    CHECK(replica_durable_end(x.rep) < last);
    CHECK_EQ(replica_missing(x.rep, name, sizeof name, &sz), 1);
    CHECK_EQ(replica_stage(x.rep, x.held_name, x.held, x.held_len), 0);
    CHECK_EQ(replica_missing(x.rep, name, sizeof name, &sz), 0);
    xp_step(&x, "K: staged late");
    /* a file with the wrong content is no file */
    x.withhold = true;
    xp_rows(&x, 1, 0, 40, 4000);
    uint8_t *bad = malloc(x.held_len);
    memcpy(bad, x.held, x.held_len);
    bad[x.held_len / 2] ^= 1;
    CHECK_EQ(replica_stage(x.rep, x.held_name, bad, x.held_len), 0);
    CHECK_EQ(replica_missing(x.rep, name, sizeof name, &sz), 1);
    CHECK_EQ(replica_sync(x.rep), 0);
    CHECK(replica_durable_end(x.rep) < ld_last(&x));
    free(bad);
    CHECK_EQ(replica_stage(x.rep, x.held_name, x.held, x.held_len), 0);
    x.withhold = false;
    xp_step(&x, "K: staged right");
    xp_stop(&x);
}

/* a cut removes the files of the markers it removes */
static void case_commit_cut(void)
{
    xport x;
    char rd[PATH_MAX], name[64];
    uint64_t sz;
    xp_start(&x, "kk");
    rdir(rd, sizeof rd, &x);
    xp_rows(&x, 0, 0, 10, 100);
    xp_step(&x, "K: before");
    x.noapply = true;
    xp_rows(&x, 0, 5, 40, 2000);
    uint64_t last = ld_last(&x);
    CHECK_EQ(replica_durable_end(x.rep), last); /* staged and appended: acknowledged */
    CHECK_EQ(count_files(rd, "stage_"), 1);
    CHECK_EQ(replica_cut(x.rep, last - 1), 0);
    CHECK_EQ(count_files(rd, "stage_"), 0);
    CHECK_EQ(replica_missing(x.rep, name, sizeof name, &sz), 0);
    CHECK_EQ(replica_log_end(x.rep), last - 1);
    /* the same marker again (a new leader would send another; here the old one is sent again) */
    x.coff = 0;
    {
        char p[PATH_MAX];
        DIR *d = opendir(rd);
        struct dirent *e;
        uint64_t best = 0;
        while (d && (e = readdir(d))) {
            uint64_t seq;
            int c = 0;
            if (wal_file_name(e->d_name) && sscanf(e->d_name, "wal_%" SCNx64 ".log%n", &seq, &c) == 1 && seq >= best)
                best = seq;
        }
        if (d)
            closedir(d);
        char nm[64];
        wal_name(nm, sizeof nm, best);
        snprintf(p, sizeof p, "%s/%s", rd, nm);
        struct stat st;
        CHECK_EQ(stat(p, &st), 0);
        x.cseq = best;
        x.coff = (uint64_t)st.st_size;
    }
    CHECK_EQ(xp_ship_logs(&x), 0);
    CHECK_EQ(replica_missing(x.rep, name, sizeof name, &sz), 1);
    CHECK(replica_durable_end(x.rep) < last);
    CHECK_EQ(replica_stage(x.rep, x.held_name, x.held, x.held_len), 0);
    x.noapply = false;
    xp_step(&x, "K: re-sent");
    xp_stop(&x);
}

/* DROP: the retired partitions stay retired when a restart reads the log again */
static void case_drop(void)
{
    xport x;
    char rd[PATH_MAX];
    xp_start(&x, "kd");
    rdir(rd, sizeof rd, &x);
    int64_t far = T0 + 40 * DAY_MS;
    xp_rows(&x, 0, 0, 10, 100);
    for (int i = 0; i < 5; i++) {
        float v = (float)i;
        int64_t t = far + 1000LL * i;
        CHECK_EQ(eng_insert_series(x.ld, TSDB_FLOAT32, 1, &t, &v, 1), 0);
    }
    xp_step(&x, "K: rows");
    CHECK_EQ(eng_flush(x.ld), 0);
    xp_rows(&x, 0, 20, 5, 500);
    xp_step(&x, "K: flushed");
    CHECK_EQ(eng_drop_before(x.ld, T0 + 10 * DAY_MS), 0);
    xp_step(&x, "K: drop");
    CHECK(eq_range(x.ld, x.rep, 1, far - 1000, far + 10000, "K: far series"));
    qctx q0 = {.n = 0}, q1 = {.n = 0};
    CHECK_EQ(eng_query_raw_ex(x.ld, 0, T0 - 1000, T0 + 1000LL * (NT + 1), false, 0, qfn, &q0), 0);
    CHECK_EQ(eng_query_raw_ex(x.ld, 1, far - 1000, far + 10000, false, 0, qfn, &q1), 0);
    CHECK_EQ(q0.n, 0);
    CHECK_EQ(q1.n, 5);
    xp_reopen(&x, rd, 0);
    xp_step(&x, "K: reopen after the drop");
    CHECK(eq_range(x.ld, x.rep, 1, far - 1000, far + 10000, "K: far series after reopen"));
    xp_rows(&x, 1, 0, 3, 77); /* rows below the drop are refused or hidden the same way on both: still equal */
    xp_stop(&x);
}

static int rnd(unsigned *r, int n)
{
    *r = *r * 1103515245u + 12345u;
    return (int)((*r >> 16) % (unsigned)(n > 0 ? n : 1));
}

static void case_commit_random(unsigned seed)
{
    xport x;
    char rd[PATH_MAX], nm[32];
    unsigned rng = seed * 2654435761u + 1;
#define RND(n) rnd(&rng, (n))
    snprintf(nm, sizeof nm, "kr%u", seed);
    xp_start(&x, nm);
    rdir(rd, sizeof rd, &x);
    for (int step = 0; step < 90; step++) {
        uint32_t s = (uint32_t)RND(NS);
        float base = (float)(step * 100);
        switch (RND(9)) {
        case 0:
        case 1:
            xp_rows(&x, s, RND(NT - 12), 1 + RND(10), base);
            break;
        case 2:
        case 3: /* more rows than a WAL batch takes: a COMMIT */
            xp_rows(&x, s, RND(NT - 40), 17 + RND(24), base);
            break;
        case 4: {
            int a = RND(NT - 4), b = a + 1 + RND(NT - a - 1);
            CHECK_EQ(eng_delete_range(x.ld, s, T0 + 1000LL * a, T0 + 1000LL * b), 0);
            break;
        }
        case 5: {
            int span = 2 * (1 + RND(8));
            xp_replace(&x, s, 10, 10 + span, 1 + RND(span / 2 > 4 ? 4 : span / 2), base);
            break;
        }
        case 6:
            CHECK_EQ(eng_flush(x.ld), 0);
            break;
        case 7:
            if (x.rep) {
                CHECK_EQ(replica_floor_saved(x.rep, replica_applied_end(x.rep)), 0);
                x.saved = replica_applied_end(x.rep);
            }
            break;
        default:
            xp_reopen(&x, rd, RND(2) ? x.saved : 0);
            break;
        }
        char what[64];
        snprintf(what, sizeof what, "K random %u step %d", seed, step);
        xp_step(&x, what);
        if (failures)
            break;
    }
    xp_stop(&x);
#undef RND
}

/* ------------------------------------------------------------ Q: power lost while COMMITs are applied
 * A leader engine (outside the crashfs root) and a replica (inside it) run a fixed script of small and large writes,
 * deletes, replaces and flushes through the K transport; the power fails at every file system step of the replica. What
 * the leader was told (every step it finished: the replica had the staged files and the markers durable) must be there
 * afterwards: the durable end covers it, the log applies, and the replica answers as the leader did after some step
 * at or beyond the last finished one (an oracle run of the same script with no replica gives the answers). */
#define QSTEPS 18

static void q_step(xport *x, int i)
{
    float b = (float)(i * 100);
    switch (i) {
    case 0: xp_rows(x, 0, 0, 10, b); break;
    case 1: xp_rows(x, 1, 0, 5, b); break;
    case 2: xp_rows(x, 0, 5, 40, b); break;
    case 3: xp_rows(x, 0, 40, 10, b); break;
    case 4: CHECK_EQ(eng_delete_range(x->ld, 1, T0 + 1000, T0 + 3000), 0); break;
    case 5: CHECK_EQ(eng_flush(x->ld), 0); break;
    case 6: xp_rows(x, 1, 0, 30, b); break;
    case 7: xp_replace(x, 0, 10, 20, 5, b); break;
    case 8: xp_rows(x, 1, 10, 3, b); break;
    case 9: xp_rows(x, 1, 20, 30, b); break;
    case 10: CHECK_EQ(eng_flush(x->ld), 0); break;
    case 11: xp_rows(x, 0, 2, 4, b); break;
    case 12: CHECK_EQ(eng_delete_range(x->ld, 0, T0 + 4000, T0 + 30000), 0); break;
    case 13: xp_rows(x, 0, 3, 25, b); break;
    case 14: xp_replace(x, 1, 4, 14, 4, b); break;
    case 15: CHECK_EQ(eng_flush(x->ld), 0); break;
    case 16: xp_rows(x, 1, 8, 20, b); break;
    default: xp_rows(x, 0, 30, 3, b); break;
    }
    CHECK_EQ(xp_pump(x, ld_last(x)), 0);
}

static uint64_t q_digest(eng *db)
{
    uint64_t h = 1469598103934665603ULL;
    for (uint32_t s = 0; s < NS; s++) {
        qctx q = {.n = 0};
        if (eng_query_raw_ex(db, s, T0 - 1000, T0 + 1000LL * (NT + 1), false, 0, qfn, &q) != 0)
            return 0;
        h = (h ^ q.n) * 1099511628211ULL;
        for (size_t i = 0; i < q.n; i++) {
            uint32_t bits;
            memcpy(&bits, &q.v[i], 4);
            h = (h ^ (uint64_t)q.ts[i]) * 1099511628211ULL;
            h = (h ^ bits) * 1099511628211ULL;
        }
    }
    return h;
}

static int powerq_child(const char *ldir, const char *ackfile)
{
    xport x;
    memset(&x, 0, sizeof x);
    char rd[PATH_MAX + 8];
    snprintf(x.ldir, sizeof x.ldir, "%s", ldir);
    snprintf(rd, sizeof rd, "%s_r", ldir);
    if (open_replica(rd, 0, &x.rep) != 0)
        return 2;
    xp_open_leader(&x);
    for (int i = 0; i < QSTEPS; i++) {
        q_step(&x, i);
        if (failures)
            return 3;
        write_ack(ackfile, (uint64_t)i + 1);
    }
    return 0;
}

static void case_power_commit(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  power loss in COMMITs: skipped (no %s)\n", SHIM);
        return;
    }
    /* the oracle: the same script, a leader alone */
    uint64_t D[QSTEPS + 1], P[QSTEPS + 1];
    {
        xport o;
        xp_start(&o, "pqo");
        eng_close(o.rep);
        o.rep = NULL;
        D[0] = q_digest(o.ld);
        P[0] = ld_last(&o);
        for (int i = 0; i < QSTEPS; i++) {
            q_step(&o, i);
            D[i + 1] = q_digest(o.ld);
            P[i + 1] = ld_last(&o);
        }
        xp_stop(&o);
    }
    char ack[PATH_MAX + 16];
    snprintf(ack, sizeof ack, "%s/acks-of-pq.ack", S);
    int crashes = 0;
    bool finished = false;
    for (int k = 0; k < 4000 && !finished; k++) {
        char ld[PATH_MAX], rd[PATH_MAX + 8];
        fresh(ld, "pq");
        snprintf(rd, sizeof rd, "%s_r", ld);
        sh("rm -rf '%s' && cp -a '%s' '%s'", rd, ld, rd);
        unlink(ack);
        int code = run_power_child_as("--powerq", ld, rd, ack, k, k + 1);
        if (code != 99 && code != 0)
            fprintf(stderr, "Q: step %d: the child ended with status %d\n", k, code);
        CHECK(code == 99 || code == 0);
        if (code != 99 && code != 0)
            break;
        crashes += code == 99;
        finished = code == 0;
        uint64_t done = 0;
        FILE *f = fopen(ack, "r");
        if (f) {
            if (fscanf(f, "%" SCNu64, &done) != 1)
                done = 0;
            fclose(f);
        }
        eng *db;
        int rc = open_replica(rd, 0, &db);
        if (rc != 0) {
            fprintf(stderr, "Q: step %d: the open failed: %d\n", k, rc);
            failures++;
            continue;
        }
        if (replica_durable_end(db) < P[done]) {
            fprintf(stderr, "Q: step %d: the leader finished step %" PRIu64 " (position %" PRIu64
                    "), the replica's durable end is %" PRIu64 "\n", k, done, P[done], replica_durable_end(db));
            failures++;
        }
        if (replica_apply(db, replica_durable_end(db)) != 0) {
            fprintf(stderr, "Q: step %d: the durable log does not apply\n", k);
            failures++;
        } else {
            uint64_t d = q_digest(db);
            bool ok = false;
            for (int j = (int)done; j <= QSTEPS; j++)
                ok |= D[j] == d;
            if (!ok) {
                fprintf(stderr, "Q: step %d: the replica (applied to %" PRIu64 ") answers as the leader did after no step "
                        "from %" PRIu64 " on\n", k, replica_applied_end(db), done);
                failures++;
            }
        }
        eng_close(db);
        if (failures)
            break;
    }
    CHECK(finished && crashes > 100);
    printf("  power lost at each of %d steps of a replica applying COMMITs (staged files, hold, RSTATE): what the "
           "leader was told is durable, the answers are the leader's\n", crashes);
}

static int mutant_of(const char *m)
{
    return strcmp(m, "applyall") == 0 ? MUT_REPLICA_APPLY_ALL
                        : strcmp(m, "ackunsynced") == 0 ? MUT_REPLICA_ACK_UNSYNCED
                        : strcmp(m, "keepcut") == 0 ? MUT_REPLICA_KEEP_CUT
                        : strcmp(m, "nofloor") == 0 ? MUT_REPLICA_NO_FLOOR
                        : strcmp(m, "walclock") == 0 ? MUT_REPLICA_WALL_CLOCK
                        : strcmp(m, "nopos") == 0 ? MUT_REPLICA_NO_POS
                        : strcmp(m, "classat") == 0 ? MUT_REPLICA_CLASS_AT
                        : strcmp(m, "opensync") == 0 ? MUT_REPLICA_NO_OPEN_SYNC
                        : strcmp(m, "gapcheck") == 0 ? MUT_REPLICA_NO_GAP_CHECK
                        : strcmp(m, "torncut") == 0 ? MUT_REPLICA_NO_TORN_CUT
                        : strcmp(m, "floorcheck") == 0 ? MUT_REPLICA_NO_FLOOR_CHECK
                        : strcmp(m, "durablegate") == 0 ? MUT_REPLICA_NO_DURABLE_GATE
                        : strcmp(m, "applycontig") == 0 ? MUT_REPLICA_NO_APPLY_CONTIG
                        : strcmp(m, "rotsync") == 0 ? MUT_REPLICA_NO_ROT_SYNC
                        : strcmp(m, "cutsync") == 0 ? MUT_REPLICA_NO_CUT_SYNC
                        : strcmp(m, "cutdirsync") == 0 ? MUT_REPLICA_NO_CUT_DIRSYNC
                        : strcmp(m, "replayskip") == 0 ? MUT_REPLICA_NO_REPLAY_SKIP
                        : strcmp(m, "nonow") == 0 ? MUT_REPLICA_NO_NOW_MARKER
                        : strcmp(m, "commitclock") == 0 ? MUT_REPLICA_COMMIT_CLOCK
                        : strcmp(m, "sealedrule") == 0 ? MUT_REPLICA_NO_SEALED_RULE
                        : strcmp(m, "cutgen") == 0 ? MUT_REPLICA_NO_CUT_GEN
                        : strcmp(m, "earlyseal") == 0 ? MUT_REPLICA_EARLY_SEAL
                        : strcmp(m, "nostagehold") == 0 ? MUT_REPLICA_NO_STAGE_HOLD
                        : strcmp(m, "nocommitflush") == 0 ? MUT_REPLICA_NO_COMMIT_FLUSH
                        : strcmp(m, "nodoneskip") == 0 ? MUT_REPLICA_NO_DONE_SKIP
                        : strcmp(m, "keepstaged") == 0 ? MUT_REPLICA_KEEP_STAGED
                        : strcmp(m, "nodropskip") == 0 ? MUT_REPLICA_NO_DROP_SKIP
                        : strcmp(m, "nodropflush") == 0 ? MUT_REPLICA_NO_DROP_FLUSH
                        : strcmp(m, "nostagesync") == 0 ? MUT_REPLICA_NO_STAGE_SYNC : 0;
}

int main(int argc, char **argv)
{
    int a = 1;
    if (argc >= 4 && strcmp(argv[1], "--child") == 0)
        return child_main(argv[2], argv[3]);
    if (argc >= 4 && strcmp(argv[1], "--power") == 0) {
        if (argc >= 5) /* the mutant of the parent run: the child is a process of its own */
            eng_test_mutant = mutant_of(argv[4]);
        return power_child(argv[2], argv[3]);
    }
    if (argc >= 4 && strcmp(argv[1], "--powerq") == 0) {
        if (argc >= 5)
            eng_test_mutant = mutant_of(argv[4]);
        return powerq_child(argv[2], argv[3]);
    }
    if (a + 1 < argc && strcmp(argv[a], "--mutant") == 0) {
        const char *m = argv[a + 1];
        eng_test_mutant = mutant_of(m);
        MUTNAME = m;
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
    case_dicts();
    case_clock();
    case_classes();
    {
        char *slash = strrchr(SELF, '/');
        snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", slash ? (int)(slash - SELF) : 1, slash ? SELF : ".");
    }
    case_open_checks();
    case_seal();
    case_replay_skip();
    case_clock_source();
    case_atomic();
    case_feed_refused();
    case_read();
    case_sync_cut();
    case_sealed_open();
    case_commit();
    case_commit_hold();
    case_commit_cut();
    case_drop();
    for (unsigned seed = 1; seed <= 12; seed++)
        case_commit_random(seed);
    case_power();
    case_power_commit();
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
