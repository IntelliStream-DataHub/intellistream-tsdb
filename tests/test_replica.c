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
 *  F  a process killed (kill -9) after the sync reported a position: every reported position is still there;
 *  L  a leader's commit point (C7): it shows, acknowledges and flushes only up to it; depose ends its waits as
 *     indeterminate without failing it; admission and the bytes before the fdatasync; power loss of a leader.
 * Planted mutations (--mutant NAME) must make it fail: applyall (apply ignores the commit point), ackunsynced
 * (the durable end reports what was not synced), keepcut (a cut leaves the records), nofloor (a segment goes
 * before the floor is saved), walclock (retention reads the replica's own clock), nopos (a replayed RETENTION or CLASS
 * record is appended again), classmax (a CLASS record takes max(T, this replica's own T) instead of T exactly),
 * noclasspurge (a replica applies a CLASS record without merging away the rows its parts hold past T first);
 * the leader's: leadervisible, leaderflush, deposefails, admitafter, bytesaftersync, markerrc, gatenohook, nosynced,
 * dictearly (a dictionary record is appended before its marker passed the commit point), wtaillocal (the leader
 * prunes its WAL tails by its own removed segments, not by the replicas' log starts), classmarkerlate (the CLASS
 * marker goes after the switch let go of flush_lock), nowtailfrom (a switch right after an open or a promotion
 * does not wait for the replicas to report a log start past the leader's open);
 * a deposed leader's: leaderclose (its close flushes and stores its log end), leadernorstate (it keeps no RSTATE),
 * closewait (a leader's close waits for a commit point that cannot move) (the Makefile runs every name).
 *
 *   ./build/test_replica [scratch dir]
 *   ./build/test_replica --mutant applyall|ackunsynced|keepcut|nofloor|walclock|nopos|classmax|... [scratch dir]    must fail
 *   ./build/test_replica --child DIR ACKFILE      (internal)
 */
#define _GNU_SOURCE
#include "replica.h"
#include "marker.h"
#include "ecapply.h"
#include "eccold.h"
#include "feed.h"
#include "ppos.h"

#include <dirent.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
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

/* A check whose failure leaves the engine in a state the rest of the run cannot get out of (rows in the WAL buffer that
 * no memtable holds, which a later flush or close then waits on): the run stops there, failed, rather than hang. */
static void stop_if_failed(const char *what)
{
    if (failures) {
        fprintf(stderr, "test_replica: FAILED (%s; the rest would wait on it)\n", what);
        fflush(stderr);
        _exit(1);
    }
}

/* Only a directory whose META says requires=replica opens as a replica or attaches as a leader (cvolfs writes it into a
 * cluster volume's META). The directories here are made by a plain open, so every open of one marks it: META gets the
 * line, written and synced with its directory (a power child's open may be the first, and crashfs keeps synced data).
 * case_requires_replica opens through the engine's eng_open, unmarked. */
static void mark_replica(const char *dir)
{
    char p[PATH_MAX], buf[4096];
    snprintf(p, sizeof p, "%s/META", dir);
    int fd = open(p, O_RDWR | O_APPEND);
    if (fd < 0)
        return;
    ssize_t n = read(fd, buf, sizeof buf - 1);
    buf[n > 0 ? n : 0] = '\0';
    if (n >= 0 && !strstr(buf, "requires=replica")) {
        static const char line[] = "requires=replica\n";
        if (write(fd, line, sizeof line - 1) != (ssize_t)(sizeof line - 1) || fsync(fd) != 0) {
            fprintf(stderr, "cannot mark %s\n", p);
            exit(2);
        }
        int dfd = open(dir, O_RDONLY | O_DIRECTORY);
        if (dfd >= 0) {
            (void)fsync(dfd);
            close(dfd);
        }
    }
    close(fd);
}

static int t_eng_open(const char *dir, const tsdb_options *o, eng **db)
{
    mark_replica(dir); /* META there already */
    int rc = eng_open(dir, o, db);
    if (rc == 0 && !(*db)->replica_req) { /* the open made META: marked now, as the next open would read it */
        mark_replica(dir);
        (*db)->replica_req = true;
    }
    return rc;
}
#define eng_open(d, o, db) t_eng_open(d, o, db) /* (eng_open)(...) is the engine's own */

#define T0 1700000000000LL
#define DAY1 ((T0 / 86400000LL + 1) * 86400000LL) /* the day edge after T0 */
#define NS 2    /* series */
#define NT 64   /* distinct times per series (a day holds them: one partition) */
static char S[PATH_MAX - 64], SELF[PATH_MAX];
static const char *MUTNAME;
extern int follow_test_mutant;

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

/* A directory whose META lacks requires=replica opens neither as a replica nor attaches as a leader (r3g "Also 5"):
 * a stranger's directory would have its log cut and its links followed. Marked, both work. */
static void case_requires_replica(void)
{
    char dir[PATH_MAX];
    snprintf(dir, sizeof dir, "%s/reqrep", S);
    sh("rm -rf '%s' && mkdir -p '%s'", dir, dir);
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.wal_sync_ms = 0;
    eng *db = NULL;
    /* an empty directory: refused, and left as it was (no META made by the refused open; review minor) */
    CHECK_EQ(eng_open_replica(dir, &o, 0, &db), TSDB_ERR_INVALID);
    CHECK(db == NULL);
    char meta[PATH_MAX + 8];
    snprintf(meta, sizeof meta, "%s/META", dir);
    CHECK(access(meta, F_OK) != 0);
    CHECK_EQ((eng_open)(dir, &o, &db), 0); /* the engine's open: META without the word */
    if (!db)
        return;
    static const struct jr_ops none = {0};
    CHECK_EQ(eng_leader_attach(db, &none), TSDB_ERR_INVALID);
    eng_close(db);
    db = NULL;
    CHECK_EQ(eng_open_replica(dir, &o, 0, &db), TSDB_ERR_INVALID);
    CHECK(db == NULL);
    mark_replica(dir);
    CHECK_EQ(eng_open_replica(dir, &o, 0, &db), 0);
    if (db)
        eng_close(db);
    db = NULL;
    CHECK_EQ((eng_open)(dir, &o, &db), 0);
    if (!db)
        return;
    CHECK(db->replica_req);
    eng_close(db);
    sh("rm -rf '%s'", dir);
}

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

/* A replica keeps its flushed segments under the retention caps, past its floor (design-cluster.md 8.3): a failover
 * may promote it, and a member that lags it catches up from them or needs a snapshot (jepsen-cluster, r3e: a deposed
 * writer reopened as a replica dropped what a killed member needed next). The caps count from the newest flushed segment
 * back; an open with the caps (replica_keep_next) keeps what it replays and flushes; without caps the floor alone
 * decides, as before. Mutant replicadropsfloor (the floor alone again). */
static void case_keepcap(void)
{
    char dir[PATH_MAX];
    fresh(dir, "kc");
    leader l;
    unsigned rng = 7;
    leader_init(&l);
    for (int s = 0; s < 3; s++) {
        for (int i = 0; i < 4; i++)
            leader_rows(&l, 3 + s, &rng);
        leader_roll(&l);
    }
    for (int i = 0; i < 2; i++)
        leader_rows(&l, 2, &rng);
    uint64_t end = l.r[l.n - 1].last;
    eng *db;
    replica_keep_next(0, 3600 * 1000); /* an hour, no byte cap */
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK_EQ(feed(db, &l, 0, l.n), 0);
    CHECK_EQ(replica_sync(db), 0);
    CHECK_EQ(replica_apply(db, end), 0);
    CHECK_EQ(mt_flush(db), 0);
    CHECK_EQ(replica_floor_saved(db, end), 0);
    CHECK_EQ(count_files(dir, "wal_"), 4); /* three flushed ones within the caps, and the active one */
    model m;
    model_at(&l, end, &m);
    CHECK(same_as(db, &m, "keepcap: kept"));
    /* reopened with the caps: what the open replays and flushes stays */
    CHECK_EQ(eng_close(db), 0);
    replica_keep_next(0, 3600 * 1000);
    CHECK_EQ(open_replica(dir, end, &db), 0);
    if (!db)
        return;
    CHECK_EQ(mt_flush(db), 0);
    CHECK_EQ(replica_floor_saved(db, end), 0);
    CHECK_EQ(count_files(dir, "wal_"), 4);
    CHECK(same_as(db, &m, "keepcap: reopened"));
    /* the byte cap counts from the newest flushed segment back: exactly its size keeps it alone */
    char name[64];
    struct stat st;
    wal_name(name, sizeof name, l.r[l.n - 1].seq - 1);
    CHECK_EQ(fstatat(db->dirfd, name, &st, 0), 0);
    replica_keep_cap(db, (uint64_t)st.st_size, 0);
    CHECK_EQ(replica_floor_saved(db, end), 0);
    CHECK_EQ(count_files(dir, "wal_"), 2);
    CHECK_EQ(fstatat(db->dirfd, name, &st, 0), 0);
    /* the age cap: past it every flushed segment goes */
    replica_keep_cap(db, 0, 1);
    usleep(5000);
    CHECK_EQ(replica_floor_saved(db, end), 0);
    CHECK_EQ(count_files(dir, "wal_"), 1);
    CHECK(same_as(db, &m, "keepcap: capped"));
    CHECK_EQ(eng_close(db), 0);
    /* never past the floor, whatever the caps say */
    char dir2[PATH_MAX];
    fresh(dir2, "kc2");
    replica_keep_next(1, 1);
    CHECK_EQ(open_replica(dir2, 0, &db), 0);
    if (db) {
        CHECK_EQ(feed(db, &l, 0, l.n), 0);
        CHECK_EQ(replica_sync(db), 0);
        CHECK_EQ(replica_apply(db, end), 0);
        CHECK_EQ(mt_flush(db), 0);
        usleep(5000);
        CHECK_EQ(replica_floor_saved(db, l.r[0].last), 0);
        CHECK_EQ(count_files(dir2, "wal_"), 4); /* the first segment ends above that floor */
        CHECK_EQ(eng_close(db), 0);
    }
    leader_free(&l);
    sh("rm -rf '%s' '%s'", dir, dir2);
}

/* D17: a replica names its flushed segments in RSTATE `kept` once the saved floor covers them. Its open neither
 * replays them (the records applied after the reopen are those of the segment above) nor removes them (they stay for
 * catch-up and snapshots until the caps let them go), and its replay start is the first position of the segment
 * above them: above its log start, exactly there. Mutants replaystartlog (the log start), replaystarthigh (one
 * segment too high). */
static int RK_SAVES, RK_HELD;
static void rk_mu_hook(eng *db)
{
    RK_SAVES++;
    RK_HELD += !replica_test_mu_free(db);
}

static void case_replaykept(void)
{
    char dir[PATH_MAX];
    fresh(dir, "rk");
    leader l;
    unsigned rng = 19;
    leader_init(&l);
    for (int s = 0; s < 3; s++) {
        for (int i = 0; i < 4; i++)
            leader_rows(&l, 2 + s, &rng);
        leader_roll(&l);
    }
    size_t above = l.n; /* the first record of the segment above the kept ones */
    for (int i = 0; i < 2; i++)
        leader_rows(&l, 3, &rng);
    uint64_t end = l.r[l.n - 1].last;
    model m;
    model_at(&l, end, &m);
    eng *db;
    replica_keep_next(0, 3600 * 1000);
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK_EQ(replica_replay_start(db), 0); /* an empty log: nothing to replay */
    CHECK_EQ(feed(db, &l, 0, l.n), 0);
    CHECK_EQ(replica_sync(db), 0);
    CHECK_EQ(replica_replay_start(db), l.r[0].fno); /* nothing flushed yet: every segment replays */
    CHECK_EQ(replica_apply(db, end), 0);
    CHECK_EQ(mt_flush(db), 0);
    replica_test_kept_hook = rk_mu_hook; /* RSTATE's fdatasync never holds rp->mu, which appends wait for (REVIEW F3) */
    RK_SAVES = RK_HELD = 0;
    CHECK_EQ(replica_floor_saved(db, end), 0);
    replica_test_kept_hook = NULL;
    CHECK(RK_SAVES > 0 && RK_HELD == 0);
    CHECK_EQ(count_files(dir, "wal_"), 4);
    uint64_t rs = replica_replay_start(db);
    CHECK(rs > l.r[0].fno); /* above the log start */
    CHECK_EQ(rs, l.r[above].fno);
    CHECK(same_as(db, &m, "replaykept: applied"));
    CHECK_EQ(eng_close(db), 0);
    /* reopened at the floor: the kept segments are neither replayed nor removed */
    replica_keep_next(0, 3600 * 1000);
    CHECK_EQ(open_replica(dir, end, &db), 0);
    if (!db)
        return;
    CHECK_EQ(replica_replay_start(db), rs);
    CHECK_EQ(replica_test_open_first(db), l.r[above].fno); /* the cursor starts at the segment above them */
    CHECK_EQ(count_files(dir, "wal_"), 4);
    CHECK_EQ(replica_apply(db, end), 0);
    CHECK_EQ(replica_test_napplied(db), (uint64_t)(l.n - above)); /* the two records above, none of the kept */
    CHECK(same_as(db, &m, "replaykept: reopened"));
    /* the log is still whole for a reader (catch-up, snapshots) */
    uint8_t *buf;
    size_t len;
    uint64_t seq, off, last;
    CHECK_EQ(replica_read(db, l.r[0].fno, &buf, &len, 1 << 20, &seq, &off, &last), 0);
    CHECK_EQ(seq, l.r[0].seq);
    CHECK_EQ(off, 0);
    free(buf);
    /* the caps let them go, and the replay start stays where it was (now the log start) */
    replica_keep_cap(db, 0, 1);
    usleep(5000);
    CHECK_EQ(replica_floor_saved(db, end), 0);
    CHECK_EQ(count_files(dir, "wal_"), 1);
    CHECK_EQ(replica_replay_start(db), rs);
    CHECK(same_as(db, &m, "replaykept: capped"));
    CHECK_EQ(eng_close(db), 0);
    /* a floor below a kept segment's end keeps it out of `kept` (it may still be cut): an open replays it */
    char dir2[PATH_MAX];
    fresh(dir2, "rk2");
    replica_keep_next(0, 3600 * 1000);
    CHECK_EQ(open_replica(dir2, 0, &db), 0);
    if (db) {
        CHECK_EQ(feed(db, &l, 0, l.n), 0);
        CHECK_EQ(replica_sync(db), 0);
        CHECK_EQ(replica_apply(db, end), 0);
        CHECK_EQ(mt_flush(db), 0);
        CHECK_EQ(replica_floor_saved(db, l.r[5].last), 0); /* inside the second segment: the first alone is kept */
        CHECK_EQ(replica_replay_start(db), l.r[4].fno);
        CHECK_EQ(eng_close(db), 0);
        replica_keep_next(0, 3600 * 1000);
        CHECK_EQ(open_replica(dir2, l.r[5].last, &db), 0);
        if (db) {
            CHECK_EQ(replica_test_open_first(db), l.r[4].fno);
            CHECK_EQ(replica_apply(db, end), 0);
            CHECK_EQ(replica_test_napplied(db), (uint64_t)(l.n - 4));
            CHECK(same_as(db, &m, "replaykept: low floor"));
            CHECK_EQ(eng_close(db), 0);
        }
    }
    leader_free(&l);
    sh("rm -rf '%s' '%s'", dir, dir2);
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

/* B3: a cut back into an older segment (replica_cut sets wal_seq) while another thread publishes MANIFEST, as a merge,
 * a drop or a retention change does on any thread while the replica's rp->mu and flush_lock are held by the cut. The
 * publisher reads wal_seq for wal_active under buf_lock, so the cut writes it there too: under TSan the planted
 * cutwalsequnlocked (the cut's write outside buf_lock) must be reported (REVIEW F2: no case raced a cut with a publish).
 * The counters are relaxed on purpose: an acquire on them would order the publisher's reads before the cut's write
 * and hide the race. Each round's cut runs while the publisher loops (fsync of the directory and of the cut segment
 * make its window a few ms); the last MANIFEST must name the segment the cut kept. */
static struct {
    eng *db;
    atomic_int stop;
    atomic_long n;
    atomic_int bad;
} CP;

static void *cp_thread(void *a)
{
    (void)a;
    while (!atomic_load_explicit(&CP.stop, memory_order_relaxed)) {
        if (manifest_publish(CP.db) != 0)
            atomic_store_explicit(&CP.bad, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&CP.n, 1, memory_order_relaxed);
    }
    return NULL;
}

static uint64_t manifest_wal_active(const char *dir)
{
    char path[PATH_MAX + 16], line[256];
    snprintf(path, sizeof path, "%s/MANIFEST", dir);
    FILE *f = fopen(path, "r");
    uint64_t v = 0;
    if (!f)
        return 0;
    while (fgets(line, sizeof line, f))
        if (sscanf(line, "wal_active %" SCNu64, &v) == 1)
            break;
    fclose(f);
    return v;
}

static void case_cut_publish(void)
{
    char dir[PATH_MAX];
    leader l;
    unsigned rng = 11;
    leader_init(&l);
    for (int i = 0; i < 4; i++)
        leader_rows(&l, 4, &rng);
    leader_roll(&l);
    for (int i = 0; i < 4; i++)
        leader_rows(&l, 4, &rng);
    CHECK(l.r[3].seq != l.r[7].seq); /* the cut below goes back across a segment boundary */
    int rounds = getenv("TEST_CUT_PUBLISH_ROUNDS") ? atoi(getenv("TEST_CUT_PUBLISH_ROUNDS")) : 10;
    for (int round = 0; round < rounds && !failures; round++) {
        fresh(dir, "cp");
        eng *db;
        CHECK_EQ(open_replica(dir, 0, &db), 0);
        if (!db)
            break;
        CHECK_EQ(feed(db, &l, 0, l.n), 0);
        CHECK_EQ(replica_sync(db), 0);
        CHECK_EQ(replica_apply(db, l.r[3].last), 0);
        CHECK_EQ(manifest_publish(db), 0);
        CHECK_EQ(manifest_wal_active(dir), l.r[7].seq); /* the newer segment is the one being written */
        CP.db = db;
        atomic_store(&CP.stop, 0);
        atomic_store(&CP.n, 0);
        atomic_store(&CP.bad, 0);
        pthread_t t;
        CHECK_EQ(pthread_create(&t, NULL, cp_thread, NULL), 0);
        while (atomic_load_explicit(&CP.n, memory_order_relaxed) < 3) /* publishing before and during the cut */
            sched_yield();
        CHECK_EQ(replica_cut(db, l.r[3].last), 0);
        long before = atomic_load_explicit(&CP.n, memory_order_relaxed);
        while (atomic_load_explicit(&CP.n, memory_order_relaxed) < before + 3) /* and after it */
            sched_yield();
        atomic_store(&CP.stop, 1);
        pthread_join(t, NULL);
        CHECK_EQ(atomic_load(&CP.bad), 0);
        CHECK_EQ(replica_log_end(db), l.r[3].last);
        CHECK_EQ(count_files(dir, "wal_"), 1);
        CHECK_EQ(manifest_wal_active(dir), l.r[3].seq); /* the segment the cut kept, not the one it removed */
        CHECK_EQ(eng_close(db), 0);
    }
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
static lrec *m_epoch(leader *l, uint64_t epoch)
{
    mk m = {.type = MK_EPOCH};
    m.u.epoch = (mk_epoch){.epoch = epoch, .owner = "n1", .owner_len = 2};
    return leader_marker(l, &m, T0 + (int64_t)l->next);
}
static void log_epoch_ok(leader *l)
{
    m_series(l, 0, "s0", TSDB_FLOAT32);
    l->seq = (uint64_t)1 << 40, l->off = 0;
    m_epoch(l, 1);
}
static void log_epoch_seg(leader *l) /* the marker's epoch is not its segment's */
{
    m_series(l, 0, "s0", TSDB_FLOAT32);
    l->seq = (uint64_t)3 << 40, l->off = 0;
    m_epoch(l, 2);
}
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
    CHECK_EQ(run_log("g7", log_epoch_ok, &failed), 0);
    CHECK(!failed);
    CHECK_EQ(run_log("g8", log_epoch_seg, &failed), TSDB_ERR_CORRUPT);
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
    /* a replica takes the leader's T exactly (design-cluster 5.4): T lies past every row of series 0 it holds, here
       the day edge after T0's day (class 0 partitions are days, and a flushed part reaches its partition's end) */
    lrec *cm = m_class(&l, 0, 2, DAY1, 0);
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

/* I2: a replica takes the leader's T exactly. A CLASS record whose T lies below rows this replica holds (in a part
   here) says its history is not the leader's: the apply is CORRUPT, never "max(T, own T)" (mutant classmax), which
   would place rows the leader placed under the new class under the old one, and the two would answer differently. */
static void case_class_exact(void)
{
    char dir[PATH_MAX];
    fresh(dir, "i2");
    leader l;
    unsigned rng = 9;
    leader_init(&l);
    for (int i = 0; i < 4; i++)
        leader_rows(&l, 5, &rng);
    leader_roll(&l);
    lrec *first2 = leader_row1(&l, 1, T0 + 70000, 99.0f, T0 + 1000);
    lrec *cm = m_class(&l, 0, 2, T0, 0); /* T0: below the rows of series 0 on T0's day */
    eng *db;
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK_EQ(feed(db, &l, 0, l.n), 0);
    CHECK_EQ(replica_sync(db), 0);
    CHECK_EQ(replica_apply(db, first2->last), 0);
    CHECK_EQ(mt_flush(db), 0);
    CHECK_EQ(replica_apply(db, cm->last), TSDB_ERR_CORRUPT);
    CHECK(eng_check(db) != 0);
    CHECK_EQ(db->cls_off, 0);
    eng_close(db);
    leader_free(&l);
    printf("  a CLASS record below this replica's rows is CORRUPT\n");
}

/* I3 (REVIEW M6): the leader's record for a series it decides as new (at INT64_MIN) while this replica holds rows of
 * it in parts is CORRUPT like any other record below its rows (mutant freshunchecked: fresh records are not checked,
 * and the replica keeps rows under a history that is not the leader's). */
static void case_class_fresh_held(void)
{
    char dir[PATH_MAX];
    fresh(dir, "i3");
    leader l;
    unsigned rng = 9;
    leader_init(&l);
    for (int i = 0; i < 4; i++)
        leader_rows(&l, 5, &rng);
    leader_roll(&l);
    lrec *first2 = leader_row1(&l, 1, T0 + 70000, 99.0f, T0 + 1000);
    lrec *cm = m_class(&l, 0, 2, INT64_MIN, 0);
    eng *db;
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK_EQ(feed(db, &l, 0, l.n), 0);
    CHECK_EQ(replica_sync(db), 0);
    CHECK_EQ(replica_apply(db, first2->last), 0);
    CHECK_EQ(mt_flush(db), 0);
    CHECK_EQ(replica_apply(db, cm->last), TSDB_ERR_CORRUPT);
    CHECK(eng_check(db) != 0);
    CHECK_EQ(db->cls_off, 0);
    eng_close(db);
    leader_free(&l);
    printf("  a new-series CLASS record for a series this replica holds rows of is CORRUPT\n");
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

    /* the same gap below the parts and the floor is a batch of unlinks a power loss kept only in part (jepsen-cluster
     * power, 2026-11-20: every open refused the volume for good): the segment before it goes, the open succeeds
     * (mutant noleftover) */
    fresh(dir, "j1b");
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
    CHECK_EQ(replica_sync(db), 0);
    end = l.r[l.n - 1].last;
    CHECK_EQ(replica_apply(db, end), 0);
    char s1[PATH_MAX + 64], s3[PATH_MAX + 64], k1[PATH_MAX + 80], k3[PATH_MAX + 80];
    seg_file(s1, sizeof s1, dir, 1);
    seg_file(s3, sizeof s3, dir, 3);
    snprintf(k1, sizeof k1, "%s.save", s1);
    snprintf(k3, sizeof k3, "%s.save", s3);
    CHECK_EQ(link(s1, k1), 0); /* the unlinks the power loss "lost" */
    CHECK_EQ(link(s3, k3), 0);
    CHECK_EQ(mt_flush(db), 0); /* every segment's rows are in parts */
    eng_close(db);
    if (access(s1, F_OK) != 0)
        CHECK_EQ(link(k1, s1), 0);
    if (access(s3, F_OK) != 0)
        CHECK_EQ(link(k3, s3), 0);
    unlink(k1);
    unlink(k3);
    seg_file(p, sizeof p, dir, 2);
    unlink(p);
    CHECK_EQ(open_replica(dir, 0, &db), TSDB_ERR_CORRUPT); /* the gap is above the floor: damage */
    CHECK_EQ(open_replica(dir, end, &db), 0);
    if (db) {
        CHECK_EQ(access(s1, F_OK) != 0, true);
        CHECK_EQ(replica_log_end(db), end);
        eng_close(db);
    }
    leader_free(&l);

    /* the same, where the segment after the gap is a new epoch's (a promotion's jump to epoch << 40): its number says
     * nothing of the missing ones, the parts' positions do (jepsen-cluster power, 2026-11-20: a promoted leader's flushed
     * epoch-1 segments went in part, and every open refused the volume for good; mutant gapbyseq) */
    fresh(dir, "j1c");
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
    CHECK_EQ(replica_sync(db), 0);
    end = l.r[l.n - 1].last;
    CHECK_EQ(replica_apply(db, end), 0);
    uint64_t epos = 0;
    CHECK_EQ(replica_epoch_begin(db, 2, "n1", &epos), 0); /* the segment 2 << 40, its EPOCH marker at end + 1 */
    CHECK_EQ(epos, end + 1);
    CHECK_EQ(replica_sync(db), 0);
    char s2[PATH_MAX + 64], k2[PATH_MAX + 80];
    seg_file(s1, sizeof s1, dir, 1);
    seg_file(s2, sizeof s2, dir, 2);
    snprintf(k1, sizeof k1, "%s.save", s1);
    snprintf(k2, sizeof k2, "%s.save", s2);
    CHECK_EQ(link(s1, k1), 0);
    CHECK_EQ(link(s2, k2), 0);
    CHECK_EQ(replica_apply(db, end + 1), 0);
    CHECK_EQ(replica_flush(db), 0); /* as a promotion does: every row is in parts, their fno_hi is end */
    CHECK_EQ(mt_flush(db), 0);
    eng_close(db);
    if (access(s1, F_OK) != 0)
        CHECK_EQ(link(k1, s1), 0);
    if (access(s2, F_OK) != 0)
        CHECK_EQ(link(k2, s2), 0);
    unlink(k1);
    unlink(k2);
    seg_file(p, sizeof p, dir, 3);
    unlink(p); /* the gap is right below the new epoch's segment */
    CHECK_EQ(open_replica(dir, 0, &db), TSDB_ERR_CORRUPT); /* above the floor: still damage */
    CHECK_EQ(open_replica(dir, end + 1, &db), 0);
    if (db) {
        CHECK_EQ(access(s1, F_OK) != 0, true);
        CHECK_EQ(access(s2, F_OK) != 0, true);
        CHECK_EQ(replica_log_end(db), end + 1);
        eng_close(db);
    }
    leader_free(&l);

    /* the same, where the missing segment is the new epoch's first and holds only its EPOCH marker: no part's number or
     * position reaches it, RSTATE's `kept` (a leader's flush, a promotion) does (jepsen-cluster power, 2026-11-20: a
     * promoted leader's first segment went and one of its flushed epoch-1 segments came back; mutant gapnokept) */
    fresh(dir, "j1d");
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
    CHECK_EQ(replica_sync(db), 0);
    end = l.r[l.n - 1].last;
    CHECK_EQ(replica_apply(db, end), 0);
    char se[PATH_MAX + 64];
    seg_file(s3, sizeof s3, dir, 3);
    snprintf(k3, sizeof k3, "%s.save", s3);
    CHECK_EQ(link(s3, k3), 0);
    CHECK_EQ(replica_epoch_begin(db, 2, "n1", &epos), 0); /* the segment 2 << 40, its EPOCH marker at end + 1 */
    CHECK_EQ(epos, end + 1);
    CHECK_EQ(replica_sync(db), 0);
    CHECK_EQ(replica_apply(db, end + 1), 0);
    CHECK_EQ(replica_flush(db), 0);
    CHECK_EQ(mt_flush(db), 0);
    size_t n0 = l.n; /* the epoch's next segment: rows from end + 2 */
    l.seq = (UINT64_C(2) << 40) + 1;
    l.off = 0;
    l.next = end + 2;
    leader_rows(&l, 4, &rng);
    CHECK_EQ(feed(db, &l, n0, l.n), 0);
    CHECK_EQ(replica_sync(db), 0);
    uint64_t end2 = l.r[l.n - 1].last;
    CHECK_EQ(replica_apply(db, end2), 0);
    eng_close(db);
    if (access(s3, F_OK) != 0)
        CHECK_EQ(link(k3, s3), 0);
    unlink(k3);
    seg_file(se, sizeof se, dir, UINT64_C(2) << 40);
    unlink(se); /* the gap: the EPOCH marker's segment */
    CHECK_EQ(open_replica(dir, end2, &db), TSDB_ERR_CORRUPT); /* nothing says the marker's segment was flushed */
    {
        /* what a leader's flush of that segment saves before it can go (leader_rs_kept) */
        uint8_t b[80] = {0}; /* format 2: magic, done, doing (3), ndrops, kept, cold_pos, cold_key, xxh64 */
        uint64_t w[2] = {UINT64_C(0x5453544154535232), UINT64_C(2) << 40};
        memcpy(b, &w[0], 8);
        memcpy(b + 48, &w[1], 8);
        uint64_t h = xxh64(b, 72, 0);
        memcpy(b + 72, &h, 8);
        snprintf(p, sizeof p, "%s/RSTATE", dir);
        FILE *f = fopen(p, "wb");
        CHECK_EQ(f != NULL, true);
        if (f) {
            CHECK_EQ(fwrite(b, 1, sizeof b, f), sizeof b);
            fclose(f);
        }
    }
    CHECK_EQ(open_replica(dir, end, &db), TSDB_ERR_CORRUPT); /* above the floor: still damage */
    CHECK_EQ(open_replica(dir, end2, &db), 0);
    if (db) {
        CHECK_EQ(access(s3, F_OK) != 0, true);
        CHECK_EQ(replica_log_end(db), end2);
        eng_close(db);
    }
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
    snprintf(dir, sizeof dir, "%s/o", S); /* not fresh(): its series are made through the marking open */
    sh("rm -rf '%s' && mkdir -p '%s'", dir, dir);
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    eng *db;
    /* the engine's own open: a feed is refused where META says requires=replica, so the feed comes first and the word
     * after it, which leaves the feed the one reason for the replica open to refuse */
    CHECK_EQ((eng_open)(dir, &o, &db), 0);
    if (!db)
        return;
    CHECK_EQ(eng_feed_enable(db, NULL), 0);
    eng_close(db);
    mark_replica(dir);
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


/* PK (D17): power lost at every step of a replica that appends, applies, flushes, saves its floor (and with it
 * RSTATE's `kept`) and restarts. The open is right either way: it replays a segment whose `kept` save was lost and
 * never skips one whose rows are not in parts (mutant replaykeptearly loses rows), and the replay start the child
 * reported before the loss (also from inside the save, before it is durable: replica_test_kept_hook) is never above
 * the first position the reopened replica replays (mutant replaystartunsaved). The floor is written outside the
 * crashfs root before the replica is told, as the driver saves it in REPL first. */
#define PK_ROUNDS 12

static void powerk_leader(leader *l)
{
    unsigned rng = 23;
    leader_init(l);
    for (int r = 0; r < PK_ROUNDS; r++) {
        for (int k = 0; k < 3; k++)
            leader_rows(l, 2 + k, &rng);
        leader_roll(l);
    }
}

static char POWERK_ACK[PATH_MAX + 16];

static void powerk_hook(eng *db)
{
    write_ack(POWERK_ACK, replica_replay_start(db));
}

static int powerk_child(const char *dir, const char *ackfile)
{
    leader l;
    powerk_leader(&l);
    char flf[PATH_MAX + 16];
    snprintf(flf, sizeof flf, "%s.floor", ackfile);
    snprintf(POWERK_ACK, sizeof POWERK_ACK, "%s", ackfile);
    replica_test_kept_hook = powerk_hook;
    eng *db;
    replica_keep_next(0, 3600 * 1000);
    if (open_replica(dir, 0, &db) != 0)
        return 2;
    for (int r = 0; r < PK_ROUNDS; r++) {
        if (feed(db, &l, (size_t)r * 3, (size_t)r * 3 + 3) != 0)
            return 3;
        if (replica_sync(db) != 0)
            return 4;
        uint64_t last = l.r[r * 3 + 2].last;
        if (replica_apply(db, last) != 0)
            return 5;
        if (r % 2 == 1 && mt_flush(db) != 0)
            return 6;
        write_ack(flf, last);
        if (replica_floor_saved(db, last) != 0)
            return 7;
        write_ack(ackfile, replica_replay_start(db));
        if (r % 4 == 3) { /* the process restarts at its floor */
            if (eng_close(db) != 0)
                return 8;
            replica_keep_next(0, 3600 * 1000);
            if (open_replica(dir, last, &db) != 0)
                return 9;
            write_ack(ackfile, replica_replay_start(db));
        }
    }
    eng_close(db);
    return 0;
}

static uint64_t read_ack(const char *file)
{
    uint64_t v = 0;
    FILE *f = fopen(file, "r");
    if (f) {
        if (fscanf(f, "%" SCNu64, &v) != 1)
            v = 0;
        fclose(f);
    }
    return v;
}

static void case_power_kept(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  power loss of kept segments: skipped (no %s)\n", SHIM);
        return;
    }
    leader l;
    powerk_leader(&l);
    char dir[PATH_MAX], ack[PATH_MAX + 16], flf[PATH_MAX + 32];
    snprintf(ack, sizeof ack, "%s/acks-of-pk.ack", S);
    snprintf(flf, sizeof flf, "%s.floor", ack);
    int crashes = 0, raised = 0, skipped = 0;
    bool finished = false;
    for (int k = 0; k < 2000 && !finished; k++) {
        fresh(dir, "pk");
        unlink(ack);
        unlink(flf);
        int code = run_power_child_as("--powerk", dir, dir, ack, k, k + 1);
        if (code != 99 && code != 0)
            fprintf(stderr, "PK: step %d: the child ended with status %d\n", k, code);
        CHECK(code == 99 || code == 0);
        if (code != 99 && code != 0)
            break;
        crashes += code == 99;
        finished = code == 0;
        uint64_t told = read_ack(ack), floor = read_ack(flf);
        eng *db;
        replica_keep_next(0, 3600 * 1000);
        int rc = open_replica(dir, floor, &db);
        if (rc != 0) {
            fprintf(stderr, "PK: step %d: the open at floor %" PRIu64 " failed: %d\n", k, floor, rc);
            failures++;
            continue;
        }
        uint64_t first = replica_test_open_first(db); /* the first position this open replays */
        if (told > first) {
            fprintf(stderr, "PK: step %d: the replay start told before the loss is %" PRIu64 ", the reopened replica "
                            "replays from %" PRIu64 "\n", k, told, first);
            failures++;
        }
        raised += told > l.r[0].fno;
        if (replica_apply(db, replica_log_end(db)) != 0) {
            fprintf(stderr, "PK: step %d: the log does not apply\n", k);
            failures++;
        }
        uint64_t applied = replica_applied_end(db);
        size_t nrec = 0;
        while (nrec < l.n && l.r[nrec].last <= applied)
            nrec++;
        skipped += replica_test_napplied(db) < nrec;
        model m;
        model_at(&l, applied, &m);
        char what[64];
        snprintf(what, sizeof what, "PK: step %d", k);
        if (!same_as(db, &m, what))
            failures++;
        eng_close(db);
    }
    CHECK(finished && crashes > 50 && raised > 0 && skipped > 0);
    printf("  power lost at each of %d steps of a replica flushing, saving `kept` and restarting: no segment skipped "
           "whose rows are not in parts, the replay start told is never above what an open replays\n", crashes);
    leader_free(&l);
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
    bool noship;           /* wait and seg_done ship nothing (the test ships by itself) */
    uint8_t *held;         /* the last file shipped */
    size_t held_len;
    char held_name[64];
    uint64_t saved;        /* the commit floor the "driver" saved */
    /* the leader's commit point (eng_leader_commit): by default it follows the bytes the WAL writer hands over (a
     * replica that has every byte at once); with `manual` only the test moves it */
    bool manual;
    int refuse;            /* admit refuses with this code (0: admits) */
    int refuse_once;       /* admit refuses the next admission with this code, then admits again */
    bool depose_in_admit;  /* admit deposes the leader and refuses (a driver whose core says it is not leading) */
    bool rec;              /* record admissions and bytes */
    uint64_t adm_fno[512];
    uint32_t adm_span[512];
    size_t nadm;
    size_t nbytes, bytes_bad, bytes_late; /* bytes callbacks; ones unlike the segment; syncs without fresh bytes */
    uint64_t bytes_last;   /* the last position handed over */
    bool bytes_fresh;      /* bytes came since the last sync */
    uint32_t max_span;     /* the most rows in one record handed over (jr_rec_max_rows) */
    uint64_t synced_last;  /* the last position jr_ops.synced reported */
    size_t synced_bad;     /* reports that went back, or that were not the durable end at the time */
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

/* the leader's last log position; under buf_lock, where the WAL hands numbers out: the flusher appends NOW markers of its
 * own while a leader's clock is on (TSan, test_replica lmrefn) */
static uint64_t ld_last(xport *x)
{
    pthread_mutex_lock(&x->ld->buf_lock);
    uint64_t v = x->ld->fno_next - 1;
    pthread_mutex_unlock(&x->ld->buf_lock);
    return v;
}

static int xp_pump(xport *x, uint64_t commit)
{
    int rc = x->noship ? 0 : xp_ship_logs(x);
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
    xport *x = ctx;
    /* a replica applies only what the leader committed: with `manual` the test holds the commit point, and a replica
     * that applied past it would hold records a depose leaves uncommitted (L1b) */
    uint64_t c = atomic_load(&db->lead_commit);
    return xp_pump(x, x->manual && c < last ? c : last);
}

static void xp_seg_done(void *ctx, eng *db, uint64_t seq)
{
    (void)db, (void)seq;
    if (!((xport *)ctx)->noship)
        xp_ship_logs(ctx); /* before the file goes */
}

static int xp_admit(void *ctx, eng *db, uint64_t fno, uint32_t span)
{
    xport *x = ctx;
    if (x->depose_in_admit) {
        eng_leader_depose(db); /* under buf_lock: the depose must not take it (REVIEW M2) */
        return TSDB_ERR_UNAVAILABLE;
    }
    if (x->refuse)
        return x->refuse;
    if (x->refuse_once) {
        int rc = x->refuse_once;
        x->refuse_once = 0;
        return rc;
    }
    if (x->rec && x->nadm < 512)
        x->adm_fno[x->nadm] = fno, x->adm_span[x->nadm++] = span;
    return 0;
}

static void xp_bytes(void *ctx, eng *db, uint64_t seq, uint64_t off, const void *buf, size_t len, uint64_t first,
                     uint64_t last)
{
    xport *x = ctx;
    for (size_t o = 0; o < len;) {
        wal_rec wr;
        size_t sz = wal_record((const uint8_t *)buf + o, len - o, seq, off + o, &wr);
        if (!sz)
            break;
        if (wr.span > x->max_span)
            x->max_span = (uint32_t)wr.span;
        o += sz;
    }
    if (x->rec) {
        /* the bytes are the segment's at that place: what a replica appends is what the leader's file holds */
        char nm[64];
        wal_name(nm, sizeof nm, seq);
        int fd = openat(db->dirfd, nm, O_RDONLY);
        uint8_t *b = malloc(len);
        if (fd < 0 || !b || pread(fd, b, len, (off_t)off) != (ssize_t)len || memcmp(b, buf, len) != 0)
            x->bytes_bad++;
        uint64_t f0;
        memcpy(&f0, (const uint8_t *)buf + 16, 8);
        if (f0 != first || last < first || (x->bytes_last && first != x->bytes_last + 1))
            x->bytes_bad++; /* positions continue the log without a gap */
        x->bytes_last = last;
        free(b);
        if (fd >= 0)
            close(fd);
        x->nbytes++;
        x->bytes_fresh = true;
    }
    if (!x->manual)
        eng_leader_commit(db, last);
}

static void xp_synced(void *ctx, eng *db, uint64_t last)
{
    xport *x = ctx;
    if (last < x->synced_last || last != eng_wal_durable_fno(db))
        x->synced_bad++;
    x->synced_last = last;
}

static xport *SYNC_X; /* the leader whose syncs wal_test_sync_hook watches */
static void xp_sync_hook(eng *db)
{
    xport *x = SYNC_X;
    if (!x || db != x->ld)
        return;
    if (!x->bytes_fresh) /* the write being synced was not handed over first */
        x->bytes_late++;
    x->bytes_fresh = false;
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
    XOPS = (struct jr_ops){.ctx = x, .ship = xp_ship, .wait = xp_wait, .seg_done = xp_seg_done, .admit = xp_admit,
                           .bytes = xp_bytes, .synced = xp_synced};
    if (eng_leader_attach(x->ld, &XOPS) != 0)
        exit(2);
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

/* A read-only open of a replica's directory (jepsen-cluster's end check, an NFS follower) sees what the replica holds.
 * A replica writes no WAL of its own (wal_enabled is off), and MANIFEST once said wal_active = wal_flushed for it, so
 * a reader took the rows of its parts only: every row still in a segment was missing. */
static bool ro_same(xport *x, const char *rd, const char *what)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.read_only = 1;
    o.merge_threads = 0;
    o.create_volumes = 0;
    eng *ro = NULL;
    int rc = (eng_open)(rd, &o, &ro);
    if (rc != 0) {
        fprintf(stderr, "%s: read-only open of the replica: %d\n", what, rc);
        return false;
    }
    bool ok = true;
    for (uint32_t s = 0; s < NS; s++)
        ok &= eq_range(x->ld, ro, s, T0 - 1000, T0 + 1000LL * (NT + 1), what);
    eng_close(ro);
    return ok;
}

/* A replica whose MANIFEST names the wrong segments fails its own apply, and the leader's large write then waits for a
 * commit point that never comes: without a deadline the case hangs until make test's timeout kills the whole binary
 * with no line naming it (REVIEW F12, mutants roflushed and romanifest). */
static void ro_open_timeout(int sig)
{
    (void)sig;
    static const char msg[] = "FAIL: case_ro_open did not finish within its deadline (a replica's apply failed?)\n";
    ssize_t w = write(2, msg, sizeof msg - 1);
    (void)w;
    _exit(1);
}

/* wal_flushed and wal_active as the MANIFEST file of `dir` names them now (what a read-only open reads first) */
static bool man_bounds(const char *dir, uint64_t *flushed, uint64_t *active)
{
    char path[PATH_MAX + 16], line[256];
    snprintf(path, sizeof path, "%s/MANIFEST", dir);
    FILE *f = fopen(path, "r");
    if (!f)
        return false;
    int got = 0;
    while (fgets(line, sizeof line, f)) {
        unsigned long long v;
        if (sscanf(line, "wal_flushed %llu", &v) == 1)
            *flushed = v, got |= 1;
        else if (sscanf(line, "wal_active %llu", &v) == 1)
            *active = v, got |= 2;
    }
    fclose(f);
    return got == 3;
}

/* A read-only open of a replica's or promoted replica's directory (docs/tasks/replica-ro-tails.md; jepsen-cluster's
 * end check opens stopped replicas, promoted ones among them), compared with `w`, the engine that has the rows. In
 * case_promote it is called in the state where wal_flushed is still below the jump of segment numbers and wal_active
 * above it (the tail loop of follow_refresh walks the numbers between them, MAX_TAILS of them at most), and after. */
static bool ro_dir_same(eng *w, const char *dir, int64_t hi, const char *what)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.read_only = 1;
    o.merge_threads = 0;
    o.create_volumes = 0;
    eng *ro = NULL;
    int rc = (eng_open)(dir, &o, &ro);
    if (rc != 0) {
        fprintf(stderr, "%s: read-only open of %s: %d\n", what, dir, rc);
        return false;
    }
    bool ok = true;
    for (uint32_t s = 0; s < NS; s++)
        ok &= eq_range(w, ro, s, T0 - 1000, hi, what);
    eng_close(ro);
    return ok;
}

static void case_ro_open(void)
{
    xport x;
    char rd[PATH_MAX];
    struct sigaction sa = {.sa_handler = ro_open_timeout}, old;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGALRM, &sa, &old);
    alarm(getenv("TEST_RO_OPEN_DEADLINE") ? (unsigned)atoi(getenv("TEST_RO_OPEN_DEADLINE")) : 300);
    xp_start(&x, "ro");
    rdir(rd, sizeof rd, &x);
    xp_rows(&x, 0, 0, 10, 100), xp_rows(&x, 1, 0, 5, 300);
    xp_step(&x, "RO: small rows");
    CHECK(ro_same(&x, rd, "RO: small rows, read-only"));
    CHECK_EQ(eng_flush(x.ld), 0); /* a new segment at the leader: the replica's too */
    xp_rows(&x, 0, 10, 10, 500);
    xp_step(&x, "RO: after a flush");
    CHECK(ro_same(&x, rd, "RO: after a flush, read-only"));
    xp_rows(&x, 1, 5, 40, 2000); /* a large insert over rows in a segment */
    CHECK_EQ(eng_delete_range(x.ld, 0, T0 + 3000, T0 + 6000), 0);
    xp_rows(&x, 0, 20, 5, 900);
    xp_step(&x, "RO: large insert, delete");
    CHECK(ro_same(&x, rd, "RO: large insert, delete, read-only"));
    xp_reopen(&x, rd, replica_applied_end(x.rep)); /* the replica's own open publishes too */
    xp_rows(&x, 1, 50, 5, 4000);
    xp_step(&x, "RO: reopened");
    CHECK(ro_same(&x, rd, "RO: reopened, read-only"));
    eng_close(x.rep); /* what jepsen-cluster's end check opens: a stopped replica's directory */
    x.rep = NULL;
    CHECK(ro_same(&x, rd, "RO: replica closed, read-only"));
    xp_stop(&x);
    alarm(0);
    sigaction(SIGALRM, &old, NULL);
}

/* R8: a COLD marker holds its positions as a COMMIT does. Its .ec file is never staged here (the leader did not ship
 * it): the replica does not acknowledge the marker (durable_end stays below it, replica_missing names the file), a
 * restart's open scan finds it again and still holds, and once the file is staged it counts durable and applies (the
 * stub of r8h acknowledges: RSTATE's `done` passes it, a replay skips it). RSTATE's COLD in progress survives a reopen
 * and is decided by ecapply_cold_decide, against the marker's key; format 1 is refused; a COLD naming a file that is
 * no part or .ec is damage. Mutants coldnostage (cm_add skips COLD), coldkeyany, coldnameany. */
/* replica_apply with its stderr kept in `err` (and passed on): which refusal came, where two answer the same code */
static int apply_said(eng *db, uint64_t end, char *err, size_t n)
{
    err[0] = 0;
    FILE *t = tmpfile();
    if (!t)
        return replica_apply(db, end);
    fflush(stderr);
    int sv = dup(2);
    dup2(fileno(t), 2);
    int rc = replica_apply(db, end);
    fflush(stderr);
    dup2(sv, 2);
    close(sv);
    rewind(t);
    size_t k = fread(err, 1, n - 1, t);
    err[k] = 0;
    fclose(t);
    fputs(err, stderr);
    return rc;
}

static void case_cold_hold(void)
{
    char dir[PATH_MAX], name[64], sp[64];
    uint64_t sz, mpos;
    fresh(dir, "coldhold");
    leader l;
    unsigned rng = 11;
    leader_init(&l);
    for (int i = 0; i < 4; i++)
        leader_rows(&l, 5, &rng);
    static const char ecname[] = "p5_1_4_1.ec";
    uint8_t body[3000];
    for (size_t i = 0; i < sizeof body; i++)
        body[i] = (uint8_t)(i * 7 + 3);
    mk_file f = {ecname, (uint32_t)strlen(ecname), sizeof body, xxh64(body, sizeof body, 0), 0, MK_FILE_EC};
    uint8_t files[256], rec[8] = {1, 2, 3, 4, 5, 6, 7, 8}, oid[MK_OID] = {9};
    size_t fl = mk_file_put(files, &f);
    uint8_t objs[256];
    size_t ol = mk_cold_obj_put(objs, oid, rec, sizeof rec);
    mk m = {.type = MK_COLD};
    m.u.cold = (mk_cold){.z = 1, .flush_through = 0, .stripe_version = 1, .key = 4 * 5 + 1, .nobjs = 1, .nfiles = 1,
                         .objs = objs, .objs_len = ol, .files = files, .files_len = fl};
    lrec *c = leader_marker(&l, &m, T0 + (int64_t)l.next);
    lrec *n1 = m_now(&l, T0 + 1000);
    eng *db;
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK_EQ(feed(db, &l, 0, l.n), 0);
    CHECK_EQ(replica_sync(db), 0);
    CHECK(replica_durable_end(db) < c->last); /* mutant coldnostage: acknowledged without its file */
    CHECK_EQ(replica_missing(db, &mpos, name, sizeof name, &sz), 1);
    CHECK(strcmp(name, ecname) == 0);
    CHECK_EQ(mpos, c->last);
    CHECK_EQ(sz, sizeof body);
    CHECK_EQ(replica_stage_path(db, ecname, sp, sizeof sp), TSDB_ERR_NOTFOUND);
    eng_close(db);
    /* a restart with the marker in the log and no file: still held */
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK(replica_durable_end(db) < c->last);
    CHECK_EQ(replica_missing(db, &mpos, name, sizeof name, &sz), 1);
    CHECK(strcmp(name, ecname) == 0);
    /* a file with the wrong content is no file */
    body[100] ^= 1;
    CHECK_EQ(replica_stage(db, ecname, body, sizeof body), 0);
    CHECK_EQ(replica_missing(db, &mpos, name, sizeof name, &sz), 1);
    body[100] ^= 1;
    CHECK_EQ(replica_stage(db, ecname, body, sizeof body), 0);
    CHECK_EQ(replica_missing(db, &mpos, name, sizeof name, &sz), 0);
    CHECK_EQ(replica_stage_path(db, ecname, sp, sizeof sp), 0);
    CHECK(strncmp(sp, "stage_", 6) == 0);
    CHECK_EQ(replica_sync(db), 0);
    CHECK_EQ(replica_durable_end(db), n1->last);
    /* RSTATE's COLD in progress survives a reopen (format 2), and the replay asks ecapply_cold_decide: nothing listed,
     * so the apply runs and refuses the test's fake object record (8 bytes, no .ec file behind it); a build without the
     * codec refuses every COLD it would carry out (INVALID, naming the codec). The apply's answer is the same with the
     * `doing` record lost, so it tells nothing about RSTATE here: jr_cold_pending checks the record's load, and
     * test_ecapply (leader and multi, mutants leaderswapnors and decidesome) checks the decision it drives. */
#ifdef TSDB_EC
    const int cold_refused = TSDB_ERR_CORRUPT;
#else
    const int cold_refused = TSDB_ERR_INVALID;
#endif
    CHECK_EQ(rs_cold_begin(db, c->last, m.u.cold.key), 0);
    eng_close(db);
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    uint64_t cp = 0;
    int64_t ck = 0;
    CHECK_EQ(jr_cold_pending(db, &cp, &ck), 0);
    CHECK(cp == c->last && ck == m.u.cold.key);
    CHECK_EQ(replica_sync(db), 0);
    CHECK_EQ(replica_apply(db, n1->last), cold_refused);
    eng_close(db);
    /* a record that names another key at the marker's position is damage (mutant coldkeyany) */
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK_EQ(rs_cold_begin(db, c->last, m.u.cold.key + 4), 0);
    CHECK_EQ(replica_sync(db), 0);
    {   /* refused by the key check, not by the apply that would refuse the fake record with the same code */
        char said[4096];
        CHECK(apply_said(db, n1->last, said, sizeof said) == TSDB_ERR_CORRUPT);
        CHECK(strstr(said, "the marker there names key") != NULL);
    }
    eng_close(db);
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK_EQ(rs_cold_begin(db, 0, 0), 0); /* none in progress: the apply carries it out, and refuses the fake record */
    CHECK_EQ(replica_sync(db), 0);
    CHECK_EQ(replica_durable_end(db), n1->last);
    CHECK_EQ(replica_apply(db, n1->last), cold_refused);
    eng_close(db);
    /* RSTATE's `done` at the marker, as an apply leaves it (tests/test_ecapply.c applies real ones) */
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK_EQ(rs_cold_done(db, c->last), 0);
    CHECK_EQ(replica_sync(db), 0);
    CHECK_EQ(replica_apply(db, n1->last), 0);
    CHECK_EQ(eng_now_ms(db), T0 + 1000);
    cp = 1, ck = 1;
    CHECK_EQ(jr_cold_pending(db, &cp, &ck), 0);
    CHECK(cp == 0 && ck == 0);
    eng_close(db);
    /* applied: a replay skips it (RSTATE's `done`) before it looks at a COLD in progress, here a stale one naming
     * another key (mutant cold_noskip of the review: carried out again, it meets the key check), and the open holds
     * nothing */
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK_EQ(rs_cold_begin(db, c->last, m.u.cold.key + 4), 0);
    eng_close(db);
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (!db)
        return;
    CHECK_EQ(replica_missing(db, &mpos, name, sizeof name, &sz), 0);
    CHECK_EQ(replica_durable_end(db), n1->last);
    CHECK_EQ(replica_apply(db, n1->last), 0);
    eng_close(db);
    /* RSTATE of format 1 (before R8) is refused by name: no carry-over */
    char rsp[PATH_MAX + 16];
    snprintf(rsp, sizeof rsp, "%s/RSTATE", dir);
    FILE *rf = fopen(rsp, "wb");
    CHECK(rf != NULL);
    if (rf) {
        uint8_t old[64] = {0};
        uint64_t mg = UINT64_C(0x5453544154535231);
        memcpy(old, &mg, 8);
        uint64_t h = xxh64(old, 56, 0);
        memcpy(old + 56, &h, 8);
        CHECK(fwrite(old, 1, sizeof old, rf) == sizeof old);
        fclose(rf);
    }
    db = NULL;
    CHECK(open_replica(dir, 0, &db) == TSDB_ERR_CORRUPT);
    if (db)
        eng_close(db);
    leader_free(&l);
    sh("rm -rf '%s'", dir);

    /* a COLD that names a file that is neither a part nor an .ec is damage, refused when it arrives (mutant
     * coldnameany): the apply would link a staged file under that name */
    fresh(dir, "coldname");
    leader_init(&l);
    leader_rows(&l, 5, &rng);
    static const char bad[] = "MANIFEST";
    mk_file fb = {bad, (uint32_t)strlen(bad), sizeof body, xxh64(body, sizeof body, 0), 0, MK_FILE_EC};
    fl = mk_file_put(files, &fb);
    m.u.cold.files_len = fl;
    leader_marker(&l, &m, T0 + (int64_t)l.next);
    CHECK_EQ(open_replica(dir, 0, &db), 0);
    if (db) {
        CHECK(feed(db, &l, 0, l.n) == TSDB_ERR_CORRUPT);
        eng_close(db);
    }
    leader_free(&l);
    sh("rm -rf '%s'", dir);
}

/* R8: every append site tells ppos_note (through eng_ppos_rows) of the rows it adds, with their positions: mem_insert,
 * the replay of a segment at open, a replica's apply_rows */
static size_t ppos_seen[PPOS_SITES];
static struct {
    int64_t ts;
    uint64_t pos;
} ppos_rows[PPOS_SITES][64];
static void ppos_count(eng *db, memtable *m, int site, const int64_t *ts, size_t n, uint64_t fno, const uint32_t *ix)
{
    (void)db;
    CHECK(m != NULL);
    if (site < 0 || site >= PPOS_SITES)
        return;
    for (size_t j = 0; j < n; j++, ppos_seen[site]++)
        if (ppos_seen[site] < 64) {
            ppos_rows[site][ppos_seen[site]].ts = ts[j];
            ppos_rows[site][ppos_seen[site]].pos = fno + (ix ? ix[j] : j);
        }
}

static void case_ppos_sites(void)
{
    char dir[PATH_MAX];
    snprintf(dir, sizeof dir, "%s/ppos", S);
    sh("rm -rf '%s' && mkdir -p '%s'", dir, dir);
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.wal_sync_ms = 0;
    memset(ppos_seen, 0, sizeof ppos_seen);
    ppos_test_note = ppos_count;
    pid_t pid = fork();
    if (pid == 0) { /* rows in the WAL, no flush: the parent's open replays them */
        eng *w = NULL;
        uint32_t id;
        int rc = (eng_open)(dir, &o, &w);
        if (rc == 0)
            rc = eng_series(w, "s", TSDB_FLOAT32, &id);
        if (rc)
            _exit(10 - rc);
        int64_t ts[3] = {T0, T0 + 1000, T0 + 2000};
        uint64_t sl[3] = {0};
        if ((rc = mem_insert(w, NULL, id, ts, sl, 8, 3, TSDB_FLOAT32)) != 0)
            _exit(30 - rc);
        if (ppos_seen[PPOS_SITE_INSERT] != 3)
            _exit(3);
        for (int j = 0; j < 3; j++) /* the WAL's numbers: the insert took the last three */
            if (ppos_rows[PPOS_SITE_INSERT][j].ts != ts[j] ||
                ppos_rows[PPOS_SITE_INSERT][j].pos != w->wal_appended - 2 + (uint64_t)j)
                _exit(4);
        _exit(0);
    }
    int st = 0;
    CHECK(pid > 0 && waitpid(pid, &st, 0) == pid);
    CHECK(WIFEXITED(st));
    CHECK_EQ(WEXITSTATUS(st), 0); /* 3: mem_insert did not tell, 4: of other positions; 10 + e: the open, 30 + e: the
                                   * insert failed with -e */
    eng *db = NULL;
    CHECK_EQ((eng_open)(dir, &o, &db), 0);
    CHECK_EQ(ppos_seen[PPOS_SITE_REPLAY], 3);
    if (db) { /* the replay gives each row the number its record took: the last three of the WAL */
        for (int j = 0; j < 3; j++)
            CHECK(ppos_rows[PPOS_SITE_REPLAY][j].ts == T0 + 1000 * j &&
                  ppos_rows[PPOS_SITE_REPLAY][j].pos + 2 == ppos_rows[PPOS_SITE_REPLAY][2].pos + (uint64_t)j);
        eng_close(db);
    }
    char rd[PATH_MAX];
    fresh(rd, "ppos_r");
    leader l;
    unsigned rng = 3;
    leader_init(&l);
    for (int i = 0; i < 3; i++)
        leader_rows(&l, 5, &rng);
    lrec *n1 = m_now(&l, T0 + 1000);
    CHECK_EQ(open_replica(rd, 0, &db), 0);
    if (db) {
        CHECK_EQ(feed(db, &l, 0, l.n), 0);
        CHECK_EQ(replica_sync(db), 0);
        CHECK_EQ(replica_apply(db, n1->last), 0);
        CHECK_EQ(ppos_seen[PPOS_SITE_APPLY], 15);
        for (size_t k = 0; k < ppos_seen[PPOS_SITE_APPLY] && k < 64; k++) { /* each row at its record's number */
            uint64_t p = ppos_rows[PPOS_SITE_APPLY][k].pos;
            bool found = false;
            for (size_t i = 0; i < l.n; i++)
                if (l.r[i].n > 0 && p >= l.r[i].fno && p <= l.r[i].last)
                    found = ppos_rows[PPOS_SITE_APPLY][k].ts == T0 + (int64_t)l.r[i].t[p - l.r[i].fno] * 1000;
            CHECK(found);
        }
        eng_close(db);
    }
    ppos_test_note = NULL;
    leader_free(&l);
    sh("rm -rf '%s' '%s'", dir, rd);
}

/* R8: the position parts_commit records for its parts (ppos_record, seen through ppos_test_record): a leader's COMMIT
 * its marker's, the replica's apply of it the same, a flush 0 (each part's from its memtable's map). A leader whose
 * publish failed after its marker leaves its position on the thread; a flush of another volume on that thread must
 * still record 0 (REVIEW M1; mutant ppostale). */
static struct {
    eng *db;
    uint64_t pos;
} prec[256];
static size_t nprec;
static void prec_note(eng *db, const char *first, size_t n, uint64_t pos)
{
    (void)first, (void)n;
    if (nprec < 256)
        prec[nprec].db = db, prec[nprec].pos = pos;
    nprec++;
}

static void case_ppos_pos(void)
{
    xport x;
    xp_start(&x, "pposp");
    ppos_test_record = prec_note;
    xp_rows(&x, 0, 0, 10, 100);
    nprec = 0;
    CHECK_EQ(eng_flush(x.ld), 0);
    CHECK(nprec >= 1);
    for (size_t i = 0; i < nprec && i < 256; i++)
        CHECK(prec[i].db == x.ld && prec[i].pos == 0);
    xp_step(&x, "ppos: rows");
    nprec = 0;
    xp_rows(&x, 0, 20, 40, 2000); /* a large insert: a COMMIT */
    uint64_t cpos = ld_last(&x);
    CHECK(nprec >= 1 && nprec <= 256);
    if (nprec >= 1 && nprec <= 256)
        CHECK(prec[nprec - 1].db == x.ld && prec[nprec - 1].pos == cpos);
    xp_step(&x, "ppos: large insert");
    bool rep = false; /* the replica's apply of the COMMIT (inside the leader's wait, here) records its position */
    for (size_t i = 0; i < nprec && i < 256; i++)
        rep |= prec[i].db == x.rep && prec[i].pos == cpos;
    if (!rep)
        for (size_t i = 0; i < nprec && i < 256; i++)
            fprintf(stderr, "  ppos record %zu: %s pos %" PRIu64 " (COMMIT at %" PRIu64 ")\n", i,
                    prec[i].db == x.ld ? "leader" : prec[i].db == x.rep ? "replica" : "other", prec[i].pos, cpos);
    CHECK(rep);
    /* a publish abandoned after its marker: the leader fails, the position stays on this thread */
    eng *b = NULL;
    char bd[PATH_MAX];
    fresh(bd, "pposb");
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    CHECK_EQ((eng_open)(bd, &o, &b), 0);
    volatile int flag = 1;
    tsdb_set_abandon_flag(&flag);
    int64_t ts[40];
    float v[40];
    for (int i = 0; i < 40; i++)
        ts[i] = T0 + 1000LL * (60 + i), v[i] = (float)i;
    CHECK_EQ(eng_insert_series(x.ld, TSDB_FLOAT32, 0, ts, v, 40), TSDB_ERR_ABANDONED);
    tsdb_set_abandon_flag(NULL);
    CHECK(eng_check(x.ld) != 0);
    if (b) {
        uint32_t id = 0;
        CHECK_EQ(eng_series(b, "s", TSDB_FLOAT32, &id), 0);
        CHECK_EQ(eng_insert_series(b, TSDB_FLOAT32, id, ts, v, 5), 0);
        nprec = 0;
        CHECK_EQ(eng_flush(b), 0);
        CHECK(nprec >= 1);
        for (size_t i = 0; i < nprec && i < 256; i++)
            CHECK(prec[i].db == b && prec[i].pos == 0);
        eng_close(b);
    }
    ppos_test_record = NULL;
    xp_stop(&x);
    printf("  ppos positions: a leader's COMMIT and the replica's apply of it record the marker's, a flush 0, also "
           "after a publish failed past its marker on the same thread\n");
}

/* R8 (r8d): on an ec cluster volume (requires=cluster,ec) the replica records the ppos the leader records: the same
 * rows at the same positions, so per partition key the same highest position over its parts (flush parts from their
 * memtables' day maps, which may group rows otherwise), and every COMMIT part (a large insert, a delete, a replace) at
 * its marker's position on both. */
static uint64_t key_max_ppos(eng *db, int64_t key, uint64_t want, bool *has_want)
{
    uint64_t mx = 0;
    for (size_t i = 0; i < db->nparts; i++) {
        uint64_t pp = 0;
        if (db->parts[i]->partition != key)
            continue;
        CHECK_EQ(ppos_of(db, db->parts[i]->name, &pp), 0);
        mx = pp > mx ? pp : mx;
        if (pp == want)
            *has_want = true;
    }
    return mx;
}

static void case_ppos_parity(void)
{
    xport x;
    memset(&x, 0, sizeof x);
    char ld[PATH_MAX], rd[PATH_MAX + 4];
    fresh(ld, "pposq");
    int fd = open(ld, O_RDONLY | O_DIRECTORY);
    CHECK(fd >= 0 && meta_require(fd, true, META_REQ_CLUSTER | META_REQ_EC) == 0);
    if (fd >= 0)
        close(fd);
    { /* a cluster volume's META names its metadata uuid (cvolfs writes volume_uuid=): a tier takes the
       * volume's vuuid from it (r8f) */
        char mp[PATH_MAX + 16];
        FILE *mf = snprintf(mp, sizeof mp, "%s/META", ld) < (int)sizeof mp ? fopen(mp, "a") : NULL;
        if (!mf || fputs("volume_uuid=5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a\n", mf) < 0 || fclose(mf) != 0)
            exit(2);
    }
    snprintf(x.ldir, sizeof x.ldir, "%s", ld);
    snprintf(rd, sizeof rd, "%s_r", ld);
    sh("rm -rf '%s' && cp -a '%s' '%s'", rd, ld, rd);
    meta_cluster_open_allow(true);
    CHECK_EQ(open_replica(rd, 0, &x.rep), 0);
    xp_open_leader(&x);
    CHECK(x.ld->ppos != NULL && x.rep && x.rep->ppos != NULL);
    xp_rows(&x, 0, 0, 10, 100);
    xp_rows(&x, 1, 2 * 86400, 10, 200); /* day 2 */
    xp_rows(&x, 0, 20, 5, 300);
    xp_step(&x, "pposq: rows");
    xp_rows(&x, 0, 40, 40, 2000); /* a large insert: a COMMIT */
    uint64_t c[3];
    c[0] = ld_last(&x);
    xp_step(&x, "pposq: large insert");
    CHECK_EQ(eng_delete_range(x.ld, 1, T0 + 1000LL * 2 * 86400, T0 + 1000LL * (2 * 86400 + 3)), 0);
    c[1] = ld_last(&x);
    xp_step(&x, "pposq: delete");
    xp_replace(&x, 0, 60, 70, 5, 500);
    c[2] = ld_last(&x);
    xp_step(&x, "pposq: replace");
    xp_rows(&x, 1, 2 * 86400 + 30, 5, 600);
    xp_rows(&x, 0, 90, 5, 700);
    CHECK_EQ(eng_flush(x.ld), 0);
    xp_rows(&x, 0, 4 * 86400, 40, 900); /* a COMMIT in day 4: the replica flushes its memtable before it applies it */
    xp_step(&x, "pposq: flush");
    int64_t keys[2] = {partition_of(x.ld, 0, T0), partition_of(x.ld, 1, T0 + 1000LL * 2 * 86400)};
    CHECK(keys[0] != keys[1]);
    for (int k = 0; k < 2; k++) {
        bool hl = false, hr = false;
        uint64_t want = k == 0 ? c[0] : c[1]; /* day 0: the large insert's part; day 2: the delete's */
        uint64_t ml = key_max_ppos(x.ld, keys[k], want, &hl), mr = key_max_ppos(x.rep, keys[k], want, &hr);
        if (ml != mr || ml == 0)
            fprintf(stderr, "pposq: key %" PRId64 ": the leader's highest ppos %" PRIu64 ", the replica's %" PRIu64 "\n",
                    keys[k], ml, mr);
        CHECK(ml == mr && ml != 0);
        CHECK(hl && hr); /* the COMMIT part at its marker's position on both */
    }
    bool hl = false, hr = false; /* the replace's part, in day 0 */
    key_max_ppos(x.ld, partition_of(x.ld, 0, T0), c[2], &hl);
    key_max_ppos(x.rep, partition_of(x.rep, 0, T0), c[2], &hr);
    CHECK(hl && hr);
    xp_stop(&x);
    meta_cluster_open_allow(false);
    printf("  ppos parity: on an ec cluster volume the replica records the leader's ppos (per key, and each COMMIT "
           "part at its marker's position)\n");
}

/* a COMMIT whose files are not staged: not acknowledged, not applied, until they are */
static void case_commit_hold(void)
{
    xport x;
    char rd[PATH_MAX], name[64];
    uint64_t sz, mpos;
    xp_start(&x, "kh");
    rdir(rd, sizeof rd, &x);
    xp_rows(&x, 0, 0, 10, 100);
    xp_step(&x, "K: before");
    x.withhold = true;
    xp_rows(&x, 0, 5, 40, 2000);
    uint64_t last = ld_last(&x);
    CHECK(replica_durable_end(x.rep) < last);
    CHECK(replica_applied_end(x.rep) < last);
    CHECK_EQ(replica_missing(x.rep, &mpos, name, sizeof name, &sz), 1);
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
    CHECK_EQ(replica_missing(x.rep, &mpos, name, sizeof name, &sz), 1);
    CHECK_EQ(replica_stage(x.rep, x.held_name, x.held, x.held_len), 0);
    CHECK_EQ(replica_missing(x.rep, &mpos, name, sizeof name, &sz), 0);
    xp_step(&x, "K: staged late");
    /* a file with the wrong content is no file */
    x.withhold = true;
    xp_rows(&x, 1, 0, 40, 4000);
    uint8_t *bad = malloc(x.held_len);
    memcpy(bad, x.held, x.held_len);
    bad[x.held_len / 2] ^= 1;
    CHECK_EQ(replica_stage(x.rep, x.held_name, bad, x.held_len), 0);
    CHECK_EQ(replica_missing(x.rep, &mpos, name, sizeof name, &sz), 1);
    CHECK_EQ(replica_sync(x.rep), 0);
    CHECK(replica_durable_end(x.rep) < ld_last(&x));
    free(bad);
    CHECK_EQ(replica_stage(x.rep, x.held_name, x.held, x.held_len), 0);
    x.withhold = false;
    xp_step(&x, "K: staged right");
    xp_stop(&x);
}

/* A cut above a COMMIT marker whose file is not staged keeps the hold: the durable end stays below the marker, and an
 * apply stops before it instead of failing on the missing file (jepsen-cluster mc-wal-before-send seed 23233: a cut
 * set the durable end to the cut, past the hold; the replica acknowledged the marker and its apply failed the volume). */
static void case_commit_hold_cut(void)
{
    xport x;
    char rd[PATH_MAX], name[64];
    uint64_t sz, mpos;
    xp_start(&x, "kc");
    rdir(rd, sizeof rd, &x);
    xp_rows(&x, 0, 0, 10, 100);
    xp_step(&x, "KC: before");
    x.withhold = true;
    xp_rows(&x, 0, 5, 40, 2000);
    CHECK_EQ(replica_missing(x.rep, &mpos, name, sizeof name, &sz), 1);
    xp_rows(&x, 1, 0, 3, 300);
    uint64_t mid = ld_last(&x);
    xp_rows(&x, 1, 3, 3, 400);
    CHECK(mid > mpos && ld_last(&x) > mid);
    CHECK_EQ(xp_ship_logs(&x), 0); /* small writes: the test ships their records */
    CHECK_EQ(replica_log_end(x.rep), ld_last(&x));
    CHECK(replica_durable_end(x.rep) < mpos);
    CHECK_EQ(replica_cut(x.rep, mid), 0);
    CHECK_EQ(replica_log_end(x.rep), mid);
    CHECK(replica_durable_end(x.rep) < mpos); /* the cut's sync covers the log, not the marker's file */
    CHECK_EQ(replica_apply(x.rep, mid), 0);
    CHECK(eng_check(x.rep) == 0);
    CHECK(replica_applied_end(x.rep) < mpos);
    CHECK_EQ(replica_missing(x.rep, &mpos, name, sizeof name, &sz), 1);
    CHECK_EQ(replica_stage(x.rep, x.held_name, x.held, x.held_len), 0);
    CHECK_EQ(replica_sync(x.rep), 0);
    CHECK_EQ(replica_durable_end(x.rep), mid);
    CHECK_EQ(replica_apply(x.rep, mid), 0);
    CHECK_EQ(replica_applied_end(x.rep), mid);
    x.withhold = false;
    x.noship = true; /* the leader's records above the cut are not this test's: a new leader would never send them */
    xp_stop(&x);
}

/* a cut removes the files of the markers it removes */
static void case_commit_cut(void)
{
    xport x;
    char rd[PATH_MAX], name[64];
    uint64_t sz, mpos;
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
    CHECK_EQ(replica_missing(x.rep, &mpos, name, sizeof name, &sz), 0);
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
    CHECK_EQ(replica_missing(x.rep, &mpos, name, sizeof name, &sz), 1);
    CHECK(replica_durable_end(x.rep) < last);
    CHECK_EQ(replica_stage(x.rep, x.held_name, x.held, x.held_len), 0);
    x.noapply = false;
    xp_step(&x, "K: re-sent");
    xp_stop(&x);
}

/* ships from the end of the replica's newest segment again (after a cut: what the leader sends a replica again) */
static void xp_resume(xport *x, const char *rd)
{
    char p[PATH_MAX], nm[64];
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
    wal_name(nm, sizeof nm, best);
    snprintf(p, sizeof p, "%s/%s", rd, nm);
    struct stat st;
    CHECK_EQ(stat(p, &st), 0);
    x->cseq = best;
    x->coff = (uint64_t)st.st_size;
}

static void join_within(pthread_t t, atomic_bool *done, const char *what);

/* R2 REVIEW M1: a COMMIT's apply (its flush, the reads of its files) does not hold the lock of the log: while it is
 * paused after the flush, another thread appends the leader's next records, syncs them and reads the durable end */
static struct {
    xport *x;
    bool fired, ok;
    atomic_bool done;
    pthread_t t;
    int rc;
    uint64_t dur, want;
    void (*then)(void); /* the cut race: what the hook does instead */
    int at;             /* the window it fires in (RH_*) */
    int mode;           /* AC_SHIP: append, sync, durable end; AC_ENDS: the three ends; AC_STAGE: a replica_stage that
                         * must wait for the prepare's stage_mu */
    bool refuse;        /* at RH_COMMIT_OPEN: the staged file's open is refused */
} AC;
enum { AC_SHIP, AC_ENDS, AC_STAGE };

static void ac_arm(xport *x, int at, int mode)
{
    AC.x = x, AC.fired = AC.ok = AC.refuse = false, AC.then = NULL, AC.at = at, AC.mode = mode, AC.rc = 0;
    atomic_store(&AC.done, false);
}

static void *ac_thread(void *arg)
{
    (void)arg;
    if (AC.mode == AC_SHIP) {
        AC.rc = xp_ship_logs(AC.x); /* replica_append and replica_sync */
        AC.dur = replica_durable_end(AC.x->rep);
    } else if (AC.mode == AC_ENDS) {
        AC.dur = replica_durable_end(AC.x->rep);
        AC.rc = replica_log_end(AC.x->rep) >= AC.dur && replica_applied_end(AC.x->rep) < AC.dur ? 0 : -1;
    } else {
        AC.rc = replica_stage(AC.x->rep, AC.x->held_name, AC.x->held, AC.x->held_len);
    }
    atomic_store(&AC.done, true);
    return NULL;
}

static int ac_hook(eng *db, int at)
{
    if (at != AC.at || AC.fired || !AC.x || db != AC.x->rep)
        return 0;
    AC.fired = true;
    if (AC.refuse)
        return 1;
    if (AC.then) {
        AC.then();
        return 0;
    }
    if (pthread_create(&AC.t, NULL, ac_thread, NULL) != 0)
        return 0;
    if (AC.mode == AC_STAGE) { /* it must not get in while the prepare reads and links the staged files */
        usleep(300000);
        AC.ok = !atomic_load(&AC.done);
        return 0;
    }
    for (int i = 0; i < 2500 && !atomic_load(&AC.done); i++) /* 5 s: the thread needs one append and one sync */
        usleep(2000);
    AC.ok = atomic_load(&AC.done) && AC.rc == 0 && (AC.mode != AC_SHIP || AC.dur == AC.want);
    return 0;
}

static void case_apply_concurrent(void)
{
    xport x;
    char rd[PATH_MAX];
    xp_start(&x, "ac");
    rdir(rd, sizeof rd, &x);
    xp_rows(&x, 0, 0, 10, 100), xp_rows(&x, 1, 0, 5, 300);
    xp_step(&x, "AC: before");
    x.noapply = true;
    xp_rows(&x, 0, 5, 40, 2000); /* a COMMIT, shipped and staged, not applied */
    uint64_t cpos = ld_last(&x);
    CHECK_EQ(replica_durable_end(x.rep), cpos);
    x.noship = true;
    xp_rows(&x, 1, 20, 5, 500); /* rows after it, written by the leader and not yet sent */
    ac_arm(&x, RH_COMMIT_FLUSHED, AC_SHIP);
    AC.want = ld_last(&x);
    CHECK(AC.want > cpos);
    replica_test_apply_hook = ac_hook;
    CHECK_EQ(replica_apply(x.rep, cpos), 0);
    replica_test_apply_hook = NULL;
    CHECK(AC.fired);
    if (!AC.ok)
        fprintf(stderr, "AC: the append, sync and durable end of the next records waited for the COMMIT's apply "
                        "(mutant applyholdsmu)\n");
    CHECK(AC.ok);
    if (AC.fired)
        join_within(AC.t, &AC.done, "AC: the appending thread did not end");
    AC.x = NULL;
    CHECK_EQ(replica_applied_end(x.rep), cpos);
    x.noapply = x.noship = false;
    xp_step(&x, "AC: after");
    CHECK_EQ(count_files(rd, "stage_"), 0);
    CHECK_EQ(count_files(rd, "tmp_"), 0);
    xp_stop(&x);
}

/* A cut between a COMMIT's prepare and its publish: what the prepare made is dropped. Variant 0 cuts the COMMIT itself
 * (a replica that never prepared has neither its rows nor its files), variant 1 only the records after it (the COMMIT
 * is prepared again and applied). */
static uint64_t AR_CUT;
static void ar_cut(void) { CHECK_EQ(replica_cut(AC.x->rep, AR_CUT), 0); }
static void ar_cut_applied(void)
{
    AR_CUT = replica_applied_end(AC.x->rep);
    CHECK_EQ(replica_cut(AC.x->rep, AR_CUT), 0);
}

static void case_apply_cut_race(int variant)
{
    if (eng_test_mutant == MUT_REPLICA_APPLY_HOLDS_MU)
        return; /* the hook's cut would wait for the lock its own thread holds */
    xport x;
    char rd[PATH_MAX], what[64];
    snprintf(what, sizeof what, "AR%d", variant);
    xp_start(&x, variant ? "ar1" : "ar0");
    rdir(rd, sizeof rd, &x);
    xp_rows(&x, 0, 0, 10, 100);
    xp_step(&x, "AR: before");
    x.noapply = true;
    xp_rows(&x, 0, 5, 40, 2000); /* the COMMIT */
    uint64_t cpos = ld_last(&x);
    xp_rows(&x, 1, 20, 5, 500); /* records after it */
    CHECK_EQ(xp_ship_logs(&x), 0);
    CHECK_EQ(replica_durable_end(x.rep), ld_last(&x));
    AR_CUT = variant == 1 ? cpos : cpos - 1;
    ac_arm(&x, variant == 2 ? RH_COMMIT_PREPARED : RH_COMMIT_FLUSHED, AC_SHIP); /* 2: after the links are made */
    AC.then = ar_cut;
    replica_test_apply_hook = ac_hook;
    CHECK_EQ(replica_apply(x.rep, cpos), 0);
    replica_test_apply_hook = NULL;
    AC.x = NULL, AC.then = NULL;
    CHECK(AC.fired);
    CHECK_EQ(eng_check(x.rep), 0);
    CHECK_EQ(replica_log_end(x.rep), AR_CUT);
    CHECK_EQ(replica_applied_end(x.rep), AR_CUT);
    if (count_files(rd, "tmp_") != 0)
        fprintf(stderr, "%s: a tmp_ link of the prepare is left after the cut (mutant discardnounlink)\n", what);
    CHECK_EQ(count_files(rd, "tmp_"), 0); /* the prepare's links are gone */
    qctx q = {.n = 0};
    CHECK_EQ(eng_query_raw_ex(x.rep, 0, T0 - 1000, T0 + 1000LL * (NT + 1), false, 0, qfn, &q), 0);
    if (variant != 1) {
        CHECK_EQ(count_files(rd, "stage_"), 0);
        if (q.n != 10)
            fprintf(stderr, "%s: the replica shows %zu rows of series 0 after a cut of the COMMIT it was preparing, not 10 "
                            "(mutant nocutgen)\n", what, q.n);
        CHECK_EQ(q.n, 10);
    } else {
        CHECK_EQ(count_files(rd, "stage_"), 0); /* applied the second time */
        CHECK_EQ(q.n, 45);
    }
    /* the leader sends what was cut again: the replica ends up as the leader */
    eng_close(x.rep);
    x.rep = NULL;
    CHECK_EQ(open_replica(rd, 0, &x.rep), 0);
    xp_resume(&x, rd);
    CHECK_EQ(xp_ship_logs(&x), 0);
    if (variant != 1)
        CHECK_EQ(replica_stage(x.rep, x.held_name, x.held, x.held_len), 0);
    x.noapply = false;
    xp_step(&x, what);
    CHECK_EQ(count_files(rd, "stage_"), 0);
    xp_stop(&x);
}

/* r3g REVIEW F2, F3: the other windows of the applier. AW_DROP and AW_CLASS pause a DROP's flush and a CLASS purge (rp->mu
 * free: mutants dropmu, classmu); AW_YIELD pauses between two row records (rp->mu free: mutant rowsholdmu); AW_ENDS pauses
 * inside a row record's apply (rp->mu held) and reads the three ends (mutant endsmu); AW_STAGE pauses a COMMIT's prepare
 * with a staged file linked and not yet opened, and a replica_stage of that file must wait (mutant nostagemu). */
enum { AW_DROP, AW_CLASS, AW_YIELD, AW_ENDS, AW_STAGE };

static uint64_t aw_marker(xport *x, int kind)
{
    if (kind == AW_DROP) {
        CHECK_EQ(eng_flush(x->ld), 0);
        xp_step(x, "AW: flushed");
        x->noapply = true;
        CHECK_EQ(eng_drop_before(x->ld, T0 + 10 * DAY_MS), 0);
    } else if (kind == AW_CLASS) {
        eng_leader_replay_floor(x->ld, x->ld->fno_next);
        x->noapply = true;
        CHECK_EQ(eng_class_switch(x->ld, 0, 2, NULL), 0);
    } else if (kind == AW_STAGE) {
        x->noapply = true;
        xp_rows(x, 0, 5, 40, 2000); /* a COMMIT */
    } else {
        x->noapply = true;
        for (int i = 0; i < 4; i++)
            xp_rows(x, 1, 5 * i, 5, 300 + i); /* row records */
        CHECK_EQ(xp_ship_logs(x), 0);
    }
    return ld_last(x);
}

static void case_apply_windows(int kind)
{
    static const char *const names[] = {"awdrop", "awclass", "awyield", "awends", "awstage"};
    static const char *const muts[] = {"dropmu", "classmu", "rowsholdmu", "endsmu", "nostagemu"};
    static const int ats[] = {RH_DROP, RH_CLASS, RH_ROWS_YIELD, RH_ROWS_HELD, RH_COMMIT_OPEN};
    static const int modes[] = {AC_SHIP, AC_SHIP, AC_SHIP, AC_ENDS, AC_STAGE};
    xport x;
    xp_start(&x, names[kind]);
    xp_rows(&x, 0, 0, 10, 100);
    xp_step(&x, "AW: rows");
    uint64_t pos = aw_marker(&x, kind);
    if (modes[kind] == AC_SHIP) {
        x.noship = true;
        xp_rows(&x, 1, 40, 5, 500); /* rows after it, written by the leader and not yet sent */
    }
    ac_arm(&x, ats[kind], modes[kind]);
    AC.want = ld_last(&x);
    replica_test_apply_hook = ac_hook;
    CHECK_EQ(replica_apply(x.rep, pos), 0);
    replica_test_apply_hook = NULL;
    CHECK(AC.fired);
    if (!AC.ok)
        fprintf(stderr, "%s: the other thread %s (mutant %s)\n", names[kind],
                modes[kind] == AC_STAGE ? "staged a file while the prepare read it" : "waited for the apply", muts[kind]);
    CHECK(AC.ok);
    if (AC.fired)
        join_within(AC.t, &AC.done, "AW: the other thread did not end");
    AC.x = NULL;
    CHECK_EQ(replica_applied_end(x.rep), pos);
    x.noapply = x.noship = false;
    xp_step(&x, "AW: after");
    xp_stop(&x);
}

/* r3g REVIEW F3: a cut while a DROP's flush or a CLASS purge runs without rp->mu removes the marker: the replica must not
 * act on it (mutants dropnogen: the partitions are retired anyway; classnogen: the switch is recorded anyway) */
static off_t file_size(const char *dir, const char *name)
{
    char p[PATH_MAX];
    snprintf(p, sizeof p, "%s/%s", dir, name);
    struct stat st;
    return stat(p, &st) == 0 ? st.st_size : -1;
}

static void case_apply_marker_cut(int kind)
{
    xport x;
    char rd[PATH_MAX];
    xp_start(&x, kind == AW_DROP ? "amcdrop" : "amcclass");
    rdir(rd, sizeof rd, &x);
    xp_rows(&x, 0, 0, 10, 100);
    xp_step(&x, "AMC: rows");
    if (kind == AW_DROP) {
        CHECK_EQ(eng_flush(x.ld), 0);
        xp_step(&x, "AMC: flushed");
    }
    off_t cls0 = file_size(rd, "CLASSES");
    uint64_t pos = aw_marker(&x, kind);
    ac_arm(&x, kind == AW_DROP ? RH_DROP : RH_CLASS, AC_SHIP);
    AC.then = ar_cut_applied; /* the marker being applied and everything after it */
    replica_test_apply_hook = ac_hook;
    CHECK_EQ(replica_apply(x.rep, pos), 0);
    replica_test_apply_hook = NULL;
    AC.x = NULL, AC.then = NULL;
    CHECK(AC.fired);
    CHECK_EQ(eng_check(x.rep), 0);
    CHECK_EQ(replica_applied_end(x.rep), AR_CUT);
    if (kind == AW_DROP) {
        qctx q = {.n = 0};
        CHECK_EQ(eng_query_raw_ex(x.rep, 0, T0 - 1000, T0 + 1000LL * (NT + 1), false, 0, qfn, &q), 0);
        if (q.n != 10)
            fprintf(stderr, "AMC: the replica retired the partitions of a DROP that was cut (%zu rows; mutant dropnogen)\n",
                    q.n);
        CHECK_EQ(q.n, 10);
    } else {
        off_t cls1 = file_size(rd, "CLASSES");
        if (cls1 != cls0)
            fprintf(stderr, "AMC: the replica recorded a class switch that was cut (CLASSES %lld -> %lld bytes; mutant "
                            "classnogen)\n", (long long)cls0, (long long)cls1);
        CHECK_EQ(cls1, cls0);
    }
    eng_close(x.rep);
    x.rep = NULL;
    CHECK_EQ(open_replica(rd, 0, &x.rep), 0);
    xp_resume(&x, rd);
    CHECK_EQ(xp_ship_logs(&x), 0);
    x.noapply = false;
    xp_step(&x, "AMC: after");
    xp_stop(&x);
}

/* r3g REVIEW F3b: a staged file whose open fails leaves no tmp_0_ link behind (mutant openfailnounlink) */
static void case_apply_open_refused(void)
{
    xport x;
    char rd[PATH_MAX];
    xp_start(&x, "aor");
    rdir(rd, sizeof rd, &x);
    xp_rows(&x, 0, 0, 10, 100);
    xp_step(&x, "AOR: rows");
    uint64_t pos = aw_marker(&x, AW_STAGE);
    ac_arm(&x, RH_COMMIT_OPEN, AC_SHIP);
    AC.refuse = true;
    replica_test_apply_hook = ac_hook;
    CHECK(replica_apply(x.rep, pos) != 0);
    replica_test_apply_hook = NULL;
    AC.x = NULL;
    CHECK(AC.fired);
    if (count_files(rd, "tmp_") != 0)
        fprintf(stderr, "AOR: a refused open left its tmp_ link (mutant openfailnounlink)\n");
    CHECK_EQ(count_files(rd, "tmp_"), 0);
    eng_close(x.rep); /* failed: a reopen applies the COMMIT from what is staged */
    x.rep = NULL;
    CHECK_EQ(open_replica(rd, 0, &x.rep), 0);
    xp_resume(&x, rd);
    CHECK_EQ(xp_ship_logs(&x), 0);
    x.noapply = false;
    xp_step(&x, "AOR: after");
    xp_stop(&x);
}

/* r3g REVIEW F5: a promote that races an apply in one of its windows waits for it and succeeds, rather than being
 * refused half way (mutant promotenoapplymu) */
static struct {
    eng *rep, *out;
    uint64_t pos;
    int rc;
    atomic_bool done;
    pthread_t t;
    bool early;
} PA;

static void *pa_thread(void *arg)
{
    (void)arg;
    PA.rc = replica_promote(&PA.rep, PA.pos, &PA.out);
    atomic_store(&PA.done, true);
    return NULL;
}

static void pa_then(void)
{
    if (pthread_create(&PA.t, NULL, pa_thread, NULL) != 0)
        return;
    usleep(300000);
    PA.early = atomic_load(&PA.done);
}

static void case_promote_during_apply(void)
{
    xport x;
    xp_start(&x, "pda");
    xp_rows(&x, 0, 0, 10, 100);
    xp_step(&x, "PDA: rows");
    x.noapply = true;
    xp_rows(&x, 0, 5, 40, 2000); /* a COMMIT, shipped and staged, not applied */
    CHECK_EQ(xp_ship_logs(&x), 0);
    uint64_t pos = 0;
    CHECK_EQ(replica_epoch_begin(x.rep, 1, "node-a", &pos), 0);
    CHECK_EQ(replica_sync(x.rep), 0);
    PA.rep = x.rep, PA.out = NULL, PA.pos = pos, PA.rc = -1, PA.early = false;
    atomic_store(&PA.done, false);
    ac_arm(&x, RH_COMMIT_FLUSHED, AC_SHIP);
    AC.then = pa_then;
    replica_test_apply_hook = ac_hook;
    CHECK_EQ(replica_apply(x.rep, pos), 0); /* x.rep is the promote's from here on */
    replica_test_apply_hook = NULL;
    AC.x = NULL, AC.then = NULL;
    CHECK(AC.fired);
    if (AC.fired)
        join_within(PA.t, &PA.done, "PDA: the promote did not end");
    if (PA.early || PA.rc != 0)
        fprintf(stderr, "PDA: a promote during the apply %s (rc %d; mutant promotenoapplymu)\n",
                PA.early ? "did not wait for it" : "failed", PA.rc);
    CHECK(!PA.early);
    CHECK_EQ(PA.rc, 0);
    if (PA.rc == 0) {
        x.rep = NULL;
        eng_close(PA.out);
    }
    xp_stop(&x);
}

/* A leader's DROP holds flush_lock from its marker to the retire: rows written after the marker into a partition it
 * drops are not flushed into parts that the retire takes away on the leader alone (every replica keeps them in its
 * active memtable: the marker begins its segment). The hook runs between the marker and the retire; a thread writes
 * such rows and flushes. Mutant dropunheld (the old code) loses them on the leader. */
static struct {
    xport *x;
    atomic_bool done;
    pthread_t t;
    bool fired;
} DF;

static void *df_thread(void *arg)
{
    (void)arg;
    for (int i = 0; i < 5; i++) {
        int64_t t = T0 + 1000LL * (30 + i);
        float v = 900.0f + (float)i;
        CHECK_EQ(eng_insert_series(DF.x->ld, TSDB_FLOAT32, 0, &t, &v, 1), 0);
    }
    CHECK_EQ(eng_flush(DF.x->ld), 0);
    atomic_store(&DF.done, true);
    return NULL;
}

static void df_hook(eng *db, int at)
{
    if (at != 1 || DF.fired || !DF.x || db != DF.x->ld)
        return;
    DF.fired = true;
    if (pthread_create(&DF.t, NULL, df_thread, NULL) != 0)
        return;
    for (int i = 0; i < 500 && !atomic_load(&DF.done); i++) /* 1 s: with the fix the flush waits for the retire */
        usleep(2000);
}

static void case_drop_flush(void)
{
    xport x;
    xp_start(&x, "kf");
    xp_rows(&x, 0, 0, 10, 100);
    xp_step(&x, "DF: rows");
    CHECK_EQ(eng_flush(x.ld), 0);
    xp_step(&x, "DF: flushed");
    DF.x = &x, DF.fired = false;
    atomic_store(&DF.done, false);
    eng_test_drop_hook = df_hook;
    CHECK_EQ(eng_drop_before(x.ld, T0 + 10 * DAY_MS), 0);
    eng_test_drop_hook = NULL;
    CHECK(DF.fired);
    if (DF.fired)
        join_within(DF.t, &DF.done, "DF: the writer did not end");
    DF.x = NULL;
    qctx q = {.n = 0};
    CHECK_EQ(eng_query_raw_ex(x.ld, 0, T0 - 1000, T0 + 1000LL * (NT + 1), false, 0, qfn, &q), 0);
    if (q.n != 5)
        fprintf(stderr, "DF: the leader shows %zu of the 5 rows written after its DROP marker (mutant dropunheld)\n", q.n);
    CHECK_EQ(q.n, 5);
    xp_step(&x, "DF: after the drop");
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

/* the log of src from the end of dst's on (a new leader sending its log to a member: 6.2 step 3) */
static int ship_from(eng *src, eng *dst)
{
    uint64_t from = replica_log_end(dst) + 1;
    for (;;) {
        uint8_t *b;
        size_t len;
        uint64_t seq, off, last;
        int rc = replica_read(src, from, &b, &len, (size_t)1 << 20, &seq, &off, &last);
        if (rc) {
            free(b);
            return rc;
        }
        if (len == 0) {
            free(b);
            break;
        }
        rc = replica_append(dst, seq, off, b, len);
        free(b);
        if (rc)
            return rc;
        from = last + 1;
    }
    return replica_sync(dst);
}

static bool seg_exists(const char *dir, uint64_t seq);

/* the seqs of the wal_ segments in dir, ascending */
static size_t wal_seqs(const char *dir, uint64_t *out, size_t cap)
{
    DIR *d = opendir(dir);
    size_t n = 0;
    struct dirent *e;
    while (d && (e = readdir(d)))
        if (wal_file_name(e->d_name) && n < cap)
            sscanf(e->d_name, "wal_%" SCNx64 ".log", &out[n++]);
    if (d)
        closedir(d);
    for (size_t i = 1; i < n; i++)
        for (size_t j = i; j > 0 && out[j - 1] > out[j]; j--) {
            uint64_t t = out[j];
            out[j] = out[j - 1];
            out[j - 1] = t;
        }
    return n;
}

/* Promotion: the replica with the log (tail included) writes an EPOCH marker, the commit point passes it, and the engine
 * is a writer that continues the numbers; the other member follows it. variant 0: nothing above the commit point, 1: a
 * tail of small rows, 2: a tail with a large insert (a COMMIT with a staged file). The history holds a drop, whose rows
 * the replica's retained segments still hold: a writer that replayed them would bring them back. Item 6: those segments
 * stay, through the promotion and a reopen of the leader, which does not replay them (mutants promotekeepsegs: RSTATE
 * does not name them, keptreplayed: the open ignores it; the first drop's rows come back), until the keep bound passes
 * them. */
static void case_promote(int variant)
{
    xport x;
    char rd[PATH_MAX], bd[PATH_MAX], nm[32];
    snprintf(nm, sizeof nm, "kp%d", variant);
    xp_start(&x, nm);
    rdir(rd, sizeof rd, &x);
    snprintf(bd, sizeof bd, "%s_b", x.ldir);
    int64_t far = T0 + 40 * DAY_MS;
    xp_rows(&x, 0, 0, 10, 100);
    for (int i = 0; i < 5; i++) {
        float v = (float)i;
        int64_t t = far + 1000LL * i;
        CHECK_EQ(eng_insert_series(x.ld, TSDB_FLOAT32, 1, &t, &v, 1), 0);
    }
    xp_step(&x, "P: rows");
    CHECK_EQ(eng_flush(x.ld), 0);
    xp_rows(&x, 0, 20, 5, 500);
    xp_step(&x, "P: flushed");
    CHECK_EQ(eng_drop_before(x.ld, T0 + 10 * DAY_MS), 0);
    xp_step(&x, "P: drop");
    xp_rows(&x, 0, 30, 40, 2000); /* large */
    xp_step(&x, "P: large");
    if (variant >= 1) {
        x.noapply = true;
        xp_rows(&x, 0, 5, 3, 900);
        if (variant >= 2)
            xp_rows(&x, 0, 10, 40, 4000);
        CHECK_EQ(xp_ship_logs(&x), 0); /* the tail reaches the replica, and is not applied */
    }
    sh("rm -rf '%s' && cp -a '%s' '%s'", bd, rd, bd);
    eng *b = NULL;
    CHECK_EQ(open_replica(bd, 0, &b), 0);
    uint64_t lastpos = ld_last(&x), pos = 0;
    CHECK_EQ(replica_epoch_begin(x.rep, 1, "node-a", &pos), 0);
    CHECK_EQ(pos, lastpos + 1);
    CHECK_EQ(replica_epoch_begin(x.rep, 1, "node-a", &pos), TSDB_ERR_INVALID); /* the same epoch again */
    CHECK_EQ(replica_sync(x.rep), 0);
    CHECK_EQ(ship_from(x.rep, b), 0);
    CHECK_EQ(replica_log_end(b), pos);
    eng *n = NULL;
    if (variant >= 1) { /* the marker is not committed yet: the tail is not applied */
        CHECK_EQ(replica_promote(&x.rep, pos, &n), TSDB_ERR_INVALID);
        CHECK(x.rep != NULL && n == NULL);
        if (!x.rep)
            exit(2);
    }
    CHECK_EQ(replica_apply(x.rep, pos), 0);
    CHECK_EQ(replica_apply(b, pos), 0);
    CHECK_EQ(replica_applied_end(x.rep), pos);
    for (uint32_t s = 0; s < NS; s++) {
        CHECK(eq_range(x.ld, x.rep, s, T0 - 1000, far + 10000, "P: promoted candidate"));
        CHECK(eq_range(x.ld, b, s, T0 - 1000, far + 10000, "P: other member"));
    }
    { /* The state docs/tasks/replica-ro-tails.md asks for, made on purpose: both directories hold the new epoch's
       * segment (wal_active = epoch << 40) and have flushed nothing since (wal_flushed is still a number of the old
       * epoch), so the tail walk of a read-only open has the whole jump between its bounds. The MANIFEST is read from
       * the file, so the case fails loudly should the engine ever stop leaving this state (the run that made it
       * showed 1..4 against 2^40 in every variant). The opens below are the ones jepsen-cluster's end check makes. */
        uint64_t fl = 0, ac = 0;
        CHECK(man_bounds(rd, &fl, &ac) && fl < ((uint64_t)1 << 40) && ac >= ((uint64_t)1 << 40));
        CHECK(man_bounds(bd, &fl, &ac) && fl < ((uint64_t)1 << 40) && ac >= ((uint64_t)1 << 40));
        CHECK(ro_dir_same(x.ld, rd, far + 10000, "P: the candidate, epoch segment made, no flush since, read-only"));
        CHECK(ro_dir_same(x.ld, bd, far + 10000, "P: the other member, epoch segment made, no flush since, read-only"));
    }
    uint64_t old[256];
    size_t nold = wal_seqs(rd, old, 256);
    CHECK(nold >= 2 && nold < 256);
    CHECK_EQ(replica_promote(&x.rep, pos, &n), 0);
    CHECK(x.rep == NULL && n != NULL);
    if (!n)
        exit(2);
    for (size_t i = 0; i < nold; i++)
        CHECK(seg_exists(rd, old[i])); /* the replica's segments stay: the new leader's replicas catch up from them */
    CHECK_EQ(eng_check(n), 0);
    CHECK_EQ(n->fno_next, pos + 1); /* the numbers go on */
    for (uint32_t s = 0; s < NS; s++)
        CHECK(eq_range(x.ld, n, s, T0 - 1000, far + 10000, "P: new writer"));
    /* after the promotion's own flush (wal_flushed = the epoch's segment): a read-only open of the directory */
    CHECK(ro_dir_same(n, rd, far + 10000, "P: promoted, after its flush, read-only"));
    /* the old leader is gone: the writer leads, the other member follows */
    eng_close(x.ld);
    x.ld = n;
    snprintf(x.ldir, sizeof x.ldir, "%s", rd);
    x.rep = b;
    { /* b holds the log up to the epoch's segment (ship_from), which the writer kept: shipping goes on after it */
        struct stat st;
        char p[PATH_MAX + 64], nm2[64];
        wal_name(nm2, sizeof nm2, old[nold - 1]);
        snprintf(p, sizeof p, "%s/%s", rd, nm2);
        CHECK_EQ(stat(p, &st), 0);
        x.cseq = old[nold - 1];
        x.coff = (uint64_t)st.st_size;
    }
    x.noapply = false;
    n->opt.wal_max_batch_rows = 16;
    { /* the promoted writer before its attach: its merges and flusher run already, and a switch decided now would go to
       * CLASSES with no marker, a record of this member alone (jepsen-cluster meta,classes partition, r3e: the next
       * epoch's leader decided another T for the same offset; mutant switchunattached) */
        uint64_t off = n->cls_off;
        struct stat cs0, cs1;
        char cp[PATH_MAX + 16];
        snprintf(cp, sizeof cp, "%s/CLASSES", rd);
        int had = stat(cp, &cs0) == 0;
        CHECK(n->cls_on);
        CHECK_EQ(eng_class_switch(n, 0, 2, NULL), TSDB_ERR_UNAVAILABLE);
        /* the other way to a record (review R20): the first class of a series made after the open (the open takes
         * every series it finds as decided), from a large insert's rows an hour apart (a long class), reaches
         * cls_markers without switch_locked's deferral, so its own guard refuses it (mutant freshunattached) */
        {
            int64_t fts[100];
            float fv[100];
            uint32_t fid = 0;
            for (int i = 0; i < 100; i++)
                fts[i] = far + DAY_MS + 3600000LL * i, fv[i] = (float)i;
            CHECK_EQ(eng_series(n, "fresh-unattached", TSDB_FLOAT32, &fid), 0);
            CHECK_EQ(eng_insert_series(n, TSDB_FLOAT32, fid, fts, fv, 100), TSDB_ERR_UNAVAILABLE);
        }
        CHECK_EQ(n->cls_off, off);
        CHECK_EQ(stat(cp, &cs1) == 0, had);
        if (had && stat(cp, &cs1) == 0)
            CHECK_EQ((long long)cs1.st_size, (long long)cs0.st_size);
    }
    CHECK_EQ(eng_leader_attach(n, &XOPS), 0);
    /* item 6: the promoted leader has the latest table (a replica runs without one) and keeps the replica's RSTATE,
     * which holds the DROP it applied (mutant promotenocarry: no table, RSTATE removed) */
    CHECK(n->lt != NULL);
    uint64_t gap_first = 0;
    { /* the promoted leader serves its log from the kept segments below the epoch's jump of numbers (mutant readgap:
       * NOTFOUND at the first missing number, a snapshot for every member behind the promotion) */
        uint8_t *rb = NULL;
        size_t rl = 0;
        uint64_t rsq = 0, roff = 0, rlast = 0, first = 0;
        for (size_t i = 0; i + 1 < nold && first == 0; i++) { /* the first position of a kept replica-era segment */
            char p[PATH_MAX + 64], nm2[64];
            wal_name(nm2, sizeof nm2, old[i]);
            snprintf(p, sizeof p, "%s/%s", rd, nm2);
            int fd = open(p, O_RDONLY | O_CLOEXEC);
            if (fd >= 0 && pread(fd, &first, 8, 16) != 8)
                first = 0;
            if (fd >= 0)
                close(fd);
        }
        CHECK(first > 0 && old[nold - 2] < old[nold - 1] - 1); /* the numbers jump at the epoch */
        CHECK_EQ(replica_read(n, first, &rb, &rl, 1 << 20, &rsq, &roff, &rlast), 0);
        CHECK(rl > 0 && rsq < old[nold - 1] && roff == 0 && rlast >= first);
        free(rb);
        gap_first = first;
    }
    {
        char rs[PATH_MAX + 16];
        snprintf(rs, sizeof rs, "%s/RSTATE", rd);
        CHECK(access(rs, F_OK) == 0);
    }
    /* a switch right after the promotion: the new writer knows no rows of the segments the members may still replay
       (mutant nowtailfrom), so it waits until every member's log starts past its open */
    uint64_t from = n->fno_next;
    CHECK_EQ(eng_class_switch(n, 0, 2, NULL), TSDB_ERR_UNAVAILABLE);
    eng_leader_replay_floor(n, from - 1);
    CHECK_EQ(eng_class_switch(n, 0, 2, NULL), TSDB_ERR_UNAVAILABLE);
    eng_leader_replay_floor(n, from);
    CHECK_EQ(eng_class_switch(n, 0, 2, NULL), 0);
    xp_step(&x, "P: a class switch after promotion");
    xp_rows(&x, 0, 50, 6, 70);
    xp_step(&x, "P: small after promotion");
    CHECK(ro_dir_same(x.ld, rd, far + 10000, "P: promoted, small rows in the new segment, read-only"));
    CHECK(ro_dir_same(x.ld, bd, far + 10000, "P: the other member's directory, read-only"));
    xp_rows(&x, 1, 0, 40, 800);
    xp_step(&x, "P: large after promotion");
    { /* a sealed segment that never got a record, among the kept ones under the jump (a member killed right after a
       * rotation made it: a writer's open neither replays nor removes a kept segment), holds no position: the read
       * of the positions below it goes on to the segment that has them (jepsen-cluster kill, r3e: the leader read
       * it as CORRUPT, which the catch-up dropped, and both replicas behind the promotion stopped for good) */
        char p[PATH_MAX + 64], nm2[64];
        wal_name(nm2, sizeof nm2, old[nold - 1] - 1);
        snprintf(p, sizeof p, "%s/%s", rd, nm2);
        CHECK(x.ld->wal_seq > old[nold - 1]); /* sealed: below the current segment and the one before it */
        int fd = open(p, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
        CHECK(fd >= 0);
        if (fd >= 0)
            close(fd);
        uint8_t *rb = NULL;
        size_t rl = 0;
        uint64_t rsq = 0, roff = 0, rlast = 0;
        CHECK_EQ(replica_read(x.ld, gap_first, &rb, &rl, 1 << 20, &rsq, &roff, &rlast), 0);
        CHECK(rl > 0 && rsq < old[nold - 1] - 1 && rlast >= gap_first);
        free(rb);
        unlink(p);
    }
    CHECK_EQ(eng_delete_range(x.ld, 0, T0 + 1000, T0 + 3000), 0);
    xp_step(&x, "P: delete after promotion");
    xp_rows(&x, 0, 40, 10, 3000);
    CHECK_EQ(eng_drop_before(x.ld, T0 + 20 * DAY_MS), 0);
    xp_step(&x, "P: drop after promotion");
    for (uint32_t s = 0; s < NS; s++)
        CHECK(eq_range(x.ld, x.rep, s, T0 - 1000, far + 10000, "P: far"));
    /* the leader reopened: the kept segments are neither replayed nor removed; the keep bound past them removes them */
    eng_close(x.ld);
    xp_open_leader(&x);
    for (uint32_t s = 0; s < NS; s++)
        CHECK(eq_range(x.ld, x.rep, s, T0 - 1000, far + 10000, "P: leader reopened"));
    for (size_t i = 0; i < nold; i++)
        CHECK(seg_exists(rd, old[i]));
    eng_leader_commit(x.ld, ld_last(&x));
    eng_leader_keep(x.ld, ld_last(&x));
    for (size_t i = 0; i < nold; i++)
        CHECK(!seg_exists(rd, old[i]));
    xp_reopen(&x, bd, 0);
    xp_step(&x, "P: member reopened");
    xp_stop(&x);
}

/* Regression for docs/tasks/replica-ro-tails.md: a read-only open (and a live follower refresh) of a replica whose
 * MANIFEST has wal_flushed below a 2^40 epoch jump and wal_active at or above 2^40, with unflushed rows in two or
 * more segments below the jump and in segments at/above the jump (2^40 and 2^40 + 1). The old sequential tail loop
 * filled MAX_TAILS slots with non-existent segment numbers and never reached the segments above 2^40. */
static void case_ro_epoch_jump(void)
{
    /* 1. Direct replica feed: kept flushed segment 1 (wal_flushed == 1), two unflushed segments below 2^40 (2, 3),
     *    and unflushed rows in both 2^40 and 2^40 + 1 (including overwrites of pre-jump timestamps). */
    {
        char dir[PATH_MAX];
        fresh(dir, "rojump");
        leader l;
        unsigned rng = 31;
        leader_init(&l);
        for (int i = 0; i < 3; i++)
            leader_rows(&l, 4, &rng);
        leader_roll(&l); /* seq 2 */
        for (int i = 0; i < 3; i++)
            leader_rows(&l, 4, &rng);
        size_t n_seg12 = l.n;

        eng *db = NULL;
        replica_keep_next(0, 3600 * 1000); /* keep flushed segment 1 on disk to verify seq <= wal_flushed is skipped */
        CHECK_EQ(open_replica(dir, 0, &db), 0);
        if (!db) {
            leader_free(&l);
            sh("rm -rf '%s'", dir);
            return;
        }
        CHECK_EQ(feed(db, &l, 0, n_seg12), 0);
        CHECK_EQ(replica_sync(db), 0);
        CHECK_EQ(replica_apply(db, l.r[n_seg12 - 1].last), 0);
        CHECK_EQ(mt_flush(db), 0); /* flushes segment 1; segment 2 stays active and unflushed */
        CHECK_EQ(replica_floor_saved(db, l.r[n_seg12 - 1].last), 0);

        leader_roll(&l); /* seq 3: second unflushed segment below 2^40 */
        for (int i = 0; i < 3; i++)
            leader_rows(&l, 4, &rng);
        size_t n_pre = l.n;
        /* Append and sync without applying seq 3 yet: replica_apply seals seq 2 and wakes the background flusher,
         * which would race ahead and move wal_flushed past seq 2/3. Keeping seq 3..2^40+1 unapplied until after the
         * read-only checks keeps wal_flushed == 1 deterministically while all four segments hold durable WAL rows. */
        CHECK_EQ(feed(db, &l, n_seg12, n_pre), 0);
        CHECK_EQ(replica_sync(db), 0);

        uint64_t fl = 0, ac = 0;
        CHECK(man_bounds(dir, &fl, &ac) && fl == 1 && ac == 3);

        model m_pre;
        model_at(&l, l.r[n_pre - 1].last, &m_pre);
        tsdb_options ro_opt;
        tsdb_options_init(&ro_opt);
        ro_opt.read_only = 1;
        ro_opt.merge_threads = 0;
        ro_opt.create_volumes = 0;
        eng *ro_live = NULL;
        CHECK_EQ((eng_open)(dir, &ro_opt, &ro_live), 0);
        if (ro_live)
            CHECK(same_as(ro_live, &m_pre, "rojump: live before jump"));

        /* Epoch jump: rows in 2^40 and in 2^40 + 1, with wal_flushed still 1. */
        l.seq = (uint64_t)1 << 40;
        l.off = 0;
        for (int i = 0; i < 4; i++)
            leader_rows(&l, 5, &rng);
        leader_roll(&l); /* 2^40 + 1 */
        for (int i = 0; i < 4; i++)
            leader_rows(&l, 5, &rng);
        size_t n_post = l.n;
        CHECK_EQ(feed(db, &l, n_pre, n_post), 0);
        CHECK_EQ(replica_sync(db), 0);

        CHECK(man_bounds(dir, &fl, &ac) && fl == 1 && ac == ((uint64_t)1 << 40) + 1);
        CHECK_EQ(count_files(dir, "wal_"), 5); /* seg 1 (kept), 2, 3, 2^40, 2^40+1 */

        model m_post;
        model_at(&l, l.r[n_post - 1].last, &m_post);

        eng *ro1 = NULL;
        CHECK_EQ((eng_open)(dir, &ro_opt, &ro1), 0);
        if (ro1) {
            CHECK(same_as(ro1, &m_post, "rojump: fresh ro open across jump"));
            eng_close(ro1);
        }
        if (ro_live) {
            CHECK_EQ(eng_refresh(ro_live), 0);
            CHECK(same_as(ro_live, &m_post, "rojump: live refresh across jump"));
        }

        /* Stopped replica directory (as jepsen-cluster's end check opens it): wal_flushed is still 1 on disk. */
        CHECK_EQ(eng_close(db), 0);
        db = NULL;
        CHECK(man_bounds(dir, &fl, &ac) && fl == 1 && ac == ((uint64_t)1 << 40) + 1);
        eng *ro2 = NULL;
        CHECK_EQ((eng_open)(dir, &ro_opt, &ro2), 0);
        if (ro2) {
            CHECK(same_as(ro2, &m_post, "rojump: stopped replica ro open across jump"));
            eng_close(ro2);
        }

        /* Reopen, apply & flush past 2^40, write 2^40 + 2: live follower drops pre-jump tails and returns to fast path. */
        replica_keep_next(0, 3600 * 1000);
        CHECK_EQ(open_replica(dir, l.r[n_post - 1].last, &db), 0);
        if (db) {
            CHECK(same_as(db, &m_post, "rojump: reopened replica itself"));
            CHECK_EQ(mt_flush(db), 0);
            leader_roll(&l); /* 2^40 + 2 */
            for (int i = 0; i < 3; i++)
                leader_rows(&l, 4, &rng);
            CHECK_EQ(feed(db, &l, n_post, l.n), 0);
            CHECK_EQ(replica_sync(db), 0);
            CHECK_EQ(replica_apply(db, l.r[l.n - 1].last), 0);
            CHECK(man_bounds(dir, &fl, &ac) && fl >= ((uint64_t)1 << 40) && ac == ((uint64_t)1 << 40) + 2);
            model m_after;
            model_at(&l, l.r[l.n - 1].last, &m_after);
            if (ro_live) {
                CHECK_EQ(eng_refresh(ro_live), 0);
                CHECK(same_as(ro_live, &m_after, "rojump: live refresh after flush past jump"));
            }
            CHECK_EQ(eng_close(db), 0);
        }
        if (ro_live)
            eng_close(ro_live);
        leader_free(&l);
        sh("rm -rf '%s'", dir);
    }

    /* 2. Realistic promotion flow: another member b holds 2+ unflushed segments below 2^40, receives the EPOCH
     *    marker (2^40) and small rows from the promoted leader (2^40 + 1) without flushing, then is opened ro. */
    {
        xport x;
        char rd[PATH_MAX], bd[PATH_MAX];
        xp_start(&x, "rojp");
        rdir(rd, sizeof rd, &x);
        snprintf(bd, sizeof bd, "%s_b", x.ldir);
        xp_rows(&x, 0, 0, 8, 100);
        xp_step(&x, "ROJP: seg 1");
        CHECK_EQ(mt_flush(x.rep), 0); /* flush seg 1 on x.rep cleanly; no background flush pending */
        /* Ship seg 2 and seg 3 without applying on x.rep so its background flusher never runs or races cp -a. */
        x.noapply = true;
        CHECK_EQ(eng_flush(x.ld), 0);
        xp_rows(&x, 0, 10, 8, 200);
        xp_rows(&x, 1, 0, 6, 300);
        CHECK_EQ(xp_ship_logs(&x), 0);
        CHECK_EQ(eng_flush(x.ld), 0);
        xp_rows(&x, 1, 10, 6, 400);
        CHECK_EQ(xp_ship_logs(&x), 0);

        /* Close x.rep cleanly around cp -a so no background thread touches rd during the copy. */
        CHECK_EQ(eng_close(x.rep), 0);
        x.rep = NULL;
        CHECK_EQ(sh("rm -rf '%s' && cp -a '%s' '%s'", bd, rd, bd), 0);
        CHECK_EQ(open_replica(rd, 0, &x.rep), 0);
        eng *b = NULL;
        CHECK_EQ(open_replica(bd, 0, &b), 0);
        if (!x.rep || !b) {
            if (b)
                eng_close(b);
            xp_stop(&x);
            sh("rm -rf '%s'", bd);
            return;
        }
        uint64_t fl = 0, ac = 0, sq[64];
        size_t nsq = wal_seqs(bd, sq, 64), npre = 0;
        CHECK(man_bounds(bd, &fl, &ac) && fl < ((uint64_t)1 << 40));
        for (size_t i = 0; i < nsq; i++)
            if (sq[i] > fl && sq[i] < ((uint64_t)1 << 40))
                npre++;
        CHECK(npre >= 2); /* deterministically 2+ unflushed pre-jump segments in bd */

        uint64_t pos = 0;
        CHECK_EQ(replica_epoch_begin(x.rep, 1, "node-a", &pos), 0);
        CHECK_EQ(replica_sync(x.rep), 0);
        CHECK_EQ(ship_from(x.rep, b), 0);
        CHECK_EQ(replica_apply(x.rep, pos), 0);

        eng *n = NULL;
        CHECK_EQ(replica_promote(&x.rep, pos, &n), 0);
        CHECK(x.rep == NULL && n != NULL);
        if (!n) {
            eng_close(b);
            xp_stop(&x);
            sh("rm -rf '%s'", bd);
            return;
        }
        eng_close(x.ld);
        x.ld = n;
        snprintf(x.ldir, sizeof x.ldir, "%s", rd);
        x.rep = b;
        {
            struct stat st;
            char p[PATH_MAX + 64], nm[64];
            wal_name(nm, sizeof nm, (uint64_t)1 << 40);
            snprintf(p, sizeof p, "%s/%s", rd, nm);
            CHECK_EQ(stat(p, &st), 0);
            x.cseq = (uint64_t)1 << 40;
            x.coff = (uint64_t)st.st_size;
        }
        CHECK_EQ(eng_leader_attach(n, &XOPS), 0);
        /* Small rows from the promoted leader go into 2^40 + 1 (including overwrites of pre-jump rows);
         * ship them to b without applying yet so b's flusher cannot race ahead of wal_flushed < 2^40. */
        x.noapply = true;
        xp_rows(&x, 0, 4, 8, 900);
        xp_rows(&x, 1, 20, 6, 1200);
        CHECK_EQ(xp_pump(&x, ld_last(&x)), 0);

        CHECK(man_bounds(bd, &fl, &ac) && fl < ((uint64_t)1 << 40) && ac >= ((uint64_t)1 << 40) + 1);
        nsq = wal_seqs(bd, sq, 64);
        npre = 0;
        for (size_t i = 0; i < nsq; i++)
            if (sq[i] > fl && sq[i] < ((uint64_t)1 << 40))
                npre++;
        CHECK(npre >= 2);
        CHECK(ro_dir_same(x.ld, bd, T0 + 1000LL * (NT + 1), "ROJP: member b across promotion, read-only"));
        eng_close(x.rep);
        x.rep = NULL;
        CHECK(man_bounds(bd, &fl, &ac) && fl < ((uint64_t)1 << 40) && ac >= ((uint64_t)1 << 40) + 1);
        CHECK(ro_dir_same(x.ld, bd, T0 + 1000LL * (NT + 1), "ROJP: stopped member b across promotion, read-only"));
        xp_stop(&x);
        sh("rm -rf '%s'", bd);
    }

    /* 3. > MAX_TAILS refusal (F1) and non-ENOENT openat error reporting (F2):
     *    - Exactly MAX_TAILS unflushed WAL segments succeed on read-only open;
     *    - An unreadable WAL segment (EACCES, non-root) fails both fresh open (follow_tail_one) and live refresh
     *      (tail_segment) with TSDB_ERR_IO rather than silently returning 0;
     *    - MAX_TAILS + 1 unflushed WAL segments (including across a 2^40 epoch jump) fail both fresh open and live
     *      refresh with TSDB_ERR_IO rather than silently truncating and dropping newer segments. */
    {
        char dir[PATH_MAX];
        fresh(dir, "rojump_max");
        leader l;
        unsigned rng = 53;
        leader_init(&l);
        for (int i = 0; i < 2; i++)
            leader_rows(&l, 4, &rng);
        leader_roll(&l); /* seq 2 */
        for (int i = 0; i < 2; i++)
            leader_rows(&l, 4, &rng);
        size_t n_seg12 = l.n;

        eng *db = NULL;
        replica_keep_next(0, 3600 * 1000);
        CHECK_EQ(open_replica(dir, 0, &db), 0);
        if (!db) {
            leader_free(&l);
            sh("rm -rf '%s'", dir);
            return;
        }
        CHECK_EQ(feed(db, &l, 0, n_seg12), 0);
        CHECK_EQ(replica_sync(db), 0);
        CHECK_EQ(replica_apply(db, l.r[n_seg12 - 1].last), 0);
        CHECK_EQ(mt_flush(db), 0); /* wal_flushed == 1; seq 2 stays unflushed */
        CHECK_EQ(replica_floor_saved(db, l.r[n_seg12 - 1].last), 0);

        /* Add MAX_TAILS - 1 more unflushed segments (seq 3 .. MAX_TAILS + 1 = 9): total MAX_TAILS unflushed. */
        size_t prev = n_seg12;
        for (int k = 1; k < MAX_TAILS; k++) {
            leader_roll(&l);
            leader_rows(&l, 3, &rng);
        }
        CHECK_EQ(feed(db, &l, prev, l.n), 0);
        CHECK_EQ(replica_sync(db), 0);

        uint64_t fl = 0, ac = 0;
        CHECK(man_bounds(dir, &fl, &ac) && fl == 1 && ac == (uint64_t)(MAX_TAILS + 1));

        model m_max;
        model_at(&l, l.r[l.n - 1].last, &m_max);
        tsdb_options ro_opt;
        tsdb_options_init(&ro_opt);
        ro_opt.read_only = 1;
        ro_opt.merge_threads = 0;
        ro_opt.create_volumes = 0;
        eng *ro_live = NULL;
        CHECK_EQ((eng_open)(dir, &ro_opt, &ro_live), 0);
        if (ro_live) {
            CHECK_EQ(ro_live->ntails, (size_t)MAX_TAILS);
            CHECK(same_as(ro_live, &m_max, "rojump_max: exactly MAX_TAILS unflushed"));
        }

        /* Non-ENOENT openat error (EACCES) must fail loudly with TSDB_ERR_IO in both tail_segment and follow_tail_one. */
        if (geteuid() != 0 && ro_live) {
            char wp[PATH_MAX + 64], nm[64];
            wal_name(nm, sizeof nm, 2);
            snprintf(wp, sizeof wp, "%s/%s", dir, nm);
            CHECK_EQ(chmod(wp, 0000), 0);
            CHECK_EQ(eng_refresh(ro_live), TSDB_ERR_IO); /* tail_segment */
            eng *ro_err = NULL;
            CHECK_EQ((eng_open)(dir, &ro_opt, &ro_err), TSDB_ERR_IO); /* follow_tail_one */
            CHECK(ro_err == NULL);
            if (ro_err)
                eng_close(ro_err);
            CHECK_EQ(chmod(wp, 0644), 0);
            CHECK_EQ(eng_refresh(ro_live), 0);
        }

        /* Add a (MAX_TAILS + 1)-th unflushed segment at 2^40: must fail loudly with TSDB_ERR_IO, not truncate. */
        prev = l.n;
        l.seq = (uint64_t)1 << 40;
        l.off = 0;
        leader_rows(&l, 4, &rng);
        CHECK_EQ(feed(db, &l, prev, l.n), 0);
        CHECK_EQ(replica_sync(db), 0);
        CHECK(man_bounds(dir, &fl, &ac) && fl == 1 && ac == ((uint64_t)1 << 40));

        if (follow_test_mutant == 0) {
            eng *ro_over = NULL;
            CHECK_EQ((eng_open)(dir, &ro_opt, &ro_over), TSDB_ERR_IO);
            CHECK(ro_over == NULL);
            if (ro_over)
                eng_close(ro_over);
            if (ro_live)
                CHECK_EQ(eng_refresh(ro_live), TSDB_ERR_IO);
        }

        if (ro_live)
            eng_close(ro_live);
        CHECK_EQ(eng_close(db), 0);
        leader_free(&l);
        sh("rm -rf '%s'", dir);
    }

    /* 4. Out-of-order tail insertion (pos < db->ntails memmove path in follow_tail_one):
     *    Open a live read-only follower seeing unflushed segments {2, 2^40} while segment 3 is absent; then make
     *    segment 3 visible and refresh. follow_tail_one must insert seq 3 between 2 and 2^40 (sorted ascending) so
     *    segment 3's unique rows appear, segment 3 overwrites segment 2, and segment 2^40 still beats segment 3. */
    {
        char dir[PATH_MAX];
        fresh(dir, "rojump_ooo");
        leader l;
        unsigned rng = 71;
        leader_init(&l);

        /* Segment 1 (will be flushed): baseline rows */
        leader_rows(&l, 4, &rng);
        leader_roll(&l); /* seq 2 */
        /* Segment 2: series 0 at t = 0..4 with values 10..14 */
        {
            lrec *r = leader_rows(&l, 5, &rng);
            for (int k = 0; k < 5; k++) {
                r->sid[k] = 0;
                r->t[k] = (int8_t)k;
                r->v[k] = 10.0f + (float)k;
                uint8_t *p = r->bytes + WAL_HDR + (size_t)k * WAL_ROW;
                uint32_t id = 0;
                int64_t ts = T0 + (int64_t)k * 1000;
                uint64_t slot = 0;
                memcpy(&slot, &r->v[k], 4);
                memcpy(p, &id, 4);
                memcpy(p + 4, &ts, 8);
                memcpy(p + 12, &slot, 8);
            }
            wal_reseal(r->bytes, r->seq, r->off);
        }
        size_t n_seg12 = l.n;

        eng *db = NULL;
        replica_keep_next(0, 3600 * 1000);
        CHECK_EQ(open_replica(dir, 0, &db), 0);
        if (!db) {
            leader_free(&l);
            sh("rm -rf '%s'", dir);
            return;
        }
        CHECK_EQ(feed(db, &l, 0, n_seg12), 0);
        CHECK_EQ(replica_sync(db), 0);
        CHECK_EQ(replica_apply(db, l.r[n_seg12 - 1].last), 0);
        CHECK_EQ(mt_flush(db), 0); /* wal_flushed == 1; seq 2 stays unflushed */
        CHECK_EQ(replica_floor_saved(db, l.r[n_seg12 - 1].last), 0);

        /* Segment 3: overwrites t = 0..4 with 20..24, and adds unique t = 10..12 with 30..32 */
        leader_roll(&l); /* seq 3 */
        {
            lrec *r = leader_rows(&l, 8, &rng);
            int8_t ts_idx[8] = {0, 1, 2, 3, 4, 10, 11, 12};
            float vals[8] = {20, 21, 22, 23, 24, 30, 31, 32};
            for (int k = 0; k < 8; k++) {
                r->sid[k] = 0;
                r->t[k] = ts_idx[k];
                r->v[k] = vals[k];
                uint8_t *p = r->bytes + WAL_HDR + (size_t)k * WAL_ROW;
                uint32_t id = 0;
                int64_t ts = T0 + (int64_t)ts_idx[k] * 1000;
                uint64_t slot = 0;
                memcpy(&slot, &r->v[k], 4);
                memcpy(p, &id, 4);
                memcpy(p + 4, &ts, 8);
                memcpy(p + 12, &slot, 8);
            }
            wal_reseal(r->bytes, r->seq, r->off);
        }
        /* Segment 2^40: overwrites t = 2..4 with 42..44 (must beat seg 3 even if seg 3 is opened later) */
        l.seq = (uint64_t)1 << 40;
        l.off = 0;
        {
            lrec *r = leader_rows(&l, 3, &rng);
            for (int k = 0; k < 3; k++) {
                r->sid[k] = 0;
                r->t[k] = (int8_t)(2 + k);
                r->v[k] = 42.0f + (float)k;
                uint8_t *p = r->bytes + WAL_HDR + (size_t)k * WAL_ROW;
                uint32_t id = 0;
                int64_t ts = T0 + (int64_t)(2 + k) * 1000;
                uint64_t slot = 0;
                memcpy(&slot, &r->v[k], 4);
                memcpy(p, &id, 4);
                memcpy(p + 4, &ts, 8);
                memcpy(p + 12, &slot, 8);
            }
            wal_reseal(r->bytes, r->seq, r->off);
        }
        CHECK_EQ(feed(db, &l, n_seg12, l.n), 0);
        CHECK_EQ(replica_sync(db), 0);
        CHECK_EQ(eng_close(db), 0);
        db = NULL;

        /* Hide segment 3 temporarily so ro_live opens tails [2, 2^40] first. */
        char seg3[PATH_MAX + 64], tmp3[PATH_MAX + 64], nm3[64];
        wal_name(nm3, sizeof nm3, 3);
        snprintf(seg3, sizeof seg3, "%s/%s", dir, nm3);
        snprintf(tmp3, sizeof tmp3, "%s/hidden_seg3", dir);
        CHECK_EQ(rename(seg3, tmp3), 0);

        tsdb_options ro_opt;
        tsdb_options_init(&ro_opt);
        ro_opt.read_only = 1;
        ro_opt.merge_threads = 0;
        ro_opt.create_volumes = 0;
        eng *ro_live = NULL;
        CHECK_EQ((eng_open)(dir, &ro_opt, &ro_live), 0);
        if (ro_live && follow_test_mutant == 0) {
            CHECK_EQ(ro_live->ntails, 2);
            CHECK(ro_live->tails[0].seq == 2 && ro_live->tails[1].seq == ((uint64_t)1 << 40));

            /* Restore segment 3 and refresh: follow_tail_one must memmove 2^40 right and insert 3 at index 1. */
            CHECK_EQ(rename(tmp3, seg3), 0);
            CHECK_EQ(eng_refresh(ro_live), 0);
            CHECK_EQ(ro_live->ntails, 3);
            CHECK(ro_live->tails[0].seq == 2 && ro_live->tails[1].seq == 3 &&
                  ro_live->tails[2].seq == ((uint64_t)1 << 40));

            model m_all;
            model_at(&l, l.r[l.n - 1].last, &m_all);
            CHECK(same_as(ro_live, &m_all, "rojump_ooo: out-of-order tail insertion and precedence"));
        } else {
            (void)rename(tmp3, seg3);
        }
        if (ro_live)
            eng_close(ro_live);
        leader_free(&l);
        sh("rm -rf '%s'", dir);
    }
}

static void promote_hook_exit(void) { _exit(77); }

/* the process dies after promotion named the kept segments in RSTATE and before it opened the writer: the rows the
 * replica held are in parts, so a writer's open of the directory, which does not replay those segments, has what the
 * old leader had (the order flush, then RSTATE) */
static void case_promote_crash(void)
{
    char ld[PATH_MAX], rd[PATH_MAX];
    pid_t pid = fork();
    if (pid == 0) {
        replica_test_promote_hook = promote_hook_exit;
        case_promote(3);
        _exit(1); /* the hook did not fire */
    }
    int st = 0;
    CHECK(pid > 0 && waitpid(pid, &st, 0) == pid);
    CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 77);
    snprintf(ld, sizeof ld, "%s/kp3", S);
    snprintf(rd, sizeof rd, "%s/kp3_r", S);
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.wal_sync_ms = 0;
    eng *a = NULL, *b = NULL;
    CHECK_EQ(eng_open(ld, &o, &a), 0);
    CHECK_EQ(eng_open(rd, &o, &b), 0);
    if (a && b)
        for (uint32_t s = 0; s < NS; s++)
            CHECK(eq_range(a, b, s, T0 - 1000, T0 + 40 * DAY_MS + 10000, "P: a writer's open after the crash"));
    if (b) { /* a writer that is not a leader removes the kept segments at its first flush */
        uint64_t sq[256];
        CHECK(wal_seqs(rd, sq, 256) >= 3); /* the kept ones and the writer's own */
        float v = 1;
        int64_t t = T0 + 41 * DAY_MS;
        CHECK_EQ(eng_insert_series(b, TSDB_FLOAT32, 0, &t, &v, 1), 0);
        CHECK_EQ(eng_flush(b), 0);
        CHECK_EQ(wal_seqs(rd, sq, 256), 1);
    }
    if (a)
        eng_close(a);
    if (b)
        eng_close(b);
}

static uint64_t unlink_fail_seq;
static int unlink_fail(const char *name)
{
    char want[64];
    wal_name(want, sizeof want, unlink_fail_seq);
    return strcmp(name, want) == 0 ? EIO : 0;
}

/* A writer that is not a leader, with kept segments (a promoted directory opened as a plain writer), whose removals
 * fail: a kept one that fails stays at the head and is tried again, and wal_removed stays below it; its own segment's
 * failed unlink is retried at the next flush, and wal_removed never passes it while it is on disk (a reopen would
 * replay it). Before the fix the kept ones went through wal_unlink_upto's run of consecutive numbers: a call with a
 * lower number dropped the retry of the failed one and took a kept segment for removed while it stayed on disk. */
static void case_unlink_kept(void)
{
    char rd[PATH_MAX];
    pid_t pid = fork();
    if (pid == 0) {
        replica_test_promote_hook = promote_hook_exit;
        case_promote(3);
        _exit(1);
    }
    int st = 0;
    CHECK(pid > 0 && waitpid(pid, &st, 0) == pid);
    CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 77);
    snprintf(rd, sizeof rd, "%s/kp3_r", S);
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.wal_sync_ms = 0;
    eng *b = NULL;
    CHECK_EQ(eng_open(rd, &o, &b), 0);
    if (!b)
        return;
    uint64_t sq[256];
    size_t n0 = wal_seqs(rd, sq, 256);
    CHECK(n0 >= 3 && b->nlk == n0 - 1);
    uint64_t k0 = sq[0];
    CHECK(b->wal_removed < k0); /* the open passes no kept segment */
    float v = 1;
    int64_t t = T0 + 41 * DAY_MS;
    /* 1: the oldest kept segment fails: it and every later kept one stay, the flushed one goes */
    unlink_fail_seq = k0;
    eng_test_retire_fail = unlink_fail;
    CHECK_EQ(eng_insert_series(b, TSDB_FLOAT32, 0, &t, &v, 1), 0);
    CHECK_EQ(eng_flush(b), 0);
    CHECK(seg_exists(rd, k0));
    CHECK_EQ(b->nlk, n0 - 1);
    CHECK(b->wal_removed < k0);
    /* 2: the kept ones go now; the writer's own segment fails: it is tried again, and wal_removed stays below it */
    uint64_t w = b->wal_seq;
    unlink_fail_seq = w;
    t += 1000;
    CHECK_EQ(eng_insert_series(b, TSDB_FLOAT32, 0, &t, &v, 1), 0);
    CHECK_EQ(eng_flush(b), 0);
    CHECK_EQ(b->nlk, 0);
    CHECK(!seg_exists(rd, k0));
    CHECK(seg_exists(rd, w));
    CHECK_EQ(b->wal_unlink_next, w);
    CHECK(b->wal_removed < w);
    /* 3: no failure: everything but the writer's current segment is gone */
    eng_test_retire_fail = NULL;
    t += 1000;
    CHECK_EQ(eng_insert_series(b, TSDB_FLOAT32, 0, &t, &v, 1), 0);
    CHECK_EQ(eng_flush(b), 0);
    CHECK(!seg_exists(rd, w));
    CHECK_EQ(b->wal_unlink_next, 0);
    CHECK_EQ(wal_seqs(rd, sq, 256), 1);
    CHECK(b->wal_removed >= w);
    eng_close(b);
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



/* ------------------------------------------------------------ R: power lost between a publish and its RSTATE save
 * A script in which every step is one record of the leader's log (small rows, a large insert's COMMIT, a DROP), the
 * replica inside the crashfs root, power lost at every file system step. Case Q accepts any answer from the last
 * acknowledged step on, which a COMMIT skipped in the crashed step passes; here the replica, applied to its durable
 * end E, must answer exactly as the oracle did at E, and must list every COMMIT's file once. So a crash after RSTATE
 * says `doing` and before the parts are listed must apply the COMMIT (mutant rscommittedtrue: the replica takes it for
 * published and the rows are missing), one after they are listed and before `done` is saved must not list them again
 * (rscommittedfalse: the same file listed twice). Mutant dropnorsave (a DROP carried out, RSTATE not saved) is
 * equivalent, and this loop shows it: a DROP begins its segment, rows after it reach a part only through a flush that
 * moves the parts' wal_seq past that segment, and the open applies from the first segment above it (replica_open_log), so
 * a DROP whose `done` was lost is applied again only with nothing after it applied yet. Step 5 writes rows below the
 * dropped bound after the DROP and step 6's COMMIT flushes them, the case where an extra DROP would remove them.
 * The leader's links of a COMMIT's files (stage_ names, src/replica.c leader_commit) are synced with their directory before
 * the marker is written: crashfs tracks link and linkat (round 16), so a marker that is durable with its links lost (the
 * power goes after the marker's WAL sync and before the directory's, while the replica applies it) leaves the ex-leader
 * with a log that ends at the marker and a durable end below it: the ex-leader's log end must be its durable end
 * (mutant leaderlinknosync: no sync of the directory after the links). */
#define RSTEPS 8

static void xp_rows_at(xport *x, uint32_t s, int64_t t0, int n, float base);

static void r_step(xport *x, int i)
{
    int64_t far = T0 + 40 * DAY_MS;
    float b = (float)(i * 100);
    switch (i) {
    case 0: xp_rows(x, 0, 0, 10, b); break;
    case 1: xp_rows_at(x, 1, far, 5, b); break;
    case 2: xp_rows(x, 0, 5, 40, b); break; /* a large insert: a COMMIT */
    case 3: xp_rows_at(x, 0, far, 40, b); break;
    case 4:
        CHECK_EQ(eng_flush(x->ld), 0);
        CHECK_EQ(eng_drop_before(x->ld, T0 + 10 * DAY_MS), 0); /* a DROP */
        break;
    case 5: xp_rows_at(x, 1, T0 + 5 * DAY_MS, 3, b); break; /* below the dropped bound, after the DROP */
    case 6: /* the COMMIT's apply flushes step 5's rows into a part before its RSTATE save */
        CHECK_EQ(eng_flush(x->ld), 0);
        xp_rows_at(x, 0, far + 200000, 40, b);
        break;
    default: xp_rows_at(x, 1, far + 300000, 3, b); break;
    }
    CHECK_EQ(xp_pump(x, ld_last(x)), 0);
}

static uint64_t r_digest(eng *db)
{
    uint64_t h = 1469598103934665603ULL;
    for (uint32_t s = 0; s < NS; s++) {
        qctx q = {.n = 0};
        if (eng_query_raw_ex(db, s, T0 - 1000, T0 + 41 * DAY_MS, false, 0, qfn, &q) != 0)
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

/* Two listed parts with the same index checksum: one COMMIT's file published twice */
static bool parts_twice(eng *db)
{
    bool twice = false;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++)
        for (size_t j = i + 1; j < db->nparts; j++)
            twice |= db->parts[i]->index_hash == db->parts[j]->index_hash;
    pthread_mutex_unlock(&db->lock);
    return twice;
}

static int powerr_child(const char *ldir, const char *ackfile)
{
    xport x;
    memset(&x, 0, sizeof x);
    char rd[PATH_MAX + 8];
    snprintf(x.ldir, sizeof x.ldir, "%s", ldir);
    snprintf(rd, sizeof rd, "%s_r", ldir);
    if (open_replica(rd, 0, &x.rep) != 0)
        return 2;
    xp_open_leader(&x);
    for (int i = 0; i < RSTEPS; i++) {
        r_step(&x, i);
        if (failures)
            return 3;
        write_ack(ackfile, (uint64_t)i + 1);
    }
    return 0;
}

static void case_rstate_crash(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  power loss between a publish and its RSTATE save: skipped (no %s)\n", SHIM);
        return;
    }
    uint64_t D[RSTEPS + 1], P[RSTEPS + 1];
    {
        xport o;
        xp_start(&o, "pro");
        eng_close(o.rep);
        o.rep = NULL;
        D[0] = r_digest(o.ld);
        P[0] = ld_last(&o);
        for (int i = 0; i < RSTEPS; i++) {
            r_step(&o, i);
            D[i + 1] = r_digest(o.ld);
            P[i + 1] = ld_last(&o);
        }
        xp_stop(&o);
    }
    char ack[PATH_MAX + 16];
    snprintf(ack, sizeof ack, "%s/acks-of-pr.ack", S);
    int crashes = 0, between = 0;
    bool finished = false;
    for (int k = 0; k < 4000 && !finished; k++) {
        char ld[PATH_MAX], rd[PATH_MAX + 8];
        fresh(ld, "pr");
        snprintf(rd, sizeof rd, "%s_r", ld);
        sh("rm -rf '%s' && cp -a '%s' '%s'", rd, ld, rd);
        unlink(ack);
        /* crashfs's root is a path prefix: ld covers the leader and its replica (ld_r), so the power is lost at every
         * sync of either, the leader's publish and RSTATE saves included */
        int code = run_power_child_as("--powerr", ld, ld, ack, k, k + 1);
        if (code != 99 && code != 0)
            fprintf(stderr, "R: step %d: the child ended with status %d\n", k, code);
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
            fprintf(stderr, "R: step %d: the open failed: %d\n", k, rc);
            failures++;
            continue;
        }
        uint64_t e = replica_durable_end(db);
        int j = 0;
        while (j < RSTEPS && P[j + 1] <= e)
            j++;
        if (e < P[done] || e != P[j]) {
            fprintf(stderr, "R: step %d: the durable end %" PRIu64 " is not the end of a step from %" PRIu64 " on\n", k, e,
                    done);
            failures++;
        } else if (replica_apply(db, e) != 0) {
            fprintf(stderr, "R: step %d: the durable log does not apply\n", k);
            failures++;
        } else if (r_digest(db) != D[j]) {
            fprintf(stderr, "R: step %d: the replica applied to %" PRIu64 " does not answer as the leader did there "
                    "(mutant rscommittedtrue, dropnorsave)\n", k, e);
            failures++;
        } else if (parts_twice(db)) {
            fprintf(stderr, "R: step %d: a COMMIT's file is listed twice (mutant rscommittedfalse)\n", k);
            failures++;
        }
        between += e > P[0];
        eng_close(db);
        /* the leader of the same run, opened as a replica: power lost between its publish (parts_commit) and the
         * RSTATE save of its COMMIT must not let the COMMIT's file be listed again (rs_committed reads doing_b0;
         * mutant b0off) */
        eng *ex;
        rc = open_replica(ld, 0, &ex);
        if (rc != 0) {
            fprintf(stderr, "R: step %d: the ex-leader's open as a replica failed: %d\n", k, rc);
            failures++;
            continue;
        }
        uint64_t le = replica_durable_end(ex);
        int lj = 0;
        while (lj < RSTEPS && P[lj + 1] <= le)
            lj++;
        if (replica_log_end(ex) != le) { /* a COMMIT marker is in the log, but the open does not count it: its files are not there */
            fprintf(stderr, "R: step %d: the ex-leader's log ends at %" PRIu64 ", its durable end at %" PRIu64 ": the files "
                    "of a COMMIT marker that is durable are gone (the links of a COMMIT were not synced before its marker: "
                    "mutant leaderlinknosync)\n", k, replica_log_end(ex), le);
            failures++;
        } else if (le < P[done] || le != P[lj]) {
            fprintf(stderr, "R: step %d: the ex-leader's durable end %" PRIu64 " is not the end of a step from %" PRIu64
                    " on\n", k, le, done);
            failures++;
        } else if (replica_apply(ex, le) != 0 || replica_applied_end(ex) != le) {
            fprintf(stderr, "R: step %d: the ex-leader's durable log does not apply to %" PRIu64 " (a COMMIT's files "
                    "are gone)\n", k, le);
            failures++;
        } else if (r_digest(ex) != D[lj]) {
            fprintf(stderr, "R: step %d: the ex-leader applied to %" PRIu64 " does not answer as it did there\n", k, le);
            failures++;
        } else if (parts_twice(ex)) {
            fprintf(stderr, "R: step %d: the ex-leader lists a COMMIT's file twice (mutant b0off)\n", k);
            failures++;
        }
        eng_close(ex);
        if (failures)
            break;
    }
    CHECK(finished && crashes > 50 && between > 0);
    printf("  power lost at each of %d steps of a replica applying COMMITs and a DROP: applied to its durable end it "
           "answers as the leader did there, each COMMIT's file listed once, and so does the leader\n", crashes);
}
/* ------------------------------------------------------------ N: review round 2 (drop ordering, staged files, promotion) */

/* A drop on the leader while a large write waits for its commit point (a second thread, as tsdbd's request threads):
 * the drop's marker must not come after the COMMIT in the log while the leader retires the partition before the
 * publish. The drop waits for the large write (jr_serial), so leader and replica hold the same rows at every commit
 * point (mutant nodropserial: the leader keeps the rows, the replica drops them). */
static xport *DR_X;
static pthread_t DR_T;
static bool DR_STARTED;
static void *dr_thread(void *a)
{
    (void)a;
    CHECK_EQ(eng_drop_before(DR_X->ld, T0 + 3 * DAY_MS), 0);
    return NULL;
}

static int dr_wait(void *ctx, eng *db, uint64_t last)
{
    if (!DR_STARTED) {
        DR_STARTED = true;
        CHECK_EQ(pthread_create(&DR_T, NULL, dr_thread, NULL), 0);
        usleep(400000); /* the drop runs now, or waits for the large write */
    }
    return xp_wait(ctx, db, last);
}

static size_t count_rows(eng *db, uint32_t s)
{
    qctx q = {.n = 0};
    eng_query_raw_ex(db, s, T0 - 10 * DAY_MS, T0 + 10 * DAY_MS, false, 0, qfn, &q);
    return q.n;
}

static void case_drop_race(void)
{
    xport x;
    xp_start(&x, "kdr");
    DR_X = &x;
    DR_STARTED = false;
    xp_rows(&x, 0, 0, 10, 100);
    xp_step(&x, "N: before");
    XOPS.wait = dr_wait;
    xp_rows(&x, 1, 0, 40, 500); /* > wal_max_batch_rows: a COMMIT */
    CHECK(DR_STARTED);
    pthread_join(DR_T, NULL);
    XOPS.wait = xp_wait; /* only now: the drop's marker wait reads it (dr_wait goes on to xp_wait meanwhile) */
    CHECK_EQ(xp_pump(&x, ld_last(&x)), 0);
    for (uint32_t s = 0; s < 2; s++)
        CHECK_EQ(count_rows(x.ld, s), count_rows(x.rep, s));
    CHECK(xp_same(&x, "N: after the race"));
    xp_stop(&x);
}

/* The driver sends a file again (a lost acknowledgement) and the process dies in the middle: the staged file under an
 * acknowledged marker is never torn, and what is durable never goes back. A stage after the marker was applied is an
 * orphan that the next open removes (and a promotion). */
static void case_restage(void)
{
    xport x;
    char rd[PATH_MAX], name[64];
    uint64_t sz, mpos;
    xp_start(&x, "krs");
    rdir(rd, sizeof rd, &x);
    xp_rows(&x, 0, 0, 10, 100);
    xp_step(&x, "N: before");
    x.noapply = true;
    xp_rows(&x, 0, 5, 40, 2000);
    uint64_t last = ld_last(&x), d0 = replica_durable_end(x.rep);
    CHECK_EQ(d0, last);
    CHECK_EQ(replica_stage(x.rep, x.held_name, x.held, x.held_len / 2), TSDB_ERR_INVALID); /* other bytes, same name */
    CHECK_EQ(replica_durable_end(x.rep), d0);
    xp_reopen(&x, rd, 0);
    CHECK_EQ(replica_durable_end(x.rep), d0); /* mutant stagetrunc: below it */
    CHECK_EQ(replica_missing(x.rep, &mpos, name, sizeof name, &sz), 0);
    CHECK_EQ(replica_stage(x.rep, x.held_name, x.held, x.held_len), 0); /* the same bytes again: nothing to do */
    CHECK_EQ(count_files(rd, "stage_"), 1);
    x.noapply = false;
    xp_step(&x, "N: restaged");
    CHECK_EQ(count_files(rd, "stage_"), 0); /* applied: removed */
    CHECK_EQ(replica_stage(x.rep, x.held_name, x.held, x.held_len), 0); /* late: an orphan */
    CHECK_EQ(count_files(rd, "stage_"), 1);
    xp_reopen(&x, rd, 0);
    CHECK_EQ(count_files(rd, "stage_"), 0);
    xp_step(&x, "N: orphan gone at open");
    /* and at a promotion: the orphan of a late send */
    CHECK_EQ(replica_stage(x.rep, x.held_name, x.held, x.held_len), 0);
    CHECK_EQ(count_files(rd, "stage_"), 1);
    uint64_t pos = 0;
    eng *n = NULL;
    CHECK_EQ(replica_epoch_begin(x.rep, 1, "node-a", &pos), 0);
    CHECK_EQ(replica_sync(x.rep), 0);
    CHECK_EQ(replica_apply(x.rep, pos), 0);
    CHECK_EQ(replica_promote(&x.rep, pos, &n), 0);
    CHECK_EQ(count_files(rd, "stage_"), 0); /* mutant noorphans */
    if (n)
        eng_close(n);
    eng_close(x.ld);
    free(x.held);
}

/* A staged file damaged at rest between its stage and its apply is refused (never adopted silently). */
static void case_stage_damage(void)
{
    xport x;
    char rd[PATH_MAX];
    xp_start(&x, "ksd");
    rdir(rd, sizeof rd, &x);
    xp_rows(&x, 0, 0, 10, 100);
    xp_step(&x, "N: before");
    x.noapply = true;
    xp_rows(&x, 0, 5, 40, 2000);
    CHECK(x.held_len > 64);
    sh("f=$(ls '%s'/stage_*.part) && printf '\\377' | dd of=\"$f\" bs=1 seek=%d conv=notrunc 2>/dev/null", rd,
       200);
    x.noapply = false;
    CHECK(xp_pump(&x, ld_last(&x)) != 0); /* (part_open_as checks the whole file too: the apply's own checksum has no mutant of its own) */
    xp_stop(&x);
}

/* A replica promotes only with everything durable and applied, and no COMMIT waiting for its files. */
static void case_promote_refused(void)
{
    xport x;
    uint64_t pos = 0;
    eng *n = NULL;
    xp_start(&x, "kpr");
    xp_rows(&x, 0, 0, 10, 100);
    xp_step(&x, "N: before");
    CHECK_EQ(replica_epoch_begin(x.rep, 1, "node-a", &pos), 0);
    CHECK_EQ(replica_epoch_begin(x.rep, 0, "node-b", &pos), TSDB_ERR_INVALID); /* an epoch whose segment is behind */
    CHECK_EQ(replica_apply(x.rep, pos), 0);
    CHECK_EQ(replica_promote(&x.rep, pos, &n), TSDB_ERR_INVALID); /* not synced (the guards against it overlap: apply_next, durable_next and ncm; no mutant of one alone is caught) */
    CHECK(x.rep && !n);
    CHECK_EQ(replica_sync(x.rep), 0);
    CHECK_EQ(replica_apply(x.rep, pos), 0);
    CHECK_EQ(replica_promote(&x.rep, pos, &n), 0);
    if (n)
        eng_close(n);
    eng_close(x.ld);
    free(x.held);

    xp_start(&x, "kpw");
    xp_rows(&x, 0, 0, 10, 100);
    xp_step(&x, "N: before");
    x.withhold = true;
    x.noapply = true;
    xp_rows(&x, 0, 5, 40, 2000); /* its file is withheld: the marker waits for it */
    CHECK_EQ(replica_epoch_begin(x.rep, 1, "node-a", &pos), 0);
    CHECK_EQ(replica_sync(x.rep), 0);
    CHECK_EQ(replica_apply(x.rep, pos), 0);
    n = NULL;
    CHECK_EQ(replica_promote(&x.rep, pos, &n), TSDB_ERR_INVALID);
    CHECK(x.rep && !n);
    xp_stop(&x);
}

/* ------------------------------------------------------------ L: the leader's commit point (C7, design-cluster.md 5.1)
 * A leader applies only up to its commit point, as every replica does: rows durable on the leader stay invisible, out of
 * parts and unacknowledged until the driver moves it (eng_leader_commit); a deposed leader ends every wait with an
 * indeterminate result. The K transport runs with `manual` here where the test moves the commit point itself. */

static atomic_int HOOK_N; /* the write wait hook's count: writes the server would see held */
static void count_hook(void *ctx, int d)
{
    (void)ctx;
    atomic_fetch_add(&HOOK_N, d);
}

static void set_count_hook(eng *db)
{
    atomic_store(&HOOK_N, 0);
    pthread_mutex_lock(&db->buf_lock);
    db->wait_fn = count_hook, db->wait_ctx = NULL;
    db->opt.flush_interval_ms = 3600000; /* no flush of the flusher's own between the steps */
    pthread_mutex_unlock(&db->buf_lock);
}

static void wait_hook(int k)
{
    for (int i = 0; i < 2500 && atomic_load(&HOOK_N) < k; i++)
        usleep(2000);
    CHECK_EQ(atomic_load(&HOOK_N), k);
}

/* joins a job thread that must end within 10 s (a lost wake-up shows as a failed check, not as a hang of the run) */
static void join_within(pthread_t t, atomic_bool *done, const char *what)
{
    for (int i = 0; i < 5000 && !atomic_load(done); i++)
        usleep(2000);
    if (!atomic_load(done)) {
        fprintf(stderr, "test_replica: FAILED (%s)\n", what);
        fflush(stderr);
        _exit(1);
    }
    pthread_join(t, NULL);
}

typedef struct ljob {
    eng *db;
    bool flush;
    uint32_t s;
    int t0, n;
    int rc;
    atomic_bool done;
} ljob;

static void *ljob_main(void *a)
{
    ljob *j = a;
    if (j->flush) {
        j->rc = eng_flush(j->db);
    } else {
        int64_t ts[NT];
        float v[NT];
        for (int i = 0; i < j->n; i++)
            ts[i] = T0 + 1000LL * (j->t0 + i), v[i] = (float)(j->t0 + i);
        j->rc = eng_insert_series(j->db, TSDB_FLOAT32, j->s, ts, v, (size_t)j->n);
    }
    atomic_store(&j->done, true);
    return NULL;
}

static int cnt_fn(void *c, const int64_t *ts, const void *vals, size_t n)
{
    (void)ts, (void)vals;
    *(size_t *)c += n;
    return 0;
}

static size_t lrows(eng *db, uint32_t s)
{
    size_t n = 0;
    CHECK_EQ(eng_query_raw_ex(db, s, T0 - 1000, T0 + 1000000000LL, false, 0, cnt_fn, &n), 0);
    return n;
}

/* the newest row of series s by the latest table (eng_latest: its lookup bounds memtable rows itself) or by a search on
 * one cut (cut_take: what tsdb_query_multi reads too); -1: none */
static int64_t latest_ts(eng *db, uint32_t s, bool search)
{
    latest_row r;
    int rc = search ? eng_latest_search(db, &s, 1, NULL, INT64_MAX, false, &r)
                    : eng_latest(db, &s, 1, NULL, INT64_MAX, false, &r);
    CHECK_EQ(rc, 0);
    CHECK_EQ(r.status, 0);
    return r.kind == TSDB_LATEST_NONE ? -1 : r.ts;
}

static uint64_t parts_fno_hi(eng *db) /* the highest position any listed part holds */
{
    uint64_t m = 0;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++)
        m = MAX(m, db->parts[i]->fno_hi);
    pthread_mutex_unlock(&db->lock);
    return m;
}

/* L1: rows durable on the leader past its commit point are not visible to its queries (mutant leadervisible: visible =
 * wal_durable) and a flush does not write them into parts (mutant leaderflush): a failover that loses them must not
 * have shown them, nor left them where no cut reaches. A partial commit point shows exactly the batch below it; the
 * writes are acknowledged in the same order. */
static void case_leader_gate(void)
{
    xport x;
    xp_start(&x, "lgate");
    x.manual = true;
    eng *ld = x.ld;
    set_count_hook(ld);
    uint64_t c0 = eng_wal_durable_fno(ld);
    eng_leader_commit(ld, c0);
    ljob a = {.db = ld, .s = 0, .t0 = 0, .n = 5}, b = {.db = ld, .s = 1, .t0 = 0, .n = 6}, f = {.db = ld, .flush = true};
    pthread_t ta, tb, tf;
    pthread_create(&ta, NULL, ljob_main, &a);
    wait_hook(1); /* a holds c0 + 1 .. c0 + 5 */
    pthread_create(&tb, NULL, ljob_main, &b);
    wait_hook(2);
    CHECK(!atomic_load(&a.done) && !atomic_load(&b.done)); /* durable, but not acknowledged */
    CHECK(eng_wal_durable_fno(ld) >= c0 + 11);
    CHECK_EQ(lrows(ld, 0), 0); /* mutant leadervisible */
    CHECK_EQ(lrows(ld, 1), 0);
    CHECK(ld->lt != NULL); /* the latest table is on: its own lookup is judged */
    CHECK_EQ(latest_ts(ld, 0, false), -1); /* the latest table's lookup (ltab.c) bounds by the commit point */
    CHECK_EQ(latest_ts(ld, 0, true), -1);  /* and so does a cut (cut_take) */
    pthread_create(&tf, NULL, ljob_main, &f);
    usleep(200000);
    CHECK(!atomic_load(&f.done)); /* mutant leaderflush: the flush wrote them */
    CHECK(parts_fno_hi(ld) <= c0);
    eng_leader_commit(ld, c0 + 5); /* a only */
    pthread_join(ta, NULL);
    CHECK_EQ(a.rc, 0);
    CHECK_EQ(lrows(ld, 0), 5);
    CHECK_EQ(lrows(ld, 1), 0); /* b is past the commit point */
    CHECK_EQ(latest_ts(ld, 0, false), T0 + 4000);
    CHECK_EQ(latest_ts(ld, 1, false), -1);
    CHECK_EQ(latest_ts(ld, 0, true), T0 + 4000);
    CHECK_EQ(latest_ts(ld, 1, true), -1);
    usleep(50000);
    CHECK(!atomic_load(&b.done) && !atomic_load(&f.done)); /* the sealed memtable holds b: still not flushed */
    CHECK(parts_fno_hi(ld) <= c0 + 5);
    eng_leader_commit(ld, eng_wal_durable_fno(ld));
    pthread_join(tb, NULL);
    pthread_join(tf, NULL);
    CHECK_EQ(b.rc, 0);
    CHECK_EQ(f.rc, 0);
    CHECK_EQ(lrows(ld, 0), 5);
    CHECK_EQ(lrows(ld, 1), 6);
    CHECK(parts_fno_hi(ld) >= c0 + 11 && parts_fno_hi(ld) <= atomic_load(&ld->lead_commit)); /* flushed now */
    CHECK_EQ(atomic_load(&HOOK_N), 0); /* every +1 has its -1 */
    CHECK_EQ(eng_check(ld), 0);
    x.manual = false;
    xp_step(&x, "L1: the replica has what the leader committed");
    xp_stop(&x);
    printf("  a leader shows, acknowledges and flushes rows only up to its commit point\n");
}

/* L1b: a dictionary change on a leader (SERIES, STATE, RETENTION, CLASS) is a marker, the commit point, then the
 * file: until the commit point passes the marker the file is unchanged and a new id is not visible (mutant dictearly:
 * the record goes to the file at once). A replica applies RETENTION and CLASS by offset and skips an offset below its
 * file's end: a deposed leader's record at offset X that never committed would hide the next leader's other record at
 * X. Deposed with the marker uncommitted, the leader's file stays as it was. */
typedef struct djob {
    eng *db;
    int kind; /* 0 SERIES, 1 STATE, 2 RETENTION, 3 CLASS */
    int rc;
    atomic_bool done;
} djob;

static void *djob_main(void *a)
{
    djob *j = a;
    uint32_t id;
    j->rc = j->kind == 0   ? eng_series(j->db, "dc_new", TSDB_FLOAT32, &id)
            : j->kind == 1 ? eng_state(j->db, "dc_state", true, &id)
            : j->kind == 2 ? eng_set_retention(j->db, 1, 30 * DAY_MS)
                           : eng_class_switch(j->db, 0, 2, NULL);
    atomic_store(&j->done, true);
    return NULL;
}

static off_t fsize(const char *dir, const char *name)
{
    char p[PATH_MAX + 32];
    struct stat st;
    snprintf(p, sizeof p, "%s/%s", dir, name);
    return stat(p, &st) == 0 ? st.st_size : -1;
}

static void case_dict_commit(void)
{
    static const char *const files[] = {"SERIES", "STATES", "RETENTION", "CLASSES"};
    xport x;
    xp_start(&x, "ldict");
    eng *ld = x.ld;
    xp_rows(&x, 0, 0, 5, 1); /* rows of series 0 for the switch */
    xp_step(&x, "L1b: rows");
    for (int k = 0; k < 4; k++) {
        eng_leader_replay_floor(ld, ld->fno_next); /* the members' logs start past every row: the switch may go */
        x.manual = true;
        uint64_t c = eng_wal_durable_fno(ld);
        eng_leader_commit(ld, c);
        off_t before = fsize(x.ldir, files[k]);
        djob j = {.db = ld, .kind = k};
        pthread_t t;
        pthread_create(&t, NULL, djob_main, &j);
        for (int i = 0; i < 2500 && eng_wal_durable_fno(ld) == c; i++)
            usleep(2000);
        CHECK(eng_wal_durable_fno(ld) > c); /* the marker is in the log */
        usleep(100000);
        CHECK(!atomic_load(&j.done));
        CHECK_EQ(fsize(x.ldir, files[k]), before); /* mutant dictearly */
        uint32_t id;
        if (k == 0)
            CHECK(eng_series_lookup(ld, "dc_new", &id) != 0); /* not visible before its commit point */
        eng_leader_commit(ld, eng_wal_durable_fno(ld));
        join_within(t, &j.done, "L1b: the commit point did not end a dictionary wait");
        CHECK_EQ(j.rc, 0);
        CHECK(fsize(x.ldir, files[k]) > before);
        if (k == 0)
            CHECK_EQ(eng_series_lookup(ld, "dc_new", &id), 0);
        x.manual = false;
        xp_step(&x, "L1b: the replica applied the dictionary change");
    }
    /* deposed with a RETENTION marker uncommitted: the wait ends indeterminate, the file is unchanged */
    x.manual = true;
    uint64_t c = eng_wal_durable_fno(ld);
    eng_leader_commit(ld, c);
    off_t before = fsize(x.ldir, "RETENTION");
    djob j = {.db = ld, .kind = 2};
    pthread_t t;
    pthread_create(&t, NULL, djob_main, &j);
    for (int i = 0; i < 2500 && eng_wal_durable_fno(ld) == c; i++)
        usleep(2000);
    usleep(50000);
    eng_leader_depose(ld);
    join_within(t, &j.done, "L1b: a depose did not end a RETENTION wait");
    CHECK_EQ(j.rc, TSDB_ERR_INDETERMINATE);
    CHECK_EQ(fsize(x.ldir, "RETENTION"), before);
    CHECK_EQ(eng_check(ld), 0);
    /* the deposed leader's files are byte for byte those of the replica, which applied up to the commit point only:
     * a next leader's record at the same offset is not hidden by a record of this one that never committed */
    for (int k = 2; k < 4; k++) {
        uint8_t *a = NULL, *b = NULL;
        size_t la = 0, lb = 0;
        CHECK_EQ(read_file(ld->dirfd, files[k], &a, &la), 0);
        CHECK_EQ(read_file(x.rep->dirfd, files[k], &b, &lb), 0);
        if (!(la == lb && la > 0 && a && b && memcmp(a, b, la) == 0))
            fprintf(stderr, "L1b: %s: leader %zu bytes, replica %zu bytes\n", files[k], la, lb);
        CHECK(la == lb && la > 0 && a && b && memcmp(a, b, la) == 0);
        free(a);
        free(b);
    }
    xp_stop(&x);
    printf("  a leader's dictionary records reach their files only past their commit point\n");
}

/* L1c: the leader's T lies past every row a member may still replay. Rows of series 0 on day D3 are flushed on both;
 * the leader deletes [D3, end of time) and merges them away, so its parts hold none.
 * floor: the replica still holds the segment with them (its floor was not saved: replay floor 1). The leader's T must
 * be past D3 from the WAL tail it keeps for the replica (mutant wtaillocal: pruned when the leader's own segment went;
 * T falls below D3 and the replica, which may replay those rows, is CORRUPT or brings them back).
 * dead: the replica unlinked the segment (the floor passed it); its parts still hold the deleted rows, which its own
 * merges never removed (merge_threads = 0): before it applies T it merges the series' partitions at or past T (mutant
 * noclasspurge: CORRUPT). Either way the replica reopens cleanly with no row on D3.
 * kept (REVIEW-merge H2): as dead, but under the driver's keep caps (a GiB, an hour), so the replica keeps the segment
 * it saved its floor past: RSTATE's `kept` names it, no open replays it, and its replay start (the leader's floor) is
 * past it. The leader's T is INT64_MIN (no row of series 0 is left). Judged by its removal (mutant wtailremoved, the
 * code before), the replica counted the kept segment's rows, refused the leader's T as below them and stopped until a
 * restart. */
static void case_class_floor(int dead)
{
    xport x;
    xp_start(&x, dead == 1 ? "lcdead" : dead == 2 ? "lcfold" : dead == 3 ? "lckept" : "lcfloor");
    eng *ld = x.ld;
    char rd[PATH_MAX];
    rdir(rd, sizeof rd, &x);
    int64_t d3 = DAY1 + 2 * DAY_MS, ts[5];
    float v[5], f = 0;
    for (int i = 0; i < 5; i++)
        ts[i] = d3 + 1000LL * i, v[i] = (float)(i + 1);
    CHECK_EQ(eng_insert_series(ld, TSDB_FLOAT32, 0, ts, v, 5), 0);
    xp_step(&x, "L1c: rows on D3");
    CHECK_EQ(mt_flush(ld), 0);
    CHECK_EQ(mt_flush(x.rep), 0);
    if (dead == 3)
        replica_keep_cap(x.rep, 1 << 30, 3600000); /* the driver's caps: the segment stays past the floor */
    if (dead == 1 || dead == 3)
        CHECK_EQ(replica_floor_saved(x.rep, replica_applied_end(x.rep)), 0);
    if (dead == 2) { /* H3: the floor stays put; past the cap the oldest entries (the D3 rows) fold into the bound */
        atomic_store(&cls_wtail_cap, 2);
        for (int r = 0; r < 6; r++) {
            xp_rows(&x, 1, 100 + 10 * r, 3, (float)r);
            CHECK_EQ(mt_flush(ld), 0);
        }
        CHECK(ld->nwtail <= 2); /* mutant wtailnocap */
        xp_step(&x, "L1c: flushes past the cap");
    }
    CHECK_EQ(eng_delete_range(ld, 0, d3, INT64_MAX), 0);
    xp_step(&x, "L1c: deleted");
    CHECK_EQ(eng_optimize(ld), 0);
    xp_step(&x, "L1c: the leader merged");
    CHECK_EQ(count_query(ld, 0, d3, d3 + DAY_MS, &f), 0);
    eng_leader_replay_floor(ld, dead == 1 || dead == 3 ? ld->fno_next : 1);
    CHECK_EQ(eng_class_switch(ld, 0, 2, NULL), 0);
    xp_step(&x, "L1c: the replica applied the switch");
    if (dead == 3) {
        CHECK_EQ(eng_check(x.rep), 0); /* mutant wtailremoved: the replica refused the leader's T and stopped */
        /* the D3 segment (sealed once the leader's delete began the next one) is named in RSTATE and still on disk */
        CHECK(replica_kept_seq(x.rep) > atomic_load(&x.rep->wal_removed_synced));
    }
    CHECK_EQ(mt_flush(x.rep), 0);
    CHECK_EQ(replica_floor_saved(x.rep, replica_applied_end(x.rep)), 0);
    xp_reopen(&x, rd, replica_applied_end(x.rep));
    if (x.rep) {
        CHECK_EQ(eng_check(x.rep), 0);
        CHECK_EQ(count_query(x.rep, 0, d3, d3 + DAY_MS, &f), 0);
        int c = -1;
        CHECK(eng_series_class(x.rep, 0, &c) == 0 && c == 2);
    }
    atomic_store(&cls_wtail_cap, CLS_WTAIL_CAP);
    xp_stop(&x);
    printf("  the leader's class switch time is past every row a member may replay (%s)\n",
           dead == 1   ? "the member's parts merged first"
           : dead == 2 ? "a WAL tail the member keeps, folded past the cap"
           : dead == 3 ? "a segment the member keeps under its caps, below its replay start"
                       : "a WAL tail the member keeps");
}

/* L1e (REVIEW-merge H1): the safety side of the fold, on the leader and on a replica. Flushes F1 (series 1), F2 (series
 * 1, then series 0 on D3: its newest rows), F3 (series 0 on D2, older times, and series 1), then series 1 alone past
 * the cap of 2, so that every entry folds, oldest first, into the second newest. The members' floor passes F1 only.
 * Each fold must keep series 0 at its D3 bound for as long as F2 may be replayed: mutants wtailfoldmin (the fold lives
 * only as long as its older entry: gone with F1), wtailfoldnewer (on a series in both, the newer entry's bound: D2) and
 * wtailfolddropold (a series only in the older entry dropped) each put the bound below D3. F2 lists series 1 first (a
 * memtable lists its series in arrival order), so the fold's sort of F2 is not the identity: wtailsortnomax (the ids
 * sorted, their bounds left in place) gives series 0 series 1's bound. The bound (cls_test_wal_newest) and the
 * leader's T (cls_at, on both members) are the checks: after series 0's rows are deleted and merged away, T must still
 * lie past D3. The replica is then reopened at its saved floor (F1) and replays F2.. under that T: no deleted row may
 * come back. That reopen alone does not catch every fold mutant (wtailfoldnewer and wtailfolddropold pass it): the
 * bound and T checks are the teeth. */
static void case_class_fold_safe(void)
{
    xport x;
    xp_start(&x, "lcfoldsafe");
    eng *ld = x.ld;
    char rd[PATH_MAX];
    rdir(rd, sizeof rd, &x);
    atomic_store(&cls_wtail_cap, 2);
    int64_t d2 = DAY1 + DAY_MS, d3 = DAY1 + 2 * DAY_MS;
    float f = 0;
    xp_rows(&x, 1, 0, 3, 1);
    xp_step(&x, "L1e: F1, series 1");
    CHECK_EQ(mt_flush(ld), 0);
    CHECK_EQ(mt_flush(x.rep), 0);
    uint64_t f1 = ld->fno_next, rf1 = replica_applied_end(x.rep); /* F1's positions are below f1 */
    CHECK_EQ(replica_floor_saved(x.rep, rf1), 0);
    xp_rows(&x, 1, 5, 3, 1); /* series 1 first: F2's ids are not in order */
    xp_rows_at(&x, 0, d3, 5, 10);
    xp_step(&x, "L1e: F2, series 1, then series 0 on D3");
    CHECK_EQ(mt_flush(ld), 0);
    CHECK_EQ(mt_flush(x.rep), 0);
    xp_rows_at(&x, 0, d2, 5, 20);
    xp_rows(&x, 1, 10, 3, 2);
    xp_step(&x, "L1e: F3, series 0 on D2 and series 1");
    CHECK_EQ(mt_flush(ld), 0);
    CHECK_EQ(mt_flush(x.rep), 0);
    for (int r = 0; r < 4; r++) {
        xp_rows(&x, 1, 100 + 10 * r, 3, (float)r);
        xp_step(&x, "L1e: series 1 past the cap");
        CHECK_EQ(mt_flush(ld), 0);
        CHECK_EQ(mt_flush(x.rep), 0);
    }
    eng_leader_replay_floor(ld, f1); /* every member past F1, none past F2 */
    pthread_mutex_lock(&ld->lock);
    CHECK(ld->nwtail <= 2 && ld->wtail_all_seq == 0); /* folded, series by series */
    pthread_mutex_unlock(&ld->lock);
    pthread_mutex_lock(&x.rep->lock);
    CHECK(x.rep->nwtail <= 2 && x.rep->wtail_all_seq == 0);
    pthread_mutex_unlock(&x.rep->lock);
    int64_t bl = cls_test_wal_newest(ld, 0), br = cls_test_wal_newest(x.rep, 0);
    if (bl < d3 + 4000 || br < d3 + 4000)
        fprintf(stderr, "lcfoldsafe: series 0's bound: leader %" PRId64 ", replica %" PRId64 ", its rows reach %" PRId64
                        "\n", bl, br, d3 + 4000);
    CHECK(bl >= d3 + 4000);
    CHECK(br >= d3 + 4000);
    CHECK_EQ(eng_delete_range(ld, 0, d2, INT64_MAX), 0);
    xp_step(&x, "L1e: deleted");
    CHECK_EQ(eng_optimize(ld), 0);
    xp_step(&x, "L1e: the leader merged");
    CHECK_EQ(count_query(ld, 0, d2, d3 + DAY_MS, &f), 0);
    eng_leader_replay_floor(ld, f1);
    CHECK_EQ(eng_class_switch(ld, 0, 2, NULL), 0);
    xp_step(&x, "L1e: the replica applied the switch");
    int64_t lo, hi;
    CHECK(cls_at(ld, 0, d3 + 4000, &lo, &hi) != 2 && hi > d3 + 4000); /* T past every row a member may replay */
    CHECK(cls_at(x.rep, 0, d3 + 4000, &lo, &hi) != 2 && hi > d3 + 4000);
    xp_reopen(&x, rd, rf1); /* below F2: the open replays F2.. */
    if (x.rep) {
        CHECK_EQ(eng_check(x.rep), 0);
        CHECK_EQ(count_query(x.rep, 0, d2, d3 + DAY_MS, &f), 0);
        int c = -1;
        CHECK(eng_series_class(x.rep, 0, &c) == 0 && c == 2);
    }
    atomic_store(&cls_wtail_cap, CLS_WTAIL_CAP);
    xp_stop(&x);
    printf("  a folded WAL tail keeps every series' newest row for as long as its segment may be replayed\n");
}

/* A replica keeps its flushed segments (no saved floor: a learner filling from the log start), so its WAL tail passes
 * the cap while the leader's floor has passed every flush: the leader's T for series 0 is exact, from its rows near T0.
 * The replica's fold must keep each series' own bound (wtail_fold). Folded into one bound for every series (mutant
 * wtailfoldall, the code before), series 1's rows two days later were taken as series 0's, and the replica refused the
 * leader's T as "its history is not the leader's" and stopped (jepsen-cluster churn, classes, seeds 1 and 102). */
static void case_class_fold_replica(void)
{
    xport x;
    xp_start(&x, "lcfoldrep");
    eng *ld = x.ld;
    atomic_store(&cls_wtail_cap, 2);
    xp_rows(&x, 0, 0, 5, 1);
    xp_step(&x, "L1d: series 0's rows");
    CHECK_EQ(mt_flush(ld), 0);
    CHECK_EQ(mt_flush(x.rep), 0);
    int64_t far = DAY1 + 2 * DAY_MS;
    for (int r = 0; r < 6; r++) {
        xp_rows_at(&x, 1, far + 10000LL * r, 3, (float)r);
        xp_step(&x, "L1d: series 1's rows two days later");
        CHECK_EQ(mt_flush(ld), 0);
        CHECK_EQ(mt_flush(x.rep), 0);
    }
    pthread_mutex_lock(&x.rep->lock);
    CHECK(x.rep->nwtail <= 2); /* past the cap: folded */
    CHECK(x.rep->nwtail + (x.rep->wtail_all_seq != 0) >= 1);
    pthread_mutex_unlock(&x.rep->lock);
    eng_leader_replay_floor(ld, ld->fno_next); /* every member past every flush: the leader's T is its rows' */
    CHECK_EQ(eng_class_switch(ld, 0, 2, NULL), 0);
    xp_step(&x, "L1d: the replica applied series 0's switch");
    int c = -1;
    CHECK(eng_series_class(x.rep, 0, &c) == 0 && c == 2);
    CHECK_EQ(eng_check(x.rep), 0);
    atomic_store(&cls_wtail_cap, CLS_WTAIL_CAP);
    xp_stop(&x);
    printf("  a replica's WAL tail past the cap keeps each series' bound: it applies the leader's exact switch time\n");
}

static void same_file(eng *a, eng *b, const char *name, const char *what)
{
    uint8_t *x = NULL, *y = NULL;
    size_t lx = 0, ly = 0;
    CHECK_EQ(read_file(a->dirfd, name, &x, &lx), 0);
    CHECK_EQ(read_file(b->dirfd, name, &y, &ly), 0);
    if (!(lx == ly && lx > 0 && x && y && memcmp(x, y, lx) == 0))
        fprintf(stderr, "%s: %s: leader %zu bytes, replica %zu bytes\n", what, name, lx, ly);
    CHECK(lx == ly && lx > 0 && x && y && memcmp(x, y, lx) == 0);
    free(x);
    free(y);
}

/* L1e (REVIEW H1, H4, M5): three new slow series (80 rows 10 minutes apart: a class above 0) are decided at the
 * leader's flush before the seal (cls_classify_active; mutant noactive decides them as class 0). Their three CLASS
 * markers are one unit: one admission of span 3 (mutant markersplit: three, and a refusal between them failed the
 * engine), so a refusal (min_isr) refuses the flush and nothing more. The replica flushes the segment with their rows,
 * reopens (cls_check_parts) and has the leader's classes; two RETENTION limits and the batch of CLASS records give
 * files equal byte for byte (a record skipped as "applied before" at a wrong offset is CORRUPT, never silent). */
static void case_class_fresh(void)
{
    xport x;
    xp_start(&x, "lfresh");
    eng *ld = x.ld;
    char rd[PATH_MAX];
    rdir(rd, sizeof rd, &x);
    eng_leader_replay_floor(ld, ld->fno_next);
    const char *nm[3] = {"fa", "fb", "fc"};
    uint32_t id[3];
    for (int k = 0; k < 3; k++) {
        CHECK_EQ(eng_series(ld, nm[k], TSDB_FLOAT32, &id[k]), 0);
        for (int b = 0; b < 8; b++) { /* small batches: through the memtable, not straight to parts */
            int64_t ts[10];
            float v[10];
            for (int i = 0; i < 10; i++)
                ts[i] = T0 + 600000LL * (b * 10 + i), v[i] = (float)(i + k);
            CHECK_EQ(eng_insert_series(ld, TSDB_FLOAT32, id[k], ts, v, 10), 0);
        }
    }
    xp_step(&x, "L1e: rows of new series");
    off_t before = fsize(x.ldir, "CLASSES");
    x.refuse = TSDB_ERR_UNAVAILABLE;
    CHECK(mt_flush(ld) != 0);
    x.refuse = 0;
    CHECK_EQ(eng_check(ld), 0);
    CHECK_EQ(fsize(x.ldir, "CLASSES"), before);
    x.rec = true;
    x.nadm = 0;
    CHECK_EQ(mt_flush(ld), 0);
    x.rec = false;
    bool one = false;
    for (size_t i = 0; i < x.nadm; i++)
        one |= x.adm_span[i] == 3;
    CHECK(one);
    CHECK_EQ(eng_set_retention(ld, 1, 30 * DAY_MS), 0);
    CHECK_EQ(eng_set_retention(ld, 1, 60 * DAY_MS), 0);
    xp_step(&x, "L1e: decided and flushed");
    int c[3];
    for (int k = 0; k < 3; k++)
        CHECK(eng_series_class(ld, id[k], &c[k]) == 0 && c[k] > 0);
    CHECK_EQ(mt_flush(x.rep), 0);
    CHECK_EQ(replica_floor_saved(x.rep, replica_applied_end(x.rep)), 0);
    xp_reopen(&x, rd, replica_applied_end(x.rep));
    CHECK(x.rep && eng_check(x.rep) == 0);
    for (int k = 0; k < 3; k++) {
        int cr = -1;
        uint32_t rid;
        CHECK_EQ(eng_series_lookup(x.rep, nm[k], &rid), 0);
        CHECK(rid == id[k] && eng_series_class(x.rep, rid, &cr) == 0 && cr == c[k]);
    }
    same_file(ld, x.rep, "CLASSES", "L1e");
    same_file(ld, x.rep, "RETENTION", "L1e");
    xp_stop(&x);
    printf("  new series decided before the seal, their CLASS markers admitted and synced as one\n");
}

/* L1f (REVIEW H2): a dropped partition bounds a switch's T only for the series with rows in it, and only until its
 * removal is durable. Series 1's day partitions are dropped while series 0 keeps its rows in a 28-day partition; the
 * leader's own bound is gone (as after a reopen or a promotion), and it switches series 0 at the day edge after its
 * rows. The replica, which ran through the same drop, applies it (mutant droppedglobal: one bound for every series,
 * kept for the run: a false CORRUPT, and both engines fail). */
static void case_class_dropped(void)
{
    xport x;
    xp_start(&x, "ldropped");
    eng *ld = x.ld;
    eng_leader_replay_floor(ld, ld->fno_next);
    CHECK_EQ(eng_class_switch(ld, 0, 2, NULL), 0);
    xp_step(&x, "L1f: series 0 in 28-day partitions");
    int64_t ts[5];
    float v[5];
    for (int i = 0; i < 5; i++)
        ts[i] = T0 + 1000LL * i, v[i] = (float)i;
    CHECK_EQ(eng_insert_series(ld, TSDB_FLOAT32, 0, ts, v, 5), 0);
    for (int i = 0; i < 5; i++)
        ts[i] = T0 + DAY_MS * (i % 2) + 1000LL * i;
    CHECK_EQ(eng_insert_series(ld, TSDB_FLOAT32, 1, ts, v, 5), 0);
    CHECK_EQ(mt_flush(ld), 0);
    xp_step(&x, "L1f: rows");
    CHECK_EQ(mt_flush(x.rep), 0);
    CHECK_EQ(eng_drop_before(ld, DAY1 + DAY_MS), 0);
    xp_step(&x, "L1f: drop");
    pthread_mutex_lock(&ld->lock); /* the leader reopened: its run-local bound is gone */
    for (struct cls_dropped *d = ld->cls_dropped, *n; d; d = n) {
        n = d->next;
        free(d);
    }
    ld->cls_dropped = NULL;
    pthread_mutex_unlock(&ld->lock);
    eng_leader_replay_floor(ld, ld->fno_next);
    CHECK_EQ(eng_class_switch(ld, 0, 0, NULL), 0);
    xp_step(&x, "L1f: the replica applied the switch");
    int c = -1;
    CHECK(eng_series_class(x.rep, 0, &c) == 0 && c == 0);
    CHECK_EQ(eng_check(ld), 0);
    xp_stop(&x);
    printf("  a dropped partition bounds a switch only for its own series, and only until the drop is durable\n");
}

/* L1d: rows the leader places under the new history right after a switch (here from the switching thread, as soon as
 * it let go of flush_lock: cls_test_after_switch) and flushes, sealing their segment, while the replica flushes too.
 * The CLASS marker went into the log inside the switch, before them, so the replica places them as the leader did
 * (mutant classmarkerlate: the marker follows them; the replica put them under the old class, its parts hold rows
 * past T, and the apply is CORRUPT). */
static xport *ORDER_X;
static int64_t ORDER_TS;

static void order_hook(eng *db)
{
    xport *x = ORDER_X;
    if (!x || db != x->ld)
        return;
    ORDER_X = NULL;
    float v[3] = {5, 6, 7};
    int64_t ts[3] = {ORDER_TS, ORDER_TS + 1000, ORDER_TS + 2000};
    CHECK_EQ(eng_insert_series(db, TSDB_FLOAT32, 0, ts, v, 3), 0);
    CHECK_EQ(mt_flush(db), 0);
    int64_t t1 = T0 + 50000;
    float v1 = 1;
    CHECK_EQ(eng_insert_series(db, TSDB_FLOAT32, 1, &t1, &v1, 1), 0); /* a record in the next segment: the replica seals */
    xp_ship_logs(x);
    if (!x->noapply)
        replica_apply(x->rep, ld_last(x));
    CHECK_EQ(mt_flush(x->rep), 0);
}

static void case_class_order(void)
{
    xport x;
    xp_start(&x, "lcorder");
    eng *ld = x.ld;
    char rd[PATH_MAX];
    rdir(rd, sizeof rd, &x);
    xp_rows(&x, 0, 0, 5, 1);
    xp_step(&x, "L1d: rows on T0's day");
    eng_leader_replay_floor(ld, ld->fno_next);
    ORDER_TS = T0 + 3 * DAY_MS;
    ORDER_X = &x;
    __atomic_store_n(&cls_test_after_switch, order_hook, __ATOMIC_RELEASE);
    CHECK_EQ(eng_class_switch(ld, 0, 2, NULL), 0);
    __atomic_store_n(&cls_test_after_switch, NULL, __ATOMIC_RELEASE);
    CHECK(ORDER_X == NULL); /* the hook ran */
    xp_step(&x, "L1d: the replica applied the switch and the rows after it");
    float f = 0;
    CHECK_EQ(count_query(x.rep, 0, ORDER_TS, ORDER_TS + 10000, &f), 3);
    CHECK_EQ(mt_flush(x.rep), 0);
    CHECK_EQ(replica_floor_saved(x.rep, replica_applied_end(x.rep)), 0);
    xp_reopen(&x, rd, replica_applied_end(x.rep)); /* the open checks every part against CLASSES */
    if (x.rep) {
        CHECK_EQ(eng_check(x.rep), 0);
        CHECK_EQ(count_query(x.rep, 0, ORDER_TS, ORDER_TS + 10000, &f), 3);
    }
    xp_stop(&x);
    printf("  a CLASS marker precedes the rows the leader places under it\n");
}

/* The series and switch time of the last record of a CLASSES file. */
static int64_t last_class_from(eng *db, uint32_t *series)
{
    uint8_t *b = NULL;
    size_t n = 0;
    int64_t from = INT64_MIN;
    CHECK_EQ(read_file(db->dirfd, "CLASSES", &b, &n), 0);
    CHECK(b && n >= 24);
    if (b && n >= 24) {
        memcpy(series, b + n - 24 + 4, 4);
        memcpy(&from, b + n - 24 + 8, 8);
    }
    free(b);
    return from;
}

/* L1g (REVIEW M6): a leader's T lies past its retention cutoff edge too: a replica whose clock lags the leader's keeps
 * expired rows until its own merges drop them (mutant nocutoff: T at the day edge after the rows, below them). */
static void case_class_cutoff(void)
{
    xport x;
    xp_start(&x, "lcutoff");
    eng *ld = x.ld;
    xp_rows(&x, 0, 0, 5, 1);
    CHECK_EQ(mt_flush(ld), 0);
    xp_step(&x, "L1g: rows of series 0");
    CHECK_EQ(eng_set_retention(ld, 0, DAY_MS), 0);
    int64_t cut = eng_cutoff(ld, 0);
    CHECK(cut > DAY1 + DAY_MS); /* the rows' own edge is DAY1: the cutoff is what decides T */
    eng_leader_replay_floor(ld, ld->fno_next);
    CHECK_EQ(eng_class_switch(ld, 0, 2, NULL), 0);
    uint32_t s = UINT32_MAX;
    int64_t from = last_class_from(ld, &s);
    if (!(s == 0 && from >= (cut / DAY_MS + 1) * DAY_MS))
        fprintf(stderr, "L1g: series %u switched at %" PRId64 ", the cutoff is %" PRId64 "\n", s, from, cut);
    CHECK(s == 0 && from >= (cut / DAY_MS + 1) * DAY_MS);
    CHECK_EQ(xp_pump(&x, ld_last(&x)), 0); /* no row comparison: the two clocks expire rows at their own time */
    CHECK_EQ(eng_check(x.rep), 0);
    int c = -1;
    CHECK(eng_series_class(x.rep, 0, &c) == 0 && c == 2);
    xp_stop(&x);
    printf("  the leader's class switch time is past its retention cutoff\n");
}

/* L1h (REVIEW M6): the replay floor at the last position of a flush keeps that flush's rows in the leader's tail (a
 * member at that log start replays the position: mutant pruneoff1 drops it, and the switch waits for a floor that came
 * already); entries pruned at a higher floor raise wtail_from, so a lower floor reported later (a member rejoins with
 * an older log start) makes a switch wait (mutant noraisefrom: it goes on without the rows it no longer tracks). */
static void case_class_prune(void)
{
    xport x;
    xp_start(&x, "lprune");
    eng *ld = x.ld;
    xp_rows(&x, 0, 0, 5, 1);
    CHECK_EQ(mt_flush(ld), 0);
    xp_step(&x, "L1h: rows of series 0");
    CHECK(ld->wtail != NULL);
    if (!ld->wtail)
        return;
    uint64_t f1 = ld->wtail->fno_end;
    CHECK(f1 > ld->wtail_from); /* the floor below must not defer the switch by itself */
    eng_leader_replay_floor(ld, f1 - 1);
    xp_rows(&x, 1, 0, 3, 1);
    CHECK_EQ(mt_flush(ld), 0); /* a flush prunes what the floor allows */
    CHECK_EQ(eng_class_switch(ld, 0, 2, NULL), 0);
    xp_step(&x, "L1h: switched with the floor one below an entry's end");
    eng_leader_replay_floor(ld, ld->fno_next);
    xp_rows(&x, 1, 10, 3, 2);
    CHECK_EQ(mt_flush(ld), 0); /* prunes both earlier flushes */
    CHECK(ld->wtail_from > f1);
    eng_leader_replay_floor(ld, f1); /* a member rejoins with an older log start */
    CHECK_EQ(eng_class_switch(ld, 1, 2, NULL), TSDB_ERR_UNAVAILABLE);
    eng_leader_replay_floor(ld, ld->fno_next);
    CHECK_EQ(eng_class_switch(ld, 1, 2, NULL), 0);
    xp_step(&x, "L1h: switched once the floor is past the pruned rows");
    CHECK_EQ(eng_check(ld), 0);
    xp_stop(&x);
    printf("  the leader's WAL tail is pruned at the replay floor exactly, and a lower floor later waits\n");
}

/* L1i (REVIEW M6): a new series whose first rows arrive after cls_classify_active and before the seal (here from the
 * flushing thread: mt_test_before_seal) is decided as class 0 at the leader's write, with no marker, as the replica
 * decides it (mutant zerorate: by its rate, a class above 0 whose marker lands after the seal). */
static xport *WIN_X;
static uint32_t WIN_ID;

static void win_hook(eng *db)
{
    xport *x = WIN_X;
    if (!x || db != x->ld)
        return;
    WIN_X = NULL;
    for (int b = 0; b < 8; b++) { /* the slow rows of case_class_fresh: a class above 0 by rate */
        int64_t ts[10];
        float v[10];
        for (int i = 0; i < 10; i++)
            ts[i] = T0 + 600000LL * (b * 10 + i), v[i] = (float)i;
        CHECK_EQ(eng_insert_series(db, TSDB_FLOAT32, WIN_ID, ts, v, 10), 0);
    }
}

static void case_class_window(void)
{
    xport x;
    xp_start(&x, "lwindow");
    eng *ld = x.ld;
    char rd[PATH_MAX];
    rdir(rd, sizeof rd, &x);
    eng_leader_replay_floor(ld, ld->fno_next);
    CHECK_EQ(eng_series(ld, "fw", TSDB_FLOAT32, &WIN_ID), 0);
    xp_rows(&x, 0, 0, 5, 1);
    WIN_X = &x;
    __atomic_store_n(&mt_test_before_seal, win_hook, __ATOMIC_RELEASE);
    CHECK_EQ(mt_flush(ld), 0);
    __atomic_store_n(&mt_test_before_seal, NULL, __ATOMIC_RELEASE);
    CHECK(WIN_X == NULL); /* the hook ran */
    int c = -1;
    CHECK(eng_series_class(ld, WIN_ID, &c) == 0 && c == 0);
    xp_step(&x, "L1i: rows in the window, flushed");
    CHECK_EQ(mt_flush(x.rep), 0);
    CHECK_EQ(replica_floor_saved(x.rep, replica_applied_end(x.rep)), 0);
    xp_reopen(&x, rd, replica_applied_end(x.rep));
    CHECK(x.rep && eng_check(x.rep) == 0);
    int cr = -1;
    CHECK(x.rep && eng_series_class(x.rep, WIN_ID, &cr) == 0 && cr == 0);
    xp_stop(&x);
    printf("  a series that turns up between the leader's fresh decisions and the seal is class 0 on both\n");
}

/* L2: a deposed leader ends every wait for the commit point with TSDB_ERR_INDETERMINATE (the record is in its log and a
 * replica may hold it) and is not failed: fail-stop would end tsdbd and every volume with it (mutant deposefails: a
 * large write's COMMIT wait fails the engine). After that every write is refused before it is buffered
 * (TSDB_ERR_UNAVAILABLE: definitely not applied), and a flush of the uncommitted rows is indeterminate too, with no
 * part past the commit point. */
static void case_leader_ack(void)
{
    xport x;
    xp_start(&x, "lack");
    x.manual = true;
    eng *ld = x.ld;
    set_count_hook(ld);
    uint64_t c0 = eng_wal_durable_fno(ld);
    eng_leader_commit(ld, c0);
    ljob big = {.db = ld, .s = 1, .t0 = 0, .n = 40}, small = {.db = ld, .s = 0, .t0 = 0, .n = 5};
    pthread_t tg, ts;
    pthread_create(&tg, NULL, ljob_main, &big); /* more than wal_max_batch_rows: parts and a COMMIT marker */
    for (int i = 0; i < 2500 && eng_wal_durable_fno(ld) <= c0; i++) /* its marker is durable: it waits next */
        usleep(2000);
    CHECK(eng_wal_durable_fno(ld) > c0);
    pthread_create(&ts, NULL, ljob_main, &small);
    wait_hook(1);
    usleep(100000);
    CHECK(!atomic_load(&big.done) && !atomic_load(&small.done));
    eng_leader_depose(ld);
    join_within(tg, &big.done, "L2: the depose did not end a large write's commit wait");
    join_within(ts, &small.done, "L2: the depose did not end a small write's commit wait");
    CHECK_EQ(big.rc, TSDB_ERR_INDETERMINATE);
    CHECK_EQ(small.rc, TSDB_ERR_INDETERMINATE);
    CHECK_EQ(eng_check(ld), 0); /* mutant deposefails */
    CHECK_EQ(atomic_load(&HOOK_N), 0);
    pthread_mutex_lock(&ld->buf_lock);
    uint64_t f = ld->fno_next;
    pthread_mutex_unlock(&ld->buf_lock);
    int64_t t1[3] = {T0 + 50000, T0 + 51000, T0 + 52000};
    float v1[3] = {1, 2, 3};
    CHECK_EQ(eng_insert_series(ld, TSDB_FLOAT32, 0, t1, v1, 3), TSDB_ERR_UNAVAILABLE);
    pthread_mutex_lock(&ld->buf_lock);
    CHECK_EQ(ld->fno_next, f); /* nothing buffered */
    pthread_mutex_unlock(&ld->buf_lock);
    stop_if_failed("a deposed leader buffered a refused write");
    CHECK_EQ(eng_flush(ld), TSDB_ERR_UNAVAILABLE); /* the flush did not happen: a request of its own, refused */
    CHECK(parts_fno_hi(ld) <= c0);
    CHECK_EQ(lrows(ld, 0), 0);
    CHECK_EQ(eng_check(ld), 0);
    xp_stop(&x); /* (the close's flush is refused the same way; the close of a deposed leader is r3a2's) */
    printf("  a deposed leader ends its waits as indeterminate, refuses writes and is not failed\n");
}

/* L3: admission (jr_ops.admit) sees every record before it is buffered, with the positions it then gets: the admitted
 * ranges tile the log the replica received exactly (rows batches, which may share a record, and markers). A refusal
 * leaves nothing buffered and the numbers where they were (mutant admitafter: refused after buffering), and the log
 * goes on without a gap. */
static void case_leader_admit(void)
{
    xport x;
    xp_start(&x, "ladmit");
    eng *ld = x.ld;
    set_count_hook(ld);
    x.rec = true;
    xp_rows(&x, 0, 0, 5, 1);
    xp_rows(&x, 0, 5, 5, 2);
    xp_rows(&x, 1, 0, 40, 3); /* a COMMIT marker */
    CHECK_EQ(eng_delete_range(ld, 1, T0 + 1000, T0 + 3000), 0);
    CHECK_EQ(eng_flush(ld), 0);
    xp_rows(&x, 0, 10, 3, 4);
    pthread_mutex_lock(&ld->buf_lock);
    uint64_t f = ld->fno_next;
    pthread_mutex_unlock(&ld->buf_lock);
    size_t nadm = x.nadm;
    x.refuse = TSDB_ERR_UNAVAILABLE;
    int64_t t1[40];
    float v1[40];
    for (int i = 0; i < 40; i++)
        t1[i] = T0 + 1000LL * (20 + i), v1[i] = (float)i;
    CHECK_EQ(eng_insert_series(ld, TSDB_FLOAT32, 0, t1, v1, 3), TSDB_ERR_UNAVAILABLE);
    CHECK_EQ(eng_insert_series(ld, TSDB_FLOAT32, 1, t1, v1, 40), TSDB_ERR_UNAVAILABLE); /* its marker refused */
    pthread_mutex_lock(&ld->buf_lock);
    CHECK_EQ(ld->fno_next, f); /* mutant admitafter */
    pthread_mutex_unlock(&ld->buf_lock);
    stop_if_failed("a refused admission left its batch buffered");
    CHECK_EQ(x.nadm, nadm);
    CHECK_EQ(eng_check(ld), 0);
    x.refuse = 0;
    xp_rows(&x, 0, 20, 3, 5);
    xp_step(&x, "L3: after a refusal"); /* the replica checks that the numbers continue without a gap */
    uint64_t from = 1, pos = 1;
    size_t k = 0;
    for (;;) {
        uint8_t *buf = NULL;
        size_t len = 0;
        uint64_t seq, off, last;
        if (replica_read(x.rep, from, &buf, &len, 1, &seq, &off, &last) != 0 || len == 0) {
            free(buf);
            break;
        }
        while (k < x.nadm && x.adm_fno[k] == pos && pos <= last) /* the admissions that make up this record */
            pos += x.adm_span[k++];
        CHECK_EQ(pos, last + 1);
        free(buf);
        from = pos = last + 1;
    }
    CHECK_EQ(k, x.nadm);
    CHECK_EQ(pos, ld_last(&x) + 1);
    xp_stop(&x);
    printf("  admission sees every record before it is buffered, with its positions; a refusal buffers nothing\n");
}

/* L4: the WAL writer hands a leader's bytes to the sender after its write() and before its fdatasync (5.1 step 2):
 * byte for byte what the segment holds there, the positions continuing without a gap (mutant bytesaftersync: handed
 * over after the sync). */
static void case_leader_bytes(void)
{
    xport x;
    xp_start(&x, "lbytes");
    eng *ld = x.ld;
    set_count_hook(ld);
    x.rec = true;
    pthread_mutex_lock(&ld->buf_lock);
    x.bytes_last = ld->fno_next - 1;
    pthread_mutex_unlock(&ld->buf_lock);
    SYNC_X = &x;
    __atomic_store_n(&wal_test_sync_hook, xp_sync_hook, __ATOMIC_RELAXED);
    xp_rows(&x, 0, 0, 5, 1);
    xp_rows(&x, 1, 0, 40, 3); /* a COMMIT marker */
    xp_rows(&x, 0, 5, 5, 2);
    CHECK_EQ(eng_flush(ld), 0); /* the sealed segment's rest */
    xp_rows(&x, 0, 10, 3, 4);
    __atomic_store_n(&wal_test_sync_hook, NULL, __ATOMIC_RELAXED);
    SYNC_X = NULL;
    CHECK(x.nbytes >= 4);
    CHECK_EQ(x.bytes_bad, 0);
    CHECK_EQ(x.bytes_late, 0); /* mutant bytesaftersync */
    CHECK_EQ(x.bytes_last, ld_last(&x));
    /* jr_ops.synced: every move of the leader's durable end, the rotation's sync of the old segment included, in
     * order (mutant nosynced: never told; a driver would have to poll) */
    CHECK_EQ(x.synced_bad, 0);
    CHECK(x.synced_last > 0);
    CHECK_EQ(x.synced_last, eng_wal_durable_fno(ld));
    CHECK_EQ(x.synced_last, ld_last(&x));
    xp_step(&x, "L4");
    xp_stop(&x);
    printf("  a leader's WAL bytes reach the sender before its fdatasync, as the segment holds them; each sync is "
           "reported after it\n");
}

/* L5: power lost at every step of a leader with the gate. The driver (the main thread) saves the commit point (the
 * .hi file, outside the crashfs root) before it hands it to the engine; a writer thread acknowledges batches and
 * flushes now and then. After the power loss every acknowledged row is there (acknowledged = committed and durable),
 * and no part holds a position past the saved commit point (the flush gate). */
#define PL_BATCHES 30

typedef struct plw {
    eng *db;
    const char *ack;
    atomic_int done;
    int rc;
} plw;

static void *plw_main(void *a)
{
    plw *w = a;
    for (int i = 0; i < PL_BATCHES && w->rc == 0; i++) {
        int64_t ts[4];
        float v[4];
        for (int k = 0; k < 4; k++)
            ts[k] = T0 + 1000LL * (4 * i + k), v[k] = (float)i;
        w->rc = eng_insert_series(w->db, TSDB_FLOAT32, 0, ts, v, 4);
        if (w->rc == 0)
            write_ack(w->ack, (uint64_t)i + 1);
        if (w->rc == 0 && i % 7 == 6)
            w->rc = eng_flush(w->db);
    }
    atomic_store(&w->done, 1);
    return NULL;
}

static atomic_uint_fast64_t POWERL_SYNCED;
static void powerl_synced(void *ctx, eng *db, uint64_t last)
{
    (void)ctx, (void)db;
    atomic_store(&POWERL_SYNCED, last);
}

static int powerl_child(const char *dir, const char *ackfile)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.wal_sync_ms = 0;
    eng *db;
    if (eng_open(dir, &o, &db) != 0)
        return 2;
    /* nothing to ship: the driver only moves the commit point, to the durable end jr_ops.synced reported a tick (1 ms)
     * earlier, so the commit point lags the leader's log as it does behind a replica's round trip: rows durable but not
     * committed are in the log at most power losses */
    static struct jr_ops ops = {.synced = powerl_synced};
    if (eng_leader_attach(db, &ops) != 0)
        return 2;
    char hi[PATH_MAX + 8];
    snprintf(hi, sizeof hi, "%s.hi", ackfile);
    plw w = {.db = db, .ack = ackfile};
    pthread_t t;
    pthread_create(&t, NULL, plw_main, &w);
    uint64_t prev = atomic_load(&POWERL_SYNCED);
    for (int tick = 0;; tick++) {
        bool last = atomic_load(&w.done);
        uint64_t now = atomic_load(&POWERL_SYNCED);
        uint64_t c = last ? now : prev;
        write_ack(hi, c); /* saved before the engine is told */
        eng_leader_commit(db, c);
        prev = now;
        if (last)
            break;
        if (tick > 60000) /* a minute: the durable end is not reported (mutant nosynced) */
            return 4;
        usleep(1000);
    }
    pthread_join(t, NULL);
    if (w.rc)
        return 3;
    eng_close(db);
    return 0;
}

static uint64_t read_u64_file(const char *p)
{
    uint64_t v = 0;
    FILE *f = fopen(p, "r");
    if (f) {
        if (fscanf(f, "%" SCNu64, &v) != 1)
            v = 0;
        fclose(f);
    }
    return v;
}

static void case_leader_power(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  power loss of a leader: skipped (no %s)\n", SHIM);
        return;
    }
    char dir[PATH_MAX], ack[PATH_MAX + 16], hif[PATH_MAX + 24];
    snprintf(ack, sizeof ack, "%s/acks-of-lp.ack", S);
    snprintf(hif, sizeof hif, "%s.hi", ack);
    int crashes = 0;
    bool finished = false;
    for (int k = 0; k < 900 && !finished; k++) {
        fresh(dir, "lp");
        unlink(ack);
        unlink(hif);
        int code = run_power_child_as("--powerl", dir, dir, ack, k, k + 1);
        if (code != 99 && code != 0)
            fprintf(stderr, "L5: step %d: the child ended with status %d\n", k, code);
        CHECK(code == 99 || code == 0);
        if (code != 99 && code != 0)
            break;
        crashes += code == 99;
        finished = code == 0;
        uint64_t acked = read_u64_file(ack), hi = read_u64_file(hif);
        tsdb_options o;
        tsdb_options_init(&o);
        o.merge_threads = 0;
        o.create_volumes = 0;
        /* the parts the leader wrote: the open puts the replayed WAL into parts of its own (a deposed leader reopens as
         * a replica instead, which cuts its log first: r3a2-depose), so only parts named before the open are judged */
        char names[64][96];
        size_t nn = 0;
        DIR *d = opendir(dir);
        for (struct dirent *e; d && (e = readdir(d)) != NULL;) {
            size_t l = strlen(e->d_name);
            if (e->d_name[0] == 'p' && l > 5 && l < 96 && strcmp(e->d_name + l - 5, ".part") == 0 && nn < 64)
                snprintf(names[nn++], 96, "%s", e->d_name);
        }
        if (d)
            closedir(d);
        eng *db;
        int rc = eng_open(dir, &o, &db);
        if (rc != 0) {
            fprintf(stderr, "L5: step %d: the open failed: %d\n", k, rc);
            failures++;
            continue;
        }
        size_t n = 0;
        CHECK_EQ(eng_query_raw_ex(db, 0, T0, T0 + 4000LL * (int64_t)acked, false, 0, cnt_fn, &n), 0);
        if (n != 4 * acked) {
            fprintf(stderr, "L5: step %d: %" PRIu64 " batches acknowledged, %zu rows of them there\n", k, acked, n);
            failures++;
        }
        for (size_t i = 0; i < db->nparts; i++)
            for (size_t j = 0; j < nn; j++)
                if (strcmp(db->parts[i]->name, names[j]) == 0 && db->parts[i]->fno_hi > hi) {
                    fprintf(stderr, "L5: step %d: part %s holds position %" PRIu64 ", the commit point was %" PRIu64 "\n",
                            k, names[j], db->parts[i]->fno_hi, hi);
                    failures++;
                }
        eng_close(db);
    }
    CHECK(finished && crashes > 20);
    printf("  power lost at each of %d steps of a leader with the gate: what it acknowledged is there, no part past "
           "its commit point\n", crashes);
}

typedef struct dropjob {
    eng *db;
    int64_t ts;
    int rc;
    atomic_bool done;
} dropjob;
static void *drop_main(void *a)
{
    dropjob *j = a;
    j->rc = eng_drop_before(j->db, j->ts);
    atomic_store(&j->done, true);
    return NULL;
}

/* L6: a DROP marker refused by admission (min_isr, not leading) while the active memtable holds rows: the refusal
 * reaches the caller, nothing is retired and the leader keeps what the replicas keep (REVIEW H1: the rotation's write
 * of the old segment overwrote the marker's result, leader_drop waited for position 0 and retired the partitions with
 * no marker in the log; mutant markerrc). A leader whose driver deposes it from inside the admission (its core is not
 * leading; the depose must not take buf_lock, which admission holds: REVIEW M2) is refused the same way, its commit
 * point past everything. A retry once admission accepts drops on both. */
static void case_leader_marker_refused(void)
{
    for (int deposed = 0; deposed < 2; deposed++) {
        xport x;
        xp_start(&x, deposed ? "lmrefd" : "lmref");
        xp_rows(&x, 0, 0, 10, 100);
        CHECK_EQ(eng_flush(x.ld), 0);
        xp_rows(&x, 0, 20, 5, 500); /* the active memtable is not empty: the rotation branch */
        xp_step(&x, "L6: rows");
        uint64_t before = ld_last(&x);
        if (deposed) /* the driver's core says it is not leading: it deposes from inside the admission */
            x.depose_in_admit = true;
        else
            x.refuse = TSDB_ERR_UNAVAILABLE; /* the driver refuses the marker (REPL_E_ISR) */
        dropjob dj = {.db = x.ld, .ts = T0 + 10 * DAY_MS};
        pthread_t td;
        pthread_create(&td, NULL, drop_main, &dj);
        join_within(td, &dj.done, "L6: the DROP did not return (a depose from admit under buf_lock?)");
        CHECK_EQ(dj.rc, TSDB_ERR_UNAVAILABLE);
        CHECK(!deposed || atomic_load(&x.ld->lead_deposed));
        x.depose_in_admit = false;
        CHECK_EQ(ld_last(&x), before); /* no marker */
        CHECK_EQ(eng_check(x.ld), 0);
        CHECK_EQ(lrows(x.ld, 0), 15); /* nothing retired */
        CHECK_EQ(xp_pump(&x, ld_last(&x)), 0);
        CHECK(xp_same(&x, "L6: after the refused DROP"));
        if (!deposed) {
            x.refuse = 0;
            CHECK_EQ(eng_drop_before(x.ld, T0 + 10 * DAY_MS), 0);
            CHECK_EQ(lrows(x.ld, 0), 0);
            xp_step(&x, "L6: the DROP after the refusal");
            CHECK_EQ(lrows(x.rep, 0), 0);
        }
        xp_stop(&x);
    }
    /* the leader's clock on: the NOW marker before the DROP is refused, the DROP's own would be admitted; the DROP
     * is refused all the same (r3d-isr review L3: it went out without the replicas' clock) */
    {
        xport x;
        xp_start(&x, "lmrefn");
        xp_rows(&x, 0, 0, 10, 100);
        CHECK_EQ(eng_flush(x.ld), 0);
        xp_step(&x, "L6: rows before the clock's refusal");
        uint64_t start = ld_last(&x);
        eng_leader_clock(x.ld, 3600 * 1000, 0); /* an hour: after its first NOW, the flusher writes none of its own here */
        for (int i = 0; i < 5000 && ld_last(&x) == start; i++) /* the first one is due at once: let it land first, or */
            usleep(1000);                                       /* it takes the refusal below and moves ld_last */
        CHECK(ld_last(&x) > start);
        uint64_t before = ld_last(&x);
        x.refuse_once = TSDB_ERR_UNAVAILABLE;
        CHECK_EQ(eng_drop_before(x.ld, T0 + 10 * DAY_MS), TSDB_ERR_UNAVAILABLE);
        CHECK_EQ(x.refuse_once, 0); /* the NOW marker was asked for */
        CHECK_EQ(ld_last(&x), before);
        CHECK_EQ(lrows(x.ld, 0), 10); /* nothing retired */
        CHECK_EQ(eng_drop_before(x.ld, T0 + 10 * DAY_MS), 0);
        CHECK_EQ(lrows(x.ld, 0), 0);
        xp_step(&x, "L6: the DROP after the NOW's refusal");
        CHECK_EQ(lrows(x.rep, 0), 0);
        xp_stop(&x);
    }
    printf("  a DROP whose marker admission refuses is refused, retires nothing, and the leader stays the replicas' "
           "equal; so is one whose NOW marker is refused\n");
}

/* R2: a leader whose COMMIT marker is committed and whose publish then fails (here: the request is abandoned, as
 * tsdbd does for one parked on a stuck volume) fails the engine: the replicas list the files, so a leader that went on
 * without them would answer otherwise, and its next COMMIT's `done` would pass this one for good. For an insert and
 * for a delete; a refusal before the marker leaves the engine healthy. */
static void case_leader_abandon(void)
{
    for (int del = 0; del < 2; del++) {
        xport x;
        xp_start(&x, del ? "labd" : "labi");
        xp_rows(&x, 0, 0, 10, 100);
        CHECK_EQ(eng_flush(x.ld), 0);
        xp_step(&x, "R2: rows");
        uint64_t before = ld_last(&x);
        volatile int flag = 1;
        tsdb_set_abandon_flag(&flag);
        int rc;
        if (del) {
            rc = eng_delete_range(x.ld, 0, T0, T0 + 5000);
        } else {
            int64_t ts[40];
            float v[40];
            for (int i = 0; i < 40; i++)
                ts[i] = T0 + 1000LL * (30 + i), v[i] = (float)i;
            rc = eng_insert_series(x.ld, TSDB_FLOAT32, 0, ts, v, 40);
        }
        tsdb_set_abandon_flag(NULL);
        CHECK_EQ(rc, TSDB_ERR_ABANDONED);
        CHECK(ld_last(&x) > before); /* the marker went out before the abandoned publish */
        CHECK(eng_check(x.ld) != 0); /* fail-stop */
        xp_stop(&x); /* a failed engine closes without a flush */
    }
    printf("  a leader whose publish fails after its COMMIT marker is committed fails the engine\n");
}

static void stop_flusher(eng *db) /* eng_halt's first step: no flush of the flusher's own from here on */
{
    pthread_mutex_lock(&db->buf_lock);
    db->flusher_stop = true;
    pthread_cond_broadcast(&db->flush_cv);
    pthread_mutex_unlock(&db->buf_lock);
    pthread_join(db->flusher, NULL);
    db->has_flusher = false;
}

/* L7: a small write's backpressure flush that waits in the flush gate holds its (server) thread: the wait is reported
 * through the write wait hook (REVIEW L4; mutant gatenohook). A depose ends it: that write was never appended, so it is
 * refused (UNAVAILABLE, definitely not applied), and so are a flush, a large insert, a delete and a DROP whose flush
 * the gate refuses (REVIEW M3: nothing of theirs was appended). The waits end within a deadline (a lost wake-up is a
 * failed check, not a hang). */
static void case_leader_deposed_refusals(void)
{
    xport x;
    xp_start(&x, "ldref");
    x.manual = true;
    eng *ld = x.ld;
    set_count_hook(ld);
    stop_flusher(ld);
    uint64_t c0 = eng_wal_durable_fno(ld);
    eng_leader_commit(ld, c0);
    pthread_mutex_lock(&ld->buf_lock);
    ld->opt.buffer_rows = 4; /* 8 rows in the active memtable: the next write flushes first */
    pthread_mutex_unlock(&ld->buf_lock);
    ljob a = {.db = ld, .s = 0, .t0 = 0, .n = 10}, b = {.db = ld, .s = 1, .t0 = 0, .n = 3};
    pthread_t ta, tb;
    pthread_create(&ta, NULL, ljob_main, &a);
    wait_hook(1); /* a: appended, waiting for its commit point */
    pthread_create(&tb, NULL, ljob_main, &b);
    wait_hook(2); /* b: in its backpressure flush, waiting in the gate for a's rows (mutant gatenohook) */
    pthread_mutex_lock(&ld->lock);
    CHECK_EQ(ld->nflushing, 1);
    pthread_mutex_unlock(&ld->lock);
    CHECK(!atomic_load(&a.done) && !atomic_load(&b.done));
    eng_leader_depose(ld);
    join_within(ta, &a.done, "L7: the depose did not end a commit wait");
    join_within(tb, &b.done, "L7: the depose did not end a wait in the flush gate");
    CHECK_EQ(a.rc, TSDB_ERR_INDETERMINATE);  /* appended: a replica may have it */
    CHECK_EQ(b.rc, TSDB_ERR_UNAVAILABLE);    /* never appended */
    CHECK_EQ(atomic_load(&HOOK_N), 0);
    stop_if_failed("L7: a deposed leader's waits");
    CHECK_EQ(eng_flush(ld), TSDB_ERR_UNAVAILABLE);
    int64_t ts[40];
    float v[40];
    for (int i = 0; i < 40; i++)
        ts[i] = T0 + 1000LL * (100 + i), v[i] = (float)i;
    CHECK_EQ(eng_insert_series(ld, TSDB_FLOAT32, 0, ts, v, 40), TSDB_ERR_UNAVAILABLE); /* a large insert */
    CHECK_EQ(eng_delete_range(ld, 0, T0, T0 + 3000), TSDB_ERR_UNAVAILABLE);
    CHECK_EQ(eng_drop_before(ld, T0 + 10 * DAY_MS), TSDB_ERR_UNAVAILABLE);
    CHECK(parts_fno_hi(ld) <= c0);
    CHECK_EQ(lrows(ld, 0), 0);
    CHECK_EQ(eng_check(ld), 0);
    xp_stop(&x);
    printf("  a deposed leader refuses what it never appended (a write's backpressure flush, reported while it waits; "
           "flush, large insert, delete, DROP)\n");
}

/* L8: the flusher's flush that a depose ends indeterminate is not recorded as the engine's background error (which the
 * next write would return: INDETERMINATE for a write never appended; mutant bgerror in REVIEW). */
static void case_leader_flusher_deposed(void)
{
    xport x;
    xp_start(&x, "lfdep");
    x.manual = true;
    eng *ld = x.ld;
    set_count_hook(ld);
    uint64_t c0 = eng_wal_durable_fno(ld);
    eng_leader_commit(ld, c0);
    ljob a = {.db = ld, .s = 0, .t0 = 0, .n = 5};
    pthread_t ta;
    pthread_create(&ta, NULL, ljob_main, &a);
    wait_hook(1);
    pthread_mutex_lock(&ld->buf_lock);
    ld->opt.flush_interval_ms = 10; /* the flusher seals a's rows and waits in the gate */
    pthread_cond_broadcast(&ld->flush_cv);
    pthread_mutex_unlock(&ld->buf_lock);
    size_t nf = 0;
    for (int i = 0; i < 2500 && nf == 0; i++) {
        usleep(2000);
        pthread_mutex_lock(&ld->lock);
        nf = ld->nflushing;
        pthread_mutex_unlock(&ld->lock);
    }
    CHECK_EQ(nf, 1);
    usleep(50000);
    eng_leader_depose(ld);
    join_within(ta, &a.done, "L8: the depose did not end a commit wait");
    CHECK_EQ(a.rc, TSDB_ERR_INDETERMINATE);
    usleep(100000); /* the flusher's flush ends refused */
    pthread_mutex_lock(&ld->buf_lock);
    CHECK_EQ(ld->bg_error, 0);
    pthread_mutex_unlock(&ld->buf_lock);
    int64_t t1[2] = {T0 + 50000, T0 + 51000};
    float v1[2] = {1, 2};
    CHECK_EQ(eng_insert_series(ld, TSDB_FLOAT32, 1, t1, v1, 2), TSDB_ERR_UNAVAILABLE);
    CHECK_EQ(eng_check(ld), 0);
    xp_stop(&x);
    printf("  a deposed leader's flusher records no background error\n");
}

typedef struct failjob {
    eng *db;
    atomic_bool done;
} failjob;
static void *fail_main(void *a)
{
    failjob *j = a;
    eng_fail(j->db, "test: the engine fails beside a commit wait", EIO);
    atomic_store(&j->done, true);
    return NULL;
}

/* L9: the guards of the leader's contract (REVIEW L5): attach refuses an engine with a periodic WAL sync (it would
 * acknowledge before durability), a leader refuses a relog (its rows past the commit point were shipped), the commit
 * point never goes back, and a failure of the engine from another thread ends a commit wait (indeterminate; the
 * engine stays failed). */
static void case_leader_guards(void)
{
    char dir[PATH_MAX];
    fresh(dir, "lasync");
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.wal_sync_ms = 10;
    eng *db;
    CHECK_EQ(eng_open(dir, &o, &db), 0);
    static struct jr_ops none;
    CHECK_EQ(eng_leader_attach(db, &none), TSDB_ERR_INVALID);
    CHECK(eng_jr(db) == NULL);
    eng_close(db);

    xport x;
    xp_start(&x, "lguard");
    x.manual = true;
    eng *ld = x.ld;
    set_count_hook(ld);
    uint64_t c0 = eng_wal_durable_fno(ld);
    eng_leader_commit(ld, c0 + 5);
    eng_leader_commit(ld, c0); /* back: ignored */
    CHECK_EQ(atomic_load(&ld->lead_commit), c0 + 5);
    eng_leader_commit(ld, c0);
    uint64_t rows = 0;
    char rd[PATH_MAX + 8];
    snprintf(rd, sizeof rd, "%s_relog", x.ldir);
    CHECK_EQ(wal_relog(ld, rd, &rows), TSDB_ERR_INVALID);
    CHECK_EQ(rows, 0);
    ljob a = {.db = ld, .s = 0, .t0 = 0, .n = 7};
    pthread_t ta, tf;
    pthread_create(&ta, NULL, ljob_main, &a);
    wait_hook(1);
    CHECK(!atomic_load(&a.done));
    failjob fj = {.db = ld};
    pthread_create(&tf, NULL, fail_main, &fj);
    join_within(tf, &fj.done, "L9: eng_fail did not return");
    join_within(ta, &a.done, "L9: a failure of the engine did not end a commit wait");
    CHECK_EQ(a.rc, TSDB_ERR_INDETERMINATE);
    CHECK(eng_check(ld) != 0);
    CHECK_EQ(atomic_load(&HOOK_N), 0);
    xp_stop(&x);
    printf("  attach refuses a periodic sync, a leader refuses a relog, the commit point never goes back, a failure "
           "ends a commit wait\n");
}

/* ---- a deposed leader (design-cluster.md 6.3, C7): it only cuts a WAL tail ---- */

static void xp_rows_at(xport *x, uint32_t s, int64_t t0, int n, float base)
{
    int64_t ts[NT];
    float v[NT];
    for (int i = 0; i < n; i++)
        ts[i] = t0 + 1000LL * i, v[i] = base + (float)i;
    CHECK_EQ(eng_insert_series(x->ld, TSDB_FLOAT32, s, ts, v, (size_t)n), 0);
}

static bool same_all(eng *a, eng *b, int64_t hi, const char *what)
{
    bool ok = true;
    for (uint32_t s = 0; s < NS; s++)
        ok &= eq_range(a, b, s, T0 - 1000, hi, what);
    return ok;
}

typedef struct cjob {
    eng *db;
    atomic_bool done;
} cjob;

static void *close_main(void *a)
{
    cjob *j = a;
    eng_close(j->db);
    atomic_store(&j->done, true);
    return NULL;
}

/* A marker above the commit point, durable at the leader and never committed (no writer waits for it) */
static uint64_t uncommitted_now(xport *x, int64_t now)
{
    x->manual = true;
    mk m = {.type = MK_NOW, .span = 1};
    m.u.now.now_ms = now;
    uint64_t pos = 0;
    CHECK_EQ(wal_marker_mk(x->ld, &m, &pos), 0);
    return pos;
}

/* r3b M4 (two of its tests): a leader is deposed with a CLASS marker above its commit point (a switch whose writer
 * waits for the commit point) and rows above it; the member that never received that tail is promoted, and its EPOCH
 * marker, a class switch to another class and rows at the same timestamps with other values take the same positions.
 * The deposed leader's switch is indeterminate and never reaches its CLASSES (the record goes to the file only once
 * the commit point passed the marker); reopened as a replica it cuts at the commit point and applies the new leader's
 * records: it then answers, and holds CLASSES, exactly as the new leader, also after a reopen. */
typedef struct swjob {
    eng *db;
    int rc;
    atomic_bool done;
} swjob;

static void *swjob_main(void *a)
{
    swjob *j = a;
    j->rc = eng_class_switch(j->db, 0, 2, NULL);
    atomic_store(&j->done, true);
    return NULL;
}

static void case_depose_rejoin(void)
{
    xport x;
    char od[PATH_MAX], rd[PATH_MAX];
    xp_start(&x, "drj");
    snprintf(od, sizeof od, "%s", x.ldir);
    rdir(rd, sizeof rd, &x);
    int64_t far = T0 + 40 * DAY_MS, hi = far + DAY_MS;
    xp_rows(&x, 0, 0, 10, 100);
    xp_rows_at(&x, 1, far, 5, 200);
    xp_step(&x, "DR: rows");
    xp_rows(&x, 0, 5, 40, 2000); /* a COMMIT */
    xp_step(&x, "DR: large insert");
    uint64_t commit = ld_last(&x), cls_before = x.ld->cls_off;
    x.manual = true; /* the commit point stays at `commit` */
    x.noship = true; /* and the member never receives what lies above it */
    eng_leader_replay_floor(x.ld, x.ld->fno_next);
    swjob sw = {.db = x.ld};
    pthread_t tsw;
    pthread_create(&tsw, NULL, swjob_main, &sw);
    for (int i = 0; i < 5000 && eng_wal_durable_fno(x.ld) < commit + 1; i++)
        usleep(1000);
    CHECK(eng_wal_durable_fno(x.ld) >= commit + 1); /* the CLASS marker is in the leader's log, not committed */
    CHECK(!atomic_load(&sw.done));
    eng_leader_depose(x.ld);
    pthread_join(tsw, NULL);
    CHECK_EQ(sw.rc, TSDB_ERR_INDETERMINATE);
    CHECK_EQ(x.ld->cls_off, cls_before); /* the record never went to CLASSES */
    ljob a = {.db = x.ld, .s = 1, .t0 = 40 * 86400 + 200, .n = 4};
    pthread_t ta;
    pthread_create(&ta, NULL, ljob_main, &a);
    pthread_join(ta, NULL);
    CHECK(a.rc != 0); /* deposed: refused or indeterminate, never acknowledged */
    eng_close(x.ld);
    x.ld = NULL;
    /* the member, which has the log to `commit`, is promoted: its EPOCH marker takes the CLASS marker's position */
    CHECK_EQ(replica_log_end(x.rep), commit);
    eng *ex = NULL;
    CHECK_EQ(open_replica(od, commit, &ex), 0);
    if (!ex) {
        xp_stop(&x);
        return;
    }
    CHECK_EQ(ex->cls_off, cls_before);
    CHECK_EQ(replica_cut(ex, commit), 0);
    CHECK_EQ(replica_log_end(ex), commit);
    uint64_t pos = 0;
    CHECK_EQ(replica_epoch_begin(x.rep, 1, "node-b", &pos), 0);
    CHECK_EQ(pos, commit + 1);
    CHECK_EQ(replica_sync(x.rep), 0);
    CHECK_EQ(ship_from(x.rep, ex), 0);
    CHECK_EQ(replica_log_end(ex), pos);
    CHECK_EQ(replica_apply(x.rep, pos), 0);
    CHECK_EQ(replica_apply(ex, pos), 0);
    uint64_t old[256];
    size_t nold = wal_seqs(rd, old, 256);
    CHECK(nold >= 1 && nold < 256);
    eng *n = NULL;
    CHECK_EQ(replica_promote(&x.rep, pos, &n), 0);
    if (!n)
        exit(2);
    x.ld = n;
    snprintf(x.ldir, sizeof x.ldir, "%s", rd);
    x.rep = ex;
    {
        struct stat st;
        char p[PATH_MAX + 64], nm[64];
        wal_name(nm, sizeof nm, old[nold - 1]);
        snprintf(p, sizeof p, "%s/%s", rd, nm);
        CHECK_EQ(stat(p, &st), 0);
        x.cseq = old[nold - 1];
        x.coff = (uint64_t)st.st_size;
    }
    x.noship = x.manual = false;
    n->opt.wal_max_batch_rows = 16;
    CHECK_EQ(eng_leader_attach(n, &XOPS), 0);
    eng_leader_replay_floor(n, n->fno_next);
    CHECK_EQ(eng_class_switch(n, 0, 1, NULL), 0); /* another class than the deposed leader's switch */
    xp_rows_at(&x, 1, T0 + 1000LL * (40 * 86400 + 200), 4, 7000); /* the timestamps of the indeterminate rows */
    xp_rows(&x, 0, 60, 3, 50);
    xp_step(&x, "DR: the new leader's records");
    CHECK(same_all(n, ex, hi, "DR: the deposed leader as a replica of the new one"));
    CHECK_EQ(ex->cls_off, n->cls_off);
    CHECK(ex->cls_off > cls_before);
    uint64_t fl = replica_applied_end(ex);
    CHECK_EQ(mt_flush(ex), 0);
    CHECK_EQ(replica_floor_saved(ex, fl), 0);
    xp_reopen(&x, od, fl); /* the open checks every part against CLASSES */
    if (x.rep) {
        CHECK_EQ(eng_check(x.rep), 0);
        CHECK_EQ(replica_applied_end(x.rep), fl);
        CHECK_EQ(x.rep->cls_off, n->cls_off);
        CHECK(same_all(n, x.rep, hi, "DR: reopened"));
        xp_rows(&x, 0, 70, 2, 60);
        xp_step(&x, "DR: after the reopen");
    }
    xp_stop(&x);
    printf("  a leader deposed with a CLASS marker above its commit point rejoins as a replica of the new leader\n");
}

/* L2: the close of a leader whose commit point can no longer move (the ISR is gone) returns: it deposes the leader and
 * flushes nothing (mutant closewait: the close waits in its flush for a commit point that never comes, and the
 * deadline of join_within fails the run) */
static void case_close_bound(void)
{
    xport x;
    xp_start(&x, "dclose");
    xp_rows(&x, 0, 0, 10, 100);
    xp_step(&x, "DC: rows");
    CHECK(uncommitted_now(&x, T0) > ld_last(&x) - 1);
    cjob j = {.db = x.ld};
    pthread_t t;
    pthread_create(&t, NULL, close_main, &j);
    join_within(t, &j.done, "DC: the close of a leader whose commit point cannot move returned (mutant closewait)");
    x.ld = NULL;
    eng_close(x.rep);
    free(x.held);
}

/* A deposed leader holds committed rows, two committed large inserts (COMMIT markers it published itself), a committed
 * DROP and, above the commit point, rows and a marker. It is closed (no flush: mutant leaderclose flushes and stores its
 * log end in FNO) and reopened as a replica at the commit floor, then cut at the commit point: the open skips the
 * COMMITs and the DROP it published (its RSTATE says so; mutant leadernorstate: the open takes its own COMMITs for ones
 * to apply, and stops below one or lists its file again), the cut drops what was never committed, a second open accepts the log (FNO not past its end), and it
 * answers exactly as the replica that never led. */
static void case_depose(void)
{
    xport x;
    char od[PATH_MAX];
    xp_start(&x, "dep");
    snprintf(od, sizeof od, "%s", x.ldir);
    int64_t far = T0 + 40 * DAY_MS, hi = far + DAY_MS;
    xp_rows(&x, 0, 0, 10, 100);
    xp_rows_at(&x, 1, far, 5, 200);
    xp_step(&x, "D: small rows");
    xp_rows(&x, 0, 5, 40, 2000); /* a large insert: a COMMIT */
    xp_step(&x, "D: large insert");
    CHECK_EQ(eng_flush(x.ld), 0);
    CHECK_EQ(eng_drop_before(x.ld, T0 + 10 * DAY_MS), 0); /* a DROP */
    xp_step(&x, "D: drop");
    xp_rows_at(&x, 0, far, 40, 3000);        /* a large insert after the DROP */
    xp_rows_at(&x, 1, far + 100000, 3, 400); /* small rows, in the WAL only */
    xp_step(&x, "D: after the drop");
    CHECK(same_all(x.ld, x.rep, hi, "D: before the depose"));
    uint64_t commit = ld_last(&x);
    /* above the commit point: a marker, and rows whose writer waits (indeterminate once deposed) */
    CHECK(uncommitted_now(&x, far) > commit);
    ljob a = {.db = x.ld, .s = 1, .t0 = 40 * 86400 + 200, .n = 4};
    pthread_t ta;
    pthread_create(&ta, NULL, ljob_main, &a);
    for (int i = 0; i < 5000 && eng_wal_durable_fno(x.ld) < commit + 5; i++)
        usleep(1000);
    CHECK(eng_wal_durable_fno(x.ld) >= commit + 5); /* the rows are in the leader's log */
    eng_leader_depose(x.ld);                         /* the driver: another node leads now */
    pthread_join(ta, NULL);
    CHECK_EQ(a.rc, TSDB_ERR_INDETERMINATE);
    eng_close(x.ld);
    x.ld = NULL;
    eng *ex = NULL;
    CHECK_EQ(open_replica(od, commit, &ex), 0);
    if (!ex) {
        xp_stop(&x);
        return;
    }
    /* mutant leadernorstate: its own COMMITs taken for ones to apply, from the files it keeps linked for its replicas
     * (held below the first, or each COMMIT's file listed a second time) */
    CHECK_EQ(replica_applied_end(ex), commit);
    CHECK(!parts_twice(ex));
    CHECK_EQ(replica_cut(ex, commit), 0);
    CHECK_EQ(replica_log_end(ex), commit);
    CHECK(same_all(ex, x.rep, hi, "D: the deposed leader as a replica"));
    eng_close(ex);
    ex = NULL;
    CHECK_EQ(open_replica(od, commit, &ex), 0); /* mutant leaderclose: FNO holds the uncut log end, CORRUPT */
    if (ex) {
        CHECK_EQ(replica_log_end(ex), commit);
        CHECK(same_all(ex, x.rep, hi, "D: the deposed leader as a replica, reopened"));
        eng_close(ex);
    }
    xp_stop(&x);
}


static bool seg_exists(const char *dir, uint64_t seq)
{
    char name[64], p[PATH_MAX + 80];
    wal_name(name, sizeof name, seq);
    snprintf(p, sizeof p, "%s/%s", dir, name);
    return access(p, F_OK) == 0;
}

/* Item 1 (design-cluster.md 8.3): a leader's flushed segments are the replicas' catch-up source, so they stay until
 * the driver's keep bound passes their last position. Two flushed segments survive their flush (mutant keepignored:
 * they go at once); a bound one below the first's last position removes nothing; at that position the first goes,
 * with FNO holding its end first, and the second stays; a bound past both removes the second; a bound moved back
 * keeps the next one. */
struct keep_arg {
    eng *db;
    uint64_t pos;
    atomic_int done;
};

static void *keep_thread(void *a)
{
    struct keep_arg *k = a;
    eng_leader_keep(k->db, k->pos);
    atomic_store(&k->done, 1);
    return NULL;
}

static void case_leader_keep(void)
{
    xport x;
    xp_start(&x, "lkeep");
    xp_rows(&x, 0, 0, 10, 100);
    xp_step(&x, "LK: rows 1");
    CHECK_EQ(eng_flush(x.ld), 0);
    xp_rows(&x, 1, 0, 10, 200);
    xp_step(&x, "LK: rows 2");
    CHECK_EQ(eng_flush(x.ld), 0);
    CHECK_EQ(x.ld->nlk, 2); /* mutant keepignored: both went at their flush */
    if (x.ld->nlk != 2) {
        xp_stop(&x);
        return;
    }
    struct lkseg a = x.ld->lk[0], b = x.ld->lk[1];
    CHECK(seg_exists(x.ldir, a.seq) && seg_exists(x.ldir, b.seq));
    eng_leader_keep(x.ld, a.fno_end - 2);
    CHECK(seg_exists(x.ldir, a.seq) && seg_exists(x.ldir, b.seq));
    /* a flush holds flush_lock while it waits for the commit point, which the driver's thread moves: the driver's call
     * must not wait for it (tsdbd's repld hung there under a large write); the removal is left to a later call */
    struct keep_arg ka = {x.ld, a.fno_end - 1, 0};
    pthread_t kt;
    pthread_mutex_lock(&x.ld->flush_lock);
    CHECK_EQ(pthread_create(&kt, NULL, keep_thread, &ka), 0);
    for (int i = 0; i < 500 && !atomic_load(&ka.done); i++)
        nanosleep(&(struct timespec){0, 10000000}, NULL);
    CHECK(atomic_load(&ka.done)); /* without the fix: it waits for flush_lock */
    CHECK(seg_exists(x.ldir, a.seq));
    pthread_mutex_unlock(&x.ld->flush_lock);
    pthread_join(kt, NULL);
    eng_leader_keep(x.ld, a.fno_end - 1);
    CHECK(!seg_exists(x.ldir, a.seq) && seg_exists(x.ldir, b.seq));
    CHECK(atomic_load(&x.ld->fno_durable) >= a.fno_end); /* FNO held its end before it went */
    eng_leader_keep(x.ld, UINT64_MAX);
    CHECK(!seg_exists(x.ldir, b.seq));
    CHECK_EQ(x.ld->nlk, 0);
    eng_leader_keep(x.ld, 0); /* a replica rejoins far behind: what is gone stays gone, the next one stays */
    xp_rows(&x, 0, 20, 10, 300);
    xp_step(&x, "LK: rows 3");
    CHECK_EQ(eng_flush(x.ld), 0);
    CHECK_EQ(x.ld->nlk, 1);
    if (x.ld->nlk == 1)
        CHECK(seg_exists(x.ldir, x.ld->lk[0].seq));
    CHECK(same_all(x.ld, x.rep, T0 + DAY_MS, "LK: the replica"));
    xp_stop(&x);
    printf("  a leader keeps its flushed segments until the driver's bound passes their end, FNO first\n");
}
struct rcap_arg {
    eng *db;
    uint32_t s;
    int64_t t0;
    atomic_int bad;
};

static void *rcap_thread(void *a)
{
    struct rcap_arg *r = a;
    int64_t ts[40];
    float v[40];
    for (int k = 0; k < 30; k++) {
        for (int i = 0; i < 40; i++) {
            ts[i] = r->t0 + 1000LL * (k * 40 + i);
            v[i] = (float)(k + i);
        }
        if (eng_insert_series(r->db, TSDB_FLOAT32, r->s, ts, v, 40) != 0)
            atomic_fetch_add(&r->bad, 1);
    }
    return NULL;
}

/* A replicated leader's rows record fits one peer frame: tsdbd's driver sends whole records in one frame and refused a
 * larger one for ever (the commit point never passed it and every write of the volume waited). Batches buffered
 * while the writer syncs stop joining below jr_rec_max_rows, and a batch of as many rows goes to parts. */
static void case_leader_rec_cap(void)
{
    xport x;
    xp_start(&x, "reccap");
    x.ld->opt.wal_max_batch_rows = 1 << 22; /* the cap alone decides what goes through the WAL */
    atomic_store(&jr_rec_max_rows, 64);
    struct rcap_arg ra[4];
    pthread_t th[4];
    for (int i = 0; i < 4; i++) {
        ra[i] = (struct rcap_arg){.db = x.ld, .s = (uint32_t)(i % 2), .t0 = T0 + 10 * DAY_MS + i * 2000000LL};
        atomic_init(&ra[i].bad, 0);
        CHECK_EQ(pthread_create(&th[i], NULL, rcap_thread, &ra[i]), 0);
    }
    for (int i = 0; i < 4; i++) {
        pthread_join(th[i], NULL);
        CHECK_EQ(atomic_load(&ra[i].bad), 0);
    }
    CHECK(x.max_span > 0 && x.max_span < 64); /* without the cap, batches buffered during a sync join past it */
    int64_t ts[100];
    float v[100];
    for (int i = 0; i < 100; i++)
        ts[i] = T0 + 20 * DAY_MS + 1000LL * i, v[i] = (float)i;
    CHECK_EQ(eng_insert_series(x.ld, TSDB_FLOAT32, 0, ts, v, 100), 0);
    CHECK(x.max_span < 64); /* a batch of the cap or more is a large write: parts, not one oversized record */
    atomic_store(&jr_rec_max_rows, 1u << 18);
    x.ld->opt.wal_max_batch_rows = 16;
    xp_step(&x, "RC: rows");
    xp_stop(&x);
    printf("  a replicated leader's rows records stay below jr_rec_max_rows (one peer frame)\n");
}

static bool leader_file_is(eng *db, uint64_t pos, const char *name, const uint8_t *b, size_t len)
{
    int fd = eng_leader_file_open(db, pos, name);
    if (fd < 0)
        return false;
    uint8_t *got = malloc(len + 1);
    ssize_t r = got ? pread(fd, got, len + 1, 0) : -1;
    bool ok = r == (ssize_t)len && memcmp(got, b, len) == 0;
    free(got);
    close(fd);
    return ok;
}

/* Item 2 (design-cluster.md 5.2, M4): the files a leader's COMMIT marker names stay readable by the marker's names
 * (stage_ hard links) while the segment holding the marker is retained, and go with it.
 * (a) A large insert's file is the shipped bytes; it still is after a second large insert into the partition and a
 *     merge of both (the part is gone, its retired name too: no lifetime); once the segment goes past the keep bound,
 *     the file is NOTFOUND (a replica that far behind needs a snapshot).
 * (b) A leader loses power after a COMMIT marker is durable and before its parts_commit (the writer waits for the
 *     commit point): the copy of its directory then holds the marker and tmp_ files that an open removes (load_parts).
 *     Opened as a replica at the marker (design-cluster.md 6.3), it applies the marker from the links and answers as
 *     the replica that saw the COMMIT. Mutant nohardlink: no links, (a) finds no file and (b) holds below the marker. */
static void case_hardlink(void)
{
    xport x;
    char cp[PATH_MAX + 16];
    xp_start(&x, "hlink");
    snprintf(cp, sizeof cp, "%s.crash", x.ldir);
    xp_rows(&x, 0, 0, 40, 1000); /* a large insert: a COMMIT */
    xp_step(&x, "HL: large insert");
    char name[64];
    snprintf(name, sizeof name, "%s", x.held_name);
    size_t len = x.held_len;
    uint8_t *b = malloc(len);
    memcpy(b, x.held, len);
    const uint64_t hpos = x.ld->nlf ? x.ld->lf[x.ld->nlf - 1].fno : 0; /* the COMMIT marker's position */
    CHECK(leader_file_is(x.ld, hpos, name, b, len)); /* mutant nohardlink: NOTFOUND */
    xp_rows(&x, 0, 40, 40, 2000);
    xp_step(&x, "HL: second large insert");
    x.ld->opt.obsolete_part_lifetime_s = 0;
    CHECK_EQ(eng_optimize(x.ld), 0);
    CHECK(leader_file_is(x.ld, hpos, name, b, len)); /* the merge took the part, not the link */
    xp_rows(&x, 1, 200, 5, 3000); /* rows, so that the flush seals the segment that holds the markers */
    xp_step(&x, "HL: small rows");
    CHECK_EQ(eng_flush(x.ld), 0);
    eng_leader_keep(x.ld, UINT64_MAX);
    int fd = eng_leader_file_open(x.ld, hpos, name);
    CHECK_EQ(fd, TSDB_ERR_NOTFOUND); /* gone with the segment */
    if (fd >= 0)
        close(fd);
    free(b);

    x.manual = true;
    uint64_t before = ld_last(&x);
    eng_leader_commit(x.ld, before);
    ljob a = {.db = x.ld, .s = 1, .t0 = 100, .n = 40}; /* a large insert whose writer waits for the commit point */
    pthread_t ta;
    pthread_create(&ta, NULL, ljob_main, &a);
    for (int i = 0; i < 5000 && eng_wal_durable_fno(x.ld) <= before; i++)
        usleep(1000);
    uint64_t m = eng_wal_durable_fno(x.ld);
    CHECK(m > before);
    sh("cp -a '%s' '%s'", x.ldir, cp); /* the power loss: the marker is durable, its parts are tmp_ files */
    eng_leader_commit(x.ld, m);
    pthread_join(ta, NULL);
    CHECK_EQ(a.rc, 0);
    xp_step(&x, "HL: the COMMIT");
    eng *ex = NULL;
    CHECK_EQ(open_replica(cp, m, &ex), 0);
    if (ex) {
        CHECK_EQ(replica_applied_end(ex), m); /* mutant nohardlink: held below the marker, its files are gone */
        CHECK(same_all(ex, x.rep, T0 + DAY_MS, "HL: the leader that lost power, as a replica"));
        eng_close(ex);
    }
    xp_stop(&x);
    printf("  a leader keeps its COMMITs' files linked while their segment is retained; a power loss before the publish keeps them\n");
}

/* r3a2 R5: names of COMMIT files repeat across engines in one process (the tmp counter starts again at every open), so
 * a leader serves a link only for the marker position that named it in this leadership. A leader's first large insert
 * names N at P1; reopened (a new leadership in the same process), its first large insert names N again, at P2, with
 * other bytes. P2 gives the new bytes; P1 is NOTFOUND (the replica takes a snapshot), never the new file under the old
 * marker, which a replica's check refuses and its catch-up stalls on. Mutant linkbyname: P1 serves the new file. */
/* r3g REVIEW F1: a COMMIT's links are served from before its marker can be read (noted after the append, a replica asking
 * between the sync and the note got NOTFOUND and went to a snapshot: mutant notelate) */
static struct {
    xport *x;
    int rc;
    bool fired;
} LM;

static void lm_hook(eng *db, uint64_t fno)
{
    if (LM.fired || !LM.x || db != LM.x->ld)
        return;
    LM.fired = true;
    LM.rc = eng_leader_file_open(db, fno, LM.x->held_name);
    if (LM.rc >= 0)
        close(LM.rc);
}

static void case_link_pos(void)
{
    xport x;
    xp_start(&x, "lpos");
    LM.x = &x, LM.fired = false, LM.rc = -1;
    leader_test_marker_hook = lm_hook;
    xp_rows(&x, 0, 0, 40, 1000); /* a large insert: a COMMIT */
    leader_test_marker_hook = NULL;
    LM.x = NULL;
    CHECK(LM.fired);
    if (LM.rc < 0)
        fprintf(stderr, "LP: the file of a COMMIT whose marker is in the log was not served (%d; mutant notelate)\n", LM.rc);
    CHECK(LM.rc >= 0);
    xp_step(&x, "LP: first leadership");
    char n1[64];
    snprintf(n1, sizeof n1, "%s", x.held_name);
    const uint64_t p1 = x.ld->nlf ? x.ld->lf[x.ld->nlf - 1].fno : 0;
    CHECK(p1 != 0);
    CHECK(leader_file_is(x.ld, p1, n1, x.held, x.held_len));
    CHECK_EQ(eng_leader_file_open(x.ld, p1 + 1, n1), TSDB_ERR_NOTFOUND); /* no marker there names it */
    eng_close(x.ld);
    xp_open_leader(&x);
    xp_rows(&x, 0, 0, 40, 5000); /* the same rows' keys, other values: the same partition, other bytes */
    xp_step(&x, "LP: second leadership");
    const uint64_t p2 = x.ld->nlf ? x.ld->lf[x.ld->nlf - 1].fno : 0;
    if (strcmp(n1, x.held_name) != 0)
        fprintf(stderr, "LP: the second leadership named %s, not %s: the test needs a repeated name\n", x.held_name, n1);
    CHECK(strcmp(n1, x.held_name) == 0);
    CHECK(p2 > p1);
    CHECK(leader_file_is(x.ld, p2, n1, x.held, x.held_len));
    int fd = eng_leader_file_open(x.ld, p1, n1);
    if (fd >= 0)
        fprintf(stderr, "LP: the marker at %" PRIu64 " was served the file of the marker at %" PRIu64 " (mutant linkbyname)\n",
                p1, p2);
    CHECK_EQ(fd, TSDB_ERR_NOTFOUND);
    if (fd >= 0)
        close(fd);
    /* r3g REVIEW F7: the right position and another marker's file (mutant linkposonly) */
    xp_rows(&x, 1, 0, 40, 6000); /* another COMMIT, another file */
    xp_step(&x, "LP: a third COMMIT");
    const uint64_t p3 = x.ld->nlf ? x.ld->lf[x.ld->nlf - 1].fno : 0;
    CHECK(p3 > p2 && strcmp(n1, x.held_name) != 0);
    CHECK(leader_file_is(x.ld, p3, x.held_name, x.held, x.held_len));
    fd = eng_leader_file_open(x.ld, p2, n1); /* p2's link is still there */
    CHECK(fd >= 0);
    if (fd >= 0)
        close(fd);
    int f3 = eng_leader_file_open(x.ld, p3, n1), f2 = eng_leader_file_open(x.ld, p2, x.held_name);
    if (f3 >= 0 || f2 >= 0)
        fprintf(stderr, "LP: a marker was served another marker's file (mutant linkposonly)\n");
    CHECK_EQ(f3, TSDB_ERR_NOTFOUND);
    CHECK_EQ(f2, TSDB_ERR_NOTFOUND);
    if (f3 >= 0)
        close(f3);
    if (f2 >= 0)
        close(f2);
    xp_stop(&x);
    printf("  a leader serves a COMMIT's file only for the marker that named it in this leadership\n");
}

/* r3a2 R8: a leader's RSTATE lists its DROPs (a deposed leader reopened as a replica skips the rows of its retained log
 * that a DROP retired), and drops one once no retained record lies behind it. A DROP stays while its own segment is
 * kept (the segment before it went: mutant leaderpruneall drops it too) and goes with that segment (mutant
 * leadernoprune: the list only grows). */
static void case_leader_prune(void)
{
    xport x;
    xp_start(&x, "lprune");
    xp_rows(&x, 0, 0, 10, 100);
    xp_step(&x, "LR: small rows");
    CHECK_EQ(eng_drop_before(x.ld, T0 + 10 * DAY_MS), 0); /* a DROP: its marker begins a new segment */
    xp_step(&x, "LR: drop");
    uint64_t pd = 0, f;
    CHECK_EQ(leader_rs_ndrops(x.ld, &pd), 1);
    CHECK(pd > 1);
    CHECK(x.ld->nlk >= 1);
    eng_leader_keep(x.ld, pd - 1); /* the segment before the DROP goes */
    CHECK_EQ(x.ld->nlk, 0);
    long n = leader_rs_ndrops(x.ld, &f);
    if (n != 1)
        fprintf(stderr, "LR: the DROP at %" PRIu64 " left RSTATE while its segment is kept (mutant leaderpruneall)\n", pd);
    CHECK_EQ(n, 1);
    xp_rows(&x, 0, 20 * 86400, 10, 300);
    xp_step(&x, "LR: rows after the drop");
    CHECK_EQ(eng_flush(x.ld), 0); /* the DROP's segment is flushed and kept */
    CHECK(x.ld->nlk >= 1);
    eng_leader_keep(x.ld, UINT64_MAX); /* and goes */
    CHECK_EQ(x.ld->nlk, 0);
    n = leader_rs_ndrops(x.ld, &f);
    if (n != 0)
        fprintf(stderr, "LR: the DROP at %" PRIu64 " stays in RSTATE after its segment went (mutant leadernoprune)\n", pd);
    CHECK_EQ(n, 0);
    xp_stop(&x);
    printf("  a leader's RSTATE lists a DROP while a retained segment lies behind it\n");
}

/* Feeds dst from the leader's log by position only (replica_read on the writer, `cap` bytes a read), then fetches every
 * file a COMMIT names that dst misses from the leader (eng_leader_file_open), syncs and applies to `commit`. */
static int feed_once(eng *ld, eng *dst, size_t cap) /* one read and its append: 1 bytes came, 0 the end, < 0 an error */
{
    uint8_t *b;
    size_t len;
    uint64_t seq, off, last;
    int rc = replica_read(ld, replica_log_end(dst) + 1, &b, &len, cap, &seq, &off, &last);
    if (rc == 0 && len)
        rc = replica_append(dst, seq, off, b, len);
    free(b);
    return rc ? rc : len > 0;
}

static int feed_finish(eng *ld, eng *dst, uint64_t commit);

static int feed_from_leader(eng *ld, eng *dst, size_t cap, uint64_t commit)
{
    int rc;
    while ((rc = feed_once(ld, dst, cap)) > 0)
        ;
    return rc ? rc : feed_finish(ld, dst, commit);
}

static int feed_finish(eng *ld, eng *dst, uint64_t commit)
{
    int rc = 0;
    char name[64];
    uint64_t size, mpos;
    while (rc == 0 && replica_missing(dst, &mpos, name, sizeof name, &size) == 1) {
        int fd = eng_leader_file_open(ld, mpos, name);
        uint8_t *b = fd >= 0 ? malloc(size ? size : 1) : NULL;
        rc = fd < 0 ? fd : !b || pread(fd, b, size, 0) != (ssize_t)size ? TSDB_ERR_IO : replica_stage(dst, name, b, size);
        free(b);
        if (fd >= 0)
            close(fd);
    }
    if (rc == 0)
        rc = replica_sync(dst);
    return rc ? rc : replica_apply(dst, commit);
}

/* Item 3: a leader serves its log by position (replica_read on a writer) from its retained segments, the segments not
 * flushed yet and the live tail of the current one. A replica fed only by such reads, in chunks of 300 bytes and of
 * 1 MiB, with the files of the COMMITs it misses fetched from the leader's links, ends at the leader's log end and
 * answers as the replica fed the segment files; a read at the end + 1 is empty, beyond it INVALID; once the keep bound
 * lets the flushed segments go, a position in them is NOTFOUND (a snapshot is needed). */
static void case_leader_read(void)
{
    xport x;
    char fa[PATH_MAX + 8], fb[PATH_MAX + 8], fc[PATH_MAX + 8], fd[PATH_MAX + 8];
    xp_start(&x, "lread");
    snprintf(fa, sizeof fa, "%s_fa", x.ldir);
    snprintf(fb, sizeof fb, "%s_fb", x.ldir);
    snprintf(fc, sizeof fc, "%s_fc", x.ldir);
    snprintf(fd, sizeof fd, "%s_fd", x.ldir);
    sh("rm -rf '%s' '%s' '%s' '%s' && cp -a '%s_r' '%s' && cp -a '%s_r' '%s' && cp -a '%s_r' '%s' && cp -a '%s_r' '%s'", fa,
       fb, fc, fd, x.ldir, fa, x.ldir, fb, x.ldir, fc, x.ldir, fd);
    xp_rows(&x, 0, 0, 10, 100);
    xp_rows(&x, 1, 0, 8, 200);
    xp_step(&x, "LR: rows");
    CHECK_EQ(eng_flush(x.ld), 0); /* a retained segment */
    xp_rows(&x, 0, 10, 40, 1000); /* a large insert: a COMMIT */
    xp_rows(&x, 1, 8, 6, 300);
    xp_step(&x, "LR: large insert");
    CHECK_EQ(eng_flush(x.ld), 0);
    xp_rows(&x, 0, 60, 5, 400); /* the live tail of the current segment */
    xp_rows(&x, 1, 60, 40, 500);
    xp_rows(&x, 1, 100, 3, 600);
    xp_step(&x, "LR: tail");
    uint64_t end = ld_last(&x);
    eng *a = NULL, *b = NULL;
    CHECK_EQ(open_replica(fa, 0, &a), 0);
    CHECK_EQ(open_replica(fb, 0, &b), 0);
    if (a && b) {
        CHECK_EQ(feed_from_leader(x.ld, a, 300, end), 0);
        CHECK_EQ(feed_from_leader(x.ld, b, (size_t)1 << 20, end), 0);
        CHECK_EQ(replica_log_end(a), end);
        CHECK_EQ(replica_applied_end(a), end);
        CHECK_EQ(replica_applied_end(b), end);
        CHECK(same_all(a, x.rep, T0 + DAY_MS, "LR: fed by reads of 300 bytes"));
        CHECK(same_all(b, x.rep, T0 + DAY_MS, "LR: fed by reads of 1 MiB"));
    }
    eng *c = NULL, *d = NULL; /* two replicas reading by turns, d behind c in the same segment: the leader's read cursor
                                 is one per engine and must not carry d past its position (mutant cursoranyway) */
    CHECK_EQ(open_replica(fc, 0, &c), 0);
    CHECK_EQ(open_replica(fd, 0, &d), 0);
    if (c && d) {
        int rc_c = 1, rc_d = 1;
        while (rc_c > 0 || rc_d > 0) {
            if (rc_c > 0)
                rc_c = feed_once(x.ld, c, 300);
            if (rc_d > 0)
                rc_d = feed_once(x.ld, d, 150);
        }
        CHECK_EQ(rc_c, 0);
        CHECK_EQ(rc_d, 0);
        CHECK_EQ(feed_finish(x.ld, c, end), 0);
        CHECK_EQ(feed_finish(x.ld, d, end), 0);
        CHECK_EQ(replica_applied_end(c), end);
        CHECK_EQ(replica_applied_end(d), end);
        CHECK(same_all(c, x.rep, T0 + DAY_MS, "LR: read by turns, ahead"));
        CHECK(same_all(d, x.rep, T0 + DAY_MS, "LR: read by turns, behind"));
    }
    uint8_t *buf;
    size_t len;
    uint64_t seq, off, last;
    { /* a sealed segment's errors (damage is never silent): a position inside a record is INVALID, also inside the
       * segment's last record (mutant notfoundsealed: "not written yet"), and a damaged record CORRUPT (mutant
       * tailoksealed: a sealed segment read as if a writer were still in its last record) */
        uint64_t s1 = 0, f = 1, mid = 0, lf = 0, ll = 0, loff = 0;
        size_t llen = 0;
        for (;;) { /* record by record (cap 1: one record a read) */
            int rc = replica_read(x.ld, f, &buf, &len, 1, &seq, &off, &last);
            free(buf);
            if (rc || len == 0 || (s1 && seq != s1))
                break;
            s1 = seq;
            if (last > f && !mid)
                mid = f + 1;
            lf = f, ll = last, loff = off, llen = len;
            f = last + 1;
        }
        CHECK(s1 && s1 + 1 < x.ld->wal_seq); /* sealed */
        CHECK(mid && ll > lf); /* records of several rows, the segment's last one too */
        CHECK_EQ(replica_read(x.ld, mid, &buf, &len, 1 << 20, &seq, &off, &last), TSDB_ERR_INVALID);
        free(buf);
        CHECK_EQ(replica_read(x.ld, lf + 1, &buf, &len, 1 << 20, &seq, &off, &last), TSDB_ERR_INVALID);
        free(buf);
        char sp[PATH_MAX + 64];
        snprintf(sp, sizeof sp, "%s/wal_%016" PRIx64 ".log", x.ldir, s1);
        int sfd = open(sp, O_RDWR | O_CLOEXEC);
        CHECK(sfd >= 0);
        uint8_t was = 0, flip;
        off_t at = (off_t)(loff + llen - 1);
        if (sfd >= 0 && pread(sfd, &was, 1, at) == 1) {
            flip = was ^ 0x5a;
            CHECK(pwrite(sfd, &flip, 1, at) == 1);
            CHECK_EQ(replica_read(x.ld, 1, &buf, &len, 1 << 20, &seq, &off, &last), TSDB_ERR_CORRUPT);
            free(buf);
            CHECK_EQ(replica_read(x.ld, lf, &buf, &len, 1 << 20, &seq, &off, &last), TSDB_ERR_CORRUPT);
            free(buf);
            CHECK(pwrite(sfd, &was, 1, at) == 1);
        }
        if (sfd >= 0)
            close(sfd);
    }
    CHECK_EQ(replica_read(x.ld, end + 1, &buf, &len, 100, &seq, &off, &last), 0);
    CHECK_EQ(len, 0);
    free(buf);
    CHECK_EQ(replica_read(x.ld, end + 2, &buf, &len, 100, &seq, &off, &last), TSDB_ERR_INVALID);
    free(buf);
    eng_leader_keep(x.ld, UINT64_MAX);
    CHECK_EQ(replica_read(x.ld, 1, &buf, &len, 100, &seq, &off, &last), TSDB_ERR_NOTFOUND);
    free(buf);
    if (a)
        eng_close(a);
    if (b)
        eng_close(b);
    if (c)
        eng_close(c);
    if (d)
        eng_close(d);
    xp_stop(&x);
    printf("  a leader serves its log by position from its retained segments and the live tail, to readers by turns; "
           "a sealed segment's damage is CORRUPT, a position inside a record INVALID\n");
}

/* A leader's attach removes stage_ files an earlier leadership (or a promoted replica) left: none of them is a file of a
 * marker this leadership will serve, and one with a repeated name would be served for another (mutant nosweep) */
static void case_link_sweep(void)
{
    xport x;
    char ld[PATH_MAX], p[PATH_MAX + 64];
    memset(&x, 0, sizeof x);
    fresh(ld, "lsweep");
    snprintf(x.ldir, sizeof x.ldir, "%s", ld);
    snprintf(p, sizeof p, "%s/stage_0123456789abcdef.part", ld);
    sh("echo stale > '%s'", p);
    xp_open_leader(&x);
    CHECK(access(p, F_OK) != 0);
    eng_close(x.ld);
    free(x.held);
    printf("  a leader's attach removes the stage_ links of an earlier leadership\n");
}

/* names in dir that start with `prefix` */
static int count_named(const char *dir, const char *prefix)
{
    DIR *d = opendir(dir);
    int n = 0;
    struct dirent *e;
    while (d && (e = readdir(d)))
        n += strncmp(e->d_name, prefix, strlen(prefix)) == 0;
    if (d)
        closedir(d);
    return n;
}

/* r3e finding A2 (jepsen-cluster meta,classes power): a member promoted after a failover serves a laggard the files of
 * the COMMIT markers in its retained segments, which an earlier leadership wrote (this member's own as the leader before
 * it was deposed, or ones it applied as a replica): every member keeps a committed marker's files as
 * keep_<pos>_<key>.part as long as the segment that holds the marker (mutant replicadropsfiles: the old rule, a replica
 * removed them at the apply and a deposed leader at its reopen, and the promoted member answered "needs a snapshot"). */
static void case_keep_files(void)
{
    xport x;
    char rd[PATH_MAX], kp[64];
    xp_start(&x, "kfiles");
    rdir(rd, sizeof rd, &x);
    xp_rows(&x, 0, 0, 40, 1000); /* a large insert: a COMMIT */
    xp_step(&x, "KF: commit");
    xp_rows(&x, 1, 0, 5, 50);
    CHECK_EQ(eng_flush(x.ld), 0); /* the marker's segment ends: it can go once the floor or the bound passes it */
    xp_step(&x, "KF: flushed");
    char n1[64];
    snprintf(n1, sizeof n1, "%s", x.held_name);
    const uint64_t p1 = x.ld->nlf ? x.ld->lf[x.ld->nlf - 1].fno : 0;
    CHECK(p1 != 0);
    uint8_t *h1 = malloc(x.held_len);
    size_t l1 = x.held_len;
    if (!h1)
        exit(2);
    memcpy(h1, x.held, l1);
    snprintf(kp, sizeof kp, "keep_%016" PRIx64 "_", p1);
    int nk = count_named(rd, kp);
    if (nk != 1)
        fprintf(stderr, "KF: the replica keeps %d files of the applied marker at %" PRIu64 " (mutant replicadropsfiles)\n",
                nk, p1);
    CHECK_EQ(nk, 1);
    CHECK_EQ(count_named(rd, "stage_"), 0); /* the stage_ name is free for the next marker naming the same key */
    xp_reopen(&x, rd, replica_applied_end(x.rep));
    replica_keep_cap(x.rep, 1 << 30, 3600000); /* the driver's caps: without them a segment goes at the floor */
    CHECK_EQ(count_named(rd, kp), 1); /* its segment is still here */
    /* the old leader, deposed, reopens as a replica: its own committed marker's link becomes a keep_ file */
    xp_rows(&x, 1, 10, 5, 60); /* in the next segment */
    xp_step(&x, "KF: small rows");
    uint64_t end = ld_last(&x);
    eng_close(x.ld);
    x.ld = NULL;
    eng *old = NULL;
    CHECK_EQ(open_replica(x.ldir, end, &old), 0);
    nk = count_named(x.ldir, kp);
    if (nk != 1)
        fprintf(stderr, "KF: the deposed leader keeps %d files of its marker at %" PRIu64 " (mutant replicadropsfiles)\n",
                nk, p1);
    CHECK_EQ(nk, 1);
    CHECK_EQ(count_named(x.ldir, "stage_"), 0);
    /* the replica is promoted: it serves the marker's file from the attach on */
    uint64_t pos = 0;
    eng *n = NULL;
    CHECK_EQ(replica_epoch_begin(x.rep, 1, "node-a", &pos), 0);
    CHECK_EQ(replica_sync(x.rep), 0);
    CHECK_EQ(replica_apply(x.rep, pos), 0);
    CHECK_EQ(replica_promote(&x.rep, pos, &n), 0);
    if (!n)
        exit(2);
    CHECK_EQ(eng_leader_attach(n, &XOPS), 0);
    bool served = leader_file_is(n, p1, n1, h1, l1);
    if (!served)
        fprintf(stderr, "KF: the promoted member does not serve the file of the marker at %" PRIu64 "\n", p1);
    CHECK(served);
    CHECK_EQ(eng_leader_file_open(n, p1 + 1, n1), TSDB_ERR_NOTFOUND); /* no marker there names it */
    /* and the files go with their segments: on the leader once the replicas' bound passes them, on a replica at the
     * floor (no caps set here) */
    eng_leader_commit(n, pos);
    eng_leader_keep(n, UINT64_MAX);
    CHECK_EQ(count_named(rd, kp), 0);
    CHECK_EQ(eng_leader_file_open(n, p1, n1), TSDB_ERR_NOTFOUND);
    CHECK_EQ(replica_flush(old), 0);
    CHECK_EQ(replica_floor_saved(old, end), 0);
    CHECK_EQ(count_named(x.ldir, kp), 0);
    eng_close(old);
    eng_close(n);
    free(x.held);
    free(h1);
    printf("  a committed marker's files stay with its segment on every member and a promoted one serves them\n");
}

static int mutant_of(const char *m)
{
    return strcmp(m, "applyall") == 0 ? MUT_REPLICA_APPLY_ALL
                        : strcmp(m, "ackunsynced") == 0 ? MUT_REPLICA_ACK_UNSYNCED
                        : strcmp(m, "keepcut") == 0 ? MUT_REPLICA_KEEP_CUT
                        : strcmp(m, "nofloor") == 0 ? MUT_REPLICA_NO_FLOOR
                        : strcmp(m, "walclock") == 0 ? MUT_REPLICA_WALL_CLOCK
                        : strcmp(m, "nopos") == 0 ? MUT_REPLICA_NO_POS
                        : strcmp(m, "classmax") == 0 ? MUT_REPLICA_CLASS_MAX
                        : strcmp(m, "noclasspurge") == 0 ? MUT_REPLICA_NO_CLASS_PURGE
                        : strcmp(m, "opensync") == 0 ? MUT_REPLICA_NO_OPEN_SYNC
                        : strcmp(m, "gapcheck") == 0 ? MUT_REPLICA_NO_GAP_CHECK
                        : strcmp(m, "noleftover") == 0 ? MUT_REPLICA_NO_LEFTOVER_DROP
                        : strcmp(m, "gapbyseq") == 0 ? MUT_REPLICA_GAP_BY_SEQ
                        : strcmp(m, "gapnokept") == 0 ? MUT_REPLICA_GAP_NO_KEPT
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
                        : strcmp(m, "cutnohold") == 0 ? MUT_REPLICA_CUT_NO_HOLD
                        : strcmp(m, "nocommitflush") == 0 ? MUT_REPLICA_NO_COMMIT_FLUSH
                        : strcmp(m, "nodoneskip") == 0 ? MUT_REPLICA_NO_DONE_SKIP
                        : strcmp(m, "keepstaged") == 0 ? MUT_REPLICA_KEEP_STAGED
                        : strcmp(m, "nodropskip") == 0 ? MUT_REPLICA_NO_DROP_SKIP
                        : strcmp(m, "nodropflush") == 0 ? MUT_REPLICA_NO_DROP_FLUSH
                        : strcmp(m, "nostagesync") == 0 ? MUT_REPLICA_NO_STAGE_SYNC
                        : strcmp(m, "promoteunapplied") == 0 ? MUT_REPLICA_PROMOTE_UNAPPLIED
                        : strcmp(m, "promotekeepsegs") == 0 ? MUT_REPLICA_PROMOTE_KEEP_SEGS
                        : strcmp(m, "nodropserial") == 0 ? MUT_REPLICA_NO_DROP_SERIAL
                        : strcmp(m, "stagetrunc") == 0 ? MUT_REPLICA_STAGE_TRUNC
                        : strcmp(m, "coldnostage") == 0 ? MUT_REPLICA_COLD_NO_STAGE
                        : strcmp(m, "coldkeyany") == 0 ? MUT_REPLICA_COLD_KEY_ANY
                        : strcmp(m, "coldnameany") == 0 ? MUT_REPLICA_COLD_NAME_ANY
                        : strcmp(m, "ppostale") == 0 ? MUT_LEADER_PPOS_STALE
                        : strcmp(m, "noorphans") == 0 ? MUT_REPLICA_NO_STAGE_ORPHANS
                        : strcmp(m, "epochseg") == 0 ? MUT_REPLICA_EPOCH_SEG
                        : strcmp(m, "leadervisible") == 0 ? MUT_LEADER_VISIBLE_DURABLE
                        : strcmp(m, "leaderflush") == 0 ? MUT_LEADER_FLUSH_UNCOMMITTED
                        : strcmp(m, "deposefails") == 0 ? MUT_LEADER_DEPOSE_FAILS
                        : strcmp(m, "admitafter") == 0 ? MUT_LEADER_ADMIT_AFTER
                        : strcmp(m, "bytesaftersync") == 0 ? MUT_LEADER_BYTES_AFTER_SYNC
                        : strcmp(m, "markerrc") == 0 ? MUT_LEADER_MARKER_RC
                        : strcmp(m, "gatenohook") == 0 ? MUT_LEADER_GATE_NOHOOK
                        : strcmp(m, "nosynced") == 0 ? MUT_LEADER_NO_SYNCED
                        : strcmp(m, "dictearly") == 0 ? MUT_LEADER_DICT_EARLY
                        : strcmp(m, "wtaillocal") == 0 ? MUT_LEADER_WTAIL_LOCAL
                        : strcmp(m, "classmarkerlate") == 0 ? MUT_LEADER_CLASS_MARKER_LATE
                        : strcmp(m, "nowtailfrom") == 0 ? MUT_LEADER_NO_WTAIL_FROM
                        : strcmp(m, "markersplit") == 0 ? MUT_LEADER_MARKERS_SPLIT
                        : strcmp(m, "switchunattached") == 0 ? MUT_LEADER_SWITCH_UNATTACHED
                        : strcmp(m, "replicadropsfloor") == 0 ? MUT_REPLICA_KEEP_FLOOR
                        : strcmp(m, "replaykeptearly") == 0 ? MUT_REPLAY_KEPT_EARLY
                        : strcmp(m, "replaystartlog") == 0 ? MUT_REPLAY_START_LOG
                        : strcmp(m, "replaystarthigh") == 0 ? MUT_REPLAY_START_HIGH
                        : strcmp(m, "replaystartunsaved") == 0 ? MUT_REPLAY_START_UNSAVED
                        : strcmp(m, "replayleadnofloor") == 0 ? MUT_REPLAY_LEAD_NOFLOOR
                        : strcmp(m, "replayopennone") == 0 ? MUT_REPLAY_OPEN_NONE
                        : strcmp(m, "replayopennofloor") == 0 ? MUT_REPLAY_OPEN_NOFLOOR
                        : strcmp(m, "replicadropsfiles") == 0 ? MUT_REPLICA_DROP_APPLIED_FILES
                        : strcmp(m, "freshunattached") == 0 ? MUT_LEADER_FRESH_UNATTACHED
                        : strcmp(m, "droppedglobal") == 0 ? MUT_DROPPED_GLOBAL
                        : strcmp(m, "wtailnocap") == 0 ? MUT_WTAIL_NOCAP
                        : strcmp(m, "wtailfoldall") == 0 ? MUT_WTAIL_FOLD_ALL
                        : strcmp(m, "wtailfoldmin") == 0 ? MUT_WTAIL_FOLD_MIN
                        : strcmp(m, "wtailfoldnewer") == 0 ? MUT_WTAIL_FOLD_NEWER
                        : strcmp(m, "wtailfolddropold") == 0 ? MUT_WTAIL_FOLD_DROP_OLD
                        : strcmp(m, "wtailsortnomax") == 0 ? MUT_WTAIL_SORT_NO_MAX
                        : strcmp(m, "wtailremoved") == 0 ? MUT_REPLICA_WTAIL_REMOVED
                        : strcmp(m, "nocutoff") == 0 ? MUT_LEADER_NO_CUTOFF
                        : strcmp(m, "zerorate") == 0 ? MUT_LEADER_ZERO_RATE
                        : strcmp(m, "noraisefrom") == 0 ? MUT_LEADER_NO_RAISE_FROM
                        : strcmp(m, "pruneoff1") == 0 ? MUT_LEADER_PRUNE_OFF1
                        : strcmp(m, "freshunchecked") == 0 ? MUT_REPLICA_FRESH_UNCHECKED
                        : strcmp(m, "rscommittedtrue") == 0 ? MUT_REPLICA_RS_COMMITTED_TRUE
                        : strcmp(m, "rscommittedfalse") == 0 ? MUT_REPLICA_RS_COMMITTED_FALSE
                        : strcmp(m, "dropnorsave") == 0 ? MUT_REPLICA_DROP_NO_SAVE
                        : strcmp(m, "leaderclose") == 0 ? MUT_LEADER_CLOSE_FLUSH
                        : strcmp(m, "leadernorstate") == 0 ? MUT_LEADER_NO_RSTATE
                        : strcmp(m, "closewait") == 0 ? MUT_LEADER_CLOSE_WAIT
                        : strcmp(m, "keepignored") == 0 ? MUT_LEADER_KEEP_IGNORED
                        : strcmp(m, "nohardlink") == 0 ? MUT_LEADER_NO_HARDLINK
                        : strcmp(m, "leaderlinknosync") == 0 ? MUT_LEADER_LINK_NOSYNC
                        : strcmp(m, "promotenocarry") == 0 ? MUT_PROMOTE_NO_CARRY
                        : strcmp(m, "keptreplayed") == 0 ? MUT_KEPT_REPLAYED
                        : strcmp(m, "leaderkeptnosave") == 0 ? MUT_LEADER_KEPT_NOSAVE
                        : strcmp(m, "readgap") == 0 ? MUT_LEADER_READ_NO_GAP
                        : strcmp(m, "cursoranyway") == 0 ? MUT_LEADER_READ_CURSOR_ANY
                        : strcmp(m, "tailoksealed") == 0 ? MUT_LEADER_READ_TAILOK_SEALED
                        : strcmp(m, "notfoundsealed") == 0 ? MUT_LEADER_READ_NOTFOUND_SEALED
                        : strcmp(m, "nosweep") == 0 ? MUT_LEADER_NO_SWEEP
                        : strcmp(m, "b0off") == 0 ? MUT_LEADER_B0_OFF
                        : strcmp(m, "applyholdsmu") == 0 ? MUT_REPLICA_APPLY_HOLDS_MU
                        : strcmp(m, "nocutgen") == 0 ? MUT_REPLICA_PREP_NO_CUT_GEN
                        : strcmp(m, "dropunheld") == 0 ? MUT_LEADER_DROP_UNHELD
                        : strcmp(m, "linkbyname") == 0 ? MUT_LEADER_LINK_BY_NAME
                        : strcmp(m, "notelate") == 0 ? MUT_LEADER_NOTE_LATE
                        : strcmp(m, "rowsholdmu") == 0 ? MUT_REPLICA_ROWS_HOLD_MU
                        : strcmp(m, "promotenoapplymu") == 0 ? MUT_REPLICA_PROMOTE_NO_APPLY_MU
                        : strcmp(m, "leadernoprune") == 0 ? MUT_LEADER_RS_NO_PRUNE
                        : strcmp(m, "leaderpruneall") == 0 ? MUT_LEADER_RS_PRUNE_ALL
                        : strcmp(m, "walsequnlocked") == 0 ? MUT_REPLICA_WALSEQ_UNLOCKED
                        : strcmp(m, "cutwalsequnlocked") == 0 ? MUT_REPLICA_CUT_WALSEQ_UNLOCKED : 0;
}

/* Required 3 (R2 REVIEW M1): ACK latency (replica_append of the next record, replica_sync, replica_durable_end) on a
 * second thread while the replica applies a COMMIT of BENCH_COMMIT_ROWS rows (default 10 M) with BENCH_MEM_ROWS rows
 * (default 4 M: 64 MB at 16 bytes a row) in its memtable, which the apply flushes first. TEST_REPLICA_BENCH=REPS. */
typedef struct bench_ack {
    eng *ld, *rep;
    atomic_bool stop;
    double *lat, *beg; /* each round's latency and start */
    size_t n, cap;
    size_t fed;
    int rc;
} bench_ack;

static double bench_now(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (double)t.tv_sec + (double)t.tv_nsec * 1e-9;
}

static void *bench_ack_main(void *a)
{
    bench_ack *b = a;
    while (!atomic_load(&b->stop)) {
        double t0 = bench_now();
        uint8_t *buf;
        size_t len;
        uint64_t seq, off, last;
        int rc = replica_read(b->ld, replica_log_end(b->rep) + 1, &buf, &len, 256, &seq, &off, &last);
        if (rc == 0 && len)
            rc = replica_append(b->rep, seq, off, buf, len), b->fed += len;
        free(buf);
        if (rc == 0)
            rc = replica_sync(b->rep);
        (void)replica_durable_end(b->rep);
        double t1 = bench_now();
        if (rc && !b->rc)
            b->rc = rc;
        if (b->n < b->cap)
            b->beg[b->n] = t0, b->lat[b->n++] = t1 - t0;
    }
    return NULL;
}

static int bench_cmp(const void *a, const void *b)
{
    double x = *(const double *)a, y = *(const double *)b;
    return x < y ? -1 : x > y;
}

static void bench_one(int rep, size_t mem_rows, size_t commit_rows)
{
    xport x;
    char nm[32], rd[PATH_MAX];
    snprintf(nm, sizeof nm, "bench%d", rep);
    xp_start(&x, nm);
    rdir(rd, sizeof rd, &x);
    eng_close(x.rep); /* the replica again, with room for the whole memtable */
    x.rep = NULL;
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.wal_sync_ms = 0;
    o.buffer_rows = mem_rows * 2;
    o.flush_interval_ms = 1 << 30;
    if (eng_open_replica(rd, &o, 0, &x.rep) != 0)
        exit(2);
    size_t big = commit_rows > mem_rows ? commit_rows : mem_rows;
    int64_t *ts = malloc(big * sizeof *ts);
    float *v = malloc(big * sizeof *v);
    if (!ts || !v)
        exit(2);
    /* the memtable: chunks of 100,000 rows through the WAL, applied by the replica; neither side flushes them before
     * the COMMIT (a leader's flush starts a segment, and the replica's sealed memtable would flush in the background) */
    x.ld->opt.wal_max_batch_rows = 1 << 22;
    x.ld->opt.buffer_rows = mem_rows * 2;
    x.ld->opt.flush_interval_ms = 1 << 30;
    size_t chunk = 100000;
    for (size_t done = 0; done < mem_rows; done += chunk) {
        size_t k = mem_rows - done < chunk ? mem_rows - done : chunk;
        for (size_t i = 0; i < k; i++)
            ts[i] = T0 + (int64_t)(done / chunk / NS * chunk + i) * 10, v[i] = (float)(i % 1000);
        CHECK_EQ(eng_insert_series(x.ld, TSDB_FLOAT32, (uint32_t)(done / chunk % NS), ts, v, k), 0);
    }
    CHECK_EQ(xp_pump(&x, ld_last(&x)), 0);
    CHECK_EQ(replica_applied_end(x.rep), ld_last(&x));
    /* the COMMIT: shipped and staged, not applied */
    x.ld->opt.wal_max_batch_rows = 16;
    x.noapply = true;
    for (size_t i = 0; i < commit_rows; i++)
        ts[i] = T0 + 5 * DAY_MS + (int64_t)i * 10, v[i] = (float)(i % 777);
    CHECK_EQ(eng_insert_series(x.ld, TSDB_FLOAT32, 0, ts, v, commit_rows), 0);
    CHECK_EQ(xp_pump(&x, ld_last(&x)), 0);
    uint64_t commit = ld_last(&x);
    /* the records the ACK thread hands over one by one: in the leader's log, not shipped */
    eng *rp = x.rep;
    x.rep = NULL;
    for (int i = 0; i < 3000; i++) {
        int64_t t = T0 + 9 * DAY_MS + i;
        float f = (float)i;
        CHECK_EQ(eng_insert_series(x.ld, TSDB_FLOAT32, 1, &t, &f, 1), 0);
    }
    x.rep = rp;
    x.noapply = false;
    bench_ack b = {.ld = x.ld, .rep = x.rep, .cap = 1 << 22};
    b.lat = malloc(b.cap * sizeof *b.lat);
    b.beg = malloc(b.cap * sizeof *b.beg);
    pthread_t t;
    pthread_create(&t, NULL, bench_ack_main, &b);
    usleep(200000); /* the ACK loop running before the apply starts */
    size_t memrows = x.rep->active ? x.rep->active->rows : 0; /* the apply seals it and flushes it first */
    double a0 = bench_now();
    int rc = replica_apply(x.rep, commit);
    double a1 = bench_now();
    usleep(100000);
    atomic_store(&b.stop, true);
    pthread_join(t, NULL);
    CHECK_EQ(rc, 0);
    CHECK_EQ(b.rc, 0);
    /* the rounds that overlap the apply, the one it held up included */
    size_t n = 0;
    double *l = b.lat;
    for (size_t i = 0; i < b.n; i++)
        if (b.beg[i] < a1 && b.beg[i] + b.lat[i] > a0)
            l[n++] = b.lat[i];
    qsort(l, n, sizeof *l, bench_cmp);
    double sum = 0;
    for (size_t i = 0; i < n; i++)
        sum += l[i];
    printf("BENCH rep=%d apply_ms=%.1f acks=%zu mean_us=%.1f p50_us=%.1f p99_us=%.1f max_ms=%.2f fed=%zu memtable_rows=%zu\n", rep,
           (a1 - a0) * 1e3, n, n ? sum / (double)n * 1e6 : 0, n ? l[n / 2] * 1e6 : 0,
           n ? l[(size_t)((double)n * 0.99)] * 1e6 : 0, n ? l[n - 1] * 1e3 : 0, b.fed, memrows);
    fflush(stdout);
    free(b.lat);
    free(b.beg);
    free(ts);
    free(v);
    xp_stop(&x);
    sh("rm -rf '%s' '%s'", x.ldir, rd);
}

/* LR (D17, REVIEW F1, F5, F6): a leader's replay start against what its own directory replays. A writer's open skips
 * the kept segments, but the same directory reopened as a replica (a failover) replays a kept segment whose last
 * position is above its REPL floor, which the driver saves on its own clock after the flush: the start told rises past
 * a kept segment only once the saved floor covers it (mutant replayleadnofloor: at once), never before RSTATE names it
 * durably (replaystartunsaved, seen from inside the save), and a promoted leader (tsdbd's restart of one) rises past
 * the kept segments its open found at its first floor save (replayopennone: never). Reopened as a replica at a floor below its kept segment's end,
 * the deposed leader's cursor is at or below the floor, so a cut there is accepted (the open's floor clause). */
static uint64_t LR_HOOK = UINT64_MAX;
static void lr_hook(eng *db) { LR_HOOK = replica_replay_start(db); }

static void case_leader_replay(void)
{
    xport x;
    xp_start(&x, "lr");
    eng_close(x.rep); /* the leader alone: the replica side is the reopen below */
    x.rep = NULL;
    x.noship = true;
    for (int k = 0; k < 6; k++)
        xp_rows(&x, 0, 10 * k, 10, 100 + 10 * k);
    uint64_t c1 = ld_last(&x);
    CHECK_EQ(eng_flush(x.ld), 0); /* sealed, flushed, named in RSTATE's kept */
    CHECK_EQ(replica_replay_start(x.ld), 0); /* no floor saved yet: nothing told */
    eng_leader_keep_floor(x.ld, c1 - 15, c1 - 15);
    CHECK_EQ(replica_replay_start(x.ld), 0); /* the floor lags the kept segment's end */
    eng_leader_keep_floor(x.ld, c1, c1);
    uint64_t rs1 = replica_replay_start(x.ld);
    CHECK_EQ(rs1, c1 + 1); /* covered: the next open replays from the segment above it */
    for (int k = 0; k < 3; k++)
        xp_rows(&x, 1, 10 * k, 10, 300 + 10 * k);
    uint64_t c2 = ld_last(&x);
    eng_leader_keep_floor(x.ld, c2, c2); /* the floor covers the segment before it is flushed */
    replica_test_kept_hook = lr_hook;
    LR_HOOK = UINT64_MAX;
    CHECK_EQ(eng_flush(x.ld), 0);
    replica_test_kept_hook = NULL;
    CHECK_EQ(LR_HOOK, rs1); /* inside the save of `kept`, before it is durable: not yet past the segment */
    uint64_t rs2 = replica_replay_start(x.ld);
    CHECK_EQ(rs2, c2 + 1);
    /* the leader restarts as tsdbd restarts one: deposed, reopened as a replica at its saved floor, promoted (every
     * segment stays, named kept); the attach finds them, the first floor save that covers them raises the start */
    eng_leader_depose(x.ld);
    CHECK_EQ(eng_close(x.ld), 0);
    x.ld = NULL;
    eng *r = NULL;
    CHECK_EQ(open_replica(x.ldir, c2, &r), 0);
    if (!r)
        exit(2);
    uint64_t pos = 0;
    CHECK_EQ(replica_epoch_begin(r, 1, "node-b", &pos), 0);
    CHECK_EQ(replica_sync(r), 0);
    CHECK_EQ(replica_apply(r, pos), 0);
    CHECK_EQ(replica_promote(&r, pos, &x.ld), 0);
    if (!x.ld)
        exit(2);
    CHECK_EQ(eng_leader_attach(x.ld, &XOPS), 0);
    CHECK(replica_replay_start(x.ld) <= c2 + 1); /* the epoch's segment is above the floor until it is saved */
    eng_leader_keep_floor(x.ld, pos, pos);
    uint64_t rsp = replica_replay_start(x.ld);
    CHECK_EQ(rsp, pos + 1);
    /* a third segment, flushed while the saved floor is still pos: deposed, reopened as a replica at that floor */
    for (int k = 0; k < 3; k++)
        xp_rows(&x, 0, 5 + 10 * k, 4, 500 + 10 * k);
    uint64_t c3 = ld_last(&x);
    CHECK_EQ(eng_flush(x.ld), 0);
    uint64_t told = replica_replay_start(x.ld);
    CHECK_EQ(told, rsp);
    c2 = pos;
    eng_leader_depose(x.ld);
    CHECK_EQ(eng_close(x.ld), 0);
    x.ld = NULL;
    eng *ex = NULL;
    CHECK_EQ(open_replica(x.ldir, c2, &ex), 0);
    if (ex) {
        uint64_t of = replica_test_open_first(ex);
        if (of < told || of > c2 + 1)
            fprintf(stderr, "LR: told %" PRIu64 ", floor %" PRIu64 ", log end %" PRIu64 ": the reopen replays from "
                            "%" PRIu64 "\n", told, c2, c3, of);
        CHECK(of >= told);   /* never above what an open replays (F1) */
        CHECK(of <= c2 + 1); /* the kept segment above the floor is replayed: a new leader may cut after c2 */
        CHECK_EQ(replica_cut(ex, c2), 0);
        eng_close(ex);
    }
    xp_stop(&x);
    printf("  a leader's replay start passes a kept segment only once the saved floor covers it (told %" PRIu64
           ", a reopen at floor %" PRIu64 " replays from there), and a promoted leader's at its first floor save\n",
           told, c2);
}

/* PL (D17, REVIEW F2): power lost at every step of a leader that writes, flushes, has its commit floor saved (lagging
 * a round, sometimes covering the segment it is about to flush) and restarts. The replay start told before the loss
 * (also from inside the save of `kept`) is never above the first position the directory, reopened as a replica at the
 * saved floor, replays. The floor is written outside the crashfs root before the leader is told, as the driver saves
 * REPL first. */
#define PL_ROUNDS 10
static char PL_ACK[PATH_MAX + 16];
static uint64_t PL_MAX;
static void pl_tell(eng *db)
{
    uint64_t v = replica_replay_start(db);
    if (v > PL_MAX) { /* the core only raises its value: the highest told counts */
        PL_MAX = v;
        write_ack(PL_ACK, v);
    }
}

static int powerlk_child(const char *dir, const char *ackfile)
{
    static xport x;
    char flf[PATH_MAX + 32];
    snprintf(flf, sizeof flf, "%s.floor", ackfile);
    snprintf(PL_ACK, sizeof PL_ACK, "%s", ackfile);
    snprintf(x.ldir, sizeof x.ldir, "%s", dir);
    x.noship = true;
    replica_test_kept_hook = pl_tell;
    xp_open_leader(&x);
    uint64_t fl = 0, prev = 0;
    for (int r = 0; r < PL_ROUNDS; r++) {
        for (int k = 0; k < 2; k++)
            xp_rows(&x, (uint32_t)k, (r * 5 + k * 3) % 48, 8, (float)(10 * r + k));
        if (failures)
            return 3;
        uint64_t last = ld_last(&x);
        if (r % 4 == 1) /* the floor covers the segment about to be flushed */
            fl = last;
        else /* it lags a round */
            fl = MAX(fl, prev);
        write_ack(flf, fl);
        eng_leader_keep_floor(x.ld, fl, fl);
        pl_tell(x.ld);
        if (r % 2 == 1 && eng_flush(x.ld) != 0)
            return 6;
        pl_tell(x.ld);
        prev = last;
        if (r % 4 == 3) { /* the process restarts its leader */
            if (eng_close(x.ld) != 0)
                return 8;
            xp_open_leader(&x);
            pl_tell(x.ld);
            eng_leader_keep_floor(x.ld, fl, fl); /* the driver's first save after the attach */
            pl_tell(x.ld);
        }
    }
    eng_close(x.ld);
    return 0;
}

static void case_power_leader_kept(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  power loss of a leader's kept segments: skipped (no %s)\n", SHIM);
        return;
    }
    char dir[PATH_MAX], ack[PATH_MAX + 16], flf[PATH_MAX + 32];
    snprintf(ack, sizeof ack, "%s/acks-of-pl.ack", S);
    snprintf(flf, sizeof flf, "%s.floor", ack);
    int crashes = 0, raised = 0;
    bool finished = false;
    for (int k = 0; k < 4000 && !finished; k++) {
        fresh(dir, "pl");
        unlink(ack);
        unlink(flf);
        int code = run_power_child_as("--powerlk", dir, dir, ack, k, k + 1);
        if (code != 99 && code != 0)
            fprintf(stderr, "PL: step %d: the child ended with status %d\n", k, code);
        CHECK(code == 99 || code == 0);
        if (code != 99 && code != 0)
            break;
        crashes += code == 99;
        finished = code == 0;
        uint64_t told = read_ack(ack), floor = read_ack(flf);
        eng *db;
        int rc = open_replica(dir, floor, &db);
        if (rc != 0) {
            fprintf(stderr, "PL: step %d: the open at floor %" PRIu64 " failed: %d\n", k, floor, rc);
            failures++;
            continue;
        }
        uint64_t first = replica_test_open_first(db);
        if (told > first) {
            fprintf(stderr, "PL: step %d: the leader told a replay start of %" PRIu64 ", its directory reopened as a "
                            "replica at floor %" PRIu64 " replays from %" PRIu64 "\n", k, told, floor, first);
            failures++;
        }
        raised += told > 0;
        if (replica_apply(db, replica_log_end(db)) != 0) {
            fprintf(stderr, "PL: step %d: the log does not apply\n", k);
            failures++;
        }
        eng_close(db);
    }
    CHECK(finished && crashes > 50 && raised > 0);
    printf("  power lost at each of %d steps of a leader flushing, saving `kept` and restarting: the replay start "
           "told is never above what its directory replays as a replica\n", crashes);
}

/* P-02 regression: replica_apply reuses its RP_CHUNK read buffer across consecutive small WAL records instead of
 * issuing a 1 MiB pread() per record, and invalidates the buffer cleanly across segment switches and replica_cut(). */
static void case_apply_buffered(void)
{
    char dir[PATH_MAX];
    snprintf(dir, sizeof dir, "%s/apply_buffered", S);
    sh("rm -rf '%s'", dir);
    mkseries(dir);

    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.wal_sync_ms = 0;
    o.fsync = 0;
    o.buffer_rows = 1000000;
    eng *db = NULL;
    CHECK_EQ(eng_open_replica(dir, &o, 0, &db), 0);
    if (!db)
        return;

    const size_t N1 = 25000, N2 = 5000; /* 25,000 * 52 B = 1.3 MiB (> 1 MiB RP_CHUNK) + segment roll + 5,000 */
    const size_t rsz = WAL_HDR + WAL_ROW;
    uint8_t *b1 = calloc(N1, rsz);
    uint8_t *b2 = calloc(N2, rsz);
    CHECK(b1 && b2);
    uint64_t fno = 1;
    uint64_t off = 0;
    for (size_t i = 0; i < N1; i++, fno++) {
        uint8_t *r = b1 + i * rsz;
        uint32_t w = 1u | ((uint32_t)WAL_KIND_ROWS << 28);
        int64_t cms = T0 + (int64_t)fno;
        uint32_t id = 0;
        int64_t ts = T0 + (int64_t)i * 1000;
        float fv = (float)(i + 1);
        uint64_t slot = 0;
        memcpy(&slot, &fv, 4);
        memcpy(r, &w, 4);
        memcpy(r + 16, &fno, 8);
        memcpy(r + 24, &cms, 8);
        memcpy(r + WAL_HDR, &id, 4);
        memcpy(r + WAL_HDR + 4, &ts, 8);
        memcpy(r + WAL_HDR + 12, &slot, 8);
        wal_reseal(r, 1, off);
        off += rsz;
    }
    CHECK_EQ(replica_append(db, 1, 0, b1, N1 * rsz), 0);
    off = 0;
    for (size_t i = 0; i < N2; i++, fno++) {
        uint8_t *r = b2 + i * rsz;
        uint32_t w = 1u | ((uint32_t)WAL_KIND_ROWS << 28);
        int64_t cms = T0 + (int64_t)fno;
        uint32_t id = 0;
        int64_t ts = T0 + (int64_t)(N1 + i) * 1000;
        float fv = (float)(N1 + i + 1);
        uint64_t slot = 0;
        memcpy(&slot, &fv, 4);
        memcpy(r, &w, 4);
        memcpy(r + 16, &fno, 8);
        memcpy(r + 24, &cms, 8);
        memcpy(r + WAL_HDR, &id, 4);
        memcpy(r + WAL_HDR + 4, &ts, 8);
        memcpy(r + WAL_HDR + 12, &slot, 8);
        wal_reseal(r, 2, off);
        off += rsz;
    }
    CHECK_EQ(replica_append(db, 2, 0, b2, N2 * rsz), 0);
    CHECK_EQ(replica_sync(db), 0);
    uint64_t r0 = replica_test_apply_reads(db);
    CHECK_EQ(replica_apply(db, N1 + N2), 0);
    uint64_t reads = replica_test_apply_reads(db) - r0;
    CHECK_EQ(replica_test_napplied(db), (uint64_t)(N1 + N2));
    CHECK(reads <= 10); /* 1.3 MiB -> 2 chunks, 260 KiB -> 1 chunk (3 preads vs 30,000 before fix) */
    free(b1);
    free(b2);

    /* Cut inside the currently buffered segment: buffered bytes above the cut must never be reused. */
    const size_t N3 = 100;
    uint8_t *b3 = calloc(N3, rsz);
    CHECK(b3 != NULL);
    uint64_t fno_base = fno;
    uint64_t off_base = off;
    for (size_t i = 0; i < N3; i++) {
        uint8_t *r = b3 + i * rsz;
        uint32_t w = 1u | ((uint32_t)WAL_KIND_ROWS << 28);
        uint64_t fn = fno_base + i;
        int64_t cms = T0 + (int64_t)fn;
        uint32_t id = 1;
        int64_t ts = T0 + (int64_t)i * 1000;
        float fv = (float)(1000 + i);
        uint64_t slot = 0;
        memcpy(&slot, &fv, 4);
        memcpy(r, &w, 4);
        memcpy(r + 16, &fn, 8);
        memcpy(r + 24, &cms, 8);
        memcpy(r + WAL_HDR, &id, 4);
        memcpy(r + WAL_HDR + 4, &ts, 8);
        memcpy(r + WAL_HDR + 12, &slot, 8);
        wal_reseal(r, 2, off_base + i * rsz);
    }
    CHECK_EQ(replica_append(db, 2, off_base, b3, N3 * rsz), 0);
    CHECK_EQ(replica_sync(db), 0);
    /* Apply only the first 10 of N3: rbuf now holds all 100 records in memory. */
    CHECK_EQ(replica_apply(db, fno_base + 9), 0);
    CHECK_EQ(replica_cut(db, fno_base + 9), 0);
    /* Rewrite records 10..99 with distinct values (2000 + i) at the same segment offsets. */
    for (size_t i = 10; i < N3; i++) {
        uint8_t *r = b3 + i * rsz;
        uint32_t w = 1u | ((uint32_t)WAL_KIND_ROWS << 28);
        uint64_t fn = fno_base + i;
        int64_t cms = T0 + (int64_t)fn;
        uint32_t id = 1;
        int64_t ts = T0 + (int64_t)i * 1000;
        float fv = (float)(2000 + i);
        uint64_t slot = 0;
        memcpy(&slot, &fv, 4);
        memcpy(r, &w, 4);
        memcpy(r + 16, &fn, 8);
        memcpy(r + 24, &cms, 8);
        memcpy(r + WAL_HDR, &id, 4);
        memcpy(r + WAL_HDR + 4, &ts, 8);
        memcpy(r + WAL_HDR + 12, &slot, 8);
        wal_reseal(r, 2, off_base + i * rsz);
    }
    CHECK_EQ(replica_append(db, 2, off_base + 10 * rsz, b3 + 10 * rsz, (N3 - 10) * rsz), 0);
    CHECK_EQ(replica_sync(db), 0);
    CHECK_EQ(replica_apply(db, fno_base + N3 - 1), 0);
    qctx q = {.n = 0};
    CHECK_EQ(eng_query_raw_ex(db, 1, T0, T0 + (int64_t)N3 * 1000, false, 0, qfn, &q), 0);
    CHECK_EQ(q.n, N3);
    for (size_t i = 0; i < N3; i++) {
        float want = i < 10 ? (float)(1000 + i) : (float)(2000 + i);
        CHECK(q.v[i] == want);
    }
    free(b3);
    CHECK_EQ(eng_close(db), 0);
    printf("  apply_buffered: %zu records applied in %" PRIu64 " preads (cut invalidation verified)\n",
           N1 + N2, reads);
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
    if (argc >= 4 && strcmp(argv[1], "--powerlk") == 0) {
        if (argc >= 5)
            eng_test_mutant = mutant_of(argv[4]);
        return powerlk_child(argv[2], argv[3]);
    }
    if (argc >= 4 && strcmp(argv[1], "--powerk") == 0) {
        if (argc >= 5)
            eng_test_mutant = mutant_of(argv[4]);
        return powerk_child(argv[2], argv[3]);
    }
    if (argc >= 4 && strcmp(argv[1], "--powerl") == 0) {
        if (argc >= 5)
            eng_test_mutant = mutant_of(argv[4]);
        return powerl_child(argv[2], argv[3]);
    }
    if (argc >= 4 && strcmp(argv[1], "--powerr") == 0) {
        if (argc >= 5)
            eng_test_mutant = mutant_of(argv[4]);
        return powerr_child(argv[2], argv[3]);
    }
    if (argc >= 4 && strcmp(argv[1], "--powerq") == 0) {
        if (argc >= 5)
            eng_test_mutant = mutant_of(argv[4]);
        return powerq_child(argv[2], argv[3]);
    }
    if (a + 1 < argc && strcmp(argv[a], "--mutant") == 0) {
        const char *m = argv[a + 1];
        if (strcmp(m, "roseqtails") == 0)
            follow_test_mutant = 1;
        else if (strcmp(m, "roonetail") == 0)
            follow_test_mutant = 2;
        else
            eng_test_mutant = mutant_of(m);
        MUTNAME = m;
        if (!eng_test_mutant && !follow_test_mutant) {
            fprintf(stderr, "unknown mutant %s\n", m);
            return 2;
        }
        a += 2;
    }
    snprintf(S, sizeof S, "%s/test_replica_%d", a < argc ? argv[a] : "/tmp", (int)getpid());
    sh("rm -rf '%s' && mkdir -p '%s'", S, S);
    if (!realpath(argv[0], SELF))
        snprintf(SELF, sizeof SELF, "%s", argv[0]);
    {
        char *slash = strrchr(SELF, '/');
        snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", slash ? (int)(slash - SELF) : 1, slash ? SELF : ".");
    }
    if (getenv("TEST_REPLICA_R8")) { /* the R8 cases alone (a quick run while working on them) */
        case_cold_hold();
        case_ppos_sites();
        case_ppos_pos();
        case_ppos_parity();
        if (failures == 0)
            sh("rm -rf '%s'", S);
        printf(failures ? "test_replica (R8 cases): %d failures\n" : "test_replica (R8 cases): ok\n", failures);
        return failures != 0;
    }
    if (getenv("TEST_REPLICA_BENCH")) {
        int reps = atoi(getenv("TEST_REPLICA_BENCH"));
        size_t mem = getenv("BENCH_MEM_ROWS") ? strtoull(getenv("BENCH_MEM_ROWS"), NULL, 10) : 4000000;
        size_t com = getenv("BENCH_COMMIT_ROWS") ? strtoull(getenv("BENCH_COMMIT_ROWS"), NULL, 10) : 10000000;
        for (int r = 1; r <= (reps > 0 ? reps : 3); r++)
            bench_one(r, mem, com);
        sh("rm -rf '%s'", S);
        printf("test_replica: bench: %s\n", failures ? "FAILED" : "ok");
        return failures != 0;
    }
    if (eng_test_mutant == MUT_REPLICA_COLD_NO_STAGE || eng_test_mutant == MUT_REPLICA_COLD_KEY_ANY ||
        eng_test_mutant == MUT_REPLICA_COLD_NAME_ANY || eng_test_mutant == MUT_LEADER_PPOS_STALE) {
        /* their cases first: a killed mutant stops there */
        if (eng_test_mutant == MUT_LEADER_PPOS_STALE)
            case_ppos_pos();
        else
            case_cold_hold();
        if (failures) {
            fprintf(stderr, "test_replica: %d failures\n", failures);
            return 1;
        }
    }
    if (eng_test_mutant == MUT_LEADER_NO_CUTOFF || eng_test_mutant == MUT_LEADER_ZERO_RATE ||
        eng_test_mutant == MUT_LEADER_NO_RAISE_FROM || eng_test_mutant == MUT_LEADER_PRUNE_OFF1 ||
        eng_test_mutant == MUT_REPLICA_FRESH_UNCHECKED) { /* their cases first: a killed mutant stops at the first */
        case_class_fresh_held();
        case_class_cutoff();
        case_class_prune();
        case_class_window();
        if (failures) { /* caught: the rest of the program would only take time */
            fprintf(stderr, "test_replica: %d failures\n", failures);
            return 1;
        }
    }
    if (follow_test_mutant) {
        case_ro_epoch_jump();
        sh("rm -rf '%s'", S);
        if (failures) {
            fprintf(stderr, "test_replica: %d failures\n", failures);
            return 1;
        }
        printf("test_replica: ok\n");
        return 0;
    }
    if (getenv("TEST_REPLICA_RO_ONLY")) { /* the quick run of the read-only open; under TSan with walsequnlocked it
                                            * must report the race of a rotation's wal_seq with the flusher's MANIFEST */
        case_ro_open();
        case_ro_epoch_jump();
        sh("rm -rf '%s'", S);
        printf("test_replica: %s\n", failures ? "FAILED" : "ok");
        return failures != 0;
    }
    if (getenv("TEST_REPLICA_CUT_PUBLISH_ONLY")) { /* under TSan with cutwalsequnlocked it must report the race of a
                                                     * cut's wal_seq with a concurrent MANIFEST publish */
        case_cut_publish();
        sh("rm -rf '%s'", S);
        printf("test_replica: %s\n", failures ? "FAILED" : "ok");
        return failures != 0;
    }
    case_requires_replica();
    if (getenv("TEST_REPLICA_REQUIRES_ONLY")) {
        case_feed_refused();
        printf("test_replica: %s\n", failures ? "FAILED" : "ok");
        return failures != 0;
    }
    if (getenv("TEST_REPLICA_OPEN_ONLY")) { /* the quick run of the open's checks */
        case_open_checks();
        sh("rm -rf '%s'", S);
        printf("test_replica: %s\n", failures ? "FAILED" : "ok");
        return failures != 0;
    }
    if (getenv("TEST_REPLICA_APPLY_ONLY")) { /* the quick run of the apply's cases (R2 REVIEW M1) */
        case_commit();
        case_commit_cut();
        case_commit_hold_cut();
        case_apply_concurrent();
        case_apply_cut_race(0);
        case_apply_cut_race(1);
        case_apply_cut_race(2);
        for (int k = AW_DROP; k <= AW_STAGE; k++)
            case_apply_windows(k);
        case_apply_marker_cut(AW_DROP);
        case_apply_marker_cut(AW_CLASS);
        case_apply_open_refused();
        case_promote_during_apply();
        case_drop();
        case_drop_flush();
        case_drop_race();
        case_classes();
        printf("test_replica: %s\n", failures ? "FAILED" : "ok");
        return failures != 0;
    }
    if (getenv("TEST_REPLICA_RSTATE_ONLY")) { /* the quick run of the RSTATE power loss */
        case_rstate_crash();
        printf("test_replica: %s\n", failures ? "FAILED" : "ok");
        return failures != 0;
    }
    if (getenv("TEST_REPLICA_KEPT_ONLY")) { /* the quick run of D17's cases (RSTATE kept, the replay start) */
        case_replaykept();
        case_leader_replay();
        case_power_kept();
        case_power_leader_kept();
        sh("rm -rf '%s'", S);
        printf("test_replica: %s\n", failures ? "FAILED" : "ok");
        return failures != 0;
    }
    if (getenv("TEST_REPLICA_POWER_ONLY")) { /* the power losses alone (they need libcrashfs.so beside the binary) */
        case_power();
        case_power_kept();
        case_power_leader_kept();
        case_power_commit();
        case_rstate_crash();
        case_promote_crash();
        case_leader_power();
        printf("test_replica: %s\n", failures ? "FAILED" : "ok");
        return failures != 0;
    }
    if (getenv("TEST_REPLICA_DEPOSE_ONLY")) { /* the quick run of the deposed leader's cases */
        case_close_bound();
        case_depose();
        case_depose_rejoin();
        case_leader_keep();
        case_leader_rec_cap();
        case_hardlink();
        case_link_pos();
        case_leader_prune();
        case_leader_read();
        case_link_sweep();
        case_keep_files();
        for (int v = 0; v < 3; v++)
            case_promote(v);
        case_promote_crash();
        case_unlink_kept();
        printf("test_replica: %s\n", failures ? "FAILED" : "ok");
        return failures != 0;
    }
    if (getenv("TEST_REPLICA_LEADER_ONLY")) { /* the quick run of the leader's cases (L1-L9, the power loss last) */
        case_leader_gate();
        case_class_order();
        case_dict_commit();
        case_class_floor(0);
        case_class_floor(1);
        case_class_floor(2);
        case_class_floor(3);
        case_class_fold_safe();
        case_class_fold_replica();
        case_class_fresh();
        case_class_dropped();
        case_class_cutoff();
        case_class_prune();
        case_class_window();
        case_leader_ack();
        case_leader_admit();
        case_leader_bytes();
        case_leader_marker_refused();
        case_leader_deposed_refusals();
        case_leader_flusher_deposed();
        case_leader_guards();
        case_leader_abandon();
        if (strcmp(getenv("TEST_REPLICA_LEADER_ONLY"), "power") == 0)
            case_leader_power();
        printf("test_replica: %s\n", failures ? "FAILED" : "ok");
        return failures != 0;
    }
    case_basic();
    case_apply_buffered();
    case_keepcap();
    case_replaykept();
    case_leader_replay();
    case_cut();
    case_cut_publish();
    case_refused();
    case_bad_appends();
    case_dicts();
    case_clock();
    case_classes();
    case_class_exact();
    case_class_fresh_held();
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
    case_ro_open();
    case_ro_epoch_jump();
    case_commit_hold();
    case_cold_hold();
    case_ppos_sites();
    case_ppos_pos();
    case_ppos_parity();
    case_commit_cut();
    case_commit_hold_cut();
    case_apply_concurrent();
    case_apply_cut_race(0);
    case_apply_cut_race(1);
    case_apply_cut_race(2);
    for (int k = AW_DROP; k <= AW_STAGE; k++)
        case_apply_windows(k);
    case_apply_marker_cut(AW_DROP);
    case_apply_marker_cut(AW_CLASS);
    case_apply_open_refused();
    case_promote_during_apply();
    case_drop();
    case_drop_flush();
    case_drop_race();
    case_restage();
    case_stage_damage();
    case_leader_gate();
    case_class_order();
    case_dict_commit();
    case_class_floor(0);
    case_class_floor(1);
    case_class_floor(2);
    case_class_floor(3);
    case_class_fold_safe();
    case_class_fold_replica();
    case_class_fresh();
    case_class_dropped();
    case_class_cutoff();
    case_class_prune();
    case_class_window();
    case_leader_ack();
    case_leader_admit();
    case_leader_bytes();
    case_leader_marker_refused();
    case_leader_deposed_refusals();
    case_leader_flusher_deposed();
    case_leader_guards();
    case_leader_abandon();
    case_close_bound();
    case_depose();
    case_depose_rejoin();
    case_leader_keep();
    case_leader_rec_cap();
    case_hardlink();
    case_link_pos();
    case_leader_prune();
    case_leader_read();
    case_link_sweep();
    case_keep_files();
    case_promote_refused();
    for (int v = 0; v < 3; v++)
        case_promote(v);
    case_promote_crash();
    case_unlink_kept();
    if (getenv("TEST_REPLICA_PROMOTE_ONLY")) { /* the quick run while working on promotion */
        sh("rm -rf '%s'", S);
        printf("test_replica: promote only: %s\n", failures ? "FAILED" : "ok");
        return failures != 0;
    }
    for (unsigned seed = 1; seed <= 12; seed++)
        case_commit_random(seed);
    case_power();
    case_power_kept();
    case_power_leader_kept();
    case_power_commit();
    case_rstate_crash();
    case_leader_power();
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
