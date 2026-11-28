/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * A leader's settle of a cluster ec volume (design-cluster-data.md 2.3 steps 3-5; the R8 plan's D3, D4, D18, D21, D22).
 * On one server the swap's input check under db->lock is enough (R7 D1). A leader must make its replicas retire the
 * same rows, so the settle needs one cut Z such that the objects and the late parts Y hold exactly the rows of the key
 * at Z, and nothing writes into the key between Z and the MK_COLD marker that names them:
 *
 *  1. The parts I are pinned by eng_settle (`settling`: no merge, no TTL pass, no second settle takes them) and each
 *     is put as an object under the volume's epoch, with no engine lock held (src/settle.c ecsettle_put).
 *  2. The stripe check, before the gate and never under it (a gate held across a store's outage would hold every
 *     write into the key's days for as long): every position UP and writable, the epoch and the stripe version the
 *     ops say now are those the objects were put under. A settle that finds a position not UP before its first put
 *     gives up at once (UNAVAILABLE, nothing put): the one settle thread of the tier never waits for an outage that
 *     was there before it began, so every other volume's settle goes on and the key is tried again an hour later.
 *     Only a position that drops during the puts is waited for, at most settle_wait_max_s (polled), then given up:
 *     the objects go to r7-gc as orphans, the policy tries the key again an hour later.
 *  3. The gate: lw_enter with a fence over every bucket of series and the key's whole time range (D3: a row of any
 *     series, of any class, into those days waits, which is cheap because a settled key's days are at rest), taking
 *     jr_serial (D4: every large write and drop of the volume waits, so marker order is effect order). L = the log end
 *     right after it. If the memtable may hold rows of the key (active rows overlapping the key's range, or a flush
 *     pending), mt_flush rotates the segment at the cut and flushes through it (a leader's flush waits for its own
 *     commit point, C7); F = wal_flushed after it, which the replicas flush through before they carry the marker out.
 *     Then the commit point must pass L: every row any member may still hold below the cut is committed, so a promoted
 *     replica cannot hold a row of the key above Z that the leader's Y lacks. Z = the log end after that wait (rows
 *     appended since are outside the fence: other days). The stripe check again, without waiting: failed -> lw_leave,
 *     the objects to r7-gc.
 *  4. Y = every non-.ec part of the key listed now that is not in I (late parts: small under D21's policy), pinned
 *     like I (a merging one gives up: STOPPED).
 *  5. The .ec files are written with ppos = Z and the stripe version (ecpart.c record_valid admits both only on a
 *     cluster ec volume), and jr_cold (src/replica.c, r8a2) ships them and Y's files, links them as a COMMIT's files,
 *     appends MK_COLD {Z, F, stripe version, key, the object records, the files} and waits for its commit point.
 *     INDETERMINATE (deposed meanwhile): nothing is cleaned up (a marker may name the objects; the reopen as a replica
 *     decides) and the engine keeps running. Any other failure after the append failed the engine (jr_cold's rules);
 *     before it, the temporaries are removed and the objects go to r7-gc.
 *  6. ec_swap_commit(I) as R7 does (D2 names); r8h's hooks inside it append the leader's RSTATE `doing` before the
 *     intent's commit point and `done` after (they find the COLD through eccold_swap_cur). A refusal after the marker
 *     committed fails the engine (the replicas apply the marker; this leader must not answer as if it had not). Then
 *     lw_leave.
 * What the gate costs: from lw_enter to lw_leave every large write, replace, delete and drop of the volume waits
 * (jr_serial) and every small write into the key's days waits (the fence), while the settle thread (nice 19) flushes
 * the memtable when it holds rows of the key, waits for the commit point, writes and syncs the np .ec files (each
 * its header, index and record: a few hundred KB, never the object), sums every Y part's map (late parts: small
 * under D21), ships and links those files (jr_cold) and commits the marker, then runs the swap (2n renames, two
 * syncs). The .ec files stay inside the gate because their record carries ppos = Z, which is known only there, and
 * Y is known only there; the objects themselves (the bulk of the work) are put before it. eccold_cut.hold_us is
 * the measured hold (test_eccold prints it for its concurrent case).
 * TTL and drops cannot take the key between the marker and the swap: eng_drop_before takes jr_serial, and
 * ttl_select_locked skips a key with a settling part on a cluster leader (src/merge.c).
 *
 * Lock order (for PLAN.md "Lock order R8 adds"): the settle thread takes jr_serial (lw_enter) first, then, as a large
 * write does, flush_lock > cls_rw (mt_flush), lead_mu (eng_leader_wait), the WAL writer role and buf_lock (jr_cold's
 * append), publish_lock > latest_rw > db->lock (the swap). No engine lock is held across a put; the stripe wait holds
 * none.
 */
#define _GNU_SOURCE
#include "internal.h"
#include "eccold.h"
#include "coldops.h"
#include "eccache.h"
#include "ecdigest.h"
#include "ecgc.h"
#include "eckeep.h"
#include "ecpart.h"
#include "ecskip.h"
#include "ecswap.h"
#include "marker.h"
#include "settle.h"
#include "tier.h"

#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

void (*_Atomic eccold_marker_hook)(struct eng *db, uint64_t pos);
void (*_Atomic eccold_test_hook)(struct eng *db, int step);
int eccold_mutant;
_Atomic size_t eccold_test_files_max;

static pthread_mutex_t last_mu = PTHREAD_MUTEX_INITIALIZER;
static eccold_cut last_cut;

int eccold_mutant_parse(const char *name)
{
    static const char *const names[] = {"",            "cutnogate",      "cutnowait",        "ttlsettling",
                                        "cutnorotate", "fencekeyonly",   "stripewaitfenced", "degtwo",
                                        "degnoupcheck", "degnopositions", "retiredunnamed"};
    for (int i = 1; i < (int)(sizeof names / sizeof *names); i++)
        if (strcmp(name, names[i]) == 0)
            return i;
    return -1;
}

#ifdef TSDB_TEST_HOOKS
static pthread_once_t mut_once = PTHREAD_ONCE_INIT;
static char pause_pre_marker_path[512];
static char pause_before_digest_path[512];
static void mut_env(void)
{
    const char *m = getenv("TSDB_ECCOLD_MUTANT");
    if (m && *m) {
        int v = eccold_mutant_parse(m);
        if (v < 0) {
            fprintf(stderr, "tsdb: TSDB_ECCOLD_MUTANT=%s names no mutant: exiting with status 2\n", m);
            _exit(2);
        }
        eccold_mutant = v;
    }
    const char *th = getenv("TSDB_ECCOLD_TEST");
    if (th && *th) {
        char buf[1024];
        snprintf(buf, sizeof buf, "%s", th);
        char *sp = NULL;
        for (char *tok = strtok_r(buf, ",", &sp); tok; tok = strtok_r(NULL, ",", &sp)) {
            if (strncmp(tok, "pause_pre_marker=", 17) == 0)
                snprintf(pause_pre_marker_path, sizeof pause_pre_marker_path, "%s", tok + 17);
            else if (strncmp(tok, "pause_before_digest=", 20) == 0)
                snprintf(pause_before_digest_path, sizeof pause_before_digest_path, "%s", tok + 20);
        }
    }
}

static void pause_pre_marker_hook(void)
{
    if (!pause_pre_marker_path[0] || access(pause_pre_marker_path, F_OK) != 0)
        return;
    fprintf(stderr, "tsdb: test: pause_pre_marker: paused before MK_COLD\n");
    while (access(pause_pre_marker_path, F_OK) == 0) {
        struct timespec ts = {0, 50 * 1000000L};
        nanosleep(&ts, NULL);
    }
    fprintf(stderr, "tsdb: test: pause_pre_marker: resumed before MK_COLD\n");
}

static void pause_before_digest_hook(void)
{
    if (!pause_before_digest_path[0] || access(pause_before_digest_path, F_OK) != 0)
        return;
    fprintf(stderr, "tsdb: test: pause_before_digest: paused before MK_DIGEST\n");
    while (access(pause_before_digest_path, F_OK) == 0) {
        struct timespec ts = {0, 50 * 1000000L};
        nanosleep(&ts, NULL);
    }
    fprintf(stderr, "tsdb: test: pause_before_digest: resumed before MK_DIGEST\n");
}

void eccold_mutant_init(void)
{
    pthread_once(&mut_once, mut_env);
}
#else
static void pause_pre_marker_hook(void) {}
static void pause_before_digest_hook(void) {}
void eccold_mutant_init(void) {}
#endif

void eccold_test_last(eccold_cut *out)
{
    pthread_mutex_lock(&last_mu);
    *out = last_cut;
    pthread_mutex_unlock(&last_mu);
}

static void step(eng *db, int s)
{
    void (*h)(eng *, int) = atomic_load(&eccold_test_hook);
    if (h)
        h(db, s);
}

/* the engine whose COLD this thread is appending (jr_cold), for eccold_after_append */
static __thread struct eng *appending;

void eccold_after_append(struct eng *db, uint64_t fno)
{
    if (appending != db) /* a COMMIT's marker, or another engine's */
        return;
    void (*mh)(eng *, uint64_t) = atomic_load(&eccold_marker_hook);
    if (mh)
        mh(db, fno);
}

/* the COLD being carried out (db->cold while its swap runs) */
struct eccold {
    uint64_t pos, z;
    int64_t key;
};

bool eccold_may_attach(struct eng *db)
{
#ifdef TSDB_EC
    return db->ec_req; /* a cluster ec volume (META requires=ec): its leader settles, its replicas read .ec parts */
#else
    (void)db;
    return false;
#endif
}

int eccold_open(struct eng *db)
{
    (void)db;
    return 0;
}

void eccold_close(struct eng *db)
{
    (void)db;
}

bool eccold_leader(struct eng *db)
{
    return db->ec_cluster && !db->rp && eng_jr(db) && !atomic_load(&db->lead_deposed);
}

bool eccold_swap_cur(struct eng *db, uint64_t *pos, int64_t *key, uint64_t *z)
{
    const struct eccold *c = db->cold;
    if (!c)
        return false;
    if (pos)
        *pos = c->pos;
    if (key)
        *key = c->key;
    if (z)
        *z = c->z;
    return true;
}

uint64_t eccold_active_sv(const struct eng *db)
{
    return db ? atomic_load_explicit(&db->cold_active_sv, memory_order_seq_cst) : 0;
}

static int64_t mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static int64_t mono_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

static uint64_t stripe_of(const coldvol *cv)
{
    return cv->ops->stripe_version ? cv->ops->stripe_version(cv->ctx) : 0;
}

/* every position UP and writable in the epoch and under the stripe version the objects were put by */
static bool stripe_ok(const coldvol *cv, uint64_t epoch, uint64_t sv)
{
    const cold_ops *o = cv->ops;
    if (o->all_up && !o->all_up(cv->ctx))
        return false;
    if (o->writable && !o->writable(cv->ctx))
        return false;
    return coldvol_epoch(cv) == epoch && stripe_of(cv) == sv;
}

/* R9 (E10): for a degraded settle (`written_mask != 0`), verify every position actually written by this settle's
 * objects is still UP and holds a durable non-unsure fence >= epoch under the current epoch and stripe version. For a
 * full settle (`written_mask == 0`), keep R8's stripe_ok() semantics (p_all_up allows unsure stores as UP and does not
 * probe positions unused by k' < k objects). */
static bool stripe_written_ok(const coldvol *cv, uint64_t epoch, uint64_t sv, uint32_t written_mask)
{
    const cold_ops *o = cv->ops;
    if (written_mask == 0 || !o->stripe_up_mask)
        return stripe_ok(cv, epoch, sv);
    if (o->writable && !o->writable(cv->ctx))
        return false;
    if (coldvol_epoch(cv) != epoch || stripe_of(cv) != sv)
        return false;
    uint32_t up = 0;
    unsigned nstripe = 0;
    if (o->stripe_up_mask(cv->ctx, epoch, written_mask, &up, &nstripe) != 0 || nstripe == 0 || nstripe > CHUNK_POS_MAX)
        return false;
    return (up & written_mask) == written_mask;
}

static int stripe_wait(eng *db, const coldvol *cv, uint64_t epoch, uint64_t sv, uint32_t written_mask)
{
    int64_t max_s = cv->set.wait_max_s > 0 ? cv->set.wait_max_s : ECCOLD_WAIT_MAX_S_DEFAULT;
    int64_t until = mono_ms() + max_s * 1000;
    for (;;) {
        if (eng_check(db))
            return TSDB_ERR_IO;
        if (ecsettle_stopping(db) || !eccold_leader(db))
            return TSDB_ERR_STOPPED;
        if (stripe_written_ok(cv, epoch, sv, written_mask))
            return 0;
        if (mono_ms() >= until) {
            fprintf(stderr, "tsdb: %s: settle: the written positions were not all UP under the stripe version and epoch "
                            "the objects were put by within %lld s: given up, the objects go to r7-gc\n",
                    db->dir, (long long)max_s);
            return TSDB_ERR_UNAVAILABLE;
        }
        struct timespec ts = {0, 20 * 1000000L};
        nanosleep(&ts, NULL);
    }
}

/* whether the memtable may hold rows of [lo, hi]: the active one's span, or anything waiting to be flushed */
static bool mt_may_hold(eng *db, int64_t lo, int64_t hi)
{
    pthread_mutex_lock(&db->lock);
    pthread_mutex_lock(&db->buf_lock);
    const memtable *a = db->active;
    bool any = db->nflushing > 0 || (a && a->rows > 0 && a->min_ts <= hi && a->max_ts >= lo);
    pthread_mutex_unlock(&db->buf_lock);
    pthread_mutex_unlock(&db->lock);
    return any;
}

static uint64_t log_end(eng *db)
{
    pthread_mutex_lock(&db->buf_lock);
    uint64_t e = db->fno_next - 1;
    pthread_mutex_unlock(&db->buf_lock);
    return e;
}

/* the fence of the gate: every bucket of series over [lo, hi] (fencekeyonly: only the buckets of I's series) */
static int fence_make(lw_fence *f, int64_t lo, int64_t hi, part **pin, size_t np)
{
    memset(f, 0, sizeof *f);
    int64_t *r = malloc(2 * LW_BUCKETS * sizeof *r);
    if (!r)
        return TSDB_ERR_NOMEM;
    bool keyonly = eccold_mutant == ECCOLD_MUT_FENCEKEYONLY;
    for (size_t b = 0; b < LW_BUCKETS; b++) {
        r[2 * b] = keyonly ? INT64_MAX : lo;
        r[2 * b + 1] = keyonly ? INT64_MIN : hi;
    }
    if (keyonly) /* the bug: a row of another series (another class) into these days passes the gate */
        for (size_t i = 0; i < np; i++)
            for (uint32_t g = 0; g < pin[i]->ng; g++) {
                size_t b = gx_series(pin[i], g) & (LW_BUCKETS - 1);
                r[2 * b] = lo;
                r[2 * b + 1] = hi;
            }
    f->nb = LW_BUCKETS;
    f->r = r;
    f->r_heap = true;
    f->lo = lo;
    f->hi = hi;
    return 0;
}

/* the size and xxh64 of a file of the volume's directory, read whole (an .ec file: header, index, record) */
static int file_sum(eng *db, const char *name, uint64_t *size, uint64_t *sum)
{
    int fd = openat(db->dirfd, name, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return TSDB_ERR_IO;
    struct stat st;
    int rc = 0;
    uint8_t *buf = NULL;
    if (fstat(fd, &st) != 0 || st.st_size <= 0) {
        rc = TSDB_ERR_IO;
    } else if (!(buf = malloc((size_t)st.st_size))) {
        rc = TSDB_ERR_NOMEM;
    } else {
        size_t off = 0;
        while (off < (size_t)st.st_size) {
            ssize_t k = pread(fd, buf + off, (size_t)st.st_size - off, (off_t)off);
            if (k <= 0) { /* a short read is an error, never the end */
                rc = TSDB_ERR_IO;
                break;
            }
            off += (size_t)k;
        }
        if (rc == 0) {
            *size = (uint64_t)st.st_size;
            *sum = xxh64(buf, (size_t)st.st_size, 0);
        }
    }
    free(buf);
    close(fd);
    return rc;
}

static void put_file(uint8_t *files, size_t *len, const char *name, uint64_t size, uint64_t sum, uint64_t hc,
                     uint8_t kind)
{
    mk_file f = {.name = name, .name_len = (uint32_t)strlen(name), .size = size, .xxh64 = sum, .header_check = hc,
                 .kind = kind};
    *len += mk_file_put(files + *len, &f);
}

_Atomic int eccold_test_marker;

/* MK_COLD for the .ec parts ec[0 .. np) (records recs), late parts y[0 .. ny), and retired oids[0 .. nretired),
 * then jr_cold. */
static int cold_marker_ex(eng *db, const eccold_cut *cut, const ec_record *recs, part **ec, size_t np, part **y,
                          size_t ny, const uint8_t *retired, uint32_t nretired, uint64_t *pos)
{
    size_t olen = 0, flen = 0, nf = np + ny, fcap = 0;
    int tm = atomic_load(&eccold_test_marker);
    for (size_t i = 0; i < np; i++) {
        olen += mk_cold_obj_size((uint32_t)ec_record_size(&recs[i]));
        mk_file f = {.name = ec[i]->name, .name_len = (uint32_t)strlen(ec[i]->name)};
        fcap += mk_file_size(&f) + (tm == ECCOLD_TM_DUPEC ? sizeof ec[0]->name : 0);
    }
    for (size_t i = 0; i < ny; i++) {
        mk_file f = {.name = y[i]->name, .name_len = (uint32_t)strlen(y[i]->name)};
        fcap += mk_file_size(&f);
    }
    uint8_t *objs = malloc(olen ? olen : 1), *files = malloc(fcap ? fcap : 1);
    const char **src = calloc(nf ? nf : 1, sizeof *src);
    int rc = objs && files && src ? 0 : TSDB_ERR_NOMEM;
    size_t off = 0;
    for (size_t j = 0; rc == 0 && j < np; j++) {
        size_t i = tm == ECCOLD_TM_REVERSE ? np - 1 - j : j;
        size_t rl = ec_record_size(&recs[i]);
        uint8_t *rb = malloc(rl);
        if (!rb) {
            rc = TSDB_ERR_NOMEM;
            break;
        }
        if ((rc = ec_record_encode_as(&recs[i], rb, rl, true)) == 0)
            off += mk_cold_obj_put(objs + off, recs[i].oid, rb, (uint32_t)rl);
        free(rb);
        uint64_t size = 0, sum = 0;
        size_t fi = tm == ECCOLD_TM_DUPEC && j == 1 ? 0 : i; /* the second .ec file named as the first */
        if (rc == 0 && !ec[fi]->pending)
            rc = TSDB_ERR_INVALID;
        if (rc == 0 && (rc = file_sum(db, ec[fi]->pending, &size, &sum)) == 0) {
            put_file(files, &flen, ec[fi]->name, size, sum, ec[fi]->index_hash, MK_FILE_EC);
            src[j] = ec[fi]->pending;
        }
    }
    for (size_t j = 0; rc == 0 && j < ny; j++) {
        size_t i = tm == ECCOLD_TM_REVERSE ? ny - 1 - j : j;
        put_file(files, &flen, y[i]->name, y[i]->map_len, xxh64(y[i]->map, y[i]->map_len, 0),
                 y[i]->index_hash ^ (tm == ECCOLD_TM_YHC), MK_FILE_Y);
        src[np + j] = y[i]->pending ? y[i]->pending : y[i]->name;
    }
    if (rc == 0) {
        mk m;
        memset(&m, 0, sizeof m);
        m.type = MK_COLD;
        m.span = 1;
        m.u.cold.z = cut->z + (tm == ECCOLD_TM_Z);
        m.u.cold.flush_through = cut->f;
        m.u.cold.stripe_version = cut->stripe_version;
        m.u.cold.key = cut->key;
        m.u.cold.nobjs = (uint32_t)np;
        m.u.cold.nfiles = (uint32_t)nf;
        m.u.cold.nretired = nretired;
        m.u.cold.objs = objs;
        m.u.cold.objs_len = off;
        m.u.cold.files = files;
        m.u.cold.files_len = flen;
        m.u.cold.retired = nretired ? retired : NULL;
        appending = db; /* eccold_after_append: the crash point right after the append (r8l's midcommit) */
        rc = jr_cold(db, &m, src, nf, pos);
        appending = NULL;
    }
    free(objs);
    free(files);
    free(src);
    return rc;
}

static int cold_marker(eng *db, const eccold_cut *cut, const ec_record *recs, part **ec, size_t np, part **y,
                       size_t ny, uint64_t *pos)
{
    return cold_marker_ex(db, cut, recs, ec, np, y, ny, NULL, 0, pos);
}

/* the .ec parts ec[0 .. n) that no marker names: their temporaries removed, their objects to r7-gc */
static void drop_ec(eng *db, part **ec, size_t n, bool discard)
{
    for (size_t i = 0; i < n; i++) {
        if (!ec[i])
            continue;
        if (discard) {
            if (ec[i]->pending)
                unlinkat(db->dirfd, ec[i]->pending, 0);
            ecgc_unlisted_object(db, ec[i]);
        }
        part_unref(ec[i]);
        ec[i] = NULL;
    }
}

/* Y and I still listed (db->lock held): the late parts of key into y (pinned), or STOPPED; INVALID when I and Y are
 * more files than one marker names (*nlate: how many late parts there were), which the policy's pick refuses too
 * (src/ecpolicy.c): parts written into the key between the pick and the gate made it so (a key at rest has few, D21) */
static int take_y_locked(eng *db, int64_t key, part **pin, size_t np, part **y, size_t *ny, size_t *nlate)
{
    *ny = 0;
    *nlate = 0;
    if (partition_dropping(db, key))
        return TSDB_ERR_STOPPED;
    uint64_t lo_i = UINT64_MAX, hi_i = 0;
    for (size_t i = 0; i < np; i++) {
        bool listed = false;
        for (size_t j = 0; j < db->nparts && !listed; j++)
            listed = db->parts[j] == pin[i];
        if (!listed || pin[i]->dropped)
            return TSDB_ERR_STOPPED;
        if (pin[i]->min_block < lo_i)
            lo_i = pin[i]->min_block;
        if (pin[i]->max_block > hi_i)
            hi_i = pin[i]->max_block;
    }
    size_t n = 0;
    for (size_t i = 0; i < db->nparts; i++) {
        part *p = db->parts[i];
        if (p->partition != key || p->dropped)
            continue;
        if (part_is_ec(p)) {
            if (p->max_block > lo_i)
                return TSDB_ERR_STOPPED; /* an .ec part above I would reorder under replicas' fresh numbering */
            continue;
        }
        bool in_i = false;
        for (size_t j = 0; j < np && !in_i; j++)
            in_i = pin[j] == p;
        if (in_i)
            continue;
        if (p->min_block < hi_i)
            return TSDB_ERR_STOPPED; /* a non-.ec part below I cannot be placed in Y above I's .ec parts */
        if (p->merging || p->settling) /* a merge of late parts in flight: its output would be another Y */
            return TSDB_ERR_STOPPED;
        y[n++] = p;
    }
    *nlate = n;
    size_t max = atomic_load(&eccold_test_files_max);
    if (np + n > (max ? max : MK_FILES_MAX))
        return TSDB_ERR_INVALID;
    for (size_t i = 0; i < n; i++) {
        y[i]->settling = true; /* no merge or TTL pass takes it until the swap is done */
        part_ref(y[i]);
    }
    *ny = n;
    return 0;
}

/* After the swap: a failed digest or append changes nothing of the settle (deposed: the next leader's replicas hold
 * their retired copies, ecdigest.h; the engine failed: it acknowledges nothing more) */
static void leader_digest(eng *db, int64_t key, uint64_t z, part **pin, size_t np, part **y, size_t ny,
                          const ec_record *recs)
{
    part **set = malloc((np + ny) * sizeof *set);
    uint8_t *oids = malloc(np * 16 + 1);
    if (!set || !oids) {
        fprintf(stderr, "tsdb: %s: settle: key %lld: no memory for its DIGEST marker\n", db->dir, (long long)key);
    } else {
        memcpy(set, pin, np * sizeof *set);
        if (ny)
            memcpy(set + np, y, ny * sizeof *set);
        for (size_t i = 0; i < np; i++)
            memcpy(oids + 16 * i, recs[i].oid, 16);
        (void)ecdigest_leader(db, key, z, set, np + ny, oids, np);
    }
    free(set);
    free(oids);
}

int eccold_settle(eng *db, int64_t key, part **pin, size_t np, part **ec)
{
    eccold_mutant_init();
    eccold_deg_wait *dw = ecsettle_deg_wait(db);
    if (dw)
        atomic_store_explicit(&dw->waiting, false, memory_order_relaxed);
    coldvol *cv = tier_cold(db);
    if (!cv)
        return TSDB_ERR_UNAVAILABLE;
    if (!np || np > MK_FILES_MAX)
        return TSDB_ERR_INVALID;
    eccold_cut cut;
    memset(&cut, 0, sizeof cut);
    cut.key = key;
    ec_record *recs = calloc(np, sizeof *recs);
    if (!recs)
        return TSDB_ERR_NOMEM;
    part **y = NULL;
    size_t ny = 0, nput = 0; /* nput: objects put that nothing names yet (r7-gc's on failure) */
    bool gated = false;
    lw_fence f;
    memset(&f, 0, sizeof f);
    int rc = 0;
    uint64_t epoch = 0, sv = stripe_of(cv);
    cut.stripe_version = sv;
    int64_t t_gate = 0;
    bool degraded = false;
    uint32_t posmask = 0;
    uint32_t written_union = 0;
    if (cv->ops->stripe_up_mask) {
        /* Note: after a leader change while a stripe member is down, chunkpeer's p_writable() requires every stripe
         * member to hold the new epoch before puts resume (fail-safe: no degraded settle until the epoch is fenced). */
        if ((cv->ops->writable && !cv->ops->writable(cv->ctx)) || coldvol_epoch(cv) == 0) {
            rc = TSDB_ERR_UNAVAILABLE;
            goto out;
        }
        uint32_t up_mask = 0;
        unsigned nstripe = 0;
        if (cv->ops->stripe_up_mask(cv->ctx, 0, 0, &up_mask, &nstripe) != 0 || nstripe == 0 ||
            nstripe > CHUNK_POS_MAX) {
            rc = TSDB_ERR_UNAVAILABLE;
            goto out;
        }
        uint32_t full = (1u << nstripe) - 1u;
        up_mask &= full;
        uint32_t down_mask = full & ~up_mask;
        unsigned nup = (unsigned)__builtin_popcount(up_mask);
        unsigned ndown = nstripe - nup;
        if (ndown == 0) {
            if (dw) {
                dw->down_mask = 0;
                dw->down_since_ms = 0;
                dw->last_seen_ms = 0;
                dw->settled = false;
                dw->one_logged = false;
                dw->two_logged = false;
            }
        } else {
            unsigned max_down = eccold_mutant == ECCOLD_MUT_DEGTWO ? 2u : 1u;
            bool can_deg = cv->set.m >= 2 && cv->ops->put_stream_pos != NULL && ndown <= max_down &&
                           nup >= (unsigned)cv->set.k;
            if (!can_deg || !dw) {
                if (dw) {
                    dw->down_mask = 0;
                    dw->down_since_ms = 0;
                    dw->last_seen_ms = 0;
                    dw->settled = false;
                    dw->one_logged = false;
                    if (ndown >= 2 && cv->set.m >= 2) {
                        if (!dw->two_logged) {
                            fprintf(stderr, "tsdb: %s: settle: %u of %u stripe positions are down (at most 1 may be "
                                            "down for a degraded settle): waiting\n", db->dir, ndown, nstripe);
                            dw->two_logged = true;
                        }
                        atomic_store_explicit(&dw->waiting, true, memory_order_relaxed);
                    }
                }
                rc = TSDB_ERR_UNAVAILABLE;
                goto out;
            }
            int64_t now = mono_ms();
            int64_t max_s = cv->set.wait_max_s > 0 ? cv->set.wait_max_s : ECCOLD_WAIT_MAX_S_DEFAULT;
            int64_t max_ms = max_s * 1000;
            /* Restart the wait clock whenever the down position(s) change or the previous observation is stale
             * (after a degraded settle, an idle gap > 5 s forces a fresh wait; while waiting, a gap > wait_max + 5 s
             * means no polls ran during the interval). */
            int64_t gap_ms = dw->settled ? 5000 : (max_ms + 5000);
            dw->two_logged = false;
            if (dw->down_mask != down_mask || dw->down_since_ms == 0 ||
                (dw->last_seen_ms > 0 && now - dw->last_seen_ms > gap_ms)) {
                dw->down_mask = down_mask;
                dw->down_since_ms = now;
                dw->settled = false;
                dw->one_logged = false;
            }
            dw->last_seen_ms = now;
            if (now - dw->down_since_ms < max_ms) {
                if (!dw->one_logged) {
                    fprintf(stderr, "tsdb: %s: settle: %u of %u stripe position%s down; waiting %lld s before "
                                    "degraded settle\n",
                            db->dir, ndown, nstripe, ndown == 1 ? "" : "s", (long long)max_s);
                    dw->one_logged = true;
                }
                atomic_store_explicit(&dw->waiting, true, memory_order_relaxed);
                rc = TSDB_ERR_UNAVAILABLE;
                goto out;
            }
            degraded = true;
            posmask = up_mask;
            cut.degraded = true;
            fprintf(stderr, "tsdb: %s: settle: %u of %u stripe position%s still down after %lld s: settling degraded "
                            "(%u of %u positions)\n",
                    db->dir, ndown, nstripe, ndown == 1 ? "" : "s", (long long)max_s, nup, nstripe);
        }
    } else if (!stripe_ok(cv, coldvol_epoch(cv), sv)) {
        /* a position already not UP: no put, no wait on the tier's thread */
        rc = TSDB_ERR_UNAVAILABLE;
        goto out;
    }
    /* 1: the objects, no engine lock held */
    for (; nput < np; nput++) {
        uint64_t e = 0;
        uint32_t wmask = 0;
        if (degraded)
            rc = ecsettle_put_pos(db, pin[nput], &recs[nput], &e, posmask, &wmask);
        else
            rc = ecsettle_put(db, pin[nput], &recs[nput], &e);
        if (rc != 0)
            break; /* this one's object went to r7-gc and its record was freed */
        if (degraded) {
            unsigned onpos = (unsigned)recs[nput].k + (unsigned)recs[nput].m;
            uint32_t ofull = onpos < 32 ? ((1u << onpos) - 1u) : ~0u;
            written_union |= (wmask & ofull);
            if ((wmask & ofull) != ofull) {
                recs[nput].positions = eccold_mutant == ECCOLD_MUT_DEGNOPOSITIONS ? ofull : (wmask & ofull);
                cut.positions = recs[nput].positions;
            }
        }
        if (nput == 0) {
            epoch = e;
        } else if (e != epoch) { /* the epoch moved between two puts: the earlier ones are under an older fence */
            nput++;
            rc = TSDB_ERR_UNAVAILABLE;
            break;
        }
    }
    if (rc)
        goto out;
    uint32_t check_mask = degraded ? (written_union ? written_union : posmask) : 0;
    step(db, ECCOLD_STEP_PUT);
    int64_t plo, phi;
    partition_range(db, key, &plo, &phi);
    int64_t hi = phi == INT64_MAX ? INT64_MAX : phi - 1;
    /* 2: the stripe check, before the gate (stripewaitfenced: under it) */
    if (eccold_mutant != ECCOLD_MUT_STRIPEWAITFENCED &&
        (rc = stripe_wait(db, cv, epoch, sv, check_mask)) != 0)
        goto out;
    /* 3: the gate */
    if ((rc = fence_make(&f, plo, hi, pin, np)) != 0)
        goto out;
    bool pending = true;
    if (eccold_mutant != ECCOLD_MUT_CUTNOGATE) {
        pending = lw_enter(db, &f);
        gated = true;
        t_gate = mono_us();
    }
    cut.gated = gated;
    cut.l = log_end(db);
    step(db, ECCOLD_STEP_GATED);
    if (eccold_mutant == ECCOLD_MUT_STRIPEWAITFENCED &&
        (rc = stripe_wait(db, cv, epoch, sv, check_mask)) != 0)
        goto out;
    if (pending && mt_may_hold(db, plo, hi) && eccold_mutant != ECCOLD_MUT_CUTNOROTATE) {
        if ((rc = mt_flush(db)) != 0)
            goto out;
        cut.flushed = true;
    }
    pthread_mutex_lock(&db->lock);
    cut.f = db->wal_flushed;
    pthread_mutex_unlock(&db->lock);
    if (eccold_mutant != ECCOLD_MUT_CUTNOWAIT && (rc = eng_leader_wait(db, cut.l, false)) != 0)
        goto out;
    cut.z = log_end(db);
    if (eccold_mutant != ECCOLD_MUT_DEGNOUPCHECK && !stripe_written_ok(cv, epoch, sv, check_mask)) {
        rc = TSDB_ERR_UNAVAILABLE;
        goto out;
    }
    /* 4: Y, and I still listed */
    pthread_mutex_lock(&db->lock);
    size_t cap = 0;
    for (size_t i = 0; i < db->nparts; i++)
        cap += db->parts[i]->partition == key;
    y = calloc(cap ? cap : 1, sizeof *y);
    size_t nlate = 0;
    rc = y ? take_y_locked(db, key, pin, np, y, &ny, &nlate) : TSDB_ERR_NOMEM;
    pthread_mutex_unlock(&db->lock);
    if (rc == TSDB_ERR_INVALID) { /* the policy tries the key again in an hour (settle_engine: ecpolicy_failed) */
        size_t max = atomic_load(&eccold_test_files_max);
        fprintf(stderr, "tsdb: %s: settle: key %lld had %zu parts to settle and %zu late ones at the cut, more than "
                        "one marker names (%zu): given up, the objects go to r7-gc, tried again in an hour\n",
                db->dir, (long long)key, np, nlate, max ? max : (size_t)MK_FILES_MAX);
    }
    cut.ny = ny;
    for (size_t i = 0; i < ny && i < ECCOLD_TEST_Y_MAX; i++)
        snprintf(cut.y[i], sizeof cut.y[i], "%.63s", y[i]->name);
    if (rc)
        goto out;
    step(db, ECCOLD_STEP_CUT);
    /* 5: the .ec files with the cut, and the marker */
    for (size_t i = 0; i < np; i++) {
        recs[i].ppos = cut.z;
        recs[i].stripe_version = sv;
        if ((rc = ecsettle_index(db, pin[i], &recs[i], &ec[i])) != 0) {
            /* object i went to r7-gc in ecsettle_index; the indexed ones are dropped here, the rest at out */
            drop_ec(db, ec, i, true);
            for (size_t j = i + 1; j < np; j++)
                ecsettle_gc_object(db, &recs[j]);
            nput = 0;
            goto out;
        }
    }
    nput = 0; /* from here on the .ec parts name them */
    cut.nobjs = np;
    step(db, ECCOLD_STEP_PRE_MARKER);
    pause_pre_marker_hook();
    /* R9 (D10): advertise this settle's stripe_version with seq_cst BEFORE the final stripe_version check so
     * cp_objlist_srv(max_version = V) either sees active_sv < V (and retries) or this check sees stripe_of >= V
     * (and aborts); no object of version < V can slip past OBJLIST into ec_swap_commit unseen. */
    atomic_store_explicit(&db->cold_active_sv, sv, memory_order_seq_cst);
    /* E10: just before MK_COLD, verify every written position is still UP in the current epoch. */
    if (eccold_mutant != ECCOLD_MUT_DEGNOUPCHECK && !stripe_written_ok(cv, epoch, sv, check_mask)) {
        fprintf(stderr, "tsdb: %s: settle: a written stripe position dropped before MK_COLD: aborting\n", db->dir);
        drop_ec(db, ec, np, true);
        rc = TSDB_ERR_UNAVAILABLE;
        goto out;
    }
    uint64_t pos = 0;
    rc = cold_marker(db, &cut, recs, ec, np, y, ny, &pos);
    cut.pos = pos;
    if (rc) {
        /* INDETERMINATE (deposed), or the engine failed: a marker may name the objects, so nothing is removed (the
         * reopen as a replica, or the next open, decides); refused before the append: no marker names them */
        drop_ec(db, ec, np, rc != TSDB_ERR_INDETERMINATE && !eng_check(db));
        goto out;
    }
    step(db, ECCOLD_STEP_MARKER);
    /* 6: the swap at the commit point */
    struct eccold cur = {.pos = pos, .z = cut.z, .key = key};
    db->cold = &cur;
    rc = ec_swap_commit(db, pin, ec, np);
    db->cold = NULL;
    if (rc) { /* the replicas carry the marker out: this leader may not go on as if it had not been written */
        if (!eng_check(db))
            eng_fail(db, "a settle's swap was refused after its COLD marker committed", rc);
        drop_ec(db, ec, np, false);
        rc = TSDB_ERR_IO;
    } else if (degraded && dw) {
        dw->settled = true;
        dw->last_seen_ms = mono_ms();
    }
out:
    atomic_store_explicit(&db->cold_active_sv, 0, memory_order_release);
    if (gated) {
        lw_leave(db, &f);
        cut.hold_us = mono_us() - t_gate;
    } else
        free(f.r);
    if (rc == 0) {
        if (degraded && cv->ops->note_missing) {
            for (size_t i = 0; i < np; i++) {
                unsigned onpos = (unsigned)recs[i].k + (unsigned)recs[i].m;
                uint32_t ofull = onpos < 32 ? ((1u << onpos) - 1u) : ~0u;
                uint32_t w = recs[i].positions ? recs[i].positions : ofull;
                for (unsigned p = 0; p < onpos; p++)
                    if (!(w & (1u << p)))
                        cv->ops->note_missing(cv->ctx, recs[i].oid, p);
            }
        }
        /* 7: E26's digest of I and Y and its marker (src/ecdigest.c): the gate released, Y still pinned */
        leader_digest(db, key, cut.z, pin, np, y, ny, recs);
    }
    if (ny) {
        pthread_mutex_lock(&db->lock);
        for (size_t i = 0; i < ny; i++)
            y[i]->settling = false;
        pthread_mutex_unlock(&db->lock);
        for (size_t i = 0; i < ny; i++)
            part_unref(y[i]);
    }
    free(y);
    for (size_t i = 0; i < nput; i++) /* put, never indexed: nothing names them */
        ecsettle_gc_object(db, &recs[i]);
    for (size_t i = 0; i < np; i++)
        ec_record_free(&recs[i]);
    free(recs);
    cut.rc = rc;
    pthread_mutex_lock(&last_mu);
    last_cut = cut;
    pthread_mutex_unlock(&last_mu);
    return rc;
}

/* Under db->lock: verifies pin[0..np) is still contiguous and listed at `base`, satisfies the cluster suffix
 * invariant (no non-.ec part of key below pin[0], no .ec part of key above pin[np-1]), and pins all trailing
 * non-.ec parts of key into y[0..*ny). */
static int take_reencode_y_locked(eng *db, int64_t key, part **pin, size_t np, part **y, size_t *ny)
{
    *ny = 0;
    if (partition_dropping(db, key))
        return TSDB_ERR_STOPPED;
    size_t base = db->nparts;
    for (size_t j = 0; j < db->nparts; j++)
        if (db->parts[j] == pin[0]) {
            base = j;
            break;
        }
    if (base + np > db->nparts)
        return TSDB_ERR_STOPPED;
    for (size_t i = 0; i < np; i++)
        if (db->parts[base + i] != pin[i] || pin[i]->dropped || pin[i]->merging)
            return TSDB_ERR_STOPPED;
    for (size_t i = 0; i < base; i++) {
        part *p = db->parts[i];
        if (p->partition == key && !p->dropped && !part_is_ec(p))
            return TSDB_ERR_STOPPED; /* a non-.ec part below I cannot be placed in Y above new_EC */
    }
    size_t n = 0;
    for (size_t i = base + np; i < db->nparts; i++) {
        part *p = db->parts[i];
        if (p->partition != key || p->dropped)
            continue;
        if (part_is_ec(p))
            return TSDB_ERR_STOPPED; /* an .ec part above I would be out-of-order after replica renumbering */
        if (p->merging || p->settling)
            return TSDB_ERR_STOPPED;
        y[n++] = p;
    }
    size_t max = atomic_load(&eccold_test_files_max);
    if (1 + n > (max ? max : MK_FILES_MAX))
        return TSDB_ERR_INVALID;
    for (size_t i = 0; i < n; i++) {
        y[i]->settling = true;
        part_ref(y[i]);
    }
    *ny = n;
    return 0;
}

int eccold_reencode(eng *db, int64_t key, part **pin, size_t np)
{
    eccold_mutant_init();
    coldvol *cv = tier_cold(db);
    if (!cv)
        return TSDB_ERR_UNAVAILABLE;
    if (!np || np > MK_FILES_MAX)
        return TSDB_ERR_INVALID;
    eccold_cut cut;
    memset(&cut, 0, sizeof cut);
    cut.key = key;
    uint64_t epoch = 0, sv = stripe_of(cv);
    cut.stripe_version = sv;
    if (!stripe_ok(cv, coldvol_epoch(cv), sv))
        return TSDB_ERR_UNAVAILABLE;

    /* 1. Freeze cutoffs & below-listed decision, then merge I outside locks (no engine lock across chunk reads). */
    ecd_cut *cuts = NULL;
    size_t ncuts = 0;
    bool tombs_below = false;
    uint32_t merge_len_gen = 0;
    int rc = 0;
    pthread_mutex_lock(&db->ret_lock);
    int64_t now = eng_now_ms(db);
    merge_len_gen = atomic_load(&db->ret_len_gen);
    rc = ecd_freeze(db, pin, np, key, now, &cuts, &ncuts);
    if (rc == 0) {
        uint64_t lo_block = pin[0]->min_block;
        pthread_mutex_lock(&db->lock);
        for (size_t i = 0; i < db->nparts && !tombs_below; i++) {
            part *p = db->parts[i];
            tombs_below = p->partition == key && !p->dropped && p->max_block < lo_block;
        }
        pthread_mutex_unlock(&db->lock);
    }
    pthread_mutex_unlock(&db->ret_lock);

    part *merged = NULL, *out = NULL;
    ec_record rec;
    memset(&rec, 0, sizeof rec);
    bool have_rec = false, put_unindexed = false;
    part **y = NULL, **part_srcs = NULL;
    size_t ny = 0, npart_srcs = 0, nret = 0;
    uint8_t (*ret_oids)[16] = NULL;
    char (*pin_names)[256] = NULL;
    bool gated = false;
    lw_fence f;
    memset(&f, 0, sizeof f);
    int64_t t_gate = 0;

    if (rc == 0)
        rc = merge_parts_reencode(db, pin, np, cuts, ncuts, tombs_below, &merged);
    if (rc == 0 && merged) {
        ec_ldigest d_in = {0}, d_m = {0};
        rc = ec_logical_digest_frozen(db, pin, np, key, cuts, ncuts, tombs_below, &d_in);
        if (rc == 0) {
            part *one[1] = {merged};
            rc = ec_logical_digest_frozen(db, one, 1, key, cuts, ncuts, tombs_below, &d_m);
        }
        if (rc == 0 && (d_in.rows != d_m.rows || d_in.series != d_m.series || d_in.xxh64 != d_m.xxh64)) {
            fprintf(stderr, "tsdb: %s: cluster re-encode of key %lld aborted before put: logical digest mismatch\n",
                    db->dir, (long long)key);
            rc = TSDB_ERR_CORRUPT;
        }
    }
    free(cuts);
    cuts = NULL;
    if (rc != 0 || !merged)
        goto out;

    /* 2. Put the combined object onto the stripe group outside every engine lock (full stripe group, no degraded). */
    if (merged->ng > 0) {
        rc = ecsettle_put(db, merged, &rec, &epoch);
        if (rc != 0)
            goto out;
        have_rec = true;
        put_unindexed = true;
        step(db, ECCOLD_STEP_PUT);
        if (eccold_mutant != ECCOLD_MUT_STRIPEWAITFENCED && (rc = stripe_wait(db, cv, epoch, sv, 0)) != 0)
            goto out;
    }

    /* 3. Gate and cut at Z (identical to eccold_settle). */
    int64_t plo, phi;
    partition_range(db, key, &plo, &phi);
    int64_t hi = phi == INT64_MAX ? INT64_MAX : phi - 1;
    if ((rc = fence_make(&f, plo, hi, pin, np)) != 0)
        goto out;
    bool pending = true;
    if (eccold_mutant != ECCOLD_MUT_CUTNOGATE) {
        pending = lw_enter(db, &f);
        gated = true;
        t_gate = mono_us();
    }
    cut.gated = gated;
    cut.l = log_end(db);
    step(db, ECCOLD_STEP_GATED);
    if (have_rec && eccold_mutant == ECCOLD_MUT_STRIPEWAITFENCED && (rc = stripe_wait(db, cv, epoch, sv, 0)) != 0)
        goto out;
    if (pending && mt_may_hold(db, plo, hi) && eccold_mutant != ECCOLD_MUT_CUTNOROTATE) {
        if ((rc = mt_flush(db)) != 0)
            goto out;
        cut.flushed = true;
    }
    pthread_mutex_lock(&db->lock);
    cut.f = db->wal_flushed;
    pthread_mutex_unlock(&db->lock);
    if (eccold_mutant != ECCOLD_MUT_CUTNOWAIT && (rc = eng_leader_wait(db, cut.l, false)) != 0)
        goto out;
    cut.z = log_end(db);
    if (atomic_load(&db->ret_len_gen) != merge_len_gen) {
        rc = TSDB_ERR_STOPPED; /* retention lengthened since merge_parts_reencode ran */
        goto out;
    }
    uint64_t want_ep = have_rec ? epoch : coldvol_epoch(cv);
    if (!stripe_ok(cv, want_ep, sv)) {
        rc = TSDB_ERR_UNAVAILABLE;
        goto out;
    }

    /* 4. Collect Y, retired oids, and post-swap filenames under db->lock. */
    pin_names = calloc(np, sizeof *pin_names);
    ret_oids = calloc(np, sizeof *ret_oids);
    part_srcs = calloc(np, sizeof *part_srcs);
    if (!pin_names || !ret_oids || !part_srcs) {
        rc = TSDB_ERR_NOMEM;
        goto out;
    }
    size_t ycap = 0;
    for (;;) {
        pthread_mutex_lock(&db->lock);
        size_t need = 0;
        for (size_t i = 0; i < db->nparts; i++)
            need += db->parts[i]->partition == key;
        if (need > ycap) {
            pthread_mutex_unlock(&db->lock);
            size_t nc = need + 16;
            part **ny_buf = realloc(y, nc * sizeof *ny_buf);
            if (!ny_buf) {
                rc = TSDB_ERR_NOMEM;
                goto out;
            }
            y = ny_buf;
            ycap = nc;
            continue;
        }
        rc = take_reencode_y_locked(db, key, pin, np, y, &ny);
        if (rc == 0) {
            for (size_t i = 0; i < np; i++) {
                struct ecpart *ep = atomic_load(&pin[i]->ec);
                if (ep && !ep->test_mark) {
                    memcpy(ret_oids[nret++], ep->rec.oid, 16);
                    snprintf(pin_names[i], 256, "retired_%s", pin[i]->name);
                } else {
                    snprintf(pin_names[i], 256, "settled_%s", pin[i]->name);
                    part_ref(pin[i]);
                    part_srcs[npart_srcs++] = pin[i];
                }
            }
        }
        pthread_mutex_unlock(&db->lock);
        break;
    }
    cut.ny = ny;
    for (size_t i = 0; i < ny && i < ECCOLD_TEST_Y_MAX; i++)
        snprintf(cut.y[i], sizeof cut.y[i], "%.63s", y[i]->name);
    if (rc)
        goto out;
    step(db, ECCOLD_STEP_CUT);

    /* 5. Index the new .ec file with ppos = Z and stripe_version, then append/commit MK_COLD with retired oids. */
    if (have_rec) {
        rec.ppos = cut.z;
        rec.stripe_version = sv;
        rc = ecsettle_index(db, merged, &rec, &out);
        put_unindexed = false; /* ecsettle_index queued GC on failure */
        if (merged->pending)
            unlinkat(db->dirfd, merged->pending, 0);
        if (rc == 0 && out)
            out->written = merged->written;
        part_unref(merged);
        merged = NULL;
        if (rc)
            goto out;
    } else {
        out = merged;
        merged = NULL;
    }
    cut.nobjs = have_rec ? 1 : 0;
    step(db, ECCOLD_STEP_PRE_MARKER);
    pause_pre_marker_hook();
    atomic_store_explicit(&db->cold_active_sv, sv, memory_order_seq_cst);
    if (!stripe_ok(cv, want_ep, sv)) {
        if (have_rec) {
            drop_ec(db, &out, 1, true);
        } else if (out) {
            if (out->pending)
                unlinkat(db->dirfd, out->pending, 0);
            part_unref(out);
            out = NULL;
        }
        rc = TSDB_ERR_UNAVAILABLE;
        goto out;
    }
    uint64_t pos = 0;
    uint32_t nret_mk = eccold_mutant == ECCOLD_MUT_RETIREDUNNAMED ? 0u : (uint32_t)nret;
    if (have_rec) {
        rc = cold_marker_ex(db, &cut, &rec, &out, 1, y, ny, (const uint8_t *)ret_oids, nret_mk, &pos);
    } else {
        part **ymk = malloc((1 + ny) * sizeof *ymk);
        if (!ymk) {
            rc = TSDB_ERR_NOMEM;
        } else {
            ymk[0] = out;
            if (ny)
                memcpy(ymk + 1, y, ny * sizeof *y);
            rc = cold_marker_ex(db, &cut, NULL, NULL, 0, ymk, 1 + ny, (const uint8_t *)ret_oids, nret_mk, &pos);
            free(ymk);
        }
    }
    cut.pos = pos;
    if (rc) {
        bool discard = rc != TSDB_ERR_INDETERMINATE && !eng_check(db);
        if (have_rec) {
            drop_ec(db, &out, 1, discard);
        } else if (out) {
            if (discard && out->pending)
                unlinkat(db->dirfd, out->pending, 0);
            part_unref(out);
            out = NULL;
        }
        goto out;
    }
    step(db, ECCOLD_STEP_MARKER);

    /* 6. Leader swap (ec_swap_reencode records PPOS, RSTATE doing/done, and retires old .ec with need_digest). */
    struct eccold cur = {.pos = pos, .z = cut.z, .key = key};
    db->cold = &cur;
    rc = ec_swap_reencode(db, pin, np, out);
    db->cold = NULL;
    if (rc) {
        if (!eng_check(db))
            eng_fail(db, "a re-encode's swap was refused after its COLD marker committed", rc);
        part_unref(out);
        out = NULL;
        rc = TSDB_ERR_IO;
        goto out;
    }

    /* Release gate and drop all in-memory references to pin[] BEFORE pause_before_digest and leader digest so no
     * local ecgc_held() masks the need_digest GC gate (testing gcbeforedigest). */
    atomic_store_explicit(&db->cold_active_sv, 0, memory_order_release);
    if (gated) {
        lw_leave(db, &f);
        f.r = NULL;
        gated = false;
        cut.hold_us = mono_us() - t_gate;
    }
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < np; i++) {
        pin[i]->settling = false;
        pin[i]->reencoding = false;
    }
    pthread_cond_broadcast(&db->merged_cv);
    pthread_mutex_unlock(&db->lock);
    for (size_t i = 0; i < np; i++) {
        part_unref(pin[i]);
        pin[i] = NULL;
    }

    /* 7. Verify leader logical digest (retired set + Y vs new output + Y read back from chunk stores), commit
     *    MK_DIGEST, and ONLY THEN release the retired objects for MK_GC and hand young settled_ sources to eckeep.
     *    Note: step 1 already verified exact logical equivalence before putting to the stripe group; if step 7 hits a
     *    brief store blip we retry up to 3 times, and on prolonged outage or failover before MK_DIGEST the retired
     *    objects stay held on this leader (reopen/promotion collects them via ecgc_load_retired once replicas pass
     *    MK_COLD), while a genuine mismatch sets durable DIVERGED and keeps both chunks and young settled_ copies. */
    pause_before_digest_hook();
    {
        part **in_set = calloc(np + ny, sizeof *in_set);
        part **out_set = calloc(1 + ny, sizeof *out_set);
        size_t n_opened = 0;
        int drc = in_set && out_set ? 0 : TSDB_ERR_NOMEM;
        for (size_t i = 0; drc == 0 && i < np; i++) {
            if (ec_retired_name(pin_names[i]))
                drc = part_open_ec(db, pin_names[i], pin_names[i] + 8, &in_set[n_opened]);
            else
                drc = part_open_as(db->dirfd, pin_names[i], pin_names[i] + 8, false, false, &in_set[n_opened]);
            if (drc == 0) {
                in_set[n_opened]->verify = true;
                n_opened++;
            }
        }
        if (drc == 0) {
            for (size_t i = 0; i < ny; i++)
                in_set[np + i] = y[i];
            out_set[0] = out;
            for (size_t i = 0; i < ny; i++)
                out_set[1 + i] = y[i];
            uint64_t digest_pos = 0;
            for (int try_n = 0; try_n < 3; try_n++) {
                if (have_rec)
                    eccache_forget(db->tier, db->tier_vol, rec.oid); /* read output back through chunk stores */
                drc = ecdigest_leader_reencode(db, key, cut.z, in_set, np + ny, out_set, 1 + ny,
                                               have_rec ? rec.oid : NULL, have_rec ? 1 : 0, &digest_pos);
                if (drc != TSDB_ERR_UNAVAILABLE || atomic_load(&db->stop) || !eccold_leader(db))
                    break;
                struct timespec ts = {0, 200 * 1000000L};
                nanosleep(&ts, NULL);
            }
            for (size_t i = 0; i < n_opened; i++)
                part_unref(in_set[i]);
            n_opened = 0;
            if (drc == 0) {
                if (nret > 0)
                    ecgc_digest_matched(db, ret_oids, nret, digest_pos);
                if (npart_srcs)
                    eckeep_swapped(db, part_srcs, npart_srcs);
            }
        } else {
            fprintf(stderr, "tsdb: %s: re-encode: key %" PRId64 ": retired copy could not be reopened for digest (%d); "
                            "retired objects and sources stay held\n", db->dir, key, drc);
            for (size_t i = 0; i < n_opened; i++)
                part_unref(in_set[i]);
        }
        free(in_set);
        free(out_set);
    }

out:
    atomic_store_explicit(&db->cold_active_sv, 0, memory_order_release);
    if (gated) {
        lw_leave(db, &f);
        cut.hold_us = mono_us() - t_gate;
    } else if (f.r) {
        free(f.r);
    }
    if (ny) {
        pthread_mutex_lock(&db->lock);
        for (size_t i = 0; i < ny; i++)
            y[i]->settling = false;
        pthread_mutex_unlock(&db->lock);
        for (size_t i = 0; i < ny; i++)
            part_unref(y[i]);
    }
    free(y);
    for (size_t i = 0; i < npart_srcs; i++)
        part_unref(part_srcs[i]);
    free(part_srcs);
    free(pin_names);
    free(ret_oids);
    if (merged) {
        if (merged->pending)
            unlinkat(db->dirfd, merged->pending, 0);
        part_unref(merged);
    }
    if (out)
        part_unref(out);
    if (put_unindexed)
        ecsettle_gc_object(db, &rec);
    if (have_rec)
        ec_record_free(&rec);
    cut.rc = rc;
    pthread_mutex_lock(&last_mu);
    last_cut = cut;
    pthread_mutex_unlock(&last_mu);
    return rc;
}
