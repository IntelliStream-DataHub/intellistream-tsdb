/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Settled sources (src/eckeep.h has the design): the held list, the deadline from the record's settle time, and the
 * digest thread.
 *
 * Every settled_ file a writer knows of has one entry here and one GY_SETTLED entry in the graveyard (whose deadline
 * stays INT64_MAX: the graveyard's clock is monotonic, a settle time is the engine's wall clock, eng_now_ms, which never
 * goes back and which a test may set). graveyard_purge asks eckeep_purge_entry, which releases only an entry that is
 * MATCHED or ORPHAN and due. A new open starts every entry PENDING again, so nothing is released before its digest has
 * run once more in this process.
 *
 * The digest compares three things that must agree: the record's object_xxh64 (computed by the settle from the bytes it
 * encoded), the hash of the object as the chunk store gives it back (each cell checked against the record's sums), and
 * the hash of settled_<name> (the source, whose rows the volume answered before the swap). The object read covers the
 * stores' damage; the file read covers a settle that encoded other bytes than the source's, with a record that agrees
 * with what it encoded. Either way the .ec part may answer other rows than the part it replaced, so the volume stops
 * answering (D5) and the only copy that is known good, the settled_ file, is never released.
 */
#include "eckeep.h"
#include "ecdigest.h"
#include "ecdiverge.h"
#include "chunk.h"
#include "ecpart.h"
#include "coldops.h"
#include "tier.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

int eckeep_mutant;
atomic_bool eckeep_test_pause;
unsigned eckeep_digest_mb_s = 64;
atomic_bool eckeep_test_exit_released;

#define DIGEST_PIECE (1u << 20)
#define RETRY_MIN_MS 1000
#define RETRY_MAX_MS 30000

typedef struct kent {
    char name[256];   /* settled_<source> */
    int status;       /* ECKEEP_* */
    bool busy;        /* the thread is digesting it: never removed meanwhile */
    bool stale;       /* unsettled while busy: drop on completion unless re-settled */
    bool logged;      /* a wait was reported once */
    uint64_t gen;     /* bumped on unsettle/re-add so a stale in-flight digest result is discarded */
    int64_t deadline; /* MATCHED, ORPHAN: eng_now_ms from which it may go */
    int64_t next_try; /* PENDING: eng_mono_ms of the next try */
    int64_t backoff;
    bool bound;       /* a replica's retired file (no .ec of its name): its digest is its MK_DIGEST's job */
    int64_t key;      /* bound: the key */
    uint64_t z;       /* bound: its COLD's Z (0: unknown, found at an open) */
    char job[64];     /* the job file that covers it ("" until its DIGEST is applied) */
} kent;

/* a replica's MK_DIGEST (src/ecdigest.c): one logical digest over the retired files it names */
typedef struct kjob {
    ecd_job j;
    int status; /* ECKEEP_PENDING (run once armed), then MATCHED, DIVERGED or ROT; ORPHAN: superseded unarmed */
    bool busy, logged;
    int64_t next_try, backoff;
} kjob;

struct eckeep_eng {
    pthread_mutex_t mu; /* a leaf */
    pthread_cond_t cv;  /* CLOCK_MONOTONIC */
    kent *e;
    size_t n, cap;
    pthread_t th;
    bool started, stop;
    int64_t keep_ms;
    uint64_t tries;
    kjob *jobs;
    size_t nj, capj;
    bool jobs_loaded; /* the directory's digest_* files were listed (eckeep_jobs_load, under load_mu) */
    pthread_mutex_t load_mu; /* taken with no other lock: one listing, before any job is looked up */
    uint64_t lost;    /* kept copies found rotted or missing (D25): counted lost, released at their deadline */
    uint64_t undigested; /* retired copies whose DIGEST can no longer come (superseded): released unchecked */
};

static struct eckeep_eng *keep_of(eng *db) { return db->ecstate ? db->ecstate->keep : NULL; }

/* ------------------------------------------------------------ xxh64 over pieces (the same as xxh64() in one call) */

#define P1 11400714785074694791ULL
#define P2 14029467366897019727ULL
#define P3 1609587929392839161ULL
#define P4 9650029242287828579ULL
#define P5 2870177450012600261ULL

typedef struct {
    uint64_t v[4], total;
    uint8_t buf[32];
    size_t nbuf;
} xs;

static inline uint64_t rotl64(uint64_t x, int r) { return (x << r) | (x >> (64 - r)); }
static inline uint64_t xround(uint64_t acc, uint64_t in) { return rotl64(acc + in * P2, 31) * P1; }
static inline uint64_t xmerge(uint64_t acc, uint64_t v) { return (acc ^ xround(0, v)) * P1 + P4; }
static inline uint64_t r64(const uint8_t *p)
{
    uint64_t x;
    memcpy(&x, p, 8);
    return x;
}
static inline uint32_t r32(const uint8_t *p)
{
    uint32_t x;
    memcpy(&x, p, 4);
    return x;
}

static void xs_init(xs *s)
{
    memset(s, 0, sizeof *s);
    s->v[0] = P1 + P2;
    s->v[1] = P2;
    s->v[2] = 0;
    s->v[3] = 0 - P1;
}

static void xs_stripe(xs *s, const uint8_t *p)
{
    for (int i = 0; i < 4; i++)
        s->v[i] = xround(s->v[i], r64(p + 8 * i));
}

static void xs_update(xs *s, const void *data, size_t len)
{
    const uint8_t *p = data, *end = p + len;
    s->total += len;
    if (s->nbuf) {
        size_t take = 32 - s->nbuf < len ? 32 - s->nbuf : len;
        memcpy(s->buf + s->nbuf, p, take);
        s->nbuf += take;
        p += take;
        if (s->nbuf < 32)
            return;
        xs_stripe(s, s->buf);
        s->nbuf = 0;
    }
    for (; end - p >= 32; p += 32)
        xs_stripe(s, p);
    memcpy(s->buf, p, (size_t)(end - p));
    s->nbuf = (size_t)(end - p);
}

static uint64_t xs_digest(const xs *s)
{
    uint64_t h;
    if (s->total >= 32) {
        h = rotl64(s->v[0], 1) + rotl64(s->v[1], 7) + rotl64(s->v[2], 12) + rotl64(s->v[3], 18);
        for (int i = 0; i < 4; i++)
            h = xmerge(h, s->v[i]);
    } else {
        h = P5;
    }
    h += s->total;
    const uint8_t *p = s->buf, *end = p + s->nbuf;
    for (; p + 8 <= end; p += 8)
        h = rotl64(h ^ xround(0, r64(p)), 27) * P1 + P4;
    if (p + 4 <= end) {
        h = rotl64(h ^ ((uint64_t)r32(p) * P1), 23) * P2 + P3;
        p += 4;
    }
    for (; p < end; p++)
        h = rotl64(h ^ (*p * P5), 11) * P1;
    h ^= h >> 33;
    h *= P2;
    h ^= h >> 29;
    h *= P3;
    h ^= h >> 32;
    return h;
}

/* tests/test_keep.c checks this against xxh64() at every length and split */
uint64_t eckeep_test_xxh64_pieces(const void *data, size_t len, size_t piece);
uint64_t eckeep_test_xxh64_pieces(const void *data, size_t len, size_t piece)
{
    xs s;
    xs_init(&s);
    for (size_t off = 0; off < len; off += piece)
        xs_update(&s, (const uint8_t *)data + off, len - off < piece ? len - off : piece);
    return xs_digest(&s);
}

/* ------------------------------------------------------------ the held list (k->mu held) */

static kent *find(struct eckeep_eng *k, const char *name)
{
    for (size_t i = 0; i < k->n; i++)
        if (strcmp(k->e[i].name, name) == 0)
            return &k->e[i];
    return NULL;
}

static int add_entry(struct eckeep_eng *k, const char *name, bool reset_existing)
{
    kent *existing = find(k, name);
    if (existing) {
        if (existing->stale || (reset_existing && !existing->busy)) {
            existing->gen++;
            existing->stale = false;
            existing->status = ECKEEP_PENDING;
            existing->deadline = 0;
            existing->next_try = 0;
            existing->backoff = RETRY_MIN_MS;
            existing->logged = false;
            existing->bound = false;
            existing->job[0] = '\0';
        }
        return 0;
    }
    if (k->n == k->cap) {
        size_t cap = k->cap ? k->cap * 2 : 16;
        kent *e = realloc(k->e, cap * sizeof *e);
        if (!e)
            return TSDB_ERR_NOMEM;
        k->e = e;
        k->cap = cap;
    }
    kent *e = &k->e[k->n++];
    memset(e, 0, sizeof *e);
    snprintf(e->name, sizeof e->name, "%s", name);
    e->status = ECKEEP_PENDING;
    e->backoff = RETRY_MIN_MS;
    return 0;
}

static int add(struct eckeep_eng *k, const char *name) { return add_entry(k, name, false); }

static void drop_entry(struct eckeep_eng *k, kent *e)
{
    *e = k->e[--k->n];
}

static void *keep_main(void *arg);

/* the thread, once (k->mu held): not before the open has listed its parts, never after the stop */
static void kick(eng *db, struct eckeep_eng *k)
{
    if (!k->started && !k->stop && tsdb_thread_create(&k->th, keep_main, db) == 0)
        k->started = true;
    pthread_cond_broadcast(&k->cv);
}

/* ------------------------------------------------------------ the digest */

enum { R_MATCH, R_WAIT, R_DIVERGED, R_ORPHAN, R_GONE, R_STOPPED, R_ROT, R_HOLD };

static bool stopping(struct eckeep_eng *k)
{
    pthread_mutex_lock(&k->mu);
    bool s = k->stop;
    pthread_mutex_unlock(&k->mu);
    return s;
}

/* sleeps up to ms on the cv (a stop wakes it): the throttle */
static void nap(struct eckeep_eng *k, int64_t ms)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    ts.tv_sec += ms / 1000;
    ts.tv_nsec += (ms % 1000) * 1000000L;
    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec++;
        ts.tv_nsec -= 1000000000L;
    }
    pthread_mutex_lock(&k->mu);
    if (!k->stop)
        pthread_cond_timedwait(&k->cv, &k->mu, &ts);
    pthread_mutex_unlock(&k->mu);
}

static int pread_all(int fd, uint8_t *buf, size_t len, uint64_t off)
{
    while (len) {
        ssize_t r = pread(fd, buf, len, (off_t)off);
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0)
            return r < 0 ? errno : EIO; /* short: the file shrank under us */
        buf += r;
        len -= (size_t)r;
        off += (uint64_t)r;
    }
    return 0;
}

static void hex16(const uint8_t *b, char out[33])
{
    for (int i = 0; i < 16; i++)
        snprintf(out + 2 * i, 3, "%02x", b[i]);
}

static void diverged(eng *db, const part *p, const char *name, const char *why)
{
    char oid[33];
    struct ecpart *ep = atomic_load(&p->ec);
    hex16(ep->rec.oid, oid);
    fprintf(stderr,
            "tsdb: volume %" PRIu32 " partition %" PRId64 ": the digest of object %s (%s) against %s failed: %s; the "
            "volume is DIVERGED (refused, also after a restart, until `tsdb DIR volume diverged --clear`) and %s is "
            "kept\n",
            db->tier_vol, p->partition, oid, p->name, name, why, name);
    ec_diverge(db, p->partition, ep->rec.oid, EC_DIV_DIGEST); /* durable, then the flag (a failed write fails the engine) */
}

static int digest_part(eng *db, struct eckeep_eng *k, part *p, const char *name, int64_t *deadline)
{
    struct ecpart *ep = atomic_load(&p->ec);
    const ec_record *r = &ep->rec;
    *deadline = r->settle_ms + k->keep_ms;
    if (eckeep_mutant == ECKEEP_MUT_KEEPMTIME) {
        struct stat st;
        if (fstatat(db->dirfd, name, &st, 0) == 0)
            *deadline = (int64_t)st.st_mtim.tv_sec * 1000 + st.st_mtim.tv_nsec / 1000000 + k->keep_ms;
    }
    if (eckeep_mutant == ECKEEP_MUT_DIGESTSKIP)
        return R_MATCH;

    int fd = openat(db->dirfd, name, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? R_GONE : R_WAIT;
    struct stat st;
    if (fstat(fd, &st) != 0) {
        close(fd);
        return R_WAIT;
    }
    uint64_t len = ep->test_mark ? p->map_len : r->object_len;
    if ((uint64_t)st.st_size != len && eckeep_mutant != ECKEEP_MUT_NOLENCHECK) {
        close(fd);
        char why[96];
        snprintf(why, sizeof why, "the file has %" PRIu64 " bytes, the object %" PRIu64, (uint64_t)st.st_size, len);
        diverged(db, p, name, why);
        return R_DIVERGED;
    }

    int res = R_MATCH;
    cold_obj *o = NULL;
    coldvol *cv = tier_cold(db);
    if (!ep->test_mark) { /* a test-marked part's object is its map (src/ecpart.h) */
        chunk_geom g;
        /* every position UP (a REPLACING store is not: it lacks chunks it will have), else the digest waits */
        int rc = !cv ? TSDB_ERR_UNAVAILABLE : !cv->ops->all_up(cv->ctx) ? TSDB_ERR_UNAVAILABLE
                 : chunk_geom_make(&g, r->k, r->m, r->cell_bytes, r->object_len);
        if (rc == 0)
            rc = cv->ops->obj_open_ver
                     ? cv->ops->obj_open_ver(cv->ctx, r->oid, &g, r->sums, r->positions, r->stripe_version, &o)
                     : cv->ops->obj_open_pos ? cv->ops->obj_open_pos(cv->ctx, r->oid, &g, r->sums, r->positions, &o)
                                             : cv->ops->obj_open(cv->ctx, r->oid, &g, r->sums, &o);
        if (rc == TSDB_ERR_UNAVAILABLE || rc == TSDB_ERR_NOMEM || rc == TSDB_ERR_IO ||
            (rc && eckeep_mutant == ECKEEP_MUT_OPENWAIT)) {
            close(fd);
            return R_WAIT;
        }
        if (rc) {
            close(fd);
            diverged(db, p, name, rc == TSDB_ERR_NOTFOUND ? "no chunk of the object is in any store"
                                                          : "the object's chunks cannot be opened");
            return R_DIVERGED;
        }
    }
    /* whole stripes per read: a cell that straddled two pieces would be read and checked twice, and a damaged stripe
     * rebuilt for each piece that touches it */
    size_t step = DIGEST_PIECE;
    if (o) {
        size_t stripe = (size_t)cv->ops->obj_geom(o)->k * cv->ops->obj_geom(o)->cell;
        step = stripe >= DIGEST_PIECE ? stripe : DIGEST_PIECE / stripe * stripe;
    }
    uint8_t *ob = malloc(step), *fb = malloc(step);
    if (!ob || !fb)
        res = R_WAIT;
    xs ho, hf;
    xs_init(&ho);
    xs_init(&hf);
    unsigned npos = o ? cv->ops->obj_geom(o)->k + cv->ops->obj_geom(o)->m : 0;
    int64_t t0 = eng_mono_ms();
    uint64_t done = 0;
    const char *why = NULL;
    for (uint64_t off = 0; res == R_MATCH && off < len;) {
        size_t piece = len - off < step ? (size_t)(len - off) : step;
        if (o) {
            chunk_report rep;
            memset(&rep, 0, sizeof rep);
            unsigned fl = eckeep_mutant == ECKEEP_MUT_DIGESTRAW ? CHUNK_RR_RAW : 0;
            int rc = cv->ops->obj_read(o, off, piece, ob, fl, &rep);
            for (unsigned i = 0; rc == 0 && i < npos && i < CHUNK_POS_MAX; i++)
                if (rep.st[i] == CHUNK_ST_DOWN)
                    rc = TSDB_ERR_UNAVAILABLE; /* never a match made around a position that is away */
            if (rc == TSDB_ERR_CORRUPT || rc == TSDB_ERR_INVALID) {
                res = R_DIVERGED, why = "the object cannot be read whole (more cells lost than it has parity)";
                break;
            }
            if (rc) {
                res = R_WAIT;
                break;
            }
        } else {
            memcpy(ob, p->map + off, piece);
        }
        if (pread_all(fd, fb, piece, off) != 0) {
            res = R_WAIT; /* the file is the copy that is kept: an error reading it holds it, never releases it */
            break;
        }
        xs_update(&ho, ob, piece);
        xs_update(&hf, fb, piece);
        off += piece;
        done += 2 * (uint64_t)piece;
        if (stopping(k)) {
            res = R_STOPPED;
            break;
        }
        unsigned rate = eckeep_digest_mb_s;
        if (rate) {
            int64_t want = (int64_t)(done * 1000 / ((uint64_t)rate << 20)), took = eng_mono_ms() - t0;
            if (want > took)
                nap(k, want - took);
        }
    }
    if (o)
        cv->ops->obj_close(o);
    close(fd);
    free(ob);
    free(fb);
    if (res == R_MATCH && !ep->test_mark && cv && !cv->ops->all_up(cv->ctx))
        res = R_WAIT; /* a store went away meanwhile: try again with all of them */
    if (res == R_MATCH) {
        uint64_t a = xs_digest(&ho), b = xs_digest(&hf), want = ep->test_mark ? xxh64(p->map, p->map_len, 0)
                                                                              : r->object_xxh64;
        if (a == want && b != want && ecdigest_mutant != ECDIGEST_MUT_ROTDIVERGES && ecdigest_copy_rotted(db, name)) {
            /* D25: the object is its record; the copy fails its own checksums: rot of the copy, not a divergence */
            fprintf(stderr, "tsdb: volume %" PRIu32 " partition %" PRId64 ": the kept copy %s fails its checksums "
                    "(rot); its object %s matches its record and is served; the copy is counted lost\n",
                    db->tier_vol, p->partition, name, p->name);
            return R_ROT;
        }
        if ((a != want && eckeep_mutant != ECKEEP_MUT_NOOBJCMP) || b != want) {
            char w[160];
            snprintf(w, sizeof w, "xxh64 of the object %016" PRIx64 ", of the file %016" PRIx64 ", recorded %016" PRIx64,
                     a, b, want);
            diverged(db, p, name, w);
            return R_DIVERGED;
        }
    } else if (res == R_DIVERGED) {
        diverged(db, p, name, why);
    }
    return res;
}

/* One digest of settled_ file `name`: its .ec part (the same stem) looked up in the list, referenced across the reads
 * (so its object is not deleted under them: r7-gc deletes at the last reference). */
static int digest(eng *db, struct eckeep_eng *k, const char *name, int64_t *deadline)
{
    const char *src = name + 8; /* "settled_" */
    size_t sl = strlen(src);
    char ecname[256];
    if (sl <= 5 || sl - 5 + 4 >= sizeof ecname)
        return R_WAIT;
    snprintf(ecname, sizeof ecname, "%.*s.ec", (int)(sl - 5), src);
    part *p = NULL;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++)
        if (strcmp(db->parts[i]->name, ecname) == 0 && !db->parts[i]->dropped && atomic_load(&db->parts[i]->ec)) {
            p = db->parts[i];
            part_ref(p);
            break;
        }
    bool keyed = false;
    if (!p && db->ppos) { /* a replica's retired file: held for its MK_DIGEST while its key is listed */
        int64_t key = strtoll(src + 1, NULL, 10);
        for (size_t i = 0; i < db->nparts && !keyed; i++)
            keyed = db->parts[i]->partition == key && !db->parts[i]->dropped;
    }
    pthread_mutex_unlock(&db->lock);
    if (keyed)
        return R_HOLD;
    if (!p) {
        *deadline = eckeep_mutant == ECKEEP_MUT_ORPHANNOW ? 0 : eng_now_ms(db) + k->keep_ms;
        return R_ORPHAN;
    }
    int res = digest_part(db, k, p, name, deadline);
    part_unref(p);
    return res;
}

static kjob *realloc_jobs(struct eckeep_eng *k)
{
    size_t cap = k->capj ? k->capj * 2 : 8;
    kjob *x = realloc(k->jobs, cap * sizeof *x);
    if (!x)
        return NULL;
    k->jobs = x;
    k->capj = cap;
    return x;
}

uint64_t eckeep_copies_lost(eng *db)
{
    struct eckeep_eng *k = keep_of(db);
    if (!k)
        return 0;
    pthread_mutex_lock(&k->mu);
    uint64_t n = k->lost;
    pthread_mutex_unlock(&k->mu);
    return n;
}

static kjob *find_job(struct eckeep_eng *k, const char *file)
{
    for (size_t i = 0; i < k->nj; i++)
        if (strcmp(k->jobs[i].j.file, file) == 0)
            return &k->jobs[i];
    return NULL;
}

/* jobs whose files are all released (k->mu held): out of the list, their file names to *out (removed by the caller,
 * with no lock but db->lock, after the releases' directory fsync) */
static size_t jobs_done_locked(struct eckeep_eng *k, char (*out)[64], size_t cap)
{
    size_t n = 0;
    for (size_t i = 0; i < k->nj && n < cap;) {
        kjob *x = &k->jobs[i];
        bool left = x->busy || x->status == ECKEEP_PENDING || x->status == ECKEEP_DIVERGED;
        for (size_t j = 0; j < k->n && !left; j++)
            left = strcmp(k->e[j].job, x->j.file) == 0;
        if (left) {
            i++;
            continue;
        }
        memcpy(out[n++], x->j.file, 64);
        ecd_job_free(&x->j);
        *x = k->jobs[--k->nj];
    }
    return n;
}

/* every file of job x to status st, released from deadline on (k->mu held) */
static void mark_job_locked(struct eckeep_eng *k, kjob *x, int st, int64_t deadline)
{
    for (size_t i = 0; i < k->n; i++)
        if (strcmp(k->e[i].job, x->j.file) == 0) {
            k->e[i].status = st;
            k->e[i].deadline = deadline;
        }
}

/* the job files none of whose copies is left go (k->mu held, let go of around the removal) */
static void done_jobs(eng *db, struct eckeep_eng *k)
{
    char done[8][64]; /* a job none of whose files is left (all released before an open) */
    size_t nd = jobs_done_locked(k, done, 8);
    if (nd) {
        pthread_mutex_unlock(&k->mu);
        for (size_t i = 0; i < nd; i++)
            ecdigest_job_remove(db, done[i]);
        pthread_mutex_lock(&k->mu);
    }
}

/* one armed job (k->mu held, let go of across the reads) */
static void run_job(eng *db, struct eckeep_eng *k, kjob *jp)
{
    jp->busy = true;
    k->tries++;
    ecd_job j = jp->j; /* the arrays stay: a busy job is never freed */
    pthread_mutex_unlock(&k->mu);
    ec_ldigest got = {0};
    size_t bad = 0;
    int rc = ecdigest_job_check(db, &j, &got, &bad);
    bool match = rc == 0 && ((got.rows == j.want.rows && got.series == j.want.series && got.xxh64 == j.want.xxh64) ||
                             ecdigest_mutant == ECDIGEST_MUT_SKIP);
    if ((rc == TSDB_ERR_CORRUPT || rc == TSDB_ERR_NOTFOUND) && ecdigest_mutant == ECDIGEST_MUT_ROTMATCHES)
        rc = 0, match = true;
    if (rc == ECD_REFUSED && ecdigest_mutant == ECDIGEST_MUT_CORRUPTROT)
        rc = TSDB_ERR_CORRUPT;
    int st = ECKEEP_PENDING;
    if (match) { /* durable first: a restart releases what is left of a matched job without a second digest, and a
                  * copy missing from a job that is not marked matched is lost, never "released before" */
        ecd_job w = j;
        w.flags |= ECDJ_MATCHED;
        if (ecdigest_job_write(db, &w) == 0)
            st = ECKEEP_MATCHED;
    } else if (rc == 0 || rc == ECD_REFUSED || (rc == TSDB_ERR_CORRUPT && ecdigest_mutant == ECDIGEST_MUT_ROTDIVERGES)) {
        if (rc == 0)
            fprintf(stderr, "tsdb: volume %" PRIu32 " partition %" PRId64 ": the logical digest of the %zu retired "
                    "copies of the COLD at Z %" PRIu64 " is %" PRIu64 " rows, %" PRIu64 " series, %016" PRIx64 "; its "
                    "DIGEST says %" PRIu64 ", %" PRIu64 ", %016" PRIx64 ": the volume is DIVERGED and the copies are "
                    "kept\n", db->tier_vol, j.key, j.nnames, j.z, got.rows, got.series, got.xxh64, j.want.rows,
                    j.want.series, j.want.xxh64);
        else /* checksums hold and the rows cannot be read (an unknown series, timestamps out of order): not rot */
            fprintf(stderr, "tsdb: volume %" PRIu32 " partition %" PRId64 ": the retired copies of the COLD at Z %"
                    PRIu64 " pass their checksums and cannot be digested: the volume is DIVERGED and the copies are "
                    "kept\n", db->tier_vol, j.key, j.z);
        ec_diverge(db, j.key, j.oid, EC_DIV_DIGEST); /* durable, then the flag; no lock held */
        st = ECKEEP_DIVERGED;
    } else if (rc == TSDB_ERR_CORRUPT || rc == TSDB_ERR_NOTFOUND) {
        /* D25: the .ec parts are served. One digest covers the whole set, so the healthy copies cannot be compared
         * either: all of them go at the deadline, the rotted or missing ones counted lost */
        fprintf(stderr, "tsdb: volume %" PRIu32 " partition %" PRId64 ": %zu of the %zu retired copies of the COLD at "
                "Z %" PRIu64 " %s: they are counted lost and the copies released at their deadline unchecked\n",
                db->tier_vol, j.key, bad, j.nnames, j.z,
                rc == TSDB_ERR_CORRUPT ? "fail their checksums (rot)" : "are missing (no match was recorded)");
        st = ECKEEP_ROT;
    }
    int64_t deadline = eng_now_ms(db) + k->keep_ms;
    pthread_mutex_lock(&k->mu);
    jp = find_job(k, j.file);
    if (!jp)
        return;
    jp->busy = false;
    if (st == ECKEEP_PENDING) {
        if (!jp->logged) {
            jp->logged = true;
            fprintf(stderr, "tsdb: volume %" PRIu32 ": the digest job %s waits (%d); its files are held\n",
                    db->tier_vol, j.file, rc);
        }
        jp->next_try = eng_mono_ms() + jp->backoff;
        jp->backoff = jp->backoff * 2 > RETRY_MAX_MS ? RETRY_MAX_MS : jp->backoff * 2;
    } else {
        jp->status = st;
        if (st == ECKEEP_MATCHED)
            jp->j.flags |= ECDJ_MATCHED;
        if (st == ECKEEP_ROT)
            k->lost += bad;
        mark_job_locked(k, jp, st, deadline);
        done_jobs(db, k);
    }
    pthread_cond_broadcast(&k->cv);
}

static void *keep_main(void *arg)
{
    eng *db = arg;
    struct eckeep_eng *k = keep_of(db);
    setpriority(PRIO_PROCESS, (id_t)syscall(SYS_gettid), 19); /* this thread only */
    eckeep_jobs_load(db); /* after the open listed its parts: every settled_ file has its entry */
    pthread_mutex_lock(&k->mu);
    while (!k->stop) {
        int64_t now = eng_mono_ms(), next = INT64_MAX;
        kent *pick = NULL;
        bool paused = atomic_load(&eckeep_test_pause);
        kjob *jp = NULL;
        for (size_t i = 0; i < k->nj && !jp; i++) {
            kjob *x = &k->jobs[i];
            if (x->status != ECKEEP_PENDING || x->busy || !(x->j.flags & ECDJ_ARMED))
                continue; /* an unarmed job waits for its DIGEST */
            if (paused)
                next = now + 50;
            else if (x->next_try <= now)
                jp = x;
            else if (x->next_try < next)
                next = x->next_try;
        }
        if (jp) {
            run_job(db, k, jp);
            continue;
        }
        for (size_t i = 0; i < k->n; i++) {
            kent *e = &k->e[i];
            if (e->status != ECKEEP_PENDING || e->busy || e->bound)
                continue;
            if (paused)
                next = now + 50;
            else if (e->next_try <= now) {
                pick = e;
                break;
            } else if (e->next_try < next)
                next = e->next_try;
        }
        if (!pick) {
            if (next == INT64_MAX) {
                pthread_cond_wait(&k->cv, &k->mu);
            } else {
                pthread_mutex_unlock(&k->mu);
                nap(k, next - now);
                pthread_mutex_lock(&k->mu);
            }
            continue;
        }
        pick->busy = true;
        uint64_t gen = pick->gen;
        k->tries++;
        char name[256];
        memcpy(name, pick->name, sizeof name);
        pthread_mutex_unlock(&k->mu);

        int64_t deadline = 0;
        int res = digest(db, k, name, &deadline);

        pthread_mutex_lock(&k->mu);
        kent *e = find(k, name); /* the array may have moved; a busy entry is never removed */
        if (!e)
            continue;
        e->busy = false;
        if (e->gen != gen) {
            if (e->stale)
                drop_entry(k, e);
            pthread_cond_broadcast(&k->cv);
            continue;
        }
        switch (res) {
        case R_MATCH:
            e->status = ECKEEP_MATCHED;
            e->deadline = deadline;
            break;
        case R_ORPHAN:
            e->status = ECKEEP_ORPHAN;
            e->deadline = deadline;
            break;
        case R_DIVERGED:
            e->status = ECKEEP_DIVERGED;
            e->deadline = deadline;
            break;
        case R_ROT:
            k->lost++;
            e->status = ECKEEP_ROT;
            e->deadline = deadline;
            break;
        case R_HOLD:
            e->bound = true;
            e->key = strtoll(name + 9, NULL, 10);
            e->z = 0;
            fprintf(stderr, "tsdb: volume %" PRIu32 ": %s is a retired copy of key %" PRId64 " whose DIGEST marker is "
                    "not applied here yet: held until it is (never released unchecked)\n", db->tier_vol, name, e->key);
            break;
        case R_GONE: /* someone else removed the file: nothing left to keep (its graveyard entry goes at the purge) */
            e->status = ECKEEP_ORPHAN;
            e->deadline = INT64_MIN;
            break;
        case R_WAIT:
            if (!e->logged) {
                e->logged = true;
                fprintf(stderr, "tsdb: volume %" PRIu32 ": the digest of %s waits (a store is not up, or a read "
                        "failed); the file is held\n", db->tier_vol, name);
            }
            e->next_try = eng_mono_ms() + e->backoff;
            e->backoff = e->backoff * 2 > RETRY_MAX_MS ? RETRY_MAX_MS : e->backoff * 2;
            break;
        default: /* R_STOPPED */
            break;
        }
        pthread_cond_broadcast(&k->cv);
    }
    pthread_mutex_unlock(&k->mu);
    return NULL;
}

/* ------------------------------------------------------------ the engine's calls */

int eckeep_load_settled(eng *db, const char *name, bool read_only)
{
    EC_CALLED(EC_CALL_KEEP_LOAD);
    struct eckeep_eng *k = keep_of(db);
    if (read_only || !k)
        return 0; /* the file is left alone: a reader releases nothing, and without a tier no open gets this far */
    int rc = graveyard_add(db, name, GY_SETTLED, INT64_MAX);
    if (rc)
        return rc;
    pthread_mutex_lock(&k->mu);
    rc = add(k, name); /* no thread yet: it starts after the open (kick) */
    pthread_mutex_unlock(&k->mu);
    return rc;
}

/* R15 (eng_unsettle): true if the .ec part matching settled_P.part is pinned (settling) in db->parts (db->lock held).
 * Binary-searches db->parts by partition key (O(log nparts)) and is only called when an entry is due to release. */
static bool ec_part_settling_locked(eng *db, const char *settled_name)
{
    size_t nlen = strlen(settled_name);
    if (nlen <= 8 + 5 || strncmp(settled_name, "settled_", 8) != 0 || strcmp(settled_name + nlen - 5, ".part") != 0)
        return false;
    int64_t key = 0;
    if (sscanf(settled_name + 8, "p%" SCNd64 "_", &key) != 1)
        return false;
    char ec_name[128];
    if (nlen - 8 - 5 + 3 >= sizeof ec_name)
        return false;
    snprintf(ec_name, sizeof ec_name, "%.*s.ec", (int)(nlen - 8 - 5), settled_name + 8);
    size_t lo = 0, hi = db->nparts;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (db->parts[mid]->partition < key)
            lo = mid + 1;
        else
            hi = mid;
    }
    for (size_t i = lo; i < db->nparts && db->parts[i]->partition == key; i++)
        if (db->parts[i]->settling && strcmp(db->parts[i]->name, ec_name) == 0)
            return true;
    return false;
}

bool eckeep_purge_entry(eng *db, const graveyard_entry *e, bool all, int64_t now)
{
    EC_CALLED(EC_CALL_KEEP_PURGE);
    (void)all;
    (void)now;
    struct eckeep_eng *k = keep_of(db);
    if (!k)
        return false;
    int64_t wall = eng_now_ms(db);
    bool release = false;
    pthread_mutex_lock(&k->mu);
    kick(db, k);
    kent *x = find(k, e->name);
    if (x && !x->busy && wall >= x->deadline &&
        (x->status == ECKEEP_MATCHED || x->status == ECKEEP_ORPHAN || x->status == ECKEEP_ROT ||
         (x->status == ECKEEP_DIVERGED && eckeep_mutant == ECKEEP_MUT_DIGESTNOHOLD)) &&
        !ec_part_settling_locked(db, e->name)) {
        drop_entry(k, x);
        release = true;
    }
    char done[8][64];
    size_t ndone = release ? jobs_done_locked(k, done, 8) : 0;
    pthread_mutex_unlock(&k->mu);
    if (!release)
        return false;
    if (unlinkat(db->dirfd, e->name, 0) != 0 && errno != ENOENT)
        fprintf(stderr, "tsdb: unlink of %s: %s (the next open digests it again)\n", e->name, strerror(errno));
    /* the release counts once the unlink is durable: a file back after a power loss would wait for a DIGEST that is
     * never applied again once its job file went (fail-stop: eng_sync_dir) */
    else if (ecdigest_mutant != ECDIGEST_MUT_RELEASENOSYNC)
        (void)eng_sync_dir(db, "fsync of the directory after a settled_ file's release");
    if (atomic_load(&eckeep_test_exit_released))
        exit(0); /* tests: the power fails here (crashfs, at exit) */
    for (size_t i = 0; i < ndone; i++)
        ecdigest_job_remove(db, done[i]);
    return true;
}

static void held(eng *db, part *const *in, size_t n, bool bound, int64_t key, uint64_t z);

void eckeep_swapped(eng *db, part *const *in, size_t n) { held(db, in, n, false, 0, 0); }

void eckeep_unsettle_locked(eng *db, const char *settled_name)
{
    for (size_t i = 0; i < db->ngraveyard; i++) {
        if (db->graveyard[i].kind == GY_SETTLED && strcmp(db->graveyard[i].name, settled_name) == 0) {
            db->graveyard[i] = db->graveyard[--db->ngraveyard];
            break;
        }
    }
    struct eckeep_eng *k = keep_of(db);
    if (!k)
        return;
    pthread_mutex_lock(&k->mu);
    kent *e = find(k, settled_name);
    if (e) {
        if (e->busy) {
            e->gen++;
            e->stale = true;
        } else {
            drop_entry(k, e);
        }
        pthread_cond_broadcast(&k->cv);
    }
    pthread_mutex_unlock(&k->mu);
}

void eckeep_unsettle_cleanup_jobs(eng *db)
{
    struct eckeep_eng *k = keep_of(db);
    if (!k)
        return;
    char done[16][64];
    size_t ndone;
    do {
        pthread_mutex_lock(&k->mu);
        ndone = jobs_done_locked(k, done, 16);
        pthread_mutex_unlock(&k->mu);
        for (size_t i = 0; i < ndone; i++)
            ecdigest_job_remove(db, done[i]);
    } while (ndone == 16);
}

void eckeep_retired(eng *db, part *const *own, size_t n, int64_t key, uint64_t z) { held(db, own, n, true, key, z); }

bool eckeep_active(eng *db) { return keep_of(db) != NULL; }

void eckeep_jobs_load(eng *db)
{
    struct eckeep_eng *k = keep_of(db);
    if (!k)
        return;
    pthread_mutex_lock(&k->load_mu);
    pthread_mutex_lock(&k->mu);
    bool done = k->jobs_loaded;
    pthread_mutex_unlock(&k->mu);
    if (!done) {
        ecdigest_load_jobs(db);
        pthread_mutex_lock(&k->mu);
        k->jobs_loaded = true;
        pthread_mutex_unlock(&k->mu);
    }
    pthread_mutex_unlock(&k->load_mu);
}

/* the key in a settled_ name (settled_p<K>_...) */
static bool name_key(const char *name, int64_t *key)
{
    if (strncmp(name, "settled_p", 9) != 0)
        return false;
    char *end;
    errno = 0;
    long long v = strtoll(name + 9, &end, 10);
    *key = v;
    return errno == 0 && end != name + 9 && *end == '_';
}

static bool anyz_take(struct eckeep_eng *k, const kent *e, int64_t key)
{
    int64_t ek;
    if (!name_key(e->name, &ek) || ek != key)
        return false;
    kjob *x = e->job[0] ? find_job(k, e->job) : NULL;
    return !x || !(x->j.flags & ECDJ_ARMED);
}

int eckeep_job_names(eng *db, const char *file, int64_t key, char (**names)[256], size_t *n)
{
    struct eckeep_eng *k = keep_of(db);
    *names = NULL;
    *n = 0;
    if (!k)
        return TSDB_ERR_NOTFOUND;
    pthread_mutex_lock(&k->mu);
    kjob *x = find_job(k, file);
    int rc = 0;
    size_t c = 0;
    if (x && (x->j.flags & ECDJ_ARMED))
        rc = ECD_ARMED;
    for (size_t i = 0; rc == 0 && i < k->n; i++) /* the planted digestanyz: every unarmed copy of the key */
        c += ecdigest_mutant == ECDIGEST_MUT_ANYZ ? anyz_take(k, &k->e[i], key) : x && strcmp(k->e[i].job, file) == 0;
    if (rc == 0 && c == 0)
        rc = TSDB_ERR_NOTFOUND;
    if (rc == 0 && !(*names = malloc(c * sizeof **names)))
        rc = TSDB_ERR_NOMEM;
    for (size_t i = 0; rc == 0 && i < k->n; i++) {
        bool take = ecdigest_mutant == ECDIGEST_MUT_ANYZ ? anyz_take(k, &k->e[i], key)
                                                         : x && strcmp(k->e[i].job, file) == 0;
        if (take)
            memcpy((*names)[(*n)++], k->e[i].name, 256);
    }
    pthread_mutex_unlock(&k->mu);
    return rc;
}

size_t eckeep_unbound(eng *db, int64_t key, char (**names)[256])
{
    struct eckeep_eng *k = keep_of(db);
    *names = NULL;
    if (!k)
        return 0;
    pthread_mutex_lock(&db->lock); /* db->lock > k->mu (held) */
    pthread_mutex_lock(&k->mu);
    size_t n = 0;
    *names = malloc((k->n ? k->n : 1) * sizeof **names);
    for (size_t i = 0; *names && i < k->n; i++) {
        kent *e = &k->e[i];
        int64_t ek;
        if (e->job[0] || !name_key(e->name, &ek) || ek != key)
            continue;
        const char *src = e->name + 8;
        size_t sl = strlen(src);
        bool ec = false; /* a leader's own settled_ file: its .ec of the same stem is listed */
        for (size_t j = 0; j < db->nparts && !ec && sl > 5; j++)
            ec = strncmp(db->parts[j]->name, src, sl - 5) == 0 && strcmp(db->parts[j]->name + sl - 5, ".ec") == 0;
        if (!ec)
            memcpy((*names)[n++], e->name, 256);
    }
    pthread_mutex_unlock(&k->mu);
    pthread_mutex_unlock(&db->lock);
    if (n == 0) {
        free(*names);
        *names = NULL;
    }
    return n;
}

void eckeep_supersede(eng *db, int64_t key, uint64_t z)
{
    struct eckeep_eng *k = keep_of(db);
    if (!k || ecdigest_mutant == ECDIGEST_MUT_NOSUPERSEDE)
        return;
    int64_t deadline = eng_now_ms(db) + k->keep_ms;
    pthread_mutex_lock(&k->mu);
    for (size_t i = 0; i < k->nj; i++) {
        kjob *x = &k->jobs[i];
        if (x->j.key != key || x->j.z >= z || (x->j.flags & (ECDJ_ARMED | ECDJ_SUPERSEDED)) || x->busy)
            continue;
        ecd_job w = x->j;
        w.flags |= ECDJ_SUPERSEDED;
        pthread_mutex_unlock(&k->mu); /* durable: a restart must not hold them again */
        int rc = ecdigest_job_write(db, &w);
        pthread_mutex_lock(&k->mu);
        x = find_job(k, w.file);
        if (rc || !x)
            continue; /* logged; held until the next DIGEST of the key */
        fprintf(stderr, "tsdb: volume %" PRIu32 " partition %" PRId64 ": the COLD at Z %" PRIu64 " got no DIGEST "
                "before the one at Z %" PRIu64 " (its leader went first): its %zu retired copies are released at their "
                "deadline unchecked\n", db->tier_vol, key, x->j.z, z, x->j.nnames);
        x->j.flags |= ECDJ_SUPERSEDED;
        x->status = ECKEEP_ORPHAN;
        k->undigested += x->j.nnames;
        mark_job_locked(k, x, ECKEEP_ORPHAN, deadline);
        i = (size_t)-1; /* the list may have moved while mu was let go */
    }
    done_jobs(db, k);
    pthread_mutex_unlock(&k->mu);
}

uint64_t eckeep_copies_undigested(eng *db)
{
    struct eckeep_eng *k = keep_of(db);
    if (!k)
        return 0;
    pthread_mutex_lock(&k->mu);
    uint64_t n = k->undigested;
    pthread_mutex_unlock(&k->mu);
    return n;
}

int eckeep_add_job(eng *db, ecd_job *j)
{
    struct eckeep_eng *k = keep_of(db);
    if (!k) {
        ecd_job_free(j);
        return 0;
    }
    pthread_mutex_lock(&k->mu);
    int rc = 0;
    kjob *x = find_job(k, j->file);
    if (x && (x->busy || (x->j.flags & ECDJ_ARMED) || !(j->flags & ECDJ_ARMED))) {
        ecd_job_free(j); /* known: only an unarmed job is replaced, by its armed form */
    } else if (!x && k->nj == k->capj && !realloc_jobs(k)) {
        ecd_job_free(j);
        rc = TSDB_ERR_NOMEM; /* the job file is durable: the next open finds it */
    } else {
        if (x)
            ecd_job_free(&x->j);
        else
            x = &k->jobs[k->nj++];
        memset(x, 0, sizeof *x);
        x->j = *j;
        x->backoff = RETRY_MIN_MS;
        x->status = (j->flags & ECDJ_MATCHED) ? ECKEEP_MATCHED : (j->flags & ECDJ_SUPERSEDED) ? ECKEEP_ORPHAN
                                                                                             : ECKEEP_PENDING;
        int64_t deadline = eng_now_ms(db) + k->keep_ms;
        for (size_t i = 0; i < j->nnames; i++) {
            kent *e = find(k, j->names[i]); /* none: released before (its job had matched) */
            if (e) {
                e->bound = true;
                e->key = j->key;
                e->z = j->z;
                snprintf(e->job, sizeof e->job, "%s", j->file);
                if (x->status != ECKEEP_PENDING) {
                    e->status = x->status;
                    e->deadline = deadline;
                }
            }
        }
        memset(j, 0, sizeof *j);
        if (x->status != ECKEEP_PENDING)
            done_jobs(db, k);
        kick(db, k);
    }
    pthread_mutex_unlock(&k->mu);
    return rc;
}

static void held(eng *db, part *const *in, size_t n, bool bound, int64_t key, uint64_t z)
{
    struct eckeep_eng *k = keep_of(db);
    if (!k)
        return;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < n; i++) {
        char name[256];
        snprintf(name, sizeof name, "settled_%s", in[i]->name);
        bool known = false;
        for (size_t j = 0; j < db->ngraveyard && !known; j++)
            known = db->graveyard[j].kind == GY_SETTLED && strcmp(db->graveyard[j].name, name) == 0;
        if (!known && graveyard_add(db, name, GY_SETTLED, INT64_MAX) != 0)
            continue; /* out of memory: the file stays, unreleased, until the next open digests it */
        pthread_mutex_lock(&k->mu);
        /* A fresh swap owns this settled_ name: reset any leftover non-busy ORPHAN/ROT entry back to PENDING. */
        if (add_entry(k, name, true) == 0 && bound) {
            kent *e = find(k, name);
            e->bound = true;
            e->key = key;
            e->z = z;
        }
        pthread_mutex_unlock(&k->mu);
    }
    pthread_mutex_lock(&k->mu);
    kick(db, k);
    pthread_mutex_unlock(&k->mu);
    pthread_mutex_unlock(&db->lock);
}

int eckeep_status(eng *db, const char *name)
{
    struct eckeep_eng *k = keep_of(db);
    if (!k)
        return TSDB_ERR_NOTFOUND;
    pthread_mutex_lock(&k->mu);
    kent *e = find(k, name);
    int st = e ? e->status : TSDB_ERR_NOTFOUND;
    pthread_mutex_unlock(&k->mu);
    return st;
}

bool eckeep_key_pending(eng *db, int64_t key)
{
    struct eckeep_eng *k = keep_of(db);
    if (!k)
        return false;
    bool pending = false;
    pthread_mutex_lock(&k->mu);
    for (size_t i = 0; i < k->n && !pending; i++) {
        const kent *e = &k->e[i];
        int64_t ek;
        if ((e->status == ECKEEP_PENDING || e->busy) && name_key(e->name, &ek) && ek == key)
            pending = true;
    }
    for (size_t i = 0; i < k->nj && !pending; i++) {
        const kjob *x = &k->jobs[i];
        if (x->j.key == key && (x->status == ECKEEP_PENDING || x->busy))
            pending = true;
    }
    pthread_mutex_unlock(&k->mu);
    return pending;
}

int eckeep_test_wait(eng *db, const char *name, int64_t timeout_ms)
{
    struct eckeep_eng *k = keep_of(db);
    if (!k)
        return TSDB_ERR_NOTFOUND;
    int64_t end = eng_mono_ms() + timeout_ms;
    int st;
    pthread_mutex_lock(&k->mu);
    kick(db, k);
    for (;;) {
        kent *e = find(k, name);
        st = e ? e->status : TSDB_ERR_NOTFOUND;
        int64_t left = end - eng_mono_ms();
        if ((e && st != ECKEEP_PENDING && !e->busy) || !e || left <= 0)
            break;
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        int64_t w = left < 50 ? left : 50;
        ts.tv_nsec += w * 1000000L;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000L;
        }
        pthread_cond_timedwait(&k->cv, &k->mu, &ts);
    }
    pthread_mutex_unlock(&k->mu);
    return st;
}

uint64_t eckeep_test_tries(eng *db)
{
    struct eckeep_eng *k = keep_of(db);
    if (!k)
        return 0;
    pthread_mutex_lock(&k->mu);
    uint64_t n = k->tries;
    pthread_mutex_unlock(&k->mu);
    return n;
}

bool eckeep_test_stopped(eng *db)
{
    struct eckeep_eng *k = keep_of(db);
    if (!k)
        return true;
    pthread_mutex_lock(&k->mu);
    bool s = k->stop;
    pthread_mutex_unlock(&k->mu);
    return s;
}

int eckeep_eng_open(eng *db)
{
    EC_CALLED(EC_CALL_ENG_OPEN);
    if (!db->ecstate || !db->tier)
        return 0;
    struct eckeep_eng *k = calloc(1, sizeof *k);
    if (!k)
        return TSDB_ERR_NOMEM;
    pthread_condattr_t ca;
    pthread_condattr_init(&ca);
    pthread_condattr_setclock(&ca, CLOCK_MONOTONIC);
    pthread_mutex_init(&k->mu, NULL);
    pthread_mutex_init(&k->load_mu, NULL);
    pthread_cond_init(&k->cv, &ca);
    pthread_condattr_destroy(&ca);
    k->keep_ms = tier_eng_settings(db)->keep_s * 1000;
    db->ecstate->keep = k;
    return 0;
}

void eckeep_eng_stop(eng *db)
{
    struct eckeep_eng *k = keep_of(db);
    if (!k)
        return;
    pthread_mutex_lock(&k->mu);
    k->stop = true;
    bool join = k->started;
    k->started = false;
    pthread_cond_broadcast(&k->cv);
    pthread_mutex_unlock(&k->mu);
    if (join)
        pthread_join(k->th, NULL);
}

void eckeep_eng_close(eng *db)
{
    EC_CALLED(EC_CALL_ENG_CLOSE);
    struct eckeep_eng *k = keep_of(db);
    if (!k)
        return;
    eckeep_eng_stop(db);
    pthread_mutex_destroy(&k->mu);
    pthread_mutex_destroy(&k->load_mu);
    pthread_cond_destroy(&k->cv);
    free(k->e);
    for (size_t i = 0; i < k->nj; i++)
        ecd_job_free(&k->jobs[i].j);
    free(k->jobs);
    free(k);
    db->ecstate->keep = NULL;
}
