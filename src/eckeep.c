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

#define DIGEST_PIECE (1u << 20)
#define RETRY_MIN_MS 1000
#define RETRY_MAX_MS 30000

typedef struct kent {
    char name[256];   /* settled_<source> */
    int status;       /* ECKEEP_* */
    bool busy;        /* the thread is digesting it: never removed meanwhile */
    bool logged;      /* a wait was reported once */
    int64_t deadline; /* MATCHED, ORPHAN: eng_now_ms from which it may go */
    int64_t next_try; /* PENDING: eng_mono_ms of the next try */
    int64_t backoff;
} kent;

struct eckeep_eng {
    pthread_mutex_t mu; /* a leaf */
    pthread_cond_t cv;  /* CLOCK_MONOTONIC */
    kent *e;
    size_t n, cap;
    pthread_t th;
    bool started, stop;
    int64_t keep_ms;
    uint64_t tries;
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

static int add(struct eckeep_eng *k, const char *name)
{
    if (find(k, name))
        return 0;
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

enum { R_MATCH, R_WAIT, R_DIVERGED, R_ORPHAN, R_GONE, R_STOPPED };

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
            "volume is DIVERGED (refused until a restart) and %s is kept\n",
            db->tier_vol, p->partition, oid, p->name, name, why, name);
    atomic_store(&db->diverged, true);
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
            rc = cv->ops->obj_open(cv->ctx, r->oid, &g, r->sums, &o);
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
    pthread_mutex_unlock(&db->lock);
    if (!p) {
        *deadline = eckeep_mutant == ECKEEP_MUT_ORPHANNOW ? 0 : eng_now_ms(db) + k->keep_ms;
        return R_ORPHAN;
    }
    int res = digest_part(db, k, p, name, deadline);
    part_unref(p);
    return res;
}

static void *keep_main(void *arg)
{
    eng *db = arg;
    struct eckeep_eng *k = keep_of(db);
    setpriority(PRIO_PROCESS, (id_t)syscall(SYS_gettid), 19); /* this thread only */
    pthread_mutex_lock(&k->mu);
    while (!k->stop) {
        int64_t now = eng_mono_ms(), next = INT64_MAX;
        kent *pick = NULL;
        bool paused = atomic_load(&eckeep_test_pause);
        for (size_t i = 0; i < k->n; i++) {
            kent *e = &k->e[i];
            if (e->status != ECKEEP_PENDING || e->busy)
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
        (x->status == ECKEEP_MATCHED || x->status == ECKEEP_ORPHAN ||
         (x->status == ECKEEP_DIVERGED && eckeep_mutant == ECKEEP_MUT_DIGESTNOHOLD))) {
        drop_entry(k, x);
        release = true;
    }
    pthread_mutex_unlock(&k->mu);
    if (release && unlinkat(db->dirfd, e->name, 0) != 0 && errno != ENOENT)
        fprintf(stderr, "tsdb: unlink of %s: %s (the next open digests it again)\n", e->name, strerror(errno));
    return release;
}

void eckeep_swapped(eng *db, part *const *in, size_t n)
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
        (void)add(k, name);
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
    pthread_cond_destroy(&k->cv);
    free(k->e);
    free(k);
    db->ecstate->keep = NULL;
}
