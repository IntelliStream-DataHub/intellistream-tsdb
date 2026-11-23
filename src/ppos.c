/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * PPOS, the log position of every part of an erasure-coded cluster volume (src/ppos.h has the design and the lock
 * order). Two halves: the per-memtable map base partition -> highest position, fed at the append sites under buf_lock,
 * and the volume's file of records, appended and synced under ppos_mu before a part's name is visible.
 */
#include "internal.h"
#include "ppos.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define PPOS_FILE "PPOS"
#define PPOS_TMP "PPOS.tmp"
#define PPOS_REC 32

int ppos_mutant;
int ppos_test_lose_map, ppos_test_fail_errno;

/* ------------------------------------------------------------ the memtable's map */

struct ppos_day {
    int64_t day;
    uint64_t hi;
};

struct ppos_mt {
    size_t li; /* the entry the last note went to: one compare per row while rows stay in its day */
    size_t n, cap;
    struct ppos_day *e; /* sorted by day */
};

/* m->ppos once an allocation failed: the flush falls back to fno_hi (never freed) */
static struct ppos_mt mt_lost;

void ppos_note(struct eng *db, struct memtable *m, int64_t ts, size_t n, uint64_t fno)
{
    if (n == 0)
        return;
    if (__builtin_expect(ppos_mutant == PPOS_MUT_REPLAY, 0) && m != db->active)
        return; /* the replay fills a memtable of its own */
    uint64_t hi = fno + n - 1;
    int64_t day = floor_div(ts, db->partition_ms);
    struct ppos_mt *t = m->ppos;
    if (__builtin_expect(t != NULL && t->n != 0 && t->e[t->li].day == day, 1)) {
        if (hi > t->e[t->li].hi)
            t->e[t->li].hi = hi;
        return;
    }
    if (t == &mt_lost)
        return;
    if (!t) {
        t = __atomic_load_n(&ppos_test_lose_map, __ATOMIC_RELAXED) ? NULL : calloc(1, sizeof *t);
        if (!t) {
            m->ppos = &mt_lost;
            return;
        }
        m->ppos = t;
    }
    size_t lo = 0, up = t->n;
    while (lo < up) {
        size_t mid = lo + (up - lo) / 2;
        if (t->e[mid].day < day)
            lo = mid + 1;
        else
            up = mid;
    }
    if (lo < t->n && t->e[lo].day == day) {
        if (hi > t->e[lo].hi)
            t->e[lo].hi = hi;
        t->li = lo;
        return;
    }
    if (t->n == t->cap) {
        size_t cap = t->cap ? 2 * t->cap : 8;
        struct ppos_day *e = realloc(t->e, cap * sizeof *e);
        if (!e) { /* the days noted so far are no bound for a part that also holds this one */
            free(t->e);
            free(t);
            m->ppos = &mt_lost;
            return;
        }
        t->e = e, t->cap = cap;
    }
    memmove(t->e + lo + 1, t->e + lo, (t->n - lo) * sizeof *t->e);
    t->e[lo] = (struct ppos_day){day, hi};
    t->n++;
    t->li = lo;
}

void ppos_mt_free(struct memtable *m)
{
    struct ppos_mt *t = m->ppos;
    m->ppos = NULL;
    if (t && t != &mt_lost) {
        free(t->e);
        free(t);
    }
}

/* the flush this thread runs (ppos_flush_begin/end): its parts' positions come from m's map */
static __thread struct eng *fw_db;
static __thread struct memtable *fw_m;

void ppos_flush_begin(struct eng *db, struct memtable *m) { fw_db = db, fw_m = m; }

void ppos_flush_end(struct eng *db, struct memtable *m)
{
    (void)db, (void)m;
    fw_db = NULL, fw_m = NULL;
}

/* A flush part's position: the highest noted for the days of its key (the name's p<K>_), or fno_hi when the map
 * cannot say. */
static uint64_t flush_pos(struct eng *db, memtable *m, const char *name, uint32_t *flags)
{
    struct ppos_mt *t = m->ppos;
    char *end = NULL;
    long long key = name[0] == 'p' ? strtoll(name + 1, &end, 10) : 0;
    if (ppos_mutant == PPOS_MUT_FLUSHHI || !t || t == &mt_lost || !end || *end != '_') {
        *flags = PPOS_F_FALLBACK;
        return ppos_mutant == PPOS_MUT_FALLBACK0 ? 0 : m->fno_hi;
    }
    int64_t lo, hi;
    partition_range(db, (int64_t)key, &lo, &hi);
    int64_t dlo = floor_div(lo, db->partition_ms);
    int64_t dhi = hi == INT64_MAX ? INT64_MAX : floor_div(hi - 1, db->partition_ms);
    uint64_t pos = 0;
    for (size_t i = 0; i < t->n; i++)
        if (t->e[i].day >= dlo && t->e[i].day <= dhi && t->e[i].hi > pos)
            pos = t->e[i].hi;
    if (pos == 0) { /* a part of days no row was noted in: should not be; never answer below its rows */
        *flags = PPOS_F_FALLBACK;
        return m->fno_hi;
    }
    *flags = PPOS_F_MAP;
    return pos;
}

/* ------------------------------------------------------------ the file */

struct ppos {
    pthread_mutex_t mu;    /* ppos_mu: an append, its fdatasync and the directory fsync of a creation */
    pthread_mutex_t mapmu; /* a leaf: the index below */
    int fd;                /* -1 until the first append of this open */
    uint64_t off;          /* the end of the valid records */
    bool broken;           /* a write or sync failed: the file may end in a torn record */
    uint64_t *keys, *vals; /* open addressing, key 0 empty */
    uint8_t *fls;          /* the flags of vals[i] */
    size_t cap, n;
    size_t nrec;   /* records in the file */
    uint8_t *pend; /* PPOS_MUT_AFTER: records not yet written */
    size_t npend;
};

static uint64_t name_hash(const char *name)
{
    uint64_t h = xxh64(name, strlen(name), 0);
    return h ? h : 1;
}

static uint64_t rec_check(const uint8_t *rec, uint64_t off) { return xxh64(rec, 24, off ^ 0x50504f53u); }

static bool rec_ok(const uint8_t *rec, uint64_t off)
{
    uint64_t c, h;
    memcpy(&c, rec + 24, 8);
    memcpy(&h, rec, 8);
    return c == rec_check(rec, off) && h != 0 && load32(rec + 16) <= PPOS_F_ALL && load32(rec + 20) == 0;
}

static void rec_make(uint8_t *rec, uint64_t h, uint64_t pos, uint32_t flags, uint64_t off)
{
    memset(rec, 0, PPOS_REC);
    memcpy(rec, &h, 8);
    memcpy(rec + 8, &pos, 8);
    store32(rec + 16, flags);
    uint64_t c = rec_check(rec, off);
    memcpy(rec + 24, &c, 8);
}

static bool map_find(const struct ppos *p, uint64_t h, size_t *at)
{
    if (!p->cap)
        return false;
    for (size_t i = (size_t)(h * 0x9E3779B97F4A7C15ull) & (p->cap - 1);; i = (i + 1) & (p->cap - 1)) {
        if (p->keys[i] == h) {
            *at = i;
            return true;
        }
        if (p->keys[i] == 0) {
            *at = i;
            return false;
        }
    }
}

/* room for `more` new keys at a load of at most one half; under mapmu */
static int map_reserve(struct ppos *p, size_t more)
{
    if ((p->n + more) * 2 <= p->cap)
        return 0;
    size_t cap = p->cap ? p->cap : 64;
    while ((p->n + more) * 2 > cap)
        cap *= 2;
    uint64_t *k = calloc(cap, sizeof *k), *v = malloc(cap * sizeof *v);
    uint8_t *f = malloc(cap);
    if (!k || !v || !f) {
        free(k);
        free(v);
        free(f);
        return TSDB_ERR_NOMEM;
    }
    uint64_t *ok = p->keys, *ov = p->vals;
    uint8_t *of = p->fls;
    size_t ocap = p->cap;
    p->keys = k, p->vals = v, p->fls = f, p->cap = cap, p->n = 0;
    for (size_t i = 0; i < ocap; i++)
        if (ok[i]) {
            size_t at = 0;
            map_find(p, ok[i], &at);
            p->keys[at] = ok[i], p->vals[at] = ov[i], p->fls[at] = of[i], p->n++;
        }
    free(ok);
    free(ov);
    free(of);
    return 0;
}

/* after map_reserve for it */
static void map_put(struct ppos *p, uint64_t h, uint64_t pos, uint32_t flags)
{
    size_t at = 0;
    if (!map_find(p, h, &at))
        p->keys[at] = h, p->n++;
    p->vals[at] = pos, p->fls[at] = (uint8_t)flags;
}

static int write_all(int fd, const uint8_t *buf, size_t len, uint64_t off)
{
    size_t done = 0;
    while (done < len) {
        ssize_t w = pwrite(fd, buf + done, len - done, (off_t)(off + done));
        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0)
            return w < 0 ? errno : ENOSPC;
        done += (size_t)w;
    }
    return 0;
}

/* Appends recs (n records, encoded at offsets from p->off) and syncs them; under mu. 0, or TSDB_ERR_IO with the engine
 * failed. */
static int append_locked(struct eng *db, struct ppos *p, const uint8_t *recs, size_t n)
{
    if (p->broken || eng_check(db))
        return TSDB_ERR_IO;
    if (p->fd < 0) {
        bool existed = faccessat(db->dirfd, PPOS_FILE, F_OK, 0) == 0;
        p->fd = openat(db->dirfd, PPOS_FILE, O_WRONLY | O_CREAT | O_CLOEXEC, 0644);
        if (p->fd < 0) {
            int err = errno;
            p->broken = true;
            eng_fail(db, "open of PPOS", err);
            return TSDB_ERR_IO;
        }
        if (!existed && db->opt.fsync) {
            int rc = eng_sync_dir(db, "fsync of the directory after creating PPOS");
            if (rc) {
                p->broken = true;
                return rc;
            }
        }
    }
    int err = __atomic_exchange_n(&ppos_test_fail_errno, 0, __ATOMIC_RELAXED);
    if (!err)
        err = write_all(p->fd, recs, n * PPOS_REC, p->off);
    if (!err && db->opt.fsync && ppos_mutant != PPOS_MUT_NOSYNC && tsdb_fdatasync(p->fd) != 0)
        err = errno ? errno : EIO;
    if (err) {
        if (ppos_mutant == PPOS_MUT_NOFAIL)
            return TSDB_ERR_IO;
        p->broken = true;
        eng_fail(db, "write or fsync of PPOS", err);
        return TSDB_ERR_IO;
    }
    p->off += n * PPOS_REC;
    p->nrec += n;
    return 0;
}

/* PPOS_MUT_AFTER: what an earlier call left, written now (after the names it was for) */
static int flush_pending_locked(struct eng *db, struct ppos *p)
{
    if (!p->npend)
        return 0;
    for (size_t i = 0; i < p->npend; i++) { /* their offsets moved with whatever went out in between */
        uint8_t *r = p->pend + i * PPOS_REC;
        uint64_t h, pos;
        memcpy(&h, r, 8);
        memcpy(&pos, r + 8, 8);
        rec_make(r, h, pos, load32(r + 16), p->off + i * PPOS_REC);
    }
    int rc = append_locked(db, p, p->pend, p->npend);
    free(p->pend);
    p->pend = NULL, p->npend = 0;
    return rc;
}

/* n names at positions pos[i] (flags[i]): reserved in the index, appended, synced, then indexed */
static int put_records(struct eng *db, const char *const *names, const uint64_t *pos, const uint32_t *flags, size_t n)
{
    struct ppos *p = db->ppos;
    uint8_t *recs = malloc(n * PPOS_REC);
    uint64_t *h = malloc(n * sizeof *h);
    if (!recs || !h) {
        free(recs);
        free(h);
        return TSDB_ERR_NOMEM;
    }
    for (size_t i = 0; i < n; i++)
        h[i] = name_hash(names[i]);
    pthread_mutex_lock(&p->mu);
    pthread_mutex_lock(&p->mapmu);
    int rc = map_reserve(p, n); /* nothing written that the index could not take */
    pthread_mutex_unlock(&p->mapmu);
    if (rc == 0)
        rc = flush_pending_locked(db, p);
    for (size_t i = 0; rc == 0 && i < n; i++)
        rec_make(recs + i * PPOS_REC, h[i], pos[i], flags[i], p->off + i * PPOS_REC);
    if (rc == 0 && ppos_mutant == PPOS_MUT_AFTER) {
        uint8_t *q = realloc(p->pend, (p->npend + n) * PPOS_REC);
        if (!q) {
            rc = TSDB_ERR_NOMEM;
        } else {
            memcpy(q + p->npend * PPOS_REC, recs, n * PPOS_REC);
            p->pend = q, p->npend += n;
        }
    } else if (rc == 0) {
        rc = append_locked(db, p, recs, n);
    }
    if (rc == 0) {
        pthread_mutex_lock(&p->mapmu);
        for (size_t i = 0; i < n; i++)
            map_put(p, h[i], pos[i], flags[i]);
        pthread_mutex_unlock(&p->mapmu);
    }
    pthread_mutex_unlock(&p->mu);
    free(recs);
    free(h);
    return rc;
}

int ppos_record(struct eng *db, const char *const *names, size_t n, uint64_t pos)
{
    if (!db->ppos || n == 0)
        return 0;
    uint64_t *ps = malloc(n * sizeof *ps);
    uint32_t *fl = malloc(n * sizeof *fl);
    if (!ps || !fl) {
        free(ps);
        free(fl);
        return TSDB_ERR_NOMEM;
    }
    bool flush = pos == 0 && fw_db == db && fw_m != NULL;
    for (size_t i = 0; i < n; i++) {
        if (flush)
            ps[i] = flush_pos(db, fw_m, names[i], &fl[i]);
        else if (pos == 0 || pos == PPOS_UNKNOWN) /* nobody knows: the top of the range, never the bottom */
            ps[i] = ppos_mutant == PPOS_MUT_UNKNOWN0 ? 0 : PPOS_UNKNOWN, fl[i] = PPOS_F_UNKNOWN;
        else
            ps[i] = pos, fl[i] = PPOS_F_GIVEN;
    }
    int rc = put_records(db, names, ps, fl, n);
    free(ps);
    free(fl);
    return rc;
}

int ppos_set(struct eng *db, const char *name, uint64_t pos)
{
    if (!db->ppos)
        return TSDB_ERR_INVALID;
    uint32_t fl = PPOS_F_GIVEN;
    if (pos == 0 || pos == PPOS_UNKNOWN)
        pos = PPOS_UNKNOWN, fl = PPOS_F_UNKNOWN;
    return put_records(db, &name, &pos, &fl, 1);
}

int ppos_of(struct eng *db, const char *name, uint64_t *pos)
{
    *pos = 0;
    struct ppos *p = db->ppos;
    if (!p)
        return TSDB_ERR_INVALID;
    uint64_t h = name_hash(name);
    size_t at;
    pthread_mutex_lock(&p->mapmu);
    bool found = map_find(p, h, &at);
    if (found)
        *pos = p->vals[at];
    pthread_mutex_unlock(&p->mapmu);
    if (!found) {
        fprintf(stderr, "tsdb: %s: PPOS has no record of the part %s\n", db->dir, name);
        return TSDB_ERR_NOTFOUND;
    }
    return 0;
}

int ppos_test_flags(struct eng *db, const char *name)
{
    struct ppos *p = db->ppos;
    if (!p)
        return -1;
    size_t at;
    pthread_mutex_lock(&p->mapmu);
    int f = map_find(p, name_hash(name), &at) ? p->fls[at] : -1;
    pthread_mutex_unlock(&p->mapmu);
    return f;
}

size_t ppos_test_records(struct eng *db)
{
    struct ppos *p = db->ppos;
    if (!p)
        return 0;
    pthread_mutex_lock(&p->mu);
    size_t n = p->nrec;
    pthread_mutex_unlock(&p->mu);
    return n;
}

/* ------------------------------------------------------------ open */

static int ppos_read_file(struct eng *db, uint8_t **out, size_t *len)
{
    *out = NULL, *len = 0;
    int fd = openat(db->dirfd, PPOS_FILE, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? 0 : TSDB_ERR_IO;
    struct stat st;
    if (fstat(fd, &st) != 0) {
        close(fd);
        return TSDB_ERR_IO;
    }
    size_t size = (size_t)st.st_size, got = 0;
    uint8_t *buf = malloc(size ? size : 1);
    if (!buf) {
        close(fd);
        return TSDB_ERR_NOMEM;
    }
    int rc = 0;
    while (got < size) {
        ssize_t r = pread(fd, buf + got, size - got, (off_t)got);
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0) {
            fprintf(stderr, "tsdb: %s: reading %s at offset %zu: %s\n", db->dir, PPOS_FILE, got,
                    r < 0 ? strerror(errno) : "short read");
            rc = TSDB_ERR_IO;
            break;
        }
        got += (size_t)r;
    }
    close(fd);
    if (rc) {
        free(buf);
        return rc;
    }
    *out = buf, *len = size;
    return 0;
}

/* the records of the listed parts only, in a new file renamed over PPOS (every one is in p's index) */
static int compact(struct eng *db, struct ppos *p)
{
    size_t n = db->nparts;
    uint8_t *buf = malloc(n ? n * PPOS_REC : 1);
    if (!buf)
        return TSDB_ERR_NOMEM;
    for (size_t i = 0; i < n; i++) {
        uint64_t h = name_hash(db->parts[i]->name);
        size_t at = 0;
        map_find(p, h, &at);
        rec_make(buf + i * PPOS_REC, h, p->vals[at], ppos_mutant == PPOS_MUT_FLAGS ? PPOS_F_GIVEN : p->fls[at],
                 (uint64_t)i * PPOS_REC);
    }
    unlinkat(db->dirfd, PPOS_TMP, 0);
    int fd = openat(db->dirfd, PPOS_TMP, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    int err = fd < 0 ? errno : write_all(fd, buf, n * PPOS_REC, 0);
    if (!err && db->opt.fsync && tsdb_fdatasync(fd) != 0)
        err = errno ? errno : EIO;
    if (fd >= 0)
        close(fd);
    free(buf);
    if (!err && renameat(db->dirfd, PPOS_TMP, db->dirfd, PPOS_FILE) != 0)
        err = errno;
    if (err) { /* PPOS as it was: the open fails, nothing relied on the new file */
        unlinkat(db->dirfd, PPOS_TMP, 0);
        fprintf(stderr, "tsdb: %s: the compaction of PPOS failed: %s\n", db->dir, strerror(err));
        return TSDB_ERR_IO;
    }
    int rc = eng_sync_dir(db, "fsync of the directory after compacting PPOS");
    if (rc)
        return rc;
    p->nrec = n, p->off = (uint64_t)n * PPOS_REC;
    return 0;
}

static void ppos_free(struct ppos *p)
{
    if (!p)
        return;
    if (p->fd >= 0)
        close(p->fd);
    pthread_mutex_destroy(&p->mu);
    pthread_mutex_destroy(&p->mapmu);
    free(p->keys);
    free(p->vals);
    free(p->fls);
    free(p->pend);
    free(p);
}

int ppos_open(struct eng *db)
{
    if (!db->ec_cluster || db->ppos)
        return 0;
    const char *e = getenv("TSDB_PPOS_MUTANT"); /* the test's children run the same bug */
    if (e && *e && ppos_mutant == PPOS_MUT_NONE)
        ppos_mutant = atoi(e);
    bool ro = db->opt.read_only;
    struct ppos *p = calloc(1, sizeof *p);
    if (!p)
        return TSDB_ERR_NOMEM;
    p->fd = -1;
    pthread_mutex_init(&p->mu, NULL);
    pthread_mutex_init(&p->mapmu, NULL);
    uint8_t *buf;
    size_t len;
    int rc = ppos_read_file(db, &buf, &len);
    size_t pos = 0;
    for (; rc == 0 && pos + PPOS_REC <= len && rec_ok(buf + pos, pos); pos += PPOS_REC) {
    }
    for (size_t q = pos + PPOS_REC; rc == 0 && ppos_mutant != PPOS_MUT_NOBADGOOD && q + PPOS_REC <= len; q += PPOS_REC)
        if (rec_ok(buf + q, q)) {
            fprintf(stderr, "tsdb: %s: %s is damaged at offset %zu, before valid records: the open fails\n", db->dir,
                    PPOS_FILE, pos);
            rc = TSDB_ERR_CORRUPT;
        }
    p->nrec = pos / PPOS_REC, p->off = pos;
    if (rc == 0)
        rc = map_reserve(p, p->nrec);
    for (size_t i = 0; rc == 0 && i < p->nrec; i++) {
        uint64_t h, v;
        memcpy(&h, buf + i * PPOS_REC, 8);
        memcpy(&v, buf + i * PPOS_REC + 8, 8);
        map_put(p, h, v, load32(buf + i * PPOS_REC + 16));
    }
    /* every listed part has its record: it was synced before the name was visible */
    for (size_t i = 0, at; rc == 0 && i < db->nparts; i++)
        if (!map_find(p, name_hash(db->parts[i]->name), &at)) {
            fprintf(stderr, "tsdb: %s: PPOS has no record of the listed part %s: the open fails\n", db->dir,
                    db->parts[i]->name);
            rc = TSDB_ERR_CORRUPT;
        }
    if (rc == 0 && !ro && pos < len) { /* a torn tail: read again, then cut */
        rc = tail_confirm(db->dirfd, PPOS_FILE, buf + pos, pos, len - pos);
        if (rc == 0) {
            fprintf(stderr, "tsdb: %s: %s: cutting a torn tail of %zu bytes at offset %zu\n", db->dir, PPOS_FILE,
                    len - pos, pos);
            int fd = openat(db->dirfd, PPOS_FILE, O_WRONLY | O_CLOEXEC);
            if (fd < 0 || ftruncate(fd, (off_t)pos) != 0 || (db->opt.fsync && tsdb_fdatasync(fd) != 0))
                rc = TSDB_ERR_IO;
            if (fd >= 0)
                close(fd);
        }
    }
    free(buf);
    /* the listed names are distinct: the records of others are the dead ones */
    if (rc == 0 && !ro && p->nrec > db->nparts && p->nrec - db->nparts > p->nrec / 2) {
        size_t before = p->nrec;
        struct ppos q = {.cap = 0};
        if ((rc = map_reserve(&q, db->nparts)) == 0 && (rc = compact(db, p)) == 0) {
            for (size_t i = 0, at = 0; i < db->nparts; i++) {
                uint64_t h = name_hash(db->parts[i]->name);
                map_find(p, h, &at);
                map_put(&q, h, p->vals[at], p->fls[at]);
            }
            free(p->keys);
            free(p->vals);
            free(p->fls);
            p->keys = q.keys, p->vals = q.vals, p->fls = q.fls, p->cap = q.cap, p->n = q.n;
            q.keys = q.vals = NULL;
            q.fls = NULL;
            fprintf(stderr, "tsdb: %s: PPOS compacted from %zu records to %zu\n", db->dir, before, p->nrec);
        }
        free(q.keys);
        free(q.vals);
        free(q.fls);
    }
    if (rc) {
        ppos_free(p);
        return rc;
    }
    db->ppos = p;
    return 0;
}

void ppos_close(struct eng *db)
{
    struct ppos *p = db->ppos;
    if (!p)
        return;
    pthread_mutex_lock(&p->mu);
    flush_pending_locked(db, p);
    pthread_mutex_unlock(&p->mu);
    db->ppos = NULL;
    ppos_free(p);
}
