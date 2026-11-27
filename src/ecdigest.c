/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The logical digest (src/ecdigest.h has the design): the digest itself, the leader's marker, and a replica's jobs
 * (the comparison runs on eckeep's thread, src/eckeep.c).
 */
#include "internal.h"
#include "ecapply.h"
#include "ecdigest.h"
#include "ecdiverge.h"
#include "ecgc.h"
#include "eckeep.h"
#include "ecpart.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

int ecdigest_mutant;
atomic_bool ecdigest_test_alter;
_Atomic int64_t ecdigest_test_retry_ms = -1;
static _Thread_local bool alter_next; /* the job check of this thread changes the value of the next row it hashes */

int ecdigest_mutant_parse(const char *name)
{
    static const char *const names[] = {"none",           "digestbytes",      "digestnotomb",       "digestnocompare",
                                         "digestnoretention", "releasenosync", "rotdiverges",        "digestbelowtrue",
                                         "digestnots",     "digestfreezerows", "digestlocalbelow",   "digestjobnosync",
                                         "digestnocrcwalk", "digestrotmatches", "digestnojobremove", "digestanyz",
                                         "digestnosupersede", "digestcorruptrot"};
    for (size_t i = 0; i < sizeof names / sizeof *names; i++)
        if (strcmp(name, names[i]) == 0)
            return (int)i;
    return -1;
}

/* ------------------------------------------------------------ the byte stream and its hash */

#define PIECE (64u << 10)

typedef struct dh {
    uint8_t *buf;
    size_t n;
    uint64_t h;
} dh;

static void dh_put(dh *d, const void *data, size_t len)
{
    const uint8_t *p = data;
    while (len) {
        size_t t = MIN(PIECE - d->n, len);
        memcpy(d->buf + d->n, p, t);
        d->n += t;
        p += t;
        len -= t;
        if (d->n == PIECE) {
            d->h = xxh64(d->buf, PIECE, d->h);
            d->n = 0;
        }
    }
}

static uint64_t dh_end(dh *d) { return xxh64(d->buf, d->n, d->h); }

typedef struct rowctx {
    dh *d;
    uint32_t series;
    size_t w;
    uint64_t rows;
} rowctx;

static int row_cb(void *ctx, const int64_t *ts, const void *v, size_t n)
{
    rowctx *r = ctx;
    uint8_t rec[4 + 8 + 8];
    size_t w = r->w > 8 ? 8 : r->w; /* type_width: 4 or 8 */
    memcpy(rec, &r->series, 4);
    for (size_t i = 0; i < n; i++) {
        if (ecdigest_mutant == ECDIGEST_MUT_NOTS)
            memset(rec + 4, 0, 8);
        else
            memcpy(rec + 4, &ts[i], 8);
        memcpy(rec + 12, (const uint8_t *)v + i * w, w);
        if (alter_next) { /* ecdigest_test_alter: a copy whose rows differ, with valid checksums */
            alter_next = false;
            rec[12] ^= 1;
        }
        dh_put(r->d, rec, 12 + w);
    }
    r->rows += n;
    return 0;
}

static int cmp_u32(const void *a, const void *b)
{
    uint32_t x = *(const uint32_t *)a, y = *(const uint32_t *)b;
    return x < y ? -1 : x > y;
}

static int cmp_block(const void *a, const void *b)
{
    const part *x = *(part *const *)a, *y = *(part *const *)b;
    return x->min_block < y->min_block ? -1 : x->min_block > y->min_block;
}

/* the distinct series of the set (with rows; with tombs, those of its tombstones too), sorted */
static int set_series(part **set, size_t n, bool tombs, uint32_t **out, size_t *nout)
{
    size_t cap = 0;
    for (size_t i = 0; i < n; i++)
        cap += (set[i]->ng ? set[i]->ix.nruns : 0) + (tombs ? set[i]->nt : 0);
    uint32_t *s = malloc((cap ? cap : 1) * sizeof *s);
    if (!s)
        return TSDB_ERR_NOMEM;
    size_t k = 0;
    for (size_t i = 0; i < n; i++) {
        for (uint32_t r = 0; set[i]->ng && r < set[i]->ix.nruns; r++)
            s[k++] = set[i]->ix.rser[r];
        for (uint32_t t = 0; tombs && t < set[i]->nt; t++)
            s[k++] = set[i]->t[t].series;
    }
    qsort(s, k, sizeof *s, cmp_u32);
    size_t o = 0;
    for (size_t i = 0; i < k; i++)
        if (o == 0 || s[o - 1] != s[i])
            s[o++] = s[i];
    *out = s;
    *nout = o;
    return 0;
}

typedef struct cutsrc {
    eng *db;
    int64_t now;
    const ecd_cut *cuts; /* frozen, or NULL: the engine's limits at now */
    size_t ncuts;
} cutsrc;

static int64_t cut_of(const cutsrc *c, uint32_t series)
{
    if (ecdigest_mutant == ECDIGEST_MUT_NORETENTION)
        return INT64_MIN;
    if (!c->cuts)
        return eng_cutoff_at(c->db, series, c->now);
    size_t lo = 0, hi = c->ncuts;
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        if (c->cuts[mid].series < series)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo < c->ncuts && c->cuts[lo].series == series ? c->cuts[lo].cutoff : INT64_MIN;
}

/* whether a listed part of key lies below every part of the set (its rows are what the set's tombstones still hide) */
static bool below_listed(eng *db, part **set, size_t n, int64_t key)
{
    uint64_t lo = UINT64_MAX;
    for (size_t i = 0; i < n; i++)
        lo = MIN(lo, set[i]->min_block);
    bool any = false;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts && !any; i++) {
        part *p = db->parts[i];
        if (p->partition != key || p->dropped || p->max_block >= lo)
            continue;
        bool in = false;
        for (size_t j = 0; j < n && !in; j++)
            in = set[j] == p;
        any = !in;
    }
    pthread_mutex_unlock(&db->lock);
    return any;
}

static int digest_bytes(part **set, size_t n, ec_ldigest *out)
{
    uint64_t h = 0;
    for (size_t i = 0; i < n; i++) { /* planted: the files, not the rows (a merged twin differs) */
        h = xxh64(set[i]->map, set[i]->map_len, h);
        out->rows += set[i]->rows;
    }
    out->xxh64 = h;
    return 0;
}

/* tombs: hash the tombstone term (1) or not (0); -1: when a listed part of the key lies below the set now */
static int digest_run(eng *db, part **in, size_t n, int64_t key, const cutsrc *cs, int tombs, ec_ldigest *out)
{
    memset(out, 0, sizeof *out);
    part **set = malloc((n ? n : 1) * sizeof *set);
    if (!set)
        return TSDB_ERR_NOMEM;
    memcpy(set, in, n * sizeof *set);
    qsort(set, n, sizeof *set, cmp_block); /* the snapshot's order: (partition, min_block) */
    for (size_t i = 0; i < n; i++)
        if (set[i]->partition != key) {
            free(set);
            return TSDB_ERR_INVALID;
        }
    if (ecdigest_mutant == ECDIGEST_MUT_BYTES) {
        int rc = digest_bytes(set, n, out);
        free(set);
        return rc;
    }
    uint32_t *ser = NULL;
    size_t ns = 0;
    dh d = {.buf = malloc(PIECE)};
    int rc = d.buf ? set_series(set, n, false, &ser, &ns) : TSDB_ERR_NOMEM;
    for (size_t i = 0; rc == 0 && i < ns; i++) {
        int type = eng_series_type(db, ser[i]);
        if (type < 0) {
            rc = TSDB_ERR_CORRUPT; /* a part names a series SERIES does not know */
            break;
        }
        rowctx r = {.d = &d, .series = ser[i], .w = type_width(type)};
        rc = eng_query_raw_parts(db, set, n, ser[i], cut_of(cs, ser[i]), INT64_MAX, row_cb, &r);
        if (rc == 0 && r.rows) {
            uint8_t tail[12];
            memcpy(tail, &ser[i], 4);
            memcpy(tail + 4, &r.rows, 8);
            dh_put(&d, tail, sizeof tail);
            out->rows += r.rows;
            out->series++;
        }
    }
    out->tombs = tombs < 0 ? (ecdigest_mutant == ECDIGEST_MUT_BELOWTRUE || below_listed(db, set, n, key)) : tombs;
    if (rc == 0 && ecdigest_mutant != ECDIGEST_MUT_NOTOMB && out->tombs) {
        int64_t klo, khi;
        partition_range(db, key, &klo, &khi);
        tomb_set ts = {0};
        for (size_t i = 0; rc == 0 && i < n; i++)
            if (set[i]->nt)
                rc = tomb_set_add(&ts, set[i]->t, set[i]->nt);
        tomb_set_finish(&ts);
        bool tagged = false;
        for (size_t i = 0; rc == 0 && i < ts.n; i++) {
            int64_t lo = MAX(ts.t[i].start, MAX(cut_of(cs, ts.t[i].series), klo)), hi = MIN(ts.t[i].end, khi);
            if (lo >= hi)
                continue;
            if (!tagged) { /* no series id is 0xffffffff: the rows end here */
                uint32_t tag = UINT32_MAX;
                dh_put(&d, &tag, 4);
                tagged = true;
            }
            uint8_t rec[20];
            memcpy(rec, &ts.t[i].series, 4);
            memcpy(rec + 4, &lo, 8);
            memcpy(rec + 12, &hi, 8);
            dh_put(&d, rec, sizeof rec);
        }
        tomb_set_free(&ts);
    }
    if (rc == 0)
        out->xxh64 = dh_end(&d);
    free(d.buf);
    free(ser);
    free(set);
    return rc;
}

int ec_logical_digest(eng *db, part **set, size_t n, int64_t key, int64_t now_ms, ec_ldigest *out)
{
    cutsrc cs = {.db = db, .now = now_ms};
    return digest_run(db, set, n, key, &cs, -1, out);
}

int ec_logical_digest_frozen(eng *db, part **set, size_t n, int64_t key, const ecd_cut *cuts, size_t ncuts,
                             bool tombs, ec_ldigest *out)
{
    cutsrc cs = {.db = db, .cuts = cuts ? cuts : (const ecd_cut *)"", .ncuts = cuts ? ncuts : 0};
    return digest_run(db, set, n, key, &cs, ecdigest_mutant == ECDIGEST_MUT_LOCALBELOW ? -1 : tombs, out);
}

int ecd_freeze(eng *db, part **set, size_t n, int64_t key, int64_t now_ms, ecd_cut **cuts, size_t *ncuts)
{
    *cuts = NULL;
    *ncuts = 0;
    uint32_t *ser = NULL;
    size_t ns = 0;
    int rc = set_series(set, n, ecdigest_mutant != ECDIGEST_MUT_FREEZEROWS, &ser, &ns);
    if (rc)
        return rc;
    int64_t klo, khi;
    partition_range(db, key, &klo, &khi);
    ecd_cut *c = NULL;
    size_t k = 0, cap = 0;
    for (size_t i = 0; i < ns; i++) {
        int64_t cut = eng_cutoff_at(db, ser[i], now_ms);
        if (cut <= klo)
            continue; /* clips nothing of the key */
        if (k == cap) {
            cap = cap ? cap * 2 : 16;
            ecd_cut *g = realloc(c, cap * sizeof *g);
            if (!g) {
                free(c);
                free(ser);
                return TSDB_ERR_NOMEM;
            }
            c = g;
        }
        c[k++] = (ecd_cut){.series = ser[i], .cutoff = cut};
    }
    free(ser);
    *cuts = c;
    *ncuts = k;
    return 0;
}

/* ------------------------------------------------------------ the leader */

static int leader_digest_ex(eng *db, int64_t key, uint64_t z, part **set, size_t n, part **out_set, size_t nout,
                            const uint8_t *oids, size_t noids, uint64_t *pos_out)
{
    if (pos_out)
        *pos_out = 0;
    if (noids > MK_FILES_MAX)
        return TSDB_ERR_INVALID;
    /* The clock, the cutoffs and the tombstone decision are taken under ret_lock, the rows are read without it, and the
     * marker goes out under it again only while no limit changed in between (ret_gen; JR_RETENTION appends under
     * ret_lock, so no RETENTION marker lies between those limits and the DIGEST); else the digest is taken again, the
     * last time wholly under the lock. The commit wait runs with no lock: a retention change and a snapshot's barrier
     * (src/snapshot.c) wait only for the short holds. The clock travels in the DIGEST (MK_NOW markers of the flusher
     * may come between, and a replica's clock is their maximum). */
    const char *what = "a settle's DIGEST marker was appended but its commit point did not come";
    uint64_t last = 0;
    int rc;
    bool mismatch = false;
    ec_ldigest d_in = {0}, d_out = {0};
    for (int tries = 0;; tries++) {
        bool under = tries >= 3;
        pthread_mutex_lock(&db->ret_lock);
        uint32_t gen = atomic_load(&db->ret_gen);
        int64_t now = eng_now_ms(db);
        bool tombs = ecdigest_mutant == ECDIGEST_MUT_BELOWTRUE || below_listed(db, set, n, key);
        ecd_cut *cuts = NULL;
        size_t nc = 0;
        rc = ecd_freeze(db, set, n, key, now, &cuts, &nc);
        if (!under)
            pthread_mutex_unlock(&db->ret_lock);
        if (rc == 0)
            rc = ec_logical_digest_frozen(db, set, n, key, cuts, nc, tombs, &d_in);
        if (rc == 0 && out_set)
            rc = ec_logical_digest_frozen(db, out_set, nout, key, cuts, nc, tombs, &d_out);
        free(cuts);
        if (!under)
            pthread_mutex_lock(&db->ret_lock);
        if (rc == 0 && atomic_load(&db->ret_gen) != gen) {
            pthread_mutex_unlock(&db->ret_lock);
            continue;
        }
        if (rc == 0 && out_set &&
            (d_in.rows != d_out.rows || d_in.series != d_out.series || d_in.xxh64 != d_out.xxh64)) {
            mismatch = true;
            rc = TSDB_ERR_CORRUPT;
            pthread_mutex_unlock(&db->ret_lock);
            break;
        }
        if (rc == 0) {
            mk m;
            memset(&m, 0, sizeof m);
            m.type = MK_DIGEST;
            m.span = 1;
            m.flags = tombs ? MKD_TOMBS : 0;
            m.u.digest = (mk_digest){.key = key, .z = z, .rows = d_in.rows, .series = d_in.series,
                                     .xxh64 = d_in.xxh64, .now_ms = now, .noids = (uint32_t)noids, .oids = oids};
            rc = jr_markers_append(db, &m, 1, what, &last);
        }
        pthread_mutex_unlock(&db->ret_lock);
        break;
    }
    if (mismatch) {
        fprintf(stderr, "tsdb: %s: re-encode: key %" PRId64 ": leader logical digest mismatch (retired %" PRIu64
                " rows/%" PRIu64 " series/%016" PRIx64 ", output %" PRIu64 "/%" PRIu64 "/%016" PRIx64
                "): volume is DIVERGED, retired objects kept\n",
                db->dir, key, d_in.rows, d_in.series, d_in.xxh64, d_out.rows, d_out.series, d_out.xxh64);
        ec_diverge(db, key, noids ? oids : NULL, EC_DIV_DIGEST);
        return TSDB_ERR_CORRUPT;
    }
    if (rc == 0)
        rc = jr_markers_wait(db, last, what);
    if (rc == 0 && pos_out)
        *pos_out = last;
    if (rc && rc != TSDB_ERR_INDETERMINATE)
        fprintf(stderr, "tsdb: %s: settle: key %" PRId64 ": the DIGEST marker was not appended (%d)\n", db->dir, key,
                rc);
    return rc;
}

int ecdigest_leader(eng *db, int64_t key, uint64_t z, part **set, size_t n, const uint8_t *oids, size_t noids)
{
    return leader_digest_ex(db, key, z, set, n, NULL, 0, oids, noids, NULL);
}

int ecdigest_leader_reencode(eng *db, int64_t key, uint64_t z, part **in_set, size_t nin, part **out_set, size_t nout,
                             const uint8_t *oids, size_t noids, uint64_t *pos_out)
{
    return leader_digest_ex(db, key, z, in_set, nin, out_set, nout, oids, noids, pos_out);
}

/* ------------------------------------------------------------ a replica's jobs */

/* The job file (local state of this replica, host byte order: every member is little-endian x86-64 or arm64; the
 * digest's stream is host order too, so a big-endian member would need both swapped): "TSDBDGJ2", key, z, flags, rows, series, xxh64, oid[16],
 * ncuts u32, nnames u32, ncuts x (series u32, cutoff i64), nnames x (len u16, name), then the xxh64 of all before. */
#define JOB_MAGIC "TSDBDGJ2"

typedef struct jb {
    uint8_t *b;
    size_t n, cap;
    bool err;
} jb;

static void jput(jb *w, const void *p, size_t n)
{
    if (w->err)
        return;
    if (w->n + n > w->cap) {
        size_t cap = w->cap ? w->cap : 256;
        while (cap < w->n + n)
            cap *= 2;
        uint8_t *b = realloc(w->b, cap);
        if (!b) {
            w->err = true;
            return;
        }
        w->b = b, w->cap = cap;
    }
    memcpy(w->b + w->n, p, n);
    w->n += n;
}

void ecd_job_free(ecd_job *j)
{
    free(j->cuts);
    free(j->names);
    j->cuts = NULL, j->names = NULL;
    j->ncuts = j->nnames = 0;
}

bool ecdigest_job_file(const char *name) { return strncmp(name, "digest_", 7) == 0; }

int ecdigest_job_write(eng *db, const ecd_job *j)
{
    jb w = {0};
    uint32_t nc = (uint32_t)j->ncuts, nn = (uint32_t)j->nnames;
    jput(&w, JOB_MAGIC, 8);
    jput(&w, &j->key, 8);
    jput(&w, &j->z, 8);
    jput(&w, &j->flags, 4);
    jput(&w, &j->want.rows, 8);
    jput(&w, &j->want.series, 8);
    jput(&w, &j->want.xxh64, 8);
    jput(&w, j->oid, 16);
    jput(&w, &nc, 4);
    jput(&w, &nn, 4);
    for (size_t i = 0; i < j->ncuts; i++) {
        jput(&w, &j->cuts[i].series, 4);
        jput(&w, &j->cuts[i].cutoff, 8);
    }
    for (size_t i = 0; i < j->nnames; i++) {
        uint16_t l = (uint16_t)strlen(j->names[i]);
        jput(&w, &l, 2);
        jput(&w, j->names[i], l);
    }
    uint64_t h = w.err ? 0 : xxh64(w.b, w.n, 0);
    jput(&w, &h, 8);
    if (w.err) {
        free(w.b);
        return TSDB_ERR_NOMEM;
    }
    char tmp[80];
    snprintf(tmp, sizeof tmp, "%s.tmp", j->file);
    int rc = 0;
    int fd = openat(db->dirfd, tmp, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0 || write(fd, w.b, w.n) != (ssize_t)w.n ||
        (ecdigest_mutant != ECDIGEST_MUT_JOBNOSYNC && fdatasync(fd) != 0))
        rc = TSDB_ERR_IO;
    if (fd >= 0)
        close(fd);
    free(w.b);
    if (rc == 0 && renameat(db->dirfd, tmp, db->dirfd, j->file) != 0)
        rc = TSDB_ERR_IO;
    if (rc) {
        fprintf(stderr, "tsdb: %s: the digest job %s was not written: %s; its files are held until an operator acts\n",
                db->dir, j->file, strerror(errno));
        unlinkat(db->dirfd, tmp, 0);
        return rc;
    }
    return eng_sync_dir(db, "fsync of the directory after a digest job file"); /* fail-stop: the job must hold */
}

static int job_read(eng *db, const char *file, ecd_job *j)
{
    memset(j, 0, sizeof *j);
    int fd = openat(db->dirfd, file, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return TSDB_ERR_IO;
    struct stat st;
    uint8_t *b = NULL;
    int rc = TSDB_ERR_CORRUPT;
    if (fstat(fd, &st) == 0 && st.st_size >= 84 && st.st_size < (1 << 26) && (b = malloc((size_t)st.st_size)) &&
        pread(fd, b, (size_t)st.st_size, 0) == st.st_size) {
        size_t n = (size_t)st.st_size, off = 8;
        uint64_t h;
        memcpy(&h, b + n - 8, 8);
        if (memcmp(b, JOB_MAGIC, 8) == 0 && xxh64(b, n - 8, 0) == h) {
            uint32_t nc, nn;
            memcpy(&j->key, b + off, 8), off += 8;
            memcpy(&j->z, b + off, 8), off += 8;
            memcpy(&j->flags, b + off, 4), off += 4;
            memcpy(&j->want.rows, b + off, 8), off += 8;
            memcpy(&j->want.series, b + off, 8), off += 8;
            memcpy(&j->want.xxh64, b + off, 8), off += 8;
            memcpy(j->oid, b + off, 16), off += 16;
            memcpy(&nc, b + off, 4), off += 4;
            memcpy(&nn, b + off, 4), off += 4;
            n -= 8;
            if ((uint64_t)nc * 12 <= n - off && nn <= MK_FILES_MAX * 2 &&
                (j->cuts = calloc(nc ? nc : 1, sizeof *j->cuts)) && (j->names = calloc(nn ? nn : 1, 256))) {
                rc = 0;
                for (uint32_t i = 0; i < nc; i++) {
                    memcpy(&j->cuts[i].series, b + off, 4);
                    memcpy(&j->cuts[i].cutoff, b + off + 4, 8);
                    off += 12;
                }
                j->ncuts = nc;
                for (uint32_t i = 0; i < nn && rc == 0; i++) {
                    uint16_t l;
                    if (n - off < 2 || (memcpy(&l, b + off, 2), off += 2, l == 0 || l > 255 || n - off < l)) {
                        rc = TSDB_ERR_CORRUPT;
                        break;
                    }
                    memcpy(j->names[i], b + off, l);
                    j->names[i][l] = 0;
                    off += l;
                    if ((!settled_part_name(j->names[i]) && !ec_retired_name(j->names[i])) || strchr(j->names[i], '/'))
                        rc = TSDB_ERR_CORRUPT;
                }
                j->nnames = nn;
                if (rc == 0 && off != n)
                    rc = TSDB_ERR_CORRUPT;
                char want[64];
                snprintf(want, sizeof want, "digest_%" PRId64 "_%" PRIu64, j->key, j->z);
                if (rc == 0 && strcmp(want, file) != 0)
                    rc = TSDB_ERR_CORRUPT; /* a job file under another job's name */
            } else if (!j->cuts || !j->names) {
                rc = nc * 12 <= n - off && nn <= MK_FILES_MAX * 2 ? TSDB_ERR_NOMEM : TSDB_ERR_CORRUPT;
            }
        }
    }
    free(b);
    close(fd);
    snprintf(j->file, sizeof j->file, "%s", file);
    if (rc)
        ecd_job_free(j);
    return rc;
}

/* the files of a job as parts (their source names: p<K>_<A>_<B>_<L>.part) */
/* the copies of a DIGEST that open, for its frozen cutoffs; one missing or whose header, index or footer is refused is
 * left out (*got < n): the job's check finds it again and counts it lost (D25), so a lost copy never holds the rest */
static int open_set(eng *db, char (*names)[256], size_t n, part ***out, size_t *got)
{
    part **set = calloc(n ? n : 1, sizeof *set);
    if (!set)
        return TSDB_ERR_NOMEM;
    int rc = 0;
    size_t m = 0;
    for (size_t i = 0; i < n && rc == 0; i++) {
        if (ec_retired_name(names[i]))
            rc = part_open_ec(db, names[i], names[i] + 8, &set[m]);
        else
            rc = part_open_as(db->dirfd, names[i], names[i] + 8, false, false, &set[m]);
        if (rc == 0)
            set[m++]->verify = true; /* a copy's granule that fails its checksum is rot (D25), never hashed */
        else if (rc == TSDB_ERR_NOTFOUND || rc == TSDB_ERR_CORRUPT) {
            fprintf(stderr, "tsdb: %s: retired copy %s %s at its DIGEST: counted lost when its job runs\n", db->dir,
                    names[i], rc == TSDB_ERR_NOTFOUND ? "is missing" : "is refused (rot)");
            set[m] = NULL;
            rc = 0;
        }
    }
    if (rc) {
        for (size_t i = 0; i < m; i++)
            part_unref(set[i]);
        free(set);
        return rc;
    }
    *out = set;
    *got = m;
    return 0;
}

static void close_set(part **set, size_t n)
{
    for (size_t i = 0; i < n; i++)
        part_unref(set[i]);
    free(set);
}

static void job_name(char *out, size_t n, int64_t key, uint64_t z)
{
    snprintf(out, n, "digest_%" PRId64 "_%" PRIu64, key, z);
}

/* an unarmed job over names[0 .. n) (taken), durable, then known to eckeep */
static void bind_job(eng *db, int64_t key, uint64_t z, char (*names)[256], size_t n)
{
    ecd_job j;
    memset(&j, 0, sizeof j);
    j.key = key;
    j.z = z;
    job_name(j.file, sizeof j.file, key, z);
    j.names = names;
    j.nnames = n;
    if (ecdigest_job_write(db, &j) == 0) /* logged when not: held, z known until the process ends */
        (void)eckeep_add_job(db, &j);
    ecd_job_free(&j);
}

void ecdigest_cold_bind_ex(eng *db, int64_t key, uint64_t z, part *const *own, size_t nown, part *const *ret_ec,
                           size_t nret_ec)
{
    size_t total = nown + nret_ec;
    if (!eckeep_active(db) || total == 0)
        return;
    eckeep_jobs_load(db);
    char (*names)[256] = malloc(total * sizeof *names);
    if (!names) {
        fprintf(stderr, "tsdb: %s: the retired copies of the COLD at Z %" PRIu64 " (key %" PRId64 ") have no job file "
                        "(out of memory): a restart holds them unbound\n", db->dir, z, key);
        return;
    }
    for (size_t i = 0; i < nown; i++)
        snprintf(names[i], 256, "settled_%s", own[i]->name);
    for (size_t i = 0; i < nret_ec; i++)
        snprintf(names[nown + i], 256, "retired_%s", ret_ec[i]->name);
    bind_job(db, key, z, names, total);
}

void ecdigest_cold_bind(eng *db, int64_t key, uint64_t z, part *const *own, size_t nown)
{
    ecdigest_cold_bind_ex(db, key, z, own, nown, NULL, 0);
}

void ecdigest_cold_rebind_ex(eng *db, int64_t key, uint64_t z, const uint8_t *retired, uint32_t nretired)
{
    if (!eckeep_active(db))
        return;
    eckeep_jobs_load(db);
    char file[64], (*names)[256] = NULL;
    size_t n = 0;
    job_name(file, sizeof file, key, z);
    if (eckeep_job_names(db, file, key, &names, &n) != TSDB_ERR_NOTFOUND) {
        free(names); /* bound already (or armed) */
        return;
    }
    n = eckeep_unbound(db, key, &names);
    char (*rnames)[256] = NULL;
    size_t nr = 0;
    if (nretired && retired)
        (void)ecgc_find_retired_names(db, key, retired, nretired, &rnames, &nr);
    if (n + nr == 0) {
        free(names);
        free(rnames);
        return;
    }
    if (nr) {
        char (*all)[256] = realloc(names, (n + nr) * sizeof *all);
        if (!all) {
            free(names);
            free(rnames);
            return;
        }
        memcpy(all + n, rnames, nr * sizeof *all);
        names = all;
        n += nr;
        free(rnames);
    }
    bind_job(db, key, z, names, n);
}

void ecdigest_cold_rebind(eng *db, int64_t key, uint64_t z)
{
    ecdigest_cold_rebind_ex(db, key, z, NULL, 0);
}

int ecdigest_apply(eng *db, uint64_t pos, const mk *m)
{
    const mk_digest *d = &m->u.digest;
    if (!eckeep_active(db))
        return 0;
    eckeep_jobs_load(db);
    ecd_job j;
    memset(&j, 0, sizeof j);
    j.key = d->key;
    j.z = d->z;
    j.flags = ECDJ_ARMED | ((m->flags & MKD_TOMBS) ? ECDJ_TOMBS : 0);
    j.want = (ec_ldigest){.rows = d->rows, .series = d->series, .xxh64 = d->xxh64};
    if (d->noids)
        memcpy(j.oid, d->oids, 16);
    job_name(j.file, sizeof j.file, d->key, d->z);
    int rc = eckeep_job_names(db, j.file, d->key, &j.names, &j.nnames);
    if (rc == TSDB_ERR_NOTFOUND || rc == ECD_ARMED) { /* nothing retired here for it (no tier then, or a COLD applied
                                                       * before this member joined), or applied again at a replay */
        free(j.names);
        eckeep_supersede(db, d->key, d->z);
        return 0;
    }
    part **set = NULL;
    size_t got = 0;
    if (rc == 0 && (rc = open_set(db, j.names, j.nnames, &set, &got)) == 0) {
        /* the leader's clock and the limits as of this position (the RETENTION markers before it are applied) */
        rc = ecd_freeze(db, set, got, d->key, d->now_ms, &j.cuts, &j.ncuts);
        close_set(set, got);
    }
    /* R10 (r10-crepl): when the retired set includes retired_*.ec objects, verify the logical digest synchronously
     * (dropping rp->mu) BEFORE writing ECDJ_ARMED and BEFORE acknowledging MK_DIGEST so the leader's
     * lead_applied_floor gate keeps those objects' chunks alive on the stripe group until every log replica has
     * actually verified them (never acknowledging past a store outage or read failure). */
    bool has_ec = false;
    for (size_t i = 0; i < j.nnames && !has_ec; i++)
        has_ec = ec_retired_name(j.names[i]);
    if (rc == 0 && has_ec && db->rp && !atomic_load(&eckeep_test_pause)) {
        int64_t retry_ms = atomic_load(&ecdigest_test_retry_ms);
        if (retry_ms < 0)
            retry_ms = 30000;
        int64_t t0 = eng_mono_ms();
        bool logged_unavail = false;
        ec_ldigest got_d = {0};
        size_t bad = 0;
        int chk = 0;
        uint64_t gen = replica_mu_drop(db);
        for (;;) {
            memset(&got_d, 0, sizeof got_d);
            bad = 0;
            chk = ecdigest_job_check(db, &j, &got_d, &bad);
            if (chk != TSDB_ERR_UNAVAILABLE || atomic_load_explicit(&db->stop, memory_order_acquire) ||
                atomic_load_explicit(&db->replica_stop, memory_order_acquire) || eng_check(db) != 0 ||
                eng_mono_ms() - t0 >= retry_ms)
                break;
            if (!logged_unavail) {
                fprintf(stderr, "tsdb: %s: MK_DIGEST at %" PRIu64 " (key %" PRId64
                                "): chunk stores unavailable during retired .ec digest; retrying\n",
                        db->dir, pos, d->key);
                logged_unavail = true;
            }
            struct timespec ts = {0, 200 * 1000000L};
            nanosleep(&ts, NULL);
            if (replica_mu_retake(db, gen)) {
                ecd_job_free(&j);
                return RP_AGAIN;
            }
            gen = replica_mu_drop(db);
        }
        bool match = chk == 0 &&
                     ((got_d.rows == j.want.rows && got_d.series == j.want.series && got_d.xxh64 == j.want.xxh64) ||
                      ecdigest_mutant == ECDIGEST_MUT_SKIP);
        bool mismatched = (chk == 0 && !match) || chk == ECD_REFUSED;
        if (match) {
            j.flags |= ECDJ_MATCHED;
            rc = ecdigest_job_write(db, &j);
        } else if (mismatched) {
            if (chk == ECD_REFUSED)
                fprintf(stderr, "tsdb: volume %" PRIu32 " partition %" PRId64 ": the %zu retired copies of the COLD "
                        "at Z %" PRIu64 " cannot be digested: the volume is DIVERGED and the copies are kept\n",
                        db->tier_vol, j.key, j.nnames, j.z);
            else
                fprintf(stderr, "tsdb: volume %" PRIu32 " partition %" PRId64 ": the logical digest of the %zu retired "
                        "copies of the COLD at Z %" PRIu64 " is %" PRIu64 " rows, %" PRIu64 " series, %016" PRIx64
                        "; its DIGEST says %" PRIu64 ", %" PRIu64 ", %016" PRIx64 ": the volume is DIVERGED and the "
                        "copies are kept\n", db->tier_vol, j.key, j.nnames, j.z, got_d.rows, got_d.series, got_d.xxh64,
                        j.want.rows, j.want.series, j.want.xxh64);
            ec_diverge(db, j.key, j.oid, EC_DIV_DIGEST);
            (void)ecdigest_job_write(db, &j);
        }
        if (replica_mu_retake(db, gen)) {
            ecd_job_free(&j);
            return RP_AGAIN;
        }
        if (mismatched) {
            (void)eckeep_add_job(db, &j);
            return TSDB_ERR_CORRUPT;
        }
        if (chk != 0) {
            /* Do NOT arm or acknowledge MK_DIGEST: keep applied < pos so lead_applied_floor holds MK_GC back until
             * this replica reopens and verifies the retired set (or an operator intervenes). */
            fprintf(stderr, "tsdb: %s: MK_DIGEST at %" PRIu64 " (key %" PRId64 "): synchronous digest of %zu retired "
                            "copies failed (%d, %zu bad): not acknowledging so retired chunks stay held\n",
                    db->dir, pos, d->key, j.nnames, chk, bad);
            ecd_job_free(&j);
            return chk;
        }
        if (rc) {
            fprintf(stderr, "tsdb: %s: the DIGEST at %" PRIu64 " (key %" PRId64 "): no job (%d); its %zu retired "
                            "files are held until an operator acts\n", db->dir, pos, d->key, rc, j.nnames);
            ecd_job_free(&j);
            return eng_check(db) ? eng_check(db) : rc;
        }
    } else {
        if (rc == 0)
            rc = ecdigest_job_write(db, &j);
        if (rc) {
            fprintf(stderr, "tsdb: %s: the DIGEST at %" PRIu64 " (key %" PRId64 "): no job (%d); its %zu retired "
                            "files are held until an operator acts\n", db->dir, pos, d->key, rc, j.nnames);
            ecd_job_free(&j);
            return eng_check(db) ? eng_check(db) : 0; /* held, never released unchecked; a failed engine stops the apply */
        }
    }
    rc = eckeep_add_job(db, &j); /* takes the job */
    eckeep_supersede(db, d->key, d->z);
    return rc;
}

int ecdigest_job_check(eng *db, const ecd_job *j, ec_ldigest *got, size_t *bad)
{
    *bad = 0;
    part **set = calloc(j->nnames ? j->nnames : 1, sizeof *set);
    if (!set)
        return TSDB_ERR_NOMEM;
    int rc = 0;
    size_t missing = 0, rotted = 0;
    bool has_ec = false;
    for (size_t i = 0; i < j->nnames && rc == 0; i++) {
        bool is_ec = ec_retired_name(j->names[i]);
        has_ec |= is_ec;
        int r = is_ec ? part_open_ec(db, j->names[i], j->names[i] + 8, &set[i])
                      : part_open_as(db->dirfd, j->names[i], j->names[i] + 8, false, false, &set[i]);
        if (r == TSDB_ERR_NOTFOUND) {
            missing++;
            continue;
        }
        if (r == TSDB_ERR_CORRUPT) { /* header, index or footer refused */
            rotted++;
            continue;
        }
        if (r) {
            rc = r; /* a read error says nothing: try again */
            break;
        }
        set[i]->verify = true; /* a granule that fails its checksum while digesting is caught as well */
        if (is_ec)
            continue; /* .ec granules are checked via the chunk seam (granule_read + encode-time cell sums) */
        bool rot = false; /* the whole copy, every granule: the digest reads only rows the cutoff and the tombstones
                           * leave */
        for (uint32_t g = 0; g < set[i]->ng && !rot && ecdigest_mutant != ECDIGEST_MUT_NOCRCWALK; g++) {
            granule gr;
            gx_load(set[i], g, GW_LOC, &gr);
            uint64_t len = (uint64_t)gr.ts_len + gr.val_len;
            rot = gr.off < set[i]->hdr_len || gr.off > set[i]->index_off || len > set[i]->index_off - gr.off ||
                  (uint32_t)xxh64(set[i]->map + gr.off, len, 0) != gr.crc;
        }
        if (rot) {
            fprintf(stderr, "tsdb: %s: retired copy %s fails a granule checksum (rot)\n", db->dir, j->names[i]);
            rotted++;
        }
    }
    if (rc == 0 && (missing || rotted)) {
        *bad = missing + rotted;
        rc = rotted ? TSDB_ERR_CORRUPT : TSDB_ERR_NOTFOUND;
    }
    if (rc == 0) {
        alter_next = atomic_exchange(&ecdigest_test_alter, false);
        rc = ec_logical_digest_frozen(db, set, j->nnames, j->key, j->cuts, j->ncuts, (j->flags & ECDJ_TOMBS) != 0,
                                      got);
        alter_next = false;
        if (rc == TSDB_ERR_CORRUPT) {
            if (!has_ec) /* every checksum held: what the digest refused is the rows themselves */
                rc = ECD_REFUSED;
            else
                *bad = 1;
        } else if (rc == TSDB_ERR_NOTFOUND) {
            *bad = 1;
        }
    }
    close_set(set, j->nnames);
    return rc;
}

void ecdigest_load_jobs(eng *db)
{
    int fd = dup(db->dirfd);
    DIR *d = fd >= 0 ? fdopendir(fd) : NULL;
    if (!d) {
        if (fd >= 0)
            close(fd);
        fprintf(stderr, "tsdb: %s: the digest jobs were not listed: %s; the replica's retired files are held\n",
                db->dir, strerror(errno));
        return;
    }
    rewinddir(d);
    struct dirent *de;
    while ((errno = 0, de = readdir(d))) {
        if (!ecdigest_job_file(de->d_name))
            continue;
        size_t l = strlen(de->d_name);
        if (l > 4 && strcmp(de->d_name + l - 4, ".tmp") == 0) { /* never renamed: its marker is applied again */
            unlinkat(db->dirfd, de->d_name, 0);
            continue;
        }
        ecd_job j;
        int rc = job_read(db, de->d_name, &j);
        if (rc) {
            fprintf(stderr, "tsdb: %s: digest job %s cannot be read (%d): kept, its files are held\n", db->dir,
                    de->d_name, rc);
            continue;
        }
        (void)eckeep_add_job(db, &j);
    }
    if (errno)
        fprintf(stderr, "tsdb: %s: the listing of digest jobs failed: %s; the files it did not reach are held\n",
                db->dir, strerror(errno));
    closedir(d);
}

void ecdigest_job_remove(eng *db, const char *file)
{
    if (ecdigest_mutant == ECDIGEST_MUT_NOJOBREMOVE)
        return;
    if (unlinkat(db->dirfd, file, 0) != 0 && errno != ENOENT) {
        fprintf(stderr, "tsdb: %s: unlink of digest job %s: %s\n", db->dir, file, strerror(errno));
        return;
    }
    (void)eng_sync_dir(db, "fsync of the directory after a digest job's removal");
}

bool ecdigest_copy_rotted(eng *db, const char *name)
{
    part *p = NULL;
    int rc = part_open_as(db->dirfd, name, name + 8, false, false, &p);
    if (rc)
        return rc == TSDB_ERR_CORRUPT; /* header, index or footer refused; a read error says nothing */
    bool rot = false;
    for (uint32_t g = 0; g < p->ng && !rot; g++) {
        granule gr;
        gx_load(p, g, GW_LOC, &gr);
        uint64_t len = (uint64_t)gr.ts_len + gr.val_len;
        rot = gr.off < p->hdr_len || gr.off > p->index_off || len > p->index_off - gr.off ||
              (uint32_t)xxh64(p->map + gr.off, len, 0) != gr.crc;
    }
    part_unref(p);
    return rot;
}
