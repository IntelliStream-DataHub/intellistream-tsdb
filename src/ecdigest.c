/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The logical digest (src/ecdigest.h has the design): the digest itself, the leader's marker, and a replica's jobs
 * (the comparison runs on eckeep's thread, src/eckeep.c).
 */
#include "internal.h"
#include "ecdigest.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int ecdigest_mutant;

int ecdigest_mutant_parse(const char *name)
{
    static const char *const names[] = {"none", "digestbytes", "digestnotomb", "digestskip", "digestnoretention"};
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
        memcpy(rec + 4, &ts[i], 8);
        memcpy(rec + 12, (const uint8_t *)v + i * w, w);
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

/* the distinct series of the set, sorted */
static int set_series(part **set, size_t n, uint32_t **out, size_t *nout)
{
    size_t cap = 0;
    for (size_t i = 0; i < n; i++)
        cap += set[i]->ng ? set[i]->ix.nruns : 0;
    uint32_t *s = malloc((cap ? cap : 1) * sizeof *s);
    if (!s)
        return TSDB_ERR_NOMEM;
    size_t k = 0;
    for (size_t i = 0; i < n; i++)
        for (uint32_t r = 0; set[i]->ng && r < set[i]->ix.nruns; r++)
            s[k++] = set[i]->ix.rser[r];
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

static int digest_run(eng *db, part **in, size_t n, int64_t key, const cutsrc *cs, ec_ldigest *out)
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
    int rc = d.buf ? set_series(set, n, &ser, &ns) : TSDB_ERR_NOMEM;
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
    if (rc == 0 && ecdigest_mutant != ECDIGEST_MUT_NOTOMB && below_listed(db, set, n, key)) {
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
    return digest_run(db, set, n, key, &cs, out);
}

int ec_logical_digest_frozen(eng *db, part **set, size_t n, int64_t key, const ecd_cut *cuts, size_t ncuts,
                             ec_ldigest *out)
{
    cutsrc cs = {.db = db, .cuts = cuts ? cuts : (const ecd_cut *)"", .ncuts = cuts ? ncuts : 0};
    return digest_run(db, set, n, key, &cs, out);
}

int ecd_freeze(eng *db, part **set, size_t n, int64_t key, int64_t now_ms, ecd_cut **cuts, size_t *ncuts)
{
    *cuts = NULL;
    *ncuts = 0;
    uint32_t *ser = NULL;
    size_t ns = 0;
    int rc = set_series(set, n, &ser, &ns);
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

int ecdigest_leader(eng *db, int64_t key, uint64_t z, part **set, size_t n, const uint8_t *oids, size_t noids)
{
    if (noids > MK_FILES_MAX)
        return TSDB_ERR_INVALID;
    /* ret_lock: no RETENTION marker between the digest's limits and its marker (JR_RETENTION appends under it) */
    pthread_mutex_lock(&db->ret_lock);
    int64_t now = eng_now_ms(db);
    ec_ldigest d;
    int rc = ec_logical_digest(db, set, n, key, now, &d);
    if (rc == 0) {
        mk m[2];
        memset(m, 0, sizeof m);
        m[0].type = MK_NOW;
        m[0].span = 1;
        m[0].u.now.now_ms = now; /* the clock the replicas digest at: theirs at the DIGEST is this */
        m[1].type = MK_DIGEST;
        m[1].span = 1;
        m[1].u.digest = (mk_digest){.key = key, .z = z, .rows = d.rows, .series = d.series, .xxh64 = d.xxh64,
                                    .noids = (uint32_t)noids, .oids = oids};
        rc = jr_markers(db, m, 2, "a settle's DIGEST marker was appended but its commit point did not come");
    }
    pthread_mutex_unlock(&db->ret_lock);
    if (rc && rc != TSDB_ERR_INDETERMINATE)
        fprintf(stderr, "tsdb: %s: settle: key %" PRId64 ": the DIGEST marker was not appended (%d)\n", db->dir, key,
                rc);
    return rc;
}

/* ------------------------------------------------------------ a replica's jobs */

int ecdigest_apply(eng *db, uint64_t pos, const mk *m)
{
    (void)db, (void)pos, (void)m;
    return 0;
}
