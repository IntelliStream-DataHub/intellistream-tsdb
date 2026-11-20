/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The settle: a part becomes an object (R7, r7-settle; design-cluster-data.md E5-E7, E26, §2.3 steps 1-2 and 6).
 *
 * eng_settle pins its parts (`settling`, under db->lock: no merge, rewrite or TTL selection takes them from then on;
 * a drop still may, and the swap's check under db->lock sees it), then encodes each part's file unchanged (E7) into
 * an object of the tier's chunk store, in ONE pass over the map (E26, chunk_be_put_stream): every stripe is copied out
 * of the map once, and the copy is what is checked and what is encoded. Two reads of a mapping can see two different
 * pages (a misdirected write, a torn page that the page cache drops and reads again), so a check that read the map and
 * an encode that read it again could pass damage into an object that then carries a fresh, valid set of sums. In fill:
 *
 *  - every granule whose bytes end in the stripe is checked against the crc of the index (a granule that straddles
 *    two stripes is assembled from both copies first);
 *  - the header, index, tombstones and footer were copied out and hashed before the encode (their hash must be the
 *    one the part's open checked, p->index_hash, and the footer's own): the stripes that hold them must hand over
 *    the same bytes (a byte compare against that early copy), so the hash checked is the hash of what is encoded;
 *  - the rows are judged against CLASSES as the open's cls_check_parts would (src/classes.c check_granule): from the
 *    granule bounds, and where a granule spans a switch of its series, from its timestamps, decoded from the copy.
 *    An .ec part's granules are never decoded at open: the record's class_check (EC_CLS_PASSED) stands for this.
 *    A switch after the check cannot contradict the part: a switch's T lies past every row of its series in parts;
 *  - the object's xxh64 is folded stripe by stripe (a running hash: the object is never in memory whole).
 * A mismatch aborts the put (no chunk is committed before the last stripe), marks the part damaged with
 * part_mark_payload's rules (in memory: out of merges and drops; a restart reads it again) and logs.
 *
 * Then: the geometry of E7 (chunk_geom_derive: cell = clamp(L / 8k, 64 KiB, 1 MiB) in 4 KiB, k' = min(k, cells), m from
 * the tier), a random oid, epoch 1 (D4), the record (settle_ms from eng_now_ms, class_check PASSED), ec_index_write's
 * tmp_*.ec, part_open_ec of it, and one ec_swap_commit for all the parts. Crash points of TSDB_SETTLE_CRASH_AT:
 * after-put (the chunks are durable) and after-ec-tmp (the tmp_*.ec too), read by r7-swap's helper. A settle that does
 * not swap clears `settling` and hands its objects to r7-gc (ecgc_unlisted_object): the open's sweep (D6) is the
 * backstop for a crash in between.
 *
 * The thread (D11): one per tier, nice 19, one settle at a time over the tier's engines (registered by
 * ecsettle_eng_ready at the end of a writer's open, never a read-only engine, a follower, a cluster or a replica
 * volume). It never holds jr_serial, flush_lock or cls_rw: a settle takes db->lock only to pin and to unpin, and the
 * swap takes publish_lock > latest_rw > db->lock like a publisher. Inserts never wait for it (a settling part still
 * counts towards a partition's parts: src/db.c). eng_halt and eng_close stop it for their engine (ecsettle_eng_stop):
 * db->stop is set by then, which the next fill sees (STOPPED: the put leaves no chunk), and the stop waits for the
 * settle to unwind before the parts go.
 */
#define _GNU_SOURCE
#include "settle.h"
#include "chunk.h"
#include "ecgc.h"
#include "ecpart.h"
#include "ecskip.h"
#include "ecswap.h"
#include "tier.h"

#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/resource.h>
#include <sys/syscall.h>
#endif

atomic_int settle_poll_ms = 1000;
void (*_Atomic settle_fill_hook)(eng *db, const part *p, uint64_t stripe);
atomic_uint_fast64_t settle_done, settle_failed, settle_abandoned, settle_rounds;
int settle_mutant;

int settle_mutant_parse(const char *name)
{
    static const char *const names[] = {"", "ecrankdown", "settlenocheck", "settletwopass", "settlebusy",
                                        "coarsewhole"};
    for (int i = 1; i < (int)(sizeof names / sizeof *names); i++)
        if (strcmp(name, names[i]) == 0)
            return i;
    return -1;
}

static pthread_once_t mut_once = PTHREAD_ONCE_INIT;
static void mut_env(void)
{
    const char *v = getenv("TSDB_SETTLE_MUTANT");
    int m = v && *v ? settle_mutant_parse(v) : 0;
    if (m < 0) {
        fprintf(stderr, "tsdb: TSDB_SETTLE_MUTANT=%s names no mutant: exiting with status 2\n", v);
        _exit(2);
    }
    if (m > 0)
        settle_mutant = m;
}

/* ------------------------------------------------------------ a streaming xxh64 (the same digest as xxh64()) */

#define P1 11400714785074694791ULL
#define P2 14029467366897019727ULL
#define P3 1609587929392839161ULL
#define P4 9650029242287828579ULL
#define P5 2870177450012600261ULL

typedef struct xxs {
    uint64_t v[4], total;
    uint8_t buf[32];
    size_t nbuf;
} xxs;

static inline uint64_t rotl64(uint64_t x, int r) { return (x << r) | (x >> (64 - r)); }
static inline uint64_t xround(uint64_t acc, uint64_t in) { return rotl64(acc + in * P2, 31) * P1; }
static inline uint64_t xmerge(uint64_t acc, uint64_t v) { return (acc ^ xround(0, v)) * P1 + P4; }
static inline uint64_t rd64le(const uint8_t *p)
{
    uint64_t v;
    memcpy(&v, p, 8);
    return v;
}
static inline uint32_t rd32le(const uint8_t *p)
{
    uint32_t v;
    memcpy(&v, p, 4);
    return v;
}

static void xxs_init(xxs *s)
{
    memset(s, 0, sizeof *s);
    s->v[0] = P1 + P2;
    s->v[1] = P2;
    s->v[2] = 0;
    s->v[3] = (uint64_t)0 - P1;
}

static void xxs_block(xxs *s, const uint8_t *p)
{
    for (int i = 0; i < 4; i++)
        s->v[i] = xround(s->v[i], rd64le(p + 8 * i));
}

static void xxs_update(xxs *s, const uint8_t *p, size_t n)
{
    s->total += n;
    if (s->nbuf) {
        size_t take = MIN(n, 32 - s->nbuf);
        memcpy(s->buf + s->nbuf, p, take);
        s->nbuf += take;
        p += take;
        n -= take;
        if (s->nbuf < 32)
            return;
        xxs_block(s, s->buf);
        s->nbuf = 0;
    }
    for (; n >= 32; p += 32, n -= 32)
        xxs_block(s, p);
    memcpy(s->buf, p, n);
    s->nbuf = n;
}

static uint64_t xxs_digest(const xxs *s)
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
    const uint8_t *p = s->buf, *end = s->buf + s->nbuf;
    for (; p + 8 <= end; p += 8)
        h = rotl64(h ^ xround(0, rd64le(p)), 27) * P1 + P4;
    if (p + 4 <= end) {
        h = rotl64(h ^ ((uint64_t)rd32le(p) * P1), 23) * P2 + P3;
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

uint64_t settle_xxh64_pieces(const uint8_t *buf, const size_t *lens, size_t n)
{
    xxs s;
    xxs_init(&s);
    for (size_t i = 0; i < n; i++) {
        xxs_update(&s, buf, lens[i]);
        buf += lens[i];
    }
    return xxs_digest(&s);
}

/* ------------------------------------------------------------ state */

struct ecsettle_tier {
    tier *t;
    pthread_mutex_t mu; /* a leaf: never held across a settle */
    pthread_cond_t cv;
    pthread_t th;
    bool has_th, stop;
    eng **engs;
    size_t neng, cap;
    eng *cur; /* the engine the thread works on (ecsettle_eng_stop waits for it to change) */
};

struct ecsettle_eng {
    ecpolicy_state pol;
    atomic_bool stopping; /* ecsettle_eng_stop ran: a settle of this engine unwinds */
};

ecpolicy_state *ecpolicy_of(eng *db)
{
    return db->ecstate && db->ecstate->settle ? &db->ecstate->settle->pol : NULL;
}

static bool eng_stopping(eng *db)
{
    return atomic_load(&db->stop) || (db->ecstate && db->ecstate->settle && atomic_load(&db->ecstate->settle->stopping));
}

/* ------------------------------------------------------------ the one pass */

typedef struct gran {
    uint64_t off;
    uint32_t len, crc, idx;
    bool decode; /* spans a class switch of its series: judged by its rows */
} gran;

typedef struct sfill {
    eng *db;
    part *p;
    part *shadow;   /* ix: the index parsed from the early copy */
    uint8_t *early; /* [0, hl) the header, then [toff, map_len) the index, tombstones and footer */
    size_t hl;
    uint64_t toff, tlen;
    gran *g;
    uint32_t ng, gi;
    uint8_t *asmb; /* the granule being assembled, with BLK_SLACK zero bytes after the longest */
    codec_ctx *cc;
    int64_t *ts;
    int pc;
    xxs h;
    uint64_t hash;
    bool aborted; /* fill returned non-zero: the put committed nothing */
} sfill;

static int fail_part(sfill *f, int rc, const char *fmt, ...) __attribute__((format(printf, 3, 4)));
static int fail_part(sfill *f, int rc, const char *fmt, ...)
{
    char why[256];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(why, sizeof why, fmt, ap);
    va_end(ap);
    fprintf(stderr, "tsdb: %s: settle of part %s refused: %s\n", f->db->dir, f->p->name, why);
    if (rc == TSDB_ERR_CORRUPT)
        part_mark_payload(f->db, f->p, "settle");
    return rc;
}

/* the class check of check_granule (src/classes.c) on the decoded timestamps of granule x */
static int judge_rows(sfill *f, const gran *x)
{
    granule g;
    gx_load(f->shadow, x->idx, GW_LOC | GW_TIME, &g);
    uint32_t s = gx_series(f->shadow, x->idx);
    int rc = granule_decode(f->cc, f->p, &g, f->asmb, 0, f->ts, NULL, f->p->grows, false);
    if (rc)
        return rc == TSDB_ERR_NOMEM ? rc : fail_part(f, TSDB_ERR_CORRUPT, "a granule of series %u does not decode", s);
    int64_t flo, fhi = INT64_MIN;
    for (uint32_t r = 0; r < g.count; r++) {
        int64_t t = f->ts[r];
        if (t < g.ts_min || t > g.ts_max || (r && t <= f->ts[r - 1]))
            return fail_part(f, TSDB_ERR_CORRUPT, "series %u: a row outside its granule's bounds or out of order",
                             s);
        if (t < fhi || fhi == INT64_MAX)
            continue;
        if (cls_at(f->db, s, t, &flo, &fhi) != f->pc)
            return fail_part(f, TSDB_ERR_CORRUPT,
                             "series %u: a row at %" PRId64 " in a partition of another class than CLASSES gives it",
                             s, t);
    }
    return 0;
}

/* checks the stripe bytes b = object [off, off + len) */
static int check_stripe(sfill *f, const uint8_t *b, uint64_t off, size_t len)
{
    uint64_t end = off + len;
    if (off < f->hl && memcmp(b, f->early + off, MIN(end, f->hl) - off) != 0)
        return fail_part(f, TSDB_ERR_CORRUPT, "its header changed since it was checked");
    if (end > f->toff) {
        uint64_t a = MAX(off, f->toff);
        if (memcmp(b + (a - off), f->early + f->hl + (a - f->toff), end - a) != 0)
            return fail_part(f, TSDB_ERR_CORRUPT, "its index, tombstones or footer changed since they were checked");
    }
    for (; f->gi < f->ng && f->g[f->gi].off < end; f->gi++) {
        gran *x = &f->g[f->gi];
        uint64_t ge = x->off + x->len, a = MAX(off, x->off), z = MIN(end, ge);
        memcpy(f->asmb + (a - x->off), b + (a - off), z - a);
        if (ge > end)
            break; /* the rest comes with the next stripe */
        if ((uint32_t)xxh64(f->asmb, x->len, 0) != x->crc)
            return fail_part(f, TSDB_ERR_CORRUPT, "granule %u at %" PRIu64 " does not match its checksum", x->idx,
                             x->off);
        int rc;
        if (x->decode && (rc = judge_rows(f, x)) != 0)
            return rc;
    }
    return 0;
}

static int settle_fill(void *ctx, uint64_t s, uint64_t off, uint8_t *buf, size_t len)
{
    sfill *f = ctx;
    if (eng_stopping(f->db)) {
        f->aborted = true;
        return TSDB_ERR_STOPPED;
    }
    void (*hook)(eng *, const part *, uint64_t) = atomic_load(&settle_fill_hook);
    int rc = 0;
    if (settle_mutant == SETTLE_MUT_TWOPASS) { /* the bug: the check and the encode read the map apart */
        rc = check_stripe(f, f->p->map + off, off, len);
        if (hook)
            hook(f->db, f->p, s);
        memcpy(buf, f->p->map + off, len);
    } else {
        memcpy(buf, f->p->map + off, len);
        if (hook)
            hook(f->db, f->p, s);
        if (settle_mutant != SETTLE_MUT_NOCHECK)
            rc = check_stripe(f, buf, off, len);
    }
    if (rc == 0 && off + len == f->p->map_len && f->gi < f->ng && settle_mutant != SETTLE_MUT_NOCHECK)
        rc = fail_part(f, TSDB_ERR_CORRUPT, "granule %u lies past the object", f->g[f->gi].idx);
    if (rc) {
        f->aborted = true;
        return rc;
    }
    xxs_update(&f->h, buf, len);
    if (off + len == f->p->map_len)
        f->hash = xxs_digest(&f->h);
    return 0;
}

static int gran_cmp(const void *a, const void *b)
{
    const gran *x = a, *y = b;
    return x->off < y->off ? -1 : x->off > y->off;
}

static void sfill_free(sfill *f)
{
    free(f->early);
    free(f->g);
    free(f->asmb);
    free(f->ts);
    free(f->shadow);
    if (f->cc)
        codec_free(f->cc);
}

/* Before the encode: the early copy of the header and of [index_off, end), its hash, the index parsed from it, the
 * granules in object order (checked to lie apart, inside the data), and the class check from the bounds. */
static int sfill_prepare(sfill *f)
{
    eng *db = f->db;
    part *p = f->p;
    f->hl = p->hdr_len;
    f->toff = p->index_off;
    f->tlen = p->map_len - p->index_off;
    if (p->map_len < f->hl + sizeof(part_footer) || f->toff < f->hl || f->tlen < sizeof(part_footer) + p->index_len)
        return fail_part(f, TSDB_ERR_CORRUPT, "its layout does not hold together");
    if (!(f->early = malloc(f->hl + f->tlen)) || !(f->shadow = calloc(1, sizeof *f->shadow)) ||
        !(f->cc = codec_new()) || !(f->ts = malloc((size_t)MAX(p->grows, 1) * sizeof *f->ts)))
        return TSDB_ERR_NOMEM;
    memcpy(f->early, p->map, f->hl);
    memcpy(f->early + f->hl, p->map + f->toff, f->tlen);
    const uint8_t *idx = f->early + f->hl, *foot = f->early + f->hl + f->tlen - sizeof(part_footer);
    size_t il = (size_t)p->index_len, tl = (size_t)(f->tlen - sizeof(part_footer) - il);
    part_footer ft;
    memcpy(&ft, foot, sizeof ft);
    uint64_t x = xxh64(f->early, f->hl, 0);
    x = xxh64(idx, il, x);
    if (tl)
        x = xxh64(idx + il, tl, x);
    x = xxh64(foot, offsetof(part_footer, index_hash), x);
    if (settle_mutant != SETTLE_MUT_NOCHECK && (x != ft.index_hash || x != p->index_hash))
        return fail_part(f, TSDB_ERR_CORRUPT, "its header, index or footer no longer match the checksum its open "
                                              "checked");
    if (gix_parse(idx, il, p->ng, &f->shadow->ix) != 0)
        return fail_part(f, TSDB_ERR_CORRUPT, "its index does not parse");
    f->shadow->ng = p->ng;
    f->ng = p->ng;
    if (!(f->g = calloc(f->ng, sizeof *f->g)))
        return TSDB_ERR_NOMEM;
    f->pc = partition_class(db, p->partition);
    int64_t lo, hi;
    partition_range(db, p->partition, &lo, &hi);
    uint32_t maxlen = 0;
    const gix *ix = &f->shadow->ix;
    for (uint32_t r = 0; r < ix->nruns; r++) {
        uint32_t s = ix->rser[r], i = gix_rstart(ix, r), j = r + 1 < ix->nruns ? gix_rstart(ix, r + 1) : f->ng;
        for (uint32_t k = i; k < j; k++) {
            granule g;
            gx_load(f->shadow, k, GW_LOC | GW_TIME, &g);
            gran *e = &f->g[k];
            *e = (gran){.off = g.off, .len = g.ts_len + g.val_len, .crc = g.crc, .idx = k};
            maxlen = MAX(maxlen, e->len);
            if (g.ts_min > g.ts_max || (k > i && g.ts_min <= gx_ts_max(f->shadow, k - 1)))
                return fail_part(f, TSDB_ERR_CORRUPT, "series %u: granules out of time order", s);
            if (g.ts_min < lo || (hi != INT64_MAX && g.ts_max >= hi))
                return fail_part(f, TSDB_ERR_CORRUPT, "series %u: rows outside the partition", s);
            if (settle_mutant == SETTLE_MUT_NOCHECK)
                continue;
            int64_t flo, fhi;
            int c = cls_at(db, s, g.ts_min, &flo, &fhi);
            if (c == f->pc && (g.ts_max < fhi || fhi == INT64_MAX))
                continue;
            if (c != f->pc || cls_at(db, s, g.ts_max, &flo, &fhi) != f->pc)
                return fail_part(f, TSDB_ERR_CORRUPT,
                                 "series %u: rows %" PRId64 " .. %" PRId64 " in a partition of another class than "
                                 "CLASSES gives them", s, g.ts_min, g.ts_max);
            e->decode = true;
        }
    }
    qsort(f->g, f->ng, sizeof *f->g, gran_cmp);
    for (uint32_t k = 0; k < f->ng; k++) {
        const gran *e = &f->g[k];
        if (e->len == 0 || e->off < f->hl || e->off + e->len > f->toff ||
            (k && e->off < f->g[k - 1].off + f->g[k - 1].len))
            return fail_part(f, TSDB_ERR_CORRUPT, "its index places granule %u where it cannot be", e->idx);
    }
    if (!(f->asmb = calloc(1, (size_t)maxlen + BLK_SLACK)))
        return TSDB_ERR_NOMEM;
    xxs_init(&f->h);
    return 0;
}

/* hands the chunks of an object that no .ec part names to r7-gc (ecgc_unlisted_object reads only the record) */
static void gc_object(eng *db, const ec_record *r)
{
    part *fake = calloc(1, sizeof *fake);
    struct ecpart *e = calloc(1, sizeof *e);
    if (fake && e) {
        e->rec = *r;
        e->tier = db->tier;
        e->volume = db->tier_vol;
        atomic_store(&fake->ec, e);
        snprintf(fake->name, sizeof fake->name, "%.*s.ec", (int)strlen(r->source) - 5, r->source);
        ecgc_unlisted_object(db, fake);
    }
    free(e);
    free(fake);
}

/* p: pinned. *out: the .ec part opened from its tmp_*.ec (pending names it). */
static int settle_one(eng *db, part *p, part **out)
{
    *out = NULL;
    const tier_cfg *cfg = tier_config(db->tier);
    chunk_geom g;
    int rc = chunk_geom_derive(&g, (unsigned)cfg->k, (unsigned)cfg->m, p->map_len);
    if (rc)
        return rc;
    ec_record r;
    memset(&r, 0, sizeof r);
    r.version = EC_RECORD_VERSION;
    r.class_check = EC_CLS_PASSED;
    chunk_random(r.oid, sizeof r.oid);
    r.key = p->partition;
    r.min_block = p->min_block;
    r.max_block = p->max_block;
    r.level = p->level;
    r.k = g.k;
    r.m = g.m;
    r.cell_bytes = g.cell;
    r.object_len = p->map_len;
    r.writer_epoch = 1; /* D4 */
    snprintf(r.source, sizeof r.source, "%s", p->name);
    r.nsums = chunk_sums_total(&g);
    if (!(r.sums = malloc(r.nsums * sizeof *r.sums)))
        return TSDB_ERR_NOMEM;
    sfill f = {.db = db, .p = p};
    if ((rc = sfill_prepare(&f)) != 0)
        goto out;
    chunk_put_src src = {.ctx = &f, .fill = settle_fill};
    rc = chunk_be_put_stream(tier_backend(db->tier), db->tier_vol, tier_vuuid(db), r.oid, &g, r.writer_epoch, &src,
                             ~0u, r.sums);
    if (rc) {
        if (!f.aborted) {
            fprintf(stderr, "tsdb: %s: settle of part %s: the put failed (%d)\n", db->dir, p->name, rc);
            gc_object(db, &r); /* a failure while committing leaves the positions committed before it */
        }
        goto out;
    }
    ecswap_crash_point(ECSWAP_CRASH_AFTER_PUT);
    r.object_xxh64 = f.hash;
    r.settle_ms = eng_now_ms(db);
    char tmp[96];
    if ((rc = ec_index_write(db, p, &r, tmp)) != 0) {
        gc_object(db, &r);
        goto out;
    }
    ecswap_crash_point(ECSWAP_CRASH_AFTER_EC_TMP);
    char name[96];
    snprintf(name, sizeof name, "%.*s.ec", (int)strlen(p->name) - 5, p->name);
    part *e = NULL;
    if ((rc = part_open_ec(db, tmp, name, &e)) != 0) {
        unlinkat(db->dirfd, tmp, 0);
        gc_object(db, &r);
        goto out;
    }
    if (!e->pending[0])
        snprintf(e->pending, sizeof e->pending, "%.63s", tmp); /* tmp_<pid>_<n>.ec: short */
    *out = e;
out:
    sfill_free(&f);
    ec_record_free(&r);
    return rc;
}

static bool listed_locked(eng *db, const part *p)
{
    for (size_t i = 0; i < db->nparts; i++)
        if (db->parts[i] == p)
            return true;
    return false;
}

int eng_settle(eng *db, int64_t partition, part **sel, size_t n, size_t *settled)
{
    pthread_once(&mut_once, mut_env);
    if (settled)
        *settled = 0;
    if (!db->tier || !db->ecstate || !db->ecstate->settle || db->replica_req || db->cluster_req || eng_jr(db))
        return TSDB_ERR_INVALID; /* D1: R7 settles on one server only */
    if (db->opt.read_only || db->opt.follow)
        return TSDB_ERR_READONLY;
    if (eng_check(db))
        return TSDB_ERR_IO;
    if (!tier_writable(db->tier))
        return TSDB_ERR_UNAVAILABLE;
    if (eng_stopping(db))
        return TSDB_ERR_STOPPED;
    part **pin = calloc(n ? n : 1, sizeof *pin), **ec = calloc(n ? n : 1, sizeof *ec);
    if (!pin || !ec) {
        free(pin);
        free(ec);
        return TSDB_ERR_NOMEM;
    }
    size_t np = 0;
    pthread_mutex_lock(&db->lock);
    if (!partition_dropping(db, partition))
        for (size_t i = 0; i < n; i++) {
            part *p = sel[i];
            bool dup = false;
            for (size_t j = 0; j < np; j++)
                dup |= pin[j] == p;
            /* a v8 part (hdr_len) is rewritten alone first (src/merge.c rewrite_alone); ec_index_write refuses it */
            if (dup || p->partition != partition || p->merging || p->settling || p->dropped || part_is_ec(p) ||
                atomic_load(&p->damaged) || p->ng == 0 || p->hdr_len != sizeof(part_header) || p->pending[0] ||
                !listed_locked(db, p))
                continue;
            p->settling = true;
            part_ref(p);
            pin[np++] = p;
        }
    pthread_mutex_unlock(&db->lock);
    int rc = np ? 0 : TSDB_ERR_NOTFOUND;
    size_t nec = 0;
    for (; rc == 0 && nec < np; nec++)
        if ((rc = settle_one(db, pin[nec], &ec[nec])) != 0)
            break;
    if (rc == 0) {
        rc = ec_swap_commit(db, pin, ec, np);
        if (rc == 0 && settle_mutant == SETTLE_MUT_RANKDOWN) { /* the bug: in memory, ranked below the others */
            pthread_mutex_lock(&db->lock);
            for (size_t i = 0; i < np; i++)
                ec[i]->min_block = ec[i]->max_block = 0;
            pthread_mutex_unlock(&db->lock);
        }
        if (rc == TSDB_ERR_INVALID || rc == TSDB_ERR_NOMEM || rc == TSDB_ERR_READONLY)
            for (size_t i = 0; i < np; i++) { /* nothing written: the temporaries and objects are ours to remove */
                unlinkat(db->dirfd, ec[i]->pending, 0);
                ecgc_unlisted_object(db, ec[i]);
            }
        /* STOPPED: the swap unlinked the temporaries and handed their objects to r7-gc; IO: the next open decides */
    } else {
        for (size_t i = 0; i < nec; i++) { /* the ones written before the failure */
            unlinkat(db->dirfd, ec[i]->pending, 0);
            ecgc_unlisted_object(db, ec[i]);
        }
    }
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < np; i++)
        pin[i]->settling = false;
    pthread_mutex_unlock(&db->lock);
    for (size_t i = 0; i < np; i++) {
        part_unref(pin[i]);
        if (ec[i])
            part_unref(ec[i]);
    }
    free(pin);
    free(ec);
    if (rc == 0) {
        atomic_fetch_add(&settle_done, 1);
        if (settled)
            *settled = np;
    } else if (rc == TSDB_ERR_STOPPED) {
        atomic_fetch_add(&settle_abandoned, 1);
    } else if (rc != TSDB_ERR_NOTFOUND) {
        atomic_fetch_add(&settle_failed, 1);
    }
    return rc;
}

/* ------------------------------------------------------------ the thread */

/* one settle of db, if the policy has one due: whether it settled anything */
static bool settle_engine(eng *db)
{
    if (eng_check(db) || eng_stopping(db) || !tier_writable(db->tier))
        return false;
    int64_t now = eng_now_ms(db), key = 0;
    part *sel[64];
    pthread_mutex_lock(&db->lock);
    size_t n = ecpolicy_pick_locked(db, now, &key, sel, sizeof sel / sizeof *sel);
    pthread_mutex_unlock(&db->lock);
    if (!n)
        return false;
    int rc = eng_settle(db, key, sel, n, NULL);
    for (size_t i = 0; i < n; i++)
        part_unref(sel[i]);
    if (rc == 0)
        ecpolicy_settled(db, now);
    else if (rc != TSDB_ERR_STOPPED && rc != TSDB_ERR_NOTFOUND)
        ecpolicy_failed(db, key, now);
    return rc == 0;
}

static void *settle_main(void *arg)
{
    struct ecsettle_tier *st = arg;
    pthread_once(&mut_once, mut_env);
#ifdef __linux__
    setpriority(PRIO_PROCESS, (id_t)syscall(SYS_gettid), 19); /* this thread only (D11) */
#endif
    pthread_mutex_lock(&st->mu);
    while (!st->stop) {
        bool any = false;
        for (size_t i = 0; !st->stop && i < st->neng; i++) {
            eng *db = st->engs[i];
            st->cur = db;
            pthread_mutex_unlock(&st->mu);
            any |= settle_engine(db);
            pthread_mutex_lock(&st->mu);
            st->cur = NULL;
            pthread_cond_broadcast(&st->cv);
        }
        atomic_fetch_add(&settle_rounds, 1);
        if (any || st->stop)
            continue;
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        int ms = atomic_load(&settle_poll_ms);
        ts.tv_sec += ms / 1000;
        ts.tv_nsec += (long)(ms % 1000) * 1000000L;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000L;
        }
        pthread_cond_timedwait(&st->cv, &st->mu, &ts);
    }
    pthread_mutex_unlock(&st->mu);
    return NULL;
}

static void tier_stop(struct ecsettle_tier *st)
{
    if (!st->has_th)
        return;
    pthread_mutex_lock(&st->mu);
    st->stop = true;
    pthread_cond_broadcast(&st->cv);
    pthread_mutex_unlock(&st->mu);
    pthread_join(st->th, NULL);
    st->has_th = false;
}

int ecsettle_tier_open(struct tier *t)
{
    EC_CALLED(EC_CALL_TIER_OPEN);
    struct ecsettle_tier *st = calloc(1, sizeof *st);
    if (!st)
        return TSDB_ERR_NOMEM;
    st->t = t;
    pthread_mutex_init(&st->mu, NULL);
    pthread_cond_init(&st->cv, NULL);
    t->settle = st;
    if (tsdb_thread_create(&st->th, settle_main, st) != 0)
        return TSDB_ERR_NOMEM;
    st->has_th = true;
    return 0;
}

void ecsettle_tier_quiesce(struct tier *t)
{
    EC_CALLED(EC_CALL_TIER_QUIESCE);
    if (t && t->settle)
        tier_stop(t->settle);
}

void ecsettle_tier_close(struct tier *t)
{
    EC_CALLED(EC_CALL_TIER_CLOSE);
    struct ecsettle_tier *st = t ? t->settle : NULL;
    if (!st)
        return;
    tier_stop(st);
    pthread_mutex_destroy(&st->mu);
    pthread_cond_destroy(&st->cv);
    free(st->engs);
    free(st);
    t->settle = NULL;
}

int ecsettle_eng_open(eng *db)
{
    EC_CALLED(EC_CALL_ENG_OPEN);
    struct ecsettle_eng *e = calloc(1, sizeof *e);
    if (!e)
        return TSDB_ERR_NOMEM;
    pthread_mutex_init(&e->pol.pmu, NULL);
    db->ecstate->settle = e;
    return 0;
}

void ecsettle_eng_ready(eng *db)
{
    struct ecsettle_tier *st = db->tier ? db->tier->settle : NULL;
    if (!st || !db->ecstate || !db->ecstate->settle || db->opt.read_only || db->opt.follow || db->replica_req ||
        db->cluster_req)
        return;
    pthread_mutex_lock(&st->mu);
    if (st->neng == st->cap) {
        size_t cap = st->cap ? 2 * st->cap : 8;
        eng **a = realloc(st->engs, cap * sizeof *a);
        if (!a) { /* the engine is not settled: nothing else depends on it */
            pthread_mutex_unlock(&st->mu);
            fprintf(stderr, "tsdb: %s: out of memory: no settles for this volume until it is opened again\n", db->dir);
            return;
        }
        st->engs = a;
        st->cap = cap;
    }
    st->engs[st->neng++] = db;
    pthread_cond_broadcast(&st->cv);
    pthread_mutex_unlock(&st->mu);
}

void ecsettle_eng_stop(eng *db)
{
    if (!db->ecstate || !db->ecstate->settle)
        return;
    atomic_store(&db->ecstate->settle->stopping, true);
    struct ecsettle_tier *st = db->tier ? db->tier->settle : NULL;
    if (!st)
        return;
    pthread_mutex_lock(&st->mu);
    for (size_t i = 0; i < st->neng; i++)
        if (st->engs[i] == db) {
            memmove(st->engs + i, st->engs + i + 1, (st->neng - i - 1) * sizeof *st->engs);
            st->neng--;
            break;
        }
    while (st->cur == db)
        pthread_cond_wait(&st->cv, &st->mu);
    pthread_mutex_unlock(&st->mu);
}

void ecsettle_eng_close(eng *db)
{
    EC_CALLED(EC_CALL_ENG_CLOSE);
    if (!db->ecstate || !db->ecstate->settle)
        return;
    ecsettle_eng_stop(db);
    pthread_mutex_destroy(&db->ecstate->settle->pol.pmu);
    free(db->ecstate->settle);
    db->ecstate->settle = NULL;
}
