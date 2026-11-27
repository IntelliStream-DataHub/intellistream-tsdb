/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Reverse intent and eng_unsettle (src/ecunsettle.h; R15 r15-reverse, D14; r8-from-r7 item 7).
 *
 * Restores local .part files for every listed .ec part P of one partition key on a single-server volume,
 * preserving P's exact name, block range and level (rank unchanged), and retiring P.ec to retired_P.ec via
 * intent v5 (`L` and `U` lines, ec_swap_unsettle in src/ecswap.c):
 *   1. Pin all listed .ec parts of the key under db->lock (`p->settling = true`); cluster, replicated,
 *      read-only and follower engines are refused before touching disk.
 *   2. For each pinned .ec part P (holding no engine lock across I/O):
 *      - Source 1 (fast path): open `settled_P.part` if present and verify header/index/tombstones/footer hash
 *        (part_open_as), index_hash match against P.ec, granule layout, every granule's xxh64 checksum, zero
 *        alignment padding, and footer rows/time range.
 *      - Source 2 (decode path): when `settled_P.part` is absent or fails verification, decode P's object
 *        through the tier seam (`cold_ops->obj_read`, checking encode-time cell sums), verify the streaming
 *        xxh64 byte digest against `rec.object_xxh64`, sync `tmp_*.part` (`tsdb_fsync`), open and run the full
 *        part integrity check before swapping.
 *   3. Commit atomically via `ec_swap_unsettle` (lock order: publish_lock > db_pub_begin > latest_rw > db->lock;
 *      only the 2n renameat calls run inside latest_rw > db->lock). If a `settled_` copy disappeared concurrently
 *      before db->lock (TSDB_ERR_NOTFOUND), retry once via the decode path.
 *   4. Single-server CLI `tsdb DIR volume diverged --clear` (`ec_unsettle_diverged_cli`): checks META/LOCK/DIVERGED,
 *      opens the tier and quiesces background settle/GC threads BEFORE opening the volume engine, unsettles every
 *      diverged key that has .ec parts, closes cleanly, and removes durable DIVERGED only after every swap is
 *      durable on disk.
 */
#include "internal.h"
#include "ecunsettle.h"
#include "ecapply.h"
#include "ecdiverge.h"
#include "eckeep.h"
#include "ecpart.h"
#include "ecswap.h"
#include "coldops.h"
#include "marker.h"
#include "tier.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

int ecunsettle_mutant;
atomic_uint_fast64_t ecunsettle_from_settled;
atomic_uint_fast64_t ecunsettle_from_decode;
static atomic_uint_fast64_t ecunsettle_keys_done;
static atomic_uint_fast64_t ecunsettle_objs_done;
static atomic_uint_fast64_t ecunsettle_bytes_done;
static atomic_uint_fast64_t ecunsettle_keys_failed;
void (*_Atomic ecunsettle_crash_hook)(struct eng *db);
void (*_Atomic ecunsettle_leader_crash_hook)(struct eng *db);
_Atomic int64_t ecunsettle_test_retry_ms = -1;

int ecunsettle_mutant_parse(const char *name)
{
    static const char *const names[] = {"none",
                                        "unsettlenodecodecheck",
                                        "unsettlerank",
                                        "unsettleleaderonly",
                                        "unsettlenogcgate",
                                        "unsettlenofloorclamp",
                                        "unsettlesnapnohold"};
    for (int i = 0; i < (int)(sizeof names / sizeof *names); i++)
        if (strcmp(name, names[i]) == 0)
            return i;
    return -1;
}

static pthread_once_t unsettle_env_once = PTHREAD_ONCE_INIT;

static void unsettle_env_init_fn(void)
{
#ifdef TSDB_TEST_HOOKS
    const char *m = getenv("TSDB_ECUNSETTLE_MUTANT");
    if (m && *m) {
        int v = ecunsettle_mutant_parse(m);
        if (v < 0) {
            fprintf(stderr, "tsdb: TSDB_ECUNSETTLE_MUTANT=%s names no mutant: exiting with status 2\n", m);
            _exit(2);
        }
        if (ecunsettle_mutant == ECUNSETTLE_MUT_NONE)
            ecunsettle_mutant = v;
        fprintf(stderr, "tsdb: ecunsettle MUTANT %s (tests only)\n", m);
    }
#endif
}

void ecunsettle_env_init(void) { pthread_once(&unsettle_env_once, unsettle_env_init_fn); }

void ecunsettle_get_progress(ecunsettle_progress *out)
{
    if (!out)
        return;
    out->keys_done = atomic_load_explicit(&ecunsettle_keys_done, memory_order_relaxed);
    out->objs_done = atomic_load_explicit(&ecunsettle_objs_done, memory_order_relaxed);
    out->bytes_done = atomic_load_explicit(&ecunsettle_bytes_done, memory_order_relaxed);
    out->keys_failed = atomic_load_explicit(&ecunsettle_keys_failed, memory_order_relaxed);
    out->from_settled = atomic_load_explicit(&ecunsettle_from_settled, memory_order_relaxed);
    out->from_decode = atomic_load_explicit(&ecunsettle_from_decode, memory_order_relaxed);
}

#ifdef TSDB_EC

/* ------------------------------------------------------------ streaming xxh64 (matches xxh64(data, len, 0)) */

#define U_P1 11400714785074694791ULL
#define U_P2 14029467366897019727ULL
#define U_P3 1609587929392839161ULL
#define U_P4 9650029242287828579ULL
#define U_P5 2870177450012600261ULL

typedef struct {
    uint64_t v[4], total;
    uint8_t buf[32];
    size_t nbuf;
} u_xs;

static inline uint64_t u_rotl64(uint64_t x, int r) { return (x << r) | (x >> (64 - r)); }
static inline uint64_t u_xround(uint64_t acc, uint64_t in) { return u_rotl64(acc + in * U_P2, 31) * U_P1; }
static inline uint64_t u_xmerge(uint64_t acc, uint64_t v) { return (acc ^ u_xround(0, v)) * U_P1 + U_P4; }
static inline uint64_t u_r64(const uint8_t *p)
{
    uint64_t x;
    memcpy(&x, p, 8);
    return x;
}
static inline uint32_t u_r32(const uint8_t *p)
{
    uint32_t x;
    memcpy(&x, p, 4);
    return x;
}

static void u_xs_init(u_xs *s)
{
    memset(s, 0, sizeof *s);
    s->v[0] = U_P1 + U_P2;
    s->v[1] = U_P2;
    s->v[2] = 0;
    s->v[3] = 0 - U_P1;
}

static void u_xs_stripe(u_xs *s, const uint8_t *p)
{
    for (int i = 0; i < 4; i++)
        s->v[i] = u_xround(s->v[i], u_r64(p + 8 * i));
}

static void u_xs_update(u_xs *s, const void *data, size_t len)
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
        u_xs_stripe(s, s->buf);
        s->nbuf = 0;
    }
    for (; end - p >= 32; p += 32)
        u_xs_stripe(s, p);
    memcpy(s->buf, p, (size_t)(end - p));
    s->nbuf = (size_t)(end - p);
}

static uint64_t u_xs_digest(const u_xs *s)
{
    uint64_t h;
    if (s->total >= 32) {
        h = u_rotl64(s->v[0], 1) + u_rotl64(s->v[1], 7) + u_rotl64(s->v[2], 12) + u_rotl64(s->v[3], 18);
        for (int i = 0; i < 4; i++)
            h = u_xmerge(h, s->v[i]);
    } else {
        h = U_P5;
    }
    h += s->total;
    const uint8_t *p = s->buf, *end = p + s->nbuf;
    for (; p + 8 <= end; p += 8)
        h = u_rotl64(h ^ u_xround(0, u_r64(p)), 27) * U_P1 + U_P4;
    if (p + 4 <= end) {
        h = u_rotl64(h ^ ((uint64_t)u_r32(p) * U_P1), 23) * U_P2 + U_P3;
        p += 4;
    }
    for (; p < end; p++)
        h = u_rotl64(h ^ (*p * U_P5), 11) * U_P1;
    h ^= h >> 33;
    h *= U_P2;
    h ^= h >> 29;
    h *= U_P3;
    h ^= h >> 32;
    return h;
}

/* ------------------------------------------------------------ part verification */

static int map_cold_rc(int rc)
{
    if (rc == TSDB_ERR_NOTFOUND)
        return TSDB_ERR_CORRUPT;
    return rc <= -100 ? TSDB_ERR_IO : rc;
}

/* Verifies an already-opened .part `p` (whose header/index/tombstones/footer hash was already checked by
 * part_open_as): checks granule_rows, partition/block/level match against `ec`, index_hash match, granule layout
 * and every granule's xxh64 checksum, zero alignment padding before index_off, and footer rows/ts range. */
static bool verify_part_full(const eng *db, const part *p, const part *ec, bool rank_mut)
{
    if (!p || !ec || p->grows != db->granule_rows || p->partition != ec->partition)
        return false;
    if (!rank_mut && (p->min_block != ec->min_block || p->max_block != ec->max_block || p->level != ec->level))
        return false;
    if (p->index_hash != ec->index_hash)
        return false;
    uint64_t rows = 0, end = p->hdr_len;
    int64_t lo = INT64_MAX, hi = INT64_MIN;
    for (uint32_t i = 0; i < p->ng; i++) {
        granule g;
        gx_load(p, i, GW_LOC, &g);
        uint64_t len = (uint64_t)g.ts_len + g.val_len;
        if (g.off < end || g.off > p->index_off || len == 0 || len > p->index_off - g.off)
            return false;
        if ((uint32_t)xxh64(p->map + g.off, len, 0) != g.crc)
            return false;
        end = g.off + len;
        rows += gx_count(p, i);
        lo = MIN(lo, gx_ts_min(p, i));
        hi = MAX(hi, gx_ts_max(p, i));
    }
    /* Alignment padding between last granule and index_off (at most 7 bytes) must be zero. */
    if (end > p->index_off || p->index_off - end >= 8)
        return false;
    for (uint64_t pos = end; pos < p->index_off; pos++)
        if (p->map[pos] != 0)
            return false;
    for (uint32_t i = 0; i < p->nt; i++) {
        lo = MIN(lo, p->t[i].start);
        hi = MAX(hi, p->t[i].end - 1);
    }
    if ((p->ng || p->nt) && (rows != p->rows || lo != p->min_ts || hi != p->max_ts))
        return false;
    return true;
}

/* Source 1 (fast path): open and verify settled_<P.part>.
 * Returns 0 and sets *out on success; TSDB_ERR_NOTFOUND or TSDB_ERR_CORRUPT means fall back to decode;
 * other errors (e.g. TSDB_ERR_NOMEM) propagate. */
static int try_settled_source(eng *db, const part *ec, const char *settled_name, const char *fin_name, bool rank_mut,
                              part **out)
{
    *out = NULL;
    part *p = NULL;
    int rc = part_open_as(db->dirfd, settled_name, fin_name, db->opt.pin_index, db->parts_v8, &p);
    if (rc == TSDB_ERR_NOTFOUND)
        return TSDB_ERR_NOTFOUND;
    if (rc == TSDB_ERR_CORRUPT || rc == TSDB_ERR_INVALID) {
        fprintf(stderr, "tsdb: %s: unsettle: %s failed header/footer verification (%s); falling back to decode\n",
                db->dir, settled_name, tsdb_strerror(rc));
        return TSDB_ERR_CORRUPT;
    }
    if (rc != 0)
        return rc;
    if (!verify_part_full(db, p, ec, rank_mut)) {
        fprintf(stderr, "tsdb: %s: unsettle: %s failed granule/integrity verification; falling back to decode\n",
                db->dir, settled_name);
        part_unref(p);
        return TSDB_ERR_CORRUPT;
    }
    p->verify = db->opt.verify_checksums;
    rc = part_set_pending(p, settled_name);
    if (rc != 0) {
        part_unref(p);
        return rc;
    }
    *out = p;
    return 0;
}

/* Source 2 (decode path): decode ec's object through the tier seam into tmp_<pid>_<cnt>.part, verify sums,
 * byte digest (object_xxh64), fsync, open, and verify the decoded .part footer and granules. */
static int decode_ec_source(eng *db, const part *ec, const char *fin_name, bool rank_mut, part **out)
{
    *out = NULL;
    struct ecpart *ep = atomic_load(&((part *)ec)->ec);
    if (!ep)
        return TSDB_ERR_INVALID;
    bool skip_check = ecunsettle_mut() == ECUNSETTLE_MUT_NODECODECHECK;

    char tmp[64];
    snprintf(tmp, sizeof tmp, "tmp_%d_%u.part", (int)getpid(), atomic_fetch_add(&db->tmp_counter, 1));
    int fd = openat(db->dirfd, tmp, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    if (fd < 0)
        return TSDB_ERR_IO;

    int rc = 0;
    if (ep->test_mark) {
        rc = write_full(fd, ec->map, ec->map_len);
    } else {
        coldvol *cv = tier_cold(db);
        const ec_record *r = &ep->rec;
        chunk_geom g;
        cold_obj *o = NULL;
        if (!cv) {
            rc = TSDB_ERR_UNAVAILABLE;
        } else {
            rc = chunk_geom_make(&g, r->k, r->m, r->cell_bytes, r->object_len);
            if (rc == 0)
                rc = cv->ops->obj_open_ver
                         ? cv->ops->obj_open_ver(cv->ctx, r->oid, &g, r->sums, r->positions, r->stripe_version, &o)
                         : cv->ops->obj_open_pos
                               ? cv->ops->obj_open_pos(cv->ctx, r->oid, &g, r->sums, r->positions, &o)
                               : cv->ops->obj_open(cv->ctx, r->oid, &g, r->sums, &o);
            if (rc != 0)
                rc = map_cold_rc(rc);
        }
        if (rc == 0) {
            const size_t target = 1u << 20;
            size_t stripe = (size_t)cv->ops->obj_geom(o)->k * cv->ops->obj_geom(o)->cell;
            size_t step = stripe >= target ? stripe : (target / stripe) * stripe;
            uint8_t *buf = malloc(step);
            if (!buf) {
                rc = TSDB_ERR_NOMEM;
            } else {
                u_xs h;
                u_xs_init(&h);
                unsigned fl = skip_check ? CHUNK_RR_RAW : 0;
                for (uint64_t off = 0; rc == 0 && off < r->object_len;) {
                    size_t piece = r->object_len - off < step ? (size_t)(r->object_len - off) : step;
                    chunk_report rep;
                    memset(&rep, 0, sizeof rep);
                    int rrc = cv->ops->obj_read(o, off, piece, buf, fl, &rep);
                    if (rrc != 0) {
                        rc = map_cold_rc(rrc);
                        break;
                    }
                    u_xs_update(&h, buf, piece);
                    if (write_full(fd, buf, piece) != 0) {
                        rc = TSDB_ERR_IO;
                        break;
                    }
                    off += piece;
                }
                free(buf);
                if (rc == 0 && !skip_check) {
                    uint64_t got = u_xs_digest(&h);
                    if (got != r->object_xxh64) {
                        fprintf(stderr, "tsdb: %s: unsettle decode of %s: byte digest %016" PRIx64
                                        " != recorded %016" PRIx64 "\n",
                                db->dir, ec->name, got, r->object_xxh64);
                        rc = TSDB_ERR_CORRUPT;
                    }
                }
            }
            cv->ops->obj_close(o);
        }
    }

    if (rc == 0 && db->opt.fsync && tsdb_fsync(fd) != 0)
        rc = TSDB_ERR_IO;
    if (close(fd) != 0 && rc == 0)
        rc = TSDB_ERR_IO;
    if (rc != 0) {
        unlinkat(db->dirfd, tmp, 0);
        return rc;
    }

    part *p = NULL;
    rc = part_open_as(db->dirfd, tmp, fin_name, db->opt.pin_index, db->parts_v8, &p);
    if (rc != 0) {
        fprintf(stderr, "tsdb: %s: unsettle decode of %s: decoded part %s failed open (%s)\n", db->dir, ec->name, tmp,
                tsdb_strerror(rc));
        unlinkat(db->dirfd, tmp, 0);
        return rc;
    }
    if (!skip_check && !verify_part_full(db, p, ec, rank_mut)) {
        fprintf(stderr, "tsdb: %s: unsettle decode of %s: decoded part failed granule/integrity verification\n",
                db->dir, ec->name);
        part_unref(p);
        unlinkat(db->dirfd, tmp, 0);
        return TSDB_ERR_CORRUPT;
    }
    p->verify = db->opt.verify_checksums;
    rc = part_set_pending(p, tmp);
    if (rc != 0) {
        part_unref(p);
        unlinkat(db->dirfd, tmp, 0);
        return rc;
    }
    *out = p;
    return 0;
}

static void cleanup_prepared(eng *db, part **out, size_t n, bool unlink_tmps)
{
    for (size_t i = 0; i < n; i++) {
        if (!out[i])
            continue;
        if (unlink_tmps && out[i]->pending && part_tmp_name(out[i]->pending) && eng_check(db) == 0)
            unlinkat(db->dirfd, out[i]->pending, 0);
        part_unref(out[i]);
        out[i] = NULL;
    }
}

static int cmp_i64(const void *a, const void *b)
{
    int64_t x = *(const int64_t *)a, y = *(const int64_t *)b;
    return (x > y) - (x < y);
}

static bool oid_in_list(const uint8_t oid[MK_OID], const uint8_t *oids, uint32_t noids)
{
    for (uint32_t i = 0; i < noids; i++)
        if (memcmp(oid, oids + (size_t)i * MK_OID, MK_OID) == 0)
            return true;
    return false;
}

static int prepare_unsettle_parts(eng *db, part **in, size_t n, bool rank_mut, bool decode_only, part **out,
                                  size_t *n_settled_out, size_t *n_decode_out)
{
    size_t n_settled = 0, n_decode = 0;
    int rc = 0;
    for (size_t i = 0; i < n && rc == 0; i++) {
        size_t elen = strlen(in[i]->name);
        if (elen <= 3 || elen - 3 + 6 >= sizeof in[i]->name) {
            rc = TSDB_ERR_INVALID;
            break;
        }
        char orig_part[128], fin_name[128], settled_name[160];
        snprintf(orig_part, sizeof orig_part, "%.*s.part", (int)(elen - 3), in[i]->name);
        snprintf(settled_name, sizeof settled_name, "settled_%s", orig_part);
        if (rank_mut) {
            uint64_t b = atomic_fetch_add(&db->next_block, 1);
            snprintf(fin_name, sizeof fin_name, "p%" PRId64 "_%" PRIu64 "_%" PRIu64 "_%u.part", in[i]->partition, b, b,
                     in[i]->level);
        } else {
            snprintf(fin_name, sizeof fin_name, "%s", orig_part);
        }

        if (!decode_only) {
            int src_rc = try_settled_source(db, in[i], settled_name, fin_name, rank_mut, &out[i]);
            if (src_rc == 0) {
                n_settled++;
                continue;
            }
            if (src_rc != TSDB_ERR_NOTFOUND && src_rc != TSDB_ERR_CORRUPT) {
                rc = src_rc;
                break;
            }
        }
        rc = decode_ec_source(db, in[i], fin_name, rank_mut, &out[i]);
        if (rc == 0)
            n_decode++;
    }
    if (n_settled_out)
        *n_settled_out = n_settled;
    if (n_decode_out)
        *n_decode_out = n_decode;
    return rc;
}

#endif /* TSDB_EC */

int eng_ec_keys(eng *db, int64_t **keys_out, size_t *nkeys_out)
{
    if (!keys_out || !nkeys_out)
        return TSDB_ERR_INVALID;
    *keys_out = NULL;
    *nkeys_out = 0;
    if (!db)
        return TSDB_ERR_INVALID;
#ifndef TSDB_EC
    return 0;
#else
    int64_t *keys = NULL;
    size_t n = 0, cap = 0;
    int rc = 0;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++) {
        part *p = db->parts[i];
        struct ecpart *ep = atomic_load(&p->ec);
        if (p->dropped || !ep || ep->test_mark)
            continue;
        bool dup = false;
        for (size_t j = 0; j < n && !dup; j++)
            dup = keys[j] == p->partition;
        if (dup)
            continue;
        if (n == cap) {
            size_t nc = cap ? cap * 2 : 16;
            int64_t *x = realloc(keys, nc * sizeof *x);
            if (!x) {
                rc = TSDB_ERR_NOMEM;
                break;
            }
            keys = x;
            cap = nc;
        }
        keys[n++] = p->partition;
    }
    pthread_mutex_unlock(&db->lock);
    if (rc != 0) {
        free(keys);
        return rc;
    }
    if (n > 1)
        qsort(keys, n, sizeof *keys, cmp_i64);
    *keys_out = keys;
    *nkeys_out = n;
    return 0;
#endif
}

int eng_unsettle_key(eng *db, int64_t partition_key, uint64_t *objs_out, uint64_t *bytes_out)
{
    if (objs_out)
        *objs_out = 0;
    if (bytes_out)
        *bytes_out = 0;
#ifndef TSDB_EC
    (void)partition_key;
    fprintf(stderr, "tsdb: %s: eng_unsettle needs the erasure codec (TSDB_EC), which this build lacks\n",
            db ? db->dir : "(null)");
    return TSDB_ERR_INVALID;
#else
    ecunsettle_env_init();
    if (!db)
        return TSDB_ERR_INVALID;
    if (db->opt.read_only || db->opt.follow || db->rp)
        return TSDB_ERR_READONLY;
    bool is_cluster = db->ec_cluster;
    if (is_cluster) {
        if (!eng_jr(db)) {
            fprintf(stderr, "tsdb: %s: eng_unsettle on a cluster ec volume requires an active leader\n", db->dir);
            return TSDB_ERR_INVALID;
        }
        if (atomic_load(&db->lead_deposed))
            return TSDB_ERR_INDETERMINATE;
    } else if (eng_jr(db) || db->replica_req) {
        fprintf(stderr, "tsdb: %s: eng_unsettle is refused on cluster or replicated volumes\n", db->dir);
        return TSDB_ERR_INVALID;
    }
    if (eng_check(db))
        return TSDB_ERR_IO;

    part **in = NULL;
    size_t n = 0, cap = 0;
    int rc = 0;

    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++) {
        part *p = db->parts[i];
        struct ecpart *ep = atomic_load(&p->ec);
        if (p->partition != partition_key || p->dropped || !ep || ep->test_mark)
            continue;
        if (p->settling || p->reencoding || p->merging) {
            rc = TSDB_ERR_LOCKED;
            break;
        }
        char retired[sizeof p->name + 16];
        snprintf(retired, sizeof retired, "retired_%s", p->name);
        for (size_t j = 0; j < db->ngraveyard; j++) {
            if (db->graveyard[j].kind == GY_EC && strcmp(db->graveyard[j].name, retired) == 0) {
                fprintf(stderr, "tsdb: %s: unsettle of %s refused while %s is still awaiting GC\n", db->dir, p->name,
                        retired);
                rc = TSDB_ERR_LOCKED;
                break;
            }
        }
        if (rc != 0)
            break;
        if (is_cluster && n >= MK_FILES_MAX) {
            fprintf(stderr,
                    "tsdb: %s: unsettle of key %" PRId64 " refused: exceeds MK_FILES_MAX (%u) objects in one "
                    "MK_UNSETTLE marker (re-encode/compact the partition first)\n",
                    db->dir, partition_key, MK_FILES_MAX);
            rc = TSDB_ERR_INVALID;
            break;
        }
        if (n == cap) {
            size_t nc = cap ? cap * 2 : 8;
            part **x = realloc(in, nc * sizeof *x);
            if (!x) {
                rc = TSDB_ERR_NOMEM;
                break;
            }
            in = x;
            cap = nc;
        }
        part_ref(p);
        p->settling = true;
        in[n++] = p;
    }
    if (rc != 0) {
        for (size_t i = 0; i < n; i++)
            in[i]->settling = false;
    }
    pthread_mutex_unlock(&db->lock);

    if (rc != 0) {
        for (size_t i = 0; i < n; i++)
            part_unref(in[i]);
        free(in);
        atomic_fetch_add_explicit(&ecunsettle_keys_failed, 1, memory_order_relaxed);
        return rc;
    }
    if (n == 0) {
        free(in);
        return 0;
    }

    /* Also check disk for any retired_<P>.ec (e.g. from an earlier unsettle still awaiting GC) before decoding. */
    for (size_t i = 0; i < n; i++) {
        char retired[sizeof in[i]->name + 16];
        snprintf(retired, sizeof retired, "retired_%s", in[i]->name);
        if (faccessat(db->dirfd, retired, F_OK, 0) == 0) {
            fprintf(stderr, "tsdb: %s: unsettle of %s refused while %s is still awaiting GC\n", db->dir, in[i]->name,
                    retired);
            rc = TSDB_ERR_LOCKED;
            goto unpin;
        }
        if (errno != ENOENT) {
            rc = TSDB_ERR_IO;
            goto unpin;
        }
    }

    uint64_t total_bytes = 0;
    uint8_t *oids = is_cluster ? malloc(n * MK_OID) : NULL;
    if (is_cluster && !oids) {
        rc = TSDB_ERR_NOMEM;
        goto unpin;
    }
    for (size_t i = 0; i < n; i++) {
        struct ecpart *ep = atomic_load(&in[i]->ec);
        if (ep) {
            total_bytes += ep->rec.object_len;
            if (oids)
                memcpy(oids + i * MK_OID, ep->rec.oid, MK_OID);
        }
    }

    part **out = calloc(n, sizeof *out);
    if (!out) {
        free(oids);
        rc = TSDB_ERR_NOMEM;
        goto unpin;
    }

    bool rank_mut = ecunsettle_mut() == ECUNSETTLE_MUT_RANK;
    size_t n_settled = 0, n_decode = 0;

    if (!is_cluster) {
        bool decode_only = false;
        for (int attempt = 0; attempt < 2; attempt++) {
            rc = prepare_unsettle_parts(db, in, n, rank_mut, decode_only, out, &n_settled, &n_decode);
            if (rc != 0) {
                cleanup_prepared(db, out, n, true);
                break;
            }
            rc = ec_swap_unsettle_at(db, in, out, n, 0);
            if (rc == TSDB_ERR_NOTFOUND && !decode_only) {
                /* A settled_ source disappeared concurrently (e.g. eckeep purge) before db->lock: retry via decode. */
                cleanup_prepared(db, out, n, true);
                decode_only = true;
                continue;
            }
            cleanup_prepared(db, out, n, rc != 0);
            break;
        }
    } else {
        /* Cluster EC leader path (R10 §6.4, D14, D27):
         * 1. Decode/prepare local .part files unlocked (outside jr_serial and db->lock).
         * 2. Under jr_serial: verify inputs still listed and not dropping, raise unsettle_hold_pos BEFORE append (so
         *    REPL.floor, keep_floor, RSTATE.kept, applied_floor and snap_make cannot pass MK_UNSETTLE before the
         *    leader's swap is durable), then append & commit MK_UNSETTLE{key, oids[]}.
         * 3. Swap .ec -> .part via ec_swap_unsettle_at(..., pos), which clears unsettle_hold_pos under publish_lock
         *    after the post-rename dir fsync and gates MK_GC until every log replica has applied MK_UNSETTLE. */
        rc = prepare_unsettle_parts(db, in, n, rank_mut, false, out, &n_settled, &n_decode);
        if (rc != 0) {
            cleanup_prepared(db, out, n, true);
        } else {
            uint64_t pos = 0;
            bool committed = false;
            jr_serial_lock(db);
            pthread_mutex_lock(&db->lock);
            bool still_ok = eng_check(db) == 0 && !partition_dropping(db, partition_key);
            for (size_t i = 0; i < n && still_ok; i++) {
                size_t j = 0;
                while (j < db->nparts && db->parts[j] != in[i])
                    j++;
                still_ok = j < db->nparts && !in[i]->dropped;
            }
            uint64_t hold_pos = 0;
            if (still_ok) {
                pthread_mutex_lock(&db->buf_lock);
                hold_pos = db->fno_next ? db->fno_next : 1;
                pthread_mutex_unlock(&db->buf_lock);
                atomic_store_explicit(&db->unsettle_hold_pos, hold_pos, memory_order_release);
            }
            pthread_mutex_unlock(&db->lock);
            if (!still_ok) {
                rc = eng_check(db) ? TSDB_ERR_IO : TSDB_ERR_STOPPED;
            } else {
                mk m;
                memset(&m, 0, sizeof m);
                m.type = MK_UNSETTLE;
                m.span = 1;
                m.u.unsettle = (mk_unsettle){.key = partition_key, .noids = (uint32_t)n, .oids = oids};
                rc = jr_markers_append(db, &m, 1, "an UNSETTLE marker was appended but its commit point did not come",
                                       &pos);
                if (rc == 0 && pos == 0)
                    rc = TSDB_ERR_INVALID;
                if (rc == 0)
                    rc = jr_markers_wait(db, pos, "an UNSETTLE marker was appended but its commit point did not come");
                if (rc == 0) {
                    committed = true;
                    void (*lch)(eng *) = atomic_load(&ecunsettle_leader_crash_hook);
                    if (lch)
                        lch(db);
                    rc = ec_swap_unsettle_at(db, in, out, n, pos);
                } else if (pos == 0) {
                    /* Marker never entered the WAL: safe to release the hold. When pos != 0 (e.g. deposed /
                     * TSDB_ERR_INDETERMINATE between append and commit), the marker is in the WAL and may still
                     * commit under the next leader, so unsettle_hold_pos stays armed until this engine closes/reopens
                     * (clamping REPL.floor and snap_make below pos). */
                    atomic_store_explicit(&db->unsettle_hold_pos, 0, memory_order_release);
                }
            }
            jr_serial_unlock(db);
            if (committed && rc == TSDB_ERR_NOTFOUND) {
                /* A settled_ source vanished: decode unlocked (slow I/O outside jr_serial while unsettle_hold_pos
                 * stays armed), then re-take jr_serial to serialize the swap against concurrent tsdb_drop_before. */
                cleanup_prepared(db, out, n, true);
                rc = prepare_unsettle_parts(db, in, n, rank_mut, true, out, &n_settled, &n_decode);
                if (rc == 0) {
                    jr_serial_lock(db);
                    pthread_mutex_lock(&db->lock);
                    bool dropped_meanwhile = partition_dropping(db, partition_key);
                    if (!dropped_meanwhile) {
                        bool any_live = false;
                        for (size_t i = 0; i < n && !any_live; i++) {
                            if (in[i]->dropped)
                                continue;
                            for (size_t j = 0; j < db->nparts; j++)
                                if (db->parts[j] == in[i]) {
                                    any_live = true;
                                    break;
                                }
                        }
                        if (!any_live)
                            dropped_meanwhile = true;
                    }
                    pthread_mutex_unlock(&db->lock);
                    if (dropped_meanwhile) {
                        /* A legitimate drop retired the key while we decoded unlocked: nothing left to swap. */
                        pthread_mutex_lock(&db->publish_lock);
                        atomic_store_explicit(&db->unsettle_hold_pos, 0, memory_order_release);
                        pthread_mutex_unlock(&db->publish_lock);
                        cleanup_prepared(db, out, n, true);
                        rc = 0;
                    } else {
                        rc = ec_swap_unsettle_at(db, in, out, n, pos);
                    }
                    jr_serial_unlock(db);
                }
            }
            if (committed && rc != 0 && eng_check(db) == 0)
                eng_fail(db, "unsettle swap failed after MK_UNSETTLE committed", EIO);
            /* On a failed swap after commit (or INDETERMINATE with pos != 0), leave unsettle_hold_pos non-zero so
             * REPL.floor and snap_make never pass pos. */
            cleanup_prepared(db, out, n, rc != 0);
        }
    }

    if (rc == 0) {
        atomic_fetch_add_explicit(&ecunsettle_from_settled, n_settled, memory_order_relaxed);
        atomic_fetch_add_explicit(&ecunsettle_from_decode, n_decode, memory_order_relaxed);
        atomic_fetch_add_explicit(&ecunsettle_keys_done, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&ecunsettle_objs_done, n, memory_order_relaxed);
        atomic_fetch_add_explicit(&ecunsettle_bytes_done, total_bytes, memory_order_relaxed);
        if (objs_out)
            *objs_out = n;
        if (bytes_out)
            *bytes_out = total_bytes;
    } else {
        atomic_fetch_add_explicit(&ecunsettle_keys_failed, 1, memory_order_relaxed);
    }

    free(oids);
    free(out);
unpin:
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < n; i++)
        in[i]->settling = false;
    pthread_mutex_unlock(&db->lock);
    for (size_t i = 0; i < n; i++)
        part_unref(in[i]);
    free(in);
    return rc;
#endif
}

int eng_unsettle(eng *db, int64_t partition_key)
{
    return eng_unsettle_key(db, partition_key, NULL, NULL);
}

int ec_unsettle_apply(eng *db, uint64_t pos, const mk *m)
{
    if (!db || !m || m->type != MK_UNSETTLE)
        return TSDB_ERR_INVALID;
#ifndef TSDB_EC
    (void)pos;
    fprintf(stderr, "tsdb: %s: MK_UNSETTLE at pos %" PRIu64 " needs the erasure codec (TSDB_EC)\n", db->dir, pos);
    return TSDB_ERR_INVALID;
#else
    ecunsettle_env_init();
    if (ecunsettle_mut() == ECUNSETTLE_MUT_LEADERONLY && db->rp)
        return 0; /* planted bug: replicas skip applying MK_UNSETTLE */
    if (eng_check(db))
        return TSDB_ERR_IO;

    const mk_unsettle *u = &m->u.unsettle;
    part **in = NULL;
    size_t n = 0, cap = 0;
    int rc = 0;

    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++) {
        part *p = db->parts[i];
        struct ecpart *ep = atomic_load(&p->ec);
        if (p->partition != u->key || p->dropped || !ep || ep->test_mark)
            continue;
        if (!oid_in_list(ep->rec.oid, u->oids, u->noids))
            continue;
        if (p->settling || p->reencoding || p->merging) {
            rc = TSDB_ERR_LOCKED;
            break;
        }
        if (n == cap) {
            size_t nc = cap ? cap * 2 : 8;
            part **x = realloc(in, nc * sizeof *x);
            if (!x) {
                rc = TSDB_ERR_NOMEM;
                break;
            }
            in = x;
            cap = nc;
        }
        part_ref(p);
        p->settling = true;
        in[n++] = p;
    }
    if (rc != 0) {
        for (size_t i = 0; i < n; i++)
            in[i]->settling = false;
    }
    pthread_mutex_unlock(&db->lock);

    if (rc != 0) {
        for (size_t i = 0; i < n; i++)
            part_unref(in[i]);
        free(in);
        return rc;
    }
    if (n == 0) {
        free(in);
        return 0; /* idempotent replay: all named .ec parts already swapped or dropped */
    }

    part **out = calloc(n, sizeof *out);
    if (!out) {
        rc = TSDB_ERR_NOMEM;
        goto unpin;
    }

    bool rank_mut = ecunsettle_mut() == ECUNSETTLE_MUT_RANK;
    bool decode_only = false;
    /* Bounded retry on transient TSDB_ERR_UNAVAILABLE (chunk-store outage/restart): replica_apply runs synchronously on
     * repld's volume thread inside REPL_A_APPLY, so retrying for up to retry_ms (default 30 s) rides out brief store
     * restarts without failing the replica, while bounding the volume-thread stall so a prolonged outage cleanly drops
     * the replica from ISR and fails it for later reopen/rebuild rather than acknowledging MK_UNSETTLE unswapped. */
    int64_t retry_ms = atomic_load(&ecunsettle_test_retry_ms);
    if (retry_ms < 0)
        retry_ms = 30000;
    int64_t t0 = eng_mono_ms();
    bool logged_unavail = false;
    for (int attempt = 0; attempt < 2; attempt++) {
        size_t n_settled = 0, n_decode = 0;
        uint64_t gen = replica_mu_drop(db);
        for (;;) {
            rc = prepare_unsettle_parts(db, in, n, rank_mut, decode_only, out, &n_settled, &n_decode);
            if (rc != TSDB_ERR_UNAVAILABLE || atomic_load_explicit(&db->stop, memory_order_acquire) ||
                atomic_load_explicit(&db->replica_stop, memory_order_acquire) || eng_check(db) != 0 ||
                eng_mono_ms() - t0 >= retry_ms)
                break;
            cleanup_prepared(db, out, n, true);
            if (!logged_unavail) {
                fprintf(stderr, "tsdb: %s: MK_UNSETTLE at pos %" PRIu64 " of key %" PRId64
                                ": chunk stores unavailable; retrying before failing replica\n",
                        db->dir, pos, u->key);
                logged_unavail = true;
            }
            struct timespec ts = {0, 200 * 1000000L};
            nanosleep(&ts, NULL);
            if (replica_mu_retake(db, gen)) {
                rc = RP_AGAIN;
                goto done_loop;
            }
            gen = replica_mu_drop(db);
        }
        if (rc == 0) {
            void (*ch)(eng *) = atomic_load(&ecunsettle_crash_hook);
            if (ch)
                ch(db);
        }
        if (replica_mu_retake(db, gen)) {
            cleanup_prepared(db, out, n, true);
            rc = RP_AGAIN;
            break;
        }
        if (rc != 0) {
            cleanup_prepared(db, out, n, true);
            break;
        }
        rc = ec_swap_unsettle_at(db, in, out, n, 0);
        if (rc == TSDB_ERR_NOTFOUND && !decode_only) {
            cleanup_prepared(db, out, n, true);
            decode_only = true;
            continue;
        }
        if (rc == 0) {
            atomic_fetch_add_explicit(&ecunsettle_from_settled, n_settled, memory_order_relaxed);
            atomic_fetch_add_explicit(&ecunsettle_from_decode, n_decode, memory_order_relaxed);
        }
        cleanup_prepared(db, out, n, rc != 0);
        break;
    }
done_loop:

    free(out);
unpin:
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < n; i++)
        in[i]->settling = false;
    pthread_mutex_unlock(&db->lock);
    for (size_t i = 0; i < n; i++)
        part_unref(in[i]);
    free(in);
    if (rc != 0 && rc != RP_AGAIN)
        fprintf(stderr, "tsdb: %s: MK_UNSETTLE at pos %" PRIu64 " of key %" PRId64 " failed: %s\n", db->dir, pos,
                u->key, tsdb_strerror(rc));
    return rc;
#endif
}

/* ------------------------------------------------------------ CLI: volume diverged --clear */

#define DIV_FILE_NAME "DIVERGED"
#define DIV_REC_BYTES 48

static bool div_rec_ok(const uint8_t *rec, uint64_t off)
{
    uint32_t c, zero;
    memcpy(&c, rec, 4);
    memcpy(&zero, rec + 44, 4);
    return c == (uint32_t)xxh64(rec + 4, DIV_REC_BYTES - 4, off) && zero == 0;
}

/* Reads unique partition keys from vol_dir/DIVERGED (read-only). 0 on success (*nkeys == 0 if none),
 * TSDB_ERR_CORRUPT on damage before valid records, TSDB_ERR_IO/NOMEM on error. */
static int read_diverged_keys(int dirfd, int64_t **keys_out, size_t *nkeys_out)
{
    *keys_out = NULL;
    *nkeys_out = 0;
    int fd = openat(dirfd, DIV_FILE_NAME, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
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
    while (got < size) {
        ssize_t r = pread(fd, buf + got, size - got, (off_t)got);
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0)
            break;
        got += (size_t)r;
    }
    close(fd);
    if (got != size) {
        free(buf);
        return TSDB_ERR_IO;
    }
    int64_t *keys = NULL;
    size_t n = 0, cap = 0, pos = 0;
    int rc = 0;
    for (; rc == 0 && pos + DIV_REC_BYTES <= size && div_rec_ok(buf + pos, pos); pos += DIV_REC_BYTES) {
        int64_t k;
        memcpy(&k, buf + pos + 8, 8);
        bool dup = false;
        for (size_t i = 0; i < n && !dup; i++)
            dup = keys[i] == k;
        if (dup)
            continue;
        if (n == cap) {
            size_t nc = cap ? cap * 2 : 8;
            int64_t *x = realloc(keys, nc * sizeof *x);
            if (!x) {
                rc = TSDB_ERR_NOMEM;
                break;
            }
            keys = x;
            cap = nc;
        }
        keys[n++] = k;
    }
    for (size_t q = pos + DIV_REC_BYTES; rc == 0 && q + DIV_REC_BYTES <= size; q += DIV_REC_BYTES) {
        if (div_rec_ok(buf + q, q)) {
            rc = TSDB_ERR_CORRUPT;
            break;
        }
    }
    free(buf);
    if (rc != 0) {
        free(keys);
        return rc;
    }
    *keys_out = keys;
    *nkeys_out = n;
    return 0;
}

/* Returns 1 if dirfd contains any .ec part whose partition key is in keys[0..nkeys), 0 if none, <0 on error. */
static int dir_has_ec_for_keys(int dirfd, const int64_t *keys, size_t nkeys)
{
    int dfd = dup(dirfd);
    DIR *d = dfd >= 0 ? fdopendir(dfd) : NULL;
    if (!d) {
        if (dfd >= 0)
            close(dfd);
        return TSDB_ERR_IO;
    }
    rewinddir(d);
    int found = 0;
    struct dirent *de;
    for (errno = 0; (de = readdir(d)) != NULL; errno = 0) {
        if (!ec_file_name(de->d_name))
            continue;
        int64_t k = 0;
        if (sscanf(de->d_name, "p%" SCNd64 "_", &k) != 1)
            continue;
        for (size_t i = 0; i < nkeys; i++) {
            if (keys[i] == k) {
                found = 1;
                break;
            }
        }
        if (found)
            break;
    }
    int err = errno;
    closedir(d);
    return found ? 1 : err ? TSDB_ERR_IO : 0;
}

int ec_unsettle_diverged_cli(const char *root_dir, const char *vol_dir, FILE *out)
{
    /* Strip trailing slashes (except "/") so O_NOFOLLOW refuses symlinked target directories too. */
    size_t dlen = strlen(vol_dir);
    while (dlen > 1 && vol_dir[dlen - 1] == '/')
        dlen--;
    char *dcopy = NULL;
    const char *p = vol_dir;
    if (vol_dir[dlen] != '\0') {
        dcopy = malloc(dlen + 1);
        if (!dcopy)
            return TSDB_ERR_NOMEM;
        memcpy(dcopy, vol_dir, dlen);
        dcopy[dlen] = '\0';
        p = dcopy;
    }
    int dirfd = open(p, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    int oerr = errno;
    free(dcopy);
    if (dirfd < 0) {
        fprintf(stderr, "tsdb: %s: %s\n", vol_dir, strerror(oerr));
        return oerr == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    }

    /* Check META and cluster refusal before opening any engine (same rules as ecdiverge_cli). */
    unsigned feats = 0;
    char meta[4096];
    ssize_t mn = -1;
    int mfd = openat(dirfd, "META", O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (mfd >= 0) {
        mn = read(mfd, meta, sizeof meta - 1);
        close(mfd);
    }
    if (mn < 0) {
        int err = errno;
        close(dirfd);
        fprintf(stderr, "tsdb: %s/META cannot be read (%s): not cleared, whether it is a cluster volume is unknown\n",
                vol_dir, strerror(err));
        return err == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    }
    meta[mn] = 0;
    meta_requires_parse(meta, &feats, NULL, 0);
    uint32_t vol_idx = 0;
    for (const char *ml = meta; ml && *ml;) {
        if (strncmp(ml, "volume=", 7) == 0) {
            unsigned long v = strtoul(ml + 7, NULL, 10);
            vol_idx = (uint32_t)v;
            break;
        }
        ml = strchr(ml, '\n');
        if (ml)
            ml++;
    }
    if (feats & META_REQ_CLUSTER) {
        close(dirfd);
        fprintf(stderr,
                "tsdb: %s is a cluster volume's directory: its DIVERGED is cleared only by rebuilding the replica, "
                "which comes with R4 (design-cluster-data.md D23: a diverged replica back in the ISR could be promoted "
                "with its diverged rows): refused\n",
                vol_dir);
        return TSDB_ERR_INVALID;
    }

    /* Verify no writer holds LOCK (and LOCK is not a symlink). */
    int lockfd = openat(dirfd, "LOCK", O_RDWR | O_CREAT | O_NOFOLLOW | O_CLOEXEC, 0644);
    if (lockfd < 0 || flock(lockfd, LOCK_EX | LOCK_NB) != 0) {
        int rc = lockfd < 0 ? TSDB_ERR_IO : TSDB_ERR_LOCKED;
        if (lockfd >= 0)
            close(lockfd);
        close(dirfd);
        fprintf(stderr, "tsdb: %s: a writer (tsdbd, or another tsdb command) holds the directory: stop it first; "
                        "nothing is cleared\n",
                vol_dir);
        return rc;
    }

    int64_t *keys = NULL;
    size_t nkeys = 0;
    int rc = read_diverged_keys(dirfd, &keys, &nkeys);
    int has_ec = (rc == 0 && nkeys > 0) ? dir_has_ec_for_keys(dirfd, keys, nkeys) : 0;
    close(lockfd);
    close(dirfd);

    if (rc != 0 || has_ec < 0) {
        free(keys);
        return rc != 0 ? rc : has_ec;
    }

    if (has_ec > 0) {
#ifndef TSDB_EC
        (void)root_dir;
        (void)vol_idx;
        free(keys);
        fprintf(stderr, "tsdb: %s: diverged key(s) have .ec parts, but this build lacks TSDB_EC: DIVERGED not cleared\n",
                vol_dir);
        return TSDB_ERR_INVALID;
#else
        tsdb_options o;
        tsdb_options_init(&o);
        o.read_only = 0;
        o.merge_threads = 0;
        o.create_volumes = 0;
        o.stats = -1;
        tier *t = NULL;
        rc = tier_open(root_dir, &o, &t);
        if (rc != 0) {
            free(keys);
            fprintf(stderr, "tsdb: %s: cannot open tier to unsettle diverged keys (%s): DIVERGED not cleared\n",
                    root_dir, tsdb_strerror(rc));
            return rc;
        }
        /* Quiesce settle/gc workers BEFORE opening the volume so no background settle can run or re-settle keys. */
        tier_quiesce(t);
        eng *target = NULL;
        tier_want(t, vol_idx);
        rc = eng_open(vol_dir, &o, &target);
        tier_want(NULL, 0);
        if (rc != 0 || !target) {
            tier_put(t);
            free(keys);
            fprintf(stderr, "tsdb: %s: cannot open volume to unsettle diverged keys (%s): DIVERGED not cleared\n",
                    vol_dir, tsdb_strerror(rc));
            return rc ? rc : TSDB_ERR_IO;
        }
        eckeep_eng_stop(target);
        /* Re-read DIVERGED after stopping eckeep so any mismatch recorded by the digest thread between eng_open and
         * eckeep_eng_stop is also unsettled before DIVERGED is cleared. */
        int64_t *keys2 = NULL;
        size_t nkeys2 = 0;
        rc = read_diverged_keys(target->dirfd, &keys2, &nkeys2);
        if (rc == 0 && nkeys2 > 0) {
            for (size_t i = 0; i < nkeys2; i++) {
                bool dup = false;
                for (size_t j = 0; j < nkeys && !dup; j++)
                    dup = keys[j] == keys2[i];
                if (dup)
                    continue;
                int64_t *grown = realloc(keys, (nkeys + 1) * sizeof *grown);
                if (!grown) {
                    rc = TSDB_ERR_NOMEM;
                    break;
                }
                keys = grown;
                keys[nkeys++] = keys2[i];
            }
        }
        free(keys2);
        for (size_t i = 0; i < nkeys && rc == 0; i++) {
            rc = eng_unsettle(target, keys[i]);
            if (rc != 0)
                fprintf(stderr, "tsdb: %s: unsettle of diverged partition %" PRId64 " failed (%s): "
                                "DIVERGED not cleared\n",
                        vol_dir, keys[i], tsdb_strerror(rc));
        }
        int crc = eng_close(target);
        tier_put(t);
        if (rc == 0 && crc != 0)
            rc = crc;
        free(keys);
        if (rc != 0)
            return rc;
#endif
    } else {
        (void)root_dir;
        (void)vol_idx;
        free(keys);
    }

    /* Only clear the durable DIVERGED file after every unsettle swap is durable on disk. Note: LOCK is released by
     * eng_close above and re-acquired inside ecdiverge_cli; operators must keep writers (tsdbd) stopped throughout. */
    return ecdiverge_cli(vol_dir, true, out);
}
