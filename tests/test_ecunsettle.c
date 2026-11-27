/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Tests for R15 r15-reverse: reverse intent v5 (L/U lines), eng_unsettle, and `tsdb DIR volume diverged --clear`.
 *
 *   settled_fast     settled_P.part fast path: rank/name preserved, LWW with newer part, stats, lgen, reopen
 *   decode_missing   settled_P.part absent: decodes from tier seam, verifies sums/digest/granules, rank preserved
 *   decode_flipped   settled_P.part has a flipped granule byte and a flipped footer byte -> falls back to decode
 *   decode_checks    corrupted single chunk cell rebuilt via parity (catches RAW/unsettlenodecodecheck) AND
 *                    tampered object_xxh64 refused with TSDB_ERR_CORRUPT (catches unsettlenodecodecheck)
 *   oracle_twins     twin databases (A never settles; B settles then unsettles via both paths): bit-for-bit equal
 *   recover_hand     every rule of v5 unsettle recovery on hand-planted intents (torn, forward, half, rollback
 *                    keeping settled_, drop before/after, damage, crafted bad intents)
 *   concurrent_drop  drop completing in ec_swap_hook before db->lock -> TSDB_ERR_STOPPED, nothing resurrected
 *   power_loss       crashfs power cut at every syscall step of eng_unsettle (both settled_ and decode paths)
 *   cli_diverged     end-to-end `tsdb DIR volume diverged --clear`: restores from settled_ even when chunks corrupt,
 *                    refuses to clear DIVERGED if unsettle fails, and succeeds via decode path
 *
 * Usage: test_ecunsettle [--quick] [--mutant none|unsettlenodecodecheck|unsettlerank]
 *                        [--child settled|decode DIR] [SCRATCH]
 */
#define _GNU_SOURCE
#include "internal.h"
#include "chunk.h"
#include "ecdiverge.h"
#include "ecgc.h"
#include "eckeep.h"
#include "ecpart.h"
#include "ecswap.h"
#include "ecunsettle.h"
#include "settle.h"
#include "stats_priv.h"
#include "tier.h"
#include "tiers.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static int failures;
#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);                                               \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

#ifdef TSDB_EC

static char S[PATH_MAX], SELF[PATH_MAX], SHIM[PATH_MAX], CLI[PATH_MAX];
static bool quick;
static const int64_t DAY = 86400000LL;
static const int64_t T0 = 20LL * 86400000LL;

static int sh(const char *fmt, ...)
{
    char cmd[4096];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(cmd, sizeof cmd, fmt, ap);
    va_end(ap);
    return system(cmd);
}

static void fresh(const char *what, char *out, size_t cap)
{
    int r = snprintf(out, cap, "%s/%s", S, what);
    CHECK(r > 0 && (size_t)r < cap);
    sh("rm -rf '%s'; mkdir -p '%s'", out, out);
}

static bool exists(const char *dir, const char *name)
{
    char p[PATH_MAX + 256];
    snprintf(p, sizeof p, "%s/%s", dir, name);
    return access(p, F_OK) == 0;
}

static size_t count_names(const char *dir, const char *prefix, const char *suffix)
{
    DIR *d = opendir(dir);
    size_t n = 0;
    struct dirent *e;
    while (d && (e = readdir(d))) {
        size_t l = strlen(e->d_name), ls = strlen(suffix);
        n += strncmp(e->d_name, prefix, strlen(prefix)) == 0 && l >= ls && strcmp(e->d_name + l - ls, suffix) == 0;
    }
    if (d)
        closedir(d);
    return n;
}

typedef struct {
    size_t n;
    double sum;
    uint64_t hash;
} acc;

static int acc_f32(void *ctx, const int64_t *ts, const void *v, size_t n)
{
    acc *a = ctx;
    const float *fv = v;
    for (size_t i = 0; i < n; i++) {
        uint8_t rec[12];
        memcpy(rec, &ts[i], 8);
        memcpy(rec + 8, &fv[i], 4);
        a->hash = xxh64(rec, sizeof rec, a->hash);
        a->sum += fv[i];
    }
    a->n += n;
    return 0;
}

static int acc_i64(void *ctx, const int64_t *ts, const void *v, size_t n)
{
    acc *a = ctx;
    const int64_t *iv = v;
    for (size_t i = 0; i < n; i++) {
        uint8_t rec[16];
        memcpy(rec, &ts[i], 8);
        memcpy(rec + 8, &iv[i], 8);
        a->hash = xxh64(rec, sizeof rec, a->hash);
        a->sum += (double)iv[i];
    }
    a->n += n;
    return 0;
}

static acc read_f32(eng *db, uint32_t s, int64_t a, int64_t b)
{
    acc r = {0, 0, 0};
    CHECK(eng_query_raw_ex(db, s, a, b, false, 0, acc_f32, &r) == 0);
    return r;
}

static acc read_i64(eng *db, uint32_t s, int64_t a, int64_t b)
{
    acc r = {0, 0, 0};
    CHECK(eng_query_raw_ex(db, s, a, b, false, 0, acc_i64, &r) == 0);
    return r;
}

static void write_f32(eng *db, uint32_t s, int64_t t0, int n, int64_t step, float bias)
{
    int64_t ts[256];
    float v[256];
    for (int i = 0; i < n; i += 256) {
        int k = n - i < 256 ? n - i : 256;
        for (int j = 0; j < k; j++) {
            ts[j] = t0 + (int64_t)(i + j) * step;
            v[j] = bias + (float)((i + j) % 977);
        }
        CHECK(eng_insert_series(db, TSDB_FLOAT32, s, ts, v, (size_t)k) == 0);
    }
}

static void write_i64(eng *db, uint32_t s, int64_t t0, int n, int64_t step, int64_t bias)
{
    int64_t ts[256], v[256];
    for (int i = 0; i < n; i += 256) {
        int k = n - i < 256 ? n - i : 256;
        for (int j = 0; j < k; j++) {
            ts[j] = t0 + (int64_t)(i + j) * step;
            v[j] = bias + (int64_t)(i + j) * 13;
        }
        CHECK(eng_insert_series(db, TSDB_INT64, s, ts, v, (size_t)k) == 0);
    }
}

/* Snapshot of a key's part metadata for exact rank/name verification. */
typedef struct {
    char stem[96];
    uint64_t min_block, max_block;
    uint32_t level;
    bool is_ec;
} part_sig;

static size_t snap_key(eng *db, int64_t key, part_sig *out, size_t cap)
{
    size_t n = 0;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts && n < cap; i++) {
        part *p = db->parts[i];
        if (p->partition != key)
            continue;
        bool ec = atomic_load(&p->ec) != NULL;
        size_t l = strlen(p->name);
        size_t sl = ec ? l - 3 : l - 5;
        snprintf(out[n].stem, sizeof out[n].stem, "%.*s", (int)sl, p->name);
        out[n].min_block = p->min_block;
        out[n].max_block = p->max_block;
        out[n].level = p->level;
        out[n].is_ec = ec;
        n++;
    }
    pthread_mutex_unlock(&db->lock);
    return n;
}

static void check_sigs_match(const part_sig *a, size_t na, const part_sig *b, size_t nb, bool want_ec,
                             const char *what)
{
    CHECK(na == nb);
    for (size_t i = 0; i < na && i < nb; i++) {
        if (strcmp(a[i].stem, b[i].stem) != 0 || a[i].min_block != b[i].min_block ||
            a[i].max_block != b[i].max_block || a[i].level != b[i].level || b[i].is_ec != want_ec) {
            fprintf(stderr, "FAIL %s[%zu]: want %s [%" PRIu64 "..%" PRIu64 "] L%u ec=%d, got %s [%" PRIu64 "..%" PRIu64
                            "] L%u ec=%d\n",
                    what, i, a[i].stem, a[i].min_block, a[i].max_block, a[i].level, (int)want_ec, b[i].stem,
                    b[i].min_block, b[i].max_block, b[i].level, (int)b[i].is_ec);
            failures++;
        }
    }
}

typedef struct {
    tier *t;
    eng *db;
} thandle;

static void close_tiered(thandle *h)
{
    if (h->db) {
        CHECK(eng_close(h->db) == 0);
        h->db = NULL;
    }
    if (h->t) {
        tier_put(h->t);
        h->t = NULL;
    }
}

static void open_tiered_quiet(const char *dbdir, thandle *h)
{
    memset(h, 0, sizeof *h);
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.partition_classes = 0;
    CHECK(tier_open(dbdir, &o, &h->t) == 0 && h->t != NULL);
    /* Quiesce settle/gc workers BEFORE opening the volume so old test keys never auto-settle. */
    tier_quiesce(h->t);
    tier_want(h->t, 0);
    CHECK(eng_open(dbdir, &o, &h->db) == 0 && h->db != NULL);
    tier_want(NULL, 0);
    if (h->db) {
        CHECK(h->db->tier != NULL);
        eckeep_eng_stop(h->db);
    }
}

/* Sets up a real R15 TIERS single-server database (dir/db + 3 stores dir/st0..2, default 2+1). */
static void make_tiered_db(const char *base, char *dbdir, size_t cap, char stores[3][PATH_MAX], thandle *h)
{
    int r = snprintf(dbdir, cap, "%s/db", base);
    CHECK(r > 0 && (size_t)r < cap);
    CHECK(mkdir(dbdir, 0755) == 0);
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.partition_classes = 0;
    tsdb *t = NULL;
    CHECK(tsdb_open(dbdir, &o, &t) == 0 && t != NULL);
    CHECK(tsdb_close(t) == 0);
    for (int i = 0; i < 3; i++) {
        r = snprintf(stores[i], PATH_MAX, "%s/st%d", base, i);
        CHECK(r > 0 && (size_t)r < PATH_MAX);
        CHECK(mkdir(stores[i], 0755) == 0);
        CHECK(tsdb_tier_add_dir(dbdir, stores[i], "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);
    }
    open_tiered_quiet(dbdir, h);
}

static int settle_key(eng *db, int64_t key)
{
    part *sel[32];
    size_t n = 0;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts && n < 32; i++) {
        part *p = db->parts[i];
        if (p->partition == key && !p->dropped && !atomic_load(&p->ec) && p->ng > 0) {
            part_ref(p);
            sel[n++] = p;
        }
    }
    pthread_mutex_unlock(&db->lock);
    if (n == 0)
        return 0;
    size_t done = 0;
    int rc = eng_settle(db, key, sel, n, &done);
    for (size_t i = 0; i < n; i++)
        part_unref(sel[i]);
    return rc == 0 && done == n ? 0 : rc ? rc : TSDB_ERR_IO;
}

/* helper: reopen_tiered_db replaced by open_tiered_quiet */

static uint64_t lgen_of(eng *db)
{
    if (!db->lt)
        return 0;
    pthread_rwlock_rdlock(&db->lt->rw);
    uint64_t g = db->lt->lgen;
    pthread_rwlock_unlock(&db->lt->rw);
    return g;
}

static void flip_byte_in_file(const char *dir, const char *name, off_t off)
{
    char p[PATH_MAX + 256];
    snprintf(p, sizeof p, "%s/%s", dir, name);
    int fd = open(p, O_RDWR);
    CHECK(fd >= 0);
    if (fd < 0)
        return;
    struct stat st;
    CHECK(fstat(fd, &st) == 0);
    if (off < 0)
        off = st.st_size + off;
    CHECK(off >= 0 && off < st.st_size);
    uint8_t b = 0;
    CHECK(pread(fd, &b, 1, off) == 1);
    b ^= 0x55;
    CHECK(pwrite(fd, &b, 1, off) == 1);
    CHECK(fsync(fd) == 0);
    close(fd);
}

/* Populates key K (= T0/DAY) with 2 flushed parts (overlapping LWW timestamps), settles them, then writes a 3rd
 * newer .part overlapping the same timestamps so any rank change (unsettlerank!) flips LWW query answers! */
static void setup_lww_settled(eng *db, int64_t key, uint32_t *s_out, acc *oracle_out, part_sig *ec_sigs, size_t *nec,
                              part_sig *all_sigs, size_t *nall)
{
    uint32_t s = 0;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    *s_out = s;
    int64_t base = key * DAY;
    /* Part 0: timestamps base + 0, 1000, ... with bias 10.0 */
    write_f32(db, s, base, 1500, 1000, 10.0f);
    CHECK(eng_flush(db) == 0);
    /* Part 1: overlaps half of Part 0 with bias 50.0 */
    write_f32(db, s, base + 500 * 1000, 1500, 1000, 50.0f);
    CHECK(eng_flush(db) == 0);

    part_sig before[8];
    size_t nb = snap_key(db, key, before, 8);
    CHECK(nb == 2);
    CHECK(settle_key(db, key) == 0);
    *nec = snap_key(db, key, ec_sigs, 8);
    CHECK(*nec == 2);
    check_sigs_match(before, nb, ec_sigs, *nec, true, "after settle");

    /* Part 2 (young .part, HIGHER block number than the settled parts!): overwrites base + 200..1200 with bias 200.0.
     * If unsettle preserves rank (blocks of Part 0 & Part 1), Part 2 STILL wins LWW.
     * If unsettlerank allocates new block numbers > Part 2, Part 0/1 overwrite Part 2 and LWW breaks! */
    write_f32(db, s, base + 200 * 1000, 1000, 1000, 200.0f);
    CHECK(eng_flush(db) == 0);

    *nall = snap_key(db, key, all_sigs, 8);
    CHECK(*nall == 3);
    *oracle_out = read_f32(db, s, INT64_MIN, INT64_MAX);
    CHECK(oracle_out->n == 2000);
}

/* ------------------------------------------------------------ 1. settled_ fast path */

static void case_settled_fast(void)
{
    char base[PATH_MAX], dbdir[PATH_MAX], stores[3][PATH_MAX];
    fresh("settled_fast", base, sizeof base);
    thandle h;
    make_tiered_db(base, dbdir, sizeof dbdir, stores, &h);
    eng *db = h.db;
    int64_t key = T0 / DAY;
    uint32_t s = 0;
    acc oracle = {0};
    part_sig ec_sigs[8], all_sigs[8], after_sigs[8];
    size_t nec = 0, nall = 0;
    setup_lww_settled(db, key, &s, &oracle, ec_sigs, &nec, all_sigs, &nall);

    CHECK(count_names(dbdir, "settled_", ".part") == 2);
    CHECK(stats_settle(db, 60000) == 0);
    uint64_t g0 = lgen_of(db);
    uint64_t s0 = atomic_load(&ecunsettle_from_settled);
    uint64_t d0 = atomic_load(&ecunsettle_from_decode);

    CHECK(eng_unsettle(db, key) == 0);
    CHECK(atomic_load(&ecunsettle_from_settled) == s0 + 2);
    CHECK(atomic_load(&ecunsettle_from_decode) == d0);
    CHECK(!db->lt || lgen_of(db) > g0);

    size_t na = snap_key(db, key, after_sigs, 8);
    check_sigs_match(all_sigs, nall, after_sigs, na, false, "settled_fast in-mem");
    CHECK(count_names(dbdir, "p", ".ec") == 0);
    CHECK(count_names(dbdir, "retired_", ".ec") == 2);
    CHECK(count_names(dbdir, "settled_", ".part") == 0);
    CHECK(count_names(dbdir, "txn_", ".intent") == 0 && count_names(dbdir, "tmp_", ".part") == 0);

    acc got = read_f32(db, s, INT64_MIN, INT64_MAX);
    CHECK(got.n == oracle.n && got.sum == oracle.sum && got.hash == oracle.hash);

    CHECK(stats_settle(db, 60000) == 0);
    struct estats *es = atomic_load(&db->stats);
    if (es) {
        pthread_mutex_lock(&db->lock);
        pthread_mutex_lock(&es->mu);
        for (size_t i = 0; i < db->nparts; i++)
            CHECK(rec_find(es, db->parts[i]) != NULL);
        pthread_mutex_unlock(&es->mu);
        pthread_mutex_unlock(&db->lock);
    }

    /* Idempotent when no .ec parts remain */
    CHECK(eng_unsettle(db, key) == 0);

    close_tiered(&h);
    open_tiered_quiet(dbdir, &h);
    db = h.db;
    na = snap_key(db, key, after_sigs, 8);
    check_sigs_match(all_sigs, nall, after_sigs, na, false, "settled_fast reopen");
    got = read_f32(db, s, INT64_MIN, INT64_MAX);
    CHECK(got.n == oracle.n && got.sum == oracle.sum && got.hash == oracle.hash);
    close_tiered(&h);
    printf("  settled_ fast path: 2 parts restored from settled_, ranks & LWW preserved across reopen\n");
}

/* ------------------------------------------------------------ 2. decode path (settled_ missing) */

static void case_decode_missing(void)
{
    char base[PATH_MAX], dbdir[PATH_MAX], stores[3][PATH_MAX];
    fresh("decode_missing", base, sizeof base);
    thandle h;
    make_tiered_db(base, dbdir, sizeof dbdir, stores, &h);
    eng *db = h.db;
    int64_t key = T0 / DAY;
    uint32_t s = 0;
    acc oracle = {0};
    part_sig ec_sigs[8], all_sigs[8], after_sigs[8];
    size_t nec = 0, nall = 0;
    setup_lww_settled(db, key, &s, &oracle, ec_sigs, &nec, all_sigs, &nall);

    for (size_t i = 0; i < nec; i++) {
        char nm[128];
        snprintf(nm, sizeof nm, "settled_%.95s.part", ec_sigs[i].stem);
        CHECK(unlinkat(db->dirfd, nm, 0) == 0);
    }
    CHECK(count_names(dbdir, "settled_", ".part") == 0);

    uint64_t s0 = atomic_load(&ecunsettle_from_settled);
    uint64_t d0 = atomic_load(&ecunsettle_from_decode);
    CHECK(eng_unsettle(db, key) == 0);
    CHECK(atomic_load(&ecunsettle_from_settled) == s0);
    CHECK(atomic_load(&ecunsettle_from_decode) == d0 + 2);

    size_t na = snap_key(db, key, after_sigs, 8);
    check_sigs_match(all_sigs, nall, after_sigs, na, false, "decode_missing in-mem");
    CHECK(count_names(dbdir, "p", ".ec") == 0);
    CHECK(count_names(dbdir, "retired_", ".ec") == 2);
    CHECK(count_names(dbdir, "txn_", ".intent") == 0 && count_names(dbdir, "tmp_", ".part") == 0);

    acc got = read_f32(db, s, INT64_MIN, INT64_MAX);
    CHECK(got.n == oracle.n && got.sum == oracle.sum && got.hash == oracle.hash);

    close_tiered(&h);
    open_tiered_quiet(dbdir, &h);
    db = h.db;
    na = snap_key(db, key, after_sigs, 8);
    check_sigs_match(all_sigs, nall, after_sigs, na, false, "decode_missing reopen");
    got = read_f32(db, s, INT64_MIN, INT64_MAX);
    CHECK(got.n == oracle.n && got.sum == oracle.sum && got.hash == oracle.hash);
    close_tiered(&h);
    printf("  decode path (settled_ missing): 2 parts decoded & verified, ranks & LWW preserved across reopen\n");
}

/* ------------------------------------------------------------ 3. flipped settled_ -> decode fallback */

static void case_decode_flipped(void)
{
    char base[PATH_MAX], dbdir[PATH_MAX], stores[3][PATH_MAX];
    fresh("decode_flipped", base, sizeof base);
    thandle h;
    make_tiered_db(base, dbdir, sizeof dbdir, stores, &h);
    eng *db = h.db;
    int64_t key = T0 / DAY;
    uint32_t s = 0;
    acc oracle = {0};
    part_sig ec_sigs[8], all_sigs[8], after_sigs[8];
    size_t nec = 0, nall = 0;
    setup_lww_settled(db, key, &s, &oracle, ec_sigs, &nec, all_sigs, &nall);

    /* Flip 1 byte in a granule payload of settled_0 (footer hash stays valid; only granule CRC catches it!)
     * and 1 byte in the footer of settled_1 (footer hash catches it!). */
    char nm0[128], nm1[128];
    snprintf(nm0, sizeof nm0, "settled_%.95s.part", ec_sigs[0].stem);
    snprintf(nm1, sizeof nm1, "settled_%.95s.part", ec_sigs[1].stem);
    flip_byte_in_file(dbdir, nm0, (off_t)sizeof(part_header) + 16);
    flip_byte_in_file(dbdir, nm1, -16);

    uint64_t s0 = atomic_load(&ecunsettle_from_settled);
    uint64_t d0 = atomic_load(&ecunsettle_from_decode);
    CHECK(eng_unsettle(db, key) == 0);
    CHECK(atomic_load(&ecunsettle_from_settled) == s0);
    CHECK(atomic_load(&ecunsettle_from_decode) == d0 + 2);

    size_t na = snap_key(db, key, after_sigs, 8);
    check_sigs_match(all_sigs, nall, after_sigs, na, false, "decode_flipped");
    acc got = read_f32(db, s, INT64_MIN, INT64_MAX);
    CHECK(got.n == oracle.n && got.sum == oracle.sum && got.hash == oracle.hash);
    close_tiered(&h);
    printf("  flipped settled_ fallback: granule-byte flip & footer flip both fell back to decode cleanly\n");
}

/* ------------------------------------------------------------ 4. decode verification & corruption checks */

/* Corrupts chunk files found under store_dir (past the 64-byte header, inside payload).
 * If `all` is false, stops after corrupting 1 chunk file; if true, corrupts every chunk file under store_dir. */
static int corrupt_chunks_in(const char *dir, bool all)
{
    DIR *d = opendir(dir);
    if (!d)
        return 0;
    struct dirent *de;
    int done = 0;
    while ((all || !done) && (de = readdir(d))) {
        if (de->d_name[0] == '.')
            continue;
        char p[PATH_MAX + 256];
        snprintf(p, sizeof p, "%s/%s", dir, de->d_name);
        struct stat st;
        if (stat(p, &st) != 0)
            continue;
        if (S_ISDIR(st.st_mode)) {
            done += corrupt_chunks_in(p, all);
        } else if (S_ISREG(st.st_mode) && strncmp(de->d_name, "META", 4) != 0 &&
                   strncmp(de->d_name, "EPOCH", 5) != 0 && strncmp(de->d_name, "LOCK", 4) != 0 && st.st_size > 128) {
            int fd = open(p, O_RDWR);
            if (fd >= 0) {
                uint8_t b = 0;
                off_t pos = st.st_size / 2;
                if (pread(fd, &b, 1, pos) == 1) {
                    b ^= 0xff;
                    if (pwrite(fd, &b, 1, pos) == 1 && fsync(fd) == 0)
                        done++;
                }
                close(fd);
            }
        }
    }
    closedir(d);
    return done;
}

static uint64_t le64_rd(const uint8_t *b)
{
    uint64_t v = 0;
    for (int i = 7; i >= 0; i--)
        v = (v << 8) | b[i];
    return v;
}
static void le64_wr(uint8_t *b, uint64_t v)
{
    for (int i = 0; i < 8; i++)
        b[i] = (uint8_t)(v >> (8 * i));
}

/* Tampers object_xxh64 in an .ec file and re-seals the trailer hashes so part_open_ec opens it cleanly. */
static void tamper_ec_object_hash(const char *dbdir, const char *ec_name)
{
    char p[PATH_MAX + 256];
    snprintf(p, sizeof p, "%s/%s", dbdir, ec_name);
    int fd = open(p, O_RDWR);
    CHECK(fd >= 0);
    if (fd < 0)
        return;
    struct stat st;
    CHECK(fstat(fd, &st) == 0 && st.st_size > EC_TRAILER_BYTES + EC_REC_FIXED);
    uint8_t *buf = malloc((size_t)st.st_size);
    CHECK(buf != NULL && pread(fd, buf, (size_t)st.st_size, 0) == st.st_size);
    uint8_t *tr = buf + st.st_size - EC_TRAILER_BYTES;
    uint64_t roff = le64_rd(tr + EC_TR_REC_OFF);
    uint64_t rlen = le64_rd(tr + EC_TR_REC_LEN);
    uint8_t *rec = buf + roff;
    uint64_t oldh = le64_rd(rec + EC_REC_OBJ_HASH);
    le64_wr(rec + EC_REC_OBJ_HASH, oldh ^ 0xdeadbeefULL);
    le64_wr(tr + EC_TR_REC_HASH, xxh64(rec, (size_t)rlen, 0));
    le64_wr(tr + EC_TR_HASH, xxh64(tr, EC_TR_HASH, 0));
    CHECK(pwrite(fd, buf, (size_t)st.st_size, 0) == st.st_size && fsync(fd) == 0);
    free(buf);
    close(fd);
}

static void case_decode_checks(void)
{
    /* Subcase A: corrupt 1 chunk on one store (recoverable via parity with m=1). Normal code checks cell sums,
     * detects bad chunk, rebuilds from parity, passes digest & granule check. unsettlenodecodecheck uses RAW
     * and writes corrupt bytes into P.part! */
    char base[PATH_MAX], dbdir[PATH_MAX], stores[3][PATH_MAX];
    fresh("decode_chk_a", base, sizeof base);
    thandle h;
    make_tiered_db(base, dbdir, sizeof dbdir, stores, &h);
    eng *db = h.db;
    int64_t key = T0 / DAY;
    uint32_t s = 0;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    write_f32(db, s, T0, 4000, 1000, 3.5f);
    CHECK(eng_flush(db) == 0);
    acc oracle = read_f32(db, s, INT64_MIN, INT64_MAX);
    CHECK(settle_key(db, key) == 0);
    part_sig sigs[4];
    size_t ns = snap_key(db, key, sigs, 4);
    CHECK(ns == 1);
    char snm[128];
    snprintf(snm, sizeof snm, "settled_%.95s.part", sigs[0].stem);
    CHECK(unlinkat(db->dirfd, snm, 0) == 0);
    int corrupted = 0;
    for (int i = 0; i < 3 && !corrupted; i++)
        corrupted = corrupt_chunks_in(stores[i], false);
    CHECK(corrupted == 1);
    CHECK(eng_unsettle(db, key) == 0);
    /* Force checksum verification on reads and compare every row against oracle */
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++)
        db->parts[i]->verify = true;
    pthread_mutex_unlock(&db->lock);
    acc got = read_f32(db, s, INT64_MIN, INT64_MAX);
    CHECK(got.n == oracle.n && got.sum == oracle.sum && got.hash == oracle.hash);
    close_tiered(&h);

    /* Subcase B: tamper object_xxh64 in the .ec file -> eng_unsettle MUST refuse with TSDB_ERR_CORRUPT! */
    fresh("decode_chk_b", base, sizeof base);
    make_tiered_db(base, dbdir, sizeof dbdir, stores, &h);
    db = h.db;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    write_f32(db, s, T0, 2000, 1000, 7.0f);
    CHECK(eng_flush(db) == 0);
    CHECK(settle_key(db, key) == 0);
    ns = snap_key(db, key, sigs, 4);
    CHECK(ns == 1);
    snprintf(snm, sizeof snm, "settled_%.95s.part", sigs[0].stem);
    CHECK(unlinkat(db->dirfd, snm, 0) == 0);
    char ecnm[128];
    snprintf(ecnm, sizeof ecnm, "%.95s.ec", sigs[0].stem);
    close_tiered(&h);
    tamper_ec_object_hash(dbdir, ecnm);
    open_tiered_quiet(dbdir, &h);
    db = h.db;
    int rc = eng_unsettle(db, key);
    CHECK(rc == TSDB_ERR_CORRUPT);
    CHECK(exists(dbdir, ecnm));
    CHECK(count_names(dbdir, "tmp_", ".part") == 0 && count_names(dbdir, "txn_", ".intent") == 0);
    close_tiered(&h);
    printf("  decode verification: parity rebuild on corrupted chunk ok; mismatched object_xxh64 refused\n");
}

/* ------------------------------------------------------------ 5. oracle twins */

static void case_oracle_twins(void)
{
    char baseA[PATH_MAX], baseB[PATH_MAX], dbB[PATH_MAX], stores[3][PATH_MAX];
    fresh("twinA", baseA, sizeof baseA);
    fresh("twinB", baseB, sizeof baseB);

    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.partition_classes = 0;
    tsdb *tA = NULL;
    eng *eA = NULL;
    CHECK(tsdb_open(baseA, &o, &tA) == 0);
    eA = tsdb_test_engine(tA, 0);
    thandle hB;
    make_tiered_db(baseB, dbB, sizeof dbB, stores, &hB);
    eng *eB = hB.db;

    uint32_t sfA = 0, siA = 0, sfB = 0, siB = 0;
    CHECK(eng_series(eA, "sf", TSDB_FLOAT32, &sfA) == 0 && eng_series(eA, "si", TSDB_INT64, &siA) == 0);
    CHECK(eng_series(eB, "sf", TSDB_FLOAT32, &sfB) == 0 && eng_series(eB, "si", TSDB_INT64, &siB) == 0);

    /* Populate 3 keys (k0, k1, k2) identically on A and B, with overlapping timestamps and tombstones! */
    int64_t k0 = T0 / DAY, k1 = k0 + 1, k2 = k0 + 2;
    eng *twins[2] = {eA, eB};
    uint32_t sfs[2] = {sfA, sfB}, sis[2] = {siA, siB};
    for (int w = 0; w < 2; w++) {
        for (int64_t k = k0; k <= k2; k++) {
            write_f32(twins[w], sfs[w], k * DAY, 1200, 1000, (float)(k * 10));
            write_i64(twins[w], sis[w], k * DAY + 500, 800, 2000, k * 100);
        }
        CHECK(eng_flush(twins[w]) == 0);
        for (int64_t k = k0; k <= k2; k++) {
            write_f32(twins[w], sfs[w], k * DAY + 400 * 1000, 1000, 1000, (float)(k * 10 + 77));
            write_i64(twins[w], sis[w], k * DAY + 300 * 1000, 600, 2000, k * 100 + 555);
        }
        CHECK(eng_flush(twins[w]) == 0);
        /* Delete a range inside k0 and k1 so tombstones are settled and unsettled too! */
        CHECK(eng_delete_range(twins[w], sfs[w], k0 * DAY + 100 * 1000, k0 * DAY + 500 * 1000) == 0);
        CHECK(eng_delete_range(twins[w], sis[w], k1 * DAY + 200 * 1000, k1 * DAY + 700 * 1000) == 0);
        CHECK(eng_flush(twins[w]) == 0);
    }

    /* Settle k0 and k1 on B; remove settled_ on k1 so k0 uses fast path and k1 uses decode path. */
    CHECK(settle_key(eB, k0) == 0);
    CHECK(settle_key(eB, k1) == 0);
    part_sig k1s[8];
    size_t nk1 = snap_key(eB, k1, k1s, 8);
    CHECK(nk1 >= 2);
    size_t n_unlinked = 0;
    for (size_t i = 0; i < nk1; i++) {
        if (!k1s[i].is_ec)
            continue;
        char snm[128];
        snprintf(snm, sizeof snm, "settled_%.95s.part", k1s[i].stem);
        CHECK(unlinkat(eB->dirfd, snm, 0) == 0);
        n_unlinked++;
    }
    CHECK(n_unlinked >= 2);

    /* Compare before unsettle */
    acc fA = read_f32(eA, sfA, INT64_MIN, INT64_MAX), fB = read_f32(eB, sfB, INT64_MIN, INT64_MAX);
    acc iA = read_i64(eA, siA, INT64_MIN, INT64_MAX), iB = read_i64(eB, siB, INT64_MIN, INT64_MAX);
    CHECK(fA.n == fB.n && fA.sum == fB.sum && fA.hash == fB.hash);
    CHECK(iA.n == iB.n && iA.sum == iB.sum && iA.hash == iB.hash);

    /* Unsettle both k0 (settled_ fast path) and k1 (decode path) on B */
    CHECK(eng_unsettle(eB, k0) == 0);
    CHECK(eng_unsettle(eB, k1) == 0);

    fB = read_f32(eB, sfB, INT64_MIN, INT64_MAX);
    iB = read_i64(eB, siB, INT64_MIN, INT64_MAX);
    CHECK(fA.n == fB.n && fA.sum == fB.sum && fA.hash == fB.hash);
    CHECK(iA.n == iB.n && iA.sum == iB.sum && iA.hash == iB.hash);

    uint32_t idsA[2] = {sfA, siA}, idsB[2] = {sfB, siB};
    latest_row lA[2], lB[2];
    CHECK(eng_latest(eA, idsA, 2, NULL, INT64_MAX, false, lA) == 0);
    CHECK(eng_latest(eB, idsB, 2, NULL, INT64_MAX, false, lB) == 0);
    for (int i = 0; i < 2; i++)
        CHECK(lA[i].status == lB[i].status && lA[i].ts == lB[i].ts && lA[i].slot == lB[i].slot);

    /* Reopen B and compare again */
    close_tiered(&hB);
    open_tiered_quiet(dbB, &hB);
    eB = hB.db;
    fB = read_f32(eB, sfB, INT64_MIN, INT64_MAX);
    iB = read_i64(eB, siB, INT64_MIN, INT64_MAX);
    CHECK(fA.n == fB.n && fA.sum == fB.sum && fA.hash == fB.hash);
    CHECK(iA.n == iB.n && iA.sum == iB.sum && iA.hash == iB.hash);
    close_tiered(&hB);
    CHECK(tsdb_close(tA) == 0);
    printf("  oracle twins: float32 + int64 + tombstones bit-for-bit identical before/after unsettle & reopen\n");
}

/* ------------------------------------------------------------ 6. hand-planted v5 unsettle recovery */

static int open_plain(const char *dir, eng **db)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.partition_classes = 0;
    return eng_open(dir, &o, db);
}

static int plant_open(const char *tag, const char *const *files, const char *body, bool seal, char *dir, size_t cap)
{
    fresh(tag, dir, cap);
    eng *db = NULL;
    CHECK(open_plain(dir, &db) == 0 && db);
    if (db)
        CHECK(eng_close(db) == 0);
    for (size_t i = 0; files && files[i]; i++) {
        char p[PATH_MAX + 256];
        snprintf(p, sizeof p, "%s/%s", dir, files[i]);
        int fd = open(p, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        CHECK(fd >= 0);
        if (fd >= 0) {
            CHECK(write(fd, "x", 1) == 1);
            close(fd);
        }
    }
    char ip[PATH_MAX + 256];
    snprintf(ip, sizeof ip, "%s/txn_7_1.intent", dir);
    int fd = open(ip, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    CHECK(fd >= 0);
    if (fd >= 0) {
        size_t len = strlen(body);
        CHECK(write(fd, body, len) == (ssize_t)len);
        if (seal) {
            char end[32];
            int n = snprintf(end, sizeof end, "end %016" PRIx64 "\n", xxh64(body, len, 0));
            CHECK(write(fd, end, (size_t)n) == (ssize_t)n);
        } else {
            const char *bad = "end 0000000000000000\n";
            CHECK(write(fd, bad, strlen(bad)) == (ssize_t)strlen(bad));
        }
        close(fd);
    }
    db = NULL;
    int rc = open_plain(dir, &db);
    if (db)
        CHECK(eng_close(db) == 0);
    return rc;
}

static const char *const UN5_SET =
    "tsdb-intent 5 4\n"
    "L settled_p500_1_1_1.part p500_1_1_1.part\n"
    "L settled_p500_2_2_1.part p500_2_2_1.part\n"
    "U p500_1_1_1.ec retired_p500_1_1_1.ec\n"
    "U p500_2_2_1.ec retired_p500_2_2_1.ec\n";

static const char *const UN5_TMP =
    "tsdb-intent 5 4\n"
    "L tmp_7_1.part p500_1_1_1.part\n"
    "L tmp_7_2.part p500_2_2_1.part\n"
    "U p500_1_1_1.ec retired_p500_1_1_1.ec\n"
    "U p500_2_2_1.ec retired_p500_2_2_1.ec\n";

static void case_recover_hand(void)
{
    char dir[PATH_MAX];
    /* 1. Torn intent (settled_ source): rolled back, settled_ KEPT, .ec untouched */
    plant_open("un-torn-set", (const char *const[]){"settled_p500_1_1_1.part", "p500_1_1_1.ec", NULL}, UN5_SET, false,
               dir, sizeof dir);
    CHECK(!exists(dir, "txn_7_1.intent") && exists(dir, "settled_p500_1_1_1.part") && exists(dir, "p500_1_1_1.ec") &&
          !exists(dir, "p500_1_1_1.part") && !exists(dir, "retired_p500_1_1_1.ec"));

    /* 2. Torn intent (tmp_ source): rolled back, tmp_ cleaned */
    plant_open("un-torn-tmp", (const char *const[]){"tmp_7_1.part", "tmp_7_2.part", NULL}, UN5_TMP, false, dir,
               sizeof dir);
    CHECK(!exists(dir, "txn_7_1.intent") && !exists(dir, "tmp_7_1.part") && !exists(dir, "tmp_7_2.part"));

    /* 3. Complete, nothing renamed, all present -> roll forward (settled_ path) */
    plant_open("un-fwd-set",
               (const char *const[]){"settled_p500_1_1_1.part", "settled_p500_2_2_1.part", "p500_1_1_1.ec",
                                     "p500_2_2_1.ec", NULL},
               UN5_SET, true, dir, sizeof dir);
    CHECK(!exists(dir, "txn_7_1.intent") && exists(dir, "p500_1_1_1.part") && exists(dir, "p500_2_2_1.part") &&
          exists(dir, "retired_p500_1_1_1.ec") && exists(dir, "retired_p500_2_2_1.ec") &&
          !exists(dir, "settled_p500_1_1_1.part") && !exists(dir, "p500_1_1_1.ec"));

    /* 4. Complete, half L renamed -> roll forward (tmp_ path) */
    plant_open("un-half-l",
               (const char *const[]){"p500_1_1_1.part", "tmp_7_2.part", "p500_1_1_1.ec", "p500_2_2_1.ec", NULL},
               UN5_TMP, true, dir, sizeof dir);
    CHECK(!exists(dir, "txn_7_1.intent") && exists(dir, "p500_1_1_1.part") && exists(dir, "p500_2_2_1.part") &&
          exists(dir, "retired_p500_1_1_1.ec") && exists(dir, "retired_p500_2_2_1.ec") &&
          !exists(dir, "tmp_7_2.part") && !exists(dir, "p500_1_1_1.ec") && !exists(dir, "p500_2_2_1.ec"));

    /* 5. Complete, all L renamed, half U renamed -> roll forward */
    plant_open("un-half-u",
               (const char *const[]){"p500_1_1_1.part", "p500_2_2_1.part", "retired_p500_1_1_1.ec", "p500_2_2_1.ec",
                                     NULL},
               UN5_TMP, true, dir, sizeof dir);
    CHECK(!exists(dir, "txn_7_1.intent") && exists(dir, "p500_1_1_1.part") && exists(dir, "p500_2_2_1.part") &&
          exists(dir, "retired_p500_1_1_1.ec") && exists(dir, "retired_p500_2_2_1.ec") &&
          !exists(dir, "p500_2_2_1.ec"));

    /* 6. U persisted before L (crashfs reordered U before L before post-rename dir fsync): rolls forward! */
    plant_open("un-ureorder-tmp",
               (const char *const[]){"tmp_7_1.part", "tmp_7_2.part", "retired_p500_1_1_1.ec", "retired_p500_2_2_1.ec",
                                     NULL},
               UN5_TMP, true, dir, sizeof dir);
    CHECK(!exists(dir, "txn_7_1.intent") && !exists(dir, "tmp_7_1.part") && !exists(dir, "tmp_7_2.part") &&
          exists(dir, "p500_1_1_1.part") && exists(dir, "p500_2_2_1.part") && exists(dir, "retired_p500_1_1_1.ec"));

    plant_open("un-ureorder-set",
               (const char *const[]){"settled_p500_1_1_1.part", "settled_p500_2_2_1.part", "retired_p500_1_1_1.ec",
                                     "retired_p500_2_2_1.ec", NULL},
               UN5_SET, true, dir, sizeof dir);
    CHECK(!exists(dir, "txn_7_1.intent") && !exists(dir, "settled_p500_1_1_1.part") &&
          exists(dir, "p500_1_1_1.part") && exists(dir, "p500_2_2_1.part"));

    /* 6b. Missing L source while P.ec still live -> rolls back: tmp_ unlinked, settled_ KEPT, P.ec untouched */
    plant_open("un-miss-tmp", (const char *const[]){"tmp_7_1.part", "p500_1_1_1.ec", "p500_2_2_1.ec", NULL}, UN5_TMP,
               true, dir, sizeof dir);
    CHECK(!exists(dir, "txn_7_1.intent") && !exists(dir, "tmp_7_1.part") && exists(dir, "p500_1_1_1.ec") &&
          exists(dir, "p500_2_2_1.ec") && !exists(dir, "p500_1_1_1.part"));

    plant_open("un-miss-set",
               (const char *const[]){"settled_p500_1_1_1.part", "p500_1_1_1.ec", "p500_2_2_1.ec", NULL}, UN5_SET, true,
               dir, sizeof dir);
    CHECK(!exists(dir, "txn_7_1.intent") && exists(dir, "settled_p500_1_1_1.part") && exists(dir, "p500_1_1_1.ec") &&
          exists(dir, "p500_2_2_1.ec") && !exists(dir, "p500_1_1_1.part"));

    /* 6c. Stale leftover retired_P.part beside live P.ec + missing L source -> NOT treated as renamed; rolls back! */
    plant_open("un-staleret",
               (const char *const[]){"retired_p500_1_1_1.part", "settled_p500_1_1_1.part", "p500_1_1_1.ec",
                                     "p500_2_2_1.ec", NULL},
               UN5_SET, true, dir, sizeof dir);
    CHECK(!exists(dir, "txn_7_1.intent") && exists(dir, "settled_p500_1_1_1.part") && exists(dir, "p500_1_1_1.ec") &&
          exists(dir, "p500_2_2_1.ec") && !exists(dir, "p500_1_1_1.part"));

    /* 6d. Drop intent naming P.ec + P2.part when P.ec was unsettled (retired_P.ec + P.part on disk):
     * drop_name_recover must retire P.part too, never leaving a half-dropped partition! */
    const char *drop_body = "tsdb-drop 1 2\np500_1_1_1.ec\np500_2_2_0.part\n";
    plant_open("un-drop-unsettled",
               (const char *const[]){"retired_p500_1_1_1.ec", "p500_1_1_1.part", "p500_2_2_0.part", NULL}, drop_body,
               true, dir, sizeof dir);
    CHECK(!exists(dir, "txn_7_1.intent") && !exists(dir, "p500_1_1_1.part") && !exists(dir, "p500_2_2_0.part") &&
          exists(dir, "retired_p500_1_1_1.part") && exists(dir, "retired_p500_2_2_0.part"));

    /* 6e. Drop intent naming P.ec when retired_P.ec was already GC'd and only P.part remains on disk:
     * drop_name_recover's second branch must retire P.part too! */
    plant_open("un-drop-unsettled-clean", (const char *const[]){"p500_1_1_1.part", "p500_2_2_0.part", NULL}, drop_body,
               true, dir, sizeof dir);
    CHECK(!exists(dir, "txn_7_1.intent") && !exists(dir, "p500_1_1_1.part") && !exists(dir, "p500_2_2_0.part") &&
          exists(dir, "retired_p500_1_1_1.part") && exists(dir, "retired_p500_2_2_0.part"));

    /* 7. Drop after swap (retired_P.part + retired_P.ec) -> forward no-op */
    plant_open("un-drop-after",
               (const char *const[]){"retired_p500_1_1_1.part", "retired_p500_2_2_1.part", "retired_p500_1_1_1.ec",
                                     "retired_p500_2_2_1.ec", NULL},
               UN5_TMP, true, dir, sizeof dir);
    CHECK(!exists(dir, "txn_7_1.intent") && !exists(dir, "p500_1_1_1.part") && exists(dir, "retired_p500_1_1_1.part"));

    /* 8. Damage: L[0] renamed, L[1] under none of its names while U[1] (.ec) still live -> TSDB_ERR_CORRUPT */
    int rc = plant_open("un-damage", (const char *const[]){"p500_1_1_1.part", "p500_1_1_1.ec", "p500_2_2_1.ec", NULL},
                        UN5_TMP, true, dir, sizeof dir);
    CHECK(rc == TSDB_ERR_CORRUPT && exists(dir, "txn_7_1.intent"));

    /* 9. Crafted invalid intents: odd line count, mixed N/U, two keys, bad L.from, wrong U.to, stem mismatch */
    static const char *const bad[] = {
        "tsdb-intent 5 1\nL tmp_7_1.part p500_1_1_1.part\n",
        "tsdb-intent 5 2\nN tmp_7_1.ec p500_1_1_1.ec\nU p500_1_1_1.ec retired_p500_1_1_1.ec\n",
        "tsdb-intent 5 2\nL tmp_7_1.part p500_1_1_1.part\nU p501_1_1_1.ec retired_p501_1_1_1.ec\n",
        "tsdb-intent 5 2\nL evil.part p500_1_1_1.part\nU p500_1_1_1.ec retired_p500_1_1_1.ec\n",
        "tsdb-intent 5 2\nL tmp_7_1.part p500_1_1_1.part\nU p500_1_1_1.ec settled_p500_1_1_1.ec\n",
        "tsdb-intent 5 2\nL settled_p500_2_2_1.part p500_1_1_1.part\nU p500_1_1_1.ec retired_p500_1_1_1.ec\n",
    };
    for (size_t i = 0; i < sizeof bad / sizeof *bad; i++) {
        rc = plant_open("un-bad", (const char *const[]){"tmp_7_1.part", "p500_1_1_1.ec", NULL}, bad[i], true, dir,
                        sizeof dir);
        CHECK(rc == TSDB_ERR_CORRUPT && exists(dir, "txn_7_1.intent"));
    }
    if (ecunsettle_mut() != ECUNSETTLE_MUT_RANK) {
        const char *bad_stem = "tsdb-intent 5 2\nL tmp_7_1.part p500_9_9_1.part\nU p500_1_1_1.ec retired_p500_1_1_1.ec\n";
        rc = plant_open("un-badstem", (const char *const[]){"tmp_7_1.part", "p500_1_1_1.ec", NULL}, bad_stem, true, dir,
                        sizeof dir);
        CHECK(rc == TSDB_ERR_CORRUPT && exists(dir, "txn_7_1.intent"));
    }
    printf("  v5 unsettle recovery: torn, forward, half-L, half-U, drop before/after, damage & crafted intents ok\n");
}

/* ------------------------------------------------------------ 7. concurrent drop */

static int64_t drop_key_ts;
static void drop_hook(eng *db) { CHECK(eng_drop_before(db, drop_key_ts) == 0); }

static void case_concurrent_drop(void)
{
    char base[PATH_MAX], dbdir[PATH_MAX], stores[3][PATH_MAX];
    fresh("conc_drop", base, sizeof base);
    thandle h;
    make_tiered_db(base, dbdir, sizeof dbdir, stores, &h);
    eng *db = h.db;
    int64_t key = T0 / DAY;
    uint32_t s = 0;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    write_f32(db, s, T0, 1500, 1000, 1.0f);
    write_f32(db, s, T0 + DAY, 1500, 1000, 2.0f);
    CHECK(eng_flush(db) == 0);
    CHECK(settle_key(db, key) == 0);
    part_sig sigs[4];
    size_t ns = snap_key(db, key, sigs, 4);
    CHECK(ns == 1);
    /* Force decode path so tmp_*.part cleanup is also tested on STOPPED */
    char snm[128];
    snprintf(snm, sizeof snm, "settled_%.95s.part", sigs[0].stem);
    CHECK(unlinkat(db->dirfd, snm, 0) == 0);

    drop_key_ts = T0 + DAY; /* drops partition key T0/DAY */
    ec_swap_hook = drop_hook;
    int rc = eng_unsettle(db, key);
    ec_swap_hook = NULL;
    CHECK(rc == TSDB_ERR_STOPPED && eng_check(db) == 0);
    CHECK(read_f32(db, s, T0, T0 + DAY).n == 0);
    CHECK(read_f32(db, s, T0 + DAY, INT64_MAX).n == 1500);
    CHECK(count_names(dbdir, "tmp_", ".part") == 0 && count_names(dbdir, "txn_", ".intent") == 0);
    close_tiered(&h);
    printf("  concurrent drop: drop in hook -> TSDB_ERR_STOPPED, tmp cleaned, key stays dropped\n");
}

/* ------------------------------------------------------------ 7b. settle -> unsettle -> re-settle & refusals */

static void drain_gc(thandle *h)
{
    graveyard_purge(h->db, true);
    for (int i = 0; i < 64 && ecgc_tier_work(h->t) > 0; i++)
        ;
    graveyard_purge(h->db, true);
}

static void case_resettle_and_refusals(void)
{
    char base[PATH_MAX], dbdir[PATH_MAX], stores[3][PATH_MAX];
    fresh("resettle_keep", base, sizeof base);
    thandle h;
    make_tiered_db(base, dbdir, sizeof dbdir, stores, &h);
    eng *db = h.db;
    int64_t key = T0 / DAY;
    uint32_t s = 0;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    write_f32(db, s, T0, 1500, 1000, 5.0f);
    CHECK(eng_flush(db) == 0);
    acc oracle = read_f32(db, s, INT64_MIN, INT64_MAX);
    close_tiered(&h);

    /* Open with eckeep active and manual GC (stop only settle thread so drain_gc can run ecgc_tier_work). */
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.partition_classes = 0;
    atomic_store(&ecgc_test_manual, true);
    CHECK(tier_open(dbdir, &o, &h.t) == 0 && h.t != NULL);
    atomic_store(&ecgc_test_manual, false);
    ecsettle_tier_quiesce(h.t);
    tier_want(h.t, 0);
    CHECK(eng_open(dbdir, &o, &h.db) == 0 && h.db != NULL);
    tier_want(NULL, 0);
    db = h.db;

    CHECK(settle_key(db, key) == 0);
    part_sig sigs[4];
    size_t ns = snap_key(db, key, sigs, 4);
    CHECK(ns == 1 && sigs[0].is_ec);
    char snm[128], rnm[128];
    snprintf(snm, sizeof snm, "settled_%.95s.part", sigs[0].stem);
    snprintf(rnm, sizeof rnm, "retired_%.95s.ec", sigs[0].stem);
    CHECK(eckeep_test_wait(db, snm, 10000) == ECKEEP_MATCHED);

    /* Refusals: busy (merging / settling), cluster, replicated, read-only */
    part *ecp = NULL;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++)
        if (db->parts[i]->partition == key && atomic_load(&db->parts[i]->ec))
            ecp = db->parts[i];
    CHECK(ecp != NULL);
    if (ecp)
        ecp->merging = true;
    pthread_mutex_unlock(&db->lock);
    CHECK(eng_unsettle(db, key) == TSDB_ERR_LOCKED);
    pthread_mutex_lock(&db->lock);
    if (ecp)
        ecp->merging = false, ecp->settling = true;
    pthread_mutex_unlock(&db->lock);
    CHECK(eng_unsettle(db, key) == TSDB_ERR_LOCKED);
    pthread_mutex_lock(&db->lock);
    if (ecp)
        ecp->settling = false;
    pthread_mutex_unlock(&db->lock);

    db->ec_cluster = true;
    CHECK(eng_unsettle(db, key) == TSDB_ERR_INVALID);
    db->ec_cluster = false;
    db->replica_req = true;
    CHECK(eng_unsettle(db, key) == TSDB_ERR_INVALID);
    db->replica_req = false;
    db->opt.read_only = 1;
    CHECK(eng_unsettle(db, key) == TSDB_ERR_READONLY);
    db->opt.read_only = 0;

    /* Fast-path unsettle must drop settled_'s eckeep & graveyard entry, and leave retired_P.ec awaiting GC. */
    CHECK(eng_unsettle(db, key) == 0);
    CHECK(eckeep_status(db, snm) == TSDB_ERR_NOTFOUND);
    CHECK(exists(dbdir, rnm));

    /* While retired_P.ec still exists, re-settling P.part into P.ec MUST be deferred (TSDB_ERR_STOPPED) so
     * ecswap_recover cannot mistake the older retired_P.ec for a renamed settle and GC records never collide. */
    if (ecunsettle_mut() != ECUNSETTLE_MUT_RANK) {
        CHECK(settle_key(db, key) == TSDB_ERR_STOPPED);
        CHECK(exists(dbdir, rnm) && count_names(dbdir, "p", ".ec") == 0);
    }

    /* Once GC deletes the retired object's chunks and unlinks retired_P.ec, re-settle succeeds with fresh keep state! */
    drain_gc(&h);
    CHECK(!exists(dbdir, rnm));

    atomic_store(&eckeep_test_pause, true);
    CHECK(settle_key(db, key) == 0);
    CHECK(exists(dbdir, snm));
    CHECK(eckeep_status(db, snm) == ECKEEP_PENDING);
    graveyard_purge(db, true);
    CHECK(exists(dbdir, snm));
    CHECK(eckeep_status(db, snm) == ECKEEP_PENDING);
    atomic_store(&eckeep_test_pause, false);
    CHECK(eckeep_test_wait(db, snm, 10000) == ECKEEP_MATCHED);

    /* eng_unsettle must refuse (TSDB_ERR_LOCKED) if a retired_P.ec exists on disk beside P.ec. */
    {
        char rp[PATH_MAX + 128];
        snprintf(rp, sizeof rp, "%s/%s", dbdir, rnm);
        int fd = open(rp, O_WRONLY | O_CREAT | O_EXCL, 0644);
        CHECK(fd >= 0);
        if (fd >= 0)
            close(fd);
        if (ecunsettle_mut() != ECUNSETTLE_MUT_RANK)
            CHECK(eng_unsettle(db, key) == TSDB_ERR_LOCKED);
        CHECK(unlink(rp) == 0);
    }

    /* Second unsettle via decode path (flip settled_P.part -> ECKEEP_ROT): must clear eckeep state AND unlink the
     * corrupt settled_P.part so a subsequent re-settle starts fresh with ECKEEP_PENDING! */
    flip_byte_in_file(dbdir, snm, (off_t)sizeof(part_header) + 16);
    CHECK(eng_unsettle(db, key) == 0);
    CHECK(eckeep_status(db, snm) == TSDB_ERR_NOTFOUND);
    CHECK(!exists(dbdir, snm));
    acc got = read_f32(db, s, INT64_MIN, INT64_MAX);
    CHECK(got.n == oracle.n && got.sum == oracle.sum && got.hash == oracle.hash);

    /* Drain GC and re-settle a third time to confirm the decode path did not poison eckeep state. */
    drain_gc(&h);
    atomic_store(&eckeep_test_pause, true);
    CHECK(settle_key(db, key) == 0);
    CHECK(exists(dbdir, snm) && eckeep_status(db, snm) == ECKEEP_PENDING);
    graveyard_purge(db, true);
    CHECK(exists(dbdir, snm) && eckeep_status(db, snm) == ECKEEP_PENDING);
    atomic_store(&eckeep_test_pause, false);
    CHECK(eckeep_test_wait(db, snm, 10000) == ECKEEP_MATCHED);
    CHECK(eng_unsettle(db, key) == 0);
    got = read_f32(db, s, INT64_MIN, INT64_MAX);
    CHECK(got.n == oracle.n && got.sum == oracle.sum && got.hash == oracle.hash);

    close_tiered(&h);
    printf("  resettle & refusals: retired_.ec guard, settle->unsettle->settle->unsettle, eckeep cleanup & refusals ok\n");
}

/* ------------------------------------------------------------ 7c. TTL retention drop vs unsettle */

static bool ttl_hook_ran, ttl_hook_dropped;

static void ttl_drop_hook(eng *db)
{
    part *sel[MAX_MERGE_PARTS];
    bool dropped = false, sync_wal = false;
    char drop_intent[64] = "";
    pthread_mutex_lock(&db->lock);
    db->last_ttl_ms = 0;
    (void)ttl_select_locked(db, sel, &dropped, &sync_wal, drop_intent);
    pthread_mutex_unlock(&db->lock);
    ttl_hook_ran = true;
    ttl_hook_dropped = dropped;
    if (dropped) {
        (void)eng_sync_dir(db, "test ttl drop hook");
        drop_intent_done(db, drop_intent);
    }
}

static void case_ttl_drop_race(void)
{
    char base[PATH_MAX], dbdir[PATH_MAX], stores[3][PATH_MAX];
    fresh("ttl_race", base, sizeof base);
    thandle h;
    make_tiered_db(base, dbdir, sizeof dbdir, stores, &h);
    eng *db = h.db;
    int64_t key = T0 / DAY;
    uint32_t s = 0;
    CHECK(eng_series(db, "s", TSDB_FLOAT32, &s) == 0);
    write_f32(db, s, T0, 1500, 1000, 3.0f);
    CHECK(eng_flush(db) == 0);
    CHECK(settle_key(db, key) == 0);
    /* Set 1 s retention: T0 (1970) is immediately expired, so ttl_select_locked would drop the key if not blocked! */
    CHECK(eng_set_retention(db, s, 1000) == 0);

    ttl_hook_ran = ttl_hook_dropped = false;
    ec_swap_hook = ttl_drop_hook;
    int rc = eng_unsettle(db, key);
    ec_swap_hook = NULL;
    CHECK(rc == 0 && ttl_hook_ran && !ttl_hook_dropped);
    CHECK(count_names(dbdir, "p", ".ec") == 0 && count_names(dbdir, "p", ".part") == 1);

    /* Once unsettle finished (p->settling cleared), ttl_select_locked CAN drop the expired partition cleanly. */
    ttl_hook_ran = ttl_hook_dropped = false;
    ttl_drop_hook(db);
    CHECK(ttl_hook_ran && ttl_hook_dropped);
    CHECK(count_names(dbdir, "p", ".part") == 0);
    close_tiered(&h);
    printf("  TTL retention drop race: ttl_select_locked skips unsettled .ec partition, drops cleanly after\n");
}

/* ------------------------------------------------------------ 8. crashfs power-loss sweep */

static int child_unsettle(const char *mode, const char *base)
{
    char dbdir[PATH_MAX + 16];
    snprintf(dbdir, sizeof dbdir, "%s/db", base);
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.partition_classes = 0;
    tier *t = NULL;
    if (tier_open(dbdir, &o, &t) != 0 || !t)
        return 10;
    tier_quiesce(t);
    eng *db = NULL;
    tier_want(t, 0);
    int orc = eng_open(dbdir, &o, &db);
    tier_want(NULL, 0);
    if (orc != 0 || !db) {
        tier_put(t);
        return 11;
    }
    eckeep_eng_stop(db);
    (void)mode;
    int rc = eng_unsettle(db, T0 / DAY);
    int crc = eng_close(db);
    tier_put(t);
    return rc == 0 && crc == 0 ? 0 : 12;
}

static int run_crashfs_child(const char *mode, const char *base, int warmup, unsigned seed)
{
    pid_t pid = fork();
    if (pid == 0) {
        char dbdir[PATH_MAX + 96], wbuf[32], sbuf[32], asan[512];
        snprintf(dbdir, sizeof dbdir, "%s/db", base);
        snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0:detect_leaks=0",
                 getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
        setenv("ASAN_OPTIONS", asan, 1);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", dbdir, 1);
        setenv("CRASHFS_PROB", "1", 1);
        snprintf(wbuf, sizeof wbuf, "%d", warmup);
        setenv("CRASHFS_WARMUP", wbuf, 1);
        setenv("CRASHFS_AT_EXIT", "1", 1);
        setenv("CRASHFS_TEAR", "1", 1);
        snprintf(sbuf, sizeof sbuf, "%u", seed);
        setenv("CRASHFS_SEED", sbuf, 1);
        int null = open("/dev/null", O_WRONLY);
        if (null >= 0) {
            dup2(null, 1);
            dup2(null, 2);
            close(null);
        }
        const char *mname = ecunsettle_mut() == ECUNSETTLE_MUT_RANK
                                ? "unsettlerank"
                                : ecunsettle_mut() == ECUNSETTLE_MUT_NODECODECHECK ? "unsettlenodecodecheck" : "none";
        execl(SELF, SELF, "--mutant", mname, "--child", mode, base, (char *)NULL);
        _exit(127);
    }
    int st = 0;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

static void power_sweep(const char *mode, bool remove_settled)
{
    char src[PATH_MAX], dbdir[PATH_MAX], stores[3][PATH_MAX];
    char tag[64];
    snprintf(tag, sizeof tag, "pwr_base_%s", mode);
    fresh(tag, src, sizeof src);
    thandle h;
    make_tiered_db(src, dbdir, sizeof dbdir, stores, &h);
    eng *db = h.db;
    int64_t key = T0 / DAY;
    uint32_t s = 0;
    acc oracle = {0};
    part_sig ec_sigs[8], all_sigs[8];
    size_t nec = 0, nall = 0;
    setup_lww_settled(db, key, &s, &oracle, ec_sigs, &nec, all_sigs, &nall);
    if (remove_settled) {
        for (size_t i = 0; i < nec; i++) {
            char snm[128];
            snprintf(snm, sizeof snm, "settled_%.95s.part", ec_sigs[i].stem);
            CHECK(unlinkat(db->dirfd, snm, 0) == 0);
        }
    }
    close_tiered(&h);

    int crashes = 0, pre = 0, post = 0;
    int seeds = quick ? 2 : 4;
    bool finished = false;
    char work[PATH_MAX + 64], wdb[PATH_MAX + 80];
    snprintf(work, sizeof work, "%s/pwr_work_%s", S, mode);
    snprintf(wdb, sizeof wdb, "%s/db", work);

    /* Keep TIERS store paths pointing at src/st0..2 (stores are read-only during unsettle!), only copy src/db! */
    for (int k = 0; k < 2000 && !finished; k++) {
        for (int sd = 0; sd < seeds; sd++) {
            sh("rm -rf '%s'; mkdir -p '%s'; cp -a '%s/db' '%s/db'", work, work, src, work);
            int code = run_crashfs_child(mode, work, k, (unsigned)(k * 13 + sd + 1));
            CHECK(code == 99 || code == 0);
            if (code != 99 && code != 0) {
                fprintf(stderr, "FAIL power(%s) k=%d sd=%d: child exit %d\n", mode, k, sd, code);
                k = 1 << 20;
                break;
            }
            crashes += code == 99;
            thandle rh;
            open_tiered_quiet(wdb, &rh);
            eng *rdb = rh.db;
            if (!rdb) {
                close_tiered(&rh);
                k = 1 << 20;
                break;
            }
            CHECK(count_names(wdb, "txn_", ".intent") == 0 && count_names(wdb, "tmp_", ".part") == 0);
            part_sig cur[8];
            size_t nc = snap_key(rdb, key, cur, 8);
            CHECK(nc == nall);
            size_t cur_ec = 0, cur_part = 0;
            for (size_t i = 0; i < nc; i++) {
                cur_ec += cur[i].is_ec;
                cur_part += !cur[i].is_ec;
            }
            /* Either still 2 .ec + 1 .part (pre-commit) OR 0 .ec + 3 .part (post-commit): never mixed! */
            CHECK((cur_ec == 2 && cur_part == 1) || (cur_ec == 0 && cur_part == 3));
            if (cur_ec == 0) {
                check_sigs_match(all_sigs, nall, cur, nc, false, "power post-commit");
                post++;
            } else {
                for (size_t i = 0; i < nc && i < nall; i++) {
                    CHECK(strcmp(cur[i].stem, all_sigs[i].stem) == 0 && cur[i].min_block == all_sigs[i].min_block &&
                          cur[i].max_block == all_sigs[i].max_block && cur[i].level == all_sigs[i].level);
                }
                pre++;
            }
            acc got = read_f32(rdb, s, INT64_MIN, INT64_MAX);
            CHECK(got.n == oracle.n && got.sum == oracle.sum && got.hash == oracle.hash);
            close_tiered(&rh);
            if (code == 0) {
                finished = true;
                CHECK(cur_ec == 0); /* acknowledged unsettle MUST be durable */
            }
        }
    }
    CHECK(finished && crashes > 5 && pre > 0 && post > 0);
    printf("  power loss (%s): %d crash steps x %d seeds -> %d pre-commit, %d post-commit, all exact & ranks held\n",
           mode, crashes / seeds, seeds, pre, post);
}

static void case_power_loss(void)
{
    if (access(SHIM, R_OK) != 0) {
        printf("  power loss: skipped (no %s)\n", SHIM);
        return;
    }
    power_sweep("settled", false);
    power_sweep("decode", true);
}

/* ------------------------------------------------------------ 9. CLI `tsdb DIR volume diverged --clear` */

static void case_cli_diverged(void)
{
    if (access(CLI, X_OK) != 0) {
        fprintf(stderr, "FAIL cli_diverged: no CLI binary at %s\n", CLI);
        failures++;
        return;
    }
    int64_t key = T0 / DAY;

    /* Subcase 1: diverged with intact settled_ and CORRUPT chunks across ALL stores -> CLI restores from settled_,
     * clears DIVERGED only after swap is durable, and next open serves exact original rows! */
    char base[PATH_MAX], dbdir[PATH_MAX], stores[3][PATH_MAX];
    fresh("cli_div_settled", base, sizeof base);
    thandle h;
    make_tiered_db(base, dbdir, sizeof dbdir, stores, &h);
    eng *db = h.db;
    uint32_t s = 0;
    acc oracle = {0};
    part_sig ec_sigs[8], all_sigs[8];
    size_t nec = 0, nall = 0;
    setup_lww_settled(db, key, &s, &oracle, ec_sigs, &nec, all_sigs, &nall);
    uint8_t oid[16] = {1, 2, 3, 4};
    CHECK(ec_diverge(db, key, oid, EC_DIV_DIGEST) == 0);
    close_tiered(&h);
    /* Corrupt ALL chunks across all 3 stores so decode would fail -> CLI MUST succeed via settled_ fast path! */
    int ncor = 0;
    for (int i = 0; i < 3; i++)
        ncor += corrupt_chunks_in(stores[i], true);
    CHECK(ncor >= 4);
    CHECK(exists(dbdir, "DIVERGED"));
    CHECK(sh("'%s' '%s' volume diverged --clear >/dev/null 2>&1", CLI, dbdir) == 0);
    CHECK(!exists(dbdir, "DIVERGED"));
    CHECK(count_names(dbdir, "p", ".ec") == 0);
    open_tiered_quiet(dbdir, &h);
    db = h.db;
    CHECK(!atomic_load(&db->diverged));
    part_sig after[8];
    size_t na = snap_key(db, key, after, 8);
    check_sigs_match(all_sigs, nall, after, na, false, "cli_div_settled");
    acc got = read_f32(db, s, INT64_MIN, INT64_MAX);
    CHECK(got.n == oracle.n && got.sum == oracle.sum && got.hash == oracle.hash);
    close_tiered(&h);

    /* Subcase 2: diverged with NO settled_ and ALL stores corrupt -> unsettle fails -> CLI MUST fail and keep
     * DIVERGED intact on disk! */
    fresh("cli_div_fail", base, sizeof base);
    make_tiered_db(base, dbdir, sizeof dbdir, stores, &h);
    db = h.db;
    setup_lww_settled(db, key, &s, &oracle, ec_sigs, &nec, all_sigs, &nall);
    for (size_t i = 0; i < nec; i++) {
        char snm[128];
        snprintf(snm, sizeof snm, "settled_%.95s.part", ec_sigs[i].stem);
        CHECK(unlinkat(db->dirfd, snm, 0) == 0);
    }
    CHECK(ec_diverge(db, key, oid, EC_DIV_DIGEST) == 0);
    close_tiered(&h);
    ncor = 0;
    for (int i = 0; i < 3; i++)
        ncor += corrupt_chunks_in(stores[i], true);
    CHECK(ncor >= 4);
    CHECK(sh("'%s' '%s' volume diverged --clear >/dev/null 2>&1", CLI, dbdir) != 0);
    CHECK(exists(dbdir, "DIVERGED")); /* DIVERGED MUST NOT be cleared when unsettle failed! */
    CHECK(count_names(dbdir, "p", ".ec") == 2);

    /* Subcase 3: diverged with NO settled_ and healthy stores -> CLI decodes from tier, swaps back, clears DIVERGED */
    fresh("cli_div_decode", base, sizeof base);
    make_tiered_db(base, dbdir, sizeof dbdir, stores, &h);
    db = h.db;
    setup_lww_settled(db, key, &s, &oracle, ec_sigs, &nec, all_sigs, &nall);
    for (size_t i = 0; i < nec; i++) {
        char snm[128];
        snprintf(snm, sizeof snm, "settled_%.95s.part", ec_sigs[i].stem);
        CHECK(unlinkat(db->dirfd, snm, 0) == 0);
    }
    CHECK(ec_diverge(db, key, oid, EC_DIV_DIGEST) == 0);
    close_tiered(&h);
    CHECK(sh("'%s' '%s' volume diverged --clear >/dev/null 2>&1", CLI, dbdir) == 0);
    CHECK(!exists(dbdir, "DIVERGED"));
    CHECK(count_names(dbdir, "p", ".ec") == 0);
    open_tiered_quiet(dbdir, &h);
    db = h.db;
    CHECK(!atomic_load(&db->diverged));
    na = snap_key(db, key, after, 8);
    check_sigs_match(all_sigs, nall, after, na, false, "cli_div_decode");
    got = read_f32(db, s, INT64_MIN, INT64_MAX);
    CHECK(got.n == oracle.n && got.sum == oracle.sum && got.hash == oracle.hash);
    close_tiered(&h);

    /* Subcase 4: non-root added volume (`tsdb DIR volume diverged VOLPATH --clear`): exercises volume= parsing from
     * META, tier_open(root_dir) + tier_want(t, vol_idx) + eng_open(vol_dir) on a non-root volume! */
    char vol1[PATH_MAX + 32];
    fresh("cli_div_vol1", base, sizeof base);
    make_tiered_db(base, dbdir, sizeof dbdir, stores, &h);
    close_tiered(&h);
    snprintf(vol1, sizeof vol1, "%s/vol1", base);
    CHECK(mkdir(vol1, 0755) == 0);
    uint32_t vidx = 0;
    CHECK(tsdb_volume_add_dir(dbdir, vol1, &vidx) == 0 && vidx == 1);
    {
        tsdb_options o;
        tsdb_options_init(&o);
        o.merge_threads = 0;
        o.create_volumes = 0;
        o.partition_classes = 0;
        CHECK(tier_open(dbdir, &o, &h.t) == 0 && h.t != NULL);
        tier_quiesce(h.t);
        tier_want(h.t, vidx);
        CHECK(eng_open(vol1, &o, &h.db) == 0 && h.db != NULL);
        tier_want(NULL, 0);
        eckeep_eng_stop(h.db);
        db = h.db;
        /* Write volume=1 into vol1/META as tsdb_open does for non-root volumes */
        setup_lww_settled(db, key, &s, &oracle, ec_sigs, &nec, all_sigs, &nall);
        for (size_t i = 0; i < nec; i++) {
            char snm[128];
            snprintf(snm, sizeof snm, "settled_%.95s.part", ec_sigs[i].stem);
            CHECK(unlinkat(db->dirfd, snm, 0) == 0);
        }
        CHECK(ec_diverge(db, key, oid, EC_DIV_DIGEST) == 0);
        close_tiered(&h);
    }
    CHECK(exists(vol1, "DIVERGED") && count_names(vol1, "p", ".ec") == 2);
    CHECK(sh("'%s' '%s' volume diverged '%s' --clear >/dev/null 2>&1", CLI, dbdir, vol1) == 0);
    CHECK(!exists(vol1, "DIVERGED") && count_names(vol1, "p", ".ec") == 0);
    {
        tsdb_options o;
        tsdb_options_init(&o);
        o.merge_threads = 0;
        o.create_volumes = 0;
        o.partition_classes = 0;
        CHECK(tier_open(dbdir, &o, &h.t) == 0 && h.t != NULL);
        tier_quiesce(h.t);
        tier_want(h.t, vidx);
        CHECK(eng_open(vol1, &o, &h.db) == 0 && h.db != NULL);
        tier_want(NULL, 0);
        eckeep_eng_stop(h.db);
        db = h.db;
        CHECK(!atomic_load(&db->diverged));
        na = snap_key(db, key, after, 8);
        check_sigs_match(all_sigs, nall, after, na, false, "cli_div_vol1");
        got = read_f32(db, s, INT64_MIN, INT64_MAX);
        CHECK(got.n == oracle.n && got.sum == oracle.sum && got.hash == oracle.hash);
        close_tiered(&h);
    }

    printf("  CLI volume diverged --clear: settled_ restore, fail-keeps-DIVERGED, decode & non-root volume ok\n");
}

#endif /* TSDB_EC */

int main(int argc, char **argv)
{
#ifndef TSDB_EC
    (void)argc;
    (void)argv;
    CHECK(eng_unsettle(NULL, 0) == TSDB_ERR_INVALID);
    printf("test_ecunsettle: skipped (built without TSDB_EC)\n");
    return failures != 0;
#else
    const char *scratch = getenv("TMPDIR") ? getenv("TMPDIR") : "/tmp";
    const char *child_mode = NULL, *child_dir = NULL, *mname = "none";
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--quick"))
            quick = true;
        else if (!strcmp(argv[i], "--mutant") && i + 1 < argc) {
            mname = argv[++i];
            int m = ecunsettle_mutant_parse(mname);
            if (m < 0) {
                fprintf(stderr, "unknown mutant %s\n", mname);
                return 2;
            }
            ecunsettle_mutant = m;
        } else if (!strcmp(argv[i], "--child") && i + 2 < argc) {
            child_mode = argv[++i];
            child_dir = argv[++i];
        } else {
            scratch = argv[i];
        }
    }
    if (child_mode)
        return child_unsettle(child_mode, child_dir);

    char tmpl[PATH_MAX];
    snprintf(tmpl, sizeof tmpl, "%s/test-ecunsettle.XXXXXX", scratch);
    if (!mkdtemp(tmpl)) {
        perror("mkdtemp");
        return 1;
    }
    snprintf(S, sizeof S, "%s", tmpl);
    if (!realpath(argv[0], SELF))
        snprintf(SELF, sizeof SELF, "%s", argv[0]);
    char *slash = strrchr(SELF, '/');
    int bdirlen = slash ? (int)(slash - SELF) : 1;
    const char *bdir = slash ? SELF : ".";
    snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", bdirlen, bdir);
    snprintf(CLI, sizeof CLI, "%.*s/tsdb", bdirlen, bdir);

    printf("test_ecunsettle%s%s\n", ecunsettle_mutant ? " --mutant " : "", ecunsettle_mutant ? mname : "");
    case_settled_fast();
    case_decode_missing();
    case_decode_flipped();
    case_decode_checks();
    case_oracle_twins();
    case_recover_hand();
    case_concurrent_drop();
    case_resettle_and_refusals();
    case_ttl_drop_race();
    case_power_loss();
    case_cli_diverged();

    if (failures == 0)
        sh("rm -rf '%s'", S);
    printf(failures ? "test_ecunsettle: %d failures\n" : "test_ecunsettle: ok\n", failures);
    return failures != 0;
#endif
}
