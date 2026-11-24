/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Settled sources (src/eckeep.c, r7-keep; design-cluster-data.md E26, the plan's D5): settled_<part>.part is kept for
 * keep_s from the .ec record's settle time and until its digest matched; a digest that does not match holds it for
 * good and makes the volume DIVERGED (UNAVAILABLE answers).
 *
 * Settles are made by hand until r7-settle's eng_settle: the part's bytes put into the tier's stores as an object
 * (chunk_be_put_object, as ec_test_attach does, optionally from other bytes), its .ec file written (ec_index_write),
 * opened (part_open_ec) and swapped in (ec_swap_commit), which renames the source settled_<name>.
 *
 *   xxh64     the digest's xxh64 over pieces equals xxh64() in one call, at every length and split
 *   match     the digest matches; the file stays until keep_s after the settle, then goes; answers unchanged
 *   restart   keep_s one day, the file's mtime two days old (a rename keeps the source's), the handle reopened a minute
 *             later: the digest matches again and the file stays
 *   damage1   one damaged chunk (a data position, at most m): the digest matches, nothing DIVERGED
 *   damageall a byte changed in every position of a stripe: DIVERGED, one line naming volume, partition, oid and both
 *             files; the file kept past keep_s; queries and writes of the volume answer UNAVAILABLE; again after a
 *             reopen
 *   othersrc  an object whose record agrees with its chunks but whose bytes are not the source's: DIVERGED
 *   badrec    chunks and sums of other bytes under the source's object_xxh64: only the object's hash tells: DIVERGED
 *   gone      every chunk file of the object removed, every store UP: DIVERGED (never a wait)
 *   long      the settled_ file longer than the object, its first bytes the source's: DIVERGED
 *   restart   (in each of the five above) a restart keeps it: DIVERGED (the file, src/ecdiverge.h) is read at the open,
 *             refused before the new digest ran; the digest that fails again adds no record
 *   clearcli  (othersrc) `volume diverged --clear` refused while open and on a cluster volume (naming R4), offline it
 *             clears and the volume serves again
 *   reopen    a DIVERGED volume 1 through tsdb_volume_reopen (not a restart): still UNAVAILABLE, before the new
 *             engine's digest ran, and after tsdb_volume_resume; also a packed insert over two volumes, a series made;
 *             eng_halt stopped the old engine's digest thread before the new engine opened
 *   pending   a digest that has not run when the handle closes: after the reopen nothing is released before it ran
 *   down      a store DOWN: the digest waits and the file stays; back UP: it matches
 *   orphan    the key dropped before the digest: the file stays, and goes keep_s (an hour) later
 *   rot       a granule byte of the kept copy flipped, the object intact: rot of the copy (D25), counted lost, the .ec
 *             served, not DIVERGED; released at its deadline (mutant rotdiverges)
 *   relpower  a release and a power loss right after it (crashfs): the file stays released (mutant releasenosync)
 *
 * Usage: test_keep [--mutant digestskip|digestraw|digestnohold|keepmtime|noobjcmp|openwait|nolencheck|orphannow|
 * nohaltstop|divergemem|clearcluster] [SCRATCH]. Without TSDB_EC it says it is
 * skipped.
 */
#include "internal.h"
#include "chunk.h"
#include "cvol.h"
#include "eckeep.h"
#include "ecdigest.h"
#include "ecpart.h"
#include "ecswap.h"
#include "ecdiverge.h"
#include "tier.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
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
uint64_t eckeep_test_xxh64_pieces(const void *data, size_t len, size_t piece);

#define K 2
#define M 1
#define NST 3
static const int64_t DAY = 86400000, T0 = 1704067200000; /* 2024-01-01 */
static char scratch[PATH_MAX];
static char dbdir[PATH_MAX + 64], stores[NST][PATH_MAX + 64];
static _Atomic int64_t clock_off; /* added to the real clock: a reopen "a minute later", keep_s passed */

static int64_t test_clock(void *arg)
{
    (void)arg;
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000 + clock_off;
}

static void rm_rf(const char *path)
{
    char cmd[PATH_MAX * 2];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", path);
    if (system(cmd) != 0)
        fprintf(stderr, "rm -rf %s failed\n", path);
}

static bool exists_in(const char *dir, const char *name)
{
    char p[PATH_MAX * 2];
    snprintf(p, sizeof p, "%s/%s", dir, name);
    return access(p, F_OK) == 0;
}

static bool exists(const char *name)
{
    char p[PATH_MAX * 2];
    snprintf(p, sizeof p, "%s/%s", dbdir, name);
    return access(p, F_OK) == 0;
}

static void fresh(const char *what)
{
    snprintf(dbdir, sizeof dbdir, "%s/%s", scratch, what);
    rm_rf(dbdir);
    CHECK(mkdir(dbdir, 0755) == 0);
    for (int i = 0; i < NST; i++) {
        snprintf(stores[i], sizeof stores[i], "%s/%s.st%d", scratch, what, i);
        rm_rf(stores[i]);
        char away[PATH_MAX + 80];
        snprintf(away, sizeof away, "%s.away", stores[i]);
        rm_rf(away);
    }
}

typedef struct {
    tsdb *t;
    eng *db;
    uint32_t s;          /* the local id of series "s" in volume v */
    uint32_t v;          /* the volume */
    const char *path;    /* its path for tsdb_series_create (NULL: the root) */
} h;

static int h_open(h *x, int keep_s)
{
    char spec[8192];
    int n = snprintf(spec, sizeof spec, "k=%d,m=%d,keep_s=%d,cache_mb=16,dirs=", K, M, keep_s);
    for (int i = 0; i < NST; i++)
        n += snprintf(spec + n, sizeof spec - (size_t)n, "%s%s", i ? ":" : "", stores[i]);
    setenv("TSDB_TEST_TIER", spec, 1);
    setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.partition_classes = 0;
    o.clock_ms = test_clock;
    x->t = NULL;
    x->db = NULL;
    x->v = 0;
    x->path = NULL;
    int rc = tsdb_open(dbdir, &o, &x->t);
    unsetenv("TSDB_TEST_TIER");
    unsetenv("TSDB_TEST_TIER_I_KNOW");
    if (rc)
        return rc;
    x->db = tsdb_test_engine(x->t, 0);
    CHECK(x->db && x->db->tier);
    if (!x->db || eng_series(x->db, "s", TSDB_FLOAT32, &x->s) != 0)
        return TSDB_ERR_INVALID;
    return 0;
}

static void h_close(h *x)
{
    if (x->t)
        tsdb_close(x->t);
    x->t = NULL;
    x->db = NULL;
}

static uint64_t lcg_state = 88172645463325252ull;
static uint64_t lcg(void)
{
    lcg_state = lcg_state * 6364136223846793005ull + 1442695040888963407ull;
    return lcg_state >> 33;
}

static void write_day(h *x, int n)
{
    int64_t ts[512];
    float v[512];
    for (int i = 0; i < n; i += 512) {
        int k = n - i < 512 ? n - i : 512;
        for (int j = 0; j < k; j++)
            ts[j] = T0 + (int64_t)(i + j) * 3000, v[j] = (float)(lcg() % 1000003) / 7.0f; /* a larger object */
        CHECK(eng_insert_series(x->db, TSDB_FLOAT32, x->s, ts, v, (size_t)k) == 0);
    }
    CHECK(eng_flush(x->db) == 0);
}

typedef struct {
    size_t n;
    double sum;
} acc;

static int acc_fn(void *ctx, const int64_t *ts, const float *v, size_t n)
{
    (void)ts;
    acc *a = ctx;
    for (size_t i = 0; i < n; i++)
        a->sum += v[i];
    a->n += n;
    return 0;
}

/* the rows of "s" through the handle (the route() family): its return code */
static int read_all(h *x, acc *a)
{
    memset(a, 0, sizeof *a);
    return tsdb_query_raw(x->t, TSDB_SID(x->v, x->s), T0 - DAY, T0 + 2 * DAY, acc_fn, a);
}

typedef struct {
    char settled[256];
    ec_record rec;
    uint32_t vol;
} settled;

/* what settle_one encodes: the part's bytes; other bytes with a record that agrees with them; other bytes with the
 * record (its object_xxh64) of the source, so that the chunks' sums agree with the object and only the object's hash
 * compared with the record's can tell */
enum { SRC_SAME, SRC_ALT, SRC_BADREC };

/* A settle by hand (see the top) of the first listed .part with granules */
static int settle_one(h *x, int how, settled *out)
{
    eng *db = x->db;
    part *p = NULL;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts && !p; i++)
        if (!atomic_load(&db->parts[i]->ec) && db->parts[i]->ng > 0) {
            p = db->parts[i];
            part_ref(p);
        }
    pthread_mutex_unlock(&db->lock);
    CHECK(p != NULL);
    if (!p)
        return TSDB_ERR_NOTFOUND;
    uint8_t *alt = NULL;
    const uint8_t *src = p->map;
    if (how != SRC_SAME) {
        alt = malloc(p->map_len);
        memcpy(alt, p->map, p->map_len);
        alt[p->map_len / 3] ^= 0x40; /* inside some granule: its record and sums agree with these bytes */
        src = alt;
    }
    chunk_backend *be = tier_backend(db->tier);
    ec_record r;
    memset(&r, 0, sizeof r);
    chunk_geom g;
    int rc = chunk_geom_derive(&g, K, M, p->map_len);
    CHECK(rc == 0);
    r.version = EC_RECORD_VERSION;
    r.class_check = EC_CLS_PASSED;
    chunk_random(r.oid, sizeof r.oid);
    r.key = p->partition;
    r.min_block = p->min_block;
    r.max_block = p->max_block;
    r.level = p->level;
    r.k = (uint16_t)g.k;
    r.m = (uint16_t)g.m;
    r.cell_bytes = g.cell;
    r.object_len = p->map_len;
    r.object_xxh64 = xxh64(how == SRC_BADREC ? p->map : src, p->map_len, 0);
    r.writer_epoch = 1;
    r.settle_ms = eng_now_ms(db);
    snprintf(r.source, sizeof r.source, "%s", p->name);
    r.nsums = chunk_sums_total(&g);
    r.sums = malloc(r.nsums * sizeof *r.sums);
    CHECK(chunk_be_see_epoch(be, db->tier_vol, 1) == 0);
    rc = chunk_be_put_object(be, db->tier_vol, tier_vuuid(db), r.oid, &g, 1, src, ~0u, r.sums);
    CHECK(rc == 0);
    char tmp[96] = "", ecname[160];
    snprintf(ecname, sizeof ecname, "%.*s.ec", (int)(strlen(p->name) - 5), p->name);
    if (rc == 0)
        CHECK((rc = ec_index_write(db, p, &r, tmp)) == 0);
    part *e = NULL;
    if (rc == 0)
        CHECK((rc = part_open_ec(db, tmp, ecname, &e)) == 0 && e);
    if (rc == 0) {
        CHECK(strlen(tmp) < sizeof e->pending); /* tmp_* names are short; the copy must not cut one */
        snprintf(e->pending, sizeof e->pending, "%.63s", tmp);
        CHECK((rc = ec_swap_commit(db, &p, &e, 1)) == 0);
        part_unref(e);
    }
    snprintf(out->settled, sizeof out->settled, "settled_%s", p->name);
    out->rec = r;
    out->vol = db->tier_vol;
    part_unref(p);
    free(alt);
    return rc;
}

static void chunk_path(const settled *s, unsigned pos, char *out, size_t cap)
{
    char hex[33];
    chunk_oid_hex(s->rec.oid, hex);
    for (int i = 0; i < NST; i++) {
        snprintf(out, cap, "%s/s%" PRIu32 "/o%s.%u.chunk", stores[i], s->vol, hex, pos);
        if (access(out, F_OK) == 0)
            return;
    }
    out[0] = '\0';
}

/* flips bytes of the cells of position pos (never its header): every 1500th from 100 */
static int flip(const settled *s, unsigned pos)
{
    chunk_geom g;
    CHECK(chunk_geom_make(&g, s->rec.k, s->rec.m, s->rec.cell_bytes, s->rec.object_len) == 0);
    char p[16384];
    chunk_path(s, pos, p, sizeof p);
    CHECK(p[0] != '\0');
    int fd = open(p, O_RDWR);
    CHECK(fd >= 0);
    if (fd < 0)
        return 0;
    int n = 0;
    for (uint64_t off = 100; off < chunk_pos_len(&g, pos); off += 1500, n++) {
        uint8_t b;
        CHECK(pread(fd, &b, 1, (off_t)(CHUNK_HDR_BYTES + off)) == 1);
        b ^= 0x5a;
        CHECK(pwrite(fd, &b, 1, (off_t)(CHUNK_HDR_BYTES + off)) == 1);
    }
    close(fd);
    return n;
}

/* stderr into a file while a block runs */
static int saved_err = -1;
static char cap_path[PATH_MAX + 64];
static void cap_begin(void)
{
    snprintf(cap_path, sizeof cap_path, "%s/stderr.txt", scratch);
    fflush(stderr);
    saved_err = dup(2);
    int fd = open(cap_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    dup2(fd, 2);
    close(fd);
}
static char *cap_end(void)
{
    fflush(stderr);
    dup2(saved_err, 2);
    close(saved_err);
    FILE *f = fopen(cap_path, "r");
    static char buf[1 << 16];
    size_t n = f ? fread(buf, 1, sizeof buf - 1, f) : 0;
    buf[n] = '\0';
    if (f)
        fclose(f);
    fputs(buf, stderr); /* still shown */
    return buf;
}

static void sleep_ms(int ms)
{
    struct timespec ts = {ms / 1000, (ms % 1000) * 1000000L};
    nanosleep(&ts, NULL);
}

/* ------------------------------------------------------------ cases */

static void case_xxh64(void)
{
    uint8_t buf[700];
    for (size_t i = 0; i < sizeof buf; i++)
        buf[i] = (uint8_t)(i * 131 + 7);
    static const size_t pieces[] = {1, 3, 7, 31, 32, 33, 64, 100, 1000};
    for (size_t len = 0; len < sizeof buf; len += len < 80 ? 1 : 37)
        for (size_t j = 0; j < sizeof pieces / sizeof *pieces; j++)
            CHECK(eckeep_test_xxh64_pieces(buf, len, pieces[j]) == xxh64(buf, len, 0));
}

static void case_match(void)
{
    fresh("match");
    h x;
    CHECK(h_open(&x, 2) == 0);
    write_day(&x, 20000);
    acc a0, a1;
    CHECK(read_all(&x, &a0) == 0 && a0.n == 20000);
    settled s;
    CHECK(settle_one(&x, SRC_SAME, &s) == 0);
    CHECK(exists(s.settled));
    CHECK(eckeep_test_wait(x.db, s.settled, 20000) == ECKEEP_MATCHED);
    graveyard_purge(x.db, false);
    if (eng_now_ms(x.db) < s.rec.settle_ms + 1900) /* a slow run (sanitizers) may be past the deadline already */
        CHECK(exists(s.settled) && eckeep_status(x.db, s.settled) == ECKEEP_MATCHED);
    while (eng_now_ms(x.db) < s.rec.settle_ms + 2100)
        sleep_ms(50);
    graveyard_purge(x.db, false);
    CHECK(!exists(s.settled) && eckeep_status(x.db, s.settled) == TSDB_ERR_NOTFOUND);
    CHECK(!atomic_load(&x.db->diverged) && tsdb_volume_diverged(x.t, x.v) == 0);
    CHECK(read_all(&x, &a1) == 0 && a1.n == a0.n && a1.sum == a0.sum);
    free(s.rec.sums);
    h_close(&x);
}

static void case_restart(void)
{
    fresh("restart");
    h x;
    CHECK(h_open(&x, 86400) == 0);
    write_day(&x, 20000);
    settled s;
    CHECK(settle_one(&x, SRC_SAME, &s) == 0);
    CHECK(eckeep_test_wait(x.db, s.settled, 20000) == ECKEEP_MATCHED);
    h_close(&x);
    /* the source was written two days ago: a rename keeps that mtime */
    char p[PATH_MAX * 2];
    snprintf(p, sizeof p, "%s/%s", dbdir, s.settled);
    struct timespec tv[2];
    clock_gettime(CLOCK_REALTIME, &tv[0]);
    tv[0].tv_sec -= 2 * 86400;
    tv[1] = tv[0];
    CHECK(utimensat(AT_FDCWD, p, tv, 0) == 0);
    clock_off = 60000; /* a minute later */
    CHECK(h_open(&x, 86400) == 0);
    CHECK(eckeep_status(x.db, s.settled) == ECKEEP_PENDING || eckeep_status(x.db, s.settled) == ECKEEP_MATCHED);
    CHECK(eckeep_test_wait(x.db, s.settled, 20000) == ECKEEP_MATCHED);
    graveyard_purge(x.db, false);
    CHECK(exists(s.settled) && eckeep_status(x.db, s.settled) == ECKEEP_MATCHED);
    h_close(&x);
    clock_off = 0;
    free(s.rec.sums);
}

static void case_damage1(void)
{
    fresh("damage1");
    h x;
    CHECK(h_open(&x, 1) == 0);
    write_day(&x, 20000);
    acc a0, a1;
    CHECK(read_all(&x, &a0) == 0);
    atomic_store(&eckeep_test_pause, true);
    settled s;
    CHECK(settle_one(&x, SRC_SAME, &s) == 0);
    CHECK(flip(&s, 0) > 0); /* a data position: a RAW read would hash these bytes */
    atomic_store(&eckeep_test_pause, false);
    CHECK(eckeep_test_wait(x.db, s.settled, 20000) == ECKEEP_MATCHED);
    CHECK(!atomic_load(&x.db->diverged) && tsdb_volume_diverged(x.t, x.v) == 0);
    CHECK(read_all(&x, &a1) == 0 && a1.n == a0.n && a1.sum == a0.sum);
    free(s.rec.sums);
    h_close(&x);
}

static void diverged_checks(h *x, const settled *s, const char *log)
{
    char hex[33];
    chunk_oid_hex(s->rec.oid, hex);
    char ecname[160];
    snprintf(ecname, sizeof ecname, "%.*s.ec", (int)(strlen(s->settled) - 8 - 5), s->settled + 8);
    char vp[48];
    snprintf(vp, sizeof vp, "volume %" PRIu32 " partition", x->v);
    CHECK(strstr(log, "DIVERGED") && strstr(log, hex) && strstr(log, ecname) && strstr(log, s->settled) &&
          strstr(log, vp));
    CHECK(atomic_load(&x->db->diverged));
    /* the accessor tsdbd reads (r8i): this volume, no other, and no volume past the last */
    CHECK(tsdb_volume_diverged(x->t, x->v) == 1);
    if (x->v != 0)
        CHECK(tsdb_volume_diverged(x->t, 0) == 0);
    CHECK(tsdb_volume_diverged(x->t, (size_t)1 << 20) == TSDB_ERR_INVALID);
    acc a;
    CHECK(read_all(x, &a) == TSDB_ERR_UNAVAILABLE);
    CHECK(tsdb_append(x->t, TSDB_SID(x->v, x->s), T0 + 5 * DAY, 1.0f) == TSDB_ERR_UNAVAILABLE);
    /* the other ways in: a packed insert of one volume (insert_slots), a series made in the volume (series_make) */
    tsdb_sid two[2] = {TSDB_SID(x->v, x->s), TSDB_SID(x->v, x->s)};
    int64_t tt[2] = {T0 + 5 * DAY, T0 + 5 * DAY + 1};
    float vv[2] = {1, 2};
    CHECK(tsdb_insert(x->t, two, tt, vv, 2) == TSDB_ERR_UNAVAILABLE);
    tsdb_sid nid = 0;
    CHECK(tsdb_series_create(x->t, "made_while_diverged", TSDB_FLOAT32, x->path, &nid) == TSDB_ERR_UNAVAILABLE);
    /* held for good: past keep_s (1 s) a purge leaves it */
    while (eng_now_ms(x->db) < s->rec.settle_ms + 1200)
        sleep_ms(50);
    graveyard_purge(x->db, false);
    CHECK(exists_in(x->path ? x->path : dbdir, s->settled) && eckeep_status(x->db, s->settled) == ECKEEP_DIVERGED);
}

/* The records of dir/DIVERGED (48 bytes each), -1 when it cannot be read; none without the file. */
static int div_records(const char *dir)
{
    char p[PATH_MAX * 2];
    if (snprintf(p, sizeof p, "%s/DIVERGED", dir) >= (int)sizeof p)
        return -1;
    struct stat st;
    if (stat(p, &st) != 0)
        return errno == ENOENT ? 0 : -1;
    return st.st_size % 48 ? -1 : (int)(st.st_size / 48);
}

/* clearcli: `tsdb DIR volume diverged --clear` (ecdiverge_cli) is refused while the volume is open (the writer's
 * LOCK), on a cluster volume's directory (D23, a line naming R4; mutant clearcluster), and offline it removes the
 * file: the next open serves the volume, its digest paused (the kept file would diverge it again). */
static void clear_checks(h *x, const settled *s)
{
    (void)s;
    char *log;
    FILE *devnull = fopen("/dev/null", "w");
    CHECK(devnull != NULL);
    cap_begin();
    CHECK(ecdiverge_cli(dbdir, true, devnull) == TSDB_ERR_LOCKED);
    log = cap_end();
    CHECK(strstr(log, "holds the directory") != NULL);
    CHECK(div_records(dbdir) == 1);
    /* a cluster volume's directory with the same record: refused, the file stays */
    char cdir[PATH_MAX + 200], src[PATH_MAX * 3], dst[PATH_MAX * 3];
    snprintf(cdir, sizeof cdir, "%s/../clustervol", dbdir);
    rm_rf(cdir);
    CHECK(mkdir(cdir, 0755) == 0);
    snprintf(src, sizeof src, "%s/DIVERGED", dbdir);
    snprintf(dst, sizeof dst, "%s/DIVERGED", cdir);
    char buf[48 * 4];
    FILE *f = fopen(src, "rb"), *g = fopen(dst, "wb");
    CHECK(f && g);
    size_t n = f ? fread(buf, 1, sizeof buf, f) : 0;
    CHECK(n == 48 && g && fwrite(buf, 1, n, g) == n);
    if (f)
        fclose(f);
    if (g)
        fclose(g);
    snprintf(dst, sizeof dst, "%s/META", cdir);
    g = fopen(dst, "w");
    CHECK(g && fputs("version=9\nrequires=cluster,replica\n", g) >= 0);
    if (g)
        fclose(g);
    cap_begin();
    int rc = ecdiverge_cli(cdir, true, devnull);
    log = cap_end();
    CHECK(rc == TSDB_ERR_INVALID && strstr(log, "R4") && strstr(log, "refused"));
    CHECK(div_records(cdir) == 1);
    CHECK(ecdiverge_cli(cdir, false, devnull) == 1); /* listing is fine anywhere */
    rm_rf(cdir);
    /* offline: cleared, logged */
    h_close(x);
    cap_begin();
    CHECK(ecdiverge_cli(dbdir, true, devnull) == 1);
    log = cap_end();
    CHECK(strstr(log, "removing the DIVERGED record") && strstr(log, "cleared"));
    CHECK(div_records(dbdir) == 0);
    fclose(devnull);
    atomic_store(&eckeep_test_pause, true);
    CHECK(h_open(x, 1) == 0);
    acc a;
    CHECK(!atomic_load(&x->db->diverged) && tsdb_volume_diverged(x->t, x->v) == 0);
    /* served again: the write it refused goes in, and the read is the engine's own answer (this object's bytes are
     * not the source's: what the operator accepted), never UNAVAILABLE */
    CHECK(tsdb_append(x->t, TSDB_SID(x->v, x->s), T0 + 5 * DAY, 1.0f) == 0);
    CHECK(read_all(x, &a) != TSDB_ERR_UNAVAILABLE);
    atomic_store(&eckeep_test_pause, false);
}

enum { DV_DAMAGEALL, DV_OTHERSRC, DV_BADREC, DV_GONE, DV_LONG };
static const char *const DV_NAME[] = {"damageall", "othersrc", "badrec", "gone", "long"};

static void case_diverged(int mode)
{
    fresh(DV_NAME[mode]);
    h x;
    CHECK(h_open(&x, 1) == 0);
    write_day(&x, 20000);
    acc a0;
    CHECK(read_all(&x, &a0) == 0);
    atomic_store(&eckeep_test_pause, true);
    settled s;
    CHECK(settle_one(&x, mode == DV_OTHERSRC ? SRC_ALT : mode == DV_BADREC ? SRC_BADREC : SRC_SAME, &s) == 0);
    unsigned npos = (unsigned)s.rec.k + s.rec.m; /* the object's k' + m */
    if (mode == DV_DAMAGEALL)
        for (unsigned pos = 0; pos < npos; pos++)
            CHECK(flip(&s, pos) > 0);
    if (mode == DV_GONE) /* every store UP, no chunk of the object anywhere: NOTFOUND from the open, never a wait */
        for (unsigned pos = 0; pos < npos; pos++) {
            char cp[16384];
            chunk_path(&s, pos, cp, sizeof cp);
            CHECK(cp[0] && unlink(cp) == 0);
        }
    if (mode == DV_LONG) { /* the kept file longer than the object, its first object_len bytes the source's */
        char fp[PATH_MAX * 2];
        snprintf(fp, sizeof fp, "%s/%s", dbdir, s.settled);
        CHECK(truncate(fp, (off_t)s.rec.object_len + 4096) == 0);
    }
    int64_t tmo = mode == DV_GONE ? 5000 : 20000; /* openwait leaves it PENDING: no need to wait long for that */
    cap_begin();
    atomic_store(&eckeep_test_pause, false);
    int st = eckeep_test_wait(x.db, s.settled, tmo);
    char *log = cap_end();
    CHECK(st == ECKEEP_DIVERGED);
    diverged_checks(&x, &s, log);
    h_close(&x);
    CHECK(div_records(dbdir) == 1);
    /* a restart (R8 D8): refused from the open on, before its own digest ran (paused); it digests again, and again it
     * does not match, which adds no record (fails on main, where the flag was memory only; mutant divergemem) */
    atomic_store(&eckeep_test_pause, true);
    cap_begin();
    CHECK(h_open(&x, 1) == 0);
    acc a;
    CHECK(atomic_load(&x.db->diverged) && tsdb_volume_diverged(x.t, x.v) == 1);
    CHECK(read_all(&x, &a) == TSDB_ERR_UNAVAILABLE);
    atomic_store(&eckeep_test_pause, false);
    st = eckeep_test_wait(x.db, s.settled, tmo);
    log = cap_end();
    CHECK(st == ECKEEP_DIVERGED);
    diverged_checks(&x, &s, log);
    if (mode == DV_OTHERSRC) /* a readable object: what the clear lets through */
        clear_checks(&x, &s);
    h_close(&x);
    CHECK(div_records(dbdir) == (mode == DV_OTHERSRC ? 0 : 1));
    free(s.rec.sums);
}

/* tsdb_volume_reopen (what tsdbd's dom_recover runs once a health domain is back) is not a restart: a volume 1 that a
 * digest found DIVERGED stays refused by the new engine, with that engine's own digest not run yet (paused); the old
 * engine's digest thread is stopped by eng_halt, before the new engine opens */
static eng *reopen_old;
static int reopen_hook_calls;
static void reopen_hook(void *arg, int phase)
{
    (void)arg;
    if (phase == 1) {
        reopen_hook_calls++;
        CHECK(eckeep_test_stopped(reopen_old));
    }
}

/* rot (D25): one byte of a granule of the kept copy flipped; the object equals its record: the copy fails its own
 * granule checksum, so it is rot of the copy (logged, counted lost, released at its deadline), the .ec served and the
 * volume not DIVERGED (mutant rotdiverges). A copy whose rows differ with valid checksums stays DIVERGED (DV_OTHERSRC). */
static void case_rot(void)
{
    fresh("rot");
    h x;
    CHECK(h_open(&x, 1) == 0);
    write_day(&x, 20000);
    acc a0, a1;
    CHECK(read_all(&x, &a0) == 0);
    atomic_store(&eckeep_test_pause, true);
    settled s;
    CHECK(settle_one(&x, SRC_SAME, &s) == 0);
    part *cp = NULL;
    CHECK(part_open_as(x.db->dirfd, s.settled, s.settled + 8, false, false, &cp) == 0);
    if (cp) {
        granule g;
        gx_load(cp, cp->ng / 2, GW_LOC, &g);
        uint64_t off = g.off + g.ts_len + g.val_len / 2;
        part_unref(cp);
        char fp[PATH_MAX * 2];
        snprintf(fp, sizeof fp, "%s/%s", dbdir, s.settled);
        int fd = open(fp, O_RDWR);
        uint8_t b = 0;
        CHECK(fd >= 0 && pread(fd, &b, 1, (off_t)off) == 1);
        b ^= 0x40;
        CHECK(pwrite(fd, &b, 1, (off_t)off) == 1 && fsync(fd) == 0);
        close(fd);
    }
    uint64_t lost0 = eckeep_copies_lost(x.db);
    cap_begin();
    atomic_store(&eckeep_test_pause, false);
    int st = eckeep_test_wait(x.db, s.settled, 20000);
    char *log = cap_end();
    CHECK(st == ECKEEP_ROT);
    CHECK(eckeep_copies_lost(x.db) == lost0 + 1);
    CHECK(log && strstr(log, "fails its checksums (rot)") && strstr(log, s.settled));
    CHECK(!atomic_load(&x.db->diverged) && tsdb_volume_diverged(x.t, x.v) == 0);
    CHECK(read_all(&x, &a1) == 0 && a1.n == a0.n && a1.sum == a0.sum); /* the .ec served */
    while (eng_now_ms(x.db) < s.rec.settle_ms + 1100)
        sleep_ms(50);
    graveyard_purge(x.db, false);
    CHECK(!exists(s.settled)); /* released at its deadline, counted lost */
    h_close(&x);
    CHECK(div_records(dbdir) == 0);
    free(s.rec.sums);
}

/* relpower: a release is durable before it counts. The child (crashfs) settles, waits for the match and the deadline,
 * purges (the file released) and loses power at its exit without a close; the open finds the file still gone. Without
 * the directory fsync after the unlink the file comes back (mutant releasenosync). */
static char **main_argv;

static void relpower_names(void) /* fresh("relpower")'s names: the parent made the root, which crashfs needs at start */
{
    snprintf(dbdir, sizeof dbdir, "%s/rpdb", scratch); /* not a prefix of the stores' paths (CRASHFS_ROOT is one) */
    for (int i = 0; i < NST; i++)
        snprintf(stores[i], sizeof stores[i], "%s/relpower.st%d", scratch, i);
}

static void relpower_child(void)
{
    relpower_names();
    h x;
    CHECK(h_open(&x, 1) == 0);
    write_day(&x, 20000);
    settled s;
    CHECK(settle_one(&x, SRC_SAME, &s) == 0);
    CHECK(eckeep_test_wait(x.db, s.settled, 20000) == ECKEEP_MATCHED);
    while (eng_now_ms(x.db) < s.rec.settle_ms + 1100)
        sleep_ms(50);
    FILE *f = fopen("relpower.name", "w"); /* outside the crashfs root (the scratch, not the database) */
    if (f) {
        fprintf(f, "%s\n", s.settled);
        fclose(f);
    }
    atomic_store(&eckeep_test_exit_released, true); /* the release's purge exits: nothing else syncs the directory */
    graveyard_purge(x.db, false);
    CHECK(!"the purge released nothing");
}

static void case_relpower(void)
{
    char shim[PATH_MAX];
    ssize_t l = readlink("/proc/self/exe", shim, sizeof shim - 32);
    if (l <= 0)
        return;
    shim[l] = 0;
    char *sl = strrchr(shim, '/');
    snprintf(sl + 1, sizeof shim - (size_t)(sl + 1 - shim), "libcrashfs.so");
    if (access(shim, R_OK) != 0) {
        printf("  relpower: skipped (no %s)\n", shim);
        return;
    }
    char root[PATH_MAX + 32], namef[PATH_MAX + 32];
    snprintf(root, sizeof root, "%s/rpdb", scratch);
    snprintf(namef, sizeof namef, "%s/relpower.name", scratch);
    unlink(namef);
    fresh("relpower"); /* the stores */
    relpower_names();
    rm_rf(dbdir);
    CHECK(mkdir(dbdir, 0755) == 0);
    pid_t pid = fork();
    if (pid == 0) {
        if (chdir(scratch) != 0)
            _exit(4);
        setenv("TEST_KEEP_RELCHILD", scratch, 1);
        setenv("LD_PRELOAD", shim, 1);
        setenv("CRASHFS_ROOT", root, 1);
        setenv("CRASHFS_AT_EXIT", "1", 1);
        execv("/proc/self/exe", main_argv);
        _exit(5);
    }
    int ws = 0;
    CHECK(pid > 0 && waitpid(pid, &ws, 0) == pid && WIFEXITED(ws) && WEXITSTATUS(ws) == 0);
    char name[300] = "";
    FILE *f = fopen(namef, "r");
    CHECK(f && fscanf(f, "%299s", name) == 1);
    if (f)
        fclose(f);
    CHECK(name[0] && !exists(name)); /* the release survived the power loss (before any open could act) */
    atomic_store(&eckeep_test_pause, true);
    h x;
    CHECK(h_open(&x, 1) == 0);
    CHECK(!exists(name) && eckeep_status(x.db, name) == TSDB_ERR_NOTFOUND);
    atomic_store(&eckeep_test_pause, false);
    h_close(&x);
    printf("  relpower: a release, power lost at once, open: the file stays released\n");
}

static void case_reopen(void)
{
    fresh("reopen");
    char vpath[PATH_MAX + 64];
    snprintf(vpath, sizeof vpath, "%s/reopen.v1", scratch);
    rm_rf(vpath);
    h x;
    CHECK(h_open(&x, 1) == 0);
    uint32_t idx = 0;
    CHECK(tsdb_volume_add(x.t, vpath, &idx) == 0 && idx == 1);
    tsdb_sid sid = 0;
    CHECK(tsdb_series_create(x.t, "s1", TSDB_FLOAT32, vpath, &sid) == 0 && TSDB_SID_VOLUME(sid) == 1);
    h y = {.t = x.t, .db = tsdb_test_engine(x.t, 1), .s = TSDB_SID_LOCAL(sid), .v = 1, .path = vpath};
    CHECK(y.db && y.db->tier && y.db->tier_vol == 1);
    if (!y.db || !y.db->tier) {
        h_close(&x);
        return;
    }
    write_day(&y, 20000);
    atomic_store(&eckeep_test_pause, true);
    settled s;
    CHECK(settle_one(&y, SRC_ALT, &s) == 0);
    cap_begin();
    atomic_store(&eckeep_test_pause, false);
    int st = eckeep_test_wait(y.db, s.settled, 20000);
    char *log = cap_end();
    CHECK(st == ECKEEP_DIVERGED);
    diverged_checks(&y, &s, log);
    /* a packed insert over two volumes refuses the whole call; the root alone goes on */
    tsdb_sid two[2] = {TSDB_SID(0, x.s), TSDB_SID(1, y.s)};
    int64_t tt[2] = {T0 + 6 * DAY, T0 + 6 * DAY};
    float vv[2] = {1, 2};
    CHECK(tsdb_insert(x.t, two, tt, vv, 2) == TSDB_ERR_UNAVAILABLE);
    CHECK(tsdb_append(x.t, TSDB_SID(0, x.s), T0 + 6 * DAY, 1.0f) == 0);

    atomic_store(&eckeep_test_pause, true); /* the new engine's digest must not be what refuses it */
    reopen_old = y.db;
    reopen_hook_calls = 0;
    tsdb_test_reopen_hook(reopen_hook, NULL);
    CHECK(tsdb_volume_quiesce(x.t, 1) == 0);
    CHECK(tsdb_volume_idle(x.t, 1, 5000) == 0);
    tsdb_volume_report rep;
    CHECK(tsdb_volume_reopen(x.t, 1, &rep) == 0);
    tsdb_test_reopen_hook(NULL, NULL);
    CHECK(reopen_hook_calls == 1);
    y.db = tsdb_test_engine(x.t, 1);
    CHECK(y.db && y.db != reopen_old);
    CHECK(eckeep_status(y.db, s.settled) == ECKEEP_PENDING);
    CHECK(atomic_load(&y.db->diverged));
    acc a;
    CHECK(read_all(&y, &a) == TSDB_ERR_UNAVAILABLE);
    (void)tsdb_volume_resume(x.t, 1); /* accepts a reopen's loss, never a digest's mismatch */
    CHECK(read_all(&y, &a) == TSDB_ERR_UNAVAILABLE);
    CHECK(tsdb_append(x.t, TSDB_SID(1, y.s), T0 + 6 * DAY, 1.0f) == TSDB_ERR_UNAVAILABLE);
    atomic_store(&eckeep_test_pause, false);
    cap_begin();
    st = eckeep_test_wait(y.db, s.settled, 20000);
    (void)cap_end();
    CHECK(st == ECKEEP_DIVERGED);
    CHECK(exists_in(vpath, s.settled));
    h_close(&x);
    free(s.rec.sums);
}

static void case_pending(void)
{
    fresh("pending");
    h x;
    CHECK(h_open(&x, 1) == 0);
    write_day(&x, 20000);
    atomic_store(&eckeep_test_pause, true);
    settled s;
    CHECK(settle_one(&x, SRC_SAME, &s) == 0);
    h_close(&x);
    CHECK(exists(s.settled));
    CHECK(h_open(&x, 1) == 0);
    while (eng_now_ms(x.db) < s.rec.settle_ms + 1200)
        sleep_ms(50);
    graveyard_purge(x.db, false); /* due, but not digested in this process: held */
    CHECK(exists(s.settled) && eckeep_status(x.db, s.settled) == ECKEEP_PENDING);
    atomic_store(&eckeep_test_pause, false);
    CHECK(eckeep_test_wait(x.db, s.settled, 20000) == ECKEEP_MATCHED);
    graveyard_purge(x.db, false);
    CHECK(!exists(s.settled));
    h_close(&x);
    free(s.rec.sums);
}

static void move_store(int i, bool away)
{
    char a[PATH_MAX + 80];
    snprintf(a, sizeof a, "%s.away", stores[i]);
    CHECK(rename(away ? stores[i] : a, away ? a : stores[i]) == 0);
}

static void case_down(void)
{
    fresh("down");
    h x;
    CHECK(h_open(&x, 1) == 0);
    write_day(&x, 20000);
    atomic_store(&eckeep_test_pause, true);
    settled s;
    CHECK(settle_one(&x, SRC_SAME, &s) == 0);
    h_close(&x);
    move_store(NST - 1, true);
    CHECK(h_open(&x, 1) == 0);
    atomic_store(&eckeep_test_pause, false);
    uint64_t t0 = eckeep_test_tries(x.db);
    CHECK(eckeep_test_wait(x.db, s.settled, 2500) == ECKEEP_PENDING);
    CHECK(eckeep_test_tries(x.db) > t0);
    while (eng_now_ms(x.db) < s.rec.settle_ms + 1200)
        sleep_ms(50);
    graveyard_purge(x.db, false);
    CHECK(exists(s.settled) && eckeep_status(x.db, s.settled) == ECKEEP_PENDING);
    CHECK(!atomic_load(&x.db->diverged) && tsdb_volume_diverged(x.t, x.v) == 0);
    h_close(&x);
    move_store(NST - 1, false);
    CHECK(h_open(&x, 1) == 0);
    CHECK(eckeep_test_wait(x.db, s.settled, 20000) == ECKEEP_MATCHED);
    graveyard_purge(x.db, false);
    CHECK(!exists(s.settled));
    h_close(&x);
    free(s.rec.sums);
}

static void case_orphan(void)
{
    fresh("orphan");
    h x;
    CHECK(h_open(&x, 3600) == 0);
    write_day(&x, 20000);
    atomic_store(&eckeep_test_pause, true);
    settled s;
    CHECK(settle_one(&x, SRC_SAME, &s) == 0);
    CHECK(eng_drop_before(x.db, T0 + 2 * DAY) == 0);
    atomic_store(&eckeep_test_pause, false);
    CHECK(eckeep_test_wait(x.db, s.settled, 20000) == ECKEEP_ORPHAN);
    CHECK(!atomic_load(&x.db->diverged) && tsdb_volume_diverged(x.t, x.v) == 0);
    graveyard_purge(x.db, false); /* not before keep_s from when it was found so */
    CHECK(exists(s.settled) && eckeep_status(x.db, s.settled) == ECKEEP_ORPHAN);
    atomic_store(&clock_off, 3601000); /* an hour later */
    graveyard_purge(x.db, false);
    CHECK(!exists(s.settled));
    h_close(&x);
    atomic_store(&clock_off, 0);
    free(s.rec.sums);
}

static const struct {
    const char *name;
    int m;
} MUTANTS[] = {{"digestskip", ECKEEP_MUT_DIGESTSKIP},
               {"digestraw", ECKEEP_MUT_DIGESTRAW},
               {"digestnohold", ECKEEP_MUT_DIGESTNOHOLD},
               {"keepmtime", ECKEEP_MUT_KEEPMTIME},
               {"noobjcmp", ECKEEP_MUT_NOOBJCMP},
               {"openwait", ECKEEP_MUT_OPENWAIT},
               {"nolencheck", ECKEEP_MUT_NOLENCHECK},
               {"orphannow", ECKEEP_MUT_ORPHANNOW},
               {"nohaltstop", ECKEEP_MUT_NOHALTSTOP},
               {"divergemem", 100 + ECDIV_MUT_MEM}, /* src/ecdiverge.h's: 100 + its number */
               {"clearcluster", 100 + ECDIV_MUT_CLEARCLUSTER},
               {"releasenosync", 200 + ECDIGEST_MUT_RELEASENOSYNC}, /* src/ecdigest.h's: 200 + its number */
               {"rotdiverges", 200 + ECDIGEST_MUT_ROTDIVERGES}};
#endif

int main(int argc, char **argv)
{
#ifndef TSDB_EC
    (void)argc;
    (void)argv;
    printf("test_keep: skipped (built without TSDB_EC)\n");
    return 0;
#else
    const char *base = getenv("TMPDIR") ? getenv("TMPDIR") : "/tmp";
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--mutant") && i + 1 < argc) {
            const char *m = argv[++i];
            eckeep_mutant = -1;
            for (size_t k = 0; k < sizeof MUTANTS / sizeof *MUTANTS; k++)
                if (!strcmp(m, MUTANTS[k].name))
                    eckeep_mutant = MUTANTS[k].m;
            if (eckeep_mutant < 0) {
                fprintf(stderr, "unknown mutant %s\n", m);
                return 2;
            }
            if (eckeep_mutant >= 200) {
                ecdigest_mutant = eckeep_mutant - 200;
                eckeep_mutant = 0;
            } else if (eckeep_mutant >= 100) {
                ecdiverge_mutant = eckeep_mutant - 100;
                eckeep_mutant = 0;
            }
        } else {
            base = argv[i];
        }
    }
    if (getenv("TEST_KEEP_RELCHILD")) { /* case_relpower's child, under crashfs */
        snprintf(scratch, sizeof scratch, "%s", getenv("TEST_KEEP_RELCHILD"));
        eckeep_digest_mb_s = 0;
        relpower_child();
        exit(failures ? 3 : 0); /* exit, not _exit: crashfs's destructor drops what was not synced */
    }
    main_argv = argv;
    snprintf(scratch, sizeof scratch, "%s/test_keep.XXXXXX", base);
    if (!mkdtemp(scratch)) {
        perror("mkdtemp");
        return 1;
    }
    eckeep_digest_mb_s = 0;
    case_xxh64();
    case_match();
    case_restart();
    case_damage1();
    for (int m = DV_DAMAGEALL; m <= DV_LONG; m++)
        case_diverged(m);
    case_reopen();
    case_pending();
    case_down();
    case_orphan();
    case_rot();
    case_relpower();
    if (failures == 0)
        rm_rf(scratch);
    printf("test_keep: %s (%d failures)\n", failures ? "FAILED" : "ok", failures);
    return failures != 0;
#endif
}
