/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Tests for r15-scrub: single-server local tier chunk scrub and repair on the tierctl nice-19 worker,
 * persisted SCRUB cursor resume across restarts, single-copy (m = 0) report-only mode, non-live object skipping,
 * token-bucket pacing (scrub_mb_s), pause/yield during rebuild and shrink, oldest-first & scrub_days scheduling,
 * read-only follower skip, tier status (text + JSON), Prometheus /metrics, and mutants `scrubnorepair` and
 * `scrubnopace` (docs/design-cluster-data.md E25, §15.7; PLAN.md D12, D20).
 *
 *   ./build/test_tierscrub [--mutant scrubnorepair|scrubnopace] [scratch dir]
 */
#define _GNU_SOURCE
#include "chunk.h"
#include "chunk_priv.h"
#include "eccache.h"
#include "ecgc.h"
#include "ecpart.h"
#include "harness.h"
#include "internal.h"
#include "settle.h"
#include "tier.h"
#include "tierctl.h"
#include "tiers.h"
#include "tierscrub.h"
#include "tsdb.h"

#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <math.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static int fails;
static int saved_err = -1;
static int cap_rd = -1;
static char S[PATH_MAX], SELF[PATH_MAX], CLI[PATH_MAX + 32], TSDBD[PATH_MAX + 32];
static const char *mutant_name = NULL;

static void fail_at(const char *file, int line, const char *what)
{
    char buf[512];
    int n = snprintf(buf, sizeof buf, "FAIL %s:%d: %s\n", file, line, what);
    if (write(saved_err >= 0 ? saved_err : 2, buf, (size_t)n) < 0)
        fails++;
    fails++;
}

#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c))                                                                                                      \
            fail_at(__FILE__, __LINE__, #c);                                                                           \
    } while (0)

static int sh(const char *fmt, ...)
{
    char cmd[4096];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(cmd, sizeof cmd, fmt, ap);
    va_end(ap);
    return system(cmd);
}

static void fresh_dir(const char *name, char *out, size_t cap)
{
    int n = snprintf(out, cap, "%s/%s", S, name);
    CHECK(n > 0 && (size_t)n < cap);
    CHECK(sh("rm -rf '%s' && mkdir -p '%s'", out, out) == 0);
}

static int64_t mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void cap_begin(void)
{
    fflush(stderr);
    int pf[2];
    saved_err = dup(2);
    if (pipe(pf) == 0) {
        fcntl(pf[0], F_SETFL, O_NONBLOCK);
        fcntl(pf[0], F_SETFD, FD_CLOEXEC);
        fcntl(pf[1], F_SETFD, FD_CLOEXEC);
        cap_rd = pf[0];
        dup2(pf[1], 2);
        close(pf[1]);
    }
}

static void cap_end(char *out, size_t cap)
{
    fflush(stderr);
    if (saved_err >= 0) {
        dup2(saved_err, 2);
        close(saved_err);
        saved_err = -1;
    }
    size_t n = 0;
    if (cap_rd >= 0) {
        char junk[4096];
        bool room = out != NULL && cap > 0;
        for (;;) {
            ssize_t r = read(cap_rd, room ? out + n : junk, room ? cap - 1 - n : sizeof junk);
            if (r <= 0)
                break;
            if (room) {
                n += (size_t)r;
                room = (n + 1 < cap);
            }
        }
        close(cap_rd);
        cap_rd = -1;
    }
    if (out && cap > 0)
        out[n] = '\0';
}

#ifdef TSDB_EC

static bool pick_all(void *ctx, uint32_t vol, int64_t partition, uint32_t level, uint64_t rows)
{
    (void)ctx, (void)vol, (void)partition, (void)level, (void)rows;
    return true;
}

static int open_db(const char *dir, tsdb **db)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.partition_classes = 0;
    o.zstd_level = 0;
    *db = NULL;
    return tsdb_open(dir, &o, db);
}

static double det_val(int p, int r, int series)
{
    uint64_t x = ((uint64_t)(uint32_t)(p + 1) << 40) ^ ((uint64_t)(uint32_t)(r + 1) << 8) ^ (uint64_t)(series + 31);
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return (double)(int64_t)(x & 0x000fffffffffffffULL) * 1e-6;
}

typedef struct f64_collect_ctx {
    int64_t *ts;
    double *vals;
    size_t cap;
    size_t n;
    bool overflow;
} f64_collect_ctx;

static int f64_collect_cb(void *vctx, const int64_t *ts, const tsdb_values *values, size_t n)
{
    f64_collect_ctx *c = vctx;
    if (values->type != TSDB_FLOAT64)
        return TSDB_ERR_INVALID;
    if (c->n + n > c->cap) {
        c->overflow = true;
        return 1;
    }
    memcpy(c->ts + c->n, ts, n * sizeof *ts);
    memcpy(c->vals + c->n, values->values, n * sizeof *c->vals);
    c->n += n;
    return 0;
}

static void insert_and_settle(tsdb *db, tsdb_sid sid, int nparts, int rows_per_part)
{
    int64_t *ts = malloc((size_t)rows_per_part * sizeof *ts);
    double *vs = malloc((size_t)rows_per_part * sizeof *vs);
    CHECK(ts != NULL && vs != NULL);
    for (int p = 0; p < nparts; p++) {
        int64_t base = (int64_t)(p + 1) * 86400000LL;
        for (int r = 0; r < rows_per_part; r++) {
            ts[r] = base + (int64_t)r * 1000LL;
            vs[r] = det_val(p, r, 0);
        }
        tsdb_values val = {.type = TSDB_FLOAT64, .values = vs, .valid = NULL};
        CHECK(tsdb_insert_series_values(db, sid, ts, &val, (size_t)rows_per_part) == 0);
        CHECK(tsdb_flush(db) == 0);
    }
    free(ts);
    free(vs);

    size_t settled = 0;
    CHECK(tsdb_test_settle(db, pick_all, NULL, &settled) == 0);
    CHECK(settled >= (size_t)nparts);
}

static void verify_all_rows(tsdb *db, tsdb_sid sid, int nparts, int rows_per_part)
{
    size_t want = (size_t)nparts * (size_t)rows_per_part;
    f64_collect_ctx c = {
        .ts = malloc(want * sizeof *c.ts),
        .vals = malloc(want * sizeof *c.vals),
        .cap = want,
    };
    CHECK(c.ts != NULL && c.vals != NULL);
    CHECK(tsdb_query_values(db, sid, 0, (int64_t)(nparts + 2) * 86400000LL, NULL, f64_collect_cb, &c) == 0);
    CHECK(!c.overflow && c.n == want);
    for (int p = 0; p < nparts; p++) {
        int64_t base = (int64_t)(p + 1) * 86400000LL;
        for (int r = 0; r < rows_per_part; r++) {
            size_t idx = (size_t)p * (size_t)rows_per_part + (size_t)r;
            CHECK(c.ts[idx] == base + (int64_t)r * 1000LL);
            CHECK(memcmp(&c.vals[idx], &(double){det_val(p, r, 0)}, sizeof(double)) == 0);
        }
    }
    free(c.ts);
    free(c.vals);
}

typedef struct ec_info {
    int64_t partition;
    uint8_t oid[16];
    uint16_t k, m;
} ec_info;

static size_t list_live_ec(eng *e, ec_info *out, size_t cap)
{
    size_t n = 0;
    pthread_mutex_lock(&e->lock);
    for (size_t i = 0; i < e->nparts && n < cap; i++) {
        part *p = e->parts[i];
        if (p->dropped)
            continue;
        struct ecpart *ep = atomic_load_explicit(&p->ec, memory_order_acquire);
        if (!ep || ep->test_mark)
            continue;
        out[n].partition = p->partition;
        memcpy(out[n].oid, ep->rec.oid, 16);
        out[n].k = ep->rec.k;
        out[n].m = ep->rec.m;
        n++;
    }
    pthread_mutex_unlock(&e->lock);
    return n;
}

/* Flips 1 byte inside the first cell (right after the 96-byte header) of chunk (vol, oid, pos) on `store_path`. */
static void chunk_path_of(const char *store_path, uint32_t vol, const uint8_t oid[16], unsigned pos, char out[PATH_MAX])
{
    char hex[33];
    chunk_oid_hex(oid, hex);
    int n = snprintf(out, PATH_MAX, "%s/s%" PRIu32 "/o%s.%u.chunk", store_path, vol, hex, pos);
    CHECK(n > 0 && n < PATH_MAX);
}

static void flip_chunk_cell_byte(const char *cpath, off_t rel_cell_off)
{
    int fd = open(cpath, O_RDWR | O_CLOEXEC);
    CHECK(fd >= 0);
    off_t off = (off_t)CHUNK_HDR_BYTES + rel_cell_off;
    uint8_t b = 0;
    CHECK(pread(fd, &b, 1, off) == 1);
    b ^= 0x5au;
    CHECK(pwrite(fd, &b, 1, off) == 1);
    CHECK(fsync(fd) == 0);
    close(fd);
}

static uint64_t scrub_check_chunk_file(chunk_backend *be, int store_idx, uint32_t vol, const uint8_t vuuid[16],
                                       const uint8_t oid[16], unsigned pos)
{
    chunk_store *st = chunk_be_store_ref(be, store_idx);
    CHECK(st != NULL);
    chunk_ident id = {.volume = vol, .pos = (uint16_t)pos};
    memcpy(id.vuuid, vuuid, 16);
    memcpy(id.oid, oid, 16);
    chunk_rd *r = NULL;
    int rc = chunk_rd_open(st, &id, &r);
    CHECK(rc == 0 && r != NULL);
    uint64_t bad = 0;
    CHECK(chunk_rd_scrub(r, &bad) == 0);
    chunk_rd_close(r);
    chunk_store_unref(st);
    return bad;
}

static void setup_6store_db(const char *base, char dbdir[PATH_MAX], char stores[6][PATH_MAX], tsdb **out_db)
{
    int n = snprintf(dbdir, PATH_MAX, "%s/db", base);
    CHECK(n > 0 && n < PATH_MAX);
    CHECK(sh("mkdir -p '%s'", dbdir) == 0);
    tsdb *db = NULL;
    CHECK(open_db(dbdir, &db) == 0 && db != NULL);
    for (int i = 0; i < 6; i++) {
        n = snprintf(stores[i], PATH_MAX, "%s/st%d", base, i);
        CHECK(n > 0 && n < PATH_MAX);
        CHECK(tsdb_tier_add(db, stores[i], "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);
    }
    CHECK(tsdb_close(db) == 0);
    CHECK(open_db(dbdir, out_db) == 0 && *out_db != NULL);
}

static void reset_test_hooks(void)
{
    tierscrub_set_test_pause(false);
    tierscrub_set_obj_hook(NULL, NULL);
    tierscrub_set_wall_fn(NULL, NULL);
    tierscrub_set_test_opts(0, 0, 0);
    tierctl_set_rebuild_hook(NULL, NULL);
    tierctl_set_poll_ms(1000);
    atomic_store(&ecgc_test_nosweep, false);
}

/* ---------------------------------------------------------------- 1. 4+2 data & parity rot repair */
static void test_data_and_parity_rot(void)
{
    reset_test_hooks();
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX];
    fresh_dir("data_parity_rot", base, sizeof base);

    /* Pause scrub during initial settle + fault injection so we control the exact pass. */
    tierscrub_set_test_pause(true);
    tierscrub_set_test_opts(50, 0, 1);

    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db);
    tsdb_sid sid = 0;
    CHECK(tsdb_series_create(db, "rot.s0", TSDB_FLOAT64, NULL, &sid) == 0);
    /* 45,000 rows/part -> >256 KiB -> full k'=4, m=2 striping across all 6 stores. */
    insert_and_settle(db, sid, 3, 45000);

    eng *e = tsdb_test_engine(db, 0);
    tier *t = e->tier;
    CHECK(t != NULL && t->be != NULL);
    const uint8_t *vuuid = tier_vuuid(e);
    CHECK(vuuid != NULL);

    ec_info ecs[16];
    size_t nec = list_live_ec(e, ecs, 16);
    CHECK(nec == 3);

    /* Pick an object with full 4+2 geometry; flip 1 byte in DATA pos 0 and 1 byte in PARITY pos k (pos 4). */
    const ec_info *target = &ecs[0];
    CHECK(target->k == 4 && target->m == 2);
    unsigned data_pos = 0;
    unsigned parity_pos = target->k;
    int st_data = chunk_be_place(t->be, target->oid, data_pos);
    int st_parity = chunk_be_place(t->be, target->oid, parity_pos);
    CHECK(st_data >= 0 && st_parity >= 0 && st_data != st_parity);

    char p_data[PATH_MAX], p_parity[PATH_MAX];
    chunk_path_of(stores[st_data], 0, target->oid, data_pos, p_data);
    chunk_path_of(stores[st_parity], 0, target->oid, parity_pos, p_parity);
    flip_chunk_cell_byte(p_data, 7);
    flip_chunk_cell_byte(p_parity, 13);

    CHECK(scrub_check_chunk_file(t->be, st_data, 0, vuuid, target->oid, data_pos) > 0);
    CHECK(scrub_check_chunk_file(t->be, st_parity, 0, vuuid, target->oid, parity_pos) > 0);

    /* Unpause scrub and wait for both stores to complete a pass. */
    tierscrub_set_test_pause(false);
    tierctl_wake(t);
    CHECK(tierscrub_wait_pass(t, st_data, 1, 15000) == 0);
    CHECK(tierscrub_wait_pass(t, st_parity, 1, 15000) == 0);

    tierscrub_snapshot snap;
    CHECK(tierscrub_get_snapshot(t, &snap));
    CHECK(snap.stores[st_data].chunks_checked >= 1);
    CHECK(snap.stores[st_data].damaged >= 1);
    CHECK(snap.stores[st_data].repaired >= 1);
    CHECK(snap.stores[st_data].unrepairable == 0);
    CHECK(snap.stores[st_parity].chunks_checked >= 1);
    CHECK(snap.stores[st_parity].damaged >= 1);
    CHECK(snap.stores[st_parity].repaired >= 1);
    CHECK(snap.stores[st_parity].unrepairable == 0);

    /* Both chunks on disk must now verify clean (0 bad cells). */
    CHECK(scrub_check_chunk_file(t->be, st_data, 0, vuuid, target->oid, data_pos) == 0);
    CHECK(scrub_check_chunk_file(t->be, st_parity, 0, vuuid, target->oid, parity_pos) == 0);

    /* Queries must be bit-exact. */
    verify_all_rows(db, sid, 3, 45000);

    /* Pause scrub while taking 2 stores DOWN to verify degraded reads using the repaired parity cell. */
    tierscrub_set_test_pause(true);

    /* Extra proof for parity repair: mark 2 OTHER stores DOWN (not st_parity) so reads MUST use the repaired parity! */
    int downed = 0;
    for (int i = 0; i < 6 && downed < 2; i++) {
        if (i == st_parity)
            continue;
        /* Prefer taking down data positions of target so decoding uses st_parity. */
        if (i == st_data || (target->k > 1 && i == chunk_be_place(t->be, target->oid, 1))) {
            CHECK(chunk_be_set_down(t->be, i, CHUNK_STORE_DOWN_STALLED) == 0);
            downed++;
        }
    }
    if (downed < 2) {
        for (int i = 0; i < 6 && downed < 2; i++) {
            if (i == st_parity || chunk_be_store_state(t->be, i) != CHUNK_STORE_UP)
                continue;
            CHECK(chunk_be_set_down(t->be, i, CHUNK_STORE_DOWN_STALLED) == 0);
            downed++;
        }
    }
    CHECK(downed == 2);
    eccache_reopen_all(t);
    verify_all_rows(db, sid, 3, 45000);

    CHECK(tsdb_close(db) == 0);
    reset_test_hooks();
    printf("test_tierscrub: data_and_parity_rot ok\n");
}

/* ---------------------------------------------------------------- 2. Single-copy (m=0) report-only */
static void join_path(char *out, size_t cap, const char *a, const char *b)
{
    int n = snprintf(out, cap, "%s/%s", a, b);
    CHECK(n > 0 && (size_t)n < cap);
}

static void test_single_copy_report_only(void)
{
    reset_test_hooks();
    char base[PATH_MAX], dbdir[PATH_MAX], st0[PATH_MAX];
    fresh_dir("single_copy", base, sizeof base);
    join_path(dbdir, sizeof dbdir, base, "db");
    join_path(st0, sizeof st0, base, "sc0");
    CHECK(sh("mkdir -p '%s'", dbdir) == 0);

    tierscrub_set_test_pause(true);
    tierscrub_set_test_opts(50, 0, 1);

    tsdb *db = NULL;
    CHECK(open_db(dbdir, &db) == 0);
    CHECK(tsdb_tier_add(db, st0, "hdd", TSDB_TIER_SINGLE_COPY | TSDB_TIER_ALLOW_SHARED_DEV) == 0);
    CHECK(tsdb_close(db) == 0);

    CHECK(open_db(dbdir, &db) == 0);
    tsdb_sid sid = 0;
    CHECK(tsdb_series_create(db, "sc.s0", TSDB_FLOAT64, NULL, &sid) == 0);
    insert_and_settle(db, sid, 2, 1000);

    eng *e = tsdb_test_engine(db, 0);
    tier *t = e->tier;
    CHECK(t != NULL && t->be != NULL);

    ec_info ecs[8];
    size_t nec = list_live_ec(e, ecs, 8);
    CHECK(nec == 2);
    CHECK(ecs[0].k == 1 && ecs[0].m == 0);

    char cpath[PATH_MAX];
    chunk_path_of(st0, 0, ecs[0].oid, 0, cpath);
    flip_chunk_cell_byte(cpath, 11);

    /* Snapshot raw chunk file bytes BEFORE scrub. */
    struct stat st_before;
    CHECK(stat(cpath, &st_before) == 0 && st_before.st_size > 0);
    uint8_t *raw_before = malloc((size_t)st_before.st_size);
    CHECK(raw_before != NULL);
    int fd = open(cpath, O_RDONLY | O_CLOEXEC);
    CHECK(fd >= 0);
    CHECK(pread(fd, raw_before, (size_t)st_before.st_size, 0) == st_before.st_size);
    close(fd);

    char errbuf[16384];
    cap_begin();
    tierscrub_set_test_pause(false);
    tierctl_wake(t);
    CHECK(tierscrub_wait_pass(t, 0, 1, 15000) == 0);
    cap_end(errbuf, sizeof errbuf);

    tierscrub_snapshot snap;
    CHECK(tierscrub_get_snapshot(t, &snap));
    CHECK(snap.stores[0].chunks_checked >= 2);
    CHECK(snap.stores[0].damaged >= 1);
    CHECK(snap.stores[0].unrepairable >= 1);
    CHECK(snap.stores[0].repaired == 0);

    /* Log line must name BOTH the object (oid hex) and its partition. */
    char ohex[33], part_tok[64];
    chunk_oid_hex(ecs[0].oid, ohex);
    snprintf(part_tok, sizeof part_tok, "partition %" PRId64, ecs[0].partition);
    CHECK(strstr(errbuf, ohex) != NULL);
    CHECK(strstr(errbuf, part_tok) != NULL);

    /* Raw chunk file on disk must be 100% untouched. */
    struct stat st_after;
    CHECK(stat(cpath, &st_after) == 0 && st_after.st_size == st_before.st_size);
    uint8_t *raw_after = malloc((size_t)st_after.st_size);
    CHECK(raw_after != NULL);
    fd = open(cpath, O_RDONLY | O_CLOEXEC);
    CHECK(fd >= 0);
    CHECK(pread(fd, raw_after, (size_t)st_after.st_size, 0) == st_after.st_size);
    close(fd);
    CHECK(memcmp(raw_before, raw_after, (size_t)st_before.st_size) == 0);
    free(raw_before);
    free(raw_after);

    /* Persisted SCRUB file must match counters. */
    tierscrub_store_stats disk_st;
    CHECK(tierscrub_read_store_file(st0, &disk_st) == 0);
    CHECK(disk_st.has_data && disk_st.damaged >= 1 && disk_st.unrepairable >= 1 && disk_st.repaired == 0);
    CHECK(disk_st.passes_done >= 1);

    CHECK(tsdb_close(db) == 0);
    reset_test_hooks();
    printf("test_tierscrub: single_copy_report_only ok\n");
}

/* ---------------------------------------------------------------- 3. Non-live (dropped) objects never repaired */
static void test_nonlive_not_repaired(void)
{
    reset_test_hooks();
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX];
    fresh_dir("nonlive", base, sizeof base);

    tierscrub_set_test_pause(true);
    tierscrub_set_test_opts(50, 0, 1);
    atomic_store(&ecgc_test_nosweep, true);

    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db);
    tsdb_sid sid = 0;
    CHECK(tsdb_series_create(db, "nl.s0", TSDB_FLOAT64, NULL, &sid) == 0);
    /* 45,000 rows/part -> full 4+2 striping so every store holds chunks for every live partition. */
    insert_and_settle(db, sid, 3, 45000);

    eng *e = tsdb_test_engine(db, 0);
    tier *t = e->tier;
    const uint8_t *vuuid = tier_vuuid(e);
    ec_info ecs[8];
    size_t nec = list_live_ec(e, ecs, 8);
    CHECK(nec == 3);

    /* Find the earliest partition (partition 1: [86400000, 172800000)). */
    int p0_idx = 0;
    for (size_t i = 1; i < nec; i++)
        if (ecs[i].partition < ecs[p0_idx].partition)
            p0_idx = (int)i;
    ec_info dropped = ecs[p0_idx];

    /* Drop partition 1 so its .ec object is no longer live, while chunks remain on disk (ecgc_test_nosweep = true). */
    CHECK(tsdb_drop_before(db, (dropped.partition + 1) * 86400000LL) == 0);
    CHECK(list_live_ec(e, ecs, 8) == 2);

    /* Corrupt pos 0 of the dropped object. */
    int st_drop = chunk_be_place(t->be, dropped.oid, 0);
    char cpath[PATH_MAX];
    chunk_path_of(stores[st_drop], 0, dropped.oid, 0, cpath);
    flip_chunk_cell_byte(cpath, 5);
    CHECK(scrub_check_chunk_file(t->be, st_drop, 0, vuuid, dropped.oid, 0) > 0);

    tierscrub_set_test_pause(false);
    tierctl_wake(t);
    CHECK(tierscrub_wait_pass(t, st_drop, 1, 15000) == 0);

    /* Live objects were checked, dropped object was NOT repaired (still corrupt on disk!). */
    tierscrub_snapshot snap;
    CHECK(tierscrub_get_snapshot(t, &snap));
    CHECK(snap.stores[st_drop].chunks_checked >= 2);
    CHECK(snap.stores[st_drop].repaired == 0);
    CHECK(snap.stores[st_drop].damaged == 0);
    CHECK(scrub_check_chunk_file(t->be, st_drop, 0, vuuid, dropped.oid, 0) > 0);

    CHECK(tsdb_close(db) == 0);
    reset_test_hooks();
    printf("test_tierscrub: nonlive_not_repaired ok\n");
}

/* ---------------------------------------------------------------- 3b. Mid-pass GC deletion is never false damage */
typedef struct midgc_hook_ctx {
    tsdb *db;
    tier *t;
    uint8_t vuuid[16];
    uint8_t victim_oid[16];
    int64_t victim_part;
    uint16_t victim_np;
    bool deleted;
} midgc_hook_ctx;

static void midgc_obj_hook(void *vctx, int store, uint32_t vol, const uint8_t oid[16],
                           const struct chunk_heal_report *delta)
{
    (void)store, (void)vol, (void)delta;
    midgc_hook_ctx *c = vctx;
    if (c->deleted || memcmp(oid, c->victim_oid, 16) == 0)
        return;
    /* Only trigger once an object strictly before victim_oid has been scrubbed, so chunk_be_heal already listed
     * victim_oid in ev and has not yet reached it. */
    if (memcmp(oid, c->victim_oid, 16) >= 0)
        return;
    c->deleted = true;
    /* Drop victim partition and delete ALL of its chunks from the backend right now (simulating GC mid-pass). */
    CHECK(tsdb_drop_before(c->db, (c->victim_part + 1) * 86400000LL) == 0);
    for (unsigned p = 0; p < c->victim_np; p++) {
        chunk_ident id = {.volume = 0, .pos = (uint16_t)p};
        memcpy(id.vuuid, c->vuuid, 16);
        memcpy(id.oid, c->victim_oid, 16);
        (void)chunk_be_del(c->t->be, &id, tier_epoch(c->t, 0));
    }
}

static void test_midpass_gc_no_false_damage(void)
{
    reset_test_hooks();
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX];
    fresh_dir("midpass_gc", base, sizeof base);

    tierscrub_set_test_pause(true);
    tierscrub_set_test_opts(50, 0, 1);

    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db);
    tsdb_sid sid = 0;
    CHECK(tsdb_series_create(db, "mgc.s0", TSDB_FLOAT64, NULL, &sid) == 0);
    /* 45,000 rows/part -> full 4+2 across all 6 stores, 4 partitions so at least one oid < partition 1's oid
     * (or pick the partition with the largest oid that we can drop, i.e., if partition 1 is not smallest). */
    insert_and_settle(db, sid, 6, 45000);

    eng *e = tsdb_test_engine(db, 0);
    tier *t = e->tier;
    const uint8_t *vuuid = tier_vuuid(e);
    ec_info ecs[16];
    size_t nec = list_live_ec(e, ecs, 16);
    CHECK(nec == 6);

    /* Find the earliest partition (partition 1) — ensure at least one other object has a smaller oid so our hook
     * fires BEFORE chunk_be_heal reaches victim_oid! If partition 1 happens to have the smallest oid, drop up to the
     * partition whose oid is largest among the first 5 so at least 1 earlier oid and 1 later live partition exist. */
    size_t vic = 0;
    bool have_vic = false;
    for (size_t i = 0; i < nec; i++) {
        if (ecs[i].partition <= 5 && (!have_vic || memcmp(ecs[i].oid, ecs[vic].oid, 16) > 0)) {
            vic = i;
            have_vic = true;
        }
    }
    CHECK(have_vic);
    bool has_smaller = false;
    for (size_t i = 0; i < nec; i++)
        if (memcmp(ecs[i].oid, ecs[vic].oid, 16) < 0)
            has_smaller = true;
    CHECK(has_smaller);

    midgc_hook_ctx mc = {
        .db = db,
        .t = t,
        .victim_part = ecs[vic].partition,
        .victim_np = (uint16_t)(ecs[vic].k + ecs[vic].m),
        .deleted = false,
    };
    memcpy(mc.vuuid, vuuid, 16);
    memcpy(mc.victim_oid, ecs[vic].oid, 16);
    tierscrub_set_obj_hook(midgc_obj_hook, &mc);

    tierscrub_set_test_pause(false);
    tierctl_wake(t);
    CHECK(tierscrub_wait_pass(t, 0, 1, 15000) == 0);
    CHECK(mc.deleted);

    tierscrub_snapshot snap;
    CHECK(tierscrub_get_snapshot(t, &snap));
    CHECK(snap.stores[0].damaged == 0);
    CHECK(snap.stores[0].repaired == 0);
    CHECK(snap.stores[0].unrepairable == 0);

    tierscrub_set_obj_hook(NULL, NULL);
    CHECK(tsdb_close(db) == 0);
    reset_test_hooks();
    printf("test_tierscrub: midpass_gc_no_false_damage ok\n");
}

/* ---------------------------------------------------------------- 4. Restart mid-pass resumes from SCRUB cursor */
typedef struct cursor_hook_ctx {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    int target_store;
    int stop_after;
    uint8_t seen[64][16];
    int nseen;
    bool hit_stop;
} cursor_hook_ctx;

static void cursor_obj_hook(void *vctx, int store, uint32_t vol, const uint8_t oid[16],
                            const struct chunk_heal_report *delta)
{
    (void)vol, (void)delta;
    cursor_hook_ctx *c = vctx;
    if (store != c->target_store)
        return;
    pthread_mutex_lock(&c->mu);
    if (c->nseen < 64) {
        memcpy(c->seen[c->nseen], oid, 16);
        c->nseen++;
    }
    if (c->stop_after > 0 && c->nseen >= c->stop_after) {
        c->hit_stop = true;
        tierscrub_set_test_pause(true); /* pause right after this object so main thread closes cleanly mid-pass */
        pthread_cond_broadcast(&c->cv);
    }
    pthread_mutex_unlock(&c->mu);
}

static void test_cursor_restart_resume(void)
{
    reset_test_hooks();
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX];
    fresh_dir("cursor_resume", base, sizeof base);

    tierscrub_set_test_pause(true);
    tierscrub_set_test_opts(50, 0, 1);

    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db);
    tsdb_sid sid = 0;
    CHECK(tsdb_series_create(db, "cur.s0", TSDB_FLOAT64, NULL, &sid) == 0);
    /* 45,000 rows/part -> full 4+2 striping so store 0 deterministically holds all 8 objects. */
    insert_and_settle(db, sid, 8, 45000);

    eng *e = tsdb_test_engine(db, 0);
    tier *t = e->tier;

    ec_info ecs[32];
    size_t nec = list_live_ec(e, ecs, 32);
    CHECK(nec == 8);
    int expect_st0 = 0;
    for (size_t i = 0; i < nec; i++) {
        unsigned np = (unsigned)ecs[i].k + (unsigned)ecs[i].m;
        for (unsigned p = 0; p < np; p++)
            if (chunk_be_place(t->be, ecs[i].oid, p) == 0) {
                expect_st0++;
                break;
            }
    }
    CHECK(expect_st0 >= 4);
    int stop_at = expect_st0 / 2;
    if (stop_at < 2)
        stop_at = 2;

    cursor_hook_ctx before = {.target_store = 0, .stop_after = stop_at};
    pthread_mutex_init(&before.mu, NULL);
    tsdb_cond_init_mono(&before.cv);
    tierscrub_set_obj_hook(cursor_obj_hook, &before);

    tierscrub_set_test_pause(false);
    tierctl_wake(t);

    /* Wait until stop_at objects on store 0 have been scrubbed and hook pauses scrub. */
    struct timespec dl;
    tsdb_cond_deadline_ms(&dl, 15000);
    pthread_mutex_lock(&before.mu);
    while (!before.hit_stop) {
        if (pthread_cond_timedwait(&before.cv, &before.mu, &dl) == ETIMEDOUT)
            break;
    }
    bool got_stop = before.hit_stop;
    int n_before = before.nseen;
    pthread_mutex_unlock(&before.mu);
    CHECK(got_stop && n_before >= stop_at && n_before < expect_st0);

    /* Close DB cleanly mid-pass. */
    tierscrub_set_obj_hook(NULL, NULL);
    CHECK(tsdb_close(db) == 0);
    db = NULL;

    /* Verify persisted SCRUB file on store 0 has cursor_valid == true and passes_done == 0. */
    tierscrub_store_stats mid_st;
    CHECK(tierscrub_read_store_file(stores[0], &mid_st) == 0);
    CHECK(mid_st.has_data && mid_st.cursor_valid && mid_st.passes_done == 0);
    CHECK(mid_st.chunks_checked >= (uint64_t)n_before);

    /* Reopen DB and record every OID scrubbed on store 0 until pass 1 completes. */
    cursor_hook_ctx after = {.target_store = 0, .stop_after = 0};
    pthread_mutex_init(&after.mu, NULL);
    tsdb_cond_init_mono(&after.cv);
    tierscrub_set_obj_hook(cursor_obj_hook, &after);
    tierscrub_set_test_pause(false);
    tierscrub_set_test_opts(60000, 0, 1); /* large interval so only pass 1 finishes on store 0 */

    CHECK(open_db(dbdir, &db) == 0);
    e = tsdb_test_engine(db, 0);
    t = e->tier;
    CHECK(tierscrub_wait_pass(t, 0, 1, 15000) == 0);

    pthread_mutex_lock(&after.mu);
    int n_after = after.nseen;
    /* None of the already-scrubbed OIDs from before restart may be re-scanned! */
    for (int i = 0; i < n_before; i++) {
        for (int j = 0; j < n_after; j++)
            CHECK(memcmp(before.seen[i], after.seen[j], 16) != 0);
    }
    CHECK(n_before + n_after == expect_st0);
    pthread_mutex_unlock(&after.mu);

    tierscrub_store_stats done_st;
    CHECK(tierscrub_read_store_file(stores[0], &done_st) == 0);
    CHECK(done_st.has_data && !done_st.cursor_valid && done_st.passes_done == 1);

    tierscrub_set_obj_hook(NULL, NULL);
    CHECK(tsdb_close(db) == 0);

    /* Corrupt SCRUB file on store 0: must log and start a fresh pass cleanly (never fatal). */
    char spath[PATH_MAX + 32];
    join_path(spath, sizeof spath, stores[0], TIERSCRUB_FILE);
    int fd = open(spath, O_WRONLY | O_TRUNC | O_CLOEXEC);
    CHECK(fd >= 0);
    CHECK(write(fd, "corrupt scrub\n", 14) == 14);
    close(fd);

    tierscrub_set_test_opts(50, 0, 1);
    CHECK(open_db(dbdir, &db) == 0);
    e = tsdb_test_engine(db, 0);
    t = e->tier;
    CHECK(tierscrub_wait_pass(t, 0, 1, 15000) == 0);
    CHECK(tsdb_close(db) == 0);

    pthread_cond_destroy(&before.cv);
    pthread_mutex_destroy(&before.mu);
    pthread_cond_destroy(&after.cv);
    pthread_mutex_destroy(&after.mu);
    reset_test_hooks();
    printf("test_tierscrub: cursor_restart_resume ok\n");
}

/* ---------------------------------------------------------------- 5. Pacing held (catches scrubnopace) */
static void test_pacing(void)
{
    reset_test_hooks();
    char base[PATH_MAX], dbdir[PATH_MAX], st0[PATH_MAX];
    fresh_dir("pacing", base, sizeof base);
    join_path(dbdir, sizeof dbdir, base, "db");
    join_path(st0, sizeof st0, base, "sc0");
    CHECK(sh("mkdir -p '%s'", dbdir) == 0);

    tierscrub_set_test_pause(true);

    tsdb *db = NULL;
    CHECK(open_db(dbdir, &db) == 0);
    CHECK(tsdb_tier_add(db, st0, "hdd", TSDB_TIER_SINGLE_COPY | TSDB_TIER_ALLOW_SHARED_DEV) == 0);
    CHECK(tsdb_close(db) == 0);

    /* Set scrub_mb_s = 1 via real CLI `tier set` (stored in TIERS). */
    CHECK(sh("'%s' '%s' tier set --scrub-mb-s 1 >/dev/null", CLI, dbdir) == 0);

    CHECK(open_db(dbdir, &db) == 0);
    tsdb_sid sid = 0;
    CHECK(tsdb_series_create(db, "pace.s0", TSDB_FLOAT64, NULL, &sid) == 0);
    /* Write ~1.6+ MiB of float64 rows across 3 partitions (~1.5+ s at 1 MiB/s). */
    insert_and_settle(db, sid, 3, 80000);

    eng *e = tsdb_test_engine(db, 0);
    tier *t = e->tier;
    CHECK(tierctl_scrub_mb_s(t) == 1);

    int64_t t0 = mono_ms();
    tierscrub_set_test_pause(false);
    tierctl_wake(t);
    CHECK(tierscrub_wait_pass(t, 0, 1, 20000) == 0);
    int64_t elapsed_ms = mono_ms() - t0;

    tierscrub_snapshot snap;
    CHECK(tierscrub_get_snapshot(t, &snap));
    uint64_t bytes = snap.stores[0].bytes_checked;
    CHECK(bytes >= (1500ull << 10)); /* >= 1.5 MiB */

    /* At 1 MiB/s (1,048,576 B/s), expected duration is bytes / 1 MiB seconds (~1.7 s).
     * Lower bound (60% of nominal) decisively catches scrubnopace (<50ms) without flaky upper-bound tuning. */
    int64_t expected_ms = (int64_t)((bytes * 1000ull) / (1024ull * 1024ull));
    int64_t min_ms = (expected_ms * 60) / 100;
    if (elapsed_ms < min_ms) {
        fprintf(stderr, "test_tierscrub: pacing too fast: bytes=%" PRIu64 " elapsed=%" PRId64 "ms want>=%" PRId64 "ms\n",
                bytes, elapsed_ms, min_ms);
    }
    CHECK(elapsed_ms >= min_ms);

    CHECK(tsdb_close(db) == 0);
    reset_test_hooks();
    printf("test_tierscrub: pacing ok (bytes=%" PRIu64 ", elapsed=%" PRId64 "ms, expected=%" PRId64 "ms)\n", bytes,
           elapsed_ms, expected_ms);
}

/* ---------------------------------------------------------------- 6. Pauses while rebuild / shrink is active */
typedef struct rb_observe_ctx {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool started;
    bool hold;
} rb_observe_ctx;

static void rb_hook_cb(void *vctx, int store, uint64_t bytes_done)
{
    (void)store, (void)bytes_done;
    rb_observe_ctx *c = vctx;
    pthread_mutex_lock(&c->mu);
    c->started = true;
    pthread_cond_broadcast(&c->cv);
    while (c->hold) {
        struct timespec dl;
        tsdb_cond_deadline_ms(&dl, 50);
        pthread_cond_timedwait(&c->cv, &c->mu, &dl);
    }
    pthread_mutex_unlock(&c->mu);
}

static void test_pause_during_rebuild_and_shrink(void)
{
    reset_test_hooks();
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX], repl[PATH_MAX];
    fresh_dir("pause_rb_sh", base, sizeof base);
    join_path(repl, sizeof repl, base, "st5_repl");

    tierctl_set_poll_ms(50);
    tierscrub_set_test_pause(true);
    tierscrub_set_test_opts(20, 0, 1);

    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db);
    tsdb_sid sid = 0;
    CHECK(tsdb_series_create(db, "prb.s0", TSDB_FLOAT64, NULL, &sid) == 0);
    /* 45,000 rows/part -> full 4+2 striping so store 5 deterministically has chunks to rebuild. */
    insert_and_settle(db, sid, 3, 45000);

    eng *e = tsdb_test_engine(db, 0);
    tier *t = e->tier;

    /* Hold the rebuild hook mid-rebuild so we can verify scrub stays completely paused while rebuild runs. */
    rb_observe_ctx rbo = {.hold = true};
    pthread_mutex_init(&rbo.mu, NULL);
    tsdb_cond_init_mono(&rbo.cv);
    tierctl_set_rebuild_hook(rb_hook_cb, &rbo);

    /* Unpause scrub and trigger a live store 5 replacement rebuild. */
    tierscrub_set_test_pause(false);
    CHECK(sh("rm -rf '%s'", stores[5]) == 0);
    CHECK(tsdb_tier_lost(db, "5", repl, TSDB_TIER_ALLOW_SHARED_DEV) == 0);

    /* Wait until rebuild is actively executing inside run_rebuild. */
    struct timespec dl;
    tsdb_cond_deadline_ms(&dl, 15000);
    pthread_mutex_lock(&rbo.mu);
    while (!rbo.started) {
        if (pthread_cond_timedwait(&rbo.cv, &rbo.mu, &dl) == ETIMEDOUT)
            break;
    }
    CHECK(rbo.started);
    pthread_mutex_unlock(&rbo.mu);

    /* Snapshot scrub counters while rebuild is active, wait 250ms, verify zero scrub progress during rebuild! */
    tierscrub_snapshot s1, s2;
    CHECK(tierscrub_get_snapshot(t, &s1));
    CHECK(!s1.active && s1.paused);
    usleep(250000);
    CHECK(tierscrub_get_snapshot(t, &s2));
    CHECK(!s2.active && s2.paused);
    for (int i = 0; i < 6; i++)
        CHECK(s2.stores[i].chunks_checked == s1.stores[i].chunks_checked);

    /* Release rebuild and wait for it to finish; scrub must resume and advance passes on all 6 stores! */
    pthread_mutex_lock(&rbo.mu);
    rbo.hold = false;
    pthread_cond_broadcast(&rbo.cv);
    pthread_mutex_unlock(&rbo.mu);
    tierctl_set_rebuild_hook(NULL, NULL);
    CHECK(tierctl_wait_idle(t, 15000) == 0);

    for (int i = 0; i < 6; i++)
        CHECK(tierscrub_wait_pass(t, i, s2.stores[i].passes_done + 1, 15000) == 0);

    /* Now test shrink: change redundancy to 3+2 and shrink store 5 (6 -> 5 stores); scrub pauses during shrink and
     * resumes on surviving stores 0..4 afterward. */
    CHECK(sh("'%s' '%s' tier set --redundancy 3+2 >/dev/null", CLI, dbdir) == 0);
    tierctl_wake(t);
    CHECK(tierctl_wait_idle(t, 10000) == 0);

    tierscrub_snapshot pre_sh;
    CHECK(tierscrub_get_snapshot(t, &pre_sh));
    CHECK(sh("rm -rf '%s'", repl) == 0);
    CHECK(tsdb_tier_lost(db, "5", NULL, 0) == 0);
    CHECK(tierctl_wait_idle(t, 20000) == 0);

    /* Surviving stores 0..4 continue scrubbing after shrink completes. */
    for (int i = 0; i < 5; i++)
        CHECK(tierscrub_wait_pass(t, i, pre_sh.stores[i].passes_done + 1, 15000) == 0);

    verify_all_rows(db, sid, 3, 45000);
    CHECK(tsdb_close(db) == 0);
    pthread_cond_destroy(&rbo.cv);
    pthread_mutex_destroy(&rbo.mu);
    reset_test_hooks();
    printf("test_tierscrub: pause_during_rebuild_and_shrink ok\n");
}

/* ---------------------------------------------------------------- 7. Oldest-first & scrub_days scheduling */
static atomic_uint_fast64_t g_fake_wall = 1700000000ull;

static uint64_t fake_wall_cb(void *ctx)
{
    (void)ctx;
    return atomic_load_explicit(&g_fake_wall, memory_order_acquire);
}

static void test_oldest_first_and_scrub_days(void)
{
    reset_test_hooks();
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX];
    fresh_dir("scrub_days", base, sizeof base);

    atomic_store(&g_fake_wall, 1700000000ull);
    tierscrub_set_wall_fn(fake_wall_cb, NULL);
    tierscrub_set_test_pause(true);
    /* No interval_ms override: uses real TIERS scrub_days (default 14 days)! */
    tierscrub_set_test_opts(0, 0, 1);

    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db);
    tsdb_sid sid = 0;
    CHECK(tsdb_series_create(db, "days.s0", TSDB_FLOAT64, NULL, &sid) == 0);
    insert_and_settle(db, sid, 3, 500);

    eng *e = tsdb_test_engine(db, 0);
    tier *t = e->tier;
    CHECK(tierctl_scrub_days(t) == 14);

    tierscrub_set_test_pause(false);
    tierctl_wake(t);
    for (int i = 0; i < 6; i++)
        CHECK(tierscrub_wait_pass(t, i, 1, 15000) == 0);

    /* Advance fake wall clock by 10 days (< 14 days): no store may start pass 2 yet, and oldest_age_s reflects 10d. */
    atomic_fetch_add(&g_fake_wall, 10ull * 86400ull);
    tierctl_wake(t);
    usleep(150000);
    tierscrub_snapshot snap;
    CHECK(tierscrub_get_snapshot(t, &snap));
    CHECK(snap.has_oldest_age && snap.oldest_age_s >= 10ull * 86400ull);
    for (int i = 0; i < 6; i++)
        CHECK(snap.stores[i].passes_done == 1);

    /* Advance fake wall clock by 5 more days (total 15 >= 14 days): all 6 stores complete pass 2! */
    atomic_fetch_add(&g_fake_wall, 5ull * 86400ull);
    tierctl_wake(t);
    for (int i = 0; i < 6; i++)
        CHECK(tierscrub_wait_pass(t, i, 2, 15000) == 0);

    CHECK(tsdb_close(db) == 0);
    reset_test_hooks();
    printf("test_tierscrub: oldest_first_and_scrub_days ok\n");
}

/* ---------------------------------------------------------------- 8. Read-only follower skips scrub */
static void test_follower_skips_scrub(void)
{
    reset_test_hooks();
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX];
    fresh_dir("follower_skip", base, sizeof base);

    tierscrub_set_test_pause(true);
    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db);
    tsdb_sid sid = 0;
    CHECK(tsdb_series_create(db, "fol.s0", TSDB_FLOAT64, NULL, &sid) == 0);
    insert_and_settle(db, sid, 2, 500);
    CHECK(tsdb_close(db) == 0);

    /* Ensure no SCRUB files exist yet, unpause scrub globally, and open a read-only follower. */
    for (int i = 0; i < 6; i++) {
        char sp[PATH_MAX + 32];
        join_path(sp, sizeof sp, stores[i], TIERSCRUB_FILE);
        CHECK(access(sp, F_OK) != 0);
    }
    tierscrub_set_test_pause(false);
    tierscrub_set_test_opts(20, 0, 1);

    tsdb_options fo;
    tsdb_options_init(&fo);
    fo.follow = 1;
    fo.read_only = 1;
    tsdb *fdb = NULL;
    CHECK(tsdb_open(dbdir, &fo, &fdb) == 0 && fdb != NULL);
    eng *fe = tsdb_test_engine(fdb, 0);
    CHECK(fe != NULL && fe->tier != NULL && fe->tier->read_only);
    tierscrub_snapshot snap;
    CHECK(!tierscrub_get_snapshot(fe->tier, &snap));

    usleep(150000);
    for (int i = 0; i < 6; i++) {
        char sp[PATH_MAX + 32];
        join_path(sp, sizeof sp, stores[i], TIERSCRUB_FILE);
        CHECK(access(sp, F_OK) != 0);
    }
    verify_all_rows(fdb, sid, 2, 500);
    CHECK(tsdb_close(fdb) == 0);
    reset_test_hooks();
    printf("test_tierscrub: follower_skips_scrub ok\n");
}

/* ---------------------------------------------------------------- 9. CLI tier status & tsdbd /metrics */
static int http_get_metrics(int port, char *buf, size_t cap)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof sa);
    sa.sin_family = AF_INET;
    sa.sin_port = htons((uint16_t)port);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr *)&sa, sizeof sa) != 0) {
        close(fd);
        return -1;
    }
    const char *req = "GET /metrics HTTP/1.0\r\nHost: localhost\r\n\r\n";
    if (write(fd, req, strlen(req)) != (ssize_t)strlen(req)) {
        close(fd);
        return -1;
    }
    size_t got = 0;
    while (got + 1 < cap) {
        ssize_t r = read(fd, buf + got, cap - 1 - got);
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0)
            break;
        got += (size_t)r;
    }
    buf[got] = '\0';
    close(fd);
    return (int)got;
}

static void test_status_and_metrics(void)
{
    reset_test_hooks();
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX];
    fresh_dir("status_metrics", base, sizeof base);

    tierscrub_set_test_pause(true);
    tierscrub_set_test_opts(50, 0, 1);

    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db);
    tsdb_sid sid = 0;
    CHECK(tsdb_series_create(db, "met.s0", TSDB_FLOAT64, NULL, &sid) == 0);
    /* 45,000 rows/part -> full 4+2 striping so every store deterministically holds chunks for every partition. */
    insert_and_settle(db, sid, 2, 45000);

    eng *e = tsdb_test_engine(db, 0);
    tier *t = e->tier;
    ec_info ecs[8];
    CHECK(list_live_ec(e, ecs, 8) == 2);
    int st_dmg = chunk_be_place(t->be, ecs[0].oid, 0);
    char cpath[PATH_MAX];
    chunk_path_of(stores[st_dmg], 0, ecs[0].oid, 0, cpath);
    flip_chunk_cell_byte(cpath, 3);

    tierscrub_set_test_pause(false);
    tierctl_wake(t);
    for (int i = 0; i < 6; i++)
        CHECK(tierscrub_wait_pass(t, i, 1, 15000) == 0);
    CHECK(tsdb_close(db) == 0);

    /* Check `tsdb DIR tier status` (text & JSON). */
    char cmd[10240];
    snprintf(cmd, sizeof cmd,
             "'%s' '%s' tier status --json | python3 -I -c '"
             "import json, sys; d = json.load(sys.stdin); "
             "assert d[\"scrub\"][\"chunks_checked\"] >= 6; "
             "assert d[\"scrub\"][\"damaged\"] >= 1; "
             "assert d[\"scrub\"][\"repaired\"] >= 1; "
             "assert d[\"scrub\"][\"unrepairable\"] == 0; "
             "assert d[\"scrub\"][\"oldest_unscrubbed_s\"] is not None; "
             "assert d[\"damaged_cells\"] == 0; "
             "assert all(s[\"scrub_passes\"] >= 1 and s[\"scrub_chunks_checked\"] >= 1 and s[\"damaged_cells\"] == 0 for s in d[\"stores\"])'",
             CLI, dbdir);
    CHECK(system(cmd) == 0);

    snprintf(cmd, sizeof cmd, "'%s' '%s' tier status | grep -q 'checked='", CLI, dbdir);
    CHECK(system(cmd) == 0);

    /* Start tsdbd with --metrics-port auto and scrape /metrics. */
    char portfile[PATH_MAX * 2], logfile[PATH_MAX + 32], follow_arg[64];
    th_portfile(portfile, sizeof portfile, base);
    join_path(logfile, sizeof logfile, base, "tsdbd.log");
    th_follow_me(follow_arg, sizeof follow_arg);

    pid_t pid = fork();
    CHECK(pid >= 0);
    if (pid == 0) {
        int lfd = open(logfile, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        if (lfd >= 0) {
            dup2(lfd, 1);
            dup2(lfd, 2);
            close(lfd);
        }
        execl(TSDBD, TSDBD, "--dir", dbdir, "--no-auth", "--io", "epoll", "--port", "0", "--port-file", portfile,
              "--metrics-port", "auto", follow_arg, (char *)NULL);
        _exit(127);
    }

    int port = 0, mport = 0;
    pid_t fpid = 0;
    for (int i = 0; i < 200; i++) {
        FILE *pf = fopen(portfile, "r");
        if (pf) {
            int p = 0, fp = 0, mp = 0;
            if (fscanf(pf, "%d %d %d", &p, &fp, &mp) == 3 && p > 0 && mp > 0) {
                port = p;
                fpid = (pid_t)fp;
                mport = mp;
                fclose(pf);
                break;
            }
            fclose(pf);
        }
        int st = 0;
        if (waitpid(pid, &st, WNOHANG) == pid)
            break;
        usleep(50000);
    }
    CHECK(port > 0 && mport > 0 && fpid == pid);
    CHECK(th_owns_port(pid, port));

    char *mbuf = malloc(256 * 1024);
    CHECK(mbuf != NULL);
    int n = http_get_metrics(mport, mbuf, 256 * 1024);
    CHECK(n > 0);
    CHECK(strstr(mbuf, "tsdb_tier_scrub_chunks_checked_total{store=\"0\"}") != NULL);
    CHECK(strstr(mbuf, "tsdb_tier_scrub_damaged_total{store=\"0\"}") != NULL);
    CHECK(strstr(mbuf, "tsdb_tier_scrub_repaired_total{store=\"0\"}") != NULL);
    CHECK(strstr(mbuf, "tsdb_tier_scrub_unrepairable_total{store=\"0\"}") != NULL);
    CHECK(strstr(mbuf, "tsdb_tier_scrub_age_seconds{store=\"0\"}") != NULL);
    CHECK(strstr(mbuf, "tsdb_tier_scrub_oldest_age_seconds ") != NULL);

    /* Verify repaired counter for st_dmg was loaded from SCRUB and exposed >= 1. */
    char want_rep[128];
    snprintf(want_rep, sizeof want_rep, "tsdb_tier_scrub_repaired_total{store=\"%d\"} ", st_dmg);
    const char *p = strstr(mbuf, want_rep);
    CHECK(p != NULL);
    if (p) {
        unsigned long long v = strtoull(p + strlen(want_rep), NULL, 10);
        CHECK(v >= 1);
    }

    free(mbuf);
    int hung = 0;
    th_stop(pid, 5000, &hung);
    CHECK(hung == 0);
    unlink(portfile);

    reset_test_hooks();
    printf("test_tierscrub: status_and_metrics ok\n");
}

/* ---------------------------------------------------------------- 10. Quiesced volume skipped without re-scanning */
typedef struct qvol_hook_ctx {
    pthread_mutex_t mu;
    int st0_vol0;
    int st0_vol1;
} qvol_hook_ctx;

static void qvol_obj_hook(void *vctx, int store, uint32_t vol, const uint8_t oid[16],
                          const struct chunk_heal_report *delta)
{
    (void)oid, (void)delta;
    if (store != 0)
        return;
    qvol_hook_ctx *c = vctx;
    pthread_mutex_lock(&c->mu);
    if (vol == 0)
        c->st0_vol0++;
    else if (vol == 1)
        c->st0_vol1++;
    pthread_mutex_unlock(&c->mu);
}

static void test_quiesced_vol_no_rescan(void)
{
    reset_test_hooks();
    char base[PATH_MAX], dbdir[PATH_MAX], vol1dir[PATH_MAX], stores[6][PATH_MAX];
    fresh_dir("quiesced_vol", base, sizeof base);
    join_path(vol1dir, sizeof vol1dir, base, "vol1");

    tierscrub_set_test_pause(true);
    tierscrub_set_test_opts(50, 0, 1);

    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db);
    uint32_t v1 = 0;
    CHECK(tsdb_volume_add(db, vol1dir, &v1) == 0 && v1 == 1);

    tsdb_sid s0 = 0, s1 = 0;
    CHECK(tsdb_series_create(db, "qv.v0", TSDB_FLOAT64, NULL, &s0) == 0);
    CHECK(tsdb_series_create(db, "qv.v1", TSDB_FLOAT64, vol1dir, &s1) == 0);
    insert_and_settle(db, s0, 2, 45000);
    insert_and_settle(db, s1, 2, 45000);

    eng *e0 = tsdb_test_engine(db, 0);
    tier *t = e0->tier;

    /* Quiesce volume 1 (e.g. during an NFS outage): volume 0 must still be scrubbed ONCE and NEVER re-scanned on
     * retry while volume 1 stays quiesced. */
    CHECK(tsdb_volume_quiesce(db, 1) == 0);

    qvol_hook_ctx qc = {0};
    pthread_mutex_init(&qc.mu, NULL);
    tierscrub_set_obj_hook(qvol_obj_hook, &qc);

    tierscrub_set_test_pause(false);
    tierctl_wake(t);

    /* Wait until store 0 has finished volume 0 (2 objects). */
    for (int i = 0; i < 200; i++) {
        pthread_mutex_lock(&qc.mu);
        int v0n = qc.st0_vol0;
        pthread_mutex_unlock(&qc.mu);
        if (v0n >= 2)
            break;
        usleep(25000);
    }
    pthread_mutex_lock(&qc.mu);
    int v0_before = qc.st0_vol0;
    int v1_before = qc.st0_vol1;
    pthread_mutex_unlock(&qc.mu);
    CHECK(v0_before == 2 && v1_before == 0);

    /* Let retries tick for 600ms while vol 1 remains quiesced: vol 0 must NOT be re-scanned! */
    usleep(600000);
    pthread_mutex_lock(&qc.mu);
    int v0_after_wait = qc.st0_vol0;
    int v1_after_wait = qc.st0_vol1;
    pthread_mutex_unlock(&qc.mu);
    CHECK(v0_after_wait == 2 && v1_after_wait == 0);

    /* Now resume vol 1: store 0 must scrub vol 1 (2 objects) and finish pass 1 WITHOUT re-scanning vol 0! */
    tierscrub_set_test_opts(60000, 0, 1); /* large interval so only pass 1 runs on store 0 */
    CHECK(tsdb_volume_resume(db, 1) == 0);
    tierctl_wake(t);
    CHECK(tierscrub_wait_pass(t, 0, 1, 15000) == 0);

    pthread_mutex_lock(&qc.mu);
    CHECK(qc.st0_vol0 == 2);
    CHECK(qc.st0_vol1 == 2);
    pthread_mutex_unlock(&qc.mu);

    tierscrub_set_obj_hook(NULL, NULL);
    CHECK(tsdb_close(db) == 0);
    pthread_mutex_destroy(&qc.mu);
    reset_test_hooks();
    printf("test_tierscrub: quiesced_vol_no_rescan ok\n");
}

/* ---------------------------------------------------------------- 11. Store DOWN at open preserves SCRUB history */
static void test_down_at_open_keeps_history(void)
{
    reset_test_hooks();
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX], away[PATH_MAX];
    fresh_dir("down_at_open", base, sizeof base);
    join_path(dbdir, sizeof dbdir, base, "db");
    join_path(away, sizeof away, base, "st5.away");
    CHECK(sh("mkdir -p '%s'", dbdir) == 0);
    for (int i = 0; i < 6; i++) {
        char sub[16];
        snprintf(sub, sizeof sub, "st%d", i);
        join_path(stores[i], sizeof stores[i], base, sub);
        CHECK(sh("mkdir -p '%s'", stores[i]) == 0);
    }

    char spec[PATH_MAX * 7];
    snprintf(spec, sizeof spec, "k=4,m=2,dirs=%s:%s:%s:%s:%s:%s", stores[0], stores[1], stores[2], stores[3], stores[4],
             stores[5]);
    setenv("TSDB_TEST_TIER", spec, 1);
    setenv("TSDB_TEST_TIER_I_KNOW", "1", 1);
    setenv("TSDB_TEST_TIER_SCRUB", "enable=1", 1);

    tierscrub_set_test_pause(true);
    tierscrub_set_test_opts(50, 0, 1);

    tsdb *db = NULL;
    CHECK(open_db(dbdir, &db) == 0 && db != NULL);
    if (!db)
        goto out;
    tsdb_sid sid = 0;
    CHECK(tsdb_series_create(db, "dao.s0", TSDB_FLOAT64, NULL, &sid) == 0);
    insert_and_settle(db, sid, 2, 45000);

    eng *e = tsdb_test_engine(db, 0);
    tier *t = e->tier;
    tierscrub_set_test_pause(false);
    tierctl_wake(t);
    for (int i = 0; i < 6; i++)
        CHECK(tierscrub_wait_pass(t, i, 1, 15000) == 0);
    CHECK(tsdb_close(db) == 0);
    db = NULL;

    tierscrub_store_stats pre;
    CHECK(tierscrub_read_store_file(stores[5], &pre) == 0);
    CHECK(pre.has_data && pre.passes_done >= 1 && pre.chunks_checked >= 2);

    /* Hide store 5 so it is DOWN_MISSING at reopen; restore and attach via chunk_be_attach (health attach path). */
    CHECK(rename(stores[5], away) == 0);
    tierscrub_set_test_pause(true);
    CHECK(open_db(dbdir, &db) == 0 && db != NULL);
    if (!db) {
        (void)rename(away, stores[5]);
        goto out;
    }
    e = tsdb_test_engine(db, 0);
    t = e->tier;
    CHECK(chunk_be_store_state(t->be, 5) == CHUNK_STORE_DOWN_MISSING);

    CHECK(rename(away, stores[5]) == 0);
    CHECK(chunk_be_attach(t->be, 5, stores[5], NULL, NULL, NULL) == 0);
    CHECK(chunk_be_store_state(t->be, 5) == CHUNK_STORE_UP);

    tierscrub_set_test_pause(false);
    tierctl_wake(t);
    CHECK(tierscrub_wait_pass(t, 5, pre.passes_done + 1, 15000) == 0);
    CHECK(tsdb_close(db) == 0);
    db = NULL;

    tierscrub_store_stats post;
    CHECK(tierscrub_read_store_file(stores[5], &post) == 0);
    CHECK(post.passes_done >= pre.passes_done + 1);
    CHECK(post.chunks_checked >= pre.chunks_checked + 2);

out:
    unsetenv("TSDB_TEST_TIER");
    unsetenv("TSDB_TEST_TIER_I_KNOW");
    unsetenv("TSDB_TEST_TIER_SCRUB");
    reset_test_hooks();
    printf("test_tierscrub: down_at_open_keeps_history ok\n");
}

/* ---------------------------------------------------------------- 12. Damaged chunk with unavailable peers logged */
static void test_unavailable_peer_counts_and_logs(void)
{
    reset_test_hooks();
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX];
    fresh_dir("unavail_peer", base, sizeof base);

    tierscrub_set_test_pause(true);
    tierscrub_set_test_opts(50, 0, 1);

    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db);
    tsdb_sid sid = 0;
    CHECK(tsdb_series_create(db, "up.s0", TSDB_FLOAT64, NULL, &sid) == 0);
    insert_and_settle(db, sid, 2, 45000);

    eng *e = tsdb_test_engine(db, 0);
    tier *t = e->tier;
    ec_info ecs[8];
    CHECK(list_live_ec(e, ecs, 8) == 2);
    int st_dmg = chunk_be_place(t->be, ecs[0].oid, 0);
    char cpath[PATH_MAX];
    chunk_path_of(stores[st_dmg], 0, ecs[0].oid, 0, cpath);
    flip_chunk_cell_byte(cpath, 9);

    /* Take 3 peer stores DOWN so < k=4 survivors are UP while peers are transiently away (-> unavailable, not silent). */
    int downed = 0;
    for (int i = 0; i < 6 && downed < 3; i++) {
        if (i == st_dmg)
            continue;
        CHECK(chunk_be_set_down(t->be, i, CHUNK_STORE_DOWN_STALLED) == 0);
        downed++;
    }
    CHECK(downed == 3);

    char errbuf[16384];
    cap_begin();
    tierscrub_set_test_pause(false);
    tierctl_wake(t);
    CHECK(tierscrub_wait_pass(t, st_dmg, 1, 15000) == 0);
    cap_end(errbuf, sizeof errbuf);

    tierscrub_snapshot snap;
    CHECK(tierscrub_get_snapshot(t, &snap));
    CHECK(snap.stores[st_dmg].damaged >= 1);
    CHECK(snap.stores[st_dmg].repaired == 0);
    CHECK(snap.stores[st_dmg].unrepairable == 0);
    CHECK(strstr(errbuf, "repair deferred (peer store unavailable)") != NULL);

    CHECK(tsdb_close(db) == 0);
    reset_test_hooks();
    printf("test_tierscrub: unavailable_peer_counts_and_logs ok\n");
}

#endif /* TSDB_EC */

int main(int argc, char **argv)
{
    const char *scratch = "/tmp";
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mutant") == 0 && i + 1 < argc) {
            mutant_name = argv[++i];
        } else {
            scratch = argv[i];
        }
    }

#ifndef TSDB_EC
    (void)scratch;
    printf("test_tierscrub: skipped (NO_EC=1)\n");
    return 0;
#else
    if (mutant_name) {
        int m = tierscrub_mutant_parse(mutant_name);
        if (m < 0) {
            fprintf(stderr, "test_tierscrub: unknown mutant '%s'\n", mutant_name);
            return 2;
        }
        tierscrub_mutant = m;
    }

    if (!realpath(argv[0], SELF))
        snprintf(SELF, sizeof SELF, "%s", argv[0]);
    char dirbuf[PATH_MAX];
    snprintf(dirbuf, sizeof dirbuf, "%s", SELF);
    char *sl = strrchr(dirbuf, '/');
    if (sl)
        *sl = '\0';
    else
        snprintf(dirbuf, sizeof dirbuf, ".");
    snprintf(CLI, sizeof CLI, "%s/tsdb", dirbuf);
    snprintf(TSDBD, sizeof TSDBD, "%s/tsdbd", dirbuf);

    int n = snprintf(S, sizeof S, "%s/test_tierscrub.%d", scratch, (int)getpid());
    if (n <= 0 || (size_t)n >= sizeof S)
        return 2;
    if (sh("rm -rf '%s' && mkdir -p '%s'", S, S) != 0)
        return 2;

    test_data_and_parity_rot();
    if (mutant_name && strcmp(mutant_name, "scrubnorepair") == 0) {
        /* Early exit once scrubnorepair has been exercised (faster mutant loop). */
        sh("rm -rf '%s'", S);
        return fails != 0;
    }
    test_pacing();
    if (mutant_name && strcmp(mutant_name, "scrubnopace") == 0) {
        sh("rm -rf '%s'", S);
        return fails != 0;
    }
    test_single_copy_report_only();
    test_nonlive_not_repaired();
    test_midpass_gc_no_false_damage();
    test_cursor_restart_resume();
    test_pause_during_rebuild_and_shrink();
    test_oldest_first_and_scrub_days();
    test_follower_skips_scrub();
    test_status_and_metrics();
    test_quiesced_vol_no_rescan();
    test_down_at_open_keeps_history();
    test_unavailable_peer_counts_and_logs();

    sh("rm -rf '%s'", S);
    if (fails == 0)
        printf("test_tierscrub: ALL PASSED\n");
    else
        fprintf(stderr, "test_tierscrub: %d FAILURE(S)\n", fails);
    return fails != 0;
#endif
}
