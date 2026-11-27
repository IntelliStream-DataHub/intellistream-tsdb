/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Tests for local tier salvage past m LOST stores (R15 r15-salvage; src/tiersalv.c, src/ecswap.c, src/ltab.c;
 * docs/design-cluster-data.md E15, §15.7; PLAN.md D11):
 *   1. e15_and_salvage: 4+2 (6 stores) with 3 stores LOST: reads of old key fail with TSDB_ERR_IO naming partition,
 *      statistics still answer; salvage keeps young parts above the retired .ec objects intact, removes old rows,
 *      updates statistics, logs rows lost per partition, updates salvage counters, and ecgc cleans surviving chunks
 *      on UP stores (LOST chunks are clean no-ops). Also verifies refusal when any store is DOWN (not LOST) without
 *      --force (catches mutant `salvagedown`), and success with --force.
 *   2. latest_and_warmer: verifies latest table matches slow-path oracle (tests/test_latest.c's check) across series
 *      with rows only in lost .ec, older surviving .part + newer lost .ec, young .part above lost .ec, and tombstone
 *      in lost .ec; verifies warmer refills unknown entries; and deterministically catches mutant `salvagelatest`
 *      (no lgen bump) by pausing the warmer at LH_WARM_BEFORE_STORE across salvage.
 *   3. recover_hand: hand-planted v5 `X` intents (torn -> rollback, complete 0/half/all renamed -> roll forward,
 *      never resurrecting retired parts, corrupt/rule-breaking intents -> TSDB_ERR_CORRUPT).
 *   4. crashfs_sweep: crashfs power loss at every syscall step of tiersalv_eng (multiple .ec parts + young .part
 *      above them), verifying atomic pre/post recovery, never resurrecting retired parts, young part intact, and
 *      latest table matching slow path.
 *   5. small_k_selective: small k'=1, m=2 objects on 6 stores: only objects with > m positions on LOST stores are
 *      retired; surviving .ec parts remain listed and readable bit-exact.
 *   6. cli_salvage: offline tiersalv_cli on a 6-store DB with 3 stores LOST.
 */
#define _GNU_SOURCE
#include "chunk.h"
#include "chunk_priv.h"
#include "eccache.h"
#include "ecgc.h"
#include "ecpart.h"
#include "ecswap.h"
#include "internal.h"
#include "tier.h"
#include "tierctl.h"
#include "tiers.h"
#include "tiersalv.h"
#include "tsdb.h"
#include "watchdog.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define CHECK(cond)                                                                      \
    do {                                                                                 \
        if (!(cond)) {                                                                   \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);              \
            exit(1);                                                                     \
        }                                                                                \
    } while (0)

#ifndef TSDB_EC

int main(int argc, char **argv)
{
    (void)argc;
    (void)argv;
    printf("test_tiersalv: skipped (NO_EC=1)\n");
    return 0;
}

#else

static char S[512];
static char SELF[PATH_MAX];
static char SHIM[PATH_MAX];
static bool quick;
static const char *mut_name = "none";

static int sh(const char *fmt, ...)
{
    char cmd[PATH_MAX * 4];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(cmd, sizeof cmd, fmt, ap);
    va_end(ap);
    if (n <= 0 || (size_t)n >= sizeof cmd)
        return -1;
    int st = system(cmd);
    return (st >= 0 && WIFEXITED(st)) ? WEXITSTATUS(st) : -1;
}

static void fresh_dir(const char *tag, char *out, size_t cap)
{
    static int seq;
    snprintf(out, cap, "%s/tsalv_%s_%d_%d", S, tag, (int)getpid(), ++seq);
    CHECK(sh("rm -rf '%s' && mkdir -p '%s'", out, out) == 0);
}

static int cap_saved = -1;
static char cap_path[PATH_MAX];

static void cap_begin(void)
{
    static int cseq;
    fflush(stderr);
    snprintf(cap_path, sizeof cap_path, "%s/cap_%d_%d.log", S, (int)getpid(), ++cseq);
    int fd = open(cap_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    CHECK(fd >= 0);
    cap_saved = dup(2);
    CHECK(cap_saved >= 0);
    CHECK(dup2(fd, 2) >= 0);
    close(fd);
}

static void cap_end(char *buf, size_t cap)
{
    fflush(stderr);
    if (cap_saved >= 0) {
        dup2(cap_saved, 2);
        close(cap_saved);
        cap_saved = -1;
    }
    if (buf && cap) {
        buf[0] = '\0';
        int fd = open(cap_path, O_RDONLY);
        if (fd >= 0) {
            ssize_t r = read(fd, buf, cap - 1);
            if (r > 0)
                buf[r] = '\0';
            close(fd);
        }
    }
    unlink(cap_path);
}

static bool exists_in(const char *dir, const char *name)
{
    char p[PATH_MAX + 256];
    snprintf(p, sizeof p, "%s/%s", dir, name);
    return access(p, F_OK) == 0;
}

static int count_names(const char *dir, const char *prefix, const char *suffix)
{
    DIR *d = opendir(dir);
    if (!d)
        return -1;
    int c = 0;
    size_t pl = prefix ? strlen(prefix) : 0;
    size_t sl = suffix ? strlen(suffix) : 0;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        size_t l = strlen(e->d_name);
        if (pl && (l < pl || strncmp(e->d_name, prefix, pl) != 0))
            continue;
        if (sl && (l < sl || strcmp(e->d_name + l - sl, suffix) != 0))
            continue;
        c++;
    }
    closedir(d);
    return c;
}

static int count_store_chunks(const char *store_dir)
{
    char vdir[PATH_MAX + 32];
    snprintf(vdir, sizeof vdir, "%s/s0", store_dir);
    int n = count_names(vdir, "o", ".chunk");
    return n < 0 ? 0 : n;
}

/* Simulates post-keep_s expiration of local settled_*.part copies so salvage exercises retirement of lost .ec parts. */
static void remove_settled_files(const char *dir)
{
    DIR *d = opendir(dir);
    CHECK(d != NULL);
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        size_t l = strlen(e->d_name);
        if (strncmp(e->d_name, "settled_", 8) == 0 && l > 13 && strcmp(e->d_name + l - 5, ".part") == 0) {
            char p[PATH_MAX + 256];
            snprintf(p, sizeof p, "%s/%s", dir, e->d_name);
            CHECK(unlink(p) == 0);
        }
    }
    closedir(d);
    int dfd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd >= 0) {
        (void)fsync(dfd);
        close(dfd);
    }
}

/* ------------------------------------------------------------ query / oracle helpers */

typedef struct rowset {
    int64_t *ts;
    uint64_t *x;
    uint8_t *nul;
    size_t n, cap;
} rowset;

static void rs_free(rowset *r)
{
    free(r->ts);
    free(r->x);
    free(r->nul);
    memset(r, 0, sizeof *r);
}

static uint64_t slot_of(tsdb_type t, const tsdb_values *c, size_t i)
{
    switch (t) {
    case TSDB_FLOAT32: { uint32_t b; memcpy(&b, (const float *)c->values + i, 4); return b; }
    case TSDB_FLOAT64: { uint64_t b; memcpy(&b, (const double *)c->values + i, 8); return b; }
    case TSDB_INT64: return (uint64_t)((const int64_t *)c->values)[i];
    case TSDB_BOOL: return ((const uint8_t *)c->values)[i];
    default: return ((const uint32_t *)c->values)[i];
    }
}

static int collect_cb(void *ctx, const int64_t *ts, const tsdb_values *v, size_t n)
{
    rowset *r = ctx;
    if (r->n + n > r->cap) {
        size_t nc = (r->n + n) * 2;
        int64_t *nts = realloc(r->ts, nc * sizeof *nts);
        uint64_t *nx = realloc(r->x, nc * sizeof *nx);
        uint8_t *nn = realloc(r->nul, nc);
        CHECK(nts && nx && nn);
        r->ts = nts;
        r->x = nx;
        r->nul = nn;
        r->cap = nc;
    }
    for (size_t i = 0; i < n; i++) {
        r->ts[r->n + i] = ts[i];
        bool known = !v->valid || ((v->valid[i >> 3] >> (i & 7)) & 1);
        r->nul[r->n + i] = !known;
        r->x[r->n + i] = known ? slot_of(v->type, v, i) : 0;
    }
    r->n += n;
    return 0;
}

static uint64_t result_bits(const tsdb_latest_result *o)
{
    switch (o->type) {
    case TSDB_FLOAT32: { uint32_t b; memcpy(&b, &o->v.f32, 4); return b; }
    case TSDB_FLOAT64: { uint64_t b; memcpy(&b, &o->v.f64, 8); return b; }
    case TSDB_INT64: return (uint64_t)o->v.i64;
    case TSDB_BOOL: return o->v.b;
    default: return o->v.state;
    }
}

static bool pick_all(void *ctx, uint32_t vol, int64_t partition, uint32_t level, uint64_t rows)
{
    (void)ctx;
    (void)vol;
    (void)partition;
    (void)level;
    (void)rows;
    return true;
}

static bool pick_heavy(void *ctx, uint32_t vol, int64_t partition, uint32_t level, uint64_t rows)
{
    (void)ctx;
    (void)vol;
    (void)partition;
    (void)level;
    return rows >= 10000;
}

/* Merges the last two parts of `partition` (keeping any earlier lower-block part unmerged so tombstones in the
 * merged higher-block part are preserved alongside data granules, matching tests/test_ecpart.c). */
static void merge_last_two_in_partition(tsdb *db, int64_t partition)
{
    eng *e = tsdb_test_engine(db, 0);
    CHECK(e != NULL);
    part *sel[2] = {NULL, NULL};
    pthread_mutex_lock(&e->lock);
    size_t nfound = 0;
    for (size_t i = 0; i < e->nparts; i++) {
        part *p = e->parts[i];
        if (p->partition != partition || p->dropped)
            continue;
        if (nfound < 2) {
            sel[nfound++] = p;
        } else {
            sel[0] = sel[1];
            sel[1] = p;
        }
    }
    CHECK(nfound == 2);
    for (size_t i = 0; i < 2; i++) {
        sel[i]->merging = true;
        sel[i]->merge_len_gen = atomic_load(&e->ret_len_gen);
        part_ref(sel[i]);
    }
    pthread_mutex_unlock(&e->lock);
    part *out = NULL;
    int rc = merge_parts(e, sel, 2, &out);
    CHECK(rc == 0 && out != NULL && out->nt > 0 && out->ng > 0);
    pthread_mutex_lock(&e->lock);
    merge_commit_locked(e, sel, 2, out, rc);
    pthread_mutex_unlock(&e->lock);
    CHECK(manifest_publish(e) == 0);
    for (size_t i = 0; i < 2; i++)
        part_unref(sel[i]);
}

/* Compares tsdb_latest() against the slow-path tsdb_query_values() scan oracle (tests/test_latest.c's invariant). */
static void check_latest_matches_slow(tsdb *db, const tsdb_sid *ids, size_t nids)
{
    tsdb_latest_result *got = calloc(nids, sizeof *got);
    CHECK(got != NULL);
    CHECK(tsdb_latest(db, ids, nids, NULL, got) == 0);
    for (size_t i = 0; i < nids; i++) {
        rowset r = {0};
        CHECK(tsdb_query_values(db, ids[i], TSDB_TS_MIN, TSDB_TS_MAX, NULL, collect_cb, &r) == 0);
        int want_kind = TSDB_LATEST_NONE;
        int64_t want_ts = 0;
        uint64_t want_x = 0;
        for (size_t k = r.n; k-- > 0;) {
            if (!r.nul[k]) {
                want_kind = TSDB_LATEST_VALUE;
                want_ts = r.ts[k];
                want_x = r.x[k];
                break;
            }
        }
        if (got[i].status != 0 || got[i].kind != want_kind ||
            (want_kind == TSDB_LATEST_VALUE && (got[i].ts != want_ts || result_bits(&got[i]) != want_x))) {
            fprintf(stderr,
                    "FAIL latest mismatch sid=%" PRIu64 ": table(status=%d kind=%d ts=%" PRId64 " x=0x%" PRIx64
                    ") vs slow(kind=%d ts=%" PRId64 " x=0x%" PRIx64 ")\n",
                    (uint64_t)ids[i], got[i].status, got[i].kind, got[i].ts, result_bits(&got[i]), want_kind, want_ts,
                    want_x);
            rs_free(&r);
            free(got);
            exit(1);
        }
        rs_free(&r);
    }
    free(got);
}

static int query_f64_count(tsdb *db, tsdb_sid s, int64_t start, int64_t end, rowset *out)
{
    memset(out, 0, sizeof *out);
    return tsdb_query_values(db, s, start, end, NULL, collect_cb, out);
}

/* ------------------------------------------------------------ 6-store 4+2 DB setup */

static void setup_6store_db(const char *base, char dbdir[PATH_MAX], char stores[6][PATH_MAX], tsdb **out_db,
                            int obs_life_s)
{
    snprintf(dbdir, PATH_MAX, "%s/db", base);
    CHECK(mkdir(dbdir, 0755) == 0);
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.partition_classes = 0;
    o.obsolete_part_lifetime_s = obs_life_s;
    tsdb *db = NULL;
    CHECK(tsdb_open(dbdir, &o, &db) == 0 && db);
    for (int i = 0; i < 6; i++) {
        snprintf(stores[i], PATH_MAX, "%s/st%d", base, i);
        CHECK(tsdb_tier_add(db, stores[i], "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);
    }
    CHECK(tsdb_close(db) == 0);

    tierctl_set_poll_ms(50);
    CHECK(tsdb_open(dbdir, &o, out_db) == 0 && *out_db);
}

static void declare_stores_lost(const char *dbdir, tier *t, const int *indices, size_t n)
{
    int dfd = open(dbdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    CHECK(dfd >= 0);
    tiers_view tv;
    tiers_view_init(&tv);
    CHECK(tiers_load(dfd, false, false, &tv) == 0);
    for (size_t i = 0; i < n; i++) {
        int idx = indices[i];
        CHECK(idx >= 0 && (size_t)idx < tv.nstores);
        const uint8_t *id = tv.stores[idx].state == TIERS_STATE_REPLACED_BY ? tv.stores[idx].replaced_by
                                                                            : tv.stores[idx].id;
        CHECK(tiers_append_state(dfd, true, id, TIERS_STATE_LOST, 0, false, NULL, NULL) == 0);
    }
    tiers_view_free(&tv);
    close(dfd);

    if (t) {
        tierctl_wake(t);
        CHECK(tierctl_wait_idle(t, 10000) == 0);
        chunk_backend *be = tier_backend(t);
        CHECK(be != NULL);
        for (size_t i = 0; i < n; i++)
            CHECK(chunk_be_store_is_lost(be, indices[i]));
    }
}

static void write_f64_varied(tsdb *db, tsdb_sid s, int64_t t0, int64_t step, size_t n, double base_val)
{
    int64_t *ts = malloc(n * sizeof *ts);
    double *vs = malloc(n * sizeof *vs);
    CHECK(ts && vs);
    for (size_t i = 0; i < n; i++) {
        ts[i] = t0 + (int64_t)i * step;
        uint64_t h = (uint64_t)i * 0x9e3779b97f4a7c15ull;
        vs[i] = base_val + (double)(h & 0xfffff) * 1e-3 + (double)((h >> 20) & 0xfff) * 1e-6;
    }
    tsdb_values col = {.type = TSDB_FLOAT64, .values = vs};
    CHECK(tsdb_insert_series_values(db, s, ts, &col, n) == 0);
    free(ts);
    free(vs);
}

static void write_f64(tsdb *db, tsdb_sid s, int64_t t0, int64_t step, size_t n, double base_val)
{
    int64_t *ts = malloc(n * sizeof *ts);
    double *vs = malloc(n * sizeof *vs);
    CHECK(ts && vs);
    for (size_t i = 0; i < n; i++) {
        ts[i] = t0 + (int64_t)i * step;
        vs[i] = base_val + (double)i * 0.5;
    }
    tsdb_values col = {.type = TSDB_FLOAT64, .values = vs};
    CHECK(tsdb_insert_series_values(db, s, ts, &col, n) == 0);
    free(ts);
    free(vs);
}

/* ------------------------------------------------------------ 1. E15 + salvage + DOWN refusal + ecgc */

static void case_e15_and_salvage(void)
{
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX];
    fresh_dir("e15salv", base, sizeof base);
    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db, 0);

    const int64_t day_ms = 86400000LL;
    tsdb_sid s0 = 0, s1 = 0;
    CHECK(tsdb_series_create(db, "salv_s0", TSDB_FLOAT64, NULL, &s0) == 0);
    CHECK(tsdb_series_create(db, "salv_s1", TSDB_FLOAT64, NULL, &s1) == 0);

    /* Write 20,000 varied rows per series into partitions 0 and 1 (> 256 KiB per part -> k'=4, m=2 across all 6 stores). */
    write_f64_varied(db, s0, 0, 1000, 20000, 10.0);
    write_f64_varied(db, s1, 0, 1000, 20000, 100.0);
    write_f64_varied(db, s0, day_ms, 1000, 20000, 20.0);
    write_f64_varied(db, s1, day_ms, 1000, 20000, 200.0);
    CHECK(tsdb_flush(db) == 0);
    size_t nset = 0;
    CHECK(tsdb_test_settle(db, pick_all, NULL, &nset) == 0 && nset >= 2);
    int p0_ec = count_names(dbdir, "p0_", ".ec");
    int p1_ec = count_names(dbdir, "p1_", ".ec");
    CHECK(p0_ec >= 1 && p1_ec >= 1);
    remove_settled_files(dbdir);

    /* Write a young .part in partition 0 ABOVE the settled .ec object (must survive salvage untouched!). */
    write_f64(db, s0, 40000000LL, 1000, 10, 5000.0);
    write_f64(db, s1, 40000000LL, 1000, 10, 6000.0);
    CHECK(tsdb_flush(db) == 0);
    CHECK(count_names(dbdir, "p0_", ".part") == 1);

    int chunks_before = count_store_chunks(stores[3]) + count_store_chunks(stores[4]) + count_store_chunks(stores[5]);
    CHECK(chunks_before >= 6);

    tier *t = tier_find_by_dir(dbdir);
    CHECK(t != NULL);

    /* Declare 3 stores (0, 1, 2) permanently LOST (> m=2). */
    int lost3[3] = {0, 1, 2};
    declare_stores_lost(dbdir, t, lost3, 3);

    /* Drop granule cache: reads of old key fail with TSDB_ERR_IO naming partition; statistics STILL answer! */
    (void)tsdb_test_ec_retries(db, true);
    eccache_reopen_all(t);

    rowset r = {0};
    char log[4096];
    cap_begin();
    int rrc = query_f64_count(db, s0, 0, 20000000LL, &r);
    if (rrc == 0) {
        rs_free(&r);
        rrc = query_f64_count(db, s1, 0, 20000000LL, &r);
    }
    cap_end(log, sizeof log);
    rs_free(&r);
    CHECK(rrc == TSDB_ERR_IO);
    CHECK(strstr(log, "partition 0") != NULL);

    /* Statistics query MUST answer from the intact local .ec index! */
    tsdb_bucket *bk = NULL;
    size_t nbk = 0;
    CHECK(tsdb_query_agg(db, s0, 0, 2 * day_ms, day_ms, &bk, &nbk) == 0);
    CHECK(nbk == 2 && bk && bk[0].count == 20010 && bk[1].count == 20000);
    tsdb_free(bk);
    bk = NULL;

    /* Salvage partition 0 ONLY (stores 3,4,5 are UP, force=0). Capture stderr to verify rows-lost log line! */
    cap_begin();
    CHECK(tsdb_tier_salvage(db, 0, 0, 0) == 0);
    cap_end(log, sizeof log);
    CHECK(strstr(log, "partition 0") != NULL && strstr(log, "40000 rows lost") != NULL);

    /* Check salvage counters */
    tiersalv_counters sc;
    tiersalv_get_counters(t, &sc);
    CHECK(sc.partitions == 1 && sc.parts == (uint64_t)p0_ec && sc.rows_lost == 40000);

    /* Partition 0: .ec retired, young .part stays intact, old rows gone, young rows exact! */
    CHECK(count_names(dbdir, "p0_", ".ec") == 0);
    CHECK(count_names(dbdir, "p0_", ".part") == 1);
    CHECK(query_f64_count(db, s0, 0, day_ms, &r) == 0);
    CHECK(r.n == 10 && r.ts[0] == 40000000LL && r.ts[9] == 40009000LL);
    double v0 = 0;
    memcpy(&v0, &r.x[0], 8);
    CHECK(v0 == 5000.0);
    rs_free(&r);
    CHECK(query_f64_count(db, s1, 0, day_ms, &r) == 0);
    CHECK(r.n == 10 && r.ts[0] == 40000000LL);
    memcpy(&v0, &r.x[0], 8);
    CHECK(v0 == 6000.0);
    rs_free(&r);

    /* Statistics for partition 0 now reflect ONLY the 10 surviving young rows (stats_ev_remove pushed). */
    nbk = 0;
    CHECK(tsdb_query_agg(db, s0, 0, day_ms, day_ms, &bk, &nbk) == 0);
    CHECK(nbk == 1 && bk && bk[0].count == 10);
    tsdb_free(bk);
    bk = NULL;

    /* Wait for ecgc to delete partition 0's surviving chunks on UP stores 3,4,5 (LOST stores 0,1,2 are no-ops)
     * and unlink retired_p0_*.ec! */
    eng *e0 = tsdb_test_engine(db, 0);
    CHECK(e0 != NULL);
    bool gc_cleaned = false;
    for (int iter = 0; iter < 200; iter++) {
        graveyard_purge(e0, true);
        ecgc_tier_work(t);
        graveyard_purge(e0, true);
        int chunks_now = count_store_chunks(stores[3]) + count_store_chunks(stores[4]) + count_store_chunks(stores[5]);
        if (count_names(dbdir, "retired_p0_", ".ec") == 0 && chunks_now < chunks_before) {
            gc_cleaned = true;
            break;
        }
        usleep(10000);
    }
    CHECK(gc_cleaned);

    /* Partition 1 is STILL untouched and STILL fails reads with TSDB_ERR_IO. */
    CHECK(count_names(dbdir, "p1_", ".ec") == p1_ec);
    rrc = query_f64_count(db, s0, day_ms, 2 * day_ms, &r);
    if (rrc == 0) {
        rs_free(&r);
        rrc = query_f64_count(db, s1, day_ms, 2 * day_ms, &r);
    }
    rs_free(&r);
    CHECK(rrc == TSDB_ERR_IO);

    /* Mark store 3 DOWN_STALLED (NOT LOST): salvage WITHOUT --force MUST be refused (kills `salvagedown`)! */
    tier_store_set_down(t, 3, CHUNK_STORE_DOWN_STALLED);
    CHECK(tsdb_tier_salvage(db, 1, 0, 0) == TSDB_ERR_UNAVAILABLE);
    CHECK(tsdb_tier_salvage(db, 0, 1, 0) == TSDB_ERR_UNAVAILABLE);
    CHECK(count_names(dbdir, "p1_", ".ec") == p1_ec); /* nothing retired when refused */

    /* With force=1, salvage of partition 1 proceeds despite store 3 being DOWN! */
    cap_begin();
    CHECK(tsdb_tier_salvage(db, 1, 0, 1) == 0);
    cap_end(log, sizeof log);
    CHECK(strstr(log, "partition 1") != NULL && strstr(log, "40000 rows lost") != NULL);
    CHECK(count_names(dbdir, "p1_", ".ec") == 0);
    CHECK(query_f64_count(db, s0, day_ms, 2 * day_ms, &r) == 0 && r.n == 0);
    rs_free(&r);

    tiersalv_get_counters(t, &sc);
    CHECK(sc.partitions == 2 && sc.parts == (uint64_t)(p0_ec + p1_ec) && sc.rows_lost == 80000);

    tier_put(t);
    CHECK(tsdb_close(db) == 0);
    printf("  e15_and_salvage: ok (IO before, stats answer, young kept, ecgc cleans UP chunks, DOWN refused without force)\n");
}

/* ------------------------------------------------------------ 2. Latest table, warmer & salvagelatest race */

typedef struct race_ctx {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool armed;
    bool hit;
    bool release;
} race_ctx;

static void race_hook(void *arg, int at)
{
    race_ctx *c = arg;
    if (at != LH_WARM_BEFORE_STORE)
        return;
    pthread_mutex_lock(&c->mu);
    if (c->armed && !c->hit) {
        c->hit = true;
        pthread_cond_broadcast(&c->cv);
        while (!c->release)
            pthread_cond_wait(&c->cv, &c->mu);
    }
    pthread_mutex_unlock(&c->mu);
}

static void wait_warmer_settled(tsdb *db, int max_ms)
{
    for (int ms = 0; ms < max_ms; ms += 10) {
        tsdb_latest_stats st;
        CHECK(tsdb_get_latest_stats(db, 0, &st) == 0 && st.enabled);
        if (st.unknown == 0)
            return;
        usleep(10000);
    }
}

static void case_latest_and_warmer(void)
{
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX];
    fresh_dir("latest", base, sizeof base);
    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db, 0);

    const int64_t day_ms = 86400000LL;
    tsdb_sid s_only = 0, s_older = 0, s_young = 0, s_tomb = 0, s_race = 0;
    CHECK(tsdb_series_create(db, "lat_only_ec", TSDB_FLOAT64, NULL, &s_only) == 0);
    CHECK(tsdb_series_create(db, "lat_older_part", TSDB_FLOAT64, NULL, &s_older) == 0);
    CHECK(tsdb_series_create(db, "lat_young_above", TSDB_FLOAT64, NULL, &s_young) == 0);
    CHECK(tsdb_series_create(db, "lat_tomb_ec", TSDB_FLOAT64, NULL, &s_tomb) == 0);
    CHECK(tsdb_series_create(db, "lat_race", TSDB_FLOAT64, NULL, &s_race) == 0);

    /* Partition 0 (settled into .ec): lower-block part + merged higher-block part containing BOTH a tombstone on
     * s_tomb AND 40,000 rows (so settling preserves nt == 1 in an .ec part and exercises lt_salvage_marks'
     * tombstone loop when both partition 0 .ec parts are retired together). */
    write_f64(db, s_older, 1000, 1000, 5, 11.0);
    write_f64_varied(db, s_tomb, 1000, 1000, 40000, 22.0);
    CHECK(tsdb_flush(db) == 0);
    CHECK(tsdb_delete_range(db, s_tomb, 1000, 50000000LL) == 0);
    write_f64_varied(db, s_only, 0, 1000, 40000, 12.0);
    CHECK(tsdb_flush(db) == 0);
    merge_last_two_in_partition(db, 0);

    /* Partition 1 (settled into .ec): heavy varied payload -> k'=4, m=2 across all 6 stores. */
    write_f64_varied(db, s_only, day_ms, 1000, 20000, 33.0);
    write_f64_varied(db, s_older, day_ms, 1000, 20000, 44.0);
    write_f64_varied(db, s_young, day_ms, 1000, 20000, 55.0);
    /* For s_race: two non-null rows in partition 1's .ec part. */
    write_f64(db, s_race, day_ms + 1000, 1000, 2, 123.0);
    CHECK(tsdb_flush(db) == 0);
    size_t nset = 0;
    CHECK(tsdb_test_settle(db, pick_all, NULL, &nset) == 0 && nset >= 2);
    remove_settled_files(dbdir);

    /* Now write young .parts AFTER settle so they remain local .part files (not .ec):
     * - s_older gets a surviving row in partition 0 (older than partition 1's lost .ec!)
     * - s_young gets a surviving row in partition 1 ABOVE the lost .ec! */
    write_f64(db, s_older, 50000, 1000, 3, 77.0);
    write_f64(db, s_young, day_ms + 40000000LL, 1000, 5, 888.0);
    CHECK(tsdb_flush(db) == 0);

    /* Drain any earlier warmer work (e.g. from s_tomb's delete) so ONLY s_race triggers race_hook below. */
    wait_warmer_settled(db, 10000);

    /* Now set up the deterministic warmer race on s_race (kills `salvagelatest`):
     * Arm LH_WARM_BEFORE_STORE, then write a NULL at s_race's newest ts (day_ms + 2000) so lt_fold marks
     * s_race LE_UNKNOWN and hints the warmer. While all 6 stores are still UP, the warmer reads partition 1's
     * .ec part, computes {ts = day_ms + 1000, val = 123.0}, and pauses at LH_WARM_BEFORE_STORE! */
    race_ctx rctx = {.mu = PTHREAD_MUTEX_INITIALIZER, .cv = PTHREAD_COND_INITIALIZER, .armed = true};
    tsdb_test_latest_hook(db, race_hook, &rctx);

    int64_t null_ts = day_ms + 2000;
    double dummy = 0.0;
    uint8_t valid0 = 0;
    tsdb_values ncol = {.type = TSDB_FLOAT64, .values = &dummy, .valid = &valid0};
    CHECK(tsdb_insert_values(db, &s_race, &null_ts, &ncol, 1) == 0);
    CHECK(tsdb_flush(db) == 0);

    /* Wait until warmer pauses right before storing the pre-salvage value from partition 1's .ec part. */
    pthread_mutex_lock(&rctx.mu);
    struct timespec dl;
    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += 10;
    while (!rctx.hit) {
        int wr = pthread_cond_timedwait(&rctx.cv, &rctx.mu, &dl);
        CHECK(wr == 0);
    }
    pthread_mutex_unlock(&rctx.mu);

    tsdb_latest_stats st_before;
    CHECK(tsdb_get_latest_stats(db, 0, &st_before) == 0 && st_before.enabled);

    /* Now declare all 6 stores LOST (guaranteeing even the small tombstone .ec part of s_tomb has > m LOST
     * positions) and salvage ALL partitions while warmer is paused at LH_WARM_BEFORE_STORE! */
    tier *t = tier_find_by_dir(dbdir);
    CHECK(t != NULL);
    int lost6[6] = {0, 1, 2, 3, 4, 5};
    declare_stores_lost(dbdir, t, lost6, 6);
    CHECK(tsdb_tier_salvage(db, 0, 1, 0) == 0);

    /* Release the paused warmer:
     * - Without mutant: lgen bumped -> warmer refuses stale fill (fills_refused++), re-runs on post-salvage cut,
     *   and stores LE_EMPTY for s_race!
     * - With mutant `salvagelatest`: lgen NOT bumped -> warmer overwrites s_race with stale {day_ms+1000, 123.0}! */
    pthread_mutex_lock(&rctx.mu);
    rctx.release = true;
    pthread_cond_broadcast(&rctx.cv);
    pthread_mutex_unlock(&rctx.mu);

    tsdb_test_latest_hook(db, NULL, NULL);
    wait_warmer_settled(db, 10000);

    tsdb_latest_stats st_after;
    CHECK(tsdb_get_latest_stats(db, 0, &st_after) == 0 && st_after.enabled);
    CHECK(st_after.unknown == 0);
    CHECK(st_after.fills_refused > st_before.fills_refused);
    CHECK(st_after.fills_stored > st_before.fills_stored);

    /* Every single series MUST match the slow path bit for bit! */
    tsdb_sid all_ids[5] = {s_only, s_older, s_young, s_tomb, s_race};
    check_latest_matches_slow(db, all_ids, 5);

    /* Double-check explicit expected answers: */
    tsdb_latest_result res[5];
    CHECK(tsdb_latest(db, all_ids, 5, NULL, res) == 0);
    CHECK(res[0].kind == TSDB_LATEST_NONE); /* s_only: lost .ec retired */
    CHECK(res[1].kind == TSDB_LATEST_VALUE && res[1].ts == 52000 && res[1].v.f64 == 78.0); /* s_older: reverted */
    CHECK(res[2].kind == TSDB_LATEST_VALUE && res[2].ts == day_ms + 40004000LL && res[2].v.f64 == 890.0); /* s_young */
    CHECK(res[3].kind == TSDB_LATEST_NONE); /* s_tomb: partition 0 .ec retired */
    CHECK(res[4].kind == TSDB_LATEST_NONE); /* s_race: stale warmer fill refused! */

    tier_put(t);
    CHECK(tsdb_close(db) == 0);
    printf("  latest_and_warmer: ok (latest == slow path, warmer refills & refuses stale fill across salvage)\n");
}

/* ------------------------------------------------------------ 3. Hand-planted v5 X intent recovery */

static int open_plain(const char *dir, eng **db)
{
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.partition_classes = 0;
    return eng_open(dir, &o, db);
}

static int plant_v5_open(const char *tag, const char *const *files, const char *body, bool seal, char *dir,
                         size_t cap)
{
    fresh_dir(tag, dir, cap);
    eng *db = NULL;
    CHECK(open_plain(dir, &db) == 0 && db);
    CHECK(eng_close(db) == 0);
    for (size_t i = 0; files && files[i]; i++) {
        char p[PATH_MAX + 256];
        snprintf(p, sizeof p, "%s/%s", dir, files[i]);
        int fd = open(p, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        CHECK(fd >= 0);
        CHECK(write(fd, "x", 1) == 1);
        close(fd);
    }
    char ip[PATH_MAX + 256];
    snprintf(ip, sizeof ip, "%s/txn_9_1.intent", dir);
    int fd = open(ip, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    CHECK(fd >= 0);
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
    db = NULL;
    int rc = open_plain(dir, &db);
    if (db)
        CHECK(eng_close(db) == 0);
    return rc;
}

static const char *const SALV5 =
    "tsdb-intent 5 2\n"
    "X p500_1_1_1.ec retired_p500_1_1_1.ec\n"
    "X p500_2_2_1.ec retired_p500_2_2_1.ec\n";

static void case_recover_hand(void)
{
    char dir[PATH_MAX];
    char log[4096];

    /* 1. Torn X intent -> rolled back, .ec files untouched, intent unlinked. (Dummy .ec files fail load_parts with
     * CORRUPT, which proves recovery left them intact rather than renaming them!) */
    cap_begin();
    (void)plant_v5_open("torn", (const char *const[]){"p500_1_1_1.ec", "p500_2_2_1.ec", NULL}, SALV5, false, dir,
                        sizeof dir);
    cap_end(log, sizeof log);
    CHECK(!exists_in(dir, "txn_9_1.intent"));
    CHECK(exists_in(dir, "p500_1_1_1.ec") && exists_in(dir, "p500_2_2_1.ec"));
    CHECK(!exists_in(dir, "retired_p500_1_1_1.ec") && !exists_in(dir, "retired_p500_2_2_1.ec"));

    /* 2. Complete X intent, 0 renamed -> rolls forward all to retired_, intent unlinked. */
    cap_begin();
    int rc = plant_v5_open("fwd0", (const char *const[]){"p500_1_1_1.ec", "p500_2_2_1.ec", NULL}, SALV5, true, dir,
                           sizeof dir);
    cap_end(log, sizeof log);
    CHECK(rc == 0);
    CHECK(!exists_in(dir, "txn_9_1.intent"));
    CHECK(!exists_in(dir, "p500_1_1_1.ec") && !exists_in(dir, "p500_2_2_1.ec"));
    CHECK(exists_in(dir, "retired_p500_1_1_1.ec") && exists_in(dir, "retired_p500_2_2_1.ec"));

    /* 3. Complete X intent, half renamed -> rolls forward remaining, NEVER resurrects retired_p500_1_1_1.ec. */
    cap_begin();
    rc = plant_v5_open("fwd1", (const char *const[]){"retired_p500_1_1_1.ec", "p500_2_2_1.ec", NULL}, SALV5, true, dir,
                       sizeof dir);
    cap_end(log, sizeof log);
    CHECK(rc == 0);
    CHECK(!exists_in(dir, "txn_9_1.intent"));
    CHECK(!exists_in(dir, "p500_1_1_1.ec") && !exists_in(dir, "p500_2_2_1.ec"));
    CHECK(exists_in(dir, "retired_p500_1_1_1.ec") && exists_in(dir, "retired_p500_2_2_1.ec"));

    /* 4. Complete X intent, all already retired -> succeeds, nothing resurrected. */
    cap_begin();
    rc = plant_v5_open("fwd2", (const char *const[]){"retired_p500_1_1_1.ec", "retired_p500_2_2_1.ec", NULL}, SALV5,
                       true, dir, sizeof dir);
    cap_end(log, sizeof log);
    CHECK(rc == 0 && !exists_in(dir, "txn_9_1.intent"));
    CHECK(!exists_in(dir, "p500_1_1_1.ec") && !exists_in(dir, "p500_2_2_1.ec"));

    /* 5. Complete X intent, all already deleted by GC -> succeeds cleanly. */
    cap_begin();
    rc = plant_v5_open("fwdgone", NULL, SALV5, true, dir, sizeof dir);
    cap_end(log, sizeof log);
    CHECK(rc == 0 && !exists_in(dir, "txn_9_1.intent"));

    /* 6. Both from and to exist simultaneously -> TSDB_ERR_CORRUPT, nothing renamed, intent kept. */
    cap_begin();
    rc = plant_v5_open("both", (const char *const[]){"p500_1_1_1.ec", "retired_p500_1_1_1.ec", "p500_2_2_1.ec", NULL},
                       SALV5, true, dir, sizeof dir);
    cap_end(log, sizeof log);
    CHECK(rc == TSDB_ERR_CORRUPT && exists_in(dir, "txn_9_1.intent") && exists_in(dir, "p500_2_2_1.ec"));

    /* 7. Rule-breaking sealed X intents -> TSDB_ERR_CORRUPT, nothing renamed. */
    static const char *const bad_intents[] = {
        "tsdb-intent 5 1\nX MANIFEST retired_MANIFEST\n",
        "tsdb-intent 5 1\nX p500_1_1_1.part retired_p500_1_1_1.part\n",
        "tsdb-intent 5 1\nX p500_1_1_1.ec settled_p500_1_1_1.ec\n",
        "tsdb-intent 5 2\nX p500_1_1_1.ec retired_p500_1_1_1.ec\nX p501_2_2_1.ec retired_p501_2_2_1.ec\n",
        "tsdb-intent 5 2\nX p500_1_1_1.ec retired_p500_1_1_1.ec\nX p500_1_1_1.ec retired_p500_1_1_1.ec\n",
        "tsdb-intent 5 2\nX p500_1_1_1.ec retired_p500_1_1_1.ec\nL tmp_1_1.part p500_1_1_1.part\n",
        "tsdb-intent 5 2\nN tmp_1_1.ec p500_1_1_1.ec\nX p500_1_1_1.ec retired_p500_1_1_1.ec\n",
    };
    for (size_t i = 0; i < sizeof bad_intents / sizeof *bad_intents; i++) {
        char tag[32];
        snprintf(tag, sizeof tag, "bad%zu", i);
        cap_begin();
        rc = plant_v5_open(tag, (const char *const[]){"p500_1_1_1.ec", NULL}, bad_intents[i], true, dir, sizeof dir);
        cap_end(log, sizeof log);
        CHECK(rc == TSDB_ERR_CORRUPT && exists_in(dir, "txn_9_1.intent") && exists_in(dir, "p500_1_1_1.ec"));
    }
    printf("  recover_hand: ok (torn rollback, 0/half/all/gone roll-forward, corrupt & rule-breaking rejected)\n");
}

/* ------------------------------------------------------------ 4. crashfs power-loss sweep */

static int child_salvage(const char *base)
{
    char dbdir[PATH_MAX + 16];
    snprintf(dbdir, sizeof dbdir, "%s/db", base);
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.create_volumes = 0;
    o.partition_classes = 0;
    o.stats = -1;
    tier *t = NULL;
    if (tier_open_salvage(dbdir, &o, &t) != 0 || !t)
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
    int rc = tiersalv_eng(db, 0, false, false, NULL);
    int crc = eng_close(db);
    tier_put(t);
    return rc == 0 && crc == 0 ? 0 : 12;
}

static int run_crashfs_child(const char *base, int warmup, unsigned seed)
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
        execl(SELF, SELF, "--mutant", mut_name, "--child", base, (char *)NULL);
        _exit(127);
    }
    int st = 0;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

static void case_crashfs_sweep(void)
{
    char src[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX];
    fresh_dir("pwr_src", src, sizeof src);
    tsdb *db = NULL;
    setup_6store_db(src, dbdir, stores, &db, 60);

    tsdb_sid s0 = 0, s1 = 0;
    CHECK(tsdb_series_create(db, "pwr_s0", TSDB_FLOAT64, NULL, &s0) == 0);
    CHECK(tsdb_series_create(db, "pwr_s1", TSDB_FLOAT64, NULL, &s1) == 0);

    /* Create TWO separate settled .ec parts in partition 0 (so salvage must atomically retire BOTH via 2 X lines)
     * plus ONE surviving young .part above them! */
    write_f64_varied(db, s0, 0, 1000, 20000, 1.0);
    write_f64_varied(db, s1, 0, 1000, 20000, 2.0);
    CHECK(tsdb_flush(db) == 0);
    size_t nset = 0;
    CHECK(tsdb_test_settle(db, pick_all, NULL, &nset) == 0 && nset >= 1);

    write_f64_varied(db, s0, 20000000LL, 1000, 20000, 3.0);
    write_f64_varied(db, s1, 20000000LL, 1000, 20000, 4.0);
    CHECK(tsdb_flush(db) == 0);
    CHECK(tsdb_test_settle(db, pick_all, NULL, &nset) == 0 && nset >= 1);

    /* Young .part above both .ec parts */
    write_f64(db, s0, 50000000LL, 1000, 8, 900.0);
    write_f64(db, s1, 50000000LL, 1000, 8, 950.0);
    CHECK(tsdb_flush(db) == 0);
    int orig_ec = count_names(dbdir, "p0_", ".ec");
    CHECK(orig_ec >= 2 && count_names(dbdir, "p0_", ".part") == 1);

    CHECK(tsdb_close(db) == 0);
    remove_settled_files(dbdir);

    /* Mark 3 stores LOST in TIERS while closed. */
    int lost3[3] = {0, 1, 2};
    declare_stores_lost(dbdir, NULL, lost3, 3);

    int crashes = 0, pre = 0, post = 0;
    int seeds = quick ? 2 : 4;
    bool finished = false;
    char work[PATH_MAX + 64], wdb[PATH_MAX + 80];
    snprintf(work, sizeof work, "%s/pwr_work_%d", S, (int)getpid());
    snprintf(wdb, sizeof wdb, "%s/db", work);

    for (int k = 0; k < 2000 && !finished; k++) {
        for (int sd = 0; sd < seeds; sd++) {
            CHECK(sh("rm -rf '%s' && mkdir -p '%s' && cp -a '%s/db' '%s/db'", work, work, src, work) == 0);
            int code = run_crashfs_child(work, k, (unsigned)(k * 17 + sd + 1));
            CHECK(code == 99 || code == 0);
            crashes += code == 99;

            /* Reopen via tier_open_salvage + eng_open (runs intents_recover + load_parts + latest_build). */
            tsdb_options o;
            tsdb_options_init(&o);
            o.merge_threads = 0;
            o.create_volumes = 0;
            o.partition_classes = 0;
            o.stats = -1;
            tier *rt = NULL;
            CHECK(tier_open_salvage(wdb, &o, &rt) == 0 && rt);
            tier_quiesce(rt);
            eng *rdb = NULL;
            tier_want(rt, 0);
            int orc = eng_open(wdb, &o, &rdb);
            tier_want(NULL, 0);
            CHECK(orc == 0 && rdb);

            /* No leftover intents after recovery! */
            CHECK(count_names(wdb, "txn_", ".intent") == 0);

            int nec = count_names(wdb, "p0_", ".ec");
            int npart = count_names(wdb, "p0_", ".part");
            /* Young part MUST always survive intact; .ec parts MUST be all-or-nothing (orig_ec pre-commit or 0 post-commit)! */
            CHECK(npart == 1);
            CHECK(nec == orig_ec || nec == 0);
            if (nec == 0) {
                post++;
                /* Post-commit: verify all retired_*.ec exist and latest matches slow path (young rows only)! */
                CHECK(count_names(wdb, "retired_p0_", ".ec") == orig_ec);
                uint32_t lids[2] = {TSDB_SID_LOCAL(s0), TSDB_SID_LOCAL(s1)};
                latest_row lr[2];
                CHECK(eng_latest(rdb, lids, 2, NULL, INT64_MAX, false, lr) == 0);
                double lv0 = 0, lv1 = 0;
                memcpy(&lv0, &lr[0].slot, 8);
                memcpy(&lv1, &lr[1].slot, 8);
                CHECK(lr[0].status == 0 && lr[0].kind == 0 && lr[0].ts == 50007000LL && lv0 == 903.5);
                CHECK(lr[1].status == 0 && lr[1].kind == 0 && lr[1].ts == 50007000LL && lv1 == 953.5);
            } else {
                pre++;
            }

            CHECK(eng_close(rdb) == 0);
            tier_put(rt);
            if (code == 0) {
                finished = true;
                CHECK(nec == 0); /* acknowledged salvage MUST be durable! */
            }
        }
    }
    CHECK(sh("rm -rf '%s'", work) == 0);
    CHECK(finished && crashes > 0 && pre > 0 && post > 0);
    printf("  crashfs_sweep: ok (%d crashes, %d pre-commit, %d post-commit, atomic 2-part retire + young kept)\n",
           crashes, pre, post);
}

/* ------------------------------------------------------------ 5. Small k' selective salvage */

static void case_small_k_selective(void)
{
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX];
    fresh_dir("smallk", base, sizeof base);
    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db, 0);

    const int64_t day_ms = 86400000LL;
    tsdb_sid s0 = 0;
    CHECK(tsdb_series_create(db, "sk_s0", TSDB_FLOAT64, NULL, &s0) == 0);

    /* Write 24 small partitions (5 rows each -> 1 cell -> k'=1, m=2 -> 3 chunks on 6 stores).
     * When stores {0, 1, 2} are LOST, only objects whose 3 chunks all land on {0, 1, 2} (prob 1/6 per object)
     * have > m=2 positions on LOST stores; the remaining ~5/6 stay readable and MUST NOT be retired! */
    const int nparts = 24;
    for (int p = 0; p < nparts; p++)
        write_f64(db, s0, (int64_t)p * day_ms, 1000, 5, (double)(p * 100));
    CHECK(tsdb_flush(db) == 0);
    size_t nset = 0;
    CHECK(tsdb_test_settle(db, pick_all, NULL, &nset) == 0 && nset == (size_t)nparts);
    remove_settled_files(dbdir);

    tier *t = tier_find_by_dir(dbdir);
    CHECK(t != NULL);
    int lost3[3] = {0, 1, 2};
    declare_stores_lost(dbdir, t, lost3, 3);
    (void)tsdb_test_ec_retries(db, true);
    eccache_reopen_all(t);

    /* Record which partitions fail IO vs remain readable BEFORE salvage. */
    bool was_lost[24] = {false};
    int n_lost = 0, n_surv = 0;
    for (int p = 0; p < nparts; p++) {
        rowset r = {0};
        int rc = query_f64_count(db, s0, (int64_t)p * day_ms, (int64_t)(p + 1) * day_ms, &r);
        rs_free(&r);
        if (rc == TSDB_ERR_IO) {
            was_lost[p] = true;
            n_lost++;
        } else {
            CHECK(rc == 0);
            n_surv++;
        }
    }
    CHECK(n_surv > 0); /* 5/6 survive */

    CHECK(tsdb_tier_salvage(db, 0, 1, 0) == 0);

    /* Every previously-lost partition now reads 0 rows (no IO error); every surviving partition STILL reads exact! */
    for (int p = 0; p < nparts; p++) {
        rowset r = {0};
        CHECK(query_f64_count(db, s0, (int64_t)p * day_ms, (int64_t)(p + 1) * day_ms, &r) == 0);
        if (was_lost[p]) {
            CHECK(r.n == 0);
        } else {
            CHECK(r.n == 5);
            double v0 = 0;
            memcpy(&v0, &r.x[0], 8);
            CHECK(v0 == (double)(p * 100));
        }
        rs_free(&r);
    }
    check_latest_matches_slow(db, &s0, 1);

    tier_put(t);
    CHECK(tsdb_close(db) == 0);
    printf("  small_k_selective: ok (%d lost retired, %d surviving k'=1+2 objects kept & verified)\n", n_lost, n_surv);
}

/* ------------------------------------------------------------ 6. Offline CLI salvage */

static void case_cli_salvage(void)
{
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX];
    fresh_dir("cli", base, sizeof base);
    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db, 0);

    tsdb_sid s0 = 0, s1 = 0;
    CHECK(tsdb_series_create(db, "cli_s0", TSDB_FLOAT64, NULL, &s0) == 0);
    CHECK(tsdb_series_create(db, "cli_s1", TSDB_FLOAT64, NULL, &s1) == 0);
    write_f64_varied(db, s0, 0, 1000, 20000, 1.0);
    write_f64_varied(db, s1, 0, 1000, 20000, 2.0);
    CHECK(tsdb_flush(db) == 0);
    size_t nset = 0;
    CHECK(tsdb_test_settle(db, pick_all, NULL, &nset) == 0 && nset >= 1);
    /* Add young part above .ec */
    write_f64(db, s0, 40000000LL, 1000, 5, 777.0);
    CHECK(tsdb_flush(db) == 0);
    int p0_ec = count_names(dbdir, "p0_", ".ec");
    CHECK(p0_ec >= 1);
    CHECK(tsdb_close(db) == 0);
    remove_settled_files(dbdir);

    /* Declare 3 stores LOST offline, then run tiersalv_cli. */
    int lost3[3] = {0, 1, 2};
    declare_stores_lost(dbdir, NULL, lost3, 3);

    tiersalv_report rep;
    CHECK(tiersalv_cli(dbdir, 0, true, false, &rep) == 0);
    CHECK(rep.partitions == 1 && rep.parts == (uint64_t)p0_ec && rep.rows_lost == 40000);
    CHECK(count_names(dbdir, "p0_", ".ec") == 0 && count_names(dbdir, "p0_", ".part") == 1);

    /* Verify offline salvage also refuses when a non-LOST store is missing unless force=true. */
    char st3_bak[PATH_MAX + 32];
    snprintf(st3_bak, sizeof st3_bak, "%s.bak", stores[3]);
    CHECK(rename(stores[3], st3_bak) == 0);
    CHECK(tiersalv_cli(dbdir, 0, true, false, &rep) == TSDB_ERR_UNAVAILABLE);
    CHECK(tiersalv_cli(dbdir, 0, true, true, &rep) == 0);
    CHECK(rename(st3_bak, stores[3]) == 0);
    printf("  cli_salvage: ok (offline salvage retired 1 part / 40000 rows, DOWN store refused without force)\n");
}

/* ------------------------------------------------------------ 7. Settled_ fast-path restore before retire */

static void corrupt_first_settled(const char *dir, const char *prefix)
{
    DIR *d = opendir(dir);
    CHECK(d != NULL);
    bool hit = false;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        size_t l = strlen(e->d_name);
        if (strncmp(e->d_name, prefix, strlen(prefix)) == 0 && l > 5 && strcmp(e->d_name + l - 5, ".part") == 0) {
            char p[PATH_MAX + 256];
            snprintf(p, sizeof p, "%s/%s", dir, e->d_name);
            int fd = open(p, O_RDWR);
            CHECK(fd >= 0);
            struct stat st;
            CHECK(fstat(fd, &st) == 0 && st.st_size > 256);
            uint8_t b = 0;
            off_t off = st.st_size / 2;
            CHECK(pread(fd, &b, 1, off) == 1);
            b ^= 0xff;
            CHECK(pwrite(fd, &b, 1, off) == 1);
            CHECK(fsync(fd) == 0);
            close(fd);
            hit = true;
            break;
        }
    }
    closedir(d);
    CHECK(hit);
}

static void case_settled_restore(void)
{
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX];
    fresh_dir("srestore", base, sizeof base);
    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db, 0);

    const int64_t day_ms = 86400000LL;
    tsdb_sid s0 = 0, s1 = 0;
    CHECK(tsdb_series_create(db, "sr_s0", TSDB_FLOAT64, NULL, &s0) == 0);
    CHECK(tsdb_series_create(db, "sr_s1", TSDB_FLOAT64, NULL, &s1) == 0);

    write_f64_varied(db, s0, 0, 1000, 20000, 10.0);
    write_f64_varied(db, s1, 0, 1000, 20000, 20.0);
    write_f64_varied(db, s0, day_ms, 1000, 20000, 30.0);
    write_f64_varied(db, s1, day_ms, 1000, 20000, 40.0);
    CHECK(tsdb_flush(db) == 0);
    size_t nset = 0;
    CHECK(tsdb_test_settle(db, pick_all, NULL, &nset) == 0 && nset >= 2);
    CHECK(count_names(dbdir, "settled_p0_", ".part") >= 1);
    CHECK(count_names(dbdir, "settled_p1_", ".part") >= 1);

    /* Corrupt partition 1's settled_ copy while keeping partition 0's settled_ copy intact. */
    corrupt_first_settled(dbdir, "settled_p1_");

    tier *t = tier_find_by_dir(dbdir);
    CHECK(t != NULL);
    int lost3[3] = {0, 1, 2};
    declare_stores_lost(dbdir, t, lost3, 3);
    (void)tsdb_test_ec_retries(db, true);
    eccache_reopen_all(t);

    /* Salvage ALL: partition 0 MUST be restored from intact settled_ (0 rows lost); partition 1 MUST be retired. */
    char log[8192];
    cap_begin();
    CHECK(tsdb_tier_salvage(db, 0, 1, 0) == 0);
    cap_end(log, sizeof log);
    CHECK(strstr(log, "restored") != NULL && strstr(log, "0 rows lost") != NULL);
    CHECK(strstr(log, "partition 1") != NULL && strstr(log, "40000 rows lost") != NULL);

    /* Partition 0 restored as local .part with ALL 20,000 rows per series intact! */
    CHECK(count_names(dbdir, "p0_", ".ec") == 0);
    CHECK(count_names(dbdir, "p0_", ".part") >= 1);
    rowset r = {0};
    CHECK(query_f64_count(db, s0, 0, day_ms, &r) == 0 && r.n == 20000);
    rs_free(&r);
    CHECK(query_f64_count(db, s1, 0, day_ms, &r) == 0 && r.n == 20000);
    rs_free(&r);

    /* Partition 1 retired and corrupt settled_p1_*.part removed (not orphaned). */
    CHECK(count_names(dbdir, "p1_", ".ec") == 0);
    CHECK(count_names(dbdir, "p1_", ".part") == 0);
    CHECK(count_names(dbdir, "settled_p1_", ".part") == 0);
    CHECK(query_f64_count(db, s0, day_ms, 2 * day_ms, &r) == 0 && r.n == 0);
    rs_free(&r);

    /* Counters count ONLY the retired partition 1, NOT the restored partition 0. */
    tiersalv_counters sc;
    tiersalv_get_counters(t, &sc);
    CHECK(sc.partitions == 1 && sc.parts == 1 && sc.rows_lost == 40000);

    tsdb_sid ids[2] = {s0, s1};
    check_latest_matches_slow(db, ids, 2);

    tier_put(t);
    CHECK(tsdb_close(db) == 0);
    printf("  settled_restore: ok (intact settled_ restored with 0 rows lost; corrupt settled_ retired & cleaned)\n");
}

/* ------------------------------------------------------------ 8. Shadowing lower-block part refusal */

static void case_shadow_refusal(void)
{
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX];
    fresh_dir("shadow", base, sizeof base);
    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db, 0);

    const int64_t day_ms = 86400000LL;
    tsdb_sid s_del = 0, s_ov = 0, s_disj = 0, s_fill = 0;
    CHECK(tsdb_series_create(db, "sh_del", TSDB_FLOAT64, NULL, &s_del) == 0);
    CHECK(tsdb_series_create(db, "sh_ov", TSDB_FLOAT64, NULL, &s_ov) == 0);
    CHECK(tsdb_series_create(db, "sh_disj", TSDB_FLOAT64, NULL, &s_disj) == 0);
    CHECK(tsdb_series_create(db, "sh_fill", TSDB_FLOAT64, NULL, &s_fill) == 0);

    /* Partition 0: lower-block local .part with rows [1000, 2000, 3000], then higher-block merged part containing
     * BOTH tombstone deleting [1500, 2500) AND 40k filler rows (settled into a 4+2 .ec). Retiring the .ec would
     * resurrect ts=2000 -> MUST be refused! */
    write_f64(db, s_del, 1000, 1000, 3, 10.0);
    CHECK(tsdb_flush(db) == 0);
    CHECK(tsdb_delete_range(db, s_del, 1500, 2500) == 0);
    write_f64_varied(db, s_fill, 0, 1000, 40000, 1.0);
    CHECK(tsdb_flush(db) == 0);
    merge_last_two_in_partition(db, 0);

    /* Partition 1: lower-block local .part with s_ov(day_ms + 1000) = 111.0, then higher-block .ec overwriting
     * s_ov(day_ms + 1000) = 222.0. Retiring the .ec would resurrect 111.0 -> MUST be refused! */
    write_f64(db, s_ov, day_ms + 1000, 1000, 1, 111.0);
    CHECK(tsdb_flush(db) == 0);
    write_f64(db, s_ov, day_ms + 1000, 1000, 1, 222.0);
    write_f64_varied(db, s_fill, day_ms, 1000, 40000, 2.0);
    CHECK(tsdb_flush(db) == 0);

    /* Partition 2: lower-block local .part with s_disj at [1000..5000], then higher-block .ec with s_disj at
     * disjoint timestamps [20000000..]. Retiring the .ec cannot resurrect anything -> MUST succeed! */
    write_f64(db, s_disj, 2 * day_ms + 1000, 1000, 5, 30.0);
    CHECK(tsdb_flush(db) == 0);
    write_f64(db, s_disj, 2 * day_ms + 20000000LL, 1000, 5, 40.0);
    write_f64_varied(db, s_fill, 2 * day_ms, 1000, 40000, 3.0);
    CHECK(tsdb_flush(db) == 0);

    /* Settle ONLY the heavy higher-block parts (rows >= 10000), leaving all 3 lower-block parts local. */
    size_t nset = 0;
    CHECK(tsdb_test_settle(db, pick_heavy, NULL, &nset) == 0 && nset == 3);
    CHECK(count_names(dbdir, "p0_", ".part") == 1 && count_names(dbdir, "p0_", ".ec") == 1);
    CHECK(count_names(dbdir, "p1_", ".part") == 1 && count_names(dbdir, "p1_", ".ec") == 1);
    CHECK(count_names(dbdir, "p2_", ".part") == 1 && count_names(dbdir, "p2_", ".ec") == 1);
    remove_settled_files(dbdir);

    tier *t = tier_find_by_dir(dbdir);
    CHECK(t != NULL);
    int lost3[3] = {0, 1, 2};
    declare_stores_lost(dbdir, t, lost3, 3);
    (void)tsdb_test_ec_retries(db, true);
    eccache_reopen_all(t);

    char log[4096];
    /* Partition 0 (tombstone shadows lower-block part) -> refused with TSDB_ERR_INVALID. */
    cap_begin();
    CHECK(tsdb_tier_salvage(db, 0, 0, 0) == TSDB_ERR_INVALID);
    cap_end(log, sizeof log);
    CHECK(strstr(log, "tombstone shadows") != NULL);
    CHECK(count_names(dbdir, "p0_", ".ec") == 1);
    /* Verify deleted row ts=2000 is STILL hidden by the tombstone! */
    rowset r = {0};
    CHECK(query_f64_count(db, s_del, 0, day_ms, &r) == 0);
    CHECK(r.n == 2 && r.ts[0] == 1000 && r.ts[1] == 3000);
    rs_free(&r);

    /* Partition 1 (overlapping overwrite shadows lower-block part) -> refused with TSDB_ERR_INVALID. */
    cap_begin();
    CHECK(tsdb_tier_salvage(db, 1, 0, 0) == TSDB_ERR_INVALID);
    cap_end(log, sizeof log);
    CHECK(strstr(log, "overlap surviving lower-block") != NULL);
    CHECK(count_names(dbdir, "p1_", ".ec") == 1);

    /* Partition 2 (disjoint timestamps) -> succeeds cleanly, lower-block rows stay intact! */
    CHECK(tsdb_tier_salvage(db, 2, 0, 0) == 0);
    CHECK(count_names(dbdir, "p2_", ".ec") == 0 && count_names(dbdir, "p2_", ".part") == 1);
    CHECK(query_f64_count(db, s_disj, 2 * day_ms, 3 * day_ms, &r) == 0 && r.n == 5 && r.ts[0] == 2 * day_ms + 1000);
    rs_free(&r);

    tier_put(t);
    CHECK(tsdb_close(db) == 0);
    printf("  shadow_refusal: ok (tombstone & overwrite shadow refused; disjoint higher-block salvaged)\n");
}

/* ------------------------------------------------------------ 9. CLI extra volume & identity validation */

static void case_cli_extra_volume(void)
{
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX], v1dir[PATH_MAX + 16];
    fresh_dir("clivol", base, sizeof base);
    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db, 0);

    snprintf(v1dir, sizeof v1dir, "%s/vol1", base);
    uint32_t vidx = 0;
    CHECK(tsdb_volume_add(db, v1dir, &vidx) == 0 && vidx == 1);

    tsdb_sid s_root = 0, s_v1 = 0;
    CHECK(tsdb_series_create(db, "cv_root", TSDB_FLOAT64, NULL, &s_root) == 0);
    CHECK(tsdb_series_create(db, "cv_v1", TSDB_FLOAT64, v1dir, &s_v1) == 0);
    write_f64_varied(db, s_root, 0, 1000, 40000, 1.0);
    write_f64_varied(db, s_v1, 0, 1000, 40000, 2.0);
    CHECK(tsdb_flush(db) == 0);
    size_t nset = 0;
    CHECK(tsdb_test_settle(db, pick_all, NULL, &nset) == 0 && nset == 2);
    CHECK(count_names(dbdir, "p0_", ".ec") == 1);
    CHECK(count_names(v1dir, "p0_", ".ec") == 1);
    CHECK(tsdb_close(db) == 0);
    remove_settled_files(dbdir);
    remove_settled_files(v1dir);

    int lost3[3] = {0, 1, 2};
    declare_stores_lost(dbdir, NULL, lost3, 3);

    char vpath[PATH_MAX + 32], mpath[PATH_MAX + 32], rpath[PATH_MAX + 32];
    snprintf(vpath, sizeof vpath, "%s/VOLUMES", dbdir);
    snprintf(mpath, sizeof mpath, "%s/META", v1dir);
    snprintf(rpath, sizeof rpath, "%s/ROOT_ID", v1dir);
    char vbak[PATH_MAX + 40], mbak[PATH_MAX + 40], rbak[PATH_MAX + 40];
    snprintf(vbak, sizeof vbak, "%s/VOLUMES.bak", base);
    snprintf(mbak, sizeof mbak, "%s/META.bak", base);
    snprintf(rbak, sizeof rbak, "%s/ROOT_ID.bak", base);
    CHECK(sh("cp '%s' '%s' && cp '%s' '%s' && cp '%s' '%s'", vpath, vbak, mpath, mbak, rpath, rbak) == 0);

    tiersalv_report rep;
    char log[4096];

    /* 1. Tampered VOLUMES checksum -> refused with TSDB_ERR_CORRUPT, neither volume touched. */
    int fd = open(vpath, O_RDWR);
    CHECK(fd >= 0);
    char ch = 0;
    CHECK(pread(fd, &ch, 1, 0) == 1);
    ch = (ch == '0') ? '1' : '0';
    CHECK(pwrite(fd, &ch, 1, 0) == 1);
    close(fd);
    cap_begin();
    CHECK(tiersalv_cli(dbdir, 0, true, false, &rep) == TSDB_ERR_CORRUPT);
    cap_end(log, sizeof log);
    CHECK(count_names(dbdir, "p0_", ".ec") == 1 && count_names(v1dir, "p0_", ".ec") == 1);
    CHECK(sh("cp '%s' '%s'", vbak, vpath) == 0);

    /* 2. Wrong META volume= in vol1 -> refused with TSDB_ERR_CORRUPT, neither volume touched. */
    CHECK(sh("sed 's/^volume=1$/volume=9/' '%s' > '%s'", mbak, mpath) == 0);
    cap_begin();
    CHECK(tiersalv_cli(dbdir, 0, true, false, &rep) == TSDB_ERR_CORRUPT);
    cap_end(log, sizeof log);
    CHECK(count_names(dbdir, "p0_", ".ec") == 1 && count_names(v1dir, "p0_", ".ec") == 1);
    CHECK(sh("cp '%s' '%s'", mbak, mpath) == 0);

    /* 3. Wrong ROOT_ID in vol1 -> refused with TSDB_ERR_CORRUPT, neither volume touched. */
    fd = open(rpath, O_WRONLY | O_TRUNC);
    CHECK(fd >= 0);
    CHECK(write(fd, "deadbeefdeadbeef\n", 17) == 17);
    close(fd);
    cap_begin();
    CHECK(tiersalv_cli(dbdir, 0, true, false, &rep) == TSDB_ERR_CORRUPT);
    cap_end(log, sizeof log);
    CHECK(count_names(dbdir, "p0_", ".ec") == 1 && count_names(v1dir, "p0_", ".ec") == 1);
    CHECK(sh("cp '%s' '%s'", rbak, rpath) == 0);

    /* 4. Valid multi-volume salvage -> retires lost .ec parts on BOTH root and vol1! */
    CHECK(tiersalv_cli(dbdir, 0, true, false, &rep) == 0);
    CHECK(rep.partitions == 2 && rep.parts == 2 && rep.rows_lost == 80000);
    CHECK(count_names(dbdir, "p0_", ".ec") == 0 && count_names(v1dir, "p0_", ".ec") == 0);
    printf("  cli_extra_volume: ok (VOLUMES checksum, META volume=, ROOT_ID enforced; both volumes salvaged)\n");
}

int main(int argc, char **argv)
{
    const char *scratch = "/tmp";
    const char *child_base = NULL;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--quick") == 0) {
            quick = true;
        } else if (strcmp(argv[i], "--mutant") == 0 && i + 1 < argc) {
            mut_name = argv[++i];
            int m = tiersalv_mutant_parse(mut_name);
            if (m < 0) {
                fprintf(stderr, "test_tiersalv: unknown mutant '%s'\n", mut_name);
                return 2;
            }
            tiersalv_mutant = m;
        } else if (strcmp(argv[i], "--child") == 0 && i + 1 < argc) {
            child_base = argv[++i];
        } else if (argv[i][0] != '-') {
            scratch = argv[i];
        }
    }

    if (child_base)
        return child_salvage(child_base);

    th_watchdog("test_tiersalv", 180);
    if (!realpath(argv[0], SELF))
        snprintf(SELF, sizeof SELF, "%s", argv[0]);
    const char *sl = strrchr(SELF, '/');
    int bdirlen = sl ? (int)(sl - SELF) : 1;
    snprintf(SHIM, sizeof SHIM, "%.*s/libcrashfs.so", bdirlen, sl ? SELF : ".");
    snprintf(S, sizeof S, "%s/test_tiersalv_%d", scratch, (int)getpid());
    CHECK(sh("rm -rf '%s' && mkdir -p '%s'", S, S) == 0);

    printf("test_tiersalv (mutant=%s%s):\n", mut_name, quick ? ", quick" : "");
    case_e15_and_salvage();
    case_latest_and_warmer();
    case_recover_hand();
    case_crashfs_sweep();
    case_small_k_selective();
    case_cli_salvage();
    case_settled_restore();
    case_shadow_refusal();
    case_cli_extra_volume();

    CHECK(sh("rm -rf '%s'", S) == 0);
    printf("test_tiersalv: ok\n");
    return 0;
}

#endif /* TSDB_EC */
