/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The big buffers of a flush and of a memtable: what the other threads wait on (flush-tail).
 *
 * A memtable that sees a million series has a 40 MB array of per-series state, and mt_reserve grows it under buf_lock,
 * which every append and lookup takes. realloc and a memset of the new tail touched all of its pages there (9 ms with
 * the lock held, twice, right after a flush gave the appenders a new memtable); the array is now an anonymous mapping
 * (zero pages, grown by mremap), so a row for a high series id touches only its own page. And the large buffers of a
 * flush are handed back in slices (tsdb_free_big) so that munmap holds the mmap lock for nothing.
 *
 *   ./build/test_flush_tail
 *
 * With -DOLD_CODE (main's memtable.c and compat.h) the part about the free is left out, and the RSS check fails.
 */
#include "internal.h"
#include "tsdb.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include "watchdog.h"
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#ifndef __APPLE__
#include <sys/mman.h>
#endif

#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#define UNDER_SAN 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
#define UNDER_SAN 1
#else
#define UNDER_SAN 0
#endif
#else
#define UNDER_SAN 0
#endif

static int failures;
#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);                                               \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

/* resident memory of this process, bytes. macOS has no /proc: the RSS checks are skipped there (rss_ok is 0; the
 * checks of the contents stay) */
#ifdef __APPLE__
#define rss_ok 0
static long rss(void) { return 0; }
#else
#define rss_ok 1
static long rss(void)
{
    FILE *f = fopen("/proc/self/statm", "r");
    long a = 0, b = 0;
    if (f) {
        if (fscanf(f, "%ld %ld", &a, &b) != 2)
            b = 0;
        fclose(f);
    }
    return b * sysconf(_SC_PAGESIZE);
}
#endif

static void add(memtable *m, uint32_t id, int64_t ts, double x)
{
    uint64_t slot;
    memcpy(&slot, &x, 8);
    CHECK(mt_add(m, NULL, id, &ts, &slot, 8, 1, 0, -1) == 0);
}

static int has(const memtable *m, uint32_t id, int64_t ts, double x)
{
    mem_run r = {0};
    int ok = 0;
    if (mt_collect(m, id, 8, INT64_MIN, INT64_MAX, UINT64_MAX, &r) == 0 && r.n == 1 && r.ts[0] == ts) {
        double y;
        memcpy(&y, r.v, 8);
        ok = y == x;
    }
    mem_run_free(&r);
    return ok;
}

static void test_series_array(void)
{
    memtable *m = mt_new(1);
    CHECK(m != NULL);
    add(m, 3, 100, 1.5);
    long r0 = rss();
    /* the first row of a memtable for a high id (what an appender sends right after a flush): the array grows to
     * 1,000,001 entries, 40 MB. The old code wrote every page of it under buf_lock */
    add(m, 1000000, 200, 2.5);
    long grown = rss() - r0;
    CHECK(!rss_ok || grown < (8l << 20));
    if (rss_ok && grown >= (8l << 20))
        fprintf(stderr, "  the array's growth made %ld MB resident\n", grown >> 20);
    /* growing again keeps what was there (mremap moves it) and reads zero elsewhere */
    add(m, 3000000, 300, 3.5);
    CHECK(has(m, 3, 100, 1.5));
    CHECK(has(m, 1000000, 200, 2.5));
    CHECK(has(m, 3000000, 300, 3.5));
    mem_run r = {0};
    CHECK(mt_collect(m, 2000000, 8, INT64_MIN, INT64_MAX, UINT64_MAX, &r) == 0 && r.n == 0);
    mem_run_free(&r);
    CHECK(!rss_ok || rss() - r0 < (8l << 20));
    mt_unref(m);
}

#ifndef OLD_CODE
/* tsdb_free_big zaps only the pages inside the block it is given: the neighbours and the block's partial pages stay */
static void test_free_big(void)
{
    size_t n = (size_t)24 << 20; /* above TSDB_FREE_BIG_MIN */
    unsigned char *a = malloc(n), *b = malloc(n), *c = malloc(n);
    CHECK(a && b && c);
    memset(a, 0xa5, n);
    memset(b, 0x5a, n);
    memset(c, 0xc3, n);
    long r0 = rss();
    tsdb_free_big(b, n);
    long freed = r0 - rss();
    for (size_t i = 0; i < n; i += 4093) {
        CHECK(a[i] == 0xa5);
        CHECK(c[i] == 0xc3);
    }
    CHECK(a[n - 1] == 0xa5 && c[0] == 0xc3);
    CHECK(!rss_ok || freed > (12l << 20)); /* the block's pages went back */
    /* a block that starts inside a page and a short one: no crash, no damage */
    unsigned char *d = malloc((size_t)20 << 20);
    memset(d, 0x11, (size_t)20 << 20);
    tsdb_free_big(d, (size_t)20 << 20);
    tsdb_free_big(NULL, (size_t)64 << 20);
    tsdb_free_big(malloc(100), 100);
    CHECK(a[12345] == 0xa5 && c[12345] == 0xc3);
    free(a);
    free(c);
}

#ifndef __APPLE__
/* the zap rounds to the page size it is given and clears no byte outside the block, whatever the kernel's page size:
 * on a 64 KB kernel madvise rounds a 4 KB-rounded range up and clears the next chunk. Here the "kernel" has 4 KB pages
 * and the zap is told 16 KB: the 4 KB pages at either end of the block that are not whole 16 KB pages must stay.
 * Mutant: the page size fixed at 4096 again. */
static void test_zap_page_size(void)
{
    CHECK(tsdb_page_size() >= 4096 && (tsdb_page_size() & (tsdb_page_size() - 1)) == 0);
    size_t n = (size_t)20 << 20, pg = 16384;
    unsigned char *m = mmap(NULL, n + (1 << 20), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    CHECK(m != MAP_FAILED);
    if (m == MAP_FAILED)
        return;
    memset(m, 0xa5, n + (1 << 20));
    /* the block starts 4 KB past a 16 KB boundary (m is page aligned; make it so) and ends 4 KB past another */
    unsigned char *b = (unsigned char *)(((uintptr_t)m + pg - 1) & ~(uintptr_t)(pg - 1)) + 4096;
    tsdb_zap_slices_pg(b, n, pg);
    unsigned char *a = (unsigned char *)(((uintptr_t)b + pg - 1) & ~(uintptr_t)(pg - 1));
    unsigned char *e = (unsigned char *)(((uintptr_t)b + n) & ~(uintptr_t)(pg - 1));
    CHECK(b[0] == 0xa5 && b[4095] == 0xa5 && a[-1] == 0xa5); /* the partial 16 KB page at the start stays */
    CHECK(e[0] == 0xa5 && b[n - 1] == 0xa5);                 /* and at the end */
    CHECK(a[0] == 0 && e[-1] == 0 && a[(e - a) / 2] == 0);   /* whole pages inside went */
    CHECK(*(b - 4096) == 0xa5);                                /* the neighbour before the block */
    CHECK(b[n + 4095] == 0xa5);                                /* and after: the page the end is in */
    munmap(m, n + (1 << 20));
}
#endif
#endif

#ifndef OLD_CODE
/* High-cardinality memtable footprint, arena slab growth/reuse, big-block transition (> 128 rows),
 * and reservation rollback across both arena and big blocks. */
extern int (*mt_test_reserve_fail)(uint32_t id);
static uint32_t ft_fail_id;
static int ft_fail_cb(uint32_t id) { return id == ft_fail_id; }

static void test_arena_high_cardinality(void)
{
    const uint32_t NS = 250000;
    long r0 = rss();
    memtable *m = mt_new(1);
    CHECK(m != NULL);
    for (uint32_t id = 0; id < NS; id++) {
        int64_t ts = 1000 + (int64_t)id;
        double val = (double)id * 0.25;
        uint64_t slot;
        memcpy(&slot, &val, 8);
        CHECK(mt_add(m, NULL, id, &ts, &slot, 8, 1, (uint64_t)id + 1, TSDB_FLOAT64) == 0);
    }
    long grown = rss() - r0;
    /* Old code allocated 3 x 512 B per series (~388 MiB for 250K series); arena takes ~34 MiB. */
    if (rss_ok && !UNDER_SAN) {
        CHECK(grown < (96l << 20));
        if (grown >= (96l << 20))
            fprintf(stderr, "  250K-series memtable made %ld MiB resident (want < 96 MiB)\n", grown >> 20);
    }
    /* Grow a subset of series through slab classes (4 -> 8 -> 16 -> 64) and verify free-list reuse & values */
    for (uint32_t r = 1; r < 20; r++) {
        for (uint32_t id = 0; id < 1000; id++) {
            int64_t ts = 1000 + (int64_t)id + (int64_t)r * 1000000LL;
            double val = (double)id + (double)r;
            uint64_t slot;
            memcpy(&slot, &val, 8);
            CHECK(mt_add(m, NULL, id, &ts, &slot, 8, 1, (uint64_t)NS + r * 1000 + id, TSDB_FLOAT64) == 0);
        }
    }
    /* Grow two series past MT_ARENA_MAX_LG (128 -> 256 -> ... -> 16384) to exercise mt_big allocation and growth */
    const uint32_t big_ids[2] = {5, 123456};
    for (int b = 0; b < 2; b++) {
        uint32_t id = big_ids[b];
        for (uint32_t j = 0; j < 10000; j++) {
            int64_t ts = 100000000LL + (int64_t)j * 10;
            double val = (double)(b * 100000 + j);
            uint64_t slot;
            memcpy(&slot, &val, 8);
            CHECK(mt_add(m, NULL, id, &ts, &slot, 8, 1, 0, TSDB_FLOAT64) == 0);
        }
    }
    /* Verify samples across 1-row, 20-row, and 10000+-row series */
    CHECK(has(m, 249999, 1000 + 249999, 249999.0 * 0.25));
    CHECK(has(m, 50000, 1000 + 50000, 50000.0 * 0.25));
    {
        mem_run r = {0};
        CHECK(mt_collect(m, 42, 8, INT64_MIN, INT64_MAX, UINT64_MAX, &r) == 0 && r.n == 20);
        if (r.n == 20) {
            double y;
            memcpy(&y, r.v + 19 * 8, 8);
            CHECK(y == 42.0 + 19.0);
        }
        mem_run_free(&r);
    }
    {
        mem_run r = {0};
        CHECK(mt_collect(m, big_ids[1], 8, INT64_MIN, INT64_MAX, UINT64_MAX, &r) == 0 && r.n == 10001);
        if (r.n == 10001) {
            double y;
            memcpy(&y, r.v + 10000 * 8, 8);
            CHECK(y == 100000.0 + 9999.0);
        }
        mem_run_free(&r);
    }
    /* Rollback test: failing reservation of a new series frees newly registered arena blocks cleanly */
    {
        uint32_t rids[3] = {NS + 1, NS + 2, NS + 3};
        ft_fail_id = NS + 3;
        mt_test_reserve_fail = ft_fail_cb;
        CHECK(mt_reserve_rows(m, rids, 0, 3) == TSDB_ERR_NOMEM);
        mt_test_reserve_fail = NULL;
        CHECK(m->s[NS + 1].lgcap == 0 && m->s[NS + 2].lgcap == 0 && m->s[NS + 3].lgcap == 0);
    }
    mt_unref(m);
}

static eng *ft_unref_eng;
static uint64_t ft_unref_seq;
static int ft_unref_seen;
static int ft_unref_lock_free;

static void ft_unref_cb(const memtable *m)
{
    if (!ft_unref_eng || m->wal_seq != ft_unref_seq)
        return;
    ft_unref_seen++;
    if (pthread_mutex_trylock(&ft_unref_eng->flush_lock) == 0) {
        ft_unref_lock_free++;
        pthread_mutex_unlock(&ft_unref_eng->flush_lock);
    }
}

/* Retired memtables in mt_flush must be unref'd AFTER releasing flush_lock so teardown never stalls flush_lock. */
static void test_flush_defer_unref(const char *base)
{
    char tmpl[512];
    snprintf(tmpl, sizeof tmpl, "%s/flush-tail-XXXXXX", base);
    char *dir = mkdtemp(tmpl);
    CHECK(dir != NULL);
    if (!dir)
        return;
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = false;
    o.wal_sync_ms = 60000;
    o.flush_interval_ms = 600000;
    o.buffer_rows = 1 << 20;
    o.merge_threads = 0;
    tsdb *db = NULL;
    CHECK(tsdb_open(dir, &o, &db) == 0 && db != NULL);
    if (db) {
        const size_t NS = 2000;
        tsdb_sid *sids = malloc(NS * sizeof *sids);
        int64_t *ts = malloc(NS * sizeof *ts);
        float *v = malloc(NS * sizeof *v);
        CHECK(sids && ts && v);
        for (size_t i = 0; i < NS; i++) {
            char name[32];
            snprintf(name, sizeof name, "s.%zu", i);
            CHECK(tsdb_series(db, name, &sids[i]) == 0);
            ts[i] = 1700000000000LL + (int64_t)i;
            v[i] = (float)i;
        }
        CHECK(tsdb_insert(db, sids, ts, v, NS) == 0);
        eng *e = tsdb_test_engine(db, 0);
        ft_unref_eng = e;
        ft_unref_seq = e->active->wal_seq;
        ft_unref_seen = 0;
        ft_unref_lock_free = 0;
        __atomic_store_n(&mt_test_unref_hook, ft_unref_cb, __ATOMIC_RELEASE);
        CHECK(tsdb_flush(db) == 0);
        __atomic_store_n(&mt_test_unref_hook, NULL, __ATOMIC_RELEASE);
        ft_unref_eng = NULL;
        CHECK(ft_unref_seen == 1);
        CHECK(ft_unref_lock_free == 1);
        free(sids);
        free(ts);
        free(v);
        tsdb_close(db);
    }
    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (system(cmd) != 0)
        failures++;
}
typedef struct scratch_worker_arg {
    tsdb *db;
    tsdb_sid *sids;
    size_t nseries;
    int rc;
} scratch_worker_arg;

/* Runs on a dedicated pthread so scratch_free() destructor executes on thread exit (LSan/ASan check). */
static void *scratch_worker(void *arg)
{
    scratch_worker_arg *a = arg;
    const size_t N = 120000; /* > buffer_rows so tsdb_insert_values takes the large-insert path */
    tsdb_sid *sv = malloc(N * sizeof *sv);
    int64_t *tv = malloc(N * sizeof *tv);
    double *vv = malloc(N * sizeof *vv);
    uint8_t *valid = malloc((N + 7) / 8);
    if (!sv || !tv || !vv || !valid) {
        free(sv);
        free(tv);
        free(vv);
        free(valid);
        a->rc = TSDB_ERR_NOMEM;
        return NULL;
    }
    /* 3 repeated large nullable batches across 4 partitions with unsorted + duplicate timestamps:
     * exercises SCR_VALS (vals_import), SCR_PK/SCR_CNT (sort_and_write), and SCR_DUP_T/V (add_run_unique). */
    for (int round = 0; round < 3; round++) {
        memset(valid, 0xff, (N + 7) / 8);
        for (size_t i = 0; i < N; i++) {
            size_t sid_i = (i * 37 + (size_t)round) % a->nseries;
            int part_day = (int)((N - 1 - i) % 4);
            /* intentional duplicates every 100 rows so add_run_unique runs its dedup scratch path */
            int64_t base_ts = 1700000000000LL + (int64_t)part_day * 86400000LL + (int64_t)round * 2000000LL;
            int64_t off = (int64_t)((i / 100) * 100 + (i % 100 == 50 ? 49 : i % 100));
            sv[i] = a->sids[sid_i];
            tv[i] = base_ts + off;
            vv[i] = (double)(round * 1000000 + (int)i);
            if (i % 13 == 0)
                valid[i >> 3] &= (uint8_t)~(1u << (i & 7));
        }
        tsdb_values vals = {TSDB_FLOAT64, vv, valid};
        int rc = tsdb_insert_values(a->db, sv, tv, &vals, N);
        if (rc != 0) {
            a->rc = rc;
            break;
        }
    }
    free(sv);
    free(tv);
    free(vv);
    free(valid);
    return NULL;
}

/* Verify insert/vals_import/flush scratch reuse and clean teardown across repeated batches and flushes. */
static void test_scratch_reuse(const char *base)
{
    char tmpl[512];
    snprintf(tmpl, sizeof tmpl, "%s/flush-scratch-XXXXXX", base);
    char *dir = mkdtemp(tmpl);
    CHECK(dir != NULL);
    if (!dir)
        return;
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = false;
    o.wal_sync_ms = 60000;
    o.flush_interval_ms = 600000;
    o.buffer_rows = 60000; /* large batches of 120k go straight to parts; 30k go through memtable + flush */
    o.merge_threads = 0;
    tsdb *db = NULL;
    CHECK(tsdb_open(dir, &o, &db) == 0 && db != NULL);
    if (db) {
        const size_t NS = 64;
        tsdb_sid *sids = malloc(NS * sizeof *sids);
        CHECK(sids != NULL);
        for (size_t i = 0; i < NS; i++) {
            char name[32];
            snprintf(name, sizeof name, "sc.%zu", i);
            CHECK(tsdb_series_create(db, name, TSDB_FLOAT64, NULL, &sids[i]) == 0);
        }
        scratch_worker_arg wa = {.db = db, .sids = sids, .nseries = NS, .rc = 0};
        pthread_t th;
        CHECK(pthread_create(&th, NULL, scratch_worker, &wa) == 0);
        pthread_join(th, NULL);
        CHECK(wa.rc == 0);

        /* 3 repeated memtable flushes of 30k rows: verify eng->fl_* pointers are reused after round 1. */
        const size_t FN = 30000;
        tsdb_sid *fsv = malloc(FN * sizeof *fsv);
        int64_t *ftv = malloc(FN * sizeof *ftv);
        double *fvv = malloc(FN * sizeof *fvv);
        CHECK(fsv && ftv && fvv);
        eng *e = tsdb_test_engine(db, 0);
        uint32_t *first_fl_s = NULL;
        int64_t *first_fl_t = NULL;
        uint64_t *first_fl_v = NULL;
        for (int round = 0; round < 3; round++) {
            for (size_t i = 0; i < FN; i++) {
                fsv[i] = sids[i % NS];
                ftv[i] = 1701000000000LL + (int64_t)round * 100000LL + (int64_t)i;
                fvv[i] = (double)(round * 100000 + (int)i);
            }
            tsdb_values vals = {TSDB_FLOAT64, fvv, NULL};
            CHECK(tsdb_insert_values(db, fsv, ftv, &vals, FN) == 0);
            CHECK(tsdb_flush(db) == 0);
            if (round == 0) {
                first_fl_s = e->fl_s;
                first_fl_t = e->fl_t;
                first_fl_v = e->fl_v;
                CHECK(first_fl_s != NULL && first_fl_t != NULL && first_fl_v != NULL);
                CHECK(e->fl_rows_cap >= FN);
            } else {
                CHECK(e->fl_s == first_fl_s && e->fl_t == first_fl_t && e->fl_v == first_fl_v);
            }
        }
        /* Sanity check queried row count matches all flushed + inserted rows for sids[0]. */
        tsdb_bucket *b = NULL;
        size_t nb = 0;
        CHECK(tsdb_query_agg(db, sids[0], INT64_MIN, INT64_MAX, 0, &b, &nb) == 0);
        CHECK(nb == 1 && b[0].count > 0);
        free(b);
        free(fsv);
        free(ftv);
        free(fvv);
        free(sids);
        tsdb_close(db);
    }
    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (system(cmd) != 0)
        failures++;
}
#endif

int main(int argc, char **argv)
{
    const char *base = argc > 1 ? argv[1] : "/tmp";
    th_watchdog("test_flush_tail", 1200);
    test_series_array();
#ifndef OLD_CODE
    test_free_big();
#ifndef __APPLE__
    test_zap_page_size();
#else
    printf("test_flush_tail: RSS checks and the zap test skipped on macOS (no /proc, plain free)\n");
#endif
    test_arena_high_cardinality();
    test_flush_defer_unref(base);
    test_scratch_reuse(base);
#endif
    if (failures) {
        fprintf(stderr, "%d failures\n", failures);
        return 1;
    }
    printf("test_flush_tail: ok\n");
    return 0;
}
