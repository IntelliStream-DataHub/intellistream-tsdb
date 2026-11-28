/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Single-server local settled tier benchmark (docs/design-cluster-data.md §15.7, §15.9; docs/tasks/r8-from-r7.md;
 * PLAN.md D30, D31).
 *
 * Measures:
 *   1. settle    CPU, peak RSS/anon memory, and wall time per GiB at 1+1, 2+1, 4+2, and 1+0 (single-copy).
 *   2. reencode  Re-encode CPU, peak RSS/anon memory, and wall time per GiB (combining 4 .ec objects into 1).
 *   3. sweep     Open's orphan sweep (ecgc_open_sweep) latency and throughput at 10k and 100k objects (scaled under
 *                --quick).
 *   4. digest    Digest cost and throttling at eckeep_digest_mb_s (unlimited vs capped).
 *   5. cold      Cold decode queries (one series by hour over 116 days, and 100 series) with and without sequential
 *                granule prefetch.
 *
 * Hardware vs shim discipline (D30):
 *   --hdd PATH   Required for hardware cold-decode numbers; refused if PATH is not a rotational block device per
 *                /sys/block/.../queue/rotational (via disk_detect).
 *   (no --hdd)   Cold decode queries run on outagefs slow/rate and are labelled "shim, not hardware" in every header
 *                and result row.
 *
 * Cold cache discipline (D31):
 *   Between cold queries, the tier granule cache is emptied (eccache_test_drop) and chunk files are evicted from the
 *   OS page cache via posix_fadvise(POSIX_FADV_DONTNEED) and checked with mincore().
 *
 * Builds cleanly without TSDB_EC (NO_EC=1) and prints a skip message with exit status 0.
 */
#define _GNU_SOURCE
#include "common.h"
#include "datasets.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef TSDB_EC
int main(int argc, char **argv)
{
    (void)argc;
    (void)argv;
    printf("tier-bench: skipped (built without TSDB_EC)\n");
    return 0;
}
#else

#include "chunk.h"
#include "disk.h"
#include "eccache.h"
#include "ecgc.h"
#include "eckeep.h"
#include "ecpart.h"
#include "ecskip.h"
#include "internal.h"
#include "settle.h"
#include "tier.h"
#include "tiers.h"
#include "tsdb.h"

#ifndef __APPLE__
#include "outagefs_ctl.h"
#include <sys/mman.h>
#endif

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define MAX_GEOMS 16
#define TB_DAY_MS 86400000LL
#define TB_HOUR_MS 3600000LL
#define TB_T0 1704067200000LL /* 2024-01-01 00:00:00 UTC */
#define TB_KEY0 (TB_T0 / TB_DAY_MS)

/* Fixed engine clock at TB_T0 so background tier_after_s settles never race explicit eng_settle / eng_reencode calls. */
static int64_t tb_clock_ms(void *arg)
{
    (void)arg;
    return TB_T0;
}

typedef struct {
    unsigned k, m;
} tb_geom;

typedef struct {
    char dir[PATH_MAX];
    char hdd[PATH_MAX];
    char outagefs[PATH_MAX];
    char sysroot[PATH_MAX];
    bool has_dir;
    bool has_hdd;
    bool quick;
    bool keep;
    bool cold_child;
    unsigned want_mask; /* bits 0..4: settle, reencode, sweep, digest, cold */
    tb_geom geoms[MAX_GEOMS];
    int ngeoms;
    size_t settle_mb;
    size_t reencode_mb;
    size_t sweep_n[2];
    size_t digest_mb;
    unsigned digest_rate;
    int cold_days;
    int cold_series;
    int shim_slow_ms;
    double shim_rate_mb;
    int query_threads;
} tb_cfg;

enum {
    SEC_SETTLE = 1u << 0,
    SEC_REENCODE = 1u << 1,
    SEC_SWEEP = 1u << 2,
    SEC_DIGEST = 1u << 3,
    SEC_COLD = 1u << 4,
    SEC_ALL = (1u << 5) - 1u,
};

static void usage(void)
{
    fprintf(stderr,
            "usage: tier-bench [OPTIONS]\n"
            "  --dir PATH           scratch directory (default: $TMPDIR/tier-bench-PID)\n"
            "  --hdd PATH           real rotational hard drive for cold decode queries (verified via /sys)\n"
            "  --quick              small sizes for a fast smoke run\n"
            "  --only LIST          comma-separated subset of settle,reencode,sweep,digest,cold\n"
            "  --geoms LIST         settle geometries (default: 1+1,2+1,4+2,1+0)\n"
            "  --settle-mb N        target part size per settle geometry in MiB (default: 128; --quick: 8)\n"
            "  --reencode-mb N      target input size for re-encode in MiB (default: 128; --quick: 8)\n"
            "  --digest-mb N        target part size for digest throttling in MiB (default: 64; --quick: 8)\n"
            "  --digest-rate N      eckeep_digest_mb_s cap in MiB/s (default: 64)\n"
            "  --days N             days for cold decode hourly queries (default: 116; --quick: 12)\n"
            "  --series N           series count for multi-series cold decode (default: 100; --quick: 10)\n"
            "  --shim-slow-ms N     outagefs seek latency ms when --hdd is absent (default: 4)\n"
            "  --shim-rate-mb N     outagefs throughput cap MB/s when --hdd is absent (default: 150)\n"
            "  --outagefs PATH      path to liboutagefs.so (auto-detected beside binary or build/)\n"
            "  --keep               keep scratch directories on exit\n"
            "  -h, --help           show this help\n");
    exit(2);
}

/* ---------------------------------------------------------------- helpers */

static double thread_cpu_s(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) != 0)
        return 0.0;
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

static double proc_cpu_s(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts) != 0)
        return 0.0;
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

static int rm_cb(const char *path, const struct stat *st, int type, struct FTW *f)
{
    (void)st;
    (void)type;
    (void)f;
    return remove(path);
}

static void rm_tree(const char *path)
{
    if (!path || !*path)
        return;
    nftw(path, rm_cb, 32, FTW_DEPTH | FTW_PHYS);
}

static int mkdir_p(const char *path)
{
    char tmp[PATH_MAX];
    if (snprintf(tmp, sizeof tmp, "%s", path) >= (int)sizeof tmp)
        return -1;
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(tmp, 0755) != 0 && errno != EEXIST)
                return -1;
            *p = '/';
        }
    }
    if (mkdir(tmp, 0755) != 0 && errno != EEXIST)
        return -1;
    return 0;
}

/* Read VmRSS, VmHWM, and RssAnon (in KiB) from /proc/self/status on Linux. */
typedef struct {
    uint64_t rss_kb;
    uint64_t hwm_kb;
    uint64_t anon_kb;
} mem_snap;

static void read_mem_snap(mem_snap *out)
{
    memset(out, 0, sizeof *out);
#ifndef __APPLE__
    FILE *f = fopen("/proc/self/status", "r");
    if (!f)
        return;
    char line[256];
    while (fgets(line, sizeof line, f)) {
        unsigned long long v = 0;
        if (sscanf(line, "VmRSS: %llu kB", &v) == 1)
            out->rss_kb = (uint64_t)v;
        else if (sscanf(line, "VmHWM: %llu kB", &v) == 1)
            out->hwm_kb = (uint64_t)v;
        else if (sscanf(line, "RssAnon: %llu kB", &v) == 1)
            out->anon_kb = (uint64_t)v;
    }
    fclose(f);
#else
    struct rusage ru;
    if (getrusage(RUSAGE_SELF, &ru) == 0) {
        out->rss_kb = (uint64_t)ru.ru_maxrss / 1024u;
        out->hwm_kb = out->rss_kb;
    }
#endif
}

typedef struct {
    atomic_bool stop;
    atomic_uint_fast64_t peak_rss_kb;
    atomic_uint_fast64_t peak_anon_kb;
    double cpu_s;
    bool started;
    pthread_t th;
} mem_sampler;

static void *sampler_main(void *arg)
{
    mem_sampler *s = arg;
    double c0 = thread_cpu_s();
    struct timespec nap = {0, 2000000L}; /* 2 ms */
    while (!atomic_load_explicit(&s->stop, memory_order_relaxed)) {
        mem_snap m;
        read_mem_snap(&m);
        uint64_t pr = atomic_load_explicit(&s->peak_rss_kb, memory_order_relaxed);
        if (m.rss_kb > pr)
            atomic_store_explicit(&s->peak_rss_kb, m.rss_kb, memory_order_relaxed);
        if (m.hwm_kb > pr)
            atomic_store_explicit(&s->peak_rss_kb, m.hwm_kb, memory_order_relaxed);
        uint64_t pa = atomic_load_explicit(&s->peak_anon_kb, memory_order_relaxed);
        if (m.anon_kb > pa)
            atomic_store_explicit(&s->peak_anon_kb, m.anon_kb, memory_order_relaxed);
        nanosleep(&nap, NULL);
    }
    s->cpu_s = thread_cpu_s() - c0;
    return NULL;
}

static void sampler_start(mem_sampler *s)
{
    mem_snap m;
    read_mem_snap(&m);
    atomic_init(&s->stop, false);
    atomic_init(&s->peak_rss_kb, m.rss_kb > m.hwm_kb ? m.rss_kb : m.hwm_kb);
    atomic_init(&s->peak_anon_kb, m.anon_kb);
    s->cpu_s = 0.0;
    s->started = (pthread_create(&s->th, NULL, sampler_main, s) == 0);
}

static void sampler_stop(mem_sampler *s, double *peak_rss_mib, double *peak_anon_mib, double *sampler_cpu_s)
{
    if (s->started) {
        atomic_store_explicit(&s->stop, true, memory_order_relaxed);
        pthread_join(s->th, NULL);
        s->started = false;
    }
    mem_snap m;
    read_mem_snap(&m);
    uint64_t pr = atomic_load_explicit(&s->peak_rss_kb, memory_order_relaxed);
    if (m.rss_kb > pr)
        pr = m.rss_kb;
    if (m.hwm_kb > pr)
        pr = m.hwm_kb;
    uint64_t pa = atomic_load_explicit(&s->peak_anon_kb, memory_order_relaxed);
    if (m.anon_kb > pa)
        pa = m.anon_kb;
    *peak_rss_mib = (double)pr / 1024.0;
    *peak_anon_mib = (double)pa / 1024.0;
    if (sampler_cpu_s)
        *sampler_cpu_s = s->cpu_s;
}

/* ---------------------------------------------------------------- cold cache eviction (D31) */

#ifndef __APPLE__
static uint64_t drop_files_total;
static uint64_t drop_pages_total;
static uint64_t drop_pages_resident;

static int drop_file_cb(const char *path, const struct stat *st, int type, struct FTW *f)
{
    (void)f;
    if (type != FTW_F || !st || st->st_size <= 0)
        return 0;
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return 0;
    (void)posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
    long psz = sysconf(_SC_PAGESIZE);
    if (psz > 0) {
        size_t len = (size_t)st->st_size;
        void *map = mmap(NULL, len, PROT_READ, MAP_SHARED, fd, 0);
        if (map != MAP_FAILED) {
            size_t npages = (len + (size_t)psz - 1u) / (size_t)psz;
            unsigned char *vec = malloc(npages);
            if (vec) {
                if (mincore(map, len, vec) == 0) {
                    drop_pages_total += npages;
                    for (size_t i = 0; i < npages; i++)
                        if (vec[i] & 1u)
                            drop_pages_resident++;
                }
                free(vec);
            }
            munmap(map, len);
        }
    }
    close(fd);
    drop_files_total++;
    return 0;
}
#endif

static void drop_dir_cache(const char *dir, uint64_t *res_pages_out)
{
    if (res_pages_out)
        *res_pages_out = 0;
#ifndef __APPLE__
    drop_files_total = 0;
    drop_pages_total = 0;
    drop_pages_resident = 0;
    nftw(dir, drop_file_cb, 32, FTW_PHYS);
    if (res_pages_out)
        *res_pages_out = drop_pages_resident;
#else
    (void)dir;
#endif
}

/* ---------------------------------------------------------------- HDD check (D30) */

static int verify_hdd_path(const char *path, const char *sysroot)
{
    int cls = DISK_CLASS_NONE;
    char why[256] = "";
    int rc = disk_detect(path, sysroot && *sysroot ? sysroot : NULL, &cls, why, sizeof why);
    if (rc != 0) {
        fprintf(stderr,
                "tier-bench: --hdd %s: refused: cannot verify rotational hard drive via /sys/block/.../queue/rotational "
                "(%s)\n",
                path, why[0] ? why : tsdb_strerror(rc));
        return -1;
    }
    if (cls != DISK_CLASS_HDD) {
        fprintf(stderr,
                "tier-bench: --hdd %s: refused: device class is '%s' (non-rotational per /sys/block/.../queue/rotational); "
                "hard-drive numbers require a real rotational drive (D30)\n",
                path, disk_class_name(cls));
        return -1;
    }
    return 0;
}

/* ---------------------------------------------------------------- DB + tier setup helpers */

typedef struct {
    tier *t;
    eng *db;
} tb_eng;

static int setup_tiered_db(const char *db_dir, const char *stores_root, unsigned k, unsigned m, bool allow_shared)
{
    unsigned nstores = k + m;
    if (mkdir_p(db_dir) != 0 || mkdir_p(stores_root) != 0) {
        fprintf(stderr, "tier-bench: mkdir failed for %s / %s: %s\n", db_dir, stores_root, strerror(errno));
        return -1;
    }
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = 0;
    o.partition_classes = 0;
    o.create_volumes = 0;
    o.obsolete_part_lifetime_s = 0;
    tsdb *tdb = NULL;
    int rc = tsdb_open(db_dir, &o, &tdb);
    if (rc != 0) {
        fprintf(stderr, "tier-bench: tsdb_open(%s) failed: %s\n", db_dir, tsdb_strerror(rc));
        return -1;
    }
    tsdb_close(tdb);

    uint32_t flags = (allow_shared ? TSDB_TIER_ALLOW_SHARED_DEV : 0u) | (m == 0 ? TSDB_TIER_SINGLE_COPY : 0u);
    for (unsigned i = 0; i < nstores; i++) {
        char st[PATH_MAX];
        if (snprintf(st, sizeof st, "%s/s%u", stores_root, i) >= (int)sizeof st)
            return -1;
        rc = tsdb_tier_add_dir(db_dir, st, "hdd", flags);
        if (rc != 0) {
            fprintf(stderr, "tier-bench: tsdb_tier_add_dir(%s) failed: %s\n", st, tsdb_strerror(rc));
            return -1;
        }
    }

    int def_k = 0, def_m = 0;
    if (!tier_default_geom((int)nstores, m == 0, &def_k, &def_m) || (unsigned)def_k != k || (unsigned)def_m != m) {
        int dfd = open(db_dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (dfd < 0)
            return -1;
        tiers_settings ts;
        tiers_settings_defaults(&ts);
        ts.set_mask = TSDB_TIER_SET_REDUNDANCY;
        ts.k = (uint16_t)k;
        ts.m = (uint16_t)m;
        rc = tiers_append_settings(dfd, true, TIERS_VOL_ALL, &ts);
        close(dfd);
        if (rc != 0) {
            fprintf(stderr, "tier-bench: tiers_append_settings(%u+%u) failed: %s\n", k, m, tsdb_strerror(rc));
            return -1;
        }
    }
    return 0;
}

static int open_tiered_eng(const char *db_dir, int merge_threads, int query_threads, tb_eng *out)
{
    memset(out, 0, sizeof *out);
    tsdb_options o;
    tsdb_options_init(&o);
    o.merge_threads = merge_threads;
    o.query_threads = query_threads;
    o.partition_classes = 0;
    o.create_volumes = 0;
    o.obsolete_part_lifetime_s = 0;
    o.wal_sync_ms = -1; /* fast load; chunk writes still fsync inside chunk_wr_commit */
    o.clock_ms = tb_clock_ms;

    int rc = tier_open(db_dir, &o, &out->t);
    if (rc != 0 || !out->t) {
        fprintf(stderr, "tier-bench: tier_open(%s) failed: %s\n", db_dir, tsdb_strerror(rc));
        return -1;
    }
    tier_want(out->t, 0);
    rc = eng_open(db_dir, &o, &out->db);
    tier_want(NULL, 0);
    if (rc != 0 || !out->db) {
        fprintf(stderr, "tier-bench: eng_open(%s) failed: %s\n", db_dir, tsdb_strerror(rc));
        tier_put(out->t);
        out->t = NULL;
        return -1;
    }
    return 0;
}

static void close_tiered_eng(tb_eng *e)
{
    if (e->t)
        tier_quiesce(e->t);
    if (e->db)
        (void)eng_close(e->db);
    if (e->t)
        tier_put(e->t);
    e->db = NULL;
    e->t = NULL;
}

/* Collect all listed parts of partition `key` (or all keys when all_keys is true) matching ec_only. Caller unrefs. */
static size_t collect_parts(eng *db, int64_t key, bool all_keys, bool want_ec, part **out, size_t cap)
{
    size_t n = 0;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts && n < cap; i++) {
        part *p = db->parts[i];
        if (p->dropped)
            continue;
        if (!all_keys && p->partition != key)
            continue;
        bool is_ec = part_is_ec(p);
        if (is_ec != want_ec)
            continue;
        part_ref(p);
        out[n++] = p;
    }
    pthread_mutex_unlock(&db->lock);
    return n;
}

static void unref_parts(part **ps, size_t n)
{
    for (size_t i = 0; i < n; i++)
        part_unref(ps[i]);
}

static uint64_t sum_part_bytes(eng *db, bool include_ec)
{
    uint64_t tot = 0;
    pthread_mutex_lock(&db->lock);
    for (size_t i = 0; i < db->nparts; i++) {
        part *p = db->parts[i];
        if (p->dropped)
            continue;
        if (part_is_ec(p)) {
            if (include_ec)
                tot += eccache_part_bytes(p);
        } else {
            tot += p->map_len;
        }
    }
    pthread_mutex_unlock(&db->lock);
    return tot;
}

/* Populate day partitions starting at day 0 with realistic mixed sensor series until on-disk .part bytes >= target_bytes. */
static int populate_to_bytes(eng *db, uint64_t target_bytes, int nseries, int64_t start_day, int64_t *days_used_out)
{
    uint32_t *sids = calloc((size_t)nseries, sizeof *sids);
    ds_state *st = calloc((size_t)nseries, sizeof *st);
    const size_t batch = 65536;
    uint32_t *sv = malloc(batch * sizeof *sv);
    int64_t *tv = malloc(batch * sizeof *tv);
    float *vv = malloc(batch * sizeof *vv);
    if (!sids || !st || !sv || !tv || !vv) {
        free(sids);
        free(st);
        free(sv);
        free(tv);
        free(vv);
        return -1;
    }
    for (int s = 0; s < nseries; s++) {
        char name[64];
        snprintf(name, sizeof name, "sensor.%04d", s);
        if (eng_series(db, name, TSDB_FLOAT32, &sids[s]) != 0)
            goto fail;
        ds_init(&st[s], (ds_kind)(s % (DS_KINDS - 1)), (uint32_t)s, TB_T0 + start_day * TB_DAY_MS);
        st[s].stamp_ms = 1000; /* 1 s samples -> 86,400 rows/day/series */
    }

    int64_t day = start_day;
    const int64_t pts_per_day = 86390;
    for (;;) {
        int64_t day_t0 = TB_T0 + day * TB_DAY_MS + 1000LL; /* +1s keeps -jitter inside the day partition */
        for (int s = 0; s < nseries; s++) {
            st[s].t0 = day_t0;
            st[s].i = 0;
        }
        for (int64_t i0 = 0; i0 < pts_per_day;) {
            size_t n = 0;
            int64_t step_pts = (int64_t)(batch / (size_t)nseries);
            if (step_pts < 1)
                step_pts = 1;
            int64_t i1 = i0 + step_pts;
            if (i1 > pts_per_day)
                i1 = pts_per_day;
            for (int64_t i = i0; i < i1; i++) {
                for (int s = 0; s < nseries; s++) {
                    sv[n] = sids[s];
                    ds_next(&st[s], &tv[n], &vv[n]);
                    n++;
                }
            }
            if (eng_insert(db, TSDB_FLOAT32, sv, tv, vv, n) != 0)
                goto fail;
            i0 = i1;
        }
        if (eng_flush(db) != 0)
            goto fail;
        day++;
        if (sum_part_bytes(db, false) >= target_bytes)
            break;
        if (day - start_day >= 512)
            break;
    }
    if (eng_optimize(db) != 0)
        goto fail;
    if (days_used_out)
        *days_used_out = day - start_day;
    free(sids);
    free(st);
    free(sv);
    free(tv);
    free(vv);
    return 0;
fail:
    free(sids);
    free(st);
    free(sv);
    free(tv);
    free(vv);
    return -1;
}

/* ---------------------------------------------------------------- 1. Settle per GiB */

typedef struct {
    int rc;
    uint64_t bytes;
    double wall_s;
    double cpu_s;
    double rss_peak_mib;
    double anon_peak_mib;
} settle_res;

static int settle_child_measure(const char *db_dir, settle_res *res)
{
    memset(res, 0, sizeof *res);
    atomic_store(&settle_poll_ms, 60000);
    atomic_store(&eckeep_test_pause, true);

    tb_eng e;
    if (open_tiered_eng(db_dir, 0, 1, &e) != 0) {
        res->rc = -1;
        return -1;
    }

    part *ps[1024];
    size_t np = collect_parts(e.db, 0, true, false, ps, 1024);
    if (np == 0) {
        close_tiered_eng(&e);
        res->rc = -1;
        return -1;
    }
    uint64_t bytes = 0;
    for (size_t i = 0; i < np; i++)
        bytes += ps[i]->map_len;

    /* Group by partition key so each eng_settle call gets one partition key's parts. */
    int64_t keys[1024];
    size_t nkeys = 0;
    for (size_t i = 0; i < np; i++) {
        bool seen = false;
        for (size_t j = 0; j < nkeys; j++)
            if (keys[j] == ps[i]->partition) {
                seen = true;
                break;
            }
        if (!seen)
            keys[nkeys++] = ps[i]->partition;
    }

    mem_sampler smp;
    sampler_start(&smp);
    double w0 = now_s();
    double c0 = proc_cpu_s();

    size_t tot_settled = 0;
    int rc = 0;
    for (size_t ki = 0; ki < nkeys && rc == 0; ki++) {
        part *kps[256];
        size_t kn = 0;
        for (size_t i = 0; i < np && kn < 256; i++)
            if (ps[i]->partition == keys[ki])
                kps[kn++] = ps[i];
        size_t got = 0;
        rc = eng_settle(e.db, keys[ki], kps, kn, &got);
        tot_settled += got;
    }

    double c1 = proc_cpu_s();
    double w1 = now_s();
    double rss_pk = 0, anon_pk = 0, smp_cpu = 0;
    sampler_stop(&smp, &rss_pk, &anon_pk, &smp_cpu);
    double cpu_s = (c1 - c0) - smp_cpu;
    if (cpu_s < 0.0)
        cpu_s = 0.0;

    unref_parts(ps, np);
    close_tiered_eng(&e);

    if (rc != 0 || tot_settled != np) {
        fprintf(stderr, "tier-bench: eng_settle failed (rc=%d, settled=%zu/%zu)\n", rc, tot_settled, np);
        res->rc = -1;
        return -1;
    }
    res->rc = 0;
    res->bytes = bytes;
    res->wall_s = w1 - w0;
    res->cpu_s = cpu_s;
    res->rss_peak_mib = rss_pk;
    res->anon_peak_mib = anon_pk;
    return 0;
}

static int run_settle_bench(const tb_cfg *cfg)
{
    printf("\n== settle (per GiB of part bytes; warm page cache) ==\n");
    printf("%-6s %10s %9s %11s %10s %9s %13s %14s\n", "geom", "bytes_MiB", "wall_s", "wall_s/GiB", "cpu_s/GiB", "MiB/s",
           "rss_peak_MiB", "anon_peak_MiB");

    uint64_t target_bytes = (uint64_t)cfg->settle_mb << 20;
    int nseries = cfg->quick ? 16 : 64;

    for (int gi = 0; gi < cfg->ngeoms; gi++) {
        unsigned k = cfg->geoms[gi].k, m = cfg->geoms[gi].m;
        char gdir[PATH_MAX], db_dir[PATH_MAX], st_root[PATH_MAX];
        if (snprintf(gdir, sizeof gdir, "%s/settle_%u_%u", cfg->dir, k, m) >= (int)sizeof gdir ||
            snprintf(db_dir, sizeof db_dir, "%s/db", gdir) >= (int)sizeof db_dir ||
            snprintf(st_root, sizeof st_root, "%s/stores", gdir) >= (int)sizeof st_root)
            return -1;
        rm_tree(gdir);
        if (setup_tiered_db(db_dir, st_root, k, m, true) != 0)
            return -1;

        /* Phase 1: populate & optimize in a child process so parent RSS stays minimal and constant across geometries. */
        fflush(stdout);
        fflush(stderr);
        pid_t prep_pid = fork();
        if (prep_pid < 0)
            return -1;
        if (prep_pid == 0) {
            atomic_store(&settle_poll_ms, 60000);
            atomic_store(&eckeep_test_pause, true);
            tb_eng e;
            if (open_tiered_eng(db_dir, 2, 1, &e) != 0)
                _exit(1);
            int prc = populate_to_bytes(e.db, target_bytes, nseries, 0, NULL);
            close_tiered_eng(&e);
            _exit(prc == 0 ? 0 : 1);
        }
        int pst = 0;
        while (waitpid(prep_pid, &pst, 0) < 0 && errno == EINTR)
            ;
        if (!WIFEXITED(pst) || WEXITSTATUS(pst) != 0) {
            if (!cfg->keep)
                rm_tree(gdir);
            fprintf(stderr, "tier-bench: settle %u+%u populate failed\n", k, m);
            return -1;
        }

        /* Phase 2: fork a clean child for isolated peak RSS/anon & settle timing. */
        int pfd[2];
        if (pipe(pfd) != 0)
            return -1;
        fflush(stdout);
        fflush(stderr);
        pid_t pid = fork();
        if (pid < 0) {
            close(pfd[0]);
            close(pfd[1]);
            return -1;
        }
        if (pid == 0) {
            close(pfd[0]);
            settle_res res;
            (void)settle_child_measure(db_dir, &res);
            ssize_t w = write(pfd[1], &res, sizeof res);
            (void)w;
            close(pfd[1]);
            _exit(res.rc == 0 ? 0 : 1);
        }
        close(pfd[1]);
        settle_res res;
        memset(&res, 0, sizeof res);
        res.rc = -1;
        ssize_t got = 0;
        while (got < (ssize_t)sizeof res) {
            ssize_t r = read(pfd[0], (char *)&res + got, sizeof res - (size_t)got);
            if (r < 0 && errno == EINTR)
                continue;
            if (r <= 0)
                break;
            got += r;
        }
        close(pfd[0]);
        int st = 0;
        while (waitpid(pid, &st, 0) < 0 && errno == EINTR)
            ;
        if (!cfg->keep)
            rm_tree(gdir);
        if (!WIFEXITED(st) || WEXITSTATUS(st) != 0 || res.rc != 0 || res.bytes == 0) {
            fprintf(stderr, "tier-bench: settle %u+%u failed\n", k, m);
            return -1;
        }

        double gib = (double)res.bytes / 1073741824.0;
        double mib = (double)res.bytes / 1048576.0;
        double wall_gib = res.wall_s / gib;
        double cpu_gib = res.cpu_s / gib;
        double mib_s = res.wall_s > 0 ? mib / res.wall_s : 0.0;

        char glbl[16];
        snprintf(glbl, sizeof glbl, "%u+%u", k, m);
        printf("%-6s %10.2f %9.3f %11.2f %10.2f %9.1f %13.1f %14.1f\n", glbl, mib, res.wall_s, wall_gib, cpu_gib,
               mib_s, res.rss_peak_mib, res.anon_peak_mib);
        printf("TIER_RESULT section=settle row=\"settle %s\" bytes_mib=%.3f wall_s=%.4f wall_s_gib=%.3f "
               "cpu_s_gib=%.3f mib_s=%.2f rss_peak_mib=%.2f anon_peak_mib=%.2f\n",
               glbl, mib, res.wall_s, wall_gib, cpu_gib, mib_s, res.rss_peak_mib, res.anon_peak_mib);
        fflush(stdout);
    }
    return 0;
}

/* ---------------------------------------------------------------- 2. Re-encode per GiB */

static int reencode_child_measure(const char *db_dir, int64_t key, size_t expect_n, settle_res *res)
{
    memset(res, 0, sizeof *res);
    atomic_store(&settle_poll_ms, 60000);
    atomic_store(&settle_reencode_pause, true);
    atomic_store(&eckeep_test_pause, true);

    tb_eng e;
    if (open_tiered_eng(db_dir, 0, 1, &e) != 0) {
        res->rc = -1;
        return -1;
    }

    part *ps[64];
    size_t np = collect_parts(e.db, key, false, true, ps, 64);
    if (np != expect_n) {
        fprintf(stderr, "tier-bench: reencode expected %zu .ec parts, found %zu\n", expect_n, np);
        unref_parts(ps, np);
        close_tiered_eng(&e);
        res->rc = -1;
        return -1;
    }
    uint64_t bytes = 0;
    for (size_t i = 0; i < np; i++)
        bytes += eccache_part_bytes(ps[i]);

    eccache_test_drop(e.t);

    mem_sampler smp;
    sampler_start(&smp);
    double w0 = now_s();
    double c0 = proc_cpu_s();

    size_t got = 0;
    int rc = eng_reencode(e.db, key, ps, np, &got);

    double c1 = proc_cpu_s();
    double w1 = now_s();
    double rss_pk = 0, anon_pk = 0, smp_cpu = 0;
    sampler_stop(&smp, &rss_pk, &anon_pk, &smp_cpu);
    double cpu_s = (c1 - c0) - smp_cpu;
    if (cpu_s < 0.0)
        cpu_s = 0.0;

    unref_parts(ps, np);
    close_tiered_eng(&e);

    if (rc != 0 || got != np) {
        fprintf(stderr, "tier-bench: eng_reencode failed (rc=%d, got=%zu/%zu)\n", rc, got, np);
        res->rc = -1;
        return -1;
    }
    res->rc = 0;
    res->bytes = bytes;
    res->wall_s = w1 - w0;
    res->cpu_s = cpu_s;
    res->rss_peak_mib = rss_pk;
    res->anon_peak_mib = anon_pk;
    return 0;
}

static int reencode_prep_child(const char *db_dir, const tb_cfg *cfg, size_t nobj)
{
    atomic_store(&settle_poll_ms, 60000);
    atomic_store(&settle_reencode_pause, true);
    atomic_store(&eckeep_test_pause, true);

    tb_eng e;
    if (open_tiered_eng(db_dir, 0, 1, &e) != 0)
        return -1;

    int nseries = cfg->quick ? 32 : 256;
    uint32_t *sids = calloc((size_t)nseries, sizeof *sids);
    ds_state *st = calloc((size_t)nseries, sizeof *st);
    const size_t batch = 65536;
    uint32_t *sv = malloc(batch * sizeof *sv);
    int64_t *tv = malloc(batch * sizeof *tv);
    float *vv = malloc(batch * sizeof *vv);
    if (!sids || !st || !sv || !tv || !vv) {
        free(sids);
        free(st);
        free(sv);
        free(tv);
        free(vv);
        close_tiered_eng(&e);
        return -1;
    }

    for (int s = 0; s < nseries; s++) {
        char name[64];
        snprintf(name, sizeof name, "reenc.%04d", s);
        if (eng_series(e.db, name, TSDB_FLOAT32, &sids[s]) != 0) {
            free(sids);
            free(st);
            free(sv);
            free(tv);
            free(vv);
            close_tiered_eng(&e);
            return -1;
        }
        ds_init(&st[s], (ds_kind)(s % (DS_KINDS - 1)), (uint32_t)s, TB_T0 + 1000LL);
        /* 200 ms spacing satisfies |jitter| <= 50 ms < step/2 (datasets.h) while staying inside TB_KEY0. */
        st[s].stamp_ms = 200;
    }

    uint64_t per_obj_bytes = ((uint64_t)cfg->reencode_mb << 20) / nobj;
    if (per_obj_bytes < (512u << 10))
        per_obj_bytes = 512u << 10;

    int64_t pt_idx = 0;
    for (size_t oi = 0; oi < nobj; oi++) {
        /* Insert chunks into partition TB_KEY0 until newly flushed .part(s) reach per_obj_bytes, optimize, then settle. */
        uint64_t start_bytes = sum_part_bytes(e.db, false);
        while (sum_part_bytes(e.db, false) - start_bytes < per_obj_bytes) {
            size_t n = 0;
            int64_t step_pts = (int64_t)(batch / (size_t)nseries);
            if (step_pts < 1)
                step_pts = 1;
            /* Stay strictly inside partition TB_KEY0 (86,400,000 ms at 200 ms/sample -> 431,000 samples max). */
            if (pt_idx + step_pts >= 431000)
                break;
            for (int64_t i = 0; i < step_pts; i++) {
                for (int s = 0; s < nseries; s++) {
                    sv[n] = sids[s];
                    ds_next(&st[s], &tv[n], &vv[n]);
                    n++;
                }
            }
            pt_idx += step_pts;
            if (eng_insert(e.db, TSDB_FLOAT32, sv, tv, vv, n) != 0) {
                free(sids);
                free(st);
                free(sv);
                free(tv);
                free(vv);
                close_tiered_eng(&e);
                return -1;
            }
            if (eng_flush(e.db) != 0) {
                free(sids);
                free(st);
                free(sv);
                free(tv);
                free(vv);
                close_tiered_eng(&e);
                return -1;
            }
        }
        /* Merge only the newly inserted young .part files into 1 .part (merges skip .ec parts), then settle it. */
        if (eng_optimize(e.db) != 0) {
            free(sids);
            free(st);
            free(sv);
            free(tv);
            free(vv);
            close_tiered_eng(&e);
            return -1;
        }
        part *yp[64];
        size_t nyp = collect_parts(e.db, TB_KEY0, false, false, yp, 64);
        size_t got = 0;
        int rc = nyp ? eng_settle(e.db, TB_KEY0, yp, nyp, &got) : TSDB_ERR_NOTFOUND;
        unref_parts(yp, nyp);
        if (rc != 0 || got == 0) {
            fprintf(stderr, "tier-bench: reencode prep settle %zu failed (rc=%d, got=%zu)\n", oi, rc, got);
            free(sids);
            free(st);
            free(sv);
            free(tv);
            free(vv);
            close_tiered_eng(&e);
            return -1;
        }
    }

    free(sids);
    free(st);
    free(sv);
    free(tv);
    free(vv);
    close_tiered_eng(&e);
    return 0;
}

static int run_reencode_bench(const tb_cfg *cfg)
{
    unsigned k = 4, m = 2;
    const size_t nobj = 4;
    char rdir[PATH_MAX], db_dir[PATH_MAX], st_root[PATH_MAX];
    if (snprintf(rdir, sizeof rdir, "%s/reencode", cfg->dir) >= (int)sizeof rdir ||
        snprintf(db_dir, sizeof db_dir, "%s/db", rdir) >= (int)sizeof db_dir ||
        snprintf(st_root, sizeof st_root, "%s/stores", rdir) >= (int)sizeof st_root)
        return -1;
    rm_tree(rdir);
    if (setup_tiered_db(db_dir, st_root, k, m, true) != 0)
        return -1;

    /* Create 4 distinct settled .ec objects in a child process so parent RSS stays minimal. */
    fflush(stdout);
    fflush(stderr);
    pid_t prep_pid = fork();
    if (prep_pid < 0)
        return -1;
    if (prep_pid == 0)
        _exit(reencode_prep_child(db_dir, cfg, nobj) == 0 ? 0 : 1);
    int pst = 0;
    while (waitpid(prep_pid, &pst, 0) < 0 && errno == EINTR)
        ;
    if (!WIFEXITED(pst) || WEXITSTATUS(pst) != 0) {
        if (!cfg->keep)
            rm_tree(rdir);
        fprintf(stderr, "tier-bench: reencode prep failed\n");
        return -1;
    }

    /* Fork clean child to measure eng_reencode. */
    int pfd[2];
    if (pipe(pfd) != 0)
        return -1;
    fflush(stdout);
    fflush(stderr);
    pid_t pid = fork();
    if (pid < 0) {
        close(pfd[0]);
        close(pfd[1]);
        return -1;
    }
    if (pid == 0) {
        close(pfd[0]);
        settle_res res;
        (void)reencode_child_measure(db_dir, TB_KEY0, nobj, &res);
        ssize_t w = write(pfd[1], &res, sizeof res);
        (void)w;
        close(pfd[1]);
        _exit(res.rc == 0 ? 0 : 1);
    }
    close(pfd[1]);
    settle_res res;
    memset(&res, 0, sizeof res);
    res.rc = -1;
    ssize_t got = 0;
    while (got < (ssize_t)sizeof res) {
        ssize_t r = read(pfd[0], (char *)&res + got, sizeof res - (size_t)got);
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0)
            break;
        got += r;
    }
    close(pfd[0]);
    int st_w = 0;
    while (waitpid(pid, &st_w, 0) < 0 && errno == EINTR)
        ;
    if (!cfg->keep)
        rm_tree(rdir);
    if (!WIFEXITED(st_w) || WEXITSTATUS(st_w) != 0 || res.rc != 0 || res.bytes == 0) {
        fprintf(stderr, "tier-bench: reencode failed\n");
        return -1;
    }

    double gib = (double)res.bytes / 1073741824.0;
    double mib = (double)res.bytes / 1048576.0;
    double wall_gib = res.wall_s / gib;
    double cpu_gib = res.cpu_s / gib;
    double mib_s = res.wall_s > 0 ? mib / res.wall_s : 0.0;

    printf("\n== reencode (%zu .ec objects -> 1, per GiB of input bytes; warm page cache) ==\n", nobj);
    printf("%-6s %6s %10s %9s %11s %10s %9s %13s %14s\n", "geom", "parts", "bytes_MiB", "wall_s", "wall_s/GiB",
           "cpu_s/GiB", "MiB/s", "rss_peak_MiB", "anon_peak_MiB");
    printf("%u+%u    %6zu %10.2f %9.3f %11.2f %10.2f %9.1f %13.1f %14.1f\n", k, m, nobj, mib, res.wall_s, wall_gib,
           cpu_gib, mib_s, res.rss_peak_mib, res.anon_peak_mib);
    printf("TIER_RESULT section=reencode row=\"reencode %u+%u (%zu objects)\" bytes_mib=%.3f wall_s=%.4f "
           "wall_s_gib=%.3f cpu_s_gib=%.3f mib_s=%.2f rss_peak_mib=%.2f anon_peak_mib=%.2f\n",
           k, m, nobj, mib, res.wall_s, wall_gib, cpu_gib, mib_s, res.rss_peak_mib, res.anon_peak_mib);
    fflush(stdout);
    return 0;
}

/* ---------------------------------------------------------------- 3. Open's orphan sweep */

static int write_file_all(int dirfd, const char *name, const uint8_t *buf, size_t n)
{
    int fd = openat(dirfd, name, O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0)
        return -1;
    size_t off = 0;
    while (off < n) {
        ssize_t w = write(fd, buf + off, n - off);
        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0) {
            close(fd);
            return -1;
        }
        off += (size_t)w;
    }
    close(fd);
    return 0;
}

static int run_sweep_bench(const tb_cfg *cfg)
{
    printf("\n== orphan sweep (ecgc_open_sweep) ==\n");
    printf("%10s %10s %10s %10s %12s\n", "objects", "swept", "wall_ms", "cpu_ms", "objects/s");

    const unsigned k = 2, m = 1, nstores = 3;
    for (int si = 0; si < 2; si++) {
        size_t nobj = cfg->sweep_n[si];
        char sdir[PATH_MAX], db_dir[PATH_MAX], st_root[PATH_MAX];
        if (snprintf(sdir, sizeof sdir, "%s/sweep_%zu", cfg->dir, nobj) >= (int)sizeof sdir ||
            snprintf(db_dir, sizeof db_dir, "%s/db", sdir) >= (int)sizeof db_dir ||
            snprintf(st_root, sizeof st_root, "%s/stores", sdir) >= (int)sizeof st_root)
            return -1;
        rm_tree(sdir);
        if (setup_tiered_db(db_dir, st_root, k, m, true) != 0)
            return -1;

        /* Open once to initialize volume 0's vuuid and store s0 directories. */
        atomic_store(&ecgc_test_manual, true);
        atomic_store(&ecgc_test_nosweep, true);
        atomic_store(&settle_poll_ms, 60000);
        atomic_store(&eckeep_test_pause, true);

        tb_eng e;
        if (open_tiered_eng(db_dir, 0, 1, &e) != 0)
            return -1;
        uint8_t vuuid[16];
        memcpy(vuuid, tier_vuuid(e.db), 16);
        chunk_backend *be = tier_backend(e.t);

        /* Prepare s0/ directories inside each store and write `nobj` tiny valid orphan objects directly.
         * For len=64 under 2+1, chunk_geom_derive gives k'=1, m=1 (2 positions per object). */
        chunk_geom g;
        if (chunk_geom_derive(&g, k, m, 64) != 0) {
            close_tiered_eng(&e);
            return -1;
        }
        uint8_t payload[64];
        memset(payload, 0x5a, sizeof payload);
        uint64_t sum = xxh64(payload, sizeof payload, 0);

        int s0fds[3] = {-1, -1, -1};
        for (unsigned i = 0; i < nstores; i++) {
            char p[PATH_MAX];
            if (snprintf(p, sizeof p, "%s/s%u/s0", st_root, i) >= (int)sizeof p || mkdir_p(p) != 0) {
                for (unsigned j = 0; j < i; j++)
                    close(s0fds[j]);
                close_tiered_eng(&e);
                return -1;
            }
            s0fds[i] = open(p, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
            if (s0fds[i] < 0) {
                for (unsigned j = 0; j < i; j++)
                    close(s0fds[j]);
                close_tiered_eng(&e);
                return -1;
            }
        }

        unsigned npos = chunk_npos(&g);
        for (size_t oi = 0; oi < nobj; oi++) {
            uint8_t oid[16];
            memset(oid, 0, sizeof oid);
            uint64_t v0 = 0x5357454550000000ull ^ (uint64_t)oi;
            uint64_t v1 = ds_mix((uint64_t)oi + 1u);
            memcpy(oid, &v0, 8);
            memcpy(oid + 8, &v1, 8);
            char hex[33];
            chunk_oid_hex(oid, hex);

            for (unsigned pos = 0; pos < npos; pos++) {
                int slot = chunk_be_place(be, oid, pos);
                chunk_hdr h;
                memset(&h, 0, sizeof h);
                memcpy(h.vuuid, vuuid, 16);
                memcpy(h.oid, oid, 16);
                h.epoch = 1;
                h.g = g;
                h.pos = (uint16_t)pos;
                h.chunk_len = sizeof payload;
                size_t img_len = 0;
                uint8_t *img = chunk_image_build(&h, payload, &sum, &img_len);
                if (!img) {
                    for (unsigned j = 0; j < nstores; j++)
                        close(s0fds[j]);
                    close_tiered_eng(&e);
                    return -1;
                }
                char fname[64];
                snprintf(fname, sizeof fname, "o%s.%u.chunk", hex, pos);
                int wrc = write_file_all(s0fds[slot], fname, img, img_len);
                free(img);
                if (wrc != 0) {
                    for (unsigned j = 0; j < nstores; j++)
                        close(s0fds[j]);
                    close_tiered_eng(&e);
                    return -1;
                }
            }
        }
        for (unsigned j = 0; j < nstores; j++)
            close(s0fds[j]);

        /* Time ecgc_open_sweep directly on the open engine (with manual GC so deletes don't run concurrently). */
        atomic_store(&ecgc_test_nosweep, false);
        ecgc_counters c0, c1;
        ecgc_get_counters(e.t, &c0);

        double w0 = now_s();
        double t0 = thread_cpu_s();
        int rc = ecgc_open_sweep(e.db);
        double t1 = thread_cpu_s();
        double w1 = now_s();

        ecgc_get_counters(e.t, &c1);
        uint64_t swept = c1.swept - c0.swept;
        close_tiered_eng(&e);
        atomic_store(&ecgc_test_manual, false);
        atomic_store(&ecgc_test_nosweep, false);

        if (!cfg->keep)
            rm_tree(sdir);
        if (rc != 0 || swept != nobj) {
            fprintf(stderr, "tier-bench: ecgc_open_sweep failed (rc=%d, swept=%" PRIu64 "/%zu)\n", rc, swept, nobj);
            return -1;
        }

        double wall_ms = (w1 - w0) * 1e3;
        double cpu_ms = (t1 - t0) * 1e3;
        double objs_s = (w1 - w0) > 0 ? (double)nobj / (w1 - w0) : 0.0;

        printf("%10zu %10" PRIu64 " %10.2f %10.2f %12.1f\n", nobj, swept, wall_ms, cpu_ms, objs_s);
        printf("TIER_RESULT section=sweep row=\"orphan sweep %zu objects\" objects=%zu swept=%" PRIu64
               " wall_ms=%.3f cpu_ms=%.3f objs_s=%.2f\n",
               nobj, nobj, swept, wall_ms, cpu_ms, objs_s);
        fflush(stdout);
    }
    return 0;
}

/* ---------------------------------------------------------------- 4. Digest throttling (eckeep_digest_mb_s) */

static int run_digest_bench(const tb_cfg *cfg)
{
    const unsigned k = 4, m = 2;
    char ddir[PATH_MAX], db_dir[PATH_MAX], st_root[PATH_MAX];
    if (snprintf(ddir, sizeof ddir, "%s/digest", cfg->dir) >= (int)sizeof ddir ||
        snprintf(db_dir, sizeof db_dir, "%s/db", ddir) >= (int)sizeof db_dir ||
        snprintf(st_root, sizeof st_root, "%s/stores", ddir) >= (int)sizeof st_root)
        return -1;
    rm_tree(ddir);
    if (setup_tiered_db(db_dir, st_root, k, m, true) != 0)
        return -1;

    atomic_store(&settle_poll_ms, 60000);
    atomic_store(&eckeep_test_pause, true);

    tb_eng e;
    if (open_tiered_eng(db_dir, 2, 1, &e) != 0)
        return -1;
    uint64_t target_bytes = (uint64_t)cfg->digest_mb << 20;
    if (populate_to_bytes(e.db, target_bytes, cfg->quick ? 16 : 64, 0, NULL) != 0) {
        close_tiered_eng(&e);
        return -1;
    }

    part *ps[256];
    size_t np = collect_parts(e.db, 0, true, false, ps, 256);
    if (np == 0) {
        close_tiered_eng(&e);
        return -1;
    }

    /* Record settled_<part>.part names and total part bytes before settling. */
    char settled_names[256][280];
    uint64_t part_bytes = 0;
    for (size_t i = 0; i < np; i++) {
        snprintf(settled_names[i], sizeof settled_names[i], "settled_%s", ps[i]->name);
        part_bytes += ps[i]->map_len;
    }

    /* Settle each partition key with digest paused. */
    int64_t keys[256];
    size_t nkeys = 0;
    for (size_t i = 0; i < np; i++) {
        bool seen = false;
        for (size_t j = 0; j < nkeys; j++)
            if (keys[j] == ps[i]->partition) {
                seen = true;
                break;
            }
        if (!seen)
            keys[nkeys++] = ps[i]->partition;
    }
    for (size_t ki = 0; ki < nkeys; ki++) {
        part *kps[64];
        size_t kn = 0;
        for (size_t i = 0; i < np && kn < 64; i++)
            if (ps[i]->partition == keys[ki])
                kps[kn++] = ps[i];
        size_t got = 0;
        int rc = eng_settle(e.db, keys[ki], kps, kn, &got);
        if (rc != 0 || got != kn) {
            unref_parts(ps, np);
            close_tiered_eng(&e);
            return -1;
        }
    }
    unref_parts(ps, np);
    close_tiered_eng(&e);

    printf("\n== digest (eckeep_digest_mb_s throttling in MiB/s; 2x part bytes read: object + settled_ file) ==\n");
    printf("%10s %10s %9s %9s %13s %10s\n", "cap_MiB/s", "read_MiB", "wall_s", "cpu_s", "actual_MiB/s", "cpu_s/GiB");

    /* Run 0 (unlimited) and cfg->digest_rate (default 64 MiB/s). Each reopen re-queues all kept settled_ files. */
    unsigned caps[2] = {0, cfg->digest_rate};
    unsigned orig_rate = eckeep_digest_mb_s;
    for (int ci = 0; ci < 2; ci++) {
        unsigned cap = caps[ci];
        eckeep_digest_mb_s = cap;
        atomic_store(&eckeep_test_pause, true);

        drop_dir_cache(st_root, NULL);
        drop_dir_cache(db_dir, NULL);

        if (open_tiered_eng(db_dir, 0, 1, &e) != 0) {
            eckeep_digest_mb_s = orig_rate;
            return -1;
        }
        eccache_test_drop(e.t);

        /* keep_main starts on the first kick() inside eckeep_test_wait, so clearing pause before calling it avoids
         * any 50 ms paused poll wait. */
        atomic_store(&eckeep_test_pause, false);
        double w0 = now_s();
        double c0 = proc_cpu_s();

        for (size_t i = 0; i < np; i++) {
            int st = eckeep_test_wait(e.db, settled_names[i], 300000);
            if (st != ECKEEP_MATCHED) {
                fprintf(stderr, "tier-bench: digest of %s returned status %d (expected ECKEEP_MATCHED=%d)\n",
                        settled_names[i], st, ECKEEP_MATCHED);
                close_tiered_eng(&e);
                eckeep_digest_mb_s = orig_rate;
                return -1;
            }
        }
        double c1 = proc_cpu_s();
        double w1 = now_s();

        close_tiered_eng(&e);

        /* eckeep counts both the object read and the settled_ file read (2x part_bytes). */
        double read_bytes = 2.0 * (double)part_bytes;
        double read_mib = read_bytes / 1048576.0;
        double read_gib = read_bytes / 1073741824.0;
        double wall_s = w1 - w0;
        double cpu_s = c1 - c0;
        double actual_mib_s = wall_s > 0 ? read_mib / wall_s : 0.0;
        double cpu_gib = read_gib > 0 ? cpu_s / read_gib : 0.0;

        printf("%10u %10.2f %9.3f %9.3f %13.1f %10.2f\n", cap, read_mib, wall_s, cpu_s, actual_mib_s, cpu_gib);
        if (cap == 0) {
            printf("TIER_RESULT section=digest row=\"digest unlimited (cap=0 MiB/s)\" cap_mib_s=0 read_mib=%.3f "
                   "wall_s=%.4f cpu_s=%.4f actual_mib_s=%.2f cpu_s_gib=%.3f\n",
                   read_mib, wall_s, cpu_s, actual_mib_s, cpu_gib);
        } else {
            printf("TIER_RESULT section=digest row=\"digest throttled (cap=%u MiB/s)\" cap_mib_s=%u read_mib=%.3f "
                   "wall_s=%.4f cpu_s=%.4f actual_mib_s=%.2f cpu_s_gib=%.3f\n",
                   cap, cap, read_mib, wall_s, cpu_s, actual_mib_s, cpu_gib);
        }
        fflush(stdout);
    }

    eckeep_digest_mb_s = orig_rate;
    atomic_store(&eckeep_test_pause, false);
    if (!cfg->keep)
        rm_tree(ddir);
    return 0;
}

/* ---------------------------------------------------------------- 5. Cold decode queries */

typedef struct {
    eng *db;
    const uint32_t *sids;
    int s0, s1;
    int64_t t_start, t_end;
    uint64_t buckets;
    int rc;
} cold_qjob;

static void *cold_qworker(void *arg)
{
    cold_qjob *j = arg;
    bucketer bk = {.kind = BK_FIXED, .interval = TB_HOUR_MS};
    for (int s = j->s0; s < j->s1; s++) {
        tsdb_bucket *out = NULL;
        size_t nout = 0;
        int rc = eng_query_group(j->db, j->sids[s], j->t_start, j->t_end, &bk, false, 0, &out, &nout);
        free(out);
        if (rc != 0) {
            j->rc = rc;
            return NULL;
        }
        j->buckets += (uint64_t)nout;
    }
    return NULL;
}

static int run_cold_query_group(eng *db, const uint32_t *sids, int nseries, int64_t t_start, int64_t t_end, int nthr,
                                uint64_t *buckets_out)
{
    if (nthr < 1)
        nthr = 1;
    if (nthr > nseries)
        nthr = nseries;
    if (nthr == 1) {
        cold_qjob j = {.db = db, .sids = sids, .s0 = 0, .s1 = nseries, .t_start = t_start, .t_end = t_end};
        cold_qworker(&j);
        *buckets_out = j.buckets;
        return j.rc;
    }
    cold_qjob *jobs = calloc((size_t)nthr, sizeof *jobs);
    pthread_t *ths = calloc((size_t)nthr, sizeof *ths);
    if (!jobs || !ths) {
        free(jobs);
        free(ths);
        return TSDB_ERR_NOMEM;
    }
    int base = nseries / nthr, rem = nseries % nthr, cur = 0, started = 0;
    int rc = 0;
    for (int i = 0; i < nthr; i++) {
        int cnt = base + (i < rem ? 1 : 0);
        jobs[i] = (cold_qjob){.db = db, .sids = sids, .s0 = cur, .s1 = cur + cnt, .t_start = t_start, .t_end = t_end};
        cur += cnt;
        if (pthread_create(&ths[i], NULL, cold_qworker, &jobs[i]) != 0) {
            rc = TSDB_ERR_IO;
            break;
        }
        started++;
    }
    uint64_t tot = 0;
    for (int i = 0; i < started; i++) {
        pthread_join(ths[i], NULL);
        if (jobs[i].rc != 0 && rc == 0)
            rc = jobs[i].rc;
        tot += jobs[i].buckets;
    }
    free(jobs);
    free(ths);
    *buckets_out = tot;
    return rc;
}

#ifndef __APPLE__
static int apply_shim(const char *ctl, const char *root, bool enable, int slow_ms, double rate_mb)
{
    if (!ctl || !*ctl || !root || !*root)
        return 0;
    char cmd[PATH_MAX + 64], out[256];
    if (!enable) {
        snprintf(cmd, sizeof cmd, "heal %s", root);
        if (outagefs_cmd(ctl, cmd, out, sizeof out) != 0) {
            fprintf(stderr, "tier-bench: outagefs heal failed: %s\n", out);
            return -1;
        }
        return 0;
    }
    snprintf(cmd, sizeof cmd, "slow %s %d", root, slow_ms);
    if (outagefs_cmd(ctl, cmd, out, sizeof out) != 0) {
        fprintf(stderr, "tier-bench: outagefs slow failed: %s\n", out);
        return -1;
    }
    snprintf(cmd, sizeof cmd, "rate %s %.2f", root, rate_mb);
    if (outagefs_cmd(ctl, cmd, out, sizeof out) != 0) {
        fprintf(stderr, "tier-bench: outagefs rate failed: %s\n", out);
        return -1;
    }
    return 0;
}
#endif

static int run_cold_bench_here(const tb_cfg *cfg)
{
    const unsigned k = 4, m = 2;
    const bool is_hw = cfg->has_hdd;
#ifndef __APPLE__
    const char *ctl = is_hw ? NULL : getenv("OUTAGEFS_CTL");
    const char *shim_root = is_hw ? NULL : getenv("TIER_BENCH_COLD_ROOT");
#else
    const char *shim_root = NULL;
#endif

    char db_dir[PATH_MAX], st_root[PATH_MAX];
    if (snprintf(db_dir, sizeof db_dir, "%s/cold_db", cfg->dir) >= (int)sizeof db_dir)
        return -1;
    if (is_hw) {
        if (snprintf(st_root, sizeof st_root, "%s/tier-bench-cold-%d", cfg->hdd, (int)getpid()) >= (int)sizeof st_root)
            return -1;
    } else if (shim_root && *shim_root) {
        if (snprintf(st_root, sizeof st_root, "%s", shim_root) >= (int)sizeof st_root)
            return -1;
    } else {
        if (snprintf(st_root, sizeof st_root, "%s/cold_stores", cfg->dir) >= (int)sizeof st_root)
            return -1;
    }

    rm_tree(db_dir);
    rm_tree(st_root);
    if (setup_tiered_db(db_dir, st_root, k, m, true) != 0)
        return -1;

    atomic_store(&settle_poll_ms, 60000);
    atomic_store(&eckeep_test_pause, true);

    tb_eng e;
    if (open_tiered_eng(db_dir, 2, 1, &e) != 0) {
        if (is_hw && !cfg->keep)
            rm_tree(st_root);
        return -1;
    }

    /*
     * Data shape (§15.7 "What reads cost"):
     *   - 3 s sample interval -> 1,200 rows/hour (< HOUR_CUT_MIN = 3,000, so granules are NOT cut on hour edges and
     *     1-hour bucket queries must decode granules from the tier stores rather than answering from .ec statistics).
     *   - 28,800 rows/day/series -> 4 granules per series per day partition (3 x 8192 + 4224), which exercises
     *     sequential granule prefetch (PF_INIT = 4) cleanly within every day partition.
     */
    int nseries = cfg->cold_series;
    int ndays = cfg->cold_days;
    const int64_t step_ms = 3000;
    const int64_t pts_per_day = TB_DAY_MS / step_ms - 1; /* 28,799; starts +1s into day so -jitter stays in day */

    uint32_t *sids = calloc((size_t)nseries, sizeof *sids);
    ds_state *st = calloc((size_t)nseries, sizeof *st);
    const size_t batch = 65536;
    uint32_t *sv = malloc(batch * sizeof *sv);
    int64_t *tv = malloc(batch * sizeof *tv);
    float *vv = malloc(batch * sizeof *vv);
    if (!sids || !st || !sv || !tv || !vv) {
        free(sids);
        free(st);
        free(sv);
        free(tv);
        free(vv);
        close_tiered_eng(&e);
        if (is_hw && !cfg->keep)
            rm_tree(st_root);
        return -1;
    }

    for (int s = 0; s < nseries; s++) {
        char name[64];
        snprintf(name, sizeof name, "cold.%04d", s);
        if (eng_series(e.db, name, TSDB_FLOAT32, &sids[s]) != 0)
            goto fail;
        ds_init(&st[s], (ds_kind)(s % (DS_KINDS - 1)), (uint32_t)s, TB_T0 + 1000LL);
        st[s].stamp_ms = step_ms;
    }

    /* Load day-by-day to bound memtable memory, optimizing and settling each day into a 4+2 .ec object. */
    for (int d = 0; d < ndays; d++) {
        int64_t day_t0 = TB_T0 + (int64_t)d * TB_DAY_MS + 1000LL;
        for (int s = 0; s < nseries; s++) {
            st[s].t0 = day_t0;
            st[s].i = 0;
        }
        for (int64_t i0 = 0; i0 < pts_per_day;) {
            size_t n = 0;
            int64_t step_pts = (int64_t)(batch / (size_t)nseries);
            if (step_pts < 1)
                step_pts = 1;
            int64_t i1 = i0 + step_pts;
            if (i1 > pts_per_day)
                i1 = pts_per_day;
            for (int64_t i = i0; i < i1; i++) {
                for (int s = 0; s < nseries; s++) {
                    sv[n] = sids[s];
                    ds_next(&st[s], &tv[n], &vv[n]);
                    n++;
                }
            }
            if (eng_insert(e.db, TSDB_FLOAT32, sv, tv, vv, n) != 0)
                goto fail;
            i0 = i1;
        }
        if (eng_flush(e.db) != 0)
            goto fail;
        int64_t dk = TB_KEY0 + d;
        if (eng_optimize_keys(e.db, &dk, 1) != 0)
            goto fail;
        part *yp[64];
        size_t nyp = collect_parts(e.db, dk, false, false, yp, 64);
        size_t got = 0;
        int rc = nyp ? eng_settle(e.db, dk, yp, nyp, &got) : TSDB_ERR_NOTFOUND;
        unref_parts(yp, nyp);
        if (rc != 0 || got != nyp) {
            fprintf(stderr, "tier-bench: cold settle day %d failed (rc=%d, got=%zu/%zu)\n", d, rc, got, nyp);
            goto fail;
        }
    }

    free(st);
    free(sv);
    free(tv);
    free(vv);
    st = NULL;
    sv = NULL;
    tv = NULL;
    vv = NULL;

    char storage_hdr[PATH_MAX + 96];
    const char *storage_tag = is_hw ? "hardware hdd, 6 stores on one device" : "shim, not hardware";
    if (is_hw)
        snprintf(storage_hdr, sizeof storage_hdr, "hardware hdd (6 stores on one device: %s)", cfg->hdd);
    else
        snprintf(storage_hdr, sizeof storage_hdr, "shim, not hardware (outagefs slow=%dms rate=%.0fMB/s)",
                 cfg->shim_slow_ms, cfg->shim_rate_mb);

    printf("\n== cold decode queries (%u+%u, %dd hourly, %s) ==\n", k, m, ndays, storage_hdr);
    printf("%-68s %10s %10s %8s %8s %8s %8s %10s\n", "query", "wall_ms", "cpu_ms", "reads", "misses", "pf_used",
           "buckets", "res_pages");

    int64_t t_start = TB_T0;
    int64_t t_end = TB_T0 + (int64_t)ndays * TB_DAY_MS;
    const struct {
        int nser;
        bool prefetch;
    } cases[4] = {
        {1, false},
        {1, true},
        {nseries, false},
        {nseries, true},
    };

    for (int ci = 0; ci < 4; ci++) {
        int qser = cases[ci].nser;
        bool pf = cases[ci].prefetch;

        /* Evict tier granule cache and OS page cache cleanly before every query (D31). */
#ifndef __APPLE__
        if (!is_hw && apply_shim(ctl, st_root, false, 0, 0) != 0)
            goto fail;
#endif
        eccache_test_drop(e.t);
        uint64_t res_pages = 0;
        drop_dir_cache(st_root, &res_pages);
        if (res_pages > 0)
            fprintf(stderr, "tier-bench: warning: %" PRIu64 " chunk pages stayed resident after DONTNEED (D31)\n",
                    res_pages);

        eccache_set_prefetch(e.t, pf);

#ifndef __APPLE__
        if (!is_hw && apply_shim(ctl, st_root, true, cfg->shim_slow_ms, cfg->shim_rate_mb) != 0)
            goto fail;
#endif

        eccache_counters c0, c1;
        eccache_get_counters(e.t, &c0);
        uint64_t buckets = 0;
        double w0 = now_s();
        double cp0 = proc_cpu_s();
        int qrc = run_cold_query_group(e.db, sids, qser, t_start, t_end, qser == 1 ? 1 : cfg->query_threads, &buckets);
        double cp1 = proc_cpu_s();
        double w1 = now_s();
        eccache_get_counters(e.t, &c1);

#ifndef __APPLE__
        if (!is_hw)
            (void)apply_shim(ctl, st_root, false, 0, 0);
#endif
        if (qrc != 0) {
            fprintf(stderr, "tier-bench: cold query failed (rc=%d)\n", qrc);
            goto fail;
        }
        uint64_t reads = c1.reads - c0.reads;
        uint64_t misses = c1.misses - c0.misses;
        uint64_t pf_used = c1.pf_used - c0.pf_used;
        uint64_t expect_buckets = (uint64_t)qser * (uint64_t)ndays * 24u;
        if (buckets != expect_buckets || reads == 0) {
            fprintf(stderr, "tier-bench: cold query mismatch (buckets=%" PRIu64 "/%" PRIu64 ", reads=%" PRIu64 ")\n",
                    buckets, expect_buckets, reads);
            goto fail;
        }

        double wall_ms = (w1 - w0) * 1e3;
        double cpu_ms = (cp1 - cp0) * 1e3;
        char row[192];
        snprintf(row, sizeof row, "%d series, %dd hourly, prefetch %s (%s)", qser, ndays, pf ? "on" : "off",
                 storage_tag);
        printf("%-68s %10.2f %10.2f %8" PRIu64 " %8" PRIu64 " %8" PRIu64 " %8" PRIu64 " %10" PRIu64 "\n", row, wall_ms,
               cpu_ms, reads, misses, pf_used, buckets, res_pages);
        printf("TIER_RESULT section=cold storage=\"%s\" row=\"%s\" wall_ms=%.3f cpu_ms=%.3f reads=%" PRIu64
               " misses=%" PRIu64 " pf_used=%" PRIu64 " buckets=%" PRIu64 " resident_pages=%" PRIu64 "\n",
               storage_tag, row, wall_ms, cpu_ms, reads, misses, pf_used, buckets, res_pages);
        fflush(stdout);
    }

    free(sids);
    close_tiered_eng(&e);
    if (!cfg->keep) {
        rm_tree(db_dir);
        rm_tree(st_root);
    }
    return 0;

fail:
    free(sids);
    free(st);
    free(sv);
    free(tv);
    free(vv);
    close_tiered_eng(&e);
    if (!cfg->keep) {
        rm_tree(db_dir);
        rm_tree(st_root);
    }
    return -1;
}

#ifndef __APPLE__
static int find_outagefs(const tb_cfg *cfg, char out[PATH_MAX])
{
    const char *cands[4];
    int nc = 0;
    if (cfg->outagefs[0])
        cands[nc++] = cfg->outagefs;
    const char *env = getenv("OUTAGEFS_LIB");
    if (env && *env)
        cands[nc++] = env;

    char self_dir[PATH_MAX], beside[PATH_MAX];
    ssize_t rl = readlink("/proc/self/exe", self_dir, sizeof self_dir - 1);
    if (rl > 0) {
        self_dir[rl] = '\0';
        char *slash = strrchr(self_dir, '/');
        if (slash) {
            *slash = '\0';
            if (snprintf(beside, sizeof beside, "%s/liboutagefs.so", self_dir) < (int)sizeof beside)
                cands[nc++] = beside;
        }
    }
    cands[nc++] = "build/liboutagefs.so";

    for (int i = 0; i < nc; i++) {
        if (access(cands[i], R_OK) == 0 && realpath(cands[i], out))
            return 0;
    }
    return -1;
}

static int run_cold_bench_shimmed(const tb_cfg *cfg, int argc, char **argv)
{
    char lib[PATH_MAX];
    if (find_outagefs(cfg, lib) != 0) {
        fprintf(stderr,
                "tier-bench: liboutagefs.so not found (build it with `make build/liboutagefs.so`, pass --outagefs "
                "PATH, or pass --hdd PATH for a real rotational drive)\n");
        return -1;
    }

    char cold_root[PATH_MAX], real_cold[PATH_MAX], ctl[PATH_MAX];
    if (snprintf(cold_root, sizeof cold_root, "%s/cold_stores", cfg->dir) >= (int)sizeof cold_root ||
        mkdir_p(cold_root) != 0 || !realpath(cold_root, real_cold) ||
        snprintf(ctl, sizeof ctl, "%s/outagefs.ctl", cfg->dir) >= (int)sizeof ctl) {
        fprintf(stderr, "tier-bench: cannot prepare outagefs paths under %s\n", cfg->dir);
        return -1;
    }

    /* Build child argv: original argv + --internal-cold-child --dir <cfg->dir> --only cold */
    char **cargv = calloc((size_t)argc + 8, sizeof *cargv);
    if (!cargv)
        return -1;
    int n = 0;
    for (int i = 0; i < argc; i++)
        cargv[n++] = argv[i];
    cargv[n++] = "--internal-cold-child";
    cargv[n++] = "--dir";
    cargv[n++] = (char *)cfg->dir;
    cargv[n++] = "--only";
    cargv[n++] = "cold";
    cargv[n] = NULL;

    fflush(stdout);
    fflush(stderr);
    pid_t pid = fork();
    if (pid < 0) {
        free(cargv);
        return -1;
    }
    if (pid == 0) {
        setenv("OUTAGEFS_ROOTS", real_cold, 1);
        setenv("OUTAGEFS_CTL", ctl, 1);
        setenv("TIER_BENCH_COLD_ROOT", real_cold, 1);
        const char *prev = getenv("LD_PRELOAD");
        if (prev && *prev) {
            char preload[PATH_MAX * 2];
            snprintf(preload, sizeof preload, "%s:%s", lib, prev);
            setenv("LD_PRELOAD", preload, 1);
        } else {
            setenv("LD_PRELOAD", lib, 1);
        }
        char asan[512];
        const char *aold = getenv("ASAN_OPTIONS");
        snprintf(asan, sizeof asan, "verify_asan_link_order=0%s%.400s", aold && *aold ? ":" : "", aold ? aold : "");
        setenv("ASAN_OPTIONS", asan, 1);
        execv("/proc/self/exe", cargv);
        perror("tier-bench: execv cold child");
        _exit(127);
    }
    free(cargv);
    int st = 0;
    while (waitpid(pid, &st, 0) < 0 && errno == EINTR)
        ;
    unlink(ctl);
    if (!WIFEXITED(st) || WEXITSTATUS(st) != 0)
        return -1;
    return 0;
}
#endif

/* ---------------------------------------------------------------- main */

static int parse_only(const char *s, unsigned *mask)
{
    unsigned m = 0;
    char buf[256];
    if (snprintf(buf, sizeof buf, "%s", s) >= (int)sizeof buf)
        return -1;
    char *sp = NULL;
    for (char *tok = strtok_r(buf, ",", &sp); tok; tok = strtok_r(NULL, ",", &sp)) {
        if (!strcmp(tok, "settle"))
            m |= SEC_SETTLE;
        else if (!strcmp(tok, "reencode"))
            m |= SEC_REENCODE;
        else if (!strcmp(tok, "sweep"))
            m |= SEC_SWEEP;
        else if (!strcmp(tok, "digest"))
            m |= SEC_DIGEST;
        else if (!strcmp(tok, "cold"))
            m |= SEC_COLD;
        else
            return -1;
    }
    if (m == 0)
        return -1;
    *mask = m;
    return 0;
}

static int parse_geoms(const char *s, tb_geom *out, int *nout)
{
    int ng = 0;
    for (const char *p = s; *p && ng < MAX_GEOMS;) {
        unsigned k = 0, m = 0;
        int used = 0;
        if (sscanf(p, "%u+%u%n", &k, &m, &used) != 2 || k < 1 || k > CHUNK_K_MAX || m > CHUNK_M_MAX ||
            (m == 0 && k != 1))
            return -1;
        out[ng++] = (tb_geom){k, m};
        p += used;
        if (*p == ',')
            p++;
        else if (*p != '\0')
            return -1;
    }
    if (ng == 0)
        return -1;
    *nout = ng;
    return 0;
}

int main(int argc, char **argv)
{
    tb_cfg cfg;
    memset(&cfg, 0, sizeof cfg);
    cfg.want_mask = SEC_ALL;
    cfg.geoms[0] = (tb_geom){1, 1};
    cfg.geoms[1] = (tb_geom){2, 1};
    cfg.geoms[2] = (tb_geom){4, 2};
    cfg.geoms[3] = (tb_geom){1, 0};
    cfg.ngeoms = 4;
    cfg.digest_rate = 64;
    cfg.shim_slow_ms = 4;
    cfg.shim_rate_mb = 150.0;
    long ncpu = sysconf(_SC_NPROCESSORS_ONLN);
    cfg.query_threads = ncpu > 8 ? 8 : (ncpu > 1 ? (int)ncpu : 1);

    bool set_settle = false, set_reenc = false, set_digest = false, set_days = false, set_series = false;

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--dir") && i + 1 < argc) {
            snprintf(cfg.dir, sizeof cfg.dir, "%s", argv[++i]);
            cfg.has_dir = true;
        } else if (!strcmp(argv[i], "--hdd") && i + 1 < argc) {
            snprintf(cfg.hdd, sizeof cfg.hdd, "%s", argv[++i]);
            cfg.has_hdd = true;
        } else if (!strcmp(argv[i], "--outagefs") && i + 1 < argc) {
            snprintf(cfg.outagefs, sizeof cfg.outagefs, "%s", argv[++i]);
        } else if (!strcmp(argv[i], "--sysroot") && i + 1 < argc) {
            snprintf(cfg.sysroot, sizeof cfg.sysroot, "%s", argv[++i]);
        } else if (!strcmp(argv[i], "--quick")) {
            cfg.quick = true;
        } else if (!strcmp(argv[i], "--keep")) {
            cfg.keep = true;
        } else if (!strcmp(argv[i], "--internal-cold-child")) {
            cfg.cold_child = true;
        } else if (!strcmp(argv[i], "--only") && i + 1 < argc) {
            if (parse_only(argv[++i], &cfg.want_mask) != 0)
                usage();
        } else if (!strcmp(argv[i], "--geoms") && i + 1 < argc) {
            if (parse_geoms(argv[++i], cfg.geoms, &cfg.ngeoms) != 0)
                usage();
        } else if (!strcmp(argv[i], "--settle-mb") && i + 1 < argc) {
            cfg.settle_mb = (size_t)strtoull(argv[++i], NULL, 10);
            set_settle = true;
        } else if (!strcmp(argv[i], "--reencode-mb") && i + 1 < argc) {
            cfg.reencode_mb = (size_t)strtoull(argv[++i], NULL, 10);
            set_reenc = true;
        } else if (!strcmp(argv[i], "--digest-mb") && i + 1 < argc) {
            cfg.digest_mb = (size_t)strtoull(argv[++i], NULL, 10);
            set_digest = true;
        } else if (!strcmp(argv[i], "--digest-rate") && i + 1 < argc) {
            cfg.digest_rate = (unsigned)strtoul(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--days") && i + 1 < argc) {
            cfg.cold_days = atoi(argv[++i]);
            set_days = true;
        } else if (!strcmp(argv[i], "--series") && i + 1 < argc) {
            cfg.cold_series = atoi(argv[++i]);
            set_series = true;
        } else if (!strcmp(argv[i], "--shim-slow-ms") && i + 1 < argc) {
            cfg.shim_slow_ms = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "--shim-rate-mb") && i + 1 < argc) {
            cfg.shim_rate_mb = atof(argv[++i]);
        } else if (!strcmp(argv[i], "-h") || !strcmp(argv[i], "--help")) {
            usage();
        } else {
            fprintf(stderr, "tier-bench: unknown option %s\n", argv[i]);
            usage();
        }
    }

    if (!set_settle)
        cfg.settle_mb = cfg.quick ? 8 : 128;
    if (!set_reenc)
        cfg.reencode_mb = cfg.quick ? 8 : 128;
    cfg.sweep_n[0] = cfg.quick ? 1000 : 10000;
    cfg.sweep_n[1] = cfg.quick ? 5000 : 100000;
    if (!set_digest)
        cfg.digest_mb = cfg.quick ? 8 : 64;
    if (!set_days)
        cfg.cold_days = cfg.quick ? 12 : 116;
    if (!set_series)
        cfg.cold_series = cfg.quick ? 10 : 100;

    if (cfg.settle_mb < 1 || cfg.reencode_mb < 1 || cfg.digest_mb < 1 || cfg.digest_rate < 1 || cfg.cold_days < 1 ||
        cfg.cold_series < 1 || cfg.shim_slow_ms < 0 || cfg.shim_rate_mb <= 0.0) {
        fprintf(stderr, "tier-bench: invalid numeric argument\n");
        return 2;
    }

    /* Validate --hdd BEFORE doing any work (D30). */
    if (cfg.has_hdd && verify_hdd_path(cfg.hdd, cfg.sysroot) != 0)
        return 2;

    bool created_root = false;
    if (!cfg.has_dir) {
        const char *tmp = getenv("TMPDIR");
        if (!tmp || !*tmp)
            tmp = "/var/tmp";
        snprintf(cfg.dir, sizeof cfg.dir, "%s/tier-bench-%d", tmp, (int)getpid());
        created_root = true;
    }
    if (mkdir_p(cfg.dir) != 0) {
        fprintf(stderr, "tier-bench: cannot create scratch dir %s: %s\n", cfg.dir, strerror(errno));
        return 1;
    }
    char real_dir[PATH_MAX];
    if (realpath(cfg.dir, real_dir))
        snprintf(cfg.dir, sizeof cfg.dir, "%s", real_dir);

    /* Ensure TSDB_TEST_TIER cannot interfere with TIERS. */
    unsetenv("TSDB_TEST_TIER");
    unsetenv("TSDB_TEST_TIER_I_KNOW");

    if (cfg.cold_child) {
        int rc = run_cold_bench_here(&cfg);
        return rc == 0 ? 0 : 1;
    }

    if (cfg.has_hdd) {
        printf("tier-bench: mode=%s cold_storage=hardware (hdd: %s)\n", cfg.quick ? "quick" : "standard", cfg.hdd);
    } else {
        printf("tier-bench: mode=%s cold_storage=shim, not hardware (outagefs slow=%dms rate=%.0fMB/s)\n",
               cfg.quick ? "quick" : "standard", cfg.shim_slow_ms, cfg.shim_rate_mb);
    }
    fflush(stdout);

    int rc = 0;
    if (rc == 0 && (cfg.want_mask & SEC_SETTLE))
        rc = run_settle_bench(&cfg);
    if (rc == 0 && (cfg.want_mask & SEC_REENCODE))
        rc = run_reencode_bench(&cfg);
    if (rc == 0 && (cfg.want_mask & SEC_SWEEP))
        rc = run_sweep_bench(&cfg);
    if (rc == 0 && (cfg.want_mask & SEC_DIGEST))
        rc = run_digest_bench(&cfg);
    if (rc == 0 && (cfg.want_mask & SEC_COLD)) {
        if (cfg.has_hdd) {
            rc = run_cold_bench_here(&cfg);
        } else {
#ifdef __APPLE__
            printf("\n== cold decode queries: skipped on macOS (no outagefs shim; pass --hdd on Linux) ==\n");
#else
            rc = run_cold_bench_shimmed(&cfg, argc, argv);
#endif
        }
    }

    if (!cfg.keep) {
        if (created_root) {
            rm_tree(cfg.dir);
        } else {
            char sub[PATH_MAX];
            const char *subs[] = {"cold_db", "cold_stores", "reencode", "digest"};
            for (size_t i = 0; i < sizeof subs / sizeof *subs; i++) {
                if (snprintf(sub, sizeof sub, "%s/%s", cfg.dir, subs[i]) < (int)sizeof sub)
                    rm_tree(sub);
            }
            for (int gi = 0; gi < cfg.ngeoms; gi++) {
                if (snprintf(sub, sizeof sub, "%s/settle_%u_%u", cfg.dir, cfg.geoms[gi].k, cfg.geoms[gi].m) <
                    (int)sizeof sub)
                    rm_tree(sub);
            }
            for (int si = 0; si < 2; si++) {
                if (snprintf(sub, sizeof sub, "%s/sweep_%zu", cfg.dir, cfg.sweep_n[si]) < (int)sizeof sub)
                    rm_tree(sub);
            }
        }
    }
    return rc == 0 ? 0 : 1;
}

#endif /* TSDB_EC */
