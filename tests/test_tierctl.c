/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Tests for r15-lost: tier worker, TIERS re-read, tsdb_tier_lost --replace, paced chunk_be_replace rebuild,
 * kill mid-rebuild, power loss (crashfs on new store) at multiple sync points, E15 >m LOST (TSDB_ERR_IO
 * naming partition, statistics still answer) vs DOWN (TSDB_ERR_UNAVAILABLE), and mutants `lostnoreplace`
 * and `rebuildfast` (docs/design-cluster-data.md §15.7, E15, E55; R15 D9, D10, D20).
 *
 *   ./build/test_tierctl [--mutant lostnoreplace|rebuildfast] [scratch dir]
 */
#define _GNU_SOURCE
#include "chunk.h"
#include "chunk_priv.h"
#include "eccache.h"
#include "ecgc.h"
#include "internal.h"
#include "settle.h"
#include "tier.h"
#include "tierctl.h"
#include "tiers.h"
#include "tsdb.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <math.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static int fails;
static int saved_err = -1;
static int cap_rd = -1;
static char S[PATH_MAX], SELF[PATH_MAX], SHIM[PATH_MAX + 32], CLI[PATH_MAX + 32];
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

static void safe_kill(pid_t pid, int sig)
{
    if (pid > 0)
        (void)kill(pid, sig);
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

/* Creates a database with 6 tier stores (4+2) and returns it open with the tier active. */
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

static double det_val(int p, int r, int series)
{
    uint64_t x = ((uint64_t)(uint32_t)(p + 1) << 40) ^ ((uint64_t)(uint32_t)(r + 1) << 8) ^ (uint64_t)(series + 17);
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

static int query_f64(tsdb *db, tsdb_sid s, int64_t start, int64_t end, int64_t *ts, double *vals, size_t cap,
                     size_t *nout)
{
    f64_collect_ctx ctx = {.ts = ts, .vals = vals, .cap = cap, .n = 0, .overflow = false};
    int rc = tsdb_query_values(db, s, start, end, NULL, f64_collect_cb, &ctx);
    *nout = ctx.n;
    if (rc == 1 && ctx.overflow)
        return 0;
    return rc;
}

/* Inserts nparts days of deterministic rows into series s0 and s1, flushes, and settles every part onto the tier. */
static void populate_and_settle(tsdb *db, int nparts, int rows_per_part, tsdb_sid *s0_out, tsdb_sid *s1_out)
{
    const int64_t day_ms = 86400000LL;
    tsdb_sid s0 = 0, s1 = 0;
    CHECK(tsdb_series_create(db, "metric_a", TSDB_FLOAT64, NULL, &s0) == 0);
    CHECK(tsdb_series_create(db, "metric_b", TSDB_FLOAT64, NULL, &s1) == 0);
    int64_t *ts = malloc((size_t)rows_per_part * sizeof *ts);
    double *va = malloc((size_t)rows_per_part * sizeof *va);
    double *vb = malloc((size_t)rows_per_part * sizeof *vb);
    CHECK(ts && va && vb);
    for (int p = 0; p < nparts; p++) {
        int64_t base_ts = (int64_t)p * day_ms + 1000LL;
        for (int r = 0; r < rows_per_part; r++) {
            ts[r] = base_ts + (int64_t)r * 1000LL;
            va[r] = det_val(p, r, 0);
            vb[r] = det_val(p, r, 1);
        }
        tsdb_values val_a = {.type = TSDB_FLOAT64, .values = va, .valid = NULL};
        tsdb_values val_b = {.type = TSDB_FLOAT64, .values = vb, .valid = NULL};
        CHECK(tsdb_insert_series_values(db, s0, ts, &val_a, (size_t)rows_per_part) == 0);
        CHECK(tsdb_insert_series_values(db, s1, ts, &val_b, (size_t)rows_per_part) == 0);
        CHECK(tsdb_flush(db) == 0);
    }
    free(ts);
    free(va);
    free(vb);
    size_t settled = 0;
    CHECK(tsdb_test_settle(db, pick_all, NULL, &settled) == 0);
    CHECK(settled >= (size_t)nparts);
    *s0_out = s0;
    *s1_out = s1;
}

/* Verifies every row across all partitions matches populate_and_settle's values bit-for-bit. */
static void verify_rows(tsdb *db, tsdb_sid s0, tsdb_sid s1, int nparts, int rows_per_part)
{
    const int64_t day_ms = 86400000LL;
    size_t total = (size_t)nparts * (size_t)rows_per_part;
    int64_t *ts = malloc(total * sizeof *ts);
    double *va = malloc(total * sizeof *va);
    double *vb = malloc(total * sizeof *vb);
    CHECK(ts && va && vb);
    size_t na = 0, nb = 0;
    int rca = query_f64(db, s0, 0, (int64_t)nparts * day_ms, ts, va, total, &na);
    CHECK(rca == 0 && na == total);
    if (rca == 0 && na == total) {
        size_t idx = 0;
        for (int p = 0; p < nparts; p++) {
            int64_t base_ts = (int64_t)p * day_ms + 1000LL;
            for (int r = 0; r < rows_per_part; r++, idx++) {
                double want = det_val(p, r, 0);
                if (ts[idx] != base_ts + (int64_t)r * 1000LL || va[idx] != want) {
                    CHECK(false);
                    goto done;
                }
            }
        }
    }
    int rcb = query_f64(db, s1, 0, (int64_t)nparts * day_ms, ts, vb, total, &nb);
    CHECK(rcb == 0 && nb == total);
    if (rcb == 0 && nb == total) {
        size_t idx = 0;
        for (int p = 0; p < nparts; p++) {
            int64_t base_ts = (int64_t)p * day_ms + 1000LL;
            for (int r = 0; r < rows_per_part; r++, idx++) {
                double want = det_val(p, r, 1);
                if (ts[idx] != base_ts + (int64_t)r * 1000LL || vb[idx] != want) {
                    CHECK(false);
                    goto done;
                }
            }
        }
    }
done:
    free(ts);
    free(va);
    free(vb);
}

/* Counts valid .chunk files in <store_path>/s0 and verifies each one parses and scrubs cleanly with 0 bad cells. */
static int count_and_scrub_store_chunks(const char *store_path, uint64_t *bytes_out, bool open_store_direct)
{
    if (bytes_out)
        *bytes_out = 0;
    char s0dir[PATH_MAX + 16];
    snprintf(s0dir, sizeof s0dir, "%s/s0", store_path);
    DIR *d = opendir(s0dir);
    if (!d)
        return 0;
    int count = 0;
    struct dirent *e;
    chunk_store *cs = NULL;
    if (open_store_direct) {
        if (chunk_store_open(store_path, NULL, &cs) != 0) {
            closedir(d);
            return -1;
        }
    }
    while ((e = readdir(d)) != NULL) {
        size_t nl = strlen(e->d_name);
        if (nl <= 6 || strcmp(e->d_name + nl - 6, ".chunk") != 0)
            continue;
        struct stat st;
        if (fstatat(dirfd(d), e->d_name, &st, AT_SYMLINK_NOFOLLOW) == 0 && S_ISREG(st.st_mode)) {
            if (bytes_out && st.st_size > 0)
                *bytes_out += (uint64_t)st.st_size;
            count++;
        }
        if (cs) {
            chunk_hdr h;
            uint8_t oid[16];
            unsigned pos = 0;
            bool hex_ok = e->d_name[0] == 'o' && nl > 34 && e->d_name[33] == '.';
            for (int k = 0; hex_ok && k < 16; k++) {
                int a = e->d_name[1 + 2 * k];
                int b = e->d_name[2 + 2 * k];
                int va = a >= '0' && a <= '9' ? a - '0' : a >= 'a' && a <= 'f' ? a - 'a' + 10 : -1;
                int vb = b >= '0' && b <= '9' ? b - '0' : b >= 'a' && b <= 'f' ? b - 'a' + 10 : -1;
                if (va < 0 || vb < 0)
                    hex_ok = false;
                else
                    oid[k] = (uint8_t)((va << 4) | vb);
            }
            if (hex_ok) {
                pos = (unsigned)strtoul(e->d_name + 34, NULL, 10);
                if (chunk_store_peek(cs, 0, oid, pos, &h) != 0) {
                    count = -1;
                    break;
                }
                chunk_ident id = {.volume = 0, .pos = (uint16_t)pos};
                memcpy(id.vuuid, h.vuuid, 16);
                memcpy(id.oid, oid, 16);
                chunk_rd *rd = NULL;
                uint64_t bad = 0;
                if (chunk_rd_open(cs, &id, &rd) != 0 || chunk_rd_scrub(rd, &bad) != 0 || bad != 0) {
                    chunk_rd_close(rd);
                    count = -1;
                    break;
                }
                chunk_rd_close(rd);
            }
        }
    }
    closedir(d);
    if (cs)
        chunk_store_close(cs);
    return count;
}

static bool wait_store_state(tier *t, int idx, int want, int timeout_ms)
{
    int64_t dl = mono_ms() + timeout_ms;
    while (mono_ms() < dl) {
        if (tier_store_state(t, idx) == want)
            return true;
        struct timespec nap = {0, 20 * 1000000L};
        nanosleep(&nap, NULL);
    }
    return tier_store_state(t, idx) == want;
}

/* ------------------------------------------------------------ 1. Lose 1 of 6 (4+2) and replace */

static void t_replace_4plus2(void)
{
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX], s2_new[PATH_MAX + 32];
    fresh_dir("replace42", base, sizeof base);
    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db);

    tsdb_sid s0 = 0, s1 = 0;
    populate_and_settle(db, 4, 20000, &s0, &s1);
    verify_rows(db, s0, s1, 4, 20000);

    /* Confirm store 2 actually holds chunks before we lose it. */
    uint64_t orig_bytes = 0;
    int orig_chunks = count_and_scrub_store_chunks(stores[2], &orig_bytes, false);
    CHECK(orig_chunks >= 4 && orig_bytes > 0);

    /* Refusals: --shrink below 4+2, unknown store, relative path, non-empty directory. */
    char log[2048];
    cap_begin();
    CHECK(tsdb_tier_lost(db, "s2", NULL, 0) == TSDB_ERR_INVALID);
    cap_end(log, sizeof log);
    CHECK(strstr(log, "fewer than") != NULL && strstr(log, "4+2") != NULL);

    CHECK(tsdb_tier_lost(db, "s99", "/tmp/x_unused", TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_NOTFOUND);
    CHECK(tsdb_tier_lost(db, "s2", "rel/path", TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_INVALID);
    char nonempty[PATH_MAX + 32];
    snprintf(nonempty, sizeof nonempty, "%s/nonempty", base);
    CHECK(sh("mkdir -p '%s' && touch '%s/junk'", nonempty, nonempty) == 0);
    CHECK(tsdb_tier_lost(db, "s2", nonempty, TSDB_TIER_ALLOW_SHARED_DEV) == TSDB_ERR_INVALID);

    /* Permanently lose store 2 and replace it onto s2_new. */
    CHECK(sh("rm -rf '%s'", stores[2]) == 0);
    snprintf(s2_new, sizeof s2_new, "%s/st2_new", base);
    CHECK(tsdb_tier_lost(db, "s2", s2_new, TSDB_TIER_ALLOW_SHARED_DEV) == 0);

    tier *t = tier_find_by_dir(dbdir);
    CHECK(t != NULL);
    CHECK(tierctl_wait_idle(t, 30000) == 0);
    CHECK(wait_store_state(t, 2, CHUNK_STORE_UP, 10000));
    CHECK(!tierctl_store_rebuilding(s2_new));

    tierctl_progress prog;
    CHECK(tierctl_get_progress(t, &prog));
    CHECK(!prog.active && prog.rebuilds_done >= 1);

    /* Every position on s2_new must actually be rebuilt (kills `lostnoreplace`). */
    uint64_t new_bytes = 0;
    int new_chunks = count_and_scrub_store_chunks(s2_new, &new_bytes, false);
    CHECK(new_chunks == orig_chunks && new_bytes == orig_bytes);

    /* Drop granule cache and take 2 OTHER stores (0 and 1) DOWN so reads MUST rely on s2_new's rebuilt chunks! */
    (void)tsdb_test_ec_retries(db, true);
    tier_store_set_down(t, 0, CHUNK_STORE_DOWN_STALLED);
    tier_store_set_down(t, 1, CHUNK_STORE_DOWN_STALLED);
    eccache_reopen_all(t);
    verify_rows(db, s0, s1, 4, 20000);

    tier_put(t);
    CHECK(tsdb_close(db) == 0);

    /* Reopen offline and scrub s2_new directly on disk to confirm every chunk is intact. */
    CHECK(count_and_scrub_store_chunks(s2_new, NULL, true) == orig_chunks);
    printf("  replace_4plus2: ok (%d chunks, %" PRIu64 " B rebuilt, verified with 2 other stores down)\n",
           new_chunks, new_bytes);
}

/* ------------------------------------------------------------ 2. Kill -9 mid-rebuild resumes cleanly */

typedef struct kill_hook_ctx {
    const char *ready_file;
    bool wrote;
} kill_hook_ctx;

static void kill_progress_hook(void *vctx, int store, uint64_t bytes_done)
{
    kill_hook_ctx *c = vctx;
    (void)store;
    if (!c->wrote && bytes_done > 0) {
        c->wrote = true;
        int fd = open(c->ready_file, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd >= 0) {
            ssize_t wr = write(fd, "1\n", 2);
            (void)wr;
            close(fd);
        }
        /* Pause inside the child so parent's SIGKILL catches it deterministically mid-rebuild! */
        for (;;)
            pause();
    }
}

static int child_kill_rebuild(const char *dbdir, const char *ready_file)
{
    kill_hook_ctx hctx = {.ready_file = ready_file, .wrote = false};
    tierctl_set_rebuild_hook(kill_progress_hook, &hctx);
    tsdb *db = NULL;
    if (open_db(dbdir, &db) != 0 || !db)
        return 2;
    /* Wait for worker to trigger hook (which pauses until SIGKILL) or finish. */
    for (int i = 0; i < 200; i++) {
        struct timespec nap = {0, 50 * 1000000L};
        nanosleep(&nap, NULL);
    }
    tsdb_close(db);
    return 1; /* should have been killed mid-rebuild */
}

static void t_kill_mid_rebuild(void)
{
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX], s0_new[PATH_MAX + 32], ready[PATH_MAX + 32];
    fresh_dir("killmid", base, sizeof base);
    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db);
    tsdb_sid s0 = 0, s1 = 0;
    populate_and_settle(db, 4, 20000, &s0, &s1);
    int orig_chunks = count_and_scrub_store_chunks(stores[0], NULL, false);
    CHECK(orig_chunks >= 4);
    CHECK(tsdb_close(db) == 0);

    /* Lose store 0 offline and record replacement (with durable REBUILDING marker). */
    CHECK(sh("rm -rf '%s'", stores[0]) == 0);
    snprintf(s0_new, sizeof s0_new, "%s/st0_new", base);
    CHECK(tierctl_lost_dir(dbdir, "s0", s0_new, NULL, TSDB_TIER_ALLOW_SHARED_DEV, NULL, NULL) == 0);
    CHECK(tierctl_store_rebuilding(s0_new));

    /* Fork child to open DB and begin rebuild; kill -9 mid-rebuild once hook signals progress. */
    snprintf(ready, sizeof ready, "%s/ready", base);
    pid_t pid = fork();
    CHECK(pid >= 0);
    if (pid == 0) {
        if (mutant_name)
            execl(SELF, "test_tierctl", "--mutant", mutant_name, "--child", "kill-rebuild", dbdir, ready, (char *)NULL);
        else
            execl(SELF, "test_tierctl", "--child", "kill-rebuild", dbdir, ready, (char *)NULL);
        _exit(127);
    }
    bool got_ready = false;
    int64_t dl = mono_ms() + 15000;
    while (mono_ms() < dl) {
        if (access(ready, F_OK) == 0) {
            got_ready = true;
            break;
        }
        int st = 0;
        if (waitpid(pid, &st, WNOHANG) == pid)
            break;
        struct timespec nap = {0, 10 * 1000000L};
        nanosleep(&nap, NULL);
    }
    if (got_ready) {
        safe_kill(pid, SIGKILL);
        int st = 0;
        waitpid(pid, &st, 0);
        CHECK(WIFSIGNALED(st) && WTERMSIG(st) == SIGKILL);
    } else {
        int st = 0;
        safe_kill(pid, SIGKILL);
        waitpid(pid, &st, 0);
        CHECK(false);
    }

    /* After kill -9 mid-rebuild, REBUILDING MUST still exist on disk (never claimed UP early!). */
    CHECK(tierctl_store_rebuilding(s0_new));

    /* Next open resumes from TIERS + REBUILDING and finishes the rebuild completely. */
    CHECK(open_db(dbdir, &db) == 0 && db != NULL);
    tier *t = tier_find_by_dir(dbdir);
    CHECK(t != NULL);
    CHECK(tierctl_wait_idle(t, 30000) == 0);
    CHECK(wait_store_state(t, 0, CHUNK_STORE_UP, 10000));
    CHECK(!tierctl_store_rebuilding(s0_new));
    CHECK(count_and_scrub_store_chunks(s0_new, NULL, false) == orig_chunks);

    /* Verify reads with 2 OTHER stores down (must use rebuilt s0_new). */
    (void)tsdb_test_ec_retries(db, true);
    tier_store_set_down(t, 3, CHUNK_STORE_DOWN_STALLED);
    tier_store_set_down(t, 4, CHUNK_STORE_DOWN_STALLED);
    eccache_reopen_all(t);
    verify_rows(db, s0, s1, 4, 20000);

    tier_put(t);
    CHECK(tsdb_close(db) == 0);
    CHECK(count_and_scrub_store_chunks(s0_new, NULL, true) == orig_chunks);
    printf("  kill_mid_rebuild: ok (killed mid-rebuild, resumed on next open, %d chunks whole)\n", orig_chunks);
}

/* ------------------------------------------------------------ 3. Power loss (crashfs on new store) */

static int child_crashfs_rebuild(const char *dbdir)
{
    tsdb *db = NULL;
    int rc = open_db(dbdir, &db);
    if (rc != 0 || !db)
        return 2;
    tier *t = tier_find_by_dir(dbdir);
    if (!t) {
        tsdb_close(db);
        return 3;
    }
    rc = tierctl_wait_idle(t, 20000);
    bool up = wait_store_state(t, 1, CHUNK_STORE_UP, 5000);
    tier_put(t);
    tsdb_close(db);
    return (rc == 0 && up) ? 0 : 4;
}

static int run_crashfs_child(const char *new_store_root, const char *dbdir, int warmup, int seed)
{
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512], wbuf[16], sbuf[16];
        const char *prev = getenv("ASAN_OPTIONS");
        snprintf(asan, sizeof asan, "%s%sverify_asan_link_order=0:detect_leaks=0",
                 prev && *prev ? prev : "", prev && *prev ? ":" : "");
        setenv("ASAN_OPTIONS", asan, 1);
        snprintf(wbuf, sizeof wbuf, "%d", warmup);
        snprintf(sbuf, sizeof sbuf, "%d", seed);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", new_store_root, 1);
        setenv("CRASHFS_PROB", "1", 1);
        setenv("CRASHFS_WARMUP", wbuf, 1);
        setenv("CRASHFS_AT_EXIT", "1", 1);
        setenv("CRASHFS_TEAR", "1", 1);
        setenv("CRASHFS_SEED", sbuf, 1);
        int null = open("/dev/null", O_WRONLY);
        if (null >= 0) {
            dup2(null, 1);
            dup2(null, 2);
            close(null);
        }
        if (mutant_name)
            execl(SELF, "test_tierctl", "--mutant", mutant_name, "--child", "crashfs-rebuild", dbdir, (char *)NULL);
        else
            execl(SELF, "test_tierctl", "--child", "crashfs-rebuild", dbdir, (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

static void t_power_loss_crashfs(void)
{
    if (access(SHIM, R_OK) != 0) {
        if (getenv("TSDB_REQUIRE_CRASHFS")) {
            fprintf(stderr, "test_tierctl: %s missing\n", SHIM);
            CHECK(false);
        } else {
            printf("  power_loss_crashfs: skipped (no libcrashfs.so)\n");
        }
        return;
    }
    /* Under TSan, LD_PRELOAD=libcrashfs.so cannot safely run inside a TSan-instrumented binary. */
#if defined(__SANITIZE_THREAD__)
    printf("  power_loss_crashfs: skipped (TSan)\n");
    return;
#endif
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX], s1_new[PATH_MAX + 32], s1_snap[PATH_MAX + 32];
    fresh_dir("powerloss", base, sizeof base);
    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db);
    tsdb_sid s0 = 0, s1 = 0;
    populate_and_settle(db, 2, 20000, &s0, &s1);
    int orig_chunks = count_and_scrub_store_chunks(stores[1], NULL, false);
    CHECK(orig_chunks >= 2);
    CHECK(tsdb_close(db) == 0);

    CHECK(sh("rm -rf '%s'", stores[1]) == 0);
    snprintf(s1_new, sizeof s1_new, "%s/st1_new", base);
    snprintf(s1_snap, sizeof s1_snap, "%s/st1_snap", base);
    CHECK(tierctl_lost_dir(dbdir, "s1", s1_new, NULL, TSDB_TIER_ALLOW_SHARED_DEV, NULL, NULL) == 0);
    CHECK(tierctl_store_rebuilding(s1_new));
    CHECK(sh("cp -a '%s' '%s'", s1_new, s1_snap) == 0);

    int cuts = 0, finished = 0;
    for (int w = 0; w < 200; w += (w < 16 ? 1 : 4)) {
        CHECK(sh("rm -rf '%s' && cp -a '%s' '%s'", s1_new, s1_snap, s1_new) == 0);
        int rc = run_crashfs_child(s1_new, dbdir, w, 100 + w);
        if (rc == 99) {
            cuts++;
            bool still_rb = tierctl_store_rebuilding(s1_new);
            if (!still_rb) {
                /* If REBUILDING was already unlinked before the crash, EVERY chunk MUST already be whole on disk! */
                int got = count_and_scrub_store_chunks(s1_new, NULL, true);
                CHECK(got == orig_chunks);
            }
            /* Reopen cleanly and verify resume finishes and all rows match. */
            CHECK(open_db(dbdir, &db) == 0 && db != NULL);
            tier *t = tier_find_by_dir(dbdir);
            CHECK(t != NULL);
            CHECK(tierctl_wait_idle(t, 20000) == 0);
            CHECK(wait_store_state(t, 1, CHUNK_STORE_UP, 10000));
            CHECK(!tierctl_store_rebuilding(s1_new));
            CHECK(count_and_scrub_store_chunks(s1_new, NULL, false) == orig_chunks);
            (void)tsdb_test_ec_retries(db, true);
            tier_store_set_down(t, 4, CHUNK_STORE_DOWN_STALLED);
            tier_store_set_down(t, 5, CHUNK_STORE_DOWN_STALLED);
            eccache_reopen_all(t);
            verify_rows(db, s0, s1, 2, 20000);
            tier_put(t);
            CHECK(tsdb_close(db) == 0);
        } else if (rc == 0) {
            finished = 1;
            CHECK(!tierctl_store_rebuilding(s1_new));
            CHECK(count_and_scrub_store_chunks(s1_new, NULL, true) == orig_chunks);
            break;
        } else {
            fprintf(stderr, "test_tierctl: crashfs warmup %d returned unexpected status %d\n", w, rc);
            CHECK(false);
            break;
        }
    }
    CHECK(cuts >= 3 && finished == 1);
    printf("  power_loss_crashfs: ok (%d power-cut points tested, all resumed cleanly)\n", cuts);
}

/* ------------------------------------------------------------ 4. Pace held within 20% at 50 MB/s */

static void t_pace_50_mb_s(void)
{
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX], s0_new[PATH_MAX + 32];
    fresh_dir("pace50", base, sizeof base);
    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db);

    /* Configure rebuild_mb_s = 50 in TIERS SETTINGS. */
    int dfd = open(dbdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    CHECK(dfd >= 0);
    tiers_settings ts;
    memset(&ts, 0, sizeof ts);
    ts.set_mask = TSDB_TIER_SET_REBUILD_MB_S;
    ts.rebuild_mb_s = 50;
    CHECK(tiers_append_settings(dfd, true, TIERS_VOL_ALL, &ts) == 0);
    close(dfd);

    tier *t = tier_find_by_dir(dbdir);
    CHECK(t != NULL);
    tierctl_wake(t);
    CHECK(tierctl_wait_idle(t, 5000) == 0);

    /* Put enough 4 MiB objects directly into the tier's backend so store 0 holds ~24-30 MiB of chunks
     * (at 50 MiB/s, ~26 MiB takes ~520 ms: fast to test, long enough for accurate rate measurement). */
    atomic_store(&ecgc_test_nosweep, true);
    chunk_backend *be = tier_backend(t);
    eng *e0 = tsdb_test_engine(db, 0);
    CHECK(be != NULL && e0 != NULL);
    const uint8_t *vuuid = tier_vuuid(e0);
    CHECK(vuuid != NULL);

    const size_t obj_len = 4u << 20; /* 4 MiB -> 1 MiB per data/parity chunk in 4+2 */
    uint8_t *payload = malloc(obj_len);
    CHECK(payload != NULL);
    for (size_t i = 0; i < obj_len; i++)
        payload[i] = (uint8_t)((i * 131u) ^ (i >> 7));

    chunk_geom g;
    CHECK(chunk_geom_derive(&g, 4, 2, obj_len) == 0);
    uint64_t s0_bytes = 0;
    for (int oi = 0; oi < 26; oi++) {
        uint8_t oid[16];
        memset(oid, 0, sizeof oid);
        oid[0] = (uint8_t)(oi + 1);
        oid[1] = 0x5a;
        oid[15] = (uint8_t)(oi * 7 + 3);
        CHECK(chunk_be_put_object(be, 0, vuuid, oid, &g, 1, payload, ~0u, NULL) == 0);
    }
    free(payload);

    int orig_chunks = count_and_scrub_store_chunks(stores[0], &s0_bytes, false);
    CHECK(orig_chunks >= 20 && s0_bytes >= (20ull << 20));

    /* Lose store 0 and time the paced rebuild onto s0_new at 50 MiB/s. */
    CHECK(sh("rm -rf '%s'", stores[0]) == 0);
    snprintf(s0_new, sizeof s0_new, "%s/st0_new", base);

    int64_t t0 = mono_ms();
    CHECK(tsdb_tier_lost(db, "s0", s0_new, TSDB_TIER_ALLOW_SHARED_DEV) == 0);
    CHECK(tierctl_wait_idle(t, 30000) == 0);
    CHECK(wait_store_state(t, 0, CHUNK_STORE_UP, 10000));
    int64_t elapsed_ms = mono_ms() - t0;

    uint64_t rebuilt_bytes = 0;
    int rebuilt_chunks = count_and_scrub_store_chunks(s0_new, &rebuilt_bytes, false);
    CHECK(rebuilt_chunks == orig_chunks && rebuilt_bytes == s0_bytes);

    double mb = (double)rebuilt_bytes / (1024.0 * 1024.0);
    double sec = (double)(elapsed_ms > 0 ? elapsed_ms : 1) / 1000.0;
    double mb_s = mb / sec;

    /* Rate MUST NOT exceed 60 MiB/s (50 + 20%) — kills `rebuildfast`! */
    CHECK(mb_s <= 60.0);

    tier_put(t);
    CHECK(tsdb_close(db) == 0);
    atomic_store(&ecgc_test_nosweep, false);
    printf("  pace_50_mb_s: ok (%.1f MiB in %" PRId64 " ms = %.1f MiB/s, cap 50 MiB/s +20%%)\n",
           mb, elapsed_ms, mb_s);
}

/* ------------------------------------------------------------ 5. Running writer picks up CLI tier changes */

static int run_cli(const char *dbdir, char *const extra_args[])
{
    pid_t pid = fork();
    if (pid == 0) {
        char *argv[16];
        int k = 0;
        argv[k++] = CLI;
        argv[k++] = (char *)dbdir;
        for (int i = 0; extra_args[i] && k < 15; i++)
            argv[k++] = extra_args[i];
        argv[k] = NULL;
        int null = open("/dev/null", O_WRONLY);
        if (null >= 0) {
            dup2(null, 1);
            dup2(null, 2);
            close(null);
        }
        execv(CLI, argv);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

static void t_live_cli_pickup(void)
{
    if (access(CLI, X_OK) != 0) {
        printf("  live_cli_pickup: skipped (no %s binary)\n", CLI);
        return;
    }
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX], s6[PATH_MAX + 32], s3_new[PATH_MAX + 32];
    fresh_dir("livecli", base, sizeof base);
    tierctl_set_poll_ms(100);

    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db);
    tsdb_sid s0 = 0, s1 = 0;
    populate_and_settle(db, 3, 20000, &s0, &s1);
    int orig_s3_chunks = count_and_scrub_store_chunks(stores[3], NULL, false);
    CHECK(orig_s3_chunks >= 3);

    tier *t = tier_find_by_dir(dbdir);
    eng *e0 = tsdb_test_engine(db, 0);
    CHECK(t != NULL && e0 != NULL);

    /* 5a. CLI `tier set` picked up by running writer's periodic TIERS stat loop (no in-process wake). */
    char *set_args[] = {"tier", "set", "--rebuild-mb-s", "123", "--after", "2d", NULL};
    CHECK(run_cli(dbdir, set_args) == 0);
    int64_t dl = mono_ms() + 5000;
    while (mono_ms() < dl) {
        const tier_settings *cs = tier_eng_settings(e0);
        if (cs && cs->rebuild_mb_s == 123 && cs->tier_after_s == 2 * 86400ull)
            break;
        struct timespec nap = {0, 25 * 1000000L};
        nanosleep(&nap, NULL);
    }
    const tier_settings *cs = tier_eng_settings(e0);
    CHECK(cs && cs->rebuild_mb_s == 123 && cs->tier_after_s == 2 * 86400ull);

    /* 5b. CLI `tier lost s3 --replace` picked up by running writer's TIERS re-read and rebuilt live! */
    CHECK(sh("rm -rf '%s'", stores[3]) == 0);
    snprintf(s3_new, sizeof s3_new, "%s/st3_new", base);
    char *lost_args[] = {"tier", "lost", "s3", "--replace", s3_new, "--allow-shared-device", NULL};
    CHECK(run_cli(dbdir, lost_args) == 0);

    dl = mono_ms() + 15000;
    while (mono_ms() < dl) {
        tierctl_progress prog;
        if (tierctl_get_progress(t, &prog) && prog.rebuilds_done >= 1 && !prog.active &&
            tier_store_state(t, 3) == CHUNK_STORE_UP)
            break;
        struct timespec nap = {0, 30 * 1000000L};
        nanosleep(&nap, NULL);
    }
    CHECK(tier_store_state(t, 3) == CHUNK_STORE_UP);
    CHECK(!tierctl_store_rebuilding(s3_new));
    CHECK(count_and_scrub_store_chunks(s3_new, NULL, false) == orig_s3_chunks);

    /* 5c. CLI `tier add` opens a newly added 7th store live in the running writer. */
    snprintf(s6, sizeof s6, "%s/st6:hdd", base);
    char *add_args[] = {"tier", "add", s6, "--allow-shared-device", NULL};
    CHECK(run_cli(dbdir, add_args) == 0);
    dl = mono_ms() + 5000;
    while (mono_ms() < dl && tier_store_count(t) < 7) {
        struct timespec nap = {0, 25 * 1000000L};
        nanosleep(&nap, NULL);
    }
    CHECK(tier_store_count(t) == 7);
    CHECK(tier_store_state(t, 6) == CHUNK_STORE_UP);

    /* Reads still answer identically (even with 2 other stores taken down). */
    (void)tsdb_test_ec_retries(db, true);
    tier_store_set_down(t, 0, CHUNK_STORE_DOWN_STALLED);
    tier_store_set_down(t, 1, CHUNK_STORE_DOWN_STALLED);
    eccache_reopen_all(t);
    verify_rows(db, s0, s1, 3, 20000);

    tier_put(t);
    CHECK(tsdb_close(db) == 0);
    tierctl_set_poll_ms(1000);
    printf("  live_cli_pickup: ok (live CLI tier set, tier lost --replace, and tier add applied)\n");
}

/* ------------------------------------------------------------ 6. E15: >m LOST -> TSDB_ERR_IO vs DOWN -> UNAVAILABLE */

static void t_e15_lost_vs_down(void)
{
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX];
    fresh_dir("e15", base, sizeof base);
    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db);

    const int64_t day_ms = 86400000LL;
    tsdb_sid s0 = 0, s1 = 0;
    populate_and_settle(db, 2, 20000, &s0, &s1);

    tier *t = tier_find_by_dir(dbdir);
    CHECK(t != NULL);

    /* Part A: 3 stores DOWN (stalled, NOT lost; 3 > m=2) -> row read returns TSDB_ERR_UNAVAILABLE, stats answer! */
    (void)tsdb_test_ec_retries(db, true);
    tier_store_set_down(t, 0, CHUNK_STORE_DOWN_STALLED);
    tier_store_set_down(t, 1, CHUNK_STORE_DOWN_STALLED);
    tier_store_set_down(t, 2, CHUNK_STORE_DOWN_STALLED);
    eccache_reopen_all(t);

    int64_t *ts_buf = malloc(20000 * sizeof *ts_buf);
    double *v_buf = malloc(20000 * sizeof *v_buf);
    CHECK(ts_buf && v_buf);
    size_t n = 0;
    /* Read both series in partition 0 (spans all k=4 data positions): pigeonhole guarantees hitting >= 1 of 3 DOWN stores. */
    int drc = query_f64(db, s0, 0, day_ms, ts_buf, v_buf, 20000, &n);
    if (drc == 0)
        drc = query_f64(db, s1, 0, day_ms, ts_buf, v_buf, 20000, &n);
    CHECK(drc == TSDB_ERR_UNAVAILABLE);

    /* Whole-day bucket statistics query is answered from the local .ec index and MUST succeed! */
    tsdb_bucket *bk = NULL;
    size_t nbk = 0;
    int brc = tsdb_query_agg(db, s0, 0, 2 * day_ms, day_ms, &bk, &nbk);
    CHECK(brc == 0 && nbk == 2 && bk && bk[0].count == 20000 && bk[1].count == 20000);
    tsdb_free(bk);
    bk = NULL;

    /* Part B: Declare those 3 stores (> m=2) permanently LOST in TIERS and let tierctl apply STATE LOST. */
    int dfd = open(dbdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    CHECK(dfd >= 0);
    tiers_view tv;
    CHECK(tiers_load(dfd, false, false, &tv) == 0 && tv.nstores == 6);
    for (int i = 0; i < 3; i++)
        CHECK(tiers_append_state(dfd, true, tv.stores[i].id, TIERS_STATE_LOST, 0, false, NULL, NULL) == 0);
    tiers_view_free(&tv);
    close(dfd);

    tierctl_wake(t);
    CHECK(tierctl_wait_idle(t, 5000) == 0);
    chunk_backend *be = tier_backend(t);
    for (int i = 0; i < 3; i++)
        CHECK(chunk_be_store_is_lost(be, i));

    /* Drop granule cache and read partition 0 (both series -> all k=4 data positions):
     * MUST return TSDB_ERR_IO and stderr MUST name partition 0 (E15)! */
    (void)tsdb_test_ec_retries(db, true);
    eccache_reopen_all(t);
    char log[4096];
    cap_begin();
    int rrc = query_f64(db, s0, 0, day_ms, ts_buf, v_buf, 20000, &n);
    if (rrc == 0)
        rrc = query_f64(db, s1, 0, day_ms, ts_buf, v_buf, 20000, &n);
    cap_end(log, sizeof log);
    CHECK(rrc == TSDB_ERR_IO);
    CHECK(strstr(log, "partition 0") != NULL);

    /* Partition 1 also returns TSDB_ERR_IO naming partition 1. */
    cap_begin();
    rrc = query_f64(db, s0, day_ms, 2 * day_ms, ts_buf, v_buf, 20000, &n);
    if (rrc == 0)
        rrc = query_f64(db, s1, day_ms, 2 * day_ms, ts_buf, v_buf, 20000, &n);
    cap_end(log, sizeof log);
    CHECK(rrc == TSDB_ERR_IO);
    CHECK(strstr(log, "partition 1") != NULL);

    /* Statistics query over the lost partitions MUST STILL ANSWER from the intact local .ec index! */
    nbk = 0;
    brc = tsdb_query_agg(db, s0, 0, 2 * day_ms, day_ms, &bk, &nbk);
    CHECK(brc == 0 && nbk == 2 && bk && bk[0].count == 20000 && bk[1].count == 20000);
    tsdb_free(bk);
    free(ts_buf);
    free(v_buf);

    tier_put(t);
    CHECK(tsdb_close(db) == 0);
    printf("  e15_lost_vs_down: ok (>m DOWN -> UNAVAILABLE, >m LOST -> TSDB_ERR_IO naming partition, stats answer)\n");
}

/* ------------------------------------------------------------ 7. Power loss inside tierctl_lost_dir (META, REBUILDING, TIERS) */

static int child_crashfs_lost_cmd(const char *dbdir, const char *new_store)
{
    int rc = tierctl_lost_dir(dbdir, "s2", new_store, NULL, TSDB_TIER_ALLOW_SHARED_DEV, NULL, NULL);
    return rc == 0 ? 0 : 4;
}

static int run_crashfs_lost_cmd(const char *root_dir, const char *dbdir, const char *new_store, int warmup, int seed)
{
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512], wbuf[16], sbuf[16];
        const char *prev = getenv("ASAN_OPTIONS");
        snprintf(asan, sizeof asan, "%s%sverify_asan_link_order=0:detect_leaks=0",
                 prev && *prev ? prev : "", prev && *prev ? ":" : "");
        setenv("ASAN_OPTIONS", asan, 1);
        snprintf(wbuf, sizeof wbuf, "%d", warmup);
        snprintf(sbuf, sizeof sbuf, "%d", seed);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", root_dir, 1);
        setenv("CRASHFS_PROB", "1", 1);
        setenv("CRASHFS_WARMUP", wbuf, 1);
        setenv("CRASHFS_AT_EXIT", "1", 1);
        setenv("CRASHFS_TEAR", "1", 1);
        setenv("CRASHFS_SEED", sbuf, 1);
        int null = open("/dev/null", O_WRONLY);
        if (null >= 0) {
            dup2(null, 1);
            dup2(null, 2);
            close(null);
        }
        if (mutant_name)
            execl(SELF, "test_tierctl", "--mutant", mutant_name, "--child", "crashfs-lost", dbdir, new_store,
                  (char *)NULL);
        else
            execl(SELF, "test_tierctl", "--child", "crashfs-lost", dbdir, new_store, (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

static void t_lost_cmd_crashfs(void)
{
    if (access(SHIM, R_OK) != 0) {
        if (getenv("TSDB_REQUIRE_CRASHFS")) {
            fprintf(stderr, "test_tierctl: %s missing\n", SHIM);
            CHECK(false);
        } else {
            printf("  lost_cmd_crashfs: skipped (no libcrashfs.so)\n");
        }
        return;
    }
#if defined(__SANITIZE_THREAD__)
    printf("  lost_cmd_crashfs: skipped (TSan)\n");
    return;
#endif
    char base[PATH_MAX], snap[PATH_MAX + 32], work[PATH_MAX + 32];
    char dbdir[PATH_MAX], stores[6][PATH_MAX];
    fresh_dir("lostcmd", base, sizeof base);
    snprintf(snap, sizeof snap, "%s/snap", base);
    snprintf(work, sizeof work, "%s/work", base);
    CHECK(sh("mkdir -p '%s'", snap) == 0);

    /* Create the snapshot DB inside `work` so recorded paths inside TIERS match `work` directly on every restore. */
    CHECK(sh("mkdir -p '%s'", work) == 0);
    tsdb *db = NULL;
    setup_6store_db(work, dbdir, stores, &db);
    tsdb_sid s0 = 0, s1 = 0;
    populate_and_settle(db, 2, 20000, &s0, &s1);
    int orig_chunks = count_and_scrub_store_chunks(stores[2], NULL, false);
    CHECK(orig_chunks >= 2);
    CHECK(tsdb_close(db) == 0);
    CHECK(sh("rm -rf '%s'", stores[2]) == 0);
    CHECK(sh("rm -rf '%s' && cp -a '%s' '%s'", snap, work, snap) == 0);

    char s2_new[PATH_MAX + 64];
    snprintf(s2_new, sizeof s2_new, "%s/st2_new", work);

    int cuts = 0, finished = 0;
    for (int w = 0; w < 80; w++) {
        CHECK(sh("rm -rf '%s' && cp -a '%s' '%s'", work, snap, work) == 0);
        int rc = run_crashfs_lost_cmd(work, dbdir, s2_new, w, 500 + w);
        if (rc == 99) {
            cuts++;
            /* Check TIERS state after cut: either s2 is not yet replaced (re-running tierctl_lost_dir must adopt any
             * half-made store and succeed!) OR s2 is already REPLACED_BY (in which case REBUILDING MUST exist!). */
            int dfd = open(dbdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
            CHECK(dfd >= 0);
            tiers_view tv;
            CHECK(tiers_load(dfd, true, true, &tv) == 0 && tv.nstores == 6);
            bool already_replaced = (tv.stores[2].state == TIERS_STATE_REPLACED_BY);
            tiers_view_free(&tv);
            close(dfd);

            if (already_replaced) {
                /* REPLACED_BY was durable in TIERS -> REBUILDING MUST already be durable before REPLACED_BY! */
                CHECK(tierctl_store_rebuilding(s2_new));
            } else {
                CHECK(tierctl_lost_dir(dbdir, "s2", s2_new, NULL, TSDB_TIER_ALLOW_SHARED_DEV, NULL, NULL) == 0);
                CHECK(tierctl_store_rebuilding(s2_new));
            }

            /* Opening the database MUST finish the rebuild cleanly and pass reads with 2 other stores DOWN. */
            CHECK(open_db(dbdir, &db) == 0 && db != NULL);
            tier *t = tier_find_by_dir(dbdir);
            CHECK(t != NULL);
            CHECK(tierctl_wait_idle(t, 20000) == 0);
            CHECK(wait_store_state(t, 2, CHUNK_STORE_UP, 10000));
            CHECK(!tierctl_store_rebuilding(s2_new));
            CHECK(count_and_scrub_store_chunks(s2_new, NULL, false) == orig_chunks);
            (void)tsdb_test_ec_retries(db, true);
            tier_store_set_down(t, 0, CHUNK_STORE_DOWN_STALLED);
            tier_store_set_down(t, 1, CHUNK_STORE_DOWN_STALLED);
            eccache_reopen_all(t);
            verify_rows(db, s0, s1, 2, 20000);
            tier_put(t);
            CHECK(tsdb_close(db) == 0);
        } else if (rc == 0) {
            finished = 1;
            CHECK(tierctl_store_rebuilding(s2_new));
            break;
        } else {
            fprintf(stderr, "test_tierctl: crashfs-lost warmup %d returned unexpected status %d\n", w, rc);
            CHECK(false);
            break;
        }
    }
    CHECK(cuts >= 3 && finished == 1);
    printf("  lost_cmd_crashfs: ok (%d power-cut points across META/REBUILDING/TIERS, all recovered cleanly)\n", cuts);
}

/* ------------------------------------------------------------ 8. Add store after settle, then lose & replace + E15 */

static void t_add_then_lost(void)
{
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX], s6[PATH_MAX + 32], s2_new[PATH_MAX + 32];
    fresh_dir("addlost", base, sizeof base);
    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db);

    const int64_t day_ms = 86400000LL;
    tsdb_sid s0 = 0, s1 = 0;
    /* Settle 6 parts under N = 6 stores (4+2). */
    populate_and_settle(db, 6, 20000, &s0, &s1);

    /* Add a 7th store LIVE via tsdb_tier_add and wait for tierctl to open it (bn becomes 7). */
    snprintf(s6, sizeof s6, "%s/st6", base);
    CHECK(tsdb_tier_add(db, s6, "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);
    tier *t = tier_find_by_dir(dbdir);
    CHECK(t != NULL);
    tierctl_wake(t);
    CHECK(tierctl_wait_idle(t, 10000) == 0);
    CHECK(tier_store_count(t) == 7);
    CHECK(tier_store_state(t, 6) == CHUNK_STORE_UP);

    /* Settle 4 MORE parts under N = 7 stores (days 6..9). */
    int64_t *ts = malloc(20000 * sizeof *ts);
    double *va = malloc(20000 * sizeof *va);
    double *vb = malloc(20000 * sizeof *vb);
    CHECK(ts && va && vb);
    for (int p = 6; p < 10; p++) {
        int64_t base_ts = (int64_t)p * day_ms + 1000LL;
        for (int r = 0; r < 20000; r++) {
            ts[r] = base_ts + (int64_t)r * 1000LL;
            va[r] = det_val(p, r, 0);
            vb[r] = det_val(p, r, 1);
        }
        tsdb_values val_a = {.type = TSDB_FLOAT64, .values = va, .valid = NULL};
        tsdb_values val_b = {.type = TSDB_FLOAT64, .values = vb, .valid = NULL};
        CHECK(tsdb_insert_series_values(db, s0, ts, &val_a, 20000) == 0);
        CHECK(tsdb_insert_series_values(db, s1, ts, &val_b, 20000) == 0);
        CHECK(tsdb_flush(db) == 0);
    }
    free(ts);
    free(va);
    free(vb);
    size_t settled = 0;
    CHECK(tsdb_test_settle(db, pick_all, NULL, &settled) == 0 && settled >= 4);

    /* Record exact chunk count & bytes on store 2 BEFORE losing it (mix of N=6 and N=7 objects!). */
    uint64_t orig_bytes = 0;
    int orig_chunks = count_and_scrub_store_chunks(stores[2], &orig_bytes, false);
    CHECK(orig_chunks >= 6 && orig_bytes > 0);

    /* Lose store 2 and replace onto s2_new while bn == 7. */
    CHECK(sh("rm -rf '%s'", stores[2]) == 0);
    snprintf(s2_new, sizeof s2_new, "%s/st2_new", base);
    CHECK(tsdb_tier_lost(db, "s2", s2_new, TSDB_TIER_ALLOW_SHARED_DEV) == 0);
    CHECK(tierctl_wait_idle(t, 30000) == 0);
    CHECK(wait_store_state(t, 2, CHUNK_STORE_UP, 10000));
    CHECK(!tierctl_store_rebuilding(s2_new));

    /* EVERY SINGLE old (N=6) AND new (N=7) position that sat on store 2 must be rebuilt onto s2_new! */
    uint64_t new_bytes = 0;
    int new_chunks = count_and_scrub_store_chunks(s2_new, &new_bytes, false);
    CHECK(new_chunks == orig_chunks && new_bytes == orig_bytes);

    /* Verify ALL 10 parts bit-for-bit with 2 OTHER stores (0 and 1) DOWN! */
    (void)tsdb_test_ec_retries(db, true);
    tier_store_set_down(t, 0, CHUNK_STORE_DOWN_STALLED);
    tier_store_set_down(t, 1, CHUNK_STORE_DOWN_STALLED);
    eccache_reopen_all(t);
    verify_rows(db, s0, s1, 10, 20000);

    /* Also verify E15 on an old N=6 object when 3 old stores (0, 1, 2) are DOWN vs LOST while s6 is UP! */
    (void)tsdb_test_ec_retries(db, true);
    tier_store_set_down(t, 2, CHUNK_STORE_DOWN_STALLED);
    eccache_reopen_all(t);
    int64_t *ts_buf = malloc(20000 * sizeof *ts_buf);
    double *v_buf = malloc(20000 * sizeof *v_buf);
    CHECK(ts_buf && v_buf);
    size_t n = 0;
    int drc = query_f64(db, s0, 0, day_ms, ts_buf, v_buf, 20000, &n);
    if (drc == 0)
        drc = query_f64(db, s1, 0, day_ms, ts_buf, v_buf, 20000, &n);
    CHECK(drc == TSDB_ERR_UNAVAILABLE);

    int dfd = open(dbdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    CHECK(dfd >= 0);
    tiers_view tv;
    CHECK(tiers_load(dfd, false, false, &tv) == 0 && tv.nstores == 7);
    for (int i = 0; i < 3; i++) {
        const uint8_t *cid = tv.stores[i].state == TIERS_STATE_REPLACED_BY ? tv.stores[i].replaced_by : tv.stores[i].id;
        CHECK(tiers_append_state(dfd, true, cid, TIERS_STATE_LOST, 0, false, NULL, NULL) == 0);
    }
    tiers_view_free(&tv);
    close(dfd);
    tierctl_wake(t);
    CHECK(tierctl_wait_idle(t, 5000) == 0);
    (void)tsdb_test_ec_retries(db, true);
    eccache_reopen_all(t);
    char log[4096];
    cap_begin();
    int rrc = query_f64(db, s0, 0, day_ms, ts_buf, v_buf, 20000, &n);
    if (rrc == 0)
        rrc = query_f64(db, s1, 0, day_ms, ts_buf, v_buf, 20000, &n);
    cap_end(log, sizeof log);
    CHECK(rrc == TSDB_ERR_IO);
    CHECK(strstr(log, "partition 0") != NULL);

    free(ts_buf);
    free(v_buf);
    tier_put(t);
    CHECK(tsdb_close(db) == 0);
    printf("  add_then_lost: ok (%d/%d mixed N=6+N=7 chunks rebuilt exact, E15 classified after tier add)\n",
           new_chunks, orig_chunks);
}

/* ------------------------------------------------------------ 9. Two stores LOST in 4+2, replace 1 -> promotes UP */

static void t_two_lost_replace_one(void)
{
    char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX], s1_new[PATH_MAX + 32];
    fresh_dir("twolost", base, sizeof base);
    tsdb *db = NULL;
    setup_6store_db(base, dbdir, stores, &db);

    tsdb_sid s0 = 0, s1 = 0;
    populate_and_settle(db, 3, 20000, &s0, &s1);
    uint64_t orig_bytes = 0;
    int orig_chunks = count_and_scrub_store_chunks(stores[1], &orig_bytes, false);
    CHECK(orig_chunks >= 3 && orig_bytes > 0);

    /* Declare store 0 permanently LOST (not replaced), then lose and replace store 1. */
    CHECK(sh("rm -rf '%s' '%s'", stores[0], stores[1]) == 0);
    int dfd = open(dbdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    CHECK(dfd >= 0);
    tiers_view tv;
    CHECK(tiers_load(dfd, false, false, &tv) == 0 && tv.nstores == 6);
    CHECK(tiers_append_state(dfd, true, tv.stores[0].id, TIERS_STATE_LOST, 0, false, NULL, NULL) == 0);
    tiers_view_free(&tv);
    close(dfd);

    tier *t = tier_find_by_dir(dbdir);
    CHECK(t != NULL);
    tierctl_wake(t);
    CHECK(tierctl_wait_idle(t, 5000) == 0);
    CHECK(chunk_be_store_is_lost(tier_backend(t), 0));

    snprintf(s1_new, sizeof s1_new, "%s/st1_new", base);
    CHECK(tsdb_tier_lost(db, "s1", s1_new, TSDB_TIER_ALLOW_SHARED_DEV) == 0);
    /* Must finish and promote s1_new to UP WITHOUT spinning forever waiting on permanently LOST store 0! */
    CHECK(tierctl_wait_idle(t, 30000) == 0);
    CHECK(wait_store_state(t, 1, CHUNK_STORE_UP, 10000));
    CHECK(!tierctl_store_rebuilding(s1_new));

    uint64_t new_bytes = 0;
    int new_chunks = count_and_scrub_store_chunks(s1_new, &new_bytes, false);
    CHECK(new_chunks == orig_chunks && new_bytes == orig_bytes);

    /* Take 1 additional store (s2) DOWN: with s0 LOST and s2 DOWN (2 total unavailable), reads MUST use s1_new! */
    (void)tsdb_test_ec_retries(db, true);
    tier_store_set_down(t, 2, CHUNK_STORE_DOWN_STALLED);
    eccache_reopen_all(t);
    verify_rows(db, s0, s1, 3, 20000);

    tier_put(t);
    CHECK(tsdb_close(db) == 0);
    printf("  two_lost_replace_one: ok (s0 LOST, s1 replaced & promoted UP with %d chunks, reads verified)\n",
           new_chunks);
}

/* ------------------------------------------------------------ 10. Shrink 7 -> 6 stores (4+2) + refusal + kill resume */

static void setup_7store_db(const char *base, char dbdir[PATH_MAX], char stores[7][PATH_MAX], tsdb **out_db)
{
    int n = snprintf(dbdir, PATH_MAX, "%s/db", base);
    CHECK(n > 0 && n < PATH_MAX);
    CHECK(sh("mkdir -p '%s'", dbdir) == 0);
    tsdb *db = NULL;
    CHECK(open_db(dbdir, &db) == 0 && db != NULL);
    for (int i = 0; i < 7; i++) {
        n = snprintf(stores[i], PATH_MAX, "%s/st%d", base, i);
        CHECK(n > 0 && n < PATH_MAX);
        CHECK(tsdb_tier_add(db, stores[i], "hdd", TSDB_TIER_ALLOW_SHARED_DEV) == 0);
    }
    CHECK(tsdb_close(db) == 0);
    CHECK(open_db(dbdir, out_db) == 0 && *out_db != NULL);
}

static int child_kill_shrink(const char *dbdir, const char *ready_file)
{
    kill_hook_ctx hctx = {.ready_file = ready_file, .wrote = false};
    tierctl_set_rebuild_hook(kill_progress_hook, &hctx);
    tsdb *db = NULL;
    if (open_db(dbdir, &db) != 0 || !db)
        return 2;
    for (int i = 0; i < 200; i++) {
        struct timespec nap = {0, 50 * 1000000L};
        nanosleep(&nap, NULL);
    }
    tsdb_close(db);
    return 1; /* should have been killed mid-shrink */
}

static void t_shrink_7to6(void)
{
    /* 10a. Refusal when remaining < k+m (6 stores at 4+2 -> 5 < 6), then succeeds once redundancy lowered to 4+1. */
    {
        char base[PATH_MAX], dbdir[PATH_MAX], stores[6][PATH_MAX], log[2048];
        fresh_dir("shrinkrefuse", base, sizeof base);
        tsdb *db = NULL;
        setup_6store_db(base, dbdir, stores, &db);
        tsdb_sid s0 = 0, s1 = 0;
        populate_and_settle(db, 2, 10000, &s0, &s1);

        cap_begin();
        CHECK(tsdb_tier_lost(db, "s4", NULL, 0) == TSDB_ERR_INVALID);
        cap_end(log, sizeof log);
        CHECK(strstr(log, "fewer than") != NULL && strstr(log, "redundancy") != NULL);

        /* Lower redundancy to 4+1 in TIERS SETTINGS, let tierctl apply it, then --shrink succeeds (5 >= 4+1). */
        int dfd = open(dbdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        CHECK(dfd >= 0);
        tiers_settings ts;
        memset(&ts, 0, sizeof ts);
        ts.set_mask = TSDB_TIER_SET_REDUNDANCY;
        ts.k = 4;
        ts.m = 1;
        CHECK(tiers_append_settings(dfd, true, TIERS_VOL_ALL, &ts) == 0);
        close(dfd);

        tier *t = tier_find_by_dir(dbdir);
        CHECK(t != NULL);
        tierctl_wake(t);
        CHECK(tierctl_wait_idle(t, 10000) == 0);

        CHECK(sh("rm -rf '%s'", stores[4]) == 0);
        CHECK(tsdb_tier_lost(db, "s4", NULL, 0) == 0);
        CHECK(tierctl_wait_idle(t, 30000) == 0);
        tier_put(t);
        CHECK(tsdb_close(db) == 0);

        /* Reopen opens 5 active stores cleanly and answers every row bit-exact. */
        CHECK(open_db(dbdir, &db) == 0 && db != NULL);
        t = tier_find_by_dir(dbdir);
        CHECK(t != NULL && tier_store_count(t) == 5);
        verify_rows(db, s0, s1, 2, 10000);
        tier_put(t);
        CHECK(tsdb_close(db) == 0);
    }

    /* 10b. Main 7 -> 6 shrink at 4+2: answers bit-exact before/during/after, all live objects placed on the 6
     * remaining stores, TIERS marks REMOVED, reopen opens 6 stores cleanly, reads survive 2 remaining stores DOWN. */
    char base[PATH_MAX], dbdir[PATH_MAX], stores[7][PATH_MAX], log[2048];
    fresh_dir("shrink7to6", base, sizeof base);
    tsdb *db = NULL;
    setup_7store_db(base, dbdir, stores, &db);

    tsdb_sid s0 = 0, s1 = 0;
    populate_and_settle(db, 6, 20000, &s0, &s1);
    verify_rows(db, s0, s1, 6, 20000);

    int orig_s2_chunks = count_and_scrub_store_chunks(stores[2], NULL, false);
    CHECK(orig_s2_chunks >= 1);

    tier *t = tier_find_by_dir(dbdir);
    CHECK(t != NULL && tier_store_count(t) == 7);

    /* Lose store 2 permanently and start --shrink. */
    CHECK(sh("rm -rf '%s'", stores[2]) == 0);
    CHECK(tsdb_tier_lost(db, "s2", NULL, 0) == 0);

    /* Reads must answer bit-exact during the shrink as well as after. */
    verify_rows(db, s0, s1, 6, 20000);

    CHECK(tierctl_wait_idle(t, 30000) == 0);
    verify_rows(db, s0, s1, 6, 20000);

    tierctl_progress prog;
    CHECK(tierctl_get_progress(t, &prog));
    CHECK(!prog.shrink_active && prog.shrink_objects_left == 0 && prog.shrinks_done >= 1);

    /* TIERS must record store 2 as TIERS_STATE_REMOVED, and 0 objects left to shrink. */
    int dfd = open(dbdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    CHECK(dfd >= 0);
    tiers_view tv;
    CHECK(tiers_load(dfd, false, false, &tv) == 0 && tv.nstores == 7);
    CHECK(tv.stores[2].state == TIERS_STATE_REMOVED);
    const char *vpaths[1] = {dbdir};
    uint64_t left_on_disk = 999;
    CHECK(tierctl_count_shrink_objects(&tv, vpaths, 1, &left_on_disk) == 0);
    /* Every live .ec object must have all 6 positions on the 6 remaining stores (kills `shrinkearly`!). */
    CHECK(left_on_disk == 0);
    tiers_view_free(&tv);
    close(dfd);

    /* Calling --shrink again on an already-REMOVED store is refused. */
    cap_begin();
    CHECK(tsdb_tier_lost(db, "s2", NULL, 0) == TSDB_ERR_INVALID);
    cap_end(log, sizeof log);
    CHECK(strstr(log, "REMOVED") != NULL);

    tier_put(t);
    CHECK(tsdb_close(db) == 0);

    /* Reopen: backend opens only the 6 active stores (skipping REMOVED store 2). */
    CHECK(open_db(dbdir, &db) == 0 && db != NULL);
    t = tier_find_by_dir(dbdir);
    CHECK(t != NULL);
    CHECK(tier_store_count(t) == 6);
    verify_rows(db, s0, s1, 6, 20000);

    /* Drop granule cache and take 2 of the 6 remaining stores DOWN -> reads MUST succeed using the 4 surviving stores
     * (double-kills `shrinkearly` and verifies post-reopen placement matches chunk_be_place!). */
    (void)tsdb_test_ec_retries(db, true);
    tier_store_set_down(t, 0, CHUNK_STORE_DOWN_STALLED);
    tier_store_set_down(t, 1, CHUNK_STORE_DOWN_STALLED);
    eccache_reopen_all(t);
    verify_rows(db, s0, s1, 6, 20000);

    tier_put(t);
    CHECK(tsdb_close(db) == 0);

    /* Scrub all 6 remaining stores offline. */
    int total_remaining = 0;
    for (int i = 0; i < 7; i++) {
        if (i == 2)
            continue;
        int c = count_and_scrub_store_chunks(stores[i], NULL, true);
        CHECK(c >= 6);
        total_remaining += c;
    }
    CHECK(total_remaining >= 36);
    printf("  shrink_7to6: ok (7->6 at 4+2, refusal below k+m verified, reopen=6 stores, 2-down read ok)\n");
}

static void t_kill_mid_shrink(void)
{
    char base[PATH_MAX], dbdir[PATH_MAX], stores[7][PATH_MAX], ready[PATH_MAX + 32];
    fresh_dir("killshrink", base, sizeof base);
    tsdb *db = NULL;
    setup_7store_db(base, dbdir, stores, &db);
    tsdb_sid s0 = 0, s1 = 0;
    populate_and_settle(db, 8, 20000, &s0, &s1);
    CHECK(tsdb_close(db) == 0);

    /* Lose store 2 offline and record STATE LOST + STATE SHRINKING in TIERS. */
    CHECK(sh("rm -rf '%s'", stores[2]) == 0);
    CHECK(tierctl_lost_dir(dbdir, "s2", NULL, NULL, 0, NULL, NULL) == 0);

    int dfd = open(dbdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    CHECK(dfd >= 0);
    tiers_view tv;
    CHECK(tiers_load(dfd, false, false, &tv) == 0 && tv.nstores == 7);
    CHECK(tv.stores[2].state == TIERS_STATE_SHRINKING);
    const char *vpaths_pre[1] = {dbdir};
    uint64_t left_pre = 0;
    CHECK(tierctl_count_shrink_objects(&tv, vpaths_pre, 1, &left_pre) == 0 && left_pre > 0);
    tiers_view_free(&tv);
    close(dfd);

    /* Fork child to open DB and begin shrink; kill -9 mid-shrink once hook signals first re-encoded object. */
    snprintf(ready, sizeof ready, "%s/ready", base);
    pid_t pid = fork();
    CHECK(pid >= 0);
    if (pid == 0) {
        if (mutant_name)
            execl(SELF, "test_tierctl", "--mutant", mutant_name, "--child", "kill-shrink", dbdir, ready, (char *)NULL);
        else
            execl(SELF, "test_tierctl", "--child", "kill-shrink", dbdir, ready, (char *)NULL);
        _exit(127);
    }
    bool got_ready = false;
    int64_t dl = mono_ms() + 15000;
    while (mono_ms() < dl) {
        if (access(ready, F_OK) == 0) {
            got_ready = true;
            break;
        }
        int st = 0;
        if (waitpid(pid, &st, WNOHANG) == pid)
            break;
        struct timespec nap = {0, 10 * 1000000L};
        nanosleep(&nap, NULL);
    }
    if (got_ready) {
        safe_kill(pid, SIGKILL);
        int st = 0;
        waitpid(pid, &st, 0);
        CHECK(WIFSIGNALED(st) && WTERMSIG(st) == SIGKILL);
    } else {
        int st = 0;
        safe_kill(pid, SIGKILL);
        waitpid(pid, &st, 0);
        CHECK(false);
    }

    /* After kill -9 mid-shrink, store 2 MUST still be SHRINKING in TIERS (never marked REMOVED early!). */
    dfd = open(dbdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    CHECK(dfd >= 0);
    CHECK(tiers_load(dfd, false, false, &tv) == 0 && tv.nstores == 7);
    CHECK(tv.stores[2].state == TIERS_STATE_SHRINKING);
    tiers_view_free(&tv);
    close(dfd);

    /* Next open resumes from TIERS SHRINKING and finishes cleanly to REMOVED. */
    CHECK(open_db(dbdir, &db) == 0 && db != NULL);
    tier *t = tier_find_by_dir(dbdir);
    CHECK(t != NULL);
    CHECK(tierctl_wait_idle(t, 30000) == 0);

    dfd = open(dbdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    CHECK(dfd >= 0);
    CHECK(tiers_load(dfd, false, false, &tv) == 0 && tv.nstores == 7);
    CHECK(tv.stores[2].state == TIERS_STATE_REMOVED);
    const char *vpaths[1] = {dbdir};
    uint64_t left_on_disk = 999;
    CHECK(tierctl_count_shrink_objects(&tv, vpaths, 1, &left_on_disk) == 0 && left_on_disk == 0);
    tiers_view_free(&tv);
    close(dfd);

    tier_put(t);
    CHECK(tsdb_close(db) == 0);

    /* Final reopen opens 6 stores cleanly and passes 2-store-DOWN read check across all 8 parts. */
    CHECK(open_db(dbdir, &db) == 0 && db != NULL);
    t = tier_find_by_dir(dbdir);
    CHECK(t != NULL && tier_store_count(t) == 6);
    (void)tsdb_test_ec_retries(db, true);
    tier_store_set_down(t, 2, CHUNK_STORE_DOWN_STALLED);
    tier_store_set_down(t, 3, CHUNK_STORE_DOWN_STALLED);
    eccache_reopen_all(t);
    verify_rows(db, s0, s1, 8, 20000);
    tier_put(t);
    CHECK(tsdb_close(db) == 0);
    printf("  kill_mid_shrink: ok (killed mid-shrink, resumed on reopen, finished to REMOVED, 6 stores ok)\n");
}

/* ------------------------------------------------------------ 11. Damaged .ec part during shrink (no spin, salvage hint) */

static void t_shrink_damaged(void)
{
    char base[PATH_MAX], dbdir[PATH_MAX], stores[7][PATH_MAX], log[4096];
    fresh_dir("shrinkdmg", base, sizeof base);
    tsdb *db = NULL;
    setup_7store_db(base, dbdir, stores, &db);
    tsdb_sid s0 = 0, s1 = 0;
    populate_and_settle(db, 3, 10000, &s0, &s1);

    /* Mark the first live .ec part as damaged in memory. */
    eng *e0 = tsdb_test_engine(db, 0);
    CHECK(e0 != NULL);
    bool marked = false;
    pthread_mutex_lock(&e0->lock);
    for (size_t i = 0; i < e0->nparts; i++) {
        part *p = e0->parts[i];
        if (!p->dropped && atomic_load_explicit(&p->ec, memory_order_acquire) != NULL) {
            atomic_store_explicit(&p->damaged, true, memory_order_release);
            marked = true;
            break;
        }
    }
    pthread_mutex_unlock(&e0->lock);
    CHECK(marked);

    CHECK(sh("rm -rf '%s'", stores[2]) == 0);
    tier *t = tier_find_by_dir(dbdir);
    CHECK(t != NULL);

    cap_begin();
    CHECK(tsdb_tier_lost(db, "s2", NULL, 0) == 0);
    /* Wait for undamaged parts to re-encode and worker to back off on the damaged part (must NOT spin forever!). */
    int64_t dl = mono_ms() + 10000;
    bool saw_backed_off = false;
    while (mono_ms() < dl) {
        tierctl_progress prog;
        if (tierctl_get_progress(t, &prog) && !prog.shrink_active && prog.shrink_objects_done >= 2 &&
            prog.shrink_objects_left == 1) {
            saw_backed_off = true;
            break;
        }
        struct timespec nap = {0, 20 * 1000000L};
        nanosleep(&nap, NULL);
    }
    cap_end(log, sizeof log);

    CHECK(saw_backed_off);
    CHECK(strstr(log, "tier salvage") != NULL);

    /* TIERS MUST still say SHRINKING (never REMOVED while a damaged part still references the store!). */
    int dfd = open(dbdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    CHECK(dfd >= 0);
    tiers_view tv;
    CHECK(tiers_load(dfd, false, false, &tv) == 0 && tv.nstores == 7);
    CHECK(tv.stores[2].state == TIERS_STATE_SHRINKING);
    tiers_view_free(&tv);
    close(dfd);

    tier_put(t);
    CHECK(tsdb_close(db) == 0);
    printf("  shrink_damaged: ok (undamaged parts re-encoded, damaged part backed off with salvage hint, no spin)\n");
}

/* ------------------------------------------------------------ 12. Power loss (crashfs on dbdir) during shrink */

static int child_crashfs_shrink(const char *dbdir)
{
    tsdb *db = NULL;
    int rc = open_db(dbdir, &db);
    if (rc != 0 || !db)
        return 2;
    tier *t = tier_find_by_dir(dbdir);
    if (!t) {
        tsdb_close(db);
        return 3;
    }
    rc = tierctl_wait_idle(t, 20000);
    tier_put(t);
    tsdb_close(db);
    if (rc != 0)
        return 4;
    int dfd = open(dbdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd < 0)
        return 5;
    tiers_view tv;
    bool ok = tiers_load(dfd, false, false, &tv) == 0 && tv.nstores == 7 && tv.stores[2].state == TIERS_STATE_REMOVED;
    tiers_view_free(&tv);
    close(dfd);
    return ok ? 0 : 6;
}

static int run_crashfs_shrink(const char *dbdir, int warmup, int seed)
{
    pid_t pid = fork();
    if (pid == 0) {
        char asan[512], wbuf[16], sbuf[16];
        const char *prev = getenv("ASAN_OPTIONS");
        snprintf(asan, sizeof asan, "%s%sverify_asan_link_order=0:detect_leaks=0",
                 prev && *prev ? prev : "", prev && *prev ? ":" : "");
        setenv("ASAN_OPTIONS", asan, 1);
        snprintf(wbuf, sizeof wbuf, "%d", warmup);
        snprintf(sbuf, sizeof sbuf, "%d", seed);
        setenv("LD_PRELOAD", SHIM, 1);
        setenv("CRASHFS_ROOT", dbdir, 1);
        setenv("CRASHFS_PROB", "1", 1);
        setenv("CRASHFS_WARMUP", wbuf, 1);
        setenv("CRASHFS_AT_EXIT", "1", 1);
        setenv("CRASHFS_TEAR", "1", 1);
        setenv("CRASHFS_SEED", sbuf, 1);
        int null = open("/dev/null", O_WRONLY);
        if (null >= 0) {
            dup2(null, 1);
            dup2(null, 2);
            close(null);
        }
        if (mutant_name)
            execl(SELF, "test_tierctl", "--mutant", mutant_name, "--child", "crashfs-shrink", dbdir, (char *)NULL);
        else
            execl(SELF, "test_tierctl", "--child", "crashfs-shrink", dbdir, (char *)NULL);
        _exit(127);
    }
    int st = -1;
    if (pid < 0 || waitpid(pid, &st, 0) != pid || !WIFEXITED(st))
        return -1;
    return WEXITSTATUS(st);
}

static void t_shrink_crashfs(void)
{
    if (access(SHIM, R_OK) != 0) {
        if (getenv("TSDB_REQUIRE_CRASHFS")) {
            fprintf(stderr, "test_tierctl: %s missing\n", SHIM);
            CHECK(false);
        } else {
            printf("  shrink_crashfs: skipped (no libcrashfs.so)\n");
        }
        return;
    }
#if defined(__SANITIZE_THREAD__)
    printf("  shrink_crashfs: skipped (TSan)\n");
    return;
#endif
    char base[PATH_MAX], snap[PATH_MAX + 32], work[PATH_MAX + 32];
    char dbdir[PATH_MAX], stores[7][PATH_MAX];
    fresh_dir("shrinkcfs", base, sizeof base);
    snprintf(snap, sizeof snap, "%s/snap", base);
    snprintf(work, sizeof work, "%s/work", base);
    CHECK(sh("mkdir -p '%s' '%s'", snap, work) == 0);

    tsdb *db = NULL;
    setup_7store_db(work, dbdir, stores, &db);
    tsdb_sid s0 = 0, s1 = 0;
    populate_and_settle(db, 2, 10000, &s0, &s1);
    CHECK(tsdb_close(db) == 0);
    CHECK(sh("rm -rf '%s'", stores[2]) == 0);
    CHECK(tierctl_lost_dir(dbdir, "s2", NULL, NULL, 0, NULL, NULL) == 0);
    CHECK(sh("rm -rf '%s' && cp -a '%s' '%s'", snap, work, snap) == 0);

    int cuts = 0, finished = 0;
    for (int w = 0; w < 240; w += (w < 16 ? 1 : 6)) {
        CHECK(sh("rm -rf '%s' && cp -a '%s' '%s'", work, snap, work) == 0);
        int rc = run_crashfs_shrink(dbdir, w, 900 + w);
        if (rc == 99) {
            cuts++;
            int dfd = open(dbdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
            CHECK(dfd >= 0);
            tiers_view tv;
            CHECK(tiers_load(dfd, true, true, &tv) == 0 && tv.nstores == 7);
            bool already_removed = (tv.stores[2].state == TIERS_STATE_REMOVED);
            if (already_removed) {
                /* If STATE REMOVED was durable in TIERS before the cut, EVERY live .ec swap MUST already be durable! */
                const char *vpaths[1] = {dbdir};
                uint64_t left = 999;
                CHECK(tierctl_count_shrink_objects(&tv, vpaths, 1, &left) == 0 && left == 0);
            }
            tiers_view_free(&tv);
            close(dfd);

            /* Reopen cleanly: must finish shrink (or already be REMOVED), then reopen with 6 stores & verify reads. */
            CHECK(open_db(dbdir, &db) == 0 && db != NULL);
            tier *t = tier_find_by_dir(dbdir);
            CHECK(t != NULL);
            CHECK(tierctl_wait_idle(t, 20000) == 0);
            tier_put(t);
            CHECK(tsdb_close(db) == 0);

            CHECK(open_db(dbdir, &db) == 0 && db != NULL);
            t = tier_find_by_dir(dbdir);
            CHECK(t != NULL && tier_store_count(t) == 6);
            (void)tsdb_test_ec_retries(db, true);
            tier_store_set_down(t, 0, CHUNK_STORE_DOWN_STALLED);
            tier_store_set_down(t, 1, CHUNK_STORE_DOWN_STALLED);
            eccache_reopen_all(t);
            verify_rows(db, s0, s1, 2, 10000);
            tier_put(t);
            CHECK(tsdb_close(db) == 0);
        } else if (rc == 0) {
            finished = 1;
            int dfd = open(dbdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
            CHECK(dfd >= 0);
            tiers_view tv;
            CHECK(tiers_load(dfd, false, false, &tv) == 0 && tv.nstores == 7 &&
                  tv.stores[2].state == TIERS_STATE_REMOVED);
            const char *vpaths[1] = {dbdir};
            uint64_t left = 999;
            CHECK(tierctl_count_shrink_objects(&tv, vpaths, 1, &left) == 0 && left == 0);
            tiers_view_free(&tv);
            close(dfd);
            break;
        } else {
            fprintf(stderr, "test_tierctl: crashfs-shrink warmup %d returned unexpected status %d\n", w, rc);
            CHECK(false);
            break;
        }
    }
    CHECK(cuts >= 3 && finished == 1);
    printf("  shrink_crashfs: ok (%d power-cut points across .ec swap & TIERS REMOVED, all recovered cleanly)\n", cuts);
}

/* ------------------------------------------------------------ 13. Salvage during shrink of s0 keeps healthy parts */

static void t_shrink_salvage_s0(void)
{
    char base[PATH_MAX], dbdir[PATH_MAX], stores[7][PATH_MAX];
    fresh_dir("shrinksalv", base, sizeof base);
    tsdb *db = NULL;
    setup_7store_db(base, dbdir, stores, &db);
    tsdb_sid s0 = 0, s1 = 0;
    populate_and_settle(db, 4, 10000, &s0, &s1);

    /* Mark partition 0's .ec part damaged so shrink re-encodes partitions 1..3 onto the 6 remaining stores
     * while store 0 remains in TIERS_STATE_SHRINKING (LOST). */
    eng *e0 = tsdb_test_engine(db, 0);
    CHECK(e0 != NULL);
    pthread_mutex_lock(&e0->lock);
    for (size_t i = 0; i < e0->nparts; i++) {
        part *p = e0->parts[i];
        if (!p->dropped && p->partition == 0 && atomic_load_explicit(&p->ec, memory_order_acquire) != NULL) {
            atomic_store_explicit(&p->damaged, true, memory_order_release);
            break;
        }
    }
    pthread_mutex_unlock(&e0->lock);

    CHECK(sh("rm -rf '%s'", stores[0]) == 0);
    tier *t = tier_find_by_dir(dbdir);
    CHECK(t != NULL);
    CHECK(tsdb_tier_lost(db, "s0", NULL, 0) == 0);

    int64_t dl = mono_ms() + 10000;
    bool ready = false;
    while (mono_ms() < dl) {
        tierctl_progress prog;
        if (tierctl_get_progress(t, &prog) && !prog.shrink_active && prog.shrink_objects_done >= 3 &&
            prog.shrink_objects_left == 1) {
            ready = true;
            break;
        }
        struct timespec nap = {0, 20 * 1000000L};
        nanosleep(&nap, NULL);
    }
    CHECK(ready);

    /* Run tier salvage while s0 is SHRINKING (LOST) and parts 1..3 are on active-store placement (-1).
     * Salvage MUST NOT retire healthy re-encoded parts 1..3! */
    CHECK(tsdb_tier_salvage(db, 0, 1, 0) == 0);

    const int64_t day_ms = 86400000LL;
    int64_t *ts = malloc(10000 * sizeof *ts);
    double *va = malloc(10000 * sizeof *va);
    CHECK(ts && va);
    for (int p = 1; p < 4; p++) {
        size_t n = 0;
        CHECK(query_f64(db, s0, (int64_t)p * day_ms, (int64_t)(p + 1) * day_ms, ts, va, 10000, &n) == 0);
        CHECK(n == 10000);
        for (int r = 0; r < 10000; r++) {
            if (va[r] != det_val(p, r, 0)) {
                CHECK(false);
                break;
            }
        }
    }
    free(ts);
    free(va);
    tier_put(t);
    CHECK(tsdb_close(db) == 0);
    printf("  shrink_salvage_s0: ok (salvage during s0 shrink retired 0 healthy re-encoded parts)\n");
}

/* ------------------------------------------------------------ 14. eng_halt waits for in-flight shrink re-encode */

static atomic_bool g_halt_entered;
static atomic_bool g_halt_finished;
static atomic_bool g_halt_was_shrink;
static tier *g_halt_tier;

static void shrink_halt_fill_hook(eng *db, const part *p, uint64_t bytes)
{
    (void)db, (void)p, (void)bytes;
    if (!atomic_exchange(&g_halt_entered, true)) {
        tierctl_progress prog;
        if (g_halt_tier && tierctl_get_progress(g_halt_tier, &prog) && prog.shrink_active)
            atomic_store(&g_halt_was_shrink, true);
        struct timespec nap = {0, 200 * 1000000L}; /* 200 ms hold inside eng_reencode -> settle_one */
        nanosleep(&nap, NULL);
        atomic_store(&g_halt_finished, true);
    }
}

static void t_shrink_eng_halt(void)
{
    char base[PATH_MAX], dbdir[PATH_MAX], stores[7][PATH_MAX];
    fresh_dir("shrinkhalt", base, sizeof base);
    tsdb *db = NULL;
    setup_7store_db(base, dbdir, stores, &db);
    tsdb_sid s0 = 0, s1 = 0;
    populate_and_settle(db, 4, 10000, &s0, &s1);

    eng *e0 = tsdb_test_engine(db, 0);
    tier *t = tier_find_by_dir(dbdir);
    CHECK(e0 != NULL && t != NULL);
    g_halt_tier = t;
    atomic_store(&g_halt_entered, false);
    atomic_store(&g_halt_finished, false);
    atomic_store(&g_halt_was_shrink, false);
    atomic_store(&settle_fill_hook, shrink_halt_fill_hook);

    CHECK(sh("rm -rf '%s'", stores[2]) == 0);
    CHECK(tsdb_tier_lost(db, "s2", NULL, 0) == 0);

    int64_t dl = mono_ms() + 10000;
    while (mono_ms() < dl && !atomic_load(&g_halt_entered)) {
        struct timespec nap = {0, 5 * 1000000L};
        nanosleep(&nap, NULL);
    }
    CHECK(atomic_load(&g_halt_entered));

    /* eng_halt(e0) MUST wait for the in-flight shrink eng_reencode to finish before returning! */
    eng_halt(e0);
    CHECK(atomic_load(&g_halt_was_shrink));
    CHECK(atomic_load(&g_halt_finished));
    atomic_store(&settle_fill_hook, NULL);
    g_halt_tier = NULL;

    tier_put(t);
    CHECK(tsdb_close(db) == 0);
    printf("  shrink_eng_halt: ok (eng_halt waited for in-flight shrink re-encode before returning)\n");
}

#endif /* TSDB_EC */

int main(int argc, char **argv)
{
    const char *scratch = "/tmp";
    const char *child_mode = NULL;
    const char *child_arg1 = NULL;
    const char *child_arg2 = NULL;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mutant") == 0 && i + 1 < argc) {
            mutant_name = argv[++i];
#ifdef TSDB_TEST_HOOKS
            if (strcmp(mutant_name, "lostnoreplace") == 0)
                tierctl_mutant = TIERCTL_MUT_LOSTNOREPLACE;
            else if (strcmp(mutant_name, "rebuildfast") == 0)
                tierctl_mutant = TIERCTL_MUT_REBUILDFAST;
            else if (strcmp(mutant_name, "shrinkearly") == 0)
                tierctl_mutant = TIERCTL_MUT_SHRINKEARLY;
            else {
                fprintf(stderr, "test_tierctl: unknown mutant '%s'\n", mutant_name);
                return 2;
            }
#endif
        } else if (strcmp(argv[i], "--child") == 0 && i + 2 < argc) {
            child_mode = argv[++i];
            child_arg1 = argv[++i];
            if (i + 1 < argc)
                child_arg2 = argv[++i];
        } else if (argv[i][0] != '-') {
            scratch = argv[i];
        }
    }

    if (!realpath(argv[0], SELF))
        snprintf(SELF, sizeof SELF, "%s", argv[0]);
    char bindir[PATH_MAX];
    snprintf(bindir, sizeof bindir, "%s", SELF);
    char *sl = strrchr(bindir, '/');
    if (sl)
        *sl = '\0';
    else
        snprintf(bindir, sizeof bindir, ".");
    snprintf(SHIM, sizeof SHIM, "%s/libcrashfs.so", bindir);
    snprintf(CLI, sizeof CLI, "%s/tsdb", bindir);

#ifndef TSDB_EC
    (void)scratch, (void)child_mode, (void)child_arg1, (void)child_arg2;
    printf("test_tierctl: skipped (NO_EC=1)\n");
    return 0;
#else
    if (child_mode) {
        if (strcmp(child_mode, "kill-rebuild") == 0 && child_arg1 && child_arg2)
            return child_kill_rebuild(child_arg1, child_arg2);
        if (strcmp(child_mode, "kill-shrink") == 0 && child_arg1 && child_arg2)
            return child_kill_shrink(child_arg1, child_arg2);
        if (strcmp(child_mode, "crashfs-shrink") == 0 && child_arg1)
            return child_crashfs_shrink(child_arg1);
        if (strcmp(child_mode, "crashfs-rebuild") == 0 && child_arg1)
            return child_crashfs_rebuild(child_arg1);
        if (strcmp(child_mode, "crashfs-lost") == 0 && child_arg1 && child_arg2)
            return child_crashfs_lost_cmd(child_arg1, child_arg2);
        return 2;
    }

    snprintf(S, sizeof S, "%s/tsdb-test-tierctl-%ld", scratch, (long)getpid());
    if (sh("rm -rf '%s' && mkdir -p '%s'", S, S) != 0)
        return 2;

    /* Fast probes/disable probes where appropriate; tier_health probes stay off under crashfs automatically. */
    tier_health_test_opts(0, -1, 0, 0);

    printf("test_tierctl%s%s:\n", mutant_name ? " --mutant " : "", mutant_name ? mutant_name : "");
    t_replace_4plus2();
    t_kill_mid_rebuild();
    t_power_loss_crashfs();
    t_lost_cmd_crashfs();
    t_pace_50_mb_s();
    t_live_cli_pickup();
    t_e15_lost_vs_down();
    t_add_then_lost();
    t_two_lost_replace_one();
    t_shrink_7to6();
    t_kill_mid_shrink();
    t_shrink_damaged();
    t_shrink_crashfs();
    t_shrink_salvage_s0();
    t_shrink_eng_halt();

    (void)sh("rm -rf '%s'", S);
    if (fails) {
        fprintf(stderr, "test_tierctl: %d failure(s)\n", fails);
        return 1;
    }
    printf("test_tierctl: ok\n");
    return 0;
#endif
}
