/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Local tier background chunk scrubber and repair (src/tierscrub.h; R15 r15-scrub; docs/design-cluster-data.md
 * E25, §15.7; PLAN.md D12, D20).
 */
#define _GNU_SOURCE
#include "tierscrub.h"

#include "chunk.h"
#include "chunk_priv.h"
#include "compat.h"
#include "eccache.h"
#include "ecpart.h"
#include "internal.h"
#include "tier.h"
#include "tierctl.h"
#include "tiers.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#ifdef TSDB_TEST_HOOKS
int tierscrub_mutant;

int tierscrub_mutant_parse(const char *name)
{
    if (!name || strcmp(name, "none") == 0)
        return TIERSCRUB_MUT_NONE;
    if (strcmp(name, "scrubnorepair") == 0)
        return TIERSCRUB_MUT_NOREPAIR;
    if (strcmp(name, "scrubnopace") == 0)
        return TIERSCRUB_MUT_NOPACE;
    return -1;
}

static inline int tierscrub_mut(void) { return tierscrub_mutant; }

static pthread_mutex_t g_test_mu = PTHREAD_MUTEX_INITIALIZER;
static uint32_t g_test_interval_ms;
static uint64_t g_test_rate_bps;
static uint32_t g_test_save_every;
static bool g_test_opts_set;
static uint64_t (*g_test_wall_fn)(void *ctx);
static void *g_test_wall_ctx;
static tierscrub_obj_hook_fn g_test_obj_hook;
static void *g_test_obj_hook_ctx;
static atomic_bool g_test_pause;
#else
int tierscrub_mutant;
int tierscrub_mutant_parse(const char *name)
{
    (void)name;
    return -1;
}
static inline int tierscrub_mut(void) { return 0; }
#endif

void tierscrub_set_test_opts(uint32_t interval_ms, uint64_t rate_bps, uint32_t save_every)
{
#ifdef TSDB_TEST_HOOKS
    pthread_mutex_lock(&g_test_mu);
    g_test_interval_ms = interval_ms;
    g_test_rate_bps = rate_bps;
    g_test_save_every = save_every;
    g_test_opts_set = (interval_ms > 0 || rate_bps > 0 || save_every > 0);
    pthread_mutex_unlock(&g_test_mu);
#else
    (void)interval_ms, (void)rate_bps, (void)save_every;
#endif
}

void tierscrub_set_wall_fn(uint64_t (*fn)(void *ctx), void *ctx)
{
#ifdef TSDB_TEST_HOOKS
    pthread_mutex_lock(&g_test_mu);
    g_test_wall_fn = fn;
    g_test_wall_ctx = ctx;
    pthread_mutex_unlock(&g_test_mu);
#else
    (void)fn, (void)ctx;
#endif
}

void tierscrub_set_obj_hook(tierscrub_obj_hook_fn fn, void *ctx)
{
#ifdef TSDB_TEST_HOOKS
    pthread_mutex_lock(&g_test_mu);
    g_test_obj_hook = fn;
    g_test_obj_hook_ctx = ctx;
    pthread_mutex_unlock(&g_test_mu);
#else
    (void)fn, (void)ctx;
#endif
}

void tierscrub_set_test_pause(bool paused)
{
#ifdef TSDB_TEST_HOOKS
    atomic_store_explicit(&g_test_pause, paused, memory_order_release);
#else
    (void)paused;
#endif
}

static uint64_t scrub_wall_s(void)
{
#ifdef TSDB_TEST_HOOKS
    pthread_mutex_lock(&g_test_mu);
    uint64_t (*fn)(void *) = g_test_wall_fn;
    void *ctx = g_test_wall_ctx;
    pthread_mutex_unlock(&g_test_mu);
    if (fn)
        return fn(ctx);
#endif
    time_t t = time(NULL);
    return t > 0 ? (uint64_t)t : 0;
}

static int hex_val(int c)
{
    return c >= '0' && c <= '9' ? c - '0' : c >= 'a' && c <= 'f' ? c - 'a' + 10 : -1;
}

static bool hex_parse(const char *s, size_t nchars, uint8_t *out)
{
    for (size_t i = 0; i < nchars; i += 2) {
        int a = hex_val(s[i]), b = hex_val(s[i + 1]);
        if (a < 0 || b < 0)
            return false;
        out[i / 2] = (uint8_t)((a << 4) | b);
    }
    return true;
}

static int scrub_format_text(char *out, size_t cap, const tierscrub_store_stats *st)
{
    char oid_hex[33];
    chunk_oid_hex(st->cursor_oid, oid_hex);
    char body[640];
    int bl = snprintf(body, sizeof body,
                      "tsdb-tier-scrub 1\n"
                      "pass_start_s %" PRIu64 "\n"
                      "last_pass_s %" PRIu64 "\n"
                      "first_pass_s %" PRIu64 "\n"
                      "cursor_valid %u\n"
                      "cursor_vol %" PRIu32 "\n"
                      "cursor_oid %s\n"
                      "chunks_checked %" PRIu64 "\n"
                      "damaged %" PRIu64 "\n"
                      "repaired %" PRIu64 "\n"
                      "unrepairable %" PRIu64 "\n"
                      "bytes_checked %" PRIu64 "\n"
                      "passes_done %" PRIu64 "\n",
                      st->pass_start_s, st->last_pass_s, st->first_pass_s, st->cursor_valid ? 1u : 0u, st->cursor_vol,
                      oid_hex, st->chunks_checked, st->damaged, st->repaired, st->unrepairable, st->bytes_checked,
                      st->passes_done);
    if (bl <= 0 || (size_t)bl >= sizeof body)
        return -1;
    uint64_t h = xxh64(body, (size_t)bl, 0);
    int fl = snprintf(out, cap, "%ssum=%016" PRIx64 "\n", body, h);
    if (fl <= 0 || (size_t)fl >= cap)
        return -1;
    return fl;
}

static int scrub_parse_text(const char *buf, size_t got, tierscrub_store_stats *out)
{
    uint64_t ps = 0, lp = 0, fp = 0;
    uint32_t cv = 0, cvol = 0;
    char oid_hex[40] = "";
    uint64_t checked = 0, damaged = 0, repaired = 0, unrep = 0, bytes = 0, passes = 0;
    uint8_t oid[16] = {0};

    if (sscanf(buf,
               "tsdb-tier-scrub 1\n"
               "pass_start_s %" SCNu64 "\n"
               "last_pass_s %" SCNu64 "\n"
               "first_pass_s %" SCNu64 "\n"
               "cursor_valid %" SCNu32 "\n"
               "cursor_vol %" SCNu32 "\n"
               "cursor_oid %39s\n"
               "chunks_checked %" SCNu64 "\n"
               "damaged %" SCNu64 "\n"
               "repaired %" SCNu64 "\n"
               "unrepairable %" SCNu64 "\n"
               "bytes_checked %" SCNu64 "\n"
               "passes_done %" SCNu64 "\n",
               &ps, &lp, &fp, &cv, &cvol, oid_hex, &checked, &damaged, &repaired, &unrep, &bytes, &passes) != 12 ||
        cv > 1 || strlen(oid_hex) != 32 || !hex_parse(oid_hex, 32, oid))
        return TSDB_ERR_CORRUPT;

    tierscrub_store_stats tmp;
    memset(&tmp, 0, sizeof tmp);
    tmp.store = -1;
    tmp.pass_start_s = ps;
    tmp.last_pass_s = lp;
    tmp.first_pass_s = fp;
    tmp.cursor_valid = cv != 0;
    tmp.cursor_vol = cvol;
    memcpy(tmp.cursor_oid, oid, 16);
    tmp.chunks_checked = checked;
    tmp.damaged = damaged;
    tmp.repaired = repaired;
    tmp.unrepairable = unrep;
    tmp.bytes_checked = bytes;
    tmp.passes_done = passes;
    tmp.has_data = true;

    char want[768];
    int wlen = scrub_format_text(want, sizeof want, &tmp);
    if (wlen <= 0 || (size_t)wlen != got || memcmp(want, buf, got) != 0)
        return TSDB_ERR_CORRUPT;

    uint64_t now_s = scrub_wall_s();
    uint64_t ref_s = tmp.last_pass_s ? tmp.last_pass_s : tmp.first_pass_s;
    tmp.oldest_age_s = (ref_s > 0 && now_s > ref_s) ? now_s - ref_s : 0;

    *out = tmp;
    return 0;
}

int tierscrub_read_store_file(const char *store_path, tierscrub_store_stats *out)
{
    if (!store_path || !*store_path || !out)
        return TSDB_ERR_INVALID;
    memset(out, 0, sizeof *out);
    out->store = -1;

    int dirfd = open(store_path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (dirfd < 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;
    int fd = openat(dirfd, TIERSCRUB_FILE, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    close(dirfd);
    if (fd < 0)
        return errno == ENOENT ? TSDB_ERR_NOTFOUND : TSDB_ERR_IO;

    char buf[1024];
    size_t got = 0;
    int rc = 0;
    for (;;) {
        ssize_t r = read(fd, buf + got, sizeof buf - 1 - got);
        if (r < 0 && errno == EINTR)
            continue;
        if (r < 0) {
            rc = TSDB_ERR_IO;
            break;
        }
        if (r == 0)
            break;
        got += (size_t)r;
        if (got >= sizeof buf - 1) {
            rc = TSDB_ERR_CORRUPT;
            break;
        }
    }
    close(fd);
    if (rc != 0)
        return rc;
    if (got == 0)
        return TSDB_ERR_CORRUPT;
    buf[got] = '\0';
    return scrub_parse_text(buf, got, out);
}

#ifdef TSDB_EC

#define TIERSCRUB_MAX_RATE_BPS (10ull << 30)
#define TIERSCRUB_BURST_NS 20000000ull            /* 20 ms virtual-time token-bucket burst ceiling */
#define TIERSCRUB_EMPTY_POLL_MS 2000u             /* initial idle re-check for a store with 0 live chunks */
#define TIERSCRUB_EMPTY_POLL_MAX_MS 3600000u      /* 1 h backoff ceiling for persistently empty stores */
#define TIERSCRUB_INCOMP_MIN_MS 200u              /* initial retry backoff when a volume is opening or quiesced */
#define TIERSCRUB_INCOMP_MAX_MS 60000u            /* 60 s backoff ceiling when only unready volumes remain */
#define TIERSCRUB_SAVE_EVERY_DEFAULT 0u           /* time-based checkpointing (15 s) by default to avoid HDD sync storm */
#define TIERSCRUB_SAVE_INTERVAL_NS 15000000000ull /* 15 s time cap between SCRUB checkpoints */

typedef struct tscrub_store {
    bool loaded;
    tierscrub_store_stats st;
    int64_t last_pass_mono_ms;
    int64_t retry_after_ms;
    int fails;
    int empty_streak;
    /* In-memory set of volumes already completed in the current pass: prevents re-listing finished volumes when
     * another volume is quiesced or opening (cleared on pass completion, store change, or close). */
    uint32_t *done_vols;
    size_t ndone_vols;
    size_t cap_done_vols;
} tscrub_store;

static bool store_done_has(const tscrub_store *ss, uint32_t vol)
{
    for (size_t i = 0; i < ss->ndone_vols; i++)
        if (ss->done_vols[i] == vol)
            return true;
    return false;
}

static void store_done_add(tscrub_store *ss, uint32_t vol)
{
    if (store_done_has(ss, vol))
        return;
    if (ss->ndone_vols == ss->cap_done_vols) {
        size_t nc = ss->cap_done_vols ? ss->cap_done_vols * 2 : 8;
        uint32_t *nv = realloc(ss->done_vols, nc * sizeof *nv);
        if (!nv)
            return;
        ss->done_vols = nv;
        ss->cap_done_vols = nc;
    }
    ss->done_vols[ss->ndone_vols++] = vol;
}

static void store_done_clear(tscrub_store *ss)
{
    free(ss->done_vols);
    ss->done_vols = NULL;
    ss->ndone_vols = 0;
    ss->cap_done_vols = 0;
}

static bool oid_is_zero(const uint8_t oid[16])
{
    uint8_t acc = 0;
    for (int i = 0; i < 16; i++)
        acc |= oid[i];
    return acc == 0;
}

struct tierscrub {
    tier *t;
    pthread_mutex_t mu; /* pure leaf lock: NEVER held across filesystem or chunk I/O (/metrics takes this lock) */
    pthread_cond_t pass_cv;

    bool disabled;
    uint32_t env_interval_ms;
    uint64_t env_rate_bps;
    uint32_t env_save_every;

    bool active;
    bool paused;
    int active_store;
    int nstores;
    uint32_t incomp_backoff_ms;
    int64_t incomp_retry_ms;
    tscrub_store stores[TIERS_STORES_MAX];
};

static int64_t scrub_mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static uint64_t scrub_mono_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static int wr_all(int fd, const void *buf, size_t n)
{
    const uint8_t *p = buf;
    size_t done = 0;
    while (done < n) {
        ssize_t w = write(fd, p + done, n - done);
        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0)
            return -1;
        done += (size_t)w;
    }
    return 0;
}

/* Atomically writes `<store_path>/SCRUB` via SCRUB.tmp -> fsync -> rename -> dir fsync on an independent dirfd so it
 * never touches or consumes writeback errors on chunk_store's own dirfd. Advisory: failure logs and continues. */
static void scrub_save_store(const char *store_path, int store_idx, const tierscrub_store_stats *snap)
{
    if (!store_path || !*store_path)
        return;
    char text[768];
    int len = scrub_format_text(text, sizeof text, snap);
    if (len <= 0)
        return;

    int dirfd = open(store_path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (dirfd < 0)
        return;

    (void)unlinkat(dirfd, TIERSCRUB_FILE ".tmp", 0);
    int fd = openat(dirfd, TIERSCRUB_FILE ".tmp", O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0644);
    int rc = fd < 0 ? -1 : 0;
    if (fd >= 0) {
        if (wr_all(fd, text, (size_t)len) != 0 || tsdb_fsync(fd) != 0)
            rc = -1;
        if (close(fd) != 0)
            rc = -1;
    }
    if (rc == 0 && renameat(dirfd, TIERSCRUB_FILE ".tmp", dirfd, TIERSCRUB_FILE) != 0)
        rc = -1;
    if (rc != 0)
        (void)unlinkat(dirfd, TIERSCRUB_FILE ".tmp", 0);
    if (rc == 0 && tsdb_fsync(dirfd) != 0)
        rc = -1;
    close(dirfd);
    if (rc != 0)
        fprintf(stderr, "tsdb: tier scrub store %d (%s/%s): could not save progress (continuing)\n", store_idx,
                store_path, TIERSCRUB_FILE);
}

/* Reads `<path>/SCRUB` WITHOUT holding sc->mu (zero I/O under lock) and fills `out`.
 * Leaves out->loaded = false when `path` is NULL/empty so a store that was DOWN at open loads its persisted
 * SCRUB history once it comes UP instead of overwriting it with zeros. */
static void scrub_load_from_disk(int idx, const char *path, tscrub_store *out)
{
    memset(out, 0, sizeof *out);
    out->st.store = idx;

    uint64_t now_s = scrub_wall_s();
    out->st.first_pass_s = now_s;
    out->st.pass_start_s = 0;
    out->st.last_pass_s = 0;

    if (!path || !*path) {
        out->loaded = false;
        return;
    }
    out->loaded = true;

    tierscrub_store_stats loaded;
    int rc = tierscrub_read_store_file(path, &loaded);
    if (rc == 0) {
        loaded.store = idx;
        if (!loaded.first_pass_s)
            loaded.first_pass_s = loaded.last_pass_s ? loaded.last_pass_s : now_s;
        out->st = loaded;
        if (loaded.last_pass_s > 0 && !loaded.cursor_valid)
            out->last_pass_mono_ms = scrub_mono_ms();
    } else if (rc != TSDB_ERR_NOTFOUND) {
        fprintf(stderr, "tsdb: tier scrub store %d (%s/%s): damaged or unreadable progress file (%s); starting fresh\n",
                idx, path, TIERSCRUB_FILE, tsdb_strerror(rc));
    }
}

static void scrub_load_env(struct tierscrub *sc)
{
#ifdef TSDB_TEST_HOOKS
    bool explicit_enable = false;
    const char *env = getenv("TSDB_TEST_TIER_SCRUB");
    if (env && *env) {
        char *copy = strdup(env), *save = NULL;
        if (copy) {
            for (char *kv = strtok_r(copy, ",", &save); kv; kv = strtok_r(NULL, ",", &save)) {
                char *eq = strchr(kv, '=');
                if (!eq)
                    continue;
                *eq = '\0';
                uint64_t v = strtoull(eq + 1, NULL, 10);
                if (strcmp(kv, "disable") == 0) {
                    sc->disabled = (v != 0);
                } else if (strcmp(kv, "enable") == 0) {
                    explicit_enable = (v != 0);
                    if (explicit_enable)
                        sc->disabled = false;
                } else if (strcmp(kv, "interval_ms") == 0 && v > 0) {
                    sc->env_interval_ms = v > UINT32_MAX ? UINT32_MAX : (uint32_t)v;
                    explicit_enable = true;
                } else if (strcmp(kv, "rate_bps") == 0 && v > 0) {
                    sc->env_rate_bps = v;
                    explicit_enable = true;
                } else if (strcmp(kv, "mb_s") == 0 && v > 0) {
                    sc->env_rate_bps = v * (1024ull * 1024ull);
                    explicit_enable = true;
                } else if (strcmp(kv, "save_every") == 0 && v > 0) {
                    sc->env_save_every = v > UINT32_MAX ? UINT32_MAX : (uint32_t)v;
                }
            }
            free(copy);
        }
    }
    pthread_mutex_lock(&g_test_mu);
    bool opts_set = g_test_opts_set;
    pthread_mutex_unlock(&g_test_mu);
    /* Under libcrashfs.so fault injection or legacy non-TIERS TSDB_TEST_TIER harnesses (which deliberately corrupt
     * chunks to test read retries/obj_closes), keep background scrub opt-in unless explicitly enabled. */
    if (!explicit_enable && !opts_set && (getenv("CRASHFS_ROOT") != NULL || (sc->t && !sc->t->from_tiers)))
        sc->disabled = true;
#else
    if (sc->t && !sc->t->from_tiers)
        sc->disabled = true;
#endif
}

static uint32_t effective_interval_ms(const struct tierscrub *sc)
{
#ifdef TSDB_TEST_HOOKS
    pthread_mutex_lock(&g_test_mu);
    uint32_t iv = g_test_interval_ms;
    pthread_mutex_unlock(&g_test_mu);
    if (iv > 0)
        return iv;
    return sc->env_interval_ms;
#else
    (void)sc;
    return 0;
#endif
}

static uint64_t effective_rate_bps(const struct tierscrub *sc, uint32_t mb_s)
{
    uint64_t ov = 0;
#ifdef TSDB_TEST_HOOKS
    pthread_mutex_lock(&g_test_mu);
    ov = g_test_rate_bps;
    pthread_mutex_unlock(&g_test_mu);
    if (ov == 0)
        ov = sc->env_rate_bps;
#else
    (void)sc;
#endif
    uint64_t r = ov > 0 ? ov : (uint64_t)(mb_s ? mb_s : TIERS_SCRUB_MB_S_DEFAULT) * (1024ull * 1024ull);
    return r > TIERSCRUB_MAX_RATE_BPS ? TIERSCRUB_MAX_RATE_BPS : r;
}

static uint32_t effective_save_every(const struct tierscrub *sc)
{
#ifdef TSDB_TEST_HOOKS
    pthread_mutex_lock(&g_test_mu);
    uint32_t se = g_test_save_every;
    pthread_mutex_unlock(&g_test_mu);
    if (se > 0)
        return se;
    if (sc->env_save_every > 0)
        return sc->env_save_every;
#else
    (void)sc;
#endif
    return TIERSCRUB_SAVE_EVERY_DEFAULT;
}

static bool scrub_is_paused_test(void)
{
#ifdef TSDB_TEST_HOOKS
    return atomic_load_explicit(&g_test_pause, memory_order_acquire);
#else
    return false;
#endif
}

/* ---------------------------------------------------------------- live .ec object metadata snapshot */

typedef struct live_ec_ent {
    uint32_t vol;
    uint8_t oid[16];
    int64_t partition;
    char name[96];
} live_ec_ent;

typedef struct live_ec_map {
    live_ec_ent *v;
    size_t n, cap;
    uint32_t *ready_vols;
    size_t nready;
} live_ec_map;

static void live_ec_map_free(live_ec_map *m)
{
    free(m->v);
    free(m->ready_vols);
    memset(m, 0, sizeof *m);
}

static bool live_ec_vol_ready(const live_ec_map *m, uint32_t vol)
{
    for (size_t i = 0; i < m->nready; i++)
        if (m->ready_vols[i] == vol)
            return true;
    return false;
}

static int live_ec_cmp(const void *a, const void *b)
{
    const live_ec_ent *x = a, *y = b;
    if (x->vol != y->vol)
        return (x->vol > y->vol) - (x->vol < y->vol);
    return memcmp(x->oid, y->oid, 16);
}

static const live_ec_ent *live_ec_find(const live_ec_map *m, uint32_t vol, const uint8_t oid[16])
{
    if (!m || m->n == 0)
        return NULL;
    live_ec_ent key;
    key.vol = vol;
    memcpy(key.oid, oid, 16);
    return bsearch(&key, m->v, m->n, sizeof *m->v, live_ec_cmp);
}

static bool eng_is_closing(const eng *db)
{
    return atomic_load_explicit(&db->stop, memory_order_acquire) ||
           atomic_load_explicit(&db->quiesced, memory_order_acquire);
}

/* Collects live .ec records per ready volume engine of tier `t` (zero engine locks held across chunk I/O).
 * Each open, non-closing, non-settling volume is recorded in out->ready_vols[] so an unrelated quiesced or settling
 * volume does not stall scrub on healthy volumes; *all_complete_out is true only when every attached/backend volume
 * was ready (required to mark a whole-store pass finished). */
static int collect_live_ec_map(tier *t, live_ec_map *out, bool *all_complete_out)
{
    memset(out, 0, sizeof *out);
    *all_complete_out = false;
    if (!t || !t->be)
        return 0;

    pthread_mutex_lock(&t->cmu);
    bool any_opening = t->eng_opening > 0 || t->nengs < t->ncvs;
    size_t n = t->nengs;
    eng **copy = n ? malloc(n * sizeof *copy) : NULL;
    if (n && copy) {
        memcpy(copy, t->engs, n * sizeof *copy);
        t->eng_active++;
    }
    pthread_mutex_unlock(&t->cmu);

    if (n == 0 || !copy) {
        free(copy);
        return n > 0 ? TSDB_ERR_NOMEM : 0;
    }

    out->ready_vols = malloc(n * sizeof *out->ready_vols);
    if (!out->ready_vols) {
        free(copy);
        pthread_mutex_lock(&t->cmu);
        if (--t->eng_active == 0)
            pthread_cond_broadcast(&t->eng_cv);
        pthread_mutex_unlock(&t->cmu);
        return TSDB_ERR_NOMEM;
    }

    bool all_ok = !any_opening;
    int rc = 0;
    for (size_t ei = 0; ei < n && rc == 0; ei++) {
        eng *db = copy[ei];
        if (eng_is_closing(db)) {
            all_ok = false;
            continue;
        }
        pthread_mutex_lock(&db->lock);
        for (size_t pi = 0; pi < db->nparts; pi++) {
            part *p = db->parts[pi];
            /* A part that is dropped or still mid-settle is not a committed .ec part yet; the live callback already
             * skips uncommitted objects so settling parts never block scrubbing committed objects in this volume. */
            if (p->dropped || p->settling)
                continue;
            struct ecpart *ep = atomic_load_explicit(&p->ec, memory_order_acquire);
            if (!ep || ep->test_mark)
                continue;
            if (out->n == out->cap) {
                size_t nc = out->cap ? out->cap * 2 : 64;
                live_ec_ent *nv = realloc(out->v, nc * sizeof *nv);
                if (!nv) {
                    rc = TSDB_ERR_NOMEM;
                    break;
                }
                out->v = nv;
                out->cap = nc;
            }
            live_ec_ent *e = &out->v[out->n++];
            e->vol = db->tier_vol;
            memcpy(e->oid, ep->rec.oid, 16);
            e->partition = p->partition;
            snprintf(e->name, sizeof e->name, "%s", p->name);
        }
        pthread_mutex_unlock(&db->lock);
        if (rc == 0)
            out->ready_vols[out->nready++] = db->tier_vol;
    }

    free(copy);
    pthread_mutex_lock(&t->cmu);
    if (--t->eng_active == 0)
        pthread_cond_broadcast(&t->eng_cv);
    pthread_mutex_unlock(&t->cmu);

    if (rc != 0) {
        live_ec_map_free(out);
        return rc;
    }
    if (out->n > 1)
        qsort(out->v, out->n, sizeof *out->v, live_ec_cmp);
    *all_complete_out = all_ok;
    return 0;
}

/* Re-checks live status directly against the registered engine (pure memory lookup, zero I/O, no chunk locks held)
 * so a part dropped concurrently during a scrub pass is never repaired. */
static bool eng_still_has_live_oid(tier *t, uint32_t vol, const uint8_t oid[16])
{
    bool live = false;
    pthread_mutex_lock(&t->cmu);
    eng *target = NULL;
    for (size_t i = 0; i < t->nengs; i++) {
        if (t->engs[i]->tier_vol == vol && !eng_is_closing(t->engs[i])) {
            target = t->engs[i];
            break;
        }
    }
    if (target)
        t->eng_active++;
    pthread_mutex_unlock(&t->cmu);

    if (!target)
        return false;

    pthread_mutex_lock(&target->lock);
    for (size_t i = 0; i < target->nparts; i++) {
        part *p = target->parts[i];
        if (p->dropped)
            continue;
        struct ecpart *ep = atomic_load_explicit(&p->ec, memory_order_acquire);
        if (ep && !ep->test_mark && memcmp(ep->rec.oid, oid, 16) == 0) {
            live = true;
            break;
        }
    }
    pthread_mutex_unlock(&target->lock);

    pthread_mutex_lock(&t->cmu);
    if (--t->eng_active == 0)
        pthread_cond_broadcast(&t->eng_cv);
    pthread_mutex_unlock(&t->cmu);
    return live;
}

/* ---------------------------------------------------------------- per-pass callbacks */

typedef struct scrub_pass_ctx {
    struct tierscrub *sc;
    tier *t;
    int store;
    char store_path[PATH_MAX];
    uint32_t cur_vol;
    bool cursor_hold; /* true once an earlier volume in this pass was skipped as unready */
    const live_ec_map *live;

    /* virtual-time token bucket (worker thread only; reset on start, never accumulates across pauses) */
    uint64_t tb_next_ns;
    bool tb_init;

    /* checkpoint tracking */
    uint64_t pass_chunks;
    uint64_t since_save;
    uint64_t last_save_ns;
} scrub_pass_ctx;

static int scrub_live_cb(void *vctx, const uint8_t oid[16])
{
    const scrub_pass_ctx *c = vctx;
    if (!live_ec_find(c->live, c->cur_vol, oid))
        return 0;
    return eng_still_has_live_oid(c->t, c->cur_vol, oid) ? 1 : 0;
}

static int scrub_cancel_cb(void *vctx)
{
    const scrub_pass_ctx *c = vctx;
    return tierctl_is_closing(c->t) ? 1 : 0;
}

static int scrub_yield_cb(void *vctx)
{
    const scrub_pass_ctx *c = vctx;
    if (scrub_is_paused_test())
        return 1;
    return tierctl_scrub_should_yield(c->t) ? 1 : 0;
}

static void scrub_pace_cb(void *vctx, uint64_t bytes_done)
{
    scrub_pass_ctx *c = vctx;
    if (!c || bytes_done == 0)
        return;
    if (tierscrub_mut() == TIERSCRUB_MUT_NOPACE)
        return; /* planted mutant: ignores scrub_mb_s pacing */

    uint32_t mb_s = tierctl_scrub_mb_s(c->t);
    uint64_t rate = effective_rate_bps(c->sc, mb_s);
    if (rate == 0)
        return;

    uint64_t now = scrub_mono_ns();
    if (!c->tb_init) {
        c->tb_next_ns = now;
        c->tb_init = true;
    } else if (now > c->tb_next_ns + TIERSCRUB_BURST_NS) {
        c->tb_next_ns = now - TIERSCRUB_BURST_NS;
    }

    uint64_t cost_ns = (bytes_done / rate) * 1000000000ull + ((bytes_done % rate) * 1000000000ull) / rate;
    c->tb_next_ns += cost_ns;

    for (;;) {
        if (tierctl_is_closing(c->t) || scrub_yield_cb(c))
            return;
        now = scrub_mono_ns();
        if (now >= c->tb_next_ns)
            return;
        uint64_t rem_ns = c->tb_next_ns - now;
        uint64_t rem_ms = (rem_ns + 999999ull) / 1000000ull;
        if (rem_ms > 50)
            rem_ms = 50;
        if (rem_ms == 0)
            rem_ms = 1;
        if (!tierctl_wait_interruptible_ms(c->t, rem_ms))
            return;
    }
}

static void scrub_on_object_cb(void *vctx, const uint8_t oid[16], const chunk_heal_report *delta)
{
    scrub_pass_ctx *c = vctx;
    struct tierscrub *sc = c->sc;

    uint64_t ok = delta->chunks_ok;
    uint64_t rep_n = delta->rebuilt_damaged + delta->rebuilt_missing;
    uint64_t unrep = delta->unrecoverable;
    uint64_t unavail = delta->unavailable;
    uint64_t fail_n = delta->failed;
    uint64_t checked = ok + rep_n + unrep + unavail + fail_n;
    uint64_t dmg = rep_n + unrep + unavail + fail_n;

    tierscrub_store_stats save_snap;
    bool do_save = false;

    pthread_mutex_lock(&sc->mu);
    tscrub_store *ss = &sc->stores[c->store];
    ss->st.has_data = true;
    ss->st.chunks_checked += checked;
    ss->st.damaged += dmg;
    ss->st.repaired += rep_n;
    ss->st.unrepairable += unrep;
    ss->st.bytes_checked += delta->bytes_checked;
    if (!c->cursor_hold) {
        ss->st.cursor_valid = true;
        ss->st.cursor_vol = c->cur_vol;
        memcpy(ss->st.cursor_oid, oid, 16);
    }

    if (checked > 0) {
        c->pass_chunks += checked;
        c->since_save++;
    }
    uint32_t save_n = effective_save_every(sc);
    uint64_t now_ns = scrub_mono_ns();
    if (dmg > 0 || (c->since_save > 0 && ((save_n > 0 && c->since_save >= save_n) ||
                                          now_ns - c->last_save_ns >= TIERSCRUB_SAVE_INTERVAL_NS))) {
        save_snap = ss->st;
        do_save = true;
        c->since_save = 0;
        c->last_save_ns = now_ns;
    }
    pthread_mutex_unlock(&sc->mu);

    if (dmg > 0) {
        char ohex[33];
        chunk_oid_hex(oid, ohex);
        const live_ec_ent *ent = live_ec_find(c->live, c->cur_vol, oid);
        int64_t part_id = ent ? ent->partition : -1;
        const char *pname = ent ? ent->name : "-";
        if (rep_n > 0) {
            /* Immediately reopen cached object handles so readers see the newly renamed healthy chunk inode. */
            if (!tierctl_is_closing(c->t))
                eccache_reopen(c->t, c->cur_vol, oid);
            fprintf(stderr,
                    "tsdb: tier scrub store %d (%s): repaired %" PRIu64 " damaged chunk(s) of volume %" PRIu32
                    " object %s partition %" PRId64 " (%s)\n",
                    c->store, c->store_path, rep_n, c->cur_vol, ohex, part_id, pname);
        }
        if (unrep > 0) {
            fprintf(stderr,
                    "tsdb: tier scrub store %d (%s): damaged unrepairable chunk in volume %" PRIu32
                    " object %s partition %" PRId64 " (%s): report only, chunk left untouched\n",
                    c->store, c->store_path, c->cur_vol, ohex, part_id, pname);
        }
        if (unavail > 0) {
            fprintf(stderr,
                    "tsdb: tier scrub store %d (%s): damaged chunk in volume %" PRIu32 " object %s partition %" PRId64
                    " (%s): repair deferred (peer store unavailable)\n",
                    c->store, c->store_path, c->cur_vol, ohex, part_id, pname);
        }
        if (fail_n > 0) {
            fprintf(stderr,
                    "tsdb: tier scrub store %d (%s): could not verify or repair chunk in volume %" PRIu32
                    " object %s partition %" PRId64 " (%s)\n",
                    c->store, c->store_path, c->cur_vol, ohex, part_id, pname);
        }
    }

    if (do_save)
        scrub_save_store(c->store_path, c->store, &save_snap);

#ifdef TSDB_TEST_HOOKS
    {
        pthread_mutex_lock(&g_test_mu);
        tierscrub_obj_hook_fn hook = g_test_obj_hook;
        void *hctx = g_test_obj_hook_ctx;
        pthread_mutex_unlock(&g_test_mu);
        if (hook && delta->objects > 0)
            hook(hctx, c->store, c->cur_vol, oid, delta);
    }
#endif
}

static int cmp_u32(const void *a, const void *b)
{
    uint32_t x = *(const uint32_t *)a, y = *(const uint32_t *)b;
    return (x > y) - (x < y);
}

/* ---------------------------------------------------------------- store selection & pass execution */

static int pick_due_store(struct tierscrub *sc, int64_t now_ms, uint64_t now_s, uint32_t scrub_days,
                          uint32_t interval_ms, int64_t *wait_ms_out)
{
    tier *t = sc->t;
    int bn = chunk_be_nstores(t->be);
    if (bn > TIERS_STORES_MAX)
        bn = TIERS_STORES_MAX;

    /* Load any unloaded store SCRUB files WITHOUT holding sc->mu so /metrics never blocks on storage I/O. */
    bool need_load[TIERS_STORES_MAX] = {false};
    pthread_mutex_lock(&sc->mu);
    if (bn > sc->nstores)
        sc->nstores = bn;
    for (int i = 0; i < bn; i++) {
        if (!sc->stores[i].loaded && !chunk_be_store_is_lost(t->be, i) && !chunk_be_store_excluded(t->be, i) &&
            chunk_be_store_state(t->be, i) == CHUNK_STORE_UP)
            need_load[i] = true;
    }
    pthread_mutex_unlock(&sc->mu);

    for (int i = 0; i < bn; i++) {
        if (!need_load[i])
            continue;
        char pbuf[PATH_MAX];
        bool ok = chunk_be_store_path_copy(t->be, i, pbuf, sizeof pbuf);
        tscrub_store tmp;
        scrub_load_from_disk(i, ok ? pbuf : NULL, &tmp);
        pthread_mutex_lock(&sc->mu);
        if (!sc->stores[i].loaded)
            sc->stores[i] = tmp;
        pthread_mutex_unlock(&sc->mu);
    }

    int best_resume = -1;
    uint64_t best_resume_start = UINT64_MAX;
    int best_fresh = -1;
    int best_old = -1;
    uint64_t best_old_ts = UINT64_MAX;
    int64_t min_wait_ms = -1;

    pthread_mutex_lock(&sc->mu);
    for (int i = 0; i < bn; i++) {
        if (chunk_be_store_is_lost(t->be, i) || chunk_be_store_excluded(t->be, i))
            continue;
        if (chunk_be_store_state(t->be, i) != CHUNK_STORE_UP)
            continue;
        tscrub_store *ss = &sc->stores[i];
        if (now_ms < ss->retry_after_ms) {
            int64_t rem = ss->retry_after_ms - now_ms;
            if (min_wait_ms < 0 || rem < min_wait_ms)
                min_wait_ms = rem;
            continue;
        }
        /* 1. Interrupted mid-pass cursor resumes first (oldest pass_start_s first). */
        if (ss->st.cursor_valid) {
            uint64_t ps = ss->st.pass_start_s;
            if (best_resume < 0 || ps < best_resume_start) {
                best_resume = i;
                best_resume_start = ps;
            }
            continue;
        }
        /* 2. Store that has never completed a pass yet is immediately due. */
        if (ss->st.last_pass_s == 0) {
            if (best_fresh < 0)
                best_fresh = i;
            continue;
        }
        /* 3. Store with a completed pass: due once scrub_days (or test interval_ms) has elapsed. */
        bool due = false;
        int64_t rem_ms = 0;
        if (interval_ms > 0) {
            int64_t due_ms = ss->last_pass_mono_ms + (int64_t)interval_ms;
            if (now_ms >= due_ms)
                due = true;
            else
                rem_ms = due_ms - now_ms;
        } else if (scrub_days > 0) {
            uint64_t due_s = ss->st.last_pass_s + (uint64_t)scrub_days * 86400ull;
            if (now_s >= due_s) {
                due = true;
            } else {
                uint64_t diff_s = due_s - now_s;
                rem_ms = diff_s > 3600ull ? 3600000LL : (int64_t)(diff_s * 1000ull);
            }
        }
        if (due) {
            if (best_old < 0 || ss->st.last_pass_s < best_old_ts) {
                best_old = i;
                best_old_ts = ss->st.last_pass_s;
            }
        } else if (rem_ms > 0 && (min_wait_ms < 0 || rem_ms < min_wait_ms)) {
            min_wait_ms = rem_ms;
        }
    }
    pthread_mutex_unlock(&sc->mu);

    if (best_resume >= 0)
        return best_resume;
    if (best_fresh >= 0)
        return best_fresh;
    if (best_old >= 0)
        return best_old;
    if (wait_ms_out && min_wait_ms >= 0 && min_wait_ms < *wait_ms_out)
        *wait_ms_out = min_wait_ms;
    return -1;
}

bool tierscrub_step(tier *t, int64_t *wait_ms_out)
{
    if (!t || !t->be || t->read_only)
        return false;
    struct tierscrub *sc = atomic_load_explicit(&t->scrub, memory_order_acquire);
    if (!sc || sc->disabled)
        return false;

    if (scrub_is_paused_test() || tierctl_scrub_should_yield(t)) {
        pthread_mutex_lock(&sc->mu);
        sc->paused = true;
        pthread_mutex_unlock(&sc->mu);
        if (wait_ms_out && *wait_ms_out > 100)
            *wait_ms_out = 100;
        return false;
    }

    uint32_t sdays = tierctl_scrub_days(t);
    uint32_t iv_ms = effective_interval_ms(sc);
    if (sdays == 0 && iv_ms == 0)
        return false;

    int64_t now_ms = scrub_mono_ms();
    if (now_ms < sc->incomp_retry_ms) {
        int64_t rem = sc->incomp_retry_ms - now_ms;
        if (wait_ms_out && rem < *wait_ms_out)
            *wait_ms_out = rem;
        return false;
    }

    uint64_t now_s = scrub_wall_s();
    int sidx = pick_due_store(sc, now_ms, now_s, sdays, iv_ms, wait_ms_out);
    if (sidx < 0) {
        pthread_mutex_lock(&sc->mu);
        sc->paused = false;
        pthread_mutex_unlock(&sc->mu);
        return false;
    }

    /* Snapshot live .ec records across all ready engines (zero engine locks held across chunk I/O). */
    live_ec_map live = {0};
    bool all_complete = false;
    if (collect_live_ec_map(t, &live, &all_complete) != 0 || live.nready == 0) {
        live_ec_map_free(&live);
        uint32_t bo = sc->incomp_backoff_ms ? sc->incomp_backoff_ms * 2 : TIERSCRUB_INCOMP_MIN_MS;
        if (bo > TIERSCRUB_INCOMP_MAX_MS)
            bo = TIERSCRUB_INCOMP_MAX_MS;
        sc->incomp_backoff_ms = bo;
        sc->incomp_retry_ms = now_ms + (int64_t)bo;
        if (wait_ms_out && (int64_t)bo < *wait_ms_out)
            *wait_ms_out = (int64_t)bo;
        return false;
    }

    char spath[PATH_MAX];
    if (!chunk_be_store_path_copy(t->be, sidx, spath, sizeof spath)) {
        live_ec_map_free(&live);
        return false;
    }

    uint32_t *vols = NULL;
    size_t nv = 0;
    if (chunk_be_volumes_list(t->be, &vols, &nv) != 0) {
        live_ec_map_free(&live);
        if (wait_ms_out && *wait_ms_out > 500)
            *wait_ms_out = 500;
        return false;
    }
    if (nv > 1)
        qsort(vols, nv, sizeof *vols, cmp_u32);

    bool had_cursor = false;
    uint32_t cvol = 0;
    uint8_t coid[16] = {0};

    pthread_mutex_lock(&sc->mu);
    sc->paused = false;
    sc->active = true;
    sc->active_store = sidx;
    tscrub_store *ss = &sc->stores[sidx];
    had_cursor = ss->st.cursor_valid;
    cvol = ss->st.cursor_vol;
    memcpy(coid, ss->st.cursor_oid, 16);
    if (!had_cursor) {
        ss->st.pass_start_s = now_s;
        if (!ss->st.first_pass_s)
            ss->st.first_pass_s = now_s;
    }
    pthread_mutex_unlock(&sc->mu);

    scrub_pass_ctx pctx;
    memset(&pctx, 0, sizeof pctx);
    pctx.sc = sc;
    pctx.t = t;
    pctx.store = sidx;
    snprintf(pctx.store_path, sizeof pctx.store_path, "%s", spath);
    pctx.live = &live;
    pctx.tb_next_ns = scrub_mono_ns();
    pctx.tb_init = true;
    pctx.last_save_ns = pctx.tb_next_ns;

    int rc = 0;
    bool any_vol_skipped = false;
    bool any_vol_scanned = false;
    for (size_t vi = 0; vi < nv && rc == 0; vi++) {
        uint32_t vol = vols[vi];
        if (had_cursor && vol < cvol)
            continue;
        pthread_mutex_lock(&sc->mu);
        bool already_done = store_done_has(&sc->stores[sidx], vol);
        pthread_mutex_unlock(&sc->mu);
        if (already_done)
            continue;
        if (!live_ec_vol_ready(&live, vol)) {
            /* Volume is opening, closing, or quiesced: skip it for now without blocking later healthy volumes, and
             * anchor cursor_vol before it so a restart still scrubs it once it recovers. */
            any_vol_skipped = true;
            continue;
        }
        if (tierctl_is_closing(t) || scrub_yield_cb(&pctx)) {
            rc = TSDB_ERR_STOPPED;
            break;
        }
        pctx.cur_vol = vol;
        pctx.cursor_hold = any_vol_skipped;
        uint64_t ep = tier_epoch(t, vol);
        chunk_heal_opts ho = {
            .epoch = ep,
            .scrub = true,
            .live = scrub_live_cb,
            .live_ctx = &pctx,
            .cancel = scrub_cancel_cb,
            .cancel_ctx = &pctx,
            .yield = scrub_yield_cb,
            .yield_ctx = &pctx,
            .pace = scrub_pace_cb,
            .pace_ctx = &pctx,
            .start_after_oid = (had_cursor && vol == cvol && !oid_is_zero(coid)) ? coid : NULL,
            .on_object = scrub_on_object_cb,
            .on_object_ctx = &pctx,
#ifdef TSDB_TEST_HOOKS
            .scrub_no_repair = tierscrub_mut() == TIERSCRUB_MUT_NOREPAIR,
#endif
        };
        chunk_heal_report vrep;
        rc = chunk_be_heal(t->be, vol, sidx, &ho, &vrep);
        if (rc == 0) {
            any_vol_scanned = true;
            pthread_mutex_lock(&sc->mu);
            tscrub_store *ds = &sc->stores[sidx];
            store_done_add(ds, vol);
            /* Advance persisted cursor past this completed volume when more volumes or unready volumes remain so
             * finished volumes are never re-listed on retry or restart. */
            if (!any_vol_skipped && (vi + 1 < nv || !all_complete) && vol < UINT32_MAX) {
                ds->st.cursor_valid = true;
                ds->st.cursor_vol = vol + 1;
                memset(ds->st.cursor_oid, 0, sizeof ds->st.cursor_oid);
            }
            pthread_mutex_unlock(&sc->mu);
        }
    }
    bool skipped_unready_vol = any_vol_skipped || !all_complete;

    free(vols);
    live_ec_map_free(&live);

    tierscrub_store_stats final_snap;
    bool do_save = false;
    bool pass_completed = false;
    uint64_t end_s = scrub_wall_s();
    int64_t end_ms = scrub_mono_ms();

    pthread_mutex_lock(&sc->mu);
    sc->active = false;
    sc->active_store = -1;
    ss = &sc->stores[sidx];
    if (rc == 0 && skipped_unready_vol) {
        /* One or more volumes are unready (e.g. quiesced): save cursor progress and back off exponentially up to 60 s
         * without re-listing already-completed volumes. */
        if (ss->st.cursor_valid || any_vol_scanned) {
            final_snap = ss->st;
            do_save = true;
        }
        uint32_t max_bo = (iv_ms > 0 && iv_ms < TIERSCRUB_INCOMP_MAX_MS)
                              ? (iv_ms > 500u ? iv_ms : 500u)
                              : TIERSCRUB_INCOMP_MAX_MS;
        uint32_t bo = any_vol_scanned ? TIERSCRUB_INCOMP_MIN_MS
                                      : (sc->incomp_backoff_ms ? sc->incomp_backoff_ms * 2 : TIERSCRUB_INCOMP_MIN_MS);
        if (bo > max_bo)
            bo = max_bo;
        sc->incomp_backoff_ms = bo;
        sc->incomp_retry_ms = end_ms + (int64_t)bo;
    } else if (rc == 0) {
        sc->incomp_backoff_ms = 0;
        sc->incomp_retry_ms = 0;
        ss->fails = 0;
        bool had_done_vols = ss->ndone_vols > 0 && (pctx.pass_chunks > 0 || had_cursor || ss->st.chunks_checked > 0);
        store_done_clear(ss);
        bool completed_pass = pctx.pass_chunks > 0 || had_cursor || had_done_vols;
        if (completed_pass) {
            ss->empty_streak = 0;
            ss->st.has_data = true;
            ss->st.last_pass_s = ss->st.pass_start_s ? ss->st.pass_start_s : end_s;
            ss->st.pass_start_s = 0;
            if (!ss->st.first_pass_s)
                ss->st.first_pass_s = ss->st.last_pass_s;
            ss->st.cursor_valid = false;
            ss->st.cursor_vol = 0;
            memset(ss->st.cursor_oid, 0, sizeof ss->st.cursor_oid);
            ss->last_pass_mono_ms = end_ms;
            ss->retry_after_ms = 0;
            final_snap = ss->st;
            final_snap.passes_done++;
            do_save = true;
            pass_completed = true;
        } else {
            /* Store has 0 live chunks right now: back off exponentially toward 1 h (capped by iv_ms in tests)
             * so empty or newly-added stores do not continuously re-list peer stores every 2 s. */
            ss->st.pass_start_s = 0;
            uint64_t poll = TIERSCRUB_EMPTY_POLL_MS;
            if (ss->empty_streak < 12)
                ss->empty_streak++;
            poll <<= (unsigned)(ss->empty_streak - 1);
            if (poll > TIERSCRUB_EMPTY_POLL_MAX_MS)
                poll = TIERSCRUB_EMPTY_POLL_MAX_MS;
            if (iv_ms > 0 && poll > iv_ms)
                poll = iv_ms;
            ss->retry_after_ms = end_ms + (int64_t)poll;
        }
    } else if (rc == TSDB_ERR_STOPPED) {
        sc->incomp_backoff_ms = 0;
        sc->incomp_retry_ms = 0;
        sc->paused = scrub_is_paused_test() || tierctl_scrub_should_yield(t);
        if (ss->st.cursor_valid) {
            final_snap = ss->st;
            do_save = true;
        }
    } else {
        if (ss->st.cursor_valid) {
            final_snap = ss->st;
            do_save = true;
        }
        if (ss->fails < 6)
            ss->fails++;
        int64_t backoff = (int64_t)500 << (ss->fails - 1);
        if (backoff > 15000)
            backoff = 15000;
        ss->retry_after_ms = end_ms + backoff;
    }
    pthread_mutex_unlock(&sc->mu);

    if (do_save)
        scrub_save_store(spath, sidx, &final_snap);

    /* Publish passes_done and wake waiters AFTER SCRUB is durable on disk so readers observe the persisted state. */
    if (pass_completed) {
        pthread_mutex_lock(&sc->mu);
        sc->stores[sidx].st.passes_done = final_snap.passes_done;
        pthread_cond_broadcast(&sc->pass_cv);
        pthread_mutex_unlock(&sc->mu);
    }

    /* If we only hit an empty store or unready volume, return false so worker_main sleeps instead of spinning. */
    return rc != 0 || (!skipped_unready_vol && (pctx.pass_chunks > 0 || had_cursor));
}

/* ---------------------------------------------------------------- lifecycle & inspection */

int tierscrub_open(tier *t)
{
    if (!t || !t->be || t->read_only)
        return 0;
    struct tierscrub *sc = calloc(1, sizeof *sc);
    if (!sc)
        return TSDB_ERR_NOMEM;
    sc->t = t;
    sc->active_store = -1;
    pthread_mutex_init(&sc->mu, NULL);
    tsdb_cond_init_mono(&sc->pass_cv);
    scrub_load_env(sc);

    int bn = chunk_be_nstores(t->be);
    if (bn > TIERS_STORES_MAX)
        bn = TIERS_STORES_MAX;
    sc->nstores = bn;
    for (int i = 0; i < bn; i++) {
        char pbuf[PATH_MAX];
        bool ok = chunk_be_store_state(t->be, i) == CHUNK_STORE_UP && chunk_be_store_path_copy(t->be, i, pbuf, sizeof pbuf);
        if (ok) {
            scrub_load_from_disk(i, pbuf, &sc->stores[i]);
        } else {
            sc->stores[i].st.store = i;
            sc->stores[i].loaded = false;
        }
    }

    atomic_store_explicit(&t->scrub, sc, memory_order_release);
    return 0;
}

void tierscrub_set_paused(tier *t, bool paused)
{
    if (!t)
        return;
    struct tierscrub *sc = atomic_load_explicit(&t->scrub, memory_order_acquire);
    if (!sc)
        return;
    pthread_mutex_lock(&sc->mu);
    sc->paused = paused;
    pthread_mutex_unlock(&sc->mu);
}

void tierscrub_quiesce(tier *t)
{
    if (!t)
        return;
    struct tierscrub *sc = atomic_load_explicit(&t->scrub, memory_order_acquire);
    if (!sc)
        return;
    pthread_mutex_lock(&sc->mu);
    pthread_cond_broadcast(&sc->pass_cv);
    pthread_mutex_unlock(&sc->mu);
}

void tierscrub_close(tier *t)
{
    if (!t)
        return;
    struct tierscrub *sc = atomic_exchange_explicit(&t->scrub, NULL, memory_order_acq_rel);
    if (!sc)
        return;
    for (int i = 0; i < TIERS_STORES_MAX; i++)
        store_done_clear(&sc->stores[i]);
    pthread_cond_destroy(&sc->pass_cv);
    pthread_mutex_destroy(&sc->mu);
    free(sc);
}

void tierscrub_on_store_changed(tier *t, int be_idx, const char *path)
{
    if (!t || be_idx < 0 || be_idx >= TIERS_STORES_MAX)
        return;
    struct tierscrub *sc = atomic_load_explicit(&t->scrub, memory_order_acquire);
    if (!sc)
        return;
    tscrub_store tmp;
    scrub_load_from_disk(be_idx, path, &tmp);
    pthread_mutex_lock(&sc->mu);
    if (be_idx >= sc->nstores)
        sc->nstores = be_idx + 1;
    store_done_clear(&sc->stores[be_idx]);
    sc->stores[be_idx] = tmp;
    sc->incomp_backoff_ms = 0;
    sc->incomp_retry_ms = 0;
    pthread_mutex_unlock(&sc->mu);
}

bool tierscrub_get_snapshot(const tier *t, tierscrub_snapshot *out)
{
    if (!out)
        return false;
    memset(out, 0, sizeof *out);
    out->active_store = -1;
    if (!t)
        return false;
    struct tierscrub *sc = atomic_load_explicit(&((tier *)t)->scrub, memory_order_acquire);
    if (!sc)
        return false;

    uint64_t now_s = scrub_wall_s();
    out->scrub_mb_s = tierctl_scrub_mb_s(t);
    out->scrub_days = tierctl_scrub_days(t);

    pthread_mutex_lock(&sc->mu);
    out->active = sc->active;
    out->paused = sc->paused;
    out->active_store = sc->active_store;
    int n = sc->nstores;
    if (t->be) {
        int bn = chunk_be_nstores(t->be);
        if (bn > n)
            n = bn;
    }
    if (n > TIERS_STORES_MAX)
        n = TIERS_STORES_MAX;
    out->nstores = n;
    for (int i = 0; i < n; i++) {
        tierscrub_store_stats s = sc->stores[i].st;
        s.store = i;
        uint64_t ref_s = s.last_pass_s ? s.last_pass_s : s.first_pass_s;
        s.oldest_age_s = (ref_s > 0 && now_s > ref_s) ? now_s - ref_s : 0;
        out->stores[i] = s;
        if (t->be && !chunk_be_store_is_lost(t->be, i) && !chunk_be_store_excluded(t->be, i) &&
            chunk_be_store_state(t->be, i) == CHUNK_STORE_UP && ref_s > 0) {
            if (!out->has_oldest_age || s.oldest_age_s > out->oldest_age_s) {
                out->oldest_age_s = s.oldest_age_s;
                out->has_oldest_age = true;
            }
        }
    }
    pthread_mutex_unlock(&sc->mu);
    return true;
}

int tierscrub_wait_pass(tier *t, int store, uint64_t want_passes, int timeout_ms)
{
    if (!t || store < 0 || store >= TIERS_STORES_MAX)
        return TSDB_ERR_INVALID;
    struct tierscrub *sc = atomic_load_explicit(&t->scrub, memory_order_acquire);
    if (!sc)
        return TSDB_ERR_INVALID;
    struct timespec dl;
    tsdb_cond_deadline_ms(&dl, timeout_ms > 0 ? (uint64_t)timeout_ms : 10000);
    pthread_mutex_lock(&sc->mu);
    for (;;) {
        if (sc->stores[store].st.passes_done >= want_passes) {
            pthread_mutex_unlock(&sc->mu);
            return 0;
        }
        if (tierctl_is_closing(t)) {
            pthread_mutex_unlock(&sc->mu);
            return TSDB_ERR_STOPPED;
        }
        if (pthread_cond_timedwait(&sc->pass_cv, &sc->mu, &dl) == ETIMEDOUT) {
            pthread_mutex_unlock(&sc->mu);
            return TSDB_ERR_UNAVAILABLE;
        }
    }
}

#else /* !TSDB_EC */

int tierscrub_open(struct tier *t)
{
    (void)t;
    return 0;
}
void tierscrub_close(struct tier *t) { (void)t; }
void tierscrub_on_store_changed(struct tier *t, int be_idx, const char *path)
{
    (void)t, (void)be_idx, (void)path;
}
bool tierscrub_step(struct tier *t, int64_t *wait_ms_out)
{
    (void)t, (void)wait_ms_out;
    return false;
}
void tierscrub_set_paused(struct tier *t, bool paused) { (void)t, (void)paused; }
void tierscrub_quiesce(struct tier *t) { (void)t; }
bool tierscrub_get_snapshot(const struct tier *t, tierscrub_snapshot *out)
{
    (void)t;
    if (out) {
        memset(out, 0, sizeof *out);
        out->active_store = -1;
    }
    return false;
}
int tierscrub_wait_pass(struct tier *t, int store, uint64_t want_passes, int timeout_ms)
{
    (void)t, (void)store, (void)want_passes, (void)timeout_ms;
    return TSDB_ERR_INVALID;
}

#endif
