/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R9 startup and return chunk reconciler (server/sreconcile.h, server/sfill.h; track r9-reconcile; R9 plan D14, D15;
 * docs/design-cluster-data.md §5.2, E10, E14, E27, E28).
 *
 * Lock order:
 *   - rec->pub_mu -> rec->mu (leaf: never held across I/O, sfill_enqueue_obj, or user callbacks).
 *   - rec->store_mu (leaf), missing_mu (leaf), reg_mu (leaf).
 */
#include "sreconcile.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "../include/tsdb.h"
#include "../src/compat.h"
#include "../src/internal.h"
#include "chunksrv.h"
#include "pace.h"

#define SRECONCILE_MISSING_CAP 4096u
#define SRECONCILE_DEFAULT_RETRY_MS 300u
#define SRECONCILE_MAX_BACKOFF_MS 60000u
#define SRECONCILE_STORE_POLL_MS 2000u
#define SRECONCILE_DRAIN_INTERVAL_MS 500u
#define SRECONCILE_DRAIN_MAX_PER_TICK 8u
#define SRECONCILE_OBJLIST_PAGE CS_OBJLIST_MAX
#define SRECONCILE_STORE_MAX 64u
#define SRECONCILE_STORE_FILE "chunk_stores"
#define SRECONCILE_STORE_TMP "chunk_stores.tmp"

_Atomic int sreconcile_mutant = SRECONCILE_MUT_NONE;

#ifdef TSDB_TEST_HOOKS
int sreconcile_test_retry_ms = 0;
#endif

int sreconcile_mutant_parse(const char *name)
{
    if (!name || !*name || strcmp(name, "none") == 0)
        return SRECONCILE_MUT_NONE;
    if (strcmp(name, "backfullcopy") == 0)
        return SRECONCILE_MUT_FULLCOPY;
    if (strcmp(name, "backskipmissing") == 0)
        return SRECONCILE_MUT_SKIPMISSING;
    return -1;
}

void sreconcile_set_mutant(int mut)
{
    atomic_store_explicit(&sreconcile_mutant, mut, memory_order_relaxed);
}

static void load_env_mutant_once(void)
{
#ifdef TSDB_TEST_HOOKS
    const char *mu = getenv("TSDB_SRECONCILE_MUTANT");
    if (mu && *mu) {
        int m = sreconcile_mutant_parse(mu);
        if (m > 0) {
            atomic_store_explicit(&sreconcile_mutant, m, memory_order_relaxed);
            fprintf(stderr, "tsdbd: sreconcile MUTANT %s (tests only)\n", mu);
        } else if (m < 0) {
            fprintf(stderr, "tsdbd: TSDB_SRECONCILE_MUTANT=%s is not a planted bug\n", mu);
            _exit(2);
        }
    }
#endif
}

static uint64_t retry_delay_ms(unsigned backoff_shift)
{
    uint64_t base = SRECONCILE_DEFAULT_RETRY_MS;
    uint64_t cap = SRECONCILE_MAX_BACKOFF_MS;
#ifdef TSDB_TEST_HOOKS
    if (sreconcile_test_retry_ms > 0) {
        base = (uint64_t)sreconcile_test_retry_ms;
        cap = base * 16u;
    }
#endif
    if (backoff_shift > 10u)
        backoff_shift = 10u;
    uint64_t d = base << backoff_shift;
    return d > cap ? cap : d;
}

static uint64_t store_poll_ms(void)
{
#ifdef TSDB_TEST_HOOKS
    if (sreconcile_test_retry_ms > 0)
        return (uint64_t)sreconcile_test_retry_ms;
#endif
    return SRECONCILE_STORE_POLL_MS;
}

static uint64_t mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

static bool id_is_zero(const uint8_t id[16])
{
    for (int i = 0; i < 16; i++)
        if (id[i] != 0)
            return false;
    return true;
}

static int hex_nibble(char c)
{
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return 10 + (c - 'a');
    if (c >= 'A' && c <= 'F')
        return 10 + (c - 'A');
    return -1;
}

static bool hex_decode16(const char *s, uint8_t out[16])
{
    for (int i = 0; i < 16; i++) {
        int hi = hex_nibble(s[2 * i]);
        int lo = hex_nibble(s[2 * i + 1]);
        if (hi < 0 || lo < 0)
            return false;
        out[i] = (uint8_t)((hi << 4) | lo);
    }
    return true;
}

static bool hex_decode_u64(const char *s, uint64_t *out)
{
    uint64_t v = 0;
    for (int i = 0; i < 16; i++) {
        int nib = hex_nibble(s[i]);
        if (nib < 0)
            return false;
        v = (v << 4) | (uint64_t)nib;
    }
    *out = v;
    return true;
}

/* ------------------------------------------------------------------ degraded-settle missing ring (E10) */

struct sreconcile_missing {
    uint32_t volume;
    uint8_t oid[16];
    unsigned pos;
    bool pending;
};

static pthread_mutex_t missing_mu = PTHREAD_MUTEX_INITIALIZER;
static struct sreconcile_missing missing_ring[SRECONCILE_MISSING_CAP];
static size_t missing_head;
static uint64_t missing_total;
static atomic_uint_fast64_t missing_pending_cnt;

void sreconcile_note_missing(uint32_t volume, const uint8_t oid[16], unsigned pos)
{
    if (!oid)
        return;
    bool overwrote_pending = false;
    uint32_t old_vol = 0;
    unsigned old_pos = 0;
    pthread_mutex_lock(&missing_mu);
    bool exists = false;
    size_t count = missing_head < SRECONCILE_MISSING_CAP ? missing_head : SRECONCILE_MISSING_CAP;
    for (size_t i = 0; i < count; i++) {
        if (missing_ring[i].pending && missing_ring[i].volume == volume && missing_ring[i].pos == pos &&
            memcmp(missing_ring[i].oid, oid, 16) == 0) {
            exists = true;
            break;
        }
    }
    if (!exists) {
        struct sreconcile_missing *m = &missing_ring[missing_head % SRECONCILE_MISSING_CAP];
        if (m->pending) {
            overwrote_pending = true;
            old_vol = m->volume;
            old_pos = m->pos;
        } else {
            atomic_fetch_add_explicit(&missing_pending_cnt, 1, memory_order_release);
        }
        m->volume = volume;
        memcpy(m->oid, oid, 16);
        m->pos = pos;
        m->pending = true;
        missing_head++;
    }
    missing_total++;
    pthread_mutex_unlock(&missing_mu);
    if (overwrote_pending) {
        fprintf(stderr,
                "tsdbd: reconcile: missing-position ring full (%u entries): dropped pending nudge for volume %" PRIu32
                " pos %u (return reconcile will still rebuild from OBJLIST)\n",
                SRECONCILE_MISSING_CAP, old_vol, old_pos);
    }
    char hex[33];
    chunk_oid_hex(oid, hex);
    fprintf(stderr, "tsdbd: volume %" PRIu32 ": reconcile noted missing position %u of object %s\n", volume, pos, hex);
}

uint64_t sreconcile_missing_count(void)
{
    pthread_mutex_lock(&missing_mu);
    uint64_t n = missing_total;
    pthread_mutex_unlock(&missing_mu);
    return n;
}

#ifdef TSDB_TEST_HOOKS
void sreconcile_test_reset_missing(void)
{
    pthread_mutex_lock(&missing_mu);
    memset(missing_ring, 0, sizeof missing_ring);
    missing_head = 0;
    missing_total = 0;
    atomic_store_explicit(&missing_pending_cnt, 0, memory_order_release);
    pthread_mutex_unlock(&missing_mu);
}
#endif

/* ------------------------------------------------------------------ sfill -> sreconcile registry */

struct rec_reg_ent {
    struct sfill *sf;
    struct sreconcile *rec;
    struct rec_reg_ent *next;
};

static pthread_mutex_t reg_mu = PTHREAD_MUTEX_INITIALIZER;
static struct rec_reg_ent *reg_head;

static void reg_add(struct sfill *sf, struct sreconcile *rec)
{
    if (!sf || !rec)
        return;
    struct rec_reg_ent *e = malloc(sizeof *e);
    if (!e)
        return;
    e->sf = sf;
    e->rec = rec;
    pthread_mutex_lock(&reg_mu);
    e->next = reg_head;
    reg_head = e;
    pthread_mutex_unlock(&reg_mu);
}

static void reg_del(struct sreconcile *rec)
{
    pthread_mutex_lock(&reg_mu);
    for (struct rec_reg_ent **pp = &reg_head; *pp;) {
        if ((*pp)->rec == rec) {
            struct rec_reg_ent *dead = *pp;
            *pp = dead->next;
            free(dead);
        } else {
            pp = &(*pp)->next;
        }
    }
    pthread_mutex_unlock(&reg_mu);
}

static struct sreconcile *reg_find(struct sfill *sf)
{
    struct sreconcile *rec = NULL;
    pthread_mutex_lock(&reg_mu);
    for (struct rec_reg_ent *e = reg_head; e; e = e->next) {
        if (e->sf == sf) {
            rec = e->rec;
            break;
        }
    }
    pthread_mutex_unlock(&reg_mu);
    return rec;
}

/* ------------------------------------------------------------------ internal state */

struct rec_store_ent {
    char path[PATH_MAX];
    uint8_t expected_id[16];
    uint8_t pending_new_id[16]; /* new store_id observed; committed to disk only after refill succeeds */
    uint8_t last_bad_id[16];
    int store_idx;              /* last observed index (0..63, -1 if unknown) */
    bool has_expected;
    bool has_pending_new_id;
    bool logged_down;
};

struct rec_job {
    struct rec_job *next;
    uint32_t volume;
    uint32_t pos;
    uint64_t stripe_version;
    uint64_t gen;
    uint64_t done;
    uint64_t total;
    uint64_t failures;
    uint64_t page_pending;    /* tasks in flight for the current streamed OBJLIST page */
    uint64_t page_errors;     /* failed tasks in the current streamed pass */
    uint64_t pass_bytes;      /* verified bytes filled in the current pass */
    uint64_t retry_at_ms;
    unsigned backoff_shift;   /* exponential backoff on persistent errors / down stores */
    uint64_t pace_id;
    uint64_t replace_mask;    /* writable stores with changed store_id that still require full refill */
    uint64_t replacing_mask;  /* CHUNK_STORE_REPLACING stores awaiting clean reconcile before promotion to UP */
    uint64_t down_wait_mask;    /* DOWN/non-writable store indices blocking deferred chunks */
    uint64_t room_wait_out_mask;/* OUT store indices waiting on local capacity room or off-node eviction */
    bool waiting_store_up;      /* paused until a bit in down_wait_mask transitions UP (no OBJLIST storm) */
    bool waiting_room;          /* paused until pick_target_store finds room or OUT set / position changes */
    bool pending_run;           /* queued for coordinator to run a reconcile pass */
    bool running;             /* coordinator is currently running a reconcile pass */
    bool reporting;           /* visible in HBS_FILL while tasks are filling */
    bool retrigger;           /* another trigger arrived while running */
    bool removed;             /* stopped (position moved away or entered FILLING) */
};

struct rec_task_ctx {
    struct sreconcile *rec;
    struct rec_job *job;
    uint64_t gen;
    uint64_t chunk_bytes;
};

struct sreconcile {
    struct sfill *sf;
    struct sreconcile_ops ops;
    char state_dir[PATH_MAX];

    pthread_mutex_t pub_mu; /* serializes publish_reports() -> on_report(); lock order: pub_mu -> mu */
    pthread_mutex_t mu;
    pthread_cond_t cv;      /* CLOCK_MONOTONIC: wakes coordinator */
    pthread_cond_t page_cv; /* CLOCK_MONOTONIC: wakes coordinator when current page drains or stop/cancel */
    pthread_cond_t done_cv; /* CLOCK_MONOTONIC: wakes sreconcile_free when total_inflight == 0 */

    pthread_t thr;
    bool thr_started;
    atomic_bool stopping;
    bool trigger_all_pending; /* on_start / on_return queued for reconcile_main (off watch thread) */
    uint64_t trigger_all_at_ms; /* earliest mono_ms to run trigger_all_pending (retry delay when copy not caught up) */
    bool drain_kicked;
    uint64_t next_drain_ms;
    uint64_t next_store_poll_ms;
    uint64_t total_inflight;

    struct rec_job *jobs;

    /* Store identity tracker (leaf lock; never held across I/O to chunk stores or rec->mu). */
    pthread_mutex_t store_mu;
    bool stores_loaded;
    bool stores_damaged; /* read/parse/checksum error on chunk_stores: refuse to overwrite */
    struct rec_store_ent stores[SRECONCILE_STORE_MAX];
    size_t nstores;

    struct {
        atomic_uint_fast64_t triggers;
        atomic_uint_fast64_t passes_ok;
        atomic_uint_fast64_t passes_err;
        atomic_uint_fast64_t missing_queued;
        atomic_uint_fast64_t missing_on_down_store;
        atomic_uint_fast64_t filled;
        atomic_uint_fast64_t already_present;
        atomic_uint_fast64_t gc_pending;
        atomic_uint_fast64_t orphans_reported;
        atomic_uint_fast64_t store_id_mismatches;
        atomic_uint_fast64_t store_down_events;
        atomic_uint_fast64_t bytes_filled;
        atomic_uint_fast64_t failed;
        atomic_uint_fast64_t holder_nudges;
    } st;
};

#define REC_STAT(r, f) atomic_fetch_add_explicit(&(r)->st.f, 1, memory_order_relaxed)
#define REC_STATN(r, f, v) atomic_fetch_add_explicit(&(r)->st.f, (v), memory_order_relaxed)

/* ------------------------------------------------------------------ store id persistence & check (E27, D15) */

static void stores_load_locked(struct sreconcile *rec)
{
    if (rec->stores_loaded)
        return;
    rec->stores_loaded = true;
    if (!rec->state_dir[0])
        return;
    char path[PATH_MAX + 32];
    snprintf(path, sizeof path, "%s/%s", rec->state_dir, SRECONCILE_STORE_FILE);
    FILE *f = fopen(path, "r");
    if (!f) {
        if (errno != ENOENT) {
            fprintf(stderr,
                    "tsdbd: reconcile: cannot read %s: %s (expected store ids unavailable; refusing to overwrite)\n",
                    path, strerror(errno));
            rec->stores_damaged = true;
        }
        return;
    }

    char *raw = NULL;
    size_t raw_len = 0, raw_cap = 0;
    char line[PATH_MAX + 64];
    bool saw_trailer = false;
    bool damaged = false;
    unsigned lineno = 0;
    uint64_t want_hash = 0;

    while (fgets(line, sizeof line, f)) {
        lineno++;
        size_t len = strlen(line);
        if (len == 0 || line[len - 1] != '\n') {
            fprintf(stderr, "tsdbd: reconcile: %s is damaged (line %u truncated or missing newline)\n", path, lineno);
            damaged = true;
            break;
        }
        if (saw_trailer) {
            fprintf(stderr, "tsdbd: reconcile: %s is damaged (trailing data after XXH64 checksum at line %u)\n", path,
                    lineno);
            damaged = true;
            break;
        }
        if (len == 23 && strncmp(line, "XXH64 ", 6) == 0 && hex_decode_u64(line + 6, &want_hash)) {
            saw_trailer = true;
            continue;
        }
        /* Append exact record line bytes to raw buffer for checksum verification. */
        if (raw_len + len > raw_cap) {
            size_t nc = raw_cap ? raw_cap * 2 : 512;
            while (nc < raw_len + len)
                nc *= 2;
            char *nr = realloc(raw, nc);
            if (!nr) {
                fprintf(stderr, "tsdbd: reconcile: out of memory reading %s\n", path);
                damaged = true;
                break;
            }
            raw = nr;
            raw_cap = nc;
        }
        memcpy(raw + raw_len, line, len);
        raw_len += len;

        size_t tlen = len - 1; /* strip '\n' */
        if (tlen > 0 && line[tlen - 1] == '\r')
            tlen--;
        line[tlen] = '\0';
        uint8_t id[16];
        if (tlen < 34 || line[32] != ' ' || !hex_decode16(line, id) || id_is_zero(id)) {
            fprintf(stderr, "tsdbd: reconcile: %s is damaged (malformed record at line %u)\n", path, lineno);
            damaged = true;
            break;
        }
        const char *sp = line + 33;
        size_t splen = tlen - 33;
        if (splen == 0 || splen >= PATH_MAX || rec->nstores >= SRECONCILE_STORE_MAX) {
            fprintf(stderr, "tsdbd: reconcile: %s is damaged (invalid store entry at line %u)\n", path, lineno);
            damaged = true;
            break;
        }
        struct rec_store_ent *e = &rec->stores[rec->nstores++];
        memset(e, 0, sizeof *e);
        e->store_idx = -1;
        memcpy(e->path, sp, splen);
        e->path[splen] = '\0';
        memcpy(e->expected_id, id, 16);
        e->has_expected = true;
    }
    if (!damaged && ferror(f)) {
        fprintf(stderr, "tsdbd: reconcile: read error on %s: %s (refusing to overwrite)\n", path, strerror(errno));
        damaged = true;
    }
    fclose(f);

    if (!damaged) {
        if (!saw_trailer) {
            fprintf(stderr, "tsdbd: reconcile: %s is damaged (missing XXH64 checksum trailer; refusing to overwrite)\n",
                    path);
            damaged = true;
        } else {
            uint64_t got_hash = xxh64(raw ? raw : "", raw_len, 0);
            if (got_hash != want_hash) {
                fprintf(stderr,
                        "tsdbd: reconcile: %s is damaged (XXH64 mismatch: got %016" PRIx64 ", want %016" PRIx64
                        "; refusing to overwrite)\n",
                        path, got_hash, want_hash);
                damaged = true;
            }
        }
    }
    free(raw);
    if (damaged) {
        rec->nstores = 0;
        rec->stores_damaged = true;
    }
}

static bool write_all_fd(int fd, const void *buf, size_t len)
{
    const uint8_t *p = buf;
    size_t off = 0;
    while (off < len) {
        ssize_t w = write(fd, p + off, len - off);
        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0)
            return false;
        off += (size_t)w;
    }
    return true;
}

static void stores_save_locked(const struct sreconcile *rec)
{
    if (!rec->state_dir[0] || rec->stores_damaged)
        return;
    char path[PATH_MAX + 32];
    snprintf(path, sizeof path, "%s/%s", rec->state_dir, SRECONCILE_STORE_FILE);

    char *buf = NULL;
    size_t used = 0, cap = 0;
    for (size_t i = 0; i < rec->nstores; i++) {
        if (!rec->stores[i].has_expected || id_is_zero(rec->stores[i].expected_id))
            continue;
        char hex[33];
        chunk_oid_hex(rec->stores[i].expected_id, hex);
        char line[PATH_MAX + 48];
        int n = snprintf(line, sizeof line, "%s %s\n", hex, rec->stores[i].path);
        if (n <= 0 || (size_t)n >= sizeof line) {
            fprintf(stderr, "tsdbd: reconcile: store path too long for %s\n", path);
            free(buf);
            return;
        }
        if (used + (size_t)n + 32 > cap) {
            size_t nc = cap ? cap * 2 : 512;
            while (nc < used + (size_t)n + 32)
                nc *= 2;
            char *nb = realloc(buf, nc);
            if (!nb) {
                fprintf(stderr, "tsdbd: reconcile: out of memory saving %s\n", path);
                free(buf);
                return;
            }
            buf = nb;
            cap = nc;
        }
        memcpy(buf + used, line, (size_t)n);
        used += (size_t)n;
    }
    uint64_t h = xxh64(buf ? buf : "", used, 0);
    char trailer[32];
    int tn = snprintf(trailer, sizeof trailer, "XXH64 %016" PRIx64 "\n", h);
    if (tn <= 0 || (size_t)tn >= sizeof trailer) {
        free(buf);
        return;
    }

    int dfd = open(rec->state_dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd < 0) {
        fprintf(stderr, "tsdbd: reconcile: cannot open state dir %s for %s: %s\n", rec->state_dir, path,
                strerror(errno));
        free(buf);
        return;
    }
    int fd = openat(dfd, SRECONCILE_STORE_TMP, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) {
        fprintf(stderr, "tsdbd: reconcile: cannot create %s.tmp: %s\n", path, strerror(errno));
        close(dfd);
        free(buf);
        return;
    }
    bool ok = (used == 0 || write_all_fd(fd, buf, used)) && write_all_fd(fd, trailer, (size_t)tn);
    if (!ok)
        fprintf(stderr, "tsdbd: reconcile: write failed on %s.tmp: %s\n", path, strerror(errno));
    free(buf);

    if (ok && tsdb_fdatasync(fd) != 0) {
        fprintf(stderr, "tsdbd: reconcile: fdatasync failed on %s.tmp: %s\n", path, strerror(errno));
        ok = false;
    }
    if (close(fd) != 0) {
        fprintf(stderr, "tsdbd: reconcile: close failed on %s.tmp: %s\n", path, strerror(errno));
        ok = false;
    }
    if (ok) {
        if (renameat(dfd, SRECONCILE_STORE_TMP, dfd, SRECONCILE_STORE_FILE) != 0) {
            fprintf(stderr, "tsdbd: reconcile: rename %s.tmp -> %s failed: %s\n", path, path, strerror(errno));
            unlinkat(dfd, SRECONCILE_STORE_TMP, 0);
        } else if (tsdb_fsync(dfd) != 0) {
            fprintf(stderr, "tsdbd: reconcile: dir fsync failed for %s: %s\n", path, strerror(errno));
        }
    } else {
        unlinkat(dfd, SRECONCILE_STORE_TMP, 0);
    }
    close(dfd);
}

static struct rec_store_ent *store_ent_find_or_add_locked(struct sreconcile *rec, const char *path)
{
    for (size_t i = 0; i < rec->nstores; i++)
        if (strcmp(rec->stores[i].path, path) == 0)
            return &rec->stores[i];
    if (rec->nstores >= SRECONCILE_STORE_MAX)
        return NULL;
    struct rec_store_ent *e = &rec->stores[rec->nstores++];
    memset(e, 0, sizeof *e);
    e->store_idx = -1;
    snprintf(e->path, sizeof e->path, "%s", path);
    return e;
}

/* Inspect all configured local chunk stores against expected store ids.
 *   - `down_mask_out`: bit `i` set for non-writable/DOWN stores (not refilled until back UP).
 *   - `replace_mask_out`: bit `i` set for writable stores whose store_id differs from expected_id (forced refill).
 *   - `all_down_out`: true iff 0 stores are currently writable.
 * Never persists a replacement store's new id until commit_replaced_stores() is called after a clean pass.
 * Never holds rec->mu; heap-allocates store_info array to keep thread stacks small. */
static void check_stores(struct sreconcile *rec, uint64_t *down_mask_out, uint64_t *replace_mask_out,
                         uint64_t *out_mask_out, uint64_t *replacing_mask_out, uint64_t *draining_mask_out,
                         bool *all_down_out)
{
    *down_mask_out = 0;
    *replace_mask_out = 0;
    if (out_mask_out)
        *out_mask_out = 0;
    if (replacing_mask_out)
        *replacing_mask_out = 0;
    if (draining_mask_out)
        *draining_mask_out = 0;
    *all_down_out = false;
    if (!rec->ops.store_count || !rec->ops.store_info)
        return;

    int n = rec->ops.store_count(rec->ops.arg);
    if (n <= 0) {
        *all_down_out = true;
        return;
    }
    if (n > (int)SRECONCILE_STORE_MAX)
        n = (int)SRECONCILE_STORE_MAX;

    struct sreconcile_store_info *infos = calloc((size_t)n, sizeof *infos);
    if (!infos)
        return;
    for (int i = 0; i < n; i++) {
        if (rec->ops.store_info(rec->ops.arg, i, &infos[i]) != 0) {
            infos[i].state = CHUNK_STORE_DOWN_MISSING;
            infos[i].disk_ok = false;
        }
    }

    int nup = 0;
    bool dirty = false;
    pthread_mutex_lock(&rec->store_mu);
    stores_load_locked(rec);
    for (int i = 0; i < n; i++) {
        const struct sreconcile_store_info *si = &infos[i];
        const char *spath = si->path[0] ? si->path : "<unknown>";
        struct rec_store_ent *ent = store_ent_find_or_add_locked(rec, spath);
        if (ent)
            ent->store_idx = i;
        if (si->is_out) {
            if (out_mask_out)
                *out_mask_out |= (1ull << (unsigned)i);
            continue;
        }
        if (si->is_draining && draining_mask_out)
            *draining_mask_out |= (1ull << (unsigned)i);
        bool up = (si->state == CHUNK_STORE_UP || si->state == CHUNK_STORE_REPLACING) && si->disk_ok &&
                  !id_is_zero(si->live_id);
        if (!up) {
            *down_mask_out |= (1ull << (unsigned)i);
            if (!ent || !ent->logged_down) {
                char exp_hex[33] = "none";
                if (ent && ent->has_expected)
                    chunk_oid_hex(ent->expected_id, exp_hex);
                fprintf(stderr,
                        "tsdbd: reconcile: store %s not answering (state %s, expected id %s, got none): "
                        "treating all chunks on it as missing\n",
                        spath, chunk_store_state_name(si->state), exp_hex);
                if (ent)
                    ent->logged_down = true;
                REC_STAT(rec, store_down_events);
            }
            continue;
        }
        if (si->state == CHUNK_STORE_REPLACING && replacing_mask_out)
            *replacing_mask_out |= (1ull << (unsigned)i);

        const uint8_t *got_id = si->live_id;
        bool disk_diverged = memcmp(si->disk_id, si->live_id, 16) != 0;
        if (disk_diverged) {
            /* On-disk META changed underneath an open store handle while tsdbd is still running: log once and treat
             * as non-writable until a restart reopens the replacement store (never force-refill into the old handle). */
            got_id = si->disk_id;
            *down_mask_out |= (1ull << (unsigned)i);
            if (!ent || memcmp(ent->last_bad_id, got_id, 16) != 0) {
                char exp_hex[33] = "none", got_hex[33] = "none";
                chunk_oid_hex(si->live_id, exp_hex);
                chunk_oid_hex(got_id, got_hex);
                fprintf(stderr,
                        "tsdbd: reconcile: store %s answered with another store id on disk (open %s, disk %s): "
                        "waiting for restart to reopen replacement store\n",
                        spath, exp_hex, got_hex);
                if (ent)
                    memcpy(ent->last_bad_id, got_id, 16);
                REC_STAT(rec, store_id_mismatches);
            }
            continue;
        }

        bool mismatch = ent && ent->has_expected && memcmp(si->live_id, ent->expected_id, 16) != 0;
        if (mismatch) {
            *replace_mask_out |= (1ull << (unsigned)i);
            if (memcmp(ent->last_bad_id, got_id, 16) != 0) {
                char exp_hex[33] = "none", got_hex[33] = "none";
                chunk_oid_hex(ent->expected_id, exp_hex);
                chunk_oid_hex(got_id, got_hex);
                fprintf(stderr,
                        "tsdbd: reconcile: store %s answered with another store id (expected %s, got %s): "
                        "treating all chunks on it as missing\n",
                        spath, exp_hex, got_hex);
                memcpy(ent->last_bad_id, got_id, 16);
                REC_STAT(rec, store_id_mismatches);
            }
            ent->logged_down = false;
            /* Hold new store_id pending until all positions using this store finish refilling cleanly. */
            memcpy(ent->pending_new_id, si->live_id, 16);
            ent->has_pending_new_id = true;
            nup++;
            continue;
        }

        if (ent) {
            ent->logged_down = false;
            ent->has_pending_new_id = false;
            memset(ent->last_bad_id, 0, 16);
            if (!ent->has_expected && !rec->stores_damaged) {
                memcpy(ent->expected_id, si->live_id, 16);
                ent->has_expected = true;
                dirty = true;
            }
        }
        nup++;
    }
    if (dirty)
        stores_save_locked(rec);
    pthread_mutex_unlock(&rec->store_mu);
    free(infos);

    if (nup == 0)
        *all_down_out = true;
}

/* Commit pending replacement store_ids to <state-dir>/chunk_stores for every store index whose bit is NO LONGER
 * referenced by any unfinished reconcile job in `rec->jobs`. */
static void commit_replaced_stores(struct sreconcile *rec, uint64_t completed_mask)
{
    if (completed_mask == 0)
        return;
    uint64_t still_needed = 0;
    pthread_mutex_lock(&rec->mu);
    for (struct rec_job *j = rec->jobs; j; j = j->next) {
        if (!j->removed && (j->pending_run || j->running || j->retrigger || j->waiting_store_up || j->waiting_room ||
                            j->page_pending > 0))
            still_needed |= j->replace_mask;
    }
    pthread_mutex_unlock(&rec->mu);

    uint64_t ready = completed_mask & ~still_needed;
    if (ready == 0)
        return;

    pthread_mutex_lock(&rec->store_mu);
    bool dirty = false;
    for (size_t i = 0; i < rec->nstores; i++) {
        struct rec_store_ent *ent = &rec->stores[i];
        if (!ent->has_pending_new_id || ent->store_idx < 0 || ent->store_idx >= 64)
            continue;
        if (!(ready & (1ull << (unsigned)ent->store_idx)))
            continue;
        memcpy(ent->expected_id, ent->pending_new_id, 16);
        ent->has_expected = true;
        ent->has_pending_new_id = false;
        memset(ent->last_bad_id, 0, 16);
        dirty = true;
    }
    if (dirty)
        stores_save_locked(rec);
    pthread_mutex_unlock(&rec->store_mu);
}

static void settle_replacing_stores(struct sreconcile *rec, uint64_t completed_mask)
{
    if (completed_mask == 0 || !rec->ops.on_replacing_settled)
        return;
    uint64_t still_needed = 0;
    pthread_mutex_lock(&rec->mu);
    for (struct rec_job *j = rec->jobs; j; j = j->next) {
        if (!j->removed && (j->pending_run || j->running || j->retrigger || j->waiting_store_up || j->waiting_room ||
                            j->page_pending > 0))
            still_needed |= j->replacing_mask;
    }
    pthread_mutex_unlock(&rec->mu);

    uint64_t ready = completed_mask & ~still_needed;
    if (ready != 0)
        rec->ops.on_replacing_settled(rec->ops.arg, ready);
}

/* ------------------------------------------------------------------ HBS_FILL reporting */

static void publish_reports(struct sreconcile *rec)
{
    if (!rec->ops.on_report)
        return;
    struct sfill_report buf[256];
    pthread_mutex_lock(&rec->pub_mu);
    uint32_t n = sreconcile_report(rec, buf, 256);
    rec->ops.on_report(rec->ops.arg, buf, n);
    pthread_mutex_unlock(&rec->pub_mu);
}

/* ------------------------------------------------------------------ sorting helpers */

static int clist_oid_cmp(const void *a, const void *b)
{
    const chunk_listed *x = a;
    const chunk_listed *y = b;
    int c = memcmp(x->oid, y->oid, 16);
    if (c != 0)
        return c;
    return (x->store > y->store) - (x->store < y->store);
}

static int oid16_cmp(const void *a, const void *b)
{
    return memcmp(a, b, 16);
}

/* Returns first matching local chunk entry for `oid` (may have >1 entry if present on multiple stores). */
static const chunk_listed *find_local_chunk(const chunk_listed *cs, size_t n, const uint8_t oid[16], size_t *idx_out)
{
    size_t lo = 0, hi = n;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        int c = memcmp(cs[mid].oid, oid, 16);
        if (c < 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    if (lo < n && memcmp(cs[lo].oid, oid, 16) == 0) {
        if (idx_out)
            *idx_out = lo;
        return &cs[lo];
    }
    return NULL;
}

/* ------------------------------------------------------------------ job list management (rec->mu held) */

static void gc_removed_jobs_locked(struct sreconcile *rec)
{
    for (struct rec_job **pp = &rec->jobs; *pp;) {
        struct rec_job *j = *pp;
        if (j->removed && !j->running && j->page_pending == 0) {
            *pp = j->next;
            if (j->pace_id) {
                pace_job_end_ex(PACE_RECV, j->pace_id);
                j->pace_id = 0;
            }
            free(j);
        } else {
            pp = &j->next;
        }
    }
}

static struct rec_job *find_job_locked(struct sreconcile *rec, uint32_t volume, uint32_t pos)
{
    for (struct rec_job *j = rec->jobs; j; j = j->next)
        if (!j->removed && j->volume == volume && j->pos == pos)
            return j;
    return NULL;
}

/* ------------------------------------------------------------------ sfill task cancel & completion callbacks */

static bool rec_task_cancelled(void *arg)
{
    const struct rec_task_ctx *tctx = arg;
    struct sreconcile *rec = tctx->rec;
    if (atomic_load_explicit(&rec->stopping, memory_order_acquire))
        return true;
    pthread_mutex_lock(&rec->mu);
    bool c = tctx->job->removed || tctx->job->gen != tctx->gen;
    pthread_mutex_unlock(&rec->mu);
    return c;
}

static void on_task_done(void *arg, int rc)
{
    struct rec_task_ctx *tctx = arg;
    struct sreconcile *rec = tctx->rec;
    struct rec_job *j = tctx->job;
    uint64_t gen = tctx->gen;
    uint64_t bytes = tctx->chunk_bytes;
    free(tctx);

    bool should_publish = false;
    pthread_mutex_lock(&rec->mu);
    if (rec->total_inflight > 0)
        rec->total_inflight--;
    if (rec->total_inflight == 0)
        pthread_cond_broadcast(&rec->done_cv);

    if (j->page_pending > 0)
        j->page_pending--;

    if (!j->removed && j->gen == gen) {
        if (rc == 0) {
            j->done++;
            j->pass_bytes += bytes;
            REC_STAT(rec, filled);
            REC_STATN(rec, bytes_filled, bytes);
            should_publish = true;
        } else {
            j->page_errors++;
            j->failures++;
            REC_STAT(rec, failed);
        }
    }
    if (j->page_pending == 0)
        pthread_cond_broadcast(&rec->page_cv);
    bool stopping = atomic_load_explicit(&rec->stopping, memory_order_acquire);
    gc_removed_jobs_locked(rec);
    pthread_mutex_unlock(&rec->mu);

    if (should_publish && !stopping)
        publish_reports(rec);
}

/* ------------------------------------------------------------------ paged local CHUNK_LIST fetch */

static int fetch_local_chunks(struct sreconcile *rec, uint32_t volume, uint32_t pos,
                              chunk_listed **out_items, size_t *out_n)
{
    *out_items = NULL;
    *out_n = 0;
    if (!rec->ops.local_list)
        return TSDB_ERR_UNAVAILABLE;

    chunk_listed *all = NULL;
    size_t nall = 0, cap = 0;
    bool has_cursor = false;
    uint8_t coid[16] = {0};
    uint16_t cpos = 0;

    for (;;) {
        chunk_listed *page = NULL;
        uint32_t got = 0;
        bool more = false;
        int rc = rec->ops.local_list(rec->ops.arg, volume, has_cursor, coid, cpos, CS_LIST_MAX, &page, &got, &more);
        if (rc != 0 || (got == 0 && more)) {
            free(page);
            free(all);
            return rc != 0 ? rc : TSDB_ERR_CORRUPT;
        }
        if (got > 0) {
            memcpy(coid, page[got - 1].oid, 16);
            cpos = page[got - 1].pos;
            has_cursor = true;
            for (uint32_t i = 0; i < got; i++) {
                if (page[i].pos != pos)
                    continue;
                if (nall == cap) {
                    size_t nc = cap ? cap * 2 : 64;
                    chunk_listed *na = realloc(all, nc * sizeof *na);
                    if (!na) {
                        free(page);
                        free(all);
                        return TSDB_ERR_NOMEM;
                    }
                    all = na;
                    cap = nc;
                }
                all[nall++] = page[i];
            }
        }
        free(page);
        if (!more)
            break;
    }
    if (nall > 1)
        qsort(all, nall, sizeof *all, clist_oid_cmp);
    *out_items = all;
    *out_n = nall;
    return 0;
}

static bool pass_cancelled(struct sreconcile *rec, struct rec_job *j, uint64_t gen)
{
    if (atomic_load_explicit(&rec->stopping, memory_order_acquire))
        return true;
    pthread_mutex_lock(&rec->mu);
    bool c = j->removed || j->gen != gen;
    pthread_mutex_unlock(&rec->mu);
    return c;
}

static void schedule_retry_locked(struct sreconcile *rec, struct rec_job *j, uint64_t gen, bool count_err)
{
    j->running = false;
    if (j->pace_id) {
        pace_job_end_ex(PACE_RECV, j->pace_id);
        j->pace_id = 0;
    }
    j->reporting = false;
    if (!j->removed && j->gen == gen && !atomic_load_explicit(&rec->stopping, memory_order_acquire)) {
        if (count_err) {
            j->failures++;
            REC_STAT(rec, passes_err);
        }
        j->pending_run = true;
        j->retry_at_ms = mono_ms() + retry_delay_ms(j->backoff_shift);
        if (j->backoff_shift < 10u)
            j->backoff_shift++;
    }
    gc_removed_jobs_locked(rec);
}

/* ------------------------------------------------------------------ paged streaming reconcile pass */

static void run_reconcile_pass(struct sreconcile *rec, struct rec_job *j, uint64_t gen,
                               uint32_t volume, uint32_t pos)
{
    struct sreconcile_vol_view vv;
    memset(&vv, 0, sizeof vv);
    if (!rec->ops.vol_view || rec->ops.vol_view(rec->ops.arg, volume, pos, &vv) != 0 || !vv.held_whole) {
        pthread_mutex_lock(&rec->mu);
        j->running = false;
        if (!j->removed && j->gen == gen) {
            j->removed = true;
            j->pending_run = false;
            j->waiting_store_up = false;
            j->down_wait_mask = 0;
            j->retrigger = false;
            j->reporting = false;
            j->replace_mask = 0;
            j->backoff_shift = 0;
        }
        gc_removed_jobs_locked(rec);
        pthread_mutex_unlock(&rec->mu);
        publish_reports(rec);
        return;
    }

    /* 1. Check local store health/ids. Keep replace_mask/replacing_mask persisted in `j` so retries never lose them. */
    uint64_t down_mask = 0, cur_replace_mask = 0, out_mask = 0, cur_replacing_mask = 0, draining_mask = 0;
    bool all_down = false;
    check_stores(rec, &down_mask, &cur_replace_mask, &out_mask, &cur_replacing_mask, &draining_mask, &all_down);

    uint64_t replace_mask = 0;
    pthread_mutex_lock(&rec->mu);
    if (!j->removed && j->gen == gen) {
        j->replace_mask |= cur_replace_mask;
        j->replacing_mask |= cur_replacing_mask;
        replace_mask = j->replace_mask;
        j->stripe_version = vv.stripe_version;
        j->done = 0;
        j->total = 0;
        j->page_errors = 0;
        j->pass_bytes = 0;
    }
    pthread_mutex_unlock(&rec->mu);

    /* If NO local store is writable, skip OBJLIST/CHUNK_LIST and wait until a store transitions back UP. */
    if (all_down) {
        pthread_mutex_lock(&rec->mu);
        j->running = false;
        j->reporting = false;
        if (!j->removed && j->gen == gen && !atomic_load_explicit(&rec->stopping, memory_order_acquire)) {
            j->waiting_store_up = true;
            j->down_wait_mask = down_mask;
            j->backoff_shift = 0;
        }
        gc_removed_jobs_locked(rec);
        pthread_mutex_unlock(&rec->mu);
        return;
    }

    /* 2. Fetch local CHUNK_LIST for this (volume, pos). A transient error retries with backoff (never forces all). */
    chunk_listed *local = NULL;
    size_t nlocal = 0;
    int rc = fetch_local_chunks(rec, volume, pos, &local, &nlocal);
    if (rc != 0) {
        pthread_mutex_lock(&rec->mu);
        schedule_retry_locked(rec, j, gen, true);
        pthread_mutex_unlock(&rec->mu);
        return;
    }

    int mut = atomic_load_explicit(&sreconcile_mutant, memory_order_relaxed);
    uint8_t vuuid[16] = {0};
    bool has_vuuid = false;

    /* Compact 16-byte oid vector of all leader objects seen across all pages (for Direction 2). */
    uint8_t (*leader_oids)[16] = NULL;
    size_t nleader_oids = 0, cap_leader_oids = 0;

    uint64_t already_present = 0;
    uint64_t skipped_down = 0;
    uint64_t skipped_no_room = 0;
    uint64_t total_queued = 0;
    bool pass_failed = false;
    bool lost_position = false;

    bool has_cursor = false;
    uint8_t cursor[16] = {0};

    /* 3. Stream full OBJLIST records (with sums, max_version = 0) page-by-page so heavy object payloads stay
     * bounded to one page while keeping compact 16-byte leader_oids for Direction 2. */
    for (;;) {
        if (pass_cancelled(rec, j, gen)) {
            pass_failed = true;
            break;
        }
        /* Re-check position ownership per page so a position transferred away mid-pass stops immediately. */
        if (has_cursor) {
            struct sreconcile_vol_view cur_vv;
            memset(&cur_vv, 0, sizeof cur_vv);
            if (rec->ops.vol_view(rec->ops.arg, volume, pos, &cur_vv) != 0 || !cur_vv.held_whole) {
                lost_position = true;
                break;
            }
            vv = cur_vv;
        }
        uint8_t vu[16] = {0};
        struct sfill_obj *page = NULL;
        uint32_t got = 0;
        bool more = false;
        rc = rec->ops.objlist(rec->ops.arg, volume, 0, has_cursor, cursor, SRECONCILE_OBJLIST_PAGE, vu, &page, &got,
                              &more);
        if (rc != 0 || (got == 0 && more)) {
            sfill_objs_free(page, got);
            pass_failed = true;
            break;
        }
        if (!has_vuuid && !id_is_zero(vu)) {
            memcpy(vuuid, vu, 16);
            has_vuuid = true;
        }
        if (got == 0) {
            sfill_objs_free(page, got);
            break;
        }
        memcpy(cursor, page[got - 1].oid, 16);
        has_cursor = true;

        /* Record compact 16-byte oids for Direction 2. */
        if (nleader_oids + got > cap_leader_oids) {
            size_t nc = cap_leader_oids ? cap_leader_oids * 2 : 128;
            while (nc < nleader_oids + got)
                nc *= 2;
            uint8_t (*nl)[16] = realloc(leader_oids, nc * 16);
            if (!nl) {
                sfill_objs_free(page, got);
                pass_failed = true;
                break;
            }
            leader_oids = nl;
            cap_leader_oids = nc;
        }
        for (uint32_t i = 0; i < got; i++)
            memcpy(leader_oids[nleader_oids++], page[i].oid, 16);

        /* Diff this page against local CHUNK_LIST. */
        uint32_t *miss_idx = malloc(got * sizeof *miss_idx);
        bool *miss_force = calloc(got, sizeof *miss_force);
        int *miss_target = malloc(got * sizeof *miss_target);
        uint32_t nmiss = 0;
        if (!miss_idx || !miss_force || !miss_target) {
            free(miss_idx);
            free(miss_force);
            free(miss_target);
            sfill_objs_free(page, got);
            pass_failed = true;
            break;
        }

        /* Cache pick_target_store(out_src) once per lost slot per OBJLIST page to avoid per-chunk statvfs calls. */
        int page_target[64];
        for (int s = 0; s < 64; s++)
            page_target[s] = -2;

        for (uint32_t i = 0; i < got; i++) {
            const struct sfill_obj *o = &page[i];
            if (pos >= chunk_npos(&o->g))
                continue; /* E7: small object has no chunk at this position */

            int placed = rec->ops.place_store ? rec->ops.place_store(rec->ops.arg, o->oid, pos) : -1;
            if (placed >= 0 && placed < 64 && (down_mask & (1ull << (unsigned)placed)) != 0) {
                /* Store is currently DOWN/non-writable: account as missing-on-down, do not spam sfill. */
                skipped_down++;
                continue;
            }
            bool on_out = placed >= 0 && placed < 64 && (out_mask & (1ull << (unsigned)placed)) != 0;
            bool on_replaced = placed >= 0 && placed < 64 && (replace_mask & (1ull << (unsigned)placed)) != 0;

            bool intact = false;
            if (!on_replaced && nlocal > 0) {
                size_t lidx = 0;
                if (find_local_chunk(local, nlocal, o->oid, &lidx)) {
                    uint64_t want_sz = chunk_file_size(&o->g, pos);
                    for (size_t k = lidx; k < nlocal && memcmp(local[k].oid, o->oid, 16) == 0; k++) {
                        const chunk_listed *c = &local[k];
                        if (c->store >= 0 && c->store < 64 &&
                            ((down_mask | replace_mask | out_mask) & (1ull << (unsigned)c->store)) != 0)
                            continue;
                        if (c->status != CHUNK_ST_OK)
                            continue;
                        if (c->epoch != o->writer_epoch)
                            continue;
                        if (has_vuuid && !id_is_zero(c->vuuid) && memcmp(c->vuuid, vuuid, 16) != 0)
                            continue;
                        if (c->size != 0 && c->size != want_sz)
                            continue;
                        intact = true;
                        break;
                    }
                }
            }

            if (intact && mut != SRECONCILE_MUT_FULLCOPY) {
                already_present++;
                continue;
            }
            if (!intact && mut == SRECONCILE_MUT_SKIPMISSING)
                continue; /* planted mutant `backskipmissing` */

            int target_store = -1;
            int out_src = -1;
            if (on_out) {
                out_src = placed;
            } else if (!on_replaced && out_mask != 0 && placed >= 0 && placed < 64 &&
                       (draining_mask & (1ull << (unsigned)placed)) != 0) {
                /* A chunk whose hash slot is draining fell back onto another store at settle time and may have lived
                 * on the OUT store: route its rebuild through pick_target_store so E50 capacity rules apply. */
                out_src = __builtin_ctzll(out_mask);
            }
            if (out_src >= 0 && out_src < 64) {
                if (page_target[out_src] == -2) {
                    page_target[out_src] =
                        rec->ops.pick_target_store ? rec->ops.pick_target_store(rec->ops.arg, out_src) : -1;
                }
                target_store = page_target[out_src];
                if (target_store < 0) {
                    /* No local capacity store has room: park on waiting_room (R9 placement evicts off-node). */
                    skipped_no_room++;
                    continue;
                }
            }

            miss_idx[nmiss] = i;
            miss_force[nmiss] = on_replaced || out_src >= 0 || (mut == SRECONCILE_MUT_FULLCOPY);
            miss_target[nmiss] = target_store;
            nmiss++;
        }

        /* Best-effort pre-enqueue filter: skip oids already on the leader's committed MK_GC pending list so
         * reconcile does not needlessly reconstruct already-retired objects. (Any narrow race where MK_GC +
         * MK_GC_DONE finishes after this look is bounded by obsolete_part_lifetime_s and swept by r8k's E14 GC.) */
        if (nmiss > 0 && rec->ops.gc_pending) {
            uint8_t (*gcp)[16] = NULL;
            uint32_t ngcp = 0;
            bool trunc = false;
            if (rec->ops.gc_pending(rec->ops.arg, volume, &gcp, &ngcp, &trunc) == 0 && ngcp > 0) {
                if (ngcp > 1)
                    qsort(gcp, ngcp, 16, oid16_cmp);
                uint32_t keep = 0;
                for (uint32_t i = 0; i < nmiss; i++) {
                    if (bsearch(page[miss_idx[i]].oid, gcp, ngcp, 16, oid16_cmp) != NULL)
                        continue;
                    miss_idx[keep] = miss_idx[i];
                    miss_force[keep] = miss_force[i];
                    miss_target[keep] = miss_target[i];
                    keep++;
                }
                nmiss = keep;
            }
            free(gcp);
        }

        /* Enqueue this page's missing chunks onto sfill and wait for this page to drain. */
        if (nmiss > 0) {
            pthread_mutex_lock(&rec->mu);
            if (atomic_load_explicit(&rec->stopping, memory_order_acquire) || j->removed || j->gen != gen) {
                pthread_mutex_unlock(&rec->mu);
                free(miss_idx);
                free(miss_force);
                free(miss_target);
                sfill_objs_free(page, got);
                pass_failed = true;
                break;
            }
            j->total += nmiss;
            j->page_pending = nmiss;
            j->reporting = true;
            if (!j->pace_id)
                j->pace_id = pace_job_begin_ex(PACE_RECV, vv.margin, 0);
            rec->total_inflight += nmiss;
            pthread_mutex_unlock(&rec->mu);

            total_queued += nmiss;
            REC_STATN(rec, missing_queued, nmiss);
            publish_reports(rec);

            for (uint32_t i = 0; i < nmiss; i++) {
                struct sfill_obj *o = &page[miss_idx[i]];
                struct rec_task_ctx *tctx = calloc(1, sizeof *tctx);
                if (!tctx) {
                    pthread_mutex_lock(&rec->mu);
                    if (rec->total_inflight > 0)
                        rec->total_inflight--;
                    if (rec->total_inflight == 0)
                        pthread_cond_broadcast(&rec->done_cv);
                    if (j->page_pending > 0)
                        j->page_pending--;
                    j->page_errors++;
                    j->failures++;
                    if (j->page_pending == 0)
                        pthread_cond_broadcast(&rec->page_cv);
                    pthread_mutex_unlock(&rec->mu);
                    continue;
                }
                tctx->rec = rec;
                tctx->job = j;
                tctx->gen = gen;
                tctx->chunk_bytes = chunk_pos_len(&o->g, pos);
                if (sfill_enqueue_obj_store(rec->sf, volume, pos, 0, vv.margin, vv.concurrency, vuuid, o,
                                            miss_force[i], miss_target[i], rec_task_cancelled, on_task_done,
                                            tctx) != 0) {
                    on_task_done(tctx, -1);
                }
            }

            free(miss_idx);
            free(miss_force);
            free(miss_target);
            sfill_objs_free(page, got);

            /* Wait until this page's tasks complete (or stop/cancel). */
            pthread_mutex_lock(&rec->mu);
            while (j->page_pending > 0 && !atomic_load_explicit(&rec->stopping, memory_order_acquire) &&
                   !j->removed && j->gen == gen) {
                pthread_cond_wait(&rec->page_cv, &rec->mu);
            }
            bool had_errs = j->page_errors > 0 || j->removed || j->gen != gen ||
                            atomic_load_explicit(&rec->stopping, memory_order_acquire);
            pthread_mutex_unlock(&rec->mu);
            if (had_errs) {
                pass_failed = true;
                break;
            }
        } else {
            free(miss_idx);
            free(miss_force);
            free(miss_target);
            sfill_objs_free(page, got);
        }

        if (!more)
            break;
    }

    /* 4. Direction 2 (only when OBJLIST streamed cleanly to EOF and position is still held): CHUNK_LIST \ leader_oids. */
    uint64_t gc_pending_cnt = 0;
    uint64_t orphans_cnt = 0;
    if (!pass_failed && !lost_position && nlocal > 0) {
        if (nleader_oids > 1)
            qsort(leader_oids, nleader_oids, 16, oid16_cmp);

        uint8_t (*extra)[16] = NULL;
        size_t nextra = 0, cap_extra = 0;
        for (size_t i = 0; i < nlocal; i++) {
            if (local[i].status != CHUNK_ST_OK)
                continue;
            if (has_vuuid && !id_is_zero(local[i].vuuid) && memcmp(local[i].vuuid, vuuid, 16) != 0)
                continue;
            if (nextra > 0 && memcmp(extra[nextra - 1], local[i].oid, 16) == 0)
                continue;
            if (nleader_oids > 0 && bsearch(local[i].oid, leader_oids, nleader_oids, 16, oid16_cmp) != NULL)
                continue;
            if (nextra == cap_extra) {
                size_t nc = cap_extra ? cap_extra * 2 : 16;
                uint8_t (*ne)[16] = realloc(extra, nc * 16);
                if (!ne)
                    break;
                extra = ne;
                cap_extra = nc;
            }
            memcpy(extra[nextra++], local[i].oid, 16);
        }

        if (nextra > 0 && rec->ops.gc_pending) {
            uint8_t (*gcp)[16] = NULL;
            uint32_t ngcp = 0;
            bool truncated = false;
            if (rec->ops.gc_pending(rec->ops.arg, volume, &gcp, &ngcp, &truncated) == 0) {
                if (ngcp > 1)
                    qsort(gcp, ngcp, 16, oid16_cmp);
                if (truncated) {
                    fprintf(stderr,
                            "tsdbd: reconcile: volume %" PRIu32 " pos %" PRIu32
                            ": leader MK_GC pending list truncated at %" PRIu32 " entries; deferring orphan report\n",
                            volume, pos, ngcp);
                }
                for (size_t i = 0; i < nextra; i++) {
                    if (ngcp > 0 && bsearch(extra[i], gcp, ngcp, 16, oid16_cmp) != NULL) {
                        gc_pending_cnt++;
                    } else if (!truncated) {
                        /* Re-check local disk after gc_pending to eliminate the race where leader GC deleted
                         * the chunk and committed MK_GC_DONE between CHUNK_LIST and CS_OP_GCPENDING. */
                        if (rec->ops.local_exists && !rec->ops.local_exists(rec->ops.arg, volume, extra[i], pos))
                            continue;
                        orphans_cnt++;
                        char hex[33];
                        chunk_oid_hex(extra[i], hex);
                        fprintf(stderr,
                                "tsdbd: reconcile: volume %" PRIu32 " pos %" PRIu32
                                ": orphan chunk %s (neither in OBJLIST nor pending MK_GC; left for MK_GC)\n",
                                volume, pos, hex);
                    }
                }
            }
            free(gcp);
        }
        free(extra);
    }

    free(leader_oids);
    free(local);

    REC_STATN(rec, already_present, already_present);
    REC_STATN(rec, missing_on_down_store, skipped_down);
    REC_STATN(rec, gc_pending, gc_pending_cnt);
    REC_STATN(rec, orphans_reported, orphans_cnt);

    /* 5. Finalize pass, update HBS_FILL, and persist replacement store_id only if everything succeeded cleanly. */
    uint64_t completed_replace = 0;
    uint64_t completed_replacing = 0;
    uint64_t l_done = 0, l_tot = 0, l_bytes = 0;
    bool log_done = false;
    bool was_reporting = false;

    pthread_mutex_lock(&rec->mu);
    was_reporting = j->reporting;
    if (j->pace_id) {
        pace_job_end_ex(PACE_RECV, j->pace_id);
        j->pace_id = 0;
    }
    j->reporting = false;

    if (lost_position) {
        j->running = false;
        if (!j->removed && j->gen == gen) {
            j->removed = true;
            j->pending_run = false;
            j->waiting_store_up = false;
            j->waiting_room = false;
            j->down_wait_mask = 0;
            j->room_wait_out_mask = 0;
            j->retrigger = false;
            j->replace_mask = 0;
            j->replacing_mask = 0;
            j->backoff_shift = 0;
        }
        gc_removed_jobs_locked(rec);
    } else if (pass_failed) {
        schedule_retry_locked(rec, j, gen, true);
    } else if (skipped_down > 0 || skipped_no_room > 0) {
        /* Healthy/UP stores reconciled cleanly: commit any replaced UP stores immediately, and park this job until a
         * bit in down_mask transitions back UP or local room opens up for room_wait_out_mask (no OBJLIST storm). */
        j->running = false;
        if (!j->removed && j->gen == gen) {
            completed_replace = j->replace_mask & ~down_mask;
            completed_replacing = j->replacing_mask & ~down_mask;
            j->replace_mask &= down_mask;
            j->replacing_mask &= down_mask;
            j->backoff_shift = 0;
            j->waiting_store_up = (skipped_down > 0);
            j->down_wait_mask = skipped_down > 0 ? down_mask : 0;
            j->waiting_room = (skipped_no_room > 0);
            j->room_wait_out_mask = skipped_no_room > 0 ? out_mask : 0;
            if (j->retrigger) {
                j->retrigger = false;
                j->pending_run = true;
                j->retry_at_ms = 0;
                pthread_cond_signal(&rec->cv);
            }
        }
        gc_removed_jobs_locked(rec);
    } else {
        j->running = false;
        if (!j->removed && j->gen == gen) {
            completed_replace = j->replace_mask;
            completed_replacing = j->replacing_mask;
            j->replace_mask = 0;
            j->replacing_mask = 0;
            j->waiting_store_up = false;
            j->waiting_room = false;
            j->down_wait_mask = 0;
            j->room_wait_out_mask = 0;
            j->backoff_shift = 0;
            l_done = j->done;
            l_tot = j->total;
            l_bytes = j->pass_bytes;
            REC_STAT(rec, passes_ok);
            log_done = (total_queued > 0 || already_present > 0 || gc_pending_cnt > 0 || orphans_cnt > 0 ||
                        replace_mask != 0);
            if (j->retrigger) {
                j->retrigger = false;
                j->pending_run = true;
                j->retry_at_ms = 0;
                pthread_cond_signal(&rec->cv);
            }
        }
        gc_removed_jobs_locked(rec);
    }
    bool stopping = atomic_load_explicit(&rec->stopping, memory_order_acquire);
    pthread_mutex_unlock(&rec->mu);

    if (completed_replace != 0)
        commit_replaced_stores(rec, completed_replace);
    if (completed_replacing != 0)
        settle_replacing_stores(rec, completed_replacing);

    if (log_done) {
        fprintf(stderr,
                "tsdbd: reconcile: volume %" PRIu32 " pos %" PRIu32 " version %" PRIu64
                ": complete (%" PRIu64 "/%" PRIu64 " missing filled, %" PRIu64 " bytes, %" PRIu64
                " already present, %" PRIu64 " gc_pending, %" PRIu64 " orphans)\n",
                volume, pos, vv.stripe_version, l_done, l_tot, l_bytes, already_present, gc_pending_cnt, orphans_cnt);
    }
    if ((was_reporting || total_queued > 0) && !stopping)
        publish_reports(rec);
}

/* ------------------------------------------------------------------ coordinator thread */

static void trigger_all_held(struct sreconcile *rec);

static void drain_noted_limited(struct sreconcile *rec, size_t max_nudges)
{
    if (!rec || !rec->ops.notify_holder)
        return;
    if (atomic_load_explicit(&missing_pending_cnt, memory_order_acquire) == 0)
        return;

    /* Snapshot exact ring entries (idx, volume, pos, oid) so clearing after a successful nudge NEVER drops a new
     * note recorded concurrently after the snapshot was taken. */
    struct {
        uint32_t volume;
        uint32_t pos;
    } pairs[64];
    struct {
        size_t idx;
        uint32_t volume;
        uint32_t pos;
        uint8_t oid[16];
    } snaps[256];
    size_t npairs = 0;
    size_t nsnaps = 0;
    size_t cap = max_nudges < 64 ? max_nudges : 64;

    pthread_mutex_lock(&missing_mu);
    size_t count = missing_head < SRECONCILE_MISSING_CAP ? missing_head : SRECONCILE_MISSING_CAP;
    for (size_t i = 0; i < count; i++) {
        if (!missing_ring[i].pending)
            continue;
        bool found = false;
        for (size_t k = 0; k < npairs; k++) {
            if (pairs[k].volume == missing_ring[i].volume && pairs[k].pos == missing_ring[i].pos) {
                found = true;
                break;
            }
        }
        if (!found) {
            if (npairs >= cap)
                continue;
            pairs[npairs].volume = missing_ring[i].volume;
            pairs[npairs].pos = missing_ring[i].pos;
            npairs++;
        }
        if (nsnaps < 256) {
            snaps[nsnaps].idx = i;
            snaps[nsnaps].volume = missing_ring[i].volume;
            snaps[nsnaps].pos = missing_ring[i].pos;
            memcpy(snaps[nsnaps].oid, missing_ring[i].oid, 16);
            nsnaps++;
        }
    }
    pthread_mutex_unlock(&missing_mu);

    for (size_t i = 0; i < npairs; i++) {
        if (atomic_load_explicit(&rec->stopping, memory_order_acquire))
            break;
        if (rec->ops.notify_holder(rec->ops.arg, pairs[i].volume, pairs[i].pos) > 0) {
            uint64_t cleared = 0;
            pthread_mutex_lock(&missing_mu);
            for (size_t s = 0; s < nsnaps; s++) {
                if (snaps[s].volume != pairs[i].volume || snaps[s].pos != pairs[i].pos)
                    continue;
                size_t idx = snaps[s].idx;
                if (missing_ring[idx].pending && missing_ring[idx].volume == snaps[s].volume &&
                    missing_ring[idx].pos == snaps[s].pos &&
                    memcmp(missing_ring[idx].oid, snaps[s].oid, 16) == 0) {
                    missing_ring[idx].pending = false;
                    cleared++;
                }
            }
            pthread_mutex_unlock(&missing_mu);
            if (cleared > 0)
                atomic_fetch_sub_explicit(&missing_pending_cnt, cleared, memory_order_acq_rel);
            REC_STAT(rec, holder_nudges);
        }
    }
}

/* Lightweight local store-state poll for jobs parked on `waiting_store_up` or `waiting_room`. Never issues OBJLIST or
 * CHUNK_LIST: wakes a parked job ONLY when a DOWN store it waited on comes back UP, or when local capacity room opens
 * up (or the OUT store mask changes) for a job parked on `waiting_room`. */
static void poll_down_stores(struct sreconcile *rec)
{
    uint64_t cur_down = 0, cur_replace = 0, cur_out = 0, cur_replacing = 0, cur_draining = 0;
    bool all_down = false;
    check_stores(rec, &cur_down, &cur_replace, &cur_out, &cur_replacing, &cur_draining, &all_down);

    /* For any waiting_room job, probe pick_target_store cheaply without holding rec->mu. */
    uint64_t room_wait_union = 0;
    pthread_mutex_lock(&rec->mu);
    for (struct rec_job *j = rec->jobs; j; j = j->next) {
        if (!j->removed && !j->running && !j->pending_run && j->waiting_room)
            room_wait_union |= j->room_wait_out_mask;
    }
    pthread_mutex_unlock(&rec->mu);

    uint64_t room_ready_mask = 0;
    if (room_wait_union != 0 && rec->ops.pick_target_store) {
        for (int s = 0; s < 64; s++) {
            if ((room_wait_union & (1ull << (unsigned)s)) == 0)
                continue;
            if (rec->ops.pick_target_store(rec->ops.arg, s) >= 0)
                room_ready_mask |= (1ull << (unsigned)s);
        }
    }

    pthread_mutex_lock(&rec->mu);
    for (struct rec_job *j = rec->jobs; j; j = j->next) {
        if (j->removed || j->running || j->pending_run || (!j->waiting_store_up && !j->waiting_room))
            continue;
        bool wake_down = j->waiting_store_up && !all_down &&
                         (j->down_wait_mask != 0
                              ? (cur_down == 0 || (j->down_wait_mask & ~cur_down) != 0)
                              : cur_down == 0);
        bool wake_room = j->waiting_room &&
                         (cur_out != j->room_wait_out_mask || (j->room_wait_out_mask & room_ready_mask) != 0);
        if (wake_down || wake_room) {
            j->waiting_store_up = false;
            j->waiting_room = false;
            j->down_wait_mask = 0;
            j->room_wait_out_mask = 0;
            j->replace_mask |= cur_replace;
            j->replacing_mask |= cur_replacing;
            j->pending_run = true;
            j->retry_at_ms = 0;
        }
    }
    pthread_mutex_unlock(&rec->mu);
}

static void *reconcile_main(void *arg)
{
    struct sreconcile *rec = arg;
    pthread_mutex_lock(&rec->mu);
    while (!atomic_load_explicit(&rec->stopping, memory_order_acquire)) {
        gc_removed_jobs_locked(rec);

        uint64_t now = mono_ms();
        uint64_t earliest = UINT64_MAX;
        if (rec->trigger_all_pending) {
            if (rec->trigger_all_at_ms <= now) {
                rec->trigger_all_pending = false;
                rec->trigger_all_at_ms = 0;
                pthread_mutex_unlock(&rec->mu);
                trigger_all_held(rec);
                pthread_mutex_lock(&rec->mu);
                continue;
            }
            if (rec->trigger_all_at_ms < earliest)
                earliest = rec->trigger_all_at_ms;
        }

        struct rec_job *pick = NULL;
        bool any_waiting_store = false;

        for (struct rec_job *j = rec->jobs; j; j = j->next) {
            if (j->removed || j->running || j->page_pending > 0)
                continue;
            if ((j->waiting_store_up || j->waiting_room) && !j->pending_run) {
                any_waiting_store = true;
                continue;
            }
            if (!j->pending_run)
                continue;
            if (j->retry_at_ms > now) {
                if (j->retry_at_ms < earliest)
                    earliest = j->retry_at_ms;
                continue;
            }
            if (!pick)
                pick = j;
        }

        bool do_store_poll = false;
        if (any_waiting_store && !pick) {
            if (now >= rec->next_store_poll_ms) {
                do_store_poll = true;
                rec->next_store_poll_ms = now + store_poll_ms();
            } else if (rec->next_store_poll_ms < earliest) {
                earliest = rec->next_store_poll_ms;
            }
        }

        bool do_drain = false;
        if (rec->ops.notify_holder && atomic_load_explicit(&missing_pending_cnt, memory_order_acquire) > 0) {
            if (rec->drain_kicked || now >= rec->next_drain_ms) {
                do_drain = true;
                rec->drain_kicked = false;
                rec->next_drain_ms = now + SRECONCILE_DRAIN_INTERVAL_MS;
            } else if (rec->next_drain_ms < earliest) {
                earliest = rec->next_drain_ms;
            }
        } else {
            rec->drain_kicked = false;
        }

        if (!pick && !do_drain && !do_store_poll) {
            if (earliest == UINT64_MAX) {
                pthread_cond_wait(&rec->cv, &rec->mu);
            } else {
                uint64_t wait_ms = earliest > now ? earliest - now : 1;
                struct timespec ts;
                tsdb_cond_deadline_ns(&ts, (int64_t)(wait_ms * 1000000ull));
                pthread_cond_timedwait(&rec->cv, &rec->mu, &ts);
            }
            continue;
        }

        uint64_t gen = 0;
        uint32_t vol = 0, pos = 0;
        if (pick) {
            pick->pending_run = false;
            pick->running = true;
            gen = pick->gen;
            vol = pick->volume;
            pos = pick->pos;
        }
        pthread_mutex_unlock(&rec->mu);

        if (do_drain)
            drain_noted_limited(rec, SRECONCILE_DRAIN_MAX_PER_TICK);
        if (do_store_poll)
            poll_down_stores(rec);
        if (pick)
            run_reconcile_pass(rec, pick, gen, vol, pos);

        pthread_mutex_lock(&rec->mu);
    }
    gc_removed_jobs_locked(rec);
    pthread_mutex_unlock(&rec->mu);
    return NULL;
}

/* ------------------------------------------------------------------ internal trigger helper */

static int trigger_job(struct sreconcile *rec, uint32_t volume, uint32_t pos, uint64_t stripe_version,
                       uint64_t replace_mask, uint64_t replacing_mask)
{
    if (!rec || pos >= CHUNK_POS_MAX)
        return -1;

    pthread_mutex_lock(&rec->mu);
    if (atomic_load_explicit(&rec->stopping, memory_order_acquire)) {
        pthread_mutex_unlock(&rec->mu);
        return -1;
    }
    struct rec_job *j = find_job_locked(rec, volume, pos);
    if (j) {
        if (stripe_version > j->stripe_version)
            j->stripe_version = stripe_version;
        j->replace_mask |= replace_mask;
        j->replacing_mask |= replacing_mask;
        j->waiting_store_up = false;
        j->waiting_room = false;
        j->down_wait_mask = 0;
        j->room_wait_out_mask = 0;
        j->backoff_shift = 0;
        if (j->running || j->page_pending > 0) {
            j->retrigger = true;
        } else {
            j->pending_run = true;
            j->retry_at_ms = 0;
        }
    } else {
        j = calloc(1, sizeof *j);
        if (!j) {
            pthread_mutex_unlock(&rec->mu);
            return -1;
        }
        j->volume = volume;
        j->pos = pos;
        j->stripe_version = stripe_version;
        j->gen = 1;
        j->replace_mask = replace_mask;
        j->replacing_mask = replacing_mask;
        j->pending_run = true;
        j->next = rec->jobs;
        rec->jobs = j;
    }
    REC_STAT(rec, triggers);
    pthread_cond_signal(&rec->cv);
    pthread_mutex_unlock(&rec->mu);
    return 0;
}

static void trigger_all_held(struct sreconcile *rec)
{
    if (!rec)
        return;
    uint64_t down_mask = 0, replace_mask = 0, out_mask = 0, replacing_mask = 0, draining_mask = 0;
    bool all_down = false;
    check_stores(rec, &down_mask, &replace_mask, &out_mask, &replacing_mask, &draining_mask, &all_down);

    if (!rec->ops.list_held)
        return;
    struct sreconcile_held_pos *held = NULL;
    uint32_t n = 0;
    if (rec->ops.list_held(rec->ops.arg, &held, &n) != 0) {
        /* Metadata copy is not caught up yet: retry shortly without promoting REPLACING stores prematurely. */
        pthread_mutex_lock(&rec->mu);
        if (!atomic_load_explicit(&rec->stopping, memory_order_acquire)) {
            rec->trigger_all_pending = true;
            rec->trigger_all_at_ms = mono_ms() + 200;
            pthread_cond_signal(&rec->cv);
        }
        pthread_mutex_unlock(&rec->mu);
        return;
    }
    for (uint32_t i = 0; i < n; i++)
        (void)trigger_job(rec, held[i].volume, held[i].pos, held[i].stripe_version, replace_mask, replacing_mask);
    free(held);
    if (n == 0) {
        if (replace_mask != 0)
            commit_replaced_stores(rec, replace_mask);
        if (replacing_mask != 0)
            settle_replacing_stores(rec, replacing_mask);
    }
}

static void queue_trigger_all(struct sreconcile *rec)
{
    if (!rec)
        return;
    pthread_mutex_lock(&rec->mu);
    if (!atomic_load_explicit(&rec->stopping, memory_order_acquire)) {
        rec->trigger_all_pending = true;
        rec->trigger_all_at_ms = 0;
        pthread_cond_signal(&rec->cv);
    }
    pthread_mutex_unlock(&rec->mu);
}

/* ------------------------------------------------------------------ public API */

struct sreconcile *sreconcile_new(struct sfill *sf, const struct sreconcile_ops *ops, const char *state_dir)
{
    static pthread_once_t once = PTHREAD_ONCE_INIT;
    pthread_once(&once, load_env_mutant_once);

    if (!sf || !ops || !ops->objlist || !ops->vol_view)
        return NULL;
    struct sreconcile *rec = calloc(1, sizeof *rec);
    if (!rec)
        return NULL;
    rec->sf = sf;
    rec->ops = *ops;
    if (state_dir && *state_dir)
        snprintf(rec->state_dir, sizeof rec->state_dir, "%s", state_dir);
    pthread_mutex_init(&rec->pub_mu, NULL);
    pthread_mutex_init(&rec->mu, NULL);
    pthread_mutex_init(&rec->store_mu, NULL);
    tsdb_cond_init_mono(&rec->cv);
    tsdb_cond_init_mono(&rec->page_cv);
    tsdb_cond_init_mono(&rec->done_cv);

    if (pthread_create(&rec->thr, NULL, reconcile_main, rec) != 0) {
        pthread_cond_destroy(&rec->done_cv);
        pthread_cond_destroy(&rec->page_cv);
        pthread_cond_destroy(&rec->cv);
        pthread_mutex_destroy(&rec->store_mu);
        pthread_mutex_destroy(&rec->mu);
        pthread_mutex_destroy(&rec->pub_mu);
        free(rec);
        return NULL;
    }
    rec->thr_started = true;
    reg_add(sf, rec);
    return rec;
}

void sreconcile_stop(struct sreconcile *rec)
{
    if (!rec)
        return;
    pthread_mutex_lock(&rec->mu);
    atomic_store_explicit(&rec->stopping, true, memory_order_release);
    bool started = rec->thr_started;
    rec->thr_started = false;
    pthread_cond_broadcast(&rec->cv);
    pthread_cond_broadcast(&rec->page_cv);
    pthread_mutex_unlock(&rec->mu);
    if (started)
        pthread_join(rec->thr, NULL);
}

void sreconcile_free(struct sreconcile *rec)
{
    if (!rec)
        return;
    reg_del(rec);
    sreconcile_stop(rec);

    /* Wait until every in-flight task submitted to sfill has invoked on_task_done. */
    pthread_mutex_lock(&rec->mu);
    while (rec->total_inflight > 0)
        pthread_cond_wait(&rec->done_cv, &rec->mu);
    for (struct rec_job *j = rec->jobs, *nx; j; j = nx) {
        nx = j->next;
        if (j->pace_id)
            pace_job_end_ex(PACE_RECV, j->pace_id);
        free(j);
    }
    rec->jobs = NULL;
    pthread_mutex_unlock(&rec->mu);

    pthread_cond_destroy(&rec->done_cv);
    pthread_cond_destroy(&rec->page_cv);
    pthread_cond_destroy(&rec->cv);
    pthread_mutex_destroy(&rec->store_mu);
    pthread_mutex_destroy(&rec->mu);
    pthread_mutex_destroy(&rec->pub_mu);
    free(rec);
}

int sreconcile_trigger_ex(struct sreconcile *rec, uint32_t volume, uint32_t pos, uint64_t stripe_version)
{
    return trigger_job(rec, volume, pos, stripe_version, 0, 0);
}

void sreconcile_on_start_ex(struct sreconcile *rec)
{
    queue_trigger_all(rec);
}

void sreconcile_on_return(struct sreconcile *rec)
{
    queue_trigger_all(rec);
}

void sreconcile_stop_pos(struct sreconcile *rec, uint32_t volume, uint32_t pos)
{
    if (!rec)
        return;
    bool changed = false;
    pthread_mutex_lock(&rec->mu);
    for (struct rec_job *j = rec->jobs; j; j = j->next) {
        if (!j->removed && j->volume == volume && j->pos == pos) {
            j->removed = true;
            j->gen++;
            j->pending_run = false;
            j->retrigger = false;
            if (j->reporting) {
                j->reporting = false;
                changed = true;
            }
            if (j->pace_id) {
                pace_job_end_ex(PACE_RECV, j->pace_id);
                j->pace_id = 0;
            }
        }
    }
    pthread_cond_broadcast(&rec->page_cv);
    gc_removed_jobs_locked(rec);
    pthread_mutex_unlock(&rec->mu);
    if (changed)
        publish_reports(rec);
}

void sreconcile_kick_noted(struct sreconcile *rec)
{
    if (!rec)
        return;
    if (atomic_load_explicit(&missing_pending_cnt, memory_order_acquire) == 0)
        return;
    pthread_mutex_lock(&rec->mu);
    if (!atomic_load_explicit(&rec->stopping, memory_order_acquire)) {
        rec->drain_kicked = true;
        pthread_cond_signal(&rec->cv);
    }
    pthread_mutex_unlock(&rec->mu);
}

void sreconcile_drain_noted(struct sreconcile *rec)
{
    drain_noted_limited(rec, 64);
}

uint32_t sreconcile_report(struct sreconcile *rec, struct sfill_report *out, uint32_t cap)
{
    if (!rec || !out || cap == 0)
        return 0;
    uint32_t n = 0;
    pthread_mutex_lock(&rec->mu);
    for (struct rec_job *j = rec->jobs; j && n < cap; j = j->next) {
        if (j->removed || !j->reporting)
            continue;
        out[n++] = (struct sfill_report){
            .volume = j->volume,
            .pos = j->pos,
            .stripe_version = j->stripe_version,
            .done = j->done,
            .total = j->total,
            .whole = 0,
            .failures = j->failures,
        };
    }
    pthread_mutex_unlock(&rec->mu);
    return n;
}

void sreconcile_get_stats(struct sreconcile *rec, struct sreconcile_stats *out)
{
    if (!rec || !out)
        return;
    out->triggers = atomic_load_explicit(&rec->st.triggers, memory_order_relaxed);
    out->passes_ok = atomic_load_explicit(&rec->st.passes_ok, memory_order_relaxed);
    out->passes_err = atomic_load_explicit(&rec->st.passes_err, memory_order_relaxed);
    out->missing_queued = atomic_load_explicit(&rec->st.missing_queued, memory_order_relaxed);
    out->missing_on_down_store = atomic_load_explicit(&rec->st.missing_on_down_store, memory_order_relaxed);
    out->filled = atomic_load_explicit(&rec->st.filled, memory_order_relaxed);
    out->already_present = atomic_load_explicit(&rec->st.already_present, memory_order_relaxed);
    out->gc_pending = atomic_load_explicit(&rec->st.gc_pending, memory_order_relaxed);
    out->orphans_reported = atomic_load_explicit(&rec->st.orphans_reported, memory_order_relaxed);
    out->store_id_mismatches = atomic_load_explicit(&rec->st.store_id_mismatches, memory_order_relaxed);
    out->store_down_events = atomic_load_explicit(&rec->st.store_down_events, memory_order_relaxed);
    out->bytes_filled = atomic_load_explicit(&rec->st.bytes_filled, memory_order_relaxed);
    out->failed = atomic_load_explicit(&rec->st.failed, memory_order_relaxed);
    out->holder_nudges = atomic_load_explicit(&rec->st.holder_nudges, memory_order_relaxed);
}

/* ------------------------------------------------------------------ sfill.h compatibility entrypoints */

int sreconcile_trigger(struct sfill *sf, uint32_t volume, uint32_t pos, uint64_t stripe_version)
{
    struct sreconcile *rec = reg_find(sf);
    if (!rec)
        return 0;
    return sreconcile_trigger_ex(rec, volume, pos, stripe_version);
}

void sreconcile_on_start(struct sfill *sf)
{
    struct sreconcile *rec = reg_find(sf);
    if (rec)
        sreconcile_on_start_ex(rec);
}
