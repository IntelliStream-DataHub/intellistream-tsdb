/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Per-store background chunk scrubber (src/chunkscrub.h; R12, r12-chunk).
 */
#define _GNU_SOURCE
#include "chunkscrub.h"

#include "chunk_priv.h"
#include "compat.h"
#include "internal.h"

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

#ifdef TSDB_TEST_HOOKS
int chunkscrub_mutant;
int chunkscrub_fault_list_vol;

int chunkscrub_mutant_parse(const char *name)
{
    if (!name || strcmp(name, "none") == 0)
        return CHUNKSCRUB_MUT_NONE;
    if (strcmp(name, "scrubskipcell") == 0)
        return CHUNKSCRUB_MUT_SKIPCELL;
    if (strcmp(name, "scrubeioclean") == 0)
        return CHUNKSCRUB_MUT_EIOCLEAN;
    if (strcmp(name, "scrubrestart") == 0)
        return CHUNKSCRUB_MUT_RESTART;
    return -1;
}
#endif

#define CHUNKSCRUB_STOPPED 1
#define CHUNKSCRUB_RETRY_MAX 256u
#define CHUNKSCRUB_RETRY_BATCH 8u             /* cap retry RPCs per flush slice so unreachable leaders do not stall */
#define CHUNKSCRUB_MAX_RATE_BPS (10ull << 30) /* 10 GiB/s ceiling prevents uint64 overflow in (bytes % rate) * 1e9 */
#define CHUNKSCRUB_BURST_NS 50000000ull       /* 50 ms virtual-time token-bucket burst ceiling */
#define CHUNKSCRUB_EMPTY_POLL_MS 2000u        /* idle re-check when a store currently has 0 chunks */
#define CHUNKSCRUB_SAVE_INTERVAL_NS 15000000000ull /* 15 s advisory SCRUB checkpoint cadence */
#define CHUNKSCRUB_LIST_ERR_DMG_STREAK 3u     /* consecutive EIO volume list failures before counting store damage */

struct scrub_key {
    uint8_t oid[16];
    uint16_t pos;
};

struct scrub_vec {
    struct scrub_key *v;
    size_t n, cap;
    int err;
};

struct chunk_scrub {
    chunk_store *st;
    int dirfd; /* independent open file description of store root so SCRUB fsync never consumes st->dirfd errors */
    chunk_scrub_cfg cfg;

    pthread_mutex_t mu;
    pthread_cond_t cv;
    pthread_t thr;
    bool started;
    bool stop;
    bool exited;
    int busy;

    /* live settings (protected by mu) */
    uint32_t scrub_days;
    uint32_t scrub_mb_s;
    uint64_t rate_bps;
    uint32_t scrub_out_count;
    uint32_t pass_interval_ms;
    uint32_t save_every_chunks;

    /* advisory progress & counters (written by scrub thread, snapshot read under mu) */
    bool cursor_valid;
    uint32_t cursor_vol;
    uint8_t cursor_oid[16];
    uint16_t cursor_pos;

    uint64_t first_pass_s;
    uint64_t pass_start_s;
    uint64_t last_pass_s;
    uint64_t bytes_total;
    uint64_t chunks_total;
    uint64_t damaged_total;
    uint64_t passes_done;
    uint64_t day_utc;
    uint64_t day_damaged;
    uint64_t retry_dropped;
    bool marked_down;

    /* virtual-time token bucket (scrub thread only) */
    uint64_t tb_next_ns;
    bool tb_init;
    uint32_t list_err_streak;

    /* bounded retry queue for reports whose leader was unreachable (scrub thread only) */
    chunk_scrub_damage retries[CHUNKSCRUB_RETRY_MAX];
    size_t nretries;
};

static uint64_t mono_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static uint64_t scrub_wall_s(const chunk_scrub *sc)
{
    if (sc->cfg.wall_s)
        return sc->cfg.wall_s(sc->cfg.ctx);
    time_t t = time(NULL);
    return t > 0 ? (uint64_t)t : 0;
}

static uint64_t effective_rate_bps(uint32_t mb_s, uint64_t override_bps)
{
    uint64_t r;
    if (override_bps > 0) {
        r = override_bps;
    } else {
        uint32_t m = mb_s ? mb_s : CHUNKSCRUB_DEFAULT_MB_S;
        r = (uint64_t)m * (1024ull * 1024ull);
    }
    return r > CHUNKSCRUB_MAX_RATE_BPS ? CHUNKSCRUB_MAX_RATE_BPS : r;
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

/* ---------------------------------------------------------------- advisory SCRUB file (D4) */

static int scrub_format_text(char *out, size_t cap, uint64_t pass_start_s, uint64_t last_pass_s, uint64_t first_pass_s,
                             bool cursor_valid, uint32_t cursor_vol, const uint8_t cursor_oid[16], uint16_t cursor_pos,
                             uint64_t day_utc, uint64_t day_damaged)
{
    char oid_hex[33];
    chunk_oid_hex(cursor_oid, oid_hex);
    char body[512];
    int bl = snprintf(body, sizeof body,
                      "tsdb-chunk-scrub 1\n"
                      "pass_start_s %" PRIu64 "\n"
                      "last_pass_s %" PRIu64 "\n"
                      "first_pass_s %" PRIu64 "\n"
                      "cursor_valid %u\n"
                      "cursor_vol %" PRIu32 "\n"
                      "cursor_oid %s\n"
                      "cursor_pos %u\n"
                      "day_utc %" PRIu64 "\n"
                      "day_damaged %" PRIu64 "\n",
                      pass_start_s, last_pass_s, first_pass_s, cursor_valid ? 1u : 0u, cursor_vol, oid_hex,
                      (unsigned)cursor_pos, day_utc, day_damaged);
    if (bl <= 0 || (size_t)bl >= sizeof body)
        return -1;
    uint64_t h = xxh64(body, (size_t)bl, 0);
    int fl = snprintf(out, cap, "%ssum=%016" PRIx64 "\n", body, h);
    if (fl <= 0 || (size_t)fl >= cap)
        return -1;
    return fl;
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

/* Persists <store>/SCRUB via tmp -> fsync -> rename -> dir fsync (D4) on sc->dirfd (independent open file description
 * so its fsync never consumes a writeback error pending on st->dirfd). Advisory: never fatal. */
static void scrub_progress_save(chunk_scrub *sc)
{
    int dirfd = sc->dirfd;
    if (dirfd < 0)
        return;

    pthread_mutex_lock(&sc->mu);
    uint64_t ps = sc->pass_start_s, lp = sc->last_pass_s, fp = sc->first_pass_s;
    bool cv = sc->cursor_valid;
    uint32_t cvol = sc->cursor_vol;
    uint8_t coid[16];
    memcpy(coid, sc->cursor_oid, 16);
    uint16_t cpos = sc->cursor_pos;
    uint64_t du = sc->day_utc, dd = sc->day_damaged;
    pthread_mutex_unlock(&sc->mu);

    char text[600];
    int len = scrub_format_text(text, sizeof text, ps, lp, fp, cv, cvol, coid, cpos, du, dd);
    if (len <= 0)
        return;

    unlinkat(dirfd, "SCRUB.tmp", 0);
    int fd = openat(dirfd, "SCRUB.tmp", O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0644);
    int rc = fd < 0 ? -1 : 0;
    if (fd >= 0) {
        if (wr_all(fd, text, (size_t)len) != 0 || tsdb_fsync(fd) != 0)
            rc = -1;
        if (close(fd) != 0)
            rc = -1;
    }
    if (rc == 0 && renameat(dirfd, "SCRUB.tmp", dirfd, "SCRUB") != 0)
        rc = -1;
    if (rc != 0)
        unlinkat(dirfd, "SCRUB.tmp", 0);
    if (rc == 0 && tsdb_fsync(dirfd) != 0)
        rc = -1;
    if (rc != 0)
        fprintf(stderr, "tsdb: chunk scrub %s/SCRUB: could not save advisory progress (continuing)\n",
                chunk_store_path(sc->st));
}

static void scrub_progress_load(chunk_scrub *sc)
{
    uint64_t now_s = scrub_wall_s(sc);
    uint64_t today = now_s / 86400ull;

    sc->cursor_valid = false;
    sc->cursor_vol = 0;
    memset(sc->cursor_oid, 0, sizeof sc->cursor_oid);
    sc->cursor_pos = 0;
    sc->first_pass_s = now_s;
    sc->pass_start_s = now_s;
    sc->last_pass_s = 0;
    sc->day_utc = today;
    sc->day_damaged = 0;

    int dirfd = sc->dirfd;
    if (dirfd < 0)
        return;

    int fd = openat(dirfd, "SCRUB", O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        if (errno == ENOENT)
            fprintf(stderr, "tsdb: chunk scrub %s/SCRUB: missing; starting fresh pass\n", chunk_store_path(sc->st));
        else
            fprintf(stderr, "tsdb: chunk scrub %s/SCRUB: unreadable (%s); restarting pass\n", chunk_store_path(sc->st),
                    strerror(errno));
        return;
    }

    char buf[768];
    size_t got = 0;
    int rc = 0;
    for (;;) {
        ssize_t r = read(fd, buf + got, sizeof buf - 1 - got);
        if (r < 0 && errno == EINTR)
            continue;
        if (r < 0) {
            rc = -1;
            break;
        }
        if (r == 0)
            break;
        got += (size_t)r;
        if (got >= sizeof buf - 1) {
            rc = -2;
            break;
        }
    }
    close(fd);

    if (rc != 0 || got == 0) {
        fprintf(stderr, "tsdb: chunk scrub %s/SCRUB: damaged or unreadable progress file; restarting pass\n",
                chunk_store_path(sc->st));
        return;
    }
    buf[got] = '\0';

    uint64_t ps = 0, lp = 0, fp = 0, du = 0, dd = 0;
    uint32_t cv = 0, cvol = 0, cpos = 0;
    char oid_hex[40] = "";
    uint8_t oid[16] = {0};

    if (sscanf(buf,
               "tsdb-chunk-scrub 1\n"
               "pass_start_s %" SCNu64 "\n"
               "last_pass_s %" SCNu64 "\n"
               "first_pass_s %" SCNu64 "\n"
               "cursor_valid %" SCNu32 "\n"
               "cursor_vol %" SCNu32 "\n"
               "cursor_oid %39s\n"
               "cursor_pos %" SCNu32 "\n"
               "day_utc %" SCNu64 "\n"
               "day_damaged %" SCNu64 "\n",
               &ps, &lp, &fp, &cv, &cvol, oid_hex, &cpos, &du, &dd) != 9 ||
        cv > 1 || cpos >= CHUNK_POS_MAX || strlen(oid_hex) != 32 || !hex_parse(oid_hex, 32, oid)) {
        fprintf(stderr, "tsdb: chunk scrub %s/SCRUB: malformed progress file; restarting pass\n",
                chunk_store_path(sc->st));
        return;
    }

    char want[600];
    int wlen = scrub_format_text(want, sizeof want, ps, lp, fp, cv != 0, cvol, oid, (uint16_t)cpos, du, dd);
    if (wlen <= 0 || (size_t)wlen != got || memcmp(want, buf, got) != 0) {
        fprintf(stderr, "tsdb: chunk scrub %s/SCRUB: checksum mismatch; restarting pass\n", chunk_store_path(sc->st));
        return;
    }

    sc->pass_start_s = ps ? ps : now_s;
    sc->last_pass_s = lp;
    sc->first_pass_s = fp ? fp : (lp ? lp : now_s);
    if (du == today)
        sc->day_damaged = dd;

#ifdef TSDB_TEST_HOOKS
    if (chunkscrub_mutant == CHUNKSCRUB_MUT_RESTART) /* planted bug: ignore saved cursor on restart */
        return;
#endif
    if (cv) {
        sc->cursor_valid = true;
        sc->cursor_vol = cvol;
        memcpy(sc->cursor_oid, oid, 16);
        sc->cursor_pos = (uint16_t)cpos;
    }
}

/* ---------------------------------------------------------------- pacing & pause */

static bool wait_interruptible_ns(chunk_scrub *sc, uint64_t wait_ns)
{
    pthread_mutex_lock(&sc->mu);
    if (!sc->stop && wait_ns > 0) {
        struct timespec ts;
        tsdb_cond_deadline_ns(&ts, (int64_t)wait_ns);
        pthread_cond_timedwait(&sc->cv, &sc->mu, &ts);
    }
    bool stopping = sc->stop;
    pthread_mutex_unlock(&sc->mu);
    return !stopping;
}

static bool is_paused_now(chunk_scrub *sc)
{
    pthread_mutex_lock(&sc->mu);
    bool b = sc->busy > 0;
    bool stopping = sc->stop;
    pthread_mutex_unlock(&sc->mu);
    if (stopping)
        return false;
    if (b)
        return true;
    if (sc->cfg.is_paused && sc->cfg.is_paused(sc->cfg.ctx, sc->cfg.store_idx))
        return true;
    return false;
}

static bool wait_unpaused(chunk_scrub *sc)
{
    bool was_paused = false;
    while (is_paused_now(sc)) {
        was_paused = true;
        if (!wait_interruptible_ns(sc, 50000000ull))
            return false;
    }
    if (was_paused) {
        /* Never accumulate burst credits while paused. */
        sc->tb_next_ns = mono_ns();
        sc->tb_init = true;
    }
    pthread_mutex_lock(&sc->mu);
    bool stopping = sc->stop;
    pthread_mutex_unlock(&sc->mu);
    return !stopping;
}

static bool pace_bytes(chunk_scrub *sc, uint64_t bytes)
{
    if (!wait_unpaused(sc))
        return false;
    if (bytes == 0)
        return true;

    pthread_mutex_lock(&sc->mu);
    bool stopping = sc->stop;
    uint64_t rate = sc->rate_bps;
    pthread_mutex_unlock(&sc->mu);
    if (stopping)
        return false;
    if (rate == 0)
        return true;
    if (rate > CHUNKSCRUB_MAX_RATE_BPS)
        rate = CHUNKSCRUB_MAX_RATE_BPS;

    uint64_t now = mono_ns();
    if (!sc->tb_init) {
        sc->tb_next_ns = now;
        sc->tb_init = true;
    } else if (now > sc->tb_next_ns + CHUNKSCRUB_BURST_NS) {
        sc->tb_next_ns = now - CHUNKSCRUB_BURST_NS;
    }

    uint64_t cost_ns = (bytes / rate) * 1000000000ull + ((bytes % rate) * 1000000000ull) / rate;
    sc->tb_next_ns += cost_ns;

    for (;;) {
        now = mono_ns();
        if (now >= sc->tb_next_ns)
            return true;
        uint64_t rem = sc->tb_next_ns - now;
        if (rem > 250000000ull)
            rem = 250000000ull;
        if (!wait_interruptible_ns(sc, rem))
            return false;
        if (!wait_unpaused(sc))
            return false;
    }
}

static int cell_pace_cb(void *ctx, size_t bytes)
{
    chunk_scrub *sc = ctx;
    return pace_bytes(sc, bytes) ? 0 : CHUNKSCRUB_STOPPED;
}

/* ---------------------------------------------------------------- damage & retries */

static void retry_add(chunk_scrub *sc, const chunk_scrub_damage *dmg)
{
    for (size_t i = 0; i < sc->nretries; i++) {
        if (sc->retries[i].volume == dmg->volume && sc->retries[i].pos == dmg->pos &&
            memcmp(sc->retries[i].oid, dmg->oid, 16) == 0) {
            sc->retries[i].bad_cells |= dmg->bad_cells;
            sc->retries[i].err = dmg->err;
            return;
        }
    }
    if (sc->nretries < CHUNKSCRUB_RETRY_MAX) {
        sc->retries[sc->nretries++] = *dmg;
    } else {
        pthread_mutex_lock(&sc->mu);
        uint64_t dropped = ++sc->retry_dropped;
        pthread_mutex_unlock(&sc->mu);
        if (dropped == 1 || (dropped & (dropped - 1)) == 0)
            fprintf(stderr,
                    "tsdb: chunk scrub %s (store %d): damage retry queue full (%u); dropped %" PRIu64 " report(s)\n",
                    chunk_store_path(sc->st), sc->cfg.store_idx, CHUNKSCRUB_RETRY_MAX, dropped);
    }
}

static void retry_flush(chunk_scrub *sc)
{
    if (!sc->cfg.on_damage || sc->nretries == 0)
        return;
    /* Bound RPCs per flush slice and skip additional entries for a volume whose leader just failed in this slice. */
    uint32_t failed_vols[8];
    size_t nfailed_vols = 0;
    size_t attempts = 0;
    size_t i = 0;
    while (i < sc->nretries && attempts < CHUNKSCRUB_RETRY_BATCH) {
        pthread_mutex_lock(&sc->mu);
        bool stopping = sc->stop;
        pthread_mutex_unlock(&sc->mu);
        if (stopping)
            return;
        uint32_t v = sc->retries[i].volume;
        bool skip_vol = false;
        for (size_t k = 0; k < nfailed_vols; k++) {
            if (failed_vols[k] == v) {
                skip_vol = true;
                break;
            }
        }
        if (skip_vol) {
            i++;
            continue;
        }
        attempts++;
        if (sc->cfg.on_damage(sc->cfg.ctx, &sc->retries[i]) == 0) {
            sc->retries[i] = sc->retries[--sc->nretries];
        } else {
            if (nfailed_vols < sizeof failed_vols / sizeof failed_vols[0])
                failed_vols[nfailed_vols++] = v;
            i++;
        }
    }
}

static void record_damage(chunk_scrub *sc, uint32_t vol, const uint8_t oid[16], uint16_t pos, uint64_t bad_cells,
                          int err, uint32_t *pass_failed_vols, size_t *npass_failed, size_t pass_failed_cap)
{
    uint64_t now_s = scrub_wall_s(sc);
    uint64_t today = now_s / 86400ull;
    bool trigger_down = false;
    uint64_t day_dmg = 0;
    uint32_t out_limit = 0;

    pthread_mutex_lock(&sc->mu);
    if (sc->day_utc != today) {
        sc->day_utc = today;
        sc->day_damaged = 0;
    }
    sc->damaged_total++;
    sc->day_damaged++;
    day_dmg = sc->day_damaged;
    out_limit = sc->scrub_out_count;
    if (day_dmg > out_limit && !sc->marked_down) {
        sc->marked_down = true;
        trigger_down = true;
    }
    pthread_mutex_unlock(&sc->mu);

    char hex[33];
    chunk_oid_hex(oid, hex);
    fprintf(stderr,
            "tsdb: chunk scrub %s (store %d): damaged chunk vol=%" PRIu32 " oid=%s pos=%u bad_cells=0x%" PRIx64
            " (%s, day_damaged=%" PRIu64 ")\n",
            chunk_store_path(sc->st), sc->cfg.store_idx, vol, hex, (unsigned)pos, bad_cells, tsdb_strerror(err),
            day_dmg);

    chunk_scrub_damage dmg = {
        .store_idx = sc->cfg.store_idx,
        .volume = vol,
        .pos = pos,
        .bad_cells = bad_cells,
        .err = err,
    };
    memcpy(dmg.oid, oid, 16);

    if (sc->cfg.on_damage) {
        bool vol_already_failed = false;
        for (size_t k = 0; k < *npass_failed; k++) {
            if (pass_failed_vols[k] == vol) {
                vol_already_failed = true;
                break;
            }
        }
        if (vol_already_failed) {
            retry_add(sc, &dmg);
        } else if (sc->cfg.on_damage(sc->cfg.ctx, &dmg) != 0) {
            if (*npass_failed < pass_failed_cap)
                pass_failed_vols[(*npass_failed)++] = vol;
            retry_add(sc, &dmg);
        }
    }

    if (trigger_down) {
        fprintf(stderr,
                "tsdb: chunk scrub %s (store %d): %" PRIu64 " damaged chunks on UTC day %" PRIu64
                " (exceeds scrub_out_count=%" PRIu32 "): marking store DOWN locally\n",
                chunk_store_path(sc->st), sc->cfg.store_idx, day_dmg, today, out_limit);
        if (sc->cfg.be)
            (void)chunk_be_mark_down(sc->cfg.be, sc->cfg.store_idx, sc->st, CHUNK_STORE_DOWN_DAMAGED);
        if (sc->cfg.on_out)
            sc->cfg.on_out(sc->cfg.ctx, sc->cfg.store_idx, day_dmg);
    }
}

/* ---------------------------------------------------------------- sorting & listing */

static int cmp_u32(const void *a, const void *b)
{
    uint32_t x = *(const uint32_t *)a, y = *(const uint32_t *)b;
    return (x > y) - (x < y);
}

static int cmp_key_raw(const uint8_t oid_a[16], uint16_t pos_a, const uint8_t oid_b[16], uint16_t pos_b)
{
    int c = memcmp(oid_a, oid_b, 16);
    if (c != 0)
        return c;
    return (pos_a > pos_b) - (pos_a < pos_b);
}

static int cmp_key(const void *a, const void *b)
{
    const struct scrub_key *x = a, *y = b;
    return cmp_key_raw(x->oid, x->pos, y->oid, y->pos);
}

static int collect_name(void *ctx, const chunk_listed *c)
{
    struct scrub_vec *v = ctx;
    if (v->n == v->cap) {
        size_t nc = v->cap ? v->cap * 2 : 128;
        struct scrub_key *nv = realloc(v->v, nc * sizeof *nv);
        if (!nv) {
            v->err = TSDB_ERR_NOMEM;
            return 1;
        }
        v->v = nv;
        v->cap = nc;
    }
    memcpy(v->v[v->n].oid, c->oid, 16);
    v->v[v->n].pos = c->pos;
    v->n++;
    return 0;
}

/* ---------------------------------------------------------------- scrub thread */

static void scrub_wait_until_due(chunk_scrub *sc, uint64_t lp)
{
    for (;;) {
        pthread_mutex_lock(&sc->mu);
        bool stp = sc->stop;
        uint32_t cur_days = sc->scrub_days;
        uint32_t cur_iv = sc->pass_interval_ms;
        pthread_mutex_unlock(&sc->mu);
        if (stp || cur_iv > 0 || cur_days == 0)
            break;
        if (sc->cfg.be && chunk_be_raw_store(sc->cfg.be, sc->cfg.store_idx) != sc->st)
            break;
        uint64_t target_s = lp + (uint64_t)cur_days * 86400ull;
        uint64_t cur_now = scrub_wall_s(sc);
        if (cur_now >= target_s)
            break;
        if (!wait_interruptible_ns(sc, 500000000ull))
            break;
        retry_flush(sc);
    }
}

static void record_store_io_damage(chunk_scrub *sc, uint32_t vol, int err)
{
    uint64_t now_s = scrub_wall_s(sc);
    uint64_t today = now_s / 86400ull;
    bool trigger_down = false;
    uint64_t day_dmg = 0;
    uint32_t out_limit = 0;

    pthread_mutex_lock(&sc->mu);
    if (sc->day_utc != today) {
        sc->day_utc = today;
        sc->day_damaged = 0;
    }
    sc->damaged_total++;
    sc->day_damaged++;
    day_dmg = sc->day_damaged;
    out_limit = sc->scrub_out_count;
    if (day_dmg > out_limit && !sc->marked_down) {
        sc->marked_down = true;
        trigger_down = true;
    }
    pthread_mutex_unlock(&sc->mu);

    if (trigger_down) {
        fprintf(stderr,
                "tsdb: chunk scrub %s (store %d): %" PRIu64 " damaged events on UTC day %" PRIu64
                " (including persistent volume %" PRIu32 " list %s; exceeds scrub_out_count=%" PRIu32
                "): marking store DOWN locally\n",
                chunk_store_path(sc->st), sc->cfg.store_idx, day_dmg, today, vol, tsdb_strerror(err), out_limit);
        if (sc->cfg.be)
            (void)chunk_be_mark_down(sc->cfg.be, sc->cfg.store_idx, sc->st, CHUNK_STORE_DOWN_DAMAGED);
        if (sc->cfg.on_out)
            sc->cfg.on_out(sc->cfg.ctx, sc->cfg.store_idx, day_dmg);
    }
}

static void *scrub_main(void *arg)
{
    chunk_scrub *sc = arg;

    /* On restart after a recently completed pass (no active mid-pass cursor), respect the remaining scrub_days wait
     * instead of immediately re-reading the entire store on every rolling restart. */
    pthread_mutex_lock(&sc->mu);
    bool init_cvalid = sc->cursor_valid;
    uint64_t init_lp = sc->last_pass_s;
    uint32_t init_iv = sc->pass_interval_ms;
    uint32_t init_days = sc->scrub_days;
    pthread_mutex_unlock(&sc->mu);
    if (!init_cvalid && init_lp > 0 && init_iv == 0 && init_days > 0)
        scrub_wait_until_due(sc, init_lp);

    for (;;) {
        pthread_mutex_lock(&sc->mu);
        bool stopping = sc->stop;
        bool is_down = sc->marked_down;
        pthread_mutex_unlock(&sc->mu);
        if (stopping)
            break;
        /* Stop cleanly if this store slot was replaced/retired in its backend. */
        if (sc->cfg.be && chunk_be_raw_store(sc->cfg.be, sc->cfg.store_idx) != sc->st)
            break;

        retry_flush(sc);
        /* Once daily damage exceeded scrub_out_count and marked this store DOWN locally, suspend further walking so a
         * dying disk does not flood logs or volume leaders; keep flushing already-queued retries until stop/replace. */
        if (is_down) {
            if (!wait_interruptible_ns(sc, 500000000ull))
                break;
            continue;
        }
        if (!wait_unpaused(sc))
            break;

        /* Record the true start time of a fresh pass (after inter-pass sleep, before reading chunks). */
        uint64_t start_now_s = scrub_wall_s(sc);
        pthread_mutex_lock(&sc->mu);
        bool had_cursor = sc->cursor_valid;
        if (!had_cursor) {
            sc->pass_start_s = start_now_s;
            if (!sc->first_pass_s)
                sc->first_pass_s = start_now_s;
        }
        pthread_mutex_unlock(&sc->mu);

        uint32_t *vols = NULL;
        uint64_t *eps = NULL;
        size_t nv = 0;
        if (chunk_store_volumes(sc->st, &vols, &eps, &nv) != 0) {
            if (!wait_interruptible_ns(sc, 500000000ull))
                break;
            continue;
        }
        free(eps);
        if (nv > 1)
            qsort(vols, nv, sizeof *vols, cmp_u32);

        uint64_t pass_chunks = 0;
        uint64_t since_save_chunks = 0;
        uint64_t last_save_ns = mono_ns();
        bool had_list_err = false;
        bool aborted = false;
        bool down_now = false;
        uint32_t pass_failed_vols[8];
        size_t npass_failed = 0;

        /* Reset token bucket start before beginning active I/O for this pass. */
        sc->tb_next_ns = mono_ns();
        sc->tb_init = true;

        for (size_t vi = 0; vi < nv && !aborted && !had_list_err && !down_now; vi++) {
            if (sc->cfg.be && chunk_be_raw_store(sc->cfg.be, sc->cfg.store_idx) != sc->st) {
                aborted = true;
                break;
            }
            uint32_t vol = vols[vi];

            pthread_mutex_lock(&sc->mu);
            stopping = sc->stop;
            bool cvalid = sc->cursor_valid;
            uint32_t cvol = sc->cursor_vol;
            pthread_mutex_unlock(&sc->mu);
            if (stopping) {
                aborted = true;
                break;
            }
            if (cvalid && vol < cvol)
                continue;

            struct scrub_vec names = {0};
            int lrc = 0;
#ifdef TSDB_TEST_HOOKS
            if (chunkscrub_fault_list_vol > 0 && (uint32_t)chunkscrub_fault_list_vol == vol) {
                chunkscrub_fault_list_vol = 0;
                lrc = TSDB_ERR_IO;
            } else
#endif
                lrc = chunk_store_list_names(sc->st, vol, collect_name, &names);
            if (lrc != 0 || names.err != 0) {
                int err = names.err ? names.err : lrc;
                free(names.v);
                had_list_err = true;
                sc->list_err_streak++;
                if (sc->list_err_streak == 1 || (sc->list_err_streak & (sc->list_err_streak - 1)) == 0)
                    fprintf(stderr,
                            "tsdb: chunk scrub %s (store %d): failed to list volume %" PRIu32 " (%s, streak=%" PRIu32
                            ")\n",
                            chunk_store_path(sc->st), sc->cfg.store_idx, vol, tsdb_strerror(err), sc->list_err_streak);
                if (err == TSDB_ERR_IO && sc->list_err_streak >= CHUNKSCRUB_LIST_ERR_DMG_STREAK) {
#ifdef TSDB_TEST_HOOKS
                    if (chunkscrub_mutant != CHUNKSCRUB_MUT_EIOCLEAN)
#endif
                        record_store_io_damage(sc, vol, err);
                }
                break;
            }
            sc->list_err_streak = 0;
            if (names.n > 1)
                qsort(names.v, names.n, sizeof *names.v, cmp_key);

            for (size_t i = 0; i < names.n; i++) {
                if (!wait_unpaused(sc)) {
                    aborted = true;
                    break;
                }

                const uint8_t *oid = names.v[i].oid;
                uint16_t pos = names.v[i].pos;

                pthread_mutex_lock(&sc->mu);
                cvalid = sc->cursor_valid;
                cvol = sc->cursor_vol;
                uint8_t coid[16];
                memcpy(coid, sc->cursor_oid, 16);
                uint16_t cpos = sc->cursor_pos;
                uint32_t save_n = sc->save_every_chunks;
                pthread_mutex_unlock(&sc->mu);

                if (cvalid && vol == cvol && cmp_key_raw(oid, pos, coid, cpos) <= 0)
                    continue;

                chunk_rd *rd = NULL;
                int orc = chunk_rd_open_any(sc->st, vol, oid, pos, &rd);
                uint64_t chunk_bytes = 0;
                bool damaged = false;
                bool skip_deleted = false;
                uint64_t bad_bitmap = 0;
                int dmg_err = 0;

                if (orc == TSDB_ERR_NOTFOUND) {
                    skip_deleted = true;
                } else if (orc == TSDB_ERR_CORRUPT) {
                    chunk_bytes = CHUNK_HDR_BYTES + CHUNK_FTR_BYTES;
                    damaged = true;
                    dmg_err = TSDB_ERR_CORRUPT;
                    if (!pace_bytes(sc, chunk_bytes)) {
                        aborted = true;
                        break;
                    }
                } else if (orc == TSDB_ERR_IO) {
                    chunk_bytes = CHUNK_HDR_BYTES;
#ifdef TSDB_TEST_HOOKS
                    if (chunkscrub_mutant != CHUNKSCRUB_MUT_EIOCLEAN)
#endif
                    {
                        damaged = true;
                        dmg_err = TSDB_ERR_IO;
                    }
                    if (!pace_bytes(sc, chunk_bytes)) {
                        aborted = true;
                        break;
                    }
                } else if (orc != 0) {
                    /* Transient (e.g. NOMEM): do not advance cursor; stop this pass and retry. */
                    had_list_err = true;
                    break;
                } else {
                    const chunk_hdr *h = chunk_rd_hdr(rd);
                    uint64_t meta_bytes =
                        CHUNK_HDR_BYTES + 8ull * chunk_pos_cells(&h->g, h->pos) + CHUNK_FTR_BYTES;
                    if (!pace_bytes(sc, meta_bytes)) {
                        chunk_rd_close(rd);
                        aborted = true;
                        break;
                    }
                    uint64_t nbad = 0, cell_bytes = 0;
                    int sr = chunk_rd_scrub_ex(rd, &nbad, &bad_bitmap, &cell_bytes, cell_pace_cb, sc);
                    chunk_rd_close(rd);
                    chunk_bytes = meta_bytes + cell_bytes;
                    if (sr == CHUNKSCRUB_STOPPED) {
                        /* Interrupted mid-chunk by stop: count verified bytes, leave cursor before this chunk. */
                        pthread_mutex_lock(&sc->mu);
                        sc->bytes_total += chunk_bytes;
                        pthread_mutex_unlock(&sc->mu);
                        aborted = true;
                        break;
                    }
                    if (sr == TSDB_ERR_IO) {
#ifdef TSDB_TEST_HOOKS
                        if (chunkscrub_mutant != CHUNKSCRUB_MUT_EIOCLEAN)
#endif
                        {
                            damaged = true;
                            dmg_err = TSDB_ERR_IO;
                        }
                    } else if (sr == 0 && nbad > 0) {
                        damaged = true;
                        dmg_err = TSDB_ERR_CORRUPT;
                    } else if (sr != 0) {
                        had_list_err = true;
                        break;
                    }
                }

                if (damaged)
                    record_damage(sc, vol, oid, pos, bad_bitmap, dmg_err, pass_failed_vols, &npass_failed,
                                  sizeof pass_failed_vols / sizeof pass_failed_vols[0]);

                pthread_mutex_lock(&sc->mu);
                sc->bytes_total += chunk_bytes;
                if (!skip_deleted) {
                    sc->chunks_total++;
                    pass_chunks++;
                }
                sc->cursor_valid = true;
                sc->cursor_vol = vol;
                memcpy(sc->cursor_oid, oid, 16);
                sc->cursor_pos = pos;
                down_now = sc->marked_down;
                pthread_mutex_unlock(&sc->mu);

                since_save_chunks++;
                uint64_t now_ns = mono_ns();
                bool due_save = damaged || (save_n > 0 && since_save_chunks >= save_n) ||
                                (save_n == 0 && now_ns - last_save_ns >= CHUNKSCRUB_SAVE_INTERVAL_NS);
                if (due_save) {
                    scrub_progress_save(sc);
                    since_save_chunks = 0;
                    last_save_ns = now_ns;
                }
                if (down_now)
                    break;
            }
            free(names.v);
        }
        free(vols);

        if (aborted) {
            scrub_progress_save(sc);
            break;
        }
        if (down_now) {
            scrub_progress_save(sc);
            retry_flush(sc);
            continue;
        }

        uint64_t now_s = scrub_wall_s(sc);
        uint32_t sdays = 0, iv_ms = 0;
        uint64_t lp = 0;
        bool completed_pass = !had_list_err && (pass_chunks > 0 || had_cursor);
        bool empty_clean = !had_list_err && !completed_pass;
        pthread_mutex_lock(&sc->mu);
        if (completed_pass) {
            sc->last_pass_s = sc->pass_start_s ? sc->pass_start_s : now_s;
            sc->pass_start_s = 0; /* set when the next pass actually begins after sleep */
            if (!sc->first_pass_s)
                sc->first_pass_s = sc->last_pass_s;
            sc->cursor_valid = false;
            sc->cursor_vol = 0;
            memset(sc->cursor_oid, 0, sizeof sc->cursor_oid);
            sc->cursor_pos = 0;
        } else if (empty_clean) {
            /* An empty store with no errors is cleanly scrubbed: keep age_s fresh in memory without 2 s SCRUB fsyncs. */
            sc->last_pass_s = sc->pass_start_s ? sc->pass_start_s : now_s;
            sc->pass_start_s = 0;
            if (!sc->first_pass_s)
                sc->first_pass_s = sc->last_pass_s;
        }
        sdays = sc->scrub_days;
        iv_ms = sc->pass_interval_ms;
        lp = sc->last_pass_s;
        pthread_mutex_unlock(&sc->mu);

        /* Skip SCRUB fsync when nothing changed (e.g. an empty store polling every 2 s). */
        if (completed_pass || (had_list_err && since_save_chunks > 0))
            scrub_progress_save(sc);
        retry_flush(sc);
        if (completed_pass) {
            pthread_mutex_lock(&sc->mu);
            sc->passes_done++;
            pthread_mutex_unlock(&sc->mu);
        }

        /* Sleep until the next pass. */
        if (iv_ms > 0 || !completed_pass) {
            uint64_t wait_ms = iv_ms > 0 ? iv_ms : CHUNKSCRUB_EMPTY_POLL_MS;
            uint64_t end_ns = mono_ns() + wait_ms * 1000000ull;
            while (mono_ns() < end_ns) {
                uint64_t rem = end_ns - mono_ns();
                uint64_t slice = rem > 500000000ull ? 500000000ull : rem;
                if (!wait_interruptible_ns(sc, slice))
                    break;
                retry_flush(sc);
            }
        } else if (sdays > 0) {
            /* Production cadence: sleep until wall time reaches last_pass_s + scrub_days * 86400. */
            scrub_wait_until_due(sc, lp);
        }
    }
    pthread_mutex_lock(&sc->mu);
    sc->exited = true;
    pthread_mutex_unlock(&sc->mu);
    return NULL;
}

/* ---------------------------------------------------------------- public API */

chunk_scrub *chunk_scrub_start(chunk_store *st, const chunk_scrub_cfg *cfg)
{
    if (!st)
        return NULL;
    chunk_scrub *sc = calloc(1, sizeof *sc);
    if (!sc)
        return NULL;
    sc->st = st;
    sc->dirfd = -1;
    int sfd = chunk_store_dirfd(st);
    if (sfd >= 0) {
        sc->dirfd = openat(sfd, ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (sc->dirfd < 0)
            fprintf(stderr, "tsdb: chunk scrub %s: could not open store dirfd for SCRUB (%s)\n", chunk_store_path(st),
                    strerror(errno));
    }
    if (cfg)
        sc->cfg = *cfg;

    sc->scrub_days = sc->cfg.scrub_days ? sc->cfg.scrub_days : CHUNKSCRUB_DEFAULT_DAYS;
    sc->scrub_mb_s = sc->cfg.scrub_mb_s ? sc->cfg.scrub_mb_s : CHUNKSCRUB_DEFAULT_MB_S;
    sc->rate_bps = effective_rate_bps(sc->scrub_mb_s, sc->cfg.rate_bps_override);
    sc->scrub_out_count = sc->cfg.scrub_out_count ? sc->cfg.scrub_out_count : CHUNKSCRUB_DEFAULT_OUT_COUNT;
    sc->pass_interval_ms = sc->cfg.pass_interval_ms;
    sc->save_every_chunks = sc->cfg.save_every_chunks; /* 0 = time-based (15 s) checkpointing */

    pthread_mutex_init(&sc->mu, NULL);
    tsdb_cond_init_mono(&sc->cv);

    scrub_progress_load(sc);

    if (pthread_create(&sc->thr, NULL, scrub_main, sc) != 0) {
        if (sc->dirfd >= 0)
            close(sc->dirfd);
        pthread_cond_destroy(&sc->cv);
        pthread_mutex_destroy(&sc->mu);
        free(sc);
        return NULL;
    }
    pthread_mutex_lock(&sc->mu);
    sc->started = true;
    pthread_mutex_unlock(&sc->mu);
    return sc;
}

void chunk_scrub_stop(chunk_scrub *sc)
{
    if (!sc)
        return;
    pthread_mutex_lock(&sc->mu);
    bool was_started = sc->started;
    sc->stop = true;
    pthread_cond_broadcast(&sc->cv);
    pthread_mutex_unlock(&sc->mu);

    if (was_started)
        pthread_join(sc->thr, NULL);

    if (sc->dirfd >= 0)
        close(sc->dirfd);
    pthread_cond_destroy(&sc->cv);
    pthread_mutex_destroy(&sc->mu);
    free(sc);
}

void chunk_scrub_pause_begin(chunk_scrub *sc)
{
    if (!sc)
        return;
    pthread_mutex_lock(&sc->mu);
    sc->busy++;
    pthread_mutex_unlock(&sc->mu);
}

void chunk_scrub_pause_end(chunk_scrub *sc)
{
    if (!sc)
        return;
    pthread_mutex_lock(&sc->mu);
    if (sc->busy > 0)
        sc->busy--;
    if (sc->busy == 0)
        pthread_cond_broadcast(&sc->cv);
    pthread_mutex_unlock(&sc->mu);
}

void chunk_scrub_get_stats(chunk_scrub *sc, chunk_scrub_stats *out)
{
    if (!out)
        return;
    memset(out, 0, sizeof *out);
    if (!sc)
        return;
    uint64_t now_s = scrub_wall_s(sc);
    pthread_mutex_lock(&sc->mu);
    out->bytes_total = sc->bytes_total;
    out->chunks_total = sc->chunks_total;
    out->damaged_total = sc->damaged_total;
    out->passes_done = sc->passes_done;
    out->day_utc = sc->day_utc;
    out->day_damaged = sc->day_damaged;
    out->first_pass_s = sc->first_pass_s;
    out->pass_start_s = sc->pass_start_s;
    out->last_pass_s = sc->last_pass_s;
    out->retry_dropped = sc->retry_dropped;
    out->marked_down = sc->marked_down;
    out->running = sc->started && !sc->stop && !sc->exited;
    uint64_t ref_s = sc->last_pass_s ? sc->last_pass_s : sc->first_pass_s;
    out->age_s = (ref_s && now_s >= ref_s) ? (now_s - ref_s) : 0;
    pthread_mutex_unlock(&sc->mu);
}

void chunk_scrub_update_cfg(chunk_scrub *sc, uint32_t scrub_days, uint32_t scrub_mb_s, uint32_t scrub_out_count)
{
    if (!sc)
        return;
    pthread_mutex_lock(&sc->mu);
    if (scrub_days > 0)
        sc->scrub_days = scrub_days;
    if (scrub_mb_s > 0) {
        sc->scrub_mb_s = scrub_mb_s;
        if (sc->cfg.rate_bps_override == 0)
            sc->rate_bps = effective_rate_bps(scrub_mb_s, 0);
    }
    if (scrub_out_count > 0)
        sc->scrub_out_count = scrub_out_count;
    pthread_cond_broadcast(&sc->cv);
    pthread_mutex_unlock(&sc->mu);
}
