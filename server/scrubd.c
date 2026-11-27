/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Per-node background young-part and .ec file scrubber (server/scrubd.h; R12, r12-young).
 *
 * One background thread per server walks held cluster volumes via cluster_copy_get() + tsdb_cvol_ref(), calling
 * eng_scrub_step() with bounded byte slices paced to scrub_mb_s. Holds tsdb_cvol_ref() only for one step so
 * promotions, demotions, and snapshot installs are never stalled.
 *
 * Advisory progress (PLAN.md D4) is persisted per replica in <dir>/v<N>/SCRUB (tmp -> fsync -> rename):
 *   - Missing or damaged SCRUB restarts the pass cleanly and is never fatal.
 *   - On restart, if SCRUB recorded a damaged part, that part is re-scrubbed first (priority) because
 *     part_mark_payload() is in-memory only.
 *   - Tracks (st_dev, st_ino) of v<N> so an in-process snapshot install or replica reset clears stale damage state.
 */
#define _GNU_SOURCE
#include "scrubd.h"

#include "cluster.h"
#include "creport.h"
#include "repld.h"
#include "../src/compat.h"
#include "../src/cvol.h"
#include "../src/internal.h"
#include "../src/scrub.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define SCRUBD_DEFAULT_MB_S 20u
#define SCRUBD_DEFAULT_INTERVAL_MS 3600000u /* 1 hour between completed passes on idle volumes (tests override) */
#define SCRUBD_STEP_BYTES (1024u << 10)
#define SCRUBD_SAVE_INTERVAL_S 10u
#define SCRUBD_VOL_MAP_MAX 65536u

struct scrub_vol {
    uint32_t number;
    bool loaded;
    dev_t dir_dev;
    ino_t dir_ino;
    scrub_cursor cur;
    uint64_t first_pass_s; /* wall-clock seconds when the first pass started */
    uint64_t pass_start_s; /* wall-clock seconds when the current pass started */
    uint64_t last_pass_s;  /* wall-clock seconds of the start of the last completed whole pass (0 if none) */
    uint64_t last_save_s;
    uint64_t next_pass_ms; /* monotonic ms before starting the next pass */
    bool damaged;
    char damaged_part[96];
    uint64_t bytes_total;
    uint64_t parts_total;
    uint64_t damaged_total;
};

static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    pthread_t th;
    int started;
    int stop;
    tsdb *t;
    char dir[4096];
    uint32_t rate_mb_s;
    uint32_t interval_ms;
    struct scrub_vol vols[CLUSTER_REPORT_VOLS];
    uint32_t nvols;
    /* Lock-free O(1) maps indexed by volume number (< SCRUBD_VOL_MAP_MAX) for hot/cross-lock callers. */
    _Atomic uint8_t dmg_map[SCRUBD_VOL_MAP_MAX];
    _Atomic uint8_t min_isr_held[SCRUBD_VOL_MAP_MAX];
} S = {.mu = PTHREAD_MUTEX_INITIALIZER};

static uint64_t mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

static uint64_t wall_s(void)
{
    time_t t = time(NULL);
    return t > 0 ? (uint64_t)t : 0;
}

static void sleep_interruptible_ms(uint64_t ms)
{
    if (ms == 0)
        return;
    pthread_mutex_lock(&S.mu);
    if (!S.stop)
        tsdb_cond_timedwait_ms(&S.cv, &S.mu, (int64_t)(ms > 60000 ? 60000 : ms));
    pthread_mutex_unlock(&S.mu);
}

static void dmg_set(uint32_t vol, int val)
{
    if (vol < SCRUBD_VOL_MAP_MAX)
        atomic_store_explicit(&S.dmg_map[vol], (uint8_t)(val ? 1 : 0), memory_order_release);
}

int scrubd_vol_damaged(uint32_t vol)
{
    if (vol >= SCRUBD_VOL_MAP_MAX)
        return 0;
    return atomic_load_explicit(&S.dmg_map[vol], memory_order_acquire) != 0;
}

void scrubd_set_min_isr_held(uint32_t vol, int held)
{
    if (vol < SCRUBD_VOL_MAP_MAX)
        atomic_store_explicit(&S.min_isr_held[vol], (uint8_t)(held ? 1 : 0), memory_order_relaxed);
}

void scrubd_vol_reset(uint32_t vol)
{
    dmg_set(vol, 0);
    scrubd_set_min_isr_held(vol, 0);
    pthread_mutex_lock(&S.mu);
    for (uint32_t i = 0; i < S.nvols; i++) {
        if (S.vols[i].number == vol) {
            S.vols[i].loaded = false;
            S.vols[i].damaged = false;
            S.vols[i].damaged_part[0] = '\0';
            S.vols[i].cur = (scrub_cursor){0};
            S.vols[i].next_pass_ms = 0;
            break;
        }
    }
    pthread_mutex_unlock(&S.mu);
}

/* ---- advisory progress file v<N>/SCRUB (D4) ---- */

static void scrub_file_save_at(int dirfd, const char *root_dir, const struct scrub_vol *sv)
{
    char body[512];
    const char *dpart = (sv->damaged && sv->damaged_part[0]) ? sv->damaged_part : "-";
    int blen = snprintf(body, sizeof body,
                        "tsdb-scrub 1\n"
                        "pass_start_s %" PRIu64 "\n"
                        "last_pass_s %" PRIu64 "\n"
                        "first_pass_s %" PRIu64 "\n"
                        "cursor %" PRId64 " %" PRIu64 " %" PRIu64 " %" PRIu32 " %" PRIu32 " %u\n"
                        "damaged %u\n"
                        "damaged_part %s\n",
                        sv->pass_start_s, sv->last_pass_s, sv->first_pass_s, sv->cur.partition, sv->cur.min_block,
                        sv->cur.max_block, sv->cur.level, sv->cur.next_granule, sv->cur.started ? 1u : 0u,
                        sv->damaged ? 1u : 0u, dpart);
    if (blen <= 0 || (size_t)blen >= sizeof body)
        return;

    uint64_t h = xxh64(body, (size_t)blen, 0);
    char full[600];
    int flen = snprintf(full, sizeof full, "%send %016" PRIx64 "\n", body, h);
    if (flen <= 0 || (size_t)flen >= sizeof full)
        return;

    int fd = openat(dirfd, "SCRUB.tmp", O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0)
        return;
    int rc = write_full(fd, full, (size_t)flen);
    if (rc == 0 && tsdb_fsync(fd) != 0)
        rc = TSDB_ERR_IO;
    if (close(fd) != 0 && rc == 0)
        rc = TSDB_ERR_IO;
    /* Advisory progress file (D4): parent directory fsync after renameat is intentionally omitted — a lost rename on
     * power loss merely restarts the scrub pass cleanly and is never fatal. */
    if (rc != 0 || renameat(dirfd, "SCRUB.tmp", dirfd, "SCRUB") != 0) {
        unlinkat(dirfd, "SCRUB.tmp", 0);
        fprintf(stderr, "tsdbd: scrubd: %s/v%" PRIu32 "/SCRUB: could not save advisory progress (continuing)\n",
                root_dir, sv->number);
    }
}

static void scrub_file_load(const char *root_dir, struct scrub_vol *sv)
{
    char path[4240];
    snprintf(path, sizeof path, "%s/v%" PRIu32 "/SCRUB", root_dir, sv->number);
    uint64_t now_s = wall_s();

    sv->cur = (scrub_cursor){0};
    sv->first_pass_s = now_s;
    sv->pass_start_s = now_s;
    sv->last_pass_s = 0;
    sv->damaged = false;
    sv->damaged_part[0] = '\0';

    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return; /* missing or unreadable: advisory, start fresh */

    char buf[1024];
    ssize_t n = read(fd, buf, sizeof buf - 1);
    close(fd);
    if (n <= 0) {
        fprintf(stderr, "tsdbd: scrubd: %s: empty or unreadable progress file; restarting pass\n", path);
        return;
    }
    buf[n] = '\0';

    char *end_line = strstr(buf, "\nend ");
    if (!end_line) {
        fprintf(stderr, "tsdbd: scrubd: %s: damaged progress file (missing checksum); restarting pass\n", path);
        return;
    }
    size_t body_len = (size_t)(end_line - buf) + 1; /* includes trailing '\n' before "end " */
    uint64_t want_h = 0;
    if (sscanf(end_line + 1, "end %16" SCNx64, &want_h) != 1 || xxh64(buf, body_len, 0) != want_h) {
        fprintf(stderr, "tsdbd: scrubd: %s: damaged progress file (checksum mismatch); restarting pass\n", path);
        return;
    }

    uint64_t ps = 0, lp = 0, fp = 0, minb = 0, maxb = 0;
    int64_t part_k = 0;
    uint32_t lvl = 0, ng = 0, started = 0, dmg = 0;
    char dpart[96] = "-";
    if (sscanf(buf,
               "tsdb-scrub 1\n"
               "pass_start_s %" SCNu64 "\n"
               "last_pass_s %" SCNu64 "\n"
               "first_pass_s %" SCNu64 "\n"
               "cursor %" SCNd64 " %" SCNu64 " %" SCNu64 " %" SCNu32 " %" SCNu32 " %" SCNu32 "\n"
               "damaged %" SCNu32 "\n"
               "damaged_part %95s\n",
               &ps, &lp, &fp, &part_k, &minb, &maxb, &lvl, &ng, &started, &dmg, dpart) != 11) {
        fprintf(stderr, "tsdbd: scrubd: %s: malformed progress file; restarting pass\n", path);
        return;
    }

    sv->pass_start_s = ps ? ps : now_s;
    sv->last_pass_s = lp;
    sv->first_pass_s = fp ? fp : (lp ? lp : now_s);
    sv->cur.partition = part_k;
    sv->cur.min_block = minb;
    sv->cur.max_block = maxb;
    sv->cur.level = lvl;
    sv->cur.next_granule = ng;
    sv->cur.started = started != 0;
    if (dmg && strcmp(dpart, "-") != 0) {
        /* part_mark_payload() is in-memory only: re-scrub the previously damaged part FIRST after restart. */
        snprintf(sv->cur.priority, sizeof sv->cur.priority, "%s", dpart);
        sv->cur.priority_granule = 0;
        snprintf(sv->damaged_part, sizeof sv->damaged_part, "%s", dpart);
    }
}

/* ---- volume table helpers ---- */

static struct scrub_vol *vol_get_or_add_locked(uint32_t number)
{
    for (uint32_t i = 0; i < S.nvols; i++)
        if (S.vols[i].number == number)
            return &S.vols[i];
    if (S.nvols >= CLUSTER_REPORT_VOLS)
        return NULL;
    struct scrub_vol *sv = &S.vols[S.nvols++];
    memset(sv, 0, sizeof *sv);
    sv->number = number;
    return sv;
}

static bool copy_holds_vol(const struct cluster_copy *cp, uint32_t *out_vols, uint32_t *out_n)
{
    *out_n = 0;
    if (!cp || !cp->self || cp->applied == 0)
        return false;
    for (uint32_t i = 0; i < cp->nvolumes && *out_n < CLUSTER_REPORT_VOLS; i++) {
        const struct cluster_copy_vol *v = &cp->volumes[i];
        if (v->kind != META_VOL_REPLICATED)
            continue;
        for (uint32_t k = 0; k < v->nrep; k++) {
            if (v->rep[k].node == cp->self) {
                out_vols[(*out_n)++] = v->number;
                break;
            }
        }
    }
    return true;
}

/* Prunes volumes no longer held by this server from S.vols and clears their dmg/min_isr_held flags. */
static void prune_unheld_locked(const uint32_t *held, uint32_t nheld)
{
    for (uint32_t i = 0; i < S.nvols;) {
        uint32_t num = S.vols[i].number;
        bool found = false;
        for (uint32_t j = 0; j < nheld; j++) {
            if (held[j] == num) {
                found = true;
                break;
            }
        }
        if (!found) {
            dmg_set(num, 0);
            scrubd_set_min_isr_held(num, 0);
            S.vols[i] = S.vols[--S.nvols];
        } else {
            i++;
        }
    }
}

/* ---- Prometheus metrics collectors (server/creport.h) ---- */

enum scrub_metric_kind {
    SM_BYTES = 0,
    SM_PARTS,
    SM_DAMAGED,
    SM_AGE,
    SM_MIN_ISR_HELD,
};

struct scrub_metric_row {
    uint32_t number;
    uint64_t bytes_total;
    uint64_t parts_total;
    uint64_t damaged_total;
    uint64_t ref_s;
    uint8_t min_isr_held;
};

static void scrubd_collect_metric(void *ctx, struct creport_sink *sink)
{
    int kind = (int)(intptr_t)ctx;
    struct scrub_metric_row rows[CLUSTER_REPORT_VOLS];
    uint32_t nrows = 0;

    const struct cluster_copy *cp = cluster_copy_get();
    if (!cp || !cp->self) {
        cluster_copy_put(cp);
        return;
    }

    uint32_t held[CLUSTER_REPORT_VOLS];
    uint32_t nheld = 0;
    copy_holds_vol(cp, held, &nheld);
    cluster_copy_put(cp);

    pthread_mutex_lock(&S.mu);
    for (uint32_t i = 0; i < nheld && nrows < CLUSTER_REPORT_VOLS; i++) {
        uint32_t num = held[i];
        struct scrub_metric_row r = {.number = num};
        for (uint32_t j = 0; j < S.nvols; j++) {
            if (S.vols[j].number == num) {
                r.bytes_total = S.vols[j].bytes_total;
                r.parts_total = S.vols[j].parts_total;
                r.damaged_total = S.vols[j].damaged_total;
                r.ref_s = S.vols[j].last_pass_s ? S.vols[j].last_pass_s : S.vols[j].first_pass_s;
                break;
            }
        }
        if (num < SCRUBD_VOL_MAP_MAX)
            r.min_isr_held = atomic_load_explicit(&S.min_isr_held[num], memory_order_relaxed);
        rows[nrows++] = r;
    }
    pthread_mutex_unlock(&S.mu);

    uint64_t now_s = wall_s();
    for (uint32_t i = 0; i < nrows; i++) {
        char vbuf[16];
        snprintf(vbuf, sizeof vbuf, "%" PRIu32, rows[i].number);
        struct creport_label lbl = {"volume", vbuf};
        switch (kind) {
        case SM_BYTES:
            creport_sink_put_u64(sink, &lbl, 1, rows[i].bytes_total);
            break;
        case SM_PARTS:
            creport_sink_put_u64(sink, &lbl, 1, rows[i].parts_total);
            break;
        case SM_DAMAGED:
            creport_sink_put_u64(sink, &lbl, 1, rows[i].damaged_total);
            break;
        case SM_AGE: {
            double age = (rows[i].ref_s && now_s >= rows[i].ref_s) ? (double)(now_s - rows[i].ref_s) : 0.0;
            creport_sink_put(sink, &lbl, 1, age);
            break;
        }
        case SM_MIN_ISR_HELD:
            creport_sink_put_u64(sink, &lbl, 1, rows[i].min_isr_held);
            break;
        default:
            break;
        }
    }
}

/* ---- scrubber thread ---- */

static void *scrubd_main(void *arg)
{
    (void)arg;
    uint32_t rr = 0; /* round-robin index across held volumes */

    for (;;) {
        pthread_mutex_lock(&S.mu);
        int stopping = S.stop;
        uint32_t rate_mb_s = S.rate_mb_s;
        uint32_t interval_ms = S.interval_ms;
        pthread_mutex_unlock(&S.mu);
        if (stopping)
            break;

        const struct cluster_copy *cp = cluster_copy_get();
        uint32_t held[CLUSTER_REPORT_VOLS];
        uint32_t nheld = 0;
        bool have_copy = copy_holds_vol(cp, held, &nheld);
        cluster_copy_put(cp);

        if (have_copy) {
            pthread_mutex_lock(&S.mu);
            prune_unheld_locked(held, nheld);
            pthread_mutex_unlock(&S.mu);
        }

        if (nheld == 0) {
            sleep_interruptible_ms(200);
            continue;
        }

        bool did_work = false;
        uint64_t bytes_step = 0;
        uint64_t now_ms_val = mono_ms();
        uint64_t min_wait_ms = interval_ms;

        for (uint32_t k = 0; k < nheld; k++) {
            pthread_mutex_lock(&S.mu);
            stopping = S.stop;
            pthread_mutex_unlock(&S.mu);
            if (stopping)
                break;

            uint32_t vol = held[(rr + k) % nheld];

            struct eng *db = tsdb_cvol_ref(S.t, vol);
            if (!db)
                continue; /* not open yet, or being swapped/installed */

            /* fstat the held engine's dirfd (no path-resolution TOCTOU race with snapshot install / replica reset). */
            struct stat vst;
            if (fstat(db->dirfd, &vst) != 0) {
                tsdb_cvol_unref(&db);
                continue;
            }

            struct scrub_vol local;
            bool need_load = false;
            pthread_mutex_lock(&S.mu);
            struct scrub_vol *sv = vol_get_or_add_locked(vol);
            if (!sv) {
                pthread_mutex_unlock(&S.mu);
                tsdb_cvol_unref(&db);
                continue;
            }
            if (!sv->loaded || sv->dir_dev != vst.st_dev || sv->dir_ino != vst.st_ino) {
                sv->dir_dev = vst.st_dev;
                sv->dir_ino = vst.st_ino;
                sv->loaded = true;
                sv->next_pass_ms = 0;
                dmg_set(vol, 0);
                scrubd_set_min_isr_held(vol, 0);
                need_load = true;
            }
            local = *sv;
            pthread_mutex_unlock(&S.mu);

            if (need_load) {
                scrub_file_load(S.dir, &local);
                pthread_mutex_lock(&S.mu);
                sv = vol_get_or_add_locked(vol);
                if (sv && sv->dir_dev == local.dir_dev && sv->dir_ino == local.dir_ino)
                    *sv = local;
                pthread_mutex_unlock(&S.mu);
            }

            char prio_name[96];
            snprintf(prio_name, sizeof prio_name, "%s", local.cur.priority);
            bool has_priority = prio_name[0] != '\0';
            if (!has_priority && local.next_pass_ms > now_ms_val) {
                uint64_t rem = local.next_pass_ms - now_ms_val;
                if (rem < min_wait_ms)
                    min_wait_ms = rem;
                tsdb_cvol_unref(&db);
                continue;
            }

            scrub_report rep;
            int rc = eng_scrub_step(db, SCRUBD_STEP_BYTES, &local.cur, &rep);
            if (rc != 0) {
                tsdb_cvol_unref(&db);
                continue;
            }

            did_work = true;
            bytes_step += rep.bytes;
            rr = (rr + k + 1) % nheld;

            uint64_t now_s = wall_s();
            bool save_now = false;
            bool notify_damage = false;

            local.bytes_total += rep.bytes;
            local.parts_total += rep.parts_done;

            if (rep.damaged) {
                if (rep.newly_damaged)
                    local.damaged_total++;
                bool newly_vol_damaged = !local.damaged;
                local.damaged = true;
                snprintf(local.damaged_part, sizeof local.damaged_part, "%s", rep.part_name);
                dmg_set(vol, 1);
                if (newly_vol_damaged || rep.newly_damaged) {
                    fprintf(stderr, "tsdbd: scrubd: volume %" PRIu32 ": young scrub found damage in %s (%s)\n", vol,
                            rep.part_name, tsdb_strerror(rep.err));
                    save_now = true;
                    notify_damage = true;
                }
            } else if (has_priority && local.cur.priority[0] == '\0') {
                /* Clear a persisted damage flag ONLY if that exact priority part was still present and verified
                 * cleanly in full. If it vanished before restart re-scrub (e.g. merged away while in-memory
                 * part_mark_payload was absent, potentially laundering bad values), keep the volume DAMAGED until
                 * replica reset or snapshot install replaces the directory. */
                bool exact_clean = !rep.skipped && rep.parts_done == 1 && strcmp(rep.part_name, prio_name) == 0;
                if (exact_clean && !local.damaged) {
                    local.damaged_part[0] = '\0';
                    dmg_set(vol, 0);
                    save_now = true;
                    notify_damage = true;
                } else if (!exact_clean) {
                    local.damaged = true;
                    snprintf(local.damaged_part, sizeof local.damaged_part, "%s", prio_name);
                    dmg_set(vol, 1);
                    fprintf(stderr,
                            "tsdbd: scrubd: volume %" PRIu32
                            ": previously damaged part %s vanished before restart re-scrub; keeping volume DAMAGED\n",
                            vol, prio_name);
                    save_now = true;
                    notify_damage = true;
                }
            }

            if (rep.pass_done) {
                local.last_pass_s = local.pass_start_s ? local.pass_start_s : now_s;
                local.pass_start_s = now_s;
                if (!local.first_pass_s)
                    local.first_pass_s = local.last_pass_s;
                local.next_pass_ms = mono_ms() + interval_ms;
                save_now = true;
            } else if (now_s >= local.last_save_s + SCRUBD_SAVE_INTERVAL_S) {
                save_now = true;
            }

            if (save_now) {
                local.last_save_s = now_s;
                /* Save through db->dirfd while holding the engine ref so a concurrent snapshot install / replica reset
                 * can never receive a stale SCRUB file written into its new directory. */
                scrub_file_save_at(db->dirfd, S.dir, &local);
            }
            tsdb_cvol_unref(&db);

            pthread_mutex_lock(&S.mu);
            sv = vol_get_or_add_locked(vol);
            if (sv && sv->dir_dev == local.dir_dev && sv->dir_ino == local.dir_ino)
                *sv = local;
            pthread_mutex_unlock(&S.mu);

            if (notify_damage) {
                cluster_beat_soon();
                repld_wake_vol(vol);
            }
            break; /* pace per step across volumes */
        }

        if (!did_work) {
            sleep_interruptible_ms(min_wait_ms > 500 ? 500 : (min_wait_ms ? min_wait_ms : 50));
        } else if (bytes_step > 0 && rate_mb_s > 0) {
            uint64_t rate_bps = (uint64_t)rate_mb_s * (1024ull * 1024ull);
            uint64_t pace_ms = (bytes_step * 1000ull) / rate_bps;
            if (pace_ms > 0)
                sleep_interruptible_ms(pace_ms);
        }
    }
    return NULL;
}

/* Preloads existing v<N>/SCRUB files before repld/cvol start so a previously DAMAGED replica never briefly reports
 * healthy or rejoins the ISR on restart before its priority re-scrub finishes. Any volume directory this node no
 * longer holds is pruned once the first applied metadata copy arrives. */
static void scrubd_preload_locked(void)
{
    DIR *d = opendir(S.dir);
    if (!d)
        return;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        if (e->d_name[0] != 'v' || e->d_name[1] < '1' || e->d_name[1] > '9')
            continue;
        char *end = NULL;
        unsigned long v = strtoul(e->d_name + 1, &end, 10);
        if (!end || *end != '\0' || v == 0 || v >= SCRUBD_VOL_MAP_MAX)
            continue;
        char vdir[4224];
        int n = snprintf(vdir, sizeof vdir, "%s/%s", S.dir, e->d_name);
        if (n <= 0 || (size_t)n >= sizeof vdir)
            continue;
        struct stat vst;
        if (stat(vdir, &vst) != 0 || !S_ISDIR(vst.st_mode))
            continue;
        struct scrub_vol *sv = vol_get_or_add_locked((uint32_t)v);
        if (!sv)
            continue;
        sv->dir_dev = vst.st_dev;
        sv->dir_ino = vst.st_ino;
        sv->loaded = true;
        sv->next_pass_ms = 0;
        scrub_file_load(S.dir, sv);
        if (sv->cur.priority[0] != '\0')
            dmg_set((uint32_t)v, 1);
    }
    closedir(d);
}

int scrubd_start(tsdb *t, const char *dir)
{
    if (!t || !dir || !*dir)
        return -1;

    pthread_mutex_lock(&S.mu);
    if (S.started) {
        pthread_mutex_unlock(&S.mu);
        return 0;
    }
    tsdb_cond_init_mono(&S.cv);
    S.t = t;
    snprintf(S.dir, sizeof S.dir, "%s", dir);
    S.stop = 0;
    S.nvols = 0;
    S.rate_mb_s = SCRUBD_DEFAULT_MB_S;
    S.interval_ms = SCRUBD_DEFAULT_INTERVAL_MS;
    for (uint32_t i = 0; i < SCRUBD_VOL_MAP_MAX; i++) {
        atomic_store_explicit(&S.dmg_map[i], 0, memory_order_relaxed);
        atomic_store_explicit(&S.min_isr_held[i], 0, memory_order_relaxed);
    }
    scrubd_preload_locked();

#ifdef TSDB_TEST_HOOKS
    const char *te = getenv("TSDB_SCRUB_TEST");
    if (te) {
        const char *p;
        unsigned v = 0;
        if ((p = strstr(te, "mb_s=")) != NULL && sscanf(p, "mb_s=%u", &v) == 1 && v > 0)
            S.rate_mb_s = v;
        if ((p = strstr(te, "interval_ms=")) != NULL && sscanf(p, "interval_ms=%u", &v) == 1 && v > 0)
            S.interval_ms = v;
    }
    const char *mu = getenv("TSDB_SCRUB_MUTANT");
    if (mu && strcmp(mu, "youngskipgranule") == 0)
        scrub_test_mutant = SCRUB_MUT_SKIPGRANULE;
    else if (mu && strcmp(mu, "youngmmap") == 0)
        scrub_test_mutant = SCRUB_MUT_MMAP;
#endif
    pthread_mutex_unlock(&S.mu);

    creport_metric_collector("tsdb_young_scrub_bytes_total", CREPORT_COUNTER,
                             "Total bytes verified by young-part and .ec scrub on this held volume.",
                             scrubd_collect_metric, (void *)(intptr_t)SM_BYTES);
    creport_metric_collector("tsdb_young_scrub_parts_total", CREPORT_COUNTER,
                             "Total young and .ec parts verified by scrub on this held volume.", scrubd_collect_metric,
                             (void *)(intptr_t)SM_PARTS);
    creport_metric_collector("tsdb_young_scrub_damaged_total", CREPORT_COUNTER,
                             "Total damaged young or .ec parts found by scrub on this held volume.",
                             scrubd_collect_metric, (void *)(intptr_t)SM_DAMAGED);
    creport_metric_collector("tsdb_young_scrub_age_seconds", CREPORT_GAUGE,
                             "Seconds since the start of the last completed young-part scrub pass (or first pass).",
                             scrubd_collect_metric, (void *)(intptr_t)SM_AGE);
    creport_metric_collector("tsdb_young_scrub_min_isr_held", CREPORT_GAUGE,
                             "1 while this damaged replica is held in the ISR at min_isr, else 0.",
                             scrubd_collect_metric, (void *)(intptr_t)SM_MIN_ISR_HELD);

    if (pthread_create(&S.th, NULL, scrubd_main, NULL) != 0) {
        fprintf(stderr, "tsdbd: scrubd: cannot start young scrub thread\n");
        creport_metric_collector_del("tsdb_young_scrub_bytes_total");
        creport_metric_collector_del("tsdb_young_scrub_parts_total");
        creport_metric_collector_del("tsdb_young_scrub_damaged_total");
        creport_metric_collector_del("tsdb_young_scrub_age_seconds");
        creport_metric_collector_del("tsdb_young_scrub_min_isr_held");
        return -1;
    }
    pthread_mutex_lock(&S.mu);
    S.started = 1;
    pthread_mutex_unlock(&S.mu);
    return 0;
}

void scrubd_stop(void)
{
    pthread_mutex_lock(&S.mu);
    if (!S.started) {
        pthread_mutex_unlock(&S.mu);
        return;
    }
    S.stop = 1;
    pthread_cond_broadcast(&S.cv);
    pthread_mutex_unlock(&S.mu);

    pthread_join(S.th, NULL);

    creport_metric_collector_del("tsdb_young_scrub_bytes_total");
    creport_metric_collector_del("tsdb_young_scrub_parts_total");
    creport_metric_collector_del("tsdb_young_scrub_damaged_total");
    creport_metric_collector_del("tsdb_young_scrub_age_seconds");
    creport_metric_collector_del("tsdb_young_scrub_min_isr_held");

    pthread_mutex_lock(&S.mu);
    S.started = 0;
    S.nvols = 0;
    S.t = NULL;
    /* Keep S.dmg_map and S.min_isr_held intact during shutdown: cluster_stop()/cvol_stop() run right after
     * scrubd_stop(), so repld must not observe a spurious damaged->0 transition on a final shutdown ACK.
     * scrubd_start() resets both maps before preloading on any fresh start. */
    pthread_cond_destroy(&S.cv);
    pthread_mutex_unlock(&S.mu);
}
