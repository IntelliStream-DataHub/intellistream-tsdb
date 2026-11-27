/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Per-store background chunk scrubber (R12, r12-chunk; docs/design-cluster-data.md §5.9, E25, E26; PLAN.md D2-D4, D6,
 * D9, D10).
 *
 * One scrub thread per open chunk_store:
 *   - Walks every volume in the store (names listing, sorted deterministically by (volume, oid, pos)), verifying each
 *     chunk's header, size, sums table, footer, and every cell's xxh64 via chunk_rd_scrub_ex.
 *   - Paced by a per-store virtual-time token bucket at scrub_mb_s (default 50 MiB/s), completing passes within
 *     scrub_days (default 14 days), and pausing without burst accumulation while a heal/fill, remote chunk read
 *     (CHUNKR), or put burst (CHUNKB) uses the store (hot replica snapshots do not read chunk stores directly).
 *   - Persists advisory progress in <store>/SCRUB (tmp, fsync, rename, dir fsync on an independent store dirfd);
 *     resumes from the saved cursor on restart; a missing or damaged SCRUB logs and restarts the pass cleanly (never
 *     fatal).
 *   - Reports each damaged chunk (bad cell, header, size, sums, footer, or EIO — never counted clean) once per pass
 *     via on_damage (retrying from a bounded in-memory queue when the leader is unreachable; reports already past the
 *     persisted cursor after a crash are re-discovered on the next pass). Leader-side active cell repair orchestration
 *     belongs to the separate repair/rebuild track; r12-chunk delivers detection, reporting, and local DOWN marking.
 *   - Tracks damage count per store and UTC day; above scrub_out_count (default 8) marks the store DOWN locally via
 *     chunk_be_mark_down (so reads decode around it), stops walking the failing store, and logs. `marked_down` is
 *     in-memory/local (cleared on restart, while `day_damaged` is persisted in SCRUB so the next damage that UTC day
 *     re-triggers local DOWN).
 */
#ifndef TSDB_CHUNKSCRUB_H
#define TSDB_CHUNKSCRUB_H

#include "chunk.h"

#include <stdbool.h>
#include <stdint.h>

/* Default scrub settings (D2; r13-meta later feeds cluster settings here). */
#define CHUNKSCRUB_DEFAULT_DAYS 14u
#define CHUNKSCRUB_DEFAULT_MB_S 50u
#define CHUNKSCRUB_DEFAULT_OUT_COUNT 8u

#ifdef TSDB_TEST_HOOKS
/* Planted mutants for test_chunkscrub --mutant NAME (tests only; omitted when TSDB_TEST_HOOKS is off). */
enum {
    CHUNKSCRUB_MUT_NONE = 0,
    CHUNKSCRUB_MUT_SKIPCELL, /* scrubskipcell: skips verifying the last cell of each chunk */
    CHUNKSCRUB_MUT_EIOCLEAN, /* scrubeioclean: counts EIO during scrub as clean instead of damaged */
    CHUNKSCRUB_MUT_RESTART,  /* scrubrestart: ignores persisted SCRUB cursor on start/restart */
};
extern int chunkscrub_mutant;
int chunkscrub_mutant_parse(const char *name);
/* Optional one-shot test fault hook: fail chunk_store_list_names once for this volume (>0) and reset to 0. */
extern int chunkscrub_fault_list_vol;
#endif

/* A damaged chunk report emitted by chunk_scrub (once per pass per damaged chunk). */
typedef struct chunk_scrub_damage {
    int store_idx;
    uint32_t volume;
    uint8_t oid[16];
    uint16_t pos;
    uint64_t bad_cells; /* bit i (0..62) = bad cell i, bit 63 = bad cell >= 63; 0 = header/size/sums/footer/EIO */
    int err;            /* TSDB_ERR_CORRUPT or TSDB_ERR_IO */
} chunk_scrub_damage;

/* Callback delivering a damaged chunk report (e.g. CHUNK_DAMAGED over R8's CHUNK link).
 * Returns 0 when acknowledged/delivered; non-zero when unreachable (retried next round). */
typedef int (*chunk_scrub_report_fn)(void *ctx, const chunk_scrub_damage *dmg);

/* Optional callback invoked once when a store's UTC-day damage count exceeds scrub_out_count. */
typedef void (*chunk_scrub_out_fn)(void *ctx, int store_idx, uint64_t day_damaged);

/* Optional callback returning true while scrub should pause for this store (heal, put burst, snapshot). */
typedef bool (*chunk_scrub_paused_fn)(void *ctx, int store_idx);

/* Optional clock hook for tests (returns UTC wall seconds; NULL uses time(NULL)). */
typedef uint64_t (*chunk_scrub_wall_fn)(void *ctx);

typedef struct chunk_scrub_cfg {
    int store_idx;              /* store index in its chunk_backend (for callbacks/logging) */
    uint32_t scrub_days;        /* target days per pass (0: CHUNKSCRUB_DEFAULT_DAYS) */
    uint32_t scrub_mb_s;        /* read rate cap in MiB/s (0: CHUNKSCRUB_DEFAULT_MB_S) */
    uint64_t rate_bps_override; /* optional exact bytes/s rate (0: use scrub_mb_s * 1 MiB/s) */
    uint32_t scrub_out_count;   /* UTC-day damage count above which store is marked DOWN (0: CHUNKSCRUB_DEFAULT_OUT_COUNT) */
    uint32_t pass_interval_ms;  /* optional test override for sleep between passes (0: scrub_days * 86400 s) */
    uint32_t save_every_chunks; /* persist SCRUB every N chunks (0: default 4) */
    chunk_backend *be;          /* optional backend: marks store_idx DOWN_DAMAGED when day_damaged > scrub_out_count */
    chunk_scrub_report_fn on_damage;
    chunk_scrub_out_fn on_out;
    chunk_scrub_paused_fn is_paused;
    chunk_scrub_wall_fn wall_s;
    void *ctx;
} chunk_scrub_cfg;

typedef struct chunk_scrub_stats {
    uint64_t bytes_total;   /* total chunk bytes verified since start */
    uint64_t chunks_total;  /* total chunks verified since start */
    uint64_t damaged_total; /* total damaged chunks detected since start */
    uint64_t passes_done;   /* completed whole passes since start */
    uint64_t day_utc;       /* current UTC day number (wall_s / 86400) */
    uint64_t day_damaged;   /* damaged chunks found on day_utc */
    uint64_t first_pass_s;  /* UTC wall seconds when first pass started */
    uint64_t pass_start_s;  /* UTC wall seconds when current pass started */
    uint64_t last_pass_s;   /* UTC wall seconds of start of last completed whole pass (0 if none) */
    uint64_t age_s;         /* oldest unscrubbed age in seconds = now - (last_pass_s ? last_pass_s : first_pass_s) */
    uint64_t retry_dropped; /* damaged reports dropped because the bounded retry queue was full */
    bool marked_down;       /* true once day_damaged > scrub_out_count triggered local DOWN */
    bool running;
} chunk_scrub_stats;

typedef struct chunk_scrub chunk_scrub;

/* Starts a background scrub thread for `st` (st must outlive chunk_scrub_stop). Returns NULL on failure. */
chunk_scrub *chunk_scrub_start(chunk_store *st, const chunk_scrub_cfg *cfg);

/* Stops and joins the scrub thread, persisting final advisory SCRUB progress, and frees `sc`. Safe on NULL. */
void chunk_scrub_stop(chunk_scrub *sc);

/* Pause/resume helpers (reference-counted): while busy count > 0, scrub pauses without accumulating burst tokens. */
void chunk_scrub_pause_begin(chunk_scrub *sc);
void chunk_scrub_pause_end(chunk_scrub *sc);

/* Thread-safe snapshot of counters and oldest unscrubbed age. */
void chunk_scrub_get_stats(chunk_scrub *sc, chunk_scrub_stats *out);

/* Live config update (for future r14-settings): updates rate/days/out_count thread-safely. */
void chunk_scrub_update_cfg(chunk_scrub *sc, uint32_t scrub_days, uint32_t scrub_mb_s, uint32_t scrub_out_count);

#endif /* TSDB_CHUNKSCRUB_H */
