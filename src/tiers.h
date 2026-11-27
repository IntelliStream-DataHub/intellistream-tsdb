/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * TIERS: durable append-only record of a single-server database's local settled tier
 * (docs/design-cluster-data.md §15.7, E55, E58; R15 D2, D3, D18).
 *
 * File: <root>/TIERS, beside VOLUMES, written under VOLUMES's OFD write lock.
 *
 * Each record is 8-byte aligned:
 *   [u32 payload_len][u8 type][payload: payload_len bytes][0..7 zero pad][u64 xxh64]
 * where xxh64 covers everything before it, seeded with (file_offset ^ TIERS_XXH_SEED).
 *
 * A valid record following a damaged region is corruption (TSDB_ERR_CORRUPT, open fails);
 * trailing bytes with no valid record after them are a torn append, truncated on writer
 * open or before the next append under the VOLUMES write lock.
 *
 * Record types:
 *   TIERS_REC_STORE    { id[16], class, flags(single_copy), path }
 *   TIERS_REC_STATE    { id[16], state(LOST|REPLACED_BY), cls, flags, new_id[16], new_path }
 *   TIERS_REC_SETTINGS { volume (or TIERS_VOL_ALL), mask, k, m, tier_after_s,
 *                        tier_high_pct, young_high_pct, rebuild_mb_s, scrub_mb_s, scrub_days }
 */
#ifndef TSDB_TIERS_H
#define TSDB_TIERS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define TIERS_FILE "TIERS"
#define TIERS_STORES_MAX 64
#define TIERS_VOL_ALL UINT32_MAX
#define TIERS_PAYLOAD_MAX 8192u
#define TIERS_XXH_SEED 0x5449455253303031ull

/* Default tier settings (PLAN.md D6, D7, D12, D20). */
#define TIERS_AFTER_S_DEFAULT 604800ull /* 7 days */
#define TIERS_HIGH_PCT_DEFAULT 80u
#define TIERS_YOUNG_HIGH_PCT_DEFAULT 90u
#define TIERS_REBUILD_MB_S_DEFAULT 200u
#define TIERS_SCRUB_MB_S_DEFAULT 50u
#define TIERS_SCRUB_DAYS_DEFAULT 14u

enum {
    TIERS_REC_STORE = 1,
    TIERS_REC_STATE = 2,
    TIERS_REC_SETTINGS = 3,
};

enum {
    TIERS_STORE_SINGLE_COPY = 1u << 0,
};

enum {
    TIERS_STATE_ACTIVE = 0,
    TIERS_STATE_LOST = 1,
    TIERS_STATE_REPLACED_BY = 2,
};

typedef struct tiers_store {
    uint8_t id[16];          /* store id at this placement slot (or lost id if REPLACED_BY) */
    uint8_t replaced_by[16]; /* replacement store id when state == TIERS_STATE_REPLACED_BY */
    uint8_t cls;             /* TSDB_TIER_CLASS_* */
    bool single_copy;
    uint8_t state;           /* TIERS_STATE_* */
    char *path;              /* current store path (replacement's path when REPLACED_BY) */
    char *orig_path;         /* original store path before replacement (NULL if never replaced) */
} tiers_store;

typedef struct tiers_settings {
    uint32_t set_mask;       /* TSDB_TIER_SET_* bits explicitly recorded */
    uint16_t k, m;           /* 0, 0 when not set */
    uint64_t tier_after_s;
    uint32_t tier_high_pct;
    uint32_t young_high_pct;
    uint32_t rebuild_mb_s;
    uint32_t scrub_mb_s;
    uint32_t scrub_days;
} tiers_settings;

typedef struct tiers_vol_settings {
    uint32_t volume;
    tiers_settings s;
} tiers_vol_settings;

typedef struct tiers_view {
    bool exists;             /* true if TIERS file existed on disk */
    tiers_store *stores;
    size_t nstores;
    tiers_settings global;   /* database-wide settings (defaults filled for unset bits) */
    tiers_vol_settings *vols;
    size_t nvols;
    uint64_t file_end;       /* byte offset of end of valid records */
    /* Historical store ids seen across STORE and REPLACED_BY records (internal duplicate guard). */
    uint8_t (*seen_ids)[16];
    size_t nseen_ids;
} tiers_view;

/* Class helpers (D18: NULL/""/"unknown" -> UNKNOWN, "nvme", "ssd", "hdd"). */
int tiers_class_parse(const char *s, uint8_t *out);
const char *tiers_class_name(uint8_t cls);

/* Path validation: absolute, < PATH_MAX, no control chars, no "." or ".." components. */
int tiers_path_valid(const char *p);

/* View lifecycle and effective per-volume settings lookup. */
void tiers_view_init(tiers_view *v);
void tiers_view_free(tiers_view *v);
void tiers_settings_defaults(tiers_settings *s);
void tiers_view_settings_for(const tiers_view *v, uint32_t volume, tiers_settings *out);

/* Pure record encoding at file offset `off`. Returns encoded byte length (>0) or negative TSDB_ERR_*. */
int tiers_encode_store(uint64_t off, const uint8_t id[16], uint8_t cls, bool single_copy, const char *path,
                       uint8_t *out, size_t cap);
int tiers_encode_state(uint64_t off, const uint8_t id[16], uint8_t state, uint8_t new_cls, bool new_single_copy,
                       const uint8_t new_id[16], const char *new_path, uint8_t *out, size_t cap);
int tiers_encode_settings(uint64_t off, uint32_t volume, const tiers_settings *s, uint8_t *out, size_t cap);

/* Pure in-memory parser of a TIERS file image (used by tiers_load_fd and fuzz_tiers).
 * Populates `out` and sets `*good_end` to the end of valid records. Returns 0 (even when a torn tail
 * exists: *good_end < size) or TSDB_ERR_CORRUPT (logging `label` to stderr) / TSDB_ERR_NOMEM. */
int tiers_parse_buf(const uint8_t *buf, size_t size, const char *label, tiers_view *out, size_t *good_end);

/* Load TIERS from an open fd (with optional torn-tail truncation when `cut` is true). */
int tiers_load_fd(int fd, bool cut, bool sync, const char *label, tiers_view *out);

/* Load <rootfd>/TIERS (absent -> empty view with exists=false, rc=0). */
int tiers_load(int rootfd, bool cut, bool sync, tiers_view *out);

/* Append a single record to <rootfd>/TIERS. Caller MUST hold the VOLUMES OFD write lock!
 * Creates TIERS if absent, cuts any torn tail first, validates against current view, writes,
 * fdatasyncs TIERS, and fsyncs rootfd when TIERS was newly created (or previously empty). */
int tiers_append_store(int rootfd, bool sync, const uint8_t id[16], uint8_t cls, bool single_copy, const char *path);
int tiers_append_state(int rootfd, bool sync, const uint8_t id[16], uint8_t state, uint8_t new_cls,
                       bool new_single_copy, const uint8_t new_id[16], const char *new_path);
int tiers_append_settings(int rootfd, bool sync, uint32_t volume, const tiers_settings *s);

/* Root META db_id=<32-hex> helpers (D2, D3).
 * tiers_db_id_read: 0 with *has_out true/false; TSDB_ERR_NOTFOUND if META missing; TSDB_ERR_CORRUPT if malformed.
 * tiers_db_id_ensure: returns existing db_id or writes a fresh random 16-byte db_id atomically
 * (tmp -> fsync -> rename -> dir fsync). Caller holds VOLUMES OFD write lock. */
int tiers_db_id_read(int rootfd, bool *has_out, uint8_t id_out[16]);
int tiers_db_id_ensure(int rootfd, bool sync, uint8_t id_out[16]);

/* Internal accessor for an open tsdb handle's db_id (loaded at tsdb_open). */
struct tsdb;
bool tiers_tsdb_db_id(const struct tsdb *db, uint8_t out[16]);

/* Planted mutants for test_tiers --mutant NAME (Makefile tiers-mutants). Active only under TSDB_TEST_HOOKS. */
enum {
    TIERS_MUT_NONE = 0,
    TIERS_MUT_NOSYNC,  /* skips fsync/fdatasync in TIERS / db_id writes: caught by crashfs sweep */
    TIERS_MUT_NODBID,  /* initializes store META without db=<hex>: caught by cross-db binding check */
    TIERS_MUT_SAMEDEV, /* skips st_dev device check in tsdb_tier_add: caught by same-device refusal test */
};
#ifdef TSDB_TEST_HOOKS
extern int tiers_test_mutant;
#endif

#endif
