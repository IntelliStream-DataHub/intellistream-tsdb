/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Engine young-part and .ec file scrubber (R12, r12-young; design-cluster-data.md §5.9, PLAN.md D7).
 *
 * Verifies one listed part at a time without holding db->lock, publish_lock, or latest_rw across I/O, using pread on
 * a fresh fd (never the part's mmap, so storage EIO returns an error instead of SIGBUS). On corruption or EIO, marks
 * the part damaged via part_mark_payload() and populates the report.
 */
#ifndef TSDB_SCRUB_H
#define TSDB_SCRUB_H

#include "internal.h"

#include <stdbool.h>
#include <stdint.h>

/* Planted mutants for test_scrub --mutant NAME (tests only; ignored when TSDB_TEST_HOOKS is off). */
enum {
    SCRUB_MUT_NONE = 0,
    SCRUB_MUT_SKIPGRANULE, /* youngskipgranule: skips verifying the last granule of a .part */
    SCRUB_MUT_MMAP,        /* youngmmap: reads through p->map instead of pread */
};
extern int scrub_test_mutant;

/* Cursor across calls to eng_scrub_step(). Holds no part reference or fd between calls. */
typedef struct scrub_cursor {
    int64_t partition;
    uint64_t min_block;
    uint64_t max_block;
    uint32_t level;
    uint32_t next_granule;     /* 0: start at next part strictly > key; >0: resume this part at granule */
    bool started;              /* false: begin at first part of the list */
    char priority[96];         /* non-empty: scrub this part name FIRST (restart re-scrub of a DAMAGED replica) */
    uint32_t priority_granule; /* resume granule within priority part */
} scrub_cursor;

/* Outcome of one eng_scrub_step() call. */
typedef struct scrub_report {
    uint64_t bytes;     /* bytes read and verified in this step */
    uint32_t parts_done;/* 1 when a part finished verification (clean or damaged), else 0 */
    uint32_t damaged;   /* 1 when corruption or EIO was detected on this part */
    bool newly_damaged; /* true when this step transitioned p->damaged from false to true */
    bool pass_done;     /* true when the pass reached the end of the part list */
    bool skipped;       /* true when the selected part was merged/dropped/settled away concurrently */
    int err;            /* 0, TSDB_ERR_CORRUPT, TSDB_ERR_IO, or TSDB_ERR_NOMEM */
    char part_name[96]; /* name of the part inspected ("" on pass_done) */
} scrub_report;

/* Verifies a .part file via pread (never touching p->map). Implemented in src/part.c to share part_hash and
 * trailer_parse. Checks header, footer, header+index+tombstones+footer hash, tombstones, packed granule index, and
 * every granule checksum in [g_start, p->ng), stopping after bytes_budget (0 = whole part; always at least one
 * granule when g_start < p->ng). Sets *g_next (== p->ng when whole part is verified) and *bytes_out.
 * Returns 0, TSDB_ERR_NOTFOUND (ENOENT on open), TSDB_ERR_CORRUPT, TSDB_ERR_IO, or TSDB_ERR_NOMEM. */
int part_scrub_pread(const part *p, uint32_t g_start, uint64_t bytes_budget, uint32_t *g_next, uint64_t *bytes_out);

/* Scrubs up to `budget` bytes (0 = whole next part) of ONE listed part of `db`, advancing `cursor` and filling
 * `report`. Takes a reference on one part under db->lock and releases db->lock before any I/O. Never holds
 * db->lock, publish_lock, or latest_rw across I/O.
 *
 * Per R12 D7 ("damage is never silent"), both TSDB_ERR_CORRUPT and storage TSDB_ERR_IO mark the part damaged
 * immediately via part_mark_payload() and report damage; transient resource pressure (EMFILE/ENFILE/ENOMEM) returns
 * TSDB_ERR_NOMEM without marking so the caller can retry. When cursor->priority names a part no longer listed in
 * db->parts (e.g. merged away before restart re-scrub), returns 0 with report->skipped = true and report->part_name
 * set to that vanished name, clearing cursor->priority so the caller keeps the replica DAMAGED. */
int eng_scrub_step(struct eng *db, uint64_t budget, scrub_cursor *cursor, scrub_report *report);

#endif /* TSDB_SCRUB_H */
