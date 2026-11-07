/*
 * The change feed's files and retained segments (docs/design-cdc.md 2.2, 2.6, 2.7, 2.13). Step F1 builds the disk side:
 * the FEED and FEEDSLOTS files, `requires=feed` in META, retained WAL segments (fwal_<seq>.log), enable and disable,
 * and the cleaner thread that removes old segments. Readers, markers of commits and slots come in later steps.
 *
 * The files of one volume directory, in the order they are made (enable) and the reverse (disable):
 *   META requires=feed   the word: no binary without the feed opens the volume (it would rename nothing and
 *                        remove links as orphans)
 *   FEEDSLOTS            8 KiB, two 4 KiB halves (anchor, scratch): the cleaner's floor (and later the slots)
 *   FEED                 64 bytes: feed_id, retention, size bound, free-space rule; its existence is "the feed is on"
 *   fwal_<seq>.log       WAL segments the engine no longer needs, kept for the feed
 * FEED implies FEEDSLOTS implies the word, so a crash in the middle of an enable or a disable leaves a state the next
 * open resolves (feed_open_reconcile).
 */
#ifndef TSDB_FEED_H
#define TSDB_FEED_H

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

struct eng;

#define FEED_FILE "FEED"
#define FEEDSLOTS_FILE "FEEDSLOTS"
#define FEED_DEF_RETENTION_S 86400u            /* 24 h */
#define FEED_DEF_MAX_BYTES (64ull << 30)       /* 64 GiB per volume */
#define FEED_DEF_MIN_FREE_PCT 10u

/* The FEED file: 64 bytes, checksummed, replaced by tmp + rename. */
typedef struct feed_cfg {
    uint64_t feed_id;  /* identity of the history; a new one makes every consumer resync (FEED_RESET) */
    uint64_t max_bytes;
    int64_t created_ms;
    uint32_t retention_s;
    uint32_t min_free_pct;
    uint32_t seeded; /* F4: slots seeded in a volume added later */
} feed_cfg;

/* One half of FEEDSLOTS. The floor is the (event, check) of the last record of the newest segment the cleaner has
 * removed: every event up to it is gone. check is the low 32 bits of that record's xxh64. */
typedef struct fslots {
    uint64_t gen;
    uint64_t floor_event;
    uint64_t ret_fed;  /* the end of the RETENTION records the feed covers (F2) */
    uint64_t ckpt_fno; /* the checkpoint covers markers up to here (F4) */
    uint32_t floor_check;
    uint32_t body_len; /* the slot table (F4); 0 now */
} fslots;

/* The options of an enable. Zero means the default. */
typedef struct feed_opts {
    uint32_t retention_s;
    uint64_t max_bytes;
    uint32_t min_free_pct;
} feed_opts;

/* A directory the feed's file operations work on: a closed one (the CLI under the directory's lock: sync decides
 * whether anything is fsynced) or an open engine's (db: directory fsyncs go through eng_sync_dir, fail-stop). */
typedef struct feed_dir {
    int dirfd;
    struct eng *db;
    bool sync;
} feed_dir;

bool fwal_file_name(const char *name); /* fwal_<16 lowercase hex>.log */
void fwal_name(char *buf, size_t len, uint64_t seq);
/* The retained segments in a directory, counted from its files (for `feed info`, also of a read-only handle). */
int feed_scan_retained(int dirfd, uint64_t *segments, uint64_t *bytes);

/* META's requires= line: the words it lists. *feed: "feed" is among them; unknown (cap bytes, NUL-terminated, may be
 * NULL): the words this release does not know, comma separated. */
void meta_requires_parse(const char *meta_text, bool *feed, char *unknown, size_t cap);

/* The files. Each returns 0 or a TSDB_ERR_*; NOTFOUND: no file; CORRUPT: a file that does not check. */
int feed_cfg_read(int dirfd, feed_cfg *c);
int feed_cfg_write(const feed_dir *d, const feed_cfg *c); /* tmp, fsync, rename; the directory fsync is the caller's */
int fslots_read(int dirfd, fslots *s);                   /* the valid half with the higher gen (info, tests) */
void feed_cfg_defaults(feed_cfg *c, const feed_opts *o); /* a new feed_id, created_ms now */

/* Enable and disable of one directory, each step with its directory fsync (the order of design-cdc.md 2.13 and the
 * advisor's: word, FEEDSLOTS, FEED; disable: FEED, the rest, the word). Idempotent: a run that died is run again.
 * floor_event: the number below which nothing is retained (the volume's next number minus 1). */
int feed_dir_enable(const feed_dir *d, const feed_cfg *cfg, uint64_t floor_event);
int feed_dir_disable(const feed_dir *d);
/* What a volume's own files say: the word, FEEDSLOTS, FEED, fwal_ names (any of them), for reconcile and info. */
typedef struct feed_state {
    bool word, feedslots, feed, any_fwal, junk;
} feed_state;
int feed_dir_state(int dirfd, feed_state *s);
/* A new FEED with a new feed_id (renewal, `feed new-id`): the old file is replaced; 0 or an error. */
int feed_dir_new_id(const feed_dir *d, feed_cfg *cfg);

/* FEEDSLOTS in an open engine: the floor, written to the scratch half, fdatasynced, then the anchor. A failure is
 * fail-stop. Returns 0 or TSDB_ERR_IO. fs_* in the engine hold the state (feed_lock). */
int fslots_open(struct eng *db);
void fslots_close(struct eng *db);
int fslots_set_floor(struct eng *db, uint64_t floor_event, uint32_t floor_check);

/* A retained segment in the cleaner's index (feedclean.c). last/last_ms/last_hash come with the segment when the
 * flush renames it, and are read lazily (header to header, the last record only) for segments found at open. */
typedef struct fseg {
    uint64_t seq;
    uint64_t size;
    uint64_t first;     /* the first number (event) of the segment; 0 while unknown */
    uint64_t last;      /* the last number of its last record: fno + span - 1 */
    int64_t last_ms;    /* that record's commit_ms */
    uint64_t last_hash; /* and its xxh64 */
    bool known;         /* first, last, last_ms and last_hash are filled */
} fseg;

/* The cleaner (feedclean.c): one thread per engine, started when the feed is on and the engine is a writer. */
int feed_cleaner_start(struct eng *db);
void feed_cleaner_stop(struct eng *db); /* joins; safe when none runs */
void feed_cleaner_wake(struct eng *db);
/* A segment became fwal_<seq> (a flush, the open, the close): tell the index. info may be NULL (read later). */
void feed_index_add(struct eng *db, uint64_t seq, const fseg *info);
void feed_index_free(struct eng *db);
/* Test hook: free space of the volume's file system as a percent (process-wide; NULL: statvfs). */
void feed_test_unlink_hook(void (*fn)(void)); /* tests: after the cleaner's first unlink of a pass */
extern int feed_test_cleaner_fail; /* tests: the next n cleaner starts fail */
extern uint64_t feed_test_fills; /* tests: segment files walked by seg_fill */
void feed_test_free_hook(int (*fn)(void *ctx, const char *dir, uint32_t *pct), void *ctx);

/* The open: bring the volume to the state the database wants (want: 1 on, 0 off, -1 as its own files say, the root's
 * rule), and the engine's feed fields. Called by the writer's open, after META is read and the LOCK is held. */
int feed_open_reconcile(struct eng *db, int want, const feed_opts *wanted);
/* After the open's numbers are known (wal_open): enable or renew FEED before the first FNO store. */
int feed_open_numbers(struct eng *db);
/* Live enable and disable, embedded (tsdb.h through volumes.c). */
int eng_feed_enable(struct eng *db, const feed_opts *o);
int eng_feed_disable(struct eng *db);
int eng_feed_new_id(struct eng *db); /* a new feed_id first, then the numbers 2^40 higher (F19) */
/* The thread-local wish for the next eng_open_sched of a volume: 1 on, 0 off, -1 as its own files say. */
void eng_set_open_feed_want(int want, const feed_opts *o);
int eng_take_open_feed_want(feed_opts *o); /* also clears it; the first statement of eng_open_sched */

#endif
