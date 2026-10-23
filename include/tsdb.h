/*
 * tsdb - an embedded time series database in the spirit of ClickHouse MergeTree.
 *
 * Data model: named series of (DateTime64(3) timestamp, Float32 value) points.
 * Timestamps are milliseconds since the Unix epoch (int64).
 *
 * Storage: every insert writes an immutable, sorted "part" file. Parts are
 * grouped into time partitions and merged in the background, exactly like
 * MergeTree. Inside a part, rows are sorted by (series, ts) and cut into
 * granules of up to `granule_rows` rows. Timestamps are encoded with
 * DoubleDelta, values with Gorilla, and each block is then compressed with
 * ZSTD when that helps. Every granule carries min/max/sum/count, so aggregates
 * over fully covered granules never touch the compressed data.
 *
 * All functions are thread safe. Time ranges are half open: [start, end).
 */
#ifndef TSDB_H
#define TSDB_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tsdb tsdb;

enum {
    TSDB_OK = 0,
    TSDB_ERR_IO = -1,
    TSDB_ERR_NOMEM = -2,
    TSDB_ERR_CORRUPT = -3,
    TSDB_ERR_INVALID = -4,
    TSDB_ERR_LOCKED = -5,
    TSDB_ERR_NOTFOUND = -6,
    TSDB_ERR_TOO_MANY_BUCKETS = -7,
    TSDB_ERR_STOPPED = -8, /* a query callback returned non-zero, or the db is closing */
    TSDB_ERR_READONLY = -9,
};

#define TSDB_TS_MIN INT64_MIN
#define TSDB_TS_MAX INT64_MAX

/* Value encodings. AUTO stores a granule as scaled decimal integers when that
 * is exactly lossless (typical for sensor readings); otherwise it keeps the
 * smallest of Chimp, bit-delta and byte-split after ZSTD. GORILLA and CHIMP
 * force one XOR codec for every granule. All are lossless. */
enum { TSDB_VALUES_AUTO = 0, TSDB_VALUES_GORILLA = 1, TSDB_VALUES_CHIMP = 2 };

typedef struct tsdb_options {
    int64_t partition_ms;    /* fixed at creation; default 1 day */
    uint32_t granule_rows;   /* fixed at creation; default 8192 */
    int zstd_level;          /* default 1; 0 disables zstd */
    int value_codec;         /* TSDB_VALUES_AUTO (default), _GORILLA or _CHIMP */
    int fsync;               /* fsync parts before publishing them; default 1 */
    int merge_threads;       /* background merge threads; default 2, 0 disables merges */
    int query_threads;       /* threads for one query; default: every logical CPU */
    size_t buffer_rows;      /* memtable rows before it is flushed into parts; default 1M */
    int flush_interval_ms;   /* memtable age before it is flushed into parts; default 5000 */
    int wal_sync_ms;         /* 0: every small write waits for fsync (group commit, default);
                              * N > 0: the WAL is fsync'd every N ms in the background;
                              * -1: no WAL, small writes live only in memory until flushed */
    size_t wal_max_batch_rows; /* tsdb_insert below this goes WAL + memtable, else straight to a part; default 65536 */
    uint64_t max_part_bytes; /* background merges never produce parts larger than this; default 4 GiB */
    int read_only;           /* open without writing, merging or locking exclusively */
    int follow;              /* read_only, and keep following a writer: new parts, merges and WAL rows */
    int refresh_ms;          /* how often a follower polls the writer's MANIFEST; default 100 */
    int obsolete_part_lifetime_s; /* writer: parts replaced by merges are deleted this long after,
                                   * so readers on other hosts (NFS) can finish with them; default 480 */
} tsdb_options;

void tsdb_options_init(tsdb_options *opts);
int tsdb_open(const char *dir, const tsdb_options *opts, tsdb **out);
int tsdb_close(tsdb *db);
const char *tsdb_strerror(int err);

/* ---- series ----
 * A series is stored in one volume: the database directory itself, or the
 * directory given to tsdb_series_at when the series is created (for example
 * one per NFS mount, to spread writes over them). Series created with the
 * same path share that volume. The path only matters at creation. */
int tsdb_series(tsdb *db, const char *name, uint32_t *id);        /* get or create in the database directory */
int tsdb_series_at(tsdb *db, const char *name, const char *path, uint32_t *id); /* get or create in volume `path` */
int tsdb_series_lookup(tsdb *db, const char *name, uint32_t *id); /* TSDB_ERR_NOTFOUND if missing */
uint32_t tsdb_series_count(tsdb *db);
const char *tsdb_series_name(tsdb *db, uint32_t id); /* valid until tsdb_close */
const char *tsdb_series_path(tsdb *db, uint32_t id); /* its volume's directory */
uint32_t tsdb_volume_count(tsdb *db);                /* 1 + the extra volumes */
const char *tsdb_volume_path(tsdb *db, uint32_t index);

/* ---- writing ----
 * (series, ts) is a unique key, as in ReplacingMergeTree read with FINAL:
 * writing an existing (series, ts) replaces its value, the last write wins
 * (within one batch, the later row), and queries never return duplicates.
 *
 * tsdb_insert returns once the rows are durable (per fsync / wal_sync_ms)
 * and visible. Large batches are written as one part per partition; small
 * ones go through the WAL into the memtable. Rows may be in any order;
 * sorted input (by series, then time) skips the sort.
 */
int tsdb_insert(tsdb *db, const uint32_t *series, const int64_t *ts, const float *values, size_t n);
int tsdb_insert_series(tsdb *db, uint32_t series, const int64_t *ts, const float *values, size_t n);

/* A single point: WAL + memtable, visible at once, durable per wal_sync_ms. */
int tsdb_append(tsdb *db, uint32_t series, int64_t ts, float value);
/* Makes everything written so far durable without creating parts. */
int tsdb_commit(tsdb *db);
/* Commits, then turns the memtable into parts. */
int tsdb_flush(tsdb *db);

/* ---- reading ---- */

/* Streams points of one series in [start, end) in time order. The arrays are
 * only valid during the call. Return non-zero from fn to stop early. */
typedef int (*tsdb_raw_fn)(void *ctx, const int64_t *ts, const float *values, size_t n);
int tsdb_query_raw(tsdb *db, uint32_t series, int64_t start, int64_t end, tsdb_raw_fn fn, void *ctx);

typedef struct tsdb_points {
    int64_t *ts;
    float *values;
    size_t n, cap;
} tsdb_points;
int tsdb_query_points(tsdb *db, uint32_t series, int64_t start, int64_t end, tsdb_points *out);
void tsdb_points_free(tsdb_points *p);

typedef struct tsdb_bucket {
    int64_t start; /* bucket start; with interval 0 this is the query start */
    uint64_t count;
    double sum;
    float min, max; /* NaN values are ignored by min/max (NaN if all are NaN), but propagate into sum */
} tsdb_bucket;

static inline double tsdb_bucket_avg(const tsdb_bucket *b)
{
    return b->count ? b->sum / (double)b->count : 0.0;
}

/* Aggregates one series over [start, end). interval_ms == 0 gives a single
 * bucket; otherwise buckets are aligned to the epoch like toStartOfInterval.
 * Only non-empty buckets are returned, in time order. Free with free(). */
int tsdb_query_agg(tsdb *db, uint32_t series, int64_t start, int64_t end, int64_t interval_ms,
                   tsdb_bucket **out, size_t *nout);

/* Order (by timestamp) and limit, for raw rows and for buckets. Descending
 * raw queries read backwards window by window, so "the newest N points"
 * touches about N rows. */
typedef enum { TSDB_ASC = 0, TSDB_DESC = 1 } tsdb_order;

typedef struct tsdb_query_opts {
    tsdb_order order;
    size_t limit; /* 0: no limit */
} tsdb_query_opts;

int tsdb_query_raw_ex(tsdb *db, uint32_t series, int64_t start, int64_t end, const tsdb_query_opts *opts,
                      tsdb_raw_fn fn, void *ctx);
int tsdb_query_points_ex(tsdb *db, uint32_t series, int64_t start, int64_t end, const tsdb_query_opts *opts,
                         tsdb_points *out);

/* Calendar grouping: `count` units per bucket, in a time zone.
 *  - TSDB_SECOND, _MINUTE, _HOUR: fixed-length buckets, aligned to multiples
 *    of their length since the epoch in the zone's standard time (for zones
 *    with whole-hour offsets, hours and minutes start where local ones do).
 *  - TSDB_DAY, _WEEK (ISO, from Monday), _MONTH, _QUARTER, _YEAR: local
 *    calendar periods; a day across a DST change lasts 23 or 25 hours.
 * Multiples are counted from 1970 in local time, like toStartOfInterval.
 * Each bucket's start is the UTC instant its local period begins.
 * timezone: NULL or "UTC", a fixed offset ("+02:00", "-0530"), or an IANA
 * name such as "Europe/Oslo" (from /usr/share/zoneinfo, or $TZDIR). */
typedef enum {
    TSDB_SECOND = 1,
    TSDB_MINUTE,
    TSDB_HOUR,
    TSDB_DAY,
    TSDB_WEEK,
    TSDB_MONTH,
    TSDB_QUARTER,
    TSDB_YEAR,
} tsdb_unit;

typedef struct tsdb_group {
    tsdb_unit unit;
    uint32_t count;       /* 0 means 1 */
    const char *timezone; /* NULL means UTC */
} tsdb_group;

int tsdb_query_group(tsdb *db, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                     tsdb_bucket **out, size_t *nout);
/* group NULL: one bucket for the whole range. */
int tsdb_query_group_ex(tsdb *db, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                        const tsdb_query_opts *opts, tsdb_bucket **out, size_t *nout);

/* ---- maintenance ---- */
int tsdb_optimize(tsdb *db);                /* merge every partition down to one part */
int tsdb_drop_before(tsdb *db, int64_t ts); /* drop whole partitions that end at or before ts */

typedef struct tsdb_stats {
    uint64_t parts, partitions, rows, bytes;
    uint64_t merges, merged_rows, passthrough_granules;
    uint64_t memtable_rows; /* rows in the WAL-backed memtable, not yet in parts */
} tsdb_stats;
int tsdb_get_stats(tsdb *db, tsdb_stats *st);

typedef struct tsdb_part_info {
    const char *name;
    int64_t partition, min_ts, max_ts;
    uint64_t rows, bytes, granules;
    uint32_t level;
} tsdb_part_info;
int tsdb_foreach_part(tsdb *db, void (*fn)(void *ctx, const tsdb_part_info *info), void *ctx);

/* Read-only databases: pick up what the writer published since the last
 * refresh (a follower does this every refresh_ms on its own). */
int tsdb_refresh(tsdb *db);

/* Name of the SIMD kernel set picked at runtime: "avx512", "avx2" or "scalar". */
const char *tsdb_simd_level(void);

#ifdef __cplusplus
}
#endif

#endif
