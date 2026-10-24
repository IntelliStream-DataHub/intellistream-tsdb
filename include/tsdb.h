/*
 * tsdb - an embedded time series database in the spirit of ClickHouse MergeTree.
 *
 * Data model: named series of (DateTime64(3) timestamp, value) points.
 * Timestamps are milliseconds since the Unix epoch (int64). A series has one
 * value type, chosen when it is created: float32 (the default), float64,
 * int64, bool, or state (a string from a small set, stored as a code). Any
 * value may be null: unknown, skipped by aggregates.
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
    int merge_idle_s;        /* a partition that got no new part for this long is merged down to one part
                              * (up to max_part_bytes) in the background; default 300 (0 too), -1 never */
    int obsolete_part_lifetime_s; /* writer: parts replaced by merges are deleted this long after,
                                   * so readers on other hosts (NFS) can finish with them; default 480 */
} tsdb_options;

void tsdb_options_init(tsdb_options *opts);
int tsdb_open(const char *dir, const tsdb_options *opts, tsdb **out);
int tsdb_close(tsdb *db);
const char *tsdb_strerror(int err);

/* ---- value types ---- */
typedef enum tsdb_type {
    TSDB_FLOAT32 = 0,
    TSDB_FLOAT64 = 1,
    TSDB_INT64 = 2,
    TSDB_BOOL = 3,
    TSDB_STATE = 4, /* a code from tsdb_state(); the names live in a per-volume dictionary */
} tsdb_type;

/* A column of values. `values` points at float, double, int64_t, uint8_t
 * (bool: 0 or 1) or uint32_t (state codes) according to `type`. `valid` is a
 * bitmap, bit i (LSB first) set when value i is known; NULL means all are
 * known. A null's value is ignored on input and unspecified on output.
 *
 * Reserved as the representation of null, so they read back as null: the
 * int64 value INT64_MIN, state code 0, and one NaN payload per float type
 * (TSDB_NULL_F32_BITS, TSDB_NULL_F64_BITS). Other NaNs are values: ignored
 * by min/max, they propagate into sum. */
typedef struct tsdb_values {
    tsdb_type type;
    const void *values;
    const uint8_t *valid;
} tsdb_values;

#define TSDB_NULL_F32_BITS 0x7fc01001u
#define TSDB_NULL_F64_BITS 0x7ff8000000001001ull

/* ---- series ----
 * A series is stored in one volume: the database directory itself, or the
 * directory given to tsdb_series_at when the series is created (for example
 * one per NFS mount, to spread writes over them). Series created with the
 * same path share that volume. The path only matters at creation. */
int tsdb_series(tsdb *db, const char *name, uint32_t *id);        /* get or create in the database directory */
int tsdb_series_at(tsdb *db, const char *name, const char *path, uint32_t *id); /* get or create in volume `path` */
int tsdb_series_lookup(tsdb *db, const char *name, uint32_t *id); /* TSDB_ERR_NOTFOUND if missing */
/* Get or create a series of `type` in volume `path` (NULL: the database
 * directory). An existing series keeps its type: asking for another one is
 * TSDB_ERR_INVALID. tsdb_series and tsdb_series_at create float32 series. */
int tsdb_series_create(tsdb *db, const char *name, tsdb_type type, const char *path, uint32_t *id);
int tsdb_series_type(tsdb *db, uint32_t id, tsdb_type *type);
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

/* The same for any value type, and with nulls. Every series of one call
 * must have the column's type (TSDB_ERR_INVALID otherwise); the float32
 * calls above are these with a float32 column and no nulls. */
int tsdb_insert_values(tsdb *db, const uint32_t *series, const int64_t *ts, const tsdb_values *values, size_t n);
int tsdb_insert_series_values(tsdb *db, uint32_t series, const int64_t *ts, const tsdb_values *values, size_t n);

/* State dictionary: names <-> codes (1, 2, ...), shared by the state series
 * of one volume. tsdb_state creates a name's code if needed; codes never
 * change. tsdb_state_name returns NULL for an unknown code. */
int tsdb_state(tsdb *db, uint32_t series, const char *name, uint32_t *code);
int tsdb_state_lookup(tsdb *db, uint32_t series, const char *name, uint32_t *code); /* TSDB_ERR_NOTFOUND */
const char *tsdb_state_name(tsdb *db, uint32_t series, uint32_t code);              /* valid until tsdb_close */

/* A single point: WAL + memtable, visible at once, durable per wal_sync_ms. */
int tsdb_append(tsdb *db, uint32_t series, int64_t ts, float value);
/* Makes everything written so far durable without creating parts. */
int tsdb_commit(tsdb *db);
/* Commits, then turns the memtable into parts. */
int tsdb_flush(tsdb *db);

/* ---- reading ---- */

/* Streams points of one float32 series in [start, end) in time order (nulls
 * read as NaN; other types: TSDB_ERR_INVALID, use tsdb_query_values). The
 * arrays are only valid during the call. Return non-zero from fn to stop early. */
typedef int (*tsdb_raw_fn)(void *ctx, const int64_t *ts, const float *values, size_t n);
int tsdb_query_raw(tsdb *db, uint32_t series, int64_t start, int64_t end, tsdb_raw_fn fn, void *ctx);

typedef struct tsdb_points {
    int64_t *ts;
    float *values;
    size_t n, cap;
} tsdb_points;
int tsdb_query_points(tsdb *db, uint32_t series, int64_t start, int64_t end, tsdb_points *out);
void tsdb_points_free(tsdb_points *p);

/* Aggregates skip nulls: count is the number of known values, and a bucket
 * with only nulls is returned with count 0 (buckets without rows are not).
 * Floats: NaN values are ignored by min/max (NaN if there are none) but
 * propagate into sum. int64 and bool (false 0, true 1): sum, min and max are
 * also exact in isum, imin and imax (isum is the exact sum, clamped to the
 * int64 range when it does not fit, whatever the order of the rows).
 * State: only count is meaningful. */
typedef struct tsdb_bucket {
    int64_t start; /* bucket start; with interval 0 this is the query start */
    uint64_t count;
    double sum;
    double min, max;
    int64_t isum, imin, imax;
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

/* Raw points of a series of any type, with nulls. */
typedef int (*tsdb_values_fn)(void *ctx, const int64_t *ts, const tsdb_values *values, size_t n);
int tsdb_query_values(tsdb *db, uint32_t series, int64_t start, int64_t end, const tsdb_query_opts *opts,
                      tsdb_values_fn fn, void *ctx);

/* Calendar grouping, in UTC: `count` units per bucket.
 *  - TSDB_SECOND, _MINUTE, _HOUR: fixed lengths, aligned to multiples of their
 *    length since the epoch.
 *  - TSDB_DAY, _WEEK (ISO, from Monday), _MONTH, _QUARTER, _YEAR: UTC calendar
 *    periods, multiples counted from 1970, like toStartOfInterval.
 * The database knows UTC only: timezone must be NULL or "UTC" (anything else is
 * TSDB_ERR_INVALID). Local days, weeks and months are bounds a client computes
 * (tsdb_calendar.h: tsdb_calendar_bounds, tsdb_query_group_local) and passes to
 * tsdb_query_bounds. */
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
    const char *timezone; /* NULL or "UTC"; clients turn other zones into bounds */
} tsdb_group;

int tsdb_query_group(tsdb *db, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                     tsdb_bucket **out, size_t *nout);
/* Buckets that begin at given UTC instants: bucket i covers [bounds[i],
 * bounds[i + 1]), the last one everything from bounds[n - 1] on; only rows in
 * [start, end) count. bounds must strictly increase. Reported starts are the
 * bounds; order and limit as in tsdb_query_group_ex. */
int tsdb_query_bounds(tsdb *db, uint32_t series, int64_t start, int64_t end, const int64_t *bounds, size_t n,
                      const tsdb_query_opts *opts, tsdb_bucket **out, size_t *nout);
/* group NULL: one bucket for the whole range. */
int tsdb_query_group_ex(tsdb *db, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                        const tsdb_query_opts *opts, tsdb_bucket **out, size_t *nout);

/* ---- query statistics ----
 * What the calling thread's last query did, like ClickHouse's "Processed N
 * rows, M MB": rows it covered (decoded, answered from granule statistics, or
 * in memory), what it decompressed, and how long it took. */
typedef struct tsdb_query_stats {
    double elapsed_s;
    uint64_t rows;                /* rows covered: rows_decoded + rows_from_stats + memtable_rows */
    uint64_t rows_decoded;        /* rows decompressed (whole granules) */
    uint64_t rows_from_stats;     /* rows answered from granule statistics, without decoding */
    uint64_t memtable_rows;       /* rows read from memtables (not yet in parts) */
    uint64_t bytes;               /* uncompressed size of the rows covered (timestamp + value) */
    uint64_t bytes_read;          /* compressed bytes of the granules decoded */
    uint64_t parts;               /* parts with rows of the series in the range */
    uint64_t granules;            /* their granules in the range */
    uint64_t granules_from_stats; /* granules answered from their index entry */
    uint64_t result_rows;         /* rows, or buckets, returned */
} tsdb_query_stats;
void tsdb_last_query_stats(tsdb_query_stats *out);

/* ---- maintenance ---- */
int tsdb_optimize(tsdb *db);                /* merge every partition down to one part */
int tsdb_drop_before(tsdb *db, int64_t ts); /* drop whole partitions that end at or before ts */

typedef struct tsdb_stats {
    uint64_t parts, partitions, rows, bytes;
    uint64_t merges, merged_rows, passthrough_granules;
    uint64_t memtable_rows; /* rows in the WAL-backed memtable, not yet in parts */
} tsdb_stats;
int tsdb_get_stats(tsdb *db, tsdb_stats *st);

/* Fail-stop. A storage error after which it is unknown what reached the disk
 * (a failed WAL write or fsync, a failed fsync of the directory after new
 * parts were named, a failed MANIFEST, series or state dictionary or CATALOG
 * write; ENOSPC there counts too)
 * puts the database, or the volume it happened in, in a failed state for good:
 * after a failed fsync Linux may drop the unwritten pages and report success
 * the next time, so retrying could acknowledge data that is not on disk.
 * From then on every insert, append, series creation, commit, flush, optimize
 * and drop of that volume returns TSDB_ERR_IO, including the writes that were
 * waiting for the failed sync; creating a series or state that already exists
 * still returns its id. Queries keep working. Close and reopen to
 * recover: every write that returned 0 is there.
 * Returns 0 while healthy; TSDB_ERR_IO once failed, with what failed (the
 * directory, the operation, the errno text) in msg. */
int tsdb_failed(tsdb *db, char *msg, size_t len);

typedef struct tsdb_part_info {
    const char *name;
    int64_t partition, min_ts, max_ts;
    uint64_t rows, bytes, granules;
    uint32_t level;
    int64_t created_s; /* when the file was written (Unix seconds) */
    int64_t written_s; /* when its newest rows were inserted; idle merges go by this */
} tsdb_part_info;
int tsdb_foreach_part(tsdb *db, void (*fn)(void *ctx, const tsdb_part_info *info), void *ctx);

/* Read-only databases: pick up what the writer published since the last
 * refresh (a follower does this every refresh_ms on its own). */
int tsdb_refresh(tsdb *db);

/* Name of the SIMD kernel set picked at runtime: "avx512", "avx2" or "scalar"; in a
 * portable build (`make dist`) followed by the CPU level of the codecs: "avx2, codecs x86-64-v3". */
const char *tsdb_simd_level(void);

#ifdef __cplusplus
}
#endif

#endif
