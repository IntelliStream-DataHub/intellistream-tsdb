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
    int fsync;               /* fsync parts, WAL and dictionaries before relying on them; default 1. With 0,
                              * a power loss (not a process crash) can leave files that fail their checks
                              * at the next open, which then needs an operator (docs/design.md) */
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
    int verify_checksums;    /* queries check each granule's checksum before decoding it (TSDB_ERR_CORRUPT
                              * on a mismatch); default 0: storage with checksums of its own (ZFS, ECC
                              * memory) catches the damage already, and without it damaged values can be
                              * returned without an error. Merges check what they decode, so damage is
                              * never rewritten under a fresh checksum; structural checksums (part header,
                              * footer and index, WAL, MANIFEST, dictionaries) are always checked. */
    int retention_merge_s;   /* a part holding rows past their series' retention is rewritten without them
                              * once it is this old; default 3600 (0 too), -1 never (merges still drop them) */
    int64_t (*clock_ms)(void *arg); /* the wall clock retention reads, ms since the epoch; NULL: CLOCK_REALTIME.
                                     * For tests: it may step back and forth like a real clock */
    void *clock_arg;
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

/* ---- deleting ----
 * tsdb_delete_range deletes every value of one series with start <= ts < end.
 * A delete is a write like any other: an insert of the same (series, ts)
 * acknowledged after the delete returned is visible, one acknowledged before
 * it started is gone. It returns once the delete is durable (fsync'd) and
 * visible; it is all or nothing across a crash. A delete costs about what a
 * large insert costs: it flushes the memtable (of all series) and writes and
 * syncs a small part per partition that holds values of the series in the
 * range, without holding up other writers meanwhile; merges absorb those
 * parts and remove the deleted rows for good later. Frequent small deletes
 * thus mean frequent small flushes. start >= end deletes nothing.
 *
 * tsdb_replace_range deletes [start, end) of the series and inserts the n
 * given rows in its place, atomically: a reader, and a crash, sees either the
 * old values or the new ones, never a mix. Every ts must lie in [start, end)
 * (TSDB_ERR_INVALID otherwise); values as in tsdb_insert_series_values. n = 0
 * is a delete. */
int tsdb_delete_range(tsdb *db, uint32_t series, int64_t start, int64_t end);
int tsdb_replace_range(tsdb *db, uint32_t series, int64_t start, int64_t end, const int64_t *ts,
                       const tsdb_values *values, size_t n);

/* ---- retention ----
 * A series' values older than now - limit_ms disappear; 0 (the default)
 * keeps them. "now" is the wall clock (tsdb_options.clock_ms), but it never
 * goes back: after the clock is stepped back, nothing expired comes back; a
 * clock stepped forward expires values early, and for good once merges have
 * removed them. Queries stop returning expired values at once; merges remove
 * them in the background, whole partitions go when every series in them has
 * expired. Raising a limit brings back only values merges have not removed yet.
 * The limit is durable when tsdb_set_retention returns. */
int tsdb_set_retention(tsdb *db, uint32_t series, int64_t limit_ms);
int tsdb_get_retention(tsdb *db, uint32_t series, int64_t *limit_ms);

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
/* Frees an array the library returned (buckets, bounds, ...). Callers in other
 * languages (embedded bindings) use it so the engine's allocator frees its own
 * memory; C callers may use free() as before. */
void tsdb_free(void *p);

/* Aggregates skip nulls: count is the number of known values, and a bucket
 * with only nulls is returned with count 0 (buckets without rows are not).
 * Floats: NaN values are ignored by min/max (NaN if there are none; unlike
 * PostgreSQL, where NaN is greater than every number) but propagate into sum.
 * Every NaN result is the one quiet NaN 0x7ff8000000000000, whatever the
 * payloads of the NaNs in the data (a raw query returns those bit for bit);
 * all NaNs are equal. int64 and bool (false 0, true 1): sum, min and max are
 * also exact in isum, imin and imax (isum is the exact sum, clamped to the
 * int64 range when it does not fit, whatever the order of the rows).
 * State: only count is meaningful.
 * Float sums are not bit-reproducible: rows are added in an order that depends
 * on how parts were merged (not on the number of query threads, nor on the
 * order or limit of the query), so the same rows written differently can give
 * sums that differ in the last bits, as in ClickHouse; where
 * the running sum overflows (rows near DBL_MAX), one order gives +inf and
 * another NaN. min, max and count are exact. Where
 * reproducible sums matter, store the values as int64 in fixed point (21.3 degC
 * as 21300 milli-degrees): isum is then exact. */
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
 * touches about N rows. Bucketed queries (aggregates, groups, bounds, the
 * spread, TWA) with a limit stop early the same way: ascending after the
 * limit-th non-empty bucket, descending walking back from the end, so "the
 * last 10 hours" of a year reads about 10 hours of granules. The result is
 * exactly that of computing every bucket and then cutting (whatever the
 * deletes, retention, memtable rows or merges); only the query statistics
 * and TSDB_ERR_TOO_MANY_BUCKETS, which counts the buckets computed, differ. */
typedef enum { TSDB_ASC = 0, TSDB_DESC = 1 } tsdb_order;

typedef struct tsdb_query_opts {
    tsdb_order order;
    size_t limit; /* 0: no limit */
} tsdb_query_opts;

/* tsdb_query_agg with order and limit (opts NULL: as tsdb_query_agg). */
int tsdb_query_agg_ex(tsdb *db, uint32_t series, int64_t start, int64_t end, int64_t interval_ms,
                      const tsdb_query_opts *opts, tsdb_bucket **out, size_t *nout);

int tsdb_query_raw_ex(tsdb *db, uint32_t series, int64_t start, int64_t end, const tsdb_query_opts *opts,
                      tsdb_raw_fn fn, void *ctx);
int tsdb_query_points_ex(tsdb *db, uint32_t series, int64_t start, int64_t end, const tsdb_query_opts *opts,
                         tsdb_points *out);

/* Raw points of a series of any type, with nulls. */
typedef int (*tsdb_values_fn)(void *ctx, const int64_t *ts, const tsdb_values *values, size_t n);
int tsdb_query_values(tsdb *db, uint32_t series, int64_t start, int64_t end, const tsdb_query_opts *opts,
                      tsdb_values_fn fn, void *ctx);

/* Downsampling for charts: Largest-Triangle-Three-Buckets (Steinarsson 2013)
 * picks `threshold` of the points in [start, end) that keep the series' visual
 * shape, and streams them in time order with their stored values, exactly
 * (bools as bytes, valid always NULL: no nulls are returned).
 *  - Points are the known, finite values: nulls, NaN and infinities are
 *    skipped (they have no place on a chart and would poison the means).
 *  - The first and the last point are always kept; points 1 .. n-2 fall into
 *    threshold - 2 buckets, bucket b starting at floor(b * every) + 1 with
 *    every = (n-2)/(threshold-2) in doubles, as in the paper's code (and
 *    ClickHouse's and the TimescaleDB toolkit's). From each bucket the point
 *    with the largest triangle formed with the point kept from the previous
 *    bucket and the mean (time, value) of the next bucket is kept (the bucket
 *    after the last is the rest: the last point, or the last two where
 *    rounding ends the last bucket early); of equal areas the first wins,
 *    and when no area compares (overflow to NaN) the bucket's first point.
 *  - Area coordinates: x is milliseconds since the first point as a double, y
 *    the value as a double (int64 rounded, bool 0 or 1).
 *  - n <= threshold returns every point. threshold < 3 or a state series:
 *    TSDB_ERR_INVALID.
 * Deleted and expired values are not points: the buckets are cut over the
 * live rows only, as if the others had never been written.
 * It reads one snapshot, so concurrent writes and merges do not tear the
 * result. Query statistics describe one read of the range (the counting pass,
 * and for very large buckets a second pass for the means, are not counted). */
int tsdb_query_lttb(tsdb *db, uint32_t series, int64_t start, int64_t end, size_t threshold, tsdb_values_fn fn,
                    void *ctx);
/* The same points newest first (TSDB_DESC), or only the first `limit` of them in that order: the choice
 * does not change, since it needs every point of the range anyway (opts NULL: as tsdb_query_lttb). Newest first
 * holds at most min(limit, threshold) chosen points in memory, and none where the threshold keeps every point
 * (a backwards read that stops at the limit). */
int tsdb_query_lttb_ex(tsdb *db, uint32_t series, int64_t start, int64_t end, size_t threshold,
                       const tsdb_query_opts *opts, tsdb_values_fn fn, void *ctx);

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

/* ---- the spread: range, variance, standard deviation ----
 * A bucket as above followed by the spread of its known values: range is
 * max - min; var_pop and stddev_pop are the population variance and standard
 * deviation (divided by count), var_samp and stddev_samp the sample ones
 * (divided by count - 1, NaN for a single value). All are NaN for a bucket
 * without known values and for state series. A NaN or an infinity among the
 * values makes the variances NaN (the deviation of an infinity from the mean
 * is undefined), not the range, which comes from min and max; so do finite
 * floats whose sum overflows (values near DBL_MAX), as it does the sum.
 *  - Floats are combined in the numerically stable form (the sum of squared
 *    deviations from the mean, granule by granule with the parallel formula of
 *    Chan et al.), so a large offset (pressure in Pa around 101325) costs no
 *    precision. Like sums, they may differ in the last bits with the order in
 *    which rows were combined (parts, merges). Constant data gives
 *    exactly 0.
 *  - int64 and bool keep exact integer sums of x and x^2: the variance is
 *    computed from exact moments, with a few roundings at the end (in long
 *    double), and does not depend on the order of the rows.
 * Like every aggregate, the spread covers only the rows that are left: a
 * deleted, replaced-away or expired (retention) value is never part of it,
 * whether merges have removed it from disk yet or not.
 * The spread costs a second pass over decoded floats and the squares of
 * integers, so only these functions compute it; tsdb_query_agg and the rest
 * do not. Arguments, order and limit as in tsdb_query_agg, tsdb_query_group_ex
 * and tsdb_query_bounds; opts may be NULL. Free with free(). */
typedef struct tsdb_spread_bucket {
    tsdb_bucket b;
    double range;
    double var_pop, var_samp;
    double stddev_pop, stddev_samp;
} tsdb_spread_bucket;

int tsdb_query_spread(tsdb *db, uint32_t series, int64_t start, int64_t end, int64_t interval_ms,
                      const tsdb_query_opts *opts, tsdb_spread_bucket **out, size_t *nout);
int tsdb_query_spread_group(tsdb *db, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                            const tsdb_query_opts *opts, tsdb_spread_bucket **out, size_t *nout);
int tsdb_query_spread_bounds(tsdb *db, uint32_t series, int64_t start, int64_t end, const int64_t *bounds,
                             size_t n, const tsdb_query_opts *opts, tsdb_spread_bucket **out, size_t *nout);

/* ---- time-weighted averages ----
 * For sensors that report irregularly or only on change, a plain average
 * weights every reading the same; a time-weighted average weights each value
 * by how long it held. Readings are joined by segments: reading i (at t_i)
 * and the next reading of the series (at t_{i+1}) bound the segment
 * [t_i, t_{i+1}).
 *  - TSDB_TWA_STEP (last observation carried forward): v_i holds over the
 *    segment. A null reading holds nothing: a null ends the previous value.
 *  - TSDB_TWA_LINEAR: the value runs on a straight line from v_i to v_{i+1}
 *    (trapezoids); a segment with a null at either end covers nothing.
 *  - max_gap_ms > 0: a segment longer than max_gap_ms covers nothing (the value
 *    is stale; LINEAR does not interpolate across it). 0: no limit.
 *  - The last reading of a series has no segment: it holds until a later
 *    reading arrives, never up to "now".
 *  - Deleted readings do not exist: the segment before a deleted range runs
 *    from the last live reading to the next live one. Expired readings
 *    (retention) do not either, and nothing is carried in from before the
 *    retention cutoff: the first segment starts at the first unexpired reading.
 * A bucket [b0, b1) integrates every segment over its intersection with the
 * bucket and with [start, end): segments are cut exactly at bucket edges and at
 * start and end (LINEAR interpolates the value at the cut), and the value in
 * force at b0 comes from the last reading before it, however long ago (within
 * max_gap_ms), even before start. Readings after end only end segments.
 *    area       = integral of value dt, in value x ms
 *    covered_ms = time covered by segments
 *    twa        = area / covered_ms (NaN when covered_ms is 0)
 *    count      = non-null readings in the bucket and in [start, end)
 * area and covered_ms add up across buckets: the average over several buckets is
 * the sum of their areas over the sum of their covered_ms. A bucket is returned
 * when covered_ms or count is not 0. Since a segment alone fills a bucket, a
 * query may span at most 4M buckets between its first and last reading in range
 * (TSDB_ERR_TOO_MANY_BUCKETS beyond).
 * NaN propagates: a NaN value held for any time makes the area and twa NaN;
 * infinities follow IEEE arithmetic, and a LINEAR segment with an infinite end
 * is that infinity throughout (+inf to -inf: NaN), wherever buckets cut it. A NaN result is the canonical quiet NaN.
 * Types: float32, float64, int64 (as double: exact up to 2^53), bool (0 or 1:
 * twa is the fraction of covered time that it was true); state series are
 * TSDB_ERR_INVALID. Whole granules inside one bucket are answered from stored
 * statistics without decoding, except where a delete overlaps them and until a
 * merge has removed the deleted rows (see docs/design.md). */
typedef enum { TSDB_TWA_STEP = 0, TSDB_TWA_LINEAR = 1 } tsdb_twa_method;

typedef struct tsdb_twa_opts {
    tsdb_twa_method method;
    int64_t max_gap_ms; /* 0: no staleness limit */
    tsdb_order order;   /* of the buckets */
    size_t limit;       /* 0: no limit */
    int no_stats;       /* nonzero: decode every granule, never its statistics (benchmarks, checks) */
} tsdb_twa_opts;

typedef struct tsdb_twa_bucket {
    int64_t start; /* bucket start; with interval 0 this is the query start */
    double twa;
    double area;
    int64_t covered_ms;
    uint64_t count;
} tsdb_twa_bucket;

/* interval_ms as in tsdb_query_agg (0: one bucket); opts NULL: STEP, no limit. Free with free(). */
int tsdb_query_twa(tsdb *db, uint32_t series, int64_t start, int64_t end, int64_t interval_ms,
                   const tsdb_twa_opts *opts, tsdb_twa_bucket **out, size_t *nout);
/* UTC calendar groups as in tsdb_query_group (group NULL: one bucket). */
int tsdb_query_twa_group(tsdb *db, uint32_t series, int64_t start, int64_t end, const tsdb_group *group,
                         const tsdb_twa_opts *opts, tsdb_twa_bucket **out, size_t *nout);
/* Client bounds as in tsdb_query_bounds: segments before bounds[0] do not count. */
int tsdb_query_twa_bounds(tsdb *db, uint32_t series, int64_t start, int64_t end, const int64_t *bounds, size_t n,
                          const tsdb_twa_opts *opts, tsdb_twa_bucket **out, size_t *nout);

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
    /* Not sent by tsdbd's STATS. A part that fails its checks (checksums, layout)
     * makes a writer's or reader's open fail with TSDB_ERR_CORRUPT and a log line
     * naming it, rather than leave its rows out (older values of the same keys
     * would come back); renaming it to <name>.broken accepts the loss. A follower
     * keeps its previous view instead, logs the part, and counts it here (only a
     * follower: a writer's open fails). */
    uint64_t broken_parts;
    uint64_t follow_errors; /* follower refreshes that failed (MANIFEST, a part or the WAL damaged, or EIO) */
} tsdb_stats;
int tsdb_get_stats(tsdb *db, tsdb_stats *st);

/* One volume's numbers, for monitoring (tsdbd --metrics-port). Cheap: counters are atomics, the
 * part list is walked once under its lock. TSDB_ERR_INVALID for a volume >= tsdb_volume_count. */
typedef struct tsdb_volume_stats {
    uint64_t parts, partitions, rows, bytes, granules;
    uint64_t memtable_rows;
    uint64_t merges, merged_rows;
    uint64_t merging_parts;  /* parts a running merge is reading now (gauge) */
    uint64_t wal_bytes;      /* written to the WAL since open, record headers included */
    uint64_t wal_syncs;      /* WAL fdatasyncs since open (0 with fsync off) */
    uint64_t broken_parts, follow_errors;
    int64_t follow_refresh_age_ms;   /* follower: time since its last successful refresh; 0 for a writer */
    int failed;              /* fail-stop state of this volume (see tsdb_failed) */
} tsdb_volume_stats;
int tsdb_get_volume_stats(tsdb *db, uint32_t volume, tsdb_volume_stats *st);

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

/* sizeof each public struct as this library was compiled, 0 for an unknown kind. For bindings that lay out the
 * structs themselves (client/java-embedded): they compare these with their own layouts when they load the library
 * and refuse a library built from other headers, instead of reading or writing past the end of a struct. */
typedef enum tsdb_struct_kind {
    TSDB_STRUCT_OPTIONS = 0,
    TSDB_STRUCT_VALUES = 1,
    TSDB_STRUCT_BUCKET = 2,
    TSDB_STRUCT_POINTS = 3,
    TSDB_STRUCT_GROUP = 4,
    TSDB_STRUCT_QUERY_OPTS = 5,
    TSDB_STRUCT_QUERY_STATS = 6,
    TSDB_STRUCT_STATS = 7,
    TSDB_STRUCT_PART_INFO = 8,
} tsdb_struct_kind;
size_t tsdb_struct_size(int kind);

#ifdef __cplusplus
}
#endif

#endif
