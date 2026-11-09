// SPDX-License-Identifier: Apache-2.0
//! Query options and results.

use crate::value::{Column, Value, ValueType};

/// The order of raw rows or buckets, by timestamp.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum Order {
    /// Oldest first.
    #[default]
    Asc,
    /// Newest first. A descending raw query reads backwards, so "the newest N
    /// points" touches about N rows on the server.
    Desc,
}

/// Order and limit for the queries: [`query_points`](crate::Client::query_points),
/// [`query_rows`](crate::Client::query_rows), [`query_group`](crate::Client::query_group),
/// [`query_bounds`](crate::Client::query_bounds) and their streaming forms.
///
/// ```
/// use tsdb_client::QueryOpts;
/// let newest_five = QueryOpts::desc().limit(5);
/// assert_eq!(newest_five.limit, Some(5));
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct QueryOpts {
    /// Ascending (the default) or descending by timestamp.
    pub order: Order,
    /// At most this many rows or buckets; `None` returns all of them.
    /// `Some(0)` returns nothing, without asking the server.
    pub limit: Option<u64>,
}

impl QueryOpts {
    /// Ascending, no limit.
    pub fn asc() -> Self {
        QueryOpts::default()
    }

    /// Descending, no limit.
    pub fn desc() -> Self {
        QueryOpts { order: Order::Desc, limit: None }
    }

    /// Sets the limit.
    pub fn limit(mut self, n: u64) -> Self {
        self.limit = Some(n);
        self
    }

    /// The limit as the protocol carries it (0: none), or `None` for `Some(0)`.
    pub(crate) fn wire_limit(&self) -> Option<u64> {
        match self.limit {
            None => Some(0),
            Some(0) => None,
            Some(n) => Some(n),
        }
    }
}

/// A calendar unit for [`Group`] (`tsdb_unit`).
///
/// Seconds, minutes and hours are fixed-length buckets; days, ISO weeks
/// (from Monday), months, quarters and years follow the local calendar, so a
/// day across a DST change lasts 23 or 25 hours.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Unit {
    /// Seconds.
    Second = 1,
    /// Minutes.
    Minute = 2,
    /// Hours.
    Hour = 3,
    /// Local days.
    Day = 4,
    /// ISO weeks, starting on Monday.
    Week = 5,
    /// Local months.
    Month = 6,
    /// Local quarters.
    Quarter = 7,
    /// Local years.
    Year = 8,
}

/// Calendar grouping for [`query_group`](crate::Client::query_group):
/// `count` units per bucket, in a time zone.
///
/// ```
/// use tsdb_client::{Group, Unit};
/// let days = Group::new(Unit::Day).timezone("Europe/Oslo");
/// let quarter_hours = Group::new(Unit::Minute).count(15);
/// # let _ = (days, quarter_hours);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Group {
    /// The unit.
    pub unit: Unit,
    /// Units per bucket; 0 is treated as 1. Multiples are counted from 1970 in local time.
    pub count: u32,
    /// `None` or `"UTC"`, a fixed offset (`"+02:00"`, `"-0530"`) or an IANA
    /// name such as `"Europe/Oslo"`. The client reads the zone from the
    /// system's zoneinfo and computes the bounds of the local periods (see
    /// [`calendar_bounds`](crate::calendar_bounds)).
    pub timezone: Option<String>,
}

impl Group {
    /// One `unit` per bucket, in UTC.
    pub fn new(unit: Unit) -> Self {
        Group { unit, count: 1, timezone: None }
    }

    /// Sets the number of units per bucket.
    pub fn count(mut self, count: u32) -> Self {
        self.count = count;
        self
    }

    /// Sets the time zone; see [`Group::timezone`](Group#structfield.timezone).
    pub fn timezone(mut self, tz: impl Into<String>) -> Self {
        self.timezone = Some(tz.into());
        self
    }
}

/// One aggregate bucket (`tsdb_bucket`).
///
/// Aggregates skip nulls: `count` is the number of known values, and a bucket
/// whose rows are all null is returned with count 0 (buckets without rows are
/// not returned). For int64 and bool series (false 0, true 1) `isum`, `imin`
/// and `imax` are exact, where the f64 fields may round; `isum` saturates at
/// `i64::MIN`/`i64::MAX`. For state series only `count` is meaningful.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bucket {
    /// The bucket's start in ms since the epoch (the UTC instant its local
    /// period begins); for a single bucket, the query's start.
    pub start: i64,
    /// Number of known values.
    pub count: u64,
    /// Sum of the values (NaN values propagate into it). Every NaN `sum`,
    /// `min` or `max` has the bits `0x7ff8000000000000`, whatever NaNs were
    /// stored, so results compare bit for bit.
    pub sum: f64,
    /// Smallest value, ignoring NaN (NaN if there is no other value).
    pub min: f64,
    /// Largest value, ignoring NaN (NaN if there is no other value).
    pub max: f64,
    /// Exact sum, for int64 and bool series.
    pub isum: i64,
    /// Exact minimum, for int64 and bool series.
    pub imin: i64,
    /// Exact maximum, for int64 and bool series.
    pub imax: i64,
}

impl Bucket {
    /// The mean, `sum / count` (0 for an empty bucket).
    pub fn avg(&self) -> f64 {
        if self.count == 0 { 0.0 } else { self.sum / self.count as f64 }
    }
}

/// A [`Bucket`] with the spread of its known values (protocol version 8):
/// `range` is max - min; the population variance and standard deviation
/// divide by `count`, the sample ones by `count - 1` (NaN for one value). All
/// are NaN for a bucket without known values and for state series; a NaN or an
/// infinity among the values makes the variances NaN, not the range.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SpreadBucket {
    /// Count, sum, min and max, as from the plain queries.
    pub bucket: Bucket,
    /// `max - min`.
    pub range: f64,
    /// Population variance.
    pub var_pop: f64,
    /// Sample variance.
    pub var_samp: f64,
    /// Population standard deviation.
    pub stddev_pop: f64,
    /// Sample standard deviation.
    pub stddev_samp: f64,
}

/// How a time-weighted average joins two readings (`tsdb_twa_method`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum TwaMethod {
    /// Each value holds until the next reading (last observation carried
    /// forward); a null reading ends the previous value.
    #[default]
    Step,
    /// The value runs on a straight line from one reading to the next; a
    /// segment with a null at either end covers nothing.
    Linear,
}

/// Options of the time-weighted average queries.
///
/// ```
/// use tsdb_client::{TwaMethod, TwaOpts};
/// let o = TwaOpts::linear().max_gap(60_000).desc().limit(10);
/// assert_eq!(o.method, TwaMethod::Linear);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct TwaOpts {
    /// Step (the default) or linear.
    pub method: TwaMethod,
    /// A segment longer than this covers nothing (the value is stale); 0: no limit.
    pub max_gap_ms: i64,
    /// Order of the buckets.
    pub order: Order,
    /// At most this many buckets; `None` returns all of them, `Some(0)` none.
    pub limit: Option<u64>,
}

impl TwaOpts {
    /// Step, no gap limit, ascending, no limit.
    pub fn step() -> Self {
        TwaOpts::default()
    }

    /// Linear interpolation, otherwise as [`step`](TwaOpts::step).
    pub fn linear() -> Self {
        TwaOpts { method: TwaMethod::Linear, ..TwaOpts::default() }
    }

    /// Sets the longest segment that still counts.
    pub fn max_gap(mut self, ms: i64) -> Self {
        self.max_gap_ms = ms;
        self
    }

    /// Newest bucket first.
    pub fn desc(mut self) -> Self {
        self.order = Order::Desc;
        self
    }

    /// Sets the limit.
    pub fn limit(mut self, n: u64) -> Self {
        self.limit = Some(n);
        self
    }

    /// The order and limit as [`QueryOpts`].
    #[doc(hidden)]
    pub fn query_opts(&self) -> QueryOpts {
        QueryOpts { order: self.order, limit: self.limit }
    }
}

/// One bucket of a time-weighted average.
///
/// `area` and `covered_ms` add up across buckets: the average over several
/// buckets is the sum of their areas over the sum of their `covered_ms`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TwaBucket {
    /// The bucket's start in ms since the epoch; for a single bucket, the query's start.
    pub start: i64,
    /// `area / covered_ms`, NaN when nothing is covered.
    pub twa: f64,
    /// The integral of the value over time, in value x ms.
    pub area: f64,
    /// The time covered by segments, in ms.
    pub covered_ms: i64,
    /// Known readings in the bucket and in the query's range.
    pub count: u64,
}

/// Raw points of one series, as two columns of equal length.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Points {
    /// Timestamps in ms since the epoch, in the query's order.
    pub ts: Vec<i64>,
    /// The value of each timestamp.
    pub values: Vec<Value>,
}

impl Points {
    /// Number of points.
    pub fn len(&self) -> usize {
        self.ts.len()
    }

    /// Whether there are no points.
    pub fn is_empty(&self) -> bool {
        self.ts.is_empty()
    }

    /// Iterates over `(timestamp, value)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (i64, Value)> + '_ {
        self.ts.iter().copied().zip(self.values.iter().copied())
    }
}

/// Raw points of a series of any type, with nulls: timestamps and a [`Column`] of equal length.
#[derive(Debug, Clone)]
pub struct Rows {
    /// Timestamps in ms since the epoch, in the query's order.
    pub ts: Vec<i64>,
    /// The value of each timestamp.
    pub values: Column,
}

impl Rows {
    /// Number of rows.
    pub fn len(&self) -> usize {
        self.ts.len()
    }

    /// Whether there are no rows.
    pub fn is_empty(&self) -> bool {
        self.ts.is_empty()
    }

    /// The series' value type.
    pub fn value_type(&self) -> ValueType {
        self.values.value_type()
    }
}

/// Database statistics (`tsdb_stats`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Stats {
    /// Parts on disk.
    pub parts: u64,
    /// Partitions.
    pub partitions: u64,
    /// Rows in parts.
    pub rows: u64,
    /// Bytes in parts.
    pub bytes: u64,
    /// Merges done since the server started.
    pub merges: u64,
    /// Rows written by those merges.
    pub merged_rows: u64,
    /// Granules merges copied without decoding.
    pub passthrough_granules: u64,
    /// Rows in the WAL-backed memtable, not yet in parts.
    pub memtable_rows: u64,
}

/// What the server did for this connection's previous query (`tsdb_query_stats`),
/// like ClickHouse's "Processed N rows, M MB".
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct QueryStats {
    /// Seconds the server spent on the query.
    pub elapsed_s: f64,
    /// Rows covered: `rows_decoded + rows_from_stats + memtable_rows`.
    pub rows: u64,
    /// Rows decompressed (whole granules).
    pub rows_decoded: u64,
    /// Rows answered from granule statistics, without decoding.
    pub rows_from_stats: u64,
    /// Rows read from memtables (not yet in parts).
    pub memtable_rows: u64,
    /// Uncompressed size of the rows covered (timestamp + value).
    pub bytes: u64,
    /// Compressed bytes of the granules decoded.
    pub bytes_read: u64,
    /// Parts with rows of the series in the range.
    pub parts: u64,
    /// Their granules in the range.
    pub granules: u64,
    /// Granules answered from their index entry.
    pub granules_from_stats: u64,
    /// Rows, or buckets, returned.
    pub result_rows: u64,
}

/// How a multi-series bucket query ([`Client::query_buckets_multi`](crate::Client::query_buckets_multi),
/// `_spread_multi`, `_twa_multi`) cuts each series' range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Buckets<'a> {
    /// One bucket for the whole range.
    One,
    /// Fixed buckets of this many ms (0: one bucket), the same for every series.
    Every(i64),
    /// Fixed buckets, `intervals[i]` ms for series `i` (as long as the series list).
    Intervals(&'a [i64]),
    /// Calendar periods, in any time zone (as `query_group`).
    Group(&'a Group),
    /// These bucket bounds for every series (as `query_bounds`).
    Bounds(&'a [i64]),
}

/// Options of the multi-series calls (protocol version 10).
///
/// ```
/// use tsdb_client::MultiOpts;
/// let starts = [0, 1_000];
/// let m = MultiOpts { starts: Some(&starts), max_rows: 1_000_000, ..MultiOpts::default() };
/// assert_eq!(m.threads, 0);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MultiOpts<'a> {
    /// Threads the request uses at most; 0: the server's (or the database's) `request_threads`.
    pub threads: u16,
    /// Rows or buckets in all series together, at most; past it the call fails with
    /// [`Status::TooManyBuckets`](crate::Status::TooManyBuckets). 0: the server's `--multi-max-rows`
    /// (embedded: no limit).
    pub max_rows: u64,
    /// The start of series `i`'s range instead of the range's (as long as the series list).
    pub starts: Option<&'a [i64]>,
    /// The end (exclusive) of series `i`'s range instead of the range's.
    pub ends: Option<&'a [i64]>,
}
