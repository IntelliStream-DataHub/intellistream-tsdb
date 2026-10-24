//! Query options and results.

use crate::value::Value;

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

/// Order and limit for [`query_points`](crate::Client::query_points),
/// [`query_raw`](crate::Client::query_raw) and
/// [`query_group`](crate::Client::query_group).
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

/// Calendar grouping: `count` units per bucket, in a time zone.
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
    /// name such as `"Europe/Oslo"`, resolved on the server.
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

    /// Sets the time zone.
    pub fn timezone(mut self, tz: impl Into<String>) -> Self {
        self.timezone = Some(tz.into());
        self
    }
}

/// One aggregate bucket. Only non-empty buckets are returned.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bucket {
    /// The bucket's start in ms since the epoch (the UTC instant its local
    /// period begins); for a single bucket, the query's start.
    pub start: i64,
    /// Number of points.
    pub count: u64,
    /// Sum of the values (NaN values propagate into it).
    pub sum: f64,
    /// Smallest value, ignoring NaN (NaN if all values are NaN).
    pub min: Value,
    /// Largest value, ignoring NaN (NaN if all values are NaN).
    pub max: Value,
}

impl Bucket {
    /// The mean, `sum / count` (0 for an empty bucket).
    pub fn avg(&self) -> f64 {
        if self.count == 0 { 0.0 } else { self.sum / self.count as f64 }
    }
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
