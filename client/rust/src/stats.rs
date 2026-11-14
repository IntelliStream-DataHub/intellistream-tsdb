// SPDX-License-Identifier: Apache-2.0
//! Statistics (`STATS_SERIES` and `STATS_TENANTS`, protocol 15; `tsdb_series_stats`, `tsdb_tenant_stats_get` and
//! `tsdb_hot_series` in the engine): the types [`Client::series_stats`](crate::Client::series_stats),
//! [`Client::tenant_stats`](crate::Client::tenant_stats) and the embedded `Db` of the crate
//! `intellistream-tsdb-embedded` share.

use crate::error::Status;
use crate::value::ValueType;

/// `TSDB_SSTAT_NO_RANGE`: no stored row at or after the retention cutoff; `first_ts` and `last_ts` are 0.
pub const SSTAT_NO_RANGE: u16 = 2;
/// `TSDB_SSTAT_WRITE_FROM_MTIME`: `last_write_ms` comes from the file time of a part written before format 9.
pub const SSTAT_WRITE_FROM_MTIME: u16 = 4;
/// `TSDB_STATS_NO_BOUND`: the `reads_err` and `writes_err` of a user without `read` on every series are 0.
pub const SSTAT_NO_BOUND: u16 = 8;

/// The storage numbers of one series: 176 bytes on the wire (`tsdb_series_stat`). "Physical": rows and bytes as
/// stored, including rows a newer write, a tombstone or retention hides until a merge removes them. Every field but
/// `status` and `flags` is 0 when `status` is set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SeriesStat {
    /// `None`: answered. `Some(Status::NotFound)`: no such series (only for a user who reads every series);
    /// `Some(Status::Denied)`: the user holds no `read` right on it, whether or not it exists.
    pub status: Option<Status>,
    /// The partition class (0 to 3).
    pub class: u8,
    /// The series' type; `None` whenever `status` is set.
    pub ty: Option<ValueType>,
    /// `SSTAT_*` bits.
    pub flags: u16,
    /// Rows in parts and memtables.
    pub rows: u64,
    /// Nulls in parts.
    pub nulls: u64,
    /// Rows in the memtables.
    pub memtable_rows: u64,
    /// Part rows times (8 + the width of the type).
    pub logical_bytes: u64,
    /// Bytes of timestamps.
    pub ts_bytes: u64,
    /// Bytes of values.
    pub value_bytes: u64,
    /// The series' share of the granule indexes.
    pub index_bytes: u64,
    /// 24 bytes per tombstone.
    pub tomb_bytes: u64,
    /// Parts that hold the series.
    pub parts: u64,
    /// Partitions that hold it.
    pub partitions: u64,
    /// Granules.
    pub granules: u64,
    /// Granules of a full 8192 rows.
    pub full_granules: u64,
    /// First stored row (0 with `SSTAT_NO_RANGE`).
    pub first_ts: i64,
    /// Last stored row (0 with `SSTAT_NO_RANGE`).
    pub last_ts: i64,
    /// The newest part holding it (its `written_ms`) or the creation of its memtable.
    pub last_write_ms: i64,
    /// Bytes of its granules in the page cache, with `resident` asked; else 0.
    pub resident_bytes: u64,
    /// Queries in the last 60 to 120 seconds, from the hot-reads list: 0 when it is not in it.
    pub reads_est: u64,
    /// The error bound of `reads_est`.
    pub reads_err: u64,
    /// Rows written in the last 60 to 120 seconds, from the hot-writes list.
    pub writes_est: u64,
    /// The error bound of `writes_est`.
    pub writes_err: u64,
    /// Reserved: 0.
    pub live_rows: u64,
}

impl SeriesStat {
    /// Bytes of one record on the wire.
    pub const WIRE_LEN: usize = 176;

    /// Decodes one record (`tsdb_series_stat`, little-endian); `None` for a type or a status out of shape.
    #[doc(hidden)]
    pub fn from_wire(b: &[u8]) -> Option<SeriesStat> {
        if b.len() != Self::WIRE_LEN {
            return None;
        }
        let u = |o: usize| u64::from_le_bytes(b[o..o + 8].try_into().unwrap());
        let status = i32::from_le_bytes(b[0..4].try_into().unwrap());
        let st = if status == 0 { None } else { Some(Status::from_code(status)) };
        let ty = match b[5] {
            _ if st.is_some() => None,
            0 => Some(ValueType::Float32),
            1 => Some(ValueType::Float64),
            2 => Some(ValueType::Int64),
            3 => Some(ValueType::Bool),
            4 => Some(ValueType::State),
            _ => return None,
        };
        Some(SeriesStat {
            status: st,
            class: b[4],
            ty,
            flags: u16::from_le_bytes(b[6..8].try_into().unwrap()),
            rows: u(8),
            nulls: u(16),
            memtable_rows: u(24),
            logical_bytes: u(32),
            ts_bytes: u(40),
            value_bytes: u(48),
            index_bytes: u(56),
            tomb_bytes: u(64),
            parts: u(72),
            partitions: u(80),
            granules: u(88),
            full_granules: u(96),
            first_ts: u(104) as i64,
            last_ts: u(112) as i64,
            last_write_ms: u(120) as i64,
            resident_bytes: u(128),
            reads_est: u(136),
            reads_err: u(144),
            writes_est: u(152),
            writes_err: u(160),
            live_rows: u(168),
        })
    }
}

/// The numbers of one tenant, in one volume or summed over the volumes (`tsdb_tenant_stats`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TenantNumbers {
    /// Series that belong to the tenant.
    pub series: u64,
    /// Series created since the server started.
    pub series_created: u64,
    /// Series active in the current hour (a HyperLogLog: about 3 percent).
    pub active_series_cur: u64,
    /// Series active in the hour before.
    pub active_series_prev: u64,
    /// Rows (physical).
    pub rows: u64,
    /// Nulls.
    pub nulls: u64,
    /// Part rows times (8 + the width of the type).
    pub logical_bytes: u64,
    /// Bytes of timestamps.
    pub ts_bytes: u64,
    /// Bytes of values.
    pub value_bytes: u64,
    /// Bytes of granule indexes.
    pub index_bytes: u64,
    /// Granules.
    pub granules: u64,
    /// Rows flushed since the start, the WAL replay left out.
    pub ingested_rows: u64,
    /// Bytes of them.
    pub ingested_bytes: u64,
    /// Rows merges read.
    pub merged_rows: u64,
    /// Rows merges removed (overwritten, deleted, expired).
    pub merge_removed_rows: u64,
    /// Rows retention dropped.
    pub retention_dropped_rows: u64,
    /// Page cache, sampled.
    pub resident_bytes: u64,
    /// The age in seconds of the oldest partition waiting for its recount.
    pub stale_s: u64,
}

/// What a tenant admin's own requests cost, per tenant (the `CALLER` numbers of a summed row).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TenantCaller {
    /// Query requests.
    pub requests_query: u64,
    /// Insert requests.
    pub requests_insert: u64,
    /// Delete, replace and retention requests.
    pub requests_delete: u64,
    /// Series and user admin requests.
    pub requests_admin: u64,
    /// Other requests.
    pub requests_other: u64,
    /// Rows the queries decoded.
    pub rows_decoded: u64,
    /// Rows they answered from granule statistics.
    pub rows_from_stats: u64,
    /// Rows they read from memtables.
    pub rows_from_memtable: u64,
    /// Bytes they read.
    pub bytes_read: u64,
    /// Rows they returned.
    pub result_rows: u64,
    /// Microseconds they took.
    pub query_us: u64,
    /// Rows inserted.
    pub inserted_rows: u64,
    /// Bytes inserted.
    pub inserted_bytes: u64,
    /// Requests that failed as invalid.
    pub errors_invalid: u64,
    /// Requests that failed as not found.
    pub errors_notfound: u64,
    /// Requests that failed with an I/O error.
    pub errors_io: u64,
    /// Other failures.
    pub errors_other: u64,
    /// Requests refused as denied.
    pub denied_requests: u64,
    /// Series refused as denied.
    pub denied_series: u64,
    /// Failed logins.
    pub failed_logins: u64,
}

/// What a hot-list entry counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HotKind {
    /// Queries in the last 60 to 120 seconds (sampled).
    Reads = 1,
    /// Rows written in the same window.
    Writes = 2,
    /// The worst bytes per row times 1000, over full granules of merged parts.
    BytesPerRow = 3,
}

impl HotKind {
    #[doc(hidden)]
    pub fn from_code(c: u8) -> Option<HotKind> {
        match c {
            1 => Some(HotKind::Reads),
            2 => Some(HotKind::Writes),
            3 => Some(HotKind::BytesPerRow),
            _ => None,
        }
    }
}

/// One hot-list entry of a tenant row. `id` 0 is the threshold: what a series not in the list weighs at most.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hot {
    /// The list.
    pub kind: HotKind,
    /// The series, or 0 for the threshold.
    pub id: u64,
    /// The estimate.
    pub estimate: u64,
    /// Its error bound.
    pub error: u64,
}

/// One row of [`TenantStats`]: a tenant, summed or in one volume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantRow {
    /// `None`: answered. `Some(Status::NotFound)`: a prefix that is not in the tenant list (for a user who may see
    /// it); `Some(Status::Denied)`: the user may not see the tenant.
    pub status: Option<Status>,
    /// `None`: the sum over the volumes; `Some(n)`: volume `n`.
    pub volume: Option<u32>,
    /// The tenant's prefix; "" is the tenant "(none)".
    pub prefix: String,
    /// `TENANT_PARTIAL`: a sweep or a move of series between tenants is not finished: the numbers are stale.
    pub partial: bool,
    /// The numbers (all 0 when `status` is set).
    pub numbers: TenantNumbers,
    /// With `caller` asked, on the summed row.
    pub caller: Option<TenantCaller>,
    /// With `hot` asked, on the summed row.
    pub hot: Vec<Hot>,
}

/// The answer of [`Client::tenant_stats`](crate::Client::tenant_stats).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TenantStats {
    /// Some sweep or move is not finished (`TSDB_TSTATS_PARTIAL`).
    pub partial: bool,
    /// The ingest numbers are approximate (`TSDB_TSTATS_INGEST_APPROX`).
    pub ingest_approx: bool,
    /// Of the tenant list: one more at every change.
    pub generation: u32,
    /// Of the list's prefixes: two servers with the same list have the same hash.
    pub list_hash: u64,
    /// Parts the sweeper has counted (0 unless the user is a superuser or reads every series).
    pub parts_counted: u64,
    /// Parts listed (0 likewise).
    pub parts_listed: u64,
    /// The rows: each tenant's summed row, then its volumes' rows with `volumes` asked.
    pub rows: Vec<TenantRow>,
}

/// What [`Client::tenant_stats`](crate::Client::tenant_stats) is asked for.
///
/// ```
/// use tsdb_client::TenantOpts;
/// let o = TenantOpts::default().volumes().hot(10).caller();
/// assert!(o.volumes && o.caller && o.hot == 10);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TenantOpts {
    /// A row per volume after each tenant's summed row.
    pub volumes: bool,
    /// Hot-list entries per kind on the summed row (at most 256); 0: none.
    pub hot: u16,
    /// The caller's counters on the summed row.
    pub caller: bool,
}

impl TenantOpts {
    /// A row per volume.
    pub fn volumes(mut self) -> Self {
        self.volumes = true;
        self
    }

    /// `k` hot-list entries per kind.
    pub fn hot(mut self, k: u16) -> Self {
        self.hot = k;
        self
    }

    /// The caller's counters.
    pub fn caller(mut self) -> Self {
        self.caller = true;
        self
    }
}
