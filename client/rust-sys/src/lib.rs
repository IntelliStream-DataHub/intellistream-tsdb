//! Raw FFI declarations of tsdb's public C API: `include/tsdb.h` and `include/tsdb_calendar.h`.
//!
//! By default the build script compiles the engine into this crate (see the README), so these
//! functions are available without a `libtsdb.so`. Everything here is `unsafe` and follows the C
//! header's rules; the comments only repeat what a Rust caller must know. Prefer the safe API,
//! `tsdb_client::Db` (crate `tsdb-client`, feature `embedded`).
//!
//! - All functions are thread safe for one `*mut tsdb`; it may be shared between threads.
//! - Arrays the library returns (`tsdb_bucket`, `tsdb_calendar_bounds`) are freed with
//!   [`tsdb_free`], [`tsdb_points`] with [`tsdb_points_free`].
//! - Strings returned by `tsdb_series_name`, `tsdb_state_name` and the like stay valid until
//!   [`tsdb_close`].
//! - Query callbacks get arrays valid only during the call; a callback must not unwind into C.
//! - C enums are `int` here (`tsdb_type`, `tsdb_order`, `tsdb_unit`): a Rust enum would make an
//!   out-of-range value from C undefined behaviour.
#![allow(non_camel_case_types)]
#![no_std]

use core::ffi::{c_char, c_int, c_uint, c_void};

// The bundled libzstd: referenced so that it is linked.
#[cfg(feature = "bundled-zstd")]
use zstd_sys as _;

/// The opaque database handle.
#[repr(C)]
pub struct tsdb {
    _private: [u8; 0],
    _not_send: core::marker::PhantomData<*mut u8>,
}

pub const TSDB_OK: c_int = 0;
pub const TSDB_ERR_IO: c_int = -1;
pub const TSDB_ERR_NOMEM: c_int = -2;
pub const TSDB_ERR_CORRUPT: c_int = -3;
pub const TSDB_ERR_INVALID: c_int = -4;
pub const TSDB_ERR_LOCKED: c_int = -5;
pub const TSDB_ERR_NOTFOUND: c_int = -6;
pub const TSDB_ERR_TOO_MANY_BUCKETS: c_int = -7;
/// A query callback returned non-zero, or the database is closing.
pub const TSDB_ERR_STOPPED: c_int = -8;
pub const TSDB_ERR_READONLY: c_int = -9;

/// A series id (`tsdb_sid`): 64 bits, opaque, not dense.
pub type tsdb_sid = u64;

/// Raised with every incompatible change of a public struct's layout or of a function's signature;
/// bindings refuse a library whose [`tsdb_abi_version`] differs.
pub const TSDB_ABI_VERSION: c_int = 6;

pub const TSDB_TS_MIN: i64 = i64::MIN;
pub const TSDB_TS_MAX: i64 = i64::MAX;

pub const TSDB_VALUES_AUTO: c_int = 0;
pub const TSDB_VALUES_GORILLA: c_int = 1;
pub const TSDB_VALUES_CHIMP: c_int = 2;

/// Options of [`tsdb_open`]; fill with [`tsdb_options_init`] first. See `tsdb.h` for each field.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct tsdb_options {
    pub partition_ms: i64,
    pub granule_rows: u32,
    pub zstd_level: c_int,
    pub value_codec: c_int,
    pub fsync: c_int,
    pub merge_threads: c_int,
    pub query_threads: c_int,
    pub buffer_rows: usize,
    pub flush_interval_ms: c_int,
    pub wal_sync_ms: c_int,
    pub wal_max_batch_rows: usize,
    pub max_part_bytes: u64,
    pub read_only: c_int,
    pub follow: c_int,
    pub refresh_ms: c_int,
    pub merge_idle_s: c_int,
    pub obsolete_part_lifetime_s: c_int,
    pub verify_checksums: c_int,
    pub retention_merge_s: c_int,
    pub clock_ms: Option<unsafe extern "C" fn(arg: *mut c_void) -> i64>,
    pub clock_arg: *mut c_void,
    pub create_volumes: c_int,
    pub request_threads: c_int,
    pub partition_classes: c_int,
    pub pin_index: c_int,
    pub insert_zstd_level: c_int,
    pub stats: c_int,
    pub stats_sweep_mb_s: c_int,
    pub latest_cache: usize,
    pub stats_recompute_s: c_int,
    pub stats_resident_s: c_int,
    pub tenants: *const *const c_char,
    pub ntenants: usize,
}

impl Default for tsdb_options {
    /// All zero, as C's `{0}`; [`tsdb_options_init`] sets the defaults.
    fn default() -> tsdb_options {
        // SAFETY: integers, a nullable function pointer (None is null) and a raw pointer: all-zero is valid.
        unsafe { core::mem::zeroed() }
    }
}

pub type tsdb_type = c_int;
pub const TSDB_FLOAT32: tsdb_type = 0;
pub const TSDB_FLOAT64: tsdb_type = 1;
pub const TSDB_INT64: tsdb_type = 2;
pub const TSDB_BOOL: tsdb_type = 3;
pub const TSDB_STATE: tsdb_type = 4;

/// A column: `values` points at `f32`, `f64`, `i64`, `u8` (bool: 0 or 1) or `u32` (state codes)
/// by `type_`; `valid` is a bitmap (bit i, LSB first, set when value i is known) or null.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct tsdb_values {
    pub type_: tsdb_type,
    pub values: *const c_void,
    pub valid: *const u8,
}

pub const TSDB_NULL_F32_BITS: u32 = 0x7fc0_1001;
pub const TSDB_NULL_F64_BITS: u64 = 0x7ff8_0000_0000_1001;

pub type tsdb_raw_fn =
    Option<unsafe extern "C" fn(ctx: *mut c_void, ts: *const i64, values: *const f32, n: usize) -> c_int>;
pub type tsdb_values_fn =
    Option<unsafe extern "C" fn(ctx: *mut c_void, ts: *const i64, values: *const tsdb_values, n: usize) -> c_int>;
pub type tsdb_part_fn = Option<unsafe extern "C" fn(ctx: *mut c_void, info: *const tsdb_part_info)>;

/// Points of [`tsdb_query_points`]; free with [`tsdb_points_free`].
#[repr(C)]
#[derive(Debug)]
pub struct tsdb_points {
    pub ts: *mut i64,
    pub values: *mut f32,
    pub n: usize,
    pub cap: usize,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct tsdb_bucket {
    pub start: i64,
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub isum: i64,
    pub imin: i64,
    pub imax: i64,
}

pub type tsdb_order = c_int;
pub const TSDB_ASC: tsdb_order = 0;
pub const TSDB_DESC: tsdb_order = 1;

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct tsdb_query_opts {
    pub order: tsdb_order,
    /// 0: no limit.
    pub limit: usize,
}

pub type tsdb_unit = c_int;
pub const TSDB_SECOND: tsdb_unit = 1;
pub const TSDB_MINUTE: tsdb_unit = 2;
pub const TSDB_HOUR: tsdb_unit = 3;
pub const TSDB_DAY: tsdb_unit = 4;
pub const TSDB_WEEK: tsdb_unit = 5;
pub const TSDB_MONTH: tsdb_unit = 6;
pub const TSDB_QUARTER: tsdb_unit = 7;
pub const TSDB_YEAR: tsdb_unit = 8;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct tsdb_group {
    pub unit: tsdb_unit,
    /// 0 means 1.
    pub count: u32,
    /// Null or "UTC" for [`tsdb_query_group_ex`]; any zone for [`tsdb_query_group_local`].
    pub timezone: *const c_char,
}

/// A bucket and the spread of its known values (`tsdb_query_spread*`).
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct tsdb_spread_bucket {
    pub b: tsdb_bucket,
    /// max - min.
    pub range: f64,
    pub var_pop: f64,
    pub var_samp: f64,
    pub stddev_pop: f64,
    pub stddev_samp: f64,
}

pub type tsdb_twa_method = c_int;
pub const TSDB_TWA_STEP: tsdb_twa_method = 0;
pub const TSDB_TWA_LINEAR: tsdb_twa_method = 1;

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct tsdb_twa_opts {
    pub method: tsdb_twa_method,
    /// 0: no staleness limit.
    pub max_gap_ms: i64,
    /// Of the buckets.
    pub order: tsdb_order,
    /// 0: no limit.
    pub limit: usize,
    /// Nonzero: decode every granule, never its statistics.
    pub no_stats: c_int,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct tsdb_twa_bucket {
    pub start: i64,
    pub twa: f64,
    pub area: f64,
    pub covered_ms: i64,
    pub count: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct tsdb_query_stats {
    pub elapsed_s: f64,
    pub rows: u64,
    pub rows_decoded: u64,
    pub rows_from_stats: u64,
    pub memtable_rows: u64,
    pub bytes: u64,
    pub bytes_read: u64,
    pub parts: u64,
    pub granules: u64,
    pub granules_from_stats: u64,
    pub result_rows: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct tsdb_stats {
    pub parts: u64,
    pub partitions: u64,
    pub rows: u64,
    pub bytes: u64,
    pub merges: u64,
    pub merged_rows: u64,
    pub passthrough_granules: u64,
    pub memtable_rows: u64,
    pub broken_parts: u64,
    pub follow_errors: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct tsdb_part_info {
    pub name: *const c_char,
    pub partition: i64,
    pub min_ts: i64,
    pub max_ts: i64,
    pub rows: u64,
    pub bytes: u64,
    pub granules: u64,
    pub level: u32,
    pub created_s: i64,
    pub written_s: i64,
    /// Partition class 0..3: 1, 7, 28 or 364 base partitions (`tsdb_options.partition_classes`).
    pub cls: c_int,
    /// The partition's time range `[start_ms, end_ms)`.
    pub start_ms: i64,
    pub end_ms: i64,
}

unsafe extern "C" {
    pub fn tsdb_options_init(opts: *mut tsdb_options);
    pub fn tsdb_open(dir: *const c_char, opts: *const tsdb_options, out: *mut *mut tsdb) -> c_int;
    pub fn tsdb_close(db: *mut tsdb) -> c_int;
    pub fn tsdb_strerror(err: c_int) -> *const c_char;

    pub fn tsdb_series(db: *mut tsdb, name: *const c_char, id: *mut tsdb_sid) -> c_int;
    pub fn tsdb_series_at(db: *mut tsdb, name: *const c_char, path: *const c_char, id: *mut tsdb_sid) -> c_int;
    pub fn tsdb_series_lookup(db: *mut tsdb, name: *const c_char, id: *mut tsdb_sid) -> c_int;
    pub fn tsdb_series_create(
        db: *mut tsdb,
        name: *const c_char,
        type_: tsdb_type,
        path: *const c_char,
        id: *mut tsdb_sid,
    ) -> c_int;
    pub fn tsdb_series_type(db: *mut tsdb, id: tsdb_sid, type_: *mut tsdb_type) -> c_int;
    pub fn tsdb_series_class(db: *mut tsdb, series: tsdb_sid, cls: *mut c_int) -> c_int;
    pub fn tsdb_series_count(db: *mut tsdb) -> u64;
    /// Calls `fn_` once for every series; stops at the first non-zero return and returns it.
    pub fn tsdb_series_foreach(
        db: *mut tsdb,
        fn_: Option<unsafe extern "C" fn(ctx: *mut c_void, id: tsdb_sid) -> c_int>,
        ctx: *mut c_void,
    ) -> c_int;
    pub fn tsdb_series_name(db: *mut tsdb, id: tsdb_sid) -> *const c_char;
    pub fn tsdb_series_path(db: *mut tsdb, id: tsdb_sid) -> *const c_char;
    pub fn tsdb_volume_count(db: *mut tsdb) -> u32;
    pub fn tsdb_volume_path(db: *mut tsdb, index: u32) -> *const c_char;

    pub fn tsdb_insert(db: *mut tsdb, series: *const tsdb_sid, ts: *const i64, values: *const f32, n: usize) -> c_int;
    pub fn tsdb_insert_series(db: *mut tsdb, series: tsdb_sid, ts: *const i64, values: *const f32, n: usize) -> c_int;
    pub fn tsdb_insert_values(
        db: *mut tsdb,
        series: *const tsdb_sid,
        ts: *const i64,
        values: *const tsdb_values,
        n: usize,
    ) -> c_int;
    pub fn tsdb_insert_series_values(
        db: *mut tsdb,
        series: tsdb_sid,
        ts: *const i64,
        values: *const tsdb_values,
        n: usize,
    ) -> c_int;

    pub fn tsdb_state(db: *mut tsdb, series: tsdb_sid, name: *const c_char, code: *mut u32) -> c_int;
    pub fn tsdb_state_lookup(db: *mut tsdb, series: tsdb_sid, name: *const c_char, code: *mut u32) -> c_int;
    pub fn tsdb_state_name(db: *mut tsdb, series: tsdb_sid, code: u32) -> *const c_char;

    pub fn tsdb_append(db: *mut tsdb, series: tsdb_sid, ts: i64, value: f32) -> c_int;
    pub fn tsdb_commit(db: *mut tsdb) -> c_int;
    pub fn tsdb_flush(db: *mut tsdb) -> c_int;

    pub fn tsdb_query_raw(
        db: *mut tsdb,
        series: tsdb_sid,
        start: i64,
        end: i64,
        f: tsdb_raw_fn,
        ctx: *mut c_void,
    ) -> c_int;
    pub fn tsdb_query_points(db: *mut tsdb, series: tsdb_sid, start: i64, end: i64, out: *mut tsdb_points) -> c_int;
    pub fn tsdb_points_free(p: *mut tsdb_points);
    pub fn tsdb_free(p: *mut c_void);

    pub fn tsdb_query_agg(
        db: *mut tsdb,
        series: tsdb_sid,
        start: i64,
        end: i64,
        interval_ms: i64,
        out: *mut *mut tsdb_bucket,
        nout: *mut usize,
    ) -> c_int;
    pub fn tsdb_query_raw_ex(
        db: *mut tsdb,
        series: tsdb_sid,
        start: i64,
        end: i64,
        opts: *const tsdb_query_opts,
        f: tsdb_raw_fn,
        ctx: *mut c_void,
    ) -> c_int;
    pub fn tsdb_query_points_ex(
        db: *mut tsdb,
        series: tsdb_sid,
        start: i64,
        end: i64,
        opts: *const tsdb_query_opts,
        out: *mut tsdb_points,
    ) -> c_int;
    pub fn tsdb_query_values(
        db: *mut tsdb,
        series: tsdb_sid,
        start: i64,
        end: i64,
        opts: *const tsdb_query_opts,
        f: tsdb_values_fn,
        ctx: *mut c_void,
    ) -> c_int;
    pub fn tsdb_query_group(
        db: *mut tsdb,
        series: tsdb_sid,
        start: i64,
        end: i64,
        group: *const tsdb_group,
        out: *mut *mut tsdb_bucket,
        nout: *mut usize,
    ) -> c_int;
    pub fn tsdb_query_bounds(
        db: *mut tsdb,
        series: tsdb_sid,
        start: i64,
        end: i64,
        bounds: *const i64,
        n: usize,
        opts: *const tsdb_query_opts,
        out: *mut *mut tsdb_bucket,
        nout: *mut usize,
    ) -> c_int;
    pub fn tsdb_query_group_ex(
        db: *mut tsdb,
        series: tsdb_sid,
        start: i64,
        end: i64,
        group: *const tsdb_group,
        opts: *const tsdb_query_opts,
        out: *mut *mut tsdb_bucket,
        nout: *mut usize,
    ) -> c_int;

    pub fn tsdb_query_agg_ex(
        db: *mut tsdb,
        series: tsdb_sid,
        start: i64,
        end: i64,
        interval_ms: i64,
        opts: *const tsdb_query_opts,
        out: *mut *mut tsdb_bucket,
        nout: *mut usize,
    ) -> c_int;
    pub fn tsdb_query_lttb(
        db: *mut tsdb,
        series: tsdb_sid,
        start: i64,
        end: i64,
        threshold: usize,
        f: tsdb_values_fn,
        ctx: *mut c_void,
    ) -> c_int;
    pub fn tsdb_query_lttb_ex(
        db: *mut tsdb,
        series: tsdb_sid,
        start: i64,
        end: i64,
        threshold: usize,
        opts: *const tsdb_query_opts,
        f: tsdb_values_fn,
        ctx: *mut c_void,
    ) -> c_int;
    pub fn tsdb_query_spread(
        db: *mut tsdb,
        series: tsdb_sid,
        start: i64,
        end: i64,
        interval_ms: i64,
        opts: *const tsdb_query_opts,
        out: *mut *mut tsdb_spread_bucket,
        nout: *mut usize,
    ) -> c_int;
    pub fn tsdb_query_spread_group(
        db: *mut tsdb,
        series: tsdb_sid,
        start: i64,
        end: i64,
        group: *const tsdb_group,
        opts: *const tsdb_query_opts,
        out: *mut *mut tsdb_spread_bucket,
        nout: *mut usize,
    ) -> c_int;
    pub fn tsdb_query_spread_bounds(
        db: *mut tsdb,
        series: tsdb_sid,
        start: i64,
        end: i64,
        bounds: *const i64,
        n: usize,
        opts: *const tsdb_query_opts,
        out: *mut *mut tsdb_spread_bucket,
        nout: *mut usize,
    ) -> c_int;
    pub fn tsdb_query_twa(
        db: *mut tsdb,
        series: tsdb_sid,
        start: i64,
        end: i64,
        interval_ms: i64,
        opts: *const tsdb_twa_opts,
        out: *mut *mut tsdb_twa_bucket,
        nout: *mut usize,
    ) -> c_int;
    pub fn tsdb_query_twa_group(
        db: *mut tsdb,
        series: tsdb_sid,
        start: i64,
        end: i64,
        group: *const tsdb_group,
        opts: *const tsdb_twa_opts,
        out: *mut *mut tsdb_twa_bucket,
        nout: *mut usize,
    ) -> c_int;
    pub fn tsdb_query_twa_bounds(
        db: *mut tsdb,
        series: tsdb_sid,
        start: i64,
        end: i64,
        bounds: *const i64,
        n: usize,
        opts: *const tsdb_twa_opts,
        out: *mut *mut tsdb_twa_bucket,
        nout: *mut usize,
    ) -> c_int;

    pub fn tsdb_delete_range(db: *mut tsdb, series: tsdb_sid, start: i64, end: i64) -> c_int;
    pub fn tsdb_replace_range(
        db: *mut tsdb,
        series: tsdb_sid,
        start: i64,
        end: i64,
        ts: *const i64,
        values: *const tsdb_values,
        n: usize,
    ) -> c_int;
    pub fn tsdb_set_retention(db: *mut tsdb, series: tsdb_sid, limit_ms: i64) -> c_int;
    pub fn tsdb_get_retention(db: *mut tsdb, series: tsdb_sid, limit_ms: *mut i64) -> c_int;

    /// What the calling thread's last query did.
    pub fn tsdb_last_query_stats(out: *mut tsdb_query_stats);

    pub fn tsdb_optimize(db: *mut tsdb) -> c_int;
    pub fn tsdb_drop_before(db: *mut tsdb, ts: i64) -> c_int;
    pub fn tsdb_get_stats(db: *mut tsdb, st: *mut tsdb_stats) -> c_int;
    pub fn tsdb_failed(db: *mut tsdb, msg: *mut c_char, len: usize) -> c_int;
    pub fn tsdb_foreach_part(db: *mut tsdb, f: tsdb_part_fn, ctx: *mut c_void) -> c_int;
    pub fn tsdb_refresh(db: *mut tsdb) -> c_int;
    pub fn tsdb_simd_level() -> *const c_char;
    /// [`TSDB_ABI_VERSION`] of the library actually linked.
    pub fn tsdb_abi_version() -> c_int;

    // tsdb_calendar.h
    /// `*bounds` is allocated by the library: free it with [`tsdb_free`].
    pub fn tsdb_calendar_bounds(
        zone: *const c_char,
        unit: tsdb_unit,
        count: u32,
        start: i64,
        end: i64,
        bounds: *mut *mut i64,
        n: *mut usize,
    ) -> c_int;
    pub fn tsdb_query_group_local(
        db: *mut tsdb,
        series: tsdb_sid,
        start: i64,
        end: i64,
        group: *const tsdb_group,
        opts: *const tsdb_query_opts,
        out: *mut *mut tsdb_bucket,
        nout: *mut usize,
    ) -> c_int;
    pub fn tsdb_query_spread_group_local(
        db: *mut tsdb,
        series: tsdb_sid,
        start: i64,
        end: i64,
        group: *const tsdb_group,
        opts: *const tsdb_query_opts,
        out: *mut *mut tsdb_spread_bucket,
        nout: *mut usize,
    ) -> c_int;
    pub fn tsdb_query_twa_group_local(
        db: *mut tsdb,
        series: tsdb_sid,
        start: i64,
        end: i64,
        group: *const tsdb_group,
        opts: *const tsdb_twa_opts,
        out: *mut *mut tsdb_twa_bucket,
        nout: *mut usize,
    ) -> c_int;
}

pub type tsdb_multi_kind = c_int;
pub const TSDB_MULTI_VALUES: tsdb_multi_kind = 1;
pub const TSDB_MULTI_BUCKETS: tsdb_multi_kind = 2;
pub const TSDB_MULTI_SPREAD: tsdb_multi_kind = 3;
pub const TSDB_MULTI_TWA: tsdb_multi_kind = 4;
pub const TSDB_MULTI_LTTB: tsdb_multi_kind = 5;
pub const TSDB_MULTI_TRIM_LO: c_uint = 1;
pub const TSDB_MULTI_TRIM_HI: c_uint = 2;

/// One query over many series (`tsdb_query_multi`); the per-series arrays are null or as long as the list.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct tsdb_multi_query {
    pub kind: tsdb_multi_kind,
    pub start: i64,
    pub end: i64,
    pub starts: *const i64,
    pub ends: *const i64,
    pub opts: tsdb_query_opts,
    pub interval_ms: i64,
    pub intervals: *const i64,
    /// Any zone.
    pub group: *const tsdb_group,
    pub bounds: *const i64,
    pub nbounds: usize,
    pub trim: c_uint,
    /// LTTB.
    pub threshold: usize,
    pub twa: tsdb_twa_opts,
}

impl Default for tsdb_multi_query {
    fn default() -> Self {
        tsdb_multi_query {
            kind: 0,
            start: 0,
            end: 0,
            starts: core::ptr::null(),
            ends: core::ptr::null(),
            opts: tsdb_query_opts::default(),
            interval_ms: 0,
            intervals: core::ptr::null(),
            group: core::ptr::null(),
            bounds: core::ptr::null(),
            nbounds: 0,
            trim: 0,
            threshold: 0,
            twa: tsdb_twa_opts::default(),
        }
    }
}

/// One call of a `tsdb_multi_fn`: rows (VALUES, LTTB) or buckets of the series at `index`.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct tsdb_series_result {
    pub index: usize,
    pub series: tsdb_sid,
    pub status: c_int,
    /// The series' last call.
    pub last: c_int,
    pub n: usize,
    pub ts: *const i64,
    pub values: tsdb_values,
    pub buckets: *const c_void,
}

pub type tsdb_multi_fn = Option<unsafe extern "C" fn(ctx: *mut c_void, r: *const tsdb_series_result) -> c_int>;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct tsdb_multi_opts {
    /// 0: tsdb_options.request_threads.
    pub threads: c_int,
    /// 0: no limit.
    pub max_rows: u64,
    /// 0: 64 MiB.
    pub max_held_bytes: usize,
    pub cancelled: Option<unsafe extern "C" fn(ctx: *mut c_void) -> c_int>,
    pub on_wait: Option<unsafe extern "C" fn(ctx: *mut c_void, delta: c_int)>,
    pub budget_take: Option<unsafe extern "C" fn(ctx: *mut c_void, bytes: usize) -> c_int>,
    pub budget_give: Option<unsafe extern "C" fn(ctx: *mut c_void, bytes: usize)>,
    pub on_deferred: Option<unsafe extern "C" fn(ctx: *mut c_void)>,
    pub hook_ctx: *mut c_void,
}

impl Default for tsdb_multi_opts {
    fn default() -> Self {
        tsdb_multi_opts {
            threads: 0,
            max_rows: 0,
            max_held_bytes: 0,
            cancelled: None,
            on_wait: None,
            budget_take: None,
            budget_give: None,
            on_deferred: None,
            hook_ctx: core::ptr::null_mut(),
        }
    }
}

unsafe extern "C" {
    pub fn tsdb_query_multi(
        db: *mut tsdb,
        q: *const tsdb_multi_query,
        series: *const tsdb_sid,
        n: usize,
        mo: *const tsdb_multi_opts,
        fn_: tsdb_multi_fn,
        ctx: *mut c_void,
    ) -> c_int;
    pub fn tsdb_query_multi_collect(
        db: *mut tsdb,
        q: *const tsdb_multi_query,
        series: *const tsdb_sid,
        n: usize,
        mo: *const tsdb_multi_opts,
        out: *mut *mut tsdb_series_result,
    ) -> c_int;
    pub fn tsdb_series_results_free(r: *mut tsdb_series_result, n: usize);
}

/// For tests/layout.rs: sizes and field offsets as the C compiler sees them (layout.c).
#[doc(hidden)]
pub mod layout {
    unsafe extern "C" {
        pub fn tsdb_sys_layout(n: *mut usize) -> *const usize;
    }
}
