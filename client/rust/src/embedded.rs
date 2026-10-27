//! The database inside this process: [`Db`] calls the C engine (crate `tsdb-sys`) directly,
//! with no tsdbd and no network, and mirrors [`Client`](crate::Client) method for method.
//!
//! Design notes:
//! - A [`Db`] is `Send + Sync` and every method takes `&self`: the engine is thread safe for one
//!   handle (`tsdb.h`: "All functions are thread safe"), so threads share a `Db` (or an
//!   `Arc<Db>`) without a lock. The handle is closed when the `Db` is dropped, or by
//!   [`Db::close`], which reports the result.
//! - Errors are the TCP client's [`Error`]: an engine status is [`Error::Server`] with the
//!   same [`Status`] tsdbd would have sent, so code that matches on statuses works for both.
//! - Query callbacks run on the calling thread, inside the engine. They are called through
//!   `extern "C"` trampolines that catch a panic, stop the query and resume the panic once the
//!   engine has returned, so a panic never unwinds through C. The slices a callback gets point
//!   into the engine's buffers (zero copy) and cannot outlive the call.
//! - Every array the engine allocates (buckets, bounds) is copied and freed with `tsdb_free`.
//! - Raw queries stream chunks of decoded rows; [`Db::query_chunks`] hands them over without a
//!   copy, [`Db::query_values`] as a [`Column`] like the TCP client.

use std::any::Any;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::fmt;
use std::ops::{ControlFlow, RangeBounds};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::path::{Path, PathBuf};
use std::ptr::{self, NonNull};
use std::sync::Mutex;

use tsdb_sys as sys;

use crate::client::bounds;
use crate::error::{Error, Result, Status};
use crate::types::{
    Bucket, Group, Order, Points, QueryOpts, QueryStats, Rows, SpreadBucket, Stats, TwaBucket, TwaMethod, TwaOpts,
};
use crate::value::{Bitmap, Column, ColumnRef, Value, ValueType, Values, ValuesRef};

/// How the engine encodes float values (`TSDB_VALUES_*`); all are lossless.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ValueCodec {
    /// Scaled decimal integers where that is exact, else the smallest of the float codecs.
    #[default]
    Auto,
    /// Gorilla XOR for every granule.
    Gorilla,
    /// Chimp XOR for every granule.
    Chimp,
}

/// Options of [`Db::open`] (`tsdb_options`). `Options::default()` holds the engine's defaults;
/// change fields with struct update syntax:
///
/// ```
/// let opts = tsdb_client::Options { fsync: false, merge_threads: 1, ..Default::default() };
/// # let _ = opts;
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Options {
    /// Width of a time partition in ms; fixed when the database is created. Default 1 day.
    pub partition_ms: i64,
    /// Rows per granule; fixed when the database is created. Default 8192.
    pub granule_rows: u32,
    /// zstd level of the blocks; 0 disables zstd. Default 1.
    pub zstd_level: i32,
    /// Value encoding.
    pub value_codec: ValueCodec,
    /// fsync parts before publishing them. Default true.
    pub fsync: bool,
    /// Background merge threads; 0 disables merges. Default 2.
    pub merge_threads: i32,
    /// Threads for one query. Default: every logical CPU.
    pub query_threads: i32,
    /// Memtable rows before it is flushed into parts. Default 1 M.
    pub buffer_rows: usize,
    /// Memtable age in ms before it is flushed into parts. Default 5000.
    pub flush_interval_ms: i32,
    /// 0: every small write waits for the WAL fsync (group commit, the default); N > 0: the WAL
    /// is synced every N ms in the background; -1: no WAL, small writes live in memory until flushed.
    pub wal_sync_ms: i32,
    /// Inserts below this many rows go WAL + memtable, larger ones straight to parts. Default 65536.
    pub wal_max_batch_rows: usize,
    /// Merges never produce parts larger than this. Default 4 GiB.
    pub max_part_bytes: u64,
    /// Open without writing, merging or locking exclusively.
    pub read_only: bool,
    /// Read only, and keep following a writer of the same directory.
    pub follow: bool,
    /// How often a follower polls the writer's MANIFEST, in ms. Default 100.
    pub refresh_ms: i32,
    /// A partition without new parts for this many seconds is merged down; -1 never. Default 300.
    pub merge_idle_s: i32,
    /// Parts replaced by merges are deleted this many seconds later (for readers on NFS). Default 480.
    pub obsolete_part_lifetime_s: i32,
    /// Queries check each granule's checksum before decoding it (a mismatch is [`Error::Server`] with
    /// `TSDB_ERR_CORRUPT`). Default false: for storage that checks its data itself (ZFS, ECC memory).
    pub verify_checksums: bool,
    /// A part holding rows past their series' retention is rewritten without them once it is this many
    /// seconds old; -1 never (merges still drop them). Default 3600.
    pub retention_merge_s: i32,
    /// [`series_at`](Db::series_at) and [`series_create`](Db::series_create) add a volume for a path that is
    /// not one yet (with the checks of `tsdb_volume_add`); false: such a path is NotFound. Default true.
    pub create_volumes: bool,
    /// Threads one query (or one multi-series call) uses at most, its caller included; 0: `query_threads`.
    pub request_threads: i32,
}

impl Default for Options {
    fn default() -> Options {
        let mut o = sys::tsdb_options::default();
        unsafe { sys::tsdb_options_init(&mut o) };
        Options {
            partition_ms: o.partition_ms,
            granule_rows: o.granule_rows,
            zstd_level: o.zstd_level,
            value_codec: match o.value_codec {
                sys::TSDB_VALUES_GORILLA => ValueCodec::Gorilla,
                sys::TSDB_VALUES_CHIMP => ValueCodec::Chimp,
                _ => ValueCodec::Auto,
            },
            fsync: o.fsync != 0,
            merge_threads: o.merge_threads,
            query_threads: o.query_threads,
            buffer_rows: o.buffer_rows,
            flush_interval_ms: o.flush_interval_ms,
            wal_sync_ms: o.wal_sync_ms,
            wal_max_batch_rows: o.wal_max_batch_rows,
            max_part_bytes: o.max_part_bytes,
            read_only: o.read_only != 0,
            follow: o.follow != 0,
            refresh_ms: o.refresh_ms,
            merge_idle_s: o.merge_idle_s,
            obsolete_part_lifetime_s: o.obsolete_part_lifetime_s,
            verify_checksums: o.verify_checksums != 0,
            retention_merge_s: o.retention_merge_s,
            create_volumes: o.create_volumes != 0,
            request_threads: o.request_threads,
        }
    }
}

impl Options {
    fn to_c(&self) -> sys::tsdb_options {
        sys::tsdb_options {
            partition_ms: self.partition_ms,
            granule_rows: self.granule_rows,
            zstd_level: self.zstd_level,
            value_codec: match self.value_codec {
                ValueCodec::Auto => sys::TSDB_VALUES_AUTO,
                ValueCodec::Gorilla => sys::TSDB_VALUES_GORILLA,
                ValueCodec::Chimp => sys::TSDB_VALUES_CHIMP,
            },
            fsync: self.fsync as c_int,
            merge_threads: self.merge_threads,
            query_threads: self.query_threads,
            buffer_rows: self.buffer_rows,
            flush_interval_ms: self.flush_interval_ms,
            wal_sync_ms: self.wal_sync_ms,
            wal_max_batch_rows: self.wal_max_batch_rows,
            max_part_bytes: self.max_part_bytes,
            read_only: self.read_only as c_int,
            follow: self.follow as c_int,
            refresh_ms: self.refresh_ms,
            merge_idle_s: self.merge_idle_s,
            obsolete_part_lifetime_s: self.obsolete_part_lifetime_s,
            verify_checksums: self.verify_checksums as c_int,
            retention_merge_s: self.retention_merge_s,
            // the wall clock: CLOCK_REALTIME
            clock_ms: None,
            clock_arg: std::ptr::null_mut(),
            create_volumes: self.create_volumes as c_int,
            request_threads: self.request_threads,
        }
    }
}

mod multi;

/// One part file, as [`Db::parts`] lists them (`tsdb_part_info`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartInfo {
    /// File name.
    pub name: String,
    /// Start of its time partition (ms).
    pub partition: i64,
    /// Oldest timestamp in it.
    pub min_ts: i64,
    /// Newest timestamp in it.
    pub max_ts: i64,
    /// Rows.
    pub rows: u64,
    /// Bytes on disk.
    pub bytes: u64,
    /// Granules.
    pub granules: u64,
    /// Merge level: 0 for a part an insert wrote.
    pub level: u32,
    /// When the file was written (Unix seconds).
    pub created_s: i64,
    /// When its newest rows were inserted (Unix seconds).
    pub written_s: i64,
}

/// Borrowed values of one chunk of a raw query, as the engine holds them: no copy.
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum RawValues<'a> {
    /// float32 values.
    Float32(&'a [f32]),
    /// float64 values.
    Float64(&'a [f64]),
    /// int64 values.
    Int64(&'a [i64]),
    /// bool values as bytes: 0 false, 1 true (a null's byte is unspecified).
    Bool(&'a [u8]),
    /// State codes.
    State(&'a [u32]),
}

/// One chunk of a raw query of any type ([`Db::query_chunks`]), borrowed from the engine.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ChunkRef<'a> {
    /// The values; a null's value is unspecified.
    pub values: RawValues<'a>,
    /// Validity bitmap, bit i (LSB first) set when value i is known; `None`: all are known.
    pub valid: Option<&'a [u8]>,
    len: usize,
}

impl ChunkRef<'_> {
    /// Number of rows.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the chunk has no rows.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The value type.
    pub fn value_type(&self) -> ValueType {
        match self.values {
            RawValues::Float32(_) => ValueType::Float32,
            RawValues::Float64(_) => ValueType::Float64,
            RawValues::Int64(_) => ValueType::Int64,
            RawValues::Bool(_) => ValueType::Bool,
            RawValues::State(_) => ValueType::State,
        }
    }

    /// Whether row `i` is null.
    pub fn is_null(&self, i: usize) -> bool {
        assert!(i < self.len, "row {i} of {}", self.len);
        self.valid.is_some_and(|v| v[i >> 3] >> (i & 7) & 1 == 0)
    }

    /// Whether any row is null.
    pub fn has_nulls(&self) -> bool {
        let Some(v) = self.valid else { return false };
        let full = self.len / 8;
        v[..full].iter().any(|&b| b != 0xff) || (self.len & 7 != 0 && v[full] | !((1u8 << (self.len & 7)) - 1) != 0xff)
    }

    /// Appends the values to `col` (which must have the chunk's type): validity only if needed.
    fn append_to(&self, col: &mut Column) {
        let before = col.len();
        match (&mut col.values, self.values) {
            (Values::Float32(d), RawValues::Float32(s)) => d.extend_from_slice(s),
            (Values::Float64(d), RawValues::Float64(s)) => d.extend_from_slice(s),
            (Values::Int64(d), RawValues::Int64(s)) => d.extend_from_slice(s),
            (Values::Bool(d), RawValues::Bool(s)) => d.extend(s.iter().map(|&b| b != 0)),
            (Values::State(d), RawValues::State(s)) => d.extend_from_slice(s),
            _ => unreachable!("chunk of another type"),
        }
        match (self.has_nulls().then_some(self.valid).flatten(), &mut col.valid) {
            (Some(bits), valid) => {
                valid.get_or_insert_with(|| Bitmap::all_valid(before)).extend_from_wire(bits, self.len)
            }
            (None, Some(valid)) => valid.extend_valid(self.len),
            (None, None) => {}
        }
    }

    /// Copies the chunk into an owned [`Column`] (with a bitmap only if there are nulls).
    pub fn to_column(&self) -> Column {
        let mut col = Column::empty(self.value_type());
        self.append_to(&mut col);
        col
    }
}

impl<'a> ChunkRef<'a> {
    /// # Safety
    /// `v` describes `n` rows that stay valid for `'a`.
    unsafe fn from_c(v: &sys::tsdb_values, n: usize) -> Option<ChunkRef<'a>> {
        unsafe {
            let values = match v.type_ {
                sys::TSDB_FLOAT32 => RawValues::Float32(slice(v.values.cast(), n)),
                sys::TSDB_FLOAT64 => RawValues::Float64(slice(v.values.cast(), n)),
                sys::TSDB_INT64 => RawValues::Int64(slice(v.values.cast(), n)),
                sys::TSDB_BOOL => RawValues::Bool(slice(v.values.cast(), n)),
                sys::TSDB_STATE => RawValues::State(slice(v.values.cast(), n)),
                _ => return None,
            };
            let valid = (!v.valid.is_null()).then(|| slice(v.valid, n.div_ceil(8)));
            Some(ChunkRef { values, valid, len: n })
        }
    }
}

/// An open database in this process: the embedded counterpart of [`Client`](crate::Client).
///
/// ```no_run
/// use tsdb_client::{Db, Group, Options, QueryOpts, Unit};
///
/// let db = Db::open("/tmp/plant", &Options::default())?;
/// let id = db.series("tank.level_bar")?;
/// db.insert_series(id, &[1_700_000_000_000, 1_700_000_060_000], &[4.2, 4.3])?;
/// for (ts, v) in db.query_points(id, .., QueryOpts::desc())?.iter() {
///     println!("{ts} {v}");
/// }
/// let days = Group::new(Unit::Day).timezone("Europe/Oslo");
/// for b in db.query_group(id, .., Some(&days), QueryOpts::asc())? {
///     println!("{} {} {}", b.start, b.count, b.avg());
/// }
/// db.close()?;
/// # Ok::<(), tsdb_client::Error>(())
/// ```
///
/// One process may open a directory once: a second [`Db::open`] of it (here or in tsdbd) fails
/// with [`Status::Locked`], unless it is [`Options::read_only`].
pub struct Db {
    db: NonNull<sys::tsdb>,
    path: PathBuf,
    /// The engine keeps the last query's statistics per thread, for whatever handle ran it; kept
    /// here after each query they belong to this Db, as they belong to a connection in the
    /// TCP client.
    last: Mutex<sys::tsdb_query_stats>,
}

// The engine is thread safe for one handle (tsdb.h), and the handle is not tied to a thread.
unsafe impl Send for Db {}
unsafe impl Sync for Db {}

impl fmt::Debug for Db {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Db").field("path", &self.path).finish()
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        unsafe { sys::tsdb_close(self.db.as_ptr()) };
    }
}

fn check(rc: c_int) -> Result<()> {
    if rc < 0 { Err(Error::Server(Status::from_code(rc))) } else { Ok(()) }
}

/// A slice from a C pointer that may be null when `n` is 0.
///
/// # Safety
/// `p` points at `n` values valid for `'a`.
unsafe fn slice<'a, T>(p: *const T, n: usize) -> &'a [T] {
    if n == 0 { &[] } else { unsafe { std::slice::from_raw_parts(p, n) } }
}

fn c_string(what: &str, s: &str) -> Result<CString> {
    if s.is_empty() {
        return Err(Error::InvalidArgument(format!("{what} is empty")));
    }
    CString::new(s).map_err(|_| Error::InvalidArgument(format!("{what} contains a NUL byte")))
}

/// A string the engine keeps until close, or `NotFound` for NULL.
fn c_str(p: *const c_char) -> Result<String> {
    if p.is_null() {
        return Err(Error::Server(Status::NotFound));
    }
    Ok(unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned())
}

fn check_lengths(series: Option<&[u32]>, ts: &[i64], values: usize) -> Result<()> {
    if ts.len() != values || series.is_some_and(|s| s.len() != ts.len()) {
        return Err(Error::InvalidArgument(format!(
            "column lengths differ: {} timestamps, {values} values{}",
            ts.len(),
            series.map_or(String::new(), |s| format!(", {} series", s.len()))
        )));
    }
    Ok(())
}

/// The C options of a query, or `None` for a limit of 0, which asks for nothing (in C, 0 means
/// no limit; the Rust API follows the TCP client).
fn c_opts(o: QueryOpts) -> Option<sys::tsdb_query_opts> {
    let limit = match o.limit {
        Some(0) => return None,
        Some(n) => usize::try_from(n).unwrap_or(usize::MAX),
        None => 0,
    };
    Some(sys::tsdb_query_opts { order: (o.order == Order::Desc) as c_int, limit })
}

fn bucket(b: &sys::tsdb_bucket) -> Bucket {
    Bucket {
        start: b.start,
        count: b.count,
        sum: b.sum,
        min: b.min,
        max: b.max,
        isum: b.isum,
        imin: b.imin,
        imax: b.imax,
    }
}

/// Runs an engine call that returns a malloc'd array, converts its elements and frees it.
fn engine_array<T, U>(f: impl FnOnce(*mut *mut T, *mut usize) -> c_int, conv: impl Fn(&T) -> U) -> Result<Vec<U>> {
    let (mut p, mut n) = (ptr::null_mut(), 0usize);
    let rc = f(&mut p, &mut n);
    let out = if p.is_null() { Vec::new() } else { unsafe { slice(p, n) }.iter().map(conv).collect() };
    unsafe { sys::tsdb_free(p.cast()) };
    check(rc)?;
    Ok(out)
}

fn spread_bucket(b: &sys::tsdb_spread_bucket) -> SpreadBucket {
    SpreadBucket {
        bucket: bucket(&b.b),
        range: b.range,
        var_pop: b.var_pop,
        var_samp: b.var_samp,
        stddev_pop: b.stddev_pop,
        stddev_samp: b.stddev_samp,
    }
}

fn twa_bucket(b: &sys::tsdb_twa_bucket) -> TwaBucket {
    TwaBucket { start: b.start, twa: b.twa, area: b.area, covered_ms: b.covered_ms, count: b.count }
}

/// The C options of a TWA query, or `None` for a limit of 0.
fn c_twa_opts(o: TwaOpts) -> Option<sys::tsdb_twa_opts> {
    let q = c_opts(o.query_opts())?;
    Some(sys::tsdb_twa_opts {
        method: match o.method {
            TwaMethod::Step => sys::TSDB_TWA_STEP,
            TwaMethod::Linear => sys::TSDB_TWA_LINEAR,
        },
        max_gap_ms: o.max_gap_ms,
        order: q.order,
        limit: q.limit,
        no_stats: 0,
    })
}

/// A [`Group`] as the engine takes it; `g.timezone` points into `_zone`, which lives as long.
struct CGroup {
    _zone: Option<CString>,
    g: Option<sys::tsdb_group>,
    /// In a time zone other than UTC: the `_local` functions of tsdb_calendar.h.
    local: bool,
}

impl CGroup {
    fn new(group: Option<&Group>) -> Result<CGroup> {
        let zone = match group.and_then(|g| g.timezone.as_deref()) {
            Some(z) => {
                Some(CString::new(z).map_err(|_| Error::InvalidArgument("time zone contains a NUL byte".into()))?)
            }
            None => None,
        };
        let g = group.map(|g| sys::tsdb_group {
            unit: g.unit as u8 as c_int,
            count: g.count,
            timezone: zone.as_ref().map_or(ptr::null(), |z| z.as_ptr()),
        });
        let local = zone.as_ref().is_some_and(|z| z.as_bytes() != b"UTC");
        if let Some(z) = zone.as_ref().filter(|_| local) {
            // the _local functions read the zone only when there are rows; over TCP an unknown
            // zone is an error whatever the series holds and whatever the limit
            check_zone(z)?;
        }
        Ok(CGroup { _zone: zone, g, local })
    }

    fn ptr(&self) -> *const sys::tsdb_group {
        self.g.as_ref().map_or(ptr::null(), |g| g as *const sys::tsdb_group)
    }
}

/// Bucket bounds as the TCP client checks them.
fn check_bounds(bounds: &[i64]) -> Result<()> {
    if bounds.is_empty() {
        return Err(Error::InvalidArgument("0 bucket bounds".into()));
    }
    if let Some(i) = bounds.windows(2).position(|w| w[0] >= w[1]) {
        return Err(Error::InvalidArgument(format!("bucket bounds do not increase at index {}", i + 1)));
    }
    Ok(())
}

/// Runs an engine call that returns malloc'd buckets, copies them and frees the array.
fn buckets(f: impl FnOnce(*mut *mut sys::tsdb_bucket, *mut usize) -> c_int) -> Result<Vec<Bucket>> {
    let (mut p, mut n) = (ptr::null_mut(), 0usize);
    let rc = f(&mut p, &mut n);
    let out = if p.is_null() { Vec::new() } else { unsafe { slice(p, n) }.iter().map(bucket).collect() };
    unsafe { sys::tsdb_free(p.cast()) };
    check(rc)?;
    Ok(out)
}

/// [`Status::Invalid`] for a time zone the calendar code does not know.
fn check_zone(zone: &CStr) -> Result<()> {
    let (mut p, mut n) = (ptr::null_mut(), 0usize);
    let rc = unsafe { sys::tsdb_calendar_bounds(zone.as_ptr(), sys::TSDB_DAY, 1, 0, 1, &mut p, &mut n) };
    unsafe { sys::tsdb_free(p.cast()) };
    check(rc)
}

/// The state of one query callback: the Rust closure, and what happened to it.
struct Call<F> {
    f: F,
    panic: Option<Box<dyn Any + Send>>,
    broke: bool,
}

impl<F> Call<F> {
    fn new(f: F) -> Self {
        Call { f, panic: None, broke: false }
    }

    /// Runs the closure; a panic is kept for later and stops the query, like a break.
    fn run(&mut self, body: impl FnOnce(&mut F) -> ControlFlow<()>) -> c_int {
        match catch_unwind(AssertUnwindSafe(|| body(&mut self.f))) {
            Ok(ControlFlow::Continue(())) => 0,
            Ok(ControlFlow::Break(())) => {
                self.broke = true;
                1
            }
            Err(p) => {
                self.panic = Some(p);
                1
            }
        }
    }

    /// The query's result, back in Rust: the closure's panic resumes here.
    fn finish(self, rc: c_int) -> Result<()> {
        if let Some(p) = self.panic {
            resume_unwind(p);
        }
        if rc == sys::TSDB_ERR_STOPPED && self.broke {
            return Ok(());
        }
        check(rc)
    }
}

unsafe extern "C" fn raw_fn<F>(ctx: *mut c_void, ts: *const i64, values: *const f32, n: usize) -> c_int
where
    F: FnMut(&[i64], &[Value]) -> ControlFlow<()>,
{
    let call = unsafe { &mut *ctx.cast::<Call<F>>() };
    let (ts, values) = unsafe { (slice(ts, n), slice(values, n)) };
    call.run(|f| f(ts, values))
}

unsafe extern "C" fn values_fn<F>(ctx: *mut c_void, ts: *const i64, values: *const sys::tsdb_values, n: usize) -> c_int
where
    F: FnMut(&[i64], ChunkRef<'_>) -> ControlFlow<()>,
{
    let call = unsafe { &mut *ctx.cast::<Call<F>>() };
    let ts = unsafe { slice(ts, n) };
    let Some(chunk) = (unsafe { ChunkRef::from_c(&*values, n) }) else {
        call.panic = Some(Box::new(format!("value type {} from the engine", unsafe { (*values).type_ })));
        return 1;
    };
    call.run(|f| f(ts, chunk))
}

unsafe extern "C" fn part_fn(ctx: *mut c_void, info: *const sys::tsdb_part_info) {
    let (out, i) = unsafe { (&mut *ctx.cast::<Vec<PartInfo>>(), &*info) };
    let name =
        if i.name.is_null() { String::new() } else { unsafe { CStr::from_ptr(i.name) }.to_string_lossy().into_owned() };
    out.push(PartInfo {
        name,
        partition: i.partition,
        min_ts: i.min_ts,
        max_ts: i.max_ts,
        rows: i.rows,
        bytes: i.bytes,
        granules: i.granules,
        level: i.level,
        created_s: i.created_s,
        written_s: i.written_s,
    });
}

impl Db {
    /// Opens (or creates) the database in directory `path`.
    ///
    /// # Errors
    ///
    /// [`Status::Locked`] if another process or `Db` has it open for writing,
    /// [`Status::Io`] or [`Status::Corrupt`] if it cannot be read.
    pub fn open(path: impl AsRef<Path>, opts: &Options) -> Result<Db> {
        use std::os::unix::ffi::OsStrExt;
        let path = path.as_ref().to_path_buf();
        let c = CString::new(path.as_os_str().as_bytes())
            .map_err(|_| Error::InvalidArgument("path contains a NUL byte".into()))?;
        let mut db = ptr::null_mut();
        check(unsafe { sys::tsdb_open(c.as_ptr(), &opts.to_c(), &mut db) })?;
        let db = NonNull::new(db).ok_or(Error::Server(Status::Io))?;
        Ok(Db { db, path, last: Mutex::default() })
    }

    /// Closes the database: commits, flushes the memtable and waits for the background work.
    ///
    /// Dropping a `Db` does the same but cannot report an error.
    pub fn close(self) -> Result<()> {
        let db = std::mem::ManuallyDrop::new(self);
        let rc = unsafe { sys::tsdb_close(db.db.as_ptr()) };
        // the path is the only other field; ManuallyDrop would leak it
        drop(unsafe { ptr::read(&db.path) });
        check(rc)
    }

    /// The directory it was opened with.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The C handle, for calls into `tsdb-sys` that this API lacks. It stays valid while the
    /// `Db` lives; do not close it.
    pub fn as_ptr(&self) -> *mut sys::tsdb {
        self.db.as_ptr()
    }

    fn p(&self) -> *mut sys::tsdb {
        self.db.as_ptr()
    }

    /// Name of the SIMD kernels the engine picked for this CPU ("avx512", "avx2" or "scalar"),
    /// followed by the CPU level of the codecs in a portable build.
    pub fn simd_level() -> &'static str {
        unsafe { CStr::from_ptr(sys::tsdb_simd_level()) }.to_str().unwrap_or("?")
    }

    // ------------------------------------------------------------- series

    /// The id of float32 series `name`, created if it does not exist.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] for an empty name or one with a NUL byte.
    pub fn series(&self, name: &str) -> Result<u32> {
        let name = c_string("series name", name)?;
        let mut id = 0;
        check(unsafe { sys::tsdb_series(self.p(), name.as_ptr(), &mut id) })?;
        Ok(id)
    }

    /// The id of an existing series; [`Status::NotFound`] if there is none.
    pub fn series_lookup(&self, name: &str) -> Result<u32> {
        let name = c_string("series name", name)?;
        let mut id = 0;
        check(unsafe { sys::tsdb_series_lookup(self.p(), name.as_ptr(), &mut id) })?;
        Ok(id)
    }

    /// Like [`series`](Db::series), creating the series in volume `path` (a directory).
    pub fn series_at(&self, name: &str, path: &str) -> Result<u32> {
        self.series_create(name, ValueType::Float32, Some(path))
    }

    /// The id of series `name` of type `ty`, created (in volume `path`, or the database
    /// directory) if it does not exist. An existing series keeps its type: asking for another
    /// is [`Status::Invalid`].
    pub fn series_create(&self, name: &str, ty: ValueType, path: Option<&str>) -> Result<u32> {
        let name = c_string("series name", name)?;
        let path = path.map(|p| c_string("volume path", p)).transpose()?;
        let mut id = 0;
        check(unsafe {
            sys::tsdb_series_create(
                self.p(),
                name.as_ptr(),
                ty.code() as c_int,
                path.as_ref().map_or(ptr::null(), |p| p.as_ptr()),
                &mut id,
            )
        })?;
        Ok(id)
    }

    /// The value type of series `id`; [`Status::NotFound`] if there is none.
    pub fn series_type(&self, id: u32) -> Result<ValueType> {
        let mut ty = 0;
        check(unsafe { sys::tsdb_series_type(self.p(), id, &mut ty) })?;
        u8::try_from(ty).ok().and_then(ValueType::from_code).ok_or(Error::Server(Status::Corrupt))
    }

    /// The name of series `id`; [`Status::NotFound`] if there is none.
    pub fn series_name(&self, id: u32) -> Result<String> {
        c_str(unsafe { sys::tsdb_series_name(self.p(), id) })
    }

    /// The volume (directory) series `id` is stored in; [`Status::NotFound`] if there is none.
    pub fn series_path(&self, id: u32) -> Result<String> {
        c_str(unsafe { sys::tsdb_series_path(self.p(), id) })
    }

    /// Number of series.
    pub fn series_count(&self) -> u32 {
        unsafe { sys::tsdb_series_count(self.p()) }
    }

    /// The directories of the volumes: the database directory first.
    pub fn volumes(&self) -> Vec<String> {
        let n = unsafe { sys::tsdb_volume_count(self.p()) };
        (0..n).filter_map(|i| c_str(unsafe { sys::tsdb_volume_path(self.p(), i) }).ok()).collect()
    }

    /// The code of state `name` for state series `series`, created if needed.
    pub fn state(&self, series: u32, name: &str) -> Result<u32> {
        let name = c_string("state name", name)?;
        let mut code = 0;
        check(unsafe { sys::tsdb_state(self.p(), series, name.as_ptr(), &mut code) })?;
        Ok(code)
    }

    /// The code of an existing state; [`Status::NotFound`] if there is none.
    pub fn state_lookup(&self, series: u32, name: &str) -> Result<u32> {
        let name = c_string("state name", name)?;
        let mut code = 0;
        check(unsafe { sys::tsdb_state_lookup(self.p(), series, name.as_ptr(), &mut code) })?;
        Ok(code)
    }

    /// The name of state `code`; [`Status::NotFound`] for an unknown code.
    pub fn state_name(&self, series: u32, code: u32) -> Result<String> {
        c_str(unsafe { sys::tsdb_state_name(self.p(), series, code) })
    }

    // ------------------------------------------------------------- writing

    /// Inserts rows into float32 series `series`. Returns once they are durable (per
    /// [`Options::fsync`] and [`Options::wal_sync_ms`]) and visible; all or nothing.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] if the slices differ in length; [`Status::Invalid`] if the
    /// series is not float32.
    pub fn insert_series(&self, series: u32, ts: &[i64], values: &[Value]) -> Result<()> {
        check_lengths(None, ts, values.len())?;
        check(unsafe { sys::tsdb_insert_series(self.p(), series, ts.as_ptr(), values.as_ptr(), ts.len()) })
    }

    /// Inserts rows of several float32 series: row i goes to `series[i]`.
    pub fn insert(&self, series: &[u32], ts: &[i64], values: &[Value]) -> Result<()> {
        check_lengths(Some(series), ts, values.len())?;
        check(unsafe { sys::tsdb_insert(self.p(), series.as_ptr(), ts.as_ptr(), values.as_ptr(), ts.len()) })
    }

    /// Inserts typed values, with nulls, into `series`: a slice of the series' native type
    /// (`&[f64]`, `&[i64]`, ...), a [`Column`] or a [`ColumnRef`]. Nothing is copied.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] if the lengths differ; [`Status::Invalid`] if the series has
    /// another type.
    pub fn insert_series_values<'a>(&self, series: u32, ts: &[i64], values: impl Into<ColumnRef<'a>>) -> Result<()> {
        let col = values.into();
        let c = c_values(&col, ts)?;
        check(unsafe { sys::tsdb_insert_series_values(self.p(), series, ts.as_ptr(), &c, ts.len()) })
    }

    /// Inserts typed values into several series of the column's type: row i goes to `series[i]`.
    pub fn insert_values<'a>(&self, series: &[u32], ts: &[i64], values: impl Into<ColumnRef<'a>>) -> Result<()> {
        let col = values.into();
        check_lengths(Some(series), ts, col.len())?;
        let c = c_values(&col, ts)?;
        check(unsafe { sys::tsdb_insert_values(self.p(), series.as_ptr(), ts.as_ptr(), &c, ts.len()) })
    }

    /// One float32 point through the WAL and memtable: visible at once, durable per
    /// [`Options::wal_sync_ms`].
    pub fn append(&self, series: u32, ts: i64, value: Value) -> Result<()> {
        check(unsafe { sys::tsdb_append(self.p(), series, ts, value) })
    }

    // ------------------------------------------------------------- reading

    /// The rows of float32 `series` in `range`, in `opts` order (nulls read as NaN).
    ///
    /// # Errors
    ///
    /// [`Status::Invalid`] if the series is not float32 (use [`query_rows`](Db::query_rows)).
    pub fn query_points(&self, series: u32, range: impl RangeBounds<i64>, opts: QueryOpts) -> Result<Points> {
        let mut p = Points::default();
        self.query_raw(series, range, opts, |ts, v| {
            p.ts.extend_from_slice(ts);
            p.values.extend_from_slice(v);
            ControlFlow::Continue(())
        })?;
        Ok(p)
    }

    /// Streams the rows of float32 `series` in `range` to `f`, a chunk at a time, straight from
    /// the engine's buffers. Return [`ControlFlow::Break`] to stop early. A panic in `f` stops
    /// the query and continues once the engine has returned.
    pub fn query_raw<F>(&self, series: u32, range: impl RangeBounds<i64>, opts: QueryOpts, f: F) -> Result<()>
    where
        F: FnMut(&[i64], &[Value]) -> ControlFlow<()>,
    {
        let (start, end) = bounds(&range);
        let Some(o) = c_opts(opts) else { return Ok(()) };
        let mut call = Call::new(f);
        let rc = unsafe {
            sys::tsdb_query_raw_ex(self.p(), series, start, end, &o, Some(raw_fn::<F>), (&raw mut call).cast())
        };
        self.record();
        call.finish(rc)
    }

    /// The rows of `series` (any type, with nulls) in `range`.
    pub fn query_rows(&self, series: u32, range: impl RangeBounds<i64>, opts: QueryOpts) -> Result<Rows> {
        let mut rows: Option<Rows> = None;
        self.query_chunks(series, range, opts, |ts, chunk| {
            let r = rows.get_or_insert_with(|| Rows { ts: Vec::new(), values: Column::empty(chunk.value_type()) });
            r.ts.extend_from_slice(ts);
            chunk.append_to(&mut r.values);
            ControlFlow::Continue(())
        })?;
        match rows {
            Some(r) => Ok(r),
            // an unknown series has no rows, typed float32 as tsdbd answers it
            None => match self.series_type(series) {
                Err(e) if e.status() == Some(Status::NotFound) => {
                    Ok(Rows { ts: Vec::new(), values: Column::empty(ValueType::Float32) })
                }
                t => Ok(Rows { ts: Vec::new(), values: Column::empty(t?) }),
            },
        }
    }

    /// Streams the rows of `series` (any type) to `f` as a [`Column`] per chunk, like
    /// [`Client::query_values`](crate::Client::query_values); the column is reused between
    /// chunks and has a validity bitmap only if the chunk has nulls. [`query_chunks`](Db::query_chunks)
    /// avoids the copy into the column.
    pub fn query_values<F>(&self, series: u32, range: impl RangeBounds<i64>, opts: QueryOpts, mut f: F) -> Result<()>
    where
        F: FnMut(&[i64], &Column) -> ControlFlow<()>,
    {
        let mut col = Column::empty(ValueType::Float32);
        self.query_chunks(series, range, opts, |ts, chunk| {
            if chunk.is_empty() {
                return ControlFlow::Continue(());
            }
            if col.value_type() == chunk.value_type() {
                col.clear();
            } else {
                col = Column::empty(chunk.value_type());
            }
            chunk.append_to(&mut col);
            f(ts, &col)
        })
    }

    /// Streams the rows of `series` (any type, with nulls) to `f` as they lie in the engine's
    /// buffers: no copy. The fastest way to read; the borrowed chunk cannot outlive the call.
    pub fn query_chunks<F>(&self, series: u32, range: impl RangeBounds<i64>, opts: QueryOpts, f: F) -> Result<()>
    where
        F: FnMut(&[i64], ChunkRef<'_>) -> ControlFlow<()>,
    {
        let (start, end) = bounds(&range);
        let Some(o) = c_opts(opts) else { return Ok(()) };
        let mut call = Call::new(f);
        let rc = unsafe {
            sys::tsdb_query_values(self.p(), series, start, end, &o, Some(values_fn::<F>), (&raw mut call).cast())
        };
        self.record();
        call.finish(rc)
    }

    /// Aggregates `series` over `range` into buckets of `interval_ms`, aligned to the epoch;
    /// 0 gives one bucket. Only non-empty buckets, oldest first.
    pub fn query_agg(&self, series: u32, range: impl RangeBounds<i64>, interval_ms: i64) -> Result<Vec<Bucket>> {
        let (start, end) = bounds(&range);
        let r = buckets(|out, n| unsafe { sys::tsdb_query_agg(self.p(), series, start, end, interval_ms, out, n) });
        self.record();
        r
    }

    /// Aggregates `series` over `range` in calendar buckets: `group` units, in its time zone
    /// (UTC when none; local zones are turned into bucket bounds in-process from the system's
    /// zoneinfo, as the TCP client does), or one bucket for the range when `group` is `None`.
    ///
    /// # Errors
    ///
    /// [`Status::Invalid`] for an unknown time zone, [`Status::TooManyBuckets`] if the range
    /// holds too many buckets.
    pub fn query_group(
        &self,
        series: u32,
        range: impl RangeBounds<i64>,
        group: Option<&Group>,
        opts: QueryOpts,
    ) -> Result<Vec<Bucket>> {
        let (start, end) = bounds(&range);
        let cg = CGroup::new(group)?;
        let (gp, local) = (cg.ptr(), cg.local);
        let Some(o) = c_opts(opts) else {
            return Ok(Vec::new());
        };
        let r = buckets(|out, n| unsafe {
            if local {
                sys::tsdb_query_group_local(self.p(), series, start, end, gp, &o, out, n)
            } else {
                sys::tsdb_query_group_ex(self.p(), series, start, end, gp, &o, out, n)
            }
        });
        self.record();
        r
    }

    /// Aggregates `series` over `range` into buckets that begin at `bounds` (UTC ms, strictly
    /// increasing): bucket i covers `[bounds[i], bounds[i + 1])`, the last one everything from
    /// the last bound on.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] if `bounds` is empty or does not strictly increase.
    pub fn query_bounds(
        &self,
        series: u32,
        range: impl RangeBounds<i64>,
        bounds: &[i64],
        opts: QueryOpts,
    ) -> Result<Vec<Bucket>> {
        let (start, end) = self::bounds(&range);
        check_bounds(bounds)?;
        let Some(o) = c_opts(opts) else { return Ok(Vec::new()) };
        let r = buckets(|out, n| unsafe {
            sys::tsdb_query_bounds(self.p(), series, start, end, bounds.as_ptr(), bounds.len(), &o, out, n)
        });
        self.record();
        r
    }

    /// As [`query_agg`](Db::query_agg), in `opts.order` and at most `opts.limit` buckets.
    pub fn query_agg_ex(
        &self,
        series: u32,
        range: impl RangeBounds<i64>,
        interval_ms: i64,
        opts: QueryOpts,
    ) -> Result<Vec<Bucket>> {
        let (start, end) = bounds(&range);
        if interval_ms < 0 {
            return Err(Error::Server(Status::Invalid));
        }
        let Some(o) = c_opts(opts) else { return Ok(Vec::new()) };
        let r =
            buckets(|out, n| unsafe { sys::tsdb_query_agg_ex(self.p(), series, start, end, interval_ms, &o, out, n) });
        self.record();
        r
    }

    /// The spread (range, variances, standard deviations) in buckets of `interval_ms`, as
    /// [`Client::query_spread`](crate::Client::query_spread).
    pub fn query_spread(
        &self,
        series: u32,
        range: impl RangeBounds<i64>,
        interval_ms: i64,
        opts: QueryOpts,
    ) -> Result<Vec<SpreadBucket>> {
        let (start, end) = bounds(&range);
        if interval_ms < 0 {
            return Err(Error::Server(Status::Invalid));
        }
        let Some(o) = c_opts(opts) else { return Ok(Vec::new()) };
        let r = engine_array(
            |out, n| unsafe { sys::tsdb_query_spread(self.p(), series, start, end, interval_ms, &o, out, n) },
            spread_bucket,
        );
        self.record();
        r
    }

    /// The spread by calendar periods, as [`query_group`](Db::query_group).
    pub fn query_spread_group(
        &self,
        series: u32,
        range: impl RangeBounds<i64>,
        group: Option<&Group>,
        opts: QueryOpts,
    ) -> Result<Vec<SpreadBucket>> {
        let (start, end) = bounds(&range);
        let cg = CGroup::new(group)?;
        let Some(o) = c_opts(opts) else { return Ok(Vec::new()) };
        let r = engine_array(
            |out, n| unsafe {
                if cg.local {
                    sys::tsdb_query_spread_group_local(self.p(), series, start, end, cg.ptr(), &o, out, n)
                } else {
                    sys::tsdb_query_spread_group(self.p(), series, start, end, cg.ptr(), &o, out, n)
                }
            },
            spread_bucket,
        );
        self.record();
        r
    }

    /// The spread in buckets with the given bounds, as [`query_bounds`](Db::query_bounds).
    pub fn query_spread_bounds(
        &self,
        series: u32,
        range: impl RangeBounds<i64>,
        bounds: &[i64],
        opts: QueryOpts,
    ) -> Result<Vec<SpreadBucket>> {
        let (start, end) = self::bounds(&range);
        check_bounds(bounds)?;
        let Some(o) = c_opts(opts) else { return Ok(Vec::new()) };
        let r = engine_array(
            |out, n| unsafe {
                sys::tsdb_query_spread_bounds(self.p(), series, start, end, bounds.as_ptr(), bounds.len(), &o, out, n)
            },
            spread_bucket,
        );
        self.record();
        r
    }

    /// Time-weighted averages in buckets of `interval_ms` (0: one bucket), as
    /// [`Client::query_twa`](crate::Client::query_twa).
    pub fn query_twa(
        &self,
        series: u32,
        range: impl RangeBounds<i64>,
        interval_ms: i64,
        opts: TwaOpts,
    ) -> Result<Vec<TwaBucket>> {
        let (start, end) = bounds(&range);
        if interval_ms < 0 {
            return Err(Error::Server(Status::Invalid));
        }
        let Some(o) = c_twa_opts(opts) else { return Ok(Vec::new()) };
        let r = engine_array(
            |out, n| unsafe { sys::tsdb_query_twa(self.p(), series, start, end, interval_ms, &o, out, n) },
            twa_bucket,
        );
        self.record();
        r
    }

    /// Time-weighted averages by calendar periods (`None`: one bucket), in any time zone.
    pub fn query_twa_group(
        &self,
        series: u32,
        range: impl RangeBounds<i64>,
        group: Option<&Group>,
        opts: TwaOpts,
    ) -> Result<Vec<TwaBucket>> {
        let (start, end) = bounds(&range);
        let cg = CGroup::new(group)?;
        let Some(o) = c_twa_opts(opts) else { return Ok(Vec::new()) };
        let r = engine_array(
            |out, n| unsafe {
                if cg.local {
                    sys::tsdb_query_twa_group_local(self.p(), series, start, end, cg.ptr(), &o, out, n)
                } else {
                    sys::tsdb_query_twa_group(self.p(), series, start, end, cg.ptr(), &o, out, n)
                }
            },
            twa_bucket,
        );
        self.record();
        r
    }

    /// Time-weighted averages in buckets with the given bounds.
    pub fn query_twa_bounds(
        &self,
        series: u32,
        range: impl RangeBounds<i64>,
        bounds: &[i64],
        opts: TwaOpts,
    ) -> Result<Vec<TwaBucket>> {
        let (start, end) = self::bounds(&range);
        check_bounds(bounds)?;
        let Some(o) = c_twa_opts(opts) else { return Ok(Vec::new()) };
        let r = engine_array(
            |out, n| unsafe {
                sys::tsdb_query_twa_bounds(self.p(), series, start, end, bounds.as_ptr(), bounds.len(), &o, out, n)
            },
            twa_bucket,
        );
        self.record();
        r
    }

    /// LTTB downsampling: at most `threshold` points that keep the shape of the line, newest
    /// first or cut to a limit with `opts`, as [`Client::query_lttb`](crate::Client::query_lttb).
    pub fn query_lttb(
        &self,
        series: u32,
        range: impl RangeBounds<i64>,
        threshold: u64,
        opts: QueryOpts,
    ) -> Result<Rows> {
        let (start, end) = bounds(&range);
        // a limit of 0 still asks (for one point, dropped) so that errors match tsdbd's
        let none = opts.limit == Some(0);
        let o = c_opts(if none { QueryOpts { limit: Some(1), ..opts } } else { opts }).expect("a limit above 0");
        let threshold = usize::try_from(threshold).unwrap_or(usize::MAX);
        let mut rows: Option<Rows> = None;
        self.lttb_chunks(series, start, end, threshold, &o, |ts, chunk| {
            let r = rows.get_or_insert_with(|| Rows { ts: Vec::new(), values: Column::empty(chunk.value_type()) });
            if !none {
                r.ts.extend_from_slice(ts);
                chunk.append_to(&mut r.values);
            }
            ControlFlow::Continue(())
        })?;
        match rows {
            Some(r) => Ok(r),
            None => match self.series_type(series) {
                Err(e) if e.status() == Some(Status::NotFound) => {
                    Ok(Rows { ts: Vec::new(), values: Column::empty(ValueType::Float32) })
                }
                t => Ok(Rows { ts: Vec::new(), values: Column::empty(t?) }),
            },
        }
    }

    fn lttb_chunks<F>(
        &self,
        series: u32,
        start: i64,
        end: i64,
        threshold: usize,
        o: &sys::tsdb_query_opts,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(&[i64], ChunkRef<'_>) -> ControlFlow<()>,
    {
        let mut call = Call::new(f);
        let rc = unsafe {
            sys::tsdb_query_lttb_ex(
                self.p(),
                series,
                start,
                end,
                threshold,
                o,
                Some(values_fn::<F>),
                (&raw mut call).cast(),
            )
        };
        self.record();
        call.finish(rc)
    }

    // ------------------------------------------------ deletes and retention

    /// Deletes the rows of a series in `range`, as [`Client::delete_range`](crate::Client::delete_range).
    pub fn delete_range(&self, series: u32, range: impl RangeBounds<i64>) -> Result<()> {
        let (start, end) = bounds(&range);
        check(unsafe { sys::tsdb_delete_range(self.p(), series, start, end) })
    }

    /// Replaces the rows of a series in `range` by the given ones, atomically; every `ts` must lie
    /// in the range. As [`Client::replace_range`](crate::Client::replace_range).
    pub fn replace_range<'a>(
        &self,
        series: u32,
        range: impl RangeBounds<i64>,
        ts: &[i64],
        values: impl Into<ColumnRef<'a>>,
    ) -> Result<()> {
        let (start, end) = bounds(&range);
        let col = values.into();
        let c = c_values(&col, ts)?;
        check(unsafe { sys::tsdb_replace_range(self.p(), series, start, end, ts.as_ptr(), &c, ts.len()) })
    }

    /// Sets how long a series keeps its values (0: for ever), durably.
    pub fn set_retention(&self, series: u32, limit_ms: i64) -> Result<()> {
        check(unsafe { sys::tsdb_set_retention(self.p(), series, limit_ms) })
    }

    /// A series' retention limit in ms (0: none).
    pub fn retention(&self, series: u32) -> Result<i64> {
        let mut v = 0i64;
        check(unsafe { sys::tsdb_get_retention(self.p(), series, &mut v) })?;
        Ok(v)
    }

    // ------------------------------------------------------------- maintenance

    /// Makes everything written so far durable, without writing parts.
    pub fn commit(&self) -> Result<()> {
        check(unsafe { sys::tsdb_commit(self.p()) })
    }

    /// Commits, then writes the memtable into parts.
    pub fn flush(&self) -> Result<()> {
        check(unsafe { sys::tsdb_flush(self.p()) })
    }

    /// Merges every partition down to one part.
    pub fn optimize(&self) -> Result<()> {
        check(unsafe { sys::tsdb_optimize(self.p()) })
    }

    /// Drops whole partitions that end at or before `ts`.
    pub fn drop_before(&self, ts: i64) -> Result<()> {
        check(unsafe { sys::tsdb_drop_before(self.p(), ts) })
    }

    /// A read-only database: picks up what the writer published since the last refresh.
    pub fn refresh(&self) -> Result<()> {
        check(unsafe { sys::tsdb_refresh(self.p()) })
    }

    /// Storage statistics.
    pub fn stats(&self) -> Result<Stats> {
        let mut s = sys::tsdb_stats::default();
        check(unsafe { sys::tsdb_get_stats(self.p(), &mut s) })?;
        Ok(Stats {
            parts: s.parts,
            partitions: s.partitions,
            rows: s.rows,
            bytes: s.bytes,
            merges: s.merges,
            merged_rows: s.merged_rows,
            passthrough_granules: s.passthrough_granules,
            memtable_rows: s.memtable_rows,
        })
    }

    /// Takes the statistics of the query this thread just ran on this Db from the engine.
    fn record(&self) {
        let mut s = sys::tsdb_query_stats::default();
        unsafe { sys::tsdb_last_query_stats(&mut s) };
        *self.last.lock().unwrap_or_else(|e| e.into_inner()) = s;
    }

    /// What the last query on this Db did (rows covered, decoded, bytes read, time), all zero
    /// before the first. When several threads query one Db, the one that finished last. After a
    /// batch call (`query_points_many`, `query_agg_many`), the totals of its queries, with the
    /// whole batch's wall time as `elapsed_s`.
    pub fn last_query_stats(&self) -> QueryStats {
        let s = *self.last.lock().unwrap_or_else(|e| e.into_inner());
        QueryStats {
            elapsed_s: s.elapsed_s,
            rows: s.rows,
            rows_decoded: s.rows_decoded,
            rows_from_stats: s.rows_from_stats,
            memtable_rows: s.memtable_rows,
            bytes: s.bytes,
            bytes_read: s.bytes_read,
            parts: s.parts,
            granules: s.granules,
            granules_from_stats: s.granules_from_stats,
            result_rows: s.result_rows,
        }
    }

    /// `None` while healthy; after a storage error that leaves it unknown what reached the
    /// disk, what failed. Writes then fail with [`Status::Io`] until the database is reopened.
    pub fn failed(&self) -> Option<String> {
        let mut msg = [0 as c_char; 512];
        let rc = unsafe { sys::tsdb_failed(self.p(), msg.as_mut_ptr(), msg.len()) };
        (rc != 0).then(|| unsafe { CStr::from_ptr(msg.as_ptr()) }.to_string_lossy().into_owned())
    }

    /// The part files, for inspection.
    pub fn parts(&self) -> Result<Vec<PartInfo>> {
        let mut out: Vec<PartInfo> = Vec::new();
        check(unsafe { sys::tsdb_foreach_part(self.p(), Some(part_fn), (&raw mut out).cast()) })?;
        Ok(out)
    }
}

/// A column as the engine reads it, borrowing the caller's memory. Rust `bool` is a byte that
/// is 0 or 1, the engine's bool layout, so no type is copied.
fn c_values(col: &ColumnRef<'_>, ts: &[i64]) -> Result<sys::tsdb_values> {
    check_lengths(None, ts, col.len())?;
    let valid = match col.valid {
        Some(v) if v.len() != col.len() => {
            return Err(Error::InvalidArgument(format!(
                "validity bitmap of {} bits for {} values",
                v.len(),
                col.len()
            )));
        }
        Some(v) => v.as_bytes().as_ptr(),
        None => ptr::null(),
    };
    let (type_, values): (sys::tsdb_type, *const c_void) = match col.values {
        ValuesRef::Float32(v) => (sys::TSDB_FLOAT32, v.as_ptr().cast()),
        ValuesRef::Float64(v) => (sys::TSDB_FLOAT64, v.as_ptr().cast()),
        ValuesRef::Int64(v) => (sys::TSDB_INT64, v.as_ptr().cast()),
        ValuesRef::Bool(v) => (sys::TSDB_BOOL, v.as_ptr().cast()),
        ValuesRef::State(v) => (sys::TSDB_STATE, v.as_ptr().cast()),
    };
    Ok(sys::tsdb_values { type_, values, valid })
}
