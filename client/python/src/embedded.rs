//! `tsdb_client.Db`: the engine inside the Python process, over `tsdb_client::Db` (the Rust
//! crate's `embedded` feature, which compiles the C engine into this extension).
//!
//! The methods mirror `Client`, so code can switch between a tsdbd connection and a local
//! directory by changing the constructor. Arguments are converted while the GIL is held and the
//! engine runs detached from the interpreter. The engine is thread safe, so threads sharing one
//! `Db` run at the same time; the read-write lock only keeps `close()` from freeing the database
//! under a call that is still running.
//!
//! There is no network round trip to hide, so the `*_async` inserts are the plain inserts and
//! `wait()` has nothing to wait for: they exist so code written for `Client` runs unchanged.

use std::path::PathBuf;
use std::sync::{Mutex, RwLock};

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

use ::tsdb_client as tc;

use crate::arrays::{BucketsMany, Insert, agg_queries, inserts, many_cols, points_list, range_queries};
use tc::ValueType;

use crate::{
    Buckets, QueryStats, Stats, buckets, values_column, closed, column_to_py, f32_column, numpy_module, opts, points_to_py, py_column,
    range, series_column, to_pyerr, ts_column, ts_to_py, unit, value_type, zone_name,
};

fn value_codec(name: &str) -> PyResult<tc::ValueCodec> {
    match name {
        "auto" => Ok(tc::ValueCodec::Auto),
        "gorilla" => Ok(tc::ValueCodec::Gorilla),
        "chimp" => Ok(tc::ValueCodec::Chimp),
        _ => Err(PyValueError::new_err(format!("value_codec must be auto, gorilla or chimp, not {name:?}"))),
    }
}

/// A tsdb database opened in this process: no server, no network.
///
/// The methods are those of Client, so code written for one runs with the
/// other. Use it as a context manager (``with Db(path) as db:``) or call
/// close(); one process at a time may open a directory for writing.
/// The engine works with the GIL released, and threads sharing one Db
/// run in parallel.
///
/// Args:
///     path: The database directory; created if missing (its parent must exist).
///     read_only: Open without writing, merging or locking exclusively.
///     follow: Read only, and keep following a writer of the same directory.
///     fsync: fsync parts before publishing them (default True).
///     wal_sync_ms: 0: every small write waits for the WAL fsync (the
///         default); N > 0: the WAL is synced every N ms; -1: no WAL.
///     merge_threads: Background merge threads; 0 disables merges.
///     query_threads: Threads for one query (default: every CPU).
///     The other keywords are the engine's tsdb_options fields of the same
///     name; None keeps the engine's default. partition_ms and granule_rows
///     are fixed when the database is created. value_codec is "auto",
///     "gorilla" or "chimp".
///
/// Raises:
///     TsdbError: ERR_LOCKED if another process has the directory open for
///         writing; ERR_IO if it cannot be created.
///     ValueError: Unknown value_codec.
#[pyclass(module = "tsdb_client")]
pub(crate) struct Db {
    db: RwLock<Option<tc::Db>>,
    path: PathBuf,
    /// What the last query through this Db read. The engine keeps it per thread; kept here it
    /// answers like Client does per connection, whichever thread asks.
    last: Mutex<tc::QueryStats>,
}

impl Drop for Db {
    /// A Db that is collected without close(): closing flushes the memtable and waits for merges,
    /// which must not stall every other Python thread. CPython's file objects close their file
    /// descriptor the same way, with the GIL released inside dealloc.
    fn drop(&mut self) {
        if let Some(db) = self.db.get_mut().unwrap_or_else(|e| e.into_inner()).take() {
            Python::attach(|py| py.detach(move || drop(db)));
        }
    }
}

impl Db {
    /// Runs `f` on the open database with the GIL released.
    fn with<T, F>(&self, py: Python<'_>, f: F) -> PyResult<T>
    where
        T: Send,
        F: FnOnce(&tc::Db) -> tc::Result<T> + Send,
    {
        py.detach(|| f(self.db.read().unwrap_or_else(|e| e.into_inner()).as_ref().ok_or_else(closed)?))
            .map_err(|e| to_pyerr(py, e))
    }

    /// Runs the query `f` like `with`, and keeps its statistics for last_query_stats().
    fn query<T, F>(&self, py: Python<'_>, f: F) -> PyResult<T>
    where
        T: Send,
        F: FnOnce(&tc::Db) -> tc::Result<T> + Send,
    {
        self.with(py, |d| {
            let r = f(d);
            *self.last.lock().unwrap_or_else(|e| e.into_inner()) = d.last_query_stats();
            r
        })
    }

    /// Typed inserts: into `one` series, or into `many` (a column of series ids).
    fn insert_typed(
        &self,
        py: Python<'_>,
        one: Option<u32>,
        many: Option<&Bound<'_, PyAny>>,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
        value_type: Option<&str>,
    ) -> PyResult<()> {
        let ts = ts_column(ts)?;
        let many = many.map(series_column).transpose()?;
        let ty = match (value_type, one.or_else(|| many.as_ref().and_then(|s| s.first().copied()))) {
            (Some(t), _) => self::value_type(t)?,
            (None, Some(id)) => self.with(py, |d| d.series_type(id))?,
            (None, None) => ValueType::Float32,
        };
        let col = values_column(values, ty)?;
        self.with(py, |d| match (&many, one) {
            (Some(s), _) => d.insert_values(s, &ts, &col),
            (None, Some(id)) => d.insert_series_values(id, &ts, &col),
            (None, None) => unreachable!("one of the two is given"),
        })
    }
}

#[pymethods]
impl Db {
    #[new]
    #[pyo3(signature = (path, *, read_only = false, follow = false, fsync = None, wal_sync_ms = None,
                        merge_threads = None, query_threads = None, partition_ms = None, granule_rows = None,
                        zstd_level = None, value_codec = None, buffer_rows = None, flush_interval_ms = None,
                        wal_max_batch_rows = None, max_part_bytes = None, refresh_ms = None, merge_idle_s = None,
                        obsolete_part_lifetime_s = None, verify_checksums = None, retention_merge_s = None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        path: PathBuf,
        read_only: bool,
        follow: bool,
        fsync: Option<bool>,
        wal_sync_ms: Option<i32>,
        merge_threads: Option<i32>,
        query_threads: Option<i32>,
        partition_ms: Option<i64>,
        granule_rows: Option<u32>,
        zstd_level: Option<i32>,
        value_codec: Option<&str>,
        buffer_rows: Option<usize>,
        flush_interval_ms: Option<i32>,
        wal_max_batch_rows: Option<usize>,
        max_part_bytes: Option<u64>,
        refresh_ms: Option<i32>,
        merge_idle_s: Option<i32>,
        obsolete_part_lifetime_s: Option<i32>,
        verify_checksums: Option<bool>,
        retention_merge_s: Option<i32>,
    ) -> PyResult<Self> {
        let d = tc::Options::default();
        let o = tc::Options {
            read_only,
            follow,
            fsync: fsync.unwrap_or(d.fsync),
            wal_sync_ms: wal_sync_ms.unwrap_or(d.wal_sync_ms),
            merge_threads: merge_threads.unwrap_or(d.merge_threads),
            query_threads: query_threads.unwrap_or(d.query_threads),
            partition_ms: partition_ms.unwrap_or(d.partition_ms),
            granule_rows: granule_rows.unwrap_or(d.granule_rows),
            zstd_level: zstd_level.unwrap_or(d.zstd_level),
            value_codec: value_codec.map(self::value_codec).transpose()?.unwrap_or(d.value_codec),
            buffer_rows: buffer_rows.unwrap_or(d.buffer_rows),
            flush_interval_ms: flush_interval_ms.unwrap_or(d.flush_interval_ms),
            wal_max_batch_rows: wal_max_batch_rows.unwrap_or(d.wal_max_batch_rows),
            max_part_bytes: max_part_bytes.unwrap_or(d.max_part_bytes),
            refresh_ms: refresh_ms.unwrap_or(d.refresh_ms),
            merge_idle_s: merge_idle_s.unwrap_or(d.merge_idle_s),
            obsolete_part_lifetime_s: obsolete_part_lifetime_s.unwrap_or(d.obsolete_part_lifetime_s),
            verify_checksums: verify_checksums.unwrap_or(d.verify_checksums),
            retention_merge_s: retention_merge_s.unwrap_or(d.retention_merge_s),
            create_volumes: d.create_volumes,
        };
        let db = py.detach(|| tc::Db::open(&path, &o)).map_err(|e| to_pyerr(py, e))?;
        Ok(Db { db: RwLock::new(Some(db)), path, last: Mutex::default() })
    }

    /// Flushes the memtable and closes the database.
    ///
    /// Waits for calls other threads are making on it. Closing twice is
    /// fine; any later call raises TsdbError (ERR_IO).
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.db.write().unwrap_or_else(|e| e.into_inner()).take().map_or(Ok(()), tc::Db::close))
            .map_err(|e| to_pyerr(py, e))
    }

    /// Returns the database itself, to use in a with statement.
    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    /// Closes the database at the end of a with block.
    #[pyo3(signature = (_exc_type, _exc, _tb))]
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc: Option<&Bound<'_, PyAny>>,
        _tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.close(py)?;
        Ok(false)
    }

    fn __repr__(&self) -> String {
        let open = self.db.read().map(|g| g.is_some()).unwrap_or(false);
        format!("<tsdb_client.Db {} {}>", self.path.display(), if open { "open" } else { "closed" })
    }

    /// The database directory.
    #[getter]
    fn path(&self) -> PathBuf {
        self.path.clone()
    }

    /// Returns the SIMD kernels the engine picked for this CPU, such as "avx2".
    #[staticmethod]
    fn simd_level() -> &'static str {
        tc::Db::simd_level()
    }

    /// Returns the id of a series, creating it (as float32) if needed.
    ///
    /// Raises:
    ///     TsdbError: ERR_NOTFOUND if create is False and there is no such series.
    #[pyo3(signature = (name, create = true))]
    fn series(&self, py: Python<'_>, name: &str, create: bool) -> PyResult<u32> {
        self.with(py, |d| if create { d.series(name) } else { d.series_lookup(name) })
    }

    /// Returns the id of a series, creating it (as float32) in a volume if needed.
    fn series_at(&self, py: Python<'_>, name: &str, path: &str) -> PyResult<u32> {
        self.with(py, |d| d.series_at(name, path))
    }

    /// Returns the id of a series of a given value type, creating it if needed.
    ///
    /// Raises:
    ///     TsdbError: ERR_INVALID if the series exists with another type.
    ///     ValueError: Unknown value_type.
    #[pyo3(signature = (name, value_type = "float32", path = None))]
    fn series_create(&self, py: Python<'_>, name: &str, value_type: &str, path: Option<&str>) -> PyResult<u32> {
        let ty = self::value_type(value_type)?;
        self.with(py, |d| d.series_create(name, ty, path))
    }

    /// Returns the value type of a series: "float32", "float64", "int64", "bool" or "state".
    fn series_type(&self, py: Python<'_>, id: u32) -> PyResult<&'static str> {
        self.with(py, |d| d.series_type(id)).map(ValueType::name)
    }

    /// Returns the name of a series.
    fn series_name(&self, py: Python<'_>, id: u32) -> PyResult<String> {
        self.with(py, |d| d.series_name(id))
    }

    /// Returns the volume (directory) a series is stored in.
    fn series_path(&self, py: Python<'_>, id: u32) -> PyResult<String> {
        self.with(py, |d| d.series_path(id))
    }

    /// Returns the number of series.
    fn series_count(&self, py: Python<'_>) -> PyResult<u32> {
        self.with(py, |d| Ok(d.series_count()))
    }

    /// Returns the directories of the volumes, the database directory first.
    fn volumes(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        self.with(py, |d| Ok(d.volumes()))
    }

    /// Returns the code of a state name, creating it if needed.
    fn state(&self, py: Python<'_>, series: u32, name: &str) -> PyResult<u32> {
        self.with(py, |d| d.state(series, name))
    }

    /// Returns the code of an existing state name (ERR_NOTFOUND if it has none).
    fn state_lookup(&self, py: Python<'_>, series: u32, name: &str) -> PyResult<u32> {
        self.with(py, |d| d.state_lookup(series, name))
    }

    /// Returns the name of a state code (ERR_NOTFOUND for an unknown code).
    fn state_name(&self, py: Python<'_>, series: u32, code: u32) -> PyResult<String> {
        self.with(py, |d| d.state_name(series, code))
    }

    /// Returns the names of many state codes; None and masked entries give None.
    fn state_names(&self, py: Python<'_>, series: u32, codes: &Bound<'_, PyAny>) -> PyResult<Vec<Option<String>>> {
        let col = py_column(codes, ValueType::State)?;
        self.with(py, |d| {
            col.iter::<u32>()
                .expect("a state column")
                .map(|code| code.map(|k| d.state_name(series, k)).transpose())
                .collect()
        })
    }

    /// Inserts float32 rows into one series. The last write wins.
    ///
    /// Args:
    ///     series: The series id.
    ///     ts: Timestamps, ms since the epoch: a list of ints or an int64 array.
    ///     values: One float per timestamp: a list or a float32/float64 array.
    fn insert_series(
        &self,
        py: Python<'_>,
        series: u32,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let (ts, values) = (ts_column(ts)?, f32_column(values)?);
        self.with(py, |d| d.insert_series(series, &ts, &values))
    }

    /// Inserts float32 rows into many series, all or nothing.
    fn insert(
        &self,
        py: Python<'_>,
        series: &Bound<'_, PyAny>,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let (series, ts, values) = (series_column(series)?, ts_column(ts)?, f32_column(values)?);
        self.with(py, |d| d.insert(&series, &ts, &values))
    }

    /// The same as insert_series(): an embedded insert has no round trip to hide.
    fn insert_series_async(
        &self,
        py: Python<'_>,
        series: u32,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.insert_series(py, series, ts, values)
    }

    /// The same as insert().
    fn insert_async(
        &self,
        py: Python<'_>,
        series: &Bound<'_, PyAny>,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.insert(py, series, ts, values)
    }

    /// Inserts values of any type, with nulls, into one series.
    ///
    /// Args:
    ///     series: The series id.
    ///     ts: Timestamps, ms since the epoch.
    ///     values: A list with None for nulls, a numpy array, or a numpy
    ///         masked array (masked = null).
    ///     value_type: The series' type; None looks it up.
    #[pyo3(signature = (series, ts, values, *, value_type = None))]
    fn insert_series_values(
        &self,
        py: Python<'_>,
        series: u32,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
        value_type: Option<&str>,
    ) -> PyResult<()> {
        self.insert_typed(py, Some(series), None, ts, values, value_type)
    }

    /// Inserts values of any type into many series of that type, all or nothing.
    #[pyo3(signature = (series, ts, values, *, value_type = None))]
    fn insert_values(
        &self,
        py: Python<'_>,
        series: &Bound<'_, PyAny>,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
        value_type: Option<&str>,
    ) -> PyResult<()> {
        self.insert_typed(py, None, Some(series), ts, values, value_type)
    }

    /// The same as insert_series_values().
    #[pyo3(signature = (series, ts, values, *, value_type = None))]
    fn insert_series_values_async(
        &self,
        py: Python<'_>,
        series: u32,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
        value_type: Option<&str>,
    ) -> PyResult<()> {
        self.insert_typed(py, Some(series), None, ts, values, value_type)
    }

    /// The same as insert_values().
    #[pyo3(signature = (series, ts, values, *, value_type = None))]
    fn insert_values_async(
        &self,
        py: Python<'_>,
        series: &Bound<'_, PyAny>,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
        value_type: Option<&str>,
    ) -> PyResult<()> {
        self.insert_typed(py, None, Some(series), ts, values, value_type)
    }

    /// Nothing to wait for: embedded inserts are done when they return.
    fn wait(&self, py: Python<'_>) -> PyResult<()> {
        self.with(py, |_| Ok(()))
    }

    /// Always 0: embedded inserts are done when they return.
    #[getter]
    fn pending(&self, py: Python<'_>) -> PyResult<usize> {
        self.with(py, |_| Ok(0))
    }

    /// Returns the float32 rows of a series as (timestamps, values).
    ///
    /// Args:
    ///     start, end: The half-open range [start, end) in ms; None is unbounded.
    ///     order: "asc" or "desc".
    ///     limit: At most this many rows; None is all.
    ///     numpy: True for numpy arrays, False for lists, None for numpy if
    ///         it can be imported.
    #[pyo3(signature = (series, start = None, end = None, *, order = "asc", limit = None, numpy = None))]
    #[allow(clippy::too_many_arguments)]
    fn query_raw<'py>(
        &self,
        py: Python<'py>,
        series: u32,
        start: Option<i64>,
        end: Option<i64>,
        order: &str,
        limit: Option<u64>,
        numpy: Option<bool>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let o = opts(order, limit)?;
        let p = self.query(py, |d| d.query_points(series, range(start, end), o))?;
        points_to_py(py, p, numpy)
    }

    /// The same as query_raw().
    #[pyo3(signature = (series, start = None, end = None, *, order = "asc", limit = None, numpy = None))]
    #[allow(clippy::too_many_arguments)]
    fn query_points<'py>(
        &self,
        py: Python<'py>,
        series: u32,
        start: Option<i64>,
        end: Option<i64>,
        order: &str,
        limit: Option<u64>,
        numpy: Option<bool>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        self.query_raw(py, series, start, end, order, limit, numpy)
    }

    /// Returns the rows of a series of any type as (timestamps, values).
    ///
    /// values is a numpy masked array (masked = null) of the series' type,
    /// or a list with None for nulls; the arguments are those of query_raw().
    #[pyo3(signature = (series, start = None, end = None, *, order = "asc", limit = None, numpy = None))]
    #[allow(clippy::too_many_arguments)]
    fn query_values<'py>(
        &self,
        py: Python<'py>,
        series: u32,
        start: Option<i64>,
        end: Option<i64>,
        order: &str,
        limit: Option<u64>,
        numpy: Option<bool>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let o = opts(order, limit)?;
        let rows = self.query(py, |d| d.query_rows(series, range(start, end), o))?;
        let np = numpy_module(py, numpy)?;
        PyTuple::new(py, [ts_to_py(py, np, rows.ts)?, column_to_py(py, np, rows.values)?])
    }

    /// Aggregates a series: one bucket per interval_ms, or one for the whole
    /// range when interval_ms is 0, as Buckets (a sequence of Bucket with
    /// numpy columns). Empty buckets are left out.
    #[pyo3(signature = (series, start = None, end = None, interval_ms = 0))]
    fn query_agg(
        &self,
        py: Python<'_>,
        series: u32,
        start: Option<i64>,
        end: Option<i64>,
        interval_ms: i64,
    ) -> PyResult<Buckets> {
        self.query(py, |d| d.query_agg(series, range(start, end), interval_ms).map(buckets))
    }

    /// Aggregates a series by calendar units, as Client.query_group() does.
    ///
    /// Args:
    ///     unit: "second", "minute", "hour", "day", "week", "month",
    ///         "quarter" or "year"; None is one bucket for the whole range.
    ///     count: Units per bucket.
    ///     timezone: A zone name or tzinfo; None is UTC.
    #[pyo3(signature = (series, start = None, end = None, *, unit = None, count = 1, timezone = None,
                        order = "asc", limit = None))]
    #[allow(clippy::too_many_arguments)]
    fn query_group(
        &self,
        py: Python<'_>,
        series: u32,
        start: Option<i64>,
        end: Option<i64>,
        unit: Option<&str>,
        count: u32,
        timezone: Option<&Bound<'_, PyAny>>,
        order: &str,
        limit: Option<u64>,
    ) -> PyResult<Buckets> {
        let o = opts(order, limit)?;
        let timezone = timezone.map(zone_name).transpose()?;
        let group = unit.map(self::unit).transpose()?.map(|u| tc::Group { unit: u, count, timezone });
        self.query(py, |d| d.query_group(series, range(start, end), group.as_ref(), o).map(buckets))
    }

    /// Aggregates a series into buckets between ascending bounds (ms).
    #[pyo3(signature = (series, bounds, start = None, end = None, *, order = "asc", limit = None))]
    #[allow(clippy::too_many_arguments)]
    fn query_bounds(
        &self,
        py: Python<'_>,
        series: u32,
        bounds: &Bound<'_, PyAny>,
        start: Option<i64>,
        end: Option<i64>,
        order: &str,
        limit: Option<u64>,
    ) -> PyResult<Buckets> {
        let o = opts(order, limit)?;
        let bounds = ts_column(bounds)?;
        self.query(py, |d| d.query_bounds(series, range(start, end), &bounds, o).map(buckets))
    }

    /// Runs many raw float32 queries at once, in parallel on every CPU.
    ///
    /// One call answers a query per entry of series, with the GIL released
    /// for all of them: a single Python thread uses every core, and the
    /// interpreter is crossed once per call rather than once per query.
    ///
    /// Args:
    ///     series: Series ids: a list or a numpy array (uint32), one per query.
    ///     start, end: None (unbounded), one timestamp for every query, or a
    ///         list or int64 array with one per query; [start, end) in ms.
    ///     order: "asc" or "desc".
    ///     limit: At most this many rows per query; None is all.
    ///
    /// Returns:
    ///     A list with (timestamps, values) of each query, in query order: an
    ///     int64 and a float32 numpy array, as query_raw() returns them.
    ///     Needs numpy.
    ///
    /// Raises:
    ///     TsdbError: The first failing query's error (in query order).
    #[pyo3(signature = (series, start = None, end = None, *, order = "asc", limit = None))]
    fn query_raw_many<'py>(
        &self,
        py: Python<'py>,
        series: &Bound<'_, PyAny>,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        order: &str,
        limit: Option<u64>,
    ) -> PyResult<Bound<'py, PyList>> {
        let (o, q) = (opts(order, limit)?, range_queries(series, start, end)?);
        let p = self.with(py, |d| d.query_points_many(&q, o))?;
        points_list(py, p)
    }

    /// Runs many aggregate queries at once, in parallel on every CPU.
    ///
    /// Args:
    ///     series: Series ids, one per query (a list or a uint32 array).
    ///     start, end: As in query_raw_many().
    ///     interval_ms: Bucket length in ms: one for every query, or a list or
    ///         int64 array of one per query; 0 (the default) is one bucket for
    ///         the query's range.
    ///
    /// Returns:
    ///     BucketsMany: result[i] is the Buckets of query i; result.buckets
    ///     holds every bucket (numpy columns) and result.offsets where each
    ///     query's begin.
    ///
    /// Raises:
    ///     TsdbError: The first failing query's error (in query order).
    #[pyo3(signature = (series, start = None, end = None, interval_ms = None))]
    fn query_agg_many(
        &self,
        py: Python<'_>,
        series: &Bound<'_, PyAny>,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        interval_ms: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<BucketsMany> {
        let q = agg_queries(series, start, end, interval_ms)?;
        let b = self.with(py, |d| d.query_agg_many(&q).map(many_cols))?;
        BucketsMany::new(py, b)
    }

    /// Runs many inserts at once, in parallel on every CPU.
    ///
    /// Each insert is all or nothing on its own; if one fails, the error of
    /// the first failing one is raised and the others may have been committed.
    ///
    /// Args:
    ///     batches: A list of (series, ts, values) tuples: series is one id or
    ///         a column of ids (one per row), ts int64 timestamps, values one
    ///         per timestamp. numpy arrays are read in place, without a copy.
    ///     value_type: The type of every batch's values; None takes it from
    ///         each values array's dtype (float32, float64, int64, bool,
    ///         uint32 for states), float32 for lists.
    #[pyo3(signature = (batches, *, value_type = None))]
    fn insert_many(&self, py: Python<'_>, batches: &Bound<'_, PyAny>, value_type: Option<&str>) -> PyResult<()> {
        let b = inserts(batches, value_type.map(crate::value_type).transpose()?)?;
        self.with(py, |d| d.insert_many(&b.iter().map(Insert::as_batch).collect::<Vec<_>>()))
    }

    /// Makes every insert so far durable (syncs the WAL).
    fn commit(&self, py: Python<'_>) -> PyResult<()> {
        self.with(py, |d| d.commit())
    }

    /// Writes the memtable into parts.
    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        self.with(py, |d| d.flush())
    }

    /// Flushes, then merges every partition down to as few parts as it can.
    fn optimize(&self, py: Python<'_>) -> PyResult<()> {
        self.with(py, |d| d.optimize())
    }

    /// Drops whole partitions that end at or before ts.
    fn drop_before(&self, py: Python<'_>, ts: i64) -> PyResult<()> {
        self.with(py, |d| d.drop_before(ts))
    }

    /// A read-only database: picks up what the writer published since the last refresh.
    fn refresh(&self, py: Python<'_>) -> PyResult<()> {
        self.with(py, |d| d.refresh())
    }

    /// Returns the storage statistics.
    fn stats(&self, py: Python<'_>) -> PyResult<Stats> {
        self.with(py, |d| d.stats()).map(Stats::from)
    }

    /// Returns what the last query through this Db read (all zero before the first).
    fn last_query_stats(&self, py: Python<'_>) -> PyResult<QueryStats> {
        self.with(py, |_| Ok(*self.last.lock().unwrap_or_else(|e| e.into_inner()))).map(QueryStats::from)
    }

    /// None while healthy; after a storage error that leaves it unknown what
    /// reached the disk, what failed (writes then fail until reopened).
    fn failed(&self, py: Python<'_>) -> PyResult<Option<String>> {
        self.with(py, |d| Ok(d.failed()))
    }
}
