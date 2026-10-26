//! Python binding for the tsdbd client (`client/rust`), built with maturin.
//!
//! Arguments are converted to Rust vectors while the GIL is held, and the
//! network call then runs detached from the interpreter (`Python::detach`, the
//! PyO3 0.26+ name of `allow_threads`), so other Python threads keep running.
//! The connection sits behind a mutex taken after detaching: threads sharing
//! one `Client` take turns without holding the GIL while they wait.
//!
//! Columns are accepted as anything exporting the buffer protocol with the
//! right element type (numpy arrays, `array.array`), as numpy masked arrays
//! (masked = null), or as sequences (`None` = null). Query results come back
//! as numpy arrays when numpy can be imported (typed queries: masked arrays),
//! else as lists with `None` for nulls.
//!
//! Series types and state names never change once created, so the client
//! caches them: an insert without an explicit type asks the server once per
//! series, and `state_names` once per code.

use std::collections::HashMap;
use std::io;
use std::ops::Bound as Range;
use std::sync::Mutex;
use std::time::Duration;

use pyo3::buffer::PyBuffer;
use pyo3::create_exception;
use pyo3::exceptions::{PyAttributeError, PyException, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

use ::tsdb_client as tc;
use tc::{Column, ColumnRef, ValueType, Values};

mod arrays;
mod embedded;
mod pool;

use arrays::{Buckets, Col, TwaBuckets, values_column, vec_to_py};

create_exception!(
    tsdb_client,
    TsdbError,
    PyException,
    "An error from tsdbd or the client; `code` is the TSDB_ERR_* number (negative)."
);

fn to_pyerr(py: Python<'_>, e: tc::Error) -> PyErr {
    let err = TsdbError::new_err(e.to_string());
    if let Err(set) = err.value(py).setattr("code", e.code()) {
        return set;
    }
    if let Err(set) = err.value(py).setattr("index", e.batch_index()) {
        return set;
    }
    err
}

fn closed() -> tc::Error {
    tc::Error::Io(io::Error::new(io::ErrorKind::NotConnected, "the client is closed"))
}

fn value_type(name: &str) -> PyResult<ValueType> {
    name.parse().map_err(|_| {
        PyValueError::new_err(format!("value type must be float32, float64, int64, bool or state, not {name:?}"))
    })
}

// ---------------------------------------------------------------- inputs

fn ts_column(obj: &Bound<'_, PyAny>) -> PyResult<Col<i64>> {
    Ok(match Col::buffer(obj)? {
        Some(c) => c,
        None => Col::Owned(obj.extract()?),
    })
}

fn series_column(obj: &Bound<'_, PyAny>) -> PyResult<Col<u32>> {
    Ok(match Col::buffer(obj)? {
        Some(c) => c,
        None => Col::Owned(obj.extract()?),
    })
}

/// float32 values of the version 1 calls: float32 or float64 buffers, or a sequence.
fn f32_column(obj: &Bound<'_, PyAny>) -> PyResult<Col<f32>> {
    if let Some(c) = Col::buffer(obj)? {
        return Ok(c);
    }
    if let Some(b) = arrays::native::<f64>(obj) {
        return Ok(Col::Owned(b.to_vec(obj.py())?.into_iter().map(|v| v as f32).collect()));
    }
    Ok(Col::Owned(obj.extract::<Vec<f64>>()?.into_iter().map(|v| v as f32).collect()))
}

/// A numpy bool array as `Vec<bool>` (the buffer protocol has no bool element
/// in PyO3, so it is viewed as bytes); `None` for anything else.
fn numpy_bools(obj: &Bound<'_, PyAny>) -> PyResult<Option<Vec<bool>>> {
    if !obj.hasattr("dtype")? || obj.getattr("dtype")?.getattr("kind")?.extract::<String>()? != "b" {
        return Ok(None);
    }
    let bytes = obj.call_method1("view", ("u1",))?;
    Ok(Some(PyBuffer::<u8>::get(&bytes)?.to_vec(obj.py())?.into_iter().map(|b| b != 0).collect()))
}

/// A sequence with `None` for nulls.
fn seq_column<T: tc::Native>(
    obj: &Bound<'_, PyAny>,
    get: impl Fn(&Bound<'_, PyAny>) -> PyResult<T>,
) -> PyResult<Column> {
    let (mut v, mut valid) = (Vec::with_capacity(obj.len().unwrap_or(0)), tc::Bitmap::default());
    for item in obj.try_iter()? {
        let item = item?;
        let known = !item.is_none();
        v.push(if known { get(&item)? } else { T::default() });
        valid.push(known);
    }
    let mut col = Column::new(v);
    col.valid = (valid.null_count() > 0).then_some(valid);
    Ok(col)
}

/// Converts typed values: a buffer of the type (or a compatible one), a
/// numpy masked array, or a sequence with `None` for nulls.
fn py_column(obj: &Bound<'_, PyAny>, ty: ValueType) -> PyResult<Column> {
    let py = obj.py();
    if obj.hasattr("mask")? && obj.hasattr("data")? {
        let ma = py.import("numpy")?.getattr("ma")?;
        if ma.call_method1("isMaskedArray", (obj,))?.is_truthy()? {
            let mut col = plain_column(&obj.getattr("data")?, ty)?;
            let nulls = numpy_bools(&ma.call_method1("getmaskarray", (obj,))?)?.unwrap_or_default();
            if nulls.len() != col.len() {
                return Err(PyValueError::new_err("the mask and the data differ in length"));
            }
            if nulls.contains(&true) {
                col.valid = Some(nulls.into_iter().map(|null| !null).collect());
            }
            return Ok(col);
        }
    }
    plain_column(obj, ty)
}

fn plain_column(obj: &Bound<'_, PyAny>, ty: ValueType) -> PyResult<Column> {
    let py = obj.py();
    Ok(match ty {
        ValueType::Float32 => match arrays::native::<f32>(obj) {
            Some(b) => Column::new(b.to_vec(py)?),
            None => match arrays::native::<f64>(obj) {
                Some(b) => Column::new(b.to_vec(py)?.into_iter().map(|v| v as f32).collect()),
                None => seq_column(obj, |x| Ok(x.extract::<f64>()? as f32))?,
            },
        },
        ValueType::Float64 => match arrays::native::<f64>(obj) {
            Some(b) => Column::new(b.to_vec(py)?),
            None => match arrays::native::<f32>(obj) {
                Some(b) => Column::new(b.to_vec(py)?.into_iter().map(f64::from).collect()),
                None => seq_column(obj, |x| x.extract::<f64>())?,
            },
        },
        ValueType::Int64 => match arrays::native::<i64>(obj) {
            Some(b) => Column::new(b.to_vec(py)?),
            None => match arrays::native::<i32>(obj) {
                Some(b) => Column::new(b.to_vec(py)?.into_iter().map(i64::from).collect()),
                None => seq_column(obj, |x| x.extract::<i64>())?,
            },
        },
        ValueType::Bool => match numpy_bools(obj)? {
            Some(v) => Column::new(v),
            None => seq_column(obj, |x| x.extract::<bool>())?,
        },
        ValueType::State => match arrays::native::<u32>(obj) {
            Some(b) => Column::new(b.to_vec(py)?),
            None => seq_column(obj, |x| x.extract::<u32>())?,
        },
        t => return Err(PyValueError::new_err(format!("value type {t} is not supported by this binding"))),
    })
}

// --------------------------------------------------------------- outputs
//
// numpy arrays take over the vectors the query filled with the GIL released
// (no copy); lists hold Python ints, floats and bools.

/// Whether to answer with numpy: required (`Some(true)`), refused (`Some(false)`) or if importable.
fn numpy_module(py: Python<'_>, numpy: Option<bool>) -> PyResult<bool> {
    arrays::use_numpy(py, numpy)
}

fn ts_to_py(py: Python<'_>, np: bool, ts: Vec<i64>) -> PyResult<Bound<'_, PyAny>> {
    vec_to_py(py, np, ts)
}

/// `(timestamps, values)` of a float32 query, as numpy arrays or lists.
fn points_to_py(py: Python<'_>, p: tc::Points, numpy: Option<bool>) -> PyResult<Bound<'_, PyTuple>> {
    let np = numpy_module(py, numpy)?;
    PyTuple::new(py, [ts_to_py(py, np, p.ts)?, vec_to_py(py, np, p.values)?])
}

/// A typed column: a numpy masked array (dtype per type, masked = null) or a list with `None`.
fn column_to_py(py: Python<'_>, np: bool, col: Column) -> PyResult<Bound<'_, PyAny>> {
    if !np {
        let list = match &col.values {
            Values::Float32(_) => PyList::new(py, col.to_options::<f32>().unwrap_or_default())?,
            Values::Float64(_) => PyList::new(py, col.to_options::<f64>().unwrap_or_default())?,
            Values::Int64(_) => PyList::new(py, col.to_options::<i64>().unwrap_or_default())?,
            Values::Bool(_) => PyList::new(py, col.to_options::<bool>().unwrap_or_default())?,
            Values::State(_) => PyList::new(py, col.to_options::<u32>().unwrap_or_default())?,
            _ => return Err(PyValueError::new_err("a value type this binding does not support")),
        };
        return Ok(list.into_any());
    }
    let mask = match &col.valid {
        Some(valid) => vec_to_py(py, true, valid.iter().map(|known| !known).collect::<Vec<bool>>())?,
        None => false.into_pyobject(py)?.to_owned().into_any(),
    };
    let data = match col.values {
        Values::Float32(v) => vec_to_py(py, true, v)?,
        Values::Float64(v) => vec_to_py(py, true, v)?,
        Values::Int64(v) => vec_to_py(py, true, v)?,
        Values::Bool(v) => vec_to_py(py, true, v)?,
        Values::State(v) => vec_to_py(py, true, v)?,
        _ => return Err(PyValueError::new_err("a value type this binding does not support")),
    };
    arrays::numpy(py)?.getattr("ma")?.call_method1("MaskedArray", (data, mask))
}

// ------------------------------------------------------------- arguments

fn range(start: Option<i64>, end: Option<i64>) -> (Range<i64>, Range<i64>) {
    (start.map_or(Range::Unbounded, Range::Included), end.map_or(Range::Unbounded, Range::Excluded))
}

fn opts(order: &str, limit: Option<u64>) -> PyResult<tc::QueryOpts> {
    let order = match order {
        "asc" => tc::Order::Asc,
        "desc" => tc::Order::Desc,
        o => return Err(PyValueError::new_err(format!("order must be 'asc' or 'desc', not {o:?}"))),
    };
    Ok(tc::QueryOpts { order, limit })
}

fn twa_opts(method: &str, max_gap_ms: i64, order: &str, limit: Option<u64>) -> PyResult<tc::TwaOpts> {
    let method = match method {
        "step" => tc::TwaMethod::Step,
        "linear" => tc::TwaMethod::Linear,
        m => return Err(PyValueError::new_err(format!("method must be 'step' or 'linear', not {m:?}"))),
    };
    let o = opts(order, limit)?;
    Ok(tc::TwaOpts { method, max_gap_ms, order: o.order, limit: o.limit })
}

/// The calendar grouping of query_group() and friends: None for one bucket over the range.
fn group(unit: Option<&str>, count: u32, timezone: Option<&Bound<'_, PyAny>>) -> PyResult<Option<tc::Group>> {
    let timezone = timezone.map(zone_name).transpose()?;
    Ok(unit.map(self::unit).transpose()?.map(|u| tc::Group { unit: u, count, timezone }))
}

fn rows_to_py<'py>(py: Python<'py>, rows: tc::Rows, numpy: Option<bool>) -> PyResult<Bound<'py, PyTuple>> {
    let np = numpy_module(py, numpy)?;
    PyTuple::new(py, [ts_to_py(py, np, rows.ts)?, column_to_py(py, np, rows.values)?])
}

fn unit(name: &str) -> PyResult<tc::Unit> {
    Ok(match name {
        "second" => tc::Unit::Second,
        "minute" => tc::Unit::Minute,
        "hour" => tc::Unit::Hour,
        "day" => tc::Unit::Day,
        "week" => tc::Unit::Week,
        "month" => tc::Unit::Month,
        "quarter" => tc::Unit::Quarter,
        "year" => tc::Unit::Year,
        u => {
            return Err(PyValueError::new_err(format!(
                "unit must be second, minute, hour, day, week, month, quarter or year, not {u:?}"
            )));
        }
    })
}

/// A time zone argument: a name, or a `zoneinfo.ZoneInfo` (its `key`).
fn zone_name(obj: &Bound<'_, PyAny>) -> PyResult<String> {
    if let Ok(s) = obj.extract::<String>() {
        return Ok(s);
    }
    if let Ok(key) = obj.getattr("key")
        && let Ok(s) = key.extract::<String>()
    {
        return Ok(s);
    }
    Err(PyTypeError::new_err("timezone must be a str or a zoneinfo.ZoneInfo with a key"))
}

// --------------------------------------------------------------- results

/// One aggregate bucket, as returned by query_agg, query_group and query_bounds.
///
/// Aggregates skip nulls. Buckets without rows are not returned; a bucket
/// whose rows are all null is returned with count 0. Two buckets are equal
/// when all their fields are (floats compared bit for bit).
#[pyclass(frozen, module = "tsdb_client")]
struct Bucket {
    start: i64,
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
    isum: i64,
    imin: i64,
    imax: i64,
    /// range, var_pop, var_samp, stddev_pop, stddev_samp: only in the results of the spread queries
    spread: Option<[f64; 5]>,
}

impl Bucket {
    fn spread(&self, i: usize, name: &str) -> PyResult<f64> {
        self.spread.map(|s| s[i]).ok_or_else(|| {
            PyAttributeError::new_err(format!("{name}: only the buckets of the query_spread*() calls have it"))
        })
    }
}

#[pymethods]
impl Bucket {
    /// Start of the bucket, ms since the epoch (UTC). For a single bucket
    /// over the whole range, the start of the range.
    #[getter]
    fn start(&self) -> i64 {
        self.start
    }

    /// Number of known (not null) values in the bucket.
    #[getter]
    fn count(&self) -> u64 {
        self.count
    }

    /// Sum of the values, as a float. A NaN sum, min or max is always the quiet NaN
    /// 0x7ff8000000000000, whatever NaNs were stored, so results compare bit for bit.
    #[getter]
    fn sum(&self) -> f64 {
        self.sum
    }

    /// Smallest value (NaN if there is none).
    #[getter]
    fn min(&self) -> f64 {
        self.min
    }

    /// Largest value (NaN if there is none).
    #[getter]
    fn max(&self) -> f64 {
        self.max
    }

    /// Exact sum for int64 and bool series (true counts 1); saturates at the
    /// int64 limits.
    #[getter]
    fn isum(&self) -> i64 {
        self.isum
    }

    /// Exact smallest value for int64 and bool series.
    #[getter]
    fn imin(&self) -> i64 {
        self.imin
    }

    /// Exact largest value for int64 and bool series.
    #[getter]
    fn imax(&self) -> i64 {
        self.imax
    }

    /// The mean, sum / count (0 for an empty bucket).
    #[getter]
    fn avg(&self) -> f64 {
        if self.count == 0 { 0.0 } else { self.sum / self.count as f64 }
    }

    /// max - min (spread queries only; AttributeError otherwise).
    #[getter]
    fn range(&self) -> PyResult<f64> {
        self.spread(0, "range")
    }

    /// Population variance (spread queries only). NaN when count is 0.
    #[getter]
    fn var_pop(&self) -> PyResult<f64> {
        self.spread(1, "var_pop")
    }

    /// Sample variance (spread queries only). NaN when count is below 2.
    #[getter]
    fn var_samp(&self) -> PyResult<f64> {
        self.spread(2, "var_samp")
    }

    /// Population standard deviation (spread queries only).
    #[getter]
    fn stddev_pop(&self) -> PyResult<f64> {
        self.spread(3, "stddev_pop")
    }

    /// Sample standard deviation (spread queries only).
    #[getter]
    fn stddev_samp(&self) -> PyResult<f64> {
        self.spread(4, "stddev_samp")
    }

    fn __repr__(&self) -> String {
        let mut r = format!(
            "Bucket(start={}, count={}, sum={:?}, min={:?}, max={:?}, isum={}, imin={}, imax={}",
            self.start, self.count, self.sum, self.min, self.max, self.isum, self.imin, self.imax
        );
        if let Some([range, vp, vs, sp, ss]) = self.spread {
            r += &format!(", range={range:?}, var_pop={vp:?}, var_samp={vs:?}, stddev_pop={sp:?}, stddev_samp={ss:?}");
        }
        r + ")"
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        other.cast::<Bucket>().is_ok_and(|o| {
            let o = o.get();
            (o.start, o.count, o.isum, o.imin, o.imax) == (self.start, self.count, self.isum, self.imin, self.imax)
                && (o.sum.to_bits(), o.min.to_bits(), o.max.to_bits())
                    == (self.sum.to_bits(), self.min.to_bits(), self.max.to_bits())
                && o.spread.map(|s| s.map(f64::to_bits)) == self.spread.map(|s| s.map(f64::to_bits))
        })
    }
}

impl From<tc::Bucket> for Bucket {
    fn from(b: tc::Bucket) -> Self {
        Bucket {
            start: b.start,
            count: b.count,
            sum: b.sum,
            min: b.min,
            max: b.max,
            isum: b.isum,
            imin: b.imin,
            imax: b.imax,
            spread: None,
        }
    }
}

impl From<tc::SpreadBucket> for Bucket {
    fn from(s: tc::SpreadBucket) -> Self {
        Bucket {
            spread: Some([s.range, s.var_pop, s.var_samp, s.stddev_pop, s.stddev_samp]),
            ..Bucket::from(s.bucket)
        }
    }
}

/// Buckets, a column per field (built with the GIL released).
fn buckets(v: Vec<tc::Bucket>) -> Buckets {
    Buckets::from(v)
}

/// Database statistics, as returned by Client.stats().
#[pyclass(frozen, module = "tsdb_client")]
struct Stats {
    parts: u64,
    partitions: u64,
    rows: u64,
    bytes: u64,
    merges: u64,
    merged_rows: u64,
    passthrough_granules: u64,
    memtable_rows: u64,
}

#[pymethods]
impl Stats {
    /// Parts (files of sorted rows) on disk.
    #[getter]
    fn parts(&self) -> u64 {
        self.parts
    }

    /// Partitions (time ranges, one day each by default).
    #[getter]
    fn partitions(&self) -> u64 {
        self.partitions
    }

    /// Rows stored in parts.
    #[getter]
    fn rows(&self) -> u64 {
        self.rows
    }

    /// Bytes of the parts on disk.
    #[getter]
    fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Merges of parts done since the server started.
    #[getter]
    fn merges(&self) -> u64 {
        self.merges
    }

    /// Rows written by those merges.
    #[getter]
    fn merged_rows(&self) -> u64 {
        self.merged_rows
    }

    /// Blocks of rows that merges copied without decompressing them.
    #[getter]
    fn passthrough_granules(&self) -> u64 {
        self.passthrough_granules
    }

    /// Rows held in memory (and in the write-ahead log), not yet in parts.
    #[getter]
    fn memtable_rows(&self) -> u64 {
        self.memtable_rows
    }

    fn __repr__(&self) -> String {
        format!(
            "Stats(parts={}, partitions={}, rows={}, bytes={}, merges={}, merged_rows={}, \
             passthrough_granules={}, memtable_rows={})",
            self.parts,
            self.partitions,
            self.rows,
            self.bytes,
            self.merges,
            self.merged_rows,
            self.passthrough_granules,
            self.memtable_rows
        )
    }
}

impl From<tc::Stats> for Stats {
    fn from(s: tc::Stats) -> Self {
        Stats {
            parts: s.parts,
            partitions: s.partitions,
            rows: s.rows,
            bytes: s.bytes,
            merges: s.merges,
            merged_rows: s.merged_rows,
            passthrough_granules: s.passthrough_granules,
            memtable_rows: s.memtable_rows,
        }
    }
}

/// What the server did for this connection's previous query, as returned by
/// Client.last_query_stats().
///
/// The server stores rows in blocks ("granules") of up to 8192 rows, each
/// with its count, sum, min and max. An aggregate over whole blocks reads
/// those numbers instead of decompressing the rows.
#[pyclass(frozen, module = "tsdb_client")]
struct QueryStats {
    elapsed_s: f64,
    rows: u64,
    rows_decoded: u64,
    rows_from_stats: u64,
    memtable_rows: u64,
    bytes: u64,
    bytes_read: u64,
    parts: u64,
    granules: u64,
    granules_from_stats: u64,
    result_rows: u64,
}

#[pymethods]
impl QueryStats {
    /// Seconds the server spent on the query.
    #[getter]
    fn elapsed_s(&self) -> f64 {
        self.elapsed_s
    }

    /// Rows covered: rows_decoded + rows_from_stats + memtable_rows.
    #[getter]
    fn rows(&self) -> u64 {
        self.rows
    }

    /// Rows decompressed (whole granules).
    #[getter]
    fn rows_decoded(&self) -> u64 {
        self.rows_decoded
    }

    /// Rows answered from granule statistics, without decompressing them.
    #[getter]
    fn rows_from_stats(&self) -> u64 {
        self.rows_from_stats
    }

    /// Rows read from memory (not yet in parts).
    #[getter]
    fn memtable_rows(&self) -> u64 {
        self.memtable_rows
    }

    /// Uncompressed size of the rows covered (timestamp and value).
    #[getter]
    fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Compressed bytes of the granules decompressed.
    #[getter]
    fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// Parts with rows of the series in the range.
    #[getter]
    fn parts(&self) -> u64 {
        self.parts
    }

    /// Granules of those parts in the range.
    #[getter]
    fn granules(&self) -> u64 {
        self.granules
    }

    /// Granules answered from their statistics.
    #[getter]
    fn granules_from_stats(&self) -> u64 {
        self.granules_from_stats
    }

    /// Rows, or buckets, returned.
    #[getter]
    fn result_rows(&self) -> u64 {
        self.result_rows
    }

    fn __repr__(&self) -> String {
        format!(
            "QueryStats(elapsed_s={:?}, rows={}, rows_decoded={}, rows_from_stats={}, memtable_rows={}, bytes={}, \
             bytes_read={}, parts={}, granules={}, granules_from_stats={}, result_rows={})",
            self.elapsed_s,
            self.rows,
            self.rows_decoded,
            self.rows_from_stats,
            self.memtable_rows,
            self.bytes,
            self.bytes_read,
            self.parts,
            self.granules,
            self.granules_from_stats,
            self.result_rows
        )
    }
}

impl From<tc::QueryStats> for QueryStats {
    fn from(s: tc::QueryStats) -> Self {
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
}

// ---------------------------------------------------------------- client

/// The connection and what never changes on the server once created.
#[derive(Default)]
struct Inner {
    conn: Option<tc::Client>,
    types: HashMap<u32, ValueType>,
    states: HashMap<(u32, u32), String>,
}

impl Inner {
    fn conn(&mut self) -> tc::Result<&mut tc::Client> {
        self.conn.as_mut().ok_or_else(closed)
    }

    fn series_type(&mut self, id: u32) -> tc::Result<ValueType> {
        if let Some(&t) = self.types.get(&id) {
            return Ok(t);
        }
        let t = self.conn()?.series_type(id)?;
        self.types.insert(id, t);
        Ok(t)
    }

    fn state_name(&mut self, series: u32, code: u32) -> tc::Result<String> {
        if let Some(n) = self.states.get(&(series, code)) {
            return Ok(n.clone());
        }
        let n = self.conn()?.state_name(series, code)?;
        self.states.insert((series, code), n.clone());
        Ok(n)
    }
}

/// One TCP connection to tsdbd.
///
/// Use it as a context manager (``with Client(...) as c:``) or call close().
/// Network calls release the GIL: threads can share one client (they take
/// turns) or open one each.
///
/// Args:
///     host: Server name or address.
///     port: Server port (7070 by default).
///     timeout: Seconds to wait for the connection and for every later read
///         or write; None (the default) gives up on the connect after 10 s
///         and lets every later call wait forever. A call that times out
///         raises TsdbError (ERR_IO) and closes the client.
///     keepalive: Turn TCP keepalive on (the default): after 30 s of silence
///         the client probes every 10 s and drops the connection after 3
///         unanswered probes, where the platform allows.
///
/// Raises:
///     TsdbError: The connection failed.
///     ValueError: timeout is not a positive number.
#[pyclass(module = "tsdb_client")]
struct Client {
    inner: Mutex<Inner>,
    addr: String,
}

impl Client {
    /// Runs `f` on the connection state with the GIL released.
    fn with_inner<T, F>(&self, py: Python<'_>, f: F) -> PyResult<T>
    where
        T: Send,
        F: FnOnce(&mut Inner) -> tc::Result<T> + Send,
    {
        py.detach(|| {
            let mut inner = self.inner.lock().unwrap_or_else(|e| {
                // a panic left the connection possibly in the middle of a reply: drop it
                self.inner.clear_poison();
                let mut g = e.into_inner();
                g.conn = None;
                g
            });
            let r = f(&mut inner);
            // after an I/O error (a timeout included) the stream is out of step: close it
            if inner.conn.as_ref().is_some_and(tc::Client::is_broken) {
                inner.conn = None;
            }
            r
        })
        .map_err(|e| to_pyerr(py, e))
    }

    /// Runs `f` on the connection with the GIL released.
    fn with<T, F>(&self, py: Python<'_>, f: F) -> PyResult<T>
    where
        T: Send,
        F: FnOnce(&mut tc::Client) -> tc::Result<T> + Send,
    {
        self.with_inner(py, |i| f(i.conn()?))
    }

    /// Typed inserts: into `one` series, or into `many` (a column of series ids).
    #[allow(clippy::too_many_arguments)]
    fn insert_typed(
        &self,
        py: Python<'_>,
        one: Option<u32>,
        many: Option<&Bound<'_, PyAny>>,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
        value_type: Option<&str>,
        wait: bool,
    ) -> PyResult<()> {
        let ts = ts_column(ts)?;
        let many = many.map(series_column).transpose()?;
        let ty = match (value_type, one.or_else(|| many.as_ref().and_then(Col::first))) {
            (Some(t), _) => self::value_type(t)?,
            (None, Some(id)) => self.with_inner(py, |i| i.series_type(id))?,
            (None, None) => ValueType::Float32,
        };
        let col = values_column(values, ty)?;
        self.with(py, |c| {
            let (ts, col, many) = (ts.snap(), col.snap(), many.as_ref().map(Col::snap));
            let col = ColumnRef::from(&col);
            match (&many, one) {
                (Some(s), _) if wait => c.insert_values(s, &ts, col),
                (Some(s), _) => c.insert_values_async(s, &ts, col),
                (None, Some(id)) if wait => c.insert_series_values(id, &ts, col),
                (None, Some(id)) => c.insert_series_values_async(id, &ts, col),
                (None, None) => unreachable!("one of the two is given"),
            }
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn replace_typed(
        &self,
        py: Python<'_>,
        series: u32,
        start: Option<i64>,
        end: Option<i64>,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
        value_type: Option<&str>,
    ) -> PyResult<()> {
        let ts = ts_column(ts)?;
        let ty = match value_type {
            Some(t) => self::value_type(t)?,
            None => self.with_inner(py, |i| i.series_type(series))?,
        };
        let col = values_column(values, ty)?;
        self.with(py, |c| c.replace_range(series, range(start, end), &ts.snap(), &col.snap()))
    }
}

fn connect(host: &str, port: u16, timeout: Option<Duration>, keepalive: bool) -> tc::Result<tc::Client> {
    let c = tc::Client::connect_with((host, port), timeout)?;
    if !keepalive {
        c.set_keepalive(None)?;
    }
    Ok(c)
}

/// A timeout in seconds as the client takes it: positive and finite, or None.
/// Anything that does not make a `Duration` (too large, or under a nanosecond)
/// is a ValueError too; `Duration::from_secs_f64` would panic on it.
fn seconds(t: Option<f64>) -> PyResult<Option<Duration>> {
    let bad = || PyValueError::new_err("timeout must be a positive number of seconds (at least 1e-9, finite)");
    match t {
        None => Ok(None),
        Some(t) if !(t > 0.0 && t.is_finite()) => Err(bad()),
        Some(t) => match Duration::try_from_secs_f64(t) {
            Ok(d) if !d.is_zero() => Ok(Some(d)),
            _ => Err(bad()),
        },
    }
}

#[pymethods]
impl Client {
    #[new]
    #[pyo3(signature = (host = "127.0.0.1", port = 7070, timeout = None, keepalive = true))]
    fn new(py: Python<'_>, host: &str, port: u16, timeout: Option<f64>, keepalive: bool) -> PyResult<Self> {
        let timeout = seconds(timeout)?;
        let c = py.detach(|| connect(host, port, timeout, keepalive)).map_err(|e| to_pyerr(py, e))?;
        Ok(Client { inner: Mutex::new(Inner { conn: Some(c), ..Inner::default() }), addr: format!("{host}:{port}") })
    }

    /// Changes how long a call waits for the server, in seconds; None waits forever.
    ///
    /// The limit applies to each wait for the server (sending or receiving), not
    /// to a whole call. A call that times out raises TsdbError (ERR_IO) and closes
    /// the client: connect again.
    ///
    /// Raises:
    ///     ValueError: timeout is not a positive number.
    ///     TsdbError: The client is closed.
    #[pyo3(signature = (timeout))]
    fn set_timeout(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<()> {
        let t = seconds(timeout)?;
        self.with(py, |c| c.set_timeout(t))
    }

    /// The timeout in seconds set at connect or by set_timeout; None waits forever.
    ///
    /// Raises:
    ///     TsdbError: The client is closed.
    #[getter]
    fn timeout(&self, py: Python<'_>) -> PyResult<Option<f64>> {
        self.with(py, |c| Ok(c.timeout().map(|d| d.as_secs_f64())))
    }

    /// Waits for pending inserts, then closes the connection.
    ///
    /// Closing twice is fine. Any later call raises TsdbError (ERR_IO).
    ///
    /// Raises:
    ///     TsdbError: The first error among the pending inserts.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        self.with_inner(py, |i| i.conn.take().map_or(Ok(()), tc::Client::close))
    }

    /// Returns the client itself, to use in a with statement.
    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    /// Closes the client at the end of a with block.
    ///
    /// If the block succeeded, this waits for pending inserts as close()
    /// does; if it raised, the connection is dropped at once.
    #[pyo3(signature = (exc_type, _exc, _tb))]
    fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: Option<&Bound<'_, PyAny>>,
        _exc: Option<&Bound<'_, PyAny>>,
        _tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        if exc_type.is_some_and(|t| !t.is_none()) {
            self.with_inner(py, |i| {
                drop(i.conn.take());
                Ok(())
            })?;
        } else {
            self.close(py)?;
        }
        Ok(false)
    }

    fn __repr__(&self) -> String {
        let open = self.inner.lock().map(|g| g.conn.is_some()).unwrap_or(false);
        format!("<tsdb_client.Client {} {}>", self.addr, if open { "open" } else { "closed" })
    }

    /// Returns the protocol version the server speaks.
    ///
    /// Version 3 added compressed inserts, version 4 grouping in time zones
    /// by the client (query_bounds).
    fn protocol_version(&self, py: Python<'_>) -> PyResult<u32> {
        self.with(py, |c| c.protocol_version())
    }

    /// Returns the id of a series, creating it (as float32) if needed.
    ///
    /// Args:
    ///     name: The series name, such as "pump7.flow".
    ///     create: If False, do not create the series.
    ///
    /// Raises:
    ///     TsdbError: ERR_NOTFOUND if create is False and there is no such series.
    #[pyo3(signature = (name, create = true))]
    fn series(&self, py: Python<'_>, name: &str, create: bool) -> PyResult<u32> {
        self.with(py, |c| if create { c.series(name) } else { c.series_lookup(name) })
    }

    /// Returns the id of a series, creating it (as float32) in a volume if needed.
    ///
    /// Args:
    ///     name: The series name.
    ///     path: A volume on the server for the new series, one the operator
    ///         added (`tsdb <dir> volume add`; else ERR_NOTFOUND); it only
    ///         matters when the series is created.
    fn series_at(&self, py: Python<'_>, name: &str, path: &str) -> PyResult<u32> {
        self.with(py, |c| c.series_at(name, path))
    }

    /// Returns the id of a series of a given value type, creating it if needed.
    ///
    /// Args:
    ///     name: The series name.
    ///     value_type: "float32", "float64", "int64", "bool" or "state".
    ///     path: A volume on the server for the new series, one the operator
    ///         added (`tsdb <dir> volume add`; else ERR_NOTFOUND); None
    ///         means the database directory.
    ///
    /// Raises:
    ///     TsdbError: ERR_INVALID if the series exists with another type.
    ///     ValueError: Unknown value_type.
    #[pyo3(signature = (name, value_type = "float32", path = None))]
    fn series_create(&self, py: Python<'_>, name: &str, value_type: &str, path: Option<&str>) -> PyResult<u32> {
        let ty = self::value_type(value_type)?;
        self.with_inner(py, |i| {
            let id = i.conn()?.series_create(name, ty, path)?;
            i.types.insert(id, ty);
            Ok(id)
        })
    }

    /// Returns the value type of a series: "float32", "float64", "int64", "bool" or "state".
    ///
    /// Raises:
    ///     TsdbError: ERR_NOTFOUND if there is no such series.
    fn series_type(&self, py: Python<'_>, id: u32) -> PyResult<&'static str> {
        self.with_inner(py, |i| i.series_type(id)).map(ValueType::name)
    }

    /// Returns the name of a series.
    ///
    /// Raises:
    ///     TsdbError: ERR_NOTFOUND if there is no such series.
    fn series_name(&self, py: Python<'_>, id: u32) -> PyResult<String> {
        self.with(py, |c| c.series_name(id))
    }

    /// Returns the code of a state name, creating it if needed.
    ///
    /// State series store small integer codes; the server keeps their names.
    /// Codes start at 1 and never change.
    ///
    /// Args:
    ///     series: A series id; its volume holds the names.
    ///     name: The state name, such as "running".
    fn state(&self, py: Python<'_>, series: u32, name: &str) -> PyResult<u32> {
        self.with(py, |c| c.state(series, name))
    }

    /// Returns the code of an existing state name.
    ///
    /// Raises:
    ///     TsdbError: ERR_NOTFOUND if the name has no code.
    fn state_lookup(&self, py: Python<'_>, series: u32, name: &str) -> PyResult<u32> {
        self.with(py, |c| c.state_lookup(series, name))
    }

    /// Returns the name of a state code.
    ///
    /// Raises:
    ///     TsdbError: ERR_NOTFOUND for an unknown code.
    fn state_name(&self, py: Python<'_>, series: u32, code: u32) -> PyResult<String> {
        self.with_inner(py, |i| i.state_name(series, code))
    }

    /// Returns the names of many state codes.
    ///
    /// Args:
    ///     series: A series id; its volume holds the names.
    ///     codes: A list, array or masked array of codes, as query_values
    ///         returns them. None and masked entries give None.
    ///
    /// Returns:
    ///     A list of names. Names are cached in the client.
    fn state_names(&self, py: Python<'_>, series: u32, codes: &Bound<'_, PyAny>) -> PyResult<Vec<Option<String>>> {
        let col = py_column(codes, ValueType::State)?;
        self.with_inner(py, |i| {
            col.iter::<u32>()
                .expect("a state column")
                .map(|code| code.map(|k| i.state_name(series, k)).transpose())
                .collect()
        })
    }

    /// Inserts float32 rows into one series and waits until the server has them.
    ///
    /// Writing a timestamp that already has a value replaces it: the last
    /// write wins.
    ///
    /// Args:
    ///     series: The series id.
    ///     ts: Timestamps, ms since the epoch: a list of ints or an int64 array.
    ///     values: One float per timestamp: a list or a float32/float64 array.
    ///
    /// Raises:
    ///     TsdbError: The server refused the rows (ERR_INVALID for a series
    ///         that is not float32).
    fn insert_series(
        &self,
        py: Python<'_>,
        series: u32,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let (ts, values) = (ts_column(ts)?, f32_column(values)?);
        self.with(py, |c| c.insert_series(series, &ts.snap(), &values.snap()))
    }

    /// Inserts float32 rows into many series and waits.
    ///
    /// Args:
    ///     series: The series id of each row: a list of ints or a uint32 array.
    ///     ts: The timestamp of each row, ms since the epoch.
    ///     values: The value of each row.
    fn insert(
        &self,
        py: Python<'_>,
        series: &Bound<'_, PyAny>,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let (series, ts, values) = (series_column(series)?, ts_column(ts)?, f32_column(values)?);
        self.with(py, |c| c.insert(&series.snap(), &ts.snap(), &values.snap()))
    }

    /// Sends float32 rows of one series without waiting for the answer.
    ///
    /// Faster for many small inserts. An error is raised by wait(), or by
    /// the next call that waits for the server.
    fn insert_series_async(
        &self,
        py: Python<'_>,
        series: u32,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let (ts, values) = (ts_column(ts)?, f32_column(values)?);
        self.with(py, |c| c.insert_series_async(series, &ts.snap(), &values.snap()))
    }

    /// Sends float32 rows of many series without waiting; see insert_series_async().
    fn insert_async(
        &self,
        py: Python<'_>,
        series: &Bound<'_, PyAny>,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let (series, ts, values) = (series_column(series)?, ts_column(ts)?, f32_column(values)?);
        self.with(py, |c| c.insert_async(&series.snap(), &ts.snap(), &values.snap()))
    }

    /// Inserts values of any type, with nulls, into one series and waits.
    ///
    /// Args:
    ///     series: The series id.
    ///     ts: Timestamps, ms since the epoch.
    ///     values: A list with None for nulls, a numpy array, or a numpy
    ///         masked array (masked = null).
    ///     value_type: The series' type. None asks the server once and
    ///         remembers it.
    ///
    /// Raises:
    ///     TsdbError: ERR_INVALID if value_type is not the series' type.
    #[pyo3(signature = (series, ts, values, *, value_type = None))]
    fn insert_series_values(
        &self,
        py: Python<'_>,
        series: u32,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
        value_type: Option<&str>,
    ) -> PyResult<()> {
        self.insert_typed(py, Some(series), None, ts, values, value_type, true)
    }

    /// Inserts values of any type into many series of that type and waits.
    ///
    /// As insert_series_values(), with a series id per row.
    #[pyo3(signature = (series, ts, values, *, value_type = None))]
    fn insert_values(
        &self,
        py: Python<'_>,
        series: &Bound<'_, PyAny>,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
        value_type: Option<&str>,
    ) -> PyResult<()> {
        self.insert_typed(py, None, Some(series), ts, values, value_type, true)
    }

    /// Sends values of any type into one series without waiting; see insert_series_async().
    #[pyo3(signature = (series, ts, values, *, value_type = None))]
    fn insert_series_values_async(
        &self,
        py: Python<'_>,
        series: u32,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
        value_type: Option<&str>,
    ) -> PyResult<()> {
        self.insert_typed(py, Some(series), None, ts, values, value_type, false)
    }

    /// Sends values of any type into many series without waiting; see insert_series_async().
    #[pyo3(signature = (series, ts, values, *, value_type = None))]
    fn insert_values_async(
        &self,
        py: Python<'_>,
        series: &Bound<'_, PyAny>,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
        value_type: Option<&str>,
    ) -> PyResult<()> {
        self.insert_typed(py, None, Some(series), ts, values, value_type, false)
    }

    /// Waits for the answers to all inserts sent without waiting.
    ///
    /// Raises:
    ///     TsdbError: The first error among them.
    fn wait(&self, py: Python<'_>) -> PyResult<()> {
        self.with(py, |c| c.wait())
    }

    /// Number of inserts sent but not yet answered.
    #[getter]
    fn pending(&self, py: Python<'_>) -> PyResult<usize> {
        self.with(py, |c| Ok(c.pending()))
    }

    /// Sets how much inserts are compressed (zstd level).
    ///
    /// Args:
    ///     level: 0 turns compression off; 1 (the default) is fast, higher
    ///         levels compress more, negative levels are faster still. Servers
    ///         older than protocol version 3 always get uncompressed inserts.
    ///
    /// Raises:
    ///     TsdbError: ERR_INVALID for a level zstd does not have.
    fn set_compression(&self, py: Python<'_>, level: i32) -> PyResult<()> {
        self.with(py, |c| c.set_compression(level))
    }

    /// The zstd level of inserts (0: off).
    #[getter]
    fn compression(&self, py: Python<'_>) -> PyResult<i32> {
        self.with(py, |c| Ok(c.compression()))
    }

    /// Bytes this client has written to the network so far.
    #[getter]
    fn bytes_sent(&self, py: Python<'_>) -> PyResult<u64> {
        self.with(py, |c| Ok(c.bytes_sent()))
    }

    /// Returns the raw rows of a float32 series.
    ///
    /// Args:
    ///     series: The series id.
    ///     start: First timestamp to include (ms); None for no lower limit.
    ///     end: Timestamp to stop before (ms); None for no upper limit.
    ///     order: "asc" (oldest first) or "desc" (newest first).
    ///     limit: At most this many rows; None for all, 0 for none.
    ///     numpy: True for numpy arrays, False for lists, None for numpy
    ///         arrays if numpy is installed.
    ///
    /// Returns:
    ///     (timestamps, values): int64 and float32 arrays, or lists of int and
    ///     float. Nulls read as NaN.
    ///
    /// Raises:
    ///     TsdbError: ERR_INVALID for a series that is not float32 (use
    ///         query_values()).
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
        let p = self.with(py, |c| c.query_points(series, range(start, end), o))?;
        points_to_py(py, p, numpy)
    }

    /// The same as query_raw() (its name in the Rust and C clients).
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

    /// Returns the raw rows of a series of any type, with nulls.
    ///
    /// Takes the same arguments as query_raw().
    ///
    /// Returns:
    ///     (timestamps, values). With numpy: an int64 array and a
    ///     numpy.ma.MaskedArray (masked = null) of float32, float64, int64,
    ///     bool or uint32 (state codes). Without: two lists, None for nulls.
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
        let rows = self.with(py, |c| c.query_rows(series, range(start, end), o))?;
        let np = numpy_module(py, numpy)?;
        PyTuple::new(py, [ts_to_py(py, np, rows.ts)?, column_to_py(py, np, rows.values)?])
    }

    /// Aggregates a series into buckets of a fixed length.
    ///
    /// Args:
    ///     series: The series id.
    ///     start: First timestamp to include (ms); None for no lower limit.
    ///     end: Timestamp to stop before (ms); None for no upper limit.
    ///     interval_ms: Bucket length in ms; buckets start at multiples of it
    ///         since the epoch. 0 gives one bucket for the whole range.
    ///     order: "asc" (oldest first) or "desc" (newest first).
    ///     limit: At most this many buckets; None for all, 0 for none.
    ///
    /// Returns:
    ///     Buckets: a sequence of Bucket with a numpy array per field
    ///     (b.counts, b.sum, ...); empty buckets are left out.
    ///
    /// Raises:
    ///     TsdbError: ERR_INVALID for a negative interval, ERR_TOO_MANY_BUCKETS
    ///         if the range holds too many buckets.
    #[pyo3(signature = (series, start = None, end = None, interval_ms = 0, *, order = "asc", limit = None))]
    #[allow(clippy::too_many_arguments)]
    fn query_agg(
        &self,
        py: Python<'_>,
        series: u32,
        start: Option<i64>,
        end: Option<i64>,
        interval_ms: i64,
        order: &str,
        limit: Option<u64>,
    ) -> PyResult<Buckets> {
        let o = opts(order, limit)?;
        self.with(py, |c| c.query_agg_ex(series, range(start, end), interval_ms, o).map(buckets))
    }

    /// Aggregates a series by calendar periods, such as days in a time zone.
    ///
    /// Days, weeks (from Monday), months, quarters and years follow the local
    /// calendar: a day across a daylight saving change lasts 23 or 25 hours.
    /// Seconds, minutes and hours have a fixed length. For a zone other than
    /// UTC the client computes where the local periods begin and sends those
    /// times (query_bounds()); a server older than protocol version 4 gets
    /// the zone itself.
    ///
    /// Args:
    ///     series: The series id.
    ///     start: First timestamp to include (ms); None for no lower limit.
    ///     end: Timestamp to stop before (ms); None for no upper limit.
    ///     unit: "second", "minute", "hour", "day", "week", "month",
    ///         "quarter" or "year"; None gives one bucket for the whole range.
    ///     count: Units per bucket, counted from 1970 (count=15 with
    ///         unit="minute" gives quarter hours).
    ///     timezone: "UTC" (or None), a fixed offset such as "+02:00", an
    ///         IANA name such as "Europe/Oslo", or a zoneinfo.ZoneInfo.
    ///     order: "asc" (oldest first) or "desc" (newest first).
    ///     limit: At most this many buckets; None for all.
    ///
    /// Returns:
    ///     Buckets (a sequence of Bucket, with numpy columns); empty buckets
    ///     are left out. A bucket's start is
    ///     the UTC time its local period begins.
    ///
    /// Raises:
    ///     TsdbError: ERR_INVALID for an unknown time zone.
    ///     ValueError: Unknown unit or order.
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
        let group = group(unit, count, timezone)?;
        self.with(py, |c| c.query_group(series, range(start, end), group.as_ref(), o).map(buckets))
    }

    /// Aggregates a series into buckets that start at the given times.
    ///
    /// Bucket i holds the rows from bounds[i] up to bounds[i + 1]; the last
    /// one holds the rows up to end. Rows before bounds[0] are left out.
    /// calendar_bounds() computes such bounds for local days, weeks, months,
    /// quarters or years; query_group() does both steps. Needs a server of
    /// protocol version 4.
    ///
    /// Args:
    ///     series: The series id.
    ///     bounds: Bucket starts, ms since the epoch, strictly increasing: a
    ///         list of ints or an int64 array.
    ///     start: First timestamp to include (ms); None for no lower limit.
    ///     end: Timestamp to stop before (ms); None for no upper limit.
    ///     order: "asc" (oldest first) or "desc" (newest first).
    ///     limit: At most this many buckets; None for all.
    ///
    /// Returns:
    ///     Buckets (a sequence of Bucket, with numpy columns); empty buckets
    ///     are left out.
    ///
    /// Raises:
    ///     TsdbError: ERR_INVALID if bounds is empty or does not increase, or
    ///         from a server older than version 4.
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
        self.with(py, |c| c.query_bounds(series, range(start, end), &bounds.snap(), o).map(buckets))
    }

    /// Count, sum, min and max like query_agg(), and the spread of the values
    /// in each bucket: range (max - min), population and sample variance and
    /// standard deviation (protocol version 8).
    ///
    /// Takes the arguments of query_agg(). The variance is computed from the
    /// granule statistics where whole granules fall in a bucket, so it costs
    /// about what query_agg() does.
    ///
    /// Returns:
    ///     Buckets with the columns range, var_pop, var_samp, stddev_pop and
    ///     stddev_samp beside the usual ones (and Bucket objects with those
    ///     fields). Variances are NaN for a bucket of nulls only, the sample
    ///     ones also for a single value.
    ///
    /// Raises:
    ///     TsdbError: ERR_INVALID for a negative interval or from a server
    ///         older than version 8 (the client does not send the request).
    #[pyo3(signature = (series, start = None, end = None, interval_ms = 0, *, order = "asc", limit = None))]
    #[allow(clippy::too_many_arguments)]
    fn query_spread(
        &self,
        py: Python<'_>,
        series: u32,
        start: Option<i64>,
        end: Option<i64>,
        interval_ms: i64,
        order: &str,
        limit: Option<u64>,
    ) -> PyResult<Buckets> {
        let o = opts(order, limit)?;
        self.with(py, |c| c.query_spread(series, range(start, end), interval_ms, o).map(Buckets::from))
    }

    /// The spread by calendar periods: query_group() with the columns of
    /// query_spread(). Takes the arguments of query_group().
    #[pyo3(signature = (series, start = None, end = None, *, unit = None, count = 1, timezone = None,
                        order = "asc", limit = None))]
    #[allow(clippy::too_many_arguments)]
    fn query_spread_group(
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
        let group = group(unit, count, timezone)?;
        self.with(py, |c| c.query_spread_group(series, range(start, end), group.as_ref(), o).map(Buckets::from))
    }

    /// The spread in buckets that start at the given times: query_bounds()
    /// with the columns of query_spread(). Takes the arguments of query_bounds().
    #[pyo3(signature = (series, bounds, start = None, end = None, *, order = "asc", limit = None))]
    #[allow(clippy::too_many_arguments)]
    fn query_spread_bounds(
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
        self.with(py, |c| c.query_spread_bounds(series, range(start, end), &bounds.snap(), o).map(Buckets::from))
    }

    /// Time-weighted averages: each reading weighted by how long it held
    /// (protocol version 6; order and limit need no newer server).
    ///
    /// A reading made once an hour counts as much as an hour of readings made
    /// every second. Use it for values sampled irregularly or on change.
    ///
    /// Args:
    ///     series: The series id.
    ///     start: First timestamp to include (ms); None for no lower limit.
    ///     end: Timestamp to stop before (ms); None for no upper limit.
    ///     interval_ms: Bucket length in ms; 0 gives one bucket for the range.
    ///     method: "step" (a value holds until the next reading) or "linear"
    ///         (a straight line between readings).
    ///     max_gap_ms: A gap between readings longer than this covers nothing
    ///         (the value is stale there); 0 for no limit.
    ///     order: "asc" (oldest first) or "desc" (newest first).
    ///     limit: At most this many buckets; None for all, 0 for none.
    ///
    /// Returns:
    ///     TwaBuckets: a sequence of TwaBucket with numpy columns start,
    ///     twa, area, covered_ms and counts.
    ///
    /// Raises:
    ///     TsdbError: ERR_INVALID for a negative interval or a server older
    ///         than version 6.
    ///     ValueError: Unknown method or order.
    #[pyo3(signature = (series, start = None, end = None, interval_ms = 0, *, method = "step", max_gap_ms = 0,
                        order = "asc", limit = None))]
    #[allow(clippy::too_many_arguments)]
    fn query_twa(
        &self,
        py: Python<'_>,
        series: u32,
        start: Option<i64>,
        end: Option<i64>,
        interval_ms: i64,
        method: &str,
        max_gap_ms: i64,
        order: &str,
        limit: Option<u64>,
    ) -> PyResult<TwaBuckets> {
        let o = twa_opts(method, max_gap_ms, order, limit)?;
        self.with(py, |c| c.query_twa(series, range(start, end), interval_ms, o).map(TwaBuckets::from))
    }

    /// Time-weighted averages by calendar periods (days in a time zone, ...).
    /// Takes the arguments of query_group() and method and max_gap_ms of query_twa().
    #[pyo3(signature = (series, start = None, end = None, *, unit = None, count = 1, timezone = None,
                        method = "step", max_gap_ms = 0, order = "asc", limit = None))]
    #[allow(clippy::too_many_arguments)]
    fn query_twa_group(
        &self,
        py: Python<'_>,
        series: u32,
        start: Option<i64>,
        end: Option<i64>,
        unit: Option<&str>,
        count: u32,
        timezone: Option<&Bound<'_, PyAny>>,
        method: &str,
        max_gap_ms: i64,
        order: &str,
        limit: Option<u64>,
    ) -> PyResult<TwaBuckets> {
        let o = twa_opts(method, max_gap_ms, order, limit)?;
        let group = group(unit, count, timezone)?;
        self.with(py, |c| c.query_twa_group(series, range(start, end), group.as_ref(), o).map(TwaBuckets::from))
    }

    /// Time-weighted averages in buckets that start at the given times.
    /// Takes the arguments of query_bounds() and method and max_gap_ms of query_twa().
    #[pyo3(signature = (series, bounds, start = None, end = None, *, method = "step", max_gap_ms = 0,
                        order = "asc", limit = None))]
    #[allow(clippy::too_many_arguments)]
    fn query_twa_bounds(
        &self,
        py: Python<'_>,
        series: u32,
        bounds: &Bound<'_, PyAny>,
        start: Option<i64>,
        end: Option<i64>,
        method: &str,
        max_gap_ms: i64,
        order: &str,
        limit: Option<u64>,
    ) -> PyResult<TwaBuckets> {
        let o = twa_opts(method, max_gap_ms, order, limit)?;
        let bounds = ts_column(bounds)?;
        self.with(py, |c| c.query_twa_bounds(series, range(start, end), &bounds.snap(), o).map(TwaBuckets::from))
    }

    /// Downsamples a series for a chart: at most threshold rows that keep the
    /// shape of the line (Largest-Triangle-Three-Buckets; protocol version 5).
    ///
    /// The first and last rows are always kept; a range of threshold rows or
    /// fewer comes back whole.
    ///
    /// Args:
    ///     series: The series id.
    ///     threshold: How many rows to keep at most; 0 keeps all of them.
    ///     start: First timestamp to include (ms); None for no lower limit.
    ///     end: Timestamp to stop before (ms); None for no upper limit.
    ///     order: "asc" (oldest first) or "desc" (newest first): the same
    ///         rows, in that order.
    ///     limit: At most this many of the chosen rows; None for all.
    ///     numpy: As for query_values().
    ///
    /// Returns:
    ///     (timestamps, values), as query_values() returns them.
    ///
    /// Raises:
    ///     TsdbError: ERR_INVALID from a server older than version 5, or
    ///         older than 9 with order="desc" or a limit.
    #[pyo3(signature = (series, threshold, start = None, end = None, *, order = "asc", limit = None, numpy = None))]
    #[allow(clippy::too_many_arguments)]
    fn query_lttb<'py>(
        &self,
        py: Python<'py>,
        series: u32,
        threshold: u64,
        start: Option<i64>,
        end: Option<i64>,
        order: &str,
        limit: Option<u64>,
        numpy: Option<bool>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let o = opts(order, limit)?;
        let rows = self.with(py, |c| c.query_lttb(series, range(start, end), threshold, o))?;
        rows_to_py(py, rows, numpy)
    }

    /// Deletes the rows of a series with start <= ts < end (protocol version 7).
    ///
    /// A delete is a write like any other: a row inserted after it returned
    /// is kept. start and end are required; None is unbounded, so
    /// delete_range(id, None, None) deletes every row of the series.
    ///
    /// Raises:
    ///     TsdbError: ERR_INVALID from a server older than version 7 (the
    ///         client asks HELLO first and does not send the request).
    #[pyo3(signature = (series, start, end))]
    fn delete_range(&self, py: Python<'_>, series: u32, start: Option<i64>, end: Option<i64>) -> PyResult<()> {
        self.with(py, |c| c.delete_range(series, range(start, end)))
    }

    /// Replaces the rows of a series with start <= ts < end by ts and values,
    /// atomically: a reader sees the old rows or the new ones, never a mix
    /// (protocol version 7).
    ///
    /// Args:
    ///     series: The series id.
    ///     start, end: The range; None is unbounded.
    ///     ts: Timestamps, ms since the epoch, each in the range.
    ///     values: As for insert_series_values(); no rows make it a delete.
    ///     value_type: The series' type. None asks the server once and
    ///         remembers it.
    ///
    /// Raises:
    ///     TsdbError: ERR_INVALID for a timestamp outside the range, a
    ///         value_type other than the series', rows that do not fit in
    ///         one request (256 MiB), or a server older than version 7.
    #[pyo3(signature = (series, start, end, ts, values, *, value_type = None))]
    #[allow(clippy::too_many_arguments)]
    fn replace_range(
        &self,
        py: Python<'_>,
        series: u32,
        start: Option<i64>,
        end: Option<i64>,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
        value_type: Option<&str>,
    ) -> PyResult<()> {
        self.replace_typed(py, series, start, end, ts, values, value_type)
    }

    /// Sets how long a series keeps its rows, in ms (protocol version 7):
    /// rows older than the server's clock minus limit_ms disappear from
    /// queries at once and from disk as merges get to them. 0 keeps them.
    ///
    /// Raises:
    ///     TsdbError: ERR_INVALID for a negative limit or a server older
    ///         than version 7.
    fn set_retention(&self, py: Python<'_>, series: u32, limit_ms: i64) -> PyResult<()> {
        self.with(py, |c| c.set_retention(series, limit_ms))
    }

    /// Returns the retention limit of a series in ms, 0 for none (protocol version 7).
    fn retention(&self, py: Python<'_>, series: u32) -> PyResult<i64> {
        self.with(py, |c| c.retention(series))
    }

    /// Makes everything written so far durable on the server's disk.
    fn commit(&self, py: Python<'_>) -> PyResult<()> {
        self.with(py, |c| c.commit())
    }

    /// Commits, then writes the rows the server holds in memory to parts on disk.
    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        self.with(py, |c| c.flush())
    }

    /// Merges the parts of every partition into one (slow on big databases).
    fn optimize(&self, py: Python<'_>) -> PyResult<()> {
        self.with(py, |c| c.optimize())
    }

    /// Returns database statistics (a Stats).
    fn stats(&self, py: Python<'_>) -> PyResult<Stats> {
        self.with(py, |c| c.stats()).map(Stats::from)
    }

    /// Returns what the server did for this connection's previous query (a QueryStats).
    ///
    /// Rows covered, decompressed or answered from block statistics, bytes
    /// and time. All zero before the first query.
    fn last_query_stats(&self, py: Python<'_>) -> PyResult<QueryStats> {
        self.with(py, |c| c.last_query_stats()).map(QueryStats::from)
    }
}

/// Returns the text for a TSDB_ERR_* code, such as ERR_NOTFOUND.
#[pyfunction]
fn strerror(code: i32) -> &'static str {
    tc::strerror(code)
}

/// Returns the UTC times at which local calendar periods begin.
///
/// These are the bucket starts query_group() uses for a time zone, and what
/// query_bounds() takes. The result is the same as the C client's
/// tsdb_calendar_bounds.
///
/// Args:
///     timezone: "UTC", a fixed offset such as "+02:00" or "-0530", an IANA
///         name such as "Europe/Oslo" (read from /usr/share/zoneinfo, or
///         $TZDIR), or a zoneinfo.ZoneInfo.
///     unit: "second", "minute", "hour", "day", "week", "month", "quarter"
///         or "year". Days and longer follow the local calendar; seconds to
///         hours have a fixed length, aligned in the zone's standard time.
///     start: Start of the range, ms since the epoch.
///     end: End of the range (not included), ms since the epoch.
///     count: Units per period, counted from 1970 in local time (0 counts
///         as 1).
///
/// Returns:
///     The start (ms, UTC) of every period that overlaps [start, end),
///     strictly increasing. A local midnight that happens twice begins its
///     day the first time; one that is skipped begins it when the clock
///     jumps.
///
/// Raises:
///     TsdbError: ERR_INVALID for an unknown time zone or more than 64 Mi
///         periods.
///     ValueError: Unknown unit.
#[pyfunction]
#[pyo3(signature = (timezone, unit, start, end, count = 1))]
fn calendar_bounds(
    py: Python<'_>,
    timezone: &Bound<'_, PyAny>,
    unit: &str,
    start: i64,
    end: i64,
    count: u32,
) -> PyResult<Vec<i64>> {
    let (zone, unit) = (zone_name(timezone)?, self::unit(unit)?);
    py.detach(|| tc::calendar_bounds(&zone, unit, count, start..end)).map_err(|e| to_pyerr(py, e))
}

/// Client for tsdbd, the TCP server of the tsdb time series database.
///
/// Connect with Client, create series, insert rows and read them back, raw
/// or aggregated into buckets. Timestamps are int milliseconds since
/// 1970-01-01 UTC. Time ranges are [start, end): start is included, end is
/// not, and None means no limit.
///
/// Example:
///     >>> from tsdb_client import Client
///     >>> with Client("127.0.0.1", 7070) as c:
///     ...     s = c.series("pump7.flow")
///     ...     c.insert_series(s, [1_700_000_000_000], [4.5])
///     ...     c.query_raw(s, numpy=False)
///     ([1700000000000], [4.5])
///
/// Errors are raised as TsdbError; its code is one of the ERR_* numbers.
#[pymodule]
fn tsdb_client(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Client>()?;
    m.add_class::<embedded::Db>()?;
    m.add_class::<pool::Pool>()?;
    m.add_class::<Bucket>()?;
    m.add_class::<Buckets>()?;
    m.add_class::<arrays::BucketsMany>()?;
    m.add_class::<arrays::TwaBucket>()?;
    m.add_class::<TwaBuckets>()?;
    // they are sequences, as the lists they replace were
    let seq = m.py().import("collections.abc")?.getattr("Sequence")?;
    seq.call_method1("register", (m.getattr("Buckets")?,))?;
    seq.call_method1("register", (m.getattr("BucketsMany")?,))?;
    seq.call_method1("register", (m.getattr("TwaBuckets")?,))?;
    m.add_class::<Stats>()?;
    m.add_class::<QueryStats>()?;
    m.add("TsdbError", m.py().get_type::<TsdbError>())?;
    m.add_function(wrap_pyfunction!(strerror, m)?)?;
    m.add_function(wrap_pyfunction!(calendar_bounds, m)?)?;
    m.add("DEFAULT_PORT", tc::DEFAULT_PORT)?;
    m.add("PROTOCOL_VERSION", tc::PROTOCOL_VERSION)?;
    m.add("DEFAULT_COMPRESSION", tc::DEFAULT_COMPRESSION)?;
    m.add("VALUE_TYPES", ValueType::ALL.map(ValueType::name).to_vec())?;
    for (name, s) in [
        ("ERR_IO", tc::Status::Io),
        ("ERR_NOMEM", tc::Status::NoMem),
        ("ERR_CORRUPT", tc::Status::Corrupt),
        ("ERR_INVALID", tc::Status::Invalid),
        ("ERR_LOCKED", tc::Status::Locked),
        ("ERR_NOTFOUND", tc::Status::NotFound),
        ("ERR_TOO_MANY_BUCKETS", tc::Status::TooManyBuckets),
        ("ERR_STOPPED", tc::Status::Stopped),
        ("ERR_READONLY", tc::Status::ReadOnly),
    ] {
        m.add(name, s.code())?;
    }
    Ok(())
}
