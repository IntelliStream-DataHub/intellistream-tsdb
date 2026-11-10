// SPDX-License-Identifier: Apache-2.0
//! The part of the Python bindings that `tsdb_client` (the network client, Apache-2.0) and `tsdb_embedded` (the
//! embedded engine, AGPL-3.0-only) share: converting columns and results between Python and the Rust client's
//! types, the multi-series and latest calls, and the result classes. No `#[pymodule]` lives here, so linking
//! this crate into an extension adds no `PyInit_*` symbol to it.
//!
//! Exceptions are imported from `tsdb_client` at the first error (`import_exception!`), never defined here: the
//! embedded extension raises the very classes the network client defines, so one `except tsdb_client.TsdbError`
//! catches both. The result classes are compiled into each extension and carry the extension's module name
//! (feature `embedded`: `tsdb_embedded`); they are distinct types, equal in what they offer.

use std::io;
use std::ops::Bound as Range;

use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyAttributeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use ::tsdb_client as tc;
use tc::{Column, ValueType, Values};

pub mod arrays;
pub mod latest;
pub mod multi;

pub use arrays::{Buckets, Col, SidCol, TwaBuckets, values_column, vec_to_py};

pyo3::import_exception!(tsdb_client, TsdbError);
pyo3::import_exception!(tsdb_client, TsdbUnavailable);
pyo3::import_exception!(tsdb_client, TsdbMoved);
pyo3::import_exception!(tsdb_client, TsdbPartial);
pyo3::import_exception!(tsdb_client, TsdbDropped);

pub fn to_pyerr(py: Python<'_>, e: tc::Error) -> PyErr {
    to_pyerr_info(py, e, None)
}

/// The error as its exception class: UNAVAILABLE, MOVED, PARTIAL and DROPPED have their own, with the body of the
/// reply (`info`, the client's last_error) as attributes.
pub fn to_pyerr_info(py: Python<'_>, e: tc::Error, info: Option<&tc::ErrorInfo>) -> PyErr {
    let status = e.status();
    let msg = e.to_string();
    let err = match status {
        Some(tc::Status::Unavailable) => TsdbUnavailable::new_err(msg),
        Some(tc::Status::Moved) => TsdbMoved::new_err(msg),
        Some(tc::Status::Partial) => TsdbPartial::new_err(msg),
        Some(tc::Status::Dropped) => TsdbDropped::new_err(msg),
        _ => TsdbError::new_err(msg),
    };
    let v = err.value(py);
    let attrs = || -> PyResult<()> {
        v.setattr("code", e.code())?;
        v.setattr("index", e.batch_index())?;
        v.setattr("result", py.None())?;
        let own = info.filter(|i| Some(i.status) == status.map(tc::Status::code));
        let empty = tc::ErrorInfo::default();
        let i = own.unwrap_or(&empty);
        match status {
            Some(tc::Status::Unavailable) => {
                v.setattr("has_body", own.is_some_and(|i| i.has_body))?;
                v.setattr("retry_after", f64::from(i.retry_after_ms()) / 1000.0)?;
                let l = PyList::empty(py);
                for u in &i.unavailable {
                    let d = PyDict::new(py);
                    d.set_item("volume", u.vol)?;
                    d.set_item("reason", u.reason)?;
                    d.set_item("epoch", u.epoch)?;
                    d.set_item("since_ms", u.since_ms)?;
                    d.set_item("retry_after", f64::from(u.retry_after_ms) / 1000.0)?;
                    d.set_item("outage_id", u.outage_id)?;
                    l.append(d)?;
                }
                v.setattr("entries", l)?;
            }
            Some(tc::Status::Moved) => {
                let l = PyList::empty(py);
                for m in &i.moved {
                    let d = PyDict::new(py);
                    d.set_item("volume", m.vol)?;
                    d.set_item("epoch", m.epoch)?;
                    d.set_item("node_id", m.node_id)?;
                    d.set_item("addr", &m.addr)?;
                    l.append(d)?;
                }
                v.setattr("moved", l)?;
            }
            Some(tc::Status::Partial) => {
                let vols: Vec<(u16, i16)> = i.volumes.iter().map(|v| (v.vol, v.status)).collect();
                v.setattr("volumes", vols)?;
                v.setattr("rows", i.rows)?;
                let rows: Vec<usize> = (0..i.rows as usize).filter(|&r| i.row_not_applied(r)).collect();
                v.setattr("not_applied", rows)?;
            }
            Some(tc::Status::Dropped) => {
                v.setattr("volume", i.dropped_vol)?;
                v.setattr("before_ms", i.dropped_before)?;
            }
            _ => {}
        }
        Ok(())
    };
    match attrs() {
        Ok(()) => err,
        Err(set) => set,
    }
}

pub fn closed() -> tc::Error {
    tc::Error::Io(io::Error::new(io::ErrorKind::NotConnected, "the client is closed"))
}

pub fn value_type(name: &str) -> PyResult<ValueType> {
    name.parse().map_err(|_| {
        PyValueError::new_err(format!("value type must be float32, float64, int64, bool or state, not {name:?}"))
    })
}

// ---------------------------------------------------------------- inputs

pub fn ts_column(obj: &Bound<'_, PyAny>) -> PyResult<Col<i64>> {
    Ok(match Col::buffer(obj)? {
        Some(c) => c,
        None => Col::Owned(obj.extract()?),
    })
}

/// Series ids: a uint64 buffer, an int64 buffer (numpy's default integer; a negative id raises ValueError), or a
/// sequence of ints (a negative one, or one of 2**64 or more, raises OverflowError). A 32-bit integer buffer
/// raises TypeError: every id is at least 2**32, so such a column holds truncated ids, and taking them would only
/// turn a type mistake into INVALID answers.
pub fn series_column(obj: &Bound<'_, PyAny>) -> PyResult<SidCol> {
    if let Some(c) = Col::<u64>::buffer(obj)? {
        return Ok(c);
    }
    if arrays::native::<u32>(obj).is_some() || arrays::native::<i32>(obj).is_some() {
        return Err(PyTypeError::new_err(
            "series ids are 64-bit (every id is at least 2**32): use a uint64 or int64 array, not a 32-bit one",
        ));
    }
    if let Some(c) = Col::<i64>::buffer(obj)? {
        // copied now, with the GIL held, so the check holds for the values the call uses (see arrays.rs)
        let v = c.snap();
        if let Some(&bad) = v.iter().find(|&&x| x < 0) {
            return Err(PyValueError::new_err(format!("a series id is never negative: {bad}")));
        }
        return Ok(Col::Owned(v.iter().map(|&x| x as u64).collect()));
    }
    Ok(Col::Owned(obj.extract()?))
}

/// float32 values of the version 1 calls: float32 or float64 buffers, or a sequence.
pub fn f32_column(obj: &Bound<'_, PyAny>) -> PyResult<Col<f32>> {
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
pub fn numpy_bools(obj: &Bound<'_, PyAny>) -> PyResult<Option<Vec<bool>>> {
    if !obj.hasattr("dtype")? || obj.getattr("dtype")?.getattr("kind")?.extract::<String>()? != "b" {
        return Ok(None);
    }
    let bytes = obj.call_method1("view", ("u1",))?;
    Ok(Some(PyBuffer::<u8>::get(&bytes)?.to_vec(obj.py())?.into_iter().map(|b| b != 0).collect()))
}

/// A sequence with `None` for nulls.
pub fn seq_column<T: tc::Native>(
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
pub fn py_column(obj: &Bound<'_, PyAny>, ty: ValueType) -> PyResult<Column> {
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

pub fn plain_column(obj: &Bound<'_, PyAny>, ty: ValueType) -> PyResult<Column> {
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
pub fn numpy_module(py: Python<'_>, numpy: Option<bool>) -> PyResult<bool> {
    arrays::use_numpy(py, numpy)
}

pub fn ts_to_py(py: Python<'_>, np: bool, ts: Vec<i64>) -> PyResult<Bound<'_, PyAny>> {
    vec_to_py(py, np, ts)
}

/// `(timestamps, values)` of a float32 query, as numpy arrays or lists.
pub fn points_to_py(py: Python<'_>, p: tc::Points, numpy: Option<bool>) -> PyResult<Bound<'_, PyTuple>> {
    let np = numpy_module(py, numpy)?;
    PyTuple::new(py, [ts_to_py(py, np, p.ts)?, vec_to_py(py, np, p.values)?])
}

/// A typed column: a numpy masked array (dtype per type, masked = null) or a list with `None`.
pub fn column_to_py(py: Python<'_>, np: bool, col: Column) -> PyResult<Bound<'_, PyAny>> {
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

pub fn range(start: Option<i64>, end: Option<i64>) -> (Range<i64>, Range<i64>) {
    (start.map_or(Range::Unbounded, Range::Included), end.map_or(Range::Unbounded, Range::Excluded))
}

pub fn opts(order: &str, limit: Option<u64>) -> PyResult<tc::QueryOpts> {
    let order = match order {
        "asc" => tc::Order::Asc,
        "desc" => tc::Order::Desc,
        o => return Err(PyValueError::new_err(format!("order must be 'asc' or 'desc', not {o:?}"))),
    };
    Ok(tc::QueryOpts { order, limit })
}

pub fn twa_opts(method: &str, max_gap_ms: i64, order: &str, limit: Option<u64>) -> PyResult<tc::TwaOpts> {
    let method = match method {
        "step" => tc::TwaMethod::Step,
        "linear" => tc::TwaMethod::Linear,
        m => return Err(PyValueError::new_err(format!("method must be 'step' or 'linear', not {m:?}"))),
    };
    let o = opts(order, limit)?;
    Ok(tc::TwaOpts { method, max_gap_ms, order: o.order, limit: o.limit })
}

/// The calendar grouping of query_group() and friends: None for one bucket over the range.
pub fn group(unit: Option<&str>, count: u32, timezone: Option<&Bound<'_, PyAny>>) -> PyResult<Option<tc::Group>> {
    let timezone = timezone.map(zone_name).transpose()?;
    Ok(unit.map(self::unit).transpose()?.map(|u| tc::Group { unit: u, count, timezone }))
}

pub fn rows_to_py<'py>(py: Python<'py>, rows: tc::Rows, numpy: Option<bool>) -> PyResult<Bound<'py, PyTuple>> {
    let np = numpy_module(py, numpy)?;
    PyTuple::new(py, [ts_to_py(py, np, rows.ts)?, column_to_py(py, np, rows.values)?])
}

pub fn unit(name: &str) -> PyResult<tc::Unit> {
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
pub fn zone_name(obj: &Bound<'_, PyAny>) -> PyResult<String> {
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
#[cfg_attr(not(feature = "embedded"), pyclass(frozen, module = "tsdb_client"))]
#[cfg_attr(feature = "embedded", pyclass(frozen, module = "tsdb_embedded"))]
pub struct Bucket {
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
        Bucket { spread: Some([s.range, s.var_pop, s.var_samp, s.stddev_pop, s.stddev_samp]), ..Bucket::from(s.bucket) }
    }
}

/// Buckets, a column per field (built with the GIL released).
pub fn buckets(v: Vec<tc::Bucket>) -> Buckets {
    Buckets::from(v)
}

/// Database statistics, as returned by Client.stats().
#[cfg_attr(not(feature = "embedded"), pyclass(frozen, module = "tsdb_client"))]
#[cfg_attr(feature = "embedded", pyclass(frozen, module = "tsdb_embedded"))]
pub struct Stats {
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
#[cfg_attr(not(feature = "embedded"), pyclass(frozen, module = "tsdb_client"))]
#[cfg_attr(feature = "embedded", pyclass(frozen, module = "tsdb_embedded"))]
pub struct QueryStats {
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

/// Adds the result classes to a module and registers the sequence types with `collections.abc.Sequence`
/// (they are sequences, as the lists they replace were).
pub fn add_result_classes(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Bucket>()?;
    m.add_class::<Buckets>()?;
    m.add_class::<arrays::BucketsMany>()?;
    m.add_class::<multi::Many>()?;
    m.add_class::<latest::LatestMany>()?;
    m.add_class::<arrays::TwaBucket>()?;
    m.add_class::<TwaBuckets>()?;
    let seq = m.py().import("collections.abc")?.getattr("Sequence")?;
    seq.call_method1("register", (m.getattr("Buckets")?,))?;
    seq.call_method1("register", (m.getattr("BucketsMany")?,))?;
    seq.call_method1("register", (m.getattr("TwaBuckets")?,))?;
    m.add_class::<Stats>()?;
    m.add_class::<QueryStats>()?;
    Ok(())
}
