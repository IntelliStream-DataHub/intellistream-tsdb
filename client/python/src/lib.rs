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
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyList, PyTuple};

use ::tsdb_client as tc;
use tc::{Column, ValueType, Values};

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

fn ts_column(obj: &Bound<'_, PyAny>) -> PyResult<Vec<i64>> {
    match PyBuffer::<i64>::get(obj) {
        Ok(b) => b.to_vec(obj.py()),
        Err(_) => obj.extract(),
    }
}

fn series_column(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u32>> {
    match PyBuffer::<u32>::get(obj) {
        Ok(b) => b.to_vec(obj.py()),
        Err(_) => obj.extract(),
    }
}

/// float32 values of the version 1 calls: float32 or float64 buffers, or a sequence.
fn f32_column(obj: &Bound<'_, PyAny>) -> PyResult<Vec<f32>> {
    if let Ok(b) = PyBuffer::<f32>::get(obj) {
        return b.to_vec(obj.py());
    }
    if let Ok(b) = PyBuffer::<f64>::get(obj) {
        return Ok(b.to_vec(obj.py())?.into_iter().map(|v| v as f32).collect());
    }
    Ok(obj.extract::<Vec<f64>>()?.into_iter().map(|v| v as f32).collect())
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
        ValueType::Float32 => match PyBuffer::<f32>::get(obj) {
            Ok(b) => Column::new(b.to_vec(py)?),
            Err(_) => match PyBuffer::<f64>::get(obj) {
                Ok(b) => Column::new(b.to_vec(py)?.into_iter().map(|v| v as f32).collect()),
                Err(_) => seq_column(obj, |x| Ok(x.extract::<f64>()? as f32))?,
            },
        },
        ValueType::Float64 => match PyBuffer::<f64>::get(obj) {
            Ok(b) => Column::new(b.to_vec(py)?),
            Err(_) => match PyBuffer::<f32>::get(obj) {
                Ok(b) => Column::new(b.to_vec(py)?.into_iter().map(f64::from).collect()),
                Err(_) => seq_column(obj, |x| x.extract::<f64>())?,
            },
        },
        ValueType::Int64 => match PyBuffer::<i64>::get(obj) {
            Ok(b) => Column::new(b.to_vec(py)?),
            Err(_) => match PyBuffer::<i32>::get(obj) {
                Ok(b) => Column::new(b.to_vec(py)?.into_iter().map(i64::from).collect()),
                Err(_) => seq_column(obj, |x| x.extract::<i64>())?,
            },
        },
        ValueType::Bool => match numpy_bools(obj)? {
            Some(v) => Column::new(v),
            None => seq_column(obj, |x| x.extract::<bool>())?,
        },
        ValueType::State => match PyBuffer::<u32>::get(obj) {
            Ok(b) => Column::new(b.to_vec(py)?),
            Err(_) => seq_column(obj, |x| x.extract::<u32>())?,
        },
        t => return Err(PyValueError::new_err(format!("value type {t} is not supported by this binding"))),
    })
}

// --------------------------------------------------------------- outputs
//
// numpy arrays are built over a bytearray filled in place (one copy), with
// the dtype of each value type; lists hold Python ints, floats and bools.

fn le_bytes<'py, T: Copy, const W: usize>(
    py: Python<'py>,
    v: &[T],
    f: impl Fn(T) -> [u8; W],
) -> PyResult<Bound<'py, PyByteArray>> {
    PyByteArray::new_with(py, v.len() * W, |b| {
        for (d, &x) in b.chunks_exact_mut(W).zip(v) {
            d.copy_from_slice(&f(x));
        }
        Ok(())
    })
}

fn array<'py, T: Copy, const W: usize>(
    np: &Bound<'py, PyModule>,
    v: &[T],
    f: impl Fn(T) -> [u8; W],
    dtype: &str,
) -> PyResult<Bound<'py, PyAny>> {
    np.call_method1("frombuffer", (le_bytes(np.py(), v, f)?, dtype))
}

/// The numpy module: required (`Some(true)`), refused (`Some(false)`) or if importable (`None`).
fn numpy_module(py: Python<'_>, numpy: Option<bool>) -> PyResult<Option<Bound<'_, PyModule>>> {
    Ok(match numpy {
        Some(false) => None,
        Some(true) => Some(py.import("numpy")?),
        None => py.import("numpy").ok(),
    })
}

fn ts_to_py<'py>(py: Python<'py>, np: Option<&Bound<'py, PyModule>>, ts: &[i64]) -> PyResult<Bound<'py, PyAny>> {
    match np {
        Some(np) => array(np, ts, i64::to_le_bytes, "<i8"),
        None => Ok(PyList::new(py, ts)?.into_any()),
    }
}

/// `(timestamps, values)` of a float32 query, as numpy arrays or lists.
fn points_to_py(py: Python<'_>, p: tc::Points, numpy: Option<bool>) -> PyResult<Bound<'_, PyTuple>> {
    let np = numpy_module(py, numpy)?;
    let values = match &np {
        Some(np) => array(np, &p.values, f32::to_le_bytes, "<f4")?,
        None => PyList::new(py, p.values)?.into_any(),
    };
    PyTuple::new(py, [ts_to_py(py, np.as_ref(), &p.ts)?, values])
}

/// A typed column: a numpy masked array (dtype per type, masked = null) or a list with `None`.
fn column_to_py<'py>(py: Python<'py>, np: Option<&Bound<'py, PyModule>>, col: &Column) -> PyResult<Bound<'py, PyAny>> {
    let Some(np) = np else {
        let list = match &col.values {
            Values::Float32(_) => PyList::new(py, col.to_options::<f32>().unwrap_or_default())?,
            Values::Float64(_) => PyList::new(py, col.to_options::<f64>().unwrap_or_default())?,
            Values::Int64(_) => PyList::new(py, col.to_options::<i64>().unwrap_or_default())?,
            Values::Bool(_) => PyList::new(py, col.to_options::<bool>().unwrap_or_default())?,
            Values::State(_) => PyList::new(py, col.to_options::<u32>().unwrap_or_default())?,
            _ => return Err(PyValueError::new_err("a value type this binding does not support")),
        };
        return Ok(list.into_any());
    };
    let data = match &col.values {
        Values::Float32(v) => array(np, v, f32::to_le_bytes, "<f4")?,
        Values::Float64(v) => array(np, v, f64::to_le_bytes, "<f8")?,
        Values::Int64(v) => array(np, v, i64::to_le_bytes, "<i8")?,
        Values::Bool(v) => array(np, v, |b| [b as u8], "?")?,
        Values::State(v) => array(np, v, u32::to_le_bytes, "<u4")?,
        _ => return Err(PyValueError::new_err("a value type this binding does not support")),
    };
    let mask = match &col.valid {
        Some(valid) => array(np, &valid.iter().map(|known| !known).collect::<Vec<_>>(), |b| [b as u8], "?")?,
        None => false.into_pyobject(py)?.to_owned().into_any(),
    };
    np.getattr("ma")?.call_method1("MaskedArray", (data, mask))
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

// --------------------------------------------------------------- results

/// One aggregate bucket.
#[pyclass(frozen, get_all, module = "tsdb_client")]
struct Bucket {
    /// Bucket start, ms since the epoch.
    start: i64,
    /// Number of known values.
    count: u64,
    /// Sum of the values.
    sum: f64,
    /// Smallest value.
    min: f64,
    /// Largest value.
    max: f64,
    /// Exact sum (int64 and bool series).
    isum: i64,
    /// Exact minimum (int64 and bool series).
    imin: i64,
    /// Exact maximum (int64 and bool series).
    imax: i64,
}

#[pymethods]
impl Bucket {
    /// The mean, sum / count (0 for an empty bucket).
    #[getter]
    fn avg(&self) -> f64 {
        if self.count == 0 { 0.0 } else { self.sum / self.count as f64 }
    }

    fn __repr__(&self) -> String {
        format!(
            "Bucket(start={}, count={}, sum={:?}, min={:?}, max={:?}, isum={}, imin={}, imax={})",
            self.start, self.count, self.sum, self.min, self.max, self.isum, self.imin, self.imax
        )
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        other.cast::<Bucket>().is_ok_and(|o| {
            let o = o.get();
            (o.start, o.count, o.isum, o.imin, o.imax) == (self.start, self.count, self.isum, self.imin, self.imax)
                && (o.sum.to_bits(), o.min.to_bits(), o.max.to_bits())
                    == (self.sum.to_bits(), self.min.to_bits(), self.max.to_bits())
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
        }
    }
}

fn buckets(v: Vec<tc::Bucket>) -> Vec<Bucket> {
    v.into_iter().map(Bucket::from).collect()
}

/// Database statistics.
#[pyclass(frozen, get_all, module = "tsdb_client")]
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

/// A connection to tsdbd.
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
        py.detach(|| f(&mut self.inner.lock().unwrap_or_else(|e| e.into_inner()))).map_err(|e| to_pyerr(py, e))
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
        let ty = match (value_type, one.or_else(|| many.as_ref().and_then(|s| s.first().copied()))) {
            (Some(t), _) => self::value_type(t)?,
            (None, Some(id)) => self.with_inner(py, |i| i.series_type(id))?,
            (None, None) => ValueType::Float32,
        };
        let col = py_column(values, ty)?;
        self.with(py, |c| match (&many, one) {
            (Some(s), _) if wait => c.insert_values(s, &ts, &col),
            (Some(s), _) => c.insert_values_async(s, &ts, &col),
            (None, Some(id)) if wait => c.insert_series_values(id, &ts, &col),
            (None, Some(id)) => c.insert_series_values_async(id, &ts, &col),
            (None, None) => unreachable!("one of the two is given"),
        })
    }
}

fn connect(host: &str, port: u16, timeout: Option<Duration>) -> tc::Result<tc::Client> {
    use std::net::ToSocketAddrs;
    let Some(t) = timeout else {
        return tc::Client::connect((host, port));
    };
    let mut last = closed();
    for addr in (host, port).to_socket_addrs()? {
        match tc::Client::connect_timeout(&addr, t) {
            Ok(c) => {
                c.set_timeout(Some(t))?;
                return Ok(c);
            }
            Err(e) => last = e,
        }
    }
    Err(last)
}

#[pymethods]
impl Client {
    /// Connects to tsdbd. `timeout` (seconds) bounds the connect and every
    /// later socket read or write; a call that times out closes the client.
    #[new]
    #[pyo3(signature = (host = "127.0.0.1", port = 7070, timeout = None))]
    fn new(py: Python<'_>, host: &str, port: u16, timeout: Option<f64>) -> PyResult<Self> {
        let timeout = match timeout {
            Some(t) if !(t > 0.0 && t.is_finite()) => {
                return Err(PyValueError::new_err("timeout must be a positive number of seconds"));
            }
            t => t.map(Duration::from_secs_f64),
        };
        let c = py.detach(|| connect(host, port, timeout)).map_err(|e| to_pyerr(py, e))?;
        Ok(Client { inner: Mutex::new(Inner { conn: Some(c), ..Inner::default() }), addr: format!("{host}:{port}") })
    }

    /// Waits for pending inserts, then closes the connection. Closing twice is fine.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        self.with_inner(py, |i| i.conn.take().map_or(Ok(()), tc::Client::close))
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    /// Closes the client: waiting for pending inserts if the block succeeded,
    /// dropping the connection at once if it raised.
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

    /// The server's protocol version.
    fn protocol_version(&self, py: Python<'_>) -> PyResult<u32> {
        self.with(py, |c| c.protocol_version())
    }

    /// The id of series `name`: created (as float32) if needed when `create`,
    /// else TsdbError (ERR_NOTFOUND) if it does not exist.
    #[pyo3(signature = (name, create = true))]
    fn series(&self, py: Python<'_>, name: &str, create: bool) -> PyResult<u32> {
        self.with(py, |c| if create { c.series(name) } else { c.series_lookup(name) })
    }

    /// The id of series `name`, created (as float32) in the volume `path` on the server if needed.
    fn series_at(&self, py: Python<'_>, name: &str, path: &str) -> PyResult<u32> {
        self.with(py, |c| c.series_at(name, path))
    }

    /// The id of series `name` of `value_type`, created if needed (in volume
    /// `path`, or the database directory). An existing series of another type
    /// is TsdbError (ERR_INVALID).
    #[pyo3(signature = (name, value_type = "float32", path = None))]
    fn series_create(&self, py: Python<'_>, name: &str, value_type: &str, path: Option<&str>) -> PyResult<u32> {
        let ty = self::value_type(value_type)?;
        self.with_inner(py, |i| {
            let id = i.conn()?.series_create(name, ty, path)?;
            i.types.insert(id, ty);
            Ok(id)
        })
    }

    /// The value type of series `id`: "float32", "float64", "int64", "bool" or "state".
    fn series_type(&self, py: Python<'_>, id: u32) -> PyResult<&'static str> {
        self.with_inner(py, |i| i.series_type(id)).map(ValueType::name)
    }

    /// The name of series `id`.
    fn series_name(&self, py: Python<'_>, id: u32) -> PyResult<String> {
        self.with(py, |c| c.series_name(id))
    }

    /// The code of state `name` for `series` (its volume's dictionary), created if needed.
    fn state(&self, py: Python<'_>, series: u32, name: &str) -> PyResult<u32> {
        self.with(py, |c| c.state(series, name))
    }

    /// The code of an existing state; TsdbError (ERR_NOTFOUND) if there is none.
    fn state_lookup(&self, py: Python<'_>, series: u32, name: &str) -> PyResult<u32> {
        self.with(py, |c| c.state_lookup(series, name))
    }

    /// The name of state `code`; TsdbError (ERR_NOTFOUND) for an unknown code.
    fn state_name(&self, py: Python<'_>, series: u32, code: u32) -> PyResult<String> {
        self.with_inner(py, |i| i.state_name(series, code))
    }

    /// The names of state codes (a list, array or masked array, as returned by
    /// query_values); None and masked entries give None. Names are cached.
    fn state_names(&self, py: Python<'_>, series: u32, codes: &Bound<'_, PyAny>) -> PyResult<Vec<Option<String>>> {
        let col = py_column(codes, ValueType::State)?;
        self.with_inner(py, |i| {
            col.iter::<u32>()
                .expect("a state column")
                .map(|code| code.map(|k| i.state_name(series, k)).transpose())
                .collect()
        })
    }

    /// Inserts float32 rows of one series and waits until the server has applied them.
    fn insert_series(
        &self,
        py: Python<'_>,
        series: u32,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let (ts, values) = (ts_column(ts)?, f32_column(values)?);
        self.with(py, |c| c.insert_series(series, &ts, &values))
    }

    /// Inserts float32 rows of many series (`series[i]` is the series of row i) and waits.
    fn insert(
        &self,
        py: Python<'_>,
        series: &Bound<'_, PyAny>,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let (series, ts, values) = (series_column(series)?, ts_column(ts)?, f32_column(values)?);
        self.with(py, |c| c.insert(&series, &ts, &values))
    }

    /// Sends float32 rows of one series without waiting; errors come from
    /// wait() or from the next call that reads a reply.
    fn insert_series_async(
        &self,
        py: Python<'_>,
        series: u32,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let (ts, values) = (ts_column(ts)?, f32_column(values)?);
        self.with(py, |c| c.insert_series_async(series, &ts, &values))
    }

    /// Sends float32 rows of many series without waiting.
    fn insert_async(
        &self,
        py: Python<'_>,
        series: &Bound<'_, PyAny>,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let (series, ts, values) = (series_column(series)?, ts_column(ts)?, f32_column(values)?);
        self.with(py, |c| c.insert_async(&series, &ts, &values))
    }

    /// Inserts typed values (None or masked = null) into one series and
    /// waits. `value_type` defaults to the series' type (asked once, then cached).
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

    /// Inserts typed values into many series of one type and waits.
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

    /// Sends typed values for one series without waiting.
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

    /// Sends typed values for many series without waiting.
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

    /// Collects the acknowledgements of pending inserts; raises the first error.
    fn wait(&self, py: Python<'_>) -> PyResult<()> {
        self.with(py, |c| c.wait())
    }

    /// Inserts sent but not yet acknowledged.
    #[getter]
    fn pending(&self, py: Python<'_>) -> PyResult<usize> {
        self.with(py, |c| Ok(c.pending()))
    }

    /// Raw points of a float32 `series` in [start, end) as (timestamps,
    /// values): numpy arrays (int64, float32) if numpy is importable and
    /// `numpy` is not False, else lists. Nulls read as NaN.
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

    /// Same as query_raw (the collecting call is query_points in the Rust and C clients).
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

    /// Rows of a series of any type as (timestamps, values). With numpy: an
    /// int64 array and a numpy.ma.MaskedArray (float32, float64, int64, bool
    /// or uint32 state codes; masked = null). Without: lists, None for nulls.
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
        PyTuple::new(py, [ts_to_py(py, np.as_ref(), &rows.ts)?, column_to_py(py, np.as_ref(), &rows.values)?])
    }

    /// Buckets of `interval_ms` aligned to the epoch (0: one bucket for the range).
    #[pyo3(signature = (series, start = None, end = None, interval_ms = 0))]
    fn query_agg(
        &self,
        py: Python<'_>,
        series: u32,
        start: Option<i64>,
        end: Option<i64>,
        interval_ms: i64,
    ) -> PyResult<Vec<Bucket>> {
        self.with(py, |c| c.query_agg(series, range(start, end), interval_ms)).map(buckets)
    }

    /// Buckets of `count` calendar `unit`s in `timezone` (None: UTC); unit
    /// None gives one bucket for the whole range.
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
        timezone: Option<String>,
        order: &str,
        limit: Option<u64>,
    ) -> PyResult<Vec<Bucket>> {
        let o = opts(order, limit)?;
        let group = unit.map(self::unit).transpose()?.map(|u| tc::Group { unit: u, count, timezone });
        self.with(py, |c| c.query_group(series, range(start, end), group.as_ref(), o)).map(buckets)
    }

    /// Makes everything written so far durable.
    fn commit(&self, py: Python<'_>) -> PyResult<()> {
        self.with(py, |c| c.commit())
    }

    /// Commits, then turns the server's memtable into parts.
    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        self.with(py, |c| c.flush())
    }

    /// Merges every partition down to one part.
    fn optimize(&self, py: Python<'_>) -> PyResult<()> {
        self.with(py, |c| c.optimize())
    }

    /// Database statistics.
    fn stats(&self, py: Python<'_>) -> PyResult<Stats> {
        self.with(py, |c| c.stats()).map(Stats::from)
    }
}

/// The text for a TSDB_ERR_* code.
#[pyfunction]
fn strerror(code: i32) -> &'static str {
    tc::strerror(code)
}

#[pymodule]
fn tsdb_client(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Client>()?;
    m.add_class::<Bucket>()?;
    m.add_class::<Stats>()?;
    m.add("TsdbError", m.py().get_type::<TsdbError>())?;
    m.add_function(wrap_pyfunction!(strerror, m)?)?;
    m.add("DEFAULT_PORT", tc::DEFAULT_PORT)?;
    m.add("PROTOCOL_VERSION", tc::PROTOCOL_VERSION)?;
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
