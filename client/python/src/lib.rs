//! Python binding for the tsdbd client (`client/rust`), built with maturin.
//!
//! Arguments are converted to Rust vectors while the GIL is held, and the
//! network call then runs detached from the interpreter (`Python::detach`, the
//! PyO3 0.26+ name of `allow_threads`), so other Python threads keep running.
//! The connection sits behind a mutex taken after detaching: threads sharing
//! one `Client` take turns without holding the GIL while they wait.
//!
//! Columns are accepted as anything exporting the buffer protocol with the
//! right element type (numpy arrays, `array.array`) or as sequences. Raw query
//! results come back as numpy arrays when numpy can be imported, else as lists.

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

// ---------------------------------------------------------------- values
//
// The value type crosses into Python only here: as a float, and as the numpy
// dtype of raw query results.

/// numpy dtype of `tc::Value` on the wire.
const VALUE_DTYPE: &str = "<f4";

fn value_column(obj: &Bound<'_, PyAny>) -> PyResult<Vec<tc::Value>> {
    if let Ok(b) = PyBuffer::<tc::Value>::get(obj) {
        return b.to_vec(obj.py());
    }
    if let Ok(b) = PyBuffer::<f64>::get(obj) {
        return Ok(b.to_vec(obj.py())?.into_iter().map(|v| v as tc::Value).collect());
    }
    Ok(obj.extract::<Vec<f64>>()?.into_iter().map(|v| v as tc::Value).collect())
}

fn values_to_numpy<'py>(np: &Bound<'py, PyModule>, v: &[tc::Value]) -> PyResult<Bound<'py, PyAny>> {
    let bytes = PyByteArray::new_with(np.py(), std::mem::size_of_val(v), |b| {
        for (d, x) in b.chunks_exact_mut(size_of::<tc::Value>()).zip(v) {
            d.copy_from_slice(&x.to_le_bytes());
        }
        Ok(())
    })?;
    np.call_method1("frombuffer", (bytes, VALUE_DTYPE))
}

// --------------------------------------------------------------- columns

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

fn ts_to_numpy<'py>(np: &Bound<'py, PyModule>, ts: &[i64]) -> PyResult<Bound<'py, PyAny>> {
    let bytes = PyByteArray::new_with(np.py(), ts.len() * 8, |b| {
        for (d, x) in b.chunks_exact_mut(8).zip(ts) {
            d.copy_from_slice(&x.to_le_bytes());
        }
        Ok(())
    })?;
    np.call_method1("frombuffer", (bytes, "<i8"))
}

/// `(timestamps, values)` as numpy arrays or lists.
fn columns<'py>(py: Python<'py>, p: tc::Points, numpy: Option<bool>) -> PyResult<Bound<'py, PyTuple>> {
    let np = match numpy {
        Some(false) => None,
        Some(true) => Some(py.import("numpy")?),
        None => py.import("numpy").ok(),
    };
    match np {
        Some(np) => PyTuple::new(py, [ts_to_numpy(&np, &p.ts)?, values_to_numpy(&np, &p.values)?]),
        None => PyTuple::new(py, [PyList::new(py, p.ts)?.into_any(), PyList::new(py, p.values)?.into_any()]),
    }
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
    /// Number of points.
    count: u64,
    /// Sum of the values.
    sum: f64,
    /// Smallest value.
    min: f64,
    /// Largest value.
    max: f64,
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
            "Bucket(start={}, count={}, sum={:?}, min={:?}, max={:?})",
            self.start, self.count, self.sum, self.min, self.max
        )
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        other.cast::<Bucket>().is_ok_and(|o| {
            let o = o.get();
            (o.start, o.count) == (self.start, self.count)
                && o.sum.to_bits() == self.sum.to_bits()
                && (o.min.to_bits(), o.max.to_bits()) == (self.min.to_bits(), self.max.to_bits())
        })
    }
}

impl From<tc::Bucket> for Bucket {
    fn from(b: tc::Bucket) -> Self {
        Bucket { start: b.start, count: b.count, sum: b.sum, min: b.min.into(), max: b.max.into() }
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

/// A connection to tsdbd.
#[pyclass(module = "tsdb_client")]
struct Client {
    inner: Mutex<Option<tc::Client>>,
    addr: String,
}

impl Client {
    /// Runs `f` on the connection with the GIL released.
    fn with<T, F>(&self, py: Python<'_>, f: F) -> PyResult<T>
    where
        T: Send,
        F: FnOnce(&mut tc::Client) -> tc::Result<T> + Send,
    {
        py.detach(|| {
            let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            g.as_mut().map_or_else(|| Err(closed()), f)
        })
        .map_err(|e| to_pyerr(py, e))
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
        Ok(Client { inner: Mutex::new(Some(c)), addr: format!("{host}:{port}") })
    }

    /// Waits for pending inserts, then closes the connection. Closing twice is fine.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            let c = self.inner.lock().unwrap_or_else(|e| e.into_inner()).take();
            c.map_or(Ok(()), tc::Client::close)
        })
        .map_err(|e| to_pyerr(py, e))
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
            py.detach(|| drop(self.inner.lock().unwrap_or_else(|e| e.into_inner()).take()));
        } else {
            self.close(py)?;
        }
        Ok(false)
    }

    fn __repr__(&self) -> String {
        let open = self.inner.lock().map(|g| g.is_some()).unwrap_or(false);
        format!("<tsdb_client.Client {} {}>", self.addr, if open { "open" } else { "closed" })
    }

    /// The server's protocol version.
    fn protocol_version(&self, py: Python<'_>) -> PyResult<u32> {
        self.with(py, |c| c.protocol_version())
    }

    /// The id of series `name`: created if needed when `create`, else
    /// TsdbError (ERR_NOTFOUND) if it does not exist.
    #[pyo3(signature = (name, create = true))]
    fn series(&self, py: Python<'_>, name: &str, create: bool) -> PyResult<u32> {
        self.with(py, |c| if create { c.series(name) } else { c.series_lookup(name) })
    }

    /// The id of series `name`, created in the volume `path` on the server if needed.
    fn series_at(&self, py: Python<'_>, name: &str, path: &str) -> PyResult<u32> {
        self.with(py, |c| c.series_at(name, path))
    }

    /// The name of series `id`.
    fn series_name(&self, py: Python<'_>, id: u32) -> PyResult<String> {
        self.with(py, |c| c.series_name(id))
    }

    /// Inserts rows of one series and waits until the server has applied them.
    fn insert_series(
        &self,
        py: Python<'_>,
        series: u32,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let (ts, values) = (ts_column(ts)?, value_column(values)?);
        self.with(py, |c| c.insert_series(series, &ts, &values))
    }

    /// Inserts rows of many series (`series[i]` is the series of row i) and waits.
    fn insert(
        &self,
        py: Python<'_>,
        series: &Bound<'_, PyAny>,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let (series, ts, values) = (series_column(series)?, ts_column(ts)?, value_column(values)?);
        self.with(py, |c| c.insert(&series, &ts, &values))
    }

    /// Sends an insert of one series without waiting; errors come from wait()
    /// or from the next call that reads a reply.
    fn insert_series_async(
        &self,
        py: Python<'_>,
        series: u32,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let (ts, values) = (ts_column(ts)?, value_column(values)?);
        self.with(py, |c| c.insert_series_async(series, &ts, &values))
    }

    /// Sends an insert of many series without waiting.
    fn insert_async(
        &self,
        py: Python<'_>,
        series: &Bound<'_, PyAny>,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let (series, ts, values) = (series_column(series)?, ts_column(ts)?, value_column(values)?);
        self.with(py, |c| c.insert_async(&series, &ts, &values))
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

    /// Raw points of `series` in [start, end) as (timestamps, values): numpy
    /// arrays (int64, float32) if numpy is importable and `numpy` is not
    /// False, else lists. None for start or end means unbounded.
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
        columns(py, p, numpy)
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
