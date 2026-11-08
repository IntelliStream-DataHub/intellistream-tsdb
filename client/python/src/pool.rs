//! `tsdb_client.Pool`: several connections to one tsdbd for batch calls (`tsdb_client::Pool`).
//!
//! A batch call converts its arguments while the GIL is held, then spreads the queries or inserts
//! over the connections, one Rust worker thread per connection, with the GIL released for the
//! whole batch. One Python thread thereby keeps every connection busy, which threads each making
//! one call at a time cannot do past a few connections: each call crosses the GIL twice.

use std::collections::HashMap;
use std::sync::{Mutex, RwLock};

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;

use ::tsdb_client as tc;
use tc::ValueType;

use crate::arrays::{BucketsMany, agg_queries, inserts, many_cols, points_list, range_queries, with_batches};
use crate::{closed, connect, connect_options, latest, multi, opts, retry_budget, to_pyerr};

/// A pool of connections to tsdbd for batch calls that use them all at once.
///
/// One call runs many queries (or inserts) spread over the connections, in
/// parallel and with the GIL released, so a single Python thread can keep
/// the server's cores busy. Use it as a context manager or call close().
///
/// Args:
///     host: Server name or address.
///     port: Server port.
///     connections: Number of connections (default: the number of CPUs here).
///     timeout: Seconds for the connect and every later read or write, as in
///         Client; None waits forever after a 10 s connect.
///     keepalive: TCP keepalive, as in Client.
///     user, password, password_file, require_auth: The login, as in Client;
///         every connection logs in, a replacement too.
///     retry: Seconds one request may be retried on UNAVAILABLE or MOVED, as in Client (30 by default; 0 for
///         none). The exceptions of a Pool's batch calls carry their class but not the reply's body.
///
/// A connection that breaks (an I/O error or a timeout) is replaced by a new
/// one before its next query, and so is every other connection made before
/// it broke (after a server restart all of them are dead). A query whose
/// connection broke runs once more on a new connection; an insert does not
/// (the server may have applied it), so after a restart one insert_many()
/// can fail with ERR_IO and the next works. Errors of batch calls name the
/// lowest failing entry (TsdbError.index), as Db's do. A Pool has no
/// last_query_stats(); a Client has.
///
/// Raises:
///     TsdbError: A connection failed.
///     ValueError: connections is 0 or timeout is not positive.
#[pyclass(module = "tsdb_client")]
pub(crate) struct Pool {
    pool: RwLock<Option<tc::Pool>>,
    addr: String,
    /// Series types, which never change: insert_many() converts values to them.
    types: Mutex<HashMap<u64, ValueType>>,
}

impl Pool {
    fn with<T, F>(&self, py: Python<'_>, f: F) -> PyResult<T>
    where
        T: Send,
        F: FnOnce(&tc::Pool) -> tc::Result<T> + Send,
    {
        py.detach(|| f(self.pool.read().unwrap_or_else(|e| e.into_inner()).as_ref().ok_or_else(closed)?))
            .map_err(|e| to_pyerr(py, e))
    }

    /// Runs a multi-series call (query_*_multi()) on one of the connections.
    fn multi(&self, py: Python<'_>, q: multi::Req) -> PyResult<Py<multi::Many>> {
        let p = self.with(py, |pool| multi::run(multi::Target::Pool(pool), &q))?;
        multi::finish(py, q, p)
    }

    /// The type of each series in `ids`, looked up once (a lookup that failed is an error of its
    /// own, not of the call: insert_many() reports it for the insert that needed it).
    fn types(&self, py: Python<'_>, ids: &[u64]) -> PyResult<Vec<tc::Result<ValueType>>> {
        let known = self.types.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let missing: Vec<u64> = ids.iter().copied().filter(|id| !known.contains_key(id)).collect();
        let mut found = HashMap::new();
        if !missing.is_empty() {
            let got = self.with(py, |p| {
                p.map_retry(missing.len(), |c, i| match c.series_type(missing[i]) {
                    Err(e) if c.is_broken() => Err(e), // retried once on a new connection
                    r => Ok(r),
                })
            })?;
            let mut cache = self.types.lock().unwrap_or_else(|e| e.into_inner());
            for (&id, r) in missing.iter().zip(got) {
                if let Ok(t) = &r {
                    cache.insert(id, *t);
                }
                found.insert(id, r);
            }
        }
        Ok(ids
            .iter()
            .map(|id| known.get(id).copied().map(Ok).unwrap_or_else(|| found.remove(id).expect("looked up")))
            .collect())
    }
}

#[pymethods]
impl Pool {
    #[new]
    #[pyo3(signature = (host = "127.0.0.1", port = tc::DEFAULT_PORT, connections = None, *, timeout = None,
                        keepalive = true, user = None, password = None, password_file = None, require_auth = false,
                        retry = 30.0))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        host: &str,
        port: u16,
        connections: Option<usize>,
        timeout: Option<f64>,
        keepalive: bool,
        user: Option<String>,
        password: Option<String>,
        password_file: Option<std::path::PathBuf>,
        require_auth: bool,
        retry: f64,
    ) -> PyResult<Self> {
        let retry = retry_budget(retry)?;
        let n = connections.unwrap_or_else(|| std::thread::available_parallelism().map_or(4, |n| n.get()));
        if n == 0 {
            return Err(PyValueError::new_err("connections must be at least 1"));
        }
        // every connection, and every one that replaces a broken one, logs in with these; PBKDF2 runs once, as
        // the Rust client caches the keys per process
        let opts = connect_options(timeout, user, password, password_file, require_auth)?;
        let h = host.to_string();
        let pool = py
            .detach(|| tc::Pool::with_connector(n, move || connect(&h, port, &opts, keepalive, retry)))
            .map_err(|e| to_pyerr(py, e))?;
        Ok(Pool { pool: RwLock::new(Some(pool)), addr: format!("{host}:{port}"), types: Mutex::default() })
    }

    /// Number of connections.
    #[getter]
    fn connections(&self) -> usize {
        self.pool.read().unwrap_or_else(|e| e.into_inner()).as_ref().map_or(0, tc::Pool::size)
    }

    /// Closes every connection; later calls raise TsdbError (ERR_IO).
    fn close(&self, py: Python<'_>) {
        py.detach(|| drop(self.pool.write().unwrap_or_else(|e| e.into_inner()).take()));
    }

    /// Returns the pool itself, to use in a with statement.
    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    /// Closes the pool at the end of a with block.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, py: Python<'_>, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        self.close(py);
        false
    }

    fn __repr__(&self) -> String {
        format!("Pool({:?}, connections={})", self.addr, self.connections())
    }

    /// Runs many raw float32 queries over the connections at once.
    ///
    /// Takes and returns what Db.query_raw_many() does: series (one id per
    /// query), start and end (None, one timestamp, or one per query), order,
    /// limit; a list of (timestamps, values) numpy arrays, one per query.
    /// Needs numpy (ImportError without it).
    ///
    /// Raises:
    ///     TsdbError: The lowest failing query's error; its index attribute is
    ///         that query's position, and every query below it succeeded.
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
        let p = self.with(py, |p| p.query_points_many(&q, o))?;
        points_list(py, p)
    }

    /// Runs many aggregate queries over the connections at once.
    ///
    /// As Db.query_agg_many(): a bucket length for every query or one per
    /// query (0: one bucket for the range); returns a BucketsMany (lists
    /// instead of numpy columns without numpy).
    ///
    /// Raises:
    ///     TsdbError: The lowest failing query's error; its index attribute is
    ///         that query's position, and every query below it succeeded.
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
        let b = self.with(py, |p| p.query_agg_many(&q).map(many_cols))?;
        BucketsMany::new(py, b)
    }

    /// Runs many inserts over the connections at once.
    ///
    /// As Db.insert_many(): a list of (series, ts, values) tuples, each all or
    /// nothing on its own; inserts that share a series run in list order (on
    /// one connection); without value_type each takes its series' type.
    ///
    /// Raises:
    ///     TsdbError: The lowest failing insert's error, with its index; every
    ///         insert below it was committed, later ones may have been (errors
    ///         found before inserting, as in Db.insert_many(), insert nothing).
    ///         An insert whose connection broke is not retried.
    #[pyo3(signature = (batches, *, value_type = None))]
    fn insert_many(&self, py: Python<'_>, batches: &Bound<'_, PyAny>, value_type: Option<&str>) -> PyResult<()> {
        let b = inserts(batches, value_type.map(crate::value_type).transpose()?, |ids| self.types(py, ids))?;
        self.with(py, |p| with_batches(&b, |b| p.insert_many(b)))
    }

    /// As Client.latest() (on one of the connections).
    #[pyo3(signature = (series, end = None, *, with_nulls = false, threads = 0, errors = "raise", numpy = None))]
    fn latest(
        &self,
        py: Python<'_>,
        series: &Bound<'_, PyAny>,
        end: Option<&Bound<'_, PyAny>>,
        with_nulls: bool,
        threads: u16,
        errors: &str,
        numpy: Option<bool>,
    ) -> PyResult<Py<latest::LatestMany>> {
        let q = latest::LatestReq::new(series, false, end, with_nulls, threads, errors, numpy)?;
        let a = self.with(py, |p| q.run(p))?;
        q.finish(py, a)
    }

    /// As Client.latest_by_name() (on one of the connections).
    #[pyo3(signature = (names, end = None, *, with_nulls = false, threads = 0, errors = "raise", numpy = None))]
    fn latest_by_name(
        &self,
        py: Python<'_>,
        names: &Bound<'_, PyAny>,
        end: Option<&Bound<'_, PyAny>>,
        with_nulls: bool,
        threads: u16,
        errors: &str,
        numpy: Option<bool>,
    ) -> PyResult<Py<latest::LatestMany>> {
        let q = latest::LatestReq::new(names, true, end, with_nulls, threads, errors, numpy)?;
        let a = self.with(py, |p| q.run(p))?;
        q.finish(py, a)
    }

    /// As Client.query_values_multi() (on one of the connections).
    #[pyo3(signature = (series, start = None, end = None, *, order = "asc", limit = None, threads = 0, max_rows = 0, errors = "raise", numpy = None))]
    #[allow(clippy::too_many_arguments)]
    fn query_values_multi(
        &self,
        py: Python<'_>,
        series: &Bound<'_, PyAny>,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        order: &str,
        limit: Option<u64>,
        threads: u16,
        max_rows: u64,
        errors: &str,
        numpy: Option<bool>,
    ) -> PyResult<Py<multi::Many>> {
        self.multi(
            py,
            multi::Req::new(multi::Kind::Values, series, start, end, order, limit, threads, max_rows, errors)?
                .numpy(numpy),
        )
    }

    /// As Client.query_lttb_multi() (on one of the connections).
    #[pyo3(signature = (series, threshold, start = None, end = None, *, order = "asc", limit = None, threads = 0, max_rows = 0, errors = "raise", numpy = None))]
    #[allow(clippy::too_many_arguments)]
    fn query_lttb_multi(
        &self,
        py: Python<'_>,
        series: &Bound<'_, PyAny>,
        threshold: u64,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        order: &str,
        limit: Option<u64>,
        threads: u16,
        max_rows: u64,
        errors: &str,
        numpy: Option<bool>,
    ) -> PyResult<Py<multi::Many>> {
        self.multi(
            py,
            multi::Req::new(multi::Kind::Lttb, series, start, end, order, limit, threads, max_rows, errors)?
                .threshold(threshold)
                .numpy(numpy),
        )
    }

    /// As Client.query_buckets_multi() (on one of the connections).
    #[pyo3(signature = (series, start = None, end = None, interval_ms = None, *, unit = None, count = 1,
                        timezone = None, bounds = None, order = "asc", limit = None, threads = 0, max_rows = 0, errors = "raise"))]
    #[allow(clippy::too_many_arguments)]
    fn query_buckets_multi(
        &self,
        py: Python<'_>,
        series: &Bound<'_, PyAny>,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        interval_ms: Option<&Bound<'_, PyAny>>,
        unit: Option<&str>,
        count: u32,
        timezone: Option<&Bound<'_, PyAny>>,
        bounds: Option<&Bound<'_, PyAny>>,
        order: &str,
        limit: Option<u64>,
        threads: u16,
        max_rows: u64,
        errors: &str,
    ) -> PyResult<Py<multi::Many>> {
        let q = multi::Req::new(multi::Kind::Buckets, series, start, end, order, limit, threads, max_rows, errors)?;
        self.multi(py, q.buckets(interval_ms, unit, count, timezone, bounds)?)
    }

    /// As Client.query_spread_multi() (on one of the connections).
    #[pyo3(signature = (series, start = None, end = None, interval_ms = None, *, unit = None, count = 1,
                        timezone = None, bounds = None, order = "asc", limit = None, threads = 0, max_rows = 0, errors = "raise"))]
    #[allow(clippy::too_many_arguments)]
    fn query_spread_multi(
        &self,
        py: Python<'_>,
        series: &Bound<'_, PyAny>,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        interval_ms: Option<&Bound<'_, PyAny>>,
        unit: Option<&str>,
        count: u32,
        timezone: Option<&Bound<'_, PyAny>>,
        bounds: Option<&Bound<'_, PyAny>>,
        order: &str,
        limit: Option<u64>,
        threads: u16,
        max_rows: u64,
        errors: &str,
    ) -> PyResult<Py<multi::Many>> {
        let q = multi::Req::new(multi::Kind::Spread, series, start, end, order, limit, threads, max_rows, errors)?;
        self.multi(py, q.buckets(interval_ms, unit, count, timezone, bounds)?)
    }

    /// As Client.query_twa_multi() (on one of the connections).
    #[pyo3(signature = (series, start = None, end = None, interval_ms = None, *, unit = None, count = 1,
                        timezone = None, bounds = None, method = "step", max_gap_ms = 0, order = "asc", limit = None, threads = 0, max_rows = 0, errors = "raise"))]
    #[allow(clippy::too_many_arguments)]
    fn query_twa_multi(
        &self,
        py: Python<'_>,
        series: &Bound<'_, PyAny>,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        interval_ms: Option<&Bound<'_, PyAny>>,
        unit: Option<&str>,
        count: u32,
        timezone: Option<&Bound<'_, PyAny>>,
        bounds: Option<&Bound<'_, PyAny>>,
        method: &str,
        max_gap_ms: i64,
        order: &str,
        limit: Option<u64>,
        threads: u16,
        max_rows: u64,
        errors: &str,
    ) -> PyResult<Py<multi::Many>> {
        let q = multi::Req::new(multi::Kind::Twa, series, start, end, order, limit, threads, max_rows, errors)?;
        self.multi(py, q.buckets(interval_ms, unit, count, timezone, bounds)?.twa(method, max_gap_ms)?)
    }
}
