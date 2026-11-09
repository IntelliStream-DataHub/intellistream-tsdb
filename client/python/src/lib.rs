// SPDX-License-Identifier: Apache-2.0
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
use std::sync::Mutex;
use std::time::Duration;

use pyo3::create_exception;
use pyo3::exceptions::{PyAttributeError, PyException, PyImportError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use ::tsdb_client as tc;
use tc::{ColumnRef, ValueType};

// What the embedded extension shares with this one lives in tsdb-client-python-common (Apache-2.0 as well).
pub(crate) use tsdb_client_python_common::*;
mod pool;

create_exception!(
    tsdb_client,
    TsdbError,
    PyException,
    "An error from tsdbd or the client; `code` is the TSDB_ERR_* number (negative)."
);

create_exception!(
    tsdb_client,
    TsdbUnavailable,
    TsdbError,
    "A volume's storage is unavailable (code ERR_UNAVAILABLE): the request was not applied. The client already \
     retried within its budget (`retry`); `retry_after` (seconds), `entries` (one dict per refused volume: \
     volume, reason, epoch, since_ms, retry_after, outage_id) and `has_body` say what the server sent."
);
create_exception!(
    tsdb_client,
    TsdbMoved,
    TsdbError,
    "Another node owns the volume (code ERR_MOVED): the request was not applied. `moved` lists dicts of volume, \
     epoch, node_id and addr."
);
create_exception!(
    tsdb_client,
    TsdbPartial,
    TsdbError,
    "A request over several volumes was applied for some only (code ERR_PARTIAL): `volumes` lists (volume, \
     status), `rows` the row count and `not_applied` the indexes of the rows that were not applied."
);
create_exception!(
    tsdb_client,
    TsdbDropped,
    TsdbError,
    "The rows are below the volume's drop watermark (code ERR_DROPPED): never applied, never to be sent again. \
     `volume` and `before_ms` say where the watermark is."
);

#[derive(Default)]
struct Inner {
    conn: Option<tc::Client>,
    types: HashMap<u64, ValueType>,
    states: HashMap<(u64, u32), String>,
}

impl Inner {
    fn conn(&mut self) -> tc::Result<&mut tc::Client> {
        self.conn.as_mut().ok_or_else(closed)
    }

    fn series_type(&mut self, id: u64) -> tc::Result<ValueType> {
        if let Some(&t) = self.types.get(&id) {
            return Ok(t);
        }
        let t = self.conn()?.series_type(id)?;
        self.types.insert(id, t);
        Ok(t)
    }

    fn state_name(&mut self, series: u64, code: u32) -> tc::Result<String> {
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
///     user: The user to log in as (SCRAM-SHA-256), when the server asks
///         for a login. The password never travels, and the connect fails
///         unless the server proves it holds the user's verifier. PBKDF2
///         runs once per process for a user and password: later connects,
///         a Pool's included, reuse the keys.
///     password: The user's password (its UTF-8 bytes, without
///         normalization; ASCII is safest): 1 to PASSWORD_MAX (1023) bytes
///         without NUL, in every client and the CLI.
///     password_file: Read the password from the first line of this file
///         instead, without the LF or CR LF that ends it, at every login.
///     require_auth: Refuse (ERR_AUTH) a server that does not ask for a
///         login (--no-auth). Otherwise such a server is used without one.
///     retry: Seconds one request may be retried: tsdbd answers UNAVAILABLE when a volume's storage is down
///         and MOVED when another node owns it, both meaning the request was not applied, so the client waits what
///         the reply asks for, follows a MOVED and sends the request again until this budget is spent; then the call
///         raises TsdbUnavailable or TsdbMoved. 30 by default; 0 turns retries off.
///
/// Raises:
///     TsdbError: The connection failed; ERR_AUTH when the login failed (an
///         unknown user and a wrong password look alike), the server did not
///         prove its signature, it asks for a login and none was given, or
///         require_auth is set and it asks for none; ERR_INVALID, naming
///         the file, when password_file cannot be read or its first line is
///         not a password (a mistake to fix, not a network error to retry).
///     ValueError: timeout is not a positive number, a user has neither
///         password nor password_file, or password breaks the rule above.
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
            // the body of an UNAVAILABLE, MOVED, PARTIAL or DROPPED reply, for the exception
            let info = match &r {
                Err(e) if matches!(e.code(), -13..=-10) => inner.conn.as_ref().map(|c| c.last_error().clone()),
                _ => None,
            };
            // after an I/O error (a timeout included) the stream is out of step: close it
            if inner.conn.as_ref().is_some_and(tc::Client::is_broken) {
                inner.conn = None;
            }
            r.map_err(|e| (e, info))
        })
        .map_err(|(e, info)| to_pyerr_info(py, e, info.as_ref()))
    }

    /// Runs `f` on the connection with the GIL released.
    fn with<T, F>(&self, py: Python<'_>, f: F) -> PyResult<T>
    where
        T: Send,
        F: FnOnce(&mut tc::Client) -> tc::Result<T> + Send,
    {
        self.with_inner(py, |i| f(i.conn()?))
    }

    /// Runs a multi-series call (query_*_multi()).
    fn multi(&self, py: Python<'_>, q: multi::Req) -> PyResult<Py<multi::Many>> {
        let p = self.with(py, |c| multi::run(multi::Target::Client(c), &q))?;
        multi::finish(py, q, p)
    }

    /// Typed inserts: into `one` series, or into `many` (a column of series ids).
    #[allow(clippy::too_many_arguments)]
    fn insert_typed(
        &self,
        py: Python<'_>,
        one: Option<u64>,
        many: Option<&Bound<'_, PyAny>>,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
        value_type: Option<&str>,
        wait: bool,
    ) -> PyResult<()> {
        let ts = ts_column(ts)?;
        let many = many.map(series_column).transpose()?;
        let ty = match (value_type, one.or_else(|| many.as_ref().and_then(SidCol::first))) {
            (Some(t), _) => self::value_type(t)?,
            (None, Some(id)) => self.with_inner(py, |i| i.series_type(id))?,
            (None, None) => ValueType::Float32,
        };
        let col = values_column(values, ty)?;
        self.with(py, |c| {
            let (ts, col, many) = (ts.snap(), col.snap(), many.as_ref().map(SidCol::snap));
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
        series: u64,
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

fn connect(
    host: &str,
    port: u16,
    opts: &tc::ConnectOptions,
    keepalive: bool,
    retry: Option<Duration>,
) -> tc::Result<tc::Client> {
    let mut c = tc::Client::connect_opts((host, port), opts)?;
    if !keepalive {
        c.set_keepalive(None)?;
    }
    c.set_retry(retry);
    Ok(c)
}

/// The time budget of one request's retries as the keyword `retry` gives it: seconds, 0 for none.
fn retry_budget(retry: f64) -> PyResult<Option<Duration>> {
    if retry == 0.0 {
        return Ok(None);
    }
    seconds(Some(retry)).map_err(|_| PyValueError::new_err("retry must be a positive number of seconds, or 0 for none"))
}

/// A topology as Python data: dicts of plain values.
fn topology_dict<'py>(py: Python<'py>, t: &tc::Topology) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("generation", t.generation)?;
    let nodes = PyList::empty(py);
    for n in &t.nodes {
        let e = PyDict::new(py);
        e.set_item("node_id", n.node_id)?;
        e.set_item("alive", n.alive)?;
        e.set_item("addr", &n.addr)?;
        nodes.append(e)?;
    }
    d.set_item("nodes", nodes)?;
    let vols = PyList::empty(py);
    for v in &t.volumes {
        let e = PyDict::new(py);
        e.set_item("volume", v.vol)?;
        e.set_item("role", v.role)?;
        e.set_item("health", v.health)?;
        e.set_item("epoch", v.epoch)?;
        e.set_item("reason", v.reason)?;
        e.set_item("domain", v.domain)?;
        e.set_item("since_ms", v.since_ms)?;
        e.set_item("owner", v.owner)?;
        e.set_item("server", v.server)?;
        e.set_item("addr", &v.addr)?;
        e.set_item("path", &v.path)?;
        vols.append(e)?;
    }
    d.set_item("volumes", vols)?;
    Ok(d)
}

/// The keyword arguments of Client and Pool as the Rust client's options. They live as long as the Client or
/// Pool: a Pool replaces a broken connection with a new login, from the password kept here (wiped when the Pool
/// goes) or the password file read again.
fn connect_options(
    timeout: Option<f64>,
    user: Option<String>,
    password: Option<String>,
    password_file: Option<std::path::PathBuf>,
    require_auth: bool,
) -> PyResult<tc::ConnectOptions> {
    if user.is_some() && password.is_none() && password_file.is_none() {
        return Err(PyValueError::new_err("a user needs a password or a password_file"));
    }
    if let Some(p) = &password {
        check_password(p)?;
    }
    let mut o = tc::ConnectOptions::new().timeout(seconds(timeout)?).require_auth(require_auth);
    if let Some(u) = user {
        o = o.user(u);
    }
    if let Some(p) = password {
        o = o.password(p);
    }
    if let Some(f) = password_file {
        o = o.password_file(f);
    }
    Ok(o)
}

/// The password rule of every client and the CLI (design-auth.md §6), as ValueError: 1 to PASSWORD_MAX bytes, no NUL.
fn check_password(p: &str) -> PyResult<()> {
    if p.is_empty() || p.len() > tc::PASSWORD_MAX || p.contains('\0') {
        return Err(PyValueError::new_err(format!("a password is 1 to {} bytes without NUL", tc::PASSWORD_MAX)));
    }
    Ok(())
}

/// Rights as a string of names ("read,insert"; "write" is insert and delete) or as the bits of RIGHT_*.
fn rights(obj: &Bound<'_, PyAny>) -> PyResult<tc::Rights> {
    if let Ok(s) = obj.extract::<String>() {
        return s.parse().map_err(|e: tc::Error| PyValueError::new_err(e.to_string()));
    }
    match obj.extract::<u8>() {
        Ok(b) if b != 0 && b & !0xf == 0 => Ok(tc::Rights::from_bits(b)),
        _ => Err(PyValueError::new_err("rights must be names (\"read,insert\") or RIGHT_* bits (1 to 15)")),
    }
}

/// A selector as the CLI writes it: "all", "prefix:P", "name:N" or "glob:G".
fn selector(s: &str) -> PyResult<tc::Selector> {
    s.parse().map_err(|e: tc::Error| PyValueError::new_err(e.to_string()))
}

/// A user of the server, from whoami() or user_list().
#[pyclass(frozen, module = "tsdb_client")]
struct User {
    name: String,
    superuser: bool,
    grants: Vec<(String, String)>,
}

impl From<tc::User> for User {
    fn from(u: tc::User) -> User {
        let grants = u.grants.iter().map(|g| (g.selector.to_string(), g.rights.to_string())).collect();
        User { name: u.name, superuser: u.superuser, grants }
    }
}

#[pymethods]
impl User {
    /// The login name; "" from a server without login (--no-auth).
    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    /// Every right on every series, user management and the database-wide operations.
    #[getter]
    fn superuser(&self) -> bool {
        self.superuser
    }

    /// The grants as (selector, rights) pairs of strings, e.g. ("prefix:acme/", "read,insert"). A user's rights
    /// on a series are the union of those whose selector matches its name. The rights read back in grant(); a
    /// selector of a kind this client does not know (a later server's) shows as "kind#N:TEXT" and does not.
    #[getter]
    fn grants(&self) -> Vec<(String, String)> {
        self.grants.clone()
    }

    fn __repr__(&self) -> String {
        let su = if self.superuser { "True" } else { "False" };
        format!("User(name={:?}, superuser={su}, grants={:?})", self.name, self.grants)
    }
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
    #[pyo3(signature = (host = "127.0.0.1", port = 7070, timeout = None, keepalive = true, *, user = None,
                        password = None, password_file = None, require_auth = false, retry = 30.0))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        host: &str,
        port: u16,
        timeout: Option<f64>,
        keepalive: bool,
        user: Option<String>,
        password: Option<String>,
        password_file: Option<std::path::PathBuf>,
        require_auth: bool,
        retry: f64,
    ) -> PyResult<Self> {
        let opts = connect_options(timeout, user, password, password_file, require_auth)?;
        let retry = retry_budget(retry)?;
        let c = py.detach(|| connect(host, port, &opts, keepalive, retry)).map_err(|e| to_pyerr(py, e))?;
        Ok(Client { inner: Mutex::new(Inner { conn: Some(c), ..Inner::default() }), addr: format!("{host}:{port}") })
    }

    /// Connects to the first of several nodes that answers.
    ///
    /// Args:
    ///     nodes: "host:port,host:port,..." (IPv6 as "[::1]:7070"; a node without a port is on 7070).
    ///         The list is shuffled once, so a fleet of clients spreads over the nodes. The client
    ///         keeps it: a MOVED reply without an address, or an address that cannot be reached,
    ///         goes on to the next node.
    ///     timeout, keepalive, user, password, password_file, require_auth, retry: As for Client().
    ///
    /// Raises:
    ///     TsdbError: No node answered (the last error), or the login failed (ERR_AUTH).
    ///     ValueError: The list is empty or a node is not a host:port.
    #[staticmethod]
    #[pyo3(signature = (nodes, timeout = None, keepalive = true, *, user = None, password = None,
                        password_file = None, require_auth = false, retry = 30.0))]
    #[allow(clippy::too_many_arguments)]
    fn connect_nodes(
        py: Python<'_>,
        nodes: &str,
        timeout: Option<f64>,
        keepalive: bool,
        user: Option<String>,
        password: Option<String>,
        password_file: Option<std::path::PathBuf>,
        require_auth: bool,
        retry: f64,
    ) -> PyResult<Self> {
        let opts = connect_options(timeout, user, password, password_file, require_auth)?;
        let retry = retry_budget(retry)?;
        let c = py.detach(|| tc::Client::connect_nodes(nodes, &opts)).map_err(|e| match e {
            tc::Error::InvalidArgument(m) => PyValueError::new_err(m),
            e => to_pyerr(py, e),
        })?;
        let mut c = c;
        if !keepalive {
            c.set_keepalive(None).map_err(|e| to_pyerr(py, e))?;
        }
        c.set_retry(retry);
        Ok(Client { inner: Mutex::new(Inner { conn: Some(c), ..Inner::default() }), addr: nodes.to_string() })
    }

    /// Sets the time budget of one request's retries, in seconds; 0 turns retries off.
    ///
    /// tsdbd answers UNAVAILABLE when a volume's storage is down and MOVED when another node owns it:
    /// both mean the request was not applied. The client waits what the reply asks for (retry_after),
    /// follows a MOVED and sends the request again until the budget is spent; then the call raises
    /// TsdbUnavailable or TsdbMoved. The default is 30 seconds.
    ///
    /// Raises:
    ///     ValueError: retry is negative, not finite, or under a nanosecond.
    ///     TsdbError: The client is closed.
    fn set_retry(&self, py: Python<'_>, retry: f64) -> PyResult<()> {
        let b = retry_budget(retry)?;
        self.with(py, |c| {
            c.set_retry(b);
            Ok(())
        })
    }

    /// The nodes and volumes the server lists (protocol 13): a dict with generation, nodes (dicts of node_id,
    /// alive, addr; addr "" is the node that answered) and volumes (dicts of volume, role, health, epoch,
    /// reason, domain, since_ms, owner, server, addr and path; path only for a superuser). A single server
    /// lists itself and every volume it serves.
    ///
    /// Raises:
    ///     TsdbError: The client is closed or the call failed.
    fn topology<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let t = self.with(py, tc::Client::topology)?;
        topology_dict(py, &t)
    }

    /// The server's status as key=value lines (a superuser).
    ///
    /// Raises:
    ///     TsdbError: ERR_DENIED for a user who is no superuser; the client is closed.
    fn status(&self, py: Python<'_>) -> PyResult<String> {
        self.with(py, tc::Client::server_status)
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
    /// The id is a 64-bit int that names the series' volume: keep it as it is (do not store it in 32 bits)
    /// and do not count on ids being 0, 1, 2 and so on.
    ///
    /// Args:
    ///     name: The series name, such as "pump7.flow".
    ///     create: If False, do not create the series.
    ///
    /// Raises:
    ///     TsdbError: ERR_NOTFOUND if create is False and there is no such series.
    #[pyo3(signature = (name, create = true))]
    fn series(&self, py: Python<'_>, name: &str, create: bool) -> PyResult<u64> {
        self.with(py, |c| if create { c.series(name) } else { c.series_lookup(name) })
    }

    /// Returns the id of a series, creating it (as float32) in a volume if needed.
    ///
    /// Args:
    ///     name: The series name.
    ///     path: A volume on the server for the new series, one the operator
    ///         added (`tsdb <dir> volume add`; else ERR_NOTFOUND); it only
    ///         matters when the series is created.
    fn series_at(&self, py: Python<'_>, name: &str, path: &str) -> PyResult<u64> {
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
    fn series_create(&self, py: Python<'_>, name: &str, value_type: &str, path: Option<&str>) -> PyResult<u64> {
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
    fn series_type(&self, py: Python<'_>, id: u64) -> PyResult<&'static str> {
        self.with_inner(py, |i| i.series_type(id)).map(ValueType::name)
    }

    /// Returns the name of a series.
    ///
    /// Raises:
    ///     TsdbError: ERR_NOTFOUND if there is no such series.
    fn series_name(&self, py: Python<'_>, id: u64) -> PyResult<String> {
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
    fn state(&self, py: Python<'_>, series: u64, name: &str) -> PyResult<u32> {
        self.with(py, |c| c.state(series, name))
    }

    /// Returns the code of an existing state name.
    ///
    /// Raises:
    ///     TsdbError: ERR_NOTFOUND if the name has no code.
    fn state_lookup(&self, py: Python<'_>, series: u64, name: &str) -> PyResult<u32> {
        self.with(py, |c| c.state_lookup(series, name))
    }

    /// Returns the name of a state code.
    ///
    /// Raises:
    ///     TsdbError: ERR_NOTFOUND for an unknown code.
    fn state_name(&self, py: Python<'_>, series: u64, code: u32) -> PyResult<String> {
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
    fn state_names(&self, py: Python<'_>, series: u64, codes: &Bound<'_, PyAny>) -> PyResult<Vec<Option<String>>> {
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
        series: u64,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let (ts, values) = (ts_column(ts)?, f32_column(values)?);
        self.with(py, |c| c.insert_series(series, &ts.snap(), &values.snap()))
    }

    /// Inserts float32 rows into many series and waits.
    ///
    /// Args:
    ///     series: The series id of each row: a list of ints, or a uint64 or int64 array.
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
        series: u64,
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
        series: u64,
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
        series: u64,
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
    ///         levels compress more, negative levels are faster still.
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
        series: u64,
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
        series: u64,
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
        series: u64,
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
        series: u64,
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
    /// times (query_bounds()).
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
        series: u64,
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
    /// quarters or years; query_group() does both steps.
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
    ///     TsdbError: ERR_INVALID if bounds is empty or does not increase.
    #[pyo3(signature = (series, bounds, start = None, end = None, *, order = "asc", limit = None))]
    #[allow(clippy::too_many_arguments)]
    fn query_bounds(
        &self,
        py: Python<'_>,
        series: u64,
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
    /// standard deviation.
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
    ///     TsdbError: ERR_INVALID for a negative interval.
    #[pyo3(signature = (series, start = None, end = None, interval_ms = 0, *, order = "asc", limit = None))]
    #[allow(clippy::too_many_arguments)]
    fn query_spread(
        &self,
        py: Python<'_>,
        series: u64,
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
        series: u64,
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
        series: u64,
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
    /// (order and limit as in query_agg()).
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
    ///     TsdbError: ERR_INVALID for a negative interval.
    ///     ValueError: Unknown method or order.
    #[pyo3(signature = (series, start = None, end = None, interval_ms = 0, *, method = "step", max_gap_ms = 0,
                        order = "asc", limit = None))]
    #[allow(clippy::too_many_arguments)]
    fn query_twa(
        &self,
        py: Python<'_>,
        series: u64,
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
        series: u64,
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
        series: u64,
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
    /// shape of the line (Largest-Triangle-Three-Buckets).
    ///
    /// The first and last rows are always kept; a range of threshold rows or
    /// fewer comes back whole.
    ///
    /// Args:
    ///     series: The series id.
    ///     threshold: How many rows to keep at most, at least 3.
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
    ///     TsdbError: ERR_INVALID for a threshold below 3 or a state series.
    #[pyo3(signature = (series, threshold, start = None, end = None, *, order = "asc", limit = None, numpy = None))]
    #[allow(clippy::too_many_arguments)]
    fn query_lttb<'py>(
        &self,
        py: Python<'py>,
        series: u64,
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

    /// Deletes the rows of a series with start <= ts < end.
    ///
    /// A delete is a write like any other: a row inserted after it returned
    /// is kept. start and end are required; None is unbounded, so
    /// delete_range(id, None, None) deletes every row of the series.
    #[pyo3(signature = (series, start, end))]
    fn delete_range(&self, py: Python<'_>, series: u64, start: Option<i64>, end: Option<i64>) -> PyResult<()> {
        self.with(py, |c| c.delete_range(series, range(start, end)))
    }

    /// Replaces the rows of a series with start <= ts < end by ts and values,
    /// atomically: a reader sees the old rows or the new ones, never a mix.
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
    ///         one request (256 MiB).
    #[pyo3(signature = (series, start, end, ts, values, *, value_type = None))]
    #[allow(clippy::too_many_arguments)]
    fn replace_range(
        &self,
        py: Python<'_>,
        series: u64,
        start: Option<i64>,
        end: Option<i64>,
        ts: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
        value_type: Option<&str>,
    ) -> PyResult<()> {
        self.replace_typed(py, series, start, end, ts, values, value_type)
    }

    /// Sets how long a series keeps its rows, in ms:
    /// rows older than the server's clock minus limit_ms disappear from
    /// queries at once and from disk as merges get to them. 0 keeps them.
    ///
    /// Raises:
    ///     TsdbError: ERR_INVALID for a negative limit.
    fn set_retention(&self, py: Python<'_>, series: u64, limit_ms: i64) -> PyResult<()> {
        self.with(py, |c| c.set_retention(series, limit_ms))
    }

    /// Returns the retention limit of a series in ms, 0 for none.
    fn retention(&self, py: Python<'_>, series: u64) -> PyResult<i64> {
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

    /// The user this connection is logged in as, with its grants (a User);
    /// name "" with every right (superuser) on a server started with --no-auth.
    fn whoami(&self, py: Python<'_>) -> PyResult<User> {
        self.with(py, |c| c.whoami()).map(User::from)
    }

    /// The users this one may see (a list of User): itself, and the users it
    /// manages (every user, for a superuser).
    ///
    /// Raises:
    ///     TsdbError: ERR_DENIED on a server started with --no-auth (users are
    ///         managed there with the offline `tsdb DIR user` verbs).
    fn user_list(&self, py: Python<'_>) -> PyResult<Vec<User>> {
        self.with(py, |c| c.user_list()).map(|v| v.into_iter().map(User::from).collect())
    }

    /// Creates user name with password, a superuser if superuser.
    ///
    /// The password does not travel: the client sends its verifier (a fresh
    /// salt, StoredKey and ServerKey) computed with iterations of PBKDF2
    /// (None: SCRAM_ITERATIONS, 4096; at least 4096, at most 100,000,000).
    ///
    /// Raises:
    ///     TsdbError: ERR_DENIED unless this user is a superuser or holds admin
    ///         on a prefix that name starts with (only a superuser creates
    ///         superusers), and on a --no-auth server; ERR_INVALID for a name
    ///         that exists or that the server refuses; ERR_INVALID for
    ///         iterations out of range.
    ///     ValueError: password is not 1 to PASSWORD_MAX (1023) bytes without
    ///         NUL (the rule of every client and the CLI).
    #[pyo3(signature = (name, password, *, superuser = false, iterations = None))]
    fn user_create(
        &self,
        py: Python<'_>,
        name: &str,
        password: &str,
        superuser: bool,
        iterations: Option<u32>,
    ) -> PyResult<()> {
        check_password(password)?;
        self.with(py, |c| c.user_create(name, password, superuser, iterations))
    }

    /// Sets the password of user name (its own, or one of a user it manages),
    /// sent as user_create() sends it.
    ///
    /// Raises:
    ///     TsdbError: As user_create(); ERR_DENIED also for a user holding a
    ///         grant outside this one's admin selectors.
    #[pyo3(signature = (name, password, *, iterations = None))]
    fn user_password(&self, py: Python<'_>, name: &str, password: &str, iterations: Option<u32>) -> PyResult<()> {
        check_password(password)?;
        self.with(py, |c| c.user_password(name, password, iterations))
    }

    /// Drops user name; its open connections are answered ERR_AUTH and closed.
    ///
    /// Raises:
    ///     TsdbError: ERR_DENIED for a user this one does not manage, and for
    ///         the last superuser.
    fn user_drop(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        self.with(py, |c| c.user_drop(name))
    }

    /// Makes user name a superuser or takes that away (superusers only).
    ///
    /// Raises:
    ///     TsdbError: ERR_DENIED unless this user is a superuser, and for
    ///         taking it from the last superuser.
    #[pyo3(signature = (name, *, superuser))]
    fn user_alter(&self, py: Python<'_>, name: &str, superuser: bool) -> PyResult<()> {
        self.with(py, |c| c.user_alter(name, superuser))
    }

    /// Gives user name rights on the series selector matches. Grants only add.
    ///
    /// selector is written as the CLI writes it: "all", "prefix:P" (a byte
    /// prefix of the name: tenants end theirs in "/"), "name:N" or "glob:G"
    /// (* any run of bytes, ? one byte, \ makes the next one literal). rights
    /// are names ("read,insert"; "write" is insert and delete) or RIGHT_* bits.
    /// Names are separated by commas, with spaces around them allowed, as
    /// tsdbd and the CLI read them; "none" and "0xHH" (bits a later server
    /// may name) read too, and the server refuses rights it cannot grant.
    ///
    /// Example:
    ///     >>> c.grant("acme/ingest", "prefix:acme/", "insert")
    ///     >>> c.grant("acme/dash", "glob:acme/*/temp", RIGHT_READ)
    ///
    /// Raises:
    ///     ValueError: A selector or rights it cannot read.
    ///     TsdbError: ERR_DENIED unless this user is a superuser or selector
    ///         lies inside its own admin selectors and it manages name;
    ///         ERR_INVALID for a selector the server refuses (a glob over 256
    ///         bytes, a 65th glob grant).
    fn grant(&self, py: Python<'_>, name: &str, selector: &str, rights: &Bound<'_, PyAny>) -> PyResult<()> {
        let (sel, r) = (self::selector(selector)?, self::rights(rights)?);
        self.with(py, |c| c.grant(name, &sel, r))
    }

    /// Takes rights on selector from user name (the grant with exactly this
    /// selector), as grant() writes them.
    ///
    /// Raises:
    ///     ValueError: A selector or rights it cannot read.
    ///     TsdbError: As grant().
    fn revoke(&self, py: Python<'_>, name: &str, selector: &str, rights: &Bound<'_, PyAny>) -> PyResult<()> {
        let (sel, r) = (self::selector(selector)?, self::rights(rights)?);
        self.with(py, |c| c.revoke(name, &sel, r))
    }

    /// The newest row of many series in one request (tsdbd protocol 14).
    ///
    /// The server answers a series from an in-memory table of each series' newest value, so the cost does
    /// not grow with the history; lists of more than 65,536 series go as several frames.
    ///
    /// Args:
    ///     series: Series ids: a list of ints, or a uint64 or int64 array.
    ///     end: Only rows before this timestamp (ms): one for every series, or a list or int64 array with
    ///         one per series; None for no bound.
    ///     with_nulls: Answer the newest row even when it is a null (then r[i] is (ts, None)); slower.
    ///     threads: Threads the request uses at most; 0 for the server's --request-threads.
    ///     errors: "raise" raises TsdbError (with .index and .result, the LatestMany) for the first series
    ///         that was NOTFOUND or DENIED (a denied series looks like a missing one); "status" returns the
    ///         LatestMany, whose .status tells which failed.
    ///     numpy: As for query_values().
    ///
    /// Returns:
    ///     LatestMany: r[i] is (ts, value), (ts, None) for a null, or None for a series without a row.
    ///     r.ts, r.kind and r.status are columns; r.values a list.
    ///
    /// Raises:
    ///     TsdbError: A series failed (errors="raise"), or the whole request failed (a server below
    ///         protocol 14 answers INVALID).
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
        let a = self.with(py, |c| q.run_client(c))?;
        q.finish(py, a)
    }

    /// As latest(), by name: the newest row of many series, asked by name (tsdbd protocol 14).
    ///
    /// The server answers a series from an in-memory table of each series' newest value, so the cost does
    /// not grow with the history; lists of more than 65,536 series go as several frames.
    ///
    /// Args:
    ///     names: Series names: a list of str. A name is looked up once per connection.
    ///     end: Only rows before this timestamp (ms): one for every series, or a list or int64 array with
    ///         one per series; None for no bound.
    ///     with_nulls: Answer the newest row even when it is a null (then r[i] is (ts, None)); slower.
    ///     threads: Threads the request uses at most; 0 for the server's --request-threads.
    ///     errors: "raise" raises TsdbError (with .index and .result, the LatestMany) for the first series
    ///         that was NOTFOUND or DENIED (a denied series looks like a missing one); "status" returns the
    ///         LatestMany, whose .status tells which failed.
    ///     numpy: As for query_values().
    ///
    /// Returns:
    ///     LatestMany: r[i] is (ts, value), (ts, None) for a null, or None for a series without a row.
    ///     r.ts, r.kind and r.status are columns; r.values a list.
    ///
    /// Raises:
    ///     TsdbError: A series failed (errors="raise"), or the whole request failed (a server below
    ///         protocol 14 answers INVALID).
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
        let a = self.with(py, |c| q.run_client(c))?;
        q.finish(py, a)
    }

    /// Returns what the server did for this connection's previous query (a QueryStats).
    ///
    /// Rows covered, decompressed or answered from block statistics, bytes
    /// and time. All zero before the first query.
    fn last_query_stats(&self, py: Python<'_>) -> PyResult<QueryStats> {
        self.with(py, |c| c.last_query_stats()).map(QueryStats::from)
    }

    /// Reads the rows of many series in one request.
    ///
    /// The server reads the series in parallel, from one consistent point in
    /// time, and answers them in the order asked. Lists of more than 65,536
    /// series go as several requests.
    ///
    /// Args:
    ///     series: Series ids: a list of ints, or a uint64 or int64 array.
    ///     start: First timestamp to include (ms): one for every series, or a
    ///         list or int64 array with one per series; None for no limit.
    ///     end: Timestamp to stop before, like start.
    ///     order: "asc" or "desc", for each series.
    ///     limit: At most this many rows of each series; None for all.
    ///     threads: Threads the request uses at most; 0 for the server's
    ///         --request-threads (Db: the database's request_threads).
    ///     max_rows: Rows of all series together, at most (ERR_TOO_MANY_BUCKETS
    ///         past it); 0 for the server's --multi-max-rows (Db: no limit).
    ///     errors: "raise" raises TsdbError (with .index and .result, the
    ///         Many) for the first series that failed; "status" returns the
    ///         Many, whose .status tells which failed.
    ///     numpy: As for query_values().
    ///
    /// Returns:
    ///     Many: r.ts and r.values hold every series' rows, r.offsets where
    ///     each begins; r[i] is series i's (timestamps, values).
    ///
    /// Raises:
    ///     TsdbError: A series failed (errors="raise"), the whole request
    ///         failed.
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

    /// Downsamples many series with LTTB (query_lttb()) in one request.
    ///
    /// Arguments and result as for query_values_multi(), with threshold as for
    /// query_lttb().
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

    /// Aggregates many series in one request.
    ///
    /// Each series is bucketed as query_agg() (interval_ms), query_group()
    /// (unit, count, timezone) or query_bounds() (bounds) would; with none of
    /// them every series gets one bucket over its range. For a zone other than
    /// UTC the client computes the local periods' bounds over all the series.
    ///
    /// Args:
    ///     series, start, end, order, limit, threads, max_rows, errors: As for
    ///         query_values_multi(); limit counts buckets.
    ///     interval_ms: Bucket length in ms: one for all, or one per series.
    ///     unit, count, timezone: Calendar periods, as for query_group().
    ///     bounds: Bucket starts for every series, as for query_bounds().
    ///
    /// Returns:
    ///     Many: r.buckets (a Buckets) holds every series' buckets, whose
    ///     columns also read from r (r.counts, r.sum, ...); r[i] is series i's
    ///     Buckets.
    ///
    /// Raises:
    ///     TsdbError: As for query_values_multi().
    ///     ValueError: More than one of interval_ms, unit and bounds.
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

    /// The spread aggregates (query_spread()) of many series in one request.
    ///
    /// Arguments and result as for query_buckets_multi(); the buckets have the
    /// spread columns (range, var_pop, ...).
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

    /// Time-weighted averages (query_twa()) of many series in one request.
    ///
    /// Arguments as for query_buckets_multi(), with method and max_gap_ms as
    /// for query_twa(). r.buckets is a TwaBuckets; r[i] is series i's.
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

/// Module attribute hook (PEP 562): the embedded `Db` left this package in 0.11.0, when the engine became
/// AGPL-3.0-only. It must not import `tsdb_embedded` for the caller: the licence change belongs in plain sight.
#[pyfunction]
fn __getattr__(py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
    if name == "Db" {
        return Err(PyImportError::new_err(
            "tsdb_client.Db (the embedded database) moved to the package intellistream-tsdb-embedded, which is \
             licensed AGPL-3.0-only (tsdb_client stays Apache-2.0): pip install intellistream-tsdb-embedded, then \
             `from tsdb_embedded import Db`",
        ));
    }
    let _ = py;
    Err(PyAttributeError::new_err(format!("module 'tsdb_client' has no attribute '{name}'")))
}

/// Returns the text for a TSDB_ERR_* code, such as ERR_NOTFOUND.
#[pyfunction]
fn strerror(code: i32) -> &'static str {
    tc::strerror(code)
}

/// Tests only (the feature test-hooks, never in a wheel): every later login of
/// this process uses nonce as its client nonce (the RFC 7677 exchange); None
/// goes back to random ones.
#[cfg(feature = "test-hooks")]
#[pyfunction]
#[pyo3(signature = (nonce))]
fn _set_client_nonce(nonce: Option<&str>) {
    tc::test_hooks::set_client_nonce(nonce);
}

/// Tests only (the feature test-hooks): how many times this process ran
/// PBKDF2 (logins and verifiers).
#[cfg(feature = "test-hooks")]
#[pyfunction]
fn _pbkdf2_runs() -> u64 {
    tc::test_hooks::pbkdf2_runs()
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
    m.add_class::<pool::Pool>()?;
    add_result_classes(m)?;
    m.add("TsdbError", m.py().get_type::<TsdbError>())?;
    m.add("TsdbUnavailable", m.py().get_type::<TsdbUnavailable>())?;
    m.add("TsdbMoved", m.py().get_type::<TsdbMoved>())?;
    m.add("TsdbPartial", m.py().get_type::<TsdbPartial>())?;
    m.add("TsdbDropped", m.py().get_type::<TsdbDropped>())?;
    m.add_function(wrap_pyfunction!(strerror, m)?)?;
    m.add_function(wrap_pyfunction!(__getattr__, m)?)?;
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
        ("ERR_MOVED", tc::Status::Moved),
        ("ERR_UNAVAILABLE", tc::Status::Unavailable),
        ("ERR_PARTIAL", tc::Status::Partial),
        ("ERR_DROPPED", tc::Status::Dropped),
        ("ERR_DENIED", tc::Status::Denied),
        ("ERR_AUTH", tc::Status::Auth),
    ] {
        m.add(name, s.code())?;
    }
    m.add_class::<User>()?;
    m.add("SCRAM_ITERATIONS", tc::SCRAM_ITERATIONS)?;
    m.add("PASSWORD_MAX", tc::PASSWORD_MAX)?;
    for (name, r) in [
        ("RIGHT_READ", tc::Rights::READ),
        ("RIGHT_INSERT", tc::Rights::INSERT),
        ("RIGHT_DELETE", tc::Rights::DELETE),
        ("RIGHT_ADMIN", tc::Rights::ADMIN),
        ("RIGHT_WRITE", tc::Rights::WRITE),
    ] {
        m.add(name, r.bits())?;
    }
    #[cfg(feature = "test-hooks")]
    {
        m.add_function(wrap_pyfunction!(_set_client_nonce, m)?)?;
        m.add_function(wrap_pyfunction!(_pbkdf2_runs, m)?)?;
    }
    Ok(())
}
