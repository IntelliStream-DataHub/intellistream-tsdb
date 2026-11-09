// SPDX-License-Identifier: Apache-2.0
//! `latest()` and `latest_by_name()` of Client, Pool and Db (protocol version 14): the newest row of many
//! series in one request. The arguments become a `LatestReq` while the GIL is held; the call runs detached.

use pyo3::exceptions::{PyIndexError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use ::tsdb_client as tc;
use tc::{Latest, LatestKind, LatestOpts, LatestValue};

use crate::TsdbError;
use crate::arrays::{use_numpy, vec_to_py};
use crate::multi::{PerSeries, per_series};

/// What a latest call asks for.
pub struct LatestReq {
    ids: Option<Vec<u64>>,
    names: Option<Vec<String>>,
    opts: LatestOpts,
    raise: bool,
    numpy: Option<bool>,
}

impl LatestReq {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        series: &Bound<'_, PyAny>,
        by_name: bool,
        end: Option<&Bound<'_, PyAny>>,
        with_nulls: bool,
        threads: u16,
        errors: &str,
        numpy: Option<bool>,
    ) -> PyResult<LatestReq> {
        let raise = match errors {
            "raise" => true,
            "status" => false,
            e => return Err(PyValueError::new_err(format!("errors must be 'raise' or 'status', not {e:?}"))),
        };
        let (ids, names, n) = if by_name {
            let v: Vec<String> = series.extract()?;
            let n = v.len();
            (None, Some(v), n)
        } else {
            let v = crate::series_column(series)?.snap().into_owned();
            let n = v.len();
            (Some(v), None, n)
        };
        let mut opts = LatestOpts { with_nulls, threads, ..LatestOpts::default() };
        match per_series(end, n, "end")? {
            PerSeries::All(v) => opts.end = v.unwrap_or(i64::MAX),
            PerSeries::Each(v) => opts.ends = Some(v),
        }
        Ok(LatestReq { ids, names, opts, raise, numpy })
    }

    /// Runs the call on a connection.
    pub fn run_client(&self, c: &mut tc::Client) -> tc::Result<Vec<Latest>> {
        match (&self.ids, &self.names) {
            (Some(ids), _) => c.latest(ids, &self.opts),
            (_, Some(names)) => {
                let v: Vec<&str> = names.iter().map(String::as_str).collect();
                c.latest_by_name(&v, &self.opts)
            }
            _ => Ok(Vec::new()),
        }
    }

    /// Runs the call on a pool or a database.
    pub fn run<T: LatestTarget + ?Sized>(&self, t: &T) -> tc::Result<Vec<Latest>> {
        match (&self.ids, &self.names) {
            (Some(ids), _) => t.latest(ids, &self.opts),
            (_, Some(names)) => {
                let v: Vec<&str> = names.iter().map(String::as_str).collect();
                t.latest_by_name(&v, &self.opts)
            }
            _ => Ok(Vec::new()),
        }
    }

    pub fn finish(self, py: Python<'_>, answers: Vec<Latest>) -> PyResult<Py<LatestMany>> {
        let np = use_numpy(py, self.numpy)?;
        let n = answers.len();
        let mut status = Vec::with_capacity(n);
        let mut kind = Vec::with_capacity(n);
        let mut ts = Vec::with_capacity(n);
        let mut values = Vec::with_capacity(n);
        for a in &answers {
            status.push(a.status.map_or(0, tc::Status::code));
            kind.push(match a.kind {
                LatestKind::Value => 0u8,
                LatestKind::Null => 1,
                LatestKind::None => 2,
            });
            ts.push(a.ts);
            values.push(a.value);
        }
        let first_err = status.iter().position(|&s| s != 0);
        let many = Py::new(
            py,
            LatestMany {
                series: self.ids.unwrap_or_default(),
                names: self.names,
                status_py: vec_to_py(py, np, status.clone())?.unbind(),
                kind_py: vec_to_py(py, np, kind.clone())?.unbind(),
                ts_py: vec_to_py(py, np, ts.clone())?.unbind(),
                status,
                kind,
                ts,
                values,
            },
        )?;
        match first_err {
            Some(i) if self.raise => Err(many.get().error(py, Some(many.clone_ref(py)), i)),
            _ => Ok(many),
        }
    }
}

/// The calls a latest request runs on: a Client, a Pool or the embedded Db (a wrapper of it in `tsdb_embedded`, which implements this).
pub trait LatestTarget {
    fn latest(&self, ids: &[u64], opts: &LatestOpts) -> tc::Result<Vec<Latest>>;
    fn latest_by_name(&self, names: &[&str], opts: &LatestOpts) -> tc::Result<Vec<Latest>>;
}

impl LatestTarget for tc::Pool {
    fn latest(&self, ids: &[u64], opts: &LatestOpts) -> tc::Result<Vec<Latest>> {
        tc::Pool::latest(self, ids, opts)
    }
    fn latest_by_name(&self, names: &[&str], opts: &LatestOpts) -> tc::Result<Vec<Latest>> {
        tc::Pool::latest_by_name(self, names, opts)
    }
}

/// The answers of one latest call, in the order asked. `r[i]` is `(ts, value)` for series i's newest value,
/// `(ts, None)` for a null (with_nulls=True), `None` when the series has no row, and raises TsdbError when
/// its status is not 0 (NOTFOUND or DENIED).
#[cfg_attr(not(feature = "embedded"), pyclass(frozen, sequence, module = "tsdb_client"))]
#[cfg_attr(feature = "embedded", pyclass(frozen, sequence, module = "tsdb_embedded"))]
pub struct LatestMany {
    series: Vec<u64>,
    names: Option<Vec<String>>,
    status: Vec<i32>,
    kind: Vec<u8>,
    ts: Vec<i64>,
    values: Vec<Option<LatestValue>>,
    status_py: Py<PyAny>,
    kind_py: Py<PyAny>,
    ts_py: Py<PyAny>,
}

pub fn value_to_py(py: Python<'_>, v: LatestValue) -> PyResult<Py<PyAny>> {
    Ok(match v {
        LatestValue::Float32(x) => f64::from(x).into_pyobject(py)?.into_any().unbind(),
        LatestValue::Float64(x) => x.into_pyobject(py)?.into_any().unbind(),
        LatestValue::Int64(x) => x.into_pyobject(py)?.into_any().unbind(),
        LatestValue::Bool(x) => x.into_pyobject(py)?.to_owned().into_any().unbind(),
        LatestValue::State(x) => x.into_pyobject(py)?.into_any().unbind(),
    })
}

impl LatestMany {
    fn error(&self, py: Python<'_>, me: Option<Py<LatestMany>>, i: usize) -> PyErr {
        let name = match &self.names {
            Some(n) => format!("{:?}", n[i]),
            None => self.series[i].to_string(),
        };
        let what = match tc::Status::from_code(self.status[i]) {
            tc::Status::NotFound => "not found",
            tc::Status::Denied => "denied",
            tc::Status::Invalid => "invalid",
            _ => "failed",
        };
        let err = TsdbError::new_err(format!("series {name} (index {i}): {what}"));
        let v = err.value(py);
        let set = v
            .setattr("code", self.status[i])
            .and_then(|()| v.setattr("index", i))
            .and_then(|()| v.setattr("result", me));
        match set {
            Ok(()) => err,
            Err(e) => e,
        }
    }
}

#[pymethods]
impl LatestMany {
    fn __len__(&self) -> usize {
        self.status.len()
    }

    fn __getitem__<'py>(slf: &Bound<'py, Self>, i: isize) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let me = slf.get();
        let n = me.status.len() as isize;
        let i = if i < 0 { i + n } else { i };
        if i < 0 || i >= n {
            return Err(PyIndexError::new_err("series index out of range"));
        }
        let i = i as usize;
        if me.status[i] != 0 {
            return Err(me.error(py, Some(slf.clone().unbind()), i));
        }
        Ok(match me.kind[i] {
            0 => {
                let v = match me.values[i] {
                    Some(v) => value_to_py(py, v)?,
                    None => py.None(),
                };
                PyTuple::new(py, [me.ts[i].into_pyobject(py)?.into_any().unbind(), v])?.into_any()
            }
            1 => PyTuple::new(py, [me.ts[i].into_pyobject(py)?.into_any().unbind(), py.None()])?.into_any(),
            _ => py.None().into_bound(py),
        })
    }

    /// The error of series i (a TsdbError, not raised), or None when it was answered.
    fn error_of(slf: &Bound<'_, Self>, i: usize) -> PyResult<Option<PyErr>> {
        let me = slf.get();
        if i >= me.status.len() {
            return Err(PyIndexError::new_err("series index out of range"));
        }
        Ok((me.status[i] != 0).then(|| me.error(slf.py(), Some(slf.clone().unbind()), i)))
    }

    /// True when every series was answered (no NOTFOUND or DENIED).
    #[getter]
    fn ok(&self) -> bool {
        self.status.iter().all(|&s| s == 0)
    }

    /// The series ids, in the order asked (empty for latest_by_name()).
    #[getter]
    fn series(&self) -> Vec<u64> {
        self.series.clone()
    }

    /// The names asked (latest_by_name()), else None.
    #[getter]
    fn names(&self) -> Option<Vec<String>> {
        self.names.clone()
    }

    /// Each answer's status: 0, or a TSDB_ERR_* code (NOTFOUND, DENIED, INVALID).
    #[getter]
    fn status(&self, py: Python<'_>) -> Py<PyAny> {
        self.status_py.clone_ref(py)
    }

    /// Each answer's kind: 0 a value, 1 a null (with_nulls=True), 2 no row (or a status).
    #[getter]
    fn kind(&self, py: Python<'_>) -> Py<PyAny> {
        self.kind_py.clone_ref(py)
    }

    /// Each row's timestamp (ms); 0 where kind is 2.
    #[getter]
    fn ts(&self, py: Python<'_>) -> Py<PyAny> {
        self.ts_py.clone_ref(py)
    }

    /// Each value as a Python object (float, int or bool; a state is its code); None where kind is not 0.
    #[getter]
    fn values(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        self.values
            .iter()
            .map(|v| match v {
                Some(v) => value_to_py(py, *v),
                None => Ok(py.None()),
            })
            .collect()
    }
}
