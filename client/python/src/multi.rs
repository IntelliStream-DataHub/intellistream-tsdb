//! The multi-series calls of Client, Pool and Db (protocol version 10): one request for many
//! series, answered in series order, each series with its own status.
//!
//! The arguments become a `Req` while the GIL is held; the call and the concatenation of the
//! results run detached (`run`), and only then are the columns handed to numpy (`finish`).
//! A `Many` keeps every series' rows or buckets in one set of columns with offsets, the way
//! BucketsMany does, so whole-batch numpy work needs no Python loop; `r[i]` slices them.

use std::ops::Bound as Range;

use pyo3::exceptions::{PyAttributeError, PyIndexError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyList, PySlice, PyTuple};

use ::tsdb_client as tc;
use tc::{Bitmap, Column, ValueType, Values};

use crate::arrays::{Buckets, Cols, TwaBuckets, use_numpy, vec_to_py};
use crate::{TsdbError, column_to_py, group, opts, ts_column, twa_opts};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Values,
    Buckets,
    Spread,
    Twa,
    Lttb,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Values => "values",
            Kind::Buckets => "buckets",
            Kind::Spread => "spread",
            Kind::Twa => "twa",
            Kind::Lttb => "lttb",
        }
    }
}

/// How each series is bucketed (tc::Buckets, owning its slices).
enum BSpec {
    One,
    Every(i64),
    Intervals(Vec<i64>),
    Group(tc::Group),
    Bounds(Vec<i64>),
}

/// A scalar for every series or one value each.
enum PerSeries {
    All(Option<i64>),
    Each(Vec<i64>),
}

fn per_series(obj: Option<&Bound<'_, PyAny>>, n: usize, what: &str) -> PyResult<PerSeries> {
    let Some(obj) = obj.filter(|o| !o.is_none()) else { return Ok(PerSeries::All(None)) };
    if let Ok(v) = obj.extract::<i64>() {
        return Ok(PerSeries::All(Some(v)));
    }
    let v = ts_column(obj)?.snap().into_owned();
    if v.len() != n {
        return Err(PyValueError::new_err(format!("{what} has {} entries for {n} series", v.len())));
    }
    Ok(PerSeries::Each(v))
}

/// One multi-series call, converted while the GIL is held.
pub(crate) struct Req {
    kind: Kind,
    series: Vec<u32>,
    start: Option<i64>,
    end: Option<i64>,
    starts: Option<Vec<i64>>,
    ends: Option<Vec<i64>>,
    buckets: BSpec,
    opts: tc::QueryOpts,
    twa: tc::TwaOpts,
    threshold: u64,
    threads: u16,
    max_rows: u64,
    raise: bool,
    numpy: Option<bool>,
}

impl Req {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        kind: Kind,
        series: &Bound<'_, PyAny>,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        order: &str,
        limit: Option<u64>,
        threads: u16,
        max_rows: u64,
        errors: &str,
    ) -> PyResult<Req> {
        let raise = match errors {
            "raise" => true,
            "status" => false,
            e => return Err(PyValueError::new_err(format!("errors must be 'raise' or 'status', not {e:?}"))),
        };
        let series = crate::series_column(series)?.snap().into_owned();
        let n = series.len();
        let (start, starts) = match per_series(start, n, "start")? {
            PerSeries::All(v) => (v, None),
            PerSeries::Each(v) => (None, Some(v)),
        };
        let (end, ends) = match per_series(end, n, "end")? {
            PerSeries::All(v) => (v, None),
            PerSeries::Each(v) => (None, Some(v)),
        };
        let opts = opts(order, limit)?;
        Ok(Req {
            kind,
            series,
            start,
            end,
            starts,
            ends,
            buckets: BSpec::One,
            opts,
            twa: tc::TwaOpts { order: opts.order, limit: opts.limit, ..tc::TwaOpts::default() },
            threshold: 0,
            threads,
            max_rows,
            raise,
            numpy: None,
        })
    }

    /// The bucketing of the bucket kinds: at most one of interval_ms, unit and bounds.
    pub(crate) fn buckets(
        mut self,
        interval_ms: Option<&Bound<'_, PyAny>>,
        unit: Option<&str>,
        count: u32,
        timezone: Option<&Bound<'_, PyAny>>,
        bounds: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Req> {
        let interval_ms = interval_ms.filter(|o| !o.is_none());
        let bounds = bounds.filter(|o| !o.is_none());
        if usize::from(interval_ms.is_some()) + usize::from(unit.is_some()) + usize::from(bounds.is_some()) > 1 {
            return Err(PyValueError::new_err("give at most one of interval_ms, unit and bounds"));
        }
        if timezone.is_some_and(|z| !z.is_none()) && unit.is_none() {
            return Err(PyValueError::new_err("timezone needs a unit"));
        }
        self.buckets = if let Some(b) = bounds {
            BSpec::Bounds(ts_column(b)?.snap().into_owned())
        } else if let Some(g) = group(unit, count, timezone)? {
            BSpec::Group(g)
        } else {
            match per_series(interval_ms, self.series.len(), "interval_ms")? {
                PerSeries::All(None) => BSpec::One,
                PerSeries::All(Some(v)) => BSpec::Every(v),
                PerSeries::Each(v) => BSpec::Intervals(v),
            }
        };
        Ok(self)
    }

    pub(crate) fn twa(mut self, method: &str, max_gap_ms: i64) -> PyResult<Req> {
        self.twa = twa_opts(
            method,
            max_gap_ms,
            if self.opts.order == tc::Order::Desc { "desc" } else { "asc" },
            self.opts.limit,
        )?;
        Ok(self)
    }

    pub(crate) fn threshold(mut self, threshold: u64) -> Req {
        self.threshold = threshold;
        self
    }

    pub(crate) fn numpy(mut self, numpy: Option<bool>) -> Req {
        self.numpy = numpy;
        self
    }

    fn range(&self) -> (Range<i64>, Range<i64>) {
        crate::range(self.start, self.end)
    }

    fn b(&self) -> tc::Buckets<'_> {
        match &self.buckets {
            BSpec::One => tc::Buckets::One,
            BSpec::Every(v) => tc::Buckets::Every(*v),
            BSpec::Intervals(v) => tc::Buckets::Intervals(v),
            BSpec::Group(g) => tc::Buckets::Group(g),
            BSpec::Bounds(v) => tc::Buckets::Bounds(v),
        }
    }

    fn m(&self) -> tc::MultiOpts<'_> {
        tc::MultiOpts {
            threads: self.threads,
            max_rows: self.max_rows,
            starts: self.starts.as_deref(),
            ends: self.ends.as_deref(),
        }
    }
}

/// What runs the call.
pub(crate) enum Target<'a> {
    Client(&'a mut tc::Client),
    Pool(&'a tc::Pool),
    Db(&'a tc::Db),
}

macro_rules! on {
    ($t:expr, $m:ident ( $($a:expr),* )) => {
        match $t {
            Target::Client(c) => c.$m($($a),*),
            Target::Pool(p) => p.$m($($a),*),
            Target::Db(d) => d.$m($($a),*),
        }
    };
}

enum Data {
    /// Every series' timestamps, and their values in one column when the series share a type
    /// (else one column each).
    Rows(Vec<i64>, Result<Column, Vec<Column>>),
    Buckets(Box<Cols>),
    Twa(Vec<tc::TwaBucket>),
}

/// The results concatenated with the GIL released.
pub(crate) struct Prepared {
    status: Vec<i32>,
    messages: Vec<Option<String>>,
    offsets: Vec<usize>,
    data: Data,
}

/// Splits per-series results into statuses and the successful ones (failed: empty).
fn split<T: Default>(v: Vec<tc::Result<T>>) -> (Vec<i32>, Vec<Option<String>>, Vec<T>) {
    let mut status = Vec::with_capacity(v.len());
    let mut messages = Vec::with_capacity(v.len());
    let mut ok = Vec::with_capacity(v.len());
    for r in v {
        match r {
            Ok(x) => {
                status.push(0);
                messages.push(None);
                ok.push(x);
            }
            Err(e) => {
                status.push(e.code());
                messages.push(Some(e.to_string()));
                ok.push(T::default());
            }
        }
    }
    (status, messages, ok)
}

fn offsets(lens: impl Iterator<Item = usize>) -> Vec<usize> {
    let mut o = vec![0];
    for l in lens {
        o.push(o[o.len() - 1] + l);
    }
    o
}

/// One column of every series' values, if they share a type (empty ones count as any type).
fn concat(cols: Vec<Column>) -> Result<Column, Vec<Column>> {
    let ty = cols.iter().find(|c| !c.is_empty()).map_or(ValueType::Float32, Column::value_type);
    if cols.iter().any(|c| !c.is_empty() && c.value_type() != ty) {
        return Err(cols);
    }
    let total: usize = cols.iter().map(Column::len).sum();
    let nulls = cols.iter().any(|c| c.valid.is_some());
    let mut values = Values::new(ty);
    let mut valid = Bitmap::default();
    for c in cols {
        if nulls {
            match &c.valid {
                Some(b) => b.iter().for_each(|k| valid.push(k)),
                None => valid.extend_valid(c.len()),
            }
        }
        if c.is_empty() {
            continue;
        }
        match (&mut values, c.values) {
            (Values::Float32(a), Values::Float32(b)) => a.extend(b),
            (Values::Float64(a), Values::Float64(b)) => a.extend(b),
            (Values::Int64(a), Values::Int64(b)) => a.extend(b),
            (Values::Bool(a), Values::Bool(b)) => a.extend(b),
            (Values::State(a), Values::State(b)) => a.extend(b),
            _ => unreachable!("one type checked above"),
        }
    }
    debug_assert_eq!(values.len(), total);
    Ok(Column { values, valid: nulls.then_some(valid) })
}

impl Default for Column2 {
    fn default() -> Self {
        Column2(Column::empty(ValueType::Float32))
    }
}

/// tc::Rows has no Default (its column has a type); failed series get an empty one.
struct Column2(Column);

fn prepare_rows(v: Vec<tc::Result<tc::Rows>>) -> Prepared {
    let (status, messages, rows) = split(v.into_iter().map(|r| r.map(|r| (r.ts, Column2(r.values)))).collect());
    let offsets = offsets(rows.iter().map(|(ts, _)| ts.len()));
    let mut ts = Vec::with_capacity(offsets[offsets.len() - 1]);
    let mut cols = Vec::with_capacity(rows.len());
    for (t, c) in rows {
        ts.extend(t);
        cols.push(c.0);
    }
    Prepared { status, messages, offsets, data: Data::Rows(ts, concat(cols)) }
}

fn prepare_buckets<B>(v: Vec<tc::Result<Vec<B>>>) -> Prepared
where
    for<'a> Cols: From<&'a [B]>,
{
    let (status, messages, parts) = split(v);
    let offsets = offsets(parts.iter().map(Vec::len));
    let all: Vec<B> = parts.into_iter().flatten().collect();
    Prepared { status, messages, offsets, data: Data::Buckets(Box::new(Cols::from(&all[..]))) }
}

/// Runs the call and concatenates its results: call it with the GIL released.
pub(crate) fn run(t: Target<'_>, q: &Req) -> tc::Result<Prepared> {
    let (s, r, o, m) = (&q.series[..], q.range(), q.opts, q.m());
    Ok(match q.kind {
        Kind::Values => prepare_rows(on!(t, query_rows_multi(s, r, o, m))?),
        Kind::Lttb => prepare_rows(on!(t, query_lttb_multi(s, r, q.threshold, o, m))?),
        Kind::Buckets => prepare_buckets(on!(t, query_buckets_multi(s, r, q.b(), o, m))?),
        Kind::Spread => prepare_buckets(on!(t, query_spread_multi(s, r, q.b(), o, m))?),
        Kind::Twa => {
            let (status, messages, parts) = split(on!(t, query_twa_multi(s, r, q.b(), q.twa, m))?);
            let offsets = offsets(parts.iter().map(Vec::len));
            Prepared { status, messages, offsets, data: Data::Twa(parts.into_iter().flatten().collect()) }
        }
    })
}

enum PyData {
    /// Timestamps, then values (one array, or one per series when their types differ).
    Rows(Py<PyAny>, Result<Py<PyAny>, Vec<Py<PyAny>>>),
    Buckets(Py<Buckets>),
    Twa(Py<TwaBuckets>),
}

/// The answer of a multi-series call (query_*_multi()).
///
/// len(r) is the number of series, r.status their statuses (int32, 0 for
/// success, else the TSDB_ERR_* code) and r.offsets (int64, one more than
/// the series) where each series' rows or buckets begin in the columns,
/// which hold every series' results concatenated in series order. r[i] is
/// series i's result as the single call returns it, and raises TsdbError
/// for a series that failed. Columns: r.ts and r.values for the values and
/// LTTB calls; r.buckets (a Buckets or TwaBuckets) for the bucket calls,
/// whose columns read straight from r too (r.counts, r.sum, r.twa, ...).
/// Without numpy the columns are lists.
#[pyclass(frozen, sequence, module = "tsdb_client")]
pub(crate) struct Many {
    kind: Kind,
    series: Vec<u32>,
    status: Vec<i32>,
    status_py: Py<PyAny>,
    messages: Vec<Option<String>>,
    offsets: Vec<usize>,
    offsets_py: Py<PyAny>,
    data: PyData,
}

impl Many {
    fn error(&self, py: Python<'_>, me: Option<Py<Many>>, i: usize) -> PyErr {
        let msg = self.messages[i].as_deref().unwrap_or("failed");
        let err = TsdbError::new_err(format!("series {} (index {i}): {msg}", self.series[i]));
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

    fn get<'py>(slf: &Bound<'py, Many>, i: usize) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let me = slf.get();
        if me.status[i] != 0 {
            return Err(me.error(py, Some(slf.clone().unbind()), i));
        }
        let (a, b) = (me.offsets[i], me.offsets[i + 1]);
        Ok(match &me.data {
            PyData::Rows(ts, values) => {
                let sl = PySlice::new(py, a as isize, b as isize, 1);
                let t = ts.bind(py).get_item(&sl)?;
                let v = match values {
                    Ok(v) => v.bind(py).get_item(&sl)?,
                    Err(each) => each[i].bind(py).clone(),
                };
                PyTuple::new(py, [t, v])?.into_any()
            }
            PyData::Buckets(all) => Bound::new(py, all.get().slice(a, b))?.into_any(),
            PyData::Twa(all) => Bound::new(py, all.get().slice(a, b))?.into_any(),
        })
    }
}

/// The Many of a call; with errors="raise" the first failed series raises.
pub(crate) fn finish(py: Python<'_>, q: Req, p: Prepared) -> PyResult<Py<Many>> {
    let np = use_numpy(py, None)?;
    let data = match p.data {
        Data::Rows(ts, values) => {
            let rows_np = use_numpy(py, q.numpy)?;
            let ts = vec_to_py(py, rows_np, ts)?.unbind();
            let values = match values {
                Ok(c) => Ok(column_to_py(py, rows_np, c)?.unbind()),
                Err(each) => Err(each
                    .into_iter()
                    .map(|c| column_to_py(py, rows_np, c).map(Bound::unbind))
                    .collect::<PyResult<Vec<_>>>()?),
            };
            PyData::Rows(ts, values)
        }
        Data::Buckets(cols) => PyData::Buckets(Py::new(py, Buckets::from_cols(*cols))?),
        Data::Twa(v) => PyData::Twa(Py::new(py, TwaBuckets::from(v))?),
    };
    let offsets_py = vec_to_py(py, np, p.offsets.iter().map(|&x| x as i64).collect::<Vec<i64>>())?.unbind();
    let status_py = vec_to_py(py, np, p.status.clone())?.unbind();
    let first_err = p.status.iter().position(|&s| s != 0);
    let many = Py::new(
        py,
        Many {
            kind: q.kind,
            series: q.series,
            status: p.status,
            status_py,
            messages: p.messages,
            offsets: p.offsets,
            offsets_py,
            data,
        },
    )?;
    match first_err {
        Some(i) if q.raise => Err(many.get().error(py, Some(many.clone_ref(py)), i)),
        _ => Ok(many),
    }
}

#[pymethods]
impl Many {
    /// The call that answered: "values", "buckets", "spread", "twa" or "lttb".
    #[getter]
    fn kind(&self) -> &'static str {
        self.kind.name()
    }

    /// The series ids, in the order asked.
    #[getter]
    fn series(&self) -> Vec<u32> {
        self.series.clone()
    }

    /// Each series' status: 0, or the TSDB_ERR_* code it failed with (int32).
    #[getter]
    fn status(&self, py: Python<'_>) -> Py<PyAny> {
        self.status_py.clone_ref(py)
    }

    /// Where each series' rows or buckets begin in the columns (int64, series + 1 entries).
    #[getter]
    fn offsets(&self, py: Python<'_>) -> Py<PyAny> {
        self.offsets_py.clone_ref(py)
    }

    /// True when every series succeeded.
    #[getter]
    fn ok(&self) -> bool {
        self.status.iter().all(|&s| s == 0)
    }

    /// The TsdbError of series i, or None if it succeeded.
    fn error_of(slf: &Bound<'_, Many>, i: usize) -> PyResult<Option<PyErr>> {
        let me = slf.get();
        if i >= me.status.len() {
            return Err(PyIndexError::new_err("series index out of range"));
        }
        Ok((me.status[i] != 0).then(|| me.error(slf.py(), Some(slf.clone().unbind()), i)))
    }

    /// Every series' timestamps, concatenated (the values and LTTB calls).
    #[getter]
    fn ts(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.data {
            PyData::Rows(ts, _) => Ok(ts.clone_ref(py)),
            _ => Err(PyAttributeError::new_err("ts: only the values and LTTB calls have it; use .buckets")),
        }
    }

    /// Every series' values, concatenated; TypeError when the series have different types (use r[i]).
    #[getter]
    fn values(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.data {
            PyData::Rows(_, Ok(v)) => Ok(v.clone_ref(py)),
            PyData::Rows(_, Err(_)) => {
                Err(PyTypeError::new_err("values: the series have different value types; use r[i]"))
            }
            _ => Err(PyAttributeError::new_err("values: only the values and LTTB calls have it; use .buckets")),
        }
    }

    /// Every series' buckets, concatenated: a Buckets (TwaBuckets for query_twa_multi()).
    #[getter]
    fn buckets(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.data {
            PyData::Buckets(b) => Ok(b.clone_ref(py).into_any()),
            PyData::Twa(b) => Ok(b.clone_ref(py).into_any()),
            PyData::Rows(..) => Err(PyAttributeError::new_err("buckets: only the bucket calls have it; use .ts")),
        }
    }

    /// The columns of .buckets (start, counts, sum, twa, ...), read from the result itself.
    fn __getattr__<'py>(&self, py: Python<'py>, name: &str) -> PyResult<Bound<'py, PyAny>> {
        if name.starts_with('_') {
            return Err(PyAttributeError::new_err(name.to_owned()));
        }
        match &self.data {
            PyData::Buckets(b) => b.bind(py).getattr(name),
            PyData::Twa(b) => b.bind(py).getattr(name),
            PyData::Rows(..) => Err(PyAttributeError::new_err(format!("'Many' object has no attribute {name:?}"))),
        }
    }

    fn __len__(&self) -> usize {
        self.status.len()
    }

    /// Series i's result, as the single call returns it; a list of them for a slice.
    fn __getitem__<'py>(slf: &Bound<'py, Many>, key: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let n = slf.get().status.len();
        if let Ok(s) = key.cast::<PySlice>() {
            let ix = s.indices(n as isize)?;
            let idx = (0..ix.slicelength).map(|k| (ix.start + k as isize * ix.step) as usize);
            let items = idx.map(|j| Many::get(slf, j)).collect::<PyResult<Vec<_>>>()?;
            return Ok(PyList::new(py, items)?.into_any());
        }
        let i: isize = key.extract()?;
        let j = if i < 0 { i + n as isize } else { i };
        if j < 0 || j >= n as isize {
            return Err(PyIndexError::new_err("series index out of range"));
        }
        Many::get(slf, j as usize)
    }

    fn __repr__(&self) -> String {
        let failed = self.status.iter().filter(|&&s| s != 0).count();
        let rows = self.offsets.last().copied().unwrap_or(0);
        format!("Many({}, {} series, {failed} failed, {rows} rows)", self.kind.name(), self.status.len())
    }
}
