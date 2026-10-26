//! Columns in and out without work under the GIL.
//!
//! In: a C-contiguous buffer of the right element type in native byte order (a numpy array,
//! `array.array`) is kept as a buffer export, which keeps the exporter from freeing or resizing
//! it, and copied once with the GIL released, where the call uses it (`Col::snap`). The engine
//! and the network only ever read that copy: another thread may write the array meanwhile
//! (another Python thread, or numpy's own loops, which run without the GIL), and the engine
//! reads some inputs twice (a series id checked, then used as an index), so reading the
//! caller's memory in place could let it act on two different values of one element. Anything
//! else (other dtypes, strided views, masked arrays, sequences, big-endian arrays) is converted
//! into a Rust vector while the GIL is held, as before.
//!
//! Out: query results are vectors filled with the GIL released; numpy takes them over without a
//! copy (`IntoPyArray`: the array's base owns the vector). Aggregates come back as `Buckets`, a
//! column per field instead of an object per bucket; it still reads as a sequence of `Bucket`.
//! Batch calls return a pair of arrays per raw query (each over its own vector), and the buckets
//! of all aggregate queries as one set of columns with offsets. Without numpy, columns and
//! offsets are lists; raw batch results need numpy.

use std::borrow::Cow;
use std::collections::HashMap;

use numpy::{IntoPyArray, PyArray1};
use pyo3::buffer::{Element, PyBuffer};
use pyo3::exceptions::{PyImportError, PyIndexError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyList, PySlice, PyTuple};

use ::tsdb_client as tc;
use tc::{Column, ColumnRef, ValueType, ValuesRef};

use crate::{Bucket, py_column};

// ------------------------------------------------------------------ in

/// `obj`'s buffer as T, or `None` if it is none or not in native byte order.
///
/// PyO3 (0.29) accepts a big-endian format ('>' or '!') on a little-endian machine as native
/// and rejects '<' (`is_matching_endian` has them the wrong way round): its elements would be
/// read byte-swapped. Those go the slow way instead, element by element through Python.
pub(crate) fn native<T: Element>(obj: &Bound<'_, PyAny>) -> Option<PyBuffer<T>> {
    let b = PyBuffer::<T>::get(obj).ok()?;
    let foreign: &[u8] = if cfg!(target_endian = "little") { b">!" } else { b"<" };
    match b.format().to_bytes().first() {
        Some(c) if foreign.contains(c) => None,
        _ => Some(b),
    }
}

/// A column kept as a Python buffer (copied where it is used), or converted into a vector.
pub(crate) enum Col<T: Element> {
    Borrowed(PyBuffer<T>),
    Owned(Vec<T>),
}

impl<T: Element + Copy> Col<T> {
    /// `obj` as a buffer of T: kept when C-contiguous, else copied; `None` for an object that
    /// is not a native buffer of T.
    pub(crate) fn buffer(obj: &Bound<'_, PyAny>) -> PyResult<Option<Self>> {
        match native::<T>(obj) {
            Some(b) if b.is_c_contiguous() => Ok(Some(Col::Borrowed(b))),
            Some(b) => Ok(Some(Col::Owned(b.to_vec(obj.py())?))),
            None => Ok(None),
        }
    }

    /// The column, read once: a buffer is copied (one memcpy). Call it where the column is
    /// used, with the GIL released, and pass on only the result (see the file header).
    pub(crate) fn snap(&self) -> Cow<'_, [T]> {
        match self {
            Col::Owned(v) => Cow::Borrowed(v),
            Col::Borrowed(b) if b.item_count() == 0 => Cow::Owned(Vec::new()),
            Col::Borrowed(b) => {
                let n = b.item_count();
                let mut v = Vec::<T>::with_capacity(n);
                // SAFETY: native() checked the element type and alignment, and buffer() keeps only
                // a C-contiguous buffer, so it holds n Ts at buf_ptr(); the export lasts as long as
                // self, so the exporter can neither free nor resize it. The bytes are copied through
                // raw pointers, never a reference into memory another thread may be writing; a racing
                // write leaves an element old, new or torn, each a valid i64, u32, f32 or f64.
                unsafe {
                    std::ptr::copy_nonoverlapping(b.buf_ptr() as *const T, v.as_mut_ptr(), n);
                    v.set_len(n);
                }
                Cow::Owned(v)
            }
        }
    }

    /// The first element, read once.
    pub(crate) fn first(&self) -> Option<T> {
        match self {
            Col::Owned(v) => v.first().copied(),
            Col::Borrowed(b) if b.item_count() == 0 => None,
            // SAFETY: as in snap(), for one element
            Col::Borrowed(b) => Some(unsafe { std::ptr::read(b.buf_ptr() as *const T) }),
        }
    }
}

/// Typed insert values: read in place when the buffer has the series' type, else a `Column`.
pub(crate) enum ValCol {
    F32(Col<f32>),
    F64(Col<f64>),
    I64(Col<i64>),
    State(Col<u32>),
    Owned(Column),
}

/// Typed insert values read once (`ValCol::snap`).
pub(crate) enum ValSnap<'a> {
    F32(Cow<'a, [f32]>),
    F64(Cow<'a, [f64]>),
    I64(Cow<'a, [i64]>),
    State(Cow<'a, [u32]>),
    Owned(&'a Column),
}

impl ValCol {
    /// The values, read once; see `Col::snap`.
    pub(crate) fn snap(&self) -> ValSnap<'_> {
        match self {
            ValCol::F32(v) => ValSnap::F32(v.snap()),
            ValCol::F64(v) => ValSnap::F64(v.snap()),
            ValCol::I64(v) => ValSnap::I64(v.snap()),
            ValCol::State(v) => ValSnap::State(v.snap()),
            ValCol::Owned(c) => ValSnap::Owned(c),
        }
    }
}

impl<'a> From<&'a ValSnap<'_>> for ColumnRef<'a> {
    fn from(c: &'a ValSnap<'_>) -> ColumnRef<'a> {
        match c {
            ValSnap::F32(v) => ColumnRef::new(ValuesRef::Float32(v)),
            ValSnap::F64(v) => ColumnRef::new(ValuesRef::Float64(v)),
            ValSnap::I64(v) => ColumnRef::new(ValuesRef::Int64(v)),
            ValSnap::State(v) => ColumnRef::new(ValuesRef::State(v)),
            ValSnap::Owned(c) => (*c).into(),
        }
    }
}

/// Values of type `ty`, as `py_column` takes them (masked arrays and sequences included).
pub(crate) fn values_column(obj: &Bound<'_, PyAny>, ty: ValueType) -> PyResult<ValCol> {
    // a masked array exports the buffer of its data, without the mask
    if !obj.hasattr("mask")? {
        let c = match ty {
            ValueType::Float32 => Col::buffer(obj)?.map(ValCol::F32),
            ValueType::Float64 => Col::buffer(obj)?.map(ValCol::F64),
            ValueType::Int64 => Col::buffer(obj)?.map(ValCol::I64),
            ValueType::State => Col::buffer(obj)?.map(ValCol::State),
            _ => None,
        };
        if let Some(c) = c {
            return Ok(c);
        }
    }
    Ok(ValCol::Owned(py_column(obj, ty)?))
}

/// Bucket starts or query bounds: None (every query gets `default`), one int for every query,
/// or a sequence or int64 array of one per query.
pub(crate) fn per_query(obj: Option<&Bound<'_, PyAny>>, n: usize, default: i64, what: &str) -> PyResult<Vec<i64>> {
    let Some(obj) = obj.filter(|o| !o.is_none()) else { return Ok(vec![default; n]) };
    if let Ok(v) = obj.extract::<i64>() {
        return Ok(vec![v; n]);
    }
    let v = crate::ts_column(obj)?.snap().into_owned();
    if v.len() != n {
        return Err(PyValueError::new_err(format!("{what} has {} entries for {n} queries", v.len())));
    }
    Ok(v)
}

/// The queries of a batch call: one per entry of `series`.
pub(crate) fn range_queries(
    series: &Bound<'_, PyAny>,
    start: Option<&Bound<'_, PyAny>>,
    end: Option<&Bound<'_, PyAny>>,
) -> PyResult<Vec<tc::RangeQuery>> {
    let series = crate::series_column(series)?.snap().into_owned();
    let (start, end) =
        (per_query(start, series.len(), i64::MIN, "start")?, per_query(end, series.len(), i64::MAX, "end")?);
    Ok(series.iter().zip(start).zip(end).map(|((&s, a), b)| tc::RangeQuery::new(s, a, b)).collect())
}

/// The queries of query_agg_many(): one per entry of `series`, with one bucket length for all
/// or one each.
pub(crate) fn agg_queries(
    series: &Bound<'_, PyAny>,
    start: Option<&Bound<'_, PyAny>>,
    end: Option<&Bound<'_, PyAny>>,
    interval_ms: Option<&Bound<'_, PyAny>>,
) -> PyResult<Vec<tc::AggQuery>> {
    let q = range_queries(series, start, end)?;
    let iv = per_query(interval_ms, q.len(), 0, "interval_ms")?;
    Ok(q.iter().zip(iv).map(|(q, iv)| tc::AggQuery::new(q.series, q.start, q.end, iv)).collect())
}

/// What the rows of one insert of a batch go to.
pub(crate) enum Dest {
    One(u32),
    Each(Col<u32>),
}

impl Dest {
    /// The series whose type the values take: the only one, or the first row's.
    fn first(&self) -> Option<u32> {
        match self {
            Dest::One(s) => Some(*s),
            Dest::Each(c) => c.first(),
        }
    }
}

/// One insert of insert_many(), converted while the GIL is held.
pub(crate) struct Insert {
    dest: Dest,
    ts: Col<i64>,
    values: ValCol,
}

/// An insert's columns read once (`Insert::snap`).
pub(crate) struct InsertSnap<'a> {
    one: u32,
    each: Option<Cow<'a, [u32]>>,
    ts: Cow<'a, [i64]>,
    values: ValSnap<'a>,
}

impl Insert {
    fn snap(&self) -> InsertSnap<'_> {
        let (one, each) = match &self.dest {
            Dest::One(s) => (*s, None),
            Dest::Each(c) => (0, Some(c.snap())),
        };
        InsertSnap { one, each, ts: self.ts.snap(), values: self.values.snap() }
    }
}

impl InsertSnap<'_> {
    fn as_batch(&self) -> tc::InsertBatch<'_> {
        let target = match &self.each {
            None => tc::Target::Series(self.one),
            Some(s) => tc::Target::Each(s),
        };
        tc::InsertBatch { target, ts: &self.ts, values: (&self.values).into() }
    }
}

/// Runs `f` on the inserts as the client library's batches, each column read once (see
/// `Col::snap`): call it with the GIL released.
pub(crate) fn with_batches<T>(b: &[Insert], f: impl FnOnce(&[tc::InsertBatch<'_>]) -> T) -> T {
    let snaps: Vec<InsertSnap<'_>> = b.iter().map(Insert::snap).collect();
    f(&snaps.iter().map(InsertSnap::as_batch).collect::<Vec<_>>())
}

/// `e`, raised for entry `index` of a batch call, as the same exception naming the entry: the
/// message starts with "batch entry {index}: " and the `index` attribute holds it.
pub(crate) fn entry_err(py: Python<'_>, index: usize, e: PyErr) -> PyErr {
    let err = PyErr::from_type(e.get_type(py), format!("batch entry {index}: {}", e.value(py)));
    let _ = err.value(py).setattr("index", index);
    if let Ok(code) = e.value(py).getattr("code") {
        let _ = err.value(py).setattr("code", code);
    }
    err.set_cause(py, Some(e));
    err
}

/// The batches of insert_many(): `(series, ts, values)` tuples, series an id or a column of ids.
///
/// The values' type is `value_type`, else the series' (the first row's for a column of ids;
/// float32 without rows), as the one-insert calls take it: `types` looks up each series once,
/// one result per id. Values of another type are converted (float64 into a float32 series, a
/// list into an int64 series), as insert_series_values() converts them.
pub(crate) fn inserts(
    batches: &Bound<'_, PyAny>,
    value_type: Option<ValueType>,
    types: impl FnOnce(&[u32]) -> PyResult<Vec<tc::Result<ValueType>>>,
) -> PyResult<Vec<Insert>> {
    let py = batches.py();
    let mut parsed = Vec::with_capacity(batches.len().unwrap_or(0));
    for (i, b) in batches.try_iter()?.enumerate() {
        let parse = || -> PyResult<(Dest, Col<i64>, Bound<'_, PyAny>)> {
            let b = b?;
            let t =
                b.cast::<PyTuple>().map_err(|_| PyTypeError::new_err("each batch is a tuple (series, ts, values)"))?;
            if t.len() != 3 {
                return Err(PyTypeError::new_err("each batch is a tuple (series, ts, values)"));
            }
            let (s, ts, v) = (t.get_item(0)?, t.get_item(1)?, t.get_item(2)?);
            let dest = match s.extract::<u32>() {
                Ok(id) => Dest::One(id),
                Err(_) => Dest::Each(crate::series_column(&s)?),
            };
            Ok((dest, crate::ts_column(&ts)?, v))
        };
        parsed.push(parse().map_err(|e| entry_err(py, i, e))?);
    }
    let mut known: HashMap<u32, tc::Result<ValueType>> = HashMap::new();
    if value_type.is_none() {
        let mut ids: Vec<u32> = parsed.iter().filter_map(|p| p.0.first()).collect();
        ids.sort_unstable();
        ids.dedup();
        if !ids.is_empty() {
            let t = types(&ids)?;
            known = ids.into_iter().zip(t).collect();
        }
    }
    let mut out = Vec::with_capacity(parsed.len());
    for (i, (dest, ts, v)) in parsed.into_iter().enumerate() {
        let ty = match (value_type, dest.first()) {
            (Some(t), _) => t,
            (None, None) => ValueType::Float32,
            (None, Some(id)) => match known.get(&id) {
                Some(Ok(t)) => *t,
                _ => {
                    let e = known
                        .remove(&id)
                        .and_then(Result::err)
                        .unwrap_or_else(|| tc::Error::Server(tc::Status::NotFound));
                    return Err(crate::to_pyerr(py, tc::Error::Batch { index: i, error: Box::new(e) }));
                }
            },
        };
        let values = values_column(&v, ty).map_err(|e| entry_err(py, i, e))?;
        out.push(Insert { dest, ts, values });
    }
    Ok(out)
}

// ------------------------------------------------------------------ out

static NUMPY: PyOnceLock<Py<PyModule>> = PyOnceLock::new();

/// The numpy module, imported once.
pub(crate) fn numpy(py: Python<'_>) -> PyResult<&Bound<'_, PyModule>> {
    Ok(NUMPY.get_or_try_init(py, || py.import("numpy").map(Bound::unbind))?.bind(py))
}

/// Whether to answer with numpy: required (`Some(true)`), refused (`Some(false)`) or if importable.
pub(crate) fn use_numpy(py: Python<'_>, numpy: Option<bool>) -> PyResult<bool> {
    Ok(match numpy {
        Some(false) => false,
        Some(true) => self::numpy(py).map(|_| true)?,
        None => self::numpy(py).is_ok(),
    })
}

/// `v` as a numpy array (taking the vector over) or a list.
pub(crate) fn vec_to_py<'py, T>(py: Python<'py>, np: bool, v: Vec<T>) -> PyResult<Bound<'py, PyAny>>
where
    T: numpy::Element + for<'a> IntoPyObject<'py>,
{
    Ok(if np { v.into_pyarray(py).into_any() } else { PyList::new(py, v)?.into_any() })
}

/// `v` copied into a numpy array, or a list.
fn slice_to_py<'py, T>(py: Python<'py>, np: bool, v: &[T]) -> PyResult<Bound<'py, PyAny>>
where
    T: numpy::Element + Copy + IntoPyObject<'py>,
{
    Ok(if np { PyArray1::from_slice(py, v).into_any() } else { PyList::new(py, v.iter().copied())?.into_any() })
}

/// The fields of many buckets, a vector each.
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct Cols {
    start: Vec<i64>,
    count: Vec<u64>,
    sum: Vec<f64>,
    min: Vec<f64>,
    max: Vec<f64>,
    isum: Vec<i64>,
    imin: Vec<i64>,
    imax: Vec<i64>,
}

impl Cols {
    fn len(&self) -> usize {
        self.start.len()
    }

    fn bucket(&self, i: usize) -> Bucket {
        Bucket::from(tc::Bucket {
            start: self.start[i],
            count: self.count[i],
            sum: self.sum[i],
            min: self.min[i],
            max: self.max[i],
            isum: self.isum[i],
            imin: self.imin[i],
            imax: self.imax[i],
        })
    }

    fn pick(&self, idx: impl Iterator<Item = usize> + Clone) -> Cols {
        fn p<T: Copy>(v: &[T], idx: impl Iterator<Item = usize>) -> Vec<T> {
            idx.map(|i| v[i]).collect()
        }
        Cols {
            start: p(&self.start, idx.clone()),
            count: p(&self.count, idx.clone()),
            sum: p(&self.sum, idx.clone()),
            min: p(&self.min, idx.clone()),
            max: p(&self.max, idx.clone()),
            isum: p(&self.isum, idx.clone()),
            imin: p(&self.imin, idx.clone()),
            imax: p(&self.imax, idx),
        }
    }

    /// Equal when every field is, floats bit for bit (as Bucket.__eq__).
    fn same(&self, o: &Cols) -> bool {
        let bits = |v: &[f64]| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>();
        (&self.start, &self.count, &self.isum, &self.imin, &self.imax)
            == (&o.start, &o.count, &o.isum, &o.imin, &o.imax)
            && (bits(&self.sum), bits(&self.min), bits(&self.max)) == (bits(&o.sum), bits(&o.min), bits(&o.max))
    }
}

impl From<&[tc::Bucket]> for Cols {
    fn from(b: &[tc::Bucket]) -> Cols {
        let mut c = Cols::default();
        for x in b {
            c.start.push(x.start);
            c.count.push(x.count);
            c.sum.push(x.sum);
            c.min.push(x.min);
            c.max.push(x.max);
            c.isum.push(x.isum);
            c.imin.push(x.imin);
            c.imax.push(x.imax);
        }
        c
    }
}

/// The buckets of an aggregate query, a column per field.
///
/// It is a sequence of Bucket (a collections.abc.Sequence: len(), indexing,
/// slicing, iteration, index(), count(), + and == with a list of Bucket), so
/// code written for the list it replaces keeps working. Each field is also a
/// column over all buckets, a numpy array (a list without numpy): start, sum,
/// min, max, avg, isum, imin, imax, and counts for the counts (count() is the
/// sequence method): ``b = db.query_agg(s, start, end, 60_000);
/// b.counts.sum(); b.max.max()``. Reading a column copies it out of the
/// result; keep it in a variable rather than reading it in a loop.
#[pyclass(frozen, sequence, module = "tsdb_client")]
pub(crate) struct Buckets {
    cols: Cols,
}

impl From<Vec<tc::Bucket>> for Buckets {
    fn from(b: Vec<tc::Bucket>) -> Buckets {
        Buckets { cols: Cols::from(&b[..]) }
    }
}

impl Buckets {
    fn list<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        PyList::new(py, (0..self.cols.len()).map(|i| self.cols.bucket(i)))
    }

    fn column<'py, T>(&self, py: Python<'py>, v: &[T]) -> PyResult<Bound<'py, PyAny>>
    where
        T: numpy::Element + Copy + IntoPyObject<'py>,
    {
        slice_to_py(py, use_numpy(py, None)?, v)
    }
}

#[pymethods]
impl Buckets {
    /// Bucket starts, ms since the epoch (int64).
    #[getter]
    fn start<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.column(py, &self.cols.start)
    }

    /// Number of known values per bucket (uint64); count() is the sequence method.
    #[getter]
    fn counts<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.column(py, &self.cols.count)
    }

    /// Sums (float64).
    #[getter]
    fn sum<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.column(py, &self.cols.sum)
    }

    /// Smallest values (float64).
    #[getter]
    fn min<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.column(py, &self.cols.min)
    }

    /// Largest values (float64).
    #[getter]
    fn max<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.column(py, &self.cols.max)
    }

    /// Exact sums of int64 and bool series (int64).
    #[getter]
    fn isum<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.column(py, &self.cols.isum)
    }

    /// Exact minimums of int64 and bool series (int64).
    #[getter]
    fn imin<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.column(py, &self.cols.imin)
    }

    /// Exact maximums of int64 and bool series (int64).
    #[getter]
    fn imax<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.column(py, &self.cols.imax)
    }

    /// Means, sum / count (0 for a bucket of nulls only) (float64).
    #[getter]
    fn avg<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let c = &self.cols;
        let avg: Vec<f64> =
            c.sum.iter().zip(&c.count).map(|(&s, &n)| if n == 0 { 0.0 } else { s / n as f64 }).collect();
        self.column(py, &avg)
    }

    fn __len__(&self) -> usize {
        self.cols.len()
    }

    /// A Bucket, or the Buckets of a slice.
    fn __getitem__<'py>(&self, py: Python<'py>, key: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let n = self.cols.len();
        if let Ok(s) = key.cast::<PySlice>() {
            let ix = s.indices(n as isize)?;
            let idx = (0..ix.slicelength).map(move |k| (ix.start + k as isize * ix.step) as usize);
            return Ok(Bound::new(py, Buckets { cols: self.cols.pick(idx) })?.into_any());
        }
        let i: isize = key.extract()?;
        let j = if i < 0 { i + n as isize } else { i };
        if j < 0 || j >= n as isize {
            return Err(PyIndexError::new_err("bucket index out of range"));
        }
        Ok(Bound::new(py, self.cols.bucket(j as usize))?.into_any())
    }

    fn __iter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.list(py)?.try_iter().map(Bound::into_any)
    }

    /// Equal to another Buckets or to a list of equal Bucket objects (as a list: never to a tuple).
    fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        if let Ok(o) = other.cast::<Buckets>() {
            return Ok(self.cols.same(&o.get().cols));
        }
        match other.cast::<PyList>() {
            Ok(l) => self.list(py)?.eq(l),
            Err(_) => Ok(false),
        }
    }

    /// A list of the buckets of both, as list + list.
    fn __add__<'py>(&self, py: Python<'py>, other: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        if !(other.is_instance_of::<PyList>() || other.is_instance_of::<Buckets>()) {
            return Ok(py.NotImplemented().into_bound(py));
        }
        let l = self.list(py)?;
        for x in other.try_iter()? {
            l.append(x?)?;
        }
        Ok(l.into_any())
    }

    /// list + Buckets: a list.
    fn __radd__<'py>(&self, py: Python<'py>, other: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        match other.cast::<PyList>() {
            Ok(l) => Ok(l.add(self.list(py)?)?),
            Err(_) => Ok(py.NotImplemented().into_bound(py)),
        }
    }

    /// Number of buckets equal to `value` (the sequence method; the column is counts).
    fn count(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<usize> {
        self.list(py)?.call_method1("count", (value,))?.extract()
    }

    /// Position of the first bucket equal to `value`; ValueError if there is none.
    #[pyo3(signature = (value, start = 0, stop = isize::MAX))]
    fn index(&self, py: Python<'_>, value: &Bound<'_, PyAny>, start: isize, stop: isize) -> PyResult<usize> {
        self.list(py)?.call_method1("index", (value, start, stop))?.extract()
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!("Buckets({})", self.list(py)?.repr()?))
    }
}

/// Offsets as an int64 array (a list without numpy): `offsets[i]:offsets[i + 1]` is query i.
fn offsets_array(py: Python<'_>, o: &[usize]) -> PyResult<Py<PyAny>> {
    let v: Vec<i64> = o.iter().map(|&x| x as i64).collect();
    Ok(vec_to_py(py, use_numpy(py, None)?, v)?.unbind())
}

/// The buckets of many aggregate queries (query_agg_many()).
///
/// result[i] is the Buckets of query i, and len(result) the number of
/// queries. For whole-batch numpy work, result.buckets holds every query's
/// buckets concatenated (columns start, count, sum, ...) and result.offsets
/// (int64, one more than the queries) where each query's begin:
/// ``b.buckets.counts[b.offsets[i]:b.offsets[i + 1]]`` are query i's counts.
/// Without numpy the columns and offsets are lists. It is a
/// collections.abc.Sequence of Buckets: a slice gives a list of them.
#[pyclass(frozen, sequence, module = "tsdb_client")]
pub(crate) struct BucketsMany {
    all: Py<Buckets>,
    offsets: Vec<usize>,
    offsets_py: Py<PyAny>,
}

impl BucketsMany {
    /// The Buckets of query j (< len).
    fn query(&self, j: usize) -> Buckets {
        Buckets { cols: self.all.get().cols.pick(self.offsets[j]..self.offsets[j + 1]) }
    }

    /// The result of a batch call; `cols` was built with the GIL released.
    pub(crate) fn new(py: Python<'_>, (cols, offsets): (Cols, Vec<usize>)) -> PyResult<BucketsMany> {
        Ok(BucketsMany { all: Py::new(py, Buckets { cols })?, offsets_py: offsets_array(py, &offsets)?, offsets })
    }
}

/// The columns of a batch of buckets, every query's concatenated, and where each query's begin;
/// built with the GIL released.
pub(crate) fn many_cols(parts: Vec<Vec<tc::Bucket>>) -> (Cols, Vec<usize>) {
    let mut offsets = Vec::with_capacity(parts.len() + 1);
    offsets.push(0);
    let all: Vec<tc::Bucket> = parts
        .into_iter()
        .flat_map(|p| {
            offsets.push(offsets.last().copied().unwrap_or(0) + p.len());
            p
        })
        .collect();
    (Cols::from(&all[..]), offsets)
}

/// The rows of a batch of raw queries: a (timestamps, values) tuple of numpy arrays per query,
/// each taking over its query's vectors.
pub(crate) fn points_list(py: Python<'_>, parts: Vec<tc::Points>) -> PyResult<Bound<'_, PyList>> {
    numpy(py).map_err(|e| {
        let err = PyImportError::new_err("query_raw_many() returns numpy arrays: it needs numpy (pip install numpy)");
        err.set_cause(py, Some(e));
        err
    })?;
    let items = parts
        .into_iter()
        .map(|p| PyTuple::new(py, [p.ts.into_pyarray(py).into_any(), p.values.into_pyarray(py).into_any()]))
        .collect::<PyResult<Vec<_>>>()?;
    PyList::new(py, items)
}

#[pymethods]
impl BucketsMany {
    /// Every query's buckets, concatenated in query order.
    #[getter]
    fn buckets(&self, py: Python<'_>) -> Py<Buckets> {
        self.all.clone_ref(py)
    }

    /// Where each query's buckets begin in buckets (int64, queries + 1 entries).
    #[getter]
    fn offsets(&self, py: Python<'_>) -> Py<PyAny> {
        self.offsets_py.clone_ref(py)
    }

    fn __len__(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    /// The Buckets of query i; a list of them for a slice.
    fn __getitem__<'py>(&self, py: Python<'py>, key: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let n = self.__len__();
        if let Ok(s) = key.cast::<PySlice>() {
            let ix = s.indices(n as isize)?;
            let idx = (0..ix.slicelength).map(|k| (ix.start + k as isize * ix.step) as usize);
            return Ok(PyList::new(py, idx.map(|j| self.query(j)))?.into_any());
        }
        let i: isize = key.extract()?;
        let j = if i < 0 { i + n as isize } else { i };
        if j < 0 || j >= n as isize {
            return Err(PyIndexError::new_err("query index out of range"));
        }
        Ok(Bound::new(py, self.query(j as usize))?.into_any())
    }

    /// Equal to another BucketsMany or a list of equal Buckets (or lists of Bucket).
    fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        if let Ok(o) = other.cast::<BucketsMany>() {
            let o = o.get();
            return Ok(self.offsets == o.offsets && self.all.get().cols.same(&o.all.get().cols));
        }
        let Ok(l) = other.cast::<PyList>() else { return Ok(false) };
        if l.len() != self.__len__() {
            return Ok(false);
        }
        for (j, x) in l.iter().enumerate() {
            if !Bound::new(py, self.query(j))?.eq(x)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Number of queries whose buckets equal `value` (the sequence method).
    fn count(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<usize> {
        let mut k = 0;
        for j in 0..self.__len__() {
            k += usize::from(Bound::new(py, self.query(j))?.eq(value)?);
        }
        Ok(k)
    }

    /// Position of the first query whose buckets equal `value`; ValueError if there is none.
    #[pyo3(signature = (value, start = 0, stop = isize::MAX))]
    fn index(&self, py: Python<'_>, value: &Bound<'_, PyAny>, start: isize, stop: isize) -> PyResult<usize> {
        let all = PyList::new(py, (0..self.__len__()).map(|j| self.query(j)))?;
        all.call_method1("index", (value, start, stop))?.extract()
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        format!("BucketsMany({} queries, {} buckets)", self.__len__(), self.all.bind(py).get().cols.len())
    }
}
