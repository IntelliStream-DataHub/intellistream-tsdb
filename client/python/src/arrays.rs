//! Columns in and out without work under the GIL.
//!
//! In: a C-contiguous buffer of the right element type (a numpy array, `array.array`) is read in
//! place: the call holds the buffer export, which keeps the exporter from freeing or resizing it,
//! and the engine or the network reads it with the GIL released. Anything else (other dtypes,
//! strided views, masked arrays, sequences) is converted into a Rust vector first, as before.
//!
//! Out: query results are vectors filled with the GIL released; numpy takes them over without a
//! copy (`IntoPyArray`: the array's base owns the vector). Aggregates come back as `Buckets`, a
//! column per field instead of an object per bucket; it still reads as a sequence of `Bucket`.
//! Batch calls return a pair of arrays per raw query (each over its own vector), and the buckets
//! of all aggregate queries as one set of columns with offsets.

use std::ops::Deref;

use numpy::{IntoPyArray, PyArray1};
use pyo3::buffer::{Element, PyBuffer};
use pyo3::exceptions::{PyIndexError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyList, PySlice, PyTuple};

use ::tsdb_client as tc;
use tc::{Column, ColumnRef, ValueType, ValuesRef};

use crate::{Bucket, py_column};

// ------------------------------------------------------------------ in

/// A column read in place from a Python buffer, or converted into a vector.
pub(crate) enum Col<T: Element> {
    Borrowed(PyBuffer<T>),
    Owned(Vec<T>),
}

impl<T: Element + Copy> Deref for Col<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        match self {
            Col::Owned(v) => v,
            Col::Borrowed(b) if b.item_count() == 0 => &[],
            // SAFETY: PyBuffer::get checked the element type and alignment, and `buffer` only
            // borrows a C-contiguous one, so it holds item_count() Ts at buf_ptr(). The export
            // lasts as long as `self`: the exporter can neither free nor resize the memory. Another
            // Python thread writing to the array during the call leaves the values read undefined,
            // as in numpy's own loops that run without the GIL.
            Col::Borrowed(b) => unsafe { std::slice::from_raw_parts(b.buf_ptr() as *const T, b.item_count()) },
        }
    }
}

impl<T: Element + Copy> Col<T> {
    /// `obj` as a buffer of T: read in place when C-contiguous, else copied; `None` for an
    /// object that is not a buffer of T.
    pub(crate) fn buffer(obj: &Bound<'_, PyAny>) -> PyResult<Option<Self>> {
        match PyBuffer::<T>::get(obj) {
            Ok(b) if b.is_c_contiguous() => Ok(Some(Col::Borrowed(b))),
            Ok(b) => Ok(Some(Col::Owned(b.to_vec(obj.py())?))),
            Err(_) => Ok(None),
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

impl<'a> From<&'a ValCol> for ColumnRef<'a> {
    fn from(c: &'a ValCol) -> ColumnRef<'a> {
        match c {
            ValCol::F32(v) => ColumnRef::new(ValuesRef::Float32(v)),
            ValCol::F64(v) => ColumnRef::new(ValuesRef::Float64(v)),
            ValCol::I64(v) => ColumnRef::new(ValuesRef::Int64(v)),
            ValCol::State(v) => ColumnRef::new(ValuesRef::State(v)),
            ValCol::Owned(c) => c.into(),
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

/// The type a numpy array's dtype names (float32, float64, int64, bool, uint32 for state codes);
/// `None` for anything else.
pub(crate) fn dtype_type(obj: &Bound<'_, PyAny>) -> PyResult<Option<ValueType>> {
    let Ok(dtype) = obj.getattr("dtype") else { return Ok(None) };
    let (kind, size): (String, usize) = (dtype.getattr("kind")?.extract()?, dtype.getattr("itemsize")?.extract()?);
    Ok(match (kind.as_str(), size) {
        ("f", 4) => Some(ValueType::Float32),
        ("f", 8) => Some(ValueType::Float64),
        ("i", 8) => Some(ValueType::Int64),
        ("b", 1) => Some(ValueType::Bool),
        ("u", 4) => Some(ValueType::State),
        _ => None,
    })
}

/// Bucket starts or query bounds: None (every query gets `default`), one int for every query,
/// or a sequence or int64 array of one per query.
pub(crate) fn per_query(obj: Option<&Bound<'_, PyAny>>, n: usize, default: i64, what: &str) -> PyResult<Vec<i64>> {
    let Some(obj) = obj.filter(|o| !o.is_none()) else { return Ok(vec![default; n]) };
    if let Ok(v) = obj.extract::<i64>() {
        return Ok(vec![v; n]);
    }
    let v = crate::ts_column(obj)?;
    if v.len() != n {
        return Err(PyValueError::new_err(format!("{what} has {} entries for {n} queries", v.len())));
    }
    Ok(v.to_vec())
}

/// The queries of a batch call: one per entry of `series`.
pub(crate) fn range_queries(
    series: &Bound<'_, PyAny>,
    start: Option<&Bound<'_, PyAny>>,
    end: Option<&Bound<'_, PyAny>>,
) -> PyResult<Vec<tc::RangeQuery>> {
    let series = crate::series_column(series)?;
    let (start, end) = (per_query(start, series.len(), i64::MIN, "start")?, per_query(end, series.len(), i64::MAX, "end")?);
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

/// One insert of insert_many(), converted while the GIL is held.
pub(crate) struct Insert {
    dest: Dest,
    ts: Col<i64>,
    values: ValCol,
}

impl Insert {
    pub(crate) fn as_batch(&self) -> tc::InsertBatch<'_> {
        let target = match &self.dest {
            Dest::One(s) => tc::Target::Series(*s),
            Dest::Each(s) => tc::Target::Each(s),
        };
        tc::InsertBatch { target, ts: &self.ts, values: (&self.values).into() }
    }
}

/// The batches of insert_many(): `(series, ts, values)` tuples, series an id or a column of ids.
/// Without `value_type` the type comes from the values' numpy dtype (float32 for anything else).
pub(crate) fn inserts(batches: &Bound<'_, PyAny>, value_type: Option<ValueType>) -> PyResult<Vec<Insert>> {
    let mut out = Vec::with_capacity(batches.len().unwrap_or(0));
    for b in batches.try_iter()? {
        let b = b?;
        let t = b.cast::<PyTuple>().map_err(|_| PyTypeError::new_err("each batch is a tuple (series, ts, values)"))?;
        if t.len() != 3 {
            return Err(PyTypeError::new_err("each batch is a tuple (series, ts, values)"));
        }
        let (s, ts, v) = (t.get_item(0)?, t.get_item(1)?, t.get_item(2)?);
        let dest = match s.extract::<u32>() {
            Ok(id) => Dest::One(id),
            Err(_) => Dest::Each(crate::series_column(&s)?),
        };
        let ty = match value_type {
            Some(ty) => ty,
            None => {
                let data = if v.hasattr("mask")? { v.getattr("data")? } else { v.clone() };
                dtype_type(&data)?.unwrap_or(ValueType::Float32)
            }
        };
        out.push(Insert { dest, ts: crate::ts_column(&ts)?, values: values_column(&v, ty)? });
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
        (&self.start, &self.count, &self.isum, &self.imin, &self.imax) == (&o.start, &o.count, &o.isum, &o.imin, &o.imax)
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
/// It reads as a sequence of Bucket (len(), indexing, slicing, iteration,
/// == against a list of Bucket), so code written for a list keeps working,
/// and each field is a numpy array over all buckets (a list without numpy):
/// ``b = db.query_agg(s, start, end, 60_000); b.count.sum(); b.max.max()``.
/// Reading a column copies it out of the result; keep it in a variable
/// rather than reading it in a loop.
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

    /// Number of known values per bucket (uint64).
    #[getter]
    fn count<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
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

    /// Equal to another Buckets or to a sequence of equal Bucket objects.
    fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        if let Ok(o) = other.cast::<Buckets>() {
            return Ok(self.cols.same(&o.get().cols));
        }
        if other.len().is_err() || other.is_instance_of::<pyo3::types::PyString>() {
            return Ok(false);
        }
        self.list(py)?.eq(PyList::new(py, other.try_iter()?.collect::<PyResult<Vec<_>>>()?)?)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!("Buckets({})", self.list(py)?.repr()?))
    }
}

/// Offsets as an int64 array: `offsets[i]:offsets[i + 1]` is query i.
fn offsets_array(py: Python<'_>, o: &[usize]) -> Py<PyAny> {
    o.iter().map(|&x| x as i64).collect::<Vec<_>>().into_pyarray(py).into_any().unbind()
}

/// The buckets of many aggregate queries (query_agg_many()).
///
/// result[i] is the Buckets of query i, and len(result) the number of
/// queries. For whole-batch numpy work, result.buckets holds every query's
/// buckets concatenated (columns start, count, sum, ...) and result.offsets
/// (int64, one more than the queries) where each query's begin:
/// ``b.buckets.count[b.offsets[i]:b.offsets[i + 1]]`` are query i's counts.
#[pyclass(frozen, sequence, module = "tsdb_client")]
pub(crate) struct BucketsMany {
    all: Py<Buckets>,
    offsets: Vec<usize>,
    offsets_py: Py<PyAny>,
}

impl BucketsMany {
    /// The result of a batch call; `cols` was built with the GIL released.
    pub(crate) fn new(py: Python<'_>, (cols, offsets): (Cols, Vec<usize>)) -> PyResult<BucketsMany> {
        Ok(BucketsMany { all: Py::new(py, Buckets { cols })?, offsets_py: offsets_array(py, &offsets), offsets })
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
    numpy(py)?;
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

    /// The Buckets of query i.
    fn __getitem__(&self, i: isize) -> PyResult<Buckets> {
        let n = self.__len__() as isize;
        let j = if i < 0 { i + n } else { i };
        if j < 0 || j >= n {
            return Err(PyIndexError::new_err("query index out of range"));
        }
        let (a, b) = (self.offsets[j as usize], self.offsets[j as usize + 1]);
        Ok(Buckets { cols: self.all.get().cols.pick(a..b) })
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        format!("BucketsMany({} queries, {} buckets)", self.__len__(), self.all.bind(py).get().cols.len())
    }
}
