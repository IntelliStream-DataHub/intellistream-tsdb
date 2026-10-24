//! Value types and typed columns: everything that knows how values look on the wire.
//!
//! A series has one [`ValueType`], chosen when it is created. Columns are
//! typed vectors ([`Values`]) plus an optional validity [`Bitmap`]: bit i set
//! when value i is known, as on the wire, so a column with nulls is sent and
//! received without converting the nulls. [`Column::from_options`] and
//! [`Column::iter`] give the `Option<T>` view.
//!
//! The protocol reserves one value per type as the representation of null
//! (`i64::MIN`, state code 0, and one NaN payload per float type,
//! [`NULL_F32_BITS`] and [`NULL_F64_BITS`]): written as known values they
//! read back as null.
//!
//! The version 1 calls carry `f32` ([`Value`]) only, without nulls; they
//! remain the fast path for float32 series.

use std::fmt;
use std::str::FromStr;

use crate::error::Error;

/// A float32 sample, the value of the version 1 calls ([`insert_series`](crate::Client::insert_series),
/// [`query_points`](crate::Client::query_points)).
pub type Value = f32;

/// The bits of the f32 NaN that stands for null (`TSDB_NULL_F32_BITS`).
pub const NULL_F32_BITS: u32 = 0x7fc0_1001;
/// The bits of the f64 NaN that stands for null (`TSDB_NULL_F64_BITS`).
pub const NULL_F64_BITS: u64 = 0x7ff8_0000_0000_1001;

/// The value type of a series (`tsdb_type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
#[non_exhaustive]
pub enum ValueType {
    /// 32-bit floats, the default.
    #[default]
    Float32 = 0,
    /// 64-bit floats.
    Float64 = 1,
    /// 64-bit integers; aggregates are exact in [`Bucket::isum`](crate::Bucket::isum) and friends.
    Int64 = 2,
    /// Booleans; aggregates count `true` as 1.
    Bool = 3,
    /// Codes of a per-volume dictionary of names ([`Client::state`](crate::Client::state)).
    State = 4,
}

impl ValueType {
    /// All types, in code order.
    pub const ALL: [ValueType; 5] =
        [ValueType::Float32, ValueType::Float64, ValueType::Int64, ValueType::Bool, ValueType::State];

    /// The type with protocol code `code`.
    pub fn from_code(code: u8) -> Option<ValueType> {
        ValueType::ALL.get(code as usize).copied()
    }

    /// The protocol code.
    pub fn code(self) -> u8 {
        self as u8
    }

    /// The lower-case name: `float32`, `float64`, `int64`, `bool` or `state`.
    pub fn name(self) -> &'static str {
        match self {
            ValueType::Float32 => "float32",
            ValueType::Float64 => "float64",
            ValueType::Int64 => "int64",
            ValueType::Bool => "bool",
            ValueType::State => "state",
        }
    }

    /// Bytes per value on the wire (`tsdb_wire_width`).
    pub(crate) fn wire_width(self) -> usize {
        match self {
            ValueType::Bool => 1,
            ValueType::Float64 | ValueType::Int64 => 8,
            ValueType::Float32 | ValueType::State => 4,
        }
    }
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for ValueType {
    type Err = Error;

    /// Parses a name as given by [`ValueType::name`].
    fn from_str(s: &str) -> Result<ValueType, Error> {
        ValueType::ALL
            .into_iter()
            .find(|t| t.name() == s)
            .ok_or_else(|| Error::InvalidArgument(format!("unknown value type {s:?}")))
    }
}

mod sealed {
    pub trait Sealed {}
}

/// A Rust type that is the in-memory form of one [`ValueType`]: `f32`,
/// `f64`, `i64`, `bool`, or `u32` for state codes.
pub trait Native: Copy + Default + fmt::Debug + PartialEq + sealed::Sealed + 'static {
    /// The value type this Rust type stands for.
    const TYPE: ValueType;
    #[doc(hidden)]
    fn values_ref(v: &[Self]) -> ValuesRef<'_>;
    #[doc(hidden)]
    fn into_values(v: Vec<Self>) -> Values;
    #[doc(hidden)]
    fn slice(v: &Values) -> Option<&[Self]>;
    #[doc(hidden)]
    fn put(v: &[Self], out: &mut Vec<u8>);
    #[doc(hidden)]
    fn extend(out: &mut Vec<Self>, b: &[u8]);
}

macro_rules! native {
    ($t:ty, $variant:ident, $width:expr, |$x:ident| $enc:expr, |$c:ident| $dec:expr) => {
        impl sealed::Sealed for $t {}
        impl Native for $t {
            const TYPE: ValueType = ValueType::$variant;
            fn values_ref(v: &[Self]) -> ValuesRef<'_> {
                ValuesRef::$variant(v)
            }
            fn into_values(v: Vec<Self>) -> Values {
                Values::$variant(v)
            }
            fn slice(v: &Values) -> Option<&[Self]> {
                match v {
                    Values::$variant(v) => Some(v),
                    _ => None,
                }
            }
            fn put(v: &[Self], out: &mut Vec<u8>) {
                out.reserve(v.len() * $width);
                for &$x in v {
                    out.extend_from_slice(&$enc);
                }
            }
            fn extend(out: &mut Vec<Self>, b: &[u8]) {
                out.extend(b.chunks_exact($width).map(|$c| $dec));
            }
        }
    };
}

native!(f32, Float32, 4, |x| x.to_le_bytes(), |c| f32::from_le_bytes(c.try_into().unwrap()));
native!(f64, Float64, 8, |x| x.to_le_bytes(), |c| f64::from_le_bytes(c.try_into().unwrap()));
native!(i64, Int64, 8, |x| x.to_le_bytes(), |c| i64::from_le_bytes(c.try_into().unwrap()));
native!(bool, Bool, 1, |x| [x as u8], |c| c[0] != 0);
native!(u32, State, 4, |x| x.to_le_bytes(), |c| u32::from_le_bytes(c.try_into().unwrap()));

/// Dispatches over the variants of [`Values`] or [`ValuesRef`] with `v` bound to the vector or slice.
macro_rules! each {
    ($e:expr, $ty:ident, $v:ident => $body:expr) => {
        match $e {
            $ty::Float32($v) => $body,
            $ty::Float64($v) => $body,
            $ty::Int64($v) => $body,
            $ty::Bool($v) => $body,
            $ty::State($v) => $body,
        }
    };
}

/// Borrowed typed values, for inserts.
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum ValuesRef<'a> {
    /// float32 values.
    Float32(&'a [f32]),
    /// float64 values.
    Float64(&'a [f64]),
    /// int64 values.
    Int64(&'a [i64]),
    /// bool values.
    Bool(&'a [bool]),
    /// State codes.
    State(&'a [u32]),
}

impl ValuesRef<'_> {
    /// The value type.
    pub fn value_type(&self) -> ValueType {
        match self {
            ValuesRef::Float32(_) => ValueType::Float32,
            ValuesRef::Float64(_) => ValueType::Float64,
            ValuesRef::Int64(_) => ValueType::Int64,
            ValuesRef::Bool(_) => ValueType::Bool,
            ValuesRef::State(_) => ValueType::State,
        }
    }

    /// Number of values.
    pub fn len(&self) -> usize {
        each!(self, ValuesRef, v => v.len())
    }

    /// Whether there are no values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Appends values `start..end` in wire form.
    pub(crate) fn put(&self, out: &mut Vec<u8>, start: usize, end: usize) {
        each!(self, ValuesRef, v => Native::put(&v[start..end], out))
    }
}

/// Owned typed values.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Values {
    /// float32 values.
    Float32(Vec<f32>),
    /// float64 values.
    Float64(Vec<f64>),
    /// int64 values.
    Int64(Vec<i64>),
    /// bool values.
    Bool(Vec<bool>),
    /// State codes.
    State(Vec<u32>),
}

impl Values {
    /// No values of type `ty`.
    pub fn new(ty: ValueType) -> Values {
        match ty {
            ValueType::Float32 => Values::Float32(Vec::new()),
            ValueType::Float64 => Values::Float64(Vec::new()),
            ValueType::Int64 => Values::Int64(Vec::new()),
            ValueType::Bool => Values::Bool(Vec::new()),
            ValueType::State => Values::State(Vec::new()),
        }
    }

    /// The value type.
    pub fn value_type(&self) -> ValueType {
        self.as_ref().value_type()
    }

    /// Number of values.
    pub fn len(&self) -> usize {
        each!(self, Values, v => v.len())
    }

    /// Whether there are no values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Borrows the values.
    pub fn as_ref(&self) -> ValuesRef<'_> {
        match self {
            Values::Float32(v) => ValuesRef::Float32(v),
            Values::Float64(v) => ValuesRef::Float64(v),
            Values::Int64(v) => ValuesRef::Int64(v),
            Values::Bool(v) => ValuesRef::Bool(v),
            Values::State(v) => ValuesRef::State(v),
        }
    }

    pub(crate) fn clear(&mut self) {
        each!(self, Values, v => v.clear())
    }

    /// Appends wire-form values (a whole number of [`ValueType::wire_width`] bytes).
    pub(crate) fn extend_wire(&mut self, b: &[u8]) {
        each!(self, Values, v => Native::extend(v, b))
    }
}

/// A validity bitmap: bit i (least significant bit first) set when value i is known.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Bitmap {
    bytes: Vec<u8>,
    len: usize,
}

impl Bitmap {
    /// `len` bits, all set (every value known).
    pub fn all_valid(len: usize) -> Bitmap {
        let mut b = Bitmap::default();
        b.extend_valid(len);
        b
    }

    /// Wraps wire-form bytes; `bytes` holds at least `(len + 7) / 8` bytes.
    pub fn from_bytes(mut bytes: Vec<u8>, len: usize) -> Option<Bitmap> {
        if bytes.len() < len.div_ceil(8) {
            return None;
        }
        bytes.truncate(len.div_ceil(8));
        let mut b = Bitmap { bytes, len };
        b.clear_tail();
        Some(b)
    }

    /// Number of bits.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether there are no bits.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Whether value `i` is known.
    ///
    /// # Panics
    ///
    /// If `i` is out of range.
    pub fn get(&self, i: usize) -> bool {
        assert!(i < self.len, "bit {i} of {}", self.len);
        self.bytes[i >> 3] >> (i & 7) & 1 != 0
    }

    /// Marks value `i` known or null.
    ///
    /// # Panics
    ///
    /// If `i` is out of range.
    pub fn set(&mut self, i: usize, known: bool) {
        assert!(i < self.len, "bit {i} of {}", self.len);
        if known {
            self.bytes[i >> 3] |= 1 << (i & 7);
        } else {
            self.bytes[i >> 3] &= !(1 << (i & 7));
        }
    }

    /// Appends one bit.
    pub fn push(&mut self, known: bool) {
        if self.len & 7 == 0 {
            self.bytes.push(0);
        }
        self.len += 1;
        self.set(self.len - 1, known);
    }

    /// Number of nulls (clear bits).
    pub fn null_count(&self) -> usize {
        self.len - self.bytes.iter().map(|b| b.count_ones() as usize).sum::<usize>()
    }

    /// The wire form: `(len + 7) / 8` bytes, unused high bits clear.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Iterates over the bits.
    pub fn iter(&self) -> impl Iterator<Item = bool> + '_ {
        (0..self.len).map(|i| self.get(i))
    }

    /// Appends `n` set bits.
    pub fn extend_valid(&mut self, n: usize) {
        self.extend_bits(&[], n, true);
    }

    /// Appends the first `n` bits of wire-form `src`.
    pub(crate) fn extend_from_wire(&mut self, src: &[u8], n: usize) {
        self.extend_bits(src, n, false);
    }

    fn extend_bits(&mut self, src: &[u8], n: usize, ones: bool) {
        if self.len & 7 == 0 {
            let nb = n.div_ceil(8);
            if ones {
                self.bytes.resize(self.bytes.len() + nb, 0xff);
            } else {
                self.bytes.extend_from_slice(&src[..nb]);
            }
            self.len += n;
            self.clear_tail();
        } else {
            for i in 0..n {
                self.push(ones || src[i >> 3] >> (i & 7) & 1 != 0);
            }
        }
    }

    fn clear_tail(&mut self) {
        if self.len & 7 != 0 {
            let last = self.bytes.len() - 1;
            self.bytes[last] &= (1u8 << (self.len & 7)) - 1;
        }
    }
}

impl FromIterator<bool> for Bitmap {
    fn from_iter<I: IntoIterator<Item = bool>>(iter: I) -> Bitmap {
        let mut b = Bitmap::default();
        for x in iter {
            b.push(x);
        }
        b
    }
}

/// Owned typed values with nulls: what a query returns.
///
/// ```
/// use tsdb_client::Column;
/// let c = Column::from_options([Some(1i64), None, Some(3)]);
/// assert_eq!(c.null_count(), 1);
/// assert_eq!(c.to_options::<i64>(), Some(vec![Some(1), None, Some(3)]));
/// ```
#[derive(Debug, Clone)]
pub struct Column {
    /// The values; a null's value is unspecified.
    pub values: Values,
    /// `None` when every value is known; otherwise as long as `values`.
    pub valid: Option<Bitmap>,
}

impl Column {
    /// A column without nulls.
    pub fn new<T: Native>(values: Vec<T>) -> Column {
        Column { values: T::into_values(values), valid: None }
    }

    /// No values of type `ty`.
    pub fn empty(ty: ValueType) -> Column {
        Column { values: Values::new(ty), valid: None }
    }

    /// A column from optional values; `None` is null.
    pub fn from_options<T: Native>(values: impl IntoIterator<Item = Option<T>>) -> Column {
        let (mut v, mut valid) = (Vec::new(), Bitmap::default());
        for x in values {
            v.push(x.unwrap_or_default());
            valid.push(x.is_some());
        }
        let valid = (valid.null_count() > 0).then_some(valid);
        Column { values: T::into_values(v), valid }
    }

    /// The value type.
    pub fn value_type(&self) -> ValueType {
        self.values.value_type()
    }

    /// Number of values, nulls included.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether there are no values.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Whether value `i` is null.
    ///
    /// # Panics
    ///
    /// If the column has nulls and `i` is out of range.
    pub fn is_null(&self, i: usize) -> bool {
        self.valid.as_ref().is_some_and(|b| !b.get(i))
    }

    /// Number of nulls.
    pub fn null_count(&self) -> usize {
        self.valid.as_ref().map_or(0, Bitmap::null_count)
    }

    /// The values as a slice of `T` (nulls hold unspecified values), or
    /// `None` if the column has another type.
    pub fn as_slice<T: Native>(&self) -> Option<&[T]> {
        T::slice(&self.values)
    }

    /// Iterates over the values as `Option<T>`, or `None` if the column has another type.
    pub fn iter<T: Native>(&self) -> Option<impl Iterator<Item = Option<T>> + '_> {
        let v = self.as_slice::<T>()?;
        Some(v.iter().enumerate().map(move |(i, &x)| (!self.is_null(i)).then_some(x)))
    }

    /// The values as `Option<T>`, or `None` if the column has another type.
    pub fn to_options<T: Native>(&self) -> Option<Vec<Option<T>>> {
        Some(self.iter::<T>()?.collect())
    }

    /// Borrows the column for an insert.
    pub fn as_column_ref(&self) -> ColumnRef<'_> {
        ColumnRef { values: self.values.as_ref(), valid: self.valid.as_ref() }
    }

    pub(crate) fn clear(&mut self) {
        self.values.clear();
        self.valid = None;
    }
}

/// Borrowed typed values with optional nulls, for inserts.
///
/// Slices, arrays and `Vec`s of a [`Native`] type, and `&Column`, convert into it:
///
/// ```
/// use tsdb_client::{Bitmap, ColumnRef, ValueType};
/// let plain: ColumnRef = (&[1.5f64, 2.5][..]).into();
/// let valid: Bitmap = [true, false].into_iter().collect();
/// let with_null = ColumnRef::from(&[7i64, 0]).with_validity(&valid);
/// assert_eq!((plain.value_type(), with_null.value_type()), (ValueType::Float64, ValueType::Int64));
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ColumnRef<'a> {
    /// The values; a null's value is ignored.
    pub values: ValuesRef<'a>,
    /// `None` when every value is known; otherwise as long as `values`.
    pub valid: Option<&'a Bitmap>,
}

impl<'a> ColumnRef<'a> {
    /// Values without nulls.
    pub fn new(values: ValuesRef<'a>) -> Self {
        ColumnRef { values, valid: None }
    }

    /// Sets the validity bitmap.
    pub fn with_validity(mut self, valid: &'a Bitmap) -> Self {
        self.valid = Some(valid);
        self
    }

    /// The value type.
    pub fn value_type(&self) -> ValueType {
        self.values.value_type()
    }

    /// Number of values.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether there are no values.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl<'a, T: Native> From<&'a [T]> for ColumnRef<'a> {
    fn from(v: &'a [T]) -> Self {
        ColumnRef::new(T::values_ref(v))
    }
}

impl<'a, T: Native, const N: usize> From<&'a [T; N]> for ColumnRef<'a> {
    fn from(v: &'a [T; N]) -> Self {
        ColumnRef::new(T::values_ref(v))
    }
}

impl<'a, T: Native> From<&'a Vec<T>> for ColumnRef<'a> {
    fn from(v: &'a Vec<T>) -> Self {
        ColumnRef::new(T::values_ref(v))
    }
}

impl<'a> From<&'a Column> for ColumnRef<'a> {
    fn from(c: &'a Column) -> Self {
        c.as_column_ref()
    }
}

impl<'a> From<ValuesRef<'a>> for ColumnRef<'a> {
    fn from(v: ValuesRef<'a>) -> Self {
        ColumnRef::new(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bitmaps() {
        let mut b: Bitmap = [true, false, true].into_iter().collect();
        assert_eq!((b.len(), b.null_count(), b.as_bytes()), (3, 1, &[0b101][..]));
        b.extend_valid(6);
        assert_eq!((b.len(), b.null_count(), b.as_bytes()), (9, 1, &[0b1111_1101, 0b1][..]));
        b.extend_from_wire(&[0b10], 2); // unaligned append
        assert_eq!(b.iter().skip(9).collect::<Vec<_>>(), vec![false, true]);
        let mut a = Bitmap::all_valid(8);
        a.extend_from_wire(&[0xff, 0b1110], 12); // aligned append clears unused bits
        assert_eq!((a.len(), a.null_count(), a.as_bytes()), (20, 1, &[0xff, 0xff, 0b1110][..]));
        assert_eq!(Bitmap::from_bytes(vec![0xff], 3).unwrap().as_bytes(), &[0b111]);
        assert!(Bitmap::from_bytes(vec![0xff], 9).is_none());
    }

    #[test]
    fn columns_and_wire() {
        let c = Column::from_options([Some(true), None, Some(false)]);
        assert_eq!(c.value_type(), ValueType::Bool);
        assert!(c.is_null(1) && !c.is_null(0));
        assert!(c.to_options::<i64>().is_none());
        assert!(Column::from_options([Some(1.0f32)]).valid.is_none());
        let mut out = Vec::new();
        ColumnRef::from(&[1i64, -2]).values.put(&mut out, 1, 2);
        assert_eq!(out, (-2i64).to_le_bytes());
        let mut v = Values::new(ValueType::Bool);
        v.extend_wire(&[1, 0, 2]);
        assert_eq!(v, Values::Bool(vec![true, false, true]));
        assert_eq!("int64".parse::<ValueType>().unwrap(), ValueType::Int64);
        assert!("int".parse::<ValueType>().is_err());
        assert_eq!(ValueType::from_code(4), Some(ValueType::State));
        assert_eq!(ValueType::from_code(5), None);
    }
}
