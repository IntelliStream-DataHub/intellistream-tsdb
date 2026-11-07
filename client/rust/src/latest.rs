//! The newest value of many series (`QUERY_LATEST`, protocol 14; `tsdb_latest` in the engine): the types
//! [`Client::latest`](crate::Client::latest), [`Client::latest_by_name`](crate::Client::latest_by_name) and the
//! embedded [`Db::latest`](crate::Db::latest) share.

use crate::error::Status;
use crate::value::ValueType;

/// What a series' answer holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatestKind {
    /// The newest non-null row (the default answer).
    Value,
    /// The newest row is a null (only with [`LatestOpts::with_nulls`]).
    Null,
    /// The series has no such row, or `status` is set.
    None,
}

/// A value of the series' type, as the answer carries it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LatestValue {
    /// A float32 value.
    Float32(f32),
    /// A float64 value.
    Float64(f64),
    /// An int64 value.
    Int64(i64),
    /// A bool value.
    Bool(bool),
    /// A state code (see [`Client::state_name`](crate::Client::state_name)).
    State(u32),
}

/// The answer for one series, in the order of the ids asked.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Latest {
    /// `None`: answered. `Some(Status::NotFound)`: no such series (or name); `Some(Status::Denied)`: the user
    /// holds no `read` right on it, whether or not it exists (tsdbd only). `kind` is `None` then.
    pub status: Option<Status>,
    /// Value, null or none.
    pub kind: LatestKind,
    /// The series' type; `None` whenever `status` is set.
    pub ty: Option<ValueType>,
    /// The row's timestamp with `Value` and `Null`; 0 otherwise.
    pub ts: i64,
    /// The row's value with `Value`.
    pub value: Option<LatestValue>,
}

impl Latest {
    /// `(ts, value)` of a non-null newest row.
    pub fn point(&self) -> Option<(i64, LatestValue)> {
        self.value.map(|v| (self.ts, v))
    }

    pub(crate) fn from_wire(status: i32, kind: u8, ty: u8, ts: i64, bits: u64) -> Option<Latest> {
        let st = if status == 0 { None } else { Some(Status::from_code(status)) };
        let kind = match kind {
            0 => LatestKind::Value,
            1 => LatestKind::Null,
            2 => LatestKind::None,
            _ => return None,
        };
        let ty = match ty {
            _ if st.is_some() => None,
            0 => Some(ValueType::Float32),
            1 => Some(ValueType::Float64),
            2 => Some(ValueType::Int64),
            3 => Some(ValueType::Bool),
            4 => Some(ValueType::State),
            _ => return None,
        };
        let value = if kind == LatestKind::Value {
            Some(match ty? {
                ValueType::Float32 => LatestValue::Float32(f32::from_bits(bits as u32)),
                ValueType::Float64 => LatestValue::Float64(f64::from_bits(bits)),
                ValueType::Int64 => LatestValue::Int64(bits as i64),
                ValueType::Bool => LatestValue::Bool(bits != 0),
                ValueType::State => LatestValue::State(bits as u32),
            })
        } else {
            None
        };
        Some(Latest { status: st, kind, ty, ts, value })
    }

    /// An id or a name that does not resolve.
    pub(crate) fn missing(status: Status) -> Latest {
        Latest { status: Some(status), kind: LatestKind::None, ty: None, ts: 0, value: None }
    }
}

/// Options of a latest call. The default is the newest non-null value, with no bound.
///
/// ```
/// use tsdb_client::LatestOpts;
/// let o = LatestOpts::default().with_nulls().end(1_700_000_000_000);
/// assert!(o.with_nulls && o.end == 1_700_000_000_000);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatestOpts {
    /// Only rows before it (exclusive); `i64::MAX`: no bound.
    pub end: i64,
    /// A bound per series instead of `end` (the same length as the ids).
    pub ends: Option<Vec<i64>>,
    /// The newest row, a null included (kind [`LatestKind::Null`]); the slow path.
    pub with_nulls: bool,
    /// Threads the server or the engine may use for one chunk; 0: its default (`--request-threads`).
    pub threads: u16,
}

impl Default for LatestOpts {
    fn default() -> Self {
        LatestOpts { end: i64::MAX, ends: None, with_nulls: false, threads: 0 }
    }
}

impl LatestOpts {
    /// Only rows before `end`.
    pub fn end(mut self, end: i64) -> Self {
        self.end = end;
        self
    }

    /// One bound per series; replaces `end`.
    pub fn ends(mut self, ends: Vec<i64>) -> Self {
        self.ends = Some(ends);
        self
    }

    /// The newest row of each series, a null included.
    pub fn with_nulls(mut self) -> Self {
        self.with_nulls = true;
        self
    }

    /// Threads for one chunk.
    pub fn threads(mut self, threads: u16) -> Self {
        self.threads = threads;
        self
    }
}
