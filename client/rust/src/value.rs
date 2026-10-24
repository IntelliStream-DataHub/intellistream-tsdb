//! The sample value type, kept in one place.
//!
//! Protocol version 1 carries every value as a little-endian `f32`: in INSERT
//! and QUERY_RAW columns and as the min/max of an aggregate bucket. Everything
//! else in the crate goes through [`Value`] and the helpers here, so a future
//! value type (`f64`, `i64`, `bool`, a dictionary-encoded state, nulls) changes
//! this module, not the request and reply code.

/// One sample value as the server stores it.
pub type Value = f32;

/// Bytes per value on the wire.
pub(crate) const VALUE_SIZE: usize = 4;

/// Appends `values` to a request payload.
pub(crate) fn put_values(out: &mut Vec<u8>, values: &[Value]) {
    out.reserve(values.len() * VALUE_SIZE);
    for v in values {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

/// Decodes one value; `b` holds exactly [`VALUE_SIZE`] bytes.
pub(crate) fn get_value(b: &[u8]) -> Value {
    Value::from_le_bytes(b.try_into().expect("value width"))
}

/// Decodes a column of values (`b.len()` is a multiple of [`VALUE_SIZE`]) onto `out`.
pub(crate) fn extend_values(out: &mut Vec<Value>, b: &[u8]) {
    out.extend(b.chunks_exact(VALUE_SIZE).map(get_value));
}
