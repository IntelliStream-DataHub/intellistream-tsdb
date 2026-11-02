//! The packed body of an INSERT_VALUES frame (protocol version 3), as
//! `client/tsdb_pack.c` writes it and tsdbd reads it.
//!
//! Inserts are mostly regular clocks and sensor readings, so a cheap reshape
//! makes them compress several times better than raw arrays:
//!
//! ```text
//! timestamps   zigzag deltas from the previous row (the first from 0), LEB128 varints
//! values       byte planes: byte 0 of every value, then byte 1, ... (bool: one plane)
//! valid        the bitmap, unchanged (with a validity bitmap)
//! series       zigzag deltas of the wrapping 64-bit ids (the first from 0), LEB128 varints (many-series layout)
//! ```
//!
//! The whole is one zstd frame (with its content size, which the server
//! checks), sent after the 16-byte header as `u32 raw_len, frame`.

use crate::value::{ValueType, ValuesRef};

/// The largest reshaped body of `n` rows (`tsdb_pack_raw_bound`): the server
/// refuses a larger `raw_len`.
pub(crate) fn raw_bound(n: usize, ty: ValueType, valid: bool, many: bool) -> usize {
    n * (10 + ty.wire_width()) + if valid { n.div_ceil(8) } else { 0 } + if many { n * 10 } else { 0 }
}

fn zigzag(x: i64) -> u64 {
    ((x as u64) << 1) ^ ((x >> 63) as u64)
}

fn put_varint(out: &mut Vec<u8>, mut x: u64) {
    while x >= 0x80 {
        out.push(x as u8 | 0x80);
        x >>= 7;
    }
    out.push(x as u8);
}

/// Appends the reshaped body of rows `start..end` to `out` (`tsdb_pack_reshape`).
/// `scratch` holds the plain values in between; `valid` is the whole bitmap
/// and `start` a multiple of 8, so the slice starts on a byte.
#[allow(clippy::too_many_arguments)]
pub(crate) fn reshape(
    ts: &[i64],
    values: ValuesRef<'_>,
    valid: Option<&[u8]>,
    series: Option<&[u64]>,
    start: usize,
    end: usize,
    scratch: &mut Vec<u8>,
    out: &mut Vec<u8>,
) {
    let n = end - start;
    let mut prev = 0i64;
    for &t in &ts[start..end] {
        put_varint(out, zigzag(t.wrapping_sub(prev)));
        prev = t;
    }
    let w = values.value_type().wire_width();
    scratch.clear();
    values.put(scratch, start, end);
    for b in 0..w {
        out.extend(scratch.iter().skip(b).step_by(w));
    }
    debug_assert_eq!(scratch.len(), n * w);
    if let Some(v) = valid {
        debug_assert_eq!(start % 8, 0);
        out.extend_from_slice(&v[start / 8..start / 8 + n.div_ceil(8)]);
    }
    if let Some(s) = series {
        // protocol 11: the wrapping 64-bit difference, so a jump across 2^32 or from 0 to u64::MAX - 1 stays
        // a delta of at most 10 varint bytes
        let mut prev = 0u64;
        for &x in &s[start..end] {
            put_varint(out, zigzag(x.wrapping_sub(prev) as i64));
            prev = x;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unzigzag(z: u64) -> i64 {
        (z >> 1) as i64 ^ -((z & 1) as i64)
    }

    fn get_varint(p: &mut &[u8]) -> Option<u64> {
        let mut v = 0u64;
        for shift in (0..70).step_by(7) {
            let (&b, rest) = p.split_first()?;
            *p = rest;
            v |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                return Some(v);
            }
        }
        None
    }

    /// `tsdb_pack_unshape`, for the round trips below.
    #[allow(clippy::type_complexity)]
    fn unshape(
        mut p: &[u8],
        n: usize,
        w: usize,
        valid: bool,
        many: bool,
    ) -> Option<(Vec<i64>, Vec<u8>, Option<Vec<u8>>, Option<Vec<u64>>)> {
        let mut ts = Vec::new();
        let mut prev = 0i64;
        for _ in 0..n {
            prev = prev.wrapping_add(unzigzag(get_varint(&mut p)?));
            ts.push(prev);
        }
        let planes = p.get(..n * w)?;
        p = &p[n * w..];
        let mut values = vec![0u8; n * w];
        for b in 0..w {
            for i in 0..n {
                values[i * w + b] = planes[b * n + i];
            }
        }
        let bits = if valid {
            let v = p.get(..n.div_ceil(8))?.to_vec();
            p = &p[n.div_ceil(8)..];
            Some(v)
        } else {
            None
        };
        let series = if many {
            let mut s = Vec::new();
            let mut prev = 0u64;
            for _ in 0..n {
                prev = prev.wrapping_add(unzigzag(get_varint(&mut p)?) as u64);
                s.push(prev);
            }
            Some(s)
        } else {
            None
        };
        p.is_empty().then_some((ts, values, bits, series))
    }

    #[test]
    fn the_c_layout() {
        // two rows of int64 in two series, with a bitmap: every byte written out by hand
        let ts = [1000i64, 999];
        let v = [0x0102i64, -1];
        let mut out = Vec::new();
        reshape(&ts, ValuesRef::Int64(&v), Some(&[0b01]), Some(&[7, 5]), 0, 2, &mut Vec::new(), &mut out);
        let mut want = vec![0xd0, 0x0f, 0x01]; // zigzag(1000) = 2000 as LEB128, zigzag(-1) = 1
        want.extend([0x02, 0xff, 0x01, 0xff]); // byte planes 0 and 1
        want.extend([0x00, 0xff].repeat(6)); // planes 2..7
        want.push(0b01); // the bitmap
        want.extend([0x0e, 0x03]); // series: zigzag(7) = 14, zigzag(-2) = 3
        assert_eq!(out, want);
        assert!(out.len() <= raw_bound(2, ValueType::Int64, true, true));
    }

    /// The series column alone: `reshape` of `ids` with one-byte (bool) values, minus the timestamps and values.
    fn series_column(ids: &[u64]) -> Vec<u8> {
        let n = ids.len();
        let (ts, v) = (vec![0i64; n], vec![false; n]);
        let mut out = Vec::new();
        reshape(&ts, ValuesRef::Bool(&v), None, Some(ids), 0, n, &mut Vec::new(), &mut out);
        out.split_off(n + n) // n one-byte timestamp deltas (0), n value bytes
    }

    #[test]
    fn wide_ids_are_wrapping_64_bit_deltas() {
        // across 2^32 up and down, and between 0 and u64::MAX - 1 (a wrapping difference of -2 and 2)
        let ids = [5u64, 0x1_0000_0005, 0xFFFF_FFFF_0000_0001, 5, 0, u64::MAX - 1, 0, 1 << 63, 0];
        let col = series_column(&ids);
        let mut want = Vec::new();
        for (i, &x) in ids.iter().enumerate() {
            let prev = if i == 0 { 0 } else { ids[i - 1] };
            put_varint(&mut want, zigzag(x.wrapping_sub(prev) as i64));
        }
        assert_eq!(col, want);
        // hand-checked bytes: 0 -> u64::MAX - 1 is -2, zigzag 3; back is +2, zigzag 4
        let two = series_column(&[u64::MAX - 1, 0]);
        assert_eq!(two, vec![0x03, 0x04]);
        // 0 -> 0x1_0000_0005: +2^32 + 5 = zigzag 2^33 + 10, a 5 byte varint
        assert_eq!(series_column(&[0x1_0000_0005]), vec![0x8a, 0x80, 0x80, 0x80, 0x20]);
        // the worst case, a jump of 2^63, takes the 10 bytes the bound reserves per id
        assert_eq!(series_column(&[1 << 63]).len(), 10);
        let mut round = Vec::new();
        reshape(
            &vec![0i64; ids.len()],
            ValuesRef::Bool(&vec![false; ids.len()]),
            None,
            Some(&ids),
            0,
            ids.len(),
            &mut Vec::new(),
            &mut round,
        );
        assert!(round.len() <= raw_bound(ids.len(), ValueType::Bool, false, true));
        let (_, _, _, s2) = unshape(&round, ids.len(), 1, false, true).expect("well formed");
        assert_eq!(s2.as_deref(), Some(&ids[..]));
    }

    #[test]
    fn round_trips() {
        let n: usize = 1000;
        let ts: Vec<i64> = (0..n as i64).map(|i| if i % 7 == 0 { i64::MIN + i } else { i * 1000 - 3 }).collect();
        let f: Vec<f64> = (0..n).map(|i| i as f64 * 0.37 - 5.0).collect();
        let b: Vec<bool> = (0..n).map(|i| i % 3 == 0).collect();
        let s: Vec<u64> = (0..n as u64).map(|i| if i % 2 == 0 { u32::MAX as u64 - i } else { i }).collect();
        let s: Vec<u64> = s.iter().enumerate().map(|(i, &x)| if i % 5 == 0 { x << 33 | 1 } else { x }).collect();
        let bits: Vec<u8> = (0..n.div_ceil(8)).map(|i| i as u8 ^ 0x5a).collect();
        for (values, w) in [(ValuesRef::Float64(&f), 8), (ValuesRef::Bool(&b), 1)] {
            for (valid, many) in [(false, false), (true, false), (false, true), (true, true)] {
                // a slice starting at row 8, as the second frame of a split insert would
                let (start, end) = (8, n);
                let mut out = Vec::new();
                let vb = valid.then_some(&bits[..]);
                reshape(&ts, values, vb, many.then_some(&s[..]), start, end, &mut Vec::new(), &mut out);
                assert!(out.len() <= raw_bound(end - start, values.value_type(), valid, many));
                let (t2, v2, b2, s2) = unshape(&out, end - start, w, valid, many).expect("well formed");
                assert_eq!(t2, &ts[start..end]);
                let mut plain = Vec::new();
                values.put(&mut plain, start, end);
                assert_eq!(v2, plain);
                assert_eq!(b2.as_deref(), vb.map(|v| &v[1..]));
                assert_eq!(s2.as_deref(), many.then_some(&s[start..end]));
            }
        }
    }
}
