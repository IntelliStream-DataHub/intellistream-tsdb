// SPDX-License-Identifier: Apache-2.0
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

/// Tile width for L1D-resident byte-plane transpose: 1024 values * 8 planes = 8 KiB, with a 64-byte pad per row so
/// the 8 plane buffers never alias in an 8-way 32 KiB L1D cache.
const TILE: usize = 1024;
const TILE_STRIDE: usize = TILE + 64;

fn transpose4<T: Copy>(src: &[T], out: &mut [u8], to_le: impl Fn(T) -> [u8; 4]) {
    let n = src.len();
    let mut tile = [[0u8; TILE_STRIDE]; 4];
    let mut i0 = 0;
    while i0 < n {
        let m = (n - i0).min(TILE);
        let chunk = &src[i0..i0 + m];
        let [t0, t1, t2, t3] = &mut tile;
        for (j, &x) in chunk.iter().enumerate() {
            let b = to_le(x);
            t0[j] = b[0];
            t1[j] = b[1];
            t2[j] = b[2];
            t3[j] = b[3];
        }
        for (plane, row) in tile.iter().enumerate() {
            out[plane * n + i0..plane * n + i0 + m].copy_from_slice(&row[..m]);
        }
        i0 += m;
    }
}

fn transpose8<T: Copy>(src: &[T], out: &mut [u8], to_le: impl Fn(T) -> [u8; 8]) {
    let n = src.len();
    let mut tile = [[0u8; TILE_STRIDE]; 8];
    let mut i0 = 0;
    while i0 < n {
        let m = (n - i0).min(TILE);
        let chunk = &src[i0..i0 + m];
        let [t0, t1, t2, t3, t4, t5, t6, t7] = &mut tile;
        for (j, &x) in chunk.iter().enumerate() {
            let b = to_le(x);
            t0[j] = b[0];
            t1[j] = b[1];
            t2[j] = b[2];
            t3[j] = b[3];
            t4[j] = b[4];
            t5[j] = b[5];
            t6[j] = b[6];
            t7[j] = b[7];
        }
        for (plane, row) in tile.iter().enumerate() {
            out[plane * n + i0..plane * n + i0 + m].copy_from_slice(&row[..m]);
        }
        i0 += m;
    }
}

/// Appends the reshaped body of rows `start..end` to `out` (`tsdb_pack_reshape`).
/// `valid` is the whole bitmap and `start` a multiple of 8, so the slice starts on a byte.
#[allow(clippy::too_many_arguments)]
pub(crate) fn reshape(
    ts: &[i64],
    values: ValuesRef<'_>,
    valid: Option<&[u8]>,
    series: Option<&[u64]>,
    start: usize,
    end: usize,
    out: &mut Vec<u8>,
) {
    let n = end - start;
    let mut prev = 0i64;
    for &t in &ts[start..end] {
        put_varint(out, zigzag(t.wrapping_sub(prev)));
        prev = t;
    }
    if n > 0 {
        let w = values.value_type().wire_width();
        let base = out.len();
        out.resize(base + n * w, 0);
        let dst = &mut out[base..base + n * w];
        match values {
            ValuesRef::Bool(v) => {
                for (d, &x) in dst.iter_mut().zip(&v[start..end]) {
                    *d = x as u8;
                }
            }
            ValuesRef::Float32(v) => transpose4(&v[start..end], dst, f32::to_le_bytes),
            ValuesRef::State(v) => transpose4(&v[start..end], dst, u32::to_le_bytes),
            ValuesRef::Float64(v) => transpose8(&v[start..end], dst, f64::to_le_bytes),
            ValuesRef::Int64(v) => transpose8(&v[start..end], dst, i64::to_le_bytes),
        }
    }
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
        reshape(&ts, ValuesRef::Int64(&v), Some(&[0b01]), Some(&[7, 5]), 0, 2, &mut out);
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
        reshape(&ts, ValuesRef::Bool(&v), None, Some(ids), 0, n, &mut out);
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
                reshape(&ts, values, vb, many.then_some(&s[..]), start, end, &mut out);
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

    fn ref_reshape(
        ts: &[i64],
        values: ValuesRef<'_>,
        valid: Option<&[u8]>,
        series: Option<&[u64]>,
        start: usize,
        end: usize,
    ) -> Vec<u8> {
        let mut out = Vec::new();
        let n = end - start;
        let mut prev = 0i64;
        for &t in &ts[start..end] {
            put_varint(&mut out, zigzag(t.wrapping_sub(prev)));
            prev = t;
        }
        let w = values.value_type().wire_width();
        let mut plain = Vec::new();
        values.put(&mut plain, start, end);
        for b in 0..w {
            out.extend(plain.iter().skip(b).step_by(w));
        }
        if let Some(v) = valid {
            out.extend_from_slice(&v[start / 8..start / 8 + n.div_ceil(8)]);
        }
        if let Some(s) = series {
            let mut prev = 0u64;
            for &x in &s[start..end] {
                put_varint(&mut out, zigzag(x.wrapping_sub(prev) as i64));
                prev = x;
            }
        }
        out
    }

    #[test]
    fn bit_exact_across_tile_boundaries() {
        let max_n = 3008usize;
        let mut r = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = || {
            r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            r
        };
        let ts: Vec<i64> = (0..max_n).map(|i| if i % 17 == 0 { next() as i64 } else { 1_700_000_000_000 + i as i64 * 1000 }).collect();
        let f32s: Vec<f32> = (0..max_n).map(|i| if i % 13 == 0 { f32::from_bits(0x7fc0_1001 ^ (i as u32 & 0xff)) } else { f32::from_bits(next() as u32) }).collect();
        let f64s: Vec<f64> = (0..max_n).map(|i| if i % 13 == 0 { f64::from_bits(0x7ff8_0000_0000_1001 ^ (i as u64 & 0xff)) } else { f64::from_bits(next()) }).collect();
        let i64s: Vec<i64> = (0..max_n).map(|_| next() as i64).collect();
        let bools: Vec<bool> = (0..max_n).map(|_| (next() & 1) != 0).collect();
        let states: Vec<u32> = (0..max_n).map(|_| next() as u32).collect();
        let sids: Vec<u64> = (0..max_n).map(|_| next()).collect();
        let bits: Vec<u8> = (0..max_n.div_ceil(8)).map(|_| next() as u8).collect();
        let all = [
            ValuesRef::Bool(&bools),
            ValuesRef::Float32(&f32s),
            ValuesRef::State(&states),
            ValuesRef::Float64(&f64s),
            ValuesRef::Int64(&i64s),
        ];
        let sizes = [0, 1, 2, 7, 8, 15, 16, 17, 63, 64, 65, 1023, 1024, 1025, 2047, 2048, 2049, 3000];
        for (si, &n) in sizes.iter().enumerate() {
            let start = if si % 2 == 0 { 0 } else { 8 };
            let end = start + n;
            let valid = (si % 3 == 0).then_some(&bits[..]);
            let many = (si % 2 == 1).then_some(&sids[..]);
            for values in all {
                let mut got = Vec::new();
                reshape(&ts, values, valid, many, start, end, &mut got);
                let want = ref_reshape(&ts, values, valid, many, start, end);
                assert_eq!(got, want, "type={:?} n={n} start={start}", values.value_type());
                assert!(got.len() <= raw_bound(n, values.value_type(), valid.is_some(), many.is_some()));
            }
        }
    }
}
