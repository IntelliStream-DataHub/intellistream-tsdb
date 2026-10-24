//! Wire format of `include/tsdb_protocol.h`: little-endian frames
//! `[u32 len][u32 id][u8 op] payload`, where `len` counts the bytes after
//! itself. Every reply has op REPLY and starts with an i32 status.
//!
//! Version 2 adds value types and nulls (SERIES_CREATE, SERIES_TYPE,
//! INSERT_VALUES, QUERY_VALUES, STATE, STATE_NAME, QUERY_BUCKETS). This client
//! sends QUERY_BUCKETS for every aggregate: its 64-byte rows carry f64 min/max
//! and the exact integer fields, which the version 1 QUERY_AGG/GROUP rows lack.
//! Version 3 adds packed, zstd-compressed INSERT_VALUES bodies (`pack.rs`).
//! Version 4: the server knows UTC only. QUERY_BUCKETS refuses a time zone
//! other than UTC; local periods arrive as their UTC bounds in QUERY_BOUNDS
//! (`calendar.rs` computes them).
//! Version 5 and 6 add LTTB and time-weighted averages, which this client does
//! not offer (it still says 7: the server answers the lower of both versions).
//! Version 7 adds DELETE_RANGE, REPLACE_RANGE, SET_RETENTION, GET_RETENTION.

use crate::error::{Error, Result};

/// The protocol version this client speaks (`TSDB_PROTOCOL_VERSION`).
pub const PROTOCOL_VERSION: u32 = 7;
/// The port tsdbd listens on by default (`TSDB_DEFAULT_PORT`).
pub const DEFAULT_PORT: u16 = 7070;

/// Largest frame either side accepts (`TSDB_MAX_FRAME`), counted like `len`.
pub(crate) const MAX_FRAME: usize = 256 << 20;
/// The series field of an INSERT whose rows carry their own series.
pub(crate) const MANY_SERIES: u32 = u32::MAX;
/// Bytes per bucket in a QUERY_BUCKETS reply (`TSDB_BUCKET_ROW`).
pub(crate) const BUCKET_ROW: usize = 64;
/// Header of a QUERY_VALUES chunk: last, type, flags, 0, n.
pub(crate) const VALUES_CHUNK_HEADER: usize = 8;
/// Flag: a validity bitmap follows the values (`TSDB_VALID`).
pub(crate) const VALID: u8 = 1;
/// Flag: the arrays are packed and zstd compressed (`TSDB_PACKED`, version 3).
pub(crate) const PACKED: u8 = 2;
/// Header of a REPLACE_RANGE request: n, series, type, flags, 0, 0, start, end
/// (`TSDB_REPLACE_HEADER`, version 7).
pub(crate) const REPLACE_HEADER: usize = 32;
/// Reply header: len, id, op, status.
pub(crate) const REPLY_HEADER: usize = 13;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Op {
    Reply = 0,
    Hello = 1,
    Series = 2,
    SeriesName = 3,
    Insert = 4,
    QueryRaw = 5,
    Commit = 7,
    Flush = 8,
    Optimize = 9,
    Stats = 10,
    SeriesCreate = 12,
    SeriesType = 13,
    InsertValues = 14,
    QueryValues = 15,
    State = 16,
    StateName = 17,
    QueryBuckets = 18,
    LastStats = 19,
    QueryBounds = 20,
    DeleteRange = 25,
    ReplaceRange = 26,
    SetRetention = 27,
    GetRetention = 28,
}

/// Builds one request frame in a reusable buffer.
#[derive(Default)]
pub(crate) struct FrameBuf {
    buf: Vec<u8>,
}

impl FrameBuf {
    pub(crate) fn begin(&mut self, id: u32, op: Op) -> &mut Self {
        self.buf.clear();
        self.buf.extend_from_slice(&[0; 4]);
        self.buf.extend_from_slice(&id.to_le_bytes());
        self.buf.push(op as u8);
        self
    }

    pub(crate) fn u8(&mut self, v: u8) -> &mut Self {
        self.buf.push(v);
        self
    }

    pub(crate) fn u16(&mut self, v: u16) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    pub(crate) fn u32(&mut self, v: u32) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    pub(crate) fn i64(&mut self, v: i64) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    pub(crate) fn u64(&mut self, v: u64) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    pub(crate) fn bytes(&mut self, v: &[u8]) -> &mut Self {
        self.buf.extend_from_slice(v);
        self
    }

    pub(crate) fn u32s(&mut self, v: &[u32]) -> &mut Self {
        self.buf.reserve(v.len() * 4);
        for x in v {
            self.buf.extend_from_slice(&x.to_le_bytes());
        }
        self
    }

    pub(crate) fn i64s(&mut self, v: &[i64]) -> &mut Self {
        self.buf.reserve(v.len() * 8);
        for x in v {
            self.buf.extend_from_slice(&x.to_le_bytes());
        }
        self
    }

    pub(crate) fn raw(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }

    /// Pads the payload (which starts after the 9-byte header) to a multiple of 4 bytes.
    pub(crate) fn align4(&mut self) -> &mut Self {
        let pad = (4 - (self.buf.len() - 9) % 4) % 4;
        self.buf.extend_from_slice(&[0; 3][..pad]);
        self
    }

    /// Fills in the length and returns the frame.
    pub(crate) fn finish(&mut self) -> &[u8] {
        let len = (self.buf.len() - 4) as u32;
        self.buf[..4].copy_from_slice(&len.to_le_bytes());
        &self.buf
    }

    /// Releases memory after a large request, keeping a modest buffer.
    pub(crate) fn shrink(&mut self) {
        const KEEP: usize = 1 << 20;
        if self.buf.capacity() > KEEP {
            self.buf = Vec::with_capacity(KEEP);
        }
    }
}

/// Reads fixed-width little-endian fields from a reply body.
pub(crate) struct Cursor<'a> {
    b: &'a [u8],
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(b: &'a [u8]) -> Self {
        Cursor { b }
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.b.len() < n {
            return Err(Error::Protocol(format!("reply too short: wanted {n} more bytes, got {}", self.b.len())));
        }
        let (h, t) = self.b.split_at(n);
        self.b = t;
        Ok(h)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self.take(N)?.try_into().expect("length checked"))
    }

    pub(crate) fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u16(&mut self) -> Result<u16> {
        self.array().map(u16::from_le_bytes)
    }

    pub(crate) fn u32(&mut self) -> Result<u32> {
        self.array().map(u32::from_le_bytes)
    }

    pub(crate) fn i64(&mut self) -> Result<i64> {
        self.array().map(i64::from_le_bytes)
    }

    pub(crate) fn u64(&mut self) -> Result<u64> {
        self.array().map(u64::from_le_bytes)
    }

    pub(crate) fn f64(&mut self) -> Result<f64> {
        self.array().map(f64::from_le_bytes)
    }

    pub(crate) fn remaining(&self) -> usize {
        self.b.len()
    }
}

/// Decodes a column of i64 (`b.len()` is a multiple of 8) onto `out`.
pub(crate) fn extend_i64s(out: &mut Vec<i64>, b: &[u8]) {
    out.extend(b.chunks_exact(8).map(|c| i64::from_le_bytes(c.try_into().expect("8 bytes"))));
}
