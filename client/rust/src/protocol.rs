// SPDX-License-Identifier: Apache-2.0
//! Wire format of `include/tsdb_protocol.h`: little-endian frames
//! `[u32 len][u32 id][u8 op] payload`, where `len` counts the bytes after
//! itself. Every reply has op REPLY and starts with an i32 status.
//!
//! Version 2 adds value types and nulls (SERIES_CREATE, SERIES_TYPE,
//! INSERT_VALUES, QUERY_VALUES, STATE, STATE_NAME, QUERY_BUCKETS). This client
//! sends QUERY_BUCKETS for every aggregate: its 64-byte rows carry f64 min/max
//! and the exact integer fields. (The version 1 QUERY_AGG and QUERY_GROUP were removed in version 11.)
//! Version 3 adds packed, zstd-compressed INSERT_VALUES bodies (`pack.rs`).
//! Version 4: the server knows UTC only. QUERY_BUCKETS refuses a time zone
//! other than UTC; local periods arrive as their UTC bounds in QUERY_BOUNDS
//! (`calendar.rs` computes them).
//! Version 5 and 6 add LTTB and time-weighted averages, version 7 DELETE_RANGE,
//! REPLACE_RANGE, SET_RETENTION, GET_RETENTION.
//! Version 11: every series id is a u64 on the wire. A server that answers HELLO with less is refused
//! when the client connects ([`Error::Protocol`]).
//! Version 12: login (SCRAM-SHA-256) and access control. HELLO's reply says whether login is required and
//! which mechanisms the server offers; AUTH_START and AUTH_CONTINUE carry the exchange (`auth.rs`), and the
//! user admin ops (WHOAMI to USER_LIST) follow.
//! Version 13: the bodies of the HA statuses (`ha.rs`: UNAVAILABLE, MOVED, PARTIAL, DROPPED), TOPOLOGY and STATUS.
//! Version 14: QUERY_LATEST (`client/latest.rs`). Version 15: statistics, STATS_SERIES and STATS_TENANTS
//! (`client/stats.rs`). The floor stays 12.

use crate::error::{Error, Result};

/// The protocol version this client speaks (`TSDB_PROTOCOL_VERSION`).
pub const PROTOCOL_VERSION: u32 = 15;
/// The oldest server version this client talks to (`TSDB_PROTOCOL_MIN`): version 11 widened every series id,
/// 12 added login; tsdbd refuses a client below 12, as this client refuses such a server.
pub(crate) const PROTOCOL_MIN: u32 = 12;
/// HELLO reply flags: the server requires a login (`TSDB_HELLO_AUTH`).
pub(crate) const HELLO_AUTH: u16 = 1;
/// The one mechanism this client speaks (`TSDB_SCRAM_SHA256`).
pub(crate) const SCRAM_SHA256: &str = "SCRAM-SHA-256";
/// User flags (`TSDB_USER_SUPERUSER`).
pub(crate) const USER_SUPERUSER: u8 = 1;
/// The port tsdbd listens on by default (`TSDB_DEFAULT_PORT`).
pub const DEFAULT_PORT: u16 = 7070;

/// Largest frame either side accepts (`TSDB_MAX_FRAME`), counted like `len`.
pub(crate) const MAX_FRAME: usize = 256 << 20;
/// STATS_SERIES: ids in one request, with `resident` (`TSDB_STATS_SERIES_MAX_IDS`, `_RESIDENT_MAX`) and its flag.
pub(crate) const STATS_SERIES_MAX_IDS: usize = 65_536;
pub(crate) const STATS_SERIES_RESIDENT_MAX: usize = 1024;
pub(crate) const STATS_RESIDENT: u32 = 1;
/// STATS_TENANTS: the request flags, the reply's shape (`TSDB_TSTATS_*`).
pub(crate) const TSTATS_VOLUMES: u16 = 1;
pub(crate) const TSTATS_HOT: u16 = 2;
pub(crate) const TSTATS_CALLER: u16 = 4;
pub(crate) const TSTATS_ROW_HEAD: usize = 16;
pub(crate) const TSTATS_DATA_FIELDS: usize = 18;
pub(crate) const TSTATS_CALLER_FIELDS: usize = 20;
pub(crate) const TSTATS_HOT_REC: usize = 32;
pub(crate) const TSTATS_MAX_K: u32 = 256;
pub(crate) const TSTATS_VOLUME_SUM: u32 = 0xffff_ffff;
pub(crate) const TSTATS_DONE: u32 = 0xffff_ffff;
/// The series field of an INSERT whose rows carry their own series.
pub(crate) const MANY_SERIES: u64 = u64::MAX;

/// Bytes per bucket in a QUERY_BUCKETS reply (`TSDB_BUCKET_ROW`).
pub(crate) const BUCKET_ROW: usize = 64;
/// QUERY_BUCKETS/QUERY_BOUNDS fields: the spread follows each bucket (version 8).
pub(crate) const SPREAD: u8 = 1;
/// Bytes per bucket in a reply with [`SPREAD`].
pub(crate) const SPREAD_ROW: usize = 104;
/// Bytes of a QUERY_TWA request before the bounds.
pub(crate) const TWA_HEADER: usize = 60;
/// Bytes per bucket in a QUERY_TWA reply.
pub(crate) const TWA_ROW: usize = 40;
/// Header of a QUERY_VALUES chunk: last, type, flags, 0, n.
pub(crate) const VALUES_CHUNK_HEADER: usize = 8;
/// Flag: a validity bitmap follows the values (`TSDB_VALID`).
pub(crate) const VALID: u8 = 1;
/// Flag: the arrays are packed and zstd compressed (`TSDB_PACKED`, version 3).
pub(crate) const PACKED: u8 = 2;
/// Header of a REPLACE_RANGE request: n, type, flags, 0, series, start, end
/// (`TSDB_REPLACE_HEADER`, version 7).
pub(crate) const REPLACE_HEADER: usize = 32;
/// Reply header: len, id, op, status.
pub(crate) const REPLY_HEADER: usize = 13;
/// QUERY_MULTI bytes before the per-series arrays (`TSDB_MULTI_HEADER`).
pub(crate) const MULTI_HEADER: usize = 24;
/// Series in one QUERY_MULTI (`TSDB_MULTI_MAX_SERIES`); longer lists are split.
pub(crate) const MULTI_MAX_SERIES: usize = 65536;
/// A QUERY_MULTI reply frame has the series' u32 index after its status (`TSDB_MULTI_PREFIX`).
pub(crate) const MULTI_PREFIX: usize = 4;
/// The index of the END frame (`TSDB_MULTI_END`).
pub(crate) const MULTI_END: u32 = u32::MAX;
/// QUERY_MULTI flags (`TSDB_MULTI_PER_START` ...).
pub(crate) const MULTI_PER_START: u8 = 1;
/// QUERY_LATEST: the ids in one request, records in one reply frame, bytes of a record (`TSDB_LATEST_*`).
pub(crate) const LATEST_MAX_IDS: usize = 1 << 20;
pub(crate) const LATEST_FRAME_IDS: usize = 65536;
pub(crate) const LATEST_REC: usize = 24;
pub(crate) const LATEST_WITH_NULLS: u8 = 1;
pub(crate) const LATEST_PER_END: u8 = 2;
pub(crate) const MULTI_PER_END: u8 = 2;
pub(crate) const MULTI_PER_INTERVAL: u8 = 4;
pub(crate) const MULTI_TRIM_LO: u8 = 8;
pub(crate) const MULTI_TRIM_HI: u8 = 16;

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
    QueryLttb = 21,
    QueryTwa = 22,
    QueryMulti = 23,
    Cancel = 24,
    DeleteRange = 25,
    ReplaceRange = 26,
    SetRetention = 27,
    GetRetention = 28,
    AuthStart = 29,
    AuthContinue = 30,
    Whoami = 31,
    UserCreate = 32,
    UserDrop = 33,
    UserAlter = 34,
    UserPassword = 35,
    Grant = 36,
    Revoke = 37,
    UserList = 38,
    QueryLatest = 39,
    StatsSeries = 46,
    StatsTenants = 47,
    Topology = 48,
    Status = 49,
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

    /// Series ids as an array of u64 (protocol 11).
    pub(crate) fn sids(&mut self, v: &[u64]) -> &mut Self {
        self.buf.reserve(v.len() * 8);
        for &x in v {
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

    /// Pads the payload (which starts after the 9-byte header) to a multiple of 8 bytes.
    pub(crate) fn align8(&mut self) -> &mut Self {
        let pad = (8 - (self.buf.len() - 9) % 8) % 8;
        self.buf.extend_from_slice(&[0; 7][..pad]);
        self
    }

    /// The frame's length so far, header included.
    pub(crate) fn len(&self) -> usize {
        self.buf.len()
    }

    /// The op of the frame begun.
    pub(crate) fn op(&self) -> u8 {
        self.buf[8]
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
