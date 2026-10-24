//! The blocking client: one [`Client`] is one TCP connection.
//!
//! Requests are written as whole frames and replies read in order. The server
//! runs one connection's requests one at a time, so asynchronous inserts can
//! be pipelined and still keep "last write wins"; their acknowledgements are
//! counted and collected by [`Client::wait`] or by the next call that needs a
//! reply. Every reply's id is checked against the request it answers. After an
//! I/O or protocol error the stream may be out of step, so the client refuses
//! further calls rather than misread replies.

use std::fmt;
use std::io::{self, BufReader, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream, ToSocketAddrs};
use std::ops::{Bound, ControlFlow, RangeBounds};
use std::time::Duration;

use crate::error::{Error, Result, Status};
use crate::protocol::extend_i64s;
use crate::protocol::{
    BUCKET_ROW, Cursor, FrameBuf, MANY_SERIES, MAX_FRAME, Op, PROTOCOL_VERSION, REPLY_HEADER, VALID,
    VALUES_CHUNK_HEADER,
};
use crate::types::{Bucket, Group, Order, Points, QueryOpts, QueryStats, Rows, Stats};
use crate::value::{Bitmap, Column, ColumnRef, Native, Value, ValueType};

/// Rows per INSERT frame: 8M rows of the many-series layout make 128 MiB,
/// half of the largest frame the server accepts.
const INSERT_FRAME_ROWS: usize = 8 << 20;
/// Rows per INSERT_VALUES frame: 6M rows of the widest layout (8 + 8 bytes,
/// a bit, 4 bytes of series) make about 115 MiB. A multiple of 8, so every
/// frame's part of the validity bitmap starts on a byte.
const INSERT_VALUE_ROWS: usize = 6 << 20;
/// Unacknowledged inserts allowed in flight. Their replies (13 bytes each)
/// then always fit in the socket buffers, so neither side can block the other.
const MAX_PIPELINE: usize = 1024;
const READ_BUF: usize = 1 << 20;

/// A connection to tsdbd.
///
/// Methods take `&mut self`: use one client per thread, or put it behind a
/// mutex. Timestamps are `i64` milliseconds since the epoch; time ranges are
/// any [`RangeBounds<i64>`], so `t0..t1` is `[t0, t1)` and `..` is everything.
///
/// ```no_run
/// use tsdb_client::{Client, Group, QueryOpts, Unit};
///
/// let mut c = Client::connect("127.0.0.1:7070")?;
/// let id = c.series("pump7.flow")?;
/// c.insert_series(id, &[1_700_000_000_000, 1_700_000_060_000], &[1.5, 2.5])?;
/// let newest = c.query_points(id, .., QueryOpts::desc().limit(1))?;
/// let per_day = c.query_group(id, .., Some(&Group::new(Unit::Day).timezone("Europe/Oslo")), QueryOpts::asc())?;
/// # let _ = (newest, per_day);
/// # Ok::<(), tsdb_client::Error>(())
/// ```
pub struct Client {
    stream: BufReader<TcpStream>,
    req: FrameBuf,
    /// The payload of the last reply, after its status.
    body: Vec<u8>,
    next_id: u32,
    /// The id of the oldest request whose reply has not been read completely.
    reply_id: u32,
    /// Insert acknowledgements not yet read.
    pending: usize,
    /// The first error status among the acknowledgements read so far.
    pending_status: i32,
    broken: bool,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("peer", &self.stream.get_ref().peer_addr().ok())
            .field("pending", &self.pending)
            .field("broken", &self.broken)
            .finish()
    }
}

impl Client {
    /// Connects to tsdbd, trying each address `addr` resolves to.
    pub fn connect(addr: impl ToSocketAddrs) -> Result<Client> {
        Client::from_stream(TcpStream::connect(addr)?)
    }

    /// Connects to one address, giving up after `timeout`.
    pub fn connect_timeout(addr: &SocketAddr, timeout: Duration) -> Result<Client> {
        Client::from_stream(TcpStream::connect_timeout(addr, timeout)?)
    }

    /// Wraps a connected stream.
    pub fn from_stream(stream: TcpStream) -> Result<Client> {
        stream.set_nodelay(true)?;
        Ok(Client {
            stream: BufReader::with_capacity(READ_BUF, stream),
            req: FrameBuf::default(),
            body: Vec::new(),
            next_id: 0,
            reply_id: 0,
            pending: 0,
            pending_status: 0,
            broken: false,
        })
    }

    /// Sets the read and write timeout of the socket (`None`: block forever).
    /// A call that times out leaves the connection unusable.
    pub fn set_timeout(&self, timeout: Option<Duration>) -> Result<()> {
        let s = self.stream.get_ref();
        s.set_read_timeout(timeout)?;
        s.set_write_timeout(timeout)?;
        Ok(())
    }

    /// The server's protocol version (HELLO).
    pub fn protocol_version(&mut self) -> Result<u32> {
        self.run(|c| {
            c.request(Op::Hello, |f| {
                f.u32(PROTOCOL_VERSION);
            })?;
            let mut r = Cursor::new(&c.body);
            let v = r.u32()?;
            end(&r)?;
            Ok(v)
        })
    }

    /// Waits for pending inserts, then closes the connection. Returns the
    /// first error among the pending inserts. Dropping a client closes it
    /// without waiting.
    pub fn close(mut self) -> Result<()> {
        let r = if self.broken { Ok(()) } else { self.wait() };
        let _ = self.stream.get_ref().shutdown(Shutdown::Both);
        r
    }

    // ------------------------------------------------------------ series

    /// Returns the id of series `name`, creating it in the database directory if needed.
    pub fn series(&mut self, name: &str) -> Result<u32> {
        self.run(|c| c.series_req(name, true, None))
    }

    /// Returns the id of an existing series; [`Status::NotFound`] if there is none.
    pub fn series_lookup(&mut self, name: &str) -> Result<u32> {
        self.run(|c| c.series_req(name, false, None))
    }

    /// Returns the id of series `name`, creating it in the volume `path` on the
    /// server if needed (see `tsdb_series_at`). The path only matters at creation;
    /// an empty path means the database directory.
    pub fn series_at(&mut self, name: &str, path: &str) -> Result<u32> {
        self.run(|c| c.series_req(name, true, Some(path)))
    }

    /// The name of series `id`; [`Status::NotFound`] if there is none. Names
    /// that are not UTF-8 are converted lossily.
    pub fn series_name(&mut self, id: u32) -> Result<String> {
        self.run(|c| {
            c.request(Op::SeriesName, |f| {
                f.u32(id);
            })?;
            let mut r = Cursor::new(&c.body);
            let n = r.u16()? as usize;
            let name = String::from_utf8_lossy(r.take(n)?).into_owned();
            end(&r)?;
            Ok(name)
        })
    }

    fn series_req(&mut self, name: &str, create: bool, path: Option<&str>) -> Result<u32> {
        let name = wire_str("series name", name, false)?;
        let path = path.map(|p| wire_str("path", p, true)).transpose()?;
        self.request(Op::Series, |f| {
            f.u8(create as u8).u16(name.len() as u16).bytes(name);
            if let Some(p) = path {
                f.u16(p.len() as u16).bytes(p);
            }
        })?;
        let mut r = Cursor::new(&self.body);
        let id = r.u32()?;
        end(&r)?;
        Ok(id)
    }

    /// Returns the id of series `name` of type `ty`, creating it if needed in
    /// the volume `path` (`None`: the database directory). An existing series
    /// of another type is [`Status::Invalid`]. [`series`](Client::series) creates float32 series.
    pub fn series_create(&mut self, name: &str, ty: ValueType, path: Option<&str>) -> Result<u32> {
        self.run(|c| {
            let name = wire_str("series name", name, false)?;
            let path = wire_str("path", path.unwrap_or(""), true)?;
            c.request(Op::SeriesCreate, |f| {
                f.u8(ty.code()).u16(name.len() as u16).bytes(name).u16(path.len() as u16).bytes(path);
            })?;
            let mut r = Cursor::new(&c.body);
            let id = r.u32()?;
            end(&r)?;
            Ok(id)
        })
    }

    /// The value type of series `id`; [`Status::NotFound`] if there is none.
    pub fn series_type(&mut self, id: u32) -> Result<ValueType> {
        self.run(|c| {
            c.request(Op::SeriesType, |f| {
                f.u32(id);
            })?;
            let mut r = Cursor::new(&c.body);
            let code = r.u8()?;
            end(&r)?;
            ValueType::from_code(code).ok_or_else(|| Error::Protocol(format!("unknown value type {code}")))
        })
    }

    /// The code of state `name` in the dictionary of `series`'s volume,
    /// created if needed. Codes start at 1 and never change.
    pub fn state(&mut self, series: u32, name: &str) -> Result<u32> {
        self.run(|c| c.state_req(series, name, true))
    }

    /// The code of an existing state; [`Status::NotFound`] if there is none.
    pub fn state_lookup(&mut self, series: u32, name: &str) -> Result<u32> {
        self.run(|c| c.state_req(series, name, false))
    }

    /// The name of state `code`; [`Status::NotFound`] for an unknown code.
    pub fn state_name(&mut self, series: u32, code: u32) -> Result<String> {
        self.run(|c| {
            c.request(Op::StateName, |f| {
                f.u32(series).u32(code);
            })?;
            let mut r = Cursor::new(&c.body);
            let n = r.u16()? as usize;
            let name = String::from_utf8_lossy(r.take(n)?).into_owned();
            end(&r)?;
            Ok(name)
        })
    }

    fn state_req(&mut self, series: u32, name: &str, create: bool) -> Result<u32> {
        let name = wire_str("state name", name, false)?;
        self.request(Op::State, |f| {
            f.u32(series).u8(create as u8).u16(name.len() as u16).bytes(name);
        })?;
        let mut r = Cursor::new(&self.body);
        let code = r.u32()?;
        end(&r)?;
        Ok(code)
    }

    // ------------------------------------------------------------ inserts

    /// Inserts rows of one series and waits until the server has applied them
    /// (durable per the server's sync settings, and visible).
    ///
    /// Writing an existing `(series, ts)` replaces its value: the last write
    /// wins, and within one batch the later row. A batch is applied all or
    /// nothing, except that batches over 8M rows are sent as several requests.
    pub fn insert_series(&mut self, series: u32, ts: &[i64], values: &[Value]) -> Result<()> {
        self.run(|c| {
            c.insert_frames(None, series, ts, values)?;
            c.wait_inner()
        })
    }

    /// Inserts rows of many series (`series[i]` is the series of row `i`) and waits.
    pub fn insert(&mut self, series: &[u32], ts: &[i64], values: &[Value]) -> Result<()> {
        self.run(|c| {
            c.insert_frames(Some(series), 0, ts, values)?;
            c.wait_inner()
        })
    }

    /// Sends an insert of one series without waiting for the acknowledgement.
    /// Errors are reported by [`wait`](Client::wait), or by the next call that
    /// reads a reply (which then is not sent).
    pub fn insert_series_async(&mut self, series: u32, ts: &[i64], values: &[Value]) -> Result<()> {
        self.run(|c| c.insert_frames(None, series, ts, values))
    }

    /// Sends an insert of many series without waiting; see [`insert_series_async`](Client::insert_series_async).
    pub fn insert_async(&mut self, series: &[u32], ts: &[i64], values: &[Value]) -> Result<()> {
        self.run(|c| c.insert_frames(Some(series), 0, ts, values))
    }

    /// Inserts typed values, with nulls, into one series and waits. The
    /// column's type must be the series' type ([`Status::Invalid`] otherwise).
    ///
    /// ```no_run
    /// # use tsdb_client::{Client, Column};
    /// # let mut c = Client::connect("127.0.0.1:7070")?;
    /// # let (id, t0) = (1, 0);
    /// c.insert_series_values(id, &[t0, t0 + 1000], &[10i64, 20])?;
    /// c.insert_series_values(id, &[t0 + 2000, t0 + 3000], &Column::from_options([Some(30i64), None]))?;
    /// # Ok::<(), tsdb_client::Error>(())
    /// ```
    pub fn insert_series_values<'a>(
        &mut self,
        series: u32,
        ts: &[i64],
        values: impl Into<ColumnRef<'a>>,
    ) -> Result<()> {
        let col = values.into();
        self.run(|c| {
            c.insert_values_frames(None, series, ts, col)?;
            c.wait_inner()
        })
    }

    /// Inserts typed values into many series (`series[i]` is the series of
    /// row `i`, all of the column's type) and waits.
    pub fn insert_values<'a>(&mut self, series: &[u32], ts: &[i64], values: impl Into<ColumnRef<'a>>) -> Result<()> {
        let col = values.into();
        self.run(|c| {
            c.insert_values_frames(Some(series), 0, ts, col)?;
            c.wait_inner()
        })
    }

    /// Sends typed values for one series without waiting; see [`insert_series_async`](Client::insert_series_async).
    pub fn insert_series_values_async<'a>(
        &mut self,
        series: u32,
        ts: &[i64],
        values: impl Into<ColumnRef<'a>>,
    ) -> Result<()> {
        let col = values.into();
        self.run(|c| c.insert_values_frames(None, series, ts, col))
    }

    /// Sends typed values for many series without waiting.
    pub fn insert_values_async<'a>(
        &mut self,
        series: &[u32],
        ts: &[i64],
        values: impl Into<ColumnRef<'a>>,
    ) -> Result<()> {
        let col = values.into();
        self.run(|c| c.insert_values_frames(Some(series), 0, ts, col))
    }

    /// Collects the acknowledgements of all pending inserts and returns the
    /// first error among them.
    pub fn wait(&mut self) -> Result<()> {
        self.run(|c| c.wait_inner())
    }

    /// Number of inserts sent but not yet acknowledged.
    pub fn pending(&self) -> usize {
        self.pending
    }

    fn insert_frames(&mut self, series: Option<&[u32]>, one: u32, ts: &[i64], values: &[Value]) -> Result<()> {
        check_lengths(series, one, ts, values.len())?;
        let n = ts.len();
        let mut off = 0;
        loop {
            let m = (n - off).min(INSERT_FRAME_ROWS);
            if self.pending >= MAX_PIPELINE {
                self.collect_ack()?;
            }
            let f = self.req.begin(self.next_id, Op::Insert);
            f.u32(m as u32).u32(series.map_or(one, |_| MANY_SERIES)).i64s(&ts[off..off + m]);
            Native::put(&values[off..off + m], f.raw());
            if let Some(s) = series {
                f.u32s(&s[off..off + m]);
            }
            self.send_insert()?;
            off += m;
            if off >= n {
                return Ok(());
            }
        }
    }

    /// INSERT_VALUES frames: header, timestamps, values, the validity bitmap
    /// if any, then (aligned to 4) the series of each row.
    fn insert_values_frames(&mut self, series: Option<&[u32]>, one: u32, ts: &[i64], col: ColumnRef<'_>) -> Result<()> {
        check_lengths(series, one, ts, col.len())?;
        if let Some(v) = col.valid
            && v.len() != col.len()
        {
            return Err(Error::InvalidArgument(format!(
                "validity bitmap of {} bits for {} values",
                v.len(),
                col.len()
            )));
        }
        let n = ts.len();
        let mut off = 0;
        loop {
            // INSERT_VALUE_ROWS is a multiple of 8, so each frame's bitmap starts on a byte
            let m = (n - off).min(INSERT_VALUE_ROWS);
            if self.pending >= MAX_PIPELINE {
                self.collect_ack()?;
            }
            let f = self.req.begin(self.next_id, Op::InsertValues);
            f.u32(m as u32)
                .u32(series.map_or(one, |_| MANY_SERIES))
                .u8(col.value_type().code())
                .u8(if col.valid.is_some() { VALID } else { 0 })
                .u16(0)
                .u32(0)
                .i64s(&ts[off..off + m]);
            col.values.put(f.raw(), off, off + m);
            if let Some(v) = col.valid {
                f.bytes(&v.as_bytes()[off / 8..(off + m).div_ceil(8)]);
            }
            if let Some(s) = series {
                f.align4().u32s(&s[off..off + m]);
            }
            self.send_insert()?;
            off += m;
            if off >= n {
                return Ok(());
            }
        }
    }

    fn send_insert(&mut self) -> Result<()> {
        let sent = self.send();
        self.req.shrink();
        sent?;
        self.pending += 1;
        Ok(())
    }

    fn collect_ack(&mut self) -> Result<()> {
        let st = self.read_frame()?;
        self.reply_id = self.reply_id.wrapping_add(1);
        self.pending -= 1;
        if st != 0 && self.pending_status == 0 {
            self.pending_status = st;
        }
        Ok(())
    }

    fn wait_inner(&mut self) -> Result<()> {
        while self.pending > 0 {
            self.collect_ack()?;
        }
        status_result(std::mem::take(&mut self.pending_status))
    }

    // ------------------------------------------------------------ queries

    /// Returns the points of a float32 `series` in `range`, in `opts.order`,
    /// at most `opts.limit` of them (QUERY_RAW: nulls read as NaN; other types
    /// are [`Status::Invalid`], use [`query_rows`](Client::query_rows)).
    pub fn query_points(&mut self, series: u32, range: impl RangeBounds<i64>, opts: QueryOpts) -> Result<Points> {
        let (start, end) = bounds(&range);
        self.run(|c| {
            let mut p = Points::default();
            c.raw_stream(series, start, end, opts, |ts, v| {
                extend_i64s(&mut p.ts, ts);
                Native::extend(&mut p.values, v);
                ControlFlow::Continue(())
            })?;
            Ok(p)
        })
    }

    /// Streams the points of a float32 `series` in `range` to `f` in chunks
    /// (the server sends up to 16384 rows per chunk). Return
    /// [`ControlFlow::Break`] from `f` to stop early; the rest of the stream is
    /// then read and discarded, since the protocol cannot cancel a query.
    pub fn query_raw<F>(&mut self, series: u32, range: impl RangeBounds<i64>, opts: QueryOpts, mut f: F) -> Result<()>
    where
        F: FnMut(&[i64], &[Value]) -> ControlFlow<()>,
    {
        let (start, end) = bounds(&range);
        self.run(|c| {
            let (mut ts, mut vs) = (Vec::new(), Vec::new());
            c.raw_stream(series, start, end, opts, |tb, vb| {
                ts.clear();
                vs.clear();
                extend_i64s(&mut ts, tb);
                Native::extend(&mut vs, vb);
                f(&ts, &vs)
            })
        })
    }

    /// Sends a QUERY_RAW and passes the timestamp and value bytes of each chunk to `sink`.
    fn raw_stream<F>(&mut self, series: u32, start: i64, end: i64, opts: QueryOpts, mut sink: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> ControlFlow<()>,
    {
        self.wait_inner()?;
        let Some(limit) = opts.wire_limit() else {
            return Ok(());
        };
        self.send_range_query(Op::QueryRaw, series, start, end, opts.order, limit)?;
        let (mut status, mut stopped) = (0, false);
        loop {
            let st = self.read_frame()?;
            if status == 0 {
                status = st;
            }
            let mut r = Cursor::new(&self.body);
            let last = r.u8()? != 0;
            let n = r.u32()? as usize;
            if r.remaining() != n * 12 {
                return Err(Error::Protocol(format!("raw chunk of {n} rows has {} bytes", r.remaining())));
            }
            if n > 0 && !stopped {
                let cols = r.take(r.remaining())?;
                let (ts, vs) = cols.split_at(n * 8);
                stopped = sink(ts, vs).is_break();
            }
            if last {
                break;
            }
        }
        self.reply_id = self.reply_id.wrapping_add(1);
        status_result(status)
    }

    /// Returns the rows of `series` (any type, with nulls) in `range`, in
    /// `opts.order`, at most `opts.limit` of them. An unknown series has no
    /// rows (and reads as float32).
    pub fn query_rows(&mut self, series: u32, range: impl RangeBounds<i64>, opts: QueryOpts) -> Result<Rows> {
        let (start, end) = bounds(&range);
        self.run(|c| {
            let mut rows = Rows { ts: Vec::new(), values: Column::empty(ValueType::Float32) };
            let mut first = true;
            c.values_stream(series, start, end, opts, |chunk| {
                let col = &mut rows.values;
                if first {
                    *col = Column::empty(chunk.ty);
                    first = false;
                } else if chunk.ty != col.value_type() {
                    return Err(Error::Protocol(format!("chunk of {} in a {} series", chunk.ty, col.value_type())));
                }
                let before = rows.ts.len();
                extend_i64s(&mut rows.ts, chunk.ts);
                col.values.extend_wire(chunk.values);
                match (chunk.valid, &mut col.valid) {
                    (Some(bits), valid) => {
                        valid.get_or_insert_with(|| Bitmap::all_valid(before)).extend_from_wire(bits, chunk.n)
                    }
                    (None, Some(valid)) => valid.extend_valid(chunk.n),
                    (None, None) => {}
                }
                Ok(ControlFlow::Continue(()))
            })?;
            Ok(rows)
        })
    }

    /// Streams the rows of `series` (any type, with nulls) in `range` to `f`
    /// in chunks of up to 16384 rows; the [`Column`] of a chunk has a
    /// validity bitmap only if the chunk has nulls. Return
    /// [`ControlFlow::Break`] to stop early, as with [`query_raw`](Client::query_raw).
    pub fn query_values<F>(
        &mut self,
        series: u32,
        range: impl RangeBounds<i64>,
        opts: QueryOpts,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(&[i64], &Column) -> ControlFlow<()>,
    {
        let (start, end) = bounds(&range);
        self.run(|c| {
            let (mut ts, mut col) = (Vec::new(), Column::empty(ValueType::Float32));
            c.values_stream(series, start, end, opts, |chunk| {
                if chunk.n == 0 {
                    return Ok(ControlFlow::Continue(()));
                }
                ts.clear();
                extend_i64s(&mut ts, chunk.ts);
                if col.value_type() == chunk.ty {
                    col.clear();
                } else {
                    col = Column::empty(chunk.ty);
                }
                col.values.extend_wire(chunk.values);
                col.valid = chunk.valid.map(|b| {
                    let mut v = Bitmap::default();
                    v.extend_from_wire(b, chunk.n);
                    v
                });
                Ok(f(&ts, &col))
            })
        })
    }

    /// Sends a QUERY_VALUES and passes every chunk, empty ones included (they
    /// still carry the type), to `sink` until it breaks. A limit of `Some(0)`
    /// asks for one row and passes none, so the type still arrives.
    fn values_stream<F>(&mut self, series: u32, start: i64, end: i64, opts: QueryOpts, mut sink: F) -> Result<()>
    where
        F: FnMut(Chunk<'_>) -> Result<ControlFlow<()>>,
    {
        self.wait_inner()?;
        let (limit, mut stopped) = match opts.wire_limit() {
            Some(l) => (l, false),
            None => (1, true),
        };
        self.send_range_query(Op::QueryValues, series, start, end, opts.order, limit)?;
        let mut status = 0;
        let mut first = true;
        loop {
            let st = self.read_frame()?;
            if status == 0 {
                status = st;
            }
            let mut r = Cursor::new(&self.body);
            let h = r.take(VALUES_CHUNK_HEADER)?;
            let last = h[0] != 0;
            let ty = ValueType::from_code(h[1]).ok_or_else(|| Error::Protocol(format!("value type {}", h[1])))?;
            let n = u32::from_le_bytes(h[4..8].try_into().expect("4 bytes")) as usize;
            let vb = n * ty.wire_width();
            let valid = if h[2] & VALID != 0 { n.div_ceil(8) } else { 0 };
            if r.remaining() != n * 8 + vb + valid {
                return Err(Error::Protocol(format!("values chunk of {n} rows has {} bytes", r.remaining())));
            }
            if !stopped || first {
                let ts = r.take(n * 8)?;
                let values = r.take(vb)?;
                let bits = (valid > 0).then(|| r.take(valid)).transpose()?;
                let chunk = if stopped {
                    Chunk { ty, n: 0, ts: &[], values: &[], valid: None }
                } else {
                    Chunk { ty, n, ts, values, valid: bits }
                };
                stopped |= sink(chunk)?.is_break();
                first = false;
            }
            if last {
                break;
            }
        }
        self.reply_id = self.reply_id.wrapping_add(1);
        status_result(status)
    }

    fn send_range_query(&mut self, op: Op, series: u32, start: i64, end: i64, order: Order, limit: u64) -> Result<()> {
        self.req.begin(self.next_id, op).u32(series).i64(start).i64(end).u8((order == Order::Desc) as u8).u64(limit);
        self.send()
    }

    /// Aggregates `series` over `range` into buckets of `interval_ms`, aligned
    /// to the epoch; 0 gives one bucket for the whole range. Only non-empty
    /// buckets are returned, in time order.
    pub fn query_agg(&mut self, series: u32, range: impl RangeBounds<i64>, interval_ms: i64) -> Result<Vec<Bucket>> {
        let (start, end) = bounds(&range);
        self.run(|c| c.buckets(series, start, end, interval_ms, None, QueryOpts::asc()))
    }

    /// Aggregates `series` over `range` by calendar periods (`None`: one
    /// bucket for the whole range), in `opts.order`, at most `opts.limit` buckets.
    pub fn query_group(
        &mut self,
        series: u32,
        range: impl RangeBounds<i64>,
        group: Option<&Group>,
        opts: QueryOpts,
    ) -> Result<Vec<Bucket>> {
        let (start, end) = bounds(&range);
        self.run(|c| c.buckets(series, start, end, 0, group, opts))
    }

    /// QUERY_BUCKETS: fixed intervals (no group) or calendar groups.
    fn buckets(
        &mut self,
        series: u32,
        start: i64,
        end: i64,
        interval: i64,
        group: Option<&Group>,
        opts: QueryOpts,
    ) -> Result<Vec<Bucket>> {
        let tz = wire_str("time zone", group.and_then(|g| g.timezone.as_deref()).unwrap_or(""), true)?;
        let Some(limit) = opts.wire_limit() else {
            self.wait_inner()?;
            return Ok(Vec::new());
        };
        self.request(Op::QueryBuckets, |f| {
            f.u32(series)
                .i64(start)
                .i64(end)
                .i64(interval)
                .u8(group.map_or(0, |g| g.unit as u8))
                .u32(group.map_or(0, |g| g.count))
                .u8((opts.order == Order::Desc) as u8)
                .u64(limit)
                .u16(tz.len() as u16)
                .bytes(tz);
        })?;
        let mut r = Cursor::new(&self.body);
        let n = r.u32()? as usize;
        if r.remaining() != n * BUCKET_ROW {
            return Err(Error::Protocol(format!("{n} buckets in {} bytes", r.remaining())));
        }
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(Bucket {
                start: r.i64()?,
                count: r.u64()?,
                sum: r.f64()?,
                min: r.f64()?,
                max: r.f64()?,
                isum: r.i64()?,
                imin: r.i64()?,
                imax: r.i64()?,
            });
        }
        Ok(out)
    }

    // -------------------------------------------------------- maintenance

    /// Makes everything written so far durable, without creating parts.
    pub fn commit(&mut self) -> Result<()> {
        self.run(|c| c.request(Op::Commit, |_| {}))
    }

    /// Commits, then turns the server's memtable into parts.
    pub fn flush(&mut self) -> Result<()> {
        self.run(|c| c.request(Op::Flush, |_| {}))
    }

    /// Merges every partition down to one part.
    pub fn optimize(&mut self) -> Result<()> {
        self.run(|c| c.request(Op::Optimize, |_| {}))
    }

    /// Database statistics.
    pub fn stats(&mut self) -> Result<Stats> {
        self.run(|c| {
            c.request(Op::Stats, |_| {})?;
            let mut r = Cursor::new(&c.body);
            Ok(Stats {
                parts: r.u64()?,
                partitions: r.u64()?,
                rows: r.u64()?,
                bytes: r.u64()?,
                merges: r.u64()?,
                merged_rows: r.u64()?,
                passthrough_granules: r.u64()?,
                memtable_rows: r.u64()?,
            })
        })
    }

    /// What the server did for this connection's previous query (LAST_STATS):
    /// rows covered, decoded or answered from granule statistics, bytes, time.
    /// A query this client answered without asking the server (a limit of
    /// `Some(0)` on [`query_points`](Client::query_points) or
    /// [`query_group`](Client::query_group)) does not count; all zero before any query.
    pub fn last_query_stats(&mut self) -> Result<QueryStats> {
        self.run(|c| {
            c.request(Op::LastStats, |_| {})?;
            let mut r = Cursor::new(&c.body);
            let st = QueryStats {
                elapsed_s: r.f64()?,
                rows: r.u64()?,
                rows_decoded: r.u64()?,
                rows_from_stats: r.u64()?,
                memtable_rows: r.u64()?,
                bytes: r.u64()?,
                bytes_read: r.u64()?,
                parts: r.u64()?,
                granules: r.u64()?,
                granules_from_stats: r.u64()?,
                result_rows: r.u64()?,
            };
            end(&r)?;
            Ok(st)
        })
    }

    // ---------------------------------------------------------------- I/O

    /// Runs one public call; an I/O or protocol error leaves the client unusable.
    fn run<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        if self.broken {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::NotConnected,
                "connection unusable after an earlier error",
            )));
        }
        let r = f(self);
        if matches!(r, Err(Error::Io(_) | Error::Protocol(_))) {
            self.broken = true;
        }
        r
    }

    /// Sends a request after collecting pending inserts, and reads its reply into `body`.
    fn request(&mut self, op: Op, build: impl FnOnce(&mut FrameBuf)) -> Result<()> {
        self.wait_inner()?;
        build(self.req.begin(self.next_id, op));
        self.send()?;
        let st = self.read_frame()?;
        self.reply_id = self.reply_id.wrapping_add(1);
        status_result(st)
    }

    fn send(&mut self) -> Result<()> {
        let frame = self.req.finish();
        if frame.len() - 4 > MAX_FRAME {
            return Err(Error::InvalidArgument(format!("request of {} bytes is too large", frame.len())));
        }
        let mut s: &TcpStream = self.stream.get_ref();
        s.write_all(frame)?;
        self.next_id = self.next_id.wrapping_add(1);
        Ok(())
    }

    /// Reads the next reply frame, which must answer request `reply_id`, into
    /// `body`, and returns its status.
    fn read_frame(&mut self) -> Result<i32> {
        let mut h = [0u8; REPLY_HEADER];
        self.stream.read_exact(&mut h)?;
        let word = |i: usize| u32::from_le_bytes(h[i..i + 4].try_into().expect("4 bytes"));
        let (len, id, op) = (word(0) as usize, word(4), h[8]);
        if !(9..=MAX_FRAME).contains(&len) || op != Op::Reply as u8 {
            return Err(Error::Protocol(format!("bad reply frame: length {len}, op {op}")));
        }
        if id != self.reply_id {
            return Err(Error::Protocol(format!("reply for request {id}, expected {}", self.reply_id)));
        }
        self.body.clear();
        self.body.resize(len - 9, 0);
        self.stream.read_exact(&mut self.body)?;
        Ok(word(9) as i32)
    }
}

fn status_result(status: i32) -> Result<()> {
    match status {
        0 => Ok(()),
        s => Err(Error::Server(Status::from_code(s))),
    }
}

fn end(r: &Cursor<'_>) -> Result<()> {
    match r.remaining() {
        0 => Ok(()),
        n => Err(Error::Protocol(format!("{n} unexpected bytes at the end of a reply"))),
    }
}

/// Checks a string field: u16-length-prefixed, and without NUL bytes, which
/// the server would silently cut the string at.
fn wire_str<'a>(what: &str, s: &'a str, allow_empty: bool) -> Result<&'a [u8]> {
    if s.is_empty() && !allow_empty {
        return Err(Error::InvalidArgument(format!("{what} is empty")));
    }
    if s.len() > u16::MAX as usize {
        return Err(Error::InvalidArgument(format!("{what} is longer than 65535 bytes")));
    }
    if s.contains('\0') {
        return Err(Error::InvalidArgument(format!("{what} contains a NUL byte")));
    }
    Ok(s.as_bytes())
}

/// One QUERY_VALUES chunk, still in wire form.
struct Chunk<'a> {
    ty: ValueType,
    n: usize,
    ts: &'a [u8],
    values: &'a [u8],
    valid: Option<&'a [u8]>,
}

fn check_lengths(series: Option<&[u32]>, one: u32, ts: &[i64], values: usize) -> Result<()> {
    if ts.len() != values || series.is_some_and(|s| s.len() != ts.len()) {
        return Err(Error::InvalidArgument(format!(
            "column lengths differ: {} timestamps, {values} values{}",
            ts.len(),
            series.map_or(String::new(), |s| format!(", {} series", s.len()))
        )));
    }
    if series.is_none() && one == MANY_SERIES {
        return Err(Error::InvalidArgument(format!("series id {one} is reserved")));
    }
    Ok(())
}

/// Converts a range to the protocol's `[start, end)`.
fn bounds(r: &impl RangeBounds<i64>) -> (i64, i64) {
    let start = match r.start_bound() {
        Bound::Included(&s) => s,
        Bound::Excluded(&s) => s.saturating_add(1),
        Bound::Unbounded => i64::MIN,
    };
    let end = match r.end_bound() {
        Bound::Included(&e) => e.saturating_add(1),
        Bound::Excluded(&e) => e,
        Bound::Unbounded => i64::MAX,
    };
    (start, end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges() {
        assert_eq!(bounds(&(..)), (i64::MIN, i64::MAX));
        assert_eq!(bounds(&(5..10)), (5, 10));
        assert_eq!(bounds(&(5..=10)), (5, 11));
        assert_eq!(bounds(&(5..)), (5, i64::MAX));
        assert_eq!(bounds(&(Bound::Excluded(5), Bound::Unbounded)), (6, i64::MAX));
    }

    #[test]
    fn strings() {
        assert!(wire_str("x", "", false).is_err());
        assert!(wire_str("x", "", true).is_ok());
        assert!(wire_str("x", "a\0b", false).is_err());
        assert!(wire_str("x", &"a".repeat(65536), false).is_err());
    }

    #[test]
    fn limits() {
        assert_eq!(QueryOpts::asc().wire_limit(), Some(0));
        assert_eq!(QueryOpts::desc().limit(3).wire_limit(), Some(3));
        assert_eq!(QueryOpts::asc().limit(0).wire_limit(), None);
    }
}
