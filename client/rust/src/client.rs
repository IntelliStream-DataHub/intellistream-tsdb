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
use crate::protocol::{AGG_ROW, Cursor, FrameBuf, MANY_SERIES, MAX_FRAME, Op, PROTOCOL_VERSION, REPLY_HEADER};
use crate::types::{Bucket, Group, Order, Points, QueryOpts, Stats};
use crate::value::{self, VALUE_SIZE, Value};

/// Rows per INSERT frame: 8M rows of the many-series layout make 128 MiB,
/// half of the largest frame the server accepts.
const INSERT_FRAME_ROWS: usize = 8 << 20;
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
        if ts.len() != values.len() || series.is_some_and(|s| s.len() != ts.len()) {
            return Err(Error::InvalidArgument(format!(
                "column lengths differ: {} timestamps, {} values{}",
                ts.len(),
                values.len(),
                series.map_or(String::new(), |s| format!(", {} series", s.len()))
            )));
        }
        if series.is_none() && one == MANY_SERIES {
            return Err(Error::InvalidArgument(format!("series id {one} is reserved")));
        }
        let n = ts.len();
        let mut off = 0;
        loop {
            let m = (n - off).min(INSERT_FRAME_ROWS);
            if self.pending >= MAX_PIPELINE {
                self.collect_ack()?;
            }
            let f = self.req.begin(self.next_id, Op::Insert);
            f.u32(m as u32).u32(series.map_or(one, |_| MANY_SERIES)).i64s(&ts[off..off + m]);
            value::put_values(f.raw(), &values[off..off + m]);
            if let Some(s) = series {
                f.u32s(&s[off..off + m]);
            }
            let sent = self.send();
            self.req.shrink();
            sent?;
            self.pending += 1;
            off += m;
            if off >= n {
                return Ok(());
            }
        }
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

    /// Returns the points of `series` in `range`, in `opts.order`, at most `opts.limit` of them.
    pub fn query_points(&mut self, series: u32, range: impl RangeBounds<i64>, opts: QueryOpts) -> Result<Points> {
        let (start, end) = bounds(&range);
        self.run(|c| {
            let mut p = Points::default();
            c.raw_stream(series, start, end, opts, |ts, v| {
                extend_i64s(&mut p.ts, ts);
                value::extend_values(&mut p.values, v);
                ControlFlow::Continue(())
            })?;
            Ok(p)
        })
    }

    /// Streams the points of `series` in `range` to `f` in chunks (the server
    /// sends up to 16384 rows per chunk). Return [`ControlFlow::Break`] from
    /// `f` to stop early; the rest of the stream is then read and discarded,
    /// since the protocol cannot cancel a query.
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
                value::extend_values(&mut vs, vb);
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
        self.req
            .begin(self.next_id, Op::QueryRaw)
            .u32(series)
            .i64(start)
            .i64(end)
            .u8((opts.order == Order::Desc) as u8)
            .u64(limit);
        self.send()?;
        let (mut status, mut stopped) = (0, false);
        loop {
            let st = self.read_frame()?;
            if status == 0 {
                status = st;
            }
            let mut r = Cursor::new(&self.body);
            let last = r.u8()? != 0;
            let n = r.u32()? as usize;
            if r.remaining() != n * (8 + VALUE_SIZE) {
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

    /// Aggregates `series` over `range` into buckets of `interval_ms`, aligned
    /// to the epoch; 0 gives one bucket for the whole range. Only non-empty
    /// buckets are returned, in time order.
    pub fn query_agg(&mut self, series: u32, range: impl RangeBounds<i64>, interval_ms: i64) -> Result<Vec<Bucket>> {
        let (start, end) = bounds(&range);
        self.run(|c| {
            c.request(Op::QueryAgg, |f| {
                f.u32(series).i64(start).i64(end).i64(interval_ms);
            })?;
            c.read_buckets()
        })
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
        self.run(|c| {
            let tz = wire_str("time zone", group.and_then(|g| g.timezone.as_deref()).unwrap_or(""), true)?;
            let Some(limit) = opts.wire_limit() else {
                c.wait_inner()?;
                return Ok(Vec::new());
            };
            c.request(Op::QueryGroup, |f| {
                f.u32(series)
                    .i64(start)
                    .i64(end)
                    .u8(group.map_or(0, |g| g.unit as u8))
                    .u32(group.map_or(0, |g| g.count))
                    .u8((opts.order == Order::Desc) as u8)
                    .u64(limit)
                    .u16(tz.len() as u16)
                    .bytes(tz);
            })?;
            c.read_buckets()
        })
    }

    fn read_buckets(&self) -> Result<Vec<Bucket>> {
        let mut r = Cursor::new(&self.body);
        let n = r.u32()? as usize;
        if r.remaining() != n * AGG_ROW {
            return Err(Error::Protocol(format!("{n} buckets in {} bytes", r.remaining())));
        }
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(Bucket {
                start: r.i64()?,
                count: r.u64()?,
                sum: r.f64()?,
                min: value::get_value(r.take(VALUE_SIZE)?),
                max: value::get_value(r.take(VALUE_SIZE)?),
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
