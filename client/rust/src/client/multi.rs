// SPDX-License-Identifier: Apache-2.0
//! Many series in one request (protocol version 10), as `tsdbc_query_multi` in `client/tsdb_client.c` sends them.
//!
//! QUERY_MULTI carries the single op's payload once, as a template, and the series (with per-series starts,
//! ends and intervals) as arrays; a list longer than `MULTI_MAX_SERIES` is split into successive requests,
//! and the indexes run on. The reply is the single op's frames for every series in index order, each with the
//! series' index after the status, then an END frame. A caller that stops early sends CANCEL (with the id of
//! the query it stops, so it takes no id of its own) and reads the reply to its END.
//!
//! Calendar periods in a time zone other than UTC become one union of UTC bounds: two VALUES probes give the
//! first and last row of every series, and the server trims the union per series (TRIM_LO, TRIM_HI) as the
//! single call computes bounds over the rows of one. Where the union is too long for one frame, or where TWA's
//! closed range sides differ between series, the series go one by one through the single calls.

use super::*;
use crate::protocol::{
    MULTI_END, MULTI_HEADER, MULTI_MAX_SERIES, MULTI_PER_END, MULTI_PER_INTERVAL, MULTI_PER_START, MULTI_TRIM_HI,
    MULTI_TRIM_LO,
};
use crate::types::{Buckets, MultiOpts};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Values,
    Lttb,
    Buckets,
    Spread,
    Twa,
}

/// One multi-series query, as the caller gave it.
#[derive(Clone, Copy)]
pub(super) struct MultiQ<'a> {
    kind: Kind,
    start: i64,
    end: i64,
    /// Order and limit of every kind but TWA.
    opts: QueryOpts,
    twa: TwaOpts,
    threshold: u64,
    buckets: Buckets<'a>,
}

/// What `multi_read` hands on, for the series of the index it comes with.
enum Item<'a> {
    /// Rows of VALUES or LTTB (also an empty chunk: it carries the type).
    Chunk(Chunk<'a>),
    /// The body of a bucket reply: u32 count, then the rows.
    Body(&'a [u8]),
    /// The series is complete, with this status.
    End(i32),
}

type Sink<'s> = dyn FnMut(usize, Item<'_>) -> Result<ControlFlow<()>> + 's;

/// The result rows of the bucket kinds, by type.
pub(super) trait BucketRow: Sized {
    const KIND: Kind;
    fn parse(b: &[u8]) -> Result<Vec<Self>>;
    /// The single call for one series (calendar periods only).
    fn single(c: &mut Client, series: SeriesId, start: i64, end: i64, g: &Group, q: &MultiQ<'_>) -> Result<Vec<Self>>;
}

impl BucketRow for Bucket {
    const KIND: Kind = Kind::Buckets;
    fn parse(b: &[u8]) -> Result<Vec<Self>> {
        parse_buckets(b)
    }
    fn single(c: &mut Client, series: SeriesId, start: i64, end: i64, g: &Group, q: &MultiQ<'_>) -> Result<Vec<Self>> {
        c.query_group(series, start..end, Some(g), q.opts)
    }
}

impl BucketRow for SpreadBucket {
    const KIND: Kind = Kind::Spread;
    fn parse(b: &[u8]) -> Result<Vec<Self>> {
        parse_spread(b)
    }
    fn single(c: &mut Client, series: SeriesId, start: i64, end: i64, g: &Group, q: &MultiQ<'_>) -> Result<Vec<Self>> {
        c.query_spread_group(series, start..end, Some(g), q.opts)
    }
}

impl BucketRow for TwaBucket {
    const KIND: Kind = Kind::Twa;
    fn parse(b: &[u8]) -> Result<Vec<Self>> {
        parse_twa(b)
    }
    fn single(c: &mut Client, series: SeriesId, start: i64, end: i64, g: &Group, q: &MultiQ<'_>) -> Result<Vec<Self>> {
        c.query_twa_group(series, start..end, Some(g), q.twa)
    }
}

impl Client {
    // ------------------------------------------------------------ many series

    /// The rows of every series in `series` over `range` (or each series' own range from `m`), as
    /// [`query_rows`](Client::query_rows) gives them for one, in one request (protocol version 10).
    ///
    /// The result has one entry per series, in the order of `series` (a series may come more than
    /// once): its rows, or the error the single call would return for it. An unknown id has no rows.
    /// Every series of one request reads the same instant of each volume, so an insert is seen in all
    /// of them or in none. Lists longer than 65,536 series go in several requests.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] if a per-series array of `m` is not as long as `series`;
    /// [`Status::TooManyBuckets`] past `m.max_rows` rows in all; [`Error::Io`] or
    /// [`Error::Protocol`] as for the single calls.
    ///
    /// ```no_run
    /// # use tsdb_client::{Client, MultiOpts, QueryOpts};
    /// # let mut c = Client::connect("127.0.0.1:7070")?;
    /// let ids = [1, 2, 3];
    /// for (id, rows) in ids.iter().zip(c.query_rows_multi(&ids, .., QueryOpts::desc().limit(1), MultiOpts::default())?) {
    ///     println!("{id}: {:?}", rows.map(|r| r.ts.first().copied()));
    /// }
    /// # Ok::<(), tsdb_client::Error>(())
    /// ```
    pub fn query_rows_multi(
        &mut self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        opts: QueryOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Rows>>> {
        let q = MultiQ::new(Kind::Values, &range, opts);
        self.run(|c| c.rows_multi(&q, series, m))
    }

    /// LTTB downsampling of every series in `series`, as [`query_lttb`](Client::query_lttb) for
    /// one, in one request; the result as [`query_rows_multi`](Client::query_rows_multi).
    pub fn query_lttb_multi(
        &mut self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        threshold: u64,
        opts: QueryOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Rows>>> {
        let mut q = MultiQ::new(Kind::Lttb, &range, opts);
        q.threshold = threshold;
        self.run(|c| c.rows_multi(&q, series, m))
    }

    /// Streams the rows of every series in `series` to `f`, a chunk at a time, in the order of
    /// `series`: `f(i, Ok((ts, values)))` for each chunk with rows of series `i`, `f(i, Err(..))`
    /// where series `i` fails (after the rows it had). Series without rows get no call. Return
    /// [`ControlFlow::Break`] to stop: the client sends CANCEL, reads the rest of the reply and
    /// returns `Ok`.
    ///
    /// # Errors
    ///
    /// As [`query_rows_multi`](Client::query_rows_multi).
    pub fn query_chunks_multi<F>(
        &mut self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        opts: QueryOpts,
        m: MultiOpts<'_>,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(usize, Result<(&[i64], &Column)>) -> ControlFlow<()>,
    {
        let q = MultiQ::new(Kind::Values, &range, opts);
        let drop_rows = opts.wire_limit().is_none();
        self.run(|c| {
            c.wait_inner()?;
            check_multi(series, &m)?;
            let (mut ts, mut col) = (Vec::new(), Column::empty(ValueType::Float32));
            c.multi_send_all(&q, series, m, &[], 0, &mut |ix, it| match it {
                Item::Chunk(chunk) if chunk.n > 0 && !drop_rows => {
                    ts.clear();
                    extend_i64s(&mut ts, chunk.ts);
                    col = chunk_column(&chunk);
                    Ok(f(ix, Ok((&ts, &col))))
                }
                Item::End(st) if st != 0 => Ok(f(ix, Err(Error::Server(Status::from_code(st))))),
                _ => Ok(ControlFlow::Continue(())),
            })?;
            Ok(())
        })
    }

    /// Aggregates every series in `series` over `range` (or each series' own range from `m`) into
    /// the buckets `b` asks for, in `opts.order`, at most `opts.limit` per series, as
    /// [`query_agg_ex`](Client::query_agg_ex), [`query_group`](Client::query_group) and
    /// [`query_bounds`](Client::query_bounds) do for one, in one request (protocol version 10).
    /// The result as [`query_rows_multi`](Client::query_rows_multi).
    ///
    /// Calendar periods in a time zone become one list of UTC bounds over the first to the last row
    /// of all the series (two probes ask for them), which the server cuts to each series' rows.
    ///
    /// ```no_run
    /// # use tsdb_client::{Buckets, Client, Group, MultiOpts, QueryOpts, Unit};
    /// # let mut c = Client::connect("127.0.0.1:7070")?;
    /// let days = Group::new(Unit::Day).timezone("Europe/Oslo");
    /// let per_series = c.query_buckets_multi(&[1, 2, 3], .., Buckets::Group(&days), QueryOpts::asc(), MultiOpts::default())?;
    /// # Ok::<(), tsdb_client::Error>(())
    /// ```
    pub fn query_buckets_multi(
        &mut self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        b: Buckets<'_>,
        opts: QueryOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Vec<Bucket>>>> {
        let mut q = MultiQ::new(Kind::Buckets, &range, opts);
        q.buckets = b;
        self.run(|c| c.bucket_multi(&q, series, m))
    }

    /// The spread of every series in `series`, as [`query_spread`](Client::query_spread) and its
    /// group and bounds forms for one; otherwise as [`query_buckets_multi`](Client::query_buckets_multi).
    pub fn query_spread_multi(
        &mut self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        b: Buckets<'_>,
        opts: QueryOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Vec<SpreadBucket>>>> {
        let mut q = MultiQ::new(Kind::Spread, &range, opts);
        q.buckets = b;
        self.run(|c| c.bucket_multi(&q, series, m))
    }

    /// Time-weighted averages of every series in `series`, as [`query_twa`](Client::query_twa) and
    /// its group and bounds forms for one; otherwise as [`query_buckets_multi`](Client::query_buckets_multi).
    pub fn query_twa_multi(
        &mut self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        b: Buckets<'_>,
        opts: TwaOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Vec<TwaBucket>>>> {
        let mut q = MultiQ::new(Kind::Twa, &range, QueryOpts::default());
        q.twa = opts;
        q.buckets = b;
        self.run(|c| c.bucket_multi(&q, series, m))
    }

    fn rows_multi(&mut self, q: &MultiQ<'_>, series: &[SeriesId], m: MultiOpts<'_>) -> Result<Vec<Result<Rows>>> {
        self.wait_inner()?;
        check_multi(series, &m)?;
        // a limit of 0 asks for one row and keeps none, so that the type still arrives (as query_rows)
        let drop_rows = q.opts.wire_limit().is_none();
        let mut out: Vec<Result<Rows>> = Vec::with_capacity(series.len());
        let mut cur: Option<Rows> = None;
        self.multi_send_all(q, series, m, &[], 0, &mut |ix, it| {
            if ix != out.len() {
                return Err(Error::Protocol(format!("rows of series {ix} after {}", out.len())));
            }
            match it {
                Item::Chunk(chunk) => {
                    let rows = cur.get_or_insert_with(|| Rows { ts: Vec::new(), values: Column::empty(chunk.ty) });
                    append_chunk(rows, &chunk, drop_rows)?;
                }
                Item::End(st) => {
                    let rows = cur.take();
                    out.push(match st {
                        0 => Ok(rows.unwrap_or(Rows { ts: Vec::new(), values: Column::empty(ValueType::Float32) })),
                        st => Err(Error::Server(Status::from_code(st))),
                    });
                }
                Item::Body(_) => return Err(Error::Protocol("bucket rows in a VALUES reply".into())),
            }
            Ok(ControlFlow::Continue(()))
        })?;
        complete(out, series.len())
    }

    fn bucket_multi<T: BucketRow>(
        &mut self,
        q: &MultiQ<'_>,
        series: &[SeriesId],
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Vec<T>>>> {
        self.wait_inner()?;
        check_multi(series, &m)?;
        let mut zone = None;
        match q.buckets {
            Buckets::Every(i) if i < 0 => return Err(Error::Server(Status::Invalid)),
            Buckets::Intervals(iv) if iv.len() != series.len() => {
                return Err(Error::InvalidArgument(format!("{} intervals for {} series", iv.len(), series.len())));
            }
            Buckets::Bounds(b) => check_bounds(b)?,
            Buckets::Group(g) => {
                if let Some(z) = g.timezone.as_deref().filter(|&z| z != "UTC") {
                    calendar::zone(z)?; // an unknown zone is an error even without rows
                    zone = Some(z);
                }
            }
            _ => {}
        }
        let limit = if T::KIND == Kind::Twa { q.twa.query_opts() } else { q.opts }.wire_limit();
        if series.is_empty() || limit.is_none() {
            // nothing asked, but a negative interval is still the series' error, as from the single call
            return Ok((0..series.len()).map(|i| nothing(&q.buckets, i)).collect());
        }
        let mut out = Vec::with_capacity(series.len());
        match (zone, q.buckets) {
            (Some(z), Buckets::Group(g)) => self.multi_local(q, z, g, series, m, &mut out)?,
            (_, Buckets::Bounds(b)) => self.collect_buckets(q, series, m, b, 0, &mut out)?,
            _ => self.collect_buckets(q, series, m, &[], 0, &mut out)?,
        }
        complete(out, series.len())
    }

    /// Bucket replies of `q` for `series` into `out`.
    fn collect_buckets<T: BucketRow>(
        &mut self,
        q: &MultiQ<'_>,
        series: &[SeriesId],
        m: MultiOpts<'_>,
        bounds: &[i64],
        trim: u8,
        out: &mut Vec<Result<Vec<T>>>,
    ) -> Result<()> {
        let base = out.len();
        let mut cur = None;
        self.multi_send_all(q, series, m, bounds, trim, &mut |ix, it| {
            if base + ix != out.len() {
                return Err(Error::Protocol(format!("buckets of series {ix} after {}", out.len() - base)));
            }
            match it {
                Item::Body(b) => cur = Some(T::parse(b)?),
                Item::End(0) => out.push(Ok(cur.take().unwrap_or_default())),
                Item::End(st) => out.push(Err(Error::Server(Status::from_code(st)))),
                Item::Chunk(_) => return Err(Error::Protocol("rows in a bucket reply".into())),
            }
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(())
    }

    /// Calendar periods in a time zone: one union of bounds, trimmed per series by the server.
    fn multi_local<T: BucketRow>(
        &mut self,
        q: &MultiQ<'_>,
        zone: &str,
        g: &Group,
        series: &[SeriesId],
        m: MultiOpts<'_>,
        out: &mut Vec<Result<Vec<T>>>,
    ) -> Result<()> {
        let twa = T::KIND == Kind::Twa;
        let (us, s0) = uniform(m.starts, q.start);
        let (ue, e0) = uniform(m.ends, q.end);
        // buckets are cut to the rows on both sides; TWA only on open sides (a closed one is the range's edge)
        let open_lo = !twa || (us && s0 == i64::MIN);
        let open_hi = !twa || (ue && e0 == i64::MAX);
        if twa && ((!us && !open_lo) || (!ue && !open_hi)) {
            return self.multi_single(q, g, series, m, out);
        }
        let n = series.len();
        let (mut lo, mut hi) = (vec![i64::MAX; n], vec![i64::MIN; n]);
        let probe = |c: &mut Client, order: Order, edge: &mut Vec<i64>| {
            let vq = MultiQ::new(Kind::Values, &(q.start..q.end), QueryOpts { order, limit: Some(1) });
            let pm = MultiOpts { max_rows: 0, ..m };
            c.multi_send_all(&vq, series, pm, &[], 0, &mut |ix, it| {
                if let Item::Chunk(ch) = it
                    && ch.n > 0
                {
                    edge[ix] = i64::from_le_bytes(ch.ts[..8].try_into().expect("8 bytes"));
                }
                Ok(ControlFlow::Continue(()))
            })
        };
        if open_lo {
            probe(self, Order::Asc, &mut lo)?;
        }
        if open_hi {
            probe(self, Order::Desc, &mut hi)?;
        }
        let l = if open_lo { lo.iter().copied().min().unwrap_or(i64::MAX) } else { s0 };
        let h = if open_hi {
            hi.iter().copied().max().unwrap_or(i64::MIN)
        } else if e0 == i64::MIN {
            i64::MIN
        } else {
            e0 - 1
        };
        if l > h {
            // no rows anywhere (or an empty range): no buckets, as the single call answers
            out.extend(series.iter().map(|_| Ok(Vec::new())));
            return Ok(());
        }
        let b = calendar::calendar_bounds(zone, g.unit, g.count, l..h.saturating_add(1))?;
        if b.len() > (MAX_FRAME - (8 << 20)) / 8 {
            return self.multi_single(q, g, series, m, out); // the union is too long for one frame
        }
        let trim = if open_lo { MULTI_TRIM_LO } else { 0 } | if open_hi { MULTI_TRIM_HI } else { 0 };
        let uq = MultiQ { buckets: Buckets::Bounds(&b), ..*q };
        self.collect_buckets(&uq, series, m, &b, trim, out)
    }

    /// The series one by one through the single calls.
    fn multi_single<T: BucketRow>(
        &mut self,
        q: &MultiQ<'_>,
        g: &Group,
        series: &[SeriesId],
        m: MultiOpts<'_>,
        out: &mut Vec<Result<Vec<T>>>,
    ) -> Result<()> {
        let mut rows = 0u64;
        for (i, &id) in series.iter().enumerate() {
            let s = m.starts.map_or(q.start, |a| a[i]);
            let e = m.ends.map_or(q.end, |a| a[i]);
            match T::single(self, id, s, e, g, q) {
                Ok(v) => {
                    rows += v.len() as u64;
                    if m.max_rows != 0 && rows > m.max_rows {
                        return Err(Error::Server(Status::TooManyBuckets));
                    }
                    out.push(Ok(v));
                }
                Err(Error::Server(st)) => out.push(Err(Error::Server(st))),
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Sends `q` for `series` in requests of at most `MULTI_MAX_SERIES` and reads each reply, passing
    /// every item to `sink` with the index into `series`. `bounds` (with `trim`) replace the bucket form.
    fn multi_send_all(
        &mut self,
        q: &MultiQ<'_>,
        series: &[SeriesId],
        m: MultiOpts<'_>,
        bounds: &[i64],
        trim: u8,
        sink: &mut Sink<'_>,
    ) -> Result<()> {
        let (op, fields, t) = template(q, &m, bounds)?;
        let per_interval = match q.buckets {
            Buckets::Intervals(iv) if bounds.is_empty() && q.kind != Kind::Values && q.kind != Kind::Lttb => Some(iv),
            _ => None,
        };
        let flags = if m.starts.is_some() { MULTI_PER_START } else { 0 }
            | if m.ends.is_some() { MULTI_PER_END } else { 0 }
            | if per_interval.is_some() { MULTI_PER_INTERVAL } else { 0 }
            | if bounds.is_empty() { 0 } else { trim & (MULTI_TRIM_LO | MULTI_TRIM_HI) };
        let values = op == Op::QueryValues || op == Op::QueryLttb;
        let row = if op == Op::QueryTwa {
            TWA_ROW
        } else if fields & SPREAD != 0 {
            SPREAD_ROW
        } else {
            BUCKET_ROW
        };
        let mut rows = 0u64;
        for at in (0..series.len()).step_by(MULTI_MAX_SERIES) {
            let k = (series.len() - at).min(MULTI_MAX_SERIES);
            let left = match m.max_rows {
                0 => 0,
                max if rows >= max => return Err(Error::Server(Status::TooManyBuckets)),
                max => max - rows,
            };
            let narrays = [m.starts.is_some(), m.ends.is_some(), per_interval.is_some()].iter().filter(|&&b| b).count();
            let body = 5 + MULTI_HEADER + narrays * k * 8 + k * 8 + t.len();
            if body > MAX_FRAME {
                return Err(Error::Server(Status::TooManyBuckets)); // bounds that no frame carries
            }
            let f = self.req.begin(self.next_id, Op::QueryMulti);
            f.u8(op as u8).u8(flags).u16(m.threads).u32(k as u32).u64(left).u32(t.len() as u32).u32(0);
            if let Some(s) = m.starts {
                f.i64s(&s[at..at + k]);
            }
            if let Some(e) = m.ends {
                f.i64s(&e[at..at + k]);
            }
            if let Some(iv) = per_interval {
                f.i64s(&iv[at..at + k]);
            }
            f.sids(&series[at..at + k]).bytes(&t);
            self.send()?;
            self.req.shrink();
            let stopped = self.multi_read(values, row, k, &mut rows, &mut |ix, it| sink(at + ix, it))?;
            if stopped {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Reads the reply of the QUERY_MULTI just sent for `n` series. Returns whether `sink` stopped it
    /// (a CANCEL went out and the reply was read to its END); an error status of the END is the error.
    fn multi_read(&mut self, values: bool, row: usize, n: usize, rows: &mut u64, sink: &mut Sink<'_>) -> Result<bool> {
        let id = self.reply_id;
        let (mut next, mut open, mut stopped, mut cancel_sent) = (0usize, false, false, false);
        loop {
            let st = self.read_frame()?;
            if self.body.len() < MULTI_PREFIX {
                return Err(Error::Protocol(format!("multi reply frame of {} bytes", self.body.len())));
            }
            let ix = u32::from_le_bytes(self.body[..4].try_into().expect("4 bytes"));
            let b = &self.body[MULTI_PREFIX..];
            if ix == MULTI_END {
                // an error END carries the HA body after its u32 answered
                let ha = (-13..=-10).contains(&st);
                if b.len() < 4 || (b.len() != 4 && !ha) || open {
                    return Err(Error::Protocol("END frame in the middle of a series".into()));
                }
                let answered = u32::from_le_bytes(b[..4].try_into().expect("4 bytes")) as usize;
                self.reply_id = self.reply_id.wrapping_add(1);
                return match st {
                    0 if answered != n && !stopped => {
                        Err(Error::Protocol(format!("{answered} of {n} series answered without an error")))
                    }
                    0 => Ok(stopped),
                    _ if stopped && Status::from_code(st) == Status::Stopped => Ok(true),
                    st => Err(Error::Server(Status::from_code(st))),
                };
            }
            let ix = ix as usize;
            if ix >= n || ix < next || (open && ix != next) {
                return Err(Error::Protocol(format!("series {ix} out of order (next {next} of {n})")));
            }
            if values {
                let mut r = Cursor::new(b);
                let h = r.take(VALUES_CHUNK_HEADER)?;
                let last = h[0] != 0;
                let ty = ValueType::from_code(h[1]).ok_or_else(|| Error::Protocol(format!("value type {}", h[1])))?;
                let k = u32::from_le_bytes(h[4..8].try_into().expect("4 bytes")) as usize;
                let vb = k * ty.wire_width();
                let valid = if h[2] & VALID != 0 { k.div_ceil(8) } else { 0 };
                if r.remaining() != k * 8 + vb + valid || (st != 0 && !last) {
                    return Err(Error::Protocol(format!("values chunk of {k} rows has {} bytes", r.remaining())));
                }
                if !stopped {
                    let (ts, v) = (r.take(k * 8)?, r.take(vb)?);
                    let bits = (valid > 0).then(|| r.take(valid)).transpose()?;
                    *rows += k as u64;
                    stopped = sink(ix, Item::Chunk(Chunk { ty, n: k, ts, values: v, valid: bits }))?.is_break();
                    if last && !stopped {
                        stopped = sink(ix, Item::End(st))?.is_break();
                    }
                }
                open = !last;
                next = if last { ix + 1 } else { ix };
            } else {
                let ok = if st != 0 {
                    b.is_empty()
                } else {
                    b.len() >= 4
                        && b.len() == 4 + u32::from_le_bytes(b[..4].try_into().expect("4 bytes")) as usize * row
                };
                if !ok {
                    return Err(Error::Protocol(format!("bucket reply of {} bytes for status {st}", b.len())));
                }
                if !stopped {
                    if st == 0 {
                        *rows += ((b.len() - 4) / row) as u64;
                        stopped = sink(ix, Item::Body(b))?.is_break();
                    }
                    if !stopped {
                        stopped = sink(ix, Item::End(st))?.is_break();
                    }
                }
                next = ix + 1;
            }
            if stopped && !cancel_sent {
                self.send_cancel(id)?;
                cancel_sent = true;
            }
        }
    }

    /// CANCEL of the QUERY_MULTI `id`. It gets no reply, so it carries the id of the query it stops
    /// and takes none of its own.
    fn send_cancel(&mut self, id: u32) -> Result<()> {
        let mut f = [0u8; 13];
        f[..4].copy_from_slice(&9u32.to_le_bytes());
        f[4..8].copy_from_slice(&id.to_le_bytes());
        f[8] = Op::Cancel as u8;
        f[9..].copy_from_slice(&id.to_le_bytes());
        let mut s: &TcpStream = self.stream.get_ref();
        s.write_all(&f)?;
        self.bytes_sent += f.len() as u64;
        Ok(())
    }
}

impl<'a> MultiQ<'a> {
    fn new(kind: Kind, range: &impl RangeBounds<i64>, opts: QueryOpts) -> Self {
        let (start, end) = bounds(range);
        MultiQ { kind, start, end, opts, twa: TwaOpts::default(), threshold: 0, buckets: Buckets::One }
    }
}

/// The single op's payload for series UINT64_MAX, with 0 in every field a per-series array replaces.
/// `bounds` (non-empty) replace the bucket form of `q`.
fn template(q: &MultiQ<'_>, m: &MultiOpts<'_>, bounds: &[i64]) -> Result<(Op, u8, Vec<u8>)> {
    let start = if m.starts.is_some() { 0 } else { q.start };
    let end = if m.ends.is_some() { 0 } else { q.end };
    let spread = q.kind == Kind::Spread;
    let fields = if spread { SPREAD } else { 0 };
    let (interval, group) = match q.buckets {
        Buckets::Every(i) => (i, None),
        Buckets::Group(g) => (0, Some(g)),
        _ => (0, None),
    };
    let (unit, count) = group.map_or((0, 0), |g| (g.unit as u8, g.count));
    let nb = bounds.len() as u32;
    let mut t = Vec::with_capacity(64 + bounds.len() * 8);
    t.extend_from_slice(&MANY_SERIES.to_le_bytes());
    t.extend_from_slice(&start.to_le_bytes());
    t.extend_from_slice(&end.to_le_bytes());
    let order = if q.kind == Kind::Twa { q.twa.order } else { q.opts.order };
    let desc = (order == Order::Desc) as u8;
    // a limit of 0 never reaches here, except as VALUES or LTTB rows that are dropped (one row asked)
    let limit = if q.kind == Kind::Twa { q.twa.query_opts() } else { q.opts }.wire_limit().unwrap_or(1);
    let op = match q.kind {
        Kind::Values => {
            t.push(desc);
            t.extend_from_slice(&limit.to_le_bytes());
            Op::QueryValues
        }
        Kind::Lttb => {
            t.extend_from_slice(&q.threshold.to_le_bytes());
            t.push(desc);
            t.extend_from_slice(&limit.to_le_bytes());
            Op::QueryLttb
        }
        Kind::Buckets | Kind::Spread if !bounds.is_empty() => {
            t.push(desc);
            t.extend_from_slice(&limit.to_le_bytes());
            t.extend_from_slice(&nb.to_le_bytes());
            bounds.iter().for_each(|b| t.extend_from_slice(&b.to_le_bytes()));
            if spread {
                t.push(SPREAD);
            }
            Op::QueryBounds
        }
        Kind::Buckets | Kind::Spread => {
            t.extend_from_slice(&(if group.is_some() { 0 } else { interval }).to_le_bytes());
            t.push(unit);
            t.extend_from_slice(&count.to_le_bytes());
            t.push(desc);
            t.extend_from_slice(&limit.to_le_bytes());
            t.extend_from_slice(&0u16.to_le_bytes()); // no zone: UTC (others come as bounds)
            if spread {
                t.push(SPREAD);
            }
            Op::QueryBuckets
        }
        Kind::Twa => {
            let some = !bounds.is_empty();
            t.push(q.twa.method as u8);
            t.push(desc);
            t.push(if some { 0 } else { unit });
            t.push(0);
            t.extend_from_slice(&q.twa.max_gap_ms.to_le_bytes());
            t.extend_from_slice(&limit.to_le_bytes());
            t.extend_from_slice(&(if some || group.is_some() { 0 } else { interval }).to_le_bytes());
            t.extend_from_slice(&(if some { 0 } else { count }).to_le_bytes());
            t.extend_from_slice(&nb.to_le_bytes());
            bounds.iter().for_each(|b| t.extend_from_slice(&b.to_le_bytes()));
            debug_assert_eq!(t.len(), TWA_HEADER + bounds.len() * 8);
            Op::QueryTwa
        }
    };
    Ok((op, fields, t))
}

/// The per-series arrays of `m` must be as long as the series list.
fn check_multi(series: &[SeriesId], m: &MultiOpts<'_>) -> Result<()> {
    for (what, a) in [("starts", m.starts), ("ends", m.ends)] {
        if let Some(a) = a
            && a.len() != series.len()
        {
            return Err(Error::InvalidArgument(format!("{} {what} for {} series", a.len(), series.len())));
        }
    }
    Ok(())
}

/// All equal (`true`, the value), or mixed (`false`).
fn uniform(a: Option<&[i64]>, one: i64) -> (bool, i64) {
    match a {
        None => (true, one),
        Some(a) => (a.iter().all(|&v| v == a[0]), a.first().copied().unwrap_or(one)),
    }
}

/// A result per series, or the reply was short of some.
fn complete<T>(out: Vec<T>, n: usize) -> Result<Vec<T>> {
    if out.len() != n {
        return Err(Error::Protocol(format!("{} of {n} series answered", out.len())));
    }
    Ok(out)
}

/// A chunk as a column of its own.
fn chunk_column(chunk: &Chunk<'_>) -> Column {
    let mut col = Column::empty(chunk.ty);
    col.values.extend_wire(chunk.values);
    col.valid = chunk.valid.map(|b| {
        let mut v = Bitmap::default();
        v.extend_from_wire(b, chunk.n);
        v
    });
    col
}

/// Appends a chunk to the rows of its series (only its type when `drop_rows`).
fn append_chunk(rows: &mut Rows, chunk: &Chunk<'_>, drop_rows: bool) -> Result<()> {
    let col = &mut rows.values;
    if chunk.ty != col.value_type() {
        return Err(Error::Protocol(format!("chunk of {} in a {} series", chunk.ty, col.value_type())));
    }
    if drop_rows || chunk.n == 0 {
        return Ok(());
    }
    let before = rows.ts.len();
    extend_i64s(&mut rows.ts, chunk.ts);
    col.values.extend_wire(chunk.values);
    match (chunk.valid, &mut col.valid) {
        (Some(bits), valid) => valid.get_or_insert_with(|| Bitmap::all_valid(before)).extend_from_wire(bits, chunk.n),
        (None, Some(valid)) => valid.extend_valid(chunk.n),
        (None, None) => {}
    }
    Ok(())
}

/// The result of series `i` when the limit asks for no buckets.
fn nothing<T>(b: &Buckets<'_>, i: usize) -> Result<Vec<T>> {
    match b {
        Buckets::Intervals(iv) if iv[i] < 0 => Err(Error::Server(Status::Invalid)),
        _ => Ok(Vec::new()),
    }
}
