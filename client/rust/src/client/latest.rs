// SPDX-License-Identifier: Apache-2.0
//! `QUERY_LATEST` (protocol 14): the newest value of up to 2^20 series in one request, as `tsdbc_latest` in
//! `client/tsdb_client.c` sends it. The reply comes in frames of up to 65,536 records of 24 bytes, in index order.
//! A read-only request: UNAVAILABLE and MOVED before the first frame are retried like any read.

use super::*;
use crate::latest::{Latest, LatestOpts};
use crate::protocol::{LATEST_FRAME_IDS, LATEST_MAX_IDS, LATEST_PER_END, LATEST_REC, LATEST_WITH_NULLS};

/// Names kept per connection.
const NAME_CACHE_MAX: usize = 1 << 20;
/// Name lookups sent before their replies are read.
const NAME_WINDOW: usize = 256;

impl Client {
    /// The newest value of each series: the row with the greatest timestamp whose value is not null (below
    /// [`LatestOpts::end`]), as the last non-null row of an ascending query would give it. `out[i]` answers
    /// `ids[i]`.
    ///
    /// A series with no such row answers [`LatestKind::None`](crate::LatestKind::None). An id that names no series
    /// answers `status` [`Status::NotFound`]; one the user may not read, [`Status::Denied`] (existing or not).
    /// Each series is read at one instant during the call.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] for more than 1,048,576 ids or `ends` of another length;
    /// [`Error::Server`] with [`Status::Invalid`] without sending anything if the server speaks protocol 13 or
    /// older; [`Error::Io`] / [`Error::Protocol`] as for every call.
    ///
    /// ```no_run
    /// use tsdb_client::{Client, LatestOpts};
    ///
    /// let mut c = Client::connect("127.0.0.1:7070")?;
    /// let id = c.series_lookup("pump7.flow")?;
    /// let answers = c.latest(&[id], &LatestOpts::default())?;
    /// if let Some((ts, v)) = answers[0].point() {
    ///     println!("{ts} {v:?}");
    /// }
    /// # Ok::<(), tsdb_client::Error>(())
    /// ```
    pub fn latest(&mut self, ids: &[SeriesId], opts: &LatestOpts) -> Result<Vec<Latest>> {
        self.run(|c| c.latest_req(ids, opts))
    }

    /// [`latest`](Client::latest) by name. A name is resolved once per connection (`series_lookup`) and its id
    /// kept; the names not known yet are looked up together (up to 256 requests out before their replies are
    /// read), not one round trip each. A name that does not resolve answers [`Status::NotFound`] or [`Status::Denied`] in its own entry.
    ///
    /// # Errors
    ///
    /// As [`latest`](Client::latest).
    pub fn latest_by_name(&mut self, names: &[&str], opts: &LatestOpts) -> Result<Vec<Latest>> {
        self.run(|c| {
            if names.len() > LATEST_MAX_IDS {
                return Err(Error::InvalidArgument(format!("{} names (at most {LATEST_MAX_IDS})", names.len())));
            }
            check_ends(names.len(), opts)?;
            if names.is_empty() {
                return Ok(Vec::new());
            }
            c.latest_version()?;
            let mut out = vec![None; names.len()];
            let mut resolved: Vec<Option<SeriesId>> = vec![None; names.len()];
            let mut misses = Vec::new();
            for (i, name) in names.iter().enumerate() {
                if name.is_empty() {
                    out[i] = Some(Latest::missing(Status::Invalid));
                } else if let Some(id) = c.names.get(*name) {
                    resolved[i] = Some(*id);
                } else {
                    misses.push(i);
                }
            }
            c.names_resolve(names, &misses, &mut resolved, &mut out)?;
            let (mut ids, mut at) = (Vec::with_capacity(names.len()), Vec::with_capacity(names.len()));
            for (i, id) in resolved.iter().enumerate() {
                if let Some(id) = id {
                    ids.push(*id);
                    at.push(i);
                }
            }
            if !ids.is_empty() {
                let sub =
                    LatestOpts { ends: opts.ends.as_ref().map(|e| at.iter().map(|&i| e[i]).collect()), ..opts.clone() };
                for (r, i) in c.latest_req(&ids, &sub)?.into_iter().zip(at) {
                    out[i] = Some(r);
                }
            }
            Ok(out.into_iter().map(|r| r.expect("every name answered")).collect())
        })
    }

    /// Looks up the names `misses` (indexes into `names`) that the cache does not hold: the requests of a window go
    /// out together and their replies are read in order, so a cold list costs a round trip per window, not per
    /// name. A name the lookup refuses (not found, denied, invalid) is answered in its own `out` entry; UNAVAILABLE
    /// and MOVED, which a lone request retries, are asked again one at a time once the window is read. Any other
    /// status fails the call.
    fn names_resolve(
        &mut self,
        names: &[&str],
        misses: &[usize],
        resolved: &mut [Option<SeriesId>],
        out: &mut [Option<Latest>],
    ) -> Result<()> {
        let refused = |s: &Status| matches!(s, Status::NotFound | Status::Denied | Status::Invalid);
        for window in misses.chunks(NAME_WINDOW) {
            self.wait_inner()?;
            let mut sent = Vec::with_capacity(window.len());
            for &i in window {
                match self.series_send(names[i], false, None) {
                    Ok(()) => sent.push(i),
                    Err(Error::InvalidArgument(_)) => out[i] = Some(Latest::missing(Status::Invalid)),
                    Err(e) => return Err(e),
                }
            }
            let (mut again, mut first) = (Vec::new(), None);
            for &i in &sent {
                match self.series_recv() {
                    Ok(id) => {
                        resolved[i] = Some(id);
                        if self.names.len() < NAME_CACHE_MAX {
                            self.names.insert(names[i].to_string(), id);
                        }
                    }
                    Err(Error::Server(s)) if refused(&s) => out[i] = Some(Latest::missing(s)),
                    Err(Error::Server(Status::Unavailable | Status::Moved)) => again.push(i),
                    Err(e @ (Error::Io(_) | Error::Protocol(_))) => return Err(e),
                    Err(e) => {
                        first.get_or_insert(e);
                    }
                }
            }
            if let Some(e) = first {
                return Err(e);
            }
            for i in again {
                match self.series_req(names[i], false, None) {
                    Ok(id) => {
                        resolved[i] = Some(id);
                        if self.names.len() < NAME_CACHE_MAX {
                            self.names.insert(names[i].to_string(), id);
                        }
                    }
                    Err(Error::Server(s)) if refused(&s) => out[i] = Some(Latest::missing(s)),
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(())
    }

    /// The server must speak 14: an older one does not know the op, and nothing is sent to it.
    fn latest_version(&mut self) -> Result<()> {
        let v = match self.server_version {
            Some(v) => v,
            None => self.hello()?,
        };
        if v < 14 {
            return Err(Error::Server(Status::Invalid));
        }
        Ok(())
    }

    fn latest_req(&mut self, ids: &[SeriesId], opts: &LatestOpts) -> Result<Vec<Latest>> {
        let n = ids.len();
        if n > LATEST_MAX_IDS {
            return Err(Error::InvalidArgument(format!("{n} ids (at most {LATEST_MAX_IDS})")));
        }
        check_ends(n, opts)?;
        if n == 0 {
            return Ok(Vec::new());
        }
        self.wait_inner()?;
        self.latest_version()?;
        let flags = (if opts.with_nulls { LATEST_WITH_NULLS } else { 0 })
            | (if opts.ends.is_some() { LATEST_PER_END } else { 0 });
        self.request_begin(Op::QueryLatest, |f| {
            f.u8(flags).u8(0).u16(opts.threads).u32(n as u32).i64(if opts.ends.is_some() { 0 } else { opts.end });
            if let Some(e) = &opts.ends {
                for t in e {
                    f.i64(*t);
                }
            }
            for id in ids {
                f.u64(*id);
            }
        })?;
        let mut out = Vec::with_capacity(n);
        let mut status = 0;
        loop {
            let st = self.read_frame()?;
            let mut r = Cursor::new(&self.body);
            let first = r.u32()? as usize;
            let m = r.u32()? as usize;
            let last = r.u8()?;
            if r.take(3)? != [0, 0, 0] || last > 1 {
                return Err(Error::Protocol("QUERY_LATEST frame with a reserved byte set".into()));
            }
            if st != 0 {
                // the request's own status: an empty closing frame (for the HA statuses the body went to `err`)
                if m != 0 || last != 1 {
                    return Err(Error::Protocol("QUERY_LATEST error frame with records".into()));
                }
                status = st;
                break;
            }
            if first != out.len()
                || m > LATEST_FRAME_IDS
                || m > n - out.len()
                || r.remaining() != m * LATEST_REC
                || (last == 1) != (out.len() + m == n)
                || (last == 0 && m == 0)
            {
                return Err(Error::Protocol(format!("QUERY_LATEST frame: first {first}, {m} records, last {last}")));
            }
            for _ in 0..m {
                let (rs, kind, ty, pad) = (r.u32()? as i32, r.u8()?, r.u8()?, r.u16()?);
                let (ts, bits) = (r.i64()?, r.u64()?);
                let rec = Latest::from_wire(rs, kind, ty, ts, bits)
                    .filter(|l| pad == 0 && (opts.with_nulls || l.kind != crate::latest::LatestKind::Null))
                    .ok_or_else(|| Error::Protocol("QUERY_LATEST record out of shape".into()))?;
                out.push(rec);
            }
            if last == 1 {
                break;
            }
        }
        self.reply_id = self.reply_id.wrapping_add(1);
        self.trim();
        status_result(status)?;
        Ok(out)
    }

    /// Sends a request and leaves its reply to the caller (a streamed one).
    fn request_begin(&mut self, op: Op, build: impl FnOnce(&mut FrameBuf)) -> Result<()> {
        build(self.req.begin(self.next_id, op));
        self.send()
    }
}

fn check_ends(n: usize, opts: &LatestOpts) -> Result<()> {
    match &opts.ends {
        Some(e) if e.len() != n => Err(Error::InvalidArgument(format!("{} ends for {n} series", e.len()))),
        Some(_) if opts.end != i64::MAX => Err(Error::InvalidArgument("both end and ends given".into())),
        _ => Ok(()),
    }
}
