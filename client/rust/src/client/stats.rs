// SPDX-License-Identifier: Apache-2.0
//! `STATS_SERIES` and `STATS_TENANTS` (protocol 15): the storage numbers of series and the numbers of tenants, as
//! `tsdbc_series_stats` and `tsdbc_tenant_stats` in `client/tsdb_client.c` ask for them. Both are read-only requests.

use super::*;
use crate::protocol::{
    STATS_RESIDENT, STATS_SERIES_MAX_IDS, STATS_SERIES_RESIDENT_MAX, TSTATS_CALLER, TSTATS_CALLER_FIELDS,
    TSTATS_DATA_FIELDS, TSTATS_DONE, TSTATS_HOT, TSTATS_HOT_REC, TSTATS_MAX_K, TSTATS_ROW_HEAD, TSTATS_VOLUME_SUM,
    TSTATS_VOLUMES,
};
use crate::stats::{Hot, HotKind, SeriesStat, TenantCaller, TenantNumbers, TenantOpts, TenantRow, TenantStats};

/// Restarts of a tenant listing whose list changed between two replies.
const TENANT_RESTARTS: u32 = 3;

impl Client {
    /// The storage numbers of each series (`out[i]` for `ids[i]`), computed by the server when asked from the
    /// granule index and the memtables. With `resident`, `resident_bytes` is filled (one `mincore` per part and
    /// series: 1,024 ids to a request instead of 65,536).
    ///
    /// Any number of ids: the call sends them in requests and asks again for what the server's work budget left.
    /// A series the user may not read answers `status` [`Status::Denied`], existing or not; a missing one for a
    /// user who reads every series, [`Status::NotFound`].
    ///
    /// # Errors
    ///
    /// [`Error::Server`] with [`Status::Invalid`], without sending anything, if the server speaks protocol 14 or
    /// older or runs with `--no-stats`; [`Error::Io`] / [`Error::Protocol`] as for every call.
    ///
    /// ```no_run
    /// use tsdb_client::Client;
    ///
    /// let mut c = Client::connect("127.0.0.1:7070")?;
    /// let id = c.series_lookup("pump7.flow")?;
    /// let s = c.series_stats(&[id], false)?;
    /// println!("{} rows in {} bytes", s[0].rows, s[0].ts_bytes + s[0].value_bytes);
    /// # Ok::<(), tsdb_client::Error>(())
    /// ```
    pub fn series_stats(&mut self, ids: &[SeriesId], resident: bool) -> Result<Vec<SeriesStat>> {
        self.run(|c| {
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            c.stats_version()?;
            let chunk = if resident { STATS_SERIES_RESIDENT_MAX } else { STATS_SERIES_MAX_IDS };
            let mut out = Vec::with_capacity(ids.len());
            while out.len() < ids.len() {
                let part = &ids[out.len()..ids.len().min(out.len() + chunk)];
                c.request(Op::StatsSeries, |f| {
                    f.u32(part.len() as u32).u32(if resident { STATS_RESIDENT } else { 0 }).sids(part);
                })?;
                let mut r = Cursor::new(&c.body);
                let done = r.u32()? as usize;
                if r.u32()? != 0 || done == 0 || done > part.len() || r.remaining() != done * SeriesStat::WIRE_LEN {
                    return Err(Error::Protocol(format!("STATS_SERIES reply: {done} of {} records", part.len())));
                }
                for _ in 0..done {
                    let rec = SeriesStat::from_wire(r.take(SeriesStat::WIRE_LEN)?)
                        .ok_or_else(|| Error::Protocol("STATS_SERIES record out of shape".into()))?;
                    out.push(rec);
                }
            }
            Ok(out)
        })
    }

    /// The numbers of tenants: `prefixes` names the tenants asked for ("" is the tenant "(none)"); none asks for
    /// every tenant the user may see. A superuser, or a user with `read` on every series, sees every tenant;
    /// a tenant admin (`admin` on `prefix:Q`) each tenant whose prefix starts with `Q`. The call follows the
    /// server's cursor, so the answer holds every row.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] for more than 65,535 prefixes, a prefix over 65,535 bytes or `hot` above 256;
    /// [`Error::Server`] with [`Status::Invalid`], without sending anything, for a server below protocol 15 and
    /// for one that runs with `--no-stats`. A user who may see no tenant gets no rows when it names none, and a row with
    /// [`Status::Denied`] for each tenant it names.
    pub fn tenant_stats(&mut self, prefixes: &[&str], opts: &TenantOpts) -> Result<TenantStats> {
        if prefixes.len() > 65_535 || opts.hot as u32 > TSTATS_MAX_K || prefixes.iter().any(|p| p.len() > 65_535) {
            return Err(Error::InvalidArgument("too many prefixes, a prefix too long, or hot above 256".into()));
        }
        self.run(|c| {
            c.stats_version()?;
            let flags = (if opts.volumes { TSTATS_VOLUMES } else { 0 })
                | (if opts.hot > 0 { TSTATS_HOT } else { 0 })
                | (if opts.caller { TSTATS_CALLER } else { 0 });
            let mut names = Vec::new();
            for p in prefixes {
                names.extend_from_slice(&(p.len() as u16).to_le_bytes());
                names.extend_from_slice(p.as_bytes());
            }
            let (mut cursor, mut restarts) = (0u32, 0u32);
            let mut out = TenantStats::default();
            loop {
                c.request(Op::StatsTenants, |f| {
                    f.u16(flags).u16(opts.hot).u32(prefixes.len() as u32).u32(cursor).u32(0).bytes(&names);
                })?;
                let mut r = Cursor::new(&c.body);
                let (count, rflags, next, generation) = (r.u32()?, r.u32()?, r.u32()?, r.u32()?);
                let (hash, counted, listed) = (r.u64()?, r.u64()?, r.u64()?);
                if cursor != 0 && (generation != out.generation || hash != out.list_hash) {
                    // the tenant list changed between two replies: a cursor into the old list means nothing
                    restarts += 1;
                    if restarts > TENANT_RESTARTS {
                        return Err(Error::Io(io::Error::other("the tenant list kept changing")));
                    }
                    out = TenantStats::default();
                    cursor = 0;
                    continue;
                }
                out.partial |= rflags & 1 != 0;
                out.ingest_approx |= rflags & 2 != 0;
                out.generation = generation;
                out.list_hash = hash;
                out.parts_counted = counted;
                out.parts_listed = listed;
                for _ in 0..count {
                    out.rows.push(tenant_row(&mut r, opts)?);
                }
                if r.remaining() != 0 {
                    return Err(Error::Protocol("STATS_TENANTS reply with trailing bytes".into()));
                }
                if next == TSTATS_DONE {
                    return Ok(out);
                }
                if next <= cursor || count == 0 {
                    return Err(Error::Protocol("STATS_TENANTS cursor does not advance".into()));
                }
                cursor = next;
            }
        })
    }

    /// The server must speak 15: an older one does not know the ops, and nothing is sent to it.
    fn stats_version(&mut self) -> Result<()> {
        let v = match self.server_version {
            Some(v) => v,
            None => self.hello()?,
        };
        if v < 15 {
            return Err(Error::Server(Status::Invalid));
        }
        Ok(())
    }
}

fn tenant_row(r: &mut Cursor, opts: &TenantOpts) -> Result<TenantRow> {
    let status = r.u32()? as i32;
    let volume = r.u32()?;
    let rflags = r.u32()?;
    let (plen, nhot) = (r.u16()? as usize, r.u16()? as usize);
    debug_assert_eq!(TSTATS_ROW_HEAD, 16);
    let sum = volume == TSTATS_VOLUME_SUM;
    if !sum && nhot != 0 {
        return Err(Error::Protocol("STATS_TENANTS: hot entries on a volume row".into()));
    }
    let padded = (plen + 7) & !7;
    let prefix = String::from_utf8_lossy(&r.take(padded)?[..plen]).into_owned();
    let mut d = [0u64; TSTATS_DATA_FIELDS];
    for x in d.iter_mut() {
        *x = r.u64()?;
    }
    let numbers = TenantNumbers {
        series: d[0],
        series_created: d[1],
        active_series_cur: d[2],
        active_series_prev: d[3],
        rows: d[4],
        nulls: d[5],
        logical_bytes: d[6],
        ts_bytes: d[7],
        value_bytes: d[8],
        index_bytes: d[9],
        granules: d[10],
        ingested_rows: d[11],
        ingested_bytes: d[12],
        merged_rows: d[13],
        merge_removed_rows: d[14],
        retention_dropped_rows: d[15],
        resident_bytes: d[16],
        stale_s: d[17],
    };
    let caller = if sum && opts.caller {
        let mut c = [0u64; TSTATS_CALLER_FIELDS];
        for x in c.iter_mut() {
            *x = r.u64()?;
        }
        Some(TenantCaller {
            requests_query: c[0],
            requests_insert: c[1],
            requests_delete: c[2],
            requests_admin: c[3],
            requests_other: c[4],
            rows_decoded: c[5],
            rows_from_stats: c[6],
            rows_from_memtable: c[7],
            bytes_read: c[8],
            result_rows: c[9],
            query_us: c[10],
            inserted_rows: c[11],
            inserted_bytes: c[12],
            errors_invalid: c[13],
            errors_notfound: c[14],
            errors_io: c[15],
            errors_other: c[16],
            denied_requests: c[17],
            denied_series: c[18],
            failed_logins: c[19],
        })
    } else {
        None
    };
    let mut hot = Vec::with_capacity(nhot);
    for _ in 0..nhot {
        let b = r.take(TSTATS_HOT_REC)?;
        let kind = HotKind::from_code(b[0]).ok_or_else(|| Error::Protocol("STATS_TENANTS: hot kind".into()))?;
        let u = |o: usize| u64::from_le_bytes(b[o..o + 8].try_into().unwrap());
        hot.push(Hot { kind, id: u(8), estimate: u(16), error: u(24) });
    }
    Ok(TenantRow {
        status: if status == 0 { None } else { Some(Status::from_code(status)) },
        volume: if sum { None } else { Some(volume) },
        prefix,
        partial: rflags & 1 != 0,
        numbers,
        caller,
        hot,
    })
}
