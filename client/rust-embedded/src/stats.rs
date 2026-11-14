// SPDX-License-Identifier: AGPL-3.0-only
//! Statistics on an embedded [`Db`] (`tsdb_series_stats`, `tsdb_tenant_stats_get`, `tsdb_hot_series`): the same
//! answers as the TCP client's `series_stats` and `tenant_stats` for a user who may see everything (an embedded
//! handle checks no rights), so the two can be compared.

use super::*;
use tsdb_client::{Hot, HotKind, SeriesStat, TenantNumbers, TenantOpts, TenantRow, TenantStats};

const MAX_HOT: usize = 256;

impl Db {
    /// The tenant list (`tsdb_set_tenants`): series whose name starts with one of the prefixes belong to the
    /// tenant with the longest such prefix, the rest to "(none)". Empty strings and duplicates are dropped.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] for a prefix with a NUL byte; [`Status::Invalid`] with `stats: -1`.
    pub fn set_tenants(&self, prefixes: &[&str]) -> Result<()> {
        let owned: Vec<CString> = prefixes.iter().map(|p| c_string("tenant prefix", p)).collect::<Result<_>>()?;
        let ptrs: Vec<*const c_char> = owned.iter().map(|c| c.as_ptr()).collect();
        check(unsafe { sys::tsdb_set_tenants(self.p(), ptrs.as_ptr(), ptrs.len()) })
    }

    /// Waits until the statistics sweeper has handled the parts listed before the call, for at most `timeout_ms`
    /// (`tsdb_stats_settle`); `Status::Unavailable` when time ran out.
    pub fn stats_settle(&self, timeout_ms: i32) -> Result<()> {
        check(unsafe { sys::tsdb_stats_settle(self.p(), timeout_ms) })
    }

    /// The storage numbers of each series, as [`Client::series_stats`](crate::Client::series_stats) (without the
    /// `Denied` answer). `out[i]` answers `ids[i]`.
    ///
    /// # Errors
    ///
    /// [`Status::Invalid`] with `stats: -1`.
    pub fn series_stats(&self, ids: &[SeriesId], resident: bool) -> Result<Vec<SeriesStat>> {
        let chunk = if resident { 1024 } else { 65_536 };
        let mut out = Vec::with_capacity(ids.len());
        while out.len() < ids.len() {
            let part = &ids[out.len()..ids.len().min(out.len() + chunk)];
            let mut raw = vec![sys::tsdb_series_stat::default(); part.len()];
            let mut done = 0usize;
            let flags = if resident { sys::TSDB_SSTAT_REQ_RESIDENT } else { 0 };
            check(unsafe {
                sys::tsdb_series_stats(self.p(), part.as_ptr(), part.len(), flags, raw.as_mut_ptr(), &mut done)
            })?;
            if done == 0 || done > part.len() {
                return Err(Error::Protocol("tsdb_series_stats answered nothing".into()));
            }
            for r in &raw[..done] {
                // the record is the wire record: 176 bytes, the same fields in the same order (layout.rs)
                let bytes = unsafe {
                    std::slice::from_raw_parts((r as *const sys::tsdb_series_stat).cast::<u8>(), SeriesStat::WIRE_LEN)
                };
                out.push(
                    SeriesStat::from_wire(bytes)
                        .ok_or_else(|| Error::Protocol("tsdb_series_stats answered out of shape".into()))?,
                );
            }
        }
        Ok(out)
    }

    fn tenant_list(&self) -> Vec<String> {
        let mut v = Vec::new();
        let mut buf = vec![0 as c_char; 4096];
        while let Some(len) = {
            let rc = unsafe { sys::tsdb_tenant_prefix(self.p(), v.len(), buf.as_mut_ptr(), buf.len()) };
            (rc >= 0).then_some(rc as usize)
        } {
            if len >= buf.len() {
                buf = vec![0 as c_char; len + 1];
                continue;
            }
            let bytes: Vec<u8> = buf[..len].iter().map(|&c| c as u8).collect();
            v.push(String::from_utf8_lossy(&bytes).into_owned());
        }
        v
    }

    /// The numbers of tenants, as [`Client::tenant_stats`](crate::Client::tenant_stats) answers a user who may see
    /// every tenant: `prefixes` empty is every tenant, "(none)" first, then the list in its order; the hot lists
    /// are filtered to the tenant's series and end in the threshold (id 0). The caller counters are `None`
    /// (nothing counts requests here), and `parts_counted` and `parts_listed` are 0.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] for `hot` above 256; [`Status::Invalid`] with `stats: -1`.
    pub fn tenant_stats(&self, prefixes: &[&str], opts: &TenantOpts) -> Result<TenantStats> {
        if opts.hot as usize > MAX_HOT {
            return Err(Error::InvalidArgument("hot above 256".into()));
        }
        let list = self.tenant_list();
        let mut none = sys::tsdb_tenant_stats::default();
        check(unsafe { sys::tsdb_tenant_stats_get(self.p(), ptr::null(), sys::TSDB_TENANT_VOLUME_SUM, &mut none) })?;
        let names: Vec<String> = if prefixes.is_empty() {
            std::iter::once(String::new()).chain(list.iter().cloned()).collect()
        } else {
            prefixes.iter().map(|p| p.to_string()).collect()
        };
        // the hot lists, once: three kinds of up to 256 entries and their thresholds
        let mut hot_lists: Vec<(Vec<(SeriesId, u64, u64)>, u64)> = Vec::new();
        if opts.hot > 0 {
            for kind in 1..=3 {
                let mut raw = vec![sys::tsdb_hot::default(); MAX_HOT];
                let (mut n, mut thr) = (0usize, 0u64);
                check(unsafe { sys::tsdb_hot_series(self.p(), kind, raw.as_mut_ptr(), MAX_HOT, &mut n, &mut thr) })?;
                hot_lists.push((raw[..n].iter().map(|h| (h.id, h.estimate, h.error)).collect(), thr));
            }
        }
        let tenant_of = |name: &str| -> &str {
            list.iter()
                .filter(|p| !p.is_empty() && name.starts_with(p.as_str()))
                .max_by_key(|p| p.len())
                .map_or("", |p| p)
        };
        let nv = unsafe { sys::tsdb_volume_count(self.p()) };
        let mut out = TenantStats { generation: none.generation, list_hash: none.list_hash, ..TenantStats::default() };
        for name in &names {
            let cname = CString::new(name.as_str())
                .map_err(|_| Error::InvalidArgument("tenant prefix with a NUL byte".into()))?;
            let get = |volume: u32| -> std::result::Result<sys::tsdb_tenant_stats, i32> {
                let mut t = sys::tsdb_tenant_stats::default();
                let p = if name.is_empty() { ptr::null() } else { cname.as_ptr() };
                match unsafe { sys::tsdb_tenant_stats_get(self.p(), p, volume, &mut t) } {
                    0 => Ok(t),
                    rc => Err(rc),
                }
            };
            let row = |volume: Option<u32>, t: std::result::Result<sys::tsdb_tenant_stats, i32>| {
                let (status, t) = match t {
                    Ok(t) => (None, t),
                    // as tsdbd: NOTFOUND for a prefix not in the list, INVALID for anything else
                    Err(rc) if rc == Status::NotFound.code() => (Some(Status::NotFound), Default::default()),
                    Err(_) => (Some(Status::Invalid), Default::default()),
                };
                TenantRow {
                    status,
                    volume,
                    prefix: name.clone(),
                    partial: t.flags & sys::TSDB_TENANT_PARTIAL != 0,
                    numbers: numbers(&t),
                    caller: None,
                    hot: Vec::new(),
                }
            };
            let mut sum = row(None, get(sys::TSDB_TENANT_VOLUME_SUM));
            if sum.status.is_none() {
                out.partial |= sum.partial;
                if opts.hot > 0 {
                    for (i, (list_k, thr)) in hot_lists.iter().enumerate() {
                        let kind = HotKind::from_code(i as u8 + 1).expect("kind 1 to 3");
                        let mut m = 0;
                        for &(id, estimate, error) in list_k {
                            if m == opts.hot {
                                break;
                            }
                            let Ok(n) = self.series_name(id) else { continue };
                            if tenant_of(&n) == name {
                                sum.hot.push(Hot { kind, id, estimate, error });
                                m += 1;
                            }
                        }
                        sum.hot.push(Hot { kind, id: 0, estimate: *thr, error: 0 });
                    }
                }
            }
            let ok = sum.status.is_none();
            out.rows.push(sum);
            if ok && opts.volumes {
                for v in 0..nv {
                    out.rows.push(row(Some(v), get(v)));
                }
            }
        }
        Ok(out)
    }
}

fn numbers(t: &sys::tsdb_tenant_stats) -> TenantNumbers {
    TenantNumbers {
        series: t.series,
        series_created: t.series_created,
        active_series_cur: t.active_series_cur,
        active_series_prev: t.active_series_prev,
        rows: t.rows,
        nulls: t.nulls,
        logical_bytes: t.logical_bytes,
        ts_bytes: t.ts_bytes,
        value_bytes: t.value_bytes,
        index_bytes: t.index_bytes,
        granules: t.granules,
        ingested_rows: t.ingested_rows,
        ingested_bytes: t.ingested_bytes,
        merged_rows: t.merged_rows,
        merge_removed_rows: t.merge_removed_rows,
        retention_dropped_rows: t.retention_dropped_rows,
        resident_bytes: t.resident_bytes,
        stale_s: t.stale_s,
    }
}
