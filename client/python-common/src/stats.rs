// SPDX-License-Identifier: Apache-2.0
//! `series_stats()` and `tenant_stats()` of Client, Pool and Db (protocol version 15): the storage numbers of
//! series and the numbers of tenants, as plain dicts and lists (see `tsdb_client.pyi` for their keys).

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use ::tsdb_client as tc;
use tc::{SeriesStat, TenantOpts, TenantStats};

use crate::TsdbError;

/// The calls a stats request runs on: a Pool or the embedded Db (a wrapper of it in `tsdb_embedded`).
pub trait StatsTarget {
    fn series_stats(&self, ids: &[u64], resident: bool) -> tc::Result<Vec<SeriesStat>>;
    fn tenant_stats(&self, prefixes: &[&str], opts: &TenantOpts) -> tc::Result<TenantStats>;
}

impl StatsTarget for tc::Pool {
    fn series_stats(&self, ids: &[u64], resident: bool) -> tc::Result<Vec<SeriesStat>> {
        tc::Pool::series_stats(self, ids, resident)
    }
    fn tenant_stats(&self, prefixes: &[&str], opts: &TenantOpts) -> tc::Result<TenantStats> {
        tc::Pool::tenant_stats(self, prefixes, opts)
    }
}

/// What a `series_stats` call asks for.
pub struct SeriesReq {
    pub ids: Vec<u64>,
    pub resident: bool,
    raise: bool,
}

impl SeriesReq {
    pub fn new(series: &Bound<'_, PyAny>, resident: bool, errors: &str) -> PyResult<SeriesReq> {
        let raise = match errors {
            "raise" => true,
            "status" => false,
            e => return Err(PyValueError::new_err(format!("errors must be 'raise' or 'status', not {e:?}"))),
        };
        Ok(SeriesReq { ids: crate::series_column(series)?.snap().into_owned(), resident, raise })
    }

    pub fn run_client(&self, c: &mut tc::Client) -> tc::Result<Vec<SeriesStat>> {
        c.series_stats(&self.ids, self.resident)
    }

    pub fn run<T: StatsTarget + ?Sized>(&self, t: &T) -> tc::Result<Vec<SeriesStat>> {
        t.series_stats(&self.ids, self.resident)
    }

    /// The list of dicts; with errors="raise" a series whose status is not 0 raises TsdbError instead (`.index`, `.code`).
    pub fn finish<'py>(&self, py: Python<'py>, stats: Vec<SeriesStat>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty(py);
        for (i, s) in stats.iter().enumerate() {
            if let (true, Some(st)) = (self.raise, s.status) {
                let what = match st {
                    tc::Status::NotFound => "not found",
                    tc::Status::Denied => "denied",
                    _ => "failed",
                };
                let err = TsdbError::new_err(format!("series {} (index {i}): {what}", self.ids[i]));
                let v = err.value(py);
                return Err(match v.setattr("code", st.code()).and_then(|()| v.setattr("index", i)) {
                    Ok(()) => err,
                    Err(e) => e,
                });
            }
            list.append(series_dict(py, self.ids[i], s)?)?;
        }
        Ok(list)
    }
}

fn series_dict<'py>(py: Python<'py>, id: u64, s: &SeriesStat) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("series", id)?;
    d.set_item("status", s.status.map_or(0, tc::Status::code))?;
    d.set_item("cls", s.class)?;
    d.set_item("type", s.ty.map(|t| t.to_string()))?;
    d.set_item("flags", s.flags)?;
    for (k, v) in [
        ("rows", s.rows),
        ("nulls", s.nulls),
        ("memtable_rows", s.memtable_rows),
        ("logical_bytes", s.logical_bytes),
        ("ts_bytes", s.ts_bytes),
        ("value_bytes", s.value_bytes),
        ("index_bytes", s.index_bytes),
        ("tomb_bytes", s.tomb_bytes),
        ("parts", s.parts),
        ("partitions", s.partitions),
        ("granules", s.granules),
        ("full_granules", s.full_granules),
    ] {
        d.set_item(k, v)?;
    }
    for (k, v) in [("first_ts", s.first_ts), ("last_ts", s.last_ts), ("last_write_ms", s.last_write_ms)] {
        d.set_item(k, v)?;
    }
    for (k, v) in [
        ("resident_bytes", s.resident_bytes),
        ("reads_est", s.reads_est),
        ("reads_err", s.reads_err),
        ("writes_est", s.writes_est),
        ("writes_err", s.writes_err),
        ("live_rows", s.live_rows),
    ] {
        d.set_item(k, v)?;
    }
    Ok(d)
}

/// What a `tenant_stats` call asks for.
pub struct TenantReq {
    pub prefixes: Vec<String>,
    pub opts: TenantOpts,
}

impl TenantReq {
    pub fn new(prefixes: Option<Vec<String>>, volumes: bool, hot: u16, caller: bool) -> PyResult<TenantReq> {
        if hot > 256 {
            return Err(PyValueError::new_err("hot is at most 256"));
        }
        Ok(TenantReq { prefixes: prefixes.unwrap_or_default(), opts: TenantOpts { volumes, hot, caller } })
    }

    pub fn run_client(&self, c: &mut tc::Client) -> tc::Result<TenantStats> {
        let v: Vec<&str> = self.prefixes.iter().map(String::as_str).collect();
        c.tenant_stats(&v, &self.opts)
    }

    pub fn run<T: StatsTarget + ?Sized>(&self, t: &T) -> tc::Result<TenantStats> {
        let v: Vec<&str> = self.prefixes.iter().map(String::as_str).collect();
        t.tenant_stats(&v, &self.opts)
    }

    /// A dict: the list's `generation` and `list_hash`, flags, the counted and listed parts and `rows`.
    pub fn finish<'py>(&self, py: Python<'py>, t: TenantStats) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new(py);
        d.set_item("partial", t.partial)?;
        d.set_item("ingest_approx", t.ingest_approx)?;
        d.set_item("generation", t.generation)?;
        d.set_item("list_hash", t.list_hash)?;
        d.set_item("parts_counted", t.parts_counted)?;
        d.set_item("parts_listed", t.parts_listed)?;
        let rows = PyList::empty(py);
        for r in &t.rows {
            let row = PyDict::new(py);
            row.set_item("status", r.status.map_or(0, tc::Status::code))?;
            row.set_item("volume", r.volume)?;
            row.set_item("prefix", &r.prefix)?;
            row.set_item("partial", r.partial)?;
            let n = &r.numbers;
            for (k, v) in [
                ("series", n.series),
                ("series_created", n.series_created),
                ("active_series_cur", n.active_series_cur),
                ("active_series_prev", n.active_series_prev),
                ("rows", n.rows),
                ("nulls", n.nulls),
                ("logical_bytes", n.logical_bytes),
                ("ts_bytes", n.ts_bytes),
                ("value_bytes", n.value_bytes),
                ("index_bytes", n.index_bytes),
                ("granules", n.granules),
                ("ingested_rows", n.ingested_rows),
                ("ingested_bytes", n.ingested_bytes),
                ("merged_rows", n.merged_rows),
                ("merge_removed_rows", n.merge_removed_rows),
                ("retention_dropped_rows", n.retention_dropped_rows),
                ("resident_bytes", n.resident_bytes),
                ("stale_s", n.stale_s),
            ] {
                row.set_item(k, v)?;
            }
            match &r.caller {
                Some(c) => {
                    let cd = PyDict::new(py);
                    for (k, v) in [
                        ("requests_query", c.requests_query),
                        ("requests_insert", c.requests_insert),
                        ("requests_delete", c.requests_delete),
                        ("requests_admin", c.requests_admin),
                        ("requests_other", c.requests_other),
                        ("rows_decoded", c.rows_decoded),
                        ("rows_from_stats", c.rows_from_stats),
                        ("rows_from_memtable", c.rows_from_memtable),
                        ("bytes_read", c.bytes_read),
                        ("result_rows", c.result_rows),
                        ("query_us", c.query_us),
                        ("inserted_rows", c.inserted_rows),
                        ("inserted_bytes", c.inserted_bytes),
                        ("errors_invalid", c.errors_invalid),
                        ("errors_notfound", c.errors_notfound),
                        ("errors_io", c.errors_io),
                        ("errors_other", c.errors_other),
                        ("denied_requests", c.denied_requests),
                        ("denied_series", c.denied_series),
                        ("failed_logins", c.failed_logins),
                    ] {
                        cd.set_item(k, v)?;
                    }
                    row.set_item("caller", cd)?;
                }
                None => row.set_item("caller", py.None())?,
            }
            let hot = PyList::empty(py);
            for h in &r.hot {
                let hd = PyDict::new(py);
                hd.set_item(
                    "kind",
                    match h.kind {
                        tc::HotKind::Reads => "reads",
                        tc::HotKind::Writes => "writes",
                        tc::HotKind::BytesPerRow => "bytes_per_row",
                    },
                )?;
                hd.set_item("series", h.id)?;
                hd.set_item("estimate", h.estimate)?;
                hd.set_item("error", h.error)?;
                hot.append(hd)?;
            }
            row.set_item("hot", hot)?;
            rows.append(row)?;
        }
        d.set_item("rows", rows)?;
        Ok(d)
    }
}
