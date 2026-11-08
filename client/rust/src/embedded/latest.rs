//! `tsdb_latest` on an embedded [`Db`]: the same answers as the TCP client's `latest`.

use super::*;
use crate::latest::{Latest, LatestOpts};

const MAX_IDS: usize = 1 << 20;

impl Db {
    /// The newest value of each series, as [`Client::latest`](crate::Client::latest) (without the `Denied`
    /// answer: an embedded handle checks no rights). `out[i]` answers `ids[i]`.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] for more than 1,048,576 ids or `ends` of another length.
    pub fn latest(&self, ids: &[SeriesId], opts: &LatestOpts) -> Result<Vec<Latest>> {
        if ids.len() > MAX_IDS {
            return Err(Error::InvalidArgument(format!("{} ids (at most {MAX_IDS})", ids.len())));
        }
        if let Some(e) = &opts.ends {
            if e.len() != ids.len() {
                return Err(Error::InvalidArgument(format!("{} ends for {} series", e.len(), ids.len())));
            }
            if opts.end != i64::MAX {
                return Err(Error::InvalidArgument("both end and ends given".into()));
            }
        }
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let c_opts = sys::tsdb_latest_opts {
            end: opts.end,
            ends: opts.ends.as_ref().map_or(ptr::null(), |e| e.as_ptr()),
            with_nulls: opts.with_nulls as c_int,
            threads: c_int::from(opts.threads),
        };
        let mut raw = vec![sys::tsdb_latest_result::default(); ids.len()];
        check(unsafe { sys::tsdb_latest(self.p(), ids.as_ptr(), ids.len(), &c_opts, raw.as_mut_ptr()) })?;
        raw.iter()
            .map(|r| {
                Latest::from_wire(r.status, r.kind, r.type_, r.ts, r.v)
                    .ok_or_else(|| Error::Protocol("tsdb_latest answered out of shape".into()))
            })
            .collect()
    }

    /// [`latest`](Db::latest) by name; a name that does not exist answers `Status::NotFound` in its entry.
    pub fn latest_by_name(&self, names: &[&str], opts: &LatestOpts) -> Result<Vec<Latest>> {
        if names.len() > MAX_IDS {
            return Err(Error::InvalidArgument(format!("{} names (at most {MAX_IDS})", names.len())));
        }
        if let Some(e) = &opts.ends {
            if e.len() != names.len() {
                return Err(Error::InvalidArgument(format!("{} ends for {} names", e.len(), names.len())));
            }
        }
        let mut out: Vec<Option<Latest>> = vec![None; names.len()];
        let (mut ids, mut at) = (Vec::new(), Vec::new());
        for (i, name) in names.iter().enumerate() {
            match self.series_lookup(name) {
                Ok(id) => {
                    ids.push(id);
                    at.push(i);
                }
                Err(Error::Server(s @ Status::NotFound)) => out[i] = Some(Latest::missing(s)),
                Err(Error::InvalidArgument(_)) => out[i] = Some(Latest::missing(Status::Invalid)),
                Err(e) => return Err(e),
            }
        }
        let sub = LatestOpts { ends: opts.ends.as_ref().map(|e| at.iter().map(|&i| e[i]).collect()), ..opts.clone() };
        for (r, i) in self.latest(&ids, &sub)?.into_iter().zip(at) {
            out[i] = Some(r);
        }
        Ok(out.into_iter().map(|r| r.expect("every name answered")).collect())
    }
}
