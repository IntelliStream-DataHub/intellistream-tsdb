//! Many series in one call on an embedded [`Db`]: `tsdb_query_multi`, with the results of the TCP
//! client's `*_multi` calls (the engine handles every time zone itself, so there are no probes).

use super::*;
use crate::types::{Buckets, MultiOpts};

/// The bucket types of the multi calls, with their engine structs.
pub(super) trait EngineRow: Sized {
    const KIND: sys::tsdb_multi_kind;
    type C;
    fn conv(c: &Self::C) -> Self;
}

impl EngineRow for Bucket {
    const KIND: sys::tsdb_multi_kind = sys::TSDB_MULTI_BUCKETS;
    type C = sys::tsdb_bucket;
    fn conv(c: &Self::C) -> Self {
        bucket(c)
    }
}

impl EngineRow for SpreadBucket {
    const KIND: sys::tsdb_multi_kind = sys::TSDB_MULTI_SPREAD;
    type C = sys::tsdb_spread_bucket;
    fn conv(c: &Self::C) -> Self {
        spread_bucket(c)
    }
}

impl EngineRow for TwaBucket {
    const KIND: sys::tsdb_multi_kind = sys::TSDB_MULTI_TWA;
    type C = sys::tsdb_twa_bucket;
    fn conv(c: &Self::C) -> Self {
        twa_bucket(c)
    }
}

unsafe extern "C" fn multi_fn<F>(ctx: *mut c_void, r: *const sys::tsdb_series_result) -> c_int
where
    F: FnMut(&sys::tsdb_series_result) -> ControlFlow<()>,
{
    let call = unsafe { &mut *ctx.cast::<Call<F>>() };
    let r = unsafe { &*r };
    call.run(|f| f(r))
}

fn opt_ptr(a: Option<&[i64]>) -> *const i64 {
    a.map_or(ptr::null(), |a| a.as_ptr())
}

/// The per-series arrays of `m` must be as long as the series list (as the TCP client checks).
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

impl Db {
    /// The rows of many series, as [`Client::query_rows_multi`](crate::Client::query_rows_multi)
    /// (`tsdb_query_multi`): one call on up to `m.threads` threads of the database's query
    /// scheduler, every series as of the same instant of each volume.
    pub fn query_rows_multi(
        &self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        opts: QueryOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Rows>>> {
        let (start, end) = bounds(&range);
        self.rows_multi(sys::TSDB_MULTI_VALUES, series, start, end, 0, opts, m)
    }

    /// LTTB of many series, as [`Client::query_lttb_multi`](crate::Client::query_lttb_multi).
    pub fn query_lttb_multi(
        &self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        threshold: u64,
        opts: QueryOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Rows>>> {
        let (start, end) = bounds(&range);
        let threshold = usize::try_from(threshold).unwrap_or(usize::MAX);
        self.rows_multi(sys::TSDB_MULTI_LTTB, series, start, end, threshold, opts, m)
    }

    /// Streams the rows of many series, as [`Client::query_chunks_multi`](crate::Client::query_chunks_multi).
    pub fn query_chunks_multi<F>(
        &self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        opts: QueryOpts,
        m: MultiOpts<'_>,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(usize, Result<(&[i64], &Column)>) -> ControlFlow<()>,
    {
        let (start, end) = bounds(&range);
        check_multi(series, &m)?;
        // a limit of 0 still asks (for one row, dropped) so that the errors arrive, as from tsdbd
        let none = opts.limit == Some(0);
        let o = c_opts(if none { QueryOpts { limit: Some(1), ..opts } } else { opts }).expect("a limit above 0");
        if series.is_empty() {
            return Ok(());
        }
        let q = sys::tsdb_multi_query {
            kind: sys::TSDB_MULTI_VALUES,
            start,
            end,
            starts: opt_ptr(m.starts),
            ends: opt_ptr(m.ends),
            opts: o,
            ..Default::default()
        };
        let mut bad = None;
        self.multi_call(&q, series, &m, |r| {
            if r.status != 0 {
                return f(r.index, Err(Error::Server(Status::from_code(r.status))));
            }
            if r.n == 0 || none {
                return ControlFlow::Continue(());
            }
            let Some(chunk) = (unsafe { ChunkRef::from_c(&r.values, r.n) }) else {
                bad = Some(r.values.type_);
                return ControlFlow::Break(());
            };
            let ts = unsafe { slice(r.ts, r.n) };
            f(r.index, Ok((ts, &chunk.to_column())))
        })?;
        match bad {
            Some(t) => Err(Error::Protocol(format!("value type {t} from the engine"))),
            None => Ok(()),
        }
    }

    /// Aggregates of many series, as [`Client::query_buckets_multi`](crate::Client::query_buckets_multi).
    pub fn query_buckets_multi(
        &self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        b: Buckets<'_>,
        opts: QueryOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Vec<Bucket>>>> {
        let (start, end) = bounds(&range);
        self.bucket_multi(series, start, end, b, opts, TwaOpts::default(), m)
    }

    /// The spread of many series, as [`Client::query_spread_multi`](crate::Client::query_spread_multi).
    pub fn query_spread_multi(
        &self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        b: Buckets<'_>,
        opts: QueryOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Vec<SpreadBucket>>>> {
        let (start, end) = bounds(&range);
        self.bucket_multi(series, start, end, b, opts, TwaOpts::default(), m)
    }

    /// Time-weighted averages of many series, as [`Client::query_twa_multi`](crate::Client::query_twa_multi).
    pub fn query_twa_multi(
        &self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        b: Buckets<'_>,
        opts: TwaOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Vec<TwaBucket>>>> {
        let (start, end) = bounds(&range);
        self.bucket_multi(series, start, end, b, QueryOpts::default(), opts, m)
    }

    /// Runs `q` over `series` with `f` called for every result, then keeps the call's statistics.
    pub(crate) fn multi_call<F>(
        &self,
        q: &sys::tsdb_multi_query,
        series: &[SeriesId],
        m: &MultiOpts<'_>,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(&sys::tsdb_series_result) -> ControlFlow<()>,
    {
        let mo = sys::tsdb_multi_opts { threads: m.threads as c_int, max_rows: m.max_rows, ..Default::default() };
        let mut call = Call::new(f);
        let rc = unsafe {
            sys::tsdb_query_multi(
                self.p(),
                q,
                series.as_ptr(),
                series.len(),
                &mo,
                Some(multi_fn::<F>),
                (&raw mut call).cast(),
            )
        };
        self.record();
        call.finish(rc)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn rows_multi(
        &self,
        kind: sys::tsdb_multi_kind,
        series: &[SeriesId],
        start: i64,
        end: i64,
        threshold: usize,
        opts: QueryOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Rows>>> {
        check_multi(series, &m)?;
        // a limit of 0 still asks (for one row, dropped) so that the type and the errors match tsdbd's
        let none = opts.limit == Some(0);
        let o = c_opts(if none { QueryOpts { limit: Some(1), ..opts } } else { opts }).expect("a limit above 0");
        if series.is_empty() {
            return Ok(Vec::new());
        }
        let q = sys::tsdb_multi_query {
            kind,
            start,
            end,
            starts: opt_ptr(m.starts),
            ends: opt_ptr(m.ends),
            opts: o,
            threshold,
            ..Default::default()
        };
        let mut out: Vec<Result<Rows>> = Vec::with_capacity(series.len());
        let mut cur: Option<Rows> = None;
        let mut bad = None;
        self.multi_call(&q, series, &m, |r| {
            let Some(chunk) = (unsafe { ChunkRef::from_c(&r.values, r.n) }) else {
                bad = Some(r.values.type_);
                return ControlFlow::Break(());
            };
            let rows = cur.get_or_insert_with(|| Rows { ts: Vec::new(), values: Column::empty(chunk.value_type()) });
            if !none && r.n > 0 {
                rows.ts.extend_from_slice(unsafe { slice(r.ts, r.n) });
                chunk.append_to(&mut rows.values);
            }
            if r.last != 0 {
                let rows = cur.take().expect("set above");
                out.push(if r.status != 0 { Err(Error::Server(Status::from_code(r.status))) } else { Ok(rows) });
            }
            ControlFlow::Continue(())
        })?;
        if let Some(t) = bad {
            return Err(Error::Protocol(format!("value type {t} from the engine")));
        }
        // a series without rows is typed as the dictionary has it (an unknown one float32), as query_rows does
        for (r, &id) in out.iter_mut().zip(series) {
            if let Ok(rows) = r
                && rows.is_empty()
                && let Ok(t) = self.series_type(id)
            {
                rows.values = Column::empty(t);
            }
        }
        complete(out, series.len())
    }

    #[allow(clippy::too_many_arguments)]
    fn bucket_multi<T: EngineRow>(
        &self,
        series: &[SeriesId],
        start: i64,
        end: i64,
        b: Buckets<'_>,
        opts: QueryOpts,
        twa: TwaOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Vec<T>>>> {
        check_multi(series, &m)?;
        let mut q = sys::tsdb_multi_query {
            kind: T::KIND,
            start,
            end,
            starts: opt_ptr(m.starts),
            ends: opt_ptr(m.ends),
            ..Default::default()
        };
        let cg;
        match b {
            Buckets::One => {}
            Buckets::Every(i) if i < 0 => return Err(Error::Server(Status::Invalid)),
            Buckets::Every(i) => q.interval_ms = i,
            Buckets::Intervals(iv) => {
                if iv.len() != series.len() {
                    return Err(Error::InvalidArgument(format!("{} intervals for {} series", iv.len(), series.len())));
                }
                q.intervals = iv.as_ptr();
            }
            Buckets::Group(g) => {
                cg = CGroup::new(Some(g))?; // an unknown zone is an error even without rows
                q.group = cg.ptr();
            }
            Buckets::Bounds(bd) => {
                check_bounds(bd)?;
                q.bounds = bd.as_ptr();
                q.nbounds = bd.len();
            }
        }
        let limited = if T::KIND == sys::TSDB_MULTI_TWA {
            c_twa_opts(twa).map(|o| q.twa = o)
        } else {
            c_opts(opts).map(|o| q.opts = o)
        };
        if series.is_empty() || limited.is_none() {
            // nothing asked, but a negative interval is still the series' error, as from the single call
            return Ok((0..series.len()).map(|i| nothing(&b, i)).collect());
        }
        let mut out: Vec<Result<Vec<T>>> = Vec::with_capacity(series.len());
        self.multi_call(&q, series, &m, |r| {
            out.push(if r.status != 0 {
                Err(Error::Server(Status::from_code(r.status)))
            } else {
                Ok(unsafe { slice(r.buckets.cast::<T::C>(), r.n) }.iter().map(T::conv).collect())
            });
            ControlFlow::Continue(())
        })?;
        complete(out, series.len())
    }
}

/// A result per series, or the engine was short of some.
fn complete<T>(out: Vec<T>, n: usize) -> Result<Vec<T>> {
    if out.len() != n {
        return Err(Error::Protocol(format!("{} of {n} series answered", out.len())));
    }
    Ok(out)
}

/// The result of series `i` when the limit asks for no buckets.
fn nothing<T>(b: &Buckets<'_>, i: usize) -> Result<Vec<T>> {
    match b {
        Buckets::Intervals(iv) if iv[i] < 0 => Err(Error::Server(Status::Invalid)),
        _ => Ok(Vec::new()),
    }
}
