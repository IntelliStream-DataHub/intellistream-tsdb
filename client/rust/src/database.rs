// SPDX-License-Identifier: Apache-2.0
//! [`Database`]: the operations a TCP [`Client`] and the embedded `Db` (another crate) share, so code can be
//! written once for both. The trait only forwards to the inherent methods, which keep their own
//! documentation; it takes `&mut self` because a `Client` needs it (a `Db` does not).

use std::ops::{ControlFlow, RangeBounds};

use crate::SeriesId;
use crate::client::Client;
use crate::error::Result;
use crate::latest::{Latest, LatestOpts};
use crate::types::{
    Bucket, Buckets, Group, MultiOpts, Points, QueryOpts, QueryStats, Rows, SpreadBucket, Stats, TwaBucket, TwaOpts,
};
use crate::value::{Column, ColumnRef, Value, ValueType};

/// What both a remote [`Client`] and the embedded `Db` (crate `intellistream-tsdb-embedded`) can do.
///
/// ```no_run
/// use tsdb_client::{Database, QueryOpts};
///
/// fn newest(db: &mut impl Database, name: &str) -> tsdb_client::Result<Option<(i64, f32)>> {
///     let id = db.series_lookup(name)?;
///     Ok(db.query_points(id, .., QueryOpts::desc().limit(1))?.iter().next())
/// }
/// let mut c = tsdb_client::Client::connect("127.0.0.1:7070")?;
/// println!("{:?}", newest(&mut c, "tank.level_bar")?);
/// # Ok::<(), tsdb_client::Error>(())
/// ```
#[allow(missing_docs)] // each method is documented on Client and Db
pub trait Database {
    fn series(&mut self, name: &str) -> Result<SeriesId>;
    fn series_lookup(&mut self, name: &str) -> Result<SeriesId>;
    fn series_at(&mut self, name: &str, path: &str) -> Result<SeriesId>;
    fn series_create(&mut self, name: &str, ty: ValueType, path: Option<&str>) -> Result<SeriesId>;
    fn series_type(&mut self, id: SeriesId) -> Result<ValueType>;
    fn series_name(&mut self, id: SeriesId) -> Result<String>;
    fn state(&mut self, series: SeriesId, name: &str) -> Result<u32>;
    fn state_lookup(&mut self, series: SeriesId, name: &str) -> Result<u32>;
    fn state_name(&mut self, series: SeriesId, code: u32) -> Result<String>;
    fn insert_series(&mut self, series: SeriesId, ts: &[i64], values: &[Value]) -> Result<()>;
    fn insert(&mut self, series: &[SeriesId], ts: &[i64], values: &[Value]) -> Result<()>;
    fn insert_series_values<'a>(
        &mut self,
        series: SeriesId,
        ts: &[i64],
        values: impl Into<ColumnRef<'a>>,
    ) -> Result<()>;
    fn insert_values<'a>(&mut self, series: &[SeriesId], ts: &[i64], values: impl Into<ColumnRef<'a>>) -> Result<()>;
    fn query_points(&mut self, series: SeriesId, range: impl RangeBounds<i64>, opts: QueryOpts) -> Result<Points>;
    fn query_raw<F>(&mut self, series: SeriesId, range: impl RangeBounds<i64>, opts: QueryOpts, f: F) -> Result<()>
    where
        F: FnMut(&[i64], &[Value]) -> ControlFlow<()>;
    fn query_rows(&mut self, series: SeriesId, range: impl RangeBounds<i64>, opts: QueryOpts) -> Result<Rows>;
    fn query_values<F>(&mut self, series: SeriesId, range: impl RangeBounds<i64>, opts: QueryOpts, f: F) -> Result<()>
    where
        F: FnMut(&[i64], &Column) -> ControlFlow<()>;
    fn query_agg(&mut self, series: SeriesId, range: impl RangeBounds<i64>, interval_ms: i64) -> Result<Vec<Bucket>>;
    fn query_group(
        &mut self,
        series: SeriesId,
        range: impl RangeBounds<i64>,
        group: Option<&Group>,
        opts: QueryOpts,
    ) -> Result<Vec<Bucket>>;
    fn query_bounds(
        &mut self,
        series: SeriesId,
        range: impl RangeBounds<i64>,
        bounds: &[i64],
        opts: QueryOpts,
    ) -> Result<Vec<Bucket>>;
    fn query_agg_ex(
        &mut self,
        series: SeriesId,
        range: impl RangeBounds<i64>,
        interval_ms: i64,
        opts: QueryOpts,
    ) -> Result<Vec<Bucket>>;
    fn query_spread(
        &mut self,
        series: SeriesId,
        range: impl RangeBounds<i64>,
        interval_ms: i64,
        opts: QueryOpts,
    ) -> Result<Vec<SpreadBucket>>;
    fn query_spread_group(
        &mut self,
        series: SeriesId,
        range: impl RangeBounds<i64>,
        group: Option<&Group>,
        opts: QueryOpts,
    ) -> Result<Vec<SpreadBucket>>;
    fn query_spread_bounds(
        &mut self,
        series: SeriesId,
        range: impl RangeBounds<i64>,
        bounds: &[i64],
        opts: QueryOpts,
    ) -> Result<Vec<SpreadBucket>>;
    fn query_twa(
        &mut self,
        series: SeriesId,
        range: impl RangeBounds<i64>,
        interval_ms: i64,
        opts: TwaOpts,
    ) -> Result<Vec<TwaBucket>>;
    fn query_twa_group(
        &mut self,
        series: SeriesId,
        range: impl RangeBounds<i64>,
        group: Option<&Group>,
        opts: TwaOpts,
    ) -> Result<Vec<TwaBucket>>;
    fn query_twa_bounds(
        &mut self,
        series: SeriesId,
        range: impl RangeBounds<i64>,
        bounds: &[i64],
        opts: TwaOpts,
    ) -> Result<Vec<TwaBucket>>;
    fn query_lttb(
        &mut self,
        series: SeriesId,
        range: impl RangeBounds<i64>,
        threshold: u64,
        opts: QueryOpts,
    ) -> Result<Rows>;
    fn query_rows_multi(
        &mut self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        opts: QueryOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Rows>>>;
    fn query_lttb_multi(
        &mut self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        threshold: u64,
        opts: QueryOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Rows>>>;
    fn query_chunks_multi<F>(
        &mut self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        opts: QueryOpts,
        m: MultiOpts<'_>,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(usize, Result<(&[i64], &Column)>) -> ControlFlow<()>;
    fn query_buckets_multi(
        &mut self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        b: Buckets<'_>,
        opts: QueryOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Vec<Bucket>>>>;
    fn query_spread_multi(
        &mut self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        b: Buckets<'_>,
        opts: QueryOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Vec<SpreadBucket>>>>;
    fn query_twa_multi(
        &mut self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        b: Buckets<'_>,
        opts: TwaOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Vec<TwaBucket>>>>;
    fn delete_range(&mut self, series: SeriesId, range: impl RangeBounds<i64>) -> Result<()>;
    fn replace_range<'a>(
        &mut self,
        series: SeriesId,
        range: impl RangeBounds<i64>,
        ts: &[i64],
        values: impl Into<ColumnRef<'a>>,
    ) -> Result<()>;
    fn set_retention(&mut self, series: SeriesId, limit_ms: i64) -> Result<()>;
    fn retention(&mut self, series: SeriesId) -> Result<i64>;
    fn commit(&mut self) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
    fn optimize(&mut self) -> Result<()>;
    fn stats(&mut self) -> Result<Stats>;
    fn last_query_stats(&mut self) -> Result<QueryStats>;
    fn latest(&mut self, ids: &[SeriesId], opts: &LatestOpts) -> Result<Vec<Latest>>;
    fn latest_by_name(&mut self, names: &[&str], opts: &LatestOpts) -> Result<Vec<Latest>>;
}

macro_rules! forward {
    ($t:ty, |$s:ident| $last_query_stats:expr) => {
        impl Database for $t {
            fn series(&mut self, name: &str) -> Result<SeriesId> {
                <$t>::series(self, name)
            }
            fn series_lookup(&mut self, name: &str) -> Result<SeriesId> {
                <$t>::series_lookup(self, name)
            }
            fn series_at(&mut self, name: &str, path: &str) -> Result<SeriesId> {
                <$t>::series_at(self, name, path)
            }
            fn series_create(&mut self, name: &str, ty: ValueType, path: Option<&str>) -> Result<SeriesId> {
                <$t>::series_create(self, name, ty, path)
            }
            fn series_type(&mut self, id: SeriesId) -> Result<ValueType> {
                <$t>::series_type(self, id)
            }
            fn series_name(&mut self, id: SeriesId) -> Result<String> {
                <$t>::series_name(self, id)
            }
            fn state(&mut self, series: SeriesId, name: &str) -> Result<u32> {
                <$t>::state(self, series, name)
            }
            fn state_lookup(&mut self, series: SeriesId, name: &str) -> Result<u32> {
                <$t>::state_lookup(self, series, name)
            }
            fn state_name(&mut self, series: SeriesId, code: u32) -> Result<String> {
                <$t>::state_name(self, series, code)
            }
            fn insert_series(&mut self, series: SeriesId, ts: &[i64], values: &[Value]) -> Result<()> {
                <$t>::insert_series(self, series, ts, values)
            }
            fn insert(&mut self, series: &[SeriesId], ts: &[i64], values: &[Value]) -> Result<()> {
                <$t>::insert(self, series, ts, values)
            }
            fn insert_series_values<'a>(
                &mut self,
                series: SeriesId,
                ts: &[i64],
                values: impl Into<ColumnRef<'a>>,
            ) -> Result<()> {
                <$t>::insert_series_values(self, series, ts, values)
            }
            fn insert_values<'a>(&mut self, series: &[SeriesId], ts: &[i64], values: impl Into<ColumnRef<'a>>) -> Result<()> {
                <$t>::insert_values(self, series, ts, values)
            }
            fn query_points(&mut self, series: SeriesId, range: impl RangeBounds<i64>, opts: QueryOpts) -> Result<Points> {
                <$t>::query_points(self, series, range, opts)
            }
            fn query_raw<F>(&mut self, series: SeriesId, range: impl RangeBounds<i64>, opts: QueryOpts, f: F) -> Result<()>
            where
                F: FnMut(&[i64], &[Value]) -> ControlFlow<()>,
            {
                <$t>::query_raw(self, series, range, opts, f)
            }
            fn query_rows(&mut self, series: SeriesId, range: impl RangeBounds<i64>, opts: QueryOpts) -> Result<Rows> {
                <$t>::query_rows(self, series, range, opts)
            }
            fn query_values<F>(&mut self, series: SeriesId, range: impl RangeBounds<i64>, opts: QueryOpts, f: F) -> Result<()>
            where
                F: FnMut(&[i64], &Column) -> ControlFlow<()>,
            {
                <$t>::query_values(self, series, range, opts, f)
            }
            fn query_agg(&mut self, series: SeriesId, range: impl RangeBounds<i64>, interval_ms: i64) -> Result<Vec<Bucket>> {
                <$t>::query_agg(self, series, range, interval_ms)
            }
            fn query_group(
                &mut self,
                series: SeriesId,
                range: impl RangeBounds<i64>,
                group: Option<&Group>,
                opts: QueryOpts,
            ) -> Result<Vec<Bucket>> {
                <$t>::query_group(self, series, range, group, opts)
            }
            fn query_bounds(
                &mut self,
                series: SeriesId,
                range: impl RangeBounds<i64>,
                bounds: &[i64],
                opts: QueryOpts,
            ) -> Result<Vec<Bucket>> {
                <$t>::query_bounds(self, series, range, bounds, opts)
            }
            fn query_agg_ex(&mut self, series: SeriesId, range: impl RangeBounds<i64>, interval_ms: i64, opts: QueryOpts) -> Result<Vec<Bucket>> {
                <$t>::query_agg_ex(self, series, range, interval_ms, opts)
            }
            fn query_spread(&mut self, series: SeriesId, range: impl RangeBounds<i64>, interval_ms: i64, opts: QueryOpts) -> Result<Vec<SpreadBucket>> {
                <$t>::query_spread(self, series, range, interval_ms, opts)
            }
            fn query_spread_group(&mut self, series: SeriesId, range: impl RangeBounds<i64>, group: Option<&Group>, opts: QueryOpts) -> Result<Vec<SpreadBucket>> {
                <$t>::query_spread_group(self, series, range, group, opts)
            }
            fn query_spread_bounds(&mut self, series: SeriesId, range: impl RangeBounds<i64>, bounds: &[i64], opts: QueryOpts) -> Result<Vec<SpreadBucket>> {
                <$t>::query_spread_bounds(self, series, range, bounds, opts)
            }
            fn query_twa(&mut self, series: SeriesId, range: impl RangeBounds<i64>, interval_ms: i64, opts: TwaOpts) -> Result<Vec<TwaBucket>> {
                <$t>::query_twa(self, series, range, interval_ms, opts)
            }
            fn query_twa_group(&mut self, series: SeriesId, range: impl RangeBounds<i64>, group: Option<&Group>, opts: TwaOpts) -> Result<Vec<TwaBucket>> {
                <$t>::query_twa_group(self, series, range, group, opts)
            }
            fn query_twa_bounds(&mut self, series: SeriesId, range: impl RangeBounds<i64>, bounds: &[i64], opts: TwaOpts) -> Result<Vec<TwaBucket>> {
                <$t>::query_twa_bounds(self, series, range, bounds, opts)
            }
            fn query_lttb(&mut self, series: SeriesId, range: impl RangeBounds<i64>, threshold: u64, opts: QueryOpts) -> Result<Rows> {
                <$t>::query_lttb(self, series, range, threshold, opts)
            }
            fn query_rows_multi(&mut self, series: &[SeriesId], range: impl RangeBounds<i64>, opts: QueryOpts, m: MultiOpts<'_>) -> Result<Vec<Result<Rows>>> {
                <$t>::query_rows_multi(self, series, range, opts, m)
            }
            fn query_lttb_multi(&mut self, series: &[SeriesId], range: impl RangeBounds<i64>, threshold: u64, opts: QueryOpts, m: MultiOpts<'_>) -> Result<Vec<Result<Rows>>> {
                <$t>::query_lttb_multi(self, series, range, threshold, opts, m)
            }
            fn query_chunks_multi<F>(&mut self, series: &[SeriesId], range: impl RangeBounds<i64>, opts: QueryOpts, m: MultiOpts<'_>, f: F) -> Result<()>
            where
                F: FnMut(usize, Result<(&[i64], &Column)>) -> ControlFlow<()>,
            {
                <$t>::query_chunks_multi(self, series, range, opts, m, f)
            }
            fn query_buckets_multi(&mut self, series: &[SeriesId], range: impl RangeBounds<i64>, b: Buckets<'_>, opts: QueryOpts, m: MultiOpts<'_>) -> Result<Vec<Result<Vec<Bucket>>>> {
                <$t>::query_buckets_multi(self, series, range, b, opts, m)
            }
            fn query_spread_multi(&mut self, series: &[SeriesId], range: impl RangeBounds<i64>, b: Buckets<'_>, opts: QueryOpts, m: MultiOpts<'_>) -> Result<Vec<Result<Vec<SpreadBucket>>>> {
                <$t>::query_spread_multi(self, series, range, b, opts, m)
            }
            fn query_twa_multi(&mut self, series: &[SeriesId], range: impl RangeBounds<i64>, b: Buckets<'_>, opts: TwaOpts, m: MultiOpts<'_>) -> Result<Vec<Result<Vec<TwaBucket>>>> {
                <$t>::query_twa_multi(self, series, range, b, opts, m)
            }
            fn delete_range(&mut self, series: SeriesId, range: impl RangeBounds<i64>) -> Result<()> {
                <$t>::delete_range(self, series, range)
            }
            fn replace_range<'a>(
                &mut self,
                series: SeriesId,
                range: impl RangeBounds<i64>,
                ts: &[i64],
                values: impl Into<ColumnRef<'a>>,
            ) -> Result<()> {
                <$t>::replace_range(self, series, range, ts, values)
            }
            fn set_retention(&mut self, series: SeriesId, limit_ms: i64) -> Result<()> {
                <$t>::set_retention(self, series, limit_ms)
            }
            fn retention(&mut self, series: SeriesId) -> Result<i64> {
                <$t>::retention(self, series)
            }
            fn commit(&mut self) -> Result<()> {
                <$t>::commit(self)
            }
            fn flush(&mut self) -> Result<()> {
                <$t>::flush(self)
            }
            fn optimize(&mut self) -> Result<()> {
                <$t>::optimize(self)
            }
            fn stats(&mut self) -> Result<Stats> {
                <$t>::stats(self)
            }
            fn last_query_stats(&mut $s) -> Result<QueryStats> {
                $last_query_stats
            }
            fn latest(&mut self, ids: &[SeriesId], opts: &LatestOpts) -> Result<Vec<Latest>> {
                <$t>::latest(self, ids, opts)
            }
            fn latest_by_name(&mut self, names: &[&str], opts: &LatestOpts) -> Result<Vec<Latest>> {
                <$t>::latest_by_name(self, names, opts)
            }
        }
    };
}

forward!(Client, |self| Client::last_query_stats(self));
