// SPDX-License-Identifier: AGPL-3.0-only
//! `impl Database for Db`: the trait of the Apache-2.0 client crate, implemented for the embedded `Db`
//! (allowed here: the trait is foreign, the type is this crate's).

use std::ops::{ControlFlow, RangeBounds};

use tsdb_client::__private::{Column, ColumnRef, Value};
use tsdb_client::{
    Bucket, Buckets, Database, Group, Latest, LatestOpts, MultiOpts, Points, QueryOpts, QueryStats, Result, Rows,
    SeriesId, SeriesStat, SpreadBucket, Stats, TenantOpts, TenantStats, TwaBucket, TwaOpts, ValueType,
};

use crate::Db;

impl Database for Db {
    fn series(&mut self, name: &str) -> Result<SeriesId> {
        Db::series(self, name)
    }
    fn series_lookup(&mut self, name: &str) -> Result<SeriesId> {
        Db::series_lookup(self, name)
    }
    fn series_at(&mut self, name: &str, path: &str) -> Result<SeriesId> {
        Db::series_at(self, name, path)
    }
    fn series_create(&mut self, name: &str, ty: ValueType, path: Option<&str>) -> Result<SeriesId> {
        Db::series_create(self, name, ty, path)
    }
    fn series_type(&mut self, id: SeriesId) -> Result<ValueType> {
        Db::series_type(self, id)
    }
    fn series_name(&mut self, id: SeriesId) -> Result<String> {
        Db::series_name(self, id)
    }
    fn state(&mut self, series: SeriesId, name: &str) -> Result<u32> {
        Db::state(self, series, name)
    }
    fn state_lookup(&mut self, series: SeriesId, name: &str) -> Result<u32> {
        Db::state_lookup(self, series, name)
    }
    fn state_name(&mut self, series: SeriesId, code: u32) -> Result<String> {
        Db::state_name(self, series, code)
    }
    fn insert_series(&mut self, series: SeriesId, ts: &[i64], values: &[Value]) -> Result<()> {
        Db::insert_series(self, series, ts, values)
    }
    fn insert(&mut self, series: &[SeriesId], ts: &[i64], values: &[Value]) -> Result<()> {
        Db::insert(self, series, ts, values)
    }
    fn insert_series_values<'a>(
        &mut self,
        series: SeriesId,
        ts: &[i64],
        values: impl Into<ColumnRef<'a>>,
    ) -> Result<()> {
        Db::insert_series_values(self, series, ts, values)
    }
    fn insert_values<'a>(&mut self, series: &[SeriesId], ts: &[i64], values: impl Into<ColumnRef<'a>>) -> Result<()> {
        Db::insert_values(self, series, ts, values)
    }
    fn query_points(&mut self, series: SeriesId, range: impl RangeBounds<i64>, opts: QueryOpts) -> Result<Points> {
        Db::query_points(self, series, range, opts)
    }
    fn query_raw<F>(&mut self, series: SeriesId, range: impl RangeBounds<i64>, opts: QueryOpts, f: F) -> Result<()>
    where
        F: FnMut(&[i64], &[Value]) -> ControlFlow<()>,
    {
        Db::query_raw(self, series, range, opts, f)
    }
    fn query_rows(&mut self, series: SeriesId, range: impl RangeBounds<i64>, opts: QueryOpts) -> Result<Rows> {
        Db::query_rows(self, series, range, opts)
    }
    fn query_values<F>(&mut self, series: SeriesId, range: impl RangeBounds<i64>, opts: QueryOpts, f: F) -> Result<()>
    where
        F: FnMut(&[i64], &Column) -> ControlFlow<()>,
    {
        Db::query_values(self, series, range, opts, f)
    }
    fn query_agg(&mut self, series: SeriesId, range: impl RangeBounds<i64>, interval_ms: i64) -> Result<Vec<Bucket>> {
        Db::query_agg(self, series, range, interval_ms)
    }
    fn query_group(
        &mut self,
        series: SeriesId,
        range: impl RangeBounds<i64>,
        group: Option<&Group>,
        opts: QueryOpts,
    ) -> Result<Vec<Bucket>> {
        Db::query_group(self, series, range, group, opts)
    }
    fn query_bounds(
        &mut self,
        series: SeriesId,
        range: impl RangeBounds<i64>,
        bounds: &[i64],
        opts: QueryOpts,
    ) -> Result<Vec<Bucket>> {
        Db::query_bounds(self, series, range, bounds, opts)
    }
    fn query_agg_ex(
        &mut self,
        series: SeriesId,
        range: impl RangeBounds<i64>,
        interval_ms: i64,
        opts: QueryOpts,
    ) -> Result<Vec<Bucket>> {
        Db::query_agg_ex(self, series, range, interval_ms, opts)
    }
    fn query_spread(
        &mut self,
        series: SeriesId,
        range: impl RangeBounds<i64>,
        interval_ms: i64,
        opts: QueryOpts,
    ) -> Result<Vec<SpreadBucket>> {
        Db::query_spread(self, series, range, interval_ms, opts)
    }
    fn query_spread_group(
        &mut self,
        series: SeriesId,
        range: impl RangeBounds<i64>,
        group: Option<&Group>,
        opts: QueryOpts,
    ) -> Result<Vec<SpreadBucket>> {
        Db::query_spread_group(self, series, range, group, opts)
    }
    fn query_spread_bounds(
        &mut self,
        series: SeriesId,
        range: impl RangeBounds<i64>,
        bounds: &[i64],
        opts: QueryOpts,
    ) -> Result<Vec<SpreadBucket>> {
        Db::query_spread_bounds(self, series, range, bounds, opts)
    }
    fn query_twa(
        &mut self,
        series: SeriesId,
        range: impl RangeBounds<i64>,
        interval_ms: i64,
        opts: TwaOpts,
    ) -> Result<Vec<TwaBucket>> {
        Db::query_twa(self, series, range, interval_ms, opts)
    }
    fn query_twa_group(
        &mut self,
        series: SeriesId,
        range: impl RangeBounds<i64>,
        group: Option<&Group>,
        opts: TwaOpts,
    ) -> Result<Vec<TwaBucket>> {
        Db::query_twa_group(self, series, range, group, opts)
    }
    fn query_twa_bounds(
        &mut self,
        series: SeriesId,
        range: impl RangeBounds<i64>,
        bounds: &[i64],
        opts: TwaOpts,
    ) -> Result<Vec<TwaBucket>> {
        Db::query_twa_bounds(self, series, range, bounds, opts)
    }
    fn query_lttb(
        &mut self,
        series: SeriesId,
        range: impl RangeBounds<i64>,
        threshold: u64,
        opts: QueryOpts,
    ) -> Result<Rows> {
        Db::query_lttb(self, series, range, threshold, opts)
    }
    fn query_rows_multi(
        &mut self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        opts: QueryOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Rows>>> {
        Db::query_rows_multi(self, series, range, opts, m)
    }
    fn query_lttb_multi(
        &mut self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        threshold: u64,
        opts: QueryOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Rows>>> {
        Db::query_lttb_multi(self, series, range, threshold, opts, m)
    }
    fn query_chunks_multi<F>(
        &mut self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        opts: QueryOpts,
        m: MultiOpts<'_>,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(usize, Result<(&[i64], &Column)>) -> ControlFlow<()>,
    {
        Db::query_chunks_multi(self, series, range, opts, m, f)
    }
    fn query_buckets_multi(
        &mut self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        b: Buckets<'_>,
        opts: QueryOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Vec<Bucket>>>> {
        Db::query_buckets_multi(self, series, range, b, opts, m)
    }
    fn query_spread_multi(
        &mut self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        b: Buckets<'_>,
        opts: QueryOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Vec<SpreadBucket>>>> {
        Db::query_spread_multi(self, series, range, b, opts, m)
    }
    fn query_twa_multi(
        &mut self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        b: Buckets<'_>,
        opts: TwaOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Vec<TwaBucket>>>> {
        Db::query_twa_multi(self, series, range, b, opts, m)
    }
    fn delete_range(&mut self, series: SeriesId, range: impl RangeBounds<i64>) -> Result<()> {
        Db::delete_range(self, series, range)
    }
    fn replace_range<'a>(
        &mut self,
        series: SeriesId,
        range: impl RangeBounds<i64>,
        ts: &[i64],
        values: impl Into<ColumnRef<'a>>,
    ) -> Result<()> {
        Db::replace_range(self, series, range, ts, values)
    }
    fn set_retention(&mut self, series: SeriesId, limit_ms: i64) -> Result<()> {
        Db::set_retention(self, series, limit_ms)
    }
    fn retention(&mut self, series: SeriesId) -> Result<i64> {
        Db::retention(self, series)
    }
    fn commit(&mut self) -> Result<()> {
        Db::commit(self)
    }
    fn flush(&mut self) -> Result<()> {
        Db::flush(self)
    }
    fn optimize(&mut self) -> Result<()> {
        Db::optimize(self)
    }
    fn stats(&mut self) -> Result<Stats> {
        Db::stats(self)
    }
    fn last_query_stats(&mut self) -> Result<QueryStats> {
        Ok(Db::last_query_stats(self))
    }
    fn latest(&mut self, ids: &[SeriesId], opts: &LatestOpts) -> Result<Vec<Latest>> {
        Db::latest(self, ids, opts)
    }
    fn latest_by_name(&mut self, names: &[&str], opts: &LatestOpts) -> Result<Vec<Latest>> {
        Db::latest_by_name(self, names, opts)
    }
    fn series_stats(&mut self, ids: &[SeriesId], resident: bool) -> Result<Vec<SeriesStat>> {
        Db::series_stats(self, ids, resident)
    }
    fn tenant_stats(&mut self, prefixes: &[&str], opts: &TenantOpts) -> Result<TenantStats> {
        Db::tenant_stats(self, prefixes, opts)
    }
}
