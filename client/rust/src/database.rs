//! [`Database`]: the operations a TCP [`Client`] and an embedded `Db` share, so code can be
//! written once for both. The trait only forwards to the inherent methods, which keep their own
//! documentation; it takes `&mut self` because a `Client` needs it (a `Db` does not).

use std::ops::{ControlFlow, RangeBounds};

use crate::client::Client;
use crate::error::Result;
use crate::types::{Bucket, Group, Points, QueryOpts, QueryStats, Rows, Stats};
use crate::value::{Column, ColumnRef, Value, ValueType};

/// What both a remote [`Client`] and an embedded `Db` (feature `embedded`) can do.
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
    fn series(&mut self, name: &str) -> Result<u32>;
    fn series_lookup(&mut self, name: &str) -> Result<u32>;
    fn series_at(&mut self, name: &str, path: &str) -> Result<u32>;
    fn series_create(&mut self, name: &str, ty: ValueType, path: Option<&str>) -> Result<u32>;
    fn series_type(&mut self, id: u32) -> Result<ValueType>;
    fn series_name(&mut self, id: u32) -> Result<String>;
    fn state(&mut self, series: u32, name: &str) -> Result<u32>;
    fn state_lookup(&mut self, series: u32, name: &str) -> Result<u32>;
    fn state_name(&mut self, series: u32, code: u32) -> Result<String>;
    fn insert_series(&mut self, series: u32, ts: &[i64], values: &[Value]) -> Result<()>;
    fn insert(&mut self, series: &[u32], ts: &[i64], values: &[Value]) -> Result<()>;
    fn insert_series_values<'a>(&mut self, series: u32, ts: &[i64], values: impl Into<ColumnRef<'a>>) -> Result<()>;
    fn insert_values<'a>(&mut self, series: &[u32], ts: &[i64], values: impl Into<ColumnRef<'a>>) -> Result<()>;
    fn query_points(&mut self, series: u32, range: impl RangeBounds<i64>, opts: QueryOpts) -> Result<Points>;
    fn query_raw<F>(&mut self, series: u32, range: impl RangeBounds<i64>, opts: QueryOpts, f: F) -> Result<()>
    where
        F: FnMut(&[i64], &[Value]) -> ControlFlow<()>;
    fn query_rows(&mut self, series: u32, range: impl RangeBounds<i64>, opts: QueryOpts) -> Result<Rows>;
    fn query_values<F>(&mut self, series: u32, range: impl RangeBounds<i64>, opts: QueryOpts, f: F) -> Result<()>
    where
        F: FnMut(&[i64], &Column) -> ControlFlow<()>;
    fn query_agg(&mut self, series: u32, range: impl RangeBounds<i64>, interval_ms: i64) -> Result<Vec<Bucket>>;
    fn query_group(
        &mut self,
        series: u32,
        range: impl RangeBounds<i64>,
        group: Option<&Group>,
        opts: QueryOpts,
    ) -> Result<Vec<Bucket>>;
    fn query_bounds(
        &mut self,
        series: u32,
        range: impl RangeBounds<i64>,
        bounds: &[i64],
        opts: QueryOpts,
    ) -> Result<Vec<Bucket>>;
    fn commit(&mut self) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
    fn optimize(&mut self) -> Result<()>;
    fn stats(&mut self) -> Result<Stats>;
    fn last_query_stats(&mut self) -> Result<QueryStats>;
}

macro_rules! forward {
    ($t:ty, |$s:ident| $last_query_stats:expr) => {
        impl Database for $t {
            fn series(&mut self, name: &str) -> Result<u32> {
                <$t>::series(self, name)
            }
            fn series_lookup(&mut self, name: &str) -> Result<u32> {
                <$t>::series_lookup(self, name)
            }
            fn series_at(&mut self, name: &str, path: &str) -> Result<u32> {
                <$t>::series_at(self, name, path)
            }
            fn series_create(&mut self, name: &str, ty: ValueType, path: Option<&str>) -> Result<u32> {
                <$t>::series_create(self, name, ty, path)
            }
            fn series_type(&mut self, id: u32) -> Result<ValueType> {
                <$t>::series_type(self, id)
            }
            fn series_name(&mut self, id: u32) -> Result<String> {
                <$t>::series_name(self, id)
            }
            fn state(&mut self, series: u32, name: &str) -> Result<u32> {
                <$t>::state(self, series, name)
            }
            fn state_lookup(&mut self, series: u32, name: &str) -> Result<u32> {
                <$t>::state_lookup(self, series, name)
            }
            fn state_name(&mut self, series: u32, code: u32) -> Result<String> {
                <$t>::state_name(self, series, code)
            }
            fn insert_series(&mut self, series: u32, ts: &[i64], values: &[Value]) -> Result<()> {
                <$t>::insert_series(self, series, ts, values)
            }
            fn insert(&mut self, series: &[u32], ts: &[i64], values: &[Value]) -> Result<()> {
                <$t>::insert(self, series, ts, values)
            }
            fn insert_series_values<'a>(
                &mut self,
                series: u32,
                ts: &[i64],
                values: impl Into<ColumnRef<'a>>,
            ) -> Result<()> {
                <$t>::insert_series_values(self, series, ts, values)
            }
            fn insert_values<'a>(&mut self, series: &[u32], ts: &[i64], values: impl Into<ColumnRef<'a>>) -> Result<()> {
                <$t>::insert_values(self, series, ts, values)
            }
            fn query_points(&mut self, series: u32, range: impl RangeBounds<i64>, opts: QueryOpts) -> Result<Points> {
                <$t>::query_points(self, series, range, opts)
            }
            fn query_raw<F>(&mut self, series: u32, range: impl RangeBounds<i64>, opts: QueryOpts, f: F) -> Result<()>
            where
                F: FnMut(&[i64], &[Value]) -> ControlFlow<()>,
            {
                <$t>::query_raw(self, series, range, opts, f)
            }
            fn query_rows(&mut self, series: u32, range: impl RangeBounds<i64>, opts: QueryOpts) -> Result<Rows> {
                <$t>::query_rows(self, series, range, opts)
            }
            fn query_values<F>(&mut self, series: u32, range: impl RangeBounds<i64>, opts: QueryOpts, f: F) -> Result<()>
            where
                F: FnMut(&[i64], &Column) -> ControlFlow<()>,
            {
                <$t>::query_values(self, series, range, opts, f)
            }
            fn query_agg(&mut self, series: u32, range: impl RangeBounds<i64>, interval_ms: i64) -> Result<Vec<Bucket>> {
                <$t>::query_agg(self, series, range, interval_ms)
            }
            fn query_group(
                &mut self,
                series: u32,
                range: impl RangeBounds<i64>,
                group: Option<&Group>,
                opts: QueryOpts,
            ) -> Result<Vec<Bucket>> {
                <$t>::query_group(self, series, range, group, opts)
            }
            fn query_bounds(
                &mut self,
                series: u32,
                range: impl RangeBounds<i64>,
                bounds: &[i64],
                opts: QueryOpts,
            ) -> Result<Vec<Bucket>> {
                <$t>::query_bounds(self, series, range, bounds, opts)
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
        }
    };
}

forward!(Client, |self| Client::last_query_stats(self));
#[cfg(feature = "embedded")]
forward!(crate::Db, |self| Ok(crate::Db::last_query_stats(self)));
