// SPDX-License-Identifier: AGPL-3.0-only
//! The batch calls of [`Db`]: many queries or inserts in one call, run in parallel on rayon's global pool (every
//! CPU; `RAYON_NUM_THREADS` changes it). Answers come back in query order; the first failure is [`Error::Batch`]
//! (see `tsdb_client::Pool` for the same calls over connections to tsdbd, and its notes).

use tsdb_client::__private::{
    Bucket, Buckets, Error, MultiOpts, NULL_F32_BITS, Points, QueryOpts, Result, Status, Values, chains, par_map,
};
use tsdb_client::{AggQuery, InsertBatch, RangeQuery, SeriesId, Target};

use crate::Db;

impl Db {
    /// The rows of many float32 queries, in one `tsdb_query_multi` on the database's query threads.
    ///
    /// # Errors
    ///
    /// [`Error::Batch`] with the lowest failing query's index and error (as
    /// [`query_points`](Db::query_points)); every query below it succeeded.
    ///
    /// Afterwards [`last_query_stats`](Db::last_query_stats) reports the totals of the
    /// batch's queries; so does it after [`query_agg_many`](Db::query_agg_many).
    pub fn query_points_many(&self, queries: &[RangeQuery], opts: QueryOpts) -> Result<Vec<Points>> {
        // one tsdb_query_multi: the database's query scheduler runs the series, all from one cut
        let series: Vec<SeriesId> = queries.iter().map(|q| q.series).collect();
        let starts: Vec<i64> = queries.iter().map(|q| q.start).collect();
        let ends: Vec<i64> = queries.iter().map(|q| q.end).collect();
        let m = MultiOpts { starts: Some(&starts), ends: Some(&ends), ..MultiOpts::default() };
        let rows = self.rows_multi(tsdb_sys::TSDB_MULTI_VALUES, &series, 0, 0, 0, opts, m)?;
        rows.into_iter()
            .enumerate()
            .map(|(index, r)| {
                let batch = |error| Error::Batch { index, error: Box::new(error) };
                let r = r.map_err(batch)?;
                // query_points reads float32 only, nulls as the null NaN
                let Values::Float32(mut v) = r.values.values else {
                    return Err(batch(Error::Server(Status::Invalid)));
                };
                if let Some(valid) = &r.values.valid {
                    for (i, x) in v.iter_mut().enumerate() {
                        if !valid.get(i) {
                            *x = f32::from_bits(NULL_F32_BITS);
                        }
                    }
                }
                Ok(Points { ts: r.ts, values: v })
            })
            .collect()
    }

    /// Aggregates many queries, each into buckets of its own length, in one `tsdb_query_multi`
    /// on the database's query threads.
    ///
    /// # Errors
    ///
    /// [`Error::Batch`] with the lowest failing query's index and error (as
    /// [`query_agg`](Db::query_agg)); every query below it succeeded.
    pub fn query_agg_many(&self, queries: &[AggQuery]) -> Result<Vec<Vec<Bucket>>> {
        let series: Vec<SeriesId> = queries.iter().map(|q| q.series).collect();
        let starts: Vec<i64> = queries.iter().map(|q| q.start).collect();
        let ends: Vec<i64> = queries.iter().map(|q| q.end).collect();
        let intervals: Vec<i64> = queries.iter().map(|q| q.interval_ms).collect();
        let m = MultiOpts { starts: Some(&starts), ends: Some(&ends), ..MultiOpts::default() };
        let r = self.query_buckets_multi(&series, .., Buckets::Intervals(&intervals), QueryOpts::asc(), m)?;
        r.into_iter()
            .enumerate()
            .map(|(index, r)| r.map_err(|error| Error::Batch { index, error: Box::new(error) }))
            .collect()
    }

    /// Runs many inserts in parallel, each all or nothing on its own. Inserts that share a
    /// series run in list order, so where two write the same row the later one wins.
    ///
    /// # Errors
    ///
    /// [`Error::Batch`] with the lowest failing insert's index and error; every insert below it
    /// was committed, later ones may have been.
    pub fn insert_many(&self, batches: &[InsertBatch<'_>]) -> Result<()> {
        let chains = chains(batches);
        par_map(batches.len(), Some(&chains), |i| {
            let b = batches[i];
            match b.target {
                Target::Series(s) => self.insert_series_values(s, b.ts, b.values),
                Target::Each(s) => self.insert_values(s, b.ts, b.values),
            }
        })
        .map(drop)
    }
}
