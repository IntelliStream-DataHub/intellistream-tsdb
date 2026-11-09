// SPDX-License-Identifier: AGPL-3.0-only
//! The embedded `Db` as a target of the shared multi-series and latest calls (`tsdb-client-python-common`). The
//! traits are the common crate's and `Db` is the engine crate's, so the impls sit on this wrapper.

use std::ops::Bound as Range;

use ::tsdb_client as tc;
use ::tsdb_embedded as te;
use tc::{Latest, LatestOpts};
use tsdb_client_python_common::latest::LatestTarget;
use tsdb_client_python_common::multi::MultiCalls;

pub(crate) struct DbCalls<'a>(pub(crate) &'a te::Db);

impl LatestTarget for DbCalls<'_> {
    fn latest(&self, ids: &[u64], opts: &LatestOpts) -> tc::Result<Vec<Latest>> {
        self.0.latest(ids, opts)
    }
    fn latest_by_name(&self, names: &[&str], opts: &LatestOpts) -> tc::Result<Vec<Latest>> {
        self.0.latest_by_name(names, opts)
    }
}

type Bounds = (Range<i64>, Range<i64>);

impl MultiCalls for DbCalls<'_> {
    fn query_rows_multi(
        &self,
        series: &[u64],
        range: Bounds,
        opts: tc::QueryOpts,
        m: tc::MultiOpts<'_>,
    ) -> tc::Result<Vec<tc::Result<tc::Rows>>> {
        self.0.query_rows_multi(series, range, opts, m)
    }
    fn query_lttb_multi(
        &self,
        series: &[u64],
        range: Bounds,
        threshold: u64,
        opts: tc::QueryOpts,
        m: tc::MultiOpts<'_>,
    ) -> tc::Result<Vec<tc::Result<tc::Rows>>> {
        self.0.query_lttb_multi(series, range, threshold, opts, m)
    }
    fn query_buckets_multi(
        &self,
        series: &[u64],
        range: Bounds,
        b: tc::Buckets<'_>,
        opts: tc::QueryOpts,
        m: tc::MultiOpts<'_>,
    ) -> tc::Result<Vec<tc::Result<Vec<tc::Bucket>>>> {
        self.0.query_buckets_multi(series, range, b, opts, m)
    }
    fn query_spread_multi(
        &self,
        series: &[u64],
        range: Bounds,
        b: tc::Buckets<'_>,
        opts: tc::QueryOpts,
        m: tc::MultiOpts<'_>,
    ) -> tc::Result<Vec<tc::Result<Vec<tc::SpreadBucket>>>> {
        self.0.query_spread_multi(series, range, b, opts, m)
    }
    fn query_twa_multi(
        &self,
        series: &[u64],
        range: Bounds,
        b: tc::Buckets<'_>,
        opts: tc::TwaOpts,
        m: tc::MultiOpts<'_>,
    ) -> tc::Result<Vec<tc::Result<Vec<tc::TwaBucket>>>> {
        self.0.query_twa_multi(series, range, b, opts, m)
    }
}
