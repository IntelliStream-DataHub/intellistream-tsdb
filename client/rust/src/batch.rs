//! Batch calls: many queries or inserts in one call, run in parallel.
//!
//! Design notes:
//! - One call answers many queries (one series and range each). [`Db`](crate::Db) runs them on
//!   rayon's global pool (every CPU; `RAYON_NUM_THREADS` changes it), [`Pool`] on its own
//!   connections, one worker thread per connection. A caller with one thread thereby uses every
//!   core, and a binding with a global interpreter lock (Python) crosses it once per batch
//!   instead of once per query.
//! - Results come back in query order, one `Points` or bucket vector per query, as the
//!   one-query calls return them: no copy beyond the one those make, so a binding can hand each
//!   vector to its language as an array without copying (Python: numpy takes it over).
//! - A pool worker owns its connection: rayon runs one job at a time per thread, so each
//!   connection's lock is never contended; it exists to make the pool `Sync`. A connection
//!   that broke (an I/O error or timeout) is replaced by a new one before its next query, and
//!   so is every other connection made before it broke: a server that went away (a restart)
//!   took them all, and each would otherwise fail a later call of its own. A query whose
//!   connection broke runs once more on a new connection (queries change nothing); an insert
//!   does not, since it may have been applied before the connection broke.
//! - An error fails the call with [`Error::Batch`]: the lowest failing entry's index and error.
//!   Entries are started in increasing order and none at or above a failed one is started, so
//!   every entry below the reported one ran and succeeded; entries above it may have run too.
//!   Inserts are each all or nothing on their own, not together.
//! - Inserts into the same series keep their list order: batches that share a series run one
//!   after another in one task, so the later one's rows win, as if the list ran one at a time.

use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use rayon::prelude::*;

use crate::SeriesId;
use crate::client::{Client, ConnectOptions};
use crate::error::{Error, Result};
use std::ops::{ControlFlow, RangeBounds};

use crate::client::bounds;
use crate::types::{Bucket, Buckets, MultiOpts, Points, QueryOpts, Rows, SpreadBucket, TwaBucket, TwaOpts};
use crate::value::{Column, ColumnRef};

/// One query of a batch: `series` over the half-open range `[start, end)` in ms since the
/// epoch. `i64::MIN` and `i64::MAX` leave a side open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RangeQuery {
    /// The series id.
    pub series: SeriesId,
    /// First timestamp included.
    pub start: i64,
    /// Timestamp to stop before.
    pub end: i64,
}

impl RangeQuery {
    /// `series` over `[start, end)`.
    pub fn new(series: SeriesId, start: i64, end: i64) -> Self {
        RangeQuery { series, start, end }
    }
}

/// One aggregate query of a batch: [`RangeQuery`] and its bucket length in ms (0: one bucket).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AggQuery {
    /// The series id.
    pub series: SeriesId,
    /// First timestamp included.
    pub start: i64,
    /// Timestamp to stop before.
    pub end: i64,
    /// Bucket length in ms, buckets aligned to the epoch; 0 gives one bucket for the range.
    pub interval_ms: i64,
}

impl AggQuery {
    /// `series` over `[start, end)` in buckets of `interval_ms`.
    pub fn new(series: SeriesId, start: i64, end: i64, interval_ms: i64) -> Self {
        AggQuery { series, start, end, interval_ms }
    }
}

/// Where the rows of an [`InsertBatch`] go.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Target<'a> {
    /// Every row into this series.
    Series(SeriesId),
    /// Row i into `series[i]`.
    Each(&'a [SeriesId]),
}

/// One insert of a batch: all or nothing on its own.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct InsertBatch<'a> {
    /// The series of the rows.
    pub target: Target<'a>,
    /// Timestamps, ms since the epoch.
    pub ts: &'a [i64],
    /// One value per timestamp, of the series' type.
    pub values: ColumnRef<'a>,
}

impl<'a> InsertBatch<'a> {
    /// Rows of one series.
    pub fn series(series: SeriesId, ts: &'a [i64], values: impl Into<ColumnRef<'a>>) -> Self {
        InsertBatch { target: Target::Series(series), ts, values: values.into() }
    }

    /// Row i into `series[i]`.
    pub fn each(series: &'a [SeriesId], ts: &'a [i64], values: impl Into<ColumnRef<'a>>) -> Self {
        InsertBatch { target: Target::Each(series), ts, values: values.into() }
    }
}

/// Runs `f` for 0..n on rayon's global pool (with `chains`, see [`claim_map`]); the results in
/// order, or the lowest failing index's error.
#[cfg(feature = "embedded")]
fn par_map<T: Send>(
    n: usize,
    chains: Option<&[Vec<usize>]>,
    f: impl Fn(usize) -> Result<T> + Sync + Send,
) -> Result<Vec<T>> {
    if n == 1 {
        // no hand-off for a batch of one
        return f(0).map(|v| vec![v]).map_err(|e| Error::Batch { index: 0, error: Box::new(e) });
    }
    claim_map(None, rayon::current_num_threads(), n, chains, f)
}

/// Runs f(0..n) as `k` tasks (on `pool`, else rayon's global pool) that each take the next group
/// of indices until none is left: one index each without `chains`, else each chain's indices in
/// order, one after another. The results in index order, or [`Error::Batch`] of the lowest
/// failing index.
///
/// Rayon's own splitting of a parallel iterator costs more here: its idle workers spin, sleep
/// and wake one another for every piece, which cut a batch of 3600-row queries to three
/// quarters of what the same threads answered one query at a time.
///
/// Every index below the reported one ran: chains are taken in the order of their first index,
/// and an index is skipped only when one below it failed already (`low` only ever falls, to the
/// reported index at the end), so a chain holding an index below it is never skipped, nor is that
/// index within it.
fn claim_map<T: Send>(
    pool: Option<&rayon::ThreadPool>,
    k: usize,
    n: usize,
    chains: Option<&[Vec<usize>]>,
    f: impl Fn(usize) -> Result<T> + Sync,
) -> Result<Vec<T>> {
    let groups = chains.map_or(n, <[_]>::len);
    let next = AtomicUsize::new(0);
    let low = AtomicUsize::new(usize::MAX); // the lowest failing index so far
    let done: Mutex<Vec<(usize, Result<T>)>> = Mutex::new(Vec::with_capacity(n));
    let task = || {
        let mut mine = Vec::new();
        let mut one = [0];
        loop {
            let g = next.fetch_add(1, Ordering::Relaxed);
            if g >= groups {
                break;
            }
            let ix: &[usize] = match chains {
                Some(c) => &c[g],
                None => {
                    one[0] = g;
                    &one
                }
            };
            if ix.first().is_some_and(|&i| i >= low.load(Ordering::Relaxed)) {
                break; // every later chain starts higher still
            }
            for &i in ix {
                if i >= low.load(Ordering::Relaxed) {
                    break;
                }
                let r = f(i);
                if r.is_err() {
                    low.fetch_min(i, Ordering::Relaxed);
                }
                mine.push((i, r));
            }
        }
        done.lock().unwrap_or_else(|e| e.into_inner()).extend(mine);
    };
    let k = k.clamp(1, groups.max(1));
    match pool {
        Some(p) => p.scope(|s| (0..k).for_each(|_| s.spawn(|_| task()))),
        None => rayon::scope(|s| (0..k).for_each(|_| s.spawn(|_| task()))),
    }
    let mut done = done.into_inner().unwrap_or_else(|e| e.into_inner());
    done.sort_unstable_by_key(|d| d.0);
    done.into_iter().map(|(index, r)| r.map_err(|e| Error::Batch { index, error: Box::new(e) })).collect()
}

/// The inserts of a batch grouped into chains of those that share a series (directly or through
/// others), each chain in list order, the chains in the order of their first insert. Running a
/// chain in one task keeps the list order where it decides which row wins.
fn chains(batches: &[InsertBatch<'_>]) -> Vec<Vec<usize>> {
    fn root(p: &mut [usize], mut x: usize) -> usize {
        while p[x] != x {
            p[x] = p[p[x]];
            x = p[x];
        }
        x
    }
    let mut parent: Vec<usize> = (0..batches.len()).collect();
    let mut owner: HashMap<SeriesId, usize> = HashMap::new();
    for (i, b) in batches.iter().enumerate() {
        let ids = match b.target {
            Target::Series(s) => std::slice::from_ref(&s).to_vec(),
            Target::Each(s) => {
                let mut v: Vec<SeriesId> = s.to_vec();
                v.dedup(); // rows of one series usually come together
                v
            }
        };
        for s in ids {
            let o = *owner.entry(s).or_insert(i);
            let (a, b) = (root(&mut parent, o), root(&mut parent, i));
            parent[a.max(b)] = a.min(b);
        }
    }
    let mut chain_of: Vec<usize> = vec![usize::MAX; batches.len()];
    let mut out: Vec<Vec<usize>> = Vec::new();
    for i in 0..batches.len() {
        let r = root(&mut parent, i);
        if chain_of[r] == usize::MAX {
            chain_of[r] = out.len();
            out.push(Vec::new());
        }
        out[chain_of[r]].push(i);
    }
    out
}

#[cfg(feature = "embedded")]
impl crate::Db {
    /// The rows of many float32 queries, in one `tsdb_query_multi` on the database's query threads.
    ///
    /// # Errors
    ///
    /// [`Error::Batch`] with the lowest failing query's index and error (as
    /// [`query_points`](crate::Db::query_points)); every query below it succeeded.
    ///
    /// Afterwards [`last_query_stats`](crate::Db::last_query_stats) reports the totals of the
    /// batch's queries; so does it after [`query_agg_many`](crate::Db::query_agg_many).
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
                let crate::Values::Float32(mut v) = r.values.values else {
                    return Err(batch(Error::Server(crate::Status::Invalid)));
                };
                if let Some(valid) = &r.values.valid {
                    for (i, x) in v.iter_mut().enumerate() {
                        if !valid.get(i) {
                            *x = f32::from_bits(crate::NULL_F32_BITS);
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
    /// [`query_agg`](crate::Db::query_agg)); every query below it succeeded.
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

type Connector = dyn Fn() -> Result<Client> + Send + Sync;

/// A pool connection and the pool's epoch when it was made.
struct Conn {
    client: Client,
    epoch: u64,
}

/// A fixed number of connections to one tsdbd, for batch calls that use them all at once.
///
/// ```no_run
/// use tsdb_client::{AggQuery, Pool};
/// let pool = Pool::connect("127.0.0.1:7070", 16)?;
/// let day = 86_400_000;
/// let queries: Vec<AggQuery> = (0..100).map(|s| AggQuery::new(s, 0, day, 3_600_000)).collect();
/// let hourly = pool.query_agg_many(&queries)?;
/// for (i, q) in queries.iter().enumerate() {
///     println!("series {}: {} buckets", q.series, hourly[i].len());
/// }
/// # Ok::<(), tsdb_client::Error>(())
/// ```
pub struct Pool {
    conns: Vec<Mutex<Option<Conn>>>,
    workers: rayon::ThreadPool,
    connect: Arc<Connector>,
    /// Bumped when a connection breaks: every connection made before is replaced before its
    /// next use.
    epoch: AtomicU64,
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool").field("connections", &self.conns.len()).finish()
    }
}

impl Pool {
    /// Opens `n` connections to `addr` (as [`Client::connect`]).
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] for `n == 0`; the first connection's error.
    pub fn connect(addr: impl ToSocketAddrs, n: usize) -> Result<Pool> {
        let addrs: Vec<SocketAddr> = addr.to_socket_addrs()?.collect();
        Pool::with_connector(n, move || Client::connect(&addrs[..]))
    }

    /// Opens `n` connections to `addr` with `opts` (as [`Client::connect_opts`]), which also replace
    /// connections that broke. Every connection logs in; PBKDF2 runs once per process for the user and
    /// password, not once per connection (the keys are cached).
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] for `n == 0`; the first connection's error ([`Error::Auth`] for a failed
    /// login).
    pub fn connect_opts(addr: impl ToSocketAddrs, n: usize, opts: ConnectOptions) -> Result<Pool> {
        let addrs: Vec<SocketAddr> = addr.to_socket_addrs()?.collect();
        Pool::with_connector(n, move || Client::connect_opts(&addrs[..], &opts))
    }

    /// Opens `n` connections with `connect` (which also replaces connections that broke).
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] for `n == 0`; the first connection's error.
    pub fn with_connector(n: usize, connect: impl Fn() -> Result<Client> + Send + Sync + 'static) -> Result<Pool> {
        if n == 0 {
            return Err(Error::InvalidArgument("a pool of 0 connections".into()));
        }
        let workers = rayon::ThreadPoolBuilder::new()
            .num_threads(n)
            .thread_name(|i| format!("tsdb-pool-{i}"))
            .build()
            .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?;
        let conns = workers.install(|| (0..n).into_par_iter().map(|_| connect()).collect::<Result<Vec<_>>>())?;
        Ok(Pool {
            conns: conns.into_iter().map(|client| Mutex::new(Some(Conn { client, epoch: 0 }))).collect(),
            workers,
            connect: Arc::new(connect),
            epoch: AtomicU64::new(0),
        })
    }

    /// Number of connections.
    pub fn size(&self) -> usize {
        self.conns.len()
    }

    /// Connection `k`; one left behind by a panic (possibly in the middle of a reply) is dropped.
    /// Runs `f` on the connection in `slot` (made first if it has none or an old one); with
    /// `retry`, once more on a new connection if this one broke.
    fn attempt<T>(&self, slot: &mut Option<Conn>, retry: bool, f: impl Fn(&mut Client) -> Result<T>) -> Result<T> {
        let mut tries = if retry { 2 } else { 1 };
        loop {
            tries -= 1;
            let epoch = self.epoch.load(Ordering::Acquire);
            if slot.as_ref().is_none_or(|c| c.epoch != epoch || c.client.is_broken()) {
                *slot = None;
                *slot = Some(Conn { client: (self.connect)()?, epoch });
            }
            let c = slot.as_mut().expect("connected above");
            let r = f(&mut c.client);
            if c.client.is_broken() {
                // the server may have gone away, and with it every connection made before
                let _ = self.epoch.compare_exchange(c.epoch, c.epoch + 1, Ordering::AcqRel, Ordering::Relaxed);
                *slot = None;
                if tries > 0 && r.is_err() {
                    continue;
                }
            }
            return r;
        }
    }

    /// Runs the query `f` on one connection, a free one if there is one (on the caller's thread:
    /// a multi-series request is one request, which the server spreads over its threads).
    fn one<T>(&self, f: impl Fn(&mut Client) -> Result<T>) -> Result<T> {
        let mut slot =
            (0..self.conns.len()).find_map(|k| self.conns[k].try_lock().ok()).unwrap_or_else(|| self.slot(0));
        self.attempt(&mut slot, true, f)
    }

    /// [`Client::query_rows_multi`] on one of the pool's connections (retried once on a new one if it breaks).
    pub fn query_rows_multi(
        &self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        opts: QueryOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Rows>>> {
        let (s, e) = bounds(&range);
        self.one(|c| c.query_rows_multi(series, s..e, opts, m))
    }

    /// [`Client::query_lttb_multi`] on one of the pool's connections.
    pub fn query_lttb_multi(
        &self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        threshold: u64,
        opts: QueryOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Rows>>> {
        let (s, e) = bounds(&range);
        self.one(|c| c.query_lttb_multi(series, s..e, threshold, opts, m))
    }

    /// [`Client::query_chunks_multi`] on one of the pool's connections (not retried: `f` may have
    /// seen chunks).
    pub fn query_chunks_multi<F>(
        &self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        opts: QueryOpts,
        m: MultiOpts<'_>,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(usize, Result<(&[i64], &Column)>) -> ControlFlow<()>,
    {
        let (s, e) = bounds(&range);
        let f = Mutex::new(f);
        let mut slot =
            (0..self.conns.len()).find_map(|k| self.conns[k].try_lock().ok()).unwrap_or_else(|| self.slot(0));
        self.attempt(&mut slot, false, |c| {
            let mut f = f.lock().unwrap_or_else(|e| e.into_inner());
            c.query_chunks_multi(series, s..e, opts, m, &mut *f)
        })
    }

    /// [`Client::query_buckets_multi`] on one of the pool's connections.
    pub fn query_buckets_multi(
        &self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        b: Buckets<'_>,
        opts: QueryOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Vec<Bucket>>>> {
        let (s, e) = bounds(&range);
        self.one(|c| c.query_buckets_multi(series, s..e, b, opts, m))
    }

    /// [`Client::query_spread_multi`] on one of the pool's connections.
    pub fn query_spread_multi(
        &self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        b: Buckets<'_>,
        opts: QueryOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Vec<SpreadBucket>>>> {
        let (s, e) = bounds(&range);
        self.one(|c| c.query_spread_multi(series, s..e, b, opts, m))
    }

    /// [`Client::query_twa_multi`] on one of the pool's connections.
    pub fn query_twa_multi(
        &self,
        series: &[SeriesId],
        range: impl RangeBounds<i64>,
        b: Buckets<'_>,
        opts: TwaOpts,
        m: MultiOpts<'_>,
    ) -> Result<Vec<Result<Vec<TwaBucket>>>> {
        let (s, e) = bounds(&range);
        self.one(|c| c.query_twa_multi(series, s..e, b, opts, m))
    }

    fn slot(&self, k: usize) -> MutexGuard<'_, Option<Conn>> {
        self.conns[k].lock().unwrap_or_else(|e| {
            self.conns[k].clear_poison();
            let mut g = e.into_inner();
            *g = None;
            g
        })
    }

    /// Sets the timeout of every connection for each later read and write (see
    /// [`Client::set_timeout`]); new connections get theirs from the connector.
    pub fn set_timeout(&self, timeout: Option<Duration>) -> Result<()> {
        for k in 0..self.conns.len() {
            if let Some(c) = self.slot(k).as_ref() {
                c.client.set_timeout(timeout)?;
            }
        }
        Ok(())
    }

    /// Runs `f(connection, i)` for 0..n on the pool's connections, in parallel; the results in
    /// order, or the lowest failing index's error ([`Error::Batch`]). An `f` that failed is not
    /// run again.
    ///
    /// # Errors
    ///
    /// The lowest failing index's error of `f`, or of reconnecting a connection that broke.
    pub fn map<T: Send>(&self, n: usize, f: impl Fn(&mut Client, usize) -> Result<T> + Sync + Send) -> Result<Vec<T>> {
        self.run(n, None, false, f)
    }

    /// As [`map`](Pool::map), for an `f` that changes nothing (queries): one whose connection
    /// broke runs once more, on a new connection.
    ///
    /// # Errors
    ///
    /// As [`map`](Pool::map), after the second attempt.
    pub fn map_retry<T: Send>(
        &self,
        n: usize,
        f: impl Fn(&mut Client, usize) -> Result<T> + Sync + Send,
    ) -> Result<Vec<T>> {
        self.run(n, None, true, f)
    }

    fn run<T: Send>(
        &self,
        n: usize,
        chains: Option<&[Vec<usize>]>,
        retry: bool,
        f: impl Fn(&mut Client, usize) -> Result<T> + Sync + Send,
    ) -> Result<Vec<T>> {
        let run = |i: usize| {
            // inside the pool every worker has an index of its own, so its connection is its own
            let mut slot = self.slot(rayon::current_thread_index().unwrap_or(0) % self.conns.len());
            self.attempt(&mut slot, retry, |c| f(c, i))
        };
        claim_map(Some(&self.workers), self.conns.len(), n, chains, run)
    }

    /// The rows of many float32 queries, spread over the connections.
    ///
    /// # Errors
    ///
    /// [`Error::Batch`] with the lowest failing query's index and error (as
    /// [`Client::query_points`]); a query whose connection broke is retried once first.
    pub fn query_points_many(&self, queries: &[RangeQuery], opts: QueryOpts) -> Result<Vec<Points>> {
        self.map_retry(queries.len(), |c, i| {
            let q = queries[i];
            c.query_points(q.series, q.start..q.end, opts)
        })
    }

    /// Aggregates many queries, each into buckets of its own length, spread over the connections.
    ///
    /// # Errors
    ///
    /// [`Error::Batch`] with the lowest failing query's index and error (as
    /// [`Client::query_agg`]); a query whose connection broke is retried once first.
    pub fn query_agg_many(&self, queries: &[AggQuery]) -> Result<Vec<Vec<Bucket>>> {
        self.map_retry(queries.len(), |c, i| {
            let q = queries[i];
            c.query_agg(q.series, q.start..q.end, q.interval_ms)
        })
    }

    /// Runs many inserts over the connections, each all or nothing on its own. Inserts that
    /// share a series run in list order on one connection, so the later one's rows win.
    ///
    /// # Errors
    ///
    /// [`Error::Batch`] with the lowest failing insert's index and error; every insert below it
    /// was committed, later ones may have been. An insert whose connection broke is not retried:
    /// the server may have applied it.
    pub fn insert_many(&self, batches: &[InsertBatch<'_>]) -> Result<()> {
        let chains = chains(batches);
        self.run(batches.len(), Some(&chains), false, |c, i| {
            let b = batches[i];
            match b.target {
                Target::Series(s) => c.insert_series_values(s, b.ts, b.values),
                Target::Each(s) => c.insert_values(s, b.ts, b.values),
            }
        })
        .map(drop)
    }
}
