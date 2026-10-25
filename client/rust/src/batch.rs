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
//!   that broke (an I/O error or timeout) is replaced by a new one before its next query.
//! - The first error (by query order) fails the call. Inserts are each all or nothing on their
//!   own, not together: when one fails, others of the same call may have been committed.

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rayon::prelude::*;

use crate::client::Client;
use crate::error::{Error, Result};
use crate::types::{Bucket, Points, QueryOpts};
use crate::value::ColumnRef;

/// One query of a batch: `series` over the half-open range `[start, end)` in ms since the
/// epoch. `i64::MIN` and `i64::MAX` leave a side open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RangeQuery {
    /// The series id.
    pub series: u32,
    /// First timestamp included.
    pub start: i64,
    /// Timestamp to stop before.
    pub end: i64,
}

impl RangeQuery {
    /// `series` over `[start, end)`.
    pub fn new(series: u32, start: i64, end: i64) -> Self {
        RangeQuery { series, start, end }
    }
}

/// One aggregate query of a batch: [`RangeQuery`] and its bucket length in ms (0: one bucket).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AggQuery {
    /// The series id.
    pub series: u32,
    /// First timestamp included.
    pub start: i64,
    /// Timestamp to stop before.
    pub end: i64,
    /// Bucket length in ms, buckets aligned to the epoch; 0 gives one bucket for the range.
    pub interval_ms: i64,
}

impl AggQuery {
    /// `series` over `[start, end)` in buckets of `interval_ms`.
    pub fn new(series: u32, start: i64, end: i64, interval_ms: i64) -> Self {
        AggQuery { series, start, end, interval_ms }
    }
}

/// Where the rows of an [`InsertBatch`] go.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Target<'a> {
    /// Every row into this series.
    Series(u32),
    /// Row i into `series[i]`.
    Each(&'a [u32]),
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
    pub fn series(series: u32, ts: &'a [i64], values: impl Into<ColumnRef<'a>>) -> Self {
        InsertBatch { target: Target::Series(series), ts, values: values.into() }
    }

    /// Row i into `series[i]`.
    pub fn each(series: &'a [u32], ts: &'a [i64], values: impl Into<ColumnRef<'a>>) -> Self {
        InsertBatch { target: Target::Each(series), ts, values: values.into() }
    }
}

/// Runs `f` for 0..n on rayon's global pool; the results in order, or the first error.
#[cfg(feature = "embedded")]
fn par_map<T: Send>(n: usize, f: impl Fn(usize) -> Result<T> + Sync + Send) -> Result<Vec<T>> {
    if n == 1 {
        return Ok(vec![f(0)?]); // no hand-off for a batch of one
    }
    claim_map(None, rayon::current_num_threads(), n, f)
}

/// Runs f(0..n) as `k` tasks (on `pool`, else rayon's global pool) that each take the next index
/// until none is left; the results in index order, or the error of the lowest failing index.
///
/// Rayon's own splitting of a parallel iterator costs more here: its idle workers spin, sleep
/// and wake one another for every piece, which cut a batch of 3600-row queries to three
/// quarters of what the same threads answered one query at a time. Indices are taken in
/// increasing order, so once one fails no new one is started, and every lower one already was.
fn claim_map<T: Send>(
    pool: Option<&rayon::ThreadPool>,
    k: usize,
    n: usize,
    f: impl Fn(usize) -> Result<T> + Sync,
) -> Result<Vec<T>> {
    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let done: Mutex<Vec<(usize, Result<T>)>> = Mutex::new(Vec::with_capacity(n));
    let task = || {
        let mut mine = Vec::new();
        while !failed.load(Ordering::Relaxed) {
            let i = next.fetch_add(1, Ordering::Relaxed);
            if i >= n {
                break;
            }
            let r = f(i);
            if r.is_err() {
                failed.store(true, Ordering::Relaxed);
            }
            mine.push((i, r));
        }
        done.lock().unwrap_or_else(|e| e.into_inner()).extend(mine);
    };
    let k = k.clamp(1, n.max(1));
    match pool {
        Some(p) => p.scope(|s| (0..k).for_each(|_| s.spawn(|_| task()))),
        None => rayon::scope(|s| (0..k).for_each(|_| s.spawn(|_| task()))),
    }
    let mut done = done.into_inner().unwrap_or_else(|e| e.into_inner());
    done.sort_unstable_by_key(|d| d.0);
    done.into_iter().map(|d| d.1).collect()
}

#[cfg(feature = "embedded")]
impl crate::Db {
    /// The rows of many float32 queries, run in parallel on every CPU.
    ///
    /// # Errors
    ///
    /// The first failing query's error (in query order), as [`query_points`](crate::Db::query_points).
    pub fn query_points_many(&self, queries: &[RangeQuery], opts: QueryOpts) -> Result<Vec<Points>> {
        par_map(queries.len(), |i| {
            let q = queries[i];
            self.query_points(q.series, q.start..q.end, opts)
        })
    }

    /// Aggregates many queries, each into buckets of its own length, in parallel.
    ///
    /// # Errors
    ///
    /// The first failing query's error, as [`query_agg`](crate::Db::query_agg).
    pub fn query_agg_many(&self, queries: &[AggQuery]) -> Result<Vec<Vec<Bucket>>> {
        par_map(queries.len(), |i| {
            let q = queries[i];
            self.query_agg(q.series, q.start..q.end, q.interval_ms)
        })
    }

    /// Runs many inserts in parallel, each all or nothing on its own.
    ///
    /// # Errors
    ///
    /// The first failing insert's error; the others may have been committed.
    pub fn insert_many(&self, batches: &[InsertBatch<'_>]) -> Result<()> {
        par_map(batches.len(), |i| {
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
    conns: Vec<Mutex<Option<Client>>>,
    workers: rayon::ThreadPool,
    connect: Arc<Connector>,
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

    /// Opens `n` connections with `connect` (which also replaces a connection that broke).
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
        let conns = workers.install(|| (0..n).into_par_iter().map(|_| connect().map(Some)).collect::<Result<Vec<_>>>())?;
        Ok(Pool { conns: conns.into_iter().map(Mutex::new).collect(), workers, connect: Arc::new(connect) })
    }

    /// Number of connections.
    pub fn size(&self) -> usize {
        self.conns.len()
    }

    /// Sets the timeout of every connection for each later read and write (see
    /// [`Client::set_timeout`]); new connections get theirs from the connector.
    pub fn set_timeout(&self, timeout: Option<Duration>) -> Result<()> {
        for c in &self.conns {
            if let Some(c) = c.lock().unwrap_or_else(|e| e.into_inner()).as_ref() {
                c.set_timeout(timeout)?;
            }
        }
        Ok(())
    }

    /// Runs `f(connection, i)` for 0..n on the pool's connections, in parallel; the results in
    /// order, or the first error.
    ///
    /// # Errors
    ///
    /// The first error of `f`, or of reconnecting a connection that broke.
    pub fn map<T: Send>(&self, n: usize, f: impl Fn(&mut Client, usize) -> Result<T> + Sync + Send) -> Result<Vec<T>> {
        let run = |i: usize| {
            // inside the pool every worker has an index of its own, so its connection is its own
            let k = rayon::current_thread_index().unwrap_or(0) % self.conns.len();
            let mut slot = self.conns[k].lock().unwrap_or_else(|e| e.into_inner());
            if slot.as_ref().is_none_or(Client::is_broken) {
                *slot = None;
                *slot = Some((self.connect)()?);
            }
            f(slot.as_mut().expect("connected above"), i)
        };
        claim_map(Some(&self.workers), self.conns.len(), n, run)
    }

    /// The rows of many float32 queries, spread over the connections.
    ///
    /// # Errors
    ///
    /// The first failing query's error, as [`Client::query_points`].
    pub fn query_points_many(&self, queries: &[RangeQuery], opts: QueryOpts) -> Result<Vec<Points>> {
        self.map(queries.len(), |c, i| {
            let q = queries[i];
            c.query_points(q.series, q.start..q.end, opts)
        })
    }

    /// Aggregates many queries, each into buckets of its own length, spread over the connections.
    ///
    /// # Errors
    ///
    /// The first failing query's error, as [`Client::query_agg`].
    pub fn query_agg_many(&self, queries: &[AggQuery]) -> Result<Vec<Vec<Bucket>>> {
        self.map(queries.len(), |c, i| {
            let q = queries[i];
            c.query_agg(q.series, q.start..q.end, q.interval_ms)
        })
    }

    /// Runs many inserts over the connections, each all or nothing on its own.
    ///
    /// # Errors
    ///
    /// The first failing insert's error; the others may have been committed.
    pub fn insert_many(&self, batches: &[InsertBatch<'_>]) -> Result<()> {
        self.map(batches.len(), |c, i| {
            let b = batches[i];
            match b.target {
                Target::Series(s) => c.insert_series_values(s, b.ts, b.values),
                Target::Each(s) => c.insert_values(s, b.ts, b.values),
            }
        })
        .map(drop)
    }
}
