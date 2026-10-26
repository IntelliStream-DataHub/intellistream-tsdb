//! Batch calls: `Db::*_many` (rayon over one handle) and `Pool` (connections to tsdbd) answer
//! exactly what the one-query calls answer, in query order, and fail with the lowest failing
//! entry's index and error; inserts into one series keep their list order; a pool survives a
//! server restart without failing a query.
#![cfg(feature = "embedded")]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use common::{Server, T0, test_dir};
use tsdb_client::{
    AggQuery, Bucket, Client, Db, Error, InsertBatch, Options, Pool, QueryOpts, RangeQuery, Status, Target, ValueType,
};

const SERIES: u32 = 24;
const ROWS: i64 = 5000;

fn rows(s: u32) -> (Vec<i64>, Vec<f32>) {
    let ts = (0..ROWS).map(|i| T0 + i * 1000 + i64::from(s)).collect();
    let v = (0..ROWS).map(|i| (i * i64::from(s + 1) % 977) as f32 * 0.5).collect();
    (ts, v)
}

/// A query per series, some over part of the range, one empty, one twice.
fn queries() -> Vec<RangeQuery> {
    let mut q: Vec<RangeQuery> =
        (0..SERIES).map(|s| RangeQuery::new(s, T0 + i64::from(s) * 77_000, T0 + 4_000_000)).collect();
    q.push(RangeQuery::new(3, i64::MIN, i64::MAX));
    q.push(RangeQuery::new(5, T0 - 10, T0 - 5));
    q.push(RangeQuery::new(3, i64::MIN, i64::MAX));
    q
}

fn data() -> Vec<(Vec<i64>, Vec<f32>)> {
    (0..SERIES).map(rows).collect()
}

/// Half the series one insert each, the other half in one insert of a series column.
fn batches<'a>(d: &'a [(Vec<i64>, Vec<f32>)], each: &'a (Vec<u32>, Vec<i64>, Vec<f32>)) -> Vec<InsertBatch<'a>> {
    let mut b: Vec<InsertBatch> =
        (0..SERIES / 2).map(|s| InsertBatch::series(s, &d[s as usize].0, &d[s as usize].1)).collect();
    b.push(InsertBatch::each(&each.0, &each.1, &each.2));
    b
}

fn each_column(d: &[(Vec<i64>, Vec<f32>)]) -> (Vec<u32>, Vec<i64>, Vec<f32>) {
    let mut c = (Vec::new(), Vec::new(), Vec::new());
    for s in SERIES / 2..SERIES {
        let (ts, v) = &d[s as usize];
        c.0.extend(std::iter::repeat_n(s, ts.len()));
        c.1.extend_from_slice(ts);
        c.2.extend_from_slice(v);
    }
    c
}

fn agg(q: &[RangeQuery], interval_ms: i64) -> Vec<AggQuery> {
    q.iter().map(|x| AggQuery::new(x.series, x.start, x.end, interval_ms)).collect()
}

fn key(b: &[Bucket]) -> String {
    format!("{b:?}")
}

#[test]
fn db_batches_match_single_queries() {
    let db = Db::open(test_dir("tsdb-batch"), &Options { fsync: false, ..Default::default() }).unwrap();
    for s in 0..SERIES {
        assert_eq!(db.series_create(&format!("s{s}"), ValueType::Float32, None).unwrap(), s);
    }
    let int64 = db.series_create("i", ValueType::Int64, None).unwrap();
    let d = data();
    let each = each_column(&d);
    db.insert_many(&batches(&d, &each)).unwrap();
    db.insert_many(&[]).unwrap();

    let q = queries();
    let p = db.query_points_many(&q, QueryOpts::asc()).unwrap();
    assert_eq!(p.len(), q.len());
    for (i, x) in q.iter().enumerate() {
        assert_eq!(p[i], db.query_points(x.series, x.start..x.end, QueryOpts::asc()).unwrap(), "query {i}");
    }
    assert_eq!(p[q.len() - 2].ts.len(), 0);
    assert_eq!(p[q.len() - 1].ts.len() as i64, ROWS);
    let desc = db.query_points_many(&q, QueryOpts::desc().limit(3)).unwrap();
    assert_eq!(desc[0], db.query_points(0, q[0].start..q[0].end, QueryOpts::desc().limit(3)).unwrap());

    let b = db.query_agg_many(&agg(&q, 60_000)).unwrap();
    for (i, x) in q.iter().enumerate() {
        assert_eq!(key(&b[i]), key(&db.query_agg(x.series, x.start..x.end, 60_000).unwrap()), "query {i}");
    }
    let whole = db.query_agg_many(&agg(&q, 0)).unwrap();
    // each query its own bucket length
    let mixed: Vec<AggQuery> =
        q.iter().enumerate().map(|(i, x)| AggQuery::new(x.series, x.start, x.end, i as i64 * 1000)).collect();
    let m = db.query_agg_many(&mixed).unwrap();
    for (i, x) in mixed.iter().enumerate() {
        assert_eq!(key(&m[i]), key(&db.query_agg(x.series, x.start..x.end, x.interval_ms).unwrap()));
    }
    assert_eq!(whole[SERIES as usize][0].count, ROWS as u64);

    // a batch counts as one query: last_query_stats holds the totals of its queries
    let p = db.query_points_many(&q, QueryOpts::asc()).unwrap();
    let st = db.last_query_stats();
    assert_eq!(st.result_rows, p.iter().map(|x| x.ts.len() as u64).sum::<u64>());
    assert!(st.result_rows > ROWS as u64 && st.rows >= st.result_rows);

    // the first failing query's error, whatever finished first
    let mut bad = q.clone();
    // a raw float32 query of an int64 series fails
    bad.insert(7, RangeQuery::new(int64, 0, 1));
    assert_eq!(failed_at(db.query_points_many(&bad, QueryOpts::asc())), (7, Status::Invalid.code()));
    assert_eq!(failed_at(db.query_agg_many(&[AggQuery::new(0, 0, 1, -1)])), (0, Status::Invalid.code()));
    // two failures, the higher one first in the list of a different error: always the lower one
    let mut two = agg(&q, 60_000);
    two[3] = AggQuery::new(0, 0, i64::MAX, 1); // too many buckets
    two[7].interval_ms = -1; // invalid
    for _ in 0..20 {
        assert_eq!(failed_at(db.query_agg_many(&two)), (3, Status::TooManyBuckets.code()));
    }
    // an insert of the wrong type fails; the call reports it
    let one = [1i64];
    let r = db.insert_many(&[InsertBatch { target: Target::Series(0), ts: &one, values: (&[1i64][..]).into() }]);
    assert!(r.is_err());
    let dir = db.path().to_path_buf();
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn pool_batches_match_single_queries() {
    let srv = Server::start();
    let mut c = srv.client();
    for s in 0..SERIES {
        assert_eq!(c.series_create(&format!("s{s}"), ValueType::Float32, None).unwrap(), s);
    }
    let int64 = c.series_create("i", ValueType::Int64, None).unwrap();
    let connects = Arc::new(AtomicUsize::new(0));
    let (n, addr) = (connects.clone(), srv.addr());
    let pool = Pool::with_connector(4, move || {
        n.fetch_add(1, Ordering::Relaxed);
        Client::connect(addr.as_str())
    })
    .unwrap();
    assert_eq!((pool.size(), connects.load(Ordering::Relaxed)), (4, 4));
    let d = data();
    let each = each_column(&d);
    pool.insert_many(&batches(&d, &each)).unwrap();

    let q = queries();
    let p = pool.query_points_many(&q, QueryOpts::asc()).unwrap();
    let b = pool.query_agg_many(&agg(&q, 60_000)).unwrap();
    assert_eq!((p.len(), b.len()), (q.len(), q.len()));
    for (i, x) in q.iter().enumerate() {
        assert_eq!(p[i], c.query_points(x.series, x.start..x.end, QueryOpts::asc()).unwrap(), "query {i}");
        assert_eq!(key(&b[i]), key(&c.query_agg(x.series, x.start..x.end, 60_000).unwrap()), "query {i}");
    }
    let mut bad = q.clone();
    bad.insert(3, RangeQuery::new(int64, 0, 1));
    assert_eq!(failed_at(pool.query_points_many(&bad, QueryOpts::asc())), (3, Status::Invalid.code()));
    let mut two = agg(&q, 60_000);
    two[3] = AggQuery::new(0, 0, i64::MAX, 1); // too many buckets
    two[7].interval_ms = -1; // invalid
    for _ in 0..20 {
        assert_eq!(failed_at(pool.query_agg_many(&two)), (3, Status::TooManyBuckets.code()));
    }
    // a server error leaves the connections usable: nothing reconnects
    assert_eq!(pool.query_agg_many(&agg(&q, 0)).unwrap().len(), q.len());
    assert_eq!(connects.load(Ordering::Relaxed), 4);
    assert!(matches!(Pool::connect(srv.addr(), 0), Err(Error::InvalidArgument(_))));
}

#[test]
fn pool_replaces_a_broken_connection() {
    let mut srv = Server::start();
    srv.client().series_create("x", ValueType::Float32, None).unwrap();
    let addr = Arc::new(std::sync::Mutex::new(srv.addr()));
    let a = addr.clone();
    let pool = Pool::with_connector(2, move || Client::connect(a.lock().unwrap().as_str())).unwrap();
    let q = [RangeQuery::new(0, i64::MIN, i64::MAX), RangeQuery::new(0, 0, 1)];
    assert_eq!(pool.query_agg_many(&agg(&q, 0)).unwrap().len(), 2);
    // the server goes away: the calls fail with I/O errors (the retry finds no server either)
    srv.stop();
    assert_eq!(failed_at(pool.query_agg_many(&agg(&q, 0))).1, Status::Io.code());
    // another server (another port): the broken connections are replaced through the connector
    let srv2 = Server::start();
    srv2.client().series_create("x", ValueType::Float32, None).unwrap();
    *addr.lock().unwrap() = srv2.addr();
    for _ in 0..3 {
        assert_eq!(pool.query_agg_many(&agg(&q, 0)).unwrap().len(), 2);
    }
}

/// A panic inside a batch closure leaves its connection's slot poisoned: that connection (which may
/// be in the middle of a reply) is dropped and replaced, not reused, and the pool keeps working.
#[test]
fn pool_drops_a_connection_left_by_a_panic() {
    let srv = Server::start();
    srv.client().series_create("x", ValueType::Float32, None).unwrap();
    let addr = srv.addr();
    let connects = Arc::new(AtomicUsize::new(0));
    let n = connects.clone();
    // one connection, so the panicking closure's slot is the one every later call uses
    let pool = Pool::with_connector(1, move || {
        n.fetch_add(1, Ordering::SeqCst);
        Client::connect(addr.as_str())
    })
    .unwrap();
    let q = AggQuery::new(0, i64::MIN, i64::MAX, 0);
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pool.map(3, |c, i| {
            let b = c.query_agg(q.series, q.start..q.end, q.interval_ms)?;
            if i == 1 {
                panic!("a closure panics");
            }
            Ok(b.len())
        })
    }));
    assert!(panicked.is_err(), "the panic reaches the caller");
    assert_eq!(connects.load(Ordering::SeqCst), 1);
    for _ in 0..3 {
        assert_eq!(pool.query_agg_many(&[q, q]).unwrap().len(), 2);
    }
    assert_eq!(connects.load(Ordering::SeqCst), 2, "the poisoned slot's connection was replaced once");
}

/// The index and code of a batch call's error.
fn failed_at<T: std::fmt::Debug>(r: Result<T, Error>) -> (usize, i32) {
    match r {
        Err(e @ Error::Batch { .. }) => (e.batch_index().unwrap(), e.code()),
        other => panic!("expected a batch error, got {other:?}"),
    }
}

#[test]
fn inserts_into_one_series_keep_their_list_order() {
    let dir = test_dir("batch-order");
    let db = Db::open(&dir, &Options { fsync: false, ..Options::default() }).unwrap();
    for s in 0..4 {
        db.series_create(&format!("s{s}"), ValueType::Float32, None).unwrap();
    }
    let ts = [T0];
    let vals: Vec<[f32; 1]> = (0..64).map(|k| [k as f32]).collect();
    let both = [0u32, 1];
    let each_ts = [T0, T0];
    let each_v = [100f32, 101.0];
    for round in 0..50 {
        // 64 batches writing one row of series 0, 1 or 2 in turn; one more writes series 0 and 1
        // through a column of ids, which ties their chains together
        let mut b: Vec<InsertBatch<'_>> =
            (0..64).map(|k| InsertBatch::series(k as u32 % 3, &ts, &vals[k][..])).collect();
        if round % 2 == 1 {
            b.insert(40, InsertBatch::each(&both, &each_ts, &each_v[..]));
        }
        db.insert_many(&b).unwrap();
        let last = |s: u32| db.query_points(s, T0..T0 + 1, QueryOpts::asc()).unwrap().values[0];
        // the last batch of each series in the list wins (63, 61, 62: k % 3)
        assert_eq!((last(0), last(1), last(2)), (63.0, 61.0, 62.0), "round {round}");
    }
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn pool_survives_a_server_restart() {
    let srv = Server::start();
    srv.client().series_create("x", ValueType::Float32, None).unwrap();
    let addr = Arc::new(std::sync::Mutex::new(srv.addr()));
    let a = addr.clone();
    let pool = Pool::with_connector(4, move || Client::connect(a.lock().unwrap().as_str())).unwrap();
    let q = [AggQuery::new(0, i64::MIN, i64::MAX, 0)];
    assert_eq!(pool.query_agg_many(&q).unwrap().len(), 1);
    // a restart (on another port): every connection is dead, yet no single-query batch fails:
    // the first broken connection marks all four for replacement, and its query runs again
    let restart = |old: Server| {
        drop(old);
        let s = Server::start();
        s.client().series_create("x", ValueType::Float32, None).unwrap();
        *addr.lock().unwrap() = s.addr();
        s
    };
    let srv = restart(srv);
    for i in 0..8 {
        assert_eq!(pool.query_agg_many(&q).map(|b| b.len()).unwrap(), 1, "query batch {i}");
    }
    // inserts are not retried (the server may have applied one): at most one fails
    let srv = restart(srv);
    let ts = [T0];
    let failed = (0..8).filter(|_| pool.insert_many(&[InsertBatch::series(0, &ts, &[1.0f32][..])]).is_err()).count();
    assert!(failed <= 1, "{failed} of 8 insert batches failed");
    drop(srv);
}
