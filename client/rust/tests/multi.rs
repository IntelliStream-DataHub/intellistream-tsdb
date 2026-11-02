//! Many series in one request (protocol version 10): every `*_multi` call answers each series exactly
//! as the single call does, on tsdbd and (feature `embedded`) on the engine, with per-series errors,
//! unknown ids, duplicates, per-series ranges and intervals, time zones, splitting past 65,536 series,
//! max_rows, and an early stop that leaves the connection usable.

mod common;

use std::ops::ControlFlow;

use common::{Server, T0};
use tsdb_client::{
    Bitmap, Buckets, Client, Column, ColumnRef, Database, Error, Group, MultiOpts, Pool, QueryOpts, Status, TwaOpts,
    Unit, ValueType,
};

const N: i64 = 30_000;
const STEP: i64 = 7_000;

/// One series of each type (nulls, a part and memtable rows) and an empty one; their ids.
fn setup(db: &mut impl Database) -> Vec<u64> {
    let ts: Vec<i64> = (0..N).map(|i| T0 + i * STEP).collect();
    let valid: Bitmap = (0..N).map(|i| i % 89 != 5).collect();
    let mut ids = Vec::new();
    for ty in ValueType::ALL {
        let id = db.series_create(&format!("multi.{ty}"), ty, None).unwrap();
        let col = match ty {
            ValueType::Float32 => Column::new((0..N).map(|i| (i as f32 * 0.01).sin() * 50.0).collect::<Vec<f32>>()),
            ValueType::Float64 => Column::new((0..N).map(|i| (i as f64).sqrt()).collect::<Vec<f64>>()),
            ValueType::Int64 => Column::new((0..N).map(|i| i * i % 1000 - 300).collect::<Vec<i64>>()),
            ValueType::Bool => Column::new((0..N).map(|i| i % 3 == 0).collect::<Vec<bool>>()),
            _ => {
                let c = [db.state(id, "on").unwrap(), db.state(id, "off").unwrap()];
                Column::new((0..N).map(|i| c[(i / 10 % 2) as usize]).collect::<Vec<u32>>())
            }
        };
        db.insert_series_values(id, &ts, ColumnRef::from(&col).with_validity(&valid)).unwrap();
        ids.push(id);
    }
    db.flush().unwrap();
    // a few rows that stay in the memtable, and a series without rows
    let f = ids[0];
    db.insert_series_values(f, &ts[200..203], &[1.5f32, 2.5, 3.5][..]).unwrap();
    ids.push(db.series("multi.empty").unwrap());
    ids
}

#[derive(Clone)]
enum B {
    One,
    Every(i64),
    Iv(Vec<i64>),
    G(Group),
    Bd(Vec<i64>),
}

impl B {
    fn wire(&self) -> Buckets<'_> {
        match self {
            B::One => Buckets::One,
            B::Every(i) => Buckets::Every(*i),
            B::Iv(v) => Buckets::Intervals(v),
            B::G(g) => Buckets::Group(g),
            B::Bd(b) => Buckets::Bounds(b),
        }
    }
}

fn key<T: std::fmt::Debug>(r: &T) -> String {
    format!("{r:?}")
}

/// The single call for series `i` of a bucket query of `kind` ("agg", "spread", "twa").
#[allow(clippy::too_many_arguments)]
fn single(
    db: &mut impl Database,
    kind: &str,
    id: u64,
    (s, e): (i64, i64),
    b: &B,
    i: usize,
    o: QueryOpts,
    t: TwaOpts,
) -> String {
    let interval = match b {
        B::One => Some(0),
        B::Every(v) => Some(*v),
        B::Iv(v) => Some(v[i]),
        _ => None,
    };
    match (kind, b, interval) {
        ("agg", B::G(g), _) => key(&db.query_group(id, s..e, Some(g), o)),
        ("agg", B::Bd(bd), _) => key(&db.query_bounds(id, s..e, bd, o)),
        ("agg", _, Some(iv)) => key(&db.query_agg_ex(id, s..e, iv, o)),
        ("spread", B::G(g), _) => key(&db.query_spread_group(id, s..e, Some(g), o)),
        ("spread", B::Bd(bd), _) => key(&db.query_spread_bounds(id, s..e, bd, o)),
        ("spread", _, Some(iv)) => key(&db.query_spread(id, s..e, iv, o)),
        ("twa", B::G(g), _) => key(&db.query_twa_group(id, s..e, Some(g), t)),
        ("twa", B::Bd(bd), _) => key(&db.query_twa_bounds(id, s..e, bd, t)),
        ("twa", _, Some(iv)) => key(&db.query_twa(id, s..e, iv, t)),
        _ => unreachable!(),
    }
}

#[allow(clippy::too_many_arguments)]
fn multi(
    db: &mut impl Database,
    kind: &str,
    series: &[u64],
    r: (i64, i64),
    b: &B,
    o: QueryOpts,
    t: TwaOpts,
    m: MultiOpts<'_>,
) -> Vec<String> {
    let (s, e) = r;
    match kind {
        "agg" => db.query_buckets_multi(series, s..e, b.wire(), o, m).unwrap().iter().map(key).collect(),
        "spread" => db.query_spread_multi(series, s..e, b.wire(), o, m).unwrap().iter().map(key).collect(),
        _ => db.query_twa_multi(series, s..e, b.wire(), t, m).unwrap().iter().map(key).collect(),
    }
}

fn multi_equals_single(db: &mut impl Database) {
    let ids = setup(db);
    let mut series = ids.clone();
    series.extend([999_999, ids[0], ids[3]]); // an unknown id, and duplicates
    let n = series.len();
    let full = (i64::MIN, i64::MAX);
    let range = (T0 + 1_000, T0 + 150_000_000);
    // per-series ranges (closed on both sides, all different) and intervals (one negative: INVALID for it)
    let starts: Vec<i64> = (0..n as i64).map(|i| T0 + i * 3_000_000).collect();
    let ends: Vec<i64> = (0..n as i64).map(|i| T0 + 100_000_000 + i * 7_000_000).collect();
    let mut ivs: Vec<i64> = (0..n as i64).map(|i| 600_000 * (i + 1)).collect();
    ivs[2] = -5;
    let oslo = Group::new(Unit::Day).timezone("Europe/Oslo");
    let shapes = [
        B::One,
        B::Every(3_600_000),
        B::Iv(ivs.clone()),
        B::G(Group::new(Unit::Hour)),
        B::G(Group::new(Unit::Minute).count(15).timezone("UTC")),
        B::G(oslo.clone()),
        B::G(Group::new(Unit::Week).timezone("America/New_York")),
        B::Bd(vec![T0, T0 + 3_600_000, T0 + 90_000_000]),
    ];
    let opts = [QueryOpts::asc(), QueryOpts::desc().limit(5), QueryOpts::asc().limit(0)];
    let twas = [TwaOpts::step(), TwaOpts::linear().max_gap(60_000).desc().limit(3)];
    for o in opts {
        for per in [false, true] {
            for r in [full, range] {
                let m = if per {
                    MultiOpts { starts: Some(&starts), ends: Some(&ends), threads: 3, ..MultiOpts::default() }
                } else {
                    MultiOpts::default()
                };
                let rng = |i: usize| if per { (starts[i], ends[i]) } else { r };
                let rows: Vec<String> = db.query_rows_multi(&series, r.0..r.1, o, m).unwrap().iter().map(key).collect();
                let lttb: Vec<String> =
                    db.query_lttb_multi(&series, r.0..r.1, 50, o, m).unwrap().iter().map(key).collect();
                for (i, &id) in series.iter().enumerate() {
                    let (s, e) = rng(i);
                    assert_eq!(rows[i], key(&db.query_rows(id, s..e, o)), "rows {i} {o:?} per {per}");
                    assert_eq!(lttb[i], key(&db.query_lttb(id, s..e, 50, o)), "lttb {i} {o:?} per {per}");
                }
                for b in &shapes {
                    for kind in ["agg", "spread", "twa"] {
                        for t in if kind == "twa" { &twas[..] } else { &twas[..1] } {
                            let t = if o.limit == Some(0) { t.limit(0) } else { *t };
                            let got = multi(db, kind, &series, r, b, o, t, m);
                            for (i, &id) in series.iter().enumerate() {
                                let want = single(db, kind, id, rng(i), b, i, o, t);
                                assert_eq!(got[i], want, "{kind} {i} {:?} {o:?} {t:?} per {per} {r:?}", key(&b.wire()));
                            }
                        }
                    }
                }
            }
        }
    }
    // TWA in a zone with closed sides that differ between series: one by one over TCP, the same answers
    let mixed = MultiOpts { starts: Some(&starts), ..MultiOpts::default() };
    let got = multi(db, "twa", &series, full, &B::G(oslo.clone()), QueryOpts::asc(), TwaOpts::step(), mixed);
    for (i, &id) in series.iter().enumerate() {
        let want =
            single(db, "twa", id, (starts[i], i64::MAX), &B::G(oslo.clone()), i, QueryOpts::asc(), TwaOpts::step());
        assert_eq!(got[i], want, "twa mixed {i}");
    }
    // the per-series error is the series' own: the negative interval
    let r =
        db.query_buckets_multi(&series, .., Buckets::Intervals(&ivs), QueryOpts::asc(), MultiOpts::default()).unwrap();
    assert_eq!(r[2].as_ref().unwrap_err().status(), Some(Status::Invalid));
    assert!(r[0].is_ok() && r[3].is_ok());
    // LTTB of a state series is INVALID for that series only
    let r = db.query_lttb_multi(&ids, .., 10, QueryOpts::asc(), MultiOpts::default()).unwrap();
    assert_eq!(r[4].as_ref().unwrap_err().status(), Some(Status::Invalid));
    assert_eq!(r[0].as_ref().unwrap().len(), 10);
    // arguments the client refuses
    let short = [T0];
    let bad = MultiOpts { starts: Some(&short), ..MultiOpts::default() };
    assert!(matches!(db.query_rows_multi(&series, .., QueryOpts::asc(), bad), Err(Error::InvalidArgument(_))));
    assert!(db.query_rows_multi(&[], .., QueryOpts::asc(), MultiOpts::default()).unwrap().is_empty());
    // early stop: one chunk, then the call returns and the handle answers the next query
    let mut calls = Vec::new();
    db.query_chunks_multi(&series, .., QueryOpts::asc(), MultiOpts::default(), |i, r| {
        calls.push((i, r.map(|(ts, _)| ts.len()).ok()));
        ControlFlow::Break(())
    })
    .unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, 0);
    let all: Vec<String> =
        db.query_rows_multi(&series, .., QueryOpts::asc(), MultiOpts::default()).unwrap().iter().map(key).collect();
    for (i, &id) in series.iter().enumerate() {
        assert_eq!(all[i], key(&db.query_rows(id, .., QueryOpts::asc())));
    }
    // the chunks of every series, in order, add up to its rows
    let mut got: Vec<Vec<i64>> = vec![Vec::new(); n];
    let mut errs = Vec::new();
    db.query_chunks_multi(&series, .., QueryOpts::asc(), MultiOpts::default(), |i, r| {
        match r {
            Ok((ts, col)) => {
                assert_eq!(ts.len(), col.len());
                got[i].extend_from_slice(ts);
            }
            Err(e) => errs.push((i, e.status())),
        }
        ControlFlow::Continue(())
    })
    .unwrap();
    assert!(errs.is_empty());
    for (i, &id) in series.iter().enumerate() {
        assert_eq!(got[i], db.query_rows(id, .., QueryOpts::asc()).unwrap().ts, "chunks of {i}");
    }
}

#[test]
fn multi_equals_single_on_tcp() {
    let srv = Server::start();
    multi_equals_single(&mut srv.client());
}

#[cfg(feature = "embedded")]
#[test]
fn multi_equals_single_embedded() {
    let dir = common::test_dir("tsdb-multi-embedded");
    let mut db = tsdb_client::Db::open(&dir, &tsdb_client::Options::default()).unwrap();
    multi_equals_single(&mut db);
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

/// The engine's multi calls answer what tsdbd's do, on the same directory.
#[cfg(feature = "embedded")]
#[test]
fn tsdbd_multi_answers_what_the_engine_answered() {
    let dir = common::test_dir("tsdb-multi-vs-tcp");
    let mut db = tsdb_client::Db::open(&dir, &tsdb_client::Options::default()).unwrap();
    // no unknown id here: tsdb_query_twa_group_local answers one with NOTFOUND, tsdbd (as a client trims
    // local periods) with no buckets; each side's multi call matches its single call (multi_equals_single)
    let ids = setup(&mut db);
    db.flush().unwrap(); // tsdbd finds the memtable rows in a part, so the engine reads them from one too
    // and the same parts: a float spread over two overlapping parts differs in the last bit from one over their
    // merge (docs/design.md), and a background merge may finish before the engine's queries or only after them
    db.optimize().unwrap();
    let embedded = answers(&mut db, &ids);
    db.close().unwrap();
    let srv = Server::start_in(dir);
    let tcp = answers(&mut srv.client(), &ids);
    assert_eq!(embedded.len(), tcp.len());
    for (e, t) in embedded.iter().zip(&tcp) {
        assert_eq!(e, t);
    }
}

#[cfg(feature = "embedded")]
fn answers(db: &mut impl Database, ids: &[u64]) -> Vec<String> {
    let full = (i64::MIN, i64::MAX);
    let oslo = Group::new(Unit::Day).timezone("Europe/Oslo");
    let starts: Vec<i64> = (0..ids.len() as i64).map(|i| T0 + i * 1_000_000).collect();
    let mut out = Vec::new();
    for m in [MultiOpts::default(), MultiOpts { starts: Some(&starts), ..MultiOpts::default() }] {
        out.extend(db.query_rows_multi(ids, .., QueryOpts::desc().limit(100), m).unwrap().iter().map(key));
        for b in [B::One, B::Every(600_000), B::G(oslo.clone()), B::Bd(vec![T0 + 5_000, T0 + 9_000_000])] {
            for kind in ["agg", "spread", "twa"] {
                out.extend(multi(db, kind, ids, full, &b, QueryOpts::asc(), TwaOpts::linear(), m));
            }
        }
    }
    out
}

/// More than 65,536 series go in several requests; the indexes run on across them.
#[test]
fn long_lists_are_split() {
    let srv = Server::start();
    let mut c = srv.client();
    let ids = setup(&mut c);
    let n = 65_536 + 3;
    // fillers: ids with a zero volume field, which no series has
    let mut series: Vec<u64> = (0..n as u64).map(|i| 5_000_000 + i).collect();
    for (k, at) in [0, 65_535, 65_536, n - 1].into_iter().enumerate() {
        series[at] = ids[k];
    }
    let b = c.query_buckets_multi(&series, .., Buckets::Every(86_400_000), QueryOpts::asc(), MultiOpts::default());
    let b = b.unwrap();
    assert_eq!(b.len(), n);
    for (i, &id) in series.iter().enumerate() {
        let r = b[i].as_ref().unwrap();
        if ids.contains(&id) {
            assert_eq!(key(r), key(&c.query_agg(id, .., 86_400_000).unwrap()), "series {i}");
        } else {
            assert!(r.is_empty());
        }
    }
    let rows = c.query_rows_multi(&series, .., QueryOpts::desc().limit(2), MultiOpts::default()).unwrap();
    assert_eq!(rows[n - 1].as_ref().unwrap().ts, c.query_rows(ids[3], .., QueryOpts::desc().limit(2)).unwrap().ts);
    // max_rows counts over all requests of the call
    let m = MultiOpts { max_rows: 3, ..MultiOpts::default() };
    let r = c.query_rows_multi(&series, .., QueryOpts::desc().limit(2), m);
    assert_eq!(r.unwrap_err().status(), Some(Status::TooManyBuckets));
    assert_eq!(c.query_rows(ids[0], .., QueryOpts::asc().limit(1)).unwrap().len(), 1, "usable after");
}

/// Stopping a long reply sends CANCEL; the reply ends early and the connection goes on.
#[test]
fn a_stopped_reply_ends_early() {
    let srv = Server::start();
    let mut c = srv.client();
    let id = c.series("big").unwrap();
    let ts: Vec<i64> = (0..400_000).map(|i| T0 + i).collect();
    c.insert_series_values(id, &ts, &(0..400_000).map(|i| i as f32).collect::<Vec<f32>>()[..]).unwrap();
    let series = vec![id; 64];
    let mut chunks = 0;
    c.query_chunks_multi(&series, .., QueryOpts::asc(), MultiOpts::default(), |_, r| {
        r.unwrap();
        chunks += 1;
        if chunks == 3 { ControlFlow::Break(()) } else { ControlFlow::Continue(()) }
    })
    .unwrap();
    assert_eq!(chunks, 3);
    let st = c.last_query_stats().unwrap();
    assert!(st.result_rows < 64 * 400_000, "the server stopped: {} rows", st.result_rows);
    assert_eq!(c.query_rows(id, .., QueryOpts::desc().limit(1)).unwrap().ts, [T0 + 399_999]);
}

/// A pool forwards a multi call to one of its connections.
#[test]
fn a_pool_answers_as_a_client() {
    let srv = Server::start();
    let mut c: Client = srv.client();
    let ids = setup(&mut c);
    let pool = Pool::connect(srv.addr(), 2).unwrap();
    let g = Group::new(Unit::Day).timezone("Europe/Oslo");
    let a = pool.query_buckets_multi(&ids, .., Buckets::Group(&g), QueryOpts::asc(), MultiOpts::default()).unwrap();
    let b = c.query_buckets_multi(&ids, .., Buckets::Group(&g), QueryOpts::asc(), MultiOpts::default()).unwrap();
    assert_eq!(key(&a), key(&b));
    let a = pool.query_twa_multi(&ids, .., Buckets::One, TwaOpts::linear(), MultiOpts::default()).unwrap();
    let b = c.query_twa_multi(&ids, .., Buckets::One, TwaOpts::linear(), MultiOpts::default()).unwrap();
    assert_eq!(key(&a), key(&b));
}
