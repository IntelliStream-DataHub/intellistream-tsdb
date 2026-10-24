//! Integration tests against a real tsdbd: every op, over the wire.

mod common;

use std::ops::ControlFlow;

use common::{Server, T0};
use tsdb_client::{Client, Error, Group, Order, QueryOpts, Status, Unit};

const MIN: i64 = 60_000;
const HOUR: i64 = 3_600_000;
const DAY: i64 = 86_400_000;

#[test]
fn series_create_lookup_name_and_volume() {
    let srv = Server::start();
    let mut c = srv.client();
    assert_eq!(c.protocol_version().unwrap(), tsdb_client::PROTOCOL_VERSION);

    let a = c.series("tank.level_bar").unwrap();
    let b = c.series("pump.flow_m3h").unwrap();
    assert_ne!(a, b);
    assert_eq!(c.series("tank.level_bar").unwrap(), a);
    assert_eq!(c.series_lookup("tank.level_bar").unwrap(), a);
    assert_eq!(c.series_name(b).unwrap(), "pump.flow_m3h");

    let e = c.series_lookup("missing").unwrap_err();
    assert_eq!(e.status(), Some(Status::NotFound));
    assert_eq!(e.code(), -6);
    assert_eq!(c.series_name(999_999).unwrap_err().status(), Some(Status::NotFound));

    // a series in another volume, then read back through the same connection
    let vol = srv.dir.with_extension("volume");
    let v = c.series_at("remote.on_volume", vol.to_str().unwrap()).unwrap();
    c.insert_series(v, &[T0], &[3.5]).unwrap();
    let p = c.query_points(v, .., QueryOpts::asc()).unwrap();
    assert_eq!((p.ts, p.values), (vec![T0], vec![3.5]));
    assert_eq!(c.series_at("plain", "").unwrap(), c.series_lookup("plain").unwrap());
}

#[test]
fn last_write_wins_and_pipelining() {
    let srv = Server::start();
    let mut c = srv.client();
    let a = c.series("a").unwrap();
    let b = c.series("b").unwrap();

    // within one batch the later row wins
    c.insert_series(a, &[T0, T0 + 1000, T0], &[1.0, 2.0, 3.0]).unwrap();
    let p = c.query_points(a, .., QueryOpts::asc()).unwrap();
    assert_eq!((p.ts, p.values), (vec![T0, T0 + 1000], vec![3.0, 2.0]));

    // a later insert of the same key replaces it, many-series layout, pipelined
    let s = [b, b, a];
    let t = [T0, T0 + 1000, T0];
    c.insert_async(&s, &t, &[1.5, 2.5, -7.0]).unwrap();
    c.insert_async(&s, &t, &[1.5, 9.0, -7.0]).unwrap();
    assert_eq!(c.pending(), 2);
    c.wait().unwrap();
    assert_eq!(c.pending(), 0);
    let p = c.query_points(b, .., QueryOpts::asc()).unwrap();
    assert_eq!((p.ts, p.values), (vec![T0, T0 + 1000], vec![1.5, 9.0]));
    assert_eq!(c.query_points(a, .., QueryOpts::asc()).unwrap().values, vec![-7.0, 2.0]);

    // many async inserts: more than the client keeps in flight, in order
    for i in 0..3000 {
        c.insert_series_async(a, &[T0 + 5000], &[i as f32]).unwrap();
    }
    c.insert(&[a], &[T0 + 5000], &[-1.0]).unwrap();
    let p = c.query_points(a, T0 + 5000..=T0 + 5000, QueryOpts::asc()).unwrap();
    assert_eq!(p.values, vec![-1.0]);

    // an empty insert is fine
    c.insert_series(a, &[], &[]).unwrap();
}

#[test]
fn large_stream_order_and_limit() {
    let srv = Server::start();
    let mut c = srv.client();
    let a = c.series("big").unwrap();
    const N: usize = 200_000; // several 16384-row chunks
    let ts: Vec<i64> = (0..N as i64).map(|i| T0 + i * 1000).collect();
    let v: Vec<f32> = (0..N).map(|i| (i % 1000) as f32 / 10.0).collect();
    c.insert_series(a, &ts, &v).unwrap();

    let p = c.query_points(a, .., QueryOpts::asc()).unwrap();
    assert_eq!(p.len(), N);
    assert!(p.ts == ts && p.values == v);

    // newest first with a limit
    let p = c.query_points(a, .., QueryOpts::desc().limit(5)).unwrap();
    assert_eq!(p.ts, ts[N - 5..].iter().rev().copied().collect::<Vec<_>>());
    assert_eq!(p.values, v[N - 5..].iter().rev().copied().collect::<Vec<_>>());
    let p = c.query_points(a, T0 + 5000.., QueryOpts { order: Order::Asc, limit: Some(3) }).unwrap();
    assert_eq!(p.ts, vec![T0 + 5000, T0 + 6000, T0 + 7000]);
    assert!(c.query_points(a, .., QueryOpts::desc().limit(0)).unwrap().is_empty());

    // half-open and inclusive ranges
    assert_eq!(c.query_points(a, T0 + 5000..T0 + 15000, QueryOpts::asc()).unwrap().len(), 10);
    assert_eq!(c.query_points(a, T0 + 5000..=T0 + 15000, QueryOpts::asc()).unwrap().len(), 11);

    // streaming, in order, and stopping early leaves the connection usable
    let (mut rows, mut calls, mut last) = (0, 0, i64::MIN);
    c.query_raw(a, .., QueryOpts::asc(), |t, vals| {
        assert_eq!(t.len(), vals.len());
        assert!(t.iter().all(|&x| x > last));
        last = *t.last().unwrap();
        rows += t.len();
        calls += 1;
        ControlFlow::Continue(())
    })
    .unwrap();
    assert_eq!(rows, N);
    assert!(calls > 1);
    let mut calls = 0;
    c.query_raw(a, .., QueryOpts::desc(), |t, _| {
        assert_eq!(t[0], ts[N - 1]);
        calls += 1;
        ControlFlow::Break(())
    })
    .unwrap();
    assert_eq!(calls, 1);
    assert_eq!(c.query_points(a, ..T0 + 2000, QueryOpts::asc()).unwrap().len(), 2);

    // a series without data, or that does not exist, has no points
    assert!(c.query_points(999_999, .., QueryOpts::asc()).unwrap().is_empty());
}

#[test]
fn aggregates() {
    let srv = Server::start();
    let mut c = srv.client();
    let a = c.series("agg").unwrap();
    let n = 7200; // two hours, one point a second
    let ts: Vec<i64> = (0..n).map(|i| T0 + i * 1000).collect();
    let v: Vec<f32> = (0..n).map(|i| ((i * 7) % 100) as f32 - 50.0).collect();
    c.insert_series(a, &ts, &v).unwrap();

    let bk = c.query_agg(a, T0..T0 + HOUR, MIN).unwrap();
    assert_eq!(bk.len(), 60);
    for (k, b) in bk.iter().enumerate() {
        let w = &v[k * 60..k * 60 + 60];
        assert_eq!(b.start, T0 + k as i64 * MIN);
        assert_eq!(b.count, 60);
        assert!((b.sum - w.iter().map(|&x| x as f64).sum::<f64>()).abs() < 1e-6);
        assert_eq!(b.min, w.iter().copied().fold(f32::INFINITY, f32::min) as f64);
        assert_eq!(b.max, w.iter().copied().fold(f32::NEG_INFINITY, f32::max) as f64);
        assert!((b.avg() - b.sum / 60.0).abs() < 1e-12);
    }

    // one bucket for the whole range starts at the query start
    let all = c.query_agg(a, T0 - 1..T0 + DAY, 0).unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!((all[0].start, all[0].count), (T0 - 1, n as u64));
    assert!(c.query_agg(a, T0 + DAY.., MIN).unwrap().is_empty());
    assert_eq!(c.query_agg(a, T0..T0 + 1, -5).unwrap_err().status(), Some(Status::Invalid));
}

#[test]
fn group_by_calendar_in_a_time_zone() {
    let srv = Server::start();
    let mut c = srv.client();
    let a = c.series("grp").unwrap();
    // three days, one point a minute, starting at 23:13 local time in Oslo (UTC+1 in November)
    let n = 3 * 1440;
    let ts: Vec<i64> = (0..n).map(|i| T0 + i * MIN).collect();
    c.insert_series(a, &ts, &vec![1.0; n as usize]).unwrap();

    let oslo = Group::new(Unit::Day).timezone("Europe/Oslo");
    let days = c.query_group(a, .., Some(&oslo), QueryOpts::asc()).unwrap();
    assert_eq!(days.len(), 4);
    for d in &days {
        assert_eq!(d.start.rem_euclid(DAY), 23 * HOUR, "local midnight is 23:00 UTC");
    }
    assert_eq!(days.iter().map(|d| d.count).sum::<u64>(), n as u64);
    assert_eq!(days[0].count, 47); // 23:13 .. 23:59
    assert_eq!(days[1].count, 1440);

    // newest first with a limit
    let last2 = c.query_group(a, .., Some(&oslo), QueryOpts::desc().limit(2)).unwrap();
    assert_eq!(last2.len(), 2);
    assert_eq!(last2[0].start - last2[1].start, DAY);
    assert_eq!(last2[0], days[3]);
    assert!(c.query_group(a, .., Some(&oslo), QueryOpts::asc().limit(0)).unwrap().is_empty());

    // in UTC days start at midnight; 6-hour buckets in a fixed offset
    let utc = c.query_group(a, .., Some(&Group::new(Unit::Day)), QueryOpts::asc()).unwrap();
    assert!(utc.iter().all(|d| d.start % DAY == 0));
    let six = Group::new(Unit::Hour).count(6).timezone("+02:00");
    let q = c.query_group(a, .., Some(&six), QueryOpts::asc()).unwrap();
    assert!(q.iter().all(|b| (b.start + 2 * HOUR).rem_euclid(6 * HOUR) == 0));
    assert_eq!(q.iter().map(|b| b.count).sum::<u64>(), n as u64);

    // no grouping: one bucket for the whole range
    let one = c.query_group(a, T0.., None, QueryOpts::asc()).unwrap();
    assert_eq!((one.len(), one[0].start, one[0].count), (1, T0, n as u64));

    // an unknown zone is an invalid argument
    let bad = Group::new(Unit::Day).timezone("Mars/Olympus_Mons");
    assert_eq!(c.query_group(a, .., Some(&bad), QueryOpts::asc()).unwrap_err().status(), Some(Status::Invalid));
    let bad = Group::new(Unit::Day).timezone("Europe\0Oslo");
    assert!(matches!(c.query_group(a, .., Some(&bad), QueryOpts::asc()), Err(Error::InvalidArgument(_))));
}

#[test]
fn maintenance_and_stats() {
    let srv = Server::start();
    let mut c = srv.client();
    let a = c.series("m").unwrap();
    for k in 0..4 {
        let ts: Vec<i64> = (0..1000).map(|i| T0 + (k * 1000 + i) * 1000).collect();
        c.insert_series(a, &ts, &vec![k as f32; 1000]).unwrap();
        c.flush().unwrap();
    }
    c.commit().unwrap();
    let st = c.stats().unwrap();
    assert_eq!(st.memtable_rows, 0);
    assert_eq!(st.rows, 4000);
    assert!(st.parts >= 1 && st.partitions >= 1 && st.bytes > 0);
    c.optimize().unwrap();
    let st = c.stats().unwrap();
    assert_eq!(st.parts, st.partitions);
    assert_eq!(c.query_agg(a, .., 0).unwrap()[0].count, 4000);
}

#[test]
fn errors() {
    let srv = Server::start();
    let mut c = srv.client();
    let a = c.series("e").unwrap();

    // rejected before sending; the connection stays usable
    let e = c.insert_series(a, &[T0, T0 + 1], &[1.0]).unwrap_err();
    assert!(matches!(e, Error::InvalidArgument(_)));
    assert_eq!(e.code(), -4);
    assert!(matches!(c.insert(&[a], &[T0, T0 + 1], &[1.0, 2.0]), Err(Error::InvalidArgument(_))));
    assert!(matches!(c.insert_series(u32::MAX, &[T0], &[1.0]), Err(Error::InvalidArgument(_))));
    assert!(matches!(c.series(""), Err(Error::InvalidArgument(_))));
    assert!(matches!(c.series("a\0b"), Err(Error::InvalidArgument(_))));
    assert!(matches!(c.series(&"x".repeat(70_000)), Err(Error::InvalidArgument(_))));

    // a server error on an insert to a series that does not exist
    let e = c.insert_series(999_999, &[T0], &[1.0]).unwrap_err();
    assert_eq!(e.status(), Some(Status::Invalid));
    assert_eq!(e.to_string(), "server error -4: invalid argument");

    // a pipelined error is reported by wait, once, and the good inserts land
    c.insert_series_async(a, &[T0], &[1.0]).unwrap();
    c.insert_series_async(999_999, &[T0], &[1.0]).unwrap();
    c.insert_series_async(a, &[T0 + 1000], &[2.0]).unwrap();
    assert_eq!(c.wait().unwrap_err().status(), Some(Status::Invalid));
    c.wait().unwrap();
    assert_eq!(c.query_points(a, .., QueryOpts::asc()).unwrap().len(), 2);

    // ...or by the next call that reads a reply
    c.insert_series_async(999_999, &[T0], &[1.0]).unwrap();
    assert_eq!(c.stats().unwrap_err().status(), Some(Status::Invalid));
    c.stats().unwrap();
    c.close().unwrap();

    // nothing listening
    let port = srv.port;
    drop(srv);
    let e = Client::connect(("127.0.0.1", port)).unwrap_err();
    assert!(matches!(e, Error::Io(_)));
    assert_eq!(e.code(), -1);
}

#[test]
fn server_going_away_breaks_the_client() {
    let mut srv = Server::start();
    let mut c = srv.client();
    c.series("x").unwrap();
    assert!(srv.stop().success());
    assert!(matches!(c.stats(), Err(Error::Io(_))));
    assert!(matches!(c.stats(), Err(Error::Io(_))));
}

#[test]
fn concurrent_clients() {
    let srv = Server::start();
    let mut c = srv.client();
    let ids: Vec<u32> = (0..8).map(|i| c.series(&format!("vib.{i}")).unwrap()).collect();
    std::thread::scope(|s| {
        for &id in &ids {
            let addr = srv.addr();
            s.spawn(move || {
                let mut c = Client::connect(addr).unwrap();
                for round in 0..50 {
                    let ts: Vec<i64> = (0..1000).map(|i| T0 + (round * 1000 + i) * 1000).collect();
                    c.insert_series_async(id, &ts, &[id as f32; 1000]).unwrap();
                }
                c.wait().unwrap();
                let b = c.query_agg(id, .., 0).unwrap();
                assert_eq!((b.len(), b[0].count, b[0].min, b[0].max), (1, 50_000, id as f64, id as f64));
            });
        }
    });
}
