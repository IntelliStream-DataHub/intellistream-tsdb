//! Grouping by local periods (protocol version 4): the server knows UTC only,
//! so the client turns a time zone into bucket bounds (QUERY_BOUNDS). Against
//! a real tsdbd the buckets are those of the C client
//! (`fixtures/group_by_zone.txt`); the frames sent are checked against a fake
//! server.

mod common;

use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

use common::Server;
use tsdb_client::{Client, Error, Group, QueryOpts, Unit, calendar_bounds};

const HOUR: i64 = 3_600_000;
const DAY: i64 = 24 * HOUR;

/// The rows of `fixtures/group_by_zone.c`: every 30 minutes around the 2024 DST changes.
fn rows() -> Vec<i64> {
    let spans = [(1_711_670_400_000i64, 1_712_016_000_000i64), (1_729_814_400_000, 1_730_160_000_000)];
    spans.iter().flat_map(|&(a, b)| (a..b).step_by(1_800_000)).collect()
}

fn load(c: &mut Client) -> u64 {
    let id = c.series("oslo").unwrap();
    let ts = rows();
    c.insert_series(id, &ts, &vec![1.0; ts.len()]).unwrap();
    id
}

fn unit(code: &str) -> Unit {
    match code {
        "3" => Unit::Hour,
        "4" => Unit::Day,
        "5" => Unit::Week,
        u => panic!("unit {u}"),
    }
}

#[test]
fn local_groups_are_those_of_the_c_client() {
    let srv = Server::start();
    let mut c = srv.client();
    assert_eq!(c.protocol_version().unwrap(), tsdb_client::PROTOCOL_VERSION);
    let id = load(&mut c);
    let expected = include_str!("fixtures/group_by_zone.txt");
    for line in expected.lines() {
        let (head, buckets) = line.split_once(':').unwrap();
        let h: Vec<&str> = head.split(' ').collect();
        let group = Group::new(unit(h[1])).count(h[2].parse().unwrap()).timezone(h[0]);
        let got: Vec<String> = c
            .query_group(id, .., Some(&group), QueryOpts::asc())
            .unwrap()
            .iter()
            .map(|b| format!("({},{})", b.start, b.count))
            .collect();
        assert_eq!(got.join(" "), buckets.trim(), "{head}");
    }
}

#[test]
fn oslo_days_begin_at_local_midnight() {
    let srv = Server::start();
    let mut c = srv.client();
    let id = load(&mut c);
    let oslo = Group::new(Unit::Day).timezone("Europe/Oslo");
    let days = c.query_group(id, .., Some(&oslo), QueryOpts::asc()).unwrap();
    assert_eq!(days.len(), 10);
    for d in &days {
        // 23:00 UTC in winter (UTC+1), 22:00 in summer (UTC+2)
        let summer = (1_711_846_800_000..1_729_990_800_000).contains(&d.start);
        assert_eq!(d.start.rem_euclid(DAY), if summer { 22 * HOUR } else { 23 * HOUR }, "{}", d.start);
    }
    // the day the clocks go forward has 23 hours, the one they go back 25
    let len = |a: &tsdb_client::Bucket, b: &tsdb_client::Bucket| b.start - a.start;
    assert_eq!((len(&days[2], &days[3]), days[2].count), (23 * HOUR, 46));
    assert_eq!((len(&days[7], &days[8]), days[7].count), (25 * HOUR, 50));
    assert_eq!(days.iter().map(|d| d.count).sum::<u64>(), rows().len() as u64);

    // the bounds the client sent, and the same query through query_bounds
    let (first, last) = (rows()[0], *rows().last().unwrap());
    let bounds = calendar_bounds("Europe/Oslo", Unit::Day, 1, first..=last).unwrap();
    let starts: Vec<i64> = days.iter().map(|d| d.start).collect();
    assert!(starts.iter().all(|s| bounds.contains(s)));
    assert_eq!(c.query_bounds(id, .., &bounds, QueryOpts::asc()).unwrap(), days);

    // newest first, a limit, a range inside the data
    let last2 = c.query_group(id, .., Some(&oslo), QueryOpts::desc().limit(2)).unwrap();
    assert_eq!(last2, [days[9], days[8]]);
    assert!(c.query_group(id, .., Some(&oslo), QueryOpts::asc().limit(0)).unwrap().is_empty());
    let mid = c.query_group(id, days[1].start + HOUR..days[3].start, Some(&oslo), QueryOpts::asc()).unwrap();
    let mid: Vec<(i64, u64)> = mid.iter().map(|b| (b.start, b.count)).collect();
    assert_eq!(mid, [(days[1].start, 46), (days[2].start, 46)]);

    // a series without rows has no buckets; an unknown zone is an error all the same
    let empty = c.series("empty").unwrap();
    assert!(c.query_group(empty, .., Some(&oslo), QueryOpts::asc()).unwrap().is_empty());
    let mars = Group::new(Unit::Day).timezone("Mars/Olympus_Mons");
    for s in [id, empty] {
        let e = c.query_group(s, .., Some(&mars), QueryOpts::asc()).unwrap_err();
        assert!(matches!(e, Error::InvalidArgument(_)) && e.code() == -4, "{e}");
    }
    // the connection is still good after the errors
    assert_eq!(c.query_group(id, .., Some(&oslo), QueryOpts::asc()).unwrap(), days);
}

#[test]
fn query_bounds_buckets_by_any_bounds() {
    let srv = Server::start();
    let mut c = srv.client();
    let id = c.series("b").unwrap();
    let ts: Vec<i64> = (0..10).map(|i| i * 1000).collect();
    c.insert_series(id, &ts, &(0..10).map(|i| i as f32).collect::<Vec<_>>()).unwrap();
    // uneven buckets; rows before the first bound belong to none
    let b = c.query_bounds(id, .., &[2000, 3000, 7000], QueryOpts::asc()).unwrap();
    let got: Vec<(i64, u64, f64)> = b.iter().map(|b| (b.start, b.count, b.sum)).collect();
    assert_eq!(got, [(2000, 1, 2.0), (3000, 4, 18.0), (7000, 3, 24.0)]);
    let b = c.query_bounds(id, ..8000, &[2000, 3000, 7000], QueryOpts::desc().limit(2)).unwrap();
    assert_eq!(b.iter().map(|b| (b.start, b.count)).collect::<Vec<_>>(), [(7000, 1), (3000, 4)]);
    assert!(c.query_bounds(id, .., &[2000], QueryOpts::asc().limit(0)).unwrap().is_empty());
    for bad in [&[][..], &[3000, 3000], &[3000, 2000]] {
        assert!(matches!(c.query_bounds(id, .., bad, QueryOpts::asc()), Err(Error::InvalidArgument(_))));
    }
    assert!(c.query_bounds(1 << 32 | 12345, .., &[0], QueryOpts::asc()).unwrap().is_empty());
}

#[test]
fn utc_groups_still_go_to_the_server() {
    let srv = Server::start();
    let mut c = srv.client();
    let id = load(&mut c);
    let utc = c.query_group(id, .., Some(&Group::new(Unit::Day).timezone("UTC")), QueryOpts::asc()).unwrap();
    assert!(utc.iter().all(|d| d.start % DAY == 0));
    assert_eq!(utc, c.query_group(id, .., Some(&Group::new(Unit::Day)), QueryOpts::asc()).unwrap());
    // fixed offsets work through bounds; hours align in the offset
    let six = Group::new(Unit::Hour).count(6).timezone("+02:00");
    let q = c.query_group(id, .., Some(&six), QueryOpts::asc()).unwrap();
    assert!(q.iter().all(|b| (b.start + 2 * HOUR).rem_euclid(6 * HOUR) == 0));
}

/// A server that answers HELLO with `version`, QUERY_VALUES with one row at
/// `FIRST` (ascending) or `LAST` (descending), QUERY_BUCKETS and QUERY_BOUNDS
/// with no buckets, and anything else with an empty OK; returns the ops and
/// payloads it saw.
/// Each request a fake server saw: op and payload.
type Seen = Vec<(u8, Vec<u8>)>;

fn fake_server(version: u32) -> (String, thread::JoinHandle<Seen>) {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    let h = thread::spawn(move || {
        let (mut s, _) = l.accept().unwrap();
        let mut seen = Vec::new();
        let mut h = [0u8; 9];
        while s.read_exact(&mut h).is_ok() {
            let len = u32::from_le_bytes(h[0..4].try_into().unwrap()) as usize;
            let mut payload = vec![0; len - 5];
            s.read_exact(&mut payload).unwrap();
            let body: Vec<u8> = match h[8] {
                1 => version.to_le_bytes().to_vec(),
                15 => {
                    let ts = if payload[24] == 1 { LAST } else { FIRST };
                    let mut chunk = vec![1, 0, 0, 0, 1, 0, 0, 0];
                    chunk.extend_from_slice(&ts.to_le_bytes());
                    chunk.extend_from_slice(&1.0f32.to_le_bytes());
                    chunk
                }
                18 | 20 => vec![0; 4],
                _ => Vec::new(),
            };
            let mut reply = ((9 + body.len()) as u32).to_le_bytes().to_vec();
            reply.extend_from_slice(&h[4..8]);
            reply.extend_from_slice(&[0, 0, 0, 0, 0]);
            reply.extend_from_slice(&body);
            s.write_all(&reply).unwrap();
            seen.push((h[8], payload));
        }
        seen
    });
    (addr, h)
}

const FIRST: i64 = 1_711_670_400_000; // 2024-03-29 00:00 UTC
const LAST: i64 = 1_712_016_000_000;

#[test]
fn local_groups_go_as_bounds() {
    let (addr, h) = fake_server(tsdb_client::PROTOCOL_VERSION);
    let mut c = Client::connect(addr).unwrap();
    let oslo = Group::new(Unit::Day).timezone("Europe/Oslo");
    assert!(c.query_group(7, 1000.., Some(&oslo), QueryOpts::desc().limit(3)).unwrap().is_empty());
    // UTC stays a QUERY_BUCKETS
    assert!(c.query_group(7, .., Some(&Group::new(Unit::Day).timezone("UTC")), QueryOpts::asc()).unwrap().is_empty());
    c.close().unwrap();
    let seen = h.join().unwrap();
    // HELLO, the first and the last row, QUERY_BOUNDS; then QUERY_BUCKETS
    assert_eq!(seen.iter().map(|s| s.0).collect::<Vec<_>>(), [1, 15, 15, 20, 18]);
    for (s, order) in [(&seen[1].1, 0), (&seen[2].1, 1)] {
        assert_eq!(i64::from_le_bytes(s[8..16].try_into().unwrap()), 1000);
        assert_eq!((s[24], u64::from_le_bytes(s[25..33].try_into().unwrap())), (order, 1));
    }
    let p = &seen[3].1;
    let expected = calendar_bounds("Europe/Oslo", Unit::Day, 1, FIRST..=LAST).unwrap();
    assert_eq!(expected.len(), 5);
    assert_eq!(p.len(), 37 + 8 * expected.len());
    assert_eq!(u64::from_le_bytes(p[0..8].try_into().unwrap()), 7);
    assert_eq!(i64::from_le_bytes(p[8..16].try_into().unwrap()), 1000); // the query's range, not the rows'
    assert_eq!(i64::from_le_bytes(p[16..24].try_into().unwrap()), i64::MAX);
    assert_eq!((p[24], u64::from_le_bytes(p[25..33].try_into().unwrap())), (1, 3)); // desc, limit
    assert_eq!(u32::from_le_bytes(p[33..37].try_into().unwrap()) as usize, expected.len());
    let sent: Vec<i64> = p[37..].chunks_exact(8).map(|b| i64::from_le_bytes(b.try_into().unwrap())).collect();
    assert_eq!(sent, expected);
    assert_eq!(&seen[4].1[48..], b"UTC");
}
