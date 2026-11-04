//! Packed, zstd-compressed inserts (protocol version 3): the server stores
//! what it unpacks exactly as the plain arrays, and compression can be turned off.

mod common;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

use common::{Server, T0};
use tsdb_client::{Bitmap, Client, Column, ColumnRef, Error, QueryOpts, ValueType, Values};

/// Rows meant to trip the encoding: timestamps that jump backwards and across
/// the whole i64 range, extreme values, NaN payloads and -0.0.
fn awkward_ts(n: usize) -> Vec<i64> {
    (0..n as i64)
        .map(|i| match i % 11 {
            0 => i64::MIN + 1 + i,
            5 => i64::MAX - i,
            7 => -T0 - i,
            _ => T0 + i * 1000 + (i % 3) * 7,
        })
        .collect()
}

fn columns(n: usize) -> Vec<(ValueType, Column)> {
    let valid: Bitmap = (0..n).map(|i| i % 13 != 4).collect();
    let with = |values: Values| Column { values, valid: Some(valid.clone()) };
    let f32s = (0..n).map(|i| match i % 5 {
        0 => f32::from_bits(0x7fc0_0001 + i as u32), // a NaN that is a value
        1 => -0.0,
        2 => f32::MAX,
        _ => i as f32 * 0.25,
    });
    vec![
        (ValueType::Float32, with(Values::Float32(f32s.collect()))),
        (ValueType::Float64, with(Values::Float64((0..n).map(|i| (i as f64).sqrt() * -1e300).collect()))),
        (
            ValueType::Int64,
            with(Values::Int64((0..n as i64).map(|i| if i % 2 == 0 { i64::MAX - i } else { i }).collect())),
        ),
        (ValueType::Bool, with(Values::Bool((0..n).map(|i| i % 3 == 0).collect()))),
        (ValueType::State, with(Values::State((0..n as u32).map(|i| 1 + i % 3).collect()))),
    ]
}

/// The stored rows, floats compared by their bits.
fn stored(c: &mut Client, id: u64) -> (Vec<i64>, Vec<Option<u64>>) {
    let r = c.query_rows(id, .., QueryOpts::asc()).unwrap();
    let v = &r.values;
    let bits: Vec<Option<u64>> = match &v.values {
        Values::Float32(_) => v.iter::<f32>().unwrap().map(|x| x.map(|x| x.to_bits() as u64)).collect(),
        Values::Float64(_) => v.iter::<f64>().unwrap().map(|x| x.map(f64::to_bits)).collect(),
        Values::Int64(_) => v.iter::<i64>().unwrap().map(|x| x.map(|x| x as u64)).collect(),
        Values::Bool(_) => v.iter::<bool>().unwrap().map(|x| x.map(u64::from)).collect(),
        Values::State(_) => v.iter::<u32>().unwrap().map(|x| x.map(u64::from)).collect(),
        _ => unreachable!(),
    };
    (r.ts, bits)
}

#[test]
fn packed_and_plain_store_the_same() {
    let srv = Server::start();
    let mut packed = srv.client();
    let mut plain = srv.client();
    plain.set_compression(0).unwrap();
    assert_eq!((packed.compression(), plain.compression()), (tsdb_client::DEFAULT_COMPRESSION, 0));
    let n = 3001; // not a multiple of 8: a partial last bitmap byte
    let ts = awkward_ts(n);
    for (ty, col) in columns(n) {
        let ids: Vec<u64> = ["p", "q", "mp0", "mp1", "mp2", "mq0", "mq1", "mq2"]
            .iter()
            .map(|s| packed.series_create(&format!("{s}.{ty}"), ty, None).unwrap())
            .collect();
        packed.insert_series_values(ids[0], &ts, &col).unwrap();
        plain.insert_series_values(ids[1], &ts, &col).unwrap();
        assert_eq!(stored(&mut packed, ids[0]), stored(&mut plain, ids[1]), "{ty}");
        assert_eq!(stored(&mut packed, ids[0]).1.iter().filter(|v| v.is_none()).count(), col.null_count(), "{ty}");

        // many series, ids going up and down
        let pick = |base: usize| -> Vec<u64> { (0..n).map(|i| ids[base + [2, 0, 1][i % 3]]).collect() };
        packed.insert_values(&pick(2), &ts, &col).unwrap();
        plain.insert_values(&pick(5), &ts, &col).unwrap();
        for k in 0..3 {
            let (a, b) = (stored(&mut packed, ids[2 + k]), stored(&mut plain, ids[5 + k]));
            assert!(!a.0.is_empty());
            assert_eq!(a, b, "{ty} series {k}");
        }
    }

    // the float32 calls go packed too
    let (a, b) = (packed.series("f.packed").unwrap(), packed.series("f.plain").unwrap());
    let v: Vec<f32> = (0..n).map(|i| (i % 17) as f32 - 3.5).collect();
    let before = packed.bytes_sent();
    packed.insert_series(a, &ts, &v).unwrap();
    let packed_bytes = packed.bytes_sent() - before;
    let before = plain.bytes_sent();
    plain.insert_series(b, &ts, &v).unwrap();
    assert_eq!(plain.bytes_sent() - before, (9 + 16 + n * 12) as u64); // the version 1 INSERT frame
    assert!(packed_bytes < (n * 12 / 2) as u64, "{packed_bytes} bytes packed");
    assert_eq!(
        packed.query_points(a, .., QueryOpts::asc()).unwrap(),
        plain.query_points(b, .., QueryOpts::asc()).unwrap()
    );
}

#[test]
fn set_compression() {
    let srv = Server::start();
    let mut c = srv.client();
    let id = c.series_create("x", ValueType::Int64, None).unwrap();
    assert!(matches!(c.set_compression(1000), Err(Error::InvalidArgument(_))));
    assert_eq!(c.compression(), 1);

    // level 0: exactly the plain INSERT_VALUES frame, many-series layout with a bitmap
    c.set_compression(0).unwrap();
    let n = 10;
    let ts: Vec<i64> = (0..n as i64).map(|i| T0 + i).collect();
    let v: Vec<i64> = (0..n as i64).collect();
    let valid: Bitmap = (0..n).map(|i| i != 3).collect();
    let before = c.bytes_sent();
    c.insert_values(&vec![id; n], &ts, ColumnRef::from(&v).with_validity(&valid)).unwrap();
    let body = 16 + n * 8 + n * 8 + 2; // header, ts, values, bitmap
    assert_eq!(c.bytes_sent() - before, (9 + body.next_multiple_of(8) + n * 8) as u64);

    // higher and negative levels compress too, and store the same
    for (k, level) in [3, 19, -5].into_iter().enumerate() {
        c.set_compression(level).unwrap();
        let t: Vec<i64> = ts.iter().map(|t| t + 1000 * (k as i64 + 1)).collect();
        c.insert_series_values(id, &t, ColumnRef::from(&v).with_validity(&valid)).unwrap();
    }
    let rows = c.query_rows(id, .., QueryOpts::asc()).unwrap();
    assert_eq!(rows.len(), 4 * n);
    let want: Vec<Option<i64>> = (0..4).flat_map(|_| (0..n as i64).map(|i| (i != 3).then_some(i))).collect();
    assert_eq!(rows.values.to_options::<i64>().unwrap(), want);
}

#[test]
fn a_pipelined_failure_is_reported_by_the_next_waiting_call() {
    let srv = Server::start();
    let mut c = srv.client();
    let id = c.series("a").unwrap();
    c.set_compression(0).unwrap();
    c.insert_series_async(999_999, &[T0], &[1.0]).unwrap(); // fails on the server
    c.set_compression(1).unwrap();
    // the connect has asked HELLO already: the packed insert is sent, and its call collects that failure
    assert_eq!(c.insert_series(id, &[T0], &[1.0]).unwrap_err().status(), Some(tsdb_client::Status::Invalid));
    c.insert_series(id, &[T0 + 1], &[2.0]).unwrap();
    assert_eq!(c.query_points(id, .., QueryOpts::asc()).unwrap().values, vec![1.0, 2.0]);
    assert_eq!(c.protocol_version().unwrap(), tsdb_client::PROTOCOL_VERSION);
}

/// One request a fake server saw: op and, for INSERT_VALUES, the flags and body.
struct Seen {
    op: u8,
    payload: Vec<u8>,
}

/// A server that answers HELLO with `version` (no login, no mechanisms) and everything else with OK.
fn fake_server(version: u32) -> (String, thread::JoinHandle<Vec<Seen>>) {
    let hello = [version.to_le_bytes(), [0; 4]].concat();
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
            let body: &[u8] = if h[8] == 1 { &hello } else { &[] };
            let mut reply = ((9 + body.len()) as u32).to_le_bytes().to_vec();
            reply.extend_from_slice(&h[4..8]);
            reply.extend_from_slice(&[0, 0, 0, 0, 0]);
            reply.extend_from_slice(body);
            write_all(&mut s, &reply);
            seen.push(Seen { op: h[8], payload });
        }
        seen
    });
    (addr, h)
}

fn write_all(s: &mut TcpStream, b: &[u8]) {
    s.write_all(b).unwrap();
}

fn talk(version: u32, level: i32) -> Vec<Seen> {
    let (addr, h) = fake_server(version);
    let mut c = Client::connect(addr).unwrap();
    c.set_compression(level).unwrap();
    c.insert_series(1, &[1000, 999], &[1.0, 2.0]).unwrap();
    c.insert_series_values(1, &[1000, 999], &[0x0102i64, -1]).unwrap();
    c.insert_series(1, &[], &[]).unwrap(); // empty inserts stay plain
    c.close().unwrap();
    h.join().unwrap()
}

#[test]
fn level_zero_sends_plain_arrays() {
    let seen = talk(12, 0);
    let ops: Vec<u8> = seen.iter().map(|s| s.op).collect();
    assert_eq!(ops, vec![1, 4, 14, 4]); // HELLO at connect, then version 1 INSERT, plain INSERT_VALUES
    assert_eq!(seen[2].payload[5], 0); // flags
    assert_eq!(seen[2].payload.len(), 16 + 2 * 16);
}

#[test]
fn new_servers_get_packed_frames() {
    let seen = talk(12, 1);
    let ops: Vec<u8> = seen.iter().map(|s| s.op).collect();
    assert_eq!(ops, vec![1, 14, 14, 4]); // the float32 insert goes as packed INSERT_VALUES
    for (s, ty) in [(&seen[1], 0u8), (&seen[2], 2)] {
        let p = &s.payload;
        assert_eq!(&p[..8], &[2, 0, 0, 0, ty, 2, 0, 0]); // n, type, PACKED, 0
        assert_eq!(&p[8..16], &[1, 0, 0, 0, 0, 0, 0, 0]); // series
        let raw_len = u32::from_le_bytes(p[16..20].try_into().unwrap()) as usize;
        let raw = zstd::bulk::decompress(&p[20..], raw_len).unwrap();
        assert_eq!(raw.len(), raw_len);
        assert_eq!(&raw[..3], &[0xd0, 0x0f, 0x01]); // zigzag deltas 1000, -1
        if ty == 2 {
            let mut planes = vec![0x02, 0xff, 0x01, 0xff];
            planes.extend([0x00, 0xff].repeat(6));
            assert_eq!(&raw[3..], &planes[..]);
        } else {
            let (a, b) = (1.0f32.to_le_bytes(), 2.0f32.to_le_bytes());
            let planes: Vec<u8> = (0..4).flat_map(|k| [a[k], b[k]]).collect();
            assert_eq!(&raw[3..], &planes[..]);
        }
    }
}
