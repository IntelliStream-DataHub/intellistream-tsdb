//! 64-bit series ids on the wire (protocol version 11): the frames carry every bit of an id (checked on a fake server
//! that records what it receives), a server of an older protocol is refused when the client connects, and tsdbd, which
//! gives ids the volume in bits 32-47, answers an id like `a + 2^32` (another volume) as an unknown series, never as `a`.

mod common;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

use common::{Server, T0};
use tsdb_client::{Bitmap, Client, ColumnRef, Error, Group, MultiOpts, QueryOpts, Status, TwaOpts, Unit, ValueType};

const A: u64 = 0x1_0000_0005;
const B: u64 = 0xFFFF_FFFF_0000_0001;
const MANY: u64 = u64::MAX;

/// What a fake server saw after the HELLO: op and payload.
type Seen = Vec<(u8, Vec<u8>)>;

/// A server that answers HELLO with `version` (from 12 on with no login and no mechanisms), SERIES and SERIES_CREATE with the id `A + 2`, the others with a valid empty answer, and everything else
/// with an empty OK (QUERY_MULTI is the one op whose answer the tests do not shape: its client fails after the request
/// went out, which is all they look at).
/// Returns the address and the thread, which ends when the client closes the connection and gives the requests.
fn fake_server(version: u32) -> (String, thread::JoinHandle<(Option<Vec<u8>>, Seen)>) {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    let h = thread::spawn(move || {
        let (mut s, _) = l.accept().unwrap();
        let (mut hello, mut seen) = (None, Vec::new());
        let mut h = [0u8; 9];
        while s.read_exact(&mut h).is_ok() {
            let len = u32::from_le_bytes(h[0..4].try_into().unwrap()) as usize;
            let mut payload = vec![0; len - 5];
            s.read_exact(&mut payload).unwrap();
            let body: Vec<u8> = match h[8] {
                1 if version >= 12 => [version.to_le_bytes(), [0; 4]].concat(),
                1 => version.to_le_bytes().to_vec(),
                2 | 12 => (A + 2).to_le_bytes().to_vec(),
                5 => vec![1, 0, 0, 0, 0],                // a raw chunk of no rows, the last
                15 | 21 => vec![1, 0, 0, 0, 0, 0, 0, 0], // a values chunk of no rows, the last
                18 | 20 | 22 | 16 => vec![0; 4],         // no buckets; a state code
                3 | 17 => vec![0; 2],                    // an empty name
                13 => vec![0],                           // a type
                28 => vec![0; 8],                        // no retention
                _ => Vec::new(),
            };
            let mut reply = ((9 + body.len()) as u32).to_le_bytes().to_vec();
            reply.extend_from_slice(&h[4..8]);
            reply.extend_from_slice(&[0, 0, 0, 0, 0]);
            reply.extend_from_slice(&body);
            if s.write_all(&reply).is_err() {
                break;
            }
            if h[8] == 1 {
                hello = Some(payload);
            } else {
                seen.push((h[8], payload));
            }
        }
        (hello, seen)
    });
    (addr, h)
}

/// Connects to a fake server of the current protocol, runs `f` and returns the requests it sent.
fn requests(level: i32, f: impl FnOnce(&mut Client)) -> Seen {
    let (addr, h) = fake_server(tsdb_client::PROTOCOL_VERSION);
    let mut c = Client::connect(addr).unwrap();
    c.set_compression(level).unwrap();
    f(&mut c);
    drop(c);
    let (hello, seen) = h.join().unwrap();
    assert_eq!(hello, Some(13u32.to_le_bytes().to_vec()));
    seen
}

fn u64_at(p: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(p[at..at + 8].try_into().unwrap())
}

fn u32_at(p: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(p[at..at + 4].try_into().unwrap())
}

#[test]
fn a_server_older_than_12_is_refused_at_connect() {
    for version in [11, 10, 7, 3, 1, 0] {
        let (addr, h) = fake_server(version);
        let Err(e) = Client::connect(addr) else { panic!("protocol {version} must be refused") };
        let want = format!("tsdbd speaks protocol {version}; this client needs 12: upgrade the server");
        assert!(matches!(&e, Error::Protocol(m) if *m == want), "{e:?}");
        assert!(e.to_string().contains(&want));
        assert_eq!(e.code(), -3);
        let (hello, seen) = h.join().unwrap();
        assert_eq!(hello, Some(13u32.to_le_bytes().to_vec())); // the client said 13
        assert!(seen.is_empty(), "nothing but HELLO was sent");
    }
    // a newer server is fine: the client speaks what it knows
    let (addr, h) = fake_server(13);
    let mut c = Client::connect(addr).unwrap();
    assert_eq!(c.protocol_version().unwrap(), 13);
    drop(c);
    h.join().unwrap();
}

#[test]
fn a_server_older_than_12_is_refused_for_a_stream_too() {
    let (addr, h) = fake_server(11);
    let s = TcpStream::connect(addr).unwrap();
    assert!(matches!(Client::from_stream(s), Err(Error::Protocol(m)) if m.contains("needs 12")));
    h.join().unwrap();
}

#[test]
fn series_replies_carry_a_u64() {
    let seen = requests(0, |c| {
        assert_eq!(c.series("x").unwrap(), A + 2);
        assert_eq!(c.series_create("y", ValueType::Int64, None).unwrap(), A + 2);
        assert_eq!(c.series_lookup("x").unwrap(), A + 2);
    });
    assert_eq!(seen.iter().map(|s| s.0).collect::<Vec<_>>(), [2, 12, 2]);
}

#[test]
fn plain_inserts_carry_the_whole_id() {
    let seen = requests(0, |c| {
        // INSERT, one series: n, 0, series, ts, v
        c.insert_series(A, &[10, 20], &[1.0f32, 2.0]).unwrap();
        // INSERT, a series per row: 3 rows, the ids after the values padded to 8
        c.insert(&[A, B, A], &[10, 20, 30], &[1.0f32, 2.0, 3.0]).unwrap();
        // INSERT_VALUES: n, type, flags, 0, series
        c.insert_series_values(A, &[10, 20], &[1i64, 2]).unwrap();
        let valid: Bitmap = [true, false].into_iter().collect();
        c.insert_values(&[B, A], &[10, 20], ColumnRef::from(&[1i64, 2][..]).with_validity(&valid)).unwrap();
    });
    assert_eq!(seen.iter().map(|s| s.0).collect::<Vec<_>>(), [4, 4, 14, 14]);

    let p = &seen[0].1;
    assert_eq!(p.len(), 16 + 2 * 8 + 2 * 4);
    assert_eq!((u32_at(p, 0), u32_at(p, 4), u64_at(p, 8)), (2, 0, A));
    assert_eq!(i64::from_le_bytes(p[16..24].try_into().unwrap()), 10);

    let p = &seen[1].1;
    assert_eq!(p.len(), 16 + 3 * 8 + 3 * 4 + 4 + 3 * 8); // 4 bytes of padding
    assert_eq!((u32_at(p, 0), u32_at(p, 4), u64_at(p, 8)), (3, 0, MANY));
    assert_eq!(&p[52..56], &[0; 4]);
    assert_eq!([u64_at(p, 56), u64_at(p, 64), u64_at(p, 72)], [A, B, A]);

    let p = &seen[2].1;
    assert_eq!(p.len(), 16 + 2 * 8 + 2 * 8);
    assert_eq!(&p[..8], &[2, 0, 0, 0, ValueType::Int64.code(), 0, 0, 0]); // n, type, flags, 0
    assert_eq!(u64_at(p, 8), A);

    let p = &seen[3].1;
    assert_eq!(p.len(), 16 + 16 + 16 + 1 + 7 + 2 * 8); // header, ts, values, bitmap, padding, ids
    assert_eq!(&p[..8], &[2, 0, 0, 0, ValueType::Int64.code(), 1, 0, 0]); // flags: VALID
    assert_eq!(u64_at(p, 8), MANY);
    assert_eq!(p[48], 0b01);
    assert_eq!(&p[49..56], &[0; 7]);
    assert_eq!([u64_at(p, 56), u64_at(p, 64)], [B, A]);
}

/// The reverse of the packer's series column, written independently of it: LEB128 varints, zigzag, wrapping sum.
fn unpack_ids(mut p: &[u8], n: usize) -> Vec<u64> {
    let (mut prev, mut out) = (0u64, Vec::new());
    for _ in 0..n {
        let (mut v, mut shift) = (0u64, 0);
        loop {
            let b = p[0];
            p = &p[1..];
            v |= ((b & 0x7f) as u64) << shift;
            shift += 7;
            if b & 0x80 == 0 {
                break;
            }
        }
        prev = prev.wrapping_add((v >> 1) ^ (v & 1).wrapping_neg());
        out.push(prev);
    }
    assert!(p.is_empty());
    out
}

#[test]
fn packed_ids_are_wrapping_64_bit_deltas() {
    // up across 2^32, down across it, 0 -> u64::MAX - 1 and back, and a jump of 2^63
    let ids = [A, 3, MANY - 1, 0, 1 << 63, 0, B, B];
    let n = ids.len();
    let ts: Vec<i64> = (1..=n as i64).collect();
    let vals = vec![0i64; n];
    let seen = requests(1, |c| c.insert_values(&ids, &ts, ColumnRef::from(&vals[..])).unwrap());
    assert_eq!(seen.len(), 1);
    let p = &seen[0].1;
    assert_eq!(&p[..8], &[n as u8, 0, 0, 0, ValueType::Int64.code(), 2, 0, 0]); // PACKED
    assert_eq!(u64_at(p, 8), MANY);
    let raw_len = u32_at(p, 16) as usize;
    let raw = zstd::bulk::decompress(&p[20..], raw_len).unwrap();
    assert!(raw_len <= n * (10 + 8) + n * 10, "the bound the server checks");
    // timestamps: n deltas of 1 (zigzag 2), then the 8 byte planes of the values, then the ids
    let col = &raw[n + n * 8..];
    assert_eq!(unpack_ids(col, n), ids);
    // by hand: +(2^32 + 5), -(2^32 + 2), -5 (3 -> u64::MAX - 1 wraps), +2, then 0 -> 2^63 and back are each 10 bytes
    assert_eq!(
        &col[..16],
        &[0x8a, 0x80, 0x80, 0x80, 0x20, 0x83, 0x80, 0x80, 0x80, 0x20, 0x09, 0x04, 0xff, 0xff, 0xff, 0xff]
    );
    // a single id jump from 0 to u64::MAX - 1 is a delta of -2
    let seen = requests(1, |c| c.insert_values(&[MANY - 1, 0], &[1, 2], ColumnRef::from(&[0i64, 0][..])).unwrap());
    let p = &seen[0].1;
    let raw = zstd::bulk::decompress(&p[20..], u32_at(p, 16) as usize).unwrap();
    assert_eq!(&raw[2 + 16..], &[0x03, 0x04]);
}

#[test]
fn lookups_deletes_and_states_carry_the_whole_id() {
    let seen = requests(0, |c| {
        let _ = c.series_name(A);
        let _ = c.series_type(B);
        let _ = c.retention(A);
        let _ = c.state(B, "x");
        let _ = c.state_lookup(B, "x");
        let _ = c.state_name(A, 3);
        c.delete_range(A, 5..9).unwrap();
        c.set_retention(B, 1000).unwrap();
        c.replace_range(A, 5..9, &[6], &[1i64]).unwrap();
    });
    let ops: Vec<u8> = seen.iter().map(|s| s.0).collect();
    assert_eq!(ops, [3, 13, 28, 16, 16, 17, 25, 27, 26]);
    let p = |i: usize| &seen[i].1;
    assert_eq!(p(0).as_slice(), &A.to_le_bytes()); // SERIES_NAME
    assert_eq!(p(1).as_slice(), &B.to_le_bytes()); // SERIES_TYPE
    assert_eq!(p(2).as_slice(), &A.to_le_bytes()); // GET_RETENTION
    assert_eq!(p(3).len(), 8 + 1 + 2 + 1); // STATE: series, create, len, name
    assert_eq!((u64_at(p(3), 0), p(3)[8], p(3)[9]), (B, 1, 1));
    assert_eq!((u64_at(p(4), 0), p(4)[8]), (B, 0));
    assert_eq!((u64_at(p(5), 0), u32_at(p(5), 8), p(5).len()), (A, 3, 12)); // STATE_NAME
    assert_eq!(p(6).len(), 24); // DELETE_RANGE: series, start, end
    assert_eq!((u64_at(p(6), 0), u64_at(p(6), 8), u64_at(p(6), 16)), (A, 5, 9));
    assert_eq!(p(7).len(), 16); // SET_RETENTION: series, limit
    assert_eq!((u64_at(p(7), 0), u64_at(p(7), 8)), (B, 1000));
    let r = p(8); // REPLACE_RANGE: 32 byte header, ts, values
    assert_eq!(r.len(), 32 + 8 + 8);
    assert_eq!(&r[..8], &[1, 0, 0, 0, ValueType::Int64.code(), 0, 0, 0]);
    assert_eq!((u64_at(r, 8), u64_at(r, 16), u64_at(r, 24)), (A, 5, 9));
}

#[test]
fn queries_start_with_a_u64_id() {
    let seen = requests(0, |c| {
        let _ = c.query_points(B, 100..200, QueryOpts::asc());
        let _ = c.query_lttb(A, 100..200, 3, QueryOpts::asc());
        let _ = c.query_lttb(A, 100..200, 3, QueryOpts::desc());
        let _ = c.query_twa(A, 100..200, 0, TwaOpts::step());
        let _ = c.query_twa_bounds(B, 100..200, &[100, 150], TwaOpts::linear());
        let _ = c.query_bounds(A, .., &[0, 10], QueryOpts::asc());
        let _ = c.query_agg(B, 0..10, 5);
        let _ = c.query_group(A, 0..10, Some(&Group::new(Unit::Day)), QueryOpts::asc());
        let _ = c.query_rows(B, 100..200, QueryOpts::asc());
    });
    let ops: Vec<u8> = seen.iter().map(|s| s.0).collect();
    assert_eq!(ops, [5, 21, 21, 22, 22, 20, 18, 18, 15]);
    let p = |i: usize| &seen[i].1;
    assert_eq!(p(0).len(), 8 + 16 + 1 + 8); // series, start, end, order, limit
    assert_eq!((u64_at(p(0), 0), u64_at(p(0), 8), u64_at(p(0), 16)), (B, 100, 200));
    assert_eq!(p(1).len(), 32); // LTTB: series, start, end, threshold
    assert_eq!((u64_at(p(1), 0), u64_at(p(1), 8), u64_at(p(1), 16), u64_at(p(1), 24)), (A, 100, 200, 3));
    assert_eq!(p(2).len(), 41); // with order and limit
    assert_eq!((u64_at(p(2), 0), p(2)[32]), (A, 1));
    assert_eq!(p(3).len(), 60); // TSDB_TWA_HEADER, no bounds
    assert_eq!((u64_at(p(3), 0), u64_at(p(3), 8), u64_at(p(3), 16)), (A, 100, 200));
    assert_eq!(p(4).len(), 60 + 16);
    assert_eq!((u64_at(p(4), 0), u32_at(p(4), 56)), (B, 2)); // n bounds
    assert_eq!(p(5).len(), 37 + 16); // QUERY_BOUNDS: series, start, end, order, limit, n, bounds
    assert_eq!((u64_at(p(5), 0), u32_at(p(5), 33)), (A, 2));
    assert_eq!(u64_at(p(6), 0), B); // QUERY_BUCKETS
    assert_eq!(u64_at(p(7), 0), A);
    assert_eq!((u64_at(p(8), 0), u64_at(p(8), 8), u64_at(p(8), 16)), (B, 100, 200)); // QUERY_VALUES
}

#[test]
fn query_multi_ids_are_u64_and_unpadded() {
    for k in [1usize, 2, 3] {
        let all = [A, B, 7];
        let ids = &all[..k];
        let seen = requests(0, |c| {
            let _ = c.query_rows_multi(ids, 10..20, QueryOpts::asc(), MultiOpts::default());
        });
        assert_eq!(seen.len(), 1);
        let (op, p) = (&seen[0].0, &seen[0].1);
        assert_eq!(*op, 23);
        assert_eq!(u32_at(p, 4), k as u32); // n
        let template = 33; // series, start, end, order, limit
        assert_eq!(p.len(), 24 + k * 8 + template, "no padding after the ids, {k} series");
        for (i, &id) in ids.iter().enumerate() {
            assert_eq!(u64_at(p, 24 + i * 8), id);
        }
        let t = &p[24 + k * 8..];
        assert_eq!((u64_at(t, 0), u64_at(t, 8), u64_at(t, 16)), (MANY, 10, 20)); // the template's series
        assert_eq!(u32_at(p, 16), template as u32); // tlen
    }
}

#[test]
fn many_series_marker_is_not_an_id() {
    let srv = Server::start();
    let mut c = srv.client();
    let e = c.insert_series(u64::MAX, &[T0], &[1.0f32]).unwrap_err();
    assert!(matches!(e, Error::InvalidArgument(_)), "{e:?}");
    assert!(!c.is_broken());
    c.set_compression(0).unwrap();
    assert!(matches!(c.insert_series_values(u64::MAX, &[T0], &[1i64]), Err(Error::InvalidArgument(_))));
    assert!(matches!(c.replace_range(u64::MAX, .., &[T0], &[1i64]), Err(Error::InvalidArgument(_))));
    // inside a per-row array it is an unknown series for tsdbd
    let a = c.series("m").unwrap();
    assert_eq!(c.insert(&[a, u64::MAX], &[T0, T0], &[1.0f32, 2.0]).unwrap_err().status(), Some(Status::Invalid));
}

/// No volume can have this id: a zero volume field (an id cut to 32 bits) or any of bits 48-63.
fn malformed(id: u64) -> bool {
    id >> 48 != 0 || id >> 32 == 0
}

#[test]
fn tsdbd_answers_a_wide_id_as_unknown_not_as_its_low_32_bits() {
    let srv = Server::start();
    let mut c = srv.client();
    let a = c.series_create("wide", ValueType::Int64, None).unwrap();
    let ts = [T0, T0 + 1000, T0 + 2000];
    c.insert_series_values(a, &ts, &[1i64, 2, 3]).unwrap();
    let before = c.query_rows(a, .., QueryOpts::asc()).unwrap();
    assert_eq!(before.len(), 3);
    let a_state = c.series_create("wide.state", ValueType::State, None).unwrap();
    assert_eq!(c.state(a_state, "up").unwrap(), 1);

    // ids that agree with `a` in the low 32 bits and differ above, and ones that agree with nothing
    let wide = [a + (1 << 32), a | 0xFFFF_FFFF_0000_0000, a + (1 << 63)];
    for level in [1, 0] {
        c.set_compression(level).unwrap();
        for &w in &wide {
            let invalid = Some(Status::Invalid);
            assert_eq!(c.insert_series_values(w, &ts, &[9i64, 9, 9]).unwrap_err().status(), invalid, "{w:#x}");
            assert_eq!(c.insert_values(&[a, w, a], &ts, &[9i64, 9, 9][..]).unwrap_err().status(), invalid, "{w:#x}");
            assert_eq!(c.series_name(w).unwrap_err().status(), Some(Status::NotFound), "{w:#x}");
            assert_eq!(c.series_type(w).unwrap_err().status(), Some(Status::NotFound), "{w:#x}");
            assert_eq!(c.retention(w).unwrap_err().status(), Some(Status::NotFound), "{w:#x}");
            assert!(c.state(w, "up").is_err() && c.state_lookup(w, "up").is_err(), "{w:#x}");
            assert!(c.state_name(w, 1).is_err(), "{w:#x}");
            assert!(c.delete_range(w, ..).is_err(), "{w:#x}");
            assert!(c.set_retention(w, 1000).is_err(), "{w:#x}");
            assert!(c.replace_range(w, .., &[T0], &[5i64]).is_err(), "{w:#x}");
            if malformed(w) {
                // no volume can have it: queries say so rather than answer an empty range
                assert_eq!(c.query_rows(w, .., QueryOpts::asc()).unwrap_err().status(), invalid, "{w:#x}");
                assert_eq!(c.query_agg(w, .., 0).unwrap_err().status(), invalid, "{w:#x}");
                assert_eq!(c.query_lttb(w, .., 3, QueryOpts::asc()).unwrap_err().status(), invalid, "{w:#x}");
                assert_eq!(c.query_twa(w, .., 0, TwaOpts::step()).unwrap_err().status(), invalid, "{w:#x}");
            } else {
                // a well formed id of a volume this server does not have: no rows
                assert_eq!(c.query_rows(w, .., QueryOpts::asc()).unwrap().len(), 0, "{w:#x}");
                assert!(c.query_agg(w, .., 0).unwrap().iter().all(|x| x.count == 0), "{w:#x}");
                assert!(c.query_lttb(w, .., 3, QueryOpts::asc()).unwrap().ts.is_empty(), "{w:#x}");
                assert!(c.query_twa(w, .., 0, TwaOpts::step()).unwrap().is_empty(), "{w:#x}");
            }
            assert!(!c.is_broken());
        }
        // a multi query: series a answers, the unknown volume gives nothing, the malformed id INVALID
        let q = [wide[0], a, wide[1]];
        let multi = c.query_rows_multi(&q, .., QueryOpts::asc(), MultiOpts::default()).unwrap();
        assert_eq!(multi.len(), 3);
        assert_eq!(multi[0].as_ref().unwrap().len(), 0);
        assert_eq!(multi[1].as_ref().unwrap().len(), 3);
        assert_eq!(multi[2].as_ref().unwrap_err().status(), Some(Status::Invalid));
        // nothing above touched series a, whose low 32 bits the wide ids share
        let after = c.query_rows(a, .., QueryOpts::asc()).unwrap();
        assert_eq!(
            (after.ts, after.values.to_options::<i64>().unwrap()),
            (before.ts.clone(), vec![Some(1), Some(2), Some(3)])
        );
        assert_eq!(c.retention(a).unwrap(), 0);
        assert_eq!(c.state_name(a_state, 1).unwrap(), "up");
    }
}
