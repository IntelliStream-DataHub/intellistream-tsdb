//! QUERY_LATEST (protocol 14): the newest value of many series, on tsdbd and (feature `embedded`) on the
//! engine, with nulls, every type, missing and denied ids and names, end bounds, per-series bounds and lists
//! longer than one reply frame.

mod common;

use common::{Server, T0};
use tsdb_client::{
    Bitmap, Client, Column, ColumnRef, ConnectOptions, Database, Latest, LatestKind, LatestOpts, LatestValue, Rights,
    Selector, Status, ValueType,
};

const N: i64 = 100;
const STEP: i64 = 1_000;

/// One series of each type with nulls at the newest rows of some, a series with no rows, and a series of nothing
/// but nulls; the ids in that order.
fn setup(db: &mut impl Database, prefix: &str) -> Vec<u64> {
    let ts: Vec<i64> = (0..N).map(|i| T0 + i * STEP).collect();
    let mut ids = Vec::new();
    for ty in ValueType::ALL {
        let id = db.series_create(&format!("{prefix}.{ty}"), ty, None).unwrap();
        // the float32 and int64 series end in two nulls; the others do not
        let valid: Bitmap =
            (0..N).map(|i| !(matches!(ty, ValueType::Float32 | ValueType::Int64) && i >= N - 2)).collect();
        let col = match ty {
            ValueType::Float32 => Column::new((0..N).map(|i| i as f32 * 0.5).collect::<Vec<f32>>()),
            ValueType::Float64 => Column::new((0..N).map(|i| i as f64 * 1e10 + 0.25).collect::<Vec<f64>>()),
            ValueType::Int64 => Column::new((0..N).map(|i| i * 3_000_000_000_000 - 7).collect::<Vec<i64>>()),
            ValueType::Bool => Column::new((0..N).map(|i| i % 3 == 0).collect::<Vec<bool>>()),
            _ => {
                let c = [db.state(id, "on").unwrap(), db.state(id, "off").unwrap()];
                Column::new((0..N).map(|i| c[(i % 2) as usize]).collect::<Vec<u32>>())
            }
        };
        db.insert_series_values(id, &ts, ColumnRef::from(&col).with_validity(&valid)).unwrap();
        ids.push(id);
    }
    db.flush().unwrap();
    // rows that stay in the memtable: a newer value for the float64 series, a null over its newest row for bool
    db.insert_series_values(ids[1], &[T0 + N * STEP], &[7.5f64][..]).unwrap();
    let nul: Bitmap = vec![false].into_iter().collect();
    db.insert_series_values(
        ids[3],
        &[T0 + (N - 1) * STEP],
        ColumnRef::from(&Column::new(vec![false])).with_validity(&nul),
    )
    .unwrap();
    ids.push(db.series_create(&format!("{prefix}.empty"), ValueType::Float32, None).unwrap());
    let nulls = db.series_create(&format!("{prefix}.nulls"), ValueType::Int64, None).unwrap();
    let nv: Bitmap = vec![false, false].into_iter().collect();
    db.insert_series_values(nulls, &[T0, T0 + 1], ColumnRef::from(&Column::new(vec![0i64, 0])).with_validity(&nv))
        .unwrap();
    ids.push(nulls);
    ids
}

fn point(l: &Latest) -> Option<(i64, LatestValue)> {
    l.point()
}

fn check(db: &mut impl Database, ids: &[u64], prefix: &str) {
    let ans = db.latest(ids, &LatestOpts::default()).unwrap();
    assert_eq!(ans.len(), ids.len());
    let last = T0 + (N - 1) * STEP;
    // float32: the newest non-null row is two before the end
    assert_eq!(point(&ans[0]), Some((last - 2 * STEP, LatestValue::Float32((N - 3) as f32 * 0.5))));
    assert_eq!(ans[0].ty, Some(ValueType::Float32));
    // float64: the memtable row one step past the last
    assert_eq!(point(&ans[1]), Some((T0 + N * STEP, LatestValue::Float64(7.5))));
    assert_eq!(point(&ans[2]), Some((last - 2 * STEP, LatestValue::Int64((N - 3) * 3_000_000_000_000 - 7))));
    // bool: the newest row is nulled in the memtable: the row before it
    assert_eq!(point(&ans[3]), Some((last - STEP, LatestValue::Bool((N - 2) % 3 == 0))));
    assert!(matches!(point(&ans[4]), Some((t, LatestValue::State(_))) if t == last));
    assert_eq!((ans[5].kind, ans[5].status, ans[5].ty), (LatestKind::None, None, Some(ValueType::Float32)));
    assert_eq!((ans[6].kind, ans[6].status), (LatestKind::None, None));

    // with nulls: the newest row, null or not
    let wn = db.latest(ids, &LatestOpts::default().with_nulls()).unwrap();
    assert_eq!((wn[0].kind, wn[0].ts, wn[0].value), (LatestKind::Null, last, None));
    assert_eq!((wn[2].kind, wn[2].ts), (LatestKind::Null, last));
    assert_eq!((wn[3].kind, wn[3].ts), (LatestKind::Null, last));
    assert_eq!(point(&wn[1]), point(&ans[1]));
    assert_eq!((wn[6].kind, wn[6].ts), (LatestKind::Null, T0 + 1));

    // an end bound, and one per series
    let bound = db.latest(ids, &LatestOpts::default().end(T0 + 10 * STEP)).unwrap();
    assert_eq!(bound[0].ts, T0 + 9 * STEP);
    assert_eq!(bound[4].ts, T0 + 9 * STEP);
    let ends: Vec<i64> = (0..ids.len() as i64).map(|i| T0 + (i + 1) * STEP).collect();
    let per = db.latest(ids, &LatestOpts::default().ends(ends.clone())).unwrap();
    for (i, a) in per.iter().enumerate().take(5) {
        assert_eq!(a.ts, T0 + i as i64 * STEP, "series {i}");
    }
    assert_eq!(per[5].kind, LatestKind::None);
    assert!(db.latest(ids, &LatestOpts::default().ends(ends[1..].to_vec())).is_err());

    // missing ids: no volume field, bits 48-63, a local number that does not exist
    let odd = [ids[0], (0u64) << 32 | 5, ids[0] | 1 << 48, (1u64 << 32) | 0xffff_ff00, ids[1]];
    let m = db.latest(&odd, &LatestOpts::default()).unwrap();
    assert_eq!(m[0], ans[0]);
    assert_eq!(m[4], ans[1]);
    for i in 1..4 {
        assert_eq!(
            (m[i].status, m[i].kind, m[i].ty, m[i].ts, m[i].value),
            (Some(Status::NotFound), LatestKind::None, None, 0, None)
        );
    }

    // by name: found, missing, empty and repeated (the second call is served from the id cache)
    let names = [
        format!("{prefix}.{}", ValueType::Float64),
        format!("{prefix}.nope"),
        format!("{prefix}.{}", ValueType::Float32),
        String::new(),
    ];
    let names: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
    for _ in 0..2 {
        let by = db.latest_by_name(&names, &LatestOpts::default()).unwrap();
        assert_eq!(by[0], ans[1]);
        assert_eq!(by[1].status, Some(Status::NotFound));
        assert_eq!(by[2], ans[0]);
        assert_eq!(by[3].status, Some(Status::Invalid));
    }
    assert!(db.latest(&[], &LatestOpts::default()).unwrap().is_empty());
    assert!(db.latest(&vec![ids[0]; (1 << 20) + 1], &LatestOpts::default()).is_err());

    // more ids than one reply frame holds (65,536): the records come back in index order
    let many: Vec<u64> = (0..140_000).map(|i| ids[i % ids.len()]).collect();
    let big = db.latest(&many, &LatestOpts::default()).unwrap();
    assert_eq!(big.len(), many.len());
    for (i, a) in big.iter().enumerate() {
        assert_eq!(*a, ans[i % ids.len()], "record {i}");
    }
}

#[test]
fn latest_on_tcp() {
    let srv = Server::start();
    let mut c = srv.client();
    let ids = setup(&mut c, "lt");
    check(&mut c, &ids, "lt");
}

#[cfg(feature = "embedded")]
#[test]
fn latest_embedded_and_tsdbd_agree() {
    let dir = common::test_dir("tsdb-latest-embedded");
    let mut db = tsdb_client::Db::open(&dir, &tsdb_client::Options::default()).unwrap();
    let ids = setup(&mut db, "lt");
    check(&mut db, &ids, "lt");
    let opts = [
        LatestOpts::default(),
        LatestOpts::default().with_nulls(),
        LatestOpts::default().end(T0 + 5 * STEP),
        LatestOpts::default().with_nulls().end(T0 + 5 * STEP),
    ];
    let embedded: Vec<Vec<Latest>> = opts.iter().map(|o| db.latest(&ids, o).unwrap()).collect();
    db.flush().unwrap();
    db.close().unwrap();
    let srv = Server::start_in(dir);
    let mut c = srv.client();
    for (o, e) in opts.iter().zip(&embedded) {
        assert_eq!(&c.latest(&ids, o).unwrap(), e);
    }
}

#[test]
fn denied_and_missing_look_alike_for_a_user_without_read_on_all() {
    let srv = Server::start_auth("root-pw");
    let mut root = Client::connect_opts(srv.addr(), &ConnectOptions::new().user("root").password("root-pw")).unwrap();
    let a = root.series("acme/a").unwrap();
    let b = root.series("other/b").unwrap();
    root.insert_series(a, &[T0], &[1.5]).unwrap();
    root.insert_series(b, &[T0], &[2.5]).unwrap();
    root.user_create("u", "u-pw", false, None).unwrap();
    root.grant("u", &Selector::Prefix("acme/".into()), Rights::READ).unwrap();
    root.user_create("all", "all-pw", false, None).unwrap();
    root.grant("all", &Selector::All, Rights::READ).unwrap();
    let missing = (1u64 << 32) | 0xffff_ff00;

    let mut u = Client::connect_opts(srv.addr(), &ConnectOptions::new().user("u").password("u-pw")).unwrap();
    let r = u.latest(&[a, b, missing], &LatestOpts::default()).unwrap();
    assert_eq!(r[0].point(), Some((T0, LatestValue::Float32(1.5))));
    // an existing series outside the grants and a missing one: the same answer
    assert_eq!(r[1], r[2]);
    assert_eq!(r[1].status, Some(Status::Denied));
    assert_eq!((r[1].kind, r[1].ty, r[1].ts, r[1].value), (LatestKind::None, None, 0, None));
    // by name: the same, for a name outside the grants and one that does not exist
    let n = u.latest_by_name(&["acme/a", "other/b", "other/nope", "acme/nope"], &LatestOpts::default()).unwrap();
    assert_eq!(n[0], r[0]);
    assert_eq!(n[1].status, Some(Status::Denied));
    assert_eq!(n[2], n[1]);
    assert_eq!(n[3].status, Some(Status::NotFound)); // inside the grants: NOTFOUND (look up, then create)

    // read on all: a missing id is NOTFOUND
    let mut all = Client::connect_opts(srv.addr(), &ConnectOptions::new().user("all").password("all-pw")).unwrap();
    let r = all.latest(&[a, b, missing], &LatestOpts::default()).unwrap();
    assert_eq!(r[1].point(), Some((T0, LatestValue::Float32(2.5))));
    assert_eq!(r[2].status, Some(Status::NotFound));
}

/// A fake server of protocol 14 that holds the replies to SERIES until the client has been quiet for 100 ms, and
/// reports the most requests it held at once and how many SERIES it saw: a client that looks names up one round trip
/// at a time never has two. Names starting "missing" are NOT FOUND, "flaky" is UNAVAILABLE the first time, the rest
/// get an id; QUERY_LATEST answers a NONE record per id.
fn lookup_server() -> (String, std::thread::JoinHandle<(usize, usize)>) {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::Duration;
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    let h = std::thread::spawn(move || {
        let (mut s, _) = l.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_millis(100))).unwrap();
        let reply = |s: &mut std::net::TcpStream, id: [u8; 4], st: i32, body: &[u8]| {
            let mut r = ((9 + body.len()) as u32).to_le_bytes().to_vec();
            r.extend_from_slice(&id);
            r.push(0);
            r.extend_from_slice(&st.to_le_bytes());
            r.extend_from_slice(body);
            s.write_all(&r).unwrap();
        };
        let (mut held, mut max_held, mut total, mut seq, mut flaked) = (Vec::<([u8; 4], String)>::new(), 0, 0, 0u64, false);
        loop {
            let mut h = [0u8; 9];
            match s.read_exact(&mut h) {
                Ok(()) => {}
                Err(e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) => {
                    max_held = max_held.max(held.len());
                    for (id, name) in held.drain(..) {
                        if name.starts_with("missing") {
                            reply(&mut s, id, -6, &[]); // NOTFOUND
                        } else if name.starts_with("flaky") && !flaked {
                            flaked = true;
                            reply(&mut s, id, -11, &[]); // UNAVAILABLE
                        } else {
                            seq += 1;
                            reply(&mut s, id, 0, &((1u64 << 32) | seq).to_le_bytes());
                        }
                    }
                    continue;
                }
                Err(_) => break,
            }
            s.set_read_timeout(None).unwrap();
            let len = u32::from_le_bytes(h[0..4].try_into().unwrap()) as usize;
            let mut p = vec![0; len - 5];
            s.read_exact(&mut p).unwrap();
            s.set_read_timeout(Some(Duration::from_millis(100))).unwrap();
            let id: [u8; 4] = h[4..8].try_into().unwrap();
            match h[8] {
                1 => reply(&mut s, id, 0, &[14u32.to_le_bytes(), [0; 4]].concat()),
                2 => {
                    total += 1;
                    let n = u16::from_le_bytes([p[1], p[2]]) as usize;
                    held.push((id, String::from_utf8_lossy(&p[3..3 + n]).into_owned()));
                }
                39 => {
                    let n = u32::from_le_bytes(p[4..8].try_into().unwrap());
                    let mut body = vec![0, 0, 0, 0];
                    body.extend_from_slice(&n.to_le_bytes());
                    body.extend_from_slice(&[1, 0, 0, 0]);
                    for _ in 0..n {
                        body.extend_from_slice(&[0, 0, 0, 0, 2, 0, 0, 0]);
                        body.extend_from_slice(&[0; 16]);
                    }
                    reply(&mut s, id, 0, &body);
                }
                _ => reply(&mut s, id, -4, &[]),
            }
        }
        (max_held, total)
    });
    (addr, h)
}

/// `latest_by_name` looks the names it does not know up together (a round trip per window, not per name), answers a
/// name the lookup refuses in its own entry (an over-long one INVALID), asks a name the server could not answer yet
/// again, alone, and keeps the ids.
#[test]
fn latest_by_name_looks_names_up_together() {
    let (addr, h) = lookup_server();
    let mut c = Client::connect(addr).unwrap();
    let long = "x".repeat(70_000);
    let names: Vec<String> =
        (0..40).map(|i| if i == 7 { "missing7".into() } else if i == 9 { "flaky9".into() } else { format!("n{i}") }).collect();
    let mut refs: Vec<&str> = names.iter().map(String::as_str).collect();
    refs[11] = &long;
    refs[12] = "";
    let out = c.latest_by_name(&refs, &LatestOpts::default()).unwrap();
    for (i, l) in out.iter().enumerate() {
        let want = match i {
            7 => Some(Status::NotFound),
            11 | 12 => Some(Status::Invalid),
            _ => None,
        };
        assert_eq!(l.status, want, "name {i}");
        assert_eq!(l.kind, LatestKind::None);
    }
    // the second call asks only the NOT FOUND name again: the others have ids
    c.latest_by_name(&refs, &LatestOpts::default()).unwrap();
    c.close().unwrap();
    let (max_held, total) = h.join().unwrap();
    assert!(max_held >= 38, "the 38 sendable names of the window were held together, not {max_held}");
    assert_eq!(total, 38 + 1 + 1); // the window, the flaky name again, the NOT FOUND name in the second call
}
