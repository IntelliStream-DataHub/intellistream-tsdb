// SPDX-License-Identifier: Apache-2.0
//! STATS_SERIES and STATS_TENANTS (protocol 15) on tsdbd: the numbers of series and tenants, the rights (a tenant
//! admin sees only its tenant; denied and missing look alike), requests longer than one frame, and fake servers
//! for a server of protocol 14, a work budget that stops a reply early and a tenant list that changes under a cursor.

mod common;

use common::{Server, T0};
use tsdb_client::{Client, ConnectOptions, HotKind, Rights, Selector, Status, TenantOpts, ValueType};

fn login(srv: &Server, user: &str, pw: &str) -> Client {
    Client::connect_opts(srv.addr(), &ConnectOptions::new().user(user).password(pw)).unwrap()
}

#[test]
fn series_stats_on_tcp() {
    let srv = Server::start();
    let mut c = srv.client();
    let a = c.series("st.a").unwrap();
    let b = c.series_create("st.b", ValueType::Int64, None).unwrap();
    let ts: Vec<i64> = (0..1000).map(|i| T0 + i * 1000).collect();
    c.insert_series(a, &ts, &(0..1000).map(|i| i as f32).collect::<Vec<_>>()).unwrap();
    c.flush().unwrap();
    let missing = (1u64 << 32) | 0xffff_ff00;
    let s = c.series_stats(&[a, b, missing], false).unwrap();
    assert_eq!(s.len(), 3);
    assert_eq!(s[0].status, None);
    assert_eq!((s[0].rows, s[0].ty, s[0].first_ts, s[0].last_ts), (1000, Some(ValueType::Float32), ts[0], ts[999]));
    assert!(s[0].ts_bytes > 0 && s[0].value_bytes > 0 && s[0].parts >= 1 && s[0].logical_bytes == 12_000);
    assert_eq!((s[1].status, s[1].rows, s[1].ty), (None, 0, Some(ValueType::Int64)));
    assert_eq!((s[2].status, s[2].rows, s[2].ty), (Some(Status::NotFound), 0, None));
    // an empty list asks nothing
    assert!(c.series_stats(&[], false).unwrap().is_empty());
    // with the page cache numbers
    let r = c.series_stats(&[a], true).unwrap();
    assert_eq!(r[0].rows, 1000);

    // more ids than one request holds (65,536; 1,024 with resident): the answers come in request order
    let many: Vec<u64> = (0..70_000).map(|i| if i % 2 == 0 { a } else { b }).collect();
    let big = c.series_stats(&many, false).unwrap();
    assert_eq!(big.len(), many.len());
    for (i, x) in big.iter().enumerate() {
        assert_eq!(x.rows, if i % 2 == 0 { 1000 } else { 0 }, "record {i}");
    }
    let big = c.series_stats(&many[..2500], true).unwrap();
    assert_eq!(big.len(), 2500);
    assert!(big.iter().step_by(2).all(|x| x.rows == 1000));
}

#[test]
fn tenants_from_a_file_and_the_hot_lists() {
    let dir = common::test_dir("tsdb-rust-tenants");
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.with_extension("tenants");
    std::fs::write(&file, "acme/\nacme/lab/\n").unwrap();
    let srv = Server::spawn_args(dir, false, &["--tenants", file.to_str().unwrap()]);
    let mut c = srv.client();
    let ts: Vec<i64> = (0..100).map(|i| T0 + i * 1000).collect();
    let mut ids = Vec::new();
    for n in ["acme/a", "acme/b", "acme/lab/c", "other/d"] {
        let id = c.series(n).unwrap();
        c.insert_series(id, &ts, &vec![1.5f32; 100]).unwrap();
        ids.push(id);
    }
    c.flush().unwrap();
    c.query_points(ids[0], T0..T0 + 50_000, Default::default()).unwrap();
    let t = c.tenant_stats(&[], &TenantOpts::default()).unwrap();
    let names: Vec<_> = t.rows.iter().map(|r| (r.prefix.as_str(), r.volume)).collect();
    assert_eq!(names, [("", None), ("acme/", None), ("acme/lab/", None)]);
    assert_eq!(t.generation, t.generation.max(1));
    // wait for the sweeper
    let mut t = t;
    for _ in 0..100 {
        if by_series(&t, "acme/") == 2 && by_series(&t, "acme/lab/") == 1 && by_series(&t, "") == 1 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
        t = c.tenant_stats(&[], &TenantOpts::default()).unwrap();
    }
    assert_eq!((by_series(&t, "acme/"), by_series(&t, "acme/lab/"), by_series(&t, "")), (2, 1, 1));

    // one tenant by name, with volumes, the caller's counters and the hot lists
    let o = TenantOpts::default().volumes().caller().hot(5);
    let t = c.tenant_stats(&["", "acme/", "nope/"], &o).unwrap();
    let rows: Vec<_> = t.rows.iter().map(|r| (r.prefix.as_str(), r.volume)).collect();
    assert_eq!(rows, [("", None), ("", Some(0)), ("acme/", None), ("acme/", Some(0)), ("nope/", None)]);
    assert_eq!(t.rows[4].status, Some(Status::NotFound));
    // without login every request is the tenant "(none)"'s: its summed row carries the counters, a volume row none
    assert!(t.rows[0].caller.is_some() && t.rows[1].caller.is_none() && t.rows[2].caller.is_some());
    assert!(t.rows[0].caller.unwrap().requests_insert >= 4 && t.rows[2].caller.unwrap().requests_insert == 0);
    let hot = &t.rows[2].hot;
    // each kind ends in its threshold, id 0; the series of the tenant, and no other, are listed
    assert_eq!(hot.iter().filter(|h| h.id == 0).count(), 3);
    assert!(hot.iter().any(|h| h.kind == HotKind::Writes && h.id == ids[0]));
    assert!(hot.iter().all(|h| h.id != ids[3] && h.id != ids[2]));
    assert!(t.rows[0].hot.iter().any(|h| h.id == ids[3]));
    // the summed row is the volume's (one volume)
    assert_eq!(t.rows[2].numbers.rows, t.rows[3].numbers.rows);
    assert!(c.tenant_stats(&[], &TenantOpts::default().hot(257)).is_err());
}

fn by_series(t: &tsdb_client::TenantStats, p: &str) -> u64 {
    t.rows.iter().find(|r| r.prefix == p).map_or(0, |r| r.numbers.series)
}

#[test]
fn a_tenant_admin_sees_only_its_tenant() {
    let dir = common::test_dir("tsdb-rust-auth-tenants");
    common::user_cli(&dir, Some("root-pw"), &["add", "root", "--superuser"]);
    let srv = Server::spawn_args(dir, true, &["--tenants-settle", "1"]);
    let mut root = login(&srv, "root", "root-pw");
    let a = root.series("acme/a").unwrap();
    let b = root.series("other/b").unwrap();
    root.insert_series(a, &[T0], &[1.5]).unwrap();
    root.insert_series(b, &[T0], &[2.5]).unwrap();
    root.user_create("acme/boss", "boss-pw", false, None).unwrap();
    root.grant("acme/boss", &Selector::Prefix("acme/".into()), Rights::READ).unwrap();
    root.grant("acme/boss", &Selector::Prefix("acme/".into()), Rights::ADMIN).unwrap();
    root.user_create("u", "u-pw", false, None).unwrap();
    root.grant("u", &Selector::Prefix("acme/".into()), Rights::READ).unwrap();
    // the tenants come from the grants of tenant admins (after the settle time)
    let mut tenants = Vec::new();
    for _ in 0..100 {
        tenants = root.tenant_stats(&[], &TenantOpts::default()).unwrap().rows;
        if tenants.iter().any(|r| r.prefix == "acme/") {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    assert!(tenants.iter().any(|r| r.prefix == "acme/") && tenants.iter().any(|r| r.prefix.is_empty()));

    let mut boss = login(&srv, "acme/boss", "boss-pw");
    let t = boss.tenant_stats(&[], &TenantOpts::default().caller()).unwrap();
    assert_eq!(t.rows.iter().map(|r| r.prefix.as_str()).collect::<Vec<_>>(), ["acme/"]);
    assert_eq!((t.parts_counted, t.parts_listed), (0, 0)); // only whoever reads everything sees them
    // another tenant, and "(none)": denied to a tenant admin, whether or not it exists
    let t = boss.tenant_stats(&["other/", "nope/", ""], &TenantOpts::default()).unwrap();
    assert!(t.rows.iter().all(|r| r.status == Some(Status::Denied)), "{:?}", t.rows);
    // a user who is no tenant admin sees nothing
    let mut u = login(&srv, "u", "u-pw");
    let t = u.tenant_stats(&[], &TenantOpts::default()).unwrap();
    assert!(t.rows.is_empty());
    let t = u.tenant_stats(&["acme/"], &TenantOpts::default()).unwrap();
    assert_eq!(t.rows[0].status, Some(Status::Denied));

    // series: denied and missing look alike
    let missing = (1u64 << 32) | 0xffff_ff00;
    let r = u.series_stats(&[a, b, missing], false).unwrap();
    assert_eq!(r[0].status, None);
    assert_eq!(r[1], r[2]);
    assert_eq!(r[1].status, Some(Status::Denied));
    // the error bounds are for whoever reads everything
    assert_eq!(r[0].reads_err, 0);
    let r = root.series_stats(&[a, missing], false).unwrap();
    assert_eq!((r[0].rows, r[1].status), (1, Some(Status::NotFound)));
}

/// A fake server of protocol `version` answering HELLO; `op` replies are made by `answer(op, payload)`, which
/// returns the status and body, or None to close. Returns its address and the number of requests it saw per op.
fn fake(
    version: u32,
    mut answer: impl FnMut(u8, &[u8]) -> (i32, Vec<u8>) + Send + 'static,
) -> (String, std::thread::JoinHandle<Vec<u8>>) {
    use std::io::{Read, Write};
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    let h = std::thread::spawn(move || {
        let (mut s, _) = l.accept().unwrap();
        let mut ops = Vec::new();
        loop {
            let mut h = [0u8; 9];
            if s.read_exact(&mut h).is_err() {
                break;
            }
            let len = u32::from_le_bytes(h[0..4].try_into().unwrap()) as usize;
            let mut p = vec![0; len - 5];
            s.read_exact(&mut p).unwrap();
            ops.push(h[8]);
            let (st, body) = if h[8] == 1 { (0, [version.to_le_bytes(), [0; 4]].concat()) } else { answer(h[8], &p) };
            let mut r = ((9 + body.len()) as u32).to_le_bytes().to_vec();
            r.extend_from_slice(&h[4..8]);
            r.push(0);
            r.extend_from_slice(&st.to_le_bytes());
            r.extend_from_slice(&body);
            s.write_all(&r).unwrap();
        }
        ops
    });
    (addr, h)
}

#[test]
fn a_server_of_protocol_14_is_sent_nothing() {
    let (addr, h) = fake(14, |_, _| (-4, vec![]));
    let mut c = Client::connect(addr).unwrap();
    assert!(matches!(c.series_stats(&[1 << 32], false), Err(tsdb_client::Error::Server(Status::Invalid))));
    assert!(matches!(c.tenant_stats(&[], &TenantOpts::default()), Err(tsdb_client::Error::Server(Status::Invalid))));
    drop(c);
    assert_eq!(h.join().unwrap(), [1]); // HELLO only
}

#[test]
fn a_reply_that_stops_at_the_work_budget_is_asked_for_again() {
    // answers at most 3 records a request; the record's `rows` field says which id it is for
    let (addr, h) = fake(15, |op, p| {
        assert_eq!(op, 46);
        let n = u32::from_le_bytes(p[0..4].try_into().unwrap()) as usize;
        let done = n.min(3);
        let mut b = (done as u32).to_le_bytes().to_vec();
        b.extend_from_slice(&[0; 4]);
        for i in 0..done {
            let id = u64::from_le_bytes(p[8 + 8 * i..16 + 8 * i].try_into().unwrap());
            let mut rec = [0u8; 176];
            rec[5] = 1; // float64
            rec[8..16].copy_from_slice(&id.to_le_bytes());
            b.extend_from_slice(&rec);
        }
        (0, b)
    });
    let mut c = Client::connect(addr).unwrap();
    let ids: Vec<u64> = (0..10).map(|i| (1 << 32) | i).collect();
    let s = c.series_stats(&ids, false).unwrap();
    assert_eq!(s.iter().map(|x| x.rows).collect::<Vec<_>>(), ids);
    drop(c);
    assert_eq!(h.join().unwrap(), [1, 46, 46, 46, 46]);
}

#[test]
fn replies_out_of_shape_are_protocol_errors() {
    for body in [
        vec![0u8; 4],                                                                        // too short
        [vec![1, 0, 0, 0, 1, 0, 0, 0], vec![0; 176]].concat(), // the second word must be 0
        [vec![2, 0, 0, 0, 0, 0, 0, 0], vec![0; 176]].concat(), // done above what was asked
        [vec![1, 0, 0, 0, 0, 0, 0, 0], vec![0; 100]].concat(), // a record cut short
        [vec![1, 0, 0, 0, 0, 0, 0, 0], vec![0, 0, 0, 0, 0, 9, 0, 0], vec![0; 168]].concat(), // a type that does not exist
    ] {
        let (addr, _h) = fake(15, move |_, _| (0, body.clone()));
        let mut c = Client::connect(addr).unwrap();
        assert!(matches!(c.series_stats(&[1 << 32], false), Err(tsdb_client::Error::Protocol(_))));
        assert!(c.series_stats(&[1 << 32], false).is_err()); // the connection is unusable
    }
}

#[test]
fn a_tenant_list_that_changes_under_a_cursor_restarts_the_listing() {
    // reply 1: one row, more to come, generation 1; reply 2: the list is generation 2 now: start over;
    // replies 3 and 4: generation 2, in two pieces
    let row = |name: &str| -> Vec<u8> {
        let mut r = vec![0u8; 16];
        r[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
        r[12..14].copy_from_slice(&(name.len() as u16).to_le_bytes());
        r.extend_from_slice(name.as_bytes());
        r.resize(16 + name.len().div_ceil(8) * 8, 0);
        r.extend_from_slice(&[0; 18 * 8]);
        r
    };
    let reply = |count: u32, next: u32, generation: u32, rows: Vec<u8>| -> (i32, Vec<u8>) {
        let mut b = Vec::new();
        for v in [count, 0, next, generation] {
            b.extend_from_slice(&v.to_le_bytes());
        }
        b.extend_from_slice(&[7, 0, 0, 0, 0, 0, 0, 0]); // list_hash
        b.extend_from_slice(&[0; 16]);
        b.extend_from_slice(&rows);
        (0, b)
    };
    let mut call = 0;
    let (addr, h) = fake(15, move |op, p| {
        assert_eq!(op, 47);
        let cursor = u32::from_le_bytes(p[8..12].try_into().unwrap());
        call += 1;
        match call {
            1 => reply(1, 1, 1, row("a/")),
            2 => {
                assert_eq!(cursor, 1);
                reply(1, 1, 2, row("zzz/"))
            }
            3 => {
                assert_eq!(cursor, 0);
                reply(1, 1, 2, row("b/"))
            }
            _ => {
                assert_eq!(cursor, 1);
                reply(1, u32::MAX, 2, row("c/"))
            }
        }
    });
    let mut c = Client::connect(addr).unwrap();
    let t = c.tenant_stats(&[], &TenantOpts::default()).unwrap();
    assert_eq!(t.generation, 2);
    assert_eq!(t.rows.iter().map(|r| r.prefix.as_str()).collect::<Vec<_>>(), ["b/", "c/"]);
    drop(c);
    assert_eq!(h.join().unwrap(), [1, 47, 47, 47, 47]);
}
