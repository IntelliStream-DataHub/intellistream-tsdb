// SPDX-License-Identifier: AGPL-3.0-only
//! Statistics on the engine (`Db::series_stats`, `Db::tenant_stats`) and as tsdbd answers on the same directory:
//! the storage numbers of series and tenants are the same, bit for bit.

mod common;

use common::{Server, T0};
use tsdb_client::{HotKind, SeriesStat, Status, TenantNumbers, TenantOpts, ValueType};
use tsdb_embedded::{Db, Options};

#[test]
fn embedded_and_tsdbd_agree() {
    let dir = common::test_dir("tsdb-stats-embedded");
    let db = Db::open(&dir, &Options { stats: 1, ..Options::default() }).unwrap();
    db.set_tenants(&["acme/", "acme/lab/"]).unwrap();
    let ts: Vec<i64> = (0..5000).map(|i| T0 + i * 1000).collect();
    let mut ids = Vec::new();
    for (n, ty) in [
        ("acme/a", ValueType::Float32),
        ("acme/b", ValueType::Float64),
        ("acme/lab/c", ValueType::Int64),
        ("other/d", ValueType::Bool),
    ] {
        let id = db.series_create(n, ty, None).unwrap();
        // small batches go through the WAL and the memtable, so the flush counts them as writes
        for k in 0..50usize {
            let (r, t) = (k * 100..(k + 1) * 100, &ts[k * 100..(k + 1) * 100]);
            match ty {
                ValueType::Float32 => {
                    db.insert_series_values(id, t, &r.map(|i| i as f32 * 0.25).collect::<Vec<f32>>()[..])
                }
                ValueType::Float64 => {
                    db.insert_series_values(id, t, &r.map(|i| i as f64 * 0.5).collect::<Vec<f64>>()[..])
                }
                ValueType::Int64 => db.insert_series_values(id, t, &r.map(|i| i as i64 * 7).collect::<Vec<i64>>()[..]),
                _ => db.insert_series_values(id, t, &r.map(|i| i % 3 == 0).collect::<Vec<bool>>()[..]),
            }
            .unwrap();
        }
        ids.push(id);
    }
    db.flush().unwrap();
    db.stats_settle(30_000).unwrap();
    let missing = (1u64 << 32) | 0xffff_ff00;
    let mut asked = ids.clone();
    asked.push(missing);

    let series = db.series_stats(&asked, false).unwrap();
    assert_eq!(series[0].rows, 5000);
    assert_eq!(series[4].status, Some(Status::NotFound));
    let o = TenantOpts::default().volumes().hot(3);
    let tenants = db.tenant_stats(&[], &o).unwrap();
    let prefixes: Vec<_> = tenants.rows.iter().map(|r| (r.prefix.as_str(), r.volume)).collect();
    assert_eq!(
        prefixes,
        [("", None), ("", Some(0)), ("acme/", None), ("acme/", Some(0)), ("acme/lab/", None), ("acme/lab/", Some(0))]
    );
    assert_eq!(tenants.rows[2].numbers.series, 2);
    assert_eq!(tenants.rows[4].numbers.series, 1);
    assert_eq!(tenants.rows[0].numbers.series, 1);
    // the hot lists of a tenant hold its series only, and end in the thresholds
    let hot = &tenants.rows[2].hot;
    assert!(hot.iter().any(|h| h.kind == HotKind::Writes && h.id == ids[0]));
    assert!(hot.iter().all(|h| h.id != ids[2] && h.id != ids[3]));
    assert_eq!(hot.iter().filter(|h| h.id == 0).count(), 3);
    let named = db.tenant_stats(&["acme/lab/", "nope/"], &TenantOpts::default()).unwrap();
    assert_eq!(named.rows[0].numbers, tenants.rows[4].numbers);
    assert_eq!(named.rows[1].status, Some(Status::NotFound));
    assert!(db.tenant_stats(&[], &TenantOpts::default().hot(257)).is_err());
    let resident = db.series_stats(&ids, true).unwrap();
    db.close().unwrap();

    // the same directory under tsdbd, with the same tenant list
    let file = dir.with_extension("tenants");
    std::fs::write(&file, "acme/\nacme/lab/\n").unwrap();
    let srv = Server::spawn_args(dir, false, &["--tenants", file.to_str().unwrap()]);
    let mut c = srv.client();
    // the sweeper's catch-up: wait for the numbers
    let mut t;
    let mut n = 0;
    loop {
        t = c.tenant_stats(&[], &o).unwrap();
        if !t.partial && t.rows[2].numbers.series == 2 && t.rows[4].numbers.series == 1 || n > 200 {
            break;
        }
        n += 1;
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    // everything but the hot-list estimates, which are what this process counted in the last two minutes
    let at_rest = |v: Vec<SeriesStat>| -> Vec<SeriesStat> {
        v.into_iter().map(|s| SeriesStat { reads_est: 0, reads_err: 0, writes_est: 0, writes_err: 0, ..s }).collect()
    };
    assert_eq!(at_rest(c.series_stats(&asked, false).unwrap()), at_rest(series));
    // what the engine measured at rest: the same
    let net = c.series_stats(&ids, true).unwrap();
    for (a, b) in net.iter().zip(&resident) {
        assert_eq!(
            (a.rows, a.parts, a.granules, a.ts_bytes, a.value_bytes),
            (b.rows, b.parts, b.granules, b.ts_bytes, b.value_bytes)
        );
    }
    assert_eq!(t.list_hash, tenants.list_hash);
    assert_eq!(t.rows.len(), tenants.rows.len());
    // the numbers of storage; what the process counted since it started (ingest, merges, active series) is its own
    let numbers = |x: &TenantNumbers| {
        (x.series, x.rows, x.nulls, x.logical_bytes, x.ts_bytes, x.value_bytes, x.index_bytes, x.granules)
    };
    for (net, emb) in t.rows.iter().zip(&tenants.rows) {
        assert_eq!((&net.prefix, net.volume, net.status), (&emb.prefix, emb.volume, emb.status));
        assert_eq!(numbers(&net.numbers), numbers(&emb.numbers), "{} {:?}", net.prefix, net.volume);
    }
}
