// SPDX-License-Identifier: AGPL-3.0-only
//! QUERY_LATEST (protocol 14): the newest value of many series, on the engine (and as tsdbd answers on the same directory), with nulls, every type, missing and denied ids and names, end bounds, per-series bounds and lists
//! longer than one reply frame.

#![allow(unused_imports, dead_code)]

mod common;

use common::{Server, T0};
use tsdb_client::{
    Bitmap, Client, Column, ColumnRef, ConnectOptions, Database, Latest, LatestKind, LatestOpts, LatestValue, Rights,
    Selector, Status, ValueType,
};
use tsdb_embedded::{Db, Options};

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
fn latest_embedded_and_tsdbd_agree() {
    let dir = common::test_dir("tsdb-latest-embedded");
    let mut db = Db::open(&dir, &Options::default()).unwrap();
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
