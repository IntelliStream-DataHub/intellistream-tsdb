//! The embedded database (`Db`): the same scenarios as over TCP, run on both through the
//! `Database` trait, plus what only embedding has (threads sharing one handle, callbacks that
//! panic, zero-copy chunks, the directory lock), and a check that tsdbd answers exactly what the
//! embedded engine answered on the same directory.
#![cfg(feature = "embedded")]

mod common;

use std::ops::ControlFlow;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use common::{Server, T0, test_dir};
use tsdb_client::{
    Bitmap, Bucket, Column, ColumnRef, Database, Db, Error, Group, NULL_F32_BITS, NULL_F64_BITS, Options, QueryOpts,
    RawValues, Rows, Status, TwaOpts, Unit, ValueType,
};

const DAY: i64 = 86_400_000;

fn opts() -> Options {
    Options { fsync: false, ..Default::default() }
}

fn open() -> Db {
    Db::open(test_dir("tsdb-embedded"), &opts()).expect("open")
}

/// Closes `db` and removes its directory.
fn done(db: Db) {
    let dir = db.path().to_path_buf();
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

fn status<T: std::fmt::Debug>(r: Result<T, Error>) -> Option<Status> {
    r.unwrap_err().status()
}

/// A column as `Option`s, printed: nulls compare equal whatever value the engine left in them,
/// and NaNs compare by their Debug form.
fn options(col: &Column) -> String {
    match col.value_type() {
        ValueType::Float32 => {
            format!("{:?}", col.to_options::<f32>().unwrap().iter().map(|o| o.map(f32::to_bits)).collect::<Vec<_>>())
        }
        ValueType::Float64 => {
            format!("{:?}", col.to_options::<f64>().unwrap().iter().map(|o| o.map(f64::to_bits)).collect::<Vec<_>>())
        }
        ValueType::Int64 => format!("{:?}", col.to_options::<i64>().unwrap()),
        ValueType::Bool => format!("{:?}", col.to_options::<bool>().unwrap()),
        ValueType::State => format!("{:?}", col.to_options::<u32>().unwrap()),
        _ => unreachable!(),
    }
}

fn rows_key(r: &Rows) -> String {
    format!("{} {:?} {}", r.value_type(), r.ts, options(&r.values))
}

fn buckets_key(b: &[Bucket]) -> String {
    format!("{b:?}")
}

// ------------------------------------------------------------------ scenarios for both

fn series_and_states(db: &mut impl Database) {
    let mut ids = Vec::new();
    for ty in ValueType::ALL {
        let id = db.series_create(&format!("t.{ty}"), ty, None).unwrap();
        assert_eq!(db.series_type(id).unwrap(), ty);
        assert_eq!(db.series_create(&format!("t.{ty}"), ty, None).unwrap(), id);
        assert_eq!(db.series_name(id).unwrap(), format!("t.{ty}"));
        ids.push(id);
    }
    assert_eq!(status(db.series_create("t.float64", ValueType::Int64, None)), Some(Status::Invalid));
    let plain = db.series("plain").unwrap();
    assert_eq!(db.series_type(plain).unwrap(), ValueType::Float32);
    assert_eq!(db.series_lookup("plain").unwrap(), plain);
    assert_eq!(status(db.series_lookup("missing")), Some(Status::NotFound));
    assert_eq!(status(db.series_type(999_999)), Some(Status::NotFound));
    // no rows, typed float32 as tsdbd answers
    let none = db.query_rows(1 << 32 | 999_999, .., QueryOpts::asc()).unwrap();
    assert!(none.ts.is_empty() && none.values.value_type() == ValueType::Float32);
    assert!(matches!(db.series(""), Err(Error::InvalidArgument(_))));

    let st = ids[4];
    let on = db.state(st, "on").unwrap();
    let off = db.state(st, "off").unwrap();
    assert!(on != 0 && off != 0 && on != off);
    assert_eq!(db.state(st, "on").unwrap(), on);
    assert_eq!(db.state_lookup(st, "off").unwrap(), off);
    assert_eq!(db.state_name(st, on).unwrap(), "on");
    assert_eq!(status(db.state_lookup(st, "standby")), Some(Status::NotFound));
    let ts = [T0, T0 + 1000, T0 + 2000];
    let valid: Bitmap = [true, false, true].into_iter().collect();
    db.insert_series_values(st, &ts, ColumnRef::from(&[on, 0, off]).with_validity(&valid)).unwrap();
    let rows = db.query_rows(st, .., QueryOpts::asc()).unwrap();
    assert_eq!(rows.values.to_options::<u32>().unwrap(), vec![Some(on), None, Some(off)]);
}

fn special_values_and_nulls(db: &mut impl Database) {
    let ts: Vec<i64> = (0..6).map(|i| T0 + i * 1000).collect();
    let f = db.series_create("f32", ValueType::Float32, None).unwrap();
    let fv = [1.5f32, f32::NAN, f32::INFINITY, f32::NEG_INFINITY, -0.0, f32::from_bits(NULL_F32_BITS)];
    let valid: Bitmap = [true, true, true, true, true, true].into_iter().collect();
    db.insert_series_values(f, &ts, ColumnRef::from(&fv).with_validity(&valid)).unwrap();
    let got = db.query_rows(f, .., QueryOpts::asc()).unwrap().values.to_options::<f32>().unwrap();
    let bits: Vec<Option<u32>> = got.iter().map(|o| o.map(f32::to_bits)).collect();
    // the reserved NaN reads back as null; every other value, NaN and -0 included, bit for bit
    let want: Vec<Option<u32>> = fv[..5].iter().map(|x| Some(x.to_bits())).chain([None]).collect();
    assert_eq!(bits, want);
    // the float32 fast path reads nulls as NaN
    let p = db.query_points(f, .., QueryOpts::asc()).unwrap();
    assert_eq!(p.ts, ts);
    assert!(p.values[5].is_nan());

    let d = db.series_create("f64", ValueType::Float64, None).unwrap();
    let dv = [f64::MIN_POSITIVE, f64::MAX, f64::NAN, -0.0, 1e-300, f64::from_bits(NULL_F64_BITS)];
    db.insert_series_values(d, &ts, &dv).unwrap();
    let got = db.query_rows(d, .., QueryOpts::asc()).unwrap().values.to_options::<f64>().unwrap();
    let bits: Vec<Option<u64>> = got.iter().map(|o| o.map(f64::to_bits)).collect();
    let want: Vec<Option<u64>> = dv[..5].iter().map(|x| Some(x.to_bits())).chain([None]).collect();
    assert_eq!(bits, want);

    let i = db.series_create("i64", ValueType::Int64, None).unwrap();
    let iv = [i64::MAX, i64::MIN + 1, 0, -1, i64::MIN, 42];
    let valid: Bitmap = [true, true, true, true, true, false].into_iter().collect();
    db.insert_series_values(i, &ts, ColumnRef::from(&iv).with_validity(&valid)).unwrap();
    let got = db.query_rows(i, .., QueryOpts::asc()).unwrap();
    assert_eq!(
        got.values.to_options::<i64>().unwrap(),
        vec![Some(i64::MAX), Some(i64::MIN + 1), Some(0), Some(-1), None, None]
    );
    // exact integer aggregates, nulls skipped
    let b = db.query_agg(i, .., 0).unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!((b[0].count, b[0].imin, b[0].imax, b[0].isum), (4, i64::MIN + 1, i64::MAX, -1));

    let bo = db.series_create("bool", ValueType::Bool, None).unwrap();
    let bv = [true, false, true, true, false, true];
    let valid: Bitmap = [true, true, false, true, true, true].into_iter().collect();
    db.insert_series_values(bo, &ts, ColumnRef::from(&bv).with_validity(&valid)).unwrap();
    let got = db.query_rows(bo, .., QueryOpts::desc()).unwrap();
    assert_eq!(
        got.values.to_options::<bool>().unwrap(),
        vec![Some(true), Some(false), Some(true), None, Some(false), Some(true)]
    );
    let b = db.query_agg(bo, .., 0).unwrap();
    assert_eq!((b[0].count, b[0].isum), (5, 3));

    // a column of the wrong type, or of the wrong length
    assert_eq!(status(db.insert_series_values(i, &ts, &fv)), Some(Status::Invalid));
    assert!(matches!(db.insert_series(f, &ts, &[1.0]), Err(Error::InvalidArgument(_))));
    assert!(matches!(db.insert(&[f], &ts, &fv), Err(Error::InvalidArgument(_))));
    let short = Bitmap::all_valid(2);
    assert!(matches!(
        db.insert_series_values(i, &ts, ColumnRef::from(&iv).with_validity(&short)),
        Err(Error::InvalidArgument(_))
    ));
}

fn last_write_wins(db: &mut impl Database) {
    let id = db.series("lww").unwrap();
    let ts: Vec<i64> = (0..10).map(|i| T0 + i * 1000).collect();
    db.insert_series(id, &ts, &[1.0; 10]).unwrap();
    db.flush().unwrap();
    db.insert_series(id, &ts[2..4], &[2.0, 2.0]).unwrap(); // memtable over a part
    db.insert_series(id, &[ts[3], ts[3]], &[3.0, 4.0]).unwrap(); // later row of one batch wins
    let other = db.series("lww.other").unwrap();
    db.insert(&[id, other, id], &[ts[9], ts[9], ts[0]], &[9.0, 7.0, 5.0]).unwrap();
    let p = db.query_points(id, .., QueryOpts::asc()).unwrap();
    assert_eq!(p.ts, ts);
    assert_eq!(p.values, vec![5.0, 1.0, 2.0, 4.0, 1.0, 1.0, 1.0, 1.0, 1.0, 9.0]);
    // a null written later replaces a value
    let n = db.series_create("lww.null", ValueType::Int64, None).unwrap();
    db.insert_series_values(n, &ts[..2], &[1i64, 2]).unwrap();
    db.flush().unwrap();
    db.insert_series_values(
        n,
        &ts[1..2],
        ColumnRef::from(&[0i64]).with_validity(&Bitmap::from_bytes(vec![0], 1).unwrap()),
    )
    .unwrap();
    db.optimize().unwrap();
    assert_eq!(
        db.query_rows(n, .., QueryOpts::asc()).unwrap().values.to_options::<i64>().unwrap(),
        vec![Some(1), None]
    );
    assert_eq!(db.query_points(other, .., QueryOpts::asc()).unwrap().values, vec![7.0]);
}

fn order_and_limit(db: &mut impl Database) {
    let id = db.series("big").unwrap();
    let n = 100_000;
    let ts: Vec<i64> = (0..n).map(|i| T0 + i * 1000).collect();
    let v: Vec<f32> = (0..n).map(|i| i as f32).collect();
    db.insert_series(id, &ts, &v).unwrap();
    let all = db.query_points(id, .., QueryOpts::asc()).unwrap();
    assert_eq!((all.ts.as_slice(), all.values.as_slice()), (ts.as_slice(), v.as_slice()));
    let newest = db.query_points(id, .., QueryOpts::desc().limit(3)).unwrap();
    assert_eq!(newest.ts, vec![ts[99_999], ts[99_998], ts[99_997]]);
    let window = db.query_points(id, ts[10]..ts[20], QueryOpts::asc().limit(5)).unwrap();
    assert_eq!(window.ts, ts[10..15].to_vec());
    let inclusive = db.query_points(id, ts[10]..=ts[20], QueryOpts::desc()).unwrap();
    assert_eq!(inclusive.ts.len(), 11);
    assert!(db.query_points(id, .., QueryOpts::asc().limit(0)).unwrap().is_empty());
    assert!(db.query_points(id, ts[5]..ts[5], QueryOpts::asc()).unwrap().is_empty());
    // streaming, with an early stop
    let (mut seen, mut calls) = (0, 0);
    db.query_raw(id, .., QueryOpts::asc(), |t, _| {
        seen += t.len();
        calls += 1;
        if seen >= 20_000 { ControlFlow::Break(()) } else { ControlFlow::Continue(()) }
    })
    .unwrap();
    assert!((20_000..n as usize).contains(&seen), "{seen} rows in {calls} chunks");
    let mut desc = Vec::new();
    db.query_values(id, .., QueryOpts::desc().limit(70_000), |t, col| {
        assert_eq!(t.len(), col.len());
        assert!(col.valid.is_none());
        desc.extend_from_slice(t);
        ControlFlow::Continue(())
    })
    .unwrap();
    assert_eq!(desc.len(), 70_000);
    assert!(desc.windows(2).all(|w| w[0] > w[1]));
}

fn aggregates_groups_and_bounds(db: &mut impl Database) {
    let id = db.series("agg").unwrap();
    // one point a minute for two days
    let ts: Vec<i64> = (0..2 * 1440).map(|i| T0 + i * 60_000).collect();
    let v: Vec<f32> = (0..ts.len()).map(|i| (i % 60) as f32).collect();
    db.insert_series(id, &ts, &v).unwrap();
    let hours = db.query_agg(id, .., 3_600_000).unwrap();
    assert_eq!(hours.iter().map(|b| b.count).sum::<u64>(), ts.len() as u64);
    let whole = db.query_agg(id, .., 0).unwrap();
    assert_eq!((whole[0].count, whole[0].min, whole[0].max), (2880, 0.0, 59.0));
    assert_eq!(status(db.query_agg(id, .., -5)), Some(Status::Invalid));

    let g = Group::new(Unit::Hour);
    let utc = db.query_group(id, .., Some(&g), QueryOpts::asc()).unwrap();
    assert_eq!(utc, hours);
    let newest = db.query_group(id, .., Some(&g.clone().count(6)), QueryOpts::desc().limit(2)).unwrap();
    assert_eq!(newest.len(), 2);
    assert!(newest[0].start > newest[1].start);
    let one = db.query_group(id, .., None, QueryOpts::asc()).unwrap();
    assert_eq!(one[0].count, 2880);
    assert!(db.query_group(id, .., Some(&g), QueryOpts::asc().limit(0)).unwrap().is_empty());

    // local days across the spring DST change in Oslo: 23-hour day on 2024-03-31
    let oslo = db.series("oslo").unwrap();
    let start = 1_711_756_800_000; // 2024-03-30 00:00 UTC
    let hourly: Vec<i64> = (0..72).map(|h| start + h * 3_600_000).collect();
    db.insert_series(oslo, &hourly, &[1.0; 72]).unwrap();
    let days =
        db.query_group(oslo, .., Some(&Group::new(Unit::Day).timezone("Europe/Oslo")), QueryOpts::asc()).unwrap();
    let got: Vec<(i64, u64)> = days.iter().map(|b| (b.start, b.count)).collect();
    assert_eq!(
        got,
        vec![(1_711_753_200_000, 23), (1_711_839_600_000, 23), (1_711_922_400_000, 24), (1_712_008_800_000, 2)]
    );
    let bad = db.query_group(oslo, .., Some(&Group::new(Unit::Day).timezone("Mars/Olympus")), QueryOpts::asc());
    assert_eq!(bad.unwrap_err().code(), -4);
    let empty = db.series("empty.zone").unwrap(); // no rows: the zone is still checked
    let bad = db.query_group(empty, .., Some(&Group::new(Unit::Day).timezone("Mars/Olympus")), QueryOpts::asc());
    assert_eq!(bad.unwrap_err().code(), -4);

    let b = db.query_bounds(oslo, .., &[start, start + DAY / 2, start + DAY], QueryOpts::asc()).unwrap();
    assert_eq!(b.iter().map(|b| b.count).collect::<Vec<_>>(), vec![12, 12, 48]);
    let b = db.query_bounds(oslo, start..start + DAY, &[start, start + DAY / 2], QueryOpts::desc().limit(1)).unwrap();
    assert_eq!((b.len(), b[0].start, b[0].count), (1, start + DAY / 2, 12));
    assert!(matches!(db.query_bounds(oslo, .., &[], QueryOpts::asc()), Err(Error::InvalidArgument(_))));
    assert!(matches!(db.query_bounds(oslo, .., &[start, start], QueryOpts::asc()), Err(Error::InvalidArgument(_))));
}

fn maintenance(db: &mut impl Database) {
    let id = db.series("m").unwrap();
    for k in 0..3 {
        let ts: Vec<i64> = (0..100_000).map(|i| T0 + k * DAY + i * 100).collect();
        db.insert_series(id, &ts, &vec![k as f32; ts.len()]).unwrap();
    }
    db.commit().unwrap();
    db.flush().unwrap();
    db.optimize().unwrap();
    let s = db.stats().unwrap();
    assert_eq!(s.rows, 300_000);
    assert_eq!(s.memtable_rows, 0);
    assert!(s.partitions >= 3 && s.parts >= s.partitions);
    db.query_agg(id, .., 0).unwrap();
    let q = db.last_query_stats().unwrap();
    assert_eq!(q.rows, 300_000);
    assert_eq!(q.result_rows, 1);
    assert!(q.rows_from_stats > 0, "{q:?}");
}

fn spread_twa_and_lttb(db: &mut impl Database) {
    let id = db.series_create("calc", ValueType::Float64, None).unwrap();
    db.insert_series_values(id, &[T0, T0 + 1000, T0 + 3000, T0 + 4000], &[10.0, 20.0, 30.0, 40.0][..]).unwrap();

    // aggregates with order and limit
    let sec = db.query_agg_ex(id, .., 1000, QueryOpts::desc().limit(2)).unwrap();
    assert_eq!(sec.iter().map(|b| (b.start, b.sum)).collect::<Vec<_>>(), vec![(T0 + 4000, 40.0), (T0 + 3000, 30.0)]);
    assert!(db.query_agg_ex(id, .., 1000, QueryOpts::asc().limit(0)).unwrap().is_empty());
    assert_eq!(db.query_agg_ex(id, .., 0, QueryOpts::asc()).unwrap(), db.query_agg(id, .., 0).unwrap());
    assert_eq!(status(db.query_agg_ex(id, .., -1, QueryOpts::asc())), Some(Status::Invalid));

    // the spread of 10, 20, 30, 40
    let s = db.query_spread(id, .., 0, QueryOpts::asc()).unwrap();
    assert_eq!(s.len(), 1);
    assert_eq!((s[0].bucket.count, s[0].bucket.sum, s[0].range), (4, 100.0, 30.0));
    assert_eq!((s[0].var_pop, s[0].stddev_pop), (125.0, 125f64.sqrt()));
    assert!((s[0].var_samp - 500.0 / 3.0).abs() < 1e-9, "{s:?}");
    let per_2s = db.query_spread(id, .., 2000, QueryOpts::desc()).unwrap();
    assert_eq!(per_2s.iter().map(|b| b.bucket.start).collect::<Vec<_>>(), vec![T0 + 4000, T0 + 2000, T0]);
    assert!(per_2s[0].var_samp.is_nan(), "one value has no sample variance");
    let g = db.query_spread_group(id, .., Some(&Group::new(Unit::Second).count(2)), QueryOpts::desc()).unwrap();
    assert_eq!(format!("{g:?}"), format!("{per_2s:?}")); // NaN != NaN
    let day = db.query_spread_group(id, .., Some(&Group::new(Unit::Day).timezone("Europe/Oslo")), QueryOpts::asc());
    assert_eq!(day.unwrap()[0].var_pop, 125.0);
    let b = db.query_spread_bounds(id, .., &[T0, T0 + 3000], QueryOpts::asc().limit(1)).unwrap();
    assert_eq!((b.len(), b[0].bucket.count, b[0].range, b[0].var_pop), (1, 2, 10.0, 25.0));
    assert!(db.query_spread(id, .., 0, QueryOpts::asc().limit(0)).unwrap().is_empty());
    assert!(matches!(db.query_spread_bounds(id, .., &[], QueryOpts::asc()), Err(Error::InvalidArgument(_))));

    // time-weighted: 10 for 1 s, 20 for 2 s, 30 for 1 s; 40 has no segment
    let r = T0..T0 + 5000;
    let step = db.query_twa(id, r.clone(), 0, TwaOpts::step()).unwrap();
    assert_eq!((step[0].area, step[0].covered_ms, step[0].count), (80_000.0, 4000, 4));
    assert_eq!(step[0].twa, 20.0);
    let lin = db.query_twa(id, r.clone(), 0, TwaOpts::linear()).unwrap();
    assert_eq!((lin[0].area, lin[0].covered_ms), (15_000.0 + 50_000.0 + 35_000.0, 4000));
    let gap = db.query_twa(id, r.clone(), 0, TwaOpts::step().max_gap(1500)).unwrap();
    assert_eq!((gap[0].area, gap[0].covered_ms), (40_000.0, 2000));
    let secs = db.query_twa(id, r.clone(), 1000, TwaOpts::step().desc().limit(2)).unwrap();
    // the last reading holds nothing (no later reading ends its segment) but is counted
    assert_eq!((secs.len(), secs[0].start, secs[0].count, secs[0].covered_ms), (2, T0 + 4000, 1, 0));
    assert!(secs[0].twa.is_nan());
    assert_eq!((secs[1].start, secs[1].twa), (T0 + 3000, 30.0));
    let g = db.query_twa_group(id, r.clone(), Some(&Group::new(Unit::Second)), TwaOpts::step()).unwrap();
    assert_eq!(format!("{g:?}"), format!("{:?}", db.query_twa(id, r.clone(), 1000, TwaOpts::step()).unwrap()));
    assert_eq!(db.query_twa_group(id, r.clone(), None, TwaOpts::step()).unwrap(), step);
    let local =
        db.query_twa_group(id, r.clone(), Some(&Group::new(Unit::Day).timezone("Europe/Oslo")), TwaOpts::step());
    assert_eq!(local.unwrap().iter().map(|b| b.area).sum::<f64>(), 80_000.0);
    let b = db.query_twa_bounds(id, r.clone(), &[T0 + 500, T0 + 2000], TwaOpts::step()).unwrap();
    assert_eq!(
        b.iter().map(|b| (b.start, b.area)).collect::<Vec<_>>(),
        vec![(T0 + 500, 25_000.0), (T0 + 2000, 50_000.0)]
    );
    assert!(db.query_twa(id, r.clone(), 0, TwaOpts::step().limit(0)).unwrap().is_empty());
    assert_eq!(status(db.query_twa(id, r.clone(), -1, TwaOpts::step())), Some(Status::Invalid));
    let st = db.series_create("calc.state", ValueType::State, None).unwrap();
    assert_eq!(status(db.query_twa(st, .., 0, TwaOpts::step())), Some(Status::Invalid));

    // LTTB: the first and last points always, the same choice in any order and limit
    let wave = db.series_create("wave", ValueType::Float32, None).unwrap();
    let ts: Vec<i64> = (0..1000).map(|i| T0 + i * 1000).collect();
    let v: Vec<f32> = (0..1000).map(|i| ((i as f32) / 30.0).sin() * 100.0).collect();
    db.insert_series(wave, &ts, &v).unwrap();
    let all = db.query_lttb(wave, .., 50, QueryOpts::asc()).unwrap();
    assert_eq!(all.len(), 50);
    assert_eq!((all.ts[0], all.ts[49]), (T0, T0 + 999_000));
    let desc = db.query_lttb(wave, .., 50, QueryOpts::desc()).unwrap();
    assert_eq!(desc.ts, all.ts.iter().rev().copied().collect::<Vec<_>>());
    let last3 = db.query_lttb(wave, .., 50, QueryOpts::desc().limit(3)).unwrap();
    assert_eq!(last3.ts, desc.ts[..3]);
    let first3 = db.query_lttb(wave, .., 50, QueryOpts::asc().limit(3)).unwrap();
    assert_eq!(first3.ts, all.ts[..3]);
    let none = db.query_lttb(wave, .., 50, QueryOpts::asc().limit(0)).unwrap();
    assert_eq!((none.len(), none.value_type()), (0, ValueType::Float32));
    let few = db.query_lttb(wave, T0..T0 + 10_000, 50, QueryOpts::asc()).unwrap();
    assert_eq!(few.len(), 10, "fewer points than the threshold: all of them");
    assert_eq!(status(db.query_lttb(wave, .., 2, QueryOpts::asc())), Some(Status::Invalid));
    assert_eq!(status(db.query_lttb(st, .., 10, QueryOpts::asc())), Some(Status::Invalid));
}

fn deletes_and_retention(db: &mut impl Database) {
    let id = db.series_create("del", ValueType::Int64, None).unwrap();
    let ts: Vec<i64> = (0..10).map(|i| T0 + i * 1000).collect();
    db.insert_series_values(id, &ts, &(0..10).collect::<Vec<i64>>()[..]).unwrap();
    db.delete_range(id, T0 + 2000..T0 + 5000).unwrap();
    let r = db.query_rows(id, .., QueryOpts::asc()).unwrap();
    assert_eq!(r.ts, [0, 1, 5, 6, 7, 8, 9].map(|i| T0 + i * 1000));
    db.replace_range(id, T0 + 5000..T0 + 8000, &[T0 + 5500], &[55i64][..]).unwrap();
    let r = db.query_rows(id, .., QueryOpts::asc()).unwrap();
    assert_eq!(r.ts, vec![T0, T0 + 1000, T0 + 5500, T0 + 8000, T0 + 9000]);
    assert_eq!(rows_key(&r), rows_key(&Rows { ts: r.ts.clone(), values: Column::new(vec![0i64, 1, 55, 8, 9]) }));
    let outside = db.replace_range(id, T0..T0 + 1000, &[T0 + 1000], &[1i64][..]);
    assert_eq!(status(outside), Some(Status::Invalid));
    assert_eq!(db.retention(id).unwrap(), 0);
    db.set_retention(id, 30 * DAY).unwrap();
    assert_eq!(db.retention(id).unwrap(), 30 * DAY);
    assert!(db.query_rows(id, .., QueryOpts::asc()).unwrap().is_empty(), "T0 is long before now - 30 days");
}

macro_rules! both {
    ($($name:ident),*) => {
        mod on_embedded {
            use super::*;
            $(#[test] fn $name() { let mut db = open(); super::$name(&mut db); done(db); })*
        }
        mod on_tcp {
            use super::*;
            $(#[test] fn $name() { let srv = Server::start(); super::$name(&mut srv.client()); })*
        }
    };
}

both!(
    series_and_states,
    special_values_and_nulls,
    last_write_wins,
    order_and_limit,
    aggregates_groups_and_bounds,
    spread_twa_and_lttb,
    deletes_and_retention,
    maintenance
);

// ------------------------------------------------------------------ embedded only

#[test]
fn chunks_are_borrowed_with_their_bitmap() {
    let db = open();
    let id = db.series_create("edges", ValueType::Int64, None).unwrap();
    let n = 50_000;
    let ts: Vec<i64> = (0..n as i64).map(|i| T0 + i * 1000).collect();
    let v: Vec<i64> = (0..n as i64).collect();
    let nulls = [0, 5, 8191, 8192, 30_000, n - 1];
    let valid: Bitmap = (0..n).map(|i| !nulls.contains(&i)).collect();
    db.insert_series_values(id, &ts, ColumnRef::from(&v).with_validity(&valid)).unwrap();
    // from the memtable the rows come as one chunk; after the flush as several, so the bitmap is
    // cut into pieces whose boundaries fall next to the nulls at 8191 and 8192
    let chunks = |db: &Db| {
        let (mut rows, mut null_rows, mut with_bitmap, mut chunks) = (0, Vec::new(), 0, 0);
        db.query_chunks(id, .., QueryOpts::asc(), |t, chunk| {
            assert_eq!(t.len(), chunk.len());
            let RawValues::Int64(vals) = chunk.values else { panic!("{:?}", chunk.value_type()) };
            for (i, &x) in vals.iter().enumerate() {
                if chunk.is_null(i) {
                    null_rows.push(rows + i);
                } else {
                    assert_eq!(x, (rows + i) as i64);
                }
            }
            with_bitmap += chunk.has_nulls() as usize;
            assert_eq!(chunk.to_column().len(), chunk.len());
            rows += chunk.len();
            chunks += 1;
            ControlFlow::Continue(())
        })
        .unwrap();
        assert_eq!(rows, n);
        assert_eq!(null_rows, nulls);
        assert!(with_bitmap >= 1);
        // the collected form stitches the chunks' bitmaps together
        let all = db.query_rows(id, .., QueryOpts::asc()).unwrap();
        assert_eq!(all.values.valid.as_ref(), Some(&valid));
        chunks
    };
    assert_eq!(chunks(&db), 1);
    db.flush().unwrap();
    assert!(chunks(&db) > 2);
    // an empty answer still has the series' type
    let none = db.query_rows(id, 0..1, QueryOpts::asc()).unwrap();
    assert_eq!((none.len(), none.value_type()), (0, ValueType::Int64));
    done(db);
}

#[test]
fn parts_and_query_stats_belong_to_their_db() {
    let (a, b) = (open(), open());
    let id = a.series("x").unwrap();
    a.insert_series(id, &[T0, T0 + 1000], &[1.0, 2.0]).unwrap();
    a.flush().unwrap();
    a.insert_series(id, &[T0 + 2000], &[3.0]).unwrap();
    a.flush().unwrap();
    let mut parts = a.parts().unwrap();
    parts.sort_by_key(|p| p.min_ts);
    let span: Vec<_> = parts.iter().map(|p| (p.rows, p.min_ts, p.max_ts, p.level)).collect();
    assert_eq!(span, [(2, T0, T0 + 1000, 0), (1, T0 + 2000, T0 + 2000, 0)]);
    assert!(parts.iter().all(|p| p.bytes > 0 && p.granules == 1 && !p.name.is_empty() && p.partition <= T0));
    // a few rows: class 0, the base partition (a day) the rows lie in
    const DAY: i64 = 86_400_000;
    assert_eq!(a.series_class(id).unwrap(), 0);
    assert!(parts.iter().all(|p| p.cls == 0
        && p.start_ms == p.partition * DAY
        && p.end_ms == p.start_ms + DAY
        && p.start_ms <= p.min_ts
        && p.max_ts < p.end_ms));
    // a reading every 10 minutes, loaded at once: a longer class, its partition a multiple of a day
    let slow = a.series("slow").unwrap();
    let ts: Vec<i64> = (0..200).map(|i| T0 + i * 600_000).collect();
    a.insert_series(slow, &ts, &vec![1.0; ts.len()]).unwrap();
    a.flush().unwrap();
    let c = a.series_class(slow).unwrap();
    assert!(c > 0, "class {c}");
    let sp: Vec<_> = a.parts().unwrap().into_iter().filter(|p| p.cls == c).collect();
    assert!(!sp.is_empty() && sp.iter().all(|p| p.start_ms == p.partition * DAY && p.end_ms - p.start_ms > DAY));
    assert_eq!(sp.iter().map(|p| p.rows).sum::<u64>(), 200);
    a.optimize().unwrap();
    // one part per partition: x's day (class 0) and slow's longer one; no row in both
    let mut merged = a.parts().unwrap();
    merged.sort_by_key(|p| p.cls);
    assert_eq!(merged.len(), 2, "{merged:?}");
    assert_eq!((merged[0].cls, merged[0].rows, merged[0].min_ts, merged[0].max_ts), (0, 3, T0, T0 + 2000));
    assert!(merged[0].level >= 1, "{merged:?}");
    assert_eq!((merged[1].cls, merged[1].rows), (c, 200), "{merged:?}");

    // a query on another Db in the same thread leaves a's statistics alone
    assert_eq!(a.last_query_stats().result_rows, 0);
    assert_eq!(a.query_points(id, .., QueryOpts::asc()).unwrap().len(), 3);
    let other = b.series("y").unwrap();
    assert_eq!(b.query_points(other, .., QueryOpts::asc()).unwrap().len(), 0);
    assert_eq!(a.last_query_stats().result_rows, 3);
    assert_eq!(b.last_query_stats().result_rows, 0);
    done(a);
    done(b);
}

#[test]
fn a_panicking_callback_does_not_cross_into_c() {
    let db = open();
    let id = db.series("p").unwrap();
    let ts: Vec<i64> = (0..100_000).map(|i| T0 + i).collect();
    db.insert_series(id, &ts, &vec![1.0; ts.len()]).unwrap();
    let mut calls = 0;
    let r = catch_unwind(AssertUnwindSafe(|| {
        db.query_raw(id, .., QueryOpts::asc(), |_, _| {
            calls += 1;
            panic!("boom in the callback");
        })
    }));
    let msg = r.unwrap_err();
    assert_eq!(msg.downcast_ref::<&str>(), Some(&"boom in the callback"));
    assert_eq!(calls, 1, "the query stopped at the panic");
    let r = catch_unwind(AssertUnwindSafe(|| {
        db.query_chunks(id, .., QueryOpts::desc(), |_, _| panic!("{}", String::from("owned payload")))
    }));
    assert_eq!(r.unwrap_err().downcast_ref::<String>().map(String::as_str), Some("owned payload"));
    // the engine is unharmed
    assert_eq!(db.query_points(id, .., QueryOpts::asc()).unwrap().len(), ts.len());
    done(db);
}

#[test]
fn threads_share_one_db() {
    let db = Arc::new(open());
    let ids: Vec<u64> = (0..4).map(|t| db.series(&format!("thread.{t}")).unwrap()).collect();
    let mut handles = Vec::new();
    for (t, &id) in ids.iter().enumerate() {
        let db = Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            for batch in 0..20 {
                let ts: Vec<i64> = (0..1000).map(|i| T0 + (batch * 1000 + i) * 10).collect();
                db.insert_series(id, &ts, &vec![t as f32; 1000]).unwrap();
            }
        }));
    }
    let reader = {
        let db = Arc::clone(&db);
        let ids = ids.clone();
        std::thread::spawn(move || {
            let mut last = 0;
            for _ in 0..200 {
                let (mut n, mut buckets) = (0u64, 0);
                for &id in &ids {
                    let b = db.query_agg(id, .., 0).unwrap();
                    n += b.first().map_or(0, |b| b.count);
                    buckets = b.len() as u64; // 0 while that series' writer has not inserted yet
                }
                assert!(n >= last, "rows went from {last} to {n}");
                last = n;
                // per thread: this thread's last query, whatever the writers query meanwhile
                assert_eq!(db.last_query_stats().result_rows, buckets);
            }
        })
    };
    for h in handles {
        h.join().unwrap();
    }
    reader.join().unwrap();
    for (t, &id) in ids.iter().enumerate() {
        let b = db.query_agg(id, .., 0).unwrap();
        assert_eq!((b[0].count, b[0].min, b[0].max), (20_000, t as f64, t as f64));
    }
    done(Arc::into_inner(db).unwrap());
}

#[test]
fn a_locked_directory_fails_cleanly() {
    let dir = test_dir("tsdb-embedded-lock");
    let db = Db::open(&dir, &opts()).unwrap();
    let id = db.series("x").unwrap();
    db.insert_series(id, &[T0], &[1.0]).unwrap();
    db.flush().unwrap();
    assert_eq!(status(Db::open(&dir, &opts())), Some(Status::Locked));
    // a reader does not need the lock
    let ro = Db::open(&dir, &Options { read_only: true, ..opts() }).unwrap();
    assert_eq!(ro.query_points(ro.series_lookup("x").unwrap(), .., QueryOpts::asc()).unwrap().values, vec![1.0]);
    assert_eq!(status(ro.series("y")), Some(Status::ReadOnly));
    drop(ro);
    db.close().unwrap();
    // closed: the lock is gone
    let again = Db::open(&dir, &opts()).unwrap();
    assert_eq!(again.series_lookup("x").unwrap(), id);
    assert!(again.failed().is_none());
    assert!(!again.parts().unwrap().is_empty());
    // volume paths are canonical (macOS: the temporary directory is a link, /var -> /private/var)
    assert_eq!(again.volumes()[0], dir.canonicalize().unwrap().to_str().unwrap());
    assert!(!Db::simd_level().is_empty());
    done(again);
}

/// Ids are `(volume + 1) << 32 | local`: every real id is past 2^32 and is taken whole, in the database directory
/// and in another volume. An id that is malformed (a zero volume field, as a truncated id has; any of bits 48-63)
/// or names an unknown volume or a local number past the volume's series is unknown to every call, and never
/// reaches the series its low 32 bits name. Queries answer INVALID for a malformed id and no rows for the others.
#[test]
fn ids_carry_the_volume_and_malformed_ones_are_unknown() {
    let dir = test_dir("tsdb-embedded");
    let vol = test_dir("tsdb-embedded-vol");
    let db = Db::open(&dir, &Options { create_volumes: true, ..opts() }).unwrap();
    let a = db.series("a").unwrap();
    let b = db.series_create("b", ValueType::State, None).unwrap();
    let c = db.series_at("c", vol.to_str().unwrap()).unwrap();
    assert_eq!((a >> 32, a & 0xFFFF_FFFF), (1, 0), "{a:#x}");
    assert_eq!((b >> 32, b & 0xFFFF_FFFF), (1, 1), "{b:#x}");
    assert_eq!((c >> 32, c & 0xFFFF_FFFF), (2, 0), "{c:#x}");
    db.insert_series(a, &[T0], &[1.0]).unwrap();
    db.insert(&[c, a], &[T0, T0 + 1], &[7.0, 1.5]).unwrap();
    db.append(c, T0 + 1, 8.0).unwrap();
    assert_eq!(db.series_name(c).unwrap(), "c");
    assert_eq!(db.series_lookup("c").unwrap(), c);
    for bad in [
        a & 0xFFFF_FFFF, // volume field 0: a truncated id
        b & 0xFFFF_FFFF,
        c & 0xFFFF_FFFF,
        a | 1 << 48, // bits 48-63 must be zero
        c | 1 << 63,
        u64::MAX,
        a + (2 << 32),          // a volume the database does not have
        (c & !0xFFFF_FFFF) | 1, // past the volume's series
        a + 1000,
    ] {
        assert_eq!(status(db.insert_series(bad, &[T0 + 1], &[2.0])), Some(Status::Invalid), "{bad:#x}");
        assert_eq!(status(db.insert(&[a, bad], &[T0 + 2, T0 + 3], &[3.0, 4.0])), Some(Status::Invalid));
        assert_eq!(status(db.append(bad, T0 + 4, 5.0)), Some(Status::Invalid));
        assert_eq!(status(db.series_name(bad)), Some(Status::NotFound), "{bad:#x}");
        assert_eq!(status(db.series_type(bad)), Some(Status::NotFound));
        assert!(db.state(bad, "on").is_err());
        assert!(db.delete_range(bad, ..).is_err());
        let multi = db.query_rows_multi(&[bad, a, c], .., QueryOpts::asc(), Default::default()).unwrap();
        if bad >> 48 != 0 || bad >> 32 == 0 {
            // no volume can have it: queries say so rather than answer an empty range
            assert_eq!(status(db.query_points(bad, .., QueryOpts::asc())), Some(Status::Invalid), "{bad:#x}");
            assert_eq!(status(db.query_agg(bad, .., 0)), Some(Status::Invalid), "{bad:#x}");
            assert_eq!(multi[0].as_ref().unwrap_err().status(), Some(Status::Invalid), "{bad:#x}");
        } else {
            assert!(db.query_points(bad, .., QueryOpts::asc()).unwrap().is_empty(), "{bad:#x}");
            assert!(db.query_agg(bad, .., 0).unwrap().iter().all(|x| x.count == 0), "{bad:#x}");
            assert!(multi[0].as_ref().unwrap().ts.is_empty(), "{bad:#x}");
        }
        assert_eq!(multi[1].as_ref().unwrap().ts, vec![T0, T0 + 1]);
        assert_eq!(multi[2].as_ref().unwrap().ts, vec![T0, T0 + 1]);
    }
    db.flush().unwrap();
    assert_eq!(db.query_points(a, .., QueryOpts::asc()).unwrap().values, vec![1.0, 1.5]);
    assert_eq!(db.query_points(c, .., QueryOpts::asc()).unwrap().values, vec![7.0, 8.0]);
    assert_eq!(db.series_name(a).unwrap(), "a");
    assert_eq!(db.series_count(), 3);
    let mut ids = db.series_ids().unwrap();
    ids.sort_unstable();
    assert_eq!(ids, vec![a, b, c]);
    db.close().unwrap();
    let db = Db::open(&dir, &opts()).unwrap();
    assert_eq!((db.series_lookup("a").unwrap(), db.series_lookup("c").unwrap()), (a, c));
    done(db);
    let _ = std::fs::remove_dir_all(vol);
}

#[test]
fn a_missing_parent_directory_is_an_error() {
    let r = Db::open("/proc/no/such/dir", &opts());
    assert!(matches!(r, Err(Error::Server(_))), "{r:?}");
}

/// The engine and tsdbd answer the same questions about the same directory identically.
#[test]
fn tsdbd_answers_what_the_embedded_engine_answered() {
    let dir = test_dir("tsdb-embedded-vs-tcp");
    let mut db = Db::open(&dir, &opts()).unwrap();
    let n = 40_000i64;
    let ts: Vec<i64> = (0..n).map(|i| T0 + i * 7_000).collect();
    let make = |ty, r: std::ops::Range<i64>, shift: i64, codes: [u32; 2]| match ty {
        ValueType::Float32 => Column::new(r.map(|i| ((i + shift) as f32).sin() * 100.0).collect::<Vec<f32>>()),
        ValueType::Float64 => Column::new(r.map(|i| ((i + shift) as f64).sqrt()).collect::<Vec<f64>>()),
        ValueType::Int64 => Column::new(r.map(|i| (i + shift) * i - 5000).collect::<Vec<i64>>()),
        ValueType::Bool => Column::new(r.map(|i| (i + shift) % 3 == 0).collect::<Vec<bool>>()),
        _ => Column::new(r.map(|i| codes[((i + shift) % 2) as usize]).collect::<Vec<u32>>()),
    };
    let mut names = Vec::new();
    for ty in ValueType::ALL {
        let name = format!("cmp.{ty}");
        let id = db.series_create(&name, ty, None).unwrap();
        let codes =
            if ty == ValueType::State { [db.state(id, "a").unwrap(), db.state(id, "b").unwrap()] } else { [0; 2] };
        let valid: Bitmap = (0..n).map(|i| i % 97 != 3).collect();
        let col = make(ty, 0..n, 0, codes);
        db.insert_series_values(id, &ts, ColumnRef::from(&col).with_validity(&valid)).unwrap();
        db.flush().unwrap();
        // corrections that stay in the memtable
        db.insert_series_values(id, &ts[100..110], &make(ty, 100..110, 1, codes)).unwrap();
        names.push(name);
    }
    let groups = [
        Group::new(Unit::Hour),
        Group::new(Unit::Minute).count(15),
        Group::new(Unit::Day).timezone("Europe/Oslo"),
        Group::new(Unit::Week).timezone("America/New_York"),
        Group::new(Unit::Month).timezone("+05:30"),
    ];
    let range = T0 + 1000..T0 + 200_000_000;
    let embedded: Vec<String> = names.iter().flat_map(|name| answers(&mut db, name, range.clone(), &groups)).collect();
    db.close().unwrap();
    let srv = Server::start_in(dir);
    let mut c = srv.client();
    let tcp: Vec<String> = names.iter().flat_map(|name| answers(&mut c, name, range.clone(), &groups)).collect();
    assert_eq!(embedded.len(), tcp.len());
    for (e, t) in embedded.iter().zip(&tcp) {
        assert_eq!(e, t);
    }
}

fn answers(db: &mut impl Database, name: &str, range: std::ops::Range<i64>, groups: &[Group]) -> Vec<String> {
    let id = db.series_lookup(name).unwrap();
    let mut out = vec![db.series_type(id).unwrap().to_string()];
    for o in [QueryOpts::asc(), QueryOpts::desc(), QueryOpts::desc().limit(1000), QueryOpts::asc().limit(1)] {
        out.push(rows_key(&db.query_rows(id, .., o).unwrap()));
        out.push(rows_key(&db.query_rows(id, range.clone(), o).unwrap()));
        for g in groups {
            out.push(buckets_key(&db.query_group(id, range.clone(), Some(g), o).unwrap()));
        }
        out.push(buckets_key(&db.query_group(id, .., None, o).unwrap()));
        let b = [range.start, range.start + 3_600_000, range.start + 90_000_000];
        out.push(buckets_key(&db.query_bounds(id, .., &b, o).unwrap()));
    }
    for interval in [0, 60_000, 86_400_000] {
        out.push(buckets_key(&db.query_agg(id, range.clone(), interval).unwrap()));
    }
    // protocol 8 and 9: aggregates with order and limit, the spread, time-weighted averages, LTTB
    let b = [range.start, range.start + 3_600_000, range.start + 90_000_000];
    for o in [QueryOpts::asc(), QueryOpts::desc().limit(7), QueryOpts::asc().limit(1)] {
        for interval in [0, 60_000, 86_400_000] {
            out.push(buckets_key(&db.query_agg_ex(id, range.clone(), interval, o).unwrap()));
            out.push(key(db.query_spread(id, range.clone(), interval, o)));
        }
        for g in groups {
            out.push(key(db.query_spread_group(id, range.clone(), Some(g), o)));
        }
        out.push(key(db.query_spread_bounds(id, .., &b, o)));
        for t in [3, 100, 5000] {
            out.push(key(db.query_lttb(id, range.clone(), t, o).map(|r| rows_key(&r))));
        }
        out.push(key(db.query_lttb(id, .., 500, o).map(|r| rows_key(&r))));
    }
    for t in [
        TwaOpts::step(),
        TwaOpts::linear(),
        TwaOpts::step().max_gap(10_000).desc(),
        TwaOpts::linear().max_gap(7_000).limit(5),
    ] {
        for interval in [0, 60_000, 86_400_000] {
            out.push(key(db.query_twa(id, range.clone(), interval, t)));
        }
        for g in groups {
            out.push(key(db.query_twa_group(id, range.clone(), Some(g), t)));
            out.push(key(db.query_twa_group(id, .., Some(g), t)));
        }
        out.push(key(db.query_twa_bounds(id, range.clone(), &b, t)));
    }
    out
}

/// An answer or the error, to compare as text (Debug prints floats exactly).
fn key<T: std::fmt::Debug>(r: Result<T, Error>) -> String {
    match r {
        Ok(v) => format!("{v:?}"),
        Err(e) => format!("error {:?}", e.status()),
    }
}
