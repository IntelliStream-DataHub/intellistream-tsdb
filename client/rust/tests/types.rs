//! Value types and nulls (protocol version 2) against a real tsdbd.

mod common;

use std::ops::ControlFlow;

use common::{Server, T0};
use tsdb_client::{Bitmap, Column, ColumnRef, Error, Group, NULL_F64_BITS, QueryOpts, Status, Unit, ValueType, Values};

fn invalid<T: std::fmt::Debug>(r: Result<T, Error>) -> bool {
    r.unwrap_err().status() == Some(Status::Invalid)
}

#[test]
fn series_types() {
    let srv = Server::start();
    let mut c = srv.client();
    let mut ids = Vec::new();
    for ty in ValueType::ALL {
        let id = c.series_create(&format!("t.{ty}"), ty, None).unwrap();
        assert_eq!(c.series_type(id).unwrap(), ty);
        assert_eq!(c.series_create(&format!("t.{ty}"), ty, None).unwrap(), id);
        ids.push(id);
    }
    assert!(invalid(c.series_create("t.float64", ValueType::Int64, None)));
    let plain = c.series("plain").unwrap();
    assert_eq!(c.series_type(plain).unwrap(), ValueType::Float32);
    assert_eq!(c.series_type(999_999).unwrap_err().status(), Some(Status::NotFound));

    let vol = srv.dir.with_extension("volume");
    let v = c.series_create("on.volume", ValueType::Int64, Some(vol.to_str().unwrap())).unwrap();
    c.insert_series_values(v, &[T0], &[42i64]).unwrap();
    assert_eq!(c.query_rows(v, .., QueryOpts::asc()).unwrap().values.to_options::<i64>(), Some(vec![Some(42)]));
}

#[test]
fn int64_nulls_order_and_limit() {
    let srv = Server::start();
    let mut c = srv.client();
    let id = c.series_create("i", ValueType::Int64, None).unwrap();
    let ts: Vec<i64> = (0..12).map(|k| T0 + k * 1000).collect();
    let vals: Vec<Option<i64>> = (0..12).map(|k| (k != 3 && k != 9).then_some(k * 1_000_000_000_000 - 5)).collect();
    c.insert_series_values(id, &ts, &Column::from_options(vals.clone())).unwrap();

    let rows = c.query_rows(id, .., QueryOpts::asc()).unwrap();
    assert_eq!(rows.value_type(), ValueType::Int64);
    assert_eq!(rows.ts, ts);
    assert_eq!(rows.values.null_count(), 2);
    assert_eq!(rows.values.to_options::<i64>().unwrap(), vals);

    let newest = c.query_rows(id, .., QueryOpts::desc().limit(3)).unwrap();
    assert_eq!(newest.ts, vec![ts[11], ts[10], ts[9]]);
    assert_eq!(newest.values.to_options::<i64>().unwrap(), vec![vals[11], vals[10], None]);
    let none = c.query_rows(id, .., QueryOpts::desc().limit(0)).unwrap();
    assert!(none.is_empty());
    assert_eq!(none.value_type(), ValueType::Int64);
    assert!(c.query_rows(999_999, .., QueryOpts::asc()).unwrap().is_empty());

    // one bucket, and calendar seconds newest first
    let b = c.query_agg(id, .., 0).unwrap();
    let known: Vec<i64> = vals.iter().flatten().copied().collect();
    assert_eq!((b.len(), b[0].count), (1, 10));
    assert_eq!((b[0].isum, b[0].imin, b[0].imax), (known.iter().sum(), known[0], known[9]));
    let g = c.query_group(id, .., Some(&Group::new(Unit::Second).count(4).timezone("UTC")), QueryOpts::desc().limit(1));
    let g = g.unwrap();
    assert_eq!((g.len(), g[0].start, g[0].count), (1, T0 + 8000, 3));
    assert_eq!(g[0].isum, vals[8].unwrap() + vals[10].unwrap() + vals[11].unwrap());

    // a bucket whose rows are all null is returned, with count 0
    let only = c.series_create("only.nulls", ValueType::Int64, None).unwrap();
    c.insert_series_values(only, &[T0, T0 + 1], &Column::from_options([None::<i64>, None])).unwrap();
    let b = c.query_agg(only, .., 0).unwrap();
    assert_eq!((b.len(), b[0].count), (1, 0));

    // last write wins, also between a value and a null
    c.insert_series_values(id, &[ts[0], ts[3]], &Column::from_options([None, Some(7i64)])).unwrap();
    let opts = c.query_rows(id, ..ts[4], QueryOpts::asc()).unwrap().values.to_options::<i64>().unwrap();
    assert_eq!(opts, vec![None, vals[1], vals[2], Some(7)]);
}

#[test]
fn nulls_at_chunk_boundaries() {
    let srv = Server::start();
    let mut c = srv.client();
    const CHUNK: usize = 16384; // rows per QUERY_VALUES chunk on the server
    let n = 3 * CHUNK + 100;
    let ts: Vec<i64> = (0..n as i64).map(|i| T0 + i * 1000).collect();
    let v: Vec<i64> = (0..n as i64).collect();

    // nulls in the first and third chunk only, at both edges; none in the second
    let a = c.series_create("edges", ValueType::Int64, None).unwrap();
    let nulls = [0, 5, CHUNK - 1, 2 * CHUNK, 2 * CHUNK + 1, 3 * CHUNK - 1, n - 1];
    let valid: Bitmap = (0..n).map(|i| !nulls.contains(&i)).collect();
    c.insert_series_values(a, &ts, ColumnRef::from(&v).with_validity(&valid)).unwrap();
    // nulls only in the second chunk: the collected bitmap starts after a chunk without one
    let b = c.series_create("late", ValueType::Int64, None).unwrap();
    let late: Bitmap = (0..n).map(|i| i != CHUNK + 7).collect();
    c.insert_series_values(b, &ts, ColumnRef::from(&v).with_validity(&late)).unwrap();

    for (id, valid) in [(a, &valid), (b, &late)] {
        let rows = c.query_rows(id, .., QueryOpts::asc()).unwrap();
        assert_eq!(rows.ts, ts);
        assert_eq!(rows.values.valid.as_ref(), Some(valid));
        assert_eq!(
            rows.values
                .as_slice::<i64>()
                .unwrap()
                .iter()
                .enumerate()
                .filter(|&(i, &x)| valid.get(i) && x != v[i])
                .count(),
            0
        );

        // newest first: the same rows reversed, chunked differently
        let desc = c.query_rows(id, .., QueryOpts::desc()).unwrap();
        let back: Vec<Option<i64>> = desc.values.to_options().unwrap().into_iter().rev().collect();
        assert_eq!(back, rows.values.to_options::<i64>().unwrap());

        // the streaming form: chunks of 16384 with a bitmap only where there are nulls
        let mut chunks = Vec::new();
        c.query_values(id, .., QueryOpts::asc(), |t, col| {
            assert_eq!(t.len(), col.len());
            chunks.push((t[0], col.len(), col.null_count(), col.valid.is_some()));
            ControlFlow::Continue(())
        })
        .unwrap();
        let expect_nulls: Vec<usize> =
            (0..4).map(|k| (k * CHUNK..((k + 1) * CHUNK).min(n)).filter(|&i| !valid.get(i)).count()).collect();
        assert_eq!(chunks.iter().map(|c| c.1).collect::<Vec<_>>(), vec![CHUNK, CHUNK, CHUNK, 100]);
        assert_eq!(chunks.iter().map(|c| c.2).collect::<Vec<_>>(), expect_nulls);
        assert!(chunks.iter().all(|c| c.3 == (c.2 > 0)));
    }

    // stopping early leaves the connection in step
    let mut calls = 0;
    c.query_values(a, .., QueryOpts::asc(), |_, _| {
        calls += 1;
        ControlFlow::Break(())
    })
    .unwrap();
    assert_eq!(calls, 1);
    assert_eq!(c.series_type(a).unwrap(), ValueType::Int64);
}

#[test]
fn exact_integer_aggregates() {
    let srv = Server::start();
    let mut c = srv.client();
    let id = c.series_create("big", ValueType::Int64, None).unwrap();
    let max = i64::MAX;
    // f64 cannot tell these apart; the integer fields can
    c.insert_series_values(id, &[T0, T0 + 1, T0 + 2, T0 + 3], &[max - 10, -(max - 20), 7, max - 11]).unwrap();
    let b = c.query_agg(id, ..T0 + 3, 0).unwrap()[0];
    assert_eq!((b.count, b.isum, b.imin, b.imax), (3, 17, -(max - 20), max - 10));
    let b = c.query_agg(id, T0 + 2.., 0).unwrap()[0];
    assert_eq!((b.count, b.isum, b.imin, b.imax), (2, max - 4, 7, max - 11));
    assert_eq!(b.max, (max - 11) as f64); // the f64 view rounds: it cannot tell max - 11 from max - 10
    assert_eq!(b.max, (max - 10) as f64);

    // the sum saturates
    let s = c.series_create("sat", ValueType::Int64, None).unwrap();
    c.insert_series_values(s, &[T0, T0 + 1, T0 + 2], &[max, max, max - 1]).unwrap();
    assert_eq!(c.query_agg(s, .., 0).unwrap()[0].isum, max);
    let m = c.series_create("satmin", ValueType::Int64, None).unwrap();
    c.insert_series_values(m, &[T0, T0 + 1], &[-max, -max]).unwrap();
    assert_eq!(c.query_agg(m, .., 0).unwrap()[0].isum, i64::MIN);

    // i64::MIN is the null representation
    c.insert_series_values(m, &[T0 + 2], &[i64::MIN]).unwrap();
    let rows = c.query_rows(m, .., QueryOpts::asc()).unwrap();
    assert_eq!(rows.values.to_options::<i64>().unwrap(), vec![Some(-max), Some(-max), None]);
}

#[test]
fn bools_and_floats() {
    let srv = Server::start();
    let mut c = srv.client();
    let b = c.series_create("b", ValueType::Bool, None).unwrap();
    let vals = [Some(true), Some(false), None, Some(true), Some(false), Some(true)];
    let ts: Vec<i64> = (0..6).map(|k| T0 + k * 1000).collect();
    c.insert_series_values(b, &ts, &Column::from_options(vals)).unwrap();
    let rows = c.query_rows(b, .., QueryOpts::asc()).unwrap();
    assert_eq!(rows.values.to_options::<bool>().unwrap(), vals.to_vec());
    let bk = c.query_agg(b, .., 0).unwrap()[0];
    assert_eq!((bk.count, bk.isum, bk.imin, bk.imax, bk.sum), (5, 3, 0, 1, 3.0));

    // float64: precision beyond f32, extremes, the null NaN and an ordinary NaN
    let d = c.series_create("d", ValueType::Float64, None).unwrap();
    let null_nan = f64::from_bits(NULL_F64_BITS);
    c.insert_series_values(d, &ts[..5], &[0.1, 1e300, -3.0, null_nan, f64::NAN]).unwrap();
    let rows = c.query_rows(d, .., QueryOpts::asc()).unwrap();
    let got = rows.values.to_options::<f64>().unwrap();
    assert_eq!(&got[..3], &[Some(0.1), Some(1e300), Some(-3.0)]);
    assert_eq!(got[3], None);
    assert!(got[4].unwrap().is_nan());
    let bk = c.query_agg(d, ..ts[3], 0).unwrap()[0];
    assert_eq!((bk.count, bk.min, bk.max), (3, -3.0, 1e300));

    // float32 through the typed calls, nulls read as NaN by the version 1 query
    let f = c.series("f").unwrap();
    c.insert_series_values(f, &ts[..3], &Column::from_options([Some(1.5f32), None, Some(2.5)])).unwrap();
    let p = c.query_points(f, .., QueryOpts::asc()).unwrap();
    assert_eq!((p.values[0], p.values[2]), (1.5, 2.5));
    assert!(p.values[1].is_nan());
    let r = c.query_rows(f, .., QueryOpts::asc()).unwrap();
    assert_eq!(r.values.to_options::<f32>().unwrap(), vec![Some(1.5), None, Some(2.5)]);
}

#[test]
fn states() {
    let srv = Server::start();
    let mut c = srv.client();
    let s = c.series_create("pump.mode", ValueType::State, None).unwrap();
    let s2 = c.series_create("fan.mode", ValueType::State, None).unwrap();
    let on = c.state(s, "on").unwrap();
    let off = c.state(s, "off").unwrap();
    assert!(on > 0 && off > 0 && on != off);
    assert_eq!(c.state(s, "on").unwrap(), on);
    assert_eq!(c.state_lookup(s, "off").unwrap(), off);
    assert_eq!(c.state_lookup(s2, "on").unwrap(), on); // one dictionary per volume
    assert_eq!(c.state_lookup(s, "standby").unwrap_err().status(), Some(Status::NotFound));
    assert_eq!(c.state_name(s, on).unwrap(), "on");
    assert_eq!(c.state_name(s, 999).unwrap_err().status(), Some(Status::NotFound));
    assert!(matches!(c.state(s, ""), Err(Error::InvalidArgument(_))));

    let ts = [T0, T0 + 1000, T0 + 2000, T0 + 3000];
    c.insert_series_values(s, &ts, &Column::from_options([Some(on), Some(off), None, Some(on)])).unwrap();
    let rows = c.query_rows(s, .., QueryOpts::asc()).unwrap();
    let codes = rows.values.to_options::<u32>().unwrap();
    assert_eq!(codes, vec![Some(on), Some(off), None, Some(on)]);
    let names: Vec<Option<String>> = codes.iter().map(|c2| c2.map(|k| c.state_name(s, k).unwrap())).collect();
    assert_eq!(names[1].as_deref(), Some("off"));
    assert_eq!(c.query_agg(s, .., 0).unwrap()[0].count, 3);
    // code 0 is the null representation
    c.insert_series_values(s, &[T0], &[0u32]).unwrap();
    assert!(c.query_rows(s, ..T0 + 1, QueryOpts::asc()).unwrap().values.is_null(0));
}

#[test]
fn many_series_layout_and_pipelining() {
    let srv = Server::start();
    let mut c = srv.client();
    let x = c.series_create("x", ValueType::Bool, None).unwrap();
    let y = c.series_create("y", ValueType::Bool, None).unwrap();
    // 5 bools + a 1-byte bitmap: the series column needs padding to stay aligned
    let col = Column::from_options([Some(true), None, Some(false), Some(true), Some(true)]);
    c.insert_values(&[x, y, x, y, x], &[T0, T0, T0 + 1, T0 + 1, T0 + 2], &col).unwrap();
    let rx = c.query_rows(x, .., QueryOpts::asc()).unwrap();
    let ry = c.query_rows(y, .., QueryOpts::asc()).unwrap();
    assert_eq!(rx.values.to_options::<bool>().unwrap(), vec![Some(true), Some(false), Some(true)]);
    assert_eq!(ry.values.to_options::<bool>().unwrap(), vec![None, Some(true)]);
    for n in 0..9 {
        // every padding length, without a bitmap
        let s: Vec<u32> = vec![x; n];
        let t: Vec<i64> = (0..n as i64).map(|i| T0 + 100 + i).collect();
        c.insert_values(&s, &t, &vec![true; n]).unwrap();
    }

    // pipelined: the last write wins, errors come back from wait
    let i = c.series_create("pipe", ValueType::Int64, None).unwrap();
    for k in 0..2000i64 {
        c.insert_series_values_async(i, &[T0], &[k]).unwrap();
    }
    c.insert_values_async(&[i], &[T0], &Column::from_options([None::<i64>])).unwrap();
    c.insert_series_values_async(x, &[T0 + 5], &[1i64]).unwrap(); // wrong type
    c.insert_values_async(&[i], &[T0 + 1], &[5i64]).unwrap();
    assert!(invalid(c.wait()));
    let r = c.query_rows(i, .., QueryOpts::asc()).unwrap();
    assert_eq!(r.values.to_options::<i64>().unwrap(), vec![None, Some(5)]);
}

#[test]
fn type_mismatches() {
    let srv = Server::start();
    let mut c = srv.client();
    let d = c.series_create("d", ValueType::Float64, None).unwrap();
    let i = c.series_create("i", ValueType::Int64, None).unwrap();
    let f = c.series("f").unwrap();
    assert!(invalid(c.insert_series_values(d, &[T0], &[1i64])));
    assert!(invalid(c.insert_series_values(f, &[T0], &[1.0f64])));
    assert!(invalid(c.insert_values(&[d, i], &[T0, T0], &[1i64, 2])));
    assert!(invalid(c.insert_series(d, &[T0], &[1.0])));
    assert!(invalid(c.query_points(i, .., QueryOpts::asc())));
    assert!(invalid(c.series_create("d", ValueType::Bool, None)));
    // checked before sending
    let short = Bitmap::all_valid(1);
    let e = c.insert_series_values(i, &[T0, T0 + 1], ColumnRef::from(&[1i64, 2]).with_validity(&short));
    assert!(matches!(e, Err(Error::InvalidArgument(_))));
    assert!(matches!(c.insert_series_values(i, &[T0], &[1i64, 2]), Err(Error::InvalidArgument(_))));
    // nothing was written, and the connection is fine
    assert!(c.query_rows(i, .., QueryOpts::asc()).unwrap().is_empty());
    assert!(c.query_rows(d, .., QueryOpts::asc()).unwrap().is_empty());
    assert!(matches!(c.query_rows(d, .., QueryOpts::asc()).unwrap().values.values, Values::Float64(_)));
}

#[test]
fn frames_split_inside_the_bitmap() {
    // more rows than one INSERT_VALUES frame holds (6M): the bitmap is split between frames
    let srv = Server::start();
    let mut c = srv.client();
    let b = c.series_create("many", ValueType::Bool, None).unwrap();
    let n = (6 << 20) + 1001;
    let ts: Vec<i64> = (0..n as i64).map(|i| T0 + i).collect();
    let v: Vec<bool> = (0..n).map(|i| i % 3 == 0).collect();
    let cut = 6 << 20;
    let valid: Bitmap = (0..n).map(|i| !(i % 1000 == 7 || i == cut - 1 || i == cut)).collect();
    c.insert_series_values(b, &ts, ColumnRef::from(&v).with_validity(&valid)).unwrap();
    let bk = c.query_agg(b, .., 0).unwrap()[0];
    let known = (0..n).filter(|&i| valid.get(i));
    assert_eq!(bk.count as usize, known.clone().count());
    assert_eq!(bk.isum as usize, known.filter(|&i| v[i]).count());
    let around = c.query_rows(b, T0 + cut as i64 - 3..T0 + cut as i64 + 3, QueryOpts::asc()).unwrap();
    let want: Vec<Option<bool>> = (cut - 3..cut + 3).map(|i| valid.get(i).then_some(v[i])).collect();
    assert_eq!(around.values.to_options::<bool>().unwrap(), want);
}
