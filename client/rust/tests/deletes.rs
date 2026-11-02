//! Range deletes, replaces and retention (protocol version 7) against a real tsdbd.

mod common;

use std::time::{SystemTime, UNIX_EPOCH};

use common::{Server, T0};
use tsdb_client::{Bitmap, Client, Column, ColumnRef, Error, Group, QueryOpts, Status, TwaOpts, Unit, ValueType};

fn invalid<T: std::fmt::Debug>(r: Result<T, Error>) -> bool {
    r.unwrap_err().status() == Some(Status::Invalid)
}

fn rows(c: &mut Client, id: u64) -> (Vec<i64>, Vec<Option<i64>>) {
    let r = c.query_rows(id, .., QueryOpts::asc()).unwrap();
    (r.ts, r.values.to_options::<i64>().unwrap())
}

#[test]
fn delete_and_replace() {
    let srv = Server::start();
    let mut c = srv.client();
    let id = c.series_create("d", ValueType::Int64, None).unwrap();
    let ts: Vec<i64> = (0..10).map(|k| T0 + k * 1000).collect();
    let vals: Vec<i64> = (0..10).collect();
    c.insert_series_values(id, &ts, &vals).unwrap();
    c.flush().unwrap();

    // [T0 + 2000, T0 + 5000): rows 2, 3, 4; an inclusive range ends one ms later
    c.delete_range(id, T0 + 2000..T0 + 5000).unwrap();
    c.delete_range(id, T0 + 7000..=T0 + 7000).unwrap();
    let keep = [0, 1, 5, 6, 8, 9];
    assert_eq!(rows(&mut c, id), (keep.map(|k| ts[k]).to_vec(), keep.map(|k| Some(k as i64)).to_vec()));

    // a later insert into a deleted range is visible
    c.insert_series_values(id, &[ts[3]], &[33i64]).unwrap();
    assert_eq!(rows(&mut c, id).0.len(), 7);

    // replace with a null among the new rows, through a column with a validity bitmap
    let col = Column::from_options([Some(100i64), None]);
    c.replace_range(id, T0..T0 + 6000, &[T0 + 500, T0 + 5500], &col).unwrap();
    assert_eq!(
        rows(&mut c, id),
        (vec![T0 + 500, T0 + 5500, ts[6], ts[8], ts[9]], vec![Some(100), None, Some(6), Some(8), Some(9)])
    );
    // and through a borrowed bitmap of more than one byte
    let n = 11;
    let rts: Vec<i64> = (0..n).map(|k| T0 + 100 * k).collect();
    let rv: Vec<i64> = (0..n).map(|k| -k).collect();
    let valid: Bitmap = (0..n).map(|k| k != 9).collect();
    let r = ColumnRef::from(&rv[..]).with_validity(&valid);
    c.replace_range(id, T0..T0 + 1100, &rts, r).unwrap();
    let (got_ts, got) = rows(&mut c, id);
    assert_eq!(&got_ts[..n as usize], &rts[..]);
    assert_eq!(got[9], None);
    assert_eq!(got[10], Some(-10));
    assert_eq!(got_ts.len(), n as usize + 4);

    // no rows: a delete
    c.replace_range(id, .., &[], &Column::empty(ValueType::Int64)).unwrap();
    c.optimize().unwrap();
    assert!(rows(&mut c, id).0.is_empty());

    // a timestamp outside the range, a column of another type, lengths that differ
    assert!(invalid(c.replace_range(id, T0..T0 + 10, &[T0 + 10], &[1i64])));
    assert!(invalid(c.replace_range(id, T0..T0 + 10, &[T0], &[1.0f64])));
    assert!(matches!(c.replace_range(id, T0..T0 + 10, &[T0], &[1i64, 2]), Err(Error::InvalidArgument(_))));
    // the connection is still good after errors from the server
    c.insert_series_values(id, &[T0], &[5i64]).unwrap();
    assert_eq!(rows(&mut c, id), (vec![T0], vec![Some(5)]));
}

#[test]
fn a_pending_insert_error_is_reported_by_the_next_call() {
    let srv = Server::start();
    let mut c = srv.client();
    let id = c.series_create("p", ValueType::Int64, None).unwrap();
    c.insert_series_values_async(id, &[T0], &[1.0f64]).unwrap();
    assert!(invalid(c.delete_range(id, ..)));
    c.delete_range(id, ..).unwrap();
}

#[test]
fn retention() {
    let srv = Server::start();
    let mut c = srv.client();
    let id = c.series_create("r", ValueType::Int64, None).unwrap();
    assert_eq!(c.retention(id).unwrap(), 0);
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    const HOUR: i64 = 3_600_000;
    c.insert_series_values(id, &[now - 2 * HOUR, now - HOUR / 6], &[1i64, 2]).unwrap();
    c.set_retention(id, HOUR).unwrap();
    assert_eq!(c.retention(id).unwrap(), HOUR);
    assert_eq!(rows(&mut c, id), (vec![now - HOUR / 6], vec![Some(2)]));
    assert!(invalid(c.set_retention(id, -1)));
    c.set_retention(id, 0).unwrap();
    assert_eq!(c.retention(id).unwrap(), 0);
}

/// More local periods than one request carries (QUERY_BOUNDS takes 32 Mi bounds, QUERY_TWA 56 bytes' worth fewer; the
/// calendar stops at 64 Mi): every grouped call fails with TOO_MANY_BUCKETS, as in the C and Java clients.
#[test]
fn over_wide_local_ranges_are_too_many_buckets() {
    let srv = Server::start();
    let mut c = srv.client();
    let too_many = |e: Error| assert_eq!(e.status(), Some(Status::TooManyBuckets), "{e:?}");
    let secs = Group::new(Unit::Second).timezone("Europe/Oslo");
    let wide = c.series_create("wide", ValueType::Float64, None).unwrap();
    c.insert_series_values(wide, &[T0, T0 + 450 * 86_400_000], &[1.0f64, 2.0]).unwrap(); // 38.9 M seconds
    let wider = c.series_create("wider", ValueType::Float64, None).unwrap();
    c.insert_series_values(wider, &[T0, T0 + 800 * 86_400_000], &[1.0f64, 2.0]).unwrap(); // 69.1 M seconds
    for id in [wide, wider] {
        too_many(c.query_group(id, .., Some(&secs), QueryOpts::asc()).unwrap_err());
        too_many(c.query_spread_group(id, .., Some(&secs), QueryOpts::asc()).unwrap_err());
        too_many(c.query_twa_group(id, .., Some(&secs), TwaOpts::step()).unwrap_err());
    }
    // bounded on both sides, TWA covers the range: 33,554,418 seconds is one past QUERY_TWA's cap
    too_many(c.query_twa_group(wide, T0..T0 + 33_554_418_000, Some(&secs), TwaOpts::step()).unwrap_err());
    assert!(!c.is_broken());
    c.close().unwrap();
}
