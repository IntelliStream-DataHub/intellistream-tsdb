//! The engine exports only its `tsdb_*` API from the static build. This binary defines C
//! functions with names the engine uses internally; were the engine's own definitions global, the
//! link would fail with duplicate symbols (or, through an archive, bind the engine to these).

use std::ffi::{CString, c_void};
use std::ptr;

macro_rules! clash {
    ($($name:ident),*) => {$(
        #[unsafe(no_mangle)]
        pub extern "C" fn $name() -> *mut c_void {
            panic!(concat!("the engine called the application's ", stringify!($name)))
        }
    )*};
}
// one name from each of: the thread pool, codecs, WAL, parts, memtable, SIMD dispatch, hashing
clash!(pool_new, codec_new, wal_open, part_open, mt_new, simd_init, xxh64, insert_rows, series_open);

#[test]
fn engine_keeps_its_internal_names_to_itself() {
    let dir = std::env::temp_dir().join(format!("tsdb-sys-symbols-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = CString::new(dir.to_str().unwrap()).unwrap();
    // a whole round trip through the engine: open, create, insert, flush, query, close
    unsafe {
        let mut db = ptr::null_mut();
        assert_eq!(tsdb_sys::tsdb_open(path.as_ptr(), ptr::null(), &mut db), 0);
        let name = CString::new("s").unwrap();
        let mut id = 0u32;
        assert_eq!(tsdb_sys::tsdb_series(db, name.as_ptr(), &mut id), 0);
        let ts: Vec<i64> = (0..1000).map(|i| i * 1000).collect();
        let vals: Vec<f32> = (0..1000).map(|i| i as f32).collect();
        assert_eq!(tsdb_sys::tsdb_insert_series(db, id, ts.as_ptr(), vals.as_ptr(), 1000), 0);
        assert_eq!(tsdb_sys::tsdb_flush(db), 0);
        let (mut b, mut n) = (ptr::null_mut(), 0usize);
        assert_eq!(tsdb_sys::tsdb_query_agg(db, id, 0, i64::MAX, i64::MAX / 2, &mut b, &mut n), 0);
        let buckets = std::slice::from_raw_parts(b, n);
        assert_eq!(buckets.iter().map(|b| b.count).sum::<u64>(), 1000);
        assert_eq!(buckets.iter().map(|b| b.sum).sum::<f64>(), 499500.0);
        tsdb_sys::tsdb_free(b.cast());
        assert_eq!(tsdb_sys::tsdb_close(db), 0);
    }
    let _ = std::fs::remove_dir_all(&dir);
}
