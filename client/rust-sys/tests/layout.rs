//! The Rust declarations must lay out every struct exactly as the C compiler does.

use std::mem::{offset_of, size_of};
use tsdb_sys::*;

#[test]
fn structs_match_the_c_header() {
    macro_rules! layout {
        ($($t:ident { $($f:ident),* })*) => {{
            let mut v = Vec::new();
            $(
                v.push((stringify!($t), "sizeof", size_of::<$t>()));
                $(v.push((stringify!($t), stringify!($f), offset_of!($t, $f)));)*
            )*
            v
        }};
    }
    let mut rust = layout! {
        tsdb_options { partition_ms, granule_rows, zstd_level, value_codec, fsync, merge_threads, query_threads,
            buffer_rows, flush_interval_ms, wal_sync_ms, wal_max_batch_rows, max_part_bytes, read_only, follow,
            refresh_ms, merge_idle_s, obsolete_part_lifetime_s }
        tsdb_values { type_, values, valid }
        tsdb_points { ts, values, n, cap }
        tsdb_bucket { start, count, sum, min, max, isum, imin, imax }
        tsdb_query_opts { order, limit }
        tsdb_group { unit, count, timezone }
        tsdb_query_stats { elapsed_s, rows, rows_decoded, rows_from_stats, memtable_rows, bytes, bytes_read, parts,
            granules, granules_from_stats, result_rows }
        tsdb_stats { parts, partitions, rows, bytes, merges, merged_rows, passthrough_granules, memtable_rows }
        tsdb_part_info { name, partition, min_ts, max_ts, rows, bytes, granules, level, created_s, written_s }
    };
    rust.push(("tsdb_type", "sizeof", size_of::<tsdb_type>()));
    rust.push(("tsdb_order", "sizeof", size_of::<tsdb_order>()));
    rust.push(("tsdb_unit", "sizeof", size_of::<tsdb_unit>()));
    let c = unsafe {
        let mut n = 0;
        let p = layout::tsdb_sys_layout(&mut n);
        std::slice::from_raw_parts(p, n).to_vec()
    };
    assert_eq!(c.len(), rust.len(), "layout.c and this test list different fields");
    for ((t, f, r), c) in rust.iter().zip(&c) {
        assert_eq!(r, c, "{t}.{f}: Rust {r}, C {c}");
    }
}

#[test]
fn engine_is_linked() {
    let mut o = tsdb_options::default();
    unsafe { tsdb_options_init(&mut o) };
    assert_eq!(o.granule_rows, 8192);
    let level = unsafe { std::ffi::CStr::from_ptr(tsdb_simd_level()) };
    assert!(!level.to_bytes().is_empty());
    eprintln!("simd level: {}", level.to_string_lossy());
}
