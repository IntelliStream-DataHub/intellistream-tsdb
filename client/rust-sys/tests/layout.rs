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
            refresh_ms, merge_idle_s, obsolete_part_lifetime_s, verify_checksums, retention_merge_s, clock_ms,
            clock_arg, create_volumes }
        tsdb_values { type_, values, valid }
        tsdb_points { ts, values, n, cap }
        tsdb_bucket { start, count, sum, min, max, isum, imin, imax }
        tsdb_query_opts { order, limit }
        tsdb_group { unit, count, timezone }
        tsdb_spread_bucket { b, range, var_pop, var_samp, stddev_pop, stddev_samp }
        tsdb_twa_opts { method, max_gap_ms, order, limit, no_stats }
        tsdb_twa_bucket { start, twa, area, covered_ms, count }
        tsdb_query_stats { elapsed_s, rows, rows_decoded, rows_from_stats, memtable_rows, bytes, bytes_read, parts,
            granules, granules_from_stats, result_rows }
        tsdb_stats { parts, partitions, rows, bytes, merges, merged_rows, passthrough_granules, memtable_rows,
            broken_parts, follow_errors }
        tsdb_part_info { name, partition, min_ts, max_ts, rows, bytes, granules, level, created_s, written_s }
    };
    rust.push(("tsdb_type", "sizeof", size_of::<tsdb_type>()));
    rust.push(("tsdb_order", "sizeof", size_of::<tsdb_order>()));
    rust.push(("tsdb_unit", "sizeof", size_of::<tsdb_unit>()));
    rust.push(("tsdb_twa_method", "sizeof", size_of::<tsdb_twa_method>()));
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

/// The offsets above only cover the fields listed; a field the header gained (in the middle, at the end, or in
/// what was padding) must be listed too, or the engine reads past the Rust struct or into its padding.
#[test]
fn every_field_of_the_header_is_listed() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let header = std::fs::read_to_string(root.join("../../include/tsdb.h")).unwrap();
    let layout = std::fs::read_to_string(root.join("layout.c")).unwrap();
    let header = strip_comments(&header);
    let mut structs: Vec<&str> = Vec::new();
    for t in layout.split("S(").skip(1).filter_map(|s| s.split(')').next()) {
        if !structs.contains(&t) && header.contains(&format!("typedef struct {t} {{")) {
            structs.push(t);
        }
    }
    assert!(structs.len() >= 8, "found only {structs:?} in layout.c");
    for t in structs {
        let start = header.find(&format!("typedef struct {t} {{")).unwrap();
        let body = &header[start..];
        let body = &body[body.find('{').unwrap() + 1..body.find(&format!("}} {t};")).unwrap()];
        let mut fields = Vec::new();
        for decl in body.split(';').map(str::trim).filter(|d| !d.is_empty()) {
            if let Some(fp) = decl.find("(*") {
                // a function pointer: int64_t (*name)(args)
                fields.push(decl[fp + 2..decl[fp..].find(')').unwrap() + fp].to_string());
                continue;
            }
            let decl = decl.split('[').next().unwrap(); // char name[256]
            // "uint64_t a, b": the names start after the last blank or '*' of the first declarator
            let first = decl.split(',').next().unwrap();
            let names = &decl[first.rfind(|c: char| c.is_whitespace() || c == '*').unwrap() + 1..];
            fields.extend(names.split(',').map(|n| n.trim().trim_start_matches('*').to_string()));
        }
        for f in &fields {
            assert!(layout.contains(&format!("O({t}, {f})")), "{t}.{f} is in tsdb.h but not in layout.c (nor in Rust)");
        }
    }
}

fn strip_comments(s: &str) -> String {
    let mut out = String::new();
    let mut rest = s;
    while let Some(i) = rest.find("/*") {
        out.push_str(&rest[..i]);
        rest = &rest[i + rest[i..].find("*/").unwrap() + 2..];
    }
    out.push_str(rest);
    out.lines().map(|l| l.split("//").next().unwrap()).collect::<Vec<_>>().join("\n")
}
