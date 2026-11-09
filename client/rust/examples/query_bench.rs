// SPDX-License-Identifier: Apache-2.0
//! Query benchmark: the random windows of `tsdbc-bench query`, over TCP from this client.
//!
//! ```text
//! cargo run --release --example query_bench -- [--addr 127.0.0.1:7070] [--series 100] [--points 1000000]
//!     [--conns 1] [--seconds 3]
//! ```
//!
//! Every connection (a thread each) draws windows from the same generator and seed as the C benchmark
//! (examples/common/mod.rs, tools/cbench.h): raw rows of 3600 samples and two aggregate classes. The raw rows
//! are counted in the callback and not kept; the aggregates are counted from the returned buckets. The output is
//! the line of `tsdbc-bench query` for each class, and a RESULT line for bench/clients/run.sh.

mod common;

use std::ops::ControlFlow;
use std::time::Instant;

use common::{CLASSES, Class};
use tsdb_client::{Client, QueryOpts};

struct Args {
    addr: String,
    series: u32,
    points: u64,
    conns: usize,
    seconds: f64,
}

fn parse() -> Result<Args, String> {
    let mut a = Args { addr: "127.0.0.1:7070".into(), series: 100, points: 1_000_000, conns: 1, seconds: 3.0 };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut val = |name: &str| it.next().ok_or(format!("{name} needs a value"));
        let bad = |s: &str, e: &dyn std::fmt::Display| format!("{s}: {e}");
        match arg.as_str() {
            "--addr" => a.addr = val("--addr")?,
            "--series" => a.series = val("--series").and_then(|s| s.parse().map_err(|e| bad(&s, &e)))?,
            "--points" => a.points = val("--points").and_then(|s| s.parse().map_err(|e| bad(&s, &e)))?,
            "--conns" => a.conns = val("--conns").and_then(|s| s.parse().map_err(|e| bad(&s, &e)))?,
            "--seconds" => a.seconds = val("--seconds").and_then(|s| s.parse().map_err(|e| bad(&s, &e)))?,
            _ => return Err(format!("unknown option {arg}")),
        }
    }
    if a.conns == 0 || a.series == 0 {
        return Err("usage: query_bench [--addr HOST:PORT] [--series N] [--points N] [--conns N] [--seconds S]".into());
    }
    Ok(a)
}

/// One connection's queries for `seconds`; returns (queries, rows, seconds).
fn querier(args: &Args, k: &Class, thread: usize) -> (u64, u64, f64) {
    let mut c = Client::connect(args.addr.as_str()).expect("connect");
    // a window's series is a number, an index into this table of ids (64-bit, with the volume), found by name
    let ids: Vec<u64> = (0..args.series)
        .map(|i| c.series_lookup(&format!("{}.{i}", common::NAMES[i as usize % 7])).expect("series"))
        .collect();
    let mut r = common::seed(thread);
    let (mut queries, mut rows) = (0u64, 0u64);
    let t0 = Instant::now();
    while t0.elapsed().as_secs_f64() < args.seconds {
        for _ in 0..16 {
            let (s, start, end) = common::window(&mut r, args.series, args.points, k.window);
            if !k.agg {
                c.query_raw(ids[s as usize], start..end, QueryOpts::default(), |ts, _| {
                    rows += ts.len() as u64;
                    ControlFlow::Continue(())
                })
                .expect("query");
            } else {
                let buckets =
                    c.query_agg(ids[s as usize], start..end, k.bucket as i64 * common::step_ms(s)).expect("query");
                rows += buckets.iter().map(|b| b.count).sum::<u64>();
            }
            queries += 1;
        }
    }
    let secs = t0.elapsed().as_secs_f64();
    c.close().expect("close");
    (queries, rows, secs)
}

fn main() {
    let args = parse().unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(2);
    });
    for k in &CLASSES {
        let cpu0 = common::cpu_s();
        let results: Vec<(u64, u64, f64)> = std::thread::scope(|sc| {
            let a = &args;
            let handles: Vec<_> = (0..args.conns).map(|i| sc.spawn(move || querier(a, k, i))).collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let queries: u64 = results.iter().map(|r| r.0).sum();
        let rows: u64 = results.iter().map(|r| r.1).sum();
        let secs = results.iter().map(|r| r.2).fold(0.0, f64::max);
        common::report_query("rust", k, "conn", args.conns, queries, rows, secs, common::cpu_s() - cpu0);
        if rows == 0 {
            eprintln!("query: class {} returned no rows: the server is empty or does not hold the data set", k.id);
            std::process::exit(1);
        }
    }
}
