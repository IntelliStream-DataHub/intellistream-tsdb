// SPDX-License-Identifier: AGPL-3.0-only
//! Embedded benchmark for bench/clients/run.sh: the `--load` and `--qps` modes of `build/tsdb-bench`, through
//! [`tsdb_client::Db`] (the engine in this process, no tsdbd).
//!
//! ```text
//! build/tsdb-bench --export /data/x.rowbinary --series 100 --points 1000000
//! cargo run --release --features embedded --example embedded_bench -- --dir /data/emb --load /data/x.rowbinary \
//!     [--threads 4] [--batch 1000000] [--zstd 1] [--no-fsync] [--optimize]
//! cargo run --release --features embedded --example embedded_bench -- --dir /data/emb --qps THREADS \
//!     [--series 100] [--points 1000000] [--seconds 3] [--query-threads N]
//! ```
//!
//! `--load` wipes `--dir`, creates the series under tsdb-bench's names (ids in file order), and has each of
//! `--threads` writers insert one contiguous slice of the file's rows in batches of `--batch`, copying them from
//! the file's packed rows into columns inside the timing, as tsdb-bench does. Then (after `--optimize`) it checks
//! that the database holds exactly the file's rows: count and sum per series from an aggregate, and a hash of
//! every (timestamp, value) read back (tools/cbench.h).
//!
//! `--qps` opens an existing database and runs the random windows of the three classes of examples/common/mod.rs
//! on THREADS threads sharing one `Db`, `--seconds` per class: the same generator, seeds and windows as
//! `tsdb-bench --qps`, so the rows line up with it and with the TCP clients. Both modes print tsdb-bench's lines
//! and a RESULT line (client `embedded-rust...`) for bench/clients/run.sh.

mod common;

use std::ops::ControlFlow;
use std::time::Instant;

use common::{CLASSES, Class};
use tsdb_client::QueryOpts;
use tsdb_embedded::{Db, Options};

/// tools/datasets.h ds_info[i].name for the 7 kinds of the mixed data set (series i is kind i % 7).
const NAMES: [&str; 7] = ["hdd_temp", "cpu_temp", "tank_bar", "tank_psi", "vibration", "flow_rate", "flow_total"];
const USAGE: &str = "usage: embedded_bench --dir DIR --load FILE [--threads N] [--batch ROWS] [--zstd L] [--no-fsync] \
                     [--optimize]\n       embedded_bench --dir DIR --qps THREADS [--series N] [--points N] \
                     [--seconds S] [--query-threads N]";

struct Args {
    dir: String,
    load: Option<String>,
    qps: usize,
    threads: usize,
    batch: usize,
    zstd: i32,
    fsync: bool,
    optimize: bool,
    series: u32,
    points: u64,
    seconds: f64,
    query_threads: i32,
}

fn parse() -> Result<Args, String> {
    let mut a = Args {
        dir: String::new(),
        load: None,
        qps: 0,
        threads: 4,
        batch: 1_000_000,
        zstd: 1,
        fsync: true,
        optimize: false,
        series: 100,
        points: 1_000_000,
        seconds: 3.0,
        query_threads: 0,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--no-fsync" => a.fsync = false,
            "--optimize" => a.optimize = true,
            _ => {
                let v = it.next().ok_or(format!("{arg} needs a value"))?;
                let num = |v: &str| v.parse::<u64>().map_err(|e| format!("{arg} {v}: {e}"));
                match arg.as_str() {
                    "--dir" => a.dir = v,
                    "--load" => a.load = Some(v),
                    "--qps" => a.qps = num(&v)? as usize,
                    "--threads" => a.threads = num(&v)? as usize,
                    "--batch" => a.batch = num(&v)? as usize,
                    "--zstd" => a.zstd = num(&v)? as i32,
                    "--series" => a.series = num(&v)? as u32,
                    "--points" => a.points = num(&v)?,
                    "--seconds" => a.seconds = v.parse().map_err(|e| format!("{arg} {v}: {e}"))?,
                    "--query-threads" => a.query_threads = num(&v)? as i32,
                    _ => return Err(format!("unknown option {arg}\n{USAGE}")),
                }
            }
        }
    }
    if a.dir.is_empty() || (a.load.is_none() && a.qps == 0) || a.threads == 0 || a.batch == 0 || a.series == 0 {
        return Err(USAGE.into());
    }
    Ok(a)
}

/// The name of the file's series `i`: its sensor kind, then its index.
fn series_name(i: usize) -> String {
    format!("{}.{i}", NAMES[i % 7])
}

/// Row i of a RowBinary file of 16-byte rows (u32 series, i64 ts, f32 value, little endian).
fn row(data: &[u8], i: usize) -> (u32, i64, f32) {
    let r = &data[i * 16..i * 16 + 16];
    (
        u32::from_le_bytes(r[0..4].try_into().unwrap()),
        i64::from_le_bytes(r[4..12].try_into().unwrap()),
        f32::from_le_bytes(r[12..16].try_into().unwrap()),
    )
}

/// tools/cbench.h cb_row_hash: summed over the rows of a series, independent of their order.
fn row_hash(t: i64, v: f32) -> u64 {
    let bits = u64::from(v.to_bits());
    common::mix((t as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ (bits << 17 | bits))
}

/// tools/cbench.h cb_expect: what each series of the file holds.
struct Expect {
    count: Vec<u64>,
    sum: Vec<f64>,
    abs: Vec<f64>,
    hash: Vec<u64>,
}

impl Expect {
    fn of(data: &[u8]) -> Expect {
        let n = data.len() / 16;
        let ns = (0..n).map(|i| row(data, i).0).max().map_or(0, |m| m as usize + 1);
        let mut e = Expect { count: vec![0; ns], sum: vec![0.0; ns], abs: vec![0.0; ns], hash: vec![0; ns] };
        for i in 0..n {
            let (s, t, v) = row(data, i);
            let s = s as usize;
            e.count[s] += 1;
            e.sum[s] += f64::from(v);
            e.abs[s] += f64::from(v).abs();
            e.hash[s] = e.hash[s].wrapping_add(row_hash(t, v));
        }
        e
    }

    /// cb_expect_verify_all: prints the reasons (up to 5) and the verdict line; true if the database holds
    /// exactly the file's rows.
    fn verify(&self, db: &Db, ids: &[u64], file_rows: usize) -> bool {
        let (mut total, mut bad) = (0u64, 0u32);
        for (s, &id) in ids.iter().enumerate().take(self.count.len()) {
            let b = db.query_agg(id, .., 0).expect("verify: aggregate");
            let (c, sum) = b.first().map_or((0, 0.0), |b| (b.count, b.sum));
            total += c;
            let why = if c != self.count[s] || (sum - self.sum[s]).abs() > 1e-9 * self.abs[s] + 1e-9 {
                Some(format!("holds {c} rows, sum {sum:.17}; the file has {}, sum {:.17}", self.count[s], self.sum[s]))
            } else {
                let (mut n, mut h) = (0u64, 0u64);
                db.query_raw(id, .., QueryOpts::default(), |ts, v| {
                    for (&t, &v) in ts.iter().zip(v) {
                        h = h.wrapping_add(row_hash(t, v));
                    }
                    n += ts.len() as u64;
                    ControlFlow::Continue(())
                })
                .expect("verify: raw");
                (n != self.count[s] || h != self.hash[s]).then(|| {
                    format!(
                        "reads back {n} rows with timestamps or values other than the file's ({} rows)",
                        self.count[s]
                    )
                })
            };
            if let Some(why) = why {
                if bad < 5 {
                    eprintln!("verify: series {s} {why}");
                }
                bad += 1;
            }
        }
        let ok = bad == 0 && total == file_rows as u64;
        println!(
            "verify: {} series, {total} rows in the database, {file_rows} in the file: {}",
            self.count.len(),
            if ok { "ok" } else { "MISMATCH" }
        );
        ok
    }
}

fn load(a: &Args, path: &str) -> bool {
    let data = std::fs::read(path).unwrap_or_else(|e| panic!("{path}: {e}"));
    assert!(!data.is_empty() && data.len().is_multiple_of(16), "{path}: not a RowBinary file of 16-byte rows");
    let n = data.len() / 16;
    match std::fs::remove_dir_all(&a.dir) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => panic!("{}: {e}", a.dir),
        _ => {}
    }
    let db = Db::open(&a.dir, &Options { zstd_level: a.zstd, fsync: a.fsync, ..Options::default() }).expect("open");
    let e = Expect::of(&data);
    // the file's series are indexes; the database hands out its own ids (not dense in general)
    let ids: Vec<u64> = (0..e.count.len()).map(|i| db.series(&series_name(i)).expect("series")).collect();
    let nt = a.threads;
    let (t0, cpu0) = (Instant::now(), common::cpu_s());
    std::thread::scope(|sc| {
        for i in 0..nt {
            let (db, data, ids) = (&db, &data, &ids);
            let (lo, hi) = (n * i / nt, n * (i + 1) / nt);
            sc.spawn(move || {
                let mut s = Vec::with_capacity(a.batch);
                let mut t = Vec::with_capacity(a.batch);
                let mut v = Vec::with_capacity(a.batch);
                for off in (lo..hi).step_by(a.batch) {
                    s.clear();
                    t.clear();
                    v.clear();
                    for k in off..hi.min(off + a.batch) {
                        let r = row(data, k);
                        s.push(ids[r.0 as usize]);
                        t.push(r.1);
                        v.push(r.2);
                    }
                    db.insert(&s, &t, &v).expect("insert");
                }
            });
        }
    });
    let (dt, cpu) = (t0.elapsed().as_secs_f64(), common::cpu_s() - cpu0);
    let client = if a.fsync { "embedded-rust-fsync" } else { "embedded-rust-nofsync" };
    let extra = format!(", batches of {}, fsync {}, zstd {}", a.batch, if a.fsync { "on" } else { "off" }, a.zstd);
    common::report_load(client, nt, 0, n as u64, dt, 0, cpu, &extra);
    if a.optimize {
        let t0 = Instant::now();
        db.optimize().expect("optimize");
        println!("optimize {:.2} s", t0.elapsed().as_secs_f64());
    }
    let ok = e.verify(&db, &ids, n);
    let st = db.stats().expect("stats");
    println!(
        "storage {:.2} MiB, {:.3} bytes/row",
        st.bytes as f64 / 1048576.0,
        st.bytes as f64 / st.rows.max(1) as f64
    ); // rows: in parts
    db.close().expect("close");
    ok
}

/// One thread's queries of class `k` for `seconds`: (queries, rows, seconds). tools/tsdb-bench.c qw_loop.
fn querier(db: &Db, ids: &[u64], a: &Args, k: &Class, thread: usize) -> (u64, u64, f64) {
    let mut r = common::seed(thread);
    let (mut queries, mut rows) = (0u64, 0u64);
    let mut touch = 0.0f64;
    let t0 = Instant::now();
    while t0.elapsed().as_secs_f64() < a.seconds {
        for _ in 0..16 {
            let (s, start, end) = common::window(&mut r, a.series, a.points, k.window);
            if !k.agg {
                db.query_raw(ids[s as usize], start..end, QueryOpts::default(), |ts, v| {
                    rows += ts.len() as u64;
                    touch += v.last().map_or(0.0, |&x| f64::from(x)); // touch the data, as tsdb-bench does
                    ControlFlow::Continue(())
                })
                .expect("query");
            } else {
                let b = db.query_agg(ids[s as usize], start..end, k.bucket as i64 * common::step_ms(s)).expect("query");
                rows += b.iter().map(|b| b.count).sum::<u64>();
            }
            queries += 1;
        }
    }
    std::hint::black_box(touch);
    (queries, rows, t0.elapsed().as_secs_f64())
}

fn qps(a: &Args) -> bool {
    if !std::path::Path::new(&a.dir).is_dir() {
        eprintln!("qps: {} is not a database directory", a.dir); // opening would create an empty one
        return false;
    }
    let mut o = Options::default();
    if a.query_threads > 0 {
        o.query_threads = a.query_threads;
    }
    let db = Db::open(&a.dir, &o).expect("open");
    let ids: Vec<u64> = match (0..a.series as usize).map(|i| db.series_lookup(&series_name(i))).collect() {
        Ok(ids) => ids,
        Err(e) => {
            eprintln!("qps: {} does not hold the data set's {} series: {e}", a.dir, a.series);
            return false;
        }
    };
    for k in &CLASSES {
        let cpu0 = common::cpu_s();
        let res: Vec<(u64, u64, f64)> = std::thread::scope(|sc| {
            let hs: Vec<_> = (0..a.qps)
                .map(|i| {
                    sc.spawn({
                        let (db, ids) = (&db, &ids);
                        move || querier(db, ids, a, k, i)
                    })
                })
                .collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let queries = res.iter().map(|r| r.0).sum();
        let rows = res.iter().map(|r| r.1).sum();
        let secs = res.iter().map(|r| r.2).fold(0.0, f64::max);
        common::report_query("embedded-rust", k, "thr", a.qps, queries, rows, secs, common::cpu_s() - cpu0);
        if rows == 0 {
            eprintln!("qps: class {} returned no rows: the database is empty or does not hold the data set", k.id);
            return false;
        }
    }
    db.close().expect("close");
    true
}

fn main() {
    let a = parse().unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(2);
    });
    let ok = match &a.load {
        Some(path) => load(&a, path),
        None => qps(&a),
    };
    std::process::exit(if ok { 0 } else { 1 });
}
