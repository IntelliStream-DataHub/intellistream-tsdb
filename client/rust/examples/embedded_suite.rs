//! Embedded benchmark, full suite: the load and the query classes of `build/tsdb-bench`'s default mode, through
//! [`tsdb_client::Db`] (the engine in this process, no tsdbd).
//!
//! ```text
//! build/tsdb-bench --export /data/bench.rowbinary [--series 100] [--points 1000000] [--dataset mixed]
//! cargo run --release --features embedded --example embedded_suite -- --dir /data/emb \
//!     --load /data/bench.rowbinary [--dataset mixed] [--batch 1000000] [--threads 4] [--zstd 1] [--no-fsync]
//! cargo run --release --features embedded --example embedded_suite -- --dir /data/emb   # queries only
//! ```
//!
//! `--load FILE` reads rows of u32 series, i64 ts (ms), f32 value (16 bytes each, as
//! `tsdb-bench --export` writes them) into memory first, so the timing covers the inserts but
//! not the data generation that tsdb-bench's own "insert+generate" line includes. Like
//! tsdb-bench, each of `--threads` writers owns a range of series and sends them time-major in
//! batches of `--batch` rows; then the database is optimized. `--dir` must not hold a database
//! when loading. Without `--load`, the queries run on an existing directory, for example one
//! that `tsdb-bench --keep` left behind.
//!
//! The queries are tsdb-bench's: a raw scan of every series, aggregates over the whole range,
//! calendar groups (best of 3), then random windows for `--seconds` (default 2) on 1 thread
//! and on every logical CPU, drawn with the same seeds, so the numbers line up row by row with
//! tsdb-bench's. The sample step of each series comes from its name (`<kind>.<n>`).

use std::ops::ControlFlow;
use std::time::Instant;

use tsdb_client::{Db, Group, Options, QueryOpts, Unit};

const T0: i64 = 1_704_067_200_000; // 2024-01-01, as tools/tsdb-bench.c
const KINDS: [(&str, i64); 8] = [
    ("hdd_temp", 60_000),
    ("cpu_temp", 1000),
    ("tank_bar", 10_000),
    ("tank_psi", 10_000),
    ("vibration", 1),
    ("flow_rate", 1000),
    ("flow_total", 1000),
    ("random", 1000),
];

struct Args {
    dir: String,
    load: Option<String>,
    dataset: Option<usize>, // None: mixed
    batch: usize,
    threads: usize,
    zstd: i32,
    fsync: bool,
    seconds: f64,
    query_threads: i32,
}

fn usage() -> ! {
    eprintln!(
        "usage: embedded_suite --dir DIR [--load FILE] [--dataset mixed|KIND] [--batch ROWS] [--threads N]\n\
         \x20      [--zstd LEVEL] [--no-fsync] [--seconds S] [--query-threads N]"
    );
    std::process::exit(2)
}

fn parse() -> Args {
    let mut a = Args {
        dir: String::new(),
        load: None,
        dataset: None,
        batch: 1_000_000,
        threads: 4,
        zstd: 1,
        fsync: true,
        seconds: 2.0,
        query_threads: 0,
    };
    let mut it = std::env::args().skip(1);
    while let Some(k) = it.next() {
        if k == "--no-fsync" {
            a.fsync = false;
            continue;
        }
        let v = it.next().unwrap_or_else(|| usage());
        match k.as_str() {
            "--dir" => a.dir = v,
            "--load" => a.load = Some(v),
            "--dataset" if v == "mixed" => a.dataset = None,
            "--dataset" => a.dataset = Some(KINDS.iter().position(|k| k.0 == v).unwrap_or_else(|| usage())),
            "--batch" => a.batch = v.parse().unwrap_or_else(|_| usage()),
            "--threads" => a.threads = v.parse().unwrap_or_else(|_| usage()),
            "--zstd" => a.zstd = v.parse().unwrap_or_else(|_| usage()),
            "--seconds" => a.seconds = v.parse().unwrap_or_else(|_| usage()),
            "--query-threads" => a.query_threads = v.parse().unwrap_or_else(|_| usage()),
            _ => usage(),
        }
    }
    if a.dir.is_empty() || a.batch == 0 || a.threads == 0 {
        usage()
    }
    a
}

fn ds_mix(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
    x ^ (x >> 33)
}

fn kind_of(dataset: Option<usize>, s: u32) -> usize {
    dataset.unwrap_or(s as usize % (KINDS.len() - 1)) // mixed: no random
}

/// One writer's batches: its series' rows in file order (time-major), cut like tsdb-bench's.
struct Batches {
    series: Vec<u32>,
    ts: Vec<i64>,
    values: Vec<f32>,
    rows_per_batch: usize,
}

fn read_rows(path: &str, threads: usize, batch: usize) -> (u32, u64, Vec<Batches>) {
    let data = std::fs::read(path).unwrap_or_else(|e| panic!("{path}: {e}"));
    assert!(data.len().is_multiple_of(16), "{path}: not rows of 16 bytes");
    let rec = |r: &[u8]| {
        (
            u32::from_le_bytes(r[0..4].try_into().unwrap()),
            i64::from_le_bytes(r[4..12].try_into().unwrap()),
            f32::from_le_bytes(r[12..16].try_into().unwrap()),
        )
    };
    let nseries = data.chunks_exact(16).map(|r| rec(r).0).max().map_or(0, |m| m + 1);
    let threads = threads.min(nseries as usize).max(1);
    let lo = |i: usize| (nseries as u64 * i as u64 / threads as u64) as u32;
    let mut out: Vec<Batches> = (0..threads)
        .map(|i| {
            let ns = (lo(i + 1) - lo(i)) as usize;
            Batches { series: Vec::new(), ts: Vec::new(), values: Vec::new(), rows_per_batch: (batch / ns).max(1) * ns }
        })
        .collect();
    for r in data.chunks_exact(16) {
        let (s, t, v) = rec(r);
        let w = (0..threads).find(|&i| s < lo(i + 1)).unwrap();
        out[w].series.push(s);
        out[w].ts.push(t);
        out[w].values.push(v);
    }
    (nseries, (data.len() / 16) as u64, out)
}

fn load(db: &Db, a: &Args, path: &str) {
    let (nseries, total, writers) = read_rows(path, a.threads, a.batch);
    for s in 0..nseries {
        db.series(&format!("{}.{s}", KINDS[kind_of(a.dataset, s)].0)).expect("series");
    }
    println!(
        "embedded_suite: {nseries} series, {:.1} M rows from {path}, batch {} rows, {} writer threads, zstd {}, \
         fsync {}, simd {}\n",
        total as f64 / 1e6,
        a.batch,
        writers.len(),
        a.zstd,
        a.fsync as i32,
        Db::simd_level()
    );
    let t0 = Instant::now();
    std::thread::scope(|sc| {
        for w in &writers {
            sc.spawn(move || {
                let n = w.rows_per_batch;
                for ((s, t), v) in w.series.chunks(n).zip(w.ts.chunks(n)).zip(w.values.chunks(n)) {
                    db.insert(s, t, v).expect("insert");
                }
            });
        }
    });
    let ti = t0.elapsed().as_secs_f64();
    let st = db.stats().expect("stats");
    println!(
        "insert          {ti:6.2} s  {:7.2} M rows/s   ({} parts, {} merges so far)",
        total as f64 / ti / 1e6,
        st.parts,
        st.merges
    );
    let t0 = Instant::now();
    db.optimize().expect("optimize");
    let to = t0.elapsed().as_secs_f64();
    let st = db.stats().expect("stats");
    println!(
        "optimize        {to:6.2} s  -> {} parts in {} partitions, {} granules copied as-is",
        st.parts, st.partitions, st.passthrough_granules
    );
}

/// What the queries need to know about the data: series count, points per series, steps.
struct Shape {
    series: u32,
    points: u64,
    step: Vec<i64>,
}

fn raw_rows(db: &Db, s: u32, range: std::ops::Range<i64>) -> (u64, f64) {
    let (mut n, mut last) = (0u64, 0f64);
    db.query_raw(s, range, QueryOpts::asc(), |_ts, v| {
        n += v.len() as u64;
        last += v.last().copied().unwrap_or(0.0) as f64; // touch the data
        ControlFlow::Continue(())
    })
    .expect("raw");
    (n, last)
}

/// A class of random-window queries: raw rows, or aggregates in buckets; sizes in samples.
struct Class {
    label: &'static str,
    agg: bool,
    window: u64,
    bucket: u64,
}

fn qps(db: &Db, sh: &Shape, seconds: f64, c: &Class, threads: usize) {
    let Class { label, agg, window, bucket } = *c;
    let res: Vec<(u64, u64, f64)> = std::thread::scope(|sc| {
        let hs: Vec<_> = (0..threads)
            .map(|i| {
                sc.spawn(move || {
                    let mut r = ds_mix(i as u64 + 77);
                    let (mut queries, mut rows) = (0u64, 0u64);
                    let t0 = Instant::now();
                    while t0.elapsed().as_secs_f64() < seconds {
                        for _ in 0..64 {
                            r = ds_mix(r);
                            let s = (r % sh.series as u64) as u32;
                            let step = sh.step[s as usize];
                            let span = if sh.points > window { sh.points - window } else { 1 };
                            let start = T0 + ((r >> 20) % span) as i64 * step;
                            let end = start + window as i64 * step;
                            if agg {
                                let b = db.query_agg(s, start..end, bucket as i64 * step).expect("agg");
                                rows += b.iter().map(|b| b.count).sum::<u64>();
                            } else {
                                rows += raw_rows(db, s, start..end).0;
                            }
                            queries += 1;
                        }
                    }
                    (queries, rows, t0.elapsed().as_secs_f64())
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let nq: u64 = res.iter().map(|r| r.0).sum();
    let rows: u64 = res.iter().map(|r| r.1).sum();
    let secs = res.iter().map(|r| r.2).fold(0.0, f64::max);
    println!(
        "  {label:<46} {threads:3} thr {:10.0} q/s  {:8.1} us/q  {:7.1} Mrows/s",
        nq as f64 / secs,
        secs * threads as f64 / nq as f64 * 1e6,
        rows as f64 / secs / 1e6
    );
}

fn queries(db: &Db, sh: &Shape, seconds: f64) {
    let st = db.stats().expect("stats");
    println!(
        "storage    {:8.2} MiB  {:.3} bytes/row  ratio {:.1}x vs 12 bytes/row (DateTime64(3)+Float32)\n",
        st.bytes as f64 / 1048576.0,
        st.bytes as f64 / st.rows as f64,
        st.rows as f64 * 12.0 / st.bytes as f64
    );
    let total = sh.series as u64 * sh.points;

    let t0 = Instant::now();
    let mut rows = 0u64;
    for s in 0..sh.series {
        rows += raw_rows(db, s, i64::MIN..i64::MAX).0;
    }
    let tr = t0.elapsed().as_secs_f64();
    println!(
        "raw scan, all series, 1 thread            {tr:8.3} s  {:7.1} M rows/s  ({rows} rows)",
        rows as f64 / tr / 1e6
    );

    let aggs: [(&str, i64); 5] = [
        ("agg whole range, 1 bucket per series", 0),
        ("agg buckets of 86400 samples", 86400),
        ("agg buckets of 3600 samples", 3600),
        ("agg buckets of 60 samples (decodes all)", 60),
        ("agg buckets of 10 samples (decodes all)", 10),
    ];
    for (label, bucket) in aggs {
        let t0 = Instant::now();
        let (mut rows, mut buckets) = (0u64, 0usize);
        for s in 0..sh.series {
            let b = db.query_agg(s, i64::MIN..i64::MAX, bucket * sh.step[s as usize]).expect("agg");
            rows += b.iter().map(|b| b.count).sum::<u64>();
            buckets += b.len();
        }
        let ta = t0.elapsed().as_secs_f64();
        println!(
            "{label:<42} {ta:8.3} s  {:7.1} M rows/s  {:6.2} ms/series  ({buckets} buckets)",
            rows as f64 / ta / 1e6,
            ta / sh.series as f64 * 1e3
        );
        if rows != total {
            println!("  !! row count mismatch: {rows} != {total}");
        }
    }

    let groups = [
        ("group by 15 minutes (UTC)", Group::new(Unit::Minute).count(15)),
        ("group by hour (UTC)", Group::new(Unit::Hour)),
        ("group by day (Europe/Oslo)", Group::new(Unit::Day).timezone("Europe/Oslo")),
        ("group by week (Europe/Oslo)", Group::new(Unit::Week).timezone("Europe/Oslo")),
        ("group by month (Europe/Oslo)", Group::new(Unit::Month).timezone("Europe/Oslo")),
        ("group by year (Europe/Oslo)", Group::new(Unit::Year).timezone("Europe/Oslo")),
    ];
    for (label, g) in &groups {
        let (mut best, mut rows, mut buckets) = (f64::MAX, 0u64, 0usize);
        for _ in 0..3 {
            let t0 = Instant::now();
            (rows, buckets) = (0, 0);
            for s in 0..sh.series {
                let b = db.query_group(s, i64::MIN..i64::MAX, Some(g), QueryOpts::asc()).expect("group");
                rows += b.iter().map(|b| b.count).sum::<u64>();
                buckets += b.len();
            }
            best = best.min(t0.elapsed().as_secs_f64());
        }
        println!("{label:<42} {best:8.3} s  {:7.1} M rows/s  ({buckets} buckets)", rows as f64 / best / 1e6);
    }

    println!("\nrandom windows ({seconds} s each):");
    let n = std::thread::available_parallelism().map_or(1, |n| n.get());
    for c in [
        Class { label: "raw, 3600 samples", agg: false, window: 3600, bucket: 0 },
        Class { label: "agg, 86400 samples in buckets of 300", agg: true, window: 86400, bucket: 300 },
        Class { label: "agg, 604800 samples in buckets of 3600", agg: true, window: 604800, bucket: 3600 },
    ] {
        if c.window == 604800 && sh.points <= 7 * 86400 {
            continue;
        }
        qps(db, sh, seconds, &c, 1);
        qps(db, sh, seconds, &c, n);
    }
}

fn main() {
    let a = parse();
    let mut o = Options { zstd_level: a.zstd, fsync: a.fsync, ..Options::default() };
    if a.query_threads > 0 {
        o.query_threads = a.query_threads;
    }
    if a.load.is_some() && std::fs::read_dir(&a.dir).is_ok_and(|mut d| d.next().is_some()) {
        eprintln!("{}: not empty; --load writes a new database", a.dir);
        std::process::exit(1);
    }
    let db = Db::open(&a.dir, &o).unwrap_or_else(|e| panic!("open {}: {e}", a.dir));
    if let Some(path) = &a.load {
        load(&db, &a, path);
    }
    let series = db.series_count();
    let step: Vec<i64> = (0..series)
        .map(|s| {
            let name = db.series_name(s).expect("series name");
            let kind = name.split('.').next().unwrap_or("");
            KINDS.iter().find(|k| k.0 == kind).map_or_else(|| panic!("series {name}: not tsdb-bench's"), |k| k.1)
        })
        .collect();
    let rows = db.stats().expect("stats").rows;
    let sh = Shape { series, points: if series > 0 { rows / series as u64 } else { 0 }, step };
    queries(&db, &sh, a.seconds);
    db.close().expect("close");
}
