//! Insert benchmark: loads a RowBinary file into tsdbd, as `tsdbc-bench load`.
//!
//! ```text
//! cargo run --release --example insert_bench -- FILE [--addr 127.0.0.1:7070] [--conns 4]
//!     [--batch 1000000] [--compression 1] [--inflight 4]
//! ```
//!
//! FILE holds rows of u32 series, i64 ts (ms), f32 value (16 bytes each), as
//! `build/tsdb-bench --export FILE` writes them. It is split into contiguous
//! slices, one per connection; each connection sends batches with the
//! many-series layout, keeping up to `--inflight` of them unacknowledged.
//! `--compression 0` sends plain arrays; any other zstd level packs them
//! (protocol version 3). Series `bench.<n>` are created first, outside the
//! timing. Reports rows/s, the bytes written to the sockets per row and the CPU time of the process, in the
//! format of tsdbc-bench (see examples/common/mod.rs).

mod common;

use std::time::Instant;

use tsdb_client::Client;

struct Args {
    file: String,
    addr: String,
    conns: usize,
    batch: usize,
    compression: i32,
    inflight: usize,
}

fn parse() -> Result<Args, String> {
    let mut a = Args {
        file: String::new(),
        addr: "127.0.0.1:7070".into(),
        conns: 4,
        batch: 1_000_000,
        compression: tsdb_client::DEFAULT_COMPRESSION,
        inflight: 4,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut val = |name: &str| it.next().ok_or(format!("{name} needs a value"));
        let num = |s: String| s.parse::<i64>().map_err(|e| format!("{s}: {e}"));
        match arg.as_str() {
            "--addr" => a.addr = val("--addr")?,
            "--conns" => a.conns = num(val("--conns")?)? as usize,
            "--batch" => a.batch = num(val("--batch")?)? as usize,
            "--compression" => a.compression = num(val("--compression")?)? as i32,
            "--inflight" => a.inflight = num(val("--inflight")?)? as usize,
            s if s.starts_with("--") => return Err(format!("unknown option {s}")),
            s => a.file = s.to_string(),
        }
    }
    if a.file.is_empty() || a.conns == 0 || a.batch == 0 || a.inflight == 0 {
        return Err("usage: insert_bench FILE [--addr HOST:PORT] [--conns N] [--batch ROWS] \
                    [--compression LEVEL] [--inflight N]"
            .into());
    }
    Ok(a)
}

fn row(rows: &[u8], i: usize) -> (u32, i64, f32) {
    let r = &rows[i * 16..i * 16 + 16];
    (
        u32::from_le_bytes(r[0..4].try_into().unwrap()),
        i64::from_le_bytes(r[4..12].try_into().unwrap()),
        f32::from_le_bytes(r[12..16].try_into().unwrap()),
    )
}

/// Sends rows `[a, b)` over one connection; returns the bytes written.
fn load(args: &Args, rows: &[u8], ids: &[u32], a: usize, b: usize) -> Result<u64, tsdb_client::Error> {
    let mut c = Client::connect(args.addr.as_str())?;
    c.set_compression(args.compression)?;
    let (mut s, mut t, mut v) = (Vec::new(), Vec::new(), Vec::new());
    for off in (a..b).step_by(args.batch) {
        if c.pending() >= args.inflight {
            c.wait()?;
        }
        s.clear();
        t.clear();
        v.clear();
        for i in off..(off + args.batch).min(b) {
            let (si, ti, vi) = row(rows, i);
            s.push(ids[si as usize]);
            t.push(ti);
            v.push(vi);
        }
        c.insert_async(&s, &t, &v)?;
    }
    c.wait()?;
    let sent = c.bytes_sent();
    c.close()?;
    Ok(sent)
}

fn main() {
    let args = parse().unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(2);
    });
    let rows = std::fs::read(&args.file).unwrap_or_else(|e| {
        eprintln!("{}: {e}", args.file);
        std::process::exit(1);
    });
    let n = rows.len() / 16;
    let max = (0..n).map(|i| row(&rows, i).0).max().unwrap_or(0);
    let mut c = Client::connect(args.addr.as_str()).expect("connect");
    let ids: Vec<u32> = (0..=max).map(|i| c.series(&format!("bench.{i}")).expect("series")).collect();

    let (t0, cpu0) = (Instant::now(), common::cpu_s());
    let sent: u64 = std::thread::scope(|sc| {
        let jobs: Vec<_> = (0..args.conns)
            .map(|k| {
                let (a, b) = (n * k / args.conns, n * (k + 1) / args.conns);
                let (args, rows, ids) = (&args, &rows, &ids);
                sc.spawn(move || load(args, rows, ids, a, b).expect("insert"))
            })
            .collect();
        jobs.into_iter().map(|j| j.join().unwrap()).sum()
    });
    let (dt, cpu) = (t0.elapsed().as_secs_f64(), common::cpu_s() - cpu0);
    common::report_load(
        "rust",
        args.conns,
        args.compression,
        n as u64,
        dt,
        sent,
        cpu,
        &format!(", batches of {}, {} in flight", args.batch, args.inflight),
    );
}
