// SPDX-License-Identifier: Apache-2.0
//! What the benchmarks of this crate share with tools/cbench.h (the C benchmarks): the random query windows,
//! the three window classes, process CPU time and the result lines that bench/clients/run.sh reads. The
//! constants and the generator are copied from the C header; change them together.

#![allow(dead_code)]

pub const T0: i64 = 1_704_067_200_000; // 2024-01-01
/// Step in ms of series s: tools/datasets.h ds_info[s % 7].step_ms.
const STEPS: [i64; 7] = [60_000, 1_000, 10_000, 10_000, 1, 1_000, 1_000];
/// tools/datasets.h ds_info[s % 7].name: series number `s` is named `<kind>.<s>`. The number (a window's series, the
/// u32 of a row file) is an index into the bench's table of ids; the ids themselves are 64-bit and carry the volume.
pub const NAMES: [&str; 7] =
    ["hdd_temp", "cpu_temp", "tank_bar", "tank_psi", "vibration", "flow_rate", "flow_total"];

pub struct Class {
    pub id: &'static str,
    pub label: &'static str,
    pub agg: bool,
    /// Samples in the window.
    pub window: u64,
    /// Samples per bucket (agg).
    pub bucket: u64,
}

pub const CLASSES: [Class; 3] = [
    Class { id: "raw", label: "raw, 3600 samples", agg: false, window: 3600, bucket: 0 },
    Class { id: "agg300", label: "agg, 86400 samples in buckets of 300", agg: true, window: 86_400, bucket: 300 },
    Class { id: "agg3600", label: "agg, 604800 samples in buckets of 3600", agg: true, window: 604_800, bucket: 3600 },
];

pub fn mix(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^ (x >> 33)
}

/// The generator state of query thread `thread`.
pub fn seed(thread: usize) -> u64 {
    mix(thread as u64 + 991)
}

pub fn step_ms(series: u32) -> i64 {
    STEPS[series as usize % 7]
}

/// The next random window: (series, start, end).
pub fn window(r: &mut u64, nseries: u32, points: u64, window: u64) -> (u32, i64, i64) {
    *r = mix(*r);
    let s = (*r % u64::from(nseries)) as u32;
    let span = if points > window { points - window } else { 1 };
    let start = T0 + ((*r >> 20) % span) as i64 * step_ms(s);
    (s, start, start + window as i64 * step_ms(s))
}

/// CPU seconds (user + system, all threads) this process has used, from /proc/self/stat.
pub fn cpu_s() -> f64 {
    let Ok(stat) = std::fs::read_to_string("/proc/self/stat") else { return 0.0 };
    // The fields after the command name (which may contain spaces) start with the state; utime and stime are the
    // 12th and 13th of them.
    let rest = stat.rsplit_once(')').map_or("", |(_, r)| r);
    let f: Vec<f64> = rest.split_whitespace().skip(11).take(2).filter_map(|x| x.parse().ok()).collect();
    if f.len() == 2 { (f[0] + f[1]) / 100.0 } else { 0.0 } // USER_HZ is 100 on Linux
}

#[allow(clippy::too_many_arguments)]
/// `unit` names what runs the queries in the human line: "conn" (connections) or "thr" (threads of an embedded Db).
pub fn report_query(client: &str, k: &Class, unit: &str, threads: usize, queries: u64, rows: u64, secs: f64, cpu: f64) {
    println!(
        "  {:<44} {:>3} {unit} {:>10.0} q/s  {:>8.1} us/q  {:>7.1} Mrows/s",
        k.label,
        threads,
        queries as f64 / secs,
        secs * threads as f64 / queries as f64 * 1e6,
        rows as f64 / secs / 1e6
    );
    println!(
        "RESULT kind=query client={client} class={} threads={threads} queries={queries} rows={rows} secs={secs:.4} \
         cpu={cpu:.4}",
        k.id
    );
}

#[allow(clippy::too_many_arguments)]
pub fn report_load(
    client: &str,
    conns: usize,
    compression: i32,
    rows: u64,
    secs: f64,
    bytes: u64,
    cpu: f64,
    extra: &str,
) {
    println!(
        "load ({client}): {rows} rows in {secs:.2} s = {:.2} M rows/s, {:.2} bytes/row, client CPU {cpu:.2} s \
         ({:.2} s per M rows), {conns} connections/threads{extra}",
        rows as f64 / secs / 1e6,
        bytes as f64 / rows as f64,
        cpu / (rows as f64 / 1e6)
    );
    println!(
        "RESULT kind=load client={client} conns={conns} compression={compression} rows={rows} secs={secs:.4} \
         bytes={bytes} cpu={cpu:.4}"
    );
}
