// SPDX-License-Identifier: Apache-2.0
use tsdb_client::{Client, ConnectOptions, QueryOpts, TwaOpts, ValueType};

fn main() -> Result<(), tsdb_client::Error> {
    // As in the quick start: the address, the user and its password file.
    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:7070".to_string());
    let user = args.next().unwrap_or_else(|| "admin".to_string());
    let password_file = args.next().unwrap_or_else(|| "/tmp/tsdb-demo/password".to_string());
    let login = ConnectOptions::new().user(user).password_file(password_file);
    let mut client = Client::connect_opts(addr.as_str(), &login)?;
    let t0: i64 = 1_699_999_200_000; // 2023-11-14 22:00 UTC
    let hour = 3_600_000;

    // A tank level, sent only when it changes: 10 until 22:45, then 70, then 40 at midnight.
    let tank = client.series_create("tank3.level", ValueType::Float64, None)?;
    let at = [t0, t0 + 45 * 60_000, t0 + hour, t0 + 2 * hour];
    client.insert_series_values(tank, &at, &[10.0f64, 70.0, 70.0, 40.0])?;

    // The plain average counts each reading once; the time-weighted one counts how long each value held.
    let plain = client.query_agg(tank, t0..t0 + 2 * hour, hour)?;
    let twa = client.query_twa(tank, t0..t0 + 2 * hour, hour, TwaOpts::step())?;
    for (p, w) in plain.iter().zip(&twa) {
        println!("hour {} avg {:.1} twa {:.1}", p.start, p.avg(), w.twa);
    }

    // How far the readings spread: range and standard deviation of the whole series.
    let s = &client.query_spread(tank, .., 0, QueryOpts::asc())?[0];
    println!("spread range {} stddev {:.2}", s.range, s.stddev_samp);

    // 10,000 vibration readings cut to 100 points that keep the shape of the line, for a chart.
    let vib = client.series("pump7.vibration")?;
    let ts: Vec<i64> = (0..10_000).map(|i| t0 + i * 1000).collect();
    let values: Vec<f32> = (0..10_000).map(|i| (i as f32 / 300.0).sin()).collect();
    client.insert_series(vib, &ts, &values)?;
    let chart = client.query_lttb(vib, .., 100, QueryOpts::asc())?;
    println!("chart {} of {} points", chart.len(), ts.len());

    client.close()
}
