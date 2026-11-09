// SPDX-License-Identifier: Apache-2.0
use tsdb_client::{Client, ConnectOptions, Group, QueryOpts, Unit};

fn main() -> Result<(), tsdb_client::Error> {
    // Connect to tsdbd and log in. The arguments, all optional: the address, the user and its password file.
    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:7070".to_string());
    let user = args.next().unwrap_or_else(|| "admin".to_string());
    let password_file = args.next().unwrap_or_else(|| "/tmp/tsdb-demo/password".to_string());
    let login = ConnectOptions::new().user(user).password_file(password_file);
    let mut client = Client::connect_opts(addr.as_str(), &login)?;

    // Get the id of a series (it is created the first time).
    let id = client.series("pump7.flow")?;

    // Insert one value per minute for a day. Times are ms since 1970-01-01 UTC.
    let t0: i64 = 1_699_999_200_000; // 2023-11-14 22:00 UTC
    let ts: Vec<i64> = (0..1440).map(|i| t0 + i * 60_000).collect();
    let values: Vec<f32> = (0..1440).map(|i| (i % 60) as f32).collect();
    client.insert_series(id, &ts, &values)?;

    // Read the newest three values.
    for (t, v) in client.query_points(id, .., QueryOpts::desc().limit(3))?.iter() {
        println!("raw  {t} {v}");
    }

    // Count, average, min and max per hour, for the first six hours.
    for b in client.query_agg(id, t0..t0 + 6 * 3_600_000, 3_600_000)? {
        println!("hour {} count {} avg {:.2} min {} max {}", b.start, b.count, b.avg(), b.min, b.max);
    }

    // Sum per day, where days begin at midnight in Oslo.
    let days = Group::new(Unit::Day).timezone("Europe/Oslo");
    for b in client.query_group(id, .., Some(&days), QueryOpts::asc())? {
        println!("day  {} count {} sum {}", b.start, b.count, b.sum);
    }

    client.close()
}
