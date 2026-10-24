use tsdb_client::{Client, Group, QueryOpts, Unit};

fn main() -> Result<(), tsdb_client::Error> {
    // Connect to tsdbd. The address is the first argument, if there is one.
    let addr = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:7070".to_string());
    let mut client = Client::connect(addr.as_str())?;

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
