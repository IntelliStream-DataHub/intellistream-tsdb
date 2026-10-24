use tsdb_client::{Client, Column, Group, QueryOpts, Unit, ValueType};

fn main() -> Result<(), tsdb_client::Error> {
    // Connect to tsdbd (the address is the first argument, if any).
    let addr = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:7070".to_string());
    let mut client = Client::connect(addr.as_str())?;

    // Get or create a series, then insert one point a minute for a day.
    let id = client.series("pump7.flow")?;
    let t0: i64 = 1_699_999_200_000; // 2023-11-14 22:00 UTC, in ms since the epoch
    let ts: Vec<i64> = (0..1440).map(|i| t0 + i * 60_000).collect();
    let values: Vec<f32> = (0..1440).map(|i| (i % 60) as f32).collect();
    client.insert_series(id, &ts, &values)?;

    // Raw query: the newest three points.
    let newest = client.query_points(id, .., QueryOpts::desc().limit(3))?;
    for (t, v) in newest.iter() {
        println!("raw  {t} {v}");
    }

    // Aggregate: hourly buckets over the first six hours.
    for b in client.query_agg(id, t0..t0 + 6 * 3_600_000, 3_600_000)? {
        println!("hour {} count {} avg {:.2} min {} max {}", b.start, b.count, b.avg(), b.min, b.max);
    }

    // Group by calendar day in a time zone.
    let days = Group::new(Unit::Day).timezone("Europe/Oslo");
    for b in client.query_group(id, .., Some(&days), QueryOpts::asc())? {
        println!("day  {} count {} sum {}", b.start, b.count, b.sum);
    }

    // Typed series: int64 with a null, aggregated exactly.
    let starts = client.series_create("pump7.starts", ValueType::Int64, None)?;
    client.insert_series_values(starts, &ts[..3], &Column::from_options([Some(3i64), None, Some(5)]))?;
    let rows = client.query_rows(starts, .., QueryOpts::asc())?;
    println!("rows {:?}", rows.values.to_options::<i64>().unwrap());
    let total = client.query_agg(starts, .., 0)?[0];
    println!("int  count {} isum {}", total.count, total.isum);

    // A state series stores codes; the server keeps their names.
    let mode = client.series_create("pump7.mode", ValueType::State, None)?;
    let (on, off) = (client.state(mode, "on")?, client.state(mode, "off")?);
    client.insert_series_values(mode, &ts[..2], &[on, off])?;
    let latest = client.query_rows(mode, .., QueryOpts::desc().limit(1))?;
    let code = latest.values.as_slice::<u32>().unwrap()[0];
    println!("mode {}", client.state_name(mode, code)?);

    client.close()
}
