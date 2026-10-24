use tsdb_client::{Db, Group, Options, QueryOpts, Unit};

fn main() -> tsdb_client::Result<()> {
    let dir = std::env::args().nth(1).unwrap_or_else(|| "/tmp/tsdb-embedded".into());
    let db = Db::open(&dir, &Options::default())?; // the engine runs in this process

    let s = db.series("pump7.flow")?;
    let t0 = 1_700_000_000_000; // one row a minute for a day
    let ts: Vec<i64> = (0..1440).map(|i| t0 + i * 60_000).collect();
    let values: Vec<f32> = (0..1440).map(|i| (i % 60) as f32).collect();
    db.insert_series(s, &ts, &values)?;

    let p = db.query_points(s, t0..t0 + 180_000, QueryOpts::asc())?;
    for (t, v) in p.ts.iter().zip(&p.values) {
        println!("raw  {t} {v}");
    }
    let days = Group::new(Unit::Day).timezone("Europe/Oslo");
    for b in db.query_group(s, .., Some(&days), QueryOpts::asc())? {
        println!("day  {} {} {}", b.start, b.count, b.sum);
    }
    db.close()
}
