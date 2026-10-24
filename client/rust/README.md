# tsdb-client (Rust)

A Rust library for talking to `tsdbd`, the tsdb database server. You use it
to store numbers with a timestamp (a "time series") and to read them back,
raw or summed up per hour, day or month.

The client waits for each answer (it is blocking, not async). Its only
dependency is `zstd`, for compressing inserts.

## Quick start

1. Build and start a server. In the root of the tsdb repository:

   ```sh
   make
   ./build/tsdbd --dir /tmp/tsdb-demo
   ```

   It listens on port 7070. Leave it running and open a second terminal.

2. Run the example program in `examples/quickstart.rs`:

   ```sh
   cd client/rust
   cargo run --example quickstart
   ```

   To use another server, add its address: `cargo run --example quickstart -- 10.0.0.5:7070`.

This is the program. It stores one value per minute for a day, then reads
them back in three ways:

```rust
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
```

It prints:

```text
raw  1700085540000 59
raw  1700085480000 58
raw  1700085420000 57
hour 1699999200000 count 60 avg 29.50 min 0 max 59
...
day  1699916400000 count 60 sum 1770
day  1700002800000 count 1380 sum 40710
```

To use the library in your own project, add it to your `Cargo.toml`:

```toml
[dependencies]
tsdb-client = { path = "path/to/tsdb/client/rust" }
```

## Connect

`Client::connect("127.0.0.1:7070")` opens one connection. Methods take
`&mut self`, so give each thread its own client. `close()` waits for
unfinished inserts and closes the connection; dropping the client closes it too.

Every method returns a `Result`. If the server says no, you get
`Error::Server(status)`, for example `Status::NotFound`. After a network
error the client stops working: connect again.

`Client::connect` gives up on each address after 10 s (`CONNECT_TIMEOUT`).
By default a call waits as long as the server takes, so a server that hangs
(a stalled disk, a dead peer) hangs the caller. `Client::connect_with(addr,
Some(Duration::from_secs(30)))` bounds the connect and every later wait for
the server (the longest silence it puts up with, not a deadline for the whole
call), and `set_timeout` changes it on a live client. A call that times
out returns `Error::Io` of kind `TimedOut` and the client refuses further
calls (`is_broken()`), so close it and connect again. TCP keepalive is on:
after 30 s of silence the client probes every 10 s and drops the connection
after 3 unanswered probes, which `set_keepalive` changes or turns off.

## Create a series

A series has a name and a number (its id). You use the id in every other call.

```rust
let id = client.series("pump7.flow")?;           // creates it if it does not exist
let id = client.series_lookup("pump7.flow")?;    // only finds it
```

## Insert rows

A row is a timestamp and a value. Timestamps are milliseconds since
1970-01-01 UTC, as `i64`. You pass two slices of the same length:

```rust
client.insert_series(id, &[1_700_000_000_000, 1_700_000_060_000], &[4.2, 4.3])?;
```

If you write the same timestamp twice, the last value wins. The call returns
when the server has stored the rows.

For many small inserts, `insert_series_async` is much faster: it does not
wait for the answer. Call `wait()` at the end to get any error.

## Read raw rows

```rust
let all = client.query_points(id, .., QueryOpts::asc())?;
let newest = client.query_points(id, .., QueryOpts::desc().limit(10))?;
let some = client.query_points(id, t0..t1, QueryOpts::asc())?; // t0 <= ts < t1
for (ts, value) in newest.iter() {
    println!("{ts} {value}");
}
```

The range is a normal Rust range: `t0..t1` includes `t0` but not `t1`, and
`..` means all time.

## Aggregate

`query_agg` splits a range into buckets of a fixed length and returns, for
each bucket, how many values it has (`count`) and their `sum`, `min`, `max`
and `avg()`:

```rust
for b in client.query_agg(id, t0..t1, 3_600_000)? {   // one bucket per hour
    println!("{} {} {}", b.start, b.count, b.avg());
}
let total = client.query_agg(id, .., 0)?;             // 0: one bucket for everything
```

## Group by day in a time zone

A day in Oslo starts at 23:00 or 22:00 UTC, depending on daylight saving
time. `query_group` takes care of that:

```rust
let days = Group::new(Unit::Day).timezone("Europe/Oslo");
for b in client.query_group(id, .., Some(&days), QueryOpts::asc())? {
    println!("{} {}", b.start, b.sum);   // b.start is the UTC time the local day begins
}
```

Units are `Second`, `Minute`, `Hour`, `Day`, `Week`, `Month`, `Quarter`
and `Year`; `.count(15)` with `Unit::Minute` gives quarter hours. A time zone
is a name such as `"Europe/Oslo"` (read from `/usr/share/zoneinfo`), an
offset such as `"+02:00"`, or `"UTC"`.

The server only knows UTC. The client works out when each local day begins
and sends those times (`calendar_bounds` and `query_bounds` let you do the
two steps yourself). With a server older than protocol version 4, the client
sends the zone name instead.

## Value types and nulls

`series()` makes series of 32-bit floats. For other types, use
`series_create` with a `ValueType`: `Float32`, `Float64`, `Int64`, `Bool` or
`State` (a small number that stands for a name, such as "on" or "off").

A null is a missing value. Use `Column::from_options` to insert values with
nulls, and `query_rows` to read any type back:

```rust
let starts = client.series_create("pump7.starts", ValueType::Int64, None)?;
client.insert_series_values(starts, &[t0, t0 + 1000], &Column::from_options([Some(3i64), None]))?;
let rows = client.query_rows(starts, .., QueryOpts::asc())?;
println!("{:?}", rows.values.to_options::<i64>());   // Some([Some(3), None])
```

Aggregates skip nulls. For int64 and bool series, `isum`, `imin` and `imax`
in a bucket are exact (the float fields may round).

## Delete, replace and retention

```rust
client.delete_range(starts, t0..t0 + 3_600_000)?;   // one hour, its end excluded
client.replace_range(starts, t0..t0 + 2000, &[t0], &[7i64])?;
client.set_retention(starts, 30 * 86_400_000)?;     // keep 30 days; 0 keeps everything
assert_eq!(client.retention(starts)?, 30 * 86_400_000);
```

A delete returns once it is durable; queries stop seeing the rows at once.
A replace is atomic: readers see the old rows or the new ones, never a mix.
Rows older than a series' retention limit disappear from queries at once and
from disk as merges get to them. These need a server of protocol version 7; an
older one gets no request and the call fails with `Status::Invalid`.

## Compression

Inserts are compressed with zstd before they are sent (level 1). This makes
them smaller on the network and usually faster. To change it:

```rust
client.set_compression(0)?;   // off: send plain arrays
client.set_compression(3)?;   // smaller, but uses more CPU
```

Servers older than protocol version 3 always get uncompressed inserts.

## Insert benchmark

`examples/insert_bench.rs` measures how fast inserts go. Make a test file
with `tsdb-bench`, then load it over 4 connections:

```sh
../../build/tsdb-bench --export /tmp/x.rb --series 10 --points 100000
cargo run --release --example insert_bench -- /tmp/x.rb --addr 127.0.0.1:7070 --conns 4 --compression 1
```

It prints rows per second and bytes sent per row. Try `--compression 0` to compare.

`examples/query_bench.rs` runs the random query windows of `tsdbc-bench query` from this client
(`--conns N` threads, `--seconds S`); `../../bench/clients/run.sh` runs it next to the other clients and the
embedded library. `examples/embedded_bench.rs` (feature `embedded`) loads and queries the same way through an
embedded `Db` (`--load FILE`, `--qps THREADS`, as `tsdb-bench`), and `examples/embedded_suite.rs` runs the
whole default suite of `tsdb-bench` through it.

## Embedded

The feature `embedded` puts the database itself into your program: no
tsdbd, no network. `Db::open` takes a directory and the engine runs in
your process at C speed, with the same methods as `Client` (the
`Database` trait has both, so code written against `impl Database` runs
on either).

```toml
tsdb-client = { path = "../tsdb/client/rust", features = ["embedded"] }
```

The build compiles the C engine (the `tsdb-sys` crate in `../rust-sys`)
with your C compiler, and zstd with it: nothing to install. It runs on
any x86-64 and picks AVX2 or AVX-512 code at run time; the feature
`native` builds for this CPU only. `embedded-system` links a `libtsdb.so`
you built with make instead (from `TSDB_LIB_DIR`, by default the
repository's `build/`).

```rust
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
```

A `Db` is `Send` and `Sync`: share it between threads (`Arc<Db>`), they
run in parallel. One process at a time can open a directory for writing;
others get `Status::Locked`, readers open it with `read_only: true`.
`query_chunks` hands out the engine's own buffers without copying.

## API documentation

```sh
cargo doc --no-deps --open
```

## Build and test

```sh
make                 # in the repository root: builds build/tsdbd
cd client/rust
cargo test           # starts ../../build/tsdbd on free ports
cargo fmt --check && cargo clippy --all-targets -- -D warnings
```

The tests use `$TSDBD` instead of `../../build/tsdbd` if it is set, and put
their data under `$TSDB_TEST_DIR` or the system temp directory.
`tests/fixtures/gen_calendar_fixtures.sh` regenerates the time zone test
data from the C client.
