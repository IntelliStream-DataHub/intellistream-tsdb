# tsdb-client (Rust)

A blocking Rust client for `tsdbd`, the TCP server of tsdb. It mirrors the C
client (`client/tsdb_client.h`), speaks the protocol in
`include/tsdb_protocol.h`, and depends only on `zstd` (for packed inserts).

## Quick start

Start a server (`make` in the repository root, then
`./build/tsdbd --dir /tmp/tsdb-demo`) and run
`cargo run --example quickstart` (optionally with `host:port`). This is
`examples/quickstart.rs`; a test checks that the two stay identical and runs
it against a live server.

```rust
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
```

## API

One `Client` is one TCP connection; methods take `&mut self`, so use one
client per thread. Timestamps are `i64` milliseconds since the epoch. Time ranges are any
`RangeBounds<i64>`: `t0..t1` is `[t0, t1)`, `t0..=t1` includes `t1`, `..` is
everything.

| Method | |
|---|---|
| `Client::connect(addr)`, `connect_timeout(&addr, d)`, `from_stream(s)` | open a connection |
| `set_timeout(Option<Duration>)` | socket read/write timeout |
| `close()` | wait for pending inserts, then close (dropping closes without waiting) |
| `protocol_version()` | HELLO |
| `series(name)`, `series_lookup(name)`, `series_at(name, path)` | get or create, look up, create in a volume |
| `series_name(id)` | the name of a series |
| `insert_series(id, &ts, &values)`, `insert(&series, &ts, &values)` | insert one series or many, and wait |
| `insert_series_async(..)`, `insert_async(..)`, `wait()`, `pending()` | pipelined inserts |
| `query_points(id, range, opts)` | raw points as `Points { ts, values }` |
| `query_raw(id, range, opts, \|ts, values\| ControlFlow)` | raw points streamed in chunks |
| `series_create(name, ValueType, Option<path>)`, `series_type(id)` | typed series (protocol version 2) |
| `insert_series_values(id, &ts, column)`, `insert_values(&series, &ts, column)` | typed values with nulls, and wait |
| `insert_series_values_async(..)`, `insert_values_async(..)` | the same, pipelined |
| `query_rows(id, range, opts)` | typed rows as `Rows { ts, values: Column }` |
| `query_values(id, range, opts, \|ts, &Column\| ControlFlow)` | typed rows streamed in chunks |
| `state(id, name)`, `state_lookup(id, name)`, `state_name(id, code)` | the state dictionary of a series' volume |
| `query_agg(id, range, interval_ms)` | `Vec<Bucket>` aligned to the epoch; 0 = one bucket |
| `query_group(id, range, Option<&Group>, opts)` | calendar buckets in a time zone |
| `commit()`, `flush()`, `optimize()`, `stats()` | maintenance |
| `set_compression(level)`, `compression()`, `bytes_sent()` | zstd level of inserts (0: plain), bytes written to the socket |
| `last_query_stats()` | `QueryStats` of this connection's previous query: rows covered, decoded or answered from granule statistics, bytes, time |

`QueryOpts` carries the order and limit (`QueryOpts::desc().limit(5)`);
`Group::new(Unit::Day).count(1).timezone("Europe/Oslo")` a calendar grouping.

### Value types and nulls

A series has a `ValueType`: `Float32` (what `series()` creates), `Float64`,
`Int64`, `Bool` or `State`. Each has a Rust type (`f32`, `f64`, `i64`,
`bool`, and `u32` for state codes; the `Native` trait). A `Column` is typed
values (`Values`) plus an optional validity `Bitmap` (bit set = known, the wire
layout, so nulls cost one bit and no conversion). `Column::from_options` and
`column.to_options::<T>()` / `iter::<T>()` give the `Option<T>` view;
`as_slice::<T>()` the raw values. Inserts take anything that converts into a
`ColumnRef`: `&[T]`, `&[T; N]`, `&Vec<T>`, `&Column`, or
`ColumnRef::from(&values).with_validity(&bitmap)`, so a plain slice is sent
without building a column. The column's type must be the series' type.

The protocol reserves one value per type for null: `i64::MIN`, state code 0,
and the NaNs with bits `NULL_F32_BITS` / `NULL_F64_BITS`; written as known
values they read back as null. Other NaNs are ordinary values.

The version 1 calls (`insert_series`, `query_points`, `query_raw`) remain the
fast path for float32 series, with plain `f32` slices; `query_points` reads
nulls as NaN, and the version 1 calls on other types fail with `Status::Invalid`.

`Bucket` has `start`, `count`, `sum`, `min`, `max` (f64) and `avg()`, plus
`isum`, `imin` and `imax`: exact for int64 and bool series (`isum` saturates).
Aggregates skip nulls; `count` counts known values. All aggregates use
QUERY_BUCKETS.

Errors are `tsdb_client::Error`: `Server(Status)` carries the server's
`TSDB_ERR_*` status, `Io`, `Protocol` and `InvalidArgument` are client-side;
`Error::code()` gives the numeric code either way. After an I/O or protocol
error the client refuses further calls, because the stream may be out of step.

Asynchronous inserts are pipelined: the server runs one connection's requests
in order, so the last write still wins. Their errors come back from `wait()`,
or from the next call that reads a reply (that call is then not sent).

Inserts are packed and zstd compressed (protocol version 3, zstd level 1 by
default), float32 ones included. The client asks the server's version with
HELLO before its first insert and sends plain arrays to servers older than
version 3. `set_compression(0)` always sends plain arrays.

## Insert benchmark

`examples/insert_bench.rs` loads a RowBinary file (u32 series, i64 ts, f32
value) the way `tsdbc-bench load` does, and reports rows/s and bytes sent per row:

```sh
../../build/tsdb-bench --export /tmp/x.rb --series 10 --points 100000
cargo run --release --example insert_bench -- /tmp/x.rb --addr 127.0.0.1:7070 --conns 4 --compression 1
```

## Build and test

```sh
make                         # in the repository root: builds build/tsdbd
cd client/rust
cargo build
cargo test                   # starts ../../build/tsdbd on free ports
cargo fmt --check && cargo clippy --all-targets -- -D warnings
```

The tests use `$TSDBD` instead of `../../build/tsdbd` if set (for example a
sanitizer build), and put their data directories under `$TSDB_TEST_DIR` or
the system temp directory.
