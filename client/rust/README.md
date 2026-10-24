# tsdb-client (Rust)

A blocking Rust client for `tsdbd`, the TCP server of tsdb. It mirrors the C
client (`client/tsdb_client.h`), speaks the protocol in
`include/tsdb_protocol.h`, and depends on nothing beyond `std`.

## Quick start

Start a server (`make` in the repository root, then
`./build/tsdbd --dir /tmp/tsdb-demo`) and run
`cargo run --example quickstart` (optionally with `host:port`). This is
`examples/quickstart.rs`; a test checks that the two stay identical and runs
it against a live server.

```rust
use tsdb_client::{Client, Group, QueryOpts, Unit};

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

    client.close()
}
```

## API

One `Client` is one TCP connection; methods take `&mut self`, so use one
client per thread. Timestamps are `i64` milliseconds since the epoch, values
are `Value` (`f32`, defined in `src/value.rs`). Time ranges are any
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
| `query_agg(id, range, interval_ms)` | `Vec<Bucket>` aligned to the epoch; 0 = one bucket |
| `query_group(id, range, Option<&Group>, opts)` | calendar buckets in a time zone |
| `commit()`, `flush()`, `optimize()`, `stats()` | maintenance |

`QueryOpts` carries the order and limit (`QueryOpts::desc().limit(5)`);
`Group::new(Unit::Day).count(1).timezone("Europe/Oslo")` a calendar grouping.
`Bucket` has `start`, `count`, `sum`, `min`, `max` and `avg()`.

Errors are `tsdb_client::Error`: `Server(Status)` carries the server's
`TSDB_ERR_*` status, `Io`, `Protocol` and `InvalidArgument` are client-side;
`Error::code()` gives the numeric code either way. After an I/O or protocol
error the client refuses further calls, because the stream may be out of step.

Asynchronous inserts are pipelined: the server runs one connection's requests
in order, so the last write still wins. Their errors come back from `wait()`,
or from the next call that reads a reply (that call is then not sent).

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
