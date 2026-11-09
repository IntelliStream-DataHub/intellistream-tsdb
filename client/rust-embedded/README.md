# intellistream-tsdb-embedded (Rust)

The tsdb database inside your own Rust program: no tsdbd, no network. `Db` has the same methods as the
network `Client` of `intellistream-tsdb-client`, with the same answers, and runs the C engine in your process.

**Licence: AGPL-3.0-only** (a commercial licence is available from IntelliStream AS; see
[LICENSING.md](../../LICENSING.md)). This crate contains the engine, so a program that links it is under the
AGPL-3.0 or needs the commercial licence. The network client, `intellistream-tsdb-client`, is Apache-2.0 and
has no engine in it. This crate re-uses its types, errors and the `Database` trait, and pins it to the same
version (`=`), because it also uses a hidden module of it that carries no semver promise.

The package is called `intellistream-tsdb-embedded`; the library inside it is `tsdb_embedded`.

Coming from 0.10 or earlier: the `embedded`, `embedded-system` and `native` features of
`intellistream-tsdb-client` are gone. Depend on this crate (`features = ["native"]` or `"system"` here), and
`use tsdb_embedded::{Db, Options};` (`Db`, `Options`, `ValueCodec`, `PartInfo`, `ChunkRef` and `RawValues` live
here; everything else stays in `tsdb_client`).

## Embedded

This crate puts the database itself into your program: no
tsdbd, no network. `Db::open` takes a directory and the engine runs in
your process at C speed, with the same methods as `Client` (the
`Database` trait of the client crate has both, so code written against `impl Database` runs
on either).

```toml
intellistream-tsdb-embedded = { path = "../tsdb/client/rust-embedded" }
# the types, the Database trait and the network client come from the Apache-2.0 crate, the same version:
intellistream-tsdb-client = { path = "../tsdb/client/rust" }
```

The build compiles the C engine (the `intellistream-tsdb-sys` crate in `../rust-sys`)
with your C compiler, and zstd with it: nothing to install. It runs on
any x86-64 and picks AVX2 or AVX-512 code at run time; the feature
`native` builds for this CPU only. `system` links a `libtsdb.so`
you built with make instead (from `TSDB_LIB_DIR`, by default the
repository's `build/`).

```rust
use tsdb_client::{Group, QueryOpts, Unit};
use tsdb_embedded::{Db, Options};

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

## Many queries at once

`query_points_many`, `query_agg_many` and `insert_many` take a whole list of queries (or inserts) and run them in
parallel on every CPU. The answers come back in the order of the queries; if one fails, the call returns the
error of the first that failed. The `_multi` calls (`query_rows_multi`, `query_buckets_multi`, ...) and `latest`
are as on `Client`: see the README of `intellistream-tsdb-client`.

## Benchmarks

`examples/embedded_bench.rs` loads and queries like `tsdb-bench` (`--load FILE`, `--qps THREADS`), and
`examples/embedded_suite.rs` runs the whole default suite of `tsdb-bench` through `Db`;
`../../bench/clients/run.sh` runs them next to the other clients.

## Build and test

```sh
make                 # in the repository root: builds build/tsdbd, which the tests compare with
cd client/rust-embedded
cargo test           # also builds the engine (../rust-sys)
cargo fmt --check && cargo clippy --all-targets -- -D warnings
```
