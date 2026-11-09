# intellistream-tsdb-embedded (Python)

`tsdb_embedded.Db` is the tsdb database itself inside your Python process: no tsdbd, no network. Give it a
directory; it has the same methods as `tsdb_client.Client`, so code switches between the two by changing one line.
The engine is compiled into the module, runs on any x86-64 and picks AVX2 or AVX-512 code at run time.

**Licence: AGPL-3.0-only** (a commercial licence is available from IntelliStream AS; see `LICENSING.md` at the
root of the repository). The network client, `intellistream-tsdb-client` (module `tsdb_client`), is a separate package
under Apache-2.0: a program that only talks to tsdbd never needs this one. This package depends on it
(`tsdb_client.TsdbError` and its subclasses are what `Db` raises), and `from tsdb_client import Db`, which worked
up to 0.10.0, now says where `Db` went.

## Install

You need Rust (`cargo`), a C compiler and Python 3.9 or newer:

```sh
make                                                  # in the repository root (the engine's sources)
cd client/python-embedded
python3 -m venv /tmp/tsdb-venv
/tmp/tsdb-venv/bin/pip install maturin
VIRTUAL_ENV=/tmp/tsdb-venv /tmp/tsdb-venv/bin/maturin develop --release
```

(Install `intellistream-tsdb-client` of the same version too; the build above does not need the network client's
own build, but at run time `import tsdb_embedded` imports it for the exceptions.)

## Embedded

```python
import tsdb_embedded

with tsdb_embedded.Db("/tmp/tsdb-embedded") as db:
    s = db.series("pump7.flow")
    t0 = 1_700_000_000_000  # one row a minute for a day
    db.insert_series(s, [t0 + i * 60_000 for i in range(1440)], [float(i % 60) for i in range(1440)])
    ts, values = db.query_raw(s, t0, t0 + 180_000, numpy=False)
    for t, v in zip(ts, values):
        print("raw ", t, v)
    for b in db.query_group(s, unit="day", timezone="Europe/Oslo"):
        print("day ", b.start, b.count, b.sum)
```

The engine works with the GIL released, and threads sharing one `Db` run in parallel. One process at a time can
open a directory for writing (others get `ERR_LOCKED`); readers open it with `Db(path, read_only=True)` and call
`refresh()` to see new data. The keyword arguments of `Db` are the engine's options, such as `fsync=False` or
`wal_sync_ms=100`; `help(tsdb_embedded.Db)` lists them.

## Errors and result types

Errors are `tsdb_client.TsdbError` (and its subclasses): one `except tsdb_client.TsdbError` catches the errors of a
`Client` and of a `Db`. The result classes (`Buckets`, `Many`, `Stats`, ...) are this package's own types, named
`tsdb_embedded.Buckets` and so on, with the attributes of the ones `tsdb_client` returns; an `isinstance` check
against `tsdb_client.Buckets` is false for them, so write code that uses the attributes, not the class.

## Many queries at once

The `_many` calls take a whole list of queries in one call and run them in parallel on every CPU, with Python out of
the way:

```python
import numpy as np
import tsdb_embedded

with tsdb_embedded.Db("/tmp/tsdb-many") as db:
    t0 = 1_700_000_000_000
    ts = t0 + np.arange(1440, dtype=np.int64) * 60_000  # one row a minute for a day
    pumps = [db.series(f"pump{i}.flow") for i in range(3)]
    db.insert_many([(s, ts, np.full(1440, i + 1.0, dtype=np.float32)) for i, s in enumerate(pumps)])

    # the first hour of every pump: one (timestamps, values) pair per query
    for i, (t, v) in enumerate(db.query_raw_many(pumps, t0, t0 + 3_600_000)):
        print("raw  ", f"pump{i}", len(t), v.sum())

    # hourly buckets of every pump, then numpy over all of them at once
    hours = db.query_agg_many(pumps, t0, t0 + 86_400_000, 3_600_000)
    print("hours", len(hours), len(hours[0]), hours.buckets.sum.sum())
```

The multi-series calls (`query_values_multi`, `query_buckets_multi`, `query_spread_multi`, `query_twa_multi`,
`query_lttb_multi`) and `latest()` / `latest_by_name()` are those of `tsdb_client.Client`: see its README.

## Benchmarks

`bench_embedded.py` loads and queries through a `Db` the way `tsdb-bench` does (`--load FILE`, `--qps THREADS`), and
`bench_embedded_suite.py` runs the whole default suite of `tsdb-bench` through it; `../../bench/clients/run.sh` runs
them next to the other clients. They import `bench_common.py` from `../python`.

## Build and test

```sh
make                                                       # in the repository root: builds build/tsdbd
cd client/python-embedded
/tmp/tsdb-venv/bin/pip install maturin pytest numpy ../python   # the network client, for the exceptions and the tests
VIRTUAL_ENV=/tmp/tsdb-venv /tmp/tsdb-venv/bin/maturin develop --release
/tmp/tsdb-venv/bin/python -m pytest tests
```

The tests run the network client's tests on a `Db` as well (they import them from `../python/tests`), and compare
`Db` with `../../build/tsdbd` (or `$TSDBD`).
