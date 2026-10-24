# tsdb_client (Python)

A Python client for `tsdbd`, the TCP server of tsdb: a PyO3 extension module
over the Rust client in `../rust`, built with maturin. numpy is optional.

## Quick start

Start a server (`make` in the repository root, then
`./build/tsdbd --dir /tmp/tsdb-demo`) and run this; `tests/test_readme.py`
runs it against a live server.

```python
from tsdb_client import Client

with Client("127.0.0.1", port=7070) as client:  # leaving the block closes it
    # Get or create a series, then insert one point a minute for a day.
    series = client.series("pump7.flow")
    t0 = 1_699_999_200_000  # 2023-11-14 22:00 UTC, in ms since the epoch
    ts = [t0 + i * 60_000 for i in range(1440)]
    values = [float(i % 60) for i in range(1440)]
    client.insert_series(series, ts, values)

    # Raw query: the newest three points (numpy arrays if numpy is installed).
    newest_ts, newest_values = client.query_raw(series, order="desc", limit=3)
    for t, v in zip(newest_ts, newest_values):
        print("raw ", int(t), float(v))

    # Aggregate: hourly buckets over the first six hours.
    for b in client.query_agg(series, t0, t0 + 6 * 3_600_000, interval_ms=3_600_000):
        print("hour", b.start, b.count, round(b.avg, 2), b.min, b.max)

    # Group by calendar day in a time zone.
    for b in client.query_group(series, unit="day", timezone="Europe/Oslo"):
        print("day ", b.start, b.count, b.sum)
```

## API

`Client(host="127.0.0.1", port=7070, timeout=None)` is one TCP connection.
Every network call releases the GIL; threads may share a client (they take
turns) or open one each. Timestamps are int milliseconds since the epoch,
values 32-bit floats. Ranges are `[start, end)`; `None` means unbounded.

| Method | |
|---|---|
| `close()`, `with Client(...) as c:` | wait for pending inserts, then close |
| `protocol_version()` | HELLO |
| `series(name, create=True)`, `series_at(name, path)`, `series_name(id)` | series ids and names |
| `insert_series(id, ts, values)`, `insert(series, ts, values)` | insert one series or many, and wait |
| `insert_series_async(...)`, `insert_async(...)`, `wait()`, `pending` | pipelined inserts |
| `query_raw(id, start=None, end=None, *, order="asc", limit=None, numpy=None)` | `(timestamps, values)` |
| `query_points(...)` | the same as `query_raw` |
| `query_agg(id, start=None, end=None, interval_ms=0)` | `list[Bucket]`, aligned to the epoch |
| `query_group(id, start=None, end=None, *, unit=None, count=1, timezone=None, order="asc", limit=None)` | calendar buckets |
| `commit()`, `flush()`, `optimize()`, `stats()` | maintenance; `stats()` returns `Stats` |

Columns can be numpy arrays or `array.array`s of the matching type (int64
timestamps, float32 or float64 values, uint32 series ids), which are copied
without iterating, or any sequence of numbers. Raw queries return numpy
arrays (`int64`, `float32`) when numpy can be imported, or lists; pass
`numpy=False` for lists or `numpy=True` to require numpy. `limit=0` returns
nothing. Units are `"second"`, `"minute"`, `"hour"`, `"day"`, `"week"`,
`"month"`, `"quarter"` and `"year"`; `unit=None` gives one bucket.

`Bucket` has `start`, `count`, `sum`, `min`, `max` and `avg`. Every error is a
`TsdbError` whose `code` is the `TSDB_ERR_*` number (`tsdb_client.ERR_NOTFOUND`
and so on; `strerror(code)` gives the text). Errors of asynchronous inserts
are raised by `wait()`, or by the next call that reads a reply.

## Build and test

```sh
make                                   # in the repository root: builds build/tsdbd
cd client/python
python3 -m venv /tmp/tsdb-venv         # any venv outside the source tree
/tmp/tsdb-venv/bin/pip install maturin pytest   # and numpy, optionally
VIRTUAL_ENV=/tmp/tsdb-venv /tmp/tsdb-venv/bin/maturin develop --release
/tmp/tsdb-venv/bin/python -m pytest tests
```

`maturin build --release` makes a wheel instead. The tests start
`../../build/tsdbd` (or `$TSDBD`) on free ports with data under
`$TSDB_TEST_DIR` or the system temp directory; the numpy test is skipped
when numpy is not installed. Type stubs are in `tsdb_client.pyi`.
