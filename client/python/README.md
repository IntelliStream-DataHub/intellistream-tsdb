# tsdb_client (Python)

A Python library for talking to `tsdbd`, the tsdb database server. You use it
to store numbers with a timestamp (a "time series") and to read them back,
raw or summed up per hour, day or month.

It is written in Rust (it uses the Rust client in `../rust`) and works with
plain lists. If numpy is installed, queries return numpy arrays.

## Quick start

1. Build and start a server. In the root of the tsdb repository:

   ```sh
   make
   ./build/tsdbd --dir /tmp/tsdb-demo
   ```

   It listens on port 7070. Leave it running and open a second terminal.

2. Build and install the library into a virtual environment. You need Rust
   (`cargo`) and Python 3.9 or newer:

   ```sh
   cd client/python
   python3 -m venv /tmp/tsdb-venv
   /tmp/tsdb-venv/bin/pip install maturin
   VIRTUAL_ENV=/tmp/tsdb-venv /tmp/tsdb-venv/bin/maturin develop --release
   ```

3. Save this program as `quickstart.py` and run it with `/tmp/tsdb-venv/bin/python quickstart.py`:

```python
from tsdb_client import Client

with Client("127.0.0.1", port=7070) as client:  # the connection closes at the end of the block
    # Get the id of a series (it is created the first time).
    series = client.series("pump7.flow")

    # Insert one value per minute for a day. Times are ms since 1970-01-01 UTC.
    t0 = 1_699_999_200_000  # 2023-11-14 22:00 UTC
    ts = [t0 + i * 60_000 for i in range(1440)]
    values = [float(i % 60) for i in range(1440)]
    client.insert_series(series, ts, values)

    # Read the newest three values.
    newest_ts, newest_values = client.query_raw(series, order="desc", limit=3)
    for t, v in zip(newest_ts, newest_values):
        print("raw ", int(t), float(v))

    # Count, average, min and max per hour, for the first six hours.
    for b in client.query_agg(series, t0, t0 + 6 * 3_600_000, interval_ms=3_600_000):
        print("hour", b.start, b.count, round(b.avg, 2), b.min, b.max)

    # Sum per day, where days begin at midnight in Oslo.
    for b in client.query_group(series, unit="day", timezone="Europe/Oslo"):
        print("day ", b.start, b.count, b.sum)
```

It prints:

```text
raw  1700085540000 59.0
raw  1700085480000 58.0
raw  1700085420000 57.0
hour 1699999200000 60 29.5 0.0 59.0
...
day  1699916400000 60 1770.0
day  1700002800000 1380 40710.0
```

## Connect

`Client(host, port=7070, timeout=None)` opens one connection. Use it in a
`with` block, or call `client.close()` when you are done. Threads can share
a client (they take turns) or open one each. `timeout` is in seconds.

Errors are raised as `TsdbError`. Its `code` tells what went wrong, for
example `tsdb_client.ERR_NOTFOUND`.

## Create a series

A series has a name and a number (its id). You use the id in every other call.

```python
series = client.series("pump7.flow")                # creates it if it does not exist
series = client.series("pump7.flow", create=False)  # only finds it
```

## Insert rows

A row is a timestamp and a value. Timestamps are int milliseconds since
1970-01-01 UTC. You pass two lists (or numpy arrays) of the same length:

```python
client.insert_series(series, [1_700_000_000_000, 1_700_000_060_000], [4.2, 4.3])
```

If you write the same timestamp twice, the last value wins. The call returns
when the server has stored the rows.

For many small inserts, `insert_series_async` is much faster: it does not
wait for the answer. Call `client.wait()` at the end to get any error.

## Read raw rows

```python
ts, values = client.query_raw(series)                           # everything, oldest first
ts, values = client.query_raw(series, order="desc", limit=10)   # the newest ten
ts, values = client.query_raw(series, t0, t1)                   # t0 <= time < t1
ts, values = client.query_raw(series, numpy=False)              # lists, even with numpy installed
```

`start` and `end` are optional: `None` means no limit. The end time itself is
not included.

## Aggregate

`query_agg` splits a time range into buckets of a fixed length. Each
`Bucket` has `start`, `count`, `sum`, `min`, `max` and `avg`:

```python
for b in client.query_agg(series, t0, t1, interval_ms=3_600_000):  # one bucket per hour
    print(b.start, b.count, b.avg)
(total,) = client.query_agg(series)                                 # one bucket for everything
```

## Group by day in a time zone

A day in Oslo starts at 23:00 or 22:00 UTC, depending on daylight saving
time. `query_group` takes care of that:

```python
for b in client.query_group(series, unit="day", timezone="Europe/Oslo"):
    print(b.start, b.sum)  # b.start is the UTC time the local day begins
```

`unit` is `"second"`, `"minute"`, `"hour"`, `"day"`, `"week"`, `"month"`,
`"quarter"` or `"year"`; `count=15` with `unit="minute"` gives quarter hours.
`timezone` is a name such as `"Europe/Oslo"`, an offset such as `"+02:00"`,
`"UTC"`, or a `zoneinfo.ZoneInfo`.

The server only knows UTC. The client works out when each local day begins
and sends those times (`tsdb_client.calendar_bounds` and `query_bounds` let
you do the two steps yourself). With a server older than protocol version 4,
the client sends the zone name instead.

## Value types and nulls

`series()` makes series of 32-bit floats. For other types, use
`series_create` with `"float64"`, `"int64"`, `"bool"` or `"state"` (a small
number that stands for a name, such as "on" or "off").

A null is a missing value: write it as `None`. Use `insert_series_values` and
`query_values` for typed series:

```python
starts = client.series_create("pump7.starts", "int64")
client.insert_series_values(starts, [t0, t0 + 1000, t0 + 2000], [3, None, 5])
ts, values = client.query_values(starts, numpy=False)  # values == [3, None, 5]

mode = client.series_create("pump7.mode", "state")
client.insert_series_values(mode, [t0], [client.state(mode, "on")])
ts, codes = client.query_values(mode)
print(client.state_names(mode, codes))  # ['on']
```

With numpy, `query_values` returns a masked array: nulls are masked.
Aggregates skip nulls. For int64 and bool series, `isum`, `imin` and `imax`
in a bucket are exact (the float fields may round).

## Compression

Inserts are compressed with zstd before they are sent (level 1). This makes
them smaller on the network and usually faster. To change it:

```python
client.set_compression(0)  # off: send plain arrays
client.set_compression(3)  # smaller, but uses more CPU
```

Servers older than protocol version 3 always get uncompressed inserts.

## Insert benchmark

`bench_insert.py` measures how fast inserts go. Make a test file with
`tsdb-bench`, then load it over 4 connections (numpy makes it faster):

```sh
../../build/tsdb-bench --export /tmp/x.rb --series 10 --points 100000
/tmp/tsdb-venv/bin/python bench_insert.py /tmp/x.rb --addr 127.0.0.1:7070 --conns 4 --compression 1
```

It prints rows per second and bytes sent per row. Try `--compression 0` to compare.

`bench_query.py` runs the random query windows of `tsdbc-bench query` (`--conns N` threads, which share the GIL;
see the file). `../../bench/clients/run.sh` runs both next to the other clients and the embedded library.

## API documentation

Every class and method has a docstring, and `tsdb_client.pyi` gives editors
the same text with the types. Read them with Python's built-in `pydoc`:

```sh
/tmp/tsdb-venv/bin/python -m pydoc tsdb_client                       # in the terminal
/tmp/tsdb-venv/bin/python -m pydoc -w tsdb_client                    # writes tsdb_client.html
```

## Build and test

```sh
make                                                # in the repository root: builds build/tsdbd
cd client/python
/tmp/tsdb-venv/bin/pip install maturin pytest numpy # numpy is optional
VIRTUAL_ENV=/tmp/tsdb-venv /tmp/tsdb-venv/bin/maturin develop --release
/tmp/tsdb-venv/bin/python -m pytest tests
```

The tests start `../../build/tsdbd` (or `$TSDBD`) on free ports, with data
under `$TSDB_TEST_DIR` or the system temp directory. `maturin build
--release` makes a wheel.
