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

The connect gives up after 10 seconds even without a `timeout`.
Without a `timeout` a call waits as long as the server takes, so a server that
hangs (a stalled disk, a dead peer) hangs your program. With one, the connect
and every later wait for the server is bounded: the longest silence it puts
up with, not a deadline for the whole call (`client.set_timeout(30)` changes
it later). A call that times out raises `TsdbError` with
`tsdb_client.ERR_IO` and closes the client; connect again. TCP keepalive is on:
after 30 s of silence the client probes every 10 s and drops the connection
after 3 unanswered probes (`keepalive=False` turns it off).

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

The result also has a numpy array per field, which is faster than a loop
when there are many buckets: `b = client.query_agg(...)`, then
`b.counts.sum()` or `b.max.max()`.

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

## Spread, time-weighted averages and charts

```python
from tsdb_client import Db

with Db("/tmp/tsdb-analytics") as db:
    s = db.series_create("tank1.level", "float64")
    db.insert_series_values(s, [0, 1000, 3000, 4000], [10.0, 20.0, 30.0, 40.0])

    (b,) = db.query_spread(s)                        # count, sum, min, max and the spread
    print("spread", b.range, b.var_pop, b.stddev_pop)

    (w,) = db.query_twa(s, 0, 5000)                  # each value weighted by how long it held
    print("twa", w.twa, w.covered_ms)

    ts, values = db.query_lttb(s, 3, numpy=False)    # at most 3 points that keep the shape
    print("chart", ts)

    last = db.query_agg(s, None, None, 1000, order="desc", limit=1)
    print("last", last[0].start, last[0].sum)
```

`query_spread` gives what `query_agg` gives plus `range` (max - min), the
population and sample variance (`var_pop`, `var_samp`) and standard deviation
(`stddev_pop`, `stddev_samp`); `query_spread_group` and `query_spread_bounds`
do the same by calendar periods and between given times. With numpy each is
also a column: `db.query_spread(s, 0, t1, 60_000).stddev_pop.max()`.

`query_twa` (and `query_twa_group`, `query_twa_bounds`) weighs each reading by
how long it held, which suits readings sent only when the value changes.
`method="linear"` draws straight lines between readings instead of steps;
`max_gap_ms` makes a longer silence count as unknown. The result is a
`TwaBuckets` with `start`, `twa`, `area` (value x ms), `covered_ms` and `counts`.

`query_lttb` thins a series out for a chart: at most the number of points you
ask for, chosen so the line keeps its shape.

Every query of one series takes `order="desc"` (newest first) and `limit`:
`query_raw`, `query_values`, `query_points`, `query_agg`, `query_group`,
`query_bounds`, the `query_spread*` and `query_twa*` calls and `query_lttb`.
Of the batch calls, `query_raw_many` takes them too; `query_agg_many` does
not. There is no batch call for the spread or time-weighted averages in
0.9.0: run `query_spread` or `query_twa` per series. Over TCP the
spread needs a server of protocol version 8, time-weighted averages 6, LTTB 5
(9 with `order` or `limit`); an older server gets no request and the call
fails with an invalid-argument error.

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

## Delete, replace and retention

```python
client.delete_range(starts, t0, t0 + 3_600_000)  # one hour, its end excluded; None leaves a side open
client.replace_range(starts, t0, t0 + 2000, [t0], [7])
client.set_retention(starts, 30 * 86_400_000)    # keep 30 days; 0 keeps everything
assert client.retention(starts) == 30 * 86_400_000
```

A delete returns once it is durable; queries stop seeing the rows at once.
A replace is atomic: readers see the old rows or the new ones, never a mix.
Rows older than a series' retention limit disappear from queries at once and
from disk as merges get to them. These need a server of protocol version 7; an
older one gets no request and the call fails with an invalid-argument error.

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
`bench_embedded.py` loads and queries the same way through an embedded `Db` (`--load FILE`, `--qps THREADS`, as
`tsdb-bench`), and `bench_embedded_suite.py` runs the whole default suite of `tsdb-bench` through it.

## Embedded

`tsdb_client.Db` is the database itself inside your Python process: no
tsdbd, no network. Give it a directory; it has the same methods as
`Client`, so code switches between the two by changing one line. The
engine is compiled into the module, runs on any x86-64 and picks AVX2 or
AVX-512 code at run time.

```python
import tsdb_client

with tsdb_client.Db("/tmp/tsdb-embedded") as db:
    s = db.series("pump7.flow")
    t0 = 1_700_000_000_000  # one row a minute for a day
    db.insert_series(s, [t0 + i * 60_000 for i in range(1440)], [float(i % 60) for i in range(1440)])
    ts, values = db.query_raw(s, t0, t0 + 180_000, numpy=False)
    for t, v in zip(ts, values):
        print("raw ", t, v)
    for b in db.query_group(s, unit="day", timezone="Europe/Oslo"):
        print("day ", b.start, b.count, b.sum)
```

The engine works with the GIL released, and threads sharing one `Db` run
in parallel. One process at a time can open a directory for writing
(others get `ERR_LOCKED`); readers open it with `Db(path, read_only=True)`
and call `refresh()` to see new data. The keyword arguments of `Db` are
the engine's options, such as `fsync=False` or `wal_sync_ms=100`;
`help(tsdb_client.Db)` lists them.

## Many series in one request

The `_multi` calls ask for many series at once: one request, one answer,
in the order of your list. The server (or, for `Db`, the database in this
process) reads the series in parallel and from one moment in time, so an
insert that wrote several series at once is seen whole or not at all.
They need tsdbd 0.9 with protocol version 10.

```python
import numpy as np
import tsdb_client

with tsdb_client.Db("/tmp/tsdb-multi") as db:
    t0 = 1_700_000_000_000
    ts = t0 + np.arange(1440, dtype=np.int64) * 60_000  # one row a minute for a day
    pumps = [db.series(f"pump{i}.flow") for i in range(3)]
    for i, s in enumerate(pumps):
        db.insert_series(s, ts, np.full(1440, i + 1.0, dtype=np.float32))

    # hourly buckets of every pump in one call; numpy over all of them at once
    r = db.query_buckets_multi(pumps, t0, t0 + 86_400_000, interval_ms=3_600_000)
    print(len(r), r.offsets.tolist(), r.sum.sum())
    print(len(r[1]), r[1].sum[0])  # pump 1 alone, as query_agg() gives it

    # the newest row of every pump
    last = db.query_values_multi(pumps, order="desc", limit=1)
    print(last.values.tolist())
```

There are five: `query_values_multi`, `query_buckets_multi`,
`query_spread_multi`, `query_twa_multi` and `query_lttb_multi`, on
`Client`, `Pool` and `Db`. `series` is a list or a numpy `uint32` array.
`start`, `end` and `interval_ms` are one number for every series or one
per series. The bucket calls take `interval_ms`, or `unit`, `count` and
`timezone` (as `query_group`), or `bounds` (as `query_bounds`).

The answer `r` holds every series' rows (`r.ts`, `r.values`) or buckets
(`r.buckets`, and its columns such as `r.sum` straight from `r`) one after
the other; series `i` is at `r.offsets[i]:r.offsets[i + 1]`, and `r[i]` is
what the one-series call returns. Each series succeeds or fails on its own:
`r.status[i]` is 0 or the error code. By default (`errors="raise"`) a
failed series raises `TsdbError`, with `err.index` (which series) and
`err.result` (the whole answer); with `errors="status"` you get the answer
and check `r.status` yourself.

## Many queries at once

Each call into the module costs a few microseconds of Python time, and
Python runs one thread at a time. With hundreds of thousands of small
queries a second, that becomes the limit. The `_many` calls take a whole
list of queries in one call and run them in parallel on every CPU, with
Python out of the way: one Python thread then does as much as many.

```python
import numpy as np
import tsdb_client

with tsdb_client.Db("/tmp/tsdb-many") as db:
    t0 = 1_700_000_000_000
    ts = t0 + np.arange(1440, dtype=np.int64) * 60_000  # one row a minute for a day
    pumps = [db.series(f"pump{i}.flow") for i in range(3)]
    db.insert_many([(s, ts, np.full(1440, i + 1.0, dtype=np.float32)) for i, s in enumerate(pumps)])

    # the first hour of every pump: one (timestamps, values) pair per query
    for s, (t, v) in zip(pumps, db.query_raw_many(pumps, t0, t0 + 3_600_000)):
        print("raw  ", s, len(t), v.sum())

    # hourly buckets of every pump, then numpy over all of them at once
    hours = db.query_agg_many(pumps, t0, t0 + 86_400_000, 3_600_000)
    print("hours", len(hours), len(hours[0]), hours.buckets.sum.sum())
```

`start`, `end` and `interval_ms` are one number for every query or a list
(or numpy array) with one per query. The answers come back in the order
of the queries. If queries fail, the call raises the error of the first
failing one, and `err.index` says which one it was (it is `None` for
errors of the one-query calls). Every entry before it has run; later
ones may have run too.

The call copies your arrays once, before it lets go of Python, so other
Python threads keep running while it works, and changing an array from
another thread during the call cannot hurt it. In `insert_many`, entries
for the same series are written in list order, so where two of them have
a row at the same time, the later entry wins, as with separate inserts.

Over the network, `tsdb_client.Pool` does the same with several
connections to one server (by default one per CPU of this machine):

```python
with tsdb_client.Pool("127.0.0.1", 7070, connections=16) as pool:
    hours = pool.query_agg_many(pumps, t0, t0 + 86_400_000, 3_600_000)
```

A connection that breaks is replaced on its next use. When the server
restarts, every connection is replaced: a query that failed because its
connection broke is sent once more on a new one, so queries keep working.
An insert is not sent again (the server may already have written it), so
the first insert after a restart can fail; send it again.

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
