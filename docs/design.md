# Design

How tsdb stores and serves data. The [README](../README.md) has the quick
start, the tests and the benchmark results.

## Data model and semantics

- A database holds named **series**. A series has one value type, chosen when
  it is created: `float32` (the default), `float64`, `int64`, `bool`, or
  `state`, a string from a small set (on/off/fault, a machine mode) stored as a
  code from the volume's state dictionary (`tsdb_state`, `tsdb_state_name`).
- Every point is `(DateTime64(3) timestamp, value)`; timestamps are
  milliseconds since the epoch.
- Any value may be **null**: unknown, as with a sensor that did not answer.
  Columns carry a validity bitmap at the API (`tsdb_values`); inside, a null is
  a value the type reserves (`INT64_MIN`, state code 0, one NaN payload per
  float type), so those cannot be stored as known values. Aggregates skip
  nulls: `count` is the number of known values, and a bucket of only nulls
  is returned with count 0. Other NaNs are values: ignored by min/max, they
  propagate into sum.
- `(series, ts)` is unique, like a `ReplacingMergeTree` that is always read
  with `FINAL`: writing a timestamp that exists replaces its value (a null
  replaces too), the last write wins (within one batch, the later row), and
  queries never return duplicates.
- Time ranges are half open, `[start, end)`.
- Aggregates: count, sum, min, max, avg. Integer types (int64, bool as 0/1)
  aggregate exactly (`isum`, `imin`, `imax`; the sum saturates at the int64
  limits, `sum` holds it as a double). For states only the count is meaningful.
- Float sums are not bit-reproducible. A float sum depends on the order of the
  additions, and that order follows how parts were merged and how a query was
  split over threads, so the same rows can give sums that differ in the last
  bits after a background merge, and where the running sum overflows a double
  (rows near `DBL_MAX`), +inf in one order and NaN (inf - inf) in another.
  ClickHouse, PostgreSQL and DuckDB behave the same.
  min, max and count are exact. Where reproducible sums matter, store the values
  as int64 in fixed point (21.3 degC as 21300 milli-degrees): sums are added in
  a 128-bit integer inside (and kept that way in each granule's statistics), so
  `isum` is exact whatever the order, as long as the total fits in an int64
  (about 9.2e18; beyond that it is clamped). int64 series compress as well as
  the fixed-point floats.
- One NaN for every NaN result. A sum that hits NaN, and a min or max of a
  bucket that holds only NaNs, is the quiet NaN `0x7ff8000000000000` (in
  float32, as in the server's version 1 rows: `0x7fc00000`), from every query,
  path (memtable, granule statistics, merged parts, threads) and client. An
  addition of NaNs keeps the payload of whichever NaN came first, and that
  order varies; PostgreSQL and ClickHouse do not canonicalise either, but print
  every NaN as `NaN`, while tsdb's clients receive the bits. A NaN result is
  never one of the null patterns (a signalling NaN with payload `0x1001`
  quiets into the float64 null), and granule statistics store their sums
  canonical too. Stored values are untouched: a raw query returns the NaN a
  sensor wrote bit for bit.
- All NaNs are equal. tsdb never compares stored values with each other (a
  write replaces by key, not by value), and where results are compared (the
  tests, the integration model) any NaN equals any other, as in PostgreSQL.
  min and max differ from PostgreSQL on purpose: they skip NaNs and are NaN
  only when a bucket has no other value, where PostgreSQL sorts NaN above
  every number, so one sensor glitch would replace a bucket's max.
- Buckets are fixed lengths aligned to the epoch, like `toStartOfInterval`,
  or calendar periods (day, ISO week, month, quarter, year) in UTC
  (`tsdb_query_group`), or buckets that begin at given instants
  (`tsdb_query_bounds`).
- The engine and the server know UTC only. Local time is the caller's
  business: `tsdb_calendar_bounds` (`include/tsdb_calendar.h`, compiled into
  both the engine and the client library) turns the periods of a time zone into
  UTC bounds, and `tsdb_query_group_local` and `tsdbc_query_group` send those.
  A period runs from its first instant to the next period's first instant, so
  a local midnight that occurs twice begins the day the first time.
- All functions are thread safe. One process opens a database for writing
  (`flock`); any number may open it `read_only` or follow it (below).

## Query statistics

Every query counts what it touched, on the thread that runs it and on the
pool's workers that help it: the parts and granules with rows of the series in
the range, the granules answered from their index entry, the rows and
compressed bytes decoded, the rows read from memtables, and the rows or
buckets returned. `tsdb_last_query_stats` returns them for the calling
thread's last query; `tsdbd` keeps them per connection for `LAST_STATS`, and
the command line prints them after each query. "Rows" are the rows a query
covered, as ClickHouse counts processed rows: a granule answered from its
statistics counts its rows without reading them, which is how a scan can
report billions of rows a second.

## Downsampling: LTTB

`tsdb_query_lttb` (and `QUERY_LTTB`, `tsdbc_query_lttb`, `tsdb DIR query S A B
lttb N`) returns N of a range's points that keep the series' shape on a chart:
Largest-Triangle-Three-Buckets (Sveinn Steinarsson, "Downsampling Time Series
for Visual Representation", 2013). The points are real stored points with their
exact values, in time order, so a chart of 1,000 points from a billion looks
like the billion, spikes included, where per-bucket averages would flatten them.

The algorithm, over the n points of the range:

- The first and the last point are kept. Points 1 .. n-2 fall into N-2 buckets
  of nearly equal count: bucket b starts at `floor(b * every) + 1` with
  `every = (n-2)/(N-2)` in doubles, exactly as the paper's code, ClickHouse's
  `largestTriangleThreeBuckets` and the TimescaleDB toolkit's `lttb()` bound
  them, so that the same points come out. Exact integer division
  (`b(n-2)/(N-2)`) would be the cleaner rule, but where b·every rounds just
  below an integer it starts the bucket one point later: n = 17 with N = 13
  already differs, and so do about 5% of random (n, N) pairs up to n = 1e8,
  each such case changing the chosen points. Past n = 2^53 the doubles lose
  whole points of a boundary, as they do in those systems.
- From each bucket, the point that forms the largest triangle with the point
  kept from the previous bucket and the mean (time, value) of the next bucket
  is kept; for the last bucket, the "next bucket" is the rest of the points
  from `floor((N-2) * every) + 1`: the last point, or the last two where that
  product rounds below n-2 (as in the paper's code). The area
  is the paper's `|(ax - cx)(y - ay) - (ax - x)(cy - ay)| / 2`. Of equal areas
  the first point wins; if no area compares (an overflow to NaN, only with
  values near the float64 limit), the bucket's first point.
- n <= N returns every point. N < 3 is `TSDB_ERR_INVALID`: one or two points
  would only be the ends, which a caller can ask for directly, and a silent
  "first and last" for a chart of 2 pixels hides a mistake.
- Points are the known, finite values. Nulls, NaN and infinities are skipped:
  they have no place on a chart, and one NaN would make every area of the
  previous bucket NaN through the mean. State series are `TSDB_ERR_INVALID`
  (codes have no order to draw); bools are 0 and 1; int64 values are rounded
  to doubles for the area only.
- x is milliseconds since the range's first point, as a double: epoch
  milliseconds (1.7e12) squared in the area would lose the low bits that
  separate neighbouring points.
- Where other systems can still choose differently: they skip no NaN (a NaN
  area never wins there, and poisons the next bucket's mean), and they measure
  x from the epoch (ClickHouse) or in their own unit, which rounds the areas
  differently; only areas equal to the last bits can then pick another point.

How it reads (src/query.c, one snapshot for every pass, so concurrent inserts,
flushes and merges cannot tear the result, and followers work as for any
query):

1. **Count.** The bucket boundaries need n. The granules of the range are
   grouped like the aggregates' clusters; a granule that lies inside the range,
   overlaps no other source (so holds no replaced row) and has a finite float
   sum (a NaN or an infinity would make it NaN or infinite) counts
   `count - nulls` from its index entry without decoding. The rest (the range's
   edges, overlapping sources through the deduplicating merge, granules with
   NaN, memtable rows) are streamed and counted.
2. **Choose.** Choosing in bucket b needs the mean of bucket b+1, known only
   once that bucket has been read. While two buckets fit in 32 MiB (16 bytes a
   point: timestamp and value; buckets up to 1M points), one pass keeps the
   points of the bucket being chosen from and of the one being read, and
   chooses when the next bucket ends. Larger buckets (a billion points into a
   few hundred) take two passes instead: the means of every bucket first
   (O(N) memory), then the choice while streaming again. Both add the
   coordinates in time order from zero, so they produce the same means and the
   same points; `test_lttb` runs every case both ways.

Cost: one decode of the range (two for huge buckets) plus the count, which
decodes nothing for the granules alone in their span: on 10M points, about
two to three times a raw read of the same range. Memory: up to 32 MiB of
buffered buckets per running query (so on a server, times its query workers),
or O(N) means; plus, as for the aggregates, one 32-byte item per granule of the
range and per memtable run for the count (a series of 1e10 points in
8192-row granules: 1.2M items, about 39 MB). The
query statistics describe one read of the range, as for a raw query, with the
points returned as `result_rows`; the count and the means pass are not added.

Tests: `tests/test_lttb.c` checks the engine point for point against a
straightforward array implementation (`tests/lttb_ref.h`) over what
`tsdb_query_values` returns: random walks, steps, spikes, constants, monotonic
series, NaN and infinities, values near the float64 limit, irregular times,
nulls, every numeric type, thresholds 3, 4, n-1, n, n+1 and beyond, ranges
that cut granules; with the data in memtables, in overlapping parts with
corrections, merged, and through a follower. `test_server` does the same over
the network.

## Time-weighted averages

Sensors that report irregularly or only when their value changes make a plain
average wrong: a value that held for an hour counts as much as one that held
for a second. `tsdb_query_twa` (and `_group`, `_bounds`, `QUERY_TWA` on the
wire, `twa` in the command line) weights each value by how long it held. The
contract is in `include/tsdb.h`; in short, following OSIsoft PI and
TimescaleDB's `time_weight` where they agree:

- Consecutive readings of a series bound a segment `[t_i, t_i+1)`. STEP
  (last observation carried forward) holds `v_i` over it; LINEAR runs a
  straight line from `v_i` to `v_i+1` (trapezoids).
- A null holds nothing: under STEP it ends the previous value; under LINEAR a
  segment with a null at either end covers nothing. With `max_gap_ms`, a
  segment longer than that covers nothing (the value is stale, and LINEAR does
  not interpolate across an outage). The last reading holds nothing: it waits
  for the next one, never runs up to "now".
- Segments are cut exactly at bucket edges and at the range's start and end
  (LINEAR interpolates the value there). The value in force at a bucket's
  start comes from the last reading before it, however long ago (within
  `max_gap_ms`), so a bucket with no reading of its own still has an average.
  ClickHouse has no time-weighted average; the usual SQL (window functions and
  `avgWeighted`) loses exactly this carried-in value.
- A bucket reports the area (value x ms), the covered time and their
  quotient, plus the count of readings in it. Areas and covered times add, so
  buckets combine into longer averages. NaN propagates (the canonical quiet
  NaN); bool series give the fraction of time they were true (run time); state
  series are refused.

**The fast path.** Each granule's index entry carries what makes whole
granules combinable without decoding them: the first and last value (and
whether they are null), the STEP and LINEAR areas of the segments inside the
granule with the durations they cover (integer ms), and the longest gap
between its rows (`granule_twa` in `src/twa.c`, called by the part writer
with the other statistics for inserts, memtable flushes and merges that
re-encode; a merge that copies a granule's bytes copies its entry). A query lists the granules of every source
(parts and memtables) around the range in time order. A granule that lies
inside the range and inside one bucket, whose longest gap is within
`max_gap_ms`, and that no other source overlaps contributes its stored areas;
the segment joining it to its neighbour comes from its first and last value
and the neighbour's. Only granules that a bucket edge or the range cuts, and
time spans where sources overlap (corrections: last write wins, merged
exactly as the raw query does), are decoded, in parallel on the query pool.
`TSDB_TWA_DECODE=1` turns the fast path off for benchmarks
(`build/twa-bench`).

The statistics cost 56 bytes per granule entry (136 instead of 80 bytes per
8192 rows: under 0.01 bytes a row). They made the on-disk format version 4.

## Storage

A database is a directory:

```
META                       format version, partition size, granule size
SERIES                     append-only series dictionary (name -> dense id, value type)
STATES                     append-only dictionary of state names (name -> code)
LOCK
MANIFEST                   the published part set, for followers
wal_<seq>.log              write-ahead log of the current memtable
p<partition>_<minblock>_<maxblock>_<level>.part
```

A database with volumes (`tsdb_series_at`) also has `VOLUMES` (their paths)
and `CATALOG` (global series id -> volume and local id) in the root; each
volume is a directory of the layout above.

Each part is one immutable file holding the rows of every series written in
one insert (or produced by one merge), sorted by `(series, ts)`:

```
header | ts block, value block, ts block, value block, ... | granule index | footer
```

A granule is up to 8192 rows of one series. Its 136-byte index entry holds
the series, min/max timestamp, row count, null count, the sum, min and max of
the known values (doubles for floats; exact for integers: int64 min/max and a
128-bit sum), block offsets and a checksum of the granule's blocks, and the
time-weighted statistics (first and last value, STEP and LINEAR areas, the
time they cover and the longest gap between rows: see "Time-weighted
averages"). The
header, the index and the footer (WAL segment, row count, time range) are
covered by one more checksum, checked when the part is opened. The on-disk
format is version 4; a database of another version is refused at open
(there is no release yet, so no older format is read). The index is the sparse primary index of MergeTree: finding a series'
rows in a time range is a binary search, and aggregates over whole granules
are answered from the stored statistics without decompressing anything.

Parts are grouped by time partition (one day by default) and merged in the
background with ClickHouse's `SimpleMergeSelector` heuristics: only adjacent
block ranges are merged, so "newest part wins" stays well defined. A merge
copies granules that need no re-encoding as compressed bytes.

Writes are published atomically (`tmp_*.part`, fsync, rename, fsync dir).
An insert that makes several parts (one per partition) commits through an
intent file listing the renames, with a count and a checksum: at open a torn
intent, or one whose parts are not all there, rolls the insert back whole;
a complete one is rolled forward. On open, leftovers of interrupted writes are
removed and parts covered by a merged part are deleted.

### Write paths

- **Large batches** (`wal_max_batch_rows`, default 65536 rows, and more)
  become parts directly, one per partition, like a MergeTree INSERT.
- **Small batches and single points** go to a write-ahead log and an
  in-memory memtable that queries read immediately. With `wal_sync_ms = 0`
  (the default) a write returns after `fdatasync`, and concurrent writers share
  one fsync (group commit). With `wal_sync_ms = 100` writes return at once and
  the WAL is synced every 100 ms. The memtable becomes parts when it reaches
  `buffer_rows` or `flush_interval_ms`.

Each part records the WAL segment it contains, so recovery replays a segment
only into partitions that do not already hold its rows, even after a crash in
the middle of a flush.

### Storage errors

A failed `fsync` is not something to retry: Linux may have dropped the dirty
pages and marked them clean, so the next `fsync` succeeds while the data is
gone (PostgreSQL's "fsyncgate", 2018). tsdb therefore stops. When a write or
sync fails on a file that an acknowledgement may rely on (a WAL segment, the
directory after new names became visible, MANIFEST, the series and state
dictionaries, CATALOG), the engine is marked failed: the writers whose rows
went through the failed call get `TSDB_ERR_IO`, no later write is acknowledged,
no new file is named (a merge that finishes afterwards deletes its output),
and `tsdb_failed` says what failed. tsdbd exits with status 1, and a restart
recovers from what is on disk; replay stops at a torn WAL record at the end
of the segment, and nothing was written after it. Errors on files nothing relies on yet (a new part before
it is committed, a merge output before it is named) fail only that insert,
flush or merge; a failed flush keeps its memtable and retries it first. The
directory fsync after naming a merged part is fail-stop, because the sources
would be retired next. The same holds across threads: all directory fsyncs
use one file descriptor, and the kernel reports a writeback error to one of
them, so a concurrent or later `fsync` of the directory can return 0 without
the entries of the failed one. They take turns, and once the engine has failed
none of them succeeds, so a commit that reached its fsync after another
thread's failure is not acknowledged. A full disk (ENOSPC) on the WAL, MANIFEST or a
dictionary is fail-stop as well, as in PostgreSQL: under systemd a full disk
means restarts that serve reads in between, until space is freed. The failure
message goes to stderr once, possibly while the caller holds engine locks;
it is one short line per engine lifetime. With several volumes,
a failure stops only the volume it happened in (the root volume for CATALOG).

### Damage on disk

A crash can tear what was being appended; storage can also return damaged
bytes or fail a read. tsdb tells the two apart where it can and never turns
damage into silently missing or shifted data:

- **WAL records** carry a checksum of their head (seeded with the segment and
  offset) and one of their rows. A bad record at the end of a segment is a
  torn tail and is cut. A bad record followed by a valid one is damage to
  acknowledged rows: it is logged and the open fails with `TSDB_ERR_CORRUPT`,
  leaving the segment in place (removing it accepts the loss). An I/O error
  while reading a segment fails the open; it is never taken for its end.
- **SERIES and STATES** records carry a checksum seeded with their offset, so
  a run of zeros or a shifted record is never read as a name. A torn tail is
  cut; damage before a valid record fails the open.
- **CATALOG** records (16 bytes) and **VOLUMES** lines carry a checksum that
  includes their position. A torn tail is cut; damage before a valid record,
  or a record naming a series its volume does not have, fails the open.
- **Parts**: a part whose header, index or footer fails its checksum fails
  the open with `TSDB_ERR_CORRUPT` and a log line naming it; renaming it to
  `*.broken` accepts the loss of its rows. Dropping it silently would answer
  queries without its rows. A follower keeps its previous view, counts the
  error in `tsdb_stats.follow_errors` / `broken_parts` and logs it once.
- **Granules**: with `verify_checksums` (`tsdbd --verify-checksums`) queries
  check each granule's checksum before decoding it and return
  `TSDB_ERR_CORRUPT` on a mismatch. It is off by default: production storage
  (ZFS, ECC memory) has checksums of its own, and hashing every granule read
  costs query time. Without it a damaged granule is decoded as it is: the
  decoders reject what does not parse and are safe on any input, but most
  damage decodes to wrong values that queries return without an error. Merges
  check every granule they decode, so damage is never rewritten under a fresh
  checksum; a granule a merge passes through unchanged is copied with its
  checksum, so the damage stays detectable.
- **Without fsync** (`fsync = 0`) nothing above is synced, so after a power
  loss (a process crash is fine) a named part, a WAL record or a dictionary
  record can hold pages that never reached the disk. That looks like damage,
  and is treated as such: the open fails and names the file, and an operator
  accepts the loss (renames the part to `*.broken`, moves the segment away).
- **Followers** reading over NFS may see appends out of order: a bad WAL
  record or MANIFEST is tolerated for a while (2 s or 20 refresh intervals,
  whichever is longer) and then reported as `TSDB_ERR_CORRUPT`, once; an
  I/O error is reported at once.

### Compression

Every granule picks its own encodings, with ZSTD (level 1) on top whenever it
saves a few percent:

**Timestamps**: DoubleDelta (ClickHouse's bit format) for regular clocks,
about 0.03 bits per row. When the clock jitters, DoubleDelta doubles the
noise, so the granule instead stores the **residual from a regular grid**
(`ts[i] - (ts0 + i * step)`) or **frame-of-reference deltas**, as 1 to 8 byte
words, whichever is estimated smaller.

**float32 values** (`value_codec = TSDB_VALUES_AUTO`) are where most of the
bytes go, and most of the ideas for them come from ALP (Afroozeh, Kuffo,
Boncz, "ALP: Adaptive Lossless floating-Point Compression", SIGMOD 2024).
`TSDB_VALUES_GORILLA` or `TSDB_VALUES_CHIMP` force one XOR codec instead.

#### ALP, and why it fits sensor data

ALP starts from the observation that most floating-point data began as a
decimal with a few digits: it multiplies each value by powers of ten, stores
the integers with frame of reference and bit-packing per vector of 1024
values, and keeps the few values that do not convert back exactly as
exceptions, choosing the exponents by sampling. Values that are not decimals
("real doubles") go to its fallback, ALP_rd, which splits each value's bits
and dictionary-encodes the front part.

Sensor data is the case ALP is built for. A sensor reports with a fixed
resolution (0.1 degC, 0.001 bar, 0.01 m3/h) or as ADC counts times a
constant, so its readings are integers times a scale. They also change
slowly from one reading to the next, which ALP does not use and tsdb does.

What tsdb takes from ALP, codec by codec:

| codec | on disk | from ALP | ours |
|---|---|---|---|
| fixed point, `decimal_bp` | kind 10, AUTO's first choice | choosing the scale on samples; exceptions; bit-packing per vector of 1024 | binary scales; zigzag deltas instead of frame of reference; round trip in double |
| ALP_rd | kind 9, an AUTO candidate | the codec, in float32 | a dictionary sized by the data |
| ALP | never written | the codec, in float32 as DuckDB does | (implemented and measured, not chosen; see below) |
| float64 fixed point | kind 11 | as `decimal_bp` | two ways to rebuild a value |
| `decimal` | kind 3, still read | sampling; exceptions | 1/2/4-byte deltas left to ZSTD; AUTO no longer writes it |

#### Fixed point: `decimal_bp` (src/codec.c)

A granule is fixed point with scale `s` if every value is
`(float)((double)k * s)` for an integer `k`. There are 35 scales: 10^0 to
10^-10, and 2^-1 to 2^-24. The binary ones are ours; they catch ADC counts
(the vibration data set is counts times 2^-13), for which ALP has no exponent.

**Choosing the scale** (`dec_sample_cost`) uses ALP's idea of sampling, on
four runs of 16 consecutive values spread over the granule. A scale costs the
byte width of the run's zigzag deltas plus 8 times its fraction of exceptions,
and is dropped if more than a quarter of the sampled values are exceptions
(checked after the first run and again at the end). The search stops at the
first scale that costs 1.0 or less, and rejects the granule if the best costs
4.5 or more. The runs are consecutive values, not ALP's spread-out sample,
because what is measured is the step between neighbours.

**Checking every value** takes two loops. The first, without branches,
computes `k = rint((double)v * scale)` (0 when out of range) and whether
`(float)((double)k * inv)` gives back the same 32 bits; the compiler
vectorizes it. The second builds the deltas and the exceptions. A value that
does not round-trip (NaN, -0.0, a glitch, beyond 2^52) is an exception, stored
as position and raw bits (`u32, u32`); its `k` is set to the previous one, so
its delta is zero and does not widen the vector. More than n/16 exceptions, or
a zigzag delta that needs more than 32 bits, rejects the granule. The inverses
`inv` are literal constants: a table filled at startup would run AVX code
before the CPU check (below).

```
u8 scale | u8 0 | i64 first | u32 nexc | per vector of 1024: u8 w, deltas at w bits | nexc x (u32 pos, u32 bits)
```

That is one byte of header per 1024 values, where our ALP vectors spend
twelve (see below).

**A worked example**: three readings of a disk temperature with 0.1 degC
resolution.

| reading (float32) | `k` at 10^-1 | delta | zigzag | ALP with (e,f) = (1,0), in float32 |
|---|---:|---:|---:|---|
| 41.5 | 415 | (header) | | 415 x 0.1f = 41.5: good |
| 41.6 | 416 | +1 | 2 | 416 x 0.1f = 41.6000023, not 41.5999985: exception |
| 41.7 | 417 | +1 | 2 | 417 x 0.1f = 41.7: good |

`decimal_bp` stores 415 in the header and packs the deltas at 2 bits each. The
decoder rebuilds 416 as `(float)(416.0 * 0.1)`: the double product is
41.600000000000001, and its one rounding to float lands on 41.5999985, the
stored value. In float32, `0.1f` is already rounded and the product is rounded
again, which misses 41.6 (and 42.1) by one ulp. ALP's way out is to search
pairs (e,f) until one happens to round right: for this sensor (6,5) does,
giving 415, 416, 417, frame-of-reference base 415, and also 2 bits for these
three. Over a whole granule, though, a slow signal covers a wider range than
its steps: on `hdd_temp` ALP's vectors need 6 bits, the deltas 2.

Measured on one granule of 8192 values: in float32, ALP's (1,0) fails on 1895
values of `hdd_temp` and (6,5) on none; on `tank_bar` the best pair, (6,3),
still fails on 229, and on `flow_rate` (6,4) fails on 342. (Only pairs at the
data's own scale, e - f = 1, 3 and 2, were counted here; ALP's encoder chose
the same pairs on these data sets, see the table below.) The round trip in
double fails on none of the three, with a single scale.

#### ALP and ALP_rd in float32 (src/codec_alp.c)

**ALP** follows the paper, with float32 arithmetic as in DuckDB:

- A vector is 1024 values; a granule of 8192 values is 8 vectors.
- `enc = lrintf(v * 10^e * 10^-f)` is good if `(float)enc * 10^f * 10^-e`
  gives back the same 32 bits, so NaN and -0.0 are exceptions. The pairs are
  0 <= f <= e <= 10, 66 in all, and |enc| < 2^23, so the integers are exact in
  a float and fit an `int32`.
- Exponents are chosen in two levels, as in the paper. Per granule: 256
  values at a fixed stride, each pair costed as the sample's frame-of-reference
  width plus 48 bits per exception; the best five pairs are kept. If even the
  best costs 32 bits a value or more, ALP declines and the granule is left to
  ALP_rd. Per vector: each kept pair on 32 equally spaced values, the cheapest
  wins (all five are tried; there is no early exit).
- Frame of reference: the base is the vector's smallest good integer, and
  `enc - base` is packed at the width of the range. Exception slots get the
  vector's first good integer, so they never widen it.
- Layout: `u32 nvectors`, then per vector a 12-byte header
  (`u8 e, u8 f, u8 w, u8 0, i32 base, u16 nexc, u16 0`), the packed integers,
  `nexc x u16` positions and `nexc x u32` raw bits. The headers cost about 0.1
  bits a value (12 bytes per 1024 values, plus 4 per granule).

Float32 arithmetic makes the encoder's check exactly what the decoder
computes, keeps the integers in `int32` and the decoder in float lanes. The
price is the rounding shown above: which pair round-trips depends on how
float32 products round, and exceptions remain where a double round trip has
none. ALP also only knows powers of ten (it declines every `vibration`
granule) and integers below 2^23 (`flow_total`, about 12000.xxx m3 at 10^-3,
exceeds it: ALP accepts 9 granules of 128, with about 390 exceptions per
vector).

**Why AUTO does not use ALP.** Per vector of 1024, from the encoders' choices
on 128 granules of each data set:

| data set | ALP (e,f) | ALP width | ALP exceptions | `decimal_bp` scale | width | exceptions |
|---|---|---:|---:|---|---:|---:|
| hdd_temp | (6,5) | 6.00 | 0 | 10^-1 | 2.00 | 0 |
| cpu_temp | (6,5) | 5.35 | 0 | 10^-1 | 4.27 | 0 |
| tank_bar | (6,3) mostly | 6.79 | 37.1 | 10^-3 | 3.00 | 0 |
| flow_rate | (6,4) | 10.93 | 40.0 | 10^-2 | 10.93 | 0 |
| flow_total | 9 granules of 128 | 13 | 390 | 10^-3 | 8.00 | 0 |
| vibration | declined | | | 2^-13 | 14.00 | 0.3 per granule |

Three things decide it. Deltas are narrower than a frame-of-reference range
for slowly moving signals (2 bits against 6 on `hdd_temp`, 3 against 6.8 on
`tank_bar`). The double round trip has no exceptions where float32 ALP has 3
to 4% (`tank_bar`, `flow_rate`): on `flow_rate` the widths are equal, and 40
exceptions of 48 bits per 1024 values, about 1.9 bits a value, are nearly the
whole gap between 12.90 and 10.95 bits (the vector headers are most of the
rest). And ALP cannot take `flow_total` or `vibration` at all. ALP is smaller
than `decimal_bp` on none of the sensor data sets, with or without ZSTD. It
stays in the code as a reference in `build/codec-bench`: with ZSTD it decodes
the fastest of the value codecs (codec alone, only raw, byte-stream split and
bit delta are faster; it has no running sum), and codec alone it encodes the
slowest (the 66-pair search; with ZSTD, Patas and ALP_rd are slower).

**ALP_rd** (kind 9) is for floats that are not decimals:

- Each value is split into the low `r` bits, packed as they are, and the high
  `32 - r` bits, which repeat and are dictionary encoded. `r` runs from 16 to
  31, so the high part keeps 1 to 16 bits and fits a `u16`.
- `r` is chosen on every (n/256)-th value: for each `r`, count the distinct
  high parts (up to 512), take the 8 most frequent as the dictionary, and
  estimate `r + b` bits a value plus 32 for each value the dictionary misses.
  The cheapest `r` wins.
- The dictionary has up to 8 entries and the index `b = bits_for(nd - 1)`, 0 to
  3 bits: it follows the number of entries found rather than being weighed
  against exceptions.
- A high part missing from the dictionary is an exception, `u16 pos, u16 high`,
  with index 0 in its slot; 16-bit positions suffice because a granule has at
  most 65536 rows.
- Layout: a 24-byte header (`u8 r, u8 b, u8 nd, u8 0, u16 dict[8], u32 nexc`),
  the packed indexes, the packed low parts, the exceptions. There are no
  vectors: the granule is one stream.

In the paper, as we read it, the choice between ALP and ALP_rd is made per row
group, from the sample (not checked against the paper's text). In tsdb they
are separate codecs and AUTO's comparison decides, with ALP not among the
candidates. `build/codec-bench` shows what the other way costs: its `alp|rd`
column takes ALP whenever ALP accepts the granule, and on `flow_total` that is
19.02 bits a value against 18.02 for ALP_rd alone (codec alone, without ZSTD).
ALP_rd is what AUTO picks for every granule of uniform random floats (27.29
bits with ZSTD, against 32.90 for Chimp); on the sensor data sets fixed point
wins, except the unrounded psi conversion, where Chimp takes 121 granules of
128.

#### How AUTO picks (src/codec.c, `block_encode_f32`)

1. **Fixed point first.** If `decimal_bp` fits in 16 bits a value or less, it
   is kept without comparing anything else.
2. **Otherwise compare.** Chimp, bit delta (deltas of the bit patterns),
   byte-stream split, ALP_rd and `decimal_bp` (at any width) are each encoded.
   Each is estimated as the smaller of its length and the order-0 entropy of
   its bytes on a 1-in-3 sample (byte-stream split per byte plane). The
   entropy uses a portable `log2`, so every CPU makes the same choice. The
   three most promising are compressed for real, with ZSTD, and the smallest
   is kept; without ZSTD (level 0) the shortest wins.
3. **ZSTD(1)** wraps a block of at least 64 bytes, and stays only if it saves
   4 bytes plus 1/32.

What AUTO picked, on 128 granules of each data set:

| data set | AUTO's codec | kept ZSTD |
|---|---|---:|
| hdd_temp, cpu_temp, tank_bar | `decimal_bp` | 128 |
| tank_psi (converted, unrounded) | Chimp 121, `decimal_bp` 7 | 1 |
| vibration | `decimal_bp` at 2^-13 | 0 |
| flow_rate | `decimal_bp` | 14 |
| flow_total | `decimal_bp` at 10^-3 | 128 |
| random | ALP_rd | 0 |

#### Numbers

`build/codec-bench` compares the XOR codecs (Gorilla, Chimp, Chimp128,
Patas), ALP, ALP_rd, the fixed-point codecs and AUTO on each sensor type, with
and without ZSTD. Bits per value with ZSTD(1); sizes are exact:

| sensor (bits per value, codec + ZSTD) | Gorilla | Chimp | Chimp128 | Patas | ALP | ALP_rd | tsdb AUTO |
|---|---:|---:|---:|---:|---:|---:|---:|
| SMART disk temperature, 0.1 degC, 60 s | 2.66 | 2.33 | 5.93 | 3.39 | 2.36 | 3.11 | **0.88** |
| CPU temperature, spikes to 100 degC, 1 s | 10.82 | 11.34 | 11.86 | 10.75 | 4.97 | 8.47 | **2.99** |
| tank pressure, 0.001 bar, 10 s | 9.09 | 8.85 | 8.79 | 7.45 | 5.97 | 9.81 | **1.82** |
| tank pressure in psi (converted, unrounded) | 23.77 | 19.57 | 19.62 | 27.24 | n/a | 21.55 | **19.50** |
| vibration, 16-bit ADC at 1 kHz | 21.51 | 23.58 | 30.78 | 27.87 | n/a | 18.52 | **14.02** |
| pipeline flow rate, 0.01 m3/h | 23.08 | 20.85 | 19.94 | 17.76 | 12.29 | 18.76 | **10.72** |
| pipeline flow totalizer, m3 | 8.68 | 6.07 | 6.10 | 8.40 | n/a | 16.05 | **1.68** |
| uniform random floats | 32.32 | 32.90 | 32.90 | 39.91 | n/a | 27.30 | **27.29** |

n/a: ALP declines at least one granule of the data set (no binary scales,
integers below 2^23 only, or mostly exceptions), and the bench then reports no
size; on `flow_total` it accepts 9 granules of 128. The bench puts ZSTD on
every granule of a single codec, AUTO only where it saves 4 bytes plus 1/32:
on `flow_rate` that is 14 granules of 128, hence 10.72 for AUTO against 10.56
for `decimal_bp` with ZSTD everywhere. The plain `decimal` codec (1.66 on the
totalizer) is no longer written.

Without ZSTD the picture is the same: `decimal_bp` takes 2.02, 4.29, 3.02,
14.02 and 10.95 bits on hdd_temp, cpu_temp, tank_bar, vibration and
flow_rate, ALP 6.10, 5.44, 8.63, n/a and 12.90.

Speed in million values a second, one thread, median over the data sets.
These are indicative only (measured on a busy machine):

| | ALP | ALP_rd | `decimal` | `decimal_bp` | AUTO | Chimp |
|---|---:|---:|---:|---:|---:|---:|
| decode, codec alone | 2336 | 938 | 1911 | 1097 | 1077 | 331 |
| encode, codec alone | 197 | 241 | 977 | 698 | 706 | 315 |
| decode, with ZSTD | 1148 | 395 | 591 | 776 | 798 | 257 |
| encode, with ZSTD | 141 | 120 | 280 | 379 | 380 | 158 |

#### Decoding and CPU levels

| codec | decoding |
|---|---|
| ALP | per vector: unpack, `(float)(int32)(u + base) * 10^f * 10^-e`, patch the exceptions; no value depends on another |
| ALP_rd | unpack indexes and low parts, OR in `dict[idx & 7] << r` without a branch, patch the exceptions |
| `decimal_bp` | per vector: unpack, running sum `k += unzigzag(z)`, `(float)((double)k * inv)`; exceptions patched at the end |
| `decimal` | after ZSTD, a running sum over 1-, 2- or 4-byte deltas |
| float64 fixed point | the integer codec straight into the output buffer, then converted to doubles in place |

Unpacking is one function per bit width, with the width a compile-time
constant and one unaligned 8-byte load per value (src/bitpack.h). The running
sum is the serial step of `decimal_bp`, which is why ALP, with the same
unpacking and no sum, decodes twice as fast (codec alone; 1.5 times with
ZSTD).

A portable build (`make dist`, baseline x86-64-v2) compiles the codec files
(codec.c, codec_alp.c, codec_typed.c, codec_xor.c, values.c) three times, for
x86-64-v2, v3 and v4 (src/levels.h), and src/cpu.c picks one set at startup;
`TSDB_CPU=v2|v3|v4` lowers the choice. A default `make` (`-march=native`)
compiles them once. Every level must write identical blocks, since parts
written on one CPU are read on another. That needs `-ffp-contract=off`
everywhere: on a CPU with FMA, a contracted `a*b+c` rounds differently, and
ALP's float32 check or the fixed-point round trip would accept different
values. It also needs AUTO's entropy estimate to use no libm function that
glibc picks by CPU. `tests/test_levels.c` compares every level the CPU has.

#### What we did not take from ALP, and why

- **Frame of reference.** Deltas between neighbours are narrower than a
  vector's range for slowly moving signals, the common sensor case.
- **Float32 arithmetic for fixed point.** The double round trip has no
  exceptions where float32 has 3 to 4% (`tank_bar`, `flow_rate`), and it
  reaches integers up to 2^52, not 2^23.
- **The pair (e,f).** With the double rebuild one scale suffices; there is no
  rounding to dodge with a second exponent.
- **The FastLanes layout.** We pack sequentially, low bits first. Unpacking is
  not what limits `decimal_bp` (the running sum is), so the interleaved layout
  would not pay for itself here.
- **Sampling per row group** (the paper's unit, as we read it). Each granule
  picks its own encoding, and a merge copies granules that need no re-encoding
  as compressed bytes, so the sample is taken from the granule alone.

#### Other value types

**float64 values** use the same fixed-point idea at double precision
(src/codec_typed.c, kind 11). The scales are 10^0 to 10^-15 and 2^-1 to 2^-30,
and a value can be rebuilt two ways, a choice that has no counterpart in ALP:
`k / 10^e`, correctly rounded, matches values parsed from decimal text, and
`k * 10^-e` matches values computed as count times resolution. A sample of
about 512 values settles which (a flag bit in the scale byte). The scale is
chosen on the same four runs of 16, costed as the delta width in bits plus 96
times the fraction of exceptions; a scale with more than 1/8 exceptions in
the sample is dropped, and a best cost of 48 bits or more rejects the granule.
Exceptions (at most n/16) are `u32 pos, u64 bits`. The integers go through the
int64 codec below, zigzag deltas packed per vector of 1024 (raw 64-bit zigzags
for a vector with a delta wider than 32 bits):

```
u8 scale|flag | u32 nexc | nexc x (u32 pos, u64 bits) | int64 block
```

AUTO keeps fixed point outright at 16 bits a value or less; otherwise it
compresses fixed point, Gorilla's XOR encoding of the 64-bit patterns and
byte-stream split for real and keeps the smallest. With ZSTD, from
`build/type-bench`: hdd_temp 0.88, cpu_temp 3.10, tank_bar 1.82, vibration
14.03, flow_rate 10.80 and flow_total 1.56 bits a value, all fixed point;
tank_psi 48.87 and random 59.10, byte-stream split.

**int64, bool and state codes**: zigzag deltas bit-packed per vector of 1024
values. Counters, codes that rarely change and flags come down to a few bits a
value and constant runs to none. `build/type-bench` measures these: a
totalizer in liters 1.18 bits a value, a sequence 0.04, a door contact
sampled at 1 Hz 0.06, a machine mode 0.13.

### SIMD

Gorilla, Chimp and DoubleDelta decoding is bit-serial: where value i+1 starts
depends on value i, so it cannot be vectorized. The aggregation kernels are
dispatched at runtime to AVX-512, AVX2 or scalar code, and the fixed-point
encoder is written so the compiler vectorizes it. AVX-512 is not used on
Skylake-SP, Cascade Lake and Cooper Lake Xeons, which lower the clock of the
core after 512-bit instructions. `TSDB_SIMD=avx512|avx2|scalar` overrides the
choice. In a portable build the codecs are compiled per CPU level instead (see
"Decoding and CPU levels" above).

## Server and clients

`tsdbd` serves a database over TCP with a small binary protocol
([`include/tsdb_protocol.h`](include/tsdb_protocol.h)): length-prefixed frames,
columnar arrays, results of raw queries streamed in chunks. One thread runs an
io_uring event loop (multishot accept, recv/send, an eventfd from the workers);
requests run on a worker pool, in order per connection, so a client can
pipeline inserts and still get "last write wins". A client that reads slowly
pauses its query instead of making the server buffer its result.

```sh
build/tsdbd --dir /data/sensors --listen 127.0.0.1 --port 7070 --wal-sync-ms 10
```

Connections: up to `--max-connections` at once (default 10,000; the descriptor
limit is raised to the hard limit at start). An idle connection costs one file
descriptor and about 16 KB; buffers grow for large frames and shrink back.
Requests run on `--workers` threads (default: one per CPU) and the rest wait
in their connection's queue. Measured with `tsdbc-bench connections`: 10,000
connections open at once, all querying, 85,000 queries/s in total, no
failures; the server's resident memory went from 148 MB at 1,000 connections
to 231 MB at 10,000.

There is no authentication or TLS: listen on loopback or a trusted network.

The client library (`build/libtsdbclient.a`, [`client/tsdb_client.h`](client/tsdb_client.h))
mirrors the embedded API and does not link the storage engine:

```c
tsdbc *c;
tsdbc_connect("127.0.0.1", 7070, &c);
uint32_t id;
tsdbc_series(c, "pump7.flow", 1, &id);
tsdbc_insert_series_async(c, id, ts, values, n);   /* pipelined */
tsdbc_wait(c);                                     /* acknowledgements */
tsdbc_query_agg(c, id, start, end, 60000, &b, &nb);
tsdbc_close(c);                                    /* collects outstanding acknowledgements first */
```

Typed series, nulls and states use the protocol's version 2 operations
(`tsdbc_series_create`, `tsdbc_insert_values`, `tsdbc_query_values`,
`tsdbc_state`); the version 1 operations are unchanged, so older clients keep
working with float32 series.

Requests the server has received run even if the client disconnects before
reading the replies; a client that half-closes its socket still gets every reply.

Clients in other languages, each with a quick start in its README:

| | directory | build and test |
|---|---|---|
| C | [`client/`](client) | `make`, `make test` |
| Rust (std only, blocking) | [`client/rust`](client/rust) | `make test-rust` |
| Python (PyO3 over the Rust client, numpy optional) | [`client/python`](client/python) | `make test-python` |
| Java 17+ (Maven, no runtime dependencies) | [`client/java`](client/java) | `make test-java` |

## Command line

```sh
build/tsdb /data/sensors create pump7.runtime_s --type int64   # float32 unless --type
build/tsdb /data/sensors create pump7.mode --type state
build/tsdb /data/sensors import < rows.csv          # series,time,value (null or empty: null)
build/tsdb /data/sensors insert pump7.flow < p.csv  # time,value
build/tsdb /data/sensors query pump7.flow 2024-01-01 2024-01-02 avg 5m
build/tsdb /data/sensors query pump7.flow now-1h now raw
build/tsdb /data/sensors query pump7.temp 2024-01-01 2024-02-01 twa 1d --tz Europe/Oslo --max-gap 10m
build/tsdb /data/sensors stats
build/tsdb /data/sensors parts
build/tsdb /data/sensors optimize
build/tsdb /data/sensors drop-before 2023-01-01
```

## One writer, many readers (also over NFS)

One process writes; any number of processes follow it, on the same host or
on other hosts that mount the directory over NFS (NFS over RDMA works well):

```c
tsdb_options o;
tsdb_options_init(&o);
o.follow = 1;            /* read only, and keep up with the writer */
o.refresh_ms = 100;      /* how often to look */
tsdb_open("/mnt/sensors", &o, &db);
```

```sh
build/tsdbd --dir /data/sensors --port 7070                    # the writer
build/tsdbd --dir /mnt/sensors  --port 7070 --follow            # query servers, anywhere
```

The writer keeps a `MANIFEST`: the active parts, the series count, and which
WAL segments are already in parts. Followers poll it, open new parts, drop
replaced ones, read new series names, and tail the WAL segments that are not
in parts yet, so rows acknowledged by the writer are visible to followers
within about `refresh_ms` (plus `wal_sync_ms` if the writer syncs
periodically). A segment's rows leave a follower's memory in the same step as
the parts that hold them arrive, so queries never see a row twice or miss one.

What makes this safe on NFS:

- `MANIFEST` is rewritten in place with a checksum rather than renamed: NFS
  revalidates a file when it is reopened (close-to-open consistency), but a
  name looked up again can come from the directory cache. A torn read fails
  the checksum and is retried at the next poll.
- A file deleted on the NFS server is gone for every client, even one that has
  it mapped (the next page fault raises SIGBUS). Parts replaced by a merge are
  therefore renamed to `retired_*` at once and deleted only
  `obsolete_part_lifetime_s` later (480 s by default, ClickHouse's
  `old_parts_lifetime`). Renaming does not disturb readers that have the file
  open, and a follower that is a step behind finds a part under its retired name.
- Mount followers with `lookupcache=positive`, so a part or WAL segment created
  a moment ago is not hidden behind a cached "no such file".
- The writer must sync the WAL (`fsync = 1`, the default) for its rows to
  reach the server; NFS clients only write back on fsync or close.

Only one writer may exist (it takes a `flock` on `LOCK`, which NFSv4 honours).
`tests/test_follow.c` runs a writer and a follower as separate processes on one
directory while the writer appends, overwrites, merges and retires parts.
It has not been run against an actual NFS server in this repository's tests.

