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
- A query reads one **snapshot**: what was committed when it started, nothing
  written after. A query over many series in one call (`tsdb_query_multi`,
  QUERY_MULTI) reads all of them from one snapshot of each volume, so an
  insert that wrote several series in one call is seen by all of them or by
  none. Separate queries, one per series, each take their own.
- A time range of one series can be deleted (`tsdb_delete_range`) or replaced
  atomically (`tsdb_replace_range`). A delete is a write: a value written
  after it returned is kept, one written before it is gone. A series can have
  a retention limit (`tsdb_set_retention`): older values disappear from
  queries at once and from disk in the background (see Storage).
- Aggregates: count, sum, min, max, avg, and the spread: range (max - min),
  population and sample variance and standard deviation (`var_pop`,
  `var_samp`, `stddev_pop`, `stddev_samp`). Integer types (int64, bool as 0/1)
  aggregate exactly (`isum`, `imin`, `imax`; the sum saturates at the int64
  limits, `sum` holds it as a double). For states only the count is meaningful.
  See "Spread" below for how variances stay exact.
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

## Spread: range, variance, standard deviation

Whole granules answer the spread from their statistics like they answer sums,
so the statistics must combine without losing precision. A raw sum of squares
does not: pressure in Pa around 101325 with a spread of +-5 has squares near
1e10 whose difference from n * mean^2 is the variance, and in a double that
difference cancels to noise (or to a negative variance). So float series keep
the stable form: per granule, besides count and sum, m2, the sum of squared
deviations from the granule's mean. A run of decoded values gets its m2 in two
passes, the mean first and then the squared deviations from it, corrected by
their sum (the corrected two-pass algorithm); runs, granules, memtables and
threads then combine with the parallel formula of Chan, Golub and LeVeque:
m2 = m2a + m2b + d^2 * na * nb / (na + nb), d the difference of the means.
Constant data gives exactly 0, and an offset costs no precision.

int64 and bool keep exact sums instead: of x (128 bits, as for `isum`) and of
x^2 in 192 bits, enough for 2^64 rows of int64 squares. The variance is
computed once, at the end: n * S2 - S1^2 exactly in 256 bits, then
converted to long double and divided by n^2 (or n (n - 1)) and rounded to a
double. Nothing cancels, the error is a few roundings in the last bits of a
long double (so close to, but not always, the correctly rounded double; on
aarch64 long double is binary128 and the last bits can differ from x86-64),
and the result depends only on the multiset of values, not on
their order. m2 would not have done: the mean of integers is not an integer,
and m2 of integers is a float with the reproducibility of float sums.

What is bit-reproducible:

- int64 and bool: every spread value, in every order, path and CPU.
- Floats: the m2 of one run of rows is the same on every CPU level and kernel
  (`TSDB_CPU`, `TSDB_SIMD`): all five kernel sets (scalar, SSE4.2, AVX,
  AVX2, AVX-512) keep 32 lanes (float32) or 16 (float64) and halve them in
  the same order, as for
  sums (test_spread and test_levels check all of them bit for bit, on varied
  runs, with and without nulls, and on whole databases). How runs and granules combine follows how parts were
  merged and the query split over threads, so like float sums the variances
  may differ in the last bits after a merge.
- range comes from min and max and is exact (for int64 the difference of two
  exact integers, rounded once to a double; the CLI prints it as an exact
  unsigned integer).

NaN and infinities: min and max skip NaN, so the range does too; a NaN or an
infinity among the values makes the variances and deviations NaN (the
deviation of an infinity from the mean is undefined), the canonical NaN like
every NaN result. So do finite float values whose sum overflows a double
(two values of 1e308): the mean is then infinite. A bucket without known
values gives NaN for all of them, a single value gives 0 for the population
and NaN for the sample measures.

The spread is opt in, because its second pass over decoded values costs time
that count, sum, min, max and avg do not need: `tsdb_query_spread`,
`tsdb_query_spread_group` and `tsdb_query_spread_bounds` (and
`tsdb_query_spread_group_local` for calendar buckets) fill
`tsdb_spread_bucket`, a `tsdb_bucket` followed by the five spread values.
`tsdb_query_agg` and the other plain queries fold in one pass, skip the
squares, and return the same bits as before; `tsdb_bucket` keeps its 64
bytes, so bindings that mirror it are unchanged. Whole granules cost the same
either way: their statistics hold m2 already. The engine keeps m2 and the sums
of squares beside each bucket's state (`agg_dev`), not in it: a plain query
of 10-row buckets holds one state per bucket, and a state of 80 instead of 48
bytes made it 20-30% slower (`build/tsdb-bench`, buckets of 10 samples). With
the state at 48 bytes the plain queries run as fast as before the spread.

The three statistics cost 24 bytes per granule entry (160 instead of 136
bytes: m2 or the low 64 bits of the integer sum of squares, and its other 128
bits) and made the on-disk format version 5. Measured with `build/tsdb-bench`
(100 series of 1 M rows, partly filled granules): 1.202 to 1.208 bytes a row,
+0.5%; 0.003 bytes a row in full 8192-row granules.

Deletes, replaces and retention need nothing of their own: the spread is one
more statistic of the aggregate path. Granules a newer tombstone overlaps are
decoded, filtered and folded like memtable rows (the snapshot's entry windows
never hand them out as whole granules), and a query starts at the retention
cutoff, so a granule that straddles it is decoded too. Merges re-encode every
granule a tombstone or the cutoff touched through the part writer, which
computes m2 and the sums of squares of the rows that are left, so whole
granules answer from statistics again, exactly, once the rows are gone from
the files (`test_spread`'s `test_deletes` checks this against the reference,
with and without background merges, on followers and at every CPU level).

Over the protocol the spread is optional (version 8; 7 is range deletes):
QUERY_BUCKETS and QUERY_BOUNDS take a `fields` byte, and with `TSDB_SPREAD`
each 64-byte bucket row is followed by the five doubles. A request without
the byte gets the old rows and the server runs the plain, one-pass query, so
clients that do not know the spread read replies unchanged and pay nothing.
The C client's `tsdbc_query_agg`, `_group` and `_bounds` never send the byte;
`tsdbc_query_spread`, `_spread_group` and `_spread_bounds` send it, and
against a server older than version 8 fail with `TSDB_ERR_INVALID` without a
request.

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

## Order, limits and early stop

Raw queries in descending order read backwards window by window, so "the
newest N points" touches about N rows. Bucketed queries (aggregates, calendar
groups, bounds, the spread, TWA) with a limit stop early the same way
(`bucket_windows` in src/query.c). The query takes one snapshot of its whole
range; then it computes windows of buckets, each as if it were the query
range, from the start (ascending) or from the end (descending), until
`limit` non-empty buckets are in. A window spans max(still needed, twice the
previous) buckets, counted from the nearest data in the remaining range (the
granule index and the memtable answer that without decoding), so empty
stretches cost nothing and a sparse range takes a few windows. Without a
limit, or with one bucket, there is one pass and the result is reversed and
cut.

The result is that of computing every bucket and then cutting, bit for bit,
because a bucket is combined the same way either way:

- Window edges are bucket edges: a bucket lies in one window, and a granule
  that a window edge cuts is decoded, as a granule that spans buckets always
  is; a granule inside one bucket answers from its statistics in both.
- Overlap clusters (granules and memtable runs of several sources whose time
  ranges overlap, merged so replaced rows are not counted) are found once for
  the whole range; a window takes each cluster that reaches into it as it is,
  so a merge chain is never split differently by a window than by the full
  range. TWA finds the overlaps per window, which merges the same rows into
  the same stream of segments (a merge emits the newest row per timestamp,
  whatever range it starts from), and takes the value carried into a window
  from the same snapshot, which reaches around the whole range.
- A worker folds the buckets inside its granule directly; the first and the
  last bucket of each granule, which other granules may share, are folded
  after the pool in work order (they were folded in completion order). Float
  sums therefore depend only on how parts were merged, not on threads, order
  or limit.

Only the query statistics and `TSDB_ERR_TOO_MANY_BUCKETS` (which counts the
buckets computed) differ. On a year of one row a minute (one granule a day),
the last 10 hourly buckets cover 1 granule and 1440 rows instead of 366 and
527040 (about 0.02 ms instead of 0.25 to 0.5 ms, on a busy machine);
tests/test_limits.c asserts the counts and checks every form against
compute-all-then-cut over random ranges, orders and limits, with deletes,
retention, the memtable, a follower and merges.

`tsdb_query_agg` has no options; `tsdb_query_agg_ex` adds order and limit.
On the wire, QUERY_AGG (version 1) keeps its 32-byte rows without order or
limit: QUERY_BUCKETS with unit 0 is the same query with both and with the
exact int64 fields, so clients use it (`tsdbc_query_agg` already did) and no
second form of QUERY_AGG is needed. LTTB needs every point of the range to
choose, so its order and limit (`tsdb_query_lttb_ex`, QUERY_LTTB's optional
order and limit in version 9) only reverse and cut the chosen points:
ascending stops the callback at the limit. Descending, when the threshold
keeps every point (it is the client's to choose, up to 2^64), is a raw read
backwards that stops at the limit; otherwise the choice runs oldest first
and a ring keeps the last min(limit, threshold) chosen points, grown as they
come, so memory follows what is returned, never the rows of the range.

Costs a limit does not remove: the overlap clusters are found once from the
granule index of the whole range (sorting every granule entry, O(G log G)
for G granules, no decoding), and the statistics count a part (and, for TWA,
a memtable run's rows) once per window that reads it, so a query that took
several windows reports more parts and memtable rows than it holds.

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
`tsdb_twa_opts.no_stats` turns the fast path off for benchmarks
(`build/twa-bench`).

Outside the range only two readings matter: the latest before it and the
earliest after it. Of the granules outside the range a query keeps those that
hold them (several only where sources tie on that time, merged by last write
wins), and takes their first and last values from the index entry without
decoding. So a query without a staleness limit, whose snapshot spans the whole
history, costs the same for a series of ten years in daily partitions as for
one of a day: otherwise every part would add a decoded granule.

The fast path pays off for buckets much wider than a granule. A granule of
8192 on-change readings can span hours; hourly buckets then cut nearly every
granule and run at decode speed (about 1.4 G rows a second, one series of 1 G
rows), while daily buckets take 82% of the granules from statistics and run
five times faster (`build/twa-bench`).

A TWA bucket needs no reading of its own (a segment across it is enough), so
two readings far apart could ask for a bucket per millisecond between them. A
request gets at most 4M buckets (`TSDB_ERR_TOO_MANY_BUCKETS` beyond), which
fits one reply frame and bounds its memory to some 400 MB. Aggregates, groups,
bounds and the spread have the same limit, for the same reason: with 64M, one
37-byte request made tsdbd allocate gigabytes before it found that the reply
would not fit a frame.

The statistics cost 56 bytes per granule entry (136 instead of 80 bytes): 0.007
bytes a row in full 8192-row granules, and 0.013 bytes a row measured on
on-change data with partly filled granules (1.189 to 1.202 bytes a row, +1.1%).
They made the on-disk format version 4.

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
volume is a directory of the layout above, with `ROOT_ID` (the root's `ID`).

Volumes are added on purpose: `tsdb <dir> volume add <path>` (`tsdb_volume_add_dir`,
also while tsdbd has the database open) or `tsdb_volume_add`. tsdbd and the CLI
open the database with `create_volumes = 0`, so a series request naming a path
that is not a volume gets NOTFOUND and creates nothing: a client never chooses
where the server writes. The embedded library adds a volume for a new path by
default (the program chooses its paths), with the same checks. An add takes an
OFD lock on `VOLUMES`, so the CLI and a running writer append in turn, and
refuses (INVALID) a path that is not absolute, holds a control character (a
newline would split its line) or a `.` or `..` component, lies inside the root
or another volume or contains one, or is the directory of another database or
holds tsdb files. It makes the directory and syncs its parent, writes the
root's `META` and `ROOT_ID` into it and then appends the line, so a line never
names a directory a power loss can take back; the engine files come with the
first writer that opens it. The lock is held only to read the list and to
append, not while the new directory (perhaps on a slow NFS server) is made. A
running writer reads the lines added since when a request names a path it
does not know (VOLUMES grew), so `volume add` needs no restart; it resolves the
client's path (realpath, for a symlink to a volume) before it takes the lock
that serialises series creation. Whoever holds the write lock cuts a torn
last line (an add that died in its write), so one does not stop a running
writer until a restart.

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

### Deletes, replaces and retention (src/delete.c, src/tomb.c, src/retention.c)

**Tombstones live in parts.** `tsdb_delete_range(series, start, end)` writes
a range tombstone `{series, start, end}`, and only into parts: never into the
WAL or a memtable. A part's tombstones hide the rows of that series in
`[start, end)` in every part of the same partition with a *lower* block number;
they never hide the rows of their own part, nor memtable rows. That is the
existing last-write-wins order (a higher block is newer, memtables are newer
than every part), so a delete needs no ordering of its own:

- A delete runs like a large insert. It flushes the memtable first (rows
  acknowledged before it get older block numbers), then finds every listed
  part with rows of the series in the range and writes one small
  tombstone-only part per such partition, outside `publish_lock`, so writers
  and flushes do not wait for those fsyncs. Under `publish_lock` it looks
  again and writes one more for a partition that got rows of the series in
  between (rare), then commits them all through `parts_commit` (an intent file
  when there is more than one). It returns once they are fsync'd and
  published. Anything published later (a flush, a large
  insert) has a higher block number and is newer than the delete; a partition
  that held no rows of the series in the range needs no tombstone.
- A replace is the tombstone parts plus the new rows' parts in one
  `parts_commit`, tombstones first (lower blocks): one intent, so a reader or a
  crash sees the old rows or the new ones. Rows outside `[start, end)` are
  refused.
- The WAL, the memtable, WAL replay and the follower's tail are unchanged.
  Replay skips a partition by its parts' `wal_seq`; a tombstone part carries
  none, and a merge output that ends up with no rows is still kept as an
  (empty) part, so that the `wal_seq` anchor of the rows it replaced survives
  and a stale WAL segment cannot replay deleted rows back. Such an anchor is
  needed only while a segment up to its `wal_seq` may come back at open (an
  unlink not yet made durable): once every such segment is unlinked and the
  directory synced, the merge thread retires a partition left with nothing but
  anchors, so deleted or expired partitions do not stay behind for good.

**On disk.** A part with tombstones has an optional trailer between the index
and the footer: `n x {u32 series, u32 0, i64 start, i64 end}`, then `u32 n,
u32 'TOMB'`; `index_hash` covers it. Parts without tombstones are byte for byte
what they were, so `FORMAT_VERSION` stays; an older binary rejects a part with
a trailer at open (its size does not match) and never misreads one. **No
downgrade while tombstone parts exist**: the older binary calls such a part
broken and suggests renaming it to `.broken`, which would bring every deleted
row back. Run `tsdb_optimize` before going back: a merge of a whole partition
leaves no older part for tombstones to hide, so it drops them all. A part's
`min_ts`/`max_ts` cover its tombstones, and a part may hold 0 granules.

**Queries.** A snapshot that has no part with tombstones costs one test per
part and nothing more. Otherwise, for each part p, the tombstones that apply to
it are those of newer snapshot parts of its partition (their lowest block above
p's highest). Granules of p that intersect them are decoded, filtered and
treated as a memtable-like run with p's priority; p's other granules stay as
they are, and aggregates keep using their statistics. So granule statistics
are never used for a granule a tombstone touches, and once a merge has removed
the rows they are exact again with no special case.

The snapshot records, per part, the window of index entries the query may use
(`sn.gi`/`sn.gj`, found with `snap_find`): the granules of the series in
range, minus those moved to mem runs. Every reader that goes past its range
(TWA looking for the readings before and after it, LTTB's counting pass) must
stay inside that window. Looking a part up directly (`part_find` on
`sn.parts`) would see a tombstoned granule a second time, deleted rows
included: LTTB counted such granules twice and read garbage, TWA carried a
deleted reading into the range. Inside the window, the nearest granule before
or after the range is the nearest live reading, because tombstoned granules
are mem runs holding only their live rows. TWA's fast path therefore applies
unchanged to the untouched granules; a TWA query without a staleness limit
takes a snapshot of the whole history and so decodes every tombstoned granule
of the series until merges remove the deleted rows. Measured (30 days of one
float64 reading a second, optimized, then one one-row delete a day, merges
off; a one-hour TWA on day 29): without a staleness limit 245760 rows decoded
and about 1 ms, against 8192 rows and 0.11 ms with a 60 s limit or without the
deletes (rows exact, times on a loaded machine). A query that searched outward
from its range for the nearest live reading would decode only the nearest dirty
run on each side; it is not done, since merges bound the cost.

**Merges** drop the rows of each source hidden by the tombstones of a newer
source, and never copy a granule that intersects them. The output keeps the
union of the sources' tombstones (clipped by retention), unless no part of the
partition is older than the merge's lowest block: then nothing is left for
them to hide and they go.

**Retention.** `tsdb_set_retention(series, limit_ms)` appends a record
`[u32 check][u32 series][i64 limit_ms]` to the file `RETENTION` (fsync'd, last
record per series wins, a torn tail is cut like `SERIES`; an older binary
ignores the file), then republishes MANIFEST so followers reload it. Set it
right after creating a series to have it from the start. "now" is the wall
clock (`tsdb_options.clock_ms`, default `CLOCK_REALTIME`) but never goes back
within a process, and it starts no earlier than the newest part's mtime at
open: after the clock is stepped back nothing expired reappears; a clock
stepped forward expires rows early, and for good once a merge has removed
them. A series' cutoff is `now - limit`:

- queries clamp their start to the cutoff, so expired rows vanish at once.
  A query that reads before its start stops at the cutoff too: TWA carries no
  reading in from before it (a decoded granule straddling the cutoff starts no
  segment at an expired reading), and LTTB counts and buckets only rows from
  the cutoff on;
- every merge drops rows below the cutoff;
- the merge thread, at most once a second, drops a whole partition once every
  series in it (its rows and its tombstones) has expired (retired like
  `drop_before`, never unlinked at once), and rewrites alone a part that holds
  expired rows and was written more than `retention_merge_s` (default 3600)
  ago. Writes and queries never wait for it.

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
| ALP_rd | kind 9, an AUTO candidate | the codec, in float32 and float64 | a dictionary sized by the data |
| ALP | never written | the codec, in float32 as DuckDB does | (implemented and measured, not chosen; see below) |
| float64 fixed point | kind 11 | as `decimal_bp` | two ways to rebuild a value |
| packed bit delta | kind 15, an AUTO candidate | bit-packing per vector of 1024 | zigzag deltas of the raw bit patterns, float32 and float64 |
| dictionary | kind 16, a merge candidate | bit-packing per vector of 1024 | ranks among the distinct values, float32 and float64 |
| float32 in float64 | kind 17, tried after float64 fixed point | | a float32 block inside a float64 one, for widened float32 readings |
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
  high parts, take the 8 most frequent as the dictionary, and estimate `r + b`
  bits a value plus 32 for each value the dictionary misses. The cheapest `r`
  wins. The sample's top 16 bits are sorted once (a radix sort); every `r`'s
  high part is a shift of them, so equal ones stay adjacent and each `r` costs
  one pass. Counting them by linear search for each `r` took a third of the
  encode time of random doubles.
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
bits with ZSTD, against 32.90 for Chimp), in float32 and in float64 (56.29
against 59.10 for byte split); on the sensor data sets fixed point wins,
except the unrounded psi conversion, where the packed bit delta takes every
granule (below).

#### Packed bit delta (kind 15, src/codec.c and src/codec_typed.c)

A reading converted to other units and left unrounded (bar to psi, liters to
gallons) is no longer a decimal at any scale, so fixed point does not apply,
yet it still moves smoothly: neighbours share the sign, the exponent and the
top of the mantissa. The difference of their raw bit patterns, as integers, is
then narrow. The packed bit delta stores the first pattern and the zigzag of
each difference (modulo 2^32 or 2^64), bit-packed per vector of 1024 at the
vector's widest:

```
u32 first (u64 for float64) | per vector: u8 bits (0..32, or 0..64) | packed zigzags
```

XOR codecs (Chimp, Gorilla) pay for a window of changed bits on every value;
the delta pays only for its width, and a vector's width adapts to the signal's
rate of change. On `tank_psi` that is 17.49 bits a value in float32 against
19.50 for Chimp, AUTO's earlier pick, and 46.51 in float64 against 48.87 for
byte split. ZSTD finds nothing more in it, and it decodes without an entropy
decoder: unpacking, the four-lane prefix sum (two lanes for float64), an add
(1,317 against 222 million values a second for `tank_psi` float32). The
`delta32` candidate is the same difference left whole for ZSTD (22.23 bits).
Values that are not smooth widen every vector to 32 or 64 bits and lose to
the others, which is why it is a candidate and not a rule. It is the transform
only: the study behind it (2026-10) also tried patching the few widest deltas
of a vector as exceptions, which saved 0.16 bits a value on `tank_psi`, not
enough for a second layout.

#### Dictionary (kind 16, src/codec_typed.c) and two efforts

A pressure read by an ADC and converted to psi, or printed at 0.001 bar while the
instrument resolves about 0.05, takes only the few hundred values the sensor's
integer steps map to. Neither fixed point (no decimal scale fits the psi values;
the bar values pay for codes that never occur) nor the bit delta (it pays for the
scale as well as the step) sees that. The dictionary stores the distinct values in
numeric order and then each value's rank among them, as zigzag deltas packed per
vector of 1024 the way `BLK_INT` packs integers: neighbouring readings have
neighbouring ranks, so a delta takes as few bits as the sensor's step.

```
u32 nd | BLK_INT stream of the nd sorted keys | per vector: u8 bits (0..22), packed rank deltas
```

A key is the bit pattern made monotone as an unsigned integer, so 0.0 and -0.0,
or NaNs with other payloads, stay apart and come back bit for bit. A block takes
at most 2048 distinct values and at most a quarter of its rows. On the UCI
hydraulic rig (`bench/hydraulic`) the six pressures in psi shrink to 0.14-0.20
times their float64 size and 0.44-0.63 times their float32 size, PS3 in bar to
0.69 times; the whole rig to 0.65 times (float32) and 0.29 times (float64). On the
synthetic sensors of `tsdb-bench`, which have no such steps, it changes 3%
(float32) and 16% (float64).

Building it costs several times the encode of the codecs it competes with (a hash
of every value, or a table indexed by the fixed-point integer, then a ZSTD pass of
its own), so AUTO has two efforts (`codec_ctx.effort`), chosen by who writes:

- **Inserts and memtable flushes write `CODEC_FAST`**: float32 as described
  below without the dictionary; float64 fixed point up to 16 bits a value, else
  wider fixed point, Gorilla's XOR and byte split, each compressed in full and the
  smallest kept. Insert speed is that of the codecs before the dictionary (means
  of 5: hydraulic float32 load 2.87 to 2.52 s, float64 2.28 to 2.28 s;
  `insert-bench` and `tsdb-bench` loads equal or faster).
- **Merges write `CODEC_MERGE`**: the dictionary joins the candidates, and
  float64 ranks bit delta and ALP_rd on a sample as float32 does. Fixed point up
  to 16 bits a value is replaced by the dictionary only when its integers are not
  dense (two runs of 64 fill at most half the codes they span), span under 65536
  and the ranks come out smaller than the integers before compression. Merges
  never copy a granule of a part no merge has written: they re-encode it, once,
  and copy granules of their own outputs as before.

A part an insert wrote that has nothing to merge with (the only part of its
partition) is rewritten alone once its partition has been idle for
`merge_idle_s`, and by `tsdb_optimize`, so every part at rest ends up encoded at
merge effort. That rewrite is the only background write the main branch did not
do: 0.70 bytes per byte loaded where each partition got a single insert (PS3,
float32), nothing where partitions got several parts. Merges write less in total,
since their sources shrink after the first: loading the hydraulic rig in windows of
98304 values with background merges until they settle, merges wrote 3.4 bytes per
byte loaded against 5.2 (float32) and 1.6 against 4.6 (float64). The cost is CPU
in the merge thread: `tsdb_optimize` of the whole rig, granules aligned so that
the main branch copied every one, took 1.36 to 2.72 s (float32) and 1.68 to 3.42 s
(float64), 65 and 51 million rows a second on one thread, still above what inserts
deliver (the `tsdb-bench` load, 4 writers, ran the same 192 merges in the same
time as before).

Tried and left out: the old byte decimal (kind 3) with ZSTD as a merge candidate
(5-20% smaller on some pressures, at 2-4 times the encode); plain values with
ZSTD and Chimp128 after the dictionary (they won 54 and 9 of 1614 granules of one
sensor); a sample to gate the dictionary (runs see a fraction of a block's
distinct values: 700 of 2000 on a sine in psi); a density gate on the whole block
that also skips fixed point of more than half its codes (PS4 in psi 20% larger).

Block headers: a byte whose low 7 bits name the kind and whose high bit says the
block is ZSTD-compressed (format 6; format 5 had 4 bits of kind and kinds 0 to 15
were all taken).

#### How AUTO picks (src/codec.c, `block_encode_f32`)

1. **Fixed point first.** If `decimal_bp` fits in 16 bits a value or less, it
   is kept without comparing anything else (in merges, unless the dictionary
   of its integers comes out smaller, see above).
2. **Otherwise compare.** Chimp, bit delta (deltas of the bit patterns,
   whole for ZSTD, and packed per vector), byte-stream split and ALP_rd each
   encode a sample of 8 KB (16 runs of 128 consecutive values spread over the
   granule; the packed delta per run, so that the seams do not widen its
   vectors), compressed with ZSTD as its block would be. The sample's size,
   scaled to the granule, ranks them against the real block of `decimal_bp`
   (at any width). Only the best and any within 3% of it are encoded in full,
   and the smallest block is kept (`auto_pick` in src/codec_typed.c, shared
   with float64, which explains the ZSTD probe in front of each attempt).
   Order-0 entropy estimates ranked the candidates until October 2026; they
   cannot see ZSTD's matches, and on real pressures converted to psi (UCI
   hydraulic rig, `bench/hydraulic`) they took the packed bit delta, which
   hides the repeated deltas, where plain deltas and ZSTD were 27% smaller.
   Float32 leaves out float64's two rules for encoding every candidate in full
   (under 24 bits a value; a full block 10% below its estimate): on those
   pressures nearly every block came out 10% below its sample's estimate,
   since values repeat across the whole granule, and encoding everything
   changed sizes by under 0.1% at half the encode speed. Half the sample
   (8 runs) chose blocks 5% larger there.
3. **ZSTD(1)** wraps a block of at least 64 bytes, and stays only if it saves
   4 bytes plus 1/32.

What AUTO picked, on 128 granules of each data set:

| data set | AUTO's codec | kept ZSTD |
|---|---|---:|
| hdd_temp, cpu_temp, tank_bar | `decimal_bp` | 128 |
| tank_psi (converted, unrounded) | packed bit delta | 0 |
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
| tank pressure in psi (converted, unrounded) | 23.77 | 19.57 | 19.62 | 27.24 | n/a | 21.55 | **17.49** |
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
| ALP_rd | unpack indexes and low parts, OR in `dict[idx & 7] << r` without a branch, patch the exceptions; float64 unpacks its 48- to 63-bit low parts with the wide unpacker |
| packed bit delta | unpack, prefix sum of the zigzags (four 32-bit lanes, or two 64-bit ones), add the vector's first pattern |
| `decimal_bp` | per vector: unpack, prefix sum of the zigzag deltas four lanes at a time relative to the vector's first integer, `(float)((base + r) * inv)`; exceptions patched at the end |
| `decimal` | after ZSTD, a running sum over 1-, 2- or 4-byte deltas |
| float64 fixed point | the integer codec straight into the output buffer (the same prefix sum, added to the vector's first integer in 64 bits), then converted to doubles in place |

Unpacking is one function per bit width, with the width a compile-time
constant and one unaligned 8-byte load per value (src/bitpack.h); the 64-bit
unpacker for widths up to 64 adds a ninth byte for a value that straddles the
load, possible only beyond 57 bits. The running
sum was the serial step of `decimal_bp`: one add chain through every value,
which is why ALP, with the same unpacking and no sum, decoded twice as fast.
`zz_prefix32` (src/bitpack.h) now sums four lanes at a time with SSE2, which
every x86-64 has, so the same code runs at every CPU level: two shifted adds
give the prefix within four values, the last lane of the previous four is
added as the carry. The sums are taken relative to the vector's first integer
`k`, in 32 bits; that is exact when the vector's deltas are at most 22 bits
wide (1024 deltas in [-2^21, 2^21) sum to within int32). The float is then
`(float)((k + r) * inv)` with `k + r` computed in double: `k` (at most 2^53)
and `r` are exact doubles, so their sum is one rounding of the integer
`k + r`, which is what the sequential loop converted, and the bits are
identical. Wider vectors, or a damaged block whose `k` lies beyond 2^53, take
the sequential loop. The int64 codec (int64, bool, state, and float64 fixed
point) uses the same sum, added to `k` in 64-bit integers.
`tests/test_levels.c` checks both against the sequential loops on crafted
blocks at every width and at the limits.

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

AUTO keeps fixed point outright at 16 bits a value or less (merges try the
dictionary of its integers too). Otherwise inserts (`CODEC_FAST`) encode wider
fixed point, Gorilla's XOR and byte split in full and keep the smallest; merges
(`CODEC_MERGE`) rank the candidates (Gorilla's XOR encoding of the 64-bit patterns, byte-stream
split, the packed bit delta and ALP_rd) first encode a sample: 8 runs of 128
consecutive values spread over the granule, the delta per run so that the
seams do not widen its vectors, each compressed with ZSTD as its block would
be. The sample's size, scaled to the granule, ranks them; only the best and any
within 3% of it are encoded in full (with ZSTD where it saves 3%), alongside
fixed point if it applies and the dictionary if the block has few enough
distinct values, and the smallest block is kept; a candidate whose estimate is
25% above the dictionary's real block is not encoded. Two cases encode
every candidate in full: a sample under 24 bits a value, where ZSTD's matches
decide and a sample's small window underrates some transforms more than
others; and a full block that comes out more than 10% below its estimate,
where matches farther apart than the runs (a random walk that comes back)
decide and may favour another transform. Against encoding every candidate in
full, sizes stay within 0.11% on the test sets and random doubles encode
about 3.5 times as fast (24.8 to 87 M values/s). Where the probe, ZSTD at
level -1, finds less than 1/128 to save (noisy doubles after any transform),
the real attempt is skipped: it would not save the 1/32 `block_wrap` asks for
either, and costs ten times as much on such bytes. An order-0 entropy
estimate in place of ZSTD on the sample was tried and ranked badly where
ZSTD's matches decide (repeated deltas of calibrated readings, the low byte
planes of a split), losing up to 9%. With ZSTD, from `build/type-bench`:
hdd_temp 0.88, cpu_temp 3.10,
tank_bar 1.82, vibration 14.03, flow_rate 10.80 and flow_total 1.56 bits a
value, all fixed point; tank_psi 46.51, packed bit delta (byte split took
48.87); random 56.29, ALP_rd (byte split took 59.10).

**ALP_rd for float64** (src/codec_alp.c) is the paper's: the high 1 to 16
bits (sign, exponent, a few mantissa bits) are dictionary coded, up to 8
entries at 0 to 3 bits an index, and the low 48 to 63 bits packed as they
are, the cut chosen on a sample of 256 values; high parts missing from the
dictionary are exceptions (u16 position, u16 high part). For doubles that
are random in their mantissa only the exponent repeats, and ALP_rd stores it
in about 3 bits where byte split and ZSTD leave nearly 8. It loses on the
smooth psi values (50.53 against 46.51 for the bit delta), so AUTO's
comparison decides.

**float32 readings in a float64 series** (kind 17, src/codec_typed.c): a collector that widens what a float32
sensor gives it writes doubles like 35.099998474121094, which are no longer decimals and carry 29 zero bits at
the bottom of every mantissa that XOR and byte split pay for anyway. When every value of a block widens back
from a float32 to its exact bits (the float64 null maps to the float32 null; the double the float32 null widens
to does not qualify, so the mapping is one to one), the block is stored as a whole float32 block behind one
header byte, and decoding widens it back. NaN payloads, -0.0 and infinities survive. AUTO tries it after
compact float64 fixed point, which real decimals take first, and 32 values probed across the block turn real
doubles away before the full check (zeros and whole numbers are float32 too, so a scan from the start can run
far). Inserts take it only when the float32 block is compact fixed point, which costs a sample and one pass;
readings that are not (pressures converted to psi in float32, then widened) are left to inserts' float64
candidates, and merges, which pick among all the float32 kinds, store them as float32. On the hydraulic rig's
float32 files widened to float64 (17 sensors and the psi conversions), the merged database takes 0.45 times the
space of format 5's (279.2 to 125.9 MB, about what the same readings take as float32) and the inserts 0.67 times
the time; tsdb-bench's float32 readings stored as float64 come down from 144.1 to 111.4 MiB. True doubles insert
at the speed they had.

Block kinds name the transform; for kinds that exist at both widths (ALP_rd,
the packed bit delta) the series' type, which lives in the SERIES dictionary,
gives the width.

**int64, bool and state codes**: zigzag deltas bit-packed per vector of 1024
values. Counters, codes that rarely change and flags come down to a few bits a
value and constant runs to none. `build/type-bench` measures these: a
totalizer in liters 1.18 bits a value, a sequence 0.04, a door contact
sampled at 1 Hz 0.06, a machine mode 0.13.

### SIMD

Gorilla, Chimp and DoubleDelta decoding is bit-serial: where value i+1 starts
depends on value i, so it cannot be vectorized. The aggregation kernels are
dispatched at runtime to one of five sets: AVX-512, AVX2, AVX (Sandy and
Ivy Bridge, which have AVX but not AVX2), SSE4.2 (any x86-64-v2 CPU) or
scalar code; the fixed-point encoder is written so the compiler vectorizes
it. AVX-512 is not used on Skylake-SP, Cascade Lake and Cooper Lake Xeons,
which lower the clock of the core after 512-bit instructions.
`TSDB_SIMD=avx512|avx2|avx|sse|scalar` overrides the choice. In a portable build the codecs are compiled per CPU level instead (see
"Decoding and CPU levels" above).

Every kernel gives the same bits, so a granule's statistics do not depend on
the CPU that wrote them. Float additions are not associative, so floats are
summed in one fixed order on every path: float32 in 32 double lanes, float64
in 16 (lane j takes values j, j+16, j+32, ...; 16 because AVX2 then still
holds sums, minima and maxima in its 16 registers), then the lanes are halved
(lane j + lane j+8, j + j+4, ...). The scalar code is written in the same
lanes, and so are the SSE and AVX kernels: their 128-bit registers hold two
float64 lanes each (eight registers for the 16 lanes), AVX2's four. A wider
register only changes how many lanes one instruction works on, never which
value goes to which lane or the order in which lanes are added. min and max
are compared lane by lane the same way, which decides the
sign of a zero minimum. A null (a reserved value, a NaN for floats) adds +0.0
to its lane instead of being skipped: a lane starts at +0.0 and never becomes
-0.0, so +0.0 changes nothing and the lane order stays by position, without a
branch per row. The kernels take the number of nulls (one quick pass counts
them first) and mask only when there are any.

A run shorter than the lanes (a bucket of a few rows, folded once per bucket)
skips the kernels: value j alone in lane j, and the halving starts at the
smallest power of two that holds the run, since an empty lane (+0.0, or the
running minimum and maximum) changes nothing in it. The bits are those of the
kernels; setting up and reducing 32 lanes would cost more than the values.
On SSE and AVX the short path is itself a 128-bit kernel and is used up to
64 values. The integer loops below likewise take short runs, and short LTTB
buckets are scanned by the scalar loop; where each set's kernels start to pay
was measured per set (`fold-bench --vary`): int64 from 16 to 48 values (with
the spread from 16 or 24 on AVX-512 and AVX2; never on SSE and AVX, which have
no 64-bit multiply for the squares and lose to the loop at every length), bool
and state codes from 24 or 32, LTTB from 8 to 48 points.

Integers (int64, bool and state codes) are summed exactly, so their order is
free: the vector kernels add 32-bit halves of the values in 64-bit
lanes (and, for int64, count the negative ones), which cannot overflow within a
block, and combine the lanes into the 128-bit sum. The spread's sum of squares
is exact too: bool and state squares (below 2^64) in the same pass, int64
squares in a second pass (one 32-bit multiply per value when every known value
of the run fits in 32 bits, three otherwise). The scalar loops in `values.c` are the
reference, and the code for short runs.

A granule's time-weighted areas (`tw_step`, `tw_lin`, written with the
granule) are float sums too, in one of two orders fixed by the granule's
length alone, the same on every CPU and kernel set. Up to `TWA_SEQ_MAX` (383)
points the segments are added one after the other, starting from +0.0, by a
plain loop in `twa.c`; that file is compiled once, so every CPU runs the same
instructions on the same values. Beyond that, segment s (from reading s to
s + 1) adds to lane s % 16 of the float64 scheme, a segment that does not count
adds +0.0, and the lanes are halved the same way, in every kernel set. The
limit is where the slowest kernel set (SSE, int64 with nulls) first matches
the loop; below it the loop's fixed cost is lower. The milliseconds and the
longest gap are integers. LTTB's scan for the largest triangle computes each point's area with
the same expression in every kernel (timestamps and int64 values converted to
double rounded once, as the scalar conversion does); each lane keeps its first
largest area and the lanes are then combined by area and lowest index, so the
pick is always the first point with the largest area, and a NaN never wins.
The areas of longer granules were summed one segment after the other before;
like the float64 sums, they may differ from those in the last bits once.

Before these kernels, float64 and float32 with nulls were summed one value
after the other; their sums and spreads may now differ from those in the last
bits (there has been no release, so stored granule statistics were not
rewritten). Since the order is fixed, this is a one-time change.

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

Memory for requests is bounded, so a client cannot make the server run out of
it. Requests received but not yet run (empty ones included), receive buffers
larger than 64 KB and compressed inserts being unpacked share
`--max-request-memory` (default an eighth of the RAM, at least 1024 MB): above
it the server stops reading from connections that want more until requests
finish, and it always lets one frame through. Outside the limit are 64 KB of
receive buffer per connection and the replies a client has not read yet (up to
about 8 MB per connection; then the request waits). A connection reads no more
once 4 MB of its requests are waiting, for example behind a large reply its
client does not read. A frame that is started but not finished within
`--frame-timeout` seconds (default 60) closes its connection; time in which the
server, not the client, held it up does not count. A reply that would not fit
in one frame (256 MB), or a query over more than 4M buckets, is
`TSDB_ERR_TOO_MANY_BUCKETS`. A compressed (packed)
insert that would be larger than a frame unpacked is refused before anything is
decompressed. A bad number on the command line (`--port 70000`, `--workers 0`)
stops the server at once with status 2. Without authentication a client can
still take the whole limit with large frames it sends slowly, for
`--frame-timeout` at a time, so that other clients' large frames wait.

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

## Many series in one request

A dashboard or a report asks the same question of many series: the last
hour of 200 sensors, a day of every pump by hour. One request per series
costs a round trip and a request's fixed work each; `tsdb_query_multi`
(embedded) and QUERY_MULTI (protocol 10) ask it once for a list of up to
65,536 series (the clients split longer lists).

```c
tsdb_multi_query q = {.kind = TSDB_MULTI_BUCKETS, .start = t0, .end = t1, .interval_ms = 3600000};
tsdb_query_multi(db, &q, ids, n, NULL, on_series, ctx);     /* on_series(ctx, r) in index order */
tsdbc_query_multi(c, &q, ids, n, NULL, on_series, ctx);     /* the same over TCP */
```

- **Same answers as one query per series.** Each series runs the unchanged
  single-series function (values, buckets, spread, TWA, LTTB; fixed, calendar
  or bounds buckets), with its own status: a bad or unknown series does not
  end the request. Start, end and interval may differ per series.
- **One snapshot.** The call takes one cut of every volume (parts and
  memtables) and every series reads from it.
- **In order, in parallel.** The caller delivers results in index order;
  the database's scheduler runs up to 2 x threads series ahead of it. Small
  series run one per thread; a large one still spreads its granules over
  threads inside its task.
- **Bounded memory.** A series that runs ahead keeps its result until its
  turn, at most 64 MiB for the whole request (charged to
  `--max-request-memory` in tsdbd). One that would need more (over 65,536
  rows or points, or 16 MiB of bucket accumulators or LTTB buffers) stops
  and runs again at its turn, streaming like a single request. The working
  memory in flight is thus at most 16 MiB per thread running a task. `max_rows` (tsdbd:
  `--multi-max-rows`, default 268,435,456) caps the rows or buckets of a
  request; past it the request ends with `TOO_MANY_BUCKETS`.
- **Stopping.** A callback that returns nonzero ends the call; a client
  sends CANCEL and the server ends the request after the series in flight.
  A CANCEL looks only at the connection's queued QUERY_MULTIs, of which
  tsdbd reads at most 16 (and 4 MiB of requests) ahead: a CANCEL sent behind
  more waits until they run.
- **Local time zones.** The server knows UTC only. For calendar buckets in a
  zone, a client asks for the first and last row of every series (two
  QUERY_MULTI VALUES with a limit of 1), builds one list of bounds for the
  union of those ranges and sends it with TRIM, which cuts each series'
  bounds to its own rows; above the bounds limit it falls back to one
  request per series.

### Threads

Every database has one query scheduler with `query_threads - 1` helper
threads (tsdbd `--query-threads`, default one per CPU). One query, or one
multi-series call, uses at most `request_threads` of them, the caller
included (`--request-threads`, default all), so that many concurrent
requests share the CPUs instead of each taking all of them. The caller of a
job always works on it: a job finishes even when every helper is busy.

Under load a job asks for no more than its share of the threads, its
caller included: threads / requests in flight, at least 1. The callers of the
other requests occupy CPUs too, so the shares add up to the threads and no
more. With as many concurrent requests as CPUs, or somewhat fewer (a
server's requests spend part of their time reading and writing sockets,
outside a query), each runs on its own thread and wakes nobody: the helpers
would be busy with the other requests, and waking them costs locks, wake-ups
and caches for nothing (an A/B run showed small aggregates 10 to 38 % slower
at 32 clients before this rule, and tsdbd's still up to 11 % slower with a
share of 1 + helpers / requests, which counted only the helpers).
A lone request still gets every helper. The opener of a job wakes two
helpers and each helper that joins wakes two more, so the opener starts on
its own items after two wake-ups instead of one per helper.

How many threads one query step uses follows its work
(`query_threads_for` in src/query.c): small reads run on the calling thread
alone, with no wake-up and no cache traffic; large ones spread out, one
thread per `QUERY_ROWS_PER_THREAD` rows to decode (a TWA's rows count
`TWA_ROW_WEIGHT` times, since it integrates every row). Both constants come
from `build/threads-bench`, which times one thread against many across
work sizes. What a thread adds up does not depend on the count, so answers
are bit-identical at any thread count (`tests/test_threads.c`).

The sweep on the 7950X (16 cores, 32 threads), one query at a time over
windows of 8192-row granules, 5 runs of 100 ms each, time per query and its
ratio to one thread:

| granules | aggregate, 1 thread | at 16384 rows per thread | TWA, 1 thread | at 16384 rows per thread |
|---:|---:|---:|---:|---:|
| 1 | 8.1 us | 8.0 us (1.00) | 105 us | 105 us (1.00) |
| 2 | 15.4 us | 15.4 us (1.00) | 208 us | 106 us (0.51) |
| 4 | 30.0 us | 21.6 us (0.72) | 415 us | 111 us (0.27) |
| 8 | 59.8 us | 25.0 us (0.42) | 833 us | 117 us (0.14) |
| 32 | 243 us | 59.5 us (0.25) | 3.33 ms | 244 us (0.07) |
| 128 | 969 us | 158 us (0.16) | 13.3 ms | 654 us (0.05) |
| 1024 | 7.76 ms | 834 us (0.11) | 104 ms | 4.45 ms (0.04) |

No window got slower than on one thread at 8192 or at 16384 rows per thread;
16384 is as fast over the whole sweep and wakes half as many helpers for
small windows. The second thread of each core (SMT) helps large reads: with
16 instead of 32 threads, the 1024-granule aggregate takes 944 us instead of
834 and the TWA 7.13 ms instead of 4.45. For aggregates of 32 to 128
granules 16 threads were up to 20 % faster than 32.

## Command line

```sh
build/tsdb /data/sensors create pump7.runtime_s --type int64   # float32 unless --type
build/tsdb /data/sensors create pump7.mode --type state
build/tsdb /data/sensors import < rows.csv          # series,time,value (null or empty: null)
build/tsdb /data/sensors insert pump7.flow < p.csv  # time,value
build/tsdb /data/sensors query pump7.flow 2024-01-01 2024-01-02 avg 5m
build/tsdb /data/sensors query pump7.flow now-1h now raw
build/tsdb /data/sensors query pump7.pressure 2024-01-01 2024-01-02 stddev 1h   # also range, stddev_samp, var, var_samp
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

