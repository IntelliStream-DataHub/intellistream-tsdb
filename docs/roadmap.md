# Roadmap

What tsdb still needs, decided with the user on 2026-10-24. Before 1.0 the on-disk
format and the wire protocol may still change, so features that touch them are
cheapest now.

tsdb stores and queries time series. The industrial context around them (assets
and their hierarchy, units and other metadata, events, data quality, live
subscriptions) belongs to the IntelliStream DataHub platform, which uses a time
series store underneath; tsdb does not duplicate it.

## Done

- Many series in one request (protocol 10, QUERY_MULTI and CANCEL): values, buckets (fixed, calendar in any
  zone, bounds), spread, TWA and LTTB for up to 65,536 series, answered in order from one snapshot, the series
  spread over the server's threads; in the library (`tsdb_query_multi`), tsdbd, the C, Rust, Python and Java
  clients and the embedded bindings. One query scheduler per database (`--query-threads`, `--request-threads`)
  and threads per query that follow the rows it decodes.
- Time-weighted averages (step and linear, staleness limit), LTTB downsampling, canonical NaN results, in the
  library, tsdbd and the C client and CLI.
- Edge buffering: the Apache Pulsar 5 sink (connectors/pulsar).
- Limits: raw, bucketed, bounds, calendar, time-weighted, spread and LTTB queries take an order and a limit;
  bucketed queries with a limit stop early (protocol 9, C client and CLI).
- Embedded bindings: Rust (`tsdb_client::Db`), Python (`tsdb_client.Db`) and Java 22+ (FFM, client/java-embedded).
- The Rust, Python and Java clients and the embedded bindings at protocol 9: the spread, LTTB and TWA with
  order and limit, range delete, replace and retention; embedded and TCP answers compared in the tests.
- Delete or replace a time range of one series: atomic replace (delete plus
  insert), in the library, tsdbd (protocol 7), the C, Rust, Python and Java
  clients and the CLI.
- Retention per series: values older than the series' limit disappear from
  queries at once and are removed by merges.
- Standard deviation and range in aggregates: range, population and sample
  variance and standard deviation (`tsdb_query_spread` and friends), exact
  for integers, numerically stable for floats, from granule statistics; in
  tsdbd (protocol 8), the C client and the CLI.
- Prometheus metrics: tsdbd serves `/metrics` in Prometheus text format on a
  separate port (`--metrics-port`, docs/metrics.md).

## Releases 0.9.0 and 1.0

Decided with the user on 2026-10-24 and 2026-10-25: the first release is 0.9.0. The disk format and the wire
protocol freeze at 1.0, not before: a 0.9.x release may still change them, with a way to carry data over
(export and import, or a migration) and a database of another format refused with a clear message. 1.0 is one
writing server per database (read-only followers on other hosts over NFS); failover comes after it. What 0.9.0
and 1.0 still need (the format and protocol changes may land in a 0.9.x, but before 1.0):

- Input safety: client-chosen volume paths, query memory that follows rows, crafted files, network frames,
  client and CLI parsing; fuzzing of decoders, file loaders and frames (in progress).
- SIMD kernels for float64, int64 and bool, the spread, time-weighted averages and LTTB, at every CPU level
  (AVX-512, AVX2, x86-64-v2), with identical results on every CPU (in progress).
- Series addressed by volume path and name with 64-bit ids, from the failover design (docs/ha.md §5): it changes
  the protocol, the on-disk layout and every client, so it lands before the formats are frozen, and failover
  can follow in 1.1 without a break.
- Release mechanics: version and tag, release notes, the compatibility promise (which formats and protocol
  versions 1.x reads), the portable tarball checked on AlmaLinux 10; export and import for 0.9.x format changes.
- The release benchmarks: the full comparison (bench/dbcmp) and the internal numbers on a quiet machine.
- VALUE_PROPOSITION.md for decision makers, written from the full comparison: storage cost compared to the most
  popular systems (InfluxDB first, per the DB-Engines ranking), servers needed, and the honest limits (one writer
  per database until failover; results to confirm with a pilot on the customer's data).

## After the first release

| feature | notes |
|---|---|
| Failover with one writer per volume, NFS outage handling | designed in docs/ha.md |
| Authentication, TLS, access control | until then tsdbd belongs on a trusted network only |
| Store on change (opt-in, per series) | keep a row only when the value changes, plus a heartbeat row after a maximum gap so a silent sensor is not mistaken for a steady one (decided with the user on 2026-10-26). Measured on 2026-10-26 (1000 series x 86,400 rows, sizes compared to keeping every row): it pays where values repeat and timestamps jitter (slow float32 0.09x, door 0.007-0.03x, 5-state mode 0.01-0.03x, counter 0.11-0.12x; raw-row queries 11-312x faster), saves less with exactly regular timestamps (0.2-0.9x: they already cost 0.03 bits a row), and costs on series that change almost every sample (1.11x jittered) and on most real hydraulic sensors (14 of 17 larger, up to 1.16x), so it stays opt-in per series. Step TWA gives identical answers on both; a client-side filter costs about 1 ns a row. Exact equality only by default (a deadband threshold would be lossy compression). Counts and plain averages then count stored rows; time-weighted averages and the `state` type stay right. Start in the clients and the Pulsar connector (no engine or format change); an engine option in the series dictionary only if that is not enough, before the 1.0 freeze |
| Interpolation at query time (resampling to a finer or regular grid) | decided with the user on 2026-10-26, replacing the earlier "not planned". A query names a range, an output step and a method, and gets one value per step: previous value (step, last observation carried forward), nearest, linear, monotone cubic (PCHIP, never overshoots the readings) and Akima (smooth, little ripple). All five are local (two readings on each side at most), so they run one granule run at a time like the TWA code, whose rules they reuse: the reading before the range is carried in, nulls and gaps longer than `max_gap_ms` give null instead of a made-up value, deleted and expired readings do not exist. bool and state allow previous and nearest only. One timestamp gives the value at a time. A cap on output points like the bucket limits; written without FMA so every CPU level gives the same values. A new operation in the protocol, in all four clients and the embedded bindings in one change. Pairs with store on change: previous value rebuilds the regular series from changes only |
| SIMD bit unpacking in the codecs | decided with the user on 2026-11-01. Decoding a bit-packed block (decimal_bp, the integer and bool/state codecs) is unpack, then the zigzag running sum, then the conversion to values. The compiler vectorizes only the conversion (AVX2 at v3, AVX-512 at v4); the running sum is hand-written SSE2, 4 lanes; the unpack (`unpack_W` in src/bitpack.h) stays scalar, one value per iteration at every level (checked in the -O3 assembly of codec.c), and it is the bottleneck. Write AVX2 and AVX-512 unpackers for the current LSB-first layout (shuffles and variable shifts: no format change), and widen the running sum to 8 and 16 lanes; both live in the per-level files (levels.h). Every level must give identical values (tests/test_levels.c, every width 0-32 and the tails of a 1024-value vector); measure with build/codec-bench and build/type-bench on a quiet machine, decode speed per codec, mean of several runs. A FastLanes-style interleaved layout could be faster still but changes the format, so it would have to come before the 1.0 freeze; decide on the numbers of the layout-preserving version first. VBMI's VPMULTISHIFTQB (row below) is a further step on CPUs that have it |

## When hardware is available

New CPU features to use once there is hardware to test on (decided with the user on 2026-10-26). Every
kernel keeps the rule of identical results on every CPU level. The Zen 5 work has a task file of its own for the test machine:
[docs/tasks/ryzen-9950x.md](tasks/ryzen-9950x.md); tuning for Intel's AVX2 cores (v3) has one for an
i9-10850K: [docs/tasks/intel-10850k.md](tasks/intel-10850k.md).

| feature | where | what tsdb gains |
|---|---|---|
| AVX-512 VBMI/VBMI2, GFNI, BITALG, VPOPCNTDQ (beyond x86-64-v4) | Intel Ice Lake and later, AMD Zen 4 and later (testable on today's machine) | faster bit unpacking in the decimal and integer codecs (VPMULTISHIFTQB, byte permutes) |
| AVX10.2 | Intel Nova Lake (late 2026), Diamond Rapids | AVX-512 on every core, desktops and laptops included; IEEE 754-2019 min/max for the min/max kernels; saturating float-to-integer conversions for the decimal and ALP codecs |
| APX | Intel Nova Lake, Diamond Rapids | 32 general registers and conditional loads/stores for the bit-serial decoders (Gorilla, Chimp, DoubleDelta); a CPU level of its own in the portable build |
| AVX2 float64 fixed point (x86-64-v3) | Intel AVX2 cores without AVX-512 (the i9-10850K task) | float64 decimal encode and decode at more than v2 speed on v3 CPUs; not written yet |
| Intel hybrid CPUs (P-cores with Hyper-Threading plus E-cores: Alder Lake to Arrow Lake; Lunar Lake and Arrow Lake dropped Hyper-Threading on P-cores) | desktops and laptops since 2021; many servers stay uniform (Xeon, EPYC) | check that the query scheduler's work claiming keeps every core busy when cores differ in speed (no equal split that waits for the slowest E-core), and what the right default thread count is; v3 level (no AVX-512) |
| aarch64 (AWS Graviton, NEON/SVE2) | rentable now | instances about 29% cheaper than comparable x86 ones; needs an aarch64 build and kernels with the same results as x86 |

## Not planned

Latest-value lookups, series metadata, quality codes, totals, M4 chart points, event frames, rollups,
subscriptions, annotations, lossy compression, asset hierarchy and calculations:
DataHub covers what is needed of these, or they are not needed.
