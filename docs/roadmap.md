# Roadmap

What tsdb still needs, decided with the user on 2026-10-24. Before 1.0 the on-disk
format and the wire protocol may still change, so features that touch them are
cheapest now.

tsdb stores and queries time series. The industrial context around them (assets
and their hierarchy, units and other metadata, events, data quality, live
subscriptions) belongs to the IntelliStream DataHub platform, which uses a time
series store underneath; tsdb does not duplicate it.

## Done

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
| Store on change (opt-in, per series) | keep a row only when the value changes, plus a heartbeat row after a maximum gap so a silent sensor is not mistaken for a steady one (decided with the user on 2026-10-26). Measured first: what it saves in bytes, rows and query time on flat data with regular and jittery timestamps. Exact equality only by default (a deadband threshold would be lossy compression). Counts and plain averages then count stored rows; time-weighted averages and the `state` type stay right. Start in the clients and the Pulsar connector (no engine or format change); an engine option in the series dictionary only if that is not enough, before the 1.0 freeze |

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
| aarch64 (AWS Graviton, NEON/SVE2) | rentable now | instances about 29% cheaper than comparable x86 ones; needs an aarch64 build and kernels with the same results as x86 |

## Not planned

Latest-value lookups, series metadata, quality codes, interpolated and
value-at-time retrieval, totals, M4 chart points, event frames, rollups,
subscriptions, annotations, lossy compression, asset hierarchy and calculations:
DataHub covers what is needed of these, or they are not needed.
