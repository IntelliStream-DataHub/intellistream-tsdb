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

## Release 1.0

Decided with the user on 2026-10-24: 1.0 is one writing server per database (read-only followers on other hosts
over NFS); failover comes after it. What 1.0 still needs:

- Input safety: client-chosen volume paths, query memory that follows rows, crafted files, network frames,
  client and CLI parsing; fuzzing of decoders, file loaders and frames (in progress).
- SIMD kernels for float64, int64 and bool, the spread, time-weighted averages and LTTB, at every CPU level
  (AVX-512, AVX2, x86-64-v2), with identical results on every CPU (in progress).
- The Rust, Python and Java clients and the embedded bindings at protocol 9: the spread, LTTB and TWA with
  order and limit, range delete, replace and retention.
- Series addressed by volume path and name with 64-bit ids, from the failover design (docs/ha.md §5): it changes
  the protocol, the on-disk layout and every client, so it lands before the formats are frozen, and failover
  can follow in 1.1 without a break.
- Release mechanics: version and tag, release notes, the compatibility promise (which formats and protocol
  versions 1.x reads), the portable tarball checked on AlmaLinux 10.
- The release benchmarks: the full comparison (bench/dbcmp) and the internal numbers on a quiet machine.

## After the first release

| feature | notes |
|---|---|
| Failover with one writer per volume, NFS outage handling | designed in docs/ha.md |
| Authentication, TLS, access control | until then tsdbd belongs on a trusted network only |

## Not planned

Latest-value lookups, series metadata, quality codes, interpolated and
value-at-time retrieval, totals, M4 chart points, event frames, rollups,
subscriptions, annotations, lossy compression, asset hierarchy and calculations:
DataHub covers what is needed of these, or they are not needed.
