# Roadmap

What tsdb still needs, decided with the user on 2026-10-24. Before 1.0 the on-disk
format and the wire protocol may still change, so features that touch them are
cheapest now.

tsdb stores and queries time series. The industrial context around them (assets
and their hierarchy, units and other metadata, events, data quality, live
subscriptions) belongs to the IntelliStream DataHub platform, which uses a time
series store underneath; tsdb does not duplicate it.

## In progress or designed

- Time-weighted averages (step and linear, staleness limit), LTTB downsampling,
  canonical NaN results.
- Embedded bindings for Rust, Python and Java (FFM), next to the TCP clients.
- Failover with one writer per volume, and NFS outage handling (docs/ha.md).
- Edge buffering: Apache Pulsar and the Pulsar sink (connectors/pulsar).

## Done

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

## Next

Nothing queued beyond the work in progress above.

## After the first release

| feature | notes |
|---|---|
| Authentication, TLS, access control | until then tsdbd belongs on a trusted network only |

## Not planned

Latest-value lookups, series metadata, quality codes, interpolated and
value-at-time retrieval, totals, M4 chart points, event frames, rollups,
subscriptions, annotations, lossy compression, asset hierarchy and calculations:
DataHub covers what is needed of these, or they are not needed.
