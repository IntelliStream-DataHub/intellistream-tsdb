# Roadmap

What tsdb still lacks, compared with what industrial users expect from systems such
as OSIsoft PI. P1 is needed for production, P2 makes tsdb pleasant to build on, P3
is optional. Before 1.0 the on-disk format and the wire protocol may still change,
so features that touch them are cheapest now.

In progress or designed (not repeated below): time-weighted averages, LTTB
downsampling, canonical NaN results, embedded bindings for Rust, Python and Java,
failover and NFS outage handling (docs/ha.md). Edge buffering is covered by Apache
Pulsar (connectors/pulsar); out-of-order writes and backfill already work (last
write wins).

## P1: production

| feature | PI equivalent | today |
|---|---|---|
| Authentication, TLS, access control per series or volume | PI trusts and identities | tsdbd trusts anyone who can connect |
| Delete or replace a time range of one series (also from clients) | edit and delete archived values | `drop_before` for whole partitions only |
| Retention per series or volume (TTL) | archive management | none |
| Backup and restore (immutable parts make consistent snapshots cheap) | PI backups | none |
| Metrics endpoint for monitoring (Prometheus format) | PI performance counters | `STATS` op only |
| Latest value of many series in one call | Snapshot | one query per series |
| Series metadata: unit of measure, description, value range, key/value tags; find series by tag | point attributes (engineering units, descriptor, zero/span) | names only |

## P1: industrial meaning of the data

| feature | PI equivalent | today |
|---|---|---|
| Data quality per value: bad input, I/O timeout, comm fail, shutdown, questionable, substituted | system digital states, quality flags | only null |
| Value at a given time: before, after, exact, interpolated (step or linear) | RecordedAtTime, InterpolatedAtTimes | raw ranges only |
| Evenly spaced interpolated values over a range | InterpolatedValues | none |
| Totals (integral with a time unit, m3/h to m3 per day) and percent of time good | Summaries: Total, PercentGood | the area from the time-weighted average |
| Chart points: first, last, min and max per pixel column (M4, exact for line charts) | PlotValues | LTTB (in progress) |

## P2

| feature | PI equivalent |
|---|---|
| Summaries: standard deviation, range, percentiles (approximate per granule) | Summaries: StdDev, Range |
| Rollups maintained as data arrives (continuous aggregates) | PI Analytics rollups |
| Subscriptions: push new values of chosen series to clients | data pipes, signups |
| Event frames: named time ranges (batches, downtime, alarms) with attributes | Event Frames |
| Aggregates across series (the sum over all pumps of a line) | AF rollups |
| Annotations on values | annotations |

## P3

| feature | PI equivalent |
|---|---|
| Optional lossy compression per series with a deadband | exception and swinging-door compression |
| Asset hierarchy and templates (likely above the database) | Asset Framework |
| Cold storage on object storage (intellistream-objects) | archive tiering |
| Expressions and calculations | Performance Equations, Analytics |
