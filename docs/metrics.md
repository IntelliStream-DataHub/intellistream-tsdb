# Prometheus metrics

`tsdbd --metrics-port 9170` serves `GET /metrics` in the Prometheus text format (0.0.4) on a port of its own.
It is off by default and binds to `--listen` unless `--metrics-bind ADDR` says otherwise. There is no
authentication, like the main port: bind it to a trusted interface. `--metrics-port auto` takes a port the kernel
chooses, which `--port-file` reports as its third number (for tests that must not compute a port).

```yaml
scrape_configs:
  - job_name: tsdb
    static_configs: [{targets: ["db1:9170"]}]
```

The endpoint runs on one thread, apart from the event loop and the workers. A scrape reads atomic
counters and takes the part-list lock of each volume briefly; it never runs a query. A client that
sends a request slowly, or not at all, is dropped after 2 seconds; a request head over 4 KB gets 431;
only `GET` and `HEAD` of `/metrics` are answered (404, 405 and 400 otherwise); every connection closes
after one reply. When 64 connections wait for their request, the oldest is closed to make room, so a crowd
of idle connections cannot keep a real scraper out. None of this touches the main port. Without `--metrics-port` the server's counting hooks return at once
(one predictable branch per call). A reply has one 1 s budget for header and body together. A bad
`--metrics-port` value, or a port that cannot be bound or a thread that cannot start, stops tsdbd at startup.
Every request op has a name in the `op` label; an op number the server does not know counts as `other`
(test_server fails when `tsdb_protocol.h` gains an op that `server/metrics.c` does not name).

Counters count from the start of the process. Per-volume metrics carry `volume` (0 is the root
directory) and `path`.

| metric | type | meaning |
|---|---|---|
| `tsdb_build_info{version,build,simd,cpu_level}` | gauge | always 1; `version` is the release (0.9.0), `build` the git describe of the build; `cpu_level` is `native`, or the codec level of a portable build |
| `tsdb_io_backend_info{backend}` | gauge | `io_uring` or `epoll` |
| `tsdb_role_info{role}` | gauge | `writer`, `read-only` or `follower` |
| `tsdb_start_time_seconds` | gauge | Unix time of the start |
| `tsdb_rows` | gauge | rows in parts on disk (merges drop overwritten ones later) |
| `tsdb_disk_bytes` | gauge | size of the part files |
| `tsdb_parts`, `tsdb_partitions`, `tsdb_granules` | gauge | parts, time partitions with parts, granules |
| `tsdb_memtable_rows` | gauge | rows in memtables, not yet in parts |
| `tsdb_merges_total`, `tsdb_merged_rows_total` | counter | merges finished, rows they wrote |
| `tsdb_merging_parts` | gauge | parts a running merge reads now (0: no merge running) |
| `tsdb_wal_bytes_total`, `tsdb_wal_syncs_total` | counter | WAL bytes written, WAL `fdatasync`s (0 with `--no-fsync`) |
| `tsdb_broken_parts` | gauge | parts a follower found damaged and left out |
| `tsdb_failed` | gauge | 1 once the volume is fail-stopped: writes return IO errors and tsdbd exits; alert on it |
| `tsdb_follow_errors_total`, `tsdb_follow_refresh_age_seconds` | counter, gauge | followers only: failed refreshes, and time since the last good one (the age of the follower's refresh, not of the writer's data: a stopped writer leaves it near 0) |
| `tsdb_requests_total{op}` | counter | requests handled, by operation (`insert`, `query_raw`, `query_agg`, ...) |
| `tsdb_inserted_rows_total` | counter | rows in successful insert requests |
| `tsdb_multi_series_total` | counter | series answered by many-series requests (QUERY_MULTI, `op="query_multi"` in the request counters) |
| `tsdb_multi_deferred_total` | counter | series of such requests too large to hold ahead of their turn, run again at it (streaming); a high rate next to `tsdb_multi_series_total` means the requests are mostly large series |
| `tsdb_request_errors_total{code}` | counter | replies with an error status: `io`, `invalid`, `notfound`, ... |
| `tsdb_query_duration_seconds{op}` | histogram | time a worker spent on a query request (0.5 ms to 30 s), up to handing the reply to the kernel, so a slow client taking a large result adds to it |
| `tsdb_connections`, `tsdb_connections_max` | gauge | open connections on the main port, and the limit |
| `tsdb_connections_accepted_total`, `tsdb_connections_rejected_total` | counter | accepted, and closed at once for the limit |
| `tsdb_workers`, `tsdb_workers_busy` | gauge | worker threads (extras start for slow clients), and those running a request |
| `tsdb_request_memory_bytes`, `tsdb_request_memory_limit_bytes` | gauge | memory of requests received and not yet run (with large receive buffers and packed inserts being unpacked), and `--max-request-memory` |
| `tsdb_metrics_scrapes_total`, `tsdb_metrics_rejected_total` | counter | this endpoint: scrapes answered, requests refused or timed out |

Useful expressions: `rate(tsdb_inserted_rows_total[1m])` is the insert rate;
`histogram_quantile(0.99, sum by (le, op) (rate(tsdb_query_duration_seconds_bucket[5m])))` the p99 query time;
`tsdb_failed == 1` pages someone; `tsdb_follow_refresh_age_seconds > 5` means the follower has stopped refreshing (it does not detect a dead writer);
`tsdb_memtable_rows` growing without bound means flushes are not keeping up.
