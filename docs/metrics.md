# Prometheus metrics

`tsdbd --metrics-port 9170` serves `GET /metrics` in the Prometheus text format (0.0.4) on a port of its own.
It is off by default and binds to `--listen` unless `--metrics-bind ADDR` says otherwise. Unlike the main port it
has no login, so Prometheus can scrape it as it is: bind it to a trusted interface. It shows counts for the whole
server and per volume, never user names, series names or values; the per-volume gauges name each volume's
directory (`path`), so with a volume per tenant whoever reaches the port sees each tenant's volume path, rows and
bytes on disk. `--metrics-port auto` takes a port the kernel
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
| `tsdb_broken_parts` | gauge | parts found damaged: by a follower (left out of its view), or by a writer whose merge, retention drop or rate measurement found an index changed since its open (a barrier for merges, never merged or dropped itself; the next open refuses it), or whose merge could not decode a granule of the part (payload damaged: a barrier, kept in memory only) |
| `tsdb_pinned_index_bytes` | gauge | heap bytes the listed parts' index copies take with `--pin-index` (about 32 to 51 per granule; 0 without it). Parts already replaced by a merge but still read by a query keep their copy until that query ends, and are not counted |
| `tsdb_logical_bytes` | gauge | what the rows in parts would take uncompressed: rows x (8 + width of the series' type); with `tsdb_disk_bytes` it gives the compression ratio. All the statistics families below are 0 with `--no-stats` |
| `tsdb_ts_bytes`, `tsdb_value_bytes`, `tsdb_index_bytes`, `tsdb_other_bytes` | gauge | where the part bytes go: compressed timestamps, compressed values, granule index entries, and headers, footers, tombstones and the fixed part of each index |
| `tsdb_stats_parts_counted`, `tsdb_stats_parts_listed` | gauge | parts the statistics sweeper has counted, and parts listed: the five numbers above are exact once the two are equal (they differ for a moment after a flush or merge, or for a while after the open of a large database) |
| `tsdb_stats_ring_overflows_total`, `tsdb_stats_cpu_seconds_total` | counter | part events the sweeper's ring dropped (it then compared its records with the part list: still exact, just later), and the CPU time of the sweeper thread |
| `tsdb_failed` | gauge | 1 once the volume is fail-stopped: writes return IO errors and tsdbd exits; alert on it |
| `tsdb_follow_errors_total`, `tsdb_follow_refresh_age_seconds` | counter, gauge | followers only: failed refreshes, and time since the last good one (the age of the follower's refresh, not of the writer's data: a stopped writer leaves it near 0) |
| `tsdb_queries_total` | counter | outermost queries the engine ran, all volumes (a QUERY_MULTI counts once). 0 with `--no-stats`; the `tsdb_query_*_total` families too |
| `tsdb_query_rows_total{source}` | counter | rows queries covered: `decoded` (granules decoded), `stats` (answered from granule statistics), `memtable` |
| `tsdb_query_granules_total{source}` | counter | granules queries covered: `decoded` or `stats` |
| `tsdb_query_bytes_read_total`, `tsdb_query_parts_total`, `tsdb_query_result_rows_total`, `tsdb_query_engine_seconds_total` | counter | compressed bytes decoded, parts touched, rows or buckets returned, wall time inside the engine's queries |
| `tsdb_major_faults_total` | counter | major page faults of the server process (`getrusage`): reads of mapped parts that went to storage |
| `tsdb_requests_total{op}` | counter | requests handled, by operation (`insert`, `query_raw`, `query_buckets`, ...) |
| `tsdb_inserted_rows_total` | counter | rows in successful insert requests |
| `tsdb_multi_series_total` | counter | series answered by many-series requests (QUERY_MULTI, `op="query_multi"` in the request counters) |
| `tsdb_multi_deferred_total` | counter | series of such requests too large to hold ahead of their turn, run again at it (streaming); a high rate next to `tsdb_multi_series_total` means the requests are mostly large series |
| `tsdb_latest_series_total{volume,path,how}` | counter | series QUERY_LATEST (and `tsdb_latest`) answered, per volume: `how="table"` from the latest table, `how="search"` by a search of the parts. A high `search` share means entries still unknown (see `tsdb_latest_unknown`), `WITH_NULLS` or as-of requests, or a volume the budget capped |
| `tsdb_latest_floor_reads_total{volume,path}` | counter | table entries read as unknown below a `tsdb_drop_before` floor (answered by a search) |
| `tsdb_latest_fills_total{volume,path,result}` | counter | the warmer's batches: `stored`, or `refused` (a write section ran after its cut; the next round tries again) |
| `tsdb_latest_unknown{volume,path}` | gauge | table entries still to fill; falls to 0 after a restart as the warmer works. Lookups of these ids are searches until then |
| `tsdb_latest_table_bytes{volume,path}` | gauge | memory the volume's latest table takes (16 bytes a series, in pages) |
| `tsdb_latest_budget_capped{volume,path}` | gauge | 1 when `--latest-cache` refused the volume a page: ids beyond it are answered by a search. Alert on it |
| `tsdb_request_errors_total{code}` | counter | replies with an error status: `io`, `invalid`, `notfound`, ...; `denied` is a request for series or operations the user holds no right on, `auth` a failed login or a request before login |
| `tsdb_logins_total{result}` | counter | login attempts: `ok`; `failed` (a wrong password, an unknown user or a malformed exchange: they are not told apart); `timeout` (not logged in within `--login-timeout`); `refused` (another request, or a message over 4 KiB, before login) |
| `tsdb_query_duration_seconds{op}` | histogram | time a worker spent on a query request (0.5 ms to 30 s), up to handing the reply to the kernel, so a slow client taking a large result adds to it |
| `tsdb_connections`, `tsdb_connections_max` | gauge | open connections on the main port, and the limit |
| `tsdb_connections_accepted_total`, `tsdb_connections_rejected_total` | counter | accepted, and closed at once for the limit |
| `tsdb_workers`, `tsdb_workers_busy` | gauge | worker threads (extras start for slow clients), and those running a request |
| `tsdb_request_memory_bytes`, `tsdb_request_memory_limit_bytes` | gauge | memory of requests received and not yet run (with large receive buffers and packed inserts being unpacked), and `--max-request-memory` |
| `tsdb_metrics_scrapes_total`, `tsdb_metrics_rejected_total` | counter | this endpoint: scrapes answered, requests refused or timed out |

Not exported, although docs/design-latest.md 6.5 names them: the engine keeps no counters for why a lookup searched
(`tsdb_latest_search_total{why}`), for the time lookups waited for a write section (`wait_seconds_total`) or for the
series whose newest row lies more than an hour ahead of the clock (`future_series`). Requests themselves are the
generic `tsdb_requests_total{op="query_latest"}` and `tsdb_query_duration_seconds{op="query_latest"}`.

Useful expressions: `tsdb_logical_bytes / tsdb_disk_bytes` is the compression ratio;
`sum(rate(tsdb_query_rows_total{source="stats"}[5m])) / sum(rate(tsdb_query_rows_total[5m]))` is the share of rows answered without decoding; `rate(tsdb_inserted_rows_total[1m])` is the insert rate;
`histogram_quantile(0.99, sum by (le, op) (rate(tsdb_query_duration_seconds_bucket[5m])))` the p99 query time;
`tsdb_failed == 1` pages someone; `tsdb_follow_refresh_age_seconds > 5` means the follower has stopped refreshing (it does not detect a dead writer);
`tsdb_memtable_rows` growing without bound means flushes are not keeping up;
`rate(tsdb_logins_total{result="failed"}[5m])` well above zero is a client with a wrong password, or someone
guessing (tsdbd's log names the user and the address, at most one line a second).
