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
| `tsdb_build_info{version,build,simd,cpu_level}` | gauge | always 1; `version` is the release (e.g. 0.11.0), `build` the git describe of the build; `cpu_level` is `native`, or the codec level of a portable build |
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
| `tsdb_failed` | gauge | 1 once the volume is fail-stopped: writes return IO errors; alert on it. A root failure, a failure on a volume outside `--health-domain` and not in a cluster, or an unreported failure exits tsdbd; volumes under `--health-domain` recover in place and cluster volumes are quiesced while tsdbd keeps serving other volumes |
| `tsdb_volume_diverged` | gauge | 1 while the volume is DIVERGED (its DIVERGED file, written when a settled copy failed its digest or a cold apply met a part above Z, or an outage reopen that found acknowledged data gone): refused with UNAVAILABLE (DIVERGED), out of a cluster volume's ISR (`LOCAL_FAULT`), until an operator acts (`tsdb DIR volume diverged`; `--clear` is refused on cluster volumes — use `tsdb cluster replica reset` to rebuild the replica); alert on it |
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
| `tsdb_fwd_frames_total{peer}`, `tsdb_fwd_bytes_total{peer}` | counter | cluster only: requests this server forwarded to the member (`peer`, its node id) that leads their volume, and their bytes with the envelope (`--forward`) |
| `tsdb_snapshot_total{event}`, `tsdb_snapshot_bytes_total{dir}`, `tsdb_snapshot_last_seconds` | counter, counter, gauge | cluster only: snapshot transfers of replicated volumes (`event`: asked, sent, received, refused_space, failed, busy, installed, refused_stale: a copy not installed because its P is at or below the receiver's log end or its epoch below the receiver's fence), the bytes of their files sent and received, and the seconds of the last one received from request to last file (STATUS `snap.*`) |
| `tsdb_peer_links{kind,check}` | gauge | cluster only: the peer link connections of this server up now, by link kind (`log`, `bulk`, `fwd`, `fwdq`, `snap`, `chunk`, `chunkb`, `chunkr`) and the frame check they run (`none`, `header`, `full`: `--peer-check`, the stronger end's level; the metadata link keeps its HMAC); STATUS prints the same as `peer links KIND: none N header N full N tcp N rdma N` |
| `tsdb_peer_links_transport{kind,transport}` | gauge | cluster only: the peer link connections of this server up now, by link kind and transport (`tcp` or `rdma`) |
| `tsdb_peer_rdma_fallbacks_total{reason}` | counter | cluster only: RDMA peer link upgrade fallbacks and declines by reason (`not_built`, `off`, `kind`, `subnet`, `pool`, `qp`, `timeout`, `auth`, `qp_error`, `backoff`, `bad`) |
| `tsdb_peer_rdma_credit_stalls_total` | counter | cluster only: times an RDMA peer link write stalled waiting for a peer data credit |
| `tsdb_peer_rdma_rnr_retries_total` | counter | cluster only: RDMA peer link completions failed with receiver-not-ready retry exceeded |
| `tsdb_peer_rdma_qp_errors_total` | counter | cluster only: RDMA peer link queue pair completion or state-transition errors |
| `tsdb_volume_young_margin{volume}`, `tsdb_volume_settled_margin{volume}`, `tsdb_volume_settled_state{volume,state}` | gauge | cluster only, per cluster volume from the metadata copy (design-cluster-data.md E15, E24): the losses an acknowledged row survives now (in-sync replicas minus one; no row for a shared volume, whose candidates see one storage and hold no copies); for an erasure-coded volume the losses its settled objects survive (stripe positions present minus k; negative: settled reads short of positions) and 1 for the state of its settled tier (`state`: ok, unavailable: fewer than k positions present, corrupt: a settled read answered CORRUPT on this server since it started, lost: more than m positions on members declared destroyed). A position counts as present while its member is up in the metadata copy: the members, not their chunk stores, so a member whose store failed or came back empty still counts until the leader reports per-object positions (a later release). A member cut off from the metadata group reports its last copy: read the metadata leader's numbers (STATUS `cluster.applied` says how far each copy is). Alert on a margin of 0 and on any state but ok (STATUS `vol.<n>.young_margin`, `settled_margin`, `tier`). `tsdb_volume_margins_missing` 1: a scrape could not list them (out of memory) |
| `tsdb_snapshot_install_last_seconds` | gauge | cluster only: the last snapshot installed, seconds from the end of its transfer to the volume handed back to be opened (STATUS `snap.install_last_s`) |
| `tsdb_fwd_hop_seconds{peer,quantile}` | summary | cluster only: time from a forward's send to its reply, p50 and p99 of every forward since the server started (cumulative, not a window); STATUS shows the same per peer (`peer.<id>.fwd.*`) |
| `tsdb_chunk_reads_total{peer}`, `tsdb_chunk_read_hedges_total{peer}`, `tsdb_chunk_read_avoids_total{peer}`, `tsdb_chunk_decoded_cells_total{peer}`, `tsdb_chunk_read_avoid{peer}` | counter, counter, counter, counter, gauge | cluster only, per storage peer (design-cluster-data.md E31): chunk cell read requests sent, hedged reads launched when a peer exceeded its hedge timer (`2 * p99` clamped to `[20, 500]` ms, capped at 5% of reads per peer), cell reads decoded around a peer while it was in `read-avoid` (8 timeouts in 10 s -> 30 s avoid), cells decoded from other stripe positions in place of that peer, and 1 while that peer is in `read-avoid` |
| `tsdb_pace_rate_bytes_per_second{dir}`, `tsdb_pace_halved`, `tsdb_pace_bytes_total{dir,class}` | gauge, gauge, counter | cluster only (design-cluster-data.md E13, design-cluster.md §8.4): current effective rebuild/balance pace rate in bytes/s by direction (`send`, `recv`; 0 when unlimited at margin 0), 1 while non-emergency pacing is halved because 1-minute query p99 or WAL sync p99 exceeds twice its 1-hour median, and total bytes paced by direction and margin class (`margin0`, `margin1`, `margin2plus`, `balance`) |

### Local tier metrics (single server, R15)

When a standalone server opens a database with a local settled tier (`TIERS`), `/metrics` also emits four tier families (omitted on standalone servers without a tier, and in cluster mode):

| metric | type | meaning |
|---|---|---|
| `tsdb_tier_bytes{store,use}` | gauge | filesystem bytes of each open local tier store (`store` is the 0-based store index; `use` is `used`, `free` (unprivileged available), or `total`; omitted for DOWN stores). Regular chunk puts stop when integer fill percent (floor of `used * 100 / total`) exceeds 95%; `dist/alerts/tsdb.rules.yml` (`TsdbTierFull7d`) alerts when the 1-day growth slope projects reaching 95% within 7 days |
| `tsdb_tier_settles_waiting` | gauge | number of tiered volumes currently waiting to settle because a tier store is down, above 95% full, or the tier is halted |
| `tsdb_tier_store_state{store}` | gauge | local tier store state (0-based store index): `0` up, `1` missing, `2` other_id, `3` damaged, `4` ioerr, `5` replacing, `6` stalled; `TsdbTierStoreDown` alerts on any DOWN state (`!= 0` and `!= 5`) |
| `tsdb_volume_full{volume}` | gauge | `1` while the tiered volume is FULL (volume filesystem integer fill percent `>= young_high_pct`, 90% default; clears below `young_high_pct - 2`), refusing inserts, appends, and non-empty replaces with `UNAVAILABLE` (`TSDB_WHY_FULL`) while deletes and drops pass; `TsdbInsertsRefusedFull` alerts on `== 1` |

Alert rules for these metrics ship in `dist/alerts/tsdb.rules.yml`.

### Cluster metrics (R14, P4 alerts)

In cluster mode (`tsdbd --cluster`), `server/creport.[ch]` exposes cluster health, Raft, member, per-held-volume, and chunk-store metrics from immutable snapshots taken without holding cluster locks across rendering. Standalone servers (`!cluster_on`) omit all of these families.

Gauges for volumes this server does **not** hold (not listed in the volume's replica set) are **absent**, never emitted as 0.

| metric | type | meaning |
|---|---|---|
| `tsdb_cluster_raft_term` | gauge | current metadata Raft term known to this server |
| `tsdb_cluster_raft_leader` | gauge | 1 while this server is the metadata Raft leader, else 0 |
| `tsdb_cluster_raft_commit_lag` | gauge | metadata Raft entries committed ahead of this server's applied index |
| `tsdb_cluster_raft_quorum` | gauge | 1 while metadata Raft quorum holds from this server's view (leader holds check-quorum lease, or non-leader holds a valid read lease), else 0; alert on 0 |
| `tsdb_cluster_member_state{node}` | gauge | cluster member state in the metadata copy: `0` up, `1` down, `2` draining, `3` out, `4` destroyed |
| `tsdb_cluster_volume_role{volume}` | gauge | per held volume: this server's role: `1` owner (leader), `2` standby (in ISR), `3` follower (out of ISR or learner) |
| `tsdb_cluster_volume_epoch{volume}` | gauge | per held volume: current leadership epoch |
| `tsdb_cluster_volume_isr_size{volume}` | gauge | per held volume: current in-sync replica count |
| `tsdb_cluster_volume_min_isr{volume}` | gauge | per held volume: configured `min_isr` |
| `tsdb_cluster_volume_young_margin{volume}` | gauge | per held volume: young-tier fault-tolerance margin (`isr_size - 1`; omitted for shared volumes); page on `<= 0` |
| `tsdb_cluster_volume_settled_margin{volume}` | gauge | per held EC volume: settled-tier fault-tolerance margin (`present stripe positions - k`; omitted for non-EC volumes); page on `<= 0` |
| `tsdb_cluster_volume_health{volume}` | gauge | per held volume: local health: `0` ok, `1` local_fault (fail-stopped or diverged), `2` domain_fault, `3` damaged (young-part or `.ec` file damage found by scrub) |
| `tsdb_cluster_volume_diverged{volume}` | gauge | per held volume: 1 while the volume is DIVERGED, else 0; page on 1 |
| `tsdb_cluster_volume_lag_positions{volume}` | gauge | per held volume: replication lag in log positions (leader: written minus commit; replica: max(commit, written) minus applied) |
| `tsdb_cluster_volume_lag_ms{volume}` | gauge | per held volume: replication lag duration in milliseconds (leader: commit stall or unacked peer age while `lag_positions > 0`; replica: active missing-file fetch wait while `lag_positions > 0`; 0 when caught up) |
| `tsdb_cluster_volume_unavailable{volume,why}` | gauge | per held volume: 1 while unavailable, labelled by reason (`no_owner`, `diverged`, `recovering`, `unreachable`, `lost`, `stuck`, `nfs_down`, ...); absent while available; page on 1 |
| `tsdb_chunk_store_state{store}` | gauge | per local chunk store (0-based index): `0` up, `1` missing, `2` other_id, `3` damaged, `4` ioerr, `5` replacing, `6` stalled |
| `tsdb_cluster_out_guard_passed_total` | counter | times automatic member OUT proceeded past the mass-OUT guard bound (R9 D4) |
| `tsdb_cluster_transfers_total{event}` | counter | planned leadership transfer events since process start (`started`, `done`, `timed_out`, `cancelled`, `fallback`, `balance`, `drain_timed_out`, `blind`, `ready`, `cuts`) |
| `tsdb_cluster_moves_total{event}` | counter | replica move lifecycle events since process start on the Raft leader (`started`, `done`, `cancelled`) |
| `tsdb_cluster_settles_total{result}` | counter | EC part settle outcomes since process start (`done`, `failed`, `abandoned`) |
| `tsdb_young_scrub_bytes_total{volume}` | counter | per held volume: total bytes verified by young-part and `.ec` scrub since process start (R12) |
| `tsdb_young_scrub_parts_total{volume}` | counter | per held volume: total young and `.ec` parts verified by scrub since process start (R12) |
| `tsdb_young_scrub_damaged_total{volume}` | counter | per held volume: total damaged young or `.ec` parts detected by scrub since process start (R12) |
| `tsdb_young_scrub_age_seconds{volume}` | gauge | per held volume: seconds since the start of the last completed young-part scrub pass, or the first pass start (R12) |
| `tsdb_young_scrub_min_isr_held{volume}` | gauge | per held volume: 1 while a damaged replica stays in the ISR because leaving would drop below `min_isr`, else 0 (R12) |
| `tsdb_cluster_rebuild_bytes_done{volume}` | gauge | per held (or stripe-filling) volume: bytes completed so far by an active snapshot transfer, snapshot install (restarting from 0 as files are verified), or stripe fill on this server (0 when idle) |
| `tsdb_cluster_rebuild_bytes_total{volume}` | gauge | per held (or stripe-filling) volume: total bytes expected by the active snapshot transfer or snapshot install, or bytes discovered so far while a stripe fill's OBJLIST pages are streaming (0 when idle) |
| `tsdb_cluster_rebuild_stalled{volume}` | gauge | per held (or stripe-filling) volume: 1 while an active snapshot or stripe rebuild has made no progress for 15 minutes (design-cluster-data.md §14.4), else 0; alert on 1 |
| `tsdb_settle_backlog_keys{volume}` | gauge | per held volume: partition keys currently due to settle and waiting on this server (0 when caught up or non-leader) |
| `tsdb_settle_backlog_oldest_seconds{volume}` | gauge | per held volume: age in seconds of the oldest waiting partition key past settle eligibility on this server (0 when `tsdb_settle_backlog_keys == 0`); alert above `settle_wait_max_s` |
| `tsdb_cluster_node_full` | gauge | 1 while this server is in FULL backpressure state (`fill_pct >= young_high_pct` with 2-point hysteresis), else 0 |
| `tsdb_cluster_full_refusals_total{volume}` | counter | per held volume: write requests refused with `UNAVAILABLE` (`TSDB_WHY_FULL`) since process start |
| `tsdb_cluster_auto_resets_total{why}` | counter | automatic damage-heal actions and blocking episodes on the Raft leader since process start (`damaged`: `REPLICA_RESET` proposed for a `DAMAGED` non-leader non-ISR replica; `failed`: proposal failed or reset replica failed to recover; `leader_transfer`: planned transfer started for a `DAMAGED` leader; `no_candidate`: blocking episode where a `DAMAGED` leader had no healthy non-DAMAGED ISR member to transfer to; `below_min_isr`: blocking episode where a reset was withheld because remaining healthy non-DAMAGED ISR `< min_isr`) (R12 D8) |
| `tsdb_cluster_heal_blocked{volume,why}` | gauge | on the Raft leader: 1 while automatic damage healing for `volume` is blocked, labelled by reason (`no_candidate`, `below_min_isr`); absent when not blocked; page on 1 (R12 D8) |
| `tsdb_chunk_scrub_bytes_total{store}` | counter | per local chunk store (0-based index): total chunk bytes verified by background scrub since process start (R12) |
| `tsdb_chunk_scrub_chunks_total{store}` | counter | per local chunk store (0-based index): total chunks verified by background scrub since process start (R12) |
| `tsdb_chunk_scrub_damaged_total{store}` | counter | per local chunk store (0-based index): total damaged chunks (bad cell, header, sums, footer, or EIO) detected by scrub since process start (R12) |
| `tsdb_chunk_scrub_age_seconds{store}` | gauge | per local chunk store (0-based index): seconds since the start of the last completed whole chunk scrub pass, or the store's first pass start (R12) |
| `tsdb_chunk_scrub_reports_received_total{volume}` | counter | per held EC volume led by this server: `CHUNK_DAMAGED` reports received from storage servers since process start (R12) |
| `tsdb_disk_class_mismatch` | gauge | number of local `--disk` entries whose runtime `/sys` block-device class detection disagreed with the authoritative class at start (R13) |
| `tsdb_disk_shared_device` | gauge | 1 while this server is running with `--allow-shared-device` (`cluster.allow.shared.device`), else 0 (R13) |

Note on overlapping families: `tsdb_volume_{young_margin,settled_margin,settled_state}` report every volume in this server's metadata copy (including volumes held elsewhere), and `tsdb_volume_diverged{volume,path}` covers local engine volumes (standalone and cluster); the `tsdb_cluster_volume_*`, `tsdb_cluster_rebuild_*`, `tsdb_settle_backlog_*`, and `tsdb_cluster_full_refusals_total` families above report only cluster volumes **held** by this server (plus active stripe fills on storage servers) and carry the fixed names used by P4's cluster alerts. Messages a replica dropped for another set version remain a rate-limited log line only (not a counter).

Not exported, although docs/design-latest.md 6.5 names them: the engine keeps no counters for why a lookup searched
(`tsdb_latest_search_total{why}`), for the time lookups waited for a write section (`wait_seconds_total`) or for the
series whose newest row lies more than an hour ahead of the clock (`future_series`). Requests themselves are the
generic `tsdb_requests_total{op="query_latest"}` and `tsdb_query_duration_seconds{op="query_latest"}`.

R8's chunk-server (`chunksrv_stats` in `server/chunksrv.h`: requests, malformed, puts_ok, puts_refused, put_bytes, reads,
read_bytes, fenced, unsure_refused, source_raises, answer_mismatch), kept-copy rot (`eckeep_copies_lost`,
`eckeep_copies_undigested` in `src/eckeep.h`), and EC GC counters (`ecgc_counters` in `src/ecgc.h`) are internal/test
counters only and are **not** exported on the metrics port yet (R14).

Useful expressions: `tsdb_logical_bytes / tsdb_disk_bytes` is the compression ratio;
`sum(rate(tsdb_query_rows_total{source="stats"}[5m])) / sum(rate(tsdb_query_rows_total[5m]))` is the share of rows answered without decoding; `rate(tsdb_inserted_rows_total[1m])` is the insert rate;
`histogram_quantile(0.99, sum by (le, op) (rate(tsdb_query_duration_seconds_bucket[5m])))` the p99 query time;
`tsdb_failed == 1` pages someone; `tsdb_follow_refresh_age_seconds > 5` means the follower has stopped refreshing (it does not detect a dead writer);
`tsdb_memtable_rows` growing without bound means flushes are not keeping up;
`rate(tsdb_logins_total{result="failed"}[5m])` well above zero is a client with a wrong password, or someone
guessing (tsdbd's log names the user and the address, at most one line a second).
