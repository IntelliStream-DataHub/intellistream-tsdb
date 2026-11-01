# tsdb against ten other databases, 100 M rows

The harness behind the README's "100 million rows" tables: tsdb (through tsdbd and embedded),
ClickHouse, TimescaleDB, QuestDB, InfluxDB 2, InfluxDB 3 Core and Prometheus, each loaded with the
same 100 M rows and asked the same queries, with every answer checked against tsdb's; and DuckDB and
chDB (ClickHouse as a library), embedded like tsdb, compared with tsdb's library in the same process.
The release driver in [`release/`](release) runs all of it unattended and adds Apache IoTDB, TDengine
and VictoriaMetrics, batches of 1, 10 and 100 series per request, and one-core runs (see "Release run").
For compression on real measurements rather than generated data, see [`../hydraulic`](../hydraulic):
a public data set from a hydraulic test rig, with the steps to reproduce its table.

Every server runs in **Podman** (never Docker), with `--network host` and its own listen setting
bound to 127.0.0.1, except IoTDB and TDengine: they bind every address, so they run in their own network
namespace with their ports published on 127.0.0.1 (`-p 127.0.0.1:PORT:PORT`, rootless Podman's pasta
forwarder). The release run measures what that forwarder costs per request on VictoriaMetrics (the same
server and point queries both ways, `p2/vm/vm.pasta.point.*`). Run one system at a time: stop each container before starting the next, and
run nothing else heavy meanwhile.

## What is here

| file | what it does |
|---|---|
| `Makefile` | builds the C programs into `build/dbcmp/` |
| `env.sh` | defaults the scripts share (`DBCMP`, `WORK`, `DATA`, `BIN`, `CLICKHOUSE`) |
| `conv.c` | RowBinary to line protocol (`lp`, `ilp32`), CSV and OpenMetrics (`om`) |
| `spec.py` | the query list every system runs (`spec.tsv`) |
| `mkq.py` | that list in each system's dialect (`q/`) |
| `check.py` | compares a system's answers with tsdb's (`ref/`) |
| `loadlp.c`, `httpc.h` | parallel line protocol loader over HTTP |
| `loadprw.c` | Prometheus remote write loader (protobuf + snappy over HTTP, sharded by series) |
| `qdrive.c` | query load driver, HTTP or PostgreSQL wire protocol |
| `tsdbq.c`, `tsdbstat.c` | the same for tsdbd, the reference answers, and tsdbd's merge state |
| `embq.c` | embedded driver: tsdb's library and DuckDB's C API, load, queries and answers |
| `ch_load.sh`, `ch_bench.sh` | ClickHouse load (with merge wait) and queries via `clickhouse-benchmark` |
| `qbench.sh` | the query runs through `qdrive` |
| `ts_compress.sh` | `compress_chunk` on every TimescaleDB chunk |
| `qdb_wait.sh`, `i2_settle.sh`, `i3_watch.sh` | wait for QuestDB's WAL apply, InfluxDB 2's compaction, InfluxDB 3's snapshot |
| `prom_settle.sh` | wait for Prometheus' head and block compactions |
| `release/` | the release driver `run.sh` (phases, smoke run, checker test), `env.sh` (pinned images), `sys_iot.sh`, `sys_td.sh`, `sys_vm.sh`, `how.py` and `tables.py` (how each result ran), `rel.py` (LTTB and TWA queries and checks), `promlost.py` (acknowledged samples Prometheus lost) |

## Requirements

Podman; gcc and make; libpq's shared library (`libpq.so.5`, package `libpq5` or `libpq`; headers
are not needed); snappy's shared library (`libsnappy.so.1`, package `libsnappy1v5` or `snappy`; no
headers); `psql` (TimescaleDB setup and `check.py ts`); curl, jq, bc; Python 3 (standard library
only); about 32 GB free on the disk under `$DBCMP` (of which the OpenMetrics input `mixed.om` takes
about 5 GB and Prometheus' WAL and blocks about 1.5 GB) and 30 GB of free memory. For DuckDB, its C
library release (below); `make` builds `embq` only when it finds that.

## Setup

Paths are absolute. The scripts read these variables (defaults in `env.sh`):

```sh
cd <repository>
make                                      # tsdb, tsdbd, tsdb-bench, tsdbc-bench, the client library
make -C bench/dbcmp                       # build/dbcmp/{conv,loadlp,loadprw,qdrive,tsdbq,tsdbstat}
export DBCMP=/var/tmp/dbcmp               # every data directory goes here: one disk for all systems
export DATA=$DBCMP/input/mixed.rowbinary
export WORK=$DBCMP/work                   # spec.tsv, q/, ref/, res/
H=$PWD/bench/dbcmp B=$PWD/build/dbcmp T=$PWD/build
mkdir -p $DBCMP/input $WORK/ref $WORK/res
```

### Input and conversion

100 series of 1,000,000 samples, kind = series % 7 (`tools/datasets.h`), 16 bytes per row:

```sh
$T/tsdb-bench --export $DATA --series 100 --points 1000000
for f in lp ilp32 csv om; do $B/conv $f $DATA $DBCMP/input/mixed.$f & done; wait   # 2 min, not part of a load
cd $WORK && python3 -I $H/spec.py $DATA 10000 > spec.tsv
```

Before each load, read its input once (`cat FILE > /dev/null`) so it comes from the page cache,
and run `sync`. After starting a server, check that its ports are on 127.0.0.1 only:
`ss -ltnp | grep -E ':(7070|8123|9000|5434|9100|9103|8812|9109|8086|8181|9090|9091)\b'`.

A smaller sample runs the same way: `tsdb-bench --export $DATA --series 10 --points 1000000` and
`export NSERIES=10` for `spec.py`, `mkq.py`, `check.py` and `tsdbq` (the windows need the 1 M
samples per series).

## tsdb (first: it gives the reference answers)

```sh
rm -rf $DBCMP/tsdb && sync
$T/tsdbd --dir $DBCMP/tsdb --listen 127.0.0.1 --port 7070 &
$T/tsdbc-bench load $DATA --conns 4 && sync -f $DBCMP
$B/tsdbstat settle                        # background merges still running after the load; size
cd $WORK
$B/tsdbq dump spec.tsv 200 > ref/windows.jsonl
$B/tsdbq full 1 5 dump 2> ref/full.jsonl  # whole range, one connection
$B/tsdbq full 16 5                        # the same with the series spread over 16 connections
for c in raw agg1d agg7d point; do
  $B/tsdbq qps spec.tsv $c 1 2 > /dev/null
  for t in 1 32; do $B/tsdbq qps spec.tsv $c $t 15; done
done
$T/tsdbc-bench query --conns 1 --seconds 5; $T/tsdbc-bench query --conns 32 --seconds 5   # cross-check
$B/tsdbstat optimize                      # optional: one part per partition
kill -INT %1
$T/tsdb-bench --dir $DBCMP/tsdb-embedded --threads 4 --keep   # embedded; deletes that directory first
```

The size is the `bytes` of `tsdbstat`, or `du -sb --exclude='retired_*' $DBCMP/tsdb` (replaced
parts stay as `retired_*` for `obsolete_part_lifetime_s`).

Many series in one request (protocol 10), the way the SQL systems get one query for 10 or 100 series:
`--batch K` sends a connection's series K at a time as one QUERY_MULTI (`--batch 1` is one plain request per
series, as without it; `full 1 5 --batch 100` is one request for all 100 series), and a qps class `NAME.bK`
(`raw.b10`, `agg1d.b100`) runs K random windows of NAME, on K series, as one request. `twa` is the
time-weighted average (step, by hour, over the whole range) with the same `--batch`; its answers go to
`ref/twa.jsonl`. Timings are the mean, sd, median, min and max of the repetitions.

```sh
$B/tsdbq full 1 5 --batch 100; $B/tsdbq full 16 5 --batch 7
$B/tsdbq twa 1 5 ref/twa.jsonl; $B/tsdbq twa 1 5 --batch 100
for c in raw agg1d agg7d point; do for k in 10 100; do $B/tsdbq qps spec.tsv $c.b$k 1 15; done; done
```

## ClickHouse 26.8.2.7

```sh
mkdir -p $DBCMP/clickhouse/data
printf '<clickhouse>\n    <listen_host replace="replace">127.0.0.1</listen_host>\n</clickhouse>\n' > $DBCMP/clickhouse/listen.xml
podman run -d --name dbcmp-clickhouse --network host --ulimit nofile=262144:262144 \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  -v $DBCMP/clickhouse/data:/var/lib/clickhouse \
  -v $DBCMP/clickhouse/listen.xml:/etc/clickhouse-server/config.d/zz-listen.xml \
  docker.io/clickhouse/clickhouse-server:26.8.2.7
podman cp dbcmp-clickhouse:/usr/bin/clickhouse $DBCMP/clickhouse-bin   # client and benchmark, same version
export CLICKHOUSE=$DBCMP/clickhouse-bin
$CLICKHOUSE client --host 127.0.0.1 -n -q "
  CREATE DATABASE cmp;
  CREATE TABLE cmp.t100m (series UInt32, ts DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
                          value Float32 CODEC(Gorilla, ZSTD(1)))
  ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY (series, ts)"
$H/ch_load.sh t100m                       # insert, syncfs, merges
cd $WORK && python3 -I $H/mkq.py ch && python3 -I $H/check.py ch 200
$H/ch_bench.sh ch | tee res/ch-bench.txt
$CLICKHOUSE client --host 127.0.0.1 -q "SELECT count(), sum(bytes_on_disk) FROM system.parts WHERE database = 'cmp' AND table = 't100m' AND active"
```

Monthly partitions follow ClickHouse's advice. The daily variant of the older README tables is
`cmp.t100m_day` with `PARTITION BY toDate(ts)` and the system name `chday`. The published run used
an existing server of the same version with the same settings (`tsdb-ch-nvme`).

## TimescaleDB 2.31.0 on PostgreSQL 17

```sh
mkdir -p $DBCMP/timescale
podman run -d --name dbcmp-timescale --network host \
  -v $DBCMP/timescale:/var/lib/postgresql/data -v $DBCMP/input:/data:ro \
  -e POSTGRES_PASSWORD=bench -e TS_TUNE_MEMORY=24GB -e TS_TUNE_NUM_CPUS=32 \
  docker.io/timescale/timescaledb:2.31.0-pg17 postgres -c listen_addresses=127.0.0.1 -c port=5434
export PGPASSWORD=bench; P="psql -h 127.0.0.1 -p 5434 -U postgres"
$P -c "ALTER SYSTEM SET max_wal_size = '16GB'" -c "SELECT pg_reload_conf()"
$P -c "CREATE TABLE t100m (series int NOT NULL, ts timestamptz NOT NULL, value real NOT NULL)" \
   -c "SELECT create_hypertable('t100m', by_range('ts', INTERVAL '1 day'))" \
   -c "ALTER TABLE t100m SET (timescaledb.compress, timescaledb.compress_segmentby = 'series', timescaledb.compress_orderby = 'ts')"
sync; time podman exec dbcmp-timescale timescaledb-parallel-copy \
  -connection "host=127.0.0.1 port=5434 user=postgres password=bench sslmode=disable dbname=postgres" \
  -table t100m -file /data/mixed.csv -workers 16 -batch-size 10000 -columns series,ts,value -disable-direct-compress
$P -Atc "SELECT hypertable_size('t100m')"          # before compression
$H/ts_compress.sh 16
$P -c "VACUUM t100m"; $P -Atc "SELECT hypertable_size('t100m')"
cd $WORK && python3 -I $H/mkq.py ts && python3 -I $H/check.py ts 200
$H/qbench.sh ts pg "host=127.0.0.1 port=5434 user=postgres password=bench dbname=postgres options='-c TimeZone=UTC'"
```

Without `-disable-direct-compress`, `timescaledb-parallel-copy` 0.13 compresses while copying
(about twice as fast); its chunks come out unordered and need the same `ts_compress.sh` pass.

## QuestDB 10.1.0

Its default ports 9000 and 9009 are ClickHouse's, hence 9100 and 9109.

```sh
mkdir -p $DBCMP/questdb
podman run -d --name dbcmp-questdb --network host -v $DBCMP/questdb:/var/lib/questdb \
  -e QDB_HTTP_NET_BIND_TO=127.0.0.1:9100 -e QDB_HTTP_MIN_NET_BIND_TO=127.0.0.1:9103 \
  -e QDB_PG_NET_BIND_TO=127.0.0.1:8812 -e QDB_LINE_TCP_NET_BIND_TO=127.0.0.1:9109 \
  -e QDB_LINE_UDP_ENABLED=false -e QDB_TELEMETRY_ENABLED=false \
  docker.io/questdb/questdb:10.1.0
curl -sG http://127.0.0.1:9100/exec --data-urlencode "query=CREATE TABLE sensors (series SYMBOL CAPACITY 128 CACHE, value FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL"
sync; $B/loadlp $DBCMP/input/mixed.ilp32 127.0.0.1 9100 "/write?precision=ms" "" 100000 8
$H/qdb_wait.sh                            # queryable once the WAL is applied
podman exec dbcmp-questdb sync -f /var/lib/questdb/db
curl -sG http://127.0.0.1:9100/exec --data-urlencode "query=SELECT sum(diskSize) FROM table_partitions('sensors')"
cd $WORK && python3 -I $H/mkq.py qdb && python3 -I $H/mkq.py qdbh && python3 -I $H/check.py qdb 200
$H/qbench.sh qdb pg "host=127.0.0.1 port=8812 user=admin password=quest dbname=qdb"
$H/qbench.sh qdbh http 127.0.0.1 9100 ""  # the HTTP API; within 10% of the PostgreSQL protocol
# variant: an index on the symbol
curl -sG http://127.0.0.1:9100/exec --data-urlencode "query=ALTER TABLE sensors ALTER COLUMN series ADD INDEX"
$H/qdb_wait.sh && FULLS=" " $H/qbench.sh qdb pg "host=127.0.0.1 port=8812 user=admin password=quest dbname=qdb"
```

**To add at the release run: QuestDB with Parquet partitions.** The numbers above are QuestDB's default
native format, which is uncompressed. Its open source version can also convert partitions to compressed
Parquet by hand (`ALTER TABLE ... CONVERT PARTITION TO PARQUET`, or `FORMAT PARQUET` at `CREATE TABLE`;
see https://questdb.com/docs/architecture/storage-engine/). Measure both: load as above, then convert every
partition (check the exact syntax for the version in use; QuestDB does not convert the active partition),
and record the size, the load plus conversion time, the same query classes and the answer check. Report the
two as separate rows ("QuestDB" and "QuestDB, Parquet partitions").

## InfluxDB 2.7.12

The image's setup variables are called `DOCKER_INFLUXDB_INIT_*`; that is only their name. During
the first start the setup step listens on `:9999` (all interfaces) for a few seconds, before any
data, and then serves on 127.0.0.1:8086 only.

```sh
mkdir -p $DBCMP/influxdb2/data $DBCMP/influxdb2/config
podman run -d --name dbcmp-influx2 --network host --memory 24g --memory-swap 24g \
  -v $DBCMP/influxdb2/data:/var/lib/influxdb2 -v $DBCMP/influxdb2/config:/etc/influxdb2 \
  -e DOCKER_INFLUXDB_INIT_MODE=setup -e DOCKER_INFLUXDB_INIT_USERNAME=bench \
  -e DOCKER_INFLUXDB_INIT_PASSWORD=benchbench -e DOCKER_INFLUXDB_INIT_ORG=bench \
  -e DOCKER_INFLUXDB_INIT_BUCKET=cmp -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=benchtoken \
  -e INFLUXD_HTTP_BIND_ADDRESS=127.0.0.1:8086 -e INFLUXD_REPORTING_DISABLED=true \
  -e INFLUXD_STORAGE_COMPACT_FULL_WRITE_COLD_DURATION=1m \
  docker.io/library/influxdb:2.7.12
I() { podman exec dbcmp-influx2 influx "$@" --host http://127.0.0.1:8086 --token benchtoken --org bench; }
BID=$(I bucket list --name cmp --hide-headers | awk '{print $1}')
I v1 dbrp create --db cmp --rp autogen --bucket-id $BID --default   # for InfluxQL
sync; $B/loadlp $DBCMP/input/mixed.lp 127.0.0.1 8086 "/api/v2/write?org=bench&bucket=cmp&precision=ms" \
  $'Authorization: Token benchtoken\r\n' 5000 8 && $H/i2_settle.sh $(date +%s)   # over 10 minutes
podman unshare du -sb $DBCMP/influxdb2/data
cd $WORK && python3 -I $H/mkq.py i2ql && python3 -I $H/mkq.py i2flux
python3 -I $H/check.py i2ql 200 && python3 -I $H/check.py i2flux 200
$H/qbench.sh i2ql http 127.0.0.1 8086 $'Authorization: Token benchtoken\r\n'
$H/qbench.sh i2flux http 127.0.0.1 8086 $'Authorization: Token benchtoken\r\nAccept: application/csv\r\n'
```

Report the faster language per query class. Flux must not share a source between yields
(`data = from(...)` feeding four aggregates): that is not pushed down to storage, runs 30-100x
slower, and the whole-range queries then exhaust memory. `mkq.py` writes independent pipelines.

## InfluxDB 3 Core 3.12.1

```sh
mkdir -p $DBCMP/influxdb3 && podman unshare chown -R 1500:1500 $DBCMP/influxdb3   # the image's user
podman run -d --name dbcmp-influx3 --network host --memory 28g --memory-swap 28g \
  -v $DBCMP/influxdb3:/var/lib/influxdb3 docker.io/library/influxdb:3.12.1-core \
  influxdb3 serve --node-id node0 --object-store file --data-dir /var/lib/influxdb3/data \
  --http-bind 127.0.0.1:8181 --without-auth --query-file-limit 1000000 \
  --exec-mem-pool-size 8GB --file-cache-size 4GB --force-snapshot-mem-size 10GB --disable-telemetry-upload
curl -s -X POST http://127.0.0.1:8181/api/v3/configure/database -H 'Content-Type: application/json' -d '{"db":"cmp"}'
sync; $B/loadlp $DBCMP/input/mixed.lp 127.0.0.1 8181 "/api/v3/write_lp?db=cmp&precision=millisecond" "" 100000 16
$H/i3_watch.sh > $WORK/res/i3-watch.log & # Parquet appears about 1 h later (force-snapshot-max-age)
# after the snapshot (kill the watcher):
podman unshare du -sb $DBCMP/influxdb3/data/node0/*   # dbs/ is Parquet, wal/ the retained WAL
cd $WORK && python3 -I $H/mkq.py i3 && python3 -I $H/check.py i3 200
CLASSES=" " $H/qbench.sh i3 http 127.0.0.1 8181 ""   # whole range: about 45 s per query
FULLS=" " $H/qbench.sh i3 http 127.0.0.1 8181 ""
```

Before the snapshot the data lives in the write buffer: whole-range queries there ran for over 10
minutes, and 32 concurrent 7-day aggregates got the server OOM-killed at 28 GB. A restart replays
the WAL (20 s) and starts the one-hour snapshot timer again. The whole-range queries touch about
100,000 Parquet files, hence `--query-file-limit`; the default was not tried.

## Prometheus 3.15.0

Set up as Prometheus' docs describe it for this data: one metric `sensors` with a label
`series` (like InfluxDB's tag), float64 values (it has no float32; they hold the exact float32
value). `prometheus.yml` here has no scrape jobs (Prometheus would scrape itself into the same
TSDB), `out_of_order_time_window: 700d` and `retention.time: 10y`: the data spans 694 days from
2024-01-01, retention counts back from the newest block, and the window must cover all of it for
remote write (below). Retention also caps the largest compacted block at 10% of it, at most 31 days;
so blocks grow to 486 h (2 h x 3^5). The flags add the remote write receiver, the admin API (only
for the size snapshot) and `--query.max-concurrency=32` (default 20) so that 32 clients run at once.
The image runs as `nobody`, hence the `chown`.

Two loads, each into its own fresh data directory and container:

- **Remote write** (protocol 1.0, protobuf + snappy; the image also accepts 2.0, which mainly saves
  bandwidth through interned labels): `loadprw` sends like Prometheus' own remote write queue,
  series spread over shards (`series % shards`), each shard one request at a time on its own
  connection, so a series' samples arrive in order. 2000 samples per request is Prometheus'
  `max_samples_per_send` default; 16 shards (its default `max_shards` is 50). Requests are encoded
  before the clock starts (as the other loaders send pre-converted files); the loader prints its
  own CPU time while sending, about 0.1 cores at 7 M samples/s, far from a bottleneck.
  The file's order (all series at sample i, then i + 1, with sample steps from 1 ms to 60 s)
  means the 60 s series run 694 days ahead of the 1 ms ones: almost every sample is older than the
  head's newest minus 1 h and goes through the out-of-order path. Head compaction then writes one
  2 h block per 2 h of the span (8,333 blocks), out-of-order compaction thousands more, and
  vertical and leveled compactions merge them: on a 10 M-row sample 29,144 compactions over 78
  minutes, down to 40 blocks; `prom_settle.sh` waits for that. The out-of-order path also cuts
  chunks at 32 samples (`out_of_order_cap_max`), and compaction keeps them: 3.7 times the chunks of
  the backfill (120 samples) and 22% more bytes.
  In some runs it loses acknowledged samples: the first sample of series whose clock jitter puts it just
  before 2024-01-01 00:00 (the 2023 block, before the head's first full block) is answered right after the
  load and gone after its compactions (smoke of 2026-10-29: series 2 and 3, one sample each, still gone
  minutes after the compactions settled; of five more loads of the same file, one lost them, four did
  not). The answer check then runs `release/promlost.py`, which finds the lost samples by bisection and
  checks every series' whole-range count, sum, min and max against the rows without them; if they explain
  every difference, the loss is a result (`loose.txt`, in the tables), not a harness failure.
- **Backfill** (the docs' route for historical data): `promtool tsdb create-blocks-from openmetrics`
  writes blocks from an OpenMetrics file (`conv om`, not part of the load), then a server starts on
  them. `--max-block-duration=744h` follows the docs' advice for long ranges: promtool picks the
  largest compaction range up to that, 486 h, the size the server compacts to, so nothing is left to
  compact. The load time is promtool's plus the server's start until `/-/ready`.

Size: the blocks, with the WAL, the out-of-order WBL and the head's chunks apart; then a snapshot
(admin API), which writes the head too as a block next to hard links of the others, so that every
row is counted once. The backfill leaves nothing outside its blocks.

```sh
P=$DBCMP/prometheus
IMG=docker.io/prom/prometheus:v3.15.0@sha256:efd719c99d83b060d9daefdcf00360461adf279f45ef5391f8d111892118753e   # index digest
mkdir -p $P/data $P/data-bf && cp $H/prometheus.yml $P/ && podman unshare chown 65534:65534 $P/data $P/data-bf
prom() { # NAME PORT DATADIR
  podman run -d --name $1 --network host -v $3:/prometheus -v $P/prometheus.yml:/etc/prometheus/prometheus.yml:ro \
    $IMG --config.file=/etc/prometheus/prometheus.yml --storage.tsdb.path=/prometheus --web.listen-address=127.0.0.1:$2 \
    --web.enable-remote-write-receiver --web.enable-admin-api --query.max-concurrency=32; }

# (a) remote write, settle, size
prom dbcmp-prom 9090 $P/data && until curl -sf 127.0.0.1:9090/-/ready; do sleep 0.1; done
cat $DATA > /dev/null; sync; $B/loadprw $DATA 127.0.0.1 9090 2000 16
time sync -f $P/data   # acknowledged is not durable: see "Fairness notes"
# 78 min for 10 M rows (see above); on 100 M expect several hours: run this under tmux or nohup
$H/prom_settle.sh $(date +%s) | tee $WORK/res/prom-settle.log
podman unshare sh -c "cd $P/data && du -scb 01* | tail -1 && du -sb wal wbl chunks_head"
S=$(curl -s -XPOST 127.0.0.1:9090/api/v1/admin/tsdb/snapshot | jq -r .data.name)
podman unshare du -sb $P/data/snapshots/$S && podman unshare rm -rf $P/data/snapshots/$S
podman stop dbcmp-prom

# (a') remote write with more shards and larger requests, load time only, each into a fresh directory;
# report the best load (+ sync) next to 16 shards, or that 16 matched it (on 10 series at most 10
# shards carry data, so only the 100-series run can tell)
for c in "2000 50" "20000 16" "20000 50"; do
  mkdir -p $P/data-sweep && podman unshare chown 65534:65534 $P/data-sweep
  prom dbcmp-prom-sweep 9090 $P/data-sweep && until curl -sf 127.0.0.1:9090/-/ready; do sleep 0.1; done
  cat $DATA > /dev/null; sync; echo "samples/request, shards: $c"; $B/loadprw $DATA 127.0.0.1 9090 $c
  time sync -f $P/data-sweep
  podman stop dbcmp-prom-sweep && podman rm dbcmp-prom-sweep && podman unshare rm -rf $P/data-sweep
done

# (b) backfill, then a server on the blocks (port 9091, so both can be kept; one runs at a time)
cat $DBCMP/input/mixed.om > /dev/null; sync
time podman run --rm --network none -v $DBCMP/input:/input:ro -v $P/data-bf:/prometheus --entrypoint /bin/promtool $IMG \
  tsdb create-blocks-from openmetrics --max-block-duration=744h -q /input/mixed.om /prometheus
time sync -f $P/data-bf   # promtool fsyncs its blocks itself, so this should be about 0
time (prom dbcmp-prom-bf 9091 $P/data-bf && until curl -sf 127.0.0.1:9091/-/ready; do sleep 0.1; done)
podman unshare sh -c "cd $P/data-bf && du -scb 01* | tail -1 && du -sb wal wbl chunks_head"
```

Queries, PromQL over the HTTP API (`mkq.py` says how each class is written). A range selector
`m[r]` at time t holds the samples in (t - r, t], so the rows of [a, b) are `m[b - a]` at b - 1 ms,
and a bucket [k w, (k + 1) w) is `count_over_time(m[w])` (and `sum_`, `min_`, `max_over_time`) at
(k + 1) w - 1; range selectors return stored samples, so lookback and staleness do not enter. The
whole buckets of a window are one `query_range` with step w; where a window starts or ends inside
a bucket, that bucket is clipped to the window by its own range at a fixed time (`@`), both ends in
one instant query: an aggregate window is two requests, sent one after the other on one
connection and timed as one query (`qdrive` runs lines of requests joined by ` && `). Whole range:
one instant query for count/sum/min/max, and for buckets one `query_range` per step group, cut at
Prometheus' 11,000 points per series (2 requests per group for buckets of 60 samples).

```sh
cd $WORK && python3 -I $H/mkq.py prom
PROM_PORT=9091 python3 -I $H/check.py prom 200 && $H/qbench.sh prom http 127.0.0.1 9091 ""   # backfill
podman stop dbcmp-prom-bf && podman start dbcmp-prom && until curl -sf 127.0.0.1:9090/-/ready; do sleep 0.1; done
python3 -I $H/check.py prom 200 && $H/qbench.sh prom http 127.0.0.1 9090 ""                   # remote write
```

## Apache IoTDB 2.0.11, TDengine 3.4.2.8, VictoriaMetrics 1.154.0 (release driver only)

These three are run only by the release driver (`release/run.sh`, "Release run" below); there are no
hand steps for them. Each has its own file in `release/`, whose header lists every setting that is not
the default and why. The images are pinned by digest in `release/env.sh`. Each runs in its own
container (`rb-iot`, `rb-td`, `rb-vm`) with the memory limit of every server (`SERVER_MEM`, 48 GB), ports published on 127.0.0.1 only.

**Apache IoTDB** (`apache/iotdb:2.0.11-standalone`, table model, `sys_iot.sh`)

- Start: `wal_mode=SYNC` (the write-ahead log is synced before IoTDB says yes, as tsdb does; the default
  syncs once a second), no cap on compaction writes (default 16 MB/s, which would make settling measure
  the cap), a 1 h query timeout, the REST service on 18080. Heap 16 GB, direct memory 4 GB.
- Load: `iotload.py`, IoTDB's own Python client (tablets over its RPC port 6667, the bulk path its docs
  recommend), 100,000 rows per tablet, 8 processes. Needs a Python venv with `iotdb-requirements.txt`
  (`IOT_VENV`).
- Settle: FLUSH, then wait until the data files stay the same for 180 s and no compaction runs.
- Size: `du -sb` of the database's sequence and unsequence directories; the WAL apart.
- Queries: SQL over REST (`/rest/table/v1/query`) through `qdrive`; a batch of K series is one statement.
- Variants: `iot` (Gorilla + LZ4, the default) and `iotz` (Gorilla + ZSTD). Floats are never stored with
  RLE or TS_2DIFF: those round to two decimals.
- `-`: LTTB (no such function). With its default memory settings IoTDB refuses some of the 32-client
  TWA window runs ("not enough memory to execute current fragment instance", code 719; a larger heap or
  operator share did not help, because its estimate grows with the pool). Refused runs are listed in
  `REFUSED.txt` and not timed.

**TDengine** (`tdengine/tsdb:3.4.2.8`, community edition, `sys_td.sh`)

- Start: the database is made with `WAL_LEVEL 2 WAL_FSYNC_PERIOD 0` (synced before it says yes; the
  default only writes the log), `PRECISION 'ms'`. The image's extra services (taosKeeper, taosExplorer,
  taosX) are switched off. REST through taosAdapter on 6041.
- Load: line protocol through taosAdapter (`/influxdb/v1/write`, schemaless), 100,000 lines per request,
  8 connections, the way QuestDB and the InfluxDBs are loaded. TDengine also has a faster bulk path (stmt
  over WebSocket) that is not used here, so its load time may look worse than it could be.
- Settle: the community edition does not compact by itself, so the driver runs FLUSH DATABASE and COMPACT
  DATABASE and waits until no compaction runs and the files stay the same.
- Size: the vnode directories of the database, without the WAL.
- Queries: SQL over REST (`/rest/sql/cmp`) through `qdrive`; a batch of K series is one statement.
- Variants: `td` (the default for FLOAT: bss + LZ4) and `tdz` (ZSTD, level high). Never the lossy `tsz`.
- `-`: LTTB. The hourly LINEAR TWA uses TDengine's `TWA()`; it agrees with tsdb's only to about 1e-8
  relative (float precision). It is checked against a stated bound instead of 1e-9 (`TWA_TOL`, default 1e-6
  relative or 1e-6 absolute), timed only within it, and the bound and the largest difference are printed under
  TDengine's table (`loose.txt`).
- One core: TDengine is pinned to one CPU; its query thread setting (`numOfVnodeQueryThreads 1`)
  deadlocked a batch of 10, so it is left at its default.

**VictoriaMetrics** (`victoriametrics/victoria-metrics:v1.154.0`, single node, `sys_vm.sh`)

- Start: `-retentionPeriod=100y` (the default of one month would delete the 2024 data),
  `-search.disableCache` (no cached answers between runs), query and queue limits raised to 1 h.
- Load: Prometheus remote write (`loadprw`, as in phase 4), 10,000 samples per request, 8 shards.
  VictoriaMetrics says yes from memory and writes to disk within 5 s: `force_flush` and `sync -f` after
  the load are timed apart, as for the other systems that do not sync before saying yes.
- Settle: `force_flush`, `force_merge` of every month, then wait until no merge runs and the files stay
  the same.
- Size: `du -sb` of `data/small`, `data/big` and `data/indexdb`.
- Values: stored as decimal digits (about 12 significant digits); a float32 reads back exactly.
- Queries: PromQL (MetricsQL) over the Prometheus HTTP API, the same queries as Prometheus. A batch of K
  series is K requests, because PromQL cannot give each series its own window in one query.
- `-`: LTTB and every time-weighted average (`integrate()` is a step integral that does not know which
  part of the bucket had data).

## DuckDB 1.5.6, embedded, against tsdb embedded

Both run in the driver's process: tsdb through `build/libtsdb.a`, DuckDB through its C API, with the
same query spec, timing and result handling (`embq.c` describes both sides). DuckDB is the C
library release from GitHub, pinned by version and checksum; nothing is installed. The archive is
data: it is unpacked into its own directory and nothing from it is run except the library the
driver links.

```sh
mkdir -p $DBCMP/duckdb/v1.5.6 && cd $DBCMP/duckdb/v1.5.6
curl -sSLO https://github.com/duckdb/duckdb/releases/download/v1.5.6/libduckdb-linux-amd64.zip
echo "b845005f5132a7d8180057c35e14a7626632258782f871a90861b19c1c03841b  libduckdb-linux-amd64.zip" | sha256sum -c
unzip -q libduckdb-linux-amd64.zip -d lib   # duckdb.h, libduckdb.so (sha256 3f4962cd...b747633)
cd <repository> && make -C bench/dbcmp DUCKDB=$DBCMP/duckdb/v1.5.6/lib   # builds build/dbcmp/embq too
```

Load, settle and size. tsdb takes the file as it is and settles with `tsdb_optimize`. DuckDB gets
one table `(series INTEGER, ts TIMESTAMP_MS, value FLOAT)` without a primary key, filled through
4 Appenders (more were not faster) with the rows sorted by (series, ts) first, so the zone maps of
its row groups prune; the sort is timed and reported apart from the load. A CHECKPOINT follows;
the size is the file plus its `.wal`, if any.

```sh
cd $WORK && mkdir -p dump
rm -rf $DBCMP/tsdb-embq && sync && $B/embq tsdb load $DBCMP/tsdb-embq $DATA 4
rm -f $DBCMP/duckdb/t.duckdb* && sync && $B/embq duckdb load $DBCMP/duckdb/t.duckdb $DATA 4
```

Every answer is checked: both engines write all 40,000 window answers and the whole-range digests,
and `check.py --dumps` compares them (the first line checks the embedded reference against tsdbd's).

```sh
$B/embq tsdb dump $DBCMP/tsdb-embq spec.tsv > dump/tsdb-windows.jsonl
$B/embq tsdb full $DBCMP/tsdb-embq 1 1 dump/tsdb-full.jsonl
$B/embq duckdb dump $DBCMP/duckdb/t.duckdb spec.tsv > dump/duckdb-windows.jsonl
$B/embq duckdb full $DBCMP/duckdb/t.duckdb 1 1 dump/duckdb-full.jsonl
python3 -I $H/check.py --dumps dump/tsdb-windows.jsonl dump/tsdb-full.jsonl ref/windows.jsonl ref/full.jsonl
python3 -I $H/check.py --dumps dump/tsdb-windows.jsonl dump/tsdb-full.jsonl dump/duckdb-windows.jsonl dump/duckdb-full.jsonl
```

Queries: whole range (median of 5 after a warm-up), then the random windows and point lookups at
1 and 32 threads, each thread with its own DuckDB connection and prepared statements.

```sh
$B/embq tsdb full $DBCMP/tsdb-embq 5 1; $B/embq tsdb full $DBCMP/tsdb-embq 5 32
$B/embq duckdb full $DBCMP/duckdb/t.duckdb 5 1
for c in raw agg1d agg7d point; do      # a 3 s warm-up per class, then 15 s at 1 and at 32 threads
  $B/embq tsdb qps $DBCMP/tsdb-embq spec.tsv $c 1 3 > /dev/null
  for t in 1 32; do $B/embq tsdb qps $DBCMP/tsdb-embq spec.tsv $c $t 15; done
done
for c in raw agg1d agg7d point; do
  $B/embq duckdb qps $DBCMP/duckdb/t.duckdb spec.tsv $c 1 3 > /dev/null
  for t in 1 32; do $B/embq duckdb qps $DBCMP/duckdb/t.duckdb spec.tsv $c $t 15; done
done
```

tsdb embedded takes the same `--batch K` for `full` and `twa` and the same `NAME.bK` qps classes, through
`tsdb_query_multi`; `--threads T` caps the threads one call uses (`request_threads`):
`$B/embq tsdb full $DBCMP/tsdb-embq 5 1 --batch 100 --threads 32`.

Variants: `EMBQ_NOPREP=1` sends literal SQL instead of prepared statements (within a few percent of
each other here); `DUCK_THREADS=1` runs DuckDB with one thread per query instead of 32;
`DUCK_MEMORY` sets its memory limit (24GB). `embq duckdb sql DB "EXPLAIN ..."` shows a plan: the
time bounds are cast to `TIMESTAMP_MS` so that both reach the scan as plain table filters.

## LTTB and time-weighted averages

Done in the release driver (`release/`): the plan below is what it runs, with the TWA fixes of the
release study (QuestDB's own `twap()`, TWA per day, LINEAR per hour and over random 1-day and 7-day windows).
Each result's `how.json` and `dash.txt` say which route a system took and why it has `-`.

The README's comparison table gets two more columns, measured like the others and checked against tsdb's
answers: LTTB downsampling (the same 1000 points per series over the whole range) and time-weighted
averages (step method, by hour, over the whole range, with the value carried in at each bucket's start).
For each system use its built-in function where it has one, otherwise its usual window-function recipe in SQL,
otherwise report "-" (not supported), and footnote which route each took:

| system | LTTB | time-weighted average |
|---|---|---|
| ClickHouse | `largestTriangleThreeBuckets` (alias `lttb`; check the pinned version has it) | `lagInFrame`/`leadInFrame` window plus `avgWeighted` |
| TimescaleDB | toolkit `lttb()` (needs the timescaledb toolkit in the image) | toolkit `time_weight()` and `average()` |
| DuckDB, InfluxDB 3 (SQL), QuestDB | window-function SQL, if the dialect supports it | window-function SQL |
| InfluxDB 2, Prometheus | - unless built in by then | - unless built in by then |

LTTB answers must be the same points (first maximum wins on ties), with one exception: where two points of a
bucket have the same triangle area, a system may pick the other one. That happens with quantized values and
jittered timestamps (tank_bar: the next bucket's mean equals the anchor's value exactly, so every point one
0.001 step above or below it has the same area, whatever its time); the double rounding of the mean then decides
the pick, and ClickHouse and chDB sum in another order than tsdb (smoke, series 9: one exact tie and five picks
after it). A series that differs is replayed on the raw rows in exact integer arithmetic along the system's own
picks (`rel.py`, `DATA`): it passes when every pick has the largest area given its anchor, within 1e-12 of the size
of the products the area is computed from, and the check line says how many picks were such alternatives.
Time-weighted averages within a stated tolerance. The twa track's REPORT.md has the ClickHouse query.

## Embedded ClickHouse (chDB)

Done in the release driver's phase 3 (`chdb` and `chdb-lz4`).

DuckDB runs in-process (embq.c), so its whole-range bucket query avoids the network and the serialisation of
1.67 M result rows that the ClickHouse server pays, and ClickHouse's size-optimised codecs (DoubleDelta,
Gorilla, ZSTD) decode one value at a time while DuckDB's lightweight encodings do not. To separate the two:
add chDB (ClickHouse's engine as an in-process library, C API libchdb; pin a release at least two weeks old) to
embq.c next to DuckDB and tsdb, with the same data, queries and answer checks, and the table created twice:
with the server run's codecs and with ClickHouse's default (LZ4). chDB against the ClickHouse server then shows
the cost of the network, Gorilla/DoubleDelta+ZSTD against LZ4 the cost of the codecs. Note how chDB hands back
results through its C API (it may serialise to Arrow or another format in memory; DuckDB returns native
vectors). `clickhouse local` is the other serverless form, but it is a separate process writing its output, so
chDB is the fair embedded counterpart.

## Compression per kind of sensor, one column per database

Done in the release driver's phase 1, for every system including IoTDB, TDengine and VictoriaMetrics.

The README's per-sensor table today compares compression methods (codec-bench). At the release run replace it
with real on-disk sizes: load each sensor data set of tools/datasets.h (1 M readings each, plus random) on its
own into every system (tsdb, ClickHouse, DuckDB, QuestDB native and Parquet, TimescaleDB, InfluxDB 2 and 3,
Prometheus), let each settle (merges, compaction, compression jobs), and record the exact size on disk the same
way as for the main table. One column per database: bytes per reading and the size compared to tsdb (tsdb is 1x; 2.6x means 2.6 times tsdb's
size). Every table of sizes in the README is shown that way, never as a ratio to the raw size. Sizes do
not depend on machine load. The real data of [`../hydraulic`](../hydraulic) (the 17 sensors of the test rig in bar, and the pressures converted
to psi) goes through the same steps, one sensor at a time, with tsdb's background merges settled like the others'
work: tsdb's dictionary codec runs in merges, and DuckDB, Parquet (QuestDB's Parquet partitions, InfluxDB 3) and
ClickHouse's `LowCardinality` have dictionaries too, so data with few distinct values must be in the comparison
before any claim about compression. Remove the containers and data afterwards.

## Release run

`release/run.sh` runs the whole comparison unattended, one phase per command. `release/env.sh` holds the
paths and the pinned images; pull the images first. Every image is pinned by digest, and every release and
image build was at least two weeks old when the run was planned. So the release run uses TimescaleDB 2.30.1,
QuestDB 10.0.1 and InfluxDB 3 Core 3.12.0, not the newer versions in the sections above. Their tags are
rebuilt in place, so a tag alone does not name the image.

```sh
BENCH_LOCK=/path/BENCH.lock bench/dbcmp/release/run.sh prep p1 p2 p3 p4   # or: all
bench/dbcmp/release/run.sh smoke       # every system on tiny data; every answer check must pass
bench/dbcmp/release/run.sh checktest   # the answer checker must catch planted mistakes
```

| phase | what it does | output in `$OUT` (`$DBCMP/results`) |
|---|---|---|
| `prep` | the inputs (not timed): the mixed table, the query list, the batch lists, one file per kind of sensor | `$DBCMP/input`, `$WORK` |
| `p1` | size on disk per kind of sensor, every system and variant, each settled | `p1/<system>.tsv`, `p1/how-<system>.json` |
| `p2` | the 100 M-row table on every server: load, settle, size, answer checks, queries | `p2/<system>/` |
| `p3` | embedded: tsdb, DuckDB, chDB | `p3/` |
| `p4` | Prometheus: backfill and remote write | `p4/` |
| `p5` | 100 billion rows, tsdb and ClickHouse (only when named; not in `all`) | `p5/` |
| `p6` | an extra pass over p2's systems: the TWA batches, and each class's peak memory (only when named) | `p2/<system>/`, `p2/<system>/memsweep/` |

Every phase writes `PHASE n DONE` or `PHASE n FAILED: why` to `driver.log`, and `steps.log` has one line per
step (rc, seconds, how busy the machine was). A phase or system that is done is skipped on the next call;
`FORCE=1` runs it again, `P1_SYSTEMS` / `P2_SYSTEMS` pick systems. Each `<name>.<run>` file is summed up in
`<name>.mean.txt` (mean, sd %, min-max over the runs), and `tables.md` collects every result with a
"How it was run" table built from the `how.json` files (system, version, image digest, settings that are
not the default, durability, loader, query client, connections, what one query is, the one-core method,
warm-ups, runs, and why a cell is `-`). Queries a server refused (out of memory, its own limits) are listed
in `REFUSED.txt` and not timed. A server that dies in a class (the kernel killed it at its memory limit, or it
stopped) is recorded there, started again on the same data, and the class run once more; if it dies again the
class ends (its other runs are skipped). A check that cannot reach the server says `SERVER DOWN` (exit 3), not
"answers differ".

A rerun after a death or a harness fix does not repeat what was measured: `RESUME=1` skips the timed steps that
ended with rc 0 in `steps.log` (loads always run, since the data is gone) and `LOADS_FROM=5` loads once instead of
five times, e.g. `FORCE=1 RESUME=1 LOADS_FROM=5 P2_SYSTEMS=qdb0 release/run.sh p2`.

**One memory limit for every server (`SERVER_MEM`, default 48g).** The release run of 2026-10-29 gave
the containers 24 to 28 GB (QuestDB and ClickHouse none) and tsdbd none, which was not the same test: QuestDB took
57 to 59 GB twice and the kernel's global OOM killer ended it, one variant died of address space (`mmap`) at 564 GB
of mapped files, and TimescaleDB was killed at its 24 GB limit by 32 clients of 100-series batches. Now every
container (`--memory`, no swap) and tsdbd (a `systemd-run --user --scope -p MemoryMax=`) get the same limit, and
a server that outgrows it is killed by its own cgroup, which is a result. `tsdbd-mem.txt` and each container's
`mem.txt` record the peak (page cache included).

Timed steps wait for the CPU governor to be `performance` and for the machine to be idle, and take
`BENCH_LOCK` exclusively; other steps take it shared. Every step has a time limit.

Durations: the smoke run (10 series x 650,000 rows, one run of everything) took about 2 h 15 min on the
Ryzen 9 7950X, most of it waiting for compactions (Prometheus about 30 min, QuestDB's ZSTD conversion,
InfluxDB 3's snapshots) and for other benchmarks holding the lock: phase 1 22 min, phase 2 about 1 h 40 min,
phase 3 2 min, phase 4 33 min. The full run on 100 M rows takes many hours (Prometheus' remote write
alone several); run it under `tmux` or `nohup`.

### TWA in batches, and the extra pass (p6)

The other systems answer a whole-table TWA for all 100 series in one SQL query. tsdb can do the same with one
QUERY_MULTI request (protocol 10), so it is measured that way too: `twa.b100.c1`, `twad.b100.c1` and
`twal.b100.c1` are every series in one request on one connection (`relq KIND 1 1 --batch 100`). The older
`twa.c1` and `twa.c16` rows are one request per series.

The TWA windows (`twa1d`, `twa7d`: 1000 random 1-day and 7-day windows) also come in batches now, like the
aggregate classes: 10 or 100 windows per request, at 1 and 32 clients. tsdb sends one QUERY_MULTI per batch
(`relq qps twa1d.b10 ...`). The SQL systems send one query per batch: the windows' queries joined with UNION ALL,
each with its member number as the first column (`rel.py qb`). Every batch is checked window by window against
tsdb's answers (`rel.py checkb`, 1e-9) before it is timed. A system without a TWA query gets a line in `dash.txt`.
In the tables, q/s of a batch class counts requests for every system (tsdbq's own lines count windows; their
requests per second are added beside them).

Phase 2 ran before these classes existed, and before the servers' memory was sampled. `p6` fills both in without
redoing phase 2. For each system it loads the data once, then:

1. runs the new TWA batch classes `REPS` times, into the system's phase-2 directory, so they appear in its table;
2. runs every other class of phase 2 once (not the per-core runs), with the server's memory sampled, into
   `p2/<system>/memsweep/`. These single runs are not averaged; `tables.md` shows the peak memory per class and
   the peak over the whole sweep under each system's table.

Every safeguard of phase 2 still applies (time limits, deaths and restarts, refusals, the idle check, the lock).

```sh
BENCH_LOCK=/path/BENCH.lock bench/dbcmp/release/run.sh p6               # every system of P2_SYSTEMS
P6_SYSTEMS="ch ts" BENCH_LOCK=/path/BENCH.lock bench/dbcmp/release/run.sh p6
RESUME=1 BENCH_LOCK=/path/BENCH.lock bench/dbcmp/release/run.sh p6      # go on after a stop
```

A system that finished has `p6/<system>.done` and is skipped. After a stop, `RESUME=1` loads the interrupted
system again and skips the steps it already measured; without it, a rerun moves the old `memsweep/` to `old/`.
In `steps.log` the pass's steps start with `p6 `.

How long it takes, from the release run's logs (an upper bound): about 1 h for tsdb, ClickHouse, QuestDB (native),
TimescaleDB and each TDengine variant; 1.3 to 1.6 h for QuestDB's Parquet variants and InfluxDB 2; about 2.2 h for
each IoTDB variant; half an hour for VictoriaMetrics; and up to 7 h for InfluxDB 3 (it waits at least an hour for
its Parquet snapshot). All of them: about 15 to 23 hours.

## Fairness notes

- **Durability at acknowledgement differs.** tsdb, PostgreSQL and both InfluxDBs flush to disk before
  acknowledging; ClickHouse, QuestDB (`cairo.commit.mode=nosync`) and Prometheus (its WAL is synced
  when a 128 MB segment is full) do not. Time the `sync -f` after their loads separately.
- **Background work.** InfluxDB 2's full-compaction wait was cut from 4 h to 1 min so it can be
  measured; its 10-minute cache snapshot wait is the default. TimescaleDB compresses from 16
  sessions, where its policy would use one job. ClickHouse backs off up to 60 s between merge selections, so `ch_settle.sh` calls the table settled only
  when no merge runs and the part count has not changed for 180 s (three times that); the release run also
  measures it again after `OPTIMIZE TABLE FINAL` (`merged.*` rows, like tsdb's). Prometheus' remote write is made for live data, not two years of
  history in one go: almost all of it takes the out-of-order path and leaves thousands of 2 h blocks
  to compact (78 minutes for 10 M rows, hours for 100 M); its backfill (promtool) is the route its
  docs give for history, and is reported next to it.
- **Types.** Values are float32 everywhere except both InfluxDBs and Prometheus, which only have
  float64 (they store the exact float32 value).
- **Indexes.** TimescaleDB has only the default `ts` index on uncompressed chunks, which favours its
  load. QuestDB is measured without and with a symbol index.
- **Input order and limits.** Every system gets the file in its original order (all series at sample
  i, then i+1), not sorted by time. Every server, tsdbd too, runs with one memory limit (`SERVER_MEM`, 48 GB of the 64 GB machine; see
  "Release run"); InfluxDB 3's memory pools are set for 28 GB. Prometheus' remote write gets each series in that
  order too (each series on one shard); its backfill reads the OpenMetrics file in that order.
- **TimescaleDB's `work_mem`.** The server of the aggregate, raw and point queries keeps
  timescaledb-tune's `work_mem`; the timescaledb-ha server of TWA and LTTB gets 256 MB, without which
  TWA's window sorts spill to disk. `work_mem` holds per sort or hash node, and a batch of 100 series
  is 100 `UNION ALL` branches: at 256 MB, 32 clients of 7-day batches took the 64 GB host's memory.
- **DuckDB.** It has no last-write-wins: without a primary key, a second write of a timestamp adds a
  row (tsdb replaces it); with one, every append would pay a key check. Its input is sorted by
  (series, ts) before the load (timed apart) while tsdb takes the file unsorted. It runs with
  threads = 32 and memory_limit = 24GB; tsdb queries use every core too (`query_threads`), so
  "1 thread" means one calling thread for both. Its fsync behaviour on commit was not examined.
- **Batches.** Every system is asked for a single series, a batch of 10 and a batch of 100 (the same
  series for every system, from `batches.tsv`). For tsdbd a batch is one request (protocol 10); for the SQL
  systems one statement of K `UNION ALL` branches, sent as one request; for Prometheus and VictoriaMetrics K
  requests, since PromQL cannot give each series its own window. The time is for the whole batch.
- **One core.** Next to the parallel numbers each system also runs on one core: the server pinned to one CPU
  with `taskset` (rootless Podman cannot use `--cpuset-cpus` here) and its own thread setting where it has
  one (tsdbd `--query-threads 1`, ClickHouse `max_threads=1`, TimescaleDB without parallel workers, IoTDB
  one query thread, VictoriaMetrics `GOMAXPROCS=1`), the client on another CPU. Servers that size their thread
  pools at start are started again for this with those pools at one thread: QuestDB (every worker pool, and a
  JVM that sees one CPU), InfluxDB 2 (`GOMAXPROCS=1`), InfluxDB 3 (`--num-datafusion-threads 1`), IoTDB and
  VictoriaMetrics. TDengine is only pinned (its query-thread setting deadlocked). `how.json` says which.
- **Timing.** ClickHouse is timed over HTTP by `qdrive`, like the others, with its own server latency from
  `system.query_log` beside it ("server only"), not by 1/QPS. tsdb is
  measured both as loaded and after its merges settled. Every number is the mean of at least 5 runs, with
  sd and min-max.
- **The three added systems.** IoTDB and TDengine sync their WAL before they say yes (set that way; it is
  not their default), as tsdb does; VictoriaMetrics says yes from memory, like ClickHouse and QuestDB, and
  its flush after the load is timed apart. TDengine is loaded through line protocol, not its faster
  WebSocket path. IoTDB refuses some heavy 32-client queries with its default memory settings; those are
  reported as refused.
- **Clients.** tsdbd uses the C client, the others `qdrive` over HTTP or libpq (in the release run
  ClickHouse too; the older tables above used `clickhouse-benchmark`). InfluxDB 3's Flight SQL was not tried. PromQL cannot clip a
  bucket to a window that starts or ends inside it, so Prometheus answers an aggregate window with
  two requests (whole buckets, then the clipped ends), timed together as one query; its whole-range
  bucket queries are 4 or 8 `query_range` requests (11,000 points per series at most), each
  limited to its step group's data span (± 1 s), because `query_range` evaluates every step, empty
  or not, where the others take fixed bounds or none. Its JSON answers are larger than the others'
  (buckets of 60 samples: about 11 MB per series). VictoriaMetrics gets MetricsQL's `aggr_over_time`, which
  computes count, sum, min and max in one pass, and up to 30,000 points per request (its default limit).
- **Data shape.** The timestamps come from `tools/datasets.h`: every kind has its own interval and clock jitter
  (hdd_temp 60 s and vibration 1 ms exactly regular, cpu_temp 1 s +-20 ms, tank_bar and tank_psi 10 s +-50 ms,
  flow_rate, flow_total and random 1 s +-5 ms), so no two timestamps of a series collide. Every answer is checked against tsdb's, but only on this data, which has no duplicate
  timestamps, no nulls and no gap longer than the narrowest bucket (`prep.sh` checks it with `shape` and stops
  otherwise). Some TWA recipes (QuestDB's `twap()` with `SAMPLE BY`, the segment recipes of `rel.py`) would
  be wrong on gaps that span buckets, and last-write-wins is not exercised for any system.

## Cleaning up

```sh
podman stop dbcmp-clickhouse dbcmp-timescale dbcmp-questdb dbcmp-influx2 dbcmp-influx3 dbcmp-prom dbcmp-prom-bf
podman rm dbcmp-clickhouse dbcmp-timescale dbcmp-questdb dbcmp-influx2 dbcmp-influx3 dbcmp-prom dbcmp-prom-bf
podman unshare rm -rf $DBCMP           # the containers' files belong to mapped users (and DuckDB's download)
```
