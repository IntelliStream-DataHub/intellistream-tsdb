# tsdb against five other databases, 100 M rows

The harness behind the README's "100 million rows, six databases" tables: tsdb (through tsdbd and
embedded), ClickHouse, TimescaleDB, QuestDB, InfluxDB 2 and InfluxDB 3 Core, each loaded with the
same 100 M rows and asked the same queries, with every answer checked against tsdb's.

Every server runs in **Podman** (never Docker), with `--network host` and its own listen setting
bound to 127.0.0.1. Run one system at a time: stop each container before starting the next.

## What is here

| file | what it does |
|---|---|
| `Makefile` | builds the C programs into `build/dbcmp/` |
| `env.sh` | defaults the scripts share (`DBCMP`, `WORK`, `DATA`, `BIN`, `CLICKHOUSE`) |
| `conv.c` | RowBinary to line protocol (`lp`, `ilp32`) and CSV |
| `spec.py` | the query list every system runs (`spec.tsv`) |
| `mkq.py` | that list in each system's dialect (`q/`) |
| `check.py` | compares a system's answers with tsdb's (`ref/`) |
| `loadlp.c`, `httpc.h` | parallel line protocol loader over HTTP |
| `qdrive.c` | query load driver, HTTP or PostgreSQL wire protocol |
| `tsdbq.c`, `tsdbstat.c` | the same for tsdbd, the reference answers, and tsdbd's merge state |
| `ch_load.sh`, `ch_bench.sh` | ClickHouse load (with merge wait) and queries via `clickhouse-benchmark` |
| `qbench.sh` | the query runs through `qdrive` |
| `ts_compress.sh` | `compress_chunk` on every TimescaleDB chunk |
| `qdb_wait.sh`, `i2_settle.sh`, `i3_watch.sh` | wait for QuestDB's WAL apply, InfluxDB 2's compaction, InfluxDB 3's snapshot |

## Requirements

Podman; gcc and make; libpq's shared library (`libpq.so.5`, package `libpq5` or `libpq`; headers
are not needed); `psql` (TimescaleDB setup and `check.py ts`); curl, jq, bc; Python 3 (standard
library only); about 25 GB free on the disk under `$DBCMP` and 30 GB of free memory.

## Setup

Paths are absolute. The scripts read these variables (defaults in `env.sh`):

```sh
cd <repository>
make                                      # tsdb, tsdbd, tsdb-bench, tsdbc-bench, the client library
make -C bench/dbcmp                       # build/dbcmp/{conv,loadlp,qdrive,tsdbq,tsdbstat}
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
for f in lp ilp32 csv; do $B/conv $f $DATA $DBCMP/input/mixed.$f & done; wait   # 2 min, not part of a load
cd $WORK && python3 -I $H/spec.py $DATA 10000 > spec.tsv
```

Before each load, read its input once (`cat FILE > /dev/null`) so it comes from the page cache,
and run `sync`. After starting a server, check that its ports are on 127.0.0.1 only:
`ss -ltnp | grep -E ':(7070|8123|9000|5434|9100|9103|8812|9109|8086|8181)\b'`.

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

## Fairness notes

- **Durability at acknowledgement differs.** tsdb, PostgreSQL and both InfluxDBs flush to disk before
  acknowledging; ClickHouse and QuestDB (`cairo.commit.mode=nosync`) do not. Time the `sync -f`
  after their loads separately.
- **Background work.** InfluxDB 2's full-compaction wait was cut from 4 h to 1 min so it can be
  measured; its 10-minute cache snapshot wait is the default. TimescaleDB compresses from 16
  sessions, where its policy would use one job. ClickHouse keeps merging lazily long after
  `ch_load.sh` calls it settled.
- **Types.** Values are float32 everywhere except both InfluxDBs, which only have float64 (they
  store the exact float32 value).
- **Indexes.** TimescaleDB has only the default `ts` index on uncompressed chunks, which favours its
  load. QuestDB is measured without and with a symbol index.
- **Input order and limits.** Every system gets the file in its original order (all series at sample
  i, then i+1), not sorted by time. InfluxDB 2 has a 24 GB and InfluxDB 3 a 28 GB container limit;
  InfluxDB 3's memory pools are set for that.
- **Clients.** ClickHouse uses its own native client (`clickhouse-benchmark`), tsdbd the C client,
  the others `qdrive` over HTTP or libpq. InfluxDB 3's Flight SQL was not tried.

## Cleaning up

```sh
podman stop dbcmp-clickhouse dbcmp-timescale dbcmp-questdb dbcmp-influx2 dbcmp-influx3
podman rm dbcmp-clickhouse dbcmp-timescale dbcmp-questdb dbcmp-influx2 dbcmp-influx3
podman unshare rm -rf $DBCMP           # the containers' files belong to mapped users
```
