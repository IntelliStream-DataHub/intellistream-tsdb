#!/usr/bin/env python3
"""How each result was run: one how.json per system and phase, written by run.sh when the system's step ends.

  python3 -I how.py write PHASE SYS OUT    OUT/p<PHASE>/<dir>/how.json (phase 2) or OUT/p<PHASE>/how-<SYS>.json
                                           (phases 1, 3, 4: one directory holds every system's files)
  python3 -I how.py check FILE...          every required field present and non-empty (exit 1 otherwise)

The static part (settings, durability, loader, query client, what one query is, per-core method) is SYSTEMS below:
the facts behind each line are in run.sh, sys_*.sh and bench/dbcmp/README.md, and a change to how a system runs
changes its line here in the same commit. Written at run time: the image with its digest (podman image inspect),
the tsdb tree and commit, REPS, SECS, batch sizes, smoke or not, and the '-' reasons (dash.txt) and refusals
(REFUSED.txt) of that run. tables.py reads these files and fails on a missing field.
"""
import json, os, subprocess, sys

REQUIRED = ["system", "version", "image", "settings", "durability", "loader", "query_client", "connections",
            "one_query", "per_core", "warmups", "runs", "dash"]

QDRIVE = "qdrive (C, one connection per client thread; times each request from send to the last byte of the answer)"
BATCH_SQL = ("a batch of K series is one SQL statement: K UNION ALL branches tagged by a member column "
             "(mkbatch.py), sent as one request")
PIN = ("the server's process tree pinned to CPU {cpu} by taskset (rootless Podman has no cpuset controller), "
       "the client on CPU {ccpu}")
WARM_SRV = ("whole range: one warm-up run, then REPS runs of the one query; random-window classes: 3 s warm-up "
            "on 1 connection, then REPS runs of SECS s per connection count; LTTB/TWA: one warm-up, then REPS runs")

SYSTEMS = {
    "tsdb": dict(
        system="tsdb (tsdbd)", image_env=None,
        settings=["defaults (wal_sync_ms 0, 1-day partitions, 8192-row granules)", "--listen 127.0.0.1 --port 0 "
                  "--port-file", "also measured after merges (tsdb_optimize: one part per partition)"],
        durability="WAL fsynced before every acknowledgement (group commit); large inserts as parts, fsynced",
        loader="tsdbc-bench load: RowBinary input, 4 connections, compressed packed inserts (protocol 10)",
        query_client="tsdbq (C client library, tsdb's binary protocol over TCP; protocol 10)",
        connections="random-window classes 1 and 32; whole range 1 connection (and per series on 16); TWA/LTTB 1",
        one_query="one request per series; a batch of K series is one QUERY_MULTI request with K series",
        per_core="tsdbd --query-threads 1 pinned to CPU {cpu} by taskset, tsdbq on CPU {ccpu}",
    ),
    "ch": dict(
        system="ClickHouse (server)", image_env="CH_IMG",
        settings=["listen_host 127.0.0.1; server defaults otherwise", "MergeTree PARTITION BY toYYYYMM(ts) ORDER BY "
                  "(series, ts); DoubleDelta / Gorilla + ZSTD(1)"],
        durability="not fsynced at acknowledgement (fsync_after_insert 0, the default); syncfs after the load, timed",
        loader="ch_load.sh: one RowBinary INSERT through clickhouse client (native protocol); merges settled",
        query_client="clickhouse-benchmark (native protocol); times from the server's query_log (query_duration_ms), "
                     "not 1/QPS",
        connections="random-window classes 1 and 32; whole range 1 query on max_threads 32 (default)",
        one_query="one SQL statement per series; " + BATCH_SQL,
        per_core="max_threads=1; " + PIN,
    ),
    "qdb": dict(
        system="QuestDB (native partitions)", image_env="QDB_IMG",
        settings=["every port bound to 127.0.0.1", "PARTITION BY DAY WAL, series SYMBOL", "telemetry off"],
        durability="not fsynced at acknowledgement (cairo.commit.mode nosync, the default); WAL applied and sync "
                   "after the load, timed",
        loader="loadlp: InfluxDB line protocol over HTTP /write, 100000 lines per request, 8 connections",
        query_client=QDRIVE + "; PostgreSQL wire protocol (8812)",
        connections="random-window classes 1 and 32; whole range 1",
        one_query="one SQL statement per series; " + BATCH_SQL,
        per_core=PIN + " (QuestDB has no single query-thread knob)",
    ),
    "ts": dict(
        system="TimescaleDB", image_env="TS_IMG",
        settings=["listen_addresses 127.0.0.1", "timescaledb-tune 24GB / 32 CPUs", "work_mem 256MB (default 6 MB "
                  "spills TWA's window sorts)", "max_wal_size 16GB", "/dev/shm 8 GB (Podman's default 64 MB fails parallel queries)", "hypertable, 1-day chunks, compressed "
                  "(segmentby series, orderby ts)", "TWA and LTTB of the toolkit run in the timescaledb-ha image"],
        durability="fsync at commit (synchronous_commit on, the default)",
        loader="timescaledb-parallel-copy: CSV, 16 workers, batches of 10000 rows; then compression of every chunk",
        query_client=QDRIVE + "; PostgreSQL wire protocol",
        connections="random-window classes 1 and 32; whole range 1 query, and per series on 16 connections",
        one_query="one SQL statement per series; " + BATCH_SQL,
        per_core="max_parallel_workers_per_gather=0; " + PIN,
    ),
    "i2": dict(
        system="InfluxDB 2 (InfluxQL and Flux)", image_env="I2_IMG",
        settings=["http-bind-address 127.0.0.1:8086", "storage-compact-full-write-cold-duration 1m",
                  "container memory 24g", "settle: bench/dbcmp/i2_settle.sh"],
        durability="WAL fsynced before acknowledgement (the default)",
        loader="loadlp: line protocol over HTTP /api/v2/write, 5000 lines per request, 8 connections",
        query_client=QDRIVE + "; HTTP (InfluxQL /query and Flux /api/v2/query)",
        connections="random-window classes 1 and 32; whole range 1",
        one_query="one query per series; a batch of K: InfluxQL K statements joined by ';' in one request, Flux K "
                  "yields in one request",
        per_core=PIN + " (no query-thread knob)",
    ),
    "i3": dict(
        system="InfluxDB 3 Core", image_env="I3_IMG",
        settings=["--http-bind 127.0.0.1:8181 --without-auth", "--object-store file", "--exec-mem-pool-size 8GB "
                  "--file-cache-size 4GB --force-snapshot-mem-size 10GB --query-file-limit 1000000",
                  "container memory 28g", "queried once Parquet is written (snapshot settled)"],
        durability="acknowledged once the WAL file of the flush interval (1 s) is written to the object store "
                   "(local files); fsync not verified here",
        loader="loadlp: line protocol over HTTP /api/v3/write_lp, 100000 lines per request, 16 connections",
        query_client=QDRIVE + "; HTTP SQL (/api/v3/query_sql)",
        connections="random-window classes 1 and 32; whole range 1",
        one_query="one SQL statement per series; " + BATCH_SQL,
        per_core=PIN + " (no query-thread knob)",
    ),
    "iot": dict(
        system="Apache IoTDB (table model)", image_env="IOT_IMG",
        settings=["wal_mode=SYNC", "compaction_write_throughput_mb_per_sec=0", "query_timeout_threshold=3600000",
                  "REST on 18080 published on 127.0.0.1 only (pasta)", "heap -Xmx16G, direct 4G, container 24g",
                  "float encoding GORILLA, compressor LZ4 (default)"],
        durability="WAL fsynced before acknowledgement (wal_mode SYNC; default ASYNC)",
        loader="iotload.py: Python TableSession tablets over the native RPC (6667), 100000 rows per tablet, 8 "
               "processes",
        query_client=QDRIVE + "; REST /rest/table/v1/query",
        connections="random-window classes 1 and 32; whole range 1",
        one_query="one SQL statement per series; " + BATCH_SQL,
        per_core="a restart with query_thread_count=1, degree_of_query_parallelism=1 and -XX:ActiveProcessorCount=1; " + PIN,
    ),
    "td": dict(
        system="TDengine (community)", image_env="TD_IMG",
        settings=["CREATE DATABASE ... WAL_LEVEL 2 WAL_FSYNC_PERIOD 0 PRECISION 'ms'", "taosKeeper, taosExplorer, "
                  "taosX off", "REST on 6041 published on 127.0.0.1 only", "value FLOAT: ENCODE bss COMPRESS lz4 "
                  "LEVEL medium (default)", "container memory 24g"],
        durability="WAL fsynced before acknowledgement (WAL_LEVEL 2, WAL_FSYNC_PERIOD 0; default WAL_LEVEL 1)",
        loader="loadlp: line protocol through taosAdapter /influxdb/v1/write (schemaless), 100000 lines per "
               "request, 8 connections (TDengine's faster stmt/WebSocket path is not used)",
        query_client=QDRIVE + "; REST /rest/sql (taosAdapter)",
        connections="random-window classes 1 and 32; whole range 1",
        one_query="one SQL statement per series; " + BATCH_SQL,
        per_core=PIN + " (numOfVnodeQueryThreads 1 deadlocked a 10-branch batch: pinned only)",
    ),
    "vm": dict(
        system="VictoriaMetrics (single node)", image_env="VM_IMG",
        settings=["-retentionPeriod=100y", "-search.disableCache", "-search.maxQueryDuration=3600s "
                  "-search.maxQueueDuration=3600s", "-precisionBits 64 (default; ~12 significant digits)",
                  "port 8428 on 127.0.0.1"],
        durability="acknowledged from memory (flushed within 5 s); force_flush and sync after the load, timed",
        loader="loadprw: Prometheus remote write, 10000 samples per request, 8 shards",
        query_client=QDRIVE + "; Prometheus HTTP API (MetricsQL)",
        connections="random-window classes 1 and 32; whole range 1",
        one_query="one HTTP query per series; a batch of K is K requests joined in one qdrive line (no multi-series "
                  "query with per-series windows in PromQL)",
        per_core="GOMAXPROCS=1 and -search.maxWorkersPerQuery=1; " + PIN,
    ),
    "prom": dict(
        system="Prometheus", image_env="PROM_IMG",
        settings=["--web.listen-address 127.0.0.1", "--web.enable-remote-write-receiver", "--query.max-concurrency=32", "bench/dbcmp/prometheus.yml"],
        durability="remote write: acknowledged from the head (WAL written, not fsynced per request); backfill: "
                   "blocks written by promtool, then sync",
        loader="phase 4: promtool tsdb create-blocks-from openmetrics (backfill) and loadprw remote write",
        query_client=QDRIVE + "; Prometheus HTTP API (PromQL)",
        connections="random-window classes 1 and 32; whole range 1",
        one_query="one HTTP query per series",
        per_core="not measured",
    ),
    "duckdb": dict(
        system="DuckDB (embedded)", image_env=None,
        settings=["threads 32, memory_limit 24GB", "table (series INTEGER, ts TIMESTAMP_MS, value FLOAT), no key",
                  "rows sorted by series and time before loading (not counted)"],
        durability="CHECKPOINT after the load (the WAL is fsynced at commit)",
        loader="embq duckdb load: 4 appenders",
        query_client="embq / duckq: the DuckDB C API in the same process (prepared statements)",
        connections="random-window classes 1 and 32 threads; whole range 1 query on 32 threads",
        one_query="one SQL statement per series",
        per_core="not measured",
    ),
    "tsdb-emb": dict(
        system="tsdb (embedded)", image_env=None,
        settings=["defaults (wal_sync_ms 0)"],
        durability="fsync on: each insert batch durable before tsdb_insert returns",
        loader="embq tsdb load: 4 threads, batches of 1 M rows",
        query_client="embq / relemb: the tsdb C API in the same process",
        connections="random-window classes 1 and 32 threads; whole range 1 and 32 threads",
        one_query="one call per series",
        per_core="not measured",
    ),
    "chdb": dict(
        system="chDB (embedded ClickHouse)", image_env=None,
        settings=["DoubleDelta / Gorilla + ZSTD(1), as the ClickHouse server"],
        durability="not fsynced at insert (ClickHouse default)",
        loader="chq load: INSERT ... SELECT FROM file() of the RowBinary input",
        query_client="chq: the chDB C API in the same process, one connection per thread",
        connections="random-window classes 1 and 32 threads; whole range 1 query",
        one_query="one SQL statement per series",
        per_core="not measured",
    ),
}
SYSTEMS["qdb-parquet"] = dict(SYSTEMS["qdb"], system="QuestDB (Parquet partitions, LZ4_RAW)",
                              settings=SYSTEMS["qdb"]["settings"] + ["ALTER TABLE CONVERT PARTITION TO PARQUET "
                                                                     "(codec LZ4_RAW, QuestDB's default)"])
SYSTEMS["qdb-parquet-zstd"] = dict(SYSTEMS["qdb"], system="QuestDB (Parquet partitions, ZSTD)",
                                   settings=SYSTEMS["qdb"]["settings"] + ["CONVERT PARTITION TO PARQUET with "
                                                                          "codec ZSTD level QDB_ZSTD_LEVEL (22)"])
SYSTEMS["iot-zstd"] = dict(SYSTEMS["iot"], system="Apache IoTDB (table model, ZSTD)",
                           settings=[s for s in SYSTEMS["iot"]["settings"] if not s.startswith("float")] +
                           ["compressor=ZSTD, float encoding GORILLA"])
SYSTEMS["td-zstd"] = dict(SYSTEMS["td"], system="TDengine (community, ZSTD)",
                          settings=[s for s in SYSTEMS["td"]["settings"] if not s.startswith("value")] +
                          ["value FLOAT: COMPRESS 'zstd' LEVEL 'high' (never the lossy tsz)"])
SYSTEMS["chdb-lz4"] = dict(SYSTEMS["chdb"], system="chDB (embedded ClickHouse, default LZ4)",
                           settings=["default codecs (LZ4)"])
SYSTEMS["duck"] = SYSTEMS["duckdb"]

# run.sh's system names -> SYSTEMS key (phase 2's names are also its result directories)
ALIAS = {"qdb0": "qdb", "qdb1": "qdb-parquet", "qdb2": "qdb-parquet-zstd", "qdbz": "qdb-parquet-zstd",
         "iotz": "iot-zstd", "tdz": "td-zstd"}


def image(env):
    """IMAGE@digest and the version from its tag."""
    ref = os.environ.get(env, "")
    if not ref:
        return "unset (%s)" % env, "unknown"
    name = ref.split("@")[0]
    tag = name.rsplit(":", 1)[1] if ":" in name.rsplit("/", 1)[-1] else "latest"
    try:
        dig = subprocess.run(["podman", "image", "inspect", "--format", "{{.Digest}}", ref], capture_output=True,
                             text=True, timeout=60).stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        dig = ""
    if "@" not in ref and dig:
        ref = "%s@%s" % (ref, dig)
    return ref, tag


def tsdb_version():
    t = os.environ.get("T", "")
    tree = os.environ.get("TSDB_TREE", "")
    try:
        v = subprocess.run([os.path.join(t, "tsdbd"), "--version"], capture_output=True, text=True,
                           timeout=10).stdout.strip()
        c = subprocess.run(["git", "-C", tree, "rev-parse", "--short", "HEAD"], capture_output=True, text=True,
                           timeout=10).stdout.strip()
    except OSError:
        return "unknown", "unknown"
    return v or "unknown", "%s at %s" % (tree, c or "?")


def lines(path):
    try:
        return [l.rstrip("\n") for l in open(path) if l.strip()]
    except OSError:
        return []


def write(phase, sysname, out):
    key = ALIAS.get(sysname, sysname)
    if key not in SYSTEMS:
        sys.exit("how.py: no description of system %r" % sysname)
    s = dict(SYSTEMS[key])
    cpu, ccpu = os.environ.get("PERCORE_CPU", "2"), os.environ.get("PERCORE_CLIENT_CPU", "3")
    s["per_core"] = s["per_core"].format(cpu=cpu, ccpu=ccpu)
    env = s.pop("image_env")
    if env:
        s["image"], s["version"] = image(env)
    elif key in ("tsdb", "tsdb-emb"):
        s["version"], s["image"] = tsdb_version()
    elif key in ("duckdb", "duck"):
        s["version"], s["image"] = "1.5.6", "libduckdb-linux-amd64.zip %s sha256 %s" % (
            os.environ.get("DUCKDB_DIR", "?"), os.environ.get("DUCKDB_SHA", "?"))
    else:
        s["version"], s["image"] = "chdb-core 26.9.0", "linux-x86_64-libchdb.tar.gz %s sha256 %s" % (
            os.environ.get("CHDB_DIR", "?"), os.environ.get("CHDB_SHA", "?"))
    ph = "p%s" % phase
    if phase == "2":
        d = os.path.join(out, ph, key if key != "duck" else "duckdb")
        f = os.path.join(d, "how.json")
    else:
        d = os.path.join(out, ph)
        f = os.path.join(d, "how-%s.json" % key)
    if phase == "1":
        s["query_client"] = s["connections"] = s["one_query"] = s["per_core"] = s["warmups"] = \
            "not applicable (phase 1 measures sizes only)"
    else:
        s.setdefault("warmups", WARM_SRV)
    s["key"], s["phase"] = key, int(phase)
    s["runs"] = "REPS=%s runs per measurement (mean, sd, min-max in *.mean.txt); SECS=%s s per random-window run; " \
                "batches of %s series" % (os.environ.get("REPS", "?"), os.environ.get("SECS", "?"),
                                          os.environ.get("KS", "1 10 100"))
    s["smoke"] = os.environ.get("SMOKE", "0") == "1"
    dash = {}
    for l in lines(os.path.join(d, "dash.txt")) if phase == "2" else []:
        k, _, why = l.partition(":")
        dash[k.strip()] = why.strip()
    s["dash"] = dash or {"none": "every query kind ran"}
    s["refused"] = lines(os.path.join(d, "REFUSED.txt")) if phase == "2" else []
    os.makedirs(d, exist_ok=True)
    with open(f, "w") as o:
        json.dump(s, o, indent=1, sort_keys=True)
        o.write("\n")
    check([f])
    print(f)


def check(files):
    bad = 0
    for f in files:
        h = json.load(open(f))
        miss = [k for k in REQUIRED if k not in h or h[k] in ("", None, [], {})]
        if miss:
            print("%s: missing %s" % (f, ", ".join(miss)), file=sys.stderr)
            bad += 1
    if bad:
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) == 5 and sys.argv[1] == "write":
        write(*sys.argv[2:5])
    elif len(sys.argv) > 2 and sys.argv[1] == "check":
        check(sys.argv[2:])
    else:
        sys.exit(__doc__)
