# Paths, pins and knobs of the release comparison; sourced by run.sh and prep.sh. Every one can be overridden.
#   TSDB_TREE  a built tsdb tree: build/tsdbd, build/tsdbc-bench, build/tsdb-bench, build/dbcmp/{tsdbq,tsdbstat,embq}
#              (default: this repository; the multi-series run points it at a detached worktree of that branch)
#   BIN        this directory's tools and bench/dbcmp's (qdrive, loadlp, relq, relemb, chq; make -C bench/dbcmp)
#   DBCMP      data directories and converted inputs (/var/tmp/dbcmp; never /tmp, a RAM tmpfs here)
#   OUT        results ($DBCMP/results)
#   BENCH_LOCK the lock every timed step takes exclusively (flock -x) and every other step shared (flock -s), per
#              step; builds take it shared; a smoke run (SMOKE=1) takes it shared for each step, never for the run
RDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
H=$(cd "$RDIR/.." && pwd)                     # bench/dbcmp: ch_load.sh, mkq.py, check.py, i2_settle.sh, ...
ROOT=$(cd "$H/../.." && pwd)
TSDB_TREE=${TSDB_TREE:-$ROOT}
T=$TSDB_TREE/build
BIN=${BIN:-$ROOT/build/dbcmp}
B=$BIN
# every curl of the driver and its helpers stops after CURL_HOME/.curlrc's max-time (1800 s): a server that never
# answers fails a step instead of holding the run (and BENCH.lock); qdrive has QDRIVE_TIMEOUT (600 s) per query, and
# run.sh a wall-clock limit per step (STEP_TIMEOUT, UNTIMED_TIMEOUT, CHECK_TIMEOUT) for every other client
export CURL_HOME=${CURL_HOME:-$RDIR/curlhome}
export QDRIVE_TIMEOUT=${QDRIVE_TIMEOUT:-600}
TB=${TB:-$TSDB_TREE/build/dbcmp}             # tsdbq, tsdbstat, embq of the tsdb under test
export DBCMP=${DBCMP:-/var/tmp/dbcmp}
export WORK=${WORK:-$DBCMP/work} DATA=${DATA:-$DBCMP/input/mixed.rowbinary} BIN CLICKHOUSE=${CLICKHOUSE:-$DBCMP/clickhouse-bin}
export PGPASSWORD=bench
IN=${IN:-$DBCMP/sizes/in}
OUT=${OUT:-$DBCMP/results}
LOCK=${BENCH_LOCK:-$DBCMP/BENCH.lock}
HYD_RAW=${HYD_RAW:-/var/tmp/datasets/uci-hydraulic}   # the UCI hydraulic rig's text files (bench/hydraulic/get.sh)
REPS=${REPS:-5}
SECS=${SECS:-15}
QUIET_MAX=${QUIET_MAX:-600}

# Pins: exact versions, each at least two weeks old when the run was planned (2026-10-27); digests in machine.txt
CH_IMG=docker.io/clickhouse/clickhouse-server:26.8.2.7
TS_IMG=docker.io/timescale/timescaledb:2.31.0-pg17
TSHA_IMG=docker.io/timescale/timescaledb-ha:pg17.11-ts2.31.0
QDB_IMG=docker.io/questdb/questdb:10.1.0
I2_IMG=docker.io/library/influxdb:2.7.12
I3_IMG=docker.io/library/influxdb:3.12.1-core
TD_IMG=docker.io/tdengine/tsdb:3.4.2.8@sha256:44f58e6f9b013bccb94c4a27dd0ec7bca2982474fd3b390cafb48f709db1f0b1
IOT_IMG=docker.io/apache/iotdb:2.0.11-standalone@sha256:5ace0f3eceff892dd349d02288224c966f3fdeb11d08c4f575b642a110b82338
VM_IMG=docker.io/victoriametrics/victoria-metrics:v1.154.0@sha256:d83112326f700fe65f6fe66b12348406191cb20826ae0df72d676a2e69d3f3c9
PROM_IMG=docker.io/prom/prometheus:v3.15.0@sha256:efd719c99d83b060d9daefdcf00360461adf279f45ef5391f8d111892118753e
IOT_VENV=${IOT_VENV:-/var/tmp/dbcmp/venv-iotdb}   # python3 -m venv + pip install -r iotdb-requirements.txt
DUCKDB_DIR=${DUCKDB_DIR:-$DBCMP/duckdb/v1.5.6}
DUCKDB_SHA=b845005f5132a7d8180057c35e14a7626632258782f871a90861b19c1c03841b   # libduckdb-linux-amd64.zip
CHDB_DIR=${CHDB_DIR:-$DBCMP/chdb/v26.9.0}
CHDB_SHA=c6398bcc71dc58d12fb81548540aacd5d8248830ec81cec537f51c114670543b     # chdb-core v26.9.0 linux-x86_64-libchdb.tar.gz

DS_KINDS="hdd_temp cpu_temp tank_bar tank_psi vibration flow_rate flow_total random"
DS_HYD="hyd_PS1 hyd_PS2 hyd_PS3 hyd_PS4 hyd_PS5 hyd_PS6 hyd_EPS1 hyd_FS1 hyd_FS2 hyd_TS1 hyd_TS2 hyd_TS3 hyd_TS4 hyd_VS1 hyd_CE hyd_CP hyd_SE hyd_PS1_psi hyd_PS2_psi hyd_PS3_psi hyd_PS4_psi hyd_PS5_psi hyd_PS6_psi"
