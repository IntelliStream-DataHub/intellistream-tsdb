#!/bin/bash
# Release comparison driver: the full comparison of bench/dbcmp/README.md plus the release additions, unattended,
# one command per phase (run.sh with no arguments lists them), e.g.
#   BENCH_LOCK=/path/BENCH.lock bench/dbcmp/release/run.sh p2
# Every phase logs "PHASE n DONE" or "PHASE n FAILED: why" in results/driver.log; a failed phase does not stop the
# later ones. Every timing step runs under flock -x BENCH.lock (other steps under flock -s, step by step), after the governor check (performance) and the idle
# check (busy CPUs < 1.5 over 2 s, from /proc/stat); steps.log has one line per step (label, rc, seconds, busy).
# Raw output of every run is kept under results/p<n>/; <name>.<run> files are summarized by runstats.py into
# <name>.mean.txt (mean, sd %, min-max per number).
set -u
. "$(dirname "${BASH_SOURCE[0]}")/env.sh"
TOOLS=$RDIR
DSLIST=${DSLIST:-"$DS_KINDS $DS_HYD"}
P1_SYSTEMS=${P1_SYSTEMS:-"tsdb ch duck qdb qdbz ts i2 i3 prom iot iotz td tdz vm"}
P2_SYSTEMS=${P2_SYSTEMS:-"tsdb ch qdb0 qdb1 qdb2 ts i2 i3 iot iotz td tdz vm"}
I3_P2_FLAGS=${I3_P2_FLAGS:-}            # smoke runs only: e.g. --force-snapshot-max-age 2m
I3_P2_MIN=${I3_P2_MIN:-600}
I3_P2_STABLE=${I3_P2_STABLE:-300} I3_P2_MAX=${I3_P2_MAX:-9000}     # Parquet unchanged for STABLE s; give up after MAX s
I3_P1_STABLE=${I3_P1_STABLE:-180} I3_P1_MAX=${I3_P1_MAX:-5400}
I3_P1_AGE=${I3_P1_AGE:-5m} I3_P1_MIN=${I3_P1_MIN:-420}
I2_EXTRA=${I2_EXTRA:-}                  # smoke runs only: e.g. -e INFLUXD_STORAGE_CACHE_SNAPSHOT_WRITE_COLD_DURATION=10s
PROM_SETTLE_TIMEOUT=${PROM_SETTLE_TIMEOUT:-36000}
P5_SERIES=${P5_SERIES:-10000} P5_POINTS=${P5_POINTS:-10000000} P5_MINFREE=${P5_MINFREE:-320}
CONTAINERS="rb-ch rb-qdb rb-ts rb-tsha rb-i2 rb-i3 rb-prom rb-prom-bf rb-prom-sweep rb-promtool rb-chbig rb-iot rb-td rb-vm"
PORTS_RE=':(7070|8123|9000|9004|9005|9009|5434|9100|9103|8812|9109|8086|9999|8181|9090|9091|6667|18080|6041|8428)\b'
mkdir -p "$OUT"

# ------------------------------------------------------------------ logging, quiet gate, timed steps
log() { echo "$(date '+%F %T') $*" | tee -a "$OUT/driver.log"; }
fail() { mkdir -p "$PH_DIR"; echo "$(date '+%T') $*" >> "$PH_DIR/FAILS"; log "  FAIL: $*"; }
gov_ok() { local g; for g in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do [ "$(cat "$g")" = performance ] || return 1; done; }
cpu_busy() { # busy CPUs over 2 s, from two reads of /proc/stat
    local c u n s i w q sq st r
    read -r c u n s i w q sq st r < /proc/stat
    local t1=$((u + n + s + i + w + q + sq + st)) i1=$((i + w))
    sleep 2
    read -r c u n s i w q sq st r < /proc/stat
    local t2=$((u + n + s + i + w + q + sq + st)) i2=$((i + w))
    echo "scale=2; (($t2 - $t1) - ($i2 - $i1)) * $(nproc) / ($t2 - $t1)" | bc
}
QMAX=$QUIET_MAX
reset_quiet() { QMAX=$QUIET_MAX; }
quiet() { # LABEL: wait for the performance governor and < 1.5 busy CPUs; after QMAX s measure anyway (logged)
    local t0 b
    [ "${SMOKE:-0}" = 1 ] && { BUSY=smoke; return 0; }   # smoke runs check, they do not time: no gates
    t0=$(date +%s)
    until gov_ok; do
        if [ $(($(date +%s) - t0)) -ge 600 ]; then log "WARN governor is not performance on every CPU before: $1"; break; fi
        sleep 10
    done
    while :; do
        b=$(cpu_busy)
        if [ "$(echo "$b < 1.5" | bc)" = 1 ]; then BUSY=$b; return 0; fi
        if [ $(($(date +%s) - t0)) -ge "$QMAX" ]; then
            log "WARN not quiet before '$1': $b busy CPUs after ${QMAX} s; measuring anyway (top: $(ps -eo pcpu,comm --sort=-pcpu | sed -n 2,4p | tr -s ' ' | tr '\n' ';'))"
            BUSY="$b(NOT-QUIET)"; QMAX=60  # this system keeps the machine busy: shorter waits for its later steps
            return 0
        fi
    done
}
# Every step has a wall-clock limit (a server that never answers once held a smoke run, and the shared benchmark lock,
# for 90 minutes): STEP_TIMEOUT for timed steps, UNTIMED_TIMEOUT for the others (settles wait up to their own limits,
# Prometheus' 10 h among them), CHECK_TIMEOUT for answer checks. A step over its limit is killed with its whole process
# tree (shell functions too, so not timeout(1)), and fails with rc 124 and "STEP TIMEOUT" in its output.
STEP_TIMEOUT=${STEP_TIMEOUT:-3600} UNTIMED_TIMEOUT=${UNTIMED_TIMEOUT:-43200} CHECK_TIMEOUT=${CHECK_TIMEOUT:-3600}
ptree() { # PID: PID's descendants, deepest first, then PID
    local k
    for k in $(cat /proc/"$1"/task/*/children 2> /dev/null); do ptree "$k"; done
    echo "$1"
}
killtree() { # PID [now]: TERM, 3 s, then KILL ("now": KILL only) to a process tree this script started (never 0, 1, -1)
    local p sig ps sigs="TERM KILL"
    [ "${2:-}" = now ] && sigs=KILL
    ps=$(ptree "$1")   # once: a child whose parent died is reparented and out of reach of a second walk
    for sig in $sigs; do
        for p in $ps; do [[ "$p" =~ ^[0-9]+$ ]] && [ "$p" -gt 1 ] && kill -"$sig" "$p" 2> /dev/null; done
        [ "$sig" = TERM ] && sleep 3
    done
}
limited() { # SECONDS OUTFILE CMD...: CMD (shell functions too) with stdout+stderr to OUTFILE; rc 124 past SECONDS
    local lim=$1 out=$2 pid wpid flag rc
    shift 2
    flag=$(mktemp "$OUT/.timeout.XXXXXX") && rm -f "$flag"
    IN_STEP=1 "$@" > "$out" 2>&1 8>&- 9>&- &
    pid=$!
    ( sleep "$lim"; : > "$flag"; killtree "$pid" ) > /dev/null 2>&1 8>&- 9>&- &
    wpid=$!
    wait "$pid"
    rc=$?
    if [ -e "$flag" ]; then
        wait "$wpid" 2> /dev/null   # the watcher finishes its TERM, then KILL
        rm -f "$flag"; echo "STEP TIMEOUT: killed after $lim s" >> "$out"; return 124
    fi
    killtree "$wpid" now 2> /dev/null; wait "$wpid" 2> /dev/null   # the watcher: a subshell and its sleep
    return "$rc"
}
# Steps take BENCH.lock: timed ones exclusively (after the quiet gate), the others shared, so no other track times
# while this one loads, settles or checks. A smoke run (SMOKE=1) times nothing that is reported and takes it shared for
# every step, never for the whole run. A step inside a step (IN_STEP) takes no lock: it would wait for its own.
steplock() { # MODE(-s|-x): BENCH.lock on fd 9 unless inside a step
    [ "${IN_STEP:-0}" = 1 ] && return 0
    exec 9>>"$LOCK"
    flock "$1" 9
}
stepunlock() { [ "${IN_STEP:-0}" = 1 ] && return 0; flock -u 9; exec 9>&-; }
tstep() { # LABEL OUTFILE -- CMD...: exclusive lock, quiet gate, CMD with stdout+stderr to OUTFILE (fd 9 closed)
    local label=$1 out=$2 s e rc
    shift 3
    if [ "${SMOKE:-0}" = 1 ]; then steplock -s; else steplock -x; fi
    quiet "$label"
    s=$(date +%s.%N)
    limited "$STEP_TIMEOUT" "$out" "$@"
    rc=$?
    e=$(date +%s.%N)
    stepunlock
    printf '%s\t%s\trc=%d\t%.1fs\tbusy=%s\n' "$(date '+%F %T')" "$label" "$rc" "$(echo "$e - $s" | bc)" "$BUSY" >> "$OUT/steps.log"
    # a run measured on a busy machine says so beside its output (OUT.notquiet), which tables.py prints with the mean
    case $BUSY in *NOT-QUIET*) echo "busy=$BUSY" > "$out.notquiet" ;; *) rm -f "$out.notquiet" ;; esac
    if [ "$rc" != 0 ] && [ "$rc" != 124 ] && [ -n "${QREFUSE-}" ] && grep -qE -- "$QREFUSE" "$out"; then
        # the server refused the query by its own limits (QREFUSE, set per system, e.g. IoTDB's code 719 "not enough
        # memory"): a result to report as "refused", not a harness failure; renamed so runstats averages no error text
        mv "$out" "$out.refused"
        echo "$label: refused by the server: $(grep -m1 -oE -- "$QREFUSE.{0,160}" "$out.refused")" >> "$(dirname "$out")/REFUSED.txt"
        log "  REFUSED: $label (see $out.refused)"
        return "$rc"
    fi
    if [ "$rc" != 0 ] && [ -n "${QCTR-}" ] && [ "${label#* load r}" = "$label" ] && oom_killed "$QCTR"; then
        # the kernel killed the server out of memory (its container limit) on this load: a result like a refusal, the
        # server started again on the same data for the steps after it (QREADY: a command that succeeds once it serves)
        mv "$out" "$out.oom"
        echo "$label: the server was killed out of memory (container limit $(podman inspect -f '{{.HostConfig.Memory}}' "$QCTR" | awk '{printf "%.0f GB", $1 / 2^30}'))" >> "$(dirname "$out")/REFUSED.txt"
        log "  OOM: $label killed $QCTR (see $out.oom); restarting it"
        ctr_restart "$QCTR" || fail "$label: $QCTR did not serve again after its OOM kill"
        return "$rc"
    fi
    [ "$rc" = 0 ] || fail "$label: rc $rc$([ "$rc" = 124 ] && echo " (STEP TIMEOUT ${STEP_TIMEOUT}s)") (see $out)"
    case "$rc $label" in 0\ *) ;; *" load r"*) LOAD_FAILED=1 ;; esac
    return "$rc"
}
load_ok() { # LABEL: after a system's load loop: a failed load skips its settle waits and queries (one FAIL, no waits)
    local f=${LOAD_FAILED:-0}
    LOAD_FAILED=0
    [ "$f" = 0 ] && return 0
    fail "$1: a load failed; settle waits and queries skipped"
    return 1
}
inputs_ok() { # N: the inputs prep makes for phase N; a phase without them fails at once instead of loading nothing
    local f miss=""
    case $1 in
        1) for f in $DSLIST; do [ -s "$DBCMP/sizes/in/$f.rowbinary" ] || miss="$miss sizes/in/$f.rowbinary"; done ;;
        2|3|4) for f in "$DBCMP"/input/mixed.{rowbinary,lp,ilp32,csv,om} "$WORK/spec.tsv" "$WORK/batches.tsv"; do
                   [ -s "$f" ] || miss="$miss $f"; done ;;
    esac
    [ -z "$miss" ] && return 0
    fail "phase $1: inputs missing (run.sh prep; see $OUT/prep.log):$miss"
    return 1
}
reps() { # NAME DIR -- CMD...: REPS timed runs into DIR/NAME.<run>
    local name=$1 dir=$2 r
    shift 3
    for r in $(seq "$REPS"); do tstep "$name r$r" "$dir/$name.$r" -- "$@"; done
}
untimed() { # OUTFILE CMD...: a step that is not a timing (checks, dumps, sizes); no lock
    local out=$1 lim=$UNTIMED_TIMEOUT rc
    shift
    case $(basename "$out") in check*) lim=$CHECK_TIMEOUT ;; esac
    steplock -s
    limited "$lim" "$out" "$@"
    rc=$?
    stepunlock
    [ "$rc" = 0 ] || { fail "$(basename "$out"): rc $rc$([ "$rc" = 124 ] && echo " (STEP TIMEOUT ${lim}s)") (see $out)"; return 1; }
    case $(basename "$out") in check*) grep -qE 'mismatches [1-9]|MISMATCH|Traceback' "$out" && { fail "answer check: $(grep -m1 -E 'mismatches [1-9]|MISMATCH|Traceback' "$out" | cut -c1-150) ($out)"; return 1; } ;; esac
    return 0
}
runstats() { python3 -I "$TOOLS/runstats.py" "$1" >> "$OUT/driver.log" 2>&1; }

# ------------------------------------------------------------------ processes and containers we own
TPID=
tsdbd_start() { # DIR [ARGS]: tsdbd on --port 0 --port-file; sets TPID and TSDB_PORT
    local d=$1 p pid i
    shift
    rm -f "$d.pf"
    ${TSDBD_WRAP:-} "$T/tsdbd" --dir "$d" --listen 127.0.0.1 --port 0 --port-file "$d.pf" "$@" > "$d.log" 2>&1 9>&- &
    TPID=$!
    echo "$TPID tsdbd $d" >> "$OUT/pids"
    for i in $(seq 600); do [ -s "$d.pf" ] && break; kill -0 "$TPID" 2>/dev/null || break; sleep 0.1; done
    read -r p pid _ < "$d.pf" 2>/dev/null || { fail "tsdbd did not start on $d (log $d.log)"; TPID=; return 1; }
    if [ "$pid" != "$TPID" ]; then fail "port file of $d names pid $pid, not our $TPID"; tsdbd_stop; return 1; fi
    export TSDB_PORT=$p TSDB_HOST=127.0.0.1
}
tsdbd_stop() {
    if [ -n "$TPID" ] && [ "$TPID" -gt 1 ] 2>/dev/null; then kill -INT "$TPID" 2>/dev/null; wait "$TPID" 2>/dev/null; fi
    TPID=
}
crm() { # stop and remove containers by name; only the rb-* names this script creates
    local c
    for c in "$@"; do
        case $c in rb-*) podman stop -t 60 "$c" > /dev/null 2>&1; podman rm -f "$c" > /dev/null 2>&1 ;; esac
    done
}
cleanup_all() { tsdbd_stop; crm $CONTAINERS; }
rmdata() { local d; for d in "$@"; do case $d in "$DBCMP"/s[0-9]*|/var/tmp/big|/var/tmp/ch-big) podman unshare rm -rf "$d" ;; esac; done; }
ports_check() { # FILE: listeners on the benchmark ports; anything not on 127.0.0.1 is a failure
    ss -ltnp 2> /dev/null | grep -E "$PORTS_RE" > "$1"
    if grep -E "$PORTS_RE" "$1" | awk '{print $4}' | grep -vqE '^127\.0\.0\.1:'; then fail "a port is not bound to 127.0.0.1: $(tr '\n' ' ' < "$1")"; fi
}
warm() { cat "$@" > /dev/null; sync; }

# ------------------------------------------------------------------ servers
ch_start() { # DATADIR
    local W=$1 i
    mkdir -p "$W/data"
    printf '<clickhouse>\n    <listen_host replace="replace">127.0.0.1</listen_host>\n</clickhouse>\n' > "$W/listen.xml"
    podman run -d --name rb-ch --network host --ulimit nofile=262144:262144 -e CLICKHOUSE_SKIP_USER_SETUP=1 \
        -v "$W/data:/var/lib/clickhouse" -v "$W/listen.xml:/etc/clickhouse-server/config.d/zz-listen.xml" "$CH_IMG" > /dev/null 9>&- || { fail "clickhouse start"; return 1; }
    for i in $(seq 120); do curl -sf http://127.0.0.1:8123/ping > /dev/null && break; sleep 0.5; done
    curl -sf http://127.0.0.1:8123/ping > /dev/null || { fail "clickhouse not ready"; return 1; }
    [ -x "$CLICKHOUSE" ] || podman cp rb-ch:/usr/bin/clickhouse "$CLICKHOUSE"
}
CHC() { "$CLICKHOUSE" client --host 127.0.0.1 "$@"; }
# QuestDB converts partitions to Parquet with LZ4_RAW unless told otherwise; the ZSTD variant (qdbz in p1, qdb2 in p2)
# sets the codec and level (QuestDB 10.1.0 PropServerConfiguration: codec default LZ4_RAW, ZSTD level default 9,
# levels 1-22; a partition compressing by less than min.compression.ratio 1.2 stays native)
# Level 22 converts about one partition per second whatever its size (smoke: 450 small partitions took > 300 s);
# qdb_wait_table's QDB_WAIT_MAX covers that.
QDB_ZSTD_LEVEL=${QDB_ZSTD_LEVEL:-22}
qdb_zstd_env() { echo "-e QDB_CAIRO_PARTITION_ENCODER_PARQUET_COMPRESSION_CODEC=ZSTD -e QDB_CAIRO_PARTITION_ENCODER_PARQUET_COMPRESSION_LEVEL=$QDB_ZSTD_LEVEL"; }
qdb_start() { # DATADIR; QDB_ZSTD=1: Parquet conversions with ZSTD
    local W=$1 i
    mkdir -p "$W"
    # shellcheck disable=SC2046
    # the HTTP header buffer (default 64 KB) holds the URL: check.py sends a batch of 100 series as one GET /exec,
    # which QuestDB otherwise drops ("URL query string is too long"); the timed queries go over PostgreSQL wire
    podman run -d --name rb-qdb --network host -v "$W:/var/lib/questdb" $([ "${QDB_ZSTD:-0}" = 1 ] && qdb_zstd_env) \
        -e QDB_HTTP_NET_BIND_TO=127.0.0.1:9100 -e QDB_HTTP_MIN_NET_BIND_TO=127.0.0.1:9103 \
        -e QDB_PG_NET_BIND_TO=127.0.0.1:8812 -e QDB_LINE_TCP_NET_BIND_TO=127.0.0.1:9109 \
        -e QDB_LINE_UDP_ENABLED=false -e QDB_TELEMETRY_ENABLED=false -e QDB_HTTP_REQUEST_HEADER_BUFFER_SIZE=1048576 \
        "$QDB_IMG" > /dev/null 9>&- || { fail "questdb start"; return 1; }
    for i in $(seq 120); do curl -sf 'http://127.0.0.1:9100/exec?query=select%201' > /dev/null && return 0; sleep 0.5; done
    fail "questdb not ready"; return 1
}
QX() { curl -sG http://127.0.0.1:9100/exec --data-urlencode "query=$1"; echo; }
qdb_wait_table() { # TABLE: until every WAL transaction is applied; fails after QDB_WAIT_MAX s (a missing table never applies)
    local r w q s t0=$SECONDS
    while [ $((SECONDS - t0)) -lt "${QDB_WAIT_MAX:-3600}" ]; do
        r=$(curl -sG http://127.0.0.1:9100/exec --data-urlencode "query=select writerTxn, sequencerTxn, suspended from wal_tables() where name='$1'" | jq -c '.dataset[0]')
        w=$(echo "$r" | jq '.[0]'); q=$(echo "$r" | jq '.[1]'); s=$(echo "$r" | jq '.[2]')
        [ "$s" = true ] && { echo "SUSPENDED $1 $r"; return 1; }
        [ "$w" = "$q" ] && [ "$w" != null ] && { echo "WAL applied for $1 ($r)"; return 0; }
        sleep 0.1
    done
    echo "WAL of $1 not applied after ${QDB_WAIT_MAX:-3600} s ($r)"
    return 1
}
TS_WORK_MEM=${TS_WORK_MEM:-256MB}  # REPORT §5.11: the default (timescaledb-tune: 6 MB) spills TWA's window sorts to disk
# Podman gives a container 64 MB of /dev/shm; PostgreSQL's parallel workers share their hash tables there, and a
# batch of 100 series failed with "could not resize shared memory segment ... No space left on device". A server on a
# host has the host's /dev/shm (half the RAM); 8 GB is plenty here.
TS_SHM=${TS_SHM:-8g}
ts_start() { # NAME IMAGE DATADIR MOUNTPOINT: TimescaleDB on 127.0.0.1:5434 with $IN or $DBCMP/input on /data
    local name=$1 img=$2 W=$3 mnt=$4 i
    mkdir -p "$W"
    if [ "$name" = rb-tsha ]; then
        podman unshare chown 1000:1000 "$W"
        podman run -d --name "$name" --network host --shm-size=$TS_SHM -v "$W:/home/postgres/pgdata" -v "$mnt:/data:ro" -e POSTGRES_PASSWORD=bench \
            -e TS_TUNE_MEMORY=24GB -e TS_TUNE_NUM_CPUS=32 "$img" postgres -c listen_addresses=127.0.0.1 -c port=5434 -c work_mem=$TS_WORK_MEM > /dev/null 9>&- || { fail "$name start"; return 1; }
    else
        podman run -d --name "$name" --network host --shm-size=$TS_SHM -v "$W:/var/lib/postgresql/data" -v "$mnt:/data:ro" -e POSTGRES_PASSWORD=bench \
            -e TS_TUNE_MEMORY=24GB -e TS_TUNE_NUM_CPUS=32 "$img" postgres -c listen_addresses=127.0.0.1 -c port=5434 -c work_mem=$TS_WORK_MEM > /dev/null 9>&- || { fail "$name start"; return 1; }
    fi
    # the image runs initdb and its init scripts on a temporary server first: wait for the extension, then again
    for i in $(seq 300); do
        [ "$(psql -h 127.0.0.1 -p 5434 -U postgres -Atc "select count(*) from pg_extension where extname='timescaledb'" 2> /dev/null)" = 1 ] && break
        sleep 1
    done
    sleep 5
    for i in $(seq 120); do psql -h 127.0.0.1 -p 5434 -U postgres -Atc 'select 1' > /dev/null 2>&1 && return 0; sleep 1; done
    fail "$name not ready"; return 1
}
PSQL() { psql -h 127.0.0.1 -p 5434 -U postgres "$@"; }
i2_start() { # DATADIR
    local W=$1 i
    mkdir -p "$W/data" "$W/config"
    podman run -d --name rb-i2 --network host --memory 24g --memory-swap 24g \
        -v "$W/data:/var/lib/influxdb2" -v "$W/config:/etc/influxdb2" \
        -e DOCKER_INFLUXDB_INIT_MODE=setup -e DOCKER_INFLUXDB_INIT_USERNAME=bench \
        -e DOCKER_INFLUXDB_INIT_PASSWORD=benchbench -e DOCKER_INFLUXDB_INIT_ORG=bench \
        -e DOCKER_INFLUXDB_INIT_BUCKET=cmp -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=benchtoken \
        -e INFLUXD_HTTP_BIND_ADDRESS=127.0.0.1:8086 -e INFLUXD_REPORTING_DISABLED=true \
        -e INFLUXD_STORAGE_COMPACT_FULL_WRITE_COLD_DURATION=1m $I2_EXTRA "$I2_IMG" > /dev/null 9>&- || { fail "influxdb2 start"; return 1; }
    for i in $(seq 240); do
        curl -sf -H 'Authorization: Token benchtoken' 'http://127.0.0.1:8086/api/v2/buckets?name=cmp' 2> /dev/null | grep -q '"name": *"cmp"' && break
        sleep 0.5
    done
    sleep 3  # the setup step restarts the server
    for i in $(seq 240); do
        curl -sf -H 'Authorization: Token benchtoken' 'http://127.0.0.1:8086/api/v2/buckets?name=cmp' 2> /dev/null | grep -q '"name": *"cmp"' && return 0
        sleep 0.5
    done
    fail "influxdb2 not ready"; return 1
}
I2() { podman exec rb-i2 influx "$@" --host http://127.0.0.1:8086 --token benchtoken --org bench; }
i3_start() { # DATADIR [EXTRA FLAGS]
    local W=$1 i
    shift
    mkdir -p "$W" && podman unshare chown -R 1500:1500 "$W"
    podman run -d --name rb-i3 --network host --memory 28g --memory-swap 28g -v "$W:/var/lib/influxdb3" "$I3_IMG" \
        influxdb3 serve --node-id node0 --object-store file --data-dir /var/lib/influxdb3/data \
        --http-bind 127.0.0.1:8181 --without-auth --query-file-limit 1000000 \
        --exec-mem-pool-size 8GB --file-cache-size 4GB --force-snapshot-mem-size 10GB --disable-telemetry-upload "$@" > /dev/null 9>&- || { fail "influxdb3 start"; return 1; }
    for i in $(seq 240); do curl -sf http://127.0.0.1:8181/health > /dev/null && return 0; sleep 0.5; done
    fail "influxdb3 not ready"; return 1
}
i3_parquet() { # DATADIR: "count bytes" of the Parquet files
    echo "$(podman unshare find "$1" -name '*.parquet' | wc -l) $(podman unshare find "$1" -name '*.parquet' -printf '%s\n' | awk '{s += $1} END {print s + 0}')"
}
i3_wait_snapshot() { # DATADIR MIN_S STABLE_S MAX_S: Parquet present and unchanged for STABLE_S, at least MIN_S after now
    local W=$1 min=$2 stable=$3 max=$4 t0 last cur changed
    t0=$(date +%s); last=""; changed=$t0
    while :; do
        cur=$(i3_parquet "$W")
        echo "$(date -u +%T) +$(($(date +%s) - t0))s parquet(count bytes)=$cur mem=$(podman stats --no-stream --format '{{.MemUsage}}' rb-i3 2> /dev/null | cut -d/ -f1)"
        [ "$cur" != "$last" ] && { last=$cur; changed=$(date +%s); }
        if [ "${cur%% *}" -gt 0 ] && [ $(($(date +%s) - t0)) -ge "$min" ] && [ $(($(date +%s) - changed)) -ge "$stable" ]; then
            echo "snapshot settled: last change $((changed - t0)) s after the load ended ($cur)"; return 0
        fi
        [ $(($(date +%s) - t0)) -ge "$max" ] && { echo "NOT settled after $max s ($cur)"; return 1; }
        sleep 20
    done
}
prom_run() { # NAME PORT DATADIR
    podman run -d --name "$1" --network host -v "$3:/prometheus" -v "$DBCMP/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
        "$PROM_IMG" --config.file=/etc/prometheus/prometheus.yml --storage.tsdb.path=/prometheus --web.listen-address=127.0.0.1:$2 \
        --web.enable-remote-write-receiver --web.enable-admin-api --query.max-concurrency=32 > /dev/null 9>&-
}
prom_ready() { local i; for i in $(seq 36000); do curl -sf "127.0.0.1:$1/-/ready" > /dev/null && return 0; sleep 0.1; done; return 1; }

# ------------------------------------------------------------------ query suites (qdrive / tsdbq / clickhouse-benchmark)
FULLS_DEFAULT="0 3600 60"
CLASSES_DEFAULT="raw agg1d agg7d point"
qsuite() { # SYS PREFIX DIR DRIVER-ARGS...: whole range (warm-up, REPS runs of one query) and classes (3 s warm-up,
           # REPS x SECS at 1 and 32 clients); FULLS / CLASSES select ("" skips)
    local S=$1 P=$2 D=$3 n c t
    shift 3
    for n in ${FULLS-$FULLS_DEFAULT}; do
        "$B/qdrive" "$@" "$WORK/q/$S.full$n.txt" 1 100000 1 > "$D/warm.$P.full$n" 2>&1 9>&- || { fail "$P full$n warm-up failed (see $D/warm.$P.full$n)"; continue; }
        reps "$P.full$n" "$D" -- "$B/qdrive" "$@" "$WORK/q/$S.full$n.txt" 1 100000 1
    done
    for c in ${CLASSES-$CLASSES_DEFAULT}; do
        "$B/qdrive" "$@" "$WORK/q/$S.$c.txt" 1 3 > "$D/warm.$P.$c" 2>&1 9>&- || { fail "$P $c warm-up failed (see $D/warm.$P.$c)"; continue; }
        for t in 1 32; do reps "$P.$c.c$t" "$D" -- "$B/qdrive" "$@" "$WORK/q/$S.$c.txt" "$t" "$SECS"; done
    done
}
qline() { # NAME DIR FILE DRIVER-ARGS...: one query (LTTB, TWA): a warm-up, then REPS runs
    local N=$1 D=$2 F=$3
    shift 3
    "$B/qdrive" "$@" "$F" 1 100000 1 > "$D/warm.$N" 2>&1 9>&- || { fail "$N warm-up failed (see $D/warm.$N)"; return 1; }
    reps "$N" "$D" -- "$B/qdrive" "$@" "$F" 1 100000 1
}
TWA_KINDS=${TWA_KINDS-twad twal}       # whole-range TWA kinds besides the hourly STEP twa (rel.py, relq)
TWA_WKINDS=${TWA_WKINDS-twa1d twa7d}   # TWA over random windows: the first TWA_N (1000) agg1d / agg7d windows
twasuite() { # SYS DIR DRIVER-ARGS...|ch: every TWA kind SYS has a query for (rel.py has), checked against tsdb's
             # answers in $WORK/ref first and timed only when they agree; a kind without a query is noted in DIR/dash.txt
    local S=$1 D=$2 k f r
    shift 2
    cd "$WORK" || return 1
    for k in $TWA_KINDS; do
        REL has "$S" "$k" || { echo "$k: no query in rel.py for $S" >> "$D/dash.txt"; continue; }
        f=$D/q.$k.txt
        REL q "$S" "$k" > "$f"
        # InfluxDB 2's timeWeightedAvg sees only its window's rows (no reading carried in across a bucket edge, none
        # interpolated to it): a third of the hourly LINEAR buckets differ, so it does less work than tsdb's twal
        case "$S:$k" in i2flux:twal)
            echo "$k: InfluxDB 2's timeWeightedAvg interpolates only between the bucket's own rows, not across its edges (a third of the buckets differ from tsdb's LINEAR TWA); not timed" >> "$D/dash.txt"
            continue ;;
        esac
        # td: TDengine's TWA() (LINEAR) has tsdb's edges but accumulates with less precision: smoke 10 x 650 k,
        # hourly, relative differences 1e-9 to 5e-8, and 1.7e-4 on an average near 0 (vibration, 8e-9 absolute).
        # It is checked against a stated bound (TWA_TOL), timed only within it, and the bound and the largest
        # difference go to DIR/loose.txt, which tables.py prints under the system's table
        case " ${TWA_LOOSE-td} " in
        *" $S "*)
            REL_TOL=${TWA_TOL:-1e-6,1e-6} untimed "$D/check-$k.txt" REL check "$S" "$k" "ref/$k.jsonl" ||
                { log "  $S $k: not timed (answers differ beyond ${TWA_TOL:-1e-6,1e-6})"; continue; }
            echo "$k: agrees with tsdb within ${TWA_TOL:-1e-6,1e-6} (relative, absolute) only, not 1e-9: $(grep -o 'largest difference.*' "$D/check-$k.txt" | head -1)" >> "$D/loose.txt";;
        *) untimed "$D/check-$k.txt" REL check "$S" "$k" "ref/$k.jsonl" || { log "  $S $k: not timed (answers differ)"; continue; };;
        esac
        if [ "$1" = ch ]; then
            chbench1 1 1 0 "$f" > /dev/null 2> "$D/warm.$k" 9>&-
            for r in $(seq "$REPS"); do tstep "$k r$r" "$D/$k.$r" -- chbench_tag "$k.$r.$$" 1 1 0 "$f"; done
        else
            qline "$k" "$D" "$f" "$@"
        fi
    done
    for k in $TWA_WKINDS; do
        REL has "$S" "$k" || { echo "$k: no query in rel.py for $S" >> "$D/dash.txt"; continue; }
        REL q "$S" "$k" > "q/$S.$k.txt"
        untimed "$D/check-$k.txt" REL check "$S" "$k" "ref/$k.jsonl" || { log "  $S $k: not timed (answers differ)"; continue; }
        if [ "$1" = ch ]; then FULLS="" CLASSES="$k" chsuite "$S" "$D" "$S"
        else FULLS="" CLASSES="$k" qsuite "$S" "$S" "$D" "$@"; fi
    done
}
tsdbtwa() { # DIR PREFIX CONNS...: relq for every TWA kind (windows at 1 and 32 connections), timed
    local D=$1 P=$2 k c
    shift 2
    for k in $TWA_KINDS; do for c in "$@"; do tstep "p2 tsdb $P$k c$c" "$D/$P$k.c$c.txt" -- "$B/relq" "$k" "$c" "$REPS"; done; done
    for k in $TWA_WKINDS; do for c in 1 32; do
        tstep "p2 tsdb $P$k c$c" "$D/$P$k.c$c.txt" -- env TWA_SPEC="$WORK/spec.tsv" "$B/relq" "$k" "$c" "$REPS"
    done; done
}
chbench1() { # CONC ITER|0 SECONDS|0 FILE [SETTING=V...]: FILE's SQL lines as HTTP POSTs through qdrive, its summary
    # line to stdout. ClickHouse is timed like every other HTTP system here: qdrive's client wall time per request, from
    # send to the last byte (clickhouse-benchmark rounds latencies to whole ms, and its 1/QPS has a fixed 100 ms wait).
    # ITER > 0: the first ITER lines once, in order (clickhouse-benchmark -i); else SECONDS on CONC connections.
    local c=$1 i=$2 s=$3 f=$4 q="" a h rc
    shift 4
    for a in "$@"; do q="$q&${a#--}"; done
    q=${q#&}
    h=$(mktemp "$WORK/.chq.XXXXXX") || return 1
    awk -v p="POST /?$q" '{ print p "\ttext/plain\t" $0 }' "$f" > "$h"
    # an exception after the first rows comes with status 200: a reply with an exception text in it is a failure
    if [ "$i" != 0 ]; then QDRIVE_ERR_SUB='DB::Exception' "$BIN/qdrive" http 127.0.0.1 8123 '' "$h" 1 100000 "$i"
    else QDRIVE_ERR_SUB='DB::Exception' "$BIN/qdrive" http 127.0.0.1 8123 '' "$h" "$c" "$s"; fi
    rc=$?
    rm -f "$h"
    return $rc
}
chsuite() { # PREFIX DIR TABLE-SYS(ch): chbench1 (qdrive over HTTP) with REPS runs per measurement
    local P=$1 D=$2 S=$3 n c t
    for n in ${FULLS-$FULLS_DEFAULT}; do
        chbench1 1 1 0 "$WORK/q/$S.full$n.txt" > /dev/null 2> "$D/warm.$P.full$n" 9>&-
        for r in $(seq "$REPS"); do tstep "$P.full$n r$r" "$D/$P.full$n.$r" -- chbench_tag "$P.full$n.$r.$$" 1 1 0 "$WORK/q/$S.full$n.txt"; done
        # like with like (REPORT §3): one query per series, 16 at a time, as tsdbq full 16 sends them
        for r in $(seq "$REPS"); do
            sed "s/@TAG@/$P.full$n.per.c16.$r.$$/" "$WORK/q/$S.full$n.per.http" > "$D/per.http"
            tstep "$P.full$n.per.c16 r$r" "$D/$P.full$n.per.c16.$r" -- bash -c "'$B/qdrive' http 127.0.0.1 8123 '' '$D/per.http' 16 100000 0 && chqlog '$P.full$n.per.c16.$r.$$'"
        done
    done
    for c in ${CLASSES-$CLASSES_DEFAULT}; do
        chbench1 1 0 3 "$WORK/q/$S.$c.txt" > /dev/null 2> "$D/warm.$P.$c" 9>&-
        for t in 1 32; do for r in $(seq "$REPS"); do
            tstep "$P.$c.c$t r$r" "$D/$P.$c.c$t.$r" -- chbench_tag "$P.$c.c$t.$r.$$" "$t" 0 "$SECS" "$WORK/q/$S.$c.txt"
        done; done
    done
}
tsdbsuite() { # PREFIX DIR CLASS-THREADS("1 32"): tsdbq qps classes against $TSDB_PORT
    local P=$1 D=$2 ths=$3 c t
    for c in ${CLASSES-$CLASSES_DEFAULT}; do
        "$TB/tsdbq" qps "$WORK/spec.tsv" "$c" 1 2 > "$D/warm.$P.$c" 2>&1 9>&-
        for t in $ths; do reps "$P.$c.c$t" "$D" -- "$TB/tsdbq" qps "$WORK/spec.tsv" "$c" "$t" "$SECS"; done
    done
}
REL() { python3 -I "$TOOLS/rel.py" "$@"; }

# ------------------------------------------------------------------ batches (batches.tsv, mkbatch.py) and one core
# A batch is one request of K series (K = 1, 10, 100; mkbatch.py says how each language does it). Whole-range
# batch files are run whole, one client, REPS times: "N queries in X s" is the total time of the set; window
# classes run their batch file at 1 and 32 clients for SECS. PC=1 is the per-core run: the server pinned to
# PERCORE_CPU (containers by ctr_taskset, tsdbd by taskset plus --query-threads 1), the engine's
# own thread knob where it has one, one client pinned to PERCORE_CLIENT_CPU; names get ".pc".
KS=${KS:-"1 10 100"}
BCLASSES=${BCLASSES-$CLASSES_DEFAULT}
PERCORE_CPU=${PERCORE_CPU:-2} PERCORE_CLIENT_CPU=${PERCORE_CLIENT_CPU:-3}
pc_client() { # CMD...: on PERCORE_CLIENT_CPU in a per-core run (shell functions too: those used are exported)
    if [ "${PC:-0}" = 1 ]; then taskset -c "$PERCORE_CLIENT_CPU" bash -c '"$@"' pc_client "$@"; else "$@"; fi
}
ctr_mem() { # CONTAINER FILE: the container's cgroup memory (peak, limit, OOM kills so far) appended to FILE, while it runs
    local p
    p=/sys/fs/cgroup$(podman inspect -f '{{.State.CgroupPath}}' "$1" 2> /dev/null)
    [ -r "$p/memory.peak" ] || { echo "$1: no cgroup memory files (not running?)" >> "$2"; return 1; }
    echo "$1 peak $(cat "$p/memory.peak") limit $(cat "$p/memory.max") $(grep '^oom_kill ' "$p/memory.events")" >> "$2"
}
oom_killed() { # CONTAINER: 0 when it no longer runs and the kernel OOM-killed a process in its cgroup
    local st id
    st=$(podman inspect -f '{{.State.Status}}' "$1" 2> /dev/null)
    case $st in running | '') return 1 ;; esac
    id=$(podman inspect -f '{{.Id}}' "$1" 2> /dev/null)
    [ -n "$id" ] && journalctl -k --since "-6h" 2> /dev/null | grep -qE "oom-kill:.*libpod-$id\.scope"
}
ctr_restart() { # CONTAINER: start it again (same settings and data), then wait up to 300 s for QREADY
    local i
    steplock -s
    podman start "$1" > /dev/null 2>&1 9>&-
    for i in $(seq 600); do
        ${QREADY:-true} > /dev/null 2>&1 || { sleep 0.5; continue; }
        stepunlock
        [ "${PC:-0}" = 1 ] && { pc_pin "$1" || return 1; }  # new processes: pinned again for the per-core runs
        return 0
    done
    stepunlock
    return 1
}
ctr_alive() { # CONTAINER WHAT: fails (into FAILS) when the container no longer runs, with the kernel's OOM kills in it
    local st id n=0
    st=$(podman inspect -f '{{.State.Status}} exit {{.State.ExitCode}}' "$1" 2> /dev/null)
    case $st in running*) return 0 ;; esac
    id=$(podman inspect -f '{{.Id}}' "$1" 2> /dev/null)
    [ -n "$id" ] && n=$(journalctl -k --since "-6h" 2> /dev/null | grep -cE "oom-kill:.*libpod-$id\.scope")
    fail "$2: $1 stopped by itself (${st:-gone}; kernel OOM kills in its cgroup: $n); the steps after it skipped"
    return 1
}
# Rootless Podman here has no cpuset controller (the user slice delegates cpu, memory and pids only), so
# --cpuset-cpus and podman update --cpuset-cpus fail; the container's processes are ours, so taskset pins every
# thread of every process in its tree (threads and processes made later inherit the mask).
ctr_taskset() { # CPUS CONTAINER: pin the container's process tree; fails if the container has no pid or nothing moved
    local cpus=$1 root p n=0 kids
    root=$(podman inspect -f '{{.State.Pid}}' "$2" 2> /dev/null)
    [[ "$root" =~ ^[1-9][0-9]*$ ]] || return 1
    local todo=("$root")
    while [ ${#todo[@]} -gt 0 ]; do
        p=${todo[0]}; todo=("${todo[@]:1}")
        taskset -a -cp "$cpus" "$p" > /dev/null 2>&1 && n=$((n + 1))
        kids=$(cat /proc/"$p"/task/*/children 2> /dev/null)
        for k in $kids; do todo+=("$k"); done
    done
    [ "$n" -gt 0 ] && [ "$(taskset -cp "$root" 2> /dev/null | sed 's/.*: //')" = "$cpus" ]
}
pc_pin() { # CONTAINER...: to PERCORE_CPU
    local c
    for c; do ctr_taskset "$PERCORE_CPU" "$c" || { fail "per-core: taskset $c"; return 1; }; done
}
how_write() { # PHASE SYS: how.json of the system's results (how.py: settings, durability, clients, per core, '-')
    env CH_IMG="$CH_IMG" TS_IMG="$TS_IMG" QDB_IMG="$QDB_IMG" I2_IMG="$I2_IMG" I3_IMG="$I3_IMG" IOT_IMG="$IOT_IMG" \
        TD_IMG="$TD_IMG" VM_IMG="$VM_IMG" PROM_IMG="$PROM_IMG" T="$T" TSDB_TREE="$TSDB_TREE" DUCKDB_DIR="$DUCKDB_DIR" \
        DUCKDB_SHA="$DUCKDB_SHA" CHDB_DIR="$CHDB_DIR" CHDB_SHA="$CHDB_SHA" REPS="$REPS" SECS="$SECS" KS="$KS" \
        SMOKE="${SMOKE:-0}" PERCORE_CPU="$PERCORE_CPU" PERCORE_CLIENT_CPU="$PERCORE_CLIENT_CPU" \
        python3 -I "$RDIR/how.py" write "$1" "$2" "$OUT" >> "$OUT/driver.log" 2>&1 || fail "how.json of $2 (phase $1)"
}
pc_unpin() { local c; for c; do ctr_taskset "0-$(( $(nproc) - 1 ))" "$c"; done; }
bsuite() { # SYS PREFIX DIR DRIVER-ARGS...: the batches of SYS through qdrive
    local S=$1 P=$2 D=$3 n k c t f nl sfx= ths=${BTHREADS:-1 32}
    shift 3
    [ "${PC:-0}" = 1 ] && { sfx=.pc; ths=1; }
    for k in $KS; do
        for n in ${FULLS-$FULLS_DEFAULT}; do
            f=$WORK/q/$S.full$n.b$k.txt; nl=$(wc -l < "$f")
            pc_client "$B/qdrive" "$@" "$f" 1 100000 "$nl" > "$D/warm.$P.full$n.b$k$sfx" 2>&1 9>&- || { fail "$P full$n.b$k warm-up failed (see $D/warm.$P.full$n.b$k$sfx)"; continue; }
            reps "$P.full$n.b$k$sfx" "$D" -- pc_client "$B/qdrive" "$@" "$f" 1 100000 "$nl"
        done
        for c in $BCLASSES; do
            f=$WORK/q/$S.$c.b$k.txt
            pc_client "$B/qdrive" "$@" "$f" 1 3 > "$D/warm.$P.$c.b$k$sfx" 2>&1 9>&- || { fail "$P $c.b$k warm-up failed (see $D/warm.$P.$c.b$k$sfx)"; continue; }
            for t in $ths; do reps "$P.$c.b$k.c$t$sfx" "$D" -- pc_client "$B/qdrive" "$@" "$f" "$t" "$SECS"; done
        done
    done
}
bcheck() { # SYS DIR: batch files and their answer check (check.py --batch); 0 when they agree with the reference
    (cd "$WORK" && python3 -I "$H/mkbatch.py" "$1") || { fail "mkbatch $1"; return 1; }
    untimed "$2/check-batch-$1.txt" bash -c "cd '$WORK' && python3 -I '$H/check.py' --batch '$1' 5"
}
# ClickHouse: qdrive drives over HTTP (chbench1), and the server's own latency comes from system.query_log, every
# query of a run tagged with log_comment (REPORT §3: never 1/QPS alone), in microseconds: query_duration_ms counts
# whole ms, and most of these queries take less than that
chqlog() { # TAG: server-side count, mean, median and p99 duration (ms, from us) and mean CPU (ms) of the queries tagged TAG
    "$CLICKHOUSE" client --host 127.0.0.1 -q "SYSTEM FLUSH LOGS" &&
    "$CLICKHOUSE" client --host 127.0.0.1 -q "WITH dateDiff('microsecond', query_start_time_microseconds,
        event_time_microseconds) / 1000 AS d SELECT 'server only', count(), 'queries, mean', round(avg(d), 3),
        'ms, median', round(quantile(0.5)(d), 3), 'ms, p99', round(quantile(0.99)(d), 3), 'ms, cpu',
        round(avg(ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / 1000, 3), 'ms'
        FROM system.query_log WHERE type = 'QueryFinish' AND log_comment = '$1' FORMAT TSV" | tr '\t' ' '
}
chbench_tag() { # TAG CONC ITER|0 SECONDS|0 FILE [SETTING...]: chbench1 with log_comment=TAG, then chqlog TAG
    local tag=$1 c=$2 i=$3 s=$4 f=$5
    shift 5
    chbench1 "$c" "$i" "$s" "$f" "log_comment=$tag" "$@" && chqlog "$tag"
}
export BIN WORK
export -f chbench1 chbench_tag chqlog
tsdb_has_batch() { grep -aq -- '--batch' "$TB/tsdbq"; }  # its usage says only "see source": the option's string
tsdbbsuite() { # PREFIX DIR: the batches through tsdbq (protocol 10, QUERY_MULTI: "full ... --batch K", CLASS.bK).
               # A tsdbq without --batch answers K = 1 only (the 10 single series, one request each; the window
               # classes one window per request) and the log says the rest is pending.
    local P=$1 D=$2 k c t sfx= ths=${BTHREADS:-1 32} n
    [ "${PC:-0}" = 1 ] && { sfx=.pc; ths=1; }
    tsdb_has_batch || log "  $P: tsdbq in $TB has no --batch: tsdb batches K > 1 pending (K = 1 only)"
    for k in $KS; do
        if [ "$k" = 1 ]; then
            reps "$P.full.b1$sfx" "$D" -- pc_client env NSERIES="$(python3 -I "$H/batches.py" --count-full1)" "$TB/tsdbq" full 1 1
        elif tsdb_has_batch; then
            reps "$P.full.b$k$sfx" "$D" -- pc_client "$TB/tsdbq" full 1 1 --batch "$k"
        else continue; fi
        for c in $BCLASSES; do
            n=$c; [ "$k" = 1 ] || n=$c.b$k
            pc_client "$TB/tsdbq" qps "$WORK/spec.tsv" "$n" 1 2 > "$D/warm.$P.$n$sfx" 2>&1 9>&-
            for t in $ths; do reps "$P.$n.c$t$sfx" "$D" -- pc_client "$TB/tsdbq" qps "$WORK/spec.tsv" "$n" "$t" "$SECS"; done
        done
    done
}
tsdbd_percore() { # DIR: restart tsdbd on PERCORE_CPU with one query thread (where tsdbd has --query-threads)
    local qt=()
    "$T/tsdbd" --help 2>&1 | grep -q -- '--query-threads' && qt=(--query-threads 1)
    tsdbd_stop
    TSDBD_WRAP="taskset -c $PERCORE_CPU" tsdbd_start "$1" "${qt[@]}"
}
chbsuite() { # PREFIX DIR TABLE-SYS(ch): the batches through chbench1 (PC=1: max_threads=1, pinned)
    local P=$1 D=$2 S=$3 n k c t f nl sfx= ths=${BTHREADS:-1 32} set=() r
    [ "${PC:-0}" = 1 ] && { sfx=.pc; ths=1; set=(--max_threads=1); }
    for k in $KS; do
        for n in ${FULLS-$FULLS_DEFAULT}; do
            f=$WORK/q/$S.full$n.b$k.txt; nl=$(wc -l < "$f")
            pc_client chbench1 1 "$nl" 0 "$f" > /dev/null 2> "$D/warm.$P.full$n.b$k$sfx" 9>&-
            for r in $(seq "$REPS"); do
                tstep "$P.full$n.b$k$sfx r$r" "$D/$P.full$n.b$k$sfx.$r" -- pc_client chbench_tag "$P.full$n.b$k$sfx.$r.$$" 1 "$nl" 0 "$f" "${set[@]}"
            done
        done
        for c in $BCLASSES; do
            f=$WORK/q/$S.$c.b$k.txt
            pc_client chbench1 1 0 3 "$f" > /dev/null 2> "$D/warm.$P.$c.b$k$sfx" 9>&-
            for t in $ths; do for r in $(seq "$REPS"); do
                tstep "$P.$c.b$k.c$t$sfx r$r" "$D/$P.$c.b$k.c$t$sfx.$r" -- pc_client chbench_tag "$P.$c.b$k.c$t$sfx.$r.$$" "$t" 0 "$SECS" "$f" "${set[@]}"
            done; done
        done
    done
}

# ================================================================== phase 1: sizes per kind of sensor
p1_tsdb() {
    local D=$OUT/p1/tsdb W=$DBCMP/s1/tsdb ds f rows bs bo du1 du2
    mkdir -p "$D"; rmdata "$W"; mkdir -p "$W"
    printf 'ds\trows\tbytes\tnote\n' > "$OUT/p1/tsdb.tsv"
    printf 'ds\trows\tbytes\tnote\n' > "$OUT/p1/tsdb-settled.tsv"
    for ds in $DSLIST; do
        tsdbd_start "$W/$ds" || continue
        f=$D/$ds.txt
        { "$T/tsdbc-bench" load "$IN/$ds.rowbinary" --conns 1 --port "$TSDB_PORT" && "$TB/tsdbstat" settle &&
            du -sb --exclude='retired_*' "$W/$ds" && "$TB/tsdbstat" optimize && sleep 1 && du -sb --exclude='retired_*' "$W/$ds"; } > "$f" 2>&1 9>&- ||
            fail "p1 tsdb $ds (see $f)"
        tsdbd_stop
        rows=$(awk '/^optimized:/ {for (i = 1; i <= NF; i++) if ($i == "rows") print $(i + 1)}' "$f")
        bs=$(awk '/^settled:/ {for (i = 1; i <= NF; i++) if ($i == "bytes") print $(i + 1)}' "$f")
        bo=$(awk '/^optimized:/ {for (i = 1; i <= NF; i++) if ($i == "bytes") print $(i + 1)}' "$f")
        du1=$(awk -F'\t' -v p="$W/$ds" '$2 == p {print $1}' "$f" | head -1)
        du2=$(awk -F'\t' -v p="$W/$ds" '$2 == p {print $1}' "$f" | tail -1)
        printf '%s\t%s\t%s\ttsdb_optimize (one part per partition, what merge_idle_s merges do); du %s\n' "$ds" "$rows" "$bo" "$du2" >> "$OUT/p1/tsdb.tsv"
        printf '%s\t%s\t%s\tafter tsdbstat settle (background merges idle 3 s); du %s\n' "$ds" "$rows" "$bs" "$du1" >> "$OUT/p1/tsdb-settled.tsv"
    done
    rmdata "$W"
}
ch_settle() { # DB: until active part counts and running merges are unchanged for 60 s
    local s last prev cur
    s=$(date +%s); last=$s; prev=""
    while :; do
        cur=$(CHC -q "SELECT (SELECT count() FROM system.parts WHERE database='$1' AND active), (SELECT count() FROM system.merges WHERE database='$1')")
        [ "$cur" != "$prev" ] && { last=$(date +%s); prev=$cur; }
        [ $(($(date +%s) - last)) -ge 60 ] && break
        [ $(($(date +%s) - s)) -ge "${CH_SETTLE_MAX:-7200}" ] && { echo "merges NOT settled after ${CH_SETTLE_MAX:-7200} s ($prev)"; return 1; }
        sleep 0.5
    done
    echo "merges settled $((last - s)) s after the start of the wait (parts, merges: $prev)"
}
p1_ch() {
    local D=$OUT/p1/ch W=$DBCMP/s1/ch ds t
    mkdir -p "$D"; rmdata "$W"
    ch_start "$W" || return 1
    ports_check "$D/ports.txt"
    CHC -q "CREATE DATABASE s1"
    for ds in $DSLIST; do
        CHC -q "CREATE TABLE s1.c_$ds (series UInt32, ts DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)), value Float32 CODEC(Gorilla, ZSTD(1))) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY (series, ts)"
        CHC -q "CREATE TABLE s1.l_$ds (series UInt32, ts DateTime64(3, 'UTC'), value Float32) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY (series, ts)"
        CHC -q "INSERT INTO s1.c_$ds FORMAT RowBinary" < "$IN/$ds.rowbinary" || fail "p1 ch insert c_$ds"
        CHC -q "INSERT INTO s1.l_$ds FORMAT RowBinary" < "$IN/$ds.rowbinary" || fail "p1 ch insert l_$ds"
    done
    sync -f "$DBCMP"
    ch_settle s1 > "$D/settle.txt" 2>&1 || fail "p1 ch merges did not settle (see $D/settle.txt)"
    local Q="SELECT table, sum(rows), sum(bytes_on_disk), count() FROM system.parts WHERE database='s1' AND active GROUP BY table ORDER BY table FORMAT TSV"
    CHC -q "$Q" > "$D/settled.tsv"
    CHC -q "SELECT table, name, data_compressed_bytes, data_uncompressed_bytes FROM system.columns WHERE database='s1' ORDER BY table, name FORMAT TSV" > "$D/columns-settled.tsv"
    for t in $(CHC -q "SELECT name FROM system.tables WHERE database='s1'"); do CHC -q "OPTIMIZE TABLE s1.$t FINAL" || fail "p1 ch optimize $t"; done
    sleep 10
    CHC -q "$Q" > "$D/optimized.tsv"
    printf 'ds\trows\tbytes\tnote\n' > "$OUT/p1/ch.tsv"
    printf 'ds\trows\tbytes\tnote\n' > "$OUT/p1/ch-optimized.tsv"
    printf 'ds\trows\tbytes\tnote\n' > "$OUT/p1/ch-lz4.tsv"
    awk -F'\t' '$1 ~ /^c_/ {sub(/^c_/, "", $1); printf "%s\t%s\t%s\tactive parts after merges settled (60 s unchanged): %s; DoubleDelta/Gorilla + ZSTD(1)\n", $1, $2, $3, $4}' "$D/settled.tsv" >> "$OUT/p1/ch.tsv"
    awk -F'\t' '$1 ~ /^c_/ {sub(/^c_/, "", $1); printf "%s\t%s\t%s\tafter OPTIMIZE FINAL: %s parts\n", $1, $2, $3, $4}' "$D/optimized.tsv" >> "$OUT/p1/ch-optimized.tsv"
    awk -F'\t' '$1 ~ /^l_/ {sub(/^l_/, "", $1); printf "%s\t%s\t%s\tdefault codecs (LZ4), merges settled: %s parts\n", $1, $2, $3, $4}' "$D/settled.tsv" >> "$OUT/p1/ch-lz4.tsv"
    crm rb-ch; rmdata "$W"
}
p1_duck() {
    local D=$OUT/p1/duckdb W=$DBCMP/s1/duck ds f
    mkdir -p "$D" "$W"
    printf 'ds\trows\tbytes\tnote\n' > "$OUT/p1/duckdb.tsv"
    for ds in $DSLIST; do
        f=$D/$ds.txt; rm -f "$W/$ds.duckdb" "$W/$ds.duckdb.wal"
        "$TB/embq" duckdb load "$W/$ds.duckdb" "$IN/$ds.rowbinary" 1 > "$f" 2>&1 9>&- || { fail "p1 duckdb $ds (see $f)"; continue; }
        awk -v ds="$ds" '/checkpoint:/ {for (i = 1; i <= NF; i++) { if ($i == "file") fb = $(i + 1); if ($i == "WAL") wb = $(i + 1) }}
            /load:/ {for (i = 1; i <= NF; i++) if ($i == "rows") r = $(i - 1)}
            END {printf "%s\t%s\t%d\tfile after CHECKPOINT %s + WAL %s\n", ds, r, fb + wb, fb, wb}' "$f" >> "$OUT/p1/duckdb.tsv"
        rm -f "$W/$ds.duckdb" "$W/$ds.duckdb.wal"
    done
    rmdata "$W"
}
p1_qdbz() { QDB_ZSTD=1 p1_qdb; }  # Parquet with ZSTD: its own server, sizes in p1/qdb-parquet-zstd.tsv
p1_qdb() {
    local D=$OUT/p1/qdb W=$DBCMP/s1/qdb ds i prev cur pqt=$OUT/p1/qdb-parquet.tsv nt=$OUT/p1/qdb.tsv
    [ "${QDB_ZSTD:-0}" = 1 ] && { D=$OUT/p1/qdbz W=$DBCMP/s1/qdbz pqt=$OUT/p1/qdb-parquet-zstd.tsv nt=$D/native.tsv; }
    mkdir -p "$D"; rmdata "$W"
    qdb_start "$W" || return 1
    ports_check "$D/ports.txt"
    for ds in $DSLIST; do
        QX "CREATE TABLE s_$ds (series SYMBOL CAPACITY 128 CACHE, value FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL" >> "$D/ddl.txt"
        "$B/loadlp" "$IN/$ds.ilp32" 127.0.0.1 9100 "/write?precision=ms" "" 100000 8 >> "$D/load.txt" 2>&1 9>&- || fail "p1 qdb load $ds"
    done
    for ds in $DSLIST; do qdb_wait_table "s_$ds" >> "$D/wal.txt" || fail "p1 qdb wal $ds"; done
    podman exec rb-qdb sync -f /var/lib/questdb/db
    printf 'ds\trows\tbytes\tnote\n' > "$nt"
    printf 'ds\trows\tbytes\tnote\n' > "$pqt"
    for ds in $DSLIST; do
        QX "SELECT count() FROM s_$ds" > "$D/rows.$ds.json"
        QX "SELECT isParquet, count(), sum(diskSize), sum(parquetFileSize) FROM table_partitions('s_$ds')" > "$D/native.$ds.json"
        jq -r --arg ds "$ds" --argjson rows "$(jq '.dataset[0][0]' "$D/rows.$ds.json")" \
            '[.dataset[] | .[2]] | add as $b | "\($ds)\t\($rows)\t\($b)\tnative, sum(diskSize) of table_partitions"' "$D/native.$ds.json" >> "$nt"
        podman unshare du -sb "$W"/db/s_"$ds"~* >> "$D/du-native.txt" 2>&1
    done
    for ds in $DSLIST; do
        QX "ALTER TABLE s_$ds CONVERT PARTITION TO PARQUET WHERE ts >= 0" >> "$D/convert.txt"
    done
    for ds in $DSLIST; do qdb_wait_table "s_$ds" >> "$D/wal-convert.txt" || fail "p1 qdb convert wal $ds"; done
    prev=""
    for i in $(seq 120); do  # until the partition listing is unchanged for 30 s
        cur=$(for ds in $DSLIST; do QX "SELECT isParquet, count(), sum(diskSize) FROM table_partitions('s_$ds')"; done | md5sum)
        [ "$cur" = "$prev" ] && break
        prev=$cur; sleep 30
    done
    sleep 60  # old native partition files go asynchronously
    podman exec rb-qdb sync -f /var/lib/questdb/db
    for ds in $DSLIST; do
        QX "SELECT count() FROM s_$ds" > "$D/rows-pq.$ds.json"
        QX "SELECT isParquet, count(), sum(diskSize), sum(parquetFileSize) FROM table_partitions('s_$ds')" > "$D/parquet.$ds.json"
        jq -r --arg ds "$ds" --argjson rows "$(jq '.dataset[0][0]' "$D/rows-pq.$ds.json")" \
            '([.dataset[] | .[2]] | add) as $b | ([.dataset[] | select(.[0]) | .[1]] | add // 0) as $np | ([.dataset[] | .[1]] | add) as $n | ([.dataset[] | .[3]] | add) as $pq
             | "\($ds)\t\($rows)\t\($b)\tsum(diskSize) after CONVERT PARTITION TO PARQUET: \($np) of \($n) partitions Parquet, parquetFileSize \($pq)"' "$D/parquet.$ds.json" >> "$pqt"
        podman unshare du -sb "$W"/db/s_"$ds"~* >> "$D/du-parquet.txt" 2>&1
    done
    crm rb-qdb; rmdata "$W"
}
p1_ts() {
    local D=$OUT/p1/ts W=$DBCMP/s1/ts ds tn
    mkdir -p "$D"; rmdata "$W"
    ts_start rb-ts "$TS_IMG" "$W" "$IN" || return 1
    ports_check "$D/ports.txt"
    PSQL -c "ALTER SYSTEM SET max_wal_size = '16GB'" -c "SELECT pg_reload_conf()" > /dev/null
    printf 'ds\trows\tbytes\tnote\n' > "$OUT/p1/ts.tsv"
    for ds in $DSLIST; do
        tn=ts_${ds,,}  # PostgreSQL folds unquoted names to lower case; timescaledb-parallel-copy quotes them
        {
            PSQL -c "CREATE TABLE $tn (series int NOT NULL, ts timestamptz NOT NULL, value real NOT NULL)" \
                -c "SELECT create_hypertable('$tn', by_range('ts', INTERVAL '1 day'))" \
                -c "ALTER TABLE $tn SET (timescaledb.compress, timescaledb.compress_segmentby = 'series', timescaledb.compress_orderby = 'ts')" &&
                podman exec rb-ts timescaledb-parallel-copy -connection "host=127.0.0.1 port=5434 user=postgres password=bench sslmode=disable dbname=postgres" \
                    -table "$tn" -file "/data/$ds.csv" -workers 8 -batch-size 10000 -columns series,ts,value -disable-direct-compress &&
                echo "before compression: $(PSQL -Atc "SELECT hypertable_size('$tn')")" &&
                PSQL -Atq -c "select c from show_chunks('$tn') c" | xargs -P 16 -I{} psql -h 127.0.0.1 -p 5434 -U postgres -Atq -c "select compress_chunk('{}')" > /dev/null &&
                PSQL -c "VACUUM ANALYZE $tn" &&
                PSQL -Atc "SELECT 'rows', count(*) FROM $tn" -c "SELECT 'size', hypertable_size('$tn')" -c "select 'chunks', status, count(*) from _timescaledb_catalog.chunk where hypertable_id = (select id from _timescaledb_catalog.hypertable where table_name='$tn') group by 2"
        } > "$D/$ds.txt" 2>&1 9>&- || fail "p1 ts $ds (see $D/$ds.txt)"
        awk -F'|' -v ds="$ds" '/^rows\|/ {r = $2} /^size\|/ {s = $2} /^before compression:/ {b = substr($0, 20)}
            END {printf "%s\t%s\t%s\tcompressed (segmentby series, orderby ts), after VACUUM; before compression%s\n", ds, r, s, b}' "$D/$ds.txt" >> "$OUT/p1/ts.tsv"
    done
    crm rb-ts; rmdata "$W"
}
p1_i2() {
    local D=$OUT/p1/i2 W=$DBCMP/s1/i2 ds id
    mkdir -p "$D"; rmdata "$W"
    i2_start "$W" || return 1
    ports_check "$D/ports.txt"
    for ds in $DSLIST; do
        I2 bucket create --name "$ds" --hide-headers >> "$D/buckets.txt" 2>&1 9>&-
        "$B/loadlp" "$IN/$ds.lp" 127.0.0.1 8086 "/api/v2/write?org=bench&bucket=$ds&precision=ms" $'Authorization: Token benchtoken\r\n' 5000 8 >> "$D/load.txt" 2>&1 9>&- || fail "p1 i2 load $ds"
    done
    I2_PORT=8086 "$H/i2_settle.sh" "$(date +%s)" > "$D/settle.txt" 2>&1 9>&- || fail "p1 i2 compaction did not settle (see $D/settle.txt); sizes taken anyway"
    I2 bucket list --hide-headers > "$D/bucket-list.txt" 2>&1 9>&-
    podman unshare sh -c "cd $W/data/engine && du -sb data/* wal/* 2>/dev/null" > "$D/du.txt"
    podman unshare sh -c "cd $W/data/engine && du -sB1 data/* 2>/dev/null" > "$D/du-allocated.txt"
    podman unshare sh -c "cd $W/data/engine && find data -type f -printf '%s\t%p\n'" > "$D/files.txt"
    printf 'ds\trows\tbytes\tnote\n' > "$OUT/p1/i2.tsv"
    for ds in $DSLIST; do
        id=$(awk -v n="$ds" '$2 == n {print $1}' "$D/bucket-list.txt")
        awk -v ds="$ds" -v id="$id" -v rows=$(($(stat -c %s "$IN/$ds.rowbinary") / 16)) '$2 == "data/" id {d = $1} $2 == "wal/" id {w = $1}
            END {printf "%s\t%d\t%d\tTSM + index (engine/data/%s, du -sb) after compaction settled; WAL %d; allocated blocks in du-allocated.txt, files in files.txt\n", ds, rows, d, id, w}' "$D/du.txt" >> "$OUT/p1/i2.tsv"
    done
    crm rb-i2; rmdata "$W"
}
p1_i3() {
    local D=$OUT/p1/i3 W=$DBCMP/s1/i3 ds
    mkdir -p "$D"; rmdata "$W"
    # --force-snapshot-max-age 5m: the snapshot that writes Parquet comes after 5 min instead of 1 h; the Parquet
    # layout (gen1 files of 10 min of data time) does not depend on it
    i3_start "$W" --force-snapshot-max-age "$I3_P1_AGE" || return 1
    ports_check "$D/ports.txt"
    # one database, one table per data set: InfluxDB 3 Core allows 5 databases (the 0.9.0 run's database per data
    # set failed past the fifth); the line protocol names the table, so each data set gets its own copy, s_<ds>
    curl -s -X POST http://127.0.0.1:8181/api/v3/configure/database -H 'Content-Type: application/json' -d '{"db":"sizes"}' >> "$D/ddl.txt"
    for ds in $DSLIST; do
        [ -s "$IN/$ds.tbl.lp" ] || sed "s/^sensors,/s_$ds,/" "$IN/$ds.lp" > "$IN/$ds.tbl.lp"
        "$B/loadlp" "$IN/$ds.tbl.lp" 127.0.0.1 8181 "/api/v3/write_lp?db=sizes&precision=millisecond" "" 100000 16 >> "$D/load.txt" 2>&1 9>&- || fail "p1 i3 load $ds"
    done
    i3_wait_snapshot "$W" "$I3_P1_MIN" "$I3_P1_STABLE" "$I3_P1_MAX" > "$D/snapshot.txt" 2>&1 || fail "p1 i3 snapshot did not settle (see $D/snapshot.txt)"
    podman unshare find "$W/data" -maxdepth 4 -type d > "$D/dirs.txt"
    podman unshare sh -c "cd $W/data && du -sb */* */*/* 2>/dev/null" > "$D/du.txt"
    printf 'ds\trows\tbytes\tnote\n' > "$OUT/p1/i3.tsv"
    for ds in $DSLIST; do
        curl -s "http://127.0.0.1:8181/api/v3/query_sql?db=sizes&format=json" --data-urlencode "q=SELECT count(*) AS files, sum(size_bytes) AS bytes, sum(row_count) AS rows FROM system.parquet_files WHERE table_name = 's_$ds'" -G > "$D/pq.$ds.json"
        jq -r --arg ds "$ds" '.[0] | "\($ds)\t\(.rows)\t\(.bytes)\tParquet files (system.parquet_files): \(.files); WAL apart (du.txt)"' "$D/pq.$ds.json" >> "$OUT/p1/i3.tsv" 2>> "$D/errors.txt" || fail "p1 i3 size $ds"
    done
    crm rb-i3; rmdata "$W"
}
p1_prom() {
    local D=$OUT/p1/prom W=$DBCMP/s1/prom ds
    mkdir -p "$D" "$W"
    printf 'ds\trows\tbytes\tnote\n' > "$OUT/p1/prom.tsv"
    for ds in $DSLIST; do
        mkdir -p "$W/$ds" && podman unshare chown 65534:65534 "$W/$ds"
        podman run --rm --name rb-promtool --network none -v "$IN:/input:ro" -v "$W/$ds:/prometheus" --entrypoint /bin/promtool "$PROM_IMG" \
            tsdb create-blocks-from openmetrics --max-block-duration=744h -q "/input/$ds.om" /prometheus > "$D/$ds.txt" 2>&1 9>&- || { fail "p1 prom $ds"; continue; }
        podman unshare sh -c "cd $W/$ds && du -scb 01* | tail -1; ls -d 01* | wc -l" >> "$D/$ds.txt"
        awk -v ds="$ds" -v rows=$(($(stat -c %s "$IN/$ds.rowbinary") / 16)) '/total$/ {b = $1} /^[0-9]+$/ {n = $1} END {printf "%s\t%d\t%d\tpromtool backfill blocks (%d), float64\n", ds, rows, b, n}' "$D/$ds.txt" >> "$OUT/p1/prom.tsv"
        podman unshare rm -rf "$W/$ds"
    done
    rmdata "$W"
}
phase1() {
    reset_quiet
    local s
    for s in $P1_SYSTEMS; do
        done_mark "$OUT/p1/$s.done" && { log "p1 $s already done"; continue; }
        log "p1 $s start"
        local nf=$(cat "$PH_DIR/FAILS" 2> /dev/null | wc -l)
        "p1_$s" || fail "p1 $s returned an error"
        cleanup_all
        how_write 1 "$s"
        [ "$(cat "$PH_DIR/FAILS" 2> /dev/null | wc -l)" = "$nf" ] && date > "$OUT/p1/$s.done"
        log "p1 $s end"
    done
    python3 -I "$TOOLS/sizes_summary.py" "$OUT/p1" > "$OUT/p1/sizes.md" 2>&1 || fail "sizes_summary"
}

# ================================================================== phase 2: the 100 M-row table
p2_tsdb() {
    local D=$OUT/p2/tsdb W=$DBCMP/s2/tsdb r
    mkdir -p "$D" "$WORK/ref"
    for r in $(seq "$REPS"); do
        rmdata "$W"; mkdir -p "$W"
        tsdbd_start "$W/db" || return 1
        warm "$DATA"
        tstep "p2 tsdb load r$r" "$D/load.$r" -- bash -c "'$T/tsdbc-bench' load '$DATA' --conns 4 --port $TSDB_PORT && sync -f '$DBCMP' && '$TB/tsdbstat' settle && du -sb --exclude='retired_*' '$W/db'"
        [ "$r" -lt "$REPS" ] && tsdbd_stop
    done
    load_ok "p2 tsdb" || return 1
    ports_check "$D/ports.txt"
    cd "$WORK" || return 1
    untimed "$D/ref-windows.log" sh -c "'$TB/tsdbq' dump spec.tsv 200 > ref/windows.jsonl"
    untimed "$D/ref-full.log" sh -c "'$TB/tsdbq' full 1 1 dump 2> ref/full.jsonl"
    untimed "$D/ref-lttb.log" "$B/relq" lttb 1 1 ref/lttb.jsonl
    untimed "$D/ref-twa.log" "$B/relq" twa 1 1 ref/twa.jsonl
    for k in $TWA_KINDS $TWA_WKINDS; do untimed "$D/ref-$k.log" env TWA_SPEC="$WORK/spec.tsv" "$B/relq" "$k" 1 1 "ref/$k.jsonl"; done
    tstep "p2 tsdb full c1" "$D/full.c1.txt" -- "$TB/tsdbq" full 1 "$REPS"
    tstep "p2 tsdb full c16" "$D/full.c16.txt" -- "$TB/tsdbq" full 16 "$REPS"
    tstep "p2 tsdb lttb c1" "$D/lttb.c1.txt" -- "$B/relq" lttb 1 "$REPS"
    tstep "p2 tsdb lttb c16" "$D/lttb.c16.txt" -- "$B/relq" lttb 16 "$REPS"
    tstep "p2 tsdb twa c1" "$D/twa.c1.txt" -- "$B/relq" twa 1 "$REPS"
    tstep "p2 tsdb twa c16" "$D/twa.c16.txt" -- "$B/relq" twa 16 "$REPS"
    tsdbtwa "$D" "" 1 16
    tsdbsuite tsdbq "$D" "1 32"
    tsdbbsuite tsdbq "$D"
    tstep "p2 tsdbc-bench query c1" "$D/tsdbc-bench.c1.txt" -- "$T/tsdbc-bench" query --conns 1 --seconds 5 --port "$TSDB_PORT"
    tstep "p2 tsdbc-bench query c32" "$D/tsdbc-bench.c32.txt" -- "$T/tsdbc-bench" query --conns 32 --seconds 5 --port "$TSDB_PORT"
    # partitions merged (README row "tsdbd, 1 connection, partitions merged")
    tstep "p2 tsdb optimize" "$D/optimize.txt" -- bash -c "'$TB/tsdbstat' optimize && du -sb --exclude='retired_*' '$W/db'"
    tstep "p2 tsdb merged full c1" "$D/merged.full.c1.txt" -- "$TB/tsdbq" full 1 "$REPS"
    tstep "p2 tsdb merged full c16" "$D/merged.full.c16.txt" -- "$TB/tsdbq" full 16 "$REPS"
    tstep "p2 tsdb merged lttb c1" "$D/merged.lttb.c1.txt" -- "$B/relq" lttb 1 "$REPS"
    tstep "p2 tsdb merged lttb c16" "$D/merged.lttb.c16.txt" -- "$B/relq" lttb 16 "$REPS"
    tstep "p2 tsdb merged twa c1" "$D/merged.twa.c1.txt" -- "$B/relq" twa 1 "$REPS"
    tstep "p2 tsdb merged twa c16" "$D/merged.twa.c16.txt" -- "$B/relq" twa 16 "$REPS"
    tsdbtwa "$D" merged. 1 16
    tsdbsuite merged "$D" "1"
    tsdbbsuite merged "$D"
    tsdbd_percore "$W/db" && PC=1 tsdbbsuite merged "$D"
    tsdbd_stop
    cd "$OUT" || true
    runstats "$D"
    rmdata "$W"
}
ch_load_rep() { # one load: fresh server and table, ch_load.sh (insert, syncfs, merges), size
    local W=$1
    rmdata "$W"
    ch_start "$W" || return 1
    CHC -n -q "CREATE DATABASE cmp; CREATE TABLE cmp.t100m (series UInt32, ts DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)), value Float32 CODEC(Gorilla, ZSTD(1))) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY (series, ts)" || return 1
}
p2_ch() {
    local D=$OUT/p2/ch W=$DBCMP/s2/ch r
    mkdir -p "$D"
    for r in $(seq "$REPS"); do
        crm rb-ch
        ch_load_rep "$W" || { fail "p2 ch start r$r"; return 1; }
        warm "$DATA"
        tstep "p2 ch load r$r" "$D/load.$r" -- bash -c "'$H/ch_load.sh' t100m && '$CLICKHOUSE' client --host 127.0.0.1 -q \"SELECT 'size', count(), sum(rows), sum(bytes_on_disk) FROM system.parts WHERE database = 'cmp' AND table = 't100m' AND active\""
        [ "$r" -lt "$REPS" ] && { crm rb-ch; rmdata "$W"; }
    done
    load_ok "p2 ch" || return 1
    ports_check "$D/ports.txt"
    CHC -q "SELECT version()" > "$D/version.txt"
    cd "$WORK" || return 1
    python3 -I "$H/mkq.py" ch
    untimed "$D/check.txt" python3 -I "$H/check.py" ch 200
    (cd "$WORK" && python3 -I "$H/mkbatch.py" ch) || fail "mkbatch ch"
    chsuite ch "$D" ch
    bcheck ch "$D" && { chbsuite ch "$D" ch; pc_pin rb-ch && PC=1 chbsuite ch "$D" ch; pc_unpin rb-ch; }
    REL q ch lttb > "$D/q.lttb.sql"; REL q ch twa > "$D/q.twa.sql"
    untimed "$D/check-lttb.txt" REL check ch lttb ref/lttb.jsonl
    untimed "$D/check-twa.txt" REL check ch twa ref/twa.jsonl
    for k in lttb twa; do  # timed by the server (query_log), not the client (REPORT §3)
        chbench1 1 1 0 "$D/q.$k.sql" > /dev/null 2> "$D/warm.$k" 9>&-
        for r in $(seq "$REPS"); do tstep "$k r$r" "$D/$k.$r" -- chbench_tag "$k.$r.$$" 1 1 0 "$D/q.$k.sql"; done
    done
    twasuite ch "$D" ch
    CHC -q "SELECT count(), sum(bytes_on_disk) FROM system.parts WHERE database = 'cmp' AND table = 't100m' AND active" > "$D/size-end.txt"
    cd "$OUT" || true
    crm rb-ch; rmdata "$W"
    runstats "$D"
}
qdb_load_rep() { # W PARQUET(0|1|2): fresh server, table, load, WAL applied, [convert], sync, size
    local W=$1 pq=$2 s e
    s=$(date +%s.%N)
    "$B/loadlp" "$DBCMP/input/mixed.ilp32" 127.0.0.1 9100 "/write?precision=ms" "" 100000 8 || return 1
    "$H/qdb_wait.sh" || return 1
    if [ "$pq" != 0 ]; then
        e=$(date +%s.%N)
        QX "ALTER TABLE sensors CONVERT PARTITION TO PARQUET WHERE ts >= 0"
        qdb_wait_table sensors || return 1
        echo "conversion to Parquet applied $(echo "$(date +%s.%N) - $e" | bc) s after the WAL apply (load + WAL + conversion $(echo "$(date +%s.%N) - $s" | bc) s)"
    fi
    local t=$(date +%s.%N)
    podman exec rb-qdb sync -f /var/lib/questdb/db
    echo "sync $(echo "$(date +%s.%N) - $t" | bc) s"
    QX "SELECT isParquet, count(), sum(diskSize), sum(parquetFileSize) FROM table_partitions('sensors')"
}
p2_qdb() { # PARQUET(0|1|2): native, Parquet LZ4_RAW (QuestDB's default codec), Parquet ZSTD (QDB_ZSTD_LEVEL)
    local pq=$1 P=qdb D W r
    [ "$pq" = 1 ] && P=qdb-parquet
    [ "$pq" = 2 ] && P=qdb-parquet-zstd
    local QDB_ZSTD=0; [ "$pq" = 2 ] && QDB_ZSTD=1
    D=$OUT/p2/$P W=$DBCMP/s2/$P
    mkdir -p "$D"
    for r in $(seq "$REPS"); do
        crm rb-qdb; rmdata "$W"
        qdb_start "$W" || return 1
        QX "CREATE TABLE sensors (series SYMBOL CAPACITY 128 CACHE, value FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL" > "$D/ddl.$r.txt"
        warm "$DBCMP/input/mixed.ilp32"
        tstep "p2 $P load r$r" "$D/load.$r" -- qdb_load_rep "$W" "$pq"
        [ "$r" -lt "$REPS" ] && { crm rb-qdb; rmdata "$W"; }
    done
    load_ok "p2 $P" || return 1
    ports_check "$D/ports.txt"
    sleep 60  # converted partitions' old files go asynchronously
    QX "SELECT isParquet, count(), sum(diskSize), sum(parquetFileSize) FROM table_partitions('sensors')" > "$D/size-final.json"
    podman unshare du -sb "$W"/db/sensors~* > "$D/du-final.txt" 2>&1
    QX "select build()" > "$D/version.txt"
    cd "$WORK" || return 1
    python3 -I "$H/mkq.py" qdb && python3 -I "$H/mkq.py" qdbh
    untimed "$D/check.txt" python3 -I "$H/check.py" qdb 200
    local PG="host=127.0.0.1 port=8812 user=admin password=quest dbname=qdb"
    qsuite qdb "$P" "$D" pg "$PG"
    bcheck qdb "$D" && { bsuite qdb "$P" "$D" pg "$PG"; pc_pin rb-qdb && PC=1 bsuite qdb "$P" "$D" pg "$PG"; pc_unpin rb-qdb; }
    REL q qdb twa > "$D/q.twa.sql"  # native twap() + SAMPLE BY 1h (REPORT §5.10), timed only when it agrees
    untimed "$D/check-twa.txt" REL check qdb twa ref/twa.jsonl && qline twa "$D" "$D/q.twa.sql" pg "$PG"
    REL q qdbseg twa > "$D/q.twaseg.sql"  # the 0.9.0 run's lead() CTE, for comparison
    untimed "$D/check-twaseg.txt" REL check qdbseg twa ref/twa.jsonl && qline twaseg "$D" "$D/q.twaseg.sql" pg "$PG"
    twasuite qdb "$D" pg "$PG"
    if [ "$pq" = 0 ]; then
        # HTTP API cross-check (README: within 10% of the PostgreSQL protocol): one run per measurement
        REPS=1 qsuite qdbh "$P-http" "$D" http 127.0.0.1 9100 ""
        # variant: an index on the symbol (README), classes only
        QX "ALTER TABLE sensors ALTER COLUMN series ADD INDEX" > "$D/index.txt"
        qdb_wait_table sensors >> "$D/index.txt"
        FULLS="" qsuite qdb "$P-index" "$D" pg "$PG"
    fi
    cd "$OUT" || true
    crm rb-qdb; rmdata "$W"
    runstats "$D"
}
ts_load_rep() { # one load into rb-ts (fresh): COPY, size, compress, VACUUM, size
    PSQL -c "ALTER SYSTEM SET max_wal_size = '16GB'" -c "SELECT pg_reload_conf()" > /dev/null
    PSQL -c "CREATE TABLE t100m (series int NOT NULL, ts timestamptz NOT NULL, value real NOT NULL)" \
        -c "SELECT create_hypertable('t100m', by_range('ts', INTERVAL '1 day'))" \
        -c "ALTER TABLE t100m SET (timescaledb.compress, timescaledb.compress_segmentby = 'series', timescaledb.compress_orderby = 'ts')" || return 1
}
ts_timed() { # CONTAINER: the timed part of a load
    sync
    local s=$(date +%s.%N)
    podman exec "$1" timescaledb-parallel-copy -connection "host=127.0.0.1 port=5434 user=postgres password=bench sslmode=disable dbname=postgres" \
        -table t100m -file /data/mixed.csv -workers 16 -batch-size 10000 -columns series,ts,value -disable-direct-compress || return 1
    echo "copy $(echo "$(date +%s.%N) - $s" | bc) s"
    echo "before compression: $(PSQL -Atc "SELECT hypertable_size('t100m')")"
    TS_PORT=5434 "$H/ts_compress.sh" 16 || return 1
    PSQL -c "VACUUM t100m" && echo "after VACUUM: $(PSQL -Atc "SELECT hypertable_size('t100m')")"
}
p2_ts() {
    local D=$OUT/p2/ts W=$DBCMP/s2/ts r
    mkdir -p "$D"
    for r in $(seq "$REPS"); do
        crm rb-ts; rmdata "$W"
        ts_start rb-ts "$TS_IMG" "$W" "$DBCMP/input" || return 1
        ts_load_rep > "$D/setup.$r.txt" 2>&1 || { fail "p2 ts setup r$r"; return 1; }
        warm "$DBCMP/input/mixed.csv"
        tstep "p2 ts load r$r" "$D/load.$r" -- ts_timed rb-ts
        [ "$r" -lt "$REPS" ] && { crm rb-ts; rmdata "$W"; }
    done
    load_ok "p2 ts" || return 1
    ports_check "$D/ports.txt"
    PSQL -Atc "select version()" -c "select extversion from pg_extension where extname='timescaledb'" > "$D/version.txt"
    cd "$WORK" || return 1
    python3 -I "$H/mkq.py" ts
    untimed "$D/check.txt" python3 -I "$H/check.py" ts 200
    qsuite ts ts "$D" pg "host=127.0.0.1 port=5434 user=postgres password=bench dbname=postgres options='-c TimeZone=UTC'"
    bcheck ts "$D" && {
        for n in ${FULLS-$FULLS_DEFAULT}; do  # like with like (REPORT §3, §5.11): one query per series, 16 connections
            reps "ts.full$n.per.c16" "$D" -- "$B/qdrive" pg "host=127.0.0.1 port=5434 user=postgres password=bench dbname=postgres options='-c TimeZone=UTC'" "$WORK/q/ts.full$n.per.txt" 16 100000 0
        done
        bsuite ts ts "$D" pg "host=127.0.0.1 port=5434 user=postgres password=bench dbname=postgres options='-c TimeZone=UTC'"
        pc_pin rb-ts && PC=1 bsuite ts ts "$D" pg "host=127.0.0.1 port=5434 user=postgres password=bench dbname=postgres options='-c TimeZone=UTC -c max_parallel_workers_per_gather=0'"
        pc_unpin rb-ts; }
    cd "$OUT" || true
    crm rb-ts; rmdata "$W"
    # LTTB and TWA need the toolkit, which is in the -ha image (same TimescaleDB 2.31.0, PostgreSQL 17.11) only:
    # one more load there (timed once, for the record), then the two queries
    W=$DBCMP/s2/tsha
    rmdata "$W"
    ts_start rb-tsha "$TSHA_IMG" "$W" "$DBCMP/input" || return 1
    { PSQL -c "CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit"; ts_load_rep; } > "$D/ha-setup.txt" 2>&1 || { fail "p2 tsha setup"; return 1; }
    ports_check "$D/ports-ha.txt"
    warm "$DBCMP/input/mixed.csv"
    tstep "p2 tsha load" "$D/ha-load.txt" -- ts_timed rb-tsha
    PSQL -Atc "select version()" -c "select extname, extversion from pg_extension" -c "show shared_buffers" -c "show work_mem" > "$D/ha-version.txt"
    cd "$WORK" || return 1
    REL q ts lttb > "$D/q.lttb.sql"; REL q ts twa > "$D/q.twa.sql"
    untimed "$D/check-lttb.txt" REL check ts lttb ref/lttb.jsonl
    untimed "$D/check-twa.txt" REL check ts twa ref/twa.jsonl
    untimed "$D/explain-twa.txt" PSQL -c "EXPLAIN (ANALYZE, BUFFERS) $(cat "$D/q.twa.sql")"
    grep -q 'Disk' "$D/explain-twa.txt" && log "  ts: the TWA plan still spills to disk with work_mem=$TS_WORK_MEM (see $D/explain-twa.txt)"
    local PG="host=127.0.0.1 port=5434 user=postgres password=bench dbname=postgres options='-c TimeZone=UTC'"
    qline lttb "$D" "$D/q.lttb.sql" pg "$PG"
    qline twa "$D" "$D/q.twa.sql" pg "$PG"
    twasuite ts "$D" pg "$PG"
    cd "$OUT" || true
    crm rb-tsha; rmdata "$W"
    runstats "$D"
}
i2_timed() {
    local s=$(date +%s)
    "$B/loadlp" "$DBCMP/input/mixed.lp" 127.0.0.1 8086 "/api/v2/write?org=bench&bucket=cmp&precision=ms" $'Authorization: Token benchtoken\r\n' 5000 8 || return 1
    I2_PORT=8086 "$H/i2_settle.sh" "$(date +%s)" || return 1
    echo "size: $(podman unshare du -sb "$1/data" | cut -f1) bytes (du -sb of the data directory)"
}
p2_i2() {
    local D=$OUT/p2/i2 W=$DBCMP/s2/i2 r bid
    mkdir -p "$D"
    for r in $(seq "$REPS"); do
        crm rb-i2; rmdata "$W"
        i2_start "$W" || return 1
        bid=$(I2 bucket list --name cmp --hide-headers 9>&- | awk '{print $1}')
        I2 v1 dbrp create --db cmp --rp autogen --bucket-id "$bid" --default > "$D/dbrp.$r.txt" 2>&1 9>&-
        warm "$DBCMP/input/mixed.lp"
        tstep "p2 i2 load r$r" "$D/load.$r" -- i2_timed "$W"
        [ "$r" -lt "$REPS" ] && { crm rb-i2; rmdata "$W"; }
    done
    load_ok "p2 i2" || return 1
    ports_check "$D/ports.txt"
    podman exec rb-i2 influxd version > "$D/version.txt" 2>&1
    # InfluxDB 2 answers a failed InfluxQL statement with status 200 and "error": in its results, and a Flux error
    # raised after the reply started as a CSV table headed ,error,reference: qdrive fails on either (untimed search)
    local QDRIVE_ERR_SUB='"error":|,error,reference'; export QDRIVE_ERR_SUB
    cd "$WORK" || return 1
    python3 -I "$H/mkq.py" i2ql && python3 -I "$H/mkq.py" i2flux
    untimed "$D/check-i2ql.txt" python3 -I "$H/check.py" i2ql 200
    untimed "$D/check-i2flux.txt" python3 -I "$H/check.py" i2flux 200
    qsuite i2ql i2ql "$D" http 127.0.0.1 8086 $'Authorization: Token benchtoken\r\n'
    qsuite i2flux i2flux "$D" http 127.0.0.1 8086 $'Authorization: Token benchtoken\r\nAccept: application/csv\r\n'
    local x
    for x in 0 1; do
        bcheck i2ql "$D" && PC=$x bsuite i2ql i2ql "$D" http 127.0.0.1 8086 $'Authorization: Token benchtoken\r\n'
        bcheck i2flux "$D" && PC=$x bsuite i2flux i2flux "$D" http 127.0.0.1 8086 $'Authorization: Token benchtoken\r\nAccept: application/csv\r\n'
        [ $x = 0 ] && { pc_pin rb-i2 || break; }
    done
    pc_unpin rb-i2
    TWA_WKINDS="" twasuite i2flux "$D" http 127.0.0.1 8086 $'Authorization: Token benchtoken\r\nAccept: application/csv\r\n'
    cd "$OUT" || true
    crm rb-i2; rmdata "$W"
    runstats "$D"
}
p2_i3() {
    # InfluxDB 3 Core was OOM-killed at its 28 GB limit by 32 concurrent batch queries over 65,000 Parquet files
    # (smoke): recorded as such and restarted (tstep), not a harness failure
    local QCTR=rb-i3 QREADY="curl -sf http://127.0.0.1:8181/health"
    local D=$OUT/p2/i3 W=$DBCMP/s2/i3 r
    mkdir -p "$D"
    for r in $(seq "$REPS"); do
        crm rb-i3; rmdata "$W"
        i3_start "$W" $I3_P2_FLAGS || return 1
        curl -s -X POST http://127.0.0.1:8181/api/v3/configure/database -H 'Content-Type: application/json' -d '{"db":"cmp"}' > "$D/ddl.$r.txt"
        warm "$DBCMP/input/mixed.lp"
        tstep "p2 i3 load r$r" "$D/load.$r" -- "$B/loadlp" "$DBCMP/input/mixed.lp" 127.0.0.1 8181 "/api/v3/write_lp?db=cmp&precision=millisecond" "" 100000 16
        [ "$r" -lt "$REPS" ] && { crm rb-i3; rmdata "$W"; }
    done
    load_ok "p2 i3" || return 1
    # default --force-snapshot-max-age (1 h): Parquet appears about an hour after the load (README); waited once
    i3_wait_snapshot "$W" "$I3_P2_MIN" "$I3_P2_STABLE" "$I3_P2_MAX" > "$D/snapshot.txt" 2>&1 || fail "p2 i3 snapshot did not settle (see $D/snapshot.txt)"
    ports_check "$D/ports.txt"
    podman unshare sh -c "cd $W/data/node0 2>/dev/null && du -sb * */* 2>/dev/null" > "$D/du.txt"
    podman exec rb-i3 influxdb3 --version > "$D/version.txt" 2>&1
    cd "$WORK" || return 1
    python3 -I "$H/mkq.py" i3
    untimed "$D/check.txt" python3 -I "$H/check.py" i3 200
    CLASSES="" qsuite i3 i3 "$D" http 127.0.0.1 8181 ""
    FULLS="" qsuite i3 i3 "$D" http 127.0.0.1 8181 ""
    bcheck i3 "$D" && { bsuite i3 i3 "$D" http 127.0.0.1 8181 ""; pc_pin rb-i3 && PC=1 bsuite i3 i3 "$D" http 127.0.0.1 8181 ""; pc_unpin rb-i3; }
    REL q i3 twa > "$D/q.twa.http"
    untimed "$D/check-twa.txt" REL check i3 twa ref/twa.jsonl
    qline twa "$D" "$D/q.twa.http" http 127.0.0.1 8181 ""
    twasuite i3 "$D" http 127.0.0.1 8181 ""
    cd "$OUT" || true
    crm rb-i3; rmdata "$W"
    runstats "$D"
}
phase2() {
    local s
    for s in $P2_SYSTEMS; do
        reset_quiet
        done_mark "$OUT/p2/$s.done" && { log "p2 $s already done"; continue; }
        log "p2 $s start"
        LOAD_FAILED=0
        local nf=$(cat "$PH_DIR/FAILS" 2> /dev/null | wc -l)
        case $s in qdb0) p2_qdb 0 ;; qdb1) p2_qdb 1 ;; qdb2) p2_qdb 2 ;; *) "p2_$s" ;; esac || fail "p2 $s returned an error"
        cleanup_all
        how_write 2 "$s"
        [ "$(cat "$PH_DIR/FAILS" 2> /dev/null | wc -l)" = "$nf" ] && date > "$OUT/p2/$s.done"
        log "p2 $s end"
    done
}

# ================================================================== phase 3: embedded (tsdb, DuckDB, chDB)
p3() {
    local D=$OUT/p3 E=$DBCMP/s3 r e
    mkdir -p "$D" "$E"
    rmdata "$E"; mkdir -p "$E"
    warm "$DATA"
    for r in $(seq "$REPS"); do
        rm -rf "$E/tsdb"; sync
        tstep "p3 tsdb load r$r" "$D/tsdb.load.$r" -- "$TB/embq" tsdb load "$E/tsdb" "$DATA" 4
        rm -f "$E/t.duckdb" "$E/t.duckdb.wal"; sync
        tstep "p3 duckdb load r$r" "$D/duckdb.load.$r" -- "$TB/embq" duckdb load "$E/t.duckdb" "$DATA" 4
        rm -rf "$E/chdb"; sync
        tstep "p3 chdb load r$r" "$D/chdb.load.$r" -- "$B/chq" load "$E/chdb" "$DATA" 1
        rm -rf "$E/chdb-lz4"; sync
        tstep "p3 chdb-lz4 load r$r" "$D/chdb-lz4.load.$r" -- "$B/chq" load "$E/chdb-lz4" "$DATA" 0
    done
    load_ok "p3" || return 1
    du -sb "$E"/* > "$D/du.txt"
    cd "$WORK" || return 1
    mkdir -p dump
    local RW=ref/windows.jsonl RF=ref/full.jsonl RL=ref/lttb.jsonl RT=ref/twa.jsonl
    untimed "$D/dump-tsdb.log" sh -c "'$TB/embq' tsdb dump '$E/tsdb' spec.tsv > dump/tsdb-windows.jsonl"
    untimed "$D/dump-tsdb-full.log" "$TB/embq" tsdb full "$E/tsdb" 1 1 dump/tsdb-full.jsonl
    untimed "$D/ref-lttb-emb.log" env TSDB_DIR="$E/tsdb" "$B/relemb" lttb 1 1 dump/tsdb-lttb.jsonl
    untimed "$D/ref-twa-emb.log" env TSDB_DIR="$E/tsdb" "$B/relemb" twa 1 1 dump/tsdb-twa.jsonl
    if [ ! -s "$RW" ] || [ ! -s "$RT" ]; then  # phase 2 gave no tsdbd reference: the embedded tsdb answers stand in
        log "p3: no tsdbd reference in ref/; checking against embedded tsdb's answers"
        RW=dump/tsdb-windows.jsonl RF=dump/tsdb-full.jsonl RL=dump/tsdb-lttb.jsonl RT=dump/tsdb-twa.jsonl
    fi
    untimed "$D/dump-duckdb.log" sh -c "'$TB/embq' duckdb dump '$E/t.duckdb' spec.tsv > dump/duckdb-windows.jsonl"
    untimed "$D/dump-duckdb-full.log" "$TB/embq" duckdb full "$E/t.duckdb" 1 1 dump/duckdb-full.jsonl
    for e in chdb chdb-lz4; do
        untimed "$D/dump-$e.log" sh -c "'$B/chq' dump '$E/$e' spec.tsv > dump/$e-windows.jsonl"
        untimed "$D/dump-$e-full.log" "$B/chq" full "$E/$e" 1 "dump/$e-full.jsonl"
    done
    # embq answers the whole range of series 0-99 whatever the data holds (its file belongs to the multi-series
    # track); with fewer series (the smoke run's NSERIES=10) the rest are empty answers, not in tsdbd's or chDB's dumps
    local f
    [ "${NSERIES:-100}" -lt 100 ] && for f in dump/tsdb-full.jsonl dump/duckdb-full.jsonl; do
        jq -c --argjson n "$NSERIES" 'select(.s < $n)' "$f" > "$f.tmp" && mv "$f.tmp" "$f"
    done
    # tsdbd's reference holds the first 200 windows per class (check.py SYS 200), embedded tsdb answers all of spec.tsv:
    # the comparison takes the windows both answered (check.py counts any other answer as a failure)
    python3 -I -c 'import json, sys
ids = {json.loads(l)["i"] for l in open(sys.argv[1])}
sys.stdout.writelines(l for l in open(sys.argv[2]) if json.loads(l)["i"] in ids)' "$RW" dump/tsdb-windows.jsonl > dump/tsdb-windows.inref.jsonl
    {
        python3 -I "$H/check.py" --dumps "$RW" "$RF" dump/tsdb-windows.inref.jsonl dump/tsdb-full.jsonl
        # as README: the other engines against embedded tsdb's answers to every query (tsdbd's ref has 200 per class)
        for e in duckdb chdb chdb-lz4; do python3 -I "$H/check.py" --dumps dump/tsdb-windows.jsonl dump/tsdb-full.jsonl "dump/$e-windows.jsonl" "dump/$e-full.jsonl"; done
        REL jsonl lttb "$RL" dump/tsdb-lttb.jsonl
        REL jsonl twa "$RT" dump/tsdb-twa.jsonl
    } > "$D/check.txt" 2>&1
    grep -E 'mismatches [1-9]|MISMATCH|Traceback|Error' "$D/check.txt" > /dev/null && fail "p3 answer check found mismatches (see $D/check.txt)"
    # whole range
    tstep "p3 tsdb full t1" "$D/tsdb.full.t1.txt" -- "$TB/embq" tsdb full "$E/tsdb" "$REPS" 1
    tstep "p3 tsdb full t32" "$D/tsdb.full.t32.txt" -- "$TB/embq" tsdb full "$E/tsdb" "$REPS" 32
    tstep "p3 duckdb full" "$D/duckdb.full.txt" -- "$TB/embq" duckdb full "$E/t.duckdb" "$REPS" 1
    tstep "p3 chdb full" "$D/chdb.full.txt" -- "$B/chq" full "$E/chdb" "$REPS" /dev/null
    tstep "p3 chdb-lz4 full" "$D/chdb-lz4.full.txt" -- "$B/chq" full "$E/chdb-lz4" "$REPS" /dev/null
    # random windows and points
    local c t
    for c in raw agg1d agg7d point; do
        "$TB/embq" tsdb qps "$E/tsdb" spec.tsv "$c" 1 3 > /dev/null 2>&1 9>&-
        for t in 1 32; do reps "tsdb.$c.t$t" "$D" -- "$TB/embq" tsdb qps "$E/tsdb" spec.tsv "$c" "$t" "$SECS"; done
        "$TB/embq" duckdb qps "$E/t.duckdb" spec.tsv "$c" 1 3 > /dev/null 2>&1 9>&-
        for t in 1 32; do reps "duckdb.$c.t$t" "$D" -- "$TB/embq" duckdb qps "$E/t.duckdb" spec.tsv "$c" "$t" "$SECS"; done
        for e in chdb chdb-lz4; do
            "$B/chq" qps "$E/$e" spec.tsv "$c" 1 3 > /dev/null 2>&1 9>&-
            for t in 1 32; do reps "$e.$c.t$t" "$D" -- "$B/chq" qps "$E/$e" spec.tsv "$c" "$t" "$SECS"; done
        done
    done
    # LTTB and TWA
    tstep "p3 tsdb lttb t1" "$D/tsdb.lttb.t1.txt" -- env TSDB_DIR="$E/tsdb" "$B/relemb" lttb 1 "$REPS"
    tstep "p3 tsdb lttb t32" "$D/tsdb.lttb.t32.txt" -- env TSDB_DIR="$E/tsdb" "$B/relemb" lttb 32 "$REPS"
    tstep "p3 tsdb twa t1" "$D/tsdb.twa.t1.txt" -- env TSDB_DIR="$E/tsdb" "$B/relemb" twa 1 "$REPS"
    tstep "p3 tsdb twa t32" "$D/tsdb.twa.t32.txt" -- env TSDB_DIR="$E/tsdb" "$B/relemb" twa 32 "$REPS"
    tstep "p3 duckdb twa" "$D/duckdb.twa.txt" -- "$B/duckq" sqlt "$E/t.duckdb" "$REPS" dump/duckdb-twa.tsv "$(REL sql duck twa)"
    for e in chdb chdb-lz4; do
        tstep "p3 $e lttb" "$D/$e.lttb.txt" -- "$B/chq" sqlt "$E/$e" "$REPS" "dump/$e-lttb.tsv" "$(REL sql chdb lttb)"
        tstep "p3 $e twa" "$D/$e.twa.txt" -- "$B/chq" sqlt "$E/$e" "$REPS" "dump/$e-twa.tsv" "$(REL sql chdb twa)"
    done
    {
        REL tsv twa "$RT" dump/duckdb-twa.tsv
        for e in chdb chdb-lz4; do REL tsv lttb "$RL" "dump/$e-lttb.tsv"; REL tsv twa "$RT" "dump/$e-twa.tsv"; done
    } > "$D/check-rel.txt" 2>&1
    grep -qE 'MISMATCH|Traceback|Error' "$D/check-rel.txt" && fail "p3 LTTB/TWA check found mismatches (see $D/check-rel.txt)"
    cd "$OUT" || true
    runstats "$D"
    rmdata "$E"
}
phase3() { reset_quiet; p3 || fail "p3 returned an error"; local e; for e in tsdb-emb duckdb chdb chdb-lz4; do how_write 3 "$e"; done; }

# ================================================================== phase 4: Prometheus
prom_bf_timed() { # DIR: promtool backfill, sync, server start until ready
    local s=$(date +%s.%N) t
    podman run --rm --name rb-promtool --network none -v "$DBCMP/input:/input:ro" -v "$1:/prometheus" --entrypoint /bin/promtool "$PROM_IMG" \
        tsdb create-blocks-from openmetrics --max-block-duration=744h -q /input/mixed.om /prometheus || return 1
    t=$(date +%s.%N); echo "promtool $(echo "$t - $s" | bc) s"
    sync -f "$1"; s=$(date +%s.%N); echo "sync $(echo "$s - $t" | bc) s"
    prom_run rb-prom-bf 9091 "$1" && prom_ready 9091 || return 1
    echo "server ready $(echo "$(date +%s.%N) - $s" | bc) s"
    podman unshare sh -c "cd $1 && du -scb 01* | tail -1 && du -sb wal wbl chunks_head 2>/dev/null"
}
prom_rw_timed() { # DIR SAMPLES SHARDS
    "$B/loadprw" "$DATA" 127.0.0.1 9090 "$2" "$3" || return 1
    local s=$(date +%s.%N)
    sync -f "$1"
    echo "sync after the load $(echo "$(date +%s.%N) - $s" | bc) s"
}
p4() {
    local D=$OUT/p4 P=$DBCMP/s4 r c S
    mkdir -p "$D"; rmdata "$P"; mkdir -p "$P"
    cp "$H/prometheus.yml" "$DBCMP/prometheus.yml"
    cd "$WORK" || return 1
    python3 -I "$H/mkq.py" prom
    # (b) backfill, REPS times into fresh directories; the last stays for the queries
    for r in $(seq "$REPS"); do
        crm rb-prom-bf; podman unshare rm -rf "$P/data-bf"
        mkdir -p "$P/data-bf" && podman unshare chown 65534:65534 "$P/data-bf"
        warm "$DBCMP/input/mixed.om"
        tstep "p4 backfill r$r" "$D/bf.load.$r" -- prom_bf_timed "$P/data-bf"
    done
    ports_check "$D/ports-bf.txt"
    untimed "$D/check-bf.txt" env PROM_PORT=9091 python3 -I "$H/check.py" prom 200
    qsuite prom bf "$D" http 127.0.0.1 9091 ""
    crm rb-prom-bf; podman unshare rm -rf "$P/data-bf"
    # (a) remote write: REPS loads (load + sync) into fresh directories; the last one settles (hours) and is queried
    for r in $(seq "$REPS"); do
        crm rb-prom; podman unshare rm -rf "$P/data"
        mkdir -p "$P/data" && podman unshare chown 65534:65534 "$P/data"
        prom_run rb-prom 9090 "$P/data" && prom_ready 9090 || { fail "p4 prom start r$r"; return 1; }
        warm "$DATA"
        tstep "p4 remote write r$r" "$D/rw.load.$r" -- prom_rw_timed "$P/data" 2000 16
    done
    ports_check "$D/ports-rw.txt"
    # not under the lock (hours): the machine's load is sampled in rw-settle-load.txt
    ( while sleep 300; do echo "$(date +%T) busy $(cpu_busy)"; done ) > "$D/rw-settle-load.txt" 2>&1 9>&- &
    local mon=$!
    timeout "$PROM_SETTLE_TIMEOUT" env PROM_PORT=9090 "$H/prom_settle.sh" "$(date +%s)" > "$D/rw-settle.txt" 2>&1 9>&- || fail "p4 remote write did not settle in $PROM_SETTLE_TIMEOUT s (see $D/rw-settle.txt)"
    kill "$mon" 2> /dev/null
    podman unshare sh -c "cd $P/data && du -scb 01* | tail -1 && du -sb wal wbl chunks_head" > "$D/rw-size.txt" 2>&1
    S=$(curl -s -m 3600 -XPOST 127.0.0.1:9090/api/v1/admin/tsdb/snapshot | jq -r .data.name)
    [ -n "$S" ] && [ "$S" != null ] && podman unshare du -sb "$P/data/snapshots/$S" >> "$D/rw-size.txt" && podman unshare rm -rf "$P/data/snapshots/$S"
    untimed "$D/check-rw.txt" env PROM_PORT=9090 python3 -I "$H/check.py" prom 200
    qsuite prom rw "$D" http 127.0.0.1 9090 ""
    crm rb-prom; podman unshare rm -rf "$P/data"
    # (a') more shards and larger requests: load time only, fresh directories
    for c in "2000 50" "20000 16" "20000 50"; do
        for r in $(seq "$REPS"); do
            crm rb-prom-sweep; podman unshare rm -rf "$P/data-sweep"
            mkdir -p "$P/data-sweep" && podman unshare chown 65534:65534 "$P/data-sweep"
            prom_run rb-prom-sweep 9090 "$P/data-sweep" && prom_ready 9090 || { fail "p4 sweep start"; continue; }
            warm "$DATA"
            tstep "p4 sweep $c r$r" "$D/sweep.${c// /x}.$r" -- prom_rw_timed "$P/data-sweep" $c
        done
    done
    crm rb-prom-sweep
    cd "$OUT" || true
    rmdata "$P"
    runstats "$D"
}
phase4() { reset_quiet; p4 || fail "p4 returned an error"; how_write 4 prom; }

# ================================================================== phase 5: 100 billion rows, tsdb and ClickHouse
ch_big_load() { # 8 streams of 1250 series, RowBinary from scale-bench export; syncfs; merges settled
    local s e f i
    s=$(date +%s.%N)
    for i in 0 1 2 3 4 5 6 7; do
        ( "$T/scale-bench" export --series "$P5_SERIES" --points "$P5_POINTS" --from $((i * P5_SERIES / 8)) --to $(((i + 1) * P5_SERIES / 8)) |
            "$CLICKHOUSE" client --host 127.0.0.1 -q "INSERT INTO scale.big FORMAT RowBinary" ) &
    done
    local bad=0
    for i in 0 1 2 3 4 5 6 7; do wait -n || bad=1; done
    e=$(date +%s.%N)
    sync -f /var/tmp
    f=$(date +%s.%N)
    echo "insert (8 streams) $(echo "$e - $s" | bc) s, syncfs $(echo "$f - $e" | bc) s, failed streams: $bad"
    local last prev cur
    last=$(date +%s); prev=""
    while :; do
        cur=$(CHC -q "SELECT (SELECT count() FROM system.parts WHERE database='scale' AND active), (SELECT count() FROM system.merges WHERE database='scale')")
        [ "$cur" != "$prev" ] && { last=$(date +%s); prev=$cur; }
        [ $(($(date +%s) - last)) -ge 60 ] && break
        sleep 1
    done
    echo "merges settled $(echo "$last - $e" | bc) s after the insert returned (parts, merges: $prev)"
    CHC -q "SELECT count(), sum(rows), sum(bytes_on_disk) FROM system.parts WHERE database = 'scale' AND table = 'big' AND active"
    return $bad
}
p5() {
    local D=$OUT/p5 W=/var/tmp/big C=/var/tmp/ch-big r avail
    mkdir -p "$D"
    avail=$(df -B1G --output=avail /var/tmp | tail -1 | tr -d ' ')
    echo "free on /var/tmp: $avail GB" > "$D/disk.txt"
    [ "$avail" -ge "$P5_MINFREE" ] || { fail "p5: only $avail GB free on /var/tmp, need about $P5_MINFREE (tsdb 116 GB + ClickHouse 170 GB + margin)"; return 1; }
    rmdata "$W" "$C"
    tstep "p5 tsdb load" "$D/tsdb.load.txt" -- "$T/scale-bench" load --dir "$W" --series "$P5_SERIES" --points "$P5_POINTS" --threads 8
    du -sb --exclude='retired_*' "$W" > "$D/tsdb.du.txt"
    mkdir -p "$D/sql"
    "$T/scale-bench" query --dir "$W" --series "$P5_SERIES" --threads 32 --sql "$D/sql" > "$D/warm.tsdb-emb" 2>&1 9>&-  # warm-up, writes the SQL
    reps tsdb-emb "$D" -- "$T/scale-bench" query --dir "$W" --series "$P5_SERIES" --threads 32
    tsdbd_start "$W" || return 1
    "$T/scale-bench" query --port "$TSDB_PORT" --series "$P5_SERIES" --threads 32 > "$D/warm.tsdbd" 2>&1 9>&-
    reps tsdbd "$D" -- "$T/scale-bench" query --port "$TSDB_PORT" --series "$P5_SERIES" --threads 32
    tsdbd_stop
    # ClickHouse on the same file system
    crm rb-ch
    mkdir -p "$C/data"
    ch_start "$C" || return 1
    ports_check "$D/ports.txt"
    CHC -n -q "CREATE DATABASE scale; CREATE TABLE scale.big (series UInt32, ts DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)), value Float32 CODEC(Delta, ZSTD(1))) ENGINE = MergeTree PARTITION BY toDate(ts) ORDER BY (series, ts)" || { fail "p5 ch create"; return 1; }
    tstep "p5 ch load" "$D/ch.load.txt" -- ch_big_load
    "$ROOT/tools/scale-bench-ch.sh" "$D/sql" scale.big "$CLICKHOUSE" > "$D/warm.ch" 2>&1 9>&-
    reps ch "$D" -- "$ROOT/tools/scale-bench-ch.sh" "$D/sql" scale.big "$CLICKHOUSE"
    CHC -q "SELECT count(), sum(rows), sum(bytes_on_disk) FROM system.parts WHERE database = 'scale' AND active" > "$D/ch.size-end.txt"
    CHC -q "SELECT name, data_compressed_bytes, data_uncompressed_bytes FROM system.columns WHERE database = 'scale' AND table = 'big'" > "$D/ch.columns.txt"
    crm rb-ch
    runstats "$D"
    rmdata "$W" "$C"
    cp /var/tmp/big.log "$D/tsdbd.log" 2> /dev/null
    rm -f /var/tmp/big.log /var/tmp/big.pf
}
phase5() { reset_quiet; p5 || fail "p5 returned an error"; }

# ================================================================== checktest: the answer checkers have teeth
phase_checktest() {
    local D=$OUT/checktest rc=0
    PH_DIR=$D; mkdir -p "$D"; rm -f "$D/FAILS"
    log "CHECKTEST START"
    python3 -I "$RDIR/checktest.py" mutate > "$D/mutate-builtin.txt" 2>&1 || fail "checktest: built-in reference (see $D/mutate-builtin.txt)"
    if [ -s "$WORK/ref/twa.jsonl" ] && [ -s "$WORK/ref/windows.jsonl" ]; then
        python3 -I "$RDIR/checktest.py" mutate "$WORK/ref/twa.jsonl" "$WORK/ref/windows.jsonl" "$WORK/ref/full.jsonl" > "$D/mutate-ref.txt" 2>&1 ||
            fail "checktest: tsdb's references in $WORK/ref (see $D/mutate-ref.txt)"
    fi
    [ -s "$D/FAILS" ] && rc=1
    log "CHECKTEST $([ $rc = 0 ] && echo DONE || echo FAILED)"
    return $rc
}

# ================================================================== smoke: every phase and system on tiny data
# run.sh again in its own DBCMP (SMOKE_DIR, under /var/tmp): prep p1 p2 p3 p4 with every system and variant, 10 series
# x 650 k rows (agg7d windows need > 604800), 4 data sets for p1, REPS=1, SECS=1, batches of 1 and 10, short settles,
# SMOKE=1 (no quiet gate; BENCH.lock shared, per step). Nothing it measures is a result: it passes when every phase
# is DONE, i.e. every answer check passed and no step failed. The tools and downloads (ClickHouse binary, DuckDB,
# chDB, IoTDB's venv, the hydraulic text files) stay the main DBCMP's; the images must be pulled.
phase_smoke() {
    local S=${SMOKE_DIR:-/var/tmp/dbcmp-smoke/full} t0=$SECONDS rc=0
    case $S in /var/tmp/?*) ;; *) log "smoke: SMOKE_DIR must be under /var/tmp, not $S"; return 1 ;; esac
    # SMOKE_PHASES and SMOKE_OUT rerun a part (P2_SYSTEMS etc. pass through) on the inputs of an earlier smoke run
    local ph=${SMOKE_PHASES:-prep p1 p2 p3 p4} so=${SMOKE_OUT:-$S/results}
    case $so in "$S"/?*) ;; *) log "smoke: SMOKE_OUT must be under $S, not $so"; return 1 ;; esac
    rm -rf "$so"; mkdir -p "$so"
    log "SMOKE START: run.sh $ph in $S (log $so/driver.log)"
    env DBCMP="$S" WORK="$S/work" DATA="$S/input/mixed.rowbinary" IN="$S/sizes/in" OUT="$so" \
        CLICKHOUSE="$CLICKHOUSE" DUCKDB_DIR="$DUCKDB_DIR" CHDB_DIR="$CHDB_DIR" IOT_VENV="$IOT_VENV" HYD_RAW="$HYD_RAW" \
        BENCH_LOCK="$LOCK" TSDB_TREE="$TSDB_TREE" TB="$TB" BIN="$BIN" \
        SMOKE=1 FORCE=1 REPS=1 SECS=1 NSERIES=10 TWA_N=60 KS="1 10 100" \
        PREP_SERIES=10 PREP_POINTS=650000 PREP_SIZE_POINTS=200000 PREP_SPEC=400 \
        DSLIST="${SMOKE_DS:-hdd_temp vibration hyd_CE hyd_PS1}" \
        IOT_STABLE=40 TD_STABLE=20 VM_STABLE=10 QDB_WAIT_MAX=900 CH_SETTLE_MAX=600 \
        I3_P2_FLAGS="--force-snapshot-max-age 2m" I3_P2_MIN=150 I3_P1_AGE=2m I3_P1_MIN=150 \
        I3_P2_STABLE=60 I3_P2_MAX=600 I3_P1_STABLE=60 I3_P1_MAX=600 \
        I2_EXTRA="-e INFLUXD_STORAGE_CACHE_SNAPSHOT_WRITE_COLD_DURATION=10s" PROM_SETTLE_TIMEOUT=3600 \
        STEP_TIMEOUT=900 UNTIMED_TIMEOUT=1800 CHECK_TIMEOUT=900 QDRIVE_TIMEOUT=300 \
        "$RDIR/run.sh" $ph || rc=1
    grep -E 'PHASE|FAIL|prep failed|END' "$so/driver.log" | tail -n 30 | sed 's/^/  smoke: /' >> "$OUT/driver.log"
    log "SMOKE $([ $rc = 0 ] && echo DONE || echo FAILED) in $(( (SECONDS - t0) / 60 )) min $(( (SECONDS - t0) % 60 )) s"
    return $rc
}

# ================================================================== the systems added for the 0.9.0 rerun
. "$RDIR/sys_iot.sh"
. "$RDIR/sys_td.sh"
. "$RDIR/sys_vm.sh"

# ================================================================== main
machine() {
    {
        date; uname -a; lscpu | grep -E 'Model name|^CPU\(s\)|Thread|MHz'; free -g | head -2; df -h /var/tmp
        echo "governor: $(cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor | sort | uniq -c | tr '\n' ' ')"
        echo "tsdb tree: $TSDB_TREE $(git -C "$TSDB_TREE" rev-parse HEAD) ($(git -C "$TSDB_TREE" diff --stat | tail -1))"
        echo "harness: $ROOT $(git -C "$ROOT" rev-parse HEAD)"
        podman --version
        for i in $CH_IMG $TS_IMG $TSHA_IMG $QDB_IMG $I2_IMG $I3_IMG "$PROM_IMG" $IOT_IMG $TD_IMG $VM_IMG; do echo "$i $(podman image inspect --format '{{.Digest}} {{.Created}}' "$i" 2> /dev/null)"; done
        echo "duckdb: $DUCKDB_DIR (libduckdb-linux-amd64.zip sha256 $DUCKDB_SHA)"
        echo "chdb: $CHDB_DIR (chdb-core v26.9.0 of 2026-09-28, linux-x86_64-libchdb.tar.gz sha256 $CHDB_SHA)"
        echo "REPS=$REPS SECS=$SECS QUIET_MAX=$QUIET_MAX"
    } > "$OUT/machine.txt" 2>&1
}
usage() {
    cat <<'U'
usage: run.sh PHASE...      (env.sh has the paths; REPS, SECS, FORCE=1, P1_SYSTEMS, P2_SYSTEMS override)
  prep       inputs (not timed): mixed.*, spec.tsv, batches.tsv, per-sensor sets; under flock -s
  p1         sizes per kind of sensor           p2   the 100 M-row table, every server system
  p3         embedded (tsdb, DuckDB, chDB)      p4   Prometheus (backfill, remote write)
  p5         100 billion rows (only when named; not in "all")
  all        prep p1 p2 p3 p4
  smoke      every system end to end on tiny data, every check must pass (DBCMP=/var/tmp/dbcmp-smoke)
  checktest  the answer checker against a crafted data set and mutated answers
A phase or system with a done marker ($OUT/p<n>/<system>.done, $OUT/p<n>.done) is skipped; FORCE=1 runs it again.
U
}
done_mark() { [ "${FORCE:-0}" != 1 ] && [ -e "$1" ]; }
runphase() { # N: one phase in a subshell, FAILS per phase, done marker when it had no failures
    local n=$1 rc
    PH_DIR=$OUT/p$n
    if done_mark "$OUT/p$n.done"; then log "PHASE $n already done ($OUT/p$n.done); FORCE=1 runs it again"; return 0; fi
    mkdir -p "$PH_DIR"
    rm -f "$PH_DIR/FAILS"
    log "PHASE $n START"
    if inputs_ok "$n"; then ( "phase$n" ); rc=$?; else rc=1; fi
    PH_DIR=$OUT/p$n
    cleanup_all
    # tsdbd processes this script started and a failed phase left behind (pid file, checked by command line)
    while read -r pid what dir; do
        [ "${pid:-0}" -gt 1 ] 2> /dev/null && kill -0 "$pid" 2> /dev/null && tr '\0' ' ' < "/proc/$pid/cmdline" | grep -q "tsdbd --dir $dir " && kill -INT "$pid"
    done 2> /dev/null < <(cat "$OUT/pids" 2> /dev/null)
    rmdata "$DBCMP/s$n"
    if [ "$rc" = 0 ] && [ ! -s "$PH_DIR/FAILS" ]; then
        log "PHASE $n DONE"; date > "$OUT/p$n.done"
    else
        log "PHASE $n FAILED: rc=$rc; $(cat "$PH_DIR/FAILS" 2> /dev/null | wc -l) failures, first: $(head -3 "$PH_DIR/FAILS" 2> /dev/null | cut -c1-200 | tr '\n' ';')"
        return 1
    fi
}
# The main part is one function, parsed whole before it runs: bash reads a script as it goes, and an edit (a commit
# in this worktree) during a run of several hours otherwise runs half old and half new lines (seen: a syntax error).
main() {
[ $# -gt 0 ] || { usage; exit 2; }
trap 'log "driver interrupted"; for j in $(jobs -p); do killtree "$j"; done; cleanup_all; exit 130' INT TERM
machine
log "run.sh $* start: REPS=$REPS, SECS=$SECS, results in $OUT"
RC=0
for a in "$@"; do
    case $a in
        all) set -- prep p1 p2 p3 p4 ;;
    esac
done
for a in "$@"; do
    case $a in
        # bash, not the file's mode: a checkout without the executable bit once ran every phase with no inputs
        prep) flock -s "$LOCK" bash "$RDIR/prep.sh" > "$OUT/prep.log" 2>&1 || { log "prep failed (see $OUT/prep.log); later phases stop"; RC=1; PREP_FAILED=1; } ;;
        p[1-5]) [ "${PREP_FAILED:-0}" = 1 ] && { log "PHASE ${a#p} not run: prep failed"; RC=1; continue; }
                runphase "${a#p}" || RC=1; TABLES=1 ;;
        smoke) phase_smoke || RC=1 ;;
        checktest) phase_checktest || RC=1 ;;
        *) usage; exit 2 ;;
    esac
done
if [ "${TABLES:-0}" = 1 ]; then  # every table with how it was run; a result without its how.json fails the run
    python3 -I "$RDIR/tables.py" "$OUT" > "$OUT/tables.md" 2> "$OUT/tables.err" || { log "tables.py failed: $(head -3 "$OUT/tables.err" | tr '\n' ';')"; RC=1; }
fi
log "run.sh END rc=$RC"
exit $RC
}
main "$@"; exit
