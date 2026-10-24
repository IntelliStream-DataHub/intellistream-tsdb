#!/bin/bash
# Embedded C, Rust and Python and the four tsdbd clients (C, Rust, Python, Java) on one data set: loads and queries,
# side by side.
# See README.md. Usage:
#   bench/clients/run.sh [--work DIR] [--port N] [--series 100] [--points 1000000] [--repeat 3] [--seconds 3]
#                        [--conns "1 4 16"] [--qthreads "1 32"] [--clients "c rust python java"]
#                        [--only load|query] [--no-build] [--force]
set -u -o pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
B=$ROOT/build
WORK=${CBENCH:-/var/tmp/cbench}
PORT=17070
SERIES=100
POINTS=1000000
REPEAT=3
SECONDS_PER_CLASS=3
CONNS="1 4 16"
QTHREADS="1 32"
CLIENTS="c rust python java"
ONLY=""
BUILD=1
FORCE=0
MAXLOAD=2   # load average at the start
MAXBUSY=1   # CPUs other processes may keep busy before a measurement (see quiet_wait)
PYVENV=${PYVENV:-$B/pyvenv}

usage() { sed -n '3,8p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
while [ $# -gt 0 ]; do
    case $1 in
    --work) WORK=$2; shift 2 ;;
    --port) PORT=$2; shift 2 ;;
    --series) SERIES=$2; shift 2 ;;
    --points) POINTS=$2; shift 2 ;;
    --repeat) REPEAT=$2; shift 2 ;;
    --seconds) SECONDS_PER_CLASS=$2; shift 2 ;;
    --conns) CONNS=$2; shift 2 ;;
    --qthreads) QTHREADS=$2; shift 2 ;;
    --clients) CLIENTS=$2; shift 2 ;;
    --only) ONLY=$2; shift 2 ;;
    --no-build) BUILD=0; shift ;;
    --force) FORCE=1; shift ;;
    *) usage ;;
    esac
done
# The embedded benchmarks: tsdb-bench (C) and, for the clients in --clients that have one, the same modes through the
# Rust and Python embedded Db (client/rust/examples/embedded_bench.rs, client/python/bench_embedded.py).
EMBEDDED="c"
for c in rust python; do case " $CLIENTS " in *" $c "*) EMBEDDED="$EMBEDDED $c" ;; esac; done

load1() { cut -d' ' -f1 /proc/loadavg; }
if [ "$FORCE" != 1 ] && awk -v l="$(load1)" -v m=$MAXLOAD 'BEGIN { exit !(l > m) }'; then
    echo "run.sh: the load average is $(load1) (limit $MAXLOAD): the machine is not quiet. Numbers taken now are not" >&2
    echo "        comparable; wait, or pass --force to run anyway." >&2
    exit 3
fi

# Busy CPUs (as a number of cores) over one second, from /proc/stat: what is running now, whatever its load average.
busy_cores() {
    local a b
    a=$(awk '/^cpu / { t = 0; for (i = 2; i <= NF; i++) t += $i; print t, $5 + $6 }' /proc/stat)
    sleep 1
    b=$(awk '/^cpu / { t = 0; for (i = 2; i <= NF; i++) t += $i; print t, $5 + $6 }' /proc/stat)
    awk -v a="$a" -v b="$b" -v n="$(nproc)" 'BEGIN { split(a, x, " "); split(b, y, " ");
        dt = y[1] - x[1]; printf "%.2f", (dt > 0) ? (dt - (y[2] - x[2])) / dt * n : 0 }'
}
# Before a measurement: wait (up to a minute) until nothing else keeps the CPUs busy; the 1-minute load average
# cannot be used, it still carries this benchmark's own previous run. Sets BUSY.
BUSY=0
quiet_wait() {
    local i
    for i in $(seq 60); do
        BUSY=$(busy_cores)
        awk -v b="$BUSY" -v m=$MAXBUSY 'BEGIN { exit !(b <= m) }' && return 0
        [ "$FORCE" = 1 ] && return 0
        sleep 1
    done
    echo "run.sh: other processes keep $BUSY CPUs busy (limit $MAXBUSY): the machine is not quiet; stopping. --force runs anyway." >&2
    exit 3
}

mkdir -p "$WORK" || exit 1
DATAFILE=$WORK/data-${SERIES}x${POINTS}.rowbinary
SRV=$WORK/db # tsdbd's data directory, wiped between loads
EMB=$WORK/emb # the embedded database
OUT=$WORK/results-$(date +%Y%m%d-%H%M%S)
mkdir -p "$OUT"
LOG=$OUT/results.log
: >"$LOG"
SPID=""

# ------------------------------------------------------------------ build
RUSTX=$ROOT/client/rust/target/release/examples
JAR=$ROOT/client/java/target/tsdb-client-0.1.0-SNAPSHOT.jar
PY=$PYVENV/bin/python
if [ "$BUILD" = 1 ]; then
    echo "== building"
    make -C "$ROOT" -j"$(nproc)" >"$OUT/build.log" 2>&1 || { tail -n 20 "$OUT/build.log"; exit 1; }
    case " $CLIENTS " in *" rust "*) (cd "$ROOT/client/rust" && cargo build --release --features embedded --examples) >>"$OUT/build.log" 2>&1 || { tail -n 20 "$OUT/build.log"; exit 1; } ;; esac
    case " $CLIENTS " in *" python "*)
        if [ ! -x "$PYVENV/bin/maturin" ]; then python3 -m venv "$PYVENV" && "$PYVENV/bin/pip" -q install maturin numpy >>"$OUT/build.log" 2>&1 || exit 1; fi
        (cd "$ROOT/client/python" && VIRTUAL_ENV=$PYVENV "$PYVENV/bin/maturin" develop --release) >>"$OUT/build.log" 2>&1 || { tail -n 20 "$OUT/build.log"; exit 1; }
        "$PYVENV/bin/python" -c 'import numpy' 2>/dev/null || "$PYVENV/bin/pip" -q install numpy >>"$OUT/build.log" 2>&1 ;;
    esac
    case " $CLIENTS " in *" java "*) (cd "$ROOT/client/java" && mvn -q package -DskipTests) >>"$OUT/build.log" 2>&1 || { tail -n 20 "$OUT/build.log"; exit 1; } ;; esac
fi
ZSTDJNI=$(find "$HOME/.m2/repository/com/github/luben/zstd-jni" -name 'zstd-jni-*.jar' 2>/dev/null | sort -V | tail -n 1)
JAVA_CP=$JAR${ZSTDJNI:+:$ZSTDJNI}
JAVA="java --enable-native-access=ALL-UNNAMED -cp $JAVA_CP"

# ------------------------------------------------------------------ machine facts
{
    echo "date:      $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "cpu:       $(lscpu | sed -n 's/^Model name: *//p' | head -n 1), $(nproc) logical CPUs"
    echo "memory:    $(awk '/MemTotal/ { printf "%.0f GiB", $2 / 1048576 }' /proc/meminfo)"
    echo "kernel:    $(uname -sr)"
    echo "governor:  $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo unknown)"
    echo "data dir:  $WORK ($(df --output=fstype "$WORK" | tail -n 1 | tr -d ' '))"
    echo "git:       $(git -C "$ROOT" rev-parse --short HEAD)$(git -C "$ROOT" diff --quiet HEAD 2>/dev/null || echo ' (modified)')"
    echo "load avg at start: $(cut -d' ' -f1-3 /proc/loadavg)"
    echo "cc:        $(cc --version | head -n 1)"
    case " $CLIENTS " in *" rust "*) echo "rustc:     $(rustc --version)" ;; esac
    case " $CLIENTS " in *" python "*) echo "python:    $($PY --version 2>&1), numpy $($PY -c 'import numpy; print(numpy.__version__)' 2>/dev/null || echo none)" ;; esac
    case " $CLIENTS " in *" java "*) echo "java:      $(java -version 2>&1 | head -n 1), zstd-jni $(basename "${ZSTDJNI:-none}")" ;; esac
    echo "run:       series=$SERIES points=$POINTS repeat=$REPEAT seconds/class=$SECONDS_PER_CLASS conns=($CONNS) qthreads=($QTHREADS) embedded=($EMBEDDED) clients=($CLIENTS)"
    echo "tsdbd:     defaults: fsync on, wal_sync_ms 0 (an insert is acknowledged once durable), zstd 1 storage, $(nproc) workers"
} >"$OUT/facts.txt"
cat "$OUT/facts.txt"

# ------------------------------------------------------------------ server
stop_server() {
    if [ -n "$SPID" ] && kill -0 "$SPID" 2>/dev/null; then
        kill "$SPID" 2>/dev/null
        wait "$SPID" 2>/dev/null
    fi
    SPID=""
}
port_open() { (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; }
start_server() { # fresh data directory unless $1 = keep
    stop_server
    if port_open; then # whatever answers there would get every load and query instead of our tsdbd
        echo "run.sh: something already listens on 127.0.0.1:$PORT; pick another --port or stop it" >&2
        exit 1
    fi
    [ "${1:-}" = keep ] || rm -rf "$SRV"
    "$B/tsdbd" --dir "$SRV" --port "$PORT" >>"$OUT/tsdbd.log" 2>&1 &
    SPID=$!
    for _ in $(seq 100); do
        kill -0 "$SPID" 2>/dev/null || { echo "tsdbd died, see $OUT/tsdbd.log" >&2; exit 1; }
        if port_open; then # and it is ours that listens, not a server that appeared after the check above
            kill -0 "$SPID" 2>/dev/null || { echo "tsdbd died, see $OUT/tsdbd.log" >&2; exit 1; }
            return 0
        fi
        sleep 0.1
    done
    echo "tsdbd did not listen on $PORT" >&2
    exit 1
}
trap 'stop_server' EXIT
server_cpu() { [ -r "/proc/$SPID/stat" ] || { echo "run.sh: tsdbd (pid $SPID) is gone" >&2; exit 1; }; awk '{ s = $0; sub(/.*\) /, "", s); split(s, f, " "); printf "%.2f", (f[12] + f[13]) / 100 }' "/proc/$SPID/stat"; }

# step CMD...: a preparation step (not measured): shows its output, keeps its verify lines, and stops the run if it fails.
step() {
    local out rc=0
    out=$("$@" 2>&1) || rc=$?
    printf '%s\n' "$out" | grep '^verify: ' | sed "s/^/VERIFY ${STEPTAG:-phase=queryset} /" >>"$LOG"
    if [ $rc -ne 0 ]; then
        printf '%s\n' "$out" | sed 's/^/    /' >&2
        echo "FAILED ($rc): $*" >&2
        exit 1
    fi
    printf '%s\n' "$out" | grep -v '^RESULT ' | sed 's/^/    /'
    return 0
}
# ------------------------------------------------------------------ one measurement
# record TAG CMD...: runs CMD, keeps its RESULT lines (with TAG fields and the load average) in the log, shows the rest.
record() {
    local tag=$1 l out rc
    shift
    quiet_wait
    l=$(load1)
    out=$("$@" 2>&1)
    rc=$?
    printf '%s\n' "$out" | grep -v '^RESULT ' | sed "s/^/    /"
    printf '%s\n' "$out" | grep '^verify: ' | sed "s/^/VERIFY $tag /" >>"$LOG"
    if [ $rc -ne 0 ]; then echo "FAILED ($rc): $*" >&2; exit 1; fi
    printf '%s\n' "$out" | grep '^RESULT ' | sed "s/^RESULT /RESULT $tag load1=$l busy=$BUSY /" >>"$LOG"
}
embedded_cmd() { # lang
    case $1 in
    c) echo "$B/tsdb-bench" ;;
    rust) echo "$RUSTX/embedded_bench" ;;
    python) echo "$PY $ROOT/client/python/bench_embedded.py" ;;
    esac
}
client_load_cmd() { # client conns level
    local c=$1 k=$2 l=$3
    case $c in
    c) echo "$B/tsdbc-bench --port $PORT --conns $k --compression $l load $DATAFILE" ;;
    rust) echo "$RUSTX/insert_bench $DATAFILE --addr 127.0.0.1:$PORT --conns $k --compression $l" ;;
    python) echo "$PY $ROOT/client/python/bench_insert.py $DATAFILE --addr 127.0.0.1:$PORT --conns $k --compression $l" ;;
    java) echo "$JAVA ai.intellistream.tsdb.bench.InsertBench $DATAFILE --port $PORT --conns $k --compression $([ "$l" = 0 ] && echo off || echo "$l")" ;;
    esac
}
client_query_cmd() { # client threads
    local c=$1 k=$2
    case $c in
    c) echo "$B/tsdbc-bench --port $PORT --conns $k --series $SERIES --points $POINTS --seconds $SECONDS_PER_CLASS query" ;;
    rust) echo "$RUSTX/query_bench --addr 127.0.0.1:$PORT --conns $k --series $SERIES --points $POINTS --seconds $SECONDS_PER_CLASS" ;;
    python) echo "$PY $ROOT/client/python/bench_query.py --addr 127.0.0.1:$PORT --conns $k --series $SERIES --points $POINTS --seconds $SECONDS_PER_CLASS" ;;
    java) echo "$JAVA ai.intellistream.tsdb.bench.QueryBench --port $PORT --conns $k --series $SERIES --points $POINTS --seconds $SECONDS_PER_CLASS" ;;
    esac
}

# ------------------------------------------------------------------ the data set, once
if [ ! -s "$DATAFILE" ]; then
    echo "== exporting $SERIES series x $POINTS points to $DATAFILE"
    "$B/tsdb-bench" --export "$DATAFILE" --series "$SERIES" --points "$POINTS" || exit 1
fi

# ------------------------------------------------------------------ loads
if [ "$ONLY" != query ]; then
    for rep in $(seq "$REPEAT"); do
        echo "== loads, repetition $rep of $REPEAT"
        for k in $CONNS; do
            for e in $EMBEDDED; do
                for fs in on off; do
                    echo "  embedded $e, $k threads, fsync $fs"
                    # shellcheck disable=SC2046
                    record "phase=load rep=$rep" $(embedded_cmd "$e") --dir "$EMB" --load "$DATAFILE" --threads "$k" $([ $fs = off ] && echo --no-fsync)
                done
            done
        done
        for c in $CLIENTS; do
            for l in 1 0; do
                for k in $CONNS; do
                    echo "  $c, $k connections, compression $l"
                    start_server
                    cpu0=$(server_cpu) || exit 1
                    # shellcheck disable=SC2046
                    record "phase=load rep=$rep" $(client_load_cmd "$c" "$k" "$l")
                    cpu1=$(server_cpu) || exit 1
                    # the next line belongs to the RESULT just written: the server's CPU time for this load
                    awk -v a="$cpu0" -v b="$cpu1" 'BEGIN { printf "SERVERCPU %.2f\n", b - a }' >>"$LOG"
                    STEPTAG="phase=verify rep=$rep client=$c conns=$k compression=$l" step "$B/tsdbc-bench" --port "$PORT" verify "$DATAFILE"
                done
            done
        done
        stop_server
    done
fi

# ------------------------------------------------------------------ queries
if [ "$ONLY" != load ]; then
    echo "== the data set for the queries: loaded once per system, merged"
    step "$B/tsdb-bench" --dir "$EMB" --load "$DATAFILE" --threads 16 --optimize
    start_server
    step "$B/tsdbc-bench" --port "$PORT" --conns 4 load "$DATAFILE"
    step "$B/tsdbc-bench" --port "$PORT" optimize
    step "$B/tsdbc-bench" --port "$PORT" verify "$DATAFILE"
    for rep in $(seq "$REPEAT"); do
        for q in $QTHREADS; do
            echo "== queries, $q threads/connections, repetition $rep of $REPEAT"
            for e in $EMBEDDED; do
                echo "  embedded $e"
                # shellcheck disable=SC2046
                record "phase=query rep=$rep" $(embedded_cmd "$e") --dir "$EMB" --qps "$q" --series "$SERIES" --points "$POINTS" --seconds "$SECONDS_PER_CLASS"
            done
            for c in $CLIENTS; do
                echo "  $c"
                # shellcheck disable=SC2046
                record "phase=query rep=$rep" $(client_query_cmd "$c" "$q")
            done
        done
    done
    stop_server
fi

echo "== summary"
python3 "$HERE/summarize.py" "$OUT" | tee "$OUT/summary.md"
rc=${PIPESTATUS[0]}
echo "results: $OUT"
[ "$rc" = 0 ] || echo "run.sh: the summary reports problems (verification or differing query work): see above" >&2
exit "$rc"
