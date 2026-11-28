#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# Cluster benchmark harness (docs/design-cluster-data.md §10.4, D30, D31).
#
#   tools/bench-cluster.sh --mode replicas=3|ec=4+2|8+2|8+4|16+4 [--reps 5] [--quick]
#                          [--dir PATH | --scratch PATH] [--out RESULTS_DIR]
#                          [--build DIR [--no-build]] [--portable] [--keep]
#                          [--io auto|uring|epoll] [--help]
#
# Starts a multi-node cluster on one machine (D30: one machine, N processes; D15: --io epoll on nodes >= 2),
# loads the dataset via tsdbc-bench, settles EC partitions (measuring settle throughput, fence length from the
# leader log, and logical digest cost per GiB), measures network bytes/row from peer link counters, runs the
# query suite warm, cold (tools/dropcache via posix_fadvise DONTNEED verified with mincore, D31), 1-down, and
# m-down, measures 1-node rebuild time against foreground query load (p50/p99 before, during, after), and
# measures durable small-batch inserts through the cluster.
#
# Output goes to RESULTS_DIR/raw/ and is summarized into RESULTS_DIR/summary.md and summary.json by
# tools/bench-summary.py. Every node is stopped by pid at the end (and on signal/exit) and scratch is removed.
set -u -o pipefail

MAKE_VARS="CFLAGS CPPFLAGS LDFLAGS LDLIBS ARCH PORTABLE CPU_LEVELS DIST_ARCH DIST_B LV_FLAGS_v3 LV_FLAGS_v4 B
    NO_URING ZSTD_LIB URING_LIB LINK_EXTRA TSDBD MAKEFLAGS MFLAGS"
ENV_SET=()
for v in $MAKE_VARS; do
    [ -n "${!v+x}" ] && ENV_SET+=("$v=${!v}")
done

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT" || exit 2

CMODE=
QUICK=0
REPS=
SCRATCH=${TMPDIR:-/var/tmp}/tsdb-bench-cluster-$(id -un)
OUT=
BUILD_DIR=
NO_BUILD=0
PORTABLE=0
KEEP=0
IO=
JOBS=${MAKE_JOBS:-4}

usage() {
    cat <<'EOF'
Cluster benchmark harness for tsdb (§10.4, D30, D31).

  tools/bench-cluster.sh --mode replicas=3|ec=4+2|8+2|8+4|16+4 [options]

Options
  --mode MODE      required: replicas=3, ec=4+2 (or 4+2), ec=8+2 (8+2), ec=8+4 (8+4), ec=16+4 (16+4)
  --reps N         repetitions (default 5; --quick defaults to 1)
  --quick          smoke-test sizes so a full mode run finishes well under 10 minutes
  --dir PATH       scratch directory for cluster nodes and data (alias: --scratch)
  --out PATH       results directory (default bench-results/cluster-MODE-HOST-DATE)
  --build DIR      build directory to use (default build-bench-cluster, or build-bench-cluster-portable)
  --no-build       reuse an existing optimized --build directory without rebuilding
  --portable       measure the portable release build (make dist)
  --keep           keep scratch directory at the end (default: remove)
  --io BACKEND     event loop for nodes 0..1 (auto|uring|epoll; nodes >= 2 always use epoll per D15)
  -h, --help       show this help
EOF
}

while [ $# -gt 0 ]; do
    case $1 in
    --mode) CMODE=${2:-}; shift ;;
    --reps) REPS=${2:-}; shift ;;
    --quick) QUICK=1 ;;
    --dir|--scratch) SCRATCH=${2:-}; shift ;;
    --out) OUT=${2:-}; shift ;;
    --build) BUILD_DIR=${2:-}; shift ;;
    --no-build) NO_BUILD=1 ;;
    --portable) PORTABLE=1 ;;
    --keep) KEEP=1 ;;
    --io) IO=${2:-}; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "bench-cluster.sh: unknown option $1 (see --help)" >&2; exit 2 ;;
    esac
    shift
done

[ -n "$CMODE" ] || { echo "bench-cluster.sh: --mode is required (see --help)" >&2; exit 2; }
case $CMODE in
replicas=3)
    MODE_NORM="replicas=3"; MODE_TAG="replicas-3"; IS_EC=0; EC_K=0; EC_M=2; NNODES=5; LOG_REPS=3 ;;
ec=4+2|4+2)
    MODE_NORM="ec=4+2"; MODE_TAG="ec-4+2"; IS_EC=1; EC_K=4; EC_M=2; NNODES=7; LOG_REPS=3 ;;
ec=8+2|8+2)
    MODE_NORM="ec=8+2"; MODE_TAG="ec-8+2"; IS_EC=1; EC_K=8; EC_M=2; NNODES=11; LOG_REPS=3 ;;
ec=8+4|8+4)
    MODE_NORM="ec=8+4"; MODE_TAG="ec-8+4"; IS_EC=1; EC_K=8; EC_M=4; NNODES=13; LOG_REPS=5 ;;
ec=16+4|16+4)
    MODE_NORM="ec=16+4"; MODE_TAG="ec-16+4"; IS_EC=1; EC_K=16; EC_M=4; NNODES=21; LOG_REPS=5 ;;
*)
    echo "bench-cluster.sh: unsupported --mode '$CMODE' (expected replicas=3, ec=4+2, 8+2, 8+4, or 16+4)" >&2
    exit 2 ;;
esac

case ${REPS:-1} in *[!0-9]*|0) echo "bench-cluster.sh: --reps wants a positive number" >&2; exit 2 ;; esac
[ -n "$SCRATCH" ] || { echo "bench-cluster.sh: --dir wants a path" >&2; exit 2; }
if [ -z "$REPS" ]; then
    if [ "$QUICK" = 1 ]; then REPS=1; else REPS=5; fi
fi
case $IO in ''|auto|uring|epoll) ;; *) echo "bench-cluster.sh: --io wants auto, uring or epoll" >&2; exit 2 ;; esac

# ------------------------------------------------------------------ sizes

if [ "$QUICK" = 1 ]; then
    RUN_SCALE=quick
    SERIES=10
    POINTS=50000
    LOAD_BATCH=100000
    Q_SECONDS=1
    Q_CONNS_HI=4
    FG_SECONDS=2
    INS_SERIES=50
    INS_SECONDS=1
    INS_PAIRS="1:1 100:4 1000:4"
    NEED_GB=1
else
    RUN_SCALE=standard
    SERIES=100
    POINTS=1000000
    LOAD_BATCH=1000000
    Q_SECONDS=3
    Q_CONNS_HI=32
    FG_SECONDS=5
    INS_SERIES=200
    INS_SECONDS=2
    INS_PAIRS="1:1 1:16 100:4 1000:4 10000:4"
    NEED_GB=8
fi
TOTAL_ROWS=$((SERIES * POINTS))
VOTERS=5

# ------------------------------------------------------------------ helpers

log() { printf '%s\n' "$*" >&2; }
die() { printf 'bench-cluster.sh: %s\n' "$*" >&2; exit 2; }
WARNINGS=()
warn() {
    WARNINGS+=("$*")
    printf 'WARNING: %s\n' "$*" >&2
}
have() { command -v "$1" >/dev/null 2>&1; }

safe_pid() {
    case ${1:-} in '' | *[!0-9]*) return 1 ;; esac
    [ "$1" -gt 1 ]
}

declare -a NODE_PIDS=()
declare -a NODE_CPORTS=()
declare -a NODE_MPORTS=()
declare -a NODE_PPORTS=()
declare -a NODE_IDS=()
FG_PID=
WORK=
CREATED_SCRATCH=0
RES=
RAW=
FAILED=0
VOL_NAME="bench"
CUR_REPDIR=
CUR_SECRET=

stop_fg() {
    local p=${FG_PID:-}
    FG_PID=
    safe_pid "$p" || return 0
    kill -TERM "$p" 2>/dev/null
    local i
    for ((i = 0; i < 50; i++)); do
        kill -0 "$p" 2>/dev/null || break
        sleep 0.1
    done
    kill -0 "$p" 2>/dev/null && kill -KILL "$p" 2>/dev/null
    wait "$p" 2>/dev/null || true
}

stop_node() {
    local idx=$1 sig=${2:-TERM}
    local p=${NODE_PIDS[$idx]:-}
    NODE_PIDS[$idx]=
    safe_pid "$p" || return 0
    kill -0 "$p" 2>/dev/null || { wait "$p" 2>/dev/null || true; return 0; }
    kill -"$sig" "$p" 2>/dev/null
    local i
    for ((i = 0; i < 200; i++)); do
        kill -0 "$p" 2>/dev/null || break
        sleep 0.1
    done
    kill -0 "$p" 2>/dev/null && kill -KILL "$p" 2>/dev/null
    wait "$p" 2>/dev/null || true
    return 0
}

stop_all_nodes() {
    local i p
    # Send SIGTERM to all live nodes first so shutdown runs in parallel.
    for ((i = 0; i < NNODES; i++)); do
        p=${NODE_PIDS[$i]:-}
        if safe_pid "$p" && kill -0 "$p" 2>/dev/null; then
            kill -TERM "$p" 2>/dev/null || true
        fi
    done
    for ((i = 0; i < NNODES; i++)); do
        stop_node "$i" TERM
    done
}

cleanup() {
    stop_fg
    stop_all_nodes
    if [ -n "$WORK" ] && [ -d "$WORK" ]; then
        if [ "$KEEP" = 1 ]; then
            log "scratch data kept in $WORK"
        else
            rm -rf -- "$WORK"
            [ "$CREATED_SCRATCH" = 1 ] && rmdir -- "$SCRATCH" 2>/dev/null || true
        fi
    fi
}
trap cleanup EXIT
trap 'log "interrupted"; exit 130' INT
trap 'log "terminated"; exit 143' TERM

record_cmd() {
    local name=$1 rep=$2
    shift 2
    if [ "$rep" = 1 ]; then
        printf '%s\t%s\n' "$name" "$(printf '%s ' "$@" | sed -e "s| $WORK/| SCRATCH/|g; s|^$WORK/|SCRATCH/|; s|--port [0-9]*|--port PORT|g; s| *\$||")" \
            >>"$RES/commands.txt"
    fi
}

run_step() {
    local name=$1 rep=$2 rc
    shift 2
    record_cmd "$name" "$rep" "$@"
    log "  $name (rep $rep/$REPS) ..."
    "$@" >>"$RAW/$name.$rep.txt" 2>&1
    rc=$?
    printf '%s %s %s\n' "$name" "$rep" "$rc" >>"$RES/status.txt"
    if [ $rc -ne 0 ]; then
        FAILED=1
        log "  $name rep $rep FAILED (rc=$rc, see $RAW/$name.$rep.txt)"
    fi
    return $rc
}

meta() { printf '%s=%s\n' "$1" "$2" >>"$RES/meta.kv"; }

# ------------------------------------------------------------------ prerequisites

CC=${CC:-cc}
missing=()
have "$CC" || missing+=("a C compiler ($CC)")
have make || missing+=("make")
have python3 || missing+=("python3")
if [ "$NO_BUILD" = 0 ] && have "$CC"; then
    printf '#include <zstd.h>\nint main(void){return ZSTD_versionNumber()==0;}\n' |
        "$CC" -x c - -o /dev/null -lzstd 2>/dev/null || missing+=("libzstd development files")
fi
URING=1
if [ "$NO_BUILD" = 0 ] && have "$CC" && ! printf '#include <liburing.h>\nint main(void){struct io_uring r; return io_uring_queue_init(2,&r,0);}\n' |
    "$CC" -x c - -o /dev/null -luring 2>/dev/null; then
    URING=0
fi
if [ ${#missing[@]} -gt 0 ]; then
    log "bench-cluster.sh: missing on this machine:"
    for m in "${missing[@]}"; do log "  - $m"; done
    exit 2
fi
NOURING_MAKE=()
if [ $URING = 0 ]; then
    log "note: liburing not installed; building tsdbd with NO_URING=1 (epoll only)."
    NOURING_MAKE=(NO_URING=1)
fi
CCV=$("$CC" --version 2>/dev/null | head -n 1)

# ------------------------------------------------------------------ scratch & results

mkdir -p "$(dirname "$SCRATCH")" 2>/dev/null
if [ ! -d "$SCRATCH" ]; then
    mkdir -p "$SCRATCH" || die "cannot create $SCRATCH"
    CREATED_SCRATCH=1
fi
WORK=$(mktemp -d "$SCRATCH/run.XXXXXX") || die "cannot create work dir in $SCRATCH"
if [ -z "$OUT" ]; then
    OUT=$ROOT/bench-results/cluster-${MODE_TAG}-$(hostname -s 2>/dev/null || echo host)-$(date +%Y-%m-%d_%H%M)
fi
if [ -e "$OUT" ] && [ -n "$(ls -A "$OUT" 2>/dev/null)" ]; then
    n=2
    while [ -e "$OUT-$n" ]; do n=$((n + 1)); done
    OUT=$OUT-$n
fi
RES=$OUT
RAW=$RES/raw
mkdir -p "$RAW" || die "cannot create $RES"
: >"$RES/commands.txt"
: >"$RES/status.txt"
: >"$RES/meta.kv"

free_kb=$(df -Pk "$WORK" | awk 'NR == 2 { print $4 }')
[ "${free_kb:-0}" -ge $((NEED_GB * 1024 * 1024)) ] ||
    die "$SCRATCH has $((${free_kb:-0} / 1024 / 1024)) GB free; need $NEED_GB GB (use --dir on a larger disk)"
fstype=$(stat -f -c %T "$WORK" 2>/dev/null || echo unknown)
case $fstype in
tmpfs | ramfs)
    warn "scratch directory $SCRATCH is on $fstype (RAM): fsync and cold-read disk costs are not real disk numbers."
    ;;
esac
mem_kb=$(awk '/^MemTotal:/ { print $2 }' /proc/meminfo 2>/dev/null)
ncpu=$(nproc 2>/dev/null || echo 1)

# ------------------------------------------------------------------ machine info & quiet check

quiet_check() {
    local a b busy load1
    read -r load1 _ </proc/loadavg
    a=$(head -n 1 /proc/stat)
    sleep 2
    b=$(head -n 1 /proc/stat)
    busy=$(awk -v a="$a" -v b="$b" -v n="$ncpu" 'BEGIN {
        split(a, x); split(b, y); ta = tb = 0
        for (i = 2; i <= 9; i++) { ta += x[i]; tb += y[i] }
        dt = tb - ta; idle = (y[5] + y[6]) - (x[5] + x[6])
        if (dt > 0) printf "%.2f", (dt - idle) / dt * n; else print "0" }')
    meta load_before "load average $load1 (1 min), $busy of $ncpu CPUs busy over 2 s"
    if awk -v b="$busy" 'BEGIN { exit !(b > 1.5) }'; then
        warn "the machine is not quiet: $busy CPUs busy over the last 2 s (rule: < 1.5). Numbers will be noisy."
    fi
    if awk -v l="$load1" -v n="$ncpu" 'BEGIN { exit !(l > 2.0 && l > n / 8) }'; then
        warn "the 1-minute load average is $load1: something ran recently."
    fi
}

machine_info() {
    local model flags f gov threads cores sockets virt os dev fs
    meta date "$(date '+%Y-%m-%d %H:%M %Z')"
    meta mode "$RUN_SCALE ($MODE_NORM)"
    meta cluster_mode "$MODE_NORM"
    meta cluster_nodes "one machine, $NNODES processes (D30; nodes >= 2 on epoll per D15)"
    if [ "$IS_EC" = 1 ]; then
        meta cluster_topology "$MODE_NORM: $NNODES nodes ($VOTERS voters, $LOG_REPS log replicas, $((EC_K + EC_M)) stripe positions + $((NNODES - EC_K - EC_M)) spare, 2 chunk stores/node)"
    else
        meta cluster_topology "$MODE_NORM: $NNODES nodes ($VOTERS voters, $LOG_REPS log replicas + $((NNODES - LOG_REPS)) spares)"
    fi
    meta reps "$REPS"
    meta host "$(hostname 2>/dev/null)"
    model=$(awk -F: '/^model name/ { sub(/^ +/, "", $2); print $2; exit }' /proc/cpuinfo 2>/dev/null)
    [ -n "$model" ] || model=$(lscpu 2>/dev/null | awk -F: '/Model name/ { sub(/^ +/, "", $2); print $2; exit }')
    meta cpu "${model:-unknown}"
    if have lscpu; then
        threads=$(lscpu | awk -F: '/^Thread\(s\) per core/ { gsub(/ /, "", $2); print $2 }')
        cores=$(lscpu | awk -F: '/^Core\(s\) per socket/ { gsub(/ /, "", $2); print $2 }')
        sockets=$(lscpu | awk -F: '/^Socket\(s\)/ { gsub(/ /, "", $2); print $2 }')
        meta cpu_topology "${sockets:-?} socket(s) x ${cores:-?} cores x ${threads:-?} threads = $ncpu logical CPUs"
    else
        meta cpu_topology "$ncpu logical CPUs"
    fi
    flags=$(awk '/^flags/ { print; exit }' /proc/cpuinfo 2>/dev/null)
    local found=""
    for f in sse4_2 avx avx2 bmi2 avx512f avx512dq avx512bw avx512vl avx512_vbmi avx512_vbmi2 avx512_bitalg avx512_vpopcntdq; do
        case " $flags " in *" $f "*) found="$found $f" ;; esac
    done
    meta cpu_flags "${found# }"
    case " $flags " in *" hypervisor "*) virt="yes, a virtual machine" ;; *) virt="no" ;; esac
    meta virt "$virt"
    meta memory "$((${mem_kb:-0} / 1024 / 1024)) GB ($(awk '/^MemAvailable:/ { printf "%d", $2 / 1048576 }' /proc/meminfo) GB available)"
    meta kernel "$(uname -sr)"
    os=$(. /etc/os-release 2>/dev/null && echo "${PRETTY_NAME:-}")
    meta os "${os:-unknown}"
    gov=$(cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null | sort | uniq -c |
        awk '{ printf "%s%s (%s CPUs)", s, $2, $1; s = ", " }')
    meta governor "${gov:-unknown (no cpufreq)}"
    if [ -n "$gov" ]; then
        case $gov in
        performance*) ;;
        *) case $gov in *performance*) ;; *)
            warn "the CPU governor is \"$gov\", not performance: clock speed may vary during measurement." ;;
        esac ;;
        esac
    fi
    meta scratch "$SCRATCH"
    fs=$(df -PT "$WORK" 2>/dev/null | awk 'NR == 2 { print $2 }')
    dev=$(df -P "$WORK" 2>/dev/null | awk 'NR == 2 { print $1 }')
    meta scratch_fs "${fs:-$fstype}, $(df -Ph "$WORK" | awk 'NR == 2 { print $4 }') free"
    local rot=""
    if [ -b "$dev" ]; then
        local base
        base=$(basename "$(readlink -f "$dev")")
        for p in "/sys/block/$base/queue/rotational" "/sys/class/block/$base/../queue/rotational"; do
            [ -r "$p" ] && rot=$([ "$(cat "$p")" = 1 ] && echo ", rotating disk" || echo ", non-rotating")
        done
    fi
    meta scratch_dev "${dev:-unknown}$rot"
    meta compiler "$CCV"
    meta nofile "$(ulimit -n)"
    if have git && git -C "$ROOT" rev-parse --git-dir >/dev/null 2>&1; then
        local dirty
        dirty=$(git -C "$ROOT" status --porcelain 2>/dev/null | wc -l)
        meta git "$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null)$([ "$dirty" -gt 0 ] && echo " + $dirty modified/untracked files (dirty)")"
        [ "$dirty" -gt 0 ] && warn "the git tree is dirty ($dirty files): results do not name a clean commit."
    else
        meta git "unknown"
    fi
}

# ------------------------------------------------------------------ build

build_bins() {
    local what v mk=() cflags arch default_build=0
    if [ -n "$BUILD_DIR" ]; then
        B=$BUILD_DIR
    elif [ "$PORTABLE" = 1 ]; then
        B=build-bench-cluster-portable
        default_build=1
    else
        B=build-bench-cluster
        default_build=1
    fi
    if [ "$NO_BUILD" = 1 ]; then
        log "reusing existing build directory $B (--no-build)"
    else
        if [ "$default_build" = 0 ]; then
            case $B in
            "" | /* | . | .. | build | build/* | */.. | */../* | ../*)
                die "refusing to build into --build '$B' (use a dedicated relative subdirectory, or pass --no-build)" ;;
            esac
            if [ -e "$ROOT/$B" ] && [ -n "$(ls -A "$ROOT/$B" 2>/dev/null)" ]; then
                die "--build '$B' already exists and is non-empty (pass --no-build to reuse it, or use a new directory)"
            fi
        fi
        for v in $MAKE_VARS; do mk+=(-u "$v"); done
        for v in "${ENV_SET[@]+"${ENV_SET[@]}"}"; do
            warn "environment variable ${v%%=*} is set (${v#*=}); ignored for the measured build."
        done
        MAKE_ENV=(env "${mk[@]}")
        if [ "$default_build" = 1 ]; then
            rm -rf -- "${ROOT:?}/$B"
        fi
        if [ "$PORTABLE" = 1 ]; then
            if ! echo 'int x;' | "${CC:-cc}" -include infiniband/verbs.h -E -x c - >/dev/null 2>&1; then
                NOURING_MAKE+=(NO_RDMA=1)
            fi
            what="make -j$JOBS dist DIST_B=$B${NOURING_MAKE[*]:+ ${NOURING_MAKE[*]}}"
            log "building portable release build: $what ..."
            "${MAKE_ENV[@]}" make -j"$JOBS" dist DIST_B="$B" ${NOURING_MAKE[@]+"${NOURING_MAKE[@]}"} >"$RAW/build.log" 2>&1
            if [ $? -eq 0 ] && [ ! -x "$B/dropcache" ]; then
                "${MAKE_ENV[@]}" make -j"$JOBS" B="$B" "$B/dropcache" >>"$RAW/build.log" 2>&1
            fi
        else
            what="make -j$JOBS B=$B${NOURING_MAKE[*]:+ ${NOURING_MAKE[*]}}"
            log "building optimized native build: $what ..."
            "${MAKE_ENV[@]}" make -j"$JOBS" B="$B" "$B/tsdbd" "$B/tsdb" "$B/tsdb-bench" "$B/tsdbc-bench" "$B/dropcache" \
                ${NOURING_MAKE[@]+"${NOURING_MAKE[@]}"} >"$RAW/build.log" 2>&1
        fi
        if [ $? -ne 0 ]; then
            tail -n 20 "$RAW/build.log" >&2
            die "build failed (full log: $RAW/build.log)"
        fi
        printf 'build\t%s\n' "$what" >>"$RES/commands.txt"
        show() { "${MAKE_ENV[@]}" make -s B="$B" --eval='show-%: ; @echo $($*)' "show-$1" 2>/dev/null | head -n 1; }
        cflags=$(show CFLAGS)
        case " $cflags " in
        *" -O2 "* | *" -O3 "*) ;;
        *) die "compile flags are not optimized (CFLAGS = $cflags)" ;;
        esac
        case $cflags in *-fsanitize* | *-O0* | *--coverage*) die "compile flags are for debugging (CFLAGS = $cflags)" ;; esac
        meta cflags "$cflags"
        if [ "$PORTABLE" = 1 ]; then
            arch="$(show DIST_ARCH) (base); codec and value files again for v3: $(show LV_FLAGS_v3) and v4: $(show LV_FLAGS_v4)"
            meta build "portable release build (make dist): what the tarball ships; the CPU level is chosen at start-up"
        else
            arch=$(show ARCH)
            meta build "optimized native build: -O3 for this CPU, no CPU levels"
        fi
        meta arch "$arch"
    fi
    for t in tsdbd tsdb tsdb-bench tsdbc-bench dropcache; do
        [ -x "$B/$t" ] || die "missing binary $B/$t"
    done
    if [ "$IS_EC" = 1 ]; then
        local has_ec=0
        if [ -x "$B/ec-bench" ]; then
            has_ec=1
        elif have nm && nm "$B/tsdbd" 2>/dev/null | grep ' iso_rs_encode$' >/dev/null; then
            has_ec=1
        elif ! have nm; then
            has_ec=1
        fi
        [ "$has_ec" = 1 ] || die "tsdbd in $B was built without TSDB_EC (NO_EC=1); cannot benchmark EC mode $MODE_NORM"
    fi
    meta build_variant "$([ $PORTABLE = 1 ] && echo portable || echo native), directory $B"
    meta tsdb_version "$("$B/tsdbd" --version 2>&1 | head -n 1)"
}

# ------------------------------------------------------------------ cluster helpers

# Start node index $1 (0 .. NNODES-1) inside $CUR_REPDIR.
start_node() {
    local idx=$1
    local ndir="$CUR_REPDIR/n$idx"
    local pf="$ndir/port"
    local me=$BASHPID
    local args=(
        --dir "$ndir/db"
        --no-auth
        --listen 127.0.0.1
        --port 0
        --port-file "$pf"
        --metrics-port auto
        --metrics-bind 127.0.0.1
        --cluster
        --cluster-secret-file "$CUR_SECRET"
        --state-dir "$ndir/state"
        --peer-port 0
        --peer-listen 127.0.0.1
        --advertise 127.0.0.1
        --exit-with-parent="$me"
        --allow-shared-device
    )
    # D15: at most 2 nodes use io_uring; nodes >= 2 always use epoll so 21-node clusters stay within memlock limits.
    if [ "$idx" -ge 2 ]; then
        args+=(--io epoll)
    elif [ -n "$IO" ]; then
        args+=(--io "$IO")
    fi
    if [ "$IS_EC" = 1 ]; then
        args+=(--disk "$ndir/chunk0:hdd" --disk "$ndir/chunk1:hdd" --ec-settle-grace-s 1 --ec-settle-keep-s 5)
    fi
    mkdir -p "$ndir/db" "$ndir/state"
    rm -f "$pf"
    if [ "$IS_EC" = 1 ]; then
        TSDB_SETTLE_STATS=1 "$B/tsdbd" "${args[@]}" >>"$ndir/log" 2>&1 &
    else
        "$B/tsdbd" "${args[@]}" >>"$ndir/log" 2>&1 &
    fi
    local pid=$!
    NODE_PIDS[$idx]=$pid
    local i cport="" spid="" mport="" pport=""
    for ((i = 0; i < 600; i++)); do
        if ! kill -0 "$pid" 2>/dev/null; then
            log "node $idx tsdbd exited at startup:"
            tail -n 10 "$ndir/log" >&2
            NODE_PIDS[$idx]=
            return 1
        fi
        if [ -s "$pf" ]; then
            read -r cport spid mport pport _ <"$pf"
            if [ -n "$cport" ] && [ -n "$pport" ]; then
                break
            fi
        fi
        sleep 0.05
    done
    if [ -z "$cport" ] || [ -z "$pport" ]; then
        log "node $idx did not write $pf within 30 s"
        stop_node "$idx"
        return 1
    fi
    if [ "$spid" != "$pid" ]; then
        log "node $idx port file names pid $spid != started pid $pid"
        stop_node "$idx"
        return 1
    fi
    NODE_CPORTS[$idx]=$cport
    NODE_MPORTS[$idx]=${mport:-0}
    NODE_PPORTS[$idx]=$pport
    return 0
}

# Pick a live node's peer address (preferring idx 0 if alive).
live_peer_addr() {
    local i
    for ((i = 0; i < NNODES; i++)); do
        if safe_pid "${NODE_PIDS[$i]:-}" && kill -0 "${NODE_PIDS[$i]}" 2>/dev/null; then
            printf '127.0.0.1:%s' "${NODE_PPORTS[$i]}"
            return 0
        fi
    done
    return 1
}

# Run `tsdb cluster status [--json] ...` against a live node.
cluster_status_json() {
    local addr
    addr=$(live_peer_addr) || return 1
    "$B/tsdb" cluster status "$addr" --json --secret-file "$CUR_SECRET" 2>/dev/null
}

vol_status_json() {
    local addr raw
    addr=$(live_peer_addr) || return 1
    raw=$("$B/tsdb" cluster status volume "$VOL_NAME" "$addr" --json --secret-file "$CUR_SECRET" 2>/dev/null) || return 1
    python3 -c 'import json, sys; d = json.load(sys.stdin); print(json.dumps(d.get("volume", d)))' <<<"$raw" 2>/dev/null
}

# Map node member ID -> local array index (0..NNODES-1) via peer port matching.
refresh_node_ids() {
    local st
    st=$(cluster_status_json) || return 1
    local pairs
    pairs=$(python3 -c '
import json, sys
d = json.load(sys.stdin)
for m in d.get("members", []):
    p = m.get("peer", "")
    port = p.rsplit(":", 1)[-1] if ":" in p else ""
    if port:
        print(m["id"], port)
' <<<"$st" 2>/dev/null) || return 1
    local nid pport i
    while read -r nid pport; do
        [ -n "$nid" ] || continue
        for ((i = 0; i < NNODES; i++)); do
            if [ "${NODE_PPORTS[$i]}" = "$pport" ]; then
                NODE_IDS[$i]=$nid
            fi
        done
    done <<<"$pairs"
}

idx_of_node_id() {
    local want=$1 i
    for ((i = 0; i < NNODES; i++)); do
        if [ "${NODE_IDS[$i]:-}" = "$want" ]; then
            printf '%d' "$i"
            return 0
        fi
    done
    return 1
}

# Wait until all NNODES members are UP in cluster status.
wait_all_up() {
    local deadline=$(($(date +%s) + 60)) st ok
    while [ "$(date +%s)" -lt "$deadline" ]; do
        st=$(cluster_status_json || true)
        ok=$(python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
ms = d.get("members", [])
up = sum(1 for m in ms if m.get("state") == "up")
q = d.get("quorum", {}).get("has_quorum", False)
print(1 if (q and up >= int(sys.argv[1])) else 0)
' "$NNODES" <<<"$st" 2>/dev/null || echo 0)
        if [ "$ok" = 1 ]; then
            refresh_node_ids
            return 0
        fi
        sleep 0.2
    done
    log "timed out waiting for all $NNODES cluster members to be UP"
    return 1
}

# Wait until volume $VOL_NAME has a leader, full ISR, and (if EC) all stripe positions WHOLE.
wait_vol_ready() {
    local want_isr=${1:-$LOG_REPS}
    local deadline=$(($(date +%s) + 60)) st ok
    while [ "$(date +%s)" -lt "$deadline" ]; do
        st=$(vol_status_json || true)
        ok=$(python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
want_isr = int(sys.argv[1])
is_ec = int(sys.argv[2])
leader = d.get("leader", 0)
isr = d.get("isr", [])
reps = d.get("replicas", [])
stripe = d.get("stripe", [])
no_learner = all(not r.get("learner", False) for r in reps)
stripe_ok = (not is_ec) or (len(stripe) > 0 and all(s.get("state") == "whole" for s in stripe))
print(1 if (leader > 0 and len(isr) >= want_isr and no_learner and stripe_ok) else 0)
' "$want_isr" "$IS_EC" <<<"$st" 2>/dev/null || echo 0)
        if [ "$ok" = 1 ]; then
            return 0
        fi
        sleep 0.2
    done
    log "timed out waiting for volume $VOL_NAME to be ready"
    return 1
}

# Query current volume leader's node ID, local index, client port, and volume number.
get_vol_leader() {
    local st info
    st=$(vol_status_json) || return 1
    info=$(python3 -c '
import json, sys
d = json.load(sys.stdin)
print(d.get("leader", 0), d.get("number", 0))
' <<<"$st" 2>/dev/null) || return 1
    VOL_LEADER_ID=${info%% *}
    VOL_NUM=${info##* }
    [ "${VOL_LEADER_ID:-0}" -gt 0 ] || return 1
    VOL_LEADER_IDX=$(idx_of_node_id "$VOL_LEADER_ID") || { refresh_node_ids && VOL_LEADER_IDX=$(idx_of_node_id "$VOL_LEADER_ID"); } || return 1
    VOL_LEADER_PORT=${NODE_CPORTS[$VOL_LEADER_IDX]}
    return 0
}

# Scrape peerlink & fwd outgoing byte counters across all live nodes (returns: repl chunk snap fwd total).
# Note: pl_stats.bytes_out counts driver frames only; bulk payloads (replicated COMMIT part files, snapshots on
# repld's peerlink, and EC chunk puts on CHUNKB) are counted in dir="bulk". Snapshot bulk bytes are already
# included in repl dir="bulk", so snap is reported for visibility only and not added again to total.
scrape_net_bytes() {
    local ports="" i
    for ((i = 0; i < NNODES; i++)); do
        if safe_pid "${NODE_PIDS[$i]:-}" && kill -0 "${NODE_PIDS[$i]}" 2>/dev/null && [ "${NODE_MPORTS[$i]:-0}" -gt 0 ]; then
            ports="$ports ${NODE_MPORTS[$i]}"
        fi
    done
    python3 -c '
import sys, urllib.request
repl = chunk = snap = fwd = 0
for p in sys.argv[1:]:
    try:
        text = urllib.request.urlopen(f"http://127.0.0.1:{p}/metrics", timeout=3).read().decode("utf-8", "replace")
    except Exception:
        continue
    for line in text.splitlines():
        if line.startswith("#") or not line:
            continue
        if line.startswith("tsdb_repl_peerlink_bytes_total{dir=\"out\"}") or \
           line.startswith("tsdb_repl_peerlink_bytes_total{dir=\"bulk\"}"):
            repl += int(float(line.rsplit(" ", 1)[-1]))
        elif line.startswith("tsdb_chunk_peerlink_bytes_total{dir=\"out\"}") or \
             line.startswith("tsdb_chunk_peerlink_bytes_total{dir=\"bulk\"}"):
            chunk += int(float(line.rsplit(" ", 1)[-1]))
        elif line.startswith("tsdb_snapshot_bytes_total{dir=\"sent\"}"):
            snap += int(float(line.rsplit(" ", 1)[-1]))
        elif line.startswith("tsdb_fwd_bytes_total{"):
            fwd += int(float(line.rsplit(" ", 1)[-1]))
print(repl, chunk, snap, fwd, repl + chunk + fwd)
' $ports
}

# Scrape total remote chunk read bytes served across all currently live nodes.
scrape_chunk_read_bytes() {
    local ports="" i
    for ((i = 0; i < NNODES; i++)); do
        if safe_pid "${NODE_PIDS[$i]:-}" && kill -0 "${NODE_PIDS[$i]}" 2>/dev/null && [ "${NODE_MPORTS[$i]:-0}" -gt 0 ]; then
            ports="$ports ${NODE_MPORTS[$i]}"
        fi
    done
    python3 -c '
import sys, urllib.request
total = 0
for p in sys.argv[1:]:
    try:
        text = urllib.request.urlopen(f"http://127.0.0.1:{p}/metrics", timeout=3).read().decode("utf-8", "replace")
    except Exception:
        continue
    for line in text.splitlines():
        if line.startswith("tsdb_chunk_srv_bytes_total{op=\"read\"}"):
            total += int(float(line.rsplit(" ", 1)[-1]))
print(total)
' $ports
}

# Wait until eligible partitions in an EC volume have settled and settle stats are in the leader's log.
# Does not demand zero p*.part files (D18 leaves level-0 or empty merge outputs as parts).
wait_ec_settled() {
    local leader_log="$CUR_REPDIR/n$VOL_LEADER_IDX/log"
    local vdir="$CUR_REPDIR/n$VOL_LEADER_IDX/db/v$VOL_NUM"
    local deadline=$(($(date +%s) + 180))
    local seen_ec=0 stable=0 last_stats=0
    while [ "$(date +%s)" -lt "$deadline" ]; do
        local ec_cnt stats_cnt backlog
        ec_cnt=$(find "$vdir" -maxdepth 1 -name 'p*.ec' 2>/dev/null | wc -l)
        stats_cnt=$(grep -c 'settle stats:' "$leader_log" 2>/dev/null || true)
        stats_cnt=${stats_cnt:-0}
        local st
        st=$(vol_status_json || true)
        backlog=$(python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
    print(d.get("settle_backlog", {}).get("keys_waiting", 0))
except Exception:
    print(999)
' <<<"$st" 2>/dev/null || echo 999)
        if [ "$ec_cnt" -gt 0 ] && [ "$stats_cnt" -gt 0 ]; then
            seen_ec=1
        fi
        if [ "$seen_ec" = 1 ] && [ "$backlog" -eq 0 ] && [ "$stats_cnt" = "$last_stats" ]; then
            stable=$((stable + 1))
            if [ "$stable" -ge 3 ]; then
                return 0
            fi
        else
            stable=0
        fi
        last_stats=$stats_cnt
        sleep 0.3
    done
    log "wait_ec_settled: timed out (ec=$ec_cnt stats=$stats_cnt backlog=$backlog, log=$leader_log)"
    return 1
}

# Parse `settle stats:` lines from leader log and emit structured RESULT lines for bench-summary.py.
extract_settle_stats() {
    local rep=$1
    local out_file="$RAW/cluster-settle.$rep.txt"
    local leader_log="$CUR_REPDIR/n$VOL_LEADER_IDX/log"
    python3 - "$leader_log" "$out_file" <<'PY'
import re, sys

log_path, out_path = sys.argv[1], sys.argv[2]
rx = re.compile(
    r"settle stats: key=(-?\d+) parts=(\d+) bytes=(\d+) rows=(\d+) "
    r"total_us=(\d+) fence_us=(\d+) digest_us=(\d+) digest_bytes=(\d+)"
)
parts = bytes_tot = rows_tot = total_us = dig_us = dig_bytes = 0
fences_us = []
count = 0
with open(log_path, errors="replace") as f:
    for line in f:
        m = rx.search(line)
        if not m:
            continue
        count += 1
        parts += int(m.group(2))
        bytes_tot += int(m.group(3))
        rows_tot += int(m.group(4))
        total_us += int(m.group(5))
        fences_us.append(int(m.group(6)))
        dig_us += int(m.group(7))
        dig_bytes += int(m.group(8))

if count == 0:
    sys.stderr.write(f"no settle stats found in {log_path}\n")
    sys.exit(1)

fences_us.sort()
settle_only_us = max(1, total_us)
settle_mib_s = (bytes_tot / 1048576.0) / (settle_only_us / 1e6)
fence_mean_ms = (sum(fences_us) / len(fences_us)) / 1000.0
fence_p50_ms = fences_us[len(fences_us) // 2] / 1000.0
fence_p99_ms = fences_us[int((len(fences_us) - 1) * 0.99)] / 1000.0
fence_max_ms = fences_us[-1] / 1000.0
dig_s = dig_us / 1e6
dig_gib = dig_bytes / 1073741824.0
dig_s_per_gib = (dig_s / dig_gib) if dig_gib > 0 else 0.0
dig_mib_s = (dig_bytes / 1048576.0) / dig_s if dig_s > 0 else 0.0

with open(out_path, "w") as out:
    out.write(
        f"settle: {count} partitions ({parts} parts, {bytes_tot / 1048576.0:.2f} MiB, {rows_tot} rows) "
        f"in {settle_only_us / 1e6:.3f} s = {settle_mib_s:.2f} MiB/s; "
        f"fence mean {fence_mean_ms:.3f} ms, p50 {fence_p50_ms:.3f} ms, p99 {fence_p99_ms:.3f} ms, max {fence_max_ms:.3f} ms\n"
    )
    out.write(
        f"RESULT kind=settle partitions={count} parts={parts} bytes={bytes_tot} rows={rows_tot} "
        f"secs={settle_only_us / 1e6:.4f} mib_s={settle_mib_s:.3f} "
        f"fence_mean_ms={fence_mean_ms:.4f} fence_p50_ms={fence_p50_ms:.4f} "
        f"fence_p99_ms={fence_p99_ms:.4f} fence_max_ms={fence_max_ms:.4f}\n"
    )
    out.write(
        f"digest: {dig_bytes / 1048576.0:.2f} MiB in {dig_s:.4f} s = {dig_s_per_gib:.3f} s/GiB ({dig_mib_s:.1f} MiB/s)\n"
    )
    out.write(
        f"RESULT kind=digest bytes={dig_bytes} secs={dig_s:.6f} s_per_gib={dig_s_per_gib:.4f} mib_s={dig_mib_s:.2f}\n"
    )
PY
    local rc=$?
    printf 'cluster-settle %s %s\n' "$rep" "$rc" >>"$RES/status.txt"
    [ $rc -eq 0 ] || FAILED=1
    return $rc
}

# Pick target node IDs to stop for down1 / downm / rebuild without losing Raft quorum or the volume leader.
# Prints space-separated member IDs.
pick_down_targets() {
    local want=$1
    local st_cl st_vol
    st_cl=$(cluster_status_json) || return 1
    st_vol=$(vol_status_json) || return 1
    python3 - "$want" "$IS_EC" "$st_cl" "$st_vol" <<'PY'
import json, sys

want = int(sys.argv[1])
is_ec = int(sys.argv[2])
cl = json.loads(sys.argv[3])
vol = json.loads(sys.argv[4])

leader = vol.get("leader", 0)
voters = set(cl.get("quorum", {}).get("voters", []))
reps = {r["node"] for r in vol.get("replicas", [])}
stripe = [s["node"] for s in vol.get("stripe", [])]

if is_ec:
    # Prefer stripe members that are: (1) not leader, (2) not log replica, (3) not Raft voter.
    cands = [n for n in stripe if n != leader]
    cands.sort(key=lambda n: (1 if n in reps else 0, 1 if n in voters else 0, n))
else:
    # replicas=3: pick follower replicas (never leader).
    cands = [n for n in reps if n != leader]
    cands.sort(key=lambda n: (1 if n in voters else 0, n))

# Ensure we never drop Raft below majority of voters.
max_voters_down = max(0, (len(voters) - 1) // 2)
picked = []
vdown = 0
for n in cands:
    if len(picked) >= want:
        break
    if n in voters and vdown >= max_voters_down:
        continue
    picked.append(n)
    if n in voters:
        vdown += 1

if len(picked) < want:
    sys.stderr.write(f"pick_down_targets: wanted {want}, found only {len(picked)} (cands={cands}, voters={voters})\n")
    sys.exit(1)

print(" ".join(str(x) for x in picked))
PY
}

# Pick another ISR member (different from current leader and any excluded node IDs passed as args).
pick_other_isr() {
    local exclude="$*"
    local st_vol
    st_vol=$(vol_status_json) || return 1
    python3 -c '
import json, sys
excl = {int(x) for x in sys.argv[1].split() if x}
d = json.load(sys.stdin)
leader = d.get("leader", 0)
for n in d.get("isr", []):
    if n != leader and n not in excl:
        print(n)
        sys.exit(0)
sys.exit(1)
' "$exclude" <<<"$st_vol" 2>/dev/null
}

# Wait until member $1 is UP and advertises peer port $2 in cluster metadata (after a single-node restart).
wait_member_peer_port() {
    local nid=$1 want_port=$2 deadline=$(($(date +%s) + 30)) st ok
    while [ "$(date +%s)" -lt "$deadline" ]; do
        st=$(cluster_status_json || true)
        ok=$(python3 -c '
import json, sys
nid, want = int(sys.argv[1]), sys.argv[2]
try:
    d = json.load(sys.stdin)
except Exception:
    print(0); sys.exit(0)
for m in d.get("members", []):
    if m.get("id") == nid and m.get("state") == "up":
        p = m.get("peer", "")
        if p.endswith(":" + want):
            print(1); sys.exit(0)
print(0)
' "$nid" "$want_port" <<<"$st" 2>/dev/null || echo 0)
        [ "$ok" = 1 ] && return 0
        sleep 0.2
    done
    return 1
}

transfer_leader_to() {
    local target=$1
    local addr out rc attempt deadline
    for ((attempt = 1; attempt <= 5; attempt++)); do
        if get_vol_leader && [ "$VOL_LEADER_ID" = "$target" ]; then
            return 0
        fi
        addr=$(live_peer_addr) || return 1
        out=$("$B/tsdb" cluster volume transfer "$addr" "$VOL_NUM" "$target" --secret-file "$CUR_SECRET" 2>&1)
        rc=$?
        if [ $rc -eq 0 ]; then
            deadline=$(($(date +%s) + 20))
            while [ "$(date +%s)" -lt "$deadline" ]; do
                local st_v ready
                st_v=$(vol_status_json || true)
                ready=$(python3 -c '
import json, sys
tgt = int(sys.argv[1])
try:
    d = json.load(sys.stdin)
except Exception:
    print(0); sys.exit(0)
if d.get("leader") != tgt:
    print(0); sys.exit(0)
ep = d.get("epoch", 0)
for r in d.get("reports", []):
    if r.get("node") == tgt and r.get("role") == "leader" and r.get("epoch") == ep:
        print(1); sys.exit(0)
print(0)
' "$target" <<<"$st_v" 2>/dev/null || echo 0)
                if [ "$ready" = 1 ] && get_vol_leader && [ "$VOL_LEADER_ID" = "$target" ]; then
                    return 0
                fi
                sleep 0.1
            done
        fi
        if [ "$attempt" -lt 5 ]; then
            sleep 1
        else
            log "  volume transfer $VOL_NUM -> node$target failed after $attempt attempts (rc=$rc): $out"
            return 1
        fi
    done
    return 1
}

# Restart one ISR follower (outside optional excluded node IDs), drop its OS page cache (plus all EC chunk stores),
# wait for cluster health, and transfer volume leadership to it. Guarantees both cold page cache (unmapped before
# posix_fadvise DONTNEED) and an empty in-process eccache while keeping Raft quorum and peer ports intact.
prepare_cold_leader() {
    local record_dc=$1 rep=$2
    shift 2
    local target tidx
    target=$(pick_other_isr "$@") || { log "could not pick another ISR member for cold leader"; return 1; }
    tidx=$(idx_of_node_id "$target") || return 1
    stop_node "$tidx" TERM
    local dc_paths=("$CUR_REPDIR/n$tidx/db/v$VOL_NUM")
    if [ "$IS_EC" = 1 ]; then
        dc_paths+=("$CUR_REPDIR"/n*/chunk0 "$CUR_REPDIR"/n*/chunk1)
    fi
    if [ "$record_dc" = 1 ]; then
        local dc_args=()
        case $fstype in
        tmpfs | ramfs) ;;
        *) dc_args=(--max-resident-pct 1.0) ;;
        esac
        run_step cluster-dropcache "$rep" "$B/dropcache" ${dc_args[@]+"${dc_args[@]}"} "${dc_paths[@]}"
    else
        "$B/dropcache" --quiet "${dc_paths[@]}" 2>/dev/null || true
    fi
    start_node "$tidx" || return 1
    wait_member_peer_port "$target" "${NODE_PPORTS[$tidx]}" || { log "node$target did not advertise new peer port"; return 1; }
    wait_all_up || return 1
    wait_vol_ready "$LOG_REPS" || return 1
    transfer_leader_to "$target" || { log "transfer_leader_to node$target failed"; return 1; }
    get_vol_leader || return 1
}

# Wait until all listed member IDs are marked DOWN in cluster status.
wait_members_down() {
    local targets="$*"
    local deadline=$(($(date +%s) + 30)) st ok
    while [ "$(date +%s)" -lt "$deadline" ]; do
        st=$(cluster_status_json || true)
        ok=$(python3 -c '
import json, sys
want = {int(x) for x in sys.argv[1].split() if x}
try:
    d = json.load(sys.stdin)
except Exception:
    print(0); sys.exit(0)
by = {m["id"]: m.get("state") for m in d.get("members", [])}
print(1 if all(by.get(n) in ("down", "out", "destroyed") for n in want) else 0)
' "$targets" <<<"$st" 2>/dev/null || echo 0)
        [ "$ok" = 1 ] && return 0
        sleep 0.2
    done
    return 1
}

# Measure steady-state live on-disk bytes across all nodes' volume + chunk directories
# (excluding retired_* merge inputs, settled_* kept copies, WAL segments, digest jobs, and *.tmp files).
measure_disk_bytes() {
    python3 - "$CUR_REPDIR" "$NNODES" "$VOL_NUM" "$IS_EC" <<'PY'
import os, sys

repdir, nn, vol, is_ec = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
skip_prefixes = ("retired_", "settled_", "wal_", "digest_", "tmp_")
total = 0

for i in range(nn):
    vdir = os.path.join(repdir, f"n{i}", "db", f"v{vol}")
    if os.path.isdir(vdir):
        for name in os.listdir(vdir):
            if name.startswith(skip_prefixes) or name.endswith(".tmp"):
                continue
            p = os.path.join(vdir, name)
            try:
                st = os.lstat(p)
                if os.path.isfile(p):
                    total += st.st_size
            except OSError:
                pass
    if is_ec:
        for c in (0, 1):
            cdir = os.path.join(repdir, f"n{i}", f"chunk{c}", "chunks")
            if not os.path.isdir(cdir):
                continue
            for root, _dirs, files in os.walk(cdir):
                for name in files:
                    if name.endswith(".tmp"):
                        continue
                    try:
                        total += os.lstat(os.path.join(root, name)).st_size
                    except OSError:
                        pass

print(total)
PY
}

# Run warm/cold/down1/downm query pair (1 conn and Q_CONNS_HI conns).
run_query_suite() {
    local tag=$1 rep=$2
    run_step "cluster-query-${tag}-c1" "$rep" \
        "$B/tsdbc-bench" --port "$VOL_LEADER_PORT" --conns 1 --series "$SERIES" --points "$POINTS" \
        --seconds "$Q_SECONDS" --lat query
    run_step "cluster-query-${tag}-c${Q_CONNS_HI}" "$rep" \
        "$B/tsdbc-bench" --port "$VOL_LEADER_PORT" --conns "$Q_CONNS_HI" --series "$SERIES" --points "$POINTS" \
        --seconds "$Q_SECONDS" --lat query
}

# ------------------------------------------------------------------ one repetition

run_rep() {
    local rep=$1
    CUR_REPDIR="$WORK/rep.$rep"
    CUR_SECRET="$CUR_REPDIR/secret"
    mkdir -p "$CUR_REPDIR"
    NODE_PIDS=()
    NODE_CPORTS=()
    NODE_MPORTS=()
    NODE_PPORTS=()
    NODE_IDS=()

    log "--- rep $rep of $REPS ($MODE_NORM, $NNODES nodes) ---"

    "$B/tsdb" cluster secret "$CUR_SECRET" >/dev/null 2>&1 || die "tsdb cluster secret failed"

    # Prepare disk metadata on every node (R13 E58: --dir and --disk directories need DISK metadata).
    local i
    for ((i = 0; i < NNODES; i++)); do
        mkdir -p "$CUR_REPDIR/n$i/db" "$CUR_REPDIR/n$i/state"
        "$B/tsdb" disk init "$CUR_REPDIR/n$i/db:nvme" >/dev/null 2>&1 || die "tsdb disk init n$i/db failed"
        if [ "$IS_EC" = 1 ]; then
            mkdir -p "$CUR_REPDIR/n$i/chunk0" "$CUR_REPDIR/n$i/chunk1"
            "$B/tsdb" disk init "$CUR_REPDIR/n$i/chunk0:hdd" >/dev/null 2>&1 || die "tsdb disk init n$i/chunk0 failed"
            "$B/tsdb" disk init "$CUR_REPDIR/n$i/chunk1:hdd" >/dev/null 2>&1 || die "tsdb disk init n$i/chunk1 failed"
        fi
    done

    # Start all N nodes.
    for ((i = 0; i < NNODES; i++)); do
        start_node "$i" || return 1
    done
    if [ "$rep" = 1 ]; then
        meta simd "$(sed -n 's/.*simd \([a-z0-9_]*\).*/\1/p' "$CUR_REPDIR/n0/log" | head -n 1)"
        meta io "nodes 0..1 $(sed -n 's/.*io \([a-z_]*\)$/\1/p' "$CUR_REPDIR/n0/log" | head -n 1), nodes 2..$((NNODES - 1)) epoll (D15)"
    fi

    # Form the cluster (--balance off so background balancing never disturbs measurements;
    # --rebuild-after-s 300 so stopped nodes stay DOWN during 1-down / m-down queries until explicit `node out`).
    "$B/tsdb" cluster init "127.0.0.1:${NODE_PPORTS[0]}" --secret-file "$CUR_SECRET" \
        --voters "$VOTERS" --balance off --rebuild-after-s 300 --failstop-out-s 300 \
        --node-timeout-ms 3000 --heartbeat-ms 500 >/dev/null 2>&1 || { log "cluster init failed"; return 1; }

    for ((i = 1; i < NNODES; i++)); do
        "$B/tsdb" cluster node add "127.0.0.1:${NODE_PPORTS[0]}" "127.0.0.1:${NODE_PPORTS[$i]}" \
            --secret-file "$CUR_SECRET" >/dev/null 2>&1 || { log "node add $i failed"; return 1; }
    done
    wait_all_up || return 1

    # Create volume `bench` (retry briefly on EC while chunk stores finish reporting).
    local added=0 t0_add=$(date +%s)
    while [ $(($(date +%s) - t0_add)) -lt 60 ]; do
        if "$B/tsdb" cluster volume add "127.0.0.1:${NODE_PPORTS[0]}" "$VOL_NAME" \
            --redundancy "$MODE_NORM" --secret-file "$CUR_SECRET" >/dev/null 2>&1; then
            added=1
            break
        fi
        sleep 0.3
    done
    [ "$added" = 1 ] || { log "volume add $VOL_NAME ($MODE_NORM) failed"; return 1; }
    wait_vol_ready "$LOG_REPS" || return 1
    get_vol_leader || { log "failed to locate volume leader"; return 1; }
    log "  cluster ready: leader=node$VOL_LEADER_ID (idx=$VOL_LEADER_IDX, port=$VOL_LEADER_PORT, vol=$VOL_NUM)"

    # Phase 1: Load + Optimize + Settle + Network bytes/row + Digest cost
    local net0 net1
    read -r -a net0 <<<"$(scrape_net_bytes)"

    run_step cluster-load "$rep" \
        "$B/tsdbc-bench" --port "$VOL_LEADER_PORT" --volume "$VOL_NAME" --conns 4 --batch "$LOAD_BATCH" load "$DATA_FILE" || return 1
    run_step cluster-optimize "$rep" \
        "$B/tsdbc-bench" --port "$VOL_LEADER_PORT" optimize || return 1

    if [ "$IS_EC" = 1 ]; then
        log "  waiting for EC settles to complete ..."
        wait_ec_settled || return 1
        extract_settle_stats "$rep" || return 1
    fi
    sleep 1

    read -r -a net1 <<<"$(scrape_net_bytes)"
    local d_repl=$((net1[0] - net0[0]))
    local d_chunk=$((net1[1] - net0[1]))
    local d_snap=$((net1[2] - net0[2]))
    local d_fwd=$((net1[3] - net0[3]))
    local d_tot=$((net1[4] - net0[4]))
    [ "$d_repl" -lt 0 ] && d_repl=0
    [ "$d_chunk" -lt 0 ] && d_chunk=0
    [ "$d_snap" -lt 0 ] && d_snap=0
    [ "$d_fwd" -lt 0 ] && d_fwd=0
    [ "$d_tot" -lt 0 ] && d_tot=0
    local disk_bytes
    disk_bytes=$(measure_disk_bytes)

    python3 - "$TOTAL_ROWS" "$d_repl" "$d_chunk" "$d_snap" "$d_fwd" "$d_tot" "$disk_bytes" "$RAW/cluster-net.$rep.txt" <<'PY'
import sys
rows = int(sys.argv[1])
repl, chunk, snap, fwd, tot, disk = [int(x) for x in sys.argv[2:8]]
out_path = sys.argv[8]
bpr = tot / rows if rows else 0.0
rbpr = repl / rows if rows else 0.0
cbpr = chunk / rows if rows else 0.0
dbpr = disk / rows if rows else 0.0
with open(out_path, "w") as f:
    f.write(
        f"network: {tot} bytes total ({bpr:.3f} bytes/row; repl {rbpr:.3f}, chunk {cbpr:.3f}, "
        f"snap {snap}, fwd {fwd}); disk {disk / 1048576.0:.2f} MiB ({dbpr:.3f} bytes/row)\n"
    )
    f.write(
        f"RESULT kind=network rows={rows} total_bytes={tot} repl_bytes={repl} chunk_bytes={chunk} "
        f"snap_bytes={snap} fwd_bytes={fwd} bytes_per_row={bpr:.4f} repl_bytes_per_row={rbpr:.4f} "
        f"chunk_bytes_per_row={cbpr:.4f} disk_bytes={disk} disk_bytes_per_row={dbpr:.4f}\n"
    )
PY
    printf 'cluster-net %s 0\n' "$rep" >>"$RES/status.txt"

    # Phase 2: Query suite — warm
    "$B/tsdbc-bench" --port "$VOL_LEADER_PORT" --conns 1 --series "$SERIES" --points "$POINTS" \
        --seconds 0.3 --class raw query >/dev/null 2>&1 || true
    run_query_suite warm "$rep"

    # Phase 3: Query suite — cold (D31).
    # tsdbd mmaps .part/.ec files (MAP_SHARED), and Linux posix_fadvise(DONTNEED) cannot evict pages actively mapped
    # by a live process. prepare_cold_leader stops one ISR follower, drops page cache on its db dir (plus all chunk
    # store dirs for EC, which use pread rather than mmap), restarts that follower (guaranteeing an empty eccache),
    # waits for ISR, and transfers leadership to it.
    log "  dropping OS page cache (tools/dropcache) on serving node for cold queries ..."
    prepare_cold_leader 1 "$rep" || return 1
    local cr0=0 cr1=0 cr_delta=0
    [ "$IS_EC" = 1 ] && cr0=$(scrape_chunk_read_bytes)
    run_query_suite cold "$rep"
    if [ "$IS_EC" = 1 ]; then
        cr1=$(scrape_chunk_read_bytes)
        cr_delta=$((cr1 - cr0))
        if [ "$cr_delta" -le 0 ]; then
            log "  ERROR: EC cold query suite served 0 remote chunk bytes (eccache was not cold or no .ec parts read)"
            FAILED=1
        else
            log "  EC cold remote chunk reads: $cr_delta bytes"
        fi
    fi

    # Phase 4: Query suite — 1 stripe member down (or 1 follower replica down for replicas=3).
    # On EC modes, bounce a fresh ISR member (outside the planned down targets) BEFORE killing target1 so EC
    # promotion does not wait on a dead stripe member's epoch fence AND the serving node starts with an empty eccache.
    # On replicas=3 there is no eccache or remote chunk decode; the leader serves directly from its local .part files.
    local targets_m target1 tid tidx
    local -a t_arr=()
    targets_m=$(pick_down_targets "$EC_M") || { log "could not pick $EC_M down targets"; return 1; }
    read -r -a t_arr <<<"$targets_m"
    if [ "$IS_EC" = 1 ]; then
        prepare_cold_leader 0 "$rep" "${t_arr[@]}" || return 1
    fi
    target1=${t_arr[0]}
    tidx=$(idx_of_node_id "$target1") || return 1
    log "  stopping 1 node (node$target1, idx=$tidx) for 1-down queries ..."
    stop_node "$tidx" KILL
    wait_members_down "$target1" || log "  warning: node$target1 not yet marked down"
    if [ "$IS_EC" = 1 ]; then
        "$B/dropcache" --quiet "$CUR_REPDIR"/n*/chunk0 "$CUR_REPDIR"/n*/chunk1 2>/dev/null || true
        cr0=$(scrape_chunk_read_bytes)
    fi
    run_query_suite down1 "$rep"
    if [ "$IS_EC" = 1 ]; then
        cr1=$(scrape_chunk_read_bytes)
        cr_delta=$((cr1 - cr0))
        if [ "$cr_delta" -le 0 ]; then
            log "  ERROR: EC down1 query suite served 0 remote chunk bytes (eccache was not cold or no .ec parts read)"
            FAILED=1
        else
            log "  EC down1 remote chunk reads: $cr_delta bytes"
        fi
    fi

    # Phase 5: Query suite — m stripe members down (or m=2 follower replicas down for replicas=3).
    # On EC modes, briefly restart target1 so prepare_cold_leader can bounce a fresh ISR member (empty eccache)
    # while all stripe members are UP, then kill all m targets together.
    # On replicas=3, m=2 leaves only the single leader replica alive so it serves directly from its local .part files.
    if [ "$IS_EC" = 1 ]; then
        start_node "$tidx" || return 1
        wait_member_peer_port "$target1" "${NODE_PPORTS[$tidx]}" || return 1
        wait_all_up || return 1
        prepare_cold_leader 0 "$rep" "${t_arr[@]}" || return 1
        log "  stopping $EC_M stripe target node(s) ($targets_m) for m-down (m=$EC_M) queries ..."
        for ((i = 0; i < EC_M; i++)); do
            tid=${t_arr[$i]}
            tidx=$(idx_of_node_id "$tid") || return 1
            stop_node "$tidx" KILL
        done
        wait_members_down "${t_arr[@]}" || log "  warning: not all m targets marked down yet"
        "$B/dropcache" --quiet "$CUR_REPDIR"/n*/chunk0 "$CUR_REPDIR"/n*/chunk1 2>/dev/null || true
        cr0=$(scrape_chunk_read_bytes)
    else
        log "  stopping remaining $((EC_M - 1)) target node(s) ($targets_m) for m-down (m=$EC_M) queries ..."
        for ((i = 1; i < EC_M; i++)); do
            tid=${t_arr[$i]}
            tidx=$(idx_of_node_id "$tid") || return 1
            stop_node "$tidx" KILL
        done
        wait_members_down "${t_arr[@]}" || log "  warning: not all m targets marked down yet"
    fi
    get_vol_leader || return 1
    run_query_suite downm "$rep"
    if [ "$IS_EC" = 1 ]; then
        cr1=$(scrape_chunk_read_bytes)
        cr_delta=$((cr1 - cr0))
        if [ "$cr_delta" -le 0 ]; then
            log "  ERROR: EC downm query suite served 0 remote chunk bytes (eccache was not cold or no .ec parts read)"
            FAILED=1
        else
            log "  EC downm remote chunk reads: $cr_delta bytes"
        fi
    fi

    # Restart the m stopped nodes so the cluster returns to full health before rebuild benchmark.
    log "  restarting stopped nodes ($targets_m) before rebuild measurement ..."
    for tid in "${t_arr[@]}"; do
        tidx=$(idx_of_node_id "$tid") || return 1
        start_node "$tidx" || return 1
    done
    wait_all_up || return 1
    wait_vol_ready "$LOG_REPS" || return 1
    get_vol_leader || return 1

    # Phase 6: 1-node rebuild against foreground query load (p50/p99 before, during, after)
    local victim vidx fg_stop="$CUR_REPDIR/fg.stop"
    victim=$(pick_down_targets 1) || return 1
    vidx=$(idx_of_node_id "$victim") || return 1
    log "  rebuild benchmark: before foreground load (victim will be node$victim, idx=$vidx) ..."
    run_step cluster-rebuild-before "$rep" \
        "$B/tsdbc-bench" --port "$VOL_LEADER_PORT" --conns 4 --series "$SERIES" --points "$POINTS" \
        --seconds "$FG_SECONDS" --class raw --lat query

    rm -f "$fg_stop"
    record_cmd cluster-rebuild-during "$rep" \
        "$B/tsdbc-bench" --port "$VOL_LEADER_PORT" --conns 4 --series "$SERIES" --points "$POINTS" \
        --seconds 600 --class raw --lat --retry-unavail --stop-file "$fg_stop" query
    "$B/tsdbc-bench" --port "$VOL_LEADER_PORT" --conns 4 --series "$SERIES" --points "$POINTS" \
        --seconds 600 --class raw --lat --retry-unavail --stop-file "$fg_stop" query \
        >>"$RAW/cluster-rebuild-during.$rep.txt" 2>&1 &
    FG_PID=$!
    sleep 0.5

    local t_reb0 t_reb1 reb_s peer_addr
    t_reb0=$(date +%s%3N)
    stop_node "$vidx" KILL
    peer_addr=$(live_peer_addr) || { stop_fg; return 1; }
    "$B/tsdb" cluster node out "$peer_addr" "$victim" --secret-file "$CUR_SECRET" >/dev/null 2>&1 || {
        log "cluster node out $victim failed"
        stop_fg
        return 1
    }

    log "  waiting for 1-node rebuild off node$victim to finish ..."
    local deadline=$(($(date +%s) + 300)) reb_ok=0 st_v
    while [ "$(date +%s)" -lt "$deadline" ]; do
        st_v=$(vol_status_json || true)
        reb_ok=$(python3 -c '
import json, sys
victim = int(sys.argv[1])
is_ec = int(sys.argv[2])
want_isr = int(sys.argv[3])
try:
    d = json.load(sys.stdin)
except Exception:
    print(0); sys.exit(0)
reps = d.get("replicas", [])
isr = d.get("isr", [])
stripe = d.get("stripe", [])
reb = d.get("rebuild", {})
no_victim_rep = all(r.get("node") != victim for r in reps)
no_learner = all(not r.get("learner", False) for r in reps)
isr_ok = len(isr) >= want_isr and (victim not in isr)
if is_ec:
    stripe_ok = len(stripe) > 0 and all(s.get("state") == "whole" and s.get("node") != victim for s in stripe)
else:
    stripe_ok = True
done = no_victim_rep and no_learner and isr_ok and stripe_ok and not reb.get("active", False)
print(1 if done else 0)
' "$victim" "$IS_EC" "$LOG_REPS" <<<"$st_v" 2>/dev/null || echo 0)
        if [ "$reb_ok" = 1 ]; then
            break
        fi
        sleep 0.2
    done
    t_reb1=$(date +%s%3N)
    : >"$fg_stop"
    local fg_rc=0
    wait "$FG_PID" || fg_rc=$?
    FG_PID=
    printf 'cluster-rebuild-during %s %s\n' "$rep" "$fg_rc" >>"$RES/status.txt"
    [ "$fg_rc" -eq 0 ] || FAILED=1

    reb_s=$(awk -v a="$t_reb0" -v b="$t_reb1" 'BEGIN { printf "%.3f", (b - a) / 1000.0 }')
    if [ "$reb_ok" = 1 ]; then
        printf 'rebuild: node%s evacuated and rebuilt in %s s\nRESULT kind=rebuild victim=%s secs=%s\n' \
            "$victim" "$reb_s" "$victim" "$reb_s" >"$RAW/cluster-rebuild.$rep.txt"
        printf 'cluster-rebuild %s 0\n' "$rep" >>"$RES/status.txt"
        log "  rebuild finished in ${reb_s} s"
    else
        printf 'rebuild: node%s TIMED OUT after %s s\n' "$victim" "$reb_s" >"$RAW/cluster-rebuild.$rep.txt"
        printf 'cluster-rebuild %s 1\n' "$rep" >>"$RES/status.txt"
        FAILED=1
        log "  rebuild TIMED OUT after ${reb_s} s"
    fi

    get_vol_leader || return 1
    run_step cluster-rebuild-after "$rep" \
        "$B/tsdbc-bench" --port "$VOL_LEADER_PORT" --conns 4 --series "$SERIES" --points "$POINTS" \
        --seconds "$FG_SECONDS" --class raw --lat query

    # Phase 7: Durable small-batch cluster inserts (tsdbc-bench insert)
    local pair b c
    for pair in $INS_PAIRS; do
        b=${pair%%:*}
        c=${pair##*:}
        run_step cluster-insert "$rep" \
            "$B/tsdbc-bench" --port "$VOL_LEADER_PORT" --volume "$VOL_NAME" --series "$INS_SERIES" \
            --batch "$b" --conns "$c" --seconds "$INS_SECONDS" insert
    done

    # Save rep 1 leader log for inspection, stop all nodes, and remove rep scratch immediately.
    if [ "$rep" = 1 ]; then
        cp "$CUR_REPDIR/n$VOL_LEADER_IDX/log" "$RAW/cluster-leader.1.txt" 2>/dev/null || true
    fi
    stop_all_nodes
    if [ "$KEEP" != 1 ]; then
        rm -rf -- "$CUR_REPDIR"
    fi
    return 0
}

# ------------------------------------------------------------------ main

log "tsdb cluster benchmark: mode $MODE_NORM ($NNODES nodes), $RUN_SCALE, $REPS rep(s), results in $RES"
log "scratch: $SCRATCH (removed at end$([ $KEEP = 1 ] && echo ', except --keep'))"

machine_info
quiet_check
build_bins

meta size_cluster_load "$SERIES series x $POINTS points = $TOTAL_ROWS rows loaded over 4 connections into volume '$VOL_NAME' ($MODE_NORM, one machine, $NNODES processes), followed by flush & optimize."
meta size_cluster_queries "Random window queries (raw, agg300, agg3600, twa3600, full0) for $Q_SECONDS s each on 1 and $Q_CONNS_HI connections: warm, cold (tools/dropcache verified with mincore + fresh node cache), 1 position down, and m ($EC_M) positions down."
meta size_cluster_rebuild "1-node failure + 'tsdb cluster node out' rebuild onto spare while 4 foreground connections query raw windows continuously; p50/p99 measured before ($FG_SECONDS s), during, and after ($FG_SECONDS s)."
meta size_cluster_insert "Synchronous durable inserts through the cluster volume '$VOL_NAME' ($INS_SERIES series, $INS_SECONDS s per batch:conns pair: $INS_PAIRS)."

DATA_FILE="$WORK/data.rowbinary"
run_step cluster-export 1 "$B/tsdb-bench" --export "$DATA_FILE" --series "$SERIES" --points "$POINTS" || die "data export failed"

START=$(date +%s)
for ((r = 1; r <= REPS; r++)); do
    if ! run_rep "$r"; then
        FAILED=1
        stop_fg
        stop_all_nodes
        [ "$KEEP" != 1 ] && rm -rf -- "$WORK/rep.$r"
    fi
done
rm -f -- "$DATA_FILE"

secs=$(($(date +%s) - START))
meta duration "$((secs / 60)) min $((secs % 60)) s (the benchmark runs, not the build)"

printf '%s\n' "${WARNINGS[@]+"${WARNINGS[@]}"}" | sed '/^$/d' >"$RES/warnings.txt"
log "generating summary ..."
python3 -I "$ROOT/tools/bench-summary.py" "$RES" || FAILED=1
log ""
log "results: $RES/summary.md (and summary.json); raw output in $RES/raw/"
if [ -f "$RES/summary.md" ]; then
    awk '/^## At a glance/ { p = 1; next } /^## / && p { exit } p' "$RES/summary.md" >&2
fi
if [ "$FAILED" = 1 ]; then
    log "some steps failed: see Problems in $RES/summary.md"
    exit 1
fi
exit 0
