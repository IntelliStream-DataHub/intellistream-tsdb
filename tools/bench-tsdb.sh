#!/bin/bash
# One command that benchmarks tsdb itself on this machine and writes a summary.
#
#   tools/bench-tsdb.sh [--quick|--standard|--full] [--reps N] [--dir SCRATCH] [--out RESULTS_DIR]
#                       [--portable] [--cpu-compare] [--keep] [--io auto|uring|epoll] [--help]
#
# Run it from a fresh clone on a machine that does nothing else. See --help for what each mode runs. The raw output
# of every run goes to RESULTS_DIR/raw, the tables (mean, standard deviation, lowest and highest run) to
# RESULTS_DIR/summary.md and RESULTS_DIR/summary.json (tools/bench-summary.py writes them). Besides the build
# directories of the clone it writes only to SCRATCH and RESULTS_DIR, and stops only the tsdbd it started.
set -u -o pipefail

# Variables that change what make builds. Taken from the environment they would silently replace the Makefile's
# optimized flags (it uses CFLAGS and ARCH if they are set), so the build runs without them, and says so.
MAKE_VARS="CFLAGS CPPFLAGS LDFLAGS LDLIBS ARCH PORTABLE CPU_LEVELS DIST_ARCH DIST_B LV_FLAGS_v3 LV_FLAGS_v4 B
    NO_URING ZSTD_LIB URING_LIB LINK_EXTRA TSDBD MAKEFLAGS MFLAGS"
ENV_SET=()
for v in $MAKE_VARS; do
    [ -n "${!v+x}" ] && ENV_SET+=("$v=${!v}")
done

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT" || exit 2

MODE=standard
REPS=
SCRATCH=${TMPDIR:-/var/tmp}/tsdb-bench-$(id -un)
OUT=
PORTABLE=0
KEEP=0
IO=
CPUCMP=0
JOBS=${MAKE_JOBS:-$(nproc 2>/dev/null || echo 2)}

usage() {
    cat <<'EOF'
Benchmark tsdb on this machine and write a summary of how fast it is.

  tools/bench-tsdb.sh [--quick | --standard | --full] [--reps N] [--dir SCRATCH] [--out RESULTS_DIR]
                      [--portable] [--cpu-compare] [--keep] [--io auto|uring|epoll]

How big
  --quick       tiny sizes, 1 run: checks that the script works here (a few minutes). The numbers mean nothing.
  --standard    the default. 100 series x 1 million rows, 5 runs of everything. About 40 minutes, plus the build.
  --full        --standard, plus float64 and int64 data, bigger durable inserts, and a large data set that fits this
                machine (up to 10 billion rows; the size is printed). About 1.5 hours on a big machine.

Options
  --reps N      runs of each test (default 5; --quick 1). Every number is a mean of the runs, with the standard
                deviation and the lowest and highest run, because a single run on a real machine varies by percent.
  --dir PATH    scratch directory for the test data (default /var/tmp/tsdb-bench-USER). Put it on a real disk,
                not on tmpfs: the durable-write numbers depend on the speed of fsync. About 8 GB free for
                --standard; --full also needs room for the large data set (1.4 GB per billion rows).
  --out PATH    where the results go (default bench-results/HOST-DATE in the clone).
  --portable    measure the release build (make dist: x86-64-v2 base, v3 and v4 copies of the codecs picked at
                start-up): what users of the tarball get, and the same on every machine, so numbers compare across
                machines. Needs the static libraries (libzstd.a, and liburing.a if liburing is installed).
                Default: the optimized build for this CPU (-O3 -march=native).
  --cpu-compare also run the scripts of bench/cpu-compare/ (step 7 below): what the second hardware thread of a
                core is worth, how queries scale with threads, and where the short-run kernels hand over to the wide
                ones on this CPU. Takes another half hour; this is the deeper per-CPU comparison, see
                bench/cpu-compare/README.md.
  --keep        keep the scratch data at the end (default: remove it).
  --io BACKEND  the tsdbd event loop (default: the server's own choice).

The build
  It always compiles with optimization, from scratch, in a directory of its own (build-bench, or
  build-bench-portable with --portable), so no debug or sanitizer build, and no stale object, is ever measured.
  CFLAGS, ARCH and the other variables that change the build are removed from make's environment (with a
  warning); the run refuses flags without -O2/-O3 or with -fsanitize. summary.md and summary.json record the
  compiler, the flags as make expanded them, native or portable, liburing or not, the CPU level and kernels
  used at run time, and the git commit.

What it runs, in this order, each test REPS times:
  1 value codecs         codec-bench, type-bench: bits per value and encode/decode speed; decode-bench: speed of
                         bit unpacking by width and of the integer and decimal codecs
  2 aggregation kernels  tools/fold-bench.sh: rows/s of the SIMD kernels, scalar up to AVX-512, in interleaved rounds
  3 embedded database    tsdb-bench: load, size per row, scans, aggregates, group by, random windows on 1 and 32 threads
  4 server               tsdbd + tsdbc-bench over TCP: load, queries on 1 and 32 connections, many connections open
  5 durable writes       insert-bench: inserts with fsync, from 1 row per call to big batches
  6 large data set       --full only (and a tiny one in --quick): scale-bench loads and queries billions of rows
  7 CPU comparison       only with --cpu-compare: bench/cpu-compare/ht-sweep.sh and thresholds.sh (their output is
                         kept as it is, one run each)

Before the first test it prints what is missing (a compiler, libzstd headers, ...), records the machine (CPU, memory,
disk, governor, git commit) and warns if the machine is busy or the CPU governor is not "performance". Run nothing
else while it measures. It leaves a summary of the results in RESULTS_DIR/summary.md (and summary.json, for comparing
two machines), and exits with status 1 if any test failed (the summary says which).
EOF
}

while [ $# -gt 0 ]; do
    case $1 in
    --quick) MODE=quick ;;
    --standard) MODE=standard ;;
    --full) MODE=full ;;
    --reps) REPS=${2:-}; shift ;;
    --dir) SCRATCH=${2:-}; shift ;;
    --out) OUT=${2:-}; shift ;;
    --portable) PORTABLE=1 ;;
    --keep) KEEP=1 ;;
    --cpu-compare) CPUCMP=1 ;;
    --io) IO=${2:-}; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "bench-tsdb.sh: unknown option $1 (see --help)" >&2; exit 2 ;;
    esac
    shift
done

case ${REPS:-1} in *[!0-9]*|0) echo "bench-tsdb.sh: --reps wants a positive number" >&2; exit 2 ;; esac
[ -n "$SCRATCH" ] || { echo "bench-tsdb.sh: --dir wants a path" >&2; exit 2; }
if [ -z "$REPS" ]; then
    if [ "$MODE" = quick ]; then REPS=1; else REPS=5; fi
fi
case $IO in ''|auto|uring|epoll) ;; *) echo "bench-tsdb.sh: --io wants auto, uring or epoll" >&2; exit 2 ;; esac

# ------------------------------------------------------------------ sizes per mode

CONNS_HI=32                    # clients of the "many clients" rows (as in the README's tables)
case $MODE in
quick)
    EMB_SERIES=10; EMB_POINTS=20000
    FOLD_ARGS="--rows 4096 --ms 20"
    DEC_ARGS="--rounds 1 --ms 5"
    CC_HT_ARGS="--rounds 1 --seconds 1"; CC_THR_ARGS="--rounds 1 --ms 5"
    TCP_SERIES=10; TCP_POINTS=20000; TCP_SECONDS=0.3; TCP_HOLD=100
    INS_ARGS="--series 100 --max-batch 10000 --seconds 0.5"
    SCALE_SERIES=100; SCALE_POINTS=100000
    NEED_GB=1
    ;;
standard)
    EMB_SERIES=100; EMB_POINTS=1000000
    FOLD_ARGS=""
    DEC_ARGS="--rounds 3 --ms 50"
    CC_HT_ARGS="--rounds \$REPS --seconds 3"; CC_THR_ARGS="--rounds \$REPS --ms 50"
    TCP_SERIES=100; TCP_POINTS=1000000; TCP_SECONDS=3; TCP_HOLD=10000
    INS_ARGS="--series 1000 --max-batch 10000000 --seconds 3"
    SCALE_SERIES=0; SCALE_POINTS=0
    NEED_GB=8
    ;;
full)
    EMB_SERIES=100; EMB_POINTS=1000000
    FOLD_ARGS=""
    DEC_ARGS="--rounds 3 --ms 50"
    CC_HT_ARGS="--rounds \$REPS --seconds 3 --server"; CC_THR_ARGS="--rounds \$REPS --ms 50 --vary"
    TCP_SERIES=100; TCP_POINTS=1000000; TCP_SECONDS=3; TCP_HOLD=10000
    INS_ARGS="--series 1000 --max-batch 100000000 --seconds 3"
    SCALE_SERIES=-1; SCALE_POINTS=10000000   # -1: chosen from the free disk and memory
    NEED_GB=10
    ;;
esac

# ------------------------------------------------------------------ helpers

log() { printf '%s\n' "$*" >&2; }
die() { printf 'bench-tsdb.sh: %s\n' "$*" >&2; exit 2; }
WARNINGS=()
warn() {
    WARNINGS+=("$*")
    printf 'WARNING: %s\n' "$*" >&2
}
have() { command -v "$1" >/dev/null 2>&1; }

# A pid we may signal: a plain number above 1 (never 0, -1 or a name: kill(-1) reaches every process of the user).
safe_pid() {
    case ${1:-} in '' | *[!0-9]*) return 1 ;; esac
    [ "$1" -gt 1 ]
}

SRV_PID=
SRV_PORT=
WORK=
CREATED_SCRATCH=0
RES=
RAW=
FAILED=0

stop_server() {
    local p=$SRV_PID i
    SRV_PID=
    safe_pid "$p" || return 0
    kill -0 "$p" 2>/dev/null || { wait "$p" 2>/dev/null; return 0; }
    kill -TERM "$p" 2>/dev/null
    for ((i = 0; i < 300; i++)); do
        kill -0 "$p" 2>/dev/null || break
        sleep 0.1
    done
    kill -0 "$p" 2>/dev/null && kill -KILL "$p" 2>/dev/null
    wait "$p" 2>/dev/null
    return 0
}

cleanup() {
    stop_server
    if [ -n "$WORK" ] && [ -d "$WORK" ]; then
        if [ "$KEEP" = 1 ]; then
            log "scratch data kept in $WORK"
        else
            rm -rf -- "$WORK"
            [ "$CREATED_SCRATCH" = 1 ] && rmdir -- "$SCRATCH" 2>/dev/null
        fi
    fi
}
trap cleanup EXIT
trap 'log "interrupted"; exit 130' INT
trap 'log "terminated"; exit 143' TERM

# run NAME REP CMD...: output (stdout and stderr) to raw/NAME.REP.txt (appended), status to status.txt.
run() {
    local name=$1 rep=$2 rc
    shift 2
    if [ "$rep" = 1 ]; then
        printf '%s\t%s\n' "$name" "$(printf '%s ' "$@" | sed -e "s| $WORK/| SCRATCH/|g; s|^$WORK/|SCRATCH/|; s|--port [0-9]*|--port PORT|; s| *\$||")" \
            >>"$RES/commands.txt"
    fi
    log "  $name, run $rep of $REPS ..."
    "$@" >>"$RAW/$name.$rep.txt" 2>&1
    rc=$?
    printf '%s %s %s\n' "$name" "$rep" "$rc" >>"$RES/status.txt"
    if [ $rc -ne 0 ]; then
        FAILED=1
        log "  $name run $rep FAILED with status $rc (see $RAW/$name.$rep.txt)"
    fi
    return $rc
}

meta() { printf '%s=%s\n' "$1" "$2" >>"$RES/meta.kv"; }

# ------------------------------------------------------------------ prerequisites

CC=${CC:-cc}
missing=()
have "$CC" || missing+=("a C compiler ($CC)")
have make || missing+=("make")
have python3 || missing+=("python3 (the summary is made by tools/bench-summary.py)")
if have "$CC"; then
    printf '#include <zstd.h>\nint main(void){return ZSTD_versionNumber()==0;}\n' |
        "$CC" -x c - -o /dev/null -lzstd 2>/dev/null || missing+=("the libzstd development files (zstd.h, -lzstd)")
fi
URING=1
if have "$CC" && ! printf '#include <liburing.h>\nint main(void){struct io_uring r; return io_uring_queue_init(2,&r,0);}\n' |
    "$CC" -x c - -o /dev/null -luring 2>/dev/null; then
    URING=0
fi
if [ ${#missing[@]} -gt 0 ]; then
    log "bench-tsdb.sh: missing on this machine:"
    for m in "${missing[@]}"; do log "  - $m"; done
    log "Ubuntu/Debian: sudo apt install build-essential libzstd-dev liburing-dev python3"
    log "AlmaLinux/RHEL: sudo dnf install gcc make libzstd-devel liburing-devel python3"
    exit 2
fi
NOURING_MAKE=()
if [ $URING = 0 ]; then
    log "note: liburing is not installed, so tsdbd is built on epoll only (make NO_URING=1)."
    NOURING_MAKE=(NO_URING=1)
fi
if [ $PORTABLE = 1 ]; then
    if ! printf 'int main(void){return 0;}\n' | "$CC" -x c - -o /dev/null -l:libzstd.a 2>/dev/null; then
        die "--portable links libzstd statically: install libzstd-static (AlmaLinux) or libzstd-dev (Ubuntu), or leave out --portable"
    fi
    if [ $URING = 1 ] && ! printf 'int main(void){return 0;}\n' | "$CC" -x c - -o /dev/null -l:liburing.a 2>/dev/null; then
        die "--portable links liburing statically: install liburing-devel with its static library, or leave out --portable"
    fi
fi
CCV=$("$CC" --version 2>/dev/null | head -n 1)

# ------------------------------------------------------------------ scratch, results

mkdir -p "$(dirname "$SCRATCH")" 2>/dev/null
if [ ! -d "$SCRATCH" ]; then
    mkdir -p "$SCRATCH" || die "cannot create $SCRATCH"
    CREATED_SCRATCH=1
fi
WORK=$(mktemp -d "$SCRATCH/run.XXXXXX") || die "cannot make a directory in $SCRATCH"
if [ -z "$OUT" ]; then
    OUT=$ROOT/bench-results/$(hostname -s 2>/dev/null || echo host)-$(date +%Y-%m-%d_%H%M)
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
    die "$SCRATCH has $((${free_kb:-0} / 1024 / 1024)) GB free; --$MODE wants $NEED_GB GB (use --dir on a bigger disk)"
fstype=$(stat -f -c %T "$WORK" 2>/dev/null || echo unknown)
case $fstype in
tmpfs | ramfs)
    warn "the scratch directory $SCRATCH is on $fstype (RAM): fsync costs nothing there, so the durable-write and load numbers are meaningless. Use --dir on a real disk."
    ;;
esac
mem_kb=$(awk '/^MemTotal:/ { print $2 }' /proc/meminfo 2>/dev/null)
ncpu=$(nproc 2>/dev/null || echo 1)

# ------------------------------------------------------------------ machine

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
        warn "the machine is not quiet: $busy CPUs busy over the last 2 s (the rule is fewer than 1.5). Numbers will be low and noisy."
    fi
    if awk -v l="$load1" -v n="$ncpu" 'BEGIN { exit !(l > 2.0 && l > n / 8) }'; then
        warn "the 1-minute load average is $load1: something ran recently."
    fi
}

machine_info() {
    local model flags f gov threads cores sockets virt os dev fs
    meta date "$(date '+%Y-%m-%d %H:%M %Z')"
    meta mode "$MODE"
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
    for f in sse4_2 avx avx2 bmi2 avx512f avx512dq avx512bw avx512vl avx512_vbmi avx512_vbmi2 avx512_bitalg avx512_vpopcntdq avx512_vp2intersect; do
        case " $flags " in *" $f "*) found="$found $f" ;; esac
    done
    meta cpu_flags "${found# }"
    CPUFLAGS=$flags
    case " $flags " in *" hypervisor "*) virt="yes, a virtual machine" ;; *) virt="no" ;; esac
    meta virt "$virt"
    meta memory "$((${mem_kb:-0} / 1024 / 1024)) GB ($(awk '/^MemAvailable:/ { printf "%d", $2 / 1048576 }' /proc/meminfo) GB available)"
    meta kernel "$(uname -sr)"
    os=$(. /etc/os-release 2>/dev/null && echo "${PRETTY_NAME:-}")
    meta os "${os:-unknown}"
    gov=$(cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null | sort | uniq -c |
        awk '{ printf "%s%s (%s CPUs)", s, $2, $1; s = ", " }')
    meta governor "${gov:-unknown (no cpufreq: a virtual machine, or a fixed frequency)}"
    if [ -n "$gov" ]; then
        case $gov in
        performance*) ;;
        *) case $gov in *performance*) ;; *)
            warn "the CPU governor is \"$gov\", not performance: the clock speed moves during a run. Set it with: sudo cpupower frequency-set -g performance (or: powerprofilesctl set performance)" ;;
        esac ;;
        esac
        case $gov in *,*) warn "the CPUs have different governors ($gov)." ;; esac
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
        [ "$dirty" -gt 0 ] && warn "the git tree is dirty ($dirty files): the results do not name a clean commit."
    else
        meta git "unknown (not a git checkout)"
    fi
}

# ------------------------------------------------------------------ build

build() {
    local what v mk=() t cflags arch
    # A build directory of its own, made from scratch every run: no stale, debug or sanitizer objects (build/,
    # build-asan, build-tsan, build-fuzz...) are ever measured. The names are fixed, so the rm cannot reach anything else.
    if [ $PORTABLE = 1 ]; then B=build-bench-portable; else B=build-bench; fi
    for v in $MAKE_VARS; do mk+=(-u "$v"); done
    for v in "${ENV_SET[@]+"${ENV_SET[@]}"}"; do
        warn "the environment variable ${v%%=*} is set (${v#*=}); it is ignored, so that the measured build has the Makefile's own optimized flags."
    done
    MAKE_ENV=(env "${mk[@]}")
    rm -rf -- "${ROOT:?}/$B"
    if [ $PORTABLE = 1 ]; then
        what="make -j$JOBS dist DIST_B=$B${NOURING_MAKE[*]:+ ${NOURING_MAKE[*]}}"
        log "building the portable release build from scratch: $what ..."
        "${MAKE_ENV[@]}" make -j"$JOBS" dist DIST_B="$B" ${NOURING_MAKE[@]+"${NOURING_MAKE[@]}"} >"$RAW/build.log" 2>&1
    else
        what="make -j$JOBS B=$B${NOURING_MAKE[*]:+ ${NOURING_MAKE[*]}}"
        log "building the optimized native build from scratch: $what ..."
        "${MAKE_ENV[@]}" make -j"$JOBS" B="$B" ${NOURING_MAKE[@]+"${NOURING_MAKE[@]}"} >"$RAW/build.log" 2>&1
    fi
    if [ $? -ne 0 ]; then
        tail -n 20 "$RAW/build.log" >&2
        die "the build failed (full log: $RAW/build.log)"
    fi
    printf 'build\t%s\n' "$what" >>"$RES/commands.txt"
    for t in codec-bench type-bench decode-bench fold-bench tsdb-bench tsdbc-bench insert-bench scale-bench tsdbd; do
        [ -x "$B/$t" ] || die "the build did not make $B/$t"
    done
    # The flags as make expanded them (not as this script thinks they are).
    show() { "${MAKE_ENV[@]}" make -s B="$B" --eval='show-%: ; @echo $($*)' "show-$1" 2>/dev/null | head -n 1; }
    cflags=$(show CFLAGS)
    case " $cflags " in
    *" -O2 "* | *" -O3 "*) ;;
    *) die "the compile flags are not optimized (CFLAGS = $cflags)" ;;
    esac
    case $cflags in *-fsanitize* | *-O0* | *--coverage*) die "the compile flags are for debugging (CFLAGS = $cflags)" ;; esac
    meta cflags "$cflags"
    if [ $PORTABLE = 1 ]; then
        arch="$(show DIST_ARCH) (base, glibc 2.34 compatible, static zstd/uring); codec and value files again for v3: $(show LV_FLAGS_v3) and v4: $(show LV_FLAGS_v4)"
        meta build "portable release build (make dist): what the tarball ships; the CPU level is chosen at start-up"
    else
        arch=$(show ARCH)
        meta build "optimized native build: -O3 for this CPU, no CPU levels"
    fi
    meta arch "$arch"
    meta build_variant "$([ $PORTABLE = 1 ] && echo portable || echo native), $([ $URING = 1 ] && echo 'with liburing' || echo 'NO_URING=1 (tsdbd on epoll only)'), directory $B (rebuilt from scratch)"
    meta tsdb_version "$("$B/tsdbd" --version 2>&1 | head -n 1)"
    if [ $PORTABLE = 1 ]; then
        local lv=v2 fl
        fl=$(awk '/^flags/ { print; exit }' /proc/cpuinfo)
        hasf() { case " $fl " in *" $1 "*) return 0 ;; esac; return 1; }
        hasf avx2 && hasf bmi2 && hasf fma && lv=v3
        [ $lv = v3 ] && hasf avx512f && hasf avx512bw && hasf avx512vl && hasf avx512dq && hasf avx512cd && lv=v4
        meta cpu_level "$lv (the highest level this CPU's features allow, which the portable build picks at start-up; TSDB_CPU=v2|v3|v4 lowers it; the fold-bench section shows the kernels of each level)"
    else
        meta cpu_level "none: a native build is compiled for this CPU; the aggregation kernels are chosen at start-up (see kernels chosen)"
    fi
}

# ------------------------------------------------------------------ server

start_server() {
    local d=$1 pf port="" spid="" i
    local ioargs=()
    [ -n "$IO" ] && ioargs=(--io "$IO")
    rm -f "$d/port"
    "$B/tsdbd" --dir "$d/db" --listen 127.0.0.1 --port 0 --port-file "$d/port" ${ioargs[@]+"${ioargs[@]}"} >"$d/log" 2>&1 &
    SRV_PID=$!
    for ((i = 0; i < 600; i++)); do
        if ! kill -0 "$SRV_PID" 2>/dev/null; then
            log "tsdbd exited at start-up:"
            tail -n 5 "$d/log" >&2
            SRV_PID=
            return 1
        fi
        if [ -s "$d/port" ]; then
            read -r port spid _ <"$d/port"
            break
        fi
        sleep 0.1
    done
    if [ -z "$port" ]; then
        log "tsdbd did not write $d/port within 60 s"
        stop_server
        return 1
    fi
    if [ "$spid" != "$SRV_PID" ]; then
        log "the port file names pid $spid, not the tsdbd started here ($SRV_PID): not using it"
        stop_server
        return 1
    fi
    SRV_PORT=$port
    return 0
}

# ------------------------------------------------------------------ the tests

t_codecs() {
    log "1/6 value codecs (codec-bench, type-bench, decode-bench)"
    meta size_decode "decode-bench $DEC_ARGS in every run; each cell is the mean of the rounds inside one run, and the table shows the mean of the runs."
    local r
    for ((r = 1; r <= REPS; r++)); do
        run codec "$r" "$B/codec-bench"
        run type "$r" "$B/type-bench"
        # shellcheck disable=SC2086
        run decode "$r" "$B/decode-bench" $DEC_ARGS
    done
}

t_fold() {
    log "2/6 aggregation kernels (fold-bench)"
    local dir=$B cfgs
    mkdir -p "$RAW/fold"
    if [ $PORTABLE = 1 ]; then
        cfgs="v2:scalar v2:sse v2:avx v3 v4"
    else
        cfgs=":scalar :sse :avx :avx2 :avx512"
    fi
    printf '%s\n' "$cfgs" >"$RAW/fold-order.txt"
    printf 'fold\tFOLD_CONFIGS="%s" tools/fold-bench.sh %s %s %s\n' "$cfgs" "$dir" "$REPS" "$FOLD_ARGS" >>"$RES/commands.txt"
    # shellcheck disable=SC2086
    FOLD_CONFIGS=$cfgs FOLD_RAW_DIR=$RAW/fold tools/fold-bench.sh "$dir" "$REPS" $FOLD_ARGS >"$RAW/fold-summary.1.txt" 2>&1
    local rc=$?
    printf 'fold-summary 1 %s\n' "$rc" >>"$RES/status.txt"
    [ $rc -ne 0 ] && { FAILED=1; log "  fold-bench.sh FAILED with status $rc (see $RAW/fold-summary.1.txt)"; }
    return 0
}

t_embedded() {
    log "3/6 embedded database (tsdb-bench: $EMB_SERIES series x $EMB_POINTS points)"
    meta size_embedded "$EMB_SERIES series x $EMB_POINTS points per run, mixed sensor kinds, 4 writer threads, fsync on, one fresh database per run. The embedded engine in this process, no network."
    local r
    for ((r = 1; r <= REPS; r++)); do
        run tsdb-bench-f32 "$r" "$B/tsdb-bench" --dir "$WORK/emb" --series "$EMB_SERIES" --points "$EMB_POINTS"
    done
    if [ "$MODE" = full ]; then
        meta size_embedded64 "the same with float64 values"
        meta size_embedded_int64 "the same with int64 values"
        for ((r = 1; r <= REPS; r++)); do
            run tsdb-bench-f64 "$r" "$B/tsdb-bench" --dir "$WORK/emb" --series "$EMB_SERIES" --points "$EMB_POINTS" --type float64
        done
        for ((r = 1; r <= REPS; r++)); do
            run tsdb-bench-i64 "$r" "$B/tsdb-bench" --dir "$WORK/emb" --series "$EMB_SERIES" --points "$EMB_POINTS" --type int64
        done
    fi
    rm -rf -- "$WORK/emb"
}

t_server() {
    log "4/6 server (tsdbd + tsdbc-bench: $TCP_SERIES series x $TCP_POINTS points)"
    local data=$WORK/tcp-data.bin r d hold nofile
    run tcp-export 1 "$B/tsdb-bench" --export "$data" --series "$TCP_SERIES" --points "$TCP_POINTS" || return 0
    # the client needs one descriptor per connection it holds
    nofile=$(ulimit -n)
    hold=$TCP_HOLD
    if [ "$nofile" != unlimited ] && [ "$hold" -gt $((nofile - 64)) ]; then
        ulimit -n "$(ulimit -Hn)" 2>/dev/null
        nofile=$(ulimit -n)
        [ "$nofile" != unlimited ] && [ "$hold" -gt $((nofile - 64)) ] && hold=$((nofile - 64))
    fi
    [ "$hold" -ge 10 ] || hold=10
    meta size_server "$TCP_SERIES series x $TCP_POINTS points loaded over 4 connections into a fresh tsdbd on 127.0.0.1 for each run, then queried for $TCP_SECONDS s per test on 1 and $CONNS_HI connections; $hold connections held open at once. tsdbd is started with --port 0 --port-file and stopped by its pid."
    for ((r = 1; r <= REPS; r++)); do
        d=$WORK/srv.$r
        mkdir -p "$d"
        if ! start_server "$d"; then
            FAILED=1
            printf 'tcp-server %s 1\n' "$r" >>"$RES/status.txt"
            continue
        fi
        if [ "$r" = 1 ]; then
            meta simd "$(sed -n 's/.*simd \([a-z0-9_]*\).*/\1/p' "$d/log" | head -n 1)"
            meta io "$(sed -n 's/.*io \([a-z_]*\)$/\1/p' "$d/log" | head -n 1)"
        fi
        run tcp-load "$r" "$B/tsdbc-bench" --port "$SRV_PORT" --conns 4 load "$data"
        run tcp-load "$r" "$B/tsdbc-bench" --port "$SRV_PORT" optimize
        run tcp-query-c1 "$r" "$B/tsdbc-bench" --port "$SRV_PORT" --conns 1 --series "$TCP_SERIES" --points "$TCP_POINTS" \
            --seconds "$TCP_SECONDS" query
        run "tcp-query-c$CONNS_HI" "$r" "$B/tsdbc-bench" --port "$SRV_PORT" --conns "$CONNS_HI" --series "$TCP_SERIES" \
            --points "$TCP_POINTS" --seconds "$TCP_SECONDS" query
        run tcp-conns "$r" "$B/tsdbc-bench" --port "$SRV_PORT" --conns "$hold" --series "$TCP_SERIES" \
            --points "$TCP_POINTS" --seconds "$TCP_SECONDS" connections
        if ! kill -0 "$SRV_PID" 2>/dev/null; then
            FAILED=1
            log "  tsdbd died during run $r (log: $d/log)"
            cp "$d/log" "$RAW/tcp-tsdbd-died.$r.txt" 2>/dev/null
        fi
        stop_server
        [ "$r" = 1 ] && cp "$d/log" "$RAW/tcp-tsdbd.1.txt" 2>/dev/null
        rm -rf -- "$d"
    done
    rm -f -- "$data"
}

t_durable() {
    log "5/6 durable writes (insert-bench, fsync on)"
    meta size_durable "arguments: $INS_ARGS. The database is on $SCRATCH ($fstype); every call waits for its fsync, so these numbers are the speed of this disk as much as of tsdb."
    local r
    for ((r = 1; r <= REPS; r++)); do
        # shellcheck disable=SC2086
        run insert "$r" "$B/insert-bench" --dir "$WORK/ins" $INS_ARGS
        rm -rf -- "$WORK/ins" "$WORK"/ins.vol*
    done
}

choose_scale() {
    # rows: 10 M points per series; ~1.4 bytes per row on disk, and the merges need room beside it
    local gb_free=$((free_kb / 1024 / 1024)) gb_mem=$((mem_kb / 1024 / 1024)) s need
    for s in 1000 500 300 100; do
        need=$((s * SCALE_POINTS / 1000000 * 14 / 10 / 1000 + 1))     # GB of data
        if [ $((need * 2)) -le "$gb_free" ] && [ "$need" -le "$((gb_mem > 0 ? gb_mem : 1))" ]; then
            SCALE_SERIES=$s
            log "  large data set: $s series x $SCALE_POINTS points = $((s * SCALE_POINTS / 1000000000)) G rows, about $need GB on disk ($gb_free GB free, $gb_mem GB RAM)"
            return 0
        fi
    done
    return 1
}

t_scale() {
    log "6/6 large data set (scale-bench)"
    if [ "$SCALE_SERIES" -lt 0 ]; then
        if ! choose_scale; then
            warn "no room for the large data set (needs 100 series x 10 M rows = 1 G rows, about 3 GB of disk and RAM): skipped."
            return 0
        fi
    fi
    local threads=$((ncpu < 8 ? ncpu : 8)) r
    [ "$threads" -ge 2 ] || threads=2
    meta size_scale "$SCALE_SERIES series x $SCALE_POINTS points = $(awk -v s="$SCALE_SERIES" -v p="$SCALE_POINTS" 'BEGIN { printf "%.2f", s * p / 1e9 }') G rows, loaded once ($threads threads, then the merges settle: this takes at least a minute), queried $REPS time(s) with $ncpu threads."
    run scale-load 1 "$B/scale-bench" load --dir "$WORK/scale" --series "$SCALE_SERIES" --points "$SCALE_POINTS" --threads "$threads" || return 0
    for ((r = 1; r <= REPS; r++)); do
        run scale-query "$r" "$B/scale-bench" query --dir "$WORK/scale" --series "$SCALE_SERIES" --points "$SCALE_POINTS" --threads "$ncpu"
    done
    rm -rf -- "$WORK/scale"
}

t_cpu_compare() {
    log "7 CPU comparison (bench/cpu-compare)"
    local cc=$WORK/cpu-compare lvl levels="v3" ht_args thr_args
    mkdir -p "$cc"
    case " $CPUFLAGS " in *" avx512f "*" avx512bw "*) levels="v3 v4" ;; esac
    ht_args=${CC_HT_ARGS//\$REPS/$REPS}
    thr_args=${CC_THR_ARGS//\$REPS/$REPS}
    meta size_cpu_compare "bench/cpu-compare scripts, output as they print it: ht-sweep.sh $ht_args, thresholds.sh $thr_args at $levels (it builds a copy of the tree in the scratch directory; make runs with $JOBS jobs). They need a quiet machine (load average below 2) and refuse otherwise."
    # shellcheck disable=SC2086
    run cpucmp-ht-sweep 1 bench/cpu-compare/ht-sweep.sh --build "$ROOT/$B" --work "$cc/ht" $ht_args
    for lvl in $levels; do
        # thresholds.sh builds with make -j$(nproc): nproc honours OMP_NUM_THREADS
        # shellcheck disable=SC2086
        run "cpucmp-thresholds-$lvl" 1 env OMP_NUM_THREADS="$JOBS" bench/cpu-compare/thresholds.sh --cpu "$lvl" --work "$cc/thr" $thr_args
    done
}

# ------------------------------------------------------------------ main

log "tsdb benchmark: mode $MODE, $REPS run(s) of each test, results in $RES"
log "scratch directory: $SCRATCH (data removed at the end$([ $KEEP = 1 ] && echo ', except --keep'))"
case $MODE in
quick) log "--quick: tiny sizes to check that the script works; the numbers mean nothing." ;;
standard) log "--standard: expect about 40 minutes with 5 runs, plus the build. Leave the machine alone meanwhile." ;;
full) log "--full: expect about 1.5 hours. Leave the machine alone meanwhile." ;;
esac

machine_info
quiet_check
build

START=$(date +%s)
t_codecs
t_fold
t_embedded
t_server
t_durable
if [ "$SCALE_SERIES" != 0 ]; then t_scale; fi
if [ $CPUCMP = 1 ]; then t_cpu_compare; fi
secs=$(($(date +%s) - START))
meta duration "$((secs / 60)) min $((secs % 60)) s (the tests, not the build)"

printf '%s\n' "${WARNINGS[@]+"${WARNINGS[@]}"}" | sed '/^$/d' >"$RES/warnings.txt"
log "making the summary ..."
python3 -I "$ROOT/tools/bench-summary.py" "$RES" || FAILED=1
log ""
log "results: $RES/summary.md (and summary.json); raw output in $RES/raw/"
if [ -f "$RES/summary.md" ]; then
    awk '/^## At a glance/ { p = 1; next } /^## / && p { exit } p' "$RES/summary.md" >&2
fi
if [ $FAILED = 1 ]; then
    log "some tests failed: see the Problems in summary.md"
    exit 1
fi
exit 0
