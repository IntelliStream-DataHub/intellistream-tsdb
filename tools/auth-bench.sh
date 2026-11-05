#!/bin/bash
# The before-and-after benchmark of login and per-series grants (docs/design-auth.md, A27): what does access control
# cost in tsdbd?
#
#   tools/auth-bench.sh [--quick] [--reps N] [--seconds S] [--dir SCRATCH] [--out RESULTS_DIR] [--lock FILE] [--keep]
#
# It builds three trees from `git archive`, each from scratch with the Makefile's optimized flags into a directory of
# its own, as tools/bench-tsdb.sh does: main before the work of 2026-11-05 (3bd4e96), main without auth (ffa4942, the
# baseline that isolates the login) and the auth build (HEAD of this clone). Seven configurations are measured:
#
#   main0      3bd4e96
#   main1      ffa4942
#   noauth     auth build, tsdbd --no-auth
#   super      auth build, logged in as a superuser
#   prefix     auth build, a user with read+insert on 8 prefix: grants
#   name5000   auth build, a user with read+insert on 5,000 name: grants (every series the benchmark touches among them)
#   glob64     auth build, a user with read+insert on 64 glob: grants (the most a user may hold), one of which matches
#
# Each has its own database, loaded once through its own tsdbd and tsdbc-bench from the same file of rows, and copied
# fresh for every run. Workloads (each REPS times, interleaved across the configurations, the order reversed every
# second repetition, every one under an exclusive lock with a quiet check before it when --lock names the lock):
#
#   queries     tsdbc-bench query: raw rows, two bucket queries, TWA and the whole range, on 1 and 32 connections;
#               and with --multi 100: QUERY_MULTI of 100 series per request, on 1 and 32 connections
#   logins      auth-bench-client logins: connect + one small request + close per iteration, on 1 and 32 threads;
#               with the client's key cache warm, and --cold (a new process for every login: PBKDF2 each time)
#   inserts     auth-bench-client insert: 1,000 rows a request, all in one series, or one row from each of 1,000
#               series, on 1 and 32 connections (last in a run: it grows the database)
#
# Every tsdbd runs with --wal-sync-ms 10, so inserts wait for no fsync and show the server's CPU work; with the
# default (an fsync per request) the per-request cost of a login check is lost in the disk's 0.7 ms.
# The raw output of every run goes to RESULTS_DIR/raw, the tables (mean, standard deviation, lowest and highest run) to
# summary.md and summary.json (tools/auth-bench-summary.py). Besides the build directories under SCRATCH it writes
# only to SCRATCH and RESULTS_DIR, and stops only the tsdbd it started, by pid.
set -u -o pipefail

# Variables that change what make builds: taken from the environment they would replace the Makefile's optimized
# flags, so the builds run without them (and say so).
MAKE_VARS="CFLAGS CPPFLAGS LDFLAGS LDLIBS ARCH PORTABLE CPU_LEVELS DIST_ARCH DIST_B LV_FLAGS_v3 LV_FLAGS_v4 B
    NO_URING ZSTD_LIB URING_LIB LINK_EXTRA TSDBD MAKEFLAGS MFLAGS"
ENV_SET=()
for v in $MAKE_VARS; do
    [ -n "${!v+x}" ] && ENV_SET+=("$v=${!v}")
done

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT" || exit 2

REF_BEFORE=${AB_REF_BEFORE:-3bd4e96}
REF_MAIN=${AB_REF_MAIN:-ffa4942}
REF_AUTH=${AB_REF_AUTH:-HEAD}

MODE=standard
REPS=
SECS=
SCRATCH=${TMPDIR:-/var/tmp}/auth-bench-$(id -un)
OUT=
LOCK=
KEEP=0
JOBS=${MAKE_JOBS:-$(nproc 2>/dev/null || echo 2)}

usage() {
    sed -n '2,/^set -u/p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'
}

while [ $# -gt 0 ]; do
    case $1 in
    --quick) MODE=quick ;;
    --reps) REPS=${2:-}; shift ;;
    --seconds) SECS=${2:-}; shift ;;
    --dir) SCRATCH=${2:-}; shift ;;
    --out) OUT=${2:-}; shift ;;
    --lock) LOCK=${2:-}; shift ;;
    --keep) KEEP=1 ;;
    -h | --help) usage; exit 0 ;;
    *) echo "auth-bench.sh: unknown option $1 (see --help)" >&2; exit 2 ;;
    esac
    shift
done

case ${REPS:-1} in *[!0-9]* | 0) echo "auth-bench.sh: --reps wants a positive number" >&2; exit 2 ;; esac
[ -n "$SCRATCH" ] || { echo "auth-bench.sh: --dir wants a path" >&2; exit 2; }
if [ "$MODE" = quick ]; then
    SERIES=10; POINTS=20000; : "${SECS:=0.3}"; : "${REPS:=1}"; NEED_GB=1
else
    SERIES=100; POINTS=1000000; : "${SECS:=3}"; : "${REPS:=5}"; NEED_GB=4
fi
POOL=1000       # series of the insert benchmark ("ins.N")
ROWS=1000       # rows an insert request carries
NAMES=5000      # name: grants of the name5000 user
GLOBS=64        # glob: grants of the glob64 user (USER_GLOBS in server/users.h)
HI=32           # the "many clients" connection count
MULTI=100       # series a QUERY_MULTI request carries

# ------------------------------------------------------------------ helpers

log() { printf '%s\n' "$*" >&2; }
die() { printf 'auth-bench.sh: %s\n' "$*" >&2; exit 2; }
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
PW=

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

meta() { printf '%s=%s\n' "$1" "$2" >>"$RES/meta.kv"; }

# shared CMD...: builds, loads and copies keep out of other jobs' timings (a shared hold of the lock).
shared() {
    if [ -n "$LOCK" ]; then flock -s "$LOCK" "$@"; else "$@"; fi
}

# ------------------------------------------------------------------ prerequisites

CC=${CC:-cc}
missing=()
have "$CC" || missing+=("a C compiler ($CC)")
have make || missing+=("make")
have git || missing+=("git")
have tar || missing+=("tar")
have python3 || missing+=("python3 (the summary is made by tools/auth-bench-summary.py)")
have flock || missing+=("flock (util-linux)")
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
    log "auth-bench.sh: missing on this machine:"
    for m in "${missing[@]}"; do log "  - $m"; done
    log "Ubuntu/Debian: sudo apt install build-essential libzstd-dev liburing-dev python3 git util-linux"
    log "AlmaLinux/RHEL: sudo dnf install gcc make libzstd-devel liburing-devel python3 git util-linux"
    exit 2
fi
NOURING_MAKE=()
if [ $URING = 0 ]; then
    log "note: liburing is not installed, so tsdbd is built on epoll only (make NO_URING=1)."
    NOURING_MAKE=(NO_URING=1)
fi
git rev-parse --git-dir >/dev/null 2>&1 || die "run it from a git clone: the trees are made with git archive"
for r in "$REF_BEFORE" "$REF_MAIN" "$REF_AUTH"; do
    git rev-parse --verify --quiet "$r^{commit}" >/dev/null || die "the commit $r is not in this clone"
done
[ -z "$LOCK" ] || [ -e "$LOCK" ] || die "the lock file $LOCK does not exist"
CCV=$("$CC" --version 2>/dev/null | head -n 1)

# ------------------------------------------------------------------ scratch, results

mkdir -p "$(dirname "$SCRATCH")" 2>/dev/null
if [ ! -d "$SCRATCH" ]; then
    mkdir -p "$SCRATCH" || die "cannot create $SCRATCH"
    CREATED_SCRATCH=1
fi
WORK=$(mktemp -d "$SCRATCH/run.XXXXXX") || die "cannot make a directory in $SCRATCH"
if [ -z "$OUT" ]; then
    OUT=$ROOT/bench-results/auth-$(hostname -s 2>/dev/null || echo host)-$(date +%Y-%m-%d_%H%M)
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
: >"$RES/quiet.txt"

free_kb=$(df -Pk "$WORK" | awk 'NR == 2 { print $4 }')
[ "${free_kb:-0}" -ge $((NEED_GB * 1024 * 1024)) ] ||
    die "$SCRATCH has $((${free_kb:-0} / 1024 / 1024)) GB free; --$MODE wants $NEED_GB GB (use --dir on a bigger disk)"
fstype=$(stat -f -c %T "$WORK" 2>/dev/null || echo unknown)
case $fstype in
tmpfs | ramfs) warn "the scratch directory $SCRATCH is on $fstype (RAM): use --dir on a real disk." ;;
esac
mem_kb=$(awk '/^MemTotal:/ { print $2 }' /proc/meminfo 2>/dev/null)
ncpu=$(nproc 2>/dev/null || echo 1)

# ------------------------------------------------------------------ the quiet check

# CPUs busy over 2 s, in hundredths, from two reads of /proc/stat (not the load average: it lags a minute behind
# the run before).
busy() {
    local _ a1 b1 c1 d1 e1 f1 g1 h1 a2 b2 c2 d2 e2 f2 g2 h2 t i
    read -r _ a1 b1 c1 d1 e1 f1 g1 h1 _ </proc/stat
    sleep 2
    read -r _ a2 b2 c2 d2 e2 f2 g2 h2 _ </proc/stat
    t=$(((a2 + b2 + c2 + d2 + e2 + f2 + g2 + h2) - (a1 + b1 + c1 + d1 + e1 + f1 + g1 + h1)))
    i=$(((d2 + e2) - (d1 + e1)))
    [ "$t" -gt 0 ] || { echo 0; return; }
    echo $(((t - i) * ncpu * 100 / t))
}

governor() {
    cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null | sort | uniq -c |
        awk '{ printf "%s%s (%s CPUs)", s, $2, $1; s = ", " }'
}

machine_info() {
    local model flags f found="" gov threads cores sockets os
    meta date "$(date '+%Y-%m-%d %H:%M %Z')"
    meta mode "$MODE"
    meta reps "$REPS"
    meta seconds_per_workload "$SECS"
    meta host "$(hostname 2>/dev/null)"
    model=$(awk -F: '/^model name/ { sub(/^ +/, "", $2); print $2; exit }' /proc/cpuinfo 2>/dev/null)
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
    for f in avx2 bmi2 avx512f avx512bw avx512vl; do
        case " $flags " in *" $f "*) found="$found $f" ;; esac
    done
    meta cpu_flags "${found# }"
    meta memory "$((${mem_kb:-0} / 1024 / 1024)) GB"
    meta kernel "$(uname -sr)"
    os=$(. /etc/os-release 2>/dev/null && echo "${PRETTY_NAME:-}")
    meta os "${os:-unknown}"
    gov=$(governor)
    meta governor "${gov:-unknown (no cpufreq)}"
    case $gov in performance*) ;; *) [ -z "$gov" ] || warn "the CPU governor is \"$gov\", not performance: the clock moves during a run." ;; esac
    meta scratch "$SCRATCH ($(df -PT "$WORK" 2>/dev/null | awk 'NR == 2 { print $2 }'), $(df -Ph "$WORK" | awk 'NR == 2 { print $4 }') free)"
    meta compiler "$CCV"
    meta lock "${LOCK:-none: other jobs are not kept out of the timings}"
    meta tsdbd_flags "--wal-sync-ms 10 --port 0 --port-file (and --no-auth for noauth)"
    meta workload_sizes "$SERIES series x $POINTS points; $SECS s per workload; inserts: $ROWS rows a request over a pool of $POOL series; QUERY_MULTI: $MULTI series a request"
    local dirty
    dirty=$(git -C "$ROOT" status --porcelain tools docs server client include 2>/dev/null | wc -l)
    meta git_clone "$(git -C "$ROOT" rev-parse --short HEAD)$([ "$dirty" -gt 0 ] && echo " + $dirty modified/untracked files (dirty)")"
    [ "$dirty" -gt 0 ] && warn "the clone has $dirty modified or untracked files: the trees are built from commits, but the script and client are the working copies."
    if [ "$ncpu" -lt 8 ]; then
        warn "only $ncpu logical CPUs: 32 connections and tsdbd compete with each other for them."
    fi
    local b
    b=$(busy)
    meta busy_at_start "$((b / 100)).$(printf '%02d' $((b % 100))) of $ncpu CPUs over 2 s"
    [ "$b" -lt 150 ] || warn "the machine is not quiet: $((b / 100)).$(printf '%02d' $((b % 100))) CPUs busy over the last 2 s (the rule is fewer than 1.5)."
}

# ------------------------------------------------------------------ trees and builds

# tree NAME REF: git archive REF into $WORK/tree-NAME and build what is measured, from scratch.
TREES=(before main auth)
tree_ref() { case $1 in before) echo "$REF_BEFORE" ;; main) echo "$REF_MAIN" ;; auth) echo "$REF_AUTH" ;; esac; }
tree_b() { echo "$WORK/tree-$1/build-bench"; }

build_tree() {
    local t=$1 dir b mk=() v cflags zl le defs=""
    dir=$WORK/tree-$t
    b=$dir/build-bench
    mkdir -p "$dir"
    git archive "$(tree_ref "$t")" | tar -x -C "$dir" || die "git archive $(tree_ref "$t") failed"
    for v in $MAKE_VARS; do mk+=(-u "$v"); done
    log "building $t ($(tree_ref "$t")) from scratch: make -j$JOBS B=build-bench ..."
    (cd "$dir" && shared env "${mk[@]}" make -j"$JOBS" B=build-bench ${NOURING_MAKE[@]+"${NOURING_MAKE[@]}"} \
        build-bench/tsdbd build-bench/tsdb build-bench/tsdb-bench build-bench/tsdbc-bench build-bench/libtsdbclient.a) \
        >"$RAW/build-$t.log" 2>&1 || { tail -n 20 "$RAW/build-$t.log" >&2; die "the build of $t failed (log: $RAW/build-$t.log)"; }
    show() { (cd "$dir" && env "${mk[@]}" make -s B=build-bench --eval='show-%: ; @echo $($*)' "show-$1" 2>/dev/null | head -n 1); }
    cflags=$(show CFLAGS)
    case " $cflags " in
    *" -O2 "* | *" -O3 "*) ;;
    *) die "the compile flags of $t are not optimized (CFLAGS = $cflags)" ;;
    esac
    case $cflags in *-fsanitize* | *-O0* | *--coverage*) die "the compile flags of $t are for debugging (CFLAGS = $cflags)" ;; esac
    zl=$(show ZSTD_LIB)
    le=$(show LINK_EXTRA)
    # the one client of the new workloads, built against this tree's client library; -DAB_LOGIN and -DAB_ADMIN where it has them
    grep -q tsdbc_connect_opts "$dir/client/tsdb_client.h" && defs="-DAB_LOGIN"
    grep -q tsdbc_grant "$dir/client/tsdb_client.h" && defs="$defs -DAB_ADMIN"
    cp "$ROOT/tools/auth-bench-client.c" "$dir/tools/auth-bench-client.c"
    # shellcheck disable=SC2086
    (cd "$dir" && $CC $cflags $defs -o build-bench/auth-bench-client tools/auth-bench-client.c build-bench/libtsdbclient.a \
        $zl -lpthread -lm $le) >>"$RAW/build-$t.log" 2>&1 || { tail -n 20 "$RAW/build-$t.log" >&2; die "auth-bench-client did not build in $t"; }
    meta "tree_$t" "$(tree_ref "$t") = $(git rev-parse --short "$(tree_ref "$t")"): $("$b/tsdbd" --version 2>&1 | head -n 1); cflags: $cflags; client defs: ${defs:-none}"
    printf 'build\t%s\tmake B=build-bench (optimized, from git archive %s)\n' "$t" "$(tree_ref "$t")" >>"$RES/commands.txt"
}

# ------------------------------------------------------------------ configurations

CFGS=(main0 main1 noauth super prefix name5000 glob64)
cfg_tree() { case $1 in main0) echo before ;; main1) echo main ;; *) echo auth ;; esac; }
cfg_user() { case $1 in super) echo u-super ;; prefix) echo u-prefix ;; name5000) echo u-name ;; glob64) echo u-glob ;; esac; }
cfg_login() { [ -n "$(cfg_user "$1")" ]; }

# start_server CFG DIR: tsdbd for CFG on DIR/db, port from its port file, never a port computed here.
start_server() {
    local cfg=$1 d=$2 b port="" spid="" i
    local flags=(--wal-sync-ms 10)
    b=$(tree_b "$(cfg_tree "$cfg")")
    [ "$cfg" = noauth ] && flags+=(--no-auth)
    rm -f "$d/port"
    "$b/tsdbd" --dir "$d/db" --listen 127.0.0.1 --port 0 --port-file "$d/port" "${flags[@]}" >"$d/log" 2>&1 &
    SRV_PID=$!
    for ((i = 0; i < 600; i++)); do
        if ! kill -0 "$SRV_PID" 2>/dev/null; then
            log "tsdbd ($cfg) exited at start-up:"
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
        log "tsdbd ($cfg) did not write $d/port within 60 s"
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

selectors() {
    local kinds=(hdd_temp cpu_temp tank_bar tank_psi vibration flow_rate flow_total) i
    mkdir -p "$WORK/sel"
    printf '%s.\n' "${kinds[@]}" ins >"$WORK/sel/prefix.txt"
    {
        for ((i = 0; i < SERIES; i++)); do echo "${kinds[i % 7]}.$i"; done
        for ((i = 0; i < POOL; i++)); do echo "ins.$i"; done
        for ((i = SERIES + POOL; i < NAMES; i++)); do echo "other.$i"; done
    } >"$WORK/sel/name.txt"
    {
        for ((i = 0; i < GLOBS - 1; i++)); do printf 'site%02d/*/temp?\n' "$i"; done
        echo '*.?*' # the one that matches every series of the benchmark; tried last, after 63 that do not
    } >"$WORK/sel/glob.txt"
}

# setup_cfg CFG: the database of CFG, loaded and (for the users) granted, left in $WORK/db.CFG.
setup_cfg() {
    local cfg=$1 t b d user lg=()
    t=$(cfg_tree "$cfg")
    b=$(tree_b "$t")
    d=$WORK/setup.$cfg
    mkdir -p "$d"
    log "  loading $cfg ..."
    if cfg_login "$cfg"; then
        shared "$b/tsdb" "$d/db" user add bench-admin --superuser --password-file "$PW" >>"$RAW/setup.$cfg.txt" 2>&1 ||
            die "tsdb user add bench-admin failed (see $RAW/setup.$cfg.txt)"
        user=$(cfg_user "$cfg")
        if [ "$cfg" = super ]; then
            shared "$b/tsdb" "$d/db" user add "$user" --superuser --password-file "$PW" >>"$RAW/setup.$cfg.txt" 2>&1 || die "user add $user failed"
        else
            shared "$b/tsdb" "$d/db" user add "$user" --password-file "$PW" >>"$RAW/setup.$cfg.txt" 2>&1 || die "user add $user failed"
        fi
        lg=(--user bench-admin --password-file "$PW")
    fi
    start_server "$cfg" "$d" || die "tsdbd for $cfg did not start (log: $d/log)"
    shared "$b/tsdbc-bench" --port "$SRV_PORT" "${lg[@]+"${lg[@]}"}" --conns 4 load "$WORK/data.bin" >>"$RAW/setup.$cfg.txt" 2>&1 ||
        die "load into $cfg failed (see $RAW/setup.$cfg.txt)"
    shared "$b/tsdbc-bench" --port "$SRV_PORT" "${lg[@]+"${lg[@]}"}" optimize >>"$RAW/setup.$cfg.txt" 2>&1 || die "optimize of $cfg failed"
    case $cfg in
    prefix) shared "$b/auth-bench-client" --port "$SRV_PORT" "${lg[@]}" grant --grantee "$user" --kind prefix --rights 3 "$WORK/sel/prefix.txt" ;;
    name5000) shared "$b/auth-bench-client" --port "$SRV_PORT" "${lg[@]}" grant --grantee "$user" --kind name --rights 3 "$WORK/sel/name.txt" ;;
    glob64) shared "$b/auth-bench-client" --port "$SRV_PORT" "${lg[@]}" grant --grantee "$user" --kind glob --rights 3 "$WORK/sel/glob.txt" ;;
    *) true ;;
    esac >>"$RAW/setup.$cfg.txt" 2>&1 || die "the grants of $cfg failed (see $RAW/setup.$cfg.txt)"
    stop_server
    mkdir -p "$WORK/db.$cfg"
    mv "$d/db" "$WORK/db.$cfg/db"
    cp "$d/log" "$RAW/setup.$cfg.tsdbd.txt" 2>/dev/null
    rm -rf -- "$d"
    check_user "$cfg"
}

# check_user CFG: a limited user can do what the workloads do (create the insert pool, insert, read the loaded
# series), on a copy of the database, before hours of timing find out otherwise.
check_user() {
    local cfg=$1 b d user lg=()
    user=$(cfg_user "$cfg")
    [ -n "$user" ] || return 0
    b=$(tree_b "$(cfg_tree "$cfg")")
    d=$WORK/check.$cfg
    mkdir -p "$d"
    cp -a "$WORK/db.$cfg/db" "$d/db"
    start_server "$cfg" "$d" || die "tsdbd for $cfg did not start (log: $d/log)"
    lg=(--user "$user" --password-file "$PW")
    "$b/auth-bench-client" --port "$SRV_PORT" "${lg[@]}" --seconds 0.1 insert series >>"$RAW/setup.$cfg.txt" 2>&1 &&
        "$b/auth-bench-client" --port "$SRV_PORT" "${lg[@]}" --seconds 0.1 logins >>"$RAW/setup.$cfg.txt" 2>&1 &&
        "$b/tsdbc-bench" --port "$SRV_PORT" "${lg[@]}" --series "$SERIES" --points "$POINTS" --seconds 0.1 query >>"$RAW/setup.$cfg.txt" 2>&1 ||
        { stop_server; die "$user cannot do the workloads of $cfg (see $RAW/setup.$cfg.txt)"; }
    stop_server
    rm -rf -- "$d"
}

# ------------------------------------------------------------------ one measurement

# measure CFG WORKLOAD REP CMD...: the command's output to raw/CFG.WORKLOAD.REP.txt, status to status.txt. Under the
# lock (when there is one) and after a quiet check: up to 150 tries of 2 s; if the machine never quiets down the run
# goes ahead and quiet.txt says so.
measure_body() {
    local f=$1 b="" k gov
    shift
    for ((k = 0; k < 150; k++)); do
        b=$(busy)
        [ "$b" -lt 150 ] && break
        sleep 1
    done
    gov=$(governor)
    printf '# busy before: %d.%02d of %d CPUs; governor %s\n' $((b / 100)) $((b % 100)) "$ncpu" "$gov" >"$f"
    if [ "$b" -ge 150 ]; then
        printf '%s: not quiet, %d.%02d CPUs busy\n' "$f" $((b / 100)) $((b % 100)) >>"$RES/quiet.txt"
    fi
    case $gov in performance*) ;; *) printf '%s: governor %s\n' "$f" "$gov" >>"$RES/quiet.txt" ;; esac
    "$@" >>"$f" 2>&1
}

measure() {
    local cfg=$1 name=$2 rep=$3 rc f
    shift 3
    f=$RAW/$cfg.$name.$rep.txt
    log "  $cfg $name, run $rep of $REPS ..."
    if [ -n "$LOCK" ]; then
        (flock -x 9 && measure_body "$f" "$@") 9>>"$LOCK"
    else
        measure_body "$f" "$@"
    fi
    rc=$?
    printf '%s %s %s %s\n' "$cfg" "$name" "$rep" "$rc" >>"$RES/status.txt"
    if [ $rc -ne 0 ]; then
        FAILED=1
        log "  $cfg $name run $rep FAILED with status $rc (see $f)"
    fi
    if [ -n "$SRV_PID" ] && ! kill -0 "$SRV_PID" 2>/dev/null; then
        FAILED=1
        log "  tsdbd ($cfg) died during $name (log: $RAW/tsdbd-died.$cfg.$rep.txt)"
        cp "$WORK/run.$cfg/log" "$RAW/tsdbd-died.$cfg.$rep.txt" 2>/dev/null
        SRV_PID=
        return 1
    fi
    return 0
}

run_cfg() {
    local rep=$1 cfg=$2 t b d lg=() user port c
    t=$(cfg_tree "$cfg")
    b=$(tree_b "$t")
    d=$WORK/run.$cfg
    rm -rf -- "$d"
    mkdir -p "$d"
    shared cp -a "$WORK/db.$cfg/db" "$d/db" || { FAILED=1; return; }
    if ! start_server "$cfg" "$d"; then
        FAILED=1
        printf '%s server %s 1\n' "$cfg" "$rep" >>"$RES/status.txt"
        return
    fi
    port=$SRV_PORT
    user=$(cfg_user "$cfg")
    [ -n "$user" ] && lg=(--user "$user" --password-file "$PW")
    local tb=("$b/tsdbc-bench" --port "$port" "${lg[@]+"${lg[@]}"}" --series "$SERIES" --points "$POINTS" --seconds "$SECS")
    local ab=("$b/auth-bench-client" --port "$port" "${lg[@]+"${lg[@]}"}" --seconds "$SECS")
    for c in 1 $HI; do
        measure "$cfg" "query-c$c" "$rep" "${tb[@]}" --conns "$c" query || break
    done
    for c in 1 $HI; do
        measure "$cfg" "multi-c$c" "$rep" "${tb[@]}" --conns "$c" --multi "$MULTI" query || break
    done
    for c in 1 $HI; do
        measure "$cfg" "login-c$c" "$rep" "${ab[@]}" --conns "$c" logins || break
    done
    for c in 1 $HI; do
        measure "$cfg" "login-cold-c$c" "$rep" "${ab[@]}" --conns "$c" --cold logins || break
    done
    for c in 1 $HI; do
        measure "$cfg" "insert-series-c$c" "$rep" "${ab[@]}" --conns "$c" --rows "$ROWS" --pool "$POOL" insert series || break
    done
    for c in 1 $HI; do
        measure "$cfg" "insert-rows-c$c" "$rep" "${ab[@]}" --conns "$c" --rows "$ROWS" --pool "$POOL" insert rows || break
    done
    if [ -n "$SRV_PID" ]; then
        [ "$rep" = 1 ] && meta "server_$cfg" "$(grep -m1 -o 'simd [a-z0-9_]*.*io [a-z_]*' "$d/log")"
        stop_server
    fi
    [ "$rep" = 1 ] && cp "$d/log" "$RAW/tsdbd.$cfg.1.txt" 2>/dev/null
    rm -rf -- "$d"
}

# ------------------------------------------------------------------ main

for v in "${ENV_SET[@]+"${ENV_SET[@]}"}"; do
    warn "the environment variable ${v%%=*} is set (${v#*=}); it is ignored, so that the measured builds have the Makefile's own optimized flags."
done
machine_info
for t in "${TREES[@]}"; do build_tree "$t"; done
PW=$WORK/password
printf 'bench-password-0123456789\n' >"$PW"
chmod 600 "$PW"
selectors
log "making the rows ($SERIES series x $POINTS points) ..."
shared "$(tree_b auth)/tsdb-bench" --export "$WORK/data.bin" --series "$SERIES" --points "$POINTS" >"$RAW/export.txt" 2>&1 ||
    die "tsdb-bench --export failed (see $RAW/export.txt)"
log "setting up the seven databases ..."
for cfg in "${CFGS[@]}"; do setup_cfg "$cfg"; done
rm -f -- "$WORK/data.bin"

printf 'run\ttools/auth-bench.sh %s --reps %s --seconds %s (refs %s %s %s)\n' "$([ "$MODE" = quick ] && echo --quick)" "$REPS" "$SECS" \
    "$REF_BEFORE" "$REF_MAIN" "$REF_AUTH" >>"$RES/commands.txt"
for ((rep = 1; rep <= REPS; rep++)); do
    order=("${CFGS[@]}")
    if [ $((rep % 2)) = 0 ]; then # every second repetition goes the other way round
        order=()
        for ((i = ${#CFGS[@]} - 1; i >= 0; i--)); do order+=("${CFGS[i]}"); done
    fi
    for cfg in "${order[@]}"; do run_cfg "$rep" "$cfg"; done
done

{
    printf 'warnings=%d\n' "${#WARNINGS[@]}"
    for w in ${WARNINGS[@]+"${WARNINGS[@]}"}; do printf 'warning=%s\n' "$w"; done
} >>"$RES/meta.kv"
log "summarizing ..."
python3 -I "$ROOT/tools/auth-bench-summary.py" "$RES" || FAILED=1
log "results: $RES/summary.md"
[ "$FAILED" = 0 ] || { log "some runs FAILED: see $RES/status.txt"; exit 1; }
exit 0
