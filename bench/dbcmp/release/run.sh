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
P1_SYSTEMS=${P1_SYSTEMS:-"tsdb ch duck qdb qdbz ts i2 i3 prom iot iotz iotc iotx iotcx td tdz tdx vm"}
P2_SYSTEMS=${P2_SYSTEMS:-"tsdb ch qdb0 qdb1 qdb2 ts i2 i3 iot iotz td tdz vm"}
I3_P2_FLAGS=${I3_P2_FLAGS:-}            # smoke runs only: e.g. --force-snapshot-max-age 2m
I3_P2_MIN=${I3_P2_MIN:-600}
I3_P2_STABLE=${I3_P2_STABLE:-300} I3_P2_MAX=${I3_P2_MAX:-9000}     # Parquet unchanged for STABLE s; give up after MAX s
I3_P1_STABLE=${I3_P1_STABLE:-180} I3_P1_MAX=${I3_P1_MAX:-5400}
I3_P1_AGE=${I3_P1_AGE:-5m} I3_P1_MIN=${I3_P1_MIN:-420}
I2_EXTRA=${I2_EXTRA:-}                  # smoke runs only: e.g. -e INFLUXD_STORAGE_CACHE_SNAPSHOT_WRITE_COLD_DURATION=10s
I2_STABLE=${I2_STABLE:-300}                # i2_settle.sh: nothing changed this long (s) before the load counts as settled
WARM_SECS=${WARM_SECS:-3}                  # every window class is warmed up this long (s) on one client before it is timed
PROM_SETTLE_TIMEOUT=${PROM_SETTLE_TIMEOUT:-36000}
P5_SERIES=${P5_SERIES:-10000} P5_POINTS=${P5_POINTS:-10000000} P5_MINFREE=${P5_MINFREE:-320} P5_DIR=${P5_DIR:-/var/tmp} P5_LOAD_TIMEOUT=${P5_LOAD_TIMEOUT:-10800}
# the many-client counts: 32 clients (window classes, TWA windows) and 16 connections (per-series whole range); the
# smoke test runs 2 and 2 (NHI, NPER): it checks that every step works, and 32 clients of 100-series batches on its
# data only made it slow (and ran unlimited containers out of host memory)
NHI=${NHI:-32} NPER=${NPER:-16}
# MANY_CLIENTS=N (with CLASS_FILTER=b100): the extra-clients pass of `run.sh p6` (phase6many) for tsdb, QuestDB, ClickHouse
# and TimescaleDB: the batch classes whose name contains CLASS_FILTER are timed at N clients, REPS runs each, into
# p2/<sys>/p6/ as NAME.cN. Written for the 100-series batches (.b100) that QuestDB's, ClickHouse's (twa7d.b100: its
# memory limit) and TimescaleDB's memory could not carry at 32 clients: the systems are then compared at 16, and the
# 32-client rows stay what they were, failures with their reason (tables.py). ClickHouse also times the TWA batch
# classes (twa1d.b100, twa7d.b100); TimescaleDB's TWA needs the toolkit image and a load of its own: not in this pass
MANY_CLIENTS=${MANY_CLIENTS-} CLASS_FILTER=${CLASS_FILTER-}
class_wanted() { # CLASS: CLASS_FILTER (a substring such as b100) picks the classes whose name contains it; empty: all
    [ -z "${CLASS_FILTER-}" ] || case $1 in *"$CLASS_FILTER"*) true ;; *) false ;; esac
}
CONTAINERS="rb-ch rb-qdb rb-ts rb-tsha rb-i2 rb-i3 rb-prom rb-prom-bf rb-prom-sweep rb-promtool rb-chbig rb-iot rb-td rb-vm"
PORTS_RE=':(7070|8123|9000|9004|9005|9009|5434|9100|9103|8812|9109|8086|9999|8181|9090|9091|6667|18080|6041|8428)\b'
mkdir -p "$OUT"
# Every container without Podman's default limit of 2048 processes (threads count): TDengine with 16 vgroups and
# taosAdapter at 32 connections most likely ran into it in the 2026-10-28 smoke (EAGAIN, "Resource temporarily unavailable", on a
# 100-series raw batch). The limit is Podman's, not a server's setting, so no system gets it; ctr_mem records pids.peak.
printf '[containers]\npids_limit = 0\n' > "$OUT/containers.conf"
export CONTAINERS_CONF_OVERRIDE=$OUT/containers.conf
# Podman's systemd cgroup manager asks the user's systemd manager for a scope per container; when that manager is gone
# (the login session ended: "Connection refused" on the user bus) no container starts. A smoke run then uses cgroupfs,
# without a cgroup per container: no memory or pids peaks and no OOM records (ctr_mem says "no cgroup memory files").
# A release run refuses, since those records belong to its results.
if ! systemctl --user show-environment > /dev/null 2>&1; then
    case " $* " in *" smoke "*) SMOKE=1 ;; esac
    if [ "${SMOKE:-0}" != 1 ]; then
        echo "run.sh: no systemd user manager (systemctl --user fails); Podman cannot give containers their own" \
            "cgroups, so memory peaks and OOM kills would go unrecorded. Log in again (or loginctl enable-linger)." >&2
        exit 1
    fi
    printf '[engine]\ncgroup_manager = "cgroupfs"\n' >> "$OUT/containers.conf"
    NO_CGROUPS=1  # rootless cgroupfs cannot apply --memory either ("rootless needs no limits"): memlim drops it
    echo "$(date '+%F %T') no systemd user manager: smoke run with cgroup_manager cgroupfs (no memory/OOM records)" \
        >> "$OUT/driver.log"
fi

# One memory limit for every server, tsdbd included (SERVER_MEM, default 48g of the 64 GB machine, which leaves the rest
# to the system, the clients and the page cache of the files being read). relbench3 (2026-10-29) gave containers 24-28 GB
# (QuestDB and ClickHouse none) and tsdbd none: QuestDB took 57-59 GB twice and the kernel's global OOM killer ended it
# (and nearly the desktop), TimescaleDB hit its 24 GB cgroup limit under 32 clients of 100-series batches. Now a server
# that outgrows 48 GB is killed by its own cgroup, whatever it is, and that is a result (REFUSED.txt, tables).
export SERVER_MEM=${SERVER_MEM:-48g}
# A container's memory limit (and its swap, none). A smoke run on cgroupfs has no cgroup to put it in and starts the
# container without one; a release run always has it.
memlim() { [ "${NO_CGROUPS:-0}" = 1 ] || printf -- '--memory %s --memory-swap %s' "${1:-$SERVER_MEM}" "${1:-$SERVER_MEM}"; }
# tsdbd runs natively: the same limit as a transient systemd scope of the user manager (no swap). systemd-run --scope
# execs the command in the scope, so the pid stays the one tsdbd_start records.
tsdbd_memwrap() { [ "${NO_CGROUPS:-0}" = 1 ] || printf 'systemd-run --user --scope --quiet -p MemoryMax=%s -p MemorySwapMax=0' "${SERVER_MEM^^}"; }

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
# The memory of the servers during a timed step (OUT.mem, one line per server): the highest memory.current of its cgroup
# (page cache included: what the limit counts) and the highest anon + shmem + kernel of memory.stat (without the page
# cache, which the kernel takes back under pressure: what the server itself holds), sampled every 0.5 s. The servers
# are the running rb-* containers and tsdbd (TPID): a step does not say which one it drives, tables.py picks by system.
mem_cgroups() { # NAME CGDIR per line: every running rb-* container and tsdbd's scope
    local c p
    for c in $(podman ps --format '{{.Names}}' 2> /dev/null | grep '^rb-'); do
        p=$(podman inspect -f '{{.State.CgroupPath}}' "$c" 2> /dev/null)
        [ -n "$p" ] && [ -r "${CGROOT:-/sys/fs/cgroup}$p/memory.stat" ] && echo "$c ${CGROOT:-/sys/fs/cgroup}$p"
    done
    if [ -n "${TPID-}" ] && [ "$TPID" -gt 1 ] 2> /dev/null; then
        p=$(sed -n 's/^0:://p' /proc/"$TPID"/cgroup 2> /dev/null)
        [ -n "$p" ] && [ -r "${CGROOT:-/sys/fs/cgroup}$p/memory.stat" ] && echo "tsdbd ${CGROOT:-/sys/fs/cgroup}$p"
    fi
}
mem_sample() { # OUTFILE NAME CGDIR [NAME CGDIR...]: until killed, the peaks of each into OUTFILE (rewritten when one grows),
               # with the first sample as "start": a peak includes what earlier steps left; the rise is this step's
    local f=$1 i n cur res grew
    shift
    local -a name=() dir=() pc=() pr=() sc=() sr=()
    while [ $# -ge 2 ]; do name+=("$1"); dir+=("$2"); pc+=(0); pr+=(0); sc+=(""); sr+=(""); shift 2; done
    n=${#name[@]}
    [ "$n" -gt 0 ] || return 0
    while :; do
        grew=0
        for ((i = 0; i < n; i++)); do
            cur=$(cat "${dir[i]}/memory.current" 2> /dev/null) || continue
            res=$(awk '$1 == "anon" || $1 == "shmem" || $1 == "kernel" { s += $2 } END { print s + 0 }' "${dir[i]}/memory.stat" 2> /dev/null)
            [ -z "${sc[i]}" ] && { sc[i]=$cur; sr[i]=${res:-0}; }
            [ "$cur" -gt "${pc[i]}" ] 2> /dev/null && { pc[i]=$cur; grew=1; }
            [ "${res:-0}" -gt "${pr[i]}" ] 2> /dev/null && { pr[i]=$res; grew=1; }
        done
        if [ "$grew" = 1 ]; then
            for ((i = 0; i < n; i++)); do
                echo "${name[i]} peak ${pc[i]} nocache ${pr[i]} start ${sc[i]:-0} startnocache ${sr[i]:-0}"
            done > "$f.tmp" && mv "$f.tmp" "$f"
        fi
        sleep 0.5
    done
}
declare -A DEAD_CLASS   # step label without its run (" r3"): the server died in it twice, so its other runs are skipped
tstep() { # LABEL OUTFILE -- CMD...: exclusive lock, quiet gate, CMD with stdout+stderr to OUTFILE (fd 9 closed)
    # LABEL_PFX (the extra pass p6: "p6 "): in front of every label, so steps.log tells its steps from phase 2's
    local label=${LABEL_PFX-}$1 out=$2 s e rc key
    key=${label% r[0-9]*}
    shift 3
    # RESUME=1 (a rerun after a death or a harness fix): a step that already ended with rc 0 in steps.log is not run again;
    # loads always run (the data of the earlier run is gone). A rerun clears the earlier run's death marks of the step.
    if [ "${RESUME:-0}" = 1 ] && [ "${label#* load r}" = "$label" ] \
        && grep -qF "$(printf '\t%s\trc=0\t' "$label")" "$OUT/steps.log" 2> /dev/null && [ -s "$out" ]; then
        log "  RESUME: $label already measured, skipped"
        return 0
    fi
    rm -f "$out.oom" "$out.died" "$out.refused"
    if [ -n "${DEAD_CLASS[$key]-}" ]; then
        echo "$label: not run, the server died twice in this class (${DEAD_CLASS[$key]})" >> "$(dirname "$out")/REFUSED.txt"
        log "  DIED: $label skipped (the server died twice in $key)"
        return 1
    fi
    if [ "${SMOKE:-0}" = 1 ]; then steplock -s; else steplock -x; fi
    quiet "$label"
    local mp
    rm -f "$out.mem"
    mem_sample "$out.mem" $(mem_cgroups) 9>&- &
    mp=$!
    s=$(date +%s.%N)
    limited "$STEP_TIMEOUT" "$out" "$@"
    rc=$?
    e=$(date +%s.%N)
    [ "$mp" -gt 1 ] 2> /dev/null && { kill "$mp" 2> /dev/null; wait "$mp" 2> /dev/null; }
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
    if [ "$rc" != 0 ] && [ "$rc" != 124 ] && [ -n "${QCTR-}" ] && [ "${label#* load r}" = "$label" ] && ctr_died "$QCTR"; then
        # the server died in this step (the kernel killed it out of memory at its limit, or it stopped by itself): a
        # result like a refusal, recorded in REFUSED.txt (the death) and kept as OUT.oom / OUT.died. The server is started
        # again on the same data (QREADY: a command that succeeds once it serves) and the step run once more; if it dies
        # again, the class is over (its remaining runs are skipped: DEAD_CLASS)
        local ext=died why=$CTR_WHY
        [ "$CTR_OOM" = 1 ] && ext=oom
        mv "$out" "$out.$ext"
        echo "$label: the server died: $why$([ -n "${TSTEP_RETRY-}" ] && echo " (again, on the retry)")" >> "$(dirname "$out")/REFUSED.txt"
        log "  DIED: $label: $QCTR $why (see $out.$ext); restarting it"
        ctr_restart "$QCTR" || { fail "$label: $QCTR did not serve again after its death"; return "$rc"; }
        if [ -z "${TSTEP_RETRY-}" ]; then
            log "  retrying $label once"
            TSTEP_RETRY=1
            LABEL_PFX= tstep "$label" "$out" -- "$@"
            rc=$?
            TSTEP_RETRY=
        else
            DEAD_CLASS[$key]=$why
        fi
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
        2|3|4|6) for f in "$DBCMP"/input/mixed.{rowbinary,lp,ilp32,csv,om} "$WORK/spec.tsv" "$WORK/batches.tsv"; do
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
    if [ "$rc" != 0 ] && [ "$rc" != 124 ] && [ -z "${TSTEP_RETRY-}" ] && [ -n "${QCTR-}" ] && ctr_died "$QCTR"; then
        # the server died (or was found dead) before or during this check: recorded, started again, the check once more
        echo "$(basename "$out"): the server died: $CTR_WHY" >> "$(dirname "$out")/REFUSED.txt"
        log "  DIED: $(basename "$out"): $QCTR $CTR_WHY; restarting it and trying once more"
        cp "$out" "$out.died" 2> /dev/null
        if ctr_restart "$QCTR"; then
            TSTEP_RETRY=1
            untimed "$out" "$@"
            rc=$?
            TSTEP_RETRY=
            return "$rc"
        fi
        fail "$(basename "$out"): $QCTR did not serve again after its death"
        return 1
    fi
    if [ "$rc" = 3 ]; then  # rel.py / check.py: SERVER DOWN, not a difference of answers
        fail "$(basename "$out"): server down, nothing was checked ($(grep -m1 'SERVER DOWN' "$out" | cut -c1-150)) (see $out)"
        return 3
    fi
    # rel.py REFUSED (4): the server answered the query with an error of its own (QuestDB's RSS limit as an HTTP 400):
    # a result the caller records (nocheck: dash.txt, REFUSED.txt), not a harness failure
    if [ "$rc" = 4 ] && grep -q '^REFUSED: ' "$out"; then
        log "  $(basename "$out"): refused by the server: $(grep -m1 '^REFUSED: ' "$out" | cut -c10-160)"
        return 4
    fi
    # UNTIMED_SOFT: an answer check whose mismatch is a result to report (a stated bound missed), not a harness failure
    [ "$rc" = 1 ] && [ -n "${UNTIMED_SOFT-}" ] && grep -q MISMATCH "$out" && ! grep -q Traceback "$out" && return 2
    # before the exit code: a checker that exits 1 on a mismatch (rel.py) is named by its mismatch in FAILS
    case $(basename "$out") in check*) grep -qE 'mismatches [1-9]|MISMATCH|Traceback' "$out" && { fail "answer check: $(grep -m1 -E 'mismatches [1-9]|MISMATCH|Traceback' "$out" | cut -c1-150) ($out)"; return 1; } ;; esac
    [ "$rc" = 0 ] || { fail "$(basename "$out"): rc $rc$([ "$rc" = 124 ] && echo " (STEP TIMEOUT ${lim}s)") (see $out)"; return 1; }
    return 0
}
runstats() { python3 -I "$TOOLS/runstats.py" "$1" >> "$OUT/driver.log" 2>&1; }

# ------------------------------------------------------------------ processes and containers we own
TPID=
tsdbd_start() { # DIR [ARGS]: tsdbd on --port 0 --port-file; sets TPID and TSDB_PORT
    local d=$1 p pid i
    shift
    rm -f "$d.pf"
    # shellcheck disable=SC2046
    $(tsdbd_memwrap) ${TSDBD_WRAP:-} "$T/tsdbd" --dir "$d" --listen 127.0.0.1 --port 0 --port-file "$d.pf" "$@" > "$d.log" 2>&1 9>&- &
    TPID=$!
    echo "$TPID tsdbd $d" >> "$OUT/pids"
    for i in $(seq 600); do [ -s "$d.pf" ] && break; kill -0 "$TPID" 2>/dev/null || break; sleep 0.1; done
    read -r p pid _ < "$d.pf" 2>/dev/null || { fail "tsdbd did not start on $d (log $d.log)"; TPID=; return 1; }
    if [ "$pid" != "$TPID" ]; then fail "port file of $d names pid $pid, not our $TPID"; tsdbd_stop; return 1; fi
    export TSDB_PORT=$p TSDB_HOST=127.0.0.1
}
tsdbd_mem() { # PID FILE: the memory cgroup of tsdbd's scope: peak (page cache included), limit, OOM kills
    local p
    p=/sys/fs/cgroup$(sed -n 's/^0:://p' /proc/"$1"/cgroup 2> /dev/null)
    [ -r "$p/memory.peak" ] && echo "$(date '+%F %T') tsdbd pid $1 peak $(cat "$p/memory.peak") limit $(cat "$p/memory.max")" \
        "$(grep '^oom_kill ' "$p/memory.events")" >> "$2"
}
tsdbd_stop() {
    [ -n "$TPID" ] && [ "$TPID" -gt 1 ] 2>/dev/null && tsdbd_mem "$TPID" "$OUT/tsdbd-mem.txt"
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
# p5's directories follow P5_DIR (the smoke's is $DBCMP/p5): a pattern of /var/tmp/big alone left the smoke's tsdb and
# ClickHouse data in place, so the next smoke loaded on top of it and stopped at "Database scale already exists"
rmdata() {
    local d p5=
    case $P5_DIR in /var/tmp | /var/tmp/?*) p5=$P5_DIR ;; esac
    for d in "$@"; do
        case $d in
            "$DBCMP"/s[0-9]* | /var/tmp/big | /var/tmp/ch-big) podman unshare rm -rf "$d" ;;
            *) [ -n "$p5" ] && { [ "$d" = "$p5/big" ] || [ "$d" = "$p5/ch-big" ]; } && podman unshare rm -rf "$d" ;;
        esac
    done
    return 0
}
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
    podman run -d --name rb-ch --network host $(memlim) --ulimit nofile=262144:262144 -e CLICKHOUSE_SKIP_USER_SETUP=1 \
        -v "$W/data:/var/lib/clickhouse" -v "$W/listen.xml:/etc/clickhouse-server/config.d/zz-listen.xml" "$CH_IMG" > /dev/null 9>&- || { fail "clickhouse start"; return 1; }
    for i in $(seq 120); do curl -sf http://127.0.0.1:8123/ping > /dev/null && break; sleep 0.5; done
    curl -sf http://127.0.0.1:8123/ping > /dev/null || { fail "clickhouse not ready"; return 1; }
    [ -x "$CLICKHOUSE" ] || podman cp rb-ch:/usr/bin/clickhouse "$CLICKHOUSE"
}
CHC() { "$CLICKHOUSE" client --host 127.0.0.1 "$@"; }
# QuestDB converts partitions to Parquet with LZ4_RAW unless told otherwise; the ZSTD variant (qdbz in p1, qdb2 in p2)
# sets the codec and level (QuestDB 10.x PropServerConfiguration: codec default LZ4_RAW, ZSTD level default 9,
# levels 1-22; a partition compressing by less than min.compression.ratio 1.2 stays native)
# Level 22 converts about one partition per second whatever its size (smoke: 450 small partitions took > 300 s);
# qdb_wait_table's QDB_WAIT_MAX covers that.
QDB_ZSTD_LEVEL=${QDB_ZSTD_LEVEL:-22}
qdb_zstd_env() { echo "-e QDB_CAIRO_PARTITION_ENCODER_PARQUET_COMPRESSION_CODEC=ZSTD -e QDB_CAIRO_PARTITION_ENCODER_PARQUET_COMPRESSION_LEVEL=$QDB_ZSTD_LEVEL"; }
qdb_start() { # DATADIR; QDB_ZSTD=1: Parquet conversions with ZSTD; QDB_PC=1: every worker pool at 1 thread (per core)
    local W=$1 i pc=()
    mkdir -p "$W"
    # QuestDB sizes its pools at start, and its workers spin before they sleep: per core gets one thread per pool
    # (shared, query, network, write, WAL apply) and a JVM that sees one CPU (GC and compiler threads)
    [ "${QDB_PC:-0}" = 1 ] && pc=(-e QDB_SHARED_WORKER_COUNT=1 -e QDB_SHARED_QUERY_WORKER_COUNT=1
        -e QDB_SHARED_NETWORK_WORKER_COUNT=1 -e QDB_SHARED_WRITE_WORKER_COUNT=1 -e QDB_WAL_APPLY_WORKER_COUNT=1
        -e JVM_PREPEND=-XX:ActiveProcessorCount=1)
    # shellcheck disable=SC2046
    # the HTTP header buffer (default 64 KB) holds the URL: check.py sends a batch of 100 series as one GET /exec,
    # which QuestDB otherwise drops ("URL query string is too long"); the timed queries go over PostgreSQL wire.
    # Every SELECT of a batch's UNION ALL holds a table reader: 32 connections x 100 series need 3200, and the
    # default pool (cairo.reader.pool.max.segments 10 x 32 readers) refused them ("table busy [reason=pool size
    # exceeded]", 2026-10-28 smoke). 128 segments (4096 readers) is a limit, not a cache: readers open on demand.
    podman run -d --name rb-qdb --network host $(memlim) -v "$W:/var/lib/questdb" $([ "${QDB_ZSTD:-0}" = 1 ] && qdb_zstd_env) \
        -e QDB_HTTP_NET_BIND_TO=127.0.0.1:9100 -e QDB_HTTP_MIN_NET_BIND_TO=127.0.0.1:9103 \
        -e QDB_PG_NET_BIND_TO=127.0.0.1:8812 -e QDB_LINE_TCP_NET_BIND_TO=127.0.0.1:9109 \
        -e QDB_LINE_UDP_ENABLED=false -e QDB_TELEMETRY_ENABLED=false -e QDB_HTTP_REQUEST_HEADER_BUFFER_SIZE=1048576 \
        -e QDB_CAIRO_READER_POOL_MAX_SEGMENTS=128 \
        "${pc[@]}" "$QDB_IMG" > /dev/null 9>&- || { fail "questdb start"; return 1; }
    for i in $(seq 120); do curl -sf 'http://127.0.0.1:9100/exec?query=select%201' > /dev/null && return 0; sleep 0.5; done
    fail "questdb not ready"; return 1
}
QX() { curl -sG http://127.0.0.1:9100/exec --data-urlencode "query=$1"; echo; }
qdb_ready() { curl -sf 'http://127.0.0.1:9100/exec?query=select%201' > /dev/null; }
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
# REPORT §5.11: timescaledb-tune's work_mem (6 MB) spills TWA's window sorts to disk, so the timescaledb-ha server,
# which runs TWA and LTTB, gets 256 MB. The plain server keeps tune's value. Neither bounds a 100-series batch: 100
# UNION ALL branches over compressed chunks, 32 clients of 7-day batches grew to ~1.7 GB per backend at 256 MB and
# again at tune's 6 MB (2026-10-28 smokes, no memory limit under cgroupfs; the host's OOM killer ended both). So both
# servers get a memory limit (TS_MEM) like the other containers: an OOM kill is then a result in REFUSED.txt, and the
# steps after it wait until PostgreSQL serves again (QCTR rb-ts).
TS_WORK_MEM=${TS_WORK_MEM:-256MB} TS_MEM=${TS_MEM:-$SERVER_MEM}
# Podman gives a container 64 MB of /dev/shm; PostgreSQL's parallel workers share their hash tables there, and a
# batch of 100 series failed with "could not resize shared memory segment ... No space left on device". A server on a
# host has the host's /dev/shm (half the RAM); 8 GB is plenty here.
TS_SHM=${TS_SHM:-8g}
ts_start() { # NAME IMAGE DATADIR MOUNTPOINT: TimescaleDB on 127.0.0.1:5434 with $IN or $DBCMP/input on /data
    local name=$1 img=$2 W=$3 mnt=$4 i
    mkdir -p "$W"
    if [ "$name" = rb-tsha ]; then
        podman unshare chown 1000:1000 "$W"
        podman run -d --name "$name" --network host $(memlim "$TS_MEM") --shm-size=$TS_SHM -v "$W:/home/postgres/pgdata" -v "$mnt:/data:ro" -e POSTGRES_PASSWORD=bench \
            -e TS_TUNE_MEMORY=24GB -e TS_TUNE_NUM_CPUS=32 "$img" postgres -c listen_addresses=127.0.0.1 -c port=5434 -c work_mem=$TS_WORK_MEM > /dev/null 9>&- || { fail "$name start"; return 1; }
    else
        podman run -d --name "$name" --network host $(memlim "$TS_MEM") --shm-size=$TS_SHM -v "$W:/var/lib/postgresql/data" -v "$mnt:/data:ro" -e POSTGRES_PASSWORD=bench \
            -e TS_TUNE_MEMORY=24GB -e TS_TUNE_NUM_CPUS=32 "$img" postgres -c listen_addresses=127.0.0.1 -c port=5434 > /dev/null 9>&- || { fail "$name start"; return 1; }
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
    podman run -d --name rb-i2 --network host $(memlim) \
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
    podman run -d --name rb-i3 --network host $(memlim) -v "$W:/var/lib/influxdb3" "$I3_IMG" \
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
    podman run -d --name "$1" --network host $(memlim) -v "$3:/prometheus" -v "$DBCMP/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
        "$PROM_IMG" --config.file=/etc/prometheus/prometheus.yml --storage.tsdb.path=/prometheus --web.listen-address=127.0.0.1:$2 \
        --web.enable-remote-write-receiver --web.enable-admin-api --query.max-concurrency=32 > /dev/null 9>&-
}
prom_ready() { local i; for i in $(seq 36000); do curl -sf "127.0.0.1:$1/-/ready" > /dev/null && return 0; sleep 0.1; done; return 1; }

# ------------------------------------------------------------------ query suites (qdrive / tsdbq / clickhouse-benchmark)
FULLS_DEFAULT="0 3600 60"
CLASSES_DEFAULT="raw agg1d agg7d point"
qsuite() { # SYS PREFIX DIR DRIVER-ARGS...: whole range (warm-up, REPS runs of one query) and classes (3 s warm-up,
           # REPS x SECS at 1 and 32 clients); FULLS / CLASSES select ("" skips)
    local S=$1 P=$2 D=$3 n c t a
    shift 3
    for n in ${FULLS-$FULLS_DEFAULT}; do
        "$B/qdrive" "$@" "$WORK/q/$S.full$n.txt" 1 100000 1 > "$D/warm.$P.full$n" 2>&1 9>&- || { fail "$P full$n warm-up failed (see $D/warm.$P.full$n)"; continue; }
        reps "$P.full$n" "$D" -- "$B/qdrive" "$@" "$WORK/q/$S.full$n.txt" 1 100000 1
    done
    for c in ${CLASSES-$CLASSES_DEFAULT}; do
        "$B/qdrive" "$@" "$WORK/q/$S.$c.txt" 1 "$WARM_SECS" > "$D/warm.$P.$c" 2>&1 9>&- || { fail "$P $c warm-up failed (see $D/warm.$P.$c)"; continue; }
        for t in 1 $NHI; do
            a=("$@"); [ "$t" != 1 ] && [ -n "${MANY_CONN-}" ] && a=("$1" "$MANY_CONN")
            reps "$P.$c.c$t" "$D" -- "$B/qdrive" "${a[@]}" "$WORK/q/$S.$c.txt" "$t" "$SECS"
        done
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
TWA_KS=${TWA_KS-10 100}                # those windows K at a time, one request per batch (twabsuite, tsdbtwab)
dashnote() { # DIR LINE: LINE into DIR/dash.txt unless it is there (the extra pass p6 may run a system again)
    grep -qxF -- "$2" "$1/dash.txt" 2> /dev/null || echo "$2" >> "$1/dash.txt"
}
nocheck() { # SYSTEM KIND DIR RC: a check that did not pass: rc 3 is a server that cannot be reached, rc 4 one that
           # refused the query (rel.py REFUSED: the server's own message, a result in REFUSED.txt), a check past its
           # time limit (limited's "STEP TIMEOUT" in DIR/check-KIND.txt) a timeout; neither is "answers differ"
    if [ "$4" = 3 ]; then
        echo "$2: not timed, the server was down when it was checked (see check-$2.txt)" >> "$3/dash.txt"
        log "  $1 $2: not timed (server down)"
    elif [ "$4" = 4 ]; then
        dashnote "$3" "$2: not timed, the server refused the query: $(grep -m1 '^REFUSED: ' "$3/check-$2.txt" | cut -c10-300) (see check-$2.txt)"
        echo "check-$2: refused by the server: $(grep -m1 '^REFUSED: ' "$3/check-$2.txt" | cut -c10-300)" >> "$3/REFUSED.txt"
        log "  $1 $2: not timed (refused by the server)"
    elif grep -q 'STEP TIMEOUT' "$3/check-$2.txt" 2> /dev/null; then
        dashnote "$3" "$2: not timed, its answer check timed out after ${CHECK_TIMEOUT}s (see check-$2.txt)"
        log "  $1 $2: not timed (the check timed out)"
    else
        log "  $1 $2: not timed (answers differ)"
    fi
}
twasuite() { # SYS DIR DRIVER-ARGS...|ch: every TWA kind SYS has a query for (rel.py has), checked against tsdb's
             # answers in $WORK/ref first and timed only when they agree; a kind without a query is noted in DIR/dash.txt
    local S=$1 D=$2 k f r V
    V=${D%/memsweep}; V=$(basename "${V%/p6}")  # the variant (qdb-parquet, iot-zstd, ...): names and step labels; S is rel.py's
    shift 2
    cd "$WORK" || return 1
    # MANY_CLIENTS (the extra-clients pass): only the TWA batch classes CLASS_FILTER names, at that many clients (twabsuite),
    # after the single-window check of the kinds they need: no whole-range kinds, no single-window timings
    local kinds=$TWA_KINDS K any
    [ -n "${MANY_CLIENTS-}" ] && kinds=
    for k in $kinds; do
        REL has "$S" "$k" || { echo "$k: no query in rel.py for $S" >> "$D/dash.txt"; continue; }
        f=$D/q.$k.txt
        REL q "$S" "$k" > "$f"
        # InfluxDB 2's timeWeightedAvg sees only its window's rows (no reading carried in across a bucket edge, none
        # interpolated to it): a third of the hourly LINEAR buckets differ, so it does less work than tsdb's twal
        case "$S:$k" in i2flux:twal)
            echo "$k: InfluxDB 2's timeWeightedAvg interpolates only between the bucket's own rows, not across its edges (a third of the buckets differ from tsdb's LINEAR TWA); not timed" >> "$D/dash.txt"
            continue ;;
        esac
        # td: TDengine's TWA() (LINEAR) has tsdb's edges but accumulates with less precision (relative differences
        # 1e-9 to 5e-8 in most hourly buckets); in the 2026-10-28 smoke (10 x 650 k) 125 of 27,811 buckets differed by
        # up to 4.6e-4 relative (0.011 absolute), at the same hours in different series. It is checked against a stated
        # bound (TWA_TOL) and timed only within it: within, the bound and the largest difference go to DIR/loose.txt
        # (printed under the system's table); beyond, the kind is '-' with the check's summary in DIR/dash.txt, a
        # result like a refusal, not a harness failure
        case " ${TWA_LOOSE-td} " in
        *" $S "*)
            REL_TOL=${TWA_TOL:-1e-6,1e-6} UNTIMED_SOFT=1 untimed "$D/check-$k.txt" REL check "$S" "$k" "ref/$k.jsonl"
            case $? in
            0) ;;
            2) echo "$k: answers differ from tsdb's beyond ${TWA_TOL:-1e-6,1e-6} (relative, absolute), not timed: $(grep -m1 -E "^$k: [0-9]+ series" "$D/check-$k.txt" | cut -d: -f2- | tr -s ' ' | sed 's/^ //; s/ (bound.*//')" >> "$D/dash.txt"
               log "  $S $k: not timed (answers differ beyond ${TWA_TOL:-1e-6,1e-6}; dash.txt)"; continue ;;
            *) nocheck "$S" "$k" "$D" $?; continue ;;
            esac
            echo "$k: agrees with tsdb within ${TWA_TOL:-1e-6,1e-6} (relative, absolute) only, not 1e-9: $(grep -o 'largest difference.*' "$D/check-$k.txt" | head -1)" >> "$D/loose.txt";;
        *) untimed "$D/check-$k.txt" REL check "$S" "$k" "ref/$k.jsonl" || { nocheck "$S" "$k" "$D" $?; continue; };;
        esac
        if [ "$1" = ch ]; then
            chbench1 1 1 0 "$f" > /dev/null 2> "$D/warm.$k" 9>&-
            for r in $(seq "$REPS"); do tstep "$k r$r" "$D/$k.$r" -- chbench_tag "$k.$r.$$" 1 1 0 "$f"; done
        else
            qline "$k" "$D" "$f" "$@"
        fi
    done
    for k in $TWA_WKINDS; do
        if [ -n "${MANY_CLIENTS-}" ]; then   # no batch class of this kind wanted: its windows need no check
            any=0; for K in $TWA_KS; do class_wanted "$k.b$K" && any=1; done
            [ $any = 1 ] || continue
        fi
        REL has "$S" "$k" || { echo "$k: no query in rel.py for $S" >> "$D/dash.txt"; continue; }
        REL q "$S" "$k" > "q/$S.$k.txt"
        untimed "$D/check-$k.txt" REL check "$S" "$k" "ref/$k.jsonl" || { nocheck "$S" "$k" "$D" $?; continue; }
        [ -n "${MANY_CLIENTS-}" ] && continue
        if [ "$1" = ch ]; then FULLS="" CLASSES="$k" chsuite "$V" "$D" "$S"
        else FULLS="" CLASSES="$k" qsuite "$S" "$V" "$D" "$@"; fi
    done
    twabsuite "$S" "$D" "$@"
}
twab_dir() { # DIR: where the TWA batch classes go: DIR, or in the extra pass (DIR .../memsweep) the system's p6/
    case $1 in */memsweep) echo "${1%/memsweep}/p6" ;; *) echo "$1" ;; esac
}
twa_single_failed() { # CHECKFILE SYS KIND: prints why, and succeeds, when KIND's one-window-per-request check of this
                      # load did not pass (a batch of windows that one at a time disagree, or took an hour, is not sent)
    if [ ! -s "$1" ]; then echo "the single-window check of $3 did not run in this load"
    elif grep -q 'STEP TIMEOUT' "$1"; then echo "the single-window check of $3 timed out after ${CHECK_TIMEOUT}s"
    elif grep -qx "$2 $3 OK" "$1"; then return 1
    else echo "the single-window check of $3 did not pass (see $(basename "$1"))"; fi
    return 0
}
# The TWA windows in batches, as the aggregate classes have them (bsuite): one request of K windows (rel.py qb: the
# window queries as UNION ALL branches with a member column, each with its own WITH tables), checked member by member
# against tsdb's answers (rel.py checkb, 1e-9 relative) and timed only when they agree, at 1 and NHI clients for SECS.
# In the extra pass (p6, DIR .../memsweep) they go to the system's p6/ directory with XREPS runs: new results of
# another load, kept apart from phase 2's files (a rerun of p6 moves p6/ aside, so no stale run is averaged in).
# Names and labels carry the variant (the result directory's name), so qdb0/1/2, iot/iotz and td/tdz never share a
# step label (RESUME, DEAD_CLASS); a kind is sent in batches only when its single windows agreed in this load.
twabsuite() { # SYS DIR DRIVER-ARGS...|ch
    local S=$1 C=$2 D V k K f t a r why REPS=${XREPS:-$REPS} ths="1 $NHI"
    [ -n "${MANY_CLIENTS-}" ] && ths=$MANY_CLIENTS   # the extra-clients pass: that many clients, the CLASS_FILTER classes
    D=$(twab_dir "$C"); V=${C%/memsweep}; V=$(basename "${V%/p6}")  # C may already be p6/ (TWA_RETIME)
    mkdir -p "$D"
    shift 2
    cd "$WORK" || return 1
    for k in $TWA_WKINDS; do for K in $TWA_KS; do
        class_wanted "$k.b$K" || continue
        # a batch is whole groups of K windows: none with fewer windows (TWA_N below K)
        [ "$K" -le "${TWA_N:-1000}" ] || { log "  $V $k.b$K: only TWA_N=${TWA_N:-1000} windows, no batch of $K"; continue; }
        REL has "$S" "$k" || { dashnote "$D" "$k.b$K: no query in rel.py for $S"; continue; }
        why=$(twa_single_failed "$C/check-$k.txt" "$S" "$k") && { dashnote "$D" "$k.b$K: not run: $why"; continue; }
        f=$WORK/q/$S.$k.b$K.txt
        REL qb "$S" "$k" "$K" > "$f" && [ -s "$f" ] || { fail "rel.py qb $S $k $K (see $f)"; continue; }
        untimed "$D/check-$k.b$K.txt" REL checkb "$S" "$k" "$K" "ref/$k.jsonl" || { nocheck "$V" "$k.b$K" "$D" $?; continue; }
        if [ "$1" = ch ]; then
            chbench1 1 0 "$WARM_SECS" "$f" > /dev/null 2> "$D/warm.$V.$k.b$K" 9>&-
            for t in $ths; do for r in $(seq "$REPS"); do
                tstep "$V.$k.b$K.c$t r$r" "$D/$V.$k.b$K.c$t.$r" -- chbench_tag "$V.$k.b$K.c$t.$r.$$" "$t" 0 "$SECS" "$f"
            done; done
        else
            "$B/qdrive" "$@" "$f" 1 "$WARM_SECS" > "$D/warm.$V.$k.b$K" 2>&1 9>&- || { fail "$V $k.b$K warm-up failed (see $D/warm.$V.$k.b$K)"; continue; }
            for t in $ths; do
                a=("$@"); [ "$t" != 1 ] && [ -n "${MANY_CONN-}" ] && a=("$1" "$MANY_CONN")
                reps "$V.$k.b$K.c$t" "$D" -- "$B/qdrive" "${a[@]}" "$f" "$t" "$SECS"
            done
        fi
    done; done
}
tsdbtwa() { # DIR PREFIX CONNS...: relq for every TWA kind (windows at 1 and 32 connections), timed
    local D=$1 P=$2 k c
    shift 2
    for k in $TWA_KINDS; do for c in "$@"; do tstep "p2 tsdb $P$k c$c" "$D/$P$k.c$c.txt" -- "$B/relq" "$k" "$c" "$REPS"; done; done
    for k in $TWA_WKINDS; do for c in 1 $NHI; do
        tstep "p2 tsdb $P$k c$c" "$D/$P$k.c$c.txt" -- env TWA_SPEC="$WORK/spec.tsv" "$B/relq" "$k" "$c" "$REPS"
    done; done
    tsdbtwab "$D" "$P"
}
# tsdb's TWA through QUERY_MULTI (protocol 10), as the other systems answer it: the whole range of every series in ONE
# request on one connection (KIND.b100.c1: what one SQL query is in ClickHouse and the others; relq --batch 100), and
# the windows K at a time (KIND.bK.cT: relq qps, requests counted as qdrive counts them, at 1 and NHI connections).
# Each form's answers are checked against ref/ (relq --batch dump, rel.py jsonl) before it is timed; REPS runs of one
# timing each (runstats: KIND.b100.c1.mean.txt). In p6 (DIR .../memsweep) into tsdb's p6/ directory, XREPS runs.
tsdbtwab() { # DIR PREFIX
    local D P=$2 k K t w=$WORK/self REPS=${XREPS:-$REPS}
    D=$(twab_dir "$1")
    mkdir -p "$w" "$D"
    for k in twa $TWA_KINDS; do
        untimed "$D/check-$P$k.b100.txt" bash -c "'$B/relq' $k 1 1 '$w/$P$k.b100.jsonl' --batch 100 && python3 -I '$TOOLS/rel.py' jsonl $k '$WORK/ref/$k.jsonl' '$w/$P$k.b100.jsonl'" || continue
        reps "$P$k.b100.c1" "$D" -- "$B/relq" "$k" 1 1 --batch 100
    done
    for k in $TWA_WKINDS; do for K in $TWA_KS; do
        [ "$K" -le "${TWA_N:-1000}" ] || { log "  tsdb $P$k.b$K: only TWA_N=${TWA_N:-1000} windows, no batch of $K"; continue; }
        untimed "$D/check-$P$k.b$K.txt" bash -c "TWA_SPEC='$WORK/spec.tsv' '$B/relq' $k 1 1 '$w/$P$k.b$K.jsonl' --batch $K && python3 -I '$TOOLS/rel.py' jsonl $k '$WORK/ref/$k.jsonl' '$w/$P$k.b$K.jsonl'" || continue
        limited "$STEP_TIMEOUT" "$D/warm.$P$k.b$K" env TWA_SPEC="$WORK/spec.tsv" "$B/relq" qps "$k.b$K" 1 "$WARM_SECS" 9>&- \
            || { fail "tsdb $P$k.b$K warm-up failed (see $D/warm.$P$k.b$K)"; continue; }
        for t in 1 $NHI; do reps "$P$k.b$K.c$t" "$D" -- env TWA_SPEC="$WORK/spec.tsv" "$B/relq" qps "$k.b$K" "$t" "$SECS"; done
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
    # qdrive starts its summary line with the file name: print the query file's, not the temporary copy's, or every
    # run's line differs and runstats.py cannot average the runs (relbench3 reported run 1 as the mean)
    { if [ "$i" != 0 ]; then QDRIVE_ERR_SUB='DB::Exception' "$BIN/qdrive" http 127.0.0.1 8123 '' "$h" 1 100000 "$i"
    else QDRIVE_ERR_SUB='DB::Exception' "$BIN/qdrive" http 127.0.0.1 8123 '' "$h" "$c" "$s"; fi; } | sed "s|^$h |$f |"
    rc=${PIPESTATUS[0]}
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
            sed "s/@TAG@/$P.full$n.per.c$NPER.$r.$$/" "$WORK/q/$S.full$n.per.http" > "$D/per.http"
            tstep "$P.full$n.per.c$NPER r$r" "$D/$P.full$n.per.c$NPER.$r" -- bash -c "'$B/qdrive' http 127.0.0.1 8123 '' '$D/per.http' $NPER 100000 0 && chqlog '$P.full$n.per.c$NPER.$r.$$'"
        done
    done
    for c in ${CLASSES-$CLASSES_DEFAULT}; do
        chbench1 1 0 "$WARM_SECS" "$WORK/q/$S.$c.txt" > /dev/null 2> "$D/warm.$P.$c" 9>&-
        for t in 1 $NHI; do for r in $(seq "$REPS"); do
            tstep "$P.$c.c$t r$r" "$D/$P.$c.c$t.$r" -- chbench_tag "$P.$c.c$t.$r.$$" "$t" 0 "$SECS" "$WORK/q/$S.$c.txt"
        done; done
    done
}
tsdbsuite() { # PREFIX DIR CLASS-THREADS("1 32"): tsdbq qps classes against $TSDB_PORT
    local P=$1 D=$2 ths=$3 c t
    for c in ${CLASSES-$CLASSES_DEFAULT}; do
        # a warm-up has the step time limit too (a tsdbd that never answers must not hold the run)
        limited "$STEP_TIMEOUT" "$D/warm.$P.$c" "$TB/tsdbq" qps "$WORK/spec.tsv" "$c" 1 "$WARM_SECS" 9>&-
        [ $? = 124 ] && { fail "$P $c warm-up: STEP TIMEOUT ${STEP_TIMEOUT}s (see $D/warm.$P.$c)"; continue; }
        for t in $ths; do reps "$P.$c.c$t" "$D" -- "$TB/tsdbq" qps "$WORK/spec.tsv" "$c" "$t" "$SECS"; done
    done
}
REL() { python3 -I "$TOOLS/rel.py" "$@"; }
lttb_loose() { # CHECKFILE DIR: LTTB answers that rel.py accepted as equal-area alternatives go to DIR/loose.txt
    awk -v f="$(basename "$1")" '/series answered with equal-area alternatives [1-9]/ {n = $NF; next}
        n && / lttb (OK|MISMATCH)$/ {printf "%s: LTTB of %d series differs from tsdb'"'"'s through ties of the triangle area " \
            "(the pick of a tie decided by rounding, the picks after it by that anchor; verified on the rows in exact " \
            "arithmetic within 1e-12, see %s)\n", $1, n, f; n = 0}' "$1" >> "$2/loose.txt"
}

# ------------------------------------------------------------------ batches (batches.tsv, mkbatch.py) and one core
# A batch is one request of K series (K = 1, 10, 100; mkbatch.py says how each language does it). Whole-range
# batch files are run whole, one client, REPS times: "N queries in X s" is the total time of the set; window
# classes run their batch file at 1 and 32 clients for SECS. PC=1 is the per-core run: the server pinned to
# PERCORE_CPU (containers by ctr_taskset, tsdbd by taskset plus --query-threads 1), the engine's
# own thread knob where it has one, one client pinned to PERCORE_CLIENT_CPU; names get ".pc".
KS=${KS:-"1 10 100"}
PC_KS=${PC_KS:-$KS}                        # the batch sizes of the per-core passes (smoke: 1 only)
BCLASSES=${BCLASSES-$CLASSES_DEFAULT}
PERCORE_CPU=${PERCORE_CPU:-2} PERCORE_CLIENT_CPU=${PERCORE_CLIENT_CPU:-3}
pc_client() { # CMD...: on PERCORE_CLIENT_CPU in a per-core run (shell functions too: those used are exported)
    if [ "${PC:-0}" = 1 ]; then taskset -c "$PERCORE_CLIENT_CPU" bash -c '"$@"' pc_client "$@"; else "$@"; fi
}
ctr_mem() { # CONTAINER FILE: the container's cgroup memory (peak, limit, OOM kills so far) appended to FILE, while it runs
    local p
    p=/sys/fs/cgroup$(podman inspect -f '{{.State.CgroupPath}}' "$1" 2> /dev/null)
    [ -r "$p/memory.peak" ] || { echo "$1: no cgroup memory files (not running?)" >> "$2"; return 1; }
    echo "$1 peak $(cat "$p/memory.peak") limit $(cat "$p/memory.max") $(grep '^oom_kill ' "$p/memory.events")" \
        "pids peak $(cat "$p/pids.peak" 2> /dev/null) limit $(cat "$p/pids.max" 2> /dev/null) $(grep '^max ' "$p/pids.events" 2> /dev/null)" >> "$2"
}
oom_killed() { # CONTAINER: 0 when the kernel OOM-killed a process in its cgroup: it no longer runs, or it still runs
    # and its cgroup's oom_kill count grew since the last call (PostgreSQL's postmaster outlives a killed backend and
    # restarts the others itself, in recovery for a moment). The count seen is kept per container id: a container made
    # again under the same name has a new cgroup counting from 0, and an old count of 15 hid its first OOM kills
    # (2026-11-01: TimescaleDB's later steps then ran against a server in recovery and all failed at once)
    local st id p n f
    st=$(podman inspect -f '{{.State.Status}}' "$1" 2> /dev/null)
    case $st in '') return 1 ;; running)
        p=${CGROOT:-/sys/fs/cgroup}$(podman inspect -f '{{.State.CgroupPath}}' "$1" 2> /dev/null)
        n=$(awk '$1 == "oom_kill" { print $2 }' "$p/memory.events" 2> /dev/null)
        f=$OUT/.oomseen.$1.$(podman inspect -f '{{.Id}}' "$1" 2> /dev/null)
        [ -n "$n" ] && [ "$n" -gt "$(cat "$f" 2> /dev/null || echo 0)" ] || return 1
        echo "$n" > "$f"
        return 0 ;;
    esac
    id=$(podman inspect -f '{{.Id}}' "$1" 2> /dev/null)
    [ -n "$id" ] && journalctl -k --since "-6h" 2> /dev/null | grep -qE "oom-kill:.*libpod-$id\.scope"
}
ctr_died() { # CONTAINER: 0 when the server died: the kernel OOM-killed a process in it, or the container no longer runs.
    # Sets CTR_WHY (what to record) and CTR_OOM (1 when out of memory). An empty name (never started) is not a death.
    local st lim why
    CTR_WHY= CTR_OOM=0
    lim=$(podman inspect -f '{{.HostConfig.Memory}}' "$1" 2> /dev/null | awk '$1 > 0 { printf "%.0f GB", $1 / 2^30 } $1 <= 0 { print "none" }')
    st=$(podman inspect -f '{{.State.Status}} exit code {{.State.ExitCode}}' "$1" 2> /dev/null)
    [ -n "$st" ] || return 1
    if oom_killed "$1"; then
        CTR_OOM=1
        CTR_WHY="killed out of memory by the kernel (container limit $lim; $st)"
        return 0
    fi
    case $st in running*) return 1 ;; esac
    CTR_WHY="stopped by itself ($st; container limit $lim)"
    return 0
}
ctr_restart() { # CONTAINER: start it again (same settings and data), then wait up to 300 s for QREADY
    local i
    steplock -s
    # a container that still runs (oom_killed) only waits; a new start makes a new cgroup, whose count starts at 0
    [ "$(podman inspect -f '{{.State.Status}}' "$1" 2> /dev/null)" = running ] || { rm -f "$OUT/.oomseen.$1".*; podman start "$1" > /dev/null 2>&1 9>&-; }
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
# NO_PC=1 (the extra pass p6): no per-core runs; every per-core start and pinning fails quietly, so the "... &&
# PC=1 ..." chains skip them, and the batch suites return at once for PC=1
want_pc() { [ "${NO_PC:-0}" != 1 ]; }
pc_pin() { # CONTAINER...: to PERCORE_CPU
    local c
    want_pc || return 1
    for c; do ctr_taskset "$PERCORE_CPU" "$c" || { fail "per-core: taskset $c"; return 1; }; done
}
how_write() { # PHASE SYS: how.json of the system's results (how.py: settings, durability, clients, per core, '-')
    env CH_IMG="$CH_IMG" TS_IMG="$TS_IMG" QDB_IMG="$QDB_IMG" I2_IMG="$I2_IMG" I3_IMG="$I3_IMG" IOT_IMG="$IOT_IMG" \
        TD_IMG="$TD_IMG" VM_IMG="$VM_IMG" PROM_IMG="$PROM_IMG" T="$T" TSDB_TREE="$TSDB_TREE" DUCKDB_DIR="$DUCKDB_DIR" \
        DUCKDB_SHA="$DUCKDB_SHA" CHDB_DIR="$CHDB_DIR" CHDB_SHA="$CHDB_SHA" REPS="$REPS" SECS="$SECS" KS="$KS" PC_KS="$PC_KS" WARM_SECS="$WARM_SECS" \
        SMOKE="${SMOKE:-0}" PERCORE_CPU="$PERCORE_CPU" PERCORE_CLIENT_CPU="$PERCORE_CLIENT_CPU" \
        python3 -I "$RDIR/how.py" write "$1" "$2" "$OUT" >> "$OUT/driver.log" 2>&1 || fail "how.json of $2 (phase $1)"
}
pc_unpin() { local c; want_pc || return 0; for c; do ctr_taskset "0-$(( $(nproc) - 1 ))" "$c"; done; }
pc_restart() { # CONTAINER CONFIG_FILE START_CMD...: the per-core start: runtimes size their thread pools at start, so
    # pinning a running server leaves it 32-wide pools on one CPU (REVIEW-fairness M7). START_CMD starts the container
    # with its thread knobs at 1; the container's configuration and pinning go to CONFIG_FILE.
    local c=$1 f=$2
    want_pc || return 1
    shift 2
    crm "$c"
    "$@" && pc_pin "$c" || { fail "per-core start of $c"; return 1; }
    { podman inspect "$c" --format '{{json .Config.Cmd}} env {{json .Config.Env}}'
      echo "pinned to CPU $PERCORE_CPU (taskset, whole container tree): $(taskset -cp "$(podman inspect -f '{{.State.Pid}}' "$c")" 2>&1)"
      echo "threads: $(podman exec "$c" sh -c 'ls /proc/[0-9]*/task 2> /dev/null | grep -c "^[0-9]"' 2>&1)"; } > "$f" 2>&1
}
# MANY_CONN, when set, replaces the connection string (the second driver arg, "DRIVER CONN") in the classes with more
# than one client: TimescaleDB's 32-client classes run with fewer parallel workers per query (ts_many_probe)
bsuite() { # SYS PREFIX DIR DRIVER-ARGS...: the batches of SYS through qdrive
    local S=$1 P=$2 D=$3 n k c t f nl sfx= ths=${BTHREADS:-1 $NHI} a fulls=${FULLS-$FULLS_DEFAULT}
    shift 3
    [ "${PC:-0}" = 1 ] && { want_pc || return 0; sfx=.pc; ths=1; }
    # MANY_CLIENTS: only the classes CLASS_FILTER names, at that many clients (the whole-range runs have one client:
    # none), XREPS runs each; the class name carries .cN like any other count, so nothing here is a new kind of file
    local REPS=$REPS
    [ -n "${MANY_CLIENTS-}" ] && { ths=$MANY_CLIENTS; fulls=; REPS=${XREPS:-$REPS}; }
    for k in $( [ "${PC:-0}" = 1 ] && echo "$PC_KS" || echo "$KS" ); do
        for n in $fulls; do
            class_wanted "full$n.b$k" || continue
            f=$WORK/q/$S.full$n.b$k.txt; nl=$(wc -l < "$f")
            pc_client "$B/qdrive" "$@" "$f" 1 100000 "$nl" > "$D/warm.$P.full$n.b$k$sfx" 2>&1 9>&- || { fail "$P full$n.b$k warm-up failed (see $D/warm.$P.full$n.b$k$sfx)"; continue; }
            reps "$P.full$n.b$k$sfx" "$D" -- pc_client "$B/qdrive" "$@" "$f" 1 100000 "$nl"
        done
        for c in $BCLASSES; do
            class_wanted "$c.b$k" || continue
            f=$WORK/q/$S.$c.b$k.txt
            pc_client "$B/qdrive" "$@" "$f" 1 "$WARM_SECS" > "$D/warm.$P.$c.b$k$sfx" 2>&1 9>&- || { fail "$P $c.b$k warm-up failed (see $D/warm.$P.$c.b$k$sfx)"; continue; }
            for t in $ths; do
                a=("$@"); [ "$t" != 1 ] && [ -n "${MANY_CONN-}" ] && a=("$1" "$MANY_CONN")
                reps "$P.$c.b$k.c$t$sfx" "$D" -- pc_client "$B/qdrive" "${a[@]}" "$f" "$t" "$SECS"
            done
        done
    done
}
perc16() { # SYS PREFIX DIR DRIVER-ARGS...: the whole range as 100 per-series queries on 16 connections (after bcheck
            # wrote q/SYS.fullN.per.txt), each system's fastest correct form of it, as for tsdb and ClickHouse
    local S=$1 P=$2 D=$3 n
    shift 3
    for n in ${FULLS-$FULLS_DEFAULT}; do reps "$P.full$n.per.c$NPER" "$D" -- "$B/qdrive" "$@" "$WORK/q/$S.full$n.per.txt" "$NPER" 100000 0; done
    return 0   # a failed run is in FAILS already; the batches still run
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
    local P=$1 D=$2 k c t sfx= ths=${BTHREADS:-1 $NHI} n
    [ "${PC:-0}" = 1 ] && { want_pc || return 0; sfx=.pc; ths=1; }
    # MANY_CLIENTS (as in bsuite): the CLASS_FILTER classes at that many clients, XREPS runs, no whole-range runs
    local REPS=$REPS
    [ -n "${MANY_CLIENTS-}" ] && { ths=$MANY_CLIENTS; REPS=${XREPS:-$REPS}; }
    # a release run must time tsdb's batches: without protocol 10's --batch the phase fails (smoke only logs it)
    tsdb_has_batch || { [ "${SMOKE:-0}" = 1 ] && log "  $P: tsdbq in $TB has no --batch: K > 1 not timed" \
        || fail "$P: tsdbq in $TB has no --batch (TSDB_TREE must be the multi-series build): K > 1 not timed"; }
    for k in $( [ "${PC:-0}" = 1 ] && echo "$PC_KS" || echo "$KS" ); do
        [ "$k" = 1 ] || tsdb_has_batch || continue
        if [ -z "${MANY_CLIENTS-}" ] && class_wanted "full.b$k"; then
            if [ "$k" = 1 ]; then
                reps "$P.full.b1$sfx" "$D" -- pc_client env NSERIES="$(python3 -I "$H/batches.py" --count-full1)" "$TB/tsdbq" full 1 1
            else
                reps "$P.full.b$k$sfx" "$D" -- pc_client "$TB/tsdbq" full 1 1 --batch "$k"
            fi
        fi
        for c in $BCLASSES; do
            n=$c; [ "$k" = 1 ] || n=$c.b$k
            class_wanted "$n" || continue
            limited "$STEP_TIMEOUT" "$D/warm.$P.$n$sfx" pc_client "$TB/tsdbq" qps "$WORK/spec.tsv" "$n" 1 "$WARM_SECS" 9>&-
            [ $? = 124 ] && { fail "$P $n$sfx warm-up: STEP TIMEOUT ${STEP_TIMEOUT}s (see $D/warm.$P.$n$sfx)"; continue; }
            for t in $ths; do reps "$P.$n.c$t$sfx" "$D" -- pc_client "$TB/tsdbq" qps "$WORK/spec.tsv" "$n" "$t" "$SECS"; done
        done
    done
}
tsdb_self() { # TAG: tsdbd's answers now against its references (cwd WORK): the protocol-10 batches of the whole range,
               # and with TAG merged, after optimize, every class, LTTB and TWA kind too. The headline numbers are
               # these paths, so they are checked like every other system's.
    local G=$1 k w=self rc=0
    mkdir -p "$w"
    if [ "$G" = merged ]; then
        # rel.py jsonl exits 1 on a mismatch: rc=1, and untimed (check-self*) names the MISMATCH line in FAILS
        { "$TB/tsdbq" dump spec.tsv 200 > "$w/$G-windows.jsonl" && "$TB/tsdbq" full 1 1 dump 2> "$w/$G-full.jsonl"; } \
            || { echo "MISMATCH: tsdbq dump failed"; rc=1; }
        python3 -I "$H/check.py" --dumps ref/windows.jsonl ref/full.jsonl "$w/$G-windows.jsonl" "$w/$G-full.jsonl" || rc=1
        "$B/relq" lttb 1 1 "$w/$G-lttb.jsonl" > /dev/null && REL jsonl lttb ref/lttb.jsonl "$w/$G-lttb.jsonl" || rc=1
        "$B/relq" twa 1 1 "$w/$G-twa.jsonl" > /dev/null && REL jsonl twa ref/twa.jsonl "$w/$G-twa.jsonl" || rc=1
        for k in $TWA_KINDS $TWA_WKINDS; do
            env TWA_SPEC="$WORK/spec.tsv" "$B/relq" "$k" 1 1 "$w/$G-$k.jsonl" > /dev/null \
                && REL jsonl "$k" "ref/$k.jsonl" "$w/$G-$k.jsonl" || rc=1
        done
    fi
    if tsdb_has_batch; then
        for k in $KS; do
            [ "$k" = 1 ] && continue
            "$TB/tsdbq" full 1 1 dump --batch "$k" 2> "$w/$G-full.b$k.jsonl" > /dev/null || { echo "MISMATCH: --batch $k dump failed"; rc=1; }
            echo "whole range, --batch $k:"
            python3 -I "$H/check.py" --dumps ref/windows.jsonl ref/full.jsonl ref/windows.jsonl "$w/$G-full.b$k.jsonl" || rc=1
        done
    else
        echo "tsdbq has no --batch: batch answers not checked (and not timed)"
    fi
    return "$rc"
}
tsdbd_percore() { # DIR: restart tsdbd on PERCORE_CPU with one query thread (where tsdbd has --query-threads)
    local qt=()
    want_pc || return 1
    "$T/tsdbd" --help 2>&1 | grep -q -- '--query-threads' && qt=(--query-threads 1)
    [ ${#qt[@]} = 0 ] && { [ "${SMOKE:-0}" = 1 ] && log "  tsdbd has no --query-threads: per core with all threads" \
        || { fail "tsdbd in $T has no --query-threads: per core not measured"; return 1; }; }
    tsdbd_stop
    TSDBD_WRAP="taskset -c $PERCORE_CPU" tsdbd_start "$1" "${qt[@]}"
}
chbsuite() { # PREFIX DIR TABLE-SYS(ch): the batches through chbench1 (PC=1: max_threads=1, pinned)
    local P=$1 D=$2 S=$3 n k c t f nl sfx= ths=${BTHREADS:-1 $NHI} set=() r fulls=${FULLS-$FULLS_DEFAULT}
    [ "${PC:-0}" = 1 ] && { want_pc || return 0; sfx=.pc; ths=1; set=(--max_threads=1); }
    # MANY_CLIENTS (as in bsuite): only the CLASS_FILTER classes, at that many clients, XREPS runs, no whole-range runs
    local REPS=$REPS
    [ -n "${MANY_CLIENTS-}" ] && { ths=$MANY_CLIENTS; fulls=; REPS=${XREPS:-$REPS}; }
    for k in $( [ "${PC:-0}" = 1 ] && echo "$PC_KS" || echo "$KS" ); do
        for n in $fulls; do
            f=$WORK/q/$S.full$n.b$k.txt; nl=$(wc -l < "$f")
            pc_client chbench1 1 "$nl" 0 "$f" > /dev/null 2> "$D/warm.$P.full$n.b$k$sfx" 9>&-
            for r in $(seq "$REPS"); do
                tstep "$P.full$n.b$k$sfx r$r" "$D/$P.full$n.b$k$sfx.$r" -- pc_client chbench_tag "$P.full$n.b$k$sfx.$r.$$" 1 "$nl" 0 "$f" "${set[@]}"
            done
        done
        for c in $BCLASSES; do
            class_wanted "$c.b$k" || continue
            f=$WORK/q/$S.$c.b$k.txt
            pc_client chbench1 1 0 "$WARM_SECS" "$f" > /dev/null 2> "$D/warm.$P.$c.b$k$sfx" 9>&-
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
        # ClickHouse's smallest lossless setting: the same codecs with ZSTD's highest level, 22
        CHC -q "CREATE TABLE s1.z_$ds (series UInt32, ts DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(22)), value Float32 CODEC(Gorilla, ZSTD(22))) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY (series, ts)" || fail "p1 ch create z_$ds"
        CHC -q "INSERT INTO s1.z_$ds FORMAT RowBinary" < "$IN/$ds.rowbinary" || fail "p1 ch insert z_$ds"
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
    printf 'ds\trows\tbytes\tnote\n' > "$OUT/p1/ch-zstd22.tsv"
    awk -F'\t' '$1 ~ /^z_/ {sub(/^z_/, "", $1); printf "%s\t%s\t%s\tDoubleDelta/Gorilla + ZSTD(22), after OPTIMIZE FINAL: %s parts\n", $1, $2, $3, $4}' "$D/optimized.tsv" >> "$OUT/p1/ch-zstd22.tsv"
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
    I2_PORT=8086 STABLE="$I2_STABLE" "$H/i2_settle.sh" "$(date +%s)" > "$D/settle.txt" 2>&1 9>&- || fail "p1 i2 compaction did not settle (see $D/settle.txt); sizes taken anyway"
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
        aside 1 "$s"
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
    local D=$OUT/p2/tsdb${SWEEP_SUB-} W=$DBCMP/s2/tsdb r
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
    if [ -n "${MANY_CLIENTS-}" ]; then  # the extra-clients pass (phase6many): this load, the batches' answer check, the CLASS_FILTER classes
        untimed "$D/check-self.txt" tsdb_self "" && tsdbbsuite tsdbq "$D"
        tsdbd_stop
        cd "$OUT" || true
        runstats "$D"
        rmdata "$W"
        return 0
    fi
    # ref/ is phase 2's reference and stays so: the extra pass (p6) dumps its own load's answers to ref-p6/ and checks
    # them against ref/ (the same input loaded the same way must answer the same; a difference fails loudly)
    local R=ref; [ -n "${SWEEP_SUB-}" ] && R=ref-p6
    mkdir -p "$WORK/$R"
    untimed "$D/ref-windows.log" sh -c "'$TB/tsdbq' dump spec.tsv 200 > $R/windows.jsonl"
    untimed "$D/ref-full.log" sh -c "'$TB/tsdbq' full 1 1 dump 2> $R/full.jsonl"
    untimed "$D/ref-lttb.log" "$B/relq" lttb 1 1 "$R/lttb.jsonl"
    untimed "$D/ref-twa.log" "$B/relq" twa 1 1 "$R/twa.jsonl"
    for k in $TWA_KINDS $TWA_WKINDS; do untimed "$D/ref-$k.log" env TWA_SPEC="$WORK/spec.tsv" "$B/relq" "$k" 1 1 "$R/$k.jsonl"; done
    [ "$R" = ref ] || untimed "$D/check-ref-p6.txt" ref_same ref "$R" \
        || log "  p6 tsdb: this load's answers differ from phase 2's reference (see $D/check-ref-p6.txt)"
    tstep "p2 tsdb full c1" "$D/full.c1.txt" -- "$TB/tsdbq" full 1 "$REPS"
    tstep "p2 tsdb full c$NPER" "$D/full.c$NPER.txt" -- "$TB/tsdbq" full $NPER "$REPS"
    tstep "p2 tsdb lttb c1" "$D/lttb.c1.txt" -- "$B/relq" lttb 1 "$REPS"
    tstep "p2 tsdb lttb c16" "$D/lttb.c16.txt" -- "$B/relq" lttb 16 "$REPS"
    tstep "p2 tsdb twa c1" "$D/twa.c1.txt" -- "$B/relq" twa 1 "$REPS"
    tstep "p2 tsdb twa c16" "$D/twa.c16.txt" -- "$B/relq" twa 16 "$REPS"
    tsdbtwa "$D" "" 1 16
    tsdbsuite tsdbq "$D" "1 $NHI"
    untimed "$D/check-self.txt" tsdb_self "" && tsdbbsuite tsdbq "$D"
    tstep "p2 tsdbc-bench query c1" "$D/tsdbc-bench.c1.txt" -- "$T/tsdbc-bench" query --conns 1 --seconds 5 --port "$TSDB_PORT"
    tstep "p2 tsdbc-bench query c$NHI" "$D/tsdbc-bench.c$NHI.txt" -- "$T/tsdbc-bench" query --conns $NHI --seconds 5 --port "$TSDB_PORT"
    # partitions merged (README row "tsdbd, 1 connection, partitions merged")
    tstep "p2 tsdb optimize" "$D/optimize.txt" -- bash -c "'$TB/tsdbstat' optimize && du -sb --exclude='retired_*' '$W/db'"
    untimed "$D/check-self.merged.txt" tsdb_self merged || { cd "$OUT"; tsdbd_stop; runstats "$D"; return 1; }
    tstep "p2 tsdb merged full c1" "$D/merged.full.c1.txt" -- "$TB/tsdbq" full 1 "$REPS"
    tstep "p2 tsdb merged full c$NPER" "$D/merged.full.c$NPER.txt" -- "$TB/tsdbq" full $NPER "$REPS"
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
ref_same() { # A B (cwd WORK): the reference dumps in B against those in A, with the checkers every system gets
    local k rc=0
    python3 -I "$H/check.py" --dumps "$1/windows.jsonl" "$1/full.jsonl" "$2/windows.jsonl" "$2/full.jsonl" || rc=1
    for k in lttb twa $TWA_KINDS $TWA_WKINDS; do REL jsonl "$k" "$1/$k.jsonl" "$2/$k.jsonl" || rc=1; done
    return "$rc"
}
ch_load_rep() { # one load: fresh server and table, ch_load.sh (insert, syncfs, merges), size
    local W=$1
    rmdata "$W"
    ch_start "$W" || return 1
    CHC -n -q "CREATE DATABASE cmp; CREATE TABLE cmp.t100m (series UInt32, ts DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)), value Float32 CODEC(Gorilla, ZSTD(1))) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY (series, ts)" || return 1
}
chparts() { # FILE: the merges running and the active parts per partition now (ClickHouse's state under a query suite)
    { echo "$(date '+%F %T') merges running: $(CHC -q "SELECT count() FROM system.merges WHERE database = 'cmp' AND table = 't100m'")"
      echo "partition, parts, rows, bytes_on_disk:"
      CHC -q "SELECT partition, count(), sum(rows), sum(bytes_on_disk) FROM system.parts WHERE database = 'cmp' AND table = 't100m' AND active GROUP BY partition ORDER BY partition FORMAT TSV"; } > "$1" 2>&1
}
ch_ready() { curl -sf http://127.0.0.1:8123/ping > /dev/null; }
ch_merged() { # DIR BATCHES-OK: the table after OPTIMIZE FINAL ("parts merged", like tsdb's merged.* rows): the same answers
    # checked, the classes, the whole range, the batches, LTTB and TWA once more under the prefix merged
    local D=$1 bok=$2 k r
    cd "$WORK" || return 1
    untimed "$D/optimize.txt" bash -c "'$CLICKHOUSE' client --host 127.0.0.1 -q 'OPTIMIZE TABLE cmp.t100m FINAL' && CH_STABLE=${CH_MERGED_STABLE:-60} '$H/ch_settle.sh' t100m" || return 1
    chparts "$D/parts-merged-before-suite.txt"
    untimed "$D/check.merged.txt" python3 -I "$H/check.py" ch 200
    chsuite merged "$D" ch
    [ "$bok" = 1 ] && chbsuite merged "$D" ch
    for k in lttb twa; do
        chbench1 1 1 0 "$D/q.$k.sql" > /dev/null 2> "$D/warm.merged.$k" 9>&-
        for r in $(seq "$REPS"); do tstep "merged.$k r$r" "$D/merged.$k.$r" -- chbench_tag "merged.$k.$r.$$" 1 1 0 "$D/q.$k.sql"; done
    done
    chparts "$D/parts-merged-after-suite.txt"
}
p2_ch() {
    local D=$OUT/p2/ch${SWEEP_SUB-} W=$DBCMP/s2/ch r bok=0
    local QCTR=rb-ch QREADY=ch_ready  # a server that dies in a class is started again and the class run once more
    # ClickHouse refusing a query by its own memory limit (max_server_memory_usage, 90% of the container's): "Code: 241
    # ... memory limit exceeded" (32 clients of 100-window weekly TWA batches, 2026-11-02): a result, not a FAIL
    local QREFUSE='Code: 241|memory limit exceeded'; export QREFUSE
    mkdir -p "$D"
    for r in $(seq "${LOADS_FROM:-1}" "$REPS"); do  # LOADS_FROM=$REPS: a rerun of later steps loads once (RESUME)
        crm rb-ch
        ch_load_rep "$W" || { fail "p2 ch start r$r"; return 1; }
        warm "$DATA"
        tstep "p2 ch load r$r" "$D/load.$r" -- bash -c "'$H/ch_load.sh' t100m && '$CLICKHOUSE' client --host 127.0.0.1 -q \"SELECT 'size', count(), sum(rows), sum(bytes_on_disk) FROM system.parts WHERE database = 'cmp' AND table = 't100m' AND active\""
        [ "$r" -lt "$REPS" ] && { crm rb-ch; rmdata "$W"; }
    done
    load_ok "p2 ch" || return 1
    ports_check "$D/ports.txt"
    CHC -q "SELECT version()" > "$D/version.txt"
    chparts "$D/parts-before-suite.txt"
    cd "$WORK" || return 1
    python3 -I "$H/mkq.py" ch
    untimed "$D/check.txt" python3 -I "$H/check.py" ch 200
    if [ -n "${MANY_CLIENTS-}" ]; then  # the extra-clients pass (phase6many): this load, the batches' answer check, the CLASS_FILTER classes
        # the aggregate batches, then the TWA windows in batches (twasuite checks their single windows first and times
        # nothing else here): twa7d.b100 is the class that ran ClickHouse out of memory at 32 clients
        bcheck ch "$D" && chbsuite ch "$D" ch
        twasuite ch "$D" ch
        cd "$OUT" || true
        ctr_mem rb-ch "$D/mem.txt"
        crm rb-ch; rmdata "$W"
        runstats "$D"
        return 0
    fi
    (cd "$WORK" && python3 -I "$H/mkbatch.py" ch) || fail "mkbatch ch"
    chsuite ch "$D" ch
    bcheck ch "$D" && { bok=1; chbsuite ch "$D" ch; pc_pin rb-ch && PC=1 chbsuite ch "$D" ch; pc_unpin rb-ch; }
    REL q ch lttb > "$D/q.lttb.sql"; REL q ch twa > "$D/q.twa.sql"
    untimed "$D/check-lttb.txt" REL check ch lttb ref/lttb.jsonl && lttb_loose "$D/check-lttb.txt" "$D"
    untimed "$D/check-twa.txt" REL check ch twa ref/twa.jsonl
    # TWA_RETIME=ch (the extra pass of 2026-11-02): ClickHouse's TWA is timed again with the full REPS into p6/,
    # next to the two-level form below, in one load
    local TD=$D TR=$REPS k r
    case "$D: ${TWA_RETIME-} " in */memsweep:*" ch "*) TD=$(twab_dir "$D"); TR=${XREPS:-$REPS}; mkdir -p "$TD" ;; esac
    for k in lttb twa; do  # timed by the server (query_log), not the client (REPORT §3)
        [ "$k" = twa ] || [ "$TD" = "$D" ] || continue
        [ "$TD" = "$D" ] || cp "$D/q.$k.sql" "$TD/q.$k.sql"
        chbench1 1 1 0 "$TD/q.$k.sql" > /dev/null 2> "$TD/warm.$k" 9>&-
        for r in $(seq "$TR"); do tstep "$k r$r" "$TD/$k.$r" -- chbench_tag "$k.$r.$$" 1 1 0 "$TD/q.$k.sql"; done
    done
    REPS=$TR twasuite ch "$TD" ch
    # the two-level STEP form (rel.py q_ch2: per-bucket arrays, then only the edges over buckets); the release table
    # quotes whichever form is faster (twa-research/fairness.md)
    for k in twa twad; do
        REL q ch2 "$k" > "$TD/q.${k}2.sql"
        untimed "$TD/check-${k}2.txt" REL check ch2 "$k" "ref/$k.jsonl" || { nocheck ch2 "${k}2" "$TD" $?; continue; }
        chbench1 1 1 0 "$TD/q.${k}2.sql" > /dev/null 2> "$TD/warm.${k}2" 9>&-
        for r in $(seq "$TR"); do tstep "${k}2 r$r" "$TD/${k}2.$r" -- chbench_tag "${k}2.$r.$$" 1 1 0 "$TD/q.${k}2.sql"; done
    done
    CHC -q "SELECT count(), sum(bytes_on_disk) FROM system.parts WHERE database = 'cmp' AND table = 't100m' AND active" > "$D/size-end.txt"
    chparts "$D/parts-after-suite.txt"
    ch_merged "$D" "$bok"
    cd "$OUT" || true
    ctr_mem rb-ch "$D/mem.txt"
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
    local QCTR=rb-qdb QREADY=qdb_ready  # a server that dies in a class is started again and the class run once more
    # QuestDB refusing by its own limits is a result (REFUSED.txt), not a harness failure: its RSS limit, and "table busy
    # [reason=pool size exceeded]" when more readers want a table than its reader pool holds
    local QREFUSE='global RSS memory limit exceeded|table busy'; export QREFUSE
    [ "$pq" = 1 ] && P=qdb-parquet
    [ "$pq" = 2 ] && P=qdb-parquet-zstd
    local QDB_ZSTD=0; [ "$pq" = 2 ] && QDB_ZSTD=1
    D=$OUT/p2/$P${SWEEP_SUB-} W=$DBCMP/s2/$P
    mkdir -p "$D"
    for r in $(seq "${LOADS_FROM:-1}" "$REPS"); do  # LOADS_FROM=$REPS: a rerun of later steps loads once (RESUME)
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
    if [ -n "${MANY_CLIENTS-}" ]; then  # the extra-clients pass (phase6many): this load, the batches' answer check, the CLASS_FILTER classes
        bcheck qdb "$D" && bsuite qdb "$P" "$D" pg "$PG"
        cd "$OUT" || true
        ctr_mem rb-qdb "$D/mem.txt"
        crm rb-qdb; rmdata "$W"
        runstats "$D"
        return 0
    fi
    qsuite qdb "$P" "$D" pg "$PG"
    local bok=0
    bcheck qdb "$D" && { bok=1; perc16 qdb "$P" "$D" pg "$PG"; bsuite qdb "$P" "$D" pg "$PG"; }
    # TWA_RETIME=qdb (the extra pass of 2026-11-02): QuestDB's TWA kinds are timed again with the full REPS into p6/,
    # because rel.py's QuestDB TWA now reads its segments once (split_once once=True) instead of twice
    local TD=$D TR=$REPS
    case "$D: ${TWA_RETIME-} " in */memsweep:*" qdb "*) TD=$(twab_dir "$D"); TR=${XREPS:-$REPS}; mkdir -p "$TD" ;; esac
    REL q qdb twa > "$TD/q.twa.sql"  # native twap() + SAMPLE BY 1h (REPORT §5.10), timed only when it agrees
    untimed "$TD/check-twa.txt" REL check qdb twa ref/twa.jsonl && REPS=$TR qline twa "$TD" "$TD/q.twa.sql" pg "$PG"
    REL q qdbseg twa > "$TD/q.twaseg.sql"  # the 0.9.0 run's lead() CTE, for comparison
    untimed "$TD/check-twaseg.txt" REL check qdbseg twa ref/twa.jsonl && REPS=$TR qline twaseg "$TD" "$TD/q.twaseg.sql" pg "$PG"
    REPS=$TR twasuite qdb "$TD" pg "$PG"
    # per core: a restart with every worker pool at one thread (the data stays), then a normal start for what follows
    if [ $bok = 1 ] && want_pc; then
        QDB_PC=1 pc_restart rb-qdb "$D/config.pc.txt" qdb_start "$W" && PC=1 bsuite qdb "$P" "$D" pg "$PG"
        crm rb-qdb; [ "$pq" = 0 ] && { qdb_start "$W" || return 1; }
    fi
    if [ "$pq" = 0 ]; then
        # HTTP API cross-check (README: within 10% of the PostgreSQL protocol): one run per measurement
        REPS=1 qsuite qdbh "$P-http" "$D" http 127.0.0.1 9100 ""
        # variant: an index on the symbol (README), classes only
        QX "ALTER TABLE sensors ALTER COLUMN series ADD INDEX" > "$D/index.txt"
        if qdb_wait_table sensors >> "$D/index.txt"; then FULLS="" qsuite qdb "$P-index" "$D" pg "$PG"
        else fail "p2 $P: the index's WAL was not applied (see $D/index.txt); index classes not timed"; fi
    fi
    cd "$OUT" || true
    ctr_mem rb-qdb "$D/mem.txt"
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
ts_ready() { [ "$(psql -h 127.0.0.1 -p 5434 -U postgres -Atc 'select 1' 2> /dev/null)" = 1 ]; }  # QREADY of rb-ts
# 32 clients of 7-day batches of 100 series ran TimescaleDB out of memory at 48 GB with its defaults (2026-11-01): each
# query is 100 UNION ALL branches that decompress their chunks, in up to 16 parallel workers per query (tune's
# max_parallel_workers_per_gather), each worker with its own memory; work_mem (6 MB) bounds none of it. On a machine that
# 32 clients already keep busy, fewer workers per query cost little, so the 32-client classes take the most workers of
# TS_MANY_TRY that this class survives, and say so (many-parallel.txt, driver.log); 1-client classes keep the default.
ts_many_probe() { # DIR CONN-PREFIX: sets MANY_CONN (the caller's local) unless every setting runs out of memory.
    # Probed at the client count of the classes it is for: NHI, or MANY_CLIENTS in the extra-clients pass
    local D=$1 base=$2 w rc f n=${MANY_CLIENTS:-$NHI}
    (cd "$WORK" && python3 -I "$H/mkbatch.py" ts) || { fail "mkbatch ts"; return 1; }
    for w in ${TS_MANY_TRY:-2 1 0}; do
        f=$D/probe.par$w.txt
        steplock -s
        limited "$STEP_TIMEOUT" "$f" "$B/qdrive" pg "$base -c max_parallel_workers_per_gather=$w'" "$WORK/q/ts.agg7d.b100.txt" "$n" "$SECS"
        rc=$?
        stepunlock
        ctr_mem rb-ts "$f"
        if ctr_died rb-ts; then
            log "  ts probe: max_parallel_workers_per_gather=$w at $n clients of agg7d.b100: $CTR_WHY"
            ctr_restart rb-ts || { fail "ts probe: rb-ts did not serve again"; return 1; }
            continue
        fi
        [ "$rc" = 0 ] || { log "  ts probe: max_parallel_workers_per_gather=$w: rc $rc (see $f)"; continue; }
        MANY_CONN="$base -c max_parallel_workers_per_gather=$w'"
        echo "max_parallel_workers_per_gather=$w in the $n-client classes (tried: ${TS_MANY_TRY:-2 1 0})" > "$D/many-parallel.txt"
        log "  ts: the $n-client classes run with max_parallel_workers_per_gather=$w (1-client classes: the default)"
        return 0
    done
    echo "default in every class: out of memory at every setting tried (${TS_MANY_TRY:-2 1 0})" > "$D/many-parallel.txt"
    log "  ts: out of memory at every max_parallel_workers_per_gather tried; the $n-client classes keep the default"
}
p2_ts() {
    local QCTR=rb-ts QREADY=ts_ready
    local D=$OUT/p2/ts${SWEEP_SUB-} W=$DBCMP/s2/ts r
    mkdir -p "$D"
    for r in $(seq "${LOADS_FROM:-1}" "$REPS"); do  # LOADS_FROM=$REPS: a rerun of later steps loads once (RESUME)
        crm rb-ts; rmdata "$W"
        ts_start rb-ts "$TS_IMG" "$W" "$DBCMP/input" || return 1
        ts_load_rep > "$D/setup.$r.txt" 2>&1 || { fail "p2 ts setup r$r"; return 1; }
        warm "$DBCMP/input/mixed.csv"
        tstep "p2 ts load r$r" "$D/load.$r" -- ts_timed rb-ts
        [ "$r" -lt "$REPS" ] && { crm rb-ts; rmdata "$W"; }
    done
    load_ok "p2 ts" || return 1
    ports_check "$D/ports.txt"
    PSQL -Atc "select version()" -c "select extversion from pg_extension where extname='timescaledb'" -c "show work_mem" > "$D/version.txt"
    cd "$WORK" || return 1
    python3 -I "$H/mkq.py" ts
    untimed "$D/check.txt" python3 -I "$H/check.py" ts 200
    local MANY_CONN=
    [ -n "${TS_MANY_TRY-x}" ] && ts_many_probe "$D" "host=127.0.0.1 port=5434 user=postgres password=bench dbname=postgres options='-c TimeZone=UTC"
    if [ -n "${MANY_CLIENTS-}" ]; then  # the extra-clients pass (phase6many): this load, the probe above (at MANY_CLIENTS
        # clients; many-parallel.txt says which setting), the batches' answer check, the CLASS_FILTER classes. Not the
        # -ha image's load for LTTB and TWA
        bcheck ts "$D" && bsuite ts ts "$D" pg "host=127.0.0.1 port=5434 user=postgres password=bench dbname=postgres options='-c TimeZone=UTC'"
        cd "$OUT" || true
        ctr_mem rb-ts "$D/mem.txt"
        crm rb-ts; rmdata "$W"
        runstats "$D"
        return 0
    fi
    qsuite ts ts "$D" pg "host=127.0.0.1 port=5434 user=postgres password=bench dbname=postgres options='-c TimeZone=UTC'"
    bcheck ts "$D" && {
        for n in ${FULLS-$FULLS_DEFAULT}; do  # like with like (REPORT §3, §5.11): one query per series, 16 connections
            reps "ts.full$n.per.c$NPER" "$D" -- "$B/qdrive" pg "host=127.0.0.1 port=5434 user=postgres password=bench dbname=postgres options='-c TimeZone=UTC'" "$WORK/q/ts.full$n.per.txt" "$NPER" 100000 0
        done
        bsuite ts ts "$D" pg "host=127.0.0.1 port=5434 user=postgres password=bench dbname=postgres options='-c TimeZone=UTC'"
        pc_pin rb-ts && PC=1 bsuite ts ts "$D" pg "host=127.0.0.1 port=5434 user=postgres password=bench dbname=postgres options='-c TimeZone=UTC -c max_parallel_workers_per_gather=0'"
        pc_unpin rb-ts; }
    cd "$OUT" || true
    ctr_mem rb-ts "$D/mem.txt"
    crm rb-ts; rmdata "$W"
    # LTTB and TWA need the toolkit, which is in the -ha image (same TimescaleDB 2.30.1, PostgreSQL 17.11, toolkit 1.26.0) only:
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
    untimed "$D/check-lttb.txt" REL check ts lttb ref/lttb.jsonl && lttb_loose "$D/check-lttb.txt" "$D"
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
    I2_PORT=8086 STABLE="$I2_STABLE" "$H/i2_settle.sh" "$(date +%s)" || return 1
    echo "size: $(podman unshare du -sb "$1/data" | cut -f1) bytes (du -sb of the data directory)"
}
p2_i2() {
    local D=$OUT/p2/i2${SWEEP_SUB-} W=$DBCMP/s2/i2 r bid
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
        bcheck i2ql "$D" && { [ $x = 1 ] || perc16 i2ql i2ql "$D" http 127.0.0.1 8086 $'Authorization: Token benchtoken\r\n'; } && PC=$x bsuite i2ql i2ql "$D" http 127.0.0.1 8086 $'Authorization: Token benchtoken\r\n'
        bcheck i2flux "$D" && { [ $x = 1 ] || perc16 i2flux i2flux "$D" http 127.0.0.1 8086 $'Authorization: Token benchtoken\r\nAccept: application/csv\r\n'; } && PC=$x bsuite i2flux i2flux "$D" http 127.0.0.1 8086 $'Authorization: Token benchtoken\r\nAccept: application/csv\r\n'
        # per core: a restart with one Go P (GOMAXPROCS=1; influxd has no query-thread knob), pinned
        [ $x = 0 ] && { want_pc || break; I2_EXTRA="$I2_EXTRA -e GOMAXPROCS=1" pc_restart rb-i2 "$D/config.pc.txt" i2_start "$W" || break; }
    done
    want_pc && { crm rb-i2; i2_start "$W" || return 1; }  # without per core (NO_PC) the normal server still runs
    TWA_WKINDS="" twasuite i2flux "$D" http 127.0.0.1 8086 $'Authorization: Token benchtoken\r\nAccept: application/csv\r\n'
    cd "$OUT" || true
    crm rb-i2; rmdata "$W"
    runstats "$D"
}
p2_i3() {
    # InfluxDB 3 Core was OOM-killed at its 28 GB limit by 32 concurrent batch queries over 65,000 Parquet files
    # (smoke): recorded as such and restarted (tstep), not a harness failure
    local QCTR=rb-i3 QREADY="curl -sf http://127.0.0.1:8181/health"
    local D=$OUT/p2/i3${SWEEP_SUB-} W=$DBCMP/s2/i3 r
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
    if bcheck i3 "$D"; then
        perc16 i3 i3 "$D" http 127.0.0.1 8181 ""; bsuite i3 i3 "$D" http 127.0.0.1 8181 ""
        # per core: a restart with one DataFusion thread (its runtime sizes itself at start), pinned; then a normal start
        if want_pc; then
            pc_restart rb-i3 "$D/config.pc.txt" i3_start "$W" $I3_P2_FLAGS --num-datafusion-threads 1 &&
                PC=1 bsuite i3 i3 "$D" http 127.0.0.1 8181 ""
            crm rb-i3; i3_start "$W" $I3_P2_FLAGS || return 1
        fi
    fi
    REL q i3 twa > "$D/q.twa.http"
    untimed "$D/check-twa.txt" REL check i3 twa ref/twa.jsonl
    qline twa "$D" "$D/q.twa.http" http 127.0.0.1 8181 ""
    twasuite i3 "$D" http 127.0.0.1 8181 ""
    cd "$OUT" || true
    crm rb-i3; rmdata "$W"
    runstats "$D"
}
p2_sys() { case $1 in qdb0) p2_qdb 0 ;; qdb1) p2_qdb 1 ;; qdb2) p2_qdb 2 ;; *) "p2_$1" ;; esac; }
phase2() {
    local s
    for s in $P2_SYSTEMS; do
        reset_quiet
        done_mark "$OUT/p2/$s.done" && { log "p2 $s already done"; continue; }
        log "p2 $s start"
        aside 2 "$s"
        LOAD_FAILED=0
        local nf=$(cat "$PH_DIR/FAILS" 2> /dev/null | wc -l)
        p2_sys "$s" || fail "p2 $s returned an error"
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
    } > "$D/check.txt" 2>&1  # every answer compared even after a mismatch (exit codes ignored): the grep judges
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
        "$TB/embq" tsdb qps "$E/tsdb" spec.tsv "$c" 1 "$WARM_SECS" > /dev/null 2>&1 9>&-
        for t in 1 $NHI; do reps "tsdb.$c.t$t" "$D" -- "$TB/embq" tsdb qps "$E/tsdb" spec.tsv "$c" "$t" "$SECS"; done
        "$TB/embq" duckdb qps "$E/t.duckdb" spec.tsv "$c" 1 "$WARM_SECS" > /dev/null 2>&1 9>&-
        for t in 1 $NHI; do reps "duckdb.$c.t$t" "$D" -- "$TB/embq" duckdb qps "$E/t.duckdb" spec.tsv "$c" "$t" "$SECS"; done
        for e in chdb chdb-lz4; do
            "$B/chq" qps "$E/$e" spec.tsv "$c" 1 "$WARM_SECS" > /dev/null 2>&1 9>&-
            for t in 1 $NHI; do reps "$e.$c.t$t" "$D" -- "$B/chq" qps "$E/$e" spec.tsv "$c" "$t" "$SECS"; done
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
    lttb_loose "$D/check-rel.txt" "$D"
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
prom_rw_check() { # check.py exits 0 on mismatches; 1 here, so that untimed's UNTIMED_SOFT path sees them
    env PROM_PORT=9090 python3 -I "$H/check.py" prom 200 > "$OUT/.check-rw.out" 2>&1
    local rc=$?
    cat "$OUT/.check-rw.out"
    [ $rc = 0 ] && grep -qE 'mismatches [1-9]|MISMATCH' "$OUT/.check-rw.out" && rc=1
    rm -f "$OUT/.check-rw.out"
    return $rc
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
    # short answers: acknowledged samples Prometheus lost in its compactions are a result to report (loose.txt), if
    # promlost.py finds them and they explain every difference; anything else fails as an answer check
    UNTIMED_SOFT=1 untimed "$D/check-rw.txt" prom_rw_check
    [ $? = 2 ] && untimed "$D/check-rw-lost.txt" env PROM_PORT=9090 python3 -I "$TOOLS/promlost.py" "$D/check-rw.txt" &&
        echo "remote write: $(tail -1 "$D/check-rw-lost.txt" | sed 's/^promlost: //') after its compactions (the load was acknowledged and every sample answered right after it; check-rw.txt, check-rw-lost.txt)" >> "$D/loose.txt"
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
# P5_SYSTEMS ("tsdb ch") reruns one side: the other side's results stay, the rerun side's earlier results move to
# $OUT/old/ first; ClickHouse needs the SQL tsdb's warm-up wrote ($D/sql). The loads (insert, then merges settled) get
# P5_LOAD_TIMEOUT: on 2026-11-01 tsdb's took 3379 s and ClickHouse's insert alone 1443 s, so STEP_TIMEOUT (3600 s)
# cut ClickHouse's settle short and its queries ran beside its merges.
p5() {
    local D=$OUT/p5 W=$P5_DIR/big C=$P5_DIR/ch-big r avail sys=" ${P5_SYSTEMS:-tsdb ch} " f old
    mkdir -p "$D"
    case $P5_DIR in /var/tmp | /var/tmp/?*) ;; *) fail "p5: P5_DIR must be under /var/tmp, not $P5_DIR"; return 1 ;; esac
    mkdir -p "$P5_DIR"
    avail=$(df -B1G --output=avail "$P5_DIR" | tail -1 | tr -d ' ')
    echo "free on $P5_DIR: $avail GB" > "$D/disk.txt"
    [ "$avail" -ge "$P5_MINFREE" ] || { fail "p5: only $avail GB free on $P5_DIR, need about $P5_MINFREE (tsdb 116 GB + ClickHouse 170 GB + margin)"; return 1; }
    if [ "$sys" != " tsdb ch " ]; then  # a rerun of one side: its earlier results aside
        old=$OUT/old/p5.$(date +%m%d-%H%M%S); mkdir -p "$old"
        case $sys in *" tsdb "*) for f in "$D"/tsdb* "$D"/warm.tsdb*; do [ -e "$f" ] && mv "$f" "$old/"; done ;; esac
        case $sys in *" ch "*) for f in "$D"/ch.* "$D"/ch "$D"/warm.ch; do [ -e "$f" ] && mv "$f" "$old/"; done ;; esac
        log "  p5: rerun of${sys% }; its earlier results moved to $old"
    fi
    case $sys in *" tsdb "*)
        rmdata "$W"
        STEP_TIMEOUT=$P5_LOAD_TIMEOUT tstep "p5 tsdb load" "$D/tsdb.load.txt" -- "$T/scale-bench" load --dir "$W" --series "$P5_SERIES" --points "$P5_POINTS" --threads 8 ||
            { fail "p5 tsdb load (see $D/tsdb.load.txt): no tsdb queries"; return 1; }
        du -sb --exclude='retired_*' "$W" > "$D/tsdb.du.txt"
        mkdir -p "$D/sql"
        "$T/scale-bench" query --dir "$W" --series "$P5_SERIES" --threads 32 --sql "$D/sql" > "$D/warm.tsdb-emb" 2>&1 9>&-  # warm-up, writes the SQL
        reps tsdb-emb "$D" -- "$T/scale-bench" query --dir "$W" --series "$P5_SERIES" --threads 32
        tsdbd_start "$W" || return 1
        "$T/scale-bench" query --port "$TSDB_PORT" --series "$P5_SERIES" --threads 32 > "$D/warm.tsdbd" 2>&1 9>&-
        reps tsdbd "$D" -- "$T/scale-bench" query --port "$TSDB_PORT" --series "$P5_SERIES" --threads 32
        tsdbd_stop
        rmdata "$W"
        cp "$W.log" "$D/tsdbd.log" 2> /dev/null
        rm -f "$W.log" "$W.pf" ;;
    esac
    case $sys in *" ch "*)
        [ -n "$(ls -A "$D/sql" 2> /dev/null)" ] || { fail "p5 ch: no SQL in $D/sql (tsdb's warm-up writes it)"; return 1; }
        # ClickHouse on the same file system
        crm rb-ch
        rmdata "$C"
        mkdir -p "$C/data"
        ch_start "$C" || return 1
        ports_check "$D/ports.txt"
        CHC -n -q "CREATE DATABASE scale; CREATE TABLE scale.big (series UInt32, ts DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)), value Float32 CODEC(Delta, ZSTD(1))) ENGINE = MergeTree PARTITION BY toDate(ts) ORDER BY (series, ts)" || { fail "p5 ch create"; return 1; }
        if STEP_TIMEOUT=$P5_LOAD_TIMEOUT tstep "p5 ch load" "$D/ch.load.txt" -- ch_big_load; then
            "$ROOT/tools/scale-bench-ch.sh" "$D/sql" scale.big "$CLICKHOUSE" > "$D/warm.ch" 2>&1 9>&-
            reps ch "$D" -- "$ROOT/tools/scale-bench-ch.sh" "$D/sql" scale.big "$CLICKHOUSE"
        else
            fail "p5 ch load (see $D/ch.load.txt): not settled, so no ClickHouse queries (they would run beside its merges)"
        fi
        CHC -q "SELECT count(), sum(rows), sum(bytes_on_disk) FROM system.parts WHERE database = 'scale' AND active" > "$D/ch.size-end.txt"
        CHC -q "SELECT name, data_compressed_bytes, data_uncompressed_bytes FROM system.columns WHERE database = 'scale' AND table = 'big'" > "$D/ch.columns.txt"
        crm rb-ch
        rmdata "$C" ;;
    esac
    runstats "$D"
}
phase5() { reset_quiet; p5 || fail "p5 returned an error"; }

# ================================================================== phase 6: the extra pass over phase 2's systems
# For results phase 2 did not have when it ran: per system, ONE load (its own p2 function, with REPS=1), then
#  (1) the TWA batch classes (tsdbtwab: every series in one QUERY_MULTI, KIND.b100.c1, and the windows K at a time;
#      twabsuite: the other systems' TWA windows K at a time), timed REPS (XREPS) times into <dir>/p6, which tables.py
#      shows in the system's table marked "p6:" (another load than phase 2's rows); they run where phase 2 runs
#      them, after the single-window TWA kinds of the same load, and only for a kind whose single windows agreed;
#  (2) a memory sweep: every other class of phase 2 once, per core left out (NO_PC), each with mem_sample, into
#      <dir>/memsweep, which runstats and tables.py keep apart from the timings (one run is not a result to average;
#      tables.py reports the peak memory per class and the system's overall peak from it).
# The p2 function's own safeguards stay as they are (step timeouts, QCTR deaths and restarts, REFUSED, the quiet gate
# and BENCH.lock in every tstep, ts_many_probe, SERVER_MEM). steps.log labels start with "p6 ". tsdb's references of
# this load go to $WORK/ref-p6 and are checked against phase 2's ref/, which stays the reference. how.json gets a p6
# block (how.py p6). $OUT/p6/<sys>.done means the pass reached the end of the system (failed or refused classes are
# results); RESUME=1 continues after a stop: done systems are skipped, the interrupted one loads again (as every
# RESUME does) and skips the steps whose output is there (p6/ and memsweep/ are kept for it; without RESUME a rerun
# moves both to $OUT/old/, and a phase-2 rerun of the system (aside) makes its p6 due again).
P6_SYSTEMS=${P6_SYSTEMS:-$P2_SYSTEMS}
how_p6() { # DIR REPS: the p6 block of DIR/how.json (how.py p6: when, runs, trees, the pass's dash and refusals)
    env T="$T" TSDB_TREE="$TSDB_TREE" XREPS="$2" MANY_CLIENTS="${MANY_CLIENTS-}" CLASS_FILTER="${CLASS_FILTER-}" python3 -I "$RDIR/how.py" p6 "$1" >> "$OUT/driver.log" 2>&1 \
        || fail "how.json p6 block of $1"
}
# The extra-clients pass (MANY_CLIENTS=16 CLASS_FILTER=b100 run.sh p6, systems tsdb, qdb0-2, ch and ts): QuestDB's
# 100-series batches ran out of memory at 32 clients (its own RSS limit, the 48 GB container limit), ClickHouse's weekly
# TWA batches of 100 windows at its memory limit, and TimescaleDB's runs have had out-of-memory trouble there as well, so
# these are timed at 16 as well. Per system ONE load, the batches' answer check, then the CLASS_FILTER classes at MANY_CLIENTS clients,
# REPS runs each, into <dir>/p6/NAME.cN (labels "p6 NAME.cN rK"; the load, checks and memory files of the load beside
# them). Nothing is moved aside: the 32-client rows, p6/ and memsweep/ stay as they were, whatever RESUME says.
# $OUT/p6/<sys>.cN.done marks a system's pass.
phase6many() {
    local s d ok reps=$REPS
    for s in $P6_SYSTEMS; do
        case $s in tsdb|qdb0|qdb1|qdb2|ch|ts) ;; *) fail "p6 $s: MANY_CLIENTS is implemented for tsdb, qdb0-qdb2, ch and ts only"; continue ;; esac
        reset_quiet
        done_mark "$OUT/p6/$s.c$MANY_CLIENTS.done" && { log "p6 $s c$MANY_CLIENTS already done"; continue; }
        d=$(res_dir 2 "$s")
        mkdir -p "$d/p6"
        log "p6 $s start: one load; the classes '${CLASS_FILTER:-all}' at $MANY_CLIENTS clients, $reps runs, into $d/p6"
        LOAD_FAILED=0
        if SWEEP_SUB=/p6 XREPS=$reps REPS=1 LOADS_FROM=1 NO_PC=1 LABEL_PFX="p6 " p2_sys "$s"; then ok=1
        else ok=0; fail "p6 $s c$MANY_CLIENTS returned an error (a start or load failed; RESUME=1 loads it again)"; fi
        cleanup_all
        [ -e "$d/how.json" ] || how_write 2 "$s"
        how_p6 "$d" "$reps"   # adds the pass to the p6 block (how.py: MANY_CLIENTS), keeps the TWA pass's facts
        runstats "$d/p6"
        [ "$ok" = 1 ] && date > "$OUT/p6/$s.c$MANY_CLIENTS.done"
        log "p6 $s c$MANY_CLIENTS end"
    done
    rmdata "$DBCMP/s2"
}
phase6() {
    local s d x ok reps=$REPS
    [ -n "${MANY_CLIENTS-}" ] && { phase6many; return; }
    for s in $P6_SYSTEMS; do
        reset_quiet
        done_mark "$OUT/p6/$s.done" && { log "p6 $s already done"; continue; }
        d=$(res_dir 2 "$s")
        mkdir -p "$d"
        # a rerun starts empty, as aside does for phase 2: the last attempt's p6/ and memsweep/ go to $OUT/old/, so a
        # stale run is never averaged with new ones (RESUME=1 keeps them: it goes on where the last attempt stopped)
        if [ "${RESUME:-0}" != 1 ]; then
            for x in p6 memsweep; do
                [ -d "$d/$x" ] || continue
                mkdir -p "$OUT/old" && mv "$d/$x" "$OUT/old/p6-$s-$x.$(date +%m%d-%H%M%S)" && log "p6 $s: earlier $x/ moved to $OUT/old/"
            done
        fi
        log "p6 $s start: one load; the TWA batch classes ($reps runs) into $d/p6; every other class once into $d/memsweep"
        LOAD_FAILED=0
        # the done marker says the pass reached the end of the system: classes that failed or were refused are results
        # it recorded (FAILS, REFUSED.txt, dash.txt), so RESUME does not load the system again for them
        if SWEEP_SUB=/memsweep XREPS=$reps REPS=1 LOADS_FROM=1 NO_PC=1 LABEL_PFX="p6 " p2_sys "$s"; then ok=1
        else ok=0; fail "p6 $s returned an error (a start or load failed; RESUME=1 loads it again)"; fi
        cleanup_all
        [ -e "$d/how.json" ] || how_write 2 "$s"   # a system phase 2 has not run: tables.py needs how it ran
        how_p6 "$d" "$reps"
        [ -d "$d/p6" ] && runstats "$d/p6"   # none for a system with no TWA query (VictoriaMetrics: dash.txt says why)
        [ "$ok" = 1 ] && date > "$OUT/p6/$s.done"
        log "p6 $s end"
    done
    rmdata "$DBCMP/s2"
}

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
# run.sh again in its own DBCMP (SMOKE_DIR, under /var/tmp): prep p1 p2 p3 p4 p5 p6 with every system and variant, 10
# series x 20,000 rows with every window and bucket 60 times shorter (SPEC_DIV), 2 data sets for p1, p5 at 100 series
# x 100,000 rows, REPS=1, SECS=1, batches of 1, 10 and 100, 2 clients where the release run has 32 or 16 (NHI, NPER),
# short settles and a 120 s step timeout, SMOKE=1 (no quiet gate; BENCH.lock shared, per step). It is the builder's
# test and should take about 20 minutes: a system that is slow at full size is a result of the release run, not a
# reason for a slow smoke. Nothing it measures is a result: it passes when every phase
# is DONE, i.e. every answer check passed and no step failed. The tools and downloads (ClickHouse binary, DuckDB,
# chDB, IoTDB's venv, the hydraulic text files) stay the main DBCMP's; the images must be pulled.
phase_smoke() {
    local S=${SMOKE_DIR:-/var/tmp/dbcmp-smoke/small} t0=$SECONDS rc=0
    case $S in /var/tmp/?*) ;; *) log "smoke: SMOKE_DIR must be under /var/tmp, not $S"; return 1 ;; esac
    # SMOKE_PHASES and SMOKE_OUT rerun a part (SMOKE_P2_SYSTEMS picks p2's systems; the smoke leaves out qdb1, iotz and tdz,
    # whose queries are qdb2's, iot's and td's; P1_SYSTEMS etc. pass through) on the inputs of an earlier smoke run;
    # the smoke also drops the 1-client batch runs (BTHREADS=2; per core still runs) and converts at ZSTD level 3
    local ph=${SMOKE_PHASES:-prep p1 p2 p3 p4 p5 p6} so=${SMOKE_OUT:-$S/results}
    case $so in "$S"/?*) ;; *) log "smoke: SMOKE_OUT must be under $S, not $so"; return 1 ;; esac
    rm -rf "$so"; mkdir -p "$so"
    log "SMOKE START: run.sh $ph in $S (log $so/driver.log)"
    env DBCMP="$S" WORK="$S/work" DATA="$S/input/mixed.rowbinary" IN="$S/sizes/in" OUT="$so" \
        CLICKHOUSE="$CLICKHOUSE" DUCKDB_DIR="$DUCKDB_DIR" CHDB_DIR="$CHDB_DIR" IOT_VENV="$IOT_VENV" HYD_RAW="$HYD_RAW" \
        BENCH_LOCK="$LOCK" TSDB_TREE="$TSDB_TREE" TB="$TB" BIN="$BIN" \
        SMOKE=1 FORCE=1 REPS=1 SECS=1 NSERIES=10 TWA_N=100 KS="1 10 100" NHI=2 NPER=2 \
        PREP_SERIES=10 PREP_POINTS=20000 POINTS=20000 SPEC_DIV=60 PREP_SIZE_POINTS=50000 PREP_SPEC=200 \
        DSLIST="${SMOKE_DS:-hdd_temp hyd_PS1}" \
        IOT_STABLE=15 TD_STABLE=10 VM_STABLE=10 I2_STABLE=15 CH_STABLE=10 CH_MERGED_STABLE=10 QDB_WAIT_MAX=300 CH_SETTLE_MAX=300 WARM_SECS=1 PC_KS=1 \
        BTHREADS="${BTHREADS:-2}" QDB_ZSTD_LEVEL="${SMOKE_QDB_ZSTD_LEVEL:-3}" \
        P2_SYSTEMS="${SMOKE_P2_SYSTEMS:-tsdb ch qdb0 qdb2 ts i2 i3 iot td vm}" \
        I3_P2_FLAGS="--force-snapshot-max-age 30s" I3_P2_MIN=45 I3_P1_AGE=30s I3_P1_MIN=45 \
        I3_P2_STABLE=20 I3_P2_MAX=300 I3_P1_STABLE=20 I3_P1_MAX=300 \
        I2_EXTRA="-e INFLUXD_STORAGE_CACHE_SNAPSHOT_WRITE_COLD_DURATION=10s" PROM_SETTLE_TIMEOUT=900 \
        P5_SERIES=100 P5_POINTS=100000 P5_MINFREE=5 P5_DIR="$S/p5" \
        STEP_TIMEOUT=120 UNTIMED_TIMEOUT=600 CHECK_TIMEOUT=300 QDRIVE_TIMEOUT=60 \
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
tree_commit() { # DIR: the commit of a tree: its repository's HEAD, or for a tree made by `git archive` (no .git, the
                # release run's source copy) the file COMMIT at its top, which whoever made the copy writes
    local c
    c=$(git -C "$1" rev-parse HEAD 2> /dev/null) || c=$(head -c 40 "$1/COMMIT" 2> /dev/null | tr -d '\n')
    echo "${c:-unknown (no repository and no COMMIT file)}"
}
machine() {
    {
        date; uname -a; lscpu | grep -E 'Model name|^CPU\(s\)|Thread|MHz'; free -g | head -2; df -h /var/tmp
        echo "governor: $(cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor | sort | uniq -c | tr '\n' ' ')"
        echo "tsdb tree: $TSDB_TREE $(tree_commit "$TSDB_TREE") ($(git -C "$TSDB_TREE" diff --stat 2> /dev/null | tail -1))"
        echo "harness: $ROOT $(tree_commit "$ROOT")"
        podman --version
        for i in $CH_IMG $TS_IMG $TSHA_IMG $QDB_IMG $I2_IMG $I3_IMG "$PROM_IMG" $IOT_IMG $TD_IMG $VM_IMG; do echo "$i $(podman image inspect --format '{{.Digest}} {{.Created}}' "$i" 2> /dev/null)"; done
        echo "duckdb: $DUCKDB_DIR (libduckdb-linux-amd64.zip sha256 $DUCKDB_SHA)"
        echo "chdb: $CHDB_DIR (chdb-core v26.9.0 of 2026-09-28, linux-x86_64-libchdb.tar.gz sha256 $CHDB_SHA)"
        echo "REPS=$REPS SECS=$SECS QUIET_MAX=$QUIET_MAX"
    } > "$OUT/machine.txt" 2>&1
}
usage() {
    cat <<'U'
usage: run.sh PHASE...      (env.sh has the paths; REPS, SECS, FORCE=1, P1_SYSTEMS, P2_SYSTEMS override;
                            RESUME=1 skips timed steps that already ended with rc 0 in steps.log, LOADS_FROM=N
                            starts the loads of ch, qdb* and ts at run N: RESUME=1 LOADS_FROM=5 loads once)
  prep       inputs (not timed): mixed.*, spec.tsv, batches.tsv, per-sensor sets; under flock -s
  p1         sizes per kind of sensor           p2   the 100 M-row table, every server system
  p3         embedded (tsdb, DuckDB, chDB)      p4   Prometheus (backfill, remote write)
  p5         100 billion rows (only when named; not in "all")
  p6         the extra pass over p2's systems (only when named): one load each, the TWA batch classes (REPS runs, into
             p2/<sys>/p6/), every other class once with its peak memory (p2/<sys>/memsweep/); P6_SYSTEMS picks
             MANY_CLIENTS=16 CLASS_FILTER=b100 P6_SYSTEMS="tsdb qdb0 qdb1 qdb2" run.sh p6: only the extra-clients
             pass instead: one load each, the classes whose name contains CLASS_FILTER at that many clients, REPS
             runs, into p2/<sys>/p6/NAME.c16 (the 32-client rows and the sweep stay; marker p6/<sys>.c16.done)
  all        prep p1 p2 p3 p4
  smoke      every system end to end on tiny data, every check must pass (DBCMP=/var/tmp/dbcmp-smoke)
  checktest  the answer checker against a crafted data set and mutated answers
A phase or system with a done marker ($OUT/p<n>/<system>.done, $OUT/p<n>.done) is skipped; FORCE=1 runs it again.
U
}
done_mark() { [ "${FORCE:-0}" != 1 ] && [ -e "$1" ]; }
res_dir() { # PHASE SYS: the result directory of a system in phase 1 or 2
    case $1.$2 in 1.duck) echo "$OUT/p1/duckdb" ;; 2.qdb0) echo "$OUT/p2/qdb" ;; 2.qdb1) echo "$OUT/p2/qdb-parquet" ;;
        2.qdb2) echo "$OUT/p2/qdb-parquet-zstd" ;; ?.iotz) echo "$OUT/p$1/iot-zstd" ;; ?.tdz) echo "$OUT/p$1/td-zstd" ;;
        1.tdx) echo "$OUT/p1/td-xz" ;; 1.iotc) echo "$OUT/p1/iot-chimp-zstd" ;; 1.iotx) echo "$OUT/p1/iot-gorilla-lzma2" ;;
        1.iotcx) echo "$OUT/p1/iot-chimp-lzma2" ;; *) echo "$OUT/p$1/$2" ;; esac
}
aside() { # PHASE SYS: a rerun starts empty: the last attempt's runs, REFUSED.txt, dash.txt and loose.txt go to
          # $OUT/old/ (outside what tables.py and runstats read), so they are neither averaged nor reported again
    local d; d=$(res_dir "$1" "$2")
    [ "$1" = 2 ] && rm -f "$OUT/p6/$2.done"   # its p6/ and memsweep/ go with the directory: the extra pass is due again
    [ -d "$d" ] || return 0
    mkdir -p "$OUT/old" && mv "$d" "$OUT/old/p$1-$2.$(date +%m%d-%H%M%S)" && log "p$1 $2: earlier results moved to $OUT/old/"
}
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
        all) local_rest=(); for b in "$@"; do [ "$b" = all ] || local_rest+=("$b"); done
             set -- prep p1 p2 p3 p4 "${local_rest[@]}"; break ;;
    esac
done
for a in "$@"; do
    case $a in
        # bash, not the file's mode: a checkout without the executable bit once ran every phase with no inputs
        prep) flock -s "$LOCK" bash "$RDIR/prep.sh" > "$OUT/prep.log" 2>&1 || { log "prep failed (see $OUT/prep.log); later phases stop"; RC=1; PREP_FAILED=1; } ;;
        p[1-6]) [ "${PREP_FAILED:-0}" = 1 ] && { log "PHASE ${a#p} not run: prep failed"; RC=1; continue; }
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
