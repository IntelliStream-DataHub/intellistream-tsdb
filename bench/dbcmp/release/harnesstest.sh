#!/bin/bash
# harnesstest.sh: run.sh's death detection (oom_killed, ctr_died) against a stub podman and a fake cgroup tree, so
# no container is needed; and ts_many_probe against a stub qdrive. Prints one line per case and "harnesstest OK" or "harnesstest FAILED".
#   bash bench/dbcmp/release/harnesstest.sh
HERE=$(dirname "$(readlink -f "$0")")
T=$(mktemp -d) || exit 2
trap 'rm -rf "$T"' EXIT
# the functions under test, taken from run.sh as they are (sourcing run.sh would run it)
eval "$(sed -n -e '/^oom_killed() {/,/^}/p' -e '/^ctr_died() {/,/^}/p' -e '/^ts_many_probe() {/,/^}/p' "$HERE/run.sh")"
OUT=$T/out CGROOT=$T/cg
mkdir -p "$OUT"
ID=id1 OOMS=0
podman() { # inspect -f FORMAT NAME: the one container "rb-x", running, with cgroup /c-$ID
    case $3 in
        *State.Status*ExitCode*) echo "running exit code 0" ;;
        *State.Status*) echo running ;;
        *CgroupPath*) echo "/c-$ID" ;;
        *.Id*) echo "$ID" ;;
        *HostConfig.Memory*) echo $((48 * 2**30)) ;;
    esac
}
journalctl() { :; }
oom() { mkdir -p "$CGROOT/c-$ID"; echo "oom_kill $1" > "$CGROOT/c-$ID/memory.events"; }
fails=0
check() { # NAME WANT(0 = died) : ctr_died rb-x must return WANT
    ctr_died rb-x; local got=$?
    if [ "$got" = "$2" ]; then echo "ok   $1"; else echo "FAIL $1 (ctr_died returned $got, want $2)"; fails=$((fails + 1)); fi
}
oom 0;  check "a running container with no OOM kill is alive" 1
oom 15; check "15 OOM kills in its cgroup: died" 0
check "the same 15 again: alive (already reported)" 1
# the container is removed and made again under the same name: a new id and a new cgroup counting from 0
ID=id2
oom 0;  check "the new container with no OOM kill is alive" 1
oom 2;  check "the new container's first OOM kills are seen (the old count of 15 must not hide them)" 0
[ "$CTR_OOM" = 1 ] && echo "ok   CTR_OOM set" || { echo "FAIL CTR_OOM not set"; fails=$((fails + 1)); }

# ts_many_probe: a stub qdrive runs the class out of memory (one more OOM kill in the cgroup, rc 1) unless the connection
# asks for at most PAR_OK parallel workers per query; the probe must settle on the first setting of TS_MANY_TRY that fits
WORK=$T/work H=$T/h B=$T/b D=$T/d NHI=32 SECS=1 STEP_TIMEOUT=60 ID=id3
mkdir -p "$WORK" "$H" "$B" "$D"; echo 'pass' > "$H/mkbatch.py"; oom 0
cat > "$B/qdrive" << 'EOS'
#!/bin/bash
w=$(echo "$2" | sed -n 's/.*max_parallel_workers_per_gather=\([0-9]*\).*/\1/p')
[ "$w" -le "$PAR_OK" ] && { echo "1000 queries"; exit 0; }
n=$(awk '{print $2}' "$EVENTS"); echo "oom_kill $((n + 1))" > "$EVENTS"; echo "server closed the connection"; exit 1
EOS
chmod +x "$B/qdrive"
export EVENTS=$CGROOT/c-id3/memory.events
steplock() { :; }; stepunlock() { :; }; ctr_mem() { :; }; ctr_restart() { :; }; log() { :; }; fail() { echo "fail: $*"; }
limited() { local s=$1 o=$2; shift 2; "$@" > "$o" 2>&1; }
probe() { # PAR_OK TRY WANT-MANY_CONN-SUFFIX
    local MANY_CONN=; export PAR_OK=$1; TS_MANY_TRY=$2
    ts_many_probe "$D" "host=x options='-c TimeZone=UTC"
    if [ "${MANY_CONN##*options=}" = "$3" ]; then echo "ok   probe: PAR_OK=$1 tries '$2' -> ${3:-no setting}"
    else echo "FAIL probe: PAR_OK=$1 tries '$2' gave MANY_CONN '$MANY_CONN', want ...$3"; fails=$((fails + 1)); fi
}
probe 1 "2 1 0" "'-c TimeZone=UTC -c max_parallel_workers_per_gather=1'"
probe 2 "2 1 0" "'-c TimeZone=UTC -c max_parallel_workers_per_gather=2'"
probe -1 "2 1 0" ""
[ "$fails" = 0 ] && echo "harnesstest OK" || { echo "harnesstest FAILED"; exit 1; }
