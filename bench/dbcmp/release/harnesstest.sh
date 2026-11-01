#!/bin/bash
# harnesstest.sh: run.sh's death detection (oom_killed, ctr_died) against a stub podman and a fake cgroup tree, so
# no container is needed; ts_many_probe against a stub qdrive; mem_sample against fake memory files; the
# extra pass (phase6, tstep's labels and RESUME, NO_PC) and the TWA batch suites (twabsuite, tsdbtwab) against stubs. Prints one line per case and "harnesstest OK" or "harnesstest FAILED".
#   bash bench/dbcmp/release/harnesstest.sh
HERE=$(dirname "$(readlink -f "$0")")
T=$(mktemp -d) || exit 2
trap 'rm -rf "$T"' EXIT
# the functions under test, taken from run.sh as they are (sourcing run.sh would run it)
eval "$(sed -n -e '/^oom_killed() {/,/^}/p' -e '/^ctr_died() {/,/^}/p' -e '/^ts_many_probe() {/,/^}/p' -e '/^mem_sample() {/,/^}/p' "$HERE/run.sh")"
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

# mem_sample: the peaks of two servers, page cache included (memory.current) and without (anon + shmem + kernel)
M=$T/mem; mkdir -p "$M/a" "$M/b"
setmem() { echo "$2" > "$M/$1/memory.current"; printf 'anon %s\nfile 999999\nkernel %s\nshmem %s\n' "$3" "$4" "$5" > "$M/$1/memory.stat"; }
setmem a 1000 100 10 1; setmem b 5000 50 5 0
mem_sample "$M/out" rb-a "$M/a" tsdbd "$M/b" &
mp=$!
sleep 1.2; setmem a 9000 700 70 7; sleep 1.2; setmem a 2000 200 20 2; sleep 1.2
kill "$mp"; wait "$mp" 2> /dev/null
want=$'rb-a peak 9000 nocache 777\ntsdbd peak 5000 nocache 55'
if [ "$(cat "$M/out")" = "$want" ]; then echo "ok   mem_sample keeps each server's peaks"
else echo "FAIL mem_sample wrote: $(cat "$M/out" | tr '\n' ';') want: ${want//$'\n'/;}"; fails=$((fails + 1)); fi
# no server running (a step before any container starts): nothing to sample, no error under run.sh's set -u
if (set -u; mem_sample "$M/none") 2> "$M/none.err" && [ ! -s "$M/none.err" ] && [ ! -e "$M/none" ]; then
    echo "ok   mem_sample with no server returns quietly"
else echo "FAIL mem_sample with no server: $(cat "$M/none.err")"; fails=$((fails + 1)); fi

# the extra pass (p6) and the TWA batch suites, against stubs: where results go, how many runs, what is skipped
eval "$(sed -n -e '/^tstep() {/,/^}/p' -e '/^phase6() {/,/^}/p' -e '/^twabsuite() {/,/^}/p' -e '/^tsdbtwab() {/,/^}/p' \
    -e '/^dashnote() {/,/^}/p' -e '/^want_pc() {/p' -e '/^pc_pin() {/,/^}/p' -e '/^bsuite() {/,/^}/p' "$HERE/run.sh")"
expect() { # NAME CONDITION...: one case
    if eval "${*:2}"; then echo "ok   $1"; else echo "FAIL $1"; fails=$((fails + 1)); fi
}
declare -A DEAD_CLASS
quiet() { BUSY=0.1; }; mem_cgroups() { :; }; reset_quiet() { :; }; cleanup_all() { :; }; rmdata() { :; }; pc_client() { "$@"; }
X6=$T/p6; OUT=$X6/out WORK=$X6/work B=$X6/b TOOLS=$X6/tools WARM_SECS=1 NHI=32 SECS=1 REPS=5 STEP_TIMEOUT=60
mkdir -p "$OUT/p2/ch" "$WORK/q" "$B" "$TOOLS"
# tstep: the label prefix goes into steps.log; RESUME skips a step logged rc=0 whose output is there, and only then
LABEL_PFX="p6 " tstep "ch.x.c1 r1" "$OUT/a.1" -- echo hello
expect "tstep: p6's label in steps.log" "grep -qP '\tp6 ch.x.c1 r1\trc=0\t' '$OUT/steps.log'"
RESUME=1 LABEL_PFX="p6 " tstep "ch.x.c1 r1" "$OUT/a.1" -- false
expect "tstep: RESUME skips a p6 step that is done" "[ \$(grep -c 'ch.x.c1 r1' '$OUT/steps.log') = 1 ]"
RESUME=1 LABEL_PFX="p6 " tstep "ch.x.c1 r1" "$OUT/memsweep.a.1" -- echo again
expect "tstep: RESUME runs it when its output (another directory) is missing" "[ -s '$OUT/memsweep.a.1' ]"
# twabsuite: the batches go to the system's own directory with XREPS runs, also when called with DIR/memsweep
REL() { case $1 in has) [ "$2" != none ] ;; qb) [ "$2" = bad ] || echo "SELECT $3 $4" ;; checkb) echo "$2 $3.b$4 OK" ;; esac; }
reps() { echo "$1 $2 REPS=$REPS" >> "$X6/reps.log"; }
cat > "$B/qdrive" << 'EOS'
#!/bin/bash
echo "warm $*" >> "$P6LOG"
EOS
chmod +x "$B/qdrive"; export P6LOG=$X6/qdrive.log
untimed() { local o=$1; shift; "$@" > "$o" 2>&1; }
nocheck() { echo "nocheck $*" >> "$X6/reps.log"; }
TWA_WKINDS="twa1d twa7d" TWA_KS="10 100"
XREPS=5 REPS=1 twabsuite ts "$OUT/p2/ts/memsweep" pg conn
expect "twabsuite: 8 timings, into p2/ts, 5 runs each" \
    "[ \$(grep -c '^ts.twa[17]d.b1\(0\|00\).c\(1\|32\) $OUT/p2/ts REPS=5\$' '$X6/reps.log') = 8 ]"
expect "twabsuite: request files from rel.py qb" "[ \"\$(cat '$WORK/q/ts.twa7d.b100.txt')\" = 'SELECT twa7d 100' ]"
expect "twabsuite: checked into the system's directory" "grep -q 'ts twa1d.b10 OK' '$OUT/p2/ts/check-twa1d.b10.txt'"
REPS=5 twabsuite none "$OUT/p2/i2" http x; REPS=5 twabsuite none "$OUT/p2/i2" http x
expect "twabsuite: no query: one dash line per class, not repeated by a rerun" \
    "[ \$(grep -c '^twa1d.b10: no query in rel.py for none\$' '$OUT/p2/i2/dash.txt') = 1 ] && [ \$(wc -l < '$OUT/p2/i2/dash.txt') = 4 ]"
TWA_N=60 REPS=5 twabsuite ts2 "$OUT/p2/ts2" pg conn
expect "twabsuite: K above TWA_N windows (the smoke's 60) skipped, the others timed" \
    "! grep -q '^ts2.twa[17]d.b100' '$X6/reps.log' && [ \$(grep -c '^ts2.twa[17]d.b10.c' '$X6/reps.log') = 4 ]"
fail() { echo "fail: $*" >> "$X6/fails"; }
REPS=5 twabsuite bad "$OUT/p2/bad" http x
expect "twabsuite: rel.py qb failing is a FAIL, nothing timed" "[ \$(grep -c 'qb bad' '$X6/fails') = 4 ] && ! grep -q '^bad\.' '$X6/reps.log'"
# tsdbtwab: the one-request whole range (twa, twad, twal .b100.c1) and the windows at 1 and NHI connections
: > "$X6/reps.log"
cat > "$B/relq" << 'EOS'
#!/bin/bash
echo "relq $*" >> "$P6LOG"
EOS
chmod +x "$B/relq"; echo 'print("x twa OK")' > "$TOOLS/rel.py"
TWA_KINDS="twad twal" XREPS=5 REPS=1 tsdbtwab "$OUT/p2/tsdb/memsweep" merged.
expect "tsdbtwab: twa twad twal as merged.KIND.b100.c1, 5 runs, in p2/tsdb" \
    "[ \$(grep -c '^merged.twa[dl]\?.b100.c1 $OUT/p2/tsdb REPS=5\$' '$X6/reps.log') = 3 ]"
expect "tsdbtwab: the windows K at a time at 1 and 32 connections" \
    "[ \$(grep -c '^merged.twa[17]d.b1\(0\|00\).c\(1\|32\) $OUT/p2/tsdb REPS=5\$' '$X6/reps.log') = 8 ]"
expect "tsdbtwab: every form checked first (relq --batch dumps, then rel.py jsonl)" \
    "[ \$(grep -c -- '--batch' '$P6LOG') = 7 ] && [ \$(cat '$OUT'/p2/tsdb/check-merged.twa*.b*.txt | grep -c 'twa OK') = 7 ]"
# NO_PC: per-core batch suites return before running anything, pinning refuses
: > "$P6LOG"
NO_PC=1 PC=1 KS=1 PC_KS=1 FULLS=0 BCLASSES=raw bsuite ch ch "$OUT/p2/ch" http x; rc=$?
expect "NO_PC: bsuite with PC=1 runs nothing" "[ $rc = 0 ] && [ ! -s '$P6LOG' ]"
expect "NO_PC: pc_pin refuses" "! NO_PC=1 pc_pin rb-x"
# phase6: per system once, REPS=1 into memsweep with XREPS for the batches, no per core, labels "p6 "; done markers;
# a rerun moves an old sweep aside unless RESUME=1
fail() { echo "fail: $*" >> "$PH_DIR/FAILS"; }
PH_DIR=$OUT/p6; mkdir -p "$PH_DIR"
res_dir() { case $2 in qdb0) echo "$OUT/p2/qdb" ;; *) echo "$OUT/p2/$2" ;; esac; }
done_mark() { [ "${FORCE:-0}" != 1 ] && [ -e "$1" ]; }
runstats() { echo "runstats $1" >> "$X6/p6.log"; }
how_write() { echo "how_write $*" >> "$X6/p6.log"; }
p2_sys() { echo "p2_sys $1 sub=$SWEEP_SUB REPS=$REPS XREPS=$XREPS LOADS_FROM=$LOADS_FROM NO_PC=$NO_PC pfx=$LABEL_PFX" >> "$X6/p6.log"
    mkdir -p "$(res_dir 2 "$1")$SWEEP_SUB"; echo x > "$(res_dir 2 "$1")$SWEEP_SUB/load.1"; [ "$1" != broken ] || fail "broken"; }
P6_SYSTEMS="tsdb qdb0 broken" REPS=5 LOADS_FROM=5 phase6
expect "phase6: each system once with the sweep's settings" \
    "[ \$(grep -c 'sub=/memsweep REPS=1 XREPS=5 LOADS_FROM=1 NO_PC=1 pfx=p6 \$' '$X6/p6.log') = 3 ]"
expect "phase6: runstats on the system's own directory" "grep -qx 'runstats $OUT/p2/qdb' '$X6/p6.log'"
expect "phase6: how.json for a system without one (phase 2 not run)" "grep -qx 'how_write 2 qdb0' '$X6/p6.log'"
expect "phase6: done markers only for systems without failures" "[ -e '$OUT/p6/tsdb.done' ] && [ -e '$OUT/p6/qdb0.done' ] && [ ! -e '$OUT/p6/broken.done' ]"
: > "$X6/p6.log"
P6_SYSTEMS="tsdb qdb0 broken" RESUME=1 phase6
expect "phase6: RESUME=1 skips done systems and keeps the sweep" "[ \$(grep -c p2_sys '$X6/p6.log') = 1 ] && [ -e '$OUT/p2/broken/memsweep/load.1' ] && [ ! -d '$OUT/old' ]"
P6_SYSTEMS="broken" phase6
expect "phase6: a rerun without RESUME moves the old sweep to OUT/old" "ls -d '$OUT'/old/p6-broken-memsweep.* > /dev/null 2>&1"
[ "$fails" = 0 ] && echo "harnesstest OK" || { echo "harnesstest FAILED"; exit 1; }
