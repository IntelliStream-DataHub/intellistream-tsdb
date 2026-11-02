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
want=$'rb-a peak 9000 nocache 777 start 1000 startnocache 111\ntsdbd peak 5000 nocache 55 start 5000 startnocache 55'
if [ "$(cat "$M/out")" = "$want" ]; then echo "ok   mem_sample keeps each server's peaks and its memory at the start"
else echo "FAIL mem_sample wrote: $(cat "$M/out" | tr '\n' ';') want: ${want//$'\n'/;}"; fails=$((fails + 1)); fi
# no server running (a step before any container starts): nothing to sample, no error under run.sh's set -u
if (set -u; mem_sample "$M/none") 2> "$M/none.err" && [ ! -s "$M/none.err" ] && [ ! -e "$M/none" ]; then
    echo "ok   mem_sample with no server returns quietly"
else echo "FAIL mem_sample with no server: $(cat "$M/none.err")"; fails=$((fails + 1)); fi

# the extra pass (p6) and the TWA batch suites, against stubs: where results go, how many runs, what is skipped
eval "$(sed -n -e '/^tstep() {/,/^}/p' -e '/^phase6() {/,/^}/p' -e '/^twabsuite() {/,/^}/p' -e '/^tsdbtwab() {/,/^}/p' \
    -e '/^dashnote() {/,/^}/p' -e '/^want_pc() {/p' -e '/^pc_pin() {/,/^}/p' -e '/^bsuite() {/,/^}/p' \
    -e '/^twab_dir() {/,/^}/p' -e '/^twa_single_failed() {/,/^}/p' -e '/^nocheck() {/,/^}/p' -e '/^aside() {/,/^}/p' \
    -e '/^tsdbsuite() {/,/^}/p' "$HERE/run.sh")"
expect() { # NAME CONDITION...: one case
    if eval "${*:2}"; then echo "ok   $1"; else echo "FAIL $1"; fails=$((fails + 1)); fi
}
declare -A DEAD_CLASS
quiet() { BUSY=0.1; }; mem_cgroups() { :; }; reset_quiet() { :; }; cleanup_all() { :; }; rmdata() { :; }; pc_client() { "$@"; }
X6=$T/p6; OUT=$X6/out WORK=$X6/work B=$X6/b TOOLS=$X6/tools WARM_SECS=1 NHI=32 SECS=1 REPS=5 STEP_TIMEOUT=60 CHECK_TIMEOUT=300
mkdir -p "$OUT/p2/ch" "$WORK/q" "$B" "$TOOLS"
# tstep: the label prefix goes into steps.log; RESUME skips a step logged rc=0 whose output is there, and only then
LABEL_PFX="p6 " tstep "ch.x.c1 r1" "$OUT/a.1" -- echo hello
expect "tstep: p6's label in steps.log" "grep -qP '\tp6 ch.x.c1 r1\trc=0\t' '$OUT/steps.log'"
RESUME=1 LABEL_PFX="p6 " tstep "ch.x.c1 r1" "$OUT/a.1" -- false
expect "tstep: RESUME skips a p6 step that is done" "[ \$(grep -c 'ch.x.c1 r1' '$OUT/steps.log') = 1 ]"
RESUME=1 LABEL_PFX="p6 " tstep "ch.x.c1 r1" "$OUT/memsweep.a.1" -- echo again
expect "tstep: RESUME runs it when its output (another directory) is missing" "[ -s '$OUT/memsweep.a.1' ]"
# twabsuite: in p6 (DIR/memsweep) the batches go to the system's p6/ with XREPS runs, named by the variant (the result
# directory); a kind is batched only when its single windows passed their check in this load
REL() { case $1 in has) [ "$2" != none ] ;; qb) [ "$2" = bad ] || echo "SELECT $3 $4" ;; checkb) echo "$2 $3.b$4 OK" ;; esac; }
reps() { echo "$1 $2 REPS=$REPS" >> "$X6/reps.log"; }
cat > "$B/qdrive" << 'EOS'
#!/bin/bash
echo "warm $*" >> "$P6LOG"
EOS
chmod +x "$B/qdrive"; export P6LOG=$X6/qdrive.log
untimed() { local o=$1; shift; "$@" > "$o" 2>&1; }
single_ok() { local s=$1 d=$2 k; mkdir -p "$d"; for k in twa1d twa7d; do echo "$s $k OK" > "$d/check-$k.txt"; done; }
TWA_WKINDS="twa1d twa7d" TWA_KS="10 100"
single_ok ts "$OUT/p2/ts/memsweep"
XREPS=5 REPS=1 twabsuite ts "$OUT/p2/ts/memsweep" pg conn
expect "twabsuite: 8 timings, into p2/ts/p6, 5 runs each" \
    "[ \$(grep -c '^ts.twa[17]d.b1\(0\|00\).c\(1\|32\) $OUT/p2/ts/p6 REPS=5\$' '$X6/reps.log') = 8 ]"
expect "twabsuite: request files from rel.py qb" "[ \"\$(cat '$WORK/q/ts.twa7d.b100.txt')\" = 'SELECT twa7d 100' ]"
expect "twabsuite: checked into p6/" "grep -q 'ts twa1d.b10 OK' '$OUT/p2/ts/p6/check-twa1d.b10.txt'"
single_ok qdb "$OUT/p2/qdb-parquet"
REPS=5 twabsuite qdb "$OUT/p2/qdb-parquet" pg conn
expect "twabsuite: a variant's steps and files are named by its directory, rel.py's system for the queries" \
    "[ \$(grep -c '^qdb-parquet.twa1d.b10.c1 $OUT/p2/qdb-parquet REPS=5\$' '$X6/reps.log') = 1 ] && ! grep -q '^qdb\.' '$X6/reps.log' && [ -s '$WORK/q/qdb.twa1d.b10.txt' ]"
REPS=5 twabsuite none "$OUT/p2/i2" http x; REPS=5 twabsuite none "$OUT/p2/i2" http x
expect "twabsuite: no query: one dash line per class, not repeated by a rerun" \
    "[ \$(grep -c '^twa1d.b10: no query in rel.py for none\$' '$OUT/p2/i2/dash.txt') = 1 ] && [ \$(wc -l < '$OUT/p2/i2/dash.txt') = 4 ]"
single_ok ts2 "$OUT/p2/ts2"
TWA_N=60 REPS=5 twabsuite ts2 "$OUT/p2/ts2" pg conn
expect "twabsuite: K above TWA_N windows skipped, the others timed" \
    "! grep -q '^ts2.twa[17]d.b100' '$X6/reps.log' && [ \$(grep -c '^ts2.twa[17]d.b10.c' '$X6/reps.log') = 4 ]"
(set -u; unset TWA_N; TWA_KS=2000 REPS=5 twabsuite ts2 "$OUT/p2/ts2" pg conn) 2> "$X6/setu.err"
expect "twabsuite: K above the default 1000 windows under set -u without TWA_N" "[ ! -s '$X6/setu.err' ]"
mkdir -p "$OUT/p2/i3"; echo "i3 twa1d OK" > "$OUT/p2/i3/check-twa1d.txt"
printf 'partial\nSTEP TIMEOUT after 3600 s\n' > "$OUT/p2/i3/check-twa7d.txt"
REPS=5 twabsuite i3 "$OUT/p2/i3" http x
expect "twabsuite: a kind whose single-window check timed out is not batched; dash says it timed out" \
    "! grep -q '^i3.twa7d' '$X6/reps.log' && grep -qx 'twa7d.b10: not run: the single-window check of twa7d timed out after 300s' '$OUT/p2/i3/dash.txt' && [ \$(grep -c '^i3.twa1d.b10.c' '$X6/reps.log') = 2 ]"
mkdir -p "$OUT/p2/td"; echo "MISMATCH td twa1d" > "$OUT/p2/td/check-twa1d.txt"
REPS=5 twabsuite td "$OUT/p2/td" http x
expect "twabsuite: a kind whose single windows disagreed (or never ran) is not batched" \
    "! grep -q '^td\.' '$X6/reps.log' && grep -q '^twa1d.b100: not run: the single-window check of twa1d did not pass' '$OUT/p2/td/dash.txt' && grep -q '^twa7d.b10: not run: the single-window check of twa7d did not run' '$OUT/p2/td/dash.txt'"
log() { :; }
mkdir -p "$OUT/p2/nc"; echo 'STEP TIMEOUT after 300 s' > "$OUT/p2/nc/check-twa1d.b10.txt"
nocheck nc twa1d.b10 "$OUT/p2/nc" 1
expect "nocheck: a timed-out check is said to have timed out, not to differ" \
    "grep -qx 'twa1d.b10: not timed, its answer check timed out after 300s (see check-twa1d.b10.txt)' '$OUT/p2/nc/dash.txt'"
fail() { echo "fail: $*" >> "$X6/fails"; }
single_ok bad "$OUT/p2/bad"
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
expect "tsdbtwab: twa twad twal as merged.KIND.b100.c1, 5 runs, in p2/tsdb/p6" \
    "[ \$(grep -c '^merged.twa[dl]\?.b100.c1 $OUT/p2/tsdb/p6 REPS=5\$' '$X6/reps.log') = 3 ]"
expect "tsdbtwab: the windows K at a time at 1 and 32 connections" \
    "[ \$(grep -c '^merged.twa[17]d.b1\(0\|00\).c\(1\|32\) $OUT/p2/tsdb/p6 REPS=5\$' '$X6/reps.log') = 8 ]"
expect "tsdbtwab: every form checked first (relq --batch dumps, then rel.py jsonl)" \
    "[ \$(grep -c -- '--batch' '$P6LOG') = 7 ] && [ \$(cat '$OUT'/p2/tsdb/p6/check-merged.twa*.b*.txt | grep -c 'twa OK') = 7 ]"
# warm-ups have the step time limit: one past it is a FAIL and its class is not timed
: > "$X6/reps.log"; : > "$X6/fails"
limited() { local s=$1 o=$2; shift 2; echo "STEP TIMEOUT" > "$o"; return 124; }
CLASSES="raw agg1d" tsdbsuite tsdbq "$OUT/p2/tsdb" "1 32"
expect "tsdbsuite: a warm-up past the time limit fails its class, nothing timed" \
    "[ \$(grep -c 'warm-up: STEP TIMEOUT' '$X6/fails') = 2 ] && [ ! -s '$X6/reps.log' ]"
TWA_KINDS="" TWA_KS=10 tsdbtwab "$OUT/p2/tsdb" ""
expect "tsdbtwab: a relq warm-up past the time limit fails, nothing timed" \
    "grep -q 'twa1d.b10 warm-up failed' '$X6/fails' && ! grep -q 'twa1d.b10.c' '$X6/reps.log'"
limited() { local s=$1 o=$2; shift 2; "$@" > "$o" 2>&1; }
# NO_PC: per-core batch suites return before running anything, pinning refuses
: > "$P6LOG"
NO_PC=1 PC=1 KS=1 PC_KS=1 FULLS=0 BCLASSES=raw bsuite ch ch "$OUT/p2/ch" http x; rc=$?
expect "NO_PC: bsuite with PC=1 runs nothing" "[ $rc = 0 ] && [ ! -s '$P6LOG' ]"
expect "NO_PC: pc_pin refuses" "! NO_PC=1 pc_pin rb-x"
# phase6: per system once, REPS=1 into memsweep with XREPS for the batches, no per core, labels "p6 "; a done marker
# when the system's pass reached its end (failed classes are results); a rerun moves p6/ and memsweep/ aside unless
# RESUME=1
fail() { echo "fail: $*" >> "$PH_DIR/FAILS"; }
PH_DIR=$OUT/p6; mkdir -p "$PH_DIR"
res_dir() { case $2 in qdb0) echo "$OUT/p2/qdb" ;; *) echo "$OUT/p2/$2" ;; esac; }
done_mark() { [ "${FORCE:-0}" != 1 ] && [ -e "$1" ]; }
runstats() { echo "runstats $1" >> "$X6/p6.log"; }
how_write() { echo "how_write $*" >> "$X6/p6.log"; }
how_p6() { echo "how_p6 $*" >> "$X6/p6.log"; }
p2_sys() { echo "p2_sys $1 sub=$SWEEP_SUB REPS=$REPS XREPS=$XREPS LOADS_FROM=$LOADS_FROM NO_PC=$NO_PC pfx=$LABEL_PFX" >> "$X6/p6.log"
    local d; d=$(res_dir 2 "$1"); mkdir -p "$d$SWEEP_SUB" "$d/p6"; echo x > "$d$SWEEP_SUB/load.1"; echo x > "$d/p6/c.1"
    [ "$1" != refusing ] || fail "a class refused"; [ "$1" != broken ]; }
P6_SYSTEMS="tsdb qdb0 refusing broken" REPS=5 LOADS_FROM=5 phase6
expect "phase6: each system once with the sweep's settings" \
    "[ \$(grep -c 'sub=/memsweep REPS=1 XREPS=5 LOADS_FROM=1 NO_PC=1 pfx=p6 \$' '$X6/p6.log') = 4 ]"
expect "phase6: runstats on the system's p6/" "grep -qx 'runstats $OUT/p2/qdb/p6' '$X6/p6.log'"
expect "phase6: how.json for a system without one (phase 2 not run), and its p6 block" \
    "grep -qx 'how_write 2 qdb0' '$X6/p6.log' && grep -qx 'how_p6 $OUT/p2/qdb 5' '$X6/p6.log'"
expect "phase6: done when the pass reached the system's end (a refused class too), not after a failed load" \
    "[ -e '$OUT/p6/tsdb.done' ] && [ -e '$OUT/p6/qdb0.done' ] && [ -e '$OUT/p6/refusing.done' ] && [ ! -e '$OUT/p6/broken.done' ]"
: > "$X6/p6.log"; rm -rf "$OUT/old"   # the first pass moved the suites' earlier test output aside
P6_SYSTEMS="tsdb qdb0 refusing broken" RESUME=1 phase6
expect "phase6: RESUME=1 skips done systems and keeps the sweep and p6/" \
    "[ \$(grep -c p2_sys '$X6/p6.log') = 1 ] && [ -e '$OUT/p2/broken/memsweep/load.1' ] && [ ! -d '$OUT/old' ]"
P6_SYSTEMS="broken" phase6
expect "phase6: a rerun without RESUME moves the old p6/ and memsweep/ to OUT/old" \
    "ls -d '$OUT'/old/p6-broken-memsweep.* '$OUT'/old/p6-broken-p6.* > /dev/null 2>&1"
mkdir -p "$OUT/p2/tsdb"
aside 2 tsdb
expect "aside: a phase-2 rerun of a system makes its extra pass due again" "[ ! -e '$OUT/p6/tsdb.done' ] && [ -e '$OUT/p6/qdb0.done' ]"
# untimed: a check that exits 1 on a mismatch (rel.py jsonl) is named by its MISMATCH line in FAILS, as when it exited 0
eval "$(sed -n -e '/^untimed() {/,/^}/p' "$HERE/run.sh")"
fail() { echo "fail: $*" >> "$X6/ufails"; }
UNTIMED_TIMEOUT=60 CHECK_TIMEOUT=60 QCTR= untimed "$X6/check-u.txt" sh -c 'echo "x twa MISMATCH"; exit 1'; rc=$?
expect "untimed: a mismatch exiting 1 fails with its MISMATCH line" "[ $rc = 1 ] && grep -q 'answer check: x twa MISMATCH' '$X6/ufails'"
UNTIMED_TIMEOUT=60 CHECK_TIMEOUT=60 QCTR= untimed "$X6/check-v.txt" sh -c 'echo "x twa OK"'; rc=$?
expect "untimed: a passing check passes" "[ $rc = 0 ] && [ \$(wc -l < '$X6/ufails') = 1 ]"
# rel.py REFUSED (exit 4): a result, passed on as 4 (not a FAIL, not "answers differ"); nocheck records the message
UNTIMED_TIMEOUT=60 CHECK_TIMEOUT=60 QCTR= untimed "$X6/check-r.txt" sh -c 'echo "REFUSED: HTTP 400: global RSS memory limit exceeded"; exit 4'; rc=$?
expect "untimed: a refused check returns 4 and is no FAIL" "[ $rc = 4 ] && [ \$(wc -l < '$X6/ufails') = 1 ]"
mkdir -p "$X6/nr"; cp "$X6/check-r.txt" "$X6/nr/check-r.txt"
nocheck x r "$X6/nr" 4
expect "nocheck: a refusal goes to dash.txt and REFUSED.txt with the server's message" \
    "grep -q 'refused the query: HTTP 400: global RSS' '$X6/nr/dash.txt' && grep -q 'check-r: refused by the server: HTTP 400' '$X6/nr/REFUSED.txt'"
# tools/scale-bench-ch.sh: the mean is wall time over the queries, not 1000 / clickhouse-benchmark's rounded QPS
# (0.009 q/s printed as 111111.111 ms); the server's query_log mean and median follow
SB=$T/sbch; mkdir -p "$SB/sql"; printf 'SELECT 1 FROM big\nSELECT 2 FROM big\n' > "$SB/sql/agg_all.sql"
cat > "$SB/ch" << 'EOS'
#!/bin/sh
case $1 in
benchmark) cat > /dev/null; sleep 0.3; printf 'Loaded 2 queries.\n127.0.0.1:9000, queries 2, QPS: 0.009, RPS: 1.0\n50%%\t0.150 sec.\n' ;;
client) printf '2\t149.500\t149.250\n' ;;
esac
EOS
chmod +x "$SB/ch"
l=$(sh "$HERE/../../../tools/scale-bench-ch.sh" "$SB/sql" big "$SB/ch")
m=$(echo "$l" | awk '{ print $5 }')
expect "scale-bench-ch: mean from wall time (about 150 ms), not 1000 / rounded QPS" \
    "awk -v m='$m' 'BEGIN { exit !(m > 140 && m < 400) }' && echo '$l' | grep -q 'p50    150.000 ms  server: 2 q, mean 149.500 ms, median 149.250 ms'"
# qdrive pg: a query on a connection the server dropped prints why (no PGresult: the connection's message), not ""
QD=${QDRIVE:-$HERE/../../../build/dbcmp/qdrive}
if [ -x "$QD" ]; then
    PF=$T/pgport; rm -f "$PF"
    python3 -I "$HERE/pgdrop.py" "$PF" & pd=$!
    for i in $(seq 50); do [ -s "$PF" ] && break; sleep 0.1; done
    echo "SELECT 1" > "$T/pg.q"
    "$QD" pg "host=127.0.0.1 port=$(cat "$PF") user=x dbname=x sslmode=disable connect_timeout=5" "$T/pg.q" 1 1 > "$T/pg.out" 2>&1
    rc=$?; [ "$pd" -gt 1 ] && wait "$pd" 2> /dev/null
    expect "qdrive pg: a dropped connection is a failure that names its reason" \
        "[ $rc != 0 ] && grep -q 'query failed: status [0-9]*: [^ ]' '$T/pg.out'"
else
    echo "skip qdrive pg case (no $QD)"
fi
# the variant name from a result dir: the system's dir, its memsweep/ or (TWA_RETIME) its p6/ all name the variant
for dd in /x/p2/qdb-parquet /x/p2/qdb-parquet/memsweep /x/p2/qdb-parquet/p6; do
    V=${dd%/memsweep}; V=$(basename "${V%/p6}")
    expect "variant of $dd is qdb-parquet" "[ '$V' = qdb-parquet ]"
done
expect "run.sh derives every variant name the same way (no basename of only %/memsweep left)" \
    "! grep -n 'basename \"\\\${[A-Z]*%/memsweep}\"' '$HERE/run.sh'"
[ "$fails" = 0 ] && echo "harnesstest OK" || { echo "harnesstest FAILED"; exit 1; }
