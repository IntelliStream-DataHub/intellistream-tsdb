#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
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
echo "$4" >> "$CLIENTS"; echo "$3" >> "${QLOG:-/dev/null}"
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
# the extra-clients pass probes at its own client count (MANY_CLIENTS), and says so in many-parallel.txt
export CLIENTS=$T/clients.log; : > "$CLIENTS"
probe 2 "2 1 0" "'-c TimeZone=UTC -c max_parallel_workers_per_gather=2'"
MANY_CLIENTS=16 probe 1 "2 1 0" "'-c TimeZone=UTC -c max_parallel_workers_per_gather=1'"
expect() { # NAME CONDITION...: one case (run.sh's functions are defined below; this one is needed here)
    if eval "${*:2}"; then echo "ok   $1"; else echo "FAIL $1"; fails=$((fails + 1)); fi
}
expect "probe: without MANY_CLIENTS it runs NHI (32) clients, with it MANY_CLIENTS (16)" \
    "[ \"\$(sort -u '$CLIENTS' | tr '\\n' ' ')\" = '16 32 ' ] && [ \"\$(tail -1 '$CLIENTS')\" = 16 ]"
expect "probe: many-parallel.txt names the client count of the pass" \
    "grep -q 'max_parallel_workers_per_gather=1 in the 16-client classes' '$D/many-parallel.txt'"
# the TWA batch classes probe on the toolkit container with their own request file and their own record (tag .twa): the
# container asked about is the one given, the file sent is the one given, and the aggregate probe's files stay as they are
export QLOG=$T/q.log CTRLOG=$T/ctr.log; : > "$QLOG"; : > "$CTRLOG"
ctr_mem() { echo "$1" >> "$CTRLOG"; }
probe_twa() { # PAR_OK WANT-MANY_CONN-SUFFIX
    local MANY_CONN=; export PAR_OK=$1; TS_MANY_TRY="2 1 0" MANY_CLIENTS=16 \
        ts_many_probe "$D" "host=x options='-c TimeZone=UTC" rb-tsha "$T/twa.txt" .twa
    [ "${MANY_CONN##*options=}" = "$2" ]
}
probe_twa 1 "'-c TimeZone=UTC -c max_parallel_workers_per_gather=1'"; rc=$?
expect "probe with a tag: the TWA batch's setting is found on the container and the request file it is given" \
    "[ $rc = 0 ] && [ \"\$(sort -u '$CTRLOG')\" = rb-tsha ] && [ \"\$(sort -u '$QLOG')\" = '$T/twa.txt' ]"
expect "probe with a tag: its own record (many-parallel.twa.txt, probe.twa.parN.txt), the aggregate probe's files untouched" \
    "grep -q 'max_parallel_workers_per_gather=1 in the 16-client TWA batch classes' '$D/many-parallel.twa.txt' \
     && [ -s '$D/probe.twa.par2.txt' ] && [ -s '$D/probe.twa.par1.txt' ] \
     && grep -q 'max_parallel_workers_per_gather=1 in the 16-client classes' '$D/many-parallel.txt'"
probe_twa -1 ""; rc=$?
expect "probe with a tag: no setting fits: the default is recorded as the TWA batches' own, MANY_CONN stays empty" \
    "[ $rc = 0 ] && grep -q 'default in every TWA batch class: out of memory' '$D/many-parallel.twa.txt' \
     && grep -q 'max_parallel_workers_per_gather=1 in the 16-client classes' '$D/many-parallel.txt'"
ctr_mem() { :; }

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
    -e '/^tsdbsuite() {/,/^}/p' -e '/^class_wanted() {/,/^}/p' -e '/^tsdbbsuite() {/,/^}/p' \
    -e '/^phase6many() {/,/^}/p' -e '/^tree_commit() {/,/^}/p' -e '/^chbsuite() {/,/^}/p' -e '/^twasuite() {/,/^}/p' \
    -e '/^p2_ch() {/,/^}/p' -e '/^p2_ts() {/,/^}/p' -e '/^agg_wanted() {/,/^}/p' -e '/^p2_tsdb() {/,/^}/p' \
    -e '/^p2_qdb() {/,/^}/p' -e '/^twa_wanted() {/,/^}/p' -e '/^ts_plain_pass() {/,/^}/p' -e '/^ts_twa_probe() {/,/^}/p' \
    "$HERE/run.sh")"
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
# MANY_CLIENTS (the extra-clients pass): bsuite and tsdbbsuite time only the CLASS_FILTER classes, at that many clients,
# XREPS runs each, and not the whole-range runs; the file names carry .c16; without the option nothing changes
: > "$X6/reps.log"; mkdir -p "$OUT/p2/qdb/p6"
MANY_CLIENTS=16 CLASS_FILTER=b100 XREPS=5 REPS=1 KS="1 10 100" FULLS="0 3600" BCLASSES="raw agg1d" bsuite qdb qdb "$OUT/p2/qdb/p6" pg conn
got=$(cat "$X6/reps.log")
want=$(printf '%s\n' "qdb.raw.b100.c16 $OUT/p2/qdb/p6 REPS=5" "qdb.agg1d.b100.c16 $OUT/p2/qdb/p6 REPS=5")
expect "MANY_CLIENTS: bsuite times the b100 classes at 16 clients, 5 runs each, no whole-range runs" '[ "$got" = "$want" ]'
: > "$X6/reps.log"
FULLS="" BCLASSES=raw KS=10 REPS=2 bsuite qdb qdb "$OUT/p2/qdb" pg conn
got=$(cat "$X6/reps.log")
want=$(printf '%s\n' "qdb.raw.b10.c1 $OUT/p2/qdb REPS=2" "qdb.raw.b10.c32 $OUT/p2/qdb REPS=2")
expect "MANY_CLIENTS: bsuite without the option still times 1 and NHI clients" '[ "$got" = "$want" ]'
mkdir -p "$X6/tb"; printf '#!/bin/bash\necho "tsdbq $*" >> "$P6LOG"\n' > "$X6/tb/tsdbq"; chmod +x "$X6/tb/tsdbq"
tsdb_has_batch() { true; }; mkdir -p "$OUT/p2/tsdb/p6"
: > "$X6/reps.log"
TB=$X6/tb MANY_CLIENTS=16 CLASS_FILTER=b100 XREPS=5 REPS=1 KS="1 10 100" BCLASSES="agg1d point" tsdbbsuite tsdbq "$OUT/p2/tsdb/p6"
got=$(cat "$X6/reps.log")
want=$(printf '%s\n' "tsdbq.agg1d.b100.c16 $OUT/p2/tsdb/p6 REPS=5" "tsdbq.point.b100.c16 $OUT/p2/tsdb/p6 REPS=5")
expect "MANY_CLIENTS: tsdbbsuite times the b100 classes at 16 clients, 5 runs each, no full.bK runs" '[ "$got" = "$want" ]'
: > "$X6/reps.log"
mkdir -p "$X6/h"; echo "print(10)" > "$X6/h/batches.py"
H=$X6/h TB=$X6/tb KS="1 10" BCLASSES=point REPS=2 tsdbbsuite tsdbq "$OUT/p2/tsdb"
got=$(cat "$X6/reps.log")
want=$(printf '%s\n' "tsdbq.full.b1 $OUT/p2/tsdb REPS=2" "tsdbq.point.c1 $OUT/p2/tsdb REPS=2" "tsdbq.point.c32 $OUT/p2/tsdb REPS=2" \
    "tsdbq.full.b10 $OUT/p2/tsdb REPS=2" "tsdbq.point.b10.c1 $OUT/p2/tsdb REPS=2" "tsdbq.point.b10.c32 $OUT/p2/tsdb REPS=2")
expect "MANY_CLIENTS: tsdbbsuite without the option still times the whole range, 1 and NHI clients" '[ "$got" = "$want" ]'
# the TWA batch classes in the extra-clients pass (tsdb and QuestDB): CLASS_FILTER may list substrings, "twa1d.b100
# twa7d.b100" picks exactly the two batches of 100 windows (the substring "twa" would take the batches of 10 as well)
cw() { CLASS_FILTER="$1" class_wanted "$2" && echo yes || echo no; }
expect "class_wanted: a list of substrings picks exactly the classes it names" \
    '[ "$(cw "twa1d.b100 twa7d.b100" twa1d.b100)$(cw "twa1d.b100 twa7d.b100" twa7d.b100)$(cw "twa1d.b100 twa7d.b100" twa1d.b10)$(cw "twa1d.b100 twa7d.b100" raw.b100)" = yesyesnono ]'
expect "class_wanted: one substring, and no filter at all, as before" \
    '[ "$(cw b100 raw.b100)$(cw b100 raw.b10)$(cw "" anything)" = yesnoyes ]'
expect "agg_wanted: no aggregate batch class under the TWA filter, some under b100 or no filter" \
    '[ "$(KS="1 10 100" BCLASSES="raw agg1d" CLASS_FILTER="twa1d.b100 twa7d.b100" agg_wanted && echo y || echo n)$(KS="1 10 100" BCLASSES="raw agg1d" CLASS_FILTER=b100 agg_wanted && echo y || echo n)$(KS="1 10 100" BCLASSES="raw agg1d" CLASS_FILTER= agg_wanted && echo y || echo n)" = nyy ]'
tw() { CLASS_FILTER="$1" twa_wanted && echo y || echo n; }
expect "twa_wanted: the TWA batch classes under the TWA filter and b100 or no filter, none under an aggregate filter" \
    '[ "$(tw "twa1d.b100 twa7d.b100")$(tw b100)$(tw "")$(tw agg7d.b100)$(tw twa7d.b100)" = yyyny ]'
: > "$X6/reps.log"; : > "$P6LOG"
TWA_KINDS="twad twal" TWA_WKINDS="twa1d twa7d" TWA_KS="10 100" MANY_CLIENTS=16 CLASS_FILTER="twa1d.b100 twa7d.b100" XREPS=3 REPS=1 \
    tsdbtwab "$OUT/p2/tsdb/p6" ""
got=$(cat "$X6/reps.log")
want=$(printf '%s\n' "twa1d.b100.c16 $OUT/p2/tsdb/p6 REPS=3" "twa7d.b100.c16 $OUT/p2/tsdb/p6 REPS=3")
expect "MANY_CLIENTS: tsdbtwab times the TWA window batches of 100 at 16 clients, 3 runs each; no whole-range request, no 1 or 32 clients" \
    '[ "$got" = "$want" ]'
expect "MANY_CLIENTS: tsdbtwab checks only those two forms first (relq --batch 100)" \
    '[ "$(grep -c -- "--batch" "$P6LOG")" = 2 ] && [ "$(grep -c -- "--batch 100\$" "$P6LOG")" = 2 ]'
: > "$X6/reps.log"
TWA_KINDS="twad twal" TWA_WKINDS="twa1d" TWA_KS="10" XREPS=3 REPS=1 tsdbtwab "$OUT/p2/tsdb/p6" ""
expect "MANY_CLIENTS: tsdbtwab without the option still times the whole range and 1 and 32 clients" \
    '[ "$(wc -l < "$X6/reps.log")" = 5 ] && grep -q "^twa1d.b10.c32 " "$X6/reps.log" && grep -q "^twal.b100.c1 " "$X6/reps.log"'
# a tree made by git archive has no repository: its commit is the file COMMIT
mkdir -p "$X6/arch" "$X6/bare"; echo "0123456789abcdef0123456789abcdef01234567 main" > "$X6/arch/COMMIT"
expect "tree_commit: a tree without a repository gives the commit in its COMMIT file" \
    '[ "$(tree_commit "$X6/arch")" = 0123456789abcdef0123456789abcdef01234567 ]'
expect "tree_commit: no repository and no COMMIT file says so" 'tree_commit "$X6/bare" | grep -q "^unknown"'
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
# phase6 with MANY_CLIENTS: the extra-clients pass; per system one load into its p6/ (SWEEP_SUB=/p6, XREPS runs), nothing
# moved aside (the sweep and the 32-client rows stay), a done marker of its own (.c16.done), a system it does not
# implement is a FAIL; how.json gets the pass added
: > "$X6/p6.log"; rm -rf "$OUT/old" "$PH_DIR/FAILS"
mkdir -p "$OUT/p2/qdb/memsweep"; echo keep > "$OUT/p2/qdb/memsweep/load.1"
MANY_CLIENTS=16 CLASS_FILTER=b100 P6_SYSTEMS="tsdb qdb0 ch ts i2" REPS=5 phase6
expect "phase6 MANY_CLIENTS: each implemented system once (tsdb, qdb0, ch, ts), into its p6/, XREPS runs, one load" \
    "[ \$(grep -c 'sub=/p6 REPS=1 XREPS=5 LOADS_FROM=1 NO_PC=1 pfx=p6 \$' '$X6/p6.log') = 4 ] && grep -q '^p2_sys tsdb ' '$X6/p6.log' && grep -q '^p2_sys qdb0 ' '$X6/p6.log' && grep -q '^p2_sys ch ' '$X6/p6.log' && grep -q '^p2_sys ts ' '$X6/p6.log'"
expect "phase6 MANY_CLIENTS: its own done markers, not the sweep's" \
    "[ -e '$OUT/p6/tsdb.c16.done' ] && [ -e '$OUT/p6/qdb0.c16.done' ] && [ -e '$OUT/p6/ch.c16.done' ] && [ -e '$OUT/p6/ts.c16.done' ] && [ ! -e '$OUT/p6/i2.c16.done' ] && [ ! -e '$OUT/p6/tsdb.done' ]"
expect "phase6 MANY_CLIENTS: a system without the pass fails" "grep -q 'p6 i2: MANY_CLIENTS is implemented for' '$PH_DIR/FAILS' && ! grep -q 'p2_sys i2' '$X6/p6.log'"
expect "phase6 MANY_CLIENTS: nothing moved aside, the sweep stays" "[ ! -d '$OUT/old' ] && [ \"\$(cat '$OUT/p2/qdb/memsweep/load.1')\" = keep ]"
expect "phase6 MANY_CLIENTS: how.json block and runstats on p6/" \
    "grep -qx 'how_p6 $OUT/p2/tsdb 5' '$X6/p6.log' && grep -qx 'runstats $OUT/p2/qdb/p6' '$X6/p6.log'"
P6_SYSTEMS="tsdb qdb0 ch ts" MANY_CLIENTS=16 CLASS_FILTER=b100 RESUME=1 phase6; n=$(grep -c '^p2_sys' "$X6/p6.log")
expect "phase6 MANY_CLIENTS: a system with its done marker is skipped (the marker is per client count)" "[ $n = 4 ]"
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
# MANY_CLIENTS for ClickHouse and TimescaleDB: chbsuite and the TWA batch classes (twasuite, twabsuite) time only the
# CLASS_FILTER classes at that many clients, XREPS runs; the branches of p2_ch and p2_ts do the load, the probe (ts), the
# answer checks and those suites, and leave out everything else (the 1- and 32-client classes, merged, LTTB, the -ha load)
untimed() { local o=$1; shift; "$@" > "$o" 2>&1; }
fail() { echo "fail: $*" >> "$X6/mfails"; }
log() { :; }
tstep() { echo "$1" >> "$X6/tstep.log"; }
chbench1() { :; }
REL() { case $1 in
    has) [ "$2" != none ] ;;
    q) echo "SELECT $3" ;;
    qb) echo "SELECT $3 $4" ;;
    check) echo "check $2 $3" >> "$X6/rel.log"; echo "$2 $3 OK" ;;
    checkb) echo "$2 $3.b$4 OK" ;;
esac; }
MD=$OUT/p2/ch/p6; mkdir -p "$MD"
: > "$X6/tstep.log"
MANY_CLIENTS=16 CLASS_FILTER=b100 XREPS=2 REPS=1 KS="1 10 100" FULLS="0 3600" BCLASSES="raw agg7d" chbsuite ch "$MD" ch
want=$(printf '%s\n' "ch.raw.b100.c16 r1" "ch.raw.b100.c16 r2" "ch.agg7d.b100.c16 r1" "ch.agg7d.b100.c16 r2")
expect "MANY_CLIENTS: chbsuite times the b100 classes at 16 clients, XREPS runs each, no whole-range runs" '[ "$(cat "$X6/tstep.log")" = "$want" ]'
: > "$X6/tstep.log"
FULLS="" KS=10 BCLASSES=raw REPS=2 chbsuite ch "$MD" ch
want=$(printf '%s\n' "ch.raw.b10.c1 r1" "ch.raw.b10.c1 r2" "ch.raw.b10.c32 r1" "ch.raw.b10.c32 r2")
expect "MANY_CLIENTS: chbsuite without the option still times 1 and NHI clients" '[ "$(cat "$X6/tstep.log")" = "$want" ]'
: > "$X6/tstep.log"; : > "$X6/rel.log"
TWA_KINDS="twad twal" TWA_WKINDS="twa1d twa7d" TWA_KS="10 100"
MANY_CLIENTS=16 CLASS_FILTER=b100 XREPS=3 REPS=1 twasuite ch "$MD" ch
want=$(printf '%s\n' "ch.twa1d.b100.c16 r1" "ch.twa1d.b100.c16 r2" "ch.twa1d.b100.c16 r3" "ch.twa7d.b100.c16 r1" "ch.twa7d.b100.c16 r2" "ch.twa7d.b100.c16 r3")
expect "MANY_CLIENTS: twasuite times only the TWA b100 classes at 16 clients (no whole-range kinds, no single windows, no b10)" \
    '[ "$(cat "$X6/tstep.log")" = "$want" ]'
expect "MANY_CLIENTS: twasuite checks the single windows of the kinds it batches (and nothing else)" \
    '[ "$(cat "$X6/rel.log")" = "$(printf "%s\n" "check ch twa1d" "check ch twa7d")" ]'
: > "$X6/tstep.log"; : > "$X6/rel.log"
MANY_CLIENTS=16 CLASS_FILTER=twa7d.b100 XREPS=1 REPS=1 twasuite ch "$MD" ch
expect "MANY_CLIENTS: a kind with no wanted batch class is not even checked" \
    '[ "$(cat "$X6/tstep.log")" = "ch.twa7d.b100.c16 r1" ] && [ "$(cat "$X6/rel.log")" = "check ch twa7d" ]'
: > "$X6/tstep.log"
TWA_KINDS="" TWA_WKINDS=twa1d TWA_KS=10 REPS=1 twabsuite ch "$MD" ch
expect "MANY_CLIENTS: twabsuite without the option still times 1 and NHI clients" \
    '[ "$(cat "$X6/tstep.log")" = "$(printf "%s\n" "ch.twa1d.b10.c1 r1" "ch.twa1d.b10.c32 r1")" ]'
# a QuestDB variant: twasuite (rel.py's QuestDB form) checks the single windows of the wanted kinds and times their batches
: > "$X6/reps.log"; : > "$X6/rel.log"; mkdir -p "$OUT/p2/qdb-parquet-zstd/p6"
MANY_CLIENTS=16 CLASS_FILTER="twa1d.b100 twa7d.b100" XREPS=3 REPS=1 twasuite qdb "$OUT/p2/qdb-parquet-zstd/p6" pg conn
want=$(printf '%s\n' "qdb-parquet-zstd.twa1d.b100.c16 $OUT/p2/qdb-parquet-zstd/p6 REPS=3" "qdb-parquet-zstd.twa7d.b100.c16 $OUT/p2/qdb-parquet-zstd/p6 REPS=3")
expect "MANY_CLIENTS: twasuite on a QuestDB variant times the two TWA batches of 100 at 16 clients, named by the variant" \
    '[ "$(cat "$X6/reps.log")" = "$want" ] && [ "$(cat "$X6/rel.log")" = "$(printf "%s\n" "check qdb twa1d" "check qdb twa7d")" ]'
# p2_ch and p2_ts with the pass: stub loads, checks and suites that log; the branch must call the batch suites (and the
# probe for ts) and none of the 1/32-client classes, the merged table, LTTB or the -ha image
mkdir -p "$X6/h"
for py in mkq check mkbatch; do
    printf 'import os, sys\nopen(os.environ["P6LOG"], "a").write("py %%s %%s\\n" %% (os.path.basename(sys.argv[0]), " ".join(sys.argv[1:])))\n' > "$X6/h/$py.py"
done
: > "$P6LOG"; : > "$X6/p2.log"
crm() { :; }; ch_load_rep() { :; }; warm() { :; }; load_ok() { :; }; ports_check() { :; }; CHC() { :; }; chparts() { :; }
ctr_mem() { echo "ctr_mem $1" >> "$X6/p2.log"; }; runstats() { echo "runstats $1" >> "$X6/p2.log"; }
bcheck() { echo "bcheck $1" >> "$X6/p2.log"; }
chbsuite() { echo "chbsuite $*" >> "$X6/p2.log"; }
twasuite() { echo "twasuite $1" >> "$X6/p2.log"; }
chsuite() { echo "chsuite" >> "$X6/p2.log"; }; ch_merged() { echo "ch_merged" >> "$X6/p2.log"; }
H=$X6/h DBCMP=$X6/db MANY_CLIENTS=16 CLASS_FILTER=b100 REPS=1 XREPS=5 LOADS_FROM=1 SWEEP_SUB=/p6 p2_ch; rc=$?
want=$(printf '%s\n' "bcheck ch" "chbsuite ch $OUT/p2/ch/p6 ch" "twasuite ch" "ctr_mem rb-ch" "runstats $OUT/p2/ch/p6")
expect "p2_ch with MANY_CLIENTS: checks, the batch suites and the TWA batches, nothing of the 1/32-client suite or merged" \
    '[ $rc = 0 ] && [ "$(cat "$X6/p2.log")" = "$want" ] && grep -qx "py mkq.py ch" "$P6LOG" && grep -qx "py check.py ch 200" "$P6LOG"'
: > "$X6/p2.log"
ts_start() { echo "ts_start $1" >> "$X6/p2.log"; }; ts_load_rep() { :; }; PSQL() { :; }
ts_many_probe() { # the aggregate classes' probe: "probed"; the TWA batch classes' (tag .twa): "probed.twa", or nothing when
    # PROBE_TWA_FAIL is set (every setting ran out of memory: MANY_CONN keeps what it was)
    echo "probe $1 [${3-}] [${5-}]" >> "$X6/p2.log"
    if [ -z "${5-}" ]; then MANY_CONN=probed; elif [ -z "${PROBE_TWA_FAIL-}" ]; then MANY_CONN=probed.twa; fi
}
bsuite() { echo "bsuite $1 $2 $3 MANY_CONN=$MANY_CONN" >> "$X6/p2.log"; }
qsuite() { echo "qsuite" >> "$X6/p2.log"; }
# p2_ts in the extra-clients pass, per filter (a subshell with stubs for the steps that need a container): the plain image
# only for the aggregate classes, the -ha image only for the TWA batch classes; there the container is rb-tsha (QCTR:
# a death is seen, recorded and restarted), the probe is the TWA batch's own on rb-tsha with the request file of the
# heaviest wanted batch, and its setting (not the aggregate probe's) reaches twasuite
p2ts_run() { # FILTER: log in $X6/p2.log, rc in $rc
    : > "$X6/p2.log"
    ( tstep() { echo "tstep $1" >> "$X6/p2.log"; }; ts_timed() { :; }
      REL() { echo "REL $*" >> "$X6/p2.log"; echo "SELECT 1"; }
      twasuite() { echo "twasuite $1 QCTR=$QCTR MANY_CONN=$MANY_CONN" >> "$X6/p2.log"; }
      H=$X6/h DBCMP=$X6/db KS="1 10 100" BCLASSES="raw agg1d" MANY_CLIENTS=16 CLASS_FILTER="$1" REPS=1 XREPS=5 LOADS_FROM=1 \
        SWEEP_SUB=/p6 WORK=$X6/work2 p2_ts ) > /dev/null 2>&1
    rc=$?
}
mkdir -p "$X6/work2"
P=$OUT/p2/ts/p6
p2ts_run b100
want=$(printf '%s\n' "ts_start rb-ts" "tstep p2 ts load r1" "probe $P [] []" "bcheck ts" "bsuite ts ts $P MANY_CONN=probed" "ctr_mem rb-ts" \
    "ts_start rb-tsha" "tstep p2 tsha load" "REL qb ts twa7d 100" "probe $P [rb-tsha] [.twa]" \
    "twasuite ts QCTR=rb-tsha MANY_CONN=probed.twa" "ctr_mem rb-tsha" "runstats $P")
expect "p2_ts with MANY_CLIENTS and b100: the plain image's classes, then the -ha image's TWA batches with a probe of their own" \
    '[ $rc = 0 ] && [ "$(cat "$X6/p2.log")" = "$want" ]'
p2ts_run "twa1d.b100 twa7d.b100"
want=$(printf '%s\n' "ts_start rb-tsha" "tstep p2 tsha load" "REL qb ts twa7d 100" "probe $P [rb-tsha] [.twa]" \
    "twasuite ts QCTR=rb-tsha MANY_CONN=probed.twa" "ctr_mem rb-tsha" "runstats $P")
expect "p2_ts with the TWA filter: only the -ha image is loaded; no plain image, aggregate probe, check or suite; QCTR is rb-tsha" \
    '[ $rc = 0 ] && [ "$(cat "$X6/p2.log")" = "$want" ]'
p2ts_run "twa1d.b100"
expect "p2_ts: the probe sends the batch the filter names (twa1d.b100 only: twa1d's request file)" \
    'grep -qx "REL qb ts twa1d 100" "$X6/p2.log" && ! grep -q "twa7d" "$X6/p2.log"'
PROBE_TWA_FAIL=1 p2ts_run "twa1d.b100 twa7d.b100"
expect "p2_ts: when no setting fits the TWA batches run with the default, not with the aggregate probe's setting" \
    'grep -qx "twasuite ts QCTR=rb-tsha MANY_CONN=" "$X6/p2.log"'
p2ts_run "agg1d.b100"
want=$(printf '%s\n' "ts_start rb-ts" "tstep p2 ts load r1" "probe $P [] []" "bcheck ts" "bsuite ts ts $P MANY_CONN=probed" "ctr_mem rb-ts" "runstats $P")
expect "p2_ts with an aggregate-only filter: no -ha image, as before" '[ $rc = 0 ] && [ "$(cat "$X6/p2.log")" = "$want" ]'
# p2_tsdb and p2_qdb with the pass: with CLASS_FILTER=b100 the aggregate classes' check and suites and the TWA batches; with
# the two TWA classes of 100 named, the TWA batches only (no answer check or suite of the aggregate classes, which the
# earlier pass timed)
mkdir -p "$X6/qh"
tsdbd_start() { echo "tsdbd_start" >> "$X6/p2.log"; }; tsdbd_stop() { echo "tsdbd_stop" >> "$X6/p2.log"; }
tsdb_self() { echo "tsdb_self" >> "$X6/p2.log"; }
tsdbbsuite() { echo "tsdbbsuite $1 $2" >> "$X6/p2.log"; }
tsdbtwab() { echo "tsdbtwab $1 [$2]" >> "$X6/p2.log"; }
qdb_start() { echo "qdb_start" >> "$X6/p2.log"; }; QX() { :; }
twasuite() { echo "twasuite $1 $2 $3" >> "$X6/p2.log"; }
for f in b100 "twa1d.b100 twa7d.b100"; do
    : > "$X6/p2.log"
    H=$X6/h DBCMP=$X6/db KS="1 10 100" BCLASSES="raw agg1d" MANY_CLIENTS=16 CLASS_FILTER="$f" REPS=1 XREPS=5 LOADS_FROM=1 SWEEP_SUB=/p6 p2_tsdb; rc=$?
    if [ "$f" = b100 ]; then
        want=$(printf '%s\n' "tsdbd_start" "tsdb_self" "tsdbbsuite tsdbq $OUT/p2/tsdb/p6" "tsdbtwab $OUT/p2/tsdb/p6 []" "tsdbd_stop" "runstats $OUT/p2/tsdb/p6")
    else
        want=$(printf '%s\n' "tsdbd_start" "tsdbtwab $OUT/p2/tsdb/p6 []" "tsdbd_stop" "runstats $OUT/p2/tsdb/p6")
    fi
    expect "p2_tsdb with MANY_CLIENTS and CLASS_FILTER '$f': [$(echo "$want" | tr '\n' ',')]" '[ $rc = 0 ] && [ "$(cat "$X6/p2.log")" = "$want" ]'
    : > "$X6/p2.log"
    H=$X6/h DBCMP=$X6/db KS="1 10 100" BCLASSES="raw agg1d" MANY_CLIENTS=16 CLASS_FILTER="$f" REPS=1 XREPS=5 LOADS_FROM=1 SWEEP_SUB=/p6 p2_qdb 0; rc=$?
    if [ "$f" = b100 ]; then
        want=$(printf '%s\n' "qdb_start" "bcheck qdb" "bsuite qdb qdb $OUT/p2/qdb/p6 MANY_CONN=" "twasuite qdb $OUT/p2/qdb/p6 pg" "ctr_mem rb-qdb" "runstats $OUT/p2/qdb/p6")
    else
        want=$(printf '%s\n' "qdb_start" "twasuite qdb $OUT/p2/qdb/p6 pg" "ctr_mem rb-qdb" "runstats $OUT/p2/qdb/p6")
    fi
    expect "p2_qdb with MANY_CLIENTS and CLASS_FILTER '$f': [$(echo "$want" | tr '\n' ',')]" '[ $rc = 0 ] && [ "$(cat "$X6/p2.log")" = "$want" ]'
done
[ "$fails" = 0 ] && echo "harnesstest OK" || { echo "harnesstest FAILED"; exit 1; }
