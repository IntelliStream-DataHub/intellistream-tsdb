#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# tools/mutants.sh with fake test programs that pass, fail, print the expected line (or not), hang and cannot be run:
# it must name exactly the survivors (all of them), keep their logs, never count a hang or a missing binary as caught,
# give every run a scratch directory of its own and respect MUTANT_JOBS and MUTANT_URING_JOBS. Seconds.
#
#   tests/test_mutants_helper.sh      (make test runs it)
set -u
cd "$(dirname "$0")/.."
H=$PWD/tools/mutants.sh
base=$(mktemp -d "${TMPDIR:-/tmp}/mutants-helper.XXXXXX") || exit 1
trap 'rm -rf "$base"' EXIT
fail() { echo "test_mutants_helper: FAIL: $*" >&2; [ -z "${out:-}" ] || printf '%s\n' "$out" >&2; exit 1; }
export XDG_RUNTIME_DIR=$base # the default pool of the helpers lives here
unset MUTANT_JOBS MUTANT_SLOTS MUTANT_SLOT_DIR MUTANT_URING_JOBS MUTANT_TIMEOUT
export TMPDIR=$base # the helper's scratch root, unless --tmp says otherwise

# the fake test: FAKE MUTANT SCRATCH, behaviour by mutant name
cat >"$base/fake" <<'EOF'
#!/bin/bash
m=$1 d=$2
case $m in
catch*) echo "FAIL: planted $m"; exit 1 ;;
live*) echo "all fine"; exit 0 ;;
half) echo "HALF DROP: rows missing"; exit 0 ;;
half1) echo "HALF DROP: rows missing"; exit 1 ;;
nohalf) echo "other failure"; exit 1 ;;
r66a) echo "manifest_publish and replica_cut"; exit 66 ;;
r66b) echo "manifest_publish only"; exit 66 ;;
r1) echo "manifest_publish and replica_cut"; exit 1 ;;
err) echo "on stderr: KILLED" >&2; echo "plain"; exit 0 ;;
hang) sleep 300 & echo $! >"$FAKE_PIDS/hang"; wait ;;
hangk) sleep 300 & echo $! >"$FAKE_PIDS/hangk"; wait ;;
died*) echo "going down"; kill -KILL "$PPID" ;; # the run's wrapper is killed (the OOM killer's doing): no status
fixed*) # a file with a fixed name in the scratch directory: two runs sharing the directory collide
    [ ! -e "$d/state" ] || { echo "collision"; exit 0; }
    touch "$d/state"; sleep 0.3; exit 1 ;;
conc*) # how many runs are alive at once
    mkdir -p "$FAKE_CONC"; touch "$FAKE_CONC/$$"
    n=$(ls "$FAKE_CONC" | wc -l); echo "$n" >>"$FAKE_CONC.max"
    sleep 0.6; rm -f "$FAKE_CONC/$$"; exit 1 ;;
esac
exit 3
EOF
chmod +x "$base/fake"
export FAKE_PIDS=$base/pids FAKE_CONC=$base/conc
mkdir -p "$FAKE_PIDS"
F=$base/fake

run() { out=$("$@" 2>&1); rc=$?; }
# a deadline for the cases that wait on a hang or a pool: a helper without its timeout or takeover must fail here,
# not hold make test for ever (timeout is GNU coreutils; without it the case runs without a deadline)
dl() { if command -v timeout >/dev/null 2>&1; then timeout 90 "$@"; else "$@"; fi; }
has() { case $out in *"$1"*) ;; *) fail "output lacks '$1'" ;; esac; }
hasnt() { case $out in *"$1"*) fail "output has '$1'" ;; esac; }
survivors_are() { # the last line names exactly these
    local last
    last=$(printf '%s\n' "$out" | tail -n 1)
    [ "${last#*SURVIVED: }" = "$*" ] || fail "last line is '$last', wanted survivors '$*'"
}

# 1. rc check: only the one that passes survives, rc 1, its log is kept, the caught ones leave nothing
run "$H" --name t --jobs 3 --list "catch1 live1 catch2" -- "$F" @M@ @D@
[ $rc -eq 1 ] || fail "survivor: rc $rc"
has "mutant catch1: caught"; has "mutant catch2: caught"; has "mutant live1: SURVIVED"
survivors_are live1
log=$(printf '%s\n' "$out" | sed -n 's/.*log \([^,]*out\.log\),.*/\1/p')
[ -f "$log" ] && grep -q 'all fine' "$log" || fail "the survivor's log '$log' was not kept"
# the caught ones' directories are gone
ls -d "$base"/mut.*/catch* 2>/dev/null && fail "a caught run's directory was kept"
rm -rf "$base"/mut.*

# 2. all caught: exit 0, nothing kept
run "$H" --name t --jobs 2 --list "catch1 catch2 catch3" -- "$F" @M@ @D@
[ $rc -eq 0 ] || fail "all caught: rc $rc"
has "all 3 mutants caught"
ls -d "$base"/mut.* 2>/dev/null && fail "scratch kept after a clean run"

# 3. every survivor is named, not only the first
run "$H" --name t --jobs 4 --list "live1 catch1 live2 live3" -- "$F" @M@ @D@
[ $rc -eq 1 ] || fail "several survivors: rc $rc"
last=$(printf '%s\n' "$out" | tail -n 1)
for m in live1 live2 live3; do case $last in *"$m"*) ;; *) fail "survivor $m not named in '$last'" ;; esac; done
case $last in *catch1*) fail "a caught mutant is named a survivor" ;; esac
rm -rf "$base"/mut.*

# 4. --pass: a loop that expects success
run "$H" --name t --pass --list "live1 catch1" -- "$F" @M@ @D@
[ $rc -eq 1 ] || fail "--pass: rc $rc"
has "mutant live1: caught"; has "mutant catch1: SURVIVED"
rm -rf "$base"/mut.*

# 5. --grep alone ignores the status (the status of `cmd | grep -q`); a missing line survives
run "$H" --name t --grep 'HALF DROP' --list "half half1 nohalf" -- "$F" @M@ @D@
[ $rc -eq 1 ] || fail "--grep: rc $rc"
has "mutant half: caught"; has "mutant half1: caught"; has "mutant nohalf: SURVIVED"
rm -rf "$base"/mut.*
# ... with --fail too: the line and a failing status
run "$H" --name t --fail --grep 'HALF DROP' --list "half half1" -- "$F" @M@ @D@
has "mutant half: SURVIVED"; has "mutant half1: caught"
rm -rf "$base"/mut.*

# 6. --rc N with several --grep: all hold or it survives (a planted race: status 66 and both names in the report)
run "$H" --name t --rc 66 --grep manifest_publish --grep replica_cut --list "r66a r66b r1" -- "$F" @M@ @D@
[ $rc -eq 1 ] || fail "--rc: rc $rc"
has "mutant r66a: caught"; has "mutant r66b: SURVIVED"; has "mutant r1: SURVIVED"
rm -rf "$base"/mut.*

# 7. --stdout-only: what is on stderr is not the output
run "$H" --name t --stdout-only --grep KILLED --list "err" -- "$F" @M@ @D@
has "mutant err: SURVIVED"
run "$H" --name t --grep KILLED --list "err" -- "$F" @M@ @D@
has "mutant err: caught"
rm -rf "$base"/mut.*

# 8. a hang is SURVIVED (timeout), its process group is killed, and the others finish
t0=$SECONDS
run dl "$H" --name t --timeout 2 --jobs 2 --list "hang catch1" -- "$F" @M@ @D@
[ $rc -eq 1 ] || fail "hang: rc $rc"
has "mutant hang: SURVIVED"; has "timeout after 2 s"; has "mutant catch1: caught"
[ $((SECONDS - t0)) -lt 20 ] || fail "the hang took $((SECONDS - t0)) s"
hp=$(cat "$FAKE_PIDS/hang")
sleep 0.3
if kill -0 "$hp" 2>/dev/null; then kill "$hp" 2>/dev/null; fail "the hung run's child $hp is still alive"; fi
rm -rf "$base"/mut.*
# MUTANT_TIMEOUT too
MUTANT_TIMEOUT=1 run dl "$H" --name t --list "hang" -- "$F" @M@ @D@
has "timeout after 1 s"
rm -rf "$base"/mut.*

# 9. a command that cannot be run is never "caught"
run "$H" --name t --list "catch1 catch2" -- "$base/no-such-binary" @M@ @D@
[ $rc -eq 1 ] || fail "missing binary: rc $rc"
has "mutant catch1: SURVIVED"; has "could not be run"
rm -rf "$base"/mut.*

# 10. every run has a scratch directory of its own (@D@), empty at the start, under the root; names made safe
run "$H" --name t --jobs 8 --tmp "$base/root" --list "fixed1 fixed2 fixed3 fixed4 fixed5 fixed6 fixed7 fixed8" -- "$F" @M@ @D@
[ $rc -eq 0 ] || fail "scratch dirs: rc $rc"
run "$H" --name t --jobs 2 --list "catch@3000 catch,13" -- "$F" @M@ @D@
[ $rc -eq 0 ] || fail "odd names: rc $rc"
run "$H" --name t --list "catch1 catch1" -- "$F" @M@ @D@
[ $rc -eq 2 ] || fail "a name listed twice: rc $rc"
run "$H" --name t --list "" -- "$F" @M@ @D@
[ $rc -eq 2 ] || fail "an empty list: rc $rc"
rm -rf "$base"/mut.*

# 11. the job limits: MUTANT_JOBS (1 = one after the other), the default, MUTANT_URING_JOBS with --uring
maxc() { sort -n "$FAKE_CONC.max" | tail -n 1; }
rm -rf "$FAKE_CONC" "$FAKE_CONC.max"
MUTANT_JOBS=1 run "$H" --name t --list "conc1 conc2 conc3 conc4" -- "$F" @M@ @D@
[ "$(maxc)" = 1 ] || fail "MUTANT_JOBS=1: $(maxc) at once"
rm -rf "$FAKE_CONC" "$FAKE_CONC.max"
MUTANT_JOBS=3 run "$H" --name t --list "conc1 conc2 conc3 conc4 conc5 conc6" -- "$F" @M@ @D@
[ "$(maxc)" = 3 ] || fail "MUTANT_JOBS=3: $(maxc) at once"
rm -rf "$FAKE_CONC" "$FAKE_CONC.max"
MUTANT_JOBS=6 MUTANT_URING_JOBS=2 run "$H" --name t --uring --list "conc1 conc2 conc3 conc4 conc5 conc6" -- "$F" @M@ @D@
[ "$(maxc)" = 2 ] || fail "MUTANT_URING_JOBS=2: $(maxc) at once"
rm -rf "$FAKE_CONC" "$FAKE_CONC.max"
run "$H" --name t --jobs 2 --list "conc1 conc2 conc3 conc4" -- "$F" @M@ @D@
[ "$(maxc)" = 2 ] || fail "--jobs 2: $(maxc) at once"
rm -rf "$base"/mut.*

# 12. --verify: the program that owns the table decides, and its line is shown
cat >"$base/table" <<'EOF'
#!/bin/bash
[ "$1" = --mutant-verify ] || exit 9
m=$2 rc=$3 log=$4
grep -q "planted $m" "$log" && { echo "oracle line of $m"; exit 0; }
echo "wrong place: $(head -n 1 "$log")"; exit 1
EOF
chmod +x "$base/table"
run "$H" --name t --verify "$base/table" --list "catch1 live1 nohalf" -- "$F" @M@ @D@
[ $rc -eq 1 ] || fail "--verify: rc $rc"
has "mutant catch1: caught"; has "oracle line of catch1"; has "mutant live1: SURVIVED"; has "mutant nohalf: SURVIVED"
rm -rf "$base"/mut.*

# 13. --detail shows the first line that matches
run "$H" --name t --detail 'FAIL:' --list "catch1" -- "$F" @M@ @D@
has "FAIL: planted catch1"
rm -rf "$base"/mut.*

# 14. a pool shared by helpers that run at once (make tsan-par): MUTANT_SLOTS runs alive in all of them together
rm -rf "$FAKE_CONC" "$FAKE_CONC.max"
export MUTANT_SLOT_DIR=$base/slots MUTANT_SLOTS=2
dl "$H" --name a --jobs 4 --list "conc1 conc2 conc3 conc4" -- "$F" @M@ @D@ >"$base/a.out" 2>&1 &
pa=$!
dl "$H" --name b --jobs 4 --list "conc5 conc6 conc7 conc8" -- "$F" @M@ @D@ >"$base/b.out" 2>&1 &
pb=$!
wait $pa || fail "pool: first helper failed"
wait $pb || fail "pool: second helper failed"
[ "$(maxc)" = 2 ] || fail "MUTANT_SLOTS=2 over two helpers: $(maxc) at once"
[ "$(sort -n "$FAKE_CONC.max" | wc -l)" = 8 ] || fail "pool: not all eight runs happened"
# no job count given: the helpers share a default pool per user (MUTANT_SLOTS wide here), jobs given: no pool
rm -rf "$FAKE_CONC" "$FAKE_CONC.max"
unset MUTANT_SLOT_DIR
MUTANT_SLOTS=1 dl "$H" --name a --list "conc1 conc2 conc3" -- "$F" @M@ @D@ >"$base/a.out" 2>&1 &
pa=$!
MUTANT_SLOTS=1 dl "$H" --name b --list "conc4 conc5 conc6" -- "$F" @M@ @D@ >"$base/b.out" 2>&1 &
pb=$!
wait $pa || fail "default pool: first helper failed"
wait $pb || fail "default pool: second helper failed"
[ "$(maxc)" = 1 ] || fail "default pool of 1 over two helpers: $(maxc) at once"
[ -d "$base/tsdb-mutant-slots-$(id -u)" ] || fail "the default pool was not made under XDG_RUNTIME_DIR"
rm -rf "$FAKE_CONC" "$FAKE_CONC.max"
MUTANT_SLOTS=1 run dl "$H" --name t --jobs 3 --list "conc1 conc2 conc3" -- "$F" @M@ @D@
[ "$(maxc)" = 3 ] || fail "--jobs given: the default pool applied ($(maxc) at once)"
export MUTANT_SLOT_DIR=$base/slots MUTANT_SLOTS=2
# a slot left by a helper that died is taken over
mkdir -p "$base/slots/1" "$base/slots/2"
echo 999999 >"$base/slots/1/pid"; echo 999999 >"$base/slots/2/pid"
run dl "$H" --name t --jobs 2 --list "catch1 catch2" -- "$F" @M@ @D@
[ $rc -eq 0 ] || fail "stale slots: rc $rc"
[ -z "$(ls "$base/slots")" ] || fail "slots left behind: $(ls "$base/slots")"
unset MUTANT_SLOT_DIR MUTANT_SLOTS
rm -rf "$base"/mut.*

# 15. a run whose wrapper ended without a status is SURVIVED, never caught
run dl "$H" --name t --jobs 2 --list "died1 catch1" -- "$F" @M@ @D@
[ $rc -eq 1 ] || fail "died: rc $rc"
has "mutant died1: SURVIVED"; has "without a status"; has "mutant catch1: caught"
rm -rf "$base"/mut.*

# 16. a SIGKILL of the helper runs no trap: the runs, each in a group of its own, must not outlive it
"$H" --name t --timeout 600 --jobs 2 --list "hangk" -- "$F" @M@ @D@ >"$base/k.out" 2>&1 &
hp=$!
for _ in $(seq 1 50); do [ -s "$FAKE_PIDS/hangk" ] && break; sleep 0.1; done
[ -s "$FAKE_PIDS/hangk" ] || { kill -KILL "$hp" 2>/dev/null; fail "the run did not start"; }
kp=$(cat "$FAKE_PIDS/hangk")
kill -0 "$kp" 2>/dev/null || fail "the run's child is not alive before the kill"
kill -KILL "$hp"; wait "$hp" 2>/dev/null
gone=
for _ in $(seq 1 60); do kill -0 "$kp" 2>/dev/null || { gone=1; break; }; sleep 0.1; done
[ -n "$gone" ] || { kill "$kp" 2>/dev/null; fail "the run's child $kp outlived a SIGKILL of the helper"; }
rm -rf "$base"/mut.*

echo "test_mutants_helper: ok"
