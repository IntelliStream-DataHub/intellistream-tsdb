#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# tests/jepsen/rerun.sh against a stand-in for jepsen whose exit statuses are scripted (a file of one status per attempt):
# INCONCLUSIVE (3) is rerun with the next seed, at most twice; a violation (1) or a harness failure (2) ends at once and is
# never rerun; three INCONCLUSIVE in a row fail with all three messages; every rerun is in the log.
# Usage: test_jepsen_rerun.sh [SCRATCH]
set -u
here=$(cd "$(dirname "$0")" && pwd)
d=$(mktemp -d "${1:-${TMPDIR:-/tmp}}/jrerun.XXXXXX") || exit 1
trap 'rm -rf "$d"' EXIT
cat > "$d/fake" <<'EOF'
#!/bin/bash
# fake jepsen: the status of this attempt is the next line of $FAKE_PLAN; it prints the seed it was given
n=$(cat "$FAKE_STATE" 2>/dev/null || echo 0)
echo $((n + 1)) > "$FAKE_STATE"
rc=$(sed -n "$((n + 1))p" "$FAKE_PLAN")
seed=
while [ $# -gt 0 ]; do [ "$1" = --seed ] && seed=$2; shift; done
echo "fake: attempt $((n + 1)) seed=$seed rc=$rc"
case $rc in
3) echo "  [inconclusive, check dead-stretch] a server lay dead for 648 ms (attempt $((n + 1)))" >&2
   echo "INCONCLUSIVE (1 failed liveness or coverage checks: dead-stretch): the run tested less than it says" ;;
1) echo "FAILED: 1 violations" ;;
0) echo valid ;;
esac
exit $rc
EOF
chmod +x "$d/fake"
fail=0
t() { # NAME WANT-RC PLAN(space separated) PATTERN... -- extra args to the driver before the command
    local name=$1 want=$2 plan=$3 base=$4; shift 4
    printf '%s\n' $plan > "$d/plan"; rm -f "$d/state" "$d/log"
    local out rc
    out=$(FAKE_PLAN=$d/plan FAKE_STATE=$d/state JEPSEN_RERUN_LOG=$d/log "$here/jepsen/rerun.sh" "$d/fake" --seed "$base" "$@" 2>&1); rc=$?
    if [ $rc -ne $want ]; then echo "FAIL $name: rc $rc, wanted $want"; echo "$out"; fail=1; return 1; fi
    echo "$out" > "$d/out"
    return 0
}
has() { grep -q -- "$2" "$d/$1" || { echo "FAIL $name: no '$2' in $1"; cat "$d/$1"; fail=1; }; }
hasnt() { ! grep -q -- "$2" "$d/$1" || { echo "FAIL $name: '$2' in $1"; cat "$d/$1"; fail=1; }; }

name="valid first time"
t "$name" 0 "0" 5 && { has out 'seed=5 rc=0'; hasnt out 'rerun'; hasnt log '^inconclusive'; }

name="inconclusive once, then valid, with the next seed"
t "$name" 0 "3 0" 5 && { has out 'seed=5 rc=3'; has out 'seed=6 rc=0'; has out 'attempt 1 of 3 (seed 5) was INCONCLUSIVE (1 failed liveness or coverage checks: dead-stretch)';
    has out 'valid on attempt 2 of 3 (seed 6) after 1 INCONCLUSIVE'; has log '^inconclusive seed=5'; hasnt log '^gate'; }

name="inconclusive twice, then valid"
t "$name" 0 "3 3 0" 5 && { has out 'seed=7 rc=0'; has out 'valid on attempt 3 of 3'; [ "$(grep -c '^inconclusive' "$d/log")" -eq 2 ] || { echo "FAIL $name: log"; fail=1; }; }

name="three in a row fail the gate with all three messages"
t "$name" 3 "3 3 3" 5 && { has out 'INCONCLUSIVE 3 times in a row (seeds 5 to 7)'; has out 'attempt 1, seed 5'; has out 'attempt 3, seed 7';
    for n in 1 2 3; do has out "(attempt $n)"; done; hasnt out 'seed=8'; has log '^gate'; [ "$(grep -c '^inconclusive' "$d/log")" -eq 3 ] || { echo "FAIL $name: log"; fail=1; }; }

name="a violation after an inconclusive attempt fails at once"
t "$name" 1 "3 1 0" 5 && { has out 'seed=6 rc=1'; hasnt out 'seed=7'; hasnt out 'valid on attempt'; }

name="a violation at the first attempt is never rerun"
t "$name" 1 "1 0 0" 5 && { hasnt out 'seed=6'; hasnt out 'rerun'; }

name="a harness failure is passed on at once"
t "$name" 2 "2 0 0" 5 && hasnt out 'seed=6'

name="no seed on the command line: the default is 1, the next is 2"
printf '3\n0\n' > "$d/plan"; rm -f "$d/state"
out=$(FAKE_PLAN=$d/plan FAKE_STATE=$d/state "$here/jepsen/rerun.sh" "$d/fake" 2>&1); rc=$?
echo "$out" > "$d/out"
[ $rc -eq 0 ] && has out 'seed=1 rc=3' && has out 'seed=2 rc=0' || { echo "FAIL $name: rc $rc"; echo "$out"; fail=1; }

name="the summary counts runs, reruns and gate failures"
printf '%s\n' 'run a' 'inconclusive seed=5 dead-stretch' 'run b' 'inconclusive seed=1 x' 'inconclusive seed=2 x' 'inconclusive seed=3 x' 'gate b' > "$d/log2"
"$here/jepsen/rerun.sh" --summary "$d/log2" > "$d/out"
has out '2 runs, 4 INCONCLUSIVE attempts'; has out '1 runs INCONCLUSIVE three times'

[ $fail -eq 0 ] && echo "test_jepsen_rerun: ok"
exit $fail
