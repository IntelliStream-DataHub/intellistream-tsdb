#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# The driver of an honest jepsen run: tests/jepsen/rerun.sh [--attempts N] JEPSEN COMMAND-LINE...
#
# jepsen ends one of four ways (tests/jepsen/jepsen.c, inc_note):
#   0  valid
#   1  VIOLATION: a wrong answer of the system under test (lost, resurrected or wrong data, a crash, a hang). The driver
#      stops at once with that status; a violation is never repeated.
#   2  the harness itself failed (a server that never starts, a bad option, a directory in use). Passed on at once.
#   3  INCONCLUSIVE: a liveness or coverage check of the run itself failed (the nemesis was too slow for its floor, a server
#      lay dead too long, a gap with no fault, a step that idled, the workload wrote or switched too little). The run
#      tested less than it says, which is no finding about the code under test: a machine that stalls a thread for half
#      a second does it to an honest run. The driver runs it again with the next seed, twice at most. Three INCONCLUSIVE
#      runs in a row fail the gate (exit 3) with all three messages. Every rerun is printed, and counted in the file named
#      by JEPSEN_RERUN_LOG (one line per attempt; `rerun.sh --summary FILE` prints the totals), so none is silent.
#
# A planted-bug case of the selftest runs through here too and must end in the three-INCONCLUSIVE failure with its check named,
# not merely in a non-zero status: a nemesis that is idle is idle on every seed, an honest one that was stalled once is not.
set -u
attempts=3
case "${1:-}" in
--summary)
    f=${2:?usage: rerun.sh --summary FILE}
    runs=$(grep -c '^run ' "$f" 2>/dev/null); inc=$(grep -c '^inconclusive ' "$f" 2>/dev/null); gate=$(grep -c '^gate ' "$f" 2>/dev/null)
    echo "jepsen reruns: ${runs:-0} runs, ${inc:-0} INCONCLUSIVE attempts (each rerun with the next seed), ${gate:-0} runs INCONCLUSIVE three times in a row"
    grep '^inconclusive ' "$f" 2>/dev/null | sed 's/^/  /'
    exit 0 ;;
--attempts) attempts=${2:?}; shift 2 ;;
esac
[ $# -ge 1 ] || { echo "usage: rerun.sh [--attempts N] JEPSEN ARGS..." >&2; exit 2; }

# the seed of the command line (jepsen's default is 1); each rerun takes the next
args=("$@")
seed=1
seed_at=-1
for ((i = 0; i < ${#args[@]}; i++)); do
    if [ "${args[i]}" = --seed ] && [ $((i + 1)) -lt ${#args[@]} ]; then
        seed=${args[i + 1]}
        seed_at=$((i + 1))
    fi
done
log=${JEPSEN_RERUN_LOG:-}
tmp=$(mktemp "${TMPDIR:-/tmp}/jepsen-rerun.XXXXXX") || exit 2
trap 'rm -f "$tmp" "$tmp".*' EXIT
[ -n "$log" ] && echo "run ${args[*]}" >> "$log"

for ((k = 0; k < attempts; k++)); do
    s=$((seed + k))
    cmd=("${args[@]}")
    if [ $seed_at -ge 0 ]; then
        cmd[seed_at]=$s
    else
        cmd+=(--seed "$s")
    fi
    "${cmd[@]}" 2>&1 | tee "$tmp"
    rc=${PIPESTATUS[0]}
    if [ $rc -ne 3 ]; then
        if [ $rc -eq 0 ] && [ $k -gt 0 ]; then
            echo "jepsen-rerun: valid on attempt $((k + 1)) of $attempts (seed $s) after $k INCONCLUSIVE attempt(s): not a finding, the machine or the nemesis' timing stalled the run"
        fi
        exit $rc
    fi
    # the failed checks, with their numbers, kept for the gate's message
    grep '\[inconclusive, check ' "$tmp" > "$tmp.$k" || :
    why=$(sed -n 's/^INCONCLUSIVE (\([^)]*\)).*/\1/p' "$tmp" | head -n 1)
    [ -n "$log" ] && echo "inconclusive seed=$s ${why:-?}" >> "$log"
    if [ $((k + 1)) -lt $attempts ]; then
        echo "jepsen-rerun: attempt $((k + 1)) of $attempts (seed $s) was INCONCLUSIVE (${why:-no check named}), not a violation: running it again with seed $((s + 1))"
    fi
done
echo "jepsen-rerun: INCONCLUSIVE $attempts times in a row (seeds $seed to $((seed + attempts - 1))): the gate fails; the run could not test what it says"
for ((k = 0; k < attempts; k++)); do
    echo "jepsen-rerun: attempt $((k + 1)), seed $((seed + k)):"
    cat "$tmp.$k"
done
[ -n "$log" ] && echo "gate ${args[*]}" >> "$log"
exit 3
