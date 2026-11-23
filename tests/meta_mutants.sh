#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# Every bug planted in server/meta.c (MUT(n)) must be caught by test_meta, and the way it is caught is checked:
#  - the full run (unit tests and simulator) must exit 1 and say "(the planted bug was caught)": a crash, a hang, a
#    missing binary or an unknown mutant is not "caught";
#  - the simulator alone (--sim-only: the unit tests off) must catch the SIM_MUTANTS, so that the simulator's checker
#    is shown to have teeth of its own, not only to ride on the unit tests that kill all of them.
# Usage: meta_mutants.sh [--part K/N] BUILD/test_meta_mut [MUTANT...]
# --part K/N runs every Nth mutant of the list, starting with the Kth (CI splits the list over runners).
set -u
part=
if [ "${1:-}" = --part ]; then
    part=$2
    shift 2
fi
bin=$1
shift
# 36 userstaleok, 37 uidfromproposer, 38 nousersbudget, 39 noactorguard; R4's replica set: 40 setvernoraise,
# 41 setverraiseisr, 42 resetinisr, 43 resetleader, 44 twomoves, 45 replacesstays, 46 moveslotlast, 47 targetnotisr,
# 48 targetleader
muts=${*:-1 2 3 4 5 6 7 8 9 10 11 12 13 14 25 28 31 35 36 37 38 39 40 41 42 43 44 45 46 47 48}
if [ -n "$part" ]; then
    k=${part%/*}
    n=${part#*/}
    case $k$n in '' | *[!0-9]*) echo "meta_mutants.sh: --part K/N, got '$part'" >&2; exit 2 ;; esac
    [ "$k" -ge 1 ] && [ "$k" -le "$n" ] || { echo "meta_mutants.sh: --part K/N needs 1 <= K <= N" >&2; exit 2; }
    sel=
    i=0
    for m in $muts; do
        [ $((i % n + 1)) -eq "$k" ] && sel="$sel $m"
        i=$((i + 1))
    done
    muts=$sel
fi
[ -n "${muts// /}" ] || exit 0 # a part with none
# the mutants the simulator's checker catches without the unit stage (measured: with it off 8 of the 17 are caught;
# R4's 40-47 all, by meta_check_step or the per-op check of the replica set's version and a reset)
SIM_MUTANTS=" 1 2 4 6 10 11 12 13 40 41 42 43 44 45 46 47 "
sim=
for n in $muts; do
    case "$SIM_MUTANTS" in *" $n "*) sim="$sim $n" ;; esac
done
# each in a scratch directory of its own, in parallel (tools/mutants.sh: MUTANT_JOBS, MUTANT_TIMEOUT); caught only
# with exit 1 and the test's own line: a crash, a hang, a missing binary or an unknown mutant is NOT caught
mut="$(dirname "$0")/../tools/mutants.sh"
caught_re='\(the planted bug was caught\)'
bad=0
"$mut" --name meta --tmp /var/tmp --timeout "${MUTANT_TIMEOUT:-3600}" --rc 1 --grep "$caught_re" --list "$muts" -- \
    "$bin" @D@ --mutant @M@ --seeds 6 --seconds 8 || bad=1
if [ -n "${sim// /}" ]; then
    "$mut" --name meta-sim-only --tmp /var/tmp --timeout "${MUTANT_TIMEOUT:-3600}" --rc 1 --grep "$caught_re" \
        --list "$sim" -- "$bin" @D@ --mutant @M@ --sim-only --seeds 6 --seconds 8 || bad=1
fi
exit $bad
