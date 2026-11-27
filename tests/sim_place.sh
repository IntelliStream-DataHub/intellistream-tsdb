#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# The placement planner's simulator (tests/sim_place.c, docs/design-cluster-data.md §10.2; track r9-simplace).
#   tests/sim_place.sh quick                       200 honest seeds, then every active mutant (<= 2000 seeds)
#   tests/sim_place.sh mutants [SEEDS]             every active planted bug, up to SEEDS (default 2000) each
#   tests/sim_place.sh soak [SEEDS] [JOBS] [FIRST] SEEDS honest seeds (default 10000) on JOBS procs, then mutants
#   tests/sim_place.sh mttdl                       print MTTDL table across geometries at 10 and 100 TB/node
# B= the build directory (build).
set -u
B=${B:-build}
mode=${1:-quick}

# Active planted mutants present in server/place.c, server/pout.c, and tests/sim_place.c:
#   place.c (r9-plan):     orderfifo rackignored twoperserver moveshrinks wholenobarrier
#   pout.c  (r9-out):      noguard guardunbounded holdignored openingunbounded failstopslow
#   sim_place.c (r9-sim):  deleteunmarked belowk
# Hook points ready to append once their owning tracks land:
#   r9-whole:    releasenomark
#   r9-balance:  balanceunbounded
mutants_active="orderfifo rackignored twoperserver moveshrinks wholenobarrier noguard guardunbounded holdignored openingunbounded failstopslow deleteunmarked belowk takebackwhole partialdelnow"

PLACE_MUTANT_SEEDS=${PLACE_MUTANT_SEEDS:-2000}

case $mode in
--mutant-run)
    m=$2
    exec "$B"/sim_place --mutant "$m" --seeds "$PLACE_MUTANT_SEEDS" 2>&1
    ;;
--mutant-verify)
    # $2 mutant name, $3 exit status, $4 log file.
    # Caught only on exit 1 with a KILLED line naming the mutant AND matching its expected checker rule
    # (never a crash 139, unknown mutant 2, missing 127, or unrelated failure).
    [ "$3" = 1 ] || exit 1
    case "$2" in
    orderfifo)        pat='E13 rebuild (order|starvation)' ;;
    rackignored)      pat='E9 rack (cap|rule)' ;;
    twoperserver)     pat='duplicate server' ;;
    moveshrinks)      pat='REPLICA_REMOVE' ;;
    wholenobarrier)   pat='STRIPE_WHOLE before barrier' ;;
    noguard)          pat='while mass-OUT guard holds' ;;
    guardunbounded)   pat='mass-OUT guard held member .* past bound' ;;
    holdignored)      pat='OUT for held member' ;;
    openingunbounded) pat='opening hold kept member .* past opening_hold_s' ;;
    failstopslow)     pat='(not marked OUT|not escalated) at failstop_out_s' ;;
    deleteunmarked)   pat='chunk deleted (without committed MK_GC|while pending MK_COLD names it)' ;;
    belowk)           pat='below k=[0-9]+ readable positions' ;;
    takebackwhole)    pat='STRIPE_SET took back WHOLE UP position' ;;
    partialdelnow)    pat='chunk deleted (without committed MK_GC|before current holder reported whole)' ;;
    *)                pat='.' ;;
    esac
    grep -m1 -F "KILLED $2 " "$4" | grep -E "$pat"
    exit
    ;;
esac

run_mutants() {
    local seeds=$1
    PLACE_MUTANT_SEEDS=$seeds "$(dirname "$0")"/../tools/mutants.sh --name sim_place \
        --tmp /tmp --timeout "${MUTANT_TIMEOUT:-600}" --verify "$0" --list "$mutants_active" \
        -- "$0" --mutant-run @M@ @D@ || {
        echo "sim_place.sh: a planted bug survived: the checker lost its teeth" >&2
        exit 1
    }
}

case $mode in
quick)
    "$B"/sim_place --seeds 200 || exit 1
    run_mutants "$PLACE_MUTANT_SEEDS"
    ;;
mutants)
    run_mutants "${2:-$PLACE_MUTANT_SEEDS}"
    ;;
soak)
    seeds=${2:-10000}
    jobs=${3:-$(nproc 2>/dev/null || echo 4)}
    first=${4:-1}
    case "$jobs" in '' | *[!0-9]*) echo "sim_place.sh: jobs must be >= 1" >&2; exit 2 ;; esac
    [ "$jobs" -ge 1 ] || { echo "sim_place.sh: jobs must be >= 1" >&2; exit 2; }
    chunk=$(( (seeds + jobs - 1) / jobs ))
    dir=$(mktemp -d "${TMPDIR:-/tmp}/sim_place.XXXXXX") || exit 1
    trap 'rm -rf "$dir"' EXIT
    echo "sim_place soak: $seeds seeds from $first on $jobs processes"
    for ((j = 0; j < jobs; j++)); do
        s=$(( first + j * chunk ))
        n=$chunk
        [ $(( s + n )) -gt $(( first + seeds )) ] && n=$(( first + seeds - s ))
        [ $n -gt 0 ] || continue
        "$B"/sim_place --first "$s" --seeds "$n" > "$dir/$j.log" 2>&1 &
    done
    rc=0
    for p in $(jobs -p); do wait "$p" || rc=1; done
    cat "$dir"/*.log
    if [ "$rc" != 0 ]; then
        echo "sim_place soak: FAILED" >&2
        exit 1
    fi
    run_mutants "$PLACE_MUTANT_SEEDS"
    echo "sim_place soak: ok"
    ;;
mttdl)
    "$B"/sim_place --mttdl all --disks-afr 0.02 --servers-afr 0.01 --rate 200
    ;;
*)
    echo "usage: tests/sim_place.sh quick | mutants [SEEDS] | soak [SEEDS] [JOBS] [FIRST] | mttdl" >&2
    exit 2
    ;;
esac
