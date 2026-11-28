#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# The placement planner's simulator (tests/sim_place.c, docs/design-cluster-data.md §10.2, §15.9; tracks r9-simplace, r13-sim).
#   tests/sim_place.sh quick                       200 honest seeds + 200 --classes seeds, then every active mutant
#   tests/sim_place.sh mutants [SEEDS]             every active planted bug (standard + --classes), up to SEEDS each
#   tests/sim_place.sh class-mutants [SEEDS]       R13 disk/class planted bugs under --classes
#   tests/sim_place.sh soak [SEEDS] [JOBS] [FIRST] SEEDS honest seeds (default 10000) on JOBS procs, then mutants
#   tests/sim_place.sh soak-classes [JOBS] [FIRST] R13 --classes soak: PLACE_CLASS_SEEDS (>0) or PLACE_SOAK_HOURS (default 4h)
#   tests/sim_place.sh mttdl                       print MTTDL table across geometries at 10 and 100 TB/node
# B= the build directory (build).
set -u
B=${B:-build}
mode=${1:-quick}

# Active planted mutants present in server/place.c, server/pout.c, server/balance.c, and tests/sim_place.c:
#   place.c   (r9-plan, r9-return): orderfifo rackignored twoperserver moveshrinks wholenobarrier takebackwhole partialdelnow
#   place.c   (r13-place, r13-sim): loghdd mixstripe placefull usechange
#   pout.c    (r9-out):             noguard guardunbounded holdignored openingunbounded failstopslow
#   balance.c (r9-balance):         balanceunbounded
#   sim_place (r9-sim):             deleteunmarked belowk
mutants_active="orderfifo rackignored twoperserver moveshrinks wholenobarrier noguard guardunbounded holdignored openingunbounded failstopslow deleteunmarked belowk takebackwhole partialdelnow balanceunbounded"
mutants_classes="loghdd mixstripe placefull usechange twoperserver"

PLACE_MUTANT_SEEDS=${PLACE_MUTANT_SEEDS:-2000}

case $mode in
--mutant-run)
    m=$2
    exec "$B"/sim_place --mutant "$m" --seeds "$PLACE_MUTANT_SEEDS" 2>&1
    ;;
--mutant-run-classes)
    m=$2
    exec "$B"/sim_place --classes --mutant "$m" --seeds "$PLACE_MUTANT_SEEDS" 2>&1
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
    balanceunbounded) pat='exceeded 25% plan cap' ;;
    loghdd)           pat='(R13 log capability/class|HDD-only node|from fast node .* to HDD-only)' ;;
    mixstripe)        pat='(mixed disk classes|across disk classes)' ;;
    placefull)        pat='(young_move_pct fill mark|chunk fill mark)' ;;
    usechange)        pat='disk use/capability' ;;
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

run_class_mutants() {
    local seeds=$1
    PLACE_MUTANT_SEEDS=$seeds "$(dirname "$0")"/../tools/mutants.sh --name sim_place_classes \
        --tmp /tmp --timeout "${MUTANT_TIMEOUT:-600}" --verify "$0" --list "$mutants_classes" \
        -- "$0" --mutant-run-classes @M@ @D@ || {
        echo "sim_place.sh: a planted R13 class bug survived: the checker lost its teeth" >&2
        exit 1
    }
}

run_parallel_batch() {
    local extra_flag=$1 seeds=$2 jobs=$3 first=$4
    local chunk=$(( (seeds + jobs - 1) / jobs ))
    local dir
    dir=$(mktemp -d "${TMPDIR:-/tmp}/sim_place.XXXXXX") || return 1
    trap 'rm -rf "$dir"' EXIT INT TERM
    local j s n rc=0
    for ((j = 0; j < jobs; j++)); do
        s=$(( first + j * chunk ))
        n=$chunk
        [ $(( s + n )) -gt $(( first + seeds )) ] && n=$(( first + seeds - s ))
        [ $n -gt 0 ] || continue
        if [ -n "$extra_flag" ]; then
            "$B"/sim_place "$extra_flag" --first "$s" --seeds "$n" > "$dir/$j.log" 2>&1 &
        else
            "$B"/sim_place --first "$s" --seeds "$n" > "$dir/$j.log" 2>&1 &
        fi
    done
    for p in $(jobs -p); do wait "$p" || rc=1; done
    cat "$dir"/*.log
    rm -rf "$dir"
    trap - EXIT INT TERM
    return "$rc"
}

case $mode in
quick)
    "$B"/sim_place --seeds 200 || exit 1
    "$B"/sim_place --classes --seeds 200 || exit 1
    run_mutants "$PLACE_MUTANT_SEEDS"
    run_class_mutants "$PLACE_MUTANT_SEEDS"
    ;;
mutants)
    run_mutants "${2:-$PLACE_MUTANT_SEEDS}"
    run_class_mutants "${2:-$PLACE_MUTANT_SEEDS}"
    ;;
class-mutants)
    run_class_mutants "${2:-$PLACE_MUTANT_SEEDS}"
    ;;
soak)
    seeds=${2:-10000}
    jobs=${3:-$(nproc 2>/dev/null || echo 4)}
    first=${4:-1}
    case "$jobs" in '' | *[!0-9]*) echo "sim_place.sh: jobs must be >= 1" >&2; exit 2 ;; esac
    [ "$jobs" -ge 1 ] || { echo "sim_place.sh: jobs must be >= 1" >&2; exit 2; }
    echo "sim_place soak: $seeds seeds from $first on $jobs processes"
    if ! run_parallel_batch "" "$seeds" "$jobs" "$first"; then
        echo "sim_place soak: FAILED" >&2
        exit 1
    fi
    run_mutants "$PLACE_MUTANT_SEEDS"
    run_class_mutants "$PLACE_MUTANT_SEEDS"
    echo "sim_place soak: ok"
    ;;
soak-classes)
    jobs=${2:-$(nproc 2>/dev/null || echo 4)}
    first=${3:-1}
    case "$jobs" in '' | *[!0-9]*) echo "sim_place.sh: jobs must be >= 1" >&2; exit 2 ;; esac
    [ "$jobs" -ge 1 ] || { echo "sim_place.sh: jobs must be >= 1" >&2; exit 2; }
    cseeds=${PLACE_CLASS_SEEDS:-0}
    hours=${PLACE_SOAK_HOURS:-4}
    secs=${PLACE_SOAK_SECONDS:-0}
    if [ "$cseeds" -gt 0 ] 2>/dev/null; then
        echo "sim_place --classes soak: $cseeds seeds from $first on $jobs processes"
        if ! run_parallel_batch "--classes" "$cseeds" "$jobs" "$first"; then
            echo "sim_place --classes soak: FAILED" >&2
            exit 1
        fi
        run_class_mutants "$PLACE_MUTANT_SEEDS"
        echo "sim_place --classes soak: ok ($cseeds seeds)"
    else
        if [ "$secs" -le 0 ] 2>/dev/null; then
            secs=$(( hours * 3600 ))
        fi
        [ "$secs" -gt 0 ] || { echo "sim_place.sh: PLACE_SOAK_HOURS/SECONDS must be > 0" >&2; exit 2; }
        start_s=$(date +%s)
        end_s=$(( start_s + secs ))
        batch=$(( jobs * 100 ))
        cur=$first
        total=0
        echo "sim_place --classes soak: ${secs}s from seed $first on $jobs processes (batch=$batch)"
        while :; do
            if ! run_parallel_batch "--classes" "$batch" "$jobs" "$cur"; then
                echo "sim_place --classes soak: FAILED (batch first=$cur count=$batch)" >&2
                exit 1
            fi
            cur=$(( cur + batch ))
            total=$(( total + batch ))
            now_s=$(date +%s)
            [ "$now_s" -ge "$end_s" ] && break
        done
        run_class_mutants "$PLACE_MUTANT_SEEDS"
        echo "sim_place --classes soak: ok ($total seeds in $(( $(date +%s) - start_s ))s)"
    fi
    ;;
mttdl)
    "$B"/sim_place --mttdl all --disks-afr 0.02 --servers-afr 0.01 --rate 200
    ;;
*)
    echo "usage: tests/sim_place.sh quick | mutants [SEEDS] | class-mutants [SEEDS] | soak [SEEDS] [JOBS] [FIRST] | soak-classes [JOBS] [FIRST] | mttdl" >&2
    exit 2
    ;;
esac
