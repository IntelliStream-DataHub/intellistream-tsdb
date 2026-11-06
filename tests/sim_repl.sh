#!/bin/bash
# The replication core's simulator (tests/sim_repl.c, docs/design-cluster.md §13.1).
#   tests/sim_repl.sh quick            make test: 300 seeds, and the planted bugs that die within 200 seeds
#   tests/sim_repl.sh mutants          every planted bug, up to 20,000 seeds each; fails if one survives
#   tests/sim_repl.sh soak [SEEDS] [JOBS] [FIRST]   SEEDS seeds (1,000,000) on JOBS processes (nproc), then mutants
# B= the build directory (build).
set -u
B=${B:-build}
mode=${1:-quick}

# Each planted bug (MUT(n) in src/repl.c) and the seeds it gets. 11 is caught only with 13: a replica that the
# metadata promotes by mistake (another rid) resigns on its own, so 11 alone is an equivalent mutant.
#   1 commit point skips ISR members not yet heard from in the epoch      8 leader counts ACKs of older epochs
#   2 follower takes APPENDs of any epoch                                  9 ACK reports the written end as durable
#   3 follower applies on a commit point before it verified its log       10 promotion ignores the ISR version
#   4 returning replica keeps its divergent tail                          11 promotion ignores the replica instance
#   5 replica leads an epoch it led before a restart                      12 learner joins the ISR below the epoch start
#   6 commit ignores a joining member (no maximal ISR)                    13 replica leads though the record names
#   7 leader acknowledges after its own sync, before commit                  another instance of it (a wiped disk)
#  14 promotion keeps only the members that answered the fence in the ISR (design §6.2 as first written): slow
#     replies shrink the ISR to one copy, whose loss then stops the volume although two intact copies are left.
#     Found once (seed 183 of an earlier configuration of the simulator); too rare now to be a required kill.
#  15 a replica takes an ANSWER by query number alone: the number restarts with the process, and an answer to the
#     previous incarnation's query (asked with an empty log) cut a whole log (seed 4985 fails with it)
quick="1 3 4 5 6 7 9 12 13 11,13"
all="1 2 3 4 5 6 7 8 9 10 12 13 11,13 15"

mutants() {
    local seeds=$1 bad=0 m out
    for m in $2; do
        out=$("$B"/sim_repl_mut --mutant "$m" --seeds "$seeds" 2>/dev/null)
        echo "$out"
        case $out in *KILLED*) ;; *) bad=1 ;; esac
    done
    [ $bad = 0 ] || { echo "sim_repl.sh: a planted bug survived: the checker lost its teeth"; exit 1; }
}

case $mode in
quick)
    "$B"/sim_repl --seeds 300 || exit 1
    # Seeds of the first 1M-seed soak where a replica still following a deposed leader cut records the next
    # epoch had committed (legitimate: the checker was too strict; see the TRUNCATE check in sim_repl.c).
    for s in 43308 200636 294653 478816 570201 653518 756115 903046; do
        "$B"/sim_repl --seed $s > /dev/null || exit 1
    done
    mutants 200 "$quick"
    ;;
mutants)
    mutants 20000 "$all"
    ;;
soak)
    seeds=${2:-1000000} jobs=${3:-$(nproc)} first=${4:-1}
    chunk=$(( (seeds + jobs - 1) / jobs ))
    dir=$(mktemp -d "${TMPDIR:-/tmp}/sim_repl.XXXXXX") || exit 1
    echo "sim_repl soak: $seeds seeds from $first on $jobs processes, logs in $dir"
    for ((j = 0; j < jobs; j++)); do
        s=$(( first + j * chunk )); n=$chunk
        [ $(( s + n )) -gt $(( first + seeds )) ] && n=$(( first + seeds - s ))
        [ $n -gt 0 ] || continue
        "$B"/sim_repl --first $s --seeds $n > "$dir/$j.log" 2>&1 &
    done
    rc=0
    for p in $(jobs -p); do wait "$p" || rc=1; done
    cat "$dir"/*.log
    [ $rc = 0 ] || { echo "sim_repl soak: FAILED (logs in $dir)"; exit 1; }
    mutants 20000 "$all"
    echo "sim_repl soak: ok"
    ;;
*)
    echo "usage: tests/sim_repl.sh quick | mutants | soak [SEEDS] [JOBS] [FIRST]" >&2
    exit 2
    ;;
esac
