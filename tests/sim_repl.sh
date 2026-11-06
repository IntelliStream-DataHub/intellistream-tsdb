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
#     replies shrink the ISR to one copy, whose loss then stops the volume although intact copies are left
#  15 a replica takes an ANSWER by query number alone: the number restarts with the process, and an answer to the
#     previous incarnation's query (asked with an empty log) cut a whole log (seed 4985 failed with it)
# From the review of 13081cc (16-31) and its fixes (34-37):
#  16 end_of_epoch one past the epoch's end          17 ACK reports durable + 1
#  18 commit over the requested ISR alone            19 a new replica instance keeps its predecessor's match
#  20 follower takes an ANSWER of any epoch/sender   21 follower applies past its durable end
#  22 a raised fence is not saved                    23 lead() saves before it sets `led`
#  26 follower ignores its fence                     27 leader never goes back on lost appends
#  28 follower never resends a lost QUERY            29 the lag rule never drops a member (availability, check 8)
#  31 min ISR ignored (check 7)                      34 promotion drops the old leader that did not answer (seed 76)
#  35 repl_synced believes a sync issued before a cut (with the naive driver)
#  36 a position the log no longer retains fails the core (no snapshot path)
#  25 promotion takes fence replies from outside the fenced ISR: caught only with 11 (a non-member has no rid in the
#     record), and only because some runs fence every replica.
# Equivalent, so not run: 11 and 25 alone (above); 24 (cut() rounds up: the next question cuts further); 30 (no
# prev_epoch check while following: never reached, the core fails there now); 37 (unanswered members wait for the lag
# rule: slower, not wrong; test_repl's `unanswered` scenario catches it).
quick="1 2 3 4 5 6 7 8 9 12 13 11,13 14 16 17 18 19 20 21 22 23 26 27 28 29 31 34 35 36"
all="1 2 3 4 5 6 7 8 9 10 12 13 11,13 14 15 16 17 18 19 20 21 22 23 11,25 26 27 28 29 31 34 35 36"
# test_repl's scenarios and the planted bug each must catch
scenarios="34 35 36 37"

mutants() {
    local seeds=$1 bad=0 m out
    for m in $2; do
        out=$("$B"/sim_repl_mut --mutant "$m" --seeds "$seeds" 2>/dev/null)
        echo "$out"
        case $out in *KILLED*) ;; *) bad=1 ;; esac
    done
    for m in $scenarios; do
        if "$B"/test_repl_mut --mutant "$m" > /dev/null 2>&1; then
            echo "test_repl: planted bug $m: SURVIVED"
            bad=1
        else
            echo "test_repl: planted bug $m: KILLED"
        fi
    done
    [ $bad = 0 ] || { echo "sim_repl.sh: a planted bug survived: the checker lost its teeth"; exit 1; }
}

case $mode in
quick)
    "$B"/test_repl || exit 1
    "$B"/sim_repl --seeds 300 || exit 1
    # Seeds of the first 1M-seed soak where a replica still following a deposed leader cut records the next
    # epoch had committed (legitimate: the checker was too strict; see the TRUNCATE check in sim_repl.c; the
    # schedules have changed since). 75881 3865 25133: a leader that went back to a position its follower had, and
    # had trimmed, sent snapshot notices for ever (the follower now ACKs where it is).
    for s in 43308 200636 294653 478816 570201 653518 756115 903046 75881 3865 25133; do
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
