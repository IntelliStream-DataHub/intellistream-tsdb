#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# The replication core's simulator (tests/sim_repl.c, docs/design-cluster.md §13.1).
#   tests/sim_repl.sh quick            make test: 300 seeds, and the planted bugs that die within 1000 seeds (M@N: N seeds)
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
#  40 a silent member stays in the ISR while nothing is written (test_repl idle_shrink)
#  41 a learner within the hold's bound is asked for at once, without waiting for the commit point (the hold of
#     isr_check, r3d-isr liveness: join_hold_ticks on in 3 seeds of 4)
#  42 no hold: a learner under writes that never pause never joins (test_repl steady_join, join_giveup)
# The replay floor (r3b, design §5.4): 38 an ACK reports durable + 1 as its log start; 39 the floor ignores members
#     not heard from in this epoch
# Replica set changes (r4-core, design-cluster.md 8.1-8.3, D8 D11 D14 D17): 43 reconfnoremap (peer state, the ISR
#     and a pending request stay by index across a set change), 44 setvaccept (a message of another set version is
#     taken), 45 removedcounts (a removed member's last match still holds the commit point), 46 replayabove (the floor
#     is the leader's own replay start), 47 snapatorbelow (a snapshot at or below the log end is wanted), 48 leaveflap
#     (a leaving member is asked for again), 49 leaveunsafe (the move's shrink ignores its min_isr), 50 targetany (a
#     planned promotion ignores its target: test_repl only)
#  25 promotion takes fence replies from outside the fenced ISR: caught only with 11 (a non-member has no rid in the
#     record), and only because some runs fence every replica.
# Equivalent, so not run: 11 and 25 alone (above); 24 (cut() rounds up: the next question cuts further); 30 (no
# prev_epoch check while following: the simulator never reaches it; test_repl's `follower_damage` is its check);
# 37 (unanswered members wait for the lag rule: slower, not wrong; test_repl's `unanswered` scenario catches it).
# 20@3000: since the idle heartbeats (idle_heartbeat_ticks varies by seed) it first dies at seed 2324.
quick="1 2 3 4 5 6 7 8 9 12 13 11,13 14 16 17 18 19 20@3000 21 22 23 26 27 28 29 31 34 35 36 41"
all="1 2 3 4 5 6 7 8 9 10 12 13 11,13 14 15 16 17 18 19 20 21 22 23 11,25 26 27 28 29 31 34 35 36 38 39 41"
# test_repl's scenarios: BUG:SCENARIO, the planted bug and the scenario that must report it. The bug has to make the
# run exit 1 with "scenario(s) failed" and a failure line of that scenario: a crash (139), an unknown bug number (2)
# or a missing binary (127) is not a planted bug caught. 24 (rounding a cut up) and 11 (on its own) cannot be seen
# by the simulator, so a scenario is their only planted-bug check; 14 is also in the simulator's list.
scenarios="30:follower_damage 34:promote 35:sync_cut 36:trim 36:cut_applied 37:unanswered 24:round_cut 11:promote
           14:promote 39:replay_floor 40:idle_shrink 42:steady_join 43:reconfig 44:reconfig 45:reconfig 46:replay
           47:snapshot 48:leave 49:leave 50:target"

# One planted bug (tools/mutants.sh runs them in parallel, each in a scratch directory of its own, and asks this
# script, with --mutant-verify, whether it was killed): "sim:M[@N]" is the simulator with MUT(M) for N seeds (the
# second argument of mutants()), "scn:B:S" test_repl with planted bug B, which scenario S must report.
SIM_SEEDS=${SIM_SEEDS:-20000}
case $mode in
--mutant-run)
    case $2 in
    scn:*:*) m=${2#scn:} && exec "$B"/test_repl_mut --mutant "${m%%:*}" 2>&1 ;;
    sim:*)
        m=${2#sim:} n=$SIM_SEEDS
        case $m in *@*) n=${m#*@} m=${m%@*} ;; esac
        exec "$B"/sim_repl_mut --mutant "$m" --seeds "$n" 2>/dev/null ;;
    esac
    exit 2 ;;
--mutant-verify)
    # $3 the exit status, $4 the log. A scenario's bug is killed only by exit 1 with a failure line of that scenario
    # before "scenario(s) failed": a crash (139), an unknown bug number (2) or a missing binary (127) is not caught.
    case $2 in
    scn:*:*)
        s=${2##*:}
        [ "$3" = 1 ] && awk -v s="test_repl: $s:" 'index($0, s) { f = 1 } f && /scenario\(s\) failed/ { ok = 1 }
            END { exit !ok }' "$4" && echo "KILLED by $s" ;;
    scn:*) exit 2 ;;
    *) grep -m1 KILLED "$4" ;;
    esac
    exit ;;
esac

mutants() {
    local seeds=$1 list= m
    for m in $2; do list="$list sim:$m"; done
    for m in $scenarios; do list="$list scn:$m"; done
    # MUTANT_JOBS, MUTANT_TIMEOUT; one that survives (or hangs) fails the run
    SIM_SEEDS=$seeds "$(dirname "$0")"/../tools/mutants.sh --name sim_repl --timeout "${MUTANT_TIMEOUT:-3600}" \
        --verify "$0" --list "$list" -- "$0" --mutant-run @M@ @D@ || {
        echo "sim_repl.sh: a planted bug survived: the checker lost its teeth"
        exit 1
    }
}

case $mode in
quick)
    "$B"/test_repl || exit 1
    "$B"/sim_repl --seeds 300 || exit 1
    # Seeds of the first 1M-seed soak where a replica still following a deposed leader cut records the next
    # epoch had committed (legitimate: the checker was too strict; see the TRUNCATE check in sim_repl.c; the
    # schedules have changed since). 75881 3865 25133: a leader that went back to a position its follower had, and
    # had trimmed, sent snapshot notices for ever (the follower now ACKs where it is). 299557 436347: a leader's
    # shrink to itself applied in the tick it lost its disk, then promoted again (D1; the checker took it for P-F1).
    for s in 43308 200636 294653 478816 570201 653518 756115 903046 75881 3865 25133 299557 436347; do
        "$B"/sim_repl --seed $s > /dev/null || exit 1
    done
    mutants 1000 "$quick"
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
