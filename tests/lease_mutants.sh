#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# The lease and liveness mutants (server/cluster.c, TSDB_CLUSTER_MUTANT): leaseanyvoter (any voter's HB_ACK extends the
# lease) and nodrift (lease_ms without the drift margin) must fail test_cluster lease, stampignored (heartbeats whatever
# the serving loop's stamp says) must fail hungloop; nocoreclock (the grant judged on the core's clock as it stood, the
# code before the review's F1) must fail lease, noquorum (grants without the core's lease) leasequorum, nopromotewait
# (members down after node_timeout alone) leasedown, shrinknocond (an ISR change proposed on the record's current epoch
# and version instead of the request's) isr, upwaits (a new Raft leader records a restarted member's incarnation only
# after node_timeout, so a restarted volume leader's RESIGN waits for detection) resign, cutshrinks (a leader cut from
# its replicas on the data links shrinks the ISR to itself instead of stepping aside) cut, probelate (a core restarted
# at another peer address learns the others' addresses only after NOLEADER_PROBE_NS, 10 s) resign, leasestale (a node
# that reports itself the writer of a volume another leads still gets a lease) leasestale, fencelease (an old leader
# whose fence answer a promotion waits for keeps its lease: no promotion) and oldpass (a promotion without the old
# leader's answer or the end of its lease: it answers reads after the new leader's first write) and movegiveup (a move
# with a replica's answer gives up waiting for the old leader's lease: the ISR shrinks to it, no new leader) oneway,
# anyepoch (a late fence answer of an earlier epoch counts as the old leader's: it answers reads after the new leader's
# first write) oneway2, nocuthold (a shrink below 2 goes ahead before the member it drops reported the cut: an ISR of
# the cut leader alone, so no replica can be promoted) cuthold, forceup (force-leader goes ahead while an ISR member is
# up) noowner; dropnotcommitted (a user op answered OK when proposed), caughtupearly (cluster_users_caught_up true from
# the start) and floornoapplied (true once a leader's commit is heard, before the copy applied it) users.
# Usage: tests/lease_mutants.sh SCRATCH [MUTANT...]
# (from the top of the tree, after make build/tsdbd build/test_cluster build/tsdb).
set -u
dir=${1:-/tmp}; shift || true
muts=${*:-leaseanyvoter nodrift stampignored nocoreclock noquorum nopromotewait shrinknocond upwaits cutshrinks probelate leasestale deposedstuck fencelease oldpass movegiveup anyepoch nocuthold forceup dropnotcommitted caughtupearly floornoapplied}
export TSDBD=${TSDBD:-build/tsdbd} TSDB_CLI=${TSDB_CLI:-build/tsdb}
survived=
for m in $muts; do
    case $m in stampignored) s=hungloop ;; noquorum) s=leasequorum ;; nopromotewait) s=leasedown ;; shrinknocond) s=isr ;; upwaits|probelate) s=resign ;; cutshrinks) s=cut ;; leasestale) s=leasestale ;; deposedstuck) s=cutstuck ;; fencelease|oldpass|movegiveup) s=oneway ;; anyepoch) s=oneway2 ;; nocuthold) s=cuthold ;; forceup) s=noowner ;; dropnotcommitted|caughtupearly|floornoapplied) s=users ;; *) s=lease ;; esac
    if ! TSDB_CLUSTER_MUTANT=$m timeout 600 ./build/test_cluster "$dir" $s > "$dir/mut-$m-$s.log" 2>&1; then
        echo "mutant $m: caught by $s ($(grep -m1 FAIL "$dir/mut-$m-$s.log"))"
    else
        echo "mutant $m: SURVIVED"
        survived="$survived $m"
    fi
done
[ -z "$survived" ] || { echo "survived:$survived"; exit 1; }
echo "every mutant was caught"
