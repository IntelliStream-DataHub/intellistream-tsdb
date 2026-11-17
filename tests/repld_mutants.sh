#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# The replication driver's mutants (server/repld.c, TSDB_REPLD_MUTANT): test_cluster's replicate and replindet run
# with each planted bug, and each must fail one of them. The ISR mutants have their own scenario: commitpastremoved
# (META handed to the core as soon as it asks, before the metadata group committed it) must fail isr, nominisr (the
# core without min_isr) and ackbelowminisr (APPLY not held while the ISR is below min_isr) minisr, shipperfile (30 s
# for each file of a COMMIT) shipstall, nocap (no keep caps) keepcap, noseal (no idle flush) idleseal,
# noinboxbound (an unbounded inbox) and nospill (every bulk file whole in memory) backpressure, keepignorespeers (the
# keep bound ignores what the replicas acked) keepfloor, nologstartcheck (the core never hears the log start moved)
# keepcap, stopnodepose (the stop leaves the leader in place: a write waiting for its commit point holds the
# shutdown), lateclock (a new leader starts its NOW markers only at its first SAVE as a writer) closegone,
# syncbyrep (a writer's SYNC answered only when the WAL moved past the engine's last report) writersync (its knobs
# syncbyhook and synclate make the race certain), inboxwaitall (a full inbox's every frame waits on the shared link) inboxshared, shipnoisr (the ship
# wait ignores a replica's exit from the ISR) shipstall.
# Usage: tests/repld_mutants.sh SCRATCH [MUTANT...]
# (from the top of the tree, after make build/tsdbd build/test_cluster build/tsdb build/libcrashfs.so).
set -u
dir=${1:-/tmp}; shift || true
muts=${*:-commitbyleader ackbeforesync floornotsaved commitpastremoved nominisr ackbelowminisr shipperfile nocap noseal noinboxbound nospill keepignorespeers nologstartcheck stopnodepose lateclock syncbyrep inboxwaitall shipnoisr}
export CRASHFS_SO=${CRASHFS_SO:-build/libcrashfs.so} TSDBD=${TSDBD:-build/tsdbd} TSDB_CLI=${TSDB_CLI:-build/tsdb}
survived=
for m in $muts; do
    caught=
    case $m in commitpastremoved) scens=isr ;; nominisr|ackbelowminisr) scens=minisr ;; shipperfile|shipnoisr) scens=shipstall ;; nocap) scens=keepcap ;; noseal) scens=idleseal ;; noinboxbound|nospill) scens=backpressure ;; keepignorespeers) scens=keepfloor ;; nologstartcheck) scens=keepcap ;; stopnodepose|lateclock) scens=closegone ;; syncbyrep) scens=writersync ;; inboxwaitall) scens=inboxshared ;; *) scens="replindet replicate" ;; esac
    for s in $scens; do
        if ! TSDB_REPLD_MUTANT=$m timeout 600 ./build/test_cluster "$dir" $s > "$dir/mut-$m-$s.log" 2>&1; then
            caught=$s
            break
        fi
    done
    if [ -n "$caught" ]; then
        echo "mutant $m: caught by $caught ($(grep -m1 FAIL "$dir/mut-$m-$caught.log"))"
    else
        echo "mutant $m: SURVIVED"
        survived="$survived $m"
    fi
done
[ -z "$survived" ] || { echo "survived:$survived"; exit 1; }
echo "every mutant was caught"
