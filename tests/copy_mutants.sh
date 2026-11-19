#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# The metadata copy's mutants (server/cluster.c, TSDB_CLUSTER_MUTANT; S2-1): snapfreeheld (a snapshot freed one
# reference early) must fail test_cluster copyhold; rolesswap (a replica outside the ISR shown as STANDBY), selfone
# (STATUS says node 1 on every server) and movedpeeraddr (MOVED names the leader's peer address) topology; kindall (the
# acceptor hands a data link of any kind to whatever peerlink is set) links; ccnowait (a swap frees the old snapshot
# without waiting for readers) and cconeslot (one reader counter, which starves the cluster thread) ccstress; ownerself
# (route_moved sends MOVED to the server itself) movedself.
# Usage: tests/copy_mutants.sh SCRATCH [MUTANT...] (from the top of the tree, after make build/tsdbd build/test_cluster
# build/tsdb).
set -u
dir=${1:-/tmp}; shift || true
muts=${*:-snapfreeheld rolesswap selfone movedpeeraddr kindall ccnowait cconeslot ownerself}
export TSDBD=${TSDBD:-build/tsdbd} TSDB_CLI=${TSDB_CLI:-build/tsdb}
survived=
for m in $muts; do
    case $m in snapfreeheld) s=copyhold ;; kindall) s=links ;; ccnowait | cconeslot) s=ccstress ;;
        ownerself) s=movedself ;; *) s=topology ;; esac
    if ! TSDB_CLUSTER_MUTANT=$m timeout 600 ./build/test_cluster "$dir" $s > "$dir/mut-$m-$s.log" 2>&1; then
        echo "mutant $m: caught by $s ($(grep -m1 FAIL "$dir/mut-$m-$s.log"))"
    else
        echo "mutant $m: SURVIVED"
        survived="$survived $m"
    fi
done
[ -z "$survived" ] || { echo "survived:$survived"; exit 1; }
echo "every mutant was caught"
