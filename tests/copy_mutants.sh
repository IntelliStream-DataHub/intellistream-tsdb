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
export TSDBD=${TSDBD:-build/tsdbd} TSDB_CLI=${TSDB_CLI:-build/tsdb}
export TEST_CLUSTER=${TEST_CLUSTER:-$(dirname "$TSDBD")/test_cluster} # the same build as tsdbd (make tsan: build-tsan)
scenario() {
    case $1 in snapfreeheld) s=copyhold ;; kindall) s=links ;; ccnowait | cconeslot) s=ccstress ;;
        ownerself) s=movedself ;; *) s=topology ;; esac
}
# one mutant (tools/mutants.sh runs them in parallel, each in a scratch directory of its own)
if [ "${1:-}" = --mutant-run ]; then
    scenario "$2"
    TSDB_CLUSTER_MUTANT=$2 exec "$TEST_CLUSTER" "$3" "$s"
fi
dir=${1:-/tmp}; shift || true
muts=${*:-snapfreeheld rolesswap selfone movedpeeraddr kindall ccnowait cconeslot ownerself}
# a mutant is caught by any failure of its scenario; MUTANT_JOBS, MUTANT_URING_JOBS (tsdbd on io_uring), MUTANT_TIMEOUT
exec "$(dirname "$0")/../tools/mutants.sh" --name copy --tmp "$dir" --uring --timeout "${MUTANT_TIMEOUT:-600}" --detail FAIL \
    --list "$muts" -- "$0" --mutant-run @M@ @D@
