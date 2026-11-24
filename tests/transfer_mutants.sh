#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# The planned leader transfer's mutants (server/rtransfer.c, TSDB_REPLD_MUTANT; server/ctransfer.c, TSDB_CLUSTER_MUTANT):
# transfernodrain (READY at once, without the drain: the target is promoted short of the old leader's end) and
# transferadmit (the old leader admits writes during the drain) must fail test_cluster transfer, as must
# transferanycand (the targeted promotion takes any candidate, not the target) and transferfewreplies (it goes ahead
# without the replies the rule needs); transferresume (the old leader admits again on its own timer after READY, with
# no answer from the metadata leader: records admitted blind) transferhold; balanceflap (the leader balance without its
# hold and backoff) balancecut; nocancel (a new Raft leader sends no CANCEL for the transfers of the old term: the
# volume leader holds its writes until the hard cap) transferraftkill.
# Usage: tests/transfer_mutants.sh SCRATCH [MUTANT...]
# (from the top of the tree, after make build/tsdbd build/test_cluster build/tsdb).
set -u
export TSDBD=${TSDBD:-build/tsdbd} TSDB_CLI=${TSDB_CLI:-build/tsdb}
export TEST_CLUSTER=${TEST_CLUSTER:-$(dirname "$TSDBD")/test_cluster} # the same build as tsdbd (make tsan: build-tsan)
scenario() {
    v=TSDB_CLUSTER_MUTANT
    case $1 in
    transfernodrain|transferadmit) s=transfer v=TSDB_REPLD_MUTANT ;;
    transferresume) s=transferhold v=TSDB_REPLD_MUTANT ;;
    balanceflap) s=balancecut ;;
    nocancel) s=transferraftkill ;;
    *) s=transfer ;;
    esac
}
# one mutant (tools/mutants.sh runs them in parallel, each in a scratch directory of its own)
if [ "${1:-}" = --mutant-run ]; then
    scenario "$2"
    export TR_TRANSFERS=${TR_TRANSFERS:-5}
    env "$v=$2" "$TEST_CLUSTER" "$3" "$s"
    exit
fi
dir=${1:-/tmp}; shift || true
muts=${*:-transfernodrain transferadmit transferresume transferanycand transferfewreplies balanceflap nocancel}
# a mutant is caught by any failure of its scenario; MUTANT_JOBS, MUTANT_URING_JOBS (tsdbd on io_uring), MUTANT_TIMEOUT
exec "$(dirname "$0")/../tools/mutants.sh" --name transfer --tmp "$dir" --uring --timeout "${MUTANT_TIMEOUT:-600}" \
    --detail FAIL --list "$muts" -- "$0" --mutant-run @M@ @D@
