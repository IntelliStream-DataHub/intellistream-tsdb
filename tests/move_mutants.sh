#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# The replica move's mutants (server/cmove.c, TSDB_CLUSTER_MUTANT): moveshrinkfirst (A shrunk out of the ISR before B
# joined) and moveremovebeforeshrink (REMOVE of A while it is in the ISR: meta refuses it, the move never ends) must fail
# test_cluster move; movenominisr (DROP_MEMBER with min_isr 0: A leaves while another member lags out) movelag;
# movenoreplaces (a move this Raft leader did not start is forgotten after an election: it never ends) moveraft;
# moveleaderfirst (REMOVE of the leader's replica without a transfer: refused by meta) moveleader.
# Usage: tests/move_mutants.sh SCRATCH [MUTANT...]
# (from the top of the tree, after make build/tsdbd build/test_cluster build/tsdb).
set -u
export TSDBD=${TSDBD:-build/tsdbd} TSDB_CLI=${TSDB_CLI:-build/tsdb}
export TEST_CLUSTER=${TEST_CLUSTER:-$(dirname "$TSDBD")/test_cluster} # the same build as tsdbd (make tsan: build-tsan)
scenario() {
    case $1 in
    movenominisr) s=movelag ;;
    movenoreplaces) s=moveraft ;;
    moveleaderfirst) s=moveleader ;;
    *) s=move ;;
    esac
}
# one mutant (tools/mutants.sh runs them in parallel, each in a scratch directory of its own)
if [ "${1:-}" = --mutant-run ]; then
    scenario "$2"
    TSDB_CLUSTER_MUTANT=$2 exec "$TEST_CLUSTER" "$3" "$s"
fi
dir=${1:-/tmp}; shift || true
muts=${*:-moveshrinkfirst moveremovebeforeshrink movenominisr movenoreplaces moveleaderfirst}
# a mutant is caught by any failure of its scenario; MUTANT_JOBS, MUTANT_URING_JOBS (tsdbd on io_uring), MUTANT_TIMEOUT
exec "$(dirname "$0")/../tools/mutants.sh" --name move --tmp "$dir" --uring --timeout "${MUTANT_TIMEOUT:-600}" \
    --detail FAIL --list "$muts" -- "$0" --mutant-run @M@ @D@
