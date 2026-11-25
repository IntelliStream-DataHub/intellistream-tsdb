#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# The cluster volumes' margins mutants (server/margins.c, TSDBD_MARGIN_MUTANT; r8o-margins): marginoff (the settled
# margin counted from k + m), youngrep (the young margin counted from the replicas, not the ISR) and lostasdown
# (destroyed positions taken for merely away: lost reads as unavailable) must each fail test_cluster margins.
# Usage: tests/margin_mutants.sh SCRATCH [MUTANT...] (from the top of the tree, after make build/tsdbd
# build/test_cluster build/tsdb).
set -u
export TSDBD=${TSDBD:-build/tsdbd} TSDB_CLI=${TSDB_CLI:-build/tsdb}
export TEST_CLUSTER=${TEST_CLUSTER:-$(dirname "$TSDBD")/test_cluster} # the same build as tsdbd (make tsan: build-tsan)
# one mutant (tools/mutants.sh runs them in parallel, each in a scratch directory of its own)
if [ "${1:-}" = --mutant-run ]; then
    TSDBD_MARGIN_MUTANT=$2 exec "$TEST_CLUSTER" "$3" margins
fi
dir=${1:-/tmp}; shift || true
muts=${*:-marginoff youngrep lostasdown}
# a mutant is caught by any failure of the scenario; MUTANT_JOBS, MUTANT_URING_JOBS (tsdbd on io_uring), MUTANT_TIMEOUT
exec "$(dirname "$0")/../tools/mutants.sh" --name margin --tmp "$dir" --uring --timeout "${MUTANT_TIMEOUT:-600}" --detail FAIL \
    --list "$muts" -- "$0" --mutant-run @M@ @D@
