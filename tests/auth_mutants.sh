#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# The cluster login mutants (S2-1, TSDB_AUTH_MUTANT; server/auth.c and tools/cluster-user-cli.c): authlocalusers (a
# cluster server reads its root's USERS), adminlocal (an admin op over the client protocol answered without reaching
# the group), loginbeforecatchup (the restart barrier ignored), barrierifempty (it holds only while the copy has no
# users: a restarted server's copy from its snapshot lets logins in), noguard (an admin op proposed without the actor
# guard: a demoted superuser's change through a lagging server commits), retryunavail (an outcome unknown answered
# UNAVAILABLE, which clients send again), pushnoretry (a push of the users a server failed to take is not asked for
# again, and the barrier opens without it) must fail test_cluster login; importmerge (users that differ between the
# servers imported silently from the first) must fail userimport. Each must fail with its own FAIL line (the pattern
# below), not with any failure: a mutant that only stops a server from starting would otherwise count as caught.
# Usage: tests/auth_mutants.sh SCRATCH [MUTANT...] (from the top of the tree, after make build/tsdbd build/test_cluster
# build/tsdb).
set -u
export TSDBD=${TSDBD:-build/tsdbd} TSDB_CLI=${TSDB_CLI:-build/tsdb}
export TEST_CLUSTER=${TEST_CLUSTER:-$(dirname "$TSDBD")/test_cluster} # the same build as tsdbd (make tsan: build-tsan)
expect() {
    case $1 in
    authlocalusers) echo 'server 0: login as root\|no login as root\|did not start\|exited' ;;
    adminlocal) echo "old password after the passwd was answered" ;;
    loginbeforecatchup) echo "a login before the cluster exists\\|not caught up" ;;
    barrierifempty) echo "not caught up" ;;
    noguard) echo "created a superuser through lagging server 2" ;;
    retryunavail) echo "outcome unknown" ;;
    pushnoretry) echo "pushes taken again\|its users watch failing every push" ;;
    importmerge) echo "import of two USERS that differ" ;;
    *) echo "FAIL" ;;
    esac
}
scenario() { case $1 in importmerge) s=userimport ;; *) s=login ;; esac; }
# one mutant, then its verdict: tools/mutants.sh runs them in parallel, each in a scratch directory of its own, and
# asks this script (--mutant-verify) whether the scenario failed where the mutant's own oracle is
case ${1:-} in
--mutant-run)
    scenario "$2"
    TSDB_AUTH_MUTANT=$2 exec "$TEST_CLUSTER" "$3" "$s" ;;
--mutant-verify)
    scenario "$2"
    if [ "$3" -eq 0 ]; then echo "$s passed"; exit 1; fi
    if grep FAIL "$4" | grep -q "$(expect "$2")"; then
        echo "caught by $s ($(grep -m1 FAIL "$4"))"
    else
        echo "failed $s, but not where expected ($(expect "$2")): $(grep -m1 FAIL "$4"; tail -n 3 "$4")"
        exit 1
    fi
    exit 0 ;;
esac
dir=${1:-/tmp}; shift || true
muts=${*:-authlocalusers adminlocal loginbeforecatchup barrierifempty noguard retryunavail pushnoretry importmerge}
# MUTANT_JOBS, MUTANT_URING_JOBS (tsdbd on io_uring), MUTANT_TIMEOUT
exec "$(dirname "$0")/../tools/mutants.sh" --name auth --tmp "$dir" --uring --timeout "${MUTANT_TIMEOUT:-600}" --fail \
    --verify "$0" --list "$muts" -- "$0" --mutant-run @M@ @D@
