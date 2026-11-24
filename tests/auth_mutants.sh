#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# The cluster login mutants (S2-1, TSDB_AUTH_MUTANT; server/auth.c and tools/cluster-user-cli.c): authlocalusers (a
# cluster server reads its root's USERS), adminlocal (an admin op over the client protocol answered without reaching
# the group), loginbeforecatchup (the restart barrier ignored), barrierifempty (it holds only while the copy has no
# users: a restarted server's copy from its snapshot lets logins in), noguard (an admin op proposed without the actor
# guard: a demoted superuser's change through a lagging server commits), retryunavail (an outcome unknown answered
# UNAVAILABLE, which clients send again), pushnoretry (a push of the users a server failed to take is not asked for
# again, and the barrier opens without it) must fail test_cluster login; importmerge (users that differ between the
# servers imported silently from the first) must fail userimport; nooplocal (a no-op or a refusal answered from the
# server's own copy, no read barrier) and syncnowait (the barrier's index not waited for) must fail userstale;
# nomembers (a no-op's barrier does not wait for the members) userstale too; pushrebuild (TSDB_CLUSTER_MUTANT: a
# promoted member's users watch given its copy while it is rebuilt) userpromote; holdempty (TSDB_CLUSTER_MUTANT: that
# hold only while the copy has no users) userreplay; syncnolease (TSDB_CLUSTER_MUTANT: the barrier answered by a
# leader without its lease) and uladdrtick (TSDB_CLUSTER_MUTANT: a new leader's users' change sent where the 100 ms
# duty last said the leader is, the one it replaced) userlease; bootempty (TSDB_CLUSTER_MUTANT: a member whose core
# starts shows an empty image in STATUS until the core catches up) userpromote. Each must fail with its own FAIL line
# (the pattern below), not with any failure: a mutant that only stops a server from starting would otherwise count as
# caught.
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
    nooplocal | syncnowait) echo "holds nowhere\|answered from its stale copy\|while the revoke stands\|not DENIED" ;;
    nomembers) echo "did not wait for the members" ;;
    pushrebuild) echo "made a voter: .* logins refused" ;;
    holdempty) echo "its users went back while its copy replayed" ;;
    syncnolease) echo "a barrier from a leader without its lease" ;;
    uladdrtick) echo "the leader it replaced" ;;
    bootempty) echo "its STATUS showed an empty image" ;;
    *) echo "FAIL" ;;
    esac
}
scenario() {
    case $1 in importmerge) s=userimport ;; nooplocal | syncnowait | nomembers) s=userstale ;;
    pushrebuild | bootempty) s=userpromote ;; holdempty) s=userreplay ;; syncnolease | uladdrtick) s=userlease ;; *) s=login ;; esac
}
# the mutants of server/cluster.c go in TSDB_CLUSTER_MUTANT, the rest in TSDB_AUTH_MUTANT
menv() { case $1 in pushrebuild | holdempty | syncnolease | uladdrtick | bootempty) echo TSDB_CLUSTER_MUTANT ;; *) echo TSDB_AUTH_MUTANT ;; esac; }
# one mutant, then its verdict: tools/mutants.sh runs them in parallel, each in a scratch directory of its own, and
# asks this script (--mutant-verify) whether the scenario failed where the mutant's own oracle is
case ${1:-} in
--mutant-run)
    scenario "$2"
    exec env "$(menv "$2")=$2" "$TEST_CLUSTER" "$3" "$s" ;;
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
muts=${*:-authlocalusers adminlocal loginbeforecatchup barrierifempty noguard retryunavail pushnoretry importmerge nooplocal syncnowait nomembers pushrebuild holdempty syncnolease uladdrtick bootempty}
# MUTANT_JOBS, MUTANT_URING_JOBS (tsdbd on io_uring), MUTANT_TIMEOUT
exec "$(dirname "$0")/../tools/mutants.sh" --name auth --tmp "$dir" --uring --timeout "${MUTANT_TIMEOUT:-600}" --fail \
    --verify "$0" --list "$muts" -- "$0" --mutant-run @M@ @D@
