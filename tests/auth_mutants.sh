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
dir=${1:-/tmp}; shift || true
muts=${*:-authlocalusers adminlocal loginbeforecatchup barrierifempty noguard retryunavail pushnoretry importmerge}
export TSDBD=${TSDBD:-build/tsdbd} TSDB_CLI=${TSDB_CLI:-build/tsdb}
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
survived=
for m in $muts; do
    case $m in importmerge) s=userimport ;; *) s=login ;; esac
    log="$dir/mut-$m-$s.log"
    if TSDB_AUTH_MUTANT=$m timeout 600 ./build/test_cluster "$dir" $s > "$log" 2>&1; then
        echo "mutant $m: SURVIVED"
        survived="$survived $m"
    elif grep FAIL "$log" | grep -q "$(expect "$m")"; then
        echo "mutant $m: caught by $s ($(grep -m1 FAIL "$log"))"
    else
        echo "mutant $m: failed $s, but not where expected ($(expect "$m")): $(grep -m1 FAIL "$log"; tail -n 3 "$log")"
        survived="$survived $m(wrong-failure)"
    fi
done
[ -z "$survived" ] || { echo "survived:$survived"; exit 1; }
echo "every mutant was caught"
