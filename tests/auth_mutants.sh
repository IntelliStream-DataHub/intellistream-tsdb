#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# The cluster login mutants (S2-1, TSDB_AUTH_MUTANT; server/auth.c and tools/cluster-user-cli.c): authlocalusers (a
# cluster server reads its root's USERS), adminlocal (an admin op over the client protocol answered without reaching
# the group), loginbeforecatchup (the restart barrier ignored) and barrierifempty (it holds only while the copy has no
# users: a restarted server's copy from its snapshot lets logins in) must fail test_cluster login; importmerge (users
# that differ between the servers imported silently from the first) must fail userimport.
# Usage: tests/auth_mutants.sh SCRATCH [MUTANT...] (from the top of the tree, after make build/tsdbd build/test_cluster
# build/tsdb).
set -u
dir=${1:-/tmp}; shift || true
muts=${*:-authlocalusers adminlocal loginbeforecatchup barrierifempty importmerge}
export TSDBD=${TSDBD:-build/tsdbd} TSDB_CLI=${TSDB_CLI:-build/tsdb}
survived=
for m in $muts; do
    case $m in importmerge) s=userimport ;; *) s=login ;; esac
    if ! TSDB_AUTH_MUTANT=$m timeout 600 ./build/test_cluster "$dir" $s > "$dir/mut-$m-$s.log" 2>&1; then
        echo "mutant $m: caught by $s ($(grep -m1 FAIL "$dir/mut-$m-$s.log"))"
    else
        echo "mutant $m: SURVIVED"
        survived="$survived $m"
    fi
done
[ -z "$survived" ] || { echo "survived:$survived"; exit 1; }
echo "every mutant was caught"
