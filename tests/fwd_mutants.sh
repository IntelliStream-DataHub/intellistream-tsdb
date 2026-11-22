#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# The forwarding mutants (server/tsdbd.c, TSDBD_FWD_MUTANT) that test_cluster forward must catch: fwdoldframe (no age
# check: a frame held at the entry is applied), fwdunavonbreak (a written forward whose link broke answered UNAVAILABLE
# instead of closing the connection), fwdnouser (the owner runs a forward without the user's rights), fwdnowait (a
# forward's wait does not count as a waiting worker: it starves the entry's own volumes), fwdauthlag (AUTH while the
# owner's users lag the entry's); test_cluster fwdlease must catch fwdnolease (the owner skips the read lease for a
# forward) and test_cluster fwddepose fwdtwice (a deposed leader forwards a forward again); test_cluster fwdload must
# catch fwdchunknowait (an owner's worker waiting for a slow reader's credit does not count as waiting: forwarded
# inserts queue behind it) and fwdinline (forwards served on the links' dispatcher: two owners forwarding to each other
# wait for each other's credit); forward must also catch fwdarrival (the age counted from the forward's start, not the
# parse), fwdownerage (the owner skips its age check), fwdownerclose (the owner answers an unreadable forward with
# nothing instead of a close), fwdtailclose (the entry ignores the owner's close), fwdepoch (the owner skips its epoch
# check) and fwdgone (a forward whose client left keeps waiting). fwdretry (a lost forward sent again) is test_fwd's
# (make test runs its mutants).
# A mutant counts as caught only by its intended FAIL line (the first FAIL of the run must match the mutant's pattern):
# a flake, a timeout, a crash or a FAIL in another part is reported as WRONG and fails the run like a survivor.
# Usage: tests/fwd_mutants.sh SCRATCH [MUTANT...]
# (from the top of the tree, after make build/tsdbd build/test_cluster build/tsdb).
set -u
dir=${1:-/tmp}; shift || true
muts=${*:-fwdoldframe fwdunavonbreak fwdnouser fwdnowait fwdauthlag fwdnolease fwdtwice fwdchunknowait fwdinline
    fwdarrival fwdownerage fwdownerclose fwdtailclose fwdepoch fwdgone}
export TSDBD=${TSDBD:-build/tsdbd} TSDB_CLI=${TSDB_CLI:-build/tsdb}
tc=${TEST_CLUSTER:-build/test_cluster}
bad=
for m in $muts; do
    case $m in
    fwdoldframe) sc=forward pat='a write held 1.5 s at the entry' ;;
    fwdunavonbreak) sc=forward pat='owner was killed before it answered' ;;
    fwdnouser) sc=forward pat="alice's write outside her grant" ;;
    fwdnowait) sc=forward pat='while two forwards waited on a stopped owner' ;;
    fwdauthlag) sc=forward pat='users lagged: error -17' ;;
    fwdnolease) sc=fwdlease pat='past its lease' ;;
    fwdtwice) sc=fwddepose pat='to the deposed leader .*not MOVED' ;;
    fwdchunknowait) sc=fwdload pat='beside a slow reader' ;;
    fwdinline) sc=fwdload pat='its read after [0-9]* rounds' ;;
    fwdarrival) sc=forward pat='at the entry before its forward began' ;;
    fwdownerage) sc=forward pat='at the owner before it ran the forward' ;;
    fwdownerclose | fwdtailclose) sc=forward pat='a forward the owner could not read' ;;
    fwdepoch) sc=forward pat='another epoch than its owner' ;;
    fwdgone) sc=forward pat='whose client left is still in flight' ;;
    *) echo "fwd_mutants: unknown mutant $m" >&2; exit 2 ;;
    esac
    log=$dir/mut-$m-$sc.log
    TSDBD_FWD_MUTANT=$m timeout 600 "$tc" "$dir" $sc > "$log" 2>&1
    rc=$?
    first=$(grep -m1 'FAIL' "$log")
    if [ $rc = 0 ]; then
        echo "mutant $m: SURVIVED"
        bad="$bad $m"
    elif echo "$first" | grep -q -- "$pat"; then
        echo "mutant $m: caught by $sc ($first)"
    else
        echo "mutant $m: WRONG: $sc exited $rc, first FAIL '${first:-none}', want '$pat' ($log)"
        bad="$bad $m"
    fi
done
[ -z "$bad" ] || { echo "not caught as intended:$bad"; exit 1; }
echo "every mutant was caught by its intended FAIL"
