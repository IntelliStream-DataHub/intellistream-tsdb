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
# check) and fwdgone (a forward whose client left keeps waiting); test_cluster fwdseries must catch fwdcreateread (a
# SERIES or STATE that creates judged a read: a replica serving reads answers MOVED instead of forwarding it);
# forward must catch fwdopkind (the owner serves a write that came on the read link) and fwdopkindread (it serves a read
# that came on the write link: the check one way only).
# fwdretry (a lost forward sent again) is test_fwd's (make test runs its mutants).
# A mutant counts as caught only by its intended FAIL line (the first FAIL of the run must match the mutant's pattern):
# a flake, a timeout, a crash or a FAIL in another part is reported as WRONG and fails the run like a survivor.
# Usage: tests/fwd_mutants.sh SCRATCH [MUTANT...]
# (from the top of the tree, after make build/tsdbd build/test_cluster build/tsdb).
set -u
export TSDBD=${TSDBD:-build/tsdbd} TSDB_CLI=${TSDB_CLI:-build/tsdb}
export TEST_CLUSTER=${TEST_CLUSTER:-$(dirname "$TSDBD")/test_cluster} # the same build as tsdbd (make tsan: build-tsan)
spec() {
    case $1 in
    fwdoldframe) sc=forward pat='a write held 1.5 s at the entry' ;;
    # the read-link case (a FWDQ write the owner refuses by closing the link) is the same written-and-lost path and
    # runs first in forward: either line is this mutant's
    fwdunavonbreak) sc=forward pat='owner was killed before it answered\|a write sent on the read link: error -11' ;;
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
    fwdcreateread) sc=fwdseries pat='a series created by path through the replica' ;;
    fwdopkind) sc=forward pat='a write sent on the read link' ;;
    fwdopkindread) sc=forward pat='a read sent on the write link' ;;
    *) echo "fwd_mutants: unknown mutant $1" >&2; exit 2 ;;
    esac
}
# one mutant, then its verdict: tools/mutants.sh runs them in parallel, each in a scratch directory of its own, and
# asks this script (--mutant-verify) whether the first FAIL matches the mutant's pattern
case ${1:-} in
--mutant-run)
    spec "$2"
    TSDBD_FWD_MUTANT=$2 exec "$TEST_CLUSTER" "$3" "$sc" ;;
--mutant-verify)
    spec "$2"
    if [ "$3" -eq 0 ]; then echo "$sc passed"; exit 1; fi
    first=$(grep -m1 'FAIL' "$4")
    if printf '%s\n' "$first" | grep -q -- "$pat"; then
        echo "caught by $sc ($first)"
        exit 0
    fi
    echo "WRONG: $sc exited $3, first FAIL '${first:-none}', want '$pat'"
    exit 1 ;;
esac
dir=${1:-${TMPDIR:-/tmp}}; shift || true
muts=${*:-fwdoldframe fwdunavonbreak fwdnouser fwdnowait fwdauthlag fwdnolease fwdtwice fwdchunknowait fwdinline fwdarrival fwdownerage fwdownerclose fwdtailclose fwdepoch fwdgone fwdcreateread fwdopkind fwdopkindread}
for m in $muts; do spec "$m"; done
# MUTANT_JOBS, MUTANT_URING_JOBS (tsdbd on io_uring), MUTANT_TIMEOUT
exec "$(dirname "$0")/../tools/mutants.sh" --name fwd --tmp "$dir" --uring --timeout "${MUTANT_TIMEOUT:-600}" --fail \
    --verify "$0" --list "$muts" -- "$0" --mutant-run @M@ @D@
