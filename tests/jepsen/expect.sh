#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# One planted-nemesis case of `make jepsen-selftest`, judged on the exact outcome, never on "non-zero":
#
#   expect.sh gate PATTERN WHAT JEPSEN-ARGS...   a nemesis that is lazy (or a shim that does not do what is asked): every one
#       of the three attempts of tests/jepsen/rerun.sh is INCONCLUSIVE, so the run ends in the gate failure (exit 3, "INCONCLUSIVE
#       3 times in a row") with a message that matches PATTERN (grep: the check that must have failed, named in the message)
#   expect.sh valid WHAT JEPSEN-ARGS...         an honest run, however odd (a nemesis that pulls its last plug late, a shim slow to
#       write its mark): exit 0 and "valid", on any attempt (an INCONCLUSIVE one is rerun, as in make jepsen)
#   expect.sh violation PATTERN WHAT JEPSEN-ARGS...   a wrong answer or a broken server: exit 1 at once (no rerun), the message
#
# Environment assignments before the command are the caller's (env X=Y expect.sh ...). On a miss it prints what it saw and exits 1.
set -u
mode=${1:?}; shift
case $mode in
gate | violation) pat=${1:?}; shift ;;
valid) pat= ;;
*) echo "expect.sh: gate | valid | violation" >&2; exit 2 ;;
esac
what=${1:?}; shift
here=$(cd "$(dirname "$0")" && pwd)
out=$("$here/rerun.sh" "$@" 2>&1); rc=$?
ok=false
case $mode in
gate)
    [ $rc -eq 3 ] && echo "$out" | grep -q 'jepsen-rerun: INCONCLUSIVE 3 times in a row' &&
        echo "$out" | grep -q "$pat" && [ "$(echo "$out" | grep -c 'jepsen-rerun: attempt [0-9] of 3 (seed [0-9]*) was INCONCLUSIVE')" -eq 2 ] && ok=true
    want="exit 3 after three INCONCLUSIVE attempts, and '$pat'" ;;
valid)
    [ $rc -eq 0 ] && echo "$out" | grep -q '^valid$' && ok=true
    want="exit 0 and 'valid'" ;;
violation)
    [ $rc -eq 1 ] && ! echo "$out" | grep -q 'jepsen-rerun: attempt' && echo "$out" | grep -q "$pat" && ok=true
    want="exit 1 at the first attempt, and '$pat'" ;;
esac
if ! $ok; then
    echo "jepsen-selftest: $what went unnoticed or ended another way (rc $rc, wanted $want)"
    echo "$out" | tail -n 25
    exit 1
fi
exit 0
