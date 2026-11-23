#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# A planted-bug check: run a test that has a bug planted in it and require the SPECIFIC failure the bug causes.
#   tests/expect_fail.sh 'TEXT' CMD [ARG...]
# passes only when CMD exits with status exactly 1 (the tests' status for a failed check) and its output (stdout and
# stderr) contains TEXT. A crash (139, 134), a timeout (124), a missing binary (127), an unknown mutant name or a
# run from the wrong directory (2) all fail this check: `! cmd` would call each of them "the planted bug was caught".
if [ $# -lt 2 ]; then
    echo "usage: expect_fail.sh 'TEXT' CMD [ARG...]" >&2
    exit 2
fi
text=$1
shift
out=$(mktemp "${TMPDIR:-/tmp}/expect_fail.XXXXXX") || exit 2
"$@" >"$out" 2>&1
rc=$?
if [ "$rc" -ne 1 ]; then
    echo "expect_fail: '$*' exited with status $rc, not 1: that is not the planted bug being caught" >&2
    tail -n 8 "$out" >&2
    rm -f "$out"
    exit 1
fi
if ! grep -qF -- "$text" "$out"; then
    echo "expect_fail: '$*' failed, but not with '$text':" >&2
    tail -n 12 "$out" >&2
    rm -f "$out"
    exit 1
fi
rm -f "$out"
echo "ok: the planted bug was caught ('$text')"
