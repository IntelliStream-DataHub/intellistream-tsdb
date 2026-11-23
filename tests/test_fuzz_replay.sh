#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# The corpus replays in `make test` must be able to fail: a replay that ran no input (an emptied or moved corpus, a
# typo in the path) proves nothing and must be a failure, and the committed corpus must replay a plausible number.
#   tests/test_fuzz_replay.sh BUILD_DIR SCRATCH
b=${1:-build}
scratch=${2:-${TMPDIR:-/tmp}}
d=$(mktemp -d "$scratch/fuzz_replay.XXXXXX") || exit 2
trap 'rm -rf "$d"' EXIT
h=$b/fuzz-wal
[ -x "$h" ] || { echo "test_fuzz_replay: $h is not built" >&2; exit 2; }
mkdir "$d/empty"
sh tests/expect_fail.sh 'no input was replayed' "$h" -replay "$d/empty" || exit 1
sh tests/expect_fail.sh 'cannot read' "$h" -replay "$d/missing" || exit 1
sh tests/expect_fail.sh 'no input was replayed' "$h" -replay || exit 1
# the committed corpora are not empty: every harness replays at least one input
for c in tests/fuzz/corpus/*; do
    n=$(find -L "$c" -type f | wc -l)
    [ "$n" -ge 1 ] || { echo "test_fuzz_replay: $c holds no input" >&2; exit 1; }
done
echo "test_fuzz_replay: ok"
