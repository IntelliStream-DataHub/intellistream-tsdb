#!/bin/bash
# `tsdb-bench --qps` opens an existing database and queries it from threads. It once used the table of series ids
# that only a load fills (a NULL pointer: every query thread crashed), so the server-less query benchmark never ran.
# It looks the ids up now, with the statistics on and with --no-stats.
#
#   B=build tests/test_bench_qps.sh      (make test runs it)
set -u
cd "$(dirname "$0")/.."
B=${B:-build}
dir=$(mktemp -d "${TMPDIR:-/tmp}/tsdb-bench-qps.XXXXXX")
trap 'rm -rf "$dir"' EXIT
fail() { echo "test_bench_qps: FAIL: $*" >&2; exit 1; }

"$B/tsdb-bench" --dir "$dir/db" --series 4 --points 20000 --keep >/dev/null 2>&1 || fail "load exited with $?"
for flag in "" --no-stats; do
    # shellcheck disable=SC2086
    out=$("$B/tsdb-bench" --dir "$dir/db" --qps 2 --series 4 --points 20000 --seconds 0.2 $flag 2>&1) ||
        fail "qps $flag exited with $? ($(printf '%s' "$out" | tail -n 3))"
    printf '%s\n' "$out" | grep -q 'q/s' || fail "qps $flag printed no results"
done
echo "test_bench_qps: ok"
