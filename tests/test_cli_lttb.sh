#!/bin/bash
# The CLI's `query ... lttb N`: N points printed like raw, the ends kept, and bad
# thresholds ("-1", "2", "x", none) refused instead of read as some number.
#
#   B=build tests/test_cli_lttb.sh      (make test runs it)
set -u
cd "$(dirname "$0")/.."
B=${B:-build}
dir=$(mktemp -d "${TMPDIR:-/tmp}/tsdb-cli-lttb.XXXXXX")
trap 'rm -rf "$dir"' EXIT
fail() { echo "test_cli_lttb: FAIL: $*" >&2; exit 1; }

"$B/tsdb" "$dir/db" create s --type float64 >/dev/null || fail "create"
awk 'BEGIN { for (i = 0; i < 1000; i++) printf "%d,%.6f\n", 1700000000000 + i * 1000, sin(i / 30) * 10 }' |
    LC_ALL=C "$B/tsdb" "$dir/db" insert s >/dev/null 2>&1 || fail "insert"
raw=$("$B/tsdb" "$dir/db" query s min max raw 2>/dev/null) || fail "raw"
out=$("$B/tsdb" "$dir/db" query s min max lttb 50 2>/dev/null) || fail "lttb 50"
[ "$(printf '%s\n' "$out" | wc -l)" -eq 50 ] || fail "lttb 50 printed $(printf '%s\n' "$out" | wc -l) lines"
[ "$(printf '%s\n' "$out" | head -n 1)" = "$(printf '%s\n' "$raw" | head -n 1)" ] || fail "first point"
[ "$(printf '%s\n' "$out" | tail -n 1)" = "$(printf '%s\n' "$raw" | tail -n 1)" ] || fail "last point"
printf '%s\n' "$out" | while IFS= read -r line; do
    printf '%s\n' "$raw" | grep -qxF -- "$line" || { echo "not a stored point: $line" >&2; exit 1; }
done || fail "lttb printed a point that is not stored"
for bad in -1 2 x 5x ""; do
    if "$B/tsdb" "$dir/db" query s min max lttb $bad >/dev/null 2>&1; then fail "lttb '$bad' accepted"; fi
done
echo "test_cli_lttb: ok"
