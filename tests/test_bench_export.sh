#!/bin/bash
# `tsdb-bench --export` writes the RowBinary file (16-byte rows: u32 series number, i64 ts, f32 value) without a
# database, so it has no series ids to put in the rows: the file keeps the NUMBERS, 0 .. series - 1. It once
# dereferenced the id table that only a database run fills, and crashed.
#
#   B=build tests/test_bench_export.sh      (make test runs it)
set -u
cd "$(dirname "$0")/.."
B=${B:-build}
dir=$(mktemp -d "${TMPDIR:-/tmp}/tsdb-bench-export.XXXXXX")
trap 'rm -rf "$dir"' EXIT
fail() { echo "test_bench_export: FAIL: $*" >&2; exit 1; }

"$B/tsdb-bench" --export "$dir/x.rb" --series 7 --points 300 >/dev/null 2>&1 || fail "export exited with $?"
size=$(stat -c %s "$dir/x.rb" 2>/dev/null || stat -f %z "$dir/x.rb") # GNU, then BSD stat
[ "$size" -eq $((7 * 300 * 16)) ] || fail "file is $size bytes, want $((7 * 300 * 16))"
# every time step lists the series 0, then 1 ... 6: the numbers, not ids
python3 - "$dir/x.rb" <<'PY' || fail "series numbers"
import struct, sys
rows = list(struct.iter_unpack("<Iqf", open(sys.argv[1], "rb").read()))
assert [r[0] for r in rows[:14]] == list(range(7)) * 2, [r[0] for r in rows[:14]]
assert max(r[0] for r in rows) == 6
PY
echo "test_bench_export: ok"
