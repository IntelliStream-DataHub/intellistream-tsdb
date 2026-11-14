#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# tools/check-exports.sh must fail on a library that exports a name no public header declares (what libtsdb.so did with
# tsdb_ec_*, the erasure codec's glue, before tools/libtsdb.map listed them as local), and pass on one that exports only
# declared names and the tsdb_test_* hooks.
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
t=$(mktemp -d "${TMPDIR:-/tmp}/exports.XXXXXX")
trap 'rm -rf "$t"' EXIT
printf 'int tsdb_open(void) { return 0; }\nint tsdb_test_hook(void) { return 0; }\n' > "$t/ok.c"
printf 'int tsdb_open(void) { return 0; }\nint tsdb_ec_select(int l) { return l; }\n' > "$t/bad.c"
cc -shared -fPIC -o "$t/ok.so" "$t/ok.c"
cc -shared -fPIC -o "$t/bad.so" "$t/bad.c"
grep -qw tsdb_open "$here/include/tsdb.h" || { echo "FAIL: tsdb_open is no longer declared; fix the test"; exit 1; }
sh "$here/tools/check-exports.sh" "$t/ok.so" > /dev/null || { echo "FAIL: declared names and test hooks are refused"; exit 1; }
if sh "$here/tools/check-exports.sh" "$t/bad.so" > /dev/null 2>&1; then echo "FAIL: an undeclared export passes"; exit 1; fi
echo "test_check_exports: ok"
