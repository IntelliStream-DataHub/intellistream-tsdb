#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# libtsdb.so exports the public API and nothing else (tools/libtsdb.map): every defined dynamic symbol must be declared
# in include/tsdb.h or include/tsdb_calendar.h. A name that is only in the .so is ABI by accident, and for the erasure
# codec's glue (tsdb_ec_*, src/ec/compat.c) a way to lift the CPU level cap from outside.
#
#   B=build tools/check-exports.sh [LIBRARY]
set -eu
cd "$(dirname "$0")/.."
lib=${1:-${B:-build}/libtsdb.so}
if [ ! -f "$lib" ]; then echo "check-exports: $lib not built, skipped"; exit 0; fi
bad=0
for s in $(nm -D --defined-only "$lib" | awk '$2 ~ /^[TDBR]$/ {print $3}' | sed 's/@.*//'); do
    case $s in tsdb_test_*) continue ;; esac # hooks of the tests, which link against the .so
    if ! grep -qwF "$s" include/tsdb.h include/tsdb_calendar.h; then
        echo "check-exports: $lib exports $s, which no public header declares" >&2
        bad=1
    fi
done
[ $bad = 0 ] && echo "check-exports: $lib exports only the public API"
exit $bad
