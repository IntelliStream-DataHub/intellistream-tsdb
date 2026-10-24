#!/bin/sh
# Regenerates calendar_bounds.txt, the bounds the C client computes, which
# tests/calendar_fixtures.rs compares the Rust port with. The zones are copied
# from the system's zoneinfo into zoneinfo/ so that both sides read the same
# files whatever tzdata the test machine has.
#
#   make -C ../../../..      # build/libtsdbclient.a
#   ./gen_calendar_fixtures.sh
set -eu
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../../../.." && pwd)
src=${TZDIR:-/usr/share/zoneinfo}
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT

rm -rf "$here/zoneinfo"
zones=$(grep -o '"[A-Z][A-Za-z_]*/[A-Za-z_+0-9]*"' "$here/calendar_bounds.c" | tr -d '"' | sort -u)
for z in $zones; do
    mkdir -p "$here/zoneinfo/$(dirname "$z")"
    cp "$src/$z" "$here/zoneinfo/$z"
done

cc -O2 -std=gnu11 -Wall -I"$root/include" -o "$out/gen" "$here/calendar_bounds.c" "$root/build/libtsdbclient.a" \
    -lzstd -lpthread
{
    echo "# tsdb_calendar_bounds over zoneinfo/ (tzdata $(sed -n 's/^# version //p' "$src/tzdata.zi" 2>/dev/null || echo unknown))"
    TZDIR="$here/zoneinfo" "$out/gen"
} >"$here/calendar_bounds.txt"
wc -l "$here/calendar_bounds.txt"
