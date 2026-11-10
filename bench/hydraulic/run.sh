#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# Compression of the UCI hydraulic sensors in tsdb's value codecs.
#
#   bench/hydraulic/run.sh DATA_DIR        (DATA_DIR: the output of convert.py)
#
# Builds build/codec-bench and build/type-bench, codes every series in
# granules of 8192 values the way parts store them, and prints one table:
# bits per value of tsdb (AUTO, the default, as background merges leave parts at
# rest: type-bench --merge) and of other codecs with ZSTD(1),
# each also as a size compared to tsdb (tsdb = 1x; 2.0x is twice as large).
# Every coded granule is decoded again and checked bit for bit.
set -eu
data=${1:?usage: run.sh DATA_DIR}
root=$(cd "$(dirname "$0")/../.." && pwd)
make -C "$root" -s build/codec-bench build/type-bench >&2
cb=$root/build/codec-bench
tb=$root/build/type-bench

sensors="PS1 PS2 PS3 PS4 PS5 PS6 EPS1 FS1 FS2 TS1 TS2 TS3 TS4 VS1 CE CP SE"

# one row: series, type, values, then "tsdb gorilla chimp bytesplit" in bits per value (- where not measured)
row() {
    awk -v s="$1" -v t="$2" -v n="$3" -v a="$4" -v g="$5" -v c="$6" -v b="$7" 'BEGIN {
        printf "| %s | %s | %d | **%.2f** |", s, t, n, a
        split(g " " c " " b, x, " ")
        for (i = 1; i <= 3; i++)
            if (x[i] == "-") printf " - |"; else printf " %.2f (%.1fx) |", x[i], x[i] / a
        printf "\n"
    }'
}

f32() { # FILE -> "values tsdb gorilla chimp bytesplit", all with ZSTD(1)
    n=$(($(wc -c < "$1") / 4))
    a=$("$tb" --merge --file "$1" float32 | awk 'NR == 1 { print $3 }')
    "$cb" --file "$1" | awk -v n="$n" -v a="$a" '/ zstd=1 / {
        for (i = 3; i <= NF; i++) { split($i, kv, "="); v[kv[1]] = kv[2] }
        print n, a, v["gorilla"], v["chimp"], v["bytesplit"] }'
}

f64() { # FILE -> "values tsdb gorilla - bytesplit"
    n=$(($(wc -c < "$1") / 8))
    "$tb" --merge --file "$1" float64 | awk -v n="$n" '
        NR == 1 { a = $3 }
        $1 == "gorilla64" { g = $5 }
        $1 == "bytesplit" { b = $5 }
        END { print n, a, g, "-", b }'
}

echo "| series | type | values | tsdb, bits/value | Gorilla + ZSTD | Chimp + ZSTD | byte split + ZSTD |"
echo "|---|---|---:|---:|---:|---:|---:|"
for s in $sensors; do
    # shellcheck disable=SC2046
    row "$s" float32 $(f32 "$data/$s.f32")
    # shellcheck disable=SC2046
    row "$s" float64 $(f64 "$data/$s.f64")
    if [ -f "$data/${s}_psi.f32" ]; then
        # shellcheck disable=SC2046
        row "$s psi" float32 $(f32 "$data/${s}_psi.f32")
        # shellcheck disable=SC2046
        row "$s psi" float64 $(f64 "$data/${s}_psi.f64")
    fi
done
