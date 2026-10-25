#!/bin/sh
# fold-bench at every CPU level of a `make dist` build, interleaved: round r runs every configuration once
# before round r + 1 starts, so a busy machine slows them alike. Prints, per case, the median rows/s of
# each configuration and the spread (lowest - highest) over the rounds.
#
#   tools/fold-bench.sh [BUILD_DIR=build-dist] [ROUNDS=5] [fold-bench options...]
#
# Configurations: TSDB_CPU=v2 with TSDB_SIMD=scalar, sse (SSE4.2) and avx (AVX without AVX2, as on Sandy
# Bridge), v3 (AVX2), v4 (AVX-512).
set -eu
dir=${1:-build-dist}
rounds=${2:-5}
[ $# -ge 2 ] && shift 2 || shift $#
bench=$dir/fold-bench
[ -x "$bench" ] || { echo "fold-bench.sh: no $bench (make dist)" >&2; exit 1; }
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
configs="v2:scalar v2:sse v2:avx v3 v4"
r=1
while [ "$r" -le "$rounds" ]; do
    for c in $configs; do
        case $c in
        *:*) TSDB_CPU=${c%%:*} TSDB_SIMD=${c#*:} "$bench" "$@" >"$tmp/$c.$r" ;;
        *) TSDB_CPU=$c "$bench" "$@" >"$tmp/$c.$r" ;;
        esac
    done
    r=$((r + 1))
done
for c in $configs; do
    printf '%s: %s\n' "$c" "$(sed -n 's/^# kernels: //p' "$tmp/$c.1")"
done
# one line per case: case, then per config: median [min-max]
awk -v configs="$configs" -v rounds="$rounds" '
    FNR == 1 { split(FILENAME, p, "/"); split(p[length(p)], q, "."); c = q[1] }
    /^#/ { next }
    {
        name = $1; for (i = 2; i < NF - 2; i++) name = name " " $i
        if (!(name in seen)) { seen[name] = 1; order[++n] = name }
        v[name, c, ++k[name, c]] = $(NF - 2)
    }
    END {
        nc = split(configs, cs, " ")
        printf "%-22s", "M rows/s"
        for (j = 1; j <= nc; j++) printf " %22s", cs[j]
        printf "\n"
        for (i = 1; i <= n; i++) {
            printf "%-22s", order[i]
            for (j = 1; j <= nc; j++) {
                m = k[order[i], cs[j]]
                for (a = 1; a <= m; a++) x[a] = v[order[i], cs[j], a]
                for (a = 1; a <= m; a++) for (b = a + 1; b <= m; b++) if (x[b] < x[a]) { t = x[a]; x[a] = x[b]; x[b] = t }
                printf " %7.0f [%6.0f-%6.0f]", x[int((m + 1) / 2)], x[1], x[m]
            }
            printf "\n"
        }
    }' "$tmp"/*.*
