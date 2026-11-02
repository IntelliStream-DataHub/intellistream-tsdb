#!/bin/bash
# Where do the short-run kernels hand over to the wide ones on this CPU? (src/simd.c: AVX2_SHORT, THR_*)
#
#   bench/cpu-compare/thresholds.sh [--cpu v3|v4] [--work DIR] [--rounds 11] [--ms 50] [--vary]
#
# Works on a scratch copy of the tree (WORK/tree) with thresholds-env.patch applied, which lets the environment
# set every threshold (TSDB_T_SHORT, _SHORT64, _I64, _I64DEV, _U32, _LTTB; "max": never). It builds a CPU_LEVELS tool
# set there and runs fold-bench with the wide kernels always ("W": thresholds 0) and the small kernels or loops
# always ("S": SIZE_MAX), at every row count, the rounds interleaved. A cell is W/S, the ratio of the means of
# M rows/s: above 1 the wide kernel is faster there. It is marked W or S only beyond 2% or two sd, '=' otherwise.
# The crossover is where W/S passes 1; compare it with the constants in src/simd.c. The repository is not touched.
# --cpu picks the level run (v3: the AVX2 sets, v4: the AVX-512 ones); --vary cycles through 256 granules so the
# branch predictor cannot learn one. The machine must be quiet; the governor on performance.
set -u -o pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
WORK=/var/tmp/cpu-compare CPU=v3 ROUNDS=11 MS=50 VARY=""
while [ $# -gt 0 ]; do
    case $1 in
    --cpu) CPU=$2; shift 2 ;;
    --work) WORK=$2; shift 2 ;;
    --rounds) ROUNDS=$2; shift 2 ;;
    --ms) MS=$2; shift 2 ;;
    --vary) VARY=--vary; shift ;;
    *) sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
    esac
done
T=$WORK/tree; O=$WORK/thr-$CPU; mkdir -p "$WORK" "$O"; rm -f "$O"/R*
if [ ! -x "$T/build/fold-bench" ]; then
    rm -rf "$T"; mkdir -p "$T"
    (cd "$ROOT" && git ls-files -z | grep -zv '^\(docs\|bench\|connectors\|tests\)/' | xargs -0 cp --parents -t "$T") || exit 1
    (cd "$T" && git init -q 2>/dev/null; patch -p1 -s <"$HERE/thresholds-env.patch") || { echo "patch failed" >&2; exit 1; }
    (cd "$T" && make -j"$(nproc)" B=build ARCH="-march=x86-64-v2 -mtune=generic" CPU_LEVELS="v3 v4" NO_URING=1 build/fold-bench) \
        >"$WORK/thr-build.log" 2>&1 || { tail -n 20 "$WORK/thr-build.log"; exit 1; }
fi
if awk -v l="$(cut -d' ' -f1 /proc/loadavg)" 'BEGIN { exit !(l > 2) }'; then
    echo "thresholds.sh: load average $(cut -d' ' -f1 /proc/loadavg): the machine is not quiet" >&2; exit 3
fi
ROWS="4 6 8 12 16 20 24 32 40 48 64 80 96 112 128 160 192 256 384 512 1024"
export TSDB_CPU=$CPU
for ((r = 1; r <= ROUNDS; r++)); do
    for R in $ROWS; do
        TSDB_T_SHORT=0 TSDB_T_SHORT64=0 TSDB_T_I64=0 TSDB_T_I64DEV=0 TSDB_T_U32=0 TSDB_T_LTTB=0 \
            "$T/build/fold-bench" --rows "$R" --ms "$MS" $VARY >"$O/R$R.W.$r" 2>&1
        TSDB_T_SHORT=max TSDB_T_SHORT64=max TSDB_T_I64=max TSDB_T_I64DEV=max TSDB_T_U32=max TSDB_T_LTTB=max \
            "$T/build/fold-bench" --rows "$R" --ms "$MS" $VARY >"$O/R$R.S.$r" 2>&1
    done
    echo "# round $r of $ROUNDS done" >&2
done
echo "# $(lscpu | sed -n 's/^Model name: *//p' | head -n 1), TSDB_CPU=$CPU, $(sed -n 's/^# kernels: //p' "$O/R8.W.1")"
echo "# W/S by rows (W: the wide kernel is faster, S: the small one, =: within 2% / 2 sd); thresholds now: see src/simd.c"
for f in "$O"/R*.[WS].[0-9]*; do
    b=$(basename "$f"); IFS=. read -r rr cfg rd <<<"$b"
    awk -v R="${rr#R}" -v C="$cfg" '/^#/ { next } { n = $0; if (match(n, /[ ]+[0-9.]+ M rows\/s/)) { name = substr(n, 1, RSTART - 1); v = substr(n, RSTART, RLENGTH) + 0; print name "\t" R "\t" C "\t" v } }' "$f"
done | sort -t$'\t' -k1,1 -k2,2n | awk -F'\t' '
    { k = $1 SUBSEP $2 SUBSEP $3; n[k]++; s[k] += $4; q[k] += $4 * $4
      if (!($1 in seen)) { seen[$1] = 1; order[++no] = $1 }
      if (!(($1 SUBSEP $2) in rs)) { rs[$1 SUBSEP $2] = 1; rows[$1] = rows[$1] " " $2 } }
    END { for (i = 1; i <= no; i++) { c = order[i]; printf "%s\n  ", c
            m = split(rows[c], R, " ")
            for (j = 1; j <= m; j++) { kw = c SUBSEP R[j] SUBSEP "W"; ks = c SUBSEP R[j] SUBSEP "S"
                mw = s[kw] / n[kw]; ms = s[ks] / n[ks]
                sw = n[kw] > 1 ? sqrt(q[kw] / n[kw] - mw * mw + 0) : 0; ss = n[ks] > 1 ? sqrt(q[ks] / n[ks] - ms * ms + 0) : 0
                tol = 0.02 * ms; t2 = 2 * (sw > ss ? sw : ss); if (t2 > tol) tol = t2
                f = (mw > ms + tol) ? "W" : (ms > mw + tol) ? "S" : "="
                printf "%s:%.2f%s ", R[j], mw / ms, f }
            printf "\n" } }'
echo "# raw output: $O"
