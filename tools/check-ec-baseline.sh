#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# A generic build (PORTABLE=1, no -march) must not run SSSE3 outside the SSSE3 kernel: src/ec/gf256.c is compiled with
# -mssse3 (its kernel has no target attribute and the copy is never edited), and the compiler vectorised iso_gf256_init
# with pshufb, which runs in every process that loads libtsdb before the CPU is asked. The Makefile's EC_CFLAGS keep
# that out (-fno-tree-vectorize); this compiles the three files with those flags and no -march and fails when an
# SSSE3 instruction (pshufb, palignr, pmaddubsw, pabs*, phadd*, psign*) is in any function but the SSSE3 kernels
# (iso_gf256_mul_add_ssse3, and mul_set's and dot's if upstream has them). x86-64 only.
#
#   EC_CFLAGS='-mssse3 -fno-tree-vectorize' CC=cc tools/check-ec-baseline.sh
set -eu
cd "$(dirname "$0")/.."
cc=${CC:-cc}
flags=${EC_CFLAGS:--mssse3 -fno-tree-vectorize}
t=$(mktemp -d "${TMPDIR:-/tmp}/ecbase.XXXXXX")
trap 'rm -rf "$t"' EXIT
bad=0
for f in gf256 rs compat; do
    $cc -O3 -g0 -std=gnu11 -ffp-contract=off -Iinclude -fPIC -DTSDB_EC $flags -Isrc/ec/compat -DISO_GF256_NO_BOOT \
        -c -o "$t/$f.o" "src/ec/$f.c"
    objdump -d "$t/$f.o" | awk -v f="$f" '
        /^[0-9a-f]+ <.*>:/ { fn = $2 }
        /[ \t](pshufb|palignr|pmaddubsw|pabs[bwd]|phadd[wd]|phaddsw|psign[bwd]|pmulhrsw)[ \t]/ && fn !~ /^<iso_gf256_(mul_(add|set)|dot)_ssse3>:/ {
            print "check-ec-baseline: src/ec/" f ".c: SSSE3 in " fn ": " $0; bad = 1 }
        END { exit bad }' || bad=1
done
[ $bad = 0 ] || exit 1
echo "check-ec-baseline: no SSSE3 outside the SSSE3 kernel in a generic build"
