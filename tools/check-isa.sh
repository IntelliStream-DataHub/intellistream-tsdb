#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# Fails if a portable build has per-level code that runs before the CPU check
# (constructors), per-level code whose rounding depends on the CPU (FMA, libm's
# dispatched functions), or uses instructions beyond x86-64-v2 outside the code
# that is only reached after a CPU check: the per-level compiles (*.v3.o,
# *.v4.o, functions *_v3/_v4), simd.c's kernels (functions *_avx/*_avx2/*_avx512,
# and GCC's .part/.constprop clones of them), the GF(256) kernels of the erasure
# codec (src/ec: iso_gf256_{mul_add,dot}_avx2/_avx512/_gfni256/_gfni512, which carry
# target attributes and which src/ec/compat.c's iso_cpu_has() lets run only at
# the CPU level src/cpu.c picked), and zstd's BMI2 variants. An
# emulator finds most such slips (qemu -cpu SandyBridge raises SIGILL), but not
# LZCNT: older CPUs run it as BSR and silently get another result. So this
# reads the machine code.
#
#   tools/check-isa.sh build-dir binary...
set -eu
[ $# -ge 2 ] || { echo "usage: $0 build-dir binary..." >&2; exit 2; }
dir=$1; shift

# mnemonics of AVX/AVX2/AVX-512 (all VEX/EVEX ones start with v), BMI1/2,
# LZCNT, MOVBE and the AVX-512 mask registers; TZCNT is left out: it runs as
# BSF where it is missing and GCC uses it only where both agree.
above_v2() {
    objdump -d --no-show-raw-insn "$1" | awk '
        /^[0-9a-f]+ <.*>:$/ { fn = substr($2, 2, length($2) - 3); next }
        NF >= 2 {
            m = $2
            if (m ~ /^v/ || m ~ /^(andn|bextr|blsi|blsmsk|blsr|bzhi|mulx|pdep|pext|rorx|sarx|shlx|shrx|lzcnt|movbe)$/ ||
                m ~ /^k(mov|and|andn|or|xnor|xor|not|ortest|test|shiftl|shiftr|unpck|add)[bwdq]*$/)
                n[fn]++
        }
        END { for (f in n) print f, n[f] }'
}

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# 0. a level object must not run anything by itself: a constructor (or any
#    .init_array/.ctors entry) of a v3/v4 copy runs at startup on every CPU,
#    before the dispatch, and dies with SIGILL on one without that level
for o in "$dir"/*.v3.o "$dir"/*.v4.o "$dir"/ec_*.o; do
    [ -e "$o" ] || continue
    if objdump -h "$o" | awk '{print $2}' | grep -Eq '^\.(init_array|preinit_array|ctors|init)(\.|$)'; then
        echo "$o: has startup code (.init_array/.ctors): it would run unchecked on every CPU" >&2
        echo x >>"$tmp/bad"
    fi
done
# ... nor call libm's transcendental functions: glibc picks their code by CPU
#     (log2 has an FMA variant), so their last bit, and a codec choice that
#     hangs on it, could differ between two machines running this binary
for o in "$dir"/*.base.o "$dir"/*.v3.o "$dir"/*.v4.o; do
    [ -e "$o" ] || continue
    nm -u "$o" | awk '{print $2}' | grep -E '^(__)?(a?(sin|cos|tan)h?|atan2|exp(2|10|m1)?|log(2|10|1p|b)?|pow|cbrt|hypot|erfc?|[lt]gamma)f?(_finite)?(@.*)?$' |
        while read -r f; do
            echo "$o: calls $f: libm dispatches it by CPU, so its rounding is not the same on every machine" >&2
            echo x >>"$tmp/bad"
        done
done
# ... nor use FMA: the codecs call no fma(), so one there is a contraction of
#     a*b+c (-ffp-contract=off missing), which rounds differently from the v2 copy
for o in "$dir"/*.v3.o "$dir"/*.v4.o; do
    [ -e "$o" ] || continue
    n=$(objdump -d --no-show-raw-insn "$o" | awk '$2 ~ /^vfn?m(add|sub)/' | wc -l)
    if [ "$n" -gt 0 ]; then
        echo "$o: $n FMA instructions: rounding differs from the baseline copy" >&2
        echo x >>"$tmp/bad"
    fi
done
# 1. objects compiled for the baseline: nothing above v2 but simd.c's kernels
for o in "$dir"/*.o; do
    case "$o" in *.v3.o | *.v4.o) continue ;; esac
    above_v2 "$o" | while read -r fn cnt; do
        # simd.c's kernels carry their own targets (*_avx, *_avx2, *_avx512); GCC may split one (agg_avx2.part.0):
        # the piece keeps its target and only the kernel calls it
        case "$(basename "$o"):${fn%%.*}" in simd.o:*_avx | simd.o:*_avx2 | simd.o:*_avx512) continue ;; esac
        # src/ec's wide kernels: two objects (ec_gf256_avx2.o; ec_gf256_avx512.o, which also holds the 256-bit GFNI
        # one), each entered only through iso_gf256_mul_add's and iso_gf256_dot's pointers, which iso_gf256_init sets
        # from iso_cpu_has (capped at the level src/cpu.c picked)
        case "$(basename "$o"):${fn%%.*}" in
        ec_gf256_avx2.o:iso_gf256_mul_add_avx2 | ec_gf256_avx2.o:iso_gf256_mul_add_gfni256 | \
            ec_gf256_avx2.o:iso_gf256_dot_avx2) continue ;;
        ec_gf256_avx512.o:iso_gf256_mul_add_avx512 | ec_gf256_avx512.o:iso_gf256_mul_add_gfni256 | \
            ec_gf256_avx512.o:iso_gf256_mul_add_gfni512 | ec_gf256_avx512.o:iso_gf256_dot_avx512 | \
            ec_gf256_avx512.o:iso_gf256_dot_gfni256 | ec_gf256_avx512.o:iso_gf256_dot_gfni512) continue ;;
        esac
        echo "$o: $fn: $cnt instructions beyond x86-64-v2" >&2
        echo x >>"$tmp/bad"
    done
done
# 2. the binaries (zstd and liburing come in here): every such function must
#    be one the dispatch reaches only on a capable CPU (step 0 rules out the
#    level objects' functions being reached from startup)
for o in "$dir"/*.v3.o "$dir"/*.v4.o; do
    [ -e "$o" ] && nm --defined-only "$o" | awk '$2 ~ /^[tT]$/ {print $3}'
done | sort -u >"$tmp/level_fns"
# simd.c's kernels by name, so a zstd or libc function that happens to end in
# _avx2 is not let through
nm --defined-only "$dir/simd.o" | awk '$2 ~ /^[tT]$/ {sub(/\..*/, "", $3); print $3}' |
    grep -E '_avx$|_avx2$|_avx512$' | sort -u >"$tmp/simd_fns"
# the erasure codec's wide kernels, by name and by the object that defines them
for o in ec_gf256_avx2.o ec_gf256_avx512.o; do
    [ -e "$dir/$o" ] && nm --defined-only "$dir/$o" | awk '$2 ~ /^[tT]$/ {sub(/\..*/, "", $3); print $3}'
done | grep -E '^iso_gf256_(mul_add|dot)_(avx2|avx512|gfni256|gfni512)$' | sort -u >"$tmp/ec_fns"
for b in "$@"; do
    above_v2 "$b" | while read -r fn cnt; do
        base=${fn%%.*} # foo.part.0, foo.constprop.0
        case "$base" in
        *_v3 | *_v4) continue ;;
        HUF_*_bmi2 | FSE_*_bmi2 | ZSTD_*_bmi2 | HUF_*_fast | HUF_*_fast_c_loop | HUF_*_fast_asm_loop) continue ;;
        esac
        grep -qx "$base" "$tmp/simd_fns" && continue
        grep -qx "$base" "$tmp/ec_fns" && continue
        grep -qx "$fn" "$tmp/level_fns" && continue # a static helper of a level compile (its base copy is step 1's)
        echo "$b: $fn: $cnt instructions beyond x86-64-v2" >&2
        echo x >>"$tmp/bad"
    done
done
# 3. the erasure codec's dot kernels hand the bytes after their last vector to a narrower kernel, never to a wider one:
#    a tail call from iso_gf256_dot_avx2 into iso_gf256_dot_gfni256 would run well on the AVX-512 machines the tests
#    use and raise SIGILL on an AVX2-only CPU (the i9-10850K), and nothing in step 1 or 2 sees it, since each wide
#    kernel is allowed by name. Calls and jumps are read from the objects (relocations and resolved targets alike).
for o in ec_gf256.o ec_gf256_avx2.o ec_gf256_avx512.o; do
    [ -e "$dir/$o" ] || continue
    objdump -dr --no-show-raw-insn "$dir/$o" | awk -v obj="$o" '
        BEGIN {
            no["scalar"] = " ssse3 avx2 avx512 gfni256 gfni512 "
            no["ssse3"] = " avx2 avx512 gfni256 gfni512 "
            no["avx2"] = " avx512 gfni256 gfni512 "
            no["gfni256"] = " avx512 gfni512 "
            no["avx512"] = " gfni256 gfni512 "
            no["gfni512"] = " "
        }
        /^[0-9a-f]+ <.*>:$/ {
            fn = $2; gsub(/[<>:]/, "", fn); sub(/\..*/, "", fn)
            me = (fn ~ /^iso_gf256_dot_/) ? substr(fn, 15) : ""
            next
        }
        me != "" {
            while (match($0, /iso_gf256_dot_[a-z0-9]+/)) {
                callee = substr($0, RSTART + 14, RLENGTH - 14)
                $0 = substr($0, RSTART + RLENGTH)
                if (index(no[me], " " callee " ") > 0) {
                    printf "%s: iso_gf256_dot_%s refers to iso_gf256_dot_%s, a kernel for CPUs it may not run on\n", obj, me, callee
                    bad = 1
                }
            }
        }
        END { exit bad }' >&2 || echo x >>"$tmp/bad"
done
if [ -s "$tmp/bad" ]; then
    echo "check-isa: the build would not run, or not write the same bytes, on every x86-64-v2 CPU" >&2
    exit 1
fi
echo "check-isa: $# binaries use nothing beyond x86-64-v2 outside the CPU dispatch"
