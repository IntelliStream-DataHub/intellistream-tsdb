#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# Fails if a binary needs a glibc symbol version newer than a ceiling, or a
# shared library other than glibc's own: what `make dist` ships must load on
# the oldest system it claims (AlmaLinux 10: glibc 2.39; EL9: 2.34).
#
#   tools/check-glibc.sh [-c 2.39] binary...      (GLIBC_MAX=2.34 works too)
set -eu
max=${GLIBC_MAX:-2.39}
if [ "${1:-}" = "-c" ]; then max=$2; shift 2; fi
[ $# -gt 0 ] || { echo "usage: $0 [-c max_glibc] binary..." >&2; exit 2; }
# libraries glibc itself provides; everything else must be linked statically
allowed='^(libc|libm|libpthread|libdl|librt|ld-linux-x86-64)\.so'
status=0
for f in "$@"; do
    if ! objdump -p "$f" >/dev/null 2>&1; then
        echo "$f: not an object file" >&2
        status=1
        continue
    fi
    ok=1
    newest=$(objdump -T "$f" | grep -o 'GLIBC_[0-9][0-9.]*' | sed 's/GLIBC_//' | sort -uV | tail -n 1)
    if [ -n "$newest" ] && [ "$(printf '%s\n%s\n' "$newest" "$max" | sort -V | tail -n 1)" != "$max" ]; then
        echo "$f: needs glibc $newest > $max:" >&2
        objdump -T "$f" | awk -v max="$max" '{for (i = 1; i <= NF; i++) if ($i ~ /GLIBC_[0-9]/) print $i, $NF}' |
            tr -d '()' | sed 's/GLIBC_//' | while read -r v sym; do
                [ "$(printf '%s\n%s\n' "$v" "$max" | sort -V | tail -n 1)" = "$max" ] || echo "    $sym ($v)" >&2
            done
        ok=0
    fi
    for lib in $(objdump -p "$f" | awk '$1 == "NEEDED" {print $2}'); do
        if ! echo "$lib" | grep -Eq "$allowed"; then
            echo "$f: needs $lib (link it statically)" >&2
            ok=0
        fi
    done
    if [ $ok -eq 1 ]; then echo "$f: glibc ${newest:-none} <= $max"; else status=1; fi
done
exit $status
