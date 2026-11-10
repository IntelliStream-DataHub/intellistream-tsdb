#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# Every source file names its licence in an SPDX-License-Identifier line (the first line, or the second after a
# #! line), and the licence must be the one of its directory (LICENSING.md):
#
#   Apache-2.0      the client libraries and what only they use: include/tsdb.h, tsdb_protocol.h, tsdb_wire.h,
#                   tsdb_calendar.h; client/tsdb_{client,crypto,calendar,pack}.{c,h}; client/rust, client/python,
#                   client/python-common, client/java, connectors/pulsar; examples/client.c
#   AGPL-3.0-only   everything else (src/, server/, tools/, tests/, bench/, the embedded bindings, ...)
#
# Also checked: the licence field of every manifest (Cargo.toml, pom.xml, pyproject.toml) is that of its directory,
# and no Apache-2.0 file includes, imports or depends on an AGPL-3.0-only one (the other way round is allowed:
# Apache code may be part of an AGPL work). Run by `make test`; a file that is not in git (a tarball) is found with find.
#
#   tools/check-spdx.sh          check the tree
#   tools/check-spdx.sh --list   print "LICENCE PATH" of every checked file
set -u
cd "$(dirname "$0")/.." || exit 2
fail=0
bad() { echo "check-spdx: $*" >&2; fail=1; }

# the licence a path must have
expected() {
    case $1 in
    include/tsdb.h | include/tsdb_protocol.h | include/tsdb_wire.h | include/tsdb_calendar.h | examples/client.c \
        | client/tsdb_client.[ch] | client/tsdb_crypto.[ch] | client/tsdb_calendar.[ch] | client/tsdb_pack.[ch] \
        | client/rust/* | client/python/* | client/python-common/* | client/java/* | connectors/pulsar/*)
        echo Apache-2.0 ;;
    *) echo AGPL-3.0-only ;;
    esac
}

if git rev-parse --git-dir > /dev/null 2>&1; then
    files=$(git ls-files -co --exclude-standard | sort -u)
else
    files=$(find . -type f -not -path './build*' -not -path '*/target/*' -not -path './.git/*' | sed 's|^\./||' | sort)
fi

# the source files: by extension, or a script with a #! line; fixtures and fuzz corpora are data
is_source() {
    case $1 in
    tests/fuzz/corpus/* | tests/fuzz/frames/* | */tests/fixtures/* | */target/* | */csrc/*) return 1 ;;
    *.c | *.h | *.inc | *.rs | *.java | *.js | *.mjs | *.py | *.pyi | *.sh) return 0 ;;
    *.* ) return 1 ;;
    esac
    [ -f "$1" ] && [ "$(head -c 2 "$1")" = '#!' ]
}

spdx_of() { # the whole expression named in the first two lines of $1 (a comment closer cut off), or nothing
    head -n 2 "$1" | sed -n 's/.*SPDX-License-Identifier: *\(.*\)$/\1/p' | head -n 1 \
        | sed 's/[[:space:]]*\(\*\/\|-->\|"""\)[[:space:]]*$//; s/[[:space:]]*$//'
}

declare -A have
list=0
[ "${1:-}" = --list ] && list=1
while IFS= read -r f; do
    [ -f "$f" ] || continue
    is_source "$f" || continue
    want=$(expected "$f")
    got=$(spdx_of "$f")
    have[$f]=$got
    [ $list = 1 ] && echo "${got:-NONE} $f"
    if [ -z "$got" ]; then bad "$f: no SPDX-License-Identifier line (want $want)"
    elif [ "$got" != "$want" ]; then bad "$f: says $got, its directory is $want"
    fi
done <<< "$files"
[ $list = 1 ] && exit 0

# manifests: the licence field of the directory
while IFS= read -r f; do
    [ -f "$f" ] || continue
    case $f in */target/* | */csrc/*) continue ;; esac
    case $f in
    */Cargo.toml) got=$(sed -n 's/^license *= *"\([^"]*\)".*/\1/p' "$f" | head -n 1) ;;
    */pyproject.toml) got=$(sed -n 's/^license *= *"\([^"]*\)".*/\1/p' "$f" | head -n 1) ;;
    */pom.xml) got=$(sed -n '/<licenses>/,/<\/licenses>/s|.*<name>\([^<]*\)</name>.*|\1|p' "$f" | head -n 1) ;;
    *) continue ;;
    esac
    want=$(expected "$(dirname "$f")/x")
    # the directory of the manifest decides (client/rust/Cargo.toml: client/rust/x)
    [ -n "$got" ] || { bad "$f: no licence field"; continue; }
    [ "$got" = "$want" ] || bad "$f: licence $got, its directory is $want"
done <<< "$files"

# an Apache-2.0 file must not depend on an AGPL-3.0-only one
agpl_dep() { # $1 file, $2 pattern: any matching line is a dependency
    if grep -nE "$2" "$1" > /dev/null 2>&1; then bad "$1 (Apache-2.0) depends on AGPL-3.0-only code: $(grep -nE "$2" "$1" | head -n 1)"; fi
}
while IFS= read -r f; do
    [ -f "$f" ] || continue
    [ "$(expected "$f")" = Apache-2.0 ] || continue
    case $f in
    */target/* | */csrc/*) continue ;;
    *.rs) agpl_dep "$f" '^\s*(pub\s+)?(use|extern\s+crate)\s+(::)?(tsdb_embedded|tsdb_sys)\b' ;;
    *.py | *.pyi) agpl_dep "$f" '^\s*(import|from)\s+tsdb_embedded\b|(import_module|__import__)\(\s*["'"'"']tsdb_embedded' ;;
    *.java) agpl_dep "$f" '^\s*import\s+(static\s+)?ai\.intellistream\.tsdb\.embedded\b' ;;
    *Cargo.toml) agpl_dep "$f" 'intellistream-tsdb-(embedded|sys)|^\s*tsdb-(embedded|sys)\s*=' ;;
    *pyproject.toml) agpl_dep "$f" 'intellistream-tsdb-embedded|tsdb_embedded' ;;
    *pom.xml) agpl_dep "$f" '<artifactId>tsdb-embedded</artifactId>' ;;
    *.c | *.h)
        while IFS= read -r inc; do
            for cand in "$(dirname "$f")/$inc" "include/$inc" "client/$inc" "src/$inc"; do
                [ -f "$cand" ] || continue
                [ "$(expected "$cand")" = Apache-2.0 ] || bad "$f (Apache-2.0) includes $cand, which is AGPL-3.0-only"
                break
            done
        done < <(sed -n 's/^[[:space:]]*#[[:space:]]*include[[:space:]]*["<]\([^">]*\)[">].*/\1/p' "$f")
        ;;
    esac
done <<< "$files"
# pyproject.toml, Cargo.toml and pom.xml are not source files above, so they are looked at here by name

[ $fail = 0 ] && echo "ok: SPDX lines, manifests and the Apache/AGPL boundary"
exit $fail
