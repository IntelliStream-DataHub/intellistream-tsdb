#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# A package of tsdb without its LICENSE fails. tsdb has two licences (LICENSING.md): the client libraries are
# Apache-2.0, the engine, the server and the embedded bindings AGPL-3.0-only. A package is one or the other, and
# carries the text of its own licence as LICENSE, the repository's NOTICE, and, where it packs other software, its
# notices (docs/licenses.md is the audit). An AGPL package that contains files of the Apache-2.0 clients (the headers,
# the calendar code) also carries the Apache text, as LICENSES/Apache-2.0.txt.
#
#   tools/check-package-licenses.sh wheel FILE.whl LICENCE         .dist-info/licenses/{LICENSE,NOTICE,THIRD-PARTY-NOTICES}
#   tools/check-package-licenses.sh sdist FILE.tar.gz LICENCE DIR=LICENCE...
#                                                                  the same at the top, and LICENSE/NOTICE in each DIR
#   tools/check-package-licenses.sh jar FILE.jar LICENCE           META-INF/{LICENSE,NOTICE}
#   tools/check-package-licenses.sh nar FILE.nar LICENCE           the jar's, plus META-INF/THIRD-PARTY-NOTICES
#   tools/check-package-licenses.sh crate DIR LICENCE              what `cargo package --list` of the crate in DIR ships
#   tools/check-package-licenses.sh sources                        the copies in the package directories equal the
#                                                                  reference texts; THIRD-PARTY-NOTICES are the lockfiles'
#
# LICENCE is apache or agpl. The LICENSE inside a package is byte for byte LICENSES/Apache-2.0.txt (apache) or the
# repository's LICENSE (agpl); NOTICE is the repository's NOTICE in both.
set -u
root=$(cd "$(dirname "$0")/.." && pwd)
fail=0
bad() { echo "FAIL: $*" >&2; fail=1; }

# the repository file a LICENSE of the given licence must equal
ref_license() {
    case $1 in
    apache) echo LICENSES/Apache-2.0.txt ;;
    agpl) echo LICENSE ;;
    *) echo "licence must be apache or agpl, not '$1'" >&2; exit 2 ;;
    esac
}

# same FILE-IN-PACKAGE ROOTFILE: reads FILE-IN-PACKAGE from stdin
same() {
    if ! cmp -s - "$root/$2"; then bad "$1: $2 is missing or differs from the repository's"; fi
}

kind=${1:-}; target=${2:-}; licence=${3:-}
case "$kind" in
wheel)
    [ -f "$target" ] || { echo "no such file: $target" >&2; exit 2; }
    lic=$(ref_license "$licence")
    names=$(unzip -Z1 "$target")
    d=$(echo "$names" | grep -m1 -E '\.dist-info/$' || true)
    d=${d:-$(echo "$names" | grep -m1 -E '\.dist-info/' | sed 's|/.*|/|')}
    extra=
    [ "$licence" = agpl ] && extra=LICENSES/Apache-2.0.txt
    for f in LICENSE NOTICE THIRD-PARTY-NOTICES $extra; do
        echo "$names" | grep -qx "${d}licenses/$f" || bad "$target: ${d}licenses/$f missing"
    done
    unzip -p "$target" "${d}licenses/LICENSE" 2>/dev/null | same "$target ${d}licenses/LICENSE" "$lic"
    unzip -p "$target" "${d}licenses/NOTICE" 2>/dev/null | same "$target ${d}licenses/NOTICE" NOTICE
    if [ -n "$extra" ]; then
        unzip -p "$target" "${d}licenses/$extra" 2>/dev/null | same "$target ${d}licenses/$extra" LICENSES/Apache-2.0.txt
    fi
    unzip -p "$target" "${d}licenses/THIRD-PARTY-NOTICES" 2>/dev/null | grep -q 'zstd' \
        || bad "$target: THIRD-PARTY-NOTICES does not mention zstd"
    ;;
sdist)
    [ -f "$target" ] || { echo "no such file: $target" >&2; exit 2; }
    lic=$(ref_license "$licence")
    names=$(tar tzf "$target")
    top=$(echo "$names" | head -n 1 | sed 's|/.*||')
    for f in LICENSE NOTICE THIRD-PARTY-NOTICES; do
        echo "$names" | grep -qx "$top/$f" || bad "$target: $top/$f missing"
    done
    tar xzOf "$target" "$top/LICENSE" 2>/dev/null | same "$target $top/LICENSE" "$lic"
    tar xzOf "$target" "$top/NOTICE" 2>/dev/null | same "$target $top/NOTICE" NOTICE
    if [ "$licence" = agpl ]; then
        tar xzOf "$target" "$top/LICENSES/Apache-2.0.txt" 2>/dev/null \
            | same "$target $top/LICENSES/Apache-2.0.txt" LICENSES/Apache-2.0.txt
    fi
    shift 3
    for spec in "$@"; do
        dir=${spec%%=*}; dl=${spec#*=}
        dlic=$(ref_license "$dl")
        for f in LICENSE NOTICE; do
            echo "$names" | grep -qx "$top/$dir/$f" || bad "$target: $top/$dir/$f missing"
        done
        tar xzOf "$target" "$top/$dir/LICENSE" 2>/dev/null | same "$target $top/$dir/LICENSE" "$dlic"
        tar xzOf "$target" "$top/$dir/NOTICE" 2>/dev/null | same "$target $top/$dir/NOTICE" NOTICE
        # an AGPL directory with Apache files in it (rust-sys: the headers and the calendar code of csrc/)
        if [ "$dl" = agpl ] && echo "$names" | grep -q "^$top/$dir/csrc/"; then
            echo "$names" | grep -qx "$top/$dir/LICENSES/Apache-2.0.txt" \
                || bad "$target: $top/$dir/LICENSES/Apache-2.0.txt missing"
        fi
    done
    ;;
jar|nar)
    [ -f "$target" ] || { echo "no such file: $target" >&2; exit 2; }
    lic=$(ref_license "$licence")
    unzip -Z1 "$target" | grep -qx "META-INF/LICENSE" || bad "$target: META-INF/LICENSE missing"
    unzip -p "$target" "META-INF/LICENSE" 2>/dev/null | same "$target META-INF/LICENSE" "$lic"
    unzip -Z1 "$target" | grep -qx "META-INF/NOTICE" || bad "$target: META-INF/NOTICE missing"
    unzip -p "$target" "META-INF/NOTICE" 2>/dev/null | same "$target META-INF/NOTICE" NOTICE
    if [ "$licence" = agpl ]; then
        unzip -Z1 "$target" | grep -qx "META-INF/LICENSES/Apache-2.0.txt" \
            || bad "$target: META-INF/LICENSES/Apache-2.0.txt missing"
        unzip -p "$target" "META-INF/LICENSES/Apache-2.0.txt" 2>/dev/null \
            | same "$target META-INF/LICENSES/Apache-2.0.txt" LICENSES/Apache-2.0.txt
    fi
    if [ "$kind" = nar ]; then
        n=$(unzip -p "$target" META-INF/THIRD-PARTY-NOTICES 2>/dev/null)
        for w in zstd-jni 'BSD 2-Clause' 'BSD License' Jackson; do
            echo "$n" | grep -q "$w" || bad "$target: META-INF/THIRD-PARTY-NOTICES does not mention $w"
        done
    fi
    ;;
crate)
    [ -d "$target" ] || { echo "no such directory: $target" >&2; exit 2; }
    lic=$(ref_license "$licence")
    list=$(cd "$target" && cargo package --list --allow-dirty --offline 2>/dev/null)
    for f in LICENSE NOTICE; do
        echo "$list" | grep -qx "$f" || bad "$target: cargo package would not ship $f"
    done
    same "$target/LICENSE" "$lic" < "$target/LICENSE"
    same "$target/NOTICE" NOTICE < "$target/NOTICE"
    if [ "$licence" = agpl ] && [ -d "$target/LICENSES" ]; then
        echo "$list" | grep -qx "LICENSES/Apache-2.0.txt" || bad "$target: cargo package would not ship LICENSES/Apache-2.0.txt"
    fi
    ;;
sources)
    # directory:licence of every package directory; the AGPL ones that hold Apache files also carry the Apache text
    for spec in client/rust:apache client/python:apache client/python-common:apache client/java:apache \
                connectors/pulsar:apache client/rust-sys:agpl client/rust-embedded:agpl client/python-embedded:agpl \
                client/java-embedded:agpl; do
        d=${spec%%:*}; l=${spec#*:}
        same "$d/LICENSE" "$(ref_license "$l")" < "$root/$d/LICENSE"
        same "$d/NOTICE" NOTICE < "$root/$d/NOTICE"
    done
    for d in client/rust-sys client/python-embedded client/java-embedded; do
        same "$d/LICENSES/Apache-2.0.txt" LICENSES/Apache-2.0.txt < "$root/$d/LICENSES/Apache-2.0.txt"
    done
    # the root LICENSE is the AGPL (the same text as intellistream-objects'), LICENSES/ has the Apache text
    grep -q 'GNU AFFERO GENERAL PUBLIC LICENSE' "$root/LICENSE" || bad "LICENSE is not the AGPL"
    grep -q 'Apache License' "$root/LICENSES/Apache-2.0.txt" || bad "LICENSES/Apache-2.0.txt is not the Apache License"
    "$root/tools/gen-third-party.py" --check >&2 || fail=1
    ;;
*)
    sed -n 3,19p "$0" >&2; exit 2 ;;
esac
[ $fail = 0 ] && echo "ok: $kind $target"
exit $fail
