#!/bin/bash
# A package of tsdb without its LICENSE fails. Apache-2.0 wants LICENSE and NOTICE with every copy, and the packages
# that link or pack other software also carry its notices (docs/licenses.md is the audit).
#
#   tools/check-package-licenses.sh wheel FILE.whl      .dist-info/licenses/{LICENSE,NOTICE,THIRD-PARTY-NOTICES}
#   tools/check-package-licenses.sh sdist FILE.tar.gz   the same at the top, and LICENSE/NOTICE in rust/ and rust-sys/
#   tools/check-package-licenses.sh jar FILE.jar        META-INF/{LICENSE,NOTICE}
#   tools/check-package-licenses.sh nar FILE.nar        the jar's, plus META-INF/THIRD-PARTY-NOTICES (zstd-jni, zstd, Jackson)
#   tools/check-package-licenses.sh crate DIR           what `cargo package --list` of the crate in DIR would ship
#   tools/check-package-licenses.sh sources             the copies in the client directories equal the root's;
#                                                       client/python/THIRD-PARTY-NOTICES is what the lockfile gives
#
# The LICENSE and NOTICE inside a package must be byte for byte those of the repository root.
set -u
root=$(cd "$(dirname "$0")/.." && pwd)
fail=0
bad() { echo "FAIL: $*" >&2; fail=1; }

# same FILE-IN-PACKAGE ROOTFILE: reads FILE-IN-PACKAGE from stdin
same() {
    if ! cmp -s - "$root/$2"; then bad "$1: $2 is missing or differs from the repository's"; fi
}

kind=${1:-}; target=${2:-}
case "$kind" in
wheel)
    [ -f "$target" ] || { echo "no such file: $target" >&2; exit 2; }
    names=$(unzip -Z1 "$target")
    d=$(echo "$names" | grep -m1 -E '\.dist-info/$' || true)
    d=${d:-$(echo "$names" | grep -m1 -E '\.dist-info/' | sed 's|/.*|/|')}
    for f in LICENSE NOTICE THIRD-PARTY-NOTICES; do
        echo "$names" | grep -qx "${d}licenses/$f" || bad "$target: ${d}licenses/$f missing"
    done
    for f in LICENSE NOTICE; do unzip -p "$target" "${d}licenses/$f" 2>/dev/null | same "$target ${d}licenses/$f" "$f"; done
    unzip -p "$target" "${d}licenses/THIRD-PARTY-NOTICES" 2>/dev/null | grep -q 'zstd' \
        || bad "$target: THIRD-PARTY-NOTICES does not mention zstd"
    ;;
sdist)
    [ -f "$target" ] || { echo "no such file: $target" >&2; exit 2; }
    names=$(tar tzf "$target")
    top=$(echo "$names" | head -n 1 | sed 's|/.*||')
    for f in LICENSE NOTICE THIRD-PARTY-NOTICES python/LICENSE python/NOTICE rust/LICENSE rust/NOTICE \
             rust-sys/LICENSE rust-sys/NOTICE; do
        echo "$names" | grep -qx "$top/$f" || bad "$target: $top/$f missing"
    done
    for f in LICENSE NOTICE rust/LICENSE rust/NOTICE rust-sys/LICENSE rust-sys/NOTICE; do
        tar xzOf "$target" "$top/$f" 2>/dev/null | same "$target $top/$f" "$(basename "$f")"
    done
    ;;
jar|nar)
    [ -f "$target" ] || { echo "no such file: $target" >&2; exit 2; }
    for f in LICENSE NOTICE; do
        unzip -Z1 "$target" | grep -qx "META-INF/$f" || bad "$target: META-INF/$f missing"
        unzip -p "$target" "META-INF/$f" 2>/dev/null | same "$target META-INF/$f" "$f"
    done
    if [ "$kind" = nar ]; then
        n=$(unzip -p "$target" META-INF/THIRD-PARTY-NOTICES 2>/dev/null)
        for w in zstd-jni 'BSD 2-Clause' 'BSD License' Jackson; do
            echo "$n" | grep -q "$w" || bad "$target: META-INF/THIRD-PARTY-NOTICES does not mention $w"
        done
    fi
    ;;
crate)
    [ -d "$target" ] || { echo "no such directory: $target" >&2; exit 2; }
    list=$(cd "$target" && cargo package --list --allow-dirty --offline 2>/dev/null)
    for f in LICENSE NOTICE; do
        echo "$list" | grep -qx "$f" || bad "$target: cargo package would not ship $f"
        same "$target/$f" "$f" < "$target/$f"
    done
    ;;
sources)
    for d in client/python client/rust client/rust-sys; do
        for f in LICENSE NOTICE; do same "$d/$f" "$f" < "$root/$d/$f"; done
    done
    "$root/tools/gen-third-party.py" --check >&2 || fail=1
    ;;
*)
    sed -n 2,14p "$0" >&2; exit 2 ;;
esac
[ $fail = 0 ] && echo "ok: $kind $target"
exit $fail
