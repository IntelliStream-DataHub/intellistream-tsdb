#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# The erasure codec (src/ec) is AGPL-3.0-only and lives in libtsdb and tsdbd; the Apache-2.0 network clients must never
# contain it (docs/design-cluster-data.md E32, docs/licenses.md): settled data is decoded on servers only. Fails when
#
#   - a source of an Apache-2.0 client names the codec (the Apache-2.0 directories of tools/check-spdx.sh: client/tsdb_{client,
#     crypto,calendar,pack}.*, client/rust, client/python, client/python-common, client/java, connectors/pulsar, include/,
#     examples/client.c; not the AGPL-3.0 bindings client/*-embedded and client/rust-sys, whose csrc/ copies the
#     engine's src/*.c, cpu.c included), or
#   - the C client library (libtsdbclient.a), or a client package that has been built, holds one of its symbols or files.
#
# Packages are looked for where the Makefile puts them (build/sdist*, client/java/target, client/rust/target) and skipped,
# with a line saying so, when they were not built: `make test-clients` builds them, and CI runs this after it.
#
#   B=build tools/check-ec-symbols.sh [--lib ARCHIVE]    --lib: look in ARCHIVE instead of libtsdbclient.a (the test
#                                                        of this script gives it libtsdb.a, which must fail it)
#   EC_ROOT=DIR                                          look in DIR instead of the repository (tests/test_check_ec_symbols.sh)
set -eu
cd "${EC_ROOT:-$(dirname "$0")/..}"
B=${B:-build}
lib=$B/libtsdbclient.a
if [ "${1:-}" = --lib ]; then lib=$2; fi
pat='iso_gf256_|iso_rs_|tsdb_ec_|gf256_avx|ISO_GF256_|src/ec/'
bad=0

apache=""
for f in client/tsdb_client.* client/tsdb_crypto.* client/tsdb_calendar.* client/tsdb_pack.* client/rust client/python \
    client/python-common client/java connectors/pulsar include examples/client.c; do
    [ -e "$f" ] && apache="$apache $f"
done
hits=$(grep -rnE "$pat|ec/(gf256|rs)\.h" $apache 2>/dev/null \
    --exclude-dir=target --exclude-dir=node_modules --exclude-dir=.git | grep -v '^client/[^:]*/\(target\|build\)/' || true)
if [ -n "$hits" ]; then
    echo "check-ec-symbols: an Apache-2.0 client source names the erasure codec:" >&2
    echo "$hits" | head -10 >&2
    bad=1
fi

if [ -f "$lib" ]; then
    if nm -A "$lib" 2>/dev/null | grep -E "$pat"; then
        echo "check-ec-symbols: $lib holds erasure codec symbols" >&2
        bad=1
    fi
else
    echo "check-ec-symbols: $lib not built, skipped"
fi

# the contents of a built package, as the strings in every member (zip and tar members are decompressed)
scan() {
    f=$1
    case "$f" in
    *.whl | *.jar | *.nar | *.zip) listing=$(unzip -Z1 "$f") && body() { unzip -p "$f" '*'; } ;;
    *.tar.gz) listing=$(tar tzf "$f") && body() { tar xzOf "$f"; } ;;
    *) listing=""; body() { cat "$f"; } ;;
    esac
    if echo "$listing" | grep -Eq '(^|/)ec/(gf256|rs)|gf256|reed'; then
        echo "check-ec-symbols: $f contains a codec file" >&2
        bad=1
    fi
    if body 2>/dev/null | strings -a | grep -Eq "$pat"; then
        echo "check-ec-symbols: $f contains codec symbols" >&2
        bad=1
    fi
}
n=0
for f in "$B"/sdist/intellistream_tsdb_client-*.tar.gz "$B"/sdist-check/whl/intellistream_tsdb_client-*.whl \
    client/java/target/*.jar client/rust/target/release/libtsdb_client*.rlib client/rust/target/debug/libtsdb_client*.rlib \
    connectors/pulsar/target/*.nar; do
    [ -f "$f" ] || continue
    scan "$f"
    n=$((n + 1))
done
echo "check-ec-symbols: $n client package(s) scanned$([ "$n" = 0 ] && echo " (none built)")"
[ $bad = 0 ] || exit 1
echo "check-ec-symbols: no erasure codec in the client libraries"
