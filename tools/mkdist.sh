#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# Packs a `make dist` build into tsdb-VERSION-x86_64-linux.tar.gz (README.dist
# describes the contents and the requirements). VERSION is the release (the file
# VERSION, e.g. 0.9.0); BUILD_ID (default: git describe) goes into BUILDINFO.
#
#   tools/mkdist.sh build-dir [version]
set -eu
b=$1
version=${2:-$(cat "$(dirname "$0")/../VERSION")}
build_id=${BUILD_ID:-$(git describe --always --dirty 2>/dev/null || echo dev)}
name=tsdb-$version-x86_64-linux
stage=$b/$name
# One tarball per build dir: `make dist-test` and dist-check-almalinux10 find it by tsdb-*.
rm -rf "$b"/tsdb-*-x86_64-linux "$b"/tsdb-*-x86_64-linux.tar.gz
mkdir -p "$stage/bin" "$stage/bench" "$stage/lib" "$stage/include" "$stage/tests" "$stage/systemd"
cp "$b/tsdbd" "$b/tsdb" "$stage/bin/"
for f in codec-bench type-bench tsdb-bench insert-bench tsdbc-bench scale-bench; do cp "$b/$f" "$stage/bench/"; done
cp "$b/libtsdb.a" "$b/libtsdbclient.a" "$stage/lib/"
cp include/tsdb.h include/tsdb_protocol.h include/tsdb_wire.h include/tsdb_calendar.h client/tsdb_client.h "$stage/include/"
for t in test_tsdb test_types test_levels test_group test_follow test_volumes test_server test_stability test_ports; do
    cp "$b/$t" "$stage/tests/"
done
cp "$b/libcrashfs.so" "$stage/tests/" # the power-loss and EIO shim test_server preloads
cp dist/run-tests.sh "$stage/tests/"
cp dist/tsdbd.service "$stage/systemd/"
cp dist/README.dist "$stage/README.dist"
# The binaries contain zstd and liburing (linked statically): their licenses require that their notices
# travel with them (dist/THIRD-PARTY-NOTICES). The AGPL LICENSE, NOTICE and LICENSING.md travel with every copy, and so
# does the Apache-2.0 text (LICENSES/): the client library and the headers in this tarball are under it, and the
# calendar code in libtsdb.a too.
cp LICENSE NOTICE LICENSING.md dist/THIRD-PARTY-NOTICES "$stage/"
mkdir -p "$stage/LICENSES"
cp LICENSES/Apache-2.0.txt "$stage/LICENSES/"
{
    echo "version  $version"
    echo "build    $build_id"
    echo "built    $(date -u +%Y-%m-%dT%H:%MZ) on $(uname -sr)"
    echo "compiler $(${CC:-cc} --version | head -n 1)"
    echo "glibc    built against $(getconf GNU_LIBC_VERSION | cut -d' ' -f2); needs at most $(objdump -T "$b/tsdbd" | grep -o 'GLIBC_[0-9.]*' | sed 's/GLIBC_//' | sort -uV | tail -n 1)"
} >"$stage/BUILDINFO"
tar -C "$b" -czf "$b/$name.tar.gz" "$name"
echo "$b/$name.tar.gz"
