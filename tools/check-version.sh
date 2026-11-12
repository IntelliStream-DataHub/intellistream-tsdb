#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# The release version lives in the file VERSION. This checks that every other place says the same:
# the macros of include/tsdb.h, `tsdbd --version`, `tsdb --version`, and the manifests of the clients
# and the connector. A release bump edits VERSION and these (docs/release.md); this script finds a miss.
#
#   tools/check-version.sh [build-dir]        (default: build)
set -u
cd "$(dirname "$0")/.."
b=${1:-build}
v=$(tr -d ' \n' <VERSION)
fail=0
case $v in
[0-9]*.[0-9]*.[0-9]*) ;;
*) echo "check-version: VERSION is '$v', not MAJOR.MINOR.PATCH"; exit 1 ;;
esac
want() { # what, found
    if [ "$2" != "$v" ]; then
        echo "check-version: $1 says '$2', VERSION says '$v'"
        fail=1
    fi
}
def() { sed -n "s/^#define $1  *\"\{0,1\}\([^\"]*\)\"\{0,1\}\$/\1/p" include/tsdb.h; }
want "include/tsdb.h macros" "$(def TSDB_VERSION_MAJOR).$(def TSDB_VERSION_MINOR).$(def TSDB_VERSION_PATCH)"
want "include/tsdb.h TSDB_VERSION" "$(def TSDB_VERSION)"
want "tsdbd --version" "$("$b/tsdbd" --version | sed -n 's/^tsdbd \([^ ]*\).*/\1/p')"
want "tsdb --version" "$("$b/tsdb" --version | sed -n 's/^tsdb \([^ ]*\).*/\1/p')"
toml() { sed -n 's/^version = "\(.*\)"$/\1/p' "$1" | head -n 1; }
for f in client/rust/Cargo.toml client/rust-sys/Cargo.toml client/rust-embedded/Cargo.toml client/python/Cargo.toml \
    client/python-common/Cargo.toml client/python-embedded/Cargo.toml; do
    want "$f" "$(toml $f)"
done
# the exact pins between our own packages (the embedded crate and wheel use the network client's hidden internals)
for f in client/rust-embedded/Cargo.toml client/python/Cargo.toml client/python-common/Cargo.toml \
    client/python-embedded/Cargo.toml; do
    want "$f (intellistream-tsdb-client pin)" \
        "$(sed -n 's/^tsdb-client = {.*version = "=\([^"]*\)".*/\1/p' $f)"
done
want "client/python-embedded/Cargo.toml (intellistream-tsdb-embedded pin)" \
    "$(sed -n 's/^tsdb-embedded = {.*version = "=\([^"]*\)".*/\1/p' client/python-embedded/Cargo.toml)"
want "client/python-embedded/pyproject.toml (intellistream-tsdb-client pin)" \
    "$(sed -n 's/^dependencies = \["intellistream-tsdb-client==\([^"]*\)"\].*/\1/p' client/python-embedded/pyproject.toml)"
# every Cargo.lock in the tree that names one of our crates (a lock file of a test crate that depends on the client
# too): cargo build --locked refuses a lock file that still names the previous version
for f in $(git ls-files '*Cargo.lock' 2>/dev/null || find . -name Cargo.lock -not -path '*/target/*'); do
    for c in intellistream-tsdb-client intellistream-tsdb-sys intellistream-tsdb-embedded tsdb-client-python-common; do
        for lv in $(sed -n "/^name = \"$c\"\$/{n;s/^version = \"\(.*\)\"\$/\1/p;}" "$f"); do
            want "$f ($c)" "$lv"
        done
    done
done
# the first <version> after the project's own artifactId, and the dependencies on the Java client
pom() { sed -n 's/^  <version>\(.*\)<\/version>$/\1/p' "$1" | head -n 1; }
for f in client/java/pom.xml client/java-embedded/pom.xml connectors/pulsar/pom.xml; do
    want "$f" "$(pom $f)"
done
want "client/java-embedded/pom.xml (tsdb-client dependency)" \
    "$(grep -A2 '<artifactId>tsdb-client</artifactId>' client/java-embedded/pom.xml | sed -n 's/.*<version>\(.*\)<\/version>.*/\1/p')"
want "connectors/pulsar/pom.xml (tsdb-client.version)" \
    "$(sed -n 's/.*<tsdb-client.version>\(.*\)<\/tsdb-client.version>.*/\1/p' connectors/pulsar/pom.xml)"
[ $fail = 0 ] && echo "check-version: $v everywhere"
exit $fail
