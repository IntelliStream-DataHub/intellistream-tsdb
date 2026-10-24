#!/bin/sh
# Copies the engine's C sources into csrc/ so that a package of this crate (the Python sdist, which
# carries tsdb-sys as a path dependency) builds without the repository around it: build.rs uses
# csrc/ when ../../include/tsdb.h is not there. csrc/ is generated, never committed.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
root=$here/../..
rm -rf "$here/csrc"
mkdir -p "$here/csrc/include" "$here/csrc/src" "$here/csrc/client"
cp "$root"/include/*.h "$here/csrc/include/"
cp "$root"/src/*.c "$root"/src/*.h "$here/csrc/src/"
cp "$root/client/tsdb_calendar.c" "$here/csrc/client/"
