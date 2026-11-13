#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# tools/check-ec-symbols.sh looks only at the Apache-2.0 directories: the AGPL-3.0 bindings (client/rust-sys, whose
# csrc/ is a copy of src/*.c made by vendor.sh and names tsdb_ec_select in cpu.c, client/*-embedded) may name the
# erasure codec, a network client may not.
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
t=$(mktemp -d "${TMPDIR:-/tmp}/ecsym.XXXXXX")
trap 'rm -rf "$t"' EXIT
mkdir -p "$t/include" "$t/examples" "$t/client/rust/src" "$t/client/rust-sys/csrc/src" "$t/client/python-embedded" "$t/connectors"
echo 'int tsdb_open(void);' > "$t/include/tsdb.h"
echo 'tsdb_ec_select(level);' > "$t/client/rust-sys/csrc/src/cpu.c"
echo 'iso_rs_new' > "$t/client/python-embedded/glue.c"
export B=$t/none EC_ROOT=$t
sh "$here/tools/check-ec-symbols.sh" > /dev/null || { echo "FAIL: AGPL bindings naming the codec are refused"; exit 1; }
echo 'use iso_gf256_init;' > "$t/client/rust/src/lib.rs"
if sh "$here/tools/check-ec-symbols.sh" > /dev/null 2>&1; then echo "FAIL: an Apache-2.0 client naming the codec passes"; exit 1; fi
rm "$t/client/rust/src/lib.rs"; echo 'tsdb_ec_select' > "$t/include/tsdb_planted.h"
if sh "$here/tools/check-ec-symbols.sh" > /dev/null 2>&1; then echo "FAIL: include/ naming the codec passes"; exit 1; fi
echo "test_check_ec_symbols: ok"
