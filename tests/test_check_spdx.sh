#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# Tests tools/check-spdx.sh against scratch trees: the real tree passes; a file without a line, a wrong licence for
# its directory, a wrong manifest licence, and an Apache-2.0 file that includes, imports or depends on AGPL-3.0-only
# code each fail (a check that never fails would pass for ever); Apache code inside AGPL code passes.
# make test runs it.
#
#   tests/test_check_spdx.sh [scratch-dir]
set -u
top=$(cd "$(dirname "$0")/.." && pwd)
scratch=$(mktemp -d "${1:-${TMPDIR:-/tmp}}/tsdb-check-spdx.XXXXXX")
trap 'rm -rf "$scratch"' EXIT
fail=0
bad() { echo "FAIL: $*"; fail=1; }

A='SPDX-License-Identifier: Apache-2.0'
G='SPDX-License-Identifier: AGPL-3.0-only'

# a fresh tree: the script itself and a minimal layout with one file of every kind, all correct
fresh() {
    t=$scratch/tree
    rm -rf "$t"
    mkdir -p "$t/tools" "$t/src" "$t/include" "$t/client/rust/src" "$t/client/python" "$t/client/rust-embedded/src" \
        "$t/client/java/src" "$t/client/java-embedded/src"
    cp "$top/tools/check-spdx.sh" "$t/tools/"
    printf '/* %s */\n#include "tsdb.h"\n' "$G" > "$t/src/engine.c"
    printf '/* %s */\n#define X 1\n' "$A" > "$t/include/tsdb.h"
    printf '/* %s */\n#include "tsdb.h"\n' "$A" > "$t/client/tsdb_client.c"
    printf '/* %s */\n' "$A" > "$t/client/tsdb_client.h"
    printf '// %s\nuse std::io;\n' "$A" > "$t/client/rust/src/lib.rs"
    printf '// %s\nuse tsdb_client::Client;\n' "$G" > "$t/client/rust-embedded/src/lib.rs"
    printf '[package]\nname = "x"\nlicense = "Apache-2.0"\n' > "$t/client/rust/Cargo.toml"
    printf '[package]\nname = "y"\nlicense = "AGPL-3.0-only"\n' > "$t/client/rust-embedded/Cargo.toml"
    printf '# %s\nimport tsdb_client\n' "$A" > "$t/client/python/mod.py"
    printf '// %s\nclass A {}\n' "$A" > "$t/client/java/src/A.java"
    printf '// %s\nimport ai.intellistream.tsdb.Client;\n' "$G" > "$t/client/java-embedded/src/B.java"
    (cd "$t" && git init -q . && git add -A) > /dev/null 2>&1
}
# expect NAME WANT(0|1) [PATTERN]: run the check in the tree; exit status and, when it must fail, its message
expect() {
    local name=$1 want=$2 pat=${3:-}
    (cd "$t" && bash tools/check-spdx.sh) > "$scratch/out" 2>&1
    local rc=$?
    if [ "$want" = 0 ] && [ $rc != 0 ]; then bad "$name: failed: $(head -n 3 "$scratch/out")"; fi
    if [ "$want" = 1 ]; then
        [ $rc != 0 ] || bad "$name: passed"
        [ -z "$pat" ] || grep -q -- "$pat" "$scratch/out" || bad "$name: no message with '$pat': $(head -n 3 "$scratch/out")"
    fi
}
restage() { (cd "$t" && git add -A) > /dev/null 2>&1; }

# 1. the real tree
(cd "$top" && bash tools/check-spdx.sh) > "$scratch/out" 2>&1 || bad "the real tree: $(head -n 5 "$scratch/out")"

fresh; expect "fresh tree" 0

fresh; printf 'int x;\n' > "$t/src/nolic.c"; restage
expect "no SPDX line" 1 "src/nolic.c: no SPDX"

fresh; printf '/* %s */\n' "$A" > "$t/src/wrong.c"; restage
expect "an engine file saying Apache" 1 "src/wrong.c: says Apache-2.0"

fresh; printf '// %s\n' "$G" > "$t/client/rust/src/wrong.rs"; restage
expect "a client file saying AGPL" 1 "client/rust/src/wrong.rs: says AGPL"

fresh; printf '#!/bin/sh\n# %s\n' "$G" > "$t/src/ok.sh"; restage
expect "an SPDX line after #!" 0

fresh; sed -i 's/Apache-2.0"/AGPL-3.0-only"/' "$t/client/rust/Cargo.toml"; restage
expect "a client manifest saying AGPL" 1 "client/rust/Cargo.toml: licence AGPL"

fresh; printf '/* %s */\n#include "engine.h"\n' "$A" > "$t/client/tsdb_client.c"
printf '/* %s */\n' "$G" > "$t/src/engine.h"; restage
expect "an Apache C file including an AGPL header" 1 "includes src/engine.h"

fresh; printf '// %s\nuse tsdb_embedded::Db;\n' "$A" > "$t/client/rust/src/lib.rs"; restage
expect "an Apache Rust file using the embedded crate" 1 "depends on AGPL"

fresh; printf '[package]\nname = "x"\nlicense = "Apache-2.0"\n[dependencies]\nintellistream-tsdb-embedded = "1"\n' \
    > "$t/client/rust/Cargo.toml"; restage
expect "an Apache crate depending on the embedded crate" 1 "depends on AGPL"

fresh; printf '# %s\nfrom tsdb_embedded import Db\n' "$A" > "$t/client/python/mod.py"; restage
expect "an Apache Python file importing the embedded wheel" 1 "depends on AGPL"

fresh; printf '// %s\nimport ai.intellistream.tsdb.embedded.EmbeddedDb;\n' "$A" > "$t/client/java/src/A.java"; restage
expect "an Apache Java file importing EmbeddedDb" 1 "depends on AGPL"

# a compound expression is not the licence of the directory, whatever its first term says
fresh; printf '// SPDX-License-Identifier: Apache-2.0 OR AGPL-3.0-only\nuse std::io;\n' > "$t/client/rust/src/lib.rs"; restage
expect "an Apache file with a dual expression" 1 "client/rust/src/lib.rs: says Apache-2.0 OR AGPL-3.0-only"

fresh; printf '/* SPDX-License-Identifier: AGPL-3.0-only AND Apache-2.0 */\n' > "$t/src/both.c"; restage
expect "an engine file with a compound expression" 1 "src/both.c: says AGPL-3.0-only AND Apache-2.0"

# an include in angle brackets is an include too
fresh; printf '/* %s */\n#include <engine.h>\n' "$A" > "$t/client/tsdb_client.c"
printf '/* %s */\n' "$G" > "$t/src/engine.h"; restage
expect "an Apache C file including an AGPL header with <>" 1 "includes src/engine.h"

fresh; printf '/* %s */\n#include <stdio.h>\n#include <tsdb.h>\n' "$A" > "$t/client/tsdb_client.c"; restage
expect "an Apache C file including system and Apache headers with <>" 0

# a dynamic import of the embedded module
fresh; printf '# %s\nimport importlib\nm = importlib.import_module("tsdb_embedded")\n' "$A" > "$t/client/python/mod.py"; restage
expect "an Apache Python file importing the embedded wheel by name" 1 "depends on AGPL"

# the other way round is allowed: AGPL code includes and uses Apache code (the fresh tree has both already)
fresh; printf '/* %s */\n#include "tsdb_client.h"\n#include "tsdb.h"\n' "$G" > "$t/src/uses_client.c"; restage
expect "AGPL code including Apache headers" 0

[ $fail = 0 ] && echo "ok: check-spdx finds what it must"
exit $fail
