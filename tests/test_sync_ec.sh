#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# tools/sync-ec.sh --check has teeth: a hand edit of src/ec fails it in every clone (the hashes in UPSTREAM), a commit
# UPSTREAM names that an existing objects repository lacks fails it (stale after a rebase), and a missing repository only
# skips the comparison with the commit.
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
t=$(mktemp -d "${TMPDIR:-/tmp}/syncec.XXXXXX")
trap 'rm -rf "$t"' EXIT
mkdir -p "$t/tools" "$t/src"
cp "$here/tools/sync-ec.sh" "$t/tools/"
cp -r "$here/src/ec" "$t/src/ec"
chk() { EC_REPO="$1" sh "$t/tools/sync-ec.sh" --check; }
chk "$t/no-such-repo" > /dev/null || { echo "FAIL: an unchanged copy without the objects repository must pass"; exit 1; }
git init -q "$t/objects"
if chk "$t/objects" > /dev/null 2>&1; then echo "FAIL: a repository without UPSTREAM's commit must fail (stale UPSTREAM)"; exit 1; fi
echo "/* edited */" >> "$t/src/ec/gf256.c"
if chk "$t/no-such-repo" > /dev/null 2>&1; then echo "FAIL: a hand edit passes without the objects repository"; exit 1; fi
echo "test_sync_ec: ok"
