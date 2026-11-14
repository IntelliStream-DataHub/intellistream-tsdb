#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# Copy the erasure codec (GF(2^8) arithmetic and Reed-Solomon) from intellistream-objects into src/ec/ and record where
# it came from in src/ec/UPSTREAM (docs/design-cluster-data.md §11 R5, decision E32).
#
#   tools/sync-ec.sh [--repo DIR] COMMIT     copy src/codec/{gf256*,rs*} of COMMIT
#   tools/sync-ec.sh [--repo DIR] --check    compare src/ec with the commit UPSTREAM names (make test runs it)
#
# The files are copied byte for byte below an AGPL-3.0-only header, so `git diff src/ec` after a sync shows exactly what
# changed upstream. Nothing in src/ec/{gf256,gf256_avx2,gf256_avx512,rs}.[ch] is edited by hand: what the files include
# from objects' src/common is src/ec/compat.h and src/ec/compat.c (tsdb's own, not synced), reached through the
# one-line headers under src/ec/compat/. gf256.c has no startup code when it is compiled with -DISO_GF256_NO_BOOT (the
# Makefile does): src/cpu.c calls iso_gf256_init() through tsdb_ec_select() instead (AGENTS.md "CPU levels").
# intellistream-objects and tsdb have the same owner (IntelliStream AS) and the same licence, AGPL-3.0-only.
#
# --check first compares the sha256 of each file in src/ec with the ones src/ec/UPSTREAM holds (always, in every clone:
# a hand edit fails), then each file with the commit UPSTREAM names. It exits 1 when one differs; 0 with a "skipped"
# line for the second part when the objects repository is not there (a clone without it, a CI runner). A repository that
# is there but has no such commit is a stale UPSTREAM (the branch was rebased, the commit never pushed): that fails.
set -eu

repo=${EC_REPO:-$(dirname "$0")/../../intellistream-objects}
if [ "${1:-}" = --repo ]; then repo=$2; shift 2; fi
[ $# -eq 1 ] || { echo "usage: $0 [--repo DIR] COMMIT|--check" >&2; exit 2; }
top=$(cd "$(dirname "$0")/.." && pwd)
dst=$top/src/ec
files="gf256.h gf256.c gf256_avx2.c gf256_avx512.c rs.h rs.c"
hdr_lines=6

header() {
    cat <<'HDR'
/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Copyright 2026 IntelliStream AS
 *
 * Copied from intellistream-objects src/codec (src/ec/UPSTREAM names the commit) by tools/sync-ec.sh; everything
 * below this comment is the upstream file unchanged. Do not edit it here: change it upstream and sync.
 */
HDR
}

if [ "$1" = --check ]; then
    if [ ! -f "$dst/UPSTREAM" ]; then echo "check-ec-sync: no src/ec/UPSTREAM" >&2; exit 1; fi
    commit=$(sed -n 's/^commit: //p' "$dst/UPSTREAM")
    bad=0
    for f in $files; do
        want=$(sed -n "s/^sha256 $f: //p" "$dst/UPSTREAM")
        have=$(sha256sum "$dst/$f" | cut -d' ' -f1)
        if [ "$want" != "$have" ]; then
            echo "check-ec-sync: src/ec/$f is not the file UPSTREAM hashes (hand edit? run tools/sync-ec.sh)" >&2
            bad=1
        fi
    done
    if [ ! -d "$repo/.git" ] && ! git -C "$repo" rev-parse --git-dir >/dev/null 2>&1; then
        echo "check-ec-sync: hashes checked; skipped the comparison with $commit (no intellistream-objects at $repo)"
        exit $bad
    fi
    if ! git -C "$repo" cat-file -e "$commit^{commit}" 2>/dev/null; then
        echo "check-ec-sync: intellistream-objects at $repo has no commit $commit: UPSTREAM is stale (rebased, or never pushed?);" \
            "run tools/sync-ec.sh on the commit that has these files" >&2
        exit 1
    fi
    for f in $files; do
        if ! { header; git -C "$repo" show "$commit:src/codec/$f"; } | cmp -s - "$dst/$f"; then
            echo "check-ec-sync: src/ec/$f differs from $commit:src/codec/$f (hand edit? run tools/sync-ec.sh)" >&2
            bad=1
        fi
    done
    [ $bad = 0 ] && echo "check-ec-sync: src/ec matches $commit"
    exit $bad
fi

commit=$(git -C "$repo" rev-parse --verify "$1^{commit}")
mkdir -p "$dst"
for f in $files; do
    tmp=$dst/.$f.tmp
    { header; git -C "$repo" show "$commit:src/codec/$f"; } > "$tmp"
    mv "$tmp" "$dst/$f"
done

sums=""
for f in $files; do sums="$sums$(printf 'sha256 %s: %s' "$f" "$(sha256sum "$dst/$f" | cut -d' ' -f1)")
"; done
sums=${sums%?}
cat > "$dst/UPSTREAM" <<UP
repository: intellistream-objects (git@repo.intellistream.ai:olavgg/intellistream-objects.git), directory src/codec
commit: $commit
files: $files
license: AGPL-3.0-only, as intellistream-objects (LICENSE there); the owner is IntelliStream AS, as tsdb's, so no grant is needed.
local: compat.h and compat.c (what the files include from isod's src/common: the CPU features capped by tsdb's level,
  the error codes, the allocators, the clock), and the one-line headers under compat/ that point there; tsdb's own, not synced.
built: with -DISO_GF256_NO_BOOT (no constructor: src/cpu.c calls tsdb_ec_select(), which calls iso_gf256_init()).
$sums
sync: tools/sync-ec.sh [--repo DIR] COMMIT; tools/sync-ec.sh --check compares the copy with these hashes and this commit.
UP
echo "src/ec synced to $commit"
