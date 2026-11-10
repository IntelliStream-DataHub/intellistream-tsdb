#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# Copy the Raft core (core, log, wire, simulator) from intellistream-objects into src/raft/ and record where it came
# from in src/raft/UPSTREAM (docs/design-cluster.md §2.3, decision D2).
#
#   tools/sync-raft.sh [--repo DIR] COMMIT
#
# The files are copied byte for byte below an AGPL-3.0-only header, so `git diff src/raft` after a sync shows exactly
# what changed upstream. Nothing in src/raft/*.[ch] is edited by hand: what tsdb needs of isod's common code is in
# src/raft/compat.h and compat.c, which this script does not touch. The owner of intellistream-objects granted
# copying these files into tsdb on 2026-11-06 under Apache 2.0; intellistream-objects is AGPL-3.0-only now, as tsdb is,
# and the header says so.
set -eu

repo=${RAFT_REPO:-$(dirname "$0")/../../intellistream-objects}
if [ "${1:-}" = --repo ]; then repo=$2; shift 2; fi
[ $# -eq 1 ] || { echo "usage: $0 [--repo DIR] COMMIT" >&2; exit 2; }
commit=$(git -C "$repo" rev-parse --verify "$1^{commit}")
top=$(cd "$(dirname "$0")/.." && pwd)
dst=$top/src/raft
files="raft.h raft.c raft_log.h raft_log.c raft_wire.h raft_wire.c raft_sim.h raft_sim.c"

mkdir -p "$dst"
for f in $files; do
    tmp=$dst/.$f.tmp
    {
        cat <<'EOF'
/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Copyright 2026 IntelliStream AS
 *
 * Copied from intellistream-objects src/raft (src/raft/UPSTREAM names the commit) by tools/sync-raft.sh; everything
 * below this comment is the upstream file unchanged. Do not edit it here: change it upstream and sync.
 */
EOF
        git -C "$repo" show "$commit:src/raft/$f"
    } > "$tmp"
    mv "$tmp" "$dst/$f"
done

cat > "$dst/UPSTREAM" <<EOF
repository: intellistream-objects (git@repo.intellistream.ai:olavgg/intellistream-objects.git), directory src/raft
commit: $commit
files: $files
license: AGPL-3.0-only, as intellistream-objects (the owner, IntelliStream AS, Olav Gjerde, granted copying the files into
  tsdb on 2026-11-06 under Apache 2.0, decision D2 of docs/design-cluster.md; both repositories are AGPL-3.0-only now).
local: compat.h and compat.c (what the files include from isod's src/common and src/proto), and the one-line
  headers under compat/ that point there; tsdb's own, not synced.
sync: tools/sync-raft.sh [--repo DIR] COMMIT
EOF
echo "src/raft synced to $commit"
