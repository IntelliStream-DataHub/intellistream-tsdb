#!/bin/sh
# Copy the Raft core (core, log, wire, simulator) from intellistream-objects into src/raft/ and record where it came
# from in src/raft/UPSTREAM (docs/design-cluster.md §2.3, decision D2).
#
#   tools/sync-raft.sh [--repo DIR] COMMIT
#
# The files are copied byte for byte below an Apache 2.0 header, so `git diff src/raft` after a sync shows exactly
# what changed upstream. Nothing in src/raft/*.[ch] is edited by hand: what tsdb needs of isod's common code is in
# src/raft/compat.h and compat.c, which this script does not touch. The owner of intellistream-objects granted
# copying these files into tsdb under Apache 2.0 on 2026-11-06.
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
/*
 * Copyright 2026 IntelliStream AS
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the License.
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
license: Apache License 2.0. The owner of intellistream-objects (IntelliStream AS, Olav Gjerde) granted copying
  these files into tsdb under Apache 2.0 on 2026-11-06 (docs/design-cluster.md, decision D2).
local: compat.h and compat.c (what the files include from isod's src/common and src/proto), and the one-line
  headers under compat/ that point there; tsdb's own, not synced.
sync: tools/sync-raft.sh [--repo DIR] COMMIT
EOF
echo "src/raft synced to $commit"
