#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# Mutation run for the directory scans: every `rewinddir` that follows `fdopendir(dup(fd))` is removed, one at a time,
# and the tests that open or scan directories must fail (the dup shares its offset with fd, so a scan that does not
# rewind sees nothing once an earlier scan ran). Usage: tests/rewind_mutants.sh NEWDIR
# Builds a copy of HEAD in NEWDIR (which must not exist: nothing is deleted), with build/libcrashfs.so (without it
# test_export fails on the clean tree, and every mutant "dies" of that: the first version of this run counted those),
# runs every test once on the clean copy (a control: it must be green, or no result means anything), then each mutant.
# A site in EQUIVALENT is one whose descriptor was opened a few lines before the scan and never scanned: removing
# its rewinddir changes no behaviour, so no test can fail (checked by reading the callers, 2026-11-09). The run fails
# when a site that is not equivalent survives, or when an equivalent one is killed (the list is stale).
set -u
dir=${1:?usage: rewind_mutants.sh NEWDIR}
[ ! -e "$dir" ] || { echo "rewind_mutants: $dir exists" >&2; exit 2; }
root=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$dir" && (cd "$root" && git archive HEAD) | tar -x -C "$dir" || exit 2
cd "$dir" || exit 2
export TMPDIR=$dir/scratch
mkdir -p "$TMPDIR"
T="test_tsdb test_recover test_volumes test_feedstore test_fno test_export test_upgrade test_damage test_graveyard test_classes test_follow test_failstop test_compat test_delete test_flush_tail"
build() { make -j"${JOBS:-6}" $(for t in $T; do echo build/$t; done) build/tsdb build/libcrashfs.so >"$dir/build.log" 2>&1; }
build || { echo "rewind_mutants: the clean build failed (see $dir/build.log)"; exit 2; }
run_all() { # prints the failing tests; empty when all pass
    local k=""
    for t in $T; do
        TSDB_REQUIRE_CRASHFS=1 timeout 900 build/$t "$TMPDIR" >"$dir/m.$t.log" 2>&1 || k="$k $t"
    done
    echo "$k"
}
control=$(run_all)
if [ -n "$control" ]; then
    echo "rewind_mutants: the control run (no mutant) fails in:$control (logs $dir/m.*.log): the result would mean nothing"
    exit 2
fi
echo "control run: green"
# file:line of each rewinddir, with `eq` for the sites no test can tell
sites="src/feedfile.c:474 src/feedfile.c:517:eq src/feedfile.c:540 src/db.c:653 src/memtable.c:1516 src/volumes.c:365:eq src/volumes.c:3529:eq src/feedclean.c:127 src/part.c:1098 tools/dump.c:1925:eq"
bad=0
for s in $sites; do
    eq=; [ "${s##*:}" = eq ] && { eq=1; s=${s%:eq}; }
    f=${s%%:*}; l=${s##*:}
    grep -q "^[[:space:]]*rewinddir(" <(sed -n "${l}p" "$f") || { echo "MUTANT $s: no rewinddir at that line (the list is stale)"; bad=1; continue; }
    cp "$f" "$dir/orig.bak"
    sed -i "${l}s/rewinddir([a-z]*);/(void)0;/" "$f"
    if build; then
        killed=$(run_all)
    else
        killed=" build-failed"
    fi
    cp "$dir/orig.bak" "$f"
    if [ -n "$killed" ]; then echo "MUTANT $s: killed by:$killed"; else echo "MUTANT $s: SURVIVED${eq:+ (expected: equivalent)}"; fi
    if [ -z "$eq" ] && [ -z "$killed" ]; then bad=1; fi
    if [ -n "$eq" ] && [ -n "$killed" ]; then echo "  an equivalent site was killed: the list is stale"; bad=1; fi
done
[ $bad = 0 ] && echo "rewind_mutants: every site that matters is killed" || echo "rewind_mutants: FAILED"
exit $bad
