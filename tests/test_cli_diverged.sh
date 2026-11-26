#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# The CLI's `volume diverged [PATH] [--clear]` (src/ecdiverge.h): a volume without DIVERGED says so and --clear has
# nothing to do; a cluster volume's directory (META requires=cluster) is refused with a line naming R4 (D23); a bad
# argument is refused. The records themselves (listing, clearing, refused while a writer holds the directory, the
# volume served again) are test_keep's (clearcli), which makes them through a real settle.
#
#   B=build tests/test_cli_diverged.sh      (make test runs it)
set -u
cd "$(dirname "$0")/.."
B=${B:-build}
dir=$(mktemp -d "${TMPDIR:-/tmp}/tsdb-cli-diverged.XXXXXX")
trap 'rm -rf "$dir"' EXIT
fail() { echo "test_cli_diverged: FAIL: $*" >&2; exit 1; }

"$B/tsdb" "$dir/db" create s >/dev/null || fail "create s"
out=$("$B/tsdb" "$dir/db" volume diverged) || fail "volume diverged: exit $?"
[[ $out == *"not diverged"* ]] || fail "volume diverged said: $out"
"$B/tsdb" "$dir/db" volume diverged --clear >/dev/null || fail "--clear with nothing to clear"
"$B/tsdb" "$dir/db" volume diverged --bogus >/dev/null 2>&1 && fail "an unknown option was taken"

mkdir "$dir/cv"
printf 'version=9\nrequires=cluster,replica\n' >"$dir/cv/META"
err=$("$B/tsdb" "$dir/cv" volume diverged --clear 2>&1 >/dev/null) && fail "--clear on a cluster volume"
[[ $err == *R4* && $err == *refused* ]] || fail "the refusal does not name R4: $err"

# r8i open: PATH must be a volume of DIR (DIR itself or listed in DIR/VOLUMES)
err=$("$B/tsdb" "$dir/db" volume diverged "$dir/cv" --clear 2>&1 >/dev/null) && fail "accepted non-volume PATH with --clear"
[[ $err == *"not a volume"* ]] || fail "non-volume refusal message: $err"
err=$("$B/tsdb" "$dir/db" volume diverged "$dir/cv" 2>&1 >/dev/null) && fail "accepted non-volume PATH without --clear"
[[ $err == *"not a volume"* ]] || fail "non-volume listing refusal message: $err"

# Another independent database directory (not a volume of $dir/db) is also refused
"$B/tsdb" "$dir/other" create o >/dev/null || fail "create o in other"
err=$("$B/tsdb" "$dir/db" volume diverged "$dir/other" --clear 2>&1 >/dev/null) && fail "accepted another db as volume"
[[ $err == *"not a volume"* ]] || fail "other db refusal message: $err"

# A real volume added to $dir/db (plus explicit DIR and trailing slash) is accepted; symlinks are refused (SEC-05)
"$B/tsdb" "$dir/db" volume add "$dir/v1" >/dev/null || fail "volume add v1"
ln -s "$dir/v1" "$dir/v1_link"
for p in "$dir/db" "$dir/db/" "$dir/v1" "$dir/v1/"; do
    out=$("$B/tsdb" "$dir/db" volume diverged "$p") || fail "volume diverged on $p: exit $?"
    [[ $out == *"not diverged"* ]] || fail "volume diverged on $p said: $out"
    "$B/tsdb" "$dir/db" volume diverged "$p" --clear >/dev/null || fail "--clear on valid volume $p"
done
for p in "$dir/v1_link" "$dir/v1_link/"; do
    "$B/tsdb" "$dir/db" volume diverged "$p" >/dev/null 2>&1 && fail "accepted symlinked volume PATH $p"
    "$B/tsdb" "$dir/db" volume diverged "$p" --clear >/dev/null 2>&1 && fail "accepted symlinked volume PATH $p --clear"
done
# Symlinked LOCK or META inside a volume directory is refused and leaves the target untouched
rm -f "$dir/v1/LOCK"
ln -s "$dir/lock_victim" "$dir/v1/LOCK"
"$B/tsdb" "$dir/db" volume diverged "$dir/v1" --clear >/dev/null 2>&1 && fail "accepted symlinked LOCK"
[ ! -e "$dir/lock_victim" ] || fail "symlinked LOCK created lock_victim"
rm -f "$dir/v1/LOCK"

# Non-existent PATH, non-directory PATH (ENOTDIR), relative PATH, and corrupted VOLUMES checksum
err=$("$B/tsdb" "$dir/db" volume diverged "$dir/nope" 2>&1 >/dev/null) && fail "accepted non-existent PATH"
[[ $err == *"nope"* ]] || fail "non-existent PATH error: $err"

: >"$dir/notadir"
err=$("$B/tsdb" "$dir/db" volume diverged "$dir/notadir" 2>&1 >/dev/null) && fail "accepted non-directory PATH"
[[ $err == *"Not a directory"* ]] || fail "non-directory PATH error: $err"

tsdb_bin=$(readlink -f "$B/tsdb")
out=$(cd "$dir" && "$tsdb_bin" db volume diverged v1) || fail "relative PATH failed: exit $?"
[[ $out == *"not diverged"* ]] || fail "relative PATH said: $out"
(cd "$dir" && "$tsdb_bin" db volume diverged v1 --clear >/dev/null) || fail "relative PATH --clear failed"
err=$(cd "$dir" && "$tsdb_bin" db volume diverged other 2>&1 >/dev/null) && fail "accepted relative non-volume PATH"
[[ $err == *"not a volume"* ]] || fail "relative non-volume error: $err"

"$B/tsdb" "$dir/badvol" create b >/dev/null || fail "create b in badvol"
printf '00000000 %s\n' "$dir/v1" >"$dir/badvol/VOLUMES"
err=$("$B/tsdb" "$dir/badvol" volume diverged "$dir/v1" 2>&1 >/dev/null) && fail "accepted corrupted VOLUMES checksum"
[[ $err == *"damaged"* ]] || fail "corrupted VOLUMES checksum error: $err"
echo "test_cli_diverged: ok"
