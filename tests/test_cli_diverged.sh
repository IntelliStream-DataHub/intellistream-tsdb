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
err=$("$B/tsdb" "$dir/db" volume diverged "$dir/cv" --clear 2>&1 >/dev/null) && fail "--clear on a cluster volume"
[[ $err == *R4* && $err == *refused* ]] || fail "the refusal does not name R4: $err"
echo "test_cli_diverged: ok"
