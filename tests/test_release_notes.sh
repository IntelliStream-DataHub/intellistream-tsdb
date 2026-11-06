#!/bin/bash
# Tests tools/release-notes.sh, which the release workflow uses for the text of a GitHub release: the notes file
# before the changelog, the files at the tag before the working tree's, the changelog section cut exactly, and
# refusals. Most of it runs in a scratch git repository with a copy of the script; make test runs it.
#
#   tests/test_release_notes.sh [scratch-dir]
set -u
top=$(cd "$(dirname "$0")/.." && pwd)
scratch=$(mktemp -d "${1:-${TMPDIR:-/tmp}}/tsdb-release-notes.XXXXXX")
trap 'rm -rf "$scratch"' EXIT
fail=0
ok() { :; }
bad() { echo "FAIL: $*"; fail=1; }
# same NAME EXPECTED-FILE CMD...: CMD succeeds and prints exactly the file
same() {
    local name=$1 want=$2
    shift 2
    if ! "$@" >"$scratch/out" 2>"$scratch/err"; then bad "$name: exit $? ($(cat "$scratch/err"))"; return; fi
    cmp -s "$scratch/out" "$want" || { bad "$name: output differs"; diff "$want" "$scratch/out" | head -n 20; }
}
# refused NAME CMD...: CMD fails and prints nothing on stdout
refused() {
    local name=$1
    shift
    if "$@" >"$scratch/out" 2>"$scratch/err"; then bad "$name: exit 0"; fi
    [ -s "$scratch/out" ] && bad "$name: printed $(head -c 200 "$scratch/out")"
    [ -s "$scratch/err" ] || bad "$name: no message"
}

# The repository's own: v0.9.0 has its notes in docs/releases (written after the tag was pushed).
same "v0.9.0 in this tree" "$top/docs/releases/v0.9.0.md" "$top/tools/release-notes.sh" v0.9.0
if git -C "$top" rev-parse -q --verify "refs/tags/v0.9.0^{commit}" >/dev/null; then
    same "v0.9.0 at its tag" "$top/docs/releases/v0.9.0.md" "$top/tools/release-notes.sh" v0.9.0 refs/tags/v0.9.0
fi

# A scratch repository with a changelog of three versions.
r=$scratch/repo
mkdir -p "$r/tools" "$r/docs/releases"
cp "$top/tools/release-notes.sh" "$r/tools/"
g() { git -C "$r" -c user.name=test -c user.email=test@example.invalid -c commit.gpgsign=false \
    -c tag.gpgsign=false -c core.hooksPath=/dev/null "$@"; }
g init -q -b main
cat >"$r/CHANGELOG.md" <<'EOF'
# Changelog

Text before the first version.

## [Unreleased]

## [1.1.0] - 2026-12-01

### Added
- one thing


## [1a0a0] - not a version: the dots are not a pattern

- decoy

## [1.0.0] - 2026-11-01

Intro of 1.0.0.

### Fixed
- `code` and $(no command) stay as they are
  ## not a heading: indented

## [0.9.0] - 2026-10-01
- the oldest
EOF
printf '### Added\n- one thing\n' >"$scratch/want-1.1.0"
printf 'Intro of 1.0.0.\n\n### Fixed\n- `code` and $(no command) stay as they are\n  ## not a heading: indented\n' \
    >"$scratch/want-1.0.0"
printf -- '- the oldest\n' >"$scratch/want-0.9.0"
g add -A && g commit -q -m one && g tag v1.0.0 && g tag -a -m "tsdb 1.1.0" v1.1.0

n=$r/tools/release-notes.sh
same "changelog section, middle" "$scratch/want-1.1.0" "$n" v1.1.0
same "changelog section, with an indented ##" "$scratch/want-1.0.0" "$n" v1.0.0
same "changelog section, last" "$scratch/want-0.9.0" "$n" v0.9.0
same "changelog section at an annotated tag" "$scratch/want-1.1.0" "$n" v1.1.0 refs/tags/v1.1.0
refused "an empty section" "$n" v9.9.9
refused "no section for the version" "$n" v1.0.1
refused "1a0a0 is not 1.0.0's section" "$n" v1.0.00
for t in 1.0.0 v1.0 v1.0.0- "v1.0.0;id" "v1.0.0 x" "v1.0.0-../../x" "v1.0.0-a..b" "-v1.0.0" "" \
    "$(printf 'v1.0.0\nv1.0.0')" "$(printf 'x\nv1.0.0')"; do
    refused "bad tag '$t'" "$n" "$t"
done
refused "no such ref" "$n" v1.0.0 refs/tags/v7.7.7
refused "no tag" "$n"
refused "three arguments" "$n" v1.0.0 HEAD extra

# Notes written after the tag (v0.9.0's case): in the working tree only, found with the tag as REF.
printf 'Notes of 1.0.0 from the branch.\n' >"$r/docs/releases/v1.0.0.md"
same "notes in the tree, not at the tag" "$r/docs/releases/v1.0.0.md" "$n" v1.0.0 refs/tags/v1.0.0
same "notes in the tree, no REF" "$r/docs/releases/v1.0.0.md" "$n" v1.0.0
# A notes file of blanks does not count.
printf '\n  \n' >"$r/docs/releases/v1.1.0.md"
same "blank notes fall back to the changelog" "$scratch/want-1.1.0" "$n" v1.1.0

# Notes committed at a tag win over the working tree's; so does the tag's changelog.
printf 'Notes of 1.2.0 at the tag.\n' >"$r/docs/releases/v1.2.0.md"
printf '## [1.2.0] - 2027-01-01\n- at the tag\n' >"$r/CHANGELOG.md"
g add -A && g commit -q -m two && g tag v1.2.0
cp "$r/docs/releases/v1.2.0.md" "$scratch/want-notes-1.2.0"
printf 'Notes of 1.2.0 changed later.\n' >"$r/docs/releases/v1.2.0.md"
same "notes at the tag win" "$scratch/want-notes-1.2.0" "$n" v1.2.0 refs/tags/v1.2.0
same "without REF the tree's notes" "$r/docs/releases/v1.2.0.md" "$n" v1.2.0
rm "$r/docs/releases/v1.2.0.md"
g rm -q --cached docs/releases/v1.2.0.md && g commit -q -m three
printf '## [1.2.0] - 2027-01-01\n- changed later\n' >"$r/CHANGELOG.md"
printf -- '- at the tag\n' >"$scratch/want-cl-1.2.0"
# the notes file is gone from the tree but still at the tag
same "notes at the tag, none in the tree" "$scratch/want-notes-1.2.0" "$n" v1.2.0 refs/tags/v1.2.0
same "the tree's changelog without REF" <(printf -- '- changed later\n') "$n" v1.2.0
g tag v1.2.1 HEAD
printf '## [1.2.1] - 2027-01-02\n- in the tree only\n' >>"$r/CHANGELOG.md"
same "the tree's changelog when the tag's has no section" <(printf -- '- in the tree only\n') "$n" v1.2.1 \
    refs/tags/v1.2.1
printf '## [1.1.0] - 2026-12-01\n- rewritten later\n' >"$r/CHANGELOG.md"
same "the tag's changelog wins over the tree's" "$scratch/want-1.1.0" "$n" v1.1.0 refs/tags/v1.1.0
same "the tree's changelog without REF (1.1.0)" <(printf -- '- rewritten later\n') "$n" v1.1.0

if [ $fail -ne 0 ]; then
    echo "test_release_notes: FAILED"
    exit 1
fi
echo "test_release_notes: all passed"
