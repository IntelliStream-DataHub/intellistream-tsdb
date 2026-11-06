#!/bin/sh
# Prints the text of a GitHub release: docs/releases/TAG.md when there is one, else the section of CHANGELOG.md for
# the tag's version ("## [X.Y.Z] - date" up to the next "## "). The release workflow
# (.github/workflows/release.yml) puts it on the release page; tests/test_release_notes.sh is its test.
#
#   tools/release-notes.sh TAG [REF]
#
# With REF (the workflow passes refs/tags/TAG), the files as they are at REF come first and the working tree's
# second, notes before the changelog: a release's own notes win, and a tag pushed before its notes were written
# (v0.9.0) takes docs/releases/TAG.md from the branch the workflow was started from by hand, while the code is
# always built from the tag. Fails, printing nothing, when neither has text for TAG.
set -eu
top=$(cd "$(dirname "$0")/.." && pwd)
die() { echo "release-notes: $*" >&2; exit 1; }
[ $# -ge 1 ] && [ $# -le 2 ] || die "usage: $0 TAG [REF]"
tag=$1
ref=${2:-}
# The same rule as the workflow's: vMAJOR.MINOR.PATCH with an optional -prerelease. The tag becomes a path below.
case $tag in *"
"*) die "'$tag' is not a release tag (vX.Y.Z)" ;; esac
printf '%s\n' "$tag" | grep -Eqx 'v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?' || die "'$tag' is not a release tag (vX.Y.Z)"
version=${tag#v}
if [ -n "$ref" ]; then
    git -C "$top" rev-parse -q --verify "$ref^{commit}" >/dev/null || die "no commit '$ref' in $top"
fi

# at SOURCE PATH: the file as it is at REF (SOURCE "ref") or in the working tree (SOURCE "tree"); fails if missing
at() {
    if [ "$1" = ref ]; then
        git -C "$top" cat-file -e "$ref:$2" 2>/dev/null || return 1
        git -C "$top" show "$ref:$2"
    else
        [ -f "$top/$2" ] || return 1
        cat "$top/$2"
    fi
}

# The lines after "## [VERSION]" up to the next level-2 heading, without the blank lines around them.
section() {
    awk -v head="## [$version]" '
        f && /^## / { exit }
        f { lines[++n] = $0 }
        !f && substr($0, 1, length(head)) == head { f = 1 }
        END {
            s = 1; while (s <= n && lines[s] ~ /^[ \t]*$/) s++
            e = n; while (e >= s && lines[e] ~ /^[ \t]*$/) e--
            for (i = s; i <= e; i++) print lines[i]
        }'
}

sources=tree
[ -n "$ref" ] && sources="ref tree"
for src in $sources; do
    if text=$(at "$src" "docs/releases/$tag.md"); then
        if printf '%s' "$text" | grep -q '[^[:space:]]'; then
            printf '%s\n' "$text"
            exit 0
        fi
    fi
done
for src in $sources; do
    if log=$(at "$src" CHANGELOG.md); then
        text=$(printf '%s\n' "$log" | section)
        if [ -n "$text" ]; then
            printf '%s\n' "$text"
            exit 0
        fi
    fi
done
die "no docs/releases/$tag.md and no section [$version] in CHANGELOG.md${ref:+ (at $ref or in the working tree)}"
