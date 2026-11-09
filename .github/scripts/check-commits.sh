#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# The commit rules of a pull request, for every commit in BASE..HEAD (merge commits are skipped):
#   - the message passes .githooks/commit-msg (no link to a Claude session; ends with a Signed-off-by trailer);
#   - that last Signed-off-by names the commit's author (the e-mail address must match): the DCO says the author
#     wrote the change or may submit it. `git commit -s` does exactly this.
# With PR_TEXT set (the pull request's title and description), it is checked for a session link as well.
#
#   .github/scripts/check-commits.sh BASE HEAD       (e.g. origin/main HEAD; needs the full history)
set -u
[ $# -eq 2 ] || { echo "usage: $0 BASE HEAD" >&2; exit 2; }
base=$1
head=$2
cd "$(dirname "$0")/../.." || exit 2
hook=.githooks/commit-msg
tmp=$(mktemp -d) || exit 2
trap 'rm -rf "$tmp"' EXIT

fail=0
n=0
commits=$(git rev-list --no-merges "$base..$head") || { echo "check-commits: cannot list $base..$head" >&2; exit 2; }
for c in $commits; do
    n=$((n + 1))
    short=$(git log -1 --format='%h %s' "$c")
    git log -1 --format=%B "$c" >"$tmp/msg"
    author=$(git log -1 --format='%an <%ae>' "$c")
    email=$(git log -1 --format=%ae "$c")
    if ! sh "$hook" "$tmp/msg" 2>"$tmp/err"; then
        echo "FAIL $short"
        sed 's/^/     /' "$tmp/err"
        fail=1
        continue
    fi
    # the hook ensured the last non-empty line is a Signed-off-by trailer; it must be the author's
    last=$(sed '/^[[:space:]]*$/d' "$tmp/msg" | tail -n 1)
    signed=$(printf '%s\n' "$last" | sed -n 's/^Signed-off-by:.*<\([^>]*\)>[[:space:]]*$/\1/p')
    lower_signed=$(printf '%s' "$signed" | tr 'A-Z' 'a-z')
    lower_email=$(printf '%s' "$email" | tr 'A-Z' 'a-z')
    if [ -z "$signed" ] || [ "$lower_signed" != "$lower_email" ]; then
        echo "FAIL $short"
        echo "     the last line is '$last', but the author is $author"
        echo "     sign off as the author: git commit --amend -s (or: git rebase --signoff $base)"
        fail=1
        continue
    fi
    echo "ok   $short"
done

if [ -n "${PR_TEXT:-}" ] && printf '%s' "$PR_TEXT" | grep -Eqi 'claude\.ai/code|^Claude-Session:'; then
    echo "FAIL the pull request title or description links to a Claude session; remove the link"
    fail=1
fi

echo "check-commits: $n commit(s) in $base..$head"
if [ $fail -ne 0 ]; then
    echo "check-commits: FAILED. See CONTRIBUTING.md, \"Sign-off (DCO)\"."
    exit 1
fi
exit 0
