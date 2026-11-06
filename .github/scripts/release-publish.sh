#!/bin/bash
# Puts the release of TAG on GitHub with its assets, through the REST API with curl and the job's GITHUB_TOKEN
# (no gh). The release workflow (.github/workflows/release.yml) runs it; tests/test_release_publish.py runs it
# against a local mock of the API.
#
#   .github/scripts/release-publish.sh TAG NOTES-FILE ASSET...
#   env: GITHUB_TOKEN, GITHUB_REPOSITORY (owner/name), GITHUB_API_URL (default https://api.github.com),
#        RELEASE_UPLOADS_URL (default https://uploads.github.com)
#
# 1. Finds the release: the published one of the tag (GET releases/tags/TAG, which never returns a draft), else a
#    draft whose tag_name is TAG (one an earlier run left when it failed), else creates a draft named
#    "tsdb X.Y.Z" with NOTES-FILE as its text, a prerelease for a tag with a -suffix (v1.0.0-rc.1).
# 2. Deletes the release's assets that have the name of one of ours (a rerun), then uploads each
#    (application/gzip for .tar.gz, text/plain otherwise) and checks the size, and the sha256 where the API reports
#    it, of what GitHub stored.
# 3. Publishes a draft only then, so that nobody sees the release without its tarball (and a repository with
#    immutable releases, which refuse new assets once a release is published, works): make_latest "false" for a
#    prerelease, else "legacy" (GitHub picks the latest by date and version, so a patch of an older line does not
#    become "latest").
# A release that was already published keeps its text, name, prerelease and latest flags; only the assets change.
set -euo pipefail
die() { echo "release-publish: $*" >&2; exit 1; }
[ $# -ge 3 ] || die "usage: $0 TAG NOTES-FILE ASSET..."
tag=$1
notes=$2
shift 2
[[ $tag =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$ ]] || die "'$tag' is not a release tag (vX.Y.Z)"
version=${tag#v}
pre=false
[[ $tag == *-* ]] && pre=true
repo=${GITHUB_REPOSITORY:-}
[[ $repo =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || die "GITHUB_REPOSITORY '$repo' is not owner/name"
[ -n "${GITHUB_TOKEN:-}" ] || die "no GITHUB_TOKEN"
api=${GITHUB_API_URL:-https://api.github.com}
uploads=${RELEASE_UPLOADS_URL:-https://uploads.github.com}
[ -f "$notes" ] || die "no notes file $notes"
# GitHub refuses a release text over 125,000 characters
[ "$(wc -m <"$notes")" -le 125000 ] || die "$notes is over 125000 characters"
for f in "$@"; do
    [ -f "$f" ] || die "no asset $f"
    [[ $(basename "$f") =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || die "asset name '$(basename "$f")' needs no URL escaping"
done
command -v jq >/dev/null || die "needs jq"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
# The token goes to curl in a config file, never in its argv (which `ps` shows).
(umask 077 && printf 'header = "Authorization: Bearer %s"\n' "$GITHUB_TOKEN" >"$tmp/auth")

# call METHOD URL [curl options]: the reply's body in $tmp/body, its status in $code. GET and DELETE are retried on
# a network error or a 5xx; a POST is not (it would make a second draft or a duplicate asset).
call() {
    local m=$1 u=$2
    shift 2
    local retry=()
    if [ "$m" = GET ] || [ "$m" = DELETE ]; then retry=(--retry 3 --retry-delay 2); fi
    code=$(curl -sS -o "$tmp/body" -w '%{http_code}' -X "$m" -K "$tmp/auth" \
        -H 'Accept: application/vnd.github+json' -H 'X-GitHub-Api-Version: 2022-11-28' \
        --connect-timeout 20 --max-time 900 "${retry[@]}" "$@" "$u") || die "$m $u: curl failed"
}
# expect STATUS WHAT: dies with the API's message unless the last call answered STATUS
expect() {
    [ "$code" = "$1" ] && return 0
    die "$2: HTTP $code: $(jq -r '.message // empty' "$tmp/body" 2>/dev/null || head -c 300 "$tmp/body")"
}

base=$api/repos/$repo
call GET "$base/releases/tags/$tag"
if [ "$code" = 200 ]; then
    cp "$tmp/body" "$tmp/release"
    echo "release-publish: $tag has a published release; replacing its assets"
elif [ "$code" = 404 ]; then
    call GET "$base/releases?per_page=100"
    expect 200 "listing the releases"
    jq --arg t "$tag" '[.[] | select(.draft and .tag_name == $t)]' "$tmp/body" >"$tmp/drafts"
    if [ "$(jq length "$tmp/drafts")" -gt 0 ]; then
        jq '.[0]' "$tmp/drafts" >"$tmp/release"
        echo "release-publish: $tag has a draft release (id $(jq .id "$tmp/release")); completing it"
    else
        jq -n --arg t "$tag" --arg n "tsdb $version" --rawfile b "$notes" --argjson p "$pre" \
            '{tag_name: $t, name: $n, body: $b, draft: true, prerelease: $p}' >"$tmp/new"
        call POST "$base/releases" -H 'Content-Type: application/json' --data-binary "@$tmp/new"
        expect 201 "creating the release of $tag"
        cp "$tmp/body" "$tmp/release"
        echo "release-publish: created a draft release of $tag (id $(jq .id "$tmp/release"))"
    fi
else
    expect 200 "looking up the release of $tag"
fi

id=$(jq -r .id "$tmp/release")
[[ $id =~ ^[0-9]+$ ]] || die "the release has no numeric id: '$id'"
draft=$(jq -r .draft "$tmp/release")
[ "$(jq -r .tag_name "$tmp/release")" = "$tag" ] || die "release $id is not of $tag"
# The upload URL comes from the reply; the token goes nowhere but where it should.
upload=$(jq -r '.upload_url // empty' "$tmp/release" | sed 's/{.*$//')
[ "$upload" = "$uploads/repos/$repo/releases/$id/assets" ] || die "unexpected upload_url '$upload'"

call GET "$base/releases/$id/assets?per_page=100"
expect 200 "listing the assets of release $id"
cp "$tmp/body" "$tmp/assets"
for f in "$@"; do
    name=$(basename "$f")
    for aid in $(jq -r --arg n "$name" '.[] | select(.name == $n) | .id' "$tmp/assets"); do
        [[ $aid =~ ^[0-9]+$ ]] || die "asset id '$aid'"
        call DELETE "$base/releases/assets/$aid"
        expect 204 "deleting the old $name (asset $aid)"
        echo "release-publish: deleted the old $name"
    done
done

for f in "$@"; do
    name=$(basename "$f")
    case $name in *.tar.gz) ctype=application/gzip ;; *) ctype=text/plain ;; esac
    size=$(stat -c %s "$f")
    sum=$(sha256sum "$f" | cut -d' ' -f1)
    call POST "$upload?name=$name" -H "Content-Type: $ctype" --data-binary "@$f"
    expect 201 "uploading $name"
    got=$(jq -r .size "$tmp/body")
    [ "$got" = "$size" ] || die "$name: GitHub stored $got bytes, the file has $size"
    digest=$(jq -r '.digest // empty' "$tmp/body")
    [ -z "$digest" ] || [ "$digest" = "sha256:$sum" ] || die "$name: GitHub stored $digest, the file is sha256:$sum"
    echo "release-publish: uploaded $name ($size bytes, sha256 $sum)"
done

if [ "$draft" = true ]; then
    latest=legacy
    [ "$pre" = true ] && latest=false
    jq -n --argjson p "$pre" --arg l "$latest" '{draft: false, prerelease: $p, make_latest: $l}' >"$tmp/publish"
    call PATCH "$base/releases/$id" -H 'Content-Type: application/json' --data-binary "@$tmp/publish"
    expect 200 "publishing release $id"
    [ "$(jq -r .draft "$tmp/body")" = false ] || die "release $id is still a draft"
    cp "$tmp/body" "$tmp/release"
    echo "release-publish: published (prerelease $pre, make_latest $latest)"
fi
echo "release-publish: $(jq -r '.html_url // empty' "$tmp/release")"
