#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# Puts the release of TAG on GitHub with its assets, through the REST API with curl and the job's GITHUB_TOKEN
# (no gh). The release workflow (.github/workflows/release.yml) runs it; tests/test_release_publish.py runs it
# against a local mock of the API.
#
#   .github/scripts/release-publish.sh TAG NOTES-FILE ASSET...
#   env: GITHUB_TOKEN, GITHUB_REPOSITORY (owner/name), GITHUB_API_URL (default https://api.github.com),
#        RELEASE_UPLOADS_URL (default https://uploads.github.com),
#        RELEASE_COMMIT (the commit that was built: the draft's target_commitish, so that a draft published after
#        its tag was deleted gets the tag on that commit, not on the default branch),
#        REPLACE_ASSETS=1 (a published release that already has a file of ours gets the new one; else refused),
#        KEEP_DRAFT=1 (upload, but leave a draft unpublished: someone checks the files first and publishes it)
#
# 1. Finds the release: the published one of the tag (GET releases/tags/TAG, which never returns a draft), else a
#    draft whose tag_name is TAG (one an earlier run left when it failed), else creates a draft named
#    "tsdb X.Y.Z" with NOTES-FILE as its text, a prerelease for a tag with a -suffix (v1.0.0-rc.1).
#    A release found keeps its text; a warning says so when it is not NOTES-FILE.
# 2. Uploads each file (application/gzip for .tar.gz, text/plain otherwise) and checks the size, and the sha256 where
#    the API reports it, of what GitHub stored. A file whose name the release already has (a rerun) is uploaded
#    first as NAME.new, and only once every file is stored is the old one deleted and NAME.new renamed: a failure
#    leaves the old file in place, never a release without its tarball. A published release's files are replaced
#    only with REPLACE_ASSETS=1: anyone who downloaded them holds the old checksum.
# 3. Publishes a draft only then, so that nobody sees the release without its tarball (and a repository with
#    immutable releases, which refuse new assets once a release is published, works): make_latest "false" for a
#    prerelease, else "legacy" (GitHub picks the latest by date and version, so a patch of an older line does not
#    become "latest"). With KEEP_DRAFT=1 it stays a draft.
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
commit=${RELEASE_COMMIT:-}
[ -z "$commit" ] || [[ $commit =~ ^[0-9a-f]{40}$ ]] || die "RELEASE_COMMIT '$commit' is not a commit id"
replace=${REPLACE_ASSETS:-0}
keep_draft=${KEEP_DRAFT:-0}
api=${GITHUB_API_URL:-https://api.github.com}
uploads=${RELEASE_UPLOADS_URL:-https://uploads.github.com}
[ -f "$notes" ] || die "no notes file $notes"
# GitHub refuses a release text over 125,000 characters
[ "$(wc -m <"$notes")" -le 125000 ] || die "$notes is over 125000 characters"
for f in "$@"; do
    [ -f "$f" ] || die "no asset $f"
    [[ $(basename "$f") =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || die "asset name '$(basename "$f")' needs no URL escaping"
    [[ $(basename "$f") != *.new ]] || die "asset name '$(basename "$f")' ends in .new, the name of an upload in flight"
done
command -v jq >/dev/null || die "needs jq"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
# The token goes to curl in a config file, never in its argv (which `ps` shows).
(umask 077 && printf 'header = "Authorization: Bearer %s"\n' "$GITHUB_TOKEN" >"$tmp/auth")

# call METHOD URL [curl options]: the reply's body in $tmp/body, its status in $code. GET, PATCH and DELETE are
# retried on a network error or a 5xx; a POST is not (it would make a second draft or a duplicate asset).
call() {
    local m=$1 u=$2
    shift 2
    local retry=()
    if [ "$m" != POST ]; then retry=(--retry 3 --retry-delay 2); fi
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
    echo "release-publish: $tag has a published release (id $(jq .id "$tmp/release")); adding the files"
elif [ "$code" = 404 ]; then
    call GET "$base/releases?per_page=100"
    expect 200 "listing the releases"
    jq --arg t "$tag" '[.[] | select(.draft and .tag_name == $t)]' "$tmp/body" >"$tmp/drafts"
    if [ "$(jq length "$tmp/drafts")" -gt 0 ]; then
        jq '.[0]' "$tmp/drafts" >"$tmp/release"
        echo "release-publish: $tag has a draft release (id $(jq .id "$tmp/release")); completing it"
    else
        jq -n --arg t "$tag" --arg n "tsdb $version" --rawfile b "$notes" --argjson p "$pre" --arg c "$commit" \
            '{tag_name: $t, name: $n, body: $b, draft: true, prerelease: $p}
             + (if $c == "" then {} else {target_commitish: $c} end)' >"$tmp/new"
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
# A release found keeps its text (someone may have edited it on the page); say so when it is not ours.
jq -j '.body // ""' "$tmp/release" >"$tmp/oldbody"
if ! cmp -s "$tmp/oldbody" "$notes"; then
    echo "::warning::the release of $tag keeps its text, which is not $(basename "$notes") of this run: check it" \
        "on the release page (a checksum it quotes is not that of the files attached now)"
fi

call GET "$base/releases/$id/assets?per_page=100"
expect 200 "listing the assets of release $id"
cp "$tmp/body" "$tmp/assets"
jq -e 'all(.[]; .id | type == "number")' "$tmp/assets" >/dev/null || die "the assets of release $id have odd ids"
# asset_ids NAME: the ids of the release's assets called NAME (as they were before this run uploaded anything)
asset_ids() {
    jq -r --arg n "$1" '.[] | select(.name == $n) | .id' "$tmp/assets"
}
# delete_asset ID WHAT
delete_asset() {
    call DELETE "$base/releases/assets/$1"
    expect 204 "deleting $2 (asset $1)"
    echo "release-publish: deleted $2"
}

if [ "$draft" != true ] && [ "$replace" != 1 ]; then
    for f in "$@"; do
        name=$(basename "$f")
        if [ -n "$(asset_ids "$name")" ]; then
            die "the published release of $tag already has $name; its files are replaced only on request" \
                "(Run workflow with 'replace' ticked; REPLACE_ASSETS=1): whoever downloaded it holds its checksum"
        fi
    done
fi
# NAME.new of a run that failed halfway: never published under its name, so it goes first
for f in "$@"; do
    for aid in $(asset_ids "$(basename "$f").new"); do
        delete_asset "$aid" "the unfinished $(basename "$f").new"
    done
done

# Every file is stored before any old one goes: a name the release has gets NAME.new for now.
renames=()
for f in "$@"; do
    name=$(basename "$f")
    as=$name
    [ -z "$(asset_ids "$name")" ] || as=$name.new
    case $name in *.tar.gz) ctype=application/gzip ;; *) ctype=text/plain ;; esac
    size=$(stat -c %s "$f")
    sum=$(sha256sum "$f" | cut -d' ' -f1)
    call POST "$upload?name=$as" -H "Content-Type: $ctype" --data-binary "@$f"
    expect 201 "uploading $as"
    got=$(jq -r .size "$tmp/body")
    [ "$got" = "$size" ] || die "$as: GitHub stored $got bytes, the file has $size"
    digest=$(jq -r '.digest // empty' "$tmp/body")
    [ -z "$digest" ] || [ "$digest" = "sha256:$sum" ] || die "$as: GitHub stored $digest, the file is sha256:$sum"
    [ "$(jq -r .name "$tmp/body")" = "$as" ] || die "GitHub stored $as as '$(jq -r .name "$tmp/body")'"
    nid=$(jq -r .id "$tmp/body")
    [[ $nid =~ ^[0-9]+$ ]] || die "asset id '$nid'"
    echo "release-publish: uploaded $as ($size bytes, sha256 $sum)"
    [ "$as" = "$name" ] || renames+=("$nid:$name")
done
# Then each old file is swapped for its new one: delete, rename (a rerun finds NAME.new if the rename fails).
for r in "${renames[@]}"; do
    nid=${r%%:*}
    name=${r#*:}
    for aid in $(asset_ids "$name"); do
        delete_asset "$aid" "the old $name"
    done
    jq -n --arg n "$name" '{name: $n}' >"$tmp/rename"
    call PATCH "$base/releases/assets/$nid" -H 'Content-Type: application/json' --data-binary "@$tmp/rename"
    expect 200 "renaming $name.new (asset $nid) to $name"
    [ "$(jq -r .name "$tmp/body")" = "$name" ] || die "asset $nid is called '$(jq -r .name "$tmp/body")', not $name"
    echo "release-publish: replaced $name"
done

if [ "$draft" = true ] && [ "$keep_draft" = 1 ]; then
    echo "::warning::the release of $tag stays a draft with its files (KEEP_DRAFT): check them, then publish it" \
        "on the release page"
elif [ "$draft" = true ]; then
    latest=legacy
    [ "$pre" = true ] && latest=false
    jq -n --argjson p "$pre" --arg l "$latest" '{draft: false, prerelease: $p, make_latest: $l}' >"$tmp/publish"
    call PATCH "$base/releases/$id" -H 'Content-Type: application/json' --data-binary "@$tmp/publish"
    expect 200 "publishing release $id"
    [ "$(jq -r .draft "$tmp/body")" = false ] || die "release $id is still a draft"
    cp "$tmp/body" "$tmp/release"
    echo "release-publish: published (prerelease $pre, make_latest $latest)"
elif [ "$keep_draft" = 1 ]; then
    echo "::warning::the release of $tag was already published, so its new files are public unchecked (KEEP_DRAFT)"
fi
echo "release-publish: $(jq -r '.html_url // empty' "$tmp/release")"
