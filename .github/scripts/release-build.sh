#!/bin/bash
# Builds and checks the release tarball of TAG from the checkout in the current directory, which must be exactly
# that tag, clean, with VERSION equal to TAG without its "v". The release workflow (.github/workflows/release.yml)
# runs it from its own checkout against a checkout of the tag, so a tag older than this script is built the same
# way; you can run it in a clean clone too.
#
#   .github/scripts/release-build.sh TAG OUT-DIR
#   EXPECT_COMMIT=SHA      fail unless the tag is that commit (a tag push: the commit that was pushed)
#   SANDYBRIDGE=0          skip make dist-check-sandybridge (it runs natively, and under qemu-x86_64 if installed)
#
# Writes OUT-DIR/tsdb-VERSION-x86_64-linux.tar.gz and OUT-DIR/tsdb-VERSION-x86_64-linux.tar.gz.sha256 (the line
# `sha256sum -c` reads, with the bare file name). The build id in BUILDINFO and `--version` is the tag itself
# (TSDB_VERSION), not `git describe`, which may name another tag on the same commit (an -rc).
set -euo pipefail
die() { echo "release-build: $*" >&2; exit 1; }
[ $# -eq 2 ] || die "usage: $0 TAG OUT-DIR"
tag=$1
out=$2
[[ $tag =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$ ]] || die "'$tag' is not a release tag (vX.Y.Z)"
version=${tag#v}

head=$(git rev-parse HEAD)
want=$(git rev-parse -q --verify "refs/tags/$tag^{commit}") || die "no tag $tag in this checkout"
[ "$head" = "$want" ] || die "the checkout is $head, the tag $tag is $want"
if [ -n "${EXPECT_COMMIT:-}" ] && [ "$want" != "$EXPECT_COMMIT" ]; then
    die "the tag $tag is $want now, but $EXPECT_COMMIT was pushed: it moved; push it again to release it"
fi
git tag --points-at HEAD | grep -qx -- "$tag" || die "the tag $tag does not point at HEAD"
[ "$(git cat-file -t "refs/tags/$tag")" = tag ] || echo "release-build: warning: $tag is a lightweight tag" >&2
echo "release-build: $tag is $head; git describe says $(git describe --tags --exact-match HEAD 2>/dev/null || echo -)"
[ -z "$(git status --porcelain --untracked-files=no)" ] || die "the checkout has local changes"
v=$(tr -d ' \n' <VERSION)
[ "$v" = "$version" ] || die "VERSION says '$v', the tag $tag says '$version'"

export TSDB_VERSION=$tag
ulimit -n 65536 2>/dev/null || true
make -j"$(nproc)" dist
make dist-test
if [ "${SANDYBRIDGE:-1}" != 0 ]; then
    make dist-check-sandybridge
fi

# Each make above packs the tarball again (mkdist.sh); the binaries are the ones the tests ran.
name=tsdb-$version-x86_64-linux.tar.gz
shopt -s nullglob
found=(build-dist/tsdb-*-x86_64-linux.tar.gz)
[ "${#found[@]}" -eq 1 ] && [ "${found[0]}" = "build-dist/$name" ] || die "expected build-dist/$name, found: ${found[*]}"
check=$(mktemp -d)
trap 'rm -rf "$check"' EXIT
tar -C "$check" -xzf "build-dist/$name"
top=$check/tsdb-$version-x86_64-linux
grep -qx "build    $tag" "$top/BUILDINFO" || die "BUILDINFO does not name the build $tag: $(cat "$top/BUILDINFO")"
for b in tsdbd tsdb; do
    got=$("$top/bin/$b" --version)
    [ "$got" = "$b $version (build $tag)" ] || die "$b --version says '$got', not '$b $version (build $tag)'"
done
cat "$top/BUILDINFO"

mkdir -p "$out"
cp "build-dist/$name" "$out/"
(cd "$out" && sha256sum "$name" >"$name.sha256" && sha256sum -c "$name.sha256" && cat "$name.sha256")
