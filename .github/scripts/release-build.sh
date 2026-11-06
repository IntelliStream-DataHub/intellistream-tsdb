#!/bin/bash
# Builds and checks the release tarball of TAG from the checkout in the current directory, which must be exactly
# that tag, clean, with VERSION equal to TAG without its "v" and without a -prerelease suffix (VERSION is always
# X.Y.Z: tools/check-version.sh). The release workflow (.github/workflows/release.yml) runs it from its own checkout
# against a checkout of the tag, so a tag older than this script is built the same way; you can run it in a clean
# clone too.
#
#   .github/scripts/release-build.sh TAG OUT-DIR
#   EXPECT_COMMIT=SHA      fail unless the tag is that commit (a tag push: the commit that was pushed)
#   RELEASE_SIGNERS=FILE   an ssh allowed_signers file: fail unless the tag is signed by one of its keys
#   SANDYBRIDGE=0          skip make dist-check-sandybridge (it runs natively, and under qemu-x86_64 if installed)
#   ALMALINUX=0            skip make dist-check-almalinux10 (the suite in AlmaLinux 10, with Podman)
#
# Writes OUT-DIR/tsdb-V-x86_64-linux.tar.gz and OUT-DIR/tsdb-V-x86_64-linux.tar.gz.sha256 (the line `sha256sum -c`
# reads, with the bare file name), V being the tag without its "v" (0.9.0, or 0.9.0-rc.1 for an -rc). The build id in
# BUILDINFO and `--version` is the tag itself (TSDB_VERSION), not `git describe`, which may name another tag on the
# same commit (an -rc).
#
# The tarball's suite runs every CPU level this machine has (a level it lacks is skipped). The highest level that
# ran ("v4" when the CPU has AVX-512) is printed last as "release-build: CPU levels tested: up to vN" and, under
# GitHub Actions, written to the step's outputs (top_level) and its summary: the workflow leaves the release a draft
# when v4 did not run.
set -euo pipefail
die() { echo "release-build: $*" >&2; exit 1; }
[ $# -eq 2 ] || die "usage: $0 TAG OUT-DIR"
tag=$1
out=$2
[[ $tag =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$ ]] || die "'$tag' is not a release tag (vX.Y.Z)"
version=${tag#v}
base=${version%%-*}

head=$(git rev-parse HEAD)
want=$(git rev-parse -q --verify "refs/tags/$tag^{commit}") || die "no tag $tag in this checkout"
[ "$head" = "$want" ] || die "the checkout is $head, the tag $tag is $want"
if [ -n "${EXPECT_COMMIT:-}" ] && [ "$want" != "$EXPECT_COMMIT" ]; then
    die "the tag $tag is $want now, but $EXPECT_COMMIT was pushed: it moved; push it again to release it"
fi
git tag --points-at HEAD | grep -qx -- "$tag" || die "the tag $tag does not point at HEAD"
if [ -n "${RELEASE_SIGNERS:-}" ]; then
    [ -s "$RELEASE_SIGNERS" ] || die "RELEASE_SIGNERS: no file $RELEASE_SIGNERS"
    git -c gpg.ssh.allowedSignersFile="$RELEASE_SIGNERS" verify-tag "$tag" ||
        die "the tag $tag is not signed by a key of the release signers"
elif [ "$(git cat-file -t "refs/tags/$tag")" != tag ]; then
    echo "release-build: warning: $tag is a lightweight tag" >&2
fi
echo "release-build: $tag is $head; git describe says $(git describe --tags --exact-match HEAD 2>/dev/null || echo -)"
[ -z "$(git status --porcelain --untracked-files=no)" ] || die "the checkout has local changes"
v=$(tr -d ' \n' <VERSION)
[ "$v" = "$base" ] || die "VERSION says '$v', the tag $tag says '$base'"

export TSDB_VERSION=$tag
ulimit -n 65536 2>/dev/null || true
# RELEASE_VERSION names the tarball and its top directory (the tag's version, -rc included); the binaries say
# VERSION and the build id.
mk=(make RELEASE_VERSION="$version")
log=$(mktemp)
trap 'rm -f "$log"' EXIT
"${mk[@]}" -j"$(nproc)" dist
"${mk[@]}" dist-test 2>&1 | tee "$log"
if [ "${SANDYBRIDGE:-1}" != 0 ]; then
    "${mk[@]}" dist-check-sandybridge
fi
# last, so the tarball it tests is the one released (each make here packs it again: mkdist.sh)
if [ "${ALMALINUX:-1}" != 0 ]; then
    "${mk[@]}" dist-check-almalinux10
fi

name=tsdb-$version-x86_64-linux.tar.gz
shopt -s nullglob
found=(build-dist/tsdb-*-x86_64-linux.tar.gz)
[ "${#found[@]}" -eq 1 ] && [ "${found[0]}" = "build-dist/$name" ] || die "expected build-dist/$name, found: ${found[*]}"
check=$(mktemp -d)
trap 'rm -rf "$check" "$log"' EXIT
tar -C "$check" -xzf "build-dist/$name"
top=$check/tsdb-$version-x86_64-linux
[ -d "$top" ] || die "the tarball has no top directory tsdb-$version-x86_64-linux"
grep -qx "version  $version" "$top/BUILDINFO" || die "BUILDINFO does not name the version $version: $(cat "$top/BUILDINFO")"
grep -qx "build    $tag" "$top/BUILDINFO" || die "BUILDINFO does not name the build $tag: $(cat "$top/BUILDINFO")"
for b in tsdbd tsdb; do
    got=$("$top/bin/$b" --version)
    [ "$got" = "$b $base (build $tag)" ] || die "$b --version says '$got', not '$b $base (build $tag)'"
done
cat "$top/BUILDINFO"

mkdir -p "$out"
cp "build-dist/$name" "$out/"
(cd "$out" && sha256sum "$name" >"$name.sha256" && sha256sum -c "$name.sha256" && cat "$name.sha256")

# test_levels' line from the suite above: "dispatch: this CPU has vN, the build uses up to vM"
line=$(grep -m1 '^dispatch: this CPU has ' "$log" || true)
[[ $line =~ ^dispatch:\ this\ CPU\ has\ (v[234]|base)\, ]] || die "no dispatch line from test_levels in make dist-test"
level=${BASH_REMATCH[1]}
echo "release-build: CPU levels tested: up to $level ($line; $(grep -m1 'model name' /proc/cpuinfo | sed 's/.*: //'))"
if [ -n "${GITHUB_OUTPUT:-}" ]; then
    echo "top_level=$level" >>"$GITHUB_OUTPUT"
fi
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    {
        echo "### $name"
        echo
        echo "- CPU: $(grep -m1 'model name' /proc/cpuinfo | sed 's/.*: //')"
        echo "- $line"
        [ "$level" = v4 ] || echo "- **The v4 (AVX-512) code was not run here**: the release stays a draft (docs/release.md, step 6)"
        echo "- sha256 \`$(cut -d' ' -f1 "$out/$name.sha256")\`"
    } >>"$GITHUB_STEP_SUMMARY"
fi
