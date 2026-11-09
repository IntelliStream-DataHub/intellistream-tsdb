#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# Runs a `make dist` tarball's test suite inside a stock AlmaLinux 10 image
# with Podman (never Docker): nothing is installed in the image, so only its
# glibc is used, as on a production host.
#
#   tests/distro/dist-check.sh build-dist/tsdb-*-x86_64-linux.tar.gz
#   IMAGE=docker.io/library/almalinux:9 ...   another image (EL9: glibc 2.34)
#   URING=0                                    keep Podman's stock seccomp profile
#
# No network, no published ports; the tarball is mounted read-only and
# unpacked in the container's /tmp. Podman's stock seccomp profile answers
# io_uring_setup with ENOSYS, so by default the profile gets the three
# io_uring syscalls added (that is the container, not tsdb or AlmaLinux).
set -u
tarball=${1:?usage: $0 tsdb-VERSION-x86_64-linux.tar.gz}
tarball=$(cd "$(dirname "$tarball")" && pwd)/$(basename "$tarball")
# AlmaLinux 10.2 as of 2026-10-24, the digest tests/distro/Containerfile.almalinux10 pins
image=${IMAGE:-docker.io/library/almalinux@sha256:ba31c3299856068f77bc10574cad51b0a4f6a3dafb882668656e1639bee63db6}
tmp=$(mktemp -d "${TMPDIR:-/tmp}/tsdb-dist-check.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

seccomp=()
if [ "${URING:-1}" != 0 ]; then
    python3 - "$tmp/seccomp.json" <<'PY' || { echo "cannot write the seccomp profile"; exit 2; }
import json, os, sys
src = next(p for p in ("/etc/containers/seccomp.json", "/usr/share/containers/seccomp.json") if os.path.exists(p))
prof = json.load(open(src))
prof["syscalls"].append({"names": ["io_uring_setup", "io_uring_enter", "io_uring_register"], "action": "SCMP_ACT_ALLOW"})
json.dump(prof, open(sys.argv[1], "w"))
PY
    seccomp=(--security-opt "seccomp=$tmp/seccomp.json")
fi

inner='
set -u
cat /etc/system-release; echo "glibc $(ldd --version | head -n 1)"
mkdir -p /tmp/dist /tmp/scratch && tar -C /tmp/dist -xzf /dist.tar.gz && cd /tmp/dist/tsdb-*-x86_64-linux || exit 2
for b in bin/* bench/* tests/test_*; do
    if ldd "$b" | grep -q "not found"; then echo "$b: missing libraries"; ldd "$b"; exit 1; fi
done
for f in LICENSE NOTICE LICENSING.md LICENSES/Apache-2.0.txt THIRD-PARTY-NOTICES; do
    [ -s "$f" ] || { echo "$f missing from the tarball"; exit 1; }
done
cat BUILDINFO
bin/tsdb 2>&1 | head -n 1
tests/run-tests.sh /tmp/scratch
'
podman run --rm --network none --security-opt label=disable "${seccomp[@]}" \
    -v "$tarball":/dist.tar.gz:ro --tmpfs /tmp:exec,size=8g \
    "$image" bash -c "$inner"
