#!/bin/bash
# Build and test tsdb on AlmaLinux 10 inside Podman (never Docker).
#
#   tests/distro/almalinux10.sh [step...]     steps: build portable test asan  (default: all)
#       portable = PORTABLE=1 ARCH=-march=x86-64-v3, then the tests with the AVX2 kernels forced
#       (TSDB_SIMD=avx2): the code an x86-64-v3 package runs on a production CPU.
#       test and asan run at -march=native / the host CPU's best kernels.
#   LOGDIR=dir                                 where per-step logs go (default: a temp dir, printed)
#   URING=1                                    allow io_uring in the container (default: blocked, as in production)
#
# The source tree is mounted read-only and copied to a scratch directory inside
# the container, so the host's build/ is never touched and `make asan` (which
# starts with `make clean`) cannot hurt a running tsdbd. No ports are published
# and the network is off. The container shares the host kernel. Podman's stock seccomp
# profile answers io_uring_setup with ENOSYS, much as production AlmaLinux 10 refuses it
# (kernel.io_uring_disabled=2, EPERM), so by default tsdbd falls back to its epoll
# backend here exactly as it must in production. URING=1 adds the three io_uring
# syscalls to the profile to test the io_uring backend too.
# Not covered: make jepsen / tsan / test-clients, EL10's kernel (6.12; containers share the
# host's), SELinux enforcing, XFS (the scratch directory is tmpfs).
set -u
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
image=localhost/tsdb-almalinux10
steps=("$@")
[ ${#steps[@]} -eq 0 ] && steps=(build portable test asan)
logdir=${LOGDIR:-$(mktemp -d "${TMPDIR:-/tmp}/tsdb-alma10.XXXXXX")}
mkdir -p "$logdir"
echo "logs: $logdir"
[ "${URING:-0}" != 0 ] && echo "NOTE: io_uring is allowed in the container (URING=1); production AlmaLinux 10 disables it."

# The backends the server tests run against: with io_uring blocked, `auto` must fall
# back to epoll on its own, as tsdbd has to in production.
if [ "${URING:-0}" != 0 ]; then io="uring epoll"; else io="auto epoll"; fi
seccomp=()
if [ "${URING:-0}" != 0 ]; then
    prof="$logdir/seccomp-uring.json"
    python3 - "$prof" <<'PY' || { echo "cannot write the seccomp profile"; exit 2; }
import json, os, sys
src = next(p for p in ("/etc/containers/seccomp.json", "/usr/share/containers/seccomp.json") if os.path.exists(p))
prof = json.load(open(src))
prof["syscalls"].append({"names": ["io_uring_setup", "io_uring_enter", "io_uring_register"], "action": "SCMP_ACT_ALLOW"})
json.dump(prof, open(sys.argv[1], "w"))
PY
    seccomp=(--security-opt "seccomp=$prof")
fi

podman build -q -t "$image" -f "$here/Containerfile.almalinux10" "$here" >/dev/null || { echo "image build failed"; exit 2; }

# Runs inside the container. $1 = step list. Each step starts from a fresh copy.
inner='
set -u
# Copy only sources: not build output (client/*/target is hundreds of MB), .git or core files.
fresh() {
    rm -rf /work && mkdir -p /work \
    && tar -C /src --exclude=./.git --exclude=./build --exclude=./build-tsan --exclude="./client/*/target" \
           --exclude="*.core" -cf - . | tar -C /work -xf - && cd /work
}
rc=0
for s in $1; do
    fresh
    echo "=== step $s: $(gcc --version | head -1)"
    echo "=== packages: $(tr "\n" " " < /etc/tsdb-packages.txt)"
    case $s in
    build)    make -j"$(nproc)" 2>&1 ;;
    portable) make -j"$(nproc)" PORTABLE=1 ARCH=-march=x86-64-v3 2>&1 \
              && mkdir -p /tmp/t && TSDB_SIMD=avx2 make PORTABLE=1 ARCH=-march=x86-64-v3 test 2>&1 ;;
    test)     make -j"$(nproc)" 2>&1 && mkdir -p /tmp/t && make test 2>&1 ;;
    asan)     make asan 2>&1 ;;
    *)        echo "unknown step $s"; false ;;
    esac
    r=$?
    echo "=== step $s exit $r"
    [ $r -ne 0 ] && rc=1
done
exit $rc
'

rc=0
rm -f "$logdir/warnings.txt"
for s in "${steps[@]}"; do
    # --network none: no exposure. --security-opt label=disable: SELinux on the host must not
    # block the bind mount; the container itself has no host data except the read-only source.
    podman run --rm --network none --security-opt label=disable "${seccomp[@]}" -e IO="$io" \
        -v "$root":/src:ro --tmpfs /tmp:exec,size=8g \
        "$image" bash -c "$inner" _ "$s" >"$logdir/$s.log" 2>&1
    r=$?
    # Belt and braces next to UBSAN_OPTIONS=halt_on_error in `make asan`: a UBSan report that
    # did not stop a run still fails the step.
    if [ $r -eq 0 ] && grep -q "runtime error:" "$logdir/$s.log"; then
        echo "$s: UBSan reported 'runtime error' in a run that exited 0"; r=1
    fi
    echo "$s: exit $r  ($logdir/$s.log)"
    [ $r -ne 0 ] && rc=1
done
# Only the logs of the steps of this run: a reused LOGDIR may hold older ones.
for s in "${steps[@]}"; do cat "$logdir/$s.log"; done | grep -h -E "warning:" | sort | uniq -c | sort -rn > "$logdir/warnings.txt"
echo "warnings: $(wc -l < "$logdir/warnings.txt") distinct, see $logdir/warnings.txt"
exit $rc
