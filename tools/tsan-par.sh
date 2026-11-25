#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# `make tsan-par`: the ThreadSanitizer run with its test groups side by side instead of one after the other.
#
# `make tsan` runs `make test` in build-tsan/: the recipe of `test` and its groups (TEST_GROUPS) one after the other,
# about 80 minutes on 4 CPUs, much longer when the planted-bug runs are one at a time (a test_replica mutant is minutes
# under TSan). This builds everything the test target and the groups need ONCE (make -j; the groups then find every
# binary up to date and only read build-tsan/), runs `make test TEST_GROUPS=` (the rest of `make test`) and each group
# as a make of its own at the same time, each with its output in its own log, and fails when any of them failed. The
# planted-bug helpers of all of them share one pool of MUTANT_SLOTS runs (default: half the CPUs), so the box is not
# oversubscribed: ThreadSanitizer runs are timing-sensitive.
#
# Usage: tools/tsan-par.sh MAKE GROUPS     (the Makefile passes $(MAKE) and $(TEST_GROUPS); TSAN_FLAGS is in the environment)
# Environment: TSAN_PAR_LOGS (log directory, default a new one under $TMPDIR), TSAN_PAR_JOBS (makes at once, 6), MUTANT_SLOTS, MUTANT_JOBS, TSAN_PAR_REST=0
# (leave out the run of the rest of `make test`: to try the build and a few groups only).
set -u
make_cmd=${1:?usage: tsan-par.sh MAKE GROUPS}
groups=${2-}
B=build-tsan
flags=${TSAN_FLAGS:-"-O1 -g -fsanitize=thread -fno-omit-frame-pointer"}
cpus=$(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || echo 4)
mk() { $make_cmd B=$B TSDBD=$B/tsdbd ARCH= CFLAGS="$flags" "$@"; }

logs=${TSAN_PAR_LOGS:-$(mktemp -d "${TMPDIR:-/tmp}/tsan-par.XXXXXX")}
mkdir -p "$logs" || exit 1
echo "tsan-par: logs in $logs"

# 1. the binaries every group needs, from make's own database (the prerequisites of `test`, of each group and of the
#    other test-* and check-* targets that are files in $B), built in one step
bins=$(mk -qp test 2>/dev/null | awk -v b="$B/" '/^(test|check)[A-Za-z0-9_-]*:/ { for (i = 2; i <= NF; i++) if (index($i, b) == 1) print $i }' | sort -u)
[ -n "$bins" ] || { echo "tsan-par: no binaries found in the Makefile's database" >&2; exit 1; }
t0=$SECONDS
# shellcheck disable=SC2086
mk -j"$cpus" $bins >"$logs/build.log" 2>&1 || { tail -n 30 "$logs/build.log"; echo "tsan-par: the build failed ($logs/build.log)" >&2; exit 1; }
echo "tsan-par: built in $((SECONDS - t0)) s"

# 2. the groups at once
export MUTANT_SLOT_DIR=$logs/slots MUTANT_SLOTS=${MUTANT_SLOTS:-$((cpus / 2 > 0 ? cpus / 2 : 1))}
export TSAN_OPTIONS="halt_on_error=1 second_deadlock_stack=1 ${TSAN_OPTIONS:-}"
# at most TSAN_PAR_JOBS makes at once (default 6: the groups are 24 and a TSan process takes several GB; the memory peak of
# the whole set is unmeasured, see AGENTS.md); a finished make leaves its status in $logs/NAME.rc
maxj=${TSAN_PAR_JOBS:-6}
names=()
start() { # name, make arguments
    local name=$1
    shift
    while [ "$(jobs -rp | wc -l)" -ge "$maxj" ]; do wait -n || true; done
    ( mk "$@" >"$logs/$name.log" 2>&1; echo $? >"$logs/$name.rc" ) &
    names+=("$name")
}
[ "${TSAN_PAR_REST:-1}" = 0 ] || start rest test TEST_GROUPS=
for g in $groups; do start "$g" "$g"; done
wait
bad=0
for n in "${names[@]}"; do
    if [ "$(cat "$logs/$n.rc" 2>/dev/null || echo 1)" = 0 ]; then
        echo "tsan-par: $n: ok"
    else
        echo "tsan-par: $n: FAILED (log $logs/$n.log)"
        tail -n 15 "$logs/$n.log" | sed 's/^/    /'
        bad=1
    fi
done
echo "tsan-par: $((SECONDS - t0)) s in all"
[ "$bad" = 0 ] || exit 1
rm -rf "$logs/slots"
echo "tsan-par: every group passed"
