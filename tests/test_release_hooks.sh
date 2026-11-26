#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# Regression test for TSDB_TEST_HOOKS build gating:
#   1. Dev binary ($B/tsdbd, built with default TSDB_TEST_HOOKS=1) must FAIL tools/check-release-hooks.sh
#      (proving the check has teeth) and honor TSDB_CRASH_AT / TSDBD_FWD_MUTANT.
#   2. Release build (TSDB_TEST_HOOKS=0) of tsdbd, tsdb, and libtsdb must compile with zero warnings,
#      PASS tools/check-release-hooks.sh, and ignore TSDB_CRASH_AT / TSDBD_FWD_MUTANT / TSDB_AUTH_MUTANT.
set -eu
cd "$(dirname "$0")/.."
B=${B:-build}
CC_ARG=${CC:-cc}
SOEXT=so
[ "$(uname -s)" = Darwin ] && SOEXT=dylib

tmp=$(mktemp -d "${TMPDIR:-/tmp}/tsdb-relhooks-XXXXXX")
srv_pid=
cleanup() {
    if [ -n "${srv_pid:-}" ] && [ "$srv_pid" -gt 0 ] 2>/dev/null; then
        kill "$srv_pid" 2>/dev/null || true
        wait "$srv_pid" 2>/dev/null || true
    fi
    rm -rf "$tmp"
}
trap cleanup EXIT

# 1. check-release-hooks.sh must catch hooks in the dev binary.
if tools/check-release-hooks.sh "$B/tsdbd" >/dev/null 2>&1; then
    echo "test_release_hooks: FAIL: check-release-hooks.sh passed on dev binary $B/tsdbd" >&2
    exit 1
fi
if tools/check-release-hooks.sh "$B/tsdb" >/dev/null 2>&1; then
    echo "test_release_hooks: FAIL: check-release-hooks.sh passed on dev binary $B/tsdb" >&2
    exit 1
fi

# 2. Build release binaries with TSDB_TEST_HOOKS=0 and require zero unused warnings.
rel="$tmp/build-rel"
jobs=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 2)
wlog="$tmp/build.err"
unset MAKEFLAGS MFLAGS MAKEOVERRIDES
make -j"$jobs" B="$rel" CC="$CC_ARG" TSDB_TEST_HOOKS=0 \
    "$rel/tsdbd" "$rel/tsdb" "$rel/libtsdb.$SOEXT" >"$tmp/build.out" 2>"$wlog"
if grep -E 'warning:.*-Wunused' "$wlog" >/dev/null 2>&1; then
    echo "test_release_hooks: FAIL: TSDB_TEST_HOOKS=0 build emitted unused warnings:" >&2
    cat "$wlog" >&2
    exit 1
fi

# 3. Release binaries must pass check-release-hooks.sh.
tools/check-release-hooks.sh "$rel/tsdbd" "$rel/tsdb" "$rel/libtsdb.$SOEXT"

# 4. Runtime check: release tsdbd ignores TSDB_CRASH_AT and TSDBD_FWD_MUTANT, while dev tsdbd honors them.
me=$BASHPID
mkdir -p "$tmp/db-rel"
pf="$tmp/rel.port"
TSDB_CRASH_AT=serving TSDBD_FWD_MUTANT=bogus TSDB_AUTH_MUTANT=noguard \
    "$rel/tsdbd" --no-auth --dir "$tmp/db-rel" --port 0 --port-file "$pf" --exit-with-parent="$me" \
    >"$tmp/rel.log" 2>&1 &
srv_pid=$!
for _ in $(seq 1 100); do
    [ -s "$pf" ] && break
    kill -0 "$srv_pid" 2>/dev/null || break
    sleep 0.05
done
if [ ! -s "$pf" ]; then
    echo "test_release_hooks: FAIL: release tsdbd failed to serve with hook env vars set:" >&2
    cat "$tmp/rel.log" >&2
    exit 1
fi
read -r port pid _ < "$pf"
if [ "$pid" != "$srv_pid" ] || [ -z "$port" ]; then
    echo "test_release_hooks: FAIL: unexpected port-file contents: $(cat "$pf")" >&2
    exit 1
fi
kill "$srv_pid" 2>/dev/null || true
wait "$srv_pid" 2>/dev/null || true
srv_pid=

# Dev tsdbd must still honor TSDBD_FWD_MUTANT (unknown name -> exit 2) and TSDB_CRASH_AT (serving -> exit 99).
mkdir -p "$tmp/db-dev"
rc=0
TSDBD_FWD_MUTANT=bogus "$B/tsdbd" --no-auth --dir "$tmp/db-dev" --port 0 --port-file "$tmp/dev1.port" \
    --exit-with-parent="$me" >"$tmp/dev1.log" 2>&1 || rc=$?
if [ "$rc" -ne 2 ]; then
    echo "test_release_hooks: FAIL: dev tsdbd with TSDBD_FWD_MUTANT=bogus exited $rc (want 2)" >&2
    cat "$tmp/dev1.log" >&2
    exit 1
fi

rc=0
TSDB_CRASH_AT=serving "$B/tsdbd" --no-auth --dir "$tmp/db-dev" --port 0 --port-file "$tmp/dev2.port" \
    --exit-with-parent="$me" >"$tmp/dev2.log" 2>&1 || rc=$?
if [ "$rc" -ne 99 ]; then
    echo "test_release_hooks: FAIL: dev tsdbd with TSDB_CRASH_AT=serving exited $rc (want 99)" >&2
    cat "$tmp/dev2.log" >&2
    exit 1
fi

echo "test_release_hooks: ok"
