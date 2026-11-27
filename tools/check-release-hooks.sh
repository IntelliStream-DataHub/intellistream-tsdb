#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# tools/check-release-hooks.sh BINARY...: release binaries (built with TSDB_TEST_HOOKS=0 / make dist) must carry no
# auth-bypass, crash-point, durability-bypass, or mutant/test-only environment hooks.
set -u
[ $# -ge 1 ] || { echo "usage: $0 BINARY..." >&2; exit 2; }
command -v strings >/dev/null 2>&1 || { echo "check-release-hooks: strings not found" >&2; exit 2; }

FORBIDDEN_STRINGS=(
    TSDBD_FWD_MUTANT
    TSDB_AUTH_MUTANT
    TSDB_AUTH_TEST
    TSDB_CRASH_AT
    TSDB_SETTLE_CRASH_AT
    TSDB_FAST_FSYNC
    TSDB_CLUSTER_MUTANT
    TSDB_CLUSTER_TEST
    TSDB_CLUSTER_WRITES
    TSDB_REPLICA_READS
    TSDB_REPLD_MUTANT
    TSDB_REPLD_TEST
    TSDB_CVOL_MUTANT
    TSDB_CVOL_TEST
    TSDB_CMOVE_TEST
    TSDB_ROUTE_MUTANT
    TSDBD_MARGIN_MUTANT
    TSDBD_DEFAULT_CONFIG
    TSDBD_TEST_
    TSDB_TEST_TIER
    TSDB_TEST_TIER_I_KNOW
    TSDB_SWAP_MUTANT
    TSDB_SETTLE_MUTANT
    TSDB_ECCOLD_MUTANT
    TSDB_ECGC_MUTANT
    TSDB_ECGC_TEST
    TSDB_PPOS_MUTANT
    TSDB_CHUNKPEER_MUTANT
    TSDB_CHUNKPEER_TEST
    TSDB_CHUNKSRV_TEST
    TSDB_CHUNKSCRUB_TEST
    TSDB_CHUNKSCRUB_MUTANT
    TSDB_PLRDMA_TEST
    fwdnouser
    noguard
    authlocalusers
    loginbeforecatchup
    importmerge
)

FORBIDDEN_SYMS='^_?(crashpt_slow|crashpt_state|crash_point_names)$'

pat=$(IFS='|'; printf '%s' "${FORBIDDEN_STRINGS[*]}")
rc=0

for f in "$@"; do
    if [ ! -f "$f" ]; then
        echo "check-release-hooks: $f: no such file" >&2
        rc=1
        continue
    fi
    hits=$(strings "$f" 2>/dev/null | grep -E "$pat" | sort -u | head -n 40 || true)
    if [ -n "$hits" ]; then
        echo "check-release-hooks: $f contains test/mutant/auth-bypass hook strings:" >&2
        printf '  %s\n' "$hits" >&2
        rc=1
    fi
    if command -v nm >/dev/null 2>&1; then
        syms=$(nm -P "$f" 2>/dev/null | awk '{print $1}' | grep -E "$FORBIDDEN_SYMS" | sort -u || true)
        if [ -n "$syms" ]; then
            echo "check-release-hooks: $f contains test/crash-point symbols:" >&2
            printf '  %s\n' "$syms" >&2
            rc=1
        fi
    fi
done

[ $rc = 0 ] && echo "check-release-hooks: ok ($*)"
exit $rc
