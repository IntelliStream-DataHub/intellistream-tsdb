#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# Each planted bug of test_settle_oracle must be caught by its own line, never by any failure: a run that fails for
# another reason (a drifted battery, a flake) would otherwise count as a kill. noretry and rankdown show only where a
# read meets a damaged data cell or an overwritten key, which a seed window may miss (tests/test_settle_oracle.c, top
# of the file), so a mutant gets up to three windows of two seeds; one that reports its bug in none of them survived.
# Usage: tests/oracle_mutants.sh ORACLE SCRATCH MUTANT...
set -u
oracle=$1 scratch=$2
shift 2
for m in "$@"; do
    case $m in
    nomark) want='test_settle_oracle: no part was settled' ;;
    *) want='^DIVERGED ' ;;
    esac
    caught=
    for first in 1 101 201; do
        out=$("$oracle" --mutant "$m" --seeds 2 --first "$first" "$scratch" 2>&1)
        rc=$?
        if [ "$rc" -ne 0 ] && grep -q "$want" <<<"$out"; then
            caught=$first
            break
        fi
    done
    if [ -z "$caught" ]; then
        echo "test_settle_oracle: mutant $m was not caught by its own line ('$want') in seed windows 1, 101, 201" >&2
        exit 1
    fi
done
