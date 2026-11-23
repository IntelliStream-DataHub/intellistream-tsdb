#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# The seed switch of tests/seed.h works: no TSDB_TEST_SEED = the test's own constants and no "seed" line; a seed prints
# its line, and two seeds give two different values (a test that ignored it would print nothing, or the same twice).
B=${B:-build}
t=$B/test_wire
v=tests/data/wire13-vectors.txt
seed_line() { TSDB_TEST_SEED=$1 "$t" "$v" 2>&1 >/dev/null | grep '^seed wire mutations: '; }
plain=$("$t" "$v" 2>&1 | grep -c '^seed ')
[ "$plain" = 0 ] || { echo "test_seeds: a run without TSDB_TEST_SEED printed a seed line" >&2; exit 1; }
a=$(seed_line 1) || { echo "test_seeds: TSDB_TEST_SEED=1 printed no 'seed wire mutations' line" >&2; exit 1; }
b=$(seed_line 2) || { echo "test_seeds: TSDB_TEST_SEED=2 printed no 'seed wire mutations' line" >&2; exit 1; }
a2=$(seed_line 1)
[ "$a" != "$b" ] || { echo "test_seeds: two seeds gave the same value: $a" >&2; exit 1; }
[ "$a" = "$a2" ] || { echo "test_seeds: one seed gave two values: $a / $a2" >&2; exit 1; }
echo "test_seeds: ok ($a)"
