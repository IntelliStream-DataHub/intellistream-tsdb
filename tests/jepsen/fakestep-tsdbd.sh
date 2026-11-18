#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# A tsdbd for the jepsen self-test that "settles" nothing: it says it did (a log line, and a settled_ name when
# FAKE_FILES=1) and, armed with TSDB_SETTLE_CRASH_AT, ends by itself as tests/jepsen/fakestep.c says (FAKE_SO, FAKE_EXIT).
# The tier's variables are taken out of the real tsdbd's environment.
unset TSDB_TEST_TIER TSDB_TEST_TIER_I_KNOW
d=""; prev=""
for a in "$@"; do [ "$prev" = "--dir" ] && d=$a; prev=$a; done
if [ "${FAKE_FILES:-0}" = 1 ] && [ -n "$d" ] && [ -d "$d" ]; then
    env -u LD_PRELOAD touch "$d/settled_p0_1_1_0.part"
fi
echo "tsdbd: fake: settled p0" >&2
[ -n "${TSDB_SETTLE_CRASH_AT:-}" ] && [ -n "${FAKE_SO:-}" ] && export LD_PRELOAD="${LD_PRELOAD:-} $FAKE_SO"
exec "${REAL_TSDBD:-./build/tsdbd}" "$@"
