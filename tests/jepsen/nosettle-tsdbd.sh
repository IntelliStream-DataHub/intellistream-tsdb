#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# A tsdbd that never settles, for the jepsen self-test: the tier's variables are taken out of its environment, so it
# runs without a tier whatever jepsen --tier says. A tier run on it must fail the settle count (a run in which nothing
# settled tested nothing of the tier).
unset TSDB_TEST_TIER TSDB_TEST_TIER_I_KNOW
exec "${REAL_TSDBD:-./build/tsdbd}" "$@"
