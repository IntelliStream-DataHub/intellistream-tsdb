#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# A tsdbd that takes SLOW_START seconds (default 2.2) to start, for the jepsen self-test: on a busy machine the servers
# start slowly and a power run makes few restarts, which is no sign that its nemesis was idle. (The follower is
# started at once.)
for a in "$@"; do [ "$a" = --follow ] && exec "${REAL_TSDBD:-./build/tsdbd}" "$@"; done
sleep "${SLOW_START:-2.2}"
exec "${REAL_TSDBD:-./build/tsdbd}" "$@"
