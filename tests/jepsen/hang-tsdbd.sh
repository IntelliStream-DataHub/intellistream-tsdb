#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# A tsdbd that hangs (SIGSTOP) shortly after it starts when it runs under the fault shim (LD_PRELOAD set), for the jepsen
# self-test: a power pull (SIGUSR2) never ends it, and the run must report it as hung, not count it as a plug pulled.
# (The follower and the clean servers of the final checks are left alone.)
for a in "$@"; do [ "$a" = --follow ] && exec "${REAL_TSDBD:-./build/tsdbd}" "$@"; done
[ -n "$LD_PRELOAD" ] && { (sleep "${HANG_AFTER:-0.4}"; kill -STOP $$) > /dev/null 2>&1 & }
exec "${REAL_TSDBD:-./build/tsdbd}" "$@"
