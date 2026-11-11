#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# A tsdbd that crashes (SIGSEGV) 0.3 s after it starts, for the jepsen self-test: with jepsen --mutant-slowcreate (which
# waits 1 s before it creates its series) the first server is dead when jepsen looks for it. The run must end with
# "cannot connect to create the series" (rc 2), not wait for a server that is gone (it hung for hours under load).
for a in "$@"; do [ "$a" = --follow ] && exec "${REAL_TSDBD:-./build/tsdbd}" "$@"; done
unset LD_PRELOAD
( sleep 0.3; kill -SEGV $$ ) &
exec "${REAL_TSDBD:-./build/tsdbd}" "$@"
