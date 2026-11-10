#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# A tsdbd that crashes (SIGSEGV) 0.1 s after it starts, without the crashfs shim, for the jepsen self-test: the power
# nemesis must report a server that dies this way as a failure, not restart it as if the shim had cut its power.
# (A follower, which runs this too, is left alone.)
for a in "$@"; do [ "$a" = --follow ] && exec "${REAL_TSDBD:-./build/tsdbd}" "$@"; done
unset LD_PRELOAD
( sleep 0.1; kill -SEGV $$ ) &
exec "${REAL_TSDBD:-./build/tsdbd}" "$@"
