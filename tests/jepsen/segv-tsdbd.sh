#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# A tsdbd that crashes (SIGSEGV) 0.1 s after it starts, without the crashfs shim, for the jepsen self-test: the power
# nemesis must report a server that dies this way as a failure, not restart it as if the shim had cut its power.
# (A follower, which runs this too, is left alone.)
for a in "$@"; do [ "$a" = --follow ] && exec "${REAL_TSDBD:-./build/tsdbd}" "$@"; done
# 0.1 s is what a server started under a shim gets. A plain one (the first, before the shim, and the final reads)
# crashes a second later: on a loaded machine 0.1 s was shorter than jepsen's connect, and the run then ended at
# "cannot connect" (before create_series was bounded it hung for hours), not at the crash the self-test looks for.
d=0.1
[ -n "${LD_PRELOAD:-}" ] || d=1
unset LD_PRELOAD
( sleep $d; kill -SEGV $$ ) &
exec "${REAL_TSDBD:-./build/tsdbd}" "$@"
