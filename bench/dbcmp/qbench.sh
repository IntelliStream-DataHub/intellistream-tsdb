#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# qbench.sh SYS DRIVER-ARGS...: the query files q/SYS.* through qdrive.
#   DRIVER-ARGS: pg CONNINFO | http HOST PORT HEADERS
# Whole-range queries: a warm-up, then the median of 5. Random windows and point lookups: a 3 s
# warm-up, then $SECS s (15) at 1 and at 32 clients. FULLS and CLASSES select a subset; set one
# to " " to skip that part (InfluxDB 3 whole-range queries take minutes).
source "$(dirname "$0")/env.sh"
S=$1; shift
FULLS=${FULLS:-"0 3600 60"}; CLASSES=${CLASSES:-"raw agg1d agg7d point"}; SECS=${SECS:-15}
for n in $FULLS; do
  "$BIN/qdrive" "$@" q/$S.full$n.txt 1 100000 1 > /dev/null || exit 1
  "$BIN/qdrive" "$@" q/$S.full$n.txt 1 100000 5
done
for c in $CLASSES; do
  "$BIN/qdrive" "$@" q/$S.$c.txt 1 3 > /dev/null || exit 1
  for t in 1 32; do "$BIN/qdrive" "$@" q/$S.$c.txt $t $SECS; done
done
