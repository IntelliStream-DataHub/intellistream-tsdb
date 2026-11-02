#!/bin/bash
# ch_settle.sh TABLE: wait until ClickHouse has finished merging cmp.TABLE.
#
# Settled means: system.merges is empty and the active part count has not changed for CH_STABLE seconds (default 180).
# The time is three times max_merge_selecting_sleep_ms (60 s): after a round with nothing to merge ClickHouse backs
# off up to that long before it looks again, so a shorter quiet window (the 60 s this script used to take, inside
# ch_load.sh) can end while merges are still to come: relbench3 (2026-10-29) ran 3 more merges during its raw-window
# queries (43 parts went to 40) and measured them 2x slower than the same queries on the merged table.
# Gives up after CH_SETTLE_MAX seconds (default 3000, below the harness's 3600 s step limit) with exit 1.
# Prints "merges settled S s after START (parts, merges: N 0)", START being the time this script started, or the
# CH_T0 given by the caller (epoch seconds, ch_load.sh passes the end of the insert).
source "$(dirname "$0")/env.sh"
CH="$CLICKHOUSE client --host 127.0.0.1"
T=$1
STABLE=${CH_STABLE:-180} MAX=${CH_SETTLE_MAX:-3000}
t0=${CH_T0:-$(date +%s.%N)}
last=$(date +%s.%N); prev=""
while :; do
  parts=$($CH -q "SELECT count() FROM system.parts WHERE database='cmp' AND table='$T' AND active") || exit 1
  merges=$($CH -q "SELECT count() FROM system.merges WHERE database='cmp' AND table='$T'") || exit 1
  # any merge running, or a different part count: the quiet time starts again
  if [ "$merges" != 0 ] || [ "$parts" != "$prev" ]; then last=$(date +%s.%N); prev=$parts; fi
  now=$(date +%s.%N)
  if [ "$(echo "$now - $last > $STABLE" | bc)" = 1 ]; then break; fi
  if [ "$(echo "$now - $t0 > $MAX" | bc)" = 1 ]; then
    echo "merges NOT settled after $MAX s (parts, merges: $parts $merges)"; exit 1
  fi
  sleep 0.2
done
echo "merges settled $(echo "$last - $t0" | bc) s after the insert returned (parts, merges: $prev 0; quiet for $STABLE s)"
