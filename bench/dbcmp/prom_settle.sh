#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# prom_settle.sh START_EPOCH_S: waits until Prometheus' TSDB has settled after a load: no compaction,
# no block (re)load and no change in the head's chunks for $STABLE s (180; the TSDB looks for work
# once a minute). Prints the state every 30 s and, at the end, the seconds from START (the end of
# the load) to the last change. Prometheus is at $PROM_PORT (9090) on 127.0.0.1.
P=${PROM_PORT:-9090}; STABLE=${STABLE:-180}
state() {
  curl -sf http://127.0.0.1:$P/metrics | awk '
    /^prometheus_tsdb_compactions_total /        { c = $2 }
    /^prometheus_tsdb_blocks_loaded /            { b = $2 }
    /^prometheus_tsdb_head_chunks /              { h = $2 }
    /^prometheus_tsdb_compaction_populating_block / { p = $2 }
    /^prometheus_tsdb_head_min_time_seconds /    { mn = $2 }
    /^prometheus_tsdb_head_max_time_seconds /    { mx = $2 }
    END { printf "compactions %d blocks %d head_chunks %d populating %d head_span_h %.1f\n", c, b, h, p, (mx - mn) / 3600 }'
}
last=$(state) || { echo "no metrics on port $P"; exit 1; }
changed=$(date +%s); shown=0
while :; do
  sleep 5
  cur=$(state) || { echo "no metrics on port $P"; exit 1; }
  t=$(date +%s)
  if [ "$cur" != "$last" ]; then last=$cur; changed=$t; fi
  if [ $((t - shown)) -ge 30 ]; then echo "$(date +%T) $cur"; shown=$t; fi
  if [ $((t - changed)) -ge "$STABLE" ] && [[ $cur == *"populating 0"* ]]; then
    echo "settled: last change $((changed - $1)) s after the load ($cur)"; exit 0
  fi
done
