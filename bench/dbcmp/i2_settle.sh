#!/bin/bash
# i2_settle.sh LOADEND_EPOCH: poll InfluxDB 2's storage metrics until the cache is empty, no compaction
# is active, and nothing has changed for $STABLE s (300); print when the last change happened,
# counted from LOADEND_EPOCH (date +%s when the load returned).
#
# With the defaults the cache of a shard is written to TSM only after 10 minutes without writes
# (storage-cache-snapshot-write-cold-duration), so this takes over 10 minutes by design.
#
# storage_compactions_queued is not required to reach 0: InfluxDB 2.7.12 can keep a planned compaction
# queued that never starts (seen 2026-10-26: one "full" compaction of one shard queued for 10 hours with
# nothing active, no failures and no TSM file changing). With nothing active and nothing changing for
# $STABLE s it will not run; the "settled" line says how many were left queued.
# Gives up after $I2_SETTLE_MAX s (7200) with "NOT settled" and exit status 1, and exits 1 at once when
# the metrics cannot be read (an empty scrape would otherwise look like an idle server).
end=$1; P=${I2_PORT:-8086}; STABLE=${STABLE:-300}; MAX=${I2_SETTLE_MAX:-7200}
prev=""; t0=$(date +%s); last=$(date +%s.%N)
while :; do
  raw=$(curl -sf -m 10 "http://127.0.0.1:$P/metrics") || { echo "no metrics on port $P"; exit 1; }
  m=$(printf '%s\n' "$raw" | grep -E "^storage_compactions_(active|queued)|^storage_cache_inuse_bytes|^storage_tsm_files_total|^storage_wal_size" | awk '{split($1,a,"{"); s[a[1]]+=$2; n++} END {if (!n) exit 1; printf "active=%d queued=%d cache=%d tsm=%d wal=%d", s["storage_compactions_active"], s["storage_compactions_queued"], s["storage_cache_inuse_bytes"], s["storage_tsm_files_total"], s["storage_wal_size"]}') ||
    { echo "no storage metrics on port $P"; exit 1; }
  if [ "$m" != "$prev" ]; then last=$(date +%s.%N); prev="$m"; echo "$(echo "$last - $end" | bc | cut -c1-7) s: $m"; fi
  if [[ "$m" == "active=0 "*" cache=0 "* ]] && [ "$(echo "$(date +%s.%N) - $last > $STABLE" | bc)" = 1 ]; then break; fi
  if [ $(($(date +%s) - t0)) -ge "$MAX" ]; then
    echo "NOT settled after $MAX s: last change $(echo "$last - $end" | bc) s after the load ended ($prev)"; exit 1
  fi
  sleep 1
done
q=${prev#*queued=}; q=${q%% *}
note=""; [ "$q" != 0 ] && note="; $q compaction(s) left queued, never started, nothing active for $STABLE s"
echo "settled: last change $(echo "$last - $end" | bc) s after the load ended ($prev)$note"
