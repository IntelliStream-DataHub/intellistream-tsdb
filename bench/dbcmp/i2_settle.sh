#!/bin/bash
# i2_settle.sh LOADEND_EPOCH: poll InfluxDB 2's storage metrics until the cache is empty, no compaction
# is active or queued, and nothing has changed for 5 minutes; print when the last change happened,
# counted from LOADEND_EPOCH (date +%s when the load returned).
#
# With the defaults the cache of a shard is written to TSM only after 10 minutes without writes
# (storage-cache-snapshot-write-cold-duration), so this takes over 10 minutes by design.
end=$1; prev=""; last=$(date +%s.%N)
while :; do
  m=$(curl -s "http://127.0.0.1:${I2_PORT:-8086}/metrics" | grep -E "^storage_compactions_(active|queued)|^storage_cache_inuse_bytes|^storage_tsm_files_total|^storage_wal_size" | awk '{split($1,a,"{"); s[a[1]]+=$2} END {printf "active=%d queued=%d cache=%d tsm=%d wal=%d", s["storage_compactions_active"], s["storage_compactions_queued"], s["storage_cache_inuse_bytes"], s["storage_tsm_files_total"], s["storage_wal_size"]}')
  if [ "$m" != "$prev" ]; then last=$(date +%s.%N); prev="$m"; echo "$(echo "$last - $end" | bc | cut -c1-7) s: $m"; fi
  if [[ "$m" == *"active=0 queued=0 cache=0 "* ]] && [ "$(echo "$(date +%s.%N) - $last > 300" | bc)" = 1 ]; then break; fi
  sleep 1
done
echo "settled: last change $(echo "$last - $end" | bc) s after the load ended ($prev)"
