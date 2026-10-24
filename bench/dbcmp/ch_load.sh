#!/bin/bash
# ch_load.sh TABLE: INSERT $DATA into cmp.TABLE as RowBinary, sync the filesystem, then wait for merges.
#
# ClickHouse does not fsync inserted parts by default, so the syncfs after the insert is timed on
# its own: insert time is "queryable", insert + syncfs is "durable". Merges are considered settled
# when the active part count and the running merges have not changed for 60 s; ClickHouse keeps
# merging small parts lazily long after that, so the part count is printed for the record.
source "$(dirname "$0")/env.sh"
CH="$CLICKHOUSE client --host 127.0.0.1"
T=$1
sync
s=$(date +%s.%N)
$CH -q "INSERT INTO cmp.$T FORMAT RowBinary" < "$DATA" || exit 1
e=$(date +%s.%N)
sync -f "$DBCMP"  # syncfs: the whole filesystem, which holds the ClickHouse data directory too
f=$(date +%s.%N)
echo "insert $(echo "$e - $s" | bc) s, syncfs $(echo "$f - $e" | bc) s"
$CH -q "SELECT count(), count(DISTINCT partition) FROM system.parts WHERE database='cmp' AND table='$T' AND active"
last=$(date +%s.%N); prev=""
while :; do
  cur=$($CH -q "SELECT (SELECT count() FROM system.parts WHERE database='cmp' AND table='$T' AND active), (SELECT count() FROM system.merges WHERE database='cmp' AND table='$T')")
  if [ "$cur" != "$prev" ]; then last=$(date +%s.%N); prev="$cur"; fi
  if [ "$(echo "$(date +%s.%N) - $last > 60" | bc)" = 1 ]; then break; fi
  sleep 0.2
done
echo "merges settled $(echo "$last - $e" | bc) s after the insert returned (parts, merges: $prev)"
