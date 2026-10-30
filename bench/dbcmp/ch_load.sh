#!/bin/bash
# ch_load.sh TABLE: INSERT $DATA into cmp.TABLE as RowBinary, sync the filesystem, then wait for merges.
#
# ClickHouse does not fsync inserted parts by default, so the syncfs after the insert is timed on
# its own: insert time is "queryable", insert + syncfs is "durable". Merges are considered settled
# (ch_settle.sh) when no merge runs and the active part count has not changed for 180 s, three times
# ClickHouse's back-off between merge selections; the part count is printed for the record.
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
CH_T0=$e exec "$(dirname "$0")/ch_settle.sh" "$T"
