#!/bin/sh
# Runs the queries written by `scale-bench query --sql DIR` in ClickHouse, one
# clickhouse-benchmark run per class over a single connection, and prints the
# mean and median latency in the format of scale-bench.
#
#   tools/scale-bench-ch.sh DIR [TABLE] [CLICKHOUSE_BINARY]
set -eu
dir=$1 table=${2:-big} ch=${3:-clickhouse}
for c in point agg_100k agg_1m agg_10m agg_1b agg_all grp_1s_hour grp_1s_minute \
         grp_100s_day grp_100s_hour grp_all_day grp_all_hour grp_all_day_utc; do
    f=$dir/$c.sql
    [ -f "$f" ] || continue
    n=$(wc -l < "$f")
    sed "s/ FROM big / FROM $table /" "$f" |
        "$ch" benchmark --host 127.0.0.1 --concurrency 1 --iterations "$n" 2>&1 |
        awk -v c="$c" -v n="$n" '
            /QPS:/ { for (i = 1; i <= NF; i++) if ($i == "QPS:") q = $(i + 1) + 0 }
            /^50%/ { p = $2 }
            /Exception|Code:/ { print; bad = 1 }
            END { if (!bad) printf "%-14s %5d q  mean %10.3f ms  p50 %10.3f ms\n", c, n, 1000 / q, p * 1000 }'
done
