#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# Runs the queries written by `scale-bench query --sql DIR` in ClickHouse, one
# clickhouse-benchmark run per class over a single connection, and prints the
# mean and median latency in the format of scale-bench.
#
# The mean is the class's wall time over its queries (one connection, one query
# at a time, so the connect is shared by all of them), not 1000 / QPS:
# clickhouse-benchmark rounds QPS to three decimals, which made a 108 s query
# 111111.111 ms (1000 / 0.009). The median is clickhouse-benchmark's (whole ms).
# Each class's queries carry a log_comment, and the server's own mean and median
# (query_log, microseconds) follow on the same line.
#
#   tools/scale-bench-ch.sh DIR [TABLE] [CLICKHOUSE_BINARY]
set -eu
dir=$1 table=${2:-big} ch=${3:-clickhouse}
run=sbch.$$.$(date +%s)
for c in point agg_100k agg_1m agg_10m agg_1b agg_all grp_1s_hour grp_1s_minute \
         grp_100s_day grp_100s_hour grp_all_day grp_all_hour grp_all_day_utc; do
    f=$dir/$c.sql
    [ -f "$f" ] || continue
    n=$(wc -l < "$f")
    s=$(date +%s%N)
    out=$(sed "s/ FROM big / FROM $table /" "$f" |
        "$ch" benchmark --host 127.0.0.1 --concurrency 1 --iterations "$n" "--log_comment=$run.$c" 2>&1)
    e=$(date +%s%N)
    srv=$("$ch" client --host 127.0.0.1 -q "SYSTEM FLUSH LOGS" -q "WITH dateDiff('microsecond',
        query_start_time_microseconds, event_time_microseconds) / 1000 AS d SELECT count(), round(avg(d), 3),
        round(quantile(0.5)(d), 3) FROM system.query_log WHERE type = 'QueryFinish' AND log_comment = '$run.$c'
        FORMAT TSV" 2> /dev/null | tail -1) || srv=""
    printf '%s\n' "$out" | awk -v c="$c" -v n="$n" -v wall="$(( (e - s) / 1000 ))" -v srv="$srv" '
        /^50%/ { p = $2 }
        /Exception|Code:/ { print; bad = 1 }
        END {
            if (bad) exit
            split(srv, a, "\t")
            printf "%-14s %5d q  mean %10.3f ms  p50 %10.3f ms", c, n, wall / 1000 / n, p * 1000
            if (a[1] != "") printf "  server: %d q, mean %.3f ms, median %.3f ms", a[1], a[2], a[3]
            printf "\n"
        }'
done
