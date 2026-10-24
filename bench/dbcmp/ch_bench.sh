#!/bin/bash
# ch_bench.sh SYS (ch | chday): the query files q/SYS.* through clickhouse-benchmark over the native
# protocol. Whole-range queries: a warm-up, then the median of 5. Random windows and point lookups:
# a 3 s warm-up, then 15 s at 1 and at 32 clients. The queries carry no FORMAT Null, so results are
# sent to the client like with every other system.
source "$(dirname "$0")/env.sh"
S=$1
B="$CLICKHOUSE benchmark --host 127.0.0.1 --delay 0"
summ() { grep -E "queries: [0-9]+, QPS|^50%" | tr '\n' ' ' | sed -E 's/.*queries: ([0-9]+), QPS: ([0-9.]+).*50%[^0-9]*([0-9.]+) sec.*/\1 queries, \2 q\/s, median \3 s/'; echo; }
for n in 0 3600 60; do
  $B -c 1 -i 1 < q/$S.full$n.txt > /dev/null 2>&1
  echo -n "full$n c=1: "; $B -c 1 -i 5 < q/$S.full$n.txt 2>&1 | summ
done
for c in raw agg1d agg7d point; do
  $B -c 1 --timelimit 3 < q/$S.$c.txt > /dev/null 2>&1
  for t in 1 32; do echo -n "$c c=$t: "; $B -c $t --timelimit 15 < q/$S.$c.txt 2>&1 | summ; done
done
