#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# Makes tests/data/v8-fixture.tar and v8-fixture.tsdbdump with the binary of release 0.9.0 (disk format 8):
#   git archive v0.9.0 | tar -x -C /tmp/v090 && make -C /tmp/v090 build/tsdb
#   tests/data/make-v8-fixture.sh /tmp/v090/build/tsdb tests/data
# The database has a root and one more volume, every value type, parts of several partitions, tombstones (a delete
# inside a partition), an empty part (every row of a partition deleted, then merged), a part that no merge wrote and
# parts that merges wrote, a retention limit and states. Closed cleanly: no WAL segment. The export is made by the
# same binary: what a format-9 database must give back after `tsdb DIR upgrade`.
set -e
TSDB=$1 OUT=${2:-.}
W=$(mktemp -d /tmp/v8fx.XXXXXX)
DB=$W/db VOL=$W/vol1
mkdir -p $VOL
B=1704067200000 # 2024-01-01 00:00:00 UTC
DAY=86400000
$TSDB $DB create f32 --type float32
$TSDB $DB volume add $VOL >/dev/null
$TSDB $DB create f64 --type float64
$TSDB $DB create i64 --type int64
$TSDB $DB create flag --type bool
$TSDB $DB create mode --type state
$TSDB $DB create gone --type float32
$TSDB $DB create v1f32 $VOL --type float32
$TSDB $DB create v1i64 $VOL --type int64
rows() { # series first_ts count step value-expression (awk, i = index)
    awk -v s="$1" -v t0="$2" -v n="$3" -v st="$4" "BEGIN { for (i = 0; i < n; i++) printf \"%s,%d,%s\\n\", s, t0 + i * st, $5 }"
}
{
    rows f32 $B 3000 60000 'sprintf("%.1f", 20 + (i % 50) / 10)'
    rows f64 $B 3000 60000 'sprintf("%.3f", i * 0.125)'
    rows i64 $B 3000 60000 'i * 7'
    rows flag $B 200 600000 '(i % 3 == 0) ? "true" : "false"'
    rows mode $B 200 600000 '(i % 4 == 0) ? "idle" : ((i % 4 == 1) ? "run" : ((i % 4 == 2) ? "stop" : "run"))'
    rows gone $((B + 9 * DAY)) 500 60000 'i'
    rows v1f32 $B 1500 120000 'sprintf("%.2f", i / 100)'
    rows v1i64 $B 1500 120000 '1000 + i'
} | $TSDB $DB import
$TSDB $DB delete f64 $((B + 600000)) $((B + 1800000))        # tombstones inside the first partition
$TSDB $DB delete gone $((B + 9 * DAY)) $((B + 10 * DAY))       # every row of its partition: an empty part after the merge
$TSDB $DB retention f32 3650d
$TSDB $DB optimize
{                                                              # parts no merge wrote, in a new and an old partition
    rows f32 $((B + 4 * DAY)) 400 60000 'sprintf("%.1f", 30 + (i % 7) / 10)'
    rows i64 $((B + DAY)) 50 1000 '-i'
    rows v1i64 $((B + 5 * DAY)) 100 1000 'i * i'
} | $TSDB $DB import
ls $DB | grep -c '^wal_' && { echo "a WAL segment is left"; exit 1; } || true
$TSDB $DB export -o $OUT/v8-fixture.tsdbdump
# the volume's path is in VOLUMES; the test rewrites that line for the place it unpacks to
( cd $W && rm -f db/LOCK vol1/LOCK && tar -cf $OUT/v8-fixture.tar db vol1 )
echo "$W: fixture written to $OUT (the scratch directory stays: rm it when done)"
