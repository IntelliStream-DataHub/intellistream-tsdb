#!/bin/bash
# prep.sh: inputs of the release benchmark (not timed); run.sh prep calls it under flock -s of BENCH.lock.
# PREP_SERIES/PREP_POINTS/PREP_SIZE_POINTS/PREP_SPEC shrink it for the smoke test.
#   main:  $DBCMP/input/mixed.{rowbinary,lp,ilp32,csv,om}, $WORK/spec.tsv
#   sizes: $DBCMP/sizes/in/<ds>.{rowbinary,lp,ilp32,csv,om}  (8 datasets.h kinds, 1 M rows each, series 0;
#          17 hydraulic sensors in bar + 6 pressures in psi, float32, series 0)
set -u
. "$(dirname "${BASH_SOURCE[0]}")/env.sh"
PREP_SERIES=${PREP_SERIES:-100} PREP_POINTS=${PREP_POINTS:-1000000} PREP_SIZE_POINTS=${PREP_SIZE_POINTS:-1000000}
PREP_SPEC=${PREP_SPEC:-10000}
mkdir -p $DBCMP/input $WORK/ref $WORK/res $DBCMP/sizes/in $DBCMP/sizes/hyd-series
fail=0
run() { echo "+ $*"; "$@" || { echo "FAILED: $*"; fail=1; }; }

DATA=$DBCMP/input/mixed.rowbinary
[ -s $DATA ] || run $T/tsdb-bench --export $DATA --series $PREP_SERIES --points $PREP_POINTS
for f in lp ilp32 csv om; do [ -s $DBCMP/input/mixed.$f ] || $B/conv $f $DATA $DBCMP/input/mixed.$f & done; wait
(cd $WORK && NSERIES=$PREP_SERIES POINTS=$PREP_POINTS python3 -I $H/spec.py $DATA $PREP_SPEC > spec.tsv && NSERIES=$PREP_SERIES python3 -I $H/batches.py spec.tsv > batches.tsv) || fail=1
# The answers are checked on this data only, and some recipes hold only for its shape (shape.c): no duplicate or
# out-of-order timestamp, no null, and no gap longer than the narrowest bucket a TWA kind gives that series
# (twa1d: 300 samples of its step, spec.py's STEP by series % 7; twa and twal: 1 h). Another shape stops the run.
if $B/shape $DATA > $WORK/shape.txt; then
  awk 'BEGIN { split("60000 1000 10000 10000 1 1000 1000", st, " ") }
       /^#/ { next }
       { b = 300 * st[$1 % 7 + 1]; if (b > 3600000) b = 3600000
         if ($3 || $4 || $5 || $6 > b) { bad++; print "shape: series " $1 ": duplicates " $3 ", unordered " $4 ", nulls " $5 ", largest gap " $6 " ms (bound " b ")" } }
       END { if (bad) { print "shape: " bad " series outside what the recipes handle"; exit 1 }
             print "shape: " NR - 1 " series, no duplicates, nulls or gaps beyond the narrowest bucket" }' $WORK/shape.txt || fail=1
else echo "FAILED: shape $DATA"; fail=1; fi

# datasets.h kinds: one series of 1 M readings each
for k in $DS_KINDS; do
  [ -s $DBCMP/sizes/in/$k.rowbinary ] || run $T/tsdb-bench --export $DBCMP/sizes/in/$k.rowbinary --series 1 --points $PREP_SIZE_POINTS --dataset $k
done
# hydraulic test rig: bench/hydraulic/convert.py on the cached UCI text files
[ -s $DBCMP/sizes/hyd-series/PS1.f32 ] || run python3 -I $ROOT/bench/hydraulic/convert.py $HYD_RAW $DBCMP/sizes/hyd-series
for s in PS1 PS2 PS3 PS4 PS5 PS6 EPS1 FS1 FS2 TS1 TS2 TS3 TS4 VS1 CE CP SE; do
  case $s in PS*|EPS1) step=10;; FS*) step=100;; *) step=1000;; esac
  [ -s $DBCMP/sizes/in/hyd_$s.rowbinary ] || run python3 -I $RDIR/rb_hyd.py $DBCMP/sizes/hyd-series/$s.f32 $step $DBCMP/sizes/in/hyd_$s.rowbinary &
  case $s in PS*) [ -s $DBCMP/sizes/in/hyd_${s}_psi.rowbinary ] || run python3 -I $RDIR/rb_hyd.py $DBCMP/sizes/hyd-series/${s}_psi.f32 $step $DBCMP/sizes/in/hyd_${s}_psi.rowbinary & ;; esac
done
wait
# conversions per dataset; QuestDB's ILP names the table in the line, so each dataset gets its own table s_<ds>
for f in $DBCMP/sizes/in/*.rowbinary; do
  d=${f%.rowbinary}; n=$(basename $d)
  ( [ -s $d.lp ] || $B/conv lp $f $d.lp
    [ -s $d.csv ] || $B/conv csv $f $d.csv
    [ -s $d.om ] || $B/conv om $f $d.om
    [ -s $d.ilp32 ] || { $B/conv ilp32 $f $d.ilp32.tmp && sed "s/^sensors,/s_$n,/" $d.ilp32.tmp > $d.ilp32 && rm $d.ilp32.tmp; } ) &
  while [ $(jobs -r | wc -l) -ge 12 ]; do sleep 0.5; done
done
wait
ls -la $DBCMP/input $DBCMP/sizes/in | awk '{print $5, $9}'
echo "PREP rc=$fail"
exit $fail
