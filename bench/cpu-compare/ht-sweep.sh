#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# Does the second hardware thread of a core help tsdb, and how do queries scale with --query-threads?
# Topology-aware (reads /sys/.../thread_siblings_list), so the same script runs on a 10850K, a 7950X and a 9950X.
#
#   bench/cpu-compare/ht-sweep.sh [--build DIR] [--work DIR] [--rounds 5] [--seconds 3] [--server] [--db DIR]
#
# A  pinned: k cores with one thread each, and k/2 cores with both threads, as many clients as CPUs, each with
#    query_threads = CPUs (tsdb-bench --qps): the per-core value of the sibling thread, and scaling over cores.
# B  one client, --query-threads Q unpinned, Q in 1 2 4 N T (N cores, T threads): one large query.
# C  with --server: tsdbd --query-threads Q, tsdbc-bench query --conns 1 --multi 100 (one QUERY_MULTI of 100 windows).
# Rounds are interleaved (round r runs every configuration before round r + 1). Prints q/s as mean (sd) over the
# rounds, and for A the factor over one CPU. Raw output is kept in WORK/ht. TSDB_CPU=v3|v4 selects the CPU level of
# a CPU_LEVELS build. Run it on a quiet machine, the governor on performance; it refuses above a load average of 2.
set -u -o pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
B=$ROOT/build WORK=/var/tmp/cpu-compare ROUNDS=5 SECS=3 SERVER=0 DB=""
while [ $# -gt 0 ]; do
    case $1 in
    --build) B=$2; shift 2 ;;
    --work) WORK=$2; shift 2 ;;
    --rounds) ROUNDS=$2; shift 2 ;;
    --seconds) SECS=$2; shift 2 ;;
    --server) SERVER=1; shift ;;
    --db) DB=$2; shift 2 ;;
    *) sed -n '3,17p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
    esac
done
[ -x "$B/tsdb-bench" ] || { echo "ht-sweep.sh: no $B/tsdb-bench (make)" >&2; exit 1; }
if awk -v l="$(cut -d' ' -f1 /proc/loadavg)" 'BEGIN { exit !(l > 2) }'; then
    echo "ht-sweep.sh: load average $(cut -d' ' -f1 /proc/loadavg): the machine is not quiet" >&2; exit 3
fi
O=$WORK/ht; mkdir -p "$O"; rm -f "$O"/*
DB=${DB:-$WORK/db}
[ -f "$DB/MANIFEST" ] || "$B/tsdb-bench" --dir "$DB" --series 100 --points 1000000 --keep >"$O/load.txt" 2>&1 || exit 1

# topology: cores[] = the first thread of each core, sib[] = its sibling (empty without SMT)
T=$(nproc --all); cores=(); sib=()
for ((c = 0; c < T; c++)); do
    l=$(cat /sys/devices/system/cpu/cpu$c/topology/thread_siblings_list 2>/dev/null || echo $c)
    first=${l%%[,-]*}
    [ "$c" = "$first" ] || continue
    cores+=("$c")
    case $l in *,*) sib+=("${l#*,}") ;; *-*) sib+=("$((first + 1))") ;; *) sib+=("") ;; esac
done
N=${#cores[@]}
echo "# $(lscpu | sed -n 's/^Model name: *//p' | head -n 1): $N cores, $T threads; governor $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo ?); level ${TSDB_CPU:-default}"
join() { local IFS=,; echo "$*"; }
cfgs=() # name:cpuset:count
for k in 1 2 4 $((N / 2)) $N; do
    [ "$k" -ge 1 ] && [ "$k" -le "$N" ] || continue
    set=$(join "${cores[@]:0:$k}")
    cfgs+=("c${k}core:$set:$k")
    if [ "$k" -ge 2 ] && [ "$((k % 2))" = 0 ] && [ -n "${sib[0]}" ]; then
        h=$((k / 2)); both=()
        for ((i = 0; i < h; i++)); do both+=("${cores[$i]}" "${sib[$i]}"); done
        cfgs+=("c${k}sib:$(join "${both[@]}"):$k")
    fi
done
[ "$T" -gt "$N" ] && cfgs+=("all${T}:0-$((T - 1)):$T")
qs=$(printf '%s\n' 1 2 4 "$N" "$T" | sort -n | uniq | tr '\n' ' ')

# an untimed pass first: the database comes into the page cache, the CPUs out of their idle states
"$B/tsdb-bench" --dir "$DB" --qps "$T" --query-threads "$T" --seconds 2 >/dev/null 2>&1
for ((r = 1; r <= ROUNDS; r++)); do
    for c in "${cfgs[@]}"; do
        IFS=: read -r name set k <<<"$c"
        taskset -c "$set" "$B/tsdb-bench" --dir "$DB" --qps "$k" --query-threads "$k" --seconds "$SECS" >"$O/A.$name.$r" 2>&1
    done
    for q in $qs; do
        "$B/tsdb-bench" --dir "$DB" --qps 1 --query-threads "$q" --seconds "$SECS" >"$O/B.q$q.$r" 2>&1
    done
    if [ "$SERVER" = 1 ]; then
        for q in $qs; do
            PF=$O/port; rm -f "$PF"; me=$BASHPID # the shell that starts tsdbd below (not $$ in a subshell)
            "$B/tsdbd" --dir "$DB" --no-auth --port 0 --port-file "$PF" --exit-with-parent="$me" --query-threads "$q" >"$O/srv.q$q.$r" 2>&1 &
            pid=$!
            for _ in $(seq 100); do [ -s "$PF" ] && break; sleep 0.1; done
            read -r port fpid _ <"$PF"
            if [ "$fpid" != "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then echo "tsdbd pid mismatch: child $pid, file $fpid" >&2; exit 1; fi
            "$B/tsdbc-bench" query --port "$port" --conns 1 --seconds "$SECS" --multi 100 >"$O/D.q$q.$r" 2>&1
            kill "$pid"; wait "$pid" 2>/dev/null
        done
    fi
    echo "# round $r of $ROUNDS done" >&2
done

# q/s mean (sd) per file prefix and class
summ() { # prefix classes...
    local p=$1; shift
    for cls in "$@"; do
        cat "$O"/$p.[0-9]* 2>/dev/null | awk -v c="$cls" '/^RESULT/ { delete v; for (i = 1; i <= NF; i++) { split($i, a, "="); v[a[1]] = a[2] }
            if (v["class"] == c) { x = v["queries"] / v["secs"]; n++; s += x; q += x * x } }
            END { if (n) { m = s / n; d = q / n - m * m; printf "%.0f %.0f %d", m, (d > 0 ? sqrt(d) : 0), n } }'
        echo
    done
}
CL="raw agg300 agg3600"
echo; echo "A. pinned, as many clients as CPUs, query_threads = CPUs; q/s mean (sd), factor over one CPU"
printf '%-10s %4s | %-24s | %-24s | %-24s\n' set cpus "raw 3600" "agg 86400" "agg 604800"
base=()
for c in "${cfgs[@]}"; do
    IFS=: read -r name set k <<<"$c"
    row=$(printf '%-10s %4s |' "$name" "$k"); i=0
    for cls in $CL; do
        read -r m sd n < <(summ "A.$name" "$cls")
        [ "$name" = "c1core" ] && base[$i]=$m
        f=$(awk -v m="${m:-0}" -v b="${base[$i]:-0}" 'BEGIN { if (b > 0) printf "%.2fx", m / b; else printf "-" }')
        row+=$(printf ' %8s (%5s) %6s |' "${m:--}" "${sd:--}" "$f"); i=$((i + 1))
    done
    echo "$row"
done
echo; echo "B. one client, query_threads Q; q/s mean (sd)"
printf '%-6s | %-18s | %-18s | %-18s\n' Q "raw 3600" "agg 86400" "agg 604800"
for q in $qs; do
    row=$(printf '%-6s |' "$q")
    for cls in $CL; do read -r m sd n < <(summ "B.q$q" "$cls"); row+=$(printf ' %8s (%5s) |' "${m:--}" "${sd:--}"); done
    echo "$row"
done
if [ "$SERVER" = 1 ]; then
    echo; echo "C. tsdbd --query-threads Q, one connection, QUERY_MULTI of 100 windows; q/s mean (sd)"
    printf '%-6s | %-18s | %-18s | %-18s | %-18s\n' Q raw "agg 86400" "agg 604800" "twa 604800"
    for q in $qs; do
        row=$(printf '%-6s |' "$q")
        for cls in raw.m100 agg300.m100 agg3600.m100 twa3600.m100; do read -r m sd n < <(summ "D.q$q" "$cls"); row+=$(printf ' %8s (%5s) |' "${m:--}" "${sd:--}"); done
        echo "$row"
    done
fi
echo; echo "raw output: $O"
