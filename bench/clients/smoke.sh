#!/bin/bash
# A quick check that the harness works (make bench-clients-smoke): embedded and the C client on a tiny data set,
# and that verification notices rows that are not the file's. The numbers mean nothing. run.sh --clients "c rust
# python java" does the same for the other clients.
set -eu
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
B=$ROOT/build
W=${1:-$B/cbench-smoke}
rm -rf "$W"
mkdir -p "$W"
"$HERE/run.sh" --force --no-build --clients c --series 5 --points 20000 --repeat 1 --seconds 0.5 --conns "1 2" \
    --qthreads "1 2" --work "$W" >"$W/run.out" 2>&1 || { tail -n 30 "$W/run.out"; exit 1; }
grep -q "0 mismatches" "$W"/results-*/summary.md

# Verification must fail when the server (or the embedded database) holds other rows than the file: another row
# count, other values at the same timestamps, the same values at other timestamps, and one row moved to another time.
"$B/tsdb-bench" --export "$W/a.rb" --series 5 --points 20000 >/dev/null
"$B/tsdb-bench" --export "$W/b.rb" --series 5 --points 20001 >/dev/null
python3 - "$W" <<'PY'
import struct, sys
w = sys.argv[1]
rows = list(struct.iter_unpack("<IqF", open(w + "/a.rb", "rb").read()))
def write(name, rs):
    open(w + "/" + name, "wb").write(b"".join(struct.pack("<IqF", *r) for r in rs))
write("values.rb", [rows[0][:2] + (rows[0][2] + 1.0,)] + rows[1:])      # one value changed
write("shifted.rb", [(s, t + 1, v) for s, t, v in rows])                 # every timestamp +1 ms
moved = list(rows)
moved[100] = (moved[100][0], moved[100][1] + 500, moved[100][2])         # one row 500 ms later, inside the range
write("moved.rb", moved)
PY
mkdir -p "$W/neg"
"$B/tsdbd" --dir "$W/neg/srv" --no-auth --port 0 --port-file "$W/neg.port" --exit-with-parent=$$ >"$W/neg.log" 2>&1 &
pid=$!
trap 'kill $pid 2>/dev/null || true' EXIT
for _ in $(seq 100); do [ -s "$W/neg.port" ] && break; kill -0 $pid; sleep 0.1; done
read -r PORT fpid <"$W/neg.port" # where tsdbd says it listens, and whose server that is
[ "$fpid" = "$pid" ] || { echo "smoke: $W/neg.port names pid $fpid, the server started here is $pid" >&2; exit 1; }
"$B/tsdbc-bench" --port "$PORT" load "$W/a.rb" >/dev/null
"$B/tsdbc-bench" --port "$PORT" verify "$W/a.rb" >/dev/null
"$B/tsdb-bench" --dir "$W/neg/emb" --load "$W/a.rb" --threads 2 >/dev/null
"$B/tsdb-bench" --dir "$W/neg/emb" --verify "$W/a.rb" >/dev/null
for bad in b values shifted moved; do
    if "$B/tsdbc-bench" --port "$PORT" verify "$W/$bad.rb" >/dev/null 2>&1; then
        echo "smoke: the TCP verify accepted $bad.rb" >&2
        exit 1
    fi
    if "$B/tsdb-bench" --dir "$W/neg/emb" --verify "$W/$bad.rb" >/dev/null 2>&1; then
        echo "smoke: the embedded verify accepted $bad.rb" >&2
        exit 1
    fi
done
# and a query benchmark on an empty server or a missing directory must fail, not report zero rows
if "$B/tsdb-bench" --dir "$W/neg/nonexistent" --qps 1 --series 5 --points 20000 --seconds 0.2 >/dev/null 2>&1; then
    echo "smoke: --qps on a missing directory succeeded" >&2
    exit 1
fi
[ ! -e "$W/neg/nonexistent" ]

# the summary must flag systems that did not run the same windows (rows per query differ), and unverified loads
mkdir -p "$W/fake"
echo "fake" >"$W/fake/facts.txt"
{
    echo "VERIFY phase=queryset verify: 5 series, 100 rows on the server, 100 in the file: ok"
    echo "RESULT phase=query rep=1 load1=0.1 busy=0.1 kind=query client=embedded class=raw threads=1 queries=1000 rows=3600000 secs=1 cpu=1"
    echo "RESULT phase=query rep=1 load1=0.1 busy=0.1 kind=query client=python class=raw threads=1 queries=1000 rows=1010000 secs=1 cpu=1"
} >"$W/fake/results.log"
if python3 "$HERE/summarize.py" "$W/fake" >"$W/fake/summary.md" 2>&1; then
    echo "smoke: the summary accepted systems with different rows per query" >&2
    exit 1
fi
grep -q "rows per query differ" "$W/fake/summary.md"
sed -i 's/rows=1010000/rows=3600000/' "$W/fake/results.log"
python3 "$HERE/summarize.py" "$W/fake" >"$W/fake/summary.md"
sed -i '/^VERIFY/d' "$W/fake/results.log"
if python3 "$HERE/summarize.py" "$W/fake" >/dev/null 2>&1; then
    echo "smoke: the summary accepted a run without any verification" >&2
    exit 1
fi
echo "bench-clients-smoke: ok"
