#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# rdma-bench.sh: the transports a peer link could use, measured between two servers for the sizes of tsdb's frames
# (docs/design-rdma.md section 3 has the results and how to read them). No address or host is built in: they are
# arguments, and the script only reaches the servers through ssh.
#
#   tools/rdma-bench.sh --srv SSH_TARGET --cli SSH_TARGET --ib-addr ADDR [--tcp-addr ADDR] [--dev mlx4_0]
#                       [--ib-port 1] [--reps 3] [--secs 3] [--only perftest,rsocket,tcp,loop] [--out DIR]
#                       [--pt-port 18515] [--bw-sizes "..."] [--lat-sizes "..."]
#
#   perftest  native verbs through rdma_cm (-R) at ADDR: ib_send_bw, ib_write_bw, ib_read_bw with completion events
#             (--events: what a poll() loop would see) and ib_send_lat, ib_write_lat, ib_read_lat busy-polling,
#             plus ib_send_lat with events; CPU per side from bash's `times` (user + system of the child)
#   rsocket   tools/link-bench --rsocket (librdmacm's rsocket API, never librspreload, which falls back to a kernel
#             socket) at --ib-addr through rdma_cm, default buffers and --buf 1 MiB (on both sides); with and without
#             peerlink's HMAC tag per frame; and --batch 64 (64 frames per write, as peerlink's write buffer sends them)
#   tcp       link-bench over TCP at --tcp-addr, which must be an Ethernet address; one write per frame and --batch 64
# There is no TCP suite at --ib-addr: no TCP is ever sent over IPoIB (the owner's rule for the test servers, 2026-11-24);
# on InfiniBand the IPoIB address serves only rdma_cm's address resolution.
#   loop      link-bench over TCP on the server's 127.0.0.1: the CPU of the stack and of the tag without a NIC
# A TCP run first checks that it can connect (a firewall that lets only ssh in): if not, it says "not measured".
# link-bench is built on each server from tools/link-bench.c and client/tsdb_crypto.c in /var/tmp/tsdb-link-bench.
# Output: OUT/raw.txt (one key=value line per run), OUT/summary.txt (mean, sd, lowest-highest per test and size).
set -u
srv= cli= ibaddr= tcpaddr= dev=mlx4_0 ibport=1 reps=3 secs=3 only=perftest,rsocket,tcp,loop out= ptport=18515
bw_sizes="64 1024 16384 65536 262144 1048576 4194304"
lat_sizes="64 1024 16384 65536 262144 1048576"
while [ $# -gt 0 ]; do
    case $1 in
    --srv) srv=$2 ;; --cli) cli=$2 ;; --ib-addr) ibaddr=$2 ;; --tcp-addr) tcpaddr=$2 ;; --dev) dev=$2 ;;
    --ib-port) ibport=$2 ;; --reps) reps=$2 ;; --secs) secs=$2 ;; --only) only=$2 ;; --out) out=$2 ;;
    --pt-port) ptport=$2 ;; --bw-sizes) bw_sizes=$2 ;; --lat-sizes) lat_sizes=$2 ;;
    *) echo "rdma-bench: unknown argument $1" >&2; exit 2 ;;
    esac
    shift 2
done
[ -n "$srv" ] && [ -n "$cli" ] && [ -n "$ibaddr" ] || { sed -n 4,9p "$0" >&2; exit 2; }
out=${out:-rdma-bench-$(date +%Y%m%d-%H%M%S)}
mkdir -p "$out"
raw=$out/raw.txt
W=/var/tmp/tsdb-link-bench
SSH="ssh -o BatchMode=yes -o ConnectTimeout=10"
root=$(cd "$(dirname "$0")/.." && pwd)
want() { case ",$only," in *",$1,"*) return 0 ;; esac; return 1; }
log() { echo "[$(date +%H:%M:%S)] $*" >&2; }

# ---- build link-bench on both servers
for h in "$srv" "$cli"; do
    tar -C "$root" -c tools/link-bench.c client/tsdb_crypto.c client/tsdb_crypto.h |
        $SSH "$h" "mkdir -p $W && tar -x -C $W && cd $W && gcc -std=gnu11 -O2 -o link-bench tools/link-bench.c \
            client/tsdb_crypto.c -ldl" || { echo "rdma-bench: cannot build link-bench on $h" >&2; exit 1; }
done
$SSH "$srv" "uname -r; lscpu | grep 'Model name'; cat /sys/class/net/*/mode 2>/dev/null | sort | uniq -c" \
    >"$out/machine.txt" 2>&1

# ---- link-bench server on $srv: prints "PORT PID"; stopped by its pid only
lb_pid=
lb_start() { # $1 bind address, $2 preload (0/1), $3 extra serve flags (--buf N); sets lb_port
    local pre=
    [ "$2" = 1 ] && pre=--rsocket
    local r
    r=$($SSH "$srv" "cd $W && rm -f pf && (setsid ./link-bench serve $pre --bind $1 --port-file pf ${3:-} \
        >serve.log 2>&1 </dev/null &) ; for i in \$(seq 100); do [ -s pf ] && break; sleep 0.1; done; cat pf")
    lb_port=${r%% *} lb_pid=${r##* }
    [ -n "$lb_port" ] && [ "$lb_port" != "$r" ] || { log "link-bench serve did not start on $1"; lb_pid=; return 1; }
}
lb_stop() {
    [ -n "$lb_pid" ] && [ "$lb_pid" -gt 1 ] 2>/dev/null && $SSH "$srv" "kill $lb_pid" 2>/dev/null
    lb_pid=
}
trap 'lb_stop' EXIT

lb_one() { # $1 mode, $2 size; uses lb_suite's locals. A failed run is a line of its own (failed=1, its exit code and
    # its stderr kept beside raw.txt), never a silently smaller n; a run whose server counted errors fails too
    local o rc
    o=$($SSH "$ch" "cd $W && timeout $((secs + 30)) ./link-bench run $cpre --host $addr --port $lb_port \
        --mode $1 --size $2 --secs $secs $t $extra --label $label" 2>"$out/lb.err")
    rc=$?
    if [ $rc = 0 ] && [ -n "$o" ]; then
        echo "$o rep=$rep" >>"$raw"
    else
        echo "label=$label mode=$1 size=$2 tag=$tag rep=$rep failed=1 rc=$rc $(echo "$o" | grep -o 'srv_errors=[0-9]*')" >>"$raw"
        cp "$out/lb.err" "$out/failed-$label-$1-$2-$tag-$rep.err"
    fi
}

lb_suite() { # $1 label, $2 server bind address, $3 address the client dials, $4 preload, $5 client host, $6.. extra
    local label=$1 bind=$2 addr=$3 pre=$4 ch=$5
    shift 5
    local extra="$*" cpre= sbuf=
    [ "$pre" = 1 ] && cpre=--rsocket
    # --buf sizes the buffers before listen and connect, so the server needs it too (an accepted socket takes them
    # from the listening one); --batch is the client's alone
    [[ " $extra " =~ \ --buf\ ([0-9]+)\  ]] && sbuf="--buf ${BASH_REMATCH[1]}"
    lb_start "$bind" "$pre" "$sbuf" || { echo "label=$label not_measured=serve_failed" >>"$raw"; return; }
    if ! $SSH "$ch" "cd $W && timeout 20 ./link-bench run $cpre --host $addr --port $lb_port --mode lat --size 64 \
        --secs 0.2 $extra" >/dev/null 2>&1; then
        log "$label: cannot connect to $addr (a firewall?): not measured"
        echo "label=$label not_measured=no_connection addr_kind=$label" >>"$raw"
        lb_stop
        return
    fi
    local rep sz tag mode
    for rep in $(seq "$reps"); do
        for tag in 0 1; do
            local t=
            [ $tag = 1 ] && t=--tag
            for sz in $bw_sizes; do lb_one bw "$sz"; done
            for sz in $lat_sizes; do lb_one lat "$sz"; done
        done
        log "$label rep $rep done"
    done
    lb_stop
}

# ---- perftest through rdma_cm: one pair of runs, server first
child_cpu() { # the second line of bash's `times`: the children's user and system time, in seconds
    awk '/^[0-9]+m[0-9.]+s [0-9]+m[0-9.]+s$/ { n++; if (n == 2) { u = $1; y = $2 } }
         END { gsub(/s/, "", u); gsub(/s/, "", y); split(u, a, "m"); split(y, b, "m");
               printf "%.3f", a[1] * 60 + a[2] + b[1] * 60 + b[2] }' "$1"
}
pt_one() { # $1 test (send_bw...), $2 size, $3 rep, $4 extra flags, $5 variant label
    local t=$1 sz=$2 rep=$3 fl=$4 var=$5 dur
    case $t in
    *_bw) # perftest's duration mode does not work with --events: iterations for about $secs at 5 GB/s
        dur=$(awk -v s="$sz" -v d="$secs" 'BEGIN { n = int(d * 5e9 / s); n = n < 5000 ? 5000 : n > 1000000 ? 1000000 : n;
            print "-n", n }') ;;
    *) dur="-n $([ "$sz" -ge 1048576 ] && echo 2000 || echo 20000)" ;;
    esac
    local cmd="ib_$t -R -d $dev -i $ibport -s $sz $dur -p $ptport -F --report_gbits $fl"
    $SSH "$srv" "timeout $((secs + 60)) bash -c '$cmd >/dev/null 2>&1; times'" >"$out/pt.srv" 2>&1 &
    local sp=$!
    sleep 1.5
    $SSH "$cli" "timeout $((secs + 60)) bash -c '$cmd $ibaddr; times'" >"$out/pt.cli" 2>&1
    wait $sp
    local line cpu_c cpu_s
    line=$(awk -v s="$sz" '$1 == s && NF >= 4 { print; exit }' "$out/pt.cli")
    cpu_c=$(child_cpu "$out/pt.cli")
    cpu_s=$(child_cpu "$out/pt.srv")
    if [ -z "$line" ]; then
        echo "label=perftest$var test=$t size=$sz rep=$rep failed=1" >>"$raw"
        cp "$out/pt.cli" "$out/failed-$t-$sz-$rep.cli"
        return
    fi
    case $t in
    *_bw) echo "$line" | awk -v t="$t" -v s="$sz" -v r="$rep" -v c="$cpu_c" -v sv="$cpu_s" -v v="$var" \
        '{ gb = $2 * s / 1e9; printf "label=perftest%s test=%s size=%s rep=%s gbit=%s mpps=%s cli_cpu_s_per_gb=%.3f srv_cpu_s_per_gb=%.3f\n", \
            v, t, s, r, $4, $5, (gb > 0 ? c / gb : 0), (gb > 0 ? sv / gb : 0) }' >>"$raw" ;;
    *) echo "$line" | awk -v t="$t" -v s="$sz" -v r="$rep" -v v="$var" \
        '{ printf "label=perftest%s test=%s size=%s rep=%s lat_typ_us=%s lat_avg_us=%s lat_p99_us=%s\n", v, t, s, r,
           $5, $6, $8 }' >>"$raw" ;;
    esac
}

if want perftest; then
    for rep in $(seq "$reps"); do
        for t in send_bw read_bw; do
            for sz in $bw_sizes; do pt_one $t "$sz" "$rep" --events -event; done
        done
        # perftest has no events for WRITE (the target is passive: its CPU is 0 by design); the writer polls
        for sz in $bw_sizes; do pt_one write_bw "$sz" "$rep" "" -poll; done
        for t in send_lat write_lat read_lat; do
            for sz in $lat_sizes; do pt_one $t "$sz" "$rep" "" -poll; done
        done
        for sz in $lat_sizes; do pt_one send_lat "$sz" "$rep" --events -event; done
        log "perftest rep $rep done"
    done
fi
want rsocket && lb_suite rsocket "$ibaddr" "$ibaddr" 1 "$cli"
want rsocket && lb_suite rsocket-buf1m "$ibaddr" "$ibaddr" 1 "$cli" --buf 1048576
[ -n "$tcpaddr" ] && want tcp && lb_suite eth-tcp "$tcpaddr" "$tcpaddr" 0 "$cli"
# frames appended to one write as peerlink's write buffer does (the rows above are a syscall per frame)
[ -n "$tcpaddr" ] && want tcp && lb_suite eth-tcp-batch "$tcpaddr" "$tcpaddr" 0 "$cli" --batch 64
want rsocket && lb_suite rsocket-batch "$ibaddr" "$ibaddr" 1 "$cli" --batch 64
want loop && lb_suite loop-tcp 127.0.0.1 127.0.0.1 0 "$srv"

# ---- summary: mean, sd and the lowest-highest run per (label, test or mode, tag, size)
awk '
/failed=1/ { nfail++ }
/not_measured|failed=1/ { print "# " $0; next }
{
    delete kv
    for (i = 1; i <= NF; i++) { split($i, p, "="); kv[p[1]] = p[2] }
    key = sprintf("%-16s %-10s tag=%s size=%-8s", kv["label"], kv["test"] != "" ? kv["test"] : kv["mode"],
                  kv["tag"] != "" ? kv["tag"] : "-", kv["size"])
    keys[key] = 1
    for (f in kv) if (f ~ /gbit|_us|cpu_s_per_gb|mpps|srv_errors/) {
        v = kv[f] + 0; k2 = key SUBSEP f; n[k2]++; s[k2] += v; q[k2] += v * v
        if (!(k2 in lo) || v < lo[k2]) lo[k2] = v
        if (!(k2 in hi) || v > hi[k2]) hi[k2] = v
        fields[f] = 1
    }
}
END {
    nk = asorti(keys, ks)
    for (i = 1; i <= nk; i++) {
        line = ks[i]
        nf = asorti(fields, fs)
        for (j = 1; j <= nf; j++) {
            k2 = ks[i] SUBSEP fs[j]
            if (!(k2 in n)) continue
            m = s[k2] / n[k2]; var = q[k2] / n[k2] - m * m; if (var < 0) var = 0
            line = line sprintf(" %s=%.3g(sd %.2g, %.3g-%.3g, n %d)", fs[j], m, sqrt(var), lo[k2], hi[k2], n[k2])
        }
        print line
    }
    if (nfail) printf "# %d failed runs (lines above starting with # ... failed=1; their stderr in failed-*)\n", nfail
}' "$raw" >"$out/summary.txt"
log "done: $out/summary.txt"
