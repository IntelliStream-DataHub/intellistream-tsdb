# VictoriaMetrics (single node, v1.154.0) for run.sh, which sources this file: start, load, settle, size, p1 and p2.
# Facts and their sources (-help of the pinned image; docs and source at tag v1.154.0): what is not default:
#   -retentionPeriod=100y         the default is 1 month: the 2024 data would be deleted
#   -search.disableCache          no cached answers between runs; without it query_range also rounds start and end
#                                 to the step for its cache (app/vmselect/promql/eval.go AdjustStartEnd)
#   -search.maxQueryDuration, -search.maxQueueDuration   30 s and 10 s by default: whole-range queries and 32
#                                 clients over -search.maxConcurrentRequests (16, its default here) would be refused
#   per core                      a second start with GOMAXPROCS=1 and -search.maxWorkersPerQuery=1, its process
#                                 tree pinned to PERCORE_CPU by taskset (pc_pin: rootless Podman has no cpuset
#                                 controller here, so --cpuset-cpus fails at container init)
# Durability: an insert is acknowledged from memory; samples reach disk within -inmemoryDataFlushInterval (5 s), and
# a part is fsynced before parts.json lists it. The timed load ends at the last acknowledgement; force_flush and
# sync -f after it are timed apart, as for the other systems that do not sync at ack.
# Values: stored as decimal mantissa and exponent with about 12 significant digits (lib/decimal FromFloat);
# -precisionBits keeps its default 64 (lower is lossy). A float32 reads back exactly as float32.
# Same timestamp: every sample is kept (no -dedup.minScrapeInterval; with it the BIGGEST value would win, not the last
# write); the release data has no duplicate timestamps.
# Load: Prometheus remote write (loadprw, as phase 4; each value is the float32's exact double) to /api/v1/write.
# InfluxDB line protocol would turn decimal text into the nearest double, not the float32's.
# Network: --network host, -httpListenAddr=127.0.0.1:8428; once more through pasta (vm.pasta.point.*: what IoTDB's
# and TDengine's -p forwarding costs per request).
# Queries: the Prometheus HTTP API on 127.0.0.1:8428 with mkq.py's PromQL (dialect vm = prom). No LTTB and no
# time-weighted average in MetricsQL (integrate() is a STEP integral without the covered time): '-'.
# Settle: force_flush, force_merge of every month partition, then until no merge runs and the files are unchanged
# for VM_STABLE s. Size: du -sb of data/small, data/big (samples) and data/indexdb (the index), summed.

VM_MEM=${VM_MEM:-$SERVER_MEM}
VM_STABLE=${VM_STABLE:-30}
VM_SETTLE_MAX=${VM_SETTLE_MAX:-10800}
VM_SAMPLES=${VM_SAMPLES:-10000} VM_SHARDS=${VM_SHARDS:-8}
VM_ARGS="-retentionPeriod=100y -search.disableCache -search.maxQueryDuration=3600s -search.maxQueueDuration=3600s"

vm_start() { # DATADIR [PODMAN ARGS...] [-- VM ARGS...]: rb-vm on 127.0.0.1:8428
    local W=$1 i pa=() va=()
    shift
    while [ $# -gt 0 ] && [ "$1" != -- ]; do pa+=("$1"); shift; done
    [ "${1:-}" = -- ] && { shift; va=("$@"); }
    mkdir -p "$W"
    # shellcheck disable=SC2086
    # host network, listening on loopback only, as every other server; VM_PASTA=1: rootless Podman's port forwarding
    # (pasta), the path IoTDB and TDengine take (their servers bind every address), to measure what it costs
    local net=(--network host) la=(-httpListenAddr=127.0.0.1:8428)
    [ "${VM_PASTA:-0}" = 1 ] && { net=(-p 127.0.0.1:8428:8428); la=(); }
    podman run -d --name rb-vm $(memlim "$VM_MEM") "${net[@]}" "${pa[@]}" \
        -v "$W:/storage" "$VM_IMG" -storageDataPath=/storage "${la[@]}" $VM_ARGS "${va[@]}" > /dev/null 9>&- || { fail "victoriametrics start"; return 1; }
    for i in $(seq 600); do
        curl -sf -m 10 127.0.0.1:8428/health 2> /dev/null | grep -q OK && return 0
        sleep 0.5
    done
    fail "victoriametrics not ready"; return 1
}
vm_get() { curl -sf -m "${CURL_MAX:-900}" "127.0.0.1:8428$1"; }
vm_metric() { # NAME-REGEX: the sum of the matching /metrics lines
    vm_get /metrics | awk -v re="^($1)[{ ]" '$0 ~ re {s += $NF} END {printf "%.0f\n", s}'
}
vm_load() { "$B/loadprw" "$1" 127.0.0.1 8428 "$VM_SAMPLES" "$VM_SHARDS"; }
vm_timed() { # FILE DATADIR: the load to its last ack, then force_flush and sync -f, timed apart
    vm_load "$1" || return 1
    local s=$(date +%s.%N)
    vm_get /internal/force_flush > /dev/null || return 1
    sync -f "$2"
    echo "force_flush and sync after the load $(echo "$(date +%s.%N) - $s" | bc) s"
}
vm_du() { podman unshare sh -c "cd '$1/data' && du -sb small big indexdb" 2> /dev/null | awk '{print $2 "\t" $1}'; }
vm_bytes() { awk -F'\t' '{s += $2} END {print s + 0}' "$1"; }
vm_settle() { # DATADIR: force_flush, force_merge of every partition, until no merge runs and the files stay the same
    local W=$1 p t0=$(date +%s) last= same=0 now m sz
    vm_get /internal/force_flush > /dev/null || { echo "force_flush failed"; return 1; }
    for p in $(podman unshare sh -c "ls '$W/data/small' '$W/data/big' 2> /dev/null" | grep -E '^[0-9]{4}_[0-9]{2}$' | sort -u); do
        vm_get "/internal/force_merge?partition_prefix=$p" > /dev/null || { echo "force_merge $p failed"; return 1; }
        echo "force_merge $p"
    done
    while :; do
        sleep 1
        now=$(date +%s)
        m=$(vm_metric 'vm_active_merges|vm_active_force_merges') || m=x
        sz=$(vm_du "$W" | tr '\n' ' ')
        if [ "$m" = 0 ] && [ "$sz" = "$last" ]; then same=$((same + 1)); else same=0; fi
        last=$sz
        [ "$same" -ge "$VM_STABLE" ] && { echo "settled after $((now - t0)) s: $sz"; return 0; }
        [ $((now - t0)) -ge "$VM_SETTLE_MAX" ] && { echo "not settled after $VM_SETTLE_MAX s: merges $m, $sz"; return 1; }
    done
}
VM_RANGE=63244800000 VM_AT=1767225599.999   # mkq.py PROM_LO .. PROM_HI (2023-12-31 to 2026-01-01)
vm_count() { vm_get "/api/v1/query?query=sum(count_over_time(sensors%5B${VM_RANGE}ms%5D))&time=$VM_AT" | jq -r '.data.result[0].value[1] // "none"'; }

p1_vm() { # a fresh server per data set, so each has its own directory to measure
    local D=$OUT/p1/vm W=$DBCMP/s1/vm ds rows got
    mkdir -p "$D"; rmdata "$W"
    printf 'ds\trows\tbytes\tnote\n' > "$OUT/p1/vm.tsv"
    for ds in $DSLIST; do
        crm rb-vm
        vm_start "$W/$ds" || return 1
        [ "$ds" = "${DSLIST%% *}" ] && ports_check "$D/ports.txt"
        vm_load "$IN/$ds.rowbinary" >> "$D/load.txt" 2>&1 9>&- || { fail "p1 vm load $ds"; continue; }
        vm_settle "$W/$ds" > "$D/settle.$ds.txt" 2>&1 || fail "p1 vm $ds did not settle (see $D/settle.$ds.txt)"
        vm_du "$W/$ds" > "$D/du.$ds.txt"
        rows=$(($(stat -c %s "$IN/$ds.rowbinary") / 16)); got=$(vm_count)
        printf '%s\t%s\t%s\tdata/{small,big,indexdb} (du -sb) after force_flush and force_merge; float64 as decimal, precisionBits 64\n' \
            "$ds" "$got" "$(vm_bytes "$D/du.$ds.txt")" >> "$OUT/p1/vm.tsv"
        [ "$got" = "$rows" ] || fail "p1 vm $ds: row count $got is not the file's $rows"
        crm rb-vm; rmdata "$W"
    done
    awk -F'\t' 'NR > 1 && ($2 == "" || $2 == "none" || $3 == 0) {bad = 1} END {exit bad}' "$OUT/p1/vm.tsv" || fail "p1 vm: a data set without rows or bytes ($OUT/p1/vm.tsv)"
    rmdata "$W"
}

p2_vm() {
    local D=$OUT/p2/vm W=$DBCMP/s2/vm r n Q="http 127.0.0.1 8428" PROM_PORT=8428
    export PROM_PORT
    mkdir -p "$D"
    for r in $(seq "$REPS"); do
        crm rb-vm; rmdata "$W"
        vm_start "$W" || return 1
        warm "$DATA"
        tstep "p2 vm load r$r" "$D/load.$r" -- vm_timed "$DATA" "$W"
        untimed "$D/settle.$r.txt" vm_settle "$W"
        vm_du "$W" > "$D/du.$r.txt"
        [ "$r" -lt "$REPS" ] && { crm rb-vm; rmdata "$W"; }
    done
    load_ok "p2 vm" || return 1
    ports_check "$D/ports.txt"
    { podman inspect rb-vm --format '{{json .Config.Cmd}}'; vm_get /metrics | grep -E '^(vm_app_version|vm_available_cpu_cores|vm_available_memory_bytes|go_gomaxprocs)'
      echo "rows $(vm_count)"; } > "$D/config.txt" 2>&1
    cd "$WORK" || return 1
    python3 -I "$H/mkq.py" vm
    if untimed "$D/check.txt" python3 -I "$H/check.py" vm 200; then
        qsuite vm vm "$D" $Q ""
    fi
    echo "lttb: MetricsQL has no LTTB" >> "$D/dash.txt"
    local k; for k in twa $TWA_KINDS $TWA_WKINDS; do
        echo "$k: MetricsQL has no time-weighted average (integrate() is a STEP integral without the covered time)" >> "$D/dash.txt"
    done
    bcheck vm "$D" && {  # bcheck writes q/vm.full$n.per.txt (mkbatch.py), so the per-series runs come after it
        for n in ${FULLS-$FULLS_DEFAULT}; do   # 100 per-series queries on 16 connections, as tsdb and ClickHouse
            reps "vm.full$n.per.c$NPER" "$D" -- "$B/qdrive" $Q "" "$WORK/q/vm.full$n.per.txt" "$NPER" 100000 0
        done
        bsuite vm vm "$D" $Q ""
    }
    ctr_alive rb-vm "p2 vm after the query suites" && ctr_mem rb-vm "$D/mem.txt"
    # the cost of pasta's forwarding, which IoTDB's and TDengine's numbers include: the same server, data and point
    # queries (the class where a per-request cost shows most) through -p 127.0.0.1:8428:8428 instead of host network
    if [ -s "$D/check.txt" ] && ! grep -qE 'mismatches [1-9]' "$D/check.txt"; then
        crm rb-vm
        if VM_PASTA=1 vm_start "$W"; then
            local t; for t in 1 $NHI; do reps "vm.pasta.point.c$t" "$D" -- "$B/qdrive" $Q "" "$WORK/q/vm.point.txt" "$t" "$SECS"; done
            ctr_alive rb-vm "p2 vm through pasta" && ctr_mem rb-vm "$D/mem.txt"
        fi
        crm rb-vm
        vm_start "$W" || return 1
    fi
    # per core: a fresh start with one Go P (GOMAXPROCS=1, which VictoriaMetrics' cgroup code keeps) and one worker per
    # query, every thread pinned to PERCORE_CPU by taskset as the other containers
    if [ -s "$D/check-batch-vm.txt" ] && ! grep -qE 'mismatches [1-9]' "$D/check-batch-vm.txt"; then
        crm rb-vm
        if vm_start "$W" -e GOMAXPROCS=1 -- -search.maxWorkersPerQuery=1 && pc_pin rb-vm; then
            { podman inspect rb-vm --format '{{json .Config.Cmd}} env {{json .Config.Env}}'
              echo "pinned to CPU $PERCORE_CPU (taskset, whole container tree): $(taskset -cp \
                  "$(podman inspect -f '{{.State.Pid}}' rb-vm)" 2>&1)"
              vm_get /metrics | grep -E '^(vm_available_cpu_cores|go_gomaxprocs)'; echo "rows $(vm_count)"; } > "$D/config.pc.txt" 2>&1
            PC=1 bsuite vm vm "$D" $Q ""
            ctr_alive rb-vm "p2 vm per core" && ctr_mem rb-vm "$D/mem.txt"
        fi
    fi
    cd "$OUT" || true
    podman logs rb-vm 2>&1 | tail -n 200 > "$D/vm.log"
    crm rb-vm; rmdata "$W"
    runstats "$D"
}
