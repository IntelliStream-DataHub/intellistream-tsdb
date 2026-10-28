# Apache IoTDB (2.0, table model) for run.sh, which sources this file: start, load, settle, size, p1 and p2.
# Facts and their sources: bench/dbcmp/README.md (IoTDB) and the builder's findings; what is not default:
#   wal_mode=SYNC        the WAL is fsynced before a write is acknowledged (default ASYNC, 1 s): as tsdb in sync mode
#   compaction_write_throughput_mb_per_sec=0   no cap on compaction writes (default 16 MB/s; <= 0 means no limit,
#                        CompactionTaskManager), so settling measures the compaction, not the cap
#   query_timeout_threshold=3600000             whole-range queries over 100 M rows may take longer than 60 s
#   enable_rest_service=true, rest_query_default_row_size_limit=2000000   the REST API (18080) that qdrive drives
#   query memory proportions: IoTDB's defaults. 32 concurrent window queries (lead() OVER, the TWA classes twa1d and
#                        twa7d) are refused with code 719 "not enough memory to execute current fragment instance"
#                        at a 4 GB and a 16 GB heap, and with the Operators share at 4x its default
#                        (chunk_timeseriesmeta_free_memory_proportion=1:100:200:50:800:200:200:50:250): the estimate
#                        per fragment grows with the pool (4.4, 16, 34 MB). Those runs are reported as refused
#                        (QREFUSE, REFUSED.txt), not timed. IOT_QMEM sets the proportion for a trial.
#   compressor=ZSTD      only in the iotz variant (default LZ4); float encoding stays GORILLA (lossless; never RLE or
#                        TS_2DIFF on floats: they round to float_precision=2)
# The REST service binds every address and has no bind setting, so IoTDB runs in its own network namespace
# (podman's default, pasta) with its two ports published on 127.0.0.1 only; every request crosses that forwarding.
IOT_HDR=$'Authorization: Basic cm9vdDpyb290\r\n'   # root:root, the default user
IOT_MEM=${IOT_MEM:-24g}                             # container limit, as InfluxDB 2's 24g; heap below
IOT_JMX=${IOT_JMX:--Xms16G -Xmx16G -XX:MaxDirectMemorySize=4G}
IOT_QMEM=${IOT_QMEM-}   # empty: IoTDB's default; else BloomFilter:ChunkCache:TSMetaCache:Coordinator:Operators:
                        # DataExchange:timeIndex:..:Subscription (default 1:100:200:50:200:200:200:50:250)
IOT_STABLE=${IOT_STABLE:-180}                       # settle: TsFiles unchanged this long (3 compaction schedules)
IOT_SETTLE_MAX=${IOT_SETTLE_MAX:-10800}
IOT_PROCS=${IOT_PROCS:-8} IOT_TABLET=${IOT_TABLET:-100000}

iot_comp() { [ "${IOT_Z:-0}" = 1 ] && echo ZSTD || echo "${IOT_COMP:-LZ4}"; }
iot_start() { # DATADIR [PODMAN ARGS...]: rb-iot; IOT_Z=1 for compressor=ZSTD, else IOT_COMP; IOT_ENC: float encoding
    local W=$1 i z="-e compressor=$(iot_comp) -e default_float_encoding=${IOT_ENC:-GORILLA}"
    shift
    [ -n "$IOT_QMEM" ] && z="$z -e chunk_timeseriesmeta_free_memory_proportion=$IOT_QMEM"
    mkdir -p "$W/data" "$W/logs"
    podman run -d --name rb-iot --memory "$IOT_MEM" --memory-swap "$IOT_MEM" \
        -p 127.0.0.1:18080:18080 -p 127.0.0.1:6667:6667 \
        -e enable_rest_service=true -e rest_query_default_row_size_limit=2000000 -e wal_mode=SYNC \
        -e query_timeout_threshold=3600000 -e compaction_write_throughput_mb_per_sec=0 -e dn_rpc_address=0.0.0.0 \
        -e IOTDB_JMX_OPTS="$IOT_JMX" $z "$@" \
        -v "$W/data:/iotdb/data" -v "$W/logs:/iotdb/logs" "$IOT_IMG" > /dev/null 9>&- || { fail "iotdb start"; return 1; }
    for i in $(seq 600); do curl -sf 127.0.0.1:18080/ping > /dev/null && return 0; sleep 0.5; done
    fail "iotdb not ready"; return 1
}
iot_nq() { # DATABASE|"" SQL: a statement through /rest/table/v1/nonQuery; fails unless IoTDB answers code 200
    local body
    if [ -n "$1" ]; then body=$(jq -cn --arg d "$1" --arg s "$2" '{database: $d, sql: $s}'); else body=$(jq -cn --arg s "$2" '{sql: $s}'); fi
    curl -s -H "${IOT_HDR%$'\r\n'}" -H 'Content-Type: application/json' -d "$body" http://127.0.0.1:18080/rest/table/v1/nonQuery |
        tee -a "${IOT_DDL_LOG:-/dev/null}" | grep -q '"code":200'
}
iot_ddl() { # DATABASE TABLE
    iot_nq "" "CREATE DATABASE $1" && iot_nq "$1" "CREATE TABLE $2 (time TIMESTAMP TIME, series STRING TAG, value FLOAT FIELD)"
}
iot_load() { # FILE DATABASE TABLE: the Python TableSession loader (tablets over RPC 6667)
    "$IOT_VENV/bin/python" -I "$RDIR/iotload.py" "$1" 127.0.0.1 6667 "$2" "$3" "$IOT_TABLET" "$IOT_PROCS"
}
iot_files() { # DATADIR: "count bytes" of the TsFiles and their .resource/.mods files (sequence and unsequence)
    podman unshare find "$1/data/datanode/data" -type f -printf '%s\n' 2> /dev/null | awk '{n++; s += $1} END {print n + 0, s + 0}'
}
iot_settle() { # DATADIR: FLUSH, then until the TsFiles are unchanged for IOT_STABLE s and no compaction is in flight
    local W=$1 t0 last= cur changed busy
    t0=$(date +%s); changed=$t0
    iot_nq "" "FLUSH" || echo "FLUSH did not answer 200"
    while :; do
        cur=$(iot_files "$W")
        busy=$(podman unshare find "$W/data/datanode/data" -type f \( -name '*.inner' -o -name '*.cross' -o -name '*.compaction.log' -o -name '*.target' \) 2> /dev/null | wc -l)
        echo "$(date -u +%T) +$(($(date +%s) - t0))s files(count bytes)=$cur compaction-files=$busy"
        [ "$cur" != "$last" ] && { last=$cur; changed=$(date +%s); }
        if [ "$busy" = 0 ] && [ $(($(date +%s) - changed)) -ge "$IOT_STABLE" ]; then
            echo "settled: last change $((changed - t0)) s after FLUSH ($cur)"; return 0
        fi
        [ $(($(date +%s) - t0)) -ge "$IOT_SETTLE_MAX" ] && { echo "NOT settled after $IOT_SETTLE_MAX s ($cur)"; return 1; }
        sleep 10
    done
}
iot_du() { # DATADIR: du -sb of every database directory under sequence/ and unsequence/, and of the WAL
    podman unshare sh -c "cd '$1/data/datanode' && du -sb data/sequence/* data/unsequence/* wal 2> /dev/null"
}

p1_iotz() { IOT_Z=1 p1_iot; }   # compressor=ZSTD: sizes in p1/iot-zstd.tsv
# sizes only: the other lossless float encoding (CHIMP) and the strongest compressor (LZMA2), for IoTDB's smallest
p1_iotc() { IOT_ENC=CHIMP IOT_COMP=ZSTD p1_iot; }      # p1/iot-chimp-zstd.tsv
p1_iotx() { IOT_COMP=LZMA2 p1_iot; }                   # p1/iot-gorilla-lzma2.tsv
p1_iotcx() { IOT_ENC=CHIMP IOT_COMP=LZMA2 p1_iot; }    # p1/iot-chimp-lzma2.tsv
p1_iot() { # a database per data set (s_<ds>, table sensors): IoTDB's files are per database, so du separates them
    local v=iot; [ "${IOT_Z:-0}" = 1 ] && v=iot-zstd
    [ -n "${IOT_ENC:-}${IOT_COMP:-}" ] && v=iot-$(echo "${IOT_ENC:-GORILLA}-$(iot_comp)" | tr A-Z a-z)
    local D=$OUT/p1/$v W=$DBCMP/s1/$v ds db
    mkdir -p "$D"; rmdata "$W"
    IOT_DDL_LOG=$D/ddl.txt iot_start "$W" || return 1
    ports_check "$D/ports.txt"
    for ds in $DSLIST; do
        db=s_${ds,,}   # IoTDB database names are lowercase
        IOT_DDL_LOG=$D/ddl.txt iot_ddl "$db" sensors || { fail "p1 $v ddl $ds"; continue; }
        iot_load "$IN/$ds.rowbinary" "$db" sensors >> "$D/load.txt" 2>&1 9>&- || fail "p1 $v load $ds"
    done
    iot_settle "$W" > "$D/settle.txt" 2>&1 || fail "p1 $v did not settle (see $D/settle.txt)"
    iot_du "$W" > "$D/du.txt"
    printf 'ds\trows\tbytes\tnote\n' > "$OUT/p1/$v.tsv"
    for ds in $DSLIST; do
        curl -s -H "${IOT_HDR%$'\r\n'}" -H 'Content-Type: application/json' http://127.0.0.1:18080/rest/table/v1/query \
            -d "{\"database\":\"s_${ds,,}\",\"sql\":\"SELECT count(value) FROM sensors\"}" > "$D/count.$ds.json"
        awk -v ds="$ds" -v db="s_${ds,,}" -v rows="$(jq -r '.values[0][0]' "$D/count.$ds.json" 2> /dev/null)" \
            -v z="${IOT_ENC:-GORILLA} + $(iot_comp)" '
            $2 == "data/sequence/" db {s = $1} $2 == "data/unsequence/" db {u = $1}
            END {printf "%s\t%s\t%d\tTsFiles (sequence %d + unsequence %d, du -sb) after FLUSH and compaction; %s; WAL apart\n", ds, rows, s + u, s, u, z}' \
            "$D/du.txt" >> "$OUT/p1/$v.tsv"
        [ "$(jq -r '.values[0][0]' "$D/count.$ds.json" 2> /dev/null)" = $(($(stat -c %s "$IN/$ds.rowbinary") / 16)) ] ||
            fail "p1 $v $ds: row count $(head -c 200 "$D/count.$ds.json") is not the file's $(($(stat -c %s "$IN/$ds.rowbinary") / 16))"
    done
    awk -F'\t' 'NR > 1 && ($2 == "" || $2 == "null" || $3 == 0) {bad = 1} END {exit bad}' "$OUT/p1/$v.tsv" || fail "p1 $v: a data set without rows or bytes ($OUT/p1/$v.tsv)"
    crm rb-iot; rmdata "$W"
}

iot_timed() { # one timed load into a fresh rb-iot: the loader, then (untimed by the caller) settle
    iot_load "$DATA" cmp sensors
}
p2_iotz() { IOT_Z=1 p2_iot; }
p2_iot() {
    local v=iot; [ "${IOT_Z:-0}" = 1 ] && v=iot-zstd
    local D=$OUT/p2/$v W=$DBCMP/s2/$v r n Q="http 127.0.0.1 18080"
    # IoTDB's REST service answers a failed query with status 200 and {"code":...}: qdrive fails on that prefix
    local QDRIVE_ERR='{"code":'; export QDRIVE_ERR
    # code 719: IoTDB refuses a fragment whose estimated operator memory exceeds what is left of its pool. 32
    # concurrent lead()-window TWA queries (twa1d, twa7d) hit it at a 16 GB heap with the Operators share at 4x the
    # default (smokes iot2-iot4: the estimate grows with the pool); reported as refused at that concurrency
    local QREFUSE='"code":719'; export QREFUSE
    mkdir -p "$D"
    for r in $(seq "$REPS"); do
        crm rb-iot; rmdata "$W"
        IOT_DDL_LOG=$D/ddl.$r.txt iot_start "$W" || return 1
        IOT_DDL_LOG=$D/ddl.$r.txt iot_ddl cmp sensors || { fail "p2 $v ddl"; return 1; }
        warm "$DATA"
        tstep "p2 $v load r$r" "$D/load.$r" -- iot_timed
        untimed "$D/settle.$r.txt" iot_settle "$W"
        iot_du "$W" > "$D/du.$r.txt"
        [ "$r" -lt "$REPS" ] && { crm rb-iot; rmdata "$W"; }
    done
    load_ok "p2 $v" || return 1
    ports_check "$D/ports.txt"
    podman exec rb-iot sh -c 'ls /iotdb/lib | grep -m1 iotdb-server' > "$D/version.txt" 2>&1
    # how it ran: every setting passed to the container (the image's entrypoint writes them into iotdb-system.properties)
    podman inspect rb-iot --format '{{range .Config.Env}}{{println .}}{{end}}' |
        grep -E '^(IOTDB_JMX_OPTS|[a-z_]+)=' > "$D/config.txt" 2>&1
    cd "$WORK" || return 1
    python3 -I "$H/mkq.py" iot
    if untimed "$D/check.txt" python3 -I "$H/check.py" iot 200; then
        qsuite iot "$v" "$D" $Q "$IOT_HDR"
    fi
    echo "lttb: IoTDB has no LTTB function" >> "$D/dash.txt"
    bcheck iot "$D" && {  # bcheck writes q/iot.full$n.per.txt (mkbatch.py), so the per-series runs come after it
        for n in ${FULLS-$FULLS_DEFAULT}; do   # 100 per-series queries on 16 connections, as tsdb and ClickHouse
            reps "$v.full$n.per.c16" "$D" -- "$B/qdrive" $Q "$IOT_HDR" "$WORK/q/iot.full$n.per.txt" 16 100000 0
        done
        bsuite iot "$v" "$D" $Q "$IOT_HDR"
    }
    REL q iot twa > "$D/q.twa.http"
    untimed "$D/check-twa.txt" REL check iot twa ref/twa.jsonl && qline twa "$D" "$D/q.twa.http" $Q "$IOT_HDR"
    twasuite iot "$D" $Q "$IOT_HDR"
    # per core: the same data, restarted with one query thread and one pipeline driver per fragment, a JVM that sizes
    # its pools for one CPU, and pinned to PERCORE_CPU (taskset in front of a rootless podman run does not reach the
    # container: the processes are made by the pause process, so pc_pin pins them once it runs)
    crm rb-iot
    if IOT_JMX="$IOT_JMX -XX:ActiveProcessorCount=1" iot_start "$W" -e query_thread_count=1 \
        -e degree_of_query_parallelism=1 && pc_pin rb-iot; then
        [ -s "$D/check-batch-iot.txt" ] && ! grep -qE 'mismatches [1-9]' "$D/check-batch-iot.txt" && PC=1 bsuite iot "$v" "$D" $Q "$IOT_HDR"
    fi
    cd "$OUT" || true
    crm rb-iot
    # the DataNode's error log goes with the results (out-of-memory refusals, failed queries), then the data goes
    podman unshare sh -c 'tail -n 300 "$1"/logs/log_datanode_error.log' sh "$W" > "$D/iotdb-error.log" 2> /dev/null
    rmdata "$W"
    runstats "$D"
}
