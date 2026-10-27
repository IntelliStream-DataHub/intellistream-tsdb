# TDengine (3.4, community) for run.sh, which sources this file: start, load, settle, size, p1 and p2.
# Facts and their sources (docs at tag ver-3.4.2.8): the builder's FINDINGS-tdengine-builder.md; what is not default:
#   CREATE DATABASE ... WAL_LEVEL 2 WAL_FSYNC_PERIOD 0   the WAL is fsynced before a write is acknowledged (default
#                        WAL_LEVEL 1: written, not synced): as tsdb in sync mode, PostgreSQL and InfluxDB
#   PRECISION 'ms'       the default too; written out because every query and the loader speak milliseconds
#   TAOS_DISABLE_KEEPER, _EXPLORER, _TAOSX = 1   the image's entrypoint starts taosKeeper (metrics), taosExplorer (web
#                        UI) and taosX next to taosd and taosAdapter by default; they serve nothing here and would
#                        compete for the CPU
#   tdz variant          ALTER STABLE sensors MODIFY COLUMN value COMPRESS 'zstd' LEVEL 'high' before the load (the
#                        default for FLOAT is ENCODE 'bss' COMPRESS 'lz4' LEVEL 'medium'); never the lossy 'tsz'
# Load: InfluxDB line protocol through taosAdapter (POST /influxdb/v1/write, schemaless; the value's f32 suffix makes a
# FLOAT column; conv tdlp), the way QuestDB and the InfluxDBs are loaded. TDengine documents a faster bulk path
# (stmt over WebSocket with asyncWrite, developer guide "high throughput"); its load times may favour that path.
# Queries: REST (POST /rest/sql/cmp, Basic root:taosdata) on taosAdapter's 6041, published on 127.0.0.1 only;
# taosd's own port 6030 is not published (taosAdapter reaches it inside the container).
# Settle: TDengine has no automatic compaction in the community edition (COMPACT_INTERVAL is Enterprise), so the
# settle is FLUSH DATABASE, COMPACT DATABASE, and SHOW COMPACTS empty with the vnode files unchanged for TD_STABLE s.
TD_HDR=$'Authorization: Basic cm9vdDp0YW9zZGF0YQ==\r\n'   # root:taosdata, the default user
TD_MEM=${TD_MEM:-24g}
TD_STABLE=${TD_STABLE:-60}
TD_SETTLE_MAX=${TD_SETTLE_MAX:-10800}
TD_PROCS=${TD_PROCS:-8} TD_LINES=${TD_LINES:-100000}

td_start() { # DATADIR [PODMAN ARGS...]: rb-td
    local W=$1 i
    shift
    mkdir -p "$W/data" "$W/log"
    podman run -d --name rb-td --memory "$TD_MEM" --memory-swap "$TD_MEM" -p 127.0.0.1:6041:6041 \
        -e TAOS_DISABLE_KEEPER=1 -e TAOS_DISABLE_EXPLORER=1 -e TAOS_DISABLE_TAOSX=1 "$@" \
        -v "$W/data:/var/lib/taos" -v "$W/log:/var/log/taos" "$TD_IMG" > /dev/null 9>&- || { fail "tdengine start"; return 1; }
    for i in $(seq 600); do
        curl -s -m 10 -H "${TD_HDR%$'\r\n'}" 127.0.0.1:6041/rest/sql -d 'SELECT server_version()' 2> /dev/null | grep -q '"code":0' && return 0
        sleep 0.5
    done
    fail "tdengine not ready"; return 1
}
td_sql() { # DATABASE|"" SQL: through REST; fails unless TDengine answers code 0; the answer to stdout
    local out
    out=$(curl -s -m "${CURL_MAX:-900}" -H "${TD_HDR%$'\r\n'}" "127.0.0.1:6041/rest/sql${1:+/$1}" -d "$2")
    echo "$out" >> "${TD_DDL_LOG:-/dev/null}"
    echo "$out"
    case $out in *'"code":0,'*) return 0 ;; esac
    return 1
}
td_ddl() { # DATABASE: the database; TD_Z=1 also makes the supertable with zstd on the value column
    td_sql "" "CREATE DATABASE $1 WAL_LEVEL 2 WAL_FSYNC_PERIOD 0 PRECISION 'ms'" > /dev/null || return 1
    if [ "${TD_Z:-0}" = 1 ]; then  # the supertable schemaless would make (DESCRIBE after a schemaless load), with zstd
        td_sql "$1" "CREATE STABLE sensors (_ts TIMESTAMP, value FLOAT COMPRESS 'zstd' LEVEL 'high') TAGS (\`series\` NCHAR(16))" > /dev/null || return 1
    fi
}
td_load() { # FILE DATABASE: line protocol through taosAdapter
    "$B/loadlp" "$1" 127.0.0.1 6041 "/influxdb/v1/write?db=$2&precision=ms" "$TD_HDR" "$TD_LINES" "$TD_PROCS"
}
td_input() { # ROWBINARY: its tdlp next to it (untimed conversion)
    local f=${1%.rowbinary}.tdlp
    [ -s "$f" ] || "$B/conv" tdlp "$1" "$f" || return 1
    echo "$f"
}
td_files() { podman unshare find "$1/data/vnode" -type f -printf '%s\n' 2> /dev/null | awk '{n++; s += $1} END {print n + 0, s + 0}'; }
td_settle() { # DATADIR DATABASE...: FLUSH and COMPACT each, then until no compaction runs and the files are unchanged
    local W=$1 t0 last= cur changed busy db
    shift
    t0=$(date +%s)
    for db; do
        td_sql "" "FLUSH DATABASE $db" > /dev/null || echo "FLUSH DATABASE $db failed"
        td_sql "" "COMPACT DATABASE $db" | head -c 300; echo
    done
    changed=$(date +%s)
    while :; do
        cur=$(td_files "$W")
        busy=$(td_sql "" "SHOW COMPACTS" | jq -r '.rows // 0' 2> /dev/null)
        echo "$(date -u +%T) +$(($(date +%s) - t0))s files(count bytes)=$cur compactions=$busy"
        [ "$cur" != "$last" ] && { last=$cur; changed=$(date +%s); }
        if [ "$busy" = 0 ] && [ $(($(date +%s) - changed)) -ge "$TD_STABLE" ]; then
            echo "settled: last change $((changed - t0)) s after FLUSH ($cur)"; return 0
        fi
        [ $(($(date +%s) - t0)) -ge "$TD_SETTLE_MAX" ] && { echo "NOT settled after $TD_SETTLE_MAX s ($cur)"; return 1; }
        sleep 5
    done
}
td_du() { # DATADIR DATABASE: "vgroup subdir bytes" for every vnode of DATABASE (SHOW db.VGROUPS: vnode<vgroup_id>)
    local W=$1 db=$2 vg
    for vg in $(td_sql "" "SHOW $db.VGROUPS" | jq -r '.data[][0]'); do
        podman unshare sh -c "cd '$W/data/vnode/vnode$vg' && du -sb *" 2> /dev/null | awk -v v="$vg" '{print v "\t" $2 "\t" $1}'
    done
}
td_bytes() { # DU-FILE: data bytes: everything of the vnodes but the WAL (tsdb files, meta, sync state)
    awk -F'\t' '$2 != "wal" {s += $3} END {print s + 0}' "$1"
}

p1_tdz() { TD_Z=1 p1_td; }   # zstd high on the value column: sizes in p1/td-zstd.tsv
p1_td() { # a database per data set (s_<ds>): its vnodes are its own, so du separates them
    local v=td; [ "${TD_Z:-0}" = 1 ] && v=td-zstd
    local D=$OUT/p1/$v W=$DBCMP/s1/$v ds db f
    mkdir -p "$D"; rmdata "$W"
    TD_DDL_LOG=$D/ddl.txt td_start "$W" || return 1
    ports_check "$D/ports.txt"
    for ds in $DSLIST; do
        db=s_${ds,,}
        TD_DDL_LOG=$D/ddl.txt td_ddl "$db" || { fail "p1 $v ddl $ds"; continue; }
        f=$(td_input "$IN/$ds.rowbinary") || { fail "p1 $v conv $ds"; continue; }
        td_load "$f" "$db" >> "$D/load.txt" 2>&1 9>&- || fail "p1 $v load $ds"
    done
    td_settle "$W" $(for ds in $DSLIST; do echo "s_${ds,,}"; done) > "$D/settle.txt" 2>&1 || fail "p1 $v did not settle (see $D/settle.txt)"
    printf 'ds\trows\tbytes\tnote\n' > "$OUT/p1/$v.tsv"
    for ds in $DSLIST; do
        db=s_${ds,,}
        td_du "$W" "$db" > "$D/du.$ds.txt"
        td_sql "$db" "SELECT count(value) FROM sensors" > "$D/count.$ds.json"
        td_sql "$db" "DESCRIBE sensors" > "$D/describe.$ds.json"
        printf '%s\t%s\t%s\tvnode files but the WAL (du -sb) after FLUSH and COMPACT; value %s\n' "$ds" \
            "$(jq -r '.data[0][0]' "$D/count.$ds.json" 2> /dev/null)" "$(td_bytes "$D/du.$ds.txt")" \
            "$(jq -r '[.data[] | select(.[0] == "value") | .[4:7] | join(" ")][0]' "$D/describe.$ds.json" 2> /dev/null)" >> "$OUT/p1/$v.tsv"
        [ "$(jq -r '.data[0][0]' "$D/count.$ds.json" 2> /dev/null)" = $(($(stat -c %s "$IN/$ds.rowbinary") / 16)) ] ||
            fail "p1 $v $ds: row count $(head -c 200 "$D/count.$ds.json") is not the file's $(($(stat -c %s "$IN/$ds.rowbinary") / 16))"
    done
    awk -F'\t' 'NR > 1 && ($2 == "" || $2 == "null" || $3 == 0) {bad = 1} END {exit bad}' "$OUT/p1/$v.tsv" || fail "p1 $v: a data set without rows or bytes ($OUT/p1/$v.tsv)"
    crm rb-td; rmdata "$W"
}

td_timed() { td_load "$TD_LP" cmp; }   # one timed load into a fresh rb-td; settle untimed by the caller
p2_tdz() { TD_Z=1 p2_td; }
p2_td() {
    local v=td; [ "${TD_Z:-0}" = 1 ] && v=td-zstd
    local D=$OUT/p2/$v W=$DBCMP/s2/$v r n Q="http 127.0.0.1 6041"
    # taosAdapter answers an error with HTTP 4xx/5xx and {"code":N,"desc":...}; a success also has "code" (0)
    local QDRIVE_ERR_SUB='"desc":'; export QDRIVE_ERR_SUB
    mkdir -p "$D"
    TD_LP=$(td_input "$DATA") || { fail "p2 $v conv"; return 1; }
    for r in $(seq "$REPS"); do
        crm rb-td; rmdata "$W"
        TD_DDL_LOG=$D/ddl.$r.txt td_start "$W" || return 1
        TD_DDL_LOG=$D/ddl.$r.txt td_ddl cmp || { fail "p2 $v ddl"; return 1; }
        warm "$TD_LP"
        tstep "p2 $v load r$r" "$D/load.$r" -- td_timed
        untimed "$D/settle.$r.txt" td_settle "$W" cmp
        td_du "$W" cmp > "$D/du.$r.txt"
        [ "$r" -lt "$REPS" ] && { crm rb-td; rmdata "$W"; }
    done
    ports_check "$D/ports.txt"
    td_sql "" "SELECT server_version()" > "$D/version.txt"
    { podman inspect rb-td --format '{{range .Config.Env}}{{println .}}{{end}}' | grep -E '^TAOS_'
      td_sql "" "SHOW DATABASES" | jq -c '.data[] | select(.[0] == "cmp")'
      td_sql cmp "DESCRIBE sensors" | jq -c '.data'; } > "$D/config.txt" 2>&1
    cd "$WORK" || return 1
    python3 -I "$H/mkq.py" td
    if untimed "$D/check.txt" python3 -I "$H/check.py" td 200; then
        qsuite td "$v" "$D" $Q "$TD_HDR"
        for n in ${FULLS-$FULLS_DEFAULT}; do   # 100 per-series queries on 16 connections, as tsdb and ClickHouse
            reps "$v.full$n.per.c16" "$D" -- "$B/qdrive" $Q "$TD_HDR" "$WORK/q/td.full$n.per.txt" 16 100000 0
        done
    fi
    echo "lttb: TDengine has no LTTB function" >> "$D/dash.txt"
    bcheck td "$D" && bsuite td "$v" "$D" $Q "$TD_HDR"
    REL q td twa > "$D/q.twa.http"
    untimed "$D/check-twa.txt" REL check td twa ref/twa.jsonl && qline twa "$D" "$D/q.twa.http" $Q "$TD_HDR"
    ctr_alive rb-td "p2 $v after the query suites" && twasuite td "$D" $Q "$TD_HDR"
    ctr_mem rb-td "$D/mem.txt"
    # per core: taosd and taosAdapter pinned to PERCORE_CPU, with TDengine's own thread settings left as they are.
    # numOfVnodeQueryThreads 1 (ALTER DNODE + restart: since 3.4.0 taosd reads taos.cfg and TAOS_* only at its first
    # start, taosd.md "Additional Notes" 3) deadlocked in the 2026-10-27 smoke run: a 10-series batch (UNION ALL of
    # PARTITION BY ... INTERVAL(60s) queries) stayed 85 minutes in SHOW QUERIES with its merge tasks in "retry" and its
    # scans "partial_succeed", answering nothing. TD_PC_THREADS=N sets it anyway (N >= 2 untested).
    local pc=1 i n
    if [ -n "${TD_PC_THREADS:-}" ]; then
        pc=0
        if ctr_alive rb-td "p2 $v before per core" &&
            td_sql "" "ALTER DNODE 1 'numOfVnodeQueryThreads' '$TD_PC_THREADS'" > "$D/config.pc.txt" &&
            { crm rb-td; td_start "$W"; }; then
            for i in $(seq 60); do
                td_sql "" "SHOW DNODE 1 VARIABLES LIKE 'numOfVnodeQueryThreads'" > "$D/config.pc.show.txt"
                jq -e --arg n "$TD_PC_THREADS" '.data[] | select(.[1] == "numOfVnodeQueryThreads") | .[2] == $n' \
                    "$D/config.pc.show.txt" > /dev/null && { pc=1; break; }
                sleep 0.5
            done
            [ "$pc" = 1 ] || fail "p2 $v per core: numOfVnodeQueryThreads is not $TD_PC_THREADS (see $D/config.pc.show.txt)"
        fi
    else
        ctr_alive rb-td "p2 $v before per core" || pc=0
    fi
    if [ "$pc" = 1 ]; then
        td_sql "" "SHOW DNODE 1 VARIABLES LIKE 'numOf%'" >> "$D/config.pc.txt"
        n=$(podman exec rb-td sh -c 'for t in /proc/$(pidof taosd)/task/*; do cat "$t/comm"; done' | grep -c '^vnode-query')
        { echo; echo "taosd vnode-query threads: $n; pinned to CPU $PERCORE_CPU (taskset, whole container tree)"; } >> "$D/config.pc.txt"
    fi
    if [ "$pc" = 1 ] && pc_pin rb-td; then
        [ -s "$D/check-batch-td.txt" ] && ! grep -qE 'mismatches [1-9]' "$D/check-batch-td.txt" && PC=1 bsuite td "$v" "$D" $Q "$TD_HDR"
        ctr_alive rb-td "p2 $v per core" && ctr_mem rb-td "$D/mem.txt"
    fi
    cd "$OUT" || true
    crm rb-td
    podman unshare sh -c 'tail -n 300 "$1"/log/taosdlog.0' sh "$W" > "$D/taosd.log" 2> /dev/null
    rmdata "$W"
    runstats "$D"
}
