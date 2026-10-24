#!/bin/bash
# qdb_wait.sh: wait until QuestDB has applied every WAL transaction of 'sensors' (writerTxn reaches
# sequencerTxn); rows are acknowledged on ingest but queryable only once applied.
s=$(date +%s.%N)
while :; do
  r=$(curl -sG "http://127.0.0.1:${QDB_HTTP_PORT:-9100}/exec" --data-urlencode "query=select writerTxn, sequencerTxn, suspended from wal_tables() where name='sensors'" | jq -c '.dataset[0]')
  w=$(echo "$r" | jq '.[0]'); q=$(echo "$r" | jq '.[1]'); sus=$(echo "$r" | jq '.[2]')
  [ "$sus" = "true" ] && { echo "SUSPENDED $r"; exit 1; }
  [ "$w" = "$q" ] && break
  sleep 0.1
done
e=$(date +%s.%N)
echo "WAL applied $(echo "$e - $s" | bc) s after the load returned ($r)"
