#!/bin/bash
# ts_compress.sh PAR: compress_chunk on every chunk of t100m from PAR sessions at once, then ANALYZE.
# A compression policy would do the same from one background job; parallel sessions report what
# the machine can do, and the session count belongs in any published number.
export PGPASSWORD=${PGPASSWORD:-bench}
P="psql -h 127.0.0.1 -p ${TS_PORT:-5434} -U postgres -At -q"
s=$(date +%s.%N)
$P -c "select c from show_chunks('t100m') c" | xargs -P "$1" -I{} $P -c "select compress_chunk('{}')" > /dev/null
e=$(date +%s.%N)
$P -c "ANALYZE t100m"
f=$(date +%s.%N)
echo "compress_chunk x $($P -c "select count(*) from show_chunks('t100m')") chunks with $1 sessions: $(echo "$e - $s" | bc) s, ANALYZE $(echo "$f - $e" | bc) s"
# chunk status 1 = compressed; 3 = compressed but unordered (direct compress on COPY), which needs this pass
$P -c "select status, count(*) from _timescaledb_catalog.chunk where hypertable_id = (select id from _timescaledb_catalog.hypertable where table_name='t100m') group by 1"
$P -c "select hypertable_size('t100m'), pg_size_pretty(hypertable_size('t100m'))"
