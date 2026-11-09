#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# i3_watch.sh: every 20 s, log InfluxDB 3's Parquet file count, data directory size and memory.
# Core persists the write buffer to Parquet only after --wal-files-per-snapshot (600) WAL files or
# --force-snapshot-max-age (1 h), so after a bulk load this is how to see the snapshot happen.
# Stop it with kill; the files belong to the container's user, hence podman unshare.
source "$(dirname "$0")/env.sh"
D=$DBCMP/influxdb3
while :; do
  n=$(podman unshare find "$D" -name "*.parquet" | wc -l)
  sz=$(podman unshare du -sb "$D" | cut -f1)
  echo "$(date -u +%T) parquet=$n bytes=$sz mem=$(podman stats --no-stream --format '{{.MemUsage}}' dbcmp-influx3 | cut -d/ -f1)"
  sleep 20
done
