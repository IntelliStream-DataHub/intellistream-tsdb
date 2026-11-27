#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# Validates dist/alerts/tsdb.rules.yml (R15 r15-full, E53, §15.7):
#   - Always checks file structure, alert names, and referenced metric names against server/metrics.c.
#   - When promtool is installed, runs `promtool check rules` and `promtool test rules` against recorded series
#     exercising TsdbTierFull7d (slope < 7d fires, slope > 7d quiet, already >= 95% fires), TsdbTierStoreDown
#     (DOWN states fire, UP and REPLACING quiet), and TsdbInsertsRefusedFull (1 fires, 0 quiet).
#   - When promtool is not installed, prints a skip line and exits 0 after structural checks pass.
set -eu
cd "$(dirname "$0")/.."

RULES="dist/alerts/tsdb.rules.yml"
if [ ! -f "$RULES" ]; then
    echo "test_alert_rules: FAIL: $RULES does not exist" >&2
    exit 1
fi

# Structural check: required alerts and metrics must be present and match server/metrics.c.
for alert in TsdbTierFull7d TsdbTierStoreDown TsdbInsertsRefusedFull; do
    if ! grep -q "alert: $alert" "$RULES"; then
        echo "test_alert_rules: FAIL: $RULES missing alert $alert" >&2
        exit 1
    fi
done

for metric in tsdb_tier_bytes tsdb_tier_store_state tsdb_volume_full; do
    if ! grep -q "$metric" "$RULES"; then
        echo "test_alert_rules: FAIL: $RULES does not reference metric $metric" >&2
        exit 1
    fi
    if ! grep -q "\"$metric\"" server/metrics.c; then
        echo "test_alert_rules: FAIL: server/metrics.c does not emit metric $metric" >&2
        exit 1
    fi
done

if ! command -v promtool >/dev/null 2>&1; then
    echo "test_alert_rules: promtool not installed, skipping promtool check/test (structural check ok)"
    exit 0
fi

promtool check rules "$RULES"

tmp=$(mktemp -d "${TMPDIR:-/tmp}/tsdb-alerts-XXXXXX")
trap 'rm -rf "$tmp"' EXIT

# Unit test recorded series for promtool test rules.
# Store 0: 100 GiB total (100000000000 B), growing 5 GiB/day from 70 GiB -> reaches 95 GiB in 5 days (< 7d) -> FIRES.
# Store 1: 100 GiB total, growing 0.5 GiB/day from 50 GiB -> reaches 95 GiB in 90 days (> 7d) -> QUIET.
# Store 2: 100 GiB total, flat at 96 GiB (>= 95%) -> FIRES even with 0 slope.
cat > "$tmp/test.yml" <<EOF
rule_files:
  - $(pwd)/$RULES

evaluation_interval: 5m

tests:
  - interval: 1h
    input_series:
      - series: 'tsdb_tier_bytes{store="0",use="total"}'
        values: '100000000000x30'
      - series: 'tsdb_tier_bytes{store="0",use="used"}'
        values: '70000000000+208333333x30'
      - series: 'tsdb_tier_bytes{store="1",use="total"}'
        values: '100000000000x30'
      - series: 'tsdb_tier_bytes{store="1",use="used"}'
        values: '50000000000+20833333x30'
      - series: 'tsdb_tier_bytes{store="2",use="total"}'
        values: '100000000000x30'
      - series: 'tsdb_tier_bytes{store="2",use="used"}'
        values: '96000000000x30'
      - series: 'tsdb_tier_store_state{store="0"}'
        values: '0x30'
      - series: 'tsdb_tier_store_state{store="1"}'
        values: '0x20 4x10'
      - series: 'tsdb_tier_store_state{store="2"}'
        values: '5x30'
      - series: 'tsdb_volume_full{volume="0"}'
        values: '0x24 1x6'
      - series: 'tsdb_volume_full{volume="1"}'
        values: '0x30'

    alert_rule_test:
      - eval_time: 26h
        alertname: TsdbTierFull7d
        exp_alerts:
          - exp_labels:
              severity: warning
              store: "0"
              use: used
            exp_annotations:
              summary: "tsdb tier store 0 projected to reach 95% put cap within 7 days"
              description: "Tier store 0 is projected from its 1-day growth slope to reach the 95% chunk-put cap within 7 days (or is already at or above 95%). Settles wait above 95%, causing young data on the fast disk to fill towards young_high_pct (FULL). Add tier storage or adjust retention before inserts are refused."
          - exp_labels:
              severity: warning
              store: "2"
              use: used
            exp_annotations:
              summary: "tsdb tier store 2 projected to reach 95% put cap within 7 days"
              description: "Tier store 2 is projected from its 1-day growth slope to reach the 95% chunk-put cap within 7 days (or is already at or above 95%). Settles wait above 95%, causing young data on the fast disk to fill towards young_high_pct (FULL). Add tier storage or adjust retention before inserts are refused."

      - eval_time: 22h
        alertname: TsdbTierStoreDown
        exp_alerts:
          - exp_labels:
              severity: critical
              store: "1"
            exp_annotations:
              summary: "tsdb tier store 1 is DOWN (state=4)"
              description: "Tier store 1 is not UP (state 1=missing, 2=other_id, 3=damaged, 4=ioerr, 6=stalled). Reads decode around it while within redundancy margin, but settles and chunk deletes are paused until the store recovers or is replaced via \`tsdb DIR tier lost\`."

      - eval_time: 25h
        alertname: TsdbInsertsRefusedFull
        exp_alerts:
          - exp_labels:
              severity: critical
              volume: "0"
            exp_annotations:
              summary: "tsdb volume 0 is FULL and refusing inserts"
              description: "Volume 0 filesystem fill reached young_high_pct (90% default). Inserts, appends, and replaces are refused with UNAVAILABLE (TSDB_WHY_FULL) before the WAL hits ENOSPC; deletes and drops remain allowed. Free space or restore tier settling to clear FULL (below young_high_pct - 2)."
EOF

promtool test rules "$tmp/test.yml"
echo "test_alert_rules: ok"
