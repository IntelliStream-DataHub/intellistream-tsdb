# SPDX-License-Identifier: AGPL-3.0-only
# Defaults shared by the scripts here; every one can be overridden from the environment.
#   DBCMP       root for data directories and converted inputs   (/var/tmp/dbcmp)
#   WORK        spec.tsv, q/, ref/ and res/; scripts run there     ($DBCMP/work)
#   DATA        the RowBinary input from tsdb-bench --export       ($DBCMP/input/mixed.rowbinary)
#   BIN         the programs built by this directory's Makefile    (<repo>/build/dbcmp)
#   CLICKHOUSE  a clickhouse binary for client and benchmark       (clickhouse)
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
DBCMP=${DBCMP:-/var/tmp/dbcmp}
WORK=${WORK:-$DBCMP/work}
DATA=${DATA:-$DBCMP/input/mixed.rowbinary}
BIN=${BIN:-$ROOT/build/dbcmp}
CLICKHOUSE=${CLICKHOUSE:-clickhouse}
mkdir -p "$WORK" && cd "$WORK" || exit 1
