#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# jepsen's outage check must not take "the server says its probes were slow" for an excuse: a tsdbd whose health
# monitor, while a flapping domain is DOWN, reports every write probe slow (more than fast_ms, with no fault and a
# healthy disk) never lets the domain out of DOWN. How soon writes resume after a heal is LIVENESS (round 11: on a machine at a load
# average of 200 the write probes took 7.5 s and an honest run failed on this as a violation), so the run must end INCONCLUSIVE (exit 3,
# check outage-heal) after OUTAGE_HARD_S, naming the slow-probe lines (none of them has a slow fdatasync of jepsen's own near it),
# instead of being waited for again after every line (the verifier's P5: judged valid after 584 s), and must not be a violation. A tsdbd
# whose volume the server itself reports as failed for good (TOPOLOGY: UNAVAILABLE, reason DIVERGED; a second mutant, which latches
# that for every volume once its domain went DOWN) and never takes a write again is a VIOLATION (exit 1): data lost, not a slow disk.
# Builds the mutants from copies of the tree (git archive of HEAD, under $TMPDIR or /var/tmp), never touches the
# working tree. Usage: tests/jepsen/mutant-slowprobe.sh JEPSEN_DIR [SEED] [SECONDS]   (default seed 2, 120 s: that
# schedule has flapping heals) (JEPSEN_BIN names another jepsen binary than build/jepsen)
set -u
dir=${1:?usage: mutant-slowprobe.sh JEPSEN_DIR [SEED] [SECONDS]}
seed=${2:-2}
secs=${3:-120}
root=$(cd "$(dirname "$0")/../.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-/var/tmp}/mutant-slowprobe.XXXXXX") || exit 2
git -C "$root" archive HEAD | tar -x -C "$tmp" || exit 2
cp -r "$tmp" "$tmp.div" || exit 2
sed -i 's|^    int ms = (int)(now - start);$|&\n    if (write \&\& d->state == TSDB_DOM_DOWN \&\& down_need_ms(d, now) > h->o.recover_ms) ms = h->o.fast_ms + 1; /* MUTANT */|' \
    "$tmp/src/health.c"
if ! grep -q 'MUTANT' "$tmp/src/health.c"; then echo "mutant-slowprobe: the mutant did not apply"; rm -rf "$tmp" "$tmp.div"; exit 1; fi
sed -i 's|^    bool diverged = v < DOM_MAX_VOLS \&\& atomic_load(\&dom.vol\[v\].diverged);$|&\n    if (di.state == TSDB_DOM_DOWN) atomic_store(\&jv_div, true); diverged = diverged \|\| atomic_load(\&jv_div); /* MUTANT */|' \
    "$tmp.div/server/tsdbd.c"
sed -i '0,/^#include <fcntl.h>$/s//#include <fcntl.h>\
static _Atomic _Bool jv_div; \/* MUTANT: the first DOWN of a domain makes every volume DIVERGED for good, and every insert UNAVAILABLE *\//' "$tmp.div/server/tsdbd.c"
# every write the clients send is refused: an insert, a delete and a replace (jepsen counts a delete or replace as a write that a
# volume takes, too, so a mutant that refused only inserts would look healed)
sed -i 's/^    case TSDB_OP_\(INSERT\|INSERT_VALUES\|DELETE_RANGE\|REPLACE_RANGE\): {$/&\
        if (atomic_load(\&jv_div)) { rc = TSDB_ERR_UNAVAILABLE; break; } \/* MUTANT *\//' "$tmp.div/server/tsdbd.c"
# the insert the clients send is the packed one (INSERT_VALUES with TSDB_PACKED, through insert_packed): refuse there too
sed -i '/^static int insert_packed(auth_conn \*a, const uint8_t \*p, size_t len, uint32_t n, tsdb_sid s)$/{n;s/^{$/{\
    if (atomic_load(\&jv_div)) return TSDB_ERR_UNAVAILABLE; \/* MUTANT *\//}' "$tmp.div/server/tsdbd.c"
if [ "$(grep -c '/\* MUTANT' "$tmp.div/server/tsdbd.c")" -ne 7 ]; then echo "mutant-slowprobe: the DIVERGED mutant did not apply"; rm -rf "$tmp" "$tmp.div"; exit 1; fi
(cd "$tmp" && make -j8 build/tsdbd >"$tmp/build.log" 2>&1) || { echo "mutant-slowprobe: the build failed"; tail -5 "$tmp/build.log"; rm -rf "$tmp" "$tmp.div"; exit 1; }
(cd "$tmp.div" && make -j8 build/tsdbd >"$tmp.div/build.log" 2>&1) || { echo "mutant-slowprobe: the build failed"; tail -5 "$tmp.div/build.log"; rm -rf "$tmp" "$tmp.div"; exit 1; }
out=$(JEPSEN_TSDBD="$tmp/build/tsdbd" "${JEPSEN_BIN:-$root/build/jepsen}" --dir "$dir" --nemesis outage --seconds "$secs" \
    --seed "$seed" --outagefs "$root/build/liboutagefs.so" 2>&1); st=$?
outd=$(JEPSEN_TSDBD="$tmp.div/build/tsdbd" "${JEPSEN_BIN:-$root/build/jepsen}" --dir "$dir" --nemesis outage --seconds "$secs" \
    --seed "$seed" --outagefs "$root/build/liboutagefs.so" 2>&1); std=$?
rm -rf "$tmp" "$tmp.div"
rc=0
if [ $st -ne 3 ] || ! echo "$out" | grep -q '\[inconclusive, check outage-heal\] volume [0-9]*: still no write acknowledged' ||
   ! echo "$out" | grep -q 'slow write probes of this domain'; then
    echo "mutant-slowprobe: a domain whose probes are all reported slow was not an INCONCLUSIVE liveness failure naming the probes (rc $st, wanted 3)"; echo "$out" | tail -n 12; rc=1
else
    echo "mutant-slowprobe: caught as INCONCLUSIVE (liveness): $(echo "$out" | grep '^outage heals')"
fi
if [ $std -ne 1 ] || ! echo "$outd" | grep -q 'and the server reports a volume as failed (UNAVAILABLE, reason DIVERGED)'; then
    echo "mutant-slowprobe: a volume that the server reports as failed for good and that never takes a write was not a violation (rc $std, wanted 1)"; echo "$outd" | tail -n 12; rc=1
else
    echo "mutant-slowprobe: a volume reported DIVERGED that never comes back is a violation"
fi
exit $rc
