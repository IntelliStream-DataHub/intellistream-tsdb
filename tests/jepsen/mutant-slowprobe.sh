#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# jepsen's outage check must not take "the server says its probes were slow" for an excuse: a tsdbd whose health
# monitor, while a flapping domain is DOWN, reports every write probe slow (more than fast_ms, with no fault and a
# healthy disk) never lets the domain out of DOWN, and the heal must fail within OUTAGE_HARD_S, naming the
# slow-probe lines (none of them has a slow fdatasync of jepsen's own near it), instead of being waited for again after
# every line (the verifier's P5: judged valid after 584 s).
# Builds the mutant from a copy of the tree (git archive of HEAD, under $TMPDIR or /var/tmp), never touches the
# working tree. Usage: tests/jepsen/mutant-slowprobe.sh JEPSEN_DIR [SEED] [SECONDS]   (default seed 2, 120 s: that
# schedule has flapping heals) (JEPSEN_BIN names another jepsen binary than build/jepsen)
set -u
dir=${1:?usage: mutant-slowprobe.sh JEPSEN_DIR [SEED] [SECONDS]}
seed=${2:-2}
secs=${3:-120}
root=$(cd "$(dirname "$0")/../.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-/var/tmp}/mutant-slowprobe.XXXXXX") || exit 2
git -C "$root" archive HEAD | tar -x -C "$tmp" || exit 2
sed -i 's|^    int ms = (int)(now - start);$|&\n    if (write \&\& d->state == TSDB_DOM_DOWN \&\& down_need_ms(d, now) > h->o.recover_ms) ms = h->o.fast_ms + 1; /* MUTANT */|' \
    "$tmp/src/health.c"
if ! grep -q 'MUTANT' "$tmp/src/health.c"; then echo "mutant-slowprobe: the mutant did not apply"; rm -rf "$tmp"; exit 1; fi
(cd "$tmp" && make -j8 build/tsdbd >"$tmp/build.log" 2>&1) || { echo "mutant-slowprobe: the build failed"; tail -5 "$tmp/build.log"; rm -rf "$tmp"; exit 1; }
out=$(JEPSEN_TSDBD="$tmp/build/tsdbd" "${JEPSEN_BIN:-$root/build/jepsen}" --dir "$dir" --nemesis outage --seconds "$secs" \
    --seed "$seed" --outagefs "$root/build/liboutagefs.so" 2>&1); st=$?
rm -rf "$tmp"
if [ $st -ne 1 ] || ! echo "$out" | grep -q 'no write acknowledged in' || ! echo "$out" | grep -q 'slow write probes of this domain'; then
    echo "mutant-slowprobe: a domain whose probes are all reported slow went unnoticed (rc $st)"; echo "$out" | tail -n 12; exit 1
fi
echo "mutant-slowprobe: caught: $(echo "$out" | grep '^outage heals')"
