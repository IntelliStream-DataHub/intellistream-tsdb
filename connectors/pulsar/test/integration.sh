#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# Integration test of the Pulsar sink: Pulsar 5.0.0 standalone in Podman, build/tsdbd on the host, the sink's NAR run
# with `pulsar-admin sinks localrun` inside the Pulsar container on a scalable topic (topic://) with key-ordered
# delivery. Produces N points over 500 series with corrections and checks that every point reaches tsdb exactly
# once with its last value, through these faults:
#   A  kill -9 of tsdbd mid-stream, restarted on the same port; poison records sent meanwhile
#   B  tsdbd stopped for STOP_S seconds while producing: Pulsar keeps the backlog, the sink stays up and blocked;
#      with login, the sink's password is changed while tsdbd is down, so tsdbd refuses the sink's login after its
#      start: the backlog must hold until the password is put back with the CLI beside the running tsdbd
#   C  kill -9 of the sink mid-stream (the instance JVM that runs it and the localrun JVM that spawned it),
#      restarted: unacked records are redelivered
# then the poison records must be on the dead-letter topic, and nothing else.
#   S  selection: the first sink stops and a second one (its own topic and subscription) takes only the series named
#      acme.* and creates them in a volume the operator added with `tsdb DIR volume add` beside the running tsdbd;
#      every acme.* point must be in that volume, and no globex.* series may exist (acknowledged and dropped)
#
# Login (the default; AUTH=0 runs tsdbd with --no-auth): USERS is made by the CLI before tsdbd starts, with a
# superuser `root` (verify reads as root) and the sink's user `sink`, granted `insert prefix:pump` and
# `insert prefix:acme.` (for S) alone. The sink
# gets its user in the sink config and its password as the secret `password` (`localrun --secrets`, whose default
# provider is clear text). One more poison record names a series outside the grant: DENIED, dead-lettered.
#
# Usage: connectors/pulsar/test/integration.sh [WORKDIR]   (env: N=300000 STOP_S=180 AUTH=1 KEEP=1 keeps the
# container). Maven uses the tree's own repository (build/m2, then ~/.m2 read-only for the rest), so the NAR packs
# this tree's tsdb-client, not another tree's build of the same version.
# Podman only. Everything is bound to 127.0.0.1; only the container and the processes this script started are
# stopped, by name and by pid.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
CONN=$(dirname "$HERE")
ROOT=$(cd "$CONN/../.." && pwd)
IMAGE=docker.io/apachepulsar/pulsar@sha256:11e9a885121e16be91da77fdd3cfc3d4cbbece4700271d75e949585dbd3a5a5b
HOSTIP=169.254.1.9 # the host's loopback as the container sees it (pasta --map-host-loopback)
N=${N:-300000}
STOP_S=${STOP_S:-180}
AUTH=${AUTH:-1}
W=${1:-$(mktemp -d "${TMPDIR:-/tmp}/tsdb-pulsar-it.XXXXXX")}
mkdir -p "$W"
W=$(cd "$W" && pwd -P) # absolute and without symlinks: the volume of S is named by its path
NAME=tsdb-pulsar-it-$$
TOPIC=topic://public/default/points
DLQ=topic://public/default/points-dlq
TSDBD_PID=

log() { echo "[$(date +%H:%M:%S)] $*"; }
die() { log "FAIL: $*"; exit 1; }
free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'; }
# Signals only a real child pid: never 0 or -1 (kill -1 reaches every process of the user).
signal() {
    local sig=$1 pid=$2
    [[ "$pid" =~ ^[0-9]+$ ]] && [ "$pid" -gt 1 ] || die "refusing to signal pid '$pid'"
    kill "-$sig" "$pid" 2>/dev/null || true
}

cleanup() {
    [ -n "$TSDBD_PID" ] && signal TERM "$TSDBD_PID"
    if [ -z "${KEEP:-}" ]; then
        podman rm -f "$NAME" > /dev/null 2>&1 || true
    else
        log "kept container $NAME"
    fi
}
trap cleanup EXIT

log "workdir $W"
log "building tsdbd, the NAR and the driver"
make -C "$ROOT" -s build/tsdbd build/tsdb
mvn() { command mvn -Dmaven.repo.local="$ROOT/build/m2" -Dmaven.repo.local.tail="$HOME/.m2/repository" "$@"; }
(cd "$ROOT/client/java" && mvn -q install -DskipTests > "$W/mvn.log" 2>&1) || die "client/java install, see $W/mvn.log"
(cd "$CONN" && mvn -q -o package -DskipTests >> "$W/mvn.log" 2>&1) || (cd "$CONN" && mvn -q package -DskipTests >> "$W/mvn.log" 2>&1)
NAR=$(ls "$CONN"/target/tsdb-pulsar-sink-*.nar)
(cd "$CONN" && mvn -q -Pintegration dependency:build-classpath -Dmdep.outputFile=target/it-cp.txt \
    -Dmdep.includeScope=test >> "$W/mvn.log" 2>&1)
mkdir -p "$CONN/target/it-classes"
javac -nowarn -cp "$(cat "$CONN/target/it-cp.txt")" -d "$CONN/target/it-classes" "$HERE/IntegrationDriver.java"
driver() { java -cp "$CONN/target/it-classes:$(cat "$CONN/target/it-cp.txt")" IntegrationDriver "$@"; }

# a tsdbd that has it ends with this script, however the script ends. A here-string, not a pipe: --help exits 2
# (and grep -q may SIGPIPE it), which under pipefail would fail the pipe and never pass the flag.
EXIT_WITH_PARENT=
if grep -q -- --exit-with-parent <<<"$("$ROOT/build/tsdbd" --help 2>&1)"; then
    EXIT_WITH_PARENT=--exit-with-parent
fi
BP=$(free_port); WP=$(free_port) # Pulsar's ports; tsdbd's port comes from tsdbd itself
TP=
URL=pulsar://127.0.0.1:$BP

# tsdbd picks its port (--port 0) and says which in a port file. The sink needs a stable port across the kill and
# restart below, so a restart asks for the same one (TP) and fails loudly if it cannot have it. In that gap the port
# is free, so the final "verify" also reads the data back from our own server.
start_tsdbd() {
    local pf="$W/tsdbd.port" want=${TP:-0} port pid
    rm -f "$pf"
    # shellcheck disable=SC2046 # no option at all with login on
    "$ROOT/build/tsdbd" --dir "$W/data" --listen 127.0.0.1 --port "$want" --port-file "$pf" --workers 2 \
        $([ "$AUTH" = 1 ] || echo --no-auth) $EXIT_WITH_PARENT >> "$W/tsdbd.log" 2>&1 &
    TSDBD_PID=$!
    for _ in $(seq 600); do
        kill -0 "$TSDBD_PID" 2>/dev/null || { wait "$TSDBD_PID" 2>/dev/null || true; die "tsdbd did not start (port $want), see $W/tsdbd.log"; }
        if [ -s "$pf" ] && read -r port pid _ < "$pf" && [ -n "$pid" ]; then
            [ "$pid" = "$TSDBD_PID" ] || die "$pf names pid $pid, but we started $TSDBD_PID"
            [ -z "${TP:-}" ] || [ "$port" = "$TP" ] || die "tsdbd restarted on port $port, not $TP"
            TP=$port
            log "tsdbd up (pid $TSDBD_PID, port $TP)"
            return
        fi
        sleep 0.05
    done
    die "tsdbd wrote no port file"
}
stop_tsdbd() { # $1: KILL or TERM
    signal "$1" "$TSDBD_PID"
    wait "$TSDBD_PID" 2>/dev/null || true
    TSDBD_PID=
}

log "starting Pulsar 5.0.0 ($NAME, broker 127.0.0.1:$BP, web 127.0.0.1:$WP)"
podman run -d --name "$NAME" --network pasta:--map-host-loopback,$HOSTIP \
    -p 127.0.0.1:$BP:$BP -p 127.0.0.1:$WP:$WP \
    -e PULSAR_PREFIX_brokerServicePort=$BP -e PULSAR_PREFIX_webServicePort=$WP \
    -e PULSAR_PREFIX_advertisedAddress=127.0.0.1 -e PULSAR_MEM="-Xms512m -Xmx1g" \
    -v "$NAR:/connector/tsdb-pulsar-sink.nar:ro" \
    "$IMAGE" sh -c 'bin/apply-config-from-env-with-prefix.py PULSAR_PREFIX_ conf/standalone.conf > /dev/null &&
        exec bin/pulsar standalone -nss -nfw' > /dev/null
for i in $(seq 120); do
    curl -sf "http://127.0.0.1:$WP/admin/v2/clusters" > /dev/null 2>&1 && break
    [ "$i" = 120 ] && { podman logs --tail 40 "$NAME"; die "Pulsar did not come up"; }
    sleep 2
done
admin() { podman exec "$NAME" bin/pulsar-admin --admin-url "http://127.0.0.1:$WP" "$@"; }
admin scalable-topics create -s 4 public/default/points
admin scalable-topics create -s 1 public/default/points-dlq
log "Pulsar up; scalable topics created"

cli() { "$ROOT/build/tsdb" "$W/data" "$@" >> "$W/cli.log" 2>&1 || die "tsdb $* failed, see $W/cli.log"; }
# a password file of a fresh random password (hex: it goes into the --secrets JSON and a shell string unquoted)
pwfile() { (umask 077; od -An -N16 -tx1 /dev/urandom | tr -d ' \n' > "$W/$1.pw"); echo "$W/$1.pw"; }
SINK_USER=; SINK_SECRETS=; VERIFY_LOGIN=()
if [ "$AUTH" = 1 ]; then
    mkdir -p "$W/data"
    cli user add root --superuser --password-file "$(pwfile root)"
    cli user add sink --password-file "$(pwfile sink)"
    cli user grant sink insert prefix:pump # insert alone: a new series needs insert on its name, nothing more
    cli user grant sink insert prefix:acme. # S: the selected tenant; globex.* is neither granted nor selected
    SINK_USER="\"user\":\"sink\","
    SINK_SECRETS="{\"password\":\"$(cat "$W/sink.pw")\"}"
    VERIFY_LOGIN=(root "$W/root.pw")
    log "USERS made by the CLI: root (superuser), sink (insert prefix:pump, prefix:acme.)"
fi

start_tsdbd

SINK_CONFIG="{$SINK_USER\"hosts\":\"$HOSTIP\",\"port\":$TP,\"connections\":2,\"maxInFlightRecords\":5000,\
\"retryMaxMs\":2000,\"connectTimeoutMs\":2000,\"deadLetterTopic\":\"$DLQ\"}"
start_sink() { # $1 $2 $3: name (also the subscription), topic, sink config; the main sink's by default
    local name=${1:-tsdb} topic=${2:-$TOPIC} config=${3:-$SINK_CONFIG}
    # $$ of this sh is the pid of the JVM: pulsar-admin execs java
    podman exec -d "$NAME" sh -c "echo \$\$ > /tmp/sink.pid; exec bin/pulsar-admin sinks localrun \
        --broker-service-url pulsar://127.0.0.1:$BP --archive /connector/tsdb-pulsar-sink.nar \
        --tenant public --namespace default --name $name --inputs $topic --subs-name $name --subs-position Earliest \
        --retain-key-ordering --processing-guarantees ATLEAST_ONCE --sink-config '$config' \
        ${SINK_SECRETS:+--secrets '$SINK_SECRETS'} >> /tmp/sink.log 2>&1" > /dev/null
    sleep 1
    SINK_PID=$(podman exec "$NAME" cat /tmp/sink.pid)
    log "sink started (pid $SINK_PID in the container)"
}
# localrun runs the sink in a child JVM (the instance); alive means both are there
sink_alive() {
    podman exec "$NAME" sh -c "grep -q PulsarAdminTool /proc/$SINK_PID/cmdline && [ -n \"\$(pgrep -P $SINK_PID)\" ]" \
        2>/dev/null
}
# what localrun printed and the instances' logs, of every start
sink_logs() {
    podman exec "$NAME" sh -c 'cat /tmp/sink.log; find /pulsar/logs/functions -name "*.log" | xargs -r cat' 2>/dev/null \
        || true
}
# entries the sink's subscription has not acknowledged, over all segments (a batch counts once: phase B sends unbatched)
backlog() {
    admin scalable-topics stats public/default/points 2>/dev/null | tr -d ' \n' \
        | grep -o '"tsdb":{"type":"[A-Z_]*","msgBacklog":[0-9]*' | grep -o '[0-9]*$' || echo "?"
}
start_sink

# --- A: kill -9 tsdbd mid-stream, poison meanwhile
log "A: producing [0, $((N / 3))) and killing tsdbd -9 in the middle"
driver produce "$URL" "$TOPIC" "$N" 0 $((N / 3)) > "$W/produce-a.log" 2>&1 &
PA=$!
sleep 4
POISON=$(driver poison "$URL" "$TOPIC" $([ "$AUTH" = 1 ] && echo denied) | tail -1)
stop_tsdbd KILL
log "A: tsdbd killed -9; down for 5 s"
sleep 5
start_tsdbd
wait $PA || die "producer A failed: $(tail -5 "$W/produce-a.log")"
cat "$W/produce-a.log"

# --- B: tsdbd stopped for minutes
log "B: stopping tsdbd for $STOP_S s while producing [$((N / 3)), $((2 * N / 3)))"
stop_tsdbd TERM
driver produce "$URL" "$TOPIC" "$N" $((N / 3)) $((2 * N / 3)) unbatched > "$W/produce-b.log" 2>&1 \
    || die "producer B failed: $(tail -5 "$W/produce-b.log")"
cat "$W/produce-b.log"
for t in $(seq 0 30 "$STOP_S"); do
    [ "$t" -gt 0 ] && sleep 30
    sink_alive || die "the sink died while tsdbd was down"
    log "B: t=${t}s sink alive, backlog $(backlog)"
done
B_BACKLOG=$(backlog)
# nothing of B can have been acknowledged while tsdbd was down: Pulsar still holds all of it
[[ "$B_BACKLOG" =~ ^[0-9]+$ ]] && [ "$B_BACKLOG" -ge $((2 * N / 3 - N / 3)) ] \
    || die "backlog $B_BACKLOG after tsdbd was down: records were acknowledged without being written"
if [ "$AUTH" = 1 ]; then
    # tsdbd comes back with another password for the sink: every login it tries is refused, nothing is written
    # or acknowledged, and the sink stays up retrying until the operator puts the password back
    cli user passwd sink --password-file "$(pwfile wrong)"
    REFUSED0=$(sink_logs | grep -c 'refused the login' || true)
    start_tsdbd
    for _ in $(seq 60); do
        [ "$(sink_logs | grep -c 'refused the login' || true)" -gt "$REFUSED0" ] && break
        sleep 1
    done
    sleep 10 # a few more refused retries
    REFUSED=$(( $(sink_logs | grep -c 'refused the login' || true) - REFUSED0 ))
    [ "$REFUSED" -gt 0 ] || die "tsdbd is up with another password, but the sink logged no refused login"
    sink_alive || die "the sink died while its login was refused"
    WB=$(backlog)
    [[ "$WB" =~ ^[0-9]+$ ]] && [ "$WB" -ge "$B_BACKLOG" ] \
        || die "backlog $WB (was $B_BACKLOG) while the sink's login was refused: records acknowledged without a login"
    log "B: sink's login refused $REFUSED times, backlog $WB held; putting its password back beside the running tsdbd"
    cli user passwd sink --password-file "$W/sink.pw" # tsdbd reloads USERS within a second
else
    start_tsdbd
fi

# --- C: kill -9 the sink mid-stream
log "C: producing [$((2 * N / 3)), $N) and killing the sink -9 in the middle"
driver produce "$URL" "$TOPIC" "$N" $((2 * N / 3)) "$N" > "$W/produce-c.log" 2>&1 &
PC=$!
sleep 3
INSTANCE=$(podman exec "$NAME" pgrep -P "$SINK_PID" | tr '\n' ' ')
[[ "$INSTANCE" =~ ^[0-9\ ]+$ ]] || die "no sink instance under localrun pid $SINK_PID"
podman exec "$NAME" sh -c "grep -q PulsarAdminTool /proc/$SINK_PID/cmdline && kill -9 $INSTANCE $SINK_PID" \
    || die "sink pid $SINK_PID is not the localrun JVM"
log "C: sink killed -9 (instance $INSTANCE, localrun $SINK_PID)"
sleep 3
start_sink
wait $PC || die "producer C failed: $(tail -5 "$W/produce-c.log")"
cat "$W/produce-c.log"

# --- results
log "waiting for every point to reach tsdb"
ok=
for _ in $(seq 120); do
    if driver verify 127.0.0.1 "$TP" "$N" "${VERIFY_LOGIN[@]}" > "$W/verify.log" 2>&1; then ok=1; break; fi
    sleep 5
done
cat "$W/verify.log"
[ -n "$ok" ] || die "not every point reached tsdb with its last value"
driver dlq "$URL" "$DLQ" "$POISON" 60 > "$W/dlq.log" 2>&1 || { cat "$W/dlq.log"; die "dead letters"; }
cat "$W/dlq.log"
sink_alive || die "the sink is not running at the end"

# --- S: a second sink that selects one tenant and creates its series in a volume of their own
log "S: stopping the first sink; a sink for acme.* only, into the volume $W/vol-acme"
INSTANCE=$(podman exec "$NAME" pgrep -P "$SINK_PID" | tr '\n' ' ')
[[ "$INSTANCE" =~ ^[0-9\ ]+$ ]] || die "no sink instance under localrun pid $SINK_PID"
podman exec "$NAME" sh -c "grep -q PulsarAdminTool /proc/$SINK_PID/cmdline && kill $INSTANCE $SINK_PID" \
    || die "sink pid $SINK_PID is not the localrun JVM"
for _ in $(seq 60); do podman exec "$NAME" test -e /proc/"$SINK_PID" || break; sleep 1; done
mkdir -p "$W/vol-acme"
cli volume add "$W/vol-acme" # beside the running tsdbd, as an operator does; the root is volume 0, this one 1
admin scalable-topics create -s 2 public/default/tenants
SEL_N=2000
SEL_CONFIG="{$SINK_USER\"hosts\":\"$HOSTIP\",\"port\":$TP,\"deadLetterTopic\":\"$DLQ\",\
\"seriesPrefixes\":[\"acme.\"],\"volumes\":{\"acme.\":\"$W/vol-acme\"}}"
start_sink tsdb-sel topic://public/default/tenants "$SEL_CONFIG"
driver tenants "$URL" topic://public/default/tenants "$SEL_N" > "$W/produce-s.log" 2>&1 \
    || die "producer S failed: $(tail -5 "$W/produce-s.log")"
cat "$W/produce-s.log"
ok=
for _ in $(seq 60); do
    if driver verify-tenants 127.0.0.1 "$TP" "$SEL_N" 1 "${VERIFY_LOGIN[@]}" > "$W/verify-s.log" 2>&1; then ok=1; break; fi
    sleep 2
done
cat "$W/verify-s.log"
[ -n "$ok" ] || die "S: the acme.* points are not all in volume 1, or a globex.* series exists"
sink_alive || die "the selecting sink is not running at the end"
sink_logs > "$W/sink.log"
grep -q 'this instance does not take and were dropped' "$W/sink.log" \
    || die "S: the selecting sink logged no dropped points"
log "sink logs: $(grep -c 'retrying in' "$W/sink.log" || true) retry lines, \
$(grep -c 'record dead-lettered' "$W/sink.log" || true) dead-letter lines"
log "PASS: N=$N points, kill -9 tsdbd, tsdbd down ${STOP_S}s (backlog ${B_BACKLOG:-?}), kill -9 sink, $POISON dead letters\
$([ "$AUTH" = 1 ] && echo ", login on (sink's login refused $REFUSED times, then let in)"), \
selection of acme.* into volume 1 ($SEL_N x 10 points kept, 10 globex.* series dropped)"
