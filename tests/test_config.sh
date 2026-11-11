#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# Configuration files (docs/config.md): the table of keys covers every option of tsdbd and no more, docs/config.md
# says what the table says, a bad file is refused with file:line (unknown, duplicate or bad key, a list where one value
# is expected, a file others can write), the command line wins, and a tsdbd started from a file serves with its
# settings (the port the kernel chose and the port file, the workers, login with a superuser made by the CLI, the
# CLI's own connection settings from tsdb.properties). The parser itself is tests/test_props.c.
#
#   B=build tests/test_config.sh [scratch dir]      (make test runs it)
set -u
cd "$(dirname "$0")/.."
B=${B:-build}
dir=$(mktemp -d "${1:-${TMPDIR:-/tmp}}/tsdb-config.XXXXXX") || exit 1
me=$BASHPID
srv=""
# A server that is a child of this shell is a zombie between its exit and our wait, and kill -0 still succeeds on a
# zombie: ask ps for the state.
alive() {
    local st
    st=$(ps -o stat= -p "$1" 2>/dev/null)
    [ -n "$st" ] && [ "${st#Z}" = "$st" ]
}
# stop_srv PID: SIGTERM, then SIGKILL after 10 s (never a bare wait: a server that ignores the signal would hang make test)
stop_srv() {
    local i
    kill "$1" 2>/dev/null
    for ((i = 0; i < 100; i++)); do alive "$1" || break; sleep 0.1; done
    alive "$1" && kill -9 "$1" 2>/dev/null
    wait "$1" 2>/dev/null
}
# wait_exit PID SECONDS: waits for the process to end and sets WRC to its status; a process that is still running after
# SECONDS is killed (by pid), WRC is 'hung' and the call fails. A server that should have refused to start, and serves
# instead, must fail the test, not hang it.
WRC=""
wait_exit() {
    local i
    for ((i = 0; i < $2 * 10; i++)); do alive "$1" || break; sleep 0.1; done
    if alive "$1"; then
        stop_srv "$1"
        WRC=hung
        return 1
    fi
    wait "$1"
    WRC=$?
}
cleanup() {
    [ -n "$srv" ] && stop_srv "$srv"
    chmod -R u+rwx "${dir:?}" 2>/dev/null
    command rm -rf "${dir:?}"
}
trap cleanup EXIT
errors=0
fail() { echo "test_config: FAIL: $*" >&2; errors=$((errors + 1)); }
cfg() { # cfg NAME text...: a file, mode 600
    local f=$dir/$1
    shift
    printf '%s\n' "$@" > "$f"
    chmod 600 "$f"
}
check() { # what, got, want
    [ "$2" = "$3" ] || fail "$1: got '$2', want '$3'"
}
# refused WHAT PATTERN CMD...: exit status 2 and PATTERN in the message
refused() {
    local what=$1 pat=$2 out rc
    shift 2
    out=$("$@" 2>&1 >/dev/null)
    rc=$?
    [ $rc = 2 ] || fail "$what: exit $rc, want 2 ($out)"
    echo "$out" | grep -q -- "$pat" || fail "$what: message '$out' lacks '$pat'"
}

# 1. every option of tsdbd.c has a row in the table, and every row an option (and the same for the file's keys)
opts=$(sed -n '/static struct option opts\[\]/,/};/p' server/tsdbd.c | grep -o '{"[a-z-]*"' | tr -d '{"' | sort -u)
rows=$("$B/tsdbd" --config-keys | sed -n 's/^| `[a-z.]*` | `--\([a-z-]*\)`.*/\1/p; /^Options with no key/,$ s/^| `--\([a-z-]*\)`.*/\1/p' | sort -u)
[ -n "$opts" ] && [ -n "$rows" ] || fail "no options or rows found"
diff <(echo "$opts") <(echo "$rows") > "$dir/d" || fail "options and table differ: $(tr '\n' ' ' < "$dir/d")"
keys=$("$B/tsdbd" --config-keys | sed -n 's/^| `\([a-z.]*\)` | `--.*/\1/p')
check "duplicate keys" "$(echo "$keys" | sort | uniq -d | tr '\n' ' ')" ""
for k in $keys; do
    case $k in [a-z]*.[a-z]*) ;; *) fail "key $k is not a dotted key" ;; esac
done

# 2. docs/config.md holds the tables that the binaries print
for prog in tsdbd tsdb; do
    "$B/$prog" --config-keys > "$dir/keys.$prog"
    awk -v p="$prog" '$0 == "<!-- keys:" p ":begin -->" {on = 1; next} $0 == "<!-- keys:" p ":end -->" {on = 0} on' \
        docs/config.md > "$dir/doc.$prog"
    [ -s "$dir/doc.$prog" ] || fail "docs/config.md has no key block for $prog"
    diff "$dir/keys.$prog" "$dir/doc.$prog" > "$dir/d" \
        || fail "docs/config.md's table for $prog differs from '$prog --config-keys' ($(wc -l < "$dir/d") lines)"
done

# the example of the tarball is a good file, and the unit reads it from where README.dist installs it
cp dist/tsdbd.properties "$dir/dist.properties" && chmod 600 "$dir/dist.properties"
"$B/tsdbd" --config "$dir/dist.properties" --check-config > /dev/null 2>&1 || fail "dist/tsdbd.properties is refused"
grep -q '^ExecStart=/usr/local/bin/tsdbd --config /etc/tsdb/tsdbd.properties$' dist/tsdbd.service \
    || fail "dist/tsdbd.service does not start tsdbd from /etc/tsdb/tsdbd.properties"
grep -q 'tsdbd.properties' tools/mkdist.sh || fail "tools/mkdist.sh does not pack dist/tsdbd.properties"

# 3. files that are refused, with the file and the line
cfg ok.properties 'storage.dir = /x' 'server.port = 7000'
"$B/tsdbd" --config "$dir/ok.properties" --check-config > /dev/null || fail "a good file was refused"
cfg unk.properties '# c' 'storage.dir = /x' 'storage.dri = /y'
refused "unknown key" 'unk.properties:3: .*unknown key "storage.dri"' "$B/tsdbd" --config "$dir/unk.properties" --check-config
cfg dup.properties 'server.port = 1' '' 'server.port = 2'
refused "duplicate key" 'dup.properties:3: .*server.port.*\(duplicate\|twice\|already\)' "$B/tsdbd" --config "$dir/dup.properties" --check-config
cfg bad.properties 'storage.dir = /x' 'server.port = seventy'
refused "bad number" 'bad.properties:2: .*server.port' "$B/tsdbd" --config "$dir/bad.properties" --check-config
cfg badb.properties 'storage.verify.checksums = maybe'
refused "bad boolean" 'badb.properties:1: .*storage.verify.checksums' "$B/tsdbd" --config "$dir/badb.properties" --check-config
cfg bade.properties 'server.io = kqueue'
refused "bad enum" 'bade.properties:1: .*server.io' "$B/tsdbd" --config "$dir/bade.properties" --check-config
cfg badd.properties 'server.send.timeout = 5 parsecs'
refused "bad duration" 'badd.properties:1: .*server.send.timeout' "$B/tsdbd" --config "$dir/badd.properties" --check-config
cfg lst.properties 'storage.dir = /a, /b'
refused "list for one value" 'lst.properties:1: .*storage.dir' "$B/tsdbd" --config "$dir/lst.properties" --check-config
cfg esc.properties 'bogus\' '= 1'
refused "syntax: continuation" 'esc.properties:1' "$B/tsdbd" --config "$dir/esc.properties" --check-config
cfg uesc.properties 'storage.dir = a\q'
refused "syntax: bad escape" 'uesc.properties:1' "$B/tsdbd" --config "$dir/uesc.properties" --check-config
cfg wr.properties 'server.port = 1'
chmod 664 "$dir/wr.properties"
refused "group-writable file" 'writable by its group or by others' "$B/tsdbd" --config "$dir/wr.properties" --check-config
chmod 646 "$dir/wr.properties"
refused "world-writable file" 'writable by its group or by others' "$B/tsdbd" --config "$dir/wr.properties" --check-config
"$B/tsdbd" --config "$dir/nonexistent.properties" --check-config > /dev/null 2>&1
check "a named file that is missing" $? 2
# a value on the command line is not excused: a bad line of the file fails even where the command line overrides it
refused "bad line overridden" 'bad.properties:2' "$B/tsdbd" --config "$dir/bad.properties" --port 7001 --check-config

# 4. lists, escapes and the syntax of .properties
cfg list.properties 'health.domains = /mnt/a, /mnt/b ,/mnt/c\,d' 'storage.dir: /data/with\ space' \
    'server.listen 0.0.0.0' '! a comment' '# another' 'wal.sync = \u0035ms' 'metrics.port = 9100' 'metrics.bind = 127.\' '   0.0.1'
out=$("$B/tsdbd" --config "$dir/list.properties" --check-config 2>&1) || fail "list file refused: $out"
echo "$out" | grep -q '^health.domains = /mnt/a, /mnt/b, /mnt/c\\,d$' || fail "list not shown as written: $(echo "$out" | grep health)"
echo "$out" | grep -q '^storage.dir = /data/with space$' || fail "escaped space: $(echo "$out" | grep storage.dir)"
echo "$out" | grep -q '^server.listen = 0.0.0.0$' || fail "whitespace separator"
echo "$out" | grep -q '^wal.sync = 5ms$' || fail "\\u escape: $(echo "$out" | grep wal.sync)"
echo "$out" | grep -q '^metrics.bind = 127.0.0.1$' || fail "continuation: $(echo "$out" | grep metrics.bind)"

# 5. precedence: the command line, then the file, then the default; --check-config says which
cfg prec.properties 'storage.dir = /x' 'server.port = 7001' 'server.workers = 3' 'storage.read.only = true'
out=$("$B/tsdbd" --config "$dir/prec.properties" --port 7002 --check-config 2>&1) || fail "check-config: $out"
check "port from the command line" "$(echo "$out" | grep -B1 '^server.port = ' | tr '\n' '|')" "# command line|server.port = 7002|"
check "workers from the file" "$(echo "$out" | grep -B1 '^server.workers = ' | tr '\n' '|' | sed "s|$dir/||")" "# prec.properties:3|server.workers = 3|"
check "io is the default" "$(echo "$out" | grep -B1 '^server.io = ' | tr '\n' '|')" "# default|server.io = auto|"
out=$("$B/tsdbd" --config "$dir/prec.properties" --no-config --dir /x --check-config 2>&1) || fail "--no-config: $out"
check "no file with --no-config" "$(echo "$out" | grep '^server.port = ')" "server.port = 7070"
# a list on the command line replaces the file's list
cfg lst2.properties 'storage.dir = /x' 'health.domains = /mnt/a, /mnt/b'
out=$("$B/tsdbd" --config "$dir/lst2.properties" --health-domain /mnt/z --check-config 2>&1)
check "list replaced" "$(echo "$out" | grep '^health.domains')" "health.domains = /mnt/z"
# a flag set in the file is undone on the command line with --FLAG=false, and set with --FLAG=true (--no-X flags: the
# option's own truth, so --no-auth=false is login on)
cfg flag.properties 'storage.dir = /x' 'auth.enabled = false' 'storage.fsync = false' 'storage.read.only = true' \
    'storage.pin.index = false'
shown() { # shown KEY CMD...: the effective value of KEY and where it comes from
    local k=$1 o
    shift
    o=$("$@" --check-config 2>&1) || { echo "refused: $o"; return; }
    echo "$o" | grep -B1 "^$k = " | tr '\n' '|' | sed "s|$dir/||"
}
F="$B/tsdbd --config $dir/flag.properties"
check "auth off from the file" "$(shown auth.enabled $F)" "# flag.properties:2|auth.enabled = false|"
check "fsync off from the file" "$(shown storage.fsync $F)" "# flag.properties:3|storage.fsync = false|"
check "--no-auth=false" "$(shown auth.enabled $F --no-auth=false)" "# command line|auth.enabled = true|"
check "--no-auth=true" "$(shown auth.enabled $F --no-auth=true)" "# command line|auth.enabled = false|"
check "--no-auth" "$(shown auth.enabled $F --no-auth)" "# command line|auth.enabled = false|"
check "--no-fsync=false" "$(shown storage.fsync $F --no-fsync=false)" "# command line|storage.fsync = true|"
check "--read-only=false" "$(shown storage.read.only $F --read-only=false)" "# command line|storage.read.only = false|"
check "--pin-index=true" "$(shown storage.pin.index $F --pin-index=true)" "# command line|storage.pin.index = true|"
refused "a flag with a word that is no boolean" 'read-only' $F --read-only=maybe --check-config
# the same at run time: login is off in the file; --no-auth=false asks for it, and a database without a superuser
# then refuses to start (without the override the server would run with no login)
mkdir -p "$dir/db0"
cfg runflag.properties "storage.dir = $dir/db0" 'server.listen = 127.0.0.1' 'server.port = 0' \
    "server.port.file = $dir/port0" 'auth.enabled = false' 'server.io = epoll'
"$B/tsdbd" --config "$dir/runflag.properties" --no-auth=false --exit-with-parent=$me > "$dir/log0" 2>&1 &
p0=$!
wait_exit "$p0" 10 || fail "--no-auth=false at run time: tsdbd served without a login instead of refusing to start"
check "--no-auth=false at run time: tsdbd refuses a database with no superuser" "$WRC" 1
grep -q 'superuser' "$dir/log0" || fail "--no-auth=false at run time: no word about a superuser: $(tail -3 "$dir/log0")"
command rm -f "$dir/port0"
"$B/tsdbd" --config "$dir/runflag.properties" --exit-with-parent=$me > "$dir/log0" 2>&1 &
p0=$!
for _ in $(seq 1 100); do
    [ -s "$dir/port0" ] && break
    alive "$p0" || break
    sleep 0.1
done
[ -s "$dir/port0" ] || fail "tsdbd with auth.enabled = false did not start: $(tail -3 "$dir/log0")"
stop_srv "$p0"
# and its warning says where the setting came from
grep -q 'auth.enabled (.*runflag.properties:5): no login' "$dir/log0" \
    || fail "the no-login warning does not name the file and line: $(grep -i 'no login' "$dir/log0")"
# one rule for repeats: the last occurrence of a flag wins, in either form, and what --check-config shows is what runs
check "--no-auth then --no-auth=false" "$(shown auth.enabled $F --no-auth --no-auth=false)" "# command line|auth.enabled = true|"
check "--no-auth=false then --no-auth" "$(shown auth.enabled $F --no-auth=false --no-auth)" "# command line|auth.enabled = false|"
check "--no-auth=true then --no-auth=false" "$(shown auth.enabled $F --no-auth=true --no-auth=false)" "# command line|auth.enabled = true|"
check "--no-auth=false then --no-auth=true" "$(shown auth.enabled $F --no-auth=false --no-auth=true)" "# command line|auth.enabled = false|"
check "--read-only then =false" "$(shown storage.read.only $F --read-only --read-only=false)" "# command line|storage.read.only = false|"
check "--read-only=false then --read-only" "$(shown storage.read.only $F --read-only=false --read-only)" "# command line|storage.read.only = true|"
check "a flag's value is not an option: --dir --no-auth" "$(shown storage.dir $F --dir --no-auth)" "# command line|storage.dir = --no-auth|"
check "the last --port wins" "$(shown server.port $F --port 7001 --port 7002)" "# command line|server.port = 7002|"
check "the last --port wins, reversed" "$(shown server.port $F --port 7002 --port 7001)" "# command line|server.port = 7001|"
check "a list keeps every element" "$(shown health.domains $F --health-domain /a --health-domain /b)" "# command line|health.domains = /a, /b|"
# at run time: the database has no superuser, so a server that would run with login refuses to start (exit 1) and one
# without login serves; the file says login off, so only the command line's last form decides
for order in "--no-auth --no-auth=false" "--no-auth=false --no-auth=true --no-auth=false"; do
    command rm -f "$dir/port0"
    "$B/tsdbd" --config "$dir/runflag.properties" $order --exit-with-parent=$me > "$dir/log0" 2>&1 &
    p0=$!
    wait_exit "$p0" 10 || fail "$order: tsdbd served without a login (the last form is =false)"
    check "$order at run time" "$WRC" 1
done
command rm -f "$dir/port0"
"$B/tsdbd" --config "$dir/runflag.properties" --no-auth=false --no-auth --exit-with-parent=$me > "$dir/log0" 2>&1 &
p0=$!
for _ in $(seq 1 100); do
    [ -s "$dir/port0" ] && break
    alive "$p0" || break
    sleep 0.1
done
[ -s "$dir/port0" ] || fail "--no-auth=false --no-auth at run time: no login was asked for, tsdbd did not serve: $(tail -3 "$dir/log0")"
stop_srv "$p0"

# every boolean row of the table, in each form, over a file that says the same or the opposite
while IFS= read -r row; do
    key=${row%% *}
    rest=${row#* }
    opt=${rest%% *}
    neg=0
    case $rest in *"(when false)"*) neg=1 ;; esac
    for fileval in true false; do
        for mode in none bare t f; do
            case $mode in
            none) arg=""; eff=$fileval ;;
            bare | t) arg="--$opt"; [ $mode = t ] && arg="--$opt=true"; [ $neg = 1 ] && eff=false || eff=true ;;
            f) arg="--$opt=false"; [ $neg = 1 ] && eff=true || eff=false ;;
            esac
            lines=("$key = $fileval")
            extra=""
            case $key in
            cluster.witness)
                lines+=('cluster.enabled = true' 'cluster.secret.file = /s' 'cluster.state.dir = /st')
                [ $eff = false ] && extra="--dir /x" ;;
            cluster.enabled)
                lines+=('storage.dir = /x')
                [ $eff = true ] && extra="--cluster-secret-file /s --state-dir /st" ;;
            *) lines+=('storage.dir = /x') ;;
            esac
            cfg bool.properties "${lines[@]}"
            [ $mode = none ] && want="# bool.properties:1|$key = $eff|" || want="# command line|$key = $eff|"
            got=$(shown "$key" "$B/tsdbd" --config "$dir/bool.properties" $arg $extra)
            check "boolean $key: file $fileval, command line '$arg'" "$got" "$want"
        done
    done
done < <("$B/tsdbd" --config-keys | awk -F'|' '$4 ~ /^ boolean *$/ {k = $2; o = $3; w = o ~ /when false/; gsub(/[` ]/, "", k); \
    sub(/ *\(when false\)/, "", o); gsub(/[` ]|^--/, "", o); sub(/^--/, "", o); print k, o (w ? " (when false)" : "")}')
nbool=$("$B/tsdbd" --config-keys | awk -F'|' '$4 ~ /^ boolean *$/' | wc -l)
[ "$nbool" -ge 8 ] || fail "found only $nbool boolean rows in the table"

# abbreviations: getopt_long takes --work for --workers, but the rule "the command line wins" needs the whole name
cfg abbr.properties 'storage.dir = /x' 'server.workers = 3' 'health.domains = /mnt/a' 'server.port.file = /x/pf'
for a in "--work 2" "--work=2" "--workers=2 --port-f /tmp/pf" "--health-dom /mnt/b" "--no-au"; do
    refused "abbreviated option $a" 'abbreviation' "$B/tsdbd" --config "$dir/abbr.properties" $a --check-config
done
refused "abbreviation without a file" 'abbreviation of --workers' "$B/tsdbd" --no-config --work 2 --check-config
out=$("$B/tsdbd" --config "$dir/abbr.properties" --workers 2 --health-domain /mnt/b --dir --work --check-config 2>&1) \
    || fail "whole option names refused: $out"
check "an option's value that looks like an abbreviation" "$(echo "$out" | grep '^storage.dir')" "storage.dir = --work"
check "whole names win over the file" "$(echo "$out" | grep -B1 '^server.workers' | tr '\n' '|')" "# command line|server.workers = 2|"

# the strings of the file's options must outlive the parse: a freed block that is overwritten (tcache off, freed
# memory filled) or unmapped (a long list) broke tsdbd's own copy once
cfg uaf.properties 'storage.dir = /x' 'server.port = 7000' 'server.workers = 3' 'wal.sync = 5ms'
out=$(GLIBC_TUNABLES=glibc.malloc.tcache_count=0 MALLOC_PERTURB_=165 "$B/tsdbd" --config "$dir/uaf.properties" --check-config 2>&1) \
    || fail "options built from the file under perturbed free: $out"
check "values under perturbed free" "$(echo "$out" | grep -c '^server.port = 7000$\|^server.workers = 3$\|^wal.sync = 5ms$')" 3
{
    printf 'storage.dir = /x\nhealth.domains = /a'
    for _ in $(seq 1 10000); do printf ',/a'; done
    printf '\n'
} > "$dir/long.properties"
chmod 600 "$dir/long.properties"
refused "a list of 10,001 domains" 'at most' env MALLOC_PERTURB_=165 "$B/tsdbd" --config "$dir/long.properties" --check-config

# the default file (/etc/tsdb/tsdbd.properties; TSDBD_DEFAULT_CONFIG puts it elsewhere for the tests): read when there
# is no --config, not by a test harness's server (--exit-with-parent), never skipped silently when it cannot be seen
cfg def.properties 'server.port = 7123' 'storage.nope = 1'
refused "the default file is read" 'def.properties:2' env TSDBD_DEFAULT_CONFIG="$dir/def.properties" "$B/tsdbd" --dir /x --check-config
out=$(TSDBD_DEFAULT_CONFIG=$dir/def.properties "$B/tsdbd" --no-config --dir /x --check-config 2>&1) || fail "--no-config reads the default: $out"
out=$(TSDBD_DEFAULT_CONFIG=$dir/def.properties "$B/tsdbd" --exit-with-parent --dir /x --check-config 2>&1) \
    || fail "a harness's tsdbd read the default file: $out"
check "a harness's tsdbd ignores the default file" "$(echo "$out" | grep '^server.port = ')" "server.port = 7070"
out=$(TSDBD_DEFAULT_CONFIG=$dir/def.properties "$B/tsdbd" --exit-with-parent --config "$dir/ok.properties" --check-config 2>&1) \
    || fail "a harness's tsdbd refused the file it names: $out"
check "a harness names its file" "$(echo "$out" | grep '^server.port = ')" "server.port = 7000"
cfg def2.properties 'server.port = 7123'
out=$(TSDBD_DEFAULT_CONFIG=$dir/def2.properties "$B/tsdbd" --dir /x --check-config 2>&1)
check "the default file's setting" "$(echo "$out" | grep '^server.port = ')" "server.port = 7123"
if [ "$(id -u)" != 0 ]; then
    mkdir -p "$dir/locked/tsdb" "$dir/xdgl/tsdb"
    cp "$dir/def.properties" "$dir/locked/tsdb/tsdbd.properties"
    cp "$dir/def.properties" "$dir/xdgl/tsdb/tsdb.properties"
    chmod 000 "$dir/locked/tsdb" "$dir/xdgl/tsdb"
    refused "an unreadable default file (tsdbd)" 'locked/tsdb/tsdbd.properties: Permission denied' \
        env TSDBD_DEFAULT_CONFIG="$dir/locked/tsdb/tsdbd.properties" "$B/tsdbd" --dir /x --check-config
    refused "an unreadable default file (tsdb)" 'xdgl/tsdb/tsdb.properties: Permission denied' \
        env XDG_CONFIG_HOME="$dir/xdgl" "$B/tsdb" --check-config
    chmod 700 "$dir/locked/tsdb" "$dir/xdgl/tsdb"
else
    echo "test_config: the unreadable-default-file checks are skipped (root can read it)"
fi

# --cluster=false turns the cluster off whatever the file says: the other cluster.* keys of the file are left out, and
# named; without it the cluster settings in a file that has the cluster off are refused with their names and lines
cfg cl.properties 'storage.dir = /x' 'cluster.enabled = true' 'cluster.secret.file = /s' 'cluster.state.dir = /st' \
    'cluster.peer.port = 7100' 'cluster.rack = r1'
"$B/tsdbd" --config "$dir/cl.properties" --check-config > /dev/null 2>&1 || fail "the cluster file is refused"
out=$("$B/tsdbd" --config "$dir/cl.properties" --cluster=false --check-config 2>"$dir/cl.err") \
    || fail "--cluster=false over a cluster file: $(cat "$dir/cl.err")"
check "--cluster=false" "$(echo "$out" | grep -B1 '^cluster.enabled = ' | tr '\n' '|')" "# command line|cluster.enabled = false|"
check "--cluster=false leaves the file's settings out" "$(echo "$out" | grep -B1 '^cluster\.' | grep -c 'cl.properties')" 0
grep -q 'cluster.secret.file.*cluster.state.dir.*cluster.peer.port.*cluster.rack' "$dir/cl.err" \
    || fail "--cluster=false does not name the settings it ignores: $(cat "$dir/cl.err")"
cfg cl2.properties 'storage.dir = /x' 'cluster.enabled = false' 'cluster.peer.port = 7100' 'cluster.rack = r1'
refused "cluster settings with the cluster off" 'cluster.peer.port (.*cl2.properties:3).*cluster.rack (.*cl2.properties:4)' \
    "$B/tsdbd" --config "$dir/cl2.properties" --check-config
refused "--peer-port next to --cluster=false" 'cluster settings are given (--peer-port)' \
    "$B/tsdbd" --config "$dir/cl.properties" --cluster=false --peer-port 9 --check-config
refused "--peer-port with no cluster" 'cluster settings are given (--peer-port)' "$B/tsdbd" --dir /x --peer-port 9 --check-config
# a bare --cluster=false with no file keys is just off
out=$("$B/tsdbd" --no-config --dir /x --cluster=false --check-config 2>&1) || fail "--cluster=false alone: $out"

# the keys that the device classes will take are reserved: in the table, refused when a file sets one
for k in storage.disks storage.tiers; do
    "$B/tsdbd" --config-keys | grep -q "^| \`$k\` | " || fail "--config-keys does not list the reserved key $k"
    cfg res.properties 'storage.dir = /x' "$k = /nvme0:nvme, /hdd1:hdd"
    refused "reserved key $k" "res.properties:2: $k is reserved and not implemented yet" \
        "$B/tsdbd" --config "$dir/res.properties" --check-config
    refused "reserved key $k with --no-config" "res.properties:2: $k is reserved" \
        "$B/tsdbd" --config "$dir/res.properties" --dir /y --check-config
done
"$B/tsdbd" --no-config --dir /x --check-config 2>&1 | grep -q '^storage.disks' && fail "--check-config prints a reserved key"

# a harness's server that skips an existing default file says so, once, with the way to use it
cfg hdef.properties 'server.port = 7123'
err=$(TSDBD_DEFAULT_CONFIG=$dir/hdef.properties "$B/tsdbd" --exit-with-parent --dir /x --check-config 2>&1 >/dev/null)
check "one line about the skipped default file" "$(echo "$err" | wc -l)" 1
echo "$err" | grep -q "hdef.properties exists and is not read.*--config $dir/hdef.properties" \
    || fail "no line that names the skipped default file and --config: $err"
err=$(TSDBD_DEFAULT_CONFIG=$dir/none.properties "$B/tsdbd" --exit-with-parent --dir /x --check-config 2>&1 >/dev/null)
check "no line when there is no default file" "$err" ""
err=$(TSDBD_DEFAULT_CONFIG=$dir/hdef.properties "$B/tsdbd" --exit-with-parent --config "$dir/ok.properties" --check-config 2>&1 >/dev/null)
check "no line when the harness names its file" "$err" ""

# 6. a tsdbd started from a file serves with its settings
cfg serve.properties "storage.dir = $dir/db" 'server.listen = 127.0.0.1' 'server.port = 0' \
    "server.port.file = $dir/port" 'server.workers = 3' 'metrics.port = auto' 'server.io = epoll'
TSDB_PASSWORD=pencil "$B/tsdb" "$dir/db" user add root --superuser > /dev/null || fail "add root"
"$B/tsdbd" --config "$dir/serve.properties" --exit-with-parent=$me > "$dir/log" 2>&1 &
srv=$!
for _ in $(seq 1 100); do
    [ -s "$dir/port" ] && break
    alive "$srv" || break
    sleep 0.1
done
if [ ! -s "$dir/port" ]; then
    fail "tsdbd did not start: $(tail -3 "$dir/log")"
else
    read -r port pid mport < "$dir/port"
    check "pid in the port file" "$pid" "$srv"
    [ "${port:-0}" -gt 0 ] 2>/dev/null || fail "no port in '$(cat "$dir/port")'"
    if [ -n "${mport:-}" ]; then
        exec 3<>"/dev/tcp/127.0.0.1/$mport"
        printf 'GET /metrics HTTP/1.0\r\n\r\n' >&3
        m=$(cat <&3)
        exec 3<&-
        echo "$m" | grep -q '^tsdb_workers 3$' || fail "tsdb_workers is not 3: $(echo "$m" | grep '^tsdb_workers ')"
    else
        fail "no metrics port in the port file"
    fi
    # login is on (the file did not turn it off): the CLI reads its connection settings from its own file
    printf 'pencil\n' > "$dir/pw"
    chmod 600 "$dir/pw"
    cfg tsdb.properties "client.server = 127.0.0.1:$port" 'client.login = root' "client.login.password.file = $dir/pw"
    out=$("$B/tsdb" --config "$dir/tsdb.properties" user whoami 2>&1) || fail "tsdb user whoami with a file: $out"
    echo "$out" | grep -q root || fail "whoami does not say root: $out"
    "$B/tsdb" --no-config user whoami --server "127.0.0.1:$port" > /dev/null 2>&1 \
        && fail "whoami without a login worked"
    # the same through the default place of the file
    mkdir -p "$dir/xdg/tsdb"
    cp "$dir/tsdb.properties" "$dir/xdg/tsdb/tsdb.properties"
    out=$(XDG_CONFIG_HOME=$dir/xdg "$B/tsdb" user whoami 2>&1) || fail "default place: $out"
    echo "$out" | grep -q root || fail "default place: whoami does not say root: $out"
    stop_srv "$srv"
    srv=""
fi
cfg cli-bad.properties 'client.server = x' 'client.nope = 1'
refused "CLI unknown key" 'cli-bad.properties:2' "$B/tsdb" --config "$dir/cli-bad.properties" --check-config

if [ $errors = 0 ]; then echo "test_config: ok"; else echo "test_config: $errors failure(s)"; fi
exit $((errors > 0))
